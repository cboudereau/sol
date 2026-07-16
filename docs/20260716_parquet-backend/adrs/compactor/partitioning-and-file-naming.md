---
status: accepted
---
# Partitioning and file naming at scale

Addresses: [FR7](../../designs/parquet-backend.md#fr7), [NFR4](../../designs/parquet-backend.md#nfr4), [NFR5](../../designs/parquet-backend.md#nfr5), [NFR10](../../designs/parquet-backend.md#nfr10)

Extends [file-layout-and-compaction-strategy](./file-layout-and-compaction-strategy.md) and [compaction-consistency](./compaction-consistency.md).

## Problem

Two questions the file-layout ADR left open, both surfacing once the number of
**distinct services** grows large (thousands+):

1. **Partitioning** — the layout today is `metrics/<subtype>/dt=YYYY-MM-DD/…`,
   files sorted by `(service_name, name, time)`. Should `service_name` become a
   partition axis (a directory level) so per-service queries prune at the path,
   and if so, how — given an unbounded, high-cardinality service set?

2. **File naming** — what guarantees uniqueness when many writers land in the
   same partition, and what keeps compacted/rollup outputs collision-free and
   idempotent? In particular, **there is no GUID** on compacted/rollup files —
   is that correct, and what does the compactor rely on instead?

## Decision

### D1 — Partition by volume, discriminate services by sort + stats, bucket only when forced

- Keep the existing axes: **`dt=` day** (every query is time-ranged) and
  **signal subtype** (prunes by metric family). Both are bounded and already
  pruned at the path.
- **Do not add a per-service directory level.** Service discrimination stays in
  the **sort + row-group min/max + bloom filter** on `service_name`/`prom_name`
  (the InfluxDB 10–100× pruning lever, already in
  [file-layout-and-compaction-strategy](./file-layout-and-compaction-strategy.md)).
  Sorting clusters each service's rows, so a single-service query skips
  non-matching row groups **within** the day's files — no directory explosion
  needed to discriminate among many services.
- **When a single day-partition outgrows efficient scanning**, split it by a
  **bounded hash bucket**, never by service identity:
  `metrics/<subtype>/dt=YYYY-MM-DD/bucket=<hash(service_name) % N>/…`. `N` is a
  fixed compactor/codec config (e.g. 16–256). A single-service query then touches
  exactly `1/N` of the day, with sort+bloom pruning inside. Sort stays
  `(service_name, name, time)` so each service lands in one bucket (good
  compression + locality).

Partitioning is therefore **driven by bytes-per-partition, not by service
count**. Service count never multiplies directories or same-named files.

| Option | Per-service prune | File/dir count | Verdict |
|---|---|---|---|
| `service=<svc>/` directory level | path-level, exact | **unbounded**, tiny-file explosion on the long tail; high S3 `LIST` ([NFR10](../../designs/parquet-backend.md#nfr10)) | rejected |
| sort + row-group stats + bloom (today) | intra-file, near-exact | bounded | **default** |
| `bucket=hash(service)%N` | path-level, 1/N coarse | bounded by `N` | **escalation** when a day-partition is too large |

### D2 — Raw files carry a UUID token; compacted/rollup files are deterministic (no GUID)

- **Raw ingest** (Vector `file` sink): the config path is
  `…/<subtype>/dt=%Y-%m-%d/%H-%M-%S.parquet` (date → `dt=` dir, filename →
  time-of-day). The sink then inserts a **per-flush `uuid::Uuid::new_v4()`
  token** before the extension (`parquet_batch_path`, `src/sinks/file/mod.rs`):
  `HH-MM-SS.parquet` → `HH-MM-SS-<uuid>[-<index>].parquet`. This is what makes
  many concurrent writers (and repeat flushes in the same second) **never
  collide** — exactly the case "many services" stresses.
- **Compacted/rollup** (`src/querier/compaction.rs`, `rollup.rs`):
  **deterministic** names with **no GUID** —
  `compacted-<date>.parquet`, `compacted-h<HH>-<date>.parquet`,
  `rollup-<tier>.parquet`. Within a partition dir these address distinct
  `(level, hour|tier)` slots, so 1 daily + ≤24 hourly + ≤3 rollup coexist
  without clashing.

  A GUID here would be **wrong**: the compactor is a **singleton**
  ([compaction-consistency](./compaction-consistency.md)) writing atomically
  (stage `.tmp` → fsync → rename → dir fsync), so re-compaction is an
  **idempotent overwrite** of the same name; and supersession is tracked by the
  footer `sol.compaction.supersedes` set **keyed on filenames**. GUID names
  would proliferate stale duplicate copies and force a manifest/catalog to pick
  the live one — defeating the deliberately catalog-free design.

| Class | Writers | Uniqueness mechanism |
|---|---|---|
| Raw | many concurrent flushes | UUIDv4 token in filename |
| Compacted / rollup | single compactor | deterministic name + atomic overwrite (no GUID) |

## Consequences

- **Scales to many services** without directory or file-count growth; the only
  lever that grows with data volume is the optional `bucket=` split, bounded by
  `N`.
- **Single-writer invariant is load-bearing.** Deterministic names + atomic
  rename are safe only with one compactor per partition. Concurrent compactors
  on the same partition would race the shared `.tmp` and the rename — out of
  scope (the singleton role forbids it). Multi-writer compaction would require
  GUID names *and* a manifest, i.e. the rejected catalog.
- **Object store (S3):** deterministic names still resolve (final-key PUT is
  last-writer-wins), but the POSIX stage→rename atomicity does not translate —
  an S3 backend needs PUT-atomicity or a copy-commit, an atomicity concern
  ([NFR10](../../designs/parquet-backend.md#nfr10)), not a naming one.
- **`parse_hour` ⇄ path-template coupling (risk to harden).** Intra-day hourly
  compaction derives the hour with `name.split('-').next()` (first token),
  which is correct **only because** the date lives in the `dt=` dir and the
  filename starts with `%H` (the trailing `-<uuid>` is harmless). If the sink
  template were changed to put the date in the filename
  (`…/%Y-%m-%d-%H-%M-%S.parquet`), `parse_hour` would read the year (`2026` ≥ 24
  → `None`) and intra-day compaction would **silently skip every raw file** —
  they would still be read (the daily seal covers them) but never hourly-merged,
  so the small-files / EMFILE failure mode returns. Mitigation: parse the hour
  from the `dt=` dir plus a position-independent time field, or assert the raw
  filename shape on discovery. The sink path template is part of the write↔read
  contract and must be kept in sync with `parse_hour`.
