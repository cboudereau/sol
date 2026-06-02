---
status: draft
---
# DataFusion table registration and Parquet file discovery

Addresses: [FR4](../DESIGN.md#fr4), [NFR1](../DESIGN.md#nfr1), [NFR4](../DESIGN.md#nfr4)

## Problem

The query backend must expose the Parquet files written by the codec as queryable DataFusion tables. **Actual layout (verified in the demo `sol-gateway.yaml` + `parquet-query.sh`):** the codec returns one Parquet blob per signal/metric-subtype, and the file sink writes **one directory per signal** with timestamped files:

```
…/logs/%Y-%m-%d-%H-%M-%S.parquet
…/traces/%Y-%m-%d-%H-%M-%S.parquet
…/metrics/%Y-%m-%d-%H-%M-%S.parquet   ← all metric subtypes share this dir today (queried union_by_name)
```

(There is **no** `data_001_*_logs.parquet` filename-suffix convention — an earlier draft of this ADR assumed one; corrected here to match the code.)

Two questions:
1. How are files mapped to logical tables? `logs` and `traces` are one directory each. **Metrics are the wrinkle**: the five subtypes (`gauge`, `sum`, `histogram`, `exp_histogram`, `summary`) currently land in the *same* `metrics/` directory with *different schemas*.
2. How does DataFusion discover newly written files without a restart, across local FS and S3?

This must also pin the **new external dependency** (DataFusion + object_store), which is the single largest dependency addition in this workspace.

## Options

### Table registration

| Option | Pros | Cons |
|---|---|---|
| A. DataFusion `ListingTable` per signal **directory** (`logs/`, `traces/`, and per-subtype metric dirs) | Built-in; directory-of-Parquet native; pruning by stats; local + object_store URLs | Requires each table to be its **own directory** — needs the sink to write metric subtypes into per-subtype subdirs |
| B. Custom `TableProvider` per signal | Full control over file→table mapping | Reimplements scan planning/pushdown/stats — defeats NFR1 |
| C. Single `metrics/` dir, classify by schema at registration (union_by_name like the demo) | No sink change | Mixed schemas in one listing; brittle subtype routing; loses per-table pruning |

### File discovery / refresh

| Option | Pros | Cons |
|---|---|---|
| A. Re-list on each query (`ListingTable` with `collect_stat`, fresh `ListingOptions`) | Always current; no background task | Listing latency per query (mitigated by cache, [query-caching-strategy](./query-caching-strategy.md)) |
| B. Periodic background re-registration (poll interval) | Listing cost amortized | Staleness window; extra task; coordination with cache TTL |
| C. Catalog system (Iceberg/Delta) | Transactional, scalable | Explicitly a rabbit hole per [DESIGN.md](../DESIGN.md#rabbit-holes) — rejected |

## Decision

**Table registration: Option A — one `ListingTable` per signal table, built from a `resolve_files`-curated list (candidate universe = the signal directory).** This requires a small, low-risk **sink-side change**: write metric subtypes into per-subtype subdirectories so each maps to a clean table:

```
logs/                 → table `logs`
traces/               → table `traces`
metrics/gauge/        → table `gauge`
metrics/sum/          → table `sum`
metrics/histogram/    → table `histogram`
metrics/exp_histogram/→ table `exp_histogram`
metrics/summary/      → table `summary`
```

This is the same write-side hint family as the [FR7](../DESIGN.md#fr7) `dt=` partitioning (a sink path-template change, not a codec change). Until the sink emits per-subtype dirs, the fallback is Option C (single `metrics/` dir, `union_by_name`) for a single combined metric table — usable but without per-subtype pruning.

**Implemented (task 14b):** the per-subtype dirs are produced by a gateway **`route` transform → per-subtype file sinks** (`metrics/<subtype>/dt=…`) — *no codec blob-tagging needed* (the earlier worry): the codec already emits one narrow Parquet per subtype, and routing puts each in its own dir. On the read side, rather than separate `gauge`/`sum`/… tables, the querier keeps a **single `metrics` union table** registered over an explicit file list built by a **recursive walk + per-partition `resolve_files`** (skips raw superseded by a compacted file → no double-count). This preserves the translators' `FROM metrics` contract while gaining narrow per-subtype files; per-subtype *tables* remain a future option. Rollup tiers register as separate `metrics_5m`/`metrics_1h`/`metrics_1d` tables (excluded from the union), selected by query `step`.

**File discovery: Option B — periodic background re-registration over a `resolve_files`-curated file set.**

> **Reconciliation with [compaction-consistency](./compaction-consistency.md):** a *blind* `ListingTable` pointed at a directory reads **every** file in it — once compaction runs, that means raw inputs **and** their compacted output together → **double-count**. So the directory is only the *candidate universe*; the **authoritative read set is produced by `resolve_files`** (highest-`level` per sub-range, superseded inputs skipped). DataFusion's `ListingTable` is built from that **explicit file list** (it accepts a list of object paths, not only a directory glob), re-registered on refresh — we keep `ListingTable`'s pushdown/stats/bloom for free while controlling exactly which files it reads. (Option B "custom `TableProvider`" is therefore only needed if `resolve_files` can't be expressed as a file-list filter — not expected.)

Concretely:
- Candidate files live under a configurable root (`storage.path` local / `storage.url` S3 via `object_store`); each table = one **directory** (above). Today files are flat-timestamped; [FR7](../DESIGN.md#fr7) adds `dt=YYYY-MM-DD/` sub-partitioning (proposed sink change, not current).
- A background task re-runs `resolve_files` per table on a configurable interval (default 15s) and re-registers each `ListingTable` from the resolved list. **Pre-compaction** the resolved set is simply "all files in the dir" (a no-op filter); **post-compaction** it excludes superseded raw inputs by footer `level`/`supersedes`.
- Each table's Arrow schema is **declared explicitly in code** (not inferred) from the column lists in [parquet-multisignal](../../../designs/20260527_parquet-multisignal.md) — the binding codec↔query contract; schema mismatch is a hard error.
- Predicate pushdown for `service_name`, `name`, timestamps via the Parquet reader (row-group stats + page index); `trace_id` bloom read when present.

**Dependency decision (resolved at implementation start, 2026-06):** add behind a new `query-backend` Cargo feature (gating both `src/query/` and the deps) — **`datafusion = "53"`** (v53.1.0; includes `parquet` + `datafusion-functions-nested` for UNNEST), **`object_store = "0.13"`** (features `fs`, `tokio`, + `aws` for S3), **`promql-parser = "0.9"`**. This is the explicit ratification of [NFR1](../DESIGN.md#nfr1).

> **Version-compat note**: the earlier worry about aligning DataFusion's bundled `parquet` with the codec's `parquet = 56.2.0` is **low-risk** — the querier *reads* Parquet files written by the codec (it does not share in-process Arrow types with it), and the Parquet *file format* (TIMESTAMP(NANOS), `FIXED_LEN_BYTE_ARRAY`, UTF8) is interoperable across reader/writer crate versions. DataFusion 53's reader reads the codec's parquet-56 output regardless of minor version skew. The first Session-1 build confirms read-back on a codec-written fixture.

## Consequences

- One new heavyweight dependency tree (`datafusion`, transitively `arrow`, `object_store`) enters the build behind the `query-backend` feature. **Update (2026-06):** by user decision `query-backend` is now in the **`default`** feature set — the stock Sol binary/image ships the query backend so the demo (and downstream images) work without a custom build. The lean-agent path remains available via `--no-default-features` (or a default set excluding `query-backend`); the accepted trade-off is a larger default binary + DataFusion/Arrow in every standard build.
- The seven Arrow schemas live in `src/query/` as the single source of truth mirroring the codec; any codec schema change is a coordinated change here (already flagged in [DESIGN.md Cross-cutting](../DESIGN.md#cross-cutting-concerns)).
- Attribute filters use `json_*` extraction on the JSON-string `attributes`/`resource_attributes`/`scope_attributes` columns ([ADR 0038](../../../adrs/0038-attributes-serialization-strategy.md)); these do not push down into Parquet — accepted per [rabbit hole 4](../DESIGN.md#rabbit-holes).
- S3 support is "free" via `object_store`; local FS is the development default. Both share the same `ListingTable` code path ([NFR4](../DESIGN.md#nfr4)).
- **Sink coupling**: clean per-subtype metric tables require the file sink to write per-subtype subdirectories (and, for FR7, `dt=` partitions). This is a documented write-side dependency of this workspace, tracked in [TASKS.md](../TASKS.md) task 2.
- Freshness is bounded by the poll interval (default 15s). Data flushed to Parquet less than one interval ago may not yet be visible — acceptable given the batch-flush freshness boundary in the non-goals.
