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

**Table registration: Option A — one `ListingTable` per signal *directory*.** This requires a small, low-risk **sink-side change**: write metric subtypes into per-subtype subdirectories so each maps to a clean table:

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

**File discovery: Option B — periodic background re-listing, with on-query listing as the correctness backstop.**

Concretely:
- Files are discovered under a configurable root (`storage.path` for local FS, `storage.url` for S3 via `object_store`). Each table = one **directory** (above). Today files are flat-timestamped within the dir; [FR7](../DESIGN.md#fr7) adds `dt=YYYY-MM-DD/` sub-partitioning for path-level pruning (a proposed sink change, not current).
- Each table's Arrow schema is **declared explicitly in code** (not inferred) from the column lists in [parquet-multisignal](../../../designs/20260527_parquet-multisignal.md) — the binding codec↔query contract; a schema mismatch is a hard error.
- A background task re-lists the root on a configurable interval (default 15s) and re-registers the tables. Once compaction lands ([compaction-consistency ADR](./compaction-consistency.md)), `resolve_files` additionally honours footer supersession (skip raw inputs covered by a compacted file).
- Predicate pushdown for `service_name`, `name`, timestamps via the Parquet reader (row-group stats + page index); `trace_id` bloom read when present.

**Dependency decision:** add `datafusion` and `object_store` behind a new `query-backend` Cargo feature (gating both `src/query/` and the deps), pinned to the DataFusion release whose embedded `parquet` crate is compatible with the codec's `parquet = 56.2.0` (verify alignment at implementation start; if DataFusion's bundled `parquet` major differs, pin DataFusion to the matching release). This is the explicit ratification of [NFR1](../DESIGN.md#nfr1).

## Consequences

- One new heavyweight dependency tree (`datafusion`, transitively `arrow`, `object_store`) enters the build, isolated behind `query-backend` — default builds and the codec's `parquet` feature are unaffected.
- The seven Arrow schemas live in `src/query/` as the single source of truth mirroring the codec; any codec schema change is a coordinated change here (already flagged in [DESIGN.md Cross-cutting](../DESIGN.md#cross-cutting-concerns)).
- Attribute filters use `json_*` extraction on the JSON-string `attributes`/`resource_attributes`/`scope_attributes` columns ([ADR 0038](../../../adrs/0038-attributes-serialization-strategy.md)); these do not push down into Parquet — accepted per [rabbit hole 4](../DESIGN.md#rabbit-holes).
- S3 support is "free" via `object_store`; local FS is the development default. Both share the same `ListingTable` code path ([NFR4](../DESIGN.md#nfr4)).
- **Sink coupling**: clean per-subtype metric tables require the file sink to write per-subtype subdirectories (and, for FR7, `dt=` partitions). This is a documented write-side dependency of this workspace, tracked in [TASKS.md](../TASKS.md) task 2.
- Freshness is bounded by the poll interval (default 15s). Data flushed to Parquet less than one interval ago may not yet be visible — acceptable given the batch-flush freshness boundary in the non-goals.
