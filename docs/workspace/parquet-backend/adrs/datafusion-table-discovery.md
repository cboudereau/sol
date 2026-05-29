---
status: draft
---
# DataFusion table registration and Parquet file discovery

Addresses: [FR4](../DESIGN.md#fr4), [NFR1](../DESIGN.md#nfr1), [NFR4](../DESIGN.md#nfr4)

## Problem

The query backend must expose the Parquet files written by the codec as queryable DataFusion tables. The codec writes **one file per signal type / metric subtype**, named with a signal suffix (e.g. `data_001_logs.parquet`, `data_001_gauge.parquet`) per [parquet-multisignal](../../../designs/20260527_parquet-multisignal.md). New files appear continuously as batches flush; old files are pruned by retention.

Two questions:
1. How are files mapped to the seven logical tables (`logs`, `traces`, `gauge`, `sum`, `histogram`, `exp_histogram`, `summary`)?
2. How does DataFusion discover newly written files without a restart, across both local FS and S3?

This must also pin the **new external dependency** (DataFusion + object_store), which is the single largest dependency addition in this workspace.

## Options

### Table registration

| Option | Pros | Cons |
|---|---|---|
| A. DataFusion `ListingTable` per signal, one listing path per table | Built-in; handles directory-of-Parquet natively; schema inference + file pruning by stats; supports local + object_store URLs out of the box | All files for a table must share a path prefix or glob — requires a filename/layout convention that segregates signals |
| B. Custom `TableProvider` per signal | Full control over file→table mapping and refresh | Reimplements scan planning, predicate pushdown, statistics — large surface, defeats NFR1 "DataFusion does the work" |

### File discovery / refresh

| Option | Pros | Cons |
|---|---|---|
| A. Re-list on each query (`ListingTable` with `collect_stat`, fresh `ListingOptions`) | Always current; no background task | Listing latency per query (mitigated by cache, [query-caching-strategy](./query-caching-strategy.md)) |
| B. Periodic background re-registration (poll interval) | Listing cost amortized | Staleness window; extra task; coordination with cache TTL |
| C. Catalog system (Iceberg/Delta) | Transactional, scalable | Explicitly a rabbit hole per [DESIGN.md](../DESIGN.md#rabbit-holes) — rejected |

## Decision

**Table registration: Option A — one `ListingTable` per signal table.**
**File discovery: Option B — periodic background re-listing, with on-query listing as the correctness backstop.**

Concretely:
- Files are discovered under a configurable root (`storage.path` for local FS, `storage.url` for S3 via `object_store`). Each signal table maps to a glob that matches its filename suffix: `**/*_logs.parquet`, `**/*_traces.parquet`, `**/*_gauge.parquet`, etc. The suffix convention is the codec's existing output naming ([parquet-multisignal §Cross-cutting](../../../designs/20260527_parquet-multisignal.md)).
- Each table's Arrow schema is **declared explicitly in code** (not inferred) from the known Parquet column lists in [parquet-multisignal](../../../designs/20260527_parquet-multisignal.md), so a schema mismatch in a file is a hard error rather than silent type drift. This is the binding contract between codec and query engine.
- A background task re-lists the storage root on a configurable interval (default 15s, aligned with the dashboard refresh and cache TTL) and re-registers the `ListingTable`s. The poll interval, not a WAL, defines query freshness (consistent with the [non-goals](../DESIGN.md#non-goals)).
- Predicate pushdown for `service_name`, `name`, and the timestamp columns is enabled via DataFusion's Parquet reader (row-group stats + page index). Bloom filters on `trace_id` are read by DataFusion when present in the file.

**Dependency decision:** add `datafusion` and `object_store` behind a new `query-backend` Cargo feature (gating both `src/query/` and the deps), pinned to the DataFusion release whose embedded `parquet` crate is compatible with the codec's `parquet = 56.2.0` (verify alignment at implementation start; if DataFusion's bundled `parquet` major differs, pin DataFusion to the matching release). This is the explicit ratification of [NFR1](../DESIGN.md#nfr1).

## Consequences

- One new heavyweight dependency tree (`datafusion`, transitively `arrow`, `object_store`) enters the build, isolated behind `query-backend` — default builds and the codec's `parquet` feature are unaffected.
- The seven Arrow schemas live in `src/query/` as the single source of truth mirroring the codec; any codec schema change is a coordinated change here (already flagged in [DESIGN.md Cross-cutting](../DESIGN.md#cross-cutting-concerns)).
- Attribute filters use `json_*` extraction on the JSON-string `attributes`/`resource_attributes`/`scope_attributes` columns ([ADR 0038](../../../adrs/0038-attributes-serialization-strategy.md)); these do not push down into Parquet — accepted per [rabbit hole 4](../DESIGN.md#rabbit-holes).
- S3 support is "free" via `object_store`; local FS is the development default. Both share the same `ListingTable` code path ([NFR4](../DESIGN.md#nfr4)).
- Freshness is bounded by the poll interval (default 15s). Data flushed to Parquet less than one interval ago may not yet be visible — acceptable given the batch-flush freshness boundary in the non-goals.
