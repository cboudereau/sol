---
status: draft
---
# Materialize `prom_name` at write time

Addresses: [FR1](../DESIGN.md#fr1), [NFR1](../DESIGN.md#nfr1)

## Problem

Metric-name filtering uses the `prom_metric_name(name, unit, is_monotonic)` UDF.
A UDF predicate is opaque to DataFusion's Parquet pruning, so every metric query
full-scans ~14 M rows and runs the UDF per row (1.8 s vs 0.1 s for a prunable
column equality). How do we make the filter prunable?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Keep UDF at read (status quo) | no schema change; raw storage stays canonical | full scan + per-row UDF on every metric query; cost grows with data |
| B. Reverse-map query name → raw `name` equality at read | no schema change | **impossible**: `prom_metric_name` is many-to-one/lossy (non-alnum→`_`, conditional unit/`_total`) — no unique inverse |
| C. Materialize `prom_name` column at write; filter the column at read | prunable equality + leverages name-sort (0.1 s class); deterministic (inputs known at write) | +1 column on metric files; codec↔catalog schema change; legacy-file migration |

## Decision

**Option C.** Compute `prom_metric_name` once per series at write time and store
it as a `prom_name` column. The inputs (`name`, `unit`, `is_monotonic`) are all
present at encode time, so the value is deterministic. Read filters compare the
column directly, restoring row-group pruning. Raw OTLP columns are kept for
fidelity (the "normalized view over raw storage" intent is preserved — the view
is now a stored column, not a per-row computation).

Option B is rejected as infeasible (no inverse). Option A is the problem.

## Consequences

- Selective metric queries prune row groups instead of full-scanning (NFR1).
- The metric schema gains a column; codec and catalog schemas must both change
  (see [normalizer-canonical-location](./normalizer-canonical-location.md) and
  the schema-contract NFR).
- Legacy files lack the column → handled by
  [legacy-file-migration](./legacy-file-migration.md).
- The UDF is retained (fallback + ad-hoc SQL), not removed.
