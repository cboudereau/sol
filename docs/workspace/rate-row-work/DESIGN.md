# rate-row-work — Design Doc

Amends: [promql-plan-cache](../../20260717_promql-plan-cache/designs/promql-plan-cache.md) and [write-side-small-files](../../20260720_write-side-small-files/README.md) — the final follow-up owning their twice-inherited latency NFRs, now that file count (write-side-small-files: 237 → ~45 in-window) and plan cost (promql-plan-cache: optimize stage eliminated) are handled and the residual is row-level execution.

## Context

Live stage means after the two predecessors: **execute ~835 ms / physical ~122 ms / optimize ~11 ms** for a cold `rate()` range query — execution-bound. Two row-level costs, both explorer-confirmed at HEAD:

1. **`rate()` lowering** (`src/querier/plan/frame.rs:183-302`) builds a LAG + **six** RANGE-frame window passes (`SUM(delta)`, `FIRST_VALUE(delta)`, `FIRST_VALUE(v)`, `MIN(t)`, `MAX(t)`, `COUNT(v)`) over each series partition. Several are redundant on an ASC-time-ordered frame: `duration_to_end` is provably 0 (frame ends at CURRENT ROW, `frame.rs:258`) so the terms it feeds drop arithmetically; `MIN(t)`/`MAX(t)` are the frame's first/last row; both FIRST_VALUE passes read the same leading row.
2. **`prom_series_key` per-row UDF** (`src/querier/udf.rs:97-131`) forms the window PARTITION BY (`prom_part`, `prometheus.rs:257-260`) by sorting+escaping the `attributes` MAP **once per scanned row**, re-evaluated in every plan node that references it (LAG + 6 windows + any outer aggregate). It also drives sum-by/topk/`*_over_time` grouping and the rollup downsample (`rollup.rs:147-149`).

The scan declares **no output ordering** (`catalog.rs:314-323`, no `with_file_sort_order`), so every window pays a `SortExec`; the on-disk sort is `(service_name, prom_name, time)` — no series-key component.

## Functional Requirements

### <a id="fr1"></a>FR1 — Reduce the `rate()` frame to the minimal window set
Fuse/drop redundant window passes in `frame.rs::rate` so the extrapolatedRate result is bit-identical (within the existing 1e-6 golden tolerance) with fewer passes: drop the `duration_to_end`-derived arithmetic (it is 0); compute the leading-row `delta`/`v` and the frame `min_t`/`max_t` from as few passes as DataFusion 53 allows (FIRST_VALUE/LAST_VALUE over the ordered frame rather than separate MIN/MAX aggregate-as-window passes). Independent of any schema change; no store wipe. Gated by the golden + instant==range parity suite.

### <a id="fr2"></a>FR2 — Stored `prom_series_key` column (clean cutover)
Materialise `prom_series_key` as a stored metric-schema column computed at write time from the datapoint attributes (mirroring the existing write-time `prom_name`, `parquet.rs:2000-2006`), added to both the codec write schema and `metric_union_schema()`. Window/aggregate/rollup paths partition on the plain column instead of the per-row UDF. Standing directive: clean cutover + store wipe, no dual-format read path. Logs/traces schemas untouched (separate schemas).

### <a id="fr3"></a>FR3 — Sort-pushdown to elide the window SortExec
Extend the metric write sort to `(service_name, prom_name, prom_series_key, time_unix_nano)` and declare it via `with_file_sort_order` on the metric `ListingOptions` so DataFusion elides the per-window `SortExec` for the common partition (`[name, service_name, series_key]` ORDER BY time). Depends on FR2 (the column must exist and be a sort-key prefix).

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Cold repeated-shape `rate()` ≤ 80 ms (demo scale), twice-inherited
Measured live on the rebuilt demo; the plan cache already delivers the warm path, so this targets the cold/execution component.

### <a id="nfr2"></a>NFR2 — 20-query dashboard burst ≤ 0.5 s wall (cold), twice-inherited

### <a id="nfr3"></a>NFR3 — No correctness regression
`querier::` suite green (expected baseline 261/0/2 — re-verify at Phase 4c); the frame.rs goldens (`test_rate_is_windowed_average_over_the_range`, `test_increase_is_windowed_sum_without_dividing`, `test_rate_extrapolates_to_window_edges`, `test_rate_is_smooth_across_grid`) and end-to-end goldens/parity (`test_rate_matches_prometheus_golden`, `test_instant_rate_matches_range_rate`, `test_instant_increase_matches_range_increase`, the multiseries sum-rate parity tests) hold bit-for-bit within 1e-6; Sol↔Mimir live rate parity unchanged.

## Non-goals

- **irate / quantile / stddev / stdvar_over_time**: untouched (irreducibly raw, per the rollup-read-routing scope).
- **In-memory recent-samples buffer**: still the rejected architecture change; if FR1+FR2+FR3 miss NFR1, that is the escalation, not this workspace.
- **Parquet retro-compat**: standing directive — FR2/FR3 ship as clean cutover + wipe.

## Rabbit holes

- **DataFusion window fusion limits**: DF 53 may not let two FIRST_VALUE columns share one window plan node, or may not expose LAST_VALUE cheaply. Cap: FR1's win is whatever the golden-gated fusion allows; if fusion saves < ~1 window pass, record it and lean on FR2/FR3. Do not hand-roll a custom window UDWF to force fusion.
- **Sort-order declaration correctness**: a wrongly-declared `with_file_sort_order` produces silently wrong results (DataFusion trusts it). Cap: FR3 asserts the declared order exactly matches the codec write sort in a test, and the reads-each-datum-once + parity suites gate it.
- **Series-key escaping vs sort collation**: the UDF's sorted+escaped string must sort consistently as a stored column. Cap: reuse the exact `series_key_string` output as the column value; no separate collation.

## Design

FR1 is a self-contained rewrite of `frame.rs::rate`, ships first (no wipe), re-profiled before committing to the heavier work. FR2 adds the stored column at the write-compute sites + both schemas; FR3 extends the write sort and declares the ordering. FR2+FR3 are a single clean-cutover bundle (both need the wipe; FR3 needs FR2's column). Plan cache and rollup interplay are benign (schema change bumps inventory generation → stale plans dropped; rollup gains the same plain-column grouping).

Decisions:
- [rate() frame reduction](./adrs/rate-frame-reduction.md) — which windows fuse/drop and why safe.
- [Stored series-key column + write-sort pushdown](./adrs/series-key-column.md) — schema/sort cutover.

## Cross-cutting Concerns

- **Observability**: reuse the `sol_querier_plan_stage_duration_seconds` seam — re-profile after FR1 and after FR2+FR3 to attribute the execute-stage drop.
- **Rollback**: FR1 is a revertable commit; FR2/FR3 are a schema cutover (revert = revert commit + wipe).
- **Verification**: re-profile bench (fixture) + live probe set from the two predecessor VERIFYs after the user rebuild (FR1) and rebuild+wipe (FR2/FR3).
