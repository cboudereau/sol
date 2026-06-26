---
status: proposed
---
# Rollup aggregate schema

Addresses: [FR6](../DESIGN.md#fr6), [FR7](../DESIGN.md#fr7), [NFR1](../DESIGN.md#nfr1), [NFR2](../DESIGN.md#nfr2)

## Problem
[FR6](../DESIGN.md#fr6) needs the rollup to carry per-bucket `{last, min, max, sum, count}` for the scalar metric value. Today `rollup_plan` (`rollup.rs:117`) emits `last_value(col ORDER BY time)` for **every** column, and the tier tables are registered over the **same** `metric_union_schema` as raw (`catalog.rs:248`). How do we add the aggregate columns without forking the schema (which would break the shared `ListingTable` registration and the supersession reads) or double-counting?

## Options
| Option | Pros | Cons |
|---|---|---|
| A. Separate tier schema (own columns) | Clean tier shape | Forks the catalog: tier and raw can't share registration helpers; cross-window merge (tier+raw in one query) must reconcile two schemas |
| B. Add 4 nullable aggregate columns to the **shared** `metric_union_schema`; raw files null them (adapter), tier files populate them | One schema everywhere; raw/tier merge trivially; existing supersession + registration unchanged; clean cutover (old tier files null them → fall back to raw for those ops) | 4 extra always-null columns on raw files (negligible — Parquet RLE-nulls them to ~0 bytes) |
| C. Reuse the existing histogram `min`/`max`/`sum`/`count` columns | No new columns | **Wrong**: those are the OTLP histogram/summary fields (per-point), not per-bucket aggregates of the gauge value — overloading them corrupts histogram reads |

## Decision
**Option B.** Add four nullable `Float64` columns to `metric_union_schema` — `value_min`, `value_max`, `value_sum`, `value_count` — computed over the **coalesced scalar value** (the same `metric_value_expr` coalesce the read path uses: `double_value` → `int_value` → …). `rollup_plan` emits them via `min/max/sum/count(value)` aggregates grouped by the existing `(name, service_name, series_key, bucket)`, alongside the unchanged `last_value(...)` columns (which still back `rate`/`histogram_quantile`). Raw files leave them null (schema adapter fills null); the main `metrics` union and supersession reads are unchanged (the columns are just present-and-null on raw).

`value_count` counts raw samples per bucket (for `count_over_time` = `Σ value_count` and `avg` = `Σ value_sum / Σ value_count`). The read path (FR7) reads these only on tier windows; raw windows ignore them and compute over `v` as today.

## Consequences
- **One schema, no fork**: tier and raw remain registerable/mergeable through the same path; NFR2 holds (no new deps, plain Arrow columns).
- **Clean cutover, no migration**: per the project's no-retro-compat-for-Parquet rule the store starts empty, so every tier file carries the columns — a tier unconditionally advertises `{Last,MinMax,SumCount}`, no per-file capability probing, no retro-compat shim.
- **Storage**: only the value column multiplies (~5 scalars/bucket vs 1); series-key/attribute columns dominate file size and don't grow. Measured baseline: rollup-5m is 9% of raw — rich rollup is estimated ~15–22% (still a 5–7× read reduction). Confirm on the live store before sealing the estimate.
- **Histograms unaffected**: they keep last-snapshot `bucket_counts` (capability `Last`); the new columns are null for histogram rows.
- A rollup-parity test must assert `min/max/sum/count/avg_over_time` over the tier equal the raw computation on a multi-sample-per-bucket fixture.
