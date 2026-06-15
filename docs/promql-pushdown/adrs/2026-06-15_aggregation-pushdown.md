---
status: accepted
supersedes: ../../workspace/parquet-backend/adrs/querier/promql-aggregate-evaluation.md
---
# Aggregation pushdown — relational core in DataFusion

Addresses: [FR2](../designs/2026-06-15_promql-pushdown.md#fr2), [NFR3](../designs/2026-06-15_promql-pushdown.md#nfr3), [NFR4](../designs/2026-06-15_promql-pushdown.md#nfr4)

**Supersedes** [promql-aggregate-evaluation](../../workspace/parquet-backend/adrs/querier/promql-aggregate-evaluation.md) (in-memory Rust composition → pushed-down plan).

## Problem

The superseded ADR evaluated `by`/`without`/nested aggregation **in Rust** because single-level `DataFrame.aggregate` couldn't express them (the grouping label set lives in the `attributes` JSON, and you can't `GROUP BY "all columns except mode"`). That fixed correctness but materializes every series×point in querier RAM and reduces single-threaded with **no spill** ([NFR3](../designs/2026-06-15_promql-pushdown.md#nfr3)), and the hand-rolled loop inherits none of DataFusion's vectorised/parallel optimizations. How do we express `by`/`without`/nested aggregation as DataFusion plans instead?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Keep Rust in-memory composition (status quo) | Simple; exact PromQL semantics; already green | Unbounded querier RAM at high cardinality; single-threaded; no spill; frozen vs DataFusion optims |
| B. **`GROUP BY prom_group_key(...)` column + chained `.aggregate()`** | Native vectorised/parallel/spillable hash aggregate; `by`+`without`+nested all expressible; inherits future DataFusion optims; stays on `Expr`/`DataFrame` (no SQL) | Needs the group-key UDF ([group-key-format](./2026-06-15_group-key-format.md)); recursive lowerer must return a canonical-schema `DataFrame`; the UDF still parses JSON per row until [materialized columns](./2026-06-15_materialized-label-columns.md) |
| C. Recursive `format!` SQL with nested `GROUP BY` subqueries | Stays in-engine | Violates the no-SQL-in-core invariant ([NFR4](../designs/2026-06-15_promql-pushdown.md#nfr4)); `without` over JSON still unsolved; high complexity |

## Decision

**Option B.** A scalar UDF computes a canonical group-key string ([group-key-format](./2026-06-15_group-key-format.md)); the evaluator lowers `sum/min/max/avg/count` to `df.aggregate([prom_group_key(...)], [agg(v)])`. Every aggregated node emits the **uniform canonical frame** `[prom_group_key, v, (time_unix_nano)]`, so nested aggregation is **chained `.aggregate()`**:
- a **leaf** inner (selector/`rate`/`over_time`, carrying `attributes` + promoted columns) → group key via `prom_group_key(attributes, promoted, mode, labels)`;
- a **nested** inner (already carrying `prom_group_key`) → group key via `prom_group_key_reproject(inner_key, mode, labels)` (parse → re-project; the format is reversible).

This makes **mixed nesting** correct — `sum by (cpu) (sum without (mode) (m))` re-projects the inner's all-except-`mode` key down to `cpu`, which an opaque key could not recover. `topk`/`bottomk` lower to a `ROW_NUMBER() OVER (PARTITION BY ts ORDER BY v) <= k` window filter. The Rust helpers `aggregate_instant_vector`, `aggregate_range_series`, `AggGrouping`, `agg_reduce` are deleted. Result labels are recovered by parsing the group-key **once per output group**.

## Consequences

- **Easier:** high-cardinality aggregation is bounded + parallel + spillable (NFR3); every DataFusion release improves it for free; one code path for `by`/`without`/nested.
- **Harder / risk:** the parity tests (NFR2) are the contract — `count(count(…) by (cpu))`, `sum without(mode)`, `topk` must produce identical results through the plan. The group-key format is now load-bearing and frozen ([group-key-format](./2026-06-15_group-key-format.md)). Until [materialized columns](./2026-06-15_materialized-label-columns.md), the UDF still parses JSON per row (but grouping is native and result parse is per-group, so the [NFR3](../designs/2026-06-15_promql-pushdown.md#nfr3) win lands immediately).
- Aggregation whose inner can't yield the canonical schema falls to the Rust shell ([relational-nonrelational-boundary](./2026-06-15_relational-nonrelational-boundary.md)), not forced into the plan.

## Amendment — range aggregation must grid-align before reducing (Sol↔Mimir parity)

**Problem found in the live demo:** `sum(rate(m[5m]))` over **multiple series** (e.g. the 2 collector replicas) under-summed vs Mimir (2.67 vs 4.11), while `sum by(host)(...)` matched (4.38 vs 4.11) and `received` (a single gateway series) matched. Root cause: the range aggregate grouped by the inner's **raw sample `time_unix_nano`**. Series scraped at slightly different instants land in different timestamp buckets, so a per-timestamp cross-series `sum` collapses to whichever single series has a point there — `resample_to_grid` ran *after* the aggregate, too late. PromQL evaluates `rate` **per series at each step**, then aggregates, so every series contributes at every step.

**Amended decision (range case only):** before the cross-series reduce, **resample each inner series onto the step grid** (`resample_to_grid`, carry-forward within staleness), then group + reduce per **grid** timestamp. This requires threading `step_ns` into `eval_range_window`. The reduce is a small Rust step over the already-materialized (and now grid-aligned) range series — bounded by `groups × grid_points` (range eval already materializes per-series points, so no new memory floor; the [NFR3](../designs/2026-06-15_promql-pushdown.md#nfr3) bound is preserved). **Instant aggregation stays in DataFusion** (no time dimension, no alignment issue). DataFusion is not used for the range cross-series reduce because Prometheus's per-step carry-forward is an as-of/gap-fill that DF 53 can't express cleanly.

**Contract:** a multi-series `sum(rate(...))` over series with **offset** sample timestamps must equal the sum of the per-series rates at each step (== Mimir), not one series.
