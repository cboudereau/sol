---
status: superseded-by docs/20260615_promql-pushdown/adrs/2026-06-15_aggregation-pushdown.md
---
# PromQL aggregate evaluation: in-memory composition vs. pushed-down SQL

Addresses: [FR1](../../DESIGN.md#fr1) (Prometheus-compatible API), [FR4](../../DESIGN.md#fr4) (DataFusion engine), [NFR5](../../DESIGN.md#nfr5) (memory budget), [NFR6](../../DESIGN.md#nfr6) (latency). Refines the [expr-lowering design](../../../../20260608_expr-lowering/designs/20260608_expr-lowering.md) (PromQL → `Expr`/`DataFrame`, no `format!` SQL).

## Problem

After expr-lowering, PromQL aggregations were lowered one level deep: `<agg>(selector)` and `<agg>(range_fn)` became a single DataFusion `DataFrame.aggregate(by_columns, [agg_expr])`. That single-level, `by`-only shape cannot express several constructs the real Node Exporter dashboard uses (and the pcap workload confirms: `count` ×194, `sum`/`sum by` ×84, `clamp_min` ×4):

- **Nested aggregates** — `count(count(node_cpu_seconds_total{…}) by (cpu))` (CPU Cores panel): the inner aggregate is not a selector, so the outer aggregate had nothing to lower.
- **`without(…)` grouping** — `sum without(mode) (rate(…))` (CPU Basic panel): grouping by *all labels except* a set is a complement over the exploded label map; the SQL path only projects an explicit `by` column list, and the labels live inside the `attributes` JSON, not as columns.
- **`clamp_min`/`clamp_max`** (RAM Used panel) and **`scalar()` in range queries** (CPU panel): element-wise functions / constant folding with no single-level SQL form.

How should aggregates (and these functions) be evaluated so arbitrary PromQL composition works, without abandoning DataFusion's strengths for the expensive leaf scans?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Push aggregation into DataFusion SQL/DataFrame (status quo, single level) | Aggregation runs in the columnar engine: vectorised, multi-threaded, can spill; scales to high cardinality. | Cannot nest; `without` is awkward (labels are JSON, not columns); no `clamp`/`scalar` composition. Fails the dashboard. |
| B. Evaluate aggregates compositionally **in Rust** over the inner result (chosen) | Arbitrary nesting; `by`/`without` uniform over the exploded `BTreeMap` label set; `clamp`/`scalar` trivial; matches PromQL semantics exactly. Reuses the existing recursive `eval_instant` / `eval_range_window`. | Materialises the inner result set in querier memory before reducing; single-threaded reduce; no spill. Cost grows with **series cardinality × points**. |
| C. Recursive SQL generation (emit nested `GROUP BY` subqueries) | Stays in-engine. | Re-introduces `format!`-built SQL (violates the no-SQL-in-core invariant); `without` over JSON labels still needs per-label extraction; high complexity for marginal benefit at the dashboard's scale. |

## Decision

**Option B — evaluate aggregates and the `clamp`/`scalar` functions compositionally in Rust, in both the instant and range evaluators.**

The evaluators are already recursive and Rust-native for arithmetic, unary, and paren nodes; only aggregates were pushed to SQL. The fix makes aggregates symmetric:

- The **leaf** (vector selector, `rate`/`*_over_time`) still lowers to DataFusion — predicate pushdown, `prom_name` pruning, and the heavy scan stay in the engine.
- The **aggregate** evaluates its inner expression to an in-memory value — `Vec<(labels, value)>` (instant) or `RangeSeries` (range) — then groups and reduces in Rust. Grouping uses `AggGrouping` (`by` keeps the listed labels; `without` keeps all except those and `__name__`; none collapses to one group) over the **exploded** label map (`LabelCols::labels`), so `without` works even though source labels live in the `attributes` JSON.
- `clamp_min`/`clamp_max` map element-wise; `scalar()` folds a one-element vector to its value (NaN otherwise), and in a range query folds a non-range inner to a constant by evaluating it as an instant at the window end.
- The range aggregate arm **yields to** the bucket-heatmap / histogram-quantile detectors, whose `sum by (le) …` shape is also a simple aggregate.

This keeps the no-SQL-in-core invariant (no `format!` SQL) and is exact w.r.t. PromQL grouping semantics.

## Consequences

### High-cardinality trade-off (the key risk)

The inner result is **fully materialised in querier memory before the reduce**. For the target workload this is negligible — Node Exporter panels aggregate dozens to a few hundred series — but it is **unbounded in the cardinality of the inner expression**:

- Memory and time are `O(series × points_in_window)` for the materialised inner set, plus the grouping map. A high-cardinality inner selector (e.g. a metric with millions of series, or a wide label set over a long range) materialises a large structure in the querier and reduces single-threaded → latency spikes and, in the worst case, querier OOM. This is the cost of moving aggregation out of the columnar engine (Option A would have spilled/parallelised it).
- The reduce does not stream or spill; it holds the whole inner set at once.

### What bounds it today

- The materialised set is the **post-filter** series, not raw rows: the leaf selector runs in DataFusion with predicate pushdown + `prom_name` column pruning, so only matching series/points are collected.
- **[NFR9](../../DESIGN.md#nfr9) guardrails** (`max_bytes_scanned`, per-signal `max_range`, `max_concurrent_queries`) cap the leaf scan and concurrency → an indirect bound on what an aggregate can materialise (a query that would scan too much is rejected 422 before the reduce).
- The **15s result cache** ([caching ADR](./query-caching-strategy.md)) amortises repeated dashboard aggregates.
- The dashboard's flagship metrics are low-cardinality (host-scoped node metrics); `histogram_quantile` and bucket heatmaps keep their dedicated Rust-native paths (not this generic reduce).

### Known limitation & future escape

This is recorded as a **known limitation**, not a regression: high-cardinality aggregation can pressure querier memory ([NFR5](../../DESIGN.md#nfr5)). If it becomes a bottleneck, the escape is a **hybrid**: push the *first* (innermost, leaf-level) aggregation back into DataFusion's `aggregate` for the simple `by`-over-selector case (vectorised, spillable), and keep the Rust composition only for the outer levels (`without`, nesting, `clamp`/`scalar`). A streaming/chunked reduce is a smaller follow-up. Neither is needed at the dashboard's scale, so both are deferred.

### Correctness note

Range aggregation reduces per timestamp across the series in a group. Series in one group share the step grid because they originate from a single range query and are resampled onto the grid afterwards (`resample_to_grid`), so per-timestamp alignment is exact; timestamps are keyed in nanoseconds for stable ordering.
