---
status: draft
supersedes: ../../parquet-backend/adrs/querier/promql-aggregate-evaluation.md
---
# Aggregation pushdown — relational core in DataFusion

Addresses: [FR2](../DESIGN.md#fr2), [NFR3](../DESIGN.md#nfr3), [NFR4](../DESIGN.md#nfr4)

**Supersedes** [promql-aggregate-evaluation](../../parquet-backend/adrs/querier/promql-aggregate-evaluation.md) (in-memory Rust composition → pushed-down plan).

## Problem

The superseded ADR evaluated `by`/`without`/nested aggregation **in Rust** because single-level `DataFrame.aggregate` couldn't express them (the grouping label set lives in the `attributes` JSON, and you can't `GROUP BY "all columns except mode"`). That fixed correctness but materializes every series×point in querier RAM and reduces single-threaded with **no spill** ([NFR3](../DESIGN.md#nfr3)), and the hand-rolled loop inherits none of DataFusion's vectorised/parallel optimizations. How do we express `by`/`without`/nested aggregation as DataFusion plans instead?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Keep Rust in-memory composition (status quo) | Simple; exact PromQL semantics; already green | Unbounded querier RAM at high cardinality; single-threaded; no spill; frozen vs DataFusion optims |
| B. **`GROUP BY prom_group_key(...)` column + chained `.aggregate()`** | Native vectorised/parallel/spillable hash aggregate; `by`+`without`+nested all expressible; inherits future DataFusion optims; stays on `Expr`/`DataFrame` (no SQL) | Needs the group-key UDF ([group-key-format](./group-key-format.md)); recursive lowerer must return a canonical-schema `DataFrame`; the UDF still parses JSON per row until [materialized columns](./materialized-label-columns.md) |
| C. Recursive `format!` SQL with nested `GROUP BY` subqueries | Stays in-engine | Violates the no-SQL-in-core invariant ([NFR4](../DESIGN.md#nfr4)); `without` over JSON still unsolved; high complexity |

## Decision

**Option B.** A scalar UDF computes a canonical group-key string per row ([group-key-format](./group-key-format.md)); the evaluator lowers `sum/min/max/avg/count` to `df.aggregate([prom_group_key(...)], [agg(v)])`. Nested aggregation is **chained `.aggregate()`** — the recursive lowerer returns a `DataFrame` with the canonical schema (`prom_group_key`-or-label columns + `v` + `time_unix_nano`), so an aggregate's inner is any sub-plan. `topk`/`bottomk` lower to a `ROW_NUMBER() OVER (PARTITION BY ts ORDER BY v) <= k` window filter. The Rust helpers `aggregate_instant_vector`, `aggregate_range_series`, `AggGrouping`, `agg_reduce` are deleted. Result labels are recovered by parsing the group-key **once per output group**.

## Consequences

- **Easier:** high-cardinality aggregation is bounded + parallel + spillable (NFR3); every DataFusion release improves it for free; one code path for `by`/`without`/nested.
- **Harder / risk:** the parity tests (NFR2) are the contract — `count(count(…) by (cpu))`, `sum without(mode)`, `topk` must produce identical results through the plan. The group-key format is now load-bearing and frozen ([group-key-format](./group-key-format.md)). Until [materialized columns](./materialized-label-columns.md), the UDF still parses JSON per row (but grouping is native and result parse is per-group, so the [NFR3](../DESIGN.md#nfr3) win lands immediately).
- Aggregation whose inner can't yield the canonical schema falls to the Rust shell ([relational-nonrelational-boundary](./relational-nonrelational-boundary.md)), not forced into the plan.
