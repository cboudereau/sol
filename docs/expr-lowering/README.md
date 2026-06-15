# expr-lowering

Sol's query backend lowers three parsed query languages (PromQL, LogQL, TraceQL) to SQL **text** built with `format!`, then hands the string to `ctx.sql(query)` (`QueryEngine::sql`), which DataFusion *re-parses* into a `LogicalPlan`. Today there are ~110 `format!` SQL-build sites (prometheus 54, loki 34, tempo 22, sql 2), each signal re-implementing the same *label/field `op` value → predicate* sh

## Design
- [20260608_expr-lowering](./designs/20260608_expr-lowering.md)

## ADRs
- [20260608_canonical-nanoseconds](./adrs/20260608_canonical-nanoseconds.md) — Canonical nanosecond units; convert only at the boundary
- [20260608_lowering-target](./adrs/20260608_lowering-target.md) — Lowering target: DataFusion `Expr` / `LogicalPlan` via the DataFrame API
- [20260608_migration-scope](./adrs/20260608_migration-scope.md) — Migration scope: full `Expr` migration (window primitives included)
- [20260608_plan-cache-keying](./adrs/20260608_plan-cache-keying.md) — Plan-based query-cache keying
