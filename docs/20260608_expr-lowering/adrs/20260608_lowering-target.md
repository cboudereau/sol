---
status: accepted
---
# Lowering target: DataFusion `Expr` / `LogicalPlan` via the DataFrame API

Addresses: [FR1](../designs/20260608_expr-lowering.md#fr1), [FR2](../designs/20260608_expr-lowering.md#fr2), [FR3](../designs/20260608_expr-lowering.md#fr3), [NFR1](../designs/20260608_expr-lowering.md#nfr1)

## Problem

The three signal lowerings build SQL as `format!` strings, which DataFusion then
re-parses. We want a shared, type-safe, injection-safe lowering target. What should
that target be?

## Options

| Option | Pros | Cons |
|---|---|---|
| **DataFusion `Expr`/`LogicalPlan`** (DataFrame / `LogicalPlanBuilder`) | The engine's native IR — SQL itself compiles to it; values are `lit()` (no injection); type-checked; skips parse; reuses registered UDFs as `Expr::ScalarFunction`; already in-tree | Window functions verbose; cache is SQL-text keyed (needs a plan key); a real refactor of tested code |
| Typed SQL builder (`sea-query`, `sql-builder`) | Type-ish assembly of SQL text | Still produces text that gets re-parsed; new dependency; dialect-targeted, not DataFusion-aware; no UDF/plan integration |
| Build `sqlparser` AST + unparse | Reuses DataFusion's parser AST | Round-trips to text anyway; awkward to assemble; no type/plan benefit over `Expr` |
| Keep `format!` strings | Zero work; familiar | The status quo: duplication across signals, injection surface, no type-safety |

## Decision

**Target the DataFusion logical layer directly: build `Expr` predicates with a
shared builder and assemble plans with the `DataFrame` API** for the migrated
(non-window) queries. SQL is a front-end over this same `LogicalPlan`; building it
programmatically is the native form, makes values literals (NFR9), and lets the
three signals share one predicate library. No new dependency.

This is `draft` until the Phase 4c pre-flight; reversible before implementation in
favour of keeping strings if the pilot slice shows the DataFrame ergonomics or
cache-keying cost outweigh the benefit.

## Consequences

**Easier**: cross-signal predicate reuse; structural injection-safety (drop `esc()`
on migrated paths); type-checked plans; one fewer parse per request.

**Harder**: window-function queries are more verbose in the API (built once as the
shared `plan::frame` primitives — see the [migration-scope ADR](20260608_migration-scope.md));
the query cache needs a plan-based key
(see [plan-cache-keying ADR](20260608_plan-cache-keying.md)); SQL-text test assertions
must become plan/result assertions.
