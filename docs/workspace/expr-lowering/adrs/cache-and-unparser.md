---
status: draft
---
# Plan-based cache keying + `Expr` unparser reuse

Addresses: [FR4](../DESIGN.md#fr4), [FR6](../DESIGN.md#fr6), [NFR2](../DESIGN.md#nfr2)

## Problem

Two coupled questions raised by targeting the logical layer:
1. `QueryEngine`'s cache keys by **SQL text** (`CacheKey::for_sql`). A DataFrame/plan
   path has no SQL string — how is it cached so equal queries still hit?
2. The shared predicate builder produces `Expr`. The SQL-staying paths
   ([hybrid-boundary](./hybrid-boundary.md)) need SQL text. Can one builder serve both
   (via `datafusion::sql::unparser`), or do those paths keep hand-written WHERE?

## Options

**Cache key for plans**

| Option | Pros | Cons |
|---|---|---|
| Key on the **optimized `LogicalPlan` display** (`plan.display_indent()` string) | Deterministic; reflects the actual executed plan; reuses existing string `CacheKey` | Display format could change across DF upgrades (acceptable — cache is best-effort) |
| Structured `CacheKey` per query shape | Explicit | Per-endpoint boilerplate; easy to under-specify and collide |

**Predicate reuse for SQL-staying paths**

| Option | Pros | Cons |
|---|---|---|
| Build `Expr`, `Unparser::expr_to_sql` for the WHERE of SQL paths | One predicate source of truth across all signals | Depends on unparser fidelity (UDF calls, anchored regex) |
| Keep SQL-staying WHERE hand-written | No unparser dependency | Some predicate logic duplicated between the two styles |

## Decision

1. **Cache plan-based queries on the optimized `LogicalPlan`'s indented display
   string**, reusing the existing string-keyed cache + 15s bucketing. Cache stays
   best-effort (a key miss only costs a recompute), so display-format drift is
   tolerable.
2. **Prefer `Unparser` reuse** of the shared `Expr` predicates for SQL-staying paths,
   **but gated on the pilot slice**: if a predicate doesn't round-trip to the
   expected SQL (UDF call / `^(?:…)$` regex), keep that path's WHERE hand-written and
   reuse the builder only in DataFrame paths (per the rabbit-hole cap in the design).

## Consequences

**Easier**: one cache implementation for both paths; potentially one predicate source
of truth across migrated + SQL queries.

**Harder**: a plan must be optimized (or at least built) before its cache key exists —
a small reordering vs the SQL path; unparser reuse may prove partial, leaving some
duplication that the design's coverage note must record.
