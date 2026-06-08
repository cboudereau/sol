---
status: draft
---
# Plan-based query-cache keying

Addresses: [FR4](../DESIGN.md#fr4), [NFR2](../DESIGN.md#nfr2)

## Problem

`QueryEngine`'s cache keys by **SQL text** (`CacheKey::for_sql`). With full migration
([migration-scope](./migration-scope.md)) queries are `LogicalPlan`s, not strings, so
the cache needs a stable key derived from the plan. (No SQL-staying paths remain, so
there is no `Unparser`/round-trip concern — that earlier coupling is dropped.)

## Options

| Option | Pros | Cons |
|---|---|---|
| Key on the **optimized `LogicalPlan` display** (`display_indent()` string) | Deterministic; reflects the executed plan; reuses the existing string-keyed cache + 15s bucketing | Display format may drift across DataFusion upgrades (tolerable — cache is best-effort) |
| Structured `CacheKey` per query shape | Explicit | Per-endpoint boilerplate; easy to under-specify → collide |
| Don't cache plan queries | Trivial | Loses the FR5/NFR6 cache benefit |

## Decision

**Key plan-based queries on the optimized `LogicalPlan`'s indented display string**,
reusing the existing string-keyed moka cache and 15s time-bucketing. The cache is
best-effort (a key miss only recomputes), so display-format drift across DataFusion
versions is acceptable. `QueryEngine::collect(plan)` mirrors `sql()` (same cache +
`record_cache`/`set_cache_memory` telemetry).

## Consequences

**Easier**: one cache implementation for both `sql()` (user endpoint) and
`collect()`; equal plans hit; telemetry unchanged.

**Harder**: the plan must be built/optimized before its key exists — a minor
reordering vs hashing SQL text up front.
