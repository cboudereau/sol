---
status: accepted
---
# Cache invalidation scope + single-flight execution

Addresses: [FR2](../designs/backend-metrics-perf.md#fr2), [FR3](../designs/backend-metrics-perf.md#fr3), [NFR2](../designs/backend-metrics-perf.md#nfr2)

## Problem

`QueryEngine::refresh()` calls `self.cache.clear()` on the entire query cache every `refresh_interval_secs` (15 s in the demo) — `src/querier/catalog.rs:608-615`. Combined with `ttl_secs: 15`, a dashboard auto-refreshing at ≥ 15 s never hits the cache (measured: warm hit 4 ms vs cold 240–410 ms). Separately, the cached path is get → execute → insert (`catalog.rs:598-604`), so N concurrent identical queries all execute the plan N times (a dashboard refresh does exactly this).

Constraint discovered during analysis: the hot path keys with `CacheKey::for_sql(plan_text)` which sets `start_bucket = end_bucket = 0` (`src/querier/cache.rs:47-52`) — the key does **not** carry the query's real time window, so any window-aware policy needs the window passed alongside at insert time (callers all have it).

## Options

### FR2 — stop wiping useful entries

| Option | Pros | Cons |
|---|---|---|
| A. Drop only mutable-window entries on refresh (window recorded at insert) | Precise; sealed entries live to full TTL | Needs window plumbed to insert; moka has no cheap predicate-scan invalidation — needs a side index or `invalidate_entries_if` |
| B. Remove `clear()` entirely; rely on TTL (15 s) | One-line change; staleness bound unchanged (TTL = refresh interval = key bucket = 15 s) | Sealed-data entries still evicted at 15 s — helps dashboards only via B+D |
| C. Snapshot-generation in the key (bump per refresh) | No clearing at all | Every refresh cold-starts everything — reproduces today's problem |
| D. Per-entry TTL (moka `Expiry`): sealed-only windows get long TTL (minutes–hours), mutable windows keep 15 s; no `clear()` | Sealed shards become effectively the "permanent historical shard cache" the frontend design intended; live staleness bound unchanged | Needs window classification at insert (same plumbing as A); wrong classification could serve stale data past 15 s — classification must reuse the established `sealed_ns = now − 1 day` wall-clock rule |

### FR3 — single-flight

| Option | Pros | Cons |
|---|---|---|
| E. moka future-cache `get_with`-style coalescing keyed by `CacheKey` | Built-in, dedups execute+insert atomically | Requires the async (`future`) moka cache variant; entry API semantics to verify |
| F. Hand-rolled in-flight map (`Mutex<HashMap<CacheKey, Shared<future>>>`) in front of the existing cache | No dependency-surface change; works with current sync cache | More code to own; error propagation to all waiters must be handled |

## Decision

**B + D for FR2; F for FR3.** Verified facts that settle it:
- moka is compiled with **only the `sync` feature** (`Cargo.toml:461`, `moka::sync::Cache` in `src/querier/cache.rs:67-69`). Option E (`moka::future` `get_with`) would require enabling a new feature on a pinned dependency, and `moka::sync::Cache::get_with` blocks the executor thread inside async handlers — both strikes. So FR3 is a small hand-rolled async single-flight (F): a `Mutex<HashMap<CacheKey, …>>` of in-flight shared results in front of the existing cache, entry removed when its leader completes, errors propagated to all waiters and **not** cached.
- The cached entry points are `QueryEngine::sql`/`collect`/`sql_user` (`src/querier/catalog.rs:519-528, 566-576, 591-604`) and none receives the query's time window today — so FR2's classification needs the window plumbed in. This is the **same plumbing FR1 introduces** (`table_scoped(name, lo, hi)` callers all know their window): a single `QueryScope { lo_ns, hi_ns }` passed down once serves both file pruning and cache classification. Windowless paths (raw SQL, unbounded metadata) classify as mutable — the safe direction.

Policy: remove the blanket `clear()` from `refresh()`; entries whose window is entirely sealed (`hi < now − 1 day`, same wall-clock rule as `resolve_metric_windows`' `SEALED_OFFSET_NS`, `src/querier/prometheus.rs:2069`) get a long per-entry TTL via moka's `Expiry` (e.g. 15 min — the byte-budget weigher still bounds memory); mutable-window and unclassified entries keep the 15 s TTL. Freshness for live data is unchanged (≤ 15 s via TTL + 15 s key bucketing); sealed results survive refreshes, which is exactly [FR2](../designs/backend-metrics-perf.md#fr2)'s intent.

## Consequences

- Warm-path latency (~4 ms measured) becomes the norm for repeated dashboard queries within a bucket, and permanently for sealed shards.
- A dashboard burst executes each distinct plan once (FR3), bounding CPU at O(distinct plans).
- The `refresh()` docstring promise ("freshly discovered data visible immediately rather than after the TTL", `catalog.rs:607-609`) is relaxed to "within TTL" — same 15 s bound the key bucketing already imposes; must be updated in code docs.
- New invariant: any code path inserting into the cache must supply the entry's window classification; an unclassifiable entry defaults to the short TTL (safe direction).
