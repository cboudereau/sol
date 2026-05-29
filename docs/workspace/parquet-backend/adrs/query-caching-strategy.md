---
status: draft
---
# Query caching strategy

Addresses: [FR5](../DESIGN.md#fr5), [NFR3](../DESIGN.md#nfr3)

## Problem

The pcap analysis shows ~130 queries per Grafana dashboard refresh, with refreshes every ~15 seconds. Many queries are identical across refreshes (same PromQL, slightly shifted time window). Without caching, every refresh re-executes all queries against Parquet files — expensive for histogram quantile and rate computations.

How should query results be cached?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. In-memory LRU cache (no external dependency) | Zero operational overhead. Fast lookup. No network latency. Embedded in Sol process. | Not shared across query nodes. Memory-bounded. Lost on restart. |
| B. Redis / Memcached external cache | Shared across nodes. Survives restarts. Proven at scale (Grafana Mimir uses memcached). | External dependency. Network latency per cache lookup. Operational burden. |
| C. No caching — rely on DataFusion's built-in optimizations | Simplest. No stale data risk. | Cannot meet NFR3 latency target for histogram quantile queries. Dashboard experience degrades. |
| D. Hybrid: in-memory LRU default, optional Redis backend | Best of A and B. Start simple, scale when needed. | More code to maintain (two cache backends). |

## Decision

**Option D — Hybrid: in-memory LRU default, optional Redis backend behind a trait.**

Rationale:
- **Start with in-memory**: zero external dependencies. For single-node deployments (the initial target, per non-goals), in-memory LRU is sufficient and fast.
- **Cache behind a trait**: `QueryCache` trait with `get(key) → Option<CachedResult>` and `put(key, result, ttl)`. Default implementation: in-memory LRU (e.g., `lru` crate or `moka` for concurrent access). Redis implementation can be added later without changing the query path.
- **Time-range bucketing**: round query time ranges to the nearest 15-second boundary before hashing. This ensures that two dashboard refreshes 15s apart produce the same cache key for the same query, even though the exact start/end timestamps differ by a few seconds.

Cache key: `hash(query_string, floor(start / 15s), floor(end / 15s))`

> **Amended for long ranges** (see [long-range-metrics-strategy](./long-range-metrics-strategy.md)): whole-range 15s bucketing misses on every refresh once the range is long (the `end` always moves). For metric `query_range`, caching is applied **per time-split shard** ([FR8](../DESIGN.md#fr8)): completed historical shards are immutable and cached permanently; only the in-progress shard is uncacheable. The whole-range key above remains correct for short, non-split queries (traces/logs, instant). For multi-node deployments the cache moves behind a **shared** backend (Redis / object-store) owned by the query-frontend ([deployment-roles ADR](./deployment-roles-and-read-scaling.md)); the per-process LRU stays the single-node default.

Cache behavior:
- TTL: 15 seconds (one dashboard refresh cycle)
- Max entries: 1000 (configurable)
- Eviction: LRU when capacity is reached
- No active invalidation — TTL expiry only

Expected hit rate: for the pcap dashboard pattern (same queries repeating every 15s), the second refresh should be ~100% cache hits. Cache miss rate depends on time-range bucketing alignment — worst case, a query that straddles a 15s boundary misses once.

## Consequences

- Add `moka` (concurrent LRU cache) as a dependency behind the `query-backend` feature flag. It is already production-proven (used by GreptimeDB for similar caching).
- Cache is per-process. In a multi-node deployment, each node has its own cache (acceptable for v1; Redis backend addresses this later).
- Stale data risk: cached results may be up to 15s stale. For dashboard use cases (which already have a 15s refresh interval), this is imperceptible. For alerting use cases (out of scope per non-goals), caching should be bypassed.
- Memory overhead: 1000 cached results × ~10KB average = ~10MB. Negligible.
