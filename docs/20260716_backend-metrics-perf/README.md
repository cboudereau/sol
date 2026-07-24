# backend-metrics-perf — querier per-query fixed-cost elimination

Eliminates the querier's per-query fixed costs surfaced by live analysis of the demo RED dashboard (every query opened all ~1,529 Parquet footers; the result cache was wiped every 15 s; no request coalescing; metadata endpoints scanned all history; `max_concurrent_queries` was parsed but unenforced).

## Delivered (9 tasks, 3 sessions; commits `cc88c6ba7`…`b92b2e624`)

- **FR1 — time-scoped file listing** ([ADR](./adrs/per-query-file-pruning.md), option A′): the gateway names each Parquet file with its exact `[min,max]` event-time bounds (layout break + demo store wipe); the engine retains a `FileInventory` from the refresh walk and windowed queries scan an ephemeral `ListingTable` over only the overlapping files. A 15-min query opens the in-window files only (test: 3→1; live bare-range 58 ms).
- **FR2 — cache survives refresh** ([ADR](./adrs/cache-invalidation-scope.md)): `refresh()` no longer clears; per-entry TTL via moka `Expiry` (sealed windows 15 min, mutable 15 s). Warm hits (5 ms) are now sustained.
- **FR3 — single-flight** (same ADR): hand-rolled async coalescing keyed by `CacheKey`; leader/followers via `watch`; errors never cached; RAII in-flight cleanup.
- **FR4 — bounded metadata**: `/labels`, `/label/:name/values`, `/series` default to `now − metadata_default_range_secs` (3 days) when `start` is absent; `/labels` now routes through the `resolve_metric_windows` choke point. Live: labels 70 ms, series 113 ms.
- **FR5 — `max_concurrent_queries` enforced** ([ADR](./adrs/concurrency-guardrail.md)): engine semaphore inside the single-flight leader, bounded wait → 503 + `Retry-After`; `sol_querier_shed_total`.

Design doc: [designs/backend-metrics-perf.md](./designs/backend-metrics-perf.md) (FR/NFR anchors, priority mapping to the original 7-item recommendation).

## Live verification & the honest miss

[VERIFY.md](./VERIFY.md) — before/after on the rebuilt demo (`sol:401e8eb90`, wiped store). FR-level wins verified; **NFR1 (cold `rate()` ≤ 50 ms) and NFR2 (burst ≤ 0.5 s) missed**: the residual ~190 ms/query is the `rate()` window-function **plan** constant (measured decomposition: bare range 58 ms vs `rate()` ~250 ms, flat in window width), which this design declared a non-goal with a revisit trigger. **The trigger fired** → the follow-up workspace [promql-plan-cache](../workspace/promql-plan-cache/TASKS.md) inherits both NFRs (levers: plan-stage reuse, instant staleness lookback, margin cleanup).

Also fixed en route: `sol_querier_files_opened` now reports real file counts (was a partition proxy); scoped scans are named so plan-display cache keys can't collide between tier and raw plans.
