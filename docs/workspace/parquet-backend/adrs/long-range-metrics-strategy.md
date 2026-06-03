---
status: draft
---
# Long-range metrics: time-split + rollups + time-partitioned layout

Addresses: [FR8](../DESIGN.md#fr8), [FR6](../DESIGN.md#fr6), [NFR7](../DESIGN.md#nfr7), [NFR6](../DESIGN.md#nfr6)

## Problem

Metrics are queried over **13 months by default, up to 2 years opt-in** ([NFR7](../DESIGN.md#nfr7)), and Grafana re-issues the same range every ~15s. Two failure modes:
1. A long-range (13mo–2y) scan at raw resolution is infeasible (cost, memory) — even with pruning.
2. The result cache keyed on the whole range ([caching ADR](./query-caching-strategy.md)) **misses on every refresh**, because `end` advances each time.

Traces and logs (≤30d) do not have this problem — short windows. So the strategy is metrics-specific.

## Options

| Lever | Without it | With it |
|---|---|---|
| **Time-partitioned layout** (`dt=YYYY-MM-DD/`) | Catalog lists/opens every file to prune | Prune whole days by path before any footer read |
| **Time-range splitting** (per-day shards) | One huge query; no cache reuse | Fan-out across queriers; historical shards cache permanently |
| **Rollups / downsampling** (5m→1h→1d) | Raw scan over 2y | Serve cold tail from coarse pre-aggregates matched to `step` |

These are complementary, not alternatives — the decision is to use all three for metrics.

## Decision

**Adopt time-partitioned layout + per-day time-splitting + resolution-tier rollups for metrics.**

1. **Layout** — metric Parquet is written under `dt=YYYY-MM-DD/` partitions so day-level pruning is a path filter ([FR7](../DESIGN.md#fr7)).

2. **Splitting** ([FR8](../DESIGN.md#fr8)) — the query-frontend splits a metric `query_range` into per-day shards aligned to UTC midnight and `step`, executes across stateless queriers ([roles ADR](./deployment-roles-and-read-scaling.md)), and merges. **Completed historical shards are cached permanently** (immutable); only the in-progress day is uncacheable. This fixes the whole-range cache defect and makes a long-range refresh ≈ 1 live shard + N cache hits (N ≈ 394 at the 13mo default, 729 at the 2y opt-in).

3. **Rollups** ([FR6](../DESIGN.md#fr6)) — the compactor produces coarser-resolution metric Parquet (e.g. 5m, 1h, 1d) for the cold tail. The query-frontend selects the tier from `(range, step)`: recent ranges → raw; long ranges → rollups. Rollups store **bucket counts** (not pre-computed quantiles) and **counter values** so `histogram_quantile` and `rate` stay correct after merge.

**Correctness rules (the hard part):**
- Range-vector functions (`rate`, `increase`) overlap each shard by the lookback/range window and stitch at boundaries — never split disjointly.
- `topk` → partial-topk per shard, then merge (not naive concat).
- `histogram_quantile` → sum bucket counts per series across shards/tiers, **then** compute the quantile (never average per-shard quantiles).
- Split/rollup boundaries align to `step` so cache keys are stable.

## Consequences

- The query-frontend ([roles ADR](./deployment-roles-and-read-scaling.md)) owns splitting + merge + tier selection; queriers stay simple (execute one SQL shard).
- Rollup generation is compactor work — ingest/compaction CPU + extra storage traded for read latency on the long tail ([NFR6](../DESIGN.md#nfr6) balance).
- Raw real-time computation remains the correctness baseline and the fallback when a rollup tier is missing or the range is recent.
- This applies to metrics only. Traces/logs are registered as day-partitioned tables **without splitting/rollups** ("plain" = no long-range machinery; they are still subject to the same `resolve_files` footer-supersession resolution post-compaction, [compaction-consistency](./compaction-consistency.md)).
- Freshness unchanged: the in-progress shard reads finalized Parquet at the flush/refresh interval; hot data stays a [non-goal](../DESIGN.md#non-goals).

## Implementation note (reconciliation with what shipped)

- Rollups are generated from the **compacted survivors** (`resolve_files`: the
  L2 daily + any non-superseded raw), **not** raw-only. This decouples rollup
  from the raw-retention/GC lifecycle — once superseded raw is reclaimed, a tier
  is still (re)buildable from the daily. (An earlier raw-only implementation went
  stale once GC deleted the raw.)
- Rollup is **single-pass and idempotent**: one `rollup-<tier>.parquet` per tier
  per sealed partition, rewritten only when the source daily is newer. It is
  **not** leveled/multi-pass — file count per tier is bounded by `retention_days`,
  so there is no small-file accumulation to compact.
- Rollups are **excluded** from `resolve_files` (the lossless union) and back
  separate `metrics_5m/1h/1d` tables; the querier routes a coarse-`step` range to
  the coarsest tier ≤ `step`, else raw.
