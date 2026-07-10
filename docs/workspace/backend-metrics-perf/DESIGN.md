# backend-metrics-perf — Design Doc

## Context

Live measurement on the demo (`demo/otel-sol-grafana-dotnet`, image `sol:ac28543d8`, store: 1,529 metrics Parquet files across 7 partition days, ~479 MB) shows Sol-Prometheus dashboard panels are slow because of **fixed per-query overhead multiplied by dashboard fan-out**, not data volume:

- A single cold `rate()` range query: **0.24–0.41 s regardless of range width** (5 m → 3 h nearly flat); a query for a **nonexistent metric costs 0.27 s**. Mimir: ~1.5 ms.
- `EXPLAIN ANALYZE` via `POST /api/v1/sql` proves the mechanism: for a 15-minute query, `files_ranges_pruned_statistics=1.35 K` — DataFusion opens **every file's footer at execution time**, reads its stats, and prunes ~1,350 of them. Pruning itself works (the optimiser unwraps the `CAST(time_unix_nano AS BIGINT)` predicate, `src/querier/prometheus.rs:177-183`); the cost is the O(all-files) footer reads.
- Root cause in code: `ParquetCatalog::build_providers` registers each signal table as a `ListingTable` over an explicit list of **every surviving file, all days** (`src/querier/catalog.rs:229-249`) with `.with_collect_stat(false)` (`catalog.rs:216-218`) — deliberate (EMFILE), but it defers all pruning to per-query execution-time footer reads.
- The RED dashboard fires ~20 concurrent range queries + 6 `label_values` variable queries per refresh: measured **~2.3 s wall at ~968 % CPU** (Mimir: ~9 ms). No coalescing — identical concurrent plans all execute (get → execute → insert, `catalog.rs:598-604`).
- The result cache is structurally useless for live dashboards: `refresh()` runs every `refresh_interval_secs` (15 s in the demo) and calls `cache.clear()` on the **entire** cache (`catalog.rs:608-615`), and the TTL is also 15 s. Warm-cache latency is 4 ms — that is what is being thrown away every 15 s.
- Metadata endpoints (`/label/:name/values`, `/series`, `/labels`) default to `start=0` = all history (`src/querier/routes.rs:204-209`); measured 0.57 s for `__name__` values, 0.37 s for `series`, on every dashboard load.
- `guardrails.max_concurrent_queries` is parsed (`src/config/querier.rs:80,124`) but referenced nowhere else in `src/` — a configured guardrail that is silently unimplemented.

## Functional Requirements

### <a id="fr1"></a>FR1 — Per-query time-scoped file listing
A query whose effective scan window is `[lo, hi]` (already computed by every caller: range/instant windows include the lookback extension) must hand DataFusion only the files whose path-encoded time interval can overlap `[lo − margin, hi]`, using the existing path convention (`<signal>/…/dt=YYYY-MM-DD/HH-MM-SS-<uuid>.parquet`, `compacted-hHH-…`, `compacted-<date>`, `rollup-<tier>.parquet`). Files whose names carry no parseable time bound are always included (safety default). A 15-minute query over the demo store must open footers for tens of files, not ~1,400.

### <a id="fr2"></a>FR2 — Query cache survives catalog refresh
`refresh()` must stop invalidating cache entries whose results cannot have changed. Entries covering only sealed/immutable data survive refresh; only entries touching the mutable window (the active day / trailing window) are dropped — or, equivalently, staleness is bounded by keying instead of clearing. Bounded staleness of ≤ `cache.ttl_secs` for live data remains acceptable (it is today's contract, `src/querier/cache.rs:30-52` 15 s bucketed keys).

### <a id="fr3"></a>FR3 — Single-flight execution of identical queries
Concurrent requests producing the same `CacheKey` must execute the underlying plan once; the others await the same result. Honest expected impact: coalescing keys on the whole plan, and one dashboard's panels are mostly *distinct* plans — the benefit is concurrent viewers and Grafana re-firing panels whose previous run is still in flight (the observed "Cancel" state), not an N× cut for a single viewer. Kept because it is cheap and composes with FR2.

### <a id="fr4"></a>FR4 — Time-bounded metadata endpoints
`/labels`, `/label/:name/values`, and `/series` without an explicit `start` must default to a bounded recent window (configurable; default covering the mutable window plus the sealed span served by rollup tiers — the tier routing for the sealed span already exists, `src/querier/prometheus.rs:1351-1370`) instead of `start=0` unbounded raw history. **FR4 is an FR1 enabler, not polish**: a windowless metadata query can never use FR1's scoped listing (no window → full-table fallback), so without FR4 the six dashboard-variable queries stay at the measured 0.37–0.57 s.

### <a id="fr5"></a>FR5 — `max_concurrent_queries` enforced or removed
The configured guardrail must do what it says: either enforce admission (semaphore around query execution, overload → fast 429/503 rather than collapse) or be deleted from the config schema. Decision in [ADR: concurrency guardrail](./adrs/concurrency-guardrail.md). **Lowest priority — truthfulness/robustness, not a performance win**; explicitly cuttable if scope must shrink.

## Priority (review against the original 7-item recommendation)

Ranked by measured impact on the demo goal; the plan implements 1→4 in this order and defers 5–7 with revisit triggers:

| Original item | Disposition |
|---|---|
| 1. File pruning | [FR1](#fr1) — priority 1, ~all of the fixed 250 ms. Its "compare `time_unix_nano` against a timestamp literal / unwrap the cast" sub-item is **already covered**: `EXPLAIN ANALYZE` on the live store shows the optimiser unwraps the CAST (`predicate=time_unix_nano@4 >= …`) and prunes 1.35 K file-ranges by stats — no work needed. |
| 4. Bound metadata | [FR4](#fr4) — priority 2 (FR1 enabler for variable queries). In-memory label index: deferred upgrade, bounded default first. |
| 2. Cache | [FR2](#fr2) — priority 3. Modest for 15-min auto-refresh (15 s key re-bucketing dominates); the real win is historical/7-day panels — the original 225 % CPU incident. |
| 3. Single-flight | [FR3](#fr3) — priority 4, cheap; tempered claim (see FR3). |
| 5. Stats at refresh | Deferred (non-goal). Revisit trigger: [NFR1](#nfr1) misses after FR1. |
| 6. Write-side small files | Deferred (non-goal). Revisit trigger: multi-hour raw-window latency after FR1. |
| 7. Write-side series key | Deferred (non-goal). Revisit trigger: `rate()` row-work dominates profiles after FR1. |
| In-memory recent-samples buffer | Non-goal (architecture change; demo goal does not need ~1 ms). |
| (not in the list) `max_concurrent_queries` | [FR5](#fr5) — last, cuttable; surfaced by verification (dead config). |

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Single-query fixed cost
Cold single-metric 15-minute range query on the demo-scale store (≥ 1,500 files, ≥ 7 partition days): **p95 ≤ 50 ms** (from 240–410 ms). Verified by a repeatable benchmark/test at demo scale, and live on the demo.

### <a id="nfr2"></a>NFR2 — Dashboard refresh cost
The 20-query RED-dashboard burst: **wall ≤ 500 ms** and total CPU **≤ 2 core-seconds** on demo hardware (from ~2.3 s / ~19 core-s), cold cache.

### <a id="nfr3"></a>NFR3 — No correctness regression
All existing `querier::` tests stay green; Sol↔Mimir live parity (gauges/max/avg/count, instant+range rate) unchanged; late-arriving or clock-skewed files are not silently excluded by FR1 (margin + include-on-unparseable rule); freshness contract unchanged (new files visible ≤ `refresh_interval_secs`).

## Non-goals

- **In-memory recent-samples buffer** (Mimir-style head): would close the last ~10× to Mimir's ~1 ms but is an architecture change; excluded for cost/complexity — revisit only if NFR1/NFR2 targets prove insufficient in practice.
- **Write-side changes**: materialising `prom_series_key` as a column, gateway flush cadence, and intra-day (closed-hour) compaction of the active day. These attack the same symptom (many small files / per-row UDF partitioning) from the write side; FR1 already removes the O(files) cost read-side. Deferred, not rejected — a follow-up workspace if `rate()` CPU (window plan, `src/querier/plan/frame.rs:183-241`) still dominates after this work.
- **`collect_stat(true)` / stats caching at refresh**: FR1 makes per-query footer reads O(matching files); amortising stats collection into refresh is a second-order optimisation on top. Excluded to keep scope small; note it as the next lever if NFR1 misses.
- **`rate()` physical-plan cost** (LAG + six RANGE-frame window aggregates, per-row `prom_series_key` UDF): pre-existing, correctness-critical (extrapolation parity just landed in `2d07c34e2`), and not the measured bottleneck at demo scale. Out of scope.
- **Pre-existing open items from other workspaces** (day-aligned sealed boundary, instant `histogram_quantile` dispatch, tier edge approximation): owned elsewhere.

## Rabbit holes

- **DataFusion-native partition columns** (`dt` as a Hive partition column with injected `dt` predicates): plausible alternative to explicit list filtering, but it changes table schemas and every query's predicate generation. Cap: evaluate on paper in [ADR: file pruning](./adrs/per-query-file-pruning.md); do **not** prototype both routes.
- **Late-data margin**: how far a file's path-encoded time can lie about its contents. Cap: reuse the established wall-clock margin convention from rollup-read-routing (`sealed_ns = now − 1 day` kept a safe 24 h margin); pick one margin constant, document it, and enforce the include-on-unparseable rule. No per-file footer verification pass.
- **Cache invalidation cleverness**: generational/versioned keys can grow into a GC project. Cap: staleness is already bounded at 15 s by TTL + bucketed keys; the simplest scheme that stops wiping sealed-data entries wins ([ADR: cache invalidation](./adrs/cache-invalidation-scope.md)).
- **Fairness/queueing policy for FR5**: no priority queues; a plain semaphore + immediate shed is enough for a guardrail.

## Design

One structural change carries FR1: the engine keeps the per-table **file inventory** it already builds at refresh time (today it is thrown into the `ListingTable` and lost), with each file's parsed time bounds. Query paths that know their window ask the engine for a **time-scoped table** instead of the registered whole-store table; the engine filters the inventory and builds a scoped provider over the surviving files. Callers without a window (unbounded SQL) keep the registered full table — behaviour unchanged.

FR2/FR3 live entirely in the cache layer: refresh stops calling `clear()` blanket-wide and instead drops only mutable-window entries (or switches to snapshot-versioned keys — ADR); execution goes through a single-flight wrapper keyed by the existing `CacheKey`.

FR4 is a routes-level default change plus config knob. FR5 is a semaphore in the request path (or config removal).

Decisions:
- [Per-query file pruning mechanism](./adrs/per-query-file-pruning.md)
- [Cache invalidation scope + single-flight](./adrs/cache-invalidation-scope.md)
- [Concurrency guardrail: enforce or remove](./adrs/concurrency-guardrail.md)

## Cross-cutting Concerns

- **Observability**: existing `sol_querier_files_opened` / `sol_querier_bytes_scanned` histograms are the acceptance signal — files-opened p95 must drop from ~O(store) to ~O(window). Add a counter for cache single-flight coalesced hits and for shed requests (FR5) if enforcement is chosen.
- **Migration/rollback**: read-side only; no file-format or schema change; every FR is independently revertable. The demo compose needs no change.
- **Verification**: unit/integration at demo scale (tempdir fixtures with many files), then live re-measurement of the same probes used in this analysis (single cold query, 20-query burst, metadata endpoints) — same commands, before/after numbers recorded in TASKS.md.
