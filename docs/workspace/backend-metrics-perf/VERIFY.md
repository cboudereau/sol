# backend-metrics-perf — live verification evidence

Store/image at measurement: `sol:401e8eb90` (S1+S2; T7 not yet in image), demo store wiped at cutover — 765 Parquet files, single active day, all exact-bounds names (`<min_ns>-<max_ns>-<uuid>.parquet`). Idle host (verified — earlier contaminated numbers from a concurrent clippy build were discarded). Probes run from the demo `curl` container against `sol-querier:9009` / `mimir:8080`.

## Before / after

| Probe | Baseline (`ac28543d8`, 1,529 files / 7 days) | After S1+S2 (`401e8eb90`, 765 files / 1 day) | Target | Verdict |
|---|---|---|---|---|
| Cold 15-min `rate()` range query | 240–410 ms | 234–475 ms (typ. ~250 ms) | ≤ 50 ms (NFR1) | **MISS — see decomposition** |
| Same query, warm (within 15 s bucket) | 4 ms (wiped every 15 s → never sustained) | **5 ms, sustained** (refresh no longer clears — FR2) | — | ✅ |
| Bare selector range query (no window fns), cold | n/a (not measured at baseline) | **58 ms** | — | ✅ scoping delivers |
| Nonexistent-metric `rate()` | ~270 ms | ~287 ms | few ms | MISS (same cause) |
| 20-query dashboard burst, wall | ~2.3 s @ ~968 % CPU (~19–22 core-s) | **~1.4 s** worst query | ≤ 500 ms / ≤ 2 core-s (NFR2) | **partial** (~40 % better; CPU sample inconclusive) |
| `/labels` (no start) | n/a (unbounded scan family ~0.4–0.6 s) | **70 ms** | bounded | ✅ (FR4) |
| `/series` (no start) | 370 ms | **113 ms** | bounded | ✅ (FR4) |
| `__name__` values (no start) | 570 ms | 625 ms | bounded | ~flat — see note |
| Simple SQL `COUNT(*)` full store (unscoped by design) | — | 220 ms | — | reference |
| Mimir reference (same query) | 1.5–9 ms | 1.6 ms | — | structural gap unchanged |

## Root-cause decomposition of the residual constant (~0.25 s)

Measured discriminators (idle host):
1. **Cold latency is flat vs window width** (15 m = 0.23 s, 1 h = 0.28 s, 4 h = 0.21 s) → scan volume is not the driver.
2. **Bare selector range = 58 ms** vs **`rate()` range = ~250 ms**, identical window → **~190 ms is the `rate()` window-function plan** (LAG + six RANGE-frame aggregates over `(t−range, t]`, per-row `prom_series_key(attributes)` UDF partition key, multiple sorts, extrapolation arithmetic — `src/querier/plan/frame.rs`).
3. Warm hit = 5 ms → the entire constant sits in plan+execute, none in HTTP/serialisation.
4. Instant selector = 385 ms: `selector_base_df` scopes `[i64::MIN, time]` (half-open — latest-≤ has no sound lower bound), i.e. a full-store scan on the instant path.

**Conclusion**: the original baseline attribution of the ~0.25 s fixed cost to Parquet footer opens was **partially wrong** — footer opens were real (EXPLAIN ANALYZE: 1.35 K file-ranges pruned/query) but their time share at demo scale was the minority; the majority is the PromQL window-function plan path, which was a declared DESIGN non-goal. FR1's mechanism itself is verified working (58 ms bare range; unit test `test_range_query_opens_only_window_files` red 3 → green 1; row-group pruning 759 → 91 matched on EXPLAIN).

## Revisit triggers fired (per DESIGN non-goals)

- **`rate()` plan cost dominates profiles after FR1** → FIRED. Follow-up workspace levers, in expected-impact order: (a) plan caching keyed by (expr shape, table, window bucket) — the warm path proves 5 ms is achievable; (b) simplified rate lowering (fewer window aggregates); (c) write-side `prom_series_key` column (deferred item 7). NFR1 (≤ 50 ms cold `rate()`) and NFR2 (≤ 0.5 s burst) are re-owned by that follow-up — the burst is 20 × the same plan constant.
- Minor, same family: instant-path half-open scope (full-store scan per instant query) — bound it with a staleness lookback (Prometheus uses 5 m) in the follow-up.
- Micro: `scoped_files` widens the *query* window by the 1 h legacy margin for all files, including exact-bounds ones (double margin — parse-time skew + query-time lateness). Harmless for correctness (superset), costs a few extra files per query; fold into the follow-up.

## Reproduce

```sh
# single cold/warm
docker exec otel-lgtm-dotnet-curl-1 sh -c 'end=$(date +%s); start=$((end-900)); q="sum by (http_response_status_code) (rate(http_server_request_duration_seconds_count{service_name=\"service\"}[2m]))"; for i in 1 2; do curl -s -o /dev/null -w "%{time_total}\n" -G "http://sol-querier:9009/prometheus/api/v1/query_range" --data-urlencode "query=$q" --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode "step=15"; done'
# burst: fire all 20 dashboard exprs concurrently (queries extracted from the RED dashboard JSON with variables substituted)
# metadata
docker exec otel-lgtm-dotnet-curl-1 sh -c 'curl -s -o /dev/null -w "%{time_total}\n" http://sol-querier:9009/prometheus/api/v1/labels'
```
Full probe scripts: see the session transcript / TASKS.md Measured-baseline table for the exact baseline commands (identical shapes).
