# promql-plan-cache — live verification (S3 T4)

Image `sol:c591624ff` (S1+S2 of this workspace), store not wiped (exact-bounds names throughout; 844 files at probe time, ~14 new files/min across sinks → **~237 files inside every 15-min window**, files_opened p95). Host under the demo's steady load (compose foreground ~1.5 cores) — same conditions as the original baseline. Earlier contaminated probes (user loading all dashboards concurrently; 12 s outlier) discarded and re-run clean.

## Results vs targets

| Probe | Pre (July-15/16 baselines) | Now | Target | Verdict |
|---|---|---|---|---|
| Instant selector | 385 ms | **84.5 ms** | ≤ 90 ms (FR3) | ✅ MET |
| Result-cache hit (within 15 s bucket) | 5 ms | **9 ms** | sustained | ✅ |
| `optimize` stage, live mean | 26–29 ms/query (pre-cache) | **10.6 ms** (mixed hit/miss; 0.00 on hits per fixture) | eliminated on hits | ✅ plan cache works live (43 hits / 158 misses during probes — generation bumps with every gateway flush, so hit windows track ingest cadence) |
| Repeated-shape cold `rate()` | ~250 ms (old image, ~90 in-window files) | 370–420 ms (≈240 in-window files) | ≤ 80 ms (NFR1) | ❌ MISS — see decomposition |
| 20-query burst, cold buckets | ~1.4 s | ~2–3 s | ≤ 0.5 s (NFR2) | ❌ MISS (result-cached burst: sub-second ✅) |
| Bare selector range | 58 ms (idle, ~90 files) | 304 ms (~240 files, loaded) | floor reference | scan floor moved with the store |

## Honest re-decomposition (live stage means, clean 3-min window, burst-inclusive)

`execute` **835 ms** · `physical` 122 ms · `optimize` 10.6 ms · `lower` 3.9 ms · `parse` 0.9 ms.

The fixture that justified skipping E (rows of 2–5 per file) under-represented execution: live files carry real row volumes, and the demo's current flush cadence puts ~240 files inside every 15-min window (the July-15 store had ~90 → the 58 ms bare floor; the floor moved with the store, not the code). **A′ did exactly what it promised — planning above physical is eliminated — but the live bill is execution-bound.** NFR1/NFR2 are therefore NOT met by this workspace and re-fire the deferred levers, in causal order:

1. **Write-side small files (original item 6, deferred twice)**: gateway flush cadence / intra-day compaction of closed hours — divides the in-window file count (and with it both `physical` and `execute` file-setup shares) by ~5–10×. Now the top lever.
2. **E — smaller `rate()` lowering** (re-fired per the ADR's recorded trigger: "revisit only if S3 live verification misses") and **write-side `prom_series_key` column (item 7)** — both attack per-row execution (window aggregates, per-row UDF partition key).
3. The demo's steady compose load is measurement environment (~1.5 cores), not a code lever.

Dashboard UX today: first refresh of a cold shape ~0.4 s/panel, every subsequent refresh within a 15 s bucket ~9 ms, new buckets ~0.4 s — usable, visibly behind Mimir (~25 ms) on cold buckets.

## Reproduce

Same probe shapes as [backend-metrics-perf VERIFY](../../20260716_backend-metrics-perf/VERIFY.md); plan-cache counters: `sum by (result) (sol_querier_plan_cache_requests_total)`; stage means: `sum by (stage) (increase(sol_querier_plan_stage_duration_seconds_sum[3m])) / sum by (stage) (increase(sol_querier_plan_stage_duration_seconds_count[3m]))` via Mimir (port 9009).
