# write-side-small-files — open-hour chunk compaction

Lever 1 of the [promql-plan-cache](../20260717_promql-plan-cache/README.md) "Next levers": the live latency floor was dominated by ~240 Parquet files inside every 15-min metrics window. Exploration corrected the premise — active-day *closed-hour* compaction already shipped; the tail was the **open hour's** raw files, which nothing compacted.

## Delivered (4 tasks, 2 sessions; commits `56d572427`…`a39cdde96`)

- **Open-hour chunk compaction** ([ADR](./adrs/open-hour-chunk-compaction.md), option A — chunked write-once): the compactor merges each closed chunk (default now 180 s, grace 60 s) of the current hour into one exact-bounds-named, level-1, provenance-footered file superseding its raws. No querier change (the inventory parser already prunes that name shape), no store wipe. Rejected alternatives with numbers: rolling-partial (~30× write amp), flush-cadence increase (freshness regression).
- **Two latent bugs fixed en route** (S1 T1): (1) intraday *hourly* compaction had been silently dead since the exact-bounds rename — `parse_hour` couldn't read the new names — so closed hours weren't collapsing either; now grouped by exact bounds. (2) `superseded_inputs`/GC only scanned `compacted-*` footers, making chunk supersessions invisible; chunk names carry a `-chunk-` token and both scans were extended.
- Config surface + demo wiring (compactor tick 300→60 s; chunk fields), deterministic in-window count test, suite 254 → 261 / 0 / 2, clippy green.

## Live verdict ([VERIFY.md](./VERIFY.md))

Metrics files **844 → 49 on disk**; in-window 15-min **237 → ~45 (5.3×)**; **bare-range floor 304 → 148 ms — NFR2 floor met** (confirming file count was the bare-range bottleneck); `rate()` 370 → ~250 ms (improved ~40 %); data freshness preserved (11 s, flush cadence untouched).

**NFR1 (≤ 40 in-window files) honestly re-decomposed, not met strictly**: ~15 files/subtype is on-target; the `metrics` table's **3-subtype union** (gauge/sum/histogram) is the entire gap. The two levers for the union total are out of scope by choice — flush cadence (rejected on freshness) and further chunk shortening (non-monotonic: adds chunk files). The latency residual on `rate()` is row-work execution cost, owned by the promql-plan-cache follow-up (levers E / series-key).
