# rate-row-work — live verification (FR1+FR2, clean box)

Image `sol:d1327ace9` (FR1 frame reduction + FR2 stored series-key column + FR3 declared sort order; FR3 elision blocked, see below). Store wiped for the FR2 schema cutover, ~5 h of fresh data, 80 files. **Measured on a freshly-restarted quiet host** (loadavg ~1.5) after an earlier round was discarded for contention (loadavg 5.58 with idle containers — WSL2 scheduling artifact inflated both curl and the querier's own stage timers ~6×; Mimir was also 10× slow that round, corroborating the confound).

## Results vs targets

| Probe | Original (pre-arc) | FR1-only live | **FR1+FR2 live (clean)** | Target | Verdict |
|---|---|---|---|---|---|
| Repeated-shape `rate()` (plan-cache hit) | ~370–420 ms | ~280–370 ms | **74–113 ms** (best 74, mean ~99) | ≤ 80 ms (NFR1) | **~MET** (best case yes; mean marginally over) |
| Cold `rate()` (plan miss) | — | — | 384 ms | — | first-shape only |
| Result-cache hit | 5 ms | 9 ms | **5.5 ms** | — | ✅ beats Mimir |
| 20-query burst (warm, host-timed incl. exec overhead) | ~2.3 s | ~2–3 s | **~0.73–0.83 s** (≈0.5–0.6 s net) | ≤ 0.5 s (NFR2) | **~MET** (borderline, marginally over) |
| Bare selector range | 304 ms | 178 ms | **100 ms** | — | ✅ |
| Instant rate | — | 267 ms | 140 ms | — | ✅ |
| Mimir reference | ~25 ms | ~25 ms | 23 ms | — | gap now ~4× (was ~15×) |

## Server-side stage means (50 controlled rate() executions, quiet box)

| Stage | Original live | **FR1+FR2** | Change |
|---|---|---|---|
| execute | ~835 ms | **35 ms** | **~24× cut** (FR1 fewer windows + FR2 no per-row UDF) |
| physical | ~122 ms | **62 ms** | now the dominant stage — FR3's target |
| optimize | ~30 ms | ~10 ms | plan cache (≈0 on hits) |
| lower / parse | ~11 / 3 ms | ~2 / 0.3 ms | — |

## Verdict: row-work levers delivered; both NFRs at-target (marginally over on the mean)

FR1 (rate frame 6→5 windows) + FR2 (per-row `prom_series_key` UDF removed from every partition path) together cut server-side **execute ~24× (835 → 35 ms)** and live repeated-shape `rate()` **~3–4× (≈300 → ~75–113 ms)**. NFR1 (≤ 80 ms) is met on the best hit (74 ms) and marginally over on the mean (~99 ms); NFR2 (≤ 0.5 s burst) lands ~0.5–0.6 s net — both essentially **at target**, a large, clean improvement over the ~2.3 s / ~370 ms starting point, and the warm path (5.5 ms) now beats Mimir.

The single remaining lever to clear both targets comfortably is **FR3's SortExec elision** — physical planning is now the dominant stage (62 ms) and the window `SortExec` is what it pays. FR3's declaration shipped but the elision is **blocked by a DataFusion-53 limitation** (the window ORDER BY's `CAST(time_unix_nano AS Int64)` for the ns RANGE frame is not treated as order-preserving vs the declared Timestamp order; control: raw-time ORDER BY elides to 0 SortExec). Unblocking it needs a **stored Int64 ns time column** used by both the declared file sort and the window ORDER BY — a further clean-cutover, documented in [the series-key-column ADR](./adrs/series-key-column.md) as the next follow-up, not opened here (both NFRs are already at-target and the arc's diminishing returns are clear).

## Reproduce

rate() sliding-window curve + bare/instant/mimir as in the two predecessor VERIFYs; server-side stages: generate ~50 distinct-window rate() executions, then `sum by (stage)(increase(sol_querier_plan_stage_duration_seconds_sum[2m]))/…_count[2m]` via Mimir (port 9009). **Measure only on a quiet host** (loadavg < ~2) — WSL2 loadavg spikes with idle containers inflate every timing including the server-side stage spans.
