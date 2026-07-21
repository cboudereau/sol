# rate-row-work — live verification (FR1 pass)

Image `sol:f9afb6a25` (FR1 only — rate frame 6→5 windows), demo restarted, **no wipe**. Idle host apart from the demo. Store: active-day metrics ~8 raw + ~12 chunk + 3 hourly per subtype; ~74 files in a 15-min window.

## Results vs targets

| Probe | Pre (promql-plan-cache / write-side VERIFYs) | FR1 live | Target | Verdict |
|---|---|---|---|---|
| Cold repeated-shape `rate()` | 370–420 ms | **280–370 ms** | ≤ 80 ms (NFR1) | ❌ MISS |
| `rate()` result-cache hit | 5–9 ms | 9.6 ms | — | ✅ |
| Bare selector range | 304 → 148 ms | **178 ms** | reference | ~flat |
| Instant rate | — | 267 ms | — | — |
| Live stage means (3-min, burst-inclusive) | execute 835 / physical 122 | **execute-dominated; physical 148 / optimize 31 / lower 8 / parse 3** | — | execute is still the wall |

(The 3-min `execute` mean was skewed by a heavy full-store SQL EXPLAIN probe; the per-probe rate() wall ~300 ms is the reliable signal. The bare-vs-rate gap — 178 ms vs ~300 ms — is the rate() window machinery over real row volumes.)

## Verdict: FR1 landed, NFR1 still missed live — FR2/FR3 revisit trigger FIRED

FR1 is correct and helped (goldens bit-identical; fixture execute cut ~7×, 68 → 9 ms). But **NFR1 is still missed live** (~300 ms vs ≤ 80 ms), and the decomposition is unambiguous about why: the cost is **per-row**, not per-window. FR1 removed one of six window passes; each *remaining* pass still (a) evaluates the `prom_series_key` UDF on every scanned row to build the partition key, and (b) sorts the real row volume. The fixture (2–5 rows/file) couldn't surface this — it mispredicted live by ~6× here, consistent with the ~15× miss at promql-plan-cache T2b. **This is exactly FR2 (stored `prom_series_key` column → no per-row UDF, all paths) + FR3 (declared sort order → elide the window SortExec).**

The ADR's deferral gate ("reopen if live misses") has fired. Reopening FR2+FR3 requires a metric-schema change → clean cutover → **store wipe** (standing directive), which is a user decision — surfaced with these numbers rather than assumed.

## Reproduce

rate() sliding buckets + bare/instant as in the two predecessor VERIFYs; stage means `sum by (stage)(increase(sol_querier_plan_stage_duration_seconds_sum[3m]))/count` via Mimir (port 9009); in-window files via `EXPLAIN ANALYZE SELECT COUNT(*) FROM metrics WHERE …`.
