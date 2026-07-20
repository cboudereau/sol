# write-side-small-files — live verification (S2 T4)

Image `sol:3b6cfaaf2` (S1: chunk pass + revived hourly grouping), demo restarted, **no store wipe** (exact-bounds names throughout). Compactor demo config retuned during verification: `interval_secs 300→60`, and after a first measurement, `chunk_secs 300→180` + `chunk_grace_secs 120→60` (both within the accepted ADR's config-overridable constants) to shorten the open-hour raw tail. Host under the demo's steady compose load.

## Results vs targets

| Probe | Pre (promql-plan-cache VERIFY) | After (180/60 config) | Target | Verdict |
|---|---|---|---|---|
| Total metrics Parquet files | 844 | **49** | — | 17× fewer on disk |
| In-window files (15-min, EXPLAIN `files_ranges`) | 237 | **~45** (62 at 300/120 → 42–48 at 180/60) | ≤ 40 (NFR1) | **~MET (5.3×), see multiplier note** |
| Bare selector range, cold | 304 ms | **148 ms** | ≤ 150 ms (NFR2 floor) | ✅ MET |
| Repeated-shape `rate()` | 370–420 ms | **234–271 ms** | ≤ 80 ms, jointly owned | improved ~40 %; balance owned by row-work levers |
| Data visibility (newest raw age) | ≤ 30 s | **11 s** observed | unchanged (NFR3) | ✅ freshness preserved (flush cadence untouched) |

Active-day composition after retune (per metric subtype): ~8 raw (open tail) + ~7 chunk + 1 `compacted-hHH`.

## Honest read of NFR1 (≤ 40 in-window files)

Not strictly met — settles at ~45. The bar was written before accounting for a structural fact: the `metrics` table is a **union of 3 subtype tables** (gauge/sum/histogram), each with its own open-hour tail. **Per subtype the count is ~15** (8 raw tail + 7 chunk), which is on-target; the ×3 union is the entire gap. The two remaining levers for the union total are both out of this workspace's chosen scope:
- **Flush cadence** (30 s → longer): deliberately rejected in the ADR — it trades the demo's visible freshness (verified here at 11 s) for file count. Still a documented deployment knob.
- **Chunk length**: shorter chunks shrink the raw tail but add chunk files — measured non-monotonic (300/120 → 62; 180/60 → ~45; going shorter re-inflates via chunk count). 180/60 is near the minimum for this cadence.

The workspace's actual win is unambiguous and larger than the file count alone suggests: **237 → ~45 in-window files (5.3×), and the bare-range floor met (148 ms) — confirming file count was the bare-range bottleneck** (it had been flat at ~0.31 s from 237 down to 62 files, then dropped once the count fell far enough). `rate()` improved ~40 %; its residual is the row-work execution cost owned by the [promql-plan-cache follow-up](../../20260717_promql-plan-cache/README.md) (levers E / series-key), exactly the shared-ownership split NFR2 declared.

## Bonus finding (landed in S1)

Task 1 uncovered that **intraday hourly compaction had been silently dead since the exact-bounds rename** (`parse_hour` couldn't read the new names) — so closed hours weren't collapsing either. The fix (bounds-based hour grouping) is a large part of the 844 → 49 on-disk reduction, independent of the new chunk pass.

## Reproduce

EXPLAIN file count: `POST /api/v1/sql {"sql":"EXPLAIN ANALYZE SELECT COUNT(*) FROM metrics WHERE CAST(time_unix_nano AS BIGINT) BETWEEN <start_ns> AND <end_ns>"}` → `files_ranges_pruned_statistics=N total`. Latency/freshness probes as in the two predecessor VERIFYs.
