---
status: accepted
---
# Precomputed counter delta + windowed-rate semantics

Addresses: [NFR6](../DESIGN.md#nfr6) (latency), Sol↔Mimir `rate` parity.

> **`accepted`** (2026-06-13) — user ratified **Option B** (compactor-computed delta) **+ windowed-rate** semantics. Split into two parts on landing:
> 1. **Windowed-rate (read-side, no cutover) — implement now.** Fixes the semantics deviation; the parity tests are updated.
> 2. **Delta perf-column (Option B, compactor) — DECLINED after measurement (2026-06-15).** Live isolation on a counter (63.6K rows): scan+filter 0.236s vs scan+**LAG** 0.294s → the LAG window adds only **~60ms (~25% over scan)**, not the cold-24h bottleneck. Decisively, **windowed-rate = LAG(delta) + RANGE-SUM**, so a precomputed delta removes only the LAG, *not* the RANGE-SUM windowing. The win ceiling (~10–20% of the rate path, compacted-only) doesn't justify a compactor change + schema change + clean cutover + path-separation. **YAGNI.** If cold-24h latency needs cutting, the lever is **fewer rows (intraday rollups)**, not precomputed deltas. (Secondary: `prom_series_key` over the MAP is now the larger per-row cost than the LAG — a future look if grouping/rate-partition cost matters.)

## Problem

Live measurements: the scan is cheap (~23 ms `count(*)` over 11.5 M rows); the cold-24h cost is the **`rate()` LAG window + step-resample** over full-resolution samples. The goal: precompute a **per-sample reset-adjusted increase (`delta`)** so `rate(m[w]) = sum(delta over (t-w, t]) / w` — a windowed sum, no query-time `LAG`, **keeping `[window]` flexibility** (unlike recording rules). Bonus: it yields a true windowed-rate, closer to Prometheus than Sol's current per-sample-slope rate.

## The obstacle (why it isn't a simple codec change)

`delta_i = v_i − v_{i-1}` for the same series (reset → `delta_i = v_i`). The codec writes **one Parquet file per flush, statelessly** — it sees only the current batch. Within a file, consecutive same-series rows give the delta, **but the first sample of each series in each file has no in-file predecessor** (its predecessor is in the *previous* flushed file). At the demo cadence (scrape 15 s, flush 30 s → ~2 samples/series/file), that's **~half the deltas missing** at file boundaries → a stateless codec delta would badly undercount. So the delta cannot be correctly computed in the stateless per-batch codec.

## Options

| Option | How | Pros | Cons |
|---|---|---|---|
| A. Stateful upstream transform | a Sol transform tracks last value per series across batches, emits `delta` before the codec | exact deltas on **all** files incl. the live tail; rate read is uniform | new stateful component: per-series state (cardinality), restart/state-loss handling, ordering — real complexity |
| B. Compactor-computed delta + hybrid read | the **compactor** (sees the full sorted partition) writes the `delta` column into compacted files; raw/uncompacted files have none; rate read uses the column where present, **falls back to LAG** for raw | no new ingest component; reuses the compactor's whole-partition view; most data is compacted → most rate queries benefit | hybrid read path (column vs LAG); the recent raw tail still pays LAG (but that's a small window); two code paths to keep consistent |
| C. Drop the idea; optimize rate differently | — | no schema/cutover | leaves the cold-24h `rate` cost (this is what we set out to fix) |

## Recommendation (proposed)

**Option B — compactor-computed delta + hybrid read.** It avoids a new stateful ingest path (B's state lives in the compactor's existing whole-partition pass), and the LAG fallback only applies to the small uncompacted tail — exactly the window where data is freshest and smallest. The `delta` column rides the **same clean cutover** as the MAP change ([materialized-label-columns](./materialized-label-columns.md)). Read path: `rate(m[w]) = sum(delta)/w`, `increase = sum(delta)`, over compacted partitions; raw tail via the existing `plan::frame::rate` LAG. Gauges/histograms unchanged.

**Semantic note:** this changes `rate` from Sol's current per-sample-slope (≈ `irate`) to a true windowed sum (≈ Prometheus `rate`, sans boundary extrapolation). It should **narrow** the Sol↔Mimir gap, but it **changes rate values** → the existing rate parity tests must be updated to the new (more correct) expected values and re-verified against Mimir. Boundary extrapolation remains a separate, optional refinement.

## Consequences

- **Easier:** long-range `rate`/`increase`/`sum(rate)` become windowed sums over a precomputed column — the measured cold-24h bottleneck — without losing `[window]` flexibility; composes with rollup tiers (store summed delta per bucket later).
- **Harder:** Option B adds a hybrid read path + compactor work + a `delta` column (storage, slightly worse counter compression); rate semantics shift (parity tests updated). Option A would instead add a stateful ingest transform.
- **Open for ratification:** A vs B (recommend B), and acceptance of the rate-semantics change (parity tests updated to windowed-rate).
