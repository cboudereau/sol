# range-rate-parity — Design Doc

## Context

On the RED dashboard ("Http Server requests Rates" + the Stats "Rates" panels), Sol's range `rate()`/`increase()` time-series **zigzag** (rapid ±15–25% oscillation) while the same panels on Mimir are **smooth**. Live diagnosis established:

- The raw counter stored by Sol is **monotonic-cumulative and near-identical to Mimir's** (`657→671→…→764` on both) — so this is **not** a counter-reset bug and **not** a cumulative↔delta temporality mismatch.
- Root cause is in `src/querier/plan/frame.rs::rate`: the windowed increase is `SUM(per-sample reset-adjusted delta)` over the **half-open `(t−range, t]` RANGE frame**, which telescopes to `v_last_in_window − v_last_sample_before_window`, then divided by the **fixed** `range_ns`. As the eval grid `t` advances by `step`, samples cross the window's trailing edge, so the actual time spanned by that increase oscillates between ≈`range` and ≈`range + scrape_interval` — but the divisor stays `range`. Result: the per-second rate oscillates by ≈±(scrape/range). With a 15 s scrape and a short `$__rate_interval`, that is the visible zigzag.
- Prometheus/Mimir avoid it by **extrapolation**: they take the first/last samples in the window, extrapolate the increase to the exact window boundaries (with counter-reset and start-of-series handling), and divide by the full window → a stable per-second value → smooth.
- Secondary: a **left-edge ramp** — the range path scans `[start, end]`, so the first grid points (whose `(t−range, t]` reaches before `start`) have a truncated window → low, ramping values at the left edge that Mimir doesn't show.

This is **pre-existing** — `rate()` predates the rollup-read-routing work, which only re-routed which table is scanned, not the rate math. The steady-state *value* is roughly right (Sol range rate at `now` ≈ Mimir), so this is a **shape/parity** defect, not a gross correctness bug — but it makes Sol's dashboards visibly diverge from Mimir.

## Functional Requirements

### <a id="fr1"></a>FR1 — Prometheus-compatible extrapolated rate/increase
`rate()`/`increase()` (and `delta` if/when implemented) compute the windowed increase, then **extrapolate to the window boundaries** using the Prometheus `extrapolatedRate` algorithm (extrapolate to the first/last sample by up to half the average inter-sample interval, capped at the window edge; counter-`rate` clamps a downward extrapolation at zero), and `rate` divides the extrapolated increase by the full window seconds. The result must be smooth across the eval grid (no per-step oscillation on a steadily-increasing counter) and match Mimir within tolerance.

### <a id="fr2"></a>FR2 — Pre-window lookback for the range path (left-edge ramp + shard-boundary dips)
The range path must scan enough history that every eval grid point — including the first point of the query **and the first point of each per-day shard** — has a full `(t−range, t]` window with a LAG predecessor. Today `handle_range` (`prometheus.rs:2220`) calls `frontend::split(lo, hi, 0, hi)` with **`lookback_ns = 0`** and maps shards to `(table, shard.start_ns, shard.end_ns)`, ignoring the shard's already-computed `query_start_ns` (`= start − lookback`). So each shard scans exactly `[shard.start, shard.end]` and rate truncates at every day boundary (and the query start). Fix: pass `lookback_ns = range_ns` to `split`, scan each shard from its `query_start_ns`, and emit output points only for `[shard.start, shard.end]` (the lookback region seeds LAG/window fill without double-emitting points that belong to the previous shard). This mirrors the instant path's `lag_margin` (`instant_range_windows`, which already scans `[anchor − 2·range, anchor]`). `frontend::split` already supports the lookback (`test_rate_shards_overlap_by_lookback`); this wires it into the range path.

### <a id="fr3"></a>FR3 — Instant/range rate stay consistent
The instant `rate`/`increase` path already matches the range path (parity tests `test_instant_rate_matches_range_rate` etc.). After FR1/FR2 change the range rate semantics, the instant path must produce the **same** extrapolated value at a given instant — the existing instant==range parity tests stay green (updated to the extrapolated expectation).

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Sol↔Mimir parity within tolerance
`rate`/`increase` over the same data equal Mimir's within a small relative tolerance (extrapolation replicates Prometheus's algorithm). Verified by golden fixtures with known counter series (exact against the analytic extrapolated value) and, manually, on the live demo RED dashboard.

### <a id="nfr2"></a>NFR2 — No scan/perf regression
The change is compute-only over the already-scanned window (add first/last sample-time window aggregates); it must not increase the bytes/rows scanned. FR2's lookback widens the scanned span by one `range` — bounded and small.

### <a id="nfr3"></a>NFR3 — No new dependencies
Within the pinned set (datafusion 53, datafusion-functions-json 0.53.1, object_store 0.13, promql-parser 0.9, moka 0.12). Plain DataFusion window functions over the existing frame.

## Non-goals
- **Day-aligned sealed boundary / tier-coverage cap** — a rollup-routing efficiency item (captures the last sealed day for the tier). It's about *which table* is scanned, not the rate math. Deferred; belongs with rollup-read-routing follow-up, not here.
- **Instant `histogram_quantile` dispatch error** — a separate instant-path dispatch bug (range histogram works). Not rate. Separate.
- **`avg/sum/count_over_time` over a tier exactness** — bucket-alignment approximation; a rollup-read-routing ADR-wording/follow-up, unrelated to rate extrapolation.
- **`quantile/stddev/stdvar_over_time`, subqueries** — not in scope.

## Rabbit holes
- **Bit-exact Prometheus parity.** Do not chase sub-millisecond float-identical output. Replicate the `extrapolatedRate` algorithm faithfully (the half-interval extrapolation + boundary cap + counter-zero-clamp) and assert **tolerance-based** parity. Cap: match the algorithm, not the last ULP.
- **Counter-reset × extrapolation interaction.** Keep the existing reset-adjustment; layer extrapolation on the reset-adjusted increase. Don't re-derive reset handling.
- **Window-function availability in DataFusion 53.** Need first/last sample value+time over the RANGE frame (FIRST_VALUE/LAST_VALUE/nth). If a needed frame aggregate isn't expressible, that's an outside-constitution gap → ADR, not a workaround.

## Design

Extend `frame.rs::rate` (and `irate` where relevant) to gather, over the same `(t−range, t]` RANGE frame, the pieces Prometheus's `extrapolatedRate` needs:
- reset-adjusted `increase` within the window (existing SUM-of-deltas),
- `first_time`, `last_time` (sample timestamps of the window's first/last members),
- sample `count` in the window,

then compute `extrapolated_increase` per Prometheus (extrapolate to window start/end by ≤ half the average inter-sample gap, capped at the window edge; for `rate`-counter clamp a start extrapolation that would go below zero), and `rate = extrapolated_increase / (range_ns/1e9)`.

FR2: the range path's scan lower bound is extended by `range` (pre-window lookback) so per-grid-point windows are complete at the left edge — mirroring `instant_range_windows`' LAG-margin, applied to `handle_range`'s source window.

Decisions:
- [Prometheus-compatible extrapolated rate](./adrs/extrapolated-rate.md)

## Cross-cutting Concerns
- **Observability**: none new; rate values shift to match Prometheus.
- **Migration / rollback**: pure read-path compute change; no data/schema change; revert is code-only. No parquet impact.
- **Correctness guard**: golden fixtures (analytic extrapolated value) + instant==range parity + manual Sol↔Mimir RED-dashboard check.
