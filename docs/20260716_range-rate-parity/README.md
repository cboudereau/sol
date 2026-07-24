# range-rate-parity — Prometheus-compatible extrapolated rate()

Fixes the range `rate()`/`increase()` Sol↔Mimir divergence: the RED-dashboard graph zigzagged on Sol (fixed-divisor windowed increase oscillates ±scrape/range as samples cross the trailing edge) and ramped at the left edge (no pre-window lookback in the range scan).

- **Design**: [designs/range-rate-parity.md](./designs/range-rate-parity.md)
- **ADR**: [extrapolated-rate](./adrs/extrapolated-rate.md) (accepted) — Prometheus `extrapolatedRate` over the RANGE frame (`plan/frame.rs`): base = sum_delta − first_delta, edge extrapolation ≤ avg_gap/2, counter zero-clamp, cnt<2 → NULL; `handle_range` wires frontend shard lookback (scan from `query_start_ns`, emit only the shard window). `irate`/`*_over_time` untouched.
- **Implemented**: S1 all 3 tasks, `2d07c34e2`. Golden test recovers true slope 6.6667/s within 1e-6; instant==range parity matrix held; querier:: 220/0/1 at close.
- **Live verification (2026-07-16, image `sol:401e8eb90`)**: Sol `rate()` jitter = 1.8 % of mean vs the historic ~37 % zigzag (Mimir same query: 0.5 %); series means agree within eval-instant tolerance. Zigzag gone; left-edge ramp gone (lookback in place). Recorded in [backend-metrics-perf VERIFY corrections](../20260716_backend-metrics-perf/VERIFY.md).
