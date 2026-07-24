---
status: accepted
---
# Prometheus-compatible extrapolated rate/increase

Addresses: [FR1](../designs/range-rate-parity.md#fr1), [NFR1](../designs/range-rate-parity.md#nfr1)

## Problem
Sol's `rate()`/`increase()` sum per-sample reset-adjusted deltas over the `(t−range, t]` frame and divide by the **fixed** `range_ns`. Because the summed increase actually spans from the sample just before the window to the last sample in it — a time that oscillates between ≈`range` and ≈`range+scrape` as the eval grid slides — dividing by a fixed window makes the per-second rate oscillate by ≈±(scrape/range). That is the dashboard zigzag; Mimir is smooth. How do we make Sol's range rate/increase match Mimir?

## Options
| Option | Pros | Cons |
|---|---|---|
| A. Replicate Prometheus `extrapolatedRate` (extrapolate the in-window increase to the window boundaries — up to half the average inter-sample interval, capped at the edge; counter-`rate` clamps a below-zero start extrapolation — then divide by the full window) | Matches Mimir/Prometheus (the actual goal — dashboard parity); well-specified, battle-tested algorithm; smooth by construction | Needs first/last sample time + count over the RANGE frame (extra window aggregates); most code |
| B. Divide the in-window increase by the **actual** sample span `(last_time − first_time)` instead of the fixed window | Removes the fixed-divisor oscillation; small change | Still not Prometheus (no boundary extrapolation) → won't match Mimir at the edges / short windows; a *different* number from both today and Mimir |
| C. Leave as-is (non-extrapolating) | No work | The zigzag persists; Sol dashboards visibly diverge from Mimir |

## Decision
**Option A** — replicate Prometheus's `extrapolatedRate`. The explicit goal is **parity with Mimir** on the shared dashboards; only matching Prometheus's algorithm achieves that. Layer the extrapolation on top of the **existing** reset-adjusted in-window increase (keep reset handling as-is): gather `first_time`, `last_time`, and sample `count` over the same `(t−range, t]` frame; compute `averageDurationBetweenSamples = (last_time − first_time)/(count − 1)`; extrapolate the increase to the window start and end by up to `averageDurationBetweenSamples/2` on each side, but never past the window boundary; for `rate`/`increase` over a counter, if the extrapolated start would imply a value below zero, clamp the start extrapolation to the zero point (Prometheus's "counters can't be negative" rule). `rate` then divides the extrapolated increase by `range` seconds; `increase` returns it directly. `irate` (last-two-sample slope) is unaffected by extrapolation and stays as-is.

## Consequences
- **Parity achieved**: range `rate`/`increase` match Mimir within tolerance; the zigzag disappears (smooth by construction).
- **Values shift**: existing tests that assert the *non-extrapolated* numbers must be updated to the extrapolated expectation; the instant==range parity tests remain valid (both paths extrapolate identically).
- **Extra window aggregates** (first/last sample time + count over the frame) — a compute-only addition, no extra scan (NFR2).
- **Faithful, not bit-exact**: parity asserted by tolerance + analytic golden fixtures, per the DESIGN rabbit hole.
- If a required frame aggregate is not expressible in DataFusion 53, that surfaces as an outside-constitution gap (new ADR), not a silent workaround.
