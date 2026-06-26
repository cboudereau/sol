---
status: proposed
---
# Operator-safety allowlist for rollup routing

Addresses: [FR2](../DESIGN.md#fr2), [NFR1](../DESIGN.md#nfr1)

## Problem
The rollup keeps the **last raw sample per (series, time-bucket)**. That is exact for operations that only need the last cumulative value, but **lossy** for operations that need intra-bucket detail. The current router picks a tier by **step alone**, so `max_over_time(m[…])` at a coarse step reads the 5m tier and silently under-reports peaks (a spike between bucket boundaries is gone). Which operators may route to a tier?

## Options
| Option | Pros | Cons |
|---|---|---|
| A. Static allowlist of rollup-safe operators; unknown/unsafe → raw | Correct by construction; tiny static table; conservative default can't be wrong | `max/min/avg/quantile_over_time` lose the tier speedup (read raw) |
| B. Route everything by step (status quo) | Max speedup, simplest | **Silently wrong** for `*_over_time` aggregations — a correctness bug, not just imprecision |
| C. Make the rollup carry per-bucket min/max/sum/count so all ops are safe | Every operator could use a tier | Write-side change (bigger rollup, more storage/compute); out of this work's scope; still wrong for `quantile_over_time` |

## Decision
**Option A.** A static classifier `op_safety(expr) -> bool` over the parsed PromQL `Expr` (`Expr::Call` with `c.func.name`, per Phase 4a analysis). Scoped to what the querier actually supports today:
- **Safe (may route to a tier):** `rate`, `increase` (counter rate = `(last−first)/window`, sampling-density-independent — exact over last-per-bucket), and `histogram_quantile` (operates on cumulative `bucket_counts`, which the rollup preserves).
- **Unsafe (force raw):**
  - `irate` — slope of the *last two* samples; over a 5m rollup the "last two" are 5m apart, changing the result. Sampling-dependent ⇒ raw.
  - `max_over_time`, `min_over_time` — intra-bucket peaks are dropped.
  - `avg_over_time` — needs all samples.
  - `sum_over_time`, `count_over_time` — sum/count of *samples in window*; the rollup has one sample per bucket ⇒ undercounts.
- **Unknown / unsupported / unclassified:** default to **raw** (fail-safe). `delta`/`resets`/`last_over_time`/`quantile_over_time` are not implemented yet (they error today); when added, `delta`/`resets`/`last_over_time` would be safe and can join the allowlist, `quantile_over_time` would not.

The choke point ([tier-resolution-choke-point](./tier-resolution-choke-point.md)) consults this; unsafe ⇒ all-raw windows.

## Consequences
- **Correctness guaranteed**: no rollup-unsafe operator ever reads a downsampled tier; `*_over_time` peak/avg/quantile panels match raw.
- `max_over_time`-style panels over long ranges stay full-resolution (slower) — accepted; correctness > cost. If they later need acceleration, **Option C** (per-bucket aggregates in the rollup) is the documented future lever, not a step-only override.
- The allowlist is a small maintenance point: a new range function defaults to raw until explicitly classified safe — fail-safe.
- Parity tests must include an unsafe operator (`max_over_time`) asserting it reads raw even at a coarse step.
