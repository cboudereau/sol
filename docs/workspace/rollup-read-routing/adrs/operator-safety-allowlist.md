---
status: proposed
---
# Operator → capability classifier + rich rollup

Addresses: [FR2](../DESIGN.md#fr2), [FR6](../DESIGN.md#fr6), [FR7](../DESIGN.md#fr7), [NFR1](../DESIGN.md#nfr1)

## Problem
The rollup keeps the **last raw sample per (series, time-bucket)**. That is exact for operators that only need the last cumulative value (`rate`/`increase`/`histogram_quantile`) but **lossy** for operators needing intra-bucket detail (`max/min/avg/sum/count_over_time`). The original router picked a tier by **step alone**, so `max_over_time(m[…])` at a coarse step read the 5m tier and silently under-reported peaks. Which operators may route to a tier — and must we forfeit the tier's acceleration for the lossy ones?

## Options
| Option | Pros | Cons |
|---|---|---|
| A. Static binary allowlist (safe→tier, else raw) | Tiny; correct by construction | `max/min/avg/sum/count_over_time` lose the tier speedup permanently — and these are heavily used (`max_over_time` is on 8+ dashboard panels). "Force raw to support resolution" is the compromise we want to avoid. |
| B. Route everything by step (status quo) | Max speedup | **Silently wrong** for `*_over_time` — a correctness bug |
| C. Rich rollup: carry per-bucket `{last,min,max,sum,count}`; classify each operator by the **capability** it needs; route to the coarsest tier that carries it | All of `max/min/avg/sum/count_over_time` route to a tier **and** stay exact; "use the best tier that can answer", not "fall back to raw"; the capability model extends cleanly (sketches → quantiles later) | Write-side change (bigger rollup + read-side per-op column selection); `quantile_over_time`/`irate` still raw |

## Decision
**Option C.** Replace the binary `op_safety -> bool` with a capability classifier `op_capability(&Expr) -> Capability`, and enrich the rollup (FR6) so every tier carries `{Last, MinMax, SumCount}`.

`Capability` (scoped to the implemented dispatch `prometheus.rs:333-353`):
- **`Last`** — `rate`, `increase`, `histogram_quantile`, bare cumulative-counter selector. Served from the last-valued columns (unchanged).
- **`MinMax`** — `max_over_time` → `MAX(value_max)`; `min_over_time` → `MIN(value_min)`.
- **`SumCount`** — `avg_over_time` → `SUM(value_sum)/SUM(value_count)`; `sum_over_time` → `SUM(value_sum)`; `count_over_time` → `SUM(value_count)`.
- **`None` (force raw)** — `irate` (last-two-sample slope, sampling-dependent), `quantile_over_time`/`stddev_over_time`/`stdvar_over_time` (need per-bucket distributions/sum-of-squares), and any unknown/unimplemented/unclassified operator (fail-safe default). `delta`/`resets`/`last_over_time` join `Last` when implemented.

The choke point ([tier-resolution-choke-point](./tier-resolution-choke-point.md)) routes to the coarsest tier ≤ resolution **whose advertised capabilities ⊇ the query's capability**; if none qualifies (or capability is `None`), all-raw.

## Consequences
- **Correctness + speed together**: `max/min/avg/sum/count_over_time` now read a tier *and* equal the raw result (no dropped peaks). This is the whole point — we stop trading the tier away to be correct.
- **Write-side scope is in**: this is no longer a read-only refactor (see [rollup aggregate schema ADR](./rollup-aggregate-schema.md)). Doing write+read together avoids re-creating the write-ahead-of-read gap that caused the original bug.
- **Irreducible raw residue**: `irate`, `quantile_over_time`, `stddev/stdvar` stay raw — documented, not a gap. A sketch-carrying tier could serve quantiles later; that's a future capability, not this scope.
- **Fail-safe**: a new range function defaults to `None` (raw) until classified.
- Parity tests must cover one operator per capability (`max_over_time`/MinMax, `avg_over_time`/SumCount, `rate`/Last) asserting tier == raw, plus a `None` op (`irate`) asserting raw even at a coarse step.
