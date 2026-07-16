---
status: proposed
---
# Plan-cache mechanism: which pipeline stage to reuse, and how

Addresses: [FR2](../DESIGN.md#fr2), [NFR1](../DESIGN.md#nfr1), [NFR2](../DESIGN.md#nfr2)

## Problem

~190 ms of every cold `rate()` query is the plan path (measured: 250 ms `rate()` vs 58 ms bare selector, identical window; flat in window width; warm result-cache hit 5 ms). Dashboards re-issue the same expression shapes with only the window sliding, so this cost is repeated ~every refresh × every panel. We must reuse the expensive stage(s) across window slides without ever serving a plan that doesn't match the current store (inventory snapshot), tables (tier routing), or config.

**Blocked on [FR1](../DESIGN.md#fr1) data**: the 190 ms is not yet split across lowering / optimizer / physical planning / execution. Each bucket has a different best remedy. This ADR goes `proposed` when the profile lands.

## Options

| Option | Reuses | Pros | Cons / risk |
|---|---|---|---|
| A. Logical-plan template cache + literal rebind | lowering (+ optimizer if rebind-then-optimize is cheap) | No DataFusion internals; rebind = TreeNode rewrite of the time literals | If the optimizer is the hot stage, rebind-then-reoptimize saves little |
| B. Optimized-plan cache with placeholder params (`$start`,`$end`) | lowering + optimizer | Biggest win if optimizer dominates | DataFusion 53 may not optimize placeholder plans fully; risk of fighting the framework (rabbit-hole cap applies) |
| C. Optimizer-pass trimming for querier sessions | part of optimizer, all queries incl. first-shape | No cache, no keying problem, helps ad-hoc too | Bounded win; needs care not to lose passes that matter for other query shapes (SQL endpoint shares the ctx) |
| D. Physical-plan cache keyed to (shape, window bucket, inventory generation) | everything above execution | Maximal reuse | Key must include the scoped FILE LIST (window slide changes files!) → hit rate collapses unless keyed per bucket; version-fragile |
| E. Cheaper `rate()` lowering (fewer window aggregates) | execution + plan size | Helps first-shape and ad-hoc too | Only correct if extrapolation semantics preserved; predecessor's parity tests are the bar; likely combined with A/C rather than alternative |

Key completeness (whatever option wins): expression text ⊕ step/window-shape bucket ⊕ resolved table set (tier routing outcome) ⊕ inventory snapshot generation ⊕ relevant config (lookback constants). Each component gets a "changing it misses" test.

## FR1 profile (release build, demo-scale fixture: 1,505 gauge + 40 histogram exact-bounds files / 7 days; ms)

| shape | run | total | parse | lower | optimize | physical | execute | files |
|---|---|---|---|---|---|---|---|---|
| `rate(m[5m])` | cold | 178.8 | 1.9 | 11.9 | 25.9 | 67.4 | 68.3 | 24 |
| `rate(m[5m])` | shape-warm | 109–118 | 0.3 | 3.7 | ~29 | 48–61 | 22–26 | 24 |
| `rate(m[5m])` | result-cache hit | 5.8 | 0.3 | 4.4 | 0 | 0 | 0 | 0 |
| bare `m` | cold | 29.5 | 0.2 | 0.7 | 4.0 | 6.4 | 16.9 | 23 |
| `histogram_quantile` | cold | 113.1 | 0.4 | 0.7 | 1.3 | 9.8 | **99.7** | 45 |
| `histogram_quantile` | shape-warm | 36–45 | 0.3 | 0.6 | 1.3 | ~11 | 23–30 | 45 |

Cold `rate()` shares: parse 1 % · lower 7 % · optimize 14 % · **physical 38 %** · execute 38 %. Shape-warm (the dashboard-refresh case, page cache hot): **planning ≈ 74 %** (optimize ≈ 26 % + physical ≈ 48 %), execute ≈ 21 %. Stage sums within 1–3 % of totals. Debug ≈ 9× inflated, same ordering. `histogram_quantile` is execution-bound (88 % cold) — a plan cache barely helps it; the result cache covers its repeats.

## Decision

**Proposed: A′ + E.** The profile overturns the draft's framing: the optimizer is NOT the dominant stage — **physical planning is** (48 % shape-warm), and it scales with plan size (bare selector pays ~8 ms where `rate()` pays ~55 ms over the same store).

- **A′ — cache the *optimized* logical plan, rebind literals, skip re-optimize**: key = (expr text, step bucket, resolved table set, inventory generation, lookback config); rebind the window's time literals via a TreeNode rewrite of the cached plan; then call `query_planner().create_physical_plan()` directly — the seam split in `execute_recording_scan` (task 1) already exposes exactly this hook. Removes lower+optimize ≈ 33 ms/query. Plain A (rebind then re-optimize) would save only ~4 ms — rejected. B (placeholder plans) is unnecessary given A′; C (optimizer trimming) is subsumed; D (physical-plan cache) is not viable — its key must include the scoped file list, which changes on every window slide.
- **E — shrink the `rate()` lowering** (fewer/fused window aggregates; the instant==range parity and extrapolation golden tests are the correctness bar): the only lever that attacks the dominant physical stage (and execute), including first-shape and ad-hoc queries. Expected combined landing: repeated-shape `rate()` ≈ 60–80 ms on the fixture; A′ alone lands ~80 ms (borderline for [NFR1](../DESIGN.md#nfr1), likely above it live at ~1.4× fixture).

Sequencing if ratified: A′ first (mechanical, low risk), re-profile, then E sized by what remains.

## Consequences

- New `sol_querier_plan_cache_*` hit/miss counters; a second keyspace beside the result cache; first occurrence of each shape after restart stays cold.
- A′'s rebind rewrite must be provably total for our generated plans (every time literal it must touch is produced by `prom_time_between`/window frames — a test asserts rebound-plan == freshly-built-plan for each query shape).
- E re-opens `plan/frame.rs` — the parity/golden suites gate it; if E's cut proves insufficient, the remaining lever is the write-side series-key column (kept deferred).
- `histogram_quantile` stays execution-bound — out of this ADR's blast radius by design.
