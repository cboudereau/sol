---
status: draft
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

## Decision

Pending FR1 profile. Expected shape of the decision: the option (or combination) that removes ≥ 80 % of the measured hot stage while keeping the correctness bar (byte-identical results, complete key). Recommendation will be written here with the profile table attached.

## Consequences

(To be completed with the decision. Known regardless of option: a new `sol_querier_plan_cache_*` telemetry pair; a second cache keyspace beside the result cache; the first occurrence of each shape after restart stays cold.)
