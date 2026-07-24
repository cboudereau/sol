---
status: accepted
---
# Single tier-resolution choke point

Addresses: [FR1](../designs/rollup-read-routing.md#fr1), [FR3](../designs/rollup-read-routing.md#fr3), [NFR3](../designs/rollup-read-routing.md#nfr3)

## Problem
Rollup-tier routing is duplicated and inconsistent across the querier's metric handlers: `handle_range` (rate/agg) routes via `select_range_table`; the range histogram/heatmap handlers got a *second* copy (`tiered_hist_source`, `40149d8fa`); instant + metadata don't route at all. Each new handler re-implements, forgets, or mis-applies the routing — the structural cause of the write-ahead-of-read gap. Where should the routing live?

## Options
| Option | Pros | Cons |
|---|---|---|
| A. One `resolve_metric_windows(engine, start, end, resolution, capability) -> Vec<(table, lo, hi)>` that every metric path calls | Single place to reason about sealed/trailing split + tier eligibility; impossible to add a handler that silently bypasses (it has no raw/tier choice of its own); deletes the two existing copies | Touches every metric handler once; a slightly wider signature than `select_range_table` |
| B. Keep per-handler routing; just add it to the missing handlers | Smallest diff per handler | Keeps N copies of the sealed-boundary + tier-eligibility logic; the next handler forgets again; the operator-safety rule would have to be duplicated too |
| C. A trait/decorator wrapping table access | "Automatic" | Over-engineered for a single resolver call; obscures where routing happens; harder to pass per-query operator-safety |

## Decision
**Option A.** A single `resolve_metric_windows` returns the ordered, time-disjoint `(table, lo, hi)` source windows. `capability=None` ⇒ `[(metrics, start, end)]`; otherwise ⇒ tier for `[start, sealed_ns]` (coarsest tier ≤ resolution **carrying the capability**, per the [operator → capability ADR](./operator-safety-allowlist.md)) + raw for `(sealed_ns, end]`. All metric handlers build their scan from it; the per-handler `.table("metrics")` choices and both routing copies are removed.

## Consequences
- One correctness surface: the sealed-boundary split, tier eligibility, and (via FR2) operator safety are decided in exactly one place.
- New metric handlers must call the resolver to get a source — there is no per-handler raw/tier branch to get wrong.
- Deletes `select_range_table` (folded in) and `tiered_hist_source` (the `40149d8fa` copy) — net less code despite more call sites.
- Slightly larger blast radius for this change (every metric handler edited once), mitigated by per-shape parity tests (NFR3/NFR1).
