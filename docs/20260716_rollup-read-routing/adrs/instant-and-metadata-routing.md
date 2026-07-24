---
status: accepted
---
# Instant & metadata tier routing

Addresses: [FR4](../designs/rollup-read-routing.md#fr4), [FR5](../designs/rollup-read-routing.md#fr5)

## Problem
Beyond range queries, two metric read families read raw and never touch a tier: **instant** queries (`handle_instant`, instant `handle_histogram`) and **metadata** (`/series`, `/label/:name/values`). Should they route through the choke point, and on what resolution input?

Instant queries have no `step`; their "window" is the range selector (`m[w]`). Metadata computes no values — it enumerates names/labels over a time range.

## Options
| Decision point | Option | Pros | Cons |
|---|---|---|---|
| **Instant** | A. Route via the choke point using the **selector window** as the resolution input, gated by `op_capability` | Stat panels with a long window use the tier per the operator's capability (`rate`→Last, `max_over_time`→MinMax, …); recent/short or `None`-capability stays raw | Slightly more logic in the instant path |
| | B. Leave instant always-raw | Simplest | A 7-day Stat panel keeps scanning raw — the same cost the range fix removed |
| **Metadata** | A. Route sealed window → tier, trailing → raw | Tier has the **same series set** (rollup keeps every series with ≥1 sample/bucket) → enumeration is exact and cheaper (far fewer rows for `DISTINCT`) | Must confirm the tier carries the label columns enumerated (it does — same schema) |
| | B. Leave metadata raw | No change | Variable-dropdown / autocomplete queries over long ranges stay full-resolution |

## Decision
- **Instant → Option A.** `handle_instant`/`handle_histogram` resolve their source via the choke point, passing the range-selector window as the resolution and `op_capability(expr)` as the gate. A bare instant selector routes via the resolver too — capability `Last`, short window ⇒ resolves to raw naturally (no hardcoded `.table("metrics")` literal; keeps the no-bypass guard absolute).
- **Metadata → Option A.** `/series` and `/label/:name/values` route the sealed window to the tier (always tier-eligible — no value computation, so capability `Last` suffices; the rollup preserves the full series/label set), raw for the trailing window.

## Consequences
- Instant Stat panels with long *safe* windows get the tier speedup; recent and unsafe-operator instants are unchanged (raw).
- Metadata enumeration over long ranges scans the smaller tier for sealed days — cheaper variable refresh, identical result set.
- Both families now share the single resolver (FR1) — no separate routing logic to drift.
- Edge: an instant query whose selector window straddles the sealed boundary gets tier+raw windows merged by the handler's existing per-timestamp logic, like the range path — no double-count (windows are time-disjoint).
