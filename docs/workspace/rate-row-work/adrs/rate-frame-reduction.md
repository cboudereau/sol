---
status: accepted
---
# rate() frame reduction: minimal window set, golden-gated

Addresses: [FR1](../DESIGN.md#fr1), [NFR1](../DESIGN.md#nfr1), [NFR3](../DESIGN.md#nfr3)

## Problem

`frame.rs::rate` (`src/querier/plan/frame.rs:183-302`) runs LAG + six RANGE-frame window passes per series partition. Execution is the dominant live cost (~835 ms). Each window pass is a separate scan+accumulate over the partition; fewer passes = less execute time, first-shape and ad-hoc included (unlike the plan cache). The extrapolation math is Prometheus-exact and was hard-won across two prior workspaces — any reduction must be bit-identical within the 1e-6 golden tolerance.

## The six passes and their reducibility (explorer-confirmed)

| Window | Used for | Reducible? |
|---|---|---|
| `SUM(delta)` | in-window increase accumulator | **Keep** — irreducible |
| `FIRST_VALUE(delta)` | subtract leading-row delta (reaches before window) | leading row of ASC frame |
| `FIRST_VALUE(v)` | counter zero-clamp `first_value/result` | leading row of ASC frame — **share the pass with FIRST_VALUE(delta)** |
| `MIN(t)` (`first_t`) | `window_start`, `sampled_interval` | = leading row's `t` on ASC frame → **FIRST_VALUE(t)**, fuse |
| `MAX(t)` (`last_t`) | frame end, `duration_to_end` | = current row's `t` (frame ends at CURRENT ROW) → **plain `t`, no window** |
| `COUNT(v)` | `avg_gap`, `cnt<2→NULL` guard | **Keep** — irreducible |
| `duration_to_end` (derived) | `cap()`, `factor` | provably **0** (`last_t = current row t`) → **drop the term** |

## Options

| Option | Passes after | Risk |
|---|---|---|
| A. Full fusion: SUM + COUNT + one FIRST_VALUE-group (delta, v, t) over the shared frame; `last_t` = current-row `t` (no window); drop `duration_to_end` | ~3 window passes (from 7 incl. LAG) | DF 53 may not co-locate three FIRST_VALUE columns in one plan node — falls back to B |
| B. Conservative: drop `duration_to_end` (free) + replace `MIN(t)`/`MAX(t)` with FIRST_VALUE(t)/current-row t; keep the two FIRST_VALUE passes separate | ~5 passes | Minimal; strictly a subset of A |
| C. Custom UDWF computing all frame quantities in one accumulator pass | 1 pass + LAG | Highest payoff, highest risk (new UDWF, re-derives the extrapolation inside Rust) — rabbit-hole cap says no |

## Decision

**Attempt A, fall back to B by what DataFusion 53 fuses** — the golden tests decide correctness, the re-profile decides whether the achieved reduction is enough or FR2/FR3 are still needed. C rejected (custom UDWF re-implements the hard-won extrapolation; the parity history is the reason to keep the formula in DataFrame expressions, not Rust). `duration_to_end`-drop and `MAX(t)`→current-row-`t` are free and taken regardless.

## Outcome (2026-07-20, task 2 re-profile)

Post-FR1 release fixture bench: rate() cold 47.1 ms / warm 26.9–29.5 ms; **execute 5–9 ms (was ~68 ms pre-FR1)** — the 6→5 window reduction cut execute ~7×; physical (~20 ms) is now the dominant stage. All ≤ 80 ms on the fixture → FR2/FR3 (series-key column + sort-pushdown) deferred pending live verification; reopen if live misses (the fixture mispredicted live ~15× at promql-plan-cache T2b).

## Consequences

- `frame.rs::rate` shrinks; the extrapolatedRate arithmetic (frame.rs:243-295) is unchanged in meaning, only its input windows are fewer. Every golden/parity test named in [NFR3](../DESIGN.md#nfr3) must stay green bit-for-bit.
- No schema change, no wipe, revertable commit — ships and re-profiles before the heavier FR2/FR3.
- If A/B save little (fusion limited), that is recorded and the execute-stage win shifts to FR2 (UDF removal) + FR3 (sort elision).
