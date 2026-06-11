---
status: draft
---
# Relational / non-relational boundary

Addresses: [FR5](../DESIGN.md#fr5), [NFR2](../DESIGN.md#nfr2)

## Problem

"Push the relational core into DataFusion" needs a hard line, or the migration scope-creeps into transpiling all of PromQL to relational algebra — exactly what Prometheus/Mimir avoided by writing a bespoke engine. Which constructs are pushed to DataFusion, and which stay in the thin Rust shell, and why?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Push *everything* (incl. vector matching, histogram_quantile, subqueries) into plans | Maximal engine leverage | PromQL semantics (staleness, NaN/absent, exact rate extrapolation, `group_left/right`, subqueries) don't map cleanly to SQL → fidelity loss or plan explosion |
| B. **Push the relational core; keep a thin Rust shell for the non-relational tail** | Engine leverage where it pays; preserves exact PromQL semantics for the hard parts; matches Mimir's own split | Two layers to maintain; a documented contract needed to stop drift |
| C. Keep status quo (mostly Rust) | No work | Leaves both measured pain points |

## Decision

**Option B**, with this explicit, enforced contract:

**Pushed to DataFusion (relational core):**
- vector selectors (already), `rate`/`irate`/`increase` (already, LAG window), `<agg>_over_time` (already, RANGE-frame window);
- grouping + aggregation `sum/min/max/avg/count` with `by`/`without`/nesting ([aggregation-pushdown](./aggregation-pushdown.md));
- `topk`/`bottomk` (`ROW_NUMBER` window);
- `clamp_min`/`clamp_max` and scalar∘vector arithmetic that is a plain per-row column op.

**Stays in the Rust shell (non-relational tail):**
- `histogram_quantile` over OTLP bucket arrays — already Rust-native (interpolation over `bucket_counts`/`explicit_bounds`); no relational gain;
- vector matching `on/ignoring/group_left/group_right` — many-to-one with label-set keying + cardinality checks; **rabbit hole**, not a measured cost;
- `scalar()` folding, staleness/NaN/absent semantics, and the step-grid resample (`resample_to_grid`);
- subqueries `[5m:1m]`, `@`, `offset` — currently unsupported; if added, they live here.

The line: a construct is pushed **iff** it is expressible as scan/group-by/window/scalar over the canonical-schema frame *without* losing PromQL semantics; otherwise it stays Rust.

## Consequences

- **Easier:** scope is bounded — the migration touches grouping/aggregation/topk/clamp, not the semantic tail; parity ([NFR2](../DESIGN.md#nfr2)) is preserved because the hard-semantics paths are untouched.
- **Harder:** the boundary must be defended in review — new PromQL features get classified against this contract, not pushed reflexively. A construct that "almost" fits relational algebra (vector matching) is deliberately left Rust to avoid fidelity loss.
