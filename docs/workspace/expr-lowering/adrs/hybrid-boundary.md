---
status: draft
---
# Hybrid boundary: which queries migrate to `Expr` vs stay SQL

Addresses: [FR3](../DESIGN.md#fr3), [FR6](../DESIGN.md#fr6), [NFR2](../DESIGN.md#nfr2)

## Problem

Not every lowering benefits equally from the DataFrame/`Expr` API. Window functions
and Arrow-native compute are clearer (and already tested) as SQL strings / Rust.
Where exactly is the migration boundary?

## Options

| Option | Pros | Cons |
|---|---|---|
| Migrate **everything** to `Expr` (incl. windows) | One uniform path | `rate`/`*_over_time`/instant `ROW_NUMBER` are very verbose via `Expr::WindowFunction`; high risk on tested code; little benefit |
| Migrate **nothing**, only extract a shared *string* predicate helper | Smallest change | Keeps the injection surface; gives up type-safety; misses the native-IR win |
| **Hybrid**: migrate filter/projection/group-by/distinct; keep window + array compute | Best benefit/risk; predicate builder reused everywhere (via unparser for SQL paths) | Two execution paths to maintain (`collect(plan)` + `sql(text)`) |

## Decision

**Hybrid.** Migrate to `Expr`/DataFrame:
- TraceQL search; LogQL streams + volume; discovery endpoints (labels, label values,
  series, tags, tag values, index stats/volume) — all are filter + project + (group-by
  / distinct), no windows.

Keep as SQL string (or Rust-native), unchanged:
- PromQL `metric_base`/`rate`/`<agg>_over_time`/instant latest-per-series, `sum by`
  / `topk` over windows — `Expr::WindowFunction` is too verbose/risky.
- `histogram_quantile`, classic-bucket heatmap, byte-volume — already Arrow-native.

The shared predicate builder ([FR1](../DESIGN.md#fr1)) is reused by the SQL-staying
paths via `Expr` unparsing where it removes duplication; otherwise those WHERE
clauses stay hand-written (see [cache-and-unparser ADR](./cache-and-unparser.md)).

## Consequences

**Easier**: the high-overlap, high-risk-of-injection filter layer is unified and
safe; window code is left alone (no regression risk).

**Harder**: two lowering styles coexist; the boundary must be documented (the design
matrix) so future work knows which side a query is on.
