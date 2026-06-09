---
status: draft
---
# Canonical normalizer location

Addresses: [FR2](../DESIGN.md#fr2)

## Problem

`prom_metric_name` + `unit_suffix` live in `src/query/udf.rs` (the `sol` crate).
The Parquet codec (`lib/codecs`) must compute the same value at write time, but
`lib/codecs` cannot depend on the `sol` crate (it is a lower-level dependency).
Where does the single source of truth live so write and read agree forever?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Duplicate the fn in `lib/codecs` | trivial | two copies drift → stored name ≠ queried name (silent data corruption) |
| B. Move the pure fns to `lib/sol-core` | one source of truth; `sol-core` already owns `OtelMetric` (the codec + query both use it); no new crate | small cross-crate move; `udf.rs` re-exports/wraps |
| C. New `lib/prom-naming` crate | clean boundary | new crate + Cargo wiring for a ~40-line fn — overkill |

## Decision

**Option B.** Move `prom_metric_name` and `unit_suffix` (pure, dependency-free
string fns) into `lib/sol-core` next to `OtelMetric`. `lib/codecs` already
depends on `sol-core`, and the `sol` crate does too, so both write and read call
the identical function. The DataFusion `ScalarUDF` wrapper
(`prom_metric_name_udf`) stays in `src/query/udf.rs` and delegates to the moved
fn (it is the FR6 fallback + ad-hoc SQL surface). The `normalize` key-name helper
and JSON `prom_attr` UDF stay in `src/query` (read-only concerns, out of scope).

## Consequences

- One normalizer; stored `prom_name` and the read-time UDF can never diverge.
- A test in `sol-core` pins the normalization rules (the existing `udf.rs` cases
  move/copy there); `udf.rs` keeps a thin wrapper test.
- Codec gains a `sol-core` call at encode (already a dependency — no new dep).
