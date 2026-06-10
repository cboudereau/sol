---
status: draft
---
# Canonical normalizer location

Addresses: [FR2](../DESIGN.md#fr2)

## Problem

`prom_metric_name` + `unit_suffix` live in `src/querier/udf.rs` (the `sol` crate).
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
depends on `sol-core`, so the **codec** calls it at write time. The DataFusion
`ScalarUDF` wrapper (`prom_metric_name_udf`) and its catalog registration are
**deleted** — with the clean cutover the read path filters the stored `prom_name`
column and never normalizes at query time. The `normalize` key-name helper and
the JSON `prom_attr` UDF stay in `src/querier` (separate read-only concerns).

## Consequences

- Normalization runs once, at write; there is no read-time normalizer to drift
  from it.
- A test in `sol-core` pins the normalization rules (the existing `udf.rs` cases
  move there). `src/querier/udf.rs` loses `prom_metric_name`/`prom_metric_name_udf`
  and their tests.
- Codec gains a `sol-core` call at encode (already a dependency — no new dep).
- The ad-hoc SQL endpoint can no longer call `prom_metric_name()` (UDF gone).
