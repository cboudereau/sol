---
status: accepted
---
# Group-key canonical format

Addresses: [FR1](../designs/2026-06-15_promql-pushdown.md#fr1), [FR3](../designs/2026-06-15_promql-pushdown.md#fr3)

## Problem

The [aggregation-pushdown](./2026-06-15_aggregation-pushdown.md) plan groups on a single string column produced by `prom_group_key(...)`. That string is both (a) the `GROUP BY` key and (b) the serialized result label set parsed back once per output group. Its format is therefore **load-bearing and hard to change** once plans depend on it. What is the canonical format, and what are the escaping/ordering rules so two rows of the same series always collide and two different series never do?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. `serde_json` object of the kept labels | Reuses a parser; familiar | Ambiguous key order unless sorted; heavier to build/parse per group; quoting overhead |
| B. **Sorted `k=v` joined by a control separator, values length-or-escape-delimited** | Compact; deterministic; trivial to build in the UDF and split per group; collision-safe with escaping | Bespoke format to document + test |
| C. Hash of the sorted labels (e.g. u64) | Tiny key | Not reversible → must carry labels separately → back to per-row materialization; collisions possible |

## Decision

**Option B.** `prom_group_key` emits sorted `key=value` pairs joined by `\x1f` (unit separator), with `=`/`\x1f` in keys/values escaped (or a length-prefix per pair) so the string is unambiguously reversible. Rules:
- Keys are the **kept** labels for the grouping: `by(L)` keeps `L∩present`; `without(L)` keeps all present labels except `L` and `__name__`; no-modifier keeps none (constant key `""`).
- Keys are sorted (`BTreeMap` order) so the same series always yields the same key regardless of source row order.
- The set unions promoted label columns (`service_name`, materialized label columns) with the `attributes` JSON keys (normalized via the existing `udf::normalize`), promoted columns winning on collision — identical to today's `LabelCols::labels` semantics, so parity holds.
- The reverse (`parse_group_key`) is applied once per output group to build the response `BTreeMap`.

### Reprojection — the canonical aggregate frame (handles mixed nesting)

Every **aggregated** relational node emits a uniform frame: `[prom_group_key: Utf8, v: Float64, (time_unix_nano)]`. The `prom_group_key` column **is** the kept-label set, and because the format is reversible, an *outer* aggregate re-projects it without ever touching the raw `attributes` JSON:

- `prom_group_key(attributes, promoted_cols, mode, labels)` — for a **leaf** inner (selector / `rate` / `over_time`, which carries `attributes` + promoted columns).
- `prom_group_key_reproject(inner_key, mode, labels)` — for a **nested** inner (another aggregate, which already carries a `prom_group_key` column): `parse(inner_key) → build(parsed, grouping)`. Both share the `GroupKey::{build,parse}` core.

This makes **mixed nesting** expressible and correct, e.g. `sum by (cpu) (sum without (mode) (m))`: the inner emits a `prom_group_key` for all-labels-except-`mode` (including `cpu`); the outer applies `prom_group_key_reproject(inner_key, by, [cpu])` and groups on the result. Without reprojection, an outer `by(cpu)` could not recover `cpu` from an opaque inner key — this is the gap the primitive closes.

## Consequences

- **Easier:** building the key is a single pass in the UDF; reconstructing labels is per-group, not per-row ([FR3](../designs/2026-06-15_promql-pushdown.md#fr3)); deterministic grouping ([NFR2](../designs/2026-06-15_promql-pushdown.md#nfr2) parity).
- **Harder:** the format is frozen — changing it invalidates nothing on disk (it's computed at query time) but must stay consistent between the UDF and `parse_group_key`. A single round-trip unit test (`build == parse⁻¹`) guards it. The separator choice (`\x1f`) assumes labels never contain control chars; values are escaped to be safe.
