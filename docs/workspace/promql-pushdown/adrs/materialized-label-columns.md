---
status: proposed
---
# Materialized hot-label columns (endgame, clean cutover)

Addresses: [FR4](../DESIGN.md#fr4), [FR3](../DESIGN.md#fr3), [NFR5](../DESIGN.md#nfr5), [NFR6](../DESIGN.md#nfr6)

> **`proposed`** — the agent recommends the approach; the **human ratifies the hot-label set + the cutover** (the two hard-to-reverse choices) before this moves to `accepted`.

## Problem

Even with [aggregation-pushdown](./aggregation-pushdown.md), the `prom_group_key` UDF (and raw-selector materialization) still parses the `attributes` JSON per row, and grouping/filtering on a JSON-embedded label can never use Parquet row-group stats or bloom filters. The endgame is to materialize hot labels as **real columns** (like `prom_name`) so the key/filter is a prunable column op. The hard question: **which** labels, given attribute keys vary per metric and are unbounded?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Materialize **all** attribute keys as columns | No JSON parse ever | Unbounded distinct keys → schema explosion / thousands of sparse columns; non-viable |
| B. **Bounded, configured allowlist of hot labels** (e.g. `cpu`, `mode`, `le`, `host`, `http_route`, …); rest stay in `attributes` JSON | Prunable columns for the labels dashboards actually group/filter on; bounded schema; `prom_name` precedent | Deployment-specific list; a metric lacking a label → null column (cheap in Parquet) |
| C. Keep everything in JSON; rely only on the UDF transition | No write-side change | No pruning/bloom on labels; per-row parse remains for the UDF/raw path |

## Decision (proposed)

**Option B — a bounded configured allowlist.** The codec writes each allowlisted label as an `OPTIONAL UTF8` column in `common_metric_schema_fields()` (extracted from the data-point `attributes` via `OtelAttributes::get_string`), mirroring the `prom_name` materialization (schema field + write-path population + tests). The read side: `prom_group_key` and label predicates use the materialized column when the label is allowlisted (prunable), else fall back to `prom_attr(attributes, key)`. The allowlist is a codec/config constant to start (the dashboard's hot labels), revisited as needed.

**Open for human ratification:**
1. **The allowlist contents** (which labels) — start set proposed: `cpu`, `mode`, `le`, `host`, `host_name`, `job`, `http_route`, `http_response_status_code`. Adjust to the real dashboard workload.
2. **Clean cutover** ([NFR5](../DESIGN.md#nfr5)): regenerate the Parquet store; old files (no materialized columns) are not read/backfilled — identical to prom-name-column. Confirm acceptable (it is, since not in prod).

## Consequences

- **Easier:** grouping/filtering on hot labels becomes a prunable column op (row-group stats + bloom filters), the JSON parse disappears for those labels ([FR3](../DESIGN.md#fr3)/[NFR6](../DESIGN.md#nfr6)), and the sort key can extend to a hot label for better locality.
- **Harder:** write-side schema change + clean cutover (store regen); the allowlist is a maintained constant; a label promoted later requires another regen. This is why it's the **last** session and gated on ratification — FR1–FR3 deliver the perf wins without it.
