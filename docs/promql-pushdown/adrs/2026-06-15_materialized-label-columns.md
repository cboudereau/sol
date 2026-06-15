---
status: accepted
---
# Columnar attributes — Arrow MAP (general); per-key columns deferred

Addresses: [FR4](../designs/2026-06-15_promql-pushdown.md#fr4), [FR3](../designs/2026-06-15_promql-pushdown.md#fr3), [NFR5](../designs/2026-06-15_promql-pushdown.md#nfr5), [NFR6](../designs/2026-06-15_promql-pushdown.md#nfr6)

> **`accepted`** (2026-06-13) — Approach A and the clean cutover ratified by the user; Session 3 cleared to run.

## Problem

Even after [aggregation-pushdown](./2026-06-15_aggregation-pushdown.md), the `prom_group_key` UDF and raw-selector materialization still parse the `attributes` **JSON string** per row, and a label buried in JSON can't use Parquet stats. The endgame is to store labels columnar so access is parse-free. The constraint: the optimization must be **general and production-ready**, not tuned to one workload's labels.

## Options

| Option | How | Kills per-row JSON parse? | Per-label prune/bloom? | Generality / cost |
|---|---|---|---|---|
| A. **`attributes` as Arrow `MAP<Utf8,Utf8>`** (dictionary-encoded) | codec writes a Parquet `MAP` column instead of a JSON string; read-side reads it columnar | **Yes — for every label** + dictionary compression | No (MAP is one column; stats mix keys) | **General**, light: no per-deployment config; one schema change |
| B. Dynamic per-key columns (InfluxDB IOx) | every label key seen for a metric → its own column; schema evolves per metric | Yes | **Yes** (per-label row-group min/max + bloom) | General but **heavy**: schema evolution, mixed-metric files → wide/sparse schemas, reader schema-union |
| C. Fixed hot-label allowlist | hardcode `cpu, mode, le, …` as columns | partial | only allowlisted | **Rejected** — workload-specific, demo-fitted, needs a regen per new hot label |

## Decision (proposed)

**Approach A — store `attributes` as a dictionary-encoded Arrow `MAP<Utf8,Utf8>` column.** It is general (no allowlist, no per-deployment tuning), kills the per-row JSON parse for **every** label, and compresses better (dictionary). The codec serializes the data-point attributes to a `MAP` instead of a JSON string (the `resource_attributes`/`scope_attributes`/exemplars JSON blobs are out of scope — only the queried `attributes` column). Read-side: `prom_group_key`/`prom_attr` read the `MAP` columnar (a map-access expression or a small UDF over the map array), **never** `serde_json::from_str`. Clean cutover — regenerate the store; old JSON-attribute files are not read ([NFR5](../designs/2026-06-15_promql-pushdown.md#nfr5)).

**Deferred — Approach B (per-key columns).** The full pruning endgame (per-label row-group stats + bloom filters) targets **high-cardinality label-*value* filtering** — a use case that is **not a measured bottleneck** here (the measured pains are grouping/materialization, which A fully addresses). Building per-key dynamic columns now is speculative complexity (YAGNI). Documented as the future option, to be revisited **only** when label-value filtering is measured as a cost.

**Open for ratification:** the clean cutover (regenerate store, no backfill — same as prom-name-column) and the go to run Session 3.

## Consequences

- **Easier:** label access is parse-free and columnar for all labels (kills the residual of [FR3](../designs/2026-06-15_promql-pushdown.md#fr3)/[NFR6](../designs/2026-06-15_promql-pushdown.md#nfr6)); dictionary compression shrinks the store; no workload-specific config to maintain; the read primitive is uniform (no JSON vs column branching).
- **Harder / to verify:** a write-side schema change + clean cutover (store regen). The load-bearing unknown is **how DataFusion reads a Parquet `MAP` for key extraction** — confirm `datafusion-functions-nested` map access vs a small UDF over the `MapArray` (pinned in Task 6). MAP gives no per-label pruning; if label-value filtering ever needs it, that's Approach B (deferred), not a regression of A.
