---
status: accepted
---
# Pipeline-internal state as struct fields, not attributes

Addresses: OTLP output purity — zero custom extensions

## Problem

Some pipeline operations require state that has no OTLP equivalent: set merge needs the original string values (not just cardinality), and gauge delta handling needs to know whether a gauge is incremental. The original implementation stored this state as `vector.*` OTLP attributes (`vector.set_values`, `vector.metric_kind`, `vector.metric_type`, `vector.statistic`). These attributes leak into OTLP output if exported via the opentelemetry sink — backends see non-standard attributes.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Keep `vector.*` attributes as exceptions | No structural change | Leaks into OTLP output; backends see non-standard attributes; fragile string-based coupling |
| B. Dedicated struct fields on `OtelMetric` | Never serialized to OTLP; type-safe; pipeline-internal by construction | Struct grows slightly; fields must be preserved through disk buffer |
| C. Drop capabilities entirely (no set merge, no gauge delta) | Purest OTLP | Loss of features that users depend on |

## Decision

Option B — dedicated struct fields on `OtelMetric` (D40, D53).

Two fields added:
- `set_values: Option<BTreeSet<String>>` — stores unique values for set merge. `BTreeSet` deduplicates by construction (repeated values cost O(1) per insert). Gauge numeric value = `set_values.len()`, recomputed on read. Only read by the aggregate transform's merge logic. Never serialized to OTLP proto.
- `kind_override: Option<MetricKind>` — stores incremental/absolute for Gauge and Summary types that lack `aggregation_temporality` in OTLP. Read by `kind()` and `set_kind()`. Never serialized to OTLP proto.

Both fields are preserved through the disk buffer (G6).

All `VECTOR_*` constants and `vector.*` prefix filtering deleted.

## Consequences

- Zero `vector.*` attributes exist anywhere in the codebase
- OTLP output is pure — no custom extensions for any backend
- Set merge and gauge delta functionality preserved
- Sinks branch on `MetricView` proto types directly instead of reading `distribution_statistic()` or attribute strings
