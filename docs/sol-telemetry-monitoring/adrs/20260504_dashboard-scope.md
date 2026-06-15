---
status: accepted
---
# SOL-native dashboards vs reusing OTel Collector dashboards

Addresses: [FR2](../designs/20260504_sol-telemetry-monitoring.md#fr2), [NFR1](../designs/20260504_sol-telemetry-monitoring.md#nfr1)

## Problem

The existing Grafana dashboards are built for OTel Collector Contrib's `otelcol_*` metric namespace and receiver/processor/exporter terminology. SOL (Vector) uses `vector_component_*` metrics with source/transform/sink terminology. Should we adapt Vector's metrics to match, or build SOL-native dashboards?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Build SOL-native dashboards using SOL's `sol_*` metrics | Natural fit for Vector's model; SOL identity; can show SOL-specific metrics like tail_sampling policies | Cannot reuse existing OTel community dashboards |
| B. Add a VRL remap to rename `vector_*` → `otelcol_*` in the self-monitoring pipeline | Reuse existing dashboards as-is | Lossy mapping (Vector concepts don't 1:1 map); confusing naming; maintenance burden; remap adds overhead |
| C. Add Rust code to emit `otelcol_*` metrics natively | Perfect OTel compatibility | Significant code changes; dual naming confusion; upstream merge conflicts |

## Decision

Option A — build SOL-native dashboards. SOL is a distinct product, not an OTel Collector drop-in. SOL's `sol_*` metric names (see [ADR: metrics namespace renaming](../../rename-vector-to-sol/adrs/20260504_metrics-namespace-renaming.md)) are well-defined and the dashboard can show SOL-specific metrics (tail sampling policy decisions, span_metrics) that have no OTel Collector equivalent. The `component_id`/`component_kind`/`component_type` label system is actually richer than OTel's receiver/processor/exporter split.

## Consequences

- New dashboard JSON must be authored and maintained
- Operators familiar with OTel Collector dashboards need to learn SOL's `sol_*` metric names (documented in the dashboard panel descriptions)
- Future SOL-specific features (e.g., servicegraph metrics) integrate naturally into the SOL dashboard
