---
status: accepted
---
# Rename metrics namespace from `vector` to `sol`

Addresses: [NFR1](../../sol-telemetry-monitoring/designs/20260504_sol-telemetry-monitoring.md#nfr1)

## Problem

SOL is a fork of Vector with its own identity. All internal metrics currently use the `vector` namespace prefix (e.g., `vector_component_received_events_total`). SOL should use `sol_*` metrics to distinguish itself from upstream Vector for two reasons:

1. **Identity**: SOL is a distinct product (see [OTEL_MIGRATION_PLAN.md](../../otlp-as-core-protocol-plan/OTEL_MIGRATION_PLAN.md)) with its own core protocol (OTLP-native), its own transforms (tail_sampling, span_metrics), and its own conventions (`service.name=sol/*`).

2. **Mixed-fleet disambiguation**: The migration plan (Strategy 2) explicitly supports mixed fleets where original Vector instances send data to SOL via the `vector` source's native protocol adapter. During this transition period, both Vector and SOL instances emit internal metrics to the same Mimir. If both use `vector_*` metrics, operators cannot distinguish which metrics come from SOL vs original Vector in dashboards and alerts. Using `sol_*` makes the source unambiguous.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Change the default namespace in `recorder.rs` and `internal_metrics.rs` from `"vector"` to `"sol"`, and strip `vector_` prefix from custom metric names | Clean identity; small code change; dashboards query `sol_*` natively | Diverges from upstream Vector; custom metrics need manual renaming |
| B. Use `internal_metrics` config `namespace: sol` per-instance (no code change) | No code change; per-instance flexibility | Custom metrics like `vector_tail_sampling_*` would become `sol_vector_tail_sampling_*` (double prefix); doesn't address the root cause |
| C. Add a VRL remap transform to rename metrics in the self-monitoring pipeline | No code change; flexible | Fragile string manipulation; performance overhead; must maintain rename rules as new metrics are added |

## Decision

Option A — change the default namespace at the registry level and fix custom metric names. The change is small (5 files, ~10 lines) and gives SOL a clean identity.

Specifically:
1. `lib/vector-core/src/metrics/recorder.rs` — change `"vector"` to `"sol"` in `visit_metrics()` (3 occurrences)
2. `lib/vector-core/src/metrics/mod.rs` — change `"vector"` to `"sol"` in `capture_metrics()` (2 occurrences)
3. `src/sources/internal_metrics.rs` — change `default_namespace()` from `"vector"` to `"sol"`, update the namespace check and test
4. Strip `vector_` prefix from custom metric names (the registry adds the `sol_` prefix):
   - `src/transforms/tail_sampling/transform.rs` — `"vector_tail_sampling_*"` → `"tail_sampling_*"`
   - `src/sinks/opentelemetry/grpc.rs` — `"vector_lb_*"` → `"lb_*"`
   - `src/sinks/opentelemetry/load_balancing.rs` — `"vector_lb_*"` → `"lb_*"`

## Consequences

- All internal metrics emitted by SOL will use the `sol_*` prefix
- In mixed fleets, operators can filter by `sol_*` vs `vector_*` to distinguish SOL from original Vector instances
- Existing Vector dashboards or tooling that queries `vector_*` metrics will not match SOL metrics — SOL gets its own dashboards
- The `internal_metrics` source still supports `namespace` config override for customization
- Custom metrics registered via `counter!()` / `gauge!()` / `histogram!()` must NOT include a namespace prefix in their name (the registry adds it)
- Convention: all new SOL-specific metrics (e.g., in new transforms) must use bare names without any prefix — the registry prepends `sol_`
