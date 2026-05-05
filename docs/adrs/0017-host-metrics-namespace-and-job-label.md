---
status: accepted
---
# Host metrics namespace and job label strategy

Addresses: [FR1](../designs/20260505_node-exporter-dashboard-demo.md#fr1), [FR2](../designs/20260505_node-exporter-dashboard-demo.md#fr2), [NFR1](../designs/20260505_node-exporter-dashboard-demo.md#nfr1)

## Problem

Sol's `host_metrics` source uses a configurable `namespace` stored as an OTLP resource attribute (`metric.namespace`). The reference Node Exporter Full dashboard (Grafana ID 1860, rev 45) queries `node_*` metrics with `{host="$host", job="$job"}` filters. Three decisions:

1. **Metric name prefix**: how to get the `node_` prefix on metric names in Mimir?
2. **Job label**: how to produce a `job` label in Prometheus/Mimir?
3. **Dimensionless units**: how to prevent Mimir from adding `_ratio` suffix to metrics like `uptime` and `load1`?

## Options

### Metric name prefix

| Option | Approach | Metric example in Mimir | Notes |
|---|---|---|---|
| A. `namespace: "node"` | Uses OTLP resource attribute `metric.namespace` | `cpu_seconds_total` (namespace not in metric name) | **Does not work** — OTLP has no namespace concept in metric names; the `namespace` config stores it as a resource attribute, not a metric name prefix |
| B. `namespace: ""` + VRL remap | Set empty namespace, use `.name = "node_" + string!(.name)` in remap transform | `node_cpu_seconds_total` | Works — directly modifies the OTLP metric name |

### Job label

| Option | Approach | Result in Mimir |
|---|---|---|
| X. VRL `.attributes."job" = "sol"` | Add `job` as a data point attribute | Creates a `job` metric-level label, but Mimir's OTLP translation also adds `job` from `service.name` — potential conflict |
| Y. `service.name: sol` resource attribute | Rely on Mimir's `service.name` → `job` mapping | `job=sol` label automatically — standard OTLP-to-Prometheus mapping |

### Dimensionless units

| Option | Approach | Result |
|---|---|---|
| P. `infer_unit()` returns `"1"` for dimensionless | OTLP convention for dimensionless ratio | Mimir with `otel-metric-suffixes-enabled` adds `_ratio` suffix → `node_uptime_ratio`, `node_load1_ratio` |
| Q. `infer_unit()` returns `""` for dimensionless | Empty unit string | Mimir adds no suffix → `node_uptime`, `node_load1` |

## Decision

**Metric name prefix: Option B** — `namespace: ""` with VRL remap `.name = "node_" + string!(.name)`. Option A was the initial plan, but implementation revealed that Sol's `namespace` config stores the value as an OTLP resource attribute (`metric.namespace`), which does not appear in the Prometheus metric name after OTLP→Prometheus translation.

**Job label: Option Y** — `service.name: sol` resource attribute. Mimir's OTLP translation maps `service.name` → `job` Prometheus label automatically. This is the standard mapping and avoids adding a redundant data point attribute.

**Dimensionless units: Option Q** — `infer_unit()` returns `""` instead of `"1"`. The OTLP spec says unit `"1"` means dimensionless ratio, but Mimir interprets this literally and appends `_ratio` to the metric name. Returning an empty unit string avoids this.

## Consequences

- `node_*` metric names in Mimir match the Node Exporter convention and are distinct from `sol_*` internal metrics
- The VRL remap approach requires an explicit transform step but is transparent and visible in the gateway config
- `service.name: sol` produces `job=sol` via standard OTLP-to-Prometheus mapping — no custom attribute manipulation needed
- The `infer_unit()` change in `src/sources/host_metrics/mod.rs` is a source code modification, not just config — but it fixes a real naming issue where Mimir would produce unusable metric names like `node_uptime_ratio`
- The `sources-host_metrics` Cargo feature must be included in the Docker build (`demo/Dockerfile.sol` FEATURES arg)
