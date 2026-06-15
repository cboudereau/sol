# sol-telemetry-monitoring — Design Doc

## Context

The SOL demo replaces OTel Collector Contrib with Vector for the full traces pipeline (gateway → load balancer → tail sampling → Tempo). Steps 1–3 are complete, but the SOL pipeline currently has **no self-monitoring**: there is no equivalent of the OTel Collector's built-in telemetry that powers the existing Grafana dashboards ("OpenTelemetry Collector" and "OpenTelemetry Collector HostMetrics").

The OTel Collector Contrib setup monitors itself through:
1. **Pipeline telemetry** (`otelcol_receiver_*`, `otelcol_processor_*`, `otelcol_exporter_*`) — spans/metrics/logs accepted, refused, dropped per receiver/processor/exporter
2. **Process telemetry** (`otelcol_process_*`) — CPU, memory, uptime of the collector process
3. **Host metrics** (`system_cpu_*`, `system_memory_*`, etc.) — host-level resource usage via the `hostmetrics` receiver

Each OTel Collector instance runs a self-monitoring agent pipeline that scrapes its own Prometheus endpoint (`:8888`) and host metrics, then exports to Mimir.

Vector already has the building blocks:
- `internal_metrics` source — scrapes Vector's own metrics registry (e.g. `component_received_events_total`, `component_sent_events_total`, `component_errors_total`, `component_received_bytes_total`, `component_sent_bytes_total`, `vector_tail_sampling_*`)
- OpenTelemetry sink — can export metrics as OTLP to Mimir
- The metrics are tagged with `component_id`, `component_type`, `component_kind`

**The gap**: SOL's internal metrics currently use the `vector_*` namespace (inherited from Vector), not the OTel Collector `otelcol_*` convention. The existing OTel Collector dashboards cannot be reused as-is. Additionally:
- The SOL configs don't include any self-monitoring pipeline today
- In mixed fleets (original Vector → SOL via native protocol, per [OTEL_MIGRATION_PLAN.md Strategy 2](../../otlp-as-core-protocol-plan/OTEL_MIGRATION_PLAN.md)), both products would emit identically-named `vector_*` metrics to the same Mimir, making them indistinguishable in dashboards

SOL needs its own `sol_*` metrics namespace and SOL-native dashboards.

## Functional Requirements

### <a id="fr1"></a>FR1 — Self-monitoring pipeline for each SOL instance

Each SOL instance (sol-gateway, sol-loadbalancer, sol-collector) must include a self-monitoring pipeline that:
- Scrapes its own internal metrics via the `internal_metrics` source
- Enriches metrics with identifying attributes (`service.name`, `service.namespace`, `host.name`)
- Exports metrics to Mimir via OTLP HTTP

This mirrors the `pipeline.agent.yml` pattern from the OTel Collector Contrib setup.

### <a id="fr2"></a>FR2 — SOL pipeline monitoring Grafana dashboard

Create a Grafana dashboard that visualizes the SOL pipeline health, equivalent to the "OpenTelemetry Collector" dashboard. It must show:

1. **Signal flows** — spans received vs sent per instance (the receiver→exporter ratio)
2. **Per-component throughput** — events received and sent per component (`component_id`), broken down by type (source/transform/sink)
3. **Tail sampling metrics** — traces sampled vs dropped, by policy
4. **Error rates** — `component_errors_total` per component
5. **Data flow graph** — node graph visualization showing source→transform→sink topology

The dashboard queries SOL's metric names (`sol_component_received_events_total`, etc.).

### <a id="fr3"></a>FR3 — SOL process and resource monitoring

Either extend the SOL pipeline dashboard or create a companion dashboard showing:

1. **Process metrics** — CPU, memory, uptime of each SOL instance (from Vector's internal process metrics or from host metrics if available)
2. **Per-instance resource utilization** — to identify which sol-collector replica is overloaded

This is the equivalent of the "OpenTelemetry Collector HostMetrics (Node Exporter)" dashboard.

### <a id="fr4"></a>FR4 — RED metrics for application health (span_metrics)

The `span_metrics` transform already generates per-service RED metrics. The existing "OpenTelemetry dotnet webapi" dashboard in `Apps/` should continue to work. Verify that the self-monitoring pipeline does not interfere with the application metrics pipeline.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Follow OpenTelemetry conventions where possible

Resource attributes on self-monitoring metrics must follow OTel semantic conventions:
- `service.name` = `sol/gateway`, `sol/loadbalancer`, `sol/collector`
- `service.namespace` = component role (e.g., `gateway`, `loadbalancer`, `collector`)
- `host.name` = hostname of the container

Metric names use the `sol` namespace prefix (`sol_component_*`, `sol_tail_sampling_*`) — see [ADR: metrics namespace renaming](../../rename-vector-to-sol/adrs/20260504_metrics-namespace-renaming.md).

### <a id="nfr2"></a>NFR2 — Minimal overhead

The self-monitoring pipeline must not significantly impact the data pipeline's throughput. Internal metrics scrape interval should be configurable (default 15s matching the OTel Collector pattern).

### <a id="nfr3"></a>NFR3 — Rename metrics namespace from `vector` to `sol`

Change the default metrics namespace from `"vector"` to `"sol"` so all internal metrics are emitted as `sol_*` (e.g., `sol_component_received_events_total`, `sol_tail_sampling_traces_sampled`). This requires a small code change — see [ADR: metrics namespace renaming](../../rename-vector-to-sol/adrs/20260504_metrics-namespace-renaming.md).

Custom metrics registered via `counter!()` must NOT include a namespace prefix in their name — the registry adds it. Existing custom metrics like `vector_tail_sampling_*` must be renamed to `tail_sampling_*`.

## Non-goals

- **Renaming metrics to `otelcol_*`** — SOL has its own identity. Metrics use `sol_*`, not `otelcol_*`.
- **Host metrics collection** — Vector does not have a `hostmetrics` receiver equivalent. Host-level monitoring (CPU, memory, disk, network) would require running a sidecar OTel Collector with `hostmetrics` receiver or using a different host agent. This is deferred to a separate workspace.
- **Reusing the existing OTel Collector Contrib dashboards** — they query `otelcol_*` metrics and assume receiver/processor/exporter terminology. Building SOL-native dashboards is cleaner and takes advantage of Vector's source/transform/sink model.
- **Alerting rules** — deferred. Dashboards come first; alerting rules will be added once the metrics and thresholds are validated in practice.

## Rabbit holes

- **Dashboard JSON authoring complexity**: Grafana dashboard JSON is verbose and error-prone to author by hand. Cap: use a minimal dashboard with essential panels only, no deep customization. Focus on the key metrics that operators need.
- **Metric cardinality explosion**: Vector emits metrics per `component_id`. With many components, this could create high cardinality. Cap: the SOL demo has a small number of components, so this is not a concern. Document the risk for production use.

## Design

### Self-monitoring pipeline architecture

Each SOL instance adds an `internal_metrics` source and an `opentelemetry` sink to export its own metrics to Mimir:

```
┌──────────────────────────────────────────┐
│ sol-gateway                              │
│                                          │
│  ┌──────────────┐   ┌────────────────┐   │
│  │ otlp source  │──▶│ sinks (traces, │   │
│  │              │   │ logs, metrics) │   │
│  └──────────────┘   └────────────────┘   │
│                                          │
│  ┌──────────────┐   ┌────────────────┐   │
│  │ internal_    │──▶│ otlp sink      │──▶│ Mimir
│  │ metrics      │   │ (self-monitor) │   │
│  └──────────────┘   └────────────────┘   │
└──────────────────────────────────────────┘
```

### Vector internal metrics → OTel Collector metrics mapping

| OTel Collector Metric | SOL Equivalent | Notes |
|---|---|---|
| `otelcol_receiver_accepted_spans_total` | `sol_component_received_events_total{component_kind="source"}` | Filtered by component_id for trace sources |
| `otelcol_receiver_refused_spans_total` | `sol_component_errors_total{component_kind="source"}` | Approximation |
| `otelcol_processor_incoming_items_total` | `sol_component_received_events_total{component_kind="transform"}` | Per-transform |
| `otelcol_processor_outgoing_items_total` | `sol_component_sent_events_total{component_kind="transform"}` | Per-transform |
| `otelcol_exporter_sent_spans_total` | `sol_component_sent_events_total{component_kind="sink"}` | Per-sink |
| `otelcol_exporter_send_failed_spans_total` | `sol_component_errors_total{component_kind="sink"}` | Per-sink |
| `otelcol_process_uptime` | `sol_uptime_seconds_total` | Process uptime |

This mapping is for reference only — the SOL dashboard queries `sol_*` metrics directly.

### Dashboard structure

**SOL Pipeline dashboard** (equivalent to "OpenTelemetry Collector"):

The dashboard is organized by signal, mirroring the OTel Collector dashboard pattern (separate panels for Spans, Metric Points, Log Records). Each signal shows the same rate metrics at each pipeline stage, making it easy to spot where data is lost.

**Row 1 — Signal flows (stat + node graph)**

Per-signal overview showing the end-to-end received/sent ratio:

| Panel | Traces | Metrics | Logs |
|---|---|---|---|
| Received rate | `sol_component_received_events_total` on `otlp.traces` source | `sol_component_received_events_total` on `otlp.metrics` source | `sol_component_received_events_total` on `otlp.logs` source |
| Sent rate | `sol_component_sent_events_total` on trace sinks | `sol_component_sent_events_total` on metric sinks | `sol_component_sent_events_total` on log sinks |
| Ratio | sent / received (%) | sent / received (%) | sent / received (%) |

Each signal also gets a node graph panel showing source → transform → sink topology with throughput on edges (same pattern as the OTel Collector "Spans Flow" / "Metric Points Flow" / "Log Records Flow" panels).

**Row 2 — Sources**

Per-source received events rate and error rate, broken down by `component_id`:
- `rate(sol_component_received_events_total{component_kind="source"}[$__rate_interval])` by `component_id`
- `rate(sol_component_errors_total{component_kind="source"}[$__rate_interval])` by `component_id`

**Row 3 — Transforms**

Per-transform incoming/outgoing rate and drop ratio:
- Incoming: `rate(sol_component_received_events_total{component_kind="transform"}[$__rate_interval])` by `component_id`
- Outgoing: `rate(sol_component_sent_events_total{component_kind="transform"}[$__rate_interval])` by `component_id`
- Drop ratio: `1 - (sent / received)` — shows how much each transform filters out (important for tail_sampling, but also useful for remap transforms that might drop events)

**Row 4 — Sinks**

Per-sink sent rate, error rate, and bytes sent:
- `rate(sol_component_sent_events_total{component_kind="sink"}[$__rate_interval])` by `component_id`
- `rate(sol_component_errors_total{component_kind="sink"}[$__rate_interval])` by `component_id`
- `rate(sol_component_sent_bytes_total{component_kind="sink"}[$__rate_interval])` by `component_id`

**Row 5 — Tail sampling details**

Trace-specific tail sampling metrics:
- Sampled vs dropped by policy: `rate(sol_tail_sampling_traces_sampled{sampled="true"}[$__rate_interval])` by `policy`
- Dropped by policy: `rate(sol_tail_sampling_traces_sampled{sampled="false"}[$__rate_interval])` by `policy`
- Traces dropped too early (capacity/size eviction): `rate(sol_tail_sampling_trace_dropped_too_early[$__rate_interval])`
- Sampling ratio: `sampled / (sampled + dropped)` — overall effective sampling rate

**Variables**: `$datasource` (Prometheus datasource), `$instance` (service.name label to filter by SOL instance)

Decisions:
- [Dashboard scope](../adrs/20260504_dashboard-scope.md)
- [Metrics namespace renaming](../../rename-vector-to-sol/adrs/20260504_metrics-namespace-renaming.md)

## Cross-cutting Concerns

- **Observability**: this design is itself about observability — it adds self-monitoring to the SOL pipeline
- **Migration**: adding the self-monitoring pipeline to existing SOL configs is additive, no disruption to data flow
- **Testing**: manual verification by running the demo and checking the Grafana dashboard
