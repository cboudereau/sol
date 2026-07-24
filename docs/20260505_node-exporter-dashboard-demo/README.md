# node-exporter-dashboard-demo

The SOL demo (`demo/otel-sol-grafana-dotnet/`) currently monitors the pipeline itself (via the SOL Pipeline dashboard) and application metrics (via the dotnet webapi dashboard). The existing "OpenTelemetry Collector HostMetrics (Node Exporter)" dashboard in `OpenTelemetry Collector Contrib/` is a legacy reference from the OTel Collector setup — it uses `system_*` metric names from the OTel `hostme

## Design
- [20260505_node-exporter-dashboard-demo](./designs/20260505_node-exporter-dashboard-demo.md)

## ADRs
- [20260505_host-metrics-namespace-and-job-label](./adrs/20260505_host-metrics-namespace-and-job-label.md) — Host metrics namespace and job label strategy
