# sol-telemetry-monitoring

The SOL demo replaces OTel Collector Contrib with Vector for the full traces pipeline (gateway → load balancer → tail sampling → Tempo). Steps 1–3 are complete, but the SOL pipeline currently has **no self-monitoring**: there is no equivalent of the OTel Collector's built-in telemetry that powers the existing Grafana dashboards ("OpenTelemetry Collector" and "OpenTelemetry Collector HostMetrics").

## Design
- [20260504_sol-telemetry-monitoring](./designs/20260504_sol-telemetry-monitoring.md)

## ADRs
- [20260504_dashboard-scope](./adrs/20260504_dashboard-scope.md) — SOL-native dashboards vs reusing OTel Collector dashboards
