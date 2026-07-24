# sink-retry-observability

While building the SOL pipeline monitoring dashboard (sol-telemetry-monitoring), we discovered that sink retry behavior is invisible to metrics. When a downstream service (Tempo, Loki, Mimir) goes down:

## Design
- [20260504_sink-retry-observability](./designs/20260504_sink-retry-observability.md)
