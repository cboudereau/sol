# servicegraph

The Sol demo pipeline currently provides per-service RED metrics via the `span_metrics` transform but lacks inter-service **edge metrics** — the client-to-server request counts and latencies that power Grafana's service graph panel. The OTel Collector Contrib project provides a `servicegraphconnector` that fills this role. Sol needs an equivalent `servicegraph` transform that emits compatible metr

## Design
- [20260505_servicegraph](./designs/20260505_servicegraph.md)

## ADRs
- [20260505_servicegraph-store-implementation](./adrs/20260505_servicegraph-store-implementation.md) — Store implementation for servicegraph edge buffering
- [20260505_span-pairing-strategy](./adrs/20260505_span-pairing-strategy.md) — Span pairing strategy for servicegraph
