---
status: accepted
---
# Load balancing equivalence

Addresses: [FR9](../DESIGN.md#fr9), [NFR1](../DESIGN.md#nfr1)

## Problem

Both Sol and otelcontribcol support trace-aware load balancing (consistent-hash routing on `trace_id` to ensure all spans of a trace land on the same collector). The implementations differ — we need to ensure the benchmark is fair.

### otelcontribcol (from o11y-weekly)

Uses a dedicated `loadbalancing` exporter:
```yaml
exporters:
  loadbalancing/traces-collector:
    routing_key: "traceID"
    protocol:
      otlp:
        timeout: 1s
        tls: { insecure: true }
    resolver:
      dns:
        hostname: otelcontribcol-traces-collector
```

This is a specialized component — separate from the `otlp` exporter. It manages its own connection pool, consistent-hash ring, and DNS resolution loop.

### Sol (from demo)

Uses the standard OTLP gRPC sink with a `load_balancing` config block:
```yaml
sinks:
  otlp_traces:
    type: opentelemetry
    protocol:
      type: grpc
      load_balancing:
        routing_key: traceID
        resolver:
          type: dns
          hostname: sol-collector
      batch:
        max_events: 1000
        timeout_secs: 1
```

This is NOT a separate component — it's a config option on the existing OTLP sink. The sink manages the connection pool, consistent-hash ring, and DNS resolution internally.

## Options

| Option | Pros | Cons |
|---|---|---|
| **A. Same topology: LB instance + 2× collector replicas** | Mirrors o11y-weekly exactly. Both systems run 3 containers. Fair. | Sol's LB instance is "thinner" (just routing, no extra processing) — could be seen as an advantage, but it's a real architectural difference. |
| **B. Skip the LB, route directly from telemetrygen** | Removes LB overhead from measurement. | Not representative of real deployments. telemetrygen can't do traceID routing. |
| **C. Different replica counts per system** | Could equalize total resource usage. | Unfair — changes the workload shape. |

## Decision

**Option A — same topology.** Both systems run:
- 1× loadbalancer container (receives OTLP, routes by traceID via DNS)
- 2× collector containers (receive routed traces, run tail sampling, sink to null)

Resource limits: each container gets 1 CPU / 1 GB (total 3 CPU / 3 GB per system — fair because both run 3 containers).

Resource measurement: the report shows aggregate CPU/memory across all 3 containers per system, plus per-container breakdown.

## Consequences

- The compose.yml needs additional services: `sol-lb`, `sol-collector` (replicas: 2), `otelcol-lb`, `otelcol-collector` (replicas: 2).
- Prometheus must scrape all containers (LB + collectors for both systems).
- `docker stats` must poll all containers and the report aggregates per system.
- The LB benchmark uses different resource limits (1 CPU / 1 GB per container) than the single-instance benchmarks (2 CPU / 2 GB) to fit on a developer laptop. This is documented in the report.
- The collector configs for the LB benchmark (`lb-collector.yaml` / `lb-collector.yml`) are identical to the tail-sampling configs but listen on internal ports only (no host port mapping).
