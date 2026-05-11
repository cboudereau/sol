# Benchmark: Sol vs otelcontribcol

Reproducible performance comparison between [Sol](https://github.com/clembs/sol) and [OpenTelemetry Collector Contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib) across three pipeline categories.

## Architecture

### Noop pipeline (pure overhead)

```
telemetrygen ──► sol             (OTLP → blackhole)
             ──► vector          (OTLP → blackhole)
             ──► otelcontribcol  (OTLP → nop)
```

### Tail sampling pipeline

```
telemetrygen ──► sol             (OTLP → tail_sampling → blackhole)
             ──► otelcontribcol  (OTLP → tail_sampling×2 → nop)
```

### Load-balanced tail sampling pipeline

```
telemetrygen ──► sol-lb ──┬──► sol-collector-1         otelcol-lb ──┬──► otelcol-collector-1
                          └──► sol-collector-2                       └──► otelcol-collector-2
                          (traceID consistent-hash via DNS)
```

## Comparison Matrix

### Agent capabilities

| Capability | Sol | Vector | otelcontribcol |
|------------|-----|--------|----------------|
| OTLP gRPC receiver | yes | yes | yes |
| OTLP HTTP receiver | yes | yes | yes |
| Null sink | `blackhole` | `blackhole` | `nop` |
| Tail sampling | 1 transform, first-match-wins policies | N/A | 2 sequential processors (double buffer) |
| Load balancing | OTLP sink + `load_balancing` block | N/A | Dedicated `loadbalancing` exporter |
| TraceID consistent hash | yes | N/A | yes |
| Internal metrics prefix | `sol_sol_*` | `vector_*` | `otelcol_*` |

### Benchmark scenarios

| Benchmark | Systems | Signal | Protocol | Compression | What it measures |
|-----------|---------|--------|----------|-------------|-----------------|
| `noop-traces-grpc-10k` | Sol, Vector, otelcol | traces | gRPC | none | Batched trace throughput |
| `noop-traces-grpc-10k-gzip` | Sol, Vector, otelcol | traces | gRPC | gzip | Batched traces with compression |
| `noop-traces-grpc-50k` | Sol, Vector, otelcol | traces | gRPC | none | Batched throughput at high load |
| `noop-traces-http-10k` | Sol, Vector, otelcol | traces | HTTP | none | HTTP trace throughput baseline |
| `noop-logs-grpc-10k` | Sol, Vector, otelcol | logs | gRPC | none | Per-request overhead (1 log/call) |
| `noop-logs-grpc-10k-gzip` | Sol, Vector, otelcol | logs | gRPC | gzip | Per-request overhead with compression |
| `noop-logs-grpc-50k` | Sol, Vector, otelcol | logs | gRPC | none | Per-request overhead at high load |
| `noop-logs-http-10k` | Sol, Vector, otelcol | logs | HTTP | none | HTTP throughput baseline |
| `noop-metrics-grpc-10k` | Sol, Vector, otelcol | metrics | gRPC | none | Metric per-request overhead |
| `noop-metrics-grpc-10k-gzip` | Sol, Vector, otelcol | metrics | gRPC | gzip | Metric per-request with compression |
| `noop-metrics-grpc-50k` | Sol, Vector, otelcol | metrics | gRPC | none | Metric throughput at high load |
| `noop-metrics-http-10k` | Sol, Vector, otelcol | metrics | HTTP | none | HTTP metric throughput |
| `tail-sampling-traces-grpc-10k` | Sol, otelcol | traces | gRPC | none | Tail sampling throughput |
| `tail-sampling-traces-grpc-10k-gzip` | Sol, otelcol | traces | gRPC | gzip | Tail sampling with compression |
| `tail-sampling-traces-grpc-50k` | Sol, otelcol | traces | gRPC | none | Tail sampling at high load |
| `lb-tail-sampling-traces-grpc-10k` | Sol, otelcol | traces | gRPC | none | LB + tail sampling |
| `lb-tail-sampling-traces-grpc-10k-gzip` | Sol, otelcol | traces | gRPC | gzip | LB + tail sampling with compression |
| `lb-tail-sampling-traces-grpc-50k` | Sol, otelcol | traces | gRPC | none | LB + tail sampling at high load |

### Key findings

| Condition | Observation | Explanation |
|-----------|-------------|-------------|
| Batched traces (gRPC) | Sol ~= otelcol at 10k; otelcol wins at 50k | Per-request overhead amortized in batch; at high load, Go's gRPC scales better |
| Logs/metrics (gRPC, 1/call, no compression) | otelcol ~45x faster than Sol/Vector | Sol/Vector ~10ms per gRPC call vs otelcol ~0.2ms — inherited from Vector |
| Logs/metrics (gRPC, 1/call, gzip) | Sol 2.5x better than uncompressed; otelcol 2x worse | Sol's DecompressionAndMetrics layer runs anyway — gzip just exercises the intended path |
| Logs/metrics (HTTP) | Sol/Vector >= otelcol | HTTP path avoids the gRPC DecompressionAndMetrics layer — Sol wins |
| Sol vs Vector (noop) | Near-identical on gRPC and HTTP | Confirms gRPC overhead is inherited from Vector, not a Sol regression |
| Tail sampling (batched) | Comparable throughput at 10k; otelcol better at 50k | Both handle the sampling pipeline |
| LB topology | Sol ~100/s vs otelcol ~10k+/s | Sol LB → collector forwarding hits the same gRPC per-request overhead |

### OTLP compression (per spec)

| | OTLP spec | otelcol default | Sol/Vector support |
|---|---|---|---|
| gRPC | `gzip`, `none` | exporter: `gzip`; receiver: accepts all | gzip only (via `DecompressionAndMetricsLayer`) |
| HTTP | `gzip` | exporter: `gzip`; receiver: gzip, zstd, zlib, snappy, deflate, lz4 | gzip, deflate, snappy, zstd |

> In production, OTLP exporters default to `gzip` compression. The `none` (uncompressed) scenarios represent a non-default configuration. The `gzip` scenarios represent the realistic default.

## Quick start

Run all scenarios (~20 min):

```bash
bash run.sh
```

Run a single scenario with custom duration:

```bash
bash run.sh --scenario noop-logs-grpc-10k --duration 15
```

Results are written to `results/RESULTS.md`.

## Scenarios

| Category | Scenarios | Systems | Duration |
|----------|-----------|---------|----------|
| Noop | 12 (traces gRPC 10k/10k-gzip/50k + HTTP 10k, logs gRPC 10k/10k-gzip/50k + HTTP 10k, metrics gRPC 10k/10k-gzip/50k + HTTP 10k) | Sol, Vector, otelcol | 60s each |
| Tail sampling | 3 (traces gRPC at 10k, 10k-gzip, 50k) | Sol, otelcol | 60s each |
| Load-balanced tail sampling | 3 (traces gRPC at 10k, 10k-gzip, 50k) | Sol, otelcol | 60s each |
| Sustained memory | 2 (noop-logs + tail-sampling-traces at 10k) | varies | 300s each |

> **Note on batching**: telemetrygen traces are batched by default (many spans per gRPC call). Logs have no batch option (always 1 log per gRPC call). `--batch=false` for traces exists but is broken in telemetrygen v0.137.0 (gRPC channel never connects). Log scenarios naturally expose per-request overhead.

## Resource limits

| Topology | Per container | Total per system |
|----------|--------------|-----------------|
| Single-instance (noop, tail-sampling) | 2 CPU / 2 GB | 2 CPU / 2 GB |
| Load-balanced (1 LB + 2 collectors) | 1 CPU / 1 GB | 3 CPU / 3 GB |

## Methodology

**Fairness measures**:
- Identical resource limits (2 CPU / 2 GB) via Docker Compose `deploy.resources.limits`
- Null sinks: Sol/Vector `blackhole`, otelcol `nop` — no network egress
- Separate `telemetrygen` per system with identical config — each system gets its own load
- Throughput from each system's internal metrics endpoint (most accurate)
- CPU/memory from `docker stats` polling (identical measurement for all)

**Known differences** (documented, not hidden):
- **Tail sampling**: otelcontribcol uses 2 sequential processors (double buffer); Sol uses 1 transform (single buffer). This is architectural — otelcol can't express Sol's policy composition in one processor.
- **Load balancing**: otelcontribcol uses a dedicated `loadbalancing` exporter; Sol uses the standard OTLP sink with a `load_balancing` config block.
- **Vector scope**: Vector participates only in noop scenarios — it has no tail_sampling or load_balancing. Its role is baseline regression detection for Sol (same codebase fork).
- **telemetrygen batching**: Traces have `--batch` flag (default true). Logs and metrics have no batch option (1 event per gRPC call). `nobatch` variants isolate per-request overhead.

## Interpreting results

- **Throughput (events/s)**: higher is better. Measured from internal counters, not the load generator.
- **CPU%**: lower is better. Peak CPU across the test duration.
- **Memory**: lower is better. Peak RSS during the test.
- **Sustained memory**: compare start vs end. Growth indicates a leak or unbounded buffer.

## Prerequisites

- Docker with Compose v2
- 8+ CPU cores, 16+ GB RAM recommended
- ~20 minutes for all scenarios

## Configs

| System | Noop | Tail sampling | LB | LB Collector |
|--------|------|--------------|-----|-------------|
| Sol | [sol/noop.yaml](sol/noop.yaml) | [sol/tail-sampling.yaml](sol/tail-sampling.yaml) | [sol/lb.yaml](sol/lb.yaml) | [sol/lb-collector.yaml](sol/lb-collector.yaml) |
| Vector | [vector/noop.yaml](vector/noop.yaml) | N/A | N/A | N/A |
| otelcontribcol | [otelcontribcol/noop.yml](otelcontribcol/noop.yml) | [otelcontribcol/tail-sampling.yml](otelcontribcol/tail-sampling.yml) | [otelcontribcol/lb.yml](otelcontribcol/lb.yml) | [otelcontribcol/lb-collector.yml](otelcontribcol/lb-collector.yml) |
