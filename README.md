# Sol

**Single Observability Layer** — a high-performance, end-to-end observability data pipeline built in Rust.

Sol collects, transforms, and routes logs, metrics, and traces to any destination.
Deploy as an agent, aggregator, or both.

**Watch the demo**: 

[![Watch the demo](https://i.ytimg.com/vi/DF6R7cKPg4M/hqdefault.jpg?sqp=-oaymwFBCNACELwBSFryq4qpAzMIARUAAIhCGAHYAQHiAQoIGBACGAY4AUAB8AEB-AH-CYAC0AWKAgwIABABGGUgYyhLMA8=&rs=AOn4CLD-4HAUlwugaIUsra4uGHm1ec3vAg)](https://www.youtube.com/watch?v=DF6R7cKPg4M)

## Quick start

```bash
docker run --rm -v $(pwd)/sol.yaml:/etc/sol/sol.yaml:ro superbeeeeeee/sol:latest --config /etc/sol/sol.yaml
```

Or run the binary directly:

```bash
sol --config /etc/sol/sol.yaml
```

### Demos

- **[OTLP drop-in replacement](demo/otel-drop-in/)** — run Sol side-by-side with the OpenTelemetry Collector Contrib and compare OTLP/JSON output
- **[Full observability stack](demo/otel-sol-grafana-dotnet/)** — Sol gateway/loadbalancer/collector pipeline with Grafana, Loki, Mimir, and Tempo

## Why Sol?

### Efficient

Sol does the same work as the OpenTelemetry Collector with a fraction of the resources with **up to 4x less memory**. For load-balanced tail sampling, Sol uses **5x less CPU** while matching throughput, and delivers **nearly 2x the throughput** at high load.

### OTLP-native

Sol speaks OpenTelemetry natively. Data enters and exits as standard OTLP — no lossy conversion, no vendor-specific attributes injected into your telemetry. This means clean, spec-compliant output and fewer surprises downstream.

### Tail sampling done right

Decide per-trace after all spans arrive: keep errors, slow requests, sample everything else. Sol's `tail_sampling` transform supports AND/OR policy composition, regex matching, and first-match-wins evaluation — in a single transform. Paired with trace-aware load balancing (consistent-hash on `trace_id`), Sol handles the full multi-collector deployment pattern out of the box.

### Drop-in replacement

All Vector sources, transforms, and sinks remain fully compatible. Existing Vector agents can forward to Sol with no config change via the built-in Vector source.

## Performance

Benchmarked against **otelcontribcol 0.122.0** (Go) and **Vector 0.55.0** (Rust) on identical hardware (12 CPUs, 15 GiB RAM, 2 CPU / 2 GB per container), 60s per scenario.

### Production workloads: LB + tail sampling

The real-world deployment pattern — load balancer routing by traceID to collectors running tail sampling (1 LB + 2 collectors per system, 1 CPU / 1 GB each).

| Scenario | Sol | otelcol | Throughput | CPU | Memory |
|---|---|---|---|---|---|
| LB + tail sampling 10k | 11,969/s | 12,791/s | 94% | **5.5x less** (25% vs 137%) | **12% less** (162 vs 184 MiB) |
| LB + tail sampling 50k | 46,726/s | 24,175/s | **193%** | **41% less** (133% vs 227%) | ~same (231 vs 219 MiB) |

### Tail sampling (single collector)

| Scenario | Sol | otelcol | Throughput | CPU |
|---|---|---|---|---|
| Tail sampling 10k | 12,402/s | 12,946/s | 96% | **3.0x less** (17% vs 51%) |
| Tail sampling 50k | 56,821/s | 48,819/s | **116%** | **26% less** (99% vs 134%) |

### Noop pipeline (OTLP source to null sink)

> Traces are batched by telemetrygen (many spans per gRPC call). Log/metric scenarios use `--batch-size=500` for production-realistic batching. All systems use identical resource limits (2 CPU / 2 GB).

| Scenario | Sol | otelcol | Vector | Sol / otelcol | Sol / Vector |
|---|---|---|---|---|---|
| Traces gRPC 10k | 11,540/s | 12,224/s | 11,168/s | 94% | 103% |
| Traces gRPC 50k | 51,139/s | 57,107/s | 7,739/s | 90% | **6.6x** |
| Logs batched 50k | 138,643/s | 143,398/s | 114,418/s | 97% | **121%** |
| Metrics batched 50k | 114,325/s | 118,199/s | 104,396/s | 97% | **110%** |

Sol uses ~11 MiB in noop scenarios vs ~47 MiB for otelcol — **4x less memory**.

### Sustained memory (5-minute runs)

| Scenario | Sol (start / end) | otelcol (start / end) |
|---|---|---|
| Noop logs 10k | 11 / 11 MiB | 35 / 35 MiB |
| Tail sampling 10k | 19 / 163 MiB | 49 / 185 MiB |

<details>
<summary>Reproduce</summary>

```bash
cd demo/benchmark
bash run.sh                                              # all scenarios
bash run.sh --scenario noop-traces-grpc-10k --duration 15  # single scenario
```
</details>

## Sol vs Vector

Sol is a fork of [Datadog Vector](https://github.com/vectordotdev/vector). The table below summarizes what changed.

| | **Vector** | **Sol** |
|---|---|---|
| **Binary name** | `vector` | `sol` |
| **Config path** | `/etc/vector/vector.yaml` | `/etc/sol/sol.yaml` |
| **Data directory** | `/var/lib/vector/` | `/var/lib/sol/` |
| **Env var prefix** | `VECTOR_*` | `SOL_*` |
| **Metrics namespace** | `vector_*` | `sol_*` |
| **Systemd unit** | `vector.service` | `sol.service` |
| **Crate names** | `vector-core`, `vector-lib`, … | `sol-core`, `sol-lib`, … |
| **Core protocol** | Vendor-neutral | OTLP-first (OpenTelemetry) |
| **Self-monitoring** | Internal metrics | OTLP pipeline |
| **Goal** | General-purpose pipeline | Single Observability Layer for self-hosted SaaS |

### Feature comparison

| Area | **Vector** | **Sol** | Why it matters |
|---|---|---|---|
| **Core protocol** | Proprietary `LogEvent` / `Metric` / `TraceEvent` types | OTLP-native (`OtelLog`, `OtelMetric`, `OtelSpan`) | No lossy conversion — data enters and exits as standard OpenTelemetry. |
| **Histogram format** | HDR Histogram / sketches (unbounded memory) | ExponentialHistogram (base-2 exponential buckets) | Bounded memory, lossless merge across agents, native OTLP encoding. |
| **OTLP output purity** | Injects `vector.*` attributes into telemetry | Zero custom attributes — pipeline state stays in struct fields | Clean, spec-compliant OTLP output. No downstream filtering needed. |
| **StatsD handling** | Emits one metric per UDP packet | Flush-interval aggregation with delta temporality | Bounded output cardinality, OTLP-compliant timestamps and temporality. |
| **Resource / Scope** | Not populated on most sources | Sensible defaults on every metric source | Correct OTLP semantics out of the box. |
| **Sink normalization** | Global temporality setting | Per-sink temporality and ExponentialHistogram conversion | Each destination gets exactly the format it expects. |
| **Encoding** | `Value` intermediary → proto conversion | Direct proto field encoding | Fewer allocations, lower latency on the serialization path. |
| **Self-monitoring** | Internal metrics API | Sol Pipeline dashboards | Pipeline ingestion monitoring and sink/source IO. |
| **Tail sampling** | `sample` transform (head-based / probabilistic only) | `tail_sampling` transform: full traces, VRL policies, AND/OR composition, regex matching | Decide per-trace after all spans arrive — keep errors, slow requests, drop noise. |
| **Trace-aware load balancing** | No built-in trace routing | OTLP gRPC sink with consistent-hash routing on `trace_id` or `service.name` | All spans for a trace land on the same collector — required for correct tail sampling. |
| **Migration** | — | Vector source for zero-downtime fleet migration | Drop-in replacement: existing Vector agents forward to Sol with no config change. |

## License

Sol is licensed under the [Mozilla Public License 2.0](LICENSE).

Built on technology originally from the [Vector project](https://github.com/vectordotdev/vector) (MPL-2.0), Copyright Datadog, Inc.
