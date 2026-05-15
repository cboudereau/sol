# Sol

**Single Observability Layer** — a high-performance, end-to-end observability data pipeline built in Rust.

Sol collects, transforms, and routes logs, metrics, and traces to any destination.
Deploy as an agent, aggregator, or both.

## Features

- **High performance** — written in Rust, memory-safe and multi-threaded
- **Unified** — logs, metrics, and traces in one tool
- **Vendor-neutral** — route data to any backend, switch vendors without disruption
- **Reliable** — built-in delivery guarantees, disk-backed buffering
- **Extensible** — 100+ sources, transforms, and sinks

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

All Vector sources, transforms, and sinks remain fully compatible.

### Why Sol over Vector?

Sol diverges from Vector on protocol and architecture choices. The table below summarizes the key differences.

| Area | **Vector** | **Sol** | Why it matters |
|---|---|---|---|
| **Core protocol** | Proprietary `LogEvent` / `Metric` / `TraceEvent` types | OTLP-native (`OtelLog`, `OtelMetric`, `OtelSpan`) | No lossy conversion — data enters and exits as standard OpenTelemetry. Removes ~14K lines of vendor-specific code. |
| **Histogram format** | HDR Histogram / sketches (unbounded memory) | ExponentialHistogram (base-2 exponential buckets) | Bounded memory, lossless merge across agents, native OTLP encoding. |
| **OTLP output purity** | Injects `vector.*` attributes into telemetry | Zero custom attributes — pipeline state stays in struct fields | Clean, spec-compliant OTLP output. No downstream filtering needed. |
| **StatsD handling** | Emits one metric per UDP packet | Flush-interval aggregation with delta temporality | Bounded output cardinality, OTLP-compliant timestamps and temporality. |
| **Resource / Scope** | Not populated on most sources | Sensible defaults on every metric source | Correct OTLP semantics out of the box — no manual enrichment transforms. |
| **Sink normalization** | Global temporality setting | Per-sink temporality and ExponentialHistogram conversion | Each destination gets exactly the format it expects; no global compromise. |
| **Encoding** | `Value` intermediary → proto conversion | Direct proto field encoding | Fewer allocations, lower latency on the serialization path. |
| **Self-monitoring** | Internal metrics API | Sol Pipeline dashboards | Pipeline ingestion monitoring and sink/source IO |
| **Tail sampling** | `sample` transform (head-based / probabilistic only) | `tail_sampling` transform: assembles full traces, VRL policies, AND/OR composition, regex matching | Decide per-trace after all spans arrive — keep errors, slow requests, drop noise. Reduces storage costs without losing important traces. |
| **Trace-aware load balancing** | No built-in trace routing | OTLP gRPC sink with consistent-hash routing on `trace_id` or `service.name` (static, DNS, K8s resolvers) | All spans for a trace land on the same collector — required for correct tail sampling in multi-instance deployments. |
| **Migration** | — | Vector source for zero-downtime fleet migration | Drop-in replacement: existing Vector agents forward to Sol with no config change. |

## Performance

Benchmarked against **otelcontribcol 0.122.0** (Go) and **Vector 0.55.0** (Rust) on identical hardware (12 CPUs, 15 GiB RAM, 2 CPU / 2 GB per container), 60s per scenario.

### Noop pipeline (OTLP source to null sink)

| Scenario | Sol | otelcol | Vector | Sol / otelcol | Sol / Vector |
|---|---|---|---|---|---|
| Traces gRPC 10k | 10,009/s | 10,123/s | 9,957/s | 99% | 101% |
| Traces gRPC 50k | 88,590/s | 99,320/s | 29,050/s | 89% | **3.0x** |
| Logs gRPC 10k | 4,667/s | 4,071/s | 97/s | **115%** | **48x** |
| Logs gRPC 50k | 5,503/s | 4,976/s | 192/s | **111%** | **29x** |
| Metrics gRPC 10k | 4,636/s | 4,046/s | 96/s | **115%** | **48x** |
| Metrics gRPC 50k | 5,578/s | 5,013/s | 187/s | **111%** | **30x** |

### Tail sampling

| Scenario | Sol | otelcol | Sol / otelcol |
|---|---|---|---|
| Tail sampling 10k | 11,226/s | 11,089/s | **101%** |
| Tail sampling 50k | 91,161/s | 69,906/s | **130%** |
| LB + tail sampling 10k | 10,811/s | 10,976/s | 98% |
| LB + tail sampling 50k | 51,661/s | 46,012/s | **112%** |

### CPU and memory

| Scenario | Sol CPU | otelcol CPU | Sol Mem | otelcol Mem |
|---|---|---|---|---|
| Traces gRPC 50k | 50% | 75% | 12 MiB | 56 MiB |
| Logs gRPC 10k | 193% | 217% | 11 MiB | 48 MiB |
| Tail sampling 10k | 7% | 42% | **161 MiB** | 201 MiB |
| Tail sampling 50k | 86% | 137% | 233 MiB | 215 MiB |
| LB + tail sampling 10k | 16% | 58% | **159 MiB** | 166 MiB |
| LB + tail sampling 50k | 139% | 264% | **227 MiB** | 299 MiB |

### Sustained memory (5-minute runs)

| Scenario | Sol (start → end) | otelcol (start → end) |
|---|---|---|
| Noop logs 10k | 10 → 10 MiB | 46 → 0 MiB |
| Tail sampling 10k | 27 → 158 MiB | 54 → 198 MiB |

**Key takeaways:**
- Sol uses **less memory than otelcol** for tail sampling at 10k spans/s (161 vs 201 MiB — **0.80x**)
- Sol uses **2--5x less CPU** than otelcol on tail sampling workloads
- Sol uses **3--5x less memory** than otelcol in noop pipelines
- Sol is **29--48x faster** than Vector on gRPC logs and metrics (Vector lacks native OTLP gRPC for these signals)
- At 50k spans/s, Sol trails otelcol slightly on tail-sampling memory (233 vs 215 MiB) but leads on throughput (130%) and CPU (63% less)
- The noop traces gRPC 50k gap (89%) is a [tonic/h2 HTTP/2 throughput ceiling](docs/designs/20260514_arc-zero-copy-optimization.md#noop-traces-grpc-50k-gap-analysis), not application overhead

<details>
<summary>Reproduce</summary>

```bash
cd demo/benchmark
bash run.sh                                              # all scenarios
bash run.sh --scenario noop-traces-grpc-10k --duration 15  # single scenario
```
</details>

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

## License

Sol is licensed under the [Mozilla Public License 2.0](LICENSE).

Built on technology originally from the [Vector project](https://github.com/vectordotdev/vector) (MPL-2.0), Copyright Datadog, Inc.
