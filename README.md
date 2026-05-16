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
| Traces gRPC 10k | 10,089/s | 10,088/s | 10,015/s | 100% | 101% |
| Traces gRPC 50k | 81,766/s | 89,865/s | 27,025/s | 91% | **3.0x** |
| Logs gRPC 10k | 4,382/s | 4,077/s | 99/s | **107%** | **44x** |
| Logs gRPC 50k | 5,054/s | 4,875/s | 192/s | **104%** | **26x** |
| Metrics gRPC 10k | 4,404/s | 4,064/s | 97/s | **108%** | **45x** |
| Metrics gRPC 50k | 5,215/s | 4,997/s | 192/s | **104%** | **27x** |

### Tail sampling

| Scenario | Sol | otelcol | Sol / otelcol |
|---|---|---|---|
| Tail sampling 10k | 11,416/s | 11,513/s | 99% |
| Tail sampling 50k | 90,662/s | 67,465/s | **134%** |
| LB + tail sampling 10k | 10,978/s | 11,057/s | 99% |
| LB + tail sampling 50k | 51,818/s | 49,783/s | **104%** |

### CPU and memory

| Scenario | Sol CPU | otelcol CPU | Sol Mem | otelcol Mem |
|---|---|---|---|---|
| Traces gRPC 50k | 47% | 67% | 12 MiB | 57 MiB |
| Logs gRPC 10k | 181% | 218% | 11 MiB | 48 MiB |
| Tail sampling 10k | 8% | 38% | **162 MiB** | 198 MiB |
| Tail sampling 50k | 84% | 111% | 234 MiB | 213 MiB |
| LB + tail sampling 10k | 16% | 65% | **143 MiB** | 170 MiB |
| LB + tail sampling 50k | 130% | 238% | **233 MiB** | 297 MiB |

### Sustained memory (5-minute runs)

| Scenario | Sol (start → end) | otelcol (start → end) |
|---|---|---|
| Noop logs 10k | 11 → 10 MiB | 47 → 48 MiB |
| Tail sampling 10k | 26 → 159 MiB | 50 → 203 MiB |

**Key takeaways:**
- Sol uses **less memory than otelcol** for tail sampling at 10k spans/s (162 vs 198 MiB — **0.82x**)
- Sol uses **2--5x less CPU** than otelcol on tail sampling workloads
- Sol uses **3--5x less memory** than otelcol in noop pipelines
- Sol is **26--45x faster** than Vector on gRPC logs and metrics (Vector lacks native OTLP gRPC for these signals)
- At 50k spans/s, Sol trails otelcol slightly on tail-sampling memory (234 vs 213 MiB) but leads on throughput (134%) and CPU (24% less)
- The noop traces gRPC 50k gap (91%) is a [tonic/h2 HTTP/2 throughput ceiling](docs/designs/20260514_arc-zero-copy-optimization.md#noop-traces-grpc-50k-gap-analysis), not application overhead

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
