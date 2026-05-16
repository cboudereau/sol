# Sol

**Single Observability Layer** — a high-performance, end-to-end observability data pipeline built in Rust.

Sol collects, transforms, and routes logs, metrics, and traces to any destination.
Deploy as an agent, aggregator, or both.

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

### OTLP-native

Sol speaks OpenTelemetry natively. Data enters and exits as standard OTLP — no lossy conversion, no vendor-specific attributes injected into your telemetry. This means clean, spec-compliant output and fewer surprises downstream.

### Tail sampling done right

Decide per-trace after all spans arrive: keep errors, slow requests, sample everything else. Sol's `tail_sampling` transform supports AND/OR policy composition, regex matching, and first-match-wins evaluation — in a single transform. Paired with trace-aware load balancing (consistent-hash on `trace_id`), Sol handles the full multi-collector deployment pattern out of the box.

### Efficient

Written in Rust. **4–5x less memory** and **up to 45% less CPU** than the OpenTelemetry Collector at equal or higher throughput. **26–45x faster** than Vector on gRPC logs and metrics.

### Drop-in replacement

All Vector sources, transforms, and sinks remain fully compatible. Existing Vector agents can forward to Sol with no config change via the built-in Vector source.

## Performance

Benchmarked against **otelcontribcol 0.122.0** (Go) and **Vector 0.55.0** (Rust) on identical hardware (12 CPUs, 15 GiB RAM, 2 CPU / 2 GB per container), 60s per scenario.

### Production workloads: LB + tail sampling

The real-world deployment pattern — load balancer routing by traceID to collectors running tail sampling (1 LB + 2 collectors per system, 1 CPU / 1 GB each).

| Scenario | Sol | otelcol | Throughput | CPU | Memory |
|---|---|---|---|---|---|
| LB + tail sampling 10k | 10,978/s | 11,057/s | 99% | **4x less** (16% vs 65%) | **16% less** (143 vs 170 MiB) |
| LB + tail sampling 50k | 51,818/s | 49,783/s | **104%** | **45% less** (130% vs 238%) | **22% less** (233 vs 297 MiB) |

### Tail sampling (single collector)

| Scenario | Sol | otelcol | Throughput | CPU |
|---|---|---|---|---|
| Tail sampling 10k | 11,416/s | 11,513/s | 99% | **4.7x less** (8% vs 38%) |
| Tail sampling 50k | 90,662/s | 67,465/s | **134%** | **24% less** (84% vs 111%) |

### Noop pipeline (OTLP source to null sink)

| Scenario | Sol | otelcol | Vector | Sol / otelcol | Sol / Vector | CPU | Memory |
|---|---|---|---|---|---|---|---|
| Traces gRPC 10k | 10,089/s | 10,088/s | 10,015/s | 100% | 101% | **36% less** (4% vs 7%) | **5x less** (12 vs 61 MiB) |
| Traces gRPC 50k | 81,766/s | 89,865/s | 27,025/s | 91% | **3.0x** | **30% less** (47% vs 67%) | **4.5x less** (13 vs 57 MiB) |
| Logs gRPC 10k | 4,382/s | 4,077/s | 99/s | **107%** | **44x** | **17% less** (181% vs 218%) | **4.4x less** (11 vs 48 MiB) |
| Logs gRPC 50k | 5,054/s | 4,875/s | 192/s | **104%** | **26x** | **17% less** (179% vs 215%) | **4.4x less** (12 vs 52 MiB) |
| Metrics gRPC 10k | 4,404/s | 4,064/s | 97/s | **108%** | **45x** | **15% less** (183% vs 215%) | **4.4x less** (11 vs 48 MiB) |
| Metrics gRPC 50k | 5,215/s | 4,997/s | 192/s | **104%** | **27x** | **15% less** (182% vs 214%) | **4.2x less** (13 vs 52 MiB) |

### Sustained memory (5-minute runs)

| Scenario | Sol (start / end) | otelcol (start / end) |
|---|---|---|
| Noop logs 10k | 11 / 10 MiB | 47 / 48 MiB |
| Tail sampling 10k | 26 / 159 MiB | 50 / 203 MiB |

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
