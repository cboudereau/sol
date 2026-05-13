# benchmark-sol-vs-otelcol — Design Doc

## Context

Sol is positioned as a drop-in replacement for OTel Collector Contrib and Vector. To promote Sol credibly, we need published, reproducible benchmarks comparing OTLP ingestion throughput, latency, and resource usage between Sol and otelcontribcol.

The existing `demo/otel-drop-in/` proves functional parity (same OTLP payloads accepted). This benchmark proves **performance** parity or superiority.

**Key constraint**: the benchmark must be fair. Both systems must do equivalent work under equivalent resource constraints. Unfair benchmarks destroy credibility.

The benchmark covers three categories:
1. **Noop pipeline** — OTLP in → null sink. Pure pipeline overhead (deserialization, routing, backpressure).
2. **Tail sampling pipeline** — OTLP traces in → tail_sampling → null sink. Measures Sol's key differentiator against otelcontribcol's tail_sampling processor with equivalent policies.
3. **Load-balanced tail sampling pipeline** — OTLP traces in → loadbalancer (traceID routing) → 2× collector (tail_sampling) → null sink. The real-world deployment pattern from o11y-weekly. Tests end-to-end multi-tier performance including inter-service gRPC, DNS-based discovery, and consistent-hash routing.

The benchmark also tracks **sustained memory** over time (5-minute run) to detect leaks or unbounded growth under load.

The original otelcontribcol configs come from [o11y-weekly](https://github.com/o11y-weekly/o11y-weekly.github.io/tree/main/2024-02-28_OpenTelemetry_Looks_Good_To_Me_dotnet), the same project the Sol demo was ported from. This ensures the comparison is grounded in a real-world pipeline, not a synthetic config.

The benchmark is a new demo under `demo/benchmark/` using Docker Compose, producing a Markdown report. It is designed to run on any developer laptop and in CI.

## Functional Requirements

### <a id="fr1"></a>FR1 — Load generation with telemetrygen

Use the official `telemetrygen` tool (already used in `tests/e2e/`) to generate sustained OTLP traffic for all 3 signals (logs, metrics, traces) over both gRPC and HTTP. The load generator must be configurable for duration, rate, and concurrency.

### <a id="fr2"></a>FR2 — Equivalent noop pipeline configurations

Both Sol and otelcontribcol must run identical logical noop pipelines:
- **Source**: OTLP receiver (gRPC on 4317, HTTP on 4318)
- **Sink**: null/noop (Sol: `blackhole` sink, otelcontribcol: `nop` exporter)
- No transforms, no processors, no batching beyond defaults
- Same resource limits (CPU, memory) applied via Docker Compose

### <a id="fr7"></a>FR7 — Equivalent tail sampling pipeline configurations

Both Sol and otelcontribcol must run equivalent tail sampling pipelines with identical policy logic:
- **Source**: OTLP receiver (gRPC on 4317)
- **Transform/Processor**: tail sampling with aligned policies (see [ADR: tail-sampling-policy-equivalence](./adrs/tail-sampling-policy-equivalence.md))
- **Sink**: null/noop
- Same `decision_wait` (10s), same `num_traces` (50000)
- Same resource limits as noop scenarios

The original o11y-weekly otelcontribcol pipeline uses two sequential tail_sampling processors:
```
tail_sampling/latency-error → tail_sampling/probabilistic → nop
```
Sol uses a single tail_sampling transform with first-match-wins policy list. The policies must produce equivalent sampling decisions.

### <a id="fr9"></a>FR9 — Equivalent load-balanced tail sampling pipeline

Both Sol and otelcontribcol must run the full multi-tier pipeline from o11y-weekly:

```
telemetrygen → loadbalancer (traceID consistent-hash) → 2× collector (tail sampling) → null sink
```

This is 3 containers per system (1 LB + 2 collectors), all with resource limits.

**otelcontribcol** (from o11y-weekly):
- `otelcontribcol-lb`: OTLP receiver → `loadbalancing` exporter (routing_key: traceID, DNS resolver → `otelcontribcol-collector`)
- `otelcontribcol-collector` ×2: OTLP receiver → `tail_sampling/latency-error` → `tail_sampling/probabilistic` → `nop`

**Sol** (from demo):
- `sol-lb`: OTLP source → OTLP gRPC sink with `load_balancing` (routing_key: traceID, DNS resolver → `sol-collector`)
- `sol-collector` ×2: OTLP source → `tail_sampling` transform → `blackhole` sink

Key architectural differences:
- otelcontribcol uses a dedicated `loadbalancing` exporter component; Sol uses the standard OTLP sink with a `load_balancing` config block
- Both use DNS-based service discovery for backend resolution
- Both route by traceID using consistent hashing
- Resource measurement aggregates CPU/memory across all containers per system (LB + 2× collector)

### <a id="fr8"></a>FR8 — Sustained memory tracking

For selected scenarios (noop logs-grpc-10k and tail-sampling traces-grpc-10k), run an extended 5-minute load test and sample memory (RSS) every 5 seconds. Report the memory time-series to detect leaks or unbounded growth. The report includes a before/after comparison (RSS at t=30s vs t=300s).

### <a id="fr3"></a>FR3 — Metrics collection via Prometheus scraping

Both systems expose Prometheus metrics. A shared Prometheus instance scrapes both at 5s intervals during the benchmark run. Metrics collected:
- CPU usage (from cAdvisor or Docker stats)
- Memory usage (RSS)
- Events received/processed counters (from each system's internal metrics)

### <a id="fr4"></a>FR4 — Benchmark runner script

A single `run.sh` script that:
1. Starts the infrastructure (Prometheus, cAdvisor)
2. Starts Sol and otelcontribcol with resource limits
3. Warms up for 10s
4. Runs telemetrygen at configured rate for configured duration (default: 60s)
5. Waits for drain
6. Queries Prometheus for results
7. Produces a `results/` directory with raw JSON + summary Markdown table

### <a id="fr5"></a>FR5 — Multiple test scenarios

The benchmark must cover multiple scenarios to avoid cherry-picking:

#### Noop scenarios (OTLP → null sink)

| Scenario | Signal | Protocol | Rate | Workers | Duration |
|----------|--------|----------|------|---------|----------|
| noop-logs-grpc-10k | logs | gRPC | 10,000/s | 4 | 60s |
| noop-logs-http-10k | logs | HTTP | 10,000/s | 4 | 60s |
| noop-traces-grpc-10k | traces | gRPC | 10,000 spans/s | 4 | 60s |
| noop-traces-http-10k | traces | HTTP | 10,000 spans/s | 4 | 60s |
| noop-metrics-grpc-10k | metrics | gRPC | 10,000/s | 4 | 60s |
| noop-metrics-http-10k | metrics | HTTP | 10,000/s | 4 | 60s |
| noop-logs-grpc-50k | logs | gRPC | 50,000/s | 8 | 60s |
| noop-traces-grpc-50k | traces | gRPC | 50,000 spans/s | 8 | 60s |

#### Tail sampling scenarios (OTLP traces → tail_sampling → null sink)

| Scenario | Policy set | Rate | Workers | Duration |
|----------|-----------|------|---------|----------|
| tail-sampling-traces-grpc-10k | equivalent policies | 10,000 spans/s | 4 | 60s |
| tail-sampling-traces-grpc-50k | equivalent policies | 50,000 spans/s | 8 | 60s |

#### Load-balanced tail sampling scenarios (LB → 2× collector w/ tail sampling → null sink)

| Scenario | Topology | Rate | Workers | Duration |
|----------|----------|------|---------|----------|
| lb-tail-sampling-traces-grpc-10k | LB + 2× collector | 10,000 spans/s | 4 | 60s |
| lb-tail-sampling-traces-grpc-50k | LB + 2× collector | 50,000 spans/s | 8 | 60s |

#### Sustained memory scenarios (extended duration)

| Scenario | Pipeline | Signal | Rate | Workers | Duration |
|----------|----------|--------|------|---------|----------|
| sustained-noop-logs-grpc-10k | noop | logs | 10,000/s | 4 | 300s |
| sustained-tail-sampling-traces-grpc-10k | tail sampling | traces | 10,000 spans/s | 4 | 300s |

### <a id="fr6"></a>FR6 — Publishable report

The output `RESULTS.md` must be ready to publish as a blog post or GitHub discussion. It must include:
- System info (CPU, memory, kernel, Docker version)
- Exact versions of Sol and otelcontribcol
- Configuration files used (inline or linked)
- Per-scenario table: throughput (events/s achieved), p50/p99 latency, peak CPU %, peak memory MB
- Methodology description (how to reproduce)
- Fair-play statement (what was kept equal, what differs)

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Reproducibility

The benchmark must produce consistent results (±5% variance) across runs on the same hardware. Docker resource limits (`cpus`, `mem_limit`) ensure isolation. A warm-up phase avoids cold-start noise.

### <a id="nfr2"></a>NFR2 — Runs on developer laptop

Must work with `docker compose up` on a machine with 8+ cores and 16+ GB RAM. No cloud-specific tooling, no Kubernetes. Total runtime under 15 minutes for all scenarios.

### <a id="nfr3"></a>NFR3 — No custom code for metrics collection

Use existing tooling only: Prometheus for scraping, cAdvisor for container metrics, `promtool` or `curl` + `jq` for querying. No custom exporter or sidecar.

## Non-goals

- **Benchmarking VRL vs OTTL transforms**: VRL and OTTL do fundamentally different things (VRL is a language, OTTL is an expression evaluator). A fair comparison requires equivalent operations, which is a separate effort.
- **Benchmarking with backpressure/disk buffers**: all sinks are null — no backpressure scenario.
- **Latency measurement from telemetrygen**: telemetrygen does not report per-request latency histograms. End-to-end latency requires a custom sender, which is out of scope. We measure throughput and resource usage only.
- **Statistical significance testing**: we report raw numbers with variance across 3 runs. Formal hypothesis testing is overkill for a blog-post benchmark.
- **Servicegraph / span_metrics benchmark**: these transforms are excluded from the collector configs to isolate tail sampling + load balancing performance.
- **Gateway tier benchmark**: the o11y-weekly architecture has gateway → loadbalancer → collector. The gateway handles logs+metrics+traces routing; this benchmark focuses on the traces path only (loadbalancer → collector). Adding the gateway tier would mix logs/metrics routing noise into the trace pipeline measurement.

## Rabbit holes

- **cAdvisor compatibility on WSL2**: cAdvisor may not work on WSL2 due to cgroup v2 issues. Cap: if cAdvisor fails, fall back to `docker stats` polling via a simple shell loop. Don't spend time debugging cAdvisor.
- **telemetrygen rate saturation**: at high rates (50k+), the generator itself may become the bottleneck. Cap: if achieved throughput is <90% of target rate for both systems equally, note it and move on. The comparison is still valid if both are bottlenecked by the same generator.
- **Prometheus scrape interference**: scraping at 5s adds minimal load but could interfere at very high throughput. Cap: accept the overhead — it's equal for both systems.
- **Tail sampling policy exact equivalence**: otelcontribcol uses two sequential processors; Sol uses one transform with first-match-wins. Proving they produce identical sampling decisions on the same traces is hard. Cap: use the same policy structure (latency + error-AND-not-4xx + probabilistic fallback) and document the architectural difference (double-buffer vs single-buffer) as a known fairness note. The throughput/resource comparison is valid even if sampling rates differ slightly.
- **telemetrygen trace structure for tail sampling**: telemetrygen produces simple traces (1-2 spans). Real-world traces have 10-50 spans with varying latency and error status. Cap: accept telemetrygen's simple traces — they still exercise the buffering, decision_wait, and policy evaluation paths. Note the limitation.
- **DNS resolution in Docker Compose for LB benchmark**: both loadbalancers use DNS to discover collector replicas. Docker Compose DNS returns all replica IPs for a service name, but resolution timing and caching differ between the `loadbalancing` exporter (Go) and Sol's DNS resolver (Rust). Cap: both use the same Docker network and same DNS; accept that resolver behavior is part of the implementation being benchmarked.
- **Collector replica count**: using 2 replicas keeps the benchmark simple. Real deployments use 3-10+. Cap: 2 replicas is enough to exercise the routing logic and measure overhead. Note the limitation.

## Design

### Architecture

**Noop benchmark:**
```
telemetrygen ──► sol             (otlp → blackhole)
             ──► otelcontribcol  (otlp → nop)
prometheus scrapes both + docker stats polls CPU/mem
```

**Tail sampling benchmark:**
```
telemetrygen ──► sol             (otlp → tail_sampling → blackhole)
             ──► otelcontribcol  (otlp → tail_sampling/latency-error
                                       → tail_sampling/probabilistic → nop)
prometheus scrapes both + docker stats polls CPU/mem
```

Note: otelcontribcol uses TWO sequential tail_sampling processors (traces are buffered twice, decision_wait applied twice). Sol uses ONE tail_sampling transform (single buffer). This is an architectural difference — otelcontribcol cannot express the same policy in a single processor because it lacks AND+first-match-wins composition. The report documents this.

**Load-balanced tail sampling benchmark** (full o11y-weekly topology):
```
                    Sol                                    otelcontribcol
telemetrygen ──► sol-lb ──┬──► sol-collector-1         otelcol-lb ──┬──► otelcol-collector-1
                          │    (tail_sampling→blackhole)             │    (tail_sampling×2→nop)
                          └──► sol-collector-2                       └──► otelcol-collector-2
                               (tail_sampling→blackhole)                  (tail_sampling×2→nop)

                    LB routing: consistent-hash on traceID via DNS discovery
                    Resource measurement: sum of CPU/mem across LB + 2× collector
```

### Measurement approach

1. **Throughput**: read from each system's internal metrics
   - Sol: `component_sent_events_total` on the blackhole sink
   - otelcontribcol: `otelcol_exporter_sent_metric_points` / `otelcol_exporter_sent_log_records` / `otelcol_exporter_sent_spans`
2. **Resource usage**: cAdvisor container metrics scraped by Prometheus
   - CPU: `container_cpu_usage_seconds_total` rate
   - Memory: `container_memory_rss`
3. **Per-scenario isolation**: each scenario runs sequentially (stop all → start infra → start systems → warm up → generate load → collect → stop)

### File structure

```
demo/benchmark/
├── README.md                          # how to run, methodology
├── run.sh                             # orchestrator script
├── compose.yml                        # all services (noop, tail-sampling, lb-tail-sampling)
├── sol/
│   ├── noop.yaml                      # OTLP source → blackhole sink
│   ├── tail-sampling.yaml             # OTLP source → tail_sampling → blackhole sink
│   ├── lb.yaml                        # OTLP source → OTLP gRPC sink (traceID LB → sol-collector)
│   └── lb-collector.yaml              # OTLP source → tail_sampling → blackhole sink
├── otelcontribcol/
│   ├── noop.yml                       # OTLP receiver → nop exporter
│   ├── tail-sampling.yml              # OTLP receiver → tail_sampling → nop exporter
│   ├── lb.yml                         # OTLP receiver → loadbalancing exporter (→ otelcol-collector)
│   └── lb-collector.yml               # OTLP receiver → tail_sampling → nop exporter
├── prometheus/
│   └── prometheus.yml                 # scrape config
└── results/                           # generated output (gitignored)
    ├── raw/                           # JSON from Prometheus queries + docker stats CSV
    └── RESULTS.md                     # summary tables
```

Decisions:
- [Null sink equivalence](./adrs/null-sink-equivalence.md)
- [Resource limits](./adrs/resource-limits.md)
- [Measurement source](./adrs/measurement-source.md)
- [Tail sampling policy equivalence](./adrs/tail-sampling-policy-equivalence.md)
- [Load balancing equivalence](./adrs/load-balancing-equivalence.md)

## Cross-cutting Concerns

- **Observability**: Prometheus collects metrics from both systems + cAdvisor. No additional tooling.
- **Versioning**: `compose.yml` pins exact image versions for both Sol and otelcontribcol. Results report includes versions.
- **Reproducibility**: `run.sh` captures system info (`uname -a`, `docker info`, `nproc`, `free -h`) at start and writes it to `results/system-info.txt`.
