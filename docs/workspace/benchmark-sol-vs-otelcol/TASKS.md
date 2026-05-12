# benchmark-sol-vs-otelcol — Tasks

Design: [DESIGN.md](./DESIGN.md)

## Analysis

Build: `docker compose build` (in `demo/benchmark/`) — builds telemetrygen image
Test: `docker compose up -d sol otelcontribcol && docker compose ps` — both healthy
Lint: `shellcheck run.sh` — if available, otherwise N/A

### Known-failing tests

| Test | Reason | Action |
|---|---|---|
| (none) | | |

### Existing assets

- **telemetrygen Dockerfile**: `tests/e2e/opentelemetry-common/telemetrygen.Dockerfile` — Alpine + telemetrygen v0.137.0 binary
- **Sol Docker image**: `superbeeeeeee/sol:latest` (pre-built) or build via `demo/Dockerfile.sol`
- **otelcontribcol image**: `otel/opentelemetry-collector-contrib:0.122.0` (used in `demo/otel-drop-in/`)
- **Sol blackhole sink**: `src/sinks/blackhole/config.rs` — `print_interval_secs: 0` (default) disables output
- **Sol prometheus_exporter sink**: `src/sinks/prometheus/exporter.rs` — default port 9598, exposes `/metrics`
- **Sol internal_metrics source**: `type: internal_metrics` — emits `component_sent_events_total` etc.
- **otelcontribcol nop exporter**: available in contrib ≥ v0.111.0

### Key metrics mapping

| Metric | Sol | otelcontribcol |
|---|---|---|
| Events received | `component_received_events_total{component_id="otlp"}` | `otelcol_receiver_accepted_metric_points` / `_log_records` / `_spans` |
| Events sent (to sink) | `component_sent_events_total{component_id="blackhole"}` | `otelcol_exporter_sent_metric_points` / `_log_records` / `_spans` |
| CPU | `docker stats` → CPU % | `docker stats` → CPU % |
| Memory | `docker stats` → MEM USAGE | `docker stats` → MEM USAGE |

### telemetrygen capabilities

```
telemetrygen logs   --otlp-endpoint=HOST:PORT --otlp-insecure [--otlp-http] --logs=N --rate=R --workers=W --duration=Ds
telemetrygen traces --otlp-endpoint=HOST:PORT --otlp-insecure [--otlp-http] --traces=N --rate=R --workers=W --duration=Ds
telemetrygen metrics --otlp-endpoint=HOST:PORT --otlp-insecure [--otlp-http] --metrics=N --rate=R --workers=W --duration=Ds
```

- `--rate=0` means send as fast as possible (one-shot)
- `--duration=60s` with `--rate=10000` sends ~600k events total
- `--workers=4` runs 4 concurrent goroutines
- gRPC is default; `--otlp-http` switches to HTTP

### o11y-weekly original configs (reference for tail sampling)

Source: https://github.com/o11y-weekly/o11y-weekly.github.io/tree/main/2024-02-28_OpenTelemetry_Looks_Good_To_Me_dotnet

**otelcontribcol traces-collector pipeline** (two sequential tail_sampling processors):
```yaml
processors:
  tail_sampling/latency-error:
    decision_wait: 10s
    policies:
      - name: latency-policy
        type: latency
        latency: {threshold_ms: 100}
      - name: error-policy
        type: and
        and:
          and_sub_policy:
            - name: status_code-error-policy
              type: status_code
              status_code: {status_codes: [ERROR]}
            - name: http-status-code-error-policy
              type: string_attribute
              string_attribute:
                key: error.type
                values: [4..]
                enabled_regex_matching: true
                invert_match: true
  tail_sampling/probabilistic:
    policies:
      - name: probabilistic-policy
        type: probabilistic
        probabilistic: {sampling_percentage: 10}

service:
  pipelines:
    traces:
      processors: [tail_sampling/latency-error, tail_sampling/probabilistic, batch/tempo]
```

**Sol collector config** (single transform, first-match-wins):
```yaml
transforms:
  tail_sampling:
    type: tail_sampling
    decision_wait_secs: 10
    num_traces: 50000
    policies:
      - type: and
        name: sampled-latency-policy
        sub_policies:
          - type: latency
            name: latency-policy
            threshold_ms: 100
          - type: probabilistic
            name: probabilistic-policy
            sampling_percentage: 10.0
      - type: latency
        name: high-latency-policy
        threshold_ms: 500
      - type: and
        name: sampled-error-policy
        sub_policies:
          - type: status_code
            name: status-code-error-policy
            status_codes: ["ERROR"]
          - type: string_attribute
            name: http-status-code-error-policy
            key: error.type
            values: [4..]
            enabled_regex_matching: true
            invert_match: true
```

### File structure

```
demo/benchmark/
├── README.md
├── run.sh
├── compose.yml
├── sol/
│   ├── noop.yaml
│   ├── tail-sampling.yaml
│   ├── lb.yaml
│   └── lb-collector.yaml
├── otelcontribcol/
│   ├── noop.yml
│   ├── tail-sampling.yml
│   ├── lb.yml
│   └── lb-collector.yml
├── prometheus/
│   └── prometheus.yml
├── .gitignore
└── results/             (gitignored)
```

## Tasks

### 1. Create noop pipeline configs ([FR2](./DESIGN.md#fr2))

**Goal**: Write minimal, equivalent noop pipeline configs — OTLP in, null sink out, internal metrics exposed for Prometheus scraping.

**Constraints**:
- [ADR: null-sink-equivalence](./adrs/null-sink-equivalence.md) — Sol uses `blackhole` (print_interval_secs: 0), otelcol uses `nop` exporter
- Sol must expose internal metrics via `prometheus_exporter` sink on port 9598
- otelcol exposes metrics on port 8888 by default (via `telemetry.metrics.address`)
- No transforms, no processors, no batching config overrides
- Both accept gRPC on 4317 and HTTP on 4318

**Files to create**:
- `demo/benchmark/sol/noop.yaml` — opentelemetry source + internal_metrics source + blackhole sink + prometheus_exporter sink
- `demo/benchmark/otelcontribcol/noop.yml` — otlp receiver + nop exporter + telemetry metrics enabled

**Acceptance criteria**:
- [ ] Sol config starts without error
- [ ] otelcol config starts without error
- [ ] Both accept OTLP on gRPC:4317 and HTTP:4318
- [ ] Both sink to null (no output, no file, no network egress)
- [ ] Both expose Prometheus-scrapable metrics (Sol on :9598, otelcol on :8888)

**Depends on**: none
**Time-box**: ~15 min

### 1b. Create tail sampling pipeline configs ([FR7](./DESIGN.md#fr7))

**Goal**: Write equivalent tail sampling configs based on the o11y-weekly pipeline. Both must exercise trace buffering, policy evaluation, and decision caching.

**Constraints**:
- [ADR: tail-sampling-policy-equivalence](./adrs/tail-sampling-policy-equivalence.md) — equivalent intent, idiomatic configs
- otelcontribcol: two sequential `tail_sampling` processors (latency-error → probabilistic), matching the original o11y-weekly config
- Sol: single `tail_sampling` transform with first-match-wins, matching the Sol demo collector config
- Both use `decision_wait: 10s`, `num_traces: 50000`
- Both use `error.type` as the string attribute key for 4xx exclusion
- Sink to blackhole/nop (no network egress)
- Internal metrics exposed for Prometheus scraping

**otelcontribcol config** (from o11y-weekly, adapted for benchmark):
```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: "0.0.0.0:4317" }
      http: { endpoint: "0.0.0.0:4318" }
processors:
  tail_sampling/latency-error:
    decision_wait: 10s
    num_traces: 50000
    policies:
      - name: latency-policy
        type: latency
        latency: { threshold_ms: 100 }
      - name: error-policy
        type: and
        and:
          and_sub_policy:
            - name: status_code-error-policy
              type: status_code
              status_code: { status_codes: [ERROR] }
            - name: http-status-code-error-policy
              type: string_attribute
              string_attribute:
                key: error.type
                values: [4..]
                enabled_regex_matching: true
                invert_match: true
  tail_sampling/probabilistic:
    decision_wait: 10s
    num_traces: 50000
    policies:
      - name: probabilistic-policy
        type: probabilistic
        probabilistic: { sampling_percentage: 10 }
exporters:
  nop: {}
service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [tail_sampling/latency-error, tail_sampling/probabilistic]
      exporters: [nop]
```

**Sol config** (from demo collector, adapted for benchmark):
```yaml
sources:
  otlp:
    type: opentelemetry
    grpc: { address: "0.0.0.0:4317" }
    http: { address: "0.0.0.0:4318" }
  self_metrics:
    type: internal_metrics
transforms:
  tail_sampling:
    type: tail_sampling
    inputs: ["otlp.traces"]
    decision_wait_secs: 10
    num_traces: 50000
    policies:
      - type: and
        name: sampled-latency-policy
        sub_policies:
          - type: latency
            name: latency-policy
            threshold_ms: 100
          - type: probabilistic
            name: probabilistic-policy
            sampling_percentage: 10.0
      - type: latency
        name: high-latency-policy
        threshold_ms: 500
      - type: and
        name: sampled-error-policy
        sub_policies:
          - type: status_code
            name: status-code-error-policy
            status_codes: ["ERROR"]
          - type: string_attribute
            name: http-status-code-error-policy
            key: error.type
            values: [4..]
            enabled_regex_matching: true
            invert_match: true
sinks:
  blackhole:
    type: blackhole
    inputs: ["tail_sampling"]
  prometheus:
    type: prometheus_exporter
    inputs: ["self_metrics"]
    address: "0.0.0.0:9598"
```

**Files to create**:
- `demo/benchmark/sol/tail-sampling.yaml`
- `demo/benchmark/otelcontribcol/tail-sampling.yml`

**Acceptance criteria**:
- [ ] Sol config starts without error and accepts traces
- [ ] otelcol config starts without error and accepts traces
- [ ] Both buffer traces for `decision_wait` before forwarding/dropping
- [ ] Sol tail_sampling metrics are emitted (`tail_sampling_traces_sampled`)
- [ ] otelcol tail_sampling metrics are emitted (`otelcol_processor_tail_sampling_*`)

**Depends on**: none
**Time-box**: ~20 min

### 1c. Create load-balanced tail sampling pipeline configs ([FR9](./DESIGN.md#fr9))

**Goal**: Write loadbalancer + collector configs for both systems, mirroring the o11y-weekly multi-tier topology.

**Constraints**:
- [ADR: load-balancing-equivalence](./adrs/load-balancing-equivalence.md) — same topology (1 LB + 2 collectors), 1 CPU / 1 GB per container
- [ADR: tail-sampling-policy-equivalence](./adrs/tail-sampling-policy-equivalence.md) — collector tail sampling policies same as task 1b
- LB configs route by traceID using DNS-based discovery
- Collector configs are the tail-sampling configs from task 1b, adapted to listen on internal ports
- No servicegraph, no span_metrics — isolated tail sampling only
- Internal metrics exposed on each container for Prometheus scraping

**Sol LB config** (`sol/lb.yaml`, adapted from `demo/otel-sol-grafana-dotnet/sol/sol-loadbalancer.yaml`):
```yaml
sources:
  otlp:
    type: opentelemetry
    grpc: { address: "0.0.0.0:4317" }
    http: { address: "0.0.0.0:4318" }
  self_metrics:
    type: internal_metrics
sinks:
  otlp_traces:
    type: opentelemetry
    inputs: ["otlp.traces"]
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
  prometheus:
    type: prometheus_exporter
    inputs: ["self_metrics"]
    address: "0.0.0.0:9598"
```

**otelcontribcol LB config** (`otelcontribcol/lb.yml`, adapted from o11y-weekly traces-loadbalancer):
```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: "0.0.0.0:4317" }
      http: { endpoint: "0.0.0.0:4318" }
exporters:
  loadbalancing/collector:
    routing_key: "traceID"
    protocol:
      otlp:
        timeout: 1s
        tls: { insecure: true }
    resolver:
      dns:
        hostname: otelcol-collector
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [loadbalancing/collector]
```

**Collector configs** (`sol/lb-collector.yaml`, `otelcontribcol/lb-collector.yml`):
- Same as tail-sampling configs from task 1b
- Listen on gRPC 4317 only (internal, no host port)
- Sink to blackhole/nop

**Files to create**:
- `demo/benchmark/sol/lb.yaml`
- `demo/benchmark/sol/lb-collector.yaml`
- `demo/benchmark/otelcontribcol/lb.yml`
- `demo/benchmark/otelcontribcol/lb-collector.yml`

**Acceptance criteria**:
- [ ] Sol LB config starts and routes traces to `sol-collector` DNS target
- [ ] otelcol LB config starts and routes traces to `otelcol-collector` DNS target
- [ ] Collector configs start and accept traces from their respective LB
- [ ] Both LBs expose Prometheus-scrapable metrics
- [ ] Sol LB uses the standard OTLP sink with `load_balancing` (not a separate component)
- [ ] otelcol LB uses the `loadbalancing` exporter (dedicated component)

**Depends on**: task 1b
**Time-box**: ~20 min

### 2. Create Docker Compose file ([FR2](./DESIGN.md#fr2), [FR3](./DESIGN.md#fr3), [FR7](./DESIGN.md#fr7), [FR9](./DESIGN.md#fr9))

**Goal**: Define all services with resource limits and networking. Support both noop and tail-sampling configs via environment variable.

**Constraints**:
- [ADR: resource-limits](./adrs/resource-limits.md) — Sol and otelcol get 2 CPU / 2 GB each
- [ADR: measurement-source](./adrs/measurement-source.md) — Prometheus scrapes internal metrics; docker stats for resources
- telemetrygen uses the existing Dockerfile from `tests/e2e/opentelemetry-common/telemetrygen.Dockerfile`
- Sol image: `superbeeeeeee/sol:latest` (pre-built, matching demo/otel-drop-in pattern)
- otelcol image: `otel/opentelemetry-collector-contrib:0.122.0`
- Prometheus image: `prom/prometheus:v3.4.0` (or latest stable)
- All services on a shared network
- Config selection via env vars: `SOL_CONFIG=sol/noop.yaml` or `SOL_CONFIG=sol/tail-sampling.yaml` (same for otelcol)
- `run.sh` swaps configs between noop and tail-sampling scenario groups

**Services**:

Single-instance (noop + tail-sampling):
1. `sol` — Sol with configurable config file, 2 CPU / 2 GB
2. `otelcontribcol` — otelcol with configurable config file, 2 CPU / 2 GB
3. `prometheus` — scrapes all targets every 5s

Load-balanced (lb-tail-sampling):
4. `sol-lb` — Sol loadbalancer, 1 CPU / 1 GB
5. `sol-collector` — Sol collector with tail sampling, replicas: 2, 1 CPU / 1 GB each
6. `otelcol-lb` — otelcol loadbalancer, 1 CPU / 1 GB
7. `otelcol-collector` — otelcol collector with tail sampling, replicas: 2, 1 CPU / 1 GB each

LB services use Docker Compose profiles (e.g., `profiles: [lb]`) so they don't start for noop/tail-sampling-only scenarios.

**Files to create**:
- `demo/benchmark/compose.yml`
- `demo/benchmark/prometheus/prometheus.yml`

**Acceptance criteria**:
- [ ] `docker compose config` validates without error
- [ ] `docker compose up -d sol otelcontribcol prometheus` starts single-instance services
- [ ] `docker compose --profile lb up -d sol-lb sol-collector otelcol-lb otelcol-collector prometheus` starts LB services
- [ ] Prometheus targets show all active services as UP
- [ ] Sol and otelcol both accept OTLP traffic on their respective ports
- [ ] LB services route traces to their collector replicas

**Depends on**: task 1, task 1b, task 1c
**Time-box**: ~40 min

### 3. Write the benchmark runner script ([FR4](./DESIGN.md#fr4), [FR5](./DESIGN.md#fr5))

**Goal**: Single `run.sh` that executes all scenarios sequentially, collects results, and produces a summary.

**Constraints**:
- [ADR: measurement-source](./adrs/measurement-source.md) — hybrid: Prometheus for throughput, docker stats for resources
- [ADR: resource-limits](./adrs/resource-limits.md) — 2 CPU / 2 GB per system
- Script must be POSIX-compatible bash (no bashisms that break on older bash)
- Each scenario: warm up 10s → run telemetrygen for 60s → wait 5s drain → collect metrics
- `docker stats` polling loop runs in background, writes CSV to `results/raw/docker-stats-{scenario}.csv`
- Post-run: query Prometheus HTTP API (`/api/v1/query`) for throughput rates
- Capture system info at start

**Scenario matrix** (from [FR5](./DESIGN.md#fr5), [FR7](./DESIGN.md#fr7), [FR8](./DESIGN.md#fr8)):

Noop scenarios (config: `noop`):

| ID | Signal | Protocol | Rate | Workers | Duration |
|---|---|---|---|---|---|
| noop-logs-grpc-10k | logs | gRPC | 10000 | 4 | 60s |
| noop-logs-http-10k | logs | HTTP | 10000 | 4 | 60s |
| noop-traces-grpc-10k | traces | gRPC | 10000 | 4 | 60s |
| noop-traces-http-10k | traces | HTTP | 10000 | 4 | 60s |
| noop-metrics-grpc-10k | metrics | gRPC | 10000 | 4 | 60s |
| noop-metrics-http-10k | metrics | HTTP | 10000 | 4 | 60s |
| noop-logs-grpc-50k | logs | gRPC | 50000 | 8 | 60s |
| noop-traces-grpc-50k | traces | gRPC | 50000 | 8 | 60s |

Tail sampling scenarios (config: `tail-sampling`):

| ID | Signal | Protocol | Rate | Workers | Duration |
|---|---|---|---|---|---|
| tail-sampling-traces-grpc-10k | traces | gRPC | 10000 | 4 | 60s |
| tail-sampling-traces-grpc-50k | traces | gRPC | 50000 | 8 | 60s |

Load-balanced tail sampling scenarios (config: `lb`, profile: `lb`):

| ID | Topology | Rate | Workers | Duration |
|---|---|---|---|---|
| lb-tail-sampling-traces-grpc-10k | LB + 2× collector | 10000 | 4 | 60s |
| lb-tail-sampling-traces-grpc-50k | LB + 2× collector | 50000 | 8 | 60s |

Sustained memory scenarios (extended duration):

| ID | Config | Signal | Rate | Workers | Duration |
|---|---|---|---|---|---|
| sustained-noop-logs-grpc-10k | noop | logs | 10000 | 4 | 300s |
| sustained-tail-sampling-traces-grpc-10k | tail-sampling | traces | 10000 | 4 | 300s |

**For each scenario, the script**:
1. Starts the appropriate services (single-instance or LB topology) with correct configs
2. Warms up for 10s
3. Starts telemetrygen targeting BOTH systems simultaneously for `DURATION` seconds
4. Polls `docker stats` every 5s in background, writes CSV
5. Waits for drain, queries Prometheus for throughput rates
6. Writes per-scenario JSON to `results/raw/{scenario}.json`
7. Stops all services before next scenario (clean state)

For LB scenarios:
- telemetrygen targets the LB containers (sol-lb / otelcol-lb)
- `docker stats` captures all 3 containers per system (LB + 2× collector)
- Report shows aggregate CPU/memory per system

**Output files**:
- `results/raw/system-info.txt` — uname, nproc, free, docker info
- `results/raw/docker-stats-{scenario}.csv` — CPU%, MEM for all containers during scenario
- `results/raw/{scenario}.json` — throughput numbers from Prometheus
- `results/RESULTS.md` — summary tables (noop, tail-sampling, lb-tail-sampling, sustained memory)

**Acceptance criteria**:
- [ ] `bash run.sh` executes end-to-end without error on a machine with Docker
- [ ] Each scenario runs sequentially (no overlap)
- [ ] `results/raw/` contains per-scenario JSON and CSV files
- [ ] `results/RESULTS.md` contains a Markdown table with all scenarios
- [ ] Script captures system info in `results/raw/system-info.txt`
- [ ] Script is idempotent — can be re-run, cleans results/ at start

**Depends on**: task 2
**Time-box**: ~60 min

### 4. Write README and .gitignore ([FR6](./DESIGN.md#fr6))

**Goal**: Document how to run the benchmark and interpret results. Gitignore results/.

**Constraints**:
- Follow the style of `demo/otel-drop-in/README.md` — concise, architecture diagram, commands
- Include methodology section explaining fairness measures
- Include "how to reproduce" section with exact commands

**Files to create**:
- `demo/benchmark/README.md`
- `demo/benchmark/.gitignore` — ignore `results/`

**Acceptance criteria**:
- [ ] README includes architecture diagram (ASCII)
- [ ] README includes "Quick start" with copy-pasteable commands
- [ ] README includes "Methodology" explaining fairness (same resource limits, null sinks, etc.)
- [ ] README includes "Interpreting results" section
- [ ] `.gitignore` excludes `results/`

**Depends on**: task 3
**Time-box**: ~20 min

### 5. Dry-run validation — noop ([NFR1](./DESIGN.md#nfr1), [NFR2](./DESIGN.md#nfr2))

**Goal**: Run one noop scenario end-to-end to validate the infrastructure works.

**Constraints**:
- Run only `noop-logs-grpc-10k` scenario
- Duration override: 15s (not full 60s)
- Verify: Prometheus has data, docker-stats CSV is populated, RESULTS.md is generated

**Acceptance criteria**:
- [ ] Sol and otelcontribcol both start with noop config and accept traffic
- [ ] Prometheus shows both targets as UP
- [ ] telemetrygen sends 10k/s logs to both systems
- [ ] `results/raw/noop-logs-grpc-10k.json` contains throughput numbers > 0
- [ ] `results/raw/docker-stats-noop-logs-grpc-10k.csv` has rows for both containers
- [ ] `results/RESULTS.md` has a noop table with the logs-grpc-10k row filled

**Depends on**: task 4
**Time-box**: ~15 min

### 6. Dry-run validation — tail sampling ([NFR1](./DESIGN.md#nfr1), [FR7](./DESIGN.md#fr7))

**Goal**: Run one tail sampling scenario end-to-end to validate tail sampling configs and metrics collection.

**Constraints**:
- Run only `tail-sampling-traces-grpc-10k` scenario
- Duration override: 30s (needs at least `decision_wait` = 10s + load time)
- Verify: both systems buffer traces and emit tail sampling metrics

**Acceptance criteria**:
- [ ] Sol starts with tail-sampling config and accepts traces
- [ ] otelcol starts with tail-sampling config and accepts traces
- [ ] After `decision_wait` (10s), traces flow through to the sink
- [ ] Sol emits `tail_sampling_traces_sampled` metrics visible in Prometheus
- [ ] otelcol emits `otelcol_processor_tail_sampling_sampling_trace_dropped_too_early` or similar metrics
- [ ] `results/raw/tail-sampling-traces-grpc-10k.json` contains throughput numbers > 0
- [ ] `results/RESULTS.md` has a tail-sampling table with the traces-grpc-10k row filled
- [ ] Memory usage is reported for both containers

**Depends on**: task 5
**Time-box**: ~20 min

### 7. Dry-run validation — load-balanced tail sampling ([NFR1](./DESIGN.md#nfr1), [FR9](./DESIGN.md#fr9))

**Goal**: Run one LB scenario end-to-end to validate the multi-tier topology works.

**Constraints**:
- Run only `lb-tail-sampling-traces-grpc-10k` scenario
- Duration override: 30s
- Verify: LBs route to collectors, collectors do tail sampling, metrics from all containers are collected

**Acceptance criteria**:
- [ ] sol-lb and otelcol-lb start and accept traces
- [ ] sol-collector (2 replicas) and otelcol-collector (2 replicas) start and receive routed traces
- [ ] Traces are distributed across both collector replicas (verify via per-replica metrics)
- [ ] `results/raw/lb-tail-sampling-traces-grpc-10k.json` contains throughput numbers > 0
- [ ] `results/raw/docker-stats-lb-tail-sampling-traces-grpc-10k.csv` has rows for all 6 containers (3 per system)
- [ ] `results/RESULTS.md` has an lb-tail-sampling table with aggregate resource usage per system

**Depends on**: task 6
**Time-box**: ~20 min

## Sessions

### Session 1 — All configs (~60 min)
Tasks: 1, 1b, 1c, 2
**Skills**: `software-engineer`
**Checkpoint**: `cd demo/benchmark && docker compose config && docker compose up -d sol otelcontribcol prometheus && sleep 10 && curl -s http://localhost:9090/api/v1/targets | grep -c '"health":"up"'` — expect 2
**Commit point**: yes — commit after checkpoint passes

### Session 2 — Runner script + docs (~90 min)
Tasks: 3, 4
**Skills**: `software-engineer`
**Checkpoint**: `cd demo/benchmark && bash run.sh --scenario noop-logs-grpc-10k --duration 15 && test -f results/RESULTS.md && cat results/RESULTS.md`
**Commit point**: yes — commit after checkpoint passes

### Session 3 — Validation (~60 min)
Tasks: 5, 6, 7
**Skills**: `software-engineer`
**Checkpoint**: `cd demo/benchmark && bash run.sh --scenario noop-logs-grpc-10k --duration 15 && bash run.sh --scenario tail-sampling-traces-grpc-10k --duration 30 && bash run.sh --scenario lb-tail-sampling-traces-grpc-10k --duration 30 && grep "lb-tail-sampling" results/RESULTS.md`
**Commit point**: yes — commit with results validation

## Quality gates (post-session review)

- [ ] Acceptance criteria: all green above
- [ ] Code review: implementation matches [DESIGN.md](./DESIGN.md) intent
- [ ] Code organization: file placement follows `demo/benchmark/` structure
- [ ] Code quality: `run.sh` is readable, uses functions, handles errors with `set -euo pipefail`
- [ ] Security review: no secrets, no external network calls beyond Docker Hub pulls
- [ ] Reproducibility: results vary <5% across two consecutive runs on same machine
- [ ] Fairness: tail sampling report documents the architectural difference (1 processor vs 2) per [ADR](./adrs/tail-sampling-policy-equivalence.md)
- [ ] Fairness: LB report documents that both systems use the same topology (1 LB + 2 collectors) per [ADR](./adrs/load-balancing-equivalence.md)
