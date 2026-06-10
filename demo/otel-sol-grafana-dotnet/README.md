# Sol + Grafana LGTM stack

End-to-end observability demo with Sol as the telemetry pipeline, example apps (to produce telemetry), and the Grafana LGTM stack (Loki, Grafana, Tempo, Mimir).

## Architecture

Two .NET webapi applications (client and service) send OTLP telemetry to a three-tier Sol pipeline:

```mermaid
graph TD
    subgraph Apps
        client[client<br/>.NET webapi]
        service[service<br/>.NET webapi]
        db[(PostgreSQL)]
        client -- HTTP --> service
        service -- SQL --> db
    end

    subgraph Sol Pipeline
        gw[sol-gateway<br/>OTLP ingestion + host metrics]
        lb[sol-loadbalancer<br/>trace_id consistent hashing]
        col[sol-collector ×2<br/>tail sampling · span metrics · service graph]
    end

    client -- OTLP gRPC --> gw
    service -- OTLP gRPC --> gw

    gw -- traces --> lb
    lb -- traces --> col

    subgraph Grafana LGTM
        loki[Loki<br/>logs]
        mimir[Mimir<br/>metrics]
        tempo[Tempo<br/>traces]
        grafana[Grafana<br/>:3000]
        loki --> grafana
        mimir --> grafana
        tempo --> grafana
    end

    gw -- logs --> loki
    gw -- metrics --> mimir
    gw -- self metrics --> mimir
    col -- sampled traces --> tempo
    col -- span metrics + service graph --> mimir
```

### Sol pipeline roles

| Component | Config | Role |
|---|---|---|
| **sol-gateway** | [sol-gateway.yaml](./sol/sol-gateway.yaml) | OTLP ingestion, host metrics, resource attribute promotion, routes logs/metrics/traces to backends |
| **sol-loadbalancer** | [sol-loadbalancer.yaml](./sol/sol-loadbalancer.yaml) | Consistent-hash routing on `trace_id` via DNS resolution to sol-collector replicas |
| **sol-collector** | [sol-collector.yaml](./sol/sol-collector.yaml) | Tail sampling, span metrics, service graph generation, forwards to Tempo and Mimir |

### Backends

| Service | Purpose |
|---|---|
| **Grafana** | Dashboards and exploration (port 3000) |
| **Loki** | Log storage |
| **Mimir** | Metric storage (OTLP HTTP) |
| **Tempo** | Trace storage (OTLP gRPC) |
| **PostgreSQL** | Application database for the .NET service |

## Run locally

```bash
docker compose up -d
```

### build from source
```bash
# Use a locally built image
TAG=$(git rev-parse --short HEAD) && docker build -f ../Dockerfile.sol -t sol:$TAG ../.. && SOL_IMAGE=sol:$TAG bash ./up.sh

# analyze parquet files
./parquet-query.sh
```

Open Grafana: http://localhost:3000

## Key Sol features demonstrated

### Tail sampling

The sol-collector assembles full traces and applies policies before forwarding to Tempo:

```yaml
transforms:
  tail_sampling:
    type: tail_sampling
    inputs: ["otlp.traces"]
    decision_wait_secs: 10
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
            key: http.response.status_code
            values: [4..]
            enabled_regex_matching: true
            invert_match: true
```

### Trace-aware load balancing

The sol-loadbalancer routes all spans for a trace to the same collector replica using consistent hashing on `trace_id`:

```yaml
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
```

### Service graph

The sol-collector computes inter-service edge metrics from trace spans:

```yaml
transforms:
  servicegraph:
    type: servicegraph
    inputs: ["otlp.traces"]
    metrics_flush_interval_secs: 15
    dimensions: ["db.system", "messaging.system"]
    store:
      ttl_secs: 2
      max_items: 1000
```

### Host metrics

The sol-gateway collects host metrics and exports them with a `node_` prefix for node-exporter dashboard compatibility:

```yaml
sources:
  host_metrics:
    type: host_metrics
    scrape_interval_secs: 15
    namespace: ""
    resource_attributes:
      service.name: sol
```

## Application workload

Two .NET webapi applications generate realistic telemetry (logs, metrics, traces):

- **client** — sends HTTP requests to the service, simulating end-user traffic
- **service** — handles requests, queries a PostgreSQL database, and produces spans with configurable failure and latency ratios

Both applications use the [OpenTelemetry .NET SDK](https://github.com/open-telemetry/opentelemetry-dotnet) with OTLP gRPC export to the sol-gateway. Any OTLP-capable application can replace them — Sol is language-agnostic.

## Grafana dashboards

Three pre-provisioned dashboards are available at http://localhost:3000:

### Sol Pipeline

[Dashboard JSON](./grafana/provisioning/dashboards/Sol/SOL%20Pipeline.json)

Monitors the Sol pipeline internals — signal flows, per-component throughput, and error rates across all three Sol instances.

| Section | Panels | What it shows |
|---|---|---|
| **Signal flows** | Traces / Metrics / Logs (received vs sent) | End-to-end event flow through the pipeline |
| **Sources** | Received events/s, errors/s | Ingestion rate and source-level errors |
| **Transforms** | Received/sent events/s, drop ratio | Transform throughput and data reduction |
| **Sinks** | Sent events/s, retry rate, errors/s | Delivery health to Loki, Mimir, Tempo |
| **Tail sampling** | Sampled/dropped by policy, dropped too early, effective sampling ratio | Sampling decisions and trace completeness |
| **Service graph** | Inter-service edge metrics | Service-to-service call relationships |

### Node Exporter (Sol host_metrics)

[Dashboard JSON](./grafana/provisioning/dashboards/Sol/Node%20Exporter%20(host_metrics).json)

Host metrics collected by the sol-gateway's `host_metrics` source, exported with a `node_` prefix for compatibility with the standard Node Exporter Full dashboard (Grafana ID 1860). Panels include CPU, memory, swap, disk, filesystem, and network.

### OpenTelemetry .NET webapi

[Dashboard JSON](./grafana/provisioning/dashboards/Apps/OpenTelemetry%20dotnet%20webapi.json) | [Grafana.com](https://grafana.com/grafana/dashboards/20568-opentelemetry-dotnet-webapi/)

Application-level dashboard using the RED (Rate, Errors, Duration) and USE (Utilization, Saturation, Errors) methods. Shows ASP.NET request rates, error rates, latency distributions, runtime metrics (GC, thread pool), and HTTP client instrumentation for the .NET client and service applications.

### Parquet output

The sol-gateway writes all OTLP signals (logs, traces, metrics) to Parquet files alongside the Grafana backends. Each batch produces a complete, self-contained Parquet file with Zstd column compression.

```yaml
sinks:
  parquet_logs:
    type: file
    inputs: ["otlp.logs"]
    path: "/data/parquet/logs/%Y-%m-%d-%H-%M-%S.parquet"
    batch_encoding:
      codec: parquet
      compression: zstd
    batch:
      max_events: 5000
      timeout_secs: 30
```

To enable the DuckDB query container and inspect the files:

```bash
docker compose up -d
# Wait ~30s for the first batch to flush
./parquet-query.sh
```

Or run ad-hoc DuckDB queries:

```bash
docker compose exec duckdb duckdb -c "
  SELECT service_name, name, duration_nanos / 1e6 AS duration_ms
  FROM read_parquet('/data/parquet/traces/*.parquet')
  ORDER BY duration_nanos DESC LIMIT 10;
"
```

## Profiling the query backend (sol-querier)

The `sol-querier` service serves the Prometheus/Tempo/Loki + SQL APIs over the Parquet. A few ways to profile it, from cheapest to deepest.

### Aggregate latency (no setup)

The **SOL Querier Backend** dashboard plots `sol_query_request_duration_seconds` / `sol_query_inflight` / cache hit-rate — start here to see *which* API or signal is slow.

### Per-query engine cost — `EXPLAIN ANALYZE`

The SQL endpoint passes SQL straight to DataFusion, so `EXPLAIN ANALYZE` gives per-operator timing:

```bash
docker compose exec curl curl -s sol-querier:9009/api/v1/sql -XPOST \
  -d '{"sql":"EXPLAIN ANALYZE SELECT name, count(*) FROM metrics GROUP BY name"}'
```

### CPU flamegraph and async profiling (local run)

Snapshot the Parquet and point a local `sol-querier` at it:

```bash
docker compose cp duckdb:/data/parquet ./parquet-snapshot
cat > sol-querier-local.yaml <<'EOF'
querier:
  address: "127.0.0.1:9009"
  storage: { path: "./parquet-snapshot" }
EOF
```

Drive load in another terminal while profiling:

```bash
while :; do curl -s localhost:9009/prometheus/api/v1/query_range \
  --data-urlencode 'query=topk(10, sum by (http_response_status_code) (rate(http_server_request_duration_seconds_count{service_name="client"}[1m])))' \
  --data "start=$(date -d -10min +%s)&end=$(date +%s)&step=15" >/dev/null; done
```

**CPU flamegraph** (`[profile.release] debug = false`, so re-enable symbols via env):

```bash
cargo install flamegraph
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
CARGO_PROFILE_RELEASE_DEBUG=line-tables-only \
  cargo flamegraph --features querier-backend --bin sol -- --config demo/otel-sol-grafana-dotnet/sol-querier-local.yaml
# run the load loop ~30-60s, then Ctrl-C -> flamegraph.svg
```

**Async tasks** (`tokio-console` is built in; needs the `tokio_unstable` cfg):

```bash
cargo install --locked tokio-console
RUSTFLAGS="--cfg tokio_unstable" \
  cargo run --features querier-backend,tokio-console --bin sol -- --config demo/otel-sol-grafana-dotnet/sol-querier-local.yaml
# in another terminal (connects to 127.0.0.1:6669):
tokio-console
```

### CPU flamegraph of the running container

Host `perf` can sample the container process directly (image must keep release debug symbols):

```bash
PID=$(docker inspect -f '{{.State.Pid}}' "$(docker compose ps -q sol-querier)")
sudo perf record -F 99 -g -p "$PID" -- sleep 30   # drive load meanwhile
sudo perf script | inferno-collapse-perf | inferno-flamegraph > flame.svg
```

## Disclaimer

This demo is for local development and hands-on exploration only. Security, scaling, and high availability are not addressed.
