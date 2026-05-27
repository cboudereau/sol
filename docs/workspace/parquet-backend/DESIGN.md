# Parquet Backend — Design Doc

## Context

The [parquet-multisignal](../parquet-multisignal/DESIGN.md) workspace defines how Sol writes OTLP logs, traces, and metrics as Parquet files. This workspace addresses the **read side**: how to query those Parquet files to serve Grafana dashboards via the Prometheus (PromQL), Tempo (TraceQL), and Loki (LogQL) APIs.

### Current architecture

```
OTLP sources → Sol pipeline → Grafana backends (Mimir, Tempo, Loki)
                                     ↑
                              Grafana queries via HTTP APIs
```

Each backend is a separate system with its own storage engine, query language, and API. Sol acts as a write proxy, forwarding signals to three independent backends.

### Target architecture

```
OTLP sources → Sol pipeline → Parquet files (one schema per signal type)
                                     ↑
                              Sol query backend (DataFusion)
                              serving Prometheus, Tempo, Loki HTTP APIs
                                     ↑
                              Grafana queries via same HTTP APIs
```

Sol becomes a self-contained observability backend: ingest OTLP, store as Parquet, query via DataFusion, serve Grafana-compatible APIs.

### Query traffic analysis (from pcap)

Analysis of a live Grafana dashboard session against the demo stack (`demo/otel-sol-grafana-dotnet/883cbd10a4e1.pcap`) revealed the following query patterns:

#### Tempo — Traces (12 queries)

| Query type | API | Example | Count |
|---|---|---|---|
| Trace search (TraceQL) | `GET /api/search?q=...` | `{resource.service.name="client" && name="GET /randomuser" && span.http.response.status_code=500}` | 1 |
| Trace by ID | `GET /api/v2/traces/:id` | `/api/v2/traces/3bc59070ba6c121cad3d88a3f889b303` | 1 |
| Tag value discovery | `GET /api/v2/search/tag/:tag/values?q=...` | Tag values for `name`, `status`, `http.response.status_code` filtered by TraceQL | 8 |
| Tag list | `GET /api/v2/search/tags` | List all available span tag names | 2 |

#### Loki — Logs (2 queries)

| Query type | API | Example | Count |
|---|---|---|---|
| Log range query (LogQL) | `GET /loki/api/v1/query_range` | `{service_name="client", service_version=~"1\\.0\\.0", deployment_environment="dev"}` backward, limit=1000 | 2 |

#### Mimir — Metrics (130 queries)

| Query type | API | Example | Count |
|---|---|---|---|
| Histogram quantile | `POST /prometheus/api/v1/query_range` | `histogram_quantile(0.95, sum(rate(..._bucket{service_name="client"}[1m])) by (le,...))` | 12 |
| Rate + sum by | `POST /prometheus/api/v1/query_range` | `topk(10, sum by(...) (rate(http_server_request_duration_seconds_count{...}[1m])))` | ~30 |
| Gauge / instant | `POST /prometheus/api/v1/query` | `node_memory_total_bytes{host="...",job="sol"}` | ~20 |
| Max over time | `POST /prometheus/api/v1/query_range` | `max by(gc_heap_generation) (max_over_time(dotnet_gc_last_collection_heap_size_bytes{...}[1m]))` | ~14 |
| Label discovery | `GET /prometheus/api/v1/label/:name/values` | Distinct values for `service_name`, `http_route`, `http_response_status_code`, etc. | 9 |
| Series existence | `GET /prometheus/api/v1/series` | `traces_service_graph_request_server_seconds` (service graph metric) | 2 |
| Sol internal metrics | `POST /prometheus/api/v1/query_range` | `sum(rate(sol_component_received_events_total{...}[1m]))` | ~12 |
| Node metrics | various | `node_cpu_seconds_total`, `node_memory_*`, `node_filesystem_*`, `node_network_*` | ~30 |

### Complexity tiers (DataFusion over Parquet)

| Tier | Query pattern | DataFusion approach | Key challenge |
|---|---|---|---|
| **Easy** | Log range queries | `WHERE service_name = ... AND time BETWEEN ...` + row group stats pruning | Straightforward scan + filter |
| **Easy** | Gauge instant queries | `WHERE name = ... AND labels match` + latest value | Simple filter |
| **Medium** | Trace search | `WHERE service_name = ... AND name = ...` + `json_extract(attributes, ...)` | JSON extraction in WHERE clause, no pushdown into JSON |
| **Medium** | Tag/label discovery | `SELECT DISTINCT json_extract(attributes, key)` | Full scan of JSON columns |
| **Medium** | Trace by ID | `WHERE trace_id = X'...'` | Random point lookup — needs bloom filter on `trace_id` |
| **Hard** | `rate()` | `LAG()` window function over `(PARTITION BY attributes ORDER BY time)` | Time-series ordering + windowed derivative |
| **Hard** | `sum by()`, `topk()`, `max_over_time()` | `GROUP BY` + window functions | Multi-step aggregation |
| **Very hard** | `histogram_quantile()` | CTE + UNNEST bucket arrays + cumulative window + interpolation | Multi-step SQL from JSON bucket data |

### PromQL → DataFusion SQL translation examples

**rate():**
```sql
WITH ordered AS (
  SELECT attributes, time_unix_nano, double_value,
    LAG(double_value) OVER w AS prev_value,
    LAG(time_unix_nano) OVER w AS prev_time
  FROM sum_metrics
  WHERE name = 'http_server_request_duration_seconds_count'
    AND service_name = 'client'
    AND time_unix_nano BETWEEN @start AND @end
  WINDOW w AS (PARTITION BY attributes ORDER BY time_unix_nano)
)
SELECT attributes, time_unix_nano,
  CASE WHEN double_value >= prev_value
    THEN (double_value - prev_value) / ((time_unix_nano - prev_time) / 1e9)
    ELSE double_value / ((time_unix_nano - prev_time) / 1e9)
  END AS rate_per_sec
FROM ordered WHERE prev_time IS NOT NULL
```

**histogram_quantile():**
```sql
WITH buckets AS (
  SELECT time_unix_nano, attributes,
    UNNEST(CAST(json_parse(bucket_counts) AS BIGINT[])) AS bucket_count,
    UNNEST(CAST(json_parse(explicit_bounds) AS DOUBLE[])) AS upper_bound,
    count AS total_count
  FROM histogram_metrics
  WHERE name = 'http_server_request_duration_seconds' AND service_name = 'client'
),
cumulative AS (
  SELECT *,
    SUM(bucket_count) OVER (PARTITION BY time_unix_nano, attributes ORDER BY upper_bound) AS cum_count,
    LAG(upper_bound) OVER (PARTITION BY time_unix_nano, attributes ORDER BY upper_bound) AS prev_bound
  FROM buckets
)
SELECT time_unix_nano,
  COALESCE(prev_bound, 0)
    + (upper_bound - COALESCE(prev_bound, 0))
    * (0.95 * total_count - LAG(cum_count) OVER (...))
    / NULLIF(cum_count - LAG(cum_count) OVER (...), 0) AS p95
FROM cumulative
WHERE cum_count >= 0.95 * total_count
```

### Pre-existing Sol features relevant to this workspace

- **`servicegraph` transform** (`src/transforms/servicegraph/`): computes `traces_service_graph_request_server_seconds` histogram metrics from trace spans at ingest time. Input: `DataType::Trace`, output: `DataType::Metric`. The cross-signal service graph metric is already materialized at ingest — no cross-signal query needed at read time.

### State of the art

| System | Storage | Query engine | Caching |
|---|---|---|---|
| **Grafana Mimir** | TSDB blocks (Prometheus format) | Custom PromQL engine | Query frontend with result cache (memcached/Redis) |
| **Grafana Tempo** | Parquet blocks + bloom filters | Custom TraceQL engine | Bloom filter cache, query frontend |
| **Grafana Loki** | Chunks + TSDB index | Custom LogQL engine | Chunk cache, query frontend |
| **SigNoz** | ClickHouse tables | ClickHouse SQL | ClickHouse query cache |
| **InfluxDB 3.0** | Parquet (Iceberg) | DataFusion | In-memory cache, query dedup |
| **GreptimeDB** | Custom columnar + Parquet | DataFusion-based | Write-ahead buffer as hot cache |

**Key observation**: InfluxDB 3.0 and GreptimeDB prove that DataFusion over Parquet is viable for production observability. Both use DataFusion as their core query engine over Parquet-stored data.

## Functional Requirements

### <a id="fr1"></a>FR1 — Prometheus-compatible HTTP API

Implement the Prometheus HTTP API endpoints observed in the pcap:
- `POST /prometheus/api/v1/query` — instant PromQL query
- `POST /prometheus/api/v1/query_range` — range PromQL query
- `GET /prometheus/api/v1/label/:name/values` — label value discovery
- `GET /prometheus/api/v1/series` — series existence check

Translate PromQL to DataFusion SQL over metric Parquet tables (gauge, sum, histogram, exp_histogram, summary).

### <a id="fr2"></a>FR2 — Tempo-compatible HTTP API

Implement the Tempo HTTP API endpoints observed in the pcap:
- `GET /api/search` — trace search with TraceQL filter
- `GET /api/v2/traces/:traceID` — trace by ID lookup
- `GET /api/v2/search/tags` — list available tag names
- `GET /api/v2/search/tag/:tag/values` — tag value discovery with TraceQL filter

Translate TraceQL to DataFusion SQL over the traces Parquet table.

### <a id="fr3"></a>FR3 — Loki-compatible HTTP API

Implement the Loki HTTP API endpoints observed in the pcap:
- `GET /loki/api/v1/query_range` — log range query with LogQL filter

Translate LogQL to DataFusion SQL over the logs Parquet table.

### <a id="fr4"></a>FR4 — DataFusion query engine integration

Integrate Apache DataFusion as the query engine:
- Register Parquet files as DataFusion table providers (one table per signal type + metric subtype)
- Configure predicate pushdown for `service_name`, `name`, and timestamp columns
- Enable Parquet bloom filters for `trace_id` column (trace-by-ID point lookups)
- Support JSON extraction functions for `attributes` columns

### <a id="fr5"></a>FR5 — Query result caching

Cache expensive query results to handle dashboard refresh patterns:
- The pcap shows identical queries repeated every ~15s (dashboard auto-refresh)
- Cache keyed by `(query_hash, time_range_bucket)` with configurable TTL
- In-memory LRU cache as default (no external dependency)
- Optional Redis backend for shared cache across query nodes

### <a id="fr6"></a>FR6 — Pre-computed aggregations (recording rules equivalent)

Support pre-computed aggregations for expensive PromQL patterns:
- `rate()` over counters — pre-compute per-second rates at ingest time
- `histogram_quantile()` — pre-compute common quantiles (p50, p95, p99) at ingest time
- Store pre-computed results as separate Parquet metric rows
- Fall back to real-time computation when pre-computed data is not available

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — DataFusion as the only query engine dependency

Use Apache DataFusion (Rust-native) as the sole query engine. No JVM (Spark), no embedded databases (DuckDB), no external query services. DataFusion is embeddable, scales via Ballista for distributed execution, and is proven for Parquet-stored observability data (InfluxDB 3.0, GreptimeDB).

### <a id="nfr2"></a>NFR2 — Grafana-compatible response formats

All API responses must be compatible with Grafana's data source plugins:
- Prometheus API: JSON response format per Prometheus HTTP API spec
- Tempo API: JSON response format per Tempo HTTP API spec
- Loki API: JSON response format per Loki HTTP API spec

No custom Grafana plugins — standard Prometheus, Tempo, and Loki data sources must work unchanged.

### <a id="nfr3"></a>NFR3 — Dashboard refresh latency

The pcap dashboard session involves ~130 queries per refresh. Target: complete all queries within 2 seconds for a single-node deployment with data volumes typical of the demo stack (~100 events/second). Caching (FR5) is expected to bring repeat refreshes under 500ms.

### <a id="nfr4"></a>NFR4 — Parquet file discovery

The query engine must discover and register Parquet files from configurable storage:
- Local filesystem (development, single-node)
- S3-compatible object storage (production)

File discovery is based on the naming convention defined in the [parquet-multisignal](../parquet-multisignal/DESIGN.md) codec design.

## Non-goals

- **Full PromQL/TraceQL/LogQL coverage**: only the subset observed in the pcap (and commonly used in Grafana dashboards) is in scope. Exotic functions (`predict_linear`, `absent_over_time`, TraceQL structural operators) are deferred.
- **Distributed query execution**: Ballista-based distributed execution is a future concern. Start with single-node DataFusion.
- **Write-ahead log / hot data**: queries run over finalized Parquet files only. Real-time tail (last few seconds of data not yet flushed to Parquet) is out of scope — the batch flush interval defines the query freshness boundary.
- **Multi-tenancy**: single-tenant deployment. Tenant isolation is a future concern.
- **Alerting / recording rules engine**: FR6 covers pre-computation at ingest time. A full recording rules engine (with rule evaluation loop, alert manager integration) is out of scope.

## Rabbit holes

1. **PromQL parser**: writing a full PromQL parser is a multi-month project. **Constraint**: use an existing Rust PromQL parser crate (e.g., `promql-parser`) or implement only the subset needed (rate, histogram_quantile, sum/avg/max/topk by, instant/range selectors). Decide in ADR.

2. **TraceQL parser**: TraceQL is less standardized than PromQL. **Constraint**: implement the subset observed in the pcap (attribute filters with `&&`, `=`, `!=`). No structural operators (span set operations) in v1.

3. **Parquet file lifecycle**: as new Parquet files are written, the query engine must discover them. Stale files must be pruned. **Constraint**: simple file-system polling with configurable retention, not a catalog system (Iceberg/Delta Lake).

4. **JSON attribute extraction performance**: every query that filters on span/metric attributes requires `json_extract` on the `attributes` column. This defeats Parquet predicate pushdown. **Constraint**: accept the performance cost for v1. Attribute promotion (materializing hot attributes as top-level columns) is a future optimization.

5. **Histogram bucket unnesting**: DataFusion's `UNNEST` over JSON-parsed arrays may have performance issues for large batch sizes. **Constraint**: benchmark with realistic histogram cardinality before committing to the JSON-unnest approach. Fall back to Rust-native histogram computation if SQL is too slow.

## Design

### Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Sol Process                        │
│                                                      │
│  ┌──────────┐    ┌──────────┐    ┌────────────────┐ │
│  │ OTLP     │───→│ Pipeline │───→│ Parquet Sink   │ │
│  │ Source    │    │          │    │ (codec writes)  │ │
│  └──────────┘    └──────────┘    └───────┬────────┘ │
│                                          │           │
│                                    Parquet files     │
│                                    (local / S3)      │
│                                          │           │
│  ┌───────────────────────────────────────┴────────┐ │
│  │            Query Backend                        │ │
│  │                                                  │ │
│  │  ┌─────────┐  ┌──────────┐  ┌───────────────┐  │ │
│  │  │PromQL   │  │TraceQL   │  │LogQL          │  │ │
│  │  │HTTP API │  │HTTP API  │  │HTTP API       │  │ │
│  │  └────┬────┘  └────┬─────┘  └──────┬────────┘  │ │
│  │       │             │               │            │ │
│  │  ┌────▼─────────────▼───────────────▼────────┐  │ │
│  │  │        Query Translator Layer              │  │ │
│  │  │  PromQL → SQL  TraceQL → SQL  LogQL → SQL │  │ │
│  │  └─────────────────────┬──────────────────────┘  │ │
│  │                        │                          │ │
│  │  ┌─────────────────────▼──────────────────────┐  │ │
│  │  │            Query Cache (LRU)               │  │ │
│  │  └─────────────────────┬──────────────────────┘  │ │
│  │                        │                          │ │
│  │  ┌─────────────────────▼──────────────────────┐  │ │
│  │  │         DataFusion SessionContext           │  │ │
│  │  │  ┌─────────┐ ┌──────┐ ┌─────┐ ┌─────────┐ │  │ │
│  │  │  │logs     │ │traces│ │gauge│ │histogram│ │  │ │
│  │  │  │(parquet)│ │(pqt) │ │(pqt)│ │(pqt)   │ │  │ │
│  │  │  └─────────┘ └──────┘ └─────┘ └─────────┘ │  │ │
│  │  └────────────────────────────────────────────┘  │ │
│  └──────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

### Query translation layer

Each query language is translated to DataFusion SQL:

| Source language | Target | Key functions |
|---|---|---|
| PromQL `rate(m{l=v}[1m])` | SQL `LAG()` window over sum/gauge tables | `PARTITION BY attributes ORDER BY time` |
| PromQL `histogram_quantile(q, ...)` | SQL CTE + UNNEST + interpolation over histogram table | JSON bucket array parsing |
| PromQL `sum by (l) (...)` | SQL `GROUP BY json_extract(attributes, l)` | JSON extraction |
| PromQL `topk(n, ...)` | SQL `ORDER BY value DESC LIMIT n` | Standard SQL |
| PromQL `max_over_time(m[1m])` | SQL `MAX() OVER (... ROWS BETWEEN ...)` | Window function |
| TraceQL `{resource.service.name="x" && name="y"}` | SQL `WHERE service_name = 'x' AND name = 'y'` | Top-level columns |
| TraceQL `{span.attr=v}` | SQL `WHERE json_extract(attributes, '$.attr') = 'v'` | JSON extraction |
| Trace by ID | SQL `WHERE trace_id = X'...'` | Bloom filter-accelerated |
| LogQL `{service_name="x"} \|= "text"` | SQL `WHERE service_name = 'x' AND body LIKE '%text%'` | Full-text on body column |

### Caching strategy

```
Query → hash(query, time_range_bucket) → LRU cache lookup
  ├─ HIT  → return cached result
  └─ MISS → DataFusion SQL → execute → cache result → return
```

- Time range bucketing: round `start`/`end` to nearest 15s boundary (matches typical dashboard refresh)
- TTL: configurable, default 15s (one dashboard refresh cycle)
- Max entries: configurable, default 1000
- Cache invalidation: TTL-based only (no active invalidation on new Parquet file arrival)

### Decisions

- [PromQL parsing strategy](./adrs/promql-parsing-strategy.md)
- [Query caching strategy](./adrs/query-caching-strategy.md)

## Cross-cutting Concerns

- **Grafana data source configuration**: Grafana connects to Sol's query backend using standard Prometheus, Tempo, and Loki data source configs. The endpoint URL changes from `http://mimir:9009` to `http://sol:9009` (or a configurable port). No custom plugins needed.
- **Parquet file schema dependency**: the query backend depends on the schema defined in [parquet-multisignal/DESIGN.md](../parquet-multisignal/DESIGN.md). Schema changes require coordinated updates to both the codec and the query engine table registrations.
- **Observability of the query backend**: expose query latency, cache hit rate, and DataFusion execution metrics as Sol internal metrics (`sol_query_*`). These feed back into the same pipeline (Sol monitoring Sol).
- **Service graph metrics**: the `servicegraph` transform already materializes `traces_service_graph_request_server_seconds` as `OtelMetric` events at ingest time. These flow through the pipeline into the histogram Parquet table. The query backend reads them as regular histogram metrics — no special handling needed.
