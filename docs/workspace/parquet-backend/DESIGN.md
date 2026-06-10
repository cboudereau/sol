# Parquet Backend — Design Doc

## Context

The [parquet-multisignal](../../designs/20260527_parquet-multisignal.md) workspace defines how Sol writes OTLP logs, traces, and metrics as Parquet files. This workspace addresses the **read side**: how to query those Parquet files to serve Grafana dashboards via the Prometheus (PromQL), Tempo (TraceQL), and Loki (LogQL) APIs.

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

#### Reanalysis — volume-weighted query mix (from `tcpdump -A` over the pcap)

Re-extracting all HTTP request lines and `/api/ds/query` bodies confirms the workload is **overwhelmingly metrics**:

| Datasource | Share of datasource queries | Endpoints hit |
|---|---|---|
| **Prometheus / Mimir** | ~95% (`"type":"prometheus"` ×121) | `query_range` ×101, `query` ×20 |
| **Tempo** | ~3% (`"type":"tempo"` ×4) | `tag-values` ×8, `traceql` markers ×68 (search refinement) |
| **Loki** | ~2% (`"type":"loki"` ×2) | `query_range` |

PromQL function frequency across the session (all refreshes): `count` ×194, `rate` ×183, `sum`/`sum by` ×84, `topk` ×48, `max_over_time` ×32, `avg` ×28, `histogram_quantile` ×24, `max by` ×10, `clamp_min` ×4. Metrics queried are .NET runtime (`dotnet_gc_*`, `dotnet_thread_pool_*`), ASP.NET (`http_server_request_duration_seconds_*`), and node-exporter (`node_cpu_*`, `node_memory_*`, `node_filesystem_*`).

**Design implication**: optimisation effort and the NFR cost/latency budget must be driven by the **PromQL `rate` + `histogram_quantile` over sum/histogram tables** path. Tempo and Loki are correctness features, not performance-critical.

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

#### InfluxDB 3.0 (IOx / FDAP stack) — the reference use case

InfluxDB 3.0 is the closest published design to this workspace: **F**light + **D**ataFusion + **A**rrow + **P**arquet. Its architecture is decoupled into four components that communicate only via a **catalog** and **object storage** ([InfluxData architecture blog](https://www.influxdata.com/blog/influxdb-3-0-system-architecture/), [storage-engine docs](https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/internals/storage-engine/)):

| Component | Role | Lesson for Sol |
|---|---|---|
| **Ingester** | Buffers writes in Arrow memory, flushes **sorted** Parquet at a size/time threshold | Sol's Parquet codec already writes the files — but it does **not sort** them. Sorting on low-cardinality columns is what gives InfluxDB 10–100× compression and fast pruning. |
| **Querier** | DataFusion over Parquet; also scans **not-yet-persisted** ingester data so queries see fresh data | Sol explicitly makes hot/unflushed data a [non-goal](#non-goals) — accept flush-interval freshness, skip this complexity. |
| **Compactor** | Merges many small flushed files into fewer large sorted/deduped files (InfluxDB default: new file ~every 15 min → compacted to ≤100 MB/day-of-data files; **InfluxDB's number, not Sol's** — Sol flushes ~30s) | **This is the critical lever.** Sol flushes one small file per batch per signal → a "small-files problem". Without compaction, DataFusion must list and open hundreds of files per query — InfluxDB users hit a 432-file query limit when fragmented ([fragmentation issue](https://github.com/influxdata/influxdb/issues/26785)). |
| **Garbage collector** | Deletes files superseded by compaction / past retention | Sol needs simple retention pruning, not a full GC. |

Two further InfluxDB facts shape Sol's cost/latency NFRs:
- **Parquet memory cache**: InfluxDB caches Parquet data/metadata in the querier; the default cache budget is **20% of available memory** ([config docs](https://docs.influxdata.com/influxdb3/enterprise/reference/config-options/)). Caching trades memory for latency.
- **Sort + columnar compression**: encoding is dramatically better when data is sorted on least-cardinality columns first ([ingest/compression blog](https://www.influxdata.com/blog/improved-data-ingest-compression-influxdb-3-0/)).

**What Sol should adopt vs. skip** (single-node, ~100 events/s demo target — not InfluxDB's distributed scale):

| InfluxDB technique | Sol decision | Why |
|---|---|---|
| DataFusion over Parquet | **Adopt** | Core of [NFR1](#nfr1). |
| Sorted Parquet (low-cardinality columns first) | **Adopt** (write-side hint + read-side assumption) | Cheapest large win for pruning + compression. |
| Compactor split from ingester | **Adopt** — standalone Parquet→Parquet component (sealed days), gateway stays dumb | Fixes the small-files problem without slowing ingest; the ingester/compactor split is InfluxDB's core lever ([FR7](#fr7)). |
| Footer/file-level provenance instead of a catalog DB | **Adopt** — supersession metadata in the compacted Parquet footer | Gets compaction consistency without Iceberg/Delta ([compaction-consistency ADR](./adrs/compaction-consistency.md)). |
| Bounded Parquet/metadata memory cache + query-result cache | **Adopt** (bounded, see [caching ADR](./adrs/query-caching-strategy.md)) | The memory⇄latency trade-off the user asked to balance. |
| Catalog DB, distributed ingester/querier/compactor split, Apache Flight | **Skip** | Over-engineered for single-node; Grafana talks HTTP, not Flight. |
| Deduplication / merge-on-read | **Skip** | Sol's Parquet output is append-only (no updates/upserts) → nothing to dedupe. |
| Querier reading unpersisted hot data (ingester-memory hot tier) | **Skip (v1)** | Hot data is a [non-goal](#non-goals); **this is precisely what caps Sol's freshness at the flush interval**. Loki/Mimir queriers read recent data from ingester RAM to get instant freshness; adding such a hot tier is the documented future escape ([NFR6](#nfr6) balance #2). |

## Functional Requirements

### <a id="fr1"></a>FR1 — Prometheus-compatible HTTP API

Implement the Prometheus HTTP API endpoints observed in the pcap:
- `POST /prometheus/api/v1/query` — instant PromQL query
- `POST /prometheus/api/v1/query_range` — range PromQL query
- `GET /prometheus/api/v1/label/:name/values` — label value discovery
- `GET /prometheus/api/v1/series` — series existence check

Translate PromQL to DataFusion SQL over metric Parquet tables (gauge, sum, histogram, exp_histogram, summary). Full endpoint surface + response schemas: [API-SPEC.md §1](./API-SPEC.md).

### <a id="fr2"></a>FR2 — Tempo-compatible HTTP API

Implement the Tempo HTTP API endpoints observed in the pcap:
- `GET /api/search` — trace search with TraceQL filter
- `GET /api/v2/traces/:traceID` — trace by ID lookup
- `GET /api/v2/search/tags` — list available tag names
- `GET /api/v2/search/tag/:tag/values` — tag value discovery with TraceQL filter

Translate TraceQL to DataFusion SQL over the traces Parquet table. Full endpoint surface + response schemas: [API-SPEC.md §3](./API-SPEC.md).

### <a id="fr3"></a>FR3 — Loki-compatible HTTP API

Implement the Loki HTTP API endpoints observed in the pcap:
- `GET /loki/api/v1/query_range` — log range query with LogQL filter

Translate LogQL to DataFusion SQL over the logs Parquet table. Full endpoint surface + response schemas: [API-SPEC.md §2](./API-SPEC.md).

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

### <a id="fr6"></a>FR6 — Metric downsampling / rollups for the long tail

Because metrics are queried over **13 months by default (2 years opt-in)** ([NFR7](#nfr7)), raw-resolution scans over the long tail are infeasible even with time-splitting. Serve metrics from **resolution tiers**, the query-frontend ([FR8](#fr8)) picking the tier from `(range, step)`:
- **Recent** (configurable, e.g. last N days): full-resolution raw Parquet — the correctness baseline computed in real time.
- **Cold tail**: pre-aggregated rollups (5m / 1h / 1d), produced by the compactor ([FR7](#fr7)) as separate `rollup-<tier>.parquet` files (separate `metrics_5m/1h/1d` tables, **excluded** from the lossless union). A rollup keeps the **last sample per (series, time-bucket)** — preserving real `bucket_counts` / counter values so `histogram_quantile` / `rate` stay correct after downsampling.
  - **Built from the compacted survivors**, not raw: `generate_rollup` reads `resolve_files` (the compacted daily + any non-superseded raw), so a tier is always (re)buildable from the daily and is **independent of raw GC** — raw can be reclaimed without losing the ability to roll up.
  - **Sealed-day only** (never the active day) and **idempotent**: one file per tier per partition, overwritten only when the source (the daily) is newer than the existing rollup. Rollup is *not* leveled/multi-pass — file count is bounded by `retention_days`.
- Rollups must preserve correctness for the dominant functions: store histogram **bucket counts** (not pre-computed quantiles) so `histogram_quantile` stays accurate after merge; store counter values so `rate` is recomputable across the coarser step.
- Fall back to real-time raw computation when a rollup tier is absent.

> This was previously framed as an optional ingest-time optimisation. The metrics **query interval** ([NFR7](#nfr7): 13 mo default, 2 y opt-in) makes it **required** to meet [NFR6](#nfr6) on the long tail — even the 13 mo default is infeasible at raw resolution for high cardinality. It does not apply to traces/logs (short query intervals).

### <a id="fr7"></a>FR7 — File layout + standalone compaction (Parquet → compacted Parquet)

Bound the small-files problem (the dominant cost driver per the InfluxDB comparison) so NFR5/NFR6 hold. Two parts — a cheap write-side hint and a separate compaction component:

- **Gateway hint (cheap, recommended, not sufficient):** the file sink writes under per-signal (and per-metric-subtype) directories with a time-partitioned path (`…/logs/dt=YYYY-MM-DD/*.parquet`, `…/metrics/gauge/dt=…/`, etc.) and sorts within each batch. Today the sink writes flat `…/logs/%Y-%m-%d-%H-%M-%S.parquet` per signal dir (metric subtypes share `metrics/`); the `dt=` + per-subtype layout is the proposed hint. This gives immediate day-level path pruning, but it does **not** merge the many small per-flush files, build rollups, or globally sort — so it cannot replace compaction. The gateway stays low-latency otherwise (small writes unchanged).
- **Compactor component (required):** a standalone **Parquet-in → compacted-Parquet-out** component — DataFusion sort-merge, sharing the querier's schemas/catalog — running as the **singleton** compactor (config `compactor:`, [NFR8](#nfr8)). It merges small files into few large globally-sorted files, builds metric **rollup tiers** ([FR6](#fr6)), and prunes per the configured retention policy. **Not** a distributed compactor/catalog/GC service.
- **Leveled compaction (level 0 → 1 → 2).** Files carry a footer `level` and the input filenames they **supersede**:
  - **L0 — raw**: gateway-written, one small file per flush (`<signal>/dt=YYYY-MM-DD/HH-MM-SS.parquet`; metrics also nested `metrics/<subtype>/dt=…/`).
  - **L1 — hourly (intra-day)**: each **completed hour** of the **active** day is merged into one file (`compacted-hHH-<date>.parquet`), once `now > end(hour) + hour_grace_secs` (default 10 min, for late arrivals). The in-progress hour is left raw. This bounds the *active* day's open-file count — the original design left the active day fully raw, which let it grow to thousands of files and exhaust the querier's file descriptors.
  - **L2 — daily (seal)**: a **sealed** day (older than `grace_days`, default 1) is merged into one `compacted-<date>.parquet` from its surviving L1 + leftover L0. The seal carries the prior daily's data forward, so it is lossless and idempotent even when a late raw arrives after sealing.
- **Supersession is transitive:** L2 supersedes L1 supersedes L0. `resolve_files` returns the surviving set (drop any file named in a superseding file's `supersedes`, regardless of level; rollups excluded), so a querier reads each datum exactly once.
- **Disk reclaim (deferred GC):** once a superseding compacted file is older than `delete_grace_secs` (default 60 s, **must exceed the querier `refresh_interval_secs`** so no querier still references the inputs in a registered table), the superseded inputs are **deleted** — reclaiming disk/inodes intra-day, not only at retention. POSIX unlink-while-open keeps in-flight scans safe. Reclaiming superseded inputs is GC, not correctness.
- **Crash safety:** each compacted file is staged to a hidden `.tmp`, **fsync'd**, renamed, then the directory is fsync'd — so a deletion never outlives a non-durable merge. Footer metadata is written before close, in the same fsync.
- **Consistency without a catalog:** all of the above lives in **Parquet footer key-value metadata** (`level`, `supersedes`), not an external catalog. Coverage references input *provenance*, not an event-time range, so late data stays orthogonal. See [compaction-consistency ADR](./adrs/compaction-consistency.md).
- Compaction is configurable (`compactor.intraday`, `grace_days`, `hour_grace_secs`, `delete_superseded`, `delete_grace_secs`, `retention_days`, `rollups`) and the whole component is disabled by simply omitting the `compactor:` section (write-heavy / low-query deployments).

### <a id="fr8"></a>FR8 — Time-range query splitting (query-frontend)

For long metric ranges, split a `query_range` into aligned sub-queries (default per-day, aligned to UTC midnight and `step`), execute them across the stateless querier replicas ([NFR8](#nfr8)), and merge:
- **Per-shard immutable caching**: completed historical shards never change → cache permanently; only the in-progress shard is uncacheable. This is what makes a 2y range refreshed every 15s cheap (729 cache hits + 1 live shard) and fixes the whole-range cache-key defect.
- **Boundary correctness**: range-vector functions (`rate`, `increase`) overlap shards by the lookback/range window and stitch; non-decomposable aggregations merge correctly across shards (`topk` = partial-topk-then-merge; `histogram_quantile` = sum bucket counts per series across shards, then compute).
- Traces and logs (≤30d) may skip splitting — the window is short enough. Splitting is primarily a metrics concern (it remains optional for the 30d trace/log windows).

### <a id="fr9"></a>FR9 — SQL query endpoint (cross-signal, the differentiator)

Expose the DataFusion `SessionContext` directly as a SQL endpoint, alongside the three Grafana-native APIs (FR1–FR3). PromQL/TraceQL/LogQL provide drop-in Grafana compatibility (the migration wedge); SQL provides the capability the three-language model **cannot** — unified analytics and **cross-signal correlation** ([MARKET §7.2](../../otlp-as-core-protocol-plan/MARKET.md)).

- **Endpoint**: `POST /api/v1/sql` (query in, results out). The three Grafana APIs already translate to SQL internally — this exposes the same engine raw, so it is nearly free.
- **Cross-signal JOINs** over the shared keys present in every schema: `trace_id` (logs ⨝ traces), `service_name` + time window (metrics ⨝ traces/logs). Enables the "impossible in Grafana Cloud" query — span p50 latency ⨝ error-log counts ⨝ CPU metric, in one statement.
- **Consumers**: Grafana's SQL data sources / SQL Expressions, plus external BI / dbt / Jupyter / DuckDB — "your observability data is a SQL table" ([MARKET §7.4](../../otlp-as-core-protocol-plan/MARKET.md)), reinforcing the own-your-data/open-format position.
- **Protocol (v1)**: HTTP with JSON results (+ optional Arrow/Arrow-stream for large results). **Postgres-wire and Arrow Flight SQL are deferred** (warrant their own ADR) — HTTP+JSON is the lowest-friction first step and is Grafana-consumable.
- **Same constraints as the querier**: stateless ([NFR8](#nfr8)), subject to the query guardrails ([NFR9](#nfr9): max bytes scanned, max range, max concurrency), reads the same compacted+rollup catalog with footer-supersession resolution.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — DataFusion as the only query engine dependency

Use Apache DataFusion (Rust-native) as the sole query engine. No JVM (Spark), no embedded databases (DuckDB), no external query services. DataFusion is embeddable and proven for Parquet-stored observability data (InfluxDB 3.0, GreptimeDB). (It *can* scale via Ballista for single-query distribution, but that is a [non-goal](#non-goals); read scaling here is by stateless querier replicas, [NFR8](#nfr8).)

**DataFusion extension crates from its own ecosystem are in scope** (they extend the engine, they do not replace it): `datafusion-functions-json` provides the JSON attribute extraction DataFusion core lacks ([JSON extraction ADR](./adrs/json-attribute-extraction.md)). The pinned set is datafusion / datafusion-functions-json / object_store / promql-parser.

### <a id="nfr2"></a>NFR2 — Grafana-compatible response formats

All API responses must be compatible with Grafana's data source plugins:
- Prometheus API: JSON response format per Prometheus HTTP API spec
- Tempo API: JSON response format per Tempo HTTP API spec
- Loki API: JSON response format per Loki HTTP API spec

No custom Grafana plugins — standard Prometheus, Tempo, and Loki data sources must work unchanged. The exact endpoint contracts (request params + response JSON schemas, with real bodies extracted from the pcap) are specified in [API-SPEC.md](./API-SPEC.md) — the acceptance target for the response builders.

### <a id="nfr3"></a>NFR3 — Dashboard refresh latency (superseded by [NFR6](#nfr6))

The pcap dashboard session involves ~130 queries per refresh. Target: complete all queries within 2 seconds for a single-node deployment with data volumes typical of the demo stack (~100 events/second). Caching (FR5) is expected to bring repeat refreshes under 500ms.

### <a id="nfr4"></a>NFR4 — Parquet file discovery

The query engine must discover and register Parquet files from configurable storage:
- Local filesystem (development, single-node)
- S3-compatible object storage (production)

File discovery is based on the naming convention defined in the [parquet-multisignal](../../designs/20260527_parquet-multisignal.md) codec design.

### <a id="nfr5"></a>NFR5 — Resource cost budget (CPU / memory)

In the default single-node deployment the querier runs **in-process** with the Sol ingest pipeline (the compactor is a separate component/role, [FR7](#fr7)/[NFR8](#nfr8)), so query work competes with ingestion for CPU and memory and must not starve it. On a single-node demo deployment (~100 events/s), with the dashboard issuing ~130 queries every 15s:

- **Memory**: total querier-backend footprint (DataFusion working set + Parquet/metadata cache + query-result cache) ≤ **256 MB** steady-state by default, and configurable. Inspired by InfluxDB's "cache = 20% of available memory" lever, but bounded by an absolute default rather than a percentage, because Sol co-hosts ingestion. Caches are the tunable knob: larger cache → lower latency → more memory.
- **CPU**: a dashboard refresh burst must not sustain >1 core-second of query CPU per refresh on demo data. The DataFusion `SessionContext` uses a **bounded** Tokio/Rayon worker pool (configurable, default = `min(4, available_parallelism)`), so query bursts cannot consume all cores and stall ingestion.
- **The small-files problem is the primary cost driver** (see InfluxDB comparison): per-query file listing + open + footer parse scales with file count. Compaction (FR7) and sort order keep file count and per-query scan cost bounded.

### <a id="nfr6"></a>NFR6 — Response-time budget and the cost/latency balance

Supersedes the latency target in NFR3 with an explicit trade-off contract:

| Path | Cold (cache miss) | Warm (cache hit) | Primary cost lever |
|---|---|---|---|
| Full dashboard refresh (~130 queries) | < **2 s** | < **500 ms** | query-result cache (FR5) |
| Single `rate()` / `sum by` range query | < **300 ms** | < **20 ms** | sort order + row-group pruning + file count (FR7) |
| `histogram_quantile()` range query | < **600 ms** | < **20 ms** | JSON-UNNEST cost ([rabbit hole 5](#rabbit-holes)); pre-compute is the FR6 escape hatch |
| Trace-by-id point lookup | < **150 ms** | n/a | `trace_id` bloom filter (FR4) |
| Log range / tag discovery | < **300 ms** | < **20 ms** | predicate pushdown |

**The balance** (the trade-off the user must be comfortable with):
1. **Memory vs latency** — caches are bounded (NFR5). If a deployment wants lower latency, it raises the cache budget; the default favours co-existing with ingestion over minimum latency.
2. **Freshness vs file quality (the flush-interval trade-off)** — three *distinct* cadences must not be conflated: the **flush interval** (~30s in the demo) sets ingest→queryable latency; the **rollup resolution** (5m/1h/1d, [FR6](#fr6)) is downsample granularity; the **compaction cadence** (sealed-day) is when files are merged. A *short* flush gives fresher data but more small files; a *long* flush gives larger files but staler data. **Compaction decouples these** — flush short to optimise freshness, compact later to optimise file size; do **not** lengthen the flush to get bigger files. Sweet spot ≈ the freshness SLA ≈ dashboard refresh (15–60s); below ~10s, tiny-file/PUT overhead outweighs the freshness gain. Because hot/unflushed data is a [non-goal](#non-goals), **Sol's freshness floor *is* the flush interval**; matching Grafana Cloud's instant freshness (Loki/Mimir ingesters serve recent data from RAM before flush; see the [InfluxDB comparison](#influxdb-30-iox--fdap-stack--the-reference-use-case)) would require a future **hot tier** — the querier also scanning an in-memory recent-events buffer.
3. **Write-amplification vs read-latency** — compaction (FR7) spends background CPU/IO to merge small files, buying lower query latency. On the demo target it is cheap; it is configurable and can be disabled for write-heavy/low-query deployments.
4. **Accuracy vs cost** — rollup/downsampling tiers (FR6) trade compaction CPU + storage for read-time latency on the metrics long tail. Required for long-range metrics ([NFR7](#nfr7): 13 mo default, 2 y opt-in), not optional.

### <a id="nfr7"></a>NFR7 — Per-signal query intervals (calibrated to Grafana Cloud)

This **query interval** — how far back a query reaches, *not* how long data is retained — drives the read-path optimisations (splitting, rollups, caching, partition pruning). Defaults are calibrated to [Grafana Cloud's per-signal retention](https://grafana.com/docs/grafana-cloud/cost-management-and-billing/manage-invoices/understand-your-invoice/logs-invoice/), which caps the max queryable range at the retention period:

| Signal | Default query interval (= max range) | Opt-in ceiling | Grafana Cloud reference | Partition | Splitting | Rollups |
|---|---|---|---|---|---|---|
| **Traces** | **30 days** | — | 30 d retention | day | no | no |
| **Logs** | **30 days** | — | 30 d retention; Loki `max_query_length` 30d1h | day | optional | no |
| **Metrics** | **13 months** (~395 d) | **2 years** | 13 mo retention | day → week/month (cold) | **required** ([FR8](#fr8)) | **required** ([FR6](#fr6)) |

- **Traces**: 30 d matches Grafana Cloud (an earlier draft used 7 d). 7 d remains available as a cost-saving config; 30 d is the default, for parity.
- **Metrics**: 13 mo default matches Grafana Cloud. **2 y is opt-in** and requires a **rollup-only cold tail** beyond ~395 d (raw aged out) — see [COMPLEXITY.md §7](./COMPLEXITY.md) (M2: 2 y raw at high cardinality is impractical).
- **Query interval ≠ retention.** Retention (deletion TTL) is a separate configurable policy enforced by the compactor GC ([FR7](#fr7)); it must be ≥ the query interval but is not defined by these numbers. The max range is enforced as a hard guardrail ([NFR9](#nfr9)). Metrics are the scaling case; traces/logs are short-interval special cases that skip the heavy long-range machinery.

### <a id="nfr8"></a>NFR8 — Horizontal read scalability (role separation)

The read path scales **out**, not just up. Because state lives in shared object storage (storage/compute separation), read scaling is by stateless replication — the lakehouse advantage over a stateful TSDB. Deployment roles (mirroring `mimir -target` / InfluxDB queriers):

- **Querier** — stateless; API translation + DataFusion over shared object storage. **Scales horizontally** behind a load balancer for query concurrency (the ~130 queries/refresh × N dashboards workload).
- **Query-frontend** (optional) — time-range splitting ([FR8](#fr8)) + a **shared** result cache (Redis/object-store) across queriers; this is the multi-node form of [FR5](#fr5).
- **Compactor** — **singleton**; the only writer of compacted/rollup files ([FR7](#fr7)). Must not be replicated.

Single-node "all components in one process" remains the default (per-process LRU cache, in-process compactor). Each component is the same binary, selected by **which config section is present** (`querier:` / `compactor:` — no `role:` field; a process may run both), so a deployment starts single-node and scales out without a rewrite. Resource isolation between ingestion and query uses the dual-runtime split (pipeline runtime > query runtime; ingestion never starves).

### <a id="nfr9"></a>NFR9 — Query guardrails (max range + max bytes scanned)

Mirror Grafana Cloud's query-protection limits ([Loki query-limit policies](https://grafana.com/docs/grafana-cloud/cost-management-and-billing/analyze-costs/logs-costs/log-query-limit-policies/)) so a single pathological query cannot blow the NFR5/NFR6 budgets. Enforced at **query validation, before execution**:

- **Max query range per signal** — traces/logs 30 d, metrics 13 mo (2 y when opt-in). Reject or clamp beyond, as Loki does (`max_query_length` 30d1h). Aligns with the [NFR7](#nfr7) query interval.
- **Max bytes scanned per query** — configurable, default ~**1 GB** (Grafana Cloud `maxQueryBytesRead` is 500 MB–1 GB). A query whose planned scan (post-pruning estimate) exceeds the budget is rejected, or forced onto a coarser rollup tier ([FR6](#fr6)).
- **Max concurrent queries per querier** and **max result series/points** — bound memory per node ([NFR5](#nfr5)).

On breach, return a clear Grafana-compatible error (e.g. HTTP 422 with a message naming the exceeded limit), never a silent truncation. These are the read-side analogue of the ingest pipeline's rate limits, and the enforcement point for the cost/latency contract in [NFR6](#nfr6).

### <a id="nfr10"></a>NFR10 — Object-store (S3) request-rate limits

The backend's cost/latency budgets assume S3-compatible object storage ([NFR4](#nfr4)), which imposes hard **per-prefix request-rate limits**: ~**5 500 GET/HEAD per second** and ~**3 500 PUT/POST/DELETE per second per prefix**, returning **`503 SlowDown`** when exceeded ([S3 performance guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)). These limits — not just $ — constrain the design (quantified in [COMPLEXITY.md §3a](./COMPLEXITY.md): a single dashboard refresh can approach the per-prefix GET ceiling, and is **100×+ over it without compaction**). Required mitigations, all already part of the design:

- **Prefix sharding**: the `dt=YYYY-MM-DD/` + per-signal/subtype layout ([FR7](#fr7)) spreads requests across prefixes; S3 scales rate *per prefix*, so aggregate throughput rises with prefix count.
- **Fewer files**: compaction ([FR7](#fr7)) cuts GETs-per-query by orders of magnitude — an S3-rate argument on top of latency.
- **Caching**: the query-result + per-day immutable cache ([FR5](#fr5)/[FR8](#fr8)) serve repeat refreshes with zero object-store requests — without it the 15 s refresh would hammer a single hot prefix.
- **Backoff & retry**: the querier/compactor must retry `503 SlowDown` with exponential backoff + jitter (the `object_store` crate provides this; it must be configured, not disabled).
- **Bounded LIST**: file discovery ([datafusion-table-discovery ADR](./adrs/datafusion-table-discovery.md)) uses paginated LIST (1 000 keys/page, rate-limited); the re-list interval and per-prefix object count are bounded by compaction + the refresh interval.

Local-filesystem deployments are not subject to these limits (sub-ms opens, no per-prefix cap); the constraints apply to the S3/object-store production target ([NFR8](#nfr8)).

## Non-goals

- **Query/API coverage**: the **full read/query API surface** of Mimir/Loki/Tempo is targeted ([API-SPEC.md](./API-SPEC.md)), and the **full query-language surface** is mapped with explicit per-construct trade-off decisions ([QUERY-MAPPING.md](./QUERY-MAPPING.md)) — *not* just the pcap subset. What stays out: genuinely unbounded or hot-data constructs (`predict_linear`/`holt_winters`/subqueries, `absent*`, TraceQL structural operators, TraceQL metrics, live tail) — deferred, with the [SQL endpoint (FR9)](#fr9) as the escape hatch. Ingestion and admin/ring/config endpoints are out (read-only backend).
- **Single-query distribution (Ballista)**: splitting *one* query across nodes (intra-query parallelism) is deferred — the workload is many *small* queries, not one giant scan. **Horizontal scaling for query concurrency is NOT a non-goal** — it is required (see [NFR8](#nfr8)): the backend runs as stateless querier replicas over shared object storage, scaled behind a load balancer. The two are different axes; only Ballista-style single-query distribution is out of scope.
- **Write-ahead log / hot data**: queries run over finalized Parquet files only. Real-time tail (last few seconds of data not yet flushed to Parquet) is out of scope — the batch flush interval defines the query freshness boundary.
- **Multi-tenancy**: single-tenant deployment. Tenant isolation is a future concern.
- **Alerting / recording rules engine**: FR6 covers pre-computation at compaction time (rollup tiers). A full recording rules engine (with rule evaluation loop, alert manager integration) is out of scope.

## Rabbit holes

1. **PromQL parser**: writing a full PromQL parser is a multi-month project. **Constraint**: use an existing Rust PromQL parser crate (e.g., `promql-parser`) or implement only the subset needed (rate, histogram_quantile, sum/avg/max/topk by, instant/range selectors). Decide in ADR.

2. **TraceQL parser**: TraceQL is less standardized than PromQL. **Constraint**: implement the subset observed in the pcap (attribute filters with `&&`, `=`, `!=`). No structural operators (span set operations) in v1.

3. **Parquet file lifecycle**: as new Parquet files are written, the query engine must discover them. **Constraint**: simple file-system / object-store re-listing, not a catalog system (Iceberg/Delta Lake). Consistency between raw and compacted files uses footer-level supersession metadata on the sealed-day boundary ([FR7](#fr7), rabbit hole 6), not a transactional catalog.

4. **JSON attribute extraction performance**: every query that filters on span/metric attributes requires `json_extract` on the `attributes` column. This defeats Parquet predicate pushdown. **Constraint**: accept the performance cost for v1, using the `datafusion-functions-json` extension ([JSON extraction ADR](./adrs/json-attribute-extraction.md)) — its `jiter`-backed lazy parser is materially cheaper than full `serde_json` document parsing, but it still parses per row. **State of the art (deferred — would supersede the JSON-string design, own ADR):** stop storing attributes as a JSON string at all. Two industry approaches: (a) **ClickHouse `JSON`/`Object` columns** auto-materialise frequently-seen keys into real *subcolumns* on write, so `attributes.host` reads as a native column (O(1) columnar, no per-row parse) — the InfluxDB 3 / GreptimeDB equivalent is promoting tags to top-level columns; (b) the **Parquet/Arrow `VARIANT` type + shredding spec** (Spark 4, 2024–25 Arrow/Parquet) stores semi-structured data in a binary, partially-columnarised form with the same effect. Sol's "attribute promotion" (materialising hot attributes as top-level Parquet columns at compaction time) is the pragmatic on-ramp to (a) and the recommended future optimisation.

5. **Histogram bucket unnesting**: DataFusion's `UNNEST` over JSON-parsed arrays may have performance issues for large batch sizes. **Constraint**: benchmark with realistic histogram cardinality before committing to the JSON-unnest approach. Fall back to Rust-native histogram computation if SQL is too slow. **Decision (v1, task 6):** Rust-native chosen up front. DataFusion 53 cannot reliably *zip* two parallel JSON-array string columns (`bucket_counts`, `explicit_bounds`) via `UNNEST` — there is no `json_parse`→array, and multiple `UNNEST`s in one projection have zip-vs-cross-join ambiguity. So `handle_histogram` selects the latest OTLP histogram row per series and interpolates the quantile in Rust (`histogram_quantile(φ, counts, bounds)` — linear within the matched bucket, `+Inf` bucket → last finite bound, empty → `None`). Bounded, unit-testable, no UNNEST risk; the SQL-UNNEST path stays available if a future benchmark justifies it.

6. **Read/compact consistency without a catalog**: a querier must read each datum exactly once while the compactor merges files. **Constraint**: only **sealed** partitions are compacted (never the active day); the compacted output declares in its **footer** which inputs it supersedes (+ a `level`); queriers resolve by level and skip superseded inputs. Coverage is by input provenance, not event-time range — late data is bounded and accepted (hot data is a non-goal). Do **not** attempt to mutate input files' state (Parquet footers are immutable; N-file flag flips are not atomic).

## Design

### Architecture — tiers (agent/client side vs backend side)

The system splits into a **write/ingest tier** (agent → gateway, unchanged from the demo) and a **read/backend tier** (compactor + querier + query-frontend). **Object storage is the only contact point** between them — the lakehouse storage/compute boundary. Every box is the same Sol binary; the component a process runs is selected by **which config section is present** — `querier:` and/or `compactor:` (there is no `role:` field; presence enables the component, and a process may run both).

```mermaid
flowchart TB
    subgraph AGENT["AGENT / CLIENT SIDE — write & ingest (unchanged)"]
        direction TB
        app["App + OTLP SDK"]
        coll["sol-collector (agent)"]
        lb["sol-loadbalancer"]
        gw["sol-gateway: OTLP source → transforms → sinks<br/>file sink → Parquet (dt-partitioned, small files)<br/>+ OTLP → Mimir/Tempo/Loki (demo)"]
        app --> coll --> lb --> gw
    end

    store[("Object storage / FS<br/>Parquet, dt=YYYY-MM-DD/<br/>L0 raw · L1 hourly · L2 daily · rollups<br/>footer: level, supersedes")]

    subgraph BACKEND["BACKEND SIDE — query & compaction"]
        direction TB
        comp["Compactor — singleton (compactor:)<br/>L0→L1 hourly (active day) · L1+L0→L2 daily (seal)<br/>rollups 5m/1h/1d from compacted survivors<br/>footer provenance · deferred GC of superseded · retention"]
        subgraph QR["Querier — stateless, scales out"]
            direction TB
            api["PromQL / TraceQL / LogQL HTTP APIs"]
            tr["Translator → DataFusion SQL"]
            cat["ParquetCatalog + resolve_files<br/>(skip superseded inputs)"]
            df["DataFusion SessionContext<br/>logs·traces·gauge·sum·histogram·exp·summary"]
            api --> tr --> cat --> df
        end
        qf["Query-frontend — optional<br/>time-split + merge + shared cache"]
    end

    gw -- small raw files --> store
    store -- read raw+compacted --> comp
    comp -- compacted + rollups --> store
    df <-- scan (pruned) --> store
    grafana["Grafana<br/>Prometheus/Tempo/Loki data sources"] --> qf --> QR

    classDef agent fill:#e8f0fe,stroke:#4285f4;
    classDef backend fill:#e6f4ea,stroke:#34a853;
    classDef storage fill:#fef7e0,stroke:#f9ab00;
    class app,coll,lb,gw agent;
    class comp,api,tr,cat,df,qf backend;
    class store storage;
```

**Tier responsibilities:**

| Tier | Role | State | Scaling | Designed in |
|---|---|---|---|---|
| Agent / client | collector → loadbalancer → **gateway** (OTLP in, transforms, file sink → Parquet) | streaming buffers | by throughput | existing demo + [parquet-multisignal](../../designs/20260527_parquet-multisignal.md); this workspace only adds the `dt=` path hint ([FR7](#fr7)) |
| Backend — **querier** | API translation + DataFusion over shared storage; `resolve_files` honours footer provenance | **stateless** | **horizontal** ([NFR8](#nfr8)) | [FR1](#fr1)–[FR4](#fr4), tasks 1–9 |
| Backend — **query-frontend** | time-split + merge + shared result cache | cache only | horizontal | [FR8](#fr8), task 11 |
| Backend — **compactor** | leveled compaction (hourly→daily), rollups, footer provenance, deferred GC + retention | owns compacted files | **singleton** | [FR6](#fr6)/[FR7](#fr7), tasks 10, 12 |

Single-node default = all backend components in one process (querier in-process, in-process compactor, per-process cache); the same binary splits into separately-deployed components — by presence of the `querier:` / `compactor:` config sections — to scale out without a rewrite.

### <a id="signal-lifecycle"></a>Signal lifecycle (ingest → compaction → query)

A datum's path from flush to query, and how the compactor's leveled tiers,
rollups, and GC interleave with reads. The compactor runs one pass every
`compactor.interval_secs`; each pass does **intraday → seal → rollup → GC** in
that order (the order matters — see below).

```mermaid
sequenceDiagram
    autonumber
    participant GW as Gateway (file sink)
    participant FS as Storage (Parquet, dt=…/)
    participant CO as Compactor (singleton)
    participant QR as Querier (stateless)
    participant GF as Grafana

    Note over GW,FS: Ingest — one small L0 file per flush (~30s)
    GW->>FS: write L0 raw  SIG/dt=DAY/HH-MM-SS.parquet

    loop every compactor.interval_secs  (intraday → seal → rollup → GC)
        Note over CO,FS: 1. Intraday — active day, completed hours only
        CO->>FS: read L0 of hour H  (now > end(H)+hour_grace_secs)
        CO->>FS: write L1  compacted-hHH-DAY.parquet  (supersedes those L0)

        Note over CO,FS: 2. Seal — sealed day (older than grace_days)
        CO->>FS: read survivors (L1 + leftover L0) via resolve_files
        CO->>FS: write L2  compacted-DAY.parquet  (supersedes them; lossless, idempotent)

        Note over CO,FS: 3. Rollup — metrics only, sealed day, from compacted survivors
        CO->>FS: read resolve_files survivors (the L2 daily)
        CO->>FS: write rollup-5m/1h/1d.parquet  (skip if newer than source)

        Note over CO,FS: 4. GC — reclaim what is safely superseded
        CO->>FS: delete inputs whose superseder is older than delete_grace_secs
        CO->>FS: delete partitions older than retention_days
    end

    Note over QR,FS: Query — read each datum once
    GF->>QR: PromQL / TraceQL / LogQL / SQL
    QR->>FS: resolve_files(dir) → surviving files only (skip superseded, exclude rollups)
    Note right of QR: coarse step → route to rollup tier (metrics_5m/1h/1d)
    FS-->>QR: pruned column/row-group scan (DataFusion)
    QR-->>GF: Grafana-shaped JSON
```

**Why the pass order is intraday → seal → rollup → GC.** Rollup reads the
compacted survivors, so it must run *after* seal (the daily exists) and *before*
GC in the same pass is irrelevant to it (it no longer needs raw) — but GC must
run last so a querier that re-registered before the superseding file appeared
has a full `refresh_interval` to drop the inputs first. `delete_grace_secs`
enforces that window.

**Compaction vs rollup — two different mechanisms in the same dir:**

| | Compaction (`compacted-*`) | Rollup (`rollup-*`) |
|---|---|---|
| Operation | **lossless** sort-merge (L0→L1→L2) | **lossy** downsample (last sample per bucket) |
| Goal | fewer files → fewer fds, faster scans | fewer rows → cheap long-range queries |
| Signals | logs, traces, metrics | **metrics only** |
| Provenance | `level` + `supersedes`; transitively deduped by `resolve_files` | none — **excluded** from `resolve_files` (separate tier tables) |
| File count | many → few, GC deletes superseded inputs | one per tier per sealed day (bounded by retention) |
| Lifecycle | active day hourly-compacted, sealed day → one daily | built once per sealed day from the daily, idempotent |

Net effect on the demo: the active day stays at ~tens of files (current hour raw
+ completed-hour L1) instead of thousands; sealed days collapse to one L2 +
three rollup tiers; superseded inputs are reclaimed within `delete_grace_secs`
rather than lingering until retention.

### Query translation layer

Each query language is translated to DataFusion SQL. The **authoritative, full-surface mapping** (every construct, with its ✅ native / ⚠️ cost-flagged / ⛔ restricted decision) lives in [QUERY-MAPPING.md](./QUERY-MAPPING.md); the table below is an illustrative excerpt:

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

- [Query backend process integration](./adrs/querier-backend-process-integration.md) — how the server embeds in the Sol process (mirrors `src/api/`)
- [DataFusion table registration and Parquet file discovery](./adrs/datafusion-table-discovery.md) — `ListingTable` per signal, periodic re-listing, dependency gating
- [File layout and compaction strategy](./adrs/file-layout-and-compaction-strategy.md) — sort order, lightweight compaction, cache budget (the NFR5/NFR6 cost/latency balance)
- [Deployment roles and horizontal read scaling](./adrs/deployment-roles-and-read-scaling.md) — querier / query-frontend / singleton compactor; dual-runtime isolation (NFR8)
- [Long-range metrics strategy](./adrs/long-range-metrics-strategy.md) — time-partitioned layout, per-day time-splitting, rollup tiers (FR6/FR8/NFR7)
- [Compaction consistency](./adrs/compaction-consistency.md) — standalone Parquet→Parquet compactor, sealed-day cadence, footer supersession metadata (no catalog)
- [PromQL parsing strategy](./adrs/promql-parsing-strategy.md)
- [Query caching strategy](./adrs/query-caching-strategy.md)
- [JSON attribute extraction](./adrs/json-attribute-extraction.md) — `datafusion-functions-json` extension over a hand-rolled UDF; attribute-promotion / Variant as the deferred SOTA (rabbit hole #4)
- [Grafana datasource API conformance](./adrs/grafana-datasource-api-conformance.md) — what response contract Sol targets (no single OpenAPI; Mimir OpenAPI + Tempo `tempopb` + Grafana `pkg/tsdb`/datasource source), validated by paired-diff against the real backends ([NFR2](#nfr2))

**Analysis artifacts (Phase 4a gate, before implementation):**
- [COMPLEXITY.md](./COMPLEXITY.md) — cost/complexity model (logs/metrics/traces) at demo / midpoint / ceiling vs AWS pricing; validates compaction/rollups/splitting and the beat-Loki / parity-Tempo / lose-to-Mimir-on-storage verdicts.
- [QUERY-MAPPING.md](./QUERY-MAPPING.md) — full-surface PromQL/LogQL/TraceQL → SQL with per-construct trade-off decisions.
- [API-SPEC.md](./API-SPEC.md) — Grafana-compatible HTTP contracts per backend (request params + response JSON), grounded in real pcap response bodies; the NFR2 acceptance target.

> **Note (analysis):** Sol's existing [`lib/prometheus-parser`](../../../lib/prometheus-parser/) parses the Prometheus **text exposition format**, not PromQL queries. The `promql-parser` crate decision is unaffected — the two solve different problems.

## Cross-cutting Concerns

- **Grafana data source configuration**: Grafana connects to Sol's query backend using standard Prometheus, Tempo, and Loki data source configs — only the URL changes (e.g. `http://sol-querier:9009/prometheus`). No custom plugins.
- **Demo integration (parallel, dual-write)**: in `demo/otel-sol-grafana-dotnet/`, the gateway **dual-writes** every signal — OTLP → Mimir/Tempo/Loki **and** Parquet → Sol — so both backends hold identical data. A `sol-querier` service serves the APIs over the shared `parquet-data` volume. Grafana gets **parallel** `Sol-Prometheus`/`Sol-Tempo`/`Sol-Loki` datasources next to the existing ones, and every demo dashboard uses a **datasource template variable** so a user flips Sol ↔ Grafana backend from a dropdown (side-by-side parity + latency comparison). Tasked in [TASKS.md](./TASKS.md) tasks 14–15.
- **Parquet file schema dependency**: the query backend depends on the schema defined in [parquet-multisignal/DESIGN.md](../../designs/20260527_parquet-multisignal.md). Schema changes require coordinated updates to both the codec and the query engine table registrations.
- **Observability of the query backend (Sol monitoring Sol)**: the backend emits internal metrics that flow through the same pipeline (`internal_metrics` source → Mimir), exactly like the existing `sol_component_*` / `sol_tail_sampling_*` metrics. The catalog (dashboarded in `demo/.../grafana/.../Sol/SOL Querier Backend.json`):

  | Metric | Type | Labels | Watches (NFR) |
  |---|---|---|---|
  | `sol_querier_requests_total` | counter | `api,signal,status` | throughput / error rate |
  | `sol_querier_request_duration_seconds` | histogram | `api,signal` | [NFR6](#nfr6) latency budget (p50/p95/p99) |
  | `sol_querier_bytes_scanned` | histogram | `signal` | [NFR5](#nfr5)/[NFR9](#nfr9) scan budget |
  | `sol_querier_files_opened` | histogram | `signal` | small-files / compaction effect ([FR7](#fr7)) |
  | `sol_querier_cache_requests_total` | counter | `cache(result\|metadata\|shard),result(hit\|miss)` | [FR5](#fr5)/[FR8](#fr8) hit rate |
  | `sol_querier_cache_memory_bytes`, `sol_querier_inflight` | gauge | — | [NFR5](#nfr5) budget / concurrency |
  | `sol_querier_rejected_total` | counter | `reason(range\|bytes\|concurrency)` | [NFR9](#nfr9) guardrails |
  | `sol_querier_unsupported_total` | counter | `lang,construct` | ⛔/⚠️ usage ([QUERY-MAPPING.md](./QUERY-MAPPING.md)) |
  | `sol_objectstore_requests_total` | counter | `op(get\|list\|put),status` | [NFR10](#nfr10) request rate |
  | `sol_objectstore_throttled_total` | counter | — | [NFR10](#nfr10) `503 SlowDown` |
  | `sol_objectstore_request_duration_seconds` | histogram | `op` | object-store latency |
  | `sol_compactor_runs_total`, `_duration_seconds` | counter/histogram | `signal,status` | [FR7](#fr7) compaction health |
  | `sol_compactor_files_input_total` / `_files_output_total` | counter | `signal` | file-count reduction (the C1 lever) |
  | `sol_compactor_rollup_rows_total` | counter | `resolution` | [FR6](#fr6) rollup output |
  | `sol_compactor_retention_deleted_total` | counter | `signal` | retention GC |
  | `sol_compactor_lag_seconds` | gauge | `signal` | sealed-boundary lag (compaction freshness) |
- **Service graph metrics**: the `servicegraph` transform already materializes `traces_service_graph_request_server_seconds` as `OtelMetric` events at ingest time. These flow through the pipeline into the histogram Parquet table. The query backend reads them as regular histogram metrics — no special handling needed.
