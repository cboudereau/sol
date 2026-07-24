# Vector SaaS / Backend — Market Study

> Study date: April 2026
> Goal: Identify features with market demand for an OTLP-native observability backend/SaaS built on Vector, competing with Grafana Cloud and Datadog.

---

## 1. Market Overview

The observability market is projected to grow at ~14% CAGR through 2031. Cloud/SaaS captured 68% of market share in 2025. Key dynamics:

- **SaaS adoption is accelerating**: 37% of orgs use "mostly" or "only" SaaS (up 42% YoY). The split model (half SaaS / half self-managed) collapsed from 22% to 6%.
- **Self-hosted remains strong in Europe/gov**: 69% of European orgs and 77% of government orgs still self-host — a segment underserved by US-centric SaaS vendors.
- **Tool consolidation is the default**: Average tech count dropped from 9 to 8 per org, but enterprises still use 16-24 data sources in Grafana. Teams want fewer tools, not more.
- **Platform switching is accelerating**: Leaders are willing to change vendors within 1-2 years.
- **Observability costs ~17% of infrastructure spend** (median 10%), and 97% of orgs have experienced cost surprises.

Sources:
- [Grafana Observability Survey 2025](https://grafana.com/observability-survey/2025/)
- [Elastic Observability Report 2026](https://www.elastic.co/resources/observability/report/landscape-observability-report)
- [Elastic Blog - 2026 Trends](https://www.elastic.co/blog/2026-observability-trends-costs-business-impact)
- [Mordor Intelligence - Market Size](https://www.mordorintelligence.com/industry-reports/observability-market)

---

## 2. Customer Pain Points (Ranked by Frequency)

| Rank | Pain Point | Data |
|------|-----------|------|
| 1 | **Cost unpredictability** | 97% experienced cost surprises; 67% say they're regular; 54% of IT leaders face pressure to justify spend |
| 2 | **Complexity / overhead** | 39% cite it as biggest obstacle — setup, maintenance, tuning |
| 3 | **Signal-to-noise / alert fatigue** | 38% — too many false positives, irrelevant alerts |
| 4 | **Cost itself** | 37% — observability is too expensive; 74% say cost is #1 selection criterion |
| 5 | **Vendor lock-in** | OpenTelemetry adoption at 71% partly driven by desire to avoid lock-in |
| 6 | **Integration complexity** | 30% face challenges integrating tools together |
| 7 | **Lack of cross-signal correlation** | Logs, metrics, traces in separate silos; manual clicking between panels |
| 8 | **Skilled workforce shortage** | Not enough OTel/observability engineers |

Sources:
- [Grafana Observability Survey 2025](https://grafana.com/observability-survey/2025/)
- [Elastic Blog - 2026 Trends](https://www.elastic.co/blog/2026-observability-trends-costs-business-impact)
- [LogicMonitor - AI Trends 2026](https://www.logicmonitor.com/blog/observability-ai-trends-2026)

---

## 3. Competitive Landscape

### 3.1 Datadog (Incumbent — $2.7B revenue 2024)

**Pricing model** (per-feature, per-host, per-volume):
| Feature | Price |
|---------|-------|
| Infrastructure monitoring | $15/host/month |
| APM | $31/host/month |
| Log ingestion | $0.10/GB |
| Log indexing | $1.70/M events |
| Custom metrics (overage) | $5.00/100 metrics/month |
| RUM | $1.50/1K sessions/month |
| Database monitoring | $70/db host/month |

**Key vulnerabilities**:
- Bills grow 30-50% YoY for most teams
- 99th percentile billing: a single autoscale spike bills for the whole month
- Custom metrics (including all OTel metrics) can be 52% of total bill
- K8s host count 3-5x expectations; misconfigured agents → 10x bill
- Teams commonly migrate logs first (highest cost, lowest switching cost)

**What it does well**: Unified UX, APM trace correlation, AI-powered anomaly detection, broad integrations.

Sources:
- [SigNoz - Datadog Pricing](https://signoz.io/blog/datadog-pricing/)
- [OpenObserve - Datadog Pricing](https://openobserve.ai/blog/datadog-pricing/)
- [Sedai - Datadog Cost Guide](https://sedai.io/blog/datadog-cost-pricing-guide)
- [Last9 - Datadog Pricing](https://last9.io/blog/datadog-pricing-all-your-questions-answered/)

### 3.2 Grafana Cloud (Open-core — Grafana Labs)

**Architecture**: Mimir (metrics) + Loki (logs) + Tempo (traces) + Grafana (UI), backed by object storage.

**Pricing**: Usage-based; enterprise tiers add HIPAA/GDPR compliance, custom SLAs.

**Key strengths**:
- Open-source core → strong community trust
- Grafana UI is the de facto standard dashboard
- PromQL / LogQL / TraceQL are established query languages
- Deep OpenTelemetry integration
- Strong European/self-hosted story

**Key vulnerabilities**:
- Three separate backends (Mimir, Loki, Tempo) = three query languages, three operational burdens
- No native cross-signal SQL query
- Cost management is a new feature area (still catching up)
- Self-hosted Mimir/Loki/Tempo is operationally complex

### 3.3 Open-Source Alternatives

| Project | Storage | Query | Strength | Weakness |
|---------|---------|-------|----------|----------|
| **SigNoz** | ClickHouse | SQL-like | Unified logs+metrics+traces, OTel-native, $0.3/GB logs | ClickHouse operational complexity |
| **OpenObserve** | Object storage (S3) | SQL | 140x lower storage than ES, Rust-based | No APM, traces still maturing |
| **Parseable** | Parquet on S3 | SQL (DataFusion) | Native Parquet, very cheap | Young project, limited features |
| **Quickwit** | Object storage | Tantivy (full-text) | Sub-second search on S3 | Logs-only, no metrics/traces |
| **VictoriaMetrics** | Custom TSDB | MetricsQL | Excellent for metrics specifically | Logs/traces are add-ons |

Sources:
- [ClickHouse - OTel Platforms](https://clickhouse.com/resources/engineering/top-opentelemetry-compatible-platforms)
- [OpenObserve vs SigNoz](https://openalternative.co/compare/openobserve/vs/signoz)
- [Parseable vs Axiom](https://www.parseable.com/blog/axiom-vs-parseable)

---

## 4. Features the Market Demands (Prioritized)

### Tier 1 — Table stakes (must have to enter the market)

| Feature | Why | Who does it |
|---------|-----|------------|
| **OTLP ingestion** (logs, metrics, traces) | 71% use OTel; it's the standard | Everyone |
| **Grafana-compatible query APIs** (PromQL, LogQL, TraceQL) | Grafana is the UI standard; nobody wants to build a new dashboard | Grafana Cloud, Mimir, Loki, Tempo |
| **Usage-based pricing** (per GB, not per host) | #1 selection criterion is cost; per-host billing is hated | SigNoz, OpenObserve |
| **Alerting** | Basic threshold + anomaly alerts | Everyone |
| **Retention policies** | Hot/warm/cold tiering | Everyone |

### Tier 2 — Differentiators (competitive advantage)

| Feature | Market Signal | Who does it | Your opportunity |
|---------|--------------|------------|-----------------|
| **SQL across all signals** | "SQL is the universal language of data analysis" — ClickHouse blog; 4 of 5 OSS competitors chose SQL | SigNoz (partial), Parseable (logs only) | **Nobody does unified SQL across logs+metrics+traces with JOINs** |
| **Cost predictability / controls** | 97% hit cost surprises; 74% rank cost #1 | Datadog (poorly), Grafana (new features) | **Real-time cost dashboard, hard budget caps, automatic sampling when budget hit** |
| **Cross-signal correlation** | Top pain point — data siloed across tools | Datadog (proprietary), nobody open-source | **SQL JOINs on trace_id/service_name across signals is the killer feature** |
| **AI/ML anomaly detection** | 31% want training-based alerts; 28% want faster root cause analysis | Datadog, Elastic, New Relic | Emerging — can differentiate with open models |
| **Data residency / GDPR compliance** | 69% EU orgs self-host; GDPR enforcement intensifying | Grafana Enterprise | **EU-hosted SaaS or managed self-hosted offering** |

### Tier 3 — Emerging / unique value (blue ocean)

| Feature | Market Signal | Competition | Your angle |
|---------|--------------|------------|-----------|
| **Observability Lakehouse** (Parquet on S3 + SQL) | Converging trend: Tempo already uses Parquet; InfluxDB 3.0 bet on DataFusion; Amazon launched S3 Tables for CloudWatch | Nobody offers it as managed SaaS | **OTLP → Parquet → Iceberg on S3, queryable via DataFusion/SQL** |
| **Portable data** (open formats, zero lock-in) | OTel adoption driven by anti-lock-in sentiment | Marketing claim by many, reality by few | **Parquet + Iceberg = customer owns their data on their S3** |
| **BI/analytics bridge** | "Observability team and data engineering team don't eat lunch together" | Nobody | **Same Parquet files queryable by dbt, Jupyter, BI tools** |
| **Pipeline-as-a-feature** | Vector's transform layer is unique — remap, filter, sample, route before storage | Nobody bundles pipeline + backend | **Reduce before you store = cost control built in** |
| **Compliance audit trails** | 45% use observability for compliance monitoring; 40% for audit trail generation | Enterprise features of DD/Grafana | Emerging need |

Sources:
- [Grafana Observability Survey 2025](https://grafana.com/observability-survey/2025/)
- [ClickHouse - Lakehouses for Observability](https://clickhouse.com/blog/lakehouses-path-to-low-cost-scalable-no-lockin-observability)
- [Clay Smith - OTel Lakehouses](https://clay.fyi/blog/cheap-opentelemetry-lakehouses-parquet-duckdb-iceberg/)
- [OneUptime - OTel Backend Cost](https://oneuptime.com/blog/post/2026-02-06-compare-opentelemetry-backend-cost-performance/view)

---

## 5. The Observability Lakehouse Opportunity (Deep Dive)

### 5.1 What it is

Convert OTLP telemetry (proto) to Parquet columnar format, store on object storage (S3/R2/MinIO), catalog with Apache Iceberg, query with SQL (DataFusion or DuckDB).

### 5.2 Why the market is ready

- **Tempo already stores traces as Parquet** — Grafana validated the format for observability
- **InfluxDB 3.0 rebuilt on DataFusion + Parquet** — validated the query engine choice
- **Amazon launched S3 Tables** (Dec 2025) with CloudWatch Logs integration — AWS sees this as the future
- **Cloudflare R2 Data Catalog** offers zero-egress Iceberg catalogs — infrastructure is ready
- **Compression**: Parquet achieves up to 90% size reduction on telemetry data vs raw formats
- **Cost**: Object storage is 10-100x cheaper than dedicated database storage

### 5.3 Technical trade-offs (honest assessment)

| Strength | Limitation |
|----------|-----------|
| Excellent for analytical scans (aggregate, GROUP BY) | Point lookups are slow (trace_id search needs bloom filters) |
| Infinite retention at commodity prices | Query latency is seconds, not milliseconds |
| Schema evolution via Iceberg | High-volume ingestion needs careful partitioning |
| Any SQL tool can query the data | Object storage is "chatty" — many HTTP round-trips per query |
| Rust ecosystem is mature (arrow-rs, datafusion, parquet) | Streaming telemetry = "million tiny writers" problem for Iceberg |

### 5.4 Architecture: Hot + Cold tiers

The ClickHouse analysis confirms: **neither hot-only nor cold-only works alone**.

```
Real-time queries (last 15-60 min)
  → Hot tier: in-memory ring buffer or WAL
  → Sub-second latency, limited retention

Analytical queries (hours to months)
  → Cold tier: Parquet on S3, Iceberg catalog
  → Seconds latency, unlimited retention, SQL

Vector pipeline sits in front:
  → OTLP in → transform (sample, filter, enrich) → write to both tiers
  → The pipeline IS the cost control layer
```

### 5.5 Query engine choice: DataFusion vs DuckDB

| Criteria | DataFusion | DuckDB |
|----------|-----------|--------|
| Language | Rust (native) | C++ (FFI from Rust) |
| Async | Yes | No |
| Concurrency | Multi-query | Single-process |
| Parquet/S3 | Native | Native |
| Used by | InfluxDB 3.0, Parseable, Comet | Analytics tooling, local dev |
| Extensibility | Custom table providers, UDFs | Extensions |
| Maturity for serving | Production (InfluxDB) | Not designed for multi-tenant serving |

**Recommendation**: DataFusion for the backend service, DuckDB as optional client-side/CLI tool.

Sources:
- [ClickHouse - Lakehouses for Observability](https://clickhouse.com/blog/lakehouses-path-to-low-cost-scalable-no-lockin-observability)
- [Clay Smith - OTel Lakehouses](https://clay.fyi/blog/cheap-opentelemetry-lakehouses-parquet-duckdb-iceberg/)
- [Bauplan - DuckDB to DataFusion migration](https://www.bauplanlabs.com/post/duck-hunt-moving-bauplan-from-duckdb-to-datafusion)
- [Parseable vs Axiom](https://www.parseable.com/blog/axiom-vs-parseable)

---

## 6. Grafana Compatibility — API Surface Required

To use Grafana as the frontend (avoiding building a UI), implement these backend APIs:

### 6.1 Prometheus-compatible (metrics)

| Endpoint | Purpose |
|----------|---------|
| `POST /api/v1/write` | Remote write ingestion (you already have this as a source) |
| `GET/POST /api/v1/query` | Instant PromQL query |
| `GET/POST /api/v1/query_range` | Range PromQL query |
| `GET /api/v1/labels` | Label discovery |
| `GET /api/v1/label/{name}/values` | Label value discovery |
| `POST /api/v1/read` | Remote read (for Grafana data source) |

### 6.2 Loki-compatible (logs)

| Endpoint | Purpose |
|----------|---------|
| `POST /loki/api/v1/push` | Log ingestion |
| `GET /loki/api/v1/query` | Instant LogQL query |
| `GET /loki/api/v1/query_range` | Range LogQL query |
| `GET /loki/api/v1/labels` | Label discovery |
| `GET /loki/api/v1/label/{name}/values` | Label value discovery |
| `GET /loki/api/v1/tail` | Live tailing (WebSocket) |

### 6.3 Tempo-compatible (traces)

| Endpoint | Purpose |
|----------|---------|
| `POST /v1/traces` | OTLP trace ingestion |
| `GET /api/traces/{traceID}` | Trace by ID lookup |
| `GET /api/search` | TraceQL search |
| `GET /api/search/tags` | Tag discovery |
| `GET /api/search/tag/{name}/values` | Tag value discovery |

### 6.4 Alternative: Grafana Plugin

Instead of implementing full PromQL/LogQL/TraceQL parsers, you could build a **Grafana backend data source plugin** that:
- Translates Grafana queries into SQL
- Runs SQL against your DataFusion backend
- Returns results in Grafana's data frame format

This is significantly less engineering than full PromQL/LogQL compatibility.

Sources:
- [Grafana - Tempo data source](https://grafana.com/docs/grafana/latest/datasources/tempo/configure-tempo-data-source/)
- [Grafana - Data source HTTP API](https://grafana.com/docs/grafana/latest/developer-resources/api-reference/http-api/data_source/)
- [Grafana - Plugin backend guide](https://grafana.com/developers/plugin-tools/how-to-guides/data-source-plugins/convert-a-frontend-datasource-to-backend)

---

## 7. Unique Value Propositions for Your SaaS

Based on market gaps and your existing assets (Vector pipeline + OTLP-native events):

### 7.1 "Pipeline-Included Observability" (nobody else has this)

Every competitor separates collection (OTel Collector) from storage/query. You can offer:
- **Transform before you store**: VRL remap, sampling, filtering, enrichment built into the backend
- **Cost control at ingestion**: Drop noisy logs, downsample metrics, sample traces — before they cost money
- **Routing**: Send critical data to hot tier, everything else to cold lakehouse
- **This is Vector's moat** — no competitor has a programmable pipeline built into the backend

### 7.2 "SQL Observability" (emerging, no mature SaaS offering)

- Unified SQL across logs, metrics, traces — not 3 query languages
- JOIN across signals: correlate traces with their logs and the metrics at that time
- Standard SQL = works with dbt, Jupyter, BI tools, data teams already know it
- The data engineering / observability convergence is an untapped market

**Example killer query** (impossible in Grafana Cloud or Datadog):
```sql
SELECT
  t.service_name, t.span_name,
  AVG(t.duration_ms) as p50_latency,
  COUNT(l.severity) FILTER (WHERE l.severity = 'ERROR') as errors,
  AVG(m.value) FILTER (WHERE m.metric_name = 'process.cpu.utilization') as cpu
FROM traces t
LEFT JOIN logs l ON t.trace_id = l.trace_id
LEFT JOIN metrics m ON m.service_name = t.service_name
  AND m.timestamp BETWEEN t.start_time AND t.end_time
WHERE t.timestamp > now() - INTERVAL '1 hour'
GROUP BY 1, 2
ORDER BY errors DESC
```

### 7.3 "Own Your Data" (anti-lock-in positioning)

- Data stored as Parquet + Iceberg on **customer's own S3 bucket**
- Customer can query their data with any tool (DuckDB CLI, Spark, Athena, Trino)
- OTLP in, Parquet out — open standards end to end
- Cancelling the SaaS doesn't lose your data — the opposite of Datadog
- Strong GDPR story: data never leaves customer's region

### 7.4 "Cost-Aware Observability" (addresses #1 pain point)

- Real-time cost dashboard: see $/GB by service, by signal, by team
- Hard budget caps: automatically trigger sampling/dropping when budget threshold hit
- Cost attribution: chargeback per team/service based on actual ingestion
- Pipeline-level optimization: automatic cardinality reduction, deduplication
- Predictable pricing: flat per-GB, no per-host, no 99th-percentile tricks

### 7.5 "Compliance-Ready" (underserved segment)

- EU-hosted deployment option (69% of EU orgs self-host for compliance)
- PII detection + redaction in the pipeline (VRL transforms) before storage
- Immutable audit trails on object storage
- Tenant isolation with customer-managed encryption keys
- GDPR right-to-erasure: Iceberg partition-level deletes

---

## 8. Pricing Strategy

Based on competitive analysis, the winning model is **usage-based, per-GB, all-inclusive**:

| Competitor | Model | Customer Complaint |
|-----------|-------|-------------------|
| Datadog | Per-host + per-GB + per-metric + per-feature | Unpredictable, 3-5x overruns |
| Grafana Cloud | Per-metric-series + per-GB logs + per-trace-span | Complex, many dimensions |
| SigNoz | $0.3/GB logs, $0.3/GB traces, $0.1/M metric samples | Simple but still 3 prices |
| **Your opportunity** | **Single $/GB across all signals** | Simplest possible model |

### Suggested tiers

| Tier | Target | Features |
|------|--------|----------|
| **Free / Dev** | Developers, POCs | 5 GB/day, 3-day retention, community support |
| **Team** ($X/GB) | Small teams | Unlimited retention, alerting, Grafana plugin, SQL queries |
| **Business** ($X/GB, volume discounts) | Mid-market | Pipeline transforms, cost controls, SSO, SLA |
| **Enterprise** (custom) | Large orgs | Dedicated deployment, BYOS3, compliance, unlimited users |

The "Bring Your Own S3" (BYOS3) model for enterprise is especially compelling — customers pay only for compute/query, storage is on their bill at S3 rates.

---

## 9. Go-to-Market Recommendations

### 9.1 Start with logs (lowest switching cost, highest pain)

- Logs are the most expensive signal (highest volume, most Datadog cost complaints)
- Logs are the first thing teams migrate away from Datadog
- Log analytics with SQL is immediately valuable and differentiated
- Loki API compatibility means Grafana works out of the box

### 9.2 Land with cost savings, expand with features

- **Land**: "Same logs, 10x cheaper, you keep your data"
- **Expand**: Add traces (Tempo API), then metrics (Prometheus API)
- **Differentiate**: SQL analytics, cross-signal JOINs, pipeline transforms

### 9.3 Target segments

| Segment | Why | Message |
|---------|-----|---------|
| **Datadog cost refugees** | Bills growing 30-50% YoY, actively looking | "Cut your observability bill by 80%" |
| **EU/regulated companies** | 69% self-host for compliance, want managed option | "EU-hosted, GDPR-native, your S3" |
| **Platform/DevOps teams** | Want pipeline control, cost attribution | "Observe + transform + store in one tool" |
| **Data-savvy orgs** | Want to query telemetry with SQL/BI tools | "Your observability data is just a SQL table" |

---

## 10. Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Grafana adds SQL / lakehouse features | High | Move fast; your pipeline integration is hard to replicate |
| ClickHouse-based competitors (SigNoz) mature faster | Medium | Parquet/S3 is cheaper at scale than ClickHouse |
| Building PromQL/LogQL parsers is months of work | High | Start with Grafana plugin (SQL backend), add native query languages later |
| DuckDB/DataFusion not fast enough for dashboards | Medium | Hot tier for recent data; cold tier for analytics |
| Object storage latency for interactive use | Medium | Caching layer, pre-computed aggregations |
| Single-person/small team building a backend | High | Focus on logs-only MVP, leverage existing OSS (DataFusion, arrow-rs, iceberg-rust) |

---

## 11. Summary: Feature Priority Matrix

| Priority | Feature | Market Demand | Competition Gap | Your Asset |
|----------|---------|--------------|----------------|-----------|
| **P0** | OTLP → Parquet sink | High (lakehouse trend) | No turnkey SaaS | Arrow codec exists |
| **P0** | SQL query API (DataFusion) | High (SQL is universal) | No unified cross-signal SQL SaaS | Rust ecosystem ready |
| **P0** | Loki-compatible API | High (Grafana is default UI) | Only Loki itself | Loki sink exists (reverse it) |
| **P1** | Cost dashboard + budget caps | Very high (97% hit surprises) | Grafana just starting, DD poor | Pipeline = natural cost control point |
| **P1** | Pipeline transforms in backend | Medium-high (unique value) | Nobody else has this | Vector IS this |
| **P1** | BYOS3 / data portability | High (anti-lock-in sentiment) | Marketing claims, few deliver | Parquet + Iceberg = real portability |
| **P2** | Tempo-compatible API | Medium | Only Tempo itself | OTLP traces exist |
| **P2** | Prometheus-compatible API | Medium (complex to implement) | Mimir, VictoriaMetrics | Prometheus source exists |
| **P2** | AI anomaly detection | High (31% want it) | DD leads | Can build on open models later |
| **P3** | Full PromQL engine | Medium | Mature implementations exist | Consider embedding Mimir's engine |
| **P3** | Grafana backend plugin | Medium (alternative to full API compat) | Custom development | Reduces need for PromQL/LogQL parsers |

---

## 12. Architecture Decision: "One Ring" (Integrated) vs Dedicated Backend

### 12.1 The question

Should the backend (storage + query) be:
- **A)** Integrated into Vector — one binary does Agent + Gateway + Backend
- **B)** A separate dedicated application — Vector pipeline feeds a standalone query service

### 12.2 How the OTel ecosystem works today

```
OTel defines two deployment patterns:

  Agent (sidecar/daemonset)  →  Gateway (central collector)  →  Backend (???)
       ↑ defined by OTel            ↑ defined by OTel            ↑ NOT defined by OTel
```

The **Backend role does NOT exist** as an OTel standard. Each vendor fills it:
- Datadog: proprietary backend
- Grafana: Mimir + Loki + Tempo (3 separate backends!)
- SigNoz: OTel Collector → ClickHouse
- Parseable: any collector → Parseable API

This is the gap. OTel standardized collection but NOT storage/query.

### 12.3 Option A — "One Ring": Vector does everything

**The idea**: One binary with deployment modes:

```
┌──────────────────────────────────────────────┐
│                 Vector Binary                 │
│                                              │
│  ┌──────────┐  ┌──────────┐  ┌────────────┐ │
│  │  Agent   │  │ Gateway  │  │  Backend   │ │
│  │  mode    │→ │  mode    │→ │  mode      │ │
│  │          │  │          │  │            │ │
│  │ collect  │  │ transform│  │ store      │ │
│  │ forward  │  │ route    │  │ query      │ │
│  │          │  │ sample   │  │ serve API  │ │
│  └──────────┘  └──────────┘  └────────────┘ │
│                                              │
│  Mode: "all-in-one" = all three enabled      │
│  Mode: "agent"      = collect + forward only │
│  Mode: "backend"    = receive + store + query│
└──────────────────────────────────────────────┘
```

**Market advantages**:

| Advantage | Why it matters |
|-----------|---------------|
| **Simplicity** | Complexity is #1 pain point (39%). One binary to deploy, configure, monitor. No coordination between services. |
| **Unique positioning** | Nobody offers Agent+Gateway+Backend in one tool. This is genuinely novel. |
| **Pipeline-as-backend-feature** | VRL transforms between ingestion and storage is your moat — impossible to replicate without owning the pipeline. |
| **Cost** | One process = less infrastructure, fewer ops, lower SaaS hosting costs. |
| **Consistency** | Same event model (OtelLog/OtelMetric/OtelSpan) end-to-end, no ser/deser between components. |
| **Dev/small team story** | `vector --mode all-in-one` replaces: OTel Collector + Loki + Tempo + Mimir. Compelling for startups and small ops teams. |

**Technical risks**:

| Risk | Reality | Mitigation |
|------|---------|-----------|
| **Scaling mismatch** (pipeline scales by throughput, query scales by concurrency) | Real, but only matters at scale (>100 GB/day). Small/medium teams won't hit this. | Deploy separate instances with different `--mode` flags at scale. Same binary, different roles. |
| **Memory contention** (query engine + pipeline compete for RAM) | Real. DataFusion can be memory-hungry on large analytical queries. | Memory budgets per subsystem; query engine gets a capped allocation. |
| **Binary size bloat** | Adding DataFusion + Parquet + Iceberg increases binary. | Cargo feature flags: `features = ["backend"]` compiles in storage+query only when needed. Agent mode stays lean. |
| **Maintenance complexity** | More code in one repo. | Already how Vector works — 100+ sources/sinks behind feature flags. Backend is just another feature. |

**What Vector already has for this**:
- Single tokio multi-threaded runtime (`src/app.rs`) — backend tasks run on same scheduler
- API server (Warp/Hyper at `src/api/server.rs`) — can add query endpoints alongside GraphQL
- Feature-gated components — `#[cfg(feature = "sources-opentelemetry")]` pattern extends to `#[cfg(feature = "backend")]`
- Prometheus exporter sink already runs an HTTP server inside a sink — proves the pattern works

### 12.4 Option B — Dedicated backend app

**The idea**: Vector remains the pipeline. A new Rust binary handles storage + query.

```
┌────────────┐    OTLP     ┌──────────────────┐
│   Vector   │ ─────────→  │  vector-backend   │
│  (pipeline)│             │  (new binary)     │
│            │             │                   │
│  collect   │             │  receive OTLP     │
│  transform │             │  store Parquet/S3 │
│  forward   │             │  serve query API  │
└────────────┘             └──────────────────┘
                                   ↑
                           ┌───────┘
                           │ Also accepts from:
                           │ - OTel Collector
                           │ - Fluentd
                           │ - Any OTLP sender
                           └─────────────────
```

**Market advantages**:

| Advantage | Why it matters |
|-----------|---------------|
| **Independent scaling** | Query and pipeline scale separately — important for large deployments |
| **Collector-agnostic** | Customers with existing OTel Collector don't need to switch to Vector |
| **Cleaner SaaS model** | You host the backend; customer runs whatever collector they want |
| **Simpler mental model** | "Vector = pipeline" and "vector-backend = storage" — clear separation |

**Market disadvantages**:

| Disadvantage | Impact |
|-------------|--------|
| **Two binaries to deploy** | Adds complexity — contradicts #1 market demand (simplicity) |
| **Loses the "one tool" story** | Marketing is weaker. "Use two tools" is what everyone else says. |
| **Pipeline not integrated** | Transforms happen before data reaches backend — backend can't transform on query. |
| **Duplicated OTLP handling** | Both Vector and backend need OTLP receiver — code duplication or shared crate. |

### 12.5 Option C — Hybrid: One codebase, flexible deployment

**Recommended approach**: Build backend capabilities INTO Vector (Option A architecture), but design so they can be deployed flexibly:

```
Small team (all-in-one):
  vector --mode all
  → one process: collect + transform + store + query

Medium team (split roles):
  vector --mode agent     (on each host)
  vector --mode gateway   (central, optional)
  vector --mode backend   (storage + query)
  → same binary everywhere, different config

Large team / SaaS:
  vector --mode backend   (hosted by you)
  customer runs their own OTel Collector or Vector agent
  → your SaaS is just Vector in backend mode

Enterprise:
  vector --mode backend --storage customer-s3://bucket
  → BYOS3, your compute, their storage
```

**Why this wins**:

1. **One binary, many deployment patterns** — like how Prometheus can be run standalone or with Thanos, but simpler
2. **The "one tool" marketing story is real** — small teams get everything in one process
3. **Scales up naturally** — large teams just run more instances with different modes
4. **SaaS-ready** — you host Vector in backend mode, customers don't need to know it's Vector
5. **OTel-compatible, not OTel-breaking** — backend mode accepts OTLP from ANY collector, not just Vector
6. **Feature flags control binary size** — agent builds stay lean

**Precedent in the market**:
- **VictoriaMetrics**: single binary, multiple modes (standalone vs cluster)
- **Grafana Mimir**: single binary with `-target` flag (all, ingester, querier, compactor)
- **Tempo**: single binary, `-target all` or split into microservices
- These "monolithic-or-microservice" designs are proven and popular

### 12.6 OTel compatibility analysis

The "one ring" approach does NOT break OTel compatibility:

```
Standard OTel flow:
  App (OTel SDK) → OTel Collector (Agent) → OTel Collector (Gateway) → Backend

With Vector "one ring":
  App (OTel SDK) → Vector (Agent mode, OTLP receiver) → Vector (Backend mode, OTLP receiver + storage + query)

Mixed (customer has existing OTel Collector):
  App (OTel SDK) → OTel Collector → Vector (Backend mode, OTLP receiver + storage + query)
```

All communication uses standard OTLP protocol. Vector's backend mode is just another OTLP endpoint — indistinguishable from Tempo, Loki, or any other backend from the collector's perspective.

**What DOESN'T exist in OTel and you're creating**:
- OTel defines receivers, processors, exporters — but NOT storage or query
- By adding storage + query to a tool that already does receive + process + export, you complete the full stack
- This is an EXTENSION of OTel, not a violation of it

### 12.7 Architecture decision summary

| Criteria | Integrated (A) | Dedicated (B) | Hybrid (C) |
|----------|----------------|---------------|------------|
| Simplicity for small teams | Best | Poor (2 binaries) | Best |
| Scalability for large teams | Limited | Best | Good (split modes) |
| Marketing story | "One tool" | Weaker | "One tool, scales up" |
| OTel compatibility | Full | Full | Full |
| SaaS model | Host Vector | Host separate app | Host Vector in backend mode |
| Engineering effort | Medium | High (new binary, shared crates) | Medium (feature flags) |
| Pipeline integration | Deep (same process) | Shallow (OTLP between) | Deep (same process) |
| Binary size (agent mode) | Risk of bloat | Clean separation | Controlled via features |
| Maintenance | One codebase | Two codebases or monorepo | One codebase |

**Recommendation: Option C (Hybrid)** — Build backend features into Vector behind feature flags, deploy with `--mode` flags. One codebase, one binary, flexible deployment. This gives you the "one ring" story for marketing while preserving the ability to scale.

Sources:
- [OTel Collector Architecture](https://opentelemetry.io/docs/collector/architecture/)
- [OTel Gateway Pattern](https://opentelemetry.io/docs/collector/deployment/gateway/)
- [OTel Agent Pattern](https://opentelemetry.io/docs/collector/deployment/agent/)
- [SigNoz Architecture](https://signoz.io/docs/architecture/)
- [VictoriaMetrics Architecture](https://victoriametrics.com/blog/announcing-1b-downloads-and-product-development-with-logs-traces-metrics/index.html)

---

## 13. Legal Analysis: License, Fork, and Commercial Safety

### 13.1 Vector's license: MPL-2.0

Vector is licensed under **Mozilla Public License 2.0** (MPL-2.0). This is a **weak copyleft, file-level** open-source license. It is NOT AGPL, NOT SSPL, NOT BSL — it is a true OSI-approved open-source license.

Vector switched to MPL-2.0 in August 2020. The Team stated: *"It is not our intent to restrict Vector usage or distribution in any way, now or in the future."*

### 13.2 What MPL-2.0 allows you to do

| Action | Allowed? | Obligation |
|--------|----------|-----------|
| **Fork Vector** | Yes | Keep MPL-2.0 on original/modified MPL files |
| **Modify Vector source files** | Yes | Modified files must remain MPL-2.0 when distributed |
| **Add new files (your backend code)** | Yes | **New files can be ANY license** (proprietary, MIT, AGPL, etc.) |
| **Build a commercial product** | Yes | No restriction on commercial use |
| **Run as SaaS** | Yes | Server-side code is NOT "distributed" — **minimal obligations** |
| **Charge money** | Yes | No restriction |
| **Use in proprietary product** | Yes | Only MPL files must remain open; new files are yours |

**Key MPL-2.0 principle** (from Mozilla FAQ): *"The code which runs on the server is not 'distributed' to the user."* This means a SaaS built on MPL-2.0 code has even fewer obligations than distributing a binary.

### 13.3 The CLA question — critical for your fork strategy

Datadog requires contributors to sign a **Contributor License Agreement (CLA)** to contribute to upstream `vectordotdev/vector`. The CLA grants Datadog:

> *"A perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable license to use, reproduce, prepare derivative works of, sublicense, and distribute the Contributions."*

**What this means for YOUR fork:**

| Scenario | CLA applies? | Explanation |
|----------|-------------|-------------|
| You contribute code TO upstream Vector | **Yes** | Signing the CLA gives Datadog sublicense rights to YOUR code |
| You fork Vector and modify it yourself | **No** | The CLA is a contract between contributor and Datadog. If you never contribute upstream, you never sign it. |
| You pull upstream changes INTO your fork | **No** | Upstream code is already MPL-2.0 licensed to you. No CLA needed to receive. |
| Datadog uses YOUR fork's code | **Only if MPL-2.0** | They can use your modifications to MPL files (that's how MPL works — reciprocal). But your NEW files under a different license are YOURS. |

### 13.4 Your safe fork strategy

**DO:**
1. Maintain your fork at `cboudereau/vector` (you already have this)
2. Pull upstream MPL-2.0 changes when useful (bug fixes, new sources/sinks)
3. Add your backend features in **new files** — these can be under any license you choose
4. Keep MPL-2.0 notices on all original and modified Vector files
5. **Rename your product** — do NOT use "Vector" as your product name (trademark belongs to Datadog/Timber)
6. Run as SaaS — server-side code is not "distributed" under MPL-2.0

**DO NOT:**
1. Sign Datadog's CLA (unless you want to contribute upstream)
2. Contribute your backend code to upstream Vector (that would give Datadog sublicense rights via CLA)
3. Use "Vector" or "Datadog" trademarks in your product name/marketing
4. Remove or alter MPL-2.0 license notices from original files

### 13.5 Can Datadog take your work?

| Your code | Can Datadog use it? | Why |
|-----------|-------------------|-----|
| Modifications to existing MPL-2.0 files | **Yes** (if you distribute them) | MPL-2.0 is copyleft at the file level — modified files must be shared under MPL-2.0, and anyone (including Datadog) can use MPL-2.0 code |
| New files you create (backend, query engine, etc.) | **No** (if under a different license) | New files in separate files are NOT covered by MPL-2.0. You control the license. |
| Your SaaS service (not distributed) | **No** | Code running on your servers is never "distributed" |

**Bottom line**: Put your proprietary/unique features in **new files** under your own license. Datadog can only use your modifications to THEIR existing files (which is fair — you can use theirs too). They CANNOT take your new backend code if it's in separate files under a non-MPL license.

### 13.6 Recommended license strategy for your product

```
your-product/
├── vector/                    # Forked Vector code
│   ├── src/                   # Modified Vector sources → MPL-2.0 (required)
│   ├── LICENSE                # MPL-2.0 (keep as-is)
│   └── ...
├── backend/                   # YOUR new backend code
│   ├── src/
│   │   ├── storage/           # Parquet writer, S3 integration
│   │   ├── query/             # DataFusion query engine, SQL API
│   │   ├── api/               # Loki/Tempo/Prometheus-compatible APIs
│   │   └── ...
│   └── LICENSE                # YOUR choice: AGPL-3.0, BSL, proprietary, etc.
└── shared/                    # Shared types/proto (if needed)
    └── LICENSE                # Your choice (or MPL-2.0 if derived from Vector types)
```

**Option A — AGPL-3.0 for backend**: Allows open-source community but prevents competitors from offering your backend as SaaS without sharing their changes. (This is what Grafana uses for Mimir, Loki, Tempo.)

**Option B — BSL (Business Source License) for backend**: Source-available but not open-source. Prevents any commercial use for N years, then converts to open-source. (This is what HashiCorp, Sentry, CockroachDB use.)

**Option C — Proprietary for backend**: Maximum protection. Only you can use it. SaaS-only delivery, no source code shared.

**Recommendation**: AGPL-3.0 for the backend. It's what Grafana Labs uses for their entire stack (Mimir, Loki, Tempo, Grafana itself). It builds community trust, allows self-hosting, but prevents competitors from offering your code as a managed service without contributing back. The Grafana model has proven this works commercially.

### 13.7 Comparison with competitor license strategies

| Product | Pipeline license | Backend license | Strategy |
|---------|-----------------|----------------|----------|
| **Grafana Stack** | Apache-2.0 (OTel Collector) | AGPL-3.0 (Mimir, Loki, Tempo, Grafana) | Open core: pipeline is permissive, backend is copyleft |
| **SigNoz** | Apache-2.0 (OTel Collector) | MIT + AGPL-3.0 (premium features) | Open core with enterprise add-ons |
| **OpenObserve** | — | AGPL-3.0 | Copyleft for community, enterprise license for commercial |
| **Parseable** | — | AGPL-3.0 | Same as OpenObserve |
| **Datadog** | MPL-2.0 (Vector) | Proprietary (Datadog backend) | Open pipeline, closed backend |
| **Your product** | MPL-2.0 (Vector fork) | AGPL-3.0 or BSL (recommended) | Same pattern as Grafana — proven to work |

### 13.8 Summary: you are safe

1. **MPL-2.0 is a genuine open-source license** — forking and commercial use are explicitly allowed
2. **The CLA only applies if you contribute upstream** — don't contribute, don't sign, no risk
3. **New files are yours** — put backend code in separate files under your own license
4. **SaaS has minimal obligations** — server code is not "distributed"
5. **Rename your product** — avoid trademark issues with "Vector"
6. **Datadog cannot take your new code** — only your modifications to their MPL files, which is reciprocal (you can use theirs too)

**IMPORTANT DISCLAIMER**: This analysis is based on reading the license text and Mozilla FAQ. It is NOT legal advice. Before launching a commercial product, consult an intellectual property attorney familiar with open-source licensing. The cost of a legal review (~$2-5K) is trivial compared to the risk of getting this wrong.

Sources:
- [Vector MPL-2.0 License](https://github.com/vectordotdev/vector/blob/master/LICENSE)
- [Vector license switch announcement](https://vector.dev/highlights/2020-08-31-mpl-2-0-license/)
- [Mozilla MPL 2.0 FAQ](https://www.mozilla.org/en-US/MPL/2.0/FAQ/)
- [MPL 2.0 explained - TLDRLegal](https://www.tldrlegal.com/license/mozilla-public-license-2-0-mpl-2)
- [MPL 2.0 explained - FOSSA](https://fossa.com/blog/open-source-software-licenses-101-mozilla-public-license-2-0/)
- [Datadog CLA text](https://gist.github.com/bits-bot/55bdc97a4fdad52d97feb4d6c3d1d618)
- [Vector CONTRIBUTING.md](https://github.com/vectordotdev/vector/blob/master/CONTRIBUTING.md)

---

## 14. Resource Control Analysis: Rate Limiting, Backpressure & Memory for --mode all

### 14.1 What Vector already has

Vector has a sophisticated multi-layered resource control system designed for reliable pipeline operation:

#### Event-level rate limiting

| Mechanism | Location | How it works |
|-----------|----------|-------------|
| **Throttle transform** | `src/transforms/throttle/` | Token-bucket per-key rate limiting (e.g., max 1000 events/sec per service). Uses `governor` crate with `DashMapStateStore`. |
| **Sample transform** | `src/transforms/sample/` | Statistical event dropping — 1-in-N or percentage-based. Supports per-group sampling and exclusion patterns. |

#### Backpressure (buffer-based)

| Mechanism | Location | How it works |
|-----------|----------|-------------|
| **Buffer `when_full` policy** | `lib/vector-buffers/src/config.rs` | Three modes: `Block` (default — propagate backpressure upstream), `DropNewest`, `Overflow` (to next buffer stage) |
| **Memory buffers** | `lib/vector-buffers/src/` | Capped by `max_events` (default: 500) or `max_bytes` |
| **Disk buffers** | `lib/vector-buffers/src/` | Durable persistence, minimum 268MB, synced every 500ms |
| **Chained buffers** | `lib/vector-buffers/src/` | Memory → disk overflow for tiered buffering |
| **Fanout backpressure** | `src/topology/` | Slowest sink determines source throughput (validated by `src/topology/test/backpressure.rs`) |

#### Sink concurrency control

| Mechanism | Location | How it works |
|-----------|----------|-------------|
| **Adaptive Request Concurrency (ARC)** | `src/sinks/util/adaptive_concurrency/` | EWMA of response times. Auto scale-down on latency increase (decrease_ratio: 0.9). Max concurrency cap: 200. |
| **Fixed concurrency** | `src/sinks/util/service/concurrency.rs` | `Concurrency::Fixed(N)` — hard limit per sink |
| **Request rate limit** | `src/sinks/util/service.rs` | `rate_limit_num` / `rate_limit_duration_secs` per sink (Tower RateLimit layer) |
| **Request timeout** | `src/sinks/util/service.rs` | Default 60s per request |
| **Fibonacci retry with jitter** | `src/sinks/util/retries.rs` | Exponential backoff with `JitterMode::Full` (anti-thundering herd) |

#### Source-side limits

| Mechanism | Location | How it works |
|-----------|----------|-------------|
| **TCP request limiter** | `src/sources/util/net/tcp/request_limiter.rs` | Caps in-flight events at 100,000 per TCP source. Dynamic permit adjustment via EWMA. |

#### Topology-level sizing

| Constant | Value | Location |
|----------|-------|----------|
| `SOURCE_SENDER_BUFFER_SIZE` | `TRANSFORM_CONCURRENCY_LIMIT * CHUNK_SIZE` | `src/topology/` |
| `TRANSFORM_CONCURRENCY_LIMIT` | Number of worker threads | `src/topology/` |
| `CHUNK_SIZE` | 1000 events | `vector_core` |
| `TOPOLOGY_BUFFER_SIZE` | 100 items | `src/topology/` |

### 14.2 What's MISSING for --mode all (pipeline + backend in one process)

| Gap | Risk | Severity |
|-----|------|----------|
| **No global memory budget** | DataFusion query engine + pipeline buffers + hot tier all compete for RAM. A large analytical query could OOM the process. | **High** |
| **No per-subsystem CPU isolation** | A heavy SQL query (full table scan on months of Parquet) could starve the ingestion pipeline, causing data loss. | **High** |
| **No query-level resource limits** | No max rows scanned, no per-query memory cap, no query timeout at the backend level. | **High** |
| **No ingestion-vs-query priority** | When resources are tight, which subsystem wins? Currently undefined. | **Medium** |
| **No cost/budget-based throttling** | No automatic sampling when storage budget is hit. | **Low** (nice-to-have) |

### 14.3 Solution: Dual-runtime architecture

The key architectural decision for `--mode all` is to run **two separate tokio runtimes** in one process:

```
┌──────────────────────────────────────────────────┐
│              Vector --mode all                    │
│                                                  │
│  ┌─────────────────────┐  ┌────────────────────┐ │
│  │  Pipeline Runtime    │  │  Query Runtime     │ │
│  │  (tokio runtime #1)  │  │  (tokio runtime #2)│ │
│  │                     │  │                    │ │
│  │  • OTLP sources     │  │  • DataFusion      │ │
│  │  • VRL transforms   │  │  • Loki/Tempo API  │ │
│  │  • Parquet writer   │  │  • Parquet reader   │ │
│  │  • Hot tier writer  │  │  • Hot tier reader  │ │
│  │                     │  │                    │ │
│  │  Threads: 60%       │  │  Threads: 40%      │ │
│  │  Memory: 60%        │  │  Memory: 40%       │ │
│  │  Priority: HIGH     │  │  Priority: NORMAL  │ │
│  │  (never drop data)  │  │  (queries can wait)│ │
│  └─────────────────────┘  └────────────────────┘ │
│                                                  │
│  Shared (read-only):                             │
│  • Parquet files on S3/disk                      │
│  • Iceberg catalog metadata                      │
│  • S3 client connection pool                     │
└──────────────────────────────────────────────────┘
```

**Why two runtimes?**
- A runaway query cannot starve the pipeline (separate thread pools)
- Memory budgets enforced per subsystem
- Pipeline always wins: ingestion must never lose data; queries can return errors
- This is a proven pattern: InfluxDB 3.0 separates ingestion from query the same way

### 14.4 DataFusion's built-in resource controls

DataFusion (recommended query engine) already provides per-query resource management:

```rust
// Limit query parallelism
SessionConfig::new()
    .with_target_partitions(4)            // max parallel partitions
    .with_batch_size(8192);               // rows per batch (memory control)

// Per-query memory limit with spill-to-disk
RuntimeConfig::new()
    .with_memory_limit(2_000_000_000, 0.8);  // 2GB max, spill at 80%

// Query timeout
execution.timeout = Duration::from_secs(30);

// Max rows returned
options.execution.parquet.pushdown_filters = true;  // predicate pushdown
```

These cover the query-side gaps:
- **Memory per query**: hard cap with spill-to-disk
- **Query timeout**: prevents infinite scans
- **Parallelism cap**: limits CPU usage per query
- **Predicate pushdown**: avoids full table scans on Parquet

### 14.5 Resource control matrix: existing + needed

| Layer | Pipeline (exists) | Backend (to build) |
|-------|-------------------|-------------------|
| **Event rate** | Throttle transform, Sample transform | N/A (queries, not events) |
| **Concurrency** | ARC (adaptive), fixed per sink | DataFusion `target_partitions` per query |
| **Memory** | Buffer `max_events` / `max_bytes` | DataFusion `memory_limit` per query + global budget for query runtime |
| **Backpressure** | Buffer block/drop/overflow | Query queue with max concurrent queries + rejection when full |
| **Timeout** | Request timeout (60s per sink) | Query timeout (30s default) |
| **Priority** | Implicit (pipeline always runs) | Pipeline runtime > Query runtime (thread allocation) |
| **Disk spill** | Disk buffers for durability | DataFusion disk spill for large queries |
| **Cost control** | Sample/throttle reduce volume | Query cost estimator: reject queries estimated to scan > N GB |

### 14.6 Summary

Vector's existing resource controls are **strong for the pipeline side** — backpressure, adaptive concurrency, rate limiting, and sampling are all production-grade. The gap is entirely on the **query/backend side**, and DataFusion fills most of it natively. The critical architectural decision is **dual tokio runtimes** to isolate pipeline from query and enforce the rule: **ingestion always wins, queries are best-effort**.

---

## 15. VRL vs SQL: Can SQL Replace VRL at Ingestion Time?

### 15.1 The two roles of a transform language

| Role | When | Purpose | Current tool |
|------|------|---------|-------------|
| **Streaming transform** | At ingestion, per event, microseconds | Parse, enrich, filter, route, redact | VRL |
| **Analytical query** | On demand, over stored data, seconds | Aggregate, correlate, search | SQL (planned) |

The question: can SQL do **both**?

### 15.2 What VRL does today (categorized by SQL replaceability)

#### Category A — SQL can replace these easily (~60% of VRL usage)

| VRL | SQL equivalent | Notes |
|-----|---------------|-------|
| `.severity = upcase(.severity)` | `SELECT UPPER(severity) as severity` | Standard string functions |
| `if .status_code >= 500 { .is_error = true }` | `CASE WHEN status_code >= 500 THEN true END as is_error` | CASE expressions |
| `.duration_ms = to_int!(.duration) / 1000000` | `CAST(duration AS INT) / 1000000 as duration_ms` | Type coercion |
| `del(.pii_field)` | Simply don't SELECT the field | Column projection |
| `.tags.env = "production"` | `'production' as tags_env` | Literal assignment |
| `if .level == "DEBUG" { abort }` | `WHERE level != 'DEBUG'` | Filtering |
| `.timestamp = now()` | `now() as timestamp` | Built-in functions |
| `.message = replace(.message, "secret", "[REDACTED]")` | `REPLACE(message, 'secret', '[REDACTED]') as message` | String functions |

#### Category B — SQL can replace with UDFs (~25% of VRL usage)

These are VRL's **domain-specific parsing functions**. SQL doesn't have them natively, but DataFusion supports **User-Defined Functions (UDFs)** in Rust:

| VRL | Possible SQL + UDF | Complexity |
|-----|-------------------|-----------|
| `parse_syslog!(.message)` | `SELECT parse_syslog(message).*` | Register VRL's parser as DataFusion UDF |
| `parse_json!(.message)` | `SELECT json_extract(message, '$.field')` | DataFusion has JSON functions; or register UDF |
| `parse_apache_log!(.message)` | `SELECT parse_apache_log(message).*` | Register as UDF |
| `parse_regex!(.message, r'pattern')` | `SELECT regexp_extract(message, 'pattern')` | DataFusion has regex support |
| `parse_timestamp!(.ts, "%Y-%m-%d")` | `SELECT to_timestamp(ts, '%Y-%m-%d')` | DataFusion has timestamp parsing |
| `encode_base64(.data)` | `SELECT base64_encode(data)` | Register as UDF |
| `redact(.message, filters: [...])` | `SELECT redact(message, ...)` | Register as UDF |

**Key insight**: VRL's parsing functions are just Rust functions. They can be registered as DataFusion UDFs, making them callable from SQL.

#### Category C — SQL CANNOT easily replace (~15% of VRL usage)

| VRL capability | Why SQL struggles | Impact |
|---------------|------------------|--------|
| **`abort` + `reroute_dropped`** | SQL has WHERE (filter) but no concept of "reroute failed events to a different output with error metadata". SQL either includes a row or excludes it — there's no third path. | High — this is critical for observability pipelines where you want to capture parsing failures |
| **`get_secret()` / `set_secret()`** | SQL has no concept of runtime secrets. Would need a special function or environment variable injection. | Medium — could be handled via UDF that accesses a secret store |
| **`get_enrichment_table_record()`** | SQL has JOINs, which are semantically similar. But VRL's enrichment tables are preloaded in-memory with index support. A SQL JOIN would need the table registered as a DataFusion table provider. | Medium — architecturally possible, just different |
| **Event splitting (1→N)** | VRL: `. = [event1, event2]` emits multiple events. SQL: `UNNEST()` / `LATERAL FLATTEN` exists but is less ergonomic for dynamic schemas. | Medium — doable but verbose |
| **`drop_on_error` semantics** | VRL: if ANY expression fails, drop or reroute the whole event. SQL: errors typically abort the query, not the row. Per-row error handling needs `TRY_CAST`, `COALESCE`, etc. — more verbose but possible. | Medium — different error model |
| **Dynamic schema manipulation** | VRL operates on semi-structured data (any field can be added/removed). SQL assumes fixed schemas. DataFusion's `struct` and `map` types help but are less flexible. | High — OTLP attributes are dynamic key-value pairs |

### 15.3 Streaming SQL is real — DataFusion supports it

DataFusion was **designed with streaming as a core architectural principle**:
- Most physical operators support "Unbounded" execution mode for infinite streams
- Arroyo (streaming SQL engine) is built entirely on DataFusion + Arrow
- InfluxDB 3.0 uses DataFusion for both batch queries AND streaming ingestion transforms

A streaming SQL transform in your pipeline would look like:

```sql
-- Ingestion-time transform (streaming, per-event)
CREATE VIEW cleaned_logs AS
SELECT
    parse_syslog(message) as parsed,
    UPPER(severity) as severity,
    REPLACE(body, regexp('\\d{3}-\\d{2}-\\d{4}'), '[SSN-REDACTED]') as body,
    now() as ingested_at,
    resource_attributes['service.name'] as service_name
FROM otlp_logs
WHERE severity != 'DEBUG'
  AND resource_attributes['service.name'] IS NOT NULL;
```

This is **more accessible** than the VRL equivalent:
```
. = parse_syslog!(.message)
.severity = upcase(.severity)
.body = redact(.body, filters: [r'\d{3}-\d{2}-\d{4}'])
.ingested_at = now()
.service_name = .resource.attributes."service.name"
if .severity == "DEBUG" { abort }
if !exists(.service_name) { abort }
```

### 15.4 The killer move: VRL functions AS DataFusion UDFs

Instead of choosing VRL OR SQL, expose VRL's domain-specific functions **inside SQL**:

```rust
// Register VRL parsers as DataFusion scalar UDFs
ctx.register_udf(create_udf(
    "parse_syslog",
    vec![DataType::Utf8],
    Arc::new(DataType::Struct(syslog_fields())),
    Volatility::Immutable,
    Arc::new(|args| { /* call VRL's parse_syslog internally */ }),
));

ctx.register_udf(create_udf("parse_apache_log", ...));
ctx.register_udf(create_udf("parse_json_nested", ...));
ctx.register_udf(create_udf("redact", ...));
ctx.register_udf(create_udf("get_secret", ...));
```

Then users write **SQL with observability superpowers**:

```sql
SELECT
    (parse_syslog(raw_message)).hostname,
    (parse_syslog(raw_message)).severity,
    redact(body, '\\b\\d{3}-\\d{2}-\\d{4}\\b') as clean_body,
    get_enrichment('geoip', source_ip) as geo
FROM otlp_logs
WHERE severity IN ('ERROR', 'CRITICAL')
```

**This is a unique differentiator**: Nobody offers SQL with built-in observability parsing functions. It combines SQL's accessibility with VRL's domain expertise.

### 15.5 Can SQL run over OTLP directly?

Yes. OTLP proto messages map naturally to SQL tables:

```
┌─────────────────────────────────────────────────────────┐
│  OTLP LogRecord proto                                   │
│                                                         │
│  time_unix_nano: u64        →  timestamp TIMESTAMP      │
│  severity_number: i32       →  severity_number INT      │
│  severity_text: string      →  severity TEXT            │
│  body: AnyValue             →  body TEXT / JSON         │
│  attributes: KeyValueList   →  attributes MAP<TEXT,TEXT> │
│  trace_id: bytes            →  trace_id TEXT (hex)      │
│  span_id: bytes             →  span_id TEXT (hex)       │
│  resource.attributes        →  resource MAP<TEXT,TEXT>   │
│  scope.name                 →  scope_name TEXT          │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  OTLP Span proto                                        │
│                                                         │
│  trace_id: bytes            →  trace_id TEXT (hex)      │
│  span_id: bytes             →  span_id TEXT (hex)       │
│  parent_span_id: bytes      →  parent_span_id TEXT      │
│  name: string               →  span_name TEXT           │
│  kind: SpanKind             →  span_kind TEXT           │
│  start_time_unix_nano: u64  →  start_time TIMESTAMP     │
│  end_time_unix_nano: u64    →  end_time TIMESTAMP       │
│  duration_ns: computed      →  duration_ns BIGINT       │
│  attributes: KeyValueList   →  attributes MAP<TEXT,TEXT> │
│  status.code: StatusCode    →  status TEXT              │
│  events: list<Event>        →  events ARRAY<STRUCT>     │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  OTLP Metric (Gauge/Sum/Histogram)                      │
│                                                         │
│  name: string               →  metric_name TEXT         │
│  description: string        →  description TEXT         │
│  unit: string               →  unit TEXT                │
│  data_points[].time         →  timestamp TIMESTAMP      │
│  data_points[].value        →  value DOUBLE             │
│  data_points[].attributes   →  attributes MAP<TEXT,TEXT> │
│  resource.attributes        →  resource MAP<TEXT,TEXT>   │
└─────────────────────────────────────────────────────────┘
```

DataFusion can register these as **streaming table providers** that read directly from the OTLP event stream (for ingestion transforms) or from Parquet files (for analytical queries). Same schema, same SQL, two execution modes.

### 15.6 Recommendation: SQL-first, VRL as escape hatch

| Phase | Transform language | Rationale |
|-------|-------------------|-----------|
| **Now** | VRL (existing) | Already works, battle-tested, zero effort |
| **MVP** | SQL for analytical queries only | DataFusion on Parquet, VRL still handles pipeline |
| **V2** | SQL for pipeline transforms too | Register VRL functions as DataFusion UDFs. Offer `type: sql_remap` transform alongside existing `type: remap` |
| **V3** | SQL as primary, VRL as escape hatch | SQL for 85% of use cases. VRL for edge cases (reroute_dropped, secret management, complex error handling) |
| **Long-term** | SQL everywhere? | If DataFusion streaming matures + all VRL functions are UDFs, VRL becomes optional. But don't remove it — backwards compatibility matters. |

**Why SQL-first wins for your SaaS**:
1. **Lower barrier to entry**: Every developer knows SQL. Nobody outside Vector knows VRL.
2. **One language for everything**: Same SQL for pipeline transforms AND analytical queries.
3. **Tooling ecosystem**: SQL works with dbt, Jupyter, BI tools, IDE autocomplete.
4. **Hiring**: You can hire SQL-literate ops engineers. VRL expertise doesn't exist in the market.
5. **Market positioning**: "Configure your observability pipeline with SQL" is a pitch that sells.

**Why keep VRL**:
1. **Backwards compatibility**: Existing Vector users have VRL configs.
2. **Edge cases**: `reroute_dropped`, secrets, enrichment tables are hard in pure SQL.
3. **Performance**: VRL compiles to optimized Rust. SQL has query planning overhead (though DataFusion is fast).
4. **Escape hatch**: When SQL can't express something, VRL can.

Sources:
- [DataFusion streaming architecture](https://www.flarion.io/blog/streaming-in-modern-query-engines-where-datafusion-shines)
- [DataFusion as streaming framework](https://www.streamingdata.tech/p/exploring-apache-datafusion-streaming-framework)
- [Arroyo SQL engine on DataFusion](https://www.arroyo.dev/blog/why-arrow-and-datafusion/)
- [Flink SQL streaming](https://www.ververica.com/blog/flink-streaming-sql-ksql-stream-processing)
- [Netflix streaming SQL in Data Mesh](https://netflixtechblog.com/streaming-sql-in-data-mesh-0d83f5a00d08)

---

## 16. DataFusion from Scratch vs Vector Fork — Build vs Reuse

### 16.1 The question

If DataFusion can handle streaming transforms AND analytical queries, should you build your entire product on DataFusion from scratch instead of forking Vector?

### 16.2 What Vector gives you (~400K LOC of battle-tested infrastructure)

| Component | LOC | What it does | Rebuild difficulty |
|-----------|-----|-------------|-------------------|
| **31 sources** | ~75K | OTLP gRPC/HTTP, Kafka, K8s logs, Prometheus scrape, syslog, file tailing, AWS S3/SQS/Kinesis, Docker, journald, etc. | VERY HIGH (31 protocols) |
| **52 sinks** | ~80K | S3, Elasticsearch, Loki, Prometheus remote_write, Kafka, ClickHouse, Datadog, Splunk, HTTP, etc. | VERY HIGH (52 APIs) |
| **Buffer/durability** | ~17K | WAL on disk, at-least-once delivery, ack tracking, crash recovery | VERY HIGH |
| **Topology orchestration** | ~6K | DAG-based pipeline, backpressure propagation, graceful reload, component health | VERY HIGH |
| **Networking** | ~38K | TLS/mTLS, auth (Basic/Bearer/OAuth/AWS SigV4/OIDC), proxy (HTTP/SOCKS5), compression (gzip/snappy/zstd) | HIGH |
| **Configuration** | ~16K | YAML/TOML/JSON, env var interpolation, hot reload, validation, schema generation | HIGH |
| **Internal observability** | ~16K | Per-component metrics, health checks, GraphQL API | MODERATE-HIGH |
| **Graceful shutdown** | ~9K | Signal handling, drain, coordinated task cancellation | MODERATE |
| **VRL runtime** | ~1K local + external crate | Full language: compiler, 100+ stdlib functions, type checker, diagnostics | EXTREME |
| **Codecs** | ~15K | JSON, protobuf, syslog, Apache log, CSV, GELF, native, Arrow IPC, OTLP | HIGH |
| **TOTAL** | **~400K** | | |

### 16.3 What DataFusion gives you

| Component | What it provides |
|-----------|-----------------|
| **SQL query engine** | Parse, plan, optimize, execute SQL on Arrow batches |
| **Parquet reader/writer** | Native, fast, with predicate pushdown |
| **Streaming execution** | Unbounded mode for infinite data streams |
| **UDF support** | Register custom Rust functions callable from SQL |
| **Memory management** | Per-query limits, spill-to-disk |
| **Arrow integration** | Zero-copy data interchange |
| **Table providers** | Pluggable data sources (S3, local files, custom) |

### 16.4 What DataFusion does NOT give you

| You need | DataFusion provides | You must build |
|----------|-------------------|----------------|
| OTLP receiver (gRPC + HTTP) | Nothing | tonic gRPC server + proto decoding (~3K LOC minimum) |
| Network sources (Kafka, syslog, file, K8s) | Nothing | Each source from scratch |
| Network sinks (S3, Loki, Prometheus, ES) | Parquet writer only | Each sink from scratch |
| Buffer / WAL / at-least-once | Nothing | Full durability system (~17K LOC) |
| Backpressure | Nothing | Channel-based flow control |
| TLS / mTLS / auth | Nothing | openssl integration, auth middleware |
| Configuration DSL | Nothing | Config loading, validation, hot reload |
| Graceful shutdown | Nothing | Signal handling, drain coordination |
| Internal metrics | Nothing | Per-component instrumentation |
| Event routing (fan-out, filter, route) | WHERE clause only | Multi-output routing logic |
| Error handling (reroute_dropped) | Query errors | Per-event error routing |

### 16.5 Can DataFusion act as an OTel Agent?

**No.** An OTel Agent needs:

| Agent requirement | DataFusion capability |
|------------------|----------------------|
| Run as lightweight sidecar/daemon (~20MB RAM) | DataFusion is a query engine (~100MB+ baseline) |
| Tail local log files | No file watching capability |
| Receive OTLP from local apps | No gRPC/HTTP server |
| Scrape Prometheus endpoints | No HTTP client for scraping |
| Collect host metrics (CPU, disk, memory) | No system metric access |
| Forward to gateway with backpressure | No outbound connection management |
| Survive restarts (disk buffer) | No persistence layer |

DataFusion is designed to **query data**, not **collect data**. Using it as an agent would be like using PostgreSQL as a log collector — technically possible but fundamentally the wrong tool.

### 16.6 Can DataFusion act as an OTel Gateway?

**Partially, with significant effort.** A Gateway needs:

| Gateway requirement | DataFusion capability | Gap |
|--------------------|----------------------|-----|
| Receive OTLP from agents | No — need tonic gRPC server | Must build |
| Transform events | **Yes** — streaming SQL | Works |
| Route to multiple backends | No — SQL has no "output routing" concept | Must build |
| Sample / rate-limit | Partial — WHERE clause filters, but no token-bucket rate limiting | Must build |
| Buffer under load | No durability | Must build |
| Forward to backends (OTLP, S3, Loki, etc.) | Parquet writer only | Must build sinks |
| TLS / auth | No | Must build |
| Backpressure | No | Must build |

You'd end up building ~80% of what Vector already provides, but with SQL transforms instead of VRL. The transform layer is only ~10% of Vector's codebase — the other 90% is infrastructure.

### 16.7 The honest comparison

```
Option A: Start from DataFusion
┌──────────────────────────────────────────────────┐
│  You build:                                       │
│  ┌──────────────┐  ┌───────────┐  ┌───────────┐  │
│  │ OTLP receiver│→ │ DataFusion│→ │ Parquet   │  │
│  │ (build)      │  │ (exists)  │  │ writer    │  │
│  │              │  │           │  │ (exists)  │  │
│  │ + TLS        │  │ streaming │  │           │  │
│  │ + auth       │  │ SQL       │  │ + S3 sink │  │
│  │ + backpress. │  │ transforms│  │ (build)   │  │
│  │ + buffers    │  │           │  │           │  │
│  └──────────────┘  └───────────┘  └───────────┘  │
│                                                   │
│  What you get: SQL transforms + SQL queries       │
│  What you lose: 31 sources, 52 sinks, VRL,        │
│    durability, backpressure, config, observability │
│  Time to MVP: 6-12+ months                        │
│  Risk: HIGH — building infra from scratch          │
└──────────────────────────────────────────────────┘

Option B: Fork Vector + add DataFusion
┌──────────────────────────────────────────────────┐
│  Vector (exists):          You add:               │
│  ┌──────────────┐  ┌───────────┐  ┌───────────┐  │
│  │ 31 sources   │→ │ VRL/SQL   │→ │ Parquet   │  │
│  │ (exists)     │  │ transforms│  │ sink      │  │
│  │              │  │ (VRL      │  │ (build)   │  │
│  │ + OTLP       │  │  exists,  │  │           │  │
│  │ + TLS/auth   │  │  add SQL) │  │ DataFusion│  │
│  │ + buffers    │  │           │  │ query API │  │
│  │ + backpress. │  │           │  │ (build)   │  │
│  └──────────────┘  └───────────┘  └───────────┘  │
│                                                   │
│  What you get: EVERYTHING + SQL queries            │
│  What you build: Parquet sink + query API          │
│  Time to MVP: 2-4 months                           │
│  Risk: LOW — building on proven infrastructure     │
└──────────────────────────────────────────────────┘
```

### 16.8 What about Arroyo? (DataFusion-based streaming)

Arroyo is a streaming SQL engine built on DataFusion. Could you use Arroyo instead of Vector?

| Arroyo provides | Arroyo lacks |
|----------------|-------------|
| Streaming SQL on DataFusion | No OTLP source (Kafka + WebSocket only) |
| Windowed aggregations | No observability-specific features |
| Watermarks and late data handling | No Parquet/S3 analytical query mode |
| Kafka source/sink | No Loki/Tempo/Prometheus API compatibility |
| Web UI | No durability/WAL for at-least-once |

Arroyo is closer to a Flink alternative than a Vector alternative. It handles the **streaming compute** layer well, but lacks the **observability infrastructure** (OTLP, telemetry-specific parsing, Grafana API compatibility).

### 16.9 The right architecture: Vector for plumbing, DataFusion for brains

The industry pattern is clear:

| Product | Plumbing (I/O, buffers, networking) | Brains (query, analytics) |
|---------|-------------------------------------|--------------------------|
| **InfluxDB 3.0** | Custom ingestion layer | DataFusion |
| **Parseable** | Custom HTTP receiver | DataFusion |
| **Arroyo** | Custom connectors (Kafka, WebSocket) | DataFusion |
| **Comet (Spark)** | Apache Spark | DataFusion |
| **Your product** | **Vector** (exists, proven) | **DataFusion** (add) |

Nobody uses DataFusion for I/O. Everyone uses DataFusion for **query and compute**. The pattern is: let a mature I/O framework handle the plumbing (sources, sinks, buffers, TLS, auth) and plug DataFusion in for the intelligence layer.

### 16.10 Decision matrix

| Criteria | DataFusion from scratch | Vector fork + DataFusion |
|----------|------------------------|--------------------------|
| Time to MVP | 6-12+ months | 2-4 months |
| OTLP support | Must build | Exists (3 signals, gRPC+HTTP) |
| Sources (Kafka, K8s, syslog, etc.) | Must build each one | 31 exist |
| Sinks (S3, Loki, Prom, etc.) | Must build each one | 52 exist |
| Durability / at-least-once | Must build WAL (~17K LOC) | Exists, battle-tested |
| Backpressure | Must build | Exists, production-grade |
| TLS / auth | Must build | Exists (mTLS, OAuth, SigV4, etc.) |
| SQL transforms | Native | Add as new transform type |
| SQL analytical queries | Native | Add via DataFusion integration |
| Agent mode | Poor fit (too heavy) | Exists |
| Gateway mode | Must build routing | Exists |
| Backend mode | Good fit | Add storage + query layer |
| Code ownership (no CLA) | 100% yours | Your new files are yours; Vector files are MPL-2.0 |
| Technical debt | None (fresh start) | Inherit Vector's codebase |
| Community / ecosystem | Start from zero | Leverage Vector's ecosystem |
| License cleanliness | Apache-2.0 (DataFusion) | MPL-2.0 (Vector) + your license (new files) |

### 16.11 Recommendation

**Fork Vector. Add DataFusion for the backend layer.**

DataFusion is the right choice for:
- The query engine (SQL over Parquet)
- Streaming SQL transforms (as a new transform type alongside VRL)
- Memory-managed analytical processing

DataFusion is the wrong choice for:
- Data collection (agent)
- Network I/O (sources, sinks)
- Durability (buffers, WAL)
- Pipeline orchestration (backpressure, routing, fan-out)

The 90% of Vector you'd be throwing away by starting fresh is the **hardest, most boring, most time-consuming infrastructure to rebuild** — and it has nothing to do with VRL or transforms. It's networking, buffers, auth, config, shutdown, and integrations. That's 400K LOC of production-grade Rust you'd be rewriting.

**The winning architecture is: Vector handles the I/O, DataFusion handles the intelligence.**

Sources:
- [InfluxDB 3.0 on DataFusion](https://www.influxdata.com/blog/7-datafusion-projects-influxdb/)
- [Arroyo SQL engine on DataFusion](https://www.arroyo.dev/blog/why-arrow-and-datafusion/)
- [DataFusion as streaming framework](https://www.streamingdata.tech/p/exploring-apache-datafusion-streaming-framework)
- [DataFusion documentation](https://datafusion.apache.org/user-guide/introduction.html)

---

## 17. Unified Backend vs Separate Backends: Problems, Blockers, and Trade-offs

### 17.1 The status quo: separate backends (Grafana model)

```
                      ┌─────────┐
                      │ Grafana │  ← 3 query languages (PromQL, LogQL, TraceQL)
                      └────┬────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │  Mimir   │ │  Loki   │ │  Tempo  │  ← 3 separate backends
         │ (metrics)│ │ (logs)  │ │ (traces)│  ← 3 storage engines
         │ PromQL   │ │ LogQL   │ │ TraceQL │  ← 3 query languages
         └─────────┘ └─────────┘ └─────────┘  ← 3 operational burdens
```

### 17.2 Problems NOT solved by separate backends

#### A. Cross-signal correlation is superficial, not analytical

| Problem | Detail | Impact |
|---------|--------|--------|
| **No cross-signal JOINs** | Grafana links signals via UI navigation (click from metric → trace → log). But the BACKENDS cannot perform joint analysis. You cannot ask "which services have error logs that correlate with latency spikes?" in a single query. | Debugging is manual. MTTR increases. |
| **"Swivel-chair analysis"** | Engineers get a metric alert in Mimir, pivot to Loki to search logs, pivot to Tempo to find traces. Each pivot is a manual step with context loss. | 38% of teams cite poor signal-to-noise as their #1 obstacle. |
| **No causal correlation** | When CPU spikes, you see the metric. But was it caused by a specific trace? A burst of error logs? Separate backends can't answer this — they literally don't have each other's data. | Root cause analysis depends on engineer intuition, not data. |
| **Duplicate data across backends** | trace_id stored in Loki logs AND Tempo traces. service_name in all three. resource attributes duplicated 3x. | Storage waste: 15-30% overhead from duplication. |

#### B. Alerting is fragmented and signal-blind

| Problem | Detail | Impact |
|---------|--------|--------|
| **Alerts can't span signals** | Mimir alerts on metrics only. Loki alerts on log patterns only. You CANNOT create an alert like: "fire when p99 latency > 500ms AND error logs > 100/min AND trace error rate > 5% for service X". | Most critical incidents involve multiple signals. Single-signal alerts miss compound failures. |
| **Alert correlation is manual** | When 3 alerts fire simultaneously (metric spike + error logs + failed traces), a human must correlate them. No system knows they're the same incident. | Alert fatigue: teams receive N separate alerts for 1 incident. |
| **No metric-from-logs** | You can't efficiently create a metric (e.g., error rate) from log data without shipping logs through a separate pipeline to Mimir. | Duplication of pipeline logic. Some signals never become metrics. |
| **No metric-from-traces** | Same problem: deriving RED metrics (Rate, Errors, Duration) from traces requires a separate spanmetrics connector, duplicating data into Mimir. | Added complexity, data staleness. |

#### C. Operational cost multiplied by 3

| Problem | Detail | Impact |
|---------|--------|--------|
| **3 backends to operate** | Mimir, Loki, Tempo each need: deployment, scaling, monitoring, upgrades, backup, disaster recovery. | 3x operational overhead. Most teams don't have dedicated infra for each. |
| **3 scaling models** | Mimir scales by metric series cardinality. Loki scales by log volume. Tempo scales by trace throughput. Different knobs, different bottlenecks. | Expertise needed in 3 different systems. |
| **3 storage strategies** | Each backend has its own compaction, retention, tiering, and garbage collection. | Configuration complexity. Risk of misconfiguration. |
| **3 query languages to learn** | PromQL, LogQL, TraceQL. Each with its own syntax, semantics, and limitations. | Higher learning curve. Fewer engineers can be effective. |
| **Inconsistent retention** | Metrics kept for 13 months, logs for 14 days, traces for 7 days. When investigating a historical issue, some signals are gone. | Incomplete investigations. Blind spots. |

#### D. Data consistency gaps

| Problem | Detail | Impact |
|---------|--------|--------|
| **Clock skew between backends** | Logs, metrics, and traces ingested at slightly different times, with different timestamp precision. | Visual correlation in Grafana is approximate, not exact. |
| **Schema divergence** | service_name might be `service.name` in traces but `service` in logs and `job` in metrics. No shared schema enforcement. | Manual field mapping. Broken correlations when naming changes. |
| **Partial failures** | If Loki ingestion is down but Mimir is up, you have metrics without corresponding logs for that period. No system detects this inconsistency. | Incomplete data without visibility into the gap. |

### 17.3 What a UNIFIED backend unblocks

#### A. True cross-signal analytics (the biggest unlock)

```sql
-- IMPOSSIBLE with separate backends. TRIVIAL with unified:

-- "What services have error logs correlated with latency spikes?"
SELECT
    t.service_name,
    AVG(t.duration_ms) as avg_latency,
    COUNT(l.severity) FILTER (WHERE l.severity = 'ERROR') as error_logs,
    COUNT(DISTINCT t.trace_id) as trace_count
FROM traces t
LEFT JOIN logs l ON t.trace_id = l.trace_id
WHERE t.timestamp > now() - INTERVAL '1 hour'
  AND t.duration_ms > 500
GROUP BY t.service_name
HAVING error_logs > 0
ORDER BY error_logs DESC;

-- "Alert: fire when latency AND errors AND log volume all spike together"
-- With unified data, this is a single SQL query on one table set.
```

#### B. Compound alerting across signals

| Unified alert capability | How it works |
|-------------------------|-------------|
| **Multi-signal alerts** | One alert rule queries logs + metrics + traces together. "p99 latency > 500ms AND error_log_rate > 100/min for service X" |
| **Automatic incident grouping** | Alerts from the same time window + same service are one incident, because the data is in one place. |
| **Derived metrics from any signal** | Compute error_rate from logs, RED metrics from traces — no separate pipeline needed. |
| **Anomaly detection across signals** | ML model sees all signals together. A CPU spike alone is normal; a CPU spike + error logs + slow traces is an incident. |

#### C. Operational simplification

| Before (3 backends) | After (1 backend) |
|---------------------|-------------------|
| 3 deployments to manage | 1 deployment |
| 3 scaling strategies | 1 scaling strategy (data volume) |
| 3 storage configurations | 1 storage configuration (Parquet on S3) |
| 3 retention policies | 1 unified retention policy |
| 3 query languages (PromQL, LogQL, TraceQL) | 1 language (SQL) |
| 3 backup/DR procedures | 1 backup/DR procedure |
| Inconsistent data across backends | Single source of truth |

#### D. Cost reduction through deduplication

| Duplicated data in separate backends | Unified backend |
|-------------------------------------|-----------------|
| trace_id stored in Loki AND Tempo | Stored once, JOINed by reference |
| service_name in all 3 backends | Stored once in resource table |
| resource attributes (3x copies) | Stored once, shared across signals |
| Estimated 15-30% storage waste | Eliminated |

### 17.4 Trade-offs and challenges of a UNIFIED backend

**This is where honesty matters.** A unified backend isn't free — there are real engineering challenges:

#### A. Metrics have fundamentally different access patterns

| Aspect | Logs & Traces | Metrics | Conflict |
|--------|--------------|---------|----------|
| **Query pattern** | Search (needle in haystack) + analytics (aggregations) | Time-series (plot over time, range queries) | Metrics need fast range scans; logs need full-text search |
| **Write pattern** | Append-only, immutable events | Continuous streams of data points, often at fixed intervals | Metrics are tiny (timestamp + value), logs are large (KB+ each) |
| **Cardinality** | Naturally high (each event is unique) | Must be controlled (each label combo = separate series) | High-cardinality metrics in Parquet is fine; but PromQL assumes low-cardinality series |
| **Aggregation** | On-demand (GROUP BY at query time) | Often pre-aggregated (rollups, recording rules) | Without pre-aggregation, metric dashboards are slow on raw Parquet |
| **Freshness** | Seconds acceptable | Sub-second expected for dashboards | Hot tier must be very fast for metrics; logs can tolerate slight delay |
| **Retention** | Days to weeks (logs), days (traces) | Months to years (metrics for capacity planning) | Different retention needs per signal type |

**The core tension**: Metrics dashboards expect sub-second response on time-range queries. Parquet-on-S3 delivers seconds, not milliseconds. A unified backend MUST have a hot tier for recent metrics to match Prometheus/Mimir dashboard performance.

#### B. Pre-aggregation / rollups are still needed for metrics

| Without rollups | With rollups |
|----------------|-------------|
| Dashboard "avg CPU over 30 days" scans 2.6M data points per metric | Scans 720 pre-aggregated hourly points |
| Query: 2-10 seconds on Parquet | Query: <100ms from pre-computed table |
| Cost: high (full scan per dashboard load) | Cost: low (small pre-computed tables) |

**The trade-off of rollups**: Once data is rolled up, original fidelity is gone. If the aggregation was too coarse or wrong dimensions were omitted, there is no way to recover them. This creates **observability blind spots**.

**Solution**: Store raw data in Parquet (unlimited retention, full fidelity) AND maintain rollup tables for dashboard performance. Query the rollups for dashboards, query raw Parquet for investigations. This is more complex than pure Mimir but gives you both speed and fidelity.

#### C. PromQL compatibility is harder without a TSDB

| PromQL feature | Native TSDB (Mimir) | Columnar/Parquet backend |
|---------------|---------------------|--------------------------|
| `rate()` over counter resets | Built-in, handles resets automatically | Must detect resets in raw data, more complex |
| `histogram_quantile()` | Native histogram bucket support | Must reconstruct from raw histogram data points |
| `absent()` / `absent_over_time()` | Knows all registered series | Must infer from data — harder without series registry |
| Label matchers (`{job=~".*"}`) | Inverted index, sub-millisecond | Column scan, slower for high-cardinality labels |
| Recording rules | Native, continuous evaluation | Must build a scheduler for continuous SQL queries |
| Alert evaluation | Native PromQL evaluator | Must build SQL-based alert evaluator |

**Implication**: Full PromQL compatibility on Parquet is a **significant engineering effort**. It's easier to support SQL and offer a Grafana plugin that translates simple PromQL to SQL, accepting that complex PromQL (recording rules, histogram functions) requires custom implementation.

#### D. Reliability: single point of failure

| Separate backends | Unified backend |
|-------------------|----------------|
| Loki down → logs lost, but metrics and traces still work | Backend down → ALL signals lost |
| Mimir slow → metric dashboards slow, but log search still fast | Backend slow → everything slow |
| Can upgrade Tempo without touching Loki or Mimir | Upgrade affects all signals |
| Blast radius is 1/3 of observability | Blast radius is 100% of observability |

**Mitigation**: This is a standard single-point-of-failure problem, solvable with:
- Replication (multi-AZ, read replicas)
- Separate ingestion and query paths (ingestion writes to S3 directly — survives query outages)
- Buffer/WAL in Vector pipeline (survives backend downtime)
- The dual-runtime architecture (Section 14.3) helps: pipeline runtime keeps buffering even if query runtime crashes

#### E. Schema complexity for metrics

| Signal | Schema | Complexity |
|--------|--------|-----------|
| **Logs** | `(timestamp, body, severity, attributes, trace_id, resource)` | Simple — each log is a self-contained row |
| **Traces** | `(trace_id, span_id, parent_span_id, name, duration, attributes, resource)` | Moderate — tree structure, but each span is a row |
| **Metrics** | `(timestamp, name, value, type, attributes, resource)` BUT with Gauge/Sum/Histogram/ExponentialHistogram/Summary subtypes, each with different fields | **Complex** — histogram has `bucket_counts[]`, `explicit_bounds[]`; summary has `quantile_values[]`; counters need reset handling |

Metrics in OTLP are significantly more complex than logs or traces. A unified Parquet schema must handle all metric subtypes without wasting storage on sparse columns.

**Solution**: Use Parquet's nested types and separate partition for metric subtypes:
```
parquet/
├── logs/          ← simple schema
├── traces/        ← moderate schema
├── metrics/
│   ├── gauge/     ← (timestamp, name, value, attributes)
│   ├── sum/       ← (timestamp, name, value, is_monotonic, attributes)
│   ├── histogram/ ← (timestamp, name, count, sum, bucket_counts[], bounds[], attributes)
│   └── summary/   ← (timestamp, name, count, sum, quantiles[], attributes)
```

### 17.5 Decision framework: which problems justify unification?

| Problem | Severity with separate backends | Fixed by unification? | Trade-off introduced? |
|---------|-------------------------------|----------------------|----------------------|
| **Cross-signal SQL JOINs** | Cannot do at all | **Yes — fully solved** | None |
| **Compound alerting** | Cannot do at all | **Yes — fully solved** | Must build alert evaluator |
| **"Swivel-chair" debugging** | Severe (manual context-switching) | **Yes — one query** | None |
| **3x operational overhead** | Real cost (3 systems to run) | **Yes — one system** | Single point of failure (mitigable) |
| **Data duplication** | 15-30% storage waste | **Yes — eliminated** | None |
| **Schema consistency** | Divergent naming across backends | **Yes — one schema** | Must handle metric complexity |
| **Metric dashboard speed** | Not a problem (Mimir is fast) | **Regression risk** | Must build hot tier + rollups |
| **PromQL compatibility** | Not a problem (Mimir is native) | **Regression risk** | Must build PromQL-to-SQL or subset |
| **Counter reset handling** | Not a problem (Mimir handles natively) | **Regression risk** | Must implement in query layer |
| **Recording rules** | Not a problem (Mimir native) | **Regression risk** | Must build continuous query scheduler |
| **Reliability / blast radius** | Isolated (1/3 at risk) | **Regression risk** | Must invest in replication + buffer |

### 17.6 Recommended strategy: unified storage, phased signal support

Given the trade-offs, the pragmatic approach is:

```
Phase 1 (MVP): Logs only → unified backend (SQL + Loki API)
  ✓ Logs are simplest schema
  ✓ Highest customer pain (cost)
  ✓ No PromQL/metrics complexity
  ✓ Proves the architecture

Phase 2: Add Traces → unified backend (SQL + Tempo API)
  ✓ Traces are second-simplest (spans as rows)
  ✓ Unlocks cross-signal JOINs (logs ↔ traces via trace_id)
  ✓ Enables compound alerting (error logs + slow traces)
  ✓ This is where "unified" becomes visibly superior

Phase 3: Add Metrics → unified backend (SQL + Prometheus API)
  ⚠ Most complex signal (histogram, counter resets, rollups)
  ⚠ Requires hot tier for dashboard speed
  ⚠ Requires pre-aggregation / continuous queries for PromQL-like behavior
  ⚠ Consider: start with gauge/sum only, add histogram later

Alternative for Phase 3: Proxy to Mimir for metrics
  → Accept that metrics are hard and proxy PromQL queries to a Mimir instance
  → Still store raw metric data in Parquet for SQL analytics
  → Dashboards use Mimir (fast), investigations use SQL on Parquet (powerful)
  → This is a pragmatic compromise that avoids rebuilding a TSDB
```

### 17.7 Summary

| | Separate backends | Unified backend |
|--|-------------------|-----------------|
| **Unblocked** | — | Cross-signal JOINs, compound alerting, single operational burden, no data duplication, one query language, consistent schema |
| **Regressed** | — | Metric dashboard latency (needs hot tier), PromQL compatibility (needs translation), blast radius (needs replication), metric complexity (needs special handling) |
| **Net assessment** | The problems that separate backends CANNOT solve (cross-signal correlation, compound alerting) are the highest-value features in the market. The trade-offs introduced by unification (metric performance, PromQL compat) are **solvable engineering problems**, not fundamental limitations. |

**Bottom line**: The features that unification enables are **impossible** to add to separate backends. The trade-offs that unification introduces are **hard but solvable**. This asymmetry favors the unified approach — but phase it wisely (logs first, traces second, metrics last).

Sources:
- [Dynatrace - Unified observability](https://www.dynatrace.com/news/blog/unified-observability-why-storing-opentelemetry-signals-in-one-place-matters/)
- [ClickHouse - Observability as analytics](https://clickhouse.com/resources/engineering/what-is-observability)
- [ClickHouse - Three villains of observability](https://clickhouse.com/blog/three-villains-agentic-observability)
- [Parseable - High cardinality and columnar](https://www.parseable.com/blog/high-cardinality-meets-columnar-time-series-system)
- [ClickHouse - Lakehouses for observability](https://clickhouse.com/blog/lakehouses-path-to-low-cost-scalable-no-lockin-observability)
- [OpenObserve - Full-stack observability](https://openobserve.ai/blog/full-stack-observability-logs-metrics-traces/)
- [SigNoz - Unified observability](https://signoz.io/unified-observability/)
- [Grafana Observability Survey 2025](https://grafana.com/observability-survey/2025/)

### 17.8 Correction: OTLP delta temporality eliminates counter reset handling

The previous analysis (Section 17.4.C) listed PromQL counter reset handling as a trade-off. **This is incorrect for an OTLP-native backend.**

OTLP metrics use **delta temporality** by default: the producer pushes the difference since the last report, not an ever-increasing counter. This means:

| Prometheus model (pull, cumulative) | OTLP model (push, delta) |
|-------------------------------------|--------------------------|
| Counter is monotonically increasing | Delta is the increment since last push |
| Receiver must detect resets (process restart → counter drops to 0) | **No resets to detect** — delta is always positive |
| `rate()` must handle reset math | `rate()` is just `SUM(deltas) / time_window` |
| Complex stateful logic in query engine | Simple stateless aggregation |

**Impact on unified backend**: One of the hardest PromQL implementation challenges (counter reset detection) **disappears entirely** when ingesting OTLP delta metrics. This significantly reduces the engineering effort for metrics support.

What remains hard:
- `histogram_quantile()` — still needs to reconstruct from OTLP ExponentialHistogram buckets
- `absent()` / `absent_over_time()` — still needs series awareness
- Recording rules — still needs a continuous query scheduler

---

### 17.9 Infrastructure cost comparison: Parquet+DataFusion vs Mimir+Loki+Tempo

#### A. Baseline: Grafana LGTM stack resource requirements

**Mimir (metrics)** — for 1M active series, ~25K samples/sec:

| Component | CPU | Memory | Disk | Replicas | Total CPU | Total Memory |
|-----------|-----|--------|------|----------|-----------|-------------|
| Distributor | 1 | 1 GB | — | 1 | 1 | 1 GB |
| Ingester | 3.3 | 8.3 GB | 16.7 GB | 3 (replication) | 10 | 25 GB |
| Querier | 1 | 1 GB | — | 1 | 1 | 1 GB |
| Store-gateway | 1 | 1 GB | 13 GB | 1 | 1 | 1 GB |
| Compactor | 1 | 4 GB | 300 GB | 1 | 1 | 4 GB |
| Query-frontend | 1 | 1 GB | — | 1 | 1 | 1 GB |
| **TOTAL** | | | | | **15 CPU** | **33 GB RAM** |

Formula: 1 CPU / 25K samples/sec (distributor), 1 CPU / 300K series (ingester), 2.5 GB / 300K series (ingester memory)

**Loki (logs)** — small tier (<3 TB/day):

| Component | CPU | Memory | Replicas | Total CPU | Total Memory |
|-----------|-----|--------|----------|-----------|-------------|
| Distributor | 2 | 0.5 GB | 4 | 8 | 2 GB |
| Ingester | 2 | 4 GB | 6 | 12 | 24 GB |
| Querier | 1 | 1 GB | 10 | 10 | 10 GB |
| Query-frontend | 1 | 2 GB | 2 | 2 | 4 GB |
| Index-gateway | 0.5 | 2 GB | 4 | 2 | 8 GB |
| Compactor | 2 | 10 GB | 1 | 2 | 10 GB |
| **TOTAL** | | | | **36 CPU** | **58 GB RAM** |

**Tempo (traces)** — minimum viable:

| Component | CPU | Memory | Notes |
|-----------|-----|--------|-------|
| Minimum node | 16 | 64 GB | Grafana recommends 1:4 CPU:memory ratio, minimum 16 cores |
| **TOTAL** | **16 CPU** | **64 GB RAM** | Plus SSD for WAL, S3 for blocks |

**Combined LGTM stack total** (for a modest deployment):

| Signal | CPU | Memory | Disk |
|--------|-----|--------|------|
| Mimir (metrics) | 15 | 33 GB | 330 GB SSD |
| Loki (logs) | 36 | 58 GB | S3 + index |
| Tempo (traces) | 16 | 64 GB | SSD + S3 |
| **TOTAL** | **67 CPU** | **155 GB RAM** | **330+ GB SSD + S3** |

Plus: 3 separate deployments to manage, 3 sets of config, 3 upgrade cycles.

#### B. Estimated: Unified Parquet+DataFusion backend

For equivalent workload (1M metric series, <3 TB/day logs, traces):

**Ingestion path** (Vector pipeline):

| Component | CPU | Memory | Notes |
|-----------|-----|--------|-------|
| OTLP receiver | 2 | 2 GB | tonic gRPC server, all 3 signals |
| Transforms (VRL/SQL) | 4 | 4 GB | Depends on transform complexity |
| Parquet writer | 2 | 4 GB | Batch Arrow records → Parquet, flush to S3 |
| Buffer/WAL | — | 2 GB | In-memory buffer + disk WAL |
| **Pipeline total** | **8 CPU** | **12 GB RAM** | Single process (Vector) |

**Query path** (DataFusion):

| Component | CPU | Memory | Notes |
|-----------|-----|--------|-------|
| DataFusion engine | 4-8 | 8-16 GB | Configurable per-query memory limit |
| HTTP API server | 1 | 1 GB | Loki/Tempo/Prometheus-compatible endpoints |
| **Query total** | **5-9 CPU** | **9-17 GB RAM** | Second runtime in same process |

**Hot tier** (recent data, fast queries):

| Option | CPU | Memory | Notes |
|--------|-----|--------|-------|
| In-memory ring buffer | 0 | 4-16 GB | Last 15-60 min of all signals, sub-ms queries |
| Redis/DragonflyDB | 2 | 8-32 GB | External process, discussed below |
| Local SSD cache | 1 | 2 GB | Parquet files cached on NVMe, 10-100ms queries |

**Unified backend total** (conservative estimate):

| Component | CPU | Memory |
|-----------|-----|--------|
| Pipeline (Vector) | 8 | 12 GB |
| Query engine (DataFusion) | 9 | 17 GB |
| Hot tier (in-memory) | 0 | 16 GB |
| **TOTAL** | **17 CPU** | **45 GB RAM** |

#### C. Side-by-side comparison

| Resource | LGTM stack (3 backends) | Unified backend | Savings |
|----------|------------------------|----------------|---------|
| **CPU** | 67 cores | 17 cores | **~75% less** |
| **Memory** | 155 GB | 45 GB | **~71% less** |
| **SSD disk** | 330+ GB | ~50 GB (WAL + cache) | **~85% less** |
| **S3 storage** | 3x copies (Mimir blocks + Loki chunks + Tempo blocks) | 1x Parquet (deduplicated) | **~60% less** |
| **Processes** | 15-20+ (across 3 stacks) | 1 (Vector all-in-one) | **~95% less** |
| **Deployments** | 3 separate Helm charts | 1 binary | **~67% less** |

**Caveats on these estimates**:
- Loki/Mimir/Tempo numbers are from Grafana's official documentation for production deployments
- Unified backend numbers are estimates based on DataFusion benchmarks and Vector's current resource profile
- Actual savings depend on query patterns, retention, and data volume
- The unified backend has not been benchmarked in production yet

#### D. Where the unified backend costs MORE

| Scenario | LGTM wins | Why |
|----------|-----------|-----|
| **Metric dashboards** (sub-second range queries) | Mimir ingester serves from memory, ~10ms | DataFusion on Parquet: 100ms-2s. Needs hot tier. |
| **Log full-text search** | Loki's inverted index is optimized for grep-like queries | Parquet column scan: slower for substring search across all log bodies |
| **High-concurrency dashboard refresh** | Mimir/Loki handle hundreds of concurrent queries | DataFusion single-process: needs careful concurrency limits |
| **Very high metric cardinality** (>10M series) | Mimir ingester is purpose-built for this | DataFusion handles it via Parquet column scans, but slower for time-range queries on specific series |

#### E. Query latency comparison

| Query type | Mimir/Loki/Tempo | DataFusion + Parquet (S3) | DataFusion + hot tier |
|-----------|-----------------|--------------------------|----------------------|
| Metric range (last 1h) | ~10-50ms | 500ms-2s | **~10-50ms** (from cache) |
| Metric range (last 30d) | ~100-500ms | 1-5s | 1-5s (cold, from S3) |
| Log search (last 1h) | ~100-500ms | 200ms-1s | **~50-200ms** (from cache) |
| Log search (last 30d) | ~1-10s | 2-10s | 2-10s (comparable) |
| Trace by ID | ~50-200ms | 200ms-1s (needs bloom filter) | **~10-50ms** (from cache) |
| Cross-signal JOIN | **IMPOSSIBLE** | 1-5s | **~500ms-2s** (from cache) |

**Key insight**: For recent data (where 90%+ of queries land), a hot tier makes the unified backend **competitive with LGTM**. For historical data, the unified backend is slightly slower but offers SQL JOINs that LGTM cannot do at all.

### 17.10 Redis / DragonflyDB as hot tier for rollup tables

#### A. Why Redis is a strong fit

| Capability | Redis/Dragonfly | Why it matters |
|-----------|-----------------|---------------|
| **Sub-millisecond reads** | Hash/sorted set lookups | Dashboard metrics need <50ms response |
| **Built-in TTL** | Auto-expire old data | Hot tier retains last N minutes, cold goes to Parquet |
| **RedisTimeSeries module** | Native downsampling + aggregation rules | `TS.CREATERULE` auto-computes rollups (avg, sum, min, max per minute/hour) |
| **Sorted sets** | Range queries by timestamp | `ZRANGEBYSCORE` for time-range metric queries |
| **Pub/Sub** | Real-time tailing | Live log/trace tailing for Grafana |
| **Low operational overhead** | Single process, battle-tested | Simpler than running Mimir ingesters |

#### B. Architecture with Redis hot tier

```
┌─────────────────────────────────────────────────────────┐
│                     Vector Pipeline                      │
│                                                         │
│  OTLP in → transform → ┬→ Parquet writer → S3 (cold)   │
│                         │                               │
│                         └→ Redis writer → Redis (hot)   │
│                            (last 1-4 hours)             │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                     Query Engine                         │
│                                                         │
│  Query arrives → router decides:                        │
│                                                         │
│  Recent data (last 1-4h)?                               │
│    → Query Redis (sub-ms)                               │
│    → For metrics: read pre-computed rollups              │
│    → For logs/traces: read from sorted sets             │
│                                                         │
│  Historical data (>4h)?                                 │
│    → Query DataFusion on Parquet/S3 (seconds)           │
│    → Full SQL power, JOINs across signals               │
│                                                         │
│  Cross-signal analytics?                                │
│    → Always DataFusion (SQL JOINs on Parquet)           │
└─────────────────────────────────────────────────────────┘
```

#### C. Redis resource requirements for hot tier

| Data volume (hot window) | Redis memory | CPU | Notes |
|-------------------------|-------------|-----|-------|
| 1 GB/hour (small) | 4-8 GB | 1 | 4h retention = ~4-32 GB with overhead |
| 10 GB/hour (medium) | 32-64 GB | 2-4 | Dragonfly handles this on a single node |
| 100 GB/hour (large) | Redis Cluster | 8+ | Shard across nodes |

For rollup tables specifically (pre-aggregated metrics):

| Metric series | Rollup granularity | Redis memory | Notes |
|--------------|-------------------|-------------|-------|
| 1M series | 1-min aggregates, 4h retention | ~2 GB | 240 data points × 1M × ~8 bytes |
| 1M series | 1-hour aggregates, 30d retention | ~6 GB | 720 data points × 1M × ~8 bytes |
| 10M series | 1-min aggregates, 4h retention | ~20 GB | Scales linearly |

#### D. Redis vs alternatives for hot tier

| Option | Latency | Memory cost | Ops complexity | Fit |
|--------|---------|------------|----------------|-----|
| **Redis/Dragonfly** | <1ms | Medium (data in RAM) | Low (single process) | **Best for rollups + metric queries** |
| **In-process ring buffer** | <0.1ms | Low (shared memory) | None (same process) | **Best for log/trace tailing (last 15-60 min)** |
| **Local NVMe Parquet cache** | 1-10ms | Low (disk-based) | Low | **Best for recent analytical queries (last 4-24h)** |
| **Memcached** | <1ms | Medium | Low | Simpler but no data structures (no sorted sets, no TS) |
| **ClickHouse** | 10-100ms | High | High | Overkill; adds the complexity you're trying to eliminate |

#### E. Recommended hybrid hot tier

Combine strategies for different query patterns:

| Query type | Hot tier | Retention | Latency |
|-----------|---------|-----------|---------|
| **Metric dashboard** (last 4h) | Redis with pre-computed rollups | 4h raw, 30d rollups | <10ms |
| **Log tail** (last 15 min) | In-process ring buffer | 15-60 min | <1ms |
| **Trace by ID** (recent) | Redis hash (trace_id → spans) | 4h | <1ms |
| **Analytical query** (any range) | DataFusion on Parquet | Unlimited | 500ms-5s |
| **Cross-signal JOIN** | DataFusion on Parquet | Unlimited | 1-5s |

#### F. Cost comparison: Redis hot tier vs Mimir/Loki/Tempo ingesters

| | Mimir+Loki+Tempo ingesters | Redis hot tier + Parquet |
|---|---|---|
| **Memory** | 25 GB (Mimir) + 24 GB (Loki) + 64 GB (Tempo) = **113 GB** | 16-32 GB (Redis) + 4 GB (ring buffer) = **20-36 GB** |
| **CPU** | 10 (Mimir) + 12 (Loki) + 16 (Tempo) = **38 cores** | 2-4 (Redis) + 0 (ring buffer) = **2-4 cores** |
| **Disk** | 330+ GB SSD | ~0 (Redis is in-memory; Parquet goes to S3) |
| **Monthly cloud cost** (estimate) | ~$800-1200/mo (38 CPU + 113 GB RAM + SSD) | ~$200-400/mo (4 CPU + 36 GB RAM) |

**The hot tier with Redis is ~3-4x cheaper than running Mimir+Loki+Tempo ingesters**, while providing equivalent dashboard latency for recent data.

### 17.11 Can DataFusion match a dedicated TSDB for time series?

#### A. The honest answer: it depends on the query type

InfluxDB 3.0 **bet the company** on replacing their custom TSDB with DataFusion + Parquet. The results are instructive:

| Workload | DataFusion (InfluxDB 3.0) | Dedicated TSDB | Winner |
|----------|--------------------------|----------------|--------|
| **Ingestion** (<1M devices) | 320K rows/sec | QuestDB: 11.4M rows/sec | TSDB wins 12-36x |
| **Ingestion** (>1M devices, high cardinality) | Better than InfluxDB 1.x/2.x | Cardinality collapse in traditional TSDBs | **DataFusion wins** |
| **Simple aggregation** (avg over time window) | Competitive (Parquet column scan) | Slightly faster (pre-indexed) | TSDB wins ~2-5x |
| **Complex analytical query** (GROUP BY, JOIN, subquery) | **Native SQL, fast** | Limited or no SQL support | **DataFusion wins** |
| **High-cardinality queries** (user_id, trace_id in metrics) | **No degradation** (columnar scan) | Exponential degradation | **DataFusion wins dramatically** |
| **Compression** | 10-100x on Parquet | 5-15x on custom formats | **DataFusion wins** |
| **Dashboard "last 1h" range query** | 100ms-2s from Parquet | 10-50ms from memory/index | TSDB wins 10-50x |

**Summary**: DataFusion is slower for simple time-range queries on indexed data, but wins on high cardinality, complex analytics, and storage efficiency. The trade-off is **dashboard latency vs analytical power**.

#### B. Why this trade-off is acceptable for your product

1. **The hot tier (Redis) covers dashboard queries**
   - 90%+ of metric dashboard queries hit the last 1-4 hours
   - Redis serves these in <10ms — matching Mimir/Prometheus
   - Only historical/analytical queries go to DataFusion on Parquet

2. **OTLP delta temporality is simpler than Prometheus pull**
   - No counter reset detection needed
   - Delta metrics are just increments — aggregate with SUM
   - This eliminates the most complex TSDB logic

3. **High cardinality is where traditional TSDBs fail**
   - Prometheus/Mimir force teams to drop labels (user_id, trace_id) to prevent cardinality explosion
   - Parquet/DataFusion handles unlimited cardinality without degradation
   - This is a major selling point: "keep all your labels, query everything"

4. **Cross-signal JOINs are impossible with a TSDB**
   - No TSDB can JOIN metrics with logs or traces
   - DataFusion does this natively — the whole point of unified storage

5. **InfluxDB 3.0 validates the approach**
   - They accepted the ingestion/simple-query trade-off
   - In exchange for: SQL support, high cardinality, better compression, Parquet portability
   - GA in April 2025 — production-proven

#### C. What you'd lose vs a dedicated TSDB (and mitigations)

| TSDB feature | Lost? | Mitigation |
|-------------|-------|-----------|
| **Sub-10ms range queries** | Yes (Parquet is 100ms+) | Redis hot tier for recent data |
| **Continuous aggregation / recording rules** | Yes (no built-in scheduler) | Build a simple cron-like scheduler that runs SQL queries periodically and writes results to Redis rollup tables |
| **Automatic downsampling** | Yes | Redis `TS.CREATERULE` for automatic rollups; or Vector transform that pre-aggregates before storage |
| **Native PromQL** | Yes | Grafana plugin that translates common PromQL to SQL; accept subset compatibility |
| **Ingestion throughput >1M rows/sec** | Possibly (DataFusion ingestion is slower than QuestDB) | For observability workloads, this is rarely the bottleneck — network I/O is |
| **`absent()` detection** | Yes (no series registry) | Build a lightweight series registry (just a set of active series names) |

#### D. The key insight: you're not building a TSDB

Your product is not a time-series database. It's an **observability analytics platform**. The difference matters:

| TSDB (Prometheus, InfluxDB) | Observability analytics (your product) |
|----------------------------|---------------------------------------|
| Optimized for time-series queries | Optimized for cross-signal investigation |
| One signal type (metrics) | All signals (logs + metrics + traces) |
| Pre-defined dashboards | Ad-hoc SQL exploration |
| Low cardinality assumed | High cardinality supported |
| Index-heavy, fast point queries | Scan-heavy, powerful aggregations |

DataFusion is not trying to be the best TSDB. It's the best **analytical engine** that also handles time series — and for an observability platform, analytical power (JOINs, arbitrary SQL, high cardinality) matters more than raw TSDB throughput.

Sources:
- [Mimir capacity planning](https://grafana.com/docs/mimir/latest/manage/run-production-environment/planning-capacity/)
- [Mimir resource calculator](https://o11y.tools/mimircalc/)
- [Loki sizing guide](https://grafana.com/docs/loki/latest/setup/size/)
- [Tempo hardware requirements](https://grafana.com/docs/enterprise-traces/latest/setup/hardware-requirements/)
- [DataFusion ClickBench results](https://datafusion.apache.org/blog/2024/11/18/datafusion-fastest-single-node-parquet-clickbench/)
- [Redis TimeSeries](https://redis.io/docs/latest/develop/data-types/timeseries/)
- [Parseable - Parquet for observability](https://www.parseable.com/blog/observability-apache-parquet)
- [Parquet S3 query latency](https://www.alluxio.io/whitepaper/meet-in-the-middle-for-a-1-000x-performance-boost-querying-parquet-files-on-petabyte-scale-data-lakes)
- [QuestDB vs InfluxDB 3 benchmarks](https://questdb.com/blog/influxdb3-core-benchmarks/)
- [InfluxDB 3.0 GA announcement](https://blocksandfiles.com/2025/04/15/influxdata-ingests-and-analyses-time-series-data-faster/)
- [InfluxDB 3 performance comparison](https://tdengine.com/influxdb-3-performance-comparison/)
- [InfluxDB official benchmarks](https://www.influxdata.com/benchmarks/)

---

## 18. Are We Reinventing InfluxDB 3.0? Is the POC Still Relevant?

### 18.1 What InfluxDB 3.0 actually is

InfluxDB 3.0 uses the exact same tech stack: **Rust + DataFusion + Arrow + Parquet**. It claims to store metrics, logs, and traces. So the question is legitimate.

| Capability | InfluxDB 3.0 | Your project |
|-----------|-------------|-------------|
| Storage format | Parquet | Parquet |
| Query engine | DataFusion | DataFusion |
| In-memory format | Arrow | Arrow |
| SQL support | Yes | Yes |
| OTLP ingestion | Yes (via otel2influx converter) | Yes (native, via Vector OTLP source) |
| Language | Rust | Rust |

**At the storage/query layer, it's the same architecture.** No point pretending otherwise.

### 18.2 What InfluxDB 3.0 is NOT (and where you're different)

| Capability | InfluxDB 3.0 | Your project | Why it matters |
|-----------|-------------|-------------|---------------|
| **Pipeline / transforms** | None. It's a database only. Needs external OTel Collector. | Built-in (Vector). VRL/SQL transforms, sampling, routing, PII redaction, enrichment. | "Pipeline-included" is your moat. InfluxDB can't transform data before storing it. |
| **Loki API** (`/loki/api/v1/query`) | No. | Yes (planned). | Grafana can't query InfluxDB as a Loki log datasource. Your product works with Grafana's log panel natively. |
| **Tempo API** (`/api/traces/{id}`) | No. | Yes (planned). | Grafana can't query InfluxDB as a Tempo trace datasource. |
| **Prometheus API** (`/api/v1/query`) | No (has its own InfluxDB plugin). | Yes (planned). | Not a drop-in Prometheus replacement. |
| **Agent mode** | No. | Yes (Vector). | InfluxDB can't run on each host as a sidecar collector. |
| **Gateway mode** | No. | Yes (Vector). | InfluxDB can't aggregate/route from multiple agents. |
| **Signal focus** | **Metrics-first**. Logs and traces are secondary (converted to line protocol). | **All signals equal**. Native OTLP schema for all 3. | InfluxDB's log/trace support is "we can store them" — not "we designed for them". |
| **Cross-signal JOINs** | Technically possible (all in one DB), but not designed for it. | **Core feature**. SQL JOINs across logs+metrics+traces. | InfluxDB's schema (line protocol) makes JOINs awkward. OTLP-native Parquet schema makes them natural. |
| **Alerting** | Basic (Kapacitor, deprecated). | Planned (compound alerts across signals). | InfluxDB alerting is weak and being phased out. |
| **Cost control** | None. | Built-in (pipeline sampling, budget caps). | Pipeline transforms = cost control before storage. |
| **Grafana drop-in** | Partial (InfluxDB datasource only). | Full (Loki + Tempo + Prometheus APIs). | Users must learn InfluxDB query syntax in Grafana. Your users use familiar LogQL/PromQL/TraceQL or SQL. |
| **Data portability / BYOS3** | No (InfluxDB manages its own storage). | Yes (Parquet on customer's S3). | InfluxDB Cloud locks your data. |
| **SaaS model** | InfluxDB Cloud (metrics-focused, proprietary). | Your SaaS (all signals, open backend). | Different market positioning. |
| **License** | Core: MIT/Apache. Enterprise: proprietary. | Pipeline: MPL-2.0 (Vector). Backend: your choice (AGPL recommended). | Similar open-core models. |

### 18.3 The real difference: database vs platform

```
InfluxDB 3.0 is a DATABASE:
  OTel Collector → InfluxDB → Grafana (InfluxDB plugin)
  └── No transforms ──┘  └── InfluxDB-specific queries ──┘

Your project is a PLATFORM:
  OTLP in → Vector (transform/sample/route) → Parquet on S3 → DataFusion → Grafana (Loki/Tempo/Prom APIs)
  └── Pipeline transforms ──────────────────┘  └── Native Grafana compat ──────────────────────┘
  └── Cost control ─────────────────────────┘  └── Cross-signal SQL JOINs ──────────────────────┘
```

InfluxDB 3.0 is one **component** (the database). Your product is the **complete stack** (collector + pipeline + database + query APIs + Grafana compatibility).

### 18.4 Could you just USE InfluxDB 3.0 as your storage layer?

Theoretically yes, but it adds problems:

| Using InfluxDB 3.0 | Using Parquet on S3 directly |
|--------------------|------------------------------|
| Another service to deploy and operate | Files on S3 — zero operational overhead |
| InfluxDB manages storage (vendor dependency) | You own the files (BYOS3) |
| InfluxDB line protocol schema | Native OTLP Parquet schema |
| License: MIT Core, but Enterprise features locked | Full control |
| Adds latency (Vector → network → InfluxDB → disk) | Direct write (Vector → Parquet → S3) |
| Query via InfluxDB's API | Query via DataFusion in your process |

The Parquet-on-S3 approach is **simpler, cheaper, and more portable** than adding InfluxDB as a dependency. You'd inherit InfluxDB's limitations (line protocol schema, no Loki/Tempo API) without gaining much — since DataFusion + Parquet is what InfluxDB uses internally anyway.

### 18.5 What InfluxDB 3.0 VALIDATES for your project

InfluxDB 3.0 is **strong validation**, not competition:

1. **DataFusion + Parquet works for time series** — InfluxDB proved it in production (GA April 2025)
2. **SQL over observability data has demand** — InfluxDB added SQL for this reason
3. **High cardinality is solvable with columnar** — InfluxDB eliminated cardinality limits
4. **The FDAP stack (Flight, DataFusion, Arrow, Parquet) is production-grade** — InfluxDB bet the company on it
5. **A complete rewrite to columnar is worth it** — InfluxDB rewrote from scratch for this

### 18.6 Why the POC is still relevant

| Your unique value | InfluxDB 3.0 equivalent | Gap |
|------------------|------------------------|-----|
| Pipeline-included (transforms before storage) | None — needs external collector | **Wide gap** |
| Grafana-native (Loki/Tempo/Prometheus APIs) | InfluxDB datasource only | **Wide gap** |
| OTLP-native schema (not line protocol) | otel2influx converter (lossy) | **Medium gap** |
| Cross-signal compound alerting | Kapacitor (deprecated) | **Wide gap** |
| BYOS3 (customer owns their data) | InfluxDB manages storage | **Wide gap** |
| Cost control (pipeline sampling + budget caps) | None | **Wide gap** |
| One binary (agent + gateway + backend) | Database only | **Wide gap** |
| "One ring" (one tool for all roles) | One component only | **Fundamental difference** |

**The POC is relevant because you're building something InfluxDB 3.0 is NOT**: an end-to-end observability platform with pipeline integration. InfluxDB is a database. You're building the full stack around a database — and you happen to use the same query engine internally.

The analogy: InfluxDB 3.0 is to your product as **PostgreSQL is to Datadog**. Yes, Datadog probably uses a database internally. That doesn't make PostgreSQL a competitor to Datadog.

### 18.7 Updated competitive positioning

| Competitor | What they are | How you differ |
|-----------|--------------|---------------|
| **Datadog** | Full platform, proprietary, expensive | Open backend, 10x cheaper, pipeline-included |
| **Grafana Cloud** | 3 backends (Mimir+Loki+Tempo), PromQL/LogQL/TraceQL | 1 unified backend, SQL, pipeline-included |
| **SigNoz** | Unified backend (ClickHouse), OTel-native | Parquet on S3 (cheaper), pipeline-included (Vector), BYOS3 |
| **InfluxDB 3.0** | Database (DataFusion+Parquet), metrics-first | Full platform, all signals equal, Grafana-native APIs, pipeline |
| **Parseable** | Log storage (DataFusion+Parquet), logs-only | All signals, pipeline-included, Grafana-native APIs |

Your closest competitor is **SigNoz** (unified, OTel-native), not InfluxDB. The differentiator vs SigNoz is: **Parquet on S3 (cheaper than ClickHouse) + pipeline integration (Vector) + BYOS3**.

Sources:
- [InfluxDB 3 OTLP tutorial](https://www.influxdata.com/blog/opentelemetry-tutorial-collect-traces-logs-metrics-influxdb-3-0-jaeger-grafana/)
- [InfluxDB observability repo](https://github.com/influxdata/influxdb-observability)
- [InfluxDB 3 Core docs](https://docs.influxdata.com/influxdb3/core/)
- [InfluxDB 3 Core - Grafana deep dive](https://grafana.com/blog/influxdb-3-core-a-complete-rewrite-designed-for-speed-and-simplicity/)

---

## 19. Open Data Export — A SaaS Differentiator

### 19.1 The model

The SaaS **owns and manages storage** (simpler operations, standard SaaS model). The differentiator: because data is stored as **open-format Parquet**, customers can **export their data at any time** — trivially, losslessly, and in a format every analytics tool understands.

```
Traditional SaaS (Datadog, Grafana Cloud):
  Vendor stores data in proprietary format.
  Export? Slow, lossy, rate-limited, or impossible.

Your SaaS:
  You store data as Parquet (open columnar format).
  Export? Download Parquet files. Done.
  Query exported data? DuckDB, Athena, Spark — any tool, no conversion.
```

### 19.2 Why this matters

#### A. No data hostage

| Vendor | Export capability | Format | Friction |
|--------|-----------------|--------|----------|
| **Datadog** | Archive to S3 (JSON), limited API export | JSON (lossy, verbose) | Days to export, throttled, expensive at scale |
| **Grafana Cloud** | No bulk export; query API only | Signal-dependent (proprietary chunks) | Must query page-by-page; no bulk download |
| **SigNoz** | ClickHouse native format | ClickHouse-specific | Requires ClickHouse to read |
| **InfluxDB Cloud** | CSV/line protocol export | Line protocol (lossy for logs/traces) | Format conversion needed |
| **Your SaaS** | **Parquet file download / S3 sync** | **Apache Parquet (open, columnar)** | **Zero conversion. Any tool reads it.** |

**The pitch**: "Cancel anytime. Take your data with you. It's standard Parquet — open it with DuckDB on your laptop in 10 seconds."

#### B. Data reuse beyond observability

Exported Parquet files are immediately usable by the customer's existing data tools:

| Tool | Use case |
|------|---------|
| **DuckDB** | Ad-hoc investigation from a laptop (`SELECT * FROM 'exported/*.parquet'`) |
| **AWS Athena** | Serverless SQL on exported files — zero infrastructure |
| **Spark / Databricks** | ML training on telemetry patterns (anomaly detection, capacity forecasting) |
| **dbt** | Transform telemetry into business metrics (SLO reporting, cost attribution) |
| **Jupyter** | Data science notebooks on observability data |
| **BI tools** (Tableau, Looker, Metabase) | Executive dashboards on service health trends |

No other observability SaaS makes this easy. Telemetry becomes a **data asset**, not a cost center.

#### C. GDPR: sensitive data must never reach the backend

GDPR compliance is **not a backend problem — it's a pipeline problem**. Sensitive data (PII, credentials, health data) must be filtered, redacted, or dropped **before** it reaches the SaaS backend. The backend should never store sensitive data in the first place.

This is where the **pipeline-included architecture** is a structural advantage:

```
App (OTLP SDK) → Vector agent (on customer's infra)
                    │
                    ├── VRL/SQL transform: redact PII
                    │   .email = redact(.email)
                    │   del(.user.password)
                    │   .ip = "xxx.xxx.xxx." + split(.ip, ".")[3]
                    │
                    ├── VRL/SQL transform: drop sensitive fields
                    │   if contains(.body, "credit_card") { abort }
                    │
                    └── Forward cleaned data → Your SaaS backend
                        (no sensitive data crosses the network)
```

| GDPR requirement | How the pipeline solves it |
|-----------------|--------------------------|
| **Data minimization** (Art. 5) | VRL/SQL transforms drop fields that aren't needed for observability before sending |
| **Purpose limitation** (Art. 5) | Pipeline filters ensure only operational telemetry reaches the backend — no business data |
| **No PII in storage** | Redaction happens at the agent/gateway level, on the customer's infra, before any data leaves their network |
| **Data portability** (Art. 20) | Export in Parquet — machine-readable, standard, open format |
| **Right to erasure** (Art. 17) | If no PII was stored, there's nothing sensitive to erase |

**Key architectural point**: The Vector agent runs **on the customer's infrastructure**. PII filtering happens there — the data that reaches your SaaS backend is already clean. This means:
- Your SaaS never processes PII (simpler DPA, less liability)
- Customer controls what leaves their network (they can audit the VRL/SQL transforms)
- No need for complex PII scanning in the backend — it was removed at the source

#### D. Disaster recovery / vendor risk

| Scenario | Traditional SaaS | Your SaaS |
|----------|-----------------|-----------|
| Vendor outage | No data access | Export recent data periodically; query offline with DuckDB |
| Vendor shuts down | Data at risk | Already exported Parquet files remain usable forever |
| Vendor raises prices | Locked in (data migration is painful) | Export and migrate to any Parquet-compatible backend |

### 19.3 Export implementation

**Export API** (simple, no vendor lock-in):

```
GET /api/v1/export?signal=logs&from=2026-04-01&to=2026-04-10&format=parquet
  → Returns: download link to Parquet files (or streams them)

GET /api/v1/export?signal=traces&service=api-gateway&format=parquet
  → Returns: filtered Parquet export

POST /api/v1/export/schedule
  { "signal": "all", "frequency": "daily", "destination": "s3://customer-bucket/backup/" }
  → Scheduled export: daily Parquet sync to customer's own S3 (optional add-on)
```

**Export format**: Parquet only. Open, columnar, compressed, queryable by every analytics tool. No need for lossy conversions.

### 19.4 Competitive positioning

**For procurement / finance:**
> "You're never locked in. Your data is stored in Apache Parquet — an open industry standard. Export it anytime, query it with any tool. No proprietary format, no conversion fees."

**For engineering:**
> "Export your last month of traces as Parquet, run `SELECT * FROM 'traces.parquet' WHERE duration_ms > 1000` in DuckDB on your laptop. Try doing that with Datadog."

**For compliance:**
> "PII never reaches our servers. The Vector agent on your infra redacts sensitive data before it leaves your network. Our backend only stores clean operational telemetry. Simpler DPA, less liability, GDPR by design."

### 19.5 Summary

| Feature | Traditional vendors | Your SaaS |
|---------|-------------------|-----------|
| Storage managed by | Vendor | Vendor (same) |
| Internal storage format | Proprietary | **Open (Parquet)** |
| Export | Slow, lossy, rate-limited | **Fast, lossless, open format** |
| Exported data usable by | Vendor's tools only | **Any analytics tool** |
| Vendor lock-in | High (data migration is painful) | **Low (export + switch is trivial)** |
| GDPR data portability | Technically compliant, practically painful | **Genuinely portable** |

---

## 20. Renaming: Trademark and Branding Requirements

### 20.1 Legal obligation

MPL-2.0 Section 2.3 states:

> *"This License does not grant any rights in the trademarks, service marks, or logos of any Contributor."*

**"Vector" is a trademark of Datadog / Timber Technologies.** You cannot use it as the name of your product, SaaS, or company. Renaming is not optional — it is required.

### 20.2 What you MUST do

| Action | Required? | Details |
|--------|----------|---------|
| **Rename the product** | Yes | Choose a new name for the binary, docs, website, and all marketing |
| **Rename the binary** | Yes | `vector` CLI → `your-name` CLI (change `Cargo.toml` `[[bin]]` name) |
| **Remove Vector logos** | Yes | Replace with your own branding |
| **Remove references to "Vector" as product name** | Yes | In docs, CLI help text, error messages, config examples |
| **Keep MPL-2.0 license notices** | Yes | Do NOT remove copyright headers or the LICENSE file from forked code |
| **Keep attribution** | Recommended | A line like "Built on technology originally from the Vector project (MPL-2.0)" is good practice |
| **Remove "Datadog" references** | Yes | Do not imply endorsement by or affiliation with Datadog |

### 20.3 What you CAN do

| Action | Allowed? | Details |
|--------|---------|---------|
| Use any new name you choose | Yes | No restrictions on what you name your fork |
| Create your own logo and branding | Yes | Fully independent identity |
| State it's a fork of Vector | Yes | Factual statements are fine: "forked from the Vector project" |
| Describe compatibility with Vector | Yes | "Compatible with Vector configurations" is a factual claim |
| Use "Vector" in comparative marketing | Yes (carefully) | "Alternative to Vector" or "migrating from Vector" is fair use. Do not imply endorsement. |

### 20.4 What to rename (checklist)

| Location | Current | Change to |
|----------|---------|-----------|
| `Cargo.toml` — `name` / `[[bin]]` | `vector` | `your-product-name` |
| `src/app.rs` — startup banner | "Vector" | Your product name |
| CLI `--version` output | `vector X.Y.Z` | `your-name X.Y.Z` |
| Config file default name | `vector.toml` / `vector.yaml` | `your-name.toml` / `your-name.yaml` |
| Systemd service name | `vector.service` | `your-name.service` |
| Docker image name | `timberio/vector` | `your-org/your-name` |
| Documentation / website | vector.dev references | Your own domain |
| Error messages mentioning "Vector" | Throughout codebase | Your product name |
| Environment variable prefix | `VECTOR_*` | `YOUR_NAME_*` |
| Internal metrics prefix | `vector_*` | `your_name_*` |

### 20.5 Precedent: successful open-source renames

| Original | Fork | License | Outcome |
|----------|------|---------|---------|
| OpenOffice (Oracle) | **LibreOffice** | LGPL | Became the dominant fork |
| MySQL (Oracle) | **MariaDB** | GPL | Drop-in replacement, independent brand |
| Elasticsearch (Elastic) | **OpenSearch** (AWS) | Apache-2.0 | AWS built a full ecosystem around the fork |
| Terraform (HashiCorp) | **OpenTofu** | MPL-2.0 → OpenTofu (same license situation as you) | Community-driven fork after BSL relicense |
| Redis (Redis Ltd) | **Valkey** (Linux Foundation) | BSD | Fork after license change |

**OpenTofu is the closest precedent**: it's an MPL-2.0 project that was forked and renamed. The community successfully built an independent brand and ecosystem. Your situation is analogous.

### 20.6 Naming considerations for your product

When choosing a name, consider:

| Criterion | Why it matters |
|-----------|---------------|
| **Not generic** | "observability" or "telemetry" are too broad; hard to trademark |
| **Distinct from "Vector"** | Avoid confusion; establish independent identity |
| **Evokes the value proposition** | Pipeline + analytics + unified signals |
| **Available as domain + GitHub org** | Check before committing |
| **Short, memorable** | CLI users type it many times a day |
| **No existing trademark conflicts** | Search USPTO / EUIPO before committing |

### 20.7 Chosen name: **Sol** — Single Observability Layer

```
 ███████╗  ██████╗  ██╗
 ██╔════╝ ██╔═══██╗ ██║
 ███████╗ ██║   ██║ ██║
 ╚════██║ ██║   ██║ ██║
 ███████║ ╚██████╔╝ ███████╗
 ╚══════╝  ╚═════╝  ╚══════╝
 Single Observability Layer
```

**Sol** = **S**ingle **O**bservability **L**ayer

| Aspect | Fit |
|--------|-----|
| **Meaning** | "Sun" (Latin) — one source of light that illuminates everything |
| **Backronym** | Single Observability Layer — one tool, one backend, one layer for all signals |
| **Simplicity** | 3 letters, fast to type, easy to remember |
| **CLI** | `sol --mode all`, `sol start`, `sol query "SELECT ..."` |
| **Philosophy** | One binary. One deployment. One query language. All signals. |
| **SQL echo** | Sol ≈ SQL — the product's query language. Subtle, memorable. |
| **Distinct from Vector** | Completely different name, no confusion |

**Before committing**: verify availability of:
- [ ] Domain: `sol.dev`, `getsol.dev`, `sol-observability.dev`, or similar
- [ ] GitHub org: `github.com/sol-observability` or similar
- [ ] Crate name: `sol` on crates.io
- [ ] Trademark: search USPTO (US) and EUIPO (EU) for "Sol" in software/SaaS class

### 20.8 Timeline

Renaming can happen at any time, but earlier is better:
- **Before any public release or marketing**: mandatory — launching under "Vector" would be trademark infringement
- **During development**: optional — you can develop internally as "vector-fork" and rename before release
- **The rename is a mechanical refactoring**: find-and-replace across the codebase, update Cargo.toml, regenerate docs. Not technically difficult, just thorough.
