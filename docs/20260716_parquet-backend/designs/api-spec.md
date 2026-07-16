# API Specification — Grafana-compatible HTTP contracts

> The wire contract for [FR1](./DESIGN.md#fr1) (Prometheus), [FR2](./DESIGN.md#fr2) (Tempo), [FR3](./DESIGN.md#fr3) (Loki), enforcing [NFR2](./DESIGN.md#nfr2) (standard Grafana data sources work unchanged). [QUERY-MAPPING.md](./query-mapping.md) covers *query → SQL*; this covers *HTTP request params → response JSON*.
> Response examples marked **(pcap)** are real bodies extracted from `883cbd10a4e1.pcap` (Grafana ↔ Mimir/Tempo/Loki); others are from the upstream API specs. Endpoints marked **[pcap]** were observed in the capture.

Conventions: all responses `Content-Type: application/json`. Errors return the backend's native error envelope (below). Guardrail breaches ([NFR9](./DESIGN.md#nfr9)) use the same envelope with HTTP 422.

### Coverage policy (full surface, with trade-off decisions)

We target the **full read/query surface** of each backend's HTTP API ([Prometheus](https://prometheus.io/docs/prometheus/latest/querying/api/), [Loki](https://grafana.com/docs/loki/latest/reference/loki-http-api/), [Tempo](https://grafana.com/docs/tempo/latest/api_docs/)), with a per-endpoint decision — same discipline as [QUERY-MAPPING.md](./query-mapping.md):

| Mark | Meaning |
|---|---|
| ✅ | implement in v1 (response builder + tests) |
| ✅ stub | minimal 200 so Grafana "Save & Test" / capability probes pass |
| ⚠️ | supported but deferred / partial (Explore extras, heavy features) — return a valid empty-ish body or implement when needed |
| ⛔ | out of scope — **ingestion** (handled by Sol's existing sources, not this read backend), **admin/ring/config** (not part of the data-source contract), or **hot-data** ([non-goal](./DESIGN.md#non-goals)) |

Each backend section below first details the **core query endpoints** (with real pcap bodies), then a **full-surface table** marking every remaining endpoint.

---

## 1. Prometheus API (Mimir-compatible) — [FR1](./DESIGN.md#fr1)

Mounted under `/prometheus`. Timestamps in responses are `[<unix_seconds: number>, "<value: string>"]`.

### `GET|POST /prometheus/api/v1/query` — instant **[pcap]**
Params: `query` (PromQL), `time` (rfc3339|unix, optional), `timeout`.
Response (`resultType: vector` | `scalar` | `string` | `matrix`):
```json
{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1779817108.845,"12"]}]}}
```
**(pcap)** — `"resultType":"vector","result":[{"metric":{},"value":[1779817108.845,"12"]}]`. `metric` is the label set; `value` is `[ts, "stringified float"]`.

### `GET|POST /prometheus/api/v1/query_range` — range **[pcap]**
Params: `query`, `start`, `end` (unix|rfc3339), `step` (duration|float secs), `timeout`.
Response (`resultType: matrix`):
```json
{"status":"success","data":{"resultType":"matrix","result":[
  {"metric":{"http_response_status_code":"200"},
   "values":[[1779817095,"2.203667557932264"],[1779817110,"3.561806236873278"]]}
]}}
```
**(pcap)** — exact shape above. Each series: `metric` label set + `values` array of `[ts, "value"]`.

### `GET /prometheus/api/v1/label/{name}/values` — **[pcap]**
Params: `match[]` (optional series selectors), `start`, `end`.
```json
{"status":"success","data":["client","service"]}
```
**(pcap)**.

### `GET /prometheus/api/v1/labels`
Same envelope as above; `data` = array of label names.

### `GET /prometheus/api/v1/series` — **[pcap]**
Params: `match[]` (≥1 selector), `start`, `end`. → `data` = array of label-set objects:
```json
{"status":"success","data":[{"__name__":"up","job":"sol"}]}
```

### Error envelope
```json
{"status":"error","errorType":"bad_data|timeout|execution|unavailable","error":"<message>"}
```

### Full surface & support (Prometheus/Mimir)
| Endpoint | Support | Note |
|---|---|---|
| `GET\|POST /api/v1/query`, `/query_range` | ✅ | core (above) |
| `GET /api/v1/series` | ✅ | `match[]` selectors |
| `GET /api/v1/labels`, `/label/{name}/values` | ✅ | label discovery |
| `GET /api/v1/query_exemplars` | ⚠️ | exemplars exist as a JSON column; implement if a panel needs it |
| `GET /api/v1/metadata` | ⚠️ | synthesise from metric `name`/`description`/`unit` columns |
| `GET /api/v1/format_query` | ⚠️ | round-trip via `promql-parser`; web-UI only |
| `GET /api/v1/status/buildinfo` | ✅ stub | Grafana data-source probe |
| `GET /api/v1/cardinality/{label_names,label_values,active_series}` (Mimir) | ⚠️ | `SELECT DISTINCT`/counts; for the cardinality panels |
| `GET /api/v1/status/{config,flags,runtimeinfo,tsdb}` | ⛔ | admin/introspection, not a data-source contract |
| `POST /api/v1/read` (remote read) | ⛔ | Grafana uses the query API, not remote-read |
| `POST /api/v1/write`, `/otlp/v1/metrics` | ⛔ | **ingestion** — Sol sources, not this backend |
| `/api/v1/{targets,rules,alerts,alertmanagers,metadata/targets}` | ⛔ | scraper/Prometheus-server concepts; N/A over Parquet |

---

## 2. Loki API — [FR3](./DESIGN.md#fr3)

Mounted under `/loki`. Log timestamps are **nanosecond strings**.

### `GET /loki/api/v1/query_range` — **[pcap]**
Params: `query` (LogQL), `start`, `end` (ns), `limit`, `direction` (`forward|backward`), `step`/`interval` (for metric queries), `since`.
Log response (`resultType: streams`):
```json
{"status":"success","data":{"resultType":"streams","result":[
  {"stream":{"service_name":"client","service_version":"1.0.0"},
   "values":[["1779817095000000000","log line text"]]}
],"stats":{}}}
```
**(pcap)** confirmed markers: `"resultType":"streams"`, `"stream":{…}`, `"values":[["<ns>","<line>"]]`.
Metric LogQL (e.g. `count_over_time`) returns `resultType: matrix` with the Prometheus matrix shape (values `[ts,"v"]`).

### `GET /loki/api/v1/query` — instant LogQL (vector/streams).
### `GET /loki/api/v1/labels` and `/loki/api/v1/label/{name}/values`
```json
{"status":"success","data":["service_name","service_version"]}
```
### `GET /loki/api/v1/series`
Params `match[]`, `start`, `end` → `data` = array of label-set objects.
### `GET /loki/api/v1/index/stats`
`{"streams":N,"chunks":N,"entries":N,"bytes":N}` (Grafana volume/cost panels).
### `GET /loki/api/v1/tail` — WebSocket live tail — **⛔ not supported** ([non-goal](./DESIGN.md#non-goals); hot data).

Error envelope: same `{"status":"error",...}` shape; plain-text errors also accepted by Grafana.

### Full surface & support (Loki)
| Endpoint | Support | Note |
|---|---|---|
| `GET /loki/api/v1/query_range`, `/query` | ✅ | core (above); log + metric LogQL |
| `GET /loki/api/v1/labels`, `/label/{name}/values`, `/series` | ✅ | discovery |
| `GET /loki/api/v1/index/stats` | ⚠️ | `streams/chunks/entries/bytes` counts — Grafana query-size hint |
| `GET /loki/api/v1/index/volume`, `/volume_range` | ⚠️ | volume-by-label; the Logs volume panel |
| `GET /loki/api/v1/patterns` | ⚠️ | log pattern mining — defer (heavy; not dashboard-critical) |
| `GET /loki/api/v1/detected_fields`, `/detected_labels` | ⚠️ | Explore enrichment — defer/optional |
| `GET /loki/api/v1/format_query` | ⚠️ | LogQL pretty-print; UI only |
| `GET /loki/api/v1/status/buildinfo`, `/ready` | ✅ stub | data-source probe |
| `GET /loki/api/v1/tail` | ⛔ | WebSocket live tail — hot data ([non-goal](./DESIGN.md#non-goals)) |
| `POST /loki/api/v1/push`, `/otlp/v1/logs` | ⛔ | **ingestion** — Sol sources, not this backend |
| `/loki/api/v1/delete` (log deletion) | ⛔ | admin/retention handled by the compactor GC, not an API |

---

## 3. Tempo API — [FR2](./DESIGN.md#fr2)

Mounted under `/api`. Times in nanoseconds (`startTimeUnixNano` strings); `durationMs` numeric.

### `GET /api/search` — TraceQL / tag search — **[pcap]**
Params: `q` (TraceQL) **or** `tags` (logfmt), `start`, `end` (unix secs), `limit`, `spss` (spans-per-spanset), `minDuration`, `maxDuration`.
Response:
```json
{"traces":[
  {"traceID":"3bc59070ba6c121cad3d88a3f889b303",
   "rootServiceName":"client","rootTraceName":"GET /randomuser",
   "startTimeUnixNano":"1779817095000000000","durationMs":42,
   "spanSet":{"spans":[{"spanID":"...","startTimeUnixNano":"...","durationNanos":"...","attributes":[...]}],"matched":1}}
],
"metrics":{"inspectedTraces":120,"inspectedBytes":"170432","completedJobs":1,"totalJobs":1}}
```
`metrics.inspectedBytes` confirmed **(pcap)**.

### `GET /api/v2/traces/{traceID}` — trace by ID — **[pcap]**
Optional `start`,`end`. Returns the trace in **OTLP JSON**:
```json
{"trace":{"resourceSpans":[{"resource":{"attributes":[...]},"scopeSpans":[{"scope":{...},"spans":[{"traceId":"...","spanId":"...","name":"...","startTimeUnixNano":"...","endTimeUnixNano":"...","attributes":[...],"status":{...}}]}]}]}}
```
(v1 `/api/traces/{id}` returns the bare OTLP `{"batches":[...]}` / `{"resourceSpans":[...]}`; v2 wraps it under `trace`.)

### `GET /api/v2/search/tags` — tag names — **[pcap]**
Optional `scope` (`resource|span|intrinsic`), `q`. Response (v2, scoped):
```json
{"scopes":[
  {"name":"resource","tags":["service.name","service.namespace"]},
  {"name":"span","tags":["http.method","http.response.status_code"]},
  {"name":"intrinsic","tags":["name","status","kind","duration"]}
]}
```
(v1 `/api/search/tags` → `{"tagNames":[...]}`.)

### `GET /api/v2/search/tag/{tag}/values` — tag values — **[pcap]**
Optional `q` (TraceQL filter). Response — values are **typed**:
```json
{"tagValues":[{"type":"keyword","value":"ok"},{"type":"keyword","value":"error"},{"type":"string","value":"GET"}],
 "metrics":{"inspectedBytes":"170432"}}
```
**(pcap)** — exact `tagValues` typed-pairs shape confirmed (types seen: `keyword`, `string`). Grafana also calls this via `GET /api/datasources/uid/tempo/resources/tag-values/...` (observed) which proxies to this endpoint.

### `GET /api/echo` — health → `echo` (200).

### TraceQL metrics — `GET /api/metrics/query_range` (+ `/api/metrics/query`)
Returns Prometheus-like time series from TraceQL metrics functions (`rate`, `count_over_time`, `quantile_over_time`, `histogram_over_time`, `compare`, …) over spans ([TraceQL metrics docs](https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-queries/)). **⚠️ deferred** — it's "PromQL over the traces table"; maps to the same window/aggregate SQL as [FR1](./DESIGN.md#fr1) but is a sizeable feature on its own and was not in the pcap. Response is the Prometheus matrix shape (§1).

### Full surface & support (Tempo)
| Endpoint | Support | Note |
|---|---|---|
| `GET /api/search` | ✅ | TraceQL / `tags` search (above) |
| `GET /api/v2/search/tags`, `/api/search/tags` | ✅ | v2 scoped + v1 flat |
| `GET /api/v2/search/tag/{tag}/values`, v1 | ✅ | typed `tagValues` (above) |
| `GET /api/v2/traces/{id}`, `/api/traces/{id}` | ✅ | OTLP-JSON trace (above) |
| `GET /api/echo` | ✅ stub | health / data-source probe |
| `GET /api/status/buildinfo` | ✅ stub | version probe |
| `GET /api/metrics/query_range`, `/api/metrics/query` | ⚠️ | TraceQL metrics (above) — deferred |
| `GET /api/v2/search/tags?scope=…`, `/api/overrides` | ⚠️/⛔ | scope filter ⚠️; overrides/limits admin ⛔ |
| `POST /otlp/v1/traces`, `/api/push` | ⛔ | **ingestion** — Sol sources, not this backend |
| `/flush`, `/shutdown`, `/ingester/*`, `/compactor/*`, ring/admin | ⛔ | operational endpoints, not the data-source contract |

Error: Tempo returns plain-text body + non-200 status (e.g. 400/404/500); 404 for unknown trace id.

---

## 4. Cross-cutting contract notes

- **Grafana discovery probes**: the data sources issue capability/health probes (`/api/v1/status/buildinfo` for Prometheus, `/api/echo` for Tempo, `/loki/api/v1/status/buildinfo`/`/ready`). Implement minimal 200 responses so the data source "Save & Test" passes.
- **Time units differ per backend**: Prometheus `[unix_seconds, "val"]` (seconds, float allowed); Loki `["<ns string>", line]`; Tempo `startTimeUnixNano` strings + `durationMs`. Response builders must convert from the Parquet `time_unix_nano` (INT64 ns) accordingly.
- **Compression**: Grafana sends `Accept-Encoding: gzip`; large trace/search bodies are gzipped on the wire (why some pcap bodies weren't plain-text). Support gzip response encoding.
- **Content negotiation**: all three accept `application/json`; Prometheus also `application/x-www-form-urlencoded` for POST query bodies.
- These response shapes are the acceptance target for the response-builder tasks (3, 4, 5, 7) and the [NFR2](./DESIGN.md#nfr2) quality gate.
