# Datasource conformance pass — Sol vs real Mimir/Tempo/Loki

Method: the demo runs **real** Mimir/Tempo/Loki beside Sol. For each endpoint the
same request was issued to both and the JSON **structurally diffed** (keys,
nesting, value types, status envelope — not data values). The real backend's
response is the authoritative contract Grafana accepts (Mimir = Prometheus HTTP
API; Tempo cross-checked against `tempopb/tempo.proto`; Loki against live Loki).

> **Caveat — tested against the deployed image `sol:81aff1e03`, which predates
> the latest commits.** Two HIGH findings are **already fixed at HEAD**, pending
> a rebuild/redeploy:
> - Tempo search missing `spanSets` (plural) → fixed `11c64a8c6`.
> - Loki log-volume metric query 400 (`query_range` now returns a `matrix`) →
>   fixed `11c64a8c6`.
> Re-run this pass after deploying HEAD to clear those.

Status: ⬜ open · ✅ fixed-at-HEAD (redeploy) · 🟡 conforms-with-note.

## Prometheus / Mimir (`/prometheus/*`)

| Endpoint | Verdict | Sev | Finding / fix |
|---|---|---|---|
| `query` (instant) | ⬜ | HIGH | **C-P1** `__name__` is the raw dotted OTLP name (`http.server.request.duration`), not the normalized `http_server_request_duration_seconds_count`; and the `attributes` JSON blob is dropped (`NON_LABEL`) so per-attribute labels (`http_route`, `http_response_status_code`, …) are **not** exploded → bare selectors collapse distinct series. (Dashboards use `sum by(…)` so are unaffected; Explore/metric-browser diverge.) Fix: emit `prom_metric_name(...)` for `__name__` and explode `attributes` into normalized labels. `src/query/prometheus.rs` (vector/series label projection ~L277). |
| `series` | ⬜ | HIGH | **C-P2** `match[]` is ignored — returns the whole series catalogue regardless of the selector; plus the same dotted-name / dropped-label issue as C-P1. Fix: apply the `match[]` matcher in `series_sql`; normalize names + labels. |
| `query_range` | ⬜ | MED | **C-P3** samples are raw point timestamps, not resampled to the `step` grid (Mimir returns one step-aligned point per bucket). Grafana mostly tolerates it; can distort graphs / shared tooltips. Fix: bucket/resample to `step`. |
| `metadata` | ⬜ | MED | **C-P4** Sol 404s `/api/v1/metadata`; Mimir returns `{status,data:{<metric>:[{type,help,unit}]}}`. Grafana metric browser loses type/unit hints (degrades gracefully). Fix: implement a minimal `metadata`. |
| `labels`, `label/__name__/values` | 🟡 | LOW | Conform. Note: `__name__/values` returns **normalized** names while `query`/`series` return **dotted** — internal inconsistency (fold into C-P1). |
| ts encoding | ⬜ | LOW | **C-P5** sample ts emitted as float (`1780498584.0`) vs Mimir integer seconds. Spec is a number; Grafana tolerates. |

## Tempo (`/tempo/*`)

| Endpoint | Verdict | Sev | Finding / fix |
|---|---|---|---|
| `api/search` `spanSets` | ✅ | HIGH | Fixed at HEAD (`11c64a8c6`) — plural `spanSets` added. Redeploy. |
| `api/v2/traces/:id` (id format) | ⬜ | HIGH | **C-T1** Tempo strips leading zeros from trace IDs (returns e.g. a 31-char id); Sol rejects odd-length with 400, so Grafana's "open trace from search" link breaks for any id with a leading zero. Fix: left-pad the path id to 32 hex before decoding instead of rejecting. `src/query/tempo.rs trace_by_id_sql`. |
| `api/v2/traces/:id` (span JSON) | ⬜ | HIGH | **C-T2** trace-by-id spans aren't OTLP proto-JSON: `attributes` is a flat object (not `[{key,value:{stringValue\|intValue}}]`), `traceId`/`spanId` are hex (not base64), and `kind`/`status`/`scope.name` are missing → Grafana's trace waterfall can't deserialize. Fix: serialize spans as OTLP proto-JSON (KeyValue-array attrs, base64 ids, kind/status/scope). |
| `api/search` `serviceStats` | ⬜ | MED | **C-T3** real Tempo includes `serviceStats{spanCount,errorCount}` per service (drives Search row counts); Sol omits. |
| `api/v2/search/tags` | 🟡 | LOW | **C-T4** Sol omits the `event` scope and the top-level `metrics` object (cosmetic; autocomplete of event-scoped tags absent). |
| `api/v2/search/tag/:t/values`, `api/search/tags`, `api/echo` | 🟡 | — | Conform (Sol is more lenient on bare `service.name` than Tempo). |

## Loki (`/loki/*`)

| Endpoint | Verdict | Sev | Finding / fix |
|---|---|---|---|
| `query_range` (volume metric) | ✅ | HIGH | Fixed at HEAD (`11c64a8c6`) — metric LogQL → `matrix`. Redeploy. (The demo's Grafana sends the volume query to `query_range`, confirmed by the original "must start with `{...}`" error — not `index/volume`.) |
| `index/volume[_range]` | ⬜ | MED | **C-L1** Sol 404s; real Loki serves them. *Newer* Grafana Loki datasources may call `index/volume` for the volume panel; the demo's version uses `query_range` (now handled), so this is a forward-compat gap, not a current break. Fix: implement `index/volume` returning a vector of byte volumes. |
| `series` | ⬜ | MED | **C-L2** Sol 404s `/loki/api/v1/series`; Grafana uses it for label/series browsing. Fix: implement returning `{status,data:[{labelset}]}`. |
| `index/stats` | ⬜ | LOW | **C-L3** Sol 404s; Loki returns a flat `{streams,chunks,bytes,entries}` (NOT `{status,data}`-wrapped) for the Explore query-size hint. |
| `query_range` (streams) | 🟡 | LOW | Conforms. Note: Sol's `stream` carries only `{detected_level,service_name}`; real Loki promotes the full OTLP attribute set (`trace_id`, `severity_text`, `host_name`, …) and includes `data.stats`. Optional: promote more labels + add `stats`. |
| `labels`, `label/:n/values` | 🟡 | — | Conform. |

## Priorities

After **redeploying HEAD** (clears the two ✅), the remaining real gaps, worst first:
1. **C-T2** trace-by-id OTLP span JSON (breaks the trace waterfall view).
2. **C-T1** trace-by-id zero-padded ids (breaks open-trace-from-search).
3. **C-P1 / C-P2** Prometheus dotted `__name__` + unexploded attribute labels + `series` ignoring `match[]` (breaks Explore/metric-browser; dashboards OK).
4. **C-P3** step resampling · **C-T3** serviceStats · **C-L2** Loki series · **C-P4** metadata.
5. Low: C-P5, C-T4, C-L1, C-L3, stream-label richness.

Common root causes worth fixing centrally: (a) one place that maps a metric to its Prometheus `__name__` + explodes `attributes` into labels (fixes C-P1/C-P2 and the C-P "labels" inconsistency); (b) an OTLP proto-JSON span serializer shared by trace-by-id (C-T2).
