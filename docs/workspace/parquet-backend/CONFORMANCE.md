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
| `query` (instant) | ✅ | HIGH | **C-P1** FIXED — a shared `LabelCols` helper now maps `prom_name` → normalized `__name__` and explodes the `attributes` JSON into normalized per-attribute labels, in both the instant and range builders. Bare selectors no longer collapse distinct series. (Grouped `sum by(…)` queries carry no `attributes`/`prom_name` column, so they're unchanged.) |
| `series` | ✅ | HIGH | **C-P2** FIXED — `series_sql(match[])` now applies the selector's name+label predicates and emits the normalized `__name__`. (`series` was the C-P1 name/label fix's clean, window-free home.) |
| binary / unary ops | ✅ | HIGH | **C-Pbin** FIXED — PromQL binary (`a/b`, `1-x`, `x>0`, `atan2`, comparisons w/ `bool`) and unary minus now evaluate via Rust-side vector matching (on/ignoring, group_left/right; scalar∘vector and one/many-to-one), for both instant and range. Unblocks Node Exporter ratio panels that previously errored ("binary operators not yet supported"). |
| `query_range` (step align) | ✅ | MED | **C-P3** FIXED — range series are resampled onto the `step` grid (one point per bucket, last-value-carry-forward within a 5-min staleness window), like Mimir. Guarded against pathological tiny steps (≤100k grid points). `step=0` keeps raw sample timestamps. |
| `metadata` | ✅ | MED | **C-P4** FIXED — `/api/v1/metadata` returns `{status:"success",data:{}}` (valid empty metadata; no more 404). Per-metric type/unit population is a future enhancement. |
| `labels`, `label/__name__/values` | 🟡 | LOW | Conform. Note: `__name__/values` returns **normalized** names while `query`/`series` return **dotted** — internal inconsistency (fold into C-P1). |
| ts encoding | ✅ | LOW | **C-P5** FIXED — sample timestamps now serialize as integer seconds when whole (`1780498584`), matching Mimir; fractional seconds still emit a float. |

## Tempo (`/tempo/*`)

| Endpoint | Verdict | Sev | Finding / fix |
|---|---|---|---|
| `api/search` `spanSets` | ✅ | HIGH | Fixed at HEAD (`11c64a8c6`) — plural `spanSets` added. Redeploy. |
| `api/v2/traces/:id` (id format) | ✅ | HIGH | **C-T1** FIXED — `trace_by_id_sql` zero-pads the id to 32 hex (Tempo strips leading zeros) instead of rejecting odd-length. |
| `api/v2/traces/:id` (span JSON) | ✅ | HIGH | **C-T2** FIXED — trace-by-id spans now OTLP proto-JSON: base64 `traceId`/`spanId`/`parentSpanId`, KeyValue-array `attributes` (span + resource), `kind`/`status.code` enum strings, `scope.name`. |
| `api/search` `serviceStats` | ✅ | MED | **C-T3** FIXED — search hits now carry `serviceStats{spanCount,errorCount}` per service (Grafana 13's results table reads `trace.serviceStats`). Also fixed: `limit` is now applied to the trace count (Sol returned every matched trace, e.g. 341 for `limit=20`). |
| `api/v2/search/tags` | ✅ | LOW | **C-T4** FIXED — tags response now carries an (empty) `event` scope and a top-level `metrics` object, matching Tempo's shape. (Sol stores span events in the `events` JSON column, not a separately-indexed scope, so `event` tags are empty.) |
| `api/v2/search/tag/:t/values`, `api/search/tags`, `api/echo` | 🟡 | — | Conform (Sol is more lenient on bare `service.name` than Tempo). |

## Loki (`/loki/*`)

| Endpoint | Verdict | Sev | Finding / fix |
|---|---|---|---|
| `query_range` (volume metric) | ✅ | HIGH | Fixed at HEAD (`11c64a8c6`) — metric LogQL → `matrix`. Redeploy. (The demo's Grafana sends the volume query to `query_range`, confirmed by the original "must start with `{...}`" error — not `index/volume`.) |
| `index/volume[_range]` | ✅ | MED | **C-L1** FIXED — `index/volume` returns a `vector` of per-`service_name` byte volumes (octet length of `body`); `index/volume_range` returns a `matrix` bucketed by `step`. |
| `series` | ✅ | MED | **C-L2** FIXED — `/loki/api/v1/series` returns `{status,data:[{labelset}]}` (distinct `service_name` + exploded, normalized resource attributes), honoring `match[]`. |
| `index/stats` | ✅ | LOW | **C-L3** FIXED — `/loki/api/v1/index/stats` returns the flat `{streams,chunks,bytes,entries}` hint (NOT `{status,data}`-wrapped); `chunks` is 0 (Sol has no chunk concept). |
| `query_range` (streams) | 🟡 | LOW | Conforms. Note: Sol's `stream` carries only `{detected_level,service_name}`; real Loki promotes the full OTLP attribute set (`trace_id`, `severity_text`, `host_name`, …) and includes `data.stats`. Optional: promote more labels + add `stats`. |
| `labels`, `label/:n/values` | 🟡 | — | Conform. |

## Priorities

All catalogued findings are now ✅ fixed at HEAD (pending a redeploy to verify
live). Remaining work is not a conformance gap but depth:
- **PromQL coverage** — `sum(a/b)` (aggregation over a binary expression),
  `without(...)` aggregation, and set operators (`and`/`or`/`unless`) are still
  unsupported (clear errors, no panic). The common ratio shape `sum(a)/sum(b)`
  works. C-P3 resampling uses last-value-carry-forward (a close approximation of
  Prometheus per-step evaluation), not a true per-step re-evaluation.
- **Stream-label richness** — Loki `streams` still carries only
  `{detected_level, service_name}`; promoting the full OTLP attribute set + a
  `data.stats` block remains optional.
- **Per-metric metadata** — `/api/v1/metadata` is a valid empty object; per-metric
  type/unit population is a future enhancement.

Common root causes already fixed centrally: (a) `LabelCols` maps a metric to its
Prometheus `__name__` + explodes `attributes` into labels (C-P1/C-P2); (b) a
shared OTLP proto-JSON span serializer for trace-by-id (C-T2); (c) a Rust-side
vector-matching evaluator for binary/unary operators (C-Pbin).
