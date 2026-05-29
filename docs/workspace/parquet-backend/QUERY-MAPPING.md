# Query Mapping — PromQL / LogQL / TraceQL → DataFusion SQL

> Gate: completed with [COMPLEXITY.md](./COMPLEXITY.md) **before** implementation ([TASKS.md](./TASKS.md) Phase 4a gate).
> Design: [DESIGN.md](./DESIGN.md). Methodology per the user directive: **translate to SQL, do not embed an existing engine**; cover the full language surface but make a **trade-off decision per construct** rather than supporting everything blindly.

## Legend (the trade-off decision per construct)

| Mark | Meaning |
|---|---|
| ✅ **native** | Direct, cheap SQL translation; supported in v1 |
| ⚠️ **cost-flagged** | Supported, but cost-bounded by guardrails ([NFR9](./DESIGN.md#nfr9)) and/or served via rollups ([FR6](./DESIGN.md#fr6)) |
| ⛔ **restricted** | Not in v1 — conflicts with an NFR (hot data, unbounded eval) or a [rabbit hole](./DESIGN.md#rabbit-holes). Returns a clear "unsupported" error; reachable via the [SQL endpoint (FR9)](./DESIGN.md#fr9) as the escape hatch |

## Shared SQL substrate (used by all three languages)

| Concept | SQL idiom |
|---|---|
| Time window | `time_unix_nano BETWEEN @start AND @end` (+ `dt=` partition prune) |
| Promoted label (`service_name`, `name`, `severity_*`, status, kind…) | top-level column `WHERE col = 'v'` — **predicate pushdown + bloom** |
| Arbitrary label/attribute | `json_extract(attributes, '$.k')` (also `resource_attributes`, `scope_attributes`) — **no pushdown** ([rabbit hole 4](./DESIGN.md#rabbit-holes)) |
| `=` / `!=` | `=` / `<>` |
| `=~` / `!~` (regex) | `regexp_like(col, 're')` / `NOT regexp_like(...)` |
| Group `by (l)` / `without (l)` | `GROUP BY <cols / json_extract>` |
| `trace_id` literal | `trace_id = X'<hex>'` (fixed-len binary, **bloom-accelerated**) |

> JSON extraction uses the `datafusion-functions-json` UDFs (added under the `query-backend` feature). Attribute filters do not push down — accepted v1 ([rabbit hole 4](./DESIGN.md#rabbit-holes)); hot-attribute promotion is a future optimisation.

---

## 1. LogQL → SQL (logs — highest priority, [FR3](./DESIGN.md#fr3))

Target table: `logs`. Query interval ≤ 30 d ([NFR7](./DESIGN.md#nfr7)).

### 1.1 Log stream selector + pipeline (log queries)

| LogQL | SQL | Decision |
|---|---|---|
| `{service_name="x"}` | `WHERE service_name='x'` | ✅ (bloom) |
| `{k="v"}` non-promoted | `WHERE json_extract(resource_attributes,'$.k')='v'` | ⚠️ no pushdown |
| `{k=~"re"}` | `regexp_like(…, 're')` | ⚠️ |
| `\|= "t"` (line contains) | `body LIKE '%t%'` | ✅ (pushed before regex) |
| `!= "t"` | `body NOT LIKE '%t%'` | ✅ |
| `\|~ "re"` / `!~ "re"` | `regexp_like(body,'re')` / `NOT …` | ⚠️ full `body` scan — cost-flagged |
| `\| json` (parse body as JSON) | `json_extract(body,'$.field')` per referenced field | ⚠️ per-row parse; only extract referenced fields |
| `\| logfmt` | logfmt UDF / regex extraction per field | ⚠️ per-row parse |
| `\| pattern "<_> <ip> <_>"` | regex capture → columns | ⚠️ |
| `\| regexp "(?P<x>…)"` | `regexp_match` capture | ⚠️ |
| `\| label_format` / `\| line_format` | projection / `format_string` over columns | ✅ |
| `\| <label> = "v"` (post-parse filter) | `WHERE` on extracted expr | ✅/⚠️ (depends on source) |
| `\| drop`, `\| keep` | column projection | ✅ |
| `limit` / `direction=backward` | `ORDER BY time_unix_nano DESC LIMIT n` | ✅ |
| `/loki/api/v1/tail` (live) | — | ⛔ hot data ([non-goal](./DESIGN.md#non-goals)) |

### 1.2 Metric queries over logs (range aggregations)

| LogQL | SQL | Decision |
|---|---|---|
| `count_over_time({…}[5m])` | `COUNT(*)` `GROUP BY` time-bucket(`step`) | ✅ |
| `rate({…}[5m])` | `COUNT(*) / range_seconds` per bucket | ✅ |
| `bytes_over_time` / `bytes_rate` | `SUM(length(body))` per bucket | ✅ |
| `sum/avg/max/min by (l) (…)` | aggregate + `GROUP BY` | ✅ (⚠️ if `l` is parsed/high-card) |
| `topk(n, …)` / `bottomk` | window `rank()` per bucket, filter ≤ n | ✅ |
| `\| unwrap field` + `quantile_over_time` | `approx_percentile_cont` over unwrapped numeric | ⚠️ |
| `\| unwrap` + `sum_over_time/avg_over_time` | aggregate over unwrapped value | ✅ |

**LogQL verdict**: full log + metric-query surface is expressible; the only ⛔ is live tail. Regex/parse constructs are ⚠️ (scan/parse cost) and bounded by [NFR9](./DESIGN.md#nfr9) `maxQueryBytesRead`. Selective `{service_name=…}` is the fast path (bloom). This is enough to be a drop-in Loki data source for dashboards + Explore.

---

## 2. PromQL → SQL (metrics, [FR1](./DESIGN.md#fr1))

Target tables: `gauge`, `sum`, `histogram`, `exp_histogram`, `summary`. Query interval 13 mo default / 2 y opt-in → **served via rollup tiers + splitting** for the long tail ([FR6](./DESIGN.md#fr6)/[FR8](./DESIGN.md#fr8)).

### 2.1 Selectors & matchers

| PromQL | SQL | Decision |
|---|---|---|
| `m{l="v"}` instant | `WHERE name='m' AND <preds>` + latest point in range | ✅ |
| `m{l=~"re"}` | `regexp_like(json_extract(attributes,'$.l'),'re')` | ⚠️ |
| `m{l!="v", l2!~"re"}` | `<>` / `NOT regexp_like` | ✅/⚠️ |
| `m[5m]` range vector | rows in `[t-5m, t]` per series | ✅ |
| `offset 1h`, `@ <ts>` | shift `@start/@end` | ✅ |

### 2.2 Aggregation operators

| PromQL | SQL | Decision |
|---|---|---|
| `sum/min/max/avg/count/group by (l)` | aggregate + `GROUP BY json_extract(attributes,'$.l')` | ✅ (⚠️ high-card `by`) |
| `… without (l)` | `GROUP BY` all-but-l | ✅ |
| `stddev/stdvar` | `stddev()/var()` | ✅ |
| `topk(n,…)/bottomk(n,…)` | window `rank()` `LIMIT n` | ✅ |
| `count_values` | `GROUP BY value` | ⚠️ cardinality |
| `quantile(φ, …)` | `approx_percentile_cont(φ)` | ✅ |

### 2.3 Functions

| PromQL | SQL | Decision |
|---|---|---|
| `rate/irate/increase/delta/idelta(v[d])` | `LAG()` window `PARTITION BY series ORDER BY time`; counter-reset rule ([PromQL ADR](./adrs/promql-parsing-strategy.md)) | ✅ |
| `*_over_time` (`avg/min/max/sum/count/last/present/stddev/stdvar/quantile`) | windowed aggregate `OVER (… ROWS …)` | ✅ |
| `histogram_quantile(φ, …)` | CTE + `UNNEST` bucket arrays + cumulative window + interpolation; or **rollup tier** for long tail | ⚠️ ([rabbit hole 5](./DESIGN.md#rabbit-holes); raw-native fallback) |
| `label_replace/label_join` | `regexp_replace` / `concat` into output labels | ✅ |
| `abs/ceil/floor/round/exp/ln/log2/log10/sqrt/clamp/clamp_min/clamp_max` | scalar SQL fns | ✅ |
| `vector(s)/scalar(v)/time()` | literal / projection | ✅ |
| `sort/sort_desc` | `ORDER BY` | ✅ |
| `absent/absent_over_time` | needs "no series" knowledge | ⛔ defer (NFR; needs series catalog) |
| `predict_linear/holt_winters/deriv/double_exponential_smoothing` | forecasting over unbounded inner eval | ⛔ defer |
| subqueries `(… [5m:1m])` | nested range eval | ⛔ defer (unbounded; [rabbit hole](./DESIGN.md#rabbit-holes)) |

### 2.4 Binary & set operators

| PromQL | SQL | Decision |
|---|---|---|
| arithmetic `+ - * / % ^` | SQL arithmetic on joined series | ✅ |
| comparison `> < == != >= <=` (+ `bool`) | `CASE`/filter; `bool` → 0/1 | ✅ |
| logical `and/or/unless` | `INTERSECT`/`UNION`/`EXCEPT` on label sets | ⚠️ join cost |
| `on()/ignoring()/group_left/group_right` | join key selection | ⚠️ vector-matching complexity |

**PromQL verdict**: the dashboard-dominant set (`rate`, `sum by`, `topk`, `max_over_time`, `histogram_quantile`, binary ops, label discovery) is ✅/⚠️ and covered. Forecasting + `absent*` + subqueries are ⛔ deferred (they conflict with bounded-cost NFRs and need a series catalog). Long-range correctness comes from rollups+splitting, not from a different translation.

---

## 3. TraceQL → SQL (traces, [FR2](./DESIGN.md#fr2))

Target table: `traces`. Query interval 30 d ([NFR7](./DESIGN.md#nfr7)).

| TraceQL | SQL | Decision |
|---|---|---|
| `{ resource.service.name = "x" }` | `WHERE service_name='x'` | ✅ (bloom) |
| `{ name = "y" }` / `{ status = error }` / `{ kind = server }` | top-level columns | ✅ |
| `{ span.http.status_code >= 500 }` | `CAST(json_extract(attributes,'$.http.status_code') AS INT) >= 500` | ⚠️ |
| `{ .attr = "v" }` (any scope) | `json_extract(attributes / resource_attributes, …)` | ⚠️ |
| `{ duration > 1s }` | `duration_nanos > 1e9` | ✅ |
| `&&` / `\|\|` | `AND` / `OR` | ✅ |
| trace by id (`/api/v2/traces/:id`) | `WHERE trace_id = X'…'` | ✅ (bloom) |
| `/search/tags`, `/search/tag/:t/values` | `SELECT DISTINCT` (top-level + `json_extract`) | ✅/⚠️ |
| aggregates `count() / avg(duration) > …` (span-set) | `GROUP BY trace_id HAVING …` | ⚠️ |
| `select(...)` | column projection | ✅ |
| structural `>>` (descendant), `>` (child), `~` (sibling) | self-join on `parent_span_id`/`trace_id` (recursive) | ⛔ defer v1 ([rabbit hole 2](./DESIGN.md#rabbit-holes)) |

**TraceQL verdict**: attribute-filter search, trace-by-id, and tag discovery (the pcap surface) are covered. **Structural/span-set operators are ⛔ deferred** — they require recursive self-joins on the span tree, expensive and rare for dashboards. Trace-by-id is bloom-accelerated.

---

## 4. What's restricted, and the SQL escape hatch

Consolidated ⛔ list and why (all conflict with an NFR or rabbit hole):

| Construct | Reason | Escape |
|---|---|---|
| LogQL live tail | hot/unflushed data ([non-goal](./DESIGN.md#non-goals)) | — (out of scope) |
| PromQL `predict_linear`/`holt_winters`/subqueries | unbounded inner evaluation → breaks [NFR6](./DESIGN.md#nfr6) | [SQL endpoint (FR9)](./DESIGN.md#fr9) |
| PromQL `absent`/`absent_over_time` | needs full series catalog | future (series index) |
| TraceQL structural operators | recursive span-tree joins, costly/rare | [SQL endpoint (FR9)](./DESIGN.md#fr9) |

Anything not expressible (or not yet implemented) in the three languages is reachable via **raw SQL ([FR9](./DESIGN.md#fr9))** over the same catalog — including cross-signal JOINs the three languages structurally cannot do. So "restricted" never means "impossible", only "not via the Grafana-native API".

## 5. Coverage summary

| Language | ✅ native | ⚠️ cost-flagged | ⛔ restricted |
|---|---|---|---|
| **LogQL** | selectors, `\|=`/`!=`, count/rate/sum/topk over time, limit/direction | regex `\|~`, json/logfmt/pattern parsers, unwrap-quantile, high-card `by` | live tail |
| **PromQL** | `rate`/`increase`, `sum/max/avg/topk by`, `*_over_time`, scalar fns, binary ops, label discovery | `histogram_quantile`, regex matchers, vector-matching, `count_values` | `absent*`, forecasting, subqueries |
| **TraceQL** | promoted-field filters, trace-by-id, tag discovery, `&&`/`\|\|`, duration | attribute (`json_extract`) filters, span-set aggregates | structural operators |

This is "support the full practical surface with conscious trade-offs", per the directive — not blanket support, and not an embedded engine.
