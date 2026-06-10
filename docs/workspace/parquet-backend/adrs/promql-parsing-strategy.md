---
status: draft
---
# PromQL parsing strategy

Addresses: [FR1](../DESIGN.md#fr1), [NFR1](../DESIGN.md#nfr1)

## Problem

The Prometheus HTTP API accepts PromQL queries. The query backend must parse PromQL and translate it to DataFusion SQL over Parquet metric tables.

Writing a full PromQL parser and evaluator is a multi-month effort. PromQL has ~50 functions, complex operator precedence, subquery syntax, and edge cases around staleness handling, counter resets, and lookback windows.

How much PromQL do we need, and how do we parse it?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Use existing `promql-parser` crate (parse) + custom SQL translator (evaluate) | Standard-compliant parsing. Focus effort on translation only. Active crate with PromQL spec coverage. | Dependency on external crate. May parse functions we don't support — need graceful unsupported-function errors. |
| B. Hand-written parser for observed subset only | No external dependency. Tight control over supported subset. | Fragile — PromQL syntax is complex (nested expressions, binary operators, modifier syntax). Easy to miss edge cases. Maintenance burden as we add functions. |
| C. Embed a full PromQL engine (e.g., port from Go) | Full compatibility. | Massive effort. Go PromQL engine is tightly coupled to TSDB iterator model, not translatable to SQL. |

## Decision

**Option A — Use `promql-parser` crate for parsing, custom translator for SQL generation.**

Rationale:
- **Parsing is solved**: the `promql-parser` crate handles the full PromQL grammar (operator precedence, modifier syntax, subqueries). Writing this correctly by hand is error-prone.
- **Translation is the real work**: converting the AST to DataFusion SQL for each supported function. This is where our effort should go.
- **Graceful degradation**: unsupported functions return a clear error (`unsupported PromQL function: predict_linear`) rather than a parse failure. Users know exactly what's not yet supported.
- **Incremental coverage**: start with the subset observed in the pcap, add functions as needed. The parser handles the full grammar — we only need to implement translators for what we support.

Supported function subset (from pcap analysis):

| PromQL function | SQL translation | Priority |
|---|---|---|
| `rate(v[d])` | `LAG()` window function | P0 |
| `sum by (l) (v)` | `GROUP BY l` + `SUM()` | P0 |
| `histogram_quantile(q, v)` | CTE + UNNEST + interpolation | P0 |
| `topk(n, v)` | `ORDER BY DESC LIMIT n` | P0 |
| `max_over_time(v[d])` | `MAX() OVER (ROWS BETWEEN ...)` | P0 |
| `max by (l) (v)` | `GROUP BY l` + `MAX()` | P0 |
| `avg(v)` | `AVG()` | P1 |
| `count(v)` | `COUNT()` | P1 |
| `scalar(v)` | passthrough | P1 |
| `clamp_min(v, min)` | `GREATEST(v, min)` | P1 |

Binary operators (`+`, `-`, `*`, `/`, `>`, `bool`) are translated to SQL arithmetic/comparison.

## Consequences

- Add `promql-parser` as a dependency behind a feature flag (e.g., `querier-backend`).
- The translator is a match on the parsed AST — one arm per supported function. Unsupported functions return an error variant.
- PromQL staleness rules (5-minute lookback, stale NaN markers) are NOT implemented in v1. Queries return the latest data point in the requested range. This is acceptable for dashboard use cases but not for alerting.
- Counter reset detection in `rate()` is simplified: if `value[t] < value[t-1]`, assume reset and use `value[t]` as the delta. Full PromQL reset handling (with staleness) is deferred.
