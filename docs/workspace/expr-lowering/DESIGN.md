# expr-lowering — Design Doc

Builds on: [designs/20260605_query-parsers.md](../../designs/20260605_query-parsers.md)

## Context

Sol's query backend lowers three parsed query languages (PromQL, LogQL, TraceQL)
to SQL **text** built with `format!`, then hands the string to `ctx.sql(query)`
(`QueryEngine::sql`), which DataFusion *re-parses* into a `LogicalPlan`. Today
there are ~110 `format!` SQL-build sites (prometheus 54, loki 34, tempo 22, sql 2),
and each signal carries its **own** label-resolution + predicate builders that all
emit SQL fragments:

- prometheus: `label_lhs`, `matcher_pred`, `metric_value_and_match`, `series_sql`.
- loki: `label_lhs`, `label_pred`, `line_pred`, `label_filter_pred`, `pipeline_preds`.
- tempo: `traceql_lhs`, `field_key`, `lower_cmp`, `lower_field_expr`, `collect_preds`.

They share the same shape — *label/field `op` value → predicate* — and the same
UDFs (`prom_attr` ×29, `prom_metric_name` ×31, `json_get_str` ×8, `regexp_like` ×17,
`octet_length` ×4), yet none of it is shared, and every value is string-interpolated
(guarded by `esc()` — an injection surface, NFR9).

DataFusion's SQL is itself a front-end over its **logical layer**: `ctx.sql(text)`
parses to a sqlparser AST then lowers to a `LogicalPlan` built from `Expr` nodes —
the *same* IR the `DataFrame`/`LogicalPlanBuilder` API builds directly. Building
`Expr` programmatically reaches that IR one step lower (no text, no re-parse) and is
the engine's native, type-safe, injection-safe form. This work introduces a **shared
`Expr` predicate builder** as the common lowering target across the three signals,
and migrates the filter/projection/group-by/distinct queries to the `DataFrame` API,
while keeping window-function metric lowering and Rust-native histogram/array compute
as-is (hybrid).

## Functional Requirements

### <a id="fr1"></a>FR1 — Shared `Expr` predicate builder
A single module builds DataFusion `Expr` predicates from a normalized
*(lhs, op, value)* triple, reused by PromQL matchers, LogQL label filters/selector
matchers, and TraceQL field comparisons. It covers `= != =~ !~ > >= < <=`, the
Prometheus/LogQL absent-label semantics (absent ≡ empty for `=""`/`!=`), anchored
regex (`^(?:…)$`), and numeric comparison. Label LHS resolution (promoted column vs
`prom_attr`/`json_get_str` UDF call on an attributes column) is a parameter, not
duplicated per signal.

### <a id="fr2"></a>FR2 — `Expr` values are literals (injection-safe)
Every query value enters as `lit(value)` (a bound literal in the plan), never as
interpolated text. The `esc()` discipline is removed from migrated paths; no query
value can alter SQL structure (NFR9 satisfied structurally, not by escaping).

### <a id="fr3"></a>FR3 — Migrate filter/projection/group-by/distinct queries to DataFrame
The "pure" queries — filter + project + sort + limit, distinct, and simple group-by
aggregation — are built and executed via the `DataFrame`/`LogicalPlanBuilder` API
instead of SQL strings: TraceQL search (`translate_search`→`handle_search`), LogQL
streams (`translate_query_range`→`handle_query_range`) and volume, and the discovery
endpoints (labels / label values / series / tags / tag values / index stats+volume).

### <a id="fr4"></a>FR4 — Plan-based execution path on the engine
`QueryEngine` gains a method to execute a built `DataFrame`/`LogicalPlan` (collect to
Arrow batches) alongside the existing `sql(&str)`, with query-cache integration
(keying derived from the plan, not SQL text). The cache contract (FR5/NFR6 of
parquet-backend) is preserved.

### <a id="fr5"></a>FR5 — Behavioural parity
Every query the current string lowering handles produces equivalent results after
the migration. `query::` tests stay green; where a test asserts SQL *text*, it is
rewritten to assert `Expr`/plan structure or result-level equivalence of equal
meaning. Public function signatures and HTTP routes are unchanged.

### <a id="fr6"></a>FR6 — Hybrid boundary made explicit
A documented list of which lowerings migrate to `Expr` and which deliberately stay
SQL-string (window functions + array/JSON-histogram compute), with the reason. The
shared predicate builder is still reused by the SQL-staying paths via `Expr`
un-parsing where it reduces duplication (see [ADR: cache + unparser](./adrs/cache-and-unparser.md)).

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — No new external dependency
Only `datafusion::logical_expr` / `datafusion::prelude` (DataFrame, `col`, `lit`,
`Expr`) and `datafusion::sql::unparser` — all already in the tree (DataFusion 53.1.0).

### <a id="nfr2"></a>NFR2 — No performance regression
Plan-based execution must not regress latency vs the SQL path (it skips a parse, so
should be ≤). The query cache must remain effective (equal queries → cache hit).

### <a id="nfr3"></a>NFR3 — Incremental, reversible migration
Migrate one signal/endpoint per vertical slice behind unchanged public functions, so
each slice is independently shippable and revertible; never a flag-day.

## Non-goals

- **Migrating window-function metric lowering.** `rate` (LAG + counter-reset),
  `<agg>_over_time` (RANGE frames), the Prometheus instant *latest-per-series*
  (`ROW_NUMBER … WHERE rn=1`), `sum by`/`topk` over windows — these are expressible
  via `Expr::WindowFunction` but far more verbose and error-prone than the existing,
  tested SQL. They **stay SQL** (hybrid). Reason: cost/risk ≫ benefit; revisit only
  if the predicate-builder reuse proves compelling there too.
- **Rust-native histogram/array compute.** `histogram_quantile`, classic-bucket
  heatmap synthesis, byte-volume `octet_length` — already computed over Arrow arrays
  in Rust (parquet-backend rabbit-hole #5), not SQL. Unaffected.
- **The cross-signal `/api/v1/sql` endpoint.** User-supplied SQL stays
  `sql_user` (sqlparser path); not a lowering we build. Unaffected.
- **A unified *source* AST across signals.** PromQL/LogQL/TraceQL stay distinct
  front-ends; the shared layer is the lowering *target* (`Expr`), not the parse AST.
  (Already established in the parsers design — a fact, not a gap.)

## Rabbit holes

- **Window functions in the DataFrame API.** Tempting to migrate `rate`/`_over_time`
  too. *Cap:* out of scope (non-goal); do not attempt — they stay SQL.
- **`Unparser` fidelity.** Rendering shared `Expr` predicates back to SQL text for
  the SQL-staying paths (so the builder is reused everywhere) depends on
  `datafusion::sql::unparser` reproducing UDF calls / anchored regex faithfully.
  *Cap:* if an `Expr` doesn't round-trip to the expected SQL within the pilot slice,
  keep those SQL paths hand-written and reuse the builder only in DataFrame paths;
  do not chase unparser edge cases.
- **Cache keying for plans.** The cache is keyed by SQL text. A plan-based key must
  be stable + collision-free. *Cap:* derive from the optimized `LogicalPlan`'s
  indented display string (deterministic); if that proves fragile, fall back to a
  structured `CacheKey`. Decide in the ADR; don't invent a bespoke hash scheme.
- **UDF lookup as `Expr`.** Calling `prom_attr`/`prom_metric_name`/`json_get_str` as
  `Expr::ScalarFunction` needs the registered `ScalarUDF` from the context registry.
  *Cap:* resolve once via the `SessionContext` UDF registry; if a UDF isn't reachable
  as an `Expr` builder, that signal's affected predicate stays SQL for now.

## Design

### Architecture (C4 level 2 — lowering targets the logical layer)

```mermaid
flowchart LR
    subgraph parse [parsers (shipped)]
      P["PromQL AST"]
      L["LogQL AST"]
      T["TraceQL AST"]
    end
    P & L & T --> PB["shared predicate builder\n(lhs, op, value) → Expr  (lit values)"]
    PB --> DF["DataFrame / LogicalPlanBuilder\n(filter · project · group-by · distinct)"]
    PB -. unparse .-> SQLW["SQL WHERE fragment\n(window-fn queries that stay SQL)"]
    DF --> ENG["QueryEngine.collect(plan)\n(cache by plan key)"]
    SQLW --> STR["format! SQL (rate / *_over_time / instant)"]
    STR --> ENGSQL["QueryEngine.sql(text)"]
    ENG & ENGSQL --> EXEC["LogicalPlan → optimize → execute (Arrow)"]
```

### Module layout

```
src/query/predicate.rs   # NEW — shared Expr builders:
                         #   label_eq/neq/re/nre/cmp(lhs: Expr, value, …) -> Expr
                         #   attr_call(udf, column, key) -> Expr   (prom_attr / json_get_str)
                         #   anchored_regex(lhs, pattern) -> Expr
                         #   absent-aware equality (NULL-or-empty)
src/query/catalog.rs     # QueryEngine::collect(df|plan) + plan-based cache key
src/query/{prometheus,loki,tempo}.rs
                         # *_lhs return Expr; pure queries build via DataFrame;
                         # window/array lowering unchanged (SQL)
```

### Domain model & interfaces

- `predicate::LabelMatch { lhs: Expr, op: MatchKind, value: String, numeric: bool }`
  → `to_expr() -> Expr` — the one place op-semantics live.
- `QueryEngine::collect(plan: DataFrame) -> Result<Vec<RecordBatch>>` — mirrors
  `sql()` (cache + telemetry), keyed by the plan (ADR).
- Each signal keeps its public `translate_*`/`handle_*` signatures; internals route
  through `predicate` + `DataFrame` for the migrated set.

Decisions:
- [Lowering target: DataFusion Expr / LogicalPlan via DataFrame](./adrs/lowering-target.md)
- [Hybrid boundary: which queries migrate vs stay SQL](./adrs/hybrid-boundary.md)
- [Plan-based cache keying + Expr unparser reuse](./adrs/cache-and-unparser.md)

## Cross-cutting Concerns

- **Observability** — `collect()` reuses the same `record_request`/`record_cache`
  telemetry as `sql()`; no dashboard change.
- **Migration** — vertical, parity-first, one endpoint/signal per slice; pilot on
  TraceQL search (purest filter case) to validate the builder, DataFrame execution,
  and cache keying before extending.
- **Rollback** — each slice sits behind an unchanged public function; revert one
  commit to restore the SQL-string lowering for that endpoint, no API impact.
