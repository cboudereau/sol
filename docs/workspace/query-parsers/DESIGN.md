# query-parsers — Design Doc

## Context

Sol's query backend translates LogQL and TraceQL to SQL over Parquet (DataFusion).
PromQL already uses a real parser — [`promql-parser`](https://crates.io/crates/promql-parser),
which is itself a grmtools (`lrpar`/`lrlex`) port of Prometheus's goyacc grammar —
so its surface is broad and faithful. LogQL and TraceQL, by contrast, are parsed
by **ad-hoc string slicing** in `src/query/loki.rs` and `src/query/tempo.rs`:
find the `{…}`, split on a fixed operator set. That covers only the demo pcap
subset and cannot express the full grammars.

This work replaces that string-slicing with real grammar-faithful parsers built
the **same way the Go upstream builds them** — a yacc/LALR grammar plus a
hand-written lexer — using grmtools so the grammar artifact ports from, and
re-syncs with, Loki's `pkg/logql/syntax/expr.y` and Tempo's `pkg/traceql/expr.y`.
Background and upstream analysis: [QUERY-PARSING.md](../parquet-backend/QUERY-PARSING.md).

A core deliverable is an explicit **coverage matrix** per language — grammar
feature → (parsed? / lowered-to-SQL? / deferred-why) — so the gap between today's
subset and full query support is legible and tracked, not implicit.

### Current surface (the baseline to preserve)

- **LogQL** (`loki.rs`): `{matchers}` (`= != =~ !~`, regex anchored), line filters
  (`|= != |~ !~`, empty backtick no-op), `service_name` promoted + `prom_attr`
  for resource attributes, the volume metric shape
  `sum by (level)(count_over_time({…}[r]))` → matrix, labels/label_values.
- **TraceQL** (`tempo.rs`): `{ a="x" && b!="y" }` — only `=`/`!=`, only `&&`,
  intrinsics (`name status kind duration`) + `span.`/`resource.`/`.attr` JSON,
  `{}` matches all. No `|| > < =~`, no structural operators, no pipeline/aggregates.

## Functional Requirements

### <a id="fr1"></a>FR1 — LogQL parser (grammar-complete)
Parse the full LogQL grammar into a typed AST faithful to Loki's `expr.y`: the two
top-level families `LogSelectorExpr` (stream selector + pipeline stages) and
`SampleExpr` (range aggregation over a log range, vector aggregation, binary ops),
including line filters, parser stages (`json`/`logfmt`/`regexp`/`pattern`/`unpack`),
label-filter expressions, `label_format`/`line_format`/`drop`/`keep`, and the
binary/label-matching operators. "Complete" means the parser *accepts* the grammar;
lowering coverage is staged (FR3, FR6).

### <a id="fr2"></a>FR2 — TraceQL parser (grammar-complete)
Parse the full TraceQL grammar into a typed AST faithful to Tempo's `expr.y`:
`SpansetPipeline` of spanset filters `{ FieldExpression }` combined by spanset
operators (`&& || >> << ~` and the `!`/`&` variants), pipeline stages
(`| aggregate cmp scalar`, `by(...)`, `select`, `coalesce`), aggregates
(`count/min/max/avg/sum`), field expressions over intrinsics + scoped attributes
(`span. resource. event. … parent.*`), with comparison + arithmetic operators.

### <a id="fr3"></a>FR3 — LogQL AST → SQL lowering
Lower the supported subset of the LogQL AST to SQL over `logs`, behind the
existing public functions (`translate_query_range`, `handle_volume`,
`handle_query_range`, `handle_labels`, `handle_label_values`, `handle_series`,
`handle_index_*`). Unsupported-but-parsed constructs return a clear, structured
"not yet supported" error — never a panic, never silently-wrong SQL.

### <a id="fr4"></a>FR4 — TraceQL AST → SQL lowering
Lower the supported subset of the TraceQL AST to SQL over `traces`, behind
`translate_search` (and feeding `handle_search` / trace-by-id). Same
parsed-but-unsupported → clear error rule as FR3.

### <a id="fr5"></a>FR5 — Behavioural parity (regression safety)
Every query the current ad-hoc parser handles must continue to produce equivalent
results after the swap. The existing `query::loki` and `query::tempo` tests are the
parity contract and must stay green unchanged (or change only where a string-level
SQL assertion is replaced by a result-level assertion of equal meaning).

### <a id="fr6"></a>FR6 — Coverage matrix (the gap, made explicit)
A per-language matrix in this workspace mapping each grammar feature to its state:
`parsed` (AST accepts it), `lowered` (produces SQL), or `deferred` (with the reason
and the blocking dependency). It is the authoritative "remaining work to support
all queries" artifact and is updated as lowering lands.

### <a id="fr7"></a>FR7 — Parse-error reporting
A parse failure returns a structured error (message + position/expected token where
grmtools provides it), surfaced as HTTP 400 by the route handlers. No input — valid,
invalid, adversarial, or empty — may panic.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Upstream sync-ability
The grammar is maintained as a port of the upstream goyacc `.y`. Each grammar file
records the upstream repo, path, and **pinned commit** it was ported from, plus the
re-sync procedure (diff upstream `.y`, mirror rule/precedence edits). What syncs:
productions + precedence/associativity. What is always rewritten in Rust: the lexer
and the semantic actions (AST construction). This split is documented, not hidden.

### <a id="nfr2"></a>NFR2 — No new external dependency
Use **grmtools** (`lrpar`, `lrlex`, `cfgrammar`), which is already in the dependency
tree via `promql-parser` (verified in `Cargo.lock`). No new third-party crate enters
the graph. Build-time codegen runs in `build.rs`.

### <a id="nfr3"></a>NFR3 — Safety: no panics, injection-safe
The parser must not panic on any input (covered by property/fuzz-style tests over
random and adversarial strings). Lowering keeps today's SQL-escaping discipline
(`esc`, parameter-free string literals escaped) so no query value can break out of
its literal (NFR9 from parquet-backend).

### <a id="nfr4"></a>NFR4 — Negligible parse latency
Parsing + lowering is one-shot per request, off the per-row path; it must add
negligible latency relative to SQL execution. No grammar ambiguity that forces
super-linear parsing.

### <a id="nfr5"></a>NFR5 — Backward-compatible API
Route handlers (`src/query/routes.rs`) and public function signatures are unchanged.
The full `query::` test suite stays green throughout.

## Non-goals

- **Log ingestion / tailing / live streaming.** Sol's query backend is read-side
  only; `query_range`/`series`/`volume` over stored Parquet. Not in scope.
- **Lowering LogQL dynamic-label pipelines** (`| json`/`| logfmt` extracting new
  labels that later `| label_filter`/`| label_format`/`by(...)` consume). The parser
  accepts them (FR1), but lowering is **deferred**: Sol's SQL backend operates on
  stored columns; runtime per-line extraction feeding downstream stages needs a
  row-pipeline executor, a separate engine. Reason: cost/complexity; revisit if a
  dashboard needs it. Tracked in the matrix (FR6) and a future ADR.
- **Lowering TraceQL structural operators** (descendant `>>`, ancestor `<<`, and the
  `&`/`!` structural variants). Parser accepts them (FR2); lowering is **deferred**:
  it requires recursive span-tree joins over flat Parquet (rabbit hole below).
  Reason: complexity + performance unknowns; revisit with a dedicated ADR.
- **`| line_format` templating.** Parsed, not lowered (would need a Go-template-like
  evaluator per row). Deferred.
- **Replacing `promql-parser`.** PromQL stays as-is; this work is LogQL + TraceQL.
- **New AST/feature coverage in the SQL cross-signal endpoint** (`sql.rs`) — unaffected.

## Rabbit holes

- **goyacc → grmtools precedence port.** goyacc resolves ambiguities with `%prec`
  and `%left`/`%right` declarations; grmtools has equivalents but conflict reports
  differ. *Cap:* port precedence/associativity declarations 1:1 first; if a
  shift/reduce conflict resists, resolve minimally and document it — do not redesign
  the grammar to dodge it.
- **TraceQL attribute-path lexing.** Tempo's lexer enters a special scan mode on `.`
  or a scope keyword to read dotted attribute paths, and `tryScanDuration` for
  durations. *Cap:* replicate Tempo's lexer modes faithfully in the Rust lexer; do
  not invent a new tokenization that drifts from upstream.
- **Structural-operator lowering over flat Parquet.** `a >> b` needs ancestor/
  descendant relationships from `parent_span_id` chains — recursive joins. *Cap:*
  out of scope for this work (non-goal); parse only, error on lower.
- **Dynamic-label LogQL pipelines.** As above — parse only, error on lower.
- **Two upstream grammars.** Loki/Tempo also ship Lezer grammars for the editor; the
  goyacc `.y` is canonical for the backend. *Cap:* port from the goyacc `.y`; use
  Lezer only as a cross-check.

## Design

### Architecture (C4 level 2 — the parse→lower pipeline)

```mermaid
flowchart LR
    Q["LogQL / TraceQL\nquery string"] --> L[lexer (.l, grmtools lrlex)]
    L --> P["parser (.y, grmtools lrpar)\nport of upstream expr.y"]
    P -->|Ok| A["typed AST\n(mirrors upstream nodes)"]
    P -->|Err| E["structured parse error → HTTP 400"]
    A --> LW["lowering pass\nAST → SQL string"]
    LW -->|supported| SQL["SQL over logs / traces"]
    LW -->|parsed-but-deferred| NS["clear 'not yet supported' error"]
    SQL --> ENG["QueryEngine.sql (DataFusion)"]
```

### Module layout

```
src/query/logql/
  mod.rs        # public re-exports; thin wrappers keep translate_*/handle_* signatures
  lexer.l       # grmtools lrlex spec (port of Loki lex.go token rules)
  grammar.y     # grmtools lrpar grammar (port of pkg/logql/syntax/expr.y)
  ast.rs        # LogSelectorExpr, SampleExpr, pipeline stages, matchers
  lower.rs      # AST → SQL (reuses esc/label_lhs/prom_attr + matrix builders)
src/query/traceql/
  mod.rs
  lexer.l       # port of Tempo lexer.go (incl. attribute-path / duration modes)
  grammar.y     # port of pkg/traceql/expr.y
  ast.rs        # SpansetPipeline, SpansetExpr, FieldExpression, Aggregate
  lower.rs      # AST → SQL (reuses traceql_lhs/matcher_sql)
build.rs        # lrlex + lrpar codegen for both grammars
```

`loki.rs`/`tempo.rs` keep their response-shaping + handlers; their `parse_selector`
/`translate_*` internals are replaced by calls into the new modules. Public
signatures (FR5/NFR5) are unchanged.

### AST principle

Node names mirror upstream (`LogSelectorExpr`/`SampleExpr`;
`SpansetPipeline`/`SpansetExpr`/`FieldExpression`). This is the single biggest
maintainability lever — it keeps the Rust AST diffable against the Go AST and the
lowering legible (as the PromQL path already demonstrates).

### Staged lowering principle

The parser is grammar-complete from the start; **lowering is staged**. A parsed
construct with no lowering returns a structured "not yet supported: <feature>"
error (FR3/FR4/FR7). The coverage matrix (FR6) is the ledger of what is `lowered`
vs `deferred`. This cleanly separates "we can read the query" from "we can answer
it", and is how the gap is made explicit.

Decisions:
- [Parser strategy: grmtools, porting the upstream goyacc grammar](./adrs/parser-strategy.md)

## Cross-cutting Concerns

- **Observability** — parse failures increment the existing per-API request metric
  with an error outcome (reuse `routes::rec` + error path); no new metric needed.
- **Migration** — vertical, parity-first: land each parser behind its existing entry
  point at *current* feature parity (all tests green), then widen parse coverage,
  then widen lowering. No flag-day; the swap is internal.
- **Rollback** — the parse→lower modules sit behind unchanged public functions; a
  regression reverts to the prior `loki.rs`/`tempo.rs` internals via git without API
  impact.
