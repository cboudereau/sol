# Query parsing — LogQL & TraceQL (analysis for a real parser)

Status: **analysis / design note** (not an ADR). It documents how Loki and Tempo
parse their query languages, how `promql-parser` mirrors Prometheus in Rust, and
the options for replacing Sol's ad-hoc LogQL/TraceQL parsing. ADRs follow once the
approach is chosen.

## Context

| Language | Sol today | Upstream parser | Rust crate |
|---|---|---|---|
| PromQL | [`promql-parser`](https://crates.io/crates/promql-parser) (real AST) | Prometheus `promql/parser` (goyacc) | ✅ mature |
| LogQL | **ad-hoc string slicing** (`src/querier/loki.rs`) | Loki `pkg/logql/syntax` (goyacc) | ❌ none mature |
| TraceQL | **ad-hoc string slicing** (`src/querier/tempo.rs`) | Tempo `pkg/traceql` (goyacc) | ❌ none mature |

Sol's `loki.rs`/`tempo.rs` find the `{…}` selector and split on a fixed set of
operators. That covers the demo's pcap subset (label matchers, line filters, the
`sum by (level) (count_over_time({…}[r]))` volume shape, simple `{ a = b }`
TraceQL) but cannot express the full grammars: LogQL parser pipelines
(`| json | label_format …`), label-filter expressions, binary ops; TraceQL
spanset combinators (`>>`, `||`), pipelines (`| count() > 2`), aggregates. PromQL
already has a real parser, so its surface is far more complete (see
[CONFORMANCE.md](./CONFORMANCE.md) C-Pbin/C-P3).

This note is the groundwork for closing that gap deliberately rather than by
growing the string-slicer.

## Reference architecture: how `promql-parser` mirrors Prometheus

Prometheus parses PromQL with **goyacc** (Go's yacc): a grammar file
(`promql/parser/generated_parser.y`) compiled to a Go LALR parser, plus a
hand-written lexer and a post-parse type-check pass.

`promql-parser` is a near-direct Rust port: it uses **grmtools** (`lrlex` +
`lrpar`) with a `promql.y` LALR grammar and a `.l` lexer spec, generated in
`build.rs`. Its AST (`Expr::{VectorSelector, MatrixSelector, Call, Aggregate,
Binary, Unary, …}`) mirrors Prometheus's node-for-node, which is why Sol can lower
it cleanly (`src/querier/prometheus.rs`).

**The lesson:** the upstream `.y` grammar is the spec. The faithful Rust path is to
port that grammar to a Rust LR generator, keeping the AST shape aligned so it
tracks upstream changes.

## Loki — LogQL parser anatomy

- Grammar: `pkg/logql/syntax/expr.y` (goyacc, LALR) → checked-in `expr.y.go`
  (`goyacc -p syntax -o … expr.y`).
- Lexer: `pkg/logql/syntax/lex.go`, built on Go's `text/scanner`; recognises
  durations, bytes, IPs, and the pipe-operator tokens.
- AST + validation: `parser.go` produces two top-level expression families and
  validates them:
  - **`LogSelectorExpr`** — a log query: stream selector + a left-to-right
    **pipeline** of stages.
  - **`SampleExpr`** — a metric query: a *range aggregation* over a log range,
    optionally wrapped in a *vector aggregation* and binary ops.
- Second grammar: [`grafana/lezer-logql`](https://github.com/grafana/lezer-logql)
  (Lezer/CodeMirror) drives the Grafana editor. Two authoritative grammars exist —
  keep that in mind when porting.

### Grammar shape (from the Lezer grammar)

```
LogExpr      := Selector Pipeline?  |  "(" LogExpr ")"
Selector     := "{" Matcher ("," Matcher)* "}"
Matcher      := ident ("=" | "!=" | "=~" | "!~") string

Pipeline     := Stage*
Stage        := LineFilter            // |= |~ |> (and !=,!~,!> negations), `or`-composable
              | "|" Parser            // json | logfmt | regexp "…" | pattern "…" | unpack
              | "|" LabelExtract      // json { lbl="field" } | logfmt { … }
              | "|" LabelFilter       // matcher on extracted labels; IP/duration/bytes/number cmp; and/or
              | "|" "label_format" …  | "|" "line_format" "…"
              | "|" ("drop" | "keep") label ("," label)*
              | "|" "decolorize"

MetricExpr   := RangeAgg | VectorAgg | BinOp | Literal | LabelReplace | Vector | "(" MetricExpr ")"
RangeAgg     := rangeOp "(" LogRange ")"  ["(" number "," LogRange ")"]  Grouping?
rangeOp      := rate | count_over_time | bytes_over_time | bytes_rate | rate_counter
              | avg/sum/min/max/stddev/stdvar/quantile/first/last/absent _over_time
VectorAgg    := vecOp Grouping? "(" MetricExpr ")"
vecOp        := sum | avg | count | min | max | stddev | stdvar | topk | bottomk | sort | sort_desc
Grouping     := ("by" | "without") "(" label,* ")"
BinOp        := MetricExpr binop ["bool"] (("on"|"ignoring") "(" … ")")?
                                          (("group_left"|"group_right") …)? MetricExpr
```

Tokens: matchers `= != =~ !~`; line filters `|= |~ |> != !~ !>`; arithmetic
`+ - * / % ^`; comparison `== != > >= < <=`; logic `and or unless`; grouping
`by without on ignoring group_left group_right`; parsers `json logfmt regexp
pattern unpack unwrap`; formatting `label_format line_format decolorize`; misc
`drop keep bool offset label_replace vector bytes duration ip`.

## Tempo — TraceQL parser anatomy

- Grammar: `pkg/traceql/expr.y` (goyacc, LALR).
- Lexer: `pkg/traceql/lexer.go`, over Go's `text/scanner`; a `tokenMap` for
  keywords/operators, `tryScanDuration` for durations, and a special scan mode
  triggered by `.` or a scope token to read attribute paths.
- AST + validation: parse → `validate()` in `ast_validate.go` (type safety,
  aggregate usage, scoping).
- Second grammar:
  [`grafana/lezer-traceql`](https://github.com/grafana/lezer-traceql) for the
  editor.

### Grammar shape (from the Lezer grammar)

```
Query        := SpansetPipeline WithHint?
SpansetPipeline
             := SpansetExpr (SpansetOp SpansetExpr)*  ( "|" PipelineStage )*
SpansetExpr  := "{" FieldExpression? "}"  |  "(" SpansetPipeline ")"
FieldExpression
             := Field (cmp Field)*  combined with && ||, arithmetic, parens
Field        := Intrinsic | AttributeRef | literal
Intrinsic    := duration | name | status | kind | span:id | trace:duration | …
AttributeRef := "." ident                       // resource-level shorthand
              | scope "." ident                  // span. resource. event. instrumentation. link.
              | "parent." scope "." ident
SpansetOp    := "&&" | "||" | ">>" | "<<" | "~"          // and, or, descendant, ancestor, regex
              | "!>" | "!>>" | "!<" | "!<<" | "!~"        // experimental negations
              | "&>>" | "&<<" | "&>" | "&<" | "&~"        // union-structural
PipelineStage:= Aggregate (cmp Scalar)?  |  "by" "(" Field,* ")"  |  select(…) | coalesce()
Aggregate    := "count" "(" ")" | ("max"|"min"|"avg"|"sum") "(" Field ")"
Metrics      := rate() | count_over_time() | histogram_over_time() | quantile_over_time()
              | (min|max|avg|sum)_over_time() | topk(n) | bottomk(n)
```

Tokens: comparison `= != < <= > >= =~ !~`; arithmetic `+ - * / % ^`; logical
`&& ||`; structural `>> << ~ |` (+ the `!`/`&` variants above); scopes `span
resource event instrumentation link parent`; aggregates `count max min avg sum
by`; metrics `rate count_over_time histogram_over_time quantile_over_time
*_over_time topk bottomk`; misc `select coalesce compare`.

## Replicating it in Rust — options

| Option | What it is | Pros | Cons |
|---|---|---|---|
| **grmtools** (`lrpar`+`lrlex`) | LR(1) generator; what `promql-parser` uses | Faithful 1:1 port of the upstream `.y`; tracks upstream; consistent with PromQL path | LR conflicts fiddly; weaker default errors; `build.rs` codegen |
| **LALRPOP** | Rust LR(1) generator | Ergonomic macro grammar; good docs | Still LR (conflict tuning); transcribe (not copy) the `.y` |
| **chumsky / winnow / nom** | Parser-combinators (hand-written) | Best error messages; no codegen; incremental, easy to grow stage-by-stage | Grammar transcribed by hand; can drift from upstream LALR |
| **pest** | PEG | Quick to stand up; readable `.pest` | PEG ≠ LALR — structurally diverges from the originals; harder to keep in sync |

## Recommendations

1. **Two parsers, one pattern.** Build `logql` and `traceql` parser modules that
   mirror the PromQL flow: `parse(&str) -> Ast`, then a separate **lowering** pass
   `Ast -> SQL`. Keep the public `translate_query_range` / `translate_search`
   signatures so the SQL side and the existing tests are untouched; the parser
   swaps in behind them.

2. **Pick the generator by goal.**
   - If the priority is *faithfulness and tracking upstream*, use **grmtools** and
     port the goyacc `.y` directly — this matches `promql-parser` exactly and the
     upstream `.y` becomes the spec of record.
   - If the priority is *error messages and incremental delivery* (Sol controls a
     subset and grows it), use **chumsky**. It is the pragmatic choice for a fork
     that wants good diagnostics and no second build step, at the cost of hand-
     transcribing the grammar.
   - Recommendation: **chumsky for LogQL/TraceQL** (Sol implements a growing
     subset, and clear errors help the demo), unless full upstream parity becomes
     a hard requirement — then **grmtools**.

3. **Pin the upstream grammar revision.** Record the Loki/Tempo commit hash of the
   `.y` (and Lezer grammar) the AST was modelled on, so future ports diff against a
   known baseline.

4. **Model the AST on upstream node names** (`LogSelectorExpr`/`SampleExpr`;
   `SpansetPipeline`/`SpansetExpr`/`FieldExpression`). Node-name alignment is what
   makes the PromQL lowering legible and is the single biggest maintainability win.

5. **Stage the migration vertically.** Land the parser behind the current entry
   points with the *existing* feature subset first (parity, all tests green), then
   add grammar coverage one stage at a time (LogQL `| json`/label filters; TraceQL
   `>>`/pipelines), each as its own slice with tests.

## Scope today vs. a real parser

| Capability | Sol now (ad-hoc) | With a real parser |
|---|---|---|
| LogQL stream selector + line filters | ✅ | ✅ |
| LogQL volume (`count_over_time` + `sum by`) | ✅ (pattern-matched) | ✅ (general) |
| LogQL parser pipeline (`\| json \| label_format`) | ❌ | ✅ |
| LogQL label-filter expressions / binary ops | ❌ | ✅ |
| TraceQL single `{ a op b }` | ✅ | ✅ |
| TraceQL spanset combinators (`&&`, `\|\|`, `>>`) | ❌ | ✅ |
| TraceQL pipeline + aggregates (`\| count() > 2`) | ❌ | ✅ |

## Next steps

- Decide generator (chumsky vs grmtools) → write **ADR: LogQL/TraceQL parser
  strategy** capturing the choice and the AST contract.
- Spike the LogQL parser first (smaller grammar, immediate demo value), behind
  `translate_query_range`, at current feature parity.
- Then TraceQL, behind `translate_search` / trace-by-id.

## Sources

- [Loki `pkg/logql/syntax`](https://pkg.go.dev/github.com/grafana/loki/v3/pkg/logql/syntax),
  [`grafana/lezer-logql`](https://github.com/grafana/lezer-logql)
- [Tempo `pkg/traceql/lexer.go`](https://github.com/grafana/tempo/blob/main/pkg/traceql/lexer.go),
  [`grafana/lezer-traceql`](https://github.com/grafana/lezer-traceql),
  [TraceQL (DeepWiki)](https://deepwiki.com/grafana/tempo/4.1-traceql-language)
- [`promql-parser`](https://crates.io/crates/promql-parser) (grmtools `promql.y`),
  Prometheus `promql/parser` (goyacc)
