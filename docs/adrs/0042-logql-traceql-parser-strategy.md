---
status: accepted
---
# Parser strategy: grmtools, porting the upstream goyacc grammar

> **Implemented (Sessions 1–4):** both parsers shipped on grmtools, wired in at
> parity. LogQL was built and reviewed first, then TraceQL.

Addresses: [FR1](../designs/20260605_query-parsers.md#fr1), [FR2](../designs/20260605_query-parsers.md#fr2), [NFR1](../designs/20260605_query-parsers.md#nfr1), [NFR2](../designs/20260605_query-parsers.md#nfr2)

## Problem

Replace the ad-hoc LogQL/TraceQL string-slicing with real grammar-faithful
parsers. The dominant requirement is **sync-ability with the Go upstream**: Loki
and Tempo evolve their query languages, and Sol should be able to track those
changes with minimal, mechanical effort. The choice of parsing technology also
fixes the new-dependency question.

How Loki and Tempo parse today (both identical in shape): a **goyacc** (yacc/LALR)
grammar in a `.y` file plus a **hand-written lexer** over Go's `text/scanner`, then
a semantic `validate()` pass. The faithful Rust analogue must let us port that `.y`.

## Options

| Option | Sync with upstream `.y` | New dep | Errors | Notes |
|---|---|---|---|---|
| **grmtools** (`lrpar`+`lrlex`) | **Best** — `.y` ports rule-for-rule; precedence ports 1:1 | **None** (already in tree via `promql-parser`) | Weak (default) | Same toolchain as the PromQL path; build.rs codegen; LR conflict tuning |
| LALRPOP | Partial — transcribe, not copy (different grammar DSL) | Yes (1 crate, lalrpop present transitively but as a generator) | Medium | Ergonomic LR macros; build.rs |
| chumsky / winnow / nom | Conceptual only | Yes | **Best** | Hand-written combinators; grammar transcribed by hand |
| Hand-written (recursive-descent + Pratt) | Conceptual only | None | DIY | Most control; most code; precedence by hand |

Reality of the sync win (applies to every cross-language option): only the
**grammar productions + precedence/associativity** port mechanically. The **lexer**
and the **semantic actions** (AST construction) are rewritten in Rust regardless,
because goyacc actions are Go and the lexer is Go `text/scanner` code. grmtools is
the only option where the upstream artifact (`.y`) is *also* the artifact we
maintain, so the re-port is a structured diff rather than a re-read.

## Decision

**Adopt grmtools (`lrpar` + `lrlex`) and maintain `logql.y`/`traceql.y` as ports of
Loki's `pkg/logql/syntax/expr.y` and Tempo's `pkg/traceql/expr.y`.**

Rationale, weighted by the stated priority (sync):
1. The upstream `.y` is the spec of record; our grammar is a line-comparable port,
   so re-syncing precedence (the most error-prone part — LogQL binary/pipe ops,
   TraceQL `&& || >> <<` structural operators) is a mechanical diff.
2. **No new dependency** — `lrpar`/`lrlex`/`cfgrammar` are already compiled in the
   tree via `promql-parser` (verified in `Cargo.lock`).
3. Unifies all three query parsers (PromQL, LogQL, TraceQL) on one toolchain.
4. The main downside — weaker default error messages — is low-impact here: inputs
   come from Grafana (well-formed), not a human REPL.

Each grammar file pins the upstream repo + path + commit it was ported from, and
documents the re-sync procedure (NFR1). The lexer and AST actions are written in
Rust and explicitly understood not to auto-sync.

This decision is `draft` until the Phase 4c pre-flight approval; it is reversible
in favour of chumsky (if error quality outranks sync) or hand-written (if zero
codegen/build.rs is preferred) before implementation begins.

## Consequences

**Easier**
- Tracking upstream grammar changes: diff their `.y`, mirror rules/precedence.
- Consistency with the existing PromQL parser and its build setup.
- No dependency-surface growth or licensing review.

**Harder**
- LR conflict tuning when porting goyacc `%prec` hacks to grmtools.
- Parse-error messages need manual enrichment if we want them friendly.
- A `build.rs` codegen step (two grammars) — build complexity, though `promql-parser`
  already establishes the pattern in the workspace.

**Unchanged / out of this decision**
- The lexer and AST-building actions are hand-written Rust either way.
- Structural-operator and dynamic-label-pipeline **lowering** remain deferred
  (DESIGN non-goals); this ADR is about *parsing*, not lowering.
