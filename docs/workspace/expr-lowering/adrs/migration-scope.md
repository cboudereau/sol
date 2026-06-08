---
status: draft
---
# Migration scope: full `Expr` migration (window primitives included)

Addresses: [FR3](../DESIGN.md#fr3), [FR6](../DESIGN.md#fr6)

## Problem

How far does the migration go? The earlier draft proposed a *hybrid* (migrate
filter/projection/aggregate; keep window-function lowering as SQL). Reconsidered:
should the window lowerings (`rate`, `*_over_time`, instant latest-per-series) also
move to `Expr`, leaving **no SQL in core**?

## Options

| Option | Pros | Cons |
|---|---|---|
| **Full** — all query construction to `Expr`, incl. windows | One execution path; one predicate/window library; uniform injection-safety; the whole surface is 9 primitives, only 3 are window helpers; canonical-ns removes the frame-unit risk | The 3 window primitives carry real parity risk; biggest rewire (PromQL range/instant) |
| Hybrid — migrate filters; windows stay SQL | Smaller; avoids window risk | Two lowering styles forever; `esc()`/injection surface persists on window WHEREs; predicate logic split; "where's the SQL?" ambiguity |
| Nothing structural — only a shared *string* predicate helper | Smallest | Keeps the injection surface; no native-IR/type-safety win |

## Decision

**Full migration.** All query *construction* targets `Expr`/`DataFrame`. The SQL
surface reduces to 9 reusable primitives (P1–P9 in the design); only P5/P6/P7
(latest-per-series, rate, `*_over_time`) are window functions, built **once** as a
parity-tested `plan::frame` module and reused by every signal. The chief window risk
— `RANGE` frame units vs the `ORDER BY` key — is removed by the canonical-nanosecond
convention ([ADR](./canonical-nanoseconds.md)).

**Remaining non-`Expr` (sanctioned):**
- `/api/v1/sql` — *user-supplied* SQL via `sql_user`; we don't build it (the one SQL site).
- Rust-native Arrow post-processing (`histogram_quantile`, bucket-heatmap explode,
  `resample_to_grid`, `topk_series`, binary-op vector matching, response shaping) —
  runs on result batches; never was SQL.

`draft` until pre-flight; reversible to hybrid if the window-primitive isolation
tests (the gate) can't reach parity within their time-box.

## Consequences

**Easier**: one query-construction path; window logic written once; structural
injection-safety everywhere; a CI-checkable "no `format!` SQL in core" invariant.

**Harder**: P5/P6/P7 must reproduce the current SQL window semantics exactly
(counter-reset, dup-timestamp drop, frame bounds) — mitigated by building/ testing
them in isolation before any rewire; the PromQL range/instant rewire is the largest
single slice.
