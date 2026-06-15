# Cast Remediation for src/ (sinks, sources, transforms) — Design Doc

Amends: [clippy-lint-remediation](../../20260510_clippy-lint-remediation/designs/20260510_clippy-lint-remediation.md) — extends
Phase 2 of the [numeric conversion conventions ADR](../adrs/20260510_numeric-conversion-conventions.md)

## Context

The [clippy-lint-remediation](../../20260510_clippy-lint-remediation/designs/20260510_clippy-lint-remediation.md) workspace
fixes all 400 clippy errors in `sol-core` (the library crate, which enforces
`deny(clippy::pedantic)`). This workspace addresses the **other side** of the
codebase: `src/` — the main crate containing sinks, sources, and transforms.

`src/lib.rs` does **not** enforce `deny(clippy::pedantic)`. It does enforce
`deny(warnings)`, which means any lint promoted to a warning becomes an error.
Currently, cast-related pedantic lints (`cast_precision_loss`,
`cast_possible_truncation`, `cast_lossless`, `cast_sign_loss`,
`cast_possible_wrap`) are not enabled, so the ~457 `as` casts compile silently.

This matches upstream Vector's approach: pedantic was enforced in the library
crate only. The `src/` casts were inherited as-is, undocumented.

### Cast inventory

| Cast type | Sinks | Sources | Transforms | Total |
|---|---|---|---|---|
| `as f64` | ~60 | ~85 | ~38 | 183 |
| `as i32` | ~30 | ~45 | ~28 | 103 |
| `as u64` | ~25 | ~30 | ~13 | 68 |
| `as usize` | ~15 | ~20 | ~8 | 43 |
| `as i64` | ~10 | ~18 | ~12 | 40 |
| `as u32` | ~11 | ~13 | ~4 | 28 |
| **Total** | **~151** | **~235** | **~71** | **457** |

**122 files** contain at least one cast. **72 casts** are in test files; **385**
are in production code.

### Top hotspot files

| File | Casts | Primary pattern |
|---|---|---|
| `transforms/20260505_servicegraph/transform.rs` | 28 | Proto enum discriminants |
| `sources/statsd/aggregator.rs` | 18 | Histogram index math |
| `sinks/prometheus/collector.rs` | 16 | Count/length to f64 |
| `transforms/span_metrics/transform.rs` | 15 | Proto enum + timestamp |
| `sources/host_metrics/memory.rs` | 15 | System metric → f64 |
| `sources/vector/convert.rs` | 14 | Protobuf field conversions |
| `sources/datadog_agent/ddsketch.rs` | 14 | Histogram index math |
| `sinks/util/buffer/metrics/mod.rs` | 14 | Metric math |

### Pattern analysis

The 457 casts cluster into a small number of repetitive patterns:

**Pattern 1 — System metrics to f64** (~145 casts, 39%):
`counter_value as f64`, `gauge_value as f64` where the source is `i64` or `u64`.
Concentrated in metric sources (`host_metrics`, `mongodb_metrics`,
`postgresql_metrics`, `nginx_metrics`, `apache_metrics`, `eventstoredb_metrics`).
Two files (`mongodb_metrics/mod.rs` and `postgresql_metrics.rs`) define identical
`counter!`/`gauge!` macros that expand `$value as f64` — fixing the macro
definition fixes all expansion sites.

**Pattern 2 — Proto enum discriminant** (~70 casts, 15%):
`AggregationTemporality::Delta as i32`, `SpanKind::Client as i32`, etc. Safe by
construction (enum discriminants are small). These are not precision-related —
they're a prost code style issue.

**Pattern 3 — Lossless widening** (~37 casts, 8%):
`u32 → f64`, `u8 → f64`, `i32 → i64`, `u16 → i64`, etc. Replace with
`.into()` or `T::from()`. Fully mechanical, auto-fixable by `cargo clippy --fix`.

**Pattern 4 — Timestamp conversions** (~20 casts, 4%):
`ts.timestamp_nanos_opt().unwrap_or(0) as u64`, nanos → chrono decomposition.
Same patterns as `sol-core`; can reuse `OtlpTimestamp` from
[clippy-lint-remediation Task 0](../../20260510_clippy-lint-remediation/designs/20260510_clippy-lint-remediation.md).

**Pattern 5 — Length/count to f64** (~20 casts, 4%):
`values.len() as f64`, `count as f64`. Precision loss negligible for collection
sizes.

**Pattern 6 — Protocol field narrowing** (~30 casts, 7%):
`len as u32` (protocol frame), `secs as i32` (gRPC), `count as u32` (DDSketch).
Need per-site `#[expect]` with domain reasoning.

**Pattern 7 — Arithmetic / algorithm-specific** (~25 casts, 5%):
DDSketch index math, adaptive concurrency controller, sample transform hash
threshold. Need per-site or per-function `#[expect]`.

**Remaining** (~110 casts, 24%):
Miscellaneous casts spread across 80+ files, typically 1-2 per file.
Same treatment: `.into()` for lossless, `#[expect]` for others.

### Effort assessment

| Tier | Approach | Casts fixed | Effort |
|---|---|---|---|
| Mechanical `.into()` | Replace lossless widening, auto-fixable | ~37 | ~30 min |
| Macro fix | Fix 4 macro definitions in 2 files | ~145 (expansions) | ~15 min |
| Bulk `#[expect]` | System metrics, length/count → f64 | ~80 | ~1.5 h |
| Per-site `#[expect]` | Sign loss, truncation, wrap, algorithm | ~125 | ~3 h |
| Proto enum cleanup | `EnumVariant as i32` annotation | ~70 | ~1 h |
| **Total** | | **~457** | **~6 h** |

The work is highly mechanical and repetitive. ~80% of casts share one of
three fix patterns (`.into()`, `#[expect(cast_precision_loss)]`, or proto
enum `#[expect(cast_possible_truncation)]`).

## Functional Requirements

### <a id="fr1"></a>FR1 — Fix all lossless widening casts

Replace all `cast_lossless` casts with `.into()` or `T::from()`. These are
type-safe widening conversions that the compiler can verify. ~37 sites.

### <a id="fr2"></a>FR2 — Fix metrics macro casts

Update the `counter!`/`gauge!` macros in `mongodb_metrics/mod.rs` and
`postgresql_metrics.rs` to handle the `as f64` cast with an `#[expect]`
annotation or a typed helper. This fixes ~145 expansion sites.

### <a id="fr3"></a>FR3 — Annotate precision-loss casts

All `cast_precision_loss` sites (`i64 as f64`, `u64 as f64`, `usize as f64`)
must have `#[expect(clippy::cast_precision_loss, reason = "...")]`. ~100 sites.

### <a id="fr4"></a>FR4 — Annotate truncation, sign-loss, and wrap casts

All `cast_possible_truncation`, `cast_sign_loss`, and `cast_possible_wrap`
sites must have `#[expect(..., reason = "...")]` with per-site or per-function
safety reasoning. ~100 sites.

### <a id="fr5"></a>FR5 — Enable cast lints on `src/`

After all casts are fixed, add the five cast lints to `src/lib.rs`:

```rust
#![deny(clippy::cast_lossless)]
#![deny(clippy::cast_precision_loss)]
#![deny(clippy::cast_possible_truncation)]
#![deny(clippy::cast_sign_loss)]
#![deny(clippy::cast_possible_wrap)]
```

Combined with the existing `#![deny(warnings)]`, this prevents regression.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — No behavioral regression

All existing tests must pass. No metric value, timestamp, or counter must
change behavior. Cast remediation is annotation-only — the generated machine
code is identical.

### <a id="nfr2"></a>NFR2 — Use `#[expect]` over `#[allow]`

Following the convention from
[clippy-lint-remediation NFR3](../../20260510_clippy-lint-remediation/designs/20260510_clippy-lint-remediation.md#nfr3),
prefer `#[expect]` so stale annotations produce a compiler warning.

### <a id="nfr3"></a>NFR3 — Reuse `sol-core` boundary types where applicable

Timestamp conversions in `src/` that match the VRL ↔ OTLP pattern should
reuse `OtlpTimestamp` from `sol-core` rather than adding independent
`#[expect]` annotations. Other boundary types (`OtlpCount`, `OtlpEnumField`)
may also apply if the conversion crosses the same VRL ↔ OTLP boundary.

## Non-goals

- **Enable `deny(clippy::pedantic)` on `src/`**: Pedantic includes many
  non-cast lints (doc_markdown, redundant_closure, etc.) that would produce
  thousands of additional errors. This workspace only enables the 5 cast-related
  lints.
- **Refactor prost enum casts**: The `EnumVariant as i32` pattern is a prost
  convention. A `From<Enum> for i32` implementation would be cleaner but is a
  separate concern.

## Rabbit holes

- **Feature-flag-gated code**: Some sinks/sources are behind feature flags.
  Ensure `--all-features` is used for clippy so all code paths are checked.
- **Macro-expanded casts**: The `counter!`/`gauge!` macros expand `as f64` at
  each call site. The `#[expect]` must be placed on the macro definition or
  on a helper function the macro calls — not at each call site.

## Design

### Fix strategy

All casts in `src/` get one of three treatments:

1. **Lossless widening** → `.into()` (compiler-enforced, zero annotation)
2. **Precision/truncation/sign/wrap** → `#[expect(clippy::..., reason = "...")]`
3. **Macro-sourced** → fix the macro definition (one annotation, many sites)

### Enforcement

After all casts are fixed, enable the 5 cast lints in `src/lib.rs`. Since
`src/lib.rs` already has `#![deny(warnings)]`, the new `#![deny(...)]` entries
prevent any future bare `as` cast from compiling.

### Ordering

This workspace depends on the
[clippy-lint-remediation](../../20260510_clippy-lint-remediation/designs/20260510_clippy-lint-remediation.md) workspace
completing first (specifically Task 0, which creates the boundary types that
`src/` timestamp conversions will reuse).

Decisions:
- [Numeric conversion conventions](../adrs/20260510_numeric-conversion-conventions.md) — Phase 2 enforcement plan
- [Cast safety strategy](../adrs/20260510_cast-safety-strategy.md) — annotation conventions

## Cross-cutting Concerns

- **CI**: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  must stay green throughout. The 5 new `#![deny]` entries in `src/lib.rs` are
  automatically enforced by CI.
- **Feature flags**: Must use `--all-features` to cover all code paths.
- **Dependency on clippy-lint-remediation**: Must complete sol-core boundary
  types (Task 0) before starting timestamp conversions here.
