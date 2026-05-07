# Clippy Lint Remediation — Design Doc

## Context

Sol forked from Vector at commit `692704adc`. The upstream `vector-core` crate
enforced `#![deny(clippy::pedantic)]` with only 10 specific `#![allow(...)]`
entries — notably, `cast_precision_loss` and `cast_possible_truncation` were
**not** allowed, meaning upstream enforced them.

Sol introduced OTLP support by adding new files (`otel_metric.rs`,
`otel_event.rs`, `otel_json.rs`, `otel_attributes.rs`, `otlp.rs`) and heavily
extending `vrl_target.rs` (+562 lines). This new code triggers **400 clippy
errors** under the existing `deny(clippy::pedantic)` + `deny(clippy::all)`.

**Error distribution by origin** (verified via `git blame`):

| Origin | Files | Errors | % |
|---|---|---|---|
| Sol-new files | `otel_event.rs`, `otel_metric.rs`, `otel_json.rs`, `otel_attributes.rs`, `otlp.rs` | 283 | 71% |
| Sol additions to `vrl_target.rs` | `vrl_target.rs` (all 93 error lines blame to Sol) | 104 | 26% |
| Inherited upstream code | `lua/event.rs`, `lua/metric.rs`, `event/mod.rs`, `metric/series.rs`, `test/serialization.rs`, `source_sender/output.rs`, `source_sender/tests.rs` | 13 | 3% |

**387 of 400 errors (97%) are in Sol-authored code.** The 13 inherited errors
are from newer clippy lints that did not exist when upstream Vector ran CI,
except `source_sender/output.rs:266` which upstream already handled with
`#[expect(clippy::cast_precision_loss)]` (Sol lost this annotation during the
fork).

### The VRL ↔ OTLP type mismatch

The core problem is a type boundary between two systems:

**VRL `Value`** (Vector's internal representation):
- `Value::Integer(i64)` — all integers, signed 64-bit
- `Value::Float(NotNan<f64>)` — all floats

**OTLP protobuf** (OpenTelemetry wire format):
- Timestamps: `fixed64` → Rust `u64` (nanoseconds since epoch)
- Counts/flags: `uint32` → Rust `u32`
- Enum fields (kind, severity, aggregation_temporality): `int32` → Rust `i32`
- Scale/offset: `sint32`/`int32` → Rust `i32`
- Metric values: `oneof { double, sfixed64 }` → Rust `f64` or `i64`

Every conversion between these two type systems requires a cast. Sol's OTLP
layer has **179 such casts** across three files. The danger depends on the
direction:

| Direction | Cast | Risk | Example |
|---|---|---|---|
| OTLP → VRL (ingestion) | `u64 as i64` | Wraps at 2^63 (year 2262 for nanos) | timestamp ingestion |
| OTLP → VRL (ingestion) | `u32 → i64` | **None** — widening, use `.into()` | `dropped_attributes_count` |
| OTLP → VRL (ingestion) | `i32 → i64` | **None** — widening, use `.into()` | `severity_number`, `kind` |
| VRL → OTLP (emission) | `i64 as u64` | Sign loss if Value is negative | timestamp emission |
| VRL → OTLP (emission) | `i64 as u32` | **Truncation** if Value > `u32::MAX` | `dropped_attributes_count` |
| VRL → OTLP (emission) | `i64 as i32` | **Truncation** if Value > `i32::MAX` | `severity_number` |
| Internal computation | `i64 as f64` | **Precision loss** if \|v\| > 2^53 | `NDPValue::AsInt` → metric math |
| Internal computation | `usize as f64` | **Precision loss** if len > 2^53 | set cardinality |
| Timestamp decomposition | `u64 / 1B as i64` | Safe — quotient fits i64 until year 2262 | chrono conversion |
| Timestamp decomposition | `u64 % 1B as u32` | **Safe** — modulo result < 10^9 < 2^30 | chrono nanoseconds |
| Histogram math | `i32 as f64`, `floor() as i32` | Algorithm-specific, safe by IEEE 754 spec | exponential histogram index |

### How upstream Vector handled precision

Upstream Vector had **two lint regimes** across its crates:

**`vector-core` (library crate)** — `deny(clippy::pedantic)`:
- The `MetricValue` type used `f64` natively for all metric values (counter,
  gauge, histogram sum, sample value). Counts were `u64`. This avoided the
  `i64 → f64` boundary in metric code entirely.
- The few remaining casts (5 total: timing, counters, cardinality) used
  `#[expect]` with inline safety reasoning at each site.

**`src/` and `opentelemetry-proto`** — no `deny(clippy::pedantic)`:
- These crates did not enforce pedantic lints. This is not inherently wrong —
  pedantic is opt-in by design, and many crates reasonably choose not to
  enable it.
- However, the casts at protocol boundaries were **undocumented**: no
  `#[expect]`, no safety comments, no reasoning about precision bounds.
- The `opentelemetry-proto` crate had 14 casts at the OTLP ↔ Vector boundary:
  - `self.point.time_unix_nano as i64` — `u64→i64` timestamp (5 occurrences)
  - `AggregationTemporality::Delta as i32` — enum discriminant (3 occurrences)
  - `i as i32` — bucket index (2 occurrences)
  - `NumberDataPointValue::AsInt(i) => Some(i as f64)` — `i64→f64` precision
    loss for |v| > 2^53 (1 occurrence)
  - `severity_number as i32`, `time_unix_nano as i64`,
    `observed_time_unix_nano as i64` (3 occurrences)
- The `src/sinks/datadog/metrics/encoder.rs` had similar undocumented casts:
  `*value / (interval as f64)`, `values.len() as f64`, `cnt as i64`

**The real issue is not the lint level — it's the lack of documentation.**
A crate without `deny(pedantic)` can still have well-documented casts. The
problem was that upstream's non-pedantic crates had bare `as` casts with no
indication of whether the author considered the precision/truncation trade-off.

### Sol's approach

Sol moved the OTLP conversion code into `sol-core`, which has
`deny(clippy::pedantic)`. This forces every cast to be explicitly acknowledged.

For crates without `deny(pedantic)` (like `src/`), Sol establishes
**documented conventions** (see
[numeric conversion conventions ADR](./adrs/numeric-conversion-conventions.md)):
every protocol-boundary cast must have either:
- `.into()` for lossless widening (compiler-enforced)
- A safety comment explaining the precision/truncation bound

This is enforced by code review, not by the compiler — the lint level does not
change for `src/`. When a sink or source is next modified, the convention
applies. See the
[ADR enforcement plan](./adrs/numeric-conversion-conventions.md) for details.

## Functional Requirements

### <a id="fr1"></a>FR1 — Keep precision lints enforced crate-wide

The existing `deny(clippy::pedantic)` in `lib/sol-core/src/lib.rs` already
enforces `cast_precision_loss` and `cast_possible_truncation`. Do **not** add
crate-wide `#![allow(...)]` for these. Instead, replace boundary casts with
[typed boundary conversions](./adrs/otlp-boundary-types.md), and annotate
remaining non-boundary casts with `#[expect(..., reason = "...")]`.

### <a id="fr2"></a>FR2 — Fix or annotate all casts in otel_metric.rs

Address all 53 casts in `otel_metric.rs` (23 `as i32`, 16 `as f64`, 5 `as u64`,
7 `as i64`, 2 `as u32`). Each cast must be either:
- Replaced with a [typed boundary conversion](./adrs/otlp-boundary-types.md)
  (`OtlpTimestamp`, `OtlpCount`, `OtlpEnumField`, `OtlpMetricInt`)
- Replaced with `.into()` for direct lossless widening (e.g., `u32 → f64`)
- Annotated with function-level `#[expect(..., reason = "...")]` for
  non-boundary casts (proto enum discriminants, histogram math)

### <a id="fr3"></a>FR3 — Fix or annotate all casts in otel_event.rs

Address all 76 casts in `otel_event.rs` (7 `as i32`, 1 `as f64`, 15 `as u64`,
40 `as i64`, 13 `as u32`). Same treatment as FR2.

### <a id="fr4"></a>FR4 — Fix or annotate all casts in vrl_target.rs

Address all 50 casts in `vrl_target.rs` (24 `as i64`, 15 `as u64`, 11
`as u32`) plus 54 other lint violations (redundant closures, collapsible ifs,
doc markdown, etc.). Same treatment as FR2 for casts; mechanical fix for style
lints.

### <a id="fr5"></a>FR5 — Fix all style lints in Sol-authored code

All 400 errors are fixable. The breakdown by lint category:

| Lint | Count | Fix strategy |
|---|---|---|
| `doc_markdown` | 81 | Add backticks in Sol-authored doc comments |
| `redundant_closure` | 67 | `cargo clippy --fix` or manual |
| `cast_possible_truncation` | 49 | Boundary types (`OtlpCount`, `OtlpEnumField`) or function-level `#[expect]` — see [ADR](./adrs/otlp-boundary-types.md) |
| `cast_lossless` | 44 | Boundary types (`.to_vrl()` uses `.into()` internally) or direct `.into()` |
| `collapsible_if` | 23 | `cargo clippy --fix` |
| `map_unwrap_or` | 14 | `cargo clippy --fix` |
| `single_match_else` | 13 | Rewrite as `if let` |
| `needless_pass_by_value` | 13 | Change to `&T` |
| `manual_let_else` | 11 | Rewrite as `let...else` |
| `cast_precision_loss` | 10 | `OtlpMetricInt::to_f64()` or local `#[expect]` for non-boundary casts — see [ADR](./adrs/otlp-boundary-types.md) |
| `match_same_arms` | 6 | Merge arms |
| `missing_errors_doc` | 6 | Add `# Errors` doc section |
| `return_self_not_must_use` | 6 | Add `#[must_use]` |
| `uninlined_format_args` | 5 | Inline variables in format strings |
| `items_after_statements` | 5 | Move items before statements |
| Other (misc style) | ~49 | Mechanical fixes |

### <a id="fr6"></a>FR6 — Handle the 13 inherited errors

The 13 errors in inherited upstream files must be fixed without large-scale
changes to inherited code:

| File | Errors | Lint | Fix |
|---|---|---|---|
| `lua/event.rs` | 3 | `semicolon_if_nothing_returned` | Add `;` |
| `lua/metric.rs` | 2 | `useless_vec` | `#[allow]` (macro-caused) |
| `lua/metric.rs` | 1 | `implicit_clone` | `.clone()` |
| `event/mod.rs` | 1 | `missing_panics_doc` | Add doc |
| `event/mod.rs` | 1 | `missing_errors_doc` | Add doc |
| `metric/series.rs` | 1 | `redundant_closure` | Simplify |
| `test/serialization.rs` | 1 | `collapsible_if` | Collapse |
| `source_sender/output.rs` | 1 | `cast_precision_loss` | Restore upstream's `#[expect]` annotation |
| `source_sender/tests.rs` | 2 | `uninlined_format_args`, `items_after_statements` | Mechanical |

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — No behavioral regression

All existing tests must pass. No metric value, timestamp, or counter must
change behavior. The CI command
`cargo clippy --workspace --all-targets --all-features -- -D warnings`
must pass after each session.

### <a id="nfr2"></a>NFR2 — Precision safety for observability data

Sol is an observability library. For the OTLP layer, the following invariants
must hold:

- **Timestamps**: `u64` nanosecond values must round-trip through `i64` without
  data loss for dates before year 2262 (max `i64` nanos = 2^63-1 ≈ year 2262).
  Encapsulated in `OtlpTimestamp`.
- **Metric values**: `i64 → f64` conversion must document precision bounds.
  Safe for |value| ≤ 2^53 (~9 quadrillion). Encapsulated in `OtlpMetricInt`.
- **Widening casts**: `u32 → i64`, `i32 → i64` encapsulated in `OtlpCount` and
  `OtlpEnumField` (use `.into()` internally). Never bare `as`.
- **Narrowing casts** (VRL → OTLP): `i64 → u32`, `i64 → i32` encapsulated in
  `OtlpCount::from_vrl()` and `OtlpEnumField::from_vrl()` (carry `#[expect]`
  with domain-constraint reasoning internally).
- **Enum discriminants**: `EnumVariant as i32` is safe by protobuf construction.
  Function-level `#[expect]`.

### <a id="nfr3"></a>NFR3 — Use `#[expect]` over `#[allow]` where supported

Following upstream Vector's convention, prefer `#[expect(clippy::lint_name)]`
over `#[allow(...)]` for local annotations. `#[expect]` causes a warning if
the lint no longer fires (e.g., after a future refactor removes the cast),
preventing stale annotations.

### <a id="nfr4"></a>NFR4 — CI coherence

The GitHub Actions CI pipeline (`ci.yml`) enforces lint and format on every PR:

| Job | Command | What it checks |
|---|---|---|
| `check-fmt` | `cargo fmt -- --check` | Formatting |
| `check-clippy` | `cargo clippy --workspace --all-targets --all-features -- -D warnings` | All clippy lints |
| `test` | `cargo nextest` | Unit tests |
| `check-deny` | `cargo deny` | Security & license audit |
| `check-proto` | `buf breaking` | Protobuf compatibility |

All these pass through `make` → `vdev` → cargo. The lint command in CI must
match the local dev command exactly. After this work, verify that:
1. `make check-clippy` passes locally (same as CI)
2. `make check-fmt` passes locally (same as CI)
3. No discrepancy between local and CI clippy flags

## Non-goals

- **Fix casts in `src/` sinks/sources/transforms**: The main crate has ~460
  unchecked `as` casts across 122 files in sinks, sources, and transforms.
  These are currently invisible because `src/lib.rs` does not enforce
  `deny(pedantic)`. Enabling cast lints would produce ~398 warnings
  (`cast_precision_loss`: 263, `cast_sign_loss`: 42, `cast_possible_truncation`:
  37, `cast_lossless`: 37, `cast_possible_wrap`: 19). The patterns are highly
  repetitive — two files alone account for 36% of all warnings. The
  [numeric conversion conventions ADR](./adrs/numeric-conversion-conventions.md)
  establishes the rules; a dedicated workspace covers the remediation.
- **Refactor the VRL Value type boundary**: The `i64 ↔ u64` mismatch is
  inherent to how VRL's `Value::Integer(i64)` represents all integers. Changing
  this would require an upstream VRL change and is out of scope.
- **Add runtime overflow checks**: Saturating/checked arithmetic for casts that
  are provably safe in context (e.g., nanosecond modulo) would add unnecessary
  runtime cost.
- **Redesign metric value storage**: Upstream Vector used `f64` natively for
  metrics, avoiding the `i64 → f64` cast. Sol's OTLP layer chose to work with
  the OTLP protobuf's `i64` representation directly. Switching to upstream's
  `f64`-native approach would be a larger refactor beyond this scope.

## Rabbit holes

- **Exponential histogram index math** (`otel_metric.rs:338-353`): Complex
  bit-manipulation for IEEE 754 decomposition. The `as i32` and `as f64` casts
  here are part of a mathematical algorithm; do not attempt to "fix" them —
  annotate with safety comments only.
- **Timestamp overflow year 2262**: `u64` nanos stored as `i64` wraps at 2^63.
  Document this as a known limitation, do not attempt a fix.

## Design

### VRL ↔ OTLP conversion boundary

The conversion boundary is where all precision/truncation risk concentrates.
Each direction has a clear strategy:

**OTLP → VRL (ingestion — reading proto fields into Value):**

```
u64 timestamp  ──OtlpTimestamp──►  Value::Integer(i64)   ✓ wraps at 2^63 (year 2262)
u32 count      ──OtlpCount──────► Value::Integer(i64)   ✓ lossless (.into() internal)
i32 enum       ──OtlpEnumField──► Value::Integer(i64)   ✓ lossless (.into() internal)
i64 metric val ─────────────────► Value::Integer(i64)   ✓ same type
f64 metric val ─────────────────► Value::Float(f64)     ✓ same type
```

**VRL → OTLP (emission — writing Value back to proto fields):**

```
Value::Integer(i64) ──OtlpTimestamp──►  u64 timestamp    ✓ clamps negative to 0
Value::Integer(i64) ──OtlpCount──────►  u32 count        ✓ truncation encapsulated
Value::Integer(i64) ──OtlpEnumField──►  i32 enum         ✓ truncation encapsulated
Value::Integer(i64) ──OtlpMetricInt──►  f64 metric math  ✓ precision loss encapsulated
```

**Internal (timestamp → chrono):**

```
u64 nanos  ──OtlpTimestamp::to_chrono()──►  DateTime<Utc>   ✓ decomposition encapsulated
DateTime   ──OtlpTimestamp::from_chrono()──► u64 nanos       ✓ sign clamp encapsulated
```

### Cast safety classification

Each cast gets one of four treatments:

1. **Lossless widening** → replace `as` with `.into()`
   - `u32 → u64`, `u32 → i64`, `i32 → i64`, `u32 → f64`, `i32 → f64`
   - Zero-cost, type system guarantees safety

2. **Bounded precision loss** → `#[expect]` with safety argument
   - `i64 as f64` for metric values — precise for |v| ≤ 2^53
   - `usize as f64` for set cardinality — precise for len ≤ 2^53
   - `i64 as f64` for timing — upstream's approach, safe in practice

3. **Domain-constrained narrowing** → typed boundary conversion
   - `i64 as u32` for `dropped_attributes_count` — `OtlpCount::from_vrl()`
   - `i64 as i32` for `severity_number` — `OtlpEnumField::from_vrl()`
   - `u64 as i64` for timestamps — `OtlpTimestamp::to_vrl()`
   - `i64 as u64` for timestamps from Value — `OtlpTimestamp::from_vrl()`

4. **Algorithm-specific** → `#[expect]` block on enclosing function
   - Exponential histogram IEEE 754 bit decomposition
   - `powf(idx as f64)` for bucket boundary calculation

### Typed boundary conversions

Rather than scattering `#[expect]` annotations across ~87 call sites with
identical safety arguments, each OTLP wire type gets a newtype that
encapsulates both the cast and its safety reasoning (see
[otlp-boundary-types ADR](./adrs/otlp-boundary-types.md)):

| Newtype | Wraps | Sites | Key conversions |
|---|---|---|---|
| `OtlpTimestamp(u64)` | nanos since epoch | ~37 | `to_vrl()`, `from_vrl()`, `to_chrono()`, `from_chrono()` |
| `OtlpCount(u32)` | counts, flags | ~24 | `to_vrl()`, `from_vrl()` |
| `OtlpEnumField(i32)` | enums, scale, offset | ~16 | `to_vrl()`, `from_vrl()` |
| `OtlpMetricInt(i64)` | integer metric values | ~10 | `to_f64()` |

Call sites become self-documenting with zero annotations:

```rust
// Before: bare cast, needs #[expect] annotation
Value::Integer(self.record.time_unix_nano as i64)
// After: type carries the safety reasoning
Value::Integer(OtlpTimestamp::from_nanos(self.record.time_unix_nano).to_vrl())

// Before: bare widening, should be .into() but still primitive
Value::Integer(res.dropped_attributes_count as i64)
// After: domain type, widening encapsulated
Value::Integer(OtlpCount::from_proto(res.dropped_attributes_count).to_vrl())
```

The remaining ~33 cast sites (proto enum discriminant comparisons, exponential
histogram bit math, set cardinality) keep function-level `#[expect]` — they
don't cross the VRL ↔ OTLP boundary.

### `#[expect]` convention for non-boundary casts

Boundary casts are handled by the typed newtypes (no annotation at call sites).
For the ~33 remaining non-boundary casts (proto enum discriminants, histogram
bit math, set cardinality), follow upstream Vector's convention — every local
annotation must include a reason:

```rust
// Proto enum discriminant — not a VRL ↔ OTLP boundary crossing
#[expect(clippy::cast_possible_truncation, reason = "proto enum discriminant")]
fn aggregation_temporality_match(...) {
    if sum.aggregation_temporality == AggregationTemporality::Delta as i32 { ... }
}

// Set cardinality — not an OTLP wire type
#[expect(clippy::cast_precision_loss, reason = "precise for len ≤ 2^53")]
let cardinality = set.len() as f64;
```

Decisions:
- [VRL ↔ OTLP cast safety strategy](./adrs/cast-safety-strategy.md)
- [Typed boundary conversions](./adrs/otlp-boundary-types.md)
- [Inherited lint policy](./adrs/inherited-lint-policy.md)
- [Numeric conversion conventions for sinks/sources/transforms](./adrs/numeric-conversion-conventions.md)

## Cross-cutting Concerns

- **CI**: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  must stay green throughout
- **Observability**: no metric precision regression — existing integration tests
  cover round-trip fidelity
- **Rollback**: each session produces an independent commit; any session can be
  reverted without affecting others
