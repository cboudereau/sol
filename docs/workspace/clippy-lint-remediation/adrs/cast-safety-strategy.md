---
status: accepted
---
# VRL ↔ OTLP cast safety strategy

Addresses: [FR1](../DESIGN.md#fr1), [FR2](../DESIGN.md#fr2), [FR3](../DESIGN.md#fr3), [FR4](../DESIGN.md#fr4), [NFR2](../DESIGN.md#nfr2), [NFR3](../DESIGN.md#nfr3)

## Problem

Sol's OTLP layer converts between two type systems with incompatible integer
representations:

- **VRL `Value`**: `Value::Integer(i64)` for all integers — signed, 64-bit
- **OTLP protobuf**: `u64` (timestamps), `u32` (counts, flags), `i32` (enums,
  scale), `i64`/`f64` (metric values)

Every crossing of this boundary requires a cast. There are 179 such casts
across `otel_metric.rs` (53), `otel_event.rs` (76), and `vrl_target.rs` (50).
The `as` keyword in Rust performs these casts silently — it never panics but
may silently truncate, wrap, or lose precision.

### Why this matters for Sol

Sol is an observability library. Silent data corruption in metric values or
timestamps means wrong dashboards, missed alerts, and incorrect billing. The
OTLP spec is precise about its wire types; Sol must honor them.

### How upstream Vector handled this

Upstream had two lint regimes:

- **`vector-core`** (`deny(pedantic)`): Used `f64` natively for all metric
  values, eliminating the `i64→f64` boundary. The 5 remaining casts each had
  `#[expect(clippy::cast_precision_loss)]` with an inline reason.
- **`opentelemetry-proto`** (no `deny(pedantic)`): Had 14 unchecked casts at
  the OTLP boundary, including `NumberDataPointValue::AsInt(i) => Some(i as f64)`
  — the exact same precision-lossy pattern Sol has. These were never annotated
  because pedantic was not enforced in that crate.

Sol moved the OTLP conversion code into `sol-core` (which has `deny(pedantic)`).
This surfaces casts that upstream silently ignored — which is an improvement,
not a regression. The task is to annotate them properly.

## Options

| Option | Pros | Cons |
|---|---|---|
| A: `TryFrom` everywhere with `Result` propagation | Maximum safety, never silently corrupts | Huge API change: every getter/setter returns `Result`, breaks all callers, runtime cost on hot path |
| B: Saturating casts (`u32::try_from(v).unwrap_or(u32::MAX)`) | No panics, bounded error | Silently clamps — wrong metric value is worse than a clear error in observability |
| C: Local `#[expect]` with safety reasoning for each cast, `.into()` for widening | Minimal code change, documents assumptions, uses type system where possible, follows upstream convention | Relies on human analysis of safety bounds |
| D: Crate-wide `#![allow]` | No work | Hides real issues, disables lint for all future code too — regression from upstream |

## Decision

**Option C**: Local `#[expect]` with safety reasoning, `.into()` for widening.

### Conversion rules by boundary crossing

#### OTLP → VRL (ingestion: reading proto fields into Value)

| Proto type | VRL type | Cast | Treatment |
|---|---|---|---|
| `u64` (timestamp nanos) | `i64` | `u64 as i64` | `OtlpTimestamp::from_nanos(v).to_vrl()` — wraps at 2^63 (year 2262), acceptable |
| `u32` (counts, flags) | `i64` | `u32 → i64` | `OtlpCount::from_proto(v).to_vrl()` — lossless widening (`.into()` internal) |
| `i32` (enums, scale) | `i64` | `i32 → i64` | `OtlpEnumField::from_proto(v).to_vrl()` — lossless widening (`.into()` internal) |
| `i64` (metric value) | `i64` | none | Same type, no cast |
| `f64` (metric value) | `f64` | none | Same type, no cast |

#### VRL → OTLP (emission: writing Value back to proto fields)

| VRL type | Proto type | Cast | Treatment |
|---|---|---|---|
| `i64` | `u64` (timestamp) | `i64 as u64` | `OtlpTimestamp::from_vrl(v).as_nanos()` — clamps negative to 0 |
| `i64` | `u32` (counts) | `i64 as u32` | `OtlpCount::from_vrl(v).as_proto()` — value originated as u32, round-trips through i64 |
| `i64` | `i32` (enums) | `i64 as i32` | `OtlpEnumField::from_vrl(v).as_proto()` — enum discriminants are small (< 20) |
| `i64` | `f64` (metric math) | `i64 as f64` | `OtlpMetricInt::from_proto(v).to_f64()` — precise for \|v\| ≤ 2^53 |

#### Internal computations

| Operation | Cast | Treatment |
|---|---|---|
| `u64 nanos` → `DateTime<Utc>` | division + modulo | `OtlpTimestamp::from_nanos(v).to_chrono()` — encapsulates both casts |
| `DateTime<Utc>` → `u64 nanos` | `i64 as u64` | `OtlpTimestamp::from_chrono(ts).as_nanos()` — clamps negative |
| `u32 rate` → `f64` for sum | `.into()` | Direct `.into()` — lossless, u32 fits in f64 mantissa (32 < 52 bits) |
| `usize` set len → `f64` cardinality | `as f64` | Local `#[expect(cast_precision_loss)]` — precise for len ≤ 2^53 |
| Exp histogram bit math | `as i32`, `as f64` | Function-level `#[expect]` — IEEE 754 algorithm |

### Annotation format

Boundary casts are handled by the typed newtypes — no `#[expect]` at call
sites. For the ~33 remaining non-boundary casts, every `#[expect]` must include
a `reason` argument:

```rust
// Proto enum discriminant — function-level #[expect]
#[expect(clippy::cast_possible_truncation, reason = "proto enum discriminant")]
fn check_temporality(sum: &Sum) -> bool {
    sum.aggregation_temporality == AggregationTemporality::Delta as i32
}
```

```rust
// Set cardinality — local #[expect]
#[expect(clippy::cast_precision_loss, reason = "precise for len ≤ 2^53")]
let cardinality = set.len() as f64;
```

For functions with many non-boundary casts of the same kind (e.g., multiple
enum discriminant comparisons), a single `#[expect]` on the function is
acceptable if all casts share the same safety argument.

### Implementation: typed boundary conversions

The conversion rules above apply to 179 cast sites. Rather than annotating each
site individually (~500 lines of identical `#[expect]` annotations), each OTLP
wire type gets a newtype that encapsulates the cast and its safety reasoning:

- **`OtlpTimestamp(u64)`** (~37 sites): nanos since epoch, `u64 ↔ i64 ↔ chrono`
- **`OtlpCount(u32)`** (~24 sites): counts and flags, `u32 ↔ i64`
- **`OtlpEnumField(i32)`** (~16 sites): enums, scale, offset, `i32 ↔ i64`
- **`OtlpMetricInt(i64)`** (~10 sites): integer metric values, `i64 → f64`

See [otlp-boundary-types](./otlp-boundary-types.md) for the full type
definitions, before/after examples, and the module-level `#[deny]` override
needed for `cast_possible_wrap` and `cast_sign_loss` (which are crate-wide
`#![allow]` in `lib.rs`).

The remaining ~33 cast sites (proto enum discriminant comparisons, exponential
histogram bit math, set cardinality) keep function-level `#[expect]` — they
cluster tightly and don't cross the VRL ↔ OTLP boundary.

The conversion rules in this ADR remain the source of truth for **what** each
cast means and **why** it is safe. The
[otlp-boundary-types](./otlp-boundary-types.md) ADR decides **how** to
implement them without repetition.

## Consequences

- Every cast site documents its safety assumption — either through a typed
  newtype (whose implementation carries the `#[expect]` once) or a local
  `#[expect(..., reason)]` for non-boundary casts
- If a safety assumption becomes wrong, `#[expect]` fires a warning when the
  lint no longer triggers (e.g., after refactoring removes the cast)
- New code must justify any `as` cast locally — no crate-wide blanket
- Widening casts use `.into()` — the type system enforces safety, no annotation
  needed
- Timestamps are protected by a newtype — a `u64` byte counter cannot
  accidentally flow through the timestamp path
- The VRL ↔ OTLP boundary is explicitly documented for anyone working on
  this code in the future
