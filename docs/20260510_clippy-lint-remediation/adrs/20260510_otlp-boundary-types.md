---
status: accepted
---
# Typed boundary conversions for VRL ↔ OTLP

Addresses: [FR2](../designs/20260510_clippy-lint-remediation.md#fr2), [FR3](../designs/20260510_clippy-lint-remediation.md#fr3), [FR4](../designs/20260510_clippy-lint-remediation.md#fr4), [NFR2](../designs/20260510_clippy-lint-remediation.md#nfr2), [NFR3](../designs/20260510_clippy-lint-remediation.md#nfr3)

## Problem

The [cast-safety-strategy](../../20260510_src-cast-remediation/adrs/20260510_cast-safety-strategy.md) ADR establishes that every
`as` cast at the VRL ↔ OTLP boundary must carry an `#[expect]` with a reason.
Applied literally to all 179 cast sites, this produces ~500 lines of repetitive
annotations — the same 5–6 safety arguments copied dozens of times.

Bare `as` casts are **primitive-obsessed**: a `u64` that holds OTLP
nanoseconds-since-epoch looks identical to a `u64` that holds a byte counter or
a hash. A `u32` dropped-attributes-count is indistinguishable from a `u32` TCP
port number. Nothing in the type system prevents passing the wrong value through
a conversion path.

### Repetition inventory

| Pattern | Proto type | Sites | Safety argument (identical each time) |
|---|---|---|---|
| `record.time_unix_nano as i64` | `u64` | ~15 | "nanos fit in i64 until year 2262" |
| `.as_integer().unwrap_or(0) as u64` (timestamp) | `u64` | ~15 | "timestamps are non-negative" |
| `ts.timestamp_nanos_opt().unwrap_or(0) as u64` | `u64` | ~7 | "timestamps are non-negative" |
| `(nanos / 1B) as i64` + `(nanos % 1B) as u32` | `u64` | 4 dup. blocks | "quotient fits / modulo < 10^9" |
| `res.dropped_attributes_count as i64` | `u32` | ~11 | widening, should be `.into()` |
| `.as_integer().unwrap_or(0) as u32` (count/flags) | `u32` | ~13 | "proto field is u32, round-trips" |
| `self.record.severity_number as i64` | `i32` | ~9 | widening, should be `.into()` |
| `n as i32` (enum/scale assignment) | `i32` | ~7 | "proto enum, discriminant fits i32" |
| `*v as f64` (NDPValue::AsInt metric math) | `i64` | ~10 | "precise for \|v\| ≤ 2^53" |

Every category has the same problem: the same cast with the same safety argument
appears at dozens of call sites.

## Options

| Option | Pros | Cons |
|---|---|---|
| A: `#[expect]` on every cast site | Explicit at each usage | ~500 lines of identical annotations, primitive obsession unchanged |
| B: `OtlpTimestamp` newtype only + functions for the rest | Timestamps protected, non-timestamp casts centralized | Inconsistent — some types get domain protection, others don't |
| C: Newtype per OTLP wire-type category | Consistent pattern, type system prevents misuse for all categories, safety documented once per type | New types to learn, slight indirection |

## Decision

**Option C**: One newtype per OTLP wire-type category. Every proto field that
crosses the VRL boundary gets a typed wrapper. The cast safety reasoning lives
in each type's implementation — once — instead of at every call site.

### Type inventory

| Newtype | Wraps | Proto wire type | Cast sites absorbed | Proto fields |
|---|---|---|---|---|
| `OtlpTimestamp(u64)` | nanos since epoch | `fixed64` | ~37 | `time_unix_nano`, `start_time_unix_nano`, `end_time_unix_nano`, `observed_time_unix_nano` |
| `OtlpCount(u32)` | counts, flags | `uint32` | ~24 | `dropped_attributes_count`, `dropped_events_count`, `dropped_links_count`, `flags`, `span_flags`, `rate` |
| `OtlpEnumField(i32)` | enum discriminants, scale, offset | `int32` | ~16 | `severity_number`, `kind`, `status_code`, `scale`, `offset` |
| `OtlpMetricInt(i64)` | integer metric data-point values | `sfixed64` | ~10 | `NumberDataPointValue::AsInt` |

### `OtlpTimestamp`

```rust
/// OTLP nanosecond timestamp (nanos since Unix epoch).
///
/// Wraps the protobuf `fixed64` wire type. Centralizes all casts between
/// the OTLP `u64`, VRL `i64`, and chrono `DateTime<Utc>` representations.
///
/// Safety boundary: wraps at 2^63 nanos (year 2262).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub(crate) struct OtlpTimestamp(u64);

impl OtlpTimestamp {
    pub(crate) fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    pub(crate) fn as_nanos(self) -> u64 {
        self.0
    }

    #[expect(clippy::cast_possible_wrap, reason = "nanos fit in i64 until year 2262")]
    pub(crate) fn to_vrl(self) -> i64 {
        self.0 as i64
    }

    #[expect(clippy::cast_sign_loss, reason = "clamped to non-negative")]
    pub(crate) fn from_vrl(v: i64) -> Self {
        Self(v.max(0) as u64)
    }

    #[allow(dead_code)]
    #[expect(clippy::cast_possible_wrap, reason = "seconds fit in i64 until year 2262")]
    #[expect(clippy::cast_possible_truncation, reason = "modulo 10^9 < 2^30, fits u32")]
    pub(crate) fn to_chrono(self) -> DateTime<Utc> {
        let secs = (self.0 / 1_000_000_000) as i64;
        let nsecs = (self.0 % 1_000_000_000) as u32;
        Utc.timestamp_opt(secs, nsecs).single().unwrap_or_default()
    }

    #[allow(dead_code)]
    #[expect(clippy::cast_sign_loss, reason = "timestamps are non-negative")]
    pub(crate) fn from_chrono(ts: DateTime<Utc>) -> Self {
        Self(ts.timestamp_nanos_opt().unwrap_or(0).max(0) as u64)
    }
}
```

**Before** (~37 sites):
```rust
// Ingestion (15 sites):
Value::Integer(self.record.time_unix_nano as i64)
// Emission (15 sites):
time_unix_nano: v.as_integer().unwrap_or(0) as u64,
// Chrono decomposition (4 duplicate blocks):
let secs = (nanos / 1_000_000_000) as i64;
let nsecs = (nanos % 1_000_000_000) as u32;
Utc.timestamp_opt(secs, nsecs).unwrap()
```

**After**:
```rust
Value::Integer(OtlpTimestamp::from_nanos(self.record.time_unix_nano).to_vrl())
time_unix_nano: OtlpTimestamp::from_vrl(v.as_integer().unwrap_or(0)).as_nanos(),
OtlpTimestamp::from_nanos(nanos).to_chrono()
```

### `OtlpCount`

```rust
/// OTLP `uint32` field (counts, flags).
///
/// Wraps proto fields like `dropped_attributes_count`, `dropped_events_count`,
/// `dropped_links_count`, `flags`, `span_flags`. Values originate as `u32` in
/// the proto, round-trip through VRL `Value::Integer(i64)`, and return to `u32`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct OtlpCount(u32);

impl OtlpCount {
    #[allow(dead_code)]
    pub(crate) fn from_proto(v: u32) -> Self {
        Self(v)
    }

    pub(crate) fn as_proto(self) -> u32 {
        self.0
    }

    #[allow(dead_code)]
    pub(crate) fn to_vrl(self) -> i64 {
        i64::from(self.0)  // lossless: u32 fits in i64
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "proto field is u32; value round-trips through i64"
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "proto field is u32; value round-trips through i64"
    )]
    pub(crate) fn from_vrl(v: i64) -> Self {
        Self(v as u32)
    }
}
```

**Before** (~24 sites):
```rust
// Ingestion — widening (11 sites):
Value::Integer(res.dropped_attributes_count as i64)
Value::Integer(self.span.dropped_links_count as i64)
Value::Integer(l.flags as i64)
// Emission — narrowing (13 sites):
dropped_attributes_count: v.as_integer().unwrap_or(0) as u32,
flags: v.as_integer().unwrap_or(0) as u32,
```

**After**:
```rust
Value::Integer(OtlpCount::from_proto(res.dropped_attributes_count).to_vrl())
dropped_attributes_count: OtlpCount::from_vrl(v.as_integer().unwrap_or(0)).as_proto(),
```

### `OtlpEnumField`

```rust
/// OTLP `int32` field (enum discriminants, scale, offset).
///
/// Wraps proto fields that use `int32` on the wire and round-trip through
/// VRL `Value::Integer(i64)`. Covers both enum discriminants
/// (`severity_number`, `kind`, `status_code`) and arithmetic parameters
/// (`scale`, `offset`). Named after the majority use case (enums); all
/// share the same `i32 ↔ i64` conversion semantics.
///
/// Note: `aggregation_temporality` is compared directly as `i32` (not
/// converted to VRL), so it uses function-level `#[expect]` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct OtlpEnumField(i32);

impl OtlpEnumField {
    #[allow(dead_code)]
    pub(crate) fn from_proto(v: i32) -> Self {
        Self(v)
    }

    pub(crate) fn as_proto(self) -> i32 {
        self.0
    }

    #[allow(dead_code)]
    pub(crate) fn to_vrl(self) -> i64 {
        i64::from(self.0)  // lossless: i32 fits in i64
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "proto field is i32; value round-trips through i64"
    )]
    pub(crate) fn from_vrl(v: i64) -> Self {
        Self(v as i32)
    }
}
```

**Before** (~16 sites):
```rust
// Ingestion — widening (9 sites):
Value::Integer(self.record.severity_number as i64)
Value::Integer(self.span.kind as i64)
Value::Integer(status.code as i64)
// Emission — narrowing (7 sites):
self.record_mut().severity_number = n as i32;
self.span.kind = n as i32;
status.code = n as i32;
```

**After**:
```rust
Value::Integer(OtlpEnumField::from_proto(self.record.severity_number).to_vrl())
self.record_mut().severity_number = OtlpEnumField::from_vrl(n).as_proto();
```

### `OtlpMetricInt`

```rust
/// OTLP integer metric value (`NumberDataPointValue::AsInt`).
///
/// Wraps `sfixed64` metric data-point values that need `f64` conversion
/// for metric math (aggregation, histogram boundaries, etc.).
/// Precise for |v| ≤ 2^53 (~9 quadrillion).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct OtlpMetricInt(i64);

impl OtlpMetricInt {
    pub(crate) fn from_proto(v: i64) -> Self {
        Self(v)
    }

    #[allow(dead_code)]
    pub(crate) fn as_proto(self) -> i64 {
        self.0
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "precise for |v| ≤ 2^53; OTLP metric values"
    )]
    pub(crate) fn to_f64(self) -> f64 {
        self.0 as f64
    }
}
```

**Before** (~10 sites):
```rust
NDPValue::AsInt(v) => Some(*v as f64),
NDPValue::AsInt(i) => *i as f64,
```

**After**:
```rust
NDPValue::AsInt(v) => Some(OtlpMetricInt::from_proto(*v).to_f64()),
NDPValue::AsInt(i) => OtlpMetricInt::from_proto(*i).to_f64(),
```

### What does NOT get a newtype

- **Proto enum → i32 comparisons** (`AggregationTemporality::Delta as i32`):
  23 sites in `otel_metric.rs`. These are prost-generated enum discriminant
  casts, not VRL ↔ OTLP boundary crossings. They compare a Rust enum to the
  proto `i32` field. These get a function-level `#[expect]` on the enclosing
  function since they cluster together.

- **Exponential histogram bit math** (`(bits >> 52) as i32`,
  `value.ln().floor() as i32`, `(1u64 << scale) as f64`): Algorithm-specific
  casts that don't cross the VRL ↔ OTLP boundary. These keep `#[expect]` on the
  enclosing function per the [cast-safety-strategy](../../20260510_src-cast-remediation/adrs/20260510_cast-safety-strategy.md).

- **Set cardinality** (`set.len() as f64`): `usize → f64`, not an OTLP wire
  type. Gets a local `#[expect]` — only ~3 sites.

### Module placement

All four types live in `lib/sol-core/src/event/otel_conv.rs`, registered in the
event module as `pub(crate)`. This keeps them co-located with the OTLP
conversion code, without polluting the public API.

#### Re-enabling crate-wide allowed lints

`lib.rs` has two crate-wide allows inherited from upstream Vector:

```rust
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
```

These suppress the lints that `OtlpTimestamp` needs `#[expect]` to trigger
(`to_vrl` uses `cast_possible_wrap`, `from_vrl`/`from_chrono` use
`cast_sign_loss`). Under a crate-wide `#![allow]`, `#[expect]` never fires,
producing an `unfulfilled_lint_expectations` error (which is itself an error
under `#![deny(warnings)]`).

**Fix**: Re-enable both lints at the module level in `otel_conv.rs`:

```rust
#![deny(clippy::cast_possible_wrap)]
#![deny(clippy::cast_sign_loss)]
```

This overrides the crate-wide `#![allow]` for this module only. The
`#[expect]` annotations on `OtlpTimestamp` methods then work as designed —
they acknowledge the lint locally and will warn if a future refactor removes
the cast. No other modules are affected.

### `#[allow(dead_code)]` on API surface methods

Several `pub(crate)` methods (`to_chrono`, `from_chrono`, `from_proto`,
`to_vrl`, `as_proto`) are not yet called outside `otel_conv.rs` — they exist
as part of the boundary-type API for future consumers. These use
`#[allow(dead_code)]` rather than `#[expect(dead_code)]` because `#[expect]`
would fire an `unfulfilled_lint_expectations` error the moment a caller is
added, which is the opposite of the desired behavior. `#[allow]` silently
permits both states (used and unused) without churn.

## Consequences

Full breakdown of all 179 cast sites:

| Treatment | Sites | Example |
|---|---|---|
| Typed newtypes | ~87 | `OtlpTimestamp::from_nanos(v).to_vrl()` |
| Direct `.into()` | ~59 | `f64::from(rate)`, `u64::from(count)` — lossless widening not through VRL |
| Function-level `#[expect]` | ~33 | Proto enum discriminants, histogram math, set cardinality |
| **Total** | **~179** | |

- **~87 cast sites** replaced by typed method calls — zero `#[expect]` at call
  sites
- **~59 lossless widening casts** (e.g., `u32 → f64` for rate, `u32 → u64`)
  replaced by direct `.into()` — no newtype needed, the type system enforces
  safety
- **4 duplicate timestamp-decomposition blocks** collapse into
  `OtlpTimestamp::to_chrono()`
- Cast safety reasoning documented in **4 type implementations** instead of
  scattered annotations
- **Primitive obsession eliminated**: each proto wire type is a distinct Rust
  type. A `u64` byte counter cannot flow through `OtlpTimestamp`, a `u32` port
  number cannot flow through `OtlpCount`
- **~33 remaining cast sites** (proto enum discriminants + histogram math +
  cardinality) keep function-level `#[expect]` — they cluster tightly and
  don't benefit from extraction
- Adding a new OTLP field automatically gets the correct conversion by choosing
  the right newtype
- All four types are zero-cost: `#[derive(Copy)]`, no heap allocation, inlined
  by the compiler
