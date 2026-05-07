---
status: draft
---
# Numeric conversion conventions for sinks, sources, and transforms

Addresses: [FR1](../DESIGN.md#fr1), [NFR2](../DESIGN.md#nfr2), [NFR4](../DESIGN.md#nfr4)

## Problem

Unchecked `as` casts are pervasive across the entire Sol codebase, not just
`sol-core`. A survey of `src/` (sinks, sources, transforms) reveals:

| Location | `as f64` | `as u32/i32` | `as u64/i64` | Notes |
|---|---|---|---|---|
| `src/sinks/` (26 crates) | ~40 | ~10 | ~20 | Metric encoding, timestamp conversion |
| `src/sources/` (20 crates) | ~50 | ~10 | ~15 | host_metrics alone has 30+ `as f64` |
| `src/transforms/` (5 crates) | ~15 | ~5 | ~5 | reduce, sample, tail_sampling |

The main crate (`src/lib.rs`) does **not** enforce `deny(clippy::pedantic)` —
inheriting upstream Vector's choice. This means the 150+ casts in sinks,
sources, and transforms are currently **unchecked by clippy**. They fire no
warnings, even though many have the same precision/truncation risks as the
casts in `sol-core`.

### Why this matters

Sol is an observability pipeline. Data flows through sources → transforms →
sinks. If a source reads `u64` host metrics and casts them to `f64`, the
precision loss propagates all the way to the sink and ultimately to the user's
dashboard. The entire pipeline must be precision-aware, not just the core
library.

### Three categories of casts across the codebase

**1. Protocol boundary casts** (sinks/sources):
External protocols define their own types. Converting to/from Vector's internal
representation always requires casts.

Examples:
- OTLP: `u64` timestamps, `u32` counts, `i32` enums ↔ `Value::Integer(i64)`
- Datadog: `f64` metric values, `i64` intervals
- Prometheus: `f64` gauge/counter values
- StatsD: `u64` counters, `f64` gauges

**2. Metric value casts** (sources, transforms):
System metrics (CPU, disk, memory) are often reported as `u64` or `usize` but
Vector's `MetricValue` expects `f64`. These are the most common casts.

Examples:
- `counter.read_bytes() as f64` — `u64 → f64`, precision loss if > 2^53 bytes (~9PB)
- `connections.active as f64` — typically small values, safe in practice
- `process.memory() as f64` — `u64 → f64`, could exceed 2^53 on large machines

**3. Internal computation casts** (transforms):
Arithmetic operations that mix integer and float types.

Examples:
- `i as f64` in reduce/merge strategies — `i64 → f64` precision loss
- `(ratio * u64::MAX as f64) as u64` — chained cast for hash threshold

## Options

| Option | Pros | Cons |
|---|---|---|
| A: Enable `deny(pedantic)` on `src/` now | All casts surfaced immediately | Thousands of errors across the entire crate, far too large for one workspace |
| B: Keep `src/` without pedantic, establish conventions for new code | No immediate breakage, gradual improvement | Existing unchecked casts remain silent |
| C: Enable only `deny(clippy::cast_precision_loss)` and `deny(clippy::cast_possible_truncation)` on `src/` | Targets the safety-relevant lints without style noise | Still hundreds of errors, but scoped to precision/truncation only |
| D: Establish conventions now, enforce per-crate as sinks/sources are touched | Incremental, risk-proportional | Slower coverage, relies on discipline |

## Decision

**Option D**: Establish conventions now, enforce incrementally per crate.

Rationale:
- Enabling `deny(pedantic)` on `src/` would trigger thousands of errors and is
  out of scope for this workspace
- The conventions established in [cast-safety-strategy](./cast-safety-strategy.md)
  for the VRL ↔ OTLP boundary apply equally to all protocol boundaries
- Sinks and sources can adopt `#[deny(clippy::cast_precision_loss)]` and
  `#[deny(clippy::cast_possible_truncation)]` at the module level as they are
  modified, without a big-bang migration

### Conventions for writing sinks and sources

#### Rule 1: Widening casts use `.into()`, never `as`

```rust
// GOOD: explicit widening, compiler-checked
let count: i64 = i64::from(proto_count_u32);

// BAD: silent, same result but not checked
let count: i64 = proto_count_u32 as i64;
```

#### Rule 2: Narrowing casts use `#[expect]` with a reason

When a protocol requires writing a narrower type than Vector's internal
representation:

```rust
// GOOD: documents the assumption
#[expect(
    clippy::cast_possible_truncation,
    reason = "proto field is u32; value originated as u32, round-trips through i64"
)]
let dropped_count = value as u32;

// BAD: silent truncation
let dropped_count = value as u32;
```

#### Rule 3: Integer → float casts document precision bounds

```rust
// GOOD: documents the precision boundary
#[expect(
    clippy::cast_precision_loss,
    reason = "precise for |v| ≤ 2^53; metric counter values"
)]
let gauge_value = counter as f64;

// BAD: silent precision loss
let gauge_value = counter as f64;
```

#### Rule 4: System metric sources should validate value ranges

For sources that read system metrics (host_metrics, nginx_metrics, etc.),
document the practical range of values:

```rust
// Disk read bytes: u64, but current drives max ~30TB/s = ~3×10^13 < 2^53
// Precision loss only possible after ~104 days at max throughput
#[expect(clippy::cast_precision_loss, reason = "disk bytes < 2^53 in practice")]
let bytes = counter.read_bytes() as f64;
```

#### Rule 5: Timestamp conversions follow the VRL ↔ OTLP pattern

The `u64` nanos ↔ `i64` ↔ chrono pattern is the same everywhere:

```rust
// Nanos to chrono: safe until year 2262
#[expect(clippy::cast_possible_wrap, reason = "nanos fit in i64 until year 2262")]
let timestamp = Utc.timestamp_nanos(nanos as i64);

// Chrono to nanos: non-negative by domain
#[expect(clippy::cast_sign_loss, reason = "timestamps are non-negative")]
let nanos = ts.timestamp_nanos_opt().unwrap_or(0) as u64;
```

### Enforcement plan

| Phase | Scope | When |
|---|---|---|
| **Phase 1** (this workspace) | `sol-core` — all 400 errors | Now |
| **Phase 2** (future) | Enable `deny(cast_precision_loss, cast_possible_truncation)` on `src/` | When sinks/sources are next modified |
| **Phase 3** (future) | Enable `deny(pedantic)` on `src/` | When inherited code is sufficiently cleaned up |

Phase 2 can be done incrementally: add `#[deny(clippy::cast_precision_loss)]`
at the module level when a sink or source is modified for any reason. This
ensures new code follows the conventions without requiring a big-bang migration.

## Consequences

- New sinks/sources must follow these conventions from day one
- Existing sinks/sources adopt the conventions when they are next modified
- The VRL ↔ OTLP cast strategy serves as the reference implementation
- No immediate large-scale migration required
- Precision and truncation lints are enforced in `sol-core` now, and will
  expand to `src/` incrementally
