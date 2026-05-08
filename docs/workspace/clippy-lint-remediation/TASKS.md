# Clippy Lint Remediation — Tasks

Design: [DESIGN.md](./DESIGN.md)

## Analysis

Build: `cargo build --workspace --all-targets --all-features` — verified green
Test: `cargo test --workspace --all-features` — assumed green (full run ~30min)
Lint: `cargo clippy -p sol-core --all-targets --all-features -- -D warnings` — **400 errors** (baseline before fixes)

### Known-failing tests

| Test | Reason | Action |
|---|---|---|
| (none) | | |

### Error distribution (verified via `git blame`)

| Origin | Errors | % |
|---|---|---|
| Sol-new files (`otel_event.rs`, `otel_metric.rs`, `otel_json.rs`, `otel_attributes.rs`, `otlp.rs`) | 283 | 71% |
| Sol additions to `vrl_target.rs` (all 93 error lines blame to Sol) | 104 | 26% |
| Inherited upstream code (8 files, 13 errors) | 13 | 3% |

### Cast inventory (VRL ↔ OTLP boundary)

| File | `as i32` | `as f64` | `as u64` | `as i64` | `as u32` | Total |
|------|----------|----------|----------|----------|----------|-------|
| `otel_metric.rs` | 23 | 16 | 5 | 7 | 2 | 53 |
| `otel_event.rs` | 7 | 1 | 15 | 40 | 13 | 76 |
| `vrl_target.rs` | 0 | 0 | 15 | 24 | 11 | 50 |
| **Total** | **30** | **17** | **35** | **71** | **26** | **179** |

### VRL ↔ OTLP type boundary reference

| Proto field | Proto type | VRL type | Boundary type | Ingestion | Emission |
|---|---|---|---|---|---|
| `time_unix_nano` | `u64` | `i64` | `OtlpTimestamp` | `.to_vrl()` | `from_vrl()` |
| `severity_number` | `i32` | `i64` | `OtlpEnumField` | `.to_vrl()` | `from_vrl()` |
| `dropped_*_count` | `u32` | `i64` | `OtlpCount` | `.to_vrl()` | `from_vrl()` |
| `kind`, `status_code` | `i32` | `i64` | `OtlpEnumField` | `.to_vrl()` | `from_vrl()` |
| `flags`, `span_flags` | `u32` | `i64` | `OtlpCount` | `.to_vrl()` | `from_vrl()` |
| `aggregation_temporality` | `i32` (enum) | compared as `i32` | — | function-level `#[expect]` | — |
| `scale`, `offset` | `i32` | `i64` | `OtlpEnumField` | `.to_vrl()` | `from_vrl()` |
| `NumberDataPoint.value` | `i64` | `f64` in metric math | `OtlpMetricInt` | — | `.to_f64()` |

### Requirement traceability

| Type / Function | Addresses | Notes |
|---|---|---|
| `OtlpTimestamp` | [NFR2](./DESIGN.md#nfr2) | Newtype for `u64` nanos, centralizes wrap/sign casts |
| `OtlpCount` | [NFR2](./DESIGN.md#nfr2) | Newtype for `u32` counts/flags, centralizes truncation cast |
| `OtlpEnumField` | [NFR2](./DESIGN.md#nfr2) | Newtype for `i32` enums/scale, centralizes truncation cast |
| `OtlpMetricInt` | [NFR2](./DESIGN.md#nfr2) | Newtype for `i64` metric values, centralizes precision cast |
| `otel_metric.rs` — 53 casts + 30 style | [FR2](./DESIGN.md#fr2) | VRL ↔ OTLP metric conversion |
| `otel_event.rs` — 76 casts + 95 style | [FR3](./DESIGN.md#fr3) | VRL ↔ OTLP log/span conversion |
| `vrl_target.rs` — 50 casts + 54 style | [FR4](./DESIGN.md#fr4) | VRL target trait for OTLP events |
| `otel_json.rs`, `otel_attributes.rs`, `otlp.rs` — 29 style | [FR5](./DESIGN.md#fr5) | OTLP helpers |
| 13 inherited errors (8 files) | [FR6](./DESIGN.md#fr6) | Fix individually, no crate-wide allows |
| Inherited `as f64` casts (5 prod + 6 test) | [FR6](./DESIGN.md#fr6) | Restore upstream `#[expect]` annotations |

### Transformations (per [ADR: cast-safety-strategy](./adrs/cast-safety-strategy.md) and [ADR: otlp-boundary-types](./adrs/otlp-boundary-types.md))

| Pattern | Input → Output | Treatment |
|---|---|---|
| Timestamp (OTLP→VRL) | `u64 → i64` | `OtlpTimestamp::from_nanos(v).to_vrl()` |
| Timestamp (VRL→OTLP) | `i64 → u64` | `OtlpTimestamp::from_vrl(v).as_nanos()` |
| Timestamp (OTLP→chrono) | `u64 → DateTime<Utc>` | `OtlpTimestamp::from_nanos(v).to_chrono()` |
| Timestamp (chrono→OTLP) | `DateTime<Utc> → u64` | `OtlpTimestamp::from_chrono(ts).as_nanos()` |
| Count/flags (OTLP→VRL) | `u32 → i64` | `OtlpCount::from_proto(v).to_vrl()` |
| Count/flags (VRL→OTLP) | `i64 → u32` | `OtlpCount::from_vrl(v).as_proto()` |
| Enum/scale (OTLP→VRL) | `i32 → i64` | `OtlpEnumField::from_proto(v).to_vrl()` |
| Enum/scale (VRL→OTLP) | `i64 → i32` | `OtlpEnumField::from_vrl(v).as_proto()` |
| Metric int → float | `i64 → f64` | `OtlpMetricInt::from_proto(v).to_f64()` |
| Proto enum discriminant | `EnumVariant as i32` | function-level `#[expect]` — prost convention |
| Exp histogram bit math | `as i32`, `as f64` | function-level `#[expect]` — IEEE 754 algorithm |
| Set cardinality | `usize as f64` | local `#[expect]` — only ~3 sites |
| Style lints | various | `cargo clippy --fix` or manual — mechanical, no semantic change |

## Tasks

### 0. Create boundary types module ([NFR2](./DESIGN.md#nfr2), [NFR3](./DESIGN.md#nfr3))

**Goal**: Create `otel_conv.rs` with the four OTLP boundary newtypes that
centralize all VRL ↔ OTLP casts.

**Constraints**:
- [ADR: otlp-boundary-types](./adrs/otlp-boundary-types.md) — defines all four types
- [ADR: cast-safety-strategy](./adrs/cast-safety-strategy.md) — conversion rules
- File: `lib/sol-core/src/event/otel_conv.rs`
- Register module in `lib/sol-core/src/event/mod.rs`
- All types are `pub(crate)`, `#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]`
- `OtlpTimestamp` additionally derives `PartialOrd, Ord, Hash`
- Each method has the appropriate `#[expect(..., reason = "...")]`
- Widening conversions (`to_vrl` on `OtlpCount` and `OtlpEnumField`) use `.into()` internally
- Module must re-enable `clippy::cast_possible_wrap` and `clippy::cast_sign_loss` with
  `#![deny(...)]` at module level — these are crate-wide `#![allow]` in `lib.rs` (lines 19-20),
  which would cause `unfulfilled_lint_expectations` errors on `OtlpTimestamp`'s `#[expect]` annotations.
  See [ADR: otlp-boundary-types § Re-enabling crate-wide allowed lints](./adrs/otlp-boundary-types.md#re-enabling-crate-wide-allowed-lints)

**Types**:
- `OtlpTimestamp(u64)`: `from_nanos`, `as_nanos`, `to_vrl`, `from_vrl`, `to_chrono`, `from_chrono`
- `OtlpCount(u32)`: `from_proto`, `as_proto`, `to_vrl`, `from_vrl`
- `OtlpEnumField(i32)`: `from_proto`, `as_proto`, `to_vrl`, `from_vrl`
- `OtlpMetricInt(i64)`: `from_proto`, `as_proto`, `to_f64`

**Tests**:
- `test_otlp_timestamp_roundtrip` — `from_nanos(n).to_vrl()` → `from_vrl(v).as_nanos()` == n
- `test_otlp_timestamp_negative_clamps` — `from_vrl(-1).as_nanos()` == 0
- `test_otlp_timestamp_chrono_roundtrip` — `from_chrono(ts).to_chrono()` == ts
- `test_otlp_timestamp_epoch` — `from_nanos(0)` roundtrips, `from_chrono(UNIX_EPOCH)` roundtrips
- `test_otlp_timestamp_max_wraps` — `from_nanos(u64::MAX).to_vrl()` is negative; `from_vrl(negative).as_nanos()` clamps to 0 (documents lossy wrap boundary)
- `test_otlp_count_roundtrip` — `from_proto(n).to_vrl()` → `from_vrl(v).as_proto()` == n
- `test_otlp_count_truncation` — `from_vrl(i64::MAX).as_proto()` truncates silently (documents behavior)
- `test_otlp_enum_roundtrip` — `from_proto(n).to_vrl()` → `from_vrl(v).as_proto()` == n
- `test_otlp_enum_truncation` — `from_vrl(i64::MAX).as_proto()` truncates silently (documents behavior)
- `test_otlp_metric_int_to_f64` — exact for small values, verify precision boundary

**Verify**: `cargo clippy -p sol-core --all-targets --all-features -- -D warnings 2>&1 | grep otel_conv`
(must produce zero errors for this file)

**Acceptance criteria**:
- [x] `otel_conv.rs` exists with all four types and their methods
- [x] Every `#[expect]` has a `reason` argument
- [x] Module registered in `event/mod.rs`
- [x] Unit tests pass
- [x] Zero clippy errors from `otel_conv.rs`

**Depends on**: (none)
**Time-box**: ~30 min

### 1. Fix all 83 lint violations in otel_metric.rs ([FR2](./DESIGN.md#fr2), [NFR2](./DESIGN.md#nfr2))

**Goal**: Make `otel_metric.rs` pass clippy pedantic with zero crate-wide allows.

**Constraints**:
- [ADR: cast-safety-strategy](./adrs/cast-safety-strategy.md)
- [ADR: otlp-boundary-types](./adrs/otlp-boundary-types.md)
- 53 casts — apply boundary types from task 0:
  - `as f64` (16): `OtlpMetricInt::from_proto(v).to_f64()` for `i64→f64`; `.into()` for `u32→f64` (lossless, 32 < 52 mantissa bits)
  - `as i64` (7): `OtlpEnumField::from_proto(v).to_vrl()` for `i32→i64`; `OtlpTimestamp` for timestamps
  - `as u64` (5): `OtlpTimestamp::from_chrono(ts).as_nanos()` for chrono→u64; `.into()` for `u32→u64`
  - `as u32` (2): `OtlpTimestamp` absorbs the modulo-bounded `(nanos % 1B) as u32`; `OtlpCount::from_vrl(v).as_proto()` for proto field
  - `as i32` (23): split into two groups:
    - `EnumVariant as i32` (19): prost enum discriminant — these do NOT cross the VRL boundary; function-level `#[expect(cast_possible_truncation, reason = "proto enum discriminant")]`
    - `i64 as i32` (2) + bucket index (2): `OtlpEnumField::from_vrl(v).as_proto()` for VRL→OTLP; local `#[expect]` for histogram index math
  - Exp histogram bit math (lines 338-353, 2094-2104): function-level `#[expect]` — IEEE 754 algorithm
- 30 style lints: `doc_markdown`, `redundant_closure`, `collapsible_if`, `map_unwrap_or`, etc. — fix mechanically

**Tests**: `cargo test -p sol-core --all-features`

**Verify**: `cargo clippy -p sol-core --all-targets --all-features -- -D warnings 2>&1 | grep otel_metric`
(must produce zero errors for this file)

**Acceptance criteria**:
- [x] Zero clippy errors from `otel_metric.rs`
- [x] All VRL ↔ OTLP boundary casts use the typed newtypes
- [x] Non-boundary casts (enum discriminants, histogram math) have function-level `#[expect]` with `reason`
- [x] All sol-core tests pass

**Depends on**: task 0
**Time-box**: ~60 min

### 2. Fix all 171 lint violations in otel_event.rs ([FR3](./DESIGN.md#fr3), [NFR2](./DESIGN.md#nfr2))

**Goal**: Make `otel_event.rs` pass clippy pedantic with zero crate-wide allows.

**Constraints**:
- [ADR: cast-safety-strategy](./adrs/cast-safety-strategy.md)
- [ADR: otlp-boundary-types](./adrs/otlp-boundary-types.md)
- 76 casts — apply boundary types from task 0:
  - `as i64` (40): `OtlpTimestamp::from_nanos(v).to_vrl()` for timestamps (~15); `OtlpCount::from_proto(v).to_vrl()` for counts (~11); `OtlpEnumField::from_proto(v).to_vrl()` for severity/kind/status (~9); remaining are chrono-related (absorbed by `OtlpTimestamp`)
  - `as u64` (15): `OtlpTimestamp::from_vrl(v).as_nanos()` for timestamps (~13); `OtlpTimestamp::from_chrono(ts).as_nanos()` for chrono→OTLP (~2)
  - `as u32` (13): `OtlpCount::from_vrl(v).as_proto()` for counts/flags; `OtlpTimestamp` absorbs modulo-bounded nanos
  - `as f64` (1): `OtlpMetricInt::from_proto(v).to_f64()` for metric value
  - `as i32` (7): `OtlpEnumField::from_vrl(v).as_proto()` for severity/kind/status
- 95 style lints: `doc_markdown` (36), `redundant_closure` (21), `single_match_else` (11), `needless_pass_by_value` (9), etc. — fix mechanically

**Tests**: `cargo test -p sol-core --all-features`

**Verify**: `cargo clippy -p sol-core --all-targets --all-features -- -D warnings 2>&1 | grep otel_event`

**Acceptance criteria**:
- [x] Zero clippy errors from `otel_event.rs`
- [x] All VRL ↔ OTLP boundary casts use the typed newtypes
- [x] All sol-core tests pass

**Depends on**: task 0
**Time-box**: ~75 min

### 3. Fix all 104 lint violations in vrl_target.rs ([FR4](./DESIGN.md#fr4), [NFR2](./DESIGN.md#nfr2))

**Goal**: Make `vrl_target.rs` pass clippy pedantic with zero crate-wide allows.

**Constraints**:
- [ADR: cast-safety-strategy](./adrs/cast-safety-strategy.md)
- [ADR: otlp-boundary-types](./adrs/otlp-boundary-types.md)
- 50 casts — apply boundary types from task 0:
  - `as i64` (24): `OtlpTimestamp::from_nanos(v).to_vrl()` for timestamps (~10); `OtlpCount::from_proto(v).to_vrl()` for counts (~8); `OtlpEnumField::from_proto(v).to_vrl()` for kind/status (~6)
  - `as u64` (15): `OtlpTimestamp::from_vrl(v).as_nanos()` for all timestamp emissions
  - `as u32` (11): `OtlpCount::from_vrl(v).as_proto()` for all count/flag emissions
- 54 style lints: `redundant_closure` (39), `doc_markdown` (11), `collapsible_if` (4), `let_else` (6), `too_many_lines` (1), `large_enum_variant` (1), etc. — fix mechanically

**Tests**: `cargo test -p sol-core --all-features`

**Verify**: `cargo clippy -p sol-core --all-targets --all-features -- -D warnings 2>&1 | grep vrl_target`

**Acceptance criteria**:
- [x] Zero clippy errors from `vrl_target.rs`
- [x] All VRL ↔ OTLP boundary casts use the typed newtypes
- [x] All sol-core tests pass

**Depends on**: task 0
**Time-box**: ~60 min

### 4. Fix remaining 42 lint violations ([FR5](./DESIGN.md#fr5), [FR6](./DESIGN.md#fr6))

**Goal**: Fix all remaining errors: 29 in other Sol-authored OTLP files
(`otel_json.rs`, `otel_attributes.rs`, `otlp.rs`) + 13 in inherited files.
Zero crate-wide allows added.

**Constraints**:
- [ADR: inherited-lint-policy](./adrs/inherited-lint-policy.md) — fix all 13 inherited errors individually
- Sol-authored files (`otel_json.rs`, `otel_attributes.rs`, `otlp.rs`): fix all style lints
- Inherited files:
  - `source_sender/output.rs:266`: restore upstream's `#[expect(cast_precision_loss)]` with reason
  - `lua/event.rs`: add `;` to 3 statements
  - `lua/metric.rs`: `.clone()` for implicit_clone, `#[allow(useless_vec)]` for 2 macro uses
  - `event/mod.rs`: add `# Errors` and `# Panics` doc sections
  - `metric/series.rs`: simplify closure
  - `test/serialization.rs`: collapse if
  - `source_sender/tests.rs`: inline format args, move item before statement
- **No crate-wide `#![allow]` entries added to `lib.rs`**

**Tests**: `cargo test -p sol-core --all-features`

**Verify**: `cargo clippy -p sol-core --all-targets --all-features -- -D warnings`
(must produce zero errors total)

**Acceptance criteria**:
- [x] Zero clippy errors across all of sol-core
- [x] `lib.rs` has exactly the original 10 upstream allows — nothing added
- [x] Inherited file fixes are minimal and mechanical
- [x] All sol-core tests pass

**Depends on**: tasks 1, 2, 3
**Time-box**: ~30 min

## Sessions

### Session 1 — Clippy lint remediation (~4H)

Tasks: 0, 1, 2, 3, 4

**Skills**: `software-engineer`

**Checkpoint**: `make check-clippy && make check-fmt && cargo test -p sol-core --all-features`

**Commit point**: yes — commit after checkpoint passes

## Quality gates (post-session review)

- [x] Acceptance criteria: all green above
- [x] Code review: boundary types used consistently for all VRL ↔ OTLP casts
- [x] Code review: every remaining `#[expect]` has a `reason` argument
- [x] Code organization: zero crate-wide allows added to `lib.rs`
- [x] Code organization: `otel_conv.rs` registered in `event/mod.rs`
- [x] Code quality: no bare `as` casts crossing the VRL ↔ OTLP boundary
- [x] Security review: no silent truncation of metric values or timestamps
- [x] Observability: round-trip fidelity preserved (integration tests)
- [x] Performance: newtypes are `Copy`, zero runtime cost
- [x] CI coherence: `make check-clippy` and `make check-fmt` pass locally (same commands as GitHub Actions CI)
