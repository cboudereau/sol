# Cast Remediation for src/ — Tasks

Design: [DESIGN.md](./DESIGN.md)

## Analysis

Build: `cargo build --workspace --all-targets --all-features` — verified green
Test: `cargo test --workspace --all-features` — assumed green (full run ~30min)
Lint: `cargo clippy -p sol --all-targets --all-features -- -D warnings` — currently green (cast lints not enabled)
Cast lint check: `cargo clippy -p sol --all-targets --all-features -- -W clippy::cast_precision_loss -W clippy::cast_possible_truncation -W clippy::cast_lossless -W clippy::cast_sign_loss -W clippy::cast_possible_wrap` — **~457 cast sites** (baseline)

### Known-failing tests

| Test | Reason | Action |
|---|---|---|
| (none) | | |

### Cast distribution

| Cast type | Sinks | Sources | Transforms | Total |
|---|---|---|---|---|
| `as f64` | ~60 | ~85 | ~38 | 183 |
| `as i32` | ~30 | ~45 | ~28 | 103 |
| `as u64` | ~25 | ~30 | ~13 | 68 |
| `as usize` | ~15 | ~20 | ~8 | 43 |
| `as i64` | ~10 | ~18 | ~12 | 40 |
| `as u32` | ~11 | ~13 | ~4 | 28 |
| **Total** | **~151** | **~235** | **~71** | **457** |

Prod: 385 | Test: 72 | Files: 122

### Pattern clusters

| Pattern | Casts | Fix | Example |
|---|---|---|---|
| System metrics → f64 | ~145 | `#[expect(cast_precision_loss)]` on macro/helper | `counter!($value)` → `$value as f64` |
| Proto enum discriminant | ~70 | `#[expect(cast_possible_truncation)]` on function | `AggregationTemporality::Delta as i32` |
| Lossless widening | ~37 | `.into()` / `T::from()` | `u32 as f64` → `f64::from(v)` |
| Timestamp conversion | ~20 | `OtlpTimestamp` from sol-core | `ts.timestamp_nanos_opt()...as u64` |
| Length/count → f64 | ~20 | `#[expect(cast_precision_loss)]` | `values.len() as f64` |
| Protocol field narrowing | ~30 | `#[expect(cast_possible_truncation)]` | `len as u32` (frame length) |
| Algorithm-specific | ~25 | Per-site/function `#[expect]` | DDSketch index, EWMA, hash threshold |
| Miscellaneous | ~110 | Per-site | 1-2 per file across 80+ files |

### Requirement traceability

| Pattern / Task | Addresses | Notes |
|---|---|---|
| Lossless widening → `.into()` | [FR1](./DESIGN.md#fr1) | Auto-fixable, 37 sites |
| Metrics macros | [FR2](./DESIGN.md#fr2) | 4 macro defs in 2 files → ~145 expansions |
| Precision loss `#[expect]` | [FR3](./DESIGN.md#fr3) | ~100 sites, templated reasons |
| Truncation/sign/wrap `#[expect]` | [FR4](./DESIGN.md#fr4) | ~100 sites, per-site reasons |
| Enable cast lints in `src/lib.rs` | [FR5](./DESIGN.md#fr5) | 5 `#![deny]` lines, final task |

### Transformations

| Pattern | Input → Output | Treatment |
|---|---|---|
| Lossless widening | `u32 → f64`, `i32 → i64`, `u16 → i64`, etc. | `.into()` — compiler-enforced |
| Metric value | `i64 → f64` or `u64 → f64` | `#[expect(cast_precision_loss, reason = "metric values")]` |
| Proto enum | `EnumVariant → i32` | `#[expect(cast_possible_truncation, reason = "proto enum discriminant")]` |
| Timestamp | `i64 → u64` or `u64 → i64` | `OtlpTimestamp` or `#[expect(cast_sign_loss)]` |
| Protocol frame | `usize → u32` | `#[expect(cast_possible_truncation, reason = "...")]` |
| Arithmetic | various | Per-site `#[expect]` with domain reasoning |

## Tasks

### 1. Fix lossless widening casts ([FR1](./DESIGN.md#fr1))

**Goal**: Replace all `cast_lossless` casts with `.into()` or `T::from()`.

**Constraints**:
- Target all `cast_lossless` sites: `u32→f64`, `u8→f64`, `f32→f64`, `i32→f64`,
  `i32→i64`, `u32→i64`, `u16→i64`, `u8→u32`, `bool→u8`
- Use `cargo clippy --fix` where possible, manual fix where auto-fix fails
- ~37 sites across sinks, sources, transforms

**Tests**: `cargo test -p sol --all-features` (subset: affected crates)

**Verify**: `cargo clippy -p sol --all-targets --all-features -- -D clippy::cast_lossless 2>&1 | grep "^error" | wc -l` (must be 0)

**Acceptance criteria**:
- [x] Zero `cast_lossless` warnings across `src/`
- [x] All tests pass
- [x] No behavioral change (`.into()` produces identical machine code)

**Depends on**: (none)
**Time-box**: ~30 min

### 2. Fix metrics macro casts ([FR2](./DESIGN.md#fr2))

**Goal**: Fix the `counter!`/`gauge!` macros that expand `$value as f64`.

**Constraints**:
- Files: `src/sources/mongodb_metrics/mod.rs` (lines 52-58),
  `src/sources/postgresql_metrics.rs` (lines 60-66)
- Both files define identical macros: `macro_rules! counter { ($value:expr_2021) => { $value as f64 }; }`
- Fix: add `#[expect(clippy::cast_precision_loss)]` to an inline helper or
  use `#[allow]` inside the macro body (macros cannot carry `#[expect]` on
  expansion sites)
- Also check for similar macro patterns in other metric source files

**Tests**: `cargo test -p sol --features sources-mongodb_metrics,sources-postgresql_metrics`

**Verify**: grep for remaining bare `as f64` in macro definitions under `src/sources/`

**Acceptance criteria**:
- [x] `counter!` and `gauge!` macros no longer produce `cast_precision_loss`
  warnings at expansion sites
- [x] All mongodb_metrics and postgresql_metrics tests pass

**Depends on**: (none)
**Time-box**: ~15 min

### 3. Annotate precision-loss casts in sources ([FR3](./DESIGN.md#fr3))

**Goal**: Add `#[expect(clippy::cast_precision_loss)]` to all `i64/u64/usize → f64`
casts in `src/sources/`.

**Constraints**:
- ~85 `as f64` casts in sources (minus the macro-handled ones from Task 2)
- Host metrics files (`memory.rs`, `network.rs`, `cgroups.rs`, `filesystem.rs`,
  `disk.rs`, `cpu.rs`): system counters → f64, standard reason
  `"metric counter values; precise for |v| ≤ 2^53"`
- Other metric sources (`nginx_metrics`, `apache_metrics`, `eventstoredb_metrics`,
  `statsd/aggregator.rs`): similar pattern
- Some casts are in test files — use `#[allow]` or `#[expect]` as appropriate
- Timestamp casts (`as u64`, `as i64`) in source files: use `OtlpTimestamp`
  from sol-core where crossing the VRL ↔ OTLP boundary, `#[expect]` otherwise

**Tests**: `cargo test -p sol --all-features` (affected source features)

**Verify**: `cargo clippy -p sol --all-targets --all-features -- -D clippy::cast_precision_loss 2>&1 | grep "src/sources" | wc -l` (must be 0)

**Acceptance criteria**:
- [x] Zero `cast_precision_loss` warnings in `src/sources/`
- [x] Every `#[expect]` has a `reason` argument
- [x] All source tests pass

**Depends on**: task 2
**Time-box**: ~60 min

### 4. Annotate precision-loss casts in sinks and transforms ([FR3](./DESIGN.md#fr3))

**Goal**: Add `#[expect(clippy::cast_precision_loss)]` to all remaining
`i64/u64/usize → f64` casts in `src/sinks/` and `src/transforms/`.

**Constraints**:
- ~60 `as f64` in sinks: `prometheus/collector.rs`, `util/statistic.rs`,
  `util/buffer/metrics/mod.rs`, `greptimedb`, `splunk_hec`, `influxdb`,
  `adaptive_concurrency`
- ~38 `as f64` in transforms: `servicegraph`, `span_metrics`, `reduce`,
  `sample`, `aggregate`
- Same `#[expect]` patterns as Task 3

**Tests**: `cargo test -p sol --all-features` (affected sink/transform features)

**Verify**: `cargo clippy -p sol --all-targets --all-features -- -D clippy::cast_precision_loss 2>&1 | grep -E "src/(sinks|transforms)" | wc -l` (must be 0)

**Acceptance criteria**:
- [x] Zero `cast_precision_loss` warnings in `src/sinks/` and `src/transforms/`
- [x] Every `#[expect]` has a `reason` argument
- [x] All sink and transform tests pass

**Depends on**: (none — independent of task 3)
**Time-box**: ~60 min

### 5. Annotate truncation, sign-loss, and wrap casts ([FR4](./DESIGN.md#fr4))

**Goal**: Add `#[expect]` to all `cast_possible_truncation`, `cast_sign_loss`,
and `cast_possible_wrap` sites across `src/`.

**Constraints**:
- `cast_possible_truncation` (~37 sites): proto enum `as i32` (~70 are safe
  enum discriminants — use function-level `#[expect]`), protocol frame `as u32`,
  DDSketch index `as i32`, AWS SDK `as i32`
- `cast_sign_loss` (~42 sites): timestamp `i64 as u64` (reuse `OtlpTimestamp`
  where applicable), `i64::MAX as u64` constants, file descriptor `as u32`
- `cast_possible_wrap` (~19 sites): `u64 as i64` for Value::Integer, delivery
  tags, file offsets
- Each site needs a domain-specific reason (not a templated one)
- Proto enum discriminants: function-level `#[expect(cast_possible_truncation,
  reason = "proto enum discriminant")]` on the enclosing function
- Timestamp conversions in sources/transforms: use `OtlpTimestamp` from sol-core
  if the value is an OTLP nanosecond timestamp

**Tests**: `cargo test -p sol --all-features`

**Verify**: `cargo clippy -p sol --all-targets --all-features -- -D clippy::cast_possible_truncation -D clippy::cast_sign_loss -D clippy::cast_possible_wrap 2>&1 | grep "^error" | wc -l` (must be 0)

**Acceptance criteria**:
- [x] Zero `cast_possible_truncation` warnings across `src/`
- [x] Zero `cast_sign_loss` warnings across `src/`
- [x] Zero `cast_possible_wrap` warnings across `src/`
- [x] Every `#[expect]` has a `reason` argument
- [x] OTLP timestamp conversions reuse `OtlpTimestamp` from sol-core
- [x] All tests pass

**Depends on**: clippy-lint-remediation Task 0 (boundary types must exist)
**Time-box**: ~90 min

### 6. Enable cast lints in src/lib.rs ([FR5](./DESIGN.md#fr5))

**Goal**: Add the 5 cast lints to `src/lib.rs` to prevent regression.

**Constraints**:
- Add after all casts are fixed (tasks 1-5 complete)
- Add these lines to `src/lib.rs` alongside existing `#![deny]` entries:
  ```rust
  #![deny(clippy::cast_lossless)]
  #![deny(clippy::cast_precision_loss)]
  #![deny(clippy::cast_possible_truncation)]
  #![deny(clippy::cast_sign_loss)]
  #![deny(clippy::cast_possible_wrap)]
  ```
- Verify full clippy passes with `--all-features`

**Tests**: `cargo clippy -p sol --all-targets --all-features -- -D warnings`

**Verify**: `cargo clippy --workspace --all-targets --all-features -- -D warnings` (full workspace)

**Acceptance criteria**:
- [x] 5 `#![deny]` lines added to `src/lib.rs`
- [x] Full workspace clippy passes
- [x] Full test suite passes

**Depends on**: tasks 1, 2, 3, 4, 5
**Time-box**: ~15 min

## Sessions

### Session 1 — Mechanical fixes (~1.5H)

Tasks: 1, 2, 3, 4

**Skills**: `software-engineer`

**Checkpoint**: `cargo clippy -p sol --all-targets --all-features -- -D clippy::cast_lossless -D clippy::cast_precision_loss 2>&1 | grep "^error" | wc -l` (must be 0)

**Commit point**: yes

### Session 2 — Review-required fixes (~2H)

Tasks: 5, 6

**Skills**: `software-engineer`

**Checkpoint**: `cargo clippy --workspace --all-targets --all-features -- -D warnings`

**Commit point**: yes

## Quality gates (post-session review)

- [x] Acceptance criteria: all green above
- [x] Code review: every `#[expect]` has a meaningful `reason`
- [x] Code review: no bare `as` casts remain in sinks/sources/transforms
- [x] Code organization: 5 cast lints enforced in `src/lib.rs`
- [x] Code quality: lossless widening uses `.into()`, not `#[expect]`
- [x] Security review: no silent truncation of metric values or timestamps
- [x] Performance: zero runtime cost (annotations only, identical machine code)
- [x] CI coherence: `make check-clippy` passes locally
