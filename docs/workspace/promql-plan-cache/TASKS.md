# promql-plan-cache — Tasks

Design: [DESIGN.md](./DESIGN.md)

## Analysis

Build: `cargo build --lib` — green (baseline = backend-metrics-perf S3 checkpoint)
Test: `cargo test --lib querier::` — green: 244 passed, 0 failed, 2 ignored (1 pre-existing + `bench_cold_range_query_demo_scale`)
Lint: `make check-clippy` (`Makefile:478`) — green at the same checkpoint

### Known-failing tests
| Test | Reason | Action |
|---|---|---|
| 1 ignored in `querier::` (pre-existing) + `bench_cold_range_query_demo_scale` (by design) | ignored | ignore |
| 6 × `codecs encoding::format::json` under `-p codecs --all-features` | pre-existing, outside scope | ignore |

### Measured starting point (see [backend-metrics-perf VERIFY](../../20260716_backend-metrics-perf/VERIFY.md))
| Probe | Now | Target |
|---|---|---|
| Cold `rate()` range query (live, release) | ~250 ms | ≤ 80 ms repeated-shape ([NFR1](./DESIGN.md#nfr1)) |
| Bare selector range (live) | 58 ms | reference floor |
| Warm result-cache hit | 5 ms | unchanged |
| 20-query burst | ~1.4 s | ≤ 0.5 s ([NFR2](./DESIGN.md#nfr2)) |
| Instant selector | 385 ms | ≤ 90 ms ([FR3](./DESIGN.md#fr3)) |
| In-repo bench (debug): cold rate / bare / warm | 609 / 108 / 13 ms | tracked by the same bench |

### Domain model

```mermaid
classDiagram
    class PlanStageProfile {
        +Duration parse
        +Duration lower
        +Duration optimize
        +Duration physical
        +Duration execute
    }
    class PlanCacheKey {
        +String expr
        +i64 step_bucket
        +Vec~String~ table_set
        +u64 inventory_generation
    }
    class PlanCache {
        +get(PlanCacheKey) Option~CachedPlan~
        +insert(PlanCacheKey, CachedPlan)
    }
    class StalenessLookback {
        <<const/config>>
        +instant lower bound = time − lookback
    }
    PlanCache --> PlanCacheKey
    PlanStageProfile ..> PlanCache : FR1 data decides CachedPlan stage
```

### Requirement traceability
| Type / Fn | Addresses | Notes |
|---|---|---|
| `PlanStageProfile` + profiling seam | [FR1](./DESIGN.md#fr1) | Timing spans around the 5 pipeline stages; bench-visible |
| `PlanCache`, `PlanCacheKey` | [FR2](./DESIGN.md#fr2) | Mechanism per [ADR](./adrs/plan-cache-mechanism.md) after FR1 |
| `StalenessLookback` in `selector_base_df`/`hist_instant_scan` | [FR3](./DESIGN.md#fr3) | Prometheus 5 m semantics, configurable |
| per-shape parse-time margins in `inventory.rs` | [FR4](./DESIGN.md#fr4) | Removes query-time 1 h widening for exact-bounds files |

### Transformations
| Function | Input → Output | Invariant / Rule |
|---|---|---|
| profile run | (query shape) → `PlanStageProfile` table | Stages sum ≈ total (±10 %); reproducible on the fixture |
| `PlanCache::get/insert` | key → cached stage artefact | Hit ⇒ byte-identical response to miss; every key component has a changing-it-misses test |
| instant lower bound | `time → [time − lookback, time]` | Series with a sample inside lookback: identical result; staler series: absent (Prometheus semantics) |
| `parse_file_interval` (FR4) | `&Path → FileInterval` | Margins live entirely in parse-time intervals per shape; `scoped_files` widens by 0 |

## Tasks

### 1. Plan-pipeline profile ([FR1](./DESIGN.md#fr1))
**Goal**: Split the ~190 ms across parse/lower/optimize/physical/execute for `rate()`, bare selector, `histogram_quantile`; produce the ADR's decision table.
**Tests**: profiling seam unit test (stages sum sanity); extend `bench_cold_range_query_demo_scale` to print the stage table.
**Verify**: `cargo test --lib querier:: && cargo test --lib querier:: -- --ignored bench_cold_range_query_demo_scale --nocapture && make check-clippy`
**Acceptance criteria**:
- [ ] Stage table (fixture + live demo) recorded in this file and in the [ADR](./adrs/plan-cache-mechanism.md); ADR moved to `proposed` with a recommendation
**Depends on**: (none)
**Time-box**: ~75 min
**⚠ SESSION GATE**: autopilot PAUSES after task 1 — the human ratifies the ADR before tasks 2–3 run. Tasks 2–3 below are shaped for option A/C; if another option is ratified, re-run Phase 4b/4c for them first.

### 2. Plan-stage reuse per the ratified ADR ([FR2](./DESIGN.md#fr2))
**Goal**: Implement the ratified mechanism; repeated shapes skip the hot stage.
**Tests** (red first): `test_plan_cache_hit_result_identical` (hit vs miss byte-equality); `test_plan_cache_key_components_miss` (each key component change ⇒ miss); `test_plan_cache_repeated_shape_faster` (deterministic proxy: hot-stage counter, not wall-clock)
**Verify**: `cargo test --lib querier:: && make check-clippy`
**Acceptance criteria**:
- [ ] Tests green; `sol_querier_plan_cache_*` counters emitted; bench shows repeated-shape cold ≤ the bare-selector band on the fixture
**Depends on**: task 1 + ADR ratification
**Time-box**: ~90 min

### 3. Instant staleness lookback + margin cleanup ([FR3](./DESIGN.md#fr3), [FR4](./DESIGN.md#fr4))
**Goal**: Bound instant scans; move margins fully to parse time.
**Tests** (red first): `test_instant_selector_bounded_scan` (files-opened drops; in-lookback series identical, staler series absent); `test_exact_bounds_files_no_query_margin` (15-min scope over exact-bounds fixture includes only true-overlap files); existing parity suite green
**Verify**: `cargo test --lib querier:: && make check-clippy`
**Acceptance criteria**:
- [ ] Tests green; instant selector live probe ≤ 90 ms recorded here
**Depends on**: (none — parallel-safe with 2)
**Time-box**: ~75 min

### 4. Live verification + evidence ([NFR1](./DESIGN.md#nfr1), [NFR2](./DESIGN.md#nfr2))
**Goal**: Re-run the predecessor's probe set on the rebuilt image; record before/after; targets met or honestly re-decomposed.
**Verify**: probe set from [backend-metrics-perf VERIFY](../../20260716_backend-metrics-perf/VERIFY.md); `cargo test --lib querier:: -- --ignored bench_cold_range_query_demo_scale --nocapture`
**Acceptance criteria**:
- [ ] VERIFY table updated: repeated-shape cold ≤ 80 ms, burst ≤ 0.5 s, instant ≤ 90 ms — or a fired-trigger note per the predecessor's pattern
**Depends on**: tasks 2, 3 (+ user image rebuild)
**Time-box**: ~45 min

## Sessions

### Session 1 — Profile → ADR proposed (~1.5 H)
Tasks: 1
**Skills**: `rust-software-engineer`, `rust-build`, `tdd`
**Checkpoint**: `cargo test --lib querier:: && make check-clippy`
**Commit point**: yes — then STOP for ADR ratification (severity-3 style gate, planned not accidental)

### Session 2 — Mechanism + instant/margin (~3 H)
Tasks: 2, 3
**Skills**: `rust-software-engineer`, `rust-build`, `tdd`
**Checkpoint**: `cargo test --lib querier:: && make check-clippy`
**Commit point**: yes

### Session 3 — Live evidence (~1 H, needs user rebuild)
Tasks: 4
**Skills**: `rust-software-engineer`, `rust-build`
**Checkpoint**: probe set green vs targets
**Commit point**: yes

## Quality gates (post-session review)
- [ ] Acceptance criteria all green
- [ ] Code review vs [DESIGN.md](./DESIGN.md) intent
- [ ] Plan-cache key completeness: every component has a changing-it-misses test
- [ ] Observability: plan-cache hit/miss counters on the querier dashboard family
- [ ] Performance: NFR table updated with live numbers
