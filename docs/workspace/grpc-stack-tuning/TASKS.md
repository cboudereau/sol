# gRPC & HTTP Stack Tuning — Tasks

Design: [DESIGN.md](./DESIGN.md)

## Analysis

Build: `cargo check --lib -p sol` — verified green
Test: `cargo test --lib -p sol -- opentelemetry` — verified green
Lint: `cargo clippy --lib -p sol` — verified green

### Known-failing tests

| Test | Reason | Action |
|---|---|---|
| (none) | | |

### Domain model

```
src/sinks/opentelemetry/grpc.rs
  ├── Channel::builder(endpoint).connect_lazy()     ← line 122 (single endpoint)
  ├── Channel::builder(uri).connect_lazy()           ← line 462 (LB initial)
  └── Channel::builder(uri).connect_lazy()           ← line 502 (LB dynamic)

src/sources/util/grpc/mod.rs
  └── grpc_server_builder() -> Server                ← line 21
```

### Requirement traceability

| Code location | Addresses | Notes |
|---|---|---|
| `build_otlp_channel()` (new) | [FR1](./DESIGN.md#fr1), [FR3](./DESIGN.md#fr3) | Shared helper for all 3 call sites |
| `grpc_server_builder()` (existing) | [FR2](./DESIGN.md#fr2) | Add `max_concurrent_streams` |

### Transformations

| Function | Input → Output | Invariant |
|---|---|---|
| `build_otlp_channel` | `Uri → Channel` | H2 windows = 1MB/2MB, adaptive window on, TCP_NODELAY on, keepalive 10s/20s, connect timeout 5s |
| `grpc_server_builder` | `() → Server` | Existing settings preserved + max_concurrent_streams = 1024 |

### Dependencies

| Crate | Version | API surface used |
|---|---|---|
| `tonic` | 0.12.3 | `Endpoint::{initial_stream_window_size, initial_connection_window_size, http2_adaptive_window, tcp_nodelay, http2_keep_alive_interval, keep_alive_timeout, connect_timeout, connect_lazy}`, `Server::{max_concurrent_streams}` |

## Tasks

### 1. Create `build_otlp_channel` helper and tune client channels ([FR1](./DESIGN.md#fr1), [FR3](./DESIGN.md#fr3))

**Goal**: Centralize client channel construction with proper H2/TCP tuning.

**Types**: `Channel`, `Endpoint` (from `tonic::transport`)

**Constraints**:
- [ADR: channel-tuning-strategy](./adrs/channel-tuning-strategy.md) — mirror server values
- All 3 `Channel::builder(...).connect_lazy()` call sites must use the new helper
- Parameters: `initial_stream_window_size(1MB)`, `initial_connection_window_size(2MB)`, `http2_adaptive_window(true)`, `tcp_nodelay(true)`, `http2_keep_alive_interval(10s)`, `keep_alive_timeout(20s)`, `connect_timeout(5s)`
- `connect_lazy()` must remain — channels are created before backends are reachable

**Tests**:
- `cargo test --lib -p sol -- opentelemetry` — existing tests still pass
- `cargo clippy --lib -p sol` — no warnings

**Verify**: `cargo check --lib -p sol && cargo clippy --lib -p sol`

**Acceptance criteria**:
- [ ] `build_otlp_channel(uri: Uri) -> Channel` function exists in `src/sinks/opentelemetry/grpc.rs`
- [ ] All 3 `Channel::builder` call sites replaced with `build_otlp_channel`
- [ ] Function sets all 7 tuning parameters listed above
- [ ] `cargo check` and `cargo clippy` pass

**Depends on**: (none)
**Time-box**: ~20 min

### 2. Add `max_concurrent_streams` to gRPC server ([FR2](./DESIGN.md#fr2))

**Goal**: Allow high H2 stream multiplexing on the server side.

**Types**: `Server` (from `tonic::transport::server`)

**Constraints**:
- Add `.max_concurrent_streams(1024)` to `grpc_server_builder()` in `src/sources/util/grpc/mod.rs`
- Do not change existing settings

**Tests**:
- `cargo test --lib -p sol -- grpc` — existing tests still pass

**Verify**: `cargo check --lib -p sol && cargo clippy --lib -p sol`

**Acceptance criteria**:
- [ ] `grpc_server_builder()` includes `.max_concurrent_streams(1024)`
- [ ] `cargo check` and `cargo clippy` pass

**Depends on**: (none)
**Time-box**: ~5 min

### 3. Benchmark validation ([NFR1](./DESIGN.md#nfr1), [NFR2](./DESIGN.md#nfr2))

**Goal**: Verify the tuning closes the 50k traces gap and causes no regressions.

**Constraints**:
- Build local Docker image with changes
- Run benchmark: `cd demo/benchmark && bash run.sh`
- Compare against baseline in [RESULTS.md](../../demo/benchmark/results/RESULTS.md)

**Tests**:
- noop-traces-grpc-50k: ≥86,000 spans/s (≥95% of otelcol's 90,549/s)
- All other scenarios: no regression below 95% of otelcol

**Verify**: Read `demo/benchmark/results/RESULTS.md` and compare

**Acceptance criteria**:
- [ ] noop-traces-grpc-50k ≥ 86,000 spans/s
- [ ] No scenario regresses below 95% of otelcol
- [ ] Results documented in RESULTS.md

**Depends on**: tasks 1, 2
**Time-box**: ~30 min (benchmark runtime)

## Sessions

### Session 1 — Implementation + benchmark (~1H)

Tasks: 1, 2, 3
**Skills**: `software-engineer`
**Checkpoint**: `cargo check --lib -p sol && cargo clippy --lib -p sol` (tasks 1-2), then benchmark run (task 3)
**Commit point**: yes — commit after tasks 1-2 pass check/clippy, before benchmark

## Quality gates (post-session review)

- [ ] Acceptance criteria: all green above
- [ ] Code review: implementation matches [DESIGN.md](./DESIGN.md) intent
- [ ] Code organization: helper function placement, no duplication
- [ ] Code quality: no new complexity, clean types
- [ ] Performance: NFR1 target met (≥95% of otelcol on 50k traces)
