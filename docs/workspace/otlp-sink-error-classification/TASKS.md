# otlp-sink-error-classification — Tasks

Design: [DESIGN.md](./DESIGN.md)

## Analysis

Build: `cargo build --no-default-features --features api,sources-opentelemetry,sinks-opentelemetry`
Test: `cargo test --no-default-features --features api,sources-opentelemetry,sinks-opentelemetry`
Lint: `cargo clippy --no-default-features --features api,sources-opentelemetry,sinks-opentelemetry`

### Known-failing tests
| Test | Reason | Action |
|---|---|---|
| (none) | | |

### Domain model

```
OtlpHttpService::call()
  └─ currently: non-2xx → Err(OtlpHttpError) → is_retriable_error() → true → Retry
  └─ target:    non-2xx → Ok(OtlpHttpResponse { status }) → should_retry_response() → Retry/DontRetry

OtlpRetryLogic (gRPC)
  └─ currently: Err(OtlpGrpcError { source: tonic::Status }) → is_retriable_error() → true → Retry
  └─ target:    Err(OtlpGrpcError { source: tonic::Status }) → is_retriable_error() → inspect code → true/false
```

### Requirement traceability

| Type / Fn | Addresses | Notes |
|---|---|---|
| `OtlpHttpResponse` | [FR3](./DESIGN.md#fr3) | Add `status: StatusCode` field |
| `OtlpHttpResponse::event_status()` | [FR3](./DESIGN.md#fr3) | Status-aware `DriverResponse` |
| `OtlpHttpService::call()` | [FR3](./DESIGN.md#fr3) | Return `Ok` for all HTTP responses |
| `OtlpHttpRetryLogic::should_retry_response()` | [FR1](./DESIGN.md#fr1) | Classify HTTP status codes |
| `OtlpRetryLogic::is_retriable_error()` | [FR2](./DESIGN.md#fr2) | Classify gRPC status codes |

### Transformations

| Function | Input → Output | Invariant / Rule |
|---|---|---|
| `OtlpHttpRetryLogic::should_retry_response()` | `&OtlpHttpResponse → RetryAction` | 4xx (except 429/408) → DontRetry; 5xx (except 501) → Retry; 429/408 → Retry; 501 → DontRetry; 2xx → Successful |
| `OtlpRetryLogic::is_retriable_error()` | `&OtlpGrpcError → bool` | UNAVAILABLE/DEADLINE_EXCEEDED/RESOURCE_EXHAUSTED/INTERNAL/UNKNOWN → true; all others → false |
| `OtlpHttpResponse::event_status()` | `&self → EventStatus` | 2xx → Delivered; 5xx → Errored; 4xx/other → Rejected |
| `OtlpHttpService::call()` | `OtlpHttpRequest → Result<OtlpHttpResponse, OtlpHttpError>` | All HTTP responses (2xx-5xx) → Ok(response); connection/build errors → Err |

### Key files

```
src/sinks/opentelemetry/http.rs:40-44     — OtlpHttpError enum
src/sinks/opentelemetry/http.rs:189-201   — OtlpHttpResponse + DriverResponse
src/sinks/opentelemetry/http.rs:214-267   — OtlpHttpService::call (status check at 251-256)
src/sinks/opentelemetry/http.rs:273-284   — OtlpHttpRetryLogic (missing should_retry_response)
src/sinks/opentelemetry/grpc.rs:34-38     — OtlpGrpcError (wraps tonic::Status)
src/sinks/opentelemetry/grpc.rs:333-344   — OtlpRetryLogic (is_retriable_error always true)
src/sinks/util/retries.rs:18-27           — RetryAction enum
src/sinks/util/retries.rs:29-51           — RetryLogic trait
src/sinks/util/retries.rs:143-218         — FibonacciRetryPolicy (uses should_retry_response + is_retriable_error)
src/sinks/util/http.rs:554-590           — HttpRetryLogic (reference implementation)
src/sinks/util/http.rs:942-977           — HttpRetryLogic tests (reference for test patterns)
src/sinks/doris/retry.rs                  — DorisRetryLogic (reference for custom retry)
```

## Tasks

### 1. Refactor OtlpHttpResponse to carry HTTP status ([FR3](./DESIGN.md#fr3))

**Goal**: Make HTTP status code available for retry decisions and event status reporting.
**Files**: `src/sinks/opentelemetry/http.rs`
**Constraints**:
- [ADR: otlp-retry-alignment](./adrs/otlp-retry-alignment.md) — return status in response, not in error
- `OtlpHttpResponse` must implement `DriverResponse` with status-aware `event_status()`
- `OtlpHttpService::call()` must return `Ok(OtlpHttpResponse)` for all HTTP responses, `Err` only for connection/build errors
- `EndpointBytesSent` must only be emitted for 2xx responses (successful delivery)
**Tests**:
- `test_otlp_http_response_event_status_2xx` — 200 returns `EventStatus::Delivered`
- `test_otlp_http_response_event_status_4xx` — 400 returns `EventStatus::Rejected`
- `test_otlp_http_response_event_status_5xx` — 500 returns `EventStatus::Errored`
**Verify**: `cargo test -p sol --no-default-features --features api,sources-opentelemetry,sinks-opentelemetry otlp_http_response`
**Acceptance criteria**:
- [x] `OtlpHttpResponse` has a `status: StatusCode` field
- [x] `event_status()` returns `Delivered` for 2xx, `Rejected` for 4xx, `Errored` for 5xx
- [x] `OtlpHttpService::call()` returns `Ok` for all HTTP responses
- [x] `EndpointBytesSent` only emitted on 2xx
- [x] Connection/builder errors still return `Err(OtlpHttpError)`
**Depends on**: (none)
**Time-box**: ~45 min

### 2. Implement should_retry_response for OtlpHttpRetryLogic ([FR1](./DESIGN.md#fr1))

**Goal**: Stop retrying 4xx client errors in the OTLP HTTP sink.
**Files**: `src/sinks/opentelemetry/http.rs`
**Constraints**:
- [ADR: otlp-retry-alignment](./adrs/otlp-retry-alignment.md) — align with OTLP spec and `HttpRetryLogic`
- Must match `HttpRetryLogic::should_retry_response()` behavior: 429/408 → Retry, 501 → DontRetry, 5xx → Retry, 2xx → Successful, other → DontRetry
- Reference: `src/sinks/util/http.rs:574-589`
**Tests**:
- `test_otlp_http_retry_logic_400` — 400 → not retryable
- `test_otlp_http_retry_logic_408` — 408 → retryable
- `test_otlp_http_retry_logic_429` — 429 → retryable
- `test_otlp_http_retry_logic_500` — 500 → retryable
- `test_otlp_http_retry_logic_501` — 501 → not retryable
- `test_otlp_http_retry_logic_200` — 200 → successful
**Verify**: `cargo test -p sol --no-default-features --features api,sources-opentelemetry,sinks-opentelemetry otlp_http_retry`
**Acceptance criteria**:
- [x] `OtlpHttpRetryLogic` implements `should_retry_response()`
- [x] 4xx (except 429/408) returns `DontRetry`
- [x] 5xx (except 501) returns `Retry`
- [x] 429/408 returns `Retry`
- [x] 501 returns `DontRetry`
- [x] 2xx returns `Successful`
- [x] All tests pass
**Depends on**: task 1
**Time-box**: ~30 min

### 3. Classify gRPC status codes in OtlpRetryLogic ([FR2](./DESIGN.md#fr2))

**Goal**: Stop retrying permanent gRPC errors (INVALID_ARGUMENT, UNIMPLEMENTED, etc.) in the OTLP gRPC sink.
**Files**: `src/sinks/opentelemetry/grpc.rs`
**Constraints**:
- [ADR: otlp-retry-alignment](./adrs/otlp-retry-alignment.md) — align with OTLP spec
- gRPC errors arrive as `Err(OtlpGrpcError::GrpcRequest { source: tonic::Status })` so classification happens in `is_retriable_error()`
- Retryable codes (per OTLP spec): `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `INTERNAL`, `UNKNOWN`, `ABORTED`, `CANCELLED`
- Non-retryable codes: `INVALID_ARGUMENT`, `NOT_FOUND`, `ALREADY_EXISTS`, `PERMISSION_DENIED`, `UNAUTHENTICATED`, `UNIMPLEMENTED`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `DATA_LOSS`
**Tests**:
- `test_otlp_grpc_retry_logic_unavailable` — UNAVAILABLE → retriable
- `test_otlp_grpc_retry_logic_deadline_exceeded` — DEADLINE_EXCEEDED → retriable
- `test_otlp_grpc_retry_logic_resource_exhausted` — RESOURCE_EXHAUSTED → retriable
- `test_otlp_grpc_retry_logic_internal` — INTERNAL → retriable
- `test_otlp_grpc_retry_logic_invalid_argument` — INVALID_ARGUMENT → not retriable
- `test_otlp_grpc_retry_logic_unimplemented` — UNIMPLEMENTED → not retriable
- `test_otlp_grpc_retry_logic_permission_denied` — PERMISSION_DENIED → not retriable
**Verify**: `cargo test -p sol --no-default-features --features api,sources-opentelemetry,sinks-opentelemetry otlp_grpc_retry`
**Acceptance criteria**:
- [x] `is_retriable_error()` inspects `tonic::Status::code()`
- [x] Transient codes (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, INTERNAL, UNKNOWN, ABORTED, CANCELLED) return `true`
- [x] Permanent codes (INVALID_ARGUMENT, UNIMPLEMENTED, etc.) return `false`
- [x] All tests pass
**Depends on**: (none)
**Time-box**: ~30 min

## Sessions

### Session 1 — OTLP error classification (~1.5H)
Tasks: 1, 2, 3
**Skills**: `rust-software-engineer`, `rust-build`
**Checkpoint**: `cargo test -p sol --no-default-features --features api,sources-opentelemetry,sinks-opentelemetry -- otlp && cargo clippy -p sol --no-default-features --features api,sources-opentelemetry,sinks-opentelemetry`
**Commit point**: yes — commit after checkpoint passes

## Quality gates (post-session review)
- [x] Acceptance criteria: all green above
- [x] Code review: changes match [DESIGN.md](./DESIGN.md) intent — proper error classification, no over-engineering
- [x] Code organization: changes scoped to `src/sinks/opentelemetry/{http,grpc}.rs`
- [x] Security review: no new attack surface
- [x] Observability: `component_errors_total` now increments for permanent failures; `component_retries_total` only for transient; Sol Pipeline dashboard works without changes
- [x] Performance: no additional overhead — status code matching is O(1)
