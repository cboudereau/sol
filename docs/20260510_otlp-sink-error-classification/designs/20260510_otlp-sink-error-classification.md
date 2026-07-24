# otlp-sink-error-classification — Design Doc

## Context

The OpenTelemetry sink (both HTTP and gRPC) treats **all** non-success responses as retriable errors. This means that HTTP 400 Bad Request (client error) is retried with Fibonacci backoff indefinitely, even though the request will never succeed — the payload is malformed and the server will always reject it.

This was discovered in practice: Loki 3.7 rejects OTLP log pushes with an empty attribute key name (HTTP 400), and Sol retries forever instead of dropping the events and reporting an error.

The base `HttpRetryLogic` (used by generic HTTP sinks) already handles this correctly — it only retries 5xx, 429, and 408. But the OTLP-specific `OtlpHttpRetryLogic` and `OtlpRetryLogic` (gRPC) override this with `is_retriable_error() -> true` and no `should_retry_response()` implementation.

### Current behavior

| HTTP Status | `HttpRetryLogic` (base) | `OtlpHttpRetryLogic` (current) |
|---|---|---|
| 2xx | Successful | Successful (never reaches retry) |
| 400 | DontRetry | **Retry** (bug) |
| 408 | Retry | **Retry** (correct by accident) |
| 429 | Retry | **Retry** (correct by accident) |
| 501 | DontRetry | **Retry** (bug) |
| 5xx | Retry | **Retry** (correct by accident) |

The OTLP HTTP service (`OtlpHttpService::call`) converts **all** non-2xx to `Err(OtlpHttpError)` at line 252-255 of `src/sinks/opentelemetry/http.rs`. Since `is_retriable_error()` returns `true`, the retry policy in `FibonacciRetryPolicy` always retries.

### Dashboard impact

The Sol Pipeline dashboard queries `sol_component_errors_total{component_kind="sink"}` for sink errors. But because events are endlessly retried (never dropped), the error counter is never incremented. Only `sol_component_retries_total` increases — which is misleading since the retries will never succeed.

## Functional Requirements

### <a id="fr1"></a>FR1 — Classify HTTP status codes in OTLP HTTP sink

Implement `should_retry_response()` on `OtlpHttpRetryLogic` to distinguish:
- **4xx** (except 429, 408) → `DontRetry` — client errors that will never succeed
- **429** → `Retry` — rate limiting, transient
- **408** → `Retry` — request timeout, transient
- **501** → `DontRetry` — endpoint not implemented
- **5xx** (except 501) → `Retry` — server errors, transient
- **2xx** → `Successful`

This aligns with the [OTLP specification's retry guidance](https://opentelemetry.io/docs/specs/otlp/#failures-1) and matches the existing `HttpRetryLogic` behavior.

### <a id="fr2"></a>FR2 — Classify gRPC status codes in OTLP gRPC sink

Implement `should_retry_response()` on `OtlpRetryLogic` (or refine `is_retriable_error()`) to distinguish:
- **UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED** → Retry (transient)
- **INVALID_ARGUMENT, NOT_FOUND, ALREADY_EXISTS, PERMISSION_DENIED, UNAUTHENTICATED, UNIMPLEMENTED** → DontRetry (permanent)
- **INTERNAL, UNKNOWN** → Retry (may be transient)
- **OK** → Successful

This aligns with the [OTLP specification's gRPC retry guidance](https://opentelemetry.io/docs/specs/otlp/#failures).

### <a id="fr3"></a>FR3 — Return HTTP status in OtlpHttpResponse for non-2xx

Refactor `OtlpHttpService::call()` to return `Ok(OtlpHttpResponse)` with the HTTP status code instead of `Err(OtlpHttpError)` for non-2xx responses. This allows `should_retry_response()` to inspect the status code and make a retry decision, rather than falling through to `is_retriable_error()` which has no status information.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Backward-compatible metrics

Existing `component_retries_total` and `component_errors_total` metrics must continue to work. The change should result in:
- 4xx errors counting toward `component_errors_total` (via `EventStatus::Rejected`)
- 5xx errors still retrying and only counting toward `component_errors_total` when retries are exhausted
- `component_retries_total` only incrementing for truly retryable errors

### <a id="nfr2"></a>NFR2 — No new dependencies

Use existing types and patterns from the codebase. No new crates.

## Non-goals

- Adding HTTP status code labels to existing metrics (e.g., `component_errors_total{status="400"}`). The existing label structure is sufficient; status codes appear in log messages.
- Changing retry behavior for non-OTLP sinks. Each sink has its own `RetryLogic`.
- Making retry behavior configurable per-sink. The OTLP spec defines which errors are retryable.
- Changing the gRPC service layer beyond error classification.

## Rabbit holes

- **Partial success handling**: OTLP supports partial success responses (HTTP 200 with error details in body). This is valuable but out of scope — track as a future enhancement.
- **Retry budget / circuit breaker**: Useful for production but orthogonal to error classification.

## Design

The fix requires two changes per transport:

### HTTP transport

1. **Refactor `OtlpHttpService::call()`** to return `Ok(OtlpHttpResponse { status, ... })` for ALL HTTP responses (not just 2xx). The response carries the status code.
2. **Implement `should_retry_response()`** on `OtlpHttpRetryLogic` to classify the status code — matching `HttpRetryLogic` behavior.
3. **Implement `DriverResponse`** on `OtlpHttpResponse` with status-aware `event_status()` — returning `Delivered` for 2xx, `Errored` for 5xx, `Rejected` for all others (3xx, 4xx).

### gRPC transport

1. **Refine `is_retriable_error()`** on `OtlpRetryLogic` to inspect `tonic::Status` code and only retry transient gRPC errors.

Decisions:
- [OTLP retry alignment](../adrs/20260510_otlp-retry-alignment.md)

## Cross-cutting Concerns

- **Observability**: After this change, the Sol Pipeline dashboard's "Sink Errors" panel will correctly show 4xx rejections. No dashboard changes needed.
- **Rollback**: Git revert restores the previous retry-everything behavior.
