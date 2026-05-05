---
status: draft
---
# Align OTLP sink retry logic with OTLP specification and base HttpRetryLogic

Addresses: [FR1](../DESIGN.md#fr1), [FR2](../DESIGN.md#fr2), [FR3](../DESIGN.md#fr3)

## Problem

The OTLP sinks (HTTP and gRPC) treat all errors as retriable, which contradicts:
1. The [OTLP specification](https://opentelemetry.io/docs/specs/otlp/#failures-1) which defines specific retryable vs non-retryable errors
2. Sol's own `HttpRetryLogic` which already classifies 4xx as non-retryable
3. Other Sol sinks (Doris, Elasticsearch) which implement proper status code classification

This causes silent data loss (events stuck in infinite retry loops that will never succeed) and incorrect metrics (no `component_errors_total` for permanent failures).

## Options

| Option | Pros | Cons |
|---|---|---|
| A: Implement `should_retry_response()` on OTLP retry types | Follows existing patterns (Doris, Elasticsearch), aligns with OTLP spec, requires refactoring `OtlpHttpService::call` to return status in response | Requires changing `OtlpHttpResponse` to carry status |
| B: Reuse `HttpRetryLogic` directly for OTLP HTTP | No new code for retry logic | OTLP uses custom error/response types, type mismatch; gRPC still needs its own logic |
| C: Add status parsing in `is_retriable_error()` by encoding status in error message | No response type changes | Fragile string parsing, ugly, doesn't follow existing patterns |

## Decision

Option A — Implement `should_retry_response()` on both `OtlpHttpRetryLogic` and `OtlpRetryLogic`.

For HTTP: refactor `OtlpHttpService::call()` to return `Ok(OtlpHttpResponse)` with the HTTP status code for all responses, allowing `should_retry_response()` to inspect the status.

For gRPC: refine `is_retriable_error()` to inspect `tonic::Status::code()` since gRPC errors arrive as `Err`, not `Ok`.

This follows the same pattern used by `DorisRetryLogic` and `HttpRetryLogic`.

## Consequences

- 4xx client errors are immediately dropped with `component_errors_total` increment
- 5xx server errors continue to retry with backoff
- Sol Pipeline dashboard "Sink Errors" panel shows real errors
- `component_retries_total` only counts genuinely retryable failures
- Aligns with the OTLP specification's retry guidance
