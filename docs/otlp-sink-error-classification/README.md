# otlp-sink-error-classification

The OpenTelemetry sink (both HTTP and gRPC) treats **all** non-success responses as retriable errors. This means that HTTP 400 Bad Request (client error) is retried with Fibonacci backoff indefinitely, even though the request will never succeed — the payload is malformed and the server will always reject it.

## Design
- [20260510_otlp-sink-error-classification](./designs/20260510_otlp-sink-error-classification.md)

## ADRs
- [20260510_otlp-retry-alignment](./adrs/20260510_otlp-retry-alignment.md) — Align OTLP sink retry logic with OTLP specification and base HttpRetryLogic
