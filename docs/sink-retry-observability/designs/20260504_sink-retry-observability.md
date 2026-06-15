# sink-retry-observability — Design Doc

## Context

While building the SOL pipeline monitoring dashboard ([sol-telemetry-monitoring](../../sol-telemetry-monitoring/designs/20260504_sol-telemetry-monitoring.md)), we discovered that sink retry behavior is invisible to metrics. When a downstream service (Tempo, Loki, Mimir) goes down:

1. The OTLP HTTP sink retries with Fibonacci backoff
2. Default `retry_attempts` is `isize::MAX` (~2 billion) — effectively infinite
3. `component_errors_total` only increments when retries are **exhausted** (`CallError` in `lib/sol-common/src/internal_event/service.rs`), which practically never happens
4. Retries are logged at `warn`/`debug` level but not counted in any metric
5. The retry policy (`FibonacciRetryPolicy` in `src/sinks/util/retries.rs`) calls `build_retry()` on each attempt but emits no counter

The result: an operator watching the SOL Pipeline dashboard sees zero errors while a backend is down for hours. The only signal is in container logs, which may not be collected or queryable.

This affects all sinks that use the Tower retry service (`Retry<FibonacciRetryPolicy<L>, Timeout<S>>` — see `src/sinks/util/service.rs:40`), not just OTLP HTTP.

## Functional Requirements

### <a id="fr1"></a>FR1 — Emit a retry counter metric

Each retry attempt must increment a counter metric `component_retries_total` with label:
- `error_type` — the retry reason category, from a fixed set: `request_failed`, `timeout`, `response_failed`

Component context labels (`component_id`, `component_kind`, `component_type`) are added automatically by the metrics recorder when the sink runs inside its component task context.

### <a id="fr2"></a>FR2 — Emit a histogram of retry backoff durations

Record each retry's backoff duration as a histogram `component_retry_backoff_seconds`. This lets operators see the distribution of backoff delays — distinguishing early retries (1s) from deep backoff (minutes).

A histogram is preferred over a gauge because:
- No "clear on success" problem — each retry records a sample independently
- Aggregatable across time windows via `histogram_quantile()`
- The gauge would go stale after retries stop (the policy is cloned per-request, no shared state to clear)

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — No overhead when not retrying

The metrics must only be registered/incremented on actual retry attempts, not on every successful request. The `counter!()` and `histogram!()` macros from the `metrics` crate are lazy — they register on first use.

### <a id="nfr2"></a>NFR2 — Works for all sinks

The implementation must be in the shared `FibonacciRetryPolicy` (`src/sinks/util/retries.rs`), not per-sink. All sinks using `TowerRequestLayer` or `TowerRequestSettings` automatically get retry observability.

## Non-goals

- Changing the default `retry_attempts` value — that's a separate configuration decision
- Adding alerting rules — those come after metrics exist
- Retry circuit-breaking or backpressure — different concern

## Rabbit holes

- **Metric cardinality**: `error_type` uses a fixed 3-value set (`request_failed`, `timeout`, `response_failed`), not free-form strings. The `Cow<'static, str>` reason from `RetryAction::Retry(reason)` is logged but NOT used as a label.

## Design

The `retry()` method in `FibonacciRetryPolicy` (Tower's `Policy` trait impl, line 147) is the decision point. It handles three retry scenarios:

1. **Response retry** — `should_retry_response()` returns `RetryAction::Retry(reason)` → error_type: `response_failed`
2. **Error retry** — `is_retriable_error()` returns `true` → error_type: `request_failed`
3. **Timeout retry** — error is `Elapsed` → error_type: `timeout`

Each scenario calls `build_retry()` to create the delay future. The metrics are emitted in `build_retry()` after adding an `error_type: &'static str` parameter:

```rust
fn build_retry(&mut self, error_type: &'static str) -> RetryPolicyFuture {
    self.advance();
    let backoff = self.backoff();
    let delay = Box::pin(sleep(backoff));

    counter!("component_retries_total", "error_type" => error_type).increment(1);
    histogram!("component_retry_backoff_seconds").record(backoff.as_secs_f64());

    debug!(message = "Retrying request.", delay_ms = %backoff.as_millis());
    RetryPolicyFuture { delay }
}
```

Call sites in `retry()`:
- Line 160: `Some(self.build_retry("response_failed"))` — response retry
- Line 172: `Some(self.build_retry("response_failed"))` — partial response retry
- Line 191: `Some(self.build_retry("request_failed"))` — retriable error
- Line 203: `Some(self.build_retry("timeout"))` — timeout/elapsed

Decisions: none — straightforward implementation

## Cross-cutting Concerns

- **Dashboard**: once the metrics exist, add a "Retry rate" panel to the SOL Pipeline dashboard showing `rate(sol_component_retries_total[$__rate_interval])` by `component_id`
- **All sinks affected**: this improves observability for every sink, not just OTLP
- **Compliance tests**: `component_retries_total` is NOT a compliance metric — it only appears during failure scenarios, not on every successful request
