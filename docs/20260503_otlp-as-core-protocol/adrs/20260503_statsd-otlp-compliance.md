---
status: accepted
---
# StatsD source OTLP-compliant redesign

Addresses: source-side OTLP fidelity for StatsD ingestion

## Problem

The original StatsD source emitted raw samples per packet (one metric event per UDP packet) with no aggregation, no timestamps, no resource attributes, and used the deleted `Distribution` type for timers. This diverged significantly from how the OTel Collector's StatsD receiver works and produced unusable data for Prometheus-style backends.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Keep per-packet emission, convert at sink | Minimal source changes | Unbounded memory from raw samples; no aggregation; Prometheus gets one bucket per sample |
| B. Flush-interval aggregation with ExponentialHistogram | OTLP-compliant; bounded memory; Prometheus-compatible | Introduces latency (flush interval); stateful source |

## Decision

Option B — full rewrite with flush-interval aggregation engine (D36-D39, D46, D48-D50).

**Aggregation engine** (`src/sources/statsd/aggregator.rs`):
- In-memory state per metric series (name + tags), flushed every `aggregation_interval_secs` (default 10s, configurable)
- **Counters**: Sum (Delta), `is_monotonic: false` by default (configurable, D37). Always f64 — lossless for text-parsed floats (D46).
- **Gauges (absolute)**: last-value wins. **Gauges (delta `+5|g`)**: stateful accumulation persisting across flushes with TTL (default 5min, configurable). Sol advantage over otelcontribcol: gauge state survives flush intervals (D38).
- **Timers/histograms/distributions**: ExponentialHistogram (MaxSize=160, scale=20). Sample rate → weight-based insertion. Always ExponentialHistogram — no `observer_type` config, no silent data loss (D50).
- **Sets**: unique value accumulation → cardinality as Gauge. otelcontribcol doesn't support sets at all — Sol advantage.

**Timestamps**: `time_unix_nano` = flush time, `start_time_unix_nano` = previous flush time (D48 includes DogStatsD v1.3 timestamp override).

**Bare tags**: `AnyValue { value: Some(StringValue("")) }` — pragmatic choice for backend reach; `None` values dropped by Prometheus/Mimir/Datadog/ES (D39).

**DogStatsD extensions**: container ID `c:<id>` → `container.id` attribute (D48).

**Global aggregation**: metrics aggregated across all senders (D49). Per-address would produce N copies for N pods. Users who need source identity add a host tag.

## Consequences

- 1000 StatsD packets → 1 data point per metric per interval (bounded output)
- Proper timestamps on every data point
- ExponentialHistogram enables lossless merge and Prometheus-compatible sink output
- Gauge state persists across flush intervals (advantage over otelcontribcol)
- Set support (otelcontribcol drops sets entirely)
