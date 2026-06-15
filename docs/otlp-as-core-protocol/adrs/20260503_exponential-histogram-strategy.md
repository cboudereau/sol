---
status: accepted
---
# ExponentialHistogram as internal histogram format

Addresses: histogram fidelity across the pipeline

## Problem

Multiple sources produce histogram-like data (StatsD timers, log_to_metric distributions, Prometheus histograms). The original Vector used `Distribution` (raw samples) and `AggregatedHistogram` (explicit bounds), both with significant limitations: unbounded memory growth for raw samples, and lossy bucket merging for explicit bounds. How should Sol represent histogram data internally?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Explicit-bounds Histogram everywhere | Simple; Prometheus-native | Unbounded bucket growth from per-sample observations; lossy merge |
| B. ExponentialHistogram internally, convert at sink boundary | Lossless merge; bounded memory (MaxSize); auto-adapting resolution | Conversion needed for Prometheus/InfluxDB/StatsD sinks |
| C. Configurable per-source (regex-based explicit vs exponential) | Maximum flexibility | UX complexity; users must understand histogram types to configure |

## Decision

Option B — ExponentialHistogram as the universal internal histogram format (D32, D36, D47, D50, D51).

**Parameters**: MaxSize=160 (OTel SDK default, ~1KB/series), starting scale=20 (highest resolution, ratchets down on data range).

**Sources**:
- StatsD timers/histograms/distributions → ExponentialHistogram with flush-interval aggregation (D36)
- `log_to_metric` histogram observations → ExponentialHistogram via `new_exponential_histogram_single` (D51)
- No `observer_type` config for StatsD timers — always ExponentialHistogram, no silent data loss (D50)

**Sink conversion**:
- Centralized normalization layer converts ExpHist → explicit-bounds Histogram for non-OTLP sinks (D32)
- Default target boundaries: `[.005,.01,.025,.05,.1,.25,.5,1,2.5,5,10]` (Prometheus defaults, configurable per sink)
- Linear interpolation within exponential buckets (standard approach, same as Prometheus native→classic)
- Per-sink conversion rather than global: InfluxDB/GreptimeDB receive native ExpHist and extract count/sum/min/max/avg (G7)

**Sol advantage over otelcontribcol**: lossless merge across instances + zero-config at source + Prometheus compatibility at sink.

## Consequences

- All histogram-like data has bounded memory usage (MaxSize=160 per series)
- Merge across pipeline instances is lossless (same ExponentialHistogram engine)
- Non-OTLP sinks get properly bucketed histograms instead of silent drops
- `MetricView::Distribution` variant deleted — no raw-samples path exists
