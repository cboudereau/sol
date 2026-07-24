---
status: accepted
---
# Sink normalization strategy

Addresses: metric type and temporality conversion for non-OTLP backends

## Problem

Different backends expect different metric temporalities and types. Prometheus expects cumulative sums and explicit-bounds histograms. InfluxDB expects delta sums. StatsD expects delta counters. How should Sol handle these conversions?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Global normalizer before all sinks | Single conversion point; consistent | Over-converts for sinks that could handle the original type; no per-sink customization |
| B. Per-sink normalizer — each sink knows its backend's requirements | Precise conversion; sinks that handle ExpHist natively skip conversion | Conversion logic distributed across sinks |

## Decision

Option B for temporality normalization — per-sink normalizer (D33). Option A centralized for ExponentialHistogram→Histogram conversion, refined to per-sink in G7.

**Temporality normalization** (D33):
- **Prometheus** expects Cumulative → if Delta Sum/Histogram, accumulate in-memory
- **InfluxDB** expects Delta → if Cumulative Sum, diff consecutive values
- **StatsD** expects Delta → if Cumulative Sum, diff
- Each normalizer reads `aggregation_temporality` from the proto and converts as needed

**ExponentialHistogram conversion** (D32, refined by G7):
- Initially centralized in MetricNormalizer
- Refined: per-sink conversion — InfluxDB and GreptimeDB receive native ExpHist and extract count/sum/min/max/avg directly (more information preserved)
- Prometheus and StatsD convert to explicit-bounds Histogram

**Sink implementation order** (D43, D44): Sinks first, then sources, then cleanup. Tier 1 (Prometheus, InfluxDB, StatsD) before Tier 2 (CloudWatch, GreptimeDB).

## Consequences

- Each sink gets the exact metric format its backend expects
- InfluxDB/GreptimeDB preserve more ExpHist information than if pre-converted to explicit bounds
- Prometheus gets properly bucketed histograms from any source
- No silent data drops — ExponentialHistogram is converted, not ignored
