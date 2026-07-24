# otlp-as-core-protocol

Sol (**S**ingle **O**bservability **L**ayer) is a true fork of Vector, rebuilt around an OpenTelemetry-centric core. See MARKET.md for the full product vision and market positioning.

## Design
- [20260503_otlp-as-core-protocol](./designs/20260503_otlp-as-core-protocol.md)

## ADRs
- [20260503_exponential-histogram-strategy](./adrs/20260503_exponential-histogram-strategy.md) — ExponentialHistogram as internal histogram format
- [20260503_non-otlp-codec-encoding](./adrs/20260503_non-otlp-codec-encoding.md) — Non-OTLP codec encoding strategy
- [20260503_otlp-as-sole-core-protocol](./adrs/20260503_otlp-as-sole-core-protocol.md) — OTLP as sole core protocol
- [20260503_pipeline-internal-struct-fields](./adrs/20260503_pipeline-internal-struct-fields.md) — Pipeline-internal state as struct fields, not attributes
- [20260503_sink-normalization-strategy](./adrs/20260503_sink-normalization-strategy.md) — Sink normalization strategy
- [20260503_source-resource-scope-conventions](./adrs/20260503_source-resource-scope-conventions.md) — Source resource and scope conventions
- [20260503_statsd-otlp-compliance](./adrs/20260503_statsd-otlp-compliance.md) — StatsD source OTLP-compliant redesign
- [20260503_vector-source-sink-restructure](./adrs/20260503_vector-source-sink-restructure.md) — Vector sink deleted, native proto at source boundary
