# benchmark-sol-vs-otelcol

Sol is positioned as a drop-in replacement for OTel Collector Contrib and Vector. To promote Sol credibly, we need published, reproducible benchmarks comparing OTLP ingestion throughput, latency, and resource usage between Sol and otelcontribcol.

## Design
- [20260513_benchmark-sol-vs-otelcol](./designs/20260513_benchmark-sol-vs-otelcol.md)

## ADRs
- [20260513_load-balancing-equivalence](./adrs/20260513_load-balancing-equivalence.md) — Load balancing equivalence
- [20260513_measurement-source](./adrs/20260513_measurement-source.md) — Measurement source
- [20260513_null-sink-equivalence](./adrs/20260513_null-sink-equivalence.md) — Null sink equivalence
- [20260513_resource-limits](./adrs/20260513_resource-limits.md) — Resource limits
- [20260513_tail-sampling-policy-equivalence](./adrs/20260513_tail-sampling-policy-equivalence.md) — Tail sampling policy equivalence
