---
status: accepted
---
# Null sink equivalence

Addresses: [FR2](../DESIGN.md#fr2), [NFR1](../DESIGN.md#nfr1)

## Problem

Sol and otelcontribcol have different null/noop sink implementations. We need to ensure they impose equivalent overhead so the benchmark measures pipeline cost, not sink cost.

## Options

| Option | Pros | Cons |
|---|---|---|
| Sol `blackhole` + otelcol `nop` exporter | Both are purpose-built null sinks. Minimal overhead. Standard config. | Slightly different implementations — impossible to guarantee identical overhead. |
| Sol `blackhole` + otelcol `debug` exporter (verbosity: basic, no output) | `debug` is the most commonly used "do nothing" exporter in otelcol docs | `debug` still formats output even at basic verbosity — unfair to otelcol. |
| Both forward to a shared `/dev/null` file sink | Exactly equal sink overhead | Adds filesystem I/O to both — measures disk rather than pipeline. |

## Decision

**Sol `blackhole` + otelcol `nop` exporter.** Both are designed to consume events with minimal work. The `nop` exporter is a true no-op (introduced in otelcol v0.111.0). Sol's `blackhole` sink acknowledges immediately and optionally logs a summary count. We disable `blackhole`'s `print_interval_secs` to make it silent.

Any residual overhead difference between these two sinks is negligible compared to the pipeline cost we're measuring (OTLP deserialization, internal routing, backpressure propagation).

## Consequences

- Must verify that the otelcol version used includes the `nop` exporter (≥ v0.111.0).
- The benchmark report must document which sinks are used and why they're considered equivalent.
