---
status: accepted
---
# OTLP as sole core protocol

Addresses: the foundational architectural decision for Sol

## Problem

Vector's core event model uses proprietary types (`LogEvent`, `Metric` with `MetricValue`, `TraceEvent`) with vendor-specific extensions embedded in core (`AgentDDSketch`, `DatadogMetricOriginMetadata`). OTLP traffic — now the dominant signal format — requires double-conversion at every source/sink boundary (OTel proto → Vector types → OTel proto), adding latency and losing fidelity.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Keep Vector types, improve OTLP adapters | No breaking changes; incremental | Double-conversion cost persists; vendor types stay in core; OTLP fidelity limited by lossy intermediate representation |
| B. Add OTLP as a parallel core type (`use_otlp_decoding` flag) | Opt-in migration; gradual rollout | Two code paths everywhere; double maintenance burden; flag must be threaded through every transform |
| C. Replace core with OTel types; keep legacy protocols as source adapters only | Zero-conversion for OTLP traffic; clean core; ~14K lines removed | Breaking changes for VRL, codecs, and configs; disk buffer format break |

## Decision

Option C — replace the entire core event model with OTel-native types. Sol is a new product (true fork), not a backward-compatible release of Vector. The breaking changes are managed through the VRL migration tool (~91% auto-rewrite) and the native protocol adapter at the vector source.

Specific sub-decisions:
- **Delete all legacy core types**: `LogEvent`, `Metric`, `TraceEvent`, `MetricValue`, `MetricData`, `MetricTags`, `NativeSerializer` (D1, D15, D17, D23)
- **Split `otel_event.rs`** into `otel_log.rs`, `otel_metric.rs`, `otel_attributes.rs` for maintainability (D19)
- **Keep `Sample`/`Bucket`/`Quantile`** as convenience constructors — used in 20+ files, no legacy semantics (D18)
- **Summary is legacy passthrough only** — the OTLP spec says "not recommended for new applications." Sol never produces Summary; received Summary passes through unchanged (D45, D52)
- **Transformer `only_fields`/`except_fields`** become OTLP-aware: `body`, `attributes.X`, `resource.X` (D25)
- **VRL migration tool** (`vector vrl-migrate`) ships as a 3-pass rewriter for automated config migration (D26)

## Consequences

- OTLP passthrough has zero conversion cost
- ~14,000 lines of proprietary code removed from core
- VRL field paths change (`.message` → `.body`, `.tags` → `.attributes`)
- Disk buffer format changes — existing buffers must be drained before upgrade
- All codecs must encode from OTLP types directly (see [ADR 0009](20260503_non-otlp-codec-encoding.md))
