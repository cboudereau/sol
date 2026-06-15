---
status: accepted
---
# Source resource and scope conventions

Addresses: OTel semantic compliance for all metric-producing sources

## Problem

OTel metrics require Resource attributes (`service.name`, `host.name`) and InstrumentationScope (`name`, `version`) on every metric. Sol's sources emitted bare metrics with no resource or scope, diverging from the OTel spec and making metrics indistinguishable in backends.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. No resource/scope — backends handle it | Zero source changes | Violates OTel spec; backends cannot distinguish Sol from other emitters |
| B. Hardcoded defaults | Simple | No user customization |
| C. Configurable with sensible defaults | Spec-compliant; user-overridable | Config surface area |

## Decision

Option C — configurable Resource and InstrumentationScope with sensible defaults (D34, D35, D42).

**Resource defaults**:
- `service.name = sol/<source_type>` (e.g., `sol/host_metrics`, `sol/statsd`)
- `host.name` = auto-detected via `hostname()`, suppressible with `""`

**InstrumentationScope defaults**:
- `name = sol/<source_type>`, `version = <build_version>`

**User override** via config:
```toml
[sources.my_host_metrics]
type = "host_metrics"
resource_attributes.service.name = "my-infra"
resource_attributes.host.name = ""  # suppress auto-detection
```

**Sink-side label promotion** (D42): Non-OTLP sinks propagate selected Resource attributes as metric labels/tags:
```toml
# Prometheus
resource_to_labels = ["service.name", "host.name"]
# InfluxDB
resource_to_tags = ["service.name", "host.name"]
```

Implemented via shared helper in `source_otel.rs` for consistency across all sources.

## Consequences

- All metric-producing sources emit OTel-compliant Resource and Scope
- Backends can filter/group by `service.name` and `host.name` out of the box
- Non-OTLP sinks (Prometheus, InfluxDB, StatsD) can promote Resource attributes to labels
- Users can override defaults per-source in config
