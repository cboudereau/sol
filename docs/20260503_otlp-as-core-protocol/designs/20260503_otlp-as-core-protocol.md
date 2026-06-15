# OTLP as Core Protocol — Design

## Context

Sol (**S**ingle **O**bservability **L**ayer) is a true fork of [Vector](https://github.com/vectordotdev/vector), rebuilt around an OpenTelemetry-centric core. See [MARKET.md](../../otlp-as-core-protocol-plan/MARKET.md) for the full product vision and market positioning.

Vector's original architecture used proprietary internal types (`LogEvent`, `Metric` with `MetricValue`, `TraceEvent`) that required lossy double-conversion for OTLP traffic: OTel proto → Vector types → OTel proto. With OTLP becoming the de facto observability standard (71% adoption), this overhead was unjustifiable. Additionally, vendor-specific types (`AgentDDSketch`, `DatadogMetricOriginMetadata`) were embedded in core, coupling the pipeline to a single vendor.

Sol replaces all proprietary core types with OTel-native types. The original Vector wire protocol is retained as a source-scoped adapter for backward compatibility.

```rust
pub enum Event {
    Log(OtelLog),       // OpenTelemetry LogRecord
    Metric(OtelMetric), // OpenTelemetry Metric (Sum, Gauge, Histogram, ExponentialHistogram, Summary†)
    Trace(OtelSpan),    // OpenTelemetry Span
}
// † Summary is a legacy OTLP type — passthrough only, never produced by Sol sources.
```

All legacy core types (`LogEvent`, `Metric`, `TraceEvent`, `MetricValue`, `MetricData`, `NativeSerializer`, DD sinks) are deleted from core. The original `event.proto`/`vector.proto` is retained as a source-scoped adapter.

## Architecture

```
Sources (adapters)              Core (OTel-native)                    Sinks (adapters)
──────────────────────────────  ────────────────────────────────────  ───────────────────────
opentelemetry (gRPC + HTTP)     OtelLog  (LogRecord)                  opentelemetry (gRPC+HTTP)
datadog_agent ──────────────►   OtelMetric (Sum/Gauge/Histogram/  ──► prometheus, influxdb
vector (native gRPC) ─────►     ExponentialHistogram/Summary†)  ──► kafka, loki, ES, …
kafka, syslog, … ──────────►   OtelSpan (Span)
                                OtelAttributes (BTreeMap wrapper)
                                Disk buffer: otlp_buffer.proto
```

### What Sol changes from the original Vector

| Aspect | Original Vector | Sol |
|--------|----------------|-----|
| **Core event model** | Proprietary types (`LogEvent`, `Metric`, `TraceEvent`) | OTel-native (`OtelLog`, `OtelMetric`, `OtelSpan`) |
| **Wire protocol** | Custom `event.proto` / `vector.proto` | OTLP (proto + JSON) — the standard |
| **OTLP support** | Partial (source + sink, but not core) | Native — OTLP IS the core |
| **Vendor types in core** | DD sketches, `MetricValue`, `StatisticKind` | None — vendor logic in adapters only |
| **`vector` sink** | Custom proto → another Vector | Deleted — use `opentelemetry` sink |
| **`vector` source** | Custom proto only | Dual-protocol: OTLP + original native gRPC on same port |
| **`opentelemetry` source** | Exists in original | OTLP gRPC + HTTP ingestion (separate from `vector` source) |

### Data flow

```
Original Vector ── vector sink (native proto) ──► vector source ──────► Core (OtelLog,
                                                                              OtelMetric,
OTel Collector  ── otlp exporter ──────────────► opentelemetry source ─► OtelSpan)

Sol / any OTLP  ── opentelemetry sink ─────────► opentelemetry source ─►
```

## Principles

1. **OTLP/OTel is the only core protocol.** No vendor types in core.
2. **Two-format rule.** OTLP/proto or OTLP/JSON only. No flat canonical format.
3. **Vendor logic in adapters only.** Core never depends on adapters.
4. **Zero `vector.*` attributes.** Pipeline-internal state (set values, gauge delta kind) lives in dedicated struct fields on `OtelMetric`, never in OTLP attributes. OTLP output is pure — no custom extensions.
5. **Summary is legacy passthrough.** The OTLP spec says "not recommended for new applications." Sol never produces Summary — all new histogram-like data uses Histogram or ExponentialHistogram. Summary data received from Prometheus clients or legacy Vector sources passes through unchanged.
6. **Features preserved.** Tail sampling, load balancing, span_metrics, aggregate — all OTel-native.
7. **Original Vector protocol supported at source boundary.** The `vector` source speaks the original native gRPC protocol for backward compatibility. OTLP ingestion is handled by the `opentelemetry` source. Native proto definitions live in source scope — adapter code, not core types.

## Migration Guide for Users

### Strategy 1 — VRL Migration Tool (recommended)

Run `vector vrl-migrate` to auto-rewrite VRL programs for the new event model (~91% coverage):

```bash
vector vrl-migrate --config /etc/vector/vector.toml --dry-run   # preview
vector vrl-migrate --config /etc/vector/vector.toml              # apply
```

Key rewrites: `.message` → `.body`, `.timestamp` → `%vector.timestamp`, `.tags."key"` → `.attributes."key"`. See [VRL migration tool spec](../../otlp-as-core-protocol-plan/VRL_MIGRATION_TOOL.md) for the full rule set.

### Strategy 2 — Direct Vector-to-Sol Connection (native protocol)

Existing Vector instances send data using their standard `vector` sink with zero configuration changes:

```toml
# Old Vector (sender) — no changes needed
[sinks.bridge]
type = "vector"
address = "sol-host:6000"

# Sol (receiver)
[sources.from_old_vector]
type = "vector"
address = "0.0.0.0:6000"
```

### Strategy 3 — OTel Collector as Bridge (optional)

For environments already running the OTel Collector as an intermediary.

### Breaking Changes

| Component | Change | Migration |
|-----------|--------|-----------|
| **VRL paths** | `.message` → `.body`, metadata moved | Run `vector vrl-migrate` |
| **Vector sink** | Deleted | Replace `type = "vector"` with `type = "opentelemetry"` |
| **logfmt output** | Attribute keys namespaced | Update downstream parsers |
| **GELF output** | Fields mapped from proto | Update GELF consumers |
| **Avro/protobuf output** | Schema must match OTLP/JSON layout | Update schemas |
| **Lua scripts** | Structured layout: `event.log.attributes.key` | Update scripts manually |
| **JSON output** | OTLP/JSON (camelCase, nested) | Update JSON parsers |
| **Transformer paths** | OTLP-aware: `body`, `attributes.X`, `resource.X` | Update configs |

## OTel Fidelity

### Core Type Deviations

| Deviation | Fidelity concern | Status |
|-----------|------------------|--------|
| `OtelAttributes` (BTreeMap) | Lossless conversion at proto boundaries | Keep |
| `EventMetadata` sidecar | Pipeline infrastructure, never in OTLP output | Keep |
| `set_values: Option<BTreeSet<String>>` field | Pipeline-internal for set merge, never serialized | Keep |
| `kind_override: Option<MetricKind>` field | Pipeline-internal for Gauge delta, never serialized | Keep |
| Summary passthrough | Legacy OTLP type, received only, never produced | Passthrough only |

**Verdict:** Pure OTLP. Zero `vector.*` attributes exist in the codebase.

### Source-Side Gaps (all resolved)

All metric-producing sources now emit with Resource (`service.name`, `host.name`), InstrumentationScope (`sol/<source_type>`), UCUM unit fields, and proper timestamps. StatsD source uses ExponentialHistogram with flush-interval aggregation.

### Sink Metric Type Support

| Sink | Sum | Gauge | Histogram | ExpHist | Summary† | Set |
|------|:---:|:-----:|:---------:|:-------:|:--------:|:---:|
| **OTLP (gRPC+HTTP)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Prometheus** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **InfluxDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **GreptimeDB** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **CloudWatch** | ✅ | ✅ | ✅ | ✅ | ❌ drop | ✅ |
| **StatsD** | ✅ | ✅ | ✅ | ❌ error | ❌ error | ✅ |
| **Splunk HEC** | ✅ | ✅ | ❌ error | ❌ error | ❌ error | ❌ error |
| **Sematext** | ✅ | ✅ | ❌ error | ❌ error | ❌ error | ❌ error |

† Summary is legacy OTLP — passthrough only, never produced by Sol.

## Performance

| Path | Cost | Status |
|------|------|--------|
| OTLP gRPC → OTLP gRPC (passthrough) | Zero conversion | Optimal |
| VRL read-only (filter, route, sample) | Original proto returned | Optimal |
| VRL mutating (remap with writes) | Proto→Value at entry, Value→Proto at exit | ~5% regression |
| VRL attribute lookup | BTreeMap O(log n) + Value clone | ~5% regression |
| VRL complex paths | Direct ObjectMap, no Value wrapper | Fixed |
| Codec encoding | Direct ObjectMap from proto | Fixed |

See [performance and tradeoffs analysis](../../otlp-as-core-protocol-plan/PERFORMANCE_AND_TRADEOFFS.md) for the full comparison with the Vector native protocol.

## Known Limitations

### H1. ExponentialHistogram `add()` rejects mismatched scale/offset (P0)

`add()` requires identical `scale`, `offset`, and `bucket_counts.len()`. Two ExpHists recording different value ranges silently reset instead of merging in the `aggregate` transform. Fix: implement rescale-and-shift before merge (~100 lines).

### H2. Non-monotonic Sum normalized as counter instead of gauge (P1)

`is_gauge()` only checks proto `Data::Gauge`, returning `false` for non-monotonic `Data::Sum`. Affects all sink normalizers for OTel UpDownCounters.

### H3. VRL reconstruction drops histogram/ExpHist/Summary fields (P1)

`otel_metric_event_to_value` projects only a subset of data point fields. Modifying any field in VRL silently loses min/max/exemplars/quantiles.

### H5. OTLP HTTP sink retries 4xx client errors (P2)

`is_retriable_error()` returns `true` unconditionally. Should retry on 429 and 5xx only.

### H6. Histogram/ExpHist/Summary JSON serialization incomplete (P2)

Custom `Serialize` impls for histogram data points omit `startTimeUnixNano`, `min`, `max`, `exemplars`, `flags`.

### H7. GCP Stackdriver drops histograms after ExpHist conversion (P2)

The sink filter drops all non-Sum/non-Gauge metrics before normalization runs.

## Decisions

| ADR | Title |
|-----|-------|
| [0002](../adrs/20260503_otlp-as-sole-core-protocol.md) | OTLP as sole core protocol |
| [0003](../adrs/20260503_vector-source-sink-restructure.md) | Vector sink deleted, native proto at source boundary |
| [0004](../adrs/20260503_exponential-histogram-strategy.md) | ExponentialHistogram as internal histogram format |
| [0005](../adrs/20260503_pipeline-internal-struct-fields.md) | Pipeline-internal state as struct fields, not attributes |
| [0006](../adrs/20260503_statsd-otlp-compliance.md) | StatsD source OTLP-compliant redesign |
| [0007](../adrs/20260503_source-resource-scope-conventions.md) | Source resource and scope conventions |
| [0008](../adrs/20260503_sink-normalization-strategy.md) | Sink normalization strategy |
| [0009](../adrs/20260503_non-otlp-codec-encoding.md) | Non-OTLP codec encoding strategy |

## Reference Analysis Documents

These documents contain the detailed analysis that informed the design decisions:

- [Market study](../../otlp-as-core-protocol-plan/MARKET.md) — Product vision and competitive positioning
- [Migration complexity study](../../otlp-as-core-protocol-plan/MIGRATION_STUDY.md) — Component-by-component analysis of the migration
- [Protocol gap analysis](../../otlp-as-core-protocol-plan/PROTOCOL_GAP_ANALYSIS.md) — Field-by-field comparison of Vector native vs OTLP
- [Performance and tradeoffs](../../otlp-as-core-protocol-plan/PERFORMANCE_AND_TRADEOFFS.md) — Native protocol optimizations and migration impact
- [Architectural guidelines](../../otlp-as-core-protocol-plan/GUIDELINES.md) — Dependency model and PR checklist
- [VRL migration tool](../../otlp-as-core-protocol-plan/VRL_MIGRATION_TOOL.md) — `vector vrl-migrate` specification
- [VRL OTel-native targets](../../otlp-as-core-protocol-plan/VRL_OTEL_NATIVE_TARGETS.md) — VRL path model for OTel types
- [VRL datapoint context](../../otlp-as-core-protocol-plan/VRL_DATAPOINT_CONTEXT.md) — `.attributes` shorthand for metric data points
