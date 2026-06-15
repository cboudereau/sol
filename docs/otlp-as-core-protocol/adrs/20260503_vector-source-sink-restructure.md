---
status: accepted
---
# Vector sink deleted, native proto at source boundary

Addresses: backward compatibility with existing Vector fleets

## Problem

The original Vector uses a proprietary gRPC protocol (`event.proto`/`vector.proto`) for inter-instance communication. With Sol using OTLP as its core protocol, how should Sol interact with existing Vector instances?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Delete both vector sink and source — OTLP only | Clean break; no legacy code | Existing Vector fleets cannot send to Sol without reconfiguration |
| B. Keep both vector sink and source | Full backward compat | Maintains redundant sink code; vector sink is just a worse OTLP sink |
| C. Delete vector sink, keep native proto in vector source | Zero-config migration for senders; no redundant sink | Source carries adapter complexity; dual-protocol on one port |

## Decision

Option C — delete the `vector` sink entirely and restore the native Vector protocol in the `vector` source as a backward-compatibility adapter (D21, D22, D24).

The `vector` source speaks **both OTLP and the original native gRPC protocol** on the same port:
- **OTLP**: LogsService, MetricsService, TraceService
- **Native Vector**: `service Vector { rpc PushEvents(...) }`

Native proto events are converted at the source boundary:
- `event.Log` → `OtelLog` (D29): `Log.value`→body, `Log.fields`→attributes, `message` key promoted to body if value absent
- `event.Metric` → `OtelMetric` (D28): Counter→Sum, Gauge→Gauge, Set→Gauge+set_values, Distribution→Histogram, AggHistogram→Histogram, AggSummary→Summary, Sketch→ExponentialHistogram
- `event.Trace` → `OtelSpan` (D30): best-effort extraction of trace_id/span_id/name/timestamps, rest→span attributes
- `interval_ms` → `startTimeUnixNano = timeUnixNano - interval_ms × 1_000_000` (D31)

To send data to another Sol instance, use `type = "opentelemetry"` directly — there is no `vector` sink.

## Consequences

- Existing Vector fleets can point their `vector` sink at Sol with zero config changes
- No bridge middlebox needed (no OTel Collector between old Vector and Sol)
- The `event.proto`/`vector.proto` definitions and conversion logic live entirely within `src/sources/vector/` — core remains pure OTLP
- Users previously using `type = "vector"` sinks must switch to `type = "opentelemetry"`
