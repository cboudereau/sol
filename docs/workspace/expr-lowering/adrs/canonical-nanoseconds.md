---
status: draft
---
# Canonical nanosecond units; convert only at the boundary

Addresses: [FR7](../DESIGN.md#fr7)

## Problem

Unit handling is scattered: ingress parsers (`parse_time_ns`, `parse_step_ns`,
`loki_step_ns`), egress `ts as f64 / 1e9`, three separate duration parsers (PromQL
`Duration::as_nanos`, TraceQL `duration_nanos`, LogQL raw `[5m]`), and pervasive
`CAST(time_unix_nano AS BIGINT)`. Internally everything is *already* nanoseconds
(storage is `Timestamp(Nanosecond)`), but the conversions aren't centralized and
`i64`/`f64`/seconds get mixed by hand — an error class, and the chief risk in the
`*_over_time` window frame (frame units must match the `ORDER BY` key).

## Options

| Option | Pros | Cons |
|---|---|---|
| **Canonical ns `i64` core, convert at boundary, newtypes** | Matches OTLP `*_unix_nano` + Arrow `Timestamp(Nanosecond)`; one place per direction; type-safe; removes window-frame unit ambiguity | A small refactor; newtypes touch signatures |
| Canonical seconds (`f64`) | Matches Prometheus wire | Lossy for ns; mismatches storage/OTLP; worse for windows |
| Status quo (ad-hoc) | No work | The sprawl + mixing-bug class persists |

## Decision

**Internal time and duration are nanoseconds `i64`, wrapped in `TimeNs` /
`DurationNs` newtypes.** Conversions exist **only** at:
- **Ingress** — HTTP param parsers (sec→ns for Prometheus/Tempo; Loki already ns) and
  a **single** `parse_duration_ns` for PromQL `[5m]`, TraceQL `1.5s`, LogQL
  `[5m]`/`offset`.
- **Egress** — response serializers (ns→sec for Prometheus output only; Loki emits
  ns; Tempo durations are ns).

No `* 1e9` / `/ 1e9` / `CAST … AS BIGINT` unit handling in core. Sample **values**
stay `f64` (Prometheus is float by spec; not standardized). `CAST(time_unix_nano AS
BIGINT)` was never a unit conversion (ns→ns reinterpret of the Timestamp column); in
the `Expr` world it becomes a single typed accessor or disappears.

## Consequences

**Easier**: one ingress + one egress conversion site each; type system prevents
sec/ms/ns mixing; window-frame bounds and `ORDER BY` keys are both ns `i64` (removes
the P7 frame-unit risk); duration parsing unified.

**Harder**: newtypes ripple through internal signatures (mechanical); `i64` ns
overflows ~year 2262 (out of scope); fractional-second Prometheus timestamps are
preserved by the `f64` egress conversion, not lost.
