---
status: accepted
---
# Non-OTLP codec encoding strategy

Addresses: how non-OTLP output formats (logfmt, GELF, Avro, protobuf, Lua, JSON) encode OTel-native events

## Problem

With OTel types as the core event model, every non-OTLP encoder must convert from proto structures. The original encoders operated on `Value`-based flat maps via `to_value_canonical()` and `convert_to_fields()`. These methods introduced unnecessary allocation (proto → Value → encoded output). How should each encoder access event data?

## Decision

Direct proto iteration — each encoder reads from `as_map()` (an `ObjectMap` built directly from proto fields) without going through `Value` wrapping. `to_value_canonical()` and `convert_to_fields()` are deleted (D1, D15).

Per-encoder decisions (D2–D10):

| Encoder | Strategy | Breaking change |
|---------|----------|----------------|
| **logfmt** (D2) | Namespace attribute keys: `attributes.my_attr=val`, proto fields flat | Yes — downstream parsers must expect namespaced keys |
| **GELF** (D3) | Direct proto mapping: `body`→`short_message`, `severity_number`→`level`, `time_unix_nano`→`timestamp`, `resource.host.name`→`host`, rest→`_attr` | Yes — field names change |
| **Avro** (D4) | OTLP/JSON via Serialize | Yes — schema must match OTLP/JSON layout (nested, camelCase) |
| **protobuf** (D5) | OTLP/JSON via Serialize → `encode_message` | Yes — descriptor must match OTLP/JSON field names |
| **Lua** (D6) | Structured layout: `{ body, attributes, resource, scope }` | Yes — `event.log.attributes.key` not `event.log.key` |
| **Arrow** (D7) | Iterate proto directly | Internal — no user-facing change |
| **honeycomb** (D8) | Serialize (OTLP/JSON) | Yes — `data` field uses OTLP/JSON layout |
| **new_relic** (D9) | Iterate proto + attrs | Transparent — NR API accepts any attributes |
| **influxdb/logs** (D10) | Iterate proto + attrs | Update tag/field key expectations |

Additional encoder decisions:
- **reduce** (D11): direct structured iteration
- **trace_to_log** (D12): transfer proto fields directly
- **schema/definition** (D13): proto-aware Kind inference
- **enrichment_tables** (D14): match on attributes directly
- **`get(event_root())`** (D16): returns OTLP/JSON-shaped Value

## Consequences

- No intermediate `Value` allocation for encoding — direct proto access
- All non-OTLP output formats produce OTLP-structured data (nested, camelCase where applicable)
- Downstream consumers of logfmt, GELF, Avro, protobuf, Lua, and JSON output must update parsers
- `to_value_canonical()` and `convert_to_fields()` are fully deleted from the codebase
