---
status: reverted
---
# Fixed-size IDs: extract-and-restore (REVERTED)

Addresses: [FR3](../designs/20260516_tail-sampling-slim-buffer.md#fr3), [FR4](../designs/20260516_tail-sampling-slim-buffer.md#fr4), [FR5](../designs/20260516_tail-sampling-slim-buffer.md#fr5), [NFR1](../designs/20260516_tail-sampling-slim-buffer.md#nfr1)

## Problem

prost generates `Vec<u8>` for protobuf `bytes` fields. trace_id (16 bytes), span_id (8 bytes), and parent_span_id (8 bytes) are always fixed-size but each gets a separate heap allocation. At 300k spans, this is 900k unnecessary small allocations (~14 MiB including allocator overhead).

## Options

| Option | Pros | Cons |
|---|---|---|
| **Extract to `[u8; N]` in OtelSpan** | No prost changes. Same pattern as attributes extraction. Eliminates 3 heap allocs/span. | Proto Span still has empty Vec\<u8\> fields (72 bytes inline, 0 heap). Serialization must restore. |
| **Custom prost bytes type** | Cleaner — proto struct itself uses fixed arrays. | Requires prost build customization. Complex. May break other proto consumers. |
| **Shadow fields (keep both)** | No proto modification. Simple reads. | Adds memory instead of saving it. |

## Decision

**Extract to `[u8; N]` in OtelSpan.** This follows the established pattern — attributes are already extracted from proto Span into OtelAttributes, then restored at serialization. IDs follow the same extract-on-construct, restore-on-serialize pattern.

Use `std::mem::take` to clear the proto Vec\<u8\> fields (deallocates heap). The empty Vec\<u8\> remains in the proto struct (24 bytes inline, 0 heap) — this is unavoidable without changing the generated proto type.

## Consequences

- **Easier**: tail sampling `extract_trace_id()` and service graph `to_trace_id()`/`to_span_id()` can read `[u8; 16]`/`[u8; 8]` directly instead of copying from Vec\<u8\>. Load balancing can hash `&[u8; 16]` without cloning.
- **Harder**: every OtelSpan constructor must extract IDs. Every serialization path must restore them. But this is already the case for attributes — the pattern is established.

## Reverted

Implemented and benchmarked. Measured savings: **0.5–7 MiB** — negligible compared to the ~86 MiB saved by Arc sharing + sorted Vec. The code complexity (touching every constructor, accessor, serialization path for OtelSpan and OtelLog) was not justified. Arc sharing + sorted Vec alone meet the ≤1.0x otelcol target (162 MiB = 0.82x).
