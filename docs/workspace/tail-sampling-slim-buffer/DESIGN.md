# Tail Sampling Slim Buffer — Design Doc

## Context

Amends: [designs/20260514_arc-zero-copy-optimization.md](../../designs/20260514_arc-zero-copy-optimization.md)

The [Arc zero-copy optimization](../../designs/20260514_arc-zero-copy-optimization.md) reduced tail sampling memory from 1.62x to 1.23x of otelcol (247 MiB vs 200 MiB at 10k spans/s). Root cause analysis of the remaining 47 MiB gap traced it to **per-span struct overhead** — not field content (telemetrygen spans have empty events/links).

### Per-span memory breakdown

Measured struct sizes (`std::mem::size_of`):

| Type | Inline size |
|---|---|
| `Event` enum | 344 bytes |
| `OtelSpan` | 344 bytes |
| `Span` (prost proto) | 264 bytes |
| `EventMetadata` | 24 bytes (Arc pointer + Option\<Instant\>) |
| `OtelAttributes` | 24 bytes (Vec header) |

Per-span heap allocations (telemetrygen benchmark):

| Allocation | Size | Notes |
|---|---|---|
| `Arc<Inner>` (EventMetadata) | **~216 bytes** | Fresh `Inner::default()` per span: Value, Secrets, ObjectMap, Uuid |
| `trace_id` Vec\<u8\> data | 16 bytes | Always 16 bytes, allocated as Vec |
| `span_id` Vec\<u8\> data | 8 bytes | Always 8 bytes, allocated as Vec |
| `parent_span_id` Vec\<u8\> data | 8 bytes | Always 8 bytes, allocated as Vec |
| `name` String data | ~20 bytes | Variable |
| `span_attrs` Vec backing | ~400 bytes | ~5 attrs x ~80 bytes |
| Allocator overhead (IDs) | ~48 bytes | 3 small allocations x ~16 bytes |

### Two root causes

**1. Per-span `EventMetadata` allocation (~216 bytes/span, ~65 MiB at 300k spans)**

`resource_spans_into_events()` (line 41) creates `EventMetadata::default()` for every span. Each call does `Arc::new(Inner::default())` — a fresh heap allocation containing:
- `Value::Object(ObjectMap::new())` — empty IndexMap header
- `Secrets::new()` — empty BTreeMap header
- `EventFinalizers::default()` — empty Vec
- `ObjectMap::new()` — empty IndexMap for dropped_fields
- `Some(Uuid::new_v4())` — random UUID per span
- Arc control block (strong + weak counts)

All spans from the same batch should share a single `Arc<Inner>`, exactly like `resource` and `scope` already do. The source pipeline sets `source_id`, `source_type`, and `upstream_id` after event creation — these are per-source, not per-span.

**2. Vec\<u8\> for fixed-size ID fields (~48 bytes/span, ~14 MiB at 300k spans)**

prost generates `Vec<u8>` for protobuf `bytes` fields. But trace_id is always 16 bytes, span_id/parent_span_id always 8 bytes. Each Vec is 24 bytes inline + heap allocation for the data. Go's otelcol uses `[16]byte`/`[8]byte` (inline, no heap).

Using `[u8; 16]`/`[u8; 8]` in `OtelSpan` (extracted from proto Span via `mem::take`, restored at serialization) eliminates 3 heap allocations per span.

### Key files

- `lib/opentelemetry-proto/src/spans.rs:7-45` — `resource_spans_into_events()` creates EventMetadata per span
- `lib/opentelemetry-proto/src/logs.rs` — same pattern for logs
- `lib/opentelemetry-proto/src/metrics.rs` — same pattern for metrics
- `lib/sol-core/src/event/metadata.rs:201-224` — `Inner::default()` and `EventMetadata::default()`
- `lib/sol-core/src/event/otel_event.rs:2423-2431` — `OtelSpan` struct definition
- `src/transforms/tail_sampling/transform.rs:96-108` — `extract_trace_id()` copies Vec to [u8;16]
- `src/transforms/servicegraph/transform.rs:434-447` — `to_trace_id()`/`to_span_id()` copy Vec to arrays
- `src/sinks/opentelemetry/load_balancing.rs:366-385` — `extract_routing_key()` clones trace_id Vec

## Functional Requirements

### <a id="fr1"></a>FR1 — Share EventMetadata across batch

`resource_spans_into_events()` (and the log/metric equivalents) must create one `EventMetadata` per batch and share it via `Arc::clone()` across all spans/logs/metrics in the batch.

### <a id="fr2"></a>FR2 — Preserve per-event metadata mutation

Downstream transforms may call `metadata.get_mut()` (which uses `Arc::make_mut`) to set source_id, source_type, upstream_id, or finalizers. Copy-on-write semantics via `Arc::make_mut` must continue to work — only the mutated event gets a private copy.

### <a id="fr3"></a>FR3 — Fixed-size trace_id in OtelSpan

Add `trace_id: [u8; 16]` field to `OtelSpan`. Extract from `span.trace_id` via `mem::take` on construction. Restore to `span.trace_id` in `span_to_proto()` and `into_parts()`.

### <a id="fr4"></a>FR4 — Fixed-size span_id and parent_span_id in OtelSpan

Same pattern for `span_id: [u8; 8]` and `parent_span_id: [u8; 8]`.

### <a id="fr5"></a>FR5 — Fixed-size trace_id/span_id in OtelLog

Add `trace_id: [u8; 16]` and `span_id: [u8; 8]` to `OtelLog`. Extract from `record.trace_id`/`record.span_id` on construction. Restore at serialization.

### <a id="fr6"></a>FR6 — Update all accessor methods

`OtelSpan::trace_id()`, `span_id()`, `parent_span_id()` must return `&[u8; N]` (or `&[u8]`) from the new fixed-size fields. `OtelLog::trace_id()`, `span_id()` similarly.

### <a id="fr7"></a>FR7 — Update VRL roundtrip

`as_map()` must read from the new fixed-size fields. `apply_value_map()` / `from_value_map()` must write to the new fields (and the proto field for serialization).

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Tail sampling memory within 1.0x of otelcol

Baseline (post Arc-sharing): Sol 247 MiB vs otelcol 200 MiB = 1.23x at 10k.
Target: ≤1.0x (~200 MiB). EventMetadata sharing alone should reach ~182 MiB (0.91x).

### <a id="nfr2"></a>NFR2 — No throughput regression

All scenarios currently at ≥95% of otelcol must remain at ≥95%. Fewer allocations should improve throughput.

### <a id="nfr3"></a>NFR3 — All existing tests pass

CI checks: `cargo fmt --all --check`, `cargo clippy -p sol-core`, `cargo test -p sol-core --features vrl --lib`, full `cargo check`.

## Non-goals

- **Removing EventMetadata entirely from buffered spans**: EventMetadata carries finalizers needed for acknowledgment. Sharing via Arc is sufficient.
- **Custom prost codegen for fixed-size bytes**: modifying the prost build to generate `[u8; N]` instead of `Vec<u8>`. Too invasive; extract-and-restore at the OtelSpan level is sufficient.
- **Stripping events/links/trace_state**: telemetrygen produces empty fields, so no benchmark gain. Deferred to a future workspace if production profiling shows benefit.
- **Arena allocation**: allocating all spans from one trace in a contiguous block. Higher complexity, uncertain benefit beyond fixed IDs.

## Rabbit holes

- **EventMetadata identity**: if any downstream code depends on per-span UUID uniqueness (`source_event_id`), sharing will break it. Cap: search for `source_event_id` usage. If used only for deduplication in sinks, evaluate whether trace_id+span_id is a better dedup key.
- **OtelLog/OtelMetric scope**: fixed-size IDs apply to OtelLog (trace_id, span_id for trace context). OtelMetric has no ID fields. Cap: implement for OtelSpan first, then OtelLog. Skip OtelMetric.
- **Downstream type expectations**: some code may expect `trace_id()` to return `&[u8]` (slice) vs `&[u8; 16]` (array ref). Cap: return `&[u8]` for backward compatibility — `&[u8; 16]` auto-derefs to `&[u8]`.

## Design

### EventMetadata sharing ([FR1](#fr1), [FR2](#fr2))

In `resource_spans_into_events()`, create one `EventMetadata::default()` before the iterator and share via `Arc::clone()` for each span:

```rust
pub fn resource_spans_into_events(rs: ResourceSpans) -> impl Iterator<Item = Event> {
    let metadata = EventMetadata::default(); // one allocation
    // ...
    scope_spans.spans.into_iter().map(move |mut span| {
        Event::Trace(OtelSpan::from_parts_shared(
            // ...
            metadata.clone(), // Arc clone = atomic refcount bump
        ))
    })
}
```

`EventMetadata::clone()` is already cheap — it clones the `Arc<Inner>` (refcount bump) and copies `Option<Instant>`. Mutations via `get_mut()` use `Arc::make_mut()` for copy-on-write, exactly like `resource_attrs` and `scope_attrs`.

Same pattern applied to `resource_logs_into_events()` and `resource_metrics_into_events()`.

### Fixed-size IDs ([FR3](#fr3), [FR4](#fr4), [FR5](#fr5), [FR6](#fr6), [FR7](#fr7))

Extract ID bytes from proto on construction, store as fixed arrays in OtelSpan:

```rust
pub struct OtelSpan {
    pub(crate) span: Span,
    pub(crate) trace_id: [u8; 16],       // extracted from span.trace_id
    pub(crate) span_id: [u8; 8],         // extracted from span.span_id
    pub(crate) parent_span_id: [u8; 8],  // extracted from span.parent_span_id
    pub(crate) span_attrs: OtelAttributes,
    // ... rest unchanged
}
```

Construction helper:
```rust
fn take_trace_id(v: &mut Vec<u8>) -> [u8; 16] {
    let mut id = [0u8; 16];
    let len = v.len().min(16);
    id[..len].copy_from_slice(&v[..len]);
    v.clear(); // release heap allocation
    id
}
```

Note: `v.clear()` sets len=0 but retains capacity. To free heap: `*v = Vec::new()` or `std::mem::take(v)`. Use `std::mem::take` to guarantee deallocation.

Serialization (`span_to_proto`):
```rust
pub fn span_to_proto(&self) -> Span {
    let mut span = self.span.clone();
    span.trace_id = self.trace_id.to_vec();
    span.span_id = self.span_id.to_vec();
    span.parent_span_id = self.parent_span_id.to_vec();
    span.attributes = self.span_attrs.to_key_values();
    span
}
```

### Decisions

- [Shared EventMetadata strategy](./adrs/shared-metadata.md)
- [Fixed-size IDs extract-and-restore](./adrs/fixed-size-ids.md)

## Cross-cutting Concerns

- **OtelLog parity**: OtelLog has `record.trace_id` and `record.span_id` for trace context. Apply the same fixed-size extraction. OtelMetric has no ID fields.
- **Benchmark validation**: run `demo/benchmark` tail-sampling-grpc-10k and sustained scenarios to measure actual memory reduction.
- **Correctness**: round-trip tests must verify span_to_proto output is identical to input for both ID fields and metadata.
