# OTLP Proto Unification — Design Doc

Prior work: [grpc-perf](../../designs/20260512_grpc-perf.md)

## Context

After the grpc-perf work (TCP_NODELAY, H2 adaptive window, DecompressionAndMetrics removal, SharedSourceSender), Sol achieved a **33x improvement** on unbatched gRPC — but a ~10% gap to otelcol remains:

| Scenario | Sol | otelcol | Sol/otelcol |
|----------|-----|---------|-------------|
| logs gRPC 10k (1 event/req) | 3,438/s | 3,844/s | 89% |
| metrics gRPC 10k (1 event/req) | 3,767/s | 4,153/s | 91% |
| traces gRPC 10k (batched) | 14,910/s | 15,052/s | 99% |

Traces are at parity (99%) because batching amortizes per-event overhead across many spans per request. Logs and metrics expose it fully — 1 event per gRPC request.

### Root cause: proto encode-decode round-trips

Sol maintains **two sets of identical OTLP proto types** generated from the same `.proto` files:

1. **Local types** (`sol-opentelemetry-proto::proto::*`) — compiled by Sol's `build.rs` with `tonic_build`. Used for gRPC service stubs, `DESCRIPTOR_BYTES`, buffer codec, and sink export.
2. **Upstream types** (`upstream_opentelemetry_proto::tonic::*`) — from the `opentelemetry-proto v0.27` crate. Stored inside `OtelLog`/`OtelSpan`/`OtelMetric` event wrappers. Used by all downstream processing (transforms, conditions, VRL, serde).

To convert between them, every event goes through **3 encode-decode round-trips** (resource, scope, record):

```
local proto → encode_to_vec() → bytes → decode() → upstream proto
```

This happens at:
- **Source ingestion** (3 round-trips per event) — `into_otel_event_iter()` in `logs.rs`, `spans.rs`, `metrics.rs`
- **Sink export** (3 round-trips per event) — `otel_*_event_to_resource_*()` in `sinks/opentelemetry/grpc.rs`
- **Buffer codec** (3 round-trips per event on decode) — `batch_to_event_array()` in `buffer_codec.rs`

For a source-to-sink pass-through pipeline, that's **6 round-trips per event** (18 allocations + 18 deallocations).

otelcol uses the OTLP protos directly as its internal format (`pdata`). Zero conversion.

### Why two proto crate copies exist

The split exists because `sol-core` (event model) depends on `opentelemetry-proto v0.27` for `OtelLog`/`OtelSpan`/`OtelMetric` field types, while `sol-opentelemetry-proto` generates its own proto types for tonic service stubs and `DESCRIPTOR_BYTES`. Both crates use tonic 0.12, so the types are structurally identical but Rust treats them as distinct.

### Compatibility fact: upstream crate has tonic service stubs

The upstream `opentelemetry-proto v0.27` crate with `features = ["full"]` (already enabled by Sol) generates **complete tonic server stubs** — `LogsService`, `TraceService`, `MetricsService` traits. Both Sol and the upstream crate use tonic 0.12, so these stubs are directly usable.

## Functional Requirements

### <a id="fr1"></a>FR1 — Eliminate source-side proto round-trips

Use upstream proto types as the canonical types for tonic gRPC service stubs. `into_otel_event_iter()` must wrap decoded protos directly into `OtelLog`/`OtelSpan`/`OtelMetric` with zero encode-decode.

### <a id="fr2"></a>FR2 — Eliminate sink-side proto round-trips

Sink export functions must extract proto types directly from `OtelLog`/`OtelSpan`/`OtelMetric` and build `ExportXxxServiceRequest` without encode-decode.

### <a id="fr3"></a>FR3 — Unify buffer codec proto types

`OtlpBufferBatch` must use the same proto types as the rest of the pipeline. No encode-decode when reading events from disk buffers.

### <a id="fr4"></a>FR4 — Retain DESCRIPTOR_BYTES for reflection-based codecs

The OTLP encoder/decoder in `lib/codecs/` uses `DESCRIPTOR_BYTES` for proto reflection. This must continue to work.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Close the gap with otelcol on unbatched gRPC

Unbatched logs/metrics gRPC throughput must reach ≥95% of otelcol (target: ≥3,650/s for logs, ≥3,950/s for metrics).

### <a id="nfr2"></a>NFR2 — No regression on existing scenarios

Batched traces, HTTP, gzip — all must stay at current levels or improve.

### <a id="nfr3"></a>NFR3 — Wire-format compatibility for disk buffers

Existing disk buffers encoded with local proto types must decode correctly after the change. Both type sets are generated from the same `.proto` files, so wire format is identical — but this must be verified.

## Non-goals

- **Tonic version upgrade** (0.12 → 0.13+): potentially beneficial for h2 performance, but requires upstream `opentelemetry-proto` compatibility. Deferred — can be explored independently after proto unification.
- **Further H2 tuning**: TCP_NODELAY, adaptive window, keepalive already configured in grpc-perf. No further server-side tuning identified.
- **Removing `opentelemetry-proto` upstream dependency**: Sol already depends on it for serde support and event field types in sol-core. Removing it would require adding serde derives to local proto generation — high effort, low value.
- **Eliminating `otel-proto-types` crate**: appears unused in production code (only referenced in comments). Can be cleaned up separately.

## Rabbit holes

- **DESCRIPTOR_BYTES generation without local types**: Sol's `build.rs` generates both Rust types and descriptor bytes from the same `tonic_build::configure()` call. We need descriptor bytes but not the Rust types (since we'll use upstream types). Verify that `build_server(false).build_client(false)` still produces descriptor bytes, or use `prost_build` directly. Cap: 30 min investigation, fall back to keeping local type generation if needed — the types would simply be unused.
- **Buffer codec wire-format migration**: existing disk buffers use local proto types. Upstream types have identical wire format, but prost might generate slightly different default values for oneof fields. Verify with a round-trip test. Cap: 30 min.

## Design

### Approach

```
┌─────────────────────────────────────────────────────────┐
│  Current: two type sets with encode-decode bridge       │
│                                                         │
│  tonic decodes → local proto types                      │
│    → encode_to_vec() → bytes → decode()                 │
│      → upstream proto types (stored in Event)           │
│        → encode_to_vec() → bytes → decode()             │
│          → local proto types (sink export)               │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Target: single type set, zero conversion               │
│                                                         │
│  tonic decodes → upstream proto types                   │
│    → wrap in Event (zero-copy)                          │
│      → unwrap from Event → build request (zero-copy)    │
└─────────────────────────────────────────────────────────┘
```

Use upstream `opentelemetry-proto v0.27` types as the **single canonical proto type set**:

1. **gRPC services** implement upstream tonic service traits directly (`upstream_opentelemetry_proto::tonic::collector::*::*_service_server::*`)
2. **`into_otel_event_iter()`** receives upstream types from tonic, wraps directly in `OtelLog`/`OtelSpan`/`OtelMetric`
3. **Sink export** extracts upstream types from events, builds `ExportXxxServiceRequest` directly
4. **Buffer codec** uses upstream types for `OtlpBufferBatch`
5. **`DESCRIPTOR_BYTES`** still generated from local `.proto` files (build.rs), but no Rust data types generated

### Decisions

- [Proto type canonical source](./adrs/proto-canonical-source.md)

## Cross-cutting Concerns

- **Wire-format compatibility**: proto wire format is identical between local and upstream types (same `.proto` source). Disk buffer migration is transparent.
- **Backward compatibility**: no config changes. All existing Sol/Vector configs work unchanged.
- **Downstream consumers**: transforms, conditions, VRL all access proto fields through `OtelLog`/`OtelSpan`/`OtelMetric` accessors, which already use upstream types. No changes needed.
- **gRPC client stubs**: the OTLP sink's gRPC client also uses local types for building requests. After unification, it uses upstream types directly.

## Expected gains

| Area | Mechanism | Estimated impact |
|------|-----------|-----------------|
| Source ingestion | Eliminate 3 encode-decode round-trips per event | ~10% throughput gain on unbatched gRPC |
| Sink export | Eliminate 3 encode-decode round-trips per event | Proportional gain on OTLP sink pipelines |
| Buffer decode | Eliminate 3 encode-decode round-trips per event | Faster disk buffer reads |
| Combined with grpc-perf | TCP_NODELAY + H2 tuning + layer removal + proto unification | ~37-45x total vs original 103/s baseline |
