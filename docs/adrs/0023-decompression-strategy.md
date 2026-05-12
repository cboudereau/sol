---
status: accepted
---
# Decompression strategy for gRPC requests

Addresses: [FR1](../DESIGN.md#fr1), [FR4](../DESIGN.md#fr4), [NFR3](../DESIGN.md#nfr3)

## Problem

The `DecompressionAndMetrics` layer creates heavy per-request infrastructure (mpsc channel, StreamBody, select loop, body-forwarding state machine) for ALL gRPC requests, including uncompressed ones. Additionally, tonic's `.accept_compressed(CompressionEncoding::Gzip)` is also configured, creating a dual decompression setup where the layer decompresses before tonic sees the body.

How should decompression and byte-size metrics be handled?

## Options

| Option | Pros | Cons |
|---|---|---|
| A: Keep layer, short-circuit uncompressed | Minimal code change. BytesReceived preserved. | Still pays channel+select cost for compressed requests. Layer remains complex. |
| B: Remove layer entirely, use tonic built-in, emit BytesReceived in handler | Removes ALL per-request layer overhead. Tonic handles decompression natively (well-tested, zero-copy). Matches HTTP pattern exactly. | Must emit BytesReceived in the gRPC handler (trivial — HTTP already does this). |
| C: Keep layer, short-circuit uncompressed, optimize compressed path | Uncompressed gets zero overhead. Compressed path streamlined. | More implementation effort. Layer still exists. |

## Decision

**Option B**: Remove the `DecompressionAndMetrics` layer entirely. Use tonic's built-in `.accept_compressed(CompressionEncoding::Gzip)` for decompression. Emit `BytesReceived` inside the gRPC `Service::handle_events` — exactly as the HTTP path does.

### How HTTP does it (the model to follow)

HTTP path (`src/sources/opentelemetry/http.rs:219-229`):
1. Warp receives the raw body
2. `decompress_body()` decompresses if needed
3. `bytes_received.emit(ByteSize(decoded_body.len()))` — emits the metric **in the handler**
4. Decode protobuf

This pattern has no tower middleware overhead and achieves ~5,000/s.

### How gRPC should work (after this change)

1. Tonic receives the request and decompresses via built-in `.accept_compressed(Gzip)` — zero-copy, no intermediate channel
2. Tonic deserializes the protobuf into the request type
3. `Service::export()` is called
4. In `handle_events`, emit `bytes_received.emit(ByteSize(...))` from the request body size
5. Process events

### What gets deleted

- `DecompressionAndMetricsLayer` removed from `run_grpc_server` and `run_grpc_server_with_routes`
- `src/sources/util/grpc/decompression.rs` — entire file deleted (or kept only if `sources::vector` still needs it)

### What gets added

- `bytes_received: Registered<BytesReceived>` field already exists on `Service` as `events_received` sibling — add the bytes metric
- `bytes_received.emit(ByteSize(byte_size))` in `handle_events` after computing event size

## Consequences

- Removes ~300 lines of complex middleware code (channel, StreamBody, select, state machine)
- Zero per-request overhead from the decompression layer
- gRPC path mirrors HTTP path for metrics emission — consistent, simple
- Tonic's compression is battle-tested (used by all Go gRPC services)
- `sources::vector` also uses `run_grpc_server_with_routes` — the layer removal affects it too (same benefit)
- The `grpc-accept-encoding` response header is now managed by tonic instead of the layer
