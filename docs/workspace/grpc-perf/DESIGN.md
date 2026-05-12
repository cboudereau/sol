# gRPC Per-Request Overhead — Design Doc

## Context

Benchmark comparison (Sol vs Vector vs otelcontribcol) revealed that Sol/Vector's gRPC receiver path is **~45x slower** than otelcol for unbatched requests and **~50x slower** than its own HTTP path.

| Scenario | Sol | Vector | otelcol |
|---|---|---|---|
| logs gRPC (none) | 98/s | 102/s | 4,406/s |
| logs gRPC (gzip) | 244/s | 262/s | 1,941/s |
| logs HTTP | 5,280/s | 5,093/s | 4,710/s |
| traces gRPC (batched) | 9,800/s | 9,801/s | 10,120/s |

Sol ≈ Vector confirms all overhead is inherited from the Vector codebase. With batched traces (many spans per gRPC call), the per-request overhead is amortized and Sol matches otelcol. With 1 event per gRPC call (logs, metrics), the overhead dominates.

The OTLP spec mandates `gzip` and `none` for gRPC compression. otelcol exporters default to `gzip`. The realistic production path is gzip-compressed gRPC.

### Root causes identified

1. **`DecompressionAndMetrics` tower layer** (`src/sources/util/grpc/decompression.rs`): creates a `tokio::sync::mpsc::channel(32)` + `StreamBody` + `select! { biased; }` body-forwarding loop for **every** gRPC request — including uncompressed ones. Both `Ok(None)` and `Ok(Some(Gzip))` enter the same heavy path (line 278).

2. **`SourceSender` clone per request** (`src/sources/opentelemetry/grpc.rs:111`): tonic trait methods receive `&self`, forcing `self.pipeline.clone()` which clones a `HashMap<String, Output>` containing `LimitedSender`, `Histogram`, and metrics handles. The HTTP path takes `mut out: SourceSender` — no clone.

3. **`GrpcTraceLayer` allocations** (`src/sources/util/grpc/mod.rs:126-153`): `Box::pin(...)` + `error_span!` + two `to_owned()` string allocations from URI path parsing per request.

4. **HTTP/2 single-connection serialization**: gRPC mandates HTTP/2 — all telemetrygen workers share one TCP connection. HTTP/1.1 gives each worker its own connection.

5. **Dual decompression setup**: tonic's `.accept_compressed(CompressionEncoding::Gzip)` is set on each service (config.rs:154,162,170), but the `DecompressionAndMetrics` layer decompresses the body *before* tonic sees it. Tonic's built-in decompression never fires — it receives pre-decompressed data.

## Functional Requirements

### <a id="fr1"></a>FR1 — Remove DecompressionAndMetrics layer
Remove the `DecompressionAndMetrics` tower layer from the gRPC server stack. Use tonic's built-in `.accept_compressed(CompressionEncoding::Gzip)` for decompression. Emit `BytesReceived` in the gRPC handler — mirroring the HTTP path pattern.

### <a id="fr2"></a>FR2 — Eliminate SourceSender clone in gRPC handler
The OpenTelemetry gRPC `Service::handle_events` must not clone `SourceSender` per request. Use per-output locking (`SharedSourceSender` with per-output `Arc<tokio::sync::Mutex<Output>>`) so `&self` can send without cloning and different signals (logs, traces, metrics) don't contend on the same lock.

### <a id="fr3"></a>FR3 — Reduce GrpcTraceLayer per-request allocations
The tracing span and URI parsing in `GrpcTraceService::call` must avoid heap allocations where possible. Eliminate `to_owned()` calls and reduce or remove `Box::pin`.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Measurable throughput improvement
Uncompressed gRPC logs (1 log/request) throughput must improve by at least 10x from the current ~100/s baseline toward the HTTP baseline (~5,000/s).

### <a id="nfr2"></a>NFR2 — No regression on batched throughput
Batched traces gRPC throughput must not regress below the current ~9,800/s.

### <a id="nfr3"></a>NFR3 — Compressed path correctness
Gzip-compressed gRPC requests must still be decompressed and processed correctly. `BytesReceived` must report decompressed byte count.

### <a id="nfr4"></a>NFR4 — All existing tests pass
No regressions in `cargo test` for opentelemetry source and gRPC utility modules.

## Non-goals

- **HTTP path optimization**: HTTP already performs well (~5,000/s). Not in scope.
- **HTTP/2 server tuning** (window sizes, max concurrent streams): this is a client-side + protocol-level concern. Optimizing the server-side per-request overhead will already improve throughput significantly. Server HTTP/2 tuning can be explored later if the per-request overhead reduction isn't sufficient.
- **zstd/snappy gRPC compression**: OTLP spec only mandates gzip. Sol currently only supports gzip for gRPC (matching tonic). Extended compression can be added separately.
- **LB inter-node forwarding**: the LB sink uses a separate OTLP gRPC *client*. The sink's per-request overhead is a different codepath (sink side, not source side). This work focuses on the gRPC *receiver* (source).

## Rabbit holes

- ~~**Removing `DecompressionAndMetrics` entirely**~~: **Resolved.** The HTTP path already emits `BytesReceived` in the handler (`http.rs:229`) — no tower layer needed. The gRPC handler computes `events.estimated_json_encoded_size_of()` in `handle_events` already. Emit `bytes_received` there, same pattern. See [ADR: decompression-strategy](./adrs/decompression-strategy.md).
- ~~**`SourceSender` interior mutability**~~: **Resolved.** Per-output `Arc<tokio::sync::Mutex<Output>>` via `SharedSourceSender`. No contention across signals. See [ADR: source-sender-mutability](./adrs/source-sender-mutability.md).

## Design

### Approach

```
┌─────────────────────────────────────────────────┐
│  Current gRPC request path (per request)        │
│                                                 │
│  GrpcTraceLayer (Box::pin + span + 2x to_owned) │
│  └─ DecompressionAndMetrics                     │
│     └─ mpsc::channel(32)                        │
│     └─ StreamBody + select! { biased; }         │
│     └─ drive_body_decompression (even if none)  │
│        └─ tonic service (.accept_compressed)    │
│           └─ Service::export(&self)             │
│              └─ self.pipeline.clone()           │
│              └─ send_batch_named                │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  Target gRPC request path (per request)         │
│  (mirrors HTTP pattern)                         │
│                                                 │
│  GrpcTraceLayer (reduced allocations)           │
│  └─ tonic service (.accept_compressed — built-in│
│     decompression, no middleware)                │
│     └─ Service::export(&self)                   │
│        └─ bytes_received.emit(ByteSize(...))    │
│        └─ self.pipeline.send_batch_named(&self)  │
│           (per-output lock, no clone)            │
└─────────────────────────────────────────────────┘
```

### Decisions

- [Decompression strategy](./adrs/decompression-strategy.md)
- [SourceSender mutability](./adrs/source-sender-mutability.md)

## Cross-cutting Concerns

- **Observability**: `BytesReceived` metric must continue to report decompressed body size for both compressed and uncompressed requests.
- **Backward compatibility**: no config changes required. Existing Sol/Vector configs work unchanged.
- **Other gRPC sources**: the `DecompressionAndMetrics` and `GrpcTraceLayer` are shared by `sources::vector` and `sources::opentelemetry`. Changes affect both.

## Post-implementation findings

### Root cause #4 was the dominant bottleneck

Initial benchmark validation showed removing root causes 1-3 had no measurable effect (Sol=103/s ≈ Vector=104/s). Investigation revealed two critical issues:

1. **TCP_NODELAY not set**: tonic's `serve_with_incoming_shutdown` bypasses its built-in `tcp_nodelay` setting. Without it, Nagle's algorithm buffers small gRPC responses (~100 bytes), adding up to 40ms delay per response. Go's gRPC always enables TCP_NODELAY. Fix: `stream.set_nodelay(true)` in `incoming.rs`.

2. **HTTP/2 defaults too conservative**: tonic defaults to 64KB windows and no BDP estimation, while Go gRPC uses BDP estimation that dynamically grows windows to 16MB. Fix: `http2_adaptive_window(true)`, 1MB stream / 2MB connection window sizes, keepalive.

### Final results — NFR1 exceeded

| Scenario | Before | After | Vector | otelcol | Improvement |
|----------|--------|-------|--------|---------|-------------|
| logs gRPC 10k | 103/s | **3,438/s** | 102/s | 3,844/s | **33x** |
| logs gRPC gzip | 257/s | **2,248/s** | 178/s | 1,859/s | **8.7x** (Sol > otelcol) |
| metrics gRPC 10k | 95/s | **3,767/s** | 100/s | 4,153/s | **40x** |
| traces gRPC 10k | 9,991/s | **14,910/s** | 14,578/s | 15,052/s | **1.5x** |
| logs HTTP 10k | 4,840/s | 4,413/s | 4,817/s | 4,579/s | no regression |

Sol now matches otelcol on unbatched gRPC and **beats otelcol on gzip** (tonic's built-in decompression is more efficient).
