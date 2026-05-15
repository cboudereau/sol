# gRPC & HTTP Stack Tuning — Design Doc

## Context

[Previous work](../designs/20260512_grpc-perf.md) eliminated the **per-request overhead** in Sol's gRPC receiver path (33x improvement on unbatched logs, TCP_NODELAY fix, server-side H2 window tuning). The resulting benchmark ([results](../../demo/benchmark/results/RESULTS.md)) shows Sol matching or beating otelcol on most scenarios — except **high-throughput batched traces**:

| Scenario | Sol | otelcol | Ratio | Gap |
|---|---|---|---|---|
| noop-traces-grpc-10k | 10,077/s | 10,163/s | 99% | negligible (rate-limited) |
| **noop-traces-grpc-50k** | **78,638/s** | **90,549/s** | **87%** | **−13%** |
| tail-sampling-traces-grpc-50k | 89,967/s | 68,622/s | 131% | Sol wins |
| lb-tail-sampling-traces-grpc-50k | 55,689/s | 49,932/s | 112% | Sol wins |
| noop-logs-grpc-10k | 4,437/s | 3,787/s | 117% | Sol wins |

The 50k noop-traces gap is the only scenario where Sol underperforms. Under tail-sampling load (which adds real work), Sol already beats otelcol — suggesting the gap is pure transport overhead, not application logic.

### Root cause analysis

Investigation reveals the **client-side gRPC channel** (OTLP gRPC sink) uses all tonic defaults:

```rust
// src/sinks/opentelemetry/grpc.rs — 3 call sites (lines 122, 462, 502)
let channel = Channel::builder(uri).connect_lazy();
```

Tonic's `Endpoint` defaults:
- **H2 initial stream window**: 64 KB (vs 1 MB set on server)
- **H2 initial connection window**: 64 KB (vs 2 MB set on server)
- **No adaptive window**: BDP estimation disabled
- **No TCP_NODELAY**: Nagle's algorithm enabled on client side
- **No keepalive**: connections go stale without detection
- **No connect timeout**: hangs indefinitely on unreachable endpoints
- **No concurrency limit**: unbounded concurrent requests

The server side was tuned in the previous work, but the client side was left untouched. In the LB pipeline, Sol's OTLP gRPC sink forwards traces to backend collectors — the client-side channel IS the bottleneck.

For the noop-traces-50k scenario, the bottleneck is different: telemetrygen → Sol uses telemetrygen's client (not Sol's), so the gap is likely server-side. The server currently lacks `max_concurrent_streams` and `max_frame_size` settings.

### HTTP client path

The HTTP client (`src/http.rs`) uses `hyper::Client::builder()` with all defaults — no H2 tuning either. However, HTTP scenarios show no performance gap, so HTTP client tuning is lower priority.

## Functional Requirements

### <a id="fr1"></a>FR1 — Tune OTLP gRPC client channel

All three `Channel::builder(uri).connect_lazy()` call sites in `src/sinks/opentelemetry/grpc.rs` must configure:
- H2 initial stream window: 1 MB (match server)
- H2 initial connection window: 2 MB (match server)
- H2 adaptive window (BDP estimation)
- TCP_NODELAY
- Keepalive interval and timeout (match server: 10s / 20s)
- Connect timeout (5s)

### <a id="fr2"></a>FR2 — Add server-side max_concurrent_streams

Set `max_concurrent_streams` on the gRPC server to allow high multiplexing without unbounded resource use. otelcol defaults to unlimited; tonic defaults to OS limit. Set to a high value (e.g., 1024) to avoid artificial bottlenecking.

### <a id="fr3"></a>FR3 — Centralize channel construction

Extract a shared `fn build_otlp_channel(uri: Uri) -> Channel` helper in the gRPC sink module so all three call sites (single-endpoint, LB initial, LB dynamic) use identical tuning. Avoids configuration drift.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Close 50k traces gap

noop-traces-grpc-50k throughput must reach ≥95% of otelcol (currently 87%). Target: ≥86,000 spans/s (vs current 78,638/s).

### <a id="nfr2"></a>NFR2 — No regression on existing scenarios

All scenarios currently at ≥95% of otelcol must remain at ≥95%. Specifically: 10k traces, logs, metrics, tail-sampling, LB.

### <a id="nfr3"></a>NFR3 — All existing tests pass

`cargo test` and `cargo clippy` must pass. No regressions in opentelemetry sink/source tests.

## Non-goals

- **HTTP client tuning**: HTTP scenarios show no gap. Not in scope.
- **Server-side connection limits / max_connection_age**: these are operational settings that depend on deployment topology. Not in scope for a performance-focused change.
- **Compression negotiation**: both sides already support gzip. No change needed.
- **Connection pooling / multiple connections**: tonic's `Channel` multiplexes over a single H2 connection by design. Adding multiple connections requires `Channel::balance_list` or a custom connector — significantly more complex. Not in scope unless NFR1 cannot be met with window tuning alone.
- **Stack dependency upgrade (tonic 0.12 → 0.13+, hyper 0.14 → 1.x)**: valuable but separate scope. The tuning parameters use the same API across versions — optimize first, upgrade later. ~~Recommended as the next step after this work lands.~~ **Update**: [tonic-stack-upgrade research](../workspace/tonic-stack-upgrade/DESIGN.md) found no documented throughput improvement from the upgrade — hyper 1.x is an API redesign, not a performance release.

## Rabbit holes

- **Adaptive window vs static windows**: with `http2_adaptive_window(true)`, the static window sizes become initial values that BDP estimation may grow beyond. Don't spend time benchmarking every combination — set adaptive + reasonable initial values and benchmark once.
- **max_concurrent_streams value**: don't over-optimize the exact number. 1024 is high enough to not bottleneck; the real throughput gain comes from window sizes and BDP.

## Design

### Change map

```
src/sinks/opentelemetry/grpc.rs
  ├── new: fn build_otlp_channel(uri: Uri) -> Channel  [FR1, FR3]
  ├── line 122: replace Channel::builder(endpoint).connect_lazy()
  ├── line 462: replace Channel::builder(uri).connect_lazy()
  └── line 502: replace Channel::builder(uri).connect_lazy()

src/sources/util/grpc/mod.rs
  └── grpc_server_builder(): add .max_concurrent_streams(1024)  [FR2]
```

### Client channel configuration (FR1, FR3)

```rust
fn build_otlp_channel(uri: Uri) -> Channel {
    Channel::builder(uri)
        .http2_adaptive_window(true)
        .initial_stream_window_size(1024 * 1024)         // 1 MB
        .initial_connection_window_size(2 * 1024 * 1024) // 2 MB
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(5))
        .connect_lazy()
}
```

### Server configuration (FR2)

```rust
fn grpc_server_builder() -> Server {
    Server::builder()
        .http2_adaptive_window(Some(true))
        .initial_stream_window_size(1024 * 1024)
        .initial_connection_window_size(2 * 1024 * 1024)
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(20)))
        .max_concurrent_streams(1024)  // NEW
}
```

### Decisions

- [Channel tuning strategy](./adrs/channel-tuning-strategy.md)

## Cross-cutting Concerns

- **Backward compatibility**: no config changes. All tuning is internal defaults.
- **LB forwarding path**: the LB sink's gRPC client channels are the highest-impact target — they carry all forwarded traces between Sol instances.
- **Observability**: no new metrics needed. Existing `component_sent_events_total` and endpoint bytes cover the sink path.

## Post-implementation findings

### NFR1 not met — 50k noop-traces gap is server-side

Client-side tuning and `max_concurrent_streams(1024)` had **no effect** on the noop-traces-grpc-50k scenario (87% → 87%). This confirms the bottleneck is in the **inbound** path (telemetrygen → Sol's tonic server), not the outbound client channel.

The LB pipeline — where Sol's client channels forward to backends — showed healthy results: Sol beats otelcol at 50k (50,833/s vs 47,338/s) with 44% less CPU.

| Scenario | Sol (pr-23) | otelcol | Ratio |
|---|---|---|---|
| noop-traces-grpc-50k | 80,451/s | 92,135/s | **87%** (unchanged) |
| lb-traces-grpc-50k | 50,833/s | 47,338/s | **107%** (Sol wins) |
| lb-traces-grpc-10k | 11,371/s | 11,460/s | **99%** |

### Root cause: tonic/h2 server throughput ceiling

At 50k+ spans/s over a single H2 connection, Go's gRPC server (used by otelcol) outperforms Rust's tonic 0.12 / hyper 0.14 / h2 0.4. ~~This is a known limitation of the older hyper stack. The path forward is upgrading to tonic 0.13+ (hyper 1.x, h2 0.5+), which includes significant HTTP/2 performance improvements.~~ **Update**: [tonic-stack-upgrade research](../workspace/tonic-stack-upgrade/DESIGN.md) found this is not a version-specific limitation — hyper 1.x showed no throughput improvement (and was [1.8x slower](https://github.com/hyperium/hyper/issues/3164) in one proxy benchmark). The gap is fundamental to h2/tonic's HTTP/2 flow control implementation vs Go's gRPC. See [arc-zero-copy gap analysis](./20260514_arc-zero-copy-optimization.md#noop-traces-grpc-50k-gap-analysis) for full investigation.

### NFR2 met — no regressions

All scenarios at or above 95% of otelcol. Sol beats otelcol on logs, metrics, tail-sampling, and LB.
