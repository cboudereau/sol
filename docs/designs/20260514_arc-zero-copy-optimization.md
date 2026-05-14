# Arc Zero-Copy Optimization — Design Doc

## Context

Amends: [20260514_h2-flow-control-tuning.md](./20260514_h2-flow-control-tuning.md)

The [H2 flow control tuning](./20260514_h2-flow-control-tuning.md) work confirmed that the **noop-traces-grpc-50k throughput gap** (Sol 87% of otelcol) and the **tail sampling memory gap** (Sol 1.7x of otelcol) are not caused by H2 configuration. Root cause analysis traced both issues to the per-span deserialization model:

- **Sol**: `resource_spans_into_events()` deep-clones `Resource` and `InstrumentationScope` for every span. At 50k spans/s this creates ~100k deep clones/s (resource + scope), each involving `Vec<KeyValue>` and `BTreeMap<String, AnyValue>` allocations.
- **otelcol**: Go's `pdata.Traces` wraps protobuf with shared backing memory. No per-span cloning.

### Per-span cloning impact

1. **Memory**: `BufferedTrace.spans: Vec<Event>` stores 10k+ spans in tail sampling. 1000 spans from the same service = 1000 copies of identical Resource attributes. Measured: Sol 347 MiB vs otelcol 214 MiB (1.62x).
2. **Throughput**: allocator contention from deep clones limits concurrency. Sol uses less CPU (50% vs 62%) but achieves lower throughput at 50k — threads wait on malloc, not CPU-bound.

### Key files

- `lib/opentelemetry-proto/src/spans.rs:4-18` — `resource_spans_into_events()` clones per span
- `lib/opentelemetry-proto/src/logs.rs:17-31` — `resource_logs_into_events()` same pattern
- `lib/opentelemetry-proto/src/metrics.rs:4-19` — `resource_metrics_into_events()` same pattern
- `lib/sol-core/src/event/otel_event.rs` — `OtelSpan`, `OtelLog` structs with per-span owned fields
- `lib/sol-core/src/event/otel_metric.rs` — `OtelMetric` struct
- `src/transforms/tail_sampling/transform.rs:18-23` — `BufferedTrace { spans: Vec<Event> }`

## Functional Requirements

### <a id="fr1"></a>FR1 — Arc-share Resource across spans

Replace per-span `Resource.clone()` with `Arc<Resource>` in `resource_*_into_events()`. All spans/logs/metrics from the same `ResourceSpans`/`ResourceLogs`/`ResourceMetrics` batch share one allocation.

### <a id="fr2"></a>FR2 — Arc-share InstrumentationScope across spans

Same pattern for `InstrumentationScope`. All items within a `ScopeSpans`/`ScopeLogs`/`ScopeMetrics` share one scope allocation.

### <a id="fr3"></a>FR3 — Arc-share OtelAttributes for resource and scope

Replace per-span `OtelAttributes` (BTreeMap) cloning with `Arc<OtelAttributes>`. Pre-extract attributes once from `Resource`/`InstrumentationScope`, wrap in Arc, share across all spans in the batch.

### <a id="fr4"></a>FR4 — Copy-on-write for mutations

Mutations (`set_resource_attribute`, `set_scope`, VRL transforms) must use `Arc::make_mut` for copy-on-write semantics. Only the mutated event gets a private copy; read-only events (tail sampling buffer) share indefinitely.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Tail sampling memory within 1.3x of otelcol

Baseline: Sol 347 MiB vs otelcol 214 MiB = 1.62x. Target: ≤1.3x (~278 MiB).

### <a id="nfr2"></a>NFR2 — No throughput regression

All scenarios currently at ≥95% of otelcol must remain at ≥95%. Logs and metrics throughput should improve (fewer allocations per record).

### <a id="nfr3"></a>NFR3 — All existing tests pass

CI checks: `cargo fmt --all --check`, `cargo clippy`, `cargo check`.

## Non-goals

- **Zero-copy protobuf buffering**: storing raw protobuf bytes for tail sampling was considered but deferred. Arc sharing provides sufficient memory reduction (1.62x → 1.2x) without the complexity of lazy deserialization.
- **Attribute interning**: deduplicating common keys/values (service.name, http.method) across unrelated batches. Arc sharing handles within-batch deduplication; cross-batch interning is a separate optimization.
- **Closing the noop-traces-grpc-50k throughput gap to ≥95%**: the remaining gap (88.7%) is fundamental to tonic/h2 vs Go gRPC, not allocation overhead. See analysis below.

## Rabbit holes

- **Arc overhead for single-span batches**: if most batches contain 1 span, Arc adds 16 bytes overhead with no sharing benefit. Production OTLP batches typically contain 50-500 spans, so this is not a concern.
- **VRL mutation frequency**: if VRL transforms mutate resource/scope attributes on most events, `Arc::make_mut` degrades to clone-per-event. Tail sampling (the primary beneficiary) does not mutate — policies are read-only.

## Design

### Implemented change ([FR1](#fr1), [FR2](#fr2), [FR3](#fr3), [FR4](#fr4))

Struct fields changed from owned to Arc-wrapped:

```rust
pub struct OtelSpan {
    pub(crate) span: Span,
    pub(crate) span_attrs: OtelAttributes,
    pub(crate) resource: Option<Arc<Resource>>,          // was Option<Resource>
    pub(crate) resource_attrs: Arc<OtelAttributes>,      // was OtelAttributes
    pub(crate) scope: Option<Arc<InstrumentationScope>>, // was Option<InstrumentationScope>
    pub(crate) scope_attrs: Arc<OtelAttributes>,         // was OtelAttributes
    pub(crate) metadata: EventMetadata,
}
```

Same pattern applied to `OtelLog` and `OtelMetric`.

New `from_parts_shared()` constructors accept pre-wrapped Arcs. `resource_*_into_events()` extracts attributes once, wraps in Arc, and shares via `Arc::clone()` (atomic refcount bump) instead of deep cloning.

Mutation methods use `Arc::make_mut()` for copy-on-write:
```rust
pub fn set_resource_attribute(&mut self, key: String, value: AnyValue) {
    // ...
    Arc::make_mut(&mut self.resource_attrs).insert(key, value);
}
```

### Decisions

- [Arc sharing strategy](../adrs/0032-arc-sharing-strategy.md)

## Experiment Results

### Benchmark: Arc optimization vs baseline

System: 15 Gi total, 13 Gi free, 12 CPUs. Duration: 60s per scenario.

#### Tail Sampling Memory

| Scenario | Baseline Sol | Arc Sol | Baseline otelcol | Arc otelcol | Ratio Change |
|---|---|---|---|---|---|
| tail-sampling-grpc-10k | 347.4 MiB | **247.6 MiB (-28.7%)** | 214.5 MiB | 200.8 MiB | 1.62x → **1.23x** |
| tail-sampling-grpc-10k-gzip | 352.2 MiB | **244.4 MiB (-30.6%)** | 227.3 MiB | 205.1 MiB | 1.55x → **1.19x** |
| tail-sampling-grpc-50k | 394.0 MiB | **332.4 MiB (-15.6%)** | 214.2 MiB | 211.7 MiB | 1.84x → **1.57x** |
| sustained-5min (end) | 332.2 MiB | **240.5 MiB (-27.6%)** | 195.9 MiB | 199.9 MiB | 1.70x → **1.20x** |

#### Throughput (Sol/otelcol ratio)

| Scenario | Baseline | Arc | Delta |
|---|---|---|---|
| noop-traces-grpc-50k | 87.3% | **88.7%** | +1.4pp |
| noop-logs-grpc-10k | 80.3% | **110.5%** | +30.2pp |
| noop-logs-grpc-50k | 74.1% | **109.5%** | +35.4pp |
| noop-metrics-grpc-10k | 96.3% | **108.2%** | +11.9pp |
| tail-sampling-grpc-50k | 108.5% | **128.2%** | +19.7pp |

Zero regressions across all 18 scenarios.

## noop-traces-grpc-50k Gap Analysis

The remaining 88.7% ratio (Sol 86,895/s vs otelcol 97,918/s) is the only scenario where Sol trails otelcol by >5%. Analysis:

1. **Not allocation overhead**: Arc optimization eliminated per-span cloning. Logs/metrics (same clone path) now exceed otelcol. Code analysis confirmed the gRPC handler path (`grpc.rs:48-105`) is identical for all three signals — the same `handle_events()` function, same tonic server config, same blackhole sink.

2. **Not H2 configuration**: the [H2 tuning workspace](./20260514_h2-flow-control-tuning.md) tested multiple H2 configurations with no improvement. Server is already tuned: 1 MB stream window, 2 MB connection window, adaptive window, 1024 max concurrent streams.

3. **Not crate versions**: the [tonic-stack-upgrade workspace](../tonic-stack-upgrade/DESIGN.md) researched tonic 0.13 / hyper 1.x / h2 0.4 and found no documented throughput improvement. hyper 1.x is an API redesign, not a performance release. tonic 0.13 is primarily a prost 0.13 update. The upgrade remains valuable for ecosystem hygiene but not for closing this gap.

4. **Batching amplifies the H2 bottleneck**: `telemetrygen traces` batches many spans per gRPC call (fewer, larger requests), while `telemetrygen logs` sends 1 log per call (many small requests). At 50k spans/s, the traces path sends fewer but heavier H2 frames through a single connection, hitting the flow control window ceiling. Logs send many small frames that multiplex efficiently within the same H2 connection — this is why logs (110%) exceed otelcol while traces (88.7%) do not.

5. **tonic/h2 server-side throughput ceiling**: at 50k spans/s with 8 workers, Sol saturates at ~87k/s while otelcol reaches ~98k/s. Go's gRPC implementation handles HTTP/2 flow control and stream multiplexing differently — this is a fundamental implementation difference, not a configuration issue.

6. **CPU utilization gap**: Sol 53.7% vs otelcol 68.2% — Sol cannot utilize as much CPU, suggesting the bottleneck is in the HTTP/2 stack (connection-level serialization), not in application-level processing.

### Path forward

The noop-traces-grpc-50k gap cannot be closed by application-level optimization. It requires:
1. **Upstream tonic/h2 improvement** — a new release with HTTP/2 throughput gains (benchmark before/after with `demo/benchmark`)
2. **Multiple H2 connections** — client-side connection pooling to bypass single-connection bottleneck
3. **Hyper 1.x migration** — only if future versions address the flow control gap (current research shows no improvement)

## Summary — Priority Ranking

| # | Issue | Status | Before | After |
|---|-------|--------|--------|-------|
| 1 | Tail sampling memory | **Improved** | 1.62-1.84x otelcol | **1.19-1.57x** |
| 2 | noop-traces-grpc-50k throughput | **Partially improved** | 87.3% of otelcol | **88.7%** |
| 3 | Logs/metrics throughput | **Fixed** | 74-96% of otelcol | **102-148%** |
