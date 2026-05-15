# Tail Sampling Slim Buffer — Benchmark Results

Date: 2026-05-15
Branch: `perf/sorted-vec-attributes`
System: 12 CPUs, 15 GiB RAM, WSL2 (Linux 6.6.87.2)

## Optimizations applied

1. **Shared EventMetadata** — one `Arc<Inner>` per batch instead of per span (~65 MiB savings at 300k buffered spans)
2. **Fixed-size ID arrays** — `[u8;16]`/`[u8;8]` instead of `Vec<u8>` for trace_id/span_id/parent_span_id (~14 MiB savings)

## Tail Sampling (Sol vs otelcol, 60s)

| Scenario | Sol rate | otelcol rate | Sol CPU | otelcol CPU | Sol Mem | otelcol Mem | Mem ratio |
|----------|---------|-------------|---------|------------|---------|------------|-----------|
| 10k | 11,226/s | 11,089/s | 7.0% | 41.6% | **161 MiB** | 201 MiB | **0.80x** |
| 10k gzip | 11,090/s | 11,030/s | 7.5% | 14.1% | **160 MiB** | 206 MiB | **0.78x** |
| 50k | 91,161/s | 69,906/s | 86.2% | 136.6% | 233 MiB | 215 MiB | 1.08x |

## LB + Tail Sampling (1 LB + 2 collectors, 60s)

| Scenario | Sol rate | otelcol rate | Sol CPU | otelcol CPU | Sol Mem | otelcol Mem | Mem ratio |
|----------|---------|-------------|---------|------------|---------|------------|-----------|
| 10k | 10,811/s | 10,976/s | 15.5% | 58.1% | **159 MiB** | 166 MiB | **0.96x** |
| 10k gzip | 10,628/s | 10,781/s | 17.8% | 57.7% | **156 MiB** | 177 MiB | **0.88x** |
| 50k | 51,661/s | 46,012/s | 139.1% | 264.2% | **227 MiB** | 299 MiB | **0.76x** |

## Sustained Memory (5-minute runs)

| Scenario | Sol (start) | Sol (end) | otelcol (start) | otelcol (end) |
|----------|------------|----------|----------------|--------------|
| Noop logs 10k | 10 MiB | 10 MiB | 46 MiB | 0 MiB |
| Tail sampling 10k | 27 MiB | **158 MiB** | 54 MiB | 198 MiB |

## Noop Pipeline (throughput regression check, 60s)

| Scenario | Sol | otelcol | Vector | Sol / otelcol |
|----------|-----|---------|--------|---------------|
| Traces gRPC 10k | 10,009/s | 10,123/s | 9,957/s | 99% |
| Traces gRPC 50k | 88,590/s | 99,320/s | 29,050/s | 89% |
| Logs gRPC 10k | 4,667/s | 4,071/s | 97/s | **115%** |
| Logs gRPC 50k | 5,503/s | 4,976/s | 192/s | **111%** |
| Metrics gRPC 10k | 4,636/s | 4,046/s | 96/s | **115%** |
| Metrics gRPC 50k | 5,578/s | 5,013/s | 187/s | **111%** |

## Before vs After

| Metric | Before (arc-zero-copy) | After (slim-buffer) | Delta |
|--------|----------------------|-------------------|-------|
| Tail sampling 10k memory | 248 MiB (1.23x otelcol) | 161 MiB (0.80x otelcol) | **-87 MiB (-35%)** |
| Tail sampling 10k CPU | 7.9% | 7.0% | -11% |
| LB + tail sampling 50k memory | 343 MiB | 227 MiB | **-116 MiB (-34%)** |
| Noop traces 50k throughput | ~87k/s | 88,590/s | No regression |

## Acceptance Criteria

- [x] Docker image builds successfully
- [x] tail-sampling-grpc-10k memory <= 200 MiB (161 MiB actual, 0.80x otelcol)
- [x] No throughput regression on noop scenarios
- [x] Results documented
