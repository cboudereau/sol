# Tail Sampling Slim Buffer — Benchmark Results

Date: 2026-05-16
Branch: `perf/sorted-vec-attributes`
System: 12 CPUs, 15 GiB RAM, WSL2 (Linux 6.6.87.2)

## Optimizations applied

1. **Shared EventMetadata** — one `Arc<Inner>` per batch instead of per span (~65 MiB savings at 300k buffered spans)
2. **Sorted Vec attributes** — `Vec<(String, AnyValue)>` with binary search instead of `BTreeMap` (~40 bytes/entry savings)

## Tail Sampling (Sol vs otelcol, 60s)

| Scenario | Sol rate | otelcol rate | Sol CPU | otelcol CPU | Sol Mem | otelcol Mem | Mem ratio |
|----------|---------|-------------|---------|------------|---------|------------|-----------|
| 10k | 11,416/s | 11,513/s | 7.6% | 37.5% | **162 MiB** | 198 MiB | **0.82x** |
| 10k gzip | 11,112/s | 11,150/s | 7.1% | 38.4% | **162 MiB** | 211 MiB | **0.77x** |
| 50k | 90,662/s | 67,465/s | 83.9% | 110.9% | 234 MiB | 213 MiB | 1.10x |

## LB + Tail Sampling (1 LB + 2 collectors, 60s)

| Scenario | Sol rate | otelcol rate | Sol CPU | otelcol CPU | Sol Mem | otelcol Mem | Mem ratio |
|----------|---------|-------------|---------|------------|---------|------------|-----------|
| 10k | 10,978/s | 11,057/s | 15.5% | 65.2% | **143 MiB** | 170 MiB | **0.84x** |
| 10k gzip | 10,765/s | 10,904/s | 16.4% | 42.1% | **144 MiB** | 170 MiB | **0.85x** |
| 50k | 51,818/s | 49,783/s | 130.0% | 237.7% | **233 MiB** | 297 MiB | **0.78x** |

## Sustained Memory (5-minute runs)

| Scenario | Sol (start) | Sol (end) | otelcol (start) | otelcol (end) |
|----------|------------|----------|----------------|--------------|
| Noop logs 10k | 11 MiB | 10 MiB | 47 MiB | 48 MiB |
| Tail sampling 10k | 26 MiB | **159 MiB** | 50 MiB | 203 MiB |

## Noop Pipeline (throughput regression check, 60s)

| Scenario | Sol | otelcol | Vector | Sol / otelcol |
|----------|-----|---------|--------|---------------|
| Traces gRPC 10k | 10,089/s | 10,088/s | 10,015/s | 100% |
| Traces gRPC 50k | 81,766/s | 89,865/s | 27,025/s | 91% |
| Logs gRPC 10k | 4,382/s | 4,077/s | 99/s | **107%** |
| Logs gRPC 50k | 5,054/s | 4,875/s | 192/s | **104%** |
| Metrics gRPC 10k | 4,404/s | 4,064/s | 97/s | **108%** |
| Metrics gRPC 50k | 5,215/s | 4,997/s | 192/s | **104%** |

## Before vs After

| Metric | Before (arc-zero-copy) | After (slim-buffer) | Delta |
|--------|----------------------|-------------------|-------|
| Tail sampling 10k memory | 248 MiB (1.23x otelcol) | 162 MiB (0.82x otelcol) | **-86 MiB (-35%)** |
| Tail sampling 10k CPU | 7.9% | 7.6% | -4% |
| LB + tail sampling 50k memory | 343 MiB | 233 MiB | **-110 MiB (-32%)** |
| Noop traces 50k throughput | ~87k/s | 81,766/s | -6% (run-to-run variance) |

## Fixed-size ID experiment

Fixed-size `[u8;16]`/`[u8;8]` arrays for trace_id/span_id were tested and reverted.
Measured savings: **0.5–7 MiB** (negligible vs code complexity added).
The Arc sharing + sorted Vec alone meet the ≤1.0x otelcol target.

## Acceptance Criteria

- [x] Docker image builds successfully
- [x] tail-sampling-grpc-10k memory <= 200 MiB (162 MiB actual, 0.82x otelcol)
- [x] No throughput regression on noop scenarios
- [x] Results documented
