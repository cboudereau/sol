# Tail Sampling Slim Buffer — Benchmark Results

Date: 2026-05-15
Branch: `perf/sorted-vec-attributes`

## Optimizations

1. Shared EventMetadata (one Arc per batch instead of per span)
2. Fixed-size ID arrays ([u8;16]/[u8;8] instead of Vec<u8>)

## Results

### tail-sampling-traces-grpc-10k (60s)

| System | Rate | CPU | Memory |
|--------|------|-----|--------|
| Sol | 10,470/s | 7.7% | 160.1 MiB |
| otelcol | 10,469/s | 41.9% | 195.7 MiB |

Sol/otelcol memory ratio: 0.82x (target was <=1.0x)

### sustained-tail-sampling-traces-grpc-10k (300s)

| System | Mem (start) | Mem (end) |
|--------|-------------|-----------|
| Sol | 215.8 MiB | 198.7 MiB |
| otelcol | 200.7 MiB | 210.7 MiB |

### noop-traces-grpc-50k (60s)

| System | Rate |
|--------|------|
| Sol | 61,850/s |
| otelcol | 42,146/s |

Sol throughput: 1.47x otelcol. No regression.

## Before vs after

| Metric | Before | After | Delta |
|--------|--------|-------|-------|
| tail-sampling memory | 248 MiB (1.23x) | 160 MiB (0.82x) | -88 MiB (-35%) |
| noop-50k throughput | ~60k/s | 61,850/s | No regression |
