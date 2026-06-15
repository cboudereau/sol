# grpc-perf

Benchmark comparison (Sol vs Vector vs otelcontribcol) revealed that Sol/Vector's gRPC receiver path is **~45x slower** than otelcol for unbatched requests and **~50x slower** than its own HTTP path.

## Design
- [20260512_grpc-perf](./designs/20260512_grpc-perf.md)

## ADRs
- [20260512_decompression-strategy](./adrs/20260512_decompression-strategy.md) — Decompression strategy for gRPC requests
- [20260512_source-sender-mutability](./adrs/20260512_source-sender-mutability.md) — SourceSender mutability in gRPC handlers
