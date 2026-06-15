# proto-unification

After the grpc-perf work (TCP_NODELAY, H2 adaptive window, DecompressionAndMetrics removal, SharedSourceSender), Sol achieved a **33x improvement** on unbatched gRPC — but a ~10% gap to otelcol remains:

## Design
- [20260512_proto-unification](./designs/20260512_proto-unification.md)

## ADRs
- [20260512_proto-canonical-source](./adrs/20260512_proto-canonical-source.md) — Proto type canonical source
