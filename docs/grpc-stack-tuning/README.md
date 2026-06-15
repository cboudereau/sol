# grpc-stack-tuning

Previous work eliminated the **per-request overhead** in Sol's gRPC receiver path (33x improvement on unbatched logs, TCP_NODELAY fix, server-side H2 window tuning). The resulting benchmark (results) shows Sol matching or beating otelcol on most scenarios — except **high-throughput batched traces**:

## Design
- [20260513_grpc-stack-tuning](./designs/20260513_grpc-stack-tuning.md)

## ADRs
- [20260513_channel-tuning-strategy](./adrs/20260513_channel-tuning-strategy.md) — Channel tuning strategy: match server defaults vs independent values
