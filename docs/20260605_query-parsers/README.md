# query-parsers

Sol's query backend translates LogQL and TraceQL to SQL over Parquet (DataFusion). PromQL already uses a real parser — `promql-parser`, which is itself a grmtools (`lrpar`/`lrlex`) port of Prometheus's goyacc grammar — so its surface is broad and faithful. LogQL and TraceQL, by contrast, are parsed by **ad-hoc string slicing** in `src/querier/loki.rs` and `src/querier/tempo.rs`: find the `{…}`, sp

## Design
- [20260605_query-parsers](./designs/20260605_query-parsers.md)

## ADRs
- [20260605_logql-traceql-parser-strategy](./adrs/20260605_logql-traceql-parser-strategy.md) — Parser strategy: grmtools, porting the upstream goyacc grammar
