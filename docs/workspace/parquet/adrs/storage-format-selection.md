---
status: draft
---
# Storage format selection: Parquet vs Avro

Addresses: [FR1](../DESIGN.md#fr1), [FR4](../DESIGN.md#fr4), [FR5](../DESIGN.md#fr5), [NFR4](../DESIGN.md#nfr4)

## Problem

OTLP data must be stored in a columnar or structured format for downstream analytics. The two dominant options in the Arrow ecosystem are Apache Parquet (columnar file format) and Apache Avro (row-based serialization). Which format should Sol use for its observability data lake output?

## Options

| Option | Pros | Cons |
|---|---|---|
| **A. Parquet** | Columnar: 10-100x compression on telemetry, predicate pushdown, column pruning. Native DataFusion/Athena/DuckDB/Spark support. Grafana Tempo already uses Parquet. InfluxDB 3.0 validated this choice. Same `arrow-rs` family (v56). | Not streamable (requires file footer). Larger write overhead per batch. Not suitable as a wire format. |
| **B. Avro** | Row-based: natural for streaming/ETL. Strong schema evolution (field defaults, aliases). Kafka-native (common wire format). Sol already has an `AvroSerializer` for per-event encoding. | Poor for analytical queries (no column pruning, no pushdown). Worse compression for wide, repetitive observability data. Not natively supported by DataFusion's table providers. |
| **C. Both** (Avro for streaming, Parquet for storage) | Best of both worlds. Avro on the wire, Parquet at rest. | Doubles codec complexity. Two schemas to maintain. Avro→Parquet conversion adds latency. Sol's batch-per-file model makes Avro wire format unnecessary (OTLP protobuf is the wire format). |

## Decision

**Option A — Parquet.**

Rationale:
- **Analytical queries are the primary use case.** The data flows: OTLP → Sol → Parquet → SQL (DataFusion/Athena/DuckDB). Every downstream tool in this pipeline has first-class Parquet support.
- **Compression on observability data.** Parquet achieves 10-100x compression on columnar telemetry (many rows, same column types, highly repetitive attributes like service names). Avro's row-level compression achieves 2-5x.
- **Predicate pushdown.** Parquet readers skip row groups that don't match filter predicates (`WHERE severity = 'ERROR'`). Avro requires full-file scan.
- **Column pruning.** `SELECT severity, body FROM logs` reads only 2 columns from Parquet. Avro reads every field.
- **DataFusion native.** DataFusion's `ParquetExec` is optimized and production-proven (used by InfluxDB 3.0). DataFusion has no native Avro table provider.
- **Ecosystem validation.** Grafana Tempo stores traces as Parquet. InfluxDB 3.0 stores all signals as Parquet. Amazon S3 Tables uses Parquet. The industry has converged.
- **No streaming need at storage layer.** Sol already uses OTLP protobuf as its wire format (gRPC/HTTP). The codec output goes to S3 (one PUT per batch) or file (one file per rotation) — both are batch-oriented, not streaming.

Avro remains available as a per-event serializer (`SerializerConfig::Avro`) for sinks that need row-based encoding (e.g., Kafka). It is not suitable as the storage format for the observability data lake.

## Consequences

- The `parquet` crate (arrow-rs v56) is added as a dependency behind the `parquet` feature flag.
- All downstream query capabilities (DataFusion SQL, Grafana protocol APIs) build on Parquet table providers.
- Schema evolution is limited: adding columns is non-breaking, removing/renaming is breaking. Managed via OTLP proto versioning.
- Avro `SerializerConfig::Avro` remains for per-event use cases but is not used for the observability backend storage path.
