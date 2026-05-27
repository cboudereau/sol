# Parquet Codec — Design Doc

> Implemented in `281ee44c6`. Log-only encoding via ArrowWriter + serde_arrow.
> Superseded by [parquet-multisignal](../workspace/parquet-multisignal/DESIGN.md) for multisignal native column writing.

## Context

Sol is an OTLP-native observability pipeline. Observability data (logs, metrics, traces) flows in via gRPC/HTTP OTLP sources and is routed to various sinks. A common data lake pattern is: **OTLP -> Sol -> S3 (or file) as Parquet** for downstream analytics via Athena, Presto, Spark, DuckDB, or ClickHouse.

Today Sol supports Arrow IPC streaming (for ClickHouse) but has no Parquet support. Arrow IPC is a streaming format without a file footer — it is not queryable by analytics engines without conversion. Parquet is the industry-standard columnar file format for data lakes, with built-in compression, predicate pushdown, and schema evolution.

The `parquet` crate from the arrow-rs project (v56.2.0, matching Sol's existing `arrow` dependency) provides native Rust Parquet writing with column-level compression codecs.

## Functional Requirements

### FR1 — Parquet batch serializer

Add a `ParquetSerializer` implementing `tokio_util::codec::Encoder<Vec<Event>>` that converts a batch of `OtelLog`, `OtelMetric`, or `OtelSpan` events into a valid Parquet file (complete with footer).

The serializer must produce a self-contained Parquet file per batch — not a streaming format — because Parquet requires a file footer for the row group metadata.

### FR2 — OTLP-native schema

Define fixed Parquet schemas for each signal type (logs, metrics, traces) derived from the OTLP proto structure. The schema must preserve the full OTLP structure: resource, scope, attributes, and signal-specific fields.

Attributes (key-value pairs with dynamic `AnyValue` types) must be serialized in a way that is both queryable and lossless. The approach must handle the recursive `AnyValue` type.

### FR3 — Parquet-native compression

Expose Parquet's built-in column-level compression codecs in the serializer configuration. Parquet compression is applied per-column inside the file format — it is distinct from and replaces sink-level compression (gzip/zstd wrapping the entire payload).

### FR4 — S3 sink integration

The Parquet serializer must work with the existing S3 sink via the `BatchSerializerConfig` / `EncoderKind::Batch` path. The S3 sink writes one object per batch — each object is a complete Parquet file.

### FR5 — File sink integration

The Parquet serializer must work with the existing file sink. Each rotated file is a complete Parquet file with proper footer.

## Non-Functional Requirements

### NFR1 — Zero new crate families

Use only the `parquet` crate from the existing arrow-rs v56 family. The `arrow` crate is already a dependency — `parquet` shares the same version, release cadence, and Arrow type system. No new crate families introduced.

### NFR2 — Feature-gated

The Parquet codec must be gated behind a `parquet` feature flag (similar to the `arrow` feature for Arrow IPC), so it does not increase binary size or compile time for users who don't need it.

### NFR3 — Consistent with Arrow codec patterns

Follow the same architectural patterns as the existing Arrow IPC codec: `BatchSerializerConfig` variant, feature gating, `SchemaProvider` trait pattern, error handling via `snafu`.

### NFR4 — Queryable output

Parquet files produced must be directly queryable by standard tools (DuckDB, Athena, Spark) without post-processing. Column names must be predictable and documented.

## Non-goals

- **Parquet reading/decoding**: this design covers encoding (sink-side) only.
- **Schema evolution at write time**: the schema is fixed per signal type.
- **Parquet encryption**: column-level encryption is out of scope.
- **Delta Lake / Iceberg integration**: table format metadata is out of scope.

## Design

### Architecture (C4 Level 2)

```mermaid
graph LR
    subgraph Sources
        GRPC[OTLP gRPC Source]
        HTTP[OTLP HTTP Source]
    end

    subgraph Pipeline
        T[Transforms]
    end

    subgraph "Sinks (Parquet path)"
        S3[S3 Sink]
        FILE[File Sink]
    end

    subgraph "Codecs (lib/codecs)"
        PB[ParquetBatchSerializer]
        PC[ParquetCompression]
        PS[ParquetSchema<br/>logs / metrics / traces]
    end

    GRPC --> T
    HTTP --> T
    T --> S3
    T --> FILE
    S3 --> PB
    FILE --> PB
    PB --> PS
    PB --> PC
```

### Codec Integration

The Parquet serializer plugs into the existing batch codec infrastructure:

```
BatchSerializerConfig::Parquet(ParquetSerializerConfig)
  -> ParquetSerializer (implements Encoder<Vec<Event>>)
    -> builds Arrow RecordBatch (reusing existing serde_arrow path)
    -> writes Parquet file via parquet::arrow::ArrowWriter
    -> Parquet-native compression applied per column
```

### Schema Design (Logs)

| Column | Parquet Type | Source |
|--------|-------------|--------|
| `time_unix_nano` | INT64 (TIMESTAMP_NANOS) | `log_record.time_unix_nano` |
| `observed_time_unix_nano` | INT64 (TIMESTAMP_NANOS) | `log_record.observed_time_unix_nano` |
| `severity_number` | INT32 | `log_record.severity_number` |
| `severity_text` | BYTE_ARRAY (UTF8) | `log_record.severity_text` |
| `body` | BYTE_ARRAY (UTF8) | `log_record.body` (JSON-serialized AnyValue) |
| `attributes` | BYTE_ARRAY (UTF8) | JSON-serialized key-value map |
| `flags` | INT32 | `log_record.flags` |
| `trace_id` | FIXED_LEN_BYTE_ARRAY(16) | `log_record.trace_id` |
| `span_id` | FIXED_LEN_BYTE_ARRAY(8) | `log_record.span_id` |
| `dropped_attributes_count` | INT32 | `log_record.dropped_attributes_count` |
| `resource_attributes` | BYTE_ARRAY (UTF8) | JSON-serialized resource attributes |
| `resource_schema_url` | BYTE_ARRAY (UTF8) | `resource.schema_url` |
| `scope_name` | BYTE_ARRAY (UTF8) | `scope.name` |
| `scope_version` | BYTE_ARRAY (UTF8) | `scope.version` |
| `scope_attributes` | BYTE_ARRAY (UTF8) | JSON-serialized scope attributes |
| `scope_schema_url` | BYTE_ARRAY (UTF8) | `scope.schema_url` |

### Decisions

- [ADR 0036: Storage format selection](../adrs/0036-storage-format-selection.md)
- [ADR 0037: Parquet compression codec](../adrs/0037-parquet-compression-codec.md)
- [ADR 0038: Attributes serialization strategy](../adrs/0038-attributes-serialization-strategy.md)
- [ADR 0039: Batch-per-file semantics](../adrs/0039-batch-per-file-semantics.md)

## Cross-cutting Concerns

- **Observability**: reuse existing codec metrics (`component_sent_bytes_total`, `component_sent_events_total`).
- **Migration**: no migration needed — this is a new codec, not a replacement.
- **Rollback**: feature-gated, so rollback = disable feature flag.
