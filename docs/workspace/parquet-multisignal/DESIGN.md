# Parquet Multisignal — Design Doc

Amends: [parquet/DESIGN.md](../parquet/DESIGN.md)

## Context

The [parquet codec](../parquet/DESIGN.md) currently encodes only OTLP logs. Sol's `Event` enum has three variants — `Log(OtelLog)`, `Metric(OtelMetric)`, `Trace(OtelSpan)` — and the Parquet codec must handle all three to be a complete OTLP-to-Parquet pipeline.

### Current implementation problem

The log implementation uses `arrow` + `serde_arrow` as an intermediary:

```
OtelLog → as_map() → ObjectMap → serde_arrow → Arrow RecordBatch → ArrowWriter → Parquet
```

This path has structural problems:
- **serde_arrow round-trip**: serializes to a dynamic ObjectMap, then deserializes into Arrow arrays with type inference — requiring `cast_with_options` corrections after the fact
- **as_map() dependency**: flattens proto fields into a dynamic key-value map, losing type safety. `OtelMetric` has no `as_map()`, making this path impossible for metrics
- **arrow dependency**: the `arrow` crate is a large intermediary that is not needed — the `parquet` crate (v56) provides a native column writer API that writes Parquet files directly from typed data

The `parquet` crate's `SerializedFileWriter` + `SerializedColumnWriter` API writes Parquet files column-by-column from typed slices (`&[i64]`, `&[ByteArray]`, etc.) with explicit definition levels for nullability. This path is:

```
Proto struct fields → Vec<T> per column → SerializedColumnWriter → Parquet bytes
```

This approach works uniformly for all signal types, has no intermediary, and provides compile-time type safety.

### State of the Art

The OpenTelemetry Collector-Contrib **parquetexporter was removed** (v0.88.0, 2023) — it was never more than a stub. There is no official OTLP Parquet schema.

Reference implementations that define the state of the art:

1. **OTel Arrow (OTAP)** — Official OTEL project ([OTEP 0156](https://github.com/open-telemetry/oteps/blob/main/text/0156-columnar-encoding.md)). Uses a normalized star schema with foreign-key joins between tables. Optimized for wire transport (streaming Arrow IPC), not at-rest storage. Complex write path (multiple tables per signal).

2. **otlp2parquet** (Rust, [github.com/smithclay/otlp2parquet](https://github.com/smithclay/otlp2parquet)) — Flat/denormalized schema. One row per log/span/data-point. Attributes as JSON strings. Separate Parquet files per metric subtype. Resource/scope fields inlined per row. Queryable by DuckDB/Athena out of the box.

3. **Grafana Tempo** (vParquet4/5) — Nested Parquet schema for traces only. Preserves Resource→Scope→Span hierarchy as repeated groups. Promotes `service.name` and high-cardinality attributes to dedicated columns.

4. **ClickHouse Exporter** (collector-contrib) — Flat schema, one table per signal (and per metric subtype). Attributes as `Map(String, String)`. Span events/links as Nested arrays.

### Design consensus across implementations

| Aspect | Consensus |
|--------|-----------|
| **Flat vs nested** | Flat/denormalized is the pragmatic choice for universal queryability |
| **Separate schemas per metric subtype** | All implementations agree — gauge/sum/histogram/exp_histogram/summary get separate schemas |
| **Attributes** | JSON strings (lossless, schema-stable) or Map columns; JSON is the simplest |
| **Span events & links** | JSON strings (otlp2parquet) or nested arrays (Tempo, ClickHouse); JSON keeps schema flat |
| **service.name promotion** | Every implementation promotes it to a top-level column |
| **Duration for traces** | OTAP and otlp2parquet store computed `duration_nanos` instead of raw `end_time_unix_nano` |
| **Exemplars** | JSON string column |

## Functional Requirements

### <a id="fr1"></a>FR1 — Native Parquet column writing (replace Arrow intermediary)

Replace the current log encoding path (`ObjectMap → serde_arrow → RecordBatch → ArrowWriter`) with direct column writing using the `parquet` crate's native API (`SerializedFileWriter` + `SerializedColumnWriter`).

This removes the `arrow` and `serde_arrow` dependencies from the `parquet` feature. The encoding path for ALL signal types becomes:

1. Extract column vectors from proto structs (e.g., `Vec<ByteArray>` for string columns, `Vec<i64>` for timestamps)
2. Compute definition levels (`Vec<i16>`) for nullable columns (0 = null, 1 = present)
3. Write each column using the typed `ColumnWriter` variant (`ByteArrayColumnWriter`, `Int64ColumnWriter`, etc.)
4. Close the row group and file writer to produce a complete Parquet file

### <a id="fr2"></a>FR2 — Log schema and encoding (rewrite)

Rewrite the existing log encoding to use native column writers. Update the schema:
- Promote `service.name` from resource attributes to a top-level `service_name` column
- Add `event_name` column (added to OTLP LogRecord proto)
- Extract fields directly from `OtelLog`'s proto (`record`, `resource`, `scope`) — no `as_map()` needed

### <a id="fr3"></a>FR3 — Trace schema and encoding

Add a Parquet schema for OTLP Spans. One row per span. Denormalized: resource/scope fields inlined. Events and links serialized as JSON strings. Promote `service.name` to top-level. Store computed `duration_nanos` alongside `start_time_unix_nano`.

Extract fields directly from `OtelSpan`'s proto (`span`, `resource`, `scope`) — no `as_map()` needed.

### <a id="fr4"></a>FR4 — Metric schemas and encoding (one per subtype)

Add separate Parquet schemas for each OTLP metric subtype:
- **Gauge** — one row per NumberDataPoint
- **Sum** — one row per NumberDataPoint (adds `aggregation_temporality`, `is_monotonic`)
- **Histogram** — one row per HistogramDataPoint (adds `count`, `sum`, `bucket_counts`, `explicit_bounds`, `min`, `max`)
- **ExponentialHistogram** — one row per ExponentialHistogramDataPoint (adds `scale`, `zero_count`, positive/negative buckets)
- **Summary** — one row per SummaryDataPoint (adds `count`, `sum`, `quantile_values`)

Resource/scope/metric-metadata fields are inlined on every row. `service.name` promoted to top-level.

Extract fields directly from `OtelMetric`'s proto (`metric.data` variants, `resource`, `scope`). Each metric in a batch may contain multiple data points — one row per data point.

### <a id="fr5"></a>FR5 — Signal-type routing in ParquetSerializer

The `ParquetSerializer` must detect the event signal type and route to the correct schema:
- `Event::Log` → logs schema (FR2)
- `Event::Trace` → traces schema (FR3)
- `Event::Metric` → appropriate metric subtype schema (FR4)

A batch may contain mixed signal types. The serializer must group events by signal type (and metric subtype) and produce one Parquet file per group.

### <a id="fr6"></a>FR6 — Shared column extraction helpers

Extract common resource/scope column writing into shared helpers usable across all signal types:
- `write_service_name_column(col_writer, events)` — extracts `service.name` from resource attributes
- `write_resource_columns(row_group, events)` — writes `resource_attributes`, `resource_schema_url`
- `write_scope_columns(row_group, events)` — writes `scope_name`, `scope_version`, `scope_attributes`, `scope_schema_url`
- `write_parquet_file(schema, write_fn, props) → Vec<u8>` — shared file writer lifecycle

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Remove `arrow` and `serde_arrow` from parquet feature

The `parquet` feature must depend only on `dep:parquet` (with compression features: `snap`, `flate2`, `zstd`). The `arrow` and `serde_arrow` crates are not needed for writing Parquet files and must be removed from the `parquet` feature's dependency list.

The `arrow` feature (for ClickHouse ArrowStream codec) remains independent and unchanged.

### <a id="nfr2"></a>NFR2 — Queryable output

DataFusion is the primary query engine target — Rust-native, embeddable, scales via Ballista, and proven for Parquet-stored observability data (InfluxDB 3.0). DuckDB is useful for local debugging only (single-node, does not scale).

Every schema must produce standard Parquet files queryable by DataFusion out of the box. Column names must be snake_case and predictable. Timestamp columns use Parquet logical type `TIMESTAMP(isAdjustedToUTC=true, unit=NANOS)` so DataFusion recognizes them as timestamps. Variable-length arrays (bucket_counts, explicit_bounds, quantile_values) are serialized as JSON strings. Standard Parquet ensures compatibility with other engines (Athena, Spark) without targeting them specifically.

### <a id="nfr3"></a>NFR3 — Consistent across signals

All schemas share the same resource/scope column block. `service_name` is always the same column name and type across logs, traces, and all metric subtypes.

## Non-goals

- **Single unified metric schema**: a union table with all metric subtypes would be extremely sparse (histogram has 6+ fields that gauge doesn't). Separate schemas per subtype is the industry consensus.
- **Nested Parquet for events/links**: Grafana Tempo uses nested groups, but this limits query engine compatibility. JSON strings are universally queryable.
- **Star schema / normalized tables**: OTAP's approach optimizes for wire compression at the cost of query complexity. Flat denormalized schemas are the right choice for at-rest analytics.
- **Attribute promotion beyond service.name**: Tempo promotes HTTP method/status/URL. This is configurable and out of scope for the initial implementation. `service.name` is the only universally agreed-upon promotion.
- **Arrow as intermediary**: the `parquet` crate's native column writer API is sufficient. Using Arrow `RecordBatch` as an intermediary adds complexity and dependencies without benefit for the write path.

## Rabbit holes

1. **Parquet native writer API learning curve**: The `SerializedFileWriter` + `SerializedColumnWriter` API is lower-level than `ArrowWriter`. Each column must be written separately with explicit definition levels. **Constraint**: build a thin helper (`write_parquet_file`) that encapsulates the file/row-group/column lifecycle. Test with logs first before extending to traces and metrics.

2. **Mixed-signal batches**: If a sink sends a `Vec<Event>` with logs and traces mixed, the serializer must handle this. **Constraint**: decide in ADR (reject vs. split).

3. **Timestamp logical types**: Native Parquet timestamps use `INT64` physical type with `TIMESTAMP(NANOS, isAdjustedToUTC=true)` logical type. **Constraint**: verify that DuckDB, Athena, and Spark correctly interpret these as timestamps with a spike test.

4. **FixedLenByteArray for trace_id/span_id**: The native writer requires `FixedLenByteArray` values to be exactly the declared length (16 bytes for trace_id, 8 for span_id). Proto fields are `bytes` which may be empty. **Constraint**: use zero-filled arrays for empty IDs; document this in the schema.

## Design

### Writing architecture (FR1)

```
Proto struct → column extraction → SerializedColumnWriter → Parquet file
```

Each signal type implements a column extraction function that produces typed column vectors:

```rust
fn write_log_columns(row_group: &mut SerializedRowGroupWriter, logs: &[OtelLog]) -> Result<()> {
    // Column 0: service_name (BYTE_ARRAY/UTF8, required)
    write_required_byte_array_column(row_group, logs.iter().map(|l| extract_service_name(l)))?;
    // Column 1: time_unix_nano (INT64/TIMESTAMP_NANOS, optional)
    write_optional_int64_column(row_group, logs.iter().map(|l| nanos_or_none(l.record.time_unix_nano)))?;
    // ... one call per column in schema order
    Ok(())
}
```

The file lifecycle is shared:

```rust
fn write_parquet_file(
    schema: TypePtr,
    props: WriterProperties,
    write_columns: impl FnOnce(&mut SerializedRowGroupWriter) -> Result<()>,
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut buf, schema, Arc::new(props))?;
    let mut row_group = writer.next_row_group()?;
    write_columns(&mut row_group)?;
    row_group.close()?;
    writer.close()?;
    Ok(buf)
}
```

### Log Schema (FR2)

| Column | Parquet Type | Nullable | Source |
|--------|-------------|----------|--------|
| `service_name` | BYTE_ARRAY (UTF8) | false | `resource.attributes["service.name"]` |
| `event_name` | BYTE_ARRAY (UTF8) | true | `log_record.event_name` |
| `time_unix_nano` | INT64 (TIMESTAMP_NANOS, UTC) | true | `log_record.time_unix_nano` |
| `observed_time_unix_nano` | INT64 (TIMESTAMP_NANOS, UTC) | true | `log_record.observed_time_unix_nano` |
| `severity_number` | INT32 | true | `log_record.severity_number` |
| `severity_text` | BYTE_ARRAY (UTF8) | true | `log_record.severity_text` |
| `body` | BYTE_ARRAY (UTF8) | true | JSON-serialized `log_record.body` (AnyValue) |
| `attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized record attributes |
| `flags` | INT32 | true | `log_record.flags` |
| `trace_id` | FIXED_LEN_BYTE_ARRAY(16) | true | `log_record.trace_id` |
| `span_id` | FIXED_LEN_BYTE_ARRAY(8) | true | `log_record.span_id` |
| `dropped_attributes_count` | INT32 | true | `log_record.dropped_attributes_count` |
| `resource_attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized (excluding `service.name`) |
| `resource_schema_url` | BYTE_ARRAY (UTF8) | true | `resource.schema_url` |
| `scope_name` | BYTE_ARRAY (UTF8) | true | `scope.name` |
| `scope_version` | BYTE_ARRAY (UTF8) | true | `scope.version` |
| `scope_attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized scope attributes |
| `scope_schema_url` | BYTE_ARRAY (UTF8) | true | `scope.schema_url` |

### Trace Schema (FR3)

| Column | Parquet Type | Nullable | Source |
|--------|-------------|----------|--------|
| `service_name` | BYTE_ARRAY (UTF8) | false | `resource.attributes["service.name"]` |
| `start_time_unix_nano` | INT64 (TIMESTAMP_NANOS, UTC) | false | `span.start_time_unix_nano` |
| `duration_nanos` | INT64 | false | `span.end_time_unix_nano - span.start_time_unix_nano` |
| `trace_id` | FIXED_LEN_BYTE_ARRAY(16) | false | `span.trace_id` |
| `span_id` | FIXED_LEN_BYTE_ARRAY(8) | false | `span.span_id` |
| `parent_span_id` | FIXED_LEN_BYTE_ARRAY(8) | true | `span.parent_span_id` |
| `trace_state` | BYTE_ARRAY (UTF8) | true | `span.trace_state` |
| `name` | BYTE_ARRAY (UTF8) | false | `span.name` |
| `kind` | INT32 | true | `span.kind` (SpanKind enum) |
| `status_code` | INT32 | true | `span.status.code` |
| `status_message` | BYTE_ARRAY (UTF8) | true | `span.status.message` |
| `attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized span attributes |
| `events` | BYTE_ARRAY (UTF8) | true | JSON-serialized span events array |
| `links` | BYTE_ARRAY (UTF8) | true | JSON-serialized span links array |
| `dropped_attributes_count` | INT32 | true | `span.dropped_attributes_count` |
| `dropped_events_count` | INT32 | true | `span.dropped_events_count` |
| `dropped_links_count` | INT32 | true | `span.dropped_links_count` |
| `flags` | INT32 | true | `span.flags` |
| `resource_attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized (excluding `service.name`) |
| `resource_schema_url` | BYTE_ARRAY (UTF8) | true | `resource.schema_url` |
| `scope_name` | BYTE_ARRAY (UTF8) | true | `scope.name` |
| `scope_version` | BYTE_ARRAY (UTF8) | true | `scope.version` |
| `scope_attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized scope attributes |
| `scope_schema_url` | BYTE_ARRAY (UTF8) | true | `scope.schema_url` |

### Metric Schemas (FR4)

#### Common metric columns (all subtypes)

| Column | Parquet Type | Nullable | Source |
|--------|-------------|----------|--------|
| `service_name` | BYTE_ARRAY (UTF8) | false | `resource.attributes["service.name"]` |
| `name` | BYTE_ARRAY (UTF8) | false | `metric.name` |
| `description` | BYTE_ARRAY (UTF8) | true | `metric.description` |
| `unit` | BYTE_ARRAY (UTF8) | true | `metric.unit` |
| `time_unix_nano` | INT64 (TIMESTAMP_NANOS, UTC) | false | `data_point.time_unix_nano` |
| `start_time_unix_nano` | INT64 (TIMESTAMP_NANOS, UTC) | true | `data_point.start_time_unix_nano` |
| `attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized data point attributes |
| `flags` | INT32 | true | `data_point.flags` |
| `exemplars` | BYTE_ARRAY (UTF8) | true | JSON-serialized exemplars array |
| `resource_attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized (excluding `service.name`) |
| `resource_schema_url` | BYTE_ARRAY (UTF8) | true | `resource.schema_url` |
| `scope_name` | BYTE_ARRAY (UTF8) | true | `scope.name` |
| `scope_version` | BYTE_ARRAY (UTF8) | true | `scope.version` |
| `scope_attributes` | BYTE_ARRAY (UTF8) | true | JSON-serialized scope attributes |
| `scope_schema_url` | BYTE_ARRAY (UTF8) | true | `scope.schema_url` |

#### Gauge-specific columns

| Column | Parquet Type | Nullable | Source |
|--------|-------------|----------|--------|
| `int_value` | INT64 | true | `NumberDataPoint.as_int` |
| `double_value` | DOUBLE | true | `NumberDataPoint.as_double` |

#### Sum-specific columns (= Gauge + 2)

| Column | Parquet Type | Nullable | Source |
|--------|-------------|----------|--------|
| `int_value` | INT64 | true | `NumberDataPoint.as_int` |
| `double_value` | DOUBLE | true | `NumberDataPoint.as_double` |
| `aggregation_temporality` | INT32 | true | `Sum.aggregation_temporality` |
| `is_monotonic` | BOOLEAN | true | `Sum.is_monotonic` |

#### Histogram-specific columns

| Column | Parquet Type | Nullable | Source |
|--------|-------------|----------|--------|
| `count` | INT64 | false | `HistogramDataPoint.count` (unsigned, stored as i64) |
| `sum` | DOUBLE | true | `HistogramDataPoint.sum` |
| `min` | DOUBLE | true | `HistogramDataPoint.min` |
| `max` | DOUBLE | true | `HistogramDataPoint.max` |
| `bucket_counts` | BYTE_ARRAY (UTF8) | true | JSON-serialized `Vec<u64>` |
| `explicit_bounds` | BYTE_ARRAY (UTF8) | true | JSON-serialized `Vec<f64>` |
| `aggregation_temporality` | INT32 | true | `Histogram.aggregation_temporality` |

#### ExponentialHistogram-specific columns

| Column | Parquet Type | Nullable | Source |
|--------|-------------|----------|--------|
| `count` | INT64 | false | `ExponentialHistogramDataPoint.count` (unsigned, stored as i64) |
| `sum` | DOUBLE | true | `ExponentialHistogramDataPoint.sum` |
| `min` | DOUBLE | true | `ExponentialHistogramDataPoint.min` |
| `max` | DOUBLE | true | `ExponentialHistogramDataPoint.max` |
| `scale` | INT32 | false | `ExponentialHistogramDataPoint.scale` |
| `zero_count` | INT64 | false | `ExponentialHistogramDataPoint.zero_count` (unsigned, stored as i64) |
| `zero_threshold` | DOUBLE | true | `ExponentialHistogramDataPoint.zero_threshold` |
| `positive_offset` | INT32 | true | `ExponentialHistogramDataPoint.positive.offset` |
| `positive_bucket_counts` | BYTE_ARRAY (UTF8) | true | JSON-serialized `Vec<u64>` |
| `negative_offset` | INT32 | true | `ExponentialHistogramDataPoint.negative.offset` |
| `negative_bucket_counts` | BYTE_ARRAY (UTF8) | true | JSON-serialized `Vec<u64>` |
| `aggregation_temporality` | INT32 | true | `ExponentialHistogram.aggregation_temporality` |

#### Summary-specific columns

| Column | Parquet Type | Nullable | Source |
|--------|-------------|----------|--------|
| `count` | INT64 | false | `SummaryDataPoint.count` (unsigned, stored as i64) |
| `sum` | DOUBLE | false | `SummaryDataPoint.sum` |
| `quantile_values` | BYTE_ARRAY (UTF8) | true | JSON-serialized `Vec<{quantile, value}>` |

### Signal routing (FR5)

```
ParquetSerializer::encode(Vec<Event>, buffer)
  ├─ partition events by signal type
  ├─ Event::Log  → write_log_columns(schema, logs)  → SerializedFileWriter → buffer
  ├─ Event::Trace → write_trace_columns(schema, spans) → SerializedFileWriter → buffer
  └─ Event::Metric → group by metric subtype
       ├─ Gauge → write_gauge_columns(schema, dps) → SerializedFileWriter → buffer
       ├─ Sum → write_sum_columns(schema, dps) → SerializedFileWriter → buffer
       ├─ Histogram → write_histogram_columns(schema, dps) → SerializedFileWriter → buffer
       ├─ ExponentialHistogram → write_exp_histogram_columns(schema, dps) → SerializedFileWriter → buffer
       └─ Summary → write_summary_columns(schema, dps) → SerializedFileWriter → buffer
```

Each group produces one self-contained Parquet file (header + row group + footer) appended to the output buffer. The sink is responsible for splitting these into separate files if needed (see [mixed-signal batch handling ADR](./adrs/mixed-signal-batch-handling.md)).

### Decisions

- [Parquet writing strategy](./adrs/parquet-writing-strategy.md)
- [Mixed-signal batch handling](./adrs/mixed-signal-batch-handling.md)

## Cross-cutting Concerns

- **Backward compatibility**: the log schema is rewritten (native column writers replace serde_arrow). Column names and types remain identical except for 2 new columns (`service_name`, `event_name`). Downstream queries using `SELECT *` will see new columns. Named column queries are unaffected.
- **Dependency reduction**: the `parquet` feature no longer pulls in `arrow` or `serde_arrow`. The `arrow` feature (for ClickHouse ArrowStream) is unaffected.
- **File naming**: when a batch produces multiple Parquet outputs (mixed signals or mixed metric subtypes), the sink must name files with a signal suffix (e.g., `data_001_logs.parquet`, `data_001_traces.parquet`, `data_001_gauge.parquet`). This is a sink-level concern, not a codec concern.
- **Observability**: reuse existing codec metrics. No new metrics needed.
