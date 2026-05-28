//! Parquet format codec for batched event encoding.
//!
//! Converts batches of OTLP log events into complete Parquet files with
//! column-level compression. Each `encode()` call produces a self-contained
//! Parquet file (header + row groups + footer).
//!
//! Uses the `parquet` crate's native column writer API (`SerializedFileWriter`
//! + `SerializedColumnWriter`) — no arrow dependency required.

use bytes::{BufMut, BytesMut};
use parquet::{
    basic::{Compression, GzipLevel, ZstdLevel},
    column::writer::ColumnWriter,
    data_type::{ByteArray, FixedLenByteArray},
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::types::Type,
};
use snafu::Snafu;
use sol_config::configurable_component;
use sol_core::event::otel_event::OtelSpan;
use sol_core::event::otel_metric::OtelMetric;
use sol_core::event::{Event, OtelLog};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Column writer helpers (Task 1)
// ---------------------------------------------------------------------------

/// Create a complete Parquet file in memory.
///
/// Opens a `SerializedFileWriter` backed by a `Vec<u8>`, adds a single row
/// group via the caller-supplied `write_fn`, and returns the finished bytes.
fn write_parquet_file(
    schema: Arc<Type>,
    props: Arc<WriterProperties>,
    write_fn: impl FnOnce(
        &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    ) -> Result<(), ParquetEncodingError>,
) -> Result<Vec<u8>, ParquetEncodingError> {
    let buf: Vec<u8> = Vec::new();
    let mut writer = SerializedFileWriter::new(buf, schema, props)
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    let mut rg = writer
        .next_row_group()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    write_fn(&mut rg)?;
    rg.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    let inner = writer
        .into_inner()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(inner)
}

/// Write a REQUIRED BYTE_ARRAY column.
fn write_required_bytes_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[ByteArray],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::ByteArrayColumnWriter(w) => {
            w.write_batch(values, None, None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write an OPTIONAL BYTE_ARRAY column.
fn write_optional_bytes_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[ByteArray],
    def_levels: &[i16],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::ByteArrayColumnWriter(w) => {
            w.write_batch(values, Some(def_levels), None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write a REQUIRED INT64 column.
fn write_required_i64_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[i64],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::Int64ColumnWriter(w) => {
            w.write_batch(values, None, None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write an OPTIONAL INT64 column.
fn write_optional_i64_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[i64],
    def_levels: &[i16],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::Int64ColumnWriter(w) => {
            w.write_batch(values, Some(def_levels), None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write an OPTIONAL INT32 column.
fn write_optional_i32_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[i32],
    def_levels: &[i16],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::Int32ColumnWriter(w) => {
            w.write_batch(values, Some(def_levels), None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write an OPTIONAL DOUBLE column.
fn write_optional_double_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[f64],
    def_levels: &[i16],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::DoubleColumnWriter(w) => {
            w.write_batch(values, Some(def_levels), None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write an OPTIONAL BOOLEAN column.
fn write_optional_bool_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[bool],
    def_levels: &[i16],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::BoolColumnWriter(w) => {
            w.write_batch(values, Some(def_levels), None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write a REQUIRED FIXED_LEN_BYTE_ARRAY column.
fn write_required_fixed_bytes_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[FixedLenByteArray],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::FixedLenByteArrayColumnWriter(w) => {
            w.write_batch(values, None, None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write a REQUIRED INT32 column.
fn write_required_i32_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[i32],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::Int32ColumnWriter(w) => {
            w.write_batch(values, None, None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write a REQUIRED DOUBLE column.
fn write_required_double_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[f64],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::DoubleColumnWriter(w) => {
            w.write_batch(values, None, None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

/// Write an OPTIONAL FIXED_LEN_BYTE_ARRAY column.
fn write_optional_fixed_bytes_column(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    values: &[FixedLenByteArray],
    def_levels: &[i16],
) -> Result<(), ParquetEncodingError> {
    let mut col = rg
        .next_column()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?
        .ok_or(ParquetEncodingError::NoColumn)?;
    match col.untyped() {
        ColumnWriter::FixedLenByteArrayColumnWriter(w) => {
            w.write_batch(values, Some(def_levels), None)
                .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        }
        _ => return Err(ParquetEncodingError::ColumnTypeMismatch),
    }
    col.close()
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared extractors (Task 1)
// ---------------------------------------------------------------------------

use sol_core::event::otel_attributes::OtelAttributes;

/// Extract the `service.name` from resource attributes, defaulting to `"unknown"`.
fn extract_service_name(resource_attrs: &OtelAttributes) -> String {
    resource_attrs
        .get_string("service.name")
        .unwrap_or("unknown")
        .to_string()
}

/// Serialize `OtelAttributes` to a JSON string.
fn attrs_to_json(attrs: &OtelAttributes) -> String {
    serde_json::to_string(attrs).unwrap_or_default()
}

/// Serialize `OtelAttributes` to JSON, excluding a specific key.
fn attrs_to_json_excluding(attrs: &OtelAttributes, exclude_key: &str) -> String {
    // Serialize to a JSON Value (map), remove the key, re-serialize.
    match serde_json::to_value(attrs) {
        Ok(serde_json::Value::Object(mut map)) => {
            map.remove(exclude_key);
            serde_json::to_string(&map).unwrap_or_default()
        }
        _ => String::from("{}"),
    }
}

/// Serialize a log body (`AnyValue`) to a JSON string using `body_string()`.
///
/// For string values this returns the raw string; for complex values it returns
/// a debug representation. This matches the existing Parquet encoding behavior
/// where body is stored as a UTF-8 string column.
fn body_to_string(log: &OtelLog) -> Option<String> {
    log.body().map(|_| log.body_string())
}

// ---------------------------------------------------------------------------
// Parquet compression & config (unchanged public API)
// ---------------------------------------------------------------------------

/// Parquet compression codec selection.
#[configurable_component]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ParquetCompression {
    /// Zstd compression (best ratio/speed tradeoff for observability data).
    #[default]
    Zstd,
    /// Snappy compression (legacy Parquet default, fast).
    Snappy,
    /// Gzip compression (universally supported).
    Gzip,
    /// No compression.
    #[serde(rename = "none")]
    Uncompressed,
}

impl ParquetCompression {
    fn to_parquet(&self) -> Compression {
        match self {
            Self::Zstd => Compression::ZSTD(ZstdLevel::default()),
            Self::Snappy => Compression::SNAPPY,
            Self::Gzip => Compression::GZIP(GzipLevel::default()),
            Self::Uncompressed => Compression::UNCOMPRESSED,
        }
    }
}

/// Configuration for the Parquet batch serializer.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct ParquetSerializerConfig {
    /// Parquet compression codec.
    #[serde(default)]
    pub compression: ParquetCompression,
}

// ---------------------------------------------------------------------------
// Log schema (Task 2)
// ---------------------------------------------------------------------------

use parquet::basic::{LogicalType, Repetition, TimeUnit as ParquetTimeUnit};

/// Build the fixed Parquet schema for OTLP LogRecord files (18 columns).
pub fn build_otel_log_schema() -> Arc<Type> {
    use parquet::basic::Type as PhysicalType;

    let fields: Vec<Arc<Type>> = vec![
        // 0: service_name — REQUIRED UTF8
        Arc::new(
            Type::primitive_type_builder("service_name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("service_name field"),
        ),
        // 1: event_name — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("event_name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("event_name field"),
        ),
        // 2: time_unix_nano — OPTIONAL TIMESTAMP NANOS
        Arc::new(
            Type::primitive_type_builder("time_unix_nano", PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: ParquetTimeUnit::NANOS(Default::default()),
                }))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("time_unix_nano field"),
        ),
        // 3: observed_time_unix_nano — OPTIONAL TIMESTAMP NANOS
        Arc::new(
            Type::primitive_type_builder("observed_time_unix_nano", PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: ParquetTimeUnit::NANOS(Default::default()),
                }))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("observed_time_unix_nano field"),
        ),
        // 4: severity_number — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("severity_number", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("severity_number field"),
        ),
        // 5: severity_text — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("severity_text", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("severity_text field"),
        ),
        // 6: body — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("body", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("body field"),
        ),
        // 7: attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("attributes field"),
        ),
        // 8: flags — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("flags", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("flags field"),
        ),
        // 9: trace_id — OPTIONAL FIXED_LEN_BYTE_ARRAY(16)
        Arc::new(
            Type::primitive_type_builder("trace_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(16)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("trace_id field"),
        ),
        // 10: span_id — OPTIONAL FIXED_LEN_BYTE_ARRAY(8)
        Arc::new(
            Type::primitive_type_builder("span_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(8)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("span_id field"),
        ),
        // 11: dropped_attributes_count — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("dropped_attributes_count", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("dropped_attributes_count field"),
        ),
        // 12: resource_attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("resource_attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("resource_attributes field"),
        ),
        // 13: resource_schema_url — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("resource_schema_url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("resource_schema_url field"),
        ),
        // 14: scope_name — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_name field"),
        ),
        // 15: scope_version — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_version", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_version field"),
        ),
        // 16: scope_attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_attributes field"),
        ),
        // 17: scope_schema_url — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_schema_url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_schema_url field"),
        ),
    ];

    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("log schema"),
    )
}

// ---------------------------------------------------------------------------
// Log column encoding (Task 2)
// ---------------------------------------------------------------------------

/// Collect an optional string field into (values, def_levels) for an OPTIONAL
/// BYTE_ARRAY column. Empty strings are treated as null.
fn collect_optional_string<F>(logs: &[&OtelLog], extractor: F) -> (Vec<ByteArray>, Vec<i16>)
where
    F: Fn(&OtelLog) -> Option<String>,
{
    let mut values = Vec::with_capacity(logs.len());
    let mut def_levels = Vec::with_capacity(logs.len());
    for log in logs {
        match extractor(log) {
            Some(s) if !s.is_empty() => {
                values.push(ByteArray::from(s.into_bytes()));
                def_levels.push(1_i16);
            }
            _ => {
                def_levels.push(0_i16);
            }
        }
    }
    (values, def_levels)
}

/// Collect an optional u64 timestamp field — 0 means absent.
fn collect_optional_nanos<F>(logs: &[&OtelLog], extractor: F) -> (Vec<i64>, Vec<i16>)
where
    F: Fn(&OtelLog) -> u64,
{
    let mut values = Vec::with_capacity(logs.len());
    let mut def_levels = Vec::with_capacity(logs.len());
    for log in logs {
        let n = extractor(log);
        if n == 0 {
            def_levels.push(0_i16);
        } else {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
            )]
            values.push(n as i64);
            def_levels.push(1_i16);
        }
    }
    (values, def_levels)
}

/// Collect an optional i32 field — 0 means absent.
fn collect_optional_i32_zero_is_null<F>(logs: &[&OtelLog], extractor: F) -> (Vec<i32>, Vec<i16>)
where
    F: Fn(&OtelLog) -> i32,
{
    let mut values = Vec::with_capacity(logs.len());
    let mut def_levels = Vec::with_capacity(logs.len());
    for log in logs {
        let n = extractor(log);
        if n == 0 {
            def_levels.push(0_i16);
        } else {
            values.push(n);
            def_levels.push(1_i16);
        }
    }
    (values, def_levels)
}

/// Collect an i32 field that is always present (every row has def_level=1).
fn collect_always_present_i32<F>(logs: &[&OtelLog], extractor: F) -> (Vec<i32>, Vec<i16>)
where
    F: Fn(&OtelLog) -> i32,
{
    let mut values = Vec::with_capacity(logs.len());
    let mut def_levels = Vec::with_capacity(logs.len());
    for log in logs {
        values.push(extractor(log));
        def_levels.push(1_i16);
    }
    (values, def_levels)
}

/// Pad or truncate bytes to a fixed length. Returns `None` if input is empty.
fn to_fixed_bytes(bytes: &[u8], len: usize) -> Option<FixedLenByteArray> {
    if bytes.is_empty() {
        return None;
    }
    let mut buf = vec![0u8; len];
    let copy_len = bytes.len().min(len);
    buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
    Some(FixedLenByteArray::from(buf))
}

/// Write all 18 log columns into the row group.
fn write_log_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    logs: &[&OtelLog],
) -> Result<(), ParquetEncodingError> {
    // Column 0: service_name (REQUIRED)
    let service_names: Vec<ByteArray> = logs
        .iter()
        .map(|l| ByteArray::from(extract_service_name(l.resource_attrs()).into_bytes()))
        .collect();
    write_required_bytes_column(rg, &service_names)?;

    // Column 1: event_name (OPTIONAL)
    // LogRecord proto does not have an event_name field in this proto version.
    // Write all nulls.
    {
        let def_levels: Vec<i16> = vec![0_i16; logs.len()];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    // Column 2: time_unix_nano (OPTIONAL, 0 = absent)
    {
        let (values, def_levels) = collect_optional_nanos(logs, OtelLog::time_unix_nano);
        write_optional_i64_column(rg, &values, &def_levels)?;
    }

    // Column 3: observed_time_unix_nano (OPTIONAL, 0 = absent)
    {
        let (values, def_levels) = collect_optional_nanos(logs, OtelLog::observed_time_unix_nano);
        write_optional_i64_column(rg, &values, &def_levels)?;
    }

    // Column 4: severity_number (OPTIONAL, 0 = absent)
    {
        let (values, def_levels) =
            collect_optional_i32_zero_is_null(logs, OtelLog::severity_number);
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 5: severity_text (OPTIONAL, empty = absent)
    {
        let (values, def_levels) = collect_optional_string(logs, |l| {
            let s = l.severity_text();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 6: body (OPTIONAL)
    {
        let (values, def_levels) = collect_optional_string(logs, body_to_string);
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 7: attributes (OPTIONAL)
    {
        let (values, def_levels) = collect_optional_string(logs, |l| {
            let attrs = l.attributes();
            if attrs.is_empty() {
                None
            } else {
                Some(attrs_to_json(attrs))
            }
        });
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 8: flags (OPTIONAL, always present when record exists)
    {
        let (values, def_levels) = collect_always_present_i32(logs, |l| {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "flags is u32 in proto, i32 in parquet column; top bit rarely used"
            )]
            let flags = l.record().flags as i32;
            flags
        });
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 9: trace_id (OPTIONAL FIXED_LEN_BYTE_ARRAY(16))
    {
        let mut values = Vec::with_capacity(logs.len());
        let mut def_levels = Vec::with_capacity(logs.len());
        for log in logs {
            match to_fixed_bytes(log.trace_id(), 16) {
                Some(fb) => {
                    values.push(fb);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_fixed_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 10: span_id (OPTIONAL FIXED_LEN_BYTE_ARRAY(8))
    {
        let mut values = Vec::with_capacity(logs.len());
        let mut def_levels = Vec::with_capacity(logs.len());
        for log in logs {
            match to_fixed_bytes(log.span_id(), 8) {
                Some(fb) => {
                    values.push(fb);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_fixed_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 11: dropped_attributes_count (OPTIONAL, 0 = absent)
    {
        let (values, def_levels) = collect_optional_i32_zero_is_null(logs, |l| {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "dropped count is u32 in proto, i32 in parquet; values are small"
            )]
            let count = l.record().dropped_attributes_count as i32;
            count
        });
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 12: resource_attributes (OPTIONAL, excluding service.name)
    {
        let (values, def_levels) = collect_optional_string(logs, |l| {
            let json = attrs_to_json_excluding(l.resource_attrs(), "service.name");
            if json == "{}" { None } else { Some(json) }
        });
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 13: resource_schema_url (OPTIONAL)
    // OtelLog does not expose resource_schema_url — always null.
    {
        let def_levels: Vec<i16> = vec![0_i16; logs.len()];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    // Column 14: scope_name (OPTIONAL)
    {
        let (values, def_levels) = collect_optional_string(logs, |l| {
            l.scope().map(|s| s.name.clone()).filter(|s| !s.is_empty())
        });
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 15: scope_version (OPTIONAL)
    {
        let (values, def_levels) = collect_optional_string(logs, |l| {
            l.scope()
                .map(|s| s.version.clone())
                .filter(|s| !s.is_empty())
        });
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 16: scope_attributes (OPTIONAL)
    {
        let (values, def_levels) = collect_optional_string(logs, |l| {
            let attrs = l.scope_attrs();
            if attrs.is_empty() {
                None
            } else {
                Some(attrs_to_json(attrs))
            }
        });
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 17: scope_schema_url (OPTIONAL)
    // OtelLog does not expose scope_schema_url — always null.
    {
        let def_levels: Vec<i16> = vec![0_i16; logs.len()];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Trace schema (Task 3)
// ---------------------------------------------------------------------------

/// Build the fixed Parquet schema for OTLP Span files (24 columns).
pub fn build_trace_schema() -> Arc<Type> {
    use parquet::basic::Type as PhysicalType;

    let fields: Vec<Arc<Type>> = vec![
        // 0: service_name — REQUIRED UTF8
        Arc::new(
            Type::primitive_type_builder("service_name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("service_name field"),
        ),
        // 1: start_time_unix_nano — REQUIRED TIMESTAMP NANOS
        Arc::new(
            Type::primitive_type_builder("start_time_unix_nano", PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: ParquetTimeUnit::NANOS(Default::default()),
                }))
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("start_time_unix_nano field"),
        ),
        // 2: duration_nanos — REQUIRED INT64
        Arc::new(
            Type::primitive_type_builder("duration_nanos", PhysicalType::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("duration_nanos field"),
        ),
        // 3: trace_id — REQUIRED FIXED_LEN_BYTE_ARRAY(16)
        Arc::new(
            Type::primitive_type_builder("trace_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(16)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("trace_id field"),
        ),
        // 4: span_id — REQUIRED FIXED_LEN_BYTE_ARRAY(8)
        Arc::new(
            Type::primitive_type_builder("span_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(8)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("span_id field"),
        ),
        // 5: parent_span_id — OPTIONAL FIXED_LEN_BYTE_ARRAY(8)
        Arc::new(
            Type::primitive_type_builder("parent_span_id", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_length(8)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("parent_span_id field"),
        ),
        // 6: trace_state — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("trace_state", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("trace_state field"),
        ),
        // 7: name — REQUIRED UTF8
        Arc::new(
            Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("name field"),
        ),
        // 8: kind — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("kind", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("kind field"),
        ),
        // 9: status_code — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("status_code", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("status_code field"),
        ),
        // 10: status_message — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("status_message", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("status_message field"),
        ),
        // 11: attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("attributes field"),
        ),
        // 12: events — OPTIONAL UTF8 (JSON)
        Arc::new(
            Type::primitive_type_builder("events", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("events field"),
        ),
        // 13: links — OPTIONAL UTF8 (JSON)
        Arc::new(
            Type::primitive_type_builder("links", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("links field"),
        ),
        // 14: dropped_attributes_count — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("dropped_attributes_count", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("dropped_attributes_count field"),
        ),
        // 15: dropped_events_count — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("dropped_events_count", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("dropped_events_count field"),
        ),
        // 16: dropped_links_count — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("dropped_links_count", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("dropped_links_count field"),
        ),
        // 17: flags — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("flags", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("flags field"),
        ),
        // 18: resource_attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("resource_attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("resource_attributes field"),
        ),
        // 19: resource_schema_url — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("resource_schema_url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("resource_schema_url field"),
        ),
        // 20: scope_name — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_name field"),
        ),
        // 21: scope_version — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_version", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_version field"),
        ),
        // 22: scope_attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_attributes field"),
        ),
        // 23: scope_schema_url — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_schema_url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_schema_url field"),
        ),
    ];

    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("trace schema"),
    )
}

// ---------------------------------------------------------------------------
// Trace column encoding (Task 3)
// ---------------------------------------------------------------------------

/// Convert bytes to a REQUIRED fixed-length byte array, zero-filling if empty.
fn to_required_fixed_bytes(bytes: &[u8], len: usize) -> FixedLenByteArray {
    let mut buf = vec![0u8; len];
    if !bytes.is_empty() {
        let copy_len = bytes.len().min(len);
        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
    }
    FixedLenByteArray::from(buf)
}

/// Write all 24 trace columns into the row group.
fn write_trace_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    spans: &[&OtelSpan],
) -> Result<(), ParquetEncodingError> {
    // Column 0: service_name (REQUIRED)
    let service_names: Vec<ByteArray> = spans
        .iter()
        .map(|s| ByteArray::from(extract_service_name(s.resource_attrs()).into_bytes()))
        .collect();
    write_required_bytes_column(rg, &service_names)?;

    // Column 1: start_time_unix_nano (REQUIRED)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
        )]
        let values: Vec<i64> = spans
            .iter()
            .map(|s| s.span().start_time_unix_nano as i64)
            .collect();
        write_required_i64_column(rg, &values)?;
    }

    // Column 2: duration_nanos (REQUIRED, computed: end - start)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
        )]
        let values: Vec<i64> = spans
            .iter()
            .map(|s| {
                (s.span().end_time_unix_nano as i64)
                    .wrapping_sub(s.span().start_time_unix_nano as i64)
            })
            .collect();
        write_required_i64_column(rg, &values)?;
    }

    // Column 3: trace_id (REQUIRED FIXED_LEN_BYTE_ARRAY(16))
    {
        let values: Vec<FixedLenByteArray> = spans
            .iter()
            .map(|s| to_required_fixed_bytes(s.trace_id(), 16))
            .collect();
        write_required_fixed_bytes_column(rg, &values)?;
    }

    // Column 4: span_id (REQUIRED FIXED_LEN_BYTE_ARRAY(8))
    {
        let values: Vec<FixedLenByteArray> = spans
            .iter()
            .map(|s| to_required_fixed_bytes(s.span_id(), 8))
            .collect();
        write_required_fixed_bytes_column(rg, &values)?;
    }

    // Column 5: parent_span_id (OPTIONAL FIXED_LEN_BYTE_ARRAY(8))
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            match to_fixed_bytes(span.span().parent_span_id.as_slice(), 8) {
                Some(fb) => {
                    values.push(fb);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_fixed_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 6: trace_state (OPTIONAL UTF8)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            let ts = &span.span().trace_state;
            if ts.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(ts.clone().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 7: name (REQUIRED)
    {
        let values: Vec<ByteArray> = spans
            .iter()
            .map(|s| ByteArray::from(s.name().to_string().into_bytes()))
            .collect();
        write_required_bytes_column(rg, &values)?;
    }

    // Column 8: kind (OPTIONAL INT32, 0 = null)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            let k = span.span().kind;
            if k == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(k);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 9: status_code (OPTIONAL INT32)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            match span.span().status.as_ref() {
                Some(status) => {
                    values.push(status.code);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 10: status_message (OPTIONAL UTF8)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            match span.span().status.as_ref() {
                Some(status) if !status.message.is_empty() => {
                    values.push(ByteArray::from(status.message.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                _ => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 11: attributes (OPTIONAL UTF8)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            let attrs = span.attributes();
            if attrs.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(attrs_to_json(attrs).into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 12: events (OPTIONAL UTF8, JSON-serialized)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            if span.span().events.is_empty() {
                def_levels.push(0_i16);
            } else {
                let json = serde_json::to_string(&span.span().events).unwrap_or_default();
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 13: links (OPTIONAL UTF8, JSON-serialized)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            if span.span().links.is_empty() {
                def_levels.push(0_i16);
            } else {
                let json = serde_json::to_string(&span.span().links).unwrap_or_default();
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 14: dropped_attributes_count (OPTIONAL INT32, 0 = null)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "dropped count is u32 in proto, i32 in parquet; values are small"
            )]
            let count = span.span().dropped_attributes_count as i32;
            if count == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(count);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 15: dropped_events_count (OPTIONAL INT32, 0 = null)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "dropped count is u32 in proto, i32 in parquet; values are small"
            )]
            let count = span.span().dropped_events_count as i32;
            if count == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(count);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 16: dropped_links_count (OPTIONAL INT32, 0 = null)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "dropped count is u32 in proto, i32 in parquet; values are small"
            )]
            let count = span.span().dropped_links_count as i32;
            if count == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(count);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 17: flags (OPTIONAL INT32, 0 = null)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "flags is u32 in proto, i32 in parquet; top bit rarely used"
            )]
            let flags = span.span().flags as i32;
            if flags == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(flags);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // Column 18: resource_attributes (OPTIONAL UTF8, excluding service.name)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            let json = attrs_to_json_excluding(span.resource_attrs(), "service.name");
            if json == "{}" {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 19: resource_schema_url (OPTIONAL UTF8) — always null for now
    {
        let def_levels: Vec<i16> = vec![0_i16; spans.len()];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    // Column 20: scope_name (OPTIONAL UTF8)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            match span.scope().map(|s| &s.name).filter(|n| !n.is_empty()) {
                Some(name) => {
                    values.push(ByteArray::from(name.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 21: scope_version (OPTIONAL UTF8)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            match span.scope().map(|s| &s.version).filter(|v| !v.is_empty()) {
                Some(version) => {
                    values.push(ByteArray::from(version.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 22: scope_attributes (OPTIONAL UTF8)
    {
        let mut values = Vec::with_capacity(spans.len());
        let mut def_levels = Vec::with_capacity(spans.len());
        for span in spans {
            let attrs = span.scope_attrs();
            if attrs.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(attrs_to_json(attrs).into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // Column 23: scope_schema_url (OPTIONAL UTF8) — always null for now
    {
        let def_levels: Vec<i16> = vec![0_i16; spans.len()];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Metric schemas (Task 4 & 5)
// ---------------------------------------------------------------------------

/// Helper: build the 15 common metric schema columns shared by all metric subtypes.
fn common_metric_schema_fields() -> Vec<Arc<Type>> {
    use parquet::basic::Type as PhysicalType;

    vec![
        // 0: service_name — REQUIRED UTF8
        Arc::new(
            Type::primitive_type_builder("service_name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("service_name field"),
        ),
        // 1: name — REQUIRED UTF8
        Arc::new(
            Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("name field"),
        ),
        // 2: description — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("description", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("description field"),
        ),
        // 3: unit — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("unit", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("unit field"),
        ),
        // 4: time_unix_nano — REQUIRED TIMESTAMP NANOS
        Arc::new(
            Type::primitive_type_builder("time_unix_nano", PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: ParquetTimeUnit::NANOS(Default::default()),
                }))
                .with_repetition(Repetition::REQUIRED)
                .build()
                .expect("time_unix_nano field"),
        ),
        // 5: start_time_unix_nano — OPTIONAL TIMESTAMP NANOS
        Arc::new(
            Type::primitive_type_builder("start_time_unix_nano", PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    is_adjusted_to_u_t_c: true,
                    unit: ParquetTimeUnit::NANOS(Default::default()),
                }))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("start_time_unix_nano field"),
        ),
        // 6: attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("attributes field"),
        ),
        // 7: flags — OPTIONAL INT32
        Arc::new(
            Type::primitive_type_builder("flags", PhysicalType::INT32)
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("flags field"),
        ),
        // 8: exemplars — OPTIONAL UTF8 (JSON)
        Arc::new(
            Type::primitive_type_builder("exemplars", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("exemplars field"),
        ),
        // 9: resource_attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("resource_attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("resource_attributes field"),
        ),
        // 10: resource_schema_url — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("resource_schema_url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("resource_schema_url field"),
        ),
        // 11: scope_name — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_name", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_name field"),
        ),
        // 12: scope_version — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_version", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_version field"),
        ),
        // 13: scope_attributes — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_attributes", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_attributes field"),
        ),
        // 14: scope_schema_url — OPTIONAL UTF8
        Arc::new(
            Type::primitive_type_builder("scope_schema_url", PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .expect("scope_schema_url field"),
        ),
    ]
}

/// Build the Parquet schema for OTLP Gauge metrics (17 columns).
pub fn build_gauge_schema() -> Arc<Type> {
    use parquet::basic::Type as PhysicalType;
    let mut fields = common_metric_schema_fields();
    fields.push(Arc::new(
        Type::primitive_type_builder("int_value", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("int_value field"),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("double_value", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("double_value field"),
    ));
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("gauge schema"),
    )
}

/// Build the Parquet schema for OTLP Sum metrics (19 columns).
pub fn build_sum_schema() -> Arc<Type> {
    use parquet::basic::Type as PhysicalType;
    let mut fields = common_metric_schema_fields();
    fields.push(Arc::new(
        Type::primitive_type_builder("int_value", PhysicalType::INT64)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("int_value field"),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("double_value", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("double_value field"),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("aggregation_temporality", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("aggregation_temporality field"),
    ));
    fields.push(Arc::new(
        Type::primitive_type_builder("is_monotonic", PhysicalType::BOOLEAN)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("is_monotonic field"),
    ));
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("sum schema"),
    )
}

/// Build the Parquet schema for OTLP Histogram metrics (22 columns).
pub fn build_histogram_schema() -> Arc<Type> {
    use parquet::basic::Type as PhysicalType;
    let mut fields = common_metric_schema_fields();
    // 15: count — REQUIRED INT64
    fields.push(Arc::new(
        Type::primitive_type_builder("count", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .expect("count field"),
    ));
    // 16: sum — OPTIONAL DOUBLE
    fields.push(Arc::new(
        Type::primitive_type_builder("sum", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("sum field"),
    ));
    // 17: min — OPTIONAL DOUBLE
    fields.push(Arc::new(
        Type::primitive_type_builder("min", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("min field"),
    ));
    // 18: max — OPTIONAL DOUBLE
    fields.push(Arc::new(
        Type::primitive_type_builder("max", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("max field"),
    ));
    // 19: bucket_counts — OPTIONAL UTF8 (JSON)
    fields.push(Arc::new(
        Type::primitive_type_builder("bucket_counts", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("bucket_counts field"),
    ));
    // 20: explicit_bounds — OPTIONAL UTF8 (JSON)
    fields.push(Arc::new(
        Type::primitive_type_builder("explicit_bounds", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("explicit_bounds field"),
    ));
    // 21: aggregation_temporality — OPTIONAL INT32
    fields.push(Arc::new(
        Type::primitive_type_builder("aggregation_temporality", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("aggregation_temporality field"),
    ));
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("histogram schema"),
    )
}

/// Build the Parquet schema for OTLP ExponentialHistogram metrics (27 columns).
pub fn build_exp_histogram_schema() -> Arc<Type> {
    use parquet::basic::Type as PhysicalType;
    let mut fields = common_metric_schema_fields();
    // 15: count — REQUIRED INT64
    fields.push(Arc::new(
        Type::primitive_type_builder("count", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .expect("count field"),
    ));
    // 16: sum — OPTIONAL DOUBLE
    fields.push(Arc::new(
        Type::primitive_type_builder("sum", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("sum field"),
    ));
    // 17: min — OPTIONAL DOUBLE
    fields.push(Arc::new(
        Type::primitive_type_builder("min", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("min field"),
    ));
    // 18: max — OPTIONAL DOUBLE
    fields.push(Arc::new(
        Type::primitive_type_builder("max", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("max field"),
    ));
    // 19: scale — REQUIRED INT32
    fields.push(Arc::new(
        Type::primitive_type_builder("scale", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .expect("scale field"),
    ));
    // 20: zero_count — REQUIRED INT64
    fields.push(Arc::new(
        Type::primitive_type_builder("zero_count", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .expect("zero_count field"),
    ));
    // 21: zero_threshold — OPTIONAL DOUBLE
    fields.push(Arc::new(
        Type::primitive_type_builder("zero_threshold", PhysicalType::DOUBLE)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("zero_threshold field"),
    ));
    // 22: positive_offset — OPTIONAL INT32
    fields.push(Arc::new(
        Type::primitive_type_builder("positive_offset", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("positive_offset field"),
    ));
    // 23: positive_bucket_counts — OPTIONAL UTF8 (JSON)
    fields.push(Arc::new(
        Type::primitive_type_builder("positive_bucket_counts", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("positive_bucket_counts field"),
    ));
    // 24: negative_offset — OPTIONAL INT32
    fields.push(Arc::new(
        Type::primitive_type_builder("negative_offset", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("negative_offset field"),
    ));
    // 25: negative_bucket_counts — OPTIONAL UTF8 (JSON)
    fields.push(Arc::new(
        Type::primitive_type_builder("negative_bucket_counts", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("negative_bucket_counts field"),
    ));
    // 26: aggregation_temporality — OPTIONAL INT32
    fields.push(Arc::new(
        Type::primitive_type_builder("aggregation_temporality", PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("aggregation_temporality field"),
    ));
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("exp_histogram schema"),
    )
}

/// Build the Parquet schema for OTLP Summary metrics (18 columns).
pub fn build_summary_schema() -> Arc<Type> {
    use parquet::basic::Type as PhysicalType;
    let mut fields = common_metric_schema_fields();
    // 15: count — REQUIRED INT64
    fields.push(Arc::new(
        Type::primitive_type_builder("count", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .expect("count field"),
    ));
    // 16: sum — REQUIRED DOUBLE
    fields.push(Arc::new(
        Type::primitive_type_builder("sum", PhysicalType::DOUBLE)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .expect("sum field"),
    ));
    // 17: quantile_values — OPTIONAL UTF8 (JSON)
    fields.push(Arc::new(
        Type::primitive_type_builder("quantile_values", PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .with_repetition(Repetition::OPTIONAL)
            .build()
            .expect("quantile_values field"),
    ));
    Arc::new(
        Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("summary schema"),
    )
}

// ---------------------------------------------------------------------------
// Metric column encoding (Task 4 & 5)
// ---------------------------------------------------------------------------

use sol_core::event::otel_metric::{MetricData, NumberDataPointValue};

/// Convert OTLP KeyValue attributes to a JSON string, or None if empty.
fn kv_attrs_to_json_opt(
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Option<String> {
    if attrs.is_empty() {
        return None;
    }
    let tmp = OtelAttributes::from_key_values(attrs.to_vec());
    let json = attrs_to_json(&tmp);
    if json == "{}" { None } else { Some(json) }
}

/// A flattened row for gauge/sum data points. One row per data point.
struct NumberDpRow<'a> {
    metric: &'a OtelMetric,
    dp: opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
}

/// Collect flattened rows for gauge metrics, using `metric_proto()` to restore dp attrs.
fn collect_gauge_rows<'a>(metrics: &[&'a OtelMetric]) -> Vec<NumberDpRow<'a>> {
    let mut rows = Vec::new();
    for metric in metrics {
        let proto = metric.metric_proto();
        if let Some(MetricData::Gauge(gauge)) = proto.data {
            for dp in gauge.data_points {
                rows.push(NumberDpRow { metric, dp });
            }
        }
    }
    rows
}

/// Collect flattened rows for sum metrics, using `metric_proto()` to restore dp attrs.
fn collect_sum_rows<'a>(metrics: &[&'a OtelMetric]) -> Vec<NumberDpRow<'a>> {
    let mut rows = Vec::new();
    for metric in metrics {
        let proto = metric.metric_proto();
        if let Some(MetricData::Sum(sum)) = proto.data {
            for dp in sum.data_points {
                rows.push(NumberDpRow { metric, dp });
            }
        }
    }
    rows
}

/// Write the 15 common metric columns for number data point rows.
fn write_common_metric_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    rows: &[NumberDpRow<'_>],
) -> Result<(), ParquetEncodingError> {
    let n = rows.len();

    // 0: service_name (REQUIRED)
    let service_names: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(extract_service_name(r.metric.resource_attrs()).into_bytes()))
        .collect();
    write_required_bytes_column(rg, &service_names)?;

    // 1: name (REQUIRED)
    let names: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(r.metric.name().to_string().into_bytes()))
        .collect();
    write_required_bytes_column(rg, &names)?;

    // 2: description (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let desc = row.metric.description();
            if desc.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(desc.to_string().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 3: unit (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let unit = row.metric.unit();
            if unit.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(unit.to_string().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 4: time_unix_nano (REQUIRED)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
        )]
        let values: Vec<i64> = rows.iter().map(|r| r.dp.time_unix_nano as i64).collect();
        write_required_i64_column(rg, &values)?;
    }

    // 5: start_time_unix_nano (OPTIONAL, 0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            if row.dp.start_time_unix_nano == 0 {
                def_levels.push(0_i16);
            } else {
                #[expect(
                    clippy::cast_possible_wrap,
                    reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
                )]
                values.push(row.dp.start_time_unix_nano as i64);
                def_levels.push(1_i16);
            }
        }
        write_optional_i64_column(rg, &values, &def_levels)?;
    }

    // 6: attributes (OPTIONAL — from data point attributes)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match kv_attrs_to_json_opt(&row.dp.attributes) {
                Some(json) => {
                    values.push(ByteArray::from(json.into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 7: flags (OPTIONAL, 0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "flags is u32 in proto, i32 in parquet; top bit rarely used"
            )]
            let flags = row.dp.flags as i32;
            if flags == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(flags);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // 8: exemplars (OPTIONAL UTF8, JSON)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            if row.dp.exemplars.is_empty() {
                def_levels.push(0_i16);
            } else {
                let json = serde_json::to_string(&row.dp.exemplars).unwrap_or_default();
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 9: resource_attributes (OPTIONAL, excluding service.name)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let json = attrs_to_json_excluding(row.metric.resource_attrs(), "service.name");
            if json == "{}" {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 10: resource_schema_url (OPTIONAL) — always null for now
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    // 11: scope_name (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row
                .metric
                .scope()
                .map(|s| &s.name)
                .filter(|s| !s.is_empty())
            {
                Some(name) => {
                    values.push(ByteArray::from(name.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 12: scope_version (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row
                .metric
                .scope()
                .map(|s| &s.version)
                .filter(|v| !v.is_empty())
            {
                Some(version) => {
                    values.push(ByteArray::from(version.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 13: scope_attributes (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let attrs = row.metric.scope_attrs();
            if attrs.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(attrs_to_json(attrs).into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 14: scope_schema_url (OPTIONAL) — always null for now
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    Ok(())
}

/// Write the int_value and double_value columns for gauge/sum data points.
fn write_number_value_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    rows: &[NumberDpRow<'_>],
) -> Result<(), ParquetEncodingError> {
    let n = rows.len();

    // int_value (OPTIONAL INT64)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row.dp.value {
                Some(NumberDataPointValue::AsInt(v)) => {
                    values.push(v);
                    def_levels.push(1_i16);
                }
                _ => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_i64_column(rg, &values, &def_levels)?;
    }

    // double_value (OPTIONAL DOUBLE)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row.dp.value {
                Some(NumberDataPointValue::AsDouble(v)) => {
                    values.push(v);
                    def_levels.push(1_i16);
                }
                _ => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_double_column(rg, &values, &def_levels)?;
    }

    Ok(())
}

/// Write all 17 gauge columns into the row group.
fn write_gauge_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    metrics: &[&OtelMetric],
) -> Result<(), ParquetEncodingError> {
    let rows = collect_gauge_rows(metrics);
    write_common_metric_columns(rg, &rows)?;
    write_number_value_columns(rg, &rows)?;
    Ok(())
}

/// Write all 19 sum columns into the row group.
fn write_sum_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    metrics: &[&OtelMetric],
) -> Result<(), ParquetEncodingError> {
    let rows = collect_sum_rows(metrics);
    write_common_metric_columns(rg, &rows)?;
    write_number_value_columns(rg, &rows)?;

    let n = rows.len();

    // aggregation_temporality (OPTIONAL INT32)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            if let Some(MetricData::Sum(sum)) = row.metric.metric().data.as_ref() {
                values.push(sum.aggregation_temporality);
                def_levels.push(1_i16);
            } else {
                def_levels.push(0_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // is_monotonic (OPTIONAL BOOLEAN)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            if let Some(MetricData::Sum(sum)) = row.metric.metric().data.as_ref() {
                values.push(sum.is_monotonic);
                def_levels.push(1_i16);
            } else {
                def_levels.push(0_i16);
            }
        }
        write_optional_bool_column(rg, &values, &def_levels)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Histogram / ExpHistogram / Summary column encoding (Task 5)
// ---------------------------------------------------------------------------

/// A flattened row for histogram data points.
struct HistogramDpRow<'a> {
    metric: &'a OtelMetric,
    dp: opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
}

/// Write the 15 common metric columns for histogram data point rows.
fn write_common_metric_columns_histogram(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    rows: &[HistogramDpRow<'_>],
) -> Result<(), ParquetEncodingError> {
    let n = rows.len();

    // 0: service_name (REQUIRED)
    let service_names: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(extract_service_name(r.metric.resource_attrs()).into_bytes()))
        .collect();
    write_required_bytes_column(rg, &service_names)?;

    // 1: name (REQUIRED)
    let names: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(r.metric.name().to_string().into_bytes()))
        .collect();
    write_required_bytes_column(rg, &names)?;

    // 2: description (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let desc = row.metric.description();
            if desc.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(desc.to_string().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 3: unit (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let unit = row.metric.unit();
            if unit.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(unit.to_string().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 4: time_unix_nano (REQUIRED)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
        )]
        let values: Vec<i64> = rows.iter().map(|r| r.dp.time_unix_nano as i64).collect();
        write_required_i64_column(rg, &values)?;
    }

    // 5: start_time_unix_nano (OPTIONAL, 0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            if row.dp.start_time_unix_nano == 0 {
                def_levels.push(0_i16);
            } else {
                #[expect(
                    clippy::cast_possible_wrap,
                    reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
                )]
                values.push(row.dp.start_time_unix_nano as i64);
                def_levels.push(1_i16);
            }
        }
        write_optional_i64_column(rg, &values, &def_levels)?;
    }

    // 6: attributes (OPTIONAL — from data point attributes)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match kv_attrs_to_json_opt(&row.dp.attributes) {
                Some(json) => {
                    values.push(ByteArray::from(json.into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 7: flags (OPTIONAL, 0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "flags is u32 in proto, i32 in parquet; top bit rarely used"
            )]
            let flags = row.dp.flags as i32;
            if flags == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(flags);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // 8: exemplars (OPTIONAL UTF8, JSON)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            if row.dp.exemplars.is_empty() {
                def_levels.push(0_i16);
            } else {
                let json = serde_json::to_string(&row.dp.exemplars).unwrap_or_default();
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 9: resource_attributes (OPTIONAL, excluding service.name)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let json = attrs_to_json_excluding(row.metric.resource_attrs(), "service.name");
            if json == "{}" {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 10: resource_schema_url (OPTIONAL) — always null
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    // 11: scope_name (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row
                .metric
                .scope()
                .map(|s| &s.name)
                .filter(|s| !s.is_empty())
            {
                Some(name) => {
                    values.push(ByteArray::from(name.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 12: scope_version (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row
                .metric
                .scope()
                .map(|s| &s.version)
                .filter(|v| !v.is_empty())
            {
                Some(version) => {
                    values.push(ByteArray::from(version.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 13: scope_attributes (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let attrs = row.metric.scope_attrs();
            if attrs.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(attrs_to_json(attrs).into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 14: scope_schema_url (OPTIONAL) — always null
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    Ok(())
}

/// Write all 22 histogram columns into the row group.
fn write_histogram_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    metrics: &[&OtelMetric],
) -> Result<(), ParquetEncodingError> {
    let mut rows = Vec::new();
    for metric in metrics {
        let proto = metric.metric_proto();
        if let Some(MetricData::Histogram(hist)) = proto.data {
            for dp in hist.data_points {
                rows.push(HistogramDpRow { metric, dp });
            }
        }
    }

    write_common_metric_columns_histogram(rg, &rows)?;

    let n = rows.len();

    // 15: count (REQUIRED INT64)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "u64 count may exceed i64::MAX; acceptable truncation"
        )]
        let values: Vec<i64> = rows.iter().map(|r| r.dp.count as i64).collect();
        write_required_i64_column(rg, &values)?;
    }

    // 16: sum (OPTIONAL DOUBLE)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.sum {
                Some(s) => {
                    values.push(s);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_double_column(rg, &values, &def_levels)?;
    }

    // 17: min (OPTIONAL DOUBLE)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.min {
                Some(m) => {
                    values.push(m);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_double_column(rg, &values, &def_levels)?;
    }

    // 18: max (OPTIONAL DOUBLE)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.max {
                Some(m) => {
                    values.push(m);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_double_column(rg, &values, &def_levels)?;
    }

    // 19: bucket_counts (OPTIONAL UTF8, JSON)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            if row.dp.bucket_counts.is_empty() {
                def_levels.push(0_i16);
            } else {
                let json = serde_json::to_string(&row.dp.bucket_counts).unwrap_or_default();
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 20: explicit_bounds (OPTIONAL UTF8, JSON)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            if row.dp.explicit_bounds.is_empty() {
                def_levels.push(0_i16);
            } else {
                let json = serde_json::to_string(&row.dp.explicit_bounds).unwrap_or_default();
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 21: aggregation_temporality (OPTIONAL INT32)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            if let Some(MetricData::Histogram(hist)) = row.metric.metric().data.as_ref() {
                values.push(hist.aggregation_temporality);
                def_levels.push(1_i16);
            } else {
                def_levels.push(0_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    Ok(())
}

/// A flattened row for exponential histogram data points.
struct ExpHistogramDpRow<'a> {
    metric: &'a OtelMetric,
    dp: opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint,
}

/// Write the 15 common metric columns for exp histogram data point rows.
fn write_common_metric_columns_exp_histogram(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    rows: &[ExpHistogramDpRow<'_>],
) -> Result<(), ParquetEncodingError> {
    let n = rows.len();

    // 0: service_name (REQUIRED)
    let service_names: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(extract_service_name(r.metric.resource_attrs()).into_bytes()))
        .collect();
    write_required_bytes_column(rg, &service_names)?;

    // 1: name (REQUIRED)
    let names: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(r.metric.name().to_string().into_bytes()))
        .collect();
    write_required_bytes_column(rg, &names)?;

    // 2: description (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let desc = row.metric.description();
            if desc.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(desc.to_string().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 3: unit (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let unit = row.metric.unit();
            if unit.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(unit.to_string().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 4: time_unix_nano (REQUIRED)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
        )]
        let values: Vec<i64> = rows.iter().map(|r| r.dp.time_unix_nano as i64).collect();
        write_required_i64_column(rg, &values)?;
    }

    // 5: start_time_unix_nano (OPTIONAL, 0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            if row.dp.start_time_unix_nano == 0 {
                def_levels.push(0_i16);
            } else {
                #[expect(
                    clippy::cast_possible_wrap,
                    reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
                )]
                values.push(row.dp.start_time_unix_nano as i64);
                def_levels.push(1_i16);
            }
        }
        write_optional_i64_column(rg, &values, &def_levels)?;
    }

    // 6: attributes (OPTIONAL — from data point attributes)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match kv_attrs_to_json_opt(&row.dp.attributes) {
                Some(json) => {
                    values.push(ByteArray::from(json.into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 7: flags (OPTIONAL, 0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "flags is u32 in proto, i32 in parquet; top bit rarely used"
            )]
            let flags = row.dp.flags as i32;
            if flags == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(flags);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // 8: exemplars (OPTIONAL UTF8, JSON)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            if row.dp.exemplars.is_empty() {
                def_levels.push(0_i16);
            } else {
                let json = serde_json::to_string(&row.dp.exemplars).unwrap_or_default();
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 9: resource_attributes (OPTIONAL, excluding service.name)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let json = attrs_to_json_excluding(row.metric.resource_attrs(), "service.name");
            if json == "{}" {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 10: resource_schema_url (OPTIONAL) — always null
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    // 11: scope_name (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row
                .metric
                .scope()
                .map(|s| &s.name)
                .filter(|s| !s.is_empty())
            {
                Some(name) => {
                    values.push(ByteArray::from(name.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 12: scope_version (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row
                .metric
                .scope()
                .map(|s| &s.version)
                .filter(|v| !v.is_empty())
            {
                Some(version) => {
                    values.push(ByteArray::from(version.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 13: scope_attributes (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let attrs = row.metric.scope_attrs();
            if attrs.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(attrs_to_json(attrs).into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 14: scope_schema_url (OPTIONAL) — always null
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    Ok(())
}

/// Write all 27 exponential histogram columns into the row group.
fn write_exp_histogram_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    metrics: &[&OtelMetric],
) -> Result<(), ParquetEncodingError> {
    let mut rows = Vec::new();
    for metric in metrics {
        let proto = metric.metric_proto();
        if let Some(MetricData::ExponentialHistogram(exp)) = proto.data {
            for dp in exp.data_points {
                rows.push(ExpHistogramDpRow { metric, dp });
            }
        }
    }

    write_common_metric_columns_exp_histogram(rg, &rows)?;

    let n = rows.len();

    // 15: count (REQUIRED INT64)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "u64 count may exceed i64::MAX; acceptable truncation"
        )]
        let values: Vec<i64> = rows.iter().map(|r| r.dp.count as i64).collect();
        write_required_i64_column(rg, &values)?;
    }

    // 16: sum (OPTIONAL DOUBLE)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.sum {
                Some(s) => {
                    values.push(s);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_double_column(rg, &values, &def_levels)?;
    }

    // 17: min (OPTIONAL DOUBLE)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.min {
                Some(m) => {
                    values.push(m);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_double_column(rg, &values, &def_levels)?;
    }

    // 18: max (OPTIONAL DOUBLE)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.max {
                Some(m) => {
                    values.push(m);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_double_column(rg, &values, &def_levels)?;
    }

    // 19: scale (REQUIRED INT32)
    {
        let values: Vec<i32> = rows.iter().map(|r| r.dp.scale).collect();
        write_required_i32_column(rg, &values)?;
    }

    // 20: zero_count (REQUIRED INT64)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "u64 zero_count may exceed i64::MAX; acceptable truncation"
        )]
        let values: Vec<i64> = rows.iter().map(|r| r.dp.zero_count as i64).collect();
        write_required_i64_column(rg, &values)?;
    }

    // 21: zero_threshold (OPTIONAL DOUBLE, 0.0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            if row.dp.zero_threshold == 0.0 {
                def_levels.push(0_i16);
            } else {
                values.push(row.dp.zero_threshold);
                def_levels.push(1_i16);
            }
        }
        write_optional_double_column(rg, &values, &def_levels)?;
    }

    // 22: positive_offset (OPTIONAL INT32)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.positive.as_ref() {
                Some(b) => {
                    values.push(b.offset);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // 23: positive_bucket_counts (OPTIONAL UTF8, JSON)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.positive.as_ref() {
                Some(b) if !b.bucket_counts.is_empty() => {
                    let json = serde_json::to_string(&b.bucket_counts).unwrap_or_default();
                    values.push(ByteArray::from(json.into_bytes()));
                    def_levels.push(1_i16);
                }
                _ => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 24: negative_offset (OPTIONAL INT32)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.negative.as_ref() {
                Some(b) => {
                    values.push(b.offset);
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // 25: negative_bucket_counts (OPTIONAL UTF8, JSON)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            match row.dp.negative.as_ref() {
                Some(b) if !b.bucket_counts.is_empty() => {
                    let json = serde_json::to_string(&b.bucket_counts).unwrap_or_default();
                    values.push(ByteArray::from(json.into_bytes()));
                    def_levels.push(1_i16);
                }
                _ => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 26: aggregation_temporality (OPTIONAL INT32)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in &rows {
            if let Some(MetricData::ExponentialHistogram(exp)) = row.metric.metric().data.as_ref() {
                values.push(exp.aggregation_temporality);
                def_levels.push(1_i16);
            } else {
                def_levels.push(0_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    Ok(())
}

/// A flattened row for summary data points.
struct SummaryDpRow<'a> {
    metric: &'a OtelMetric,
    dp: opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
}

/// Write the 15 common metric columns for summary data point rows.
fn write_common_metric_columns_summary(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    rows: &[SummaryDpRow<'_>],
) -> Result<(), ParquetEncodingError> {
    let n = rows.len();

    // 0: service_name (REQUIRED)
    let service_names: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(extract_service_name(r.metric.resource_attrs()).into_bytes()))
        .collect();
    write_required_bytes_column(rg, &service_names)?;

    // 1: name (REQUIRED)
    let names: Vec<ByteArray> = rows
        .iter()
        .map(|r| ByteArray::from(r.metric.name().to_string().into_bytes()))
        .collect();
    write_required_bytes_column(rg, &names)?;

    // 2: description (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let desc = row.metric.description();
            if desc.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(desc.to_string().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 3: unit (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let unit = row.metric.unit();
            if unit.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(unit.to_string().into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 4: time_unix_nano (REQUIRED)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
        )]
        let values: Vec<i64> = rows.iter().map(|r| r.dp.time_unix_nano as i64).collect();
        write_required_i64_column(rg, &values)?;
    }

    // 5: start_time_unix_nano (OPTIONAL, 0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            if row.dp.start_time_unix_nano == 0 {
                def_levels.push(0_i16);
            } else {
                #[expect(
                    clippy::cast_possible_wrap,
                    reason = "nanosecond timestamps may exceed i64::MAX in distant future; safe for practical use"
                )]
                values.push(row.dp.start_time_unix_nano as i64);
                def_levels.push(1_i16);
            }
        }
        write_optional_i64_column(rg, &values, &def_levels)?;
    }

    // 6: attributes (OPTIONAL — from data point attributes)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match kv_attrs_to_json_opt(&row.dp.attributes) {
                Some(json) => {
                    values.push(ByteArray::from(json.into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 7: flags (OPTIONAL, 0 = null)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            #[expect(
                clippy::cast_possible_wrap,
                reason = "flags is u32 in proto, i32 in parquet; top bit rarely used"
            )]
            let flags = row.dp.flags as i32;
            if flags == 0 {
                def_levels.push(0_i16);
            } else {
                values.push(flags);
                def_levels.push(1_i16);
            }
        }
        write_optional_i32_column(rg, &values, &def_levels)?;
    }

    // 8: exemplars (OPTIONAL) — Summary has no exemplars, always null
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    // 9: resource_attributes (OPTIONAL, excluding service.name)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let json = attrs_to_json_excluding(row.metric.resource_attrs(), "service.name");
            if json == "{}" {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 10: resource_schema_url (OPTIONAL) — always null
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    // 11: scope_name (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row
                .metric
                .scope()
                .map(|s| &s.name)
                .filter(|s| !s.is_empty())
            {
                Some(name) => {
                    values.push(ByteArray::from(name.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 12: scope_version (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            match row
                .metric
                .scope()
                .map(|s| &s.version)
                .filter(|v| !v.is_empty())
            {
                Some(version) => {
                    values.push(ByteArray::from(version.clone().into_bytes()));
                    def_levels.push(1_i16);
                }
                None => {
                    def_levels.push(0_i16);
                }
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 13: scope_attributes (OPTIONAL)
    {
        let mut values = Vec::with_capacity(n);
        let mut def_levels = Vec::with_capacity(n);
        for row in rows {
            let attrs = row.metric.scope_attrs();
            if attrs.is_empty() {
                def_levels.push(0_i16);
            } else {
                values.push(ByteArray::from(attrs_to_json(attrs).into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    // 14: scope_schema_url (OPTIONAL) — always null
    {
        let def_levels: Vec<i16> = vec![0_i16; n];
        write_optional_bytes_column(rg, &[], &def_levels)?;
    }

    Ok(())
}

/// Write all 18 summary columns into the row group.
fn write_summary_columns(
    rg: &mut SerializedRowGroupWriter<'_, Vec<u8>>,
    metrics: &[&OtelMetric],
) -> Result<(), ParquetEncodingError> {
    let mut rows = Vec::new();
    for metric in metrics {
        let proto = metric.metric_proto();
        if let Some(MetricData::Summary(summary)) = proto.data {
            for dp in summary.data_points {
                rows.push(SummaryDpRow { metric, dp });
            }
        }
    }

    write_common_metric_columns_summary(rg, &rows)?;

    // 15: count (REQUIRED INT64)
    {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "u64 count may exceed i64::MAX; acceptable truncation"
        )]
        let values: Vec<i64> = rows.iter().map(|r| r.dp.count as i64).collect();
        write_required_i64_column(rg, &values)?;
    }

    // 16: sum (REQUIRED DOUBLE)
    {
        let values: Vec<f64> = rows.iter().map(|r| r.dp.sum).collect();
        write_required_double_column(rg, &values)?;
    }

    // 17: quantile_values (OPTIONAL UTF8, JSON)
    {
        let mut values = Vec::with_capacity(rows.len());
        let mut def_levels = Vec::with_capacity(rows.len());
        for row in &rows {
            if row.dp.quantile_values.is_empty() {
                def_levels.push(0_i16);
            } else {
                let json = serde_json::to_string(&row.dp.quantile_values).unwrap_or_default();
                values.push(ByteArray::from(json.into_bytes()));
                def_levels.push(1_i16);
            }
        }
        write_optional_bytes_column(rg, &values, &def_levels)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// ParquetSerializer (Task 2)
// ---------------------------------------------------------------------------

/// Serializes batches of events into complete Parquet files.
///
/// Supports all signal types: logs, traces, and metrics (gauge, sum,
/// histogram, exponential histogram, summary). A single `encode()` call
/// partitions events by signal type and metric subtype, producing one
/// Parquet file per non-empty group.
#[derive(Clone, Debug)]
pub struct ParquetSerializer {
    log_schema: Arc<Type>,
    trace_schema: Arc<Type>,
    gauge_schema: Arc<Type>,
    sum_schema: Arc<Type>,
    histogram_schema: Arc<Type>,
    exp_histogram_schema: Arc<Type>,
    summary_schema: Arc<Type>,
    writer_props: WriterProperties,
}

impl ParquetSerializer {
    /// Create a new Parquet serializer with the given configuration.
    pub fn new(config: &ParquetSerializerConfig) -> Self {
        let writer_props = WriterProperties::builder()
            .set_compression(config.compression.to_parquet())
            .build();
        Self {
            log_schema: build_otel_log_schema(),
            trace_schema: build_trace_schema(),
            gauge_schema: build_gauge_schema(),
            sum_schema: build_sum_schema(),
            histogram_schema: build_histogram_schema(),
            exp_histogram_schema: build_exp_histogram_schema(),
            summary_schema: build_summary_schema(),
            writer_props,
        }
    }
}

/// Errors from Parquet encoding.
#[derive(Debug, Snafu)]
pub enum ParquetEncodingError {
    /// No events to encode.
    #[snafu(display("no events to encode"))]
    NoEvents,
    /// Parquet write failed.
    #[snafu(display("parquet write error: {source}"))]
    ParquetWrite {
        /// The source error.
        source: parquet::errors::ParquetError,
    },
    /// Expected a column but the row group had none remaining.
    #[snafu(display("no column available in row group"))]
    NoColumn,
    /// Column physical type did not match the expected writer type.
    #[snafu(display("column physical type mismatch"))]
    ColumnTypeMismatch,
}

impl From<std::io::Error> for ParquetEncodingError {
    fn from(source: std::io::Error) -> Self {
        Self::ParquetWrite {
            source: parquet::errors::ParquetError::External(Box::new(source)),
        }
    }
}

impl tokio_util::codec::Encoder<Vec<Event>> for ParquetSerializer {
    type Error = ParquetEncodingError;

    fn encode(&mut self, events: Vec<Event>, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        if events.is_empty() {
            return Err(ParquetEncodingError::NoEvents);
        }

        // Partition events by signal type and metric subtype.
        let mut logs: Vec<&OtelLog> = Vec::new();
        let mut traces: Vec<&OtelSpan> = Vec::new();
        let mut gauge_metrics: Vec<&OtelMetric> = Vec::new();
        let mut sum_metrics: Vec<&OtelMetric> = Vec::new();
        let mut histogram_metrics: Vec<&OtelMetric> = Vec::new();
        let mut exp_histogram_metrics: Vec<&OtelMetric> = Vec::new();
        let mut summary_metrics: Vec<&OtelMetric> = Vec::new();

        for event in &events {
            match event {
                Event::Log(log) => logs.push(log),
                Event::Trace(span) => traces.push(span),
                Event::Metric(metric) => match metric.metric().data {
                    Some(MetricData::Gauge(_)) => gauge_metrics.push(metric),
                    Some(MetricData::Sum(_)) => sum_metrics.push(metric),
                    Some(MetricData::Histogram(_)) => histogram_metrics.push(metric),
                    Some(MetricData::ExponentialHistogram(_)) => {
                        exp_histogram_metrics.push(metric);
                    }
                    Some(MetricData::Summary(_)) => summary_metrics.push(metric),
                    None => {} // skip metrics with no data
                },
            }
        }

        let mut wrote_any = false;
        let props = Arc::new(self.writer_props.clone());

        if !logs.is_empty() {
            let buf = write_parquet_file(Arc::clone(&self.log_schema), Arc::clone(&props), |rg| {
                write_log_columns(rg, &logs)
            })?;
            buffer.put_slice(&buf);
            wrote_any = true;
        }

        if !traces.is_empty() {
            let buf =
                write_parquet_file(Arc::clone(&self.trace_schema), Arc::clone(&props), |rg| {
                    write_trace_columns(rg, &traces)
                })?;
            buffer.put_slice(&buf);
            wrote_any = true;
        }

        if !gauge_metrics.is_empty() {
            let buf =
                write_parquet_file(Arc::clone(&self.gauge_schema), Arc::clone(&props), |rg| {
                    write_gauge_columns(rg, &gauge_metrics)
                })?;
            buffer.put_slice(&buf);
            wrote_any = true;
        }

        if !sum_metrics.is_empty() {
            let buf = write_parquet_file(Arc::clone(&self.sum_schema), Arc::clone(&props), |rg| {
                write_sum_columns(rg, &sum_metrics)
            })?;
            buffer.put_slice(&buf);
            wrote_any = true;
        }

        if !histogram_metrics.is_empty() {
            let buf = write_parquet_file(
                Arc::clone(&self.histogram_schema),
                Arc::clone(&props),
                |rg| write_histogram_columns(rg, &histogram_metrics),
            )?;
            buffer.put_slice(&buf);
            wrote_any = true;
        }

        if !exp_histogram_metrics.is_empty() {
            let buf = write_parquet_file(
                Arc::clone(&self.exp_histogram_schema),
                Arc::clone(&props),
                |rg| write_exp_histogram_columns(rg, &exp_histogram_metrics),
            )?;
            buffer.put_slice(&buf);
            wrote_any = true;
        }

        if !summary_metrics.is_empty() {
            let buf =
                write_parquet_file(Arc::clone(&self.summary_schema), Arc::clone(&props), |rg| {
                    write_summary_columns(rg, &summary_metrics)
                })?;
            buffer.put_slice(&buf);
            wrote_any = true;
        }

        if !wrote_any {
            return Err(ParquetEncodingError::NoEvents);
        }

        Ok(())
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::Type as PhysicalType;
    use parquet::column::reader::ColumnReader;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use sol_core::event::otel_event::string_value;
    use tokio_util::codec::Encoder;

    // --- Helpers ---

    fn make_simple_schema(repetition: Repetition) -> Arc<Type> {
        Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("col", PhysicalType::BYTE_ARRAY)
                        .with_logical_type(Some(LogicalType::String))
                        .with_repetition(repetition)
                        .build()
                        .expect("field"),
                )])
                .build()
                .expect("schema"),
        )
    }

    fn default_props() -> Arc<WriterProperties> {
        Arc::new(WriterProperties::builder().build())
    }

    fn encode_events(
        events: Vec<Event>,
        compression: ParquetCompression,
    ) -> Result<bytes::Bytes, ParquetEncodingError> {
        let config = ParquetSerializerConfig { compression };
        let mut serializer = ParquetSerializer::new(&config);
        let mut buffer = BytesMut::new();
        serializer.encode(events, &mut buffer)?;
        Ok(buffer.freeze())
    }

    fn create_log_event(severity: &str, body: &str) -> Event {
        use opentelemetry_proto::tonic::common::v1::{
            InstrumentationScope, any_value::Value as OtelValueKind,
        };
        use opentelemetry_proto::tonic::logs::v1::LogRecord;
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use sol_core::event::otel_event::AnyValue;

        let record = LogRecord {
            severity_text: severity.to_string(),
            severity_number: 17,
            body: Some(AnyValue {
                value: Some(OtelValueKind::StringValue(body.to_string())),
            }),
            flags: 0,
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("my-service")),
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let scope = InstrumentationScope {
            name: "test-scope".to_string(),
            version: "1.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        Event::Log(OtelLog::from_parts(
            record,
            Some(resource),
            Some(scope),
            sol_core::event::EventMetadata::default(),
        ))
    }

    fn reader_from_bytes(data: &[u8]) -> SerializedFileReader<bytes::Bytes> {
        SerializedFileReader::new(bytes::Bytes::copy_from_slice(data))
            .expect("failed to create parquet reader")
    }

    // -----------------------------------------------------------------------
    // Task 1: Column writer helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_parquet_file_produces_valid_output() {
        let schema = make_simple_schema(Repetition::REQUIRED);
        let data = write_parquet_file(Arc::clone(&schema), default_props(), |rg| {
            let values = vec![ByteArray::from("hello")];
            write_required_bytes_column(rg, &values)
        })
        .expect("write failed");

        // Parquet magic bytes
        assert!(data.len() > 4);
        assert_eq!(&data[..4], b"PAR1");
        assert_eq!(&data[data.len() - 4..], b"PAR1");
    }

    #[test]
    fn test_write_required_bytes_column() {
        let schema = make_simple_schema(Repetition::REQUIRED);
        let data = write_parquet_file(Arc::clone(&schema), default_props(), |rg| {
            let values = vec![
                ByteArray::from("alpha"),
                ByteArray::from("beta"),
                ByteArray::from("gamma"),
            ];
            write_required_bytes_column(rg, &values)
        })
        .expect("write failed");

        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3);

        let rg = reader.get_row_group(0).expect("row group");
        let mut col_reader = rg.get_column_reader(0).expect("column reader");
        // Buffers must start empty — read_records appends to Vecs.
        let mut vals: Vec<ByteArray> = Vec::new();
        match &mut col_reader {
            ColumnReader::ByteArrayColumnReader(r) => {
                let (read, _, _) = r.read_records(3, None, None, &mut vals).expect("read");
                assert_eq!(read, 3);
            }
            _ => panic!("wrong column reader type"),
        }
        let strings: Vec<String> = vals
            .iter()
            .map(|ba| String::from_utf8(ba.data().to_vec()).expect("utf8"))
            .collect();
        assert_eq!(strings, vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn test_write_optional_bytes_column_with_nulls() {
        let schema = make_simple_schema(Repetition::OPTIONAL);
        let data = write_parquet_file(Arc::clone(&schema), default_props(), |rg| {
            // Row pattern: present, null, present
            let values = vec![ByteArray::from("first"), ByteArray::from("third")];
            let def_levels = vec![1_i16, 0_i16, 1_i16];
            write_optional_bytes_column(rg, &values, &def_levels)
        })
        .expect("write failed");

        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3);

        let rg = reader.get_row_group(0).expect("row group");
        let mut col_reader = rg.get_column_reader(0).expect("column reader");
        let mut vals: Vec<ByteArray> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::ByteArrayColumnReader(r) => {
                let (read, values_read, _) = r
                    .read_records(3, Some(&mut def), None, &mut vals)
                    .expect("read");
                assert_eq!(read, 3);
                assert_eq!(values_read, 2);
            }
            _ => panic!("wrong column reader type"),
        }
        assert_eq!(def, vec![1, 0, 1]);
        assert_eq!(
            String::from_utf8(vals[0].data().to_vec()).expect("utf8"),
            "first"
        );
        assert_eq!(
            String::from_utf8(vals[1].data().to_vec()).expect("utf8"),
            "third"
        );
    }

    #[test]
    fn test_write_optional_i64_column() {
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("val", PhysicalType::INT64)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .expect("field"),
                )])
                .build()
                .expect("schema"),
        );
        let data = write_parquet_file(Arc::clone(&schema), default_props(), |rg| {
            // present(100), null, present(300)
            let values = vec![100_i64, 300_i64];
            let def_levels = vec![1_i16, 0_i16, 1_i16];
            write_optional_i64_column(rg, &values, &def_levels)
        })
        .expect("write failed");

        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3);

        let rg = reader.get_row_group(0).expect("row group");
        let mut col_reader = rg.get_column_reader(0).expect("column reader");
        let mut vals: Vec<i64> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::Int64ColumnReader(r) => {
                let (read, values_read, _) = r
                    .read_records(3, Some(&mut def), None, &mut vals)
                    .expect("read");
                assert_eq!(read, 3);
                assert_eq!(values_read, 2);
            }
            _ => panic!("wrong column reader type"),
        }
        assert_eq!(def, vec![1, 0, 1]);
        assert_eq!(vals[0], 100);
        assert_eq!(vals[1], 300);
    }

    #[test]
    fn test_write_optional_i32_column() {
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("val", PhysicalType::INT32)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .expect("field"),
                )])
                .build()
                .expect("schema"),
        );
        let data = write_parquet_file(Arc::clone(&schema), default_props(), |rg| {
            let values = vec![42_i32];
            let def_levels = vec![1_i16, 0_i16];
            write_optional_i32_column(rg, &values, &def_levels)
        })
        .expect("write failed");

        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);
    }

    #[test]
    fn test_write_optional_double_column() {
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("val", PhysicalType::DOUBLE)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .expect("field"),
                )])
                .build()
                .expect("schema"),
        );
        let data = write_parquet_file(Arc::clone(&schema), default_props(), |rg| {
            let values = vec![1.5_f64, 2.5_f64];
            let def_levels = vec![1_i16, 0_i16, 1_i16];
            write_optional_double_column(rg, &values, &def_levels)
        })
        .expect("write failed");

        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 3);
    }

    #[test]
    fn test_write_optional_bool_column() {
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("val", PhysicalType::BOOLEAN)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .expect("field"),
                )])
                .build()
                .expect("schema"),
        );
        let data = write_parquet_file(Arc::clone(&schema), default_props(), |rg| {
            let values = vec![true];
            let def_levels = vec![1_i16, 0_i16];
            write_optional_bool_column(rg, &values, &def_levels)
        })
        .expect("write failed");

        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);
    }

    #[test]
    fn test_write_optional_fixed_bytes_column() {
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    Type::primitive_type_builder("val", PhysicalType::FIXED_LEN_BYTE_ARRAY)
                        .with_length(16)
                        .with_repetition(Repetition::OPTIONAL)
                        .build()
                        .expect("field"),
                )])
                .build()
                .expect("schema"),
        );
        let data = write_parquet_file(Arc::clone(&schema), default_props(), |rg| {
            let values = vec![FixedLenByteArray::from(vec![0xAB_u8; 16])];
            let def_levels = vec![1_i16, 0_i16];
            write_optional_fixed_bytes_column(rg, &values, &def_levels)
        })
        .expect("write failed");

        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);

        let rg = reader.get_row_group(0).expect("row group");
        let mut col_reader = rg.get_column_reader(0).expect("column reader");
        let mut vals: Vec<FixedLenByteArray> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::FixedLenByteArrayColumnReader(r) => {
                let (read, values_read, _) = r
                    .read_records(2, Some(&mut def), None, &mut vals)
                    .expect("read");
                assert_eq!(read, 2);
                assert_eq!(values_read, 1);
            }
            _ => panic!("wrong column reader type"),
        }
        assert_eq!(def, vec![1, 0]);
        assert_eq!(vals[0].data(), &[0xAB_u8; 16]);
    }

    // -----------------------------------------------------------------------
    // Task 1: Shared extractor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_service_name_present() {
        let mut attrs = OtelAttributes::new();
        attrs.insert("service.name".to_string(), string_value("my-svc"));
        assert_eq!(extract_service_name(&attrs), "my-svc");
    }

    #[test]
    fn test_extract_service_name_missing() {
        let attrs = OtelAttributes::new();
        assert_eq!(extract_service_name(&attrs), "unknown");
    }

    #[test]
    fn test_attrs_to_json() {
        let mut attrs = OtelAttributes::new();
        attrs.insert("key1".to_string(), string_value("val1"));
        let json = attrs_to_json(&attrs);
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert!(parsed.is_object());
        assert_eq!(parsed["key1"], "val1");
    }

    #[test]
    fn test_attrs_to_json_excluding() {
        let mut attrs = OtelAttributes::new();
        attrs.insert("service.name".to_string(), string_value("my-svc"));
        attrs.insert("other".to_string(), string_value("val"));
        let json = attrs_to_json_excluding(&attrs, "service.name");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert!(parsed.is_object());
        assert!(parsed.get("service.name").is_none());
        assert_eq!(parsed["other"], "val");
    }

    // -----------------------------------------------------------------------
    // Task 2: Log schema tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_log_schema_column_count() {
        let schema = build_otel_log_schema();
        assert_eq!(
            schema.get_fields().len(),
            18,
            "expected 18 columns in log schema"
        );
    }

    #[test]
    fn test_log_schema_column_names() {
        let schema = build_otel_log_schema();
        let names: Vec<&str> = schema.get_fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec![
                "service_name",
                "event_name",
                "time_unix_nano",
                "observed_time_unix_nano",
                "severity_number",
                "severity_text",
                "body",
                "attributes",
                "flags",
                "trace_id",
                "span_id",
                "dropped_attributes_count",
                "resource_attributes",
                "resource_schema_url",
                "scope_name",
                "scope_version",
                "scope_attributes",
                "scope_schema_url",
            ]
        );
    }

    // -----------------------------------------------------------------------
    // Task 2: Log encoding tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_log_encode_single_event() {
        let event = create_log_event("ERROR", "something went wrong");
        let data = encode_events(vec![event], ParquetCompression::Zstd).expect("encode failed");
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .schema()
                .get_fields()
                .len(),
            18
        );
    }

    #[test]
    fn test_log_encode_service_name() {
        let event = create_log_event("INFO", "test");
        let data =
            encode_events(vec![event], ParquetCompression::Uncompressed).expect("encode failed");
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 0 is service_name (REQUIRED BYTE_ARRAY)
        let mut col_reader = rg.get_column_reader(0).expect("column reader");
        let mut vals: Vec<ByteArray> = Vec::new();
        match &mut col_reader {
            ColumnReader::ByteArrayColumnReader(r) => {
                let (read, _, _) = r.read_records(1, None, None, &mut vals).expect("read");
                assert_eq!(read, 1);
            }
            _ => panic!("expected byte array reader"),
        }
        assert_eq!(
            String::from_utf8(vals[0].data().to_vec()).expect("utf8"),
            "my-service"
        );
    }

    #[test]
    fn test_log_encode_timestamps_as_nanos() {
        use opentelemetry_proto::tonic::logs::v1::LogRecord;

        let record = LogRecord {
            time_unix_nano: 1_000_000_000,
            observed_time_unix_nano: 2_000_000_000,
            severity_text: "INFO".to_string(),
            ..Default::default()
        };
        let log = OtelLog::from_parts(
            record,
            None,
            None,
            sol_core::event::EventMetadata::default(),
        );
        let data = encode_events(vec![Event::Log(log)], ParquetCompression::Uncompressed)
            .expect("encode failed");

        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 2 is time_unix_nano
        let mut col_reader = rg.get_column_reader(2).expect("column reader");
        let mut vals: Vec<i64> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::Int64ColumnReader(r) => {
                let (read, values_read, _) = r
                    .read_records(1, Some(&mut def), None, &mut vals)
                    .expect("read");
                assert_eq!(read, 1);
                assert_eq!(values_read, 1);
            }
            _ => panic!("expected int64 reader"),
        }
        assert_eq!(def[0], 1);
        assert_eq!(vals[0], 1_000_000_000_i64);
    }

    #[test]
    fn test_log_encode_trace_id_fixed_len() {
        use opentelemetry_proto::tonic::logs::v1::LogRecord;

        let record = LogRecord {
            trace_id: vec![0xAB; 16],
            ..Default::default()
        };
        let log = OtelLog::from_parts(
            record,
            None,
            None,
            sol_core::event::EventMetadata::default(),
        );
        let data = encode_events(vec![Event::Log(log)], ParquetCompression::Uncompressed)
            .expect("encode failed");

        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 9 is trace_id (FIXED_LEN_BYTE_ARRAY(16))
        let mut col_reader = rg.get_column_reader(9).expect("column reader");
        let mut vals: Vec<FixedLenByteArray> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::FixedLenByteArrayColumnReader(r) => {
                let (read, values_read, _) = r
                    .read_records(1, Some(&mut def), None, &mut vals)
                    .expect("read");
                assert_eq!(read, 1);
                assert_eq!(values_read, 1);
            }
            _ => panic!("expected fixed len byte array reader"),
        }
        assert_eq!(def[0], 1);
        assert_eq!(vals[0].data(), &[0xAB_u8; 16]);
    }

    #[test]
    fn test_log_encode_sparse_nulls() {
        // Minimal fields — just a default log with severity_text set.
        let mut log = OtelLog::default();
        log.record_mut().severity_text = "ERROR".to_string();
        let event = Event::Log(log);

        let data =
            encode_events(vec![event], ParquetCompression::Uncompressed).expect("encode failed");
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
    }

    #[test]
    fn test_log_encode_batch_100() {
        let events: Vec<Event> = (0..100)
            .map(|i| create_log_event("INFO", &format!("log message {i}")))
            .collect();
        let data = encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 100);
    }

    #[test]
    fn test_log_compression_roundtrip() {
        for compression in [
            ParquetCompression::Zstd,
            ParquetCompression::Snappy,
            ParquetCompression::Gzip,
            ParquetCompression::Uncompressed,
        ] {
            let event = create_log_event("INFO", "compression test");
            let data = encode_events(vec![event], compression.clone()).expect("encode failed");
            let reader = reader_from_bytes(&data);
            assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
        }
    }

    #[test]
    fn test_parquet_encode_empty_events_errors() {
        let result = encode_events(vec![], ParquetCompression::Zstd);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), ParquetEncodingError::NoEvents),
            "expected NoEvents error"
        );
    }

    // -----------------------------------------------------------------------
    // Task 3: Trace schema tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_trace_schema_column_count() {
        let schema = build_trace_schema();
        assert_eq!(
            schema.get_fields().len(),
            24,
            "expected 24 columns in trace schema"
        );
    }

    #[test]
    fn test_trace_schema_column_names() {
        let schema = build_trace_schema();
        let names: Vec<&str> = schema.get_fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec![
                "service_name",
                "start_time_unix_nano",
                "duration_nanos",
                "trace_id",
                "span_id",
                "parent_span_id",
                "trace_state",
                "name",
                "kind",
                "status_code",
                "status_message",
                "attributes",
                "events",
                "links",
                "dropped_attributes_count",
                "dropped_events_count",
                "dropped_links_count",
                "flags",
                "resource_attributes",
                "resource_schema_url",
                "scope_name",
                "scope_version",
                "scope_attributes",
                "scope_schema_url",
            ]
        );
    }

    // -----------------------------------------------------------------------
    // Task 3: Trace encoding tests
    // -----------------------------------------------------------------------

    fn create_trace_span() -> OtelSpan {
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::Span;

        let span = Span {
            trace_id: vec![0xAA; 16],
            span_id: vec![0xBB; 8],
            parent_span_id: vec![0xCC; 8],
            name: "test-span".to_string(),
            kind: 2, // SERVER
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            status: Some(opentelemetry_proto::tonic::trace::v1::Status {
                code: 2, // ERROR
                message: "something failed".to_string(),
            }),
            trace_state: "key=value".to_string(),
            flags: 1,
            dropped_attributes_count: 0,
            dropped_events_count: 0,
            dropped_links_count: 0,
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "http.method".to_string(),
                value: Some(string_value("GET")),
            }],
            events: vec![],
            links: vec![],
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("trace-svc")),
            }],
            dropped_attributes_count: 0,
            ..Default::default()
        };

        let scope = InstrumentationScope {
            name: "test-scope".to_string(),
            version: "1.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        OtelSpan::from_parts(
            span,
            Some(resource),
            Some(scope),
            sol_core::event::EventMetadata::default(),
        )
    }

    fn write_trace_parquet(spans: &[&OtelSpan]) -> Vec<u8> {
        let schema = build_trace_schema();
        let props = default_props();
        write_parquet_file(schema, props, |rg| write_trace_columns(rg, spans))
            .expect("write trace parquet failed")
    }

    fn read_string_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<Option<String>> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<ByteArray> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::ByteArrayColumnReader(r) => {
                r.read_records(n, Some(&mut def), None, &mut vals)
                    .expect("read");
            }
            _ => panic!("expected byte array reader for column {col}"),
        }
        let mut result = Vec::new();
        let mut val_idx = 0;
        for &d in &def {
            if d == 1 {
                result.push(Some(
                    String::from_utf8(vals[val_idx].data().to_vec()).expect("utf8"),
                ));
                val_idx += 1;
            } else {
                result.push(None);
            }
        }
        result
    }

    fn read_required_string_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<String> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<ByteArray> = Vec::new();
        match &mut col_reader {
            ColumnReader::ByteArrayColumnReader(r) => {
                r.read_records(n, None, None, &mut vals).expect("read");
            }
            _ => panic!("expected byte array reader for column {col}"),
        }
        vals.iter()
            .map(|v| String::from_utf8(v.data().to_vec()).expect("utf8"))
            .collect()
    }

    fn read_required_i64_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<i64> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<i64> = Vec::new();
        match &mut col_reader {
            ColumnReader::Int64ColumnReader(r) => {
                r.read_records(n, None, None, &mut vals).expect("read");
            }
            _ => panic!("expected int64 reader for column {col}"),
        }
        vals
    }

    fn read_optional_i32_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<Option<i32>> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<i32> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::Int32ColumnReader(r) => {
                r.read_records(n, Some(&mut def), None, &mut vals)
                    .expect("read");
            }
            _ => panic!("expected int32 reader for column {col}"),
        }
        let mut result = Vec::new();
        let mut val_idx = 0;
        for &d in &def {
            if d == 1 {
                result.push(Some(vals[val_idx]));
                val_idx += 1;
            } else {
                result.push(None);
            }
        }
        result
    }

    fn read_optional_i64_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<Option<i64>> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<i64> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::Int64ColumnReader(r) => {
                r.read_records(n, Some(&mut def), None, &mut vals)
                    .expect("read");
            }
            _ => panic!("expected int64 reader for column {col}"),
        }
        let mut result = Vec::new();
        let mut val_idx = 0;
        for &d in &def {
            if d == 1 {
                result.push(Some(vals[val_idx]));
                val_idx += 1;
            } else {
                result.push(None);
            }
        }
        result
    }

    fn read_optional_double_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<Option<f64>> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<f64> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::DoubleColumnReader(r) => {
                r.read_records(n, Some(&mut def), None, &mut vals)
                    .expect("read");
            }
            _ => panic!("expected double reader for column {col}"),
        }
        let mut result = Vec::new();
        let mut val_idx = 0;
        for &d in &def {
            if d == 1 {
                result.push(Some(vals[val_idx]));
                val_idx += 1;
            } else {
                result.push(None);
            }
        }
        result
    }

    fn read_required_double_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<f64> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<f64> = Vec::new();
        match &mut col_reader {
            ColumnReader::DoubleColumnReader(r) => {
                r.read_records(n, None, None, &mut vals).expect("read");
            }
            _ => panic!("expected double reader for column {col}"),
        }
        vals
    }

    fn read_optional_bool_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<Option<bool>> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<bool> = Vec::new();
        let mut def: Vec<i16> = Vec::new();
        match &mut col_reader {
            ColumnReader::BoolColumnReader(r) => {
                r.read_records(n, Some(&mut def), None, &mut vals)
                    .expect("read");
            }
            _ => panic!("expected bool reader for column {col}"),
        }
        let mut result = Vec::new();
        let mut val_idx = 0;
        for &d in &def {
            if d == 1 {
                result.push(Some(vals[val_idx]));
                val_idx += 1;
            } else {
                result.push(None);
            }
        }
        result
    }

    fn read_required_fixed_bytes_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<Vec<u8>> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<FixedLenByteArray> = Vec::new();
        match &mut col_reader {
            ColumnReader::FixedLenByteArrayColumnReader(r) => {
                r.read_records(n, None, None, &mut vals).expect("read");
            }
            _ => panic!("expected fixed len byte array reader for column {col}"),
        }
        vals.iter().map(|v| v.data().to_vec()).collect()
    }

    fn read_required_i32_column(
        rg: &dyn parquet::file::reader::RowGroupReader,
        col: usize,
        n: usize,
    ) -> Vec<i32> {
        let mut col_reader = rg.get_column_reader(col).expect("column reader");
        let mut vals: Vec<i32> = Vec::new();
        match &mut col_reader {
            ColumnReader::Int32ColumnReader(r) => {
                r.read_records(n, None, None, &mut vals).expect("read");
            }
            _ => panic!("expected int32 reader for column {col}"),
        }
        vals
    }

    #[test]
    fn test_trace_encode_single_span() {
        let span = create_trace_span();
        let data = write_trace_parquet(&[&span]);
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .schema()
                .get_fields()
                .len(),
            24
        );
    }

    #[test]
    fn test_trace_encode_duration_nanos() {
        let span = create_trace_span();
        let data = write_trace_parquet(&[&span]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 2: duration_nanos = end - start = 2_000_000_000 - 1_000_000_000 = 1_000_000_000
        let durations = read_required_i64_column(&*rg, 2, 1);
        assert_eq!(durations, vec![1_000_000_000_i64]);
    }

    #[test]
    fn test_trace_encode_service_name() {
        let span = create_trace_span();
        let data = write_trace_parquet(&[&span]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 0: service_name
        let names = read_required_string_column(&*rg, 0, 1);
        assert_eq!(names, vec!["trace-svc"]);
    }

    #[test]
    fn test_trace_encode_events_as_json() {
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{Span, span};

        let span_proto = Span {
            trace_id: vec![0xAA; 16],
            span_id: vec![0xBB; 8],
            name: "with-events".to_string(),
            kind: 1,
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 2_000,
            events: vec![span::Event {
                time_unix_nano: 1_500,
                name: "test-event".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }],
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("svc")),
            }],
            ..Default::default()
        };

        let otel_span = OtelSpan::from_parts(
            span_proto,
            Some(resource),
            Some(InstrumentationScope::default()),
            sol_core::event::EventMetadata::default(),
        );

        let data = write_trace_parquet(&[&otel_span]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 12: events (OPTIONAL UTF8)
        let events = read_string_column(&*rg, 12, 1);
        assert!(events[0].is_some(), "events should be present");
        let json: serde_json::Value =
            serde_json::from_str(events[0].as_ref().unwrap()).expect("valid json");
        assert!(json.is_array());
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"], "test-event");
    }

    #[test]
    fn test_trace_encode_links_as_json() {
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use opentelemetry_proto::tonic::trace::v1::{Span, span};

        let span_proto = Span {
            trace_id: vec![0xAA; 16],
            span_id: vec![0xBB; 8],
            name: "with-links".to_string(),
            kind: 1,
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 2_000,
            links: vec![span::Link {
                trace_id: vec![0xDD; 16],
                span_id: vec![0xEE; 8],
                trace_state: "".to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
            }],
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("svc")),
            }],
            ..Default::default()
        };

        let otel_span = OtelSpan::from_parts(
            span_proto,
            Some(resource),
            Some(InstrumentationScope::default()),
            sol_core::event::EventMetadata::default(),
        );

        let data = write_trace_parquet(&[&otel_span]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 13: links (OPTIONAL UTF8)
        let links = read_string_column(&*rg, 13, 1);
        assert!(links[0].is_some(), "links should be present");
        let json: serde_json::Value =
            serde_json::from_str(links[0].as_ref().unwrap()).expect("valid json");
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_trace_encode_status() {
        let span = create_trace_span();
        let data = write_trace_parquet(&[&span]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 9: status_code (OPTIONAL INT32)
        let codes = read_optional_i32_column(&*rg, 9, 1);
        assert_eq!(codes, vec![Some(2)]); // ERROR = 2

        // Column 10: status_message (OPTIONAL UTF8)
        let messages = read_string_column(&*rg, 10, 1);
        assert_eq!(messages, vec![Some("something failed".to_string())]);
    }

    #[test]
    fn test_trace_encode_fixed_len_ids() {
        let span = create_trace_span();
        let data = write_trace_parquet(&[&span]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 3: trace_id (REQUIRED FIXED_LEN_BYTE_ARRAY(16))
        let trace_ids = read_required_fixed_bytes_column(&*rg, 3, 1);
        assert_eq!(trace_ids[0], vec![0xAA_u8; 16]);

        // Column 4: span_id (REQUIRED FIXED_LEN_BYTE_ARRAY(8))
        let span_ids = read_required_fixed_bytes_column(&*rg, 4, 1);
        assert_eq!(span_ids[0], vec![0xBB_u8; 8]);
    }

    #[test]
    fn test_trace_encode_batch() {
        let span1 = create_trace_span();
        let mut span2_proto = opentelemetry_proto::tonic::trace::v1::Span {
            trace_id: vec![0x11; 16],
            span_id: vec![0x22; 8],
            name: "second-span".to_string(),
            kind: 3, // CLIENT
            start_time_unix_nano: 3_000_000_000,
            end_time_unix_nano: 4_000_000_000,
            ..Default::default()
        };
        let _ = &mut span2_proto; // suppress unused warning
        let resource = opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("trace-svc-2")),
            }],
            ..Default::default()
        };
        let span2 = OtelSpan::from_parts(
            span2_proto,
            Some(resource),
            None,
            sol_core::event::EventMetadata::default(),
        );

        let data = write_trace_parquet(&[&span1, &span2]);
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);

        let rg = reader.get_row_group(0).expect("row group");
        let names = read_required_string_column(&*rg, 7, 2); // Column 7: name
        assert_eq!(names, vec!["test-span", "second-span"]);
    }

    // -----------------------------------------------------------------------
    // Task 4: Gauge schema and encoding tests
    // -----------------------------------------------------------------------

    fn create_gauge_metric(int_value: Option<i64>, double_value: Option<f64>) -> OtelMetric {
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, number_data_point::Value as NDPValue,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let value = match (int_value, double_value) {
            (Some(i), _) => Some(NDPValue::AsInt(i)),
            (_, Some(d)) => Some(NDPValue::AsDouble(d)),
            _ => None,
        };

        let proto = Metric {
            name: "test.gauge".to_string(),
            description: "a test gauge".to_string(),
            unit: "ms".to_string(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 1_000_000_000,
                    start_time_unix_nano: 500_000_000,
                    value,
                    attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "dp.key".to_string(),
                        value: Some(string_value("dp-val")),
                    }],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("gauge-svc")),
            }],
            ..Default::default()
        };

        let scope = InstrumentationScope {
            name: "test-scope".to_string(),
            version: "1.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        OtelMetric::from_parts(
            proto,
            Some(resource),
            Some(scope),
            sol_core::event::EventMetadata::default(),
        )
    }

    fn write_gauge_parquet(metrics: &[&OtelMetric]) -> Vec<u8> {
        let schema = build_gauge_schema();
        let props = default_props();
        write_parquet_file(schema, props, |rg| write_gauge_columns(rg, metrics))
            .expect("write gauge parquet failed")
    }

    #[test]
    fn test_gauge_schema_column_count() {
        let schema = build_gauge_schema();
        assert_eq!(
            schema.get_fields().len(),
            17,
            "expected 17 columns in gauge schema"
        );
    }

    #[test]
    fn test_gauge_encode_int_value() {
        let metric = create_gauge_metric(Some(42), None);
        let data = write_gauge_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 1);

        let rg = reader.get_row_group(0).expect("row group");

        // Column 0: service_name
        let names = read_required_string_column(&*rg, 0, 1);
        assert_eq!(names, vec!["gauge-svc"]);

        // Column 15: int_value (OPTIONAL INT64)
        let int_vals = read_optional_i64_column(&*rg, 15, 1);
        assert_eq!(int_vals, vec![Some(42)]);

        // Column 16: double_value (OPTIONAL DOUBLE)
        let dbl_vals = read_optional_double_column(&*rg, 16, 1);
        assert_eq!(dbl_vals, vec![None]);
    }

    #[test]
    fn test_gauge_encode_double_value() {
        let metric = create_gauge_metric(None, Some(42.5));
        let data = write_gauge_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 15: int_value (OPTIONAL INT64)
        let int_vals = read_optional_i64_column(&*rg, 15, 1);
        assert_eq!(int_vals, vec![None]);

        // Column 16: double_value (OPTIONAL DOUBLE)
        let dbl_vals = read_optional_double_column(&*rg, 16, 1);
        assert_eq!(dbl_vals.len(), 1);
        assert!((dbl_vals[0].unwrap() - 42.5).abs() < 1e-10);
    }

    #[test]
    fn test_gauge_encode_multiple_data_points() {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, number_data_point::Value as NDPValue,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let proto = Metric {
            name: "multi.gauge".to_string(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![
                    NumberDataPoint {
                        time_unix_nano: 1_000,
                        value: Some(NDPValue::AsInt(10)),
                        ..Default::default()
                    },
                    NumberDataPoint {
                        time_unix_nano: 2_000,
                        value: Some(NDPValue::AsInt(20)),
                        ..Default::default()
                    },
                ],
            })),
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("svc")),
            }],
            ..Default::default()
        };

        let metric = OtelMetric::from_parts(
            proto,
            Some(resource),
            None,
            sol_core::event::EventMetadata::default(),
        );

        let data = write_gauge_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        // Two data points = two rows
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);

        let rg = reader.get_row_group(0).expect("row group");
        let int_vals = read_optional_i64_column(&*rg, 15, 2);
        assert_eq!(int_vals, vec![Some(10), Some(20)]);
    }

    // -----------------------------------------------------------------------
    // Task 4: Sum schema and encoding tests
    // -----------------------------------------------------------------------

    fn create_sum_metric(value: f64, is_monotonic: bool, temporality: i32) -> OtelMetric {
        use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric, NumberDataPoint, Sum, number_data_point::Value as NDPValue,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let proto = Metric {
            name: "test.sum".to_string(),
            description: "a test sum".to_string(),
            unit: "1".to_string(),
            data: Some(Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 1_000_000_000,
                    value: Some(NDPValue::AsDouble(value)),
                    ..Default::default()
                }],
                aggregation_temporality: temporality,
                is_monotonic,
            })),
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("sum-svc")),
            }],
            ..Default::default()
        };

        let scope = InstrumentationScope {
            name: "test-scope".to_string(),
            version: "1.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        };

        OtelMetric::from_parts(
            proto,
            Some(resource),
            Some(scope),
            sol_core::event::EventMetadata::default(),
        )
    }

    fn write_sum_parquet(metrics: &[&OtelMetric]) -> Vec<u8> {
        let schema = build_sum_schema();
        let props = default_props();
        write_parquet_file(schema, props, |rg| write_sum_columns(rg, metrics))
            .expect("write sum parquet failed")
    }

    #[test]
    fn test_sum_schema_column_count() {
        let schema = build_sum_schema();
        assert_eq!(
            schema.get_fields().len(),
            19,
            "expected 19 columns in sum schema"
        );
    }

    #[test]
    fn test_sum_encode_with_temporality() {
        // CUMULATIVE = 2
        let metric = create_sum_metric(100.0, false, 2);
        let data = write_sum_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 17: aggregation_temporality (OPTIONAL INT32)
        let temps = read_optional_i32_column(&*rg, 17, 1);
        assert_eq!(temps, vec![Some(2)]);

        // Column 18: is_monotonic (OPTIONAL BOOLEAN)
        let mono = read_optional_bool_column(&*rg, 18, 1);
        assert_eq!(mono, vec![Some(false)]);
    }

    #[test]
    fn test_sum_encode_counter() {
        // DELTA = 1, is_monotonic = true
        let metric = create_sum_metric(99.5, true, 1);
        let data = write_sum_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 16: double_value
        let dbl_vals = read_optional_double_column(&*rg, 16, 1);
        assert_eq!(dbl_vals.len(), 1);
        assert!((dbl_vals[0].unwrap() - 99.5).abs() < 1e-10);

        // Column 17: aggregation_temporality
        let temps = read_optional_i32_column(&*rg, 17, 1);
        assert_eq!(temps, vec![Some(1)]); // DELTA

        // Column 18: is_monotonic
        let mono = read_optional_bool_column(&*rg, 18, 1);
        assert_eq!(mono, vec![Some(true)]);
    }

    // -----------------------------------------------------------------------
    // Task 5: Histogram schema and encoding tests
    // -----------------------------------------------------------------------

    fn create_histogram_metric() -> OtelMetric {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
        use opentelemetry_proto::tonic::metrics::v1::{Histogram, HistogramDataPoint, Metric};
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let proto = Metric {
            name: "test.histogram".to_string(),
            description: "a test histogram".to_string(),
            unit: "ms".to_string(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 1_000_000_000,
                    count: 100,
                    sum: Some(5000.0),
                    min: Some(1.0),
                    max: Some(999.0),
                    bucket_counts: vec![10, 30, 40, 20],
                    explicit_bounds: vec![10.0, 100.0, 500.0],
                    ..Default::default()
                }],
                aggregation_temporality: 2, // CUMULATIVE
            })),
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("hist-svc")),
            }],
            ..Default::default()
        };

        OtelMetric::from_parts(
            proto,
            Some(resource),
            None,
            sol_core::event::EventMetadata::default(),
        )
    }

    fn write_histogram_parquet(metrics: &[&OtelMetric]) -> Vec<u8> {
        let schema = build_histogram_schema();
        let props = default_props();
        write_parquet_file(schema, props, |rg| write_histogram_columns(rg, metrics))
            .expect("write histogram parquet failed")
    }

    #[test]
    fn test_histogram_schema_column_count() {
        let schema = build_histogram_schema();
        assert_eq!(
            schema.get_fields().len(),
            22,
            "expected 22 columns in histogram schema"
        );
    }

    #[test]
    fn test_histogram_encode_buckets() {
        let metric = create_histogram_metric();
        let data = write_histogram_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 19: bucket_counts (OPTIONAL UTF8, JSON)
        let bc = read_string_column(&*rg, 19, 1);
        assert!(bc[0].is_some());
        let arr: Vec<u64> = serde_json::from_str(bc[0].as_ref().unwrap()).expect("valid json");
        assert_eq!(arr, vec![10, 30, 40, 20]);

        // Column 20: explicit_bounds (OPTIONAL UTF8, JSON)
        let eb = read_string_column(&*rg, 20, 1);
        assert!(eb[0].is_some());
        let bounds: Vec<f64> = serde_json::from_str(eb[0].as_ref().unwrap()).expect("valid json");
        assert_eq!(bounds, vec![10.0, 100.0, 500.0]);
    }

    #[test]
    fn test_histogram_encode_count_sum_min_max() {
        let metric = create_histogram_metric();
        let data = write_histogram_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 15: count (REQUIRED INT64)
        let counts = read_required_i64_column(&*rg, 15, 1);
        assert_eq!(counts, vec![100]);

        // Column 16: sum (OPTIONAL DOUBLE)
        let sums = read_optional_double_column(&*rg, 16, 1);
        assert!((sums[0].unwrap() - 5000.0).abs() < 1e-10);

        // Column 17: min (OPTIONAL DOUBLE)
        let mins = read_optional_double_column(&*rg, 17, 1);
        assert!((mins[0].unwrap() - 1.0).abs() < 1e-10);

        // Column 18: max (OPTIONAL DOUBLE)
        let maxes = read_optional_double_column(&*rg, 18, 1);
        assert!((maxes[0].unwrap() - 999.0).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // Task 5: ExpHistogram schema and encoding tests
    // -----------------------------------------------------------------------

    fn create_exp_histogram_metric() -> OtelMetric {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
        use opentelemetry_proto::tonic::metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint, Metric,
            exponential_histogram_data_point::Buckets,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let proto = Metric {
            name: "test.exp_histogram".to_string(),
            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 1_000_000_000,
                    count: 50,
                    sum: Some(2500.0),
                    min: Some(0.5),
                    max: Some(500.0),
                    scale: 3,
                    zero_count: 2,
                    zero_threshold: 0.001,
                    positive: Some(Buckets {
                        offset: 1,
                        bucket_counts: vec![5, 10, 15, 20],
                    }),
                    negative: Some(Buckets {
                        offset: -2,
                        bucket_counts: vec![3, 7],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: 2,
            })),
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("exp-hist-svc")),
            }],
            ..Default::default()
        };

        OtelMetric::from_parts(
            proto,
            Some(resource),
            None,
            sol_core::event::EventMetadata::default(),
        )
    }

    fn write_exp_histogram_parquet(metrics: &[&OtelMetric]) -> Vec<u8> {
        let schema = build_exp_histogram_schema();
        let props = default_props();
        write_parquet_file(schema, props, |rg| write_exp_histogram_columns(rg, metrics))
            .expect("write exp histogram parquet failed")
    }

    #[test]
    fn test_exp_histogram_schema_column_count() {
        let schema = build_exp_histogram_schema();
        assert_eq!(
            schema.get_fields().len(),
            27,
            "expected 27 columns in exp histogram schema"
        );
    }

    #[test]
    fn test_exp_histogram_encode_buckets() {
        let metric = create_exp_histogram_metric();
        let data = write_exp_histogram_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 19: scale (REQUIRED INT32)
        let scales = read_required_i32_column(&*rg, 19, 1);
        assert_eq!(scales, vec![3]);

        // Column 20: zero_count (REQUIRED INT64)
        let zc = read_required_i64_column(&*rg, 20, 1);
        assert_eq!(zc, vec![2]);

        // Column 22: positive_offset (OPTIONAL INT32)
        let po = read_optional_i32_column(&*rg, 22, 1);
        assert_eq!(po, vec![Some(1)]);

        // Column 23: positive_bucket_counts (OPTIONAL UTF8, JSON)
        let pbc = read_string_column(&*rg, 23, 1);
        assert!(pbc[0].is_some());
        let arr: Vec<u64> = serde_json::from_str(pbc[0].as_ref().unwrap()).expect("valid json");
        assert_eq!(arr, vec![5, 10, 15, 20]);

        // Column 24: negative_offset (OPTIONAL INT32)
        let no = read_optional_i32_column(&*rg, 24, 1);
        assert_eq!(no, vec![Some(-2)]);

        // Column 25: negative_bucket_counts (OPTIONAL UTF8, JSON)
        let nbc = read_string_column(&*rg, 25, 1);
        assert!(nbc[0].is_some());
        let narr: Vec<u64> = serde_json::from_str(nbc[0].as_ref().unwrap()).expect("valid json");
        assert_eq!(narr, vec![3, 7]);

        // Column 26: aggregation_temporality (OPTIONAL INT32)
        let temps = read_optional_i32_column(&*rg, 26, 1);
        assert_eq!(temps, vec![Some(2)]);
    }

    // -----------------------------------------------------------------------
    // Task 5: Summary schema and encoding tests
    // -----------------------------------------------------------------------

    fn create_summary_metric() -> OtelMetric {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric, Summary, SummaryDataPoint, summary_data_point::ValueAtQuantile,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let proto = Metric {
            name: "test.summary".to_string(),
            data: Some(Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    time_unix_nano: 1_000_000_000,
                    count: 200,
                    sum: 10_000.0,
                    quantile_values: vec![
                        ValueAtQuantile {
                            quantile: 0.5,
                            value: 50.0,
                        },
                        ValueAtQuantile {
                            quantile: 0.99,
                            value: 990.0,
                        },
                    ],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };

        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("summary-svc")),
            }],
            ..Default::default()
        };

        OtelMetric::from_parts(
            proto,
            Some(resource),
            None,
            sol_core::event::EventMetadata::default(),
        )
    }

    fn write_summary_parquet(metrics: &[&OtelMetric]) -> Vec<u8> {
        let schema = build_summary_schema();
        let props = default_props();
        write_parquet_file(schema, props, |rg| write_summary_columns(rg, metrics))
            .expect("write summary parquet failed")
    }

    #[test]
    fn test_summary_schema_column_count() {
        let schema = build_summary_schema();
        assert_eq!(
            schema.get_fields().len(),
            18,
            "expected 18 columns in summary schema"
        );
    }

    #[test]
    fn test_summary_encode_quantiles() {
        let metric = create_summary_metric();
        let data = write_summary_parquet(&[&metric]);
        let reader = reader_from_bytes(&data);
        let rg = reader.get_row_group(0).expect("row group");

        // Column 15: count (REQUIRED INT64)
        let counts = read_required_i64_column(&*rg, 15, 1);
        assert_eq!(counts, vec![200]);

        // Column 16: sum (REQUIRED DOUBLE)
        let sums = read_required_double_column(&*rg, 16, 1);
        assert!((sums[0] - 10_000.0).abs() < 1e-10);

        // Column 17: quantile_values (OPTIONAL UTF8, JSON)
        let qv = read_string_column(&*rg, 17, 1);
        assert!(qv[0].is_some());
        let json: serde_json::Value =
            serde_json::from_str(qv[0].as_ref().unwrap()).expect("valid json");
        assert!(json.is_array());
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Task 6: Signal routing and ParquetSerializer integration tests
    // -----------------------------------------------------------------------

    /// Count the number of Parquet files concatenated in a buffer.
    ///
    /// Each Parquet file starts with `PAR1` magic and ends with `PAR1`.
    /// We count non-overlapping occurrences and divide by 2.
    fn count_parquet_files(data: &[u8]) -> usize {
        let magic = b"PAR1";
        let count = data.windows(4).filter(|w| *w == magic).count();
        count / 2
    }

    fn create_trace_event() -> Event {
        Event::Trace(create_trace_span())
    }

    fn create_gauge_event() -> Event {
        Event::Metric(create_gauge_metric(Some(42), None))
    }

    fn create_histogram_event() -> Event {
        Event::Metric(create_histogram_metric())
    }

    #[test]
    fn test_encode_logs_only() {
        let events = vec![
            create_log_event("INFO", "log1"),
            create_log_event("ERROR", "log2"),
        ];
        let data = encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
        assert_eq!(count_parquet_files(&data), 1);
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .schema()
                .get_fields()
                .len(),
            18, // log schema has 18 columns
        );
    }

    #[test]
    fn test_encode_traces_only() {
        let events = vec![create_trace_event(), create_trace_event()];
        let data = encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
        assert_eq!(count_parquet_files(&data), 1);
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 2);
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .schema()
                .get_fields()
                .len(),
            24, // trace schema has 24 columns
        );
    }

    #[test]
    fn test_encode_gauge_only() {
        let events = vec![create_gauge_event()];
        let data = encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
        assert_eq!(count_parquet_files(&data), 1);
        let reader = reader_from_bytes(&data);
        assert_eq!(reader.metadata().file_metadata().num_rows(), 1);
        assert_eq!(
            reader
                .metadata()
                .file_metadata()
                .schema()
                .get_fields()
                .len(),
            17, // gauge schema has 17 columns
        );
    }

    #[test]
    fn test_encode_mixed_signals() {
        // logs + traces -> two Parquet files in buffer
        let events = vec![create_log_event("INFO", "a log"), create_trace_event()];
        let data = encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
        assert_eq!(count_parquet_files(&data), 2);
    }

    #[test]
    fn test_encode_mixed_metric_subtypes() {
        // gauge + histogram -> two Parquet files
        let events = vec![create_gauge_event(), create_histogram_event()];
        let data = encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
        assert_eq!(count_parquet_files(&data), 2);
    }

    #[test]
    fn test_encode_empty_batch_error() {
        let result = encode_events(vec![], ParquetCompression::Uncompressed);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ParquetEncodingError::NoEvents
        ));
    }

    #[test]
    fn test_encode_all_signal_types() {
        // logs + traces + gauge + histogram -> 4 Parquet files
        let events = vec![
            create_log_event("INFO", "a log"),
            create_trace_event(),
            create_gauge_event(),
            create_histogram_event(),
        ];
        let data = encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
        assert_eq!(count_parquet_files(&data), 4);
    }

    // -----------------------------------------------------------------------
    // Existing tests below
    // -----------------------------------------------------------------------

    #[test]
    fn test_parquet_config_default_compression() {
        let config = ParquetSerializerConfig::default();
        assert_eq!(config.compression, ParquetCompression::Zstd);
    }

    #[test]
    fn test_parquet_config_deserialize() {
        let json = r#"{"compression":"snappy"}"#;
        let config: ParquetSerializerConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(config.compression, ParquetCompression::Snappy);

        let json = r#"{"compression":"none"}"#;
        let config: ParquetSerializerConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(config.compression, ParquetCompression::Uncompressed);

        let json = r#"{}"#;
        let config: ParquetSerializerConfig = serde_json::from_str(json).expect("deserialize");
        assert_eq!(config.compression, ParquetCompression::Zstd);
    }
}
