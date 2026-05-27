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
    write_fn: impl FnOnce(&mut SerializedRowGroupWriter<'_, Vec<u8>>) -> Result<(), ParquetEncodingError>,
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
        let (values, def_levels) =
            collect_optional_nanos(logs, OtelLog::observed_time_unix_nano);
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
            if s.is_empty() { None } else { Some(s.to_string()) }
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
            if attrs.is_empty() { None } else { Some(attrs_to_json(attrs)) }
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
// ParquetSerializer (Task 2)
// ---------------------------------------------------------------------------

/// Serializes batches of events into complete Parquet files.
#[derive(Clone, Debug)]
pub struct ParquetSerializer {
    log_schema: Arc<Type>,
    writer_props: WriterProperties,
}

impl ParquetSerializer {
    /// Create a new Parquet serializer with the given configuration.
    pub fn new(config: &ParquetSerializerConfig) -> Self {
        let log_schema = build_otel_log_schema();
        let writer_props = WriterProperties::builder()
            .set_compression(config.compression.to_parquet())
            .build();
        Self {
            log_schema,
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

        let logs: Vec<&OtelLog> = events
            .iter()
            .filter_map(|e| match e {
                Event::Log(otel_log) => Some(otel_log),
                _ => None,
            })
            .collect();

        if logs.is_empty() {
            return Err(ParquetEncodingError::NoEvents);
        }

        let schema = Arc::clone(&self.log_schema);
        let props = Arc::new(self.writer_props.clone());

        let buf = write_parquet_file(schema, props, |rg| write_log_columns(rg, &logs))?;

        buffer.put_slice(&buf);
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
                let (read, _, _) = r
                    .read_records(3, None, None, &mut vals)
                    .expect("read");
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
        let names: Vec<&str> = schema
            .get_fields()
            .iter()
            .map(|f| f.name())
            .collect();
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
                let (read, _, _) = r
                    .read_records(1, None, None, &mut vals)
                    .expect("read");
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
        let data = encode_events(
            vec![Event::Log(log)],
            ParquetCompression::Uncompressed,
        )
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
        let data = encode_events(
            vec![Event::Log(log)],
            ParquetCompression::Uncompressed,
        )
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
        let data =
            encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
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
            let data =
                encode_events(vec![event], compression.clone()).expect("encode failed");
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
