//! Parquet format codec for batched event encoding.
//!
//! Converts batches of OTLP log events into complete Parquet files with
//! column-level compression. Each `encode()` call produces a self-contained
//! Parquet file (header + row groups + footer).

use arrow::{
    array::ArrayRef,
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use bytes::{BufMut, BytesMut};
use chrono::{DateTime, Utc};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use snafu::Snafu;
use sol_config::configurable_component;
use sol_core::event::{Event, OtelLog, Value};
use std::sync::Arc;

/// Build the fixed Arrow schema for OTLP LogRecord Parquet files.
pub fn build_otel_log_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            "observed_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("flags", DataType::Int32, true),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        Field::new("dropped_attributes_count", DataType::Int32, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
    ]))
}

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

/// Serializes batches of events into complete Parquet files.
#[derive(Clone, Debug)]
pub struct ParquetSerializer {
    schema: SchemaRef,
    writer_props: WriterProperties,
}

impl ParquetSerializer {
    /// Create a new Parquet serializer with the given configuration.
    pub fn new(config: &ParquetSerializerConfig) -> Self {
        let schema = build_otel_log_schema();
        let writer_props = WriterProperties::builder()
            .set_compression(config.compression.to_parquet())
            .build();
        Self {
            schema,
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
    /// serde_arrow serialization failed.
    #[snafu(display("serde_arrow error: {source}"))]
    SerdeArrow {
        /// The source error.
        source: serde_arrow::Error,
    },
    /// Arrow RecordBatch creation failed.
    #[snafu(display("record batch error: {source}"))]
    RecordBatchCreation {
        /// The source error.
        source: arrow::error::ArrowError,
    },
    /// Parquet write failed.
    #[snafu(display("parquet write error: {source}"))]
    ParquetWrite {
        /// The source error.
        source: parquet::errors::ParquetError,
    },
    /// Timestamp overflow.
    #[snafu(display("timestamp overflow for field '{field_name}': {timestamp}"))]
    TimestampOverflow {
        /// The field that overflowed.
        field_name: String,
        /// The timestamp value.
        timestamp: String,
    },
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

        let record_batch = build_record_batch(Arc::clone(&self.schema), &events)?;

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut buf,
            record_batch.schema(),
            Some(self.writer_props.clone()),
        )
        .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        writer
            .write(&record_batch)
            .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;
        writer
            .close()
            .map_err(|source| ParquetEncodingError::ParquetWrite { source })?;

        buffer.put_slice(&buf);
        Ok(())
    }
}

fn build_record_batch(
    schema: SchemaRef,
    events: &[Event],
) -> Result<RecordBatch, ParquetEncodingError> {
    let log_events: Vec<OtelLog> = events
        .iter()
        .filter_map(|e| match e {
            Event::Log(otel_log) => Some(otel_log.clone()),
            _ => None,
        })
        .collect();

    let mut flat_maps: Vec<vrl::value::ObjectMap> = log_events
        .iter()
        .map(|log| log.as_map().unwrap_or_default())
        .collect();

    for map in &mut flat_maps {
        convert_timestamps_in_map(map, &schema)?;
    }

    let batch = serde_arrow::to_record_batch(schema.fields(), &flat_maps)
        .map_err(|source| ParquetEncodingError::SerdeArrow { source })?;

    let columns: Result<Vec<ArrayRef>, _> = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(col, field)| {
            if col.data_type() == field.data_type() {
                Ok(col.clone())
            } else {
                cast_with_options(col, field.data_type(), &CastOptions::default())
                    .map_err(|source| ParquetEncodingError::RecordBatchCreation { source })
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns?)
        .map_err(|source| ParquetEncodingError::RecordBatchCreation { source })
}

fn convert_timestamps_in_map(
    map: &mut vrl::value::ObjectMap,
    schema: &SchemaRef,
) -> Result<(), ParquetEncodingError> {
    for field in schema.fields() {
        if let DataType::Timestamp(unit, _) = field.data_type() {
            let field_name = field.name().as_str();
            let key = vrl::value::KeyString::from(field_name.to_string());
            let ts = match map.get(&key) {
                Some(Value::Timestamp(ts)) => Some(*ts),
                Some(Value::Bytes(b)) => std::str::from_utf8(b).ok().and_then(|s| {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .ok()
                        .map(|dt| dt.with_timezone(&Utc))
                }),
                _ => None,
            };
            if let Some(ts) = ts {
                let val = timestamp_to_nanos(&ts).ok_or_else(|| {
                    ParquetEncodingError::TimestampOverflow {
                        field_name: field_name.to_string(),
                        timestamp: ts.to_rfc3339(),
                    }
                })?;
                let _ = unit; // all our timestamp fields use Nanosecond
                map.insert(key, Value::Integer(val));
            }
        }
    }
    Ok(())
}

fn timestamp_to_nanos(ts: &DateTime<Utc>) -> Option<i64> {
    ts.timestamp_nanos_opt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tokio_util::codec::Encoder;

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

    fn read_parquet(data: &[u8]) -> RecordBatch {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(data))
            .expect("failed to create parquet reader")
            .build()
            .expect("failed to build reader");
        let batches: Vec<RecordBatch> = reader.map(|r| r.expect("read error")).collect();
        assert_eq!(batches.len(), 1, "expected exactly one row group");
        batches.into_iter().next().expect("no batch")
    }

    fn create_log_event(severity: &str, body: &str) -> Event {
        let mut log = OtelLog::default();
        log.insert("severity_text", Value::from(severity.to_string()));
        log.insert("severity_number", Value::Integer(17));
        log.insert("body", Value::from(body.to_string()));
        log.insert(
            "attributes",
            Value::from(r#"{"service.name":"test"}"#.to_string()),
        );
        log.insert("flags", Value::Integer(0));
        log.insert("dropped_attributes_count", Value::Integer(0));
        log.insert("resource_schema_url", Value::from(String::new()));
        log.insert("scope_name", Value::from("test-scope".to_string()));
        log.insert("scope_version", Value::from("1.0".to_string()));
        log.insert("scope_attributes", Value::from(r#"{}"#.to_string()));
        log.insert("scope_schema_url", Value::from(String::new()));
        log.insert(
            "resource_attributes",
            Value::from(r#"{"service.name":"my-service"}"#.to_string()),
        );
        Event::Log(log)
    }

    // --- Schema tests ---

    #[test]
    fn test_otel_log_schema_column_count() {
        let schema = build_otel_log_schema();
        assert_eq!(schema.fields().len(), 16);
    }

    #[test]
    fn test_otel_log_schema_column_names() {
        let schema = build_otel_log_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
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

    #[test]
    fn test_otel_log_schema_column_types() {
        let schema = build_otel_log_schema();

        let assert_type = |name: &str, expected: DataType| {
            let field = schema.field_with_name(name).unwrap_or_else(|_| {
                panic!("field '{name}' not found");
            });
            assert_eq!(
                field.data_type(),
                &expected,
                "field '{name}' has wrong type"
            );
            assert!(field.is_nullable(), "field '{name}' should be nullable");
        };

        assert_type(
            "time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        );
        assert_type(
            "observed_time_unix_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        );
        assert_type("severity_number", DataType::Int32);
        assert_type("severity_text", DataType::Utf8);
        assert_type("body", DataType::Utf8);
        assert_type("attributes", DataType::Utf8);
        assert_type("flags", DataType::Int32);
        assert_type("trace_id", DataType::FixedSizeBinary(16));
        assert_type("span_id", DataType::FixedSizeBinary(8));
        assert_type("dropped_attributes_count", DataType::Int32);
        assert_type("resource_attributes", DataType::Utf8);
        assert_type("resource_schema_url", DataType::Utf8);
        assert_type("scope_name", DataType::Utf8);
        assert_type("scope_version", DataType::Utf8);
        assert_type("scope_attributes", DataType::Utf8);
        assert_type("scope_schema_url", DataType::Utf8);
    }

    // --- Encoding tests ---

    #[test]
    fn test_parquet_encode_single_event() {
        let event = create_log_event("ERROR", "something went wrong");
        let data = encode_events(vec![event], ParquetCompression::Zstd).expect("encode failed");
        let batch = read_parquet(&data);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 16);
    }

    #[test]
    fn test_parquet_encode_batch() {
        let events: Vec<Event> = (0..100)
            .map(|i| create_log_event("INFO", &format!("log message {i}")))
            .collect();
        let data = encode_events(events, ParquetCompression::Uncompressed).expect("encode failed");
        let batch = read_parquet(&data);
        assert_eq!(batch.num_rows(), 100);
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
    fn test_parquet_encode_attributes_as_json() {
        let event = create_log_event("INFO", "test");
        let data = encode_events(vec![event], ParquetCompression::Zstd).expect("encode failed");
        let batch = read_parquet(&data);

        let attrs_col = batch
            .column_by_name("attributes")
            .expect("attributes column missing");
        let attrs_array = attrs_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("attributes should be StringArray");
        let json_str = attrs_array.value(0);
        let parsed: serde_json::Value =
            serde_json::from_str(json_str).expect("attributes should be valid JSON");
        assert!(parsed.is_object());
    }

    #[test]
    fn test_parquet_roundtrip() {
        let event = create_log_event("WARN", "round trip test");
        let data = encode_events(vec![event], ParquetCompression::Zstd).expect("encode failed");
        let batch = read_parquet(&data);

        let severity_col = batch
            .column_by_name("severity_text")
            .expect("severity_text missing");
        let severity = severity_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should be StringArray");
        assert_eq!(severity.value(0), "WARN");

        let body_col = batch.column_by_name("body").expect("body missing");
        let body = body_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should be StringArray");
        assert_eq!(body.value(0), "round trip test");

        let severity_num_col = batch
            .column_by_name("severity_number")
            .expect("severity_number missing");
        let severity_num = severity_num_col
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("should be Int32Array");
        assert_eq!(severity_num.value(0), 17);
    }

    // --- Compression tests ---

    #[test]
    fn test_parquet_compression_zstd() {
        let event = create_log_event("INFO", "zstd test");
        let data = encode_events(vec![event], ParquetCompression::Zstd).expect("encode failed");
        let batch = read_parquet(&data);
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_parquet_compression_snappy() {
        let event = create_log_event("INFO", "snappy test");
        let data = encode_events(vec![event], ParquetCompression::Snappy).expect("encode failed");
        let batch = read_parquet(&data);
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_parquet_compression_gzip() {
        let event = create_log_event("INFO", "gzip test");
        let data = encode_events(vec![event], ParquetCompression::Gzip).expect("encode failed");
        let batch = read_parquet(&data);
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_parquet_compression_uncompressed() {
        let event = create_log_event("INFO", "uncompressed test");
        let data =
            encode_events(vec![event], ParquetCompression::Uncompressed).expect("encode failed");
        let batch = read_parquet(&data);
        assert_eq!(batch.num_rows(), 1);
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

    #[test]
    fn test_parquet_with_sparse_otlp_data() {
        let mut log = OtelLog::default();
        log.insert("severity_text", Value::from("ERROR".to_string()));
        let event = Event::Log(log);

        let data = encode_events(vec![event], ParquetCompression::Zstd).expect("encode failed");
        let batch = read_parquet(&data);
        assert_eq!(batch.num_rows(), 1);

        let body_col = batch.column_by_name("body").expect("body missing");
        assert!(body_col.is_null(0), "body should be null for sparse event");
    }
}
