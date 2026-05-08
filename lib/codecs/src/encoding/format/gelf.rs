use crate::{VALID_FIELD_REGEX, encoding::GelfChunker, gelf::GELF_TARGET_PATHS, gelf_fields::*};
use bytes::{BufMut, BytesMut};
use lookup::event_path;
use lookup::{OwnedTargetPath, owned_value_path};
use ordered_float::NotNan;
use snafu::Snafu;
use sol_config_macros::configurable_component;
use sol_core::{
    config::DataType,
    event::{Event, KeyString, OtelLog, Value},
    schema,
};
use tokio_util::codec::Encoder;

/// Config used to build a `GelfSerializer`.
#[configurable_component]
#[derive(Debug, Clone)]
pub struct GelfSerializerOptions {
    /// Maximum size for each GELF chunked datagram (including 12-byte header).
    /// Chunking starts when datagrams exceed this size.
    /// For Graylog target, keep at or below 8192 bytes; for Vector target (`gelf` decoding with `chunked_gelf` framing), up to 65,500 bytes is recommended.
    #[configurable(validation(range(min = 13)))]
    #[serde(default = "default_max_chunk_size")]
    pub max_chunk_size: usize,
}

const fn default_max_chunk_size() -> usize {
    8192
}

impl Default for GelfSerializerOptions {
    fn default() -> Self {
        Self {
            max_chunk_size: default_max_chunk_size(),
        }
    }
}

/// On GELF encoding behavior:
///   Graylog has a relaxed parsing. They are much more lenient than the spec would
///   suggest. We've elected to take a more strict approach to maintain backwards compatibility
///   in the event that we need to change the behavior to be more relaxed, so that prior versions
///   of vector will still work.
///   The exception is that if 'Additional fields' are found to be missing an underscore prefix and
///   are otherwise valid field names, we prepend the underscore.
///
/// Errors that can occur during GELF serialization.
#[derive(Debug, Snafu)]
pub enum GelfSerializerError {
    #[snafu(display(r#"OtelLog does not contain required field: "{}""#, field))]
    MissingField { field: KeyString },
    #[snafu(display(
        r#"OtelLog contains field with invalid name not matching pattern '{}': "{}""#,
        pattern,
        field,
    ))]
    InvalidField { field: KeyString, pattern: String },
    #[snafu(display(
        r#"OtelLog contains a value with an invalid type. field = "{}" type = "{}" expected type = "{}""#,
        field,
        actual_type,
        expected_type
    ))]
    InvalidValueType {
        field: String,
        actual_type: String,
        expected_type: String,
    },
}

/// Config used to build a `GelfSerializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct GelfSerializerConfig {
    /// The GELF Serializer Options.
    #[serde(default, rename = "gelf")]
    pub options: GelfSerializerOptions,
}

impl GelfSerializerConfig {
    /// Creates a new `GelfSerializerConfig`.
    pub const fn new(options: GelfSerializerOptions) -> Self {
        Self { options }
    }

    /// Build the `GelfSerializer` from this configuration.
    pub fn build(&self) -> GelfSerializer {
        GelfSerializer::new(self.options.clone())
    }

    /// The data type of events that are accepted by `GelfSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        // While technically we support `Value` variants that can't be losslessly serialized to
        // JSON, we don't want to enforce that limitation to users yet.
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the GELF format.
/// Spec: <https://docs.graylog.org/docs/gelf>
#[derive(Debug, Clone)]
pub struct GelfSerializer {
    options: GelfSerializerOptions,
}

impl GelfSerializer {
    /// Creates a new `GelfSerializer`.
    pub fn new(options: GelfSerializerOptions) -> Self {
        GelfSerializer { options }
    }

    /// Encode event and represent it as JSON value.
    pub fn to_json_value(&self, event: Event) -> Result<serde_json::Value, sol_common::Error> {
        let mut log = event.into_log();
        to_gelf_event(&mut log)?;
        serde_json::to_value(log.as_map().unwrap_or_default()).map_err(|e| e.to_string().into())
    }

    /// Instantiates the GELF chunking configuration.
    pub fn chunker(&self) -> GelfChunker {
        GelfChunker {
            max_chunk_size: self.options.max_chunk_size,
        }
    }
}

impl Encoder<Event> for GelfSerializer {
    type Error = sol_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let mut log = event.into_log();
        to_gelf_event(&mut log)?;
        let writer = buffer.writer();
        let map = log.as_map().unwrap_or_default();
        serde_json::to_writer(writer, &map)?;
        Ok(())
    }
}

/// Returns Error for invalid type.
fn err_invalid_type(field: &str, expected_type: &str, actual_type: &str) -> sol_common::Result<()> {
    InvalidValueTypeSnafu {
        field,
        actual_type,
        expected_type,
    }
    .fail()
    .map_err(|e| e.to_string().into())
}

/// Validate and coerce an OtelLog into valid GELF format.
fn to_gelf_event(log: &mut OtelLog) -> sol_common::Result<()> {
    fn err_missing_field(field: &str) -> sol_common::Result<()> {
        MissingFieldSnafu { field }
            .fail()
            .map_err(|e| e.to_string().into())
    }

    // Required fields
    if log.get(&GELF_TARGET_PATHS.version).is_none() {
        log.insert(&GELF_TARGET_PATHS.version, GELF_VERSION);
    }
    if log.get(&GELF_TARGET_PATHS.host).is_none() {
        err_missing_field(HOST)?;
    }
    if log.get(&GELF_TARGET_PATHS.short_message).is_none() {
        let body_path = OwnedTargetPath::event(owned_value_path!("body"));
        if log.get(&body_path).is_some() {
            log.rename_key(&body_path, &GELF_TARGET_PATHS.short_message);
        } else {
            err_missing_field(SHORT_MESSAGE)?;
        }
    }

    // Validate field types and collect mutations
    let fields = log.convert_to_fields();
    let mut timestamp_replacement = None;
    let mut missing_prefix = Vec::new();

    for (field, value) in &fields {
        match field.as_ref() {
            VERSION | HOST | SHORT_MESSAGE | FULL_MESSAGE | FACILITY | FILE => {
                if !value.is_bytes() {
                    err_invalid_type(field, "UTF-8 string", value.kind_str())?;
                }
            }
            TIMESTAMP => {
                let parsed_ts = match value {
                    Value::Timestamp(ts) => Some(*ts),
                    Value::Integer(_) => None,
                    Value::Bytes(b) => {
                        let s = std::str::from_utf8(b).unwrap_or("");
                        chrono::DateTime::parse_from_rfc3339(s)
                            .ok()
                            .map(|dt| dt.with_timezone(&chrono::Utc))
                    }
                    _ => {
                        err_invalid_type(field, "timestamp or integer", value.kind_str())?;
                        None
                    }
                };
                if let Some(ts) = parsed_ts {
                    let ts_millis = ts.timestamp_millis();
                    timestamp_replacement = Some(if ts_millis % 1000 != 0 {
                        #[expect(
                            clippy::cast_precision_loss,
                            reason = "timestamp millis; precise for practical values"
                        )]
                        Value::Float(
                            NotNan::new(ts_millis as f64 / 1000.0)
                                .expect("i64 -> f64 produced NaN"),
                        )
                    } else {
                        Value::Integer(ts.timestamp())
                    });
                }
            }
            LEVEL => {
                if !value.is_integer() {
                    err_invalid_type(field, "integer", value.kind_str())?;
                }
            }
            LINE => {
                if !(value.is_float() || value.is_integer()) {
                    err_invalid_type(field, "number", value.kind_str())?;
                }
            }
            _ => {
                if !VALID_FIELD_REGEX.is_match(field) {
                    return InvalidFieldSnafu {
                        field: field.clone(),
                        pattern: VALID_FIELD_REGEX.to_string(),
                    }
                    .fail()
                    .map_err(|e| e.to_string().into());
                }
                if !(value.is_integer() || value.is_float() || value.is_bytes()) {
                    err_invalid_type(field, "string or number", value.kind_str())?;
                }
                if !field.is_empty() && !field.starts_with('_') {
                    missing_prefix.push(field.to_string());
                }
            }
        }
    }

    // Apply mutations
    if let Some(ts_val) = timestamp_replacement
        && let Ok(path) = vrl::path::parse_target_path(TIMESTAMP)
    {
        log.insert(&path, ts_val);
    }
    for field in missing_prefix {
        log.rename_key(
            event_path!(field.as_str()),
            event_path!(format!("_{}", &field).as_str()),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use sol_core::event::{Event, EventMetadata, OtelLog};
    use vrl::{
        btreemap,
        value::{ObjectMap, Value},
    };

    use super::*;
    use crate::encoding::SerializerConfig;

    fn do_serialize(expect_success: bool, event_fields: ObjectMap) -> Option<serde_json::Value> {
        let config = GelfSerializerConfig::new(GelfSerializerOptions::default());
        let mut serializer = config.build();
        let event: Event = OtelLog::from_map(event_fields, EventMetadata::default()).into();
        let mut buffer = BytesMut::new();

        if expect_success {
            let result = serializer.encode(event, &mut buffer);
            assert!(result.is_ok(), "encode failed: {:?}", result.err());
            let buffer_str = std::str::from_utf8(&buffer).unwrap();
            let result = serde_json::from_str(buffer_str);
            assert!(result.is_ok());
            Some(result.unwrap())
        } else {
            assert!(serializer.encode(event, &mut buffer).is_err());
            None
        }
    }

    #[test]
    fn gelf_serde_json_to_value_supported_success() {
        let serializer = SerializerConfig::Gelf(Default::default()).build().unwrap();

        let event_fields = btreemap! {
            VERSION => "1.1",
            HOST => "example.org",
            SHORT_MESSAGE => "Some message",
        };

        let log_event: Event = OtelLog::from_map(event_fields, EventMetadata::default()).into();
        assert!(serializer.supports_json());
        assert!(serializer.to_json_value(log_event).is_ok());
    }

    #[test]
    fn gelf_serde_json_to_value_supported_failure_to_encode() {
        let serializer = SerializerConfig::Gelf(Default::default()).build().unwrap();
        let event_fields = btreemap! {};
        let log_event: Event = OtelLog::from_map(event_fields, EventMetadata::default()).into();
        assert!(serializer.supports_json());
        assert!(serializer.to_json_value(log_event).is_err());
    }

    #[test]
    fn gelf_serializing_valid() {
        let event_fields = btreemap! {
            VERSION => "1.1",
            HOST => "example.org",
            SHORT_MESSAGE => "Some message",
            FULL_MESSAGE => "Even more message",
            FACILITY => "",
            FILE => "/tmp/foobar",
            LINE => Value::Float(ordered_float::NotNan::new(1.5).unwrap()),
            LEVEL => 5,
        };

        let jsn = do_serialize(true, event_fields).unwrap();

        assert_eq!(jsn.get(VERSION).unwrap(), "1.1");
        assert_eq!(jsn.get(HOST).unwrap(), "example.org");
        assert_eq!(jsn.get(SHORT_MESSAGE).unwrap(), "Some message");
    }

    #[test]
    fn gelf_serializing_coerced() {
        // no underscore
        {
            let event_fields = btreemap! {
                VERSION => "1.1",
                HOST => "example.org",
                SHORT_MESSAGE => "Some message",
                "noUnderScore" => 0,
            };

            let jsn = do_serialize(true, event_fields).unwrap();
            assert_eq!(jsn.get("_noUnderScore").unwrap(), 0);
        }

        // "message" => SHORT_MESSAGE
        {
            let event_fields = btreemap! {
                VERSION => "1.1",
                HOST => "example.org",
                "body" => "Some message",
            };

            let jsn = do_serialize(true, event_fields).unwrap();
            assert_eq!(jsn.get(SHORT_MESSAGE).unwrap(), "Some message");
        }
    }

    #[test]
    fn gelf_serializing_timestamp() {
        // floating point in case of sub second timestamp
        {
            let naive_dt =
                NaiveDateTime::parse_from_str("1970-01-01 00:00:00.1", "%Y-%m-%d %H:%M:%S%.f");
            let dt = naive_dt.unwrap().and_utc();

            let event_fields = btreemap! {
                VERSION => "1.1",
                SHORT_MESSAGE => "Some message",
                HOST => "example.org",
                TIMESTAMP => dt,
            };

            let jsn = do_serialize(true, event_fields).unwrap();
            assert!(jsn.get(TIMESTAMP).unwrap().is_f64());
            assert_eq!(jsn.get(TIMESTAMP).unwrap().as_f64().unwrap(), 0.1,);
        }

        // integer in case of no sub second timestamp
        {
            let naive_dt =
                NaiveDateTime::parse_from_str("1970-01-01 00:00:00.0", "%Y-%m-%d %H:%M:%S%.f");
            let dt = naive_dt.unwrap().and_utc();

            let event_fields = btreemap! {
                VERSION => "1.1",
                SHORT_MESSAGE => "Some message",
                HOST => "example.org",
                TIMESTAMP => dt,
            };

            let jsn = do_serialize(true, event_fields).unwrap();
            assert!(jsn.get(TIMESTAMP).unwrap().is_i64());
            assert_eq!(jsn.get(TIMESTAMP).unwrap().as_i64().unwrap(), 0);
        }
    }

    #[test]
    fn gelf_serializing_invalid_error() {
        // no host
        {
            let event_fields = btreemap! {
                VERSION => "1.1",
                SHORT_MESSAGE => "Some message",
            };
            do_serialize(false, event_fields);
        }
        // no message
        {
            let event_fields = btreemap! {
                HOST => "example.org",
                VERSION => "1.1",
            };
            do_serialize(false, event_fields);
        }
        // expected string
        {
            let event_fields = btreemap! {
                HOST => "example.org",
                VERSION => "1.1",
                SHORT_MESSAGE => 0,
            };
            do_serialize(false, event_fields);
        }
        // expected integer
        {
            let event_fields = btreemap! {
                HOST => "example.org",
                VERSION => "1.1",
                SHORT_MESSAGE => "Some message",
                LEVEL => "1",
            };
            do_serialize(false, event_fields);
        }
        // expected float
        {
            let event_fields = btreemap! {
                HOST => "example.org",
                VERSION => "1.1",
                SHORT_MESSAGE => "Some message",
                LINE => "1.2",
            };
            do_serialize(false, event_fields);
        }
        // invalid field name
        {
            let event_fields = btreemap! {
                HOST => "example.org",
                VERSION => "1.1",
                SHORT_MESSAGE => "Some message",
                "invalid%field" => "foo",
            };
            do_serialize(false, event_fields);
        }
        // invalid additional value type - bool
        {
            let event_fields = btreemap! {
                HOST => "example.org",
                VERSION => "1.1",
                SHORT_MESSAGE => "Some message",
                "_foobar" => false,
            };
            do_serialize(false, event_fields);
        }
        // invalid additional value type - null
        {
            let event_fields = btreemap! {
                HOST => "example.org",
                VERSION => "1.1",
                SHORT_MESSAGE => "Some message",
                "_foobar" => serde_json::Value::Null,
            };
            do_serialize(false, event_fields);
        }
    }
}
