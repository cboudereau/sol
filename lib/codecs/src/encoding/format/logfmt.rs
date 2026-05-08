use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use sol_common::encode_logfmt;
use sol_core::{config::DataType, event::Event, schema};
use tokio_util::codec::Encoder;
use vrl::value::ObjectMap;

/// Config used to build a `LogfmtSerializer`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct LogfmtSerializerConfig;

impl LogfmtSerializerConfig {
    /// Creates a new `LogfmtSerializerConfig`.
    pub const fn new() -> Self {
        Self
    }

    /// Build the `LogfmtSerializer` from this configuration.
    pub const fn build(&self) -> LogfmtSerializer {
        LogfmtSerializer
    }

    /// The data type of events that are accepted by `LogfmtSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        // While technically we support `Value` variants that can't be losslessly serialized to
        // logfmt, we don't want to enforce that limitation to users yet.
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the logfmt format.
#[derive(Debug, Clone)]
pub struct LogfmtSerializer;

impl Encoder<Event> for LogfmtSerializer {
    type Error = sol_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let log = event.into_log();
        let map: ObjectMap = log.as_map().unwrap_or_default();
        let string = encode_logfmt::encode_map(&map)?;
        buffer.extend_from_slice(string.as_bytes());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use sol_core::event::{OtelLog, Value};
    use vrl::btreemap;
    use vrl::value::ObjectMap;

    use super::*;

    #[test]
    fn serialize_logfmt() {
        let event = Event::Log(OtelLog::from(btreemap! {
            "foo" => Value::from("bar")
        } as ObjectMap));
        let mut serializer = LogfmtSerializer;
        let mut bytes = BytesMut::new();

        serializer.encode(event, &mut bytes).unwrap();

        assert_eq!(bytes.freeze(), "foo=bar");
    }

    #[test]
    fn serialize_otel_log_logfmt() {
        use opentelemetry_proto::tonic::common::v1::{
            AnyValue, KeyValue, any_value::Value as Kind,
        };
        use sol_core::event::OtelLog;

        let event = Event::Log(OtelLog::new(
            opentelemetry_proto::tonic::logs::v1::LogRecord {
                body: Some(AnyValue {
                    value: Some(Kind::StringValue("hello".into())),
                }),
                attributes: vec![KeyValue {
                    key: "env".into(),
                    value: Some(AnyValue {
                        value: Some(Kind::StringValue("prod".into())),
                    }),
                }],
                ..Default::default()
            },
        ));
        let mut serializer = LogfmtSerializer;
        let mut bytes = BytesMut::new();

        serializer.encode(event, &mut bytes).unwrap();

        let output = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(output.contains("env=prod"));
        assert!(output.contains("body=hello"));
    }
}
