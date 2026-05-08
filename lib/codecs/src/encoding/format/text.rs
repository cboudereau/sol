use bytes::{BufMut, BytesMut};
use sol_config_macros::configurable_component;
use sol_core::{config::DataType, event::Event, schema};
use tokio_util::codec::Encoder;

use crate::{MetricTagValues, encoding::format::common::get_serializer_schema_requirement};

/// Config used to build a `TextSerializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct TextSerializerConfig {
    /// Controls how metric tag values are encoded.
    ///
    /// When set to `single`, only the last non-bare value of tags are displayed with the
    /// metric.  When set to `full`, all metric tags are exposed as separate assignments.
    #[serde(default, skip_serializing_if = "sol_core::serde::is_default")]
    pub metric_tag_values: MetricTagValues,
}

impl TextSerializerConfig {
    /// Creates a new `TextSerializerConfig`.
    pub const fn new(metric_tag_values: MetricTagValues) -> Self {
        Self { metric_tag_values }
    }

    /// Build the `TextSerializer` from this configuration.
    pub const fn build(&self) -> TextSerializer {
        TextSerializer::new(self.metric_tag_values)
    }

    /// The data type of events that are accepted by `TextSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::Log | DataType::Metric
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        get_serializer_schema_requirement()
    }
}

/// Serializer that converts a log to bytes by extracting the message key, or converts a metric
/// to bytes by calling its `Display` implementation.
///
/// This serializer exists to emulate the behavior of the `StandardEncoding::Text` for backwards
/// compatibility, until it is phased out completely.
#[derive(Debug, Clone)]
pub struct TextSerializer {
    // Kept for API compat; OtelMetric::Display uses iter_single() which
    // already filters to single values.
    _metric_tag_values: MetricTagValues,
}

impl TextSerializer {
    /// Creates a new `TextSerializer`.
    pub const fn new(metric_tag_values: MetricTagValues) -> Self {
        Self {
            _metric_tag_values: metric_tag_values,
        }
    }
}

impl Encoder<Event> for TextSerializer {
    type Error = sol_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match event {
            Event::Log(ref otel_log) => {
                let s = otel_log.body_string();
                if !s.is_empty() {
                    buffer.put(s.as_bytes());
                }
            }
            Event::Metric(metric) => {
                let bytes = metric.to_string();
                buffer.put(bytes.as_ref());
            }
            Event::Trace(_) => {}
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use sol_core::event::{MetricKind, OtelAttributes, OtelLog, OtelMetric};

    use super::*;

    #[test]
    fn serialize_log() {
        let buffer = serialize(
            TextSerializerConfig::default(),
            Event::Log(OtelLog::from_str_legacy("foo")),
        );
        assert_eq!(buffer, Bytes::from("foo"));
    }

    #[test]
    fn serialize_metric() {
        let buffer = serialize(
            TextSerializerConfig::default(),
            Event::Metric(OtelMetric::new_set_from_values(
                "users",
                MetricKind::Incremental,
                vec!["bob"],
            )),
        );
        assert_eq!(buffer, Bytes::from("users{} + set: 1 values"));
    }

    #[test]
    fn serialize_metric_tags_single() {
        let buffer = serialize(
            TextSerializerConfig {
                metric_tag_values: MetricTagValues::Single,
            },
            metric2(),
        );
        assert_eq!(buffer, Bytes::from(r#"counter{a="second"} + counter: 1"#));
    }

    fn metric2() -> Event {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, any_value};
        let array_tag = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(any_value::Value::StringValue("first".into())),
                    },
                    AnyValue { value: None },
                    AnyValue {
                        value: Some(any_value::Value::StringValue("second".into())),
                    },
                ],
            })),
        };
        let mut tags = OtelAttributes::default();
        tags.insert("a".into(), array_tag);
        Event::Metric(
            OtelMetric::new_counter("counter", MetricKind::Incremental, 1.0).with_tags(Some(tags)),
        )
    }

    #[test]
    fn serialize_otel_log() {
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use sol_core::event::OtelLog;

        let event = Event::Log(OtelLog::new(
            opentelemetry_proto::tonic::logs::v1::LogRecord {
                body: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "hello otel".into(),
                        ),
                    ),
                }),
                ..Default::default()
            },
        ));
        let buffer = serialize(TextSerializerConfig::default(), event);
        assert_eq!(buffer, Bytes::from("hello otel"));
    }

    fn serialize(config: TextSerializerConfig, input: Event) -> Bytes {
        let mut buffer = BytesMut::new();
        config.build().encode(input, &mut buffer).unwrap();
        buffer.freeze()
    }
}
