use bytes::{BufMut, BytesMut};
use sol_config_macros::configurable_component;
use sol_core::{config::DataType, event::Event, schema};
use tokio_util::codec::Encoder;

use crate::MetricTagValues;

/// Config used to build a `JsonSerializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct JsonSerializerConfig {
    /// Controls how metric tag values are encoded.
    ///
    /// When set to `single`, only the last non-bare value of tags are displayed with the
    /// metric. When set to `full`, all metric tags are exposed as separate assignments.
    #[serde(default, skip_serializing_if = "sol_core::serde::is_default")]
    pub metric_tag_values: MetricTagValues,

    /// Options for the JsonSerializer.
    #[serde(default, rename = "json")]
    pub options: JsonSerializerOptions,
}

/// Options for the JsonSerializer.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct JsonSerializerOptions {
    /// Whether to use pretty JSON formatting.
    #[serde(default)]
    pub pretty: bool,
}

impl JsonSerializerConfig {
    /// Creates a new `JsonSerializerConfig`.
    pub const fn new(metric_tag_values: MetricTagValues, options: JsonSerializerOptions) -> Self {
        Self {
            metric_tag_values,
            options,
        }
    }

    /// Build the `JsonSerializer` from this configuration.
    pub fn build(&self) -> JsonSerializer {
        JsonSerializer::new(self.metric_tag_values, self.options.clone())
    }

    /// The data type of events that are accepted by `JsonSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::all_bits()
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        // While technically we support `Value` variants that can't be losslessly serialized to
        // JSON, we don't want to enforce that limitation to users yet.
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the JSON format.
#[derive(Debug, Clone)]
pub struct JsonSerializer {
    metric_tag_values: MetricTagValues,
    options: JsonSerializerOptions,
}

impl JsonSerializer {
    /// Creates a new `JsonSerializer`.
    pub const fn new(metric_tag_values: MetricTagValues, options: JsonSerializerOptions) -> Self {
        Self {
            metric_tag_values,
            options,
        }
    }

    /// Encode event and represent it as JSON value.
    pub fn to_json_value(&self, event: Event) -> Result<serde_json::Value, sol_common::Error> {
        match event {
            Event::Log(log) => serde_json::to_value(&log),
            Event::Metric(metric) => serde_json::to_value(&metric),
            Event::Trace(trace) => serde_json::to_value(&trace),
        }
        .map_err(|e| e.to_string().into())
    }
}

impl Encoder<Event> for JsonSerializer {
    type Error = sol_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let writer = buffer.writer();
        if self.options.pretty {
            match event {
                Event::Log(log) => serde_json::to_writer_pretty(writer, &log),
                Event::Metric(mut metric) => {
                    if self.metric_tag_values == MetricTagValues::Single {
                        metric.reduce_tags_to_single();
                    }
                    serde_json::to_writer_pretty(writer, &metric)
                }
                Event::Trace(trace) => serde_json::to_writer_pretty(writer, &trace),
            }
        } else {
            match event {
                Event::Log(log) => serde_json::to_writer(writer, &log),
                Event::Metric(mut metric) => {
                    if self.metric_tag_values == MetricTagValues::Single {
                        metric.reduce_tags_to_single();
                    }
                    serde_json::to_writer(writer, &metric)
                }
                Event::Trace(trace) => serde_json::to_writer(writer, &trace),
            }
        }
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use chrono::{TimeZone, Timelike, Utc};
    use sol_core::event::{MetricKind, ObjectMap, OtelAttributes, OtelLog, OtelMetric, Value};
    use sol_core::otel_tags;
    use vrl::btreemap;

    use super::*;

    #[test]
    fn serialize_json_log() {
        let event = Event::Log(OtelLog::from(btreemap! {
            "x" => Value::from("23"),
            "z" => Value::from(25),
            "a" => Value::from("0"),
        } as ObjectMap));
        let bytes = serialize(JsonSerializerConfig::default(), event);
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let attrs = v["attributes"].as_array().expect("attributes array");
        let find_attr = |key: &str| -> &serde_json::Value {
            &attrs
                .iter()
                .find(|a| a["key"] == key)
                .unwrap_or_else(|| panic!("key {key} not found"))["value"]
        };
        assert_eq!(find_attr("a")["stringValue"], "0");
        assert_eq!(find_attr("x")["stringValue"], "23");
        assert_eq!(find_attr("z")["intValue"], "25");
    }

    #[test]
    fn serialize_json_metric_counter() {
        let event = Event::Metric(
            OtelMetric::new_counter("foos", MetricKind::Incremental, 100.0)
                .with_namespace(Some("vector"))
                .with_tags(Some(otel_tags!(
                    "key2" => "value2",
                    "key1" => "value1",
                    "Key3" => "Value3",
                )))
                .with_timestamp(Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                        .single()
                        .and_then(|t| t.with_nanosecond(11))
                        .expect("invalid timestamp"),
                )),
        );

        let bytes = serialize(JsonSerializerConfig::default(), event);

        assert_eq!(
            bytes,
            r#"{"name":"foos","sum":{"dataPoints":[{"asDouble":100.0,"attributes":[{"key":"Key3","value":{"stringValue":"Value3"}},{"key":"key1","value":{"stringValue":"value1"}},{"key":"key2","value":{"stringValue":"value2"}}],"timeUnixNano":"1542182950000000011"}],"aggregationTemporality":1,"isMonotonic":true},"resource":{"attributes":[{"key":"metric.namespace","value":{"stringValue":"vector"}}]}}"#
        );
    }

    #[test]
    fn serialize_equals_to_json_value() {
        let event = Event::Log(OtelLog::from(btreemap! {
            "foo" => Value::from("bar")
        } as ObjectMap));
        let mut serializer = JsonSerializerConfig::default().build();
        let mut bytes = BytesMut::new();

        serializer.encode(event.clone(), &mut bytes).unwrap();

        let json = serializer.to_json_value(event).unwrap();

        assert_eq!(bytes.freeze(), serde_json::to_string(&json).unwrap());
    }

    #[test]
    fn serialize_metric_tags_full() {
        let bytes = serialize(
            JsonSerializerConfig {
                metric_tag_values: MetricTagValues::Full,
                options: JsonSerializerOptions::default(),
            },
            metric2(),
        );

        assert_eq!(
            bytes,
            r#"{"name":"counter","sum":{"dataPoints":[{"asDouble":1.0,"attributes":[{"key":"a","value":{"arrayValue":{"values":[{"stringValue":"first"},{},{"stringValue":"second"}]}}}]}],"aggregationTemporality":1,"isMonotonic":true}}"#
        );
    }

    #[test]
    fn serialize_metric_tags_single() {
        let bytes = serialize(
            JsonSerializerConfig {
                metric_tag_values: MetricTagValues::Single,
                options: JsonSerializerOptions::default(),
            },
            metric2(),
        );

        assert_eq!(
            bytes,
            r#"{"name":"counter","sum":{"dataPoints":[{"asDouble":1.0,"attributes":[{"key":"a","value":{"stringValue":"second"}}]}],"aggregationTemporality":1,"isMonotonic":true}}"#
        );
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

    fn serialize(config: JsonSerializerConfig, input: Event) -> Bytes {
        let mut buffer = BytesMut::new();
        config.build().encode(input, &mut buffer).unwrap();
        buffer.freeze()
    }

    mod pretty_json {
        use bytes::{Bytes, BytesMut};
        use chrono::{TimeZone, Timelike, Utc};
        use sol_core::event::{MetricKind, ObjectMap, OtelAttributes, OtelLog, OtelMetric, Value};
        use sol_core::otel_tags;
        use vrl::btreemap;

        use super::*;

        fn get_pretty_json_config() -> JsonSerializerConfig {
            JsonSerializerConfig {
                options: JsonSerializerOptions { pretty: true },
                ..Default::default()
            }
        }

        #[test]
        fn serialize_json_log() {
            let event = Event::Log(OtelLog::from(
                btreemap! {"x" => Value::from("23"),"z" => Value::from(25),"a" => Value::from("0"),}
                    as ObjectMap,
            ));
            let bytes = serialize(get_pretty_json_config(), event);
            let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let attrs = v["attributes"].as_array().expect("attributes array");
            let find_attr = |key: &str| -> &serde_json::Value {
                &attrs
                    .iter()
                    .find(|a| a["key"] == key)
                    .unwrap_or_else(|| panic!("key {key} not found"))["value"]
            };
            assert_eq!(find_attr("a")["stringValue"], "0");
            assert_eq!(find_attr("x")["stringValue"], "23");
            assert_eq!(find_attr("z")["intValue"], "25");
        }
        #[test]
        fn serialize_json_metric_counter() {
            let event = Event::Metric(
                OtelMetric::new_counter("foos", MetricKind::Incremental, 100.0)
                    .with_namespace(Some("vector"))
                    .with_tags(Some(
                        otel_tags!("key2" => "value2","key1" => "value1","Key3" => "Value3",),
                    ))
                    .with_timestamp(Some(
                        Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                            .single()
                            .and_then(|t| t.with_nanosecond(11))
                            .expect("invalid timestamp"),
                    )),
            );
            let bytes = serialize(get_pretty_json_config(), event);
            assert_eq!(
                bytes,
                r#"{
  "name": "foos",
  "sum": {
    "dataPoints": [
      {
        "asDouble": 100.0,
        "attributes": [
          {
            "key": "Key3",
            "value": {
              "stringValue": "Value3"
            }
          },
          {
            "key": "key1",
            "value": {
              "stringValue": "value1"
            }
          },
          {
            "key": "key2",
            "value": {
              "stringValue": "value2"
            }
          }
        ],
        "timeUnixNano": "1542182950000000011"
      }
    ],
    "aggregationTemporality": 1,
    "isMonotonic": true
  },
  "resource": {
    "attributes": [
      {
        "key": "metric.namespace",
        "value": {
          "stringValue": "vector"
        }
      }
    ]
  }
}"#
            );
        }
        #[test]
        fn serialize_equals_to_json_value() {
            let event = Event::Log(OtelLog::from(
                btreemap! {"foo" => Value::from("bar")} as ObjectMap
            ));
            let mut serializer = get_pretty_json_config().build();
            let mut bytes = BytesMut::new();
            serializer.encode(event.clone(), &mut bytes).unwrap();
            let json = serializer.to_json_value(event).unwrap();
            assert_eq!(bytes.freeze(), serde_json::to_string_pretty(&json).unwrap());
        }
        #[test]
        fn serialize_metric_tags_full() {
            let bytes = serialize(
                JsonSerializerConfig {
                    metric_tag_values: MetricTagValues::Full,
                    options: JsonSerializerOptions { pretty: true },
                },
                metric2(),
            );
            assert_eq!(
                bytes,
                r#"{
  "name": "counter",
  "sum": {
    "dataPoints": [
      {
        "asDouble": 1.0,
        "attributes": [
          {
            "key": "a",
            "value": {
              "arrayValue": {
                "values": [
                  {
                    "stringValue": "first"
                  },
                  {},
                  {
                    "stringValue": "second"
                  }
                ]
              }
            }
          }
        ]
      }
    ],
    "aggregationTemporality": 1,
    "isMonotonic": true
  }
}"#
            );
        }
        #[test]
        fn serialize_metric_tags_single() {
            let bytes = serialize(
                JsonSerializerConfig {
                    metric_tag_values: MetricTagValues::Single,
                    options: JsonSerializerOptions { pretty: true },
                },
                metric2(),
            );
            assert_eq!(
                bytes,
                r#"{
  "name": "counter",
  "sum": {
    "dataPoints": [
      {
        "asDouble": 1.0,
        "attributes": [
          {
            "key": "a",
            "value": {
              "stringValue": "second"
            }
          }
        ]
      }
    ],
    "aggregationTemporality": 1,
    "isMonotonic": true
  }
}"#
            );
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
                OtelMetric::new_counter("counter", MetricKind::Incremental, 1.0)
                    .with_tags(Some(tags)),
            )
        }
        fn serialize(config: JsonSerializerConfig, input: Event) -> Bytes {
            let mut buffer = BytesMut::new();
            config.build().encode(input, &mut buffer).unwrap();
            buffer.freeze()
        }
    }
}
