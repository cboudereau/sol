use std::borrow::Cow;

use bytes::Bytes;
use chrono::DateTime;
use derivative::Derivative;
use influxdb_line_protocol::{FieldValue, ParsedLine};
use smallvec::SmallVec;
use sol_config::configurable_component;
use sol_core::{
    config::DataType,
    event::{Event, OtelAttributes, OtelMetric},
    schema,
};
use vrl::value::{Kind, kind::Collection};

use super::Deserializer;
use crate::decoding::format::default_lossy;

/// Config used to build a `InfluxdbDeserializer`.
/// - [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v1/write_protocols/line_protocol_tutorial/):
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct InfluxdbDeserializerConfig {
    /// Influxdb-specific decoding options.
    #[serde(default, skip_serializing_if = "sol_core::serde::is_default")]
    pub influxdb: InfluxdbDeserializerOptions,
}

impl InfluxdbDeserializerConfig {
    /// new constructs a new InfluxdbDeserializerConfig
    pub fn new(options: InfluxdbDeserializerOptions) -> Self {
        Self { influxdb: options }
    }

    /// build constructs a new InfluxdbDeserializer
    pub fn build(&self) -> InfluxdbDeserializer {
        Into::<InfluxdbDeserializer>::into(self)
    }

    /// The output type produced by the deserializer.
    pub fn output_type(&self) -> DataType {
        DataType::Metric
    }

    /// The schema produced by the deserializer.
    pub fn schema_definition(&self) -> schema::Definition {
        schema::Definition::new_with_default_metadata(Kind::object(Collection::empty()))
    }
}

/// Influxdb-specific decoding options.
#[configurable_component]
#[derive(Debug, Clone, PartialEq, Eq, Derivative)]
#[derivative(Default)]
pub struct InfluxdbDeserializerOptions {
    /// Determines whether to replace invalid UTF-8 sequences instead of failing.
    ///
    /// When true, invalid UTF-8 sequences are replaced with the [`U+FFFD REPLACEMENT CHARACTER`][U+FFFD].
    ///
    /// [U+FFFD]: https://en.wikipedia.org/wiki/Specials_(Unicode_block)#Replacement_character
    #[serde(
        default = "default_lossy",
        skip_serializing_if = "sol_core::serde::is_default"
    )]
    #[derivative(Default(value = "default_lossy()"))]
    pub lossy: bool,
}

/// Deserializer for the influxdb line protocol
#[derive(Debug, Clone, Derivative)]
#[derivative(Default)]
pub struct InfluxdbDeserializer {
    #[derivative(Default(value = "default_lossy()"))]
    lossy: bool,
}

impl InfluxdbDeserializer {
    /// new constructs a new InfluxdbDeserializer
    pub fn new(lossy: bool) -> Self {
        Self { lossy }
    }
}

impl Deserializer for InfluxdbDeserializer {
    fn parse(&self, bytes: Bytes) -> sol_common::Result<SmallVec<[Event; 1]>> {
        let line: Cow<str> = match self.lossy {
            true => String::from_utf8_lossy(&bytes),
            false => Cow::from(std::str::from_utf8(&bytes)?),
        };
        let parsed_line = influxdb_line_protocol::parse_lines(&line);

        let res = parsed_line
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .flat_map(|line| {
                let ParsedLine {
                    series,
                    field_set,
                    timestamp,
                } = line;

                field_set
                    .iter()
                    .filter_map(|f| {
                        let measurement = series.measurement.clone();
                        let tags = series.tag_set.as_ref();
                        #[expect(
                            clippy::cast_precision_loss,
                            reason = "InfluxDB field values; precise for |v| <= 2^53"
                        )]
                        let val = match f.1 {
                            FieldValue::I64(v) => v as f64,
                            FieldValue::U64(v) => v as f64,
                            FieldValue::F64(v) => v,
                            FieldValue::Boolean(v) => {
                                if v {
                                    1.0
                                } else {
                                    0.0
                                }
                            }
                            FieldValue::String(_) => return None, // String values cannot be modelled in our schema
                        };
                        Some(Event::Metric(
                            OtelMetric::new_gauge(format!("{0}_{1}", measurement, f.0), val)
                                .with_tags(tags.map(|ts| {
                                    OtelAttributes::from_iter(
                                        ts.iter().map(|t| (t.0.to_string(), t.1.to_string())),
                                    )
                                }))
                                .with_timestamp(timestamp.map(DateTime::from_timestamp_nanos)),
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        Ok(res)
    }
}

impl From<&InfluxdbDeserializerConfig> for InfluxdbDeserializer {
    fn from(config: &InfluxdbDeserializerConfig) -> Self {
        Self {
            lossy: config.influxdb.lossy,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use sol_core::event::{MetricKind, MetricView};

    use crate::decoding::format::{Deserializer, InfluxdbDeserializer};

    #[test]
    fn deserialize_success() {
        let deser = InfluxdbDeserializer::new(true);
        let now = chrono::Utc::now();
        let now_timestamp_nanos = now.timestamp_nanos_opt().unwrap();
        let buffer = Bytes::from(format!(
            "cpu,host=A,region=west usage_system=64i,usage_user=10i {now_timestamp_nanos}"
        ));
        let events = deser.parse(buffer).unwrap();
        assert_eq!(events.len(), 2);

        let m0 = events[0].as_metric();
        assert_eq!(m0.name(), "cpu_usage_system");
        assert_eq!(m0.kind(), MetricKind::Absolute);
        assert!(matches!(m0.view(), MetricView::Gauge { value } if value == 64.));
        assert_eq!(m0.tag_value("host"), Some("A".to_string()));
        assert_eq!(m0.tag_value("region"), Some("west".to_string()));
        assert!(m0.timestamp().is_some());

        let m1 = events[1].as_metric();
        assert_eq!(m1.name(), "cpu_usage_user");
        assert_eq!(m1.kind(), MetricKind::Absolute);
        assert!(matches!(m1.view(), MetricView::Gauge { value } if value == 10.));
        assert_eq!(m1.tag_value("host"), Some("A".to_string()));
        assert_eq!(m1.tag_value("region"), Some("west".to_string()));
        assert!(m1.timestamp().is_some());
    }

    #[test]
    fn deserialize_error() {
        let deser = InfluxdbDeserializer::new(true);
        let buffer = Bytes::from("some invalid string");
        assert!(deser.parse(buffer).is_err());
    }
}
