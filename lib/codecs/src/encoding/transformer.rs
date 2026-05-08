#![deny(missing_docs)]

use chrono::{DateTime, Utc};
use lookup::{PathPrefix, lookup_v2::ConfigValuePath};
use ordered_float::NotNan;
use serde::{Deserialize, Deserializer};
use sol_config::configurable_component;
use sol_core::{
    event::{Event, OtelLog},
    schema::meaning,
    serde::is_default,
};
use vrl::{path::OwnedValuePath, value::Value};

/// Transformations to prepare an event for serialization.
#[configurable_component(no_deser)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Transformer {
    /// List of fields that are included in the encoded event.
    #[serde(default, skip_serializing_if = "is_default")]
    only_fields: Option<Vec<ConfigValuePath>>,

    /// List of fields that are excluded from the encoded event.
    #[serde(default, skip_serializing_if = "is_default")]
    except_fields: Option<Vec<ConfigValuePath>>,

    /// Format used for timestamp fields.
    #[serde(default, skip_serializing_if = "is_default")]
    timestamp_format: Option<TimestampFormat>,
}

impl<'de> Deserialize<'de> for Transformer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TransformerInner {
            #[serde(default)]
            only_fields: Option<Vec<OwnedValuePath>>,
            #[serde(default)]
            except_fields: Option<Vec<OwnedValuePath>>,
            #[serde(default)]
            timestamp_format: Option<TimestampFormat>,
        }

        let inner: TransformerInner = Deserialize::deserialize(deserializer)?;
        Self::new(
            inner
                .only_fields
                .map(|v| v.iter().map(|p| ConfigValuePath(p.clone())).collect()),
            inner
                .except_fields
                .map(|v| v.iter().map(|p| ConfigValuePath(p.clone())).collect()),
            inner.timestamp_format,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl Transformer {
    /// Creates a new `Transformer`.
    ///
    /// Returns `Err` if `only_fields` and `except_fields` fail validation, i.e. are not mutually
    /// exclusive.
    pub fn new(
        only_fields: Option<Vec<ConfigValuePath>>,
        except_fields: Option<Vec<ConfigValuePath>>,
        timestamp_format: Option<TimestampFormat>,
    ) -> sol_common::Result<Self> {
        Self::validate_fields(only_fields.as_ref(), except_fields.as_ref())?;

        Ok(Self {
            only_fields,
            except_fields,
            timestamp_format,
        })
    }

    /// Get the `Transformer`'s `only_fields`.
    #[cfg(any(test, feature = "test"))]
    pub const fn only_fields(&self) -> &Option<Vec<ConfigValuePath>> {
        &self.only_fields
    }

    /// Get the `Transformer`'s `except_fields`.
    pub const fn except_fields(&self) -> &Option<Vec<ConfigValuePath>> {
        &self.except_fields
    }

    /// Get the `Transformer`'s `timestamp_format`.
    pub const fn timestamp_format(&self) -> &Option<TimestampFormat> {
        &self.timestamp_format
    }

    /// Check if `except_fields` and `only_fields` items are mutually exclusive.
    ///
    /// If an error is returned, the entire encoding configuration should be considered inoperable.
    fn validate_fields(
        only_fields: Option<&Vec<ConfigValuePath>>,
        except_fields: Option<&Vec<ConfigValuePath>>,
    ) -> sol_common::Result<()> {
        if let (Some(only_fields), Some(except_fields)) = (only_fields, except_fields)
            && except_fields
                .iter()
                .any(|f| only_fields.iter().any(|v| v == f))
        {
            return Err("`except_fields` and `only_fields` should be mutually exclusive.".into());
        }
        Ok(())
    }

    /// Prepare an event for serialization by the given transformation rules.
    pub fn transform(&self, event: &mut Event) {
        let has_rules = self.only_fields.is_some()
            || self.except_fields.is_some()
            || self.timestamp_format.is_some();

        if has_rules && let Event::Log(otel_log) = event {
            self.apply_except_fields_otel(otel_log);
            self.apply_only_fields_otel(otel_log);
            self.apply_timestamp_format_otel(otel_log);
        }
    }

    // OtelLog-native field transformation methods.

    fn apply_except_fields_otel(&self, log: &mut OtelLog) {
        if let Some(except_fields) = self.except_fields.as_ref() {
            for field in except_fields {
                let value_path = &field.0;
                let value = log.remove((PathPrefix::Event, value_path));

                let service_path = log
                    .metadata()
                    .schema_definition()
                    .meaning_path(meaning::SERVICE);
                if let (Some(v), Some(service_path)) = (value, service_path)
                    && service_path.path == *value_path
                {
                    log.metadata_mut()
                        .add_dropped_field(meaning::SERVICE.into(), v);
                }
            }
        }
    }

    fn apply_only_fields_otel(&self, log: &mut OtelLog) {
        if let Some(only_fields) = self.only_fields.as_ref() {
            let only_set: std::collections::HashSet<String> = only_fields
                .iter()
                .map(|f| String::from(f.clone()))
                .collect();

            // Preserve service meaning in dropped_fields before removal
            let service_path = log
                .metadata()
                .schema_definition()
                .meaning_path(meaning::SERVICE)
                .cloned();

            // Collect all current keys
            let keys_to_check: Vec<String> = log
                .keys()
                .map(|keys| keys.map(|k| k.to_string()).collect())
                .unwrap_or_default();

            for key in &keys_to_check {
                if !only_set.contains(key.as_str())
                    && let Ok(path) = vrl::path::parse_target_path(key)
                {
                    if let Some(service_path) = &service_path
                        && service_path.path.to_string() == *key
                        && let Some(v) = log.remove(&path)
                    {
                        log.metadata_mut()
                            .add_dropped_field(meaning::SERVICE.into(), v);
                        continue;
                    }
                    log.remove(&path);
                }
            }
        }
    }

    fn apply_timestamp_format_otel(&self, log: &mut OtelLog) {
        if let Some(timestamp_format) = self.timestamp_format.as_ref() {
            match timestamp_format {
                TimestampFormat::Unix => self.format_timestamps_otel(log, |ts| ts.timestamp()),
                TimestampFormat::UnixMs => {
                    self.format_timestamps_otel(log, |ts| ts.timestamp_millis())
                }
                TimestampFormat::UnixUs => {
                    self.format_timestamps_otel(log, |ts| ts.timestamp_micros())
                }
                TimestampFormat::UnixNs => self.format_timestamps_otel(log, |ts| {
                    ts.timestamp_nanos_opt().expect("Timestamp out of range")
                }),
                TimestampFormat::UnixFloat => self.format_timestamps_otel(log, |ts| {
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "timestamp micros; precise for practical values"
                    )]
                    let secs = ts.timestamp_micros() as f64 / 1e6;
                    NotNan::new(secs).expect("this division will never produce a NaN")
                }),
                TimestampFormat::Rfc3339 => (),
            }
        }
    }

    fn format_timestamps_otel<F, T>(&self, log: &mut OtelLog, extract: F)
    where
        F: Fn(&DateTime<Utc>) -> T,
        T: Into<Value>,
    {
        let mut replacements = Vec::new();
        for (k, v) in log.convert_to_fields() {
            if let Value::Timestamp(ts) = v {
                replacements.push((k, extract(&ts).into()));
            }
        }
        for (k, v) in replacements {
            if let Ok(path) = vrl::path::parse_target_path(&k) {
                log.insert(&path, v);
            }
        }
    }

    /// Set the `except_fields` value.
    ///
    /// Returns `Err` if the new `except_fields` fail validation, i.e. are not mutually exclusive
    /// with `only_fields`.
    #[cfg(any(test, feature = "test"))]
    pub fn set_except_fields(
        &mut self,
        except_fields: Option<Vec<ConfigValuePath>>,
    ) -> sol_common::Result<()> {
        Self::validate_fields(self.only_fields.as_ref(), except_fields.as_ref())?;
        self.except_fields = except_fields;
        Ok(())
    }
}

#[configurable_component]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
/// The format in which a timestamp should be represented.
pub enum TimestampFormat {
    /// Represent the timestamp as a Unix timestamp.
    Unix,

    /// Represent the timestamp as a RFC 3339 timestamp.
    Rfc3339,

    /// Represent the timestamp as a Unix timestamp in milliseconds.
    UnixMs,

    /// Represent the timestamp as a Unix timestamp in microseconds.
    UnixUs,

    /// Represent the timestamp as a Unix timestamp in nanoseconds.
    UnixNs,

    /// Represent the timestamp as a Unix timestamp in floating point.
    UnixFloat,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use indoc::indoc;
    use lookup::path::parse_target_path;
    use sol_core::{event::OtelLog, schema};
    use vrl::{btreemap, value::Kind};

    use super::*;

    #[test]
    fn serialize() {
        let string =
            r#"{"only_fields":["a.b[0]"],"except_fields":["ignore_me"],"timestamp_format":"unix"}"#;

        let transformer = serde_json::from_str::<Transformer>(string).unwrap();

        let serialized = serde_json::to_string(&transformer).unwrap();

        assert_eq!(string, serialized);
    }

    #[test]
    fn serialize_empty() {
        let string = "{}";

        let transformer = serde_json::from_str::<Transformer>(string).unwrap();

        let serialized = serde_json::to_string(&transformer).unwrap();

        assert_eq!(string, serialized);
    }

    #[test]
    fn deserialize_and_transform_except() {
        let transformer: Transformer =
            toml::from_str(r#"except_fields = ["a.b.c", "b", "c[0].y", "d.z", "e"]"#).unwrap();
        let mut log = OtelLog::default();
        {
            log.insert("a", 1);
            log.insert("a.b", 1);
            log.insert("a.b.c", 1);
            log.insert("a.b.d", 1);
            log.insert("b[0]", 1);
            log.insert("b[1].x", 1);
            log.insert("c[0].x", 1);
            log.insert("c[0].y", 1);
            log.insert("d.z", 1);
            log.insert("e.a", 1);
            log.insert("e.b", 1);
        }
        let mut event = Event::Log(log);
        transformer.transform(&mut event);
        let log = event.as_log();
        assert!(
            !log.parse_path_and_get_value("a.b.c")
                .ok()
                .flatten()
                .is_some()
        );
        assert!(!log.parse_path_and_get_value("b").ok().flatten().is_some());
        assert!(
            !log.parse_path_and_get_value("b[1].x")
                .ok()
                .flatten()
                .is_some()
        );
        assert!(
            !log.parse_path_and_get_value("c[0].y")
                .ok()
                .flatten()
                .is_some()
        );
        assert!(!log.parse_path_and_get_value("d.z").ok().flatten().is_some());
        assert!(!log.parse_path_and_get_value("e.a").ok().flatten().is_some());

        assert!(
            log.parse_path_and_get_value("a.b.d")
                .ok()
                .flatten()
                .is_some()
        );
        assert!(
            log.parse_path_and_get_value("c[0].x")
                .ok()
                .flatten()
                .is_some()
        );
    }

    #[test]
    fn exclusivity_violation() {
        let config: std::result::Result<Transformer, _> = toml::from_str(indoc! {r#"
            except_fields = ["Doop"]
            only_fields = ["Doop"]
        "#});
        assert!(config.is_err())
    }

    #[test]
    fn deny_unknown_fields() {
        // We're only checking this explicitly because of our custom deserializer arrangement to
        // make it possible to throw the exclusivity error during deserialization, to ensure that we
        // enforce this on the top-level `Transformer` type even though it has to be applied at the
        // intermediate deserialization stage, on `TransformerInner`.
        let config: std::result::Result<Transformer, _> = toml::from_str(indoc! {r#"
            onlyfields = ["Doop"]
        "#});
        assert!(config.is_err())
    }

    #[test]
    fn except_fields_with_service() {
        let transformer: Transformer =
            toml::from_str(r#"except_fields = ["thing.service"]"#).unwrap();
        let mut log = OtelLog::default();
        {
            log.insert("body", 1);
            log.insert("thing.service", "carrot");
        }

        let schema = schema::Definition::new_with_default_metadata(Kind::object(btreemap! {
            "thing" => Kind::object(btreemap! {
                "service" => Kind::bytes(),
            }),
        }));

        let schema = schema.with_meaning(parse_target_path("thing.service").unwrap(), "service");

        let mut event = Event::Log(log);

        event
            .metadata_mut()
            .set_schema_definition(&Arc::new(schema));

        transformer.transform(&mut event);
        let log = event.as_log();
        assert!(
            log.parse_path_and_get_value("body")
                .ok()
                .flatten()
                .is_some()
        );

        // Event no longer contains the service field.
        assert!(
            !log.parse_path_and_get_value("thing.service")
                .ok()
                .flatten()
                .is_some()
        );

        // But we can still get the service by meaning.
        assert_eq!(
            Value::from("carrot"),
            log.get_by_meaning("service").unwrap()
        );
    }
}
