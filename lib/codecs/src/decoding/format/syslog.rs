use std::borrow::Cow;

use bytes::Bytes;
use chrono::{DateTime, Datelike, Utc};
use derivative::Derivative;
use lookup::owned_value_path;
use smallvec::{SmallVec, smallvec};
use sol_config::configurable_component;
use sol_core::{
    config::{DataType, insert_source_metadata},
    event::{Event, EventMetadata, ObjectMap, OtelLog, Value},
    schema,
};
use syslog_loose::{IncompleteDate, Message, ProcId, Protocol, Variant};
use vrl::value::{Kind, kind::Collection};

use super::{Deserializer, default_lossy};

/// Config used to build a `SyslogDeserializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct SyslogDeserializerConfig {
    #[serde(skip)]
    source: Option<&'static str>,

    /// Syslog-specific decoding options.
    #[serde(default, skip_serializing_if = "sol_core::serde::is_default")]
    pub syslog: SyslogDeserializerOptions,
}

impl SyslogDeserializerConfig {
    /// Creates a new `SyslogDeserializerConfig`.
    pub fn new(options: SyslogDeserializerOptions) -> Self {
        Self {
            source: None,
            syslog: options,
        }
    }

    /// Create the `SyslogDeserializer` from the given source name.
    pub fn from_source(source: &'static str) -> Self {
        Self {
            source: Some(source),
            ..Default::default()
        }
    }

    /// Build the `SyslogDeserializer` from this configuration.
    pub const fn build(&self) -> SyslogDeserializer {
        SyslogDeserializer {
            source: self.source,
            lossy: self.syslog.lossy,
        }
    }

    /// Return the type of event build by this deserializer.
    pub fn output_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema produced by the deserializer.
    pub fn schema_definition(&self) -> schema::Definition {
        let mut definition = schema::Definition::empty_definition()
            // The `message` field is always defined. If parsing fails, the entire body becomes the
            // message.
            .with_event_field(&owned_value_path!("body"), Kind::bytes(), Some("message"));

        {
            let timestamp_key = owned_value_path!("time_unix_nano");
            // All other fields are optional.
            definition =
                definition.optional_field(&timestamp_key, Kind::integer(), Some("timestamp"));
        }

        definition = definition
            .optional_field(&owned_value_path!("hostname"), Kind::bytes(), Some("host"))
            .optional_field(
                &owned_value_path!("severity"),
                Kind::bytes(),
                Some("severity"),
            )
            .optional_field(&owned_value_path!("facility"), Kind::bytes(), None)
            .optional_field(&owned_value_path!("version"), Kind::integer(), None)
            .optional_field(
                &owned_value_path!("appname"),
                Kind::bytes(),
                Some("service"),
            )
            .optional_field(&owned_value_path!("msgid"), Kind::bytes(), None)
            .optional_field(
                &owned_value_path!("procid"),
                Kind::integer().or_bytes(),
                None,
            )
            // "structured data" is placed at the root. It will always be a map of strings
            .unknown_fields(Kind::object(Collection::from_unknown(Kind::bytes())));

        if self.source.is_some() {
            // This field is added by the syslog source. It will not be present if the data
            // is coming from the codec.
            definition.optional_field(&owned_value_path!("source_ip"), Kind::bytes(), None)
        } else {
            definition
        }
    }
}

/// Syslog-specific decoding options.
#[configurable_component]
#[derive(Debug, Clone, PartialEq, Eq, Derivative)]
#[derivative(Default)]
pub struct SyslogDeserializerOptions {
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

/// Deserializer that builds an `Event` from a byte frame containing a syslog
/// message.
#[derive(Debug, Clone, Derivative)]
#[derivative(Default)]
pub struct SyslogDeserializer {
    /// The syslog source needs it's own syslog deserializer separate from the
    /// syslog codec since it needs to handle the structured of the decoded data
    /// differently when using the Vector lognamespace.
    pub source: Option<&'static str>,
    #[derivative(Default(value = "default_lossy()"))]
    lossy: bool,
}

impl Deserializer for SyslogDeserializer {
    fn parse(&self, bytes: Bytes) -> sol_common::Result<SmallVec<[Event; 1]>> {
        let line: Cow<str> = match self.lossy {
            true => String::from_utf8_lossy(&bytes),
            false => Cow::from(std::str::from_utf8(&bytes)?),
        };
        let line = line.trim();
        let parsed =
            syslog_loose::parse_message_with_year_exact(line, resolve_year, Variant::Either)?;

        let log = match self.source {
            Some(source) => {
                let mut log = OtelLog::from_value_map(
                    Value::Bytes(Bytes::from(parsed.msg.to_string())),
                    EventMetadata::default(),
                );
                insert_metadata_fields_from_syslog(&mut log, source, parsed);
                log
            }
            None => {
                let map = build_fields_from_syslog(parsed);
                OtelLog::from_value_map(Value::Object(map), EventMetadata::default())
            }
        };

        Ok(smallvec![Event::Log(log)])
    }
}

/// Function used to resolve the year for syslog messages that don't include the
/// year.
///
/// If the current month is January, and the syslog message is for December, it
/// will take the previous year.
///
/// Otherwise, take the current year.
fn resolve_year((month, _date, _hour, _min, _sec): IncompleteDate) -> i32 {
    let now = Utc::now();
    if now.month() == 1 && month == 12 {
        now.year() - 1
    } else {
        now.year()
    }
}

fn insert_metadata_fields_from_syslog(
    log: &mut OtelLog,
    source: &'static str,
    parsed: Message<&str>,
) {
    if let Some(timestamp) = parsed.timestamp {
        let timestamp = DateTime::<Utc>::from(timestamp);
        insert_source_metadata(source, log, &owned_value_path!("timestamp"), timestamp);
    }
    if let Some(host) = parsed.hostname {
        insert_source_metadata(
            source,
            log,
            &owned_value_path!("hostname"),
            host.to_string(),
        );
    }
    if let Some(severity) = parsed.severity {
        insert_source_metadata(
            source,
            log,
            &owned_value_path!("severity"),
            severity.as_str().to_owned(),
        );
    }
    if let Some(facility) = parsed.facility {
        insert_source_metadata(
            source,
            log,
            &owned_value_path!("facility"),
            facility.as_str().to_owned(),
        );
    }
    if let Protocol::RFC5424(version) = parsed.protocol {
        insert_source_metadata(
            source,
            log,
            &owned_value_path!("version"),
            i64::from(version),
        );
    }
    if let Some(app_name) = parsed.appname {
        insert_source_metadata(
            source,
            log,
            &owned_value_path!("appname"),
            app_name.to_owned(),
        );
    }
    if let Some(msg_id) = parsed.msgid {
        insert_source_metadata(source, log, &owned_value_path!("msgid"), msg_id.to_owned());
    }
    if let Some(procid) = parsed.procid {
        let value: Value = match procid {
            ProcId::PID(pid) => pid.into(),
            ProcId::Name(name) => name.to_string().into(),
        };
        insert_source_metadata(source, log, &owned_value_path!("procid"), value);
    }

    let mut sdata = ObjectMap::new();
    for element in parsed.structured_data.into_iter() {
        let mut data = ObjectMap::new();

        for (name, value) in element.params() {
            data.insert(name.to_string().into(), value.into());
        }

        sdata.insert(element.id.into(), data.into());
    }

    insert_source_metadata(source, log, &owned_value_path!("structured_data"), sdata);
}

fn build_fields_from_syslog(parsed: Message<&str>) -> ObjectMap {
    let mut map = ObjectMap::new();

    map.insert("body".into(), Value::from(parsed.msg));

    if let Some(timestamp) = parsed.timestamp {
        let timestamp = DateTime::<Utc>::from(timestamp);
        #[expect(
            clippy::cast_sign_loss,
            reason = "syslog timestamps are always non-negative nanoseconds since epoch"
        )]
        let nanos = timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;
        #[expect(
            clippy::cast_possible_wrap,
            reason = "nanosecond timestamps within representable range for both u64 and i64"
        )]
        let nanos_i64 = nanos as i64;
        map.insert("time_unix_nano".into(), Value::Integer(nanos_i64));
    }
    if let Some(host) = parsed.hostname {
        map.insert("hostname".into(), Value::from(host.to_string()));
    }
    if let Some(severity) = parsed.severity {
        map.insert("severity".into(), Value::from(severity.as_str().to_owned()));
    }
    if let Some(facility) = parsed.facility {
        map.insert("facility".into(), Value::from(facility.as_str().to_owned()));
    }
    if let Protocol::RFC5424(version) = parsed.protocol {
        map.insert("version".into(), Value::from(i64::from(version)));
    }
    if let Some(app_name) = parsed.appname {
        map.insert("appname".into(), Value::from(app_name.to_owned()));
    }
    if let Some(msg_id) = parsed.msgid {
        map.insert("msgid".into(), Value::from(msg_id.to_owned()));
    }
    if let Some(procid) = parsed.procid {
        let value: Value = match procid {
            ProcId::PID(pid) => pid.into(),
            ProcId::Name(name) => name.to_string().into(),
        };
        map.insert("procid".into(), value);
    }

    for element in parsed.structured_data.into_iter() {
        let mut sdata = ObjectMap::new();
        for (name, value) in element.params() {
            sdata.insert(name.to_string().into(), value.into());
        }
        map.insert(element.id.to_string().into(), Value::Object(sdata));
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_syslog() {
        let input =
            Bytes::from("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - MSG");
        let deserializer = SyslogDeserializer::default();

        let events = deserializer.parse(input).unwrap();
        assert_eq!(events.len(), 1);
        let log = events[0].as_log();
        assert_eq!(log.get_body(), Some("MSG".into()));
        assert!(log.get_timestamp().is_some());
    }
}
