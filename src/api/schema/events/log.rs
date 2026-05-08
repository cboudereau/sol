use async_graphql::Object;
use chrono::{DateTime, Utc};
use sol_lib::{encode_logfmt, event, tap::topology::TapOutput};

use super::EventEncodingType;

#[derive(Debug, Clone)]
pub struct Log {
    output: TapOutput,
    event: event::OtelLog,
}

impl Log {
    pub const fn new(output: TapOutput, event: event::OtelLog) -> Self {
        Self { output, event }
    }

    pub fn get_body(&self) -> Option<String> {
        Some(self.event.get_body()?.to_string_lossy().into_owned())
    }

    pub fn get_timestamp(&self) -> Option<DateTime<Utc>> {
        self.event.get_timestamp()?.as_timestamp().copied()
    }
}

#[Object]
/// Log event with fields for querying log data
impl Log {
    /// Id of the component associated with the log event
    async fn component_id(&self) -> &str {
        self.output.output_id.component.id()
    }

    /// Type of component associated with the log event
    async fn component_type(&self) -> &str {
        self.output.component_type.as_ref()
    }

    /// Kind of component associated with the log event
    async fn component_kind(&self) -> &str {
        self.output.component_kind
    }

    /// Log message
    async fn message(&self) -> Option<String> {
        self.get_body()
    }

    /// Log timestamp
    async fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.get_timestamp()
    }

    /// Log event as an encoded string format
    async fn string(&self, encoding: EventEncodingType) -> String {
        match encoding {
            EventEncodingType::Json => serde_json::to_string(&self.event)
                .expect("JSON serialization of log event failed. Please report."),
            EventEncodingType::Yaml => serde_yaml::to_string(&self.event)
                .expect("YAML serialization of log event failed. Please report."),
            EventEncodingType::Logfmt => encode_logfmt::encode_value(&self.event.value())
                .expect("logfmt serialization of log event failed. Please report."),
        }
    }

    /// Get JSON field data on the log event, by field name
    async fn json(&self, field: String) -> Option<String> {
        self.event
            .parse_path_and_get_value(&field)
            .ok()
            .flatten()
            .map(|field| {
                serde_json::to_string(&field)
                    .expect("JSON serialization of trace event field failed. Please report.")
            })
    }
}
