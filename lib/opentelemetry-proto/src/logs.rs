use prost::Message;
use sol_core::event::{Event, EventMetadata, OtelLog};

use crate::proto::{
    common::v1::InstrumentationScope,
    logs::v1::{LogRecord, ResourceLogs},
    resource::v1::Resource,
};

pub const RESOURCE_KEY: &str = "resources";
pub const ATTRIBUTES_KEY: &str = "attributes";
pub const SCOPE_KEY: &str = "scope";
pub const NAME_KEY: &str = "name";
pub const VERSION_KEY: &str = "version";
pub const TRACE_ID_KEY: &str = "trace_id";
pub const SPAN_ID_KEY: &str = "span_id";
pub const SEVERITY_TEXT_KEY: &str = "severity_text";
pub const SEVERITY_NUMBER_KEY: &str = "severity_number";
pub const OBSERVED_TIMESTAMP_KEY: &str = "observed_timestamp";
pub const DROPPED_ATTRIBUTES_COUNT_KEY: &str = "dropped_attributes_count";
pub const FLAGS_KEY: &str = "flags";

impl ResourceLogs {
    /// Convert into an iterator of `Event::OtelLog`, preserving the proto
    /// structs with zero field-level conversion.
    pub fn into_otel_event_iter(self) -> impl Iterator<Item = Event> {
        let resource = proto_convert_resource(self.resource);

        self.scope_logs.into_iter().flat_map(move |scope_log| {
            let scope = proto_convert_scope(scope_log.scope);
            let resource = resource.clone();
            scope_log.log_records.into_iter().map(move |log_record| {
                let otel_record = proto_convert_log_record(log_record);
                Event::Log(OtelLog::from_parts(
                    otel_record,
                    resource.clone(),
                    scope.clone(),
                    EventMetadata::default(),
                ))
            })
        })
    }
}

fn proto_convert_resource(
    r: Option<Resource>,
) -> Option<upstream_opentelemetry_proto::tonic::resource::v1::Resource> {
    let r = r?;
    let bytes = r.encode_to_vec();
    upstream_opentelemetry_proto::tonic::resource::v1::Resource::decode(bytes::Bytes::from(bytes))
        .ok()
}

fn proto_convert_scope(
    s: Option<InstrumentationScope>,
) -> Option<upstream_opentelemetry_proto::tonic::common::v1::InstrumentationScope> {
    let s = s?;
    let bytes = s.encode_to_vec();
    upstream_opentelemetry_proto::tonic::common::v1::InstrumentationScope::decode(
        bytes::Bytes::from(bytes),
    )
    .ok()
}

fn proto_convert_log_record(
    r: LogRecord,
) -> upstream_opentelemetry_proto::tonic::logs::v1::LogRecord {
    let bytes = r.encode_to_vec();
    upstream_opentelemetry_proto::tonic::logs::v1::LogRecord::decode(bytes::Bytes::from(bytes))
        .expect("LogRecord proto decode failed on same-schema message")
}

#[cfg(test)]
mod tests {

    use crate::proto::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
        resource::v1::Resource,
    };

    fn make_resource_logs() -> ResourceLogs {
        ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("log-svc".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "log-lib".to_string(),
                    version: "3.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                log_records: vec![
                    LogRecord {
                        time_unix_nano: 1_000_000_000,
                        observed_time_unix_nano: 1_100_000_000,
                        severity_number: SeverityNumber::Info as i32,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("hello world".to_string())),
                        }),
                        attributes: vec![KeyValue {
                            key: "http.status".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::IntValue(200)),
                            }),
                        }],
                        trace_id: vec![0xAB; 16],
                        span_id: vec![0xCD; 8],
                        flags: 1,
                        dropped_attributes_count: 0,
                    },
                    LogRecord {
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("second record".to_string())),
                        }),
                        ..Default::default()
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    #[test]
    fn otel_log_event_iter_preserves_record_fields() {
        let rl = make_resource_logs();
        let events: Vec<_> = rl.into_otel_event_iter().collect();
        assert_eq!(events.len(), 2, "one event per log record");

        let log_a = events[0].as_otel_log();
        assert_eq!(log_a.time_unix_nano(), 1_000_000_000);
        assert_eq!(log_a.observed_time_unix_nano(), 1_100_000_000);
        assert_eq!(log_a.severity_number(), SeverityNumber::Info as i32);
        assert_eq!(log_a.severity_text(), "INFO");
        assert_eq!(log_a.trace_id(), &[0xAB; 16]);
        assert_eq!(log_a.span_id(), &[0xCD; 8]);

        let body = log_a.body().expect("body must exist");
        match &body.value {
            Some(
                upstream_opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
            ) => {
                assert_eq!(s, "hello world")
            }
            other => panic!("unexpected body: {:?}", other),
        }

        let log_b = events[1].as_otel_log();
        assert_eq!(log_b.time_unix_nano(), 0);
    }

    #[test]
    fn otel_log_event_iter_preserves_resource() {
        let rl = make_resource_logs();
        let events: Vec<_> = rl.into_otel_event_iter().collect();

        let log = events[0].as_otel_log();
        let resource = log.resource_proto().expect("resource must be present");
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");
    }

    #[test]
    fn otel_log_event_iter_preserves_scope() {
        let rl = make_resource_logs();
        let events: Vec<_> = rl.into_otel_event_iter().collect();

        let log = events[0].as_otel_log();
        let scope = log.scope().expect("scope must be present");
        assert_eq!(scope.name, "log-lib");
        assert_eq!(scope.version, "3.0.0");
    }

    #[test]
    fn otel_log_event_iter_preserves_attributes() {
        let rl = make_resource_logs();
        let events: Vec<_> = rl.into_otel_event_iter().collect();

        let log = events[0].as_otel_log();
        let attr = log.attribute("http.status").expect("attribute must exist");
        match &attr.value {
            Some(upstream_opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(
                v,
            )) => {
                assert_eq!(*v, 200)
            }
            other => panic!("unexpected attribute value: {:?}", other),
        }
    }

    #[test]
    fn otel_log_event_iter_no_resource_no_scope() {
        let rl = ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("bare".to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        };
        let events: Vec<_> = rl.into_otel_event_iter().collect();
        assert_eq!(events.len(), 1);
        let log = events[0].as_otel_log();
        assert!(log.resource().is_none());
        assert!(log.scope().is_none());
    }
}
