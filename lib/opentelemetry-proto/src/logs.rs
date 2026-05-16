use std::sync::Arc;

use sol_core::event::otel_attributes::OtelAttributes;
use sol_core::event::{Event, EventMetadata, OtelLog};
use upstream_opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};

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

pub fn resource_logs_into_events(rl: ResourceLogs) -> impl Iterator<Item = Event> {
    let metadata = EventMetadata::default();
    let (resource, resource_attrs) = match rl.resource {
        Some(mut r) => {
            let attrs = Arc::new(OtelAttributes::from_key_values(std::mem::take(
                &mut r.attributes,
            )));
            (Some(Arc::new(r)), attrs)
        }
        None => (None, Arc::new(OtelAttributes::new())),
    };

    rl.scope_logs.into_iter().flat_map(move |scope_log| {
        let (scope, scope_attrs) = match scope_log.scope {
            Some(mut s) => {
                let attrs = Arc::new(OtelAttributes::from_key_values(std::mem::take(
                    &mut s.attributes,
                )));
                (Some(Arc::new(s)), attrs)
            }
            None => (None, Arc::new(OtelAttributes::new())),
        };

        let resource = resource.clone();
        let resource_attrs = resource_attrs.clone();
        let metadata = metadata.clone();

        scope_log.log_records.into_iter().map(move |log_record| {
            Event::Log(OtelLog::from_parts_shared(
                log_record,
                resource.clone(),
                resource_attrs.clone(),
                scope.clone(),
                scope_attrs.clone(),
                metadata.clone(),
            ))
        })
    })
}

pub fn otel_log_to_resource_logs(log: &OtelLog) -> ResourceLogs {
    ResourceLogs {
        resource: log.resource_proto(),
        scope_logs: vec![ScopeLogs {
            scope: log.scope_proto(),
            log_records: vec![log.record_to_proto()],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

#[cfg(test)]
mod tests {

    use upstream_opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
        resource::v1::Resource,
    };

    use super::resource_logs_into_events;

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
        let events: Vec<_> = resource_logs_into_events(rl).collect();
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
            Some(any_value::Value::StringValue(s)) => {
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
        let events: Vec<_> = resource_logs_into_events(rl).collect();

        let log = events[0].as_otel_log();
        let resource = log.resource_proto().expect("resource must be present");
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");
    }

    #[test]
    fn otel_log_event_iter_preserves_scope() {
        let rl = make_resource_logs();
        let events: Vec<_> = resource_logs_into_events(rl).collect();

        let log = events[0].as_otel_log();
        let scope = log.scope().expect("scope must be present");
        assert_eq!(scope.name, "log-lib");
        assert_eq!(scope.version, "3.0.0");
    }

    #[test]
    fn otel_log_event_iter_preserves_attributes() {
        let rl = make_resource_logs();
        let events: Vec<_> = resource_logs_into_events(rl).collect();

        let log = events[0].as_otel_log();
        let attr = log.attribute("http.status").expect("attribute must exist");
        match &attr.value {
            Some(any_value::Value::IntValue(v)) => {
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
        let events: Vec<_> = resource_logs_into_events(rl).collect();
        assert_eq!(events.len(), 1);
        let log = events[0].as_otel_log();
        assert!(log.resource().is_none());
        assert!(log.scope().is_none());
    }
}
