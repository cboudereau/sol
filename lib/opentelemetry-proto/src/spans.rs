use prost::Message;
use sol_core::event::{Event, EventMetadata, OtelSpan};

use super::proto::{
    common::v1::InstrumentationScope,
    resource::v1::Resource,
    trace::v1::{ResourceSpans, Span},
};

impl ResourceSpans {
    /// Convert into an iterator of `Event::OtelSpan`, preserving the proto
    /// structs with zero field-level conversion.
    pub fn into_otel_event_iter(self) -> impl Iterator<Item = Event> {
        let resource = proto_convert_resource(self.resource);

        self.scope_spans.into_iter().flat_map(move |scope_spans| {
            let scope = proto_convert_scope(scope_spans.scope);
            let resource = resource.clone();
            scope_spans.spans.into_iter().map(move |span| {
                let otel_span = proto_convert_span(span);
                Event::Trace(OtelSpan::from_parts(
                    otel_span,
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

fn proto_convert_span(s: Span) -> upstream_opentelemetry_proto::tonic::trace::v1::Span {
    let bytes = s.encode_to_vec();
    upstream_opentelemetry_proto::tonic::trace::v1::Span::decode(bytes::Bytes::from(bytes))
        .expect("Span proto decode failed on same-schema message")
}

#[cfg(test)]
mod tests {

    use crate::proto::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };

    fn make_resource_spans() -> ResourceSpans {
        use crate::proto::resource::v1::Resource;
        ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("my-svc".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "my-lib".to_string(),
                    version: "2.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                spans: vec![
                    Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        name: "span-a".to_string(),
                        kind: 2,
                        start_time_unix_nano: 1_000_000_000,
                        end_time_unix_nano: 2_000_000_000,
                        attributes: vec![KeyValue {
                            key: "http.method".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("GET".to_string())),
                            }),
                        }],
                        ..Default::default()
                    },
                    Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![3u8; 8],
                        name: "span-b".to_string(),
                        ..Default::default()
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    #[test]
    fn otel_event_iter_preserves_span_fields() {
        let rs = make_resource_spans();
        let events: Vec<_> = rs.into_otel_event_iter().collect();
        assert_eq!(events.len(), 2, "one event per span");

        let span_a = events[0].as_otel_span();
        assert_eq!(span_a.name(), "span-a");
        assert_eq!(span_a.trace_id(), &[1u8; 16]);
        assert_eq!(span_a.span_id(), &[2u8; 8]);
        assert_eq!(span_a.start_time_unix_nano(), 1_000_000_000);
        assert_eq!(span_a.end_time_unix_nano(), 2_000_000_000);
        assert_eq!(span_a.kind(), 2);

        let span_b = events[1].as_otel_span();
        assert_eq!(span_b.name(), "span-b");
        assert_eq!(span_b.span_id(), &[3u8; 8]);
    }

    #[test]
    fn otel_event_iter_preserves_resource() {
        let rs = make_resource_spans();
        let events: Vec<_> = rs.into_otel_event_iter().collect();

        let span = events[0].as_otel_span();
        let resource = span.resource_proto().expect("resource must be present");
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");
    }

    #[test]
    fn otel_event_iter_preserves_scope() {
        let rs = make_resource_spans();
        let events: Vec<_> = rs.into_otel_event_iter().collect();

        let span = events[0].as_otel_span();
        let scope = span.scope().expect("scope must be present");
        assert_eq!(scope.name, "my-lib");
        assert_eq!(scope.version, "2.0.0");
    }

    #[test]
    fn otel_event_iter_preserves_attributes() {
        let rs = make_resource_spans();
        let events: Vec<_> = rs.into_otel_event_iter().collect();

        let span = events[0].as_otel_span();
        let attr = span.attribute("http.method").expect("attribute must exist");
        match &attr.value {
            Some(
                upstream_opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
            ) => {
                assert_eq!(s, "GET")
            }
            other => panic!("unexpected attribute value: {:?}", other),
        }
    }

    #[test]
    fn otel_event_iter_no_scope() {
        let rs = ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![Span {
                    trace_id: vec![0u8; 16],
                    span_id: vec![0u8; 8],
                    name: "lonely".to_string(),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        };
        let events: Vec<_> = rs.into_otel_event_iter().collect();
        assert_eq!(events.len(), 1);
        let span = events[0].as_otel_span();
        assert!(span.scope().is_none());
        assert!(span.resource().is_none());
        assert_eq!(span.name(), "lonely");
    }
}
