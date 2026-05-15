use std::sync::Arc;

use sol_core::event::otel_attributes::OtelAttributes;
use sol_core::event::{Event, EventMetadata, OtelSpan};
use upstream_opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};

pub fn resource_spans_into_events(rs: ResourceSpans) -> impl Iterator<Item = Event> {
    let (resource, resource_attrs) = match rs.resource {
        Some(mut r) => {
            let attrs = Arc::new(OtelAttributes::from_key_values(std::mem::take(
                &mut r.attributes,
            )));
            (Some(Arc::new(r)), attrs)
        }
        None => (None, Arc::new(OtelAttributes::new())),
    };

    rs.scope_spans.into_iter().flat_map(move |scope_spans| {
        let (scope, scope_attrs) = match scope_spans.scope {
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

        scope_spans.spans.into_iter().map(move |mut span| {
            let span_attrs = OtelAttributes::from_key_values(std::mem::take(&mut span.attributes));
            Event::Trace(OtelSpan::from_parts_shared(
                span,
                span_attrs,
                resource.clone(),
                resource_attrs.clone(),
                scope.clone(),
                scope_attrs.clone(),
                EventMetadata::default(),
            ))
        })
    })
}

pub fn otel_span_to_resource_spans(span: &OtelSpan) -> ResourceSpans {
    ResourceSpans {
        resource: span.resource_proto(),
        scope_spans: vec![ScopeSpans {
            scope: span.scope_proto(),
            spans: vec![span.span_to_proto()],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

#[cfg(test)]
mod tests {

    use upstream_opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };

    use super::resource_spans_into_events;

    fn make_resource_spans() -> ResourceSpans {
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
        let events: Vec<_> = resource_spans_into_events(rs).collect();
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
        let events: Vec<_> = resource_spans_into_events(rs).collect();

        let span = events[0].as_otel_span();
        let resource = span.resource_proto().expect("resource must be present");
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");
    }

    #[test]
    fn otel_event_iter_preserves_scope() {
        let rs = make_resource_spans();
        let events: Vec<_> = resource_spans_into_events(rs).collect();

        let span = events[0].as_otel_span();
        let scope = span.scope().expect("scope must be present");
        assert_eq!(scope.name, "my-lib");
        assert_eq!(scope.version, "2.0.0");
    }

    #[test]
    fn otel_event_iter_preserves_attributes() {
        let rs = make_resource_spans();
        let events: Vec<_> = resource_spans_into_events(rs).collect();

        let span = events[0].as_otel_span();
        let attr = span.attribute("http.method").expect("attribute must exist");
        match &attr.value {
            Some(any_value::Value::StringValue(s)) => {
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
        let events: Vec<_> = resource_spans_into_events(rs).collect();
        assert_eq!(events.len(), 1);
        let span = events[0].as_otel_span();
        assert!(span.scope().is_none());
        assert!(span.resource().is_none());
        assert_eq!(span.name(), "lonely");
    }
}
