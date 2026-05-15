use std::sync::Arc;

use sol_core::event::otel_attributes::OtelAttributes;
use sol_core::event::{Event, EventMetadata, OtelMetric};
use upstream_opentelemetry_proto::tonic::metrics::v1::{ResourceMetrics, ScopeMetrics};

pub fn resource_metrics_into_events(rm: ResourceMetrics) -> impl Iterator<Item = Event> {
    let (resource, resource_attrs) = match rm.resource {
        Some(mut r) => {
            let attrs = Arc::new(OtelAttributes::from_key_values(std::mem::take(
                &mut r.attributes,
            )));
            (Some(Arc::new(r)), attrs)
        }
        None => (None, Arc::new(OtelAttributes::new())),
    };

    rm.scope_metrics.into_iter().flat_map(move |scope_metrics| {
        let (scope, scope_attrs) = match scope_metrics.scope {
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

        scope_metrics.metrics.into_iter().map(move |metric| {
            Event::Metric(OtelMetric::from_parts_shared(
                metric,
                resource.clone(),
                resource_attrs.clone(),
                scope.clone(),
                scope_attrs.clone(),
                EventMetadata::default(),
            ))
        })
    })
}

pub fn otel_metric_to_resource_metrics(metric: &OtelMetric) -> ResourceMetrics {
    ResourceMetrics {
        resource: metric.resource_proto(),
        scope_metrics: vec![ScopeMetrics {
            scope: metric.scope_proto(),
            metrics: vec![metric.metric_proto()],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

#[cfg(test)]
mod tests {

    use upstream_opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        metrics::v1::{
            AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            Sum, metric, number_data_point::Value as NDPValue,
        },
        resource::v1::Resource,
    };

    use super::resource_metrics_into_events;

    fn make_resource_metrics() -> ResourceMetrics {
        ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("metric-svc".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "metric-lib".to_string(),
                    version: "2.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                metrics: vec![
                    Metric {
                        name: "request.count".to_string(),
                        description: "Total requests".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "method".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue(
                                            "GET".to_string(),
                                        )),
                                    }),
                                }],
                                start_time_unix_nano: 1_000_000_000,
                                time_unix_nano: 2_000_000_000,
                                value: Some(NDPValue::AsInt(42)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            is_monotonic: true,
                        })),
                    },
                    Metric {
                        name: "cpu.usage".to_string(),
                        description: String::new(),
                        unit: "%".to_string(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: 3_000_000_000,
                                value: Some(NDPValue::AsDouble(75.5)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                        })),
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }
    }

    #[test]
    fn otel_metric_event_iter_preserves_metric_names() {
        let rm = make_resource_metrics();
        let events: Vec<_> = resource_metrics_into_events(rm).collect();
        assert_eq!(events.len(), 2, "one event per OTel Metric");

        let m0 = events[0].as_otel_metric();
        assert_eq!(m0.metric().name, "request.count");
        assert_eq!(m0.metric().description, "Total requests");
        assert_eq!(m0.metric().unit, "1");

        let m1 = events[1].as_otel_metric();
        assert_eq!(m1.metric().name, "cpu.usage");
        assert_eq!(m1.metric().unit, "%");
    }

    #[test]
    fn otel_metric_event_iter_preserves_resource() {
        let rm = make_resource_metrics();
        let events: Vec<_> = resource_metrics_into_events(rm).collect();

        let m = events[0].as_otel_metric();
        let resource = m.resource_proto().expect("resource must be present");
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "service.name");
    }

    #[test]
    fn otel_metric_event_iter_preserves_scope() {
        let rm = make_resource_metrics();
        let events: Vec<_> = resource_metrics_into_events(rm).collect();

        let m = events[0].as_otel_metric();
        let scope = m.scope().expect("scope must be present");
        assert_eq!(scope.name, "metric-lib");
        assert_eq!(scope.version, "2.0.0");
    }

    #[test]
    fn otel_metric_event_iter_preserves_data_points() {
        let rm = make_resource_metrics();
        let events: Vec<_> = resource_metrics_into_events(rm).collect();

        let m0 = events[0].as_otel_metric();
        match &m0.metric().data {
            Some(metric::Data::Sum(sum)) => {
                assert_eq!(sum.data_points.len(), 1);
                assert!(sum.is_monotonic);
                assert_eq!(sum.data_points[0].time_unix_nano, 2_000_000_000);
            }
            other => panic!("expected Sum, got {:?}", other),
        }

        let m1 = events[1].as_otel_metric();
        match &m1.metric().data {
            Some(metric::Data::Gauge(gauge)) => {
                assert_eq!(gauge.data_points.len(), 1);
                assert_eq!(gauge.data_points[0].time_unix_nano, 3_000_000_000);
            }
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn otel_metric_event_iter_no_resource_no_scope() {
        let rm = ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![Metric {
                    name: "bare.metric".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: 0,
                            value: Some(NDPValue::AsDouble(1.0)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        };
        let events: Vec<_> = resource_metrics_into_events(rm).collect();
        assert_eq!(events.len(), 1);
        let m = events[0].as_otel_metric();
        assert!(m.resource().is_none());
        assert!(m.scope().is_none());
        assert_eq!(m.metric().name, "bare.metric");
    }
}
