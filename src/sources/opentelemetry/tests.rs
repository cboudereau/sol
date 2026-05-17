use std::{
    net,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    SourceSender,
    config::{SourceConfig, SourceContext},
    event::{Event, EventStatus, into_event_stream},
    sources::opentelemetry::config::{
        GrpcConfig, HttpConfig, LOGS, METRICS, OpentelemetryConfig, TRACES,
    },
    test_util::{
        self,
        addr::next_addr,
        components::{SOURCE_TAGS, assert_source_compliance},
    },
};
use futures::Stream;
use futures_util::StreamExt;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as OtelMetricData;
use prost::Message;
use similar_asserts::assert_eq;
use sol_lib::opentelemetry::upstream_opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
        metrics::v1::{ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient},
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value::Value::StringValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
        Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        Summary, SummaryDataPoint, exponential_histogram_data_point::Buckets, metric::Data,
        summary_data_point::ValueAtQuantile,
    },
    resource::v1::{Resource, Resource as OtelResource},
};
fn create_test_logs_request() -> tonic_0_12::Request<ExportLogsServiceRequest> {
    tonic_0_12::Request::new(ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(OtelResource {
                attributes: vec![KeyValue {
                    key: "res_key".into(),
                    value: Some(AnyValue {
                        value: Some(StringValue("res_val".into())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "some.scope.name".into(),
                    version: "1.2.3".into(),
                    attributes: vec![KeyValue {
                        key: "scope_attr".into(),
                        value: Some(AnyValue {
                            value: Some(StringValue("scope_val".into())),
                        }),
                    }],
                    dropped_attributes_count: 7,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1,
                    observed_time_unix_nano: 2,
                    severity_number: 9,
                    severity_text: "info".into(),
                    body: Some(AnyValue {
                        value: Some(StringValue("log body".into())),
                    }),
                    attributes: vec![KeyValue {
                        key: "attr_key".into(),
                        value: Some(AnyValue {
                            value: Some(StringValue("attr_val".into())),
                        }),
                    }],
                    dropped_attributes_count: 3,
                    flags: 4,
                    // opentelemetry sdk will hex::decode the given trace_id and span_id
                    trace_id: str_into_hex_bytes("4ac52aadf321c2e531db005df08792f5"),
                    span_id: str_into_hex_bytes("0b9e4bda2a55530d"),
                }],
                schema_url: "v1".into(),
            }],
            schema_url: "v1".into(),
        }],
    })
}

#[test]
fn generate_config() {
    test_util::test_generate_config::<OpentelemetryConfig>();
}

#[tokio::test]
async fn receive_grpc_logs_vector_namespace() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(LOGS).await;

        let mut client = LogsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let req = create_test_logs_request();
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let event = output.pop().unwrap();

        let otel_log = event.as_otel_log();

        // Body - round-trip through proto encode→decode to verify content
        // without needing opentelemetry_proto::tonic as a direct dependency.
        assert!(otel_log.body().is_some());
        let body_debug = format!("{:?}", otel_log.body().unwrap());
        assert!(body_debug.contains("log body"));

        // Resource
        let resource = otel_log.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes.len(), 1);
        assert_eq!(resource.attributes[0].key, "res_key");

        // Scope
        let scope = otel_log.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "some.scope.name");
        assert_eq!(scope.version, "1.2.3");
        assert_eq!(scope.attributes.len(), 1);
        assert_eq!(scope.attributes[0].key, "scope_attr");
        assert_eq!(scope.dropped_attributes_count, 7);

        // Log record fields
        assert_eq!(otel_log.severity_text(), "info");
        assert_eq!(otel_log.severity_number(), 9);
        assert_eq!(otel_log.time_unix_nano(), 1);
        assert_eq!(otel_log.observed_time_unix_nano(), 2);
        assert_eq!(
            otel_log.trace_id(),
            &str_into_hex_bytes("4ac52aadf321c2e531db005df08792f5")
        );
        assert_eq!(otel_log.span_id(), &str_into_hex_bytes("0b9e4bda2a55530d"));
        assert_eq!(otel_log.record().flags, 4);
        assert_eq!(otel_log.record().dropped_attributes_count, 3);

        // Attributes
        assert_eq!(otel_log.attributes().len(), 1);
        assert!(otel_log.attributes().get("attr_key").is_some());
    })
    .await;
}

#[tokio::test]
async fn receive_grpc_logs_legacy_namespace() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(LOGS).await;

        let mut client = LogsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let req = create_test_logs_request();
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        // OTel source now always emits Event::Log regardless of namespace
        let otel_log = actual_event.as_otel_log();
        assert_eq!(otel_log.severity_text(), "info");
        assert_eq!(otel_log.severity_number(), 9);
        assert_eq!(otel_log.time_unix_nano(), 1);
        assert_eq!(otel_log.observed_time_unix_nano(), 2);
        assert_eq!(otel_log.record().flags, 4);
        assert_eq!(otel_log.record().dropped_attributes_count, 3);

        assert!(otel_log.body().is_some());
        let body_debug = format!("{:?}", otel_log.body().unwrap());
        assert!(body_debug.contains("log body"));

        let resource = otel_log.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "res_key");

        let scope = otel_log.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "some.scope.name");
        assert_eq!(scope.version, "1.2.3");
    })
    .await;
}

#[tokio::test]
async fn receive_sum_metric() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(METRICS).await;

        // send request via grpc client
        let mut client = MetricsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let (_event_time, event_time_nanos) = current_time_and_nanos();
        let req = tonic_0_12::Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(StringValue("vector-collector".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                schema_url: "".to_string(),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "vector-collector-instrumentation".to_string(),
                        version: "0.111.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "".to_string(),
                    metrics: vec![Metric {
                        name: "some.random.metric".to_string(),
                        description: "Some random metric we use for test".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("localhost".to_string())),
                                        }),
                                    }, KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("vector-collector".to_string())),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: event_time_nanos,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(sol_lib::opentelemetry::upstream_opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.0)),
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            // monotonic =  incremental
                            is_monotonic: true,
                        })),
                    }],
                }],
            }],
        });
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        let otel_metric = actual_event.as_otel_metric();
        assert_eq!(otel_metric.metric().name, "some.random.metric");
        assert_eq!(otel_metric.metric().description, "Some random metric we use for test");
        assert_eq!(otel_metric.metric().unit, "1");
        let proto = otel_metric.metric_proto();
        match &proto.data {
            Some(OtelMetricData::Sum(sum)) => {
                assert_eq!(sum.data_points.len(), 1);
                assert!(sum.is_monotonic);
                assert_eq!(sum.aggregation_temporality, AggregationTemporality::Cumulative as i32);
                let dp = &sum.data_points[0];
                assert_eq!(dp.time_unix_nano, event_time_nanos);
                assert_eq!(dp.attributes.len(), 2);
                assert_eq!(dp.attributes[0].key, "host");
                assert_eq!(dp.attributes[1].key, "service");
                let val_debug = format!("{:?}", dp.value);
                assert!(val_debug.contains("42"), "expected 42.0 in value, got {val_debug}");
            }
            other => panic!("expected Sum, got {:?}", other),
        }
        let resource = otel_metric.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_metric.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "vector-collector-instrumentation");
        assert_eq!(scope.version, "0.111.0");
    })
        .await;
}

#[tokio::test]
async fn receive_sum_non_monotonic_metric() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(METRICS).await;

        // send request via grpc client
        let mut client = MetricsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let (_event_time, event_time_nanos) = current_time_and_nanos();

        let req = tonic_0_12::Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(StringValue("vector-collector".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                schema_url: "".to_string(),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "vector-collector-instrumentation".to_string(),
                        version: "0.111.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "".to_string(),
                    metrics: vec![Metric {
                        name: "some.random.metric".to_string(),
                        description: "Some random metric we use for test".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("localhost".to_string())),
                                        }),
                                    }, KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("vector-collector".to_string())),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: event_time_nanos,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(sol_lib::opentelemetry::upstream_opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.0)),
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            // monotonic =  incremental
                            is_monotonic: false,
                        })),
                    }],
                }],
            }],
        });
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        let otel_metric = actual_event.as_otel_metric();
        assert_eq!(otel_metric.metric().name, "some.random.metric");
        match &otel_metric.metric().data {
            Some(OtelMetricData::Sum(sum)) => {
                assert_eq!(sum.data_points.len(), 1);
                assert!(!sum.is_monotonic);
                assert_eq!(sum.aggregation_temporality, AggregationTemporality::Cumulative as i32);
                let dp = &sum.data_points[0];
                assert_eq!(dp.time_unix_nano, event_time_nanos);
                let val_debug = format!("{:?}", dp.value);
                assert!(val_debug.contains("42"), "expected 42.0 in value, got {val_debug}");
            }
            other => panic!("expected Sum, got {:?}", other),
        }
        let resource = otel_metric.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_metric.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "vector-collector-instrumentation");
    })
        .await;
}

#[tokio::test]
async fn receive_gauge_metric() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(METRICS).await;

        // send request via grpc client
        let mut client = MetricsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let (_event_time, event_time_nanos) = current_time_and_nanos();

        let req = tonic_0_12::Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(StringValue("vector-collector".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                schema_url: "".to_string(),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "vector-collector-instrumentation".to_string(),
                        version: "0.111.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "".to_string(),
                    metrics: vec![Metric {
                        name: "some.random.metric".to_string(),
                        description: "Some random metric we use for test".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("localhost".to_string())),
                                        }),
                                    }, KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("vector-collector".to_string())),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: event_time_nanos,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(sol_lib::opentelemetry::upstream_opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.0)),
                            }],
                        })),
                    }],
                }],
            }],
        });
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        let otel_metric = actual_event.as_otel_metric();
        assert_eq!(otel_metric.metric().name, "some.random.metric");
        match &otel_metric.metric().data {
            Some(OtelMetricData::Gauge(gauge)) => {
                assert_eq!(gauge.data_points.len(), 1);
                let dp = &gauge.data_points[0];
                assert_eq!(dp.time_unix_nano, event_time_nanos);
                let val_debug = format!("{:?}", dp.value);
                assert!(val_debug.contains("42"), "expected 42.0 in value, got {val_debug}");
            }
            other => panic!("expected Gauge, got {:?}", other),
        }
        let resource = otel_metric.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_metric.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "vector-collector-instrumentation");
    })
        .await;
}

#[tokio::test]
async fn receive_histogram_metric() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(METRICS).await;

        // send request via grpc client
        let mut client = MetricsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let (_event_time, event_time_nanos) = current_time_and_nanos();

        let req = tonic_0_12::Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(StringValue("vector-collector".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                schema_url: "".to_string(),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "vector-collector-instrumentation".to_string(),
                        version: "0.111.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "".to_string(),
                    metrics: vec![Metric {
                        name: "some.random.metric".to_string(),
                        description: "Some random metric we use for test".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(Data::Histogram(Histogram {
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("localhost".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue(
                                                "vector-collector".to_string(),
                                            )),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: event_time_nanos,
                                count: 9,
                                sum: Some(123.45),
                                bucket_counts: vec![1, 2, 2, 4],
                                explicit_bounds: vec![50.0, 100.0, 150.0],
                                exemplars: vec![],
                                flags: 0,
                                min: Some(10.0),
                                max: Some(60.0),
                            }],
                        })),
                    }],
                }],
            }],
        });
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        let otel_metric = actual_event.as_otel_metric();
        assert_eq!(otel_metric.metric().name, "some.random.metric");
        match &otel_metric.metric().data {
            Some(OtelMetricData::Histogram(hist)) => {
                assert_eq!(
                    hist.aggregation_temporality,
                    AggregationTemporality::Cumulative as i32
                );
                assert_eq!(hist.data_points.len(), 1);
                let dp = &hist.data_points[0];
                assert_eq!(dp.time_unix_nano, event_time_nanos);
                assert_eq!(dp.count, 9);
                assert_eq!(dp.sum, Some(123.45));
                assert_eq!(dp.bucket_counts, vec![1, 2, 2, 4]);
                assert_eq!(dp.explicit_bounds, vec![50.0, 100.0, 150.0]);
                assert_eq!(dp.min, Some(10.0));
                assert_eq!(dp.max, Some(60.0));
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
        let resource = otel_metric.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_metric.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "vector-collector-instrumentation");
    })
    .await;
}

#[tokio::test]
async fn receive_histogram_delta_metric() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(METRICS).await;

        // send request via grpc client
        let mut client = MetricsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let (_event_time, event_time_nanos) = current_time_and_nanos();

        let req = tonic_0_12::Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(StringValue("vector-collector".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                schema_url: "".to_string(),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "vector-collector-instrumentation".to_string(),
                        version: "0.111.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "".to_string(),
                    metrics: vec![Metric {
                        name: "some.random.metric".to_string(),
                        description: "Some random metric we use for test".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(Data::Histogram(Histogram {
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("localhost".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue(
                                                "vector-collector".to_string(),
                                            )),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: event_time_nanos,
                                count: 9,
                                sum: Some(123.45),
                                bucket_counts: vec![1, 2, 2, 4],
                                explicit_bounds: vec![50.0, 100.0, 150.0],
                                exemplars: vec![],
                                flags: 0,
                                min: Some(10.0),
                                max: Some(60.0),
                            }],
                        })),
                    }],
                }],
            }],
        });
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        let otel_metric = actual_event.as_otel_metric();
        assert_eq!(otel_metric.metric().name, "some.random.metric");
        match &otel_metric.metric().data {
            Some(OtelMetricData::Histogram(hist)) => {
                assert_eq!(
                    hist.aggregation_temporality,
                    AggregationTemporality::Delta as i32
                );
                assert_eq!(hist.data_points.len(), 1);
                let dp = &hist.data_points[0];
                assert_eq!(dp.time_unix_nano, event_time_nanos);
                assert_eq!(dp.count, 9);
                assert_eq!(dp.sum, Some(123.45));
                assert_eq!(dp.bucket_counts, vec![1, 2, 2, 4]);
                assert_eq!(dp.explicit_bounds, vec![50.0, 100.0, 150.0]);
                assert_eq!(dp.min, Some(10.0));
                assert_eq!(dp.max, Some(60.0));
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
        let resource = otel_metric.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_metric.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "vector-collector-instrumentation");
    })
    .await;
}

#[tokio::test]
async fn receive_expontential_histogram_metric() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(METRICS).await;

        // send request via grpc client
        let mut client = MetricsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let (_event_time, event_time_nanos) = current_time_and_nanos();

        let req = tonic_0_12::Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(StringValue("vector-collector".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                schema_url: "".to_string(),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "vector-collector-instrumentation".to_string(),
                        version: "0.111.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "".to_string(),
                    metrics: vec![Metric {
                        name: "some.random.metric".to_string(),
                        description: "Some random metric we use for test".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            data_points: vec![ExponentialHistogramDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("localhost".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue(
                                                "vector-collector".to_string(),
                                            )),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: event_time_nanos,
                                count: 7,
                                sum: Some(700.0),
                                scale: 2,
                                zero_count: 1,
                                positive: Some(Buckets {
                                    offset: 0,
                                    bucket_counts: vec![2, 1],
                                }),
                                negative: Some(Buckets {
                                    offset: -1,
                                    bucket_counts: vec![1, 2],
                                }),
                                min: Some(-120.0),
                                max: Some(150.0),
                                exemplars: vec![],
                                flags: 0,
                                zero_threshold: 0f64,
                            }],
                        })),
                    }],
                }],
            }],
        });
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        let otel_metric = actual_event.as_otel_metric();
        assert_eq!(otel_metric.metric().name, "some.random.metric");
        match &otel_metric.metric().data {
            Some(OtelMetricData::ExponentialHistogram(hist)) => {
                assert_eq!(
                    hist.aggregation_temporality,
                    AggregationTemporality::Cumulative as i32
                );
                assert_eq!(hist.data_points.len(), 1);
                let dp = &hist.data_points[0];
                assert_eq!(dp.time_unix_nano, event_time_nanos);
                assert_eq!(dp.count, 7);
                assert_eq!(dp.sum, Some(700.0));
                assert_eq!(dp.scale, 2);
                assert_eq!(dp.zero_count, 1);
                let positive = dp.positive.as_ref().expect("positive buckets");
                assert_eq!(positive.offset, 0);
                assert_eq!(positive.bucket_counts, vec![2, 1]);
                let negative = dp.negative.as_ref().expect("negative buckets");
                assert_eq!(negative.offset, -1);
                assert_eq!(negative.bucket_counts, vec![1, 2]);
                assert_eq!(dp.min, Some(-120.0));
                assert_eq!(dp.max, Some(150.0));
            }
            other => panic!("expected ExponentialHistogram, got {:?}", other),
        }
        let resource = otel_metric.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_metric.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "vector-collector-instrumentation");
    })
    .await;
}

#[tokio::test]
async fn receive_summary_metric() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(METRICS).await;

        // send request via grpc client
        let mut client = MetricsServiceClient::connect(format!("http://{}", env.grpc_addr))
            .await
            .unwrap();
        let (_event_time, event_time_nanos) = current_time_and_nanos();

        let req = tonic_0_12::Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(StringValue("vector-collector".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                schema_url: "".to_string(),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "vector-collector-instrumentation".to_string(),
                        version: "0.111.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "".to_string(),
                    metrics: vec![Metric {
                        name: "some.random.metric".to_string(),
                        description: "Some random metric we use for test".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                attributes: vec![
                                    KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue("localhost".to_string())),
                                        }),
                                    },
                                    KeyValue {
                                        key: "service".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(StringValue(
                                                "vector-collector".to_string(),
                                            )),
                                        }),
                                    },
                                ],
                                start_time_unix_nano: 0,
                                time_unix_nano: event_time_nanos,
                                count: 5,
                                sum: 122.5,
                                quantile_values: vec![
                                    ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 24.5,
                                    },
                                    ValueAtQuantile {
                                        quantile: 0.9,
                                        value: 45.0,
                                    },
                                    ValueAtQuantile {
                                        quantile: 1.0,
                                        value: 60.0,
                                    },
                                ],
                                flags: 0,
                            }],
                        })),
                    }],
                }],
            }],
        });
        _ = client.export(req).await;
        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        let otel_metric = actual_event.as_otel_metric();
        assert_eq!(otel_metric.metric().name, "some.random.metric");
        match &otel_metric.metric().data {
            Some(OtelMetricData::Summary(summary)) => {
                assert_eq!(summary.data_points.len(), 1);
                let dp = &summary.data_points[0];
                assert_eq!(dp.time_unix_nano, event_time_nanos);
                assert_eq!(dp.count, 5);
                assert_eq!(dp.sum, 122.5);
                assert_eq!(dp.quantile_values.len(), 3);
                assert_eq!(dp.quantile_values[0].quantile, 0.5);
                assert_eq!(dp.quantile_values[0].value, 24.5);
                assert_eq!(dp.quantile_values[1].quantile, 0.9);
                assert_eq!(dp.quantile_values[1].value, 45.0);
                assert_eq!(dp.quantile_values[2].quantile, 1.0);
                assert_eq!(dp.quantile_values[2].value, 60.0);
            }
            other => panic!("expected Summary, got {:?}", other),
        }
        let resource = otel_metric.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_metric.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "vector-collector-instrumentation");
    })
    .await;
}

fn get_source_config_with_headers(
    grpc_addr: net::SocketAddr,
    http_addr: net::SocketAddr,
) -> OpentelemetryConfig {
    OpentelemetryConfig {
        grpc: GrpcConfig {
            address: grpc_addr,
            tls: Default::default(),
        },
        http: HttpConfig {
            address: http_addr,
            tls: Default::default(),
            keepalive: Default::default(),
            headers: vec![
                "User-Agent".to_string(),
                "X-*".to_string(),
                "AbsentHeader".to_string(),
            ],
        },
        acknowledgements: Default::default(),
    }
}

#[tokio::test]
async fn http_logs_emits_otel_native_events() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let (_guard_0, grpc_addr) = next_addr();
        let (_guard_1, http_addr) = next_addr();

        let source = get_source_config_with_headers(grpc_addr, http_addr);

        let (sender, logs_output, _) = new_source(EventStatus::Delivered, LOGS.to_string());
        let server = source
            .build(SourceContext::new_test(sender, None))
            .await
            .unwrap();
        tokio::spawn(server);
        test_util::wait_for_tcp(http_addr).await;

        let client = reqwest::Client::new();
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        observed_time_unix_nano: 2,
                        severity_number: 9,
                        severity_text: "info".into(),
                        body: Some(AnyValue {
                            value: Some(StringValue("log body".into())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 4,
                        trace_id: str_into_hex_bytes("4ac52aadf321c2e531db005df08792f5"),
                        span_id: str_into_hex_bytes("0b9e4bda2a55530d"),
                    }],
                    schema_url: "v1".into(),
                }],
                schema_url: "v1".into(),
            }],
        };
        let _res = client
            .post(format!("http://{http_addr}/v1/logs"))
            .header("Content-Type", "application/x-protobuf")
            .header("User-Agent", "Test")
            .body(req.encode_to_vec())
            .send()
            .await
            .expect("Failed to send log to Opentelemetry Collector.");

        let mut output = test_util::collect_ready(logs_output).await;
        assert_eq!(output.len(), 1);
        let actual_event = output.pop().unwrap();

        // OTel source now emits Event::Log; header enrichment is not
        // applicable to OTel-native events (headers are a transport concern).
        let otel_log = actual_event.as_otel_log();
        assert_eq!(otel_log.severity_text(), "info");
        assert_eq!(otel_log.severity_number(), 9);
        assert_eq!(otel_log.time_unix_nano(), 1);
        assert_eq!(otel_log.observed_time_unix_nano(), 2);
        assert_eq!(otel_log.record().flags, 4);
        assert!(otel_log.resource().is_none());
        assert!(otel_log.scope().is_none());
    })
    .await;
}

pub struct OTelTestEnv {
    pub grpc_addr: String,
    pub http_addr: String,
    pub _config: OpentelemetryConfig,
    pub output: Box<dyn Stream<Item = Event> + Unpin + Send>,
}

pub async fn build_otlp_test_env(event_name: &'static str) -> OTelTestEnv {
    let (_guard_0, grpc_addr) = next_addr();
    let (_guard_1, http_addr) = next_addr();

    let config = OpentelemetryConfig {
        grpc: GrpcConfig {
            address: grpc_addr,
            tls: Default::default(),
        },
        http: HttpConfig {
            address: http_addr,
            tls: Default::default(),
            keepalive: Default::default(),
            headers: Default::default(),
        },
        acknowledgements: Default::default(),
    };

    let (sender, output, _) = new_source(EventStatus::Delivered, event_name.to_string());

    let server = config
        .build(SourceContext::new_test(sender.clone(), None))
        .await
        .expect("Failed to build source");

    tokio::spawn(server);
    test_util::wait_for_tcp(grpc_addr).await;
    test_util::wait_for_tcp(http_addr).await;

    OTelTestEnv {
        grpc_addr: grpc_addr.to_string(),
        http_addr: http_addr.to_string(),
        _config: config,
        output: Box::new(output),
    }
}

pub(super) fn new_source(
    status: EventStatus,
    event_name: String,
) -> (
    SourceSender,
    impl Stream<Item = Event>,
    impl Stream<Item = Event>,
) {
    let (mut sender, recv) = SourceSender::new_test_finalize(status);
    let output = sender
        .add_outputs(status, event_name)
        .flat_map(into_event_stream);
    (sender, output, recv)
}

fn str_into_hex_bytes(s: &str) -> Vec<u8> {
    // unwrap is okay in test
    hex::decode(s).unwrap()
}

fn current_time_and_nanos() -> (SystemTime, u64) {
    let time = SystemTime::now();
    let nanos = time
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() * 1_000_000_000 + u64::from(d.subsec_nanos()))
        .unwrap();
    (time, nanos)
}

// ---------------------------------------------------------------------------
// OTLP HTTP JSON ingestion tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_json_logs() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(LOGS).await;

        let payload = serde_json::json!({
            "resourceLogs": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": { "stringValue": "json-test" }
                    }]
                },
                "scopeLogs": [{
                    "scope": {
                        "name": "test.scope",
                        "version": "0.1.0"
                    },
                    "logRecords": [{
                        "timeUnixNano": "1000000001",
                        "observedTimeUnixNano": "2000000002",
                        "severityNumber": 9,
                        "severityText": "INFO",
                        "body": { "stringValue": "hello from json" },
                        "attributes": [{
                            "key": "env",
                            "value": { "stringValue": "staging" }
                        }],
                        "droppedAttributesCount": 1,
                        "flags": 5,
                        "traceId": "4ac52aadf321c2e531db005df08792f5",
                        "spanId": "0b9e4bda2a55530d"
                    }]
                }]
            }]
        });

        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/v1/logs", env.http_addr))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .expect("Failed to send JSON logs");
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "application/json"
        );

        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let event = output.pop().unwrap();
        let otel_log = event.as_otel_log();

        assert_eq!(otel_log.severity_text(), "INFO");
        assert_eq!(otel_log.severity_number(), 9);
        assert_eq!(otel_log.time_unix_nano(), 1_000_000_001);
        assert_eq!(otel_log.observed_time_unix_nano(), 2_000_000_002);
        assert_eq!(otel_log.record().flags, 5);
        assert_eq!(otel_log.record().dropped_attributes_count, 1);

        let body_debug = format!("{:?}", otel_log.body().unwrap());
        assert!(body_debug.contains("hello from json"));

        let resource = otel_log.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");

        let scope = otel_log.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "test.scope");
        assert_eq!(scope.version, "0.1.0");

        assert_eq!(otel_log.attributes().len(), 1);
        assert!(otel_log.attributes().get("env").is_some());

        assert_eq!(
            otel_log.trace_id(),
            &str_into_hex_bytes("4ac52aadf321c2e531db005df08792f5")
        );
        assert_eq!(otel_log.span_id(), &str_into_hex_bytes("0b9e4bda2a55530d"));
    })
    .await;
}

#[tokio::test]
async fn http_json_metrics() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(METRICS).await;

        let payload = serde_json::json!({
            "resourceMetrics": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": { "stringValue": "json-metric-test" }
                    }]
                },
                "scopeMetrics": [{
                    "scope": {
                        "name": "test.metric.scope",
                        "version": "1.0.0"
                    },
                    "metrics": [{
                        "name": "http.requests",
                        "description": "Total HTTP requests",
                        "unit": "1",
                        "sum": {
                            "dataPoints": [{
                                "asDouble": 42.0,
                                "timeUnixNano": "1713525203000000000",
                                "attributes": [{
                                    "key": "method",
                                    "value": { "stringValue": "GET" }
                                }]
                            }],
                            "aggregationTemporality": 2,
                            "isMonotonic": true
                        }
                    }]
                }]
            }]
        });

        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/v1/metrics", env.http_addr))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .expect("Failed to send JSON metrics");
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "application/json"
        );

        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let event = output.pop().unwrap();
        let otel_metric = event.as_otel_metric();

        assert_eq!(otel_metric.metric().name, "http.requests");
        assert_eq!(otel_metric.metric().description, "Total HTTP requests");
        assert_eq!(otel_metric.metric().unit, "1");

        let proto = otel_metric.metric_proto();
        match &proto.data {
            Some(OtelMetricData::Sum(sum)) => {
                assert!(sum.is_monotonic);
                assert_eq!(
                    sum.aggregation_temporality,
                    AggregationTemporality::Cumulative as i32
                );
                assert_eq!(sum.data_points.len(), 1);
                let dp = &sum.data_points[0];
                assert_eq!(dp.attributes[0].key, "method");
                let val_debug = format!("{:?}", dp.value);
                assert!(val_debug.contains("42"), "expected 42.0, got {val_debug}");
            }
            other => panic!("expected Sum, got {:?}", other),
        }

        let resource = otel_metric.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_metric.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "test.metric.scope");
    })
    .await;
}

#[tokio::test]
async fn http_json_traces() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(TRACES).await;

        let payload = serde_json::json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [{
                        "key": "service.name",
                        "value": { "stringValue": "json-trace-test" }
                    }]
                },
                "scopeSpans": [{
                    "scope": {
                        "name": "test.trace.scope",
                        "version": "2.0.0"
                    },
                    "spans": [{
                        "traceId": "4ac52aadf321c2e531db005df08792f5",
                        "spanId": "0b9e4bda2a55530d",
                        "parentSpanId": "",
                        "name": "my-span",
                        "kind": 1,
                        "startTimeUnixNano": "1713525203000000000",
                        "endTimeUnixNano": "1713525205000000000",
                        "attributes": [{
                            "key": "http.method",
                            "value": { "stringValue": "GET" }
                        }],
                        "status": { "code": 1 }
                    }]
                }]
            }]
        });

        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/v1/traces", env.http_addr))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .expect("Failed to send JSON traces");
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "application/json"
        );

        let mut output = test_util::collect_ready(env.output).await;
        assert_eq!(output.len(), 1);
        let event = output.pop().unwrap();
        let otel_span = event.as_otel_span();

        assert_eq!(otel_span.span().name, "my-span");
        assert_eq!(otel_span.span().kind, 1);
        assert_eq!(otel_span.span().start_time_unix_nano, 1713525203000000000);
        assert_eq!(otel_span.span().end_time_unix_nano, 1713525205000000000);
        assert_eq!(otel_span.attributes().len(), 1);
        assert!(otel_span.attributes().get("http.method").is_some());
        assert_eq!(
            otel_span.span().trace_id,
            str_into_hex_bytes("4ac52aadf321c2e531db005df08792f5")
        );
        assert_eq!(
            otel_span.span().span_id,
            str_into_hex_bytes("0b9e4bda2a55530d")
        );

        let resource = otel_span.resource_proto().expect("resource must exist");
        assert_eq!(resource.attributes[0].key, "service.name");
        let scope = otel_span.scope_proto().expect("scope must exist");
        assert_eq!(scope.name, "test.trace.scope");
        assert_eq!(scope.version, "2.0.0");
    })
    .await;
}

#[tokio::test]
async fn http_unsupported_content_type_returns_415() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(LOGS).await;

        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/v1/logs", env.http_addr))
            .header("Content-Type", "text/plain")
            .body("not valid")
            .send()
            .await
            .expect("Failed to send request");
        assert_eq!(res.status(), 415);
    })
    .await;
}

#[tokio::test]
async fn http_protobuf_response_content_type() {
    assert_source_compliance(&SOURCE_TAGS, async {
        let env = build_otlp_test_env(LOGS).await;

        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        severity_text: "DEBUG".into(),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let client = reqwest::Client::new();
        let res = client
            .post(format!("http://{}/v1/logs", env.http_addr))
            .header("Content-Type", "application/x-protobuf")
            .body(req.encode_to_vec())
            .send()
            .await
            .expect("Failed to send protobuf request");
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get("content-type").unwrap().to_str().unwrap(),
            "application/x-protobuf"
        );
    })
    .await;
}
