use crate::encoding::ProtobufSerializer;
use bytes::BytesMut;
use prost::Message;
use sol_config_macros::configurable_component;
use sol_core::{
    config::DataType,
    event::{Event, OtelLog, OtelMetric, OtelSpan},
    schema,
};
use sol_opentelemetry_proto::{
    logs::otel_log_to_resource_logs,
    metrics::otel_metric_to_resource_metrics,
    proto::{
        DESCRIPTOR_BYTES, LOGS_REQUEST_MESSAGE_TYPE, METRICS_REQUEST_MESSAGE_TYPE,
        TRACES_REQUEST_MESSAGE_TYPE,
    },
    spans::otel_span_to_resource_spans,
    upstream_opentelemetry_proto::tonic::collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
};
use tokio_util::codec::Encoder;
use vrl::protobuf::encode::Options;

/// Config used to build an `OtlpSerializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct OtlpSerializerConfig {
    // No configuration options needed - OTLP serialization is opinionated
}

impl OtlpSerializerConfig {
    /// Build the `OtlpSerializer` from this configuration.
    pub fn build(&self) -> Result<OtlpSerializer, crate::encoding::BuildError> {
        OtlpSerializer::new()
    }

    /// The data type of events that are accepted by `OtlpSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::Log | DataType::Metric | DataType::Trace
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the OTLP (OpenTelemetry Protocol) protobuf format.
///
/// This serializer encodes events using the OTLP protobuf specification, which is the recommended
/// encoding format for OpenTelemetry data. The output is suitable for sending to OTLP-compatible
/// endpoints with `content-type: application/x-protobuf`.
///
/// # Implementation approach
///
/// This serializer converts Vector's internal event representation to the appropriate OTLP message type
/// based on the top-level field in the event:
/// - `resourceLogs` → `ExportLogsServiceRequest`
/// - `resourceMetrics` → `ExportMetricsServiceRequest`
/// - `resourceSpans` → `ExportTraceServiceRequest`
///
/// The implementation is the inverse of what the `opentelemetry` source does when decoding,
/// ensuring round-trip compatibility.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields will be used once encoding is implemented
pub struct OtlpSerializer {
    logs_descriptor: ProtobufSerializer,
    metrics_descriptor: ProtobufSerializer,
    traces_descriptor: ProtobufSerializer,
    options: Options,
}

impl OtlpSerializer {
    /// Creates a new OTLP serializer with the appropriate message descriptors.
    pub fn new() -> sol_common::Result<Self> {
        let options = Options {
            use_json_names: true,
        };

        let logs_descriptor = ProtobufSerializer::new_from_bytes(
            DESCRIPTOR_BYTES,
            LOGS_REQUEST_MESSAGE_TYPE,
            &options,
        )?;

        let metrics_descriptor = ProtobufSerializer::new_from_bytes(
            DESCRIPTOR_BYTES,
            METRICS_REQUEST_MESSAGE_TYPE,
            &options,
        )?;

        let traces_descriptor = ProtobufSerializer::new_from_bytes(
            DESCRIPTOR_BYTES,
            TRACES_REQUEST_MESSAGE_TYPE,
            &options,
        )?;

        Ok(Self {
            logs_descriptor,
            metrics_descriptor,
            traces_descriptor,
            options,
        })
    }
}

impl Encoder<Event> for OtlpSerializer {
    type Error = sol_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match &event {
            Event::Log(log_event) => {
                let request = otel_log_to_export_request(log_event);
                Message::encode(&request, buffer).map_err(|e| e.to_string())?;
                Ok(())
            }
            Event::Metric(metric_event) => {
                let request = otel_metric_to_export_request(metric_event);
                Message::encode(&request, buffer).map_err(|e| e.to_string())?;
                Ok(())
            }
            Event::Trace(span_event) => {
                let request = otel_span_to_export_request(span_event);
                Message::encode(&request, buffer).map_err(|e| e.to_string())?;
                Ok(())
            }
        }
    }
}

fn otel_log_to_export_request(log_event: &OtelLog) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![otel_log_to_resource_logs(log_event)],
    }
}

fn otel_metric_to_export_request(metric_event: &OtelMetric) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![otel_metric_to_resource_metrics(metric_event)],
    }
}

fn otel_span_to_export_request(span_event: &OtelSpan) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![otel_span_to_resource_spans(span_event)],
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use prost::Message;
    use sol_core::event::{Event, EventMetadata, OtelLog, OtelMetric, OtelSpan, metric::Bucket};
    use tokio_util::codec::Encoder as _;

    use super::OtlpSerializer;

    fn make_serializer() -> OtlpSerializer {
        OtlpSerializer::new().expect("OtlpSerializer::new must succeed")
    }

    #[test]
    fn encodes_counter_without_error() {
        use sol_core::event::MetricKind;
        let mut ser = make_serializer();
        let metric = OtelMetric::new_counter("http_requests_total", MetricKind::Incremental, 100.0);
        let mut buf = BytesMut::new();
        ser.encode(Event::Metric(metric), &mut buf)
            .expect("counter encode must succeed");
        assert!(!buf.is_empty(), "encoded bytes must not be empty");
    }

    #[test]
    fn encodes_gauge_without_error() {
        let mut ser = make_serializer();
        let metric = OtelMetric::new_gauge("cpu_usage", 0.75);
        let mut buf = BytesMut::new();
        ser.encode(Event::Metric(metric), &mut buf)
            .expect("gauge encode must succeed");
        assert!(!buf.is_empty());
    }

    #[test]
    fn encodes_histogram_without_error() {
        use sol_core::event::MetricKind;
        let mut ser = make_serializer();
        let buckets = vec![
            Bucket {
                upper_limit: 0.1,
                count: 10,
            },
            Bucket {
                upper_limit: 1.0,
                count: 25,
            },
            Bucket {
                upper_limit: f64::INFINITY,
                count: 5,
            },
        ];
        let metric =
            OtelMetric::new_histogram("request_latency", MetricKind::Absolute, &buckets, 40, 12.5);
        let mut buf = BytesMut::new();
        ser.encode(Event::Metric(metric), &mut buf)
            .expect("histogram encode must succeed");
        assert!(!buf.is_empty());
    }

    #[test]
    fn encodes_otel_log_event() {
        use opentelemetry_proto::tonic::{
            common::v1::{AnyValue, KeyValue, any_value::Value as AnyValueKind},
            logs::v1::LogRecord,
            resource::v1::Resource,
        };

        let record = LogRecord {
            severity_text: "ERROR".into(),
            severity_number: 17,
            body: Some(AnyValue {
                value: Some(AnyValueKind::StringValue("something broke".into())),
            }),
            ..Default::default()
        };
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(AnyValueKind::StringValue("my-svc".into())),
                }),
            }],
            ..Default::default()
        };
        let event = Event::Log(OtelLog::from_parts(
            record,
            Some(resource),
            None,
            EventMetadata::default(),
        ));

        let mut ser = make_serializer();
        let mut buf = BytesMut::new();
        ser.encode(event, &mut buf)
            .expect("OtelLog encode must succeed");
        assert!(!buf.is_empty());

        let decoded =
            opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest::decode(
                bytes::Bytes::from(buf.to_vec()),
            )
            .expect("must decode as ExportLogsServiceRequest");
        assert_eq!(decoded.resource_logs.len(), 1);
        let rl = &decoded.resource_logs[0];
        assert!(rl.resource.is_some());
        assert_eq!(
            rl.resource.as_ref().unwrap().attributes[0].key,
            "service.name"
        );
        assert_eq!(rl.scope_logs.len(), 1);
        assert_eq!(rl.scope_logs[0].log_records.len(), 1);
        assert_eq!(rl.scope_logs[0].log_records[0].severity_text, "ERROR");
    }

    #[test]
    fn encodes_otel_metric_event() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, NumberDataPoint, metric::Data as MetricData,
            number_data_point::Value as NdpValue,
        };

        let proto_metric = opentelemetry_proto::tonic::metrics::v1::Metric {
            name: "cpu.usage".into(),
            description: "CPU usage".into(),
            unit: "1".into(),
            metadata: vec![],
            data: Some(MetricData::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    value: Some(NdpValue::AsDouble(0.85)),
                    ..Default::default()
                }],
            })),
        };
        let event = Event::Metric(OtelMetric::from_parts(
            proto_metric,
            None,
            None,
            EventMetadata::default(),
        ));

        let mut ser = make_serializer();
        let mut buf = BytesMut::new();
        ser.encode(event, &mut buf)
            .expect("OtelMetric encode must succeed");
        assert!(!buf.is_empty());

        let decoded =
            opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest::decode(
                bytes::Bytes::from(buf.to_vec()),
            )
            .expect("must decode as ExportMetricsServiceRequest");
        assert_eq!(decoded.resource_metrics.len(), 1);
        let rm = &decoded.resource_metrics[0];
        assert_eq!(rm.scope_metrics.len(), 1);
        assert_eq!(rm.scope_metrics[0].metrics.len(), 1);
        assert_eq!(rm.scope_metrics[0].metrics[0].name, "cpu.usage");
    }

    #[test]
    fn encodes_otel_span_event() {
        use opentelemetry_proto::tonic::trace::v1::Span;

        let span = Span {
            name: "GET /api/users".into(),
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            ..Default::default()
        };
        let event = Event::Trace(OtelSpan::from_parts(
            span,
            None,
            None,
            EventMetadata::default(),
        ));

        let mut ser = make_serializer();
        let mut buf = BytesMut::new();
        ser.encode(event, &mut buf)
            .expect("OtelSpan encode must succeed");
        assert!(!buf.is_empty());

        let decoded =
            opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest::decode(
                bytes::Bytes::from(buf.to_vec()),
            )
            .expect("must decode as ExportTraceServiceRequest");
        assert_eq!(decoded.resource_spans.len(), 1);
        let rs = &decoded.resource_spans[0];
        assert_eq!(rs.scope_spans.len(), 1);
        assert_eq!(rs.scope_spans[0].spans.len(), 1);
        assert_eq!(rs.scope_spans[0].spans[0].name, "GET /api/users");
    }
}
