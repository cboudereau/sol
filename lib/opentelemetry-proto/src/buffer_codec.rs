/// `OtlpCodec` implementation for `EventArray ↔ OtlpBufferBatch`.
///
/// This codec is registered at process startup via
/// `sol_core::event::register_otlp_codec` so that `vector-core`'s disk-buffer
/// layer can encode/decode without a circular crate dependency.
///
/// The encode path uses `otel_logs_to_export`, `otel_metrics_to_export`, and
/// `otel_spans_to_export` to produce proto directly from OTel-native event
/// arrays.
use bytes::Bytes;
use prost::Message as _;
use sol_core::event::{
    EventArray, LogArray, MetricArray, OtelLogArray, OtelMetricArray, OtelSpanArray, OtlpCodec,
    TraceArray,
};
use upstream_opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, KeyValue, any_value},
    logs::v1::{ResourceLogs, ScopeLogs},
    trace::v1::{ResourceSpans, ScopeSpans},
};
use vrl::value::Value;

/// Wire format: `OtlpBufferBatch` protobuf.
///
/// Defined here instead of in `vector-core` to avoid a circular dependency
/// (`opentelemetry-proto` → `vector-core` already exists).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, prost::Message)]
struct OtlpBufferBatch {
    #[prost(message, optional, tag = "1")]
    logs: Option<ExportLogsServiceRequest>,
    #[prost(message, optional, tag = "2")]
    metrics: Option<ExportMetricsServiceRequest>,
    #[prost(message, optional, tag = "3")]
    traces: Option<ExportTraceServiceRequest>,
    #[prost(message, repeated, tag = "4")]
    metric_extensions: Vec<MetricExtension>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, prost::Message)]
struct MetricExtension {
    #[prost(uint32, tag = "1")]
    metric_index: u32,
    #[prost(string, repeated, tag = "2")]
    set_values: Vec<String>,
    #[prost(uint32, tag = "3")]
    kind_override: u32,
}

/// Register the OTLP buffer codec with `vector-core`.
///
/// Must be called once at process startup, before any disk buffer is opened with
/// `buffer_format = "otlp"` or `buffer_format = "migrate"`.
/// Safe to call multiple times (subsequent calls are no-ops).
pub fn init() {
    sol_core::event::register_otlp_codec(Box::new(VectorOtlpCodec));
}

pub struct VectorOtlpCodec;

impl OtlpCodec for VectorOtlpCodec {
    fn encode(&self, array: &EventArray, buf: &mut Vec<u8>) -> Result<(), String> {
        event_array_to_batch(array)
            .encode(buf)
            .map_err(|e| format!("OtlpBufferBatch encode: {e}"))
    }

    fn decode(&self, buf: Bytes) -> Result<EventArray, String> {
        let batch =
            OtlpBufferBatch::decode(buf).map_err(|e| format!("OtlpBufferBatch decode: {e}"))?;
        Ok(batch_to_event_array(batch))
    }
}

// ---------------------------------------------------------------------------
// EventArray → OtlpBufferBatch
// ---------------------------------------------------------------------------

fn event_array_to_batch(array: &EventArray) -> OtlpBufferBatch {
    match array {
        EventArray::Logs(logs) => OtlpBufferBatch {
            logs: Some(otel_logs_to_export(logs)),
            ..Default::default()
        },
        EventArray::Metrics(metrics) => {
            let extensions = collect_metric_extensions(metrics);
            OtlpBufferBatch {
                metrics: Some(otel_metrics_to_export(metrics)),
                metric_extensions: extensions,
                ..Default::default()
            }
        }
        EventArray::Traces(traces) => OtlpBufferBatch {
            traces: Some(otel_spans_to_export(traces)),
            ..Default::default()
        },
    }
}

// --- OTel-native logs -------------------------------------------------------

fn otel_logs_to_export(otel_logs: &OtelLogArray) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: otel_logs
            .iter()
            .map(|otel| {
                let record = otel.record_to_proto();
                let resource = otel.resource_proto();
                let scope = otel.scope_proto();
                ResourceLogs {
                    resource,
                    scope_logs: vec![ScopeLogs {
                        scope,
                        log_records: vec![record],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }
            })
            .collect(),
    }
}

// --- OTel-native metrics ----------------------------------------------------

fn otel_metrics_to_export(otel_metrics: &OtelMetricArray) -> ExportMetricsServiceRequest {
    use upstream_opentelemetry_proto::tonic::metrics::v1::{ResourceMetrics, ScopeMetrics};

    ExportMetricsServiceRequest {
        resource_metrics: otel_metrics
            .iter()
            .map(|otel| {
                let metric = otel.metric_proto().clone();
                let resource = otel.resource_proto();
                let scope = otel.scope_proto();
                ResourceMetrics {
                    resource,
                    scope_metrics: vec![ScopeMetrics {
                        scope,
                        metrics: vec![metric],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }
            })
            .collect(),
    }
}

fn collect_metric_extensions(otel_metrics: &OtelMetricArray) -> Vec<MetricExtension> {
    use sol_core::event::MetricKind;

    otel_metrics
        .iter()
        .enumerate()
        .filter_map(|(i, otel)| {
            let has_set = otel.set_values().is_some();
            let has_kind = otel.kind_override().is_some();
            if !has_set && !has_kind {
                return None;
            }
            #[expect(
                clippy::cast_possible_truncation,
                reason = "metric batch size is bounded by OTLP message limits, well within u32::MAX"
            )]
            let metric_index = i as u32;
            Some(MetricExtension {
                metric_index,
                set_values: otel
                    .set_values()
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default(),
                kind_override: match otel.kind_override() {
                    Some(MetricKind::Incremental) => 1,
                    Some(MetricKind::Absolute) => 2,
                    None => 0,
                },
            })
        })
        .collect()
}

fn apply_metric_extensions(metrics: &mut MetricArray, extensions: Vec<MetricExtension>) {
    use sol_core::event::MetricKind;
    use std::collections::BTreeSet;

    for ext in extensions {
        let idx = ext.metric_index as usize;
        if idx >= metrics.len() {
            continue;
        }
        if !ext.set_values.is_empty() {
            metrics[idx].set_set_values(ext.set_values.into_iter().collect::<BTreeSet<_>>());
        }
        match ext.kind_override {
            1 => metrics[idx].set_kind_override(Some(MetricKind::Incremental)),
            2 => metrics[idx].set_kind_override(Some(MetricKind::Absolute)),
            _ => {}
        }
    }
}

// --- OTel-native spans ------------------------------------------------------

fn otel_spans_to_export(otel_spans: &OtelSpanArray) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: otel_spans
            .iter()
            .map(|otel| {
                let span = otel.span_to_proto().clone();
                let resource = otel.resource_proto();
                let scope = otel.scope_proto();
                ResourceSpans {
                    resource,
                    scope_spans: vec![ScopeSpans {
                        scope,
                        spans: vec![span],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// OtlpBufferBatch → EventArray
// ---------------------------------------------------------------------------

fn batch_to_event_array(batch: OtlpBufferBatch) -> EventArray {
    if let Some(req) = batch.logs {
        let logs: LogArray = req
            .resource_logs
            .into_iter()
            .flat_map(|rl| {
                crate::logs::resource_logs_into_events(rl).filter_map(|e| e.try_into_log())
            })
            .collect();
        EventArray::Logs(logs)
    } else if let Some(req) = batch.metrics {
        let mut metrics: MetricArray = req
            .resource_metrics
            .into_iter()
            .flat_map(|rm| {
                crate::metrics::resource_metrics_into_events(rm)
                    .filter_map(|e| e.try_into_otel_metric())
            })
            .collect();
        if !batch.metric_extensions.is_empty() {
            apply_metric_extensions(&mut metrics, batch.metric_extensions);
        }
        EventArray::Metrics(metrics)
    } else if let Some(req) = batch.traces {
        let traces: TraceArray = req
            .resource_spans
            .into_iter()
            .flat_map(|rs| {
                crate::spans::resource_spans_into_events(rs).filter_map(|e| e.try_into_trace())
            })
            .collect();
        EventArray::Traces(traces)
    } else {
        EventArray::Logs(LogArray::default())
    }
}

// ---------------------------------------------------------------------------
// Shared metadata readers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

pub fn value_into_any_value(v: Value) -> any_value::Value {
    match v {
        Value::Bytes(b) => any_value::Value::StringValue(String::from_utf8_lossy(&b).into_owned()),
        Value::Integer(i) => any_value::Value::IntValue(i),
        Value::Float(f) => any_value::Value::DoubleValue(f.into_inner()),
        Value::Boolean(b) => any_value::Value::BoolValue(b),
        Value::Null => any_value::Value::StringValue(String::new()),
        Value::Timestamp(ts) => {
            any_value::Value::StringValue(ts.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        Value::Object(map) => {
            use upstream_opentelemetry_proto::tonic::common::v1::KeyValueList;
            let kvs = map
                .into_iter()
                .map(|(k, val)| KeyValue {
                    key: k.to_string(),
                    value: Some(AnyValue {
                        value: Some(value_into_any_value(val)),
                    }),
                })
                .collect();
            any_value::Value::KvlistValue(KeyValueList { values: kvs })
        }
        Value::Array(arr) => {
            use upstream_opentelemetry_proto::tonic::common::v1::ArrayValue;
            let vals = arr
                .into_iter()
                .map(|val| AnyValue {
                    value: Some(value_into_any_value(val)),
                })
                .collect();
            any_value::Value::ArrayValue(ArrayValue { values: vals })
        }
        Value::Regex(r) => any_value::Value::StringValue(r.to_string()),
    }
}

pub fn hex_value_to_bytes(v: &Value, expected_len: usize) -> Option<Vec<u8>> {
    let s = v.as_str()?;
    let bytes = hex::decode(s.as_ref()).ok()?;
    (bytes.len() == expected_len).then_some(bytes)
}

pub fn value_to_kv_list(v: &Value) -> Option<Vec<KeyValue>> {
    let map = v.as_object()?;
    Some(
        map.iter()
            .map(|(k, val)| KeyValue {
                key: k.to_string(),
                value: Some(AnyValue {
                    value: Some(value_into_any_value(val.clone())),
                }),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use sol_core::event::{EventArray, MetricKind, OtelLog, OtelMetric};
    use vrl::value::Value;

    use super::{VectorOtlpCodec, init};
    use sol_core::event::OtlpCodec as _;

    fn setup() {
        init();
    }

    #[test]
    fn round_trip_log() {
        setup();
        let log = OtelLog::from(Value::from("hello otlp"));
        let array = EventArray::from(log);

        let codec = VectorOtlpCodec;
        let mut buf = Vec::new();
        codec.encode(&array, &mut buf).expect("encode failed");

        let decoded = codec
            .decode(bytes::Bytes::from(buf))
            .expect("decode failed");

        match decoded {
            EventArray::Logs(logs) => {
                assert_eq!(logs.len(), 1);
                assert_eq!(logs[0].body_string(), "hello otlp");
            }
            other => panic!("expected Logs, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_counter() {
        setup();
        let metric = OtelMetric::new_counter("requests_total", MetricKind::Incremental, 42.0);
        let array = EventArray::from(metric);

        let codec = VectorOtlpCodec;
        let mut buf = Vec::new();
        codec.encode(&array, &mut buf).expect("encode failed");

        let decoded = codec
            .decode(bytes::Bytes::from(buf))
            .expect("decode failed");

        match decoded {
            EventArray::Metrics(metrics) => {
                assert_eq!(metrics.len(), 1);
                assert_eq!(metrics[0].name(), "requests_total");
            }
            other => panic!("expected Metrics, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_set_metric_preserves_set_values() {
        setup();
        let metric = OtelMetric::new_set_from_values(
            "unique_users",
            MetricKind::Incremental,
            ["alice", "bob", "charlie"],
        );
        assert!(metric.is_set());
        assert_eq!(metric.set_values().unwrap().len(), 3);

        let array = EventArray::from(metric);
        let codec = VectorOtlpCodec;
        let mut buf = Vec::new();
        codec.encode(&array, &mut buf).expect("encode failed");

        let decoded = codec
            .decode(bytes::Bytes::from(buf))
            .expect("decode failed");
        match decoded {
            EventArray::Metrics(metrics) => {
                assert_eq!(metrics.len(), 1);
                assert_eq!(metrics[0].name(), "unique_users");
                assert!(
                    metrics[0].is_set(),
                    "set_values should survive buffer round-trip"
                );
                let vals = metrics[0].set_values().expect("set_values should be Some");
                assert_eq!(vals.len(), 3);
                assert!(vals.contains("alice"));
                assert!(vals.contains("bob"));
                assert!(vals.contains("charlie"));
            }
            other => panic!("expected Metrics, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_delta_gauge_preserves_kind_override() {
        setup();
        let mut metric = OtelMetric::new_gauge("temperature", 22.5);
        metric.set_kind_override(Some(MetricKind::Incremental));
        assert_eq!(metric.kind(), MetricKind::Incremental);

        let array = EventArray::from(metric);
        let codec = VectorOtlpCodec;
        let mut buf = Vec::new();
        codec.encode(&array, &mut buf).expect("encode failed");

        let decoded = codec
            .decode(bytes::Bytes::from(buf))
            .expect("decode failed");
        match decoded {
            EventArray::Metrics(metrics) => {
                assert_eq!(metrics.len(), 1);
                assert_eq!(metrics[0].name(), "temperature");
                assert_eq!(
                    metrics[0].kind_override(),
                    Some(MetricKind::Incremental),
                    "kind_override should survive buffer round-trip"
                );
                assert_eq!(metrics[0].kind(), MetricKind::Incremental);
            }
            other => panic!("expected Metrics, got {other:?}"),
        }
    }

    #[test]
    fn otlp_buffer_round_trip_via_encodable() {
        use sol_buffers::encoding::Encodable;

        setup();

        let log = OtelLog::from(Value::from("otlp buffer record"));
        let array = EventArray::from(log);

        let metadata = EventArray::get_metadata();
        let mut buf = Vec::new();
        array.encode(&mut buf).expect("encode failed");

        assert!(EventArray::can_decode(metadata));
        let decoded = EventArray::decode(metadata, buf.as_slice()).expect("decode failed");
        match decoded {
            EventArray::Logs(logs) => assert_eq!(logs.len(), 1),
            other => panic!("expected Logs, got {other:?}"),
        }
    }
}
