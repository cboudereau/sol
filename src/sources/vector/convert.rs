//! Conversion from native Vector proto events to Sol's OTel-native events.
//!
//! This module bridges the gap between Vector's original wire protocol
//! (protobuf types in `super::proto::event`) and the OTel-native event
//! model (`OtelLog`, `OtelMetric`, `OtelSpan`).

use std::collections::BTreeMap;

use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value as OtelVal,
};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::trace::v1::Span;
use sol_lib::event::otel_metric::OtelMetric;
use sol_lib::event::{Event, MetricKind, OtelLog, OtelSpan, string_value};

use super::proto::event;

/// Convert a native Vector `EventWrapper` to a Sol `Event`.
pub fn convert_event(wrapper: event::EventWrapper) -> Option<Event> {
    match wrapper.event? {
        event::event_wrapper::Event::Log(log) => Some(Event::Log(convert_log(log))),
        event::event_wrapper::Event::Metric(metric) => Some(Event::Metric(convert_metric(metric))),
        event::event_wrapper::Event::Trace(trace) => Some(Event::Trace(convert_trace(trace))),
    }
}

// ---------------------------------------------------------------------------
// Log conversion
// ---------------------------------------------------------------------------

fn convert_log(log: event::Log) -> OtelLog {
    let has_value = log.value.is_some();

    // Build the body: prefer `value` field, fall back to `fields` map.
    let body = if let Some(value) = log.value {
        proto_value_to_any_value(value)
    } else if !log.fields.is_empty() {
        // Promote "message" to body when present; otherwise whole map is body.
        if let Some(msg_value) = log.fields.get("message") {
            proto_value_to_any_value(msg_value.clone())
        } else {
            proto_fields_to_any_value(&log.fields)
        }
    } else {
        AnyValue { value: None }
    };

    // All fields become record attributes.
    let mut attributes: Vec<KeyValue> = Vec::new();
    for (k, v) in &log.fields {
        // If we already promoted "message" from the fields-only path, skip it in attrs.
        if !has_value && k == "message" {
            continue;
        }
        attributes.push(KeyValue {
            key: k.clone(),
            value: Some(proto_value_to_any_value(v.clone())),
        });
    }

    let record = LogRecord {
        body: Some(body),
        attributes,
        ..Default::default()
    };

    OtelLog::new(record)
}

// ---------------------------------------------------------------------------
// Metric conversion
// ---------------------------------------------------------------------------

fn convert_metric(metric: event::Metric) -> OtelMetric {
    let kind = if metric.kind == event::metric::Kind::Absolute as i32 {
        MetricKind::Absolute
    } else {
        MetricKind::Incremental
    };

    let full_name = if metric.namespace.is_empty() {
        metric.name.clone()
    } else {
        format!("{}.{}", metric.namespace, metric.name)
    };

    #[expect(
        clippy::cast_sign_loss,
        reason = "protobuf Timestamp seconds/nanos are non-negative for post-epoch dates"
    )]
    let ts_nanos = metric
        .timestamp
        .as_ref()
        .map(|ts| ts.seconds as u64 * 1_000_000_000 + ts.nanos as u64)
        .unwrap_or(0);

    let start_ts_nanos = if metric.interval_ms > 0 && ts_nanos > 0 {
        ts_nanos.saturating_sub(u64::from(metric.interval_ms) * 1_000_000)
    } else {
        0
    };

    let mut otel = match metric.value {
        Some(event::metric::Value::Counter(c)) => {
            OtelMetric::new_counter(&full_name, kind, c.value)
        }
        Some(event::metric::Value::Gauge(g)) => {
            if kind == MetricKind::Incremental {
                OtelMetric::new_gauge_delta(&full_name, g.value)
            } else {
                OtelMetric::new_gauge(&full_name, g.value)
            }
        }
        Some(event::metric::Value::Set(s)) => {
            OtelMetric::new_set_from_values(&full_name, kind, s.values)
        }
        Some(event::metric::Value::Distribution1(d)) => convert_distribution1(&full_name, kind, d),
        Some(event::metric::Value::Distribution2(d)) => convert_distribution2(&full_name, kind, d),
        Some(event::metric::Value::AggregatedHistogram1(h)) => {
            convert_aggregated_histogram1(&full_name, kind, h)
        }
        Some(event::metric::Value::AggregatedHistogram2(h)) => {
            convert_aggregated_histogram2(&full_name, kind, h)
        }
        Some(event::metric::Value::AggregatedHistogram3(h)) => {
            convert_aggregated_histogram3(&full_name, kind, h)
        }
        Some(event::metric::Value::AggregatedSummary1(s)) => {
            convert_aggregated_summary1(&full_name, kind, s)
        }
        Some(event::metric::Value::AggregatedSummary2(s)) => {
            convert_aggregated_summary2(&full_name, kind, s)
        }
        Some(event::metric::Value::AggregatedSummary3(s)) => {
            convert_aggregated_summary3(&full_name, kind, s)
        }
        Some(event::metric::Value::Sketch(sk)) => convert_sketch(&full_name, kind, sk),
        None => {
            // No value — create a gauge with 0 as fallback.
            OtelMetric::new_gauge(&full_name, 0.0)
        }
    };

    // Apply timestamp to data points.
    apply_metric_timestamps(&mut otel, ts_nanos, start_ts_nanos);

    // Merge tags into data-point attributes.
    apply_metric_tags(&mut otel, &metric.tags_v1, &metric.tags_v2);

    otel
}

fn convert_distribution1(name: &str, kind: MetricKind, d: event::Distribution1) -> OtelMetric {
    let samples: Vec<sol_lib::event::metric::Sample> = d
        .values
        .iter()
        .zip(d.sample_rates.iter())
        .map(|(&value, &rate)| sol_lib::event::metric::Sample { value, rate })
        .collect();
    OtelMetric::new_histogram_from_samples(name, kind, &samples)
}

fn convert_distribution2(name: &str, kind: MetricKind, d: event::Distribution2) -> OtelMetric {
    let samples: Vec<sol_lib::event::metric::Sample> = d
        .samples
        .iter()
        .map(|s| sol_lib::event::metric::Sample {
            value: s.value,
            rate: s.rate,
        })
        .collect();
    OtelMetric::new_histogram_from_samples(name, kind, &samples)
}

fn convert_aggregated_histogram1(
    name: &str,
    kind: MetricKind,
    h: event::AggregatedHistogram1,
) -> OtelMetric {
    let buckets: Vec<sol_lib::event::metric::Bucket> = h
        .buckets
        .iter()
        .zip(h.counts.iter())
        .map(|(&upper_limit, &count)| sol_lib::event::metric::Bucket {
            upper_limit,
            count: u64::from(count),
        })
        .collect();
    OtelMetric::new_histogram(name, kind, &buckets, u64::from(h.count), h.sum)
}

fn convert_aggregated_histogram2(
    name: &str,
    kind: MetricKind,
    h: event::AggregatedHistogram2,
) -> OtelMetric {
    let buckets: Vec<sol_lib::event::metric::Bucket> = h
        .buckets
        .iter()
        .map(|b| sol_lib::event::metric::Bucket {
            upper_limit: b.upper_limit,
            count: u64::from(b.count),
        })
        .collect();
    OtelMetric::new_histogram(name, kind, &buckets, u64::from(h.count), h.sum)
}

fn convert_aggregated_histogram3(
    name: &str,
    kind: MetricKind,
    h: event::AggregatedHistogram3,
) -> OtelMetric {
    let buckets: Vec<sol_lib::event::metric::Bucket> = h
        .buckets
        .iter()
        .map(|b| sol_lib::event::metric::Bucket {
            upper_limit: b.upper_limit,
            count: b.count,
        })
        .collect();
    OtelMetric::new_histogram(name, kind, &buckets, h.count, h.sum)
}

fn convert_aggregated_summary1(
    name: &str,
    _kind: MetricKind,
    s: event::AggregatedSummary1,
) -> OtelMetric {
    let quantiles: Vec<sol_lib::event::metric::Quantile> = s
        .quantiles
        .iter()
        .zip(s.values.iter())
        .map(|(&quantile, &value)| sol_lib::event::metric::Quantile { quantile, value })
        .collect();
    OtelMetric::new_summary(name, &quantiles, u64::from(s.count), s.sum)
}

fn convert_aggregated_summary2(
    name: &str,
    _kind: MetricKind,
    s: event::AggregatedSummary2,
) -> OtelMetric {
    let quantiles: Vec<sol_lib::event::metric::Quantile> = s
        .quantiles
        .iter()
        .map(|q| sol_lib::event::metric::Quantile {
            quantile: q.quantile,
            value: q.value,
        })
        .collect();
    OtelMetric::new_summary(name, &quantiles, u64::from(s.count), s.sum)
}

fn convert_aggregated_summary3(
    name: &str,
    _kind: MetricKind,
    s: event::AggregatedSummary3,
) -> OtelMetric {
    let quantiles: Vec<sol_lib::event::metric::Quantile> = s
        .quantiles
        .iter()
        .map(|q| sol_lib::event::metric::Quantile {
            quantile: q.quantile,
            value: q.value,
        })
        .collect();
    OtelMetric::new_summary(name, &quantiles, s.count, s.sum)
}

fn convert_sketch(name: &str, _kind: MetricKind, sketch: event::Sketch) -> OtelMetric {
    // DDSketch → best-effort gauge with sum/count.
    // A full ExponentialHistogram mapping is complex; fall back to a gauge.
    // TODO: convert DDSketch to OTLP ExponentialHistogram for higher fidelity.
    match sketch.sketch {
        Some(event::sketch::Sketch::AgentDdSketch(dd)) => {
            let mut m = OtelMetric::new_gauge(name, dd.sum);
            m.set_data_point_attribute(
                "sketch.count".to_string(),
                AnyValue {
                    value: Some(OtelVal::IntValue(i64::from(dd.count))),
                },
            );
            m.set_data_point_attribute(
                "sketch.min".to_string(),
                AnyValue {
                    value: Some(OtelVal::DoubleValue(dd.min)),
                },
            );
            m.set_data_point_attribute(
                "sketch.max".to_string(),
                AnyValue {
                    value: Some(OtelVal::DoubleValue(dd.max)),
                },
            );
            m.set_data_point_attribute(
                "sketch.avg".to_string(),
                AnyValue {
                    value: Some(OtelVal::DoubleValue(dd.avg)),
                },
            );
            m
        }
        None => OtelMetric::new_gauge(name, 0.0),
    }
}

/// Set `time_unix_nano` and `start_time_unix_nano` on all data points.
fn apply_metric_timestamps(otel: &mut OtelMetric, ts_nanos: u64, start_ts_nanos: u64) {
    let metric = otel.metric_mut();
    if let Some(data) = metric.data.as_mut() {
        macro_rules! set_ts {
            ($data_points:expr) => {
                for dp in $data_points.iter_mut() {
                    dp.time_unix_nano = ts_nanos;
                    dp.start_time_unix_nano = start_ts_nanos;
                }
            };
        }
        match data {
            MetricData::Sum(s) => set_ts!(s.data_points),
            MetricData::Gauge(g) => set_ts!(g.data_points),
            MetricData::Histogram(h) => set_ts!(h.data_points),
            MetricData::Summary(s) => set_ts!(s.data_points),
            MetricData::ExponentialHistogram(e) => set_ts!(e.data_points),
        }
    }
}

/// Merge `tags_v1` and `tags_v2` into data-point attributes.
fn apply_metric_tags(
    otel: &mut OtelMetric,
    tags_v1: &BTreeMap<String, String>,
    tags_v2: &BTreeMap<String, event::TagValues>,
) {
    // tags_v1: simple key=value
    for (k, v) in tags_v1 {
        otel.set_data_point_attribute(k.clone(), string_value(v));
    }

    // tags_v2: multi-value tags
    for (k, tag_values) in tags_v2 {
        let values: Vec<AnyValue> = tag_values
            .values
            .iter()
            .map(|tv| {
                tv.value
                    .as_ref()
                    .map(string_value)
                    .unwrap_or(AnyValue { value: None })
            })
            .collect();

        if values.len() == 1 {
            // Single-value tag — just use the value directly.
            otel.set_data_point_attribute(k.clone(), values.into_iter().next().unwrap());
        } else {
            // Multi-value tag — use an array.
            otel.set_data_point_attribute(
                k.clone(),
                AnyValue {
                    value: Some(OtelVal::ArrayValue(ArrayValue { values })),
                },
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Trace conversion
// ---------------------------------------------------------------------------

fn convert_trace(trace: event::Trace) -> OtelSpan {
    let mut span = Span::default();
    let mut extra_attrs: Vec<KeyValue> = Vec::new();

    for (k, v) in &trace.fields {
        match k.as_str() {
            "trace_id" => {
                if let Some(b) = extract_bytes(v) {
                    span.trace_id = b;
                }
            }
            "span_id" => {
                if let Some(b) = extract_bytes(v) {
                    span.span_id = b;
                }
            }
            "parent_span_id" => {
                if let Some(b) = extract_bytes(v) {
                    span.parent_span_id = b;
                }
            }
            "name" | "operation_name" => {
                if let Some(s) = extract_string(v) {
                    span.name = s;
                }
            }
            "start_time" | "start" => {
                if let Some(ts) = extract_timestamp(v) {
                    span.start_time_unix_nano = ts;
                }
            }
            "end_time" | "end" => {
                if let Some(ts) = extract_timestamp(v) {
                    span.end_time_unix_nano = ts;
                }
            }
            "kind" => {
                if let Some(event::value::Kind::Integer(i)) = &v.kind {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "OTel SpanKind enum value fits i32"
                    )]
                    let kind = *i as i32;
                    span.kind = kind;
                }
            }
            _ => {
                extra_attrs.push(KeyValue {
                    key: k.clone(),
                    value: Some(proto_value_to_any_value(v.clone())),
                });
            }
        }
    }

    span.attributes = extra_attrs;
    OtelSpan::new(span)
}

// ---------------------------------------------------------------------------
// Value conversion helpers
// ---------------------------------------------------------------------------

fn proto_value_to_any_value(v: event::Value) -> AnyValue {
    match v.kind {
        Some(event::value::Kind::RawBytes(b)) => AnyValue {
            value: Some(OtelVal::StringValue(
                String::from_utf8_lossy(&b).into_owned(),
            )),
        },
        Some(event::value::Kind::Integer(i)) => AnyValue {
            value: Some(OtelVal::IntValue(i)),
        },
        Some(event::value::Kind::Float(f)) => AnyValue {
            value: Some(OtelVal::DoubleValue(f)),
        },
        Some(event::value::Kind::Boolean(b)) => AnyValue {
            value: Some(OtelVal::BoolValue(b)),
        },
        Some(event::value::Kind::Timestamp(ts)) => {
            let nanos = ts.seconds * 1_000_000_000 + i64::from(ts.nanos);
            AnyValue {
                value: Some(OtelVal::StringValue(nanos.to_string())),
            }
        }
        Some(event::value::Kind::Map(m)) => {
            let kvs: Vec<KeyValue> = m
                .fields
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k,
                    value: Some(proto_value_to_any_value(v)),
                })
                .collect();
            AnyValue {
                value: Some(OtelVal::KvlistValue(KeyValueList { values: kvs })),
            }
        }
        Some(event::value::Kind::Array(a)) => {
            let values: Vec<AnyValue> = a.items.into_iter().map(proto_value_to_any_value).collect();
            AnyValue {
                value: Some(OtelVal::ArrayValue(ArrayValue { values })),
            }
        }
        Some(event::value::Kind::Null(_)) | None => AnyValue { value: None },
    }
}

fn proto_fields_to_any_value(fields: &BTreeMap<String, event::Value>) -> AnyValue {
    let kvs: Vec<KeyValue> = fields
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.clone(),
            value: Some(proto_value_to_any_value(v.clone())),
        })
        .collect();
    AnyValue {
        value: Some(OtelVal::KvlistValue(KeyValueList { values: kvs })),
    }
}

fn extract_bytes(v: &event::Value) -> Option<Vec<u8>> {
    match &v.kind {
        Some(event::value::Kind::RawBytes(b)) => Some(b.clone()),
        _ => None,
    }
}

fn extract_string(v: &event::Value) -> Option<String> {
    match &v.kind {
        Some(event::value::Kind::RawBytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

#[expect(
    clippy::cast_sign_loss,
    reason = "protobuf Timestamp seconds/nanos and integer timestamps are non-negative for post-epoch dates"
)]
const fn extract_timestamp(v: &event::Value) -> Option<u64> {
    match &v.kind {
        Some(event::value::Kind::Timestamp(ts)) => {
            Some(ts.seconds as u64 * 1_000_000_000 + ts.nanos as u64)
        }
        Some(event::value::Kind::Integer(i)) => Some(*i as u64),
        _ => None,
    }
}
