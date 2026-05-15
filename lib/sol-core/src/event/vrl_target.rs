use std::marker::PhantomData;

use lookup::{OwnedTargetPath, PathPrefix};
#[cfg(test)]
use opentelemetry_proto::tonic::common::v1::AnyValue as OtelAnyValue;
#[cfg(test)]
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValueKind;
use opentelemetry_proto::tonic::common::v1::{
    InstrumentationScope as OtelScope, KeyValue as OtelKeyValue,
};
use opentelemetry_proto::tonic::resource::v1::Resource as OtelResource;
use vrl::{
    compiler::{ProgramInfo, SecretTarget, Target},
    value::{KeyString, ObjectMap, Value},
};

use super::otel_conv::{OtlpCount, OtlpEnumField, OtlpTimestamp};
use super::otel_fields as f;
use super::{Event, EventMetadata, OtelAttributes, OtelLog, OtelMetric, OtelSpan};
use crate::schema::Definition;
#[cfg(test)]
use lookup::owned_value_path;
#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use vrl::{prelude::Collection, value::Kind};

// ---------------------------------------------------------------------------
// OTel AnyValue <-> VRL Value conversion — delegates to otel_event.rs
// ---------------------------------------------------------------------------

use super::otel_event::{
    any_value_to_vrl as otel_any_value_to_vrl, kvlist_to_object_map as otel_kvlist_to_object_map,
    object_map_to_kvlist as object_map_to_otel_kvlist,
};
use super::vrl_value_to_any_value as vrl_value_to_otel_any_value;

fn otel_resource_to_value(resource: &OtelResource) -> Value {
    let mut map = ObjectMap::new();
    map.insert(
        f::ATTRIBUTES.into(),
        Value::Object(otel_kvlist_to_object_map(&resource.attributes)),
    );
    map.insert(
        f::DROPPED_ATTRIBUTES_COUNT.into(),
        Value::Integer(i64::from(resource.dropped_attributes_count)),
    );
    Value::Object(map)
}

fn value_to_otel_resource(val: &Value) -> Option<OtelResource> {
    let map = val.as_object()?;
    let attributes = map
        .get(f::ATTRIBUTES)
        .and_then(|v| v.as_object())
        .map(object_map_to_otel_kvlist)
        .unwrap_or_default();
    let dropped_attributes_count = OtlpCount::from_vrl(
        map.get(f::DROPPED_ATTRIBUTES_COUNT)
            .and_then(vrl::value::Value::as_integer)
            .unwrap_or(0),
    )
    .as_proto();
    Some(OtelResource {
        attributes,
        dropped_attributes_count,
    })
}

fn otel_scope_to_value(scope: &OtelScope) -> Value {
    let mut map = ObjectMap::new();
    map.insert(f::NAME.into(), Value::Bytes(scope.name.clone().into()));
    map.insert(
        f::VERSION.into(),
        Value::Bytes(scope.version.clone().into()),
    );
    map.insert(
        f::ATTRIBUTES.into(),
        Value::Object(otel_kvlist_to_object_map(&scope.attributes)),
    );
    map.insert(
        f::DROPPED_ATTRIBUTES_COUNT.into(),
        Value::Integer(i64::from(scope.dropped_attributes_count)),
    );
    Value::Object(map)
}

fn value_to_otel_scope(val: &Value) -> Option<OtelScope> {
    let map = val.as_object()?;
    let name = map
        .get(f::NAME)
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let version = map
        .get(f::VERSION)
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let attributes = map
        .get(f::ATTRIBUTES)
        .and_then(|v| v.as_object())
        .map(object_map_to_otel_kvlist)
        .unwrap_or_default();
    let dropped_attributes_count = OtlpCount::from_vrl(
        map.get(f::DROPPED_ATTRIBUTES_COUNT)
            .and_then(vrl::value::Value::as_integer)
            .unwrap_or(0),
    )
    .as_proto();
    Some(OtelScope {
        name,
        version,
        attributes,
        dropped_attributes_count,
    })
}

use super::otel_event::hex_encode as hex_encode_bytes;

fn hex_decode_value(val: &Value) -> Vec<u8> {
    let Some(b) = val.as_bytes() else {
        return Vec::new();
    };
    let s = String::from_utf8_lossy(b);
    if !s.len().is_multiple_of(2) {
        return b.to_vec();
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut chars = s.chars();
    while let (Some(hi), Some(lo)) = (chars.next(), chars.next()) {
        #[expect(clippy::cast_possible_truncation, reason = "h * 16 + l is in 0..=255")]
        match (hi.to_digit(16), lo.to_digit(16)) {
            (Some(h), Some(l)) => out.push((h * 16 + l) as u8),
            _ => return b.to_vec(),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// OtelLog -> Value projection
// ---------------------------------------------------------------------------

/// OTel-native `LogRecord` → VRL Value projection.
///
/// Proto fields become top-level keys. `LogRecord` attributes are **flattened**
/// into the top-level map so that VRL programs can use `.key` directly
/// (matching the `OTel` Collector's `transform` processor behavior where
/// `attributes["key"]` is the standard access pattern, but Vector's VRL
/// convention is flat top-level access).
///
/// The `.body` key holds the log body (no `.message` alias).
/// `.resource` and `.scope` are nested objects.
fn otel_log_event_to_value(event: &OtelLog) -> Value {
    let record = &event.record;
    let mut map = ObjectMap::new();

    if let Some(body) = &record.body {
        map.insert(f::BODY.into(), otel_any_value_to_vrl(body));
    }

    if record.severity_number != 0 {
        map.insert(
            f::SEVERITY_NUMBER.into(),
            Value::Integer(i64::from(record.severity_number)),
        );
    }
    if !record.severity_text.is_empty() {
        map.insert(
            f::SEVERITY_TEXT.into(),
            Value::Bytes(record.severity_text.clone().into()),
        );
    }
    if record.time_unix_nano != 0 {
        map.insert(
            f::TIME_UNIX_NANO.into(),
            Value::Integer(OtlpTimestamp::from_nanos(record.time_unix_nano).to_vrl()),
        );
    }
    if record.observed_time_unix_nano != 0 {
        map.insert(
            f::OBSERVED_TIME_UNIX_NANO.into(),
            Value::Integer(OtlpTimestamp::from_nanos(record.observed_time_unix_nano).to_vrl()),
        );
    }
    if !record.trace_id.is_empty() {
        map.insert(f::LOG_TRACE_ID.into(), hex_encode_bytes(&record.trace_id));
    }
    if !record.span_id.is_empty() {
        map.insert(f::LOG_SPAN_ID.into(), hex_encode_bytes(&record.span_id));
    }
    if record.flags != 0 {
        map.insert(f::LOG_FLAGS.into(), Value::Integer(i64::from(record.flags)));
    }

    // Flatten LogRecord attributes into top-level (VRL convention).
    // This means .attributes."key" AND ."key" both work.
    for (key, val) in event.record_attrs.iter() {
        map.insert(key.clone().into(), otel_any_value_to_vrl(val));
    }
    // Also keep .attributes as nested object for explicit access
    if !event.record_attrs.is_empty() {
        let attrs_map: ObjectMap = event
            .record_attrs
            .iter()
            .map(|(k, v)| (KeyString::from(k.clone()), otel_any_value_to_vrl(v)))
            .collect();
        map.insert(f::ATTRIBUTES.into(), Value::Object(attrs_map));
    }

    if let Some(resource) = event.resource_proto() {
        map.insert(f::RESOURCE.into(), otel_resource_to_value(&resource));
    }
    if let Some(scope) = event.scope_proto() {
        map.insert(f::SCOPE.into(), otel_scope_to_value(&scope));
    }

    Value::Object(map)
}

/// VRL Value → `OtelLog` reconstruction.
///
/// Known `OTel` fields (.body, .`severity_text`, etc.) are extracted into proto
/// fields. All other top-level keys become `LogRecord` attributes.
fn value_to_otel_log_event(value: Value, metadata: EventMetadata) -> OtelLog {
    use opentelemetry_proto::tonic::logs::v1::LogRecord;

    let mut map = match value {
        Value::Object(m) => m,
        other => {
            let mut m = ObjectMap::new();
            m.insert(f::BODY.into(), other);
            m
        }
    };

    let body = map.remove(f::BODY).map(|v| vrl_value_to_otel_any_value(&v));

    let severity_number = OtlpEnumField::from_vrl(
        map.remove(f::SEVERITY_NUMBER)
            .and_then(|v| v.as_integer())
            .unwrap_or(0),
    )
    .as_proto();
    let severity_text = map
        .remove(f::SEVERITY_TEXT)
        .and_then(|v| {
            v.as_bytes()
                .map(|b| String::from_utf8_lossy(b).into_owned())
        })
        .unwrap_or_default();
    let time_unix_nano = OtlpTimestamp::from_vrl(
        map.remove(f::TIME_UNIX_NANO)
            .and_then(|v| v.as_integer())
            .unwrap_or(0),
    )
    .as_nanos();
    let observed_time_unix_nano = OtlpTimestamp::from_vrl(
        map.remove(f::OBSERVED_TIME_UNIX_NANO)
            .and_then(|v| v.as_integer())
            .unwrap_or(0),
    )
    .as_nanos();
    let trace_id_val = map.remove(f::LOG_TRACE_ID);
    let trace_id = trace_id_val
        .as_ref()
        .map(hex_decode_value)
        .unwrap_or_default();
    let span_id_val = map.remove(f::LOG_SPAN_ID);
    let span_id = span_id_val
        .as_ref()
        .map(hex_decode_value)
        .unwrap_or_default();
    let flags = OtlpCount::from_vrl(
        map.remove(f::LOG_FLAGS)
            .and_then(|v| v.as_integer())
            .unwrap_or(0),
    )
    .as_proto();
    let dropped_attributes_count = OtlpCount::from_vrl(
        map.remove(f::DROPPED_ATTRIBUTES_COUNT)
            .and_then(|v| v.as_integer())
            .unwrap_or(0),
    )
    .as_proto();

    map.remove(f::ATTRIBUTES);

    let resource_val = map.remove(f::RESOURCE);
    let resource = resource_val.as_ref().and_then(value_to_otel_resource);
    let scope_val = map.remove(f::SCOPE);
    let scope = scope_val.as_ref().and_then(value_to_otel_scope);

    // Everything remaining in map becomes LogRecord attributes.
    let attributes: Vec<OtelKeyValue> = map
        .into_iter()
        .map(|(k, v)| OtelKeyValue {
            key: k.to_string(),
            value: Some(vrl_value_to_otel_any_value(&v)),
        })
        .collect();

    let record = LogRecord {
        body,
        severity_number,
        severity_text,
        time_unix_nano,
        observed_time_unix_nano,
        trace_id,
        span_id,
        flags,
        dropped_attributes_count,
        attributes,
    };

    OtelLog::from_parts(record, resource, scope, metadata)
}

// ---------------------------------------------------------------------------
// OtelSpan -> Value projection
// ---------------------------------------------------------------------------

#[expect(
    clippy::too_many_lines,
    reason = "OTLP span has many proto fields to convert"
)]
fn otel_span_event_to_value(event: &OtelSpan) -> Value {
    let span_proto = event.span();
    let mut map = ObjectMap::new();
    map.insert(
        f::SPAN_TRACE_ID.into(),
        hex_encode_bytes(event.trace_id()),
    );
    map.insert(
        f::SPAN_SPAN_ID.into(),
        hex_encode_bytes(event.span_id()),
    );
    map.insert(
        "trace_state".into(),
        Value::Bytes(span_proto.trace_state.clone().into()),
    );
    map.insert(
        f::PARENT_SPAN_ID.into(),
        hex_encode_bytes(event.parent_span_id()),
    );
    map.insert(f::NAME.into(), Value::Bytes(span_proto.name.clone().into()));
    map.insert(
        f::SPAN_KIND.into(),
        Value::Integer(i64::from(span_proto.kind)),
    );
    map.insert(
        f::START_TIME_UNIX_NANO.into(),
        Value::Integer(OtlpTimestamp::from_nanos(span_proto.start_time_unix_nano).to_vrl()),
    );
    map.insert(
        f::END_TIME_UNIX_NANO.into(),
        Value::Integer(OtlpTimestamp::from_nanos(span_proto.end_time_unix_nano).to_vrl()),
    );
    map.insert(
        f::ATTRIBUTES.into(),
        Value::Object(event.attributes().to_object_map()),
    );
    map.insert(
        f::DROPPED_ATTRIBUTES_COUNT.into(),
        Value::Integer(i64::from(span_proto.dropped_attributes_count)),
    );

    let events_arr: Vec<Value> = span_proto
        .events
        .iter()
        .map(|e| {
            let mut em = ObjectMap::new();
            em.insert(
                f::TIME_UNIX_NANO.into(),
                Value::Integer(OtlpTimestamp::from_nanos(e.time_unix_nano).to_vrl()),
            );
            em.insert(f::NAME.into(), Value::Bytes(e.name.clone().into()));
            em.insert(
                f::ATTRIBUTES.into(),
                Value::Object(otel_kvlist_to_object_map(&e.attributes)),
            );
            em.insert(
                f::DROPPED_ATTRIBUTES_COUNT.into(),
                Value::Integer(i64::from(e.dropped_attributes_count)),
            );
            Value::Object(em)
        })
        .collect();
    map.insert(f::SPAN_EVENTS.into(), Value::Array(events_arr));
    map.insert(
        "dropped_events_count".into(),
        Value::Integer(i64::from(span_proto.dropped_events_count)),
    );

    let links_arr: Vec<Value> = span_proto
        .links
        .iter()
        .map(|l| {
            let mut lm = ObjectMap::new();
            lm.insert(f::SPAN_TRACE_ID.into(), hex_encode_bytes(&l.trace_id));
            lm.insert(f::SPAN_SPAN_ID.into(), hex_encode_bytes(&l.span_id));
            lm.insert(
                "trace_state".into(),
                Value::Bytes(l.trace_state.clone().into()),
            );
            lm.insert(
                f::ATTRIBUTES.into(),
                Value::Object(otel_kvlist_to_object_map(&l.attributes)),
            );
            lm.insert(
                f::DROPPED_ATTRIBUTES_COUNT.into(),
                Value::Integer(i64::from(l.dropped_attributes_count)),
            );
            Value::Object(lm)
        })
        .collect();
    map.insert(f::SPAN_LINKS.into(), Value::Array(links_arr));
    map.insert(
        "dropped_links_count".into(),
        Value::Integer(i64::from(span_proto.dropped_links_count)),
    );

    if let Some(status) = &span_proto.status {
        let mut sm = ObjectMap::new();
        sm.insert(
            "message".into(),
            Value::Bytes(status.message.clone().into()),
        );
        sm.insert("code".into(), Value::Integer(i64::from(status.code)));
        map.insert(f::SPAN_STATUS.into(), Value::Object(sm));
    }

    if let Some(resource) = event.resource_proto() {
        map.insert(f::RESOURCE.into(), otel_resource_to_value(&resource));
    }
    if let Some(scope) = event.scope_proto() {
        map.insert(f::SCOPE.into(), otel_scope_to_value(&scope));
    }

    Value::Object(map)
}

#[expect(
    clippy::too_many_lines,
    reason = "OTLP span has many proto fields to restore"
)]
fn value_to_otel_span_event(value: Value, metadata: EventMetadata) -> OtelSpan {
    use opentelemetry_proto::tonic::trace::v1::{Span, Status, span};

    let map = match value {
        Value::Object(m) => m,
        _ => ObjectMap::new(),
    };

    let events = map
        .get(f::SPAN_EVENTS)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let em = v.as_object()?;
                    Some(span::Event {
                        time_unix_nano: OtlpTimestamp::from_vrl(
                            em.get(f::TIME_UNIX_NANO)
                                .and_then(vrl::value::Value::as_integer)
                                .unwrap_or(0),
                        )
                        .as_nanos(),
                        name: em
                            .get(f::NAME)
                            .and_then(|v| v.as_bytes())
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                            .unwrap_or_default(),
                        attributes: em
                            .get(f::ATTRIBUTES)
                            .and_then(|v| v.as_object())
                            .map(object_map_to_otel_kvlist)
                            .unwrap_or_default(),
                        dropped_attributes_count: OtlpCount::from_vrl(
                            em.get(f::DROPPED_ATTRIBUTES_COUNT)
                                .and_then(vrl::value::Value::as_integer)
                                .unwrap_or(0),
                        )
                        .as_proto(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let links = map
        .get(f::SPAN_LINKS)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let lm = v.as_object()?;
                    Some(span::Link {
                        trace_id: lm
                            .get(f::SPAN_TRACE_ID)
                            .map(hex_decode_value)
                            .unwrap_or_default(),
                        span_id: lm
                            .get(f::SPAN_SPAN_ID)
                            .map(hex_decode_value)
                            .unwrap_or_default(),
                        trace_state: lm
                            .get("trace_state")
                            .and_then(|v| v.as_bytes())
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                            .unwrap_or_default(),
                        attributes: lm
                            .get(f::ATTRIBUTES)
                            .and_then(|v| v.as_object())
                            .map(object_map_to_otel_kvlist)
                            .unwrap_or_default(),
                        dropped_attributes_count: OtlpCount::from_vrl(
                            lm.get(f::DROPPED_ATTRIBUTES_COUNT)
                                .and_then(vrl::value::Value::as_integer)
                                .unwrap_or(0),
                        )
                        .as_proto(),
                        flags: OtlpCount::from_vrl(
                            lm.get(f::SPAN_FLAGS)
                                .and_then(vrl::value::Value::as_integer)
                                .unwrap_or(0),
                        )
                        .as_proto(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let status = map.get(f::SPAN_STATUS).and_then(|v| {
        let sm = v.as_object()?;
        Some(Status {
            message: sm
                .get("message")
                .and_then(|v| v.as_bytes())
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default(),
            code: OtlpEnumField::from_vrl(
                sm.get("code")
                    .and_then(vrl::value::Value::as_integer)
                    .unwrap_or(0),
            )
            .as_proto(),
        })
    });

    let span_proto = Span {
        trace_id: map
            .get(f::SPAN_TRACE_ID)
            .map(hex_decode_value)
            .unwrap_or_default(),
        span_id: map
            .get(f::SPAN_SPAN_ID)
            .map(hex_decode_value)
            .unwrap_or_default(),
        trace_state: map
            .get("trace_state")
            .and_then(|v| v.as_bytes())
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default(),
        parent_span_id: map
            .get(f::PARENT_SPAN_ID)
            .map(hex_decode_value)
            .unwrap_or_default(),
        name: map
            .get(f::NAME)
            .and_then(|v| v.as_bytes())
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default(),
        kind: OtlpEnumField::from_vrl(
            map.get(f::SPAN_KIND)
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0),
        )
        .as_proto(),
        start_time_unix_nano: OtlpTimestamp::from_vrl(
            map.get(f::START_TIME_UNIX_NANO)
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0),
        )
        .as_nanos(),
        end_time_unix_nano: OtlpTimestamp::from_vrl(
            map.get(f::END_TIME_UNIX_NANO)
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0),
        )
        .as_nanos(),
        attributes: map
            .get(f::ATTRIBUTES)
            .and_then(|v| v.as_object())
            .map(object_map_to_otel_kvlist)
            .unwrap_or_default(),
        dropped_attributes_count: OtlpCount::from_vrl(
            map.get(f::DROPPED_ATTRIBUTES_COUNT)
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0),
        )
        .as_proto(),
        events,
        dropped_events_count: OtlpCount::from_vrl(
            map.get("dropped_events_count")
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0),
        )
        .as_proto(),
        links,
        dropped_links_count: OtlpCount::from_vrl(
            map.get("dropped_links_count")
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0),
        )
        .as_proto(),
        status,
        flags: OtlpCount::from_vrl(
            map.get(f::SPAN_FLAGS)
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0),
        )
        .as_proto(),
    };

    let resource = map.get(f::RESOURCE).and_then(value_to_otel_resource);
    let scope = map.get(f::SCOPE).and_then(value_to_otel_scope);

    OtelSpan::from_parts(span_proto, resource, scope, metadata)
}

// ---------------------------------------------------------------------------
// OtelMetric -> Value projection (restricted, read-heavy)
// ---------------------------------------------------------------------------

/// OTel-native Metric → VRL Value projection.
///
/// Exposes legacy paths (.kind, .namespace) and OTel-native paths (.data).
/// `.attributes` flattens first data point's attributes.
#[expect(
    clippy::too_many_lines,
    reason = "OTLP conversion requires handling many fields"
)]
fn otel_metric_event_to_value(event: &OtelMetric) -> Value {
    use opentelemetry_proto::tonic::metrics::v1::metric;

    let mut map = ObjectMap::new();

    // Proto fields
    map.insert("name".into(), Value::Bytes(event.name().to_owned().into()));
    if !event.metric().description.is_empty() {
        map.insert(
            "description".into(),
            Value::Bytes(event.metric().description.clone().into()),
        );
    }
    if !event.metric().unit.is_empty() {
        map.insert(
            "unit".into(),
            Value::Bytes(event.metric().unit.clone().into()),
        );
    }

    if let Some(resource) = event.resource_proto() {
        map.insert("resource".into(), otel_resource_to_value(&resource));
    }
    if let Some(scope) = event.scope_proto() {
        map.insert("scope".into(), otel_scope_to_value(&scope));
    }

    // .attributes — shorthand for first data point's attributes
    if let Some(dp) = event.first_dp_attrs()
        && !dp.is_empty()
    {
        map.insert("attributes".into(), Value::Object(dp.to_object_map()));
    }

    // .kind — "absolute" or "incremental"
    let kind_str = match event.kind() {
        crate::event::MetricKind::Absolute => "absolute",
        crate::event::MetricKind::Incremental => "incremental",
    };
    map.insert("kind".into(), Value::Bytes(kind_str.into()));

    // .namespace
    if let Some(ns) = event.namespace() {
        map.insert("namespace".into(), Value::Bytes(ns.to_owned().into()));
    }

    // .data — full OTel proto structure (with dp attrs populated)
    let metric_with_attrs = event.metric_proto();
    if let Some(data) = &metric_with_attrs.data {
        let mut data_map = ObjectMap::new();
        match data {
            metric::Data::Sum(sum) => {
                data_map.insert("type".into(), Value::Bytes("sum".into()));
                let mut sum_map = ObjectMap::new();
                sum_map.insert("is_monotonic".into(), Value::Boolean(sum.is_monotonic));
                sum_map.insert(
                    "aggregation_temporality".into(),
                    Value::Integer(i64::from(sum.aggregation_temporality)),
                );
                sum_map.insert(
                    "data_points".into(),
                    number_data_points_to_value(&sum.data_points),
                );
                data_map.insert("sum".into(), Value::Object(sum_map));
            }
            metric::Data::Gauge(gauge) => {
                data_map.insert("type".into(), Value::Bytes("gauge".into()));
                let mut gauge_map = ObjectMap::new();
                gauge_map.insert(
                    "data_points".into(),
                    number_data_points_to_value(&gauge.data_points),
                );
                data_map.insert("gauge".into(), Value::Object(gauge_map));
            }
            metric::Data::Histogram(histo) => {
                data_map.insert("type".into(), Value::Bytes("histogram".into()));
                let mut h_map = ObjectMap::new();
                h_map.insert(
                    "aggregation_temporality".into(),
                    Value::Integer(i64::from(histo.aggregation_temporality)),
                );
                let dps: Vec<Value> = histo
                    .data_points
                    .iter()
                    .map(|dp| {
                        let mut m = ObjectMap::new();
                        m.insert(
                            "attributes".into(),
                            Value::Object(otel_kvlist_to_object_map(&dp.attributes)),
                        );
                        #[expect(
                            clippy::cast_possible_wrap,
                            reason = "histogram count fits in i64 in practice"
                        )]
                        m.insert("count".into(), Value::Integer(dp.count as i64));
                        if let Some(sum) = dp.sum {
                            m.insert(
                                "sum".into(),
                                Value::Float(
                                    ordered_float::NotNan::new(sum)
                                        .unwrap_or(ordered_float::NotNan::new(0.0).unwrap()),
                                ),
                            );
                        }
                        #[expect(
                            clippy::cast_possible_wrap,
                            reason = "bucket counts fit in i64 in practice"
                        )]
                        m.insert(
                            "bucket_counts".into(),
                            Value::Array(
                                dp.bucket_counts
                                    .iter()
                                    .map(|c| Value::Integer(*c as i64))
                                    .collect(),
                            ),
                        );
                        m.insert(
                            "explicit_bounds".into(),
                            Value::Array(
                                dp.explicit_bounds
                                    .iter()
                                    .map(|b| {
                                        Value::Float(
                                            ordered_float::NotNan::new(*b).unwrap_or(
                                                ordered_float::NotNan::new(0.0).unwrap(),
                                            ),
                                        )
                                    })
                                    .collect(),
                            ),
                        );
                        m.insert(
                            "time_unix_nano".into(),
                            Value::Integer(OtlpTimestamp::from_nanos(dp.time_unix_nano).to_vrl()),
                        );
                        Value::Object(m)
                    })
                    .collect();
                h_map.insert("data_points".into(), Value::Array(dps));
                data_map.insert("histogram".into(), Value::Object(h_map));
            }
            metric::Data::ExponentialHistogram(eh) => {
                data_map.insert("type".into(), Value::Bytes("exponential_histogram".into()));
                let mut eh_map = ObjectMap::new();
                eh_map.insert(
                    "aggregation_temporality".into(),
                    Value::Integer(i64::from(eh.aggregation_temporality)),
                );
                let dps: Vec<Value> = eh
                    .data_points
                    .iter()
                    .map(|dp| {
                        let mut m = ObjectMap::new();
                        m.insert(
                            "attributes".into(),
                            Value::Object(otel_kvlist_to_object_map(&dp.attributes)),
                        );
                        #[expect(
                            clippy::cast_possible_wrap,
                            reason = "exponential histogram count fits in i64 in practice"
                        )]
                        m.insert("count".into(), Value::Integer(dp.count as i64));
                        if let Some(sum) = dp.sum {
                            m.insert(
                                "sum".into(),
                                Value::Float(
                                    ordered_float::NotNan::new(sum)
                                        .unwrap_or(ordered_float::NotNan::new(0.0).unwrap()),
                                ),
                            );
                        }
                        m.insert("scale".into(), Value::Integer(i64::from(dp.scale)));
                        #[expect(
                            clippy::cast_possible_wrap,
                            reason = "exponential histogram zero_count fits in i64 in practice"
                        )]
                        m.insert("zero_count".into(), Value::Integer(dp.zero_count as i64));
                        m.insert(
                            "time_unix_nano".into(),
                            Value::Integer(OtlpTimestamp::from_nanos(dp.time_unix_nano).to_vrl()),
                        );
                        Value::Object(m)
                    })
                    .collect();
                eh_map.insert("data_points".into(), Value::Array(dps));
                data_map.insert("exponential_histogram".into(), Value::Object(eh_map));
            }
            metric::Data::Summary(summary) => {
                data_map.insert("type".into(), Value::Bytes("summary".into()));
                let mut s_map = ObjectMap::new();
                let dps: Vec<Value> = summary
                    .data_points
                    .iter()
                    .map(|dp| {
                        let mut m = ObjectMap::new();
                        m.insert(
                            "attributes".into(),
                            Value::Object(otel_kvlist_to_object_map(&dp.attributes)),
                        );
                        #[expect(
                            clippy::cast_possible_wrap,
                            reason = "summary count fits in i64 in practice"
                        )]
                        m.insert("count".into(), Value::Integer(dp.count as i64));
                        m.insert(
                            "sum".into(),
                            Value::Float(
                                ordered_float::NotNan::new(dp.sum)
                                    .unwrap_or(ordered_float::NotNan::new(0.0).unwrap()),
                            ),
                        );
                        m.insert(
                            "time_unix_nano".into(),
                            Value::Integer(OtlpTimestamp::from_nanos(dp.time_unix_nano).to_vrl()),
                        );
                        Value::Object(m)
                    })
                    .collect();
                s_map.insert("data_points".into(), Value::Array(dps));
                data_map.insert("summary".into(), Value::Object(s_map));
            }
        }
        map.insert("data".into(), Value::Object(data_map));
    }

    Value::Object(map)
}

/// Convert `NumberDataPoint` array to VRL Value.
fn number_data_points_to_value(
    dps: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
) -> Value {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point;
    Value::Array(
        dps.iter()
            .map(|dp| {
                let mut m = ObjectMap::new();
                m.insert(
                    "attributes".into(),
                    Value::Object(otel_kvlist_to_object_map(&dp.attributes)),
                );
                m.insert(
                    "time_unix_nano".into(),
                    Value::Integer(OtlpTimestamp::from_nanos(dp.time_unix_nano).to_vrl()),
                );
                if dp.start_time_unix_nano != 0 {
                    m.insert(
                        "start_time_unix_nano".into(),
                        Value::Integer(OtlpTimestamp::from_nanos(dp.start_time_unix_nano).to_vrl()),
                    );
                }
                match &dp.value {
                    Some(number_data_point::Value::AsDouble(d)) => {
                        m.insert(
                            "value".into(),
                            Value::Float(
                                ordered_float::NotNan::new(*d)
                                    .unwrap_or(ordered_float::NotNan::new(0.0).unwrap()),
                            ),
                        );
                    }
                    Some(number_data_point::Value::AsInt(i)) => {
                        m.insert("value".into(), Value::Integer(*i));
                    }
                    None => {}
                }
                Value::Object(m)
            })
            .collect(),
    )
}

/// VRL Value → `OtelMetric` reconstruction.
///
/// Known `OTel` fields are extracted from the Value and used to rebuild
/// the proto metric. The format mirrors what `otel_metric_event_to_value()`
/// produces.
#[expect(
    clippy::too_many_lines,
    reason = "OTLP conversion requires handling many fields"
)]
fn value_to_otel_metric_event(value: Value, metadata: EventMetadata) -> OtelMetric {
    use opentelemetry_proto::tonic::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint,
        Metric as OtelMetricProto, Sum, Summary, SummaryDataPoint, metric::Data as MetricData,
    };

    let map = match value {
        Value::Object(m) => m,
        _ => ObjectMap::new(),
    };

    let name = map
        .get("name")
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let description = map
        .get("description")
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let unit = map
        .get("unit")
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();

    let resource = map.get("resource").and_then(value_to_otel_resource);
    let scope = map.get("scope").and_then(value_to_otel_scope);

    // Reconstruct .data from the Value representation
    let data = map.get("data").and_then(|d| {
        let data_map = d.as_object()?;
        let data_type = data_map
            .get("type")
            .and_then(|v| v.as_bytes())
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default();

        match data_type.as_str() {
            "sum" => {
                let sum_map = data_map.get("sum")?.as_object()?;
                let is_monotonic = sum_map
                    .get("is_monotonic")
                    .and_then(vrl::value::Value::as_boolean)
                    .unwrap_or(false);
                let aggregation_temporality = OtlpEnumField::from_vrl(
                    sum_map
                        .get("aggregation_temporality")
                        .and_then(vrl::value::Value::as_integer)
                        .unwrap_or(0),
                )
                .as_proto();
                let data_points = value_to_number_data_points(sum_map.get("data_points"));
                Some(MetricData::Sum(Sum {
                    data_points,
                    aggregation_temporality,
                    is_monotonic,
                }))
            }
            "gauge" => {
                let gauge_map = data_map.get("gauge")?.as_object()?;
                let data_points = value_to_number_data_points(gauge_map.get("data_points"));
                Some(MetricData::Gauge(Gauge { data_points }))
            }
            "histogram" => {
                let h_map = data_map.get("histogram")?.as_object()?;
                let aggregation_temporality = OtlpEnumField::from_vrl(
                    h_map
                        .get("aggregation_temporality")
                        .and_then(vrl::value::Value::as_integer)
                        .unwrap_or(0),
                )
                .as_proto();
                let data_points = h_map
                    .get("data_points")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|dp_val| {
                                let m = dp_val.as_object()?;
                                let attributes = m
                                    .get("attributes")
                                    .and_then(|v| v.as_object())
                                    .map(object_map_to_otel_kvlist)
                                    .unwrap_or_default();
                                let count =
                                    m.get("count")
                                        .and_then(vrl::value::Value::as_integer)
                                        .unwrap_or(0) as u64;
                                let sum = m
                                    .get("sum")
                                    .and_then(vrl::value::Value::as_float)
                                    .map(ordered_float::NotNan::into_inner);
                                let bucket_counts = m
                                    .get("bucket_counts")
                                    .and_then(|v| v.as_array())
                                    .map(|a| {
                                        a.iter()
                                            .filter_map(|v| v.as_integer().map(|i| i as u64))
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                let explicit_bounds = m
                                    .get("explicit_bounds")
                                    .and_then(|v| v.as_array())
                                    .map(|a| {
                                        a.iter()
                                            .filter_map(|v| {
                                                v.as_float().map(ordered_float::NotNan::into_inner)
                                            })
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                let time_unix_nano =
                                    m.get("time_unix_nano")
                                        .and_then(vrl::value::Value::as_integer)
                                        .unwrap_or(0) as u64;
                                Some(HistogramDataPoint {
                                    attributes,
                                    count,
                                    sum,
                                    bucket_counts,
                                    explicit_bounds,
                                    time_unix_nano,
                                    ..Default::default()
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some(MetricData::Histogram(Histogram {
                    data_points,
                    aggregation_temporality,
                }))
            }
            "exponential_histogram" => {
                let eh_map = data_map.get("exponential_histogram")?.as_object()?;
                let aggregation_temporality = OtlpEnumField::from_vrl(
                    eh_map
                        .get("aggregation_temporality")
                        .and_then(vrl::value::Value::as_integer)
                        .unwrap_or(0),
                )
                .as_proto();
                let data_points = eh_map
                    .get("data_points")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|dp_val| {
                                let m = dp_val.as_object()?;
                                let attributes = m
                                    .get("attributes")
                                    .and_then(|v| v.as_object())
                                    .map(object_map_to_otel_kvlist)
                                    .unwrap_or_default();
                                let count =
                                    m.get("count")
                                        .and_then(vrl::value::Value::as_integer)
                                        .unwrap_or(0) as u64;
                                let sum = m
                                    .get("sum")
                                    .and_then(vrl::value::Value::as_float)
                                    .map(ordered_float::NotNan::into_inner);
                                let scale = OtlpEnumField::from_vrl(
                                    m.get("scale")
                                        .and_then(vrl::value::Value::as_integer)
                                        .unwrap_or(0),
                                )
                                .as_proto();
                                let zero_count = OtlpTimestamp::from_vrl(
                                    m.get("zero_count")
                                        .and_then(vrl::value::Value::as_integer)
                                        .unwrap_or(0),
                                )
                                .as_nanos();
                                let time_unix_nano =
                                    m.get("time_unix_nano")
                                        .and_then(vrl::value::Value::as_integer)
                                        .unwrap_or(0) as u64;
                                Some(ExponentialHistogramDataPoint {
                                    attributes,
                                    count,
                                    sum,
                                    scale,
                                    zero_count,
                                    time_unix_nano,
                                    ..Default::default()
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some(MetricData::ExponentialHistogram(ExponentialHistogram {
                    data_points,
                    aggregation_temporality,
                }))
            }
            "summary" => {
                let s_map = data_map.get("summary")?.as_object()?;
                let data_points = s_map
                    .get("data_points")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|dp_val| {
                                let m = dp_val.as_object()?;
                                let attributes = m
                                    .get("attributes")
                                    .and_then(|v| v.as_object())
                                    .map(object_map_to_otel_kvlist)
                                    .unwrap_or_default();
                                let count =
                                    m.get("count")
                                        .and_then(vrl::value::Value::as_integer)
                                        .unwrap_or(0) as u64;
                                let sum = m
                                    .get("sum")
                                    .and_then(vrl::value::Value::as_float)
                                    .map_or(0.0, ordered_float::NotNan::into_inner);
                                let time_unix_nano =
                                    m.get("time_unix_nano")
                                        .and_then(vrl::value::Value::as_integer)
                                        .unwrap_or(0) as u64;
                                Some(SummaryDataPoint {
                                    attributes,
                                    count,
                                    sum,
                                    time_unix_nano,
                                    ..Default::default()
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some(MetricData::Summary(Summary { data_points }))
            }
            _ => None,
        }
    });

    let metric_proto = OtelMetricProto {
        name,
        description,
        unit,
        data,
        metadata: Vec::new(),
    };

    // Reconstruct .kind and .namespace from legacy fields
    let kind = map
        .get("kind")
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned());

    let namespace = map
        .get("namespace")
        .and_then(|v| v.as_bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned());

    let mut otel_metric = OtelMetric::from_parts(metric_proto, resource, scope, metadata);

    if let Some(kind_str) = &kind {
        let metric_kind = match kind_str.as_str() {
            "incremental" => crate::event::MetricKind::Incremental,
            _ => crate::event::MetricKind::Absolute,
        };
        otel_metric.set_kind(metric_kind);
    }

    if let Some(ns) = &namespace {
        otel_metric.set_namespace(ns.clone());
    }

    // Apply only the DIFF from .attributes shorthand to data point attributes.
    // .attributes is projected from the first DP's attrs. If VRL modified it,
    // only apply changes (adds/updates/deletes), not pre-existing per-DP values.
    if let Some(attrs_val) = map.get("attributes")
        && let Some(attrs_map) = attrs_val.as_object()
    {
        let shorthand = OtelAttributes::from_object_map(attrs_map);
        let first_dp = otel_metric.first_dp_attrs().cloned().unwrap_or_default();

        for (key, val) in shorthand.iter() {
            if first_dp.get(key) != Some(val) {
                otel_metric.set_data_point_attribute(key.clone(), val.clone());
            }
        }

        for (key, _) in first_dp.iter() {
            if shorthand.get(key).is_none() {
                otel_metric.remove_data_point_attribute(key);
            }
        }
    }

    otel_metric
}

/// Convert Value back to `NumberDataPoint` array.
fn value_to_number_data_points(
    val: Option<&Value>,
) -> Vec<opentelemetry_proto::tonic::metrics::v1::NumberDataPoint> {
    use opentelemetry_proto::tonic::metrics::v1::{NumberDataPoint, number_data_point};
    let Some(arr) = val.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|dp_val| {
            let m = dp_val.as_object()?;
            let attributes = m
                .get("attributes")
                .and_then(|v| v.as_object())
                .map(object_map_to_otel_kvlist)
                .unwrap_or_default();
            let time_unix_nano = m
                .get("time_unix_nano")
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0) as u64;
            let start_time_unix_nano = m
                .get("start_time_unix_nano")
                .and_then(vrl::value::Value::as_integer)
                .unwrap_or(0) as u64;
            let value = if let Some(f) = m.get("value").and_then(vrl::value::Value::as_float) {
                Some(number_data_point::Value::AsDouble(f.into_inner()))
            } else {
                m.get("value")
                    .and_then(vrl::value::Value::as_integer)
                    .map(number_data_point::Value::AsInt)
            };
            Some(NumberDataPoint {
                attributes,
                time_unix_nano,
                start_time_unix_nano,
                value,
                ..Default::default()
            })
        })
        .collect()
}

/// An adapter to turn `Event`s into `vrl_lib::Target`s.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum VrlTarget {
    OtelLog(Value, EventMetadata, Option<Event>),
    OtelSpan(Value, EventMetadata, Option<Event>),
    OtelMetric(Value, EventMetadata, Option<Event>),
}

#[expect(
    clippy::large_enum_variant,
    reason = "Event variant is inherently large; boxing would add allocation overhead"
)]
pub enum TargetEvents {
    One(Event),
    OtelLogs(TargetIter<OtelLog>),
    OtelSpans(TargetIter<OtelSpan>),
}

pub struct TargetIter<T> {
    iter: std::vec::IntoIter<Value>,
    metadata: EventMetadata,
    _marker: PhantomData<T>,
}

impl Iterator for TargetIter<OtelLog> {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|v| Event::Log(value_to_otel_log_event(v, self.metadata.clone())))
    }
}

impl Iterator for TargetIter<OtelSpan> {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|v| Event::Trace(value_to_otel_span_event(v, self.metadata.clone())))
    }
}

impl VrlTarget {
    pub fn new(event: Event, _info: &ProgramInfo, _multi_value_metric_tags: bool) -> Self {
        match &event {
            Event::Log(log) => {
                let metadata = log.metadata().clone();
                let value = otel_log_event_to_value(log);
                VrlTarget::OtelLog(value, metadata, Some(event))
            }
            Event::Trace(span) => {
                let metadata = span.metadata().clone();
                let value = otel_span_event_to_value(span);
                VrlTarget::OtelSpan(value, metadata, Some(event))
            }
            Event::Metric(metric) => {
                let metadata = metric.metadata().clone();
                let value = otel_metric_event_to_value(metric);
                VrlTarget::OtelMetric(value, metadata, Some(event))
            }
        }
    }

    /// Modifies a schema in the same way that the `into_events` function modifies the event
    pub fn modify_schema_definition_for_into_events(input: Definition) -> Definition {
        merge_array_definitions(input)
    }

    /// Turn the target back into events.
    ///
    /// This returns an iterator of events as one event can be turned into multiple by assigning an
    /// array to `.` in VRL.
    pub fn into_events(self) -> TargetEvents {
        match self {
            VrlTarget::OtelLog(value, metadata, original) => {
                if let Some(mut orig) = original {
                    *orig.metadata_mut() = metadata;
                    return TargetEvents::One(orig);
                }
                match value {
                    Value::Array(values) => TargetEvents::OtelLogs(TargetIter {
                        iter: values.into_iter(),
                        metadata,
                        _marker: PhantomData,
                    }),
                    value @ Value::Object(_) | value => {
                        TargetEvents::One(Event::Log(value_to_otel_log_event(value, metadata)))
                    }
                }
            }
            VrlTarget::OtelSpan(value, metadata, original) => {
                if let Some(mut orig) = original {
                    *orig.metadata_mut() = metadata;
                    return TargetEvents::One(orig);
                }
                match value {
                    Value::Array(values) => TargetEvents::OtelSpans(TargetIter {
                        iter: values.into_iter(),
                        metadata,
                        _marker: PhantomData,
                    }),
                    value @ Value::Object(_) | value => {
                        TargetEvents::One(Event::Trace(value_to_otel_span_event(value, metadata)))
                    }
                }
            }
            VrlTarget::OtelMetric(value, metadata, original) => {
                if let Some(mut orig) = original {
                    *orig.metadata_mut() = metadata;
                    return TargetEvents::One(orig);
                }
                TargetEvents::One(Event::Metric(value_to_otel_metric_event(value, metadata)))
            }
        }
    }

    fn metadata(&self) -> &EventMetadata {
        match self {
            VrlTarget::OtelLog(_, metadata, _)
            | VrlTarget::OtelSpan(_, metadata, _)
            | VrlTarget::OtelMetric(_, metadata, _) => metadata,
        }
    }

    fn metadata_mut(&mut self) -> &mut EventMetadata {
        match self {
            VrlTarget::OtelLog(_, metadata, _)
            | VrlTarget::OtelSpan(_, metadata, _)
            | VrlTarget::OtelMetric(_, metadata, _) => metadata,
        }
    }
}

/// If the VRL returns a value that is not an array (see [`merge_array_definitions`]),
/// or an object, that data is moved into the `message` field.
#[cfg(test)]
fn move_field_definitions_into_message(mut definition: Definition) -> Definition {
    let mut message = definition.event_kind().clone();
    message.remove_object();
    message.remove_array();

    if !message.is_never() {
        let message_key = owned_value_path!("body");
        // We need to add the given message type to a field called `body`
        // in the event.
        let message = Kind::object(Collection::from(BTreeMap::from([(
            message_key.to_string().into(),
            message,
        )])));

        definition.event_kind_mut().remove_bytes();
        definition.event_kind_mut().remove_integer();
        definition.event_kind_mut().remove_float();
        definition.event_kind_mut().remove_boolean();
        definition.event_kind_mut().remove_timestamp();
        definition.event_kind_mut().remove_regex();
        definition.event_kind_mut().remove_null();

        *definition.event_kind_mut() = definition.event_kind().union(message);
    }

    definition
}

/// If the transform returns an array, the elements of this array will be separated
/// out into it's individual elements and passed downstream.
///
/// The potential types that the transform can output are any of the arrays
/// elements or any non-array elements that are within the definition. All these
/// definitions need to be merged together.
fn merge_array_definitions(mut definition: Definition) -> Definition {
    if let Some(array) = definition.event_kind().as_array() {
        let array_kinds = array.reduced_kind();

        let kind = definition.event_kind_mut();
        kind.remove_array();
        *kind = kind.union(array_kinds);
    }

    definition
}

impl Target for VrlTarget {
    fn target_insert(&mut self, target_path: &OwnedTargetPath, value: Value) -> Result<(), String> {
        match target_path.prefix {
            PathPrefix::Event => match self {
                VrlTarget::OtelLog(log, _, orig)
                | VrlTarget::OtelSpan(log, _, orig)
                | VrlTarget::OtelMetric(log, _, orig) => {
                    *orig = None;
                    log.insert(&target_path.path, value);
                    Ok(())
                }
            },
            PathPrefix::Metadata => {
                self.metadata_mut()
                    .value_mut()
                    .insert(&target_path.path, value);
                Ok(())
            }
        }
    }

    #[allow(clippy::redundant_closure_for_method_calls)] // false positive
    fn target_get(&self, target_path: &OwnedTargetPath) -> Result<Option<&Value>, String> {
        match target_path.prefix {
            PathPrefix::Event => match self {
                VrlTarget::OtelLog(value, _, _)
                | VrlTarget::OtelSpan(value, _, _)
                | VrlTarget::OtelMetric(value, _, _) => Ok(value.get(&target_path.path)),
            },
            PathPrefix::Metadata => Ok(self.metadata().value().get(&target_path.path)),
        }
    }

    fn target_get_mut(
        &mut self,
        target_path: &OwnedTargetPath,
    ) -> Result<Option<&mut Value>, String> {
        match target_path.prefix {
            PathPrefix::Event => match self {
                VrlTarget::OtelLog(value, _, orig)
                | VrlTarget::OtelSpan(value, _, orig)
                | VrlTarget::OtelMetric(value, _, orig) => {
                    *orig = None;
                    Ok(value.get_mut(&target_path.path))
                }
            },
            PathPrefix::Metadata => Ok(self.metadata_mut().value_mut().get_mut(&target_path.path)),
        }
    }

    fn target_remove(
        &mut self,
        target_path: &OwnedTargetPath,
        compact: bool,
    ) -> Result<Option<vrl::value::Value>, String> {
        match target_path.prefix {
            PathPrefix::Event => match self {
                VrlTarget::OtelLog(value, _, orig)
                | VrlTarget::OtelSpan(value, _, orig)
                | VrlTarget::OtelMetric(value, _, orig) => {
                    *orig = None;
                    Ok(value.remove(&target_path.path, compact))
                }
            },
            PathPrefix::Metadata => Ok(self
                .metadata_mut()
                .value_mut()
                .remove(&target_path.path, compact)),
        }
    }
}

impl SecretTarget for VrlTarget {
    fn get_secret(&self, key: &str) -> Option<&str> {
        self.metadata().secrets().get_secret(key)
    }

    fn insert_secret(&mut self, key: &str, value: &str) {
        self.metadata_mut().secrets_mut().insert_secret(key, value);
    }

    fn remove_secret(&mut self, key: &str) {
        self.metadata_mut().secrets_mut().remove_secret(key);
    }
}

#[cfg(test)]
mod test {
    use lookup::{OwnedValuePath, owned_value_path};
    use similar_asserts::assert_eq;
    use vrl::{btreemap, value::kind::Index};

    use super::{super::OtelMetric, *};
    #[test]
    fn test_field_definitions_in_message() {
        let definition = Definition::new_with_default_metadata(Kind::bytes());
        assert_eq!(
            Definition::new_with_default_metadata(Kind::object(BTreeMap::from([(
                "body".into(),
                Kind::bytes()
            )]))),
            move_field_definitions_into_message(definition)
        );

        // Test when a body field already exists.
        let definition = Definition::new_with_default_metadata(
            Kind::object(BTreeMap::from([("body".into(), Kind::integer())])).or_bytes(),
        );
        assert_eq!(
            Definition::new_with_default_metadata(Kind::object(BTreeMap::from([(
                "body".into(),
                Kind::bytes().or_integer()
            )]))),
            move_field_definitions_into_message(definition)
        );
    }

    #[test]
    fn test_merged_array_definitions_simple() {
        // Test merging the array definitions where the schema definition
        // is simple, containing only one possible type in the array.
        let object: BTreeMap<vrl::value::kind::Field, Kind> = [
            ("carrot".into(), Kind::bytes()),
            ("potato".into(), Kind::integer()),
        ]
        .into();

        let kind = Kind::array(Collection::from_unknown(Kind::object(object)));

        let definition = Definition::new_with_default_metadata(kind);

        let kind = Kind::object(BTreeMap::from([
            ("carrot".into(), Kind::bytes()),
            ("potato".into(), Kind::integer()),
        ]));

        let wanted = Definition::new_with_default_metadata(kind);
        let merged = merge_array_definitions(definition);

        assert_eq!(wanted, merged);
    }

    #[test]
    fn test_merged_array_definitions_complex() {
        // Test merging the array definitions where the schema definition
        // is fairly complex containing multiple different possible types.
        let object: BTreeMap<vrl::value::kind::Field, Kind> = [
            ("carrot".into(), Kind::bytes()),
            ("potato".into(), Kind::integer()),
        ]
        .into();

        let array: BTreeMap<Index, Kind> = [
            (Index::from(0), Kind::integer()),
            (Index::from(1), Kind::boolean()),
            (
                Index::from(2),
                Kind::object(BTreeMap::from([("peas".into(), Kind::bytes())])),
            ),
        ]
        .into();

        let mut kind = Kind::bytes();
        kind.add_object(object);
        kind.add_array(array);

        let definition = Definition::new_with_default_metadata(kind);

        let mut kind = Kind::bytes();
        kind.add_integer();
        kind.add_boolean();
        kind.add_object(BTreeMap::from([
            ("carrot".into(), Kind::bytes().or_undefined()),
            ("potato".into(), Kind::integer().or_undefined()),
            ("peas".into(), Kind::bytes().or_undefined()),
        ]));

        let wanted = Definition::new_with_default_metadata(kind);
        let merged = merge_array_definitions(definition);

        assert_eq!(wanted, merged);
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn log_insert() {
        let cases = vec![
            (
                BTreeMap::from([("foo".into(), "bar".into())]),
                owned_value_path!(0),
                btreemap! { "baz" => "qux" }.into(),
                btreemap! { "baz" => "qux" },
                Ok(()),
            ),
            (
                BTreeMap::from([("foo".into(), "bar".into())]),
                owned_value_path!("foo"),
                "baz".into(),
                btreemap! { "foo" => "baz" },
                Ok(()),
            ),
            (
                BTreeMap::from([("foo".into(), "bar".into())]),
                owned_value_path!("foo", 2, "bar baz", "a", "b"),
                true.into(),
                btreemap! {
                    "foo" => vec![
                        Value::Null,
                        Value::Null,
                        btreemap! {
                            "bar baz" => btreemap! { "a" => btreemap! { "b" => true } },
                        }.into()
                    ]
                },
                Ok(()),
            ),
            (
                btreemap! { "foo" => vec![0, 1, 2] },
                owned_value_path!("foo", 5),
                "baz".into(),
                btreemap! {
                    "foo" => vec![
                        0.into(),
                        1.into(),
                        2.into(),
                        Value::Null,
                        Value::Null,
                        Value::from("baz"),
                    ],
                },
                Ok(()),
            ),
            (
                BTreeMap::from([("foo".into(), "bar".into())]),
                owned_value_path!("foo", 0),
                "baz".into(),
                btreemap! { "foo" => vec!["baz"] },
                Ok(()),
            ),
            (
                btreemap! { "foo" => Value::Array(vec![]) },
                owned_value_path!("foo", 0),
                "baz".into(),
                btreemap! { "foo" => vec!["baz"] },
                Ok(()),
            ),
            (
                btreemap! { "foo" => Value::Array(vec![0.into()]) },
                owned_value_path!("foo", 0),
                "baz".into(),
                btreemap! { "foo" => vec!["baz"] },
                Ok(()),
            ),
            (
                btreemap! { "foo" => Value::Array(vec![0.into(), 1.into()]) },
                owned_value_path!("foo", 0),
                "baz".into(),
                btreemap! { "foo" => Value::Array(vec!["baz".into(), 1.into()]) },
                Ok(()),
            ),
            (
                btreemap! { "foo" => Value::Array(vec![0.into(), 1.into()]) },
                owned_value_path!("foo", 1),
                "baz".into(),
                btreemap! { "foo" => Value::Array(vec![0.into(), "baz".into()]) },
                Ok(()),
            ),
        ];

        for (object, path, value, expect, result) in cases {
            let object: ObjectMap = object;
            let info = ProgramInfo {
                fallible: false,
                abortable: false,
                target_queries: vec![],
                target_assignments: vec![],
            };
            let mut target = VrlTarget::new(
                Event::Log(OtelLog::from(Value::Object(object))),
                &info,
                false,
            );
            let expect = OtelLog::from(Value::Object(expect));
            let value: Value = value;
            let path = OwnedTargetPath::event(path);

            assert_eq!(
                Target::target_insert(&mut target, &path, value.clone()),
                result
            );
            assert_eq!(
                Target::target_get(&target, &path).map(Option::<&Value>::cloned),
                Ok(Some(value))
            );
            assert_eq!(
                match target.into_events() {
                    TargetEvents::One(event) => vec![event],
                    TargetEvents::OtelLogs(events) => events.collect::<Vec<_>>(),
                    TargetEvents::OtelSpans(events) => events.collect::<Vec<_>>(),
                }
                .first()
                .cloned()
                .unwrap(),
                Event::Log(expect)
            );
        }
    }

    // -- OTel-native VrlTarget tests --

    fn make_info_with_queries(paths: &[&str]) -> ProgramInfo {
        ProgramInfo {
            fallible: false,
            abortable: false,
            target_queries: paths
                .iter()
                .map(|p| OwnedTargetPath::event(OwnedValuePath::try_from(p.to_string()).unwrap()))
                .collect(),
            target_assignments: vec![],
        }
    }

    fn make_empty_info() -> ProgramInfo {
        ProgramInfo {
            fallible: false,
            abortable: false,
            target_queries: vec![],
            target_assignments: vec![],
        }
    }

    #[test]
    fn otel_log_vrl_target_get_severity_text() {
        use opentelemetry_proto::tonic::logs::v1::LogRecord;
        let event = OtelLog::new(LogRecord {
            severity_text: "ERROR".to_string(),
            ..Default::default()
        });
        let info = make_empty_info();
        let target = VrlTarget::new(Event::Log(event), &info, false);

        let path = OwnedTargetPath::event(owned_value_path!("severity_text"));
        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(result, Some(&Value::Bytes("ERROR".into())));
    }

    #[test]
    fn otel_log_vrl_target_get_attribute() {
        use opentelemetry_proto::tonic::logs::v1::LogRecord;
        let event = OtelLog::new(LogRecord {
            attributes: vec![OtelKeyValue {
                key: "host.name".to_string(),
                value: Some(OtelAnyValue {
                    value: Some(OtelValueKind::StringValue("myhost".to_string())),
                }),
            }],
            ..Default::default()
        });
        let info = make_empty_info();
        let target = VrlTarget::new(Event::Log(event), &info, false);

        let path = OwnedTargetPath::event(owned_value_path!("attributes", "host.name"));
        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(result, Some(&Value::Bytes("myhost".into())));
    }

    #[test]
    fn otel_log_vrl_target_insert_attribute() {
        use opentelemetry_proto::tonic::logs::v1::LogRecord;
        let event = OtelLog::new(LogRecord::default());
        let info = make_empty_info();
        let mut target = VrlTarget::new(Event::Log(event), &info, false);

        let path = OwnedTargetPath::event(owned_value_path!("attributes", "host.name"));
        Target::target_insert(&mut target, &path, Value::Bytes("myhost".into())).unwrap();

        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(result, Some(&Value::Bytes("myhost".into())));
    }

    #[test]
    fn otel_log_vrl_target_roundtrip_into_events() {
        use opentelemetry_proto::tonic::logs::v1::LogRecord;
        let event = OtelLog::new(LogRecord {
            severity_text: "INFO".to_string(),
            severity_number: 9,
            time_unix_nano: 1_234_567_890,
            attributes: vec![OtelKeyValue {
                key: "service".to_string(),
                value: Some(OtelAnyValue {
                    value: Some(OtelValueKind::StringValue("web".to_string())),
                }),
            }],
            ..Default::default()
        });
        let info = make_empty_info();
        let target = VrlTarget::new(Event::Log(event), &info, false);

        let events: Vec<Event> = match target.into_events() {
            TargetEvents::One(e) => vec![e],
            _ => panic!("expected one event"),
        };
        assert_eq!(events.len(), 1);

        let Event::Log(restored) = &events[0] else {
            panic!("expected OtelLog")
        };
        assert_eq!(restored.severity_text(), "INFO");
        assert_eq!(restored.severity_number(), 9);
        assert_eq!(restored.time_unix_nano(), 1_234_567_890);
        assert_eq!(restored.attributes().len(), 1);
        assert!(restored.attributes().get("service").is_some());
    }

    #[test]
    fn otel_log_vrl_target_mutate_and_roundtrip() {
        use opentelemetry_proto::tonic::logs::v1::LogRecord;
        let event = OtelLog::new(LogRecord {
            severity_text: "DEBUG".to_string(),
            ..Default::default()
        });
        let info = make_empty_info();
        let mut target = VrlTarget::new(Event::Log(event), &info, false);

        let path = OwnedTargetPath::event(owned_value_path!("severity_text"));
        Target::target_insert(&mut target, &path, Value::Bytes("WARN".into())).unwrap();

        let events: Vec<Event> = match target.into_events() {
            TargetEvents::One(e) => vec![e],
            _ => panic!("expected one event"),
        };

        let Event::Log(restored) = &events[0] else {
            panic!("expected OtelLog")
        };
        assert_eq!(restored.severity_text(), "WARN");
    }

    #[test]
    fn otel_span_vrl_target_get_name() {
        use opentelemetry_proto::tonic::trace::v1::Span;
        let event = OtelSpan::new(Span {
            name: "my-span".to_string(),
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            ..Default::default()
        });
        let info = make_empty_info();
        let target = VrlTarget::new(Event::Trace(event), &info, false);

        let path = OwnedTargetPath::event(owned_value_path!("name"));
        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(result, Some(&Value::Bytes("my-span".into())));

        let path = OwnedTargetPath::event(owned_value_path!("trace_id"));
        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(
            result,
            Some(&Value::Bytes("0102030405060708090a0b0c0d0e0f10".into()))
        );
    }

    #[test]
    fn otel_span_vrl_target_roundtrip() {
        use opentelemetry_proto::tonic::trace::v1::Span;
        let event = OtelSpan::new(Span {
            name: "test-span".to_string(),
            kind: 2,
            start_time_unix_nano: 1000,
            end_time_unix_nano: 2000,
            ..Default::default()
        });
        let info = make_empty_info();
        let target = VrlTarget::new(Event::Trace(event), &info, false);

        let events: Vec<Event> = match target.into_events() {
            TargetEvents::One(e) => vec![e],
            _ => panic!("expected one event"),
        };

        let Event::Trace(restored) = &events[0] else {
            panic!("expected OtelSpan")
        };
        assert_eq!(restored.name(), "test-span");
        assert_eq!(restored.kind(), 2);
        assert_eq!(restored.start_time_unix_nano(), 1000);
        assert_eq!(restored.end_time_unix_nano(), 2000);
    }

    #[test]
    fn otel_metric_vrl_target_get_name() {
        use opentelemetry_proto::tonic::metrics::v1::Metric as OtelMetricProto;
        let event = OtelMetric::new(OtelMetricProto {
            name: "http.duration".to_string(),
            description: "request duration".to_string(),
            unit: "ms".to_string(),
            ..Default::default()
        });
        let info = make_info_with_queries(&[".name", ".description", ".unit"]);
        let target = VrlTarget::new(Event::Metric(event), &info, false);

        let path = OwnedTargetPath::event(owned_value_path!("name"));
        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(result, Some(&Value::Bytes("http.duration".into())));

        let path = OwnedTargetPath::event(owned_value_path!("unit"));
        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(result, Some(&Value::Bytes("ms".into())));
    }

    #[test]
    fn otel_metric_vrl_target_set_name() {
        use opentelemetry_proto::tonic::metrics::v1::Metric as OtelMetricProto;
        let event = OtelMetric::new(OtelMetricProto {
            name: "old.name".to_string(),
            ..Default::default()
        });
        let info = make_info_with_queries(&[".name"]);
        let mut target = VrlTarget::new(Event::Metric(event), &info, false);

        let path = OwnedTargetPath::event(owned_value_path!("name"));
        Target::target_insert(&mut target, &path, Value::Bytes("new.name".into())).unwrap();

        let events: Vec<Event> = match target.into_events() {
            TargetEvents::One(e) => vec![e],
            _ => panic!("expected one event"),
        };

        let Event::Metric(restored) = &events[0] else {
            panic!("expected OtelMetric")
        };
        assert_eq!(restored.name(), "new.name");
    }

    #[test]
    fn otel_log_resource_and_scope_roundtrip() {
        use opentelemetry_proto::tonic::logs::v1::LogRecord;
        let mut event = OtelLog::new(LogRecord::default());
        event.set_resource(OtelResource {
            attributes: vec![OtelKeyValue {
                key: "service.name".to_string(),
                value: Some(OtelAnyValue {
                    value: Some(OtelValueKind::StringValue("my-svc".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
        });
        event.set_scope(OtelScope {
            name: "my-lib".to_string(),
            version: "1.0".to_string(),
            ..Default::default()
        });

        let info = make_empty_info();
        let target = VrlTarget::new(Event::Log(event), &info, false);

        let path =
            OwnedTargetPath::event(owned_value_path!("resource", "attributes", "service.name"));
        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(result, Some(&Value::Bytes("my-svc".into())));

        let path = OwnedTargetPath::event(owned_value_path!("scope", "name"));
        let result = Target::target_get(&target, &path).unwrap();
        assert_eq!(result, Some(&Value::Bytes("my-lib".into())));

        let events: Vec<Event> = match target.into_events() {
            TargetEvents::One(e) => vec![e],
            _ => panic!("expected one event"),
        };

        let Event::Log(restored) = &events[0] else {
            panic!("expected OtelLog")
        };
        assert!(restored.resource().is_some());
        assert_eq!(
            restored.resource_attribute("service.name").and_then(|v| {
                match &v.value {
                    Some(OtelValueKind::StringValue(s)) => Some(s.as_str()),
                    _ => None,
                }
            }),
            Some("my-svc")
        );
        assert_eq!(restored.scope().unwrap().name, "my-lib");
        assert_eq!(restored.scope().unwrap().version, "1.0");
    }

    #[test]
    fn otel_any_value_conversion_roundtrip() {
        let cases = vec![
            OtelAnyValue {
                value: Some(OtelValueKind::StringValue("hello".to_string())),
            },
            OtelAnyValue {
                value: Some(OtelValueKind::BoolValue(true)),
            },
            OtelAnyValue {
                value: Some(OtelValueKind::IntValue(42)),
            },
            OtelAnyValue {
                value: Some(OtelValueKind::DoubleValue(std::f64::consts::PI)),
            },
            OtelAnyValue {
                value: Some(OtelValueKind::BytesValue(vec![1, 2, 3])),
            },
            OtelAnyValue { value: None },
        ];

        for original in &cases {
            let vrl_val = otel_any_value_to_vrl(original);
            let restored = vrl_value_to_otel_any_value(&vrl_val);
            match (&original.value, &restored.value) {
                (Some(OtelValueKind::BytesValue(a)), Some(OtelValueKind::StringValue(b))) => {
                    // bytes -> string lossy conversion is expected
                    assert_eq!(String::from_utf8_lossy(a).as_ref(), b.as_str());
                }
                (None, None) => {}
                _ => assert_eq!(original, &restored, "roundtrip failed for {original:?}"),
            }
        }
    }

    #[test]
    fn hex_encode_decode_roundtrip() {
        let original = vec![0x01, 0x23, 0xab, 0xcd, 0xef, 0x00, 0xff];
        let encoded = hex_encode_bytes(&original);
        assert_eq!(encoded, Value::Bytes("0123abcdef00ff".into()));
        let decoded = hex_decode_value(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn otel_metric_promote_resource_attrs_to_dp_attrs() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric as OtelMetricProto, NumberDataPoint, Sum, number_data_point::Value as NDPValue,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource as OtelResource;

        let mut event = OtelMetric::new(OtelMetricProto {
            name: "http.requests".to_string(),
            data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(
                Sum {
                    data_points: vec![NumberDataPoint {
                        value: Some(NDPValue::AsInt(42)),
                        ..Default::default()
                    }],
                    aggregation_temporality: 2,
                    is_monotonic: true,
                },
            )),
            ..Default::default()
        });
        event.set_resource(OtelResource {
            attributes: vec![
                OtelKeyValue {
                    key: "service.name".to_string(),
                    value: Some(OtelAnyValue {
                        value: Some(OtelValueKind::StringValue("my-dotnet-app".to_string())),
                    }),
                },
                OtelKeyValue {
                    key: "host.name".to_string(),
                    value: Some(OtelAnyValue {
                        value: Some(OtelValueKind::StringValue("server-1".to_string())),
                    }),
                },
            ],
            dropped_attributes_count: 0,
        });

        let info = make_empty_info();
        let mut target = VrlTarget::new(Event::Metric(event), &info, false);

        // Read .resource.attributes."service.name"
        let read_path =
            OwnedTargetPath::event(owned_value_path!("resource", "attributes", "service.name"));
        let svc_name = Target::target_get(&target, &read_path).unwrap().cloned();
        assert_eq!(
            svc_name,
            Some(Value::Bytes("my-dotnet-app".into())),
            "should read resource attribute service.name"
        );

        // Write .attributes."service.name" = .resource.attributes."service.name"
        let write_path = OwnedTargetPath::event(owned_value_path!("attributes", "service.name"));
        Target::target_insert(&mut target, &write_path, svc_name.unwrap()).unwrap();

        // Read .resource.attributes."host.name"
        let read_path2 =
            OwnedTargetPath::event(owned_value_path!("resource", "attributes", "host.name"));
        let host_name = Target::target_get(&target, &read_path2).unwrap().cloned();
        assert_eq!(host_name, Some(Value::Bytes("server-1".into())));

        // Write .attributes."host.name" = .resource.attributes."host.name"
        let write_path2 = OwnedTargetPath::event(owned_value_path!("attributes", "host.name"));
        Target::target_insert(&mut target, &write_path2, host_name.unwrap()).unwrap();

        // Convert back to event
        let events: Vec<Event> = match target.into_events() {
            TargetEvents::One(e) => vec![e],
            _ => panic!("expected one event"),
        };

        let Event::Metric(restored) = &events[0] else {
            panic!("expected OtelMetric")
        };

        // Verify data point attributes contain the promoted resource attributes
        let dp_attrs = restored.first_dp_attrs().expect("should have dp attrs");
        let svc_attr = dp_attrs.get("service.name");
        assert!(
            svc_attr.is_some(),
            "data point should have service.name attribute, got: {dp_attrs:?}"
        );
        let host_attr = dp_attrs.get("host.name");
        assert!(
            host_attr.is_some(),
            "data point should have host.name attribute, got: {dp_attrs:?}"
        );

        // Verify resource attributes are still present
        let resource = restored
            .resource_proto()
            .expect("resource should still exist");
        let svc_resource_attr = resource
            .attributes
            .iter()
            .find(|kv| kv.key == "service.name");
        assert!(
            svc_resource_attr.is_some(),
            "resource should still have service.name"
        );
    }

    #[test]
    fn multi_dp_attributes_preserved_when_vrl_adds_attribute() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric as OtelMetricProto, NumberDataPoint, Sum, metric::Data as MetricData,
            number_data_point,
        };

        fn str_val(attrs: &super::OtelAttributes, key: &str) -> String {
            match attrs.get(key).and_then(|v| v.value.as_ref()) {
                Some(V::StringValue(s)) => s.clone(),
                other => panic!("expected string for {key}, got {other:?}"),
            }
        }

        let dp1 = NumberDataPoint {
            attributes: vec![
                KeyValue {
                    key: "method".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("GET".into())),
                    }),
                },
                KeyValue {
                    key: "status_code".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("200".into())),
                    }),
                },
            ],
            value: Some(number_data_point::Value::AsDouble(10.0)),
            time_unix_nano: 1000,
            ..Default::default()
        };
        let dp2 = NumberDataPoint {
            attributes: vec![
                KeyValue {
                    key: "method".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("POST".into())),
                    }),
                },
                KeyValue {
                    key: "status_code".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("404".into())),
                    }),
                },
            ],
            value: Some(number_data_point::Value::AsDouble(5.0)),
            time_unix_nano: 2000,
            ..Default::default()
        };

        let metric_proto = OtelMetricProto {
            name: "http_requests".into(),
            data: Some(MetricData::Sum(Sum {
                data_points: vec![dp1, dp2],
                aggregation_temporality: 2,
                is_monotonic: true,
            })),
            ..Default::default()
        };

        let otel_metric =
            OtelMetric::from_parts(metric_proto, None, None, EventMetadata::default());
        let mut value = otel_metric_event_to_value(&otel_metric);

        // Simulate VRL: .attributes."service.name" = "myapp"
        if let Value::Object(ref mut map) = value
            && let Some(Value::Object(attrs)) = map.get_mut("attributes")
        {
            attrs.insert("service.name".into(), Value::Bytes("myapp".into()));
        }

        let restored = value_to_otel_metric_event(value, EventMetadata::default());

        assert_eq!(
            restored.dp_attrs.len(),
            2,
            "should still have 2 data points"
        );

        let dp1_attrs = &restored.dp_attrs[0];
        let dp2_attrs = &restored.dp_attrs[1];

        // Both DPs should have the new attribute
        assert_eq!(str_val(dp1_attrs, "service.name"), "myapp");
        assert_eq!(str_val(dp2_attrs, "service.name"), "myapp");

        // Each DP should preserve its own unique attributes
        assert_eq!(str_val(dp1_attrs, "method"), "GET");
        assert_eq!(str_val(dp1_attrs, "status_code"), "200");
        assert_eq!(str_val(dp2_attrs, "method"), "POST");
        assert_eq!(str_val(dp2_attrs, "status_code"), "404");
    }

    #[test]
    fn multi_dp_vrl_delete_attribute_removes_from_all_dps() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric as OtelMetricProto, NumberDataPoint, Sum, metric::Data as MetricData,
            number_data_point,
        };

        fn str_val(attrs: &super::OtelAttributes, key: &str) -> String {
            match attrs.get(key).and_then(|v| v.value.as_ref()) {
                Some(V::StringValue(s)) => s.clone(),
                other => panic!("expected string for {key}, got {other:?}"),
            }
        }

        let dp1 = NumberDataPoint {
            attributes: vec![
                KeyValue {
                    key: "method".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("GET".into())),
                    }),
                },
                KeyValue {
                    key: "env".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("prod".into())),
                    }),
                },
            ],
            value: Some(number_data_point::Value::AsDouble(10.0)),
            ..Default::default()
        };
        let dp2 = NumberDataPoint {
            attributes: vec![
                KeyValue {
                    key: "method".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("POST".into())),
                    }),
                },
                KeyValue {
                    key: "env".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("prod".into())),
                    }),
                },
            ],
            value: Some(number_data_point::Value::AsDouble(5.0)),
            ..Default::default()
        };

        let metric_proto = OtelMetricProto {
            name: "http_requests".into(),
            data: Some(MetricData::Sum(Sum {
                data_points: vec![dp1, dp2],
                aggregation_temporality: 2,
                is_monotonic: true,
            })),
            ..Default::default()
        };

        let otel_metric =
            OtelMetric::from_parts(metric_proto, None, None, EventMetadata::default());
        let mut value = otel_metric_event_to_value(&otel_metric);

        // Simulate VRL: del(.attributes.env)
        if let Value::Object(ref mut map) = value
            && let Some(Value::Object(attrs)) = map.get_mut("attributes")
        {
            attrs.remove("env");
        }

        let restored = value_to_otel_metric_event(value, EventMetadata::default());

        let dp1_attrs = &restored.dp_attrs[0];
        let dp2_attrs = &restored.dp_attrs[1];

        assert!(
            dp1_attrs.get("env").is_none(),
            "env should be removed from dp1"
        );
        assert!(
            dp2_attrs.get("env").is_none(),
            "env should be removed from dp2"
        );

        assert_eq!(str_val(dp1_attrs, "method"), "GET");
        assert_eq!(str_val(dp2_attrs, "method"), "POST");
    }

    #[test]
    fn multi_dp_vrl_no_change_preserves_all_attrs() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as V};
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric as OtelMetricProto, NumberDataPoint, Sum, metric::Data as MetricData,
            number_data_point,
        };

        fn str_val(attrs: &super::OtelAttributes, key: &str) -> String {
            match attrs.get(key).and_then(|v| v.value.as_ref()) {
                Some(V::StringValue(s)) => s.clone(),
                other => panic!("expected string for {key}, got {other:?}"),
            }
        }

        let dp1 = NumberDataPoint {
            attributes: vec![
                KeyValue {
                    key: "method".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("GET".into())),
                    }),
                },
                KeyValue {
                    key: "status_code".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("200".into())),
                    }),
                },
            ],
            value: Some(number_data_point::Value::AsDouble(10.0)),
            ..Default::default()
        };
        let dp2 = NumberDataPoint {
            attributes: vec![
                KeyValue {
                    key: "method".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("POST".into())),
                    }),
                },
                KeyValue {
                    key: "status_code".into(),
                    value: Some(AnyValue {
                        value: Some(V::StringValue("404".into())),
                    }),
                },
            ],
            value: Some(number_data_point::Value::AsDouble(5.0)),
            ..Default::default()
        };

        let metric_proto = OtelMetricProto {
            name: "http_requests".into(),
            data: Some(MetricData::Sum(Sum {
                data_points: vec![dp1, dp2],
                aggregation_temporality: 2,
                is_monotonic: true,
            })),
            ..Default::default()
        };

        let otel_metric =
            OtelMetric::from_parts(metric_proto, None, None, EventMetadata::default());
        // No VRL modification — just round-trip
        let value = otel_metric_event_to_value(&otel_metric);
        let restored = value_to_otel_metric_event(value, EventMetadata::default());

        assert_eq!(restored.dp_attrs.len(), 2);
        assert_eq!(str_val(&restored.dp_attrs[0], "method"), "GET");
        assert_eq!(str_val(&restored.dp_attrs[0], "status_code"), "200");
        assert_eq!(str_val(&restored.dp_attrs[1], "method"), "POST");
        assert_eq!(str_val(&restored.dp_attrs[1], "status_code"), "404");
    }
}
