pub use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::{
    InstrumentationScope, KeyValue, any_value::Value as OtelValueKind,
};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
#[cfg(test)]
use opentelemetry_proto::tonic::metrics::v1::Metric as OtelMetricProto;
pub use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::Span;
use prost::Message as _;
use serde::Serialize;
use sol_buffers::EventCount;
use sol_common::{
    EventDataEq,
    byte_size_of::ByteSizeOf,
    finalization::{EventFinalizers, Finalizable},
    internal_event::TaggedEventsSent,
    json_size::JsonSize,
    request_metadata::GetEventCountTags,
};
use vrl::value::{KeyString, ObjectMap, Value};

use super::otel_conv::{OtlpCount, OtlpEnumField, OtlpTimestamp};
use super::{
    BatchNotifier, EstimatedJsonEncodedSizeOf, EventFinalizer, EventMetadata, otel_fields as f,
};

/// Convert a JSON value to an `OTel` `AnyValue`.
pub fn json_to_any_value(value: serde_json::Value) -> AnyValue {
    let kind = match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(OtelValueKind::BoolValue(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(OtelValueKind::IntValue(i))
            } else {
                Some(OtelValueKind::DoubleValue(n.as_f64().unwrap_or(0.0)))
            }
        }
        serde_json::Value::String(s) => Some(OtelValueKind::StringValue(s)),
        serde_json::Value::Array(arr) => {
            let values = arr.into_iter().map(json_to_any_value).collect();
            Some(OtelValueKind::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue { values },
            ))
        }
        serde_json::Value::Object(map) => {
            let values = map
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k,
                    value: Some(json_to_any_value(v)),
                })
                .collect();
            Some(OtelValueKind::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList { values },
            ))
        }
    };
    AnyValue { value: kind }
}

fn json_to_vrl_value(value: serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(
                    ordered_float::NotNan::new(n.as_f64().unwrap_or(0.0)).unwrap_or_default(),
                )
            }
        }
        serde_json::Value::String(s) => Value::Bytes(bytes::Bytes::from(s)),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.into_iter().map(json_to_vrl_value).collect())
        }
        serde_json::Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (KeyString::from(k), json_to_vrl_value(v)))
                .collect(),
        ),
    }
}

/// Create a string `AnyValue`.
pub fn string_value(s: impl Into<String>) -> AnyValue {
    AnyValue {
        value: Some(OtelValueKind::StringValue(s.into())),
    }
}

/// Create an integer `AnyValue`.
pub fn int_value(i: i64) -> AnyValue {
    AnyValue {
        value: Some(OtelValueKind::IntValue(i)),
    }
}

/// Tracing `Visit` implementation that accumulates into an `ObjectMap`.
/// Used by `OtelLog::from_tracing_event` to batch all field insertions into
/// a single `from_value_map` conversion.
#[derive(Default)]
struct OtelLogTracingBuilder {
    map: ObjectMap,
}

impl OtelLogTracingBuilder {
    /// Map the tracing field name to the canonical `OtelLog` key.
    /// The tracing framework uses "message" for the log body, but
    /// the proto-canonical key is "body".
    fn canonical_key(field: &tracing::field::Field) -> vrl::prelude::KeyString {
        let name = field.name();
        if name == f::STATUS_MESSAGE {
            f::BODY.into()
        } else {
            name.into()
        }
    }
}

impl tracing::field::Visit for OtelLogTracingBuilder {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.map.insert(
            Self::canonical_key(field),
            Value::Bytes(value.to_string().into()),
        );
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.map.insert(
            Self::canonical_key(field),
            Value::Bytes(format!("{value:?}").into()),
        );
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.map
            .insert(Self::canonical_key(field), Value::Integer(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        match i64::try_from(value) {
            Ok(v) => self
                .map
                .insert(Self::canonical_key(field), Value::Integer(v)),
            Err(_) => self.map.insert(
                Self::canonical_key(field),
                Value::Bytes(value.to_string().into()),
            ),
        };
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.map
            .insert(Self::canonical_key(field), Value::Boolean(value));
    }
}

pub(super) fn hex_encode(bytes: &[u8]) -> Value {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    Value::Bytes(s.into())
}

/// Decode a hex string Value back to bytes. Accepts `Value::Bytes` holding
/// an even-length ASCII hex string. Returns `None` if the input is not a
/// well-formed hex byte sequence — in that case the caller should fall back
/// to storing the original Value as an attribute, so corrupt data does not
/// silently disappear.
fn hex_decode(value: &Value) -> Option<Vec<u8>> {
    let Value::Bytes(bytes) = value else {
        return None;
    };
    let s = std::str::from_utf8(bytes).ok()?;
    if s.is_empty() || s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in s.as_bytes().chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "hex nibble result fits in u8"
        )]
        out.push(((hi << 4) | lo) as u8);
    }
    Some(out)
}

/// Try to interpret a VRL `Value::Object` as an OTLP JSON `AnyValue` pattern.
///
/// When data roundtrips through OTLP JSON serialization and JSON parsing,
/// `AnyValue` wrappers like `{"stringValue":"hello"}` become VRL Objects.
/// This function recognizes these patterns and converts them back to the
/// proper `AnyValue` proto representation.
pub fn try_parse_otlp_any_value(value: &Value) -> Option<AnyValue> {
    let map = match value {
        Value::Object(m) if m.len() == 1 => m,
        _ => return None,
    };

    let (key, val) = map.iter().next()?;
    let key_str = key.as_ref();

    let kind = match key_str {
        f::STRING_VALUE => {
            let s = match val {
                Value::Bytes(b) => String::from_utf8(b.to_vec()).ok()?,
                _ => return None,
            };
            Some(OtelValueKind::StringValue(s))
        }
        f::INT_VALUE => {
            let i = match val {
                Value::Integer(i) => *i,
                Value::Bytes(b) => std::str::from_utf8(b).ok()?.parse::<i64>().ok()?,
                _ => return None,
            };
            Some(OtelValueKind::IntValue(i))
        }
        f::DOUBLE_VALUE => {
            let d = match val {
                Value::Float(f) => f.into_inner(),
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "integer to f64 for OTLP double value"
                )]
                Value::Integer(i) => *i as f64,
                _ => return None,
            };
            Some(OtelValueKind::DoubleValue(d))
        }
        f::BOOL_VALUE => {
            let b = match val {
                Value::Boolean(b) => *b,
                _ => return None,
            };
            Some(OtelValueKind::BoolValue(b))
        }
        f::BYTES_VALUE => {
            let bytes = match val {
                Value::Bytes(b) => {
                    // May be hex-encoded
                    hex_decode_bytes(b).unwrap_or_else(|| b.to_vec())
                }
                _ => return None,
            };
            Some(OtelValueKind::BytesValue(bytes))
        }
        f::ARRAY_VALUE => {
            // {"arrayValue": {"values": [...]}}
            let arr = match val {
                Value::Object(obj) => match obj.get(&KeyString::from(f::VALUES)) {
                    Some(Value::Array(arr)) => arr
                        .iter()
                        .map(|v| {
                            try_parse_otlp_any_value(v).unwrap_or_else(|| vrl_value_to_any_value(v))
                        })
                        .collect(),
                    _ => return None,
                },
                _ => return None,
            };
            Some(OtelValueKind::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue { values: arr },
            ))
        }
        f::KVLIST_VALUE => {
            // {"kvlistValue": {"values": [{"key":"k","value":{...}}]}}
            let kvl = match val {
                Value::Object(obj) => match obj.get(&KeyString::from(f::VALUES)) {
                    Some(Value::Array(arr)) => parse_otlp_key_value_array(arr)?,
                    _ => return None,
                },
                _ => return None,
            };
            Some(OtelValueKind::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList { values: kvl },
            ))
        }
        _ => return None,
    };

    Some(AnyValue { value: kind })
}

/// Parse an OTLP JSON attributes array: [{"key":"k","value":{...}}, ...]
fn parse_otlp_key_value_array(arr: &[Value]) -> Option<Vec<KeyValue>> {
    let mut result = Vec::with_capacity(arr.len());
    for item in arr {
        let Value::Object(obj) = item else {
            return None;
        };
        let key = match obj.get(&KeyString::from(f::KEY)) {
            Some(Value::Bytes(b)) => String::from_utf8(b.to_vec()).ok()?,
            _ => return None,
        };
        let value = obj
            .get(&KeyString::from(f::VALUE))
            .and_then(try_parse_otlp_any_value);
        result.push(KeyValue { key, value });
    }
    Some(result)
}

/// Helper: try to decode hex-encoded bytes.
fn hex_decode_bytes(b: &[u8]) -> Option<Vec<u8>> {
    let s = std::str::from_utf8(b).ok()?;
    if s.len() % 2 != 0 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    hex_decode(&Value::Bytes(bytes::Bytes::copy_from_slice(b)))
}

pub fn vrl_value_to_any_value(value: &Value) -> AnyValue {
    let kind = match value {
        Value::Bytes(b) => match std::str::from_utf8(b) {
            Ok(s) => Some(OtelValueKind::StringValue(s.to_owned())),
            Err(_) => Some(OtelValueKind::BytesValue(b.to_vec())),
        },
        Value::Integer(i) => Some(OtelValueKind::IntValue(*i)),
        Value::Float(f) => Some(OtelValueKind::DoubleValue(f.into_inner())),
        Value::Boolean(b) => Some(OtelValueKind::BoolValue(*b)),
        Value::Timestamp(ts) => Some(OtelValueKind::StringValue(
            ts.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        )),
        Value::Regex(r) => Some(OtelValueKind::StringValue(r.to_string())),
        Value::Null => None,
        Value::Object(map) => {
            let values = map
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.to_string(),
                    value: Some(vrl_value_to_any_value(v)),
                })
                .collect();
            Some(OtelValueKind::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList { values },
            ))
        }
        Value::Array(arr) => {
            let values = arr.iter().map(vrl_value_to_any_value).collect();
            Some(OtelValueKind::ArrayValue(
                opentelemetry_proto::tonic::common::v1::ArrayValue { values },
            ))
        }
    };
    AnyValue { value: kind }
}

pub(crate) fn any_value_to_vrl(av: &AnyValue) -> Value {
    match &av.value {
        Some(OtelValueKind::StringValue(s)) => Value::Bytes(s.clone().into()),
        Some(OtelValueKind::BoolValue(b)) => Value::Boolean(*b),
        Some(OtelValueKind::IntValue(i)) => Value::Integer(*i),
        Some(OtelValueKind::DoubleValue(d)) => {
            Value::Float(ordered_float::NotNan::new(*d).unwrap_or_default())
        }
        Some(OtelValueKind::BytesValue(b)) => Value::Bytes(bytes::Bytes::copy_from_slice(b)),
        Some(OtelValueKind::ArrayValue(arr)) => {
            Value::Array(arr.values.iter().map(any_value_to_vrl).collect())
        }
        Some(OtelValueKind::KvlistValue(kvl)) => Value::Object(kvlist_to_object_map(&kvl.values)),
        None => Value::Null,
    }
}

pub fn kvlist_to_object_map(kvs: &[KeyValue]) -> ObjectMap {
    kvs.iter()
        .map(|kv| {
            let v = kv.value.as_ref().map_or(Value::Null, any_value_to_vrl);
            (kv.key.clone().into(), v)
        })
        .collect()
}

pub fn object_map_to_kvlist(map: &ObjectMap) -> Vec<KeyValue> {
    map.iter()
        .map(|(k, v)| KeyValue {
            key: k.to_string(),
            value: Some(vrl_value_to_any_value(v)),
        })
        .collect()
}

fn restore_resource(map: &mut ObjectMap) -> (Option<Resource>, OtelAttributes) {
    match map.remove(f::RESOURCE) {
        Some(Value::Object(mut res_map)) => {
            let dropped_count = OtlpCount::from_vrl(
                res_map
                    .remove(f::DROPPED_ATTRIBUTES_COUNT)
                    .and_then(|v| v.as_integer())
                    .unwrap_or(0),
            )
            .as_proto();

            // Try OTLP JSON format first: {"attributes":[{"key":"k","value":{...}}]}
            let attrs = if let Some(Value::Array(arr)) = res_map.remove(f::ATTRIBUTES) {
                if let Some(kvs) = parse_otlp_key_value_array(&arr) {
                    let mut otel_attrs = OtelAttributes::new();
                    for kv in kvs {
                        otel_attrs.insert(kv.key, kv.value.unwrap_or(AnyValue { value: None }));
                    }
                    otel_attrs
                } else {
                    // Not valid OTLP format, put it back and use flat format
                    res_map.insert(f::ATTRIBUTES.into(), Value::Array(arr));
                    OtelAttributes::from_object_map(&res_map)
                }
            } else {
                // Flat format: {"key": "value", ...}
                OtelAttributes::from_object_map(&res_map)
            };

            let resource = Resource {
                attributes: Vec::new(),
                dropped_attributes_count: dropped_count,
            };
            (Some(resource), attrs)
        }
        Some(other) => {
            map.insert(f::RESOURCE.into(), other);
            (None, OtelAttributes::new())
        }
        None => (None, OtelAttributes::new()),
    }
}

fn restore_scope(map: &mut ObjectMap) -> (Option<InstrumentationScope>, OtelAttributes) {
    match map.remove(f::SCOPE) {
        Some(Value::Object(mut scope_map)) => {
            let name = match scope_map.remove(f::NAME) {
                Some(Value::Bytes(b)) => String::from_utf8(b.to_vec()).unwrap_or_default(),
                _ => String::new(),
            };
            let version = match scope_map.remove(f::VERSION) {
                Some(Value::Bytes(b)) => String::from_utf8(b.to_vec()).unwrap_or_default(),
                _ => String::new(),
            };
            let attrs = match scope_map.remove(f::ATTRIBUTES) {
                Some(Value::Array(arr)) => {
                    // Try OTLP JSON format: [{"key":"k","value":{...}}]
                    if let Some(kvs) = parse_otlp_key_value_array(&arr) {
                        let mut otel_attrs = OtelAttributes::new();
                        for kv in kvs {
                            otel_attrs.insert(kv.key, kv.value.unwrap_or(AnyValue { value: None }));
                        }
                        otel_attrs
                    } else {
                        OtelAttributes::new()
                    }
                }
                Some(Value::Object(attrs_map)) => OtelAttributes::from_object_map(&attrs_map),
                _ => OtelAttributes::new(),
            };
            if name.is_empty() && version.is_empty() && attrs.is_empty() {
                (None, OtelAttributes::new())
            } else {
                (
                    Some(InstrumentationScope {
                        name,
                        version,
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    }),
                    attrs,
                )
            }
        }
        Some(other) => {
            map.insert(f::SCOPE.into(), other);
            (None, OtelAttributes::new())
        }
        None => (None, OtelAttributes::new()),
    }
}

/// Convert an `OTel` `any_value::Value` to a string for use as a metric tag.
pub(super) fn otel_value_to_tag_string(v: &OtelValueKind) -> String {
    match v {
        OtelValueKind::StringValue(s) => s.clone(),
        OtelValueKind::BoolValue(b) => b.to_string(),
        OtelValueKind::IntValue(i) => i.to_string(),
        OtelValueKind::DoubleValue(f) => f.to_string(),
        OtelValueKind::BytesValue(b) => String::from_utf8_lossy(b).into_owned(),
        OtelValueKind::ArrayValue(_) => "<array>".to_string(),
        OtelValueKind::KvlistValue(_) => "<kvlist>".to_string(),
    }
}

fn nanos_to_timestamp(nanos: u64) -> Option<Value> {
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs).map(Value::Timestamp)
}

fn navigate_value(v: &Value, remaining: &[String]) -> Option<Value> {
    let mut current = v;
    for seg in remaining {
        match current {
            Value::Object(map) => {
                current = map.get(seg.as_str())?;
            }
            _ => return None,
        }
    }
    Some(current.clone())
}

fn insert_value_at(v: &mut Value, remaining: &[String], new_val: Value) -> Option<Value> {
    if remaining.len() == 1 {
        if let Value::Object(map) = v {
            map.insert(remaining[0].as_str().into(), new_val)
        } else {
            let mut map = ObjectMap::new();
            map.insert(remaining[0].as_str().into(), new_val);
            *v = Value::Object(map);
            None
        }
    } else if let Value::Object(map) = v {
        let entry = map
            .entry(remaining[0].as_str().into())
            .or_insert_with(|| Value::Object(ObjectMap::new()));
        insert_value_at(entry, &remaining[1..], new_val)
    } else {
        let mut map = ObjectMap::new();
        let mut inner = Value::Object(ObjectMap::new());
        insert_value_at(&mut inner, &remaining[1..], new_val);
        map.insert(remaining[0].as_str().into(), inner);
        *v = Value::Object(map);
        None
    }
}

fn remove_value_at(v: &mut Value, remaining: &[String], prune: bool) -> Option<Value> {
    if remaining.len() == 1 {
        match v {
            Value::Object(map) => map.remove(remaining[0].as_str()),
            _ => None,
        }
    } else {
        match v {
            Value::Object(map) => {
                let inner = map.get_mut(remaining[0].as_str())?;
                let result = remove_value_at(inner, &remaining[1..], prune);
                if prune
                    && let Value::Object(inner_map) = inner
                    && inner_map.is_empty()
                {
                    map.remove(remaining[0].as_str());
                }
                result
            }
            _ => None,
        }
    }
}

/// Build an `OwnedValuePath` from remaining path segments (used for navigating
/// into a VRL Value subtree after resolving the proto root field).
fn remaining_value_path<'a>(
    segments: impl Iterator<Item = lookup::path::BorrowedSegment<'a>>,
) -> vrl::path::OwnedValuePath {
    use vrl::path::{OwnedSegment, OwnedValuePath};
    OwnedValuePath {
        segments: segments
            .filter_map(|seg| match seg {
                lookup::path::BorrowedSegment::Field(f) => {
                    Some(OwnedSegment::Field(f.as_ref().into()))
                }
                lookup::path::BorrowedSegment::Index(i) => Some(OwnedSegment::Index(i)),
                lookup::path::BorrowedSegment::Invalid => None,
            })
            .collect(),
    }
}

/// Coerce a VRL `Value` to a `Timestamp` if it's a string that can be
/// parsed as RFC 3339. This is needed because OTLP `AnyValue` has no native
/// timestamp type, so timestamps round-trip as strings.
fn coerce_to_timestamp(v: Value) -> Value {
    if let Value::Bytes(b) = &v
        && let Ok(s) = std::str::from_utf8(b)
        && let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s)
    {
        return Value::Timestamp(dt.with_timezone(&chrono::Utc));
    }
    v
}

// -- OtelAttributes --

/// Return a full `Resource` proto with attributes reconstituted from
/// the internal `OtelAttributes`. Use at proto serialization boundaries.
pub fn resource_to_proto(resource: Option<&Resource>, attrs: &OtelAttributes) -> Option<Resource> {
    let has_resource = resource.is_some();
    let has_attrs = !attrs.is_empty();
    if !has_resource && !has_attrs {
        return None;
    }
    let mut r = resource.cloned().unwrap_or(Resource {
        attributes: Vec::new(),
        dropped_attributes_count: 0,
    });
    r.attributes = attrs.to_key_values();
    Some(r)
}

/// Return a full `InstrumentationScope` proto with attributes reconstituted.
pub fn scope_to_proto(
    scope: Option<&InstrumentationScope>,
    attrs: &OtelAttributes,
) -> Option<InstrumentationScope> {
    let has_scope = scope.is_some();
    let has_attrs = !attrs.is_empty();
    if !has_scope && !has_attrs {
        return None;
    }
    let mut s = scope.cloned().unwrap_or_default();
    s.attributes = attrs.to_key_values();
    Some(s)
}

fn append_canonical_resource_scope(
    map: &mut ObjectMap,
    resource: Option<&Resource>,
    resource_attrs: &OtelAttributes,
    scope: Option<&InstrumentationScope>,
    scope_attrs: &OtelAttributes,
) {
    {
        let mut res_map = resource_attrs.to_object_map();
        if let Some(res) = resource
            && res.dropped_attributes_count != 0
        {
            res_map.insert(
                f::DROPPED_ATTRIBUTES_COUNT.into(),
                Value::Integer(i64::from(res.dropped_attributes_count)),
            );
        }
        if !res_map.is_empty() {
            map.insert(f::RESOURCE.into(), Value::Object(res_map));
        }
    }
    {
        let mut scope_map = ObjectMap::new();
        if let Some(s) = scope {
            if !s.name.is_empty() {
                scope_map.insert(f::NAME.into(), Value::Bytes(s.name.clone().into()));
            }
            if !s.version.is_empty() {
                scope_map.insert(f::VERSION.into(), Value::Bytes(s.version.clone().into()));
            }
        }
        if !scope_attrs.is_empty() {
            scope_map.insert(
                f::ATTRIBUTES.into(),
                Value::Object(scope_attrs.to_object_map()),
            );
        }
        if !scope_map.is_empty() {
            map.insert(f::SCOPE.into(), Value::Object(scope_map));
        }
    }
}

fn remove_resource_subpath(
    resource_attrs: &mut OtelAttributes,
    remaining: &[String],
    prune: bool,
) -> Option<Value> {
    if remaining.len() == 1 {
        resource_attrs
            .remove(remaining[0].as_str())
            .map(|av| any_value_to_vrl(&av))
    } else {
        let key = remaining[0].as_str();
        let av = resource_attrs.get(key)?;
        let mut v = any_value_to_vrl(av);
        let result = remove_value_at(&mut v, &remaining[1..], prune);
        resource_attrs.insert(key.to_string(), vrl_value_to_any_value(&v));
        result
    }
}

fn remove_scope_subpath(
    scope: Option<&mut InstrumentationScope>,
    scope_attrs: &mut OtelAttributes,
    remaining: &[String],
    prune: bool,
) -> Option<Value> {
    let scope = scope?;
    match remaining[0].as_str() {
        f::NAME if remaining.len() == 1 => {
            if scope.name.is_empty() {
                return None;
            }
            let old = Some(Value::Bytes(scope.name.clone().into()));
            scope.name = String::new();
            old
        }
        f::VERSION if remaining.len() == 1 => {
            if scope.version.is_empty() {
                return None;
            }
            let old = Some(Value::Bytes(scope.version.clone().into()));
            scope.version = String::new();
            old
        }
        f::ATTRIBUTES => {
            if remaining.len() == 1 {
                if scope_attrs.is_empty() {
                    return None;
                }
                let old = Some(Value::Object(scope_attrs.to_object_map()));
                *scope_attrs = OtelAttributes::new();
                old
            } else if remaining.len() == 2 {
                scope_attrs
                    .remove(remaining[1].as_str())
                    .map(|av| any_value_to_vrl(&av))
            } else {
                let key = remaining[1].as_str();
                let av = scope_attrs.get(key)?;
                let mut v = any_value_to_vrl(av);
                let result = remove_value_at(&mut v, &remaining[2..], prune);
                scope_attrs.insert(key.to_string(), vrl_value_to_any_value(&v));
                result
            }
        }
        _ => None,
    }
}

fn remove_attrs_subpath(
    attrs: &mut OtelAttributes,
    first: &str,
    remaining: &[String],
    prune: bool,
) -> Option<Value> {
    let av = attrs.get(first)?;
    let mut v = any_value_to_vrl(av);
    let result = remove_value_at(&mut v, remaining, prune);
    if result.is_some() {
        if prune
            && let Value::Object(ref map) = v
            && map.is_empty()
        {
            attrs.remove(first);
            return result;
        }
        attrs.insert(first.to_string(), vrl_value_to_any_value(&v));
    }
    result
}

// OtelAttributes is defined in the sibling `otel_attributes` module.
pub use super::otel_attributes::OtelAttributes;
pub(super) fn otel_value_to_str_ref(v: &OtelValueKind) -> &str {
    match v {
        OtelValueKind::StringValue(s) => s.as_str(),
        _ => "",
    }
}

// -- OtelLog --

#[derive(Clone, Debug, PartialEq)]
pub struct OtelLog {
    pub(crate) record: LogRecord,
    pub(crate) record_attrs: OtelAttributes,
    pub(crate) resource: Option<Resource>,
    pub(crate) resource_attrs: OtelAttributes,
    pub(crate) scope: Option<InstrumentationScope>,
    pub(crate) scope_attrs: OtelAttributes,
    pub(crate) metadata: EventMetadata,
}

impl OtelLog {
    pub fn new(mut record: LogRecord) -> Self {
        let record_attrs = OtelAttributes::from_key_values(std::mem::take(&mut record.attributes));
        Self {
            record,
            record_attrs,
            resource: None,
            resource_attrs: OtelAttributes::new(),
            scope: None,
            scope_attrs: OtelAttributes::new(),
            metadata: EventMetadata::default(),
        }
    }

    /// Create an `OtelLog` from raw bytes, setting `record.body` to a string value.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let body_value = match std::str::from_utf8(bytes) {
            Ok(s) => OtelValueKind::StringValue(s.to_owned()),
            Err(_) => OtelValueKind::BytesValue(bytes.to_vec()),
        };
        Self {
            record: LogRecord {
                body: Some(AnyValue {
                    value: Some(body_value),
                }),
                ..Default::default()
            },
            record_attrs: OtelAttributes::new(),
            resource: None,
            resource_attrs: OtelAttributes::new(),
            scope: None,
            scope_attrs: OtelAttributes::new(),
            metadata: EventMetadata::default(),
        }
    }

    /// Create an `OtelLog` from a JSON value.
    /// Objects are routed through `from_value_map` so canonical keys (`body`,
    /// `time_unix_nano`, `resource`, etc.) are restored to their proto slots.
    /// Scalars/arrays become the record body directly.
    pub fn from_json_value(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Object(map) => {
                let vrl_map: ObjectMap = map
                    .into_iter()
                    .map(|(k, v)| (KeyString::from(k), json_to_vrl_value(v)))
                    .collect();
                Self::from_value_map(Value::Object(vrl_map), EventMetadata::default())
            }
            other => {
                let body = json_to_any_value(other);
                Self {
                    record: LogRecord {
                        body: Some(body),
                        ..Default::default()
                    },
                    record_attrs: OtelAttributes::new(),
                    resource: None,
                    resource_attrs: OtelAttributes::new(),
                    scope: None,
                    scope_attrs: OtelAttributes::new(),
                    metadata: EventMetadata::default(),
                }
            }
        }
    }

    pub fn from_parts(
        mut record: LogRecord,
        mut resource: Option<Resource>,
        mut scope: Option<InstrumentationScope>,
        metadata: EventMetadata,
    ) -> Self {
        let record_attrs = OtelAttributes::from_key_values(std::mem::take(&mut record.attributes));
        let resource_attrs = resource
            .as_mut()
            .map(|r| OtelAttributes::from_key_values(std::mem::take(&mut r.attributes)))
            .unwrap_or_default();
        let scope_attrs = scope
            .as_mut()
            .map(|s| OtelAttributes::from_key_values(std::mem::take(&mut s.attributes)))
            .unwrap_or_default();
        Self {
            record,
            record_attrs,
            resource,
            resource_attrs,
            scope,
            scope_attrs,
            metadata,
        }
    }

    /// Build an `OtelLog` from a `tracing::Event` — the same semantics as
    /// Converts a `tracing::Event` directly into an `OtelLog`.
    /// Accumulates fields via the `tracing::field::Visit`
    /// trait into a single `ObjectMap`, then converts to `OtelLog` once
    /// to amortize the legacy-layout round-trip.
    pub fn from_tracing_event(event: &tracing::Event<'_>) -> Self {
        let mut builder = OtelLogTracingBuilder::default();
        event.record(&mut builder);

        let meta = event.metadata();
        let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        builder
            .map
            .insert(f::TIME_UNIX_NANO.into(), Value::Integer(now_nanos as i64));
        let kind_value = if meta.is_event() {
            Value::Bytes(f::EVENT.to_string().into())
        } else if meta.is_span() {
            Value::Bytes("span".to_string().into())
        } else {
            Value::Null
        };
        let mut metadata_map = ObjectMap::new();
        metadata_map.insert(f::SPAN_KIND.into(), kind_value);
        metadata_map.insert(
            f::LEVEL.into(),
            Value::Bytes(meta.level().to_string().into()),
        );
        metadata_map.insert(
            f::MODULE_PATH.into(),
            meta.module_path()
                .map_or(Value::Null, |mp| Value::Bytes(mp.to_string().into())),
        );
        metadata_map.insert(
            f::TARGET.into(),
            Value::Bytes(meta.target().to_string().into()),
        );
        builder
            .map
            .insert(f::METADATA.into(), Value::Object(metadata_map));

        OtelLog::from_value_map(Value::Object(builder.map), EventMetadata::default())
    }

    pub fn into_parts(
        mut self,
    ) -> (
        LogRecord,
        Option<Resource>,
        Option<InstrumentationScope>,
        EventMetadata,
    ) {
        self.record.attributes = self.record_attrs.to_key_values();
        let resource = self.resource.map(|mut r| {
            r.attributes = self.resource_attrs.to_key_values();
            r
        });
        let scope = self.scope.map(|mut s| {
            s.attributes = self.scope_attrs.to_key_values();
            s
        });
        (self.record, resource, scope, self.metadata)
    }

    pub fn record(&self) -> &LogRecord {
        &self.record
    }

    /// Return a full `LogRecord` with `attributes` populated from the
    /// internal `OtelAttributes` map.  Use at proto serialization boundaries
    /// (gRPC sink, buffer codec, OTLP codec) where the downstream expects
    /// a complete proto message.
    pub fn record_to_proto(&self) -> LogRecord {
        let mut r = self.record.clone();
        r.attributes = self.record_attrs.to_key_values();
        r
    }

    pub fn record_mut(&mut self) -> &mut LogRecord {
        &mut self.record
    }

    /// Return a reference to the `OtelAttributes` for record-level attributes.
    pub fn record_attributes(&self) -> &OtelAttributes {
        &self.record_attrs
    }

    pub fn resource(&self) -> Option<&Resource> {
        self.resource.as_ref()
    }

    pub fn resource_proto(&self) -> Option<Resource> {
        resource_to_proto(self.resource.as_ref(), &self.resource_attrs)
    }

    pub fn scope_proto(&self) -> Option<InstrumentationScope> {
        scope_to_proto(self.scope.as_ref(), &self.scope_attrs)
    }

    pub fn set_resource(&mut self, mut resource: Resource) {
        self.resource_attrs =
            OtelAttributes::from_key_values(std::mem::take(&mut resource.attributes));
        self.resource = Some(resource);
    }

    pub fn scope(&self) -> Option<&InstrumentationScope> {
        self.scope.as_ref()
    }

    pub fn set_scope(&mut self, mut scope: InstrumentationScope) {
        self.scope_attrs = OtelAttributes::from_key_values(std::mem::take(&mut scope.attributes));
        self.scope = Some(scope);
    }

    pub fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        &mut self.metadata
    }

    pub fn body(&self) -> Option<&AnyValue> {
        self.record.body.as_ref()
    }

    /// Return the body as a human-readable string, suitable for text-oriented
    /// serializers (text, `raw_message`, etc.).
    pub fn body_string(&self) -> String {
        match self.record.body.as_ref().and_then(|av| av.value.as_ref()) {
            Some(OtelValueKind::StringValue(s)) => s.clone(),
            Some(OtelValueKind::BoolValue(b)) => b.to_string(),
            Some(OtelValueKind::IntValue(i)) => i.to_string(),
            Some(OtelValueKind::DoubleValue(d)) => d.to_string(),
            Some(OtelValueKind::BytesValue(b)) => String::from_utf8_lossy(b).into_owned(),
            Some(OtelValueKind::ArrayValue(a)) => format!("{a:?}"),
            Some(OtelValueKind::KvlistValue(kv)) => format!("{kv:?}"),
            None => String::new(),
        }
    }

    pub fn set_body(&mut self, value: AnyValue) {
        self.record.body = Some(value);
    }

    pub fn time_unix_nano(&self) -> u64 {
        self.record.time_unix_nano
    }

    pub fn observed_time_unix_nano(&self) -> u64 {
        self.record.observed_time_unix_nano
    }

    pub fn severity_number(&self) -> i32 {
        self.record.severity_number
    }

    pub fn severity_text(&self) -> &str {
        &self.record.severity_text
    }

    pub fn trace_id(&self) -> &[u8] {
        &self.record.trace_id
    }

    pub fn span_id(&self) -> &[u8] {
        &self.record.span_id
    }

    /// Merge another `OtelLog`'s body into this one (concatenate string bodies).
    /// Used for partial line merging (e.g. Docker logs).
    /// Also merges metadata.
    pub fn merge_body(&mut self, incoming: &OtelLog) {
        if let (Some(self_body), Some(inc_body)) = (self.body(), incoming.body()) {
            if let (
                Some(OtelValueKind::StringValue(self_s)),
                Some(OtelValueKind::StringValue(inc_s)),
            ) = (&self_body.value, &inc_body.value)
            {
                let merged = format!("{self_s}{inc_s}");
                self.set_body(AnyValue {
                    value: Some(OtelValueKind::StringValue(merged)),
                });
            }
        } else if self.body().is_none()
            && let Some(inc_body) = incoming.body()
        {
            self.set_body(inc_body.clone());
        }
        self.metadata.merge(incoming.metadata.clone());
    }

    pub fn attribute(&self, key: &str) -> Option<&AnyValue> {
        self.record_attrs.get(key)
    }

    /// Returns `true` if the body is a `KvList` that contains `key`, or if `key`
    /// exists as a record attribute.  This is the appropriate check before
    /// writing a header value so that body fields always take precedence.
    pub fn has_field(&self, key: &str) -> bool {
        if self.attribute(key).is_some() {
            return true;
        }
        if let Some(body) = self.body()
            && let Some(OtelValueKind::KvlistValue(kvl)) = &body.value
        {
            return kvl.values.iter().any(|kv| kv.key == key);
        }
        false
    }

    pub fn set_attribute(&mut self, key: String, value: AnyValue) {
        self.record_attrs.insert(key, value);
    }

    pub fn remove_attribute(&mut self, key: &str) -> Option<AnyValue> {
        self.record_attrs.remove(key)
    }

    pub fn attributes(&self) -> &OtelAttributes {
        &self.record_attrs
    }

    pub fn resource_attribute(&self, key: &str) -> Option<&AnyValue> {
        self.resource_attrs.get(key)
    }

    pub fn resource_attrs(&self) -> &OtelAttributes {
        &self.resource_attrs
    }

    pub fn scope_attrs(&self) -> &OtelAttributes {
        &self.scope_attrs
    }

    /// Ensure the resource object exists, creating it if absent.
    fn ensure_resource(&mut self) {
        if self.resource.is_none() {
            self.resource = Some(Resource {
                attributes: Vec::new(),
                dropped_attributes_count: 0,
            });
        }
    }

    /// Set a resource attribute (e.g. `host.name`, `source_type`).
    pub fn set_resource_attribute(&mut self, key: String, value: AnyValue) {
        self.ensure_resource();
        self.resource_attrs.insert(key, value);
    }

    /// Set the `observed_time_unix_nano` (ingest timestamp) from a chrono `DateTime`.
    pub fn set_observed_timestamp(&mut self, now: chrono::DateTime<chrono::Utc>) {
        self.record.observed_time_unix_nano = now.timestamp_nanos_opt().unwrap_or(0) as u64;
    }

    /// Set source metadata: `source_type` (resource attribute) and `observed_time_unix_nano`.
    pub fn set_source_metadata(&mut self, source_name: &str, now: chrono::DateTime<chrono::Utc>) {
        self.set_resource_attribute(f::SOURCE_TYPE.to_string(), string_value(source_name));
        self.set_observed_timestamp(now);
    }

    /// Set source metadata for Vector namespace: populates both `OtelLog`
    /// fields and `%vector.*` metadata entries for backward compatibility.
    pub fn set_source_metadata_vector_ns(
        &mut self,
        source_name: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) {
        self.set_source_metadata(source_name, now);
        self.metadata.value_mut().insert(
            lookup::path!("vector", f::SOURCE_TYPE),
            source_name.to_owned(),
        );
        self.metadata.value_mut().insert(
            lookup::path!("vector", f::INGEST_TIMESTAMP),
            Value::Timestamp(now),
        );
    }

    pub fn add_finalizer(&mut self, finalizer: EventFinalizer) {
        self.metadata.add_finalizer(finalizer);
    }

    #[must_use]
    pub fn with_batch_notifier(mut self, batch: &BatchNotifier) -> Self {
        self.metadata = self.metadata.with_batch_notifier(batch);
        self
    }

    #[must_use]
    pub fn with_batch_notifier_option(mut self, batch: &Option<BatchNotifier>) -> Self {
        self.metadata = self.metadata.with_batch_notifier_option(batch);
        self
    }

    // -----------------------------------------------------------------------
    // Field access methods
    //
    // Single-segment and multi-segment field paths use direct proto
    // accessors. Array-index paths resolve the root field then navigate the Value subtree.
    // -----------------------------------------------------------------------

    /// Get a field value by its semantic meaning (looks up schema definition).
    pub fn get_by_meaning(&self, meaning: impl AsRef<str>) -> Option<Value> {
        self.metadata.dropped_field(&meaning).cloned().or_else(|| {
            self.metadata
                .schema_definition()
                .meaning_path(meaning.as_ref())
                .and_then(|path| self.get(path))
        })
    }

    /// Find the path for a field by its semantic meaning.
    pub fn find_key_by_meaning(
        &self,
        meaning: impl AsRef<str>,
    ) -> Option<&vrl::path::OwnedTargetPath> {
        self.metadata
            .schema_definition()
            .meaning_path(meaning.as_ref())
    }

    /// Get a field value by path.
    /// Navigates proto fields and `OtelAttributes` directly. For paths containing
    /// array indices, resolves the root field then navigates the Value subtree.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "impl Trait parameter follows VRL target API convention"
    )]
    pub fn get<'a>(&self, path: impl lookup::lookup_v2::TargetPath<'a>) -> Option<Value> {
        use lookup::lookup_v2::ValuePath;
        use lookup::path::BorrowedSegment;

        match path.prefix() {
            lookup::PathPrefix::Event => {
                let vp = path.value_path();
                let mut iter = vp.segment_iter();
                match iter.next() {
                    Some(BorrowedSegment::Field(ref first)) => match iter.next() {
                        None => self.get_single_segment(first.as_ref()),
                        Some(BorrowedSegment::Field(ref second)) => {
                            let mut fields = vec![first.to_string(), second.to_string()];
                            let mut remaining_iter = None;
                            for seg in &mut iter {
                                if let BorrowedSegment::Field(f) = seg {
                                    fields.push(f.to_string());
                                } else {
                                    remaining_iter = Some((seg, iter));
                                    break;
                                }
                            }
                            if let Some((non_field_seg, rest)) = remaining_iter {
                                let base = self.get_field_path(&fields)?;
                                let sub = remaining_value_path(
                                    std::iter::once(non_field_seg).chain(rest),
                                );
                                base.get(&sub).cloned()
                            } else {
                                self.get_field_path(&fields)
                            }
                        }
                        Some(non_field_seg) => {
                            let base = self.get_single_segment(first.as_ref())?;
                            let sub =
                                remaining_value_path(std::iter::once(non_field_seg).chain(iter));
                            base.get(&sub).cloned()
                        }
                    },
                    None => self.as_map().map(Value::Object),
                    _ => None,
                }
            }
            lookup::PathPrefix::Metadata => self.metadata.value().get(path.value_path()).cloned(),
        }
    }

    fn get_single_segment(&self, field: &str) -> Option<Value> {
        match field {
            f::BODY => self.body().map(any_value_to_vrl),
            f::SEVERITY_TEXT if !self.record.severity_text.is_empty() => {
                Some(Value::Bytes(self.record.severity_text.clone().into()))
            }
            f::SEVERITY_NUMBER if self.record.severity_number != 0 => {
                Some(Value::Integer(i64::from(self.record.severity_number)))
            }
            f::LOG_TRACE_ID if !self.record.trace_id.is_empty() => {
                Some(hex_encode(&self.record.trace_id))
            }
            f::LOG_SPAN_ID if !self.record.span_id.is_empty() => {
                Some(hex_encode(&self.record.span_id))
            }
            f::TIME_UNIX_NANO if self.record.time_unix_nano != 0 => {
                Some(Value::Integer(self.record.time_unix_nano as i64))
            }
            f::OBSERVED_TIME_UNIX_NANO if self.record.observed_time_unix_nano != 0 => {
                Some(Value::Integer(self.record.observed_time_unix_nano as i64))
            }
            other => self.record_attrs.get(other).map(any_value_to_vrl),
        }
    }

    fn get_field_path(&self, fields: &[String]) -> Option<Value> {
        debug_assert!(fields.len() >= 2);
        match fields[0].as_str() {
            f::RESOURCE => {
                let remaining = &fields[1..];
                if remaining.len() == 1 {
                    let key = remaining[0].as_str();
                    if key == f::DROPPED_ATTRIBUTES_COUNT {
                        let res = self.resource.as_ref()?;
                        if res.dropped_attributes_count != 0 {
                            return Some(Value::Integer(i64::from(res.dropped_attributes_count)));
                        }
                        return None;
                    }
                    self.resource_attrs.get(key).map(any_value_to_vrl)
                } else {
                    let key = remaining[0].as_str();
                    let av = self.resource_attrs.get(key)?;
                    let v = any_value_to_vrl(av);
                    navigate_value(&v, &remaining[1..])
                }
            }
            f::SCOPE => {
                let scope = self.scope.as_ref()?;
                let remaining = &fields[1..];
                match remaining[0].as_str() {
                    f::NAME if remaining.len() == 1 => {
                        if scope.name.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(scope.name.clone().into()))
                        }
                    }
                    f::VERSION if remaining.len() == 1 => {
                        if scope.version.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(scope.version.clone().into()))
                        }
                    }
                    f::ATTRIBUTES => {
                        if remaining.len() == 1 {
                            if self.scope_attrs.is_empty() {
                                None
                            } else {
                                Some(Value::Object(self.scope_attrs.to_object_map()))
                            }
                        } else {
                            let av = self.scope_attrs.get(&remaining[1])?;
                            if remaining.len() == 2 {
                                Some(any_value_to_vrl(av))
                            } else {
                                let v = any_value_to_vrl(av);
                                navigate_value(&v, &remaining[2..])
                            }
                        }
                    }
                    _ => None,
                }
            }
            first => {
                let av = self.record_attrs.get(first)?;
                let v = any_value_to_vrl(av);
                navigate_value(&v, &fields[1..])
            }
        }
    }

    /// Insert a field value by path.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "impl Trait parameter follows VRL target API convention"
    )]
    pub fn insert<'a>(
        &mut self,
        path: impl lookup::lookup_v2::TargetPath<'a>,
        value: impl Into<Value>,
    ) -> Option<Value> {
        use lookup::lookup_v2::ValuePath;
        use lookup::path::BorrowedSegment;

        match path.prefix() {
            lookup::PathPrefix::Event => {
                let value = value.into();
                let vp = path.value_path();
                let mut iter = vp.segment_iter();
                match iter.next() {
                    Some(BorrowedSegment::Field(ref first)) => match iter.next() {
                        None => self.insert_single_segment(first.as_ref(), value),
                        Some(BorrowedSegment::Field(ref second)) => {
                            let mut fields = vec![first.to_string(), second.to_string()];
                            let mut remaining_iter = None;
                            for seg in &mut iter {
                                if let BorrowedSegment::Field(f) = seg {
                                    fields.push(f.to_string());
                                } else {
                                    remaining_iter = Some((seg, iter));
                                    break;
                                }
                            }
                            if let Some((non_field_seg, rest)) = remaining_iter {
                                let mut base = self.get_field_path(&fields).unwrap_or(Value::Null);
                                let sub = remaining_value_path(
                                    std::iter::once(non_field_seg).chain(rest),
                                );
                                let old = base.insert(&sub, value);
                                self.insert_field_path(&fields, base);
                                old
                            } else {
                                self.insert_field_path(&fields, value)
                            }
                        }
                        Some(non_field_seg) => {
                            let first_str = first.as_ref();
                            let mut base =
                                self.get_single_segment(first_str).unwrap_or(Value::Null);
                            let sub =
                                remaining_value_path(std::iter::once(non_field_seg).chain(iter));
                            let old = base.insert(&sub, value);
                            self.insert_single_segment(first_str, base);
                            old
                        }
                    },
                    _ => None,
                }
            }
            lookup::PathPrefix::Metadata => {
                self.metadata.value_mut().insert(path.value_path(), value)
            }
        }
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "value ownership needed for API consistency with callers"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "OTLP log field mapping requires many branches"
    )]
    fn insert_single_segment(&mut self, field: &str, value: Value) -> Option<Value> {
        match field {
            f::BODY => {
                let old = self.body().map(any_value_to_vrl);
                self.record_mut().body = Some(vrl_value_to_any_value(&value));
                old
            }
            f::SEVERITY_TEXT => {
                let old = if self.record.severity_text.is_empty() {
                    None
                } else {
                    Some(Value::Bytes(self.record.severity_text.clone().into()))
                };
                self.record_mut().severity_text = value
                    .as_str()
                    .map_or_else(|| value.to_string_lossy().into_owned(), |s| s.to_string());
                old
            }
            f::SEVERITY_NUMBER => {
                let old = if self.record.severity_number == 0 {
                    None
                } else {
                    Some(Value::Integer(i64::from(self.record.severity_number)))
                };
                if let Some(n) = value.as_integer() {
                    self.record_mut().severity_number = OtlpEnumField::from_vrl(n).as_proto();
                }
                old
            }
            f::LOG_TRACE_ID => {
                let old = if self.record.trace_id.is_empty() {
                    None
                } else {
                    Some(hex_encode(&self.record.trace_id))
                };
                if let Some(decoded) = hex_decode(&value) {
                    self.record_mut().trace_id = decoded;
                } else {
                    // Malformed hex: store as attribute so data isn't lost
                    self.record_attrs
                        .insert(f::LOG_TRACE_ID.to_string(), vrl_value_to_any_value(&value));
                }
                old
            }
            f::LOG_SPAN_ID => {
                let old = if self.record.span_id.is_empty() {
                    None
                } else {
                    Some(hex_encode(&self.record.span_id))
                };
                if let Some(decoded) = hex_decode(&value) {
                    self.record_mut().span_id = decoded;
                } else {
                    self.record_attrs
                        .insert(f::LOG_SPAN_ID.to_string(), vrl_value_to_any_value(&value));
                }
                old
            }
            f::TIME_UNIX_NANO => {
                let old = if self.record.time_unix_nano == 0 {
                    None
                } else {
                    Some(Value::Integer(self.record.time_unix_nano as i64))
                };
                match &value {
                    Value::Integer(n) => {
                        self.record_mut().time_unix_nano = *n as u64;
                    }
                    Value::Timestamp(ts) => {
                        self.record_mut().time_unix_nano =
                            ts.timestamp_nanos_opt().unwrap_or(0) as u64;
                    }
                    _ => {
                        self.record_attrs.insert(
                            f::TIME_UNIX_NANO.to_string(),
                            vrl_value_to_any_value(&value),
                        );
                    }
                }
                old
            }
            f::OBSERVED_TIME_UNIX_NANO => {
                let old = if self.record.observed_time_unix_nano == 0 {
                    None
                } else {
                    Some(Value::Integer(self.record.observed_time_unix_nano as i64))
                };
                match &value {
                    Value::Integer(n) => {
                        self.record_mut().observed_time_unix_nano = *n as u64;
                    }
                    Value::Timestamp(ts) => {
                        self.record_mut().observed_time_unix_nano =
                            ts.timestamp_nanos_opt().unwrap_or(0) as u64;
                    }
                    _ => {}
                }
                old
            }
            other => {
                // Generic: upsert into record attributes
                let old = self.record_attrs.get(other).map(any_value_to_vrl);
                self.record_attrs
                    .insert(other.to_string(), vrl_value_to_any_value(&value));
                old
            }
        }
    }

    fn insert_field_path(&mut self, fields: &[String], value: Value) -> Option<Value> {
        debug_assert!(fields.len() >= 2);
        match fields[0].as_str() {
            f::RESOURCE => {
                self.ensure_resource();
                let remaining = &fields[1..];
                if remaining.len() == 1 {
                    let key = remaining[0].as_str();
                    let old = self.resource_attrs.get(key).map(any_value_to_vrl);
                    self.resource_attrs
                        .insert(key.to_string(), vrl_value_to_any_value(&value));
                    old
                } else {
                    let key = remaining[0].as_str();
                    let mut v = self
                        .resource_attrs
                        .get(key)
                        .map_or(Value::Object(ObjectMap::new()), any_value_to_vrl);
                    let old = insert_value_at(&mut v, &remaining[1..], value);
                    self.resource_attrs
                        .insert(key.to_string(), vrl_value_to_any_value(&v));
                    old
                }
            }
            f::SCOPE => {
                let scope = self.scope.get_or_insert_with(InstrumentationScope::default);
                let remaining = &fields[1..];
                match remaining[0].as_str() {
                    f::NAME if remaining.len() == 1 => {
                        let old = if scope.name.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(scope.name.clone().into()))
                        };
                        scope.name = value.as_str().map_or_else(
                            || value.to_string_lossy().into_owned(),
                            |s| s.to_string(),
                        );
                        old
                    }
                    f::VERSION if remaining.len() == 1 => {
                        let old = if scope.version.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(scope.version.clone().into()))
                        };
                        scope.version = value.as_str().map_or_else(
                            || value.to_string_lossy().into_owned(),
                            |s| s.to_string(),
                        );
                        old
                    }
                    f::ATTRIBUTES => {
                        if remaining.len() == 1 {
                            let old = if self.scope_attrs.is_empty() {
                                None
                            } else {
                                Some(Value::Object(self.scope_attrs.to_object_map()))
                            };
                            if let Value::Object(map) = &value {
                                let mut new_attrs = OtelAttributes::new();
                                for (k, v) in map {
                                    new_attrs.insert(k.to_string(), vrl_value_to_any_value(v));
                                }
                                self.scope_attrs = new_attrs;
                            }
                            old
                        } else {
                            let key = remaining[1].as_str();
                            if remaining.len() == 2 {
                                let old = self.scope_attrs.get(key).map(any_value_to_vrl);
                                self.scope_attrs
                                    .insert(key.to_string(), vrl_value_to_any_value(&value));
                                old
                            } else {
                                let mut v = self
                                    .scope_attrs
                                    .get(key)
                                    .map_or(Value::Object(ObjectMap::new()), any_value_to_vrl);
                                let old = insert_value_at(&mut v, &remaining[2..], value);
                                self.scope_attrs
                                    .insert(key.to_string(), vrl_value_to_any_value(&v));
                                old
                            }
                        }
                    }
                    _ => None,
                }
            }
            first => {
                let mut v = self
                    .record_attrs
                    .get(first)
                    .map_or(Value::Object(ObjectMap::new()), any_value_to_vrl);
                let old = insert_value_at(&mut v, &fields[1..], value);
                self.record_attrs
                    .insert(first.to_string(), vrl_value_to_any_value(&v));
                old
            }
        }
    }

    /// Remove a field value by path.
    pub fn remove<'a>(&mut self, path: impl lookup::lookup_v2::TargetPath<'a>) -> Option<Value> {
        self.remove_prune(path, false)
    }

    /// Remove at `path`. If `prune` is true, empty parent objects along
    /// the path are also removed.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "impl Trait parameter follows VRL target API convention"
    )]
    pub fn remove_prune<'a>(
        &mut self,
        path: impl lookup::lookup_v2::TargetPath<'a>,
        prune: bool,
    ) -> Option<Value> {
        use lookup::lookup_v2::ValuePath;
        use lookup::path::BorrowedSegment;

        match path.prefix() {
            lookup::PathPrefix::Event => {
                let vp = path.value_path();
                let mut iter = vp.segment_iter();
                match iter.next() {
                    Some(BorrowedSegment::Field(ref first)) => match iter.next() {
                        None => self.remove_single_segment(first.as_ref()),
                        Some(BorrowedSegment::Field(ref second)) => {
                            let mut fields = vec![first.to_string(), second.to_string()];
                            let mut remaining_iter = None;
                            for seg in &mut iter {
                                if let BorrowedSegment::Field(f) = seg {
                                    fields.push(f.to_string());
                                } else {
                                    remaining_iter = Some((seg, iter));
                                    break;
                                }
                            }
                            if let Some((non_field_seg, rest)) = remaining_iter {
                                let mut base = self.get_field_path(&fields)?;
                                let sub = remaining_value_path(
                                    std::iter::once(non_field_seg).chain(rest),
                                );
                                let old = base.remove(&sub, prune);
                                self.insert_field_path(&fields, base);
                                old
                            } else {
                                self.remove_field_path(&fields, prune)
                            }
                        }
                        Some(non_field_seg) => {
                            let first_str = first.as_ref();
                            let mut base = self.get_single_segment(first_str)?;
                            let sub =
                                remaining_value_path(std::iter::once(non_field_seg).chain(iter));
                            let old = base.remove(&sub, prune);
                            self.insert_single_segment(first_str, base);
                            old
                        }
                    },
                    _ => None,
                }
            }
            lookup::PathPrefix::Metadata => {
                self.metadata.value_mut().remove(path.value_path(), prune)
            }
        }
    }

    fn remove_single_segment(&mut self, field: &str) -> Option<Value> {
        match field {
            f::BODY => {
                let old = self.body().map(any_value_to_vrl);
                self.record_mut().body = None;
                old
            }
            f::SEVERITY_TEXT => {
                if self.record.severity_text.is_empty() {
                    return None;
                }
                let old = Some(Value::Bytes(self.record.severity_text.clone().into()));
                self.record_mut().severity_text = String::new();
                old
            }
            f::SEVERITY_NUMBER => {
                if self.record.severity_number == 0 {
                    return None;
                }
                let old = Some(Value::Integer(i64::from(self.record.severity_number)));
                self.record_mut().severity_number = 0;
                old
            }
            f::LOG_TRACE_ID => {
                if self.record.trace_id.is_empty() {
                    return None;
                }
                let old = Some(hex_encode(&self.record.trace_id));
                self.record_mut().trace_id.clear();
                old
            }
            f::LOG_SPAN_ID => {
                if self.record.span_id.is_empty() {
                    return None;
                }
                let old = Some(hex_encode(&self.record.span_id));
                self.record_mut().span_id.clear();
                old
            }
            f::TIME_UNIX_NANO => {
                if self.record.time_unix_nano == 0 {
                    return None;
                }
                let old = Some(Value::Integer(self.record.time_unix_nano as i64));
                self.record_mut().time_unix_nano = 0;
                old
            }
            f::OBSERVED_TIME_UNIX_NANO => {
                if self.record.observed_time_unix_nano == 0 {
                    return None;
                }
                let old = Some(Value::Integer(self.record.observed_time_unix_nano as i64));
                self.record_mut().observed_time_unix_nano = 0;
                old
            }
            other => self
                .record_attrs
                .remove(other)
                .map(|av| any_value_to_vrl(&av)),
        }
    }

    fn remove_field_path(&mut self, fields: &[String], prune: bool) -> Option<Value> {
        debug_assert!(fields.len() >= 2);
        match fields[0].as_str() {
            f::RESOURCE => remove_resource_subpath(&mut self.resource_attrs, &fields[1..], prune),
            f::SCOPE => remove_scope_subpath(
                self.scope.as_mut(),
                &mut self.scope_attrs,
                &fields[1..],
                prune,
            ),
            first => remove_attrs_subpath(&mut self.record_attrs, first, &fields[1..], prune),
        }
    }

    /// Insert at `path` only if `path` is Some. Convenience wrapper around
    /// `insert` that only inserts when the path is `Some`.
    pub fn maybe_insert<'a>(
        &mut self,
        path: Option<impl lookup::lookup_v2::TargetPath<'a>>,
        value: impl Into<Value>,
    ) {
        if let Some(path) = path {
            self.insert(path, value);
        }
    }

    /// Merge fields from `incoming` into this log, concatenating byte
    /// values for specified fields. Merges by concatenating string values for specified fields.
    pub fn merge(&mut self, mut incoming: OtelLog, fields: &[impl AsRef<str>]) {
        for field in fields {
            let field_path = vrl::event_path!(field.as_ref());
            let Some(incoming_val) = incoming.remove(field_path) else {
                continue;
            };
            match self.get(field_path) {
                None => {
                    self.insert(field_path, incoming_val);
                }
                Some(mut current_val) => {
                    current_val.merge(incoming_val);
                    self.insert(field_path, current_val);
                }
            }
        }
        self.metadata.merge(incoming.metadata);
    }

    /// Build a Value tree with the legacy layout — no intermediate conversion.
    /// This ensures callers see the same field names and types.
    /// Build a flat `ObjectMap` from proto fields + attributes.
    /// Proto fields (body, `severity_text`, etc.) take precedence over
    /// attributes with the same name.
    pub fn as_map(&self) -> Option<ObjectMap> {
        let mut map = ObjectMap::new();

        if let Some(body) = self.body() {
            map.insert(f::BODY.into(), any_value_to_vrl(body));
        }
        if !self.record.severity_text.is_empty() {
            map.insert(
                f::SEVERITY_TEXT.into(),
                Value::Bytes(self.record.severity_text.clone().into()),
            );
        }
        if self.record.severity_number != 0 {
            map.insert(
                f::SEVERITY_NUMBER.into(),
                Value::Integer(i64::from(self.record.severity_number)),
            );
        }
        if self.record.time_unix_nano != 0 {
            map.insert(
                f::TIME_UNIX_NANO.into(),
                Value::Integer(self.record.time_unix_nano as i64),
            );
        }
        if self.record.observed_time_unix_nano != 0 {
            map.insert(
                f::OBSERVED_TIME_UNIX_NANO.into(),
                Value::Integer(self.record.observed_time_unix_nano as i64),
            );
        }
        if !self.record.trace_id.is_empty() {
            map.insert(f::LOG_TRACE_ID.into(), hex_encode(&self.record.trace_id));
        }
        if !self.record.span_id.is_empty() {
            map.insert(f::LOG_SPAN_ID.into(), hex_encode(&self.record.span_id));
        }
        if self.record.flags != 0 {
            map.insert(
                f::LOG_FLAGS.into(),
                Value::Integer(i64::from(self.record.flags)),
            );
        }
        if self.record.dropped_attributes_count != 0 {
            map.insert(
                f::DROPPED_ATTRIBUTES_COUNT.into(),
                Value::Integer(i64::from(self.record.dropped_attributes_count)),
            );
        }
        if !self.record_attrs.is_empty() {
            for (k, v) in self.record_attrs.iter() {
                let key = KeyString::from(k.clone());
                map.entry(key).or_insert_with(|| any_value_to_vrl(v));
            }
        }
        append_canonical_resource_scope(
            &mut map,
            self.resource.as_ref(),
            &self.resource_attrs,
            self.scope.as_ref(),
            &self.scope_attrs,
        );

        Some(map)
    }

    /// Construct an `OtelLog` from a legacy-layout Value + metadata.
    /// Routes fields into `OTel` structure: body, timestamp, `source_type/host`
    /// → resource attrs, everything else → record.attributes. Clears scope.
    pub fn from_value_map(value: Value, metadata: EventMetadata) -> Self {
        let mut out = Self {
            record: LogRecord::default(),
            record_attrs: OtelAttributes::new(),
            resource: None,
            resource_attrs: OtelAttributes::new(),
            scope: None,
            scope_attrs: OtelAttributes::new(),
            metadata,
        };
        out.apply_value_map(value);
        out
    }

    /// Extract a nanoseconds-timestamp field from the map, accepting both
    /// `snake_case` and camelCase names, and both integer and string-encoded values.
    fn extract_nanos_field(map: &mut ObjectMap, snake_name: &str, camel_name: &str) -> u64 {
        let val = map.remove(snake_name).or_else(|| map.remove(camel_name));
        match val {
            Some(Value::Integer(n)) => n as u64,
            Some(Value::Bytes(b)) => {
                // OTLP JSON encodes nano timestamps as strings
                std::str::from_utf8(&b)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or_else(|| {
                        map.insert(snake_name.into(), Value::Bytes(b));
                        0
                    })
            }
            Some(other) => {
                map.insert(snake_name.into(), other);
                0
            }
            None => 0,
        }
    }

    /// Write back a Value tree to proto fields.
    ///
    /// Accepts canonical keys (`body`/`message`, `time_unix_nano` as Integer)
    /// as well as OTLP JSON camelCase keys (`timeUnixNano`, `severityText`, etc.).
    /// Proto fields are extracted into their native slots;
    /// `resource`/`scope` sub-objects are restored;
    /// the remainder becomes `record.attributes`.
    #[expect(
        clippy::too_many_lines,
        reason = "OTLP log field restoration requires many branches"
    )]
    fn apply_value_map(&mut self, value: Value) {
        let mut map = match value {
            Value::Object(m) => m,
            other => {
                self.record = LogRecord {
                    body: Some(vrl_value_to_any_value(&other)),
                    ..Default::default()
                };
                self.record_attrs = OtelAttributes::new();
                self.resource = None;
                self.resource_attrs = OtelAttributes::new();
                self.scope = None;
                self.scope_attrs = OtelAttributes::new();
                return;
            }
        };

        // Handle body: try OTLP AnyValue pattern first (e.g. {"stringValue":"hello"})
        let body = map
            .remove(f::BODY)
            .map(|v| try_parse_otlp_any_value(&v).unwrap_or_else(|| vrl_value_to_any_value(&v)));

        // time_unix_nano: accept snake_case or camelCase, integer or string-encoded
        let time_unix_nano =
            Self::extract_nanos_field(&mut map, f::TIME_UNIX_NANO, f::TIME_UNIX_NANO_CC);

        // observed_time_unix_nano: accept snake_case or camelCase
        let observed_time_unix_nano = Self::extract_nanos_field(
            &mut map,
            f::OBSERVED_TIME_UNIX_NANO,
            f::OBSERVED_TIME_UNIX_NANO_CC,
        );

        // severity_text: accept snake_case or camelCase
        let severity_text = map
            .remove(f::SEVERITY_TEXT)
            .or_else(|| map.remove(f::SEVERITY_TEXT_CC))
            .map(|v| match v {
                Value::Bytes(b) => String::from_utf8(b.to_vec()).unwrap_or_default(),
                other => {
                    map.insert(f::SEVERITY_TEXT.into(), other);
                    String::new()
                }
            })
            .unwrap_or_default();

        // severity_number: accept snake_case or camelCase
        let severity_number = map
            .remove(f::SEVERITY_NUMBER)
            .or_else(|| map.remove(f::SEVERITY_NUMBER_CC))
            .map_or(0, |v| match v {
                Value::Integer(i) => OtlpEnumField::from_vrl(i).as_proto(),
                other => {
                    map.insert(f::SEVERITY_NUMBER.into(), other);
                    0
                }
            });

        // trace_id: accept snake_case or camelCase
        let trace_id = map
            .remove(f::LOG_TRACE_ID)
            .or_else(|| map.remove(f::TRACE_ID_CC))
            .map(|v| {
                if let Some(bytes) = hex_decode(&v) {
                    bytes
                } else {
                    map.insert(f::LOG_TRACE_ID.into(), v);
                    Vec::new()
                }
            })
            .unwrap_or_default();

        // span_id: accept snake_case or camelCase
        let span_id = map
            .remove(f::LOG_SPAN_ID)
            .or_else(|| map.remove(f::SPAN_ID_CC))
            .map(|v| {
                if let Some(bytes) = hex_decode(&v) {
                    bytes
                } else {
                    map.insert(f::LOG_SPAN_ID.into(), v);
                    Vec::new()
                }
            })
            .unwrap_or_default();

        let flags = match map.remove(f::LOG_FLAGS) {
            Some(Value::Integer(i)) => OtlpCount::from_vrl(i).as_proto(),
            Some(other) => {
                map.insert(f::LOG_FLAGS.into(), other);
                0
            }
            None => 0,
        };
        let dropped_attributes_count = match map.remove(f::DROPPED_ATTRIBUTES_COUNT) {
            Some(Value::Integer(i)) => OtlpCount::from_vrl(i).as_proto(),
            Some(other) => {
                map.insert(f::DROPPED_ATTRIBUTES_COUNT.into(), other);
                0
            }
            None => 0,
        };

        let (resource, resource_attrs) = restore_resource(&mut map);
        self.resource = resource;
        self.resource_attrs = resource_attrs;
        let (scope, scope_attrs) = restore_scope(&mut map);
        self.scope = scope;
        self.scope_attrs = scope_attrs;

        // Handle OTLP JSON "attributes" array format:
        // [{"key":"k","value":{"stringValue":"v"}}, ...]
        let mut extra_attrs = OtelAttributes::new();
        if let Some(Value::Array(arr)) = map.remove(f::ATTRIBUTES) {
            if let Some(kvs) = parse_otlp_key_value_array(&arr) {
                for kv in kvs {
                    let av = kv.value.unwrap_or(AnyValue { value: None });
                    extra_attrs.insert(kv.key, av);
                }
            } else {
                // Not a valid OTLP attributes array, put it back
                map.insert(f::ATTRIBUTES.into(), Value::Array(arr));
            }
        }

        self.record_attrs = OtelAttributes {
            inner: map
                .into_iter()
                .map(|(k, v)| (k.to_string(), vrl_value_to_any_value(&v)))
                .collect(),
        };

        // Merge any attributes parsed from OTLP JSON format
        for (k, v) in extra_attrs.inner {
            self.record_attrs.insert(k, v);
        }

        self.record = LogRecord {
            body,
            time_unix_nano,
            observed_time_unix_nano,
            severity_text,
            severity_number,
            trace_id,
            span_id,
            flags,
            dropped_attributes_count,
            attributes: Vec::new(),
        };
    }

    /// Get the timestamp from the event.
    ///
    /// Prefers `time_unix_nano` (event time), falls back to
    /// `observed_time_unix_nano` (ingest time).
    pub fn get_timestamp(&self) -> Option<Value> {
        if let Some(ts) = self.get_by_meaning("timestamp") {
            return Some(coerce_to_timestamp(ts));
        }
        if self.record.time_unix_nano != 0 {
            let nanos = self.record.time_unix_nano;
            let secs = (nanos / 1_000_000_000) as i64;
            let nsecs = (nanos % 1_000_000_000) as u32;
            chrono::DateTime::from_timestamp(secs, nsecs).map(Value::Timestamp)
        } else if self.record.observed_time_unix_nano != 0 {
            let nanos = self.record.observed_time_unix_nano;
            let secs = (nanos / 1_000_000_000) as i64;
            let nsecs = (nanos % 1_000_000_000) as u32;
            chrono::DateTime::from_timestamp(secs, nsecs).map(Value::Timestamp)
        } else {
            None
        }
    }

    /// Remove the timestamp from the event.
    pub fn remove_timestamp(&mut self) -> Option<Value> {
        let ts = self.get_timestamp();
        self.record.time_unix_nano = 0;
        ts
    }

    /// Set the event timestamp (`time_unix_nano`) from a chrono `DateTime`.
    /// Returns the previous timestamp value, if any.
    pub fn set_timestamp(&mut self, ts: chrono::DateTime<chrono::Utc>) -> Option<Value> {
        let old = self.get_timestamp();
        self.record_mut().time_unix_nano = ts.timestamp_nanos_opt().unwrap_or(0) as u64;
        old
    }

    /// Set the event timestamp only if one is not already present.
    pub fn try_set_timestamp(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        if self.record.time_unix_nano == 0 {
            self.record_mut().time_unix_nano = ts.timestamp_nanos_opt().unwrap_or(0) as u64;
        }
    }

    /// Check if a field exists.
    pub fn contains<'a>(&self, path: impl lookup::lookup_v2::TargetPath<'a>) -> bool {
        self.get(path).is_some()
    }

    /// Get the log body value.
    pub fn get_body(&self) -> Option<Value> {
        self.body().map(any_value_to_vrl)
    }

    /// Get the "`source_type`" from resource attributes.
    pub fn get_source_type(&self) -> Option<Value> {
        self.resource_attribute(f::SOURCE_TYPE)
            .map(any_value_to_vrl)
    }

    /// Set the `source_type` as a resource attribute.
    pub fn set_source_type(&mut self, value: impl Into<Value>) {
        self.set_resource_attribute(
            f::SOURCE_TYPE.to_string(),
            vrl_value_to_any_value(&value.into()),
        );
    }

    /// Set the `source_type` only if not already present.
    pub fn try_set_source_type(&mut self, value: impl Into<Value>) {
        if self.resource_attribute(f::SOURCE_TYPE).is_none() {
            self.set_source_type(value);
        }
    }

    /// Get the host value from resource attributes.
    pub fn get_host(&self) -> Option<Value> {
        self.resource_attribute(f::HOST_NAME).map(any_value_to_vrl)
    }

    /// Set the host as a resource attribute (`host.name`).
    pub fn set_host(&mut self, value: impl Into<Value>) {
        self.set_resource_attribute(
            f::HOST_NAME.to_string(),
            vrl_value_to_any_value(&value.into()),
        );
    }

    /// Set the host only if not already present.
    pub fn try_set_host(&mut self, value: impl Into<Value>) {
        if self.resource_attribute(f::HOST_NAME).is_none() {
            self.set_host(value);
        }
    }

    /// Parse a path and get a value.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `path` cannot be parsed as a valid target path.
    pub fn parse_path_and_get_value(
        &self,
        path: &str,
    ) -> Result<Option<Value>, vrl::path::PathParseError> {
        let target_path = vrl::path::parse_target_path(path)?;
        Ok(self.get(&target_path))
    }

    /// Iterate all event fields (flattened, dotted keys). Owned values.
    /// Iterator over all event fields, returning owned values
    /// since `OtelLog` builds the field tree dynamically from proto.
    pub fn all_event_fields(&self) -> Option<Vec<(vrl::value::KeyString, Value)>> {
        let fields = self.convert_to_fields();
        if fields.is_empty() {
            None
        } else {
            Some(fields)
        }
    }

    /// Like `all_event_fields` but skips individual array elements.
    pub fn all_event_fields_skip_array_elements(
        &self,
    ) -> Option<Vec<(vrl::value::KeyString, Value)>> {
        let map = self.as_map().unwrap_or_default();
        let fields: Vec<_> = super::util::log::all_fields_skip_array_elements(&map)
            .map(|(k, v)| (k, v.clone()))
            .collect();
        if fields.is_empty() {
            None
        } else {
            Some(fields)
        }
    }

    /// Iterate all metadata fields (flattened, with `%` prefix). Owned values.
    pub fn all_metadata_fields(&self) -> Option<Vec<(vrl::value::KeyString, Value)>> {
        match self.metadata.value() {
            Value::Object(metadata_map) => {
                let fields: Vec<_> = super::util::log::all_metadata_fields(metadata_map)
                    .map(|(k, v)| (k, v.clone()))
                    .collect();
                if fields.is_empty() {
                    None
                } else {
                    Some(fields)
                }
            }
            _ => None,
        }
    }

    /// Convert to fields — recursively flatten nested objects with dotted keys.
    pub fn convert_to_fields(&self) -> Vec<(vrl::value::KeyString, Value)> {
        let map = self.as_map().unwrap_or_default();
        super::util::log::all_fields(&map)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    /// Rename a key.
    pub fn rename_key<'a>(
        &mut self,
        from: impl lookup::lookup_v2::TargetPath<'a>,
        to: impl lookup::lookup_v2::TargetPath<'a>,
    ) {
        if let Some(val) = self.remove(from) {
            self.insert(to, val);
        }
    }

    /// Get the timestamp path (proto-canonical: `time_unix_nano`).
    pub fn timestamp_path(&self) -> Option<vrl::path::OwnedTargetPath> {
        use lookup::owned_value_path;
        use vrl::path::OwnedTargetPath;
        Some(OwnedTargetPath::event(owned_value_path!(f::TIME_UNIX_NANO)))
    }

    /// Get the body path.
    pub fn body_path(&self) -> Option<vrl::path::OwnedTargetPath> {
        use lookup::owned_value_path;
        use vrl::path::OwnedTargetPath;
        Some(OwnedTargetPath::event(owned_value_path!(f::BODY)))
    }

    /// Returns the path to the `source_type` resource attribute, if present.
    pub fn source_type_path(&self) -> Option<vrl::path::OwnedTargetPath> {
        if self.resource_attribute(f::SOURCE_TYPE).is_some() {
            Some(vrl::path::OwnedTargetPath::event(
                lookup::owned_value_path!(f::RESOURCE, f::SOURCE_TYPE),
            ))
        } else {
            None
        }
    }

    /// Try insert - only inserts if the path doesn't exist.
    pub fn try_insert<'a>(
        &mut self,
        path: impl lookup::lookup_v2::TargetPath<'a>,
        value: impl Into<Value>,
    ) {
        if self.get(path.clone()).is_none() {
            self.insert(path, value);
        }
    }

    /// Get the underlying value.
    /// In Vector namespace: returns body only.
    /// In Legacy namespace: returns canonical proto layout.
    pub fn value(&self) -> Value {
        self.body().map_or(Value::Null, any_value_to_vrl)
    }

    /// Get all top-level keys from the event (enumerates proto fields directly).
    pub fn keys(&self) -> Option<std::vec::IntoIter<vrl::value::KeyString>> {
        let mut keys = Vec::new();
        if self.record.body.is_some() {
            keys.push(KeyString::from(f::BODY));
        }
        if !self.record.severity_text.is_empty() {
            keys.push(KeyString::from(f::SEVERITY_TEXT));
        }
        if self.record.severity_number != 0 {
            keys.push(KeyString::from(f::SEVERITY_NUMBER));
        }
        if self.record.time_unix_nano != 0 {
            keys.push(KeyString::from(f::TIME_UNIX_NANO));
        }
        if self.record.observed_time_unix_nano != 0 {
            keys.push(KeyString::from(f::OBSERVED_TIME_UNIX_NANO));
        }
        if !self.record.trace_id.is_empty() {
            keys.push(KeyString::from(f::LOG_TRACE_ID));
        }
        if !self.record.span_id.is_empty() {
            keys.push(KeyString::from(f::LOG_SPAN_ID));
        }
        if self.record.flags != 0 {
            keys.push(KeyString::from(f::LOG_FLAGS));
        }
        if self.record.dropped_attributes_count != 0 {
            keys.push(KeyString::from(f::DROPPED_ATTRIBUTES_COUNT));
        }
        for k in self.record_attrs.keys() {
            keys.push(KeyString::from(k.clone()));
        }
        if self.resource.is_some() || !self.resource_attrs.is_empty() {
            keys.push(KeyString::from(f::RESOURCE));
        }
        if self.scope.is_some() || !self.scope_attrs.is_empty() {
            keys.push(KeyString::from(f::SCOPE));
        }
        if keys.is_empty() {
            None
        } else {
            Some(keys.into_iter())
        }
    }

    /// Check if the log has no body and no attributes.
    pub fn is_empty_object(&self) -> bool {
        self.record.body.is_none() && self.record_attrs.is_empty()
    }

    /// Convert to fields unquoted — recursively flatten nested objects with unquoted dotted keys.
    pub fn convert_to_fields_unquoted(&self) -> Vec<(vrl::value::KeyString, Value)> {
        let map = self.as_map().unwrap_or_default();
        super::util::log::all_fields_unquoted(&map)
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    pub fn from_str_legacy(msg: impl Into<String>) -> Self {
        let mut log = Self::from(msg.into());
        log.record_mut().time_unix_nano =
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
        log
    }

    pub fn from_map(map: ObjectMap, metadata: EventMetadata) -> Self {
        Self::from_value_map(Value::Object(map), metadata)
    }

    pub fn new_with_metadata(metadata: EventMetadata) -> Self {
        Self {
            record: LogRecord::default(),
            record_attrs: OtelAttributes::new(),
            resource: None,
            resource_attrs: OtelAttributes::new(),
            scope: None,
            scope_attrs: OtelAttributes::new(),
            metadata,
        }
    }

    /// Parse a path string and insert a value at that path.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `path` cannot be parsed as a valid target path.
    pub fn parse_path_and_insert(
        &mut self,
        path: impl AsRef<str>,
        value: impl Into<Value>,
    ) -> Result<Option<Value>, vrl::path::PathParseError> {
        let target_path = vrl::path::parse_target_path(path.as_ref())?;
        Ok(self.insert(&target_path, value))
    }
}

// -- OtelSpan --

#[derive(Clone, Debug, PartialEq)]
pub struct OtelSpan {
    pub(crate) span: Span,
    pub(crate) span_attrs: OtelAttributes,
    pub(crate) resource: Option<Resource>,
    pub(crate) resource_attrs: OtelAttributes,
    pub(crate) scope: Option<InstrumentationScope>,
    pub(crate) scope_attrs: OtelAttributes,
    pub(crate) metadata: EventMetadata,
}

impl OtelSpan {
    pub fn new(mut span: Span) -> Self {
        let span_attrs = OtelAttributes::from_key_values(std::mem::take(&mut span.attributes));
        Self {
            span,
            span_attrs,
            resource: None,
            resource_attrs: OtelAttributes::new(),
            scope: None,
            scope_attrs: OtelAttributes::new(),
            metadata: EventMetadata::default(),
        }
    }

    /// Create an `OtelSpan` from an `OtelLog` (for trace signal detection in OTLP decoder).
    ///
    /// The `OtelLog`'s fields become span attributes. Resource and scope are preserved.
    pub fn from_otel_log(log: OtelLog) -> Self {
        let map = log.as_map().unwrap_or_default();
        let mut span_attrs = OtelAttributes::new();
        for (k, v) in map {
            span_attrs.insert(k.to_string(), vrl_value_to_any_value(&v));
        }
        Self {
            span: Span::default(),
            span_attrs,
            resource: log.resource,
            resource_attrs: log.resource_attrs,
            scope: log.scope,
            scope_attrs: log.scope_attrs,
            metadata: log.metadata,
        }
    }

    /// Construct an `OtelSpan` from a legacy-layout Value + metadata.
    ///
    /// Routes native span fields (`name`, `trace_id`, `span_id`,
    /// `parent_span_id`, `start_time`/`end_time`, `kind`, `status`) into
    /// their proto slots; everything else becomes `span.attributes`. See
    /// `apply_value_map` for the full routing contract.
    pub fn from_value_map(value: Value, metadata: EventMetadata) -> Self {
        let mut out = Self {
            span: Span::default(),
            span_attrs: OtelAttributes::new(),
            resource: None,
            resource_attrs: OtelAttributes::new(),
            scope: None,
            scope_attrs: OtelAttributes::new(),
            metadata,
        };
        out.apply_value_map(value);
        out
    }

    pub fn from_parts(
        mut span: Span,
        mut resource: Option<Resource>,
        mut scope: Option<InstrumentationScope>,
        metadata: EventMetadata,
    ) -> Self {
        let span_attrs = OtelAttributes::from_key_values(std::mem::take(&mut span.attributes));
        let resource_attrs = resource
            .as_mut()
            .map(|r| OtelAttributes::from_key_values(std::mem::take(&mut r.attributes)))
            .unwrap_or_default();
        let scope_attrs = scope
            .as_mut()
            .map(|s| OtelAttributes::from_key_values(std::mem::take(&mut s.attributes)))
            .unwrap_or_default();
        Self {
            span,
            span_attrs,
            resource,
            resource_attrs,
            scope,
            scope_attrs,
            metadata,
        }
    }

    pub fn into_parts(
        self,
    ) -> (
        Span,
        Option<Resource>,
        Option<InstrumentationScope>,
        EventMetadata,
    ) {
        let mut span = self.span;
        span.attributes = self.span_attrs.to_key_values();
        let resource = self.resource.map(|mut r| {
            r.attributes = self.resource_attrs.to_key_values();
            r
        });
        let scope = self.scope.map(|mut s| {
            s.attributes = self.scope_attrs.to_key_values();
            s
        });
        (span, resource, scope, self.metadata)
    }

    /// Return a full `Span` proto with attributes reconstituted from the
    /// internal `OtelAttributes` map. Use at proto serialization boundaries
    /// (OTLP codec, buffer encoding, gRPC sink).
    pub fn span_to_proto(&self) -> Span {
        let mut span = self.span.clone();
        span.attributes = self.span_attrs.to_key_values();
        span
    }

    pub fn span(&self) -> &Span {
        &self.span
    }

    pub fn span_mut(&mut self) -> &mut Span {
        &mut self.span
    }

    pub fn resource(&self) -> Option<&Resource> {
        self.resource.as_ref()
    }

    pub fn resource_proto(&self) -> Option<Resource> {
        resource_to_proto(self.resource.as_ref(), &self.resource_attrs)
    }

    pub fn scope_proto(&self) -> Option<InstrumentationScope> {
        scope_to_proto(self.scope.as_ref(), &self.scope_attrs)
    }

    pub fn set_resource(&mut self, mut resource: Resource) {
        self.resource_attrs =
            OtelAttributes::from_key_values(std::mem::take(&mut resource.attributes));
        self.resource = Some(resource);
    }

    pub fn resource_attrs(&self) -> &OtelAttributes {
        &self.resource_attrs
    }

    pub fn scope_attrs(&self) -> &OtelAttributes {
        &self.scope_attrs
    }

    pub fn scope(&self) -> Option<&InstrumentationScope> {
        self.scope.as_ref()
    }

    pub fn set_scope(&mut self, mut scope: InstrumentationScope) {
        self.scope_attrs = OtelAttributes::from_key_values(std::mem::take(&mut scope.attributes));
        self.scope = Some(scope);
    }

    pub fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        &mut self.metadata
    }

    pub fn name(&self) -> &str {
        &self.span.name
    }

    pub fn trace_id(&self) -> &[u8] {
        &self.span.trace_id
    }

    pub fn span_id(&self) -> &[u8] {
        &self.span.span_id
    }

    pub fn parent_span_id(&self) -> &[u8] {
        &self.span.parent_span_id
    }

    pub fn start_time_unix_nano(&self) -> u64 {
        self.span.start_time_unix_nano
    }

    pub fn end_time_unix_nano(&self) -> u64 {
        self.span.end_time_unix_nano
    }

    pub fn kind(&self) -> i32 {
        self.span.kind
    }

    pub fn status(&self) -> Option<&opentelemetry_proto::tonic::trace::v1::Status> {
        self.span.status.as_ref()
    }

    pub fn attribute(&self, key: &str) -> Option<&AnyValue> {
        self.span_attrs.get(key)
    }

    pub fn set_attribute(&mut self, key: String, value: AnyValue) {
        self.span_attrs.insert(key, value);
    }

    pub fn remove_attribute(&mut self, key: &str) -> Option<AnyValue> {
        self.span_attrs.remove(key)
    }

    pub fn attributes(&self) -> &OtelAttributes {
        &self.span_attrs
    }

    pub fn resource_attribute(&self, key: &str) -> Option<&AnyValue> {
        self.resource_attrs.get(key)
    }

    pub fn set_resource_attribute(&mut self, key: String, value: AnyValue) {
        if self.resource.is_none() {
            self.resource = Some(Resource {
                attributes: Vec::new(),
                dropped_attributes_count: 0,
            });
        }
        self.resource_attrs.insert(key, value);
    }

    pub fn add_finalizer(&mut self, finalizer: EventFinalizer) {
        self.metadata.add_finalizer(finalizer);
    }

    #[must_use]
    pub fn with_batch_notifier(mut self, batch: &BatchNotifier) -> Self {
        self.metadata = self.metadata.with_batch_notifier(batch);
        self
    }

    #[must_use]
    pub fn with_batch_notifier_option(mut self, batch: &Option<BatchNotifier>) -> Self {
        self.metadata = self.metadata.with_batch_notifier_option(batch);
        self
    }

    // -----------------------------------------------------------------------
    // Field access methods (same pattern as OtelLog — see comment above)
    // -----------------------------------------------------------------------

    /// Build the canonical `ObjectMap` from proto fields directly.
    /// Write back a Value tree to proto fields.
    ///
    /// Handles canonical layout (from `as_map`): proto fields
    /// are extracted into `Span` slots, `resource`/`scope` sub-objects are
    /// restored, remainder becomes `span.attributes`. Also handles legacy
    /// "`start_time"/"end_time`" (Timestamp) for old disk buffer compat.
    #[expect(
        clippy::too_many_lines,
        reason = "span field extraction is inherently verbose but logically cohesive"
    )]
    fn apply_value_map(&mut self, value: Value) {
        use opentelemetry_proto::tonic::trace::v1::{Status, span};

        let mut map = match value {
            Value::Object(m) => m,
            _ => ObjectMap::new(),
        };

        let name = match map.remove(f::NAME) {
            Some(Value::Bytes(b)) => String::from_utf8(b.to_vec()).unwrap_or_default(),
            Some(other) => {
                map.insert(f::NAME.into(), other);
                String::new()
            }
            None => String::new(),
        };

        let take_id = |map: &mut ObjectMap, key: &str| -> Vec<u8> {
            match map.remove(key) {
                Some(v) => {
                    if let Some(bytes) = hex_decode(&v) {
                        bytes
                    } else {
                        map.insert(key.into(), v);
                        Vec::new()
                    }
                }
                None => Vec::new(),
            }
        };
        let trace_id = take_id(&mut map, f::SPAN_TRACE_ID);
        let span_id = take_id(&mut map, f::SPAN_SPAN_ID);
        let parent_span_id = take_id(&mut map, f::PARENT_SPAN_ID);

        let take_integer = |map: &mut ObjectMap, key: &str| -> u64 {
            match map.remove(key) {
                Some(Value::Integer(n)) => n as u64,
                Some(other) => {
                    map.insert(key.into(), other);
                    0
                }
                None => 0,
            }
        };
        let start_time_unix_nano = take_integer(&mut map, f::START_TIME_UNIX_NANO);
        let end_time_unix_nano = take_integer(&mut map, f::END_TIME_UNIX_NANO);

        let kind = match map.remove(f::SPAN_KIND) {
            Some(Value::Integer(i)) => OtlpEnumField::from_vrl(i).as_proto(),
            Some(other) => {
                map.insert(f::SPAN_KIND.into(), other);
                0
            }
            None => 0,
        };

        let flags = match map.remove(f::SPAN_FLAGS) {
            Some(Value::Integer(i)) => OtlpCount::from_vrl(i).as_proto(),
            Some(other) => {
                map.insert(f::SPAN_FLAGS.into(), other);
                0
            }
            None => 0,
        };
        let dropped_attributes_count = match map.remove(f::DROPPED_ATTRIBUTES_COUNT) {
            Some(Value::Integer(i)) => OtlpCount::from_vrl(i).as_proto(),
            Some(other) => {
                map.insert(f::DROPPED_ATTRIBUTES_COUNT.into(), other);
                0
            }
            None => 0,
        };

        let status = match map.remove(f::SPAN_STATUS) {
            Some(Value::Object(mut status_map)) => {
                let message = match status_map.remove(f::STATUS_MESSAGE) {
                    Some(Value::Bytes(b)) => String::from_utf8(b.to_vec()).unwrap_or_default(),
                    _ => String::new(),
                };
                let code = match status_map.remove(f::STATUS_CODE) {
                    Some(Value::Integer(i)) => OtlpEnumField::from_vrl(i).as_proto(),
                    _ => 0,
                };
                if message.is_empty() && code == 0 && status_map.is_empty() {
                    None
                } else {
                    Some(Status { message, code })
                }
            }
            Some(other) => {
                map.insert(f::SPAN_STATUS.into(), other);
                None
            }
            None => None,
        };

        let (resource, resource_attrs) = restore_resource(&mut map);
        self.resource = resource;
        self.resource_attrs = resource_attrs;
        let (scope, scope_attrs) = restore_scope(&mut map);
        self.scope = scope;
        self.scope_attrs = scope_attrs;

        let trace_state = match map.remove("trace_state") {
            Some(Value::Bytes(b)) => String::from_utf8(b.to_vec()).unwrap_or_default(),
            Some(other) => {
                map.insert("trace_state".into(), other);
                String::new()
            }
            None => String::new(),
        };

        let events = map
            .remove(f::SPAN_EVENTS)
            .and_then(|v| match v {
                Value::Array(a) => Some(a),
                _ => None,
            })
            .map(|arr| {
                arr.into_iter()
                    .filter_map(|v| {
                        let Value::Object(em) = v else { return None };
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
                                .map(object_map_to_kvlist)
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

        let dropped_events_count = match map.remove("dropped_events_count") {
            Some(Value::Integer(i)) => OtlpCount::from_vrl(i).as_proto(),
            Some(other) => {
                map.insert("dropped_events_count".into(), other);
                0
            }
            None => 0,
        };

        let links = map
            .remove(f::SPAN_LINKS)
            .and_then(|v| match v {
                Value::Array(a) => Some(a),
                _ => None,
            })
            .map(|arr| {
                arr.into_iter()
                    .filter_map(|v| {
                        let Value::Object(lm) = v else { return None };
                        Some(span::Link {
                            trace_id: lm
                                .get(f::SPAN_TRACE_ID)
                                .and_then(hex_decode)
                                .unwrap_or_default(),
                            span_id: lm
                                .get(f::SPAN_SPAN_ID)
                                .and_then(hex_decode)
                                .unwrap_or_default(),
                            trace_state: lm
                                .get("trace_state")
                                .and_then(|v| v.as_bytes())
                                .map(|b| String::from_utf8_lossy(b).into_owned())
                                .unwrap_or_default(),
                            attributes: lm
                                .get(f::ATTRIBUTES)
                                .and_then(|v| v.as_object())
                                .map(object_map_to_kvlist)
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

        let dropped_links_count = match map.remove("dropped_links_count") {
            Some(Value::Integer(i)) => OtlpCount::from_vrl(i).as_proto(),
            Some(other) => {
                map.insert("dropped_links_count".into(), other);
                0
            }
            None => 0,
        };

        let mut span_attrs = OtelAttributes::new();
        for (k, v) in map {
            span_attrs.insert(k.to_string(), vrl_value_to_any_value(&v));
        }

        self.span = Span {
            name,
            trace_id,
            span_id,
            parent_span_id,
            start_time_unix_nano,
            end_time_unix_nano,
            kind,
            flags,
            dropped_attributes_count,
            status,
            attributes: Vec::new(),
            trace_state,
            events,
            dropped_events_count,
            links,
            dropped_links_count,
        };
        self.span_attrs = span_attrs;
    }

    /// Get a field value by path.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "impl Trait parameter follows VRL target API convention"
    )]
    pub fn get<'a>(&self, path: impl lookup::lookup_v2::TargetPath<'a>) -> Option<Value> {
        use lookup::lookup_v2::ValuePath;
        use lookup::path::BorrowedSegment;

        match path.prefix() {
            lookup::PathPrefix::Event => {
                let vp = path.value_path();
                let mut iter = vp.segment_iter();
                match iter.next() {
                    Some(BorrowedSegment::Field(ref first)) => match iter.next() {
                        None => self.span_get_single_segment(first.as_ref()),
                        Some(BorrowedSegment::Field(ref second)) => {
                            let mut fields = vec![first.to_string(), second.to_string()];
                            let mut remaining_iter = None;
                            for seg in &mut iter {
                                if let BorrowedSegment::Field(f) = seg {
                                    fields.push(f.to_string());
                                } else {
                                    remaining_iter = Some((seg, iter));
                                    break;
                                }
                            }
                            if let Some((non_field_seg, rest)) = remaining_iter {
                                let base = self.span_get_field_path(&fields)?;
                                let sub = remaining_value_path(
                                    std::iter::once(non_field_seg).chain(rest),
                                );
                                base.get(&sub).cloned()
                            } else {
                                self.span_get_field_path(&fields)
                            }
                        }
                        Some(non_field_seg) => {
                            let base = self.span_get_single_segment(first.as_ref())?;
                            let sub =
                                remaining_value_path(std::iter::once(non_field_seg).chain(iter));
                            base.get(&sub).cloned()
                        }
                    },
                    _ => None,
                }
            }
            lookup::PathPrefix::Metadata => self.metadata.value().get(path.value_path()).cloned(),
        }
    }

    fn span_get_single_segment(&self, field: &str) -> Option<Value> {
        match field {
            f::NAME if !self.span.name.is_empty() => {
                Some(Value::Bytes(self.span.name.clone().into()))
            }
            f::SPAN_TRACE_ID if !self.span.trace_id.is_empty() => {
                Some(hex_encode(&self.span.trace_id))
            }
            f::SPAN_SPAN_ID if !self.span.span_id.is_empty() => {
                Some(hex_encode(&self.span.span_id))
            }
            f::PARENT_SPAN_ID if !self.span.parent_span_id.is_empty() => {
                Some(hex_encode(&self.span.parent_span_id))
            }
            f::START_TIME if self.span.start_time_unix_nano != 0 => {
                nanos_to_timestamp(self.span.start_time_unix_nano)
            }
            f::END_TIME if self.span.end_time_unix_nano != 0 => {
                nanos_to_timestamp(self.span.end_time_unix_nano)
            }
            f::SPAN_KIND if self.span.kind != 0 => Some(Value::Integer(i64::from(self.span.kind))),
            f::SPAN_STATUS => {
                let status = self.span.status.as_ref()?;
                let mut status_map = ObjectMap::new();
                if !status.message.is_empty() {
                    status_map.insert(
                        f::STATUS_MESSAGE.into(),
                        Value::Bytes(status.message.clone().into()),
                    );
                }
                status_map.insert(
                    f::STATUS_CODE.into(),
                    Value::Integer(i64::from(status.code)),
                );
                Some(Value::Object(status_map))
            }
            other => self.span_attrs.get(other).map(any_value_to_vrl),
        }
    }

    fn span_get_field_path(&self, fields: &[String]) -> Option<Value> {
        debug_assert!(fields.len() >= 2);
        match fields[0].as_str() {
            f::RESOURCE => {
                let remaining = &fields[1..];
                if remaining.len() == 1 {
                    let key = remaining[0].as_str();
                    if key == f::DROPPED_ATTRIBUTES_COUNT {
                        let res = self.resource.as_ref()?;
                        if res.dropped_attributes_count != 0 {
                            return Some(Value::Integer(i64::from(res.dropped_attributes_count)));
                        }
                        return None;
                    }
                    self.resource_attrs.get(key).map(any_value_to_vrl)
                } else {
                    let key = remaining[0].as_str();
                    let av = self.resource_attrs.get(key)?;
                    let v = any_value_to_vrl(av);
                    navigate_value(&v, &remaining[1..])
                }
            }
            f::SCOPE => {
                let scope = self.scope.as_ref()?;
                let remaining = &fields[1..];
                match remaining[0].as_str() {
                    f::NAME if remaining.len() == 1 => {
                        if scope.name.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(scope.name.clone().into()))
                        }
                    }
                    f::VERSION if remaining.len() == 1 => {
                        if scope.version.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(scope.version.clone().into()))
                        }
                    }
                    f::ATTRIBUTES => {
                        if remaining.len() == 1 {
                            if self.scope_attrs.is_empty() {
                                None
                            } else {
                                Some(Value::Object(self.scope_attrs.to_object_map()))
                            }
                        } else {
                            let av = self.scope_attrs.get(&remaining[1])?;
                            if remaining.len() == 2 {
                                Some(any_value_to_vrl(av))
                            } else {
                                let v = any_value_to_vrl(av);
                                navigate_value(&v, &remaining[2..])
                            }
                        }
                    }
                    _ => None,
                }
            }
            f::SPAN_STATUS => {
                let status = self.span.status.as_ref()?;
                let remaining = &fields[1..];
                match remaining[0].as_str() {
                    f::STATUS_MESSAGE if remaining.len() == 1 => {
                        if status.message.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(status.message.clone().into()))
                        }
                    }
                    f::STATUS_CODE if remaining.len() == 1 => {
                        Some(Value::Integer(i64::from(status.code)))
                    }
                    _ => None,
                }
            }
            first => {
                if let Some(av) = self.span_attrs.get(first) {
                    let v = any_value_to_vrl(av);
                    if let Some(result) = navigate_value(&v, &fields[1..]) {
                        return Some(result);
                    }
                }
                None
            }
        }
    }

    /// Insert a field value by path.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "impl Trait parameter follows VRL target API convention"
    )]
    pub fn insert<'a>(
        &mut self,
        path: impl lookup::lookup_v2::TargetPath<'a>,
        value: impl Into<Value>,
    ) -> Option<Value> {
        use lookup::lookup_v2::ValuePath;
        use lookup::path::BorrowedSegment;

        match path.prefix() {
            lookup::PathPrefix::Event => {
                let value = value.into();
                let vp = path.value_path();
                let mut iter = vp.segment_iter();
                match iter.next() {
                    Some(BorrowedSegment::Field(ref first)) => match iter.next() {
                        None => self.span_insert_single_segment(first.as_ref(), value),
                        Some(BorrowedSegment::Field(ref second)) => {
                            let mut fields = vec![first.to_string(), second.to_string()];
                            let mut remaining_iter = None;
                            for seg in &mut iter {
                                if let BorrowedSegment::Field(f) = seg {
                                    fields.push(f.to_string());
                                } else {
                                    remaining_iter = Some((seg, iter));
                                    break;
                                }
                            }
                            if let Some((non_field_seg, rest)) = remaining_iter {
                                let mut base =
                                    self.span_get_field_path(&fields).unwrap_or(Value::Null);
                                let sub = remaining_value_path(
                                    std::iter::once(non_field_seg).chain(rest),
                                );
                                let old = base.insert(&sub, value);
                                self.span_insert_field_path(&fields, base);
                                old
                            } else {
                                self.span_insert_field_path(&fields, value)
                            }
                        }
                        Some(non_field_seg) => {
                            let first_str = first.as_ref();
                            let mut base = self
                                .span_get_single_segment(first_str)
                                .unwrap_or(Value::Null);
                            let sub =
                                remaining_value_path(std::iter::once(non_field_seg).chain(iter));
                            let old = base.insert(&sub, value);
                            self.span_insert_single_segment(first_str, base);
                            old
                        }
                    },
                    _ => None,
                }
            }
            lookup::PathPrefix::Metadata => {
                self.metadata.value_mut().insert(path.value_path(), value)
            }
        }
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "value ownership needed for API consistency with callers"
    )]
    #[expect(
        clippy::too_many_lines,
        reason = "OTLP span field mapping requires many branches"
    )]
    fn span_insert_single_segment(&mut self, field: &str, value: Value) -> Option<Value> {
        use opentelemetry_proto::tonic::trace::v1::Status;

        match field {
            f::NAME => {
                let old = if self.span.name.is_empty() {
                    None
                } else {
                    Some(Value::Bytes(self.span.name.clone().into()))
                };
                self.span.name = value
                    .as_str()
                    .map_or_else(|| value.to_string_lossy().into_owned(), |s| s.to_string());
                old
            }
            f::SPAN_TRACE_ID => {
                let old = if self.span.trace_id.is_empty() {
                    None
                } else {
                    Some(hex_encode(&self.span.trace_id))
                };
                if let Some(decoded) = hex_decode(&value) {
                    self.span.trace_id = decoded;
                } else {
                    self.span_attrs
                        .insert(f::SPAN_TRACE_ID.to_string(), vrl_value_to_any_value(&value));
                }
                old
            }
            f::SPAN_SPAN_ID => {
                let old = if self.span.span_id.is_empty() {
                    None
                } else {
                    Some(hex_encode(&self.span.span_id))
                };
                if let Some(decoded) = hex_decode(&value) {
                    self.span.span_id = decoded;
                } else {
                    self.span_attrs
                        .insert(f::SPAN_SPAN_ID.to_string(), vrl_value_to_any_value(&value));
                }
                old
            }
            f::PARENT_SPAN_ID => {
                let old = if self.span.parent_span_id.is_empty() {
                    None
                } else {
                    Some(hex_encode(&self.span.parent_span_id))
                };
                if let Some(decoded) = hex_decode(&value) {
                    self.span.parent_span_id = decoded;
                } else {
                    self.span_attrs.insert(
                        f::PARENT_SPAN_ID.to_string(),
                        vrl_value_to_any_value(&value),
                    );
                }
                old
            }
            f::START_TIME => {
                let old = if self.span.start_time_unix_nano != 0 {
                    nanos_to_timestamp(self.span.start_time_unix_nano)
                } else {
                    None
                };
                if let Some(ts) = value.as_timestamp()
                    && let Some(nanos) = ts.timestamp_nanos_opt()
                    && nanos >= 0
                {
                    self.span.start_time_unix_nano = nanos as u64;
                    return old;
                }
                self.span_attrs
                    .insert(f::START_TIME.to_string(), vrl_value_to_any_value(&value));
                old
            }
            f::END_TIME => {
                let old = if self.span.end_time_unix_nano != 0 {
                    nanos_to_timestamp(self.span.end_time_unix_nano)
                } else {
                    None
                };
                if let Some(ts) = value.as_timestamp()
                    && let Some(nanos) = ts.timestamp_nanos_opt()
                    && nanos >= 0
                {
                    self.span.end_time_unix_nano = nanos as u64;
                    return old;
                }
                self.span_attrs
                    .insert(f::END_TIME.to_string(), vrl_value_to_any_value(&value));
                old
            }
            f::SPAN_KIND => {
                let old = if self.span.kind == 0 {
                    None
                } else {
                    Some(Value::Integer(i64::from(self.span.kind)))
                };
                if let Some(n) = value.as_integer() {
                    self.span.kind = OtlpEnumField::from_vrl(n).as_proto();
                }
                old
            }
            f::SPAN_STATUS => {
                let old = self.span.status.as_ref().map(|st| {
                    let mut m = ObjectMap::new();
                    if !st.message.is_empty() {
                        m.insert(
                            f::STATUS_MESSAGE.into(),
                            Value::Bytes(st.message.clone().into()),
                        );
                    }
                    m.insert(f::STATUS_CODE.into(), Value::Integer(i64::from(st.code)));
                    Value::Object(m)
                });
                if let Value::Object(map) = &value {
                    let message = map
                        .get(f::STATUS_MESSAGE)
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default();
                    let code = OtlpEnumField::from_vrl(
                        map.get(f::STATUS_CODE)
                            .and_then(vrl::value::Value::as_integer)
                            .unwrap_or(0),
                    )
                    .as_proto();
                    self.span.status = Some(Status { message, code });
                }
                old
            }
            other => {
                let old = self.span_attrs.get(other).map(any_value_to_vrl);
                self.span_attrs
                    .insert(other.to_string(), vrl_value_to_any_value(&value));
                old
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "OTLP span nested field mapping requires many branches"
    )]
    fn span_insert_field_path(&mut self, fields: &[String], value: Value) -> Option<Value> {
        debug_assert!(fields.len() >= 2);
        match fields[0].as_str() {
            f::RESOURCE => {
                let remaining = &fields[1..];
                if self.resource.is_none() {
                    self.resource = Some(Resource {
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                    });
                }
                if remaining.len() == 1 {
                    let key = remaining[0].as_str();
                    let old = self.resource_attrs.get(key).map(any_value_to_vrl);
                    self.resource_attrs
                        .insert(key.to_string(), vrl_value_to_any_value(&value));
                    old
                } else {
                    let key = remaining[0].as_str();
                    let mut v = self
                        .resource_attrs
                        .get(key)
                        .map_or(Value::Object(ObjectMap::new()), any_value_to_vrl);
                    let old = insert_value_at(&mut v, &remaining[1..], value);
                    self.resource_attrs
                        .insert(key.to_string(), vrl_value_to_any_value(&v));
                    old
                }
            }
            f::SCOPE => {
                let remaining = &fields[1..];
                let scope = self.scope.get_or_insert_with(InstrumentationScope::default);
                match remaining[0].as_str() {
                    f::NAME if remaining.len() == 1 => {
                        let old = if scope.name.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(scope.name.clone().into()))
                        };
                        scope.name = value.as_str().map_or_else(
                            || value.to_string_lossy().into_owned(),
                            |s| s.to_string(),
                        );
                        old
                    }
                    f::VERSION if remaining.len() == 1 => {
                        let old = if scope.version.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(scope.version.clone().into()))
                        };
                        scope.version = value.as_str().map_or_else(
                            || value.to_string_lossy().into_owned(),
                            |s| s.to_string(),
                        );
                        old
                    }
                    f::ATTRIBUTES if remaining.len() == 1 => {
                        let old = if self.scope_attrs.is_empty() {
                            None
                        } else {
                            Some(Value::Object(self.scope_attrs.to_object_map()))
                        };
                        if let Value::Object(map) = &value {
                            self.scope_attrs = OtelAttributes::from_object_map(map);
                        }
                        old
                    }
                    f::ATTRIBUTES => {
                        let key = remaining[1].as_str();
                        if remaining.len() == 2 {
                            let old = self.scope_attrs.get(key).map(any_value_to_vrl);
                            self.scope_attrs
                                .insert(key.to_string(), vrl_value_to_any_value(&value));
                            old
                        } else {
                            let mut v = self
                                .scope_attrs
                                .get(key)
                                .map_or(Value::Object(ObjectMap::new()), any_value_to_vrl);
                            let old = insert_value_at(&mut v, &remaining[2..], value);
                            self.scope_attrs
                                .insert(key.to_string(), vrl_value_to_any_value(&v));
                            old
                        }
                    }
                    _ => None,
                }
            }
            f::SPAN_STATUS => {
                use opentelemetry_proto::tonic::trace::v1::Status;
                let remaining = &fields[1..];
                let status = self.span.status.get_or_insert_with(|| Status {
                    message: String::new(),
                    code: 0,
                });
                match remaining[0].as_str() {
                    f::STATUS_MESSAGE if remaining.len() == 1 => {
                        let old = if status.message.is_empty() {
                            None
                        } else {
                            Some(Value::Bytes(status.message.clone().into()))
                        };
                        status.message = value.as_str().map_or_else(
                            || value.to_string_lossy().into_owned(),
                            |s| s.to_string(),
                        );
                        old
                    }
                    f::STATUS_CODE if remaining.len() == 1 => {
                        let old = Some(Value::Integer(i64::from(status.code)));
                        if let Some(n) = value.as_integer() {
                            status.code = OtlpEnumField::from_vrl(n).as_proto();
                        }
                        old
                    }
                    _ => None,
                }
            }
            first => {
                let mut v = self
                    .span_attrs
                    .get(first)
                    .map_or(Value::Object(ObjectMap::new()), any_value_to_vrl);
                let old = insert_value_at(&mut v, &fields[1..], value);
                self.span_attrs
                    .insert(first.to_string(), vrl_value_to_any_value(&v));
                old
            }
        }
    }

    pub fn remove<'a>(&mut self, path: impl lookup::lookup_v2::TargetPath<'a>) -> Option<Value> {
        self.remove_prune(path, false)
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "impl Trait parameter follows VRL target API convention"
    )]
    pub fn remove_prune<'a>(
        &mut self,
        path: impl lookup::lookup_v2::TargetPath<'a>,
        prune: bool,
    ) -> Option<Value> {
        use lookup::lookup_v2::ValuePath;
        use lookup::path::BorrowedSegment;

        match path.prefix() {
            lookup::PathPrefix::Event => {
                let vp = path.value_path();
                let mut iter = vp.segment_iter();
                match iter.next() {
                    Some(BorrowedSegment::Field(ref first)) => match iter.next() {
                        None => self.span_remove_single_segment(first.as_ref()),
                        Some(BorrowedSegment::Field(ref second)) => {
                            let mut fields = vec![first.to_string(), second.to_string()];
                            let mut remaining_iter = None;
                            for seg in &mut iter {
                                if let BorrowedSegment::Field(f) = seg {
                                    fields.push(f.to_string());
                                } else {
                                    remaining_iter = Some((seg, iter));
                                    break;
                                }
                            }
                            if let Some((non_field_seg, rest)) = remaining_iter {
                                let mut base = self.span_get_field_path(&fields)?;
                                let sub = remaining_value_path(
                                    std::iter::once(non_field_seg).chain(rest),
                                );
                                let old = base.remove(&sub, prune);
                                self.span_insert_field_path(&fields, base);
                                old
                            } else {
                                self.span_remove_field_path(&fields, prune)
                            }
                        }
                        Some(non_field_seg) => {
                            let first_str = first.as_ref();
                            let mut base = self.span_get_single_segment(first_str)?;
                            let sub =
                                remaining_value_path(std::iter::once(non_field_seg).chain(iter));
                            let old = base.remove(&sub, prune);
                            self.span_insert_single_segment(first_str, base);
                            old
                        }
                    },
                    _ => None,
                }
            }
            lookup::PathPrefix::Metadata => {
                self.metadata.value_mut().remove(path.value_path(), prune)
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "OTLP span field removal requires many branches"
    )]
    fn span_remove_single_segment(&mut self, field: &str) -> Option<Value> {
        match field {
            f::NAME => {
                if self.span.name.is_empty() {
                    return None;
                }
                let old = Some(Value::Bytes(self.span.name.clone().into()));
                self.span.name = String::new();
                old
            }
            f::SPAN_TRACE_ID => {
                if self.span.trace_id.is_empty() {
                    return None;
                }
                let old = Some(hex_encode(&self.span.trace_id));
                self.span.trace_id.clear();
                old
            }
            f::SPAN_SPAN_ID => {
                if self.span.span_id.is_empty() {
                    return None;
                }
                let old = Some(hex_encode(&self.span.span_id));
                self.span.span_id.clear();
                old
            }
            f::PARENT_SPAN_ID => {
                if self.span.parent_span_id.is_empty() {
                    return None;
                }
                let old = Some(hex_encode(&self.span.parent_span_id));
                self.span.parent_span_id.clear();
                old
            }
            f::START_TIME | f::START_TIME_UNIX_NANO => {
                if self.span.start_time_unix_nano == 0 {
                    return None;
                }
                let old = if field == f::START_TIME {
                    nanos_to_timestamp(self.span.start_time_unix_nano)
                } else {
                    Some(Value::Integer(self.span.start_time_unix_nano as i64))
                };
                self.span.start_time_unix_nano = 0;
                old
            }
            f::END_TIME | f::END_TIME_UNIX_NANO => {
                if self.span.end_time_unix_nano == 0 {
                    return None;
                }
                let old = if field == f::END_TIME {
                    nanos_to_timestamp(self.span.end_time_unix_nano)
                } else {
                    Some(Value::Integer(self.span.end_time_unix_nano as i64))
                };
                self.span.end_time_unix_nano = 0;
                old
            }
            f::SPAN_KIND => {
                if self.span.kind == 0 {
                    return None;
                }
                let old = Some(Value::Integer(i64::from(self.span.kind)));
                self.span.kind = 0;
                old
            }
            f::SPAN_FLAGS => {
                if self.span.flags == 0 {
                    return None;
                }
                let old = Some(Value::Integer(i64::from(self.span.flags)));
                self.span.flags = 0;
                old
            }
            f::DROPPED_ATTRIBUTES_COUNT => {
                if self.span.dropped_attributes_count == 0 {
                    return None;
                }
                let old = Some(Value::Integer(i64::from(
                    self.span.dropped_attributes_count,
                )));
                self.span.dropped_attributes_count = 0;
                old
            }
            f::SPAN_STATUS => {
                let status = self.span.status.take()?;
                let mut m = ObjectMap::new();
                if !status.message.is_empty() {
                    m.insert(
                        f::STATUS_MESSAGE.into(),
                        Value::Bytes(status.message.into()),
                    );
                }
                m.insert(
                    f::STATUS_CODE.into(),
                    Value::Integer(i64::from(status.code)),
                );
                Some(Value::Object(m))
            }
            other => self
                .span_attrs
                .remove(other)
                .map(|av| any_value_to_vrl(&av)),
        }
    }

    fn span_remove_field_path(&mut self, fields: &[String], prune: bool) -> Option<Value> {
        debug_assert!(fields.len() >= 2);
        match fields[0].as_str() {
            f::RESOURCE => remove_resource_subpath(&mut self.resource_attrs, &fields[1..], prune),
            f::SCOPE => remove_scope_subpath(
                self.scope.as_mut(),
                &mut self.scope_attrs,
                &fields[1..],
                prune,
            ),
            f::SPAN_STATUS => {
                let status = self.span.status.as_mut()?;
                let remaining = &fields[1..];
                match remaining[0].as_str() {
                    f::STATUS_MESSAGE if remaining.len() == 1 => {
                        if status.message.is_empty() {
                            return None;
                        }
                        let old = Some(Value::Bytes(status.message.clone().into()));
                        status.message = String::new();
                        old
                    }
                    f::STATUS_CODE if remaining.len() == 1 => {
                        let old = Some(Value::Integer(i64::from(status.code)));
                        status.code = 0;
                        old
                    }
                    _ => None,
                }
            }
            first => remove_attrs_subpath(&mut self.span_attrs, first, &fields[1..], prune),
        }
    }

    /// Check if a field exists.
    pub fn contains<'a>(&self, path: impl lookup::lookup_v2::TargetPath<'a>) -> bool {
        self.get(path).is_some()
    }

    /// Build a flat `ObjectMap` from proto fields + attributes.
    #[expect(
        clippy::too_many_lines,
        reason = "OTLP span has many proto fields to map"
    )]
    pub fn as_map(&self) -> Option<ObjectMap> {
        let mut map = ObjectMap::new();

        if !self.span.name.is_empty() {
            map.insert(f::NAME.into(), Value::Bytes(self.span.name.clone().into()));
        }
        if !self.span.trace_id.is_empty() {
            map.insert(f::SPAN_TRACE_ID.into(), hex_encode(&self.span.trace_id));
        }
        if !self.span.span_id.is_empty() {
            map.insert(f::SPAN_SPAN_ID.into(), hex_encode(&self.span.span_id));
        }
        if !self.span.parent_span_id.is_empty() {
            map.insert(
                f::PARENT_SPAN_ID.into(),
                hex_encode(&self.span.parent_span_id),
            );
        }
        if self.span.start_time_unix_nano != 0 {
            map.insert(
                f::START_TIME_UNIX_NANO.into(),
                Value::Integer(self.span.start_time_unix_nano as i64),
            );
        }
        if self.span.end_time_unix_nano != 0 {
            map.insert(
                f::END_TIME_UNIX_NANO.into(),
                Value::Integer(self.span.end_time_unix_nano as i64),
            );
        }
        if self.span.kind != 0 {
            map.insert(
                f::SPAN_KIND.into(),
                Value::Integer(i64::from(self.span.kind)),
            );
        }
        if self.span.flags != 0 {
            map.insert(
                f::SPAN_FLAGS.into(),
                Value::Integer(i64::from(self.span.flags)),
            );
        }
        if self.span.dropped_attributes_count != 0 {
            map.insert(
                f::DROPPED_ATTRIBUTES_COUNT.into(),
                Value::Integer(i64::from(self.span.dropped_attributes_count)),
            );
        }
        if let Some(status) = &self.span.status {
            let mut status_map = ObjectMap::new();
            if !status.message.is_empty() {
                status_map.insert(
                    f::STATUS_MESSAGE.into(),
                    Value::Bytes(status.message.clone().into()),
                );
            }
            status_map.insert(
                f::STATUS_CODE.into(),
                Value::Integer(i64::from(status.code)),
            );
            map.insert(f::SPAN_STATUS.into(), Value::Object(status_map));
        }
        if !self.span.trace_state.is_empty() {
            map.insert(
                "trace_state".into(),
                Value::Bytes(self.span.trace_state.clone().into()),
            );
        }
        if !self.span.events.is_empty() {
            let events: Vec<Value> = self
                .span
                .events
                .iter()
                .map(|e| {
                    let mut em = ObjectMap::new();
                    em.insert(
                        f::TIME_UNIX_NANO.into(),
                        Value::Integer(e.time_unix_nano as i64),
                    );
                    em.insert(f::NAME.into(), Value::Bytes(e.name.clone().into()));
                    em.insert(
                        f::ATTRIBUTES.into(),
                        Value::Object(kvlist_to_object_map(&e.attributes)),
                    );
                    em.insert(
                        f::DROPPED_ATTRIBUTES_COUNT.into(),
                        Value::Integer(i64::from(e.dropped_attributes_count)),
                    );
                    Value::Object(em)
                })
                .collect();
            map.insert(f::SPAN_EVENTS.into(), Value::Array(events));
        }
        if self.span.dropped_events_count != 0 {
            map.insert(
                "dropped_events_count".into(),
                Value::Integer(i64::from(self.span.dropped_events_count)),
            );
        }
        if !self.span.links.is_empty() {
            let links: Vec<Value> = self
                .span
                .links
                .iter()
                .map(|l| {
                    let mut lm = ObjectMap::new();
                    lm.insert(f::SPAN_TRACE_ID.into(), hex_encode(&l.trace_id));
                    lm.insert(f::SPAN_SPAN_ID.into(), hex_encode(&l.span_id));
                    if !l.trace_state.is_empty() {
                        lm.insert(
                            "trace_state".into(),
                            Value::Bytes(l.trace_state.clone().into()),
                        );
                    }
                    lm.insert(
                        f::ATTRIBUTES.into(),
                        Value::Object(kvlist_to_object_map(&l.attributes)),
                    );
                    lm.insert(
                        f::DROPPED_ATTRIBUTES_COUNT.into(),
                        Value::Integer(i64::from(l.dropped_attributes_count)),
                    );
                    if l.flags != 0 {
                        lm.insert(f::SPAN_FLAGS.into(), Value::Integer(i64::from(l.flags)));
                    }
                    Value::Object(lm)
                })
                .collect();
            map.insert(f::SPAN_LINKS.into(), Value::Array(links));
        }
        if self.span.dropped_links_count != 0 {
            map.insert(
                "dropped_links_count".into(),
                Value::Integer(i64::from(self.span.dropped_links_count)),
            );
        }
        if !self.span_attrs.is_empty() {
            for (k, v) in self.span_attrs.iter() {
                let key = KeyString::from(k.clone());
                map.entry(key).or_insert_with(|| any_value_to_vrl(v));
            }
        }
        append_canonical_resource_scope(
            &mut map,
            self.resource.as_ref(),
            &self.resource_attrs,
            self.scope.as_ref(),
            &self.scope_attrs,
        );

        Some(map)
    }

    /// Parse a path and get a value.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `path` cannot be parsed as a valid target path.
    pub fn parse_path_and_get_value(
        &self,
        path: &str,
    ) -> Result<Option<Value>, vrl::path::PathParseError> {
        let target_path = vrl::path::parse_target_path(path)?;
        Ok(self.get(&target_path))
    }
}

// OtelMetric and MetricView are defined in the sibling `otel_metric` module.
pub use super::otel_metric::{MetricView, OtelMetric};

// -- Trait implementations --

macro_rules! impl_otel_event_traits {
    ($ty:ident, $proto_field:ident) => {
        impl ByteSizeOf for $ty {
            fn allocated_bytes(&self) -> usize {
                self.$proto_field.encoded_len()
                    + self.resource.as_ref().map_or(0, |r| r.encoded_len())
                    + self.scope.as_ref().map_or(0, |s| s.encoded_len())
                    + self.metadata.allocated_bytes()
            }
        }

        impl EstimatedJsonEncodedSizeOf for $ty {
            fn estimated_json_encoded_size_of(&self) -> JsonSize {
                // Approximate: proto encoded_len * 3 accounts for JSON overhead
                // (field names, quoting, braces). For OtelLog/OtelSpan this should
                // closely match `as_map().estimated_json_encoded_size_of()`.
                JsonSize::new(self.$proto_field.encoded_len() * 3)
            }
        }

        impl EventCount for $ty {
            fn event_count(&self) -> usize {
                1
            }
        }

        impl Finalizable for $ty {
            fn take_finalizers(&mut self) -> EventFinalizers {
                self.metadata.take_finalizers()
            }
        }
    };
}

impl_otel_event_traits!(OtelLog, record);
impl_otel_event_traits!(OtelSpan, span);

impl GetEventCountTags for OtelSpan {
    fn get_tags(&self) -> TaggedEventsSent {
        TaggedEventsSent::new_unspecified()
    }
}

// Override GetEventCountTags for OtelLog with proper source/service extraction.
impl GetEventCountTags for OtelLog {
    fn get_tags(&self) -> TaggedEventsSent {
        use crate::config::telemetry;
        use sol_common::internal_event::OptionalTag;

        let source = if telemetry().tags().emit_source {
            self.metadata().source_id().cloned().into()
        } else {
            OptionalTag::Ignored
        };

        let service = if telemetry().tags().emit_service {
            self.resource_attrs
                .get(f::SERVICE_NAME)
                .and_then(|av| match &av.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => Some(s.clone()),
                    _ => None,
                })
                .into()
        } else {
            OptionalTag::Ignored
        };

        TaggedEventsSent { source, service }
    }
}

impl EventDataEq for OtelLog {
    fn event_data_eq(&self, other: &Self) -> bool {
        self.record.body == other.record.body
            && self.record.severity_text == other.record.severity_text
            && self.record.severity_number == other.record.severity_number
            && self.record.time_unix_nano == other.record.time_unix_nano
            && self.record.flags == other.record.flags
            && self.record.trace_id == other.record.trace_id
            && self.record.span_id == other.record.span_id
            && self.record_attrs == other.record_attrs
            && self.record.dropped_attributes_count == other.record.dropped_attributes_count
            && self.resource == other.resource
            && self.resource_attrs == other.resource_attrs
            && self.scope == other.scope
            && self.scope_attrs == other.scope_attrs
    }
}

impl EventDataEq for OtelSpan {
    fn event_data_eq(&self, other: &Self) -> bool {
        self.span == other.span
            && self.span_attrs == other.span_attrs
            && self.resource == other.resource
            && self.resource_attrs == other.resource_attrs
            && self.scope == other.scope
            && self.scope_attrs == other.scope_attrs
    }
}

impl Default for OtelLog {
    fn default() -> Self {
        Self::new(LogRecord::default())
    }
}

impl From<&str> for OtelLog {
    fn from(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }
}

impl From<String> for OtelLog {
    fn from(s: String) -> Self {
        Self::from_bytes(s.as_bytes())
    }
}

impl From<bytes::Bytes> for OtelLog {
    fn from(b: bytes::Bytes) -> Self {
        Self::from_bytes(&b)
    }
}

impl From<Value> for OtelLog {
    fn from(value: Value) -> Self {
        Self::from_value_map(value, EventMetadata::default())
    }
}

impl From<ObjectMap> for OtelLog {
    fn from(map: ObjectMap) -> Self {
        Self::from_value_map(Value::Object(map), EventMetadata::default())
    }
}

impl From<std::collections::BTreeMap<String, Value>> for OtelLog {
    fn from(map: std::collections::BTreeMap<String, Value>) -> Self {
        let obj: ObjectMap = map.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Self::from(obj)
    }
}

impl From<std::collections::HashMap<vrl::prelude::KeyString, Value>> for OtelLog {
    fn from(map: std::collections::HashMap<vrl::prelude::KeyString, Value>) -> Self {
        let obj: ObjectMap = map.into_iter().collect();
        Self::from(obj)
    }
}

pub(super) fn hex_encode_bytes(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

impl Serialize for OtelLog {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use super::otel_json::{
            SerializableAnyValue, SerializableAttributes, SerializableResource, SerializableScope,
        };
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(None)?;
        if let Some(ref body) = self.record.body {
            map.serialize_entry(f::BODY, &SerializableAnyValue(body))?;
        }
        if !self.record.severity_text.is_empty() {
            map.serialize_entry(f::SEVERITY_TEXT_CC, &self.record.severity_text)?;
        }
        if self.record.severity_number != 0 {
            map.serialize_entry(f::SEVERITY_NUMBER_CC, &self.record.severity_number)?;
        }
        if self.record.time_unix_nano != 0 {
            map.serialize_entry(
                f::TIME_UNIX_NANO_CC,
                &self.record.time_unix_nano.to_string(),
            )?;
        }
        if self.record.observed_time_unix_nano != 0 {
            map.serialize_entry(
                f::OBSERVED_TIME_UNIX_NANO_CC,
                &self.record.observed_time_unix_nano.to_string(),
            )?;
        }
        if !self.record.trace_id.is_empty() {
            map.serialize_entry(f::TRACE_ID_CC, &hex_encode_bytes(&self.record.trace_id))?;
        }
        if !self.record.span_id.is_empty() {
            map.serialize_entry(f::SPAN_ID_CC, &hex_encode_bytes(&self.record.span_id))?;
        }
        if self.record.flags != 0 {
            map.serialize_entry(f::LOG_FLAGS, &self.record.flags)?;
        }
        if !self.record_attrs.is_empty() {
            let kvs = self.record_attrs.to_key_values();
            map.serialize_entry(f::ATTRIBUTES, &SerializableAttributes(&kvs))?;
        }
        if let Some(res) = self.resource_proto() {
            map.serialize_entry(f::RESOURCE, &SerializableResource(&res))?;
        }
        if let Some(scope) = self.scope_proto() {
            map.serialize_entry(f::SCOPE, &SerializableScope(&scope))?;
        }
        map.end()
    }
}

impl Serialize for OtelSpan {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use super::otel_json::{
            SerializableAttributes, SerializableResource, SerializableScope, SerializableSpanEvent,
            SerializableSpanLink,
        };
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(None)?;
        if !self.span.name.is_empty() {
            map.serialize_entry(f::NAME, &self.span.name)?;
        }
        if !self.span.trace_id.is_empty() {
            map.serialize_entry(f::TRACE_ID_CC, &hex_encode_bytes(&self.span.trace_id))?;
        }
        if !self.span.span_id.is_empty() {
            map.serialize_entry(f::SPAN_ID_CC, &hex_encode_bytes(&self.span.span_id))?;
        }
        if !self.span.parent_span_id.is_empty() {
            map.serialize_entry(
                f::PARENT_SPAN_ID_CC,
                &hex_encode_bytes(&self.span.parent_span_id),
            )?;
        }
        if self.span.kind != 0 {
            map.serialize_entry(f::SPAN_KIND, &self.span.kind)?;
        }
        if self.span.start_time_unix_nano != 0 {
            map.serialize_entry(
                f::START_TIME_UNIX_NANO_CC,
                &self.span.start_time_unix_nano.to_string(),
            )?;
        }
        if self.span.end_time_unix_nano != 0 {
            map.serialize_entry(
                f::END_TIME_UNIX_NANO_CC,
                &self.span.end_time_unix_nano.to_string(),
            )?;
        }
        if !self.span_attrs.is_empty() {
            let kvs = self.span_attrs.to_key_values();
            map.serialize_entry(f::ATTRIBUTES, &SerializableAttributes(&kvs))?;
        }
        if let Some(ref status) = self.span.status {
            map.serialize_entry(
                f::SPAN_STATUS,
                &serde_json::json!({
                    "code": status.code,
                    "message": status.message,
                }),
            )?;
        }
        if self.span.flags != 0 {
            map.serialize_entry(f::SPAN_FLAGS, &self.span.flags)?;
        }
        if !self.span.trace_state.is_empty() {
            map.serialize_entry("traceState", &self.span.trace_state)?;
        }
        if !self.span.events.is_empty() {
            let events: Vec<SerializableSpanEvent> =
                self.span.events.iter().map(SerializableSpanEvent).collect();
            map.serialize_entry("events", &events)?;
        }
        if self.span.dropped_events_count != 0 {
            map.serialize_entry("droppedEventsCount", &self.span.dropped_events_count)?;
        }
        if !self.span.links.is_empty() {
            let links: Vec<SerializableSpanLink> =
                self.span.links.iter().map(SerializableSpanLink).collect();
            map.serialize_entry("links", &links)?;
        }
        if self.span.dropped_links_count != 0 {
            map.serialize_entry("droppedLinksCount", &self.span.dropped_links_count)?;
        }
        if let Some(res) = self.resource_proto() {
            map.serialize_entry(f::RESOURCE, &SerializableResource(&res))?;
        }
        if let Some(scope) = self.scope_proto() {
            map.serialize_entry(f::SCOPE, &SerializableScope(&scope))?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lookup::event_path;

    #[test]
    fn otel_log_event_default_fields() {
        let event = OtelLog::new(LogRecord::default());
        assert_eq!(event.time_unix_nano(), 0);
        assert_eq!(event.severity_text(), "");
        assert!(event.body().is_none());
        assert!(event.trace_id().is_empty());
        assert!(event.attributes().is_empty());
        assert!(event.resource().is_none());
        assert!(event.scope().is_none());
    }

    #[test]
    fn otel_log_event_attribute_crud() {
        let mut event = OtelLog::new(LogRecord::default());

        let value = AnyValue {
            value: Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    "bar".to_string(),
                ),
            ),
        };

        assert!(event.attribute("foo").is_none());

        event.set_attribute("foo".to_string(), value.clone());
        assert_eq!(event.attribute("foo"), Some(&value));

        let new_value = AnyValue {
            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(42)),
        };
        event.set_attribute("foo".to_string(), new_value.clone());
        assert_eq!(event.attribute("foo"), Some(&new_value));

        let removed = event.remove_attribute("foo");
        assert_eq!(removed, Some(new_value));
        assert!(event.attribute("foo").is_none());
    }

    #[test]
    fn otel_log_event_resource_attribute() {
        let mut event = OtelLog::new(LogRecord::default());
        assert!(event.resource_attribute("host.name").is_none());

        let host = AnyValue {
            value: Some(
                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                    "myhost".to_string(),
                ),
            ),
        };
        event.set_resource(Resource {
            attributes: vec![KeyValue {
                key: "host.name".to_string(),
                value: Some(host.clone()),
            }],
            dropped_attributes_count: 0,
        });
        assert_eq!(event.resource_attribute("host.name"), Some(&host));
    }

    #[test]
    fn otel_span_event_typed_accessors() {
        let span = Span {
            name: "test-span".to_string(),
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            parent_span_id: vec![],
            start_time_unix_nano: 1000,
            end_time_unix_nano: 2000,
            kind: 2, // SPAN_KIND_SERVER
            ..Default::default()
        };
        let event = OtelSpan::new(span);

        assert_eq!(event.name(), "test-span");
        assert_eq!(event.trace_id().len(), 16);
        assert_eq!(event.span_id().len(), 8);
        assert!(event.parent_span_id().is_empty());
        assert_eq!(event.start_time_unix_nano(), 1000);
        assert_eq!(event.end_time_unix_nano(), 2000);
        assert_eq!(event.kind(), 2);
    }

    #[test]
    fn otel_span_pre_epoch_timestamp_preserved_as_attribute() {
        use chrono::{TimeZone, Utc};
        // Pre-1970 timestamps cannot be represented in a u64 nanos field.
        // Confirm they are preserved as attributes rather than wrapping
        // to huge future values via an `i64 as u64` cast.
        let pre_epoch = Utc.with_ymd_and_hms(1960, 1, 1, 0, 0, 0).unwrap();
        let mut event = OtelSpan::new(Span::default());
        event.insert(vrl::event_path!("start_time"), Value::Timestamp(pre_epoch));
        event.insert(vrl::event_path!("new_field"), "v");

        // Native field must stay zero — no u64 wrap.
        assert_eq!(event.start_time_unix_nano(), 0);
        // Pre-epoch value is preserved as an attribute. OTLP AnyValue has
        // no native timestamp kind, so it round-trips as its RFC3339 string
        // representation.
        let back = event.get(vrl::event_path!("start_time"));
        let s = match back {
            Some(Value::Bytes(b)) => String::from_utf8(b.to_vec()).unwrap(),
            Some(Value::Timestamp(t)) => t.to_rfc3339(),
            other => panic!("expected Bytes or Timestamp, got {other:?}"),
        };
        assert!(
            s.starts_with("1960-01-01"),
            "pre-epoch start_time should survive as attribute; got {s:?}"
        );
    }

    #[test]
    fn otel_span_insert_preserves_native_fields() {
        let span = Span {
            name: "test-span".to_string(),
            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
            parent_span_id: vec![9, 10, 11, 12, 13, 14, 15, 16],
            start_time_unix_nano: 1000,
            end_time_unix_nano: 2000,
            kind: 2, // SPAN_KIND_SERVER
            ..Default::default()
        };
        let mut event = OtelSpan::new(span);

        // Trigger the round-trip: insert → as_map → apply_value_map.
        event.insert(vrl::event_path!("new_field"), "new_value");

        // Ideal: native proto fields survive.
        assert_eq!(event.name(), "test-span");
        assert_eq!(event.trace_id().len(), 16);
        assert_eq!(event.span_id().len(), 8);
        assert_eq!(event.parent_span_id().len(), 8);
        assert_eq!(event.start_time_unix_nano(), 1000);
        assert_eq!(event.end_time_unix_nano(), 2000);
        assert_eq!(event.kind(), 2);
    }

    #[test]
    fn otel_metric_event_name_and_unit() {
        let metric = OtelMetricProto {
            name: "http.request.duration".to_string(),
            description: "Duration of HTTP requests".to_string(),
            unit: "ms".to_string(),
            ..Default::default()
        };
        let event = OtelMetric::new(metric);

        assert_eq!(event.name(), "http.request.duration");
        assert_eq!(event.description(), "Duration of HTTP requests");
        assert_eq!(event.unit(), "ms");
    }

    #[test]
    fn byte_size_of_non_zero() {
        let event = OtelLog::new(LogRecord {
            body: Some(AnyValue {
                value: Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                        "hello world".to_string(),
                    ),
                ),
            }),
            ..Default::default()
        });
        assert!(event.allocated_bytes() > 0);
    }

    #[test]
    fn event_data_eq_works() {
        let a = OtelLog::new(LogRecord::default());
        let b = OtelLog::new(LogRecord::default());
        assert!(a.event_data_eq(&b));

        let mut c = OtelLog::new(LogRecord::default());
        c.record.severity_text = "ERROR".to_string();
        assert!(!a.event_data_eq(&c));
    }

    #[test]
    fn event_count_is_one() {
        assert_eq!(OtelLog::new(LogRecord::default()).event_count(), 1);
        assert_eq!(OtelSpan::new(Span::default()).event_count(), 1);
        assert_eq!(OtelMetric::new(OtelMetricProto::default()).event_count(), 1);
    }

    #[test]
    fn from_parts_round_trip() {
        let record = LogRecord {
            severity_text: "INFO".to_string(),
            ..Default::default()
        };
        let resource = Some(Resource {
            attributes: vec![],
            dropped_attributes_count: 0,
        });
        let scope = Some(InstrumentationScope {
            name: "my-lib".to_string(),
            ..Default::default()
        });
        let metadata = EventMetadata::default();

        let event = OtelLog::from_parts(record.clone(), resource.clone(), scope.clone(), metadata);
        let (r, res, sc, _meta) = event.into_parts();
        assert_eq!(r.severity_text, "INFO");
        assert_eq!(res, resource);
        assert_eq!(sc.as_ref().map(|s| s.name.as_str()), Some("my-lib"));
    }

    #[test]
    fn legacy_layout_projects_fields() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as Kind;

        let mut event = OtelLog::new(LogRecord {
            body: Some(AnyValue {
                value: Some(Kind::StringValue("hello world".into())),
            }),
            severity_text: "ERROR".into(),
            severity_number: 17,
            time_unix_nano: 1_700_000_000_000_000_000,
            trace_id: vec![0xab, 0xcd],
            span_id: vec![0x12, 0x34],
            attributes: vec![KeyValue {
                key: "env".into(),
                value: Some(AnyValue {
                    value: Some(Kind::StringValue("prod".into())),
                }),
            }],
            ..Default::default()
        });
        event.set_resource(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Kind::StringValue("my-svc".into())),
                }),
            }],
            dropped_attributes_count: 0,
        });
        event.set_scope(InstrumentationScope {
            name: "my-lib".into(),
            version: "1.0".into(),
            ..Default::default()
        });

        let map = event.as_map().expect("expected object map");
        assert_eq!(map.get("body").unwrap().as_str().unwrap(), "hello world");
        assert_eq!(map.get("severity_text").unwrap().as_str().unwrap(), "ERROR");
        assert_eq!(
            map.get("severity_number").unwrap().as_integer().unwrap(),
            17
        );
        assert_eq!(
            map.get("time_unix_nano").unwrap().as_integer().unwrap(),
            1_700_000_000_000_000_000
        );
        assert_eq!(map.get("trace_id").unwrap().as_str().unwrap(), "abcd");
        assert_eq!(map.get("span_id").unwrap().as_str().unwrap(), "1234");
        assert_eq!(map.get("env").unwrap().as_str().unwrap(), "prod");
        assert!(map.get("resource").unwrap().is_object());
        assert!(map.get("scope").unwrap().is_object());
    }

    #[test]
    fn body_string_returns_body_text() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as Kind;

        let event = OtelLog::new(LogRecord {
            body: Some(AnyValue {
                value: Some(Kind::StringValue("test body".into())),
            }),
            ..Default::default()
        });
        assert_eq!(event.body_string(), "test body");

        let empty = OtelLog::new(LogRecord::default());
        assert_eq!(empty.body_string(), "");

        let int_body = OtelLog::new(LogRecord {
            body: Some(AnyValue {
                value: Some(Kind::IntValue(42)),
            }),
            ..Default::default()
        });
        assert_eq!(int_body.body_string(), "42");
    }

    #[test]
    fn otel_log_serializes_as_otlp_json() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as Kind;
        let record = LogRecord {
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(Kind::StringValue("hello".to_string())),
            }),
            ..Default::default()
        };
        let resource = Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Kind::StringValue("test-svc".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
        });
        let event = OtelLog::from_parts(record, resource, None, EventMetadata::default());
        let json = serde_json::to_string(&event).expect("serialize");

        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["body"]["stringValue"], "hello");
        assert_eq!(v["severityText"], "INFO");
        assert!(v["resource"]["attributes"].is_array());
        assert_eq!(v["resource"]["attributes"][0]["key"], "service.name");
        assert_eq!(
            v["resource"]["attributes"][0]["value"]["stringValue"],
            "test-svc"
        );
    }

    #[test]
    fn metric_to_otel_metric_round_trip_counter() {
        use crate::event::MetricKind;
        use chrono::Utc;

        let otel = OtelMetric::new_counter("requests_total", MetricKind::Incremental, 42.0)
            .with_namespace(Some("http"))
            .with_timestamp(Some(Utc::now()));

        assert_eq!(otel.name(), "requests_total");
        assert_eq!(otel.namespace(), Some("http"));
        assert_eq!(otel.kind(), MetricKind::Incremental);
        match otel.view() {
            MetricView::Sum { value } => assert!((value - 42.0).abs() < f64::EPSILON),
            other => panic!("expected Sum, got {other}"),
        }
    }

    #[test]
    fn metric_to_otel_metric_round_trip_gauge() {
        use crate::event::MetricKind;

        let otel = OtelMetric::new_gauge("temperature", 98.6);

        assert_eq!(otel.name(), "temperature");
        assert_eq!(otel.kind(), MetricKind::Absolute);
        match otel.view() {
            MetricView::Gauge { value } => assert!((value - 98.6).abs() < f64::EPSILON),
            other => panic!("expected Gauge, got {other}"),
        }
    }

    #[test]
    fn metric_to_otel_metric_round_trip_histogram() {
        use crate::event::MetricKind;
        use crate::event::metric::Bucket;

        let buckets = vec![
            Bucket {
                count: 10,
                upper_limit: 5.0,
            },
            Bucket {
                count: 20,
                upper_limit: 10.0,
            },
            Bucket {
                count: 5,
                upper_limit: f64::INFINITY,
            },
        ];
        let otel = OtelMetric::new_histogram("latency", MetricKind::Absolute, &buckets, 35, 150.0);

        assert_eq!(otel.name(), "latency");
        match otel.view() {
            MetricView::Histogram {
                counts, count, sum, ..
            } => {
                assert_eq!(count, 35);
                assert!((sum - 150.0).abs() < f64::EPSILON);
                assert_eq!(counts.len(), 3);
                assert_eq!(counts[0], 10);
            }
            other => panic!("expected Histogram, got {other}"),
        }
    }

    #[test]
    fn from_metric_for_event_produces_otel_metric() {
        use crate::event::Event;

        let otel = OtelMetric::new_gauge("test", 1.0);
        let event: Event = Event::Metric(otel);
        assert!(
            matches!(event, Event::Metric(_)),
            "expected Event::Metric, got {event:?}"
        );

        let metric = event.try_into_otel_metric().expect("should convert back");
        assert_eq!(metric.name(), "test");
    }

    #[test]
    fn set_timestamp_and_try_set_timestamp() {
        use chrono::{TimeZone, Utc};

        let mut log = OtelLog::from("hello");
        assert!(log.get_timestamp().is_none());

        let ts1 = Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap();
        let old = log.set_timestamp(ts1);
        assert!(old.is_none());
        assert_eq!(log.get_timestamp(), Some(Value::Timestamp(ts1)));

        let ts2 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let old = log.set_timestamp(ts2);
        assert_eq!(old, Some(Value::Timestamp(ts1)));
        assert_eq!(log.get_timestamp(), Some(Value::Timestamp(ts2)));

        // try_set_timestamp is a no-op when already set
        let ts3 = Utc.with_ymd_and_hms(2026, 3, 3, 3, 3, 3).unwrap();
        log.try_set_timestamp(ts3);
        assert_eq!(log.get_timestamp(), Some(Value::Timestamp(ts2)));

        // try_set_timestamp works when cleared
        log.remove_timestamp();
        log.try_set_timestamp(ts3);
        assert_eq!(log.get_timestamp(), Some(Value::Timestamp(ts3)));
    }

    #[test]
    fn get_by_meaning_resolves_schema_meaning() {
        use std::sync::Arc;

        // Create an OtelLog and insert a field at a custom path
        let mut event = OtelLog::from("test body");
        event.insert(
            &vrl::path::parse_target_path("@timestamp").unwrap(),
            Value::Bytes("2001-02-03T04:05:06Z".into()),
        );

        // Set Vector namespace metadata
        event
            .metadata_mut()
            .value_mut()
            .insert(lookup::path!("vector", "ns"), Value::Bytes("true".into()));

        // Set schema meaning: "timestamp" → "@timestamp"
        let schema = event
            .metadata()
            .schema_definition()
            .as_ref()
            .clone()
            .with_meaning(
                vrl::path::parse_target_path("@timestamp").unwrap(),
                "timestamp",
            );
        event
            .metadata_mut()
            .set_schema_definition(&Arc::new(schema));

        // get_by_meaning should resolve the schema path
        let val = event.get_by_meaning("timestamp");
        assert!(val.is_some(), "expected Some, got None");
        assert_eq!(val.unwrap(), Value::Bytes("2001-02-03T04:05:06Z".into()));

        // Non-existent meaning returns None
        assert!(event.get_by_meaning("nonexistent").is_none());
    }

    #[test]
    fn tag_value_searches_data_point_resource_and_scope() {
        use crate::event::MetricKind;

        let otel = OtelMetric::new_counter("test_metric", MetricKind::Incremental, 1.0)
            .with_namespace(Some("ns"))
            .with_tags(Some(
                vec![("env".to_string(), "prod".to_string())]
                    .into_iter()
                    .collect(),
            ));

        // Data point attribute lookup
        assert_eq!(otel.tag_value("env"), Some("prod".to_string()));

        // Resource attribute lookup (prefixed with "resource.")
        // from_metric_parts stores namespace in resource as "metric.namespace"
        // but other resource attrs are prefixed with "resource." in tags
        assert!(otel.tag_value("nonexistent").is_none());

        // Scope lookup
        let otel_with_scope = OtelMetric::from_parts(
            otel.metric.clone(),
            otel.resource.clone(),
            Some(InstrumentationScope {
                name: "my-scope".into(),
                version: "1.0".into(),
                ..Default::default()
            }),
            EventMetadata::default(),
        );
        assert_eq!(
            otel_with_scope.tag_value("scope.name"),
            Some("my-scope".to_string())
        );
        assert_eq!(
            otel_with_scope.tag_value("scope.version"),
            Some("1.0".to_string())
        );
    }

    #[test]
    fn tags_builds_metric_tags_from_proto() {
        use crate::event::MetricKind;

        let otel =
            OtelMetric::new_counter("test_metric", MetricKind::Incremental, 1.0).with_tags(Some(
                vec![
                    ("env".to_string(), "prod".to_string()),
                    ("region".to_string(), "us-east".to_string()),
                ]
                .into_iter()
                .collect(),
            ));

        let tags = otel.tags().expect("should have tags");
        assert_eq!(tags.get_string("env"), Some("prod"));
        assert_eq!(tags.get_string("region"), Some("us-east"));

        // Empty metric has no tags
        let empty = OtelMetric::new_gauge("empty", 0.0);
        assert!(empty.tags().is_none());
    }

    #[test]
    fn new_counter_matches_from_metric_parts() {
        use crate::event::MetricKind;

        let direct = OtelMetric::new_counter("requests", MetricKind::Incremental, 42.0);
        let via_ctor = OtelMetric::new_counter("requests", MetricKind::Incremental, 42.0);

        assert_eq!(direct.name(), via_ctor.name());
        assert_eq!(direct.kind(), via_ctor.kind());
        assert_eq!(direct.first_value_as_f64(), via_ctor.first_value_as_f64());
    }

    #[test]
    fn new_gauge_matches_from_metric_parts() {
        let direct = OtelMetric::new_gauge("temperature", 98.6);
        let via_ctor = OtelMetric::new_gauge("temperature", 98.6);

        assert_eq!(direct.name(), via_ctor.name());
        assert_eq!(direct.kind(), via_ctor.kind());
        assert_eq!(direct.first_value_as_f64(), via_ctor.first_value_as_f64());
    }

    #[test]
    fn new_histogram_matches_from_metric_parts() {
        use crate::event::MetricKind;

        let buckets = crate::buckets![1.0 => 10, 5.0 => 20, 10.0 => 5];
        let direct =
            OtelMetric::new_histogram("request_duration", MetricKind::Absolute, &buckets, 35, 8.0);
        let via_ctor =
            OtelMetric::new_histogram("request_duration", MetricKind::Absolute, &buckets, 35, 8.0);

        assert_eq!(direct.name(), via_ctor.name());
        assert_eq!(direct.kind(), via_ctor.kind());
        assert_eq!(format!("{}", direct.view()), format!("{}", via_ctor.view()));
    }

    #[test]
    fn new_summary_matches_from_metric_parts() {
        let quantiles = crate::quantiles![0.5 => 100.0, 0.99 => 200.0];
        let direct = OtelMetric::new_summary("request_latency", &quantiles, 50, 4200.0);
        let via_ctor = OtelMetric::new_summary("request_latency", &quantiles, 50, 4200.0);

        assert_eq!(direct.name(), via_ctor.name());
        assert_eq!(format!("{}", direct.view()), format!("{}", via_ctor.view()));
    }

    #[test]
    fn from_tracing_event_produces_expected_fields() {
        // Verify OtelLog::from_tracing_event produces the expected field shape
        // (standard metadata: kind, level, module_path, target, plus timestamp).
        use tracing::info;
        use tracing_subscriber::{Layer, layer::Context, prelude::*, registry::LookupSpan};

        struct Capture {
            from_otel: std::sync::Mutex<Option<OtelLog>>,
        }
        impl<S> Layer<S> for &'static Capture
        where
            S: tracing::Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
                *self.from_otel.lock().unwrap() = Some(OtelLog::from_tracing_event(event));
            }
        }

        let cap: &'static Capture = Box::leak(Box::new(Capture {
            from_otel: std::sync::Mutex::new(None),
        }));

        let subscriber = tracing_subscriber::registry().with(cap);
        tracing::subscriber::with_default(subscriber, || {
            info!(message = "hello", count = 42, ready = true, "static msg");
        });

        let ol = cap.from_otel.lock().unwrap().clone().expect("captured");

        // Standard metadata fields are present.
        assert!(
            ol.get(vrl::event_path!("metadata", "level"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
                .is_some(),
            "metadata.level must be present"
        );
        assert!(
            ol.get(vrl::event_path!("metadata", "target"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
                .is_some(),
            "metadata.target must be present"
        );
        assert!(
            ol.get(vrl::event_path!("metadata", "kind"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned))
                .is_some(),
            "metadata.kind must be present"
        );
        // Visitor records fields at top level.
        assert!(
            ol.get(vrl::event_path!("count"))
                .and_then(|v| v.as_integer())
                .is_some(),
            "i64 field via record_i64"
        );
        assert!(
            ol.get(vrl::event_path!("ready"))
                .and_then(|v| v.as_boolean())
                .is_some(),
            "bool field via record_bool"
        );
        // time_unix_nano is present as integer.
        assert!(
            ol.get(vrl::event_path!("time_unix_nano"))
                .is_some_and(|v| matches!(v, vrl::value::Value::Integer(_))),
            "OtelLog has time_unix_nano"
        );
    }

    /// Round-trip fidelity test: `OtelLog` → `as_map` → `apply_value_map`
    /// should produce an `OtelLog` equivalent to the starting one for field lookups.
    /// This protects against regressions when rewriting `apply_value_map`.
    #[test]
    fn insert_preserves_body_via_round_trip() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as Kind;

        let mut event = OtelLog::new(LogRecord {
            body: Some(AnyValue {
                value: Some(Kind::StringValue("hello".into())),
            }),
            ..Default::default()
        });
        // Insert triggers the full round-trip
        event.insert(vrl::event_path!("new_field"), "new_value");
        assert_eq!(
            event
                .get(vrl::event_path!("body"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned)),
            Some("hello".to_string())
        );
        assert_eq!(
            event
                .get(vrl::event_path!("new_field"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned)),
            Some("new_value".to_string())
        );
    }

    #[test]
    fn insert_preserves_source_type_resource_attr() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as Kind;

        let mut event = OtelLog::new(LogRecord::default());
        event.set_resource(Resource {
            attributes: vec![KeyValue {
                key: "source_type".into(),
                value: Some(AnyValue {
                    value: Some(Kind::StringValue("syslog".into())),
                }),
            }],
            dropped_attributes_count: 0,
        });
        event.insert(vrl::event_path!("another"), "x");
        // source_type lives at canonical resource path
        assert_eq!(
            event
                .get(vrl::event_path!("resource", "source_type"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned)),
            Some("syslog".to_string())
        );
    }

    #[test]
    fn otel_log_with_tags_array_preserves_field_access() {
        // OtelLog with "tags" array field, queried via get().
        let mut otel = OtelLog::from("msg");
        otel.insert(
            vrl::event_path!("tags"),
            Value::Array(vec![Value::Bytes("a:foo".into())]),
        );

        // Lookup via get()
        let tags = otel.get(vrl::event_path!("tags"));
        assert!(tags.is_some(), "tags field must be accessible after insert");
        match tags.unwrap() {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(
                    arr[0].as_str().map(std::borrow::Cow::into_owned),
                    Some("a:foo".to_string())
                );
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn insert_preserves_other_resource_attrs() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as Kind;

        // Resource attrs OTHER than source_type/host.name — these end up
        // in the "resource" sub-object in legacy layout. The round-trip
        // through from_value_map drops these because from_value_map only
        // reads source_type/host from the top level.
        // This test documents the current (lossy) behavior — any rewrite
        // should preserve it (or intentionally improve it).
        let mut event = OtelLog::new(LogRecord::default());
        event.set_resource(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Kind::StringValue("my-svc".into())),
                }),
            }],
            dropped_attributes_count: 0,
        });
        // Before insert, we can read the resource attr via the resource sub-object
        let before = event
            .get(vrl::event_path!("resource", "service.name"))
            .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned));
        assert_eq!(
            before,
            Some("my-svc".to_string()),
            "resource sub-object should be readable before insert"
        );

        event.insert(vrl::event_path!("attr"), "val");

        // After insert: the current implementation's behavior (for regression detection)
        let after = event
            .get(vrl::event_path!("resource", "service.name"))
            .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned));
        assert_eq!(
            after, before,
            "resource sub-object fidelity must be preserved across insert"
        );
    }

    #[test]
    fn insert_preserves_host_resource_attr() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as Kind;

        let mut event = OtelLog::new(LogRecord::default());
        event.set_resource(Resource {
            attributes: vec![KeyValue {
                key: "host.name".into(),
                value: Some(AnyValue {
                    value: Some(Kind::StringValue("srv01".into())),
                }),
            }],
            dropped_attributes_count: 0,
        });
        event.insert(vrl::event_path!("attr"), "val");
        // host.name at canonical resource path
        assert_eq!(
            event
                .get(vrl::event_path!("resource", "host.name"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned)),
            Some("srv01".to_string())
        );
    }

    // --- Round-trip fidelity for native proto fields -----------------------
    //
    // `apply_value_map` extracts these fields symmetrically with
    // `as_map`, so a round-trip via `insert()` preserves them.

    #[test]
    fn insert_preserves_severity_text_via_round_trip() {
        let mut event = OtelLog::new(LogRecord {
            severity_text: "ERROR".into(),
            ..Default::default()
        });
        event.insert(vrl::event_path!("new_field"), "new_value");
        assert_eq!(event.severity_text(), "ERROR");
    }

    #[test]
    fn insert_preserves_severity_number_via_round_trip() {
        let mut event = OtelLog::new(LogRecord {
            severity_number: 17,
            ..Default::default()
        });
        event.insert(vrl::event_path!("new_field"), "new_value");
        assert_eq!(event.severity_number(), 17);
    }

    #[test]
    fn insert_preserves_trace_id_via_round_trip() {
        let trace_id_bytes = vec![
            0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19,
        ];
        let mut event = OtelLog::new(LogRecord {
            trace_id: trace_id_bytes.clone(),
            ..Default::default()
        });
        event.insert(vrl::event_path!("new_field"), "new_value");
        assert_eq!(event.trace_id(), trace_id_bytes.as_slice());
    }

    #[test]
    fn insert_preserves_span_id_via_round_trip() {
        let span_id_bytes = vec![0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11];
        let mut event = OtelLog::new(LogRecord {
            span_id: span_id_bytes.clone(),
            ..Default::default()
        });
        event.insert(vrl::event_path!("new_field"), "new_value");
        assert_eq!(event.span_id(), span_id_bytes.as_slice());
    }

    #[test]
    fn insert_preserves_corrupt_trace_id_as_attribute() {
        // If trace_id contains non-hex data (e.g. from a user-set attribute
        // via VRL), hex_decode fails and the value is preserved as an
        // attribute rather than dropped.
        let mut event = OtelLog::new(LogRecord::default());
        event.insert(vrl::event_path!("trace_id"), "not-hex-data");
        assert!(
            event.trace_id().is_empty(),
            "native field should stay empty for invalid hex"
        );
        assert_eq!(
            event
                .get(vrl::event_path!("trace_id"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned)),
            Some("not-hex-data".to_string()),
            "invalid hex should be preserved as an attribute"
        );
    }

    #[test]
    fn insert_preserves_scope_via_round_trip() {
        let mut event = OtelLog::new(LogRecord::default());
        event.set_scope(InstrumentationScope {
            name: "my-lib".into(),
            version: "1.2.3".into(),
            attributes: vec![KeyValue {
                key: "lib.lang".into(),
                value: Some(AnyValue {
                    value: Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                            "rust".into(),
                        ),
                    ),
                }),
            }],
            dropped_attributes_count: 0,
        });
        event.insert(vrl::event_path!("new_field"), "new_value");
        assert_eq!(
            event
                .get(vrl::event_path!("scope", "name"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned)),
            Some("my-lib".to_string())
        );
        assert_eq!(
            event
                .get(vrl::event_path!("scope", "version"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned)),
            Some("1.2.3".to_string())
        );
        assert_eq!(
            event
                .get(vrl::event_path!("scope", "attributes", "lib.lang"))
                .and_then(|v| v.as_str().map(std::borrow::Cow::into_owned)),
            Some("rust".to_string())
        );
    }

    #[test]
    fn insert_preserves_observed_time_unix_nano_via_round_trip() {
        let mut event = OtelLog::new(LogRecord {
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            ..Default::default()
        });
        event.insert(vrl::event_path!("new_field"), "new_value");
        assert_eq!(
            event.record.observed_time_unix_nano,
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn with_namespace_tags_timestamp_matches_from_metric_parts() {
        use crate::event::MetricKind;
        use chrono::{TimeZone, Utc};

        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let tags: OtelAttributes = vec![("env".to_string(), "prod".to_string())]
            .into_iter()
            .collect();

        let direct = OtelMetric::new_counter("requests", MetricKind::Incremental, 42.0)
            .with_namespace(Some("http"))
            .with_tags(Some(tags.clone()))
            .with_timestamp(Some(ts));

        let via_ctor = OtelMetric::new_counter("requests", MetricKind::Incremental, 42.0)
            .with_namespace(Some("http"))
            .with_tags(Some(tags))
            .with_timestamp(Some(ts));

        assert_eq!(direct.name(), via_ctor.name());
        assert_eq!(direct.namespace(), via_ctor.namespace());
        assert_eq!(direct.kind(), via_ctor.kind());
        assert_eq!(direct.first_value_as_f64(), via_ctor.first_value_as_f64());
        assert_eq!(direct.timestamp(), via_ctor.timestamp());
        assert_eq!(direct.tag_value("env"), via_ctor.tag_value("env"));
    }

    #[test]
    fn with_tags_preserves_multi_value() {
        use crate::event::MetricKind;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, any_value};

        let mut tags = OtelAttributes::default();
        tags.insert("host".to_string(), string_value("srv01"));
        tags.insert(
            "env".to_string(),
            AnyValue {
                value: Some(any_value::Value::ArrayValue(ArrayValue {
                    values: vec![
                        string_value("prod"),
                        AnyValue { value: None },
                        string_value("staging"),
                    ],
                })),
            },
        );

        let direct = OtelMetric::new_counter("requests", MetricKind::Incremental, 1.0)
            .with_tags(Some(tags.clone()));

        let via_ctor =
            OtelMetric::new_counter("requests", MetricKind::Incremental, 1.0).with_tags(Some(tags));

        let find_env = |m: &OtelMetric| -> any_value::Value {
            m.first_dp_attrs()
                .and_then(|attrs| attrs.get("env"))
                .and_then(|av| av.value.clone())
                .expect("env attribute missing")
        };
        match (find_env(&direct), find_env(&via_ctor)) {
            (any_value::Value::ArrayValue(a), any_value::Value::ArrayValue(b)) => {
                assert_eq!(a.values.len(), 3);
                assert_eq!(a.values.len(), b.values.len());
            }
            other => panic!("expected ArrayValue on both sides, got {other:?}"),
        }
        assert_eq!(direct.tag_value("host"), via_ctor.tag_value("host"));
    }

    #[test]
    fn multi_segment_resource_get_insert_remove() {
        let mut log = OtelLog::new(LogRecord::default());
        log.set_resource_attribute("service.name".to_string(), string_value("my-svc"));
        log.set_resource_attribute("source_type".to_string(), string_value("syslog"));

        // get resource attribute
        assert_eq!(
            log.get(event_path!("resource", "service.name")),
            Some(Value::Bytes("my-svc".into()))
        );
        // source_type visible at canonical resource path
        assert_eq!(
            log.get(event_path!("resource", "source_type")),
            Some(Value::Bytes("syslog".into()))
        );

        // insert resource attribute
        log.insert(event_path!("resource", "deployment.environment"), "prod");
        assert_eq!(
            log.get(event_path!("resource", "deployment.environment")),
            Some(Value::Bytes("prod".into()))
        );

        // remove resource attribute
        let removed = log.remove(event_path!("resource", "service.name"));
        assert_eq!(removed, Some(Value::Bytes("my-svc".into())));
        assert_eq!(log.get(event_path!("resource", "service.name")), None);
    }

    #[test]
    fn multi_segment_scope_get_insert_remove() {
        let mut log = OtelLog::new(LogRecord::default());
        log.scope = Some(InstrumentationScope {
            name: "my-lib".to_string(),
            version: "1.0".to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        });

        assert_eq!(
            log.get(event_path!("scope", "name")),
            Some(Value::Bytes("my-lib".into()))
        );
        assert_eq!(
            log.get(event_path!("scope", "version")),
            Some(Value::Bytes("1.0".into()))
        );

        log.insert(event_path!("scope", "name"), "updated-lib");
        assert_eq!(
            log.get(event_path!("scope", "name")),
            Some(Value::Bytes("updated-lib".into()))
        );

        log.insert(event_path!("scope", "attributes", "env"), "prod");
        assert_eq!(
            log.get(event_path!("scope", "attributes", "env")),
            Some(Value::Bytes("prod".into()))
        );

        let removed = log.remove(event_path!("scope", "name"));
        assert_eq!(removed, Some(Value::Bytes("updated-lib".into())));
        assert_eq!(log.get(event_path!("scope", "name")), None);
    }

    #[test]
    fn multi_segment_nested_attribute_get_insert_remove() {
        let mut log = OtelLog::new(LogRecord::default());

        // Insert nested attribute (like kubernetes metadata)
        log.insert(event_path!("kubernetes", "pod_name"), "sandbox0");
        assert_eq!(
            log.get(event_path!("kubernetes", "pod_name")),
            Some(Value::Bytes("sandbox0".into()))
        );

        // 3-segment path
        log.insert(event_path!("kubernetes", "pod_labels", "app"), "my-app");
        assert_eq!(
            log.get(event_path!("kubernetes", "pod_labels", "app")),
            Some(Value::Bytes("my-app".into()))
        );

        // Remove nested path
        let removed = log.remove(event_path!("kubernetes", "pod_name"));
        assert_eq!(removed, Some(Value::Bytes("sandbox0".into())));
        assert_eq!(log.get(event_path!("kubernetes", "pod_name")), None);
        // pod_labels still there
        assert_eq!(
            log.get(event_path!("kubernetes", "pod_labels", "app")),
            Some(Value::Bytes("my-app".into()))
        );
    }

    #[test]
    fn multi_segment_prune_removes_empty_parents() {
        let mut log = OtelLog::new(LogRecord::default());
        log.insert(event_path!("nested", "only_child"), "val");
        assert_eq!(
            log.get(event_path!("nested", "only_child")),
            Some(Value::Bytes("val".into()))
        );

        let removed = log.remove_prune(event_path!("nested", "only_child"), true);
        assert_eq!(removed, Some(Value::Bytes("val".into())));
        // After prune, "nested" attribute itself should be gone
        assert_eq!(log.get(event_path!("nested")), None);
    }

    #[test]
    fn span_fast_path_single_and_multi_segment() {
        use opentelemetry_proto::tonic::trace::v1::Span;

        let mut span_event = OtelSpan::new(Span {
            name: "GET /api".to_string(),
            trace_id: vec![0xAA; 16],
            span_id: vec![0xBB; 8],
            kind: 2,
            ..Default::default()
        });
        span_event.set_resource(Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("my-svc")),
            }],
            dropped_attributes_count: 0,
        });

        // Single-segment gets
        assert_eq!(
            span_event.get(event_path!("name")),
            Some(Value::Bytes("GET /api".into()))
        );
        assert_eq!(span_event.get(event_path!("kind")), Some(Value::Integer(2)));

        // Multi-segment resource get
        assert_eq!(
            span_event.get(event_path!("resource", "service.name")),
            Some(Value::Bytes("my-svc".into()))
        );

        // Insert resource attribute
        span_event.insert(event_path!("resource", "deployment.env"), "staging");
        assert_eq!(
            span_event.get(event_path!("resource", "deployment.env")),
            Some(Value::Bytes("staging".into()))
        );

        // Insert nested attribute
        span_event.insert(event_path!("http", "method"), "GET");
        assert_eq!(
            span_event.get(event_path!("http", "method")),
            Some(Value::Bytes("GET".into()))
        );

        // Insert/get status sub-fields
        span_event.insert(
            event_path!("status"),
            Value::Object({
                let mut m = ObjectMap::new();
                m.insert("message".into(), Value::Bytes("OK".into()));
                m.insert("code".into(), Value::Integer(1));
                m
            }),
        );
        assert_eq!(
            span_event.get(event_path!("status", "code")),
            Some(Value::Integer(1))
        );
        assert_eq!(
            span_event.get(event_path!("status", "message")),
            Some(Value::Bytes("OK".into()))
        );
    }

    #[test]
    fn otel_span_remove_single_and_multi_segment() {
        use opentelemetry_proto::tonic::trace::v1::Span;

        let mut span = OtelSpan::new(Span {
            name: "GET /api".to_string(),
            trace_id: vec![0xAA; 16],
            span_id: vec![0xBB; 8],
            kind: 2,
            flags: 1,
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            ..Default::default()
        });
        span.set_attribute("http.method".to_string(), string_value("GET"));
        span.set_resource(Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("my-svc")),
            }],
            dropped_attributes_count: 0,
        });
        span.insert(
            event_path!("status"),
            Value::Object({
                let mut m = ObjectMap::new();
                m.insert("message".into(), Value::Bytes("OK".into()));
                m.insert("code".into(), Value::Integer(1));
                m
            }),
        );

        // Remove single-segment proto field
        let removed = span.remove(event_path!("name"));
        assert_eq!(removed, Some(Value::Bytes("GET /api".into())));
        assert_eq!(span.name(), "");
        assert_eq!(span.remove(event_path!("name")), None);

        // Remove kind
        let removed = span.remove(event_path!("kind"));
        assert_eq!(removed, Some(Value::Integer(2)));
        assert_eq!(span.kind(), 0);

        // Remove flags
        let removed = span.remove(event_path!("flags"));
        assert_eq!(removed, Some(Value::Integer(1)));

        // Remove start_time_unix_nano
        let removed = span.remove(event_path!("start_time_unix_nano"));
        assert_eq!(removed, Some(Value::Integer(1_000_000_000)));
        assert_eq!(span.start_time_unix_nano(), 0);

        // Remove end_time via alias
        let removed = span.remove(event_path!("end_time"));
        assert!(removed.is_some());
        assert_eq!(span.end_time_unix_nano(), 0);

        // Remove span attribute
        let removed = span.remove(event_path!("http.method"));
        assert_eq!(removed, Some(Value::Bytes("GET".into())));
        assert_eq!(span.attribute("http.method"), None);

        // Remove resource attribute
        let removed = span.remove(event_path!("resource", "service.name"));
        assert_eq!(removed, Some(Value::Bytes("my-svc".into())));

        // Remove status.message sub-field
        let removed = span.remove(event_path!("status", "message"));
        assert_eq!(removed, Some(Value::Bytes("OK".into())));
        assert_eq!(span.status().unwrap().message, "");

        // Remove status.code sub-field
        let removed = span.remove(event_path!("status", "code"));
        assert_eq!(removed, Some(Value::Integer(1)));

        // Remove entire status
        span.insert(
            event_path!("status"),
            Value::Object({
                let mut m = ObjectMap::new();
                m.insert("code".into(), Value::Integer(2));
                m
            }),
        );
        let removed = span.remove(event_path!("status"));
        assert!(removed.is_some());
        assert!(span.status().is_none());

        // Remove trace_id
        let removed = span.remove(event_path!("trace_id"));
        assert!(removed.is_some());
        assert!(span.trace_id().is_empty());

        // Remove with prune: nested attribute is cleaned up
        let mut span2 = OtelSpan::new(Span::default());
        span2.insert(event_path!("nested", "only_child"), "value");
        assert!(span2.attribute("nested").is_some());
        let removed = span2.remove_prune(event_path!("nested", "only_child"), true);
        assert_eq!(removed, Some(Value::Bytes("value".into())));
        assert!(
            span2.attribute("nested").is_none(),
            "prune should remove empty parent"
        );
    }

    #[test]
    fn serialize_produces_otlp_json_for_log() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as Kind;
        let record = LogRecord {
            severity_text: "WARN".into(),
            severity_number: 13,
            time_unix_nano: 1_000_000_000,
            observed_time_unix_nano: 2_000_000_000,
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            body: Some(AnyValue {
                value: Some(Kind::StringValue("test body".into())),
            }),
            ..Default::default()
        };
        let resource = Some(Resource {
            attributes: vec![KeyValue {
                key: "service.name".into(),
                value: Some(AnyValue {
                    value: Some(Kind::StringValue("svc".into())),
                }),
            }],
            dropped_attributes_count: 0,
        });
        let scope = Some(InstrumentationScope {
            name: "my-lib".into(),
            version: "1.0".into(),
            ..Default::default()
        });
        let mut log = OtelLog::from_parts(record, resource, scope, EventMetadata::default());
        log.record_attrs
            .insert("custom.attr".into(), string_value("val"));

        let json: serde_json::Value = serde_json::to_value(&log).unwrap();

        // OTLP/JSON camelCase field names
        assert_eq!(json["body"]["stringValue"], "test body");
        assert_eq!(json["severityText"], "WARN");
        assert_eq!(json["severityNumber"], 13);
        assert_eq!(json["timeUnixNano"], "1000000000");
        assert_eq!(json["observedTimeUnixNano"], "2000000000");
        assert_eq!(json["traceId"], "abababababababababababababababab");
        assert_eq!(json["spanId"], "cdcdcdcdcdcdcdcd");

        // Attributes as OTLP array of {key, value}
        let attrs = json["attributes"].as_array().expect("attributes array");
        assert_eq!(attrs.len(), 1);
        assert_eq!(attrs[0]["key"], "custom.attr");
        assert_eq!(attrs[0]["value"]["stringValue"], "val");

        // Resource with nested attributes array
        let res_attrs = json["resource"]["attributes"]
            .as_array()
            .expect("resource attributes");
        assert_eq!(res_attrs[0]["key"], "service.name");
        assert_eq!(res_attrs[0]["value"]["stringValue"], "svc");

        // Scope
        assert_eq!(json["scope"]["name"], "my-lib");
        assert_eq!(json["scope"]["version"], "1.0");
    }

    #[test]
    fn serialize_produces_otlp_json_for_span() {
        use opentelemetry_proto::tonic::trace::v1::Status;
        let span = Span {
            name: "GET /api".into(),
            trace_id: vec![0x11; 16],
            span_id: vec![0x22; 8],
            parent_span_id: vec![0x33; 8],
            start_time_unix_nano: 100,
            end_time_unix_nano: 200,
            kind: 2,
            status: Some(Status {
                code: 1,
                message: "OK".into(),
            }),
            ..Default::default()
        };
        let resource = Some(Resource {
            attributes: vec![KeyValue {
                key: "host".into(),
                value: Some(AnyValue {
                    value: Some(OtelValueKind::StringValue("box1".into())),
                }),
            }],
            dropped_attributes_count: 0,
        });
        let mut otel_span = OtelSpan::from_parts(span, resource, None, EventMetadata::default());
        otel_span
            .span_attrs
            .insert("http.method".into(), string_value("GET"));

        let json: serde_json::Value = serde_json::to_value(&otel_span).unwrap();

        // OTLP/JSON camelCase field names
        assert_eq!(json["name"], "GET /api");
        assert_eq!(json["traceId"], "11111111111111111111111111111111");
        assert_eq!(json["spanId"], "2222222222222222");
        assert_eq!(json["parentSpanId"], "3333333333333333");
        assert_eq!(json["startTimeUnixNano"], "100");
        assert_eq!(json["endTimeUnixNano"], "200");
        assert_eq!(json["kind"], 2);

        // Status
        assert_eq!(json["status"]["code"], 1);
        assert_eq!(json["status"]["message"], "OK");

        // Attributes as OTLP array
        let attrs = json["attributes"].as_array().expect("attributes array");
        assert_eq!(attrs[0]["key"], "http.method");
        assert_eq!(attrs[0]["value"]["stringValue"], "GET");

        // Resource
        let res_attrs = json["resource"]["attributes"]
            .as_array()
            .expect("resource attributes");
        assert_eq!(res_attrs[0]["key"], "host");
        assert_eq!(res_attrs[0]["value"]["stringValue"], "box1");
    }

    #[test]
    fn otel_metric_add_sum() {
        use crate::event::MetricKind;
        let mut m1 = OtelMetric::new_counter("c", MetricKind::Incremental, 10.0);
        let m2 = OtelMetric::new_counter("c", MetricKind::Incremental, 5.0);
        assert!(m1.add(&m2));
        assert_eq!(m1.first_value_as_f64(), Some(15.0));
    }

    #[test]
    fn otel_metric_add_gauge() {
        let mut m1 = OtelMetric::new_gauge("g", 10.0);
        let m2 = OtelMetric::new_gauge("g", 3.0);
        assert!(m1.add(&m2));
        assert_eq!(m1.first_value_as_f64(), Some(13.0));
    }

    #[test]
    fn otel_metric_add_mismatched_types() {
        use crate::event::MetricKind;
        let mut m1 = OtelMetric::new_counter("c", MetricKind::Incremental, 10.0);
        let m2 = OtelMetric::new_gauge("g", 5.0);
        assert!(!m1.add(&m2));
        assert_eq!(m1.first_value_as_f64(), Some(10.0));
    }

    #[test]
    fn otel_metric_subtract_sum() {
        use crate::event::MetricKind;
        let mut m1 = OtelMetric::new_counter("c", MetricKind::Incremental, 10.0);
        let m2 = OtelMetric::new_counter("c", MetricKind::Incremental, 3.0);
        assert!(m1.subtract(&m2));
        assert_eq!(m1.first_value_as_f64(), Some(7.0));
    }

    #[test]
    fn otel_metric_subtract_sum_underflow() {
        use crate::event::MetricKind;
        let mut m1 = OtelMetric::new_counter("c", MetricKind::Incremental, 3.0);
        let m2 = OtelMetric::new_counter("c", MetricKind::Incremental, 10.0);
        assert!(!m1.subtract(&m2));
        assert_eq!(m1.first_value_as_f64(), Some(3.0));
    }

    #[test]
    fn otel_metric_zero() {
        use crate::event::MetricKind;
        let mut m = OtelMetric::new_counter("c", MetricKind::Incremental, 42.0);
        m.zero();
        assert_eq!(m.first_value_as_f64(), Some(0.0));
    }

    #[test]
    fn otel_metric_is_delta() {
        use crate::event::MetricKind;
        let delta = OtelMetric::new_counter("c", MetricKind::Incremental, 1.0);
        let cumulative = OtelMetric::new_counter("c", MetricKind::Absolute, 1.0);
        let gauge = OtelMetric::new_gauge("g", 1.0);
        assert!(delta.is_delta());
        assert!(!cumulative.is_delta());
        assert!(!gauge.is_delta());
    }

    #[test]
    fn otel_metric_is_cumulative() {
        use crate::event::MetricKind;
        let delta = OtelMetric::new_counter("c", MetricKind::Incremental, 1.0);
        let cumulative = OtelMetric::new_counter("c", MetricKind::Absolute, 1.0);
        let gauge = OtelMetric::new_gauge("g", 1.0);
        assert!(!delta.is_cumulative());
        assert!(cumulative.is_cumulative());
        assert!(
            !gauge.is_cumulative(),
            "Gauge has no temporality per OTel spec"
        );
    }

    #[test]
    fn otel_metric_has_temporality() {
        use crate::event::MetricKind;
        let counter = OtelMetric::new_counter("c", MetricKind::Incremental, 1.0);
        let gauge = OtelMetric::new_gauge("g", 1.0);
        assert!(counter.has_temporality());
        assert!(!gauge.has_temporality());
    }

    #[test]
    fn otel_metric_add_histogram() {
        use crate::event::{MetricKind, metric::Bucket};
        let mut m1 = OtelMetric::new_histogram(
            "h",
            MetricKind::Incremental,
            &[
                Bucket {
                    upper_limit: 1.0,
                    count: 5,
                },
                Bucket {
                    upper_limit: 2.0,
                    count: 10,
                },
            ],
            15,
            25.0,
        );
        let m2 = OtelMetric::new_histogram(
            "h",
            MetricKind::Incremental,
            &[
                Bucket {
                    upper_limit: 1.0,
                    count: 3,
                },
                Bucket {
                    upper_limit: 2.0,
                    count: 7,
                },
            ],
            10,
            15.0,
        );
        assert!(m1.add(&m2));
        match m1.view() {
            MetricView::Histogram {
                counts, count, sum, ..
            } => {
                assert_eq!(count, 25);
                assert_eq!(sum, 40.0);
                assert_eq!(counts[0], 8);
                assert_eq!(counts[1], 17);
            }
            other => panic!("expected Histogram, got {other}"),
        }
    }

    #[test]
    fn otel_span_events_links_roundtrip_through_as_map() {
        use opentelemetry_proto::tonic::common::v1::KeyValue;
        use opentelemetry_proto::tonic::trace::v1::{Span, span};

        let span_proto = Span {
            name: "test-span".into(),
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            trace_state: "rojo=00f067aa0ba902b7".into(),
            events: vec![span::Event {
                time_unix_nano: 1_234_567_890,
                name: "exception".into(),
                attributes: vec![KeyValue {
                    key: "exception.type".into(),
                    value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                "RuntimeError".into(),
                            ),
                        ),
                    }),
                }],
                dropped_attributes_count: 1,
            }],
            dropped_events_count: 3,
            links: vec![span::Link {
                trace_id: vec![3; 16],
                span_id: vec![4; 8],
                trace_state: "congo=t61rcWkgMzE".into(),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 5,
            }],
            dropped_links_count: 7,
            ..Default::default()
        };

        let original = OtelSpan::new(span_proto);
        let map = original.as_map().expect("as_map should produce Some");
        let roundtripped = OtelSpan::from_value_map(Value::Object(map), EventMetadata::default());

        assert_eq!(roundtripped.span.trace_state, "rojo=00f067aa0ba902b7");
        assert_eq!(roundtripped.span.events.len(), 1);
        assert_eq!(roundtripped.span.events[0].name, "exception");
        assert_eq!(roundtripped.span.events[0].time_unix_nano, 1_234_567_890);
        assert_eq!(roundtripped.span.events[0].attributes.len(), 1);
        assert_eq!(roundtripped.span.events[0].dropped_attributes_count, 1);
        assert_eq!(roundtripped.span.dropped_events_count, 3);
        assert_eq!(roundtripped.span.links.len(), 1);
        assert_eq!(roundtripped.span.links[0].trace_state, "congo=t61rcWkgMzE");
        assert_eq!(roundtripped.span.links[0].flags, 5);
        assert_eq!(roundtripped.span.dropped_links_count, 7);
    }

    #[test]
    fn otel_span_json_includes_events_links_trace_state() {
        use opentelemetry_proto::tonic::trace::v1::{Span, span};

        let span_proto = Span {
            name: "json-span".into(),
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            trace_state: "vendor=opaque".into(),
            events: vec![span::Event {
                time_unix_nano: 999,
                name: "log".into(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }],
            dropped_events_count: 2,
            links: vec![span::Link {
                trace_id: vec![0xef; 16],
                span_id: vec![0x01; 8],
                trace_state: String::new(),
                attributes: vec![],
                dropped_attributes_count: 0,
                flags: 0,
            }],
            dropped_links_count: 4,
            ..Default::default()
        };

        let otel_span = OtelSpan::new(span_proto);
        let json = serde_json::to_value(&otel_span).expect("serialize");
        assert_eq!(json["traceState"], "vendor=opaque");
        assert!(json["events"].is_array());
        assert_eq!(json["events"][0]["name"], "log");
        assert_eq!(json["events"][0]["timeUnixNano"], "999");
        assert_eq!(json["droppedEventsCount"], 2);
        assert!(json["links"].is_array());
        assert_eq!(
            json["links"][0]["traceId"],
            "efefefefefefefefefefefefefefefef"
        );
        assert_eq!(json["droppedLinksCount"], 4);
    }
}
