use std::collections::BTreeMap;

use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value::Value as OtelValueKind};
use sol_common::byte_size_of::ByteSizeOf;
use vrl::value::{KeyString, ObjectMap};

pub use opentelemetry_proto::tonic::common::v1::AnyValue;

use super::otel_event::{
    any_value_to_vrl, json_to_any_value, otel_value_to_str_ref, otel_value_to_tag_string,
    string_value, vrl_value_to_any_value,
};

/// BTreeMap-backed attribute container for O(log n) lookup.
/// Converts to/from `Vec<KeyValue>` at proto serialization boundaries.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OtelAttributes {
    pub(crate) inner: BTreeMap<String, AnyValue>,
}

impl OtelAttributes {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&AnyValue> {
        self.inner.get(key)
    }

    pub fn insert(&mut self, key: String, value: AnyValue) -> Option<AnyValue> {
        self.inner.insert(key, value)
    }

    pub fn remove(&mut self, key: &str) -> Option<AnyValue> {
        self.inner.remove(key)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &AnyValue)> {
        self.inner.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.inner.keys()
    }

    /// Convert from proto `Vec<KeyValue>` (at source ingestion boundary).
    /// Duplicate keys are merged into an `ArrayValue`.
    pub fn from_key_values(kvs: Vec<KeyValue>) -> Self {
        let mut inner = BTreeMap::new();
        for kv in kvs {
            let val = kv.value.unwrap_or(AnyValue { value: None });
            match inner.entry(kv.key) {
                std::collections::btree_map::Entry::Vacant(e) => {
                    e.insert(val);
                }
                std::collections::btree_map::Entry::Occupied(mut e) => {
                    let existing = e.get_mut();
                    if let Some(OtelValueKind::ArrayValue(arr)) = &mut existing.value {
                        arr.values.push(val);
                    } else {
                        let old = std::mem::take(existing);
                        *existing = AnyValue {
                            value: Some(OtelValueKind::ArrayValue(
                                opentelemetry_proto::tonic::common::v1::ArrayValue {
                                    values: vec![old, val],
                                },
                            )),
                        };
                    }
                }
            }
        }
        Self { inner }
    }

    /// Convert to proto `Vec<KeyValue>` (at sink egress boundary).
    pub fn to_key_values(&self) -> Vec<KeyValue> {
        self.inner
            .iter()
            .map(|(k, v)| {
                let value = if v.value.is_none() {
                    None
                } else {
                    Some(v.clone())
                };
                KeyValue {
                    key: k.clone(),
                    value,
                }
            })
            .collect()
    }

    /// Convert from VRL `ObjectMap` (at deserialization boundary).
    pub fn from_object_map(map: &ObjectMap) -> Self {
        let inner = map
            .iter()
            .map(|(k, v)| (k.to_string(), vrl_value_to_any_value(v)))
            .collect();
        Self { inner }
    }

    /// Convert to VRL `ObjectMap` for canonical value representation.
    pub fn to_object_map(&self) -> ObjectMap {
        self.inner
            .iter()
            .map(|(k, v)| (KeyString::from(k.clone()), any_value_to_vrl(v)))
            .collect()
    }
}

impl Eq for OtelAttributes {}

impl std::hash::Hash for OtelAttributes {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_usize(self.inner.len());
        for (k, v) in &self.inner {
            k.hash(state);
            hash_any_value(v, state);
        }
    }
}

impl PartialOrd for OtelAttributes {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OtelAttributes {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let mut a_iter = self.inner.iter();
        let mut b_iter = other.inner.iter();
        loop {
            match (a_iter.next(), b_iter.next()) {
                (None, None) => return std::cmp::Ordering::Equal,
                (None, Some(_)) => return std::cmp::Ordering::Less,
                (Some(_), None) => return std::cmp::Ordering::Greater,
                (Some((ak, av)), Some((bk, bv))) => {
                    match ak.cmp(bk) {
                        std::cmp::Ordering::Equal => {}
                        o => return o,
                    }
                    match cmp_any_value(av, bv) {
                        std::cmp::Ordering::Equal => {}
                        o => return o,
                    }
                }
            }
        }
    }
}

impl ByteSizeOf for OtelAttributes {
    fn allocated_bytes(&self) -> usize {
        self.inner.iter().fold(0, |acc, (k, v)| {
            acc + k.len() + any_value_allocated_bytes(v)
        })
    }
}

impl serde::Serialize for OtelAttributes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.inner.len()))?;
        for (k, v) in &self.inner {
            map.serialize_entry(k, &SerializableAnyValue(v))?;
        }
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for OtelAttributes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let map: BTreeMap<String, serde_json::Value> = BTreeMap::deserialize(deserializer)?;
        let inner = map
            .into_iter()
            .map(|(k, v)| (k, json_to_any_value(v)))
            .collect();
        Ok(Self { inner })
    }
}

impl OtelAttributes {
    pub fn get_string(&self, key: &str) -> Option<&str> {
        match self.inner.get(key) {
            Some(AnyValue {
                value: Some(OtelValueKind::StringValue(s)),
            }) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.inner.contains_key(key)
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "public API; callers pass owned values"
    )]
    pub fn insert_string(&mut self, key: String, value: String) -> Option<AnyValue> {
        self.inner.insert(key, string_value(&value))
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "public API; callers pass owned values"
    )]
    pub fn replace_string(&mut self, key: String, value: String) -> Option<String> {
        let old = self.inner.insert(key, string_value(&value));
        old.and_then(|av| match av.value {
            Some(OtelValueKind::StringValue(s)) => Some(s),
            _ => None,
        })
    }

    /// Iterate over all tag values, expanding `ArrayValue` entries into multiple pairs.
    /// This is the multi-valued counterpart to `iter_single()`.
    pub fn iter_all(&self) -> impl Iterator<Item = (&str, Option<&str>)> {
        self.inner.iter().flat_map(|(k, v)| {
            let pairs: Vec<(&str, Option<&str>)> = match &v.value {
                Some(OtelValueKind::StringValue(s)) => vec![(k.as_str(), Some(s.as_str()))],
                None => vec![(k.as_str(), None)],
                Some(OtelValueKind::ArrayValue(arr)) => arr
                    .values
                    .iter()
                    .map(|item| {
                        let s = match &item.value {
                            Some(OtelValueKind::StringValue(s)) => Some(s.as_str()),
                            None => None,
                            Some(other) => Some(otel_value_to_str_ref(other)),
                        };
                        (k.as_str(), s)
                    })
                    .collect(),
                Some(other) => vec![(k.as_str(), Some(otel_value_to_str_ref(other)))],
            };
            pairs.into_iter()
        })
    }

    pub fn iter_single(&self) -> impl Iterator<Item = (&str, Option<&str>)> {
        self.inner.iter().map(|(k, v)| {
            let s = match &v.value {
                Some(OtelValueKind::StringValue(s)) => Some(s.as_str()),
                None => None,
                Some(OtelValueKind::ArrayValue(arr)) => {
                    arr.values.last().and_then(|v| match &v.value {
                        Some(OtelValueKind::StringValue(s)) => Some(s.as_str()),
                        _ => None,
                    })
                }
                Some(other) => Some(otel_value_to_str_ref(other)),
            };
            (k.as_str(), s)
        })
    }

    pub fn into_iter_single(self) -> impl Iterator<Item = (String, String)> {
        self.inner.into_iter().filter_map(|(k, v)| {
            match v.value {
                Some(OtelValueKind::StringValue(s)) => Some((k, s)),
                Some(OtelValueKind::ArrayValue(arr)) => {
                    // For array values, take the last element (consistent with iter_single).
                    arr.values
                        .into_iter()
                        .last()
                        .and_then(|item| match item.value {
                            Some(OtelValueKind::StringValue(s)) => Some((k, s)),
                            Some(other) => Some((k, otel_value_to_tag_string(&other))),
                            None => None,
                        })
                }
                Some(other) => Some((k, otel_value_to_tag_string(&other))),
                None => None,
            }
        })
    }

    pub fn extend_strings(&mut self, pairs: impl IntoIterator<Item = (String, String)>) {
        for (k, v) in pairs {
            self.inner.insert(k, string_value(&v));
        }
    }

    pub fn retain<F: FnMut(&String, &mut AnyValue) -> bool>(&mut self, f: F) {
        self.inner.retain(f);
    }

    pub fn as_option(self) -> Option<Self> {
        if self.inner.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

impl std::iter::FromIterator<(String, String)> for OtelAttributes {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self {
            inner: iter
                .into_iter()
                .map(|(k, v)| (k, string_value(&v)))
                .collect(),
        }
    }
}

impl From<BTreeMap<String, String>> for OtelAttributes {
    fn from(map: BTreeMap<String, String>) -> Self {
        map.into_iter().collect()
    }
}

impl From<Vec<KeyValue>> for OtelAttributes {
    fn from(kvs: Vec<KeyValue>) -> Self {
        Self::from_key_values(kvs)
    }
}

fn hash_any_value<H: std::hash::Hasher>(av: &AnyValue, state: &mut H) {
    use std::hash::Hash;
    match &av.value {
        None => 0u8.hash(state),
        Some(OtelValueKind::StringValue(s)) => {
            1u8.hash(state);
            s.hash(state);
        }
        Some(OtelValueKind::BoolValue(b)) => {
            2u8.hash(state);
            b.hash(state);
        }
        Some(OtelValueKind::IntValue(i)) => {
            3u8.hash(state);
            i.hash(state);
        }
        Some(OtelValueKind::DoubleValue(f)) => {
            4u8.hash(state);
            f.to_bits().hash(state);
        }
        Some(OtelValueKind::ArrayValue(arr)) => {
            5u8.hash(state);
            state.write_usize(arr.values.len());
            for v in &arr.values {
                hash_any_value(v, state);
            }
        }
        Some(OtelValueKind::KvlistValue(kvl)) => {
            6u8.hash(state);
            state.write_usize(kvl.values.len());
            for kv in &kvl.values {
                kv.key.hash(state);
                if let Some(v) = &kv.value {
                    hash_any_value(v, state);
                }
            }
        }
        Some(OtelValueKind::BytesValue(b)) => {
            7u8.hash(state);
            b.hash(state);
        }
    }
}

fn cmp_any_value(a: &AnyValue, b: &AnyValue) -> std::cmp::Ordering {
    fn discriminant(av: &AnyValue) -> u8 {
        match &av.value {
            None => 0,
            Some(OtelValueKind::StringValue(_)) => 1,
            Some(OtelValueKind::BoolValue(_)) => 2,
            Some(OtelValueKind::IntValue(_)) => 3,
            Some(OtelValueKind::DoubleValue(_)) => 4,
            Some(OtelValueKind::ArrayValue(_)) => 5,
            Some(OtelValueKind::KvlistValue(_)) => 6,
            Some(OtelValueKind::BytesValue(_)) => 7,
        }
    }
    match (&a.value, &b.value) {
        (None, None) => std::cmp::Ordering::Equal,
        (Some(OtelValueKind::StringValue(a)), Some(OtelValueKind::StringValue(b))) => a.cmp(b),
        (Some(OtelValueKind::BoolValue(a)), Some(OtelValueKind::BoolValue(b))) => a.cmp(b),
        (Some(OtelValueKind::IntValue(a)), Some(OtelValueKind::IntValue(b))) => a.cmp(b),
        (Some(OtelValueKind::DoubleValue(a)), Some(OtelValueKind::DoubleValue(b))) => {
            a.total_cmp(b)
        }
        (Some(OtelValueKind::BytesValue(a)), Some(OtelValueKind::BytesValue(b))) => a.cmp(b),
        (Some(OtelValueKind::ArrayValue(a)), Some(OtelValueKind::ArrayValue(b))) => a
            .values
            .iter()
            .zip(b.values.iter())
            .map(|(x, y)| cmp_any_value(x, y))
            .find(|o| *o != std::cmp::Ordering::Equal)
            .unwrap_or_else(|| a.values.len().cmp(&b.values.len())),
        (Some(OtelValueKind::KvlistValue(a)), Some(OtelValueKind::KvlistValue(b))) => a
            .values
            .iter()
            .zip(b.values.iter())
            .map(|(x, y)| {
                x.key.cmp(&y.key).then_with(|| match (&x.value, &y.value) {
                    (Some(xv), Some(yv)) => cmp_any_value(xv, yv),
                    (None, None) => std::cmp::Ordering::Equal,
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                })
            })
            .find(|o| *o != std::cmp::Ordering::Equal)
            .unwrap_or_else(|| a.values.len().cmp(&b.values.len())),
        _ => discriminant(a).cmp(&discriminant(b)),
    }
}

fn any_value_allocated_bytes(av: &AnyValue) -> usize {
    match &av.value {
        Some(OtelValueKind::StringValue(s)) => s.len(),
        Some(OtelValueKind::BytesValue(b)) => b.len(),
        Some(OtelValueKind::ArrayValue(arr)) => {
            arr.values
                .iter()
                .map(any_value_allocated_bytes)
                .sum::<usize>()
                + arr.values.capacity() * std::mem::size_of::<AnyValue>()
        }
        Some(OtelValueKind::KvlistValue(kvl)) => {
            kvl.values
                .iter()
                .map(|kv| kv.key.len() + kv.value.as_ref().map_or(0, any_value_allocated_bytes))
                .sum::<usize>()
                + kvl.values.capacity() * std::mem::size_of::<KeyValue>()
        }
        _ => 0,
    }
}

pub(super) struct SerializableAnyValue<'a>(pub(super) &'a AnyValue);

impl serde::Serialize for SerializableAnyValue<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0.value {
            None => serializer.serialize_none(),
            Some(OtelValueKind::StringValue(s)) => serializer.serialize_str(s),
            Some(OtelValueKind::BoolValue(b)) => serializer.serialize_bool(*b),
            Some(OtelValueKind::IntValue(i)) => serializer.serialize_i64(*i),
            Some(OtelValueKind::DoubleValue(f)) => serializer.serialize_f64(*f),
            Some(OtelValueKind::BytesValue(b)) => serializer.serialize_bytes(b),
            Some(OtelValueKind::ArrayValue(arr)) => {
                use serde::ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(arr.values.len()))?;
                for v in &arr.values {
                    seq.serialize_element(&SerializableAnyValue(v))?;
                }
                seq.end()
            }
            Some(OtelValueKind::KvlistValue(kvl)) => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(kvl.values.len()))?;
                for kv in &kvl.values {
                    let val = kv.value.as_ref().map(SerializableAnyValue);
                    map.serialize_entry(&kv.key, &val)?;
                }
                map.end()
            }
        }
    }
}
