// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Query-side OTLP→Prometheus attribute lookup (`prom_attr`).
//!
//! Sol stores attributes with their raw OTLP keys (dotted, e.g. `http.route`,
//! `deployment.environment`). Grafana/Mimir query the **Prometheus-normalized**
//! name (`http_route`, `deployment_environment`) — Mimir normalizes on OTLP
//! ingest, Sol does not. `prom_attr(attributes_json, 'http_route')` bridges that
//! on the read side: it returns the value of the stored key whose normalized
//! form (`[^A-Za-z0-9_]` → `_`) equals the requested Prometheus name, so the
//! Prometheus API presents a normalized view over raw OTLP storage.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, Signature, SimpleScalarUDF, TypeSignature, Volatility, create_udf,
};

/// The Arrow `Map<Utf8,Utf8>` data type the codec writes for the metric
/// `attributes` column, used as the UDF argument type so the read side binds the
/// columnar map directly (no JSON parse). Mirrors the codec schema
/// (`key_value { key: Utf8 (non-null), value: Utf8 (nullable) }`, map non-null
/// entries, `keys_sorted = false`).
pub(super) fn attributes_map_type() -> DataType {
    let key = Field::new("key", DataType::Utf8, false);
    let value = Field::new("value", DataType::Utf8, true);
    let entries = Field::new(
        "key_value",
        DataType::Struct(vec![key, value].into()),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

/// Downcast a UDF argument array to a `MapArray`, erroring with `name`.
fn as_map_array<'a>(arr: &'a ArrayRef, name: &str) -> DfResult<&'a MapArray> {
    arr.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!("{name} expects a Map argument"))
    })
}

/// The `(raw_key, value)` entries of the attributes map at row `i`, or `None`
/// for a null map. Keys/values are read columnar from the map's string child
/// arrays — no JSON parsing. Keys are the raw OTLP keys (normalization is the
/// caller's responsibility, via [`normalize`]).
pub(super) fn map_row_entries(map: &MapArray, i: usize) -> Option<Vec<(String, String)>> {
    if map.is_null(i) {
        return None;
    }
    let keys = map.keys();
    let values = map.values();
    let keys = keys.as_any().downcast_ref::<StringArray>()?;
    let values = values.as_any().downcast_ref::<StringArray>()?;
    let offsets = map.value_offsets();
    // Map offsets are monotonically non-negative by construction.
    let start = usize::try_from(offsets[i]).unwrap_or(0);
    let end = usize::try_from(offsets[i + 1]).unwrap_or(0);
    let mut out = Vec::with_capacity(end - start);
    for j in start..end {
        if keys.is_null(j) {
            continue;
        }
        let v = if values.is_null(j) {
            String::new()
        } else {
            values.value(j).to_string()
        };
        out.push((keys.value(j).to_string(), v));
    }
    Some(out)
}

/// Build the normalized label map for the attributes map at row `i`: each raw
/// key is normalized via [`normalize`]; the first occurrence of a normalized key
/// wins (matching the stable iteration order of the original JSON object path).
pub(super) fn map_row_normalized_labels(map: &MapArray, i: usize) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    if let Some(entries) = map_row_entries(map, i) {
        for (k, v) in entries {
            m.entry(normalize(&k)).or_insert(v);
        }
    }
    m
}

/// `prom_series_key(attributes) -> Utf8`: a canonical, groupable string key for a
/// data point's label set, derived from the columnar `attributes` MAP. DataFusion
/// cannot `GROUP BY`/`PARTITION BY` a `Map` column directly (Arrow row-format does
/// not support maps), so window/partition plans key on this UDF's output instead
/// of the raw map. The key is the sorted, escaped `k=v` join of the *raw* OTLP
/// entries (raw keys keep it injective; normalization for display happens in the
/// materialization path). A null map yields the empty string.
pub fn prom_series_key_udf() -> ScalarUDF {
    let fun = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let map = as_map_array(&arrays[0], "prom_series_key")?;
        let out: StringArray = (0..map.len())
            .map(|i| Some(series_key_string(map, i)))
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    };
    create_udf(
        "prom_series_key",
        vec![attributes_map_type()],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(fun),
    )
}

/// Canonical series-key string for the map at row `i`: sorted raw `k=v` pairs
/// joined by `\x1f`, with `\`, `=`, `\x1f` backslash-escaped so the encoding is
/// unambiguous (matches the `GroupKey` escaping scheme).
fn series_key_string(map: &MapArray, i: usize) -> String {
    let mut entries = map_row_entries(map, i).unwrap_or_default();
    entries.sort();
    let mut out = String::new();
    for (k, v) in entries {
        if !out.is_empty() {
            out.push('\u{1f}');
        }
        push_escaped(&mut out, &k);
        out.push('=');
        push_escaped(&mut out, &v);
    }
    out
}

/// Append `s` escaping `\`, `=`, `\x1f` (shared escaping with `GroupKey`).
fn push_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        if c == '\\' || c == '=' || c == '\u{1f}' {
            out.push('\\');
        }
        out.push(c);
    }
}

/// Normalize an OTLP attribute key to its Prometheus label name: every
/// character outside `[A-Za-z0-9_]` becomes `_` (matches the OTLP→Prometheus
/// normalization Mimir applies on ingest). Shared with the label-discovery
/// endpoints (`/labels`), which surface stored keys under their normalized name.
pub(super) fn normalize(key: &str) -> String {
    key.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Look up `prom_name` in the attributes MAP at row `i` by normalized key,
/// reading columnar (no JSON parse). Returns the first matching entry's value.
fn lookup_map(map: &MapArray, i: usize, prom_name: &str) -> Option<String> {
    let entries = map_row_entries(map, i)?;
    for (key, value) in entries {
        if normalize(&key) == prom_name {
            return Some(value);
        }
    }
    None
}

/// Look up `prom_name` in a JSON-object attributes string by normalized key.
/// Used for the `resource_attributes`/`scope_attributes` JSON columns and the
/// logs/traces `attributes` (which remain JSON — only the metric `attributes`
/// column is a columnar MAP, per the materialized-label-columns ADR).
fn lookup_json(attributes_json: &str, prom_name: &str) -> Option<String> {
    let map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(attributes_json).ok()?;
    for (key, value) in &map {
        if normalize(key) == prom_name {
            return match value {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Null => None,
                other => Some(other.to_string()),
            };
        }
    }
    None
}

/// Core of `prom_attr`: `(attributes, prom_name)` arrays → value array. The first
/// argument is either the columnar metric-`attributes` MAP (read parse-free) or a
/// JSON-string attribute column (`resource_attributes`/logs/traces); dispatched on
/// the runtime array type. Split out so it is unit-testable.
fn eval_prom_attr(arrays: &[ArrayRef]) -> DfResult<ArrayRef> {
    let names = arrays[1].as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "prom_attr expects a Utf8 key argument".to_string(),
        )
    })?;
    let name_at = |i: usize| (!names.is_null(i)).then(|| names.value(i));
    let out: StringArray = if let Some(map) = arrays[0].as_any().downcast_ref::<MapArray>() {
        (0..map.len())
            .map(|i| {
                if map.is_null(i) {
                    None
                } else {
                    name_at(i).and_then(|n| lookup_map(map, i, n))
                }
            })
            .collect()
    } else {
        let attrs = arrays[0].as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "prom_attr expects a Map or Utf8 attributes argument".to_string(),
            )
        })?;
        (0..attrs.len())
            .map(|i| {
                if attrs.is_null(i) {
                    None
                } else {
                    name_at(i).and_then(|n| lookup_json(attrs.value(i), n))
                }
            })
            .collect()
    };
    Ok(Arc::new(out) as ArrayRef)
}

/// `prom_attr(attributes, prom_name) -> value | NULL`. Accepts either the columnar
/// metric `attributes` MAP (extracted parse-free, promql-pushdown T7) or a
/// JSON-string attribute column (`resource_attributes`, logs/traces `attributes`).
pub fn prom_attr_udf() -> ScalarUDF {
    let fun = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        Ok(ColumnarValue::Array(eval_prom_attr(&arrays)?))
    };
    let signature = Signature::one_of(
        vec![
            TypeSignature::Exact(vec![attributes_map_type(), DataType::Utf8]),
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
        ],
        Volatility::Immutable,
    );
    ScalarUDF::from(SimpleScalarUDF::new_with_signature(
        "prom_attr",
        signature,
        DataType::Utf8,
        Arc::new(fun),
    ))
}

#[cfg(test)]
pub(in crate::querier) mod tests {
    use super::*;

    #[test]
    fn test_normalize_otlp_to_prometheus() {
        assert_eq!(normalize("http.route"), "http_route");
        assert_eq!(
            normalize("deployment.environment"),
            "deployment_environment"
        );
        assert_eq!(normalize("already_ok"), "already_ok");
        assert_eq!(normalize("url.scheme"), "url_scheme");
    }

    /// Build a single-row `MapArray` from `entries` (test helper). `None` builds a
    /// null map row.
    pub(in crate::querier) fn map_array_from(rows: &[Option<&[(&str, &str)]>]) -> MapArray {
        use datafusion::arrow::array::{MapBuilder, StringBuilder};
        let mut b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for row in rows {
            match row {
                Some(entries) => {
                    for (k, v) in *entries {
                        b.keys().append_value(k);
                        b.values().append_value(v);
                    }
                    b.append(true).unwrap();
                }
                None => {
                    b.append(false).unwrap();
                }
            }
        }
        b.finish()
    }

    /// The Arrow `Field` for the `attributes` MAP column, named with the codec's
    /// `key_value`/`key`/`value` child names so test fixtures match the catalog
    /// schema (and DataFusion's parquet reader) exactly.
    pub(in crate::querier) fn attributes_map_field() -> datafusion::arrow::datatypes::Field {
        datafusion::arrow::datatypes::Field::new("attributes", attributes_map_type(), true)
    }

    /// Build a `MapArray` for test fixtures from JSON-object strings (the form the
    /// fixtures previously stored as a Utf8 column). Each string is parsed; a
    /// string value is stored verbatim, any other scalar as its JSON text — the
    /// same value semantics the codec applies. An empty object `{}` is stored as a
    /// present-but-empty map. Returns an array matching [`attributes_map_field`].
    pub(in crate::querier) fn json_map_array<S: AsRef<str>>(rows: &[S]) -> ArrayRef {
        use datafusion::arrow::array::{MapBuilder, MapFieldNames, StringBuilder};
        let names = MapFieldNames {
            entry: "key_value".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
        for row in rows {
            if let Ok(serde_json::Value::Object(map)) =
                serde_json::from_str::<serde_json::Value>(row.as_ref())
            {
                for (k, v) in map {
                    let val = match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    b.keys().append_value(&k);
                    b.values().append_value(&val);
                }
            }
            b.append(true).unwrap();
        }
        Arc::new(b.finish())
    }

    #[test]
    fn test_lookup_reads_map_columnar() {
        // T7: prom_attr resolves a normalized key from the columnar MAP — no JSON.
        let m = map_array_from(&[Some(&[
            ("http.route", "/x"),
            ("deployment.environment", "dev"),
            ("n", "5"),
        ])]);
        assert_eq!(lookup_map(&m, 0, "http_route").as_deref(), Some("/x"));
        assert_eq!(
            lookup_map(&m, 0, "deployment_environment").as_deref(),
            Some("dev")
        );
        assert_eq!(lookup_map(&m, 0, "n").as_deref(), Some("5"));
        assert_eq!(lookup_map(&m, 0, "absent"), None);
        // A null map yields no value.
        let null_map = map_array_from(&[None]);
        assert_eq!(lookup_map(&null_map, 0, "x"), None);
    }

    #[test]
    fn test_prom_series_key_is_injective_and_sorted() {
        let m = map_array_from(&[
            Some(&[("cpu", "0"), ("mode", "idle")]),
            Some(&[("mode", "idle"), ("cpu", "0")]),
            None,
        ]);
        // Order-independent (sorted) → rows 0 and 1 collide; null → empty key.
        assert_eq!(series_key_string(&m, 0), series_key_string(&m, 1));
        assert!(series_key_string(&m, 0).contains("cpu=0"));
        assert_eq!(series_key_string(&m, 2), "");
    }

    #[test]
    fn test_prom_attr_reads_map_column() {
        // T7: the prom_attr UDF evaluates over a Map<Utf8,Utf8> argument, resolving
        // the normalized key columnar — no JSON. (`http.route` → `http_route`.)
        let map: ArrayRef = Arc::new(map_array_from(&[
            Some(&[("http.route", "/x"), ("code", "200")]),
            None,
        ]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![Some("http_route"), Some("code")]));
        let out = eval_prom_attr(&[map, names]).unwrap();
        let arr = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "/x"); // normalized key match, columnar
        assert!(arr.is_null(1)); // null map → NULL
        // And the UDF declares the Map signature.
        assert_eq!(prom_attr_udf().name(), "prom_attr");
    }
}
