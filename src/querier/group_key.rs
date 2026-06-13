// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Canonical group-key format + `prom_group_key`/`prom_group_key_reproject` UDFs.
//!
//! The aggregation-pushdown plan (`promql-pushdown` Task 2) groups on a single
//! string column produced by `prom_group_key(...)`. That string is both the
//! `GROUP BY` key and the serialized result label set, parsed back once per
//! output group. Its format is therefore load-bearing.
//!
//! Format ([ADR: group-key-format]): sorted `key=value` pairs joined by `\x1f`
//! (unit separator). `=`, `\x1f` and the escape char `\` are backslash-escaped
//! in both keys and values, so [`GroupKey::parse`] is the exact inverse of
//! [`GroupKey::build`]. Empty key set → empty string `""`.
//!
//! Grouping rules (matching today's `LabelCols::labels` semantics so parity
//! holds): `by(L)` keeps `L ∩ present`; `without(L)` keeps every present label
//! except `L` and `__name__`; no modifier keeps nothing (constant key `""`).
//!
//! [ADR: group-key-format]: ../../docs/workspace/promql-pushdown/adrs/group-key-format.md

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use promql_parser::parser::LabelModifier;

/// PromQL aggregation grouping: `by(labels)` keeps only those labels in the
/// result; `without(labels)` keeps every label except those (and `__name__`);
/// no modifier collapses all series into one empty-labelled group. Operating on
/// an exploded label map lets `without` work even though the source labels live
/// inside the `attributes` JSON.
pub(super) enum AggGrouping {
    By(Vec<String>),
    Without(Vec<String>),
    All,
}

impl AggGrouping {
    pub(super) fn from(modifier: &Option<LabelModifier>) -> Self {
        match modifier {
            Some(LabelModifier::Include(l)) => AggGrouping::By(l.labels.clone()),
            Some(LabelModifier::Exclude(l)) => AggGrouping::Without(l.labels.clone()),
            None => AggGrouping::All,
        }
    }

    /// The labels carried by a series' result group (the grouping projection).
    pub(super) fn result_labels(
        &self,
        labels: &BTreeMap<String, String>,
    ) -> BTreeMap<String, String> {
        match self {
            AggGrouping::By(set) => labels
                .iter()
                .filter(|(k, _)| set.iter().any(|s| s == *k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            AggGrouping::Without(set) => labels
                .iter()
                .filter(|(k, _)| k.as_str() != "__name__" && !set.iter().any(|s| s == *k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            AggGrouping::All => BTreeMap::new(),
        }
    }

    /// Encode this grouping as the `(mode, labels)` UDF argument pair: `mode` is
    /// `by`/`without`/`all`; `labels` is the `\x1f`-joined label list (empty for
    /// `all`). Inverse of [`grouping_from_args`] — keeps the on-wire encoding in
    /// one place so plan builders never construct it by hand.
    pub(super) fn encode(&self) -> (&'static str, String) {
        match self {
            AggGrouping::By(l) => ("by", l.join(&SEP.to_string())),
            AggGrouping::Without(l) => ("without", l.join(&SEP.to_string())),
            AggGrouping::All => ("all", String::new()),
        }
    }
}

/// `prom_group_key(attributes, promoted, mode, labels)` as an `Expr` for a **leaf**
/// inner (a selector / range function carrying `attributes` + a promoted column).
pub(super) fn prom_group_key_call(
    attributes: datafusion::logical_expr::Expr,
    promoted: datafusion::logical_expr::Expr,
    grouping: &AggGrouping,
) -> datafusion::logical_expr::Expr {
    use datafusion::prelude::lit;
    let (mode, labels) = grouping.encode();
    prom_group_key_udf().call(vec![attributes, promoted, lit(mode), lit(labels)])
}

/// `prom_group_key_reproject(inner_key, mode, labels)` as an `Expr` for a **nested**
/// inner (another aggregate that already carries a `prom_group_key` column).
pub(super) fn prom_group_key_reproject_call(
    inner_key: datafusion::logical_expr::Expr,
    grouping: &AggGrouping,
) -> datafusion::logical_expr::Expr {
    use datafusion::prelude::lit;
    let (mode, labels) = grouping.encode();
    prom_group_key_reproject_udf().call(vec![inner_key, lit(mode), lit(labels)])
}

/// Canonical, reversible group-key string. See module docs for the format.
pub(super) struct GroupKey;

/// Pair separator: ASCII unit separator (`\x1f`).
const SEP: char = '\u{1f}';
/// Key/value separator inside a pair.
const KV: char = '=';
/// Escape character.
const ESC: char = '\\';

/// Append `s` to `out`, escaping `\`, `=` and `\x1f` with a backslash so the
/// encoding is unambiguous and reversible.
fn push_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        if c == ESC || c == KV || c == SEP {
            out.push(ESC);
        }
        out.push(c);
    }
}

impl GroupKey {
    /// Build the canonical key for `labels` projected through `grouping`.
    pub(super) fn build(labels: &BTreeMap<String, String>, grouping: &AggGrouping) -> String {
        let projected = grouping.result_labels(labels);
        let mut out = String::new();
        for (k, v) in &projected {
            if !out.is_empty() {
                out.push(SEP);
            }
            push_escaped(&mut out, k);
            out.push(KV);
            push_escaped(&mut out, v);
        }
        out
    }

    /// Parse a canonical key back into its label map — the exact inverse of
    /// [`GroupKey::build`] (round-trip invariant).
    pub(super) fn parse(key: &str) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        if key.is_empty() {
            return map;
        }
        // Split into pairs on unescaped SEP, then each pair on unescaped KV,
        // un-escaping as we consume characters.
        let mut k = String::new();
        let mut v = String::new();
        let mut in_value = false;
        let mut escaped = false;
        let mut commit = |k: &mut String, v: &mut String, in_value: &mut bool| {
            map.insert(std::mem::take(k), std::mem::take(v));
            *in_value = false;
        };
        for c in key.chars() {
            let target = if in_value { &mut v } else { &mut k };
            if escaped {
                target.push(c);
                escaped = false;
            } else if c == ESC {
                escaped = true;
            } else if c == KV && !in_value {
                in_value = true;
            } else if c == SEP {
                commit(&mut k, &mut v, &mut in_value);
            } else {
                target.push(c);
            }
        }
        commit(&mut k, &mut v, &mut in_value);
        map
    }
}

/// Decode the `(mode, labels)` UDF arguments into an [`AggGrouping`]. `mode` is
/// one of `by`/`without`/`all`; `labels` is a `\x1f`-joined list (empty when
/// `mode = all`). Keeping the encoding here means callers never build SQL.
fn grouping_from_args(mode: &str, labels: &str) -> DfResult<AggGrouping> {
    let parse_labels = || -> Vec<String> {
        if labels.is_empty() {
            Vec::new()
        } else {
            labels.split(SEP).map(str::to_string).collect()
        }
    };
    match mode {
        "by" => Ok(AggGrouping::By(parse_labels())),
        "without" => Ok(AggGrouping::Without(parse_labels())),
        "all" => Ok(AggGrouping::All),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "prom_group_key: unknown mode {other:?} (expected by/without/all)"
        ))),
    }
}

/// Build the full label map for one row: promoted columns unioned with the
/// columnar `attributes` MAP keys (each normalized via [`super::udf::normalize`]),
/// promoted columns winning on a key collision — identical to
/// `LabelCols::labels`, but read parse-free from the MAP (promql-pushdown T7).
/// `__name__` is supplied by the caller via the promoted columns (the `prom_name`
/// column).
fn row_labels(
    attributes: Option<&MapArray>,
    promoted: &[(String, &StringArray)],
    i: usize,
) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for (key, arr) in promoted {
        if !arr.is_null(i) {
            m.insert(key.clone(), arr.value(i).to_string());
        }
    }
    if let Some(map) = attributes {
        for (k, v) in super::udf::map_row_normalized_labels(map, i) {
            // Promoted columns win over attributes on a key collision.
            m.entry(k).or_insert(v);
        }
    }
    m
}

/// Downcast each arg to a `StringArray`, erroring with `name` on a type mismatch.
fn as_string_arrays<'a>(
    arrays: &'a [ArrayRef],
    expected: usize,
    name: &str,
) -> DfResult<Vec<&'a StringArray>> {
    if arrays.len() != expected {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "{name} expects {expected} Utf8 arguments"
        )));
    }
    arrays
        .iter()
        .map(|a| {
            a.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{name} expects Utf8 arguments"
                ))
            })
        })
        .collect()
}

/// Core of `prom_group_key`: `(attributes, promoted, mode, labels)` arrays → key
/// array. Split out so it is unit-testable without constructing a full
/// `ScalarFunctionArgs`.
fn eval_group_key(arrays: &[ArrayRef]) -> DfResult<ArrayRef> {
    if arrays.len() != 4 {
        return Err(datafusion::error::DataFusionError::Execution(
            "prom_group_key expects 4 arguments (Map, Utf8, Utf8, Utf8)".to_string(),
        ));
    }
    let attrs = arrays[0].as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(
            "prom_group_key expects a Map first argument".to_string(),
        )
    })?;
    let rest = as_string_arrays(&arrays[1..], 3, "prom_group_key")?;
    let (promoted_arr, mode_arr, labels_arr) = (rest[0], rest[1], rest[2]);
    let out: StringArray = (0..attrs.len())
        .map(|i| {
            let mode = if mode_arr.is_null(i) { "all" } else { mode_arr.value(i) };
            let labels = if labels_arr.is_null(i) { "" } else { labels_arr.value(i) };
            let grouping = grouping_from_args(mode, labels).ok()?;
            let promoted: Vec<(String, &StringArray)> =
                vec![("service_name".to_string(), promoted_arr)];
            let attributes = if attrs.is_null(i) { None } else { Some(attrs) };
            let labels_map = row_labels(attributes, &promoted, i);
            Some(GroupKey::build(&labels_map, &grouping))
        })
        .collect();
    Ok(Arc::new(out) as ArrayRef)
}

/// `prom_group_key(attributes_json, promoted, mode, labels) -> Utf8` group key.
///
/// Builds the row's label map (promoted column unioned with the parsed
/// `attributes` JSON, promoted winning) and returns [`GroupKey::build`] for the
/// grouping decoded from `(mode, labels)`. The `promoted` argument carries one
/// promoted label (e.g. `service_name`); its column name is fixed to
/// `service_name` here — the canonical promoted label for metrics.
pub(super) fn prom_group_key_udf() -> ScalarUDF {
    let fun = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        Ok(ColumnarValue::Array(eval_group_key(&arrays)?))
    };
    create_udf(
        "prom_group_key",
        vec![
            super::udf::attributes_map_type(),
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
        ],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(fun),
    )
}

/// Core of `prom_group_key_reproject`: `(inner_key, mode, labels)` arrays → key.
fn eval_group_key_reproject(arrays: &[ArrayRef]) -> DfResult<ArrayRef> {
    let cols = as_string_arrays(arrays, 3, "prom_group_key_reproject")?;
    let (key_arr, mode_arr, labels_arr) = (cols[0], cols[1], cols[2]);
    let out: StringArray = (0..key_arr.len())
        .map(|i| {
            if key_arr.is_null(i) {
                return None;
            }
            let mode = if mode_arr.is_null(i) { "all" } else { mode_arr.value(i) };
            let labels = if labels_arr.is_null(i) { "" } else { labels_arr.value(i) };
            let grouping = grouping_from_args(mode, labels).ok()?;
            let inner = GroupKey::parse(key_arr.value(i));
            Some(GroupKey::build(&inner, &grouping))
        })
        .collect();
    Ok(Arc::new(out) as ArrayRef)
}

/// `prom_group_key_reproject(inner_key, mode, labels) -> Utf8` =
/// `GroupKey::build(GroupKey::parse(inner_key), grouping)`.
///
/// Re-keys an already-built group key for an outer aggregate, enabling mixed
/// nesting (e.g. `sum by (cpu) (sum without (mode) (m))`) without touching the
/// raw `attributes` JSON.
pub(super) fn prom_group_key_reproject_udf() -> ScalarUDF {
    let fun = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        Ok(ColumnarValue::Array(eval_group_key_reproject(&arrays)?))
    };
    create_udf(
        "prom_group_key_reproject",
        vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(fun),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn by(l: &[&str]) -> AggGrouping {
        AggGrouping::By(l.iter().map(|s| (*s).to_string()).collect())
    }

    fn without(l: &[&str]) -> AggGrouping {
        AggGrouping::Without(l.iter().map(|s| (*s).to_string()).collect())
    }

    #[test]
    fn test_group_key_build_parse_roundtrip() {
        let l = labels(&[
            ("__name__", "node_cpu"),
            ("cpu", "0"),
            ("mode", "idle"),
            ("service_name", "node=exporter\u{1f}weird\\"),
        ]);
        for g in [by(&["cpu", "service_name"]), without(&["mode"]), AggGrouping::All] {
            let key = GroupKey::build(&l, &g);
            assert_eq!(GroupKey::parse(&key), g.result_labels(&l));
        }
    }

    #[test]
    fn test_group_key_by_keeps_only_listed() {
        let l = labels(&[("cpu", "0"), ("mode", "idle"), ("__name__", "m")]);
        // `present` requested → kept; `absent` requested → not invented.
        let key = GroupKey::build(&l, &by(&["cpu", "absent"]));
        assert_eq!(GroupKey::parse(&key), labels(&[("cpu", "0")]));
    }

    #[test]
    fn test_group_key_without_drops_set_and_name() {
        let l = labels(&[("cpu", "0"), ("mode", "idle"), ("__name__", "m")]);
        let key = GroupKey::build(&l, &without(&["mode"]));
        // drops `mode` and `__name__`, keeps the rest.
        assert_eq!(GroupKey::parse(&key), labels(&[("cpu", "0")]));
    }

    #[test]
    fn test_group_key_promoted_wins_on_collision() {
        // service_name present both as a promoted column and in the attributes MAP;
        // promoted must win. The map is read columnar (no JSON).
        let promoted_arr = StringArray::from(vec![Some("promoted-svc")]);
        let promoted: Vec<(String, &StringArray)> =
            vec![("service_name".to_string(), &promoted_arr)];
        let map = super::super::udf::tests::map_array_from(&[Some(&[
            ("service.name", "attr-svc"),
            ("cpu", "0"),
        ])]);
        let m = row_labels(Some(&map), &promoted, 0);
        assert_eq!(m.get("service_name").map(String::as_str), Some("promoted-svc"));
        assert_eq!(m.get("cpu").map(String::as_str), Some("0"));
    }

    #[test]
    fn test_group_key_reads_map_column() {
        // T7: prom_group_key builds the key from the columnar MAP — no JSON parse.
        // The UDF is registered (see catalog.rs); exercise its evaluation core
        // over an arrow batch directly.
        let attrs: ArrayRef = Arc::new(super::super::udf::tests::map_array_from(&[
            Some(&[("cpu", "0"), ("mode", "idle")]),
            Some(&[("cpu", "1"), ("mode", "system")]),
        ]));
        let promoted: ArrayRef = Arc::new(StringArray::from(vec![Some("svc-a"), Some("svc-a")]));
        let mode: ArrayRef = Arc::new(StringArray::from(vec![Some("by"), Some("by")]));
        let lbls: ArrayRef = Arc::new(StringArray::from(vec![Some("cpu"), Some("cpu")]));
        let out = eval_group_key(&[attrs, promoted, mode, lbls]).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        // by(cpu): only the cpu label survives.
        assert_eq!(GroupKey::parse(out.value(0)), labels(&[("cpu", "0")]));
        assert_eq!(GroupKey::parse(out.value(1)), labels(&[("cpu", "1")]));
        // and the UDF wires that core in with the right signature.
        assert_eq!(prom_group_key_udf().name(), "prom_group_key");
        assert_eq!(
            prom_group_key_reproject_udf().name(),
            "prom_group_key_reproject"
        );
    }

    #[test]
    fn test_grouping_encode_roundtrips_through_args() {
        // encode() is the inverse of grouping_from_args(): a grouping survives the
        // (mode, labels) UDF-arg encoding used by the plan builders.
        let l = labels(&[("cpu", "0"), ("mode", "user"), ("le", "1"), ("__name__", "m")]);
        for g in [by(&["cpu", "mode"]), without(&["le"]), AggGrouping::All] {
            let (mode, label_list) = g.encode();
            let decoded = grouping_from_args(mode, &label_list).unwrap();
            assert_eq!(decoded.result_labels(&l), g.result_labels(&l));
        }
    }

    #[test]
    fn test_group_key_reproject() {
        // reproject(build(labels, without[mode]), by[cpu]) == build(labels, by[cpu]).
        let l = labels(&[
            ("__name__", "m"),
            ("cpu", "0"),
            ("mode", "idle"),
            ("service_name", "svc"),
        ]);
        let inner = GroupKey::build(&l, &without(&["mode"]));
        let reprojected = GroupKey::build(&GroupKey::parse(&inner), &by(&["cpu"]));
        let direct = GroupKey::build(&l, &by(&["cpu"]));
        assert_eq!(reprojected, direct);
        assert_eq!(GroupKey::parse(&reprojected), labels(&[("cpu", "0")]));
    }
}
