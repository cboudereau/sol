// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! LogQL → SQL translation + Loki `query_range` response types (task 3).
//!
//! Covers the pcap subset (label matchers `=`/`!=`/`=~`/`!~`, line filters
//! `|=`/`!=`/`|~`/`!~`) per [QUERY-MAPPING.md](../../../docs/workspace/parquet-backend/QUERY-MAPPING.md).
//! Non-promoted labels use `prom_attr(resource_attributes, '<key>')` — the
//! query-side OTLP→Prometheus normalization, matching the Prometheus label name
//! against the raw OTLP key (`deployment_environment` → `deployment.environment`).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The only promoted (top-level column) log label; everything else lives in the
/// JSON `resource_attributes` column.
const PROMOTED_LABEL: &str = "service_name";

/// Unescape a double-quoted LogQL/PromQL/TraceQL string literal (Go-style
/// escapes). Grafana sends regex matchers with escaped backslashes
/// (`"1\\.0\\.0"` on the wire), which must collapse to the regex `1\.0\.0` to
/// match `1.0.0`. Recognized: `\\ \" \n \t \r`; any other `\x` is kept
/// verbatim (lenient — preserves regex metasequences like `\.`/`\d` when a
/// client single-escapes). Shared with the TraceQL parser ([`super::tempo`]).
pub(super) fn unescape_dquoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('"') => out.push('"'),
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// Map an OTLP `severity_number` to Loki's `detected_level` stream label
/// (the value Grafana uses to colour log lines). Ranges per the OTLP log
/// spec; out-of-range/unspecified (0, NULL) mirrors Loki's `"unknown"`.
fn detected_level(severity_number: i32) -> &'static str {
    match severity_number {
        1..=4 => "trace",
        5..=8 => "debug",
        9..=12 => "info",
        13..=16 => "warn",
        17..=20 => "error",
        21..=24 => "fatal",
        _ => "unknown",
    }
}

use super::logql::ast;

/// The first range aggregation reachable in a metric expression (for the volume
/// path, which needs the underlying stream selector).
fn first_range(expr: &ast::SampleExpr) -> Option<&ast::LogRange> {
    match expr {
        ast::SampleExpr::RangeAgg { range, .. } => Some(range),
        ast::SampleExpr::VectorAgg { inner, .. } => first_range(inner),
        ast::SampleExpr::Binary { lhs, rhs, .. } => first_range(lhs).or_else(|| first_range(rhs)),
        ast::SampleExpr::Number(_) => None,
    }
}

// --- `Expr`/DataFrame lowering (expr-lowering migration) ---

use datafusion::functions::expr_fn::coalesce;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::octet_length;
use datafusion::functions_aggregate::expr_fn::{count, count_distinct, sum};
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::when;
use datafusion::prelude::{DataFrame, Expr, col, lit};

/// `COALESCE(sum(octet_length(body)), 0)` — total log bytes, as an `Expr`.
fn bytes_sum() -> Expr {
    coalesce(vec![
        sum(octet_length().call(vec![col("body")])),
        lit(0_i64),
    ])
}

/// Cast the timestamp column to ns `i64` and bound it to `[start, end]`.
fn time_between(start_ns: i64, end_ns: i64) -> Expr {
    cast(
        col("time_unix_nano"),
        datafusion::arrow::datatypes::DataType::Int64,
    )
    .between(lit(start_ns), lit(end_ns))
}

/// The file-pruning scope of a `[start, end]`-windowed logs scan (FR1): every
/// Loki range path filters `time_unix_nano` to exactly this window, so
/// `engine.table_scoped("logs", …)` may skip files provably outside it.
fn log_scope(start_ns: i64, end_ns: i64) -> super::QueryScope {
    super::QueryScope {
        lo_ns: start_ns,
        hi_ns: end_ns,
    }
}

/// Label LHS as an `Expr`: promoted `service_name` column, else `prom_attr` on
/// the `resource_attributes` JSON column.
fn label_lhs_expr(name: &str) -> Expr {
    if name == PROMOTED_LABEL {
        col(PROMOTED_LABEL)
    } else {
        super::plan::predicate::prom_attr("resource_attributes", name)
    }
}

fn matchop_kind(op: &ast::MatchOp) -> super::plan::predicate::MatchKind {
    use super::plan::predicate::MatchKind;
    match op {
        ast::MatchOp::Eq => MatchKind::Eq,
        ast::MatchOp::Neq => MatchKind::Neq,
        ast::MatchOp::Re => MatchKind::Re,
        ast::MatchOp::Nre => MatchKind::Nre,
    }
}

/// Map a label-filter comparison op to `(MatchKind, numeric)`.
fn cmpop_kind(op: &ast::CmpOp) -> (super::plan::predicate::MatchKind, bool) {
    use super::plan::predicate::MatchKind;
    match op {
        ast::CmpOp::Eq => (MatchKind::Eq, false),
        ast::CmpOp::Neq => (MatchKind::Neq, false),
        ast::CmpOp::Re => (MatchKind::Re, false),
        ast::CmpOp::Nre => (MatchKind::Nre, false),
        ast::CmpOp::EqEq => (MatchKind::Eq, true),
        ast::CmpOp::Gt => (MatchKind::Gt, true),
        ast::CmpOp::Gte => (MatchKind::Gte, true),
        ast::CmpOp::Lt => (MatchKind::Lt, true),
        ast::CmpOp::Lte => (MatchKind::Lte, true),
    }
}

/// A line filter (`body`) as an `Expr`. Pattern filters (`|>`/`!>`) deferred.
fn line_pred_expr(op: &ast::LineOp, value: &str) -> Result<Expr, String> {
    let body = col("body");
    Ok(match op {
        ast::LineOp::Contains => body.like(lit(format!("%{value}%"))),
        ast::LineOp::NotContains => !body.like(lit(format!("%{value}%"))),
        ast::LineOp::Re => regexp_like(body, lit(value.to_string()), None),
        ast::LineOp::Nre => !regexp_like(body, lit(value.to_string()), None),
        ast::LineOp::Pattern | ast::LineOp::NotPattern => {
            return Err("pattern line filters (|>/!>) are not yet supported".to_string());
        }
    })
}

/// Combine `a` into the running filter with `AND` (or seed it).
fn and_opt(acc: Option<Expr>, e: Expr) -> Option<Expr> {
    Some(match acc {
        Some(a) => a.and(e),
        None => e,
    })
}

/// Build the WHERE filter `Expr` from a parsed log pipeline: selector matchers +
/// line filters + stored-label filters; parser/format stages are no-ops; a label
/// filter after an extraction stage errors — the dynamic-label non-goal.
fn pipeline_pred_expr(p: &ast::LogPipeline) -> Result<Option<Expr>, String> {
    use ast::Stage;
    let mut acc: Option<Expr> = None;
    for m in &p.selector.matchers {
        let e = super::plan::predicate::cmp(
            label_lhs_expr(&m.name),
            matchop_kind(&m.op),
            &m.value,
            false,
        )?;
        acc = and_opt(acc, e);
    }
    let mut extracted = false;
    for stage in &p.stages {
        match stage {
            Stage::Line { op, value } => {
                if !value.is_empty() {
                    acc = and_opt(acc, line_pred_expr(op, value)?);
                }
            }
            Stage::Json | Stage::Logfmt | Stage::Unpack | Stage::Regexp(_) | Stage::Pattern(_) => {
                extracted = true;
            }
            Stage::Decolorize
            | Stage::LineFormat(_)
            | Stage::LabelFormat(_)
            | Stage::Drop(_)
            | Stage::Keep(_)
            | Stage::Unwrap(_) => {}
            Stage::LabelFilter(lf) => {
                if extracted {
                    return Err(format!(
                        "label filter on a runtime-extracted label is not supported: {}",
                        lf.name
                    ));
                }
                let (kind, numeric) = cmpop_kind(&lf.op);
                acc = and_opt(
                    acc,
                    super::plan::predicate::cmp(
                        label_lhs_expr(&lf.name),
                        kind,
                        &lf.value,
                        numeric,
                    )?,
                );
            }
        }
    }
    Ok(acc)
}

/// Build the LogQL streams query as a `DataFrame` (P3). Columns match the order
/// [`handle_query_range`] reads: service_name, time_unix_nano, body, severity_number.
pub async fn build_streams(
    engine: &super::QueryEngine,
    logql: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
    forward: bool,
) -> crate::Result<DataFrame> {
    let pipeline = super::logql::parse_pipeline(logql).map_err(|e| to_err(e.to_string()))?;
    let pred = pipeline_pred_expr(&pipeline).map_err(to_err)?;
    let time = cast(
        col("time_unix_nano"),
        datafusion::arrow::datatypes::DataType::Int64,
    )
    .between(lit(start_ns), lit(end_ns));
    let mut df = engine
        .table_scoped("logs", log_scope(start_ns, end_ns))
        .await?
        .filter(time)?;
    if let Some(p) = pred {
        df = df.filter(p)?;
    }
    Ok(df
        .select(vec![
            col("service_name"),
            col("time_unix_nano"),
            col("body"),
            col("severity_number"),
        ])?
        .sort(vec![col("time_unix_nano").sort(forward, false)])?
        .limit(0, Some(limit as usize))?)
}

/// `detected_level` CASE over the OTLP severity ranges, as an `Expr`.
fn detected_level_expr() -> Expr {
    let sev = || col("severity_number");
    when(sev().between(lit(1), lit(4)), lit("trace"))
        .when(sev().between(lit(5), lit(8)), lit("debug"))
        .when(sev().between(lit(9), lit(12)), lit("info"))
        .when(sev().between(lit(13), lit(16)), lit("warn"))
        .when(sev().between(lit(17), lit(20)), lit("error"))
        .when(sev().between(lit(21), lit(24)), lit("fatal"))
        .otherwise(lit("unknown"))
        .expect("CASE with otherwise is total")
}

/// Build the log-volume query as a `DataFrame` (P8): count per
/// `(detected_level, step-bucket)` over the metric query's underlying selector.
/// Columns match [`handle_volume`]: `lvl`, `bkt`, `c`.
pub async fn build_volume(
    engine: &super::QueryEngine,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
) -> crate::Result<DataFrame> {
    let expr = super::logql::parse(query).map_err(|e| to_err(e.to_string()))?;
    let range = match &expr {
        ast::LogQlExpr::Sample(s) => first_range(s).ok_or_else(|| {
            to_err("log volume query must contain a range aggregation".to_string())
        })?,
        ast::LogQlExpr::Log(_) => {
            return Err(to_err(
                "log volume query must be a metric query".to_string(),
            ));
        }
    };
    let pred = pipeline_pred_expr(&range.pipeline).map_err(to_err)?;
    let step = step_ns.max(1);
    let time = cast(
        col("time_unix_nano"),
        datafusion::arrow::datatypes::DataType::Int64,
    )
    .between(lit(start_ns), lit(end_ns));
    let mut df = engine
        .table_scoped("logs", log_scope(start_ns, end_ns))
        .await?
        .filter(time)?;
    if let Some(p) = pred {
        df = df.filter(p)?;
    }
    let bucket = (cast(
        col("time_unix_nano"),
        datafusion::arrow::datatypes::DataType::Int64,
    ) / lit(step))
        * lit(step);
    Ok(df
        .aggregate(
            vec![detected_level_expr().alias("lvl"), bucket.alias("bkt")],
            vec![count(lit(1_i64)).alias("c")],
        )?
        .sort(vec![col("bkt").sort(true, false)])?)
}

/// Filter `Expr` from a bare `{selector}` matcher string (series/index endpoints).
fn selector_pred_expr(query: &str) -> Result<Option<Expr>, String> {
    let q = query.trim();
    if q.is_empty() || q == "{}" {
        return Ok(None);
    }
    let p = super::logql::parse_pipeline(q).map_err(|e| e.to_string())?;
    pipeline_pred_expr(&p)
}

/// Build the Loki `series` query as a `DataFrame` (P4): distinct
/// `(service_name, resource_attributes)` matching `matcher`.
pub async fn build_series(
    engine: &super::QueryEngine,
    matcher: Option<&str>,
    start_ns: i64,
    end_ns: i64,
) -> crate::Result<DataFrame> {
    let pred = selector_pred_expr(matcher.unwrap_or("{}")).map_err(to_err)?;
    let time = cast(
        col("time_unix_nano"),
        datafusion::arrow::datatypes::DataType::Int64,
    )
    .between(lit(start_ns), lit(end_ns));
    let mut df = engine
        .table_scoped("logs", log_scope(start_ns, end_ns))
        .await?
        .filter(time)?;
    if let Some(p) = pred {
        df = df.filter(p)?;
    }
    Ok(df
        .select(vec![col("service_name"), col("resource_attributes")])?
        .distinct()?)
}

/// Run Loki `labels` (label-name discovery for Grafana's log browser): the
/// promoted column plus the normalized resource-attribute keys.
pub async fn handle_labels(engine: &super::QueryEngine) -> crate::Result<serde_json::Value> {
    let keys = super::prometheus::distinct_json_keys(engine, "logs", "resource_attributes").await?;
    let mut names: std::collections::BTreeSet<String> = [PROMOTED_LABEL.to_string()].into();
    names.extend(keys.into_iter().map(|k| super::udf::normalize(&k)));
    let names: Vec<String> = names.into_iter().collect();
    Ok(serde_json::json!({ "status": "success", "data": names }))
}

/// Run Loki `label/:name/values` and build `{status, data:[...]}`.
/// Build the Loki `label/:name/values` query as a `DataFrame` (P4): distinct
/// non-null values of `label`.
pub async fn build_label_values(
    engine: &super::QueryEngine,
    label: &str,
) -> crate::Result<DataFrame> {
    let lhs = label_lhs_expr(label);
    Ok(engine
        .table("logs")
        .await?
        .filter(lhs.clone().is_not_null())?
        .select(vec![lhs.alias("v")])?
        .distinct()?
        .sort(vec![col("v").sort(true, false)])?)
}

/// Run Loki `label/:name/values` and build `{status, data:[...]}`.
pub async fn handle_label_values(
    engine: &super::QueryEngine,
    label: &str,
) -> crate::Result<serde_json::Value> {
    let df = build_label_values(engine, label).await?;
    // Unbounded discovery scan — no window to classify (short cache TTL).
    let values = super::prometheus::string_column_df(engine, df, None).await?;
    Ok(serde_json::json!({ "status": "success", "data": values }))
}

/// Whether a LogQL query is a **metric** query (volume / aggregation) rather
/// than a plain `{...}` log-stream selector. Grafana's "Logs volume" panel
/// issues `sum by (level) (count_over_time({sel}[range]))`, which must produce
/// a Prometheus-style matrix, not a `streams` result.
pub fn is_metric_query(logql: &str) -> bool {
    !logql.trim_start().starts_with('{')
}

/// Run a log-volume metric query and build a Prometheus-style `matrix` response
/// (one series per `detected_level`). This is what Grafana's "Logs volume"
/// panel consumes; a plain log query goes through [`handle_query_range`].
pub async fn handle_volume(
    engine: &super::QueryEngine,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
) -> crate::Result<serde_json::Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Int64Type;

    let df = build_volume(engine, query, start_ns, end_ns, step_ns).await?;
    let batches = engine
        .collect_scoped(df, Some(log_scope(start_ns, end_ns)))
        .await?;

    let mut series: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
    for batch in &batches {
        let lvl = batch.column(0).as_string::<i32>();
        let bkt = batch.column(1).as_primitive::<Int64Type>();
        let c = batch.column(2).as_primitive::<Int64Type>();
        for i in 0..batch.num_rows() {
            let level = if lvl.is_null(i) {
                "unknown".to_string()
            } else {
                lvl.value(i).to_string()
            };
            #[allow(clippy::cast_precision_loss)] // ns→s for the matrix timestamp
            let ts = bkt.value(i) as f64 / 1e9;
            series
                .entry(level)
                .or_default()
                .push(serde_json::json!([ts, c.value(i).to_string()]));
        }
    }
    let result: Vec<serde_json::Value> = series
        .into_iter()
        .map(|(lvl, values)| serde_json::json!({ "metric": { "detected_level": lvl }, "values": values }))
        .collect();
    Ok(serde_json::json!({
        "status": "success",
        "data": { "resultType": "matrix", "result": result }
    }))
}

fn to_err(e: String) -> Box<dyn std::error::Error + Send + Sync> {
    Box::<dyn std::error::Error + Send + Sync>::from(e)
}

/// Explode a `resource_attributes` JSON blob into normalized labels, merged into
/// `m` (existing keys win).
fn merge_attrs(m: &mut BTreeMap<String, String>, json: &str) {
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(json) {
        for (k, v) in map {
            let val = match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            m.entry(super::udf::normalize(&k)).or_insert(val);
        }
    }
}

/// Loki `series` (C-L2): the distinct stream label sets — `service_name` plus
/// the exploded, normalized resource attributes — matching an optional `match[]`
/// selector. Grafana uses this for the label/series browser.
pub async fn handle_series(
    engine: &super::QueryEngine,
    matcher: Option<&str>,
    start_ns: i64,
    end_ns: i64,
) -> crate::Result<serde_json::Value> {
    use datafusion::arrow::array::{Array, AsArray};

    let df = build_series(engine, matcher, start_ns, end_ns).await?;
    let batches = engine
        .collect_scoped(df, Some(log_scope(start_ns, end_ns)))
        .await?;
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut data: Vec<BTreeMap<String, String>> = Vec::new();
    for batch in &batches {
        let svc = batch.column(0).as_string::<i32>();
        let ra = batch.column(1).as_string::<i32>();
        for i in 0..batch.num_rows() {
            let mut m = BTreeMap::new();
            if !svc.is_null(i) {
                m.insert("service_name".to_string(), svc.value(i).to_string());
            }
            if !ra.is_null(i) {
                merge_attrs(&mut m, ra.value(i));
            }
            if seen.insert(format!("{m:?}")) {
                data.push(m);
            }
        }
    }
    Ok(serde_json::json!({ "status": "success", "data": data }))
}

/// Loki `index/stats` (C-L3): a flat query-size hint (NOT `{status,data}`-wrapped)
/// — `streams`/`chunks`/`bytes`/`entries` over the matched range. Sol has no chunk
/// concept, so `chunks` is 0.
pub async fn handle_index_stats(
    engine: &super::QueryEngine,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> crate::Result<serde_json::Value> {
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::Int64Type;

    let pred = selector_pred_expr(query).map_err(to_err)?;
    let mut df = engine
        .table_scoped("logs", log_scope(start_ns, end_ns))
        .await?
        .filter(time_between(start_ns, end_ns))?;
    if let Some(p) = pred {
        df = df.filter(p)?;
    }
    let df = df.aggregate(
        vec![],
        vec![
            count_distinct(col("service_name")).alias("streams"),
            count(lit(1_i64)).alias("entries"),
            bytes_sum().alias("bytes"),
        ],
    )?;
    let batches = engine
        .collect_scoped(df, Some(log_scope(start_ns, end_ns)))
        .await?;
    let (mut streams, mut entries, mut bytes) = (0i64, 0i64, 0i64);
    if let Some(batch) = batches.iter().find(|b| b.num_rows() > 0) {
        streams = batch.column(0).as_primitive::<Int64Type>().value(0);
        entries = batch.column(1).as_primitive::<Int64Type>().value(0);
        bytes = batch.column(2).as_primitive::<Int64Type>().value(0);
    }
    Ok(serde_json::json!({
        "streams": streams, "chunks": 0, "bytes": bytes, "entries": entries
    }))
}

/// Loki `index/volume[_range]` (C-L1): byte volume per `service_name`. `range`
/// emits a Prometheus `matrix` bucketed by `step_ns`; otherwise a `vector` with
/// the total at `end_ns`. (Newer Grafana Loki datasources call this for the log
/// volume panel; the demo's version uses `query_range`, already handled.)
pub async fn handle_index_volume(
    engine: &super::QueryEngine,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    range: bool,
) -> crate::Result<serde_json::Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Int64Type;

    let pred = selector_pred_expr(query).map_err(to_err)?;
    #[allow(clippy::cast_precision_loss)] // ns→s for the matrix/vector timestamp
    let end_s = end_ns as f64 / 1e9;
    let base = {
        let mut df = engine
            .table_scoped("logs", log_scope(start_ns, end_ns))
            .await?
            .filter(time_between(start_ns, end_ns))?;
        if let Some(p) = &pred {
            df = df.filter(p.clone())?;
        }
        df
    };
    if range {
        let step = step_ns.max(1);
        let bucket = (cast(
            col("time_unix_nano"),
            datafusion::arrow::datatypes::DataType::Int64,
        ) / lit(step))
            * lit(step);
        let df = base
            .aggregate(
                vec![col("service_name").alias("svc"), bucket.alias("bkt")],
                vec![bytes_sum().alias("b")],
            )?
            .sort(vec![col("bkt").sort(true, false)])?;
        let batches = engine
            .collect_scoped(df, Some(log_scope(start_ns, end_ns)))
            .await?;
        let mut series: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
        for batch in &batches {
            let svc = batch.column(0).as_string::<i32>();
            let bkt = batch.column(1).as_primitive::<Int64Type>();
            let b = batch.column(2).as_primitive::<Int64Type>();
            for i in 0..batch.num_rows() {
                let s = if svc.is_null(i) {
                    String::new()
                } else {
                    svc.value(i).to_string()
                };
                #[allow(clippy::cast_precision_loss)]
                let ts = bkt.value(i) as f64 / 1e9;
                series
                    .entry(s)
                    .or_default()
                    .push(serde_json::json!([ts, b.value(i).to_string()]));
            }
        }
        let result: Vec<serde_json::Value> = series
            .into_iter()
            .map(|(s, values)| serde_json::json!({ "metric": { "service_name": s }, "values": values }))
            .collect();
        return Ok(serde_json::json!({
            "status": "success",
            "data": { "resultType": "matrix", "result": result }
        }));
    }
    let df = base.aggregate(
        vec![col("service_name").alias("svc")],
        vec![bytes_sum().alias("b")],
    )?;
    let batches = engine
        .collect_scoped(df, Some(log_scope(start_ns, end_ns)))
        .await?;
    let mut result: Vec<serde_json::Value> = Vec::new();
    for batch in &batches {
        let svc = batch.column(0).as_string::<i32>();
        let b = batch.column(1).as_primitive::<Int64Type>();
        for i in 0..batch.num_rows() {
            let s = if svc.is_null(i) {
                String::new()
            } else {
                svc.value(i).to_string()
            };
            result.push(serde_json::json!({
                "metric": { "service_name": s },
                "value": [end_s, b.value(i).to_string()]
            }));
        }
    }
    Ok(serde_json::json!({
        "status": "success",
        "data": { "resultType": "vector", "result": result }
    }))
}

// --- Loki query_range response (resultType=streams) ---

/// Loki `query_range` response envelope.
#[derive(Debug, Serialize, Deserialize)]
pub struct LokiResponse {
    /// Always `"success"` on a 200.
    pub status: String,
    /// Result payload.
    pub data: LokiData,
}

/// Loki response data block.
#[derive(Debug, Serialize, Deserialize)]
pub struct LokiData {
    /// `"streams"` for log queries.
    #[serde(rename = "resultType")]
    pub result_type: String,
    /// One entry per stream (label set).
    pub result: Vec<LokiStream>,
}

/// One log stream: a label set plus its `[ns, line]` value pairs.
#[derive(Debug, Serialize, Deserialize)]
pub struct LokiStream {
    /// Stream labels.
    pub stream: BTreeMap<String, String>,
    /// `[nanosecond-timestamp-string, log-line]` pairs.
    pub values: Vec<[String; 2]>,
}

impl LokiResponse {
    /// Build a `streams` response from `(service_name, detected_level, ts_ns, body)`
    /// rows. Streams are keyed by (service, level) — `detected_level` is the
    /// label Loki itself attaches for Grafana's log-level colouring.
    pub fn streams(rows: impl IntoIterator<Item = (String, &'static str, i64, String)>) -> Self {
        let mut by_stream: BTreeMap<(String, &'static str), Vec<[String; 2]>> = BTreeMap::new();
        for (service_name, level, ts, body) in rows {
            by_stream
                .entry((service_name, level))
                .or_default()
                .push([ts.to_string(), body]);
        }
        let result = by_stream
            .into_iter()
            .map(|((service_name, level), values)| {
                let mut stream = BTreeMap::new();
                stream.insert("service_name".to_string(), service_name);
                stream.insert("detected_level".to_string(), level.to_string());
                LokiStream { stream, values }
            })
            .collect();
        LokiResponse {
            status: "success".to_string(),
            data: LokiData {
                result_type: "streams".to_string(),
                result,
            },
        }
    }
}

/// Run a LogQL `query_range` against the engine and build a Loki streams response.
pub async fn handle_query_range(
    engine: &super::QueryEngine,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
    forward: bool,
) -> crate::Result<LokiResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::{Int32Type, TimestampNanosecondType};

    let df = build_streams(engine, query, start_ns, end_ns, limit, forward).await?;
    let batches = engine
        .collect_scoped(df, Some(log_scope(start_ns, end_ns)))
        .await?;

    let mut rows: Vec<(String, &'static str, i64, String)> = Vec::new();
    for batch in &batches {
        let svc = batch.column(0).as_string::<i32>();
        let ts = batch.column(1).as_primitive::<TimestampNanosecondType>();
        let body = batch.column(2).as_string::<i32>();
        let sev = batch.column(3).as_primitive::<Int32Type>();
        for i in 0..batch.num_rows() {
            let service = if svc.is_null(i) {
                String::new()
            } else {
                svc.value(i).to_string()
            };
            let nanos = if ts.is_null(i) { 0 } else { ts.value(i) };
            let line = if body.is_null(i) {
                String::new()
            } else {
                body.value(i).to_string()
            };
            let level = detected_level(if sev.is_null(i) { 0 } else { sev.value(i) });
            rows.push((service, level, nanos, line));
        }
    }
    Ok(LokiResponse::streams(rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_selector_value_is_bound_literal_not_injected() {
        // A LogQL stream-selector value with SQL metacharacters must bind as a
        // single literal, leaving the predicate a plain equality.
        let evil = r#"a' OR '1'='1 && x"#;
        let q = format!("{{app=\"{evil}\"}}");
        let e = selector_pred_expr(&q).unwrap().expect("selector lowers");
        let s = format!("{e}");
        assert!(
            s.contains(&format!("Utf8({evil:?})")),
            "value bound as one literal, not injected: {s}"
        );
    }

    #[test]
    fn test_is_metric_query_detects_volume() {
        assert!(is_metric_query(
            r#"sum by (detected_level) (count_over_time({service_name="client"}[1m]))"#
        ));
        assert!(!is_metric_query(r#"{service_name="client"} |= "x""#));
    }

    #[test]
    fn test_loki_query_range_response_shape() {
        let resp = LokiResponse::streams([
            (
                "client".to_string(),
                "info",
                1700000000000000000,
                "hello".to_string(),
            ),
            (
                "client".to_string(),
                "info",
                1700000000000000001,
                "world".to_string(),
            ),
        ]);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""resultType":"streams""#), "json: {json}");
        assert!(
            json.contains(r#""stream":{"detected_level":"info","service_name":"client"}"#),
            "json: {json}"
        );
        assert!(
            json.contains(r#"["1700000000000000000","hello"]"#),
            "json: {json}"
        );
    }

    #[test]
    fn test_detected_level_otlp_severity_ranges() {
        // OTLP severity_number ranges -> Loki detected_level values (what the
        // real Loki attaches for Grafana's log-level colouring).
        assert_eq!(detected_level(1), "trace");
        assert_eq!(detected_level(5), "debug");
        assert_eq!(detected_level(9), "info"); // .NET "Information"
        assert_eq!(detected_level(13), "warn"); // .NET "Warning"
        assert_eq!(detected_level(17), "error"); // .NET "Error"
        assert_eq!(detected_level(21), "fatal");
        assert_eq!(detected_level(0), "unknown");
        assert_eq!(detected_level(99), "unknown");
    }

    #[test]
    fn test_streams_group_by_service_and_level() {
        let resp = LokiResponse::streams([
            ("client".to_string(), "info", 1, "a".to_string()),
            ("client".to_string(), "error", 2, "b".to_string()),
            ("client".to_string(), "info", 3, "c".to_string()),
        ]);
        assert_eq!(resp.data.result.len(), 2, "one stream per (service, level)");
        let err = resp
            .data
            .result
            .iter()
            .find(|s| s.stream["detected_level"] == "error")
            .expect("error stream");
        assert_eq!(err.values.len(), 1);
        assert_eq!(err.values[0][1], "b");
    }

    #[tokio::test]
    async fn test_loki_handle_query_range_end_to_end() {
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("logs").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["client", "client", "other"])),
                Arc::new(TimestampNanosecondArray::from(vec![10i64, 20, 30]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec!["hello world", "bye", "hello again"])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("f.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let opts = QuerierOptions {
            storage: StorageConfig {
                path: tmp.path().into(),
                url: None,
            },
            ..QuerierOptions::default()
        };
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();

        let resp = handle_query_range(
            &engine,
            r#"{service_name="client"} |= "hello""#,
            0,
            1000,
            100,
            false,
        )
        .await
        .unwrap();
        assert_eq!(resp.data.result_type, "streams");
        assert_eq!(resp.data.result.len(), 1, "one stream (client)");
        let s = &resp.data.result[0];
        assert_eq!(s.stream["service_name"], "client");
        // Fixture has no severity_number column -> NULL -> "unknown" (Loki parity).
        assert_eq!(s.stream["detected_level"], "unknown");
        assert_eq!(s.values.len(), 1, "only 'hello world' matches");
        assert_eq!(s.values[0][1], "hello world");
    }

    #[tokio::test]
    async fn test_loki_labels_end_to_end() {
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("logs").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["client"])),
                Arc::new(TimestampNanosecondArray::from(vec![10i64]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(StringArray::from(vec![Some(
                    r#"{"deployment.environment":"dev","service.version":"1.0.0"}"#,
                )])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("f.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let opts = QuerierOptions {
            storage: StorageConfig {
                path: tmp.path().into(),
                url: None,
            },
            ..QuerierOptions::default()
        };
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();

        // Label names: promoted column + normalized resource-attribute keys.
        let labels = handle_labels(&engine).await.unwrap();
        let names: Vec<&str> = labels["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap())
            .collect();
        for expected in ["service_name", "deployment_environment", "service_version"] {
            assert!(names.contains(&expected), "missing {expected}: {names:?}");
        }

        // Label values resolve through the normalized attribute lookup.
        let values = handle_label_values(&engine, "deployment_environment")
            .await
            .unwrap();
        assert_eq!(
            values["data"].as_array().unwrap(),
            &[serde_json::json!("dev")]
        );
    }

    async fn logs_engine_with_attrs() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp.path().join("logs").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["client", "client"])),
                Arc::new(TimestampNanosecondArray::from(vec![10i64, 20]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec!["hello", "world"])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"deployment.environment":"dev"}"#),
                    Some(r#"{"deployment.environment":"dev"}"#),
                ])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("f.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let opts = QuerierOptions {
            storage: StorageConfig {
                path: tmp.path().into(),
                url: None,
            },
            ..QuerierOptions::default()
        };
        crate::querier::QueryEngine::new(&opts).await.unwrap()
    }

    #[tokio::test]
    async fn test_loki_series_returns_label_sets() {
        let engine = logs_engine_with_attrs().await;
        let resp = handle_series(&engine, Some(r#"{service_name="client"}"#), 0, 1000)
            .await
            .unwrap();
        let data = resp["data"].as_array().unwrap();
        assert_eq!(data.len(), 1, "one distinct stream: {data:?}");
        assert_eq!(data[0]["service_name"], "client");
        // resource attributes exploded + normalized
        assert_eq!(data[0]["deployment_environment"], "dev");
    }

    #[tokio::test]
    async fn test_loki_index_stats_flat_shape() {
        let engine = logs_engine_with_attrs().await;
        let stats = handle_index_stats(&engine, "{}", 0, 1000).await.unwrap();
        // flat object, NOT {status,data}-wrapped (Loki's contract).
        assert!(stats.get("status").is_none(), "must be flat: {stats}");
        assert_eq!(stats["entries"], 2);
        assert_eq!(stats["streams"], 1);
        assert_eq!(stats["chunks"], 0);
        assert_eq!(stats["bytes"], 10, "octet_length('hello')+('world') = 10");
    }

    #[tokio::test]
    async fn test_loki_index_volume_vector() {
        let engine = logs_engine_with_attrs().await;
        let vol = handle_index_volume(&engine, "{}", 0, 1000, 60_000_000_000, false)
            .await
            .unwrap();
        assert_eq!(vol["data"]["resultType"], "vector");
        let result = vol["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["metric"]["service_name"], "client");
        assert_eq!(result[0]["value"][1], "10");
    }

    #[test]
    fn test_loki_response_deserializes() {
        let resp = LokiResponse::streams([("svc".to_string(), "info", 1, "x".to_string())]);
        let json = serde_json::to_string(&resp).unwrap();
        let back: LokiResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.status, "success");
        assert_eq!(back.data.result_type, "streams");
        assert_eq!(
            back.data.result[0].values[0],
            ["1".to_string(), "x".to_string()]
        );
    }
}
