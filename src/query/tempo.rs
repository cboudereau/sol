// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! TraceQL → SQL translation + Tempo HTTP API response types (task 7).
//!
//! Covers the pcap subset of TraceQL (`{a=b && c!=d}` with `=`/`!=`) per
//! [QUERY-MAPPING.md](../../../docs/workspace/parquet-backend/QUERY-MAPPING.md)
//! and the Tempo response shapes in
//! [API-SPEC.md](../../../docs/workspace/parquet-backend/API-SPEC.md) §3. No
//! structural / span-set operators (rabbit hole 2). Spans live in the `traces`
//! table; `trace_id`/`span_id` are `FixedSizeBinary`, attributes are JSON.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Box a `String` message into the crate error type.
fn to_err(e: String) -> Box<dyn std::error::Error + Send + Sync> {
    Box::<dyn std::error::Error + Send + Sync>::from(e)
}

use super::traceql::ast as tast;

/// Format an `f64` literal without a trailing `.0` for whole numbers.
fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 9e15 {
        #[allow(clippy::cast_possible_truncation)]
        let i = n as i64;
        i.to_string()
    } else {
        n.to_string()
    }
}

/// Parse a duration literal (e.g. `1.5s`, `200ms`) to nanoseconds.
fn duration_nanos(raw: &str) -> Option<i64> {
    // Single shared duration parser (FR7) — also handles fractional/compound forms.
    super::units::parse_duration_ns(raw).map(|d| d.ns())
}

// --- `Expr`/DataFrame lowering (expr-lowering migration) ---

use datafusion::logical_expr::expr_fn::cast;
use datafusion::prelude::{DataFrame, Expr, col, lit};

/// JSON attribute LHS as an `Expr` (`json_get_str(column, key)`).
fn json_attr(column: &str, key: &str) -> Expr {
    datafusion_functions_json::udfs::json_get_str_udf().call(vec![col(column), lit(key)])
}

/// LHS `Expr` for a raw TraceQL tag string: promoted intrinsic columns, or JSON
/// extraction for `span.*`/`resource.*`/bare `.attr`.
fn tag_lhs_expr(tag: &str) -> Expr {
    match tag {
        "name" => col("name"),
        "status" | "status.code" => col("status_code"),
        "kind" => col("kind"),
        "duration" => col("duration_nanos"),
        "resource.service.name" | "service.name" | ".service.name" => col("service_name"),
        _ => {
            if let Some(a) = tag.strip_prefix("resource.") {
                json_attr("resource_attributes", a)
            } else if let Some(a) = tag.strip_prefix("span.") {
                json_attr("attributes", a)
            } else {
                json_attr("attributes", tag.strip_prefix('.').unwrap_or(tag))
            }
        }
    }
}

/// Build the Tempo trace-by-id query as a `DataFrame` (P3 + P9): the spans of one
/// trace, ids base64-encoded. Columns match [`handle_trace_by_id`]'s reads.
pub async fn build_trace_by_id(
    engine: &super::QueryEngine,
    trace_id_hex: &str,
) -> crate::Result<DataFrame> {
    let mut hex = trace_id_hex.trim().to_lowercase();
    if hex.is_empty() || hex.len() > 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(to_err(
            "trace id must be a hex string of at most 32 chars".to_string(),
        ));
    }
    if hex.len() < 32 {
        hex = format!("{hex:0>32}"); // zero-pad to the full 16-byte id
    }
    let bytes: Vec<u8> = (0..16)
        .map(|i| u8::from_str_radix(&hex[2 * i..2 * i + 2], 16))
        .collect::<Result<_, _>>()
        .map_err(|e: std::num::ParseIntError| to_err(e.to_string()))?;
    let id = Expr::Literal(
        datafusion::scalar::ScalarValue::FixedSizeBinary(16, Some(bytes)),
        None,
    );
    let b64 = |c: &str| super::plan::ids::encode_as(col(c), "base64");
    Ok(engine
        .table("traces")
        .await?
        .filter(col("trace_id").eq(id))?
        .select(vec![
            b64("trace_id").alias("trace_b64"),
            b64("span_id").alias("span_b64"),
            col("service_name"),
            col("name"),
            col("start_time_unix_nano"),
            col("duration_nanos"),
            col("status_code"),
            col("attributes"),
            col("resource_attributes"),
            b64("parent_span_id").alias("parent_b64"),
            col("kind"),
            col("scope_name"),
        ])?
        .sort(vec![col("start_time_unix_nano").sort(true, false)])?)
}

/// Build the Tempo `tag/:tag/values` query as a `DataFrame` (P4): distinct
/// stringified values of the tag.
pub async fn build_tag_values(engine: &super::QueryEngine, tag: &str) -> crate::Result<DataFrame> {
    let lhs = tag_lhs_expr(tag);
    Ok(engine
        .table("traces")
        .await?
        .filter(lhs.clone().is_not_null())?
        .select(vec![
            cast(lhs, datafusion::arrow::datatypes::DataType::Utf8).alias("v"),
        ])?
        .distinct()?
        .sort(vec![col("v").sort(true, false)])?)
}

/// Resolve a TraceQL field to its `Expr` LHS (promoted column or JSON extraction).
/// `event`/`instrumentation`/`link`/`parent` scopes are parsed but not lowered.
fn field_lhs_expr(f: &tast::Field) -> Result<Expr, String> {
    use tast::{AttrScope, Field};
    Ok(match f {
        Field::Intrinsic(s) => match s.as_str() {
            "name" => col("name"),
            "status" | "status.code" => col("status_code"),
            "kind" => col("kind"),
            "duration" => col("duration_nanos"),
            other => {
                return Err(format!(
                    "intrinsic '{other}' is not yet supported in lowering"
                ));
            }
        },
        Field::Attr { scope, path } => match scope {
            AttrScope::Resource | AttrScope::Unscoped if path == "service.name" => {
                col("service_name")
            }
            AttrScope::Span | AttrScope::Unscoped => json_attr("attributes", path),
            AttrScope::Resource => json_attr("resource_attributes", path),
            other => return Err(format!("{other:?} scope is not yet supported in lowering")),
        },
        _ => return Err("expected a field reference on the left of a comparison".to_string()),
    })
}

fn field_matchkind(op: &tast::FieldOp) -> super::plan::predicate::MatchKind {
    use super::plan::predicate::MatchKind;
    use tast::FieldOp;
    match op {
        FieldOp::Eq => MatchKind::Eq,
        FieldOp::Neq => MatchKind::Neq,
        FieldOp::Re => MatchKind::Re,
        FieldOp::Nre => MatchKind::Nre,
        FieldOp::Gt => MatchKind::Gt,
        FieldOp::Gte => MatchKind::Gte,
        FieldOp::Lt => MatchKind::Lt,
        FieldOp::Lte => MatchKind::Lte,
    }
}

/// The RHS literal text + whether the comparison is numeric (duration → nanos).
fn field_rhs(lhs: &tast::Field, rhs: &tast::Field) -> Result<(String, bool), String> {
    use tast::Field;
    let numeric_lhs = matches!(
        lhs,
        Field::Intrinsic(s) if s == "duration" || s == "status" || s == "status.code" || s == "kind"
    );
    match rhs {
        Field::Str(s) => Ok((s.clone(), false)),
        Field::Bool(b) => Ok((b.to_string(), false)),
        Field::Num(n) => Ok((fmt_num(*n), numeric_lhs)),
        Field::Duration(d) => {
            let nanos = duration_nanos(d).ok_or_else(|| format!("invalid duration: {d}"))?;
            Ok((nanos.to_string(), true))
        }
        _ => Err("expected a literal on the right of a comparison".to_string()),
    }
}

/// Lower a TraceQL field expression to a boolean filter `Expr`.
fn field_pred(fe: &tast::FieldExpr) -> Result<Expr, String> {
    use tast::FieldExpr;
    match fe {
        FieldExpr::Cmp { lhs, op, rhs } => {
            let l = field_lhs_expr(lhs)?;
            let (value, numeric) = field_rhs(lhs, rhs)?;
            super::plan::predicate::cmp(l, field_matchkind(op), &value, numeric)
        }
        FieldExpr::And(a, b) => Ok(field_pred(a)?.and(field_pred(b)?)),
        FieldExpr::Or(a, b) => Ok(field_pred(a)?.or(field_pred(b)?)),
        FieldExpr::Field(f) => Ok(field_lhs_expr(f)?.is_not_null()),
    }
}

/// Build the TraceQL search query as a `DataFrame` (P3 + P9). Columns match the
/// order [`handle_search`] reads (trace_hex, span_hex, service_name, name, start,
/// duration, parent, attributes, status_code).
pub async fn build_search(
    engine: &super::QueryEngine,
    traceql: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
) -> crate::Result<DataFrame> {
    let expr = super::traceql::parse(traceql).map_err(|e| to_err(e.to_string()))?;
    let pred = match &expr {
        tast::SpansetExpr::Filter(None) => None,
        tast::SpansetExpr::Filter(Some(fe)) => Some(field_pred(fe).map_err(to_err)?),
        tast::SpansetExpr::Op { .. } => {
            return Err(to_err(
                "spanset operators (&& || >> <<) between sets are not yet supported in search"
                    .to_string(),
            ));
        }
    };
    let time = cast(
        col("start_time_unix_nano"),
        datafusion::arrow::datatypes::DataType::Int64,
    )
    .between(lit(start_ns), lit(end_ns));
    let mut df = engine.table("traces").await?.filter(time)?;
    if let Some(p) = pred {
        df = df.filter(p)?;
    }
    let df = df
        .select(vec![
            super::plan::ids::encode_as(col("trace_id"), "hex").alias("trace_hex"),
            super::plan::ids::encode_as(col("span_id"), "hex").alias("span_hex"),
            col("service_name"),
            col("name"),
            col("start_time_unix_nano"),
            col("duration_nanos"),
            col("parent_span_id"),
            col("attributes"),
            col("status_code"),
            col("resource_attributes"),
        ])?
        .sort(vec![col("start_time_unix_nano").sort(false, false)])?
        .limit(0, Some((limit as usize).saturating_mul(64)))?;
    Ok(df)
}

/// OTLP `SpanKind` enum string for a stored kind int (proto-JSON form Grafana
/// decodes for the trace waterfall).
fn span_kind_str(kind: i32) -> &'static str {
    match kind {
        1 => "SPAN_KIND_INTERNAL",
        2 => "SPAN_KIND_SERVER",
        3 => "SPAN_KIND_CLIENT",
        4 => "SPAN_KIND_PRODUCER",
        5 => "SPAN_KIND_CONSUMER",
        _ => "SPAN_KIND_UNSPECIFIED",
    }
}

/// OTLP `StatusCode` enum string for a stored status_code int.
fn status_code_str(code: i32) -> &'static str {
    match code {
        1 => "STATUS_CODE_OK",
        2 => "STATUS_CODE_ERROR",
        _ => "STATUS_CODE_UNSET",
    }
}

/// Convert a span's stored `attributes` JSON object to the OTLP KeyValue array
/// Tempo/Grafana expect: `[{"key":"http.method","value":{"stringValue":"GET"}}]`.
/// Keys stay raw OTLP (TraceQL uses dotted names, unlike the Prometheus surface).
fn otlp_attributes(json: &str) -> Vec<Value> {
    let Ok(map) = serde_json::from_str::<serde_json::Map<String, Value>>(json) else {
        return Vec::new();
    };
    map.into_iter()
        .map(|(key, v)| json!({ "key": key, "value": json_to_otlp_value(&v) }))
        .collect()
}

/// A stored JSON attribute value → its OTLP `AnyValue` JSON form.
fn json_to_otlp_value(v: &Value) -> Value {
    match v {
        Value::String(s) => json!({ "stringValue": s }),
        Value::Bool(b) => json!({ "boolValue": b }),
        Value::Number(n) if n.is_i64() => {
            json!({ "intValue": n.as_i64().unwrap_or(0).to_string() })
        }
        Value::Number(n) => json!({ "doubleValue": n.as_f64().unwrap_or(0.0) }),
        other => json!({ "stringValue": other.to_string() }),
    }
}

/// Where a search-result attribute's value is sourced from.
enum AttrSource {
    /// The promoted `service_name` column (TraceQL `service.name`).
    Service,
    /// A key in the span `attributes` JSON (`span.`/unscoped).
    Span(String),
    /// A key in the `resource_attributes` JSON (`resource.`).
    Resource(String),
}

/// An attribute the query referenced — surfaced (uniformly) on every result
/// span. Tempo only returns the attributes the TraceQL touched; returning *all*
/// span attributes (with per-span-varying key sets) makes Grafana's table frame
/// builder throw `Cannot read properties of undefined (reading '0')`.
struct MatchedAttr {
    out_key: String,
    source: AttrSource,
}

fn add_matched_field(f: &tast::Field, out: &mut Vec<MatchedAttr>) {
    use tast::{AttrScope, Field};
    let Field::Attr { scope, path } = f else { return };
    let (out_key, source) = if path == "service.name" {
        ("service.name".to_string(), AttrSource::Service)
    } else {
        match scope {
            AttrScope::Resource => (path.clone(), AttrSource::Resource(path.clone())),
            AttrScope::Span | AttrScope::Unscoped => (path.clone(), AttrSource::Span(path.clone())),
            _ => return,
        }
    };
    if !out.iter().any(|m| m.out_key == out_key) {
        out.push(MatchedAttr { out_key, source });
    }
}

fn collect_matched_attrs(fe: &tast::FieldExpr, out: &mut Vec<MatchedAttr>) {
    use tast::FieldExpr;
    match fe {
        FieldExpr::Cmp { lhs, rhs, .. } => {
            add_matched_field(lhs, out);
            add_matched_field(rhs, out);
        }
        FieldExpr::Field(f) => add_matched_field(f, out),
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            collect_matched_attrs(a, out);
            collect_matched_attrs(b, out);
        }
    }
}

/// The attributes a search query referenced (empty for intrinsic-only / `{}`).
fn search_matched_attrs(traceql: &str) -> Vec<MatchedAttr> {
    let mut out = Vec::new();
    if let Ok(tast::SpansetExpr::Filter(Some(fe))) = super::traceql::parse(traceql) {
        collect_matched_attrs(&fe, &mut out);
    }
    out
}

/// Build a result span's `attributes`: only the matched keys, the SAME set on
/// every span (uniform) so Grafana's table renders. Absent values become an
/// empty string (the span is in a matched trace but may lack the span-level key).
fn project_span_attrs(
    matched: &[MatchedAttr],
    service: &str,
    attrs_json: Option<&str>,
    res_json: Option<&str>,
) -> Vec<Value> {
    if matched.is_empty() {
        return Vec::new();
    }
    let parse = |j: Option<&str>| {
        j.and_then(|s| serde_json::from_str::<serde_json::Map<String, Value>>(s).ok())
    };
    let span_map = parse(attrs_json);
    let res_map = parse(res_json);
    matched
        .iter()
        .map(|m| {
            let value = match &m.source {
                AttrSource::Service => json!({ "stringValue": service }),
                AttrSource::Span(p) => span_map
                    .as_ref()
                    .and_then(|mm| mm.get(p))
                    .map_or_else(|| json!({ "stringValue": "" }), json_to_otlp_value),
                AttrSource::Resource(p) => res_map
                    .as_ref()
                    .and_then(|mm| mm.get(p))
                    .map_or_else(|| json!({ "stringValue": "" }), json_to_otlp_value),
            };
            json!({ "key": m.out_key, "value": value })
        })
        .collect()
}

/// The intrinsic tag names always available (attribute-key discovery over JSON
/// blobs is deferred — v1 advertises promoted/intrinsic tags).
pub fn intrinsic_tags() -> Vec<&'static str> {
    vec!["name", "status", "kind", "duration", "service.name"]
}

// --- Tempo response types ---

/// `GET /api/search` response.
#[derive(Debug, Serialize, Deserialize)]
pub struct TempoSearchResponse {
    /// One entry per matching trace.
    pub traces: Vec<TempoTrace>,
    /// Search metrics envelope (Grafana's Tempo datasource reads it).
    pub metrics: TempoSearchMetrics,
}

/// `metrics` block of a Tempo search response.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TempoSearchMetrics {
    /// Traces inspected.
    #[serde(rename = "inspectedTraces")]
    pub inspected_traces: u64,
    /// Completed jobs (single-node: 1).
    #[serde(rename = "completedJobs")]
    pub completed_jobs: u64,
    /// Total jobs (single-node: 1).
    #[serde(rename = "totalJobs")]
    pub total_jobs: u64,
}

/// A span set on a search hit — the matched spans Grafana renders as table rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TempoSpanSet {
    /// Matched spans.
    pub spans: Vec<TempoSpan>,
    /// Number of matched spans.
    pub matched: usize,
}

/// One matched span in a search hit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TempoSpan {
    /// Hex span id.
    #[serde(rename = "spanID")]
    pub span_id: String,
    /// Span start (ns) as a string.
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    /// Span duration (ns) as a string.
    #[serde(rename = "durationNanos")]
    pub duration_nanos: String,
    /// Matched attributes — only the keys the query referenced, uniform across
    /// spans (omitted entirely for intrinsic-only / `{}` matches, like Tempo).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub attributes: Vec<serde_json::Value>,
}

/// A single search hit (one trace).
#[derive(Debug, Serialize, Deserialize)]
pub struct TempoTrace {
    /// Hex trace id.
    #[serde(rename = "traceID")]
    pub trace_id: String,
    /// Root span's service.
    #[serde(rename = "rootServiceName")]
    pub root_service_name: String,
    /// Root span's name.
    #[serde(rename = "rootTraceName")]
    pub root_trace_name: String,
    /// Trace start (ns) as a string, per Tempo.
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    /// Trace duration in milliseconds.
    #[serde(rename = "durationMs")]
    pub duration_ms: i64,
    /// Matched span set — Grafana's TraceQL results table builds its columns
    /// from these; an absent `spanSet` makes the frame undefined.
    #[serde(rename = "spanSet")]
    pub span_set: TempoSpanSet,
    /// Same span set as a one-element array. Tempo's API exposes both the
    /// deprecated singular `spanSet` and the current plural `spanSets`; Grafana
    /// 13's Tempo datasource reads `spanSets[0]`, so omitting it crashes the
    /// Search view (`Cannot read properties of undefined (reading '0')`).
    #[serde(rename = "spanSets")]
    pub span_sets: Vec<TempoSpanSet>,
    /// Per-service span / error counts for the trace — Grafana's Search results
    /// table reads `trace.serviceStats`. Tempo always includes it; omitting it
    /// leaves the column undefined.
    #[serde(rename = "serviceStats")]
    pub service_stats: std::collections::BTreeMap<String, ServiceStat>,
}

/// One service's span / error counts within a trace (`serviceStats` value).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceStat {
    /// Number of spans for this service in the trace.
    #[serde(rename = "spanCount")]
    pub span_count: u64,
    /// Number of error-status spans (status_code == ERROR) for this service.
    #[serde(rename = "errorCount")]
    pub error_count: u64,
}

/// Run a TraceQL search and group matching spans into trace hits.
pub async fn handle_search(
    engine: &super::QueryEngine,
    traceql: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
) -> crate::Result<TempoSearchResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Int64Type};

    let df = build_search(engine, traceql, start_ns, end_ns, limit).await?;
    let batches = engine.collect(df).await?;
    // Only the query-referenced attributes are surfaced on result spans (uniform
    // across spans, like Tempo) — see project_span_attrs.
    let matched = search_matched_attrs(traceql);

    // Per-trace accumulator: root fields + the matched spans (for spanSet).
    struct Acc {
        service: String,
        name: String,
        start_ns: i64,
        duration_ns: i64,
        has_root: bool,
        spans: Vec<TempoSpan>,
        service_stats: std::collections::BTreeMap<String, ServiceStat>,
    }
    let mut traces: std::collections::BTreeMap<String, Acc> = std::collections::BTreeMap::new();
    let mut inspected: u64 = 0;
    for batch in &batches {
        let hex = cast(batch.column(0), &DataType::Utf8)?;
        let hex = hex.as_string::<i32>();
        let span_hex = cast(batch.column(1), &DataType::Utf8)?;
        let span_hex = span_hex.as_string::<i32>();
        let svc = cast(batch.column(2), &DataType::Utf8)?;
        let svc = svc.as_string::<i32>();
        let name = cast(batch.column(3), &DataType::Utf8)?;
        let name = name.as_string::<i32>();
        let start = cast(batch.column(4), &DataType::Int64)?;
        let start = start.as_primitive::<Int64Type>();
        let dur = cast(batch.column(5), &DataType::Int64)?;
        let dur = dur.as_primitive::<Int64Type>();
        let parent = batch.column(6);
        let attrs = cast(batch.column(7), &DataType::Utf8)?;
        let attrs = attrs.as_string::<i32>();
        let status = cast(batch.column(8), &DataType::Int32)?;
        let status = status.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        let res_attrs = cast(batch.column(9), &DataType::Utf8)?;
        let res_attrs = res_attrs.as_string::<i32>();
        for i in 0..batch.num_rows() {
            if hex.is_null(i) {
                continue;
            }
            inspected += 1;
            let id = hex.value(i).to_string();
            let service = if svc.is_null(i) {
                String::new()
            } else {
                svc.value(i).to_string()
            };
            let span_name = if name.is_null(i) {
                String::new()
            } else {
                name.value(i).to_string()
            };
            let start_ns = if start.is_null(i) { 0 } else { start.value(i) };
            let duration_ns = if dur.is_null(i) { 0 } else { dur.value(i) };
            let is_root = parent.is_null(i);
            let span = TempoSpan {
                span_id: if span_hex.is_null(i) {
                    String::new()
                } else {
                    span_hex.value(i).to_string()
                },
                start_time_unix_nano: start_ns.to_string(),
                duration_nanos: duration_ns.to_string(),
                attributes: project_span_attrs(
                    &matched,
                    &service,
                    (!attrs.is_null(i)).then(|| attrs.value(i)),
                    (!res_attrs.is_null(i)).then(|| res_attrs.value(i)),
                ),
            };

            let entry = traces.entry(id).or_insert_with(|| Acc {
                service: service.clone(),
                name: span_name.clone(),
                start_ns,
                duration_ns,
                has_root: false,
                spans: Vec::new(),
                service_stats: std::collections::BTreeMap::new(),
            });
            // Per-service span/error counts (status_code 2 == ERROR).
            let stat = entry.service_stats.entry(service.clone()).or_default();
            stat.span_count += 1;
            if !status.is_null(i) && status.value(i) == 2 {
                stat.error_count += 1;
            }
            entry.spans.push(span);
            // Prefer the root span's fields; else the earliest-starting span.
            if !entry.has_root && (is_root || start_ns < entry.start_ns) {
                entry.service = service;
                entry.name = span_name;
                entry.start_ns = start_ns;
                entry.duration_ns = duration_ns;
            }
            if is_root {
                entry.has_root = true;
            }
        }
    }

    // Most-recent-first, capped at the requested `limit` (Tempo returns the
    // first N traces; without this Sol returned every matched trace).
    let mut ordered: Vec<(String, Acc)> = traces.into_iter().collect();
    ordered.sort_by(|a, b| b.1.start_ns.cmp(&a.1.start_ns));
    ordered.truncate(limit as usize);
    let traces: Vec<TempoTrace> = ordered
        .into_iter()
        .map(|(id, acc)| {
            let span_set = TempoSpanSet {
                matched: acc.spans.len(),
                spans: acc.spans,
            };
            TempoTrace {
                trace_id: id,
                root_service_name: acc.service,
                root_trace_name: acc.name,
                start_time_unix_nano: acc.start_ns.to_string(),
                duration_ms: acc.duration_ns / 1_000_000,
                span_sets: vec![span_set.clone()],
                span_set,
                service_stats: acc.service_stats,
            }
        })
        .collect();
    let metrics = TempoSearchMetrics {
        inspected_traces: inspected,
        completed_jobs: 1,
        total_jobs: 1,
    };
    Ok(TempoSearchResponse { traces, metrics })
}

/// Run a trace-by-id lookup and build an OTLP-JSON `{trace:{resourceSpans:[…]}}`.
pub async fn handle_trace_by_id(
    engine: &super::QueryEngine,
    trace_id_hex: &str,
) -> crate::Result<Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Int64Type};

    let df = build_trace_by_id(engine, trace_id_hex).await?;
    let batches = engine.collect(df).await?;

    let mut spans: Vec<Value> = Vec::new();
    let mut resource_attrs: Vec<Value> = Vec::new();
    let mut scope_name = String::new();
    for batch in &batches {
        let trace_b64 = batch.column(0).as_string::<i32>();
        let span_b64 = batch.column(1).as_string::<i32>();
        let name_arr = cast(batch.column(3), &DataType::Utf8)?;
        let name = name_arr.as_string::<i32>();
        let start = cast(batch.column(4), &DataType::Int64)?;
        let start = start.as_primitive::<Int64Type>();
        let dur = cast(batch.column(5), &DataType::Int64)?;
        let dur = dur.as_primitive::<Int64Type>();
        let status = cast(batch.column(6), &DataType::Int32)?;
        let status = status.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        let attrs = batch.column(7).as_string::<i32>();
        let res_attrs = batch.column(8).as_string::<i32>();
        let parent_b64 = batch.column(9).as_string::<i32>();
        let kind = cast(batch.column(10), &DataType::Int32)?;
        let kind = kind.as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        let scope = batch.column(11).as_string::<i32>();
        for i in 0..batch.num_rows() {
            let start_ns = if start.is_null(i) { 0 } else { start.value(i) };
            let dur_ns = if dur.is_null(i) { 0 } else { dur.value(i) };
            // OTLP KeyValue-array attributes (not the raw JSON object).
            let attributes = if attrs.is_null(i) {
                Vec::new()
            } else {
                otlp_attributes(attrs.value(i))
            };
            if resource_attrs.is_empty() && !res_attrs.is_null(i) {
                resource_attrs = otlp_attributes(res_attrs.value(i));
            }
            if scope_name.is_empty() && !scope.is_null(i) {
                scope_name = scope.value(i).to_string();
            }
            let mut span = json!({
                "traceId": if trace_b64.is_null(i) { "" } else { trace_b64.value(i) },
                "spanId": if span_b64.is_null(i) { "" } else { span_b64.value(i) },
                "name": if name.is_null(i) { "" } else { name.value(i) },
                "kind": span_kind_str(if kind.is_null(i) { 0 } else { kind.value(i) }),
                "startTimeUnixNano": start_ns.to_string(),
                "endTimeUnixNano": (start_ns + dur_ns).to_string(),
                "attributes": attributes,
                "status": { "code": status_code_str(if status.is_null(i) { 0 } else { status.value(i) }) },
            });
            // parentSpanId only when present (root spans have none).
            if !parent_b64.is_null(i) && !parent_b64.value(i).is_empty() {
                span["parentSpanId"] = json!(parent_b64.value(i));
            }
            spans.push(span);
        }
    }
    Ok(json!({
        "trace": {
            "resourceSpans": [{
                "resource": { "attributes": resource_attrs },
                "scopeSpans": [{ "scope": { "name": scope_name }, "spans": spans }],
            }],
        }
    }))
}

/// Run `GET /api/v2/search/tags` (scoped): intrinsics plus the stored span /
/// resource attribute keys (raw OTLP dotted names — TraceQL uses them
/// unnormalized), so Grafana's trace browser offers real tags.
pub async fn handle_tags(engine: &super::QueryEngine) -> crate::Result<Value> {
    let span = super::prometheus::distinct_json_keys(engine, "traces", "attributes").await?;
    let resource =
        super::prometheus::distinct_json_keys(engine, "traces", "resource_attributes").await?;
    Ok(json!({
        "scopes": [
            { "name": "intrinsic", "tags": intrinsic_tags() },
            { "name": "resource", "tags": resource.into_iter().collect::<Vec<_>>() },
            { "name": "span", "tags": span.into_iter().collect::<Vec<_>>() },
            // Sol stores span events in the `events` JSON column, not as a
            // separately-indexed scope; expose an empty `event` scope so Grafana's
            // tag-scope selector matches Tempo's shape (C-T4).
            { "name": "event", "tags": [] },
        ],
        // Tempo emits a per-request `metrics` object alongside the scopes.
        "metrics": {}
    }))
}

/// Run `GET /api/search/tags` (v1 flat): intrinsics + span + resource keys.
pub async fn handle_tags_flat(engine: &super::QueryEngine) -> crate::Result<Value> {
    let span = super::prometheus::distinct_json_keys(engine, "traces", "attributes").await?;
    let resource =
        super::prometheus::distinct_json_keys(engine, "traces", "resource_attributes").await?;
    let mut names: std::collections::BTreeSet<String> =
        intrinsic_tags().into_iter().map(String::from).collect();
    names.extend(resource);
    names.extend(span);
    Ok(json!({ "tagNames": names.into_iter().collect::<Vec<_>>() }))
}

/// Run `tag/:tag/values` and build the typed `{tagValues:[…]}` response.
pub async fn handle_tag_values(engine: &super::QueryEngine, tag: &str) -> crate::Result<Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let df = build_tag_values(engine, tag).await?;
    let batches = engine.collect(df).await?;
    let mut values: Vec<Value> = Vec::new();
    for batch in &batches {
        let col = cast(batch.column(0), &DataType::Utf8)?;
        let col = col.as_string::<i32>();
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                values.push(json!({ "type": "keyword", "value": col.value(i) }));
            }
        }
    }
    Ok(json!({ "tagValues": values }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_attrs_are_matched_only_and_uniform() {
        // resource.service.name → just `service.name`, from the service column —
        // NOT the span's full attribute set (which crashed Grafana's table).
        let m = search_matched_attrs(r#"{resource.service.name="client"}"#);
        let a = project_span_attrs(&m, "client", Some(r#"{"http.method":"GET","x":"y"}"#), None);
        assert_eq!(a.len(), 1);
        assert_eq!(a[0]["key"], "service.name");
        assert_eq!(a[0]["value"]["stringValue"], "client");

        // span attribute → only that key (uniform), value looked up from the JSON.
        let m = search_matched_attrs(r#"{span.http.status_code=500}"#);
        let a = project_span_attrs(&m, "client", Some(r#"{"http.status_code":"500","o":"p"}"#), None);
        assert_eq!(a.len(), 1);
        assert_eq!(a[0]["key"], "http.status_code");

        // intrinsic-only / no attribute refs → attributes omitted (like Tempo).
        let m = search_matched_attrs(r#"{duration > 0ms}"#);
        assert!(project_span_attrs(&m, "client", Some(r#"{"a":"b"}"#), None).is_empty());
    }

    #[test]
    fn test_field_value_is_bound_literal_not_injected() {
        // A TraceQL field value with SQL metacharacters must bind as a single
        // literal in the lowered predicate.
        let evil = r#"a' OR 1=1 && x"#;
        let q = format!("{{ name=\"{evil}\" }}");
        let parsed = super::super::traceql::parse(&q).expect("parses");
        let tast::SpansetExpr::Filter(Some(fe)) = parsed else {
            panic!("expected a single filter spanset");
        };
        let e = field_pred(&fe).expect("field lowers");
        let s = format!("{e}");
        assert!(
            s.contains(&format!("Utf8({evil:?})")),
            "value bound as one literal, not injected: {s}"
        );
    }

    #[tokio::test]
    async fn test_tags_include_stored_attribute_keys() {
        let engine = trace_engine().await;
        let v = handle_tags(&engine).await.unwrap();
        let scopes = v["scopes"].as_array().unwrap();
        let tags_of = |name: &str| -> Vec<String> {
            scopes.iter().find(|s| s["name"] == name).unwrap()["tags"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t.as_str().unwrap().to_string())
                .collect()
        };
        let span = tags_of("span");
        assert!(span.contains(&"http.method".to_string()), "span: {span:?}");
        assert!(span.contains(&"db.system".to_string()), "span: {span:?}");
        assert!(tags_of("resource").contains(&"host".to_string()));
        assert!(tags_of("intrinsic").contains(&"name".to_string()));
        // C-T4: event scope present (empty) + top-level metrics object.
        assert!(
            scopes.iter().any(|s| s["name"] == "event"),
            "event scope: {scopes:?}"
        );
        assert!(v["metrics"].is_object(), "metrics object: {v}");

        let flat = handle_tags_flat(&engine).await.unwrap();
        let names: Vec<&str> = flat["tagNames"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap())
            .collect();
        for expected in ["name", "http.method", "db.system", "host"] {
            assert!(names.contains(&expected), "missing {expected}: {names:?}");
        }
    }

    #[test]
    fn test_tempo_search_response_shape() {
        let resp = TempoSearchResponse {
            traces: vec![TempoTrace {
                trace_id: "3bc59070ba6c121cad3d88a3f889b303".to_string(),
                root_service_name: "client".to_string(),
                root_trace_name: "GET /randomuser".to_string(),
                start_time_unix_nano: "1779817095000000000".to_string(),
                duration_ms: 42,
                span_set: TempoSpanSet {
                    spans: vec![TempoSpan {
                        span_id: "abc".to_string(),
                        start_time_unix_nano: "1779817095000000000".to_string(),
                        duration_nanos: "42000000".to_string(),
                        attributes: Vec::new(),
                    }],
                    matched: 1,
                },
                span_sets: Vec::new(),
                service_stats: std::collections::BTreeMap::new(),
            }],
            metrics: TempoSearchMetrics {
                inspected_traces: 1,
                completed_jobs: 1,
                total_jobs: 1,
            },
        };
        let j = serde_json::to_string(&resp).unwrap();
        assert!(
            j.contains(r#""traceID":"3bc59070ba6c121cad3d88a3f889b303""#),
            "json: {j}"
        );
        assert!(j.contains(r#""rootServiceName":"client""#), "json: {j}");
        assert!(j.contains(r#""durationMs":42"#), "json: {j}");
        // spanSet present (Grafana's TraceQL table needs it) + metrics envelope
        assert!(
            j.contains(r#""spanSet":{"spans":[{"spanID":"abc""#),
            "json: {j}"
        );
        assert!(j.contains(r#""matched":1"#), "json: {j}");
        assert!(j.contains(r#""inspectedTraces":1"#), "json: {j}");
    }

    // --- end-to-end over a 2-span trace fixture ---
    async fn trace_engine() -> crate::query::QueryEngine {
        use crate::config::query::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{
            FixedSizeBinaryArray, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
        };
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        // local hex decode (the `hex` crate is feature-gated off here).
        fn hx(s: &str) -> Vec<u8> {
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
                .collect()
        }

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp.path().join("traces").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();

        let tid = hx("3bc59070ba6c121cad3d88a3f889b303");
        let root_span = hx("aaaaaaaaaaaaaaaa");
        let child_span = hx("bbbbbbbbbbbbbbbb");

        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "start_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("duration_nanos", DataType::Int64, false),
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("span_id", DataType::FixedSizeBinary(8), false),
            Field::new("parent_span_id", DataType::FixedSizeBinary(8), true),
            Field::new("name", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
            Field::new("status_code", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["client", "client"])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64, 1_010_000_000])
                        .with_timezone("UTC"),
                ),
                Arc::new(Int64Array::from(vec![42_000_000i64, 5_000_000])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(vec![tid.clone(), tid].into_iter())
                        .unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(
                        vec![root_span.clone(), child_span].into_iter(),
                    )
                    .unwrap(),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        vec![None, Some(root_span)].into_iter(),
                        8,
                    )
                    .unwrap(),
                ),
                Arc::new(StringArray::from(vec!["GET /randomuser", "db.query"])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"http.method":"GET"}"#),
                    Some(r#"{"db.system":"pg"}"#),
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"host":"a"}"#),
                    Some(r#"{"host":"a"}"#),
                ])),
                Arc::new(Int32Array::from(vec![Some(0), Some(0)])),
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
        crate::query::QueryEngine::new(&opts).await.unwrap()
    }

    #[tokio::test]
    async fn test_search_groups_spans_into_traces() {
        let engine = trace_engine().await;
        let resp = handle_search(
            &engine,
            r#"{resource.service.name="client"}"#,
            0,
            10_000_000_000,
            20,
        )
        .await
        .unwrap();
        assert_eq!(resp.traces.len(), 1, "two spans → one trace");
        let t = &resp.traces[0];
        assert_eq!(t.trace_id, "3bc59070ba6c121cad3d88a3f889b303");
        assert_eq!(t.root_service_name, "client");
        assert_eq!(
            t.root_trace_name, "GET /randomuser",
            "root span chosen by null parent"
        );
        assert_eq!(t.duration_ms, 42);
        // Both the deprecated singular `spanSet` and the plural `spanSets`
        // (which Grafana 13 reads as spanSets[0]) must be populated.
        assert_eq!(t.span_sets.len(), 1, "spanSets present for Grafana 13");
        // serviceStats per service (Grafana reads trace.serviceStats).
        assert_eq!(
            t.service_stats["client"].span_count, 2,
            "stats: {:?}",
            t.service_stats
        );
        assert_eq!(t.service_stats["client"].error_count, 0);
        assert_eq!(t.span_sets[0].matched, t.span_set.matched);
        // spanSet carries both spans, with their attributes as OTLP KeyValue
        assert_eq!(t.span_set.matched, 2, "both spans in the span set");
        // Search surfaces ONLY the query-matched attribute (`service.name` here),
        // uniformly on every span — not each span's full, per-span-varying
        // attribute set, which made Grafana's table frame builder throw
        // "Cannot read properties of undefined (reading '0')".
        let j = serde_json::to_string(&t.span_set).unwrap();
        assert!(
            j.contains(r#"{"key":"service.name","value":{"stringValue":"client"}}"#),
            "matched attr surfaced: {j}"
        );
        assert!(
            !j.contains("http.method") && !j.contains("db.system"),
            "unmatched span attributes are NOT surfaced: {j}"
        );
    }

    #[tokio::test]
    async fn test_trace_by_id_binary_lookup_executes() {
        let engine = trace_engine().await;
        let v = handle_trace_by_id(&engine, "3bc59070ba6c121cad3d88a3f889b303")
            .await
            .unwrap();
        let spans = &v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"];
        let arr = spans.as_array().unwrap();
        assert_eq!(arr.len(), 2, "both spans returned: {v}");
        let s = &arr[0];
        // C-T2 OTLP proto-JSON span shape: base64 ids (not hex), KeyValue-array
        // attributes, kind + status enum strings.
        assert_ne!(
            s["spanId"], "aaaaaaaaaaaaaaaa",
            "spanId must be base64, not hex: {s}"
        );
        assert!(
            s["attributes"].is_array(),
            "attributes is a KeyValue array: {s}"
        );
        assert!(
            serde_json::to_string(&s["attributes"])
                .unwrap()
                .contains(r#"{"key":"http.method","value":{"stringValue":"GET"}}"#),
            "attrs: {s}"
        );
        assert!(
            s["kind"].as_str().unwrap().starts_with("SPAN_KIND"),
            "kind enum: {s}"
        );
        assert!(
            s["status"]["code"]
                .as_str()
                .unwrap()
                .starts_with("STATUS_CODE"),
            "status enum: {s}"
        );
        // resource attributes also a KeyValue array
        assert!(
            v["trace"]["resourceSpans"][0]["resource"]["attributes"].is_array(),
            "{v}"
        );
    }

    #[tokio::test]
    async fn test_tag_values_executes() {
        let engine = trace_engine().await;
        let v = handle_tag_values(&engine, "name").await.unwrap();
        let vals: Vec<&str> = v["tagValues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["value"].as_str().unwrap())
            .collect();
        assert!(vals.contains(&"GET /randomuser"), "values: {vals:?}");
        assert!(vals.contains(&"db.query"), "values: {vals:?}");
    }
}
