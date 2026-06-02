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

/// SQL-escape a string literal value (NFR9 — guards against injection).
fn esc(value: &str) -> String {
    value.replace('\'', "''")
}

/// Map a TraceQL field to a SQL left-hand side. Intrinsics and
/// `resource.service.name` are promoted columns; `span.*` / `resource.*` and
/// bare `.attr` go through JSON extraction.
fn traceql_lhs(key: &str) -> String {
    match key {
        "name" => "name".to_string(),
        "status" | "status.code" => "status_code".to_string(),
        "kind" => "kind".to_string(),
        "duration" => "duration_nanos".to_string(),
        "resource.service.name" | "service.name" | ".service.name" => "service_name".to_string(),
        _ => {
            if let Some(attr) = key.strip_prefix("resource.") {
                format!("json_get_str(resource_attributes, '{}')", esc(attr))
            } else if let Some(attr) = key.strip_prefix("span.") {
                format!("json_get_str(attributes, '{}')", esc(attr))
            } else {
                let attr = key.strip_prefix('.').unwrap_or(key);
                format!("json_get_str(attributes, '{}')", esc(attr))
            }
        }
    }
}

/// Parse a `{ a="x" && b!="y" }` selector into `(key, op, value)` matchers.
fn parse_selector(traceql: &str) -> Result<Vec<(String, String, String)>, String> {
    let inner = traceql
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or("TraceQL must be a `{ ... }` selector")?;
    let mut out = Vec::new();
    for part in inner.split("&&") {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, op, val) = if let Some(i) = part.find("!=") {
            (&part[..i], "!=", &part[i + 2..])
        } else if let Some(i) = part.find('=') {
            (&part[..i], "=", &part[i + 1..])
        } else {
            return Err(format!("unsupported TraceQL matcher: {part} (only = / != in v1)"));
        };
        let val = val.trim().trim_matches('"');
        out.push((key.trim().to_string(), op.to_string(), val.to_string()));
    }
    Ok(out)
}

fn matcher_sql(key: &str, op: &str, val: &str) -> String {
    let lhs = traceql_lhs(key);
    match op {
        "!=" => format!("{lhs} <> '{}'", esc(val)),
        _ => format!("{lhs} = '{}'", esc(val)),
    }
}

/// Translate a TraceQL search query into SQL over the `traces` table. Returns
/// one row per matching span (the handler groups by trace).
pub fn translate_search(traceql: &str, start_ns: i64, end_ns: i64, limit: u32) -> Result<String, String> {
    let mut preds: Vec<String> = Vec::new();
    let traceql = traceql.trim();
    // An empty `{}` selector matches everything (time-bounded).
    if traceql != "{}" && !traceql.is_empty() {
        for (k, op, v) in parse_selector(traceql)? {
            preds.push(matcher_sql(&k, &op, &v));
        }
    }
    preds.push(format!("CAST(start_time_unix_nano AS BIGINT) BETWEEN {start_ns} AND {end_ns}"));
    Ok(format!(
        "SELECT encode(trace_id, 'hex') AS trace_hex, encode(span_id, 'hex') AS span_hex, \
         service_name, name, start_time_unix_nano, duration_nanos, parent_span_id \
         FROM traces WHERE {} ORDER BY start_time_unix_nano DESC LIMIT {}",
        preds.join(" AND "),
        limit.saturating_mul(64) // headroom: several spans per trace
    ))
}

/// Validate a hex trace-id and render the `WHERE trace_id = X'..'` lookup SQL.
pub fn trace_by_id_sql(trace_id_hex: &str) -> Result<String, String> {
    let hex = trace_id_hex.trim().to_lowercase();
    if hex.is_empty() || !hex.len().is_multiple_of(2) || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("trace id must be an even-length hex string".to_string());
    }
    // FixedSizeBinary needs an explicit cast from the binary literal.
    Ok(format!(
        "SELECT encode(trace_id, 'hex') AS trace_hex, encode(span_id, 'hex') AS span_hex, \
         service_name, name, start_time_unix_nano, duration_nanos, status_code, \
         attributes, resource_attributes \
         FROM traces WHERE trace_id = arrow_cast(X'{hex}', 'FixedSizeBinary(16)') \
         ORDER BY start_time_unix_nano"
    ))
}

/// `SELECT DISTINCT` SQL for `tag/:tag/values`.
pub fn tag_values_sql(tag: &str) -> String {
    let lhs = traceql_lhs(tag);
    format!("SELECT DISTINCT CAST({lhs} AS VARCHAR) AS v FROM traces WHERE {lhs} IS NOT NULL ORDER BY v")
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
#[derive(Debug, Serialize, Deserialize)]
pub struct TempoSpanSet {
    /// Matched spans.
    pub spans: Vec<TempoSpan>,
    /// Number of matched spans.
    pub matched: usize,
}

/// One matched span in a search hit.
#[derive(Debug, Serialize, Deserialize)]
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
    /// Matched attributes (empty for resource/intrinsic-only matches).
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

    let to_err = |e: String| Box::<dyn std::error::Error + Send + Sync>::from(e);
    let sql = translate_search(traceql, start_ns, end_ns, limit).map_err(to_err)?;
    let batches = engine.sql(&sql).await?;

    // Per-trace accumulator: root fields + the matched spans (for spanSet).
    struct Acc {
        service: String,
        name: String,
        start_ns: i64,
        duration_ns: i64,
        has_root: bool,
        spans: Vec<TempoSpan>,
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
        for i in 0..batch.num_rows() {
            if hex.is_null(i) {
                continue;
            }
            inspected += 1;
            let id = hex.value(i).to_string();
            let service = if svc.is_null(i) { String::new() } else { svc.value(i).to_string() };
            let span_name = if name.is_null(i) { String::new() } else { name.value(i).to_string() };
            let start_ns = if start.is_null(i) { 0 } else { start.value(i) };
            let duration_ns = if dur.is_null(i) { 0 } else { dur.value(i) };
            let is_root = parent.is_null(i);
            let span = TempoSpan {
                span_id: if span_hex.is_null(i) { String::new() } else { span_hex.value(i).to_string() },
                start_time_unix_nano: start_ns.to_string(),
                duration_nanos: duration_ns.to_string(),
                attributes: Vec::new(),
            };

            let entry = traces.entry(id).or_insert_with(|| Acc {
                service: service.clone(),
                name: span_name.clone(),
                start_ns,
                duration_ns,
                has_root: false,
                spans: Vec::new(),
            });
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

    let traces: Vec<TempoTrace> = traces
        .into_iter()
        .map(|(id, acc)| TempoTrace {
            trace_id: id,
            root_service_name: acc.service,
            root_trace_name: acc.name,
            start_time_unix_nano: acc.start_ns.to_string(),
            duration_ms: acc.duration_ns / 1_000_000,
            span_set: TempoSpanSet { matched: acc.spans.len(), spans: acc.spans },
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

    let to_err = |e: String| Box::<dyn std::error::Error + Send + Sync>::from(e);
    let sql = trace_by_id_sql(trace_id_hex).map_err(to_err)?;
    let batches = engine.sql(&sql).await?;

    let mut spans: Vec<Value> = Vec::new();
    let mut resource_attrs: Value = json!([]);
    for batch in &batches {
        let trace_hex = batch.column(0).as_string::<i32>();
        let span_hex = batch.column(1).as_string::<i32>();
        let name_arr = cast(batch.column(3), &DataType::Utf8)?;
        let name = name_arr.as_string::<i32>();
        let start = cast(batch.column(4), &DataType::Int64)?;
        let start = start.as_primitive::<Int64Type>();
        let dur = cast(batch.column(5), &DataType::Int64)?;
        let dur = dur.as_primitive::<Int64Type>();
        let attrs = batch.column(7).as_string::<i32>();
        let res_attrs = batch.column(8).as_string::<i32>();
        for i in 0..batch.num_rows() {
            let start_ns = if start.is_null(i) { 0 } else { start.value(i) };
            let dur_ns = if dur.is_null(i) { 0 } else { dur.value(i) };
            let attributes: Value = if attrs.is_null(i) {
                json!({})
            } else {
                serde_json::from_str(attrs.value(i)).unwrap_or(json!({}))
            };
            if !res_attrs.is_null(i)
                && let Ok(v) = serde_json::from_str::<Value>(res_attrs.value(i))
            {
                resource_attrs = v;
            }
            spans.push(json!({
                "traceId": if trace_hex.is_null(i) { "" } else { trace_hex.value(i) },
                "spanId": if span_hex.is_null(i) { "" } else { span_hex.value(i) },
                "name": if name.is_null(i) { "" } else { name.value(i) },
                "startTimeUnixNano": start_ns.to_string(),
                "endTimeUnixNano": (start_ns + dur_ns).to_string(),
                "attributes": attributes,
            }));
        }
    }
    Ok(json!({
        "trace": {
            "resourceSpans": [{
                "resource": { "attributes": resource_attrs },
                "scopeSpans": [{ "scope": {}, "spans": spans }],
            }],
        }
    }))
}

/// `GET /api/v2/search/tags` (scoped) response.
pub fn tags_response() -> Value {
    json!({
        "scopes": [
            { "name": "intrinsic", "tags": intrinsic_tags() },
            { "name": "span", "tags": [] },
            { "name": "resource", "tags": [] },
        ]
    })
}

/// `GET /api/search/tags` (v1 flat) response.
pub fn tags_flat_response() -> Value {
    json!({ "tagNames": intrinsic_tags() })
}

/// Run `tag/:tag/values` and build the typed `{tagValues:[…]}` response.
pub async fn handle_tag_values(
    engine: &super::QueryEngine,
    tag: &str,
) -> crate::Result<Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let batches = engine.sql(&tag_values_sql(tag)).await?;
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
    fn test_traceql_top_level_columns() {
        let sql =
            translate_search(r#"{resource.service.name="client" && name="GET /x"}"#, 0, 100, 20)
                .unwrap();
        assert!(sql.contains("service_name = 'client'"), "sql: {sql}");
        assert!(sql.contains("name = 'GET /x'"), "sql: {sql}");
        assert!(sql.contains("encode(trace_id, 'hex')"), "sql: {sql}");
    }

    #[test]
    fn test_traceql_span_attr_json_extract() {
        let sql = translate_search(r#"{span.http.method="GET" && .code!="0"}"#, 0, 1, 5).unwrap();
        assert!(sql.contains("json_get_str(attributes, 'http.method') = 'GET'"), "sql: {sql}");
        assert!(sql.contains("json_get_str(attributes, 'code') <> '0'"), "sql: {sql}");
    }

    #[test]
    fn test_trace_by_id_hex_to_binary_literal() {
        let sql = trace_by_id_sql("3bc59070ba6c121cad3d88a3f889b303").unwrap();
        assert!(
            sql.contains("X'3bc59070ba6c121cad3d88a3f889b303'"),
            "binary literal missing; sql: {sql}"
        );
        assert!(sql.contains("FixedSizeBinary(16)"), "sql: {sql}");
        // odd-length / non-hex is rejected
        assert!(trace_by_id_sql("xyz").is_err());
        assert!(trace_by_id_sql("abc").is_err());
    }

    #[test]
    fn test_tag_values_distinct() {
        assert!(tag_values_sql("name").contains("SELECT DISTINCT"));
        assert!(tag_values_sql("name").contains("FROM traces"));
        assert!(tag_values_sql("span.http.method").contains("json_get_str(attributes, 'http.method')"));
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
            }],
            metrics: TempoSearchMetrics { inspected_traces: 1, completed_jobs: 1, total_jobs: 1 },
        };
        let j = serde_json::to_string(&resp).unwrap();
        assert!(j.contains(r#""traceID":"3bc59070ba6c121cad3d88a3f889b303""#), "json: {j}");
        assert!(j.contains(r#""rootServiceName":"client""#), "json: {j}");
        assert!(j.contains(r#""durationMs":42"#), "json: {j}");
        // spanSet present (Grafana's TraceQL table needs it) + metrics envelope
        assert!(j.contains(r#""spanSet":{"spans":[{"spanID":"abc""#), "json: {j}");
        assert!(j.contains(r#""matched":1"#), "json: {j}");
        assert!(j.contains(r#""inspectedTraces":1"#), "json: {j}");
    }

    // --- end-to-end over a 2-span trace fixture ---
    async fn trace_engine() -> crate::query::QueryEngine {
        use crate::config::query::{Options, StorageConfig};
        use datafusion::arrow::array::{
            FixedSizeBinaryArray, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
        };
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        // local hex decode (the `hex` crate is feature-gated off here).
        fn hx(s: &str) -> Vec<u8> {
            (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
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
                    FixedSizeBinaryArray::try_from_iter(vec![tid.clone(), tid].into_iter()).unwrap(),
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
                Arc::new(StringArray::from(vec![Some(r#"{"host":"a"}"#), Some(r#"{"host":"a"}"#)])),
                Arc::new(Int32Array::from(vec![Some(0), Some(0)])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("f.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let opts = Options {
            storage: StorageConfig { path: tmp.path().into(), url: None },
            ..Options::default()
        };
        crate::query::QueryEngine::new(&opts).await.unwrap()
    }

    #[tokio::test]
    async fn test_search_groups_spans_into_traces() {
        let engine = trace_engine().await;
        let resp = handle_search(&engine, r#"{resource.service.name="client"}"#, 0, 10_000_000_000, 20)
            .await
            .unwrap();
        assert_eq!(resp.traces.len(), 1, "two spans → one trace");
        let t = &resp.traces[0];
        assert_eq!(t.trace_id, "3bc59070ba6c121cad3d88a3f889b303");
        assert_eq!(t.root_service_name, "client");
        assert_eq!(t.root_trace_name, "GET /randomuser", "root span chosen by null parent");
        assert_eq!(t.duration_ms, 42);
    }

    #[tokio::test]
    async fn test_trace_by_id_binary_lookup_executes() {
        let engine = trace_engine().await;
        let v = handle_trace_by_id(&engine, "3bc59070ba6c121cad3d88a3f889b303").await.unwrap();
        let spans = &v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"];
        assert_eq!(spans.as_array().unwrap().len(), 2, "both spans returned: {v}");
        assert_eq!(spans[0]["spanId"], "aaaaaaaaaaaaaaaa");
    }

    #[tokio::test]
    async fn test_tag_values_executes() {
        let engine = trace_engine().await;
        let v = handle_tag_values(&engine, "name").await.unwrap();
        let vals: Vec<&str> = v["tagValues"].as_array().unwrap().iter().map(|x| x["value"].as_str().unwrap()).collect();
        assert!(vals.contains(&"GET /randomuser"), "values: {vals:?}");
        assert!(vals.contains(&"db.query"), "values: {vals:?}");
    }
}
