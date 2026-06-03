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

/// SQL-escape a string literal value (double single-quotes) — guards against
/// injection from label/line-filter values (NFR9 security).
fn esc(value: &str) -> String {
    value.replace('\'', "''")
}

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

/// Render a label predicate. Promoted label → column; others → `prom_attr`,
/// which matches the Prometheus-normalized name against the raw OTLP key (so
/// `deployment_environment` hits the stored `deployment.environment`). Matcher
/// semantics mirror Prometheus: an absent label behaves like the empty string.
fn label_pred(key: &str, op: &str, value: &str) -> Result<String, String> {
    let lhs = if key == PROMOTED_LABEL {
        PROMOTED_LABEL.to_string()
    } else {
        format!("prom_attr(resource_attributes, '{}')", esc(key))
    };
    let v = esc(value);
    Ok(match op {
        "=" if value.is_empty() => format!("({lhs} IS NULL OR {lhs} = '')"),
        "=" => format!("{lhs} = '{v}'"),
        "!=" if value.is_empty() => format!("({lhs} IS NOT NULL AND {lhs} <> '')"),
        "!=" => format!("({lhs} IS NULL OR {lhs} <> '{v}')"),
        // LogQL anchors label-matcher regexes (`^(?:RE)$`), unlike DataFusion's
        // substring regexp_like. (Line filters `|~`/`!~` below stay unanchored —
        // those are substring matches in LogQL.)
        "=~" => format!("regexp_like(COALESCE({lhs}, ''), '^(?:{v})$')"),
        "!~" => format!("NOT regexp_like(COALESCE({lhs}, ''), '^(?:{v})$')"),
        other => return Err(format!("unsupported label matcher op: {other}")),
    })
}

/// Render a line filter over the `body` column.
fn line_pred(op: &str, value: &str) -> Result<String, String> {
    Ok(match op {
        "|=" => format!("body LIKE '%{}%'", esc(value)),
        "!=" => format!("body NOT LIKE '%{}%'", esc(value)),
        "|~" => format!("regexp_like(body, '{}')", esc(value)),
        "!~" => format!("NOT regexp_like(body, '{}')", esc(value)),
        other => return Err(format!("unsupported line filter op: {other}")),
    })
}

/// Parse the `{...}` stream selector into `(key, op, value)` matchers.
fn parse_selector(sel: &str) -> Result<Vec<(String, String, String)>, String> {
    let inner = sel
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or("LogQL must start with a `{...}` stream selector")?;
    let mut out = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // op is the longest match of =~, !~, !=, = (check 2-char ops first).
        let (key, op, val) = if let Some(i) = part.find("=~") {
            (&part[..i], "=~", &part[i + 2..])
        } else if let Some(i) = part.find("!~") {
            (&part[..i], "!~", &part[i + 2..])
        } else if let Some(i) = part.find("!=") {
            (&part[..i], "!=", &part[i + 2..])
        } else if let Some(i) = part.find('=') {
            (&part[..i], "=", &part[i + 1..])
        } else {
            return Err(format!("malformed label matcher: {part}"));
        };
        // Strip the quotes, then unescape: double-quoted values use Go-style
        // escapes (`\\` -> `\`); backtick values are raw strings (verbatim).
        let val = val.trim();
        let val = if let Some(inner) = val.strip_prefix('`').and_then(|s| s.strip_suffix('`')) {
            inner.to_string()
        } else if let Some(inner) = val.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
            unescape_dquoted(inner)
        } else {
            val.trim_matches('"').to_string()
        };
        out.push((key.trim().to_string(), op.to_string(), val));
    }
    Ok(out)
}

/// Parse the pipeline after the selector into `(op, value)` line filters.
fn parse_line_filters(pipeline: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut rest = pipeline.trim();
    while !rest.is_empty() {
        let op = ["|=", "!=", "|~", "!~"]
            .iter()
            .find(|o| rest.starts_with(**o))
            .ok_or_else(|| format!("unsupported LogQL pipeline near: {rest}"))?;
        rest = rest[op.len()..].trim_start();
        // LogQL line-filter values are quoted with `"` or backticks; Grafana's
        // Explore sends an empty `|= \`\`` which is a no-op (matches everything).
        let quote = match rest.chars().next() {
            Some(q @ ('"' | '`')) => q,
            _ => return Err("line filter value must be quoted".to_string()),
        };
        let end = rest[1..]
            .find(quote)
            .ok_or("unterminated line filter string")?;
        let raw = &rest[1..=end];
        // Double-quoted filter values carry Go-style escapes; backticks are raw.
        let val = if quote == '"' {
            unescape_dquoted(raw)
        } else {
            raw.to_string()
        };
        if !val.is_empty() {
            out.push((op.to_string(), val));
        }
        rest = rest[end + 2..].trim_start();
    }
    Ok(out)
}

/// Translate a LogQL log query + range params into SQL over the `logs` table.
pub fn translate_query_range(
    logql: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
    forward: bool,
) -> Result<String, String> {
    let logql = logql.trim();
    let brace_end = logql.find('}').ok_or("missing `}` in stream selector")?;
    let (selector, pipeline) = logql.split_at(brace_end + 1);

    let mut preds: Vec<String> = Vec::new();
    for (k, op, v) in parse_selector(selector)? {
        preds.push(label_pred(&k, &op, &v)?);
    }
    for (op, v) in parse_line_filters(pipeline)? {
        preds.push(line_pred(&op, &v)?);
    }
    preds.push(format!(
        "CAST(time_unix_nano AS BIGINT) BETWEEN {start_ns} AND {end_ns}"
    ));

    let dir = if forward { "ASC" } else { "DESC" };
    Ok(format!(
        "SELECT service_name, time_unix_nano, body, severity_number FROM logs WHERE {} ORDER BY time_unix_nano {dir} LIMIT {limit}",
        preds.join(" AND ")
    ))
}

/// `SELECT DISTINCT` SQL for Loki `label/:name/values` over the logs table.
pub fn label_values_sql(label: &str) -> String {
    let lhs = if label == PROMOTED_LABEL {
        PROMOTED_LABEL.to_string()
    } else {
        format!("prom_attr(resource_attributes, '{}')", esc(label))
    };
    format!("SELECT DISTINCT {lhs} AS v FROM logs WHERE {lhs} IS NOT NULL ORDER BY v")
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
pub async fn handle_label_values(
    engine: &super::QueryEngine,
    label: &str,
) -> crate::Result<serde_json::Value> {
    let values = super::prometheus::string_column(engine, &label_values_sql(label)).await?;
    Ok(serde_json::json!({ "status": "success", "data": values }))
}

/// Whether a LogQL query is a **metric** query (volume / aggregation) rather
/// than a plain `{...}` log-stream selector. Grafana's "Logs volume" panel
/// issues `sum by (level) (count_over_time({sel}[range]))`, which must produce
/// a Prometheus-style matrix, not a `streams` result.
pub fn is_metric_query(logql: &str) -> bool {
    !logql.trim_start().starts_with('{')
}

/// SQL for a log-volume query: count logs per `(detected_level, time-bucket of
/// step_ns)` over the inner `{selector}`. Grafana renders the per-level volume
/// bars from the resulting matrix. We always group by `detected_level` (the
/// Loki volume default), regardless of the query's `by(...)`.
fn volume_sql(logql: &str, start_ns: i64, end_ns: i64, step_ns: i64) -> Result<String, String> {
    let open = logql.find('{').ok_or("log volume query must contain a {...} selector")?;
    let close = logql[open..].find('}').ok_or("unterminated {...} selector")? + open;
    let selector = &logql[open..=close];
    let mut preds: Vec<String> = parse_selector(selector)?
        .into_iter()
        .map(|(k, op, v)| label_pred(&k, &op, &v))
        .collect::<Result<_, _>>()?;
    preds.push(format!("CAST(time_unix_nano AS BIGINT) BETWEEN {start_ns} AND {end_ns}"));
    let step = step_ns.max(1);
    // detected_level via the same severity ranges as `detected_level()`.
    let level = "CASE \
        WHEN severity_number BETWEEN 1 AND 4 THEN 'trace' \
        WHEN severity_number BETWEEN 5 AND 8 THEN 'debug' \
        WHEN severity_number BETWEEN 9 AND 12 THEN 'info' \
        WHEN severity_number BETWEEN 13 AND 16 THEN 'warn' \
        WHEN severity_number BETWEEN 17 AND 20 THEN 'error' \
        WHEN severity_number BETWEEN 21 AND 24 THEN 'fatal' ELSE 'unknown' END";
    Ok(format!(
        "SELECT {level} AS lvl, (CAST(time_unix_nano AS BIGINT) / {step}) * {step} AS bkt, \
         count(*) AS c FROM logs WHERE {} GROUP BY lvl, bkt ORDER BY bkt",
        preds.join(" AND ")
    ))
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

    let sql = volume_sql(query, start_ns, end_ns, step_ns)
        .map_err(Box::<dyn std::error::Error + Send + Sync>::from)?;
    let batches = engine.sql(&sql).await?;

    let mut series: BTreeMap<String, Vec<serde_json::Value>> = BTreeMap::new();
    for batch in &batches {
        let lvl = batch.column(0).as_string::<i32>();
        let bkt = batch.column(1).as_primitive::<Int64Type>();
        let c = batch.column(2).as_primitive::<Int64Type>();
        for i in 0..batch.num_rows() {
            let level =
                if lvl.is_null(i) { "unknown".to_string() } else { lvl.value(i).to_string() };
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

    let sql = translate_query_range(query, start_ns, end_ns, limit, forward)
        .map_err(Box::<dyn std::error::Error + Send + Sync>::from)?;
    let batches = engine.sql(&sql).await?;

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
    fn test_logql_label_matchers_to_where() {
        let sql = translate_query_range(
            r#"{service_name="client", service_version=~"1\.0\.0"}"#,
            100,
            200,
            1000,
            false,
        )
        .unwrap();
        assert!(sql.contains("service_name = 'client'"), "sql: {sql}");
        assert!(
            sql.contains(r#"regexp_like(COALESCE(prom_attr(resource_attributes, 'service_version'), ''), '^(?:1\.0\.0)$')"#),
            "sql: {sql}"
        );
        assert!(sql.contains("CAST(time_unix_nano AS BIGINT) BETWEEN 100 AND 200"));
        assert!(sql.contains("ORDER BY time_unix_nano DESC LIMIT 1000"));
    }

    #[test]
    fn test_logql_regex_double_backslash_unescaped() {
        // Grafana sends regex matchers with escaped backslashes on the wire:
        //   service_version=~"1\\.0\\.0"
        // A double-quoted LogQL/PromQL string must be unescaped (`\\` -> `\`)
        // before use, collapsing to the regex `1\.0\.0` which matches "1.0.0".
        // Regression: the value was used verbatim, producing a regex with literal
        // backslashes that never matched -> empty log panels in the demo.
        let sql = translate_query_range(
            r#"{service_name="client", service_version=~"1\\.0\\.0"}"#,
            100,
            200,
            1000,
            false,
        )
        .unwrap();
        assert!(
            sql.contains(r#"regexp_like(COALESCE(prom_attr(resource_attributes, 'service_version'), ''), '^(?:1\.0\.0)$')"#),
            "double-backslash regex must unescape to single backslash: {sql}"
        );
    }

    #[test]
    fn test_logql_line_filter_to_like() {
        let sql =
            translate_query_range(r#"{service_name="client"} |= "error""#, 0, 1, 10, true).unwrap();
        assert!(sql.contains("body LIKE '%error%'"), "sql: {sql}");
        assert!(sql.contains("ORDER BY time_unix_nano ASC"));
    }

    #[test]
    fn test_logql_escapes_quotes() {
        let sql = translate_query_range(r#"{service_name="a'b"}"#, 0, 1, 1, true).unwrap();
        assert!(sql.contains("service_name = 'a''b'"), "sql: {sql}");
    }

    #[test]
    fn test_is_metric_query_detects_volume() {
        assert!(is_metric_query(
            r#"sum by (detected_level) (count_over_time({service_name="client"}[1m]))"#
        ));
        assert!(!is_metric_query(r#"{service_name="client"} |= "x""#));
    }

    #[test]
    fn test_volume_sql_buckets_by_step_and_level() {
        let sql = volume_sql(
            r#"sum by (detected_level) (count_over_time({service_name="client"}[1m]))"#,
            0,
            1_000_000_000_000,
            60_000_000_000,
        )
        .unwrap();
        assert!(sql.contains("service_name = 'client'"), "selector filter: {sql}");
        assert!(sql.contains("count(*) AS c"), "{sql}");
        assert!(sql.contains("GROUP BY lvl, bkt"), "{sql}");
        assert!(sql.contains("/ 60000000000) * 60000000000"), "step bucketing: {sql}");
        assert!(sql.contains("'info'") && sql.contains("'error'"), "level CASE: {sql}");
    }

    #[test]
    fn test_logql_empty_backtick_filter_is_noop() {
        // Grafana Explore sends an empty backtick line filter; it must parse and
        // add no body predicate (matches everything), not error.
        let sql = translate_query_range(
            "{service_name=\"client\", deployment_environment=\"dev\"} |= ``",
            0,
            1,
            10,
            true,
        )
        .unwrap();
        assert!(
            !sql.contains("body LIKE"),
            "empty filter adds no body predicate: {sql}"
        );
        assert!(sql.contains("service_name = 'client'"), "sql: {sql}");
        // normalized attribute label
        assert!(
            sql.contains("prom_attr(resource_attributes, 'deployment_environment') = 'dev'"),
            "sql: {sql}"
        );
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

    #[test]
    fn test_logql_selects_severity_number() {
        let sql = translate_query_range(r#"{service_name="client"}"#, 0, 1, 10, true).unwrap();
        assert!(
            sql.contains("SELECT service_name, time_unix_nano, body, severity_number"),
            "sql: {sql}"
        );
    }

    #[tokio::test]
    async fn test_loki_handle_query_range_end_to_end() {
        use crate::config::query::{QuerierOptions, StorageConfig};
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
            schema.clone(),
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
        let engine = crate::query::QueryEngine::new(&opts).await.unwrap();

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

    #[test]
    fn test_loki_label_values_sql() {
        assert!(
            label_values_sql("service_name")
                .contains("SELECT DISTINCT service_name AS v FROM logs"),
        );
        assert!(
            label_values_sql("deployment_environment")
                .contains("prom_attr(resource_attributes, 'deployment_environment')")
        );
    }

    #[tokio::test]
    async fn test_loki_labels_end_to_end() {
        use crate::config::query::{QuerierOptions, StorageConfig};
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
            schema.clone(),
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
        let engine = crate::query::QueryEngine::new(&opts).await.unwrap();

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
