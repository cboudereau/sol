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
        "=~" => format!("regexp_like(COALESCE({lhs}, ''), '{v}')"),
        "!~" => format!("NOT regexp_like(COALESCE({lhs}, ''), '{v}')"),
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
        let val = val.trim().trim_matches('"');
        out.push((key.trim().to_string(), op.to_string(), val.to_string()));
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
        let end = rest[1..].find(quote).ok_or("unterminated line filter string")?;
        let val = &rest[1..=end];
        if !val.is_empty() {
            out.push((op.to_string(), val.to_string()));
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
        "SELECT service_name, time_unix_nano, body FROM logs WHERE {} ORDER BY time_unix_nano {dir} LIMIT {limit}",
        preds.join(" AND ")
    ))
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
    /// Build a `streams` response from `(service_name, ts_ns, body)` rows.
    pub fn streams(rows: impl IntoIterator<Item = (String, i64, String)>) -> Self {
        let mut by_stream: BTreeMap<String, Vec<[String; 2]>> = BTreeMap::new();
        for (service_name, ts, body) in rows {
            by_stream
                .entry(service_name)
                .or_default()
                .push([ts.to_string(), body]);
        }
        let result = by_stream
            .into_iter()
            .map(|(service_name, values)| {
                let mut stream = BTreeMap::new();
                stream.insert("service_name".to_string(), service_name);
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
    use datafusion::arrow::datatypes::TimestampNanosecondType;

    let sql = translate_query_range(query, start_ns, end_ns, limit, forward)
        .map_err(Box::<dyn std::error::Error + Send + Sync>::from)?;
    let batches = engine.sql(&sql).await?;

    let mut rows: Vec<(String, i64, String)> = Vec::new();
    for batch in &batches {
        let svc = batch.column(0).as_string::<i32>();
        let ts = batch.column(1).as_primitive::<TimestampNanosecondType>();
        let body = batch.column(2).as_string::<i32>();
        for i in 0..batch.num_rows() {
            let service = if svc.is_null(i) { String::new() } else { svc.value(i).to_string() };
            let nanos = if ts.is_null(i) { 0 } else { ts.value(i) };
            let line = if body.is_null(i) { String::new() } else { body.value(i).to_string() };
            rows.push((service, nanos, line));
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
            sql.contains(r#"regexp_like(COALESCE(prom_attr(resource_attributes, 'service_version'), ''), '1\.0\.0')"#),
            "sql: {sql}"
        );
        assert!(sql.contains("CAST(time_unix_nano AS BIGINT) BETWEEN 100 AND 200"));
        assert!(sql.contains("ORDER BY time_unix_nano DESC LIMIT 1000"));
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
        assert!(!sql.contains("body LIKE"), "empty filter adds no body predicate: {sql}");
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
            ("client".to_string(), 1700000000000000000, "hello".to_string()),
            ("client".to_string(), 1700000000000000001, "world".to_string()),
        ]);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""resultType":"streams""#), "json: {json}");
        assert!(json.contains(r#""stream":{"service_name":"client"}"#), "json: {json}");
        assert!(json.contains(r#"["1700000000000000000","hello"]"#), "json: {json}");
    }

    #[tokio::test]
    async fn test_loki_handle_query_range_end_to_end() {
        use crate::config::query::{Options, StorageConfig};
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

        let opts = Options {
            storage: StorageConfig { path: tmp.path().into(), url: None },
            ..Options::default()
        };
        let engine = crate::query::QueryEngine::new(&opts).await.unwrap();

        let resp =
            handle_query_range(&engine, r#"{service_name="client"} |= "hello""#, 0, 1000, 100, false)
                .await
                .unwrap();
        assert_eq!(resp.data.result_type, "streams");
        assert_eq!(resp.data.result.len(), 1, "one stream (client)");
        let s = &resp.data.result[0];
        assert_eq!(s.stream["service_name"], "client");
        assert_eq!(s.values.len(), 1, "only 'hello world' matches");
        assert_eq!(s.values[0][1], "hello world");
    }

    #[test]
    fn test_loki_response_deserializes() {
        let resp = LokiResponse::streams([("svc".to_string(), 1, "x".to_string())]);
        let json = serde_json::to_string(&resp).unwrap();
        let back: LokiResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.status, "success");
        assert_eq!(back.data.result_type, "streams");
        assert_eq!(back.data.result[0].values[0], ["1".to_string(), "x".to_string()]);
    }
}
