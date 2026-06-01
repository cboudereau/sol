//! LogQL → SQL translation + Loki `query_range` response types (task 3).
//!
//! Covers the pcap subset (label matchers `=`/`!=`/`=~`/`!~`, line filters
//! `|=`/`!=`/`|~`/`!~`) per [QUERY-MAPPING.md](../../../docs/workspace/parquet-backend/QUERY-MAPPING.md).
//! Non-promoted labels use `json_get_str(<col>, '<key>')` — a Sol UDF backed by
//! `serde_json` (registered by the engine in task 3b); DataFusion core has no
//! built-in JSON extraction and we do not add a new crate for it.

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

/// Render a label predicate. Promoted label → column; others → `json_get_str`.
fn label_pred(key: &str, op: &str, value: &str) -> Result<String, String> {
    let lhs = if key == PROMOTED_LABEL {
        PROMOTED_LABEL.to_string()
    } else {
        format!("json_get_str(resource_attributes, '{}')", esc(key))
    };
    Ok(match op {
        "=" => format!("{lhs} = '{}'", esc(value)),
        "!=" => format!("{lhs} <> '{}'", esc(value)),
        "=~" => format!("regexp_like({lhs}, '{}')", esc(value)),
        "!~" => format!("NOT regexp_like({lhs}, '{}')", esc(value)),
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
        let rest_bytes = rest.as_bytes();
        if rest_bytes.first() != Some(&b'"') {
            return Err("line filter value must be quoted".to_string());
        }
        let end = rest[1..]
            .find('"')
            .ok_or("unterminated line filter string")?;
        let val = &rest[1..=end];
        out.push((op.to_string(), val.to_string()));
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
    preds.push(format!("time_unix_nano BETWEEN {start_ns} AND {end_ns}"));

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
            sql.contains(r#"regexp_like(json_get_str(resource_attributes, 'service_version'), '1\.0\.0')"#),
            "sql: {sql}"
        );
        assert!(sql.contains("time_unix_nano BETWEEN 100 AND 200"));
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
