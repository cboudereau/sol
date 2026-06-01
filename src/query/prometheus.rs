//! PromQL → SQL (instant queries) + Prometheus API response types (task 4).
//!
//! Parses PromQL with the `promql-parser` crate and translates instant vector
//! selectors + simple `<agg> by (...)` aggregations to SQL over the `metrics`
//! table. Unsupported expressions (range functions, binary ops, subqueries)
//! return an error, never a panic — per [QUERY-MAPPING.md](../../../docs/workspace/parquet-backend/QUERY-MAPPING.md)
//! and [API-SPEC.md](../../../docs/workspace/parquet-backend/API-SPEC.md) §1.

use std::collections::BTreeMap;

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{self, AggregateExpr, Expr, LabelModifier, VectorSelector, token};
use serde::{Deserialize, Serialize};

fn esc(v: &str) -> String {
    v.replace('\'', "''")
}

/// Left-hand side for a label: promoted `service_name` is a column; everything
/// else is extracted from the JSON `attributes` column.
fn label_lhs(key: &str) -> String {
    if key == "service_name" {
        "service_name".to_string()
    } else {
        format!("json_get_str(attributes, '{}')", esc(key))
    }
}

fn sql_ident(key: &str) -> String {
    key.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect()
}

fn matcher_pred(m: &Matcher) -> Option<String> {
    if m.name == "__name__" {
        return None;
    }
    let lhs = label_lhs(&m.name);
    Some(match &m.op {
        MatchOp::Equal => format!("{lhs} = '{}'", esc(&m.value)),
        MatchOp::NotEqual => format!("{lhs} <> '{}'", esc(&m.value)),
        MatchOp::Re(_) => format!("regexp_like({lhs}, '{}')", esc(&m.value)),
        MatchOp::NotRe(_) => format!("NOT regexp_like({lhs}, '{}')", esc(&m.value)),
    })
}

/// Subquery selecting, per series, the latest sample at/before `time_ns`
/// (`rn = 1`). Value is the gauge/sum numeric value.
fn latest_per_series(vs: &VectorSelector, time_ns: i64) -> Result<String, String> {
    let name = vs.name.as_deref().ok_or("metric selector requires a name")?;
    let mut preds = vec![format!("name = '{}'", esc(name))];
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_pred(m) {
            preds.push(p);
        }
    }
    preds.push(format!("CAST(time_unix_nano AS BIGINT) <= {time_ns}"));
    Ok(format!(
        "SELECT name, service_name, attributes, \
         COALESCE(double_value, CAST(int_value AS DOUBLE)) AS v, time_unix_nano, \
         row_number() OVER (PARTITION BY name, attributes ORDER BY time_unix_nano DESC) AS rn \
         FROM metrics WHERE {}",
        preds.join(" AND ")
    ))
}

fn latest_selected(vs: &VectorSelector, time_ns: i64) -> Result<String, String> {
    Ok(format!(
        "SELECT name, service_name, attributes, v, time_unix_nano FROM ({}) WHERE rn = 1",
        latest_per_series(vs, time_ns)?
    ))
}

fn agg_name(op: token::TokenType) -> Result<&'static str, String> {
    match op.id() {
        token::T_SUM => Ok("sum"),
        token::T_MAX => Ok("max"),
        token::T_MIN => Ok("min"),
        token::T_AVG => Ok("avg"),
        token::T_COUNT => Ok("count"),
        _ => Err("unsupported aggregation operator (instant: sum/max/min/avg/count)".to_string()),
    }
}

fn lower_aggregate(agg: &AggregateExpr, time_ns: i64) -> Result<String, String> {
    let vs = match agg.expr.as_ref() {
        Expr::VectorSelector(vs) => vs,
        Expr::Paren(p) => match p.expr.as_ref() {
            Expr::VectorSelector(vs) => vs,
            _ => return Err("aggregate inner must be a vector selector (instant)".to_string()),
        },
        _ => return Err("aggregate inner must be a vector selector (rate etc. is task 5)".to_string()),
    };
    let op = agg_name(agg.op)?;
    let by = match &agg.modifier {
        Some(LabelModifier::Include(labels)) => labels.labels.clone(),
        _ => return Err("aggregation requires `by (...)` grouping in v1".to_string()),
    };
    if by.is_empty() {
        return Err("aggregation requires at least one `by` label in v1".to_string());
    }
    let select_cols: Vec<String> =
        by.iter().map(|k| format!("{} AS {}", label_lhs(k), sql_ident(k))).collect();
    let group_refs: Vec<String> = by.iter().map(|k| label_lhs(k)).collect();
    Ok(format!(
        "SELECT {}, {}(v) AS v FROM ({}) GROUP BY {}",
        select_cols.join(", "),
        op,
        latest_selected(vs, time_ns)?,
        group_refs.join(", ")
    ))
}

/// Translate an instant PromQL query to SQL over the `metrics` table.
pub fn translate_instant(query: &str, time_ns: i64) -> Result<String, String> {
    lower(&parser::parse(query)?, time_ns)
}

fn lower(expr: &Expr, time_ns: i64) -> Result<String, String> {
    match expr {
        Expr::VectorSelector(vs) => latest_selected(vs, time_ns),
        Expr::Paren(p) => lower(&p.expr, time_ns),
        Expr::Aggregate(agg) => lower_aggregate(agg, time_ns),
        Expr::Call(_) => {
            Err("unsupported PromQL function for instant query (range functions are task 5)".to_string())
        }
        Expr::MatrixSelector(_) | Expr::Subquery(_) => {
            Err("range/subquery selectors require query_range (task 5)".to_string())
        }
        Expr::Binary(_) => Err("binary operators not yet supported (v1)".to_string()),
        Expr::Unary(_) => Err("unary operators not yet supported (v1)".to_string()),
        _ => Err("unsupported PromQL expression".to_string()),
    }
}

/// `SELECT DISTINCT` SQL for `label/:name/values`.
pub fn label_values_sql(label: &str) -> String {
    let lhs = label_lhs(label);
    format!("SELECT DISTINCT {lhs} AS v FROM metrics WHERE {lhs} IS NOT NULL ORDER BY v")
}

/// `SELECT DISTINCT` SQL for `series` (identifying columns).
pub fn series_sql() -> String {
    "SELECT DISTINCT name, service_name FROM metrics".to_string()
}

// --- Prometheus API response (resultType=vector) ---

/// Prometheus query response envelope.
#[derive(Debug, Serialize, Deserialize)]
pub struct PromResponse {
    /// `"success"` or `"error"`.
    pub status: String,
    /// Result payload.
    pub data: PromData,
}

/// Prometheus response data block.
#[derive(Debug, Serialize, Deserialize)]
pub struct PromData {
    /// `"vector"` for instant queries.
    #[serde(rename = "resultType")]
    pub result_type: String,
    /// One sample per series.
    pub result: Vec<PromSample>,
}

/// One instant sample: label set + `[unix_seconds, "value"]`.
#[derive(Debug, Serialize, Deserialize)]
pub struct PromSample {
    /// Series label set.
    pub metric: BTreeMap<String, String>,
    /// `[unix-seconds (float), stringified value]`.
    pub value: (f64, String),
}

impl PromResponse {
    /// Build a `vector` response from `(labels, unix_seconds, value)` samples.
    pub fn vector(samples: impl IntoIterator<Item = (BTreeMap<String, String>, f64, f64)>) -> Self {
        let result = samples
            .into_iter()
            .map(|(metric, ts, v)| PromSample { metric, value: (ts, v.to_string()) })
            .collect();
        PromResponse {
            status: "success".to_string(),
            data: PromData { result_type: "vector".to_string(), result },
        }
    }
}

// --- Engine handlers (execute SQL, shape Prometheus JSON) ---

fn to_err(e: String) -> Box<dyn std::error::Error + Send + Sync> {
    Box::<dyn std::error::Error + Send + Sync>::from(e)
}

/// Run an instant PromQL query and build a `resultType=vector` response.
///
/// The sample timestamp returned is the evaluation time (`time_ns`), per the
/// Prometheus instant-query contract — not the underlying sample time.
pub async fn handle_instant(
    engine: &super::QueryEngine,
    query: &str,
    time_ns: i64,
) -> crate::Result<PromResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Float64Type};

    let sql = translate_instant(query, time_ns).map_err(to_err)?;
    let batches = engine.sql(&sql).await?;
    // ns→seconds for the Prometheus sample timestamp; sub-ms precision is irrelevant here.
    #[allow(clippy::cast_precision_loss)]
    let time_s = time_ns as f64 / 1_000_000_000.0;

    // Columns that are not labels: the value, the raw JSON blob, internal cols.
    const NON_LABEL: [&str; 3] = ["v", "attributes", "time_unix_nano"];

    let mut samples: Vec<(BTreeMap<String, String>, f64, f64)> = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let v_idx = schema.index_of("v").map_err(|e| to_err(e.to_string()))?;
        let v = cast(batch.column(v_idx), &DataType::Float64)?;
        let v = v.as_primitive::<Float64Type>();

        // Pre-cast label columns to Utf8 so we can read any string-ish type.
        let labels: Vec<(String, _)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !NON_LABEL.contains(&f.name().as_str()))
            .map(|(i, f)| {
                let key = if f.name() == "name" { "__name__".to_string() } else { f.name().clone() };
                (key, cast(batch.column(i), &DataType::Utf8))
            })
            .collect();

        for i in 0..batch.num_rows() {
            if v.is_null(i) {
                continue;
            }
            let mut metric = BTreeMap::new();
            for (key, arr) in &labels {
                let arr = arr.as_ref().map_err(|e| to_err(e.to_string()))?;
                let arr = arr.as_string::<i32>();
                if !arr.is_null(i) {
                    metric.insert(key.clone(), arr.value(i).to_string());
                }
            }
            samples.push((metric, time_s, v.value(i)));
        }
    }
    Ok(PromResponse::vector(samples))
}

/// Run `label/:name/values` and build `{status, data:[...]}`.
pub async fn handle_label_values(
    engine: &super::QueryEngine,
    label: &str,
) -> crate::Result<serde_json::Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let batches = engine.sql(&label_values_sql(label)).await?;
    let mut values: Vec<String> = Vec::new();
    for batch in &batches {
        let col = cast(batch.column(0), &DataType::Utf8)?;
        let col = col.as_string::<i32>();
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                values.push(col.value(i).to_string());
            }
        }
    }
    Ok(serde_json::json!({ "status": "success", "data": values }))
}

/// Run `series` and build `{status, data:[{__name__, service_name}, ...]}`.
pub async fn handle_series(engine: &super::QueryEngine) -> crate::Result<serde_json::Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let batches = engine.sql(&series_sql()).await?;
    let mut series: Vec<BTreeMap<String, String>> = Vec::new();
    for batch in &batches {
        let name_arr = cast(batch.column(0), &DataType::Utf8)?;
        let name = name_arr.as_string::<i32>();
        let svc_arr = cast(batch.column(1), &DataType::Utf8)?;
        let svc = svc_arr.as_string::<i32>();
        for i in 0..batch.num_rows() {
            let mut m = BTreeMap::new();
            if !name.is_null(i) {
                m.insert("__name__".to_string(), name.value(i).to_string());
            }
            if !svc.is_null(i) {
                m.insert("service_name".to_string(), svc.value(i).to_string());
            }
            series.push(m);
        }
    }
    Ok(serde_json::json!({ "status": "success", "data": series }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_promql_instant_selector_to_sql() {
        let sql = translate_instant(r#"node_memory_total_bytes{host="h1"}"#, 1000).unwrap();
        assert!(sql.contains("name = 'node_memory_total_bytes'"), "sql: {sql}");
        assert!(sql.contains("json_get_str(attributes, 'host') = 'h1'"), "sql: {sql}");
        assert!(sql.contains("CAST(time_unix_nano AS BIGINT) <= 1000"));
        assert!(sql.contains("WHERE rn = 1"));
    }

    #[test]
    fn test_promql_sum_by_label_groups_on_json_extract() {
        let sql = translate_instant(r#"sum by (le) (http_bucket{service_name="client"})"#, 5).unwrap();
        assert!(sql.contains("sum(v) AS v"), "sql: {sql}");
        assert!(sql.contains("json_get_str(attributes, 'le')"), "sql: {sql}");
        assert!(sql.contains("GROUP BY json_get_str(attributes, 'le')"), "sql: {sql}");
        assert!(sql.contains("service_name = 'client'"), "sql: {sql}");
    }

    #[test]
    fn test_promql_unsupported_fn_returns_error() {
        assert!(translate_instant("rate(http_total[1m])", 0).is_err());
        assert!(translate_instant("predict_linear(x[1h], 60)", 0).is_err());
        // never panics on a parse error either
        assert!(translate_instant("{{bad", 0).is_err());
    }

    #[test]
    fn test_label_values_and_series_sql() {
        assert_eq!(
            label_values_sql("service_name"),
            "SELECT DISTINCT service_name AS v FROM metrics WHERE service_name IS NOT NULL ORDER BY v"
        );
        assert!(label_values_sql("http_route").contains("json_get_str(attributes, 'http_route')"));
        assert!(series_sql().contains("SELECT DISTINCT name, service_name FROM metrics"));
    }

    #[test]
    fn test_prom_vector_response_shape() {
        let mut m = BTreeMap::new();
        m.insert("service_name".to_string(), "client".to_string());
        let resp = PromResponse::vector([(m, 1700000000.0, 42.0)]);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""resultType":"vector""#), "json: {json}");
        assert!(json.contains(r#""value":[1700000000.0,"42"]"#), "json: {json}");
    }
}
