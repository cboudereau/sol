//! PromQL → SQL (instant queries) + Prometheus API response types (task 4).
//!
//! Parses PromQL with the `promql-parser` crate and translates instant vector
//! selectors + simple `<agg> by (...)` aggregations to SQL over the `metrics`
//! table. Unsupported expressions (range functions, binary ops, subqueries)
//! return an error, never a panic — per [QUERY-MAPPING.md](../../../docs/workspace/parquet-backend/QUERY-MAPPING.md)
//! and [API-SPEC.md](../../../docs/workspace/parquet-backend/API-SPEC.md) §1.

use std::collections::BTreeMap;
use std::time::Duration;

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{
    self, AggregateExpr, Call, Expr, LabelModifier, VectorSelector, token,
};
use serde::{Deserialize, Serialize};

fn esc(v: &str) -> String {
    v.replace('\'', "''")
}

/// Left-hand side for a label: promoted `service_name` is a column; everything
/// else is extracted from the JSON `attributes` column via `prom_attr`, which
/// matches the Prometheus-normalized name against the raw OTLP key.
fn label_lhs(key: &str) -> String {
    if key == "service_name" {
        "service_name".to_string()
    } else {
        format!("prom_attr(attributes, '{}')", esc(key))
    }
}

fn sql_ident(key: &str) -> String {
    key.chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect()
}

/// Resolve a metric selector to `(value_expr, name_predicate)`, synthesizing the
/// classic histogram component series from the OTLP histogram columns:
/// `<base>_count` → the `count` column, `<base>_sum` → the `sum` column (both
/// guarded to histogram rows), anything else → the gauge/counter value. The
/// name predicate also matches a real metric named exactly `<name>`.
fn metric_value_and_match(name: &str) -> (String, String) {
    let exact = format!(
        "prom_metric_name(name, unit, is_monotonic) = '{}'",
        esc(name)
    );
    let hist = |base: &str| {
        format!(
            "prom_metric_name(name, unit, is_monotonic) = '{}' AND bucket_counts IS NOT NULL",
            esc(base)
        )
    };
    if let Some(base) = name.strip_suffix("_count") {
        (
            "COALESCE(double_value, CAST(int_value AS DOUBLE), CAST(\"count\" AS DOUBLE))"
                .to_string(),
            format!("({exact} OR ({}))", hist(base)),
        )
    } else if let Some(base) = name.strip_suffix("_sum") {
        (
            "COALESCE(double_value, \"sum\")".to_string(),
            format!("({exact} OR ({}))", hist(base)),
        )
    } else {
        (
            "COALESCE(double_value, CAST(int_value AS DOUBLE))".to_string(),
            exact,
        )
    }
}

fn matcher_pred(m: &Matcher) -> Option<String> {
    if m.name == "__name__" {
        return None;
    }
    let lhs = label_lhs(&m.name);
    // Prometheus matcher semantics: an absent label behaves like the empty
    // string. So `=""` matches absent/empty; `!="v"` matches absent; regex
    // matchers test the value with absent coerced to '' (so `=~".*"` matches
    // everything, including series lacking the label).
    let v = esc(&m.value);
    Some(match &m.op {
        MatchOp::Equal if m.value.is_empty() => format!("({lhs} IS NULL OR {lhs} = '')"),
        MatchOp::Equal => format!("{lhs} = '{v}'"),
        MatchOp::NotEqual if m.value.is_empty() => format!("({lhs} IS NOT NULL AND {lhs} <> '')"),
        MatchOp::NotEqual => format!("({lhs} IS NULL OR {lhs} <> '{v}')"),
        // Prometheus fully anchors regex matchers (`^(?:RE)$`); DataFusion
        // regexp_like is unanchored (substring), so anchor explicitly or
        // `pod=~"web"` would wrongly match `web-1`.
        MatchOp::Re(_) => format!("regexp_like(COALESCE({lhs}, ''), '^(?:{v})$')"),
        MatchOp::NotRe(_) => format!("NOT regexp_like(COALESCE({lhs}, ''), '^(?:{v})$')"),
    })
}

/// Subquery selecting, per series, the latest sample at/before `time_ns`
/// (`rn = 1`). Value is the gauge/sum numeric value.
fn latest_per_series(vs: &VectorSelector, time_ns: i64) -> Result<String, String> {
    let name = vs
        .name
        .as_deref()
        .ok_or("metric selector requires a name")?;
    let (value_expr, name_pred) = metric_value_and_match(name);
    let mut preds = vec![name_pred];
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_pred(m) {
            preds.push(p);
        }
    }
    preds.push(format!("CAST(time_unix_nano AS BIGINT) <= {time_ns}"));
    Ok(format!(
        "SELECT name, service_name, attributes, \
         {value_expr} AS v, time_unix_nano, \
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
        _ => {
            return Err(
                "aggregate inner must be a vector selector (rate etc. is task 5)".to_string(),
            );
        }
    };
    let op = agg_name(agg.op)?;
    let by = match &agg.modifier {
        Some(LabelModifier::Include(labels)) => labels.labels.clone(),
        Some(LabelModifier::Exclude(_)) => {
            return Err("`without (...)` aggregation not supported (v1)".to_string());
        }
        None => Vec::new(), // bare `sum(...)` → aggregate across all series
    };
    let inner = latest_selected(vs, time_ns)?;
    if by.is_empty() {
        return Ok(format!("SELECT {op}(v) AS v FROM ({inner})"));
    }
    let select_cols: Vec<String> = by
        .iter()
        .map(|k| format!("{} AS {}", label_lhs(k), sql_ident(k)))
        .collect();
    let group_refs: Vec<String> = by.iter().map(|k| label_lhs(k)).collect();
    Ok(format!(
        "SELECT {}, {}(v) AS v FROM ({}) GROUP BY {}",
        select_cols.join(", "),
        op,
        inner,
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
        Expr::Call(_) => Err(
            "unsupported PromQL function for instant query (range functions are task 5)"
                .to_string(),
        ),
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
    if label == "__name__" {
        // Metric-name discovery (Grafana's metric browser): the normalized
        // base names plus the synthetic `_bucket`/`_count`/`_sum` series
        // exposed for histogram metrics.
        return "SELECT DISTINCT v FROM ( \
                SELECT prom_metric_name(name, unit, is_monotonic) AS v FROM metrics \
                UNION ALL SELECT prom_metric_name(name, unit, is_monotonic) || '_bucket' FROM metrics WHERE bucket_counts IS NOT NULL \
                UNION ALL SELECT prom_metric_name(name, unit, is_monotonic) || '_count' FROM metrics WHERE bucket_counts IS NOT NULL \
                UNION ALL SELECT prom_metric_name(name, unit, is_monotonic) || '_sum' FROM metrics WHERE bucket_counts IS NOT NULL \
            ) AS t WHERE v IS NOT NULL ORDER BY v"
            .to_string();
    }
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
            .map(|(metric, ts, v)| PromSample {
                metric,
                value: (ts, v.to_string()),
            })
            .collect();
        PromResponse {
            status: "success".to_string(),
            data: PromData {
                result_type: "vector".to_string(),
                result,
            },
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

    // histogram_quantile is computed Rust-native from OTLP bucket arrays.
    let expr = parser::parse(query).map_err(to_err)?;
    if let Some((phi, vs)) = histogram_quantile_parts(&expr) {
        return handle_histogram(engine, phi, vs, time_ns).await;
    }

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
                let key = if f.name() == "name" {
                    "__name__".to_string()
                } else {
                    f.name().clone()
                };
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

/// Run a single-string-column SQL and collect the non-null values. Shared by
/// the label/tag discovery endpoints (Prometheus, Loki).
pub(super) async fn string_column(
    engine: &super::QueryEngine,
    sql: &str,
) -> crate::Result<Vec<String>> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let batches = engine.sql(sql).await?;
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
    Ok(values)
}

/// Distinct raw attribute keys across a JSON object column. Bounded by label-set
/// cardinality: only each *distinct* blob is parsed.
pub(super) async fn distinct_json_keys(
    engine: &super::QueryEngine,
    table: &str,
    column: &str,
) -> crate::Result<std::collections::BTreeSet<String>> {
    // Cap the distinct blobs scanned: label/tag discovery is bounded by
    // label-set cardinality, but a high-cardinality attribute (e.g. a per-request
    // id embedded in the JSON) would otherwise make this an unbounded scan +
    // parse. 10k distinct blobs is far more label sets than any real schema.
    const MAX_DISTINCT_BLOBS: usize = 10_000;
    let sql = format!(
        "SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL LIMIT {MAX_DISTINCT_BLOBS}"
    );
    let mut keys = std::collections::BTreeSet::new();
    for blob in string_column(engine, &sql).await? {
        if let Ok(serde_json::Value::Object(map)) = serde_json::from_str(&blob) {
            keys.extend(map.keys().cloned());
        }
    }
    Ok(keys)
}

/// Run `label/:name/values` and build `{status, data:[...]}`.
pub async fn handle_label_values(
    engine: &super::QueryEngine,
    label: &str,
) -> crate::Result<serde_json::Value> {
    let values = string_column(engine, &label_values_sql(label)).await?;
    Ok(serde_json::json!({ "status": "success", "data": values }))
}

/// Run `labels` (label-name discovery for Grafana's metric browser): the
/// promoted columns plus the Prometheus-normalized metric attribute keys.
pub async fn handle_labels(engine: &super::QueryEngine) -> crate::Result<serde_json::Value> {
    let keys = distinct_json_keys(engine, "metrics", "attributes").await?;
    let mut names: std::collections::BTreeSet<String> =
        ["__name__".to_string(), "service_name".to_string()].into();
    names.extend(keys.into_iter().map(|k| super::udf::normalize(&k)));
    let names: Vec<String> = names.into_iter().collect();
    Ok(serde_json::json!({ "status": "success", "data": names }))
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

// --- Range queries (query_range → resultType=matrix) ---

/// Base per-sample selection over `metrics` for a range query: exposes the
/// grouping columns plus a numeric `v` (gauge/counter value) and the time.
fn metric_base(
    vs: &VectorSelector,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> Result<String, String> {
    let name = vs
        .name
        .as_deref()
        .ok_or("metric selector requires a name")?;
    let (value_expr, name_pred) = metric_value_and_match(name);
    let mut preds = vec![name_pred];
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_pred(m) {
            preds.push(p);
        }
    }
    preds.push(format!(
        "CAST(time_unix_nano AS BIGINT) BETWEEN {start_ns} AND {end_ns}"
    ));
    Ok(format!(
        "SELECT name, service_name, attributes, time_unix_nano, \
         {value_expr} AS v FROM {table} WHERE {}",
        preds.join(" AND ")
    ))
}

/// `rate(m[d])` — per-sample delta via `LAG` over the series window. Counter
/// resets (`v < prev_v`) use the current value as the delta (simplified, per
/// the PromQL ADR). The range `[d]` bounds the outer time filter only.
fn rate_sql(
    vs: &VectorSelector,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> Result<String, String> {
    let base = metric_base(vs, start_ns, end_ns, table)?;
    Ok(format!(
        "WITH ordered AS (SELECT name, service_name, attributes, time_unix_nano, v, \
         LAG(v) OVER w AS prev_v, LAG(CAST(time_unix_nano AS BIGINT)) OVER w AS prev_t \
         FROM ({base}) WINDOW w AS (PARTITION BY name, service_name, attributes ORDER BY time_unix_nano)) \
         SELECT service_name, attributes, time_unix_nano, \
         CASE WHEN v >= prev_v THEN (v - prev_v) ELSE v END \
         / ((CAST(time_unix_nano AS BIGINT) - prev_t) / 1e9) AS v \
         FROM ordered WHERE prev_t IS NOT NULL \
         AND CAST(time_unix_nano AS BIGINT) <> prev_t"
    ))
}

/// `<agg>_over_time(m[d])` — a sliding window aggregate over the last `d`.
fn over_time_sql(
    vs: &VectorSelector,
    range: Duration,
    start_ns: i64,
    end_ns: i64,
    agg: &str,
    table: &str,
) -> Result<String, String> {
    let base = metric_base(vs, start_ns, end_ns, table)?;
    let range_ns = i64::try_from(range.as_nanos()).unwrap_or(i64::MAX);
    Ok(format!(
        "SELECT service_name, attributes, time_unix_nano, \
         {agg}(v) OVER (PARTITION BY name, service_name, attributes \
         ORDER BY CAST(time_unix_nano AS BIGINT) \
         RANGE BETWEEN {range_ns} PRECEDING AND CURRENT ROW) AS v FROM ({base})"
    ))
}

fn lower_call(c: &Call, start_ns: i64, end_ns: i64, table: &str) -> Result<String, String> {
    let (vs, range) = match c.args.args.first().map(|b| b.as_ref()) {
        Some(Expr::MatrixSelector(ms)) => (&ms.vs, ms.range),
        _ => {
            return Err(format!(
                "{}() expects a range-vector argument like m[5m]",
                c.func.name
            ));
        }
    };
    match c.func.name {
        "rate" | "irate" | "increase" => rate_sql(vs, start_ns, end_ns, table),
        "max_over_time" => over_time_sql(vs, range, start_ns, end_ns, "MAX", table),
        "min_over_time" => over_time_sql(vs, range, start_ns, end_ns, "MIN", table),
        "avg_over_time" => over_time_sql(vs, range, start_ns, end_ns, "AVG", table),
        "sum_over_time" => over_time_sql(vs, range, start_ns, end_ns, "SUM", table),
        "count_over_time" => over_time_sql(vs, range, start_ns, end_ns, "COUNT", table),
        other => Err(format!("unsupported range function: {other}() (v1)")),
    }
}

fn as_count(expr: &Expr) -> Result<i64, String> {
    match expr {
        // Prometheus requires an integer scalar here; reject non-integral or
        // out-of-range values rather than silently truncating (e.g. topk(2.9,…)
        // → 2, or 1e30 → i64::MAX).
        Expr::NumberLiteral(n) => {
            if !n.val.is_finite() || n.val.fract() != 0.0 || n.val.abs() >= 9.007e15 {
                return Err(format!(
                    "topk/bottomk count must be an integer, got {}",
                    n.val
                ));
            }
            #[allow(clippy::cast_possible_truncation)]
            Ok(n.val as i64)
        }
        _ => Err("topk/bottomk requires a scalar count parameter".to_string()),
    }
}

fn lower_range_aggregate(
    agg: &AggregateExpr,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> Result<String, String> {
    // topk/bottomk: order the inner series by value and limit.
    if agg.op.id() == token::T_TOPK || agg.op.id() == token::T_BOTTOMK {
        let n = as_count(
            agg.param
                .as_deref()
                .ok_or("topk/bottomk requires a count")?,
        )?;
        let inner = lower_range(agg.expr.as_ref(), start_ns, end_ns, table)?;
        let dir = if agg.op.id() == token::T_TOPK {
            "DESC"
        } else {
            "ASC"
        };
        return Ok(format!(
            "SELECT * FROM ({inner}) ORDER BY v {dir} LIMIT {n}"
        ));
    }
    // sum/max/min/avg/count [by (...)] over a range expression.
    let op = agg_name(agg.op)?;
    let by = match &agg.modifier {
        Some(LabelModifier::Include(labels)) => labels.labels.clone(),
        Some(LabelModifier::Exclude(_)) => {
            return Err("`without (...)` aggregation not supported (v1)".to_string());
        }
        None => Vec::new(), // bare `sum(rate(...))` → aggregate across all series
    };
    let inner = lower_range(agg.expr.as_ref(), start_ns, end_ns, table)?;
    if by.is_empty() {
        return Ok(format!(
            "SELECT time_unix_nano, {op}(v) AS v FROM ({inner}) GROUP BY time_unix_nano"
        ));
    }
    let select_cols: Vec<String> = by
        .iter()
        .map(|k| format!("{} AS {}", label_lhs(k), sql_ident(k)))
        .collect();
    let group_refs: Vec<String> = by.iter().map(|k| label_lhs(k)).collect();
    Ok(format!(
        "SELECT {}, time_unix_nano, {}(v) AS v FROM ({}) GROUP BY {}, time_unix_nano",
        select_cols.join(", "),
        op,
        inner,
        group_refs.join(", ")
    ))
}

fn lower_range(expr: &Expr, start_ns: i64, end_ns: i64, table: &str) -> Result<String, String> {
    match expr {
        Expr::Call(c) => lower_call(c, start_ns, end_ns, table),
        Expr::Paren(p) => lower_range(&p.expr, start_ns, end_ns, table),
        Expr::Aggregate(agg) => lower_range_aggregate(agg, start_ns, end_ns, table),
        Expr::VectorSelector(_) => Err(
            "range query needs a function over a range vector, e.g. rate(m[5m]) (v1)".to_string(),
        ),
        Expr::Binary(_) | Expr::Unary(_) => {
            Err("binary/unary operators not yet supported for query_range (v1)".to_string())
        }
        _ => Err("unsupported PromQL expression for query_range (v1)".to_string()),
    }
}

/// Translate a range PromQL query to SQL over the `metrics` table. The `step`
/// is applied by the caller (no SQL-side resampling in v1).
pub fn translate_range(query: &str, start_ns: i64, end_ns: i64) -> Result<String, String> {
    translate_range_on(query, start_ns, end_ns, "metrics")
}

/// Like [`translate_range`] but targeting an explicit table — the query-frontend
/// passes a rollup tier table (`metrics_5m`/`metrics_1h`/`metrics_1d`) for coarse
/// long-range queries (FR6).
pub fn translate_range_on(
    query: &str,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> Result<String, String> {
    lower_range(&parser::parse(query)?, start_ns, end_ns, table)
}

/// Prometheus `query_range` response envelope (`resultType=matrix`).
#[derive(Debug, Serialize, Deserialize)]
pub struct PromMatrixResponse {
    /// `"success"` or `"error"`.
    pub status: String,
    /// Result payload.
    pub data: PromMatrixData,
}

/// Matrix response data block.
#[derive(Debug, Serialize, Deserialize)]
pub struct PromMatrixData {
    /// Always `"matrix"` for range queries.
    #[serde(rename = "resultType")]
    pub result_type: String,
    /// One range series per label set.
    pub result: Vec<PromRange>,
}

/// One range series: a label set plus its `[unix_seconds, "value"]` points.
#[derive(Debug, Serialize, Deserialize)]
pub struct PromRange {
    /// Series label set.
    pub metric: BTreeMap<String, String>,
    /// `[unix-seconds, stringified value]` pairs, ascending by time.
    pub values: Vec<(f64, String)>,
}

impl PromMatrixResponse {
    /// Build a `matrix` response from `(labels, [(unix_seconds, value), …])`.
    pub fn matrix(
        series: impl IntoIterator<Item = (BTreeMap<String, String>, Vec<(f64, f64)>)>,
    ) -> Self {
        let result = series
            .into_iter()
            .map(|(metric, points)| PromRange {
                metric,
                values: points
                    .into_iter()
                    .map(|(t, v)| (t, v.to_string()))
                    .collect(),
            })
            .collect();
        PromMatrixResponse {
            status: "success".to_string(),
            data: PromMatrixData {
                result_type: "matrix".to_string(),
                result,
            },
        }
    }
}

/// A grouped range result: label-set debug key → (label set, time-ordered points).
type RangeSeries = BTreeMap<String, (BTreeMap<String, String>, Vec<(f64, f64)>)>;

/// Run a range PromQL query over a single `[start_ns, end_ns)` window and group
/// the rows into per-series point lists.
async fn range_series(
    engine: &super::QueryEngine,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> crate::Result<RangeSeries> {
    let sql = translate_range_on(query, start_ns, end_ns, table).map_err(to_err)?;
    range_series_sql(engine, &sql).await
}

/// Group the rows of an already-built range SQL into per-series point lists.
async fn range_series_sql(engine: &super::QueryEngine, sql: &str) -> crate::Result<RangeSeries> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type};

    let batches = engine.sql(sql).await?;

    const NON_LABEL: [&str; 3] = ["v", "attributes", "time_unix_nano"];

    // Group points by their (ordered) label set; BTreeMap key keeps it stable.
    let mut series: RangeSeries = BTreeMap::new();
    for batch in &batches {
        let schema = batch.schema();
        let v_idx = schema.index_of("v").map_err(|e| to_err(e.to_string()))?;
        let v = cast(batch.column(v_idx), &DataType::Float64)?;
        let v = v.as_primitive::<Float64Type>();
        let t_idx = schema
            .index_of("time_unix_nano")
            .map_err(|e| to_err(e.to_string()))?;
        let t = cast(batch.column(t_idx), &DataType::Int64)?;
        let t = t.as_primitive::<Int64Type>();

        let labels: Vec<(String, _)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !NON_LABEL.contains(&f.name().as_str()))
            .map(|(i, f)| {
                let key = if f.name() == "name" {
                    "__name__".to_string()
                } else {
                    f.name().clone()
                };
                (key, cast(batch.column(i), &DataType::Utf8))
            })
            .collect();

        for i in 0..batch.num_rows() {
            if v.is_null(i) || t.is_null(i) {
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
            #[allow(clippy::cast_precision_loss)]
            let ts_s = t.value(i) as f64 / 1_000_000_000.0;
            let key = format!("{metric:?}");
            series
                .entry(key)
                .or_insert_with(|| (metric, Vec::new()))
                .1
                .push((ts_s, v.value(i)));
        }
    }
    Ok(series)
}

// --- Classic-histogram synthesis from OTLP array histograms (#4) ---
//
// Mimir explodes OTLP histograms into classic `<base>_bucket{le}`/`_count`/`_sum`
// series; Sol stores the native OTLP histogram (bucket_counts + explicit_bounds
// arrays). The dashboards send `histogram_quantile(φ, sum(rate(<base>_bucket[d]))
// by (le, G))` (often under `topk`). We recognise that shape, match the OTLP
// histogram by its normalized base name, and compute the quantile per (group G,
// timestamp) from the arrays.

/// A recognised classic-histogram quantile query (owned, no AST borrow).
struct HistSpec {
    phi: f64,
    base: String,       // normalized base name (without `_bucket`)
    preds: Vec<String>, // matcher predicates (excluding `le`)
    group_by: Vec<String>,
    topk: Option<(i64, bool)>, // (n, is_topk) — bottomk when false
}

/// Find the underlying histogram selector + `by(...)` labels (minus `le`).
fn find_hist_base(expr: &Expr) -> Option<(&VectorSelector, Vec<String>)> {
    match expr {
        Expr::Paren(p) => find_hist_base(&p.expr),
        Expr::Aggregate(agg) => {
            let mut gb = match &agg.modifier {
                Some(LabelModifier::Include(l)) => l.labels.clone(),
                _ => Vec::new(),
            };
            gb.retain(|x| x != "le");
            let (vs, _) = find_hist_base(agg.expr.as_ref())?;
            Some((vs, gb))
        }
        Expr::Call(c) => match c.args.args.first().map(|b| b.as_ref()) {
            Some(Expr::MatrixSelector(ms)) => Some((&ms.vs, Vec::new())),
            Some(Expr::VectorSelector(vs)) => Some((vs, Vec::new())),
            Some(other) => find_hist_base(other),
            None => None,
        },
        Expr::MatrixSelector(ms) => Some((&ms.vs, Vec::new())),
        Expr::VectorSelector(vs) => Some((vs, Vec::new())),
        _ => None,
    }
}

/// Detect `histogram_quantile(φ, …)` (optionally under `topk`/`bottomk`).
fn detect_hist_quantile(expr: &Expr) -> Option<HistSpec> {
    match expr {
        Expr::Paren(p) => detect_hist_quantile(&p.expr),
        Expr::Aggregate(agg) if agg.op.id() == token::T_TOPK || agg.op.id() == token::T_BOTTOMK => {
            let n = as_count(agg.param.as_deref()?).ok()?;
            let mut spec = detect_hist_quantile(agg.expr.as_ref())?;
            spec.topk = Some((n, agg.op.id() == token::T_TOPK));
            Some(spec)
        }
        Expr::Call(c) if c.func.name == "histogram_quantile" => {
            let phi = match c.args.args.first().map(|b| b.as_ref()) {
                Some(Expr::NumberLiteral(n)) => n.val,
                _ => return None,
            };
            let inner = c.args.args.get(1).map(|b| b.as_ref())?;
            let (vs, group_by) = find_hist_base(inner)?;
            let name = vs.name.as_deref()?;
            let base = name.strip_suffix("_bucket").unwrap_or(name).to_string();
            let preds = vs
                .matchers
                .matchers
                .iter()
                .filter_map(matcher_pred)
                .collect();
            Some(HistSpec {
                phi,
                base,
                preds,
                group_by,
                topk: None,
            })
        }
        _ => None,
    }
}

/// Serve a classic-histogram quantile query from OTLP array histograms: match
/// the histogram by normalized base name, sum bucket counts per (group, ts),
/// and interpolate the quantile from the arrays.
async fn handle_hist_quantile_range(
    engine: &super::QueryEngine,
    spec: &HistSpec,
    start_ns: i64,
    end_ns: i64,
) -> crate::Result<PromMatrixResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Int64Type};

    let mut preds = vec![format!(
        "prom_metric_name(name, unit, is_monotonic) = '{}'",
        esc(&spec.base)
    )];
    preds.extend(spec.preds.iter().cloned());
    preds.push(format!(
        "CAST(time_unix_nano AS BIGINT) BETWEEN {start_ns} AND {end_ns}"
    ));
    let group_cols: String = spec
        .group_by
        .iter()
        .map(|g| format!(", {} AS {}", label_lhs(g), sql_ident(g)))
        .collect();
    let sql = format!(
        "SELECT time_unix_nano, bucket_counts, explicit_bounds{group_cols} FROM metrics WHERE {}",
        preds.join(" AND ")
    );
    let batches = engine.sql(&sql).await?;

    // group key → (label map, ts → summed bucket counts, bounds)
    type Group = (BTreeMap<String, String>, BTreeMap<i64, Vec<f64>>, Vec<f64>);
    let mut groups: BTreeMap<String, Group> = BTreeMap::new();
    for batch in &batches {
        let t = cast(batch.column(0), &DataType::Int64)?;
        let t = t.as_primitive::<Int64Type>();
        let bc = batch.column(1).as_string::<i32>();
        let eb = batch.column(2).as_string::<i32>();
        let labels: Vec<(String, _)> = spec
            .group_by
            .iter()
            .enumerate()
            .map(|(j, g)| (g.clone(), cast(batch.column(3 + j), &DataType::Utf8)))
            .collect();
        for i in 0..batch.num_rows() {
            if t.is_null(i) {
                continue;
            }
            let counts = parse_f64_array((!bc.is_null(i)).then(|| bc.value(i)));
            let bounds = parse_f64_array((!eb.is_null(i)).then(|| eb.value(i)));
            if counts.is_empty() {
                continue;
            }
            let mut metric = BTreeMap::new();
            for (name, arr) in &labels {
                let arr = arr.as_ref().map_err(|e| to_err(e.to_string()))?;
                let arr = arr.as_string::<i32>();
                if !arr.is_null(i) {
                    metric.insert(name.clone(), arr.value(i).to_string());
                }
            }
            let key = format!("{metric:?}");
            let entry = groups
                .entry(key)
                .or_insert_with(|| (metric, BTreeMap::new(), bounds.clone()));
            let acc = entry
                .1
                .entry(t.value(i))
                .or_insert_with(|| vec![0.0; counts.len()]);
            if acc.len() < counts.len() {
                acc.resize(counts.len(), 0.0);
            }
            for (a, c) in acc.iter_mut().zip(&counts) {
                *a += c;
            }
        }
    }

    #[allow(clippy::cast_precision_loss)]
    let to_secs = |ns: i64| ns as f64 / 1_000_000_000.0;
    let mut series: Vec<(BTreeMap<String, String>, Vec<(f64, f64)>)> = groups
        .into_values()
        .map(|(metric, by_ts, bounds)| {
            let mut points: Vec<(f64, f64)> = by_ts
                .into_iter()
                .filter_map(|(ts, counts)| {
                    histogram_quantile(spec.phi, &counts, &bounds).map(|q| (to_secs(ts), q))
                })
                .collect();
            points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            (metric, points)
        })
        .collect();

    if let Some((n, is_topk)) = spec.topk {
        let last = |p: &Vec<(f64, f64)>| p.last().map(|x| x.1).unwrap_or(f64::MIN);
        series.sort_by(|a, b| {
            let (la, lb) = (last(&a.1), last(&b.1));
            if is_topk {
                lb.partial_cmp(&la)
            } else {
                la.partial_cmp(&lb)
            }
            .unwrap_or(std::cmp::Ordering::Equal)
        });
        series.truncate(usize::try_from(n.max(0)).unwrap_or(usize::MAX));
    }
    Ok(PromMatrixResponse::matrix(series))
}

// --- Classic `_bucket{le}` heatmap synthesis (#4, heatmap panels) ---
//
// `sum(rate(<base>_bucket[d])) by (le[, G])` (no histogram_quantile — a heatmap)
// needs the classic per-`le` *cumulative* bucket series. We explode the OTLP
// `bucket_counts`/`explicit_bounds` arrays into per-`le` cumulative counts and
// emit a per-`(le, G)` rate series.

/// A recognised `_bucket`-by-`le` heatmap query.
struct BucketSpec {
    base: String,
    preds: Vec<String>,
    group_by: Vec<String>, // extra grouping labels (the `by` set minus `le`)
}

/// Detect `sum(rate(<base>_bucket[d])) by (le[, G])` (no histogram_quantile).
fn detect_bucket_heatmap(expr: &Expr) -> Option<BucketSpec> {
    let agg = match expr {
        Expr::Paren(p) => return detect_bucket_heatmap(&p.expr),
        Expr::Aggregate(a) => a,
        _ => return None,
    };
    // must group by `le` (the bucket dimension)
    let by = match &agg.modifier {
        Some(LabelModifier::Include(l)) => l.labels.clone(),
        _ => return None,
    };
    if !by.iter().any(|l| l == "le") {
        return None;
    }
    let (vs, _) = find_hist_base(agg.expr.as_ref())?;
    let name = vs.name.as_deref()?;
    let base = name.strip_suffix("_bucket")?.to_string(); // only `_bucket` selectors
    let preds = vs
        .matchers
        .matchers
        .iter()
        .filter_map(matcher_pred)
        .collect();
    let group_by: Vec<String> = by.into_iter().filter(|l| l != "le").collect();
    Some(BucketSpec {
        base,
        preds,
        group_by,
    })
}

/// Serve a `_bucket`-by-`le` heatmap from OTLP array histograms: explode each
/// row to per-`le` cumulative counts, then emit a per-`(le, G)` rate series
/// (consecutive-sample delta, counter-reset aware — like `rate_sql`).
#[allow(clippy::cast_precision_loss)] // ns→seconds; sub-ms precision irrelevant
async fn handle_bucket_heatmap(
    engine: &super::QueryEngine,
    spec: &BucketSpec,
    start_ns: i64,
    end_ns: i64,
) -> crate::Result<PromMatrixResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Int64Type};

    let mut preds = vec![
        format!(
            "prom_metric_name(name, unit, is_monotonic) = '{}'",
            esc(&spec.base)
        ),
        "bucket_counts IS NOT NULL".to_string(),
    ];
    preds.extend(spec.preds.iter().cloned());
    preds.push(format!(
        "CAST(time_unix_nano AS BIGINT) BETWEEN {start_ns} AND {end_ns}"
    ));
    let group_cols: String = spec
        .group_by
        .iter()
        .map(|g| format!(", {} AS {}", label_lhs(g), sql_ident(g)))
        .collect();
    let sql = format!(
        "SELECT time_unix_nano, bucket_counts, explicit_bounds{group_cols} FROM metrics WHERE {}",
        preds.join(" AND ")
    );
    let batches = engine.sql(&sql).await?;

    // (G-values + le) → ts → cumulative count
    let mut series: BTreeMap<String, (BTreeMap<String, String>, BTreeMap<i64, f64>)> =
        BTreeMap::new();
    for batch in &batches {
        let t = cast(batch.column(0), &DataType::Int64)?;
        let t = t.as_primitive::<Int64Type>();
        let bc = batch.column(1).as_string::<i32>();
        let eb = batch.column(2).as_string::<i32>();
        let glabels: Vec<(String, _)> = spec
            .group_by
            .iter()
            .enumerate()
            .map(|(j, g)| (g.clone(), cast(batch.column(3 + j), &DataType::Utf8)))
            .collect();
        for i in 0..batch.num_rows() {
            if t.is_null(i) || bc.is_null(i) {
                continue;
            }
            let counts = parse_f64_array(Some(bc.value(i)));
            let bounds = parse_f64_array((!eb.is_null(i)).then(|| eb.value(i)));
            if counts.is_empty() {
                continue;
            }
            // base label set G for this row
            let mut base_metric = BTreeMap::new();
            for (name, arr) in &glabels {
                let arr = arr.as_ref().map_err(|e| to_err(e.to_string()))?;
                let arr = arr.as_string::<i32>();
                if !arr.is_null(i) {
                    base_metric.insert(name.clone(), arr.value(i).to_string());
                }
            }
            // cumulative counts → classic `_bucket{le}` (le = bound, last = +Inf)
            let mut cum = 0.0;
            for (idx, c) in counts.iter().enumerate() {
                cum += c;
                let le = bounds
                    .get(idx)
                    .map_or_else(|| "+Inf".to_string(), |b| format!("{b}"));
                let mut metric = base_metric.clone();
                metric.insert("le".to_string(), le);
                let key = format!("{metric:?}");
                let entry = series
                    .entry(key)
                    .or_insert_with(|| (metric, BTreeMap::new()));
                *entry.1.entry(t.value(i)).or_insert(0.0) += cum;
            }
        }
    }

    let to_secs = |ns: i64| ns as f64 / 1_000_000_000.0;
    let out = series.into_values().map(|(metric, by_ts)| {
        // per-le rate: consecutive-sample delta / dt (counter-reset aware)
        let pts: Vec<(i64, f64)> = by_ts.into_iter().collect(); // BTreeMap → ts-sorted
        let mut rated = Vec::new();
        for w in pts.windows(2) {
            let (t0, v0) = w[0];
            let (t1, v1) = w[1];
            let dt = (t1 - t0) as f64 / 1_000_000_000.0;
            if dt > 0.0 {
                let delta = if v1 >= v0 { v1 - v0 } else { v1 };
                rated.push((to_secs(t1), delta / dt));
            }
        }
        (metric, rated)
    });
    Ok(PromMatrixResponse::matrix(out))
}

/// If `expr` is `topk(n, inner)` / `bottomk(n, inner)`, return
/// `(n, is_topk, &inner)`. Prometheus `topk` selects the top-N *series* (each
/// returned with all its points) — applied in Rust over the matrix, not as a
/// SQL row `LIMIT` (which would collapse series to N scattered points). The
/// inner is returned as a borrowed AST node so we translate it directly (no
/// `Display`→re-parse round-trip, which could mangle matcher values).
fn topk_parts(expr: &Expr) -> Option<(i64, bool, &Expr)> {
    match expr {
        Expr::Paren(p) => topk_parts(&p.expr),
        Expr::Aggregate(agg) if agg.op.id() == token::T_TOPK || agg.op.id() == token::T_BOTTOMK => {
            let n = as_count(agg.param.as_deref()?).ok()?;
            Some((n, agg.op.id() == token::T_TOPK, agg.expr.as_ref()))
        }
        _ => None,
    }
}

/// Pick the table to serve a range query: the coarsest registered rollup tier
/// whose resolution ≤ `step_ns` (FR6), else raw `metrics`.
fn select_range_table(engine: &super::QueryEngine, step_ns: i64) -> String {
    let available: Vec<super::rollup::RollupTier> = super::rollup::RollupTier::all()
        .into_iter()
        .filter(|t| engine.has_table(&format!("metrics_{}", t.label())))
        .collect();
    match super::rollup::select_tier(step_ns, &available) {
        super::rollup::RollupTier::Raw => "metrics".to_string(),
        tier => format!("metrics_{}", tier.label()),
    }
}

/// Run a range PromQL query and build a `resultType=matrix` response. Long
/// ranges are split into per-day shards by the query-frontend ([`super::frontend`])
/// and merged (FR8); a coarse `step_ns` selects a rollup tier table (FR6).
pub async fn handle_range(
    engine: &super::QueryEngine,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
) -> crate::Result<PromMatrixResponse> {
    let parsed = parser::parse(query).ok();
    // Classic-histogram queries are computed from OTLP array buckets:
    // histogram_quantile(…) and bare `_bucket`-by-`le` heatmaps.
    if let Some(expr) = &parsed {
        if let Some(spec) = detect_hist_quantile(expr) {
            return handle_hist_quantile_range(engine, &spec, start_ns, end_ns).await;
        }
        if let Some(spec) = detect_bucket_heatmap(expr) {
            return handle_bucket_heatmap(engine, &spec, start_ns, end_ns).await;
        }
    }

    // A top-level topk/bottomk: select top-N *series* in Rust; translate the
    // inner AST node directly (no Display round-trip).
    let mut topk: Option<(i64, bool)> = None;
    let inner_expr: Option<&Expr> =
        parsed
            .as_ref()
            .and_then(topk_parts)
            .map(|(n, is_topk, inner)| {
                topk = Some((n, is_topk));
                inner
            });

    let table = select_range_table(engine, step_ns);
    let windows: Vec<(i64, i64)> = if super::frontend::should_split(start_ns, end_ns) {
        // Per-day shards aligned to UTC midnight; everything before the last day
        // is sealed/cacheable. `split` emits the shard-count metric.
        let sealed_ns = end_ns.saturating_sub(86_400_000_000_000);
        super::frontend::split(start_ns, end_ns, 0, sealed_ns)
            .into_iter()
            .map(|s| (s.start_ns, s.end_ns))
            .collect()
    } else {
        vec![(start_ns, end_ns)]
    };

    let mut merged: RangeSeries = BTreeMap::new();
    for (s, e) in windows {
        let part = match inner_expr {
            Some(inner) => {
                let sql = lower_range(inner, s, e, &table).map_err(to_err)?;
                range_series_sql(engine, &sql).await?
            }
            None => range_series(engine, query, s, e, &table).await?,
        };
        for (key, (metric, points)) in part {
            merged
                .entry(key)
                .or_insert_with(|| (metric, Vec::new()))
                .1
                .extend(points);
        }
    }

    let mut series: Vec<(BTreeMap<String, String>, Vec<(f64, f64)>)> = merged
        .into_values()
        .map(|(metric, mut points)| {
            points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            points.dedup_by(|a, b| a.0 == b.0); // drop boundary-overlap duplicates
            (metric, points)
        })
        .collect();

    // topk/bottomk: keep the N highest/lowest *series* (by peak value), each with
    // all its points and labels — not N individual points.
    if let Some((n, is_topk)) = topk {
        let score = |p: &[(f64, f64)]| p.iter().map(|x| x.1).fold(f64::MIN, f64::max);
        series.sort_by(|a, b| {
            let (sa, sb) = (score(&a.1), score(&b.1));
            if is_topk {
                sb.partial_cmp(&sa)
            } else {
                sa.partial_cmp(&sb)
            }
            .unwrap_or(std::cmp::Ordering::Equal)
        });
        series.truncate(usize::try_from(n.max(0)).unwrap_or(usize::MAX));
    }
    Ok(PromMatrixResponse::matrix(series))
}

// --- histogram_quantile (OTLP explicit-bounds histograms, Rust-native) ---
//
// Rabbit hole #5 (DESIGN): UNNEST of two parallel JSON-array string columns in
// DataFusion is fragile (zip vs cross-join, no `json_parse`→array). Within the
// task time-box we take the documented raw-native fallback: SQL selects the
// histogram rows, the quantile is interpolated in Rust.

/// If `expr` is `histogram_quantile(φ, <selector>)`, return `(φ, selector)`.
fn histogram_quantile_parts(expr: &Expr) -> Option<(f64, &VectorSelector)> {
    let c = match expr {
        Expr::Call(c) => c,
        Expr::Paren(p) => return histogram_quantile_parts(&p.expr),
        _ => return None,
    };
    if c.func.name != "histogram_quantile" {
        return None;
    }
    let phi = match c.args.args.first().map(|b| b.as_ref()) {
        Some(Expr::NumberLiteral(n)) => n.val,
        _ => return None,
    };
    // v1: inner must be a bare vector selector over an OTLP histogram metric.
    match c.args.args.get(1).map(|b| b.as_ref()) {
        Some(Expr::VectorSelector(vs)) => Some((phi, vs)),
        Some(Expr::Paren(p)) => match p.expr.as_ref() {
            Expr::VectorSelector(vs) => Some((phi, vs)),
            _ => None,
        },
        _ => None,
    }
}

/// Linear-interpolated quantile over an OTLP explicit-bounds histogram.
///
/// `bounds` are the `explicit_bounds` (length n); `counts` are the per-bucket
/// `bucket_counts` (length n+1, last is the `+Inf` overflow bucket). Returns
/// `None` for an empty histogram (no panic, no division by zero).
pub(crate) fn histogram_quantile(phi: f64, counts: &[f64], bounds: &[f64]) -> Option<f64> {
    let total: f64 = counts.iter().sum();
    if total <= 0.0 || counts.is_empty() {
        return None;
    }
    let phi = phi.clamp(0.0, 1.0);
    let rank = phi * total;

    let mut cum_before = 0.0;
    for (i, &c) in counts.iter().enumerate() {
        let cum = cum_before + c;
        if cum >= rank {
            // Bucket i: (lower, upper]. lower = bounds[i-1] (0 for i==0);
            // upper = bounds[i] (the +Inf bucket has no finite upper bound).
            let lower = if i == 0 { 0.0 } else { bounds[i - 1] };
            let upper = bounds.get(i).copied();
            let Some(upper) = upper else {
                // +Inf overflow bucket — return the last finite bound.
                return bounds.last().copied().or(Some(lower));
            };
            if c <= 0.0 {
                return Some(upper);
            }
            return Some(lower + (upper - lower) * (rank - cum_before) / c);
        }
        cum_before = cum;
    }
    bounds.last().copied()
}

/// Parse a JSON array string (`"[1,2,3]"`) into `f64`s; tolerant of nulls.
fn parse_f64_array(json: Option<&str>) -> Vec<f64> {
    let Some(json) = json else { return Vec::new() };
    serde_json::from_str::<Vec<f64>>(json).unwrap_or_default()
}

/// Run `histogram_quantile(φ, m{…})` and build a `resultType=vector` response.
async fn handle_histogram(
    engine: &super::QueryEngine,
    phi: f64,
    vs: &VectorSelector,
    time_ns: i64,
) -> crate::Result<PromResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let name = vs
        .name
        .as_deref()
        .ok_or_else(|| to_err("histogram selector requires a name".into()))?;
    let mut preds = vec![format!(
        "prom_metric_name(name, unit, is_monotonic) = '{}'",
        esc(name)
    )];
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_pred(m) {
            preds.push(p);
        }
    }
    preds.push(format!("CAST(time_unix_nano AS BIGINT) <= {time_ns}"));
    // Latest histogram row per series at/before the eval time.
    let sql = format!(
        "SELECT service_name, attributes, bucket_counts, explicit_bounds FROM (\
         SELECT service_name, attributes, bucket_counts, explicit_bounds, \
         row_number() OVER (PARTITION BY name, service_name, attributes ORDER BY time_unix_nano DESC) AS rn \
         FROM metrics WHERE {}) WHERE rn = 1",
        preds.join(" AND ")
    );
    let batches = engine.sql(&sql).await?;
    #[allow(clippy::cast_precision_loss)]
    let time_s = time_ns as f64 / 1_000_000_000.0;

    let mut samples: Vec<(BTreeMap<String, String>, f64, f64)> = Vec::new();
    for batch in &batches {
        let svc_arr = cast(batch.column(0), &DataType::Utf8)?;
        let svc = svc_arr.as_string::<i32>();
        let bc_arr = cast(batch.column(2), &DataType::Utf8)?;
        let bc = bc_arr.as_string::<i32>();
        let eb_arr = cast(batch.column(3), &DataType::Utf8)?;
        let eb = eb_arr.as_string::<i32>();
        for i in 0..batch.num_rows() {
            let counts = parse_f64_array((!bc.is_null(i)).then(|| bc.value(i)));
            let bounds = parse_f64_array((!eb.is_null(i)).then(|| eb.value(i)));
            let Some(v) = histogram_quantile(phi, &counts, &bounds) else {
                continue;
            };
            let mut metric = BTreeMap::new();
            metric.insert("__name__".to_string(), name.to_string());
            if !svc.is_null(i) {
                metric.insert("service_name".to_string(), svc.value(i).to_string());
            }
            samples.push((metric, time_s, v));
        }
    }
    Ok(PromResponse::vector(samples))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_values_sql_metric_name_discovery() {
        // `__name__` lists normalized metric names plus the synthetic
        // histogram series; other labels keep the plain DISTINCT path.
        let sql = label_values_sql("__name__");
        assert!(
            sql.contains("prom_metric_name(name, unit, is_monotonic)"),
            "sql: {sql}"
        );
        for suffix in ["'_bucket'", "'_count'", "'_sum'"] {
            assert!(sql.contains(suffix), "missing {suffix}: {sql}");
        }
        assert!(sql.contains("bucket_counts IS NOT NULL"), "sql: {sql}");

        let plain = label_values_sql("service_name");
        assert!(
            plain.contains("SELECT DISTINCT service_name AS v FROM metrics"),
            "sql: {plain}"
        );
    }

    #[test]
    fn test_promql_instant_selector_to_sql() {
        let sql = translate_instant(r#"node_memory_total_bytes{host="h1"}"#, 1000).unwrap();
        assert!(
            sql.contains("prom_metric_name(name, unit, is_monotonic) = 'node_memory_total_bytes'"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("prom_attr(attributes, 'host') = 'h1'"),
            "sql: {sql}"
        );
        assert!(sql.contains("CAST(time_unix_nano AS BIGINT) <= 1000"));
        assert!(sql.contains("WHERE rn = 1"));
    }

    #[test]
    fn test_promql_sum_by_label_groups_on_json_extract() {
        let sql =
            translate_instant(r#"sum by (le) (http_bucket{service_name="client"})"#, 5).unwrap();
        assert!(sql.contains("sum(v) AS v"), "sql: {sql}");
        assert!(sql.contains("prom_attr(attributes, 'le')"), "sql: {sql}");
        assert!(
            sql.contains("GROUP BY prom_attr(attributes, 'le')"),
            "sql: {sql}"
        );
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
        assert!(label_values_sql("http_route").contains("prom_attr(attributes, 'http_route')"));
        assert!(series_sql().contains("SELECT DISTINCT name, service_name FROM metrics"));
    }

    #[test]
    fn test_prom_vector_response_shape() {
        let mut m = BTreeMap::new();
        m.insert("service_name".to_string(), "client".to_string());
        let resp = PromResponse::vector([(m, 1700000000.0, 42.0)]);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""resultType":"vector""#), "json: {json}");
        assert!(
            json.contains(r#""value":[1700000000.0,"42"]"#),
            "json: {json}"
        );
    }

    #[test]
    fn test_rate_translates_to_lag_window() {
        let sql = translate_range("rate(http_total[1m])", 0, 100).unwrap();
        assert!(sql.contains("LAG(v) OVER w"), "sql: {sql}");
        assert!(
            sql.contains("PARTITION BY name, service_name, attributes ORDER BY time_unix_nano"),
            "sql: {sql}"
        );
        assert!(sql.contains("WHERE prev_t IS NOT NULL"), "sql: {sql}");
        assert!(
            sql.contains("prom_metric_name(name, unit, is_monotonic) = 'http_total'"),
            "sql: {sql}"
        );
    }

    #[test]
    fn test_range_targets_selected_tier_table() {
        // FR6: the frontend routes coarse queries to a rollup tier table.
        let tier = translate_range_on("rate(http_total[1m])", 0, 100, "metrics_1h").unwrap();
        assert!(tier.contains("FROM metrics_1h WHERE"), "sql: {tier}");
        // the default still targets raw `metrics`
        let raw = translate_range("rate(http_total[1m])", 0, 100).unwrap();
        assert!(raw.contains("FROM metrics WHERE"), "sql: {raw}");
        assert!(!raw.contains("metrics_1h"), "sql: {raw}");
    }

    #[test]
    fn test_rate_counter_reset_uses_current_value() {
        let sql = translate_range("rate(http_total[1m])", 0, 100).unwrap();
        assert!(
            sql.contains("CASE WHEN v >= prev_v THEN (v - prev_v) ELSE v END"),
            "counter-reset branch missing; sql: {sql}"
        );
    }

    #[test]
    fn test_topk_orders_and_limits() {
        let sql = translate_range("topk(3, rate(http_total[1m]))", 0, 100).unwrap();
        assert!(sql.contains("ORDER BY v DESC LIMIT 3"), "sql: {sql}");
        // inner rate is still present
        assert!(sql.contains("LAG(v) OVER w"), "sql: {sql}");
    }

    #[test]
    fn test_regex_matcher_is_anchored() {
        // H1: Prometheus fully anchors `=~`/`!~`; unanchored would match substrings.
        let sql = translate_instant(r#"http_total{pod=~"web"}"#, 1000).unwrap();
        assert!(
            sql.contains("regexp_like(COALESCE(prom_attr(attributes, 'pod'), ''), '^(?:web)$')"),
            "regex matcher must be anchored: {sql}"
        );
        let neg = translate_instant(r#"http_total{pod!~"web"}"#, 1000).unwrap();
        assert!(
            neg.contains(
                "NOT regexp_like(COALESCE(prom_attr(attributes, 'pod'), ''), '^(?:web)$')"
            ),
            "{neg}"
        );
    }

    #[test]
    fn test_rate_drops_duplicate_timestamps() {
        // M2: equal consecutive timestamps would divide by zero (inf/NaN).
        let sql = translate_range("rate(http_total[1m])", 0, 100).unwrap();
        assert!(
            sql.contains("CAST(time_unix_nano AS BIGINT) <> prev_t"),
            "rate must skip zero-dt pairs: {sql}"
        );
    }

    #[test]
    fn test_topk_rejects_non_integer_count() {
        // L2: Prometheus errors on a non-integer count; we must too (not truncate).
        assert!(translate_range("topk(2.9, rate(http_total[1m]))", 0, 100).is_err());
        assert!(translate_range("topk(1e30, rate(http_total[1m]))", 0, 100).is_err());
        // an integer count still works
        assert!(translate_range("topk(3, rate(http_total[1m]))", 0, 100).is_ok());
    }

    #[test]
    fn test_max_over_time_window() {
        let sql = translate_range("max_over_time(cpu_usage[5m])", 0, 100).unwrap();
        assert!(sql.contains("MAX(v) OVER"), "sql: {sql}");
        assert!(
            sql.contains("RANGE BETWEEN 300000000000 PRECEDING AND CURRENT ROW"),
            "sql: {sql}"
        );
    }

    #[test]
    fn test_sum_by_over_rate_groups_on_label() {
        let sql = translate_range("sum by (service_name) (rate(http_total[1m]))", 0, 100).unwrap();
        assert!(sql.contains("sum(v) AS v"), "sql: {sql}");
        assert!(
            sql.contains("GROUP BY service_name, time_unix_nano"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("LAG(v) OVER w"),
            "inner rate missing; sql: {sql}"
        );
    }

    #[test]
    fn test_range_unsupported_returns_error() {
        assert!(
            translate_range("http_total", 0, 1).is_err(),
            "instant selector is not a range query"
        );
        assert!(translate_range("predict_linear(x[1h], 60)", 0, 1).is_err());
        assert!(translate_range("{{bad", 0, 1).is_err());
    }

    #[test]
    fn test_prom_matrix_response_shape() {
        let mut m = BTreeMap::new();
        m.insert("service_name".to_string(), "client".to_string());
        let resp =
            PromMatrixResponse::matrix([(m, vec![(1700000000.0, 1.5), (1700000060.0, 2.0)])]);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""resultType":"matrix""#), "json: {json}");
        assert!(
            json.contains(r#""values":[[1700000000.0,"1.5"],[1700000060.0,"2"]]"#),
            "json: {json}"
        );
    }

    // A 3-sample counter fixture (http_total, service=client) at t=1s,2s,3s.
    async fn counter_engine() -> crate::query::QueryEngine {
        use crate::config::query::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp.path().join("metrics").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("double_value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["client", "client", "client"])),
                Arc::new(StringArray::from(vec![
                    "http_total",
                    "http_total",
                    "http_total",
                ])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![
                        1_000_000_000i64,
                        2_000_000_000,
                        3_000_000_000,
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec!["{}", "{}", "{}"])),
                Arc::new(Float64Array::from(vec![10.0, 30.0, 60.0])),
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
    async fn test_rate_executes_and_computes_values() {
        let engine = counter_engine().await;
        let resp = handle_range(&engine, "rate(http_total[5m])", 0, 10_000_000_000, 0)
            .await
            .unwrap();
        assert_eq!(resp.data.result_type, "matrix");
        assert_eq!(resp.data.result.len(), 1, "one series");
        let s = &resp.data.result[0];
        assert_eq!(s.metric["service_name"], "client");
        // first sample has no predecessor → dropped; rate at 2s,3s = 20,30 per sec.
        assert_eq!(
            s.values,
            vec![(2.0, "20".to_string()), (3.0, "30".to_string())]
        );
    }

    #[tokio::test]
    async fn test_range_splits_long_window_and_merges() {
        // A >1-day range is split into per-day shards by the frontend and merged.
        // The counter fixture's data lives in the first shard; the merged result
        // must equal the unsplit rate (split/merge preserves results — FR8).
        let engine = counter_engine().await;
        let two_days = 2 * 86_400_000_000_000i64;
        let resp = handle_range(&engine, "rate(http_total[5m])", 0, two_days, 0)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1, "one merged series across shards");
        assert_eq!(
            resp.data.result[0].values,
            vec![(2.0, "20".to_string()), (3.0, "30".to_string())],
            "split+merge equals the unsplit rate"
        );
    }

    #[tokio::test]
    async fn test_topk_returns_top_n_series_with_all_points() {
        // topk must keep the top-N *series* (all their points + labels), not N
        // scattered rows. Two series (sc=a high, sc=b low); topk(1) → sc=a only.
        use crate::config::query::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp.path().join("metrics").join("sum").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("double_value", DataType::Float64, true),
        ]));
        // two counter series at t=1s,2s: sc=a rises 10→30 (rate 20), sc=b 5→10 (rate 5)
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["s", "s", "s", "s"])),
                Arc::new(StringArray::from(vec!["reqs", "reqs", "reqs", "reqs"])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"sc":"a"}"#),
                    Some(r#"{"sc":"a"}"#),
                    Some(r#"{"sc":"b"}"#),
                    Some(r#"{"sc":"b"}"#),
                ])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![
                        1_000_000_000i64,
                        2_000_000_000,
                        1_000_000_000,
                        2_000_000_000,
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(Float64Array::from(vec![10.0, 30.0, 5.0, 10.0])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("m.parquet")).unwrap();
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
        let resp = handle_range(
            &engine,
            "topk(1, sum by (sc) (rate(reqs[5m])))",
            0,
            10_000_000_000,
            0,
        )
        .await
        .unwrap();
        // exactly the one top series (sc=a), with its point(s) — not 1 scattered row
        assert_eq!(
            resp.data.result.len(),
            1,
            "top-1 series only: {:?}",
            resp.data.result
        );
        assert_eq!(resp.data.result[0].metric["sc"], "a", "the higher series");
        assert_eq!(resp.data.result[0].values, vec![(2.0, "20".to_string())]);
    }

    #[tokio::test]
    async fn test_max_over_time_executes_with_range_frame() {
        let engine = counter_engine().await;
        let resp = handle_range(
            &engine,
            "max_over_time(http_total[5m])",
            0,
            10_000_000_000,
            0,
        )
        .await
        .unwrap();
        let s = &resp.data.result[0];
        // sliding max up to each point: 10, 30, 60.
        assert_eq!(
            s.values,
            vec![
                (1.0, "10".to_string()),
                (2.0, "30".to_string()),
                (3.0, "60".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn test_histogram_quantile_range_from_otlp_arrays() {
        // #4: the dashboard's `histogram_quantile(φ, sum(rate(<base>_bucket[d])) by (le))`
        // is served from the native OTLP array histogram (no classic _bucket series).
        use crate::config::query::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{BooleanArray, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp
            .path()
            .join("metrics")
            .join("histogram")
            .join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("unit", DataType::Utf8, true),
            Field::new("is_monotonic", DataType::Boolean, true),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["client"])),
                Arc::new(StringArray::from(vec!["http.server.request.duration"])),
                Arc::new(StringArray::from(vec![Some("s")])),
                Arc::new(BooleanArray::from(vec![Some(false)])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec![Some("{}")])),
                Arc::new(StringArray::from(vec![Some("[0,20,30,30,15,5]")])),
                Arc::new(StringArray::from(vec![Some("[10,20,30,40,50]")])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("h.parquet")).unwrap();
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
        // base `_bucket` query (normalized name http_server_request_duration_seconds)
        let resp = handle_range(
            &engine,
            "histogram_quantile(0.95, sum(rate(http_server_request_duration_seconds_bucket[1m])) by (le))",
            0,
            10_000_000_000,
            15,
        )
        .await
        .unwrap();
        assert_eq!(resp.data.result_type, "matrix");
        assert_eq!(
            resp.data.result.len(),
            1,
            "one series: {:?}",
            resp.data.result
        );
        let v = &resp.data.result[0].values;
        assert_eq!(v.len(), 1, "one point: {v:?}");
        assert!(
            (v[0].1.parse::<f64>().unwrap() - 50.0).abs() < 1e-9,
            "p95 from OTLP buckets = 50: {v:?}"
        );
    }

    #[tokio::test]
    async fn test_bucket_heatmap_explodes_le_series() {
        // #4 heatmap: sum(rate(<base>_bucket[d])) by (le) → per-le cumulative
        // bucket rate series, exploded from the OTLP arrays.
        use crate::config::query::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{BooleanArray, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp
            .path()
            .join("metrics")
            .join("histogram")
            .join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("unit", DataType::Utf8, true),
            Field::new("is_monotonic", DataType::Boolean, true),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
        ]));
        // two cumulative-increasing snapshots at t=1s and t=2s (bounds [10,20])
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["client", "client"])),
                Arc::new(StringArray::from(vec![
                    "http.server.request.duration",
                    "http.server.request.duration",
                ])),
                Arc::new(StringArray::from(vec![Some("s"), Some("s")])),
                Arc::new(BooleanArray::from(vec![Some(false), Some(false)])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64, 2_000_000_000])
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec![Some("{}"), Some("{}")])),
                Arc::new(StringArray::from(vec![Some("[0,2,3]"), Some("[0,4,6]")])),
                Arc::new(StringArray::from(vec![Some("[10,20]"), Some("[10,20]")])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("h.parquet")).unwrap();
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
        let resp = handle_range(
            &engine,
            "sum(rate(http_server_request_duration_seconds_bucket[1m])) by (le)",
            0,
            10_000_000_000,
            15,
        )
        .await
        .unwrap();
        // three le buckets: 10, 20, +Inf
        assert_eq!(
            resp.data.result.len(),
            3,
            "le series: {:?}",
            resp.data.result
        );
        let by_le: std::collections::BTreeMap<String, f64> = resp
            .data
            .result
            .iter()
            .map(|r| {
                (
                    r.metric["le"].clone(),
                    r.values.last().unwrap().1.parse().unwrap(),
                )
            })
            .collect();
        // cumulative: le=10 stays 0 → rate 0; le=20: (4-2)/1s=2; le=+Inf: (10-5)/1s=5
        assert!((by_le["20"] - 2.0).abs() < 1e-9, "le=20 rate: {by_le:?}");
        assert!(
            (by_le["+Inf"] - 5.0).abs() < 1e-9,
            "le=+Inf rate: {by_le:?}"
        );
    }

    #[test]
    fn test_histogram_quantile_p95_interpolation() {
        // bounds (n=5), counts (n+1=6); total = 100.
        let bounds = [10.0, 20.0, 30.0, 40.0, 50.0];
        let counts = [0.0, 20.0, 30.0, 30.0, 15.0, 5.0];
        let p95 = histogram_quantile(0.95, &counts, &bounds).unwrap();
        assert!((p95 - 50.0).abs() < 1e-9, "p95 = {p95}");
        let p50 = histogram_quantile(0.50, &counts, &bounds).unwrap();
        assert!((p50 - 30.0).abs() < 1e-9, "p50 = {p50}");
    }

    #[test]
    fn test_histogram_quantile_handles_empty_buckets() {
        // all-zero counts → no observations → None, no panic / div-by-zero.
        assert_eq!(
            histogram_quantile(0.95, &[0.0, 0.0, 0.0], &[1.0, 2.0]),
            None
        );
        // no buckets at all.
        assert_eq!(histogram_quantile(0.95, &[], &[]), None);
        // everything in the +Inf overflow bucket → last finite bound, no panic.
        let v = histogram_quantile(
            0.95,
            &[0.0, 0.0, 0.0, 0.0, 0.0, 100.0],
            &[10.0, 20.0, 30.0, 40.0, 50.0],
        );
        assert_eq!(v, Some(50.0));
    }

    #[test]
    fn test_histogram_quantile_parses_query() {
        let expr =
            parser::parse(r#"histogram_quantile(0.95, http_req_duration{service_name="client"})"#)
                .unwrap();
        let (phi, vs) = histogram_quantile_parts(&expr).expect("recognised as histogram_quantile");
        assert!((phi - 0.95).abs() < 1e-9);
        assert_eq!(vs.name.as_deref(), Some("http_req_duration"));
        // a non-histogram query is not misclassified
        assert!(histogram_quantile_parts(&parser::parse("rate(x[1m])").unwrap()).is_none());
    }
}
