// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
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
    self, AggregateExpr, BinModifier, Expr, LabelModifier, VectorMatchCardinality, VectorSelector,
    token,
};
use serde::{Deserialize, Serialize};

fn sql_ident(key: &str) -> String {
    key.chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect()
}

// --- PromQL Expr/DataFrame lowering (expr-lowering migration) ---

/// Label LHS as an `Expr`: promoted `service_name`, else `prom_attr(attributes, key)`.
fn label_lhs_expr(key: &str) -> datafusion::logical_expr::Expr {
    if key == "service_name" {
        datafusion::prelude::col("service_name")
    } else {
        super::plan::predicate::prom_attr("attributes", key)
    }
}

/// The materialized, prunable `prom_name` column (the normalized Prometheus
/// name). Written at ingest by the codec; the read path filters it directly
/// instead of recomputing `prom_metric_name` per row.
fn prom_name_expr() -> datafusion::logical_expr::Expr {
    datafusion::prelude::col("prom_name")
}

/// A matcher → filter `Expr` (None for `__name__`, covered by the name predicate).
fn matcher_expr(m: &Matcher) -> Option<datafusion::logical_expr::Expr> {
    use super::plan::predicate::{MatchKind, cmp};
    if m.name == "__name__" {
        return None;
    }
    let kind = match &m.op {
        MatchOp::Equal => MatchKind::Eq,
        MatchOp::NotEqual => MatchKind::Neq,
        MatchOp::Re(_) => MatchKind::Re,
        MatchOp::NotRe(_) => MatchKind::Nre,
    };
    // Matchers are always string ops (Eq/Neq/Re/Nre) — the numeric branch that
    // can error is never taken, so `.ok()` is total here.
    cmp(label_lhs_expr(&m.name), kind, &m.value, false).ok()
}

/// Metric-name predicate `Expr`: the exact
/// normalized name, plus the histogram `_count`/`_sum` synthesis on bucket rows.
fn name_pred_expr(name: &str) -> datafusion::logical_expr::Expr {
    use datafusion::prelude::{col, lit};
    let exact = |n: &str| prom_name_expr().eq(lit(n.to_string()));
    let hist = |base: &str| exact(base).and(col("bucket_counts").is_not_null());
    if let Some(base) = name.strip_suffix("_count") {
        exact(name).or(hist(base))
    } else if let Some(base) = name.strip_suffix("_sum") {
        exact(name).or(hist(base))
    } else {
        exact(name)
    }
}

/// Build the PromQL `series` query as a `DataFrame` (P4): distinct
/// `(normalized __name__, service_name)` matching an optional `match[]` selector.
pub async fn build_series(
    engine: &super::QueryEngine,
    matcher: Option<&str>,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let mut df = engine.table("metrics").await?;
    if let Some(sel) = matcher.map(str::trim).filter(|s| !s.is_empty()) {
        let expr = parser::parse(sel).map_err(to_err)?;
        let vs = match &expr {
            Expr::VectorSelector(vs) => vs,
            _ => {
                return Err(to_err(
                    "series match[] must be a metric selector".to_string(),
                ));
            }
        };
        if let Some(name) = vs.name.as_deref() {
            df = df.filter(name_pred_expr(name))?;
        }
        for m in &vs.matchers.matchers {
            if let Some(p) = matcher_expr(m) {
                df = df.filter(p)?;
            }
        }
    }
    Ok(df
        .select(vec![prom_name_expr().alias("name"), col("service_name")])?
        .distinct()?)
}

/// Numeric value `Expr`: the gauge/counter value, or the histogram count/sum.
fn metric_value_expr(name: &str) -> datafusion::logical_expr::Expr {
    use datafusion::arrow::datatypes::DataType::Float64;
    use datafusion::functions::expr_fn::coalesce;
    use datafusion::logical_expr::expr_fn::cast;
    use datafusion::prelude::col;
    let dv = col("double_value");
    let iv = cast(col("int_value"), Float64);
    if name.ends_with("_count") {
        coalesce(vec![dv, iv, cast(col("count"), Float64)])
    } else if name.ends_with("_sum") {
        coalesce(vec![dv, col("sum")])
    } else {
        coalesce(vec![dv, iv])
    }
}

/// `CAST(time_unix_nano AS BIGINT) BETWEEN start AND end`.
fn prom_time_between(start_ns: i64, end_ns: i64) -> datafusion::logical_expr::Expr {
    use datafusion::arrow::datatypes::DataType::Int64;
    use datafusion::logical_expr::expr_fn::cast;
    use datafusion::prelude::{col, lit};
    cast(col("time_unix_nano"), Int64).between(lit(start_ns), lit(end_ns))
}

/// Per-sample base over `metrics` as a `DataFrame` (P3): the matched series'
/// `(prom_name, name, service_name, attributes, time, v)`.
async fn metric_base_df(
    engine: &super::QueryEngine,
    vs: &VectorSelector,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let name = vs
        .name
        .as_deref()
        .ok_or_else(|| to_err("metric selector requires a name".to_string()))?;
    let mut df = engine.table(table).await?.filter(name_pred_expr(name))?;
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_expr(m) {
            df = df.filter(p)?;
        }
    }
    df = df.filter(prom_time_between(start_ns, end_ns))?;
    Ok(df.select(vec![
        prom_name_expr().alias("prom_name"),
        col("name"),
        col("service_name"),
        col("attributes"),
        col("time_unix_nano"),
        metric_value_expr(name).alias("v"),
    ])?)
}

/// The `(name, service_name, attributes)` window partition for PromQL.
fn prom_part() -> Vec<datafusion::logical_expr::Expr> {
    use datafusion::prelude::col;
    vec![col("name"), col("service_name"), col("attributes")]
}

fn agg_value_expr(op: &str, e: datafusion::logical_expr::Expr) -> datafusion::logical_expr::Expr {
    use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
    match op {
        "max" => max(e),
        "min" => min(e),
        "avg" => avg(e),
        "count" => count(e),
        _ => sum(e),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn range_to_ns(range: Duration) -> i64 {
    i64::try_from(range.as_nanos()).unwrap_or(i64::MAX)
}

/// `<agg> [by (...)]` over a range expression, as a `DataFrame` (P8).
async fn lower_range_aggregate_df(
    engine: &super::QueryEngine,
    agg: &AggregateExpr,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let op = agg_name(agg.op).map_err(to_err)?;
    let by = match &agg.modifier {
        Some(LabelModifier::Include(labels)) => labels.labels.clone(),
        Some(LabelModifier::Exclude(_)) => {
            return Err(to_err(
                "`without (...)` aggregation not supported (v1)".to_string(),
            ));
        }
        None => Vec::new(),
    };
    let inner = Box::pin(lower_range_df(
        engine,
        agg.expr.as_ref(),
        start_ns,
        end_ns,
        table,
    ))
    .await?;
    let v = agg_value_expr(op, col("v")).alias("v");
    if by.is_empty() {
        Ok(inner.aggregate(vec![col("time_unix_nano")], vec![v])?)
    } else {
        let mut group: Vec<datafusion::logical_expr::Expr> = by
            .iter()
            .map(|k| label_lhs_expr(k).alias(sql_ident(k)))
            .collect();
        group.push(col("time_unix_nano"));
        Ok(inner.aggregate(group, vec![v])?)
    }
}

/// Lower a range PromQL expression to a `DataFrame`: rate/`*_over_time` via
/// [`super::plan::frame`], `<agg> by` via group-by, bare selectors as raw samples.
async fn lower_range_df(
    engine: &super::QueryEngine,
    expr: &Expr,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use super::plan::frame::{OverTimeAgg, over_time, rate};
    use datafusion::prelude::col;
    match expr {
        Expr::Call(c) => {
            let (vs, range) = match c.args.args.first().map(|b| b.as_ref()) {
                Some(Expr::MatrixSelector(ms)) => (&ms.vs, ms.range),
                _ => {
                    return Err(to_err(format!(
                        "{}() expects a range-vector argument like m[5m]",
                        c.func.name
                    )));
                }
            };
            let base = metric_base_df(engine, vs, start_ns, end_ns, table).await?;
            let part = prom_part();
            let r = range_to_ns(range);
            match c.func.name {
                "rate" | "irate" | "increase" => rate(base, part, "v", "time_unix_nano"),
                "max_over_time" => {
                    over_time(base, part, "v", "time_unix_nano", r, OverTimeAgg::Max)
                }
                "min_over_time" => {
                    over_time(base, part, "v", "time_unix_nano", r, OverTimeAgg::Min)
                }
                "avg_over_time" => {
                    over_time(base, part, "v", "time_unix_nano", r, OverTimeAgg::Avg)
                }
                "sum_over_time" => {
                    over_time(base, part, "v", "time_unix_nano", r, OverTimeAgg::Sum)
                }
                "count_over_time" => {
                    over_time(base, part, "v", "time_unix_nano", r, OverTimeAgg::Count)
                }
                other => Err(to_err(format!(
                    "unsupported range function: {other}() (v1)"
                ))),
            }
        }
        Expr::Paren(p) => Box::pin(lower_range_df(engine, &p.expr, start_ns, end_ns, table)).await,
        Expr::Aggregate(agg) => {
            lower_range_aggregate_df(engine, agg, start_ns, end_ns, table).await
        }
        Expr::VectorSelector(vs) => Ok(metric_base_df(engine, vs, start_ns, end_ns, table)
            .await?
            .sort(vec![col("time_unix_nano").sort(true, false)])?),
        _ => Err(to_err(
            "unsupported PromQL expression for query_range (v1)".to_string(),
        )),
    }
}

/// Latest sample per series at/before `time_ns`, as a `DataFrame` (P5): the
/// instant-query base (`metric_base` filtered `<= time_ns`, then `rn = 1`).
async fn latest_selected_df(
    engine: &super::QueryEngine,
    vs: &VectorSelector,
    time_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::arrow::datatypes::DataType::Int64;
    use datafusion::logical_expr::expr_fn::cast;
    use datafusion::prelude::{col, lit};
    let name = vs
        .name
        .as_deref()
        .ok_or_else(|| to_err("metric selector requires a name".to_string()))?;
    let mut df = engine
        .table("metrics")
        .await?
        .filter(name_pred_expr(name))?;
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_expr(m) {
            df = df.filter(p)?;
        }
    }
    df = df.filter(cast(col("time_unix_nano"), Int64).lt_eq(lit(time_ns)))?;
    let base = df.select(vec![
        prom_name_expr().alias("prom_name"),
        col("name"),
        col("service_name"),
        col("attributes"),
        col("time_unix_nano"),
        metric_value_expr(name).alias("v"),
    ])?;
    // latest per (name, attributes) — matches the SQL row_number partition.
    super::plan::frame::latest_per_series(
        base,
        vec![col("name"), col("attributes")],
        "time_unix_nano",
    )
}

/// Instant `<agg> [by (...)]` over the latest samples, as a `DataFrame` (P4).
/// Matrix range (ns) of a range expression (`rate(m[5m])`, `<agg>_over_time(m[d])`)
/// — the lookback window used to evaluate it at a single instant.
fn matrix_range_ns(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Call(c) => match c.args.args.first().map(std::convert::AsRef::as_ref) {
            Some(Expr::MatrixSelector(ms)) => Some(range_to_ns(ms.range)),
            _ => None,
        },
        Expr::Paren(p) => matrix_range_ns(&p.expr),
        _ => None,
    }
}

async fn lower_aggregate_instant_df(
    engine: &super::QueryEngine,
    agg: &AggregateExpr,
    time_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let op = agg_name(agg.op).map_err(to_err)?;
    let by = match &agg.modifier {
        Some(LabelModifier::Include(labels)) => labels.labels.clone(),
        Some(LabelModifier::Exclude(_)) => {
            return Err(to_err(
                "`without (...)` aggregation not supported (v1)".to_string(),
            ));
        }
        None => Vec::new(),
    };
    // Bare selector inner (`sum(metric)`): latest sample per series, then aggregate.
    let selector = match agg.expr.as_ref() {
        Expr::VectorSelector(vs) => Some(vs),
        Expr::Paren(p) => match p.expr.as_ref() {
            Expr::VectorSelector(vs) => Some(vs),
            _ => None,
        },
        _ => None,
    };
    if let Some(vs) = selector {
        let inner = latest_selected_df(engine, vs, time_ns).await?;
        let v = agg_value_expr(op, col("v")).alias("v");
        return if by.is_empty() {
            Ok(inner.aggregate(vec![], vec![v])?)
        } else {
            let group: Vec<datafusion::logical_expr::Expr> = by
                .iter()
                .map(|k| label_lhs_expr(k).alias(sql_ident(k)))
                .collect();
            Ok(inner.aggregate(group, vec![v])?)
        };
    }
    // Range-expression inner (`avg(rate(metric[5m]))`, common on gauge panels):
    // evaluate `<agg>(range)` over the [T-range, T] window with the range engine,
    // then take the value at T (latest sample per group) — an instant scalar/vector.
    let range = matrix_range_ns(agg.expr.as_ref()).ok_or_else(|| {
        to_err("instant aggregate inner must be a selector or a range function (v1)".to_string())
    })?;
    let start = time_ns.saturating_sub(range);
    let ranged = lower_range_aggregate_df(engine, agg, start, time_ns, "metrics").await?;
    let part: Vec<datafusion::logical_expr::Expr> =
        by.iter().map(|k| col(sql_ident(k))).collect();
    super::plan::frame::latest_per_series(ranged, part, "time_unix_nano")
}

/// Lower an instant PromQL expression to a `DataFrame`: latest-per-series
/// selectors and `<agg> by` aggregations.
async fn lower_instant_df(
    engine: &super::QueryEngine,
    expr: &Expr,
    time_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    match expr {
        Expr::VectorSelector(vs) => latest_selected_df(engine, vs, time_ns).await,
        Expr::Paren(p) => Box::pin(lower_instant_df(engine, &p.expr, time_ns)).await,
        Expr::Aggregate(agg) => lower_aggregate_instant_df(engine, agg, time_ns).await,
        // Bare range function at an instant (`rate(metric[5m])`): evaluate over the
        // [T-range, T] window via the range engine, then keep the value at T.
        Expr::Call(_) => {
            let range = matrix_range_ns(expr).ok_or_else(|| {
                to_err("instant range function expects a range vector like m[5m] (v1)".to_string())
            })?;
            let start = time_ns.saturating_sub(range);
            let series = lower_range_df(engine, expr, start, time_ns, "metrics").await?;
            // rate/over_time project to (service_name, attributes, time, v) — a
            // series is identified by those, so partition the latest-pick on them.
            let part = vec![
                datafusion::prelude::col("service_name"),
                datafusion::prelude::col("attributes"),
            ];
            super::plan::frame::latest_per_series(series, part, "time_unix_nano")
        }
        _ => Err(to_err(
            "unsupported PromQL expression for instant query (v1)".to_string(),
        )),
    }
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

/// Serialize a unix-seconds timestamp as an integer JSON number when it has no
/// fractional part (Mimir/Prometheus emit integer seconds), else as a float
/// (C-P5). The field deserializes back into `f64` either way.
fn ts_number(ts: f64) -> serde_json::Value {
    if ts.fract() == 0.0 && ts.abs() < 9.007e15 {
        #[allow(clippy::cast_possible_truncation)]
        let secs = ts as i64;
        serde_json::Value::Number(secs.into())
    } else {
        serde_json::Number::from_f64(ts)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

/// A `(ts, value)` sample serialized as the Prometheus `[ts, "v"]` array, with an
/// integer timestamp when whole.
struct Sample<'a>(&'a (f64, String));
impl Serialize for Sample<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(2)?;
        t.serialize_element(&ts_number(self.0.0))?;
        t.serialize_element(&self.0.1)?;
        t.end()
    }
}

fn ser_pair<S: serde::Serializer>(v: &(f64, String), s: S) -> Result<S::Ok, S::Error> {
    Sample(v).serialize(s)
}

fn ser_pairs<S: serde::Serializer>(v: &[(f64, String)], s: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut seq = s.serialize_seq(Some(v.len()))?;
    for p in v {
        seq.serialize_element(&Sample(p))?;
    }
    seq.end()
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
    /// `[unix-seconds, stringified value]` — integer seconds when whole.
    #[serde(serialize_with = "ser_pair")]
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

/// Per-batch accessor that turns a metrics result row into Prometheus labels
/// (C-P1): promoted string columns become labels, `prom_name` → the normalized
/// `__name__`, and the `attributes` JSON column is exploded into normalized
/// per-attribute labels. Built once per batch; `labels(i)` yields one row's set.
/// (Grouped queries project their `by(…)` labels as columns and carry no
/// `attributes`/`prom_name`, so they're handled by the same path unchanged.)
struct LabelCols {
    promoted: Vec<(String, datafusion::arrow::array::ArrayRef)>,
    attrs: Option<datafusion::arrow::array::ArrayRef>,
}

impl LabelCols {
    fn build(batch: &datafusion::arrow::record_batch::RecordBatch) -> crate::Result<Self> {
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::DataType;
        // value / internal / raw-name columns are not labels.
        const SKIP: [&str; 4] = ["v", "time_unix_nano", "rn", "name"];
        let schema = batch.schema();
        let mut promoted = Vec::new();
        let mut attrs = None;
        for (i, f) in schema.fields().iter().enumerate() {
            let n = f.name().as_str();
            if n == "attributes" {
                attrs = Some(
                    cast(batch.column(i), &DataType::Utf8).map_err(|e| to_err(e.to_string()))?,
                );
                continue;
            }
            if SKIP.contains(&n) {
                continue;
            }
            let key = if n == "prom_name" {
                "__name__".to_string()
            } else {
                n.to_string()
            };
            let arr = cast(batch.column(i), &DataType::Utf8).map_err(|e| to_err(e.to_string()))?;
            promoted.push((key, arr));
        }
        Ok(Self { promoted, attrs })
    }

    fn labels(&self, i: usize) -> BTreeMap<String, String> {
        use datafusion::arrow::array::{Array, AsArray};
        let mut m = BTreeMap::new();
        for (key, arr) in &self.promoted {
            let a = arr.as_string::<i32>();
            if !a.is_null(i) {
                m.insert(key.clone(), a.value(i).to_string());
            }
        }
        if let Some(arr) = &self.attrs {
            let a = arr.as_string::<i32>();
            if !a.is_null(i)
                && let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_str::<serde_json::Value>(a.value(i))
            {
                for (k, v) in map {
                    let val = match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    // Promoted columns win over attributes on a key collision.
                    m.entry(super::udf::normalize(&k)).or_insert(val);
                }
            }
        }
        m
    }
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
    // histogram_quantile, binary/unary operators and aggregates are all handled
    // by the recursive evaluator; leaves fall through to SQL.
    let expr = parser::parse(query).map_err(to_err)?;
    // ns→seconds for the Prometheus sample timestamp; sub-ms precision is irrelevant here.
    #[allow(clippy::cast_precision_loss)]
    let time_s = time_ns as f64 / 1_000_000_000.0;

    let samples: Vec<(BTreeMap<String, String>, f64, f64)> =
        match eval_instant(engine, &expr, time_ns).await? {
            InstantVal::Scalar(s) => vec![(BTreeMap::new(), time_s, s)],
            InstantVal::Vector(v) => v.into_iter().map(|(m, x)| (m, time_s, x)).collect(),
        };
    Ok(PromResponse::vector(samples))
}

/// Collect the first (string) column of a built `DataFrame`. Shared by the
/// label/tag-value discovery endpoints (Prometheus, Loki).
pub(super) async fn string_column_df(
    engine: &super::QueryEngine,
    df: datafusion::dataframe::DataFrame,
) -> crate::Result<Vec<String>> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let batches = engine.collect(df).await?;
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

pub(super) async fn distinct_json_keys(
    engine: &super::QueryEngine,
    table: &str,
    column: &str,
) -> crate::Result<std::collections::BTreeSet<String>> {
    // Cap the distinct blobs scanned: label/tag discovery is bounded by
    // label-set cardinality, but a high-cardinality attribute (e.g. a per-request
    // id embedded in the JSON) would otherwise make this an unbounded scan +
    // parse. 10k distinct blobs is far more label sets than any real schema.
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;
    const MAX_DISTINCT_BLOBS: usize = 10_000;
    let df = engine
        .table(table)
        .await?
        .filter(datafusion::prelude::col(column).is_not_null())?
        .select(vec![datafusion::prelude::col(column)])?
        .distinct()?
        .limit(0, Some(MAX_DISTINCT_BLOBS))?;
    let batches = engine.collect(df).await?;
    let mut keys = std::collections::BTreeSet::new();
    for batch in &batches {
        let c = cast(batch.column(0), &DataType::Utf8)?;
        let c = c.as_string::<i32>();
        for i in 0..batch.num_rows() {
            if !c.is_null(i)
                && let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_str::<serde_json::Value>(c.value(i))
            {
                keys.extend(map.keys().cloned());
            }
        }
    }
    Ok(keys)
}

/// Build the PromQL `label/:name/values` query as a `DataFrame` (P4). `__name__`
/// is the normalized metric names plus the synthetic `_bucket`/`_count`/`_sum`
/// histogram series; other labels are the distinct promoted/`prom_attr` values.
pub async fn build_label_values(
    engine: &super::QueryEngine,
    label: &str,
    start_ns: i64,
    end_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::functions::expr_fn::concat;
    use datafusion::prelude::{col, lit};
    // Scope every scan to the requested time window so listing a label only
    // processes rows in range (and gets time-stats pruning) instead of all
    // history — an absent window is `[0, i64::MAX]`, i.e. unchanged behaviour.
    let window = || prom_time_between(start_ns, end_ns);
    if label == "__name__" {
        let names = engine
            .table("metrics")
            .await?
            .filter(window())?
            .select(vec![prom_name_expr().alias("v")])?;
        let variant = |suffix: &str| concat(vec![prom_name_expr(), lit(suffix.to_string())]);
        let bkt = engine
            .table("metrics")
            .await?
            .filter(window().and(col("bucket_counts").is_not_null()))?
            .select(vec![variant("_bucket").alias("v")])?;
        let cnt = engine
            .table("metrics")
            .await?
            .filter(window().and(col("bucket_counts").is_not_null()))?
            .select(vec![variant("_count").alias("v")])?;
        let sm = engine
            .table("metrics")
            .await?
            .filter(window().and(col("bucket_counts").is_not_null()))?
            .select(vec![variant("_sum").alias("v")])?;
        return Ok(names
            .union(bkt)?
            .union(cnt)?
            .union(sm)?
            .filter(col("v").is_not_null())?
            .distinct()?
            .sort(vec![col("v").sort(true, false)])?);
    }
    let lhs = label_lhs_expr(label);
    Ok(engine
        .table("metrics")
        .await?
        .filter(window().and(lhs.clone().is_not_null()))?
        .select(vec![lhs.alias("v")])?
        .distinct()?
        .sort(vec![col("v").sort(true, false)])?)
}

/// Run `label/:name/values` and build `{status, data:[...]}`.
pub async fn handle_label_values(
    engine: &super::QueryEngine,
    label: &str,
    start_ns: i64,
    end_ns: i64,
) -> crate::Result<serde_json::Value> {
    let df = build_label_values(engine, label, start_ns, end_ns).await?;
    let values = string_column_df(engine, df).await?;
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
pub async fn handle_series(
    engine: &super::QueryEngine,
    matcher: Option<&str>,
) -> crate::Result<serde_json::Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let df = build_series(engine, matcher).await?;
    let batches = engine.collect(df).await?;
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
    #[serde(serialize_with = "ser_pairs")]
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

/// Group the rows of an already-built range SQL into per-series point lists.
/// Group an already-built range `DataFrame`'s rows into per-series point lists.
async fn range_series_from_df(
    engine: &super::QueryEngine,
    df: datafusion::dataframe::DataFrame,
) -> crate::Result<RangeSeries> {
    let batches = engine.collect(df).await?;
    group_range_series(&batches)
}

/// Group result batches (`v` + `time_unix_nano` + label columns) into per-series
/// point lists keyed by the (ordered) label set.
fn group_range_series(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> crate::Result<RangeSeries> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type};

    let mut series: RangeSeries = BTreeMap::new();
    for batch in batches {
        let schema = batch.schema();
        let v_idx = schema.index_of("v").map_err(|e| to_err(e.to_string()))?;
        let v = cast(batch.column(v_idx), &DataType::Float64)?;
        let v = v.as_primitive::<Float64Type>();
        let t_idx = schema
            .index_of("time_unix_nano")
            .map_err(|e| to_err(e.to_string()))?;
        let t = cast(batch.column(t_idx), &DataType::Int64)?;
        let t = t.as_primitive::<Int64Type>();
        let cols = LabelCols::build(batch)?;

        for i in 0..batch.num_rows() {
            if v.is_null(i) || t.is_null(i) {
                continue;
            }
            let metric = cols.labels(i);
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
    base: String, // normalized base name (without `_bucket`)
    preds: Vec<datafusion::logical_expr::Expr>, // matcher predicates (excluding `le`)
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
                .filter_map(matcher_expr)
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

    use datafusion::prelude::{col, lit};
    let mut df = engine
        .table("metrics")
        .await?
        .filter(prom_name_expr().eq(lit(spec.base.clone())))?;
    for p in &spec.preds {
        df = df.filter(p.clone())?;
    }
    df = df.filter(prom_time_between(start_ns, end_ns))?;
    let mut proj = vec![
        col("time_unix_nano"),
        col("bucket_counts"),
        col("explicit_bounds"),
    ];
    for g in &spec.group_by {
        proj.push(label_lhs_expr(g).alias(sql_ident(g)));
    }
    let batches = engine.collect(df.select(proj)?).await?;

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
    preds: Vec<datafusion::logical_expr::Expr>,
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
        .filter_map(matcher_expr)
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

    use datafusion::prelude::{col, lit};
    let mut df = engine
        .table("metrics")
        .await?
        .filter(prom_name_expr().eq(lit(spec.base.clone())))?
        .filter(col("bucket_counts").is_not_null())?;
    for p in &spec.preds {
        df = df.filter(p.clone())?;
    }
    df = df.filter(prom_time_between(start_ns, end_ns))?;
    let mut proj = vec![
        col("time_unix_nano"),
        col("bucket_counts"),
        col("explicit_bounds"),
    ];
    for g in &spec.group_by {
        proj.push(label_lhs_expr(g).alias(sql_ident(g)));
    }
    let batches = engine.collect(df.select(proj)?).await?;

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
    let parsed = parser::parse(query).map_err(to_err)?;
    // Classic-histogram queries are computed from OTLP array buckets:
    // histogram_quantile(…) and bare `_bucket`-by-`le` heatmaps.
    if let Some(spec) = detect_hist_quantile(&parsed) {
        return handle_hist_quantile_range(engine, &spec, start_ns, end_ns).await;
    }
    if let Some(spec) = detect_bucket_heatmap(&parsed) {
        return handle_bucket_heatmap(engine, &spec, start_ns, end_ns).await;
    }

    // A top-level topk/bottomk: keep the top-N *series* after merge; evaluate the
    // inner AST node directly.
    let mut topk: Option<(i64, bool)> = None;
    let eval_expr: &Expr = match topk_parts(&parsed) {
        Some((n, is_topk, inner)) => {
            topk = Some((n, is_topk));
            inner
        }
        None => &parsed,
    };

    // A coarse step routes to a rollup tier table — but rollups only cover
    // *sealed* days (the compactor never rolls up the active day). Routing the
    // whole range to the tier would silently drop the live tail, so each window
    // picks its source per the sealed boundary below. When no tier qualifies,
    // every window falls back to raw `metrics`.
    let tier = select_range_table(engine, step_ns);
    // The trailing day of the range is treated as unsealed and read from raw —
    // a rolling `end − 1d` (not a wall-clock "today"), so the boundary day of a
    // historical range is raw too. That is coarser, not wrong: the `metrics`
    // union includes the compacted daily, so the data is present either way.
    let sealed_ns = end_ns.saturating_sub(86_400_000_000_000);
    let windows: Vec<(i64, i64)> = if super::frontend::should_split(start_ns, end_ns) {
        // Per-day shards aligned to UTC midnight; everything before the last day
        // is sealed/cacheable. `split` emits the shard-count metric.
        super::frontend::split(start_ns, end_ns, 0, sealed_ns)
            .into_iter()
            .map(|s| (s.start_ns, s.end_ns))
            .collect()
    } else {
        vec![(start_ns, end_ns)]
    };

    let mut merged: RangeSeries = BTreeMap::new();
    for (s, e) in windows {
        // Sealed windows read the rollup tier; the trailing window — which the
        // tier never covers — reads raw `metrics`.
        let table: &str = if e <= sealed_ns { &tier } else { "metrics" };
        match eval_range_window(engine, eval_expr, s, e, table).await? {
            RangeVal::Vector(part) => {
                for (key, (metric, points)) in part {
                    merged
                        .entry(key)
                        .or_insert_with(|| (metric, Vec::new()))
                        .1
                        .extend(points);
                }
            }
            RangeVal::Scalar(sc) => {
                // A pure scalar range query (e.g. `1`): one empty-label series,
                // constant across the window boundaries.
                #[allow(clippy::cast_precision_loss)]
                let (ts0, ts1) = (s as f64 / 1e9, e as f64 / 1e9);
                let entry = merged
                    .entry("{}".to_string())
                    .or_insert_with(|| (BTreeMap::new(), Vec::new()));
                entry.1.push((ts0, sc));
                entry.1.push((ts1, sc));
            }
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

    // C-P3: resample each series onto the `step` grid (one point per bucket, like
    // Mimir) by carrying the last sample forward within the staleness window.
    // Gated on a sane step count so a tiny/garbage step can't explode the grid.
    if step_ns > 0 && (end_ns - start_ns) / step_ns <= MAX_GRID_POINTS {
        let staleness = step_ns.max(STALENESS_NS);
        for (_metric, points) in &mut series {
            *points = resample_to_grid(points, start_ns, end_ns, step_ns, staleness);
        }
    }
    Ok(PromMatrixResponse::matrix(series))
}

/// Max grid points to resample to (C-P3 guard against a pathological tiny step).
const MAX_GRID_POINTS: i64 = 100_000;
/// Prometheus default lookback delta: carry a sample forward up to 5 minutes.
const STALENESS_NS: i64 = 300_000_000_000;

/// Resample time-ordered `(secs, value)` points onto the `[start, end]` grid at
/// `step_ns`, emitting one point per grid timestamp that has a sample at or
/// before it within `staleness_ns` (last-value-carry-forward).
#[allow(clippy::cast_precision_loss)] // ns→s; sub-ms precision irrelevant for the grid
fn resample_to_grid(
    points: &[(f64, f64)],
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    staleness_ns: i64,
) -> Vec<(f64, f64)> {
    let stale_s = staleness_ns as f64 / 1e9;
    let mut out = Vec::new();
    let mut idx = 0usize;
    let mut last: Option<(f64, f64)> = None;
    let mut g = start_ns;
    while g <= end_ns {
        let gt = g as f64 / 1e9;
        while idx < points.len() && points[idx].0 <= gt + 1e-9 {
            last = Some(points[idx]);
            idx += 1;
        }
        if let Some((lt, lv)) = last
            && gt - lt <= stale_s + 1e-9
        {
            out.push((gt, lv));
        }
        g += step_ns;
    }
    out
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
    use datafusion::arrow::datatypes::DataType::Int64;
    use datafusion::logical_expr::expr_fn::cast as df_cast;
    use datafusion::prelude::{col, lit};
    let mut df = engine
        .table("metrics")
        .await?
        .filter(prom_name_expr().eq(lit(name.to_string())))?;
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_expr(m) {
            df = df.filter(p)?;
        }
    }
    df = df.filter(df_cast(col("time_unix_nano"), Int64).lt_eq(lit(time_ns)))?;
    let base = df.select(vec![
        col("name"),
        col("service_name"),
        col("attributes"),
        col("bucket_counts"),
        col("explicit_bounds"),
        col("time_unix_nano"),
    ])?;
    // Latest histogram row per series at/before the eval time.
    let latest = super::plan::frame::latest_per_series(
        base,
        vec![col("name"), col("service_name"), col("attributes")],
        "time_unix_nano",
    )?;
    let batches = engine
        .collect(latest.select(vec![
            col("service_name"),
            col("attributes"),
            col("bucket_counts"),
            col("explicit_bounds"),
        ])?)
        .await?;
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

// === Binary & unary operators — Rust-side vector matching ===
//
// PromQL binary ops (`a / b`, `1 - x`, `x > 0`, …) require evaluating each
// operand to a set of series and combining them by matching label sets — not
// expressible as one SQL statement. We evaluate operands through the same leaf
// machinery (selectors, rate, aggregates, histogram_quantile) and combine in
// Rust, honoring `on(…)`/`ignoring(…)` and `group_left`/`group_right`. Node
// Exporter ratio panels (`avail / size`, `100 - idle%`) depend on this.

/// A range expression evaluates to either an instant scalar (constant over the
/// range) or a vector of per-series point lists.
enum RangeVal {
    Scalar(f64),
    Vector(RangeSeries),
}

/// An instant expression evaluates to a scalar or a vector of `(labels, value)`.
enum InstantVal {
    Scalar(f64),
    Vector(Vec<(BTreeMap<String, String>, f64)>),
}

/// Vector-matching key derivation: `on(set)` matches on exactly those labels;
/// `ignoring(set)` (and the default) matches on all labels except `__name__`
/// and the ignored set.
enum MatchKind {
    On(Vec<String>),
    Ignoring(Vec<String>),
}

impl MatchKind {
    fn from(modifier: &Option<BinModifier>) -> Self {
        match modifier.as_ref().and_then(|m| m.matching.as_ref()) {
            Some(LabelModifier::Include(l)) => MatchKind::On(l.labels.clone()),
            Some(LabelModifier::Exclude(l)) => MatchKind::Ignoring(l.labels.clone()),
            None => MatchKind::Ignoring(Vec::new()),
        }
    }
    /// Whether label `k` participates in matching / appears in the result.
    fn keeps(&self, k: &str) -> bool {
        match self {
            MatchKind::On(set) => set.iter().any(|s| s == k),
            MatchKind::Ignoring(set) => k != "__name__" && !set.iter().any(|s| s == k),
        }
    }
    /// Stable key over the labels two series must share to match.
    fn key(&self, labels: &BTreeMap<String, String>) -> String {
        let mut kv: Vec<(&str, &str)> = labels
            .iter()
            .filter(|(k, _)| self.keeps(k))
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        kv.sort_unstable();
        format!("{kv:?}")
    }
    /// Labels carried by the result series (from the result-bearing operand).
    fn result_labels(&self, labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
        labels
            .iter()
            .filter(|(k, _)| self.keeps(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

/// Group cardinality + labels copied from the "one" side (`group_left(…)` /
/// `group_right(…)`).
enum Card {
    OneToOne,
    ManyToOne(Vec<String>),
    OneToMany(Vec<String>),
}

fn cardinality(modifier: &Option<BinModifier>) -> Result<Card, String> {
    Ok(match modifier.as_ref().map(|m| &m.card) {
        None | Some(VectorMatchCardinality::OneToOne) => Card::OneToOne,
        Some(VectorMatchCardinality::ManyToOne(l)) => Card::ManyToOne(l.labels.clone()),
        Some(VectorMatchCardinality::OneToMany(l)) => Card::OneToMany(l.labels.clone()),
        Some(VectorMatchCardinality::ManyToMany) => {
            return Err("set operators (and/or/unless) not supported (v1)".to_string());
        }
    })
}

/// Apply a scalar PromQL binary op. For filtering comparisons (no `bool`), emit
/// `keep_val` (the vector operand's value) when the predicate holds, else `None`
/// (the sample is dropped). With `bool`, comparisons emit `1`/`0`.
fn apply_binop(
    op: token::TokenType,
    l: f64,
    r: f64,
    return_bool: bool,
    keep_val: f64,
) -> Option<f64> {
    let cmp = |t: bool| {
        if return_bool {
            Some(if t { 1.0 } else { 0.0 })
        } else if t {
            Some(keep_val)
        } else {
            None
        }
    };
    match op.id() {
        token::T_ADD => Some(l + r),
        token::T_SUB => Some(l - r),
        token::T_MUL => Some(l * r),
        token::T_DIV => Some(l / r),
        token::T_MOD => Some(l % r),
        token::T_POW => Some(l.powf(r)),
        token::T_ATAN2 => Some(l.atan2(r)),
        token::T_EQLC => cmp((l - r).abs() < f64::EPSILON),
        token::T_NEQ => cmp((l - r).abs() >= f64::EPSILON),
        token::T_GTR => cmp(l > r),
        token::T_LSS => cmp(l < r),
        token::T_GTE => cmp(l >= r),
        token::T_LTE => cmp(l <= r),
        _ => None,
    }
}

fn return_bool(modifier: &Option<BinModifier>) -> bool {
    modifier.as_ref().is_some_and(|m| m.return_bool)
}

fn drop_name(mut m: BTreeMap<String, String>) -> BTreeMap<String, String> {
    m.remove("__name__");
    m
}

/// Convert a built matrix response (histogram/bucket fast paths) back into the
/// internal per-series point map so it can feed a binary operand.
fn matrix_to_series(resp: PromMatrixResponse) -> RangeSeries {
    let mut out: RangeSeries = BTreeMap::new();
    for r in resp.data.result {
        let pts: Vec<(f64, f64)> = r
            .values
            .iter()
            .filter_map(|(t, v)| v.parse::<f64>().ok().map(|x| (*t, x)))
            .collect();
        out.insert(format!("{:?}", r.metric), (r.metric, pts));
    }
    out
}

/// Keep the top/bottom-N series by peak value (used for `topk` nested inside a
/// larger expression; the top-level case is handled in [`handle_range`]).
fn topk_series(series: RangeSeries, n: i64, is_topk: bool) -> RangeSeries {
    let mut v: Vec<(String, (BTreeMap<String, String>, Vec<(f64, f64)>))> =
        series.into_iter().collect();
    let score = |p: &[(f64, f64)]| p.iter().map(|x| x.1).fold(f64::MIN, f64::max);
    v.sort_by(|a, b| {
        let (sa, sb) = (score(&a.1.1), score(&b.1.1));
        if is_topk {
            sb.partial_cmp(&sa)
        } else {
            sa.partial_cmp(&sb)
        }
        .unwrap_or(std::cmp::Ordering::Equal)
    });
    v.truncate(usize::try_from(n.max(0)).unwrap_or(usize::MAX));
    v.into_iter().collect()
}

fn scalar_op_scalar(op: token::TokenType, a: f64, b: f64) -> f64 {
    // Scalar/scalar comparisons require `bool` in PromQL; we always yield 0/1.
    apply_binop(op, a, b, true, a).unwrap_or(f64::NAN)
}

/// `scalar ∘ vector` / `vector ∘ scalar` over a range. Result keeps the vector's
/// labels minus `__name__`.
fn scalar_vector_range(
    op: token::TokenType,
    scalar: f64,
    scalar_left: bool,
    vec: RangeSeries,
    rb: bool,
) -> RangeSeries {
    let mut out: RangeSeries = BTreeMap::new();
    for (_k, (labels, points)) in vec {
        let labels = drop_name(labels);
        let mut pts = Vec::new();
        for (t, v) in points {
            let (l, r) = if scalar_left {
                (scalar, v)
            } else {
                (v, scalar)
            };
            if let Some(res) = apply_binop(op, l, r, rb, v) {
                pts.push((t, res));
            }
        }
        if !pts.is_empty() {
            out.insert(format!("{labels:?}"), (labels, pts));
        }
    }
    out
}

/// `vector ∘ vector` over a range: match series by label key, then combine
/// points that share a timestamp.
fn vector_vector_range(
    op: token::TokenType,
    lhs: RangeSeries,
    rhs: RangeSeries,
    modifier: &Option<BinModifier>,
    rb: bool,
) -> Result<RangeSeries, String> {
    let kind = MatchKind::from(modifier);
    let card = cardinality(modifier)?;
    let group_right = matches!(card, Card::OneToMany(_));
    let extra: Vec<String> = match &card {
        Card::ManyToOne(e) | Card::OneToMany(e) => e.clone(),
        Card::OneToOne => Vec::new(),
    };
    let (many, one) = if group_right { (rhs, lhs) } else { (lhs, rhs) };
    // Index the "one" side by match key (first wins on ambiguity).
    let mut one_idx: BTreeMap<String, &(BTreeMap<String, String>, Vec<(f64, f64)>)> =
        BTreeMap::new();
    for entry in one.values() {
        one_idx.entry(kind.key(&entry.0)).or_insert(entry);
    }
    let mut out: RangeSeries = BTreeMap::new();
    for (mlabels, mpoints) in many.values() {
        let Some((olabels, opoints)) = one_idx.get(&kind.key(mlabels)).copied() else {
            continue;
        };
        let mut rl = kind.result_labels(mlabels);
        for e in &extra {
            if let Some(v) = olabels.get(e) {
                rl.insert(e.clone(), v.clone());
            }
        }
        let omap: std::collections::HashMap<u64, f64> =
            opoints.iter().map(|(t, v)| (t.to_bits(), *v)).collect();
        let mut pts = Vec::new();
        for (t, mv) in mpoints {
            if let Some(&ov) = omap.get(&t.to_bits()) {
                let (l, r) = if group_right { (ov, *mv) } else { (*mv, ov) };
                if let Some(res) = apply_binop(op, l, r, rb, l) {
                    pts.push((*t, res));
                }
            }
        }
        if !pts.is_empty() {
            out.insert(format!("{rl:?}"), (rl, pts));
        }
    }
    Ok(out)
}

fn combine_range(
    op: token::TokenType,
    lhs: RangeVal,
    rhs: RangeVal,
    modifier: &Option<BinModifier>,
) -> Result<RangeVal, String> {
    let rb = return_bool(modifier);
    Ok(match (lhs, rhs) {
        (RangeVal::Scalar(a), RangeVal::Scalar(b)) => RangeVal::Scalar(scalar_op_scalar(op, a, b)),
        (RangeVal::Scalar(a), RangeVal::Vector(v)) => {
            RangeVal::Vector(scalar_vector_range(op, a, true, v, rb))
        }
        (RangeVal::Vector(v), RangeVal::Scalar(b)) => {
            RangeVal::Vector(scalar_vector_range(op, b, false, v, rb))
        }
        (RangeVal::Vector(l), RangeVal::Vector(r)) => {
            RangeVal::Vector(vector_vector_range(op, l, r, modifier, rb)?)
        }
    })
}

/// Evaluate a range sub-expression over one `[s, e]` window against `table`.
async fn eval_range_window(
    engine: &super::QueryEngine,
    expr: &Expr,
    s: i64,
    e: i64,
    table: &str,
) -> crate::Result<RangeVal> {
    match expr {
        Expr::NumberLiteral(n) => Ok(RangeVal::Scalar(n.val)),
        Expr::Paren(p) => Box::pin(eval_range_window(engine, &p.expr, s, e, table)).await,
        Expr::Unary(u) => {
            let v = Box::pin(eval_range_window(engine, &u.expr, s, e, table)).await?;
            Ok(match v {
                RangeVal::Scalar(x) => RangeVal::Scalar(-x),
                RangeVal::Vector(mut m) => {
                    for (_labels, pts) in m.values_mut() {
                        for p in pts.iter_mut() {
                            p.1 = -p.1;
                        }
                    }
                    RangeVal::Vector(m)
                }
            })
        }
        Expr::Binary(b) => {
            let l = Box::pin(eval_range_window(engine, &b.lhs, s, e, table)).await?;
            let r = Box::pin(eval_range_window(engine, &b.rhs, s, e, table)).await?;
            combine_range(b.op, l, r, &b.modifier).map_err(to_err)
        }
        _ => {
            if let Some(spec) = detect_hist_quantile(expr) {
                let resp = handle_hist_quantile_range(engine, &spec, s, e).await?;
                Ok(RangeVal::Vector(matrix_to_series(resp)))
            } else if let Some(spec) = detect_bucket_heatmap(expr) {
                let resp = handle_bucket_heatmap(engine, &spec, s, e).await?;
                Ok(RangeVal::Vector(matrix_to_series(resp)))
            } else if let Some((n, is_topk, inner)) = topk_parts(expr) {
                let v = Box::pin(eval_range_window(engine, inner, s, e, table)).await?;
                Ok(match v {
                    RangeVal::Scalar(x) => RangeVal::Scalar(x),
                    RangeVal::Vector(m) => RangeVal::Vector(topk_series(m, n, is_topk)),
                })
            } else {
                let df = lower_range_df(engine, expr, s, e, table).await?;
                Ok(RangeVal::Vector(range_series_from_df(engine, df).await?))
            }
        }
    }
}

/// Run a single-`v`/`time_unix_nano` SQL and group rows into instant samples
/// (latest value per series via [`LabelCols`]).
async fn instant_vector_from_df(
    engine: &super::QueryEngine,
    df: datafusion::dataframe::DataFrame,
) -> crate::Result<Vec<(BTreeMap<String, String>, f64)>> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Float64Type};

    let batches = engine.collect(df).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let v_idx = schema.index_of("v").map_err(|e| to_err(e.to_string()))?;
        let v = cast(batch.column(v_idx), &DataType::Float64)?;
        let v = v.as_primitive::<Float64Type>();
        let cols = LabelCols::build(batch)?;
        for i in 0..batch.num_rows() {
            if v.is_null(i) {
                continue;
            }
            out.push((cols.labels(i), v.value(i)));
        }
    }
    Ok(out)
}

fn scalar_vector_instant(
    op: token::TokenType,
    scalar: f64,
    scalar_left: bool,
    vec: Vec<(BTreeMap<String, String>, f64)>,
    rb: bool,
) -> Vec<(BTreeMap<String, String>, f64)> {
    vec.into_iter()
        .filter_map(|(labels, v)| {
            let (l, r) = if scalar_left {
                (scalar, v)
            } else {
                (v, scalar)
            };
            apply_binop(op, l, r, rb, v).map(|res| (drop_name(labels), res))
        })
        .collect()
}

fn vector_vector_instant(
    op: token::TokenType,
    lhs: Vec<(BTreeMap<String, String>, f64)>,
    rhs: Vec<(BTreeMap<String, String>, f64)>,
    modifier: &Option<BinModifier>,
    rb: bool,
) -> Result<Vec<(BTreeMap<String, String>, f64)>, String> {
    let kind = MatchKind::from(modifier);
    let card = cardinality(modifier)?;
    let group_right = matches!(card, Card::OneToMany(_));
    let extra: Vec<String> = match &card {
        Card::ManyToOne(e) | Card::OneToMany(e) => e.clone(),
        Card::OneToOne => Vec::new(),
    };
    let (many, one) = if group_right { (rhs, lhs) } else { (lhs, rhs) };
    let mut one_idx: BTreeMap<String, (&BTreeMap<String, String>, f64)> = BTreeMap::new();
    for (labels, val) in &one {
        one_idx.entry(kind.key(labels)).or_insert((labels, *val));
    }
    let mut out = Vec::new();
    for (mlabels, mval) in &many {
        let Some((olabels, oval)) = one_idx.get(&kind.key(mlabels)) else {
            continue;
        };
        let (l, r) = if group_right {
            (*oval, *mval)
        } else {
            (*mval, *oval)
        };
        let Some(res) = apply_binop(op, l, r, rb, l) else {
            continue;
        };
        let mut rl = kind.result_labels(mlabels);
        for e in &extra {
            if let Some(v) = olabels.get(e) {
                rl.insert(e.clone(), v.clone());
            }
        }
        out.push((rl, res));
    }
    Ok(out)
}

fn combine_instant(
    op: token::TokenType,
    lhs: InstantVal,
    rhs: InstantVal,
    modifier: &Option<BinModifier>,
) -> Result<InstantVal, String> {
    let rb = return_bool(modifier);
    Ok(match (lhs, rhs) {
        (InstantVal::Scalar(a), InstantVal::Scalar(b)) => {
            InstantVal::Scalar(scalar_op_scalar(op, a, b))
        }
        (InstantVal::Scalar(a), InstantVal::Vector(v)) => {
            InstantVal::Vector(scalar_vector_instant(op, a, true, v, rb))
        }
        (InstantVal::Vector(v), InstantVal::Scalar(b)) => {
            InstantVal::Vector(scalar_vector_instant(op, b, false, v, rb))
        }
        (InstantVal::Vector(l), InstantVal::Vector(r)) => {
            InstantVal::Vector(vector_vector_instant(op, l, r, modifier, rb)?)
        }
    })
}

/// Evaluate an instant sub-expression at `time_ns`.
async fn eval_instant(
    engine: &super::QueryEngine,
    expr: &Expr,
    time_ns: i64,
) -> crate::Result<InstantVal> {
    match expr {
        Expr::NumberLiteral(n) => Ok(InstantVal::Scalar(n.val)),
        Expr::Paren(p) => Box::pin(eval_instant(engine, &p.expr, time_ns)).await,
        Expr::Unary(u) => {
            let v = Box::pin(eval_instant(engine, &u.expr, time_ns)).await?;
            Ok(match v {
                InstantVal::Scalar(x) => InstantVal::Scalar(-x),
                InstantVal::Vector(mut m) => {
                    for s in &mut m {
                        s.1 = -s.1;
                    }
                    InstantVal::Vector(m)
                }
            })
        }
        Expr::Binary(b) => {
            let l = Box::pin(eval_instant(engine, &b.lhs, time_ns)).await?;
            let r = Box::pin(eval_instant(engine, &b.rhs, time_ns)).await?;
            combine_instant(b.op, l, r, &b.modifier).map_err(to_err)
        }
        _ => {
            if let Some((phi, vs)) = histogram_quantile_parts(expr) {
                let resp = handle_histogram(engine, phi, vs, time_ns).await?;
                let v = resp
                    .data
                    .result
                    .into_iter()
                    .map(|s| (s.metric, s.value.1.parse::<f64>().unwrap_or(f64::NAN)))
                    .collect();
                Ok(InstantVal::Vector(v))
            } else {
                let df = lower_instant_df(engine, expr, time_ns).await?;
                Ok(InstantVal::Vector(
                    instant_vector_from_df(engine, df).await?,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matcher_value_is_bound_literal_not_injected() {
        // A PromQL matcher value carrying SQL metacharacters must land as a
        // single bound `Utf8` literal — never interpolated into plan structure.
        let evil = r#"a' OR 1=1 && x"#;
        let m = Matcher::new(MatchOp::Equal, "pod", evil);
        let e = matcher_expr(&m).expect("string matcher lowers");
        let s = format!("{e}");
        assert!(
            s.contains(&format!("Utf8({evil:?})")),
            "value bound as one literal, not injected: {s}"
        );
    }

    #[test]
    fn test_prom_vector_response_shape() {
        let mut m = BTreeMap::new();
        m.insert("service_name".to_string(), "client".to_string());
        let resp = PromResponse::vector([(m, 1700000000.0, 42.0)]);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains(r#""resultType":"vector""#), "json: {json}");
        assert!(
            json.contains(r#""value":[1700000000,"42"]"#),
            "integer seconds (Mimir parity): {json}"
        );
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
            json.contains(r#""values":[[1700000000,"1.5"],[1700000060,"2"]]"#),
            "integer seconds (Mimir parity): {json}"
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
            Field::new("prom_name", DataType::Utf8, false),
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
                Arc::new(StringArray::from(vec![
                    "http_total",
                    "http_total",
                    "http_total",
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
        crate::query::QueryEngine::new(&opts).await.unwrap()
    }

    #[tokio::test]
    async fn test_instant_aggregate_over_rate() {
        // Gauge-panel shape: an *instant* `avg(rate(metric[5m]))` must evaluate
        // (over the [T-5m, T] window) instead of erroring "aggregate inner must
        // be a vector selector".
        let engine = counter_engine().await;
        let resp = handle_instant(&engine, "avg(rate(http_total[5m]))", 3_000_000_000)
            .await
            .unwrap();
        assert_eq!(
            resp.data.result.len(),
            1,
            "one aggregated instant value: {:?}",
            resp.data.result
        );
        // bare instant rate is also accepted (not just inside an aggregate).
        let bare = handle_instant(&engine, "rate(http_total[5m])", 3_000_000_000)
            .await
            .unwrap();
        assert_eq!(bare.data.result.len(), 1, "one rate series: {:?}", bare.data.result);
    }

    #[tokio::test]
    async fn test_bare_selector_range_returns_raw_matrix() {
        let engine = counter_engine().await;
        let resp = handle_range(
            &engine,
            r#"http_total{service_name="client"}"#,
            0,
            10_000_000_000,
            0,
        )
        .await
        .unwrap();
        assert_eq!(resp.data.result_type, "matrix");
        assert_eq!(resp.data.result.len(), 1, "one series");
        let s = &resp.data.result[0];
        assert_eq!(
            s.metric["__name__"], "http_total",
            "normalized name: {:?}",
            s.metric
        );
        assert_eq!(
            s.values,
            vec![
                (1.0, "10".to_string()),
                (2.0, "30".to_string()),
                (3.0, "60".to_string())
            ],
            "raw samples over the range"
        );
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
    async fn test_instant_normalizes_name_and_explodes_attributes() {
        // C-P1: a bare instant selector must return the normalized __name__ and
        // explode the attributes JSON into per-label series (not collapse them).
        use crate::config::query::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("unit", DataType::Utf8, true),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("double_value", DataType::Float64, true),
            Field::new("is_monotonic", DataType::Boolean, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        // Two series of the same OTLP metric, differing only by status_code.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["client", "client"])),
                Arc::new(StringArray::from(vec![
                    "http.server.requests",
                    "http.server.requests",
                ])),
                Arc::new(StringArray::from(vec![Some("By"), Some("By")])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64, 1_000_000_000])
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"http.response.status_code":"200","http.route":"/user"}"#),
                    Some(r#"{"http.response.status_code":"500","http.route":"/user"}"#),
                ])),
                Arc::new(Float64Array::from(vec![3.0, 1.0])),
                Arc::new(datafusion::arrow::array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                ])),
                Arc::new(StringArray::from(vec![
                    "http_server_requests_bytes",
                    "http_server_requests_bytes",
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
        let engine = crate::query::QueryEngine::new(&opts).await.unwrap();

        let resp = handle_instant(&engine, "http_server_requests_bytes", 2_000_000_000)
            .await
            .unwrap();
        // Two distinct series (200 vs 500) — not collapsed into one.
        assert_eq!(
            resp.data.result.len(),
            2,
            "attributes exploded → 2 series: {:?}",
            resp.data.result
        );
        for s in &resp.data.result {
            // normalized __name__ (dots→_, unit suffix), not the raw dotted name
            assert_eq!(
                s.metric["__name__"], "http_server_requests_bytes",
                "metric: {:?}",
                s.metric
            );
            assert_eq!(s.metric["service_name"], "client");
            assert_eq!(s.metric["http_route"], "/user");
            assert!(
                s.metric.contains_key("http_response_status_code"),
                "status label: {:?}",
                s.metric
            );
        }
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
    async fn test_long_range_keeps_live_tail_when_tier_selected() {
        // Regression: a coarse-step long range routes to a rollup tier table
        // (`metrics_5m`). Rollups only cover *sealed* days — the active day is
        // never rolled up. Routing the *whole* range to the tier silently drops
        // the live tail (the symptom: rate panels miss recent data while the raw
        // histogram path shows it). The live (unsealed) shard must read raw.
        use crate::config::query::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        const DAY_NS: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;

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
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let mk = |times: &[i64], vals: &[f64]| {
            let n = times.len();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["s"; n])),
                    Arc::new(StringArray::from(vec!["reqs"; n])),
                    Arc::new(StringArray::from(vec![r#"{"sc":"a"}"#; n])),
                    Arc::new(TimestampNanosecondArray::from(times.to_vec()).with_timezone("UTC")),
                    Arc::new(Float64Array::from(vals.to_vec())),
                    Arc::new(StringArray::from(vec!["reqs"; n])),
                ],
            )
            .unwrap()
        };
        // raw `metrics`: a monotonic counter spanning day 0 (sealed) AND day 1
        // (live). Day-0 raw rises 10→20 (rate 1/30) — deliberately a *different*
        // slope from the tier below, so we can tell which source served day 0.
        let raw = mk(
            &[M5, 2 * M5, DAY_NS + M5, DAY_NS + 2 * M5],
            &[10.0, 20.0, 30.0, 40.0],
        );
        let f = std::fs::File::create(dir.join("m.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&raw).unwrap();
        w.close().unwrap();
        // rollup-5m tier: ONLY the sealed day 0 (the live day is never rolled
        // up). Rises 10→40 (rate 3/30 = 0.1) — distinct from day-0 raw.
        let rolled = mk(&[M5, 2 * M5], &[10.0, 40.0]);
        let f = std::fs::File::create(dir.join("rollup-5m.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&rolled).unwrap();
        w.close().unwrap();

        let opts = QuerierOptions {
            storage: StorageConfig {
                path: tmp.path().into(),
                url: None,
            },
            ..QuerierOptions::default()
        };
        let engine = crate::query::QueryEngine::new(&opts).await.unwrap();
        assert!(engine.has_table("metrics_5m"), "tier registered");

        // 5-minute step over a 2-day range → splits per day AND selects the M5 tier.
        let resp = handle_range(&engine, "sum by (sc) (rate(reqs[5m]))", 0, 2 * DAY_NS, M5)
            .await
            .unwrap();
        assert_eq!(
            resp.data.result.len(),
            1,
            "one series: {:?}",
            resp.data.result
        );
        let pts = &resp.data.result[0].values;
        #[allow(clippy::cast_precision_loss)]
        let day1_start_s = DAY_NS as f64 / 1e9;
        let val = |ts: f64| -> Option<f64> {
            pts.iter()
                .find(|(t, _)| (*t - ts).abs() < 1.0)
                .and_then(|(_, v)| v.parse::<f64>().ok())
        };
        #[allow(clippy::cast_precision_loss)]
        let (day0_ts, day1_ts) = (2.0 * M5 as f64 / 1e9, (DAY_NS + 2 * M5) as f64 / 1e9);
        // Day 0 (sealed) is served by the tier — its 0.1 slope, NOT raw's 1/30.
        assert!(
            val(day0_ts).is_some_and(|v| (v - 0.1).abs() < 1e-6),
            "sealed day 0 must come from the tier (rate 0.1), got: {pts:?}"
        );
        // Day 1 (trailing/live) survives — and is served by raw (its 1/30 slope).
        assert!(
            val(day1_ts).is_some_and(|v| (v - 1.0 / 30.0).abs() < 1e-6),
            "live day 1 must survive via raw (rate 1/30), got: {pts:?}"
        );
        assert!(
            pts.iter().any(|(ts, _)| *ts >= day1_start_s),
            "live (day-1) data must survive tier routing, got: {pts:?}"
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
            Field::new("prom_name", DataType::Utf8, false),
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
                Arc::new(StringArray::from(vec!["reqs", "reqs", "reqs", "reqs"])),
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
            Field::new("prom_name", DataType::Utf8, false),
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
                Arc::new(StringArray::from(vec!["http_server_request_duration_seconds"])),
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
            Field::new("prom_name", DataType::Utf8, false),
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
                Arc::new(StringArray::from(vec![
                    "http_server_request_duration_seconds",
                    "http_server_request_duration_seconds",
                ])),
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

    // --- binary / unary operators ---

    fn binop_of(q: &str) -> (token::TokenType, Option<BinModifier>) {
        match parser::parse(q).unwrap() {
            Expr::Binary(b) => (b.op, b.modifier),
            other => panic!("not a binary expr: {other:?}"),
        }
    }

    #[test]
    fn test_apply_binop_arithmetic_and_comparison() {
        let (add, _) = binop_of("a + b");
        assert_eq!(apply_binop(add, 2.0, 3.0, false, 0.0), Some(5.0));
        let (gt, _) = binop_of("a > b");
        // filtering comparison keeps the operand value when true, drops when false
        assert_eq!(apply_binop(gt, 10.0, 5.0, false, 10.0), Some(10.0));
        assert_eq!(apply_binop(gt, 3.0, 5.0, false, 3.0), None);
        // `bool` modifier → 0/1 instead of filtering
        assert_eq!(apply_binop(gt, 3.0, 5.0, true, 3.0), Some(0.0));
        assert_eq!(apply_binop(gt, 9.0, 5.0, true, 9.0), Some(1.0));
    }

    #[test]
    fn test_vector_vector_instant_matches_and_drops_name() {
        let (op, modifier) = binop_of("a / b");
        let mk = |name: &str, svc: &str, code: &str| {
            let mut m = BTreeMap::new();
            m.insert("__name__".to_string(), name.to_string());
            m.insert("service_name".to_string(), svc.to_string());
            m.insert("code".to_string(), code.to_string());
            m
        };
        // default matching is on all labels except __name__: {service_name,code}.
        let out = vector_vector_instant(
            op,
            vec![(mk("a", "x", "200"), 10.0), (mk("a", "x", "500"), 4.0)],
            vec![(mk("b", "x", "200"), 2.0), (mk("b", "x", "500"), 4.0)],
            &modifier,
            false,
        )
        .unwrap();
        let by: BTreeMap<String, f64> = out.iter().map(|(m, v)| (m["code"].clone(), *v)).collect();
        assert_eq!(by["200"], 5.0);
        assert_eq!(by["500"], 1.0);
        assert!(
            out.iter().all(|(m, _)| !m.contains_key("__name__")),
            "drops __name__"
        );
    }

    #[test]
    fn test_ignoring_matches_across_extra_label() {
        // `a / ignoring(code) b` matches a{code=…} against b with no code label.
        let (op, modifier) = binop_of("a / ignoring(code) b");
        let mut a = BTreeMap::new();
        a.insert("__name__".to_string(), "a".to_string());
        a.insert("service_name".to_string(), "x".to_string());
        a.insert("code".to_string(), "200".to_string());
        let mut b = BTreeMap::new();
        b.insert("__name__".to_string(), "b".to_string());
        b.insert("service_name".to_string(), "x".to_string());
        let out =
            vector_vector_instant(op, vec![(a, 9.0)], vec![(b, 3.0)], &modifier, false).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, 3.0);
        // `ignoring(code)` drops code from the result too.
        assert!(!out[0].0.contains_key("code"), "result: {:?}", out[0].0);
        assert_eq!(out[0].0["service_name"], "x");
    }

    #[tokio::test]
    async fn test_range_scalar_division() {
        let engine = counter_engine().await;
        let resp = handle_range(
            &engine,
            r#"http_total{service_name="client"} / 10"#,
            0,
            10_000_000_000,
            0,
        )
        .await
        .unwrap();
        assert_eq!(resp.data.result.len(), 1, "{:?}", resp.data.result);
        assert_eq!(
            resp.data.result[0].values,
            vec![
                (1.0, "1".to_string()),
                (2.0, "3".to_string()),
                (3.0, "6".to_string())
            ]
        );
        assert!(
            !resp.data.result[0].metric.contains_key("__name__"),
            "binary op drops __name__: {:?}",
            resp.data.result[0].metric
        );
    }

    #[tokio::test]
    async fn test_instant_scalar_mul_and_unary_minus() {
        let engine = counter_engine().await;
        let resp = handle_instant(&engine, "http_total * 2", 3_000_000_000)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1);
        assert_eq!(resp.data.result[0].value.1, "120");
        let neg = handle_instant(&engine, "- http_total", 3_000_000_000)
            .await
            .unwrap();
        assert_eq!(neg.data.result[0].value.1, "-60");
    }

    #[tokio::test]
    async fn test_instant_vector_vector_self_ratio() {
        let engine = counter_engine().await;
        let resp = handle_instant(&engine, "http_total / http_total", 3_000_000_000)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1);
        assert_eq!(resp.data.result[0].value.1, "1");
        assert!(!resp.data.result[0].metric.contains_key("__name__"));
    }

    #[tokio::test]
    async fn test_instant_comparison_filters_and_bool() {
        let engine = counter_engine().await;
        let none = handle_instant(&engine, "http_total > 100", 3_000_000_000)
            .await
            .unwrap();
        assert!(
            none.data.result.is_empty(),
            "60 > 100 is false → filtered out"
        );
        let some = handle_instant(&engine, "http_total > 50", 3_000_000_000)
            .await
            .unwrap();
        assert_eq!(
            some.data.result[0].value.1, "60",
            "kept value is the LHS sample"
        );
        let b = handle_instant(&engine, "http_total > bool 100", 3_000_000_000)
            .await
            .unwrap();
        assert_eq!(b.data.result[0].value.1, "0", "bool comparison → 0/1");
    }

    // --- C-P3 step-alignment ---

    #[test]
    fn test_resample_to_grid_carries_forward() {
        // samples at 1s,2s,3s → grid 0..5s @1s: 0 dropped (no prior sample),
        // 4s/5s carry the last value forward (within staleness).
        let pts = [(1.0, 10.0), (2.0, 30.0), (3.0, 60.0)];
        let out = resample_to_grid(&pts, 0, 5_000_000_000, 1_000_000_000, STALENESS_NS);
        assert_eq!(
            out,
            vec![
                (1.0, 10.0),
                (2.0, 30.0),
                (3.0, 60.0),
                (4.0, 60.0),
                (5.0, 60.0)
            ]
        );
    }

    #[test]
    fn test_resample_drops_stale_points() {
        // a single sample at 1s, short staleness → only grid points within the
        // window carry it; later grid points go stale and are dropped.
        let pts = [(1.0, 7.0)];
        let out = resample_to_grid(&pts, 0, 5_000_000_000, 1_000_000_000, 1_500_000_000);
        // 1s (exact), 2s (Δ=1s ≤ 1.5s) kept; 3s (Δ=2s) onward dropped.
        assert_eq!(out, vec![(1.0, 7.0), (2.0, 7.0)]);
    }

    #[tokio::test]
    async fn test_range_step_aligns_to_grid() {
        let engine = counter_engine().await;
        let resp = handle_range(
            &engine,
            r#"http_total{service_name="client"}"#,
            0,
            5_000_000_000,
            1_000_000_000,
        )
        .await
        .unwrap();
        assert_eq!(resp.data.result.len(), 1);
        // grid-aligned, one point per step, last value carried forward.
        assert_eq!(
            resp.data.result[0].values,
            vec![
                (1.0, "10".to_string()),
                (2.0, "30".to_string()),
                (3.0, "60".to_string()),
                (4.0, "60".to_string()),
                (5.0, "60".to_string()),
            ]
        );
    }
}
