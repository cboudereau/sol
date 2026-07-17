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
/// Apply a Prometheus `match[]` selector (metric name + label matchers) to the
/// metrics table. Shared by `/series` and `/label/:name/values` so both honor
/// the selector — e.g. a `$host` variable query `label_values(m{service_name=
/// "X"}, host)` must scope `host` to that service, not return every host.
fn apply_match_selector(
    mut df: datafusion::dataframe::DataFrame,
    matcher: Option<&str>,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    if let Some(sel) = matcher.map(str::trim).filter(|s| !s.is_empty()) {
        let expr = parser::parse(sel).map_err(to_err)?;
        let Expr::VectorSelector(vs) = &expr else {
            return Err(to_err("match[] must be a metric selector".to_string()));
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
    Ok(df)
}

/// Build the `/series` result: distinct `(name, service_name)` pairs, optionally
/// narrowed by a `match[]` selector.
pub async fn build_series(
    engine: &super::QueryEngine,
    matcher: Option<&str>,
    time_range: Option<(i64, i64)>,
    now_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let project = |df: datafusion::dataframe::DataFrame| -> crate::Result<_> {
        Ok(apply_match_selector(df, matcher)?
            .select(vec![prom_name_expr().alias("name"), col("service_name")])?
            .distinct()?)
    };
    // FR5: with an explicit `[start, end]` range, enumerate the distinct series
    // over the resolver's source windows (coarsest tier ≤ ∞ for the sealed span,
    // raw for the trailing ≤1-day window) and UNION the per-window distinct sets.
    // Metadata computes no values, so capability `Last` is always tier-eligible;
    // the rollup preserves the full series set, so the union equals the raw-only
    // enumeration. Without an explicit range we cannot split sealed/live, so we
    // keep the raw `metrics` scan (the active day lives only in raw) — conservative.
    let dfs = metadata_sources(engine, time_range, now_ns).await?;
    let mut acc: Option<datafusion::dataframe::DataFrame> = None;
    for df in dfs {
        let part = project(df)?;
        acc = Some(match acc {
            Some(a) => a.union(part)?.distinct()?,
            None => part,
        });
    }
    acc.ok_or_else(|| to_err("build_series: no source windows".to_string()))
}

/// The metadata-path source `DataFrame`s for an optional explicit `[start, end]`
/// range (FR5). With a range, [`resolve_metric_windows`] (capability `Last`,
/// resolution `i64::MAX` → coarsest available tier for the sealed span) yields
/// the time-disjoint `(table, lo, hi)` windows; each window's scan is filtered to
/// its `[lo, hi]` so the unioned distinct enumeration equals the raw-only result.
/// Without a range, a single unfiltered raw `metrics` scan (the active day lives
/// only in raw and there is no sealed/live split to compute) — conservative.
async fn metadata_sources(
    engine: &super::QueryEngine,
    time_range: Option<(i64, i64)>,
    now_ns: i64,
) -> crate::Result<Vec<datafusion::dataframe::DataFrame>> {
    let Some((start_ns, end_ns)) = time_range else {
        return Ok(vec![engine.table("metrics").await?]);
    };
    let windows = resolve_metric_windows(engine, start_ns, end_ns, i64::MAX, Capability::Last, now_ns);
    let mut out = Vec::with_capacity(windows.len());
    for (table, lo, hi) in windows {
        // FR1: each window scan prunes to its own `[lo, hi]` file interval.
        let scope = super::QueryScope {
            lo_ns: lo,
            hi_ns: hi,
        };
        out.push(
            engine
                .table_scoped(&table, scope)
                .await?
                .filter(prom_time_between(lo, hi))?,
        );
    }
    Ok(out)
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

/// Per-sample base over `table` as a `DataFrame` (P3): the matched series'
/// identity columns (`prom_name, name, service_name, attributes, time`) plus the
/// `value_cols` value projections. Most callers want the single coalesced value
/// `v`; tier `*_over_time` windows project the per-bucket aggregate column(s)
/// instead (FR7, via [`metric_value_cols`]).
async fn metric_base_df(
    engine: &super::QueryEngine,
    vs: &VectorSelector,
    start_ns: i64,
    end_ns: i64,
    table: &str,
    value_cols: Vec<datafusion::logical_expr::Expr>,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let name = vs
        .name
        .as_deref()
        .ok_or_else(|| to_err("metric selector requires a name".to_string()))?;
    // FR1: prune the scan to the caller's window. `[start_ns, end_ns]` already
    // includes any lookback the caller computed (shard `query_start_ns` /
    // `instant_range_windows` LAG extension), so the scope is exactly the scan
    // window — `table_scoped` must not re-widen beyond its fixed margin.
    let scope = super::QueryScope {
        lo_ns: start_ns,
        hi_ns: end_ns,
    };
    let mut df = engine
        .table_scoped(table, scope)
        .await?
        .filter(name_pred_expr(name))?;
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_expr(m) {
            df = df.filter(p)?;
        }
    }
    df = df.filter(prom_time_between(start_ns, end_ns))?;
    let mut proj = vec![
        prom_name_expr().alias("prom_name"),
        col("name"),
        col("service_name"),
        col("attributes"),
        col("time_unix_nano"),
    ];
    proj.extend(value_cols);
    Ok(df.select(proj)?)
}

/// The default single value projection `[coalesced v]` for the non-over-time
/// paths (selectors, `rate`/`increase`, raw windows).
fn metric_value_cols(name: &str) -> Vec<datafusion::logical_expr::Expr> {
    vec![metric_value_expr(name).alias("v")]
}

/// A groupable series key over the columnar `attributes` MAP. DataFusion cannot
/// `PARTITION BY`/`GROUP BY` a `Map` column, so window partitions key on this
/// `prom_series_key(attributes)` UDF output instead (promql-pushdown T7).
fn prom_series_key_expr() -> datafusion::logical_expr::Expr {
    use datafusion::prelude::col;
    super::udf::prom_series_key_udf().call(vec![col("attributes")])
}

/// The `(name, service_name, series-key)` window partition for PromQL.
fn prom_part() -> Vec<datafusion::logical_expr::Expr> {
    use datafusion::prelude::col;
    vec![col("name"), col("service_name"), prom_series_key_expr()]
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

/// Internal column the inner `prom_group_key` is renamed to before re-projection,
/// so the outer aggregate can alias its own group key back to `prom_group_key`
/// without colliding with the inner column of the same name (DataFusion rejects
/// an aggregate whose group alias shadows an input column).
const INNER_GROUP_KEY: &str = "prom_group_key_inner";

/// Whether an already-lowered inner frame is a **nested** aggregate (carries a
/// `prom_group_key` column) rather than a **leaf** (carries label columns). Called
/// before [`rename_inner_group_key`], so it tests the original column name.
fn inner_is_nested(inner: &datafusion::dataframe::DataFrame) -> bool {
    inner
        .schema()
        .has_column(&datafusion::common::Column::from_name("prom_group_key"))
}

/// The group-key column for an aggregate's already-lowered inner frame, *after*
/// [`rename_inner_group_key`] has run — so a nested inner is detected by the
/// presence of [`INNER_GROUP_KEY`].
///
/// A **leaf** inner (selector / `rate` / `over_time`) carries `attributes` +
/// `service_name`, so the key is `prom_group_key(attributes, service_name, …)`. A
/// **nested** inner re-projects its key via `prom_group_key_reproject(…)` — both
/// share the `GroupKey` core, which is what makes mixed nesting (e.g.
/// `sum by (cpu) (sum without (mode) (m))`) correct.
fn agg_group_key_expr(
    inner: &datafusion::dataframe::DataFrame,
    grouping: &AggGrouping,
) -> datafusion::logical_expr::Expr {
    use super::group_key::{prom_group_key_call, prom_group_key_reproject_call};
    use datafusion::prelude::col;
    if inner
        .schema()
        .has_column(&datafusion::common::Column::from_name(INNER_GROUP_KEY))
    {
        prom_group_key_reproject_call(col(INNER_GROUP_KEY), grouping)
    } else {
        prom_group_key_call(col("attributes"), col("service_name"), grouping)
    }
}

/// Rename a nested inner's `prom_group_key` column to [`INNER_GROUP_KEY`] so the
/// outer aggregate can alias its re-projected key back to `prom_group_key`. A leaf
/// inner (no such column) passes through unchanged.
fn rename_inner_group_key(
    inner: datafusion::dataframe::DataFrame,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    if inner_is_nested(&inner) {
        Ok(inner.with_column_renamed("prom_group_key", INNER_GROUP_KEY)?)
    } else {
        Ok(inner)
    }
}

/// Lower an aggregate over a **range** expression to the canonical aggregate frame
/// `[prom_group_key, time_unix_nano, v]` via `GROUP BY prom_group_key, time` +
/// `agg(v)` ([ADR: aggregation-pushdown]). Nested aggregates chain through the
/// same path over the uniform frame.
///
/// [ADR: aggregation-pushdown]: ../../docs/20260615_promql-pushdown/adrs/2026-06-15_aggregation-pushdown.md
async fn lower_aggregate_range(
    engine: &super::QueryEngine,
    agg: &AggregateExpr,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let op = agg_name(agg.op).map_err(to_err)?;
    let grouping = AggGrouping::from(&agg.modifier);
    let inner =
        Box::pin(lower_aggregate_inner_range(engine, agg.expr.as_ref(), start_ns, end_ns, table))
            .await?;
    let inner = rename_inner_group_key(inner)?;
    let key = agg_group_key_expr(&inner, &grouping).alias("prom_group_key");
    let v = agg_value_expr(op, col("v")).alias("v");
    Ok(inner.aggregate(vec![key, col("time_unix_nano")], vec![v])?)
}

/// Lower the **inner** of a range aggregate to a frame carrying `v` +
/// `time_unix_nano` plus either label columns (leaf) or a `prom_group_key`
/// column (nested aggregate), ready for [`agg_group_key_expr`].
async fn lower_aggregate_inner_range(
    engine: &super::QueryEngine,
    expr: &Expr,
    start_ns: i64,
    end_ns: i64,
    table: &str,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    match expr {
        Expr::Paren(p) => {
            Box::pin(lower_aggregate_inner_range(engine, &p.expr, start_ns, end_ns, table)).await
        }
        Expr::Aggregate(inner) => {
            Box::pin(lower_aggregate_range(engine, inner, start_ns, end_ns, table)).await
        }
        // A leaf range inner (bare selector / rate / *_over_time): carries
        // `attributes` + `service_name`, the leaf group-key inputs.
        _ => lower_range_df(engine, expr, start_ns, end_ns, table).await,
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
    use super::plan::frame::{irate, rate};
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
            let name = vs
                .name
                .as_deref()
                .ok_or_else(|| to_err("metric selector requires a name".to_string()))?;
            let part = prom_part();
            let r = range_to_ns(range);
            let is_tier = table != "metrics";
            match c.func.name {
                // Windowed Prometheus semantics: reset-adjusted increase over the
                // matrix window `[w]`, divided by `w` seconds for `rate` (kept raw
                // for `increase`). `irate` is the latest inter-sample slope. These
                // need the last cumulative value per bucket — the coalesced `v` is
                // correct on both raw and tier (Capability::Last), so no override.
                "rate" => {
                    let base =
                        metric_base_df(engine, vs, start_ns, end_ns, table, metric_value_cols(name))
                            .await?;
                    rate(base, part, "v", "time_unix_nano", r, true)
                }
                "increase" => {
                    let base =
                        metric_base_df(engine, vs, start_ns, end_ns, table, metric_value_cols(name))
                            .await?;
                    rate(base, part, "v", "time_unix_nano", r, false)
                }
                "irate" => {
                    let base =
                        metric_base_df(engine, vs, start_ns, end_ns, table, metric_value_cols(name))
                            .await?;
                    irate(base, part, "v", "time_unix_nano")
                }
                // `*_over_time`: on a tier window the value comes from the matching
                // per-bucket aggregate column (FR7) rather than recomputing over the
                // last-valued `v` (which would drop intra-bucket detail); on a raw
                // window the coalesced `v` with the natural agg (unchanged).
                "max_over_time" | "min_over_time" | "avg_over_time" | "sum_over_time"
                | "count_over_time" => {
                    lower_over_time(engine, c.func.name, vs, name, start_ns, end_ns, table,
                        is_tier, part, r)
                        .await
                }
                other => Err(to_err(format!(
                    "unsupported range function: {other}() (v1)"
                ))),
            }
        }
        Expr::Paren(p) => Box::pin(lower_range_df(engine, &p.expr, start_ns, end_ns, table)).await,
        Expr::Aggregate(agg) => {
            Box::pin(lower_aggregate_range(engine, agg, start_ns, end_ns, table)).await
        }
        Expr::VectorSelector(vs) => {
            Ok(metric_base_df(engine, vs, start_ns, end_ns, table, metric_value_cols(
                vs.name.as_deref().unwrap_or_default(),
            ))
            .await?
            .sort(vec![col("time_unix_nano").sort(true, false)])?)
        }
        _ => Err(to_err(
            "unsupported PromQL expression for query_range (v1)".to_string(),
        )),
    }
}

/// Lower a `*_over_time` range function with capability-aware value selection
/// (FR7). On a **raw** window the agg runs over the coalesced `v` with its
/// natural [`OverTimeAgg`]. On a **tier** window the per-bucket aggregate column
/// is selected per the [operator → capability ADR][adr]: `max→MAX(value_max)`,
/// `min→MIN(value_min)`, `sum→SUM(value_sum)`, `count→SUM(value_count)`, and
/// `avg→Σvalue_sum/Σvalue_count` (the one case a single windowed column cannot
/// express — see [`super::plan::frame::over_time_ratio`]).
///
/// [adr]: ../../../docs/workspace/rollup-read-routing/adrs/operator-safety-allowlist.md
/// The single per-op **tier** value-column + merge agg for a `*_over_time` op
/// (FR7), shared by the range ([`lower_over_time`]) and instant
/// ([`over_time_window_value`]) paths so the mapping lives in one place:
/// `max→MAX(value_max)`, `min→MIN(value_min)`, `sum→SUM(value_sum)`,
/// `count→SUM(value_count)` (the per-bucket counts are *summed*, not counted).
/// `None` for `avg_over_time` (handled separately via the two-column ratio frame)
/// and any op the classifier should never route here.
fn tier_over_time_value(
    op: &str,
) -> Option<(datafusion::logical_expr::Expr, super::plan::frame::OverTimeAgg)> {
    use super::plan::frame::OverTimeAgg;
    use datafusion::prelude::col;
    match op {
        "max_over_time" => Some((col("value_max"), OverTimeAgg::Max)),
        "min_over_time" => Some((col("value_min"), OverTimeAgg::Min)),
        "sum_over_time" => Some((col("value_sum"), OverTimeAgg::Sum)),
        "count_over_time" => Some((col("value_count"), OverTimeAgg::Sum)),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn lower_over_time(
    engine: &super::QueryEngine,
    op: &str,
    vs: &VectorSelector,
    name: &str,
    start_ns: i64,
    end_ns: i64,
    table: &str,
    is_tier: bool,
    part: Vec<datafusion::logical_expr::Expr>,
    range_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use super::plan::frame::{OverTimeAgg, over_time, over_time_ratio};
    use datafusion::prelude::col;
    // avg over a tier is the ratio of two windowed sums — it needs both the
    // `value_sum` and `value_count` columns in the base, then a dedicated frame.
    if is_tier && op == "avg_over_time" {
        let base = metric_base_df(
            engine,
            vs,
            start_ns,
            end_ns,
            table,
            vec![col("value_sum").alias("v"), col("value_count").alias("c")],
        )
        .await?;
        return over_time_ratio(base, part, "v", "c", "time_unix_nano", range_ns);
    }
    // The single value column + merge agg for this op on this source.
    let (value_expr, agg) = if is_tier {
        // avg is special-cased above, so `None` here is a genuine miswire.
        match tier_over_time_value(op) {
            Some(va) => va,
            None => return Err(to_err(format!("unexpected over_time op: {op}"))),
        }
    } else {
        let agg = match op {
            "max_over_time" => OverTimeAgg::Max,
            "min_over_time" => OverTimeAgg::Min,
            "avg_over_time" => OverTimeAgg::Avg,
            "sum_over_time" => OverTimeAgg::Sum,
            "count_over_time" => OverTimeAgg::Count,
            other => return Err(to_err(format!("unexpected over_time op: {other}"))),
        };
        (metric_value_expr(name), agg)
    };
    let base =
        metric_base_df(engine, vs, start_ns, end_ns, table, vec![value_expr.alias("v")]).await?;
    over_time(base, part, "v", "time_unix_nano", range_ns, agg)
}

/// The per-window value projection(s) for a `*_over_time` op, **normalised** so a
/// single merge agg is exact across a `union` of tier and raw windows (the instant
/// path collapses the whole `[t-window, t]` span to one value, so a tier window and
/// the trailing raw window must aggregate together — a per-window split then
/// latest-pick would drop the sealed window's contribution). On a tier window the
/// per-bucket aggregate column is used (FR7); a raw window normalises to the same
/// merge semantics: `count_over_time` counts each raw sample as `1`, the others
/// reduce over the raw sample value. The returned `(value_cols, agg)` feed
/// [`super::plan::frame::over_time`]; `avg_over_time` is handled separately (it
/// needs the two-column ratio frame).
fn over_time_window_value(
    op: &str,
    name: &str,
    is_tier: bool,
) -> (
    Vec<datafusion::logical_expr::Expr>,
    super::plan::frame::OverTimeAgg,
) {
    use super::plan::frame::OverTimeAgg;
    use datafusion::prelude::lit;
    if is_tier {
        // The op set is guaranteed by `op_capability`; avg is handled separately
        // by the caller before reaching here, so `None` is unreachable.
        let value = tier_over_time_value(op)
            .unwrap_or_else(|| unreachable!("over_time op gated by op_capability: {op}"));
        (vec![value.0.alias("v")], value.1)
    } else {
        let value = match op {
            "max_over_time" => (metric_value_expr(name), OverTimeAgg::Max),
            "min_over_time" => (metric_value_expr(name), OverTimeAgg::Min),
            "sum_over_time" => (metric_value_expr(name), OverTimeAgg::Sum),
            // A raw sample counts as one toward the merged `SUM(count)` — keeping
            // the same merge agg as the tier arm so the union is exact.
            "count_over_time" => (lit(1.0_f64), OverTimeAgg::Sum),
            _ => (metric_value_expr(name), OverTimeAgg::Max),
        };
        (vec![value.0.alias("v")], value.1)
    }
}

/// The unified `[service_name, attributes, time_unix_nano, v]` frame for a leaf
/// **instant** range function evaluated over the resolver `windows` (FR4/FR7). The
/// per-window bases are `union`-ed *before* the single windowing pass so the value
/// at `t` aggregates the entire `[t-window, t]` span across the tier/raw boundary
/// (a per-window split would lose the sealed window — see [`over_time_window_value`]).
/// `rate`/`increase`/`irate` need the last cumulative `v` (Capability::Last), exact
/// on both raw and tier, so they union the coalesced `v` and slope once over it.
async fn instant_leaf_frame(
    engine: &super::QueryEngine,
    op: &str,
    vs: &VectorSelector,
    name: &str,
    windows: &[MetricWindow],
    range_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use super::plan::frame::{irate, over_time, over_time_ratio, rate};
    use datafusion::prelude::{col, lit};
    let part = prom_part();
    // Build the unioned base with the op-appropriate, union-compatible value cols.
    let value_cols = |is_tier: bool| -> Vec<datafusion::logical_expr::Expr> {
        match op {
            "rate" | "increase" | "irate" => metric_value_cols(name),
            "avg_over_time" if is_tier => {
                vec![col("value_sum").alias("v"), col("value_count").alias("c")]
            }
            "avg_over_time" => vec![metric_value_expr(name).alias("v"), lit(1.0_f64).alias("c")],
            _ => over_time_window_value(op, name, is_tier).0,
        }
    };
    let mut base: Option<datafusion::dataframe::DataFrame> = None;
    for (table, lo, hi) in windows {
        let is_tier = table != "metrics";
        let part_df = metric_base_df(engine, vs, *lo, *hi, table, value_cols(is_tier)).await?;
        base = Some(match base {
            Some(acc) => acc.union(part_df)?,
            None => part_df,
        });
    }
    let base = base.ok_or_else(|| to_err("resolver returned no windows".to_string()))?;
    match op {
        "rate" => rate(base, part, "v", "time_unix_nano", range_ns, true),
        "increase" => rate(base, part, "v", "time_unix_nano", range_ns, false),
        "irate" => irate(base, part, "v", "time_unix_nano"),
        "avg_over_time" => over_time_ratio(base, part, "v", "c", "time_unix_nano", range_ns),
        _ => {
            // The merge agg is the same across the tier/raw arms by construction, so
            // either arm yields it; pick the raw arm's (`is_tier=false` is irrelevant
            // for the agg, only the value col differed, already applied above).
            let (_v, agg) = over_time_window_value(op, name, false);
            over_time(base, part, "v", "time_unix_nano", range_ns, agg)
        }
    }
}

/// Lower a leaf **instant** range expression (`rate(m[w])` / `<fn>_over_time(m[w])`)
/// to the `[service_name, attributes, time_unix_nano, v]` frame over the resolved
/// `windows`. Mirrors the leaf arm of [`lower_range_df`] but unions the windows at
/// the base (FR4) so the instant value aggregates across the sealed/live boundary.
async fn lower_instant_leaf(
    engine: &super::QueryEngine,
    expr: &Expr,
    windows: &[MetricWindow],
    range_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    let Expr::Call(c) = expr else {
        return Err(to_err(
            "instant range function expects a range vector like m[5m] (v1)".to_string(),
        ));
    };
    let vs = match c.args.args.first().map(std::convert::AsRef::as_ref) {
        Some(Expr::MatrixSelector(ms)) => &ms.vs,
        _ => {
            return Err(to_err(format!(
                "{}() expects a range-vector argument like m[5m]",
                c.func.name
            )));
        }
    };
    let name = vs
        .name
        .as_deref()
        .ok_or_else(|| to_err("metric selector requires a name".to_string()))?;
    match c.func.name {
        "rate" | "increase" | "irate" | "max_over_time" | "min_over_time" | "avg_over_time"
        | "sum_over_time" | "count_over_time" => {
            instant_leaf_frame(engine, c.func.name, vs, name, windows, range_ns).await
        }
        other => Err(to_err(format!("unsupported range function: {other}() (v1)"))),
    }
}

/// Apply `topk(k, …)` / `bottomk` to an already-lowered range frame as a window
/// plan ([`super::plan::frame::lower_topk`]), then drop the window scratch
/// columns. Partition by the frame's series identity: `prom_group_key` for a
/// grouped (aggregate) frame, else the raw label columns (`prom_part`).
fn lower_topk_df(
    df: datafusion::dataframe::DataFrame,
    k: i64,
    is_topk: bool,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let has = |n: &str| {
        df.schema()
            .has_column(&datafusion::common::Column::from_name(n))
    };
    // Series identity: the grouped frame's `prom_group_key`, else whichever raw
    // label columns the lowered frame actually carries (`rate`/`over_time` drop
    // `name`; a bare selector keeps it).
    let part: Vec<datafusion::logical_expr::Expr> = if has("prom_group_key") {
        vec![col("prom_group_key")]
    } else {
        // `attributes` is a Map (not partitionable) → key on prom_series_key(attributes).
        ["name", "service_name", "attributes"]
            .into_iter()
            .filter(|n| has(n))
            .map(|n| {
                if n == "attributes" {
                    prom_series_key_expr()
                } else {
                    col(n)
                }
            })
            .collect()
    };
    // The output columns we keep (everything the lowered frame carried, minus the
    // window scratch columns `peak`/`series_rank`).
    let keep: Vec<datafusion::logical_expr::Expr> = df
        .schema()
        .fields()
        .iter()
        .map(|f| col(f.name()))
        .collect();
    let ranked = super::plan::frame::lower_topk(df, part, "v", k, is_topk)?;
    Ok(ranked.select(keep)?)
}

/// Latest sample per series at/before `time_ns`, as a `DataFrame` (P5): the
/// instant-query base (`metric_base` filtered `<= time_ns`, then `rn = 1`). A
/// bare selector has no matrix window (`matrix_range_ns` → None ⇒ resolution 0)
/// and capability [`Capability::None`], so [`resolve_metric_windows`] yields a
/// single raw window — no hardcoded `"metrics"` literal (FR4, keeping the
/// no-bypass guard absolute). A selector spanning the sealed boundary would
/// resolve to tier+raw windows; we union the per-window bases, time-disjoint, so
/// the global latest-per-series picks the most recent across both.
async fn latest_selected_df(
    engine: &super::QueryEngine,
    vs: &VectorSelector,
    time_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let name = vs
        .name
        .as_deref()
        .ok_or_else(|| to_err("metric selector requires a name".to_string()))?;
    // Bare selectors evaluate the latest sample only: no lookback window
    // (resolution 0) and capability None ⇒ a single raw window naturally.
    // Capability::None short-circuits to a single raw window before the
    // wall-clock sealed boundary is used, so the `now_ns` arg is value-irrelevant
    // here; pass `time_ns` (in scope) rather than widen this fn's signature.
    let windows = resolve_metric_windows(engine, time_ns, time_ns, 0, Capability::None, time_ns);
    let mut base: Option<datafusion::dataframe::DataFrame> = None;
    for (table, _lo, hi) in windows {
        let part = selector_base_df(engine, vs, name, &table, hi).await?;
        base = Some(match base {
            Some(acc) => acc.union(part)?,
            None => part,
        });
    }
    let base = base.ok_or_else(|| to_err("resolver returned no windows".to_string()))?;
    // latest per (name, series-key) — matches the SQL row_number partition; keyed
    // on prom_series_key(attributes) since the Map isn't partitionable.
    super::plan::frame::latest_per_series(
        base,
        vec![col("name"), prom_series_key_expr()],
        "time_unix_nano",
    )
}

/// One filtered `<= time_ns` scan of `table` for the bare-selector instant base:
/// the matched series' identity columns + the coalesced value `v`. Factored out
/// of [`latest_selected_df`] so each resolver window contributes a union arm.
async fn selector_base_df(
    engine: &super::QueryEngine,
    vs: &VectorSelector,
    name: &str,
    table: &str,
    time_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::arrow::datatypes::DataType::Int64;
    use datafusion::logical_expr::expr_fn::cast;
    use datafusion::prelude::{col, lit};
    // FR1: the `latest ≤ time_ns` base has only an upper bound (the latest
    // sample may sit arbitrarily far back), so the scope is half-open — it
    // prunes just the files that provably start after the instant.
    let scope = super::QueryScope {
        lo_ns: i64::MIN,
        hi_ns: time_ns,
    };
    let mut df = engine
        .table_scoped(table, scope)
        .await?
        .filter(name_pred_expr(name))?;
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_expr(m) {
            df = df.filter(p)?;
        }
    }
    df = df.filter(cast(col("time_unix_nano"), Int64).lt_eq(lit(time_ns)))?;
    Ok(df.select(vec![
        prom_name_expr().alias("prom_name"),
        col("name"),
        col("service_name"),
        col("attributes"),
        col("time_unix_nano"),
        metric_value_expr(name).alias("v"),
    ])?)
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

/// Resolve the source windows for an **instant** range expression evaluated at
/// `time_ns` (FR4). The selector window `matrix_range_ns(expr)` is the resolution
/// input (analogous to `step` for the range path), and [`op_capability`] is the
/// safety gate: the coarsest tier ≤ the selector window that carries the
/// operator's capability serves the sealed portion of `[time_ns-window, time_ns]`,
/// raw serves the trailing live portion. A bare instant selector (no matrix
/// window) is the caller's responsibility (see [`latest_selected_df`]); this is
/// for range-function instants, so a missing matrix window is an error.
/// Whether the governing range function of `expr` is a LAG-based counter op
/// (`rate`/`increase`/`irate`) — the ops whose per-sample delta needs the sample
/// *before* the window as a LAG predecessor (see [`instant_range_windows`]).
/// Recurses through `Paren`/`Aggregate` like [`op_capability`] so `sum(rate(m[w]))`
/// is detected. `*_over_time` and bare selectors return `false`.
fn is_lag_range_op(expr: &Expr) -> bool {
    match expr {
        Expr::Paren(p) => is_lag_range_op(&p.expr),
        Expr::Aggregate(a) => is_lag_range_op(a.expr.as_ref()),
        Expr::Call(c) => matches!(c.func.name, "rate" | "increase" | "irate"),
        _ => false,
    }
}

fn instant_range_windows(
    engine: &super::QueryEngine,
    expr: &Expr,
    time_ns: i64,
    now_ns: i64,
) -> crate::Result<Vec<MetricWindow>> {
    let range = matrix_range_ns(expr).ok_or_else(|| {
        to_err("instant range function expects a range vector like m[5m] (v1)".to_string())
    })?;
    // Anchor an omitted `time` (i64::MAX) to now so `[T-range, T]` lands on real
    // data, not the year 2262 (empty). A finite explicit time passes through.
    let anchor = instant_anchor(time_ns, now_ns);
    // `rate`/`increase`/`irate` compute each sample's delta as `v - LAG(v)` and SUM
    // the deltas whose timestamp falls in the `range`-wide frame ending at `anchor`.
    // The window's *leading* sample (at `anchor-range`) needs its predecessor sample
    // — which sits just *before* the window — to contribute its delta. The range path
    // scans the whole query span, so that predecessor is present; an instant scan of
    // exactly `[anchor-range, anchor]` lacks it, dropping the leading delta and
    // under-reporting (live: ~½). Extend the scan lower bound back by an extra `range`
    // for the LAG ops so the predecessor is scanned; the `range`-wide SUM frame at
    // `anchor` is unchanged, so the extra older rows only seed LAG (not the sum), and
    // `latest_per_series` still emits only the value at `anchor`. `*_over_time` must
    // NOT extend (it would pull older rows into its windowed aggregate).
    let lag_margin = if is_lag_range_op(expr) { range } else { 0 };
    let start = anchor.saturating_sub(range).saturating_sub(lag_margin);
    Ok(resolve_metric_windows(engine, start, anchor, range, op_capability(expr), now_ns))
}

/// Lower an aggregate at an **instant** to the canonical aggregate frame
/// `[prom_group_key, v]` via `GROUP BY prom_group_key` + `agg(v)`
/// ([ADR: aggregation-pushdown]). The inner is one of:
/// - a **leaf selector** (`sum(m)`): the latest sample per series, then aggregate;
/// - a **range function** (`avg(rate(m[5m]))`): the range aggregate over the
///   `[T-range, T]` window, then the value at `T` (latest per group);
/// - a **nested aggregate** (`count(count(m) by (cpu))`): recurse, then re-project.
///
/// [ADR: aggregation-pushdown]: ../../docs/20260615_promql-pushdown/adrs/2026-06-15_aggregation-pushdown.md
async fn lower_aggregate_instant(
    engine: &super::QueryEngine,
    agg: &AggregateExpr,
    time_ns: i64,
    now_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let op = agg_name(agg.op).map_err(to_err)?;
    let grouping = AggGrouping::from(&agg.modifier);
    let v = agg_value_expr(op, col("v")).alias("v");

    // Nested aggregate inner: recurse to the canonical frame, then re-project.
    if let Some(inner_agg) = aggregate_inner(agg.expr.as_ref()) {
        let inner = Box::pin(lower_aggregate_instant(engine, inner_agg, time_ns, now_ns)).await?;
        let inner = rename_inner_group_key(inner)?;
        let key = agg_group_key_expr(&inner, &grouping).alias("prom_group_key");
        return Ok(inner.aggregate(vec![key], vec![v])?);
    }

    // Leaf selector inner (`sum(m)`): latest sample per series, then aggregate.
    // A bare selector's `latest ≤ time_ns` is correct at i64::MAX (newest), so it
    // keeps `time_ns` — only the range-window paths need the anchor.
    if let Some(vs) = aggregate_inner_selector(agg.expr.as_ref()) {
        let inner = latest_selected_df(engine, vs, time_ns).await?;
        let key = agg_group_key_expr(&inner, &grouping).alias("prom_group_key");
        return Ok(inner.aggregate(vec![key], vec![v])?);
    }

    // Range-function inner (`avg(rate(m[5m]))`, common on gauge panels): evaluate
    // `<agg>(range)` over the [T-range, T] window via the resolver-derived source
    // (FR4). `lower_instant_aggregate_range` collapses each inner series to its
    // single value at the anchor FIRST, then aggregates across series — so the
    // result is already the canonical `[prom_group_key, v]` (no per-series time, no
    // re-pick needed). The resolver windows are unioned at the leaf base so a
    // sealed/live straddle aggregates together rather than dropping the sealed window.
    let range = matrix_range_ns(agg.expr.as_ref()).ok_or_else(|| {
        to_err("instant aggregate inner must be a selector or a range function (v1)".to_string())
    })?;
    let windows = instant_range_windows(engine, agg.expr.as_ref(), time_ns, now_ns)?;
    lower_instant_aggregate_range(engine, agg, &windows, range).await
}

/// Lower an instant `<agg>(rate|*_over_time(m[w]))` to the canonical aggregate
/// frame `[prom_group_key, v]` over the resolver `windows` (FR4). Mirrors
/// [`lower_aggregate_range`] but builds the leaf over the unioned windows (via
/// [`lower_instant_leaf`]) so the aggregated instant value spans the sealed/live
/// boundary, AND **collapses each inner series to its single value at the anchor
/// before aggregating** — `sum(rate(m[w]))` at instant T = Σ over series of (that
/// series' windowed rate at T). The windowed leaf frame carries one row per sample
/// timestamp; series scraped at offset instants have their latest point at
/// different timestamps, so grouping by `(key, time)` and then picking the global
/// latest timestamp per group would silently drop every series whose last sample
/// isn't on the max timestamp (Sol↔Mimir parity bug). Reducing to latest-per-series
/// FIRST (keyed by series identity, no time in the cross-series grouping) makes all
/// series contribute. Nested aggregates recurse — each level returns a collapsed
/// `[prom_group_key, v]`, so the outer groups by the re-projected key with no time.
async fn lower_instant_aggregate_range(
    engine: &super::QueryEngine,
    agg: &AggregateExpr,
    windows: &[MetricWindow],
    range_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::prelude::col;
    let op = agg_name(agg.op).map_err(to_err)?;
    let grouping = AggGrouping::from(&agg.modifier);
    let inner = match agg.expr.as_ref() {
        // Nested aggregate inner: the recursion already collapsed it to one row per
        // `prom_group_key` (no time) — use it directly.
        Expr::Aggregate(inner_agg) => {
            Box::pin(lower_instant_aggregate_range(engine, inner_agg, windows, range_ns)).await?
        }
        Expr::Paren(p) if matches!(p.expr.as_ref(), Expr::Aggregate(_)) => {
            let Expr::Aggregate(inner_agg) = p.expr.as_ref() else {
                unreachable!("guarded by the match arm")
            };
            Box::pin(lower_instant_aggregate_range(engine, inner_agg, windows, range_ns)).await?
        }
        // Leaf range-function inner (`rate`/`*_over_time`): the windowed frame carries
        // one row per (series, sample-time). Collapse each series to its value at the
        // anchor (latest sample per series identity) so every series — even those
        // whose last sample lands on an earlier timestamp — contributes to the
        // cross-series aggregate.
        leaf => {
            let frame = lower_instant_leaf(engine, leaf, windows, range_ns).await?;
            super::plan::frame::latest_per_series(
                frame,
                vec![col("service_name"), prom_series_key_expr()],
                "time_unix_nano",
            )?
        }
    };
    let inner = rename_inner_group_key(inner)?;
    let key = agg_group_key_expr(&inner, &grouping).alias("prom_group_key");
    let v = agg_value_expr(op, col("v")).alias("v");
    // No `time_unix_nano` in the grouping: the inner is already one row per series
    // at the anchor, so the cross-series aggregate is a single value per group.
    Ok(inner.aggregate(vec![key], vec![v])?)
}

/// The inner aggregate of `expr`, unwrapping parens (`Some` only if the inner is
/// itself an aggregate — the nested-aggregate case).
fn aggregate_inner(expr: &Expr) -> Option<&AggregateExpr> {
    match expr {
        Expr::Aggregate(a) => Some(a),
        Expr::Paren(p) => aggregate_inner(&p.expr),
        _ => None,
    }
}

/// The inner vector selector of `expr`, unwrapping parens (the leaf-selector case).
fn aggregate_inner_selector(expr: &Expr) -> Option<&VectorSelector> {
    match expr {
        Expr::VectorSelector(vs) => Some(vs),
        Expr::Paren(p) => aggregate_inner_selector(&p.expr),
        _ => None,
    }
}

/// Lower an instant PromQL expression to a `DataFrame`: latest-per-series
/// selectors and `<agg> by` aggregations.
async fn lower_instant_df(
    engine: &super::QueryEngine,
    expr: &Expr,
    time_ns: i64,
    now_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    match expr {
        // A bare selector's `latest ≤ time_ns` is correct at i64::MAX, so it keeps
        // `time_ns`; only the range-window paths below need the anchor.
        Expr::VectorSelector(vs) => latest_selected_df(engine, vs, time_ns).await,
        Expr::Paren(p) => Box::pin(lower_instant_df(engine, &p.expr, time_ns, now_ns)).await,
        Expr::Aggregate(agg) => lower_aggregate_instant(engine, agg, time_ns, now_ns).await,
        // Bare range function at an instant (`rate(metric[5m])`): evaluate over the
        // [T-range, T] window using the resolver-derived source (FR4) — the windows
        // are unioned at the leaf base so the value at T aggregates across the
        // sealed/live boundary — then keep the value at T.
        Expr::Call(_) => {
            let range = matrix_range_ns(expr).ok_or_else(|| {
                to_err("instant range function expects a range vector like m[5m] (v1)".to_string())
            })?;
            let windows = instant_range_windows(engine, expr, time_ns, now_ns)?;
            let series = lower_instant_leaf(engine, expr, &windows, range).await?;
            // rate/over_time project to (service_name, attributes, time, v) — a
            // series is identified by those; partition the latest-pick on the
            // series-key UDF since the attributes Map isn't partitionable.
            let part = vec![
                datafusion::prelude::col("service_name"),
                prom_series_key_expr(),
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

use super::group_key::AggGrouping;

/// Apply `clamp_min`/`clamp_max` (value floored/capped at `bound`) to a value.
fn clamp_value(is_min: bool, x: f64, bound: f64) -> f64 {
    if is_min { x.max(bound) } else { x.min(bound) }
}

/// Fold an instant value to a scalar, per PromQL `scalar()`: a scalar passes
/// through, a one-element vector yields its value, anything else is NaN.
fn instant_to_scalar(v: InstantVal) -> f64 {
    match v {
        InstantVal::Scalar(x) => x,
        InstantVal::Vector(items) if items.len() == 1 => items[0].1,
        InstantVal::Vector(_) => f64::NAN,
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
/// `__name__`, and the columnar `attributes` MAP is exploded into normalized
/// per-attribute labels — read parse-free, no `serde_json` (promql-pushdown T7).
/// Built once per batch; `labels(i)` yields one row's set. (Grouped queries
/// project their `by(…)` labels as columns and carry no `attributes`/`prom_name`,
/// so they're handled by the same path unchanged.)
struct LabelCols {
    promoted: Vec<(String, datafusion::arrow::array::ArrayRef)>,
    attrs: Option<datafusion::arrow::array::MapArray>,
}

impl LabelCols {
    fn build(batch: &datafusion::arrow::record_batch::RecordBatch) -> crate::Result<Self> {
        use datafusion::arrow::array::{Array, MapArray};
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
                // The attributes column is a columnar MAP — keep it as a MapArray
                // and read entries directly (no cast-to-Utf8, no JSON parse).
                attrs = batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .map(|m| MapArray::from(m.to_data()));
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
        if let Some(map) = &self.attrs {
            // Columnar MAP read: normalize keys, promoted columns win on collision.
            for (k, v) in super::udf::map_row_normalized_labels(map, i) {
                m.entry(k).or_insert(v);
            }
        }
        m
    }
}

/// Per-batch label accessor that abstracts over the two result shapes: a grouped
/// aggregate result (a `prom_group_key` column, parsed once per row = once per
/// output group via [`super::group_key::GroupKey::parse`]) and a raw selector
/// result (label/`attributes` columns via [`LabelCols`]).
enum SeriesLabels {
    /// Aggregated frame: the cast `prom_group_key` string column.
    Grouped(datafusion::arrow::array::ArrayRef),
    /// Raw selector frame: promoted + exploded `attributes` label columns.
    /// Boxed — `LabelCols` carries a `MapArray`, far larger than the `Grouped`
    /// variant's single `ArrayRef`.
    Raw(Box<LabelCols>),
}

impl SeriesLabels {
    fn build(batch: &datafusion::arrow::record_batch::RecordBatch) -> crate::Result<Self> {
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::DataType;
        if let Ok(idx) = batch.schema().index_of("prom_group_key") {
            let key = cast(batch.column(idx), &DataType::Utf8).map_err(|e| to_err(e.to_string()))?;
            Ok(SeriesLabels::Grouped(key))
        } else {
            Ok(SeriesLabels::Raw(Box::new(LabelCols::build(batch)?)))
        }
    }

    fn labels(&self, i: usize) -> BTreeMap<String, String> {
        use datafusion::arrow::array::{Array, AsArray};
        match self {
            SeriesLabels::Grouped(arr) => {
                let a = arr.as_string::<i32>();
                if a.is_null(i) {
                    BTreeMap::new()
                } else {
                    super::group_key::GroupKey::parse(a.value(i))
                }
            }
            SeriesLabels::Raw(cols) => cols.labels(i),
        }
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
    now_ns: i64,
) -> crate::Result<PromResponse> {
    // histogram_quantile, binary/unary operators and aggregates are all handled
    // by the recursive evaluator; leaves fall through to SQL.
    let expr = parser::parse(query).map_err(to_err)?;
    // An omitted `time` arrives as i64::MAX ("latest"); anchor it to wall-clock now
    // for the response sample timestamp (Mimir stamps "now", not year 2262) and the
    // `[T-range, T]` scan windows (see `instant_anchor`).
    let anchor = instant_anchor(time_ns, now_ns);
    // ns→seconds for the Prometheus sample timestamp; sub-ms precision is irrelevant here.
    #[allow(clippy::cast_precision_loss)]
    let time_s = anchor as f64 / 1_000_000_000.0;

    let samples: Vec<(BTreeMap<String, String>, f64, f64)> =
        match eval_instant(engine, &expr, time_ns, now_ns).await? {
            InstantVal::Scalar(s) => vec![(BTreeMap::new(), time_s, s)],
            InstantVal::Vector(v) => v.into_iter().map(|(m, x)| (m, time_s, x)).collect(),
        };
    Ok(PromResponse::vector(samples))
}

/// Anchor an instant query's evaluation time: an omitted `time` param reaches the
/// query layer as `i64::MAX` ("latest"), which is correct for a bare-selector
/// `latest sample ≤ T` but breaks a range-function `[T-range, T]` window (it would
/// land in the year 2262, past all data → empty). Resolve the sentinel to a
/// real wall-clock `now_ns` (captured at the request boundary, so the core query
/// fns stay clock-free and testable); a finite explicit time passes through.
fn instant_anchor(time_ns: i64, now_ns: i64) -> i64 {
    if time_ns == i64::MAX { now_ns } else { time_ns }
}

/// The cache-classification window of an instant query (FR2): the anchored
/// evaluation point `[anchor, anchor]`. Sealedness only reads the window's
/// upper bound, so the point window classifies exactly — an explicit `time`
/// older than a day → sealed (long TTL), a live/omitted `time` → mutable.
fn instant_scope(time_ns: i64, now_ns: i64) -> super::QueryScope {
    let anchor = instant_anchor(time_ns, now_ns);
    super::QueryScope {
        lo_ns: anchor,
        hi_ns: anchor,
    }
}

/// Collect the first (string) column of a built `DataFrame`. Shared by the
/// label/tag-value discovery endpoints (Prometheus, Loki). `scope` is the
/// query's window when the caller has one — cache TTL classification (FR2).
pub(super) async fn string_column_df(
    engine: &super::QueryEngine,
    df: datafusion::dataframe::DataFrame,
    scope: Option<super::QueryScope>,
) -> crate::Result<Vec<String>> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let batches = engine.collect_scoped(df, scope).await?;
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
    let df = engine.table(table).await?;
    // Unbounded discovery scan — no window to classify (short cache TTL).
    distinct_json_keys_df(engine, df, column, None).await
}

/// [`distinct_json_keys`] over an already-built source `DataFrame` — the
/// metrics `/labels` path passes each ranged [`metadata_sources`] window scan
/// here (FR4) instead of the full registered table, along with its window
/// (`scope`) for cache TTL classification (FR2).
async fn distinct_json_keys_df(
    engine: &super::QueryEngine,
    df: datafusion::dataframe::DataFrame,
    column: &str,
    scope: Option<super::QueryScope>,
) -> crate::Result<std::collections::BTreeSet<String>> {
    // Cap the distinct blobs scanned: label/tag discovery is bounded by
    // label-set cardinality, but a high-cardinality attribute (e.g. a per-request
    // id embedded in the JSON) would otherwise make this an unbounded scan +
    // parse. 10k distinct blobs is far more label sets than any real schema.
    use datafusion::arrow::array::{Array, AsArray, MapArray};
    const MAX_DISTINCT_BLOBS: usize = 10_000;
    // The `metrics` table's `attributes` is a columnar MAP (read its keys directly,
    // no JSON parse); `logs`/`traces` keep a JSON-string `attributes` column. A Map
    // column can't be `.distinct()`-ed, so we cap rows via `limit` (the key set
    // dedups). Discovery is bounded by label-set cardinality, so the cap sits far
    // above any real schema's distinct label sets.
    let df = df
        .filter(datafusion::prelude::col(column).is_not_null())?
        .select(vec![datafusion::prelude::col(column)])?
        .limit(0, Some(MAX_DISTINCT_BLOBS))?;
    let batches = engine.collect_scoped(df, scope).await?;
    let mut keys = std::collections::BTreeSet::new();
    for batch in &batches {
        let c = batch.column(0);
        if let Some(map) = c.as_any().downcast_ref::<MapArray>() {
            for i in 0..map.len() {
                if let Some(entries) = super::udf::map_row_entries(map, i) {
                    keys.extend(entries.into_iter().map(|(k, _)| k));
                }
            }
        } else {
            // JSON-string attributes (logs/traces): parse each object's keys.
            let s = c.as_string::<i32>();
            for i in 0..s.len() {
                if !s.is_null(i)
                    && let Ok(serde_json::Value::Object(map)) =
                        serde_json::from_str::<serde_json::Value>(s.value(i))
                {
                    keys.extend(map.keys().cloned());
                }
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
    matcher: Option<&str>,
    now_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::functions::expr_fn::concat;
    use datafusion::prelude::{col, lit};
    // FR5: route each label-value enumeration through the tier-resolution choke
    // point. Metadata computes no values, so capability `Last` is always
    // tier-eligible; passing `resolution_ns = i64::MAX` selects the coarsest
    // available tier (fewest rows → cheapest `DISTINCT`) for the sealed span, raw
    // for the trailing ≤1-day window. The windows are time-disjoint and the tier
    // preserves the full label/series set, so the UNION-distinct equals the
    // raw-only enumeration. With no registered tier / no sealed portion the
    // resolver returns a single raw `metrics` window — unchanged behaviour.
    let windows = resolve_metric_windows(engine, start_ns, end_ns, i64::MAX, Capability::Last, now_ns);
    // One scan of a resolver window `(table, lo, hi)`: scope to the window's time
    // range and the `match[]` selector — a `$host` variable query like
    // `label_values(up{service_name="X"}, host)` must restrict `host` to that
    // service, not list every host in the store — plus any caller `extra` pred.
    let scan = |table: String, lo: i64, hi: i64, extra: datafusion::prelude::Expr| async move {
        // FR1: each window scan prunes to its own `[lo, hi]` file interval.
        let scope = super::QueryScope {
            lo_ns: lo,
            hi_ns: hi,
        };
        let df = engine
            .table_scoped(&table, scope)
            .await?
            .filter(prom_time_between(lo, hi).and(extra))?;
        apply_match_selector(df, matcher)
    };
    // UNION the per-window value projections into one distinct, sorted result.
    let union_distinct =
        |acc: Option<datafusion::dataframe::DataFrame>, df: datafusion::dataframe::DataFrame| match acc {
            Some(a) => a.union(df),
            None => Ok(df),
        };
    if label == "__name__" {
        let variant = |suffix: &str| concat(vec![prom_name_expr(), lit(suffix.to_string())]);
        let mut acc: Option<datafusion::dataframe::DataFrame> = None;
        for (table, lo, hi) in &windows {
            let names = scan(table.clone(), *lo, *hi, lit(true))
                .await?
                .select(vec![prom_name_expr().alias("v")])?;
            let with_buckets = || col("bucket_counts").is_not_null();
            let bkt = scan(table.clone(), *lo, *hi, with_buckets())
                .await?
                .select(vec![variant("_bucket").alias("v")])?;
            let cnt = scan(table.clone(), *lo, *hi, with_buckets())
                .await?
                .select(vec![variant("_count").alias("v")])?;
            let sm = scan(table.clone(), *lo, *hi, with_buckets())
                .await?
                .select(vec![variant("_sum").alias("v")])?;
            for df in [names, bkt, cnt, sm] {
                acc = Some(union_distinct(acc, df)?);
            }
        }
        let merged = acc.ok_or_else(|| to_err("build_label_values: no source windows".to_string()))?;
        return Ok(merged
            .filter(col("v").is_not_null())?
            .distinct()?
            .sort(vec![col("v").sort(true, false)])?);
    }
    let lhs = label_lhs_expr(label);
    let mut acc: Option<datafusion::dataframe::DataFrame> = None;
    for (table, lo, hi) in &windows {
        let df = scan(table.clone(), *lo, *hi, lhs.clone().is_not_null())
            .await?
            .select(vec![lhs.clone().alias("v")])?;
        acc = Some(union_distinct(acc, df)?);
    }
    let merged = acc.ok_or_else(|| to_err("build_label_values: no source windows".to_string()))?;
    Ok(merged.distinct()?.sort(vec![col("v").sort(true, false)])?)
}

/// Run `label/:name/values` and build `{status, data:[...]}`.
pub async fn handle_label_values(
    engine: &super::QueryEngine,
    label: &str,
    start_ns: i64,
    end_ns: i64,
    matcher: Option<&str>,
    now_ns: i64,
) -> crate::Result<serde_json::Value> {
    let df = build_label_values(engine, label, start_ns, end_ns, matcher, now_ns).await?;
    let scope = super::QueryScope {
        lo_ns: start_ns,
        hi_ns: end_ns,
    };
    let values = string_column_df(engine, df, Some(scope)).await?;
    Ok(serde_json::json!({ "status": "success", "data": values }))
}

/// Run `labels` (label-name discovery for Grafana's metric browser): the
/// promoted columns plus the Prometheus-normalized metric attribute keys.
///
/// With a `[start, end]` range (the route always supplies one now — FR4
/// defaults an absent `start` to a bounded recent window) the key discovery
/// runs over the ranged [`metadata_sources`] windows: sealed span → rollup
/// tier, trailing live window → raw, each scan pruned to its file interval
/// (FR1). `None` keeps the historical unbounded raw scan.
pub async fn handle_labels(
    engine: &super::QueryEngine,
    time_range: Option<(i64, i64)>,
    now_ns: i64,
) -> crate::Result<serde_json::Value> {
    let mut keys = std::collections::BTreeSet::new();
    let scope = time_range.map(|(lo, hi)| super::QueryScope {
        lo_ns: lo,
        hi_ns: hi,
    });
    for df in metadata_sources(engine, time_range, now_ns).await? {
        keys.extend(distinct_json_keys_df(engine, df, "attributes", scope).await?);
    }
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
    time_range: Option<(i64, i64)>,
    now_ns: i64,
) -> crate::Result<serde_json::Value> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let df = build_series(engine, matcher, time_range, now_ns).await?;
    let scope = time_range.map(|(lo, hi)| super::QueryScope {
        lo_ns: lo,
        hi_ns: hi,
    });
    let batches = engine.collect_scoped(df, scope).await?;
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

/// Group an already-built range `DataFrame`'s rows into per-series point
/// lists. `scope` is the query's scan window — cache TTL classification (FR2).
/// `step_ns` is the query step, a plan-cache key component (promql-plan-cache
/// task 2a) — the lowered plan itself is step-free (gridding happens in Rust),
/// so the step rides along solely to keep the key complete.
async fn range_series_from_df(
    engine: &super::QueryEngine,
    df: datafusion::dataframe::DataFrame,
    scope: super::QueryScope,
    step_ns: i64,
) -> crate::Result<RangeSeries> {
    let batches = engine
        .collect_scoped_stepped(df, Some(scope), step_ns)
        .await?;
    group_range_series(&batches)
}

/// Group result batches (`v` + `time_unix_nano` + label columns) into per-series
/// point lists keyed by the (ordered) label set.
///
/// A grouped result carries a `prom_group_key` column (the canonical aggregate
/// frame): its labels are recovered via [`SeriesLabels::parse`] once per group.
/// A raw selector carries label columns instead, handled by [`LabelCols`].
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
        let labels = SeriesLabels::build(batch)?;

        for i in 0..batch.num_rows() {
            if v.is_null(i) || t.is_null(i) {
                continue;
            }
            let metric = labels.labels(i);
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
    step_ns: i64,
    now_ns: i64,
) -> crate::Result<PromMatrixResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Int64Type};

    use datafusion::prelude::col;
    // Sealed windows from the rollup tier (coarse step), trailing day from raw.
    // Profiling seam (promql-plan-cache FR1): the tier-routed scan + projection
    // construction is this path's logical lowering (`lower` stage).
    let t = std::time::Instant::now();
    let df = hist_source(engine, &spec.base, &spec.preds, start_ns, end_ns, step_ns, now_ns).await?;
    let mut proj = vec![
        col("time_unix_nano"),
        col("bucket_counts"),
        col("explicit_bounds"),
    ];
    for g in &spec.group_by {
        proj.push(label_lhs_expr(g).alias(sql_ident(g)));
    }
    let df = df.select(proj)?;
    super::telemetry::record_plan_stage("lower", t.elapsed());
    let scope = super::QueryScope {
        lo_ns: start_ns,
        hi_ns: end_ns,
    };
    let batches = engine.collect_scoped(df, Some(scope)).await?;

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
    step_ns: i64,
    now_ns: i64,
) -> crate::Result<PromMatrixResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Int64Type};

    use datafusion::prelude::col;
    // Sealed windows from the rollup tier (coarse step), trailing day from raw.
    let df = hist_source(engine, &spec.base, &spec.preds, start_ns, end_ns, step_ns, now_ns)
        .await?
        .filter(col("bucket_counts").is_not_null())?;
    let mut proj = vec![
        col("time_unix_nano"),
        col("bucket_counts"),
        col("explicit_bounds"),
    ];
    for g in &spec.group_by {
        proj.push(label_lhs_expr(g).alias(sql_ident(g)));
    }
    let scope = super::QueryScope {
        lo_ns: start_ns,
        hi_ns: end_ns,
    };
    let batches = engine.collect_scoped(df.select(proj)?, Some(scope)).await?;

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
/// The per-bucket information a range operator needs from a rollup tier (FR2,
/// per the [operator → capability ADR](../../../docs/workspace/rollup-read-routing/adrs/operator-safety-allowlist.md)).
/// `None` means "no tier can answer this exactly" — the query must read raw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// Last cumulative value per bucket: `rate`/`increase`/`histogram_quantile`.
    Last,
    /// Per-bucket extrema: `max_over_time`/`min_over_time`.
    MinMax,
    /// Per-bucket sum/count: `avg_/sum_/count_over_time`.
    SumCount,
    /// Force raw: `irate`, `quantile/stddev/stdvar_over_time`, bare selectors,
    /// and any unclassified operator (fail-safe default).
    None,
}

/// Whether `expr` references any metric series (a `VectorSelector` or
/// `MatrixSelector`) anywhere in its tree. Used to tell a capability-neutral
/// **scalar** binary operand (`* 2`, `/ 1024`, `> 5`) — which carries no
/// selector — from a metric operand, so scalar-scaled metric expressions stay
/// tier-eligible (they inherit the metric operand's capability).
fn expr_has_selector(expr: &Expr) -> bool {
    match expr {
        Expr::VectorSelector(_) | Expr::MatrixSelector(_) => true,
        Expr::Paren(p) => expr_has_selector(&p.expr),
        Expr::Unary(u) => expr_has_selector(&u.expr),
        Expr::Aggregate(a) => expr_has_selector(a.expr.as_ref()),
        Expr::Subquery(s) => expr_has_selector(&s.expr),
        Expr::Binary(b) => expr_has_selector(&b.lhs) || expr_has_selector(&b.rhs),
        Expr::Call(c) => c.args.args.iter().any(|a| expr_has_selector(a)),
        _ => false,
    }
}

/// Statically classify the governing range operator of `expr` into the rollup
/// [`Capability`] it needs. Recurses through `Paren`, `Aggregate` (e.g.
/// `sum by(le)(…)`), and `topk`/`bottomk` wrappers the same way
/// [`detect_hist_quantile`] does, so `histogram_quantile(0.9, sum by(le)(rate(..)))`
/// and `topk(k, histogram_quantile(..))` both classify as [`Capability::Last`].
/// Unknown/unimplemented functions and bare selectors fall back to
/// [`Capability::None`] (raw) by design.
pub fn op_capability(expr: &Expr) -> Capability {
    // `histogram_quantile` may sit under topk/aggregate/rate — reuse the same
    // recursion the histogram path uses to find it.
    if detect_hist_quantile(expr).is_some() {
        return Capability::Last;
    }
    match expr {
        Expr::Paren(p) => op_capability(&p.expr),
        Expr::Aggregate(agg) => op_capability(agg.expr.as_ref()),
        // A unary op (`-rate(m[5m])`) carries its operand's capability — the value
        // column selection is unchanged, only its sign flips downstream.
        Expr::Unary(u) => op_capability(&u.expr),
        // A binary op. A **scalar** operand (no metric selector — e.g. `* 2`,
        // `/ 1024`, `> 5`) is capability-*neutral*: it inherits the metric
        // operand's capability so unit-scaling/threshold panels still tier. Two
        // metric operands must agree on a value column (`combine_capability`).
        Expr::Binary(b) => {
            let (lc, rc) = (op_capability(&b.lhs), op_capability(&b.rhs));
            match (expr_has_selector(&b.lhs), expr_has_selector(&b.rhs)) {
                // No metric anywhere — never reaches the range tier path; force raw.
                (false, false) => Capability::None,
                (false, true) => rc, // lhs scalar → inherit rhs
                (true, false) => lc, // rhs scalar → inherit lhs
                (true, true) => combine_capability(lc, rc),
            }
        }
        Expr::Call(c) => match c.func.name {
            "rate" | "increase" => Capability::Last,
            "max_over_time" | "min_over_time" => Capability::MinMax,
            "avg_over_time" | "sum_over_time" | "count_over_time" => Capability::SumCount,
            _ => Capability::None,
        },
        _ => Capability::None,
    }
}

/// Combine the capabilities of a binary op's two operands. Tier routing needs a
/// single value column for the whole window, so two operands may share a tier
/// only when they agree: equal capabilities pass through; any `None`, or two
/// different (column-incompatible) capabilities (e.g. `MinMax` vs `Last`), force
/// raw. Conservative by design — when unsure, `None`.
fn combine_capability(lhs: Capability, rhs: Capability) -> Capability {
    match (lhs, rhs) {
        (Capability::None, _) | (_, Capability::None) => Capability::None,
        (a, b) if a == b => a,
        _ => Capability::None,
    }
}

/// A time-disjoint source window for a metric query: `(table, lo_ns, hi_ns)`,
/// both bounds inclusive. The resolver returns these ordered and covering the
/// requested span.
pub type MetricWindow = (String, i64, i64);

/// One day in nanoseconds — the sealed/live boundary offset (rollups only cover
/// fully-sealed days; the trailing ≤1-day window is always raw). Canonical day
/// value from [`super::units::DurationNs::DAY`] (canonical-ns ADR — no duplicated
/// ns literals). `pub(super)` so [`super::inventory::QueryScope::is_sealed`]
/// (FR2 cache classification) shares the exact same wall-clock rule.
pub(super) const SEALED_OFFSET_NS: i64 = super::units::DurationNs::DAY.ns();

/// The single tier-resolution choke point (FR1, per the
/// [tier-resolution-choke-point ADR](../../../docs/workspace/rollup-read-routing/adrs/tier-resolution-choke-point.md)):
/// resolve the ordered, time-disjoint `(table, lo, hi)` source windows covering
/// `[start_ns, end_ns]`. `capability == None` ⇒ a single raw window. Otherwise
/// the sealed part `[start_ns, sealed_ns]` reads the coarsest registered tier
/// whose resolution ≤ `resolution_ns` (via [`super::rollup::select_tier`]), and
/// the trailing live part `(sealed_ns, end_ns]` — which no tier covers — reads
/// raw `metrics`. Every rollup file now carries all capabilities (clean
/// cutover), so capability only gates None-vs-not, not which tier is eligible.
///
/// The sealed/live boundary is **wall-clock-relative**: `sealed_ns = now_ns -
/// SEALED_OFFSET_NS`, where `now_ns` is real wall-clock now captured at the
/// request boundary (the core fns stay clock-free + testable). It is *not*
/// relative to `end_ns` — a historical dashboard view (`end_ns` in the past)
/// over a long-sealed day must still route that day to the tier, not read it
/// raw just because it sits in the last day before the query's own `end`.
pub fn resolve_metric_windows(
    engine: &super::QueryEngine,
    start_ns: i64,
    end_ns: i64,
    resolution_ns: i64,
    capability: Capability,
    now_ns: i64,
) -> Vec<MetricWindow> {
    let raw_all = || vec![("metrics".to_string(), start_ns, end_ns)];
    if capability == Capability::None {
        return raw_all();
    }
    let available: Vec<super::rollup::RollupTier> = super::rollup::RollupTier::all()
        .into_iter()
        .filter(|t| engine.has_table(&format!("metrics_{}", t.label())))
        .collect();
    let tier = match super::rollup::select_tier(resolution_ns, &available) {
        super::rollup::RollupTier::Raw => return raw_all(),
        tier => tier,
    };
    let sealed_ns = now_ns - SEALED_OFFSET_NS;
    // No sealed part falls in range — the whole span is live → raw.
    if start_ns > sealed_ns {
        return raw_all();
    }
    let tier_table = format!("metrics_{}", tier.label());
    let mut windows = vec![(tier_table, start_ns, end_ns.min(sealed_ns))];
    if end_ns > sealed_ns {
        windows.push(("metrics".to_string(), sealed_ns + 1, end_ns));
    }
    windows
}

/// One filtered scan of `table` for a classic-histogram range query:
/// `prom_name == base`, the `preds`, and `time ∈ [lo, hi]`.
async fn hist_scan(
    engine: &super::QueryEngine,
    table: &str,
    base: &str,
    preds: &[datafusion::logical_expr::Expr],
    lo: i64,
    hi: i64,
) -> crate::Result<datafusion::prelude::DataFrame> {
    // FR1: prune the scan to the resolver window's `[lo, hi]` file interval.
    let scope = super::QueryScope {
        lo_ns: lo,
        hi_ns: hi,
    };
    let mut df = engine
        .table_scoped(table, scope)
        .await?
        .filter(prom_name_expr().eq(datafusion::prelude::lit(base.to_string())))?;
    for p in preds {
        df = df.filter(p.clone())?;
    }
    Ok(df.filter(prom_time_between(lo, hi))?)
}

/// Tier-routed row source for a classic-histogram range query, built from the
/// single tier-resolution choke point ([`resolve_metric_windows`], FR1/FR3). A
/// histogram-quantile / heatmap range query has capability [`Capability::Last`]
/// (it reads the cumulative `bucket_counts`/`explicit_bounds` the rollup
/// preserves per bucket), so its source windows are the coarsest tier ≤
/// `step_ns` for the sealed span and raw `metrics` for the trailing ≤1-day
/// window. Each window is scanned via [`hist_scan`] and the per-window
/// DataFrames are UNIONed; the windows are time-disjoint, so a later
/// per-timestamp quantile/heatmap aggregation never double-counts.
async fn hist_source(
    engine: &super::QueryEngine,
    base: &str,
    preds: &[datafusion::logical_expr::Expr],
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    now_ns: i64,
) -> crate::Result<datafusion::prelude::DataFrame> {
    let windows = resolve_metric_windows(engine, start_ns, end_ns, step_ns, Capability::Last, now_ns);
    let mut df: Option<datafusion::prelude::DataFrame> = None;
    for (table, lo, hi) in windows {
        let scan = hist_scan(engine, &table, base, preds, lo, hi).await?;
        df = Some(match df {
            Some(acc) => acc.union(scan)?,
            None => scan,
        });
    }
    // `resolve_metric_windows` always returns ≥1 window, so `df` is always set.
    df.ok_or_else(|| to_err("resolve_metric_windows returned no windows".to_string()))
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
    now_ns: i64,
) -> crate::Result<PromMatrixResponse> {
    // Profiling seam (promql-plan-cache FR1): time the PromQL parse stage.
    let t = std::time::Instant::now();
    let parsed = parser::parse(query).map_err(to_err)?;
    super::telemetry::record_plan_stage("parse", t.elapsed());
    // Classic-histogram queries are computed from OTLP array buckets:
    // histogram_quantile(…) and bare `_bucket`-by-`le` heatmaps.
    if let Some(spec) = detect_hist_quantile(&parsed) {
        return handle_hist_quantile_range(engine, &spec, start_ns, end_ns, step_ns, now_ns).await;
    }
    if let Some(spec) = detect_bucket_heatmap(&parsed) {
        return handle_bucket_heatmap(engine, &spec, start_ns, end_ns, step_ns, now_ns).await;
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

    // Route through the single tier-resolution choke point (FR3): the operator's
    // capability (recursing through binary/unary/aggregate/topk) decides whether a
    // tier may serve the sealed portion at all, and the resolver returns the
    // ordered, time-disjoint `(table, lo, hi)` windows — coarsest eligible tier for
    // the sealed span, raw `metrics` for the trailing ≤1-day (unsealed) window.
    let cap = op_capability(eval_expr);
    let resolved = resolve_metric_windows(engine, start_ns, end_ns, step_ns, cap, now_ns);
    // FR2: a windowed op (`rate`/`increase`/`*_over_time`) at the first grid point
    // of the query — and of each per-day shard — needs the samples in its
    // `(t−range, t]` window (plus a LAG predecessor). We therefore scan each shard
    // from `query_start = shard.start − lookback` (`lookback` = the matrix window)
    // but emit points only for `[shard.start, shard.end]`, so the lookback region
    // seeds the window/LAG without double-emitting points that belong to the
    // previous shard. A bare selector (no matrix window) has `lookback = 0`.
    let lookback_ns = matrix_range_ns(eval_expr).unwrap_or(0);
    // Within each resolver window, keep the frontend's per-day shard split (for
    // the historical-shard cache); every shard inherits its window's table. Each
    // entry is `(table, query_start, emit_start, emit_end)`.
    let windows: Vec<(String, i64, i64, i64)> = resolved
        .into_iter()
        .flat_map(|(table, lo, hi)| {
            if super::frontend::should_split(lo, hi) {
                // Per-day shards aligned to UTC midnight; `split` emits the
                // shard-count metric. The whole window is one tier, so treat it as
                // fully sealed for the split (the tier/raw boundary already lives in
                // the resolver windows). `lookback_ns` gives each shard its
                // `query_start_ns = start − lookback` for the left-edge window.
                super::frontend::split(lo, hi, lookback_ns, hi)
                    .into_iter()
                    .map(|s| (table.clone(), s.query_start_ns, s.start_ns, s.end_ns))
                    .collect::<Vec<_>>()
            } else {
                // Unsplit (sub-day) window: still scan from `lo − lookback` so the
                // query's first grid points have a full window, emit only `[lo, hi]`.
                vec![(table, lo - lookback_ns, lo, hi)]
            }
        })
        .collect();

    let mut merged: RangeSeries = BTreeMap::new();
    for (table, scan_start, s, e) in windows {
        let win = RangeWindow { scan_start, start: s, end: e };
        match eval_range_window(engine, eval_expr, win, &table, step_ns, now_ns).await? {
            RangeVal::Vector(mut part) => {
                // Emit only points inside this shard's `[s, e]` window; the lookback
                // region `[scan_start, s)` was scanned solely to seed the window/LAG.
                #[allow(clippy::cast_precision_loss)]
                let (lo_s, hi_s) = (s as f64 / 1e9, e as f64 / 1e9);
                for (_key, (_metric, points)) in part.iter_mut() {
                    points.retain(|(t, _)| *t >= lo_s - 1e-9 && *t <= hi_s + 1e-9);
                }
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

/// One filtered `<= time_ns` scan of `table` for the instant-histogram base: the
/// matched series' identity + the OTLP `bucket_counts`/`explicit_bounds` arrays.
/// Factored out of [`handle_histogram`] so each resolver window contributes a
/// union arm (the bucket columns are shared across the raw/tier schemas).
async fn hist_instant_scan(
    engine: &super::QueryEngine,
    vs: &VectorSelector,
    name: &str,
    table: &str,
    time_ns: i64,
) -> crate::Result<datafusion::dataframe::DataFrame> {
    use datafusion::arrow::datatypes::DataType::Int64;
    use datafusion::logical_expr::expr_fn::cast as df_cast;
    use datafusion::prelude::{col, lit};
    // FR1: like `selector_base_df`, the `latest ≤ time_ns` base is half-open —
    // only files provably starting after the instant are pruned.
    let scope = super::QueryScope {
        lo_ns: i64::MIN,
        hi_ns: time_ns,
    };
    let mut df = engine
        .table_scoped(table, scope)
        .await?
        .filter(prom_name_expr().eq(lit(name.to_string())))?;
    for m in &vs.matchers.matchers {
        if let Some(p) = matcher_expr(m) {
            df = df.filter(p)?;
        }
    }
    df = df.filter(df_cast(col("time_unix_nano"), Int64).lt_eq(lit(time_ns)))?;
    Ok(df.select(vec![
        col("name"),
        col("service_name"),
        col("attributes"),
        col("bucket_counts"),
        col("explicit_bounds"),
        col("time_unix_nano"),
    ])?)
}

/// Run `histogram_quantile(φ, m{…})` and build a `resultType=vector` response.
async fn handle_histogram(
    engine: &super::QueryEngine,
    phi: f64,
    vs: &VectorSelector,
    time_ns: i64,
    now_ns: i64,
) -> crate::Result<PromResponse> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;

    let name = vs
        .name
        .as_deref()
        .ok_or_else(|| to_err("histogram selector requires a name".into()))?;
    use datafusion::prelude::col;
    // An instant histogram has no lookback window (resolution 0); `histogram_quantile`
    // is Capability::Last, but resolution 0 ⇒ the resolver yields a single raw
    // window (FR4) — no hardcoded `"metrics"` literal. The bucket columns are
    // shared across raw/tier schemas, so a boundary-straddling window (were the
    // resolution ever > 0) unions cleanly. Anchor an omitted `time` (i64::MAX) to
    // now so the latest-sample filter and the response timestamp are sensible.
    let anchor = instant_anchor(time_ns, now_ns);
    let windows = resolve_metric_windows(engine, anchor, anchor, 0, Capability::Last, now_ns);
    let mut base: Option<datafusion::dataframe::DataFrame> = None;
    for (table, _lo, hi) in windows {
        let part = hist_instant_scan(engine, vs, name, &table, hi).await?;
        base = Some(match base {
            Some(acc) => acc.union(part)?,
            None => part,
        });
    }
    let base = base.ok_or_else(|| to_err("resolver returned no windows".to_string()))?;
    // Latest histogram row per series at/before the eval time (keyed on the
    // series-key UDF since the attributes Map isn't partitionable).
    let latest = super::plan::frame::latest_per_series(
        base,
        vec![col("name"), col("service_name"), prom_series_key_expr()],
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
    let time_s = anchor as f64 / 1_000_000_000.0;

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

/// Fold one contributing series value at a grid timestamp into the aggregate's
/// running accumulator. `n` is the count of series already folded in for this
/// timestamp (so `n == 0` is the first contributor — `min`/`max` must seed from
/// `v`, not from the `0.0` initial accumulator). `avg` divides the summed
/// accumulator by `n` at the end (see [`aggregate_range_series`]).
fn reduce_step(op: &str, acc: f64, n: u64, v: f64) -> f64 {
    match op {
        "min" if n == 0 => v,
        "max" if n == 0 => v,
        "min" => acc.min(v),
        "max" => acc.max(v),
        "count" => acc + 1.0,
        // sum and avg both accumulate the sum; avg divides by `n` afterwards.
        _ => acc + v,
    }
}

/// Reduce already-evaluated inner range series across series, per **grid**
/// timestamp ([ADR: aggregation-pushdown] "Amendment"). Each inner series is
/// resampled onto the `[s, e]` grid at `step_ns` (carry-forward within
/// staleness) so every live series contributes at every grid step; series are
/// then grouped by the `AggGrouping` canonical key and reduced
/// (sum/min/max/avg/count) at each grid timestamp.
///
/// `step_ns <= 0` (raw-sample step) skips grid alignment and reduces over the
/// raw points as-is — the aggregate then groups by the literal sample timestamp,
/// matching the pre-grid behaviour for the rare no-step path.
///
/// [ADR: aggregation-pushdown]: ../../docs/20260615_promql-pushdown/adrs/2026-06-15_aggregation-pushdown.md
#[allow(clippy::cast_precision_loss)] // ns→s for the grid; sub-ms precision irrelevant
fn aggregate_range_series(
    op: &str,
    grouping: &AggGrouping,
    inner: RangeSeries,
    s: i64,
    e: i64,
    step_ns: i64,
) -> RangeSeries {
    let grid = step_ns > 0 && (e - s) / step_ns <= MAX_GRID_POINTS;
    let staleness = step_ns.max(STALENESS_NS);
    // group key → (result labels, ts(bits) → (acc, count)).
    let mut groups: BTreeMap<String, (BTreeMap<String, String>, BTreeMap<u64, (f64, u64)>)> =
        BTreeMap::new();
    for (_k, (labels, points)) in inner {
        let aligned = if grid {
            resample_to_grid(&points, s, e, step_ns, staleness)
        } else {
            points
        };
        let result_labels = grouping.result_labels(&labels);
        let gk = super::group_key::GroupKey::build(&labels, grouping);
        let entry = groups
            .entry(gk)
            .or_insert_with(|| (result_labels, BTreeMap::new()));
        for (t, v) in aligned {
            let cell = entry.1.entry(t.to_bits()).or_insert((0.0, 0));
            cell.0 = reduce_step(op, cell.0, cell.1, v);
            cell.1 += 1;
        }
    }
    let mut out: RangeSeries = BTreeMap::new();
    for (gk, (labels, by_ts)) in groups {
        let mut pts: Vec<(f64, f64)> = by_ts
            .into_iter()
            .map(|(tbits, (acc, n))| {
                let t = f64::from_bits(tbits);
                #[allow(clippy::cast_precision_loss)]
                let v = if op == "avg" { acc / n as f64 } else { acc };
                (t, v)
            })
            .collect();
        pts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        out.insert(gk, (labels, pts));
    }
    out
}

/// A range shard's scan + emit bounds (FR2). `scan_start ≤ start`: the underlying
/// scan runs `[scan_start, end]` so a windowed op has its full `(t−range, t]`
/// window at `start`, while output points are emitted only for `[start, end]`.
#[derive(Debug, Clone, Copy)]
struct RangeWindow {
    /// Scan lower bound `= start − lookback` (seeds the window/LAG at the edge).
    scan_start: i64,
    /// First emitted timestamp (inclusive).
    start: i64,
    /// Last emitted timestamp (inclusive) and the scan upper bound.
    end: i64,
}

/// Evaluate a range sub-expression over one `[s, e]` window against `table`.
///
/// `step_ns` is the query step: the range cross-series aggregate uses it to
/// grid-align each inner series **before** reducing, so series scraped at
/// offset instants all contribute at every grid timestamp (Sol↔Mimir parity —
/// [ADR: aggregation-pushdown] "Amendment"). `step_ns <= 0` means no grid
/// (raw-sample step), in which case the aggregate reduces over the raw points.
///
/// `scan_start` (FR2) is the underlying scan's lower bound — `s − lookback` — so a
/// windowed op has its full `(t−range, t]` window (and LAG predecessor) at `s`; the
/// emit/grid window stays `[s, e]`, and the caller filters the returned points to
/// `[s, e]` so the lookback region isn't double-emitted across shards.
///
/// [ADR: aggregation-pushdown]: ../../docs/20260615_promql-pushdown/adrs/2026-06-15_aggregation-pushdown.md
async fn eval_range_window(
    engine: &super::QueryEngine,
    expr: &Expr,
    win: RangeWindow,
    table: &str,
    step_ns: i64,
    now_ns: i64,
) -> crate::Result<RangeVal> {
    let RangeWindow { scan_start, start: s, end: e } = win;
    match expr {
        Expr::NumberLiteral(n) => Ok(RangeVal::Scalar(n.val)),
        Expr::Paren(p) => Box::pin(eval_range_window(engine, &p.expr, win, table, step_ns, now_ns)).await,
        Expr::Unary(u) => {
            let v = Box::pin(eval_range_window(engine, &u.expr, win, table, step_ns, now_ns)).await?;
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
            let l = Box::pin(eval_range_window(engine, &b.lhs, win, table, step_ns, now_ns)).await?;
            let r = Box::pin(eval_range_window(engine, &b.rhs, win, table, step_ns, now_ns)).await?;
            combine_range(b.op, l, r, &b.modifier).map_err(to_err)
        }
        // `scalar(v)` folds to a constant over the window (e.g. a CPU count as a
        // divisor): evaluate it as an instant at the window end. This also lets a
        // non-range inner like `count(count(…) by (cpu))` work in a range query.
        Expr::Call(c) if c.func.name == "scalar" => {
            let arg = c
                .args
                .args
                .first()
                .ok_or_else(|| to_err("scalar() requires one argument".to_string()))?;
            // Range-window context: `e` is a finite window end, so it doubles as
            // the anchor (instant_anchor is a no-op for a non-sentinel time).
            let v = Box::pin(eval_instant(engine, arg, e, e)).await?;
            Ok(RangeVal::Scalar(instant_to_scalar(v)))
        }
        // `clamp_min`/`clamp_max`: floor/cap every point of every series.
        Expr::Call(c) if matches!(c.func.name, "clamp_min" | "clamp_max") => {
            let is_min = c.func.name == "clamp_min";
            let vec_arg = c
                .args
                .args
                .first()
                .ok_or_else(|| to_err(format!("{}() requires two arguments", c.func.name)))?;
            let bound_arg = c
                .args
                .args
                .get(1)
                .ok_or_else(|| to_err(format!("{}() requires two arguments", c.func.name)))?;
            let v = Box::pin(eval_range_window(engine, vec_arg, win, table, step_ns, now_ns)).await?;
            let bound = instant_to_scalar(Box::pin(eval_instant(engine, bound_arg, e, e)).await?);
            Ok(match v {
                RangeVal::Scalar(x) => RangeVal::Scalar(clamp_value(is_min, x, bound)),
                RangeVal::Vector(mut m) => {
                    for (_labels, pts) in m.values_mut() {
                        for p in pts.iter_mut() {
                            p.1 = clamp_value(is_min, p.1, bound);
                        }
                    }
                    RangeVal::Vector(m)
                }
            })
        }
        // Range cross-series aggregation: PromQL evaluates the inner (rate /
        // *_over_time / selector) **per series at each step**, then reduces across
        // series at that step. We therefore evaluate the inner to per-series point
        // lists, grid-align each onto the `[s, e]` step grid (carry-forward within
        // staleness), then group by `by`/`without`/all and reduce per **grid**
        // timestamp ([ADR: aggregation-pushdown] "Amendment"). Grouping by the raw
        // sample timestamp (the old DataFusion `GROUP BY …, time_unix_nano`) made
        // offset-scraped series fall in different buckets, collapsing a cross-series
        // `sum` to one series — the Sol↔Mimir under-sum. Yield to the `_` arm's
        // special detectors (bucket heatmap / histogram quantile), whose
        // `sum by (le) …` shape is also a simple aggregate.
        Expr::Aggregate(agg)
            if agg_name(agg.op).is_ok()
                && detect_bucket_heatmap(expr).is_none()
                && detect_hist_quantile(expr).is_none() =>
        {
            let op = agg_name(agg.op).map_err(to_err)?;
            let grouping = AggGrouping::from(&agg.modifier);
            let inner =
                Box::pin(eval_range_window(engine, agg.expr.as_ref(), win, table, step_ns, now_ns)).await?;
            let inner = match inner {
                // A scalar inner has no series to reduce — pass it through.
                RangeVal::Scalar(x) => return Ok(RangeVal::Scalar(x)),
                RangeVal::Vector(v) => v,
            };
            // Grid stays `[s, e]` (the emit window); the inner already scanned back
            // to `scan_start` so its window is full at the left edge.
            Ok(RangeVal::Vector(aggregate_range_series(
                op, &grouping, inner, s, e, step_ns,
            )))
        }
        _ => {
            if let Some(spec) = detect_hist_quantile(expr) {
                let resp = handle_hist_quantile_range(engine, &spec, s, e, step_ns, now_ns).await?;
                Ok(RangeVal::Vector(matrix_to_series(resp)))
            } else if let Some(spec) = detect_bucket_heatmap(expr) {
                let resp = handle_bucket_heatmap(engine, &spec, s, e, step_ns, now_ns).await?;
                Ok(RangeVal::Vector(matrix_to_series(resp)))
            } else if let Some((n, is_topk, inner)) = topk_parts(expr) {
                // topk/bottomk is relational: lower the inner to a DataFrame and
                // rank whole series by peak via a window (`DENSE_RANK … <= k`),
                // superseding the Rust sort-truncate. Scan from `scan_start` for the
                // left-edge window; the caller filters points back to `[s, e]`.
                // Profiling seam (promql-plan-cache FR1): the logical lowering —
                // AST → `DataFrame` plan construction — is the `lower` stage.
                let t = std::time::Instant::now();
                let df = lower_range_df(engine, inner, scan_start, e, table).await?;
                let df = lower_topk_df(df, n, is_topk)?;
                super::telemetry::record_plan_stage("lower", t.elapsed());
                let scope = super::QueryScope {
                    lo_ns: scan_start,
                    hi_ns: e,
                };
                Ok(RangeVal::Vector(
                    range_series_from_df(engine, df, scope, step_ns).await?,
                ))
            } else {
                // Profiling seam (promql-plan-cache FR1): `lower` stage, as above.
                let t = std::time::Instant::now();
                let df = lower_range_df(engine, expr, scan_start, e, table).await?;
                super::telemetry::record_plan_stage("lower", t.elapsed());
                let scope = super::QueryScope {
                    lo_ns: scan_start,
                    hi_ns: e,
                };
                Ok(RangeVal::Vector(
                    range_series_from_df(engine, df, scope, step_ns).await?,
                ))
            }
            // NB: `step_ns` is unused here — the leaf/topk lowerings emit raw
            // per-series points; only the cross-series aggregate above grid-aligns.
        }
    }
}

/// Run a single-`v`/`time_unix_nano` SQL and group rows into instant samples
/// (latest value per series via [`LabelCols`]). `scope` is the query's
/// (anchored) evaluation point — cache TTL classification (FR2): only its
/// `hi_ns` decides sealedness, so the point window `[anchor, anchor]` is
/// exact even though the underlying scan looks back below the anchor.
async fn instant_vector_from_df(
    engine: &super::QueryEngine,
    df: datafusion::dataframe::DataFrame,
    scope: super::QueryScope,
) -> crate::Result<Vec<(BTreeMap<String, String>, f64)>> {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Float64Type};

    let batches = engine.collect_scoped(df, Some(scope)).await?;
    let mut out = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let v_idx = schema.index_of("v").map_err(|e| to_err(e.to_string()))?;
        let v = cast(batch.column(v_idx), &DataType::Float64)?;
        let v = v.as_primitive::<Float64Type>();
        let labels = SeriesLabels::build(batch)?;
        for i in 0..batch.num_rows() {
            if v.is_null(i) {
                continue;
            }
            out.push((labels.labels(i), v.value(i)));
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

/// Evaluate an instant sub-expression at `time_ns`. `now_ns` anchors an omitted
/// `time` (i64::MAX) for the range-window paths (see [`instant_anchor`]); it is
/// threaded but unused by the scalar/bare-selector branches.
async fn eval_instant(
    engine: &super::QueryEngine,
    expr: &Expr,
    time_ns: i64,
    now_ns: i64,
) -> crate::Result<InstantVal> {
    match expr {
        Expr::NumberLiteral(n) => Ok(InstantVal::Scalar(n.val)),
        Expr::Paren(p) => Box::pin(eval_instant(engine, &p.expr, time_ns, now_ns)).await,
        Expr::Unary(u) => {
            let v = Box::pin(eval_instant(engine, &u.expr, time_ns, now_ns)).await?;
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
            let l = Box::pin(eval_instant(engine, &b.lhs, time_ns, now_ns)).await?;
            let r = Box::pin(eval_instant(engine, &b.rhs, time_ns, now_ns)).await?;
            combine_instant(b.op, l, r, &b.modifier).map_err(to_err)
        }
        // `scalar(v)` collapses an instant vector to a scalar: the sole element's
        // value if the vector has exactly one element, else NaN (PromQL spec).
        Expr::Call(c) if c.func.name == "scalar" => {
            let arg = c
                .args
                .args
                .first()
                .ok_or_else(|| to_err("scalar() requires one argument".to_string()))?;
            let v = Box::pin(eval_instant(engine, arg, time_ns, now_ns)).await?;
            Ok(InstantVal::Scalar(instant_to_scalar(v)))
        }
        // `clamp_min(v, m)` / `clamp_max(v, m)`: floor/cap each sample at the
        // scalar bound `m`. Used by the RAM Used gauge's `clamp_min(…, 0)`.
        Expr::Call(c) if matches!(c.func.name, "clamp_min" | "clamp_max") => {
            let is_min = c.func.name == "clamp_min";
            let vec_arg = c
                .args
                .args
                .first()
                .ok_or_else(|| to_err(format!("{}() requires two arguments", c.func.name)))?;
            let bound_arg = c
                .args
                .args
                .get(1)
                .ok_or_else(|| to_err(format!("{}() requires two arguments", c.func.name)))?;
            let v = Box::pin(eval_instant(engine, vec_arg, time_ns, now_ns)).await?;
            let bound =
                instant_to_scalar(Box::pin(eval_instant(engine, bound_arg, time_ns, now_ns)).await?);
            Ok(match v {
                InstantVal::Scalar(x) => InstantVal::Scalar(clamp_value(is_min, x, bound)),
                InstantVal::Vector(mut items) => {
                    for it in &mut items {
                        it.1 = clamp_value(is_min, it.1, bound);
                    }
                    InstantVal::Vector(items)
                }
            })
        }
        // Aggregations are pushed into DataFusion as `GROUP BY prom_group_key` +
        // `agg(v)`, chained for nesting; the canonical frame is materialized back
        // into labels per output group ([ADR: aggregation-pushdown]).
        Expr::Aggregate(agg) if agg_name(agg.op).is_ok() => {
            let df = lower_aggregate_instant(engine, agg, time_ns, now_ns).await?;
            Ok(InstantVal::Vector(
                instant_vector_from_df(engine, df, instant_scope(time_ns, now_ns)).await?,
            ))
        }
        _ => {
            if let Some((phi, vs)) = histogram_quantile_parts(expr) {
                let resp = handle_histogram(engine, phi, vs, time_ns, now_ns).await?;
                let v = resp
                    .data
                    .result
                    .into_iter()
                    .map(|s| (s.metric, s.value.1.parse::<f64>().unwrap_or(f64::NAN)))
                    .collect();
                Ok(InstantVal::Vector(v))
            } else {
                let df = lower_instant_df(engine, expr, time_ns, now_ns).await?;
                Ok(InstantVal::Vector(
                    instant_vector_from_df(engine, df, instant_scope(time_ns, now_ns)).await?,
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
    async fn counter_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
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
                crate::querier::udf::tests::json_map_array(&["{}", "{}", "{}"]),
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
        crate::querier::QueryEngine::new(&opts).await.unwrap()
    }

    // Two services (`sol-collector` on hosts a,b; `other` on host c) each
    // exposing `up`, used to prove label-value queries honor a `match[]` scope.
    async fn host_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["sol-collector", "sol-collector", "other"])),
                Arc::new(StringArray::from(vec!["up", "up", "up"])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64, 2_000_000_000, 3_000_000_000])
                        .with_timezone("UTC"),
                ),
                crate::querier::udf::tests::json_map_array(&[
                    r#"{"host":"a"}"#,
                    r#"{"host":"b"}"#,
                    r#"{"host":"c"}"#,
                ]),
                Arc::new(Float64Array::from(vec![1.0, 1.0, 1.0])),
                Arc::new(StringArray::from(vec!["up", "up", "up"])),
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
    async fn test_label_values_honors_match_selector() {
        // A `$host` variable scoped to one service must only list that service's
        // hosts — `label_values(up{service_name="sol-collector"}, host)` → [a,b],
        // not the global [a,b,c]. Mirrors Grafana's match[]-scoped label query.
        let engine = host_engine().await;
        let scoped = handle_label_values(
            &engine,
            "host",
            0,
            i64::MAX,
            Some(r#"up{service_name="sol-collector"}"#),
            i64::MAX,
        )
        .await
        .unwrap();
        assert_eq!(scoped["data"], serde_json::json!(["a", "b"]), "scoped: {scoped}");

        // No selector → every host.
        let all = handle_label_values(&engine, "host", 0, i64::MAX, None, i64::MAX).await.unwrap();
        assert_eq!(all["data"], serde_json::json!(["a", "b", "c"]), "unscoped: {all}");
    }

    #[tokio::test]
    async fn test_instant_scalar_function_unwraps_single_series() {
        // `scalar(v)` on a one-element vector yields that value (then composes
        // in arithmetic) — the Sys Load gauge shape `scalar(node_load1{…}) * …`.
        let engine = counter_engine().await;
        let resp = handle_instant(&engine, "scalar(http_total) * 2", 3_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1, "scalar → one value: {:?}", resp.data.result);
        assert_eq!(resp.data.result[0].value.1, "120", "60 * 2");
    }

    // Four series of `m` over (cpu ∈ {0,1}) × (mode ∈ {user,system}) at one
    // service, two timestamps — the shape the Node Exporter CPU panels query.
    async fn cpu_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        // (cpu, mode, value) at t=1s, repeated at t=2s with the same values.
        let dims = [("0", "user", 1.0), ("0", "system", 2.0), ("1", "user", 3.0), ("1", "system", 4.0)];
        let (mut svc, mut name, mut t, mut attrs, mut val, mut pn) =
            (vec![], vec![], vec![], vec![], vec![], vec![]);
        for ts in [1_000_000_000i64, 2_000_000_000] {
            for (cpu, mode, v) in dims {
                svc.push("svc");
                name.push("m");
                t.push(ts);
                attrs.push(format!(r#"{{"cpu":"{cpu}","mode":"{mode}"}}"#));
                val.push(v);
                pn.push("m");
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(svc)),
                Arc::new(StringArray::from(name)),
                Arc::new(TimestampNanosecondArray::from(t).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&attrs),
                Arc::new(Float64Array::from(val)),
                Arc::new(StringArray::from(pn)),
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
    async fn test_instant_nested_count_aggregate() {
        // CPU Cores panel: count(count(m) by (cpu)) → number of cpus (2). Requires
        // an aggregate whose inner is itself an aggregate (not a bare selector).
        let engine = cpu_engine().await;
        let r = handle_instant(&engine, "count(count(m) by (cpu))", 2_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(r.data.result.len(), 1, "{:?}", r.data.result);
        assert_eq!(r.data.result[0].value.1, "2");
    }

    #[tokio::test]
    async fn test_instant_without_keeps_complement() {
        // sum without(mode) (m): groups drop `mode` (and __name__), keep cpu →
        // cpu0: 1+2=3, cpu1: 3+4=7.
        let engine = cpu_engine().await;
        let r = handle_instant(&engine, "sum without(mode) (m)", 2_000_000_000, i64::MAX)
            .await
            .unwrap();
        let mut got: Vec<(String, String)> = r
            .data
            .result
            .iter()
            .map(|s| (s.metric.get("cpu").cloned().unwrap_or_default(), s.value.1.clone()))
            .collect();
        got.sort();
        assert_eq!(got, vec![("0".into(), "3".into()), ("1".into(), "7".into())]);
        assert!(r.data.result.iter().all(|s| !s.metric.contains_key("mode")), "mode dropped");
    }

    #[tokio::test]
    async fn test_instant_clamp_min_floors_values() {
        // RAM Used panel uses clamp_min(…, 0); here clamp_min(m, 2) floors at 2.
        let engine = cpu_engine().await;
        let r = handle_instant(&engine, "clamp_min(m, 2)", 2_000_000_000, i64::MAX).await.unwrap();
        let mut vals: Vec<&str> = r.data.result.iter().map(|s| s.value.1.as_str()).collect();
        vals.sort_unstable();
        assert_eq!(vals, ["2", "2", "3", "4"]); // 1→2, 2→2, 3,4 unchanged
    }

    #[tokio::test]
    async fn test_range_without_aggregation_and_scalar_divisor() {
        // CPU Basic shape: sum without(mode) (m) over a range → per-cpu series.
        let engine = cpu_engine().await;
        let r = handle_range(&engine, "sum without(mode) (m)", 1_000_000_000, 2_000_000_000, 1_000_000_000, 2_000_000_000)
            .await
            .unwrap();
        let mut cpus: Vec<&str> = r
            .data
            .result
            .iter()
            .map(|s| s.metric.get("cpu").map(String::as_str).unwrap_or_default())
            .collect();
        cpus.sort_unstable();
        assert_eq!(cpus, ["0", "1"], "one series per cpu: {:?}", r.data.result);
        // Every point on each series is the user+system sum (3 / 7).
        for s in &r.data.result {
            let want = if s.metric.get("cpu").map(String::as_str) == Some("0") { "3" } else { "7" };
            assert!(s.values.iter().all(|(_, v)| v == want), "cpu sum: {:?}", s.values);
        }

        // CPU panel shape: scalar(count(count(m) by (cpu))) folds to the cpu count.
        let r = handle_range(
            &engine,
            "sum(m) / scalar(count(count(m) by (cpu)))",
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            2_000_000_000,
        )
        .await
        .unwrap();
        // sum(m) = 10 over all four series; /2 cpus = 5.
        assert!(!r.data.result.is_empty());
        assert!(r.data.result[0].values.iter().all(|(_, v)| v == "5"), "{:?}", r.data.result[0].values);
    }

    #[tokio::test]
    async fn test_instant_aggregate_over_rate() {
        // Gauge-panel shape: an *instant* `avg(rate(metric[5m]))` must evaluate
        // (over the [T-5m, T] window) instead of erroring "aggregate inner must
        // be a vector selector".
        let engine = counter_engine().await;
        let resp = handle_instant(&engine, "avg(rate(http_total[5m]))", 3_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(
            resp.data.result.len(),
            1,
            "one aggregated instant value: {:?}",
            resp.data.result
        );
        // bare instant rate is also accepted (not just inside an aggregate).
        let bare = handle_instant(&engine, "rate(http_total[5m])", 3_000_000_000, i64::MAX)
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
            10_000_000_000,
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

    #[test]
    fn test_materialization_reads_map_no_json() {
        // FR3/T7: the raw-selector materialization reads the columnar `attributes`
        // MAP directly — no JSON parse. Build a LabelCols over a Map column and
        // assert the exploded labels (normalized keys, promoted column present).
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let rows = [
            r#"{"http.route":"/a","code":"200"}"#,
            r#"{"http.route":"/b","code":"500"}"#,
            r#"{"http.route":"/a","code":"200"}"#,
        ];
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            crate::querier::udf::tests::attributes_map_field(),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["client"; rows.len()])),
                crate::querier::udf::tests::json_map_array(&rows),
            ],
        )
        .unwrap();

        let cols = LabelCols::build(&batch).unwrap();
        // The attributes column was bound as a MapArray (not Utf8).
        assert!(cols.attrs.is_some(), "attributes read as a MAP column");
        for i in 0..batch.num_rows() {
            let m = cols.labels(i);
            assert_eq!(m["service_name"], "client");
            // dotted OTLP keys are normalized: http.route → http_route.
            assert!(m.contains_key("http_route") && m.contains_key("code"));
        }
        // Distinct label sets preserved across the repeated blob.
        assert_eq!(cols.labels(0)["http_route"], "/a");
        assert_eq!(cols.labels(1)["http_route"], "/b");
    }

    #[tokio::test]
    async fn test_rate_executes_and_computes_values() {
        let engine = counter_engine().await;
        let resp = handle_range(&engine, "rate(http_total[5m])", 0, 10_000_000_000, 0, 10_000_000_000)
            .await
            .unwrap();
        assert_eq!(resp.data.result_type, "matrix");
        assert_eq!(resp.data.result.len(), 1, "one series");
        let s = &resp.data.result[0];
        assert_eq!(s.metric["service_name"], "client");
        // Windowed Prometheus `extrapolatedRate` (NOT irate), 5m (300s) window.
        // Fixture: t=1s→10, t=2s→30, t=3s→60. t=1s is the first sample (delta NULL
        // → no point). Hand-derivation of extrapolatedRate (promql/functions.go):
        //  • t=2s, window (−298,2]: in-window samples {1s:10, 2s:30}; base
        //    reset-adjusted increase result = 30−10 = 20; first_value = 10; cnt = 2;
        //    sampledInterval = (2−1) = 1s; avg_gap = 1/(2−1) = 1s.
        //    durationToStart: raw = first_t − (last_t−range) = 1−(2−300) = 299s, but
        //    counter zero-clamp caps it — durationToZero = sampledInterval ·
        //    (first_value/result) = 1·(10/20) = 0.5s < 299 → durationToStart = 0.5s
        //    (< 1.1·avg_gap, so no boundary cap). durationToEnd = 0.
        //    factor = (1 + 0.5 + 0)/1 = 1.5; extrapolated = 20·1.5 = 30;
        //    rate = 30/300 = 0.1.
        //  • t=3s, window (−297,3]: samples {1s:10, 2s:30, 3s:60}; result = 60−10 =
        //    50; first_value = 10; cnt = 3; sampledInterval = (3−1) = 2s;
        //    avg_gap = 2/(3−1) = 1s. durationToZero = 2·(10/50) = 0.4s <
        //    durationToStart_raw(298) → durationToStart = 0.4s; durationToEnd = 0.
        //    factor = (2 + 0.4 + 0)/2 = 1.2; extrapolated = 50·1.2 = 60;
        //    rate = 60/300 = 0.2.
        assert_eq!(
            s.values,
            vec![
                (2.0, (0.1_f64).to_string()),
                (3.0, (0.2_f64).to_string())
            ]
        );
    }

    /// promql-plan-cache task 2a (ADR A′): per cached shape — rate range, bare
    /// selector range, sum-by — REBINDING the cached optimized plan onto a new
    /// window (window-literal rewrite + provider swap) must produce exactly
    /// the plan a fresh build+optimize would, display-level. Two engines over
    /// identical stores: the optimizer's alias generator is session-scoped, so
    /// displays only compare across fresh sessions.
    #[tokio::test]
    async fn test_rebound_plan_equals_fresh_plan() {
        use crate::querier::plan_cache;
        for query in [
            "rate(http_total[2s])",
            "http_total",
            "sum by (service_name) (rate(http_total[2s]))",
        ] {
            let expr = parser::parse(query).unwrap();
            let engine_a = counter_engine().await;
            let engine_b = counter_engine().await;
            // Same shape, slid (and resized) window.
            let df_a = lower_range_df(&engine_a, &expr, 1_000_000_000, 3_000_000_000, "metrics")
                .await
                .unwrap();
            let df_b = lower_range_df(&engine_b, &expr, 61_000_000_000, 64_000_000_000, "metrics")
                .await
                .unwrap();
            let (state_a, plan_a) = df_a.into_parts();
            let (state_b, plan_b) = df_b.into_parts();
            let shape_a = plan_cache::analyze(&plan_a).unwrap();
            let shape_b = plan_cache::analyze(&plan_b).unwrap();
            assert_eq!(
                shape_a.shape, shape_b.shape,
                "window slide must not change the shape key for {query}"
            );
            let cached = plan_cache::CachedPlan {
                optimized: state_a.optimize(&plan_a).unwrap(),
                window_values: shape_a.window_values,
            };
            let rebound = plan_cache::rebind(&cached, &shape_b)
                .unwrap_or_else(|| panic!("rebind must be total for {query}"));
            let fresh = state_b.optimize(&plan_b).unwrap();
            assert_eq!(
                rebound.display_indent().to_string(),
                fresh.display_indent().to_string(),
                "rebound cached plan must equal freshly-optimized plan for {query}"
            );
        }
    }

    /// promql-plan-cache task 2a: a plan-cache HIT must serve a byte-identical
    /// HTTP-level response to a cold (miss) evaluation of the same query. Two
    /// engines over identical stores: on the first, window A warms the shape
    /// and window B is served via the rebound cached plan; on the second,
    /// window B runs cold. The serialized responses must match byte-for-byte.
    #[tokio::test]
    async fn test_plan_cache_hit_result_identical() {
        let (q, step, now) = ("rate(http_total[2s])", 1_000_000_000, 10_000_000_000);
        let warmed = counter_engine().await;
        let _a = handle_range(&warmed, q, 1_000_000_000, 3_000_000_000, step, now)
            .await
            .unwrap();
        let b_hit = handle_range(&warmed, q, 2_000_000_000, 3_000_000_000, step, now)
            .await
            .unwrap();
        assert_eq!(
            warmed.plan_cache_counts(),
            (1, 1, 0),
            "(hits, misses, bypasses): window B must be served via the plan cache"
        );

        let cold = counter_engine().await;
        let b_cold = handle_range(&cold, q, 2_000_000_000, 3_000_000_000, step, now)
            .await
            .unwrap();
        assert_eq!(cold.plan_cache_counts(), (0, 1, 0), "cold engine misses");
        assert_eq!(
            serde_json::to_vec(&b_hit).unwrap(),
            serde_json::to_vec(&b_cold).unwrap(),
            "hit and miss must serve byte-identical responses"
        );
    }

    /// A **bursty** counter (http_total, service=client): idle at 0 for the first
    /// three samples, a single +600 jump between t=3s and t=4s, then idle again at
    /// 600. Used to prove `rate` is windowed (average over the range), not `irate`
    /// (the latest inter-sample slope).
    async fn bursty_counter_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        const S: i64 = 1_000_000_000;
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["client"; 5])),
                Arc::new(StringArray::from(vec!["http_total"; 5])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![S, 2 * S, 3 * S, 4 * S, 5 * S])
                        .with_timezone("UTC"),
                ),
                crate::querier::udf::tests::json_map_array(&["{}"; 5]),
                // idle, idle, idle, +600 burst, idle
                Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0, 600.0, 600.0])),
                Arc::new(StringArray::from(vec!["http_total"; 5])),
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
    async fn test_rate_is_windowed_not_irate() {
        // Bursty counter: a single +600 jump, idle before and after. Evaluated at
        // t=5s with a 5m window covering the whole burst.
        //   windowed rate = total increase 600 / 300s window = 2/s.
        //   irate (latest inter-sample slope) = (600-600)/1s = 0/s.
        // The two differ sharply, proving `rate` is NOT irate.
        let engine = bursty_counter_engine().await;

        let windowed = handle_instant(&engine, "rate(http_total[5m])", 5_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(windowed.data.result.len(), 1, "{:?}", windowed.data.result);
        let v: f64 = windowed.data.result[0].value.1.parse().unwrap();
        assert!(
            (v - 2.0).abs() < 1e-9,
            "windowed rate = 600/300s = 2/s, got {v} (would be 0/s if irate)"
        );

        // The latest inter-sample slope (irate) at t=5s is 0/s — distinct from the
        // windowed 2/s, confirming the new semantics are windowed, not irate.
        let irate = handle_instant(&engine, "irate(http_total[5m])", 5_000_000_000, i64::MAX)
            .await
            .unwrap();
        let iv: f64 = irate.data.result[0].value.1.parse().unwrap();
        assert!((iv - 0.0).abs() < 1e-9, "irate = latest slope = 0/s, got {iv}");
    }

    // Two monotonic counter series of `reqs` distinguished by `host` (a, b),
    // scraped at *offset* instants: host a at t=0,30,60,90s; host b at
    // t=15,45,75,105s. Both rise by 30 every 30s → a per-series rate of 1/s.
    // The shape that exposed the Sol↔Mimir under-sum: a cross-series
    // `sum(rate(reqs[2m]))` must equal a+b (~2/s), not collapse to one series.
    async fn offset_counter_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        const S: i64 = 1_000_000_000;
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let (mut svc, mut name, mut t, mut attrs, mut val, mut pn) =
            (vec![], vec![], vec![], vec![], vec![], vec![]);
        // host a at 0,30,60,90s; host b offset by 15s at 15,45,75,105s.
        for (host, offset) in [("a", 0i64), ("b", 15)] {
            for step in 0..4i64 {
                svc.push("svc".to_string());
                name.push("reqs".to_string());
                t.push((offset + step * 30) * S);
                attrs.push(format!(r#"{{"host":"{host}"}}"#));
                #[allow(clippy::cast_precision_loss)]
                val.push((step * 30) as f64); // 0,30,60,90 → +30 per 30s = 1/s
                pn.push("reqs".to_string());
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(svc)),
                Arc::new(StringArray::from(name)),
                Arc::new(TimestampNanosecondArray::from(t).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&attrs),
                Arc::new(Float64Array::from(val)),
                Arc::new(StringArray::from(pn)),
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
    async fn test_range_sum_rate_over_offset_series_matches_sum_of_rates() {
        // Sol↔Mimir parity contract: `sum(rate(reqs[2m]))` over two series scraped
        // at offset instants must equal rate_a + rate_b at each grid step (~2/s),
        // NOT collapse to one series (~1/s). The fix grid-aligns each inner series
        // before the cross-series reduce; before it, the aggregate grouped by the
        // raw sample timestamp, so each grid step saw only one host's point.
        const S: i64 = 1_000_000_000;
        let engine = offset_counter_engine().await;
        let resp = handle_range(
            &engine,
            "sum(rate(reqs[2m]))",
            0,
            120 * S,
            30 * S,
            120 * S,
        )
        .await
        .unwrap();
        assert_eq!(resp.data.result.len(), 1, "one summed series: {:?}", resp.data.result);
        let s = &resp.data.result[0];
        // Windowed rate (no extrapolation): a counter rising +30 per 30s, sampled
        // every 30s over a 2m=120s window, captures ~3 deltas (90 increase) → a
        // per-host windowed rate of ~0.75/s, NOT the irate 1/s (the documented
        // extrapolation gap). The parity property the test guards is that BOTH
        // hosts contribute: the summed rate (~1.5/s) must clearly exceed a single
        // host's (~0.75/s) — it does not collapse to one series.
        let late: Vec<f64> = s
            .values
            .iter()
            .filter(|(t, _)| *t >= 90.0)
            .map(|(_, v)| v.parse::<f64>().unwrap())
            .collect();
        assert!(!late.is_empty(), "grid has late points: {:?}", s.values);
        for v in &late {
            assert!(
                (*v - 1.5).abs() < 0.3,
                "summed windowed rate ≈ 1.5/s (a+b ≈ 0.75 each), not one series ≈ 0.75/s: {:?}",
                s.values
            );
        }
    }

    #[tokio::test]
    async fn test_range_sum_by_host_rate_keeps_two_series() {
        // Regression guard: `sum by(host)(rate(reqs[2m]))` over the same offset
        // fixture still yields two correct per-host series (each ~1/s).
        const S: i64 = 1_000_000_000;
        let engine = offset_counter_engine().await;
        let resp = handle_range(
            &engine,
            "sum by(host)(rate(reqs[2m]))",
            0,
            120 * S,
            30 * S,
            120 * S,
        )
        .await
        .unwrap();
        let mut hosts: Vec<&str> = resp
            .data
            .result
            .iter()
            .map(|s| s.metric.get("host").map(String::as_str).unwrap_or_default())
            .collect();
        hosts.sort_unstable();
        assert_eq!(hosts, ["a", "b"], "one series per host: {:?}", resp.data.result);
        for s in &resp.data.result {
            // Windowed rate (no extrapolation): as the 120s window fills, each
            // per-host rate ramps toward ~0.75/s (90 increase / 120s window) by the
            // last grid step — NOT the irate 1/s (the documented extrapolation
            // gap). Assert the fully-established value at the final grid step.
            let last = s.values.last().map(|(_, v)| v.parse::<f64>().unwrap());
            assert!(
                last.is_some_and(|x| (x - 0.75).abs() < 0.2),
                "per-host windowed rate ≈ 0.75/s when established: {:?}",
                s.values
            );
        }
    }

    #[tokio::test]
    async fn test_instant_normalizes_name_and_explodes_attributes() {
        // C-P1: a bare instant selector must return the normalized __name__ and
        // explode the attributes JSON into per-label series (not collapse them).
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("is_monotonic", DataType::Boolean, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        // Two series of the same OTLP metric, differing only by status_code.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
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
                crate::querier::udf::tests::json_map_array(&[
                    r#"{"http.response.status_code":"200","http.route":"/user"}"#,
                    r#"{"http.response.status_code":"500","http.route":"/user"}"#,
                ]),
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
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();

        let resp = handle_instant(&engine, "http_server_requests_bytes", 2_000_000_000, i64::MAX)
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
        let resp = handle_range(&engine, "rate(http_total[5m])", 0, two_days, 0, two_days)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1, "one merged series across shards");
        // Same `counter_engine` fixture + 5m window as
        // `test_rate_executes_and_computes_values`, so the same hand-derived
        // extrapolatedRate points: t=2s → 20·1.5/300 = 0.1, t=3s → 50·1.2/300 = 0.2
        // (see that test for the full derivation). Split/merge must preserve them.
        assert_eq!(
            resp.data.result[0].values,
            vec![
                (2.0, (0.1_f64).to_string()),
                (3.0, (0.2_f64).to_string())
            ],
            "split+merge equals the unsplit windowed rate"
        );
    }

    #[tokio::test]
    async fn test_long_range_keeps_live_tail_when_tier_selected() {
        // Regression: a coarse-step long range routes to a rollup tier table
        // (`metrics_5m`). Rollups only cover *sealed* days — the active day is
        // never rolled up. Routing the *whole* range to the tier silently drops
        // the live tail (the symptom: rate panels miss recent data while the raw
        // histogram path shows it). The live (unsealed) shard must read raw.
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        const DAY_NS: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        // dt=1970-01-01: the epoch day the fixture's epoch-relative timestamps
        // actually live in — a `rollup-*.parquet` file's pruning interval (FR1)
        // is parsed from its `dt=` day, so a mismatched day would prune it out.
        let dir = tmp.path().join("metrics").join("sum").join("dt=1970-01-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            crate::querier::udf::tests::attributes_map_field(),
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
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["s"; n])),
                    Arc::new(StringArray::from(vec!["reqs"; n])),
                    crate::querier::udf::tests::json_map_array(&vec![r#"{"sc":"a"}"#; n]),
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
        let mut w = ArrowWriter::try_new(f, Arc::clone(&schema), None).unwrap();
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
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();
        assert!(engine.has_table("metrics_5m"), "tier registered");

        // 5-minute step over a 2-day range → splits per day AND selects the M5 tier.
        let resp = handle_range(&engine, "sum by (sc) (rate(reqs[5m]))", 0, 2 * DAY_NS, M5, 2 * DAY_NS)
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
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
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
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["s", "s", "s", "s"])),
                Arc::new(StringArray::from(vec!["reqs", "reqs", "reqs", "reqs"])),
                crate::querier::udf::tests::json_map_array(&[
                    r#"{"sc":"a"}"#,
                    r#"{"sc":"a"}"#,
                    r#"{"sc":"b"}"#,
                    r#"{"sc":"b"}"#,
                ]),
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
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();
        let resp = handle_range(
            &engine,
            "topk(1, sum by (sc) (rate(reqs[5m])))",
            0,
            10_000_000_000,
            0,
            10_000_000_000,
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
        // Windowed extrapolatedRate over 5m (300s): sc=a is {t=1s:10, t=2s:30} — the
        // same shape as the `counter_engine` t=2s case: result = 30−10 = 20,
        // first_value = 10, cnt = 2, sampledInterval = 1s; durationToZero =
        // 1·(10/20) = 0.5s → factor = (1+0.5)/1 = 1.5; extrapolated = 20·1.5 = 30;
        // rate = 30/300 = 0.1 (t=1s is the first sample → no point). sc=b extrapolates
        // to a lower rate, so topk(1) keeps sc=a.
        assert_eq!(resp.data.result[0].values, vec![(2.0, (0.1_f64).to_string())]);
    }

    #[tokio::test]
    async fn test_topk_uses_window_plan() {
        // topk must lower to a window plan (DENSE_RANK over the series peak),
        // not the removed Rust sort-truncate: the logical plan carries a
        // WindowAggr with dense_rank — no `topk_series` on the path.
        let engine = counter_engine().await;
        let inner = parser::parse("rate(http_total[5m])").unwrap();
        let df = lower_range_df(&engine, &inner, 0, 10_000_000_000, "metrics")
            .await
            .unwrap();
        let df = lower_topk_df(df, 1, true).unwrap();
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(plan.contains("WindowAggr:"), "windowed topk: {plan}");
        assert!(plan.contains("dense_rank"), "ranks via dense_rank: {plan}");
        assert!(
            plan.contains("series_rank"),
            "filters on the rank column: {plan}"
        );
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
            10_000_000_000,
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
    async fn test_aggregation_grouped_in_plan() {
        // The aggregate must be pushed into DataFusion: the lowered logical plan
        // for `sum by (cpu) (m)` contains an `Aggregate` node (not a Rust reduce).
        let engine = cpu_engine().await;
        let expr = parser::parse("sum by (cpu) (m)").unwrap();
        let Expr::Aggregate(agg) = &expr else {
            panic!("expected an aggregate expr");
        };
        let df = lower_aggregate_instant(&engine, agg, 2_000_000_000, i64::MAX)
            .await
            .unwrap();
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(plan.contains("Aggregate:"), "grouping in-plan: {plan}");
        // and it groups on the prom_group_key column, not a Rust loop.
        assert!(plan.contains("prom_group_key"), "group key in-plan: {plan}");
    }

    #[tokio::test]
    async fn test_mixed_nesting_by_over_without() {
        // sum by (cpu) (sum without (mode) (m)): the inner drops `mode` (keeping
        // cpu), the outer re-projects to `by (cpu)`. Exercises reprojection.
        // cpu0: (1+2)=3, cpu1: (3+4)=7 — one series per cpu.
        let engine = cpu_engine().await;
        let r = handle_instant(
            &engine,
            "sum by (cpu) (sum without (mode) (m))",
            2_000_000_000,
            i64::MAX,
        )
        .await
        .unwrap();
        let mut got: Vec<(String, String)> = r
            .data
            .result
            .iter()
            .map(|s| {
                (
                    s.metric.get("cpu").cloned().unwrap_or_default(),
                    s.value.1.clone(),
                )
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![("0".into(), "3".into()), ("1".into(), "7".into())],
            "{:?}",
            r.data.result
        );
        // only cpu survives the outer by(cpu).
        assert!(
            r.data.result.iter().all(|s| !s.metric.contains_key("mode")),
            "mode dropped: {:?}",
            r.data.result
        );
    }

    #[tokio::test]
    async fn test_histogram_quantile_range_from_otlp_arrays() {
        // #4: the dashboard's `histogram_quantile(φ, sum(rate(<base>_bucket[d])) by (le))`
        // is served from the native OTLP array histogram (no classic _bucket series).
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["client"])),
                Arc::new(StringArray::from(vec!["http.server.request.duration"])),
                Arc::new(StringArray::from(vec![Some("s")])),
                Arc::new(BooleanArray::from(vec![Some(false)])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone("UTC"),
                ),
                crate::querier::udf::tests::json_map_array(&["{}"]),
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
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();
        // base `_bucket` query (normalized name http_server_request_duration_seconds)
        let resp = handle_range(
            &engine,
            "histogram_quantile(0.95, sum(rate(http_server_request_duration_seconds_bucket[1m])) by (le))",
            0,
            10_000_000_000,
            15,
            10_000_000_000,
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
    async fn test_histogram_quantile_range_routes_sealed_window_to_tier() {
        // histogram_quantile range must tier-route like handle_range: a sealed
        // window reads the 5m rollup (which preserves bucket_counts), NOT raw.
        // Distinct bucket_counts in raw vs tier prove which source served: raw
        // day-0 mass is all in the first bucket (p95 ≈ 9.5), tier day-0 mass is
        // all in +Inf (p95 = 50). A coarse-step query must yield the tier's 50.
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{BooleanArray, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        const DAY_NS: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        // dt=1970-01-01: matches the epoch-relative timestamps (the rollup
        // file's FR1 pruning interval derives from the `dt=` day).
        let dir = tmp
            .path()
            .join("metrics")
            .join("histogram")
            .join("dt=1970-01-01");
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let mk = |counts: &str| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["client"])),
                    Arc::new(StringArray::from(vec!["dur"])),
                    Arc::new(StringArray::from(vec![Some("s")])),
                    Arc::new(BooleanArray::from(vec![Some(false)])),
                    Arc::new(TimestampNanosecondArray::from(vec![M5]).with_timezone("UTC")),
                    crate::querier::udf::tests::json_map_array(&["{}"]),
                    Arc::new(StringArray::from(vec![Some(counts)])),
                    Arc::new(StringArray::from(vec![Some("[10,20,30,40,50]")])),
                    Arc::new(StringArray::from(vec!["dur_seconds"])),
                ],
            )
            .unwrap()
        };
        // Build both batches first so the `mk` closure's borrow of `schema` ends
        // before `schema` is moved into the second writer.
        let raw_batch = mk("[100,0,0,0,0,0]"); // all mass in first bucket → p95 ≈ 9.5
        let tier_batch = mk("[0,0,0,0,0,100]"); // all mass in +Inf → p95 = 50
        // raw day-0 (the "wrong" answer if served from raw).
        let f = std::fs::File::create(dir.join("h.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, Arc::clone(&schema), None).unwrap();
        w.write(&raw_batch).unwrap();
        w.close().unwrap();
        // rollup-5m tier day-0 (the answer that proves tier routing).
        let f = std::fs::File::create(dir.join("rollup-5m.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&tier_batch).unwrap();
        w.close().unwrap();

        let opts = QuerierOptions {
            storage: StorageConfig {
                path: tmp.path().into(),
                url: None,
            },
            ..QuerierOptions::default()
        };
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();
        assert!(engine.has_table("metrics_5m"), "tier registered");

        // coarse step (M5) over [0, 2d]: day-0 is sealed → must read the tier.
        let resp = handle_range(
            &engine,
            "histogram_quantile(0.95, sum(rate(dur_seconds_bucket[5m])) by (le))",
            0,
            2 * DAY_NS,
            M5,
            2 * DAY_NS,
        )
        .await
        .unwrap();
        let qs: Vec<f64> = resp
            .data
            .result
            .iter()
            .flat_map(|s| s.values.iter().map(|v| v.1.parse::<f64>().unwrap()))
            .collect();
        assert!(!qs.is_empty(), "expected a sealed-day point, got none");
        assert!(
            qs.iter().all(|q| (*q - 50.0).abs() < 1e-6),
            "sealed window must be served from the rollup tier (p95=50), got {qs:?}"
        );
    }

    #[tokio::test]
    async fn test_histogram_quantile_range_sealed_tier_trailing_raw_parity() {
        // Consolidation guard (FR3): the histogram range path now routes through
        // the single `resolve_metric_windows` choke point (capability Last) — the
        // standalone `tiered_hist_source`/`select_range_table` copy is gone. Prove
        // both windows of one query route correctly: a sealed-day point reads the
        // tier (rollup-5m: all mass in +Inf → p95=50), a trailing-day point reads
        // raw (all mass in first bucket → p95≈9.5, equal to the raw-only result).
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{BooleanArray, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        const DAY_NS: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let mk = |ts: i64, counts: &str| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["client"])),
                    Arc::new(StringArray::from(vec!["dur"])),
                    Arc::new(StringArray::from(vec![Some("s")])),
                    Arc::new(BooleanArray::from(vec![Some(false)])),
                    Arc::new(TimestampNanosecondArray::from(vec![ts]).with_timezone("UTC")),
                    crate::querier::udf::tests::json_map_array(&["{}"]),
                    Arc::new(StringArray::from(vec![Some(counts)])),
                    Arc::new(StringArray::from(vec![Some("[10,20,30,40,50]")])),
                    Arc::new(StringArray::from(vec!["dur_seconds"])),
                ],
            )
            .unwrap()
        };
        // Sealed day-0: raw mass in first bucket (the "wrong" answer), tier mass in
        // +Inf (the answer that proves day-0 read the tier).
        let raw_d0 = mk(M5, "[100,0,0,0,0,0]");
        let tier_d0 = mk(M5, "[0,0,0,0,0,100]");
        // Trailing day-2 (within the live ≤1-day window of end=2d): raw only, mass
        // in first bucket → p95≈9.5 must come from raw (no tier covers it).
        let raw_d2 = mk(2 * DAY_NS, "[100,0,0,0,0,0]");

        // dt=1970-01-01: matches the epoch-relative timestamps (the rollup
        // file's FR1 pruning interval derives from the `dt=` day). The raw
        // `h.parquet` name is interval-unparseable → unbounded, so its day-2
        // row is reachable from this day-0 dir either way.
        let d0 = tmp
            .path()
            .join("metrics")
            .join("histogram")
            .join("dt=1970-01-01");
        std::fs::create_dir_all(&d0).unwrap();
        let f = std::fs::File::create(d0.join("h.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, Arc::clone(&schema), None).unwrap();
        w.write(&raw_d0).unwrap();
        w.write(&raw_d2).unwrap();
        w.close().unwrap();
        let f = std::fs::File::create(d0.join("rollup-5m.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&tier_d0).unwrap();
        w.close().unwrap();

        let opts = QuerierOptions {
            storage: StorageConfig {
                path: tmp.path().into(),
                url: None,
            },
            ..QuerierOptions::default()
        };
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();
        assert!(engine.has_table("metrics_5m"), "tier registered");

        let resp = handle_range(
            &engine,
            "histogram_quantile(0.95, sum(rate(dur_seconds_bucket[5m])) by (le))",
            0,
            2 * DAY_NS,
            M5,
            2 * DAY_NS,
        )
        .await
        .unwrap();
        let qs: Vec<f64> = resp
            .data
            .result
            .iter()
            .flat_map(|s| s.values.iter().map(|v| v.1.parse::<f64>().unwrap()))
            .collect();
        assert!(!qs.is_empty(), "expected points, got none");
        // Sealed window → tier (p95=50); trailing window → raw (p95≈9.5).
        assert!(
            qs.iter().any(|q| (*q - 50.0).abs() < 1e-6),
            "a sealed-window point must come from the tier (p95=50), got {qs:?}"
        );
        assert!(
            qs.iter().any(|q| (*q - 9.5).abs() < 1e-6),
            "a trailing-window point must come from raw (p95≈9.5), got {qs:?}"
        );
    }

    #[tokio::test]
    async fn test_bucket_heatmap_explodes_le_series() {
        // #4 heatmap: sum(rate(<base>_bucket[d])) by (le) → per-le cumulative
        // bucket rate series, exploded from the OTLP arrays.
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        // two cumulative-increasing snapshots at t=1s and t=2s (bounds [10,20])
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
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
                crate::querier::udf::tests::json_map_array(&["{}", "{}"]),
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
        let engine = crate::querier::QueryEngine::new(&opts).await.unwrap();
        let resp = handle_range(
            &engine,
            "sum(rate(http_server_request_duration_seconds_bucket[1m])) by (le)",
            0,
            10_000_000_000,
            15,
            10_000_000_000,
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
            10_000_000_000,
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
        let resp = handle_instant(&engine, "http_total * 2", 3_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1);
        assert_eq!(resp.data.result[0].value.1, "120");
        let neg = handle_instant(&engine, "- http_total", 3_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(neg.data.result[0].value.1, "-60");
    }

    #[tokio::test]
    async fn test_instant_vector_vector_self_ratio() {
        let engine = counter_engine().await;
        let resp = handle_instant(&engine, "http_total / http_total", 3_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1);
        assert_eq!(resp.data.result[0].value.1, "1");
        assert!(!resp.data.result[0].metric.contains_key("__name__"));
    }

    #[tokio::test]
    async fn test_instant_comparison_filters_and_bool() {
        let engine = counter_engine().await;
        let none = handle_instant(&engine, "http_total > 100", 3_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert!(
            none.data.result.is_empty(),
            "60 > 100 is false → filtered out"
        );
        let some = handle_instant(&engine, "http_total > 50", 3_000_000_000, i64::MAX)
            .await
            .unwrap();
        assert_eq!(
            some.data.result[0].value.1, "60",
            "kept value is the LHS sample"
        );
        let b = handle_instant(&engine, "http_total > bool 100", 3_000_000_000, i64::MAX)
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
            5_000_000_000,
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

    /// High-cardinality synthetic store: `CARDINALITY` distinct `cpu` values, one
    /// service, `POINTS` timestamps each. One series per cpu (the `mode` label is
    /// constant), value = `cpu` index so `sum by (cpu)` is exactly the cpu index.
    /// Mirrors `cpu_engine` (writes a small Parquet store to a leaked tempdir) but
    /// at a scale that would blow an O(series×points) in-memory Rust reduce — the
    /// deleted baseline — while the DataFusion `GROUP BY prom_group_key` plan
    /// stays bounded.
    const HC_CARDINALITY: i64 = 300;
    const HC_POINTS: i64 = 60;

    async fn high_cardinality_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        #[allow(clippy::cast_possible_truncation)] // small test-fixture cardinality
        let n = (HC_CARDINALITY * HC_POINTS) as usize;
        let (mut svc, mut name, mut t, mut attrs, mut val, mut pn) = (
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
            Vec::with_capacity(n),
        );
        for p in 0..HC_POINTS {
            let ts = (p + 1) * 1_000_000_000;
            for cpu in 0..HC_CARDINALITY {
                svc.push("svc");
                name.push("m");
                t.push(ts);
                attrs.push(format!(r#"{{"cpu":"{cpu}","mode":"user"}}"#));
                #[allow(clippy::cast_precision_loss)] // small test-fixture cpu index
                val.push(cpu as f64);
                pn.push("m");
            }
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(svc)),
                Arc::new(StringArray::from(name)),
                Arc::new(TimestampNanosecondArray::from(t).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&attrs),
                Arc::new(Float64Array::from(val)),
                Arc::new(StringArray::from(pn)),
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
    async fn test_high_cardinality_aggregate_bounded() {
        // [NFR2]/[NFR3] regression guard: the migrated aggregate path runs through
        // the DataFusion `GROUP BY prom_group_key` plan (the O(series×points) Rust
        // reduce is deleted). Over HC_CARDINALITY series × HC_POINTS points, prove:
        //   * `sum by (cpu) (m)` yields exactly HC_CARDINALITY series (parity), each
        //     value = its cpu index (sum of one series == the series' value);
        //   * `sum without(mode) (m)` collapses the constant `mode` to the same set.
        // Deterministic counts/values only — no wall-clock assertion.
        let engine = high_cardinality_engine().await;
        let at = HC_POINTS * 1_000_000_000;

        let by = handle_instant(&engine, "sum by (cpu) (m)", at, i64::MAX).await.unwrap();
        #[allow(clippy::cast_possible_truncation)] // small test-fixture cardinality
        let expected_cardinality = HC_CARDINALITY as usize;
        assert_eq!(
            by.data.result.len(),
            expected_cardinality,
            "one series per distinct cpu (bounded by cardinality, not row count)"
        );
        // Each group is a single series, so its sum equals that cpu's value (index).
        let mut got: Vec<i64> = by
            .data
            .result
            .iter()
            .map(|s| {
                let cpu: i64 = s.metric["cpu"].parse().unwrap();
                #[allow(clippy::cast_possible_truncation)] // exact small integer sum
                let v: i64 = s.value.1.parse::<f64>().unwrap() as i64;
                assert_eq!(v, cpu, "sum of the single cpu={cpu} series is its value");
                cpu
            })
            .collect();
        got.sort_unstable();
        assert_eq!(got, (0..HC_CARDINALITY).collect::<Vec<_>>(), "all cpus present, no dupes");

        // `without(mode)` (mode constant) yields the same per-cpu cardinality.
        let without = handle_instant(&engine, "sum without(mode) (m)", at, i64::MAX).await.unwrap();
        #[allow(clippy::cast_possible_truncation)] // small test-fixture cardinality
        let expected_without = HC_CARDINALITY as usize;
        assert_eq!(without.data.result.len(), expected_without);
        assert!(
            without.data.result.iter().all(|s| !s.metric.contains_key("mode")),
            "mode dropped by without()"
        );

        // Grand total `sum(m)` collapses to one series = Σ cpu indices.
        let total = handle_instant(&engine, "sum(m)", at, i64::MAX).await.unwrap();
        assert_eq!(total.data.result.len(), 1, "grand total is one series");
        #[allow(clippy::cast_precision_loss)] // small test-fixture cpu indices
        let expected: f64 = (0..HC_CARDINALITY).map(|c| c as f64).sum();
        assert_eq!(total.data.result[0].value.1, format!("{expected}"));
    }

    /// Lightweight, non-gating benchmark. Run with
    /// `cargo test --features querier-backend --lib querier::bench_aggregate_24h -- --ignored --nocapture`
    /// to capture timings; ignored by default so it never flakes CI. Uses
    /// `std::time::Instant` only (no criterion dependency added to the run).
    #[tokio::test]
    #[ignore = "timing benchmark — run manually with --ignored --nocapture"]
    #[allow(clippy::print_stderr)] // benchmark timing output (run with --nocapture)
    async fn bench_aggregate_24h() {
        use std::time::Instant;
        let engine = high_cardinality_engine().await;
        let start = 1_000_000_000i64;
        let end = HC_POINTS * 1_000_000_000;
        let step = 1_000_000_000i64;

        let t0 = Instant::now();
        let agg = handle_range(&engine, "sum by (cpu) (m)", start, end, step, end).await.unwrap();
        let d_agg = t0.elapsed();

        let t1 = Instant::now();
        let rate = handle_range(&engine, "sum(rate(m[5m]))", start, end, step, end).await.unwrap();
        let d_rate = t1.elapsed();

        let t2 = Instant::now();
        let raw = handle_range(&engine, "m", start, end, step, end).await.unwrap();
        let d_raw = t2.elapsed();

        eprintln!(
            "bench_aggregate_24h: {} series × {} points\n  sum by (cpu)    -> {:>4} series in {:?}\n  sum(rate(m[5m])) -> {:>4} series in {:?}\n  raw selector     -> {:>4} series in {:?}",
            HC_CARDINALITY,
            HC_POINTS,
            agg.data.result.len(),
            d_agg,
            rate.data.result.len(),
            d_rate,
            raw.data.result.len(),
            d_raw,
        );
    }

    // --- operator → capability classifier + tier-resolution choke point ---

    #[test]
    fn test_op_capability_classes() {
        let cap = |q: &str| op_capability(&parser::parse(q).unwrap());
        // Last: rate/increase/histogram_quantile (incl. under sum-by-le + topk).
        assert_eq!(cap("rate(m[5m])"), Capability::Last);
        assert_eq!(cap("increase(m[5m])"), Capability::Last);
        assert_eq!(
            cap("topk(3, histogram_quantile(0.9, sum by(le)(rate(m_bucket[5m]))))"),
            Capability::Last
        );
        // MinMax: max_over_time/min_over_time.
        assert_eq!(cap("max_over_time(m[5m])"), Capability::MinMax);
        assert_eq!(cap("min_over_time(m[5m])"), Capability::MinMax);
        // SumCount: avg/sum/count_over_time.
        assert_eq!(cap("avg_over_time(m[5m])"), Capability::SumCount);
        assert_eq!(cap("sum_over_time(m[5m])"), Capability::SumCount);
        assert_eq!(cap("count_over_time(m[5m])"), Capability::SumCount);
        // None (force raw): irate, bare selector, unknown fn.
        assert_eq!(cap("irate(m[5m])"), Capability::None);
        assert_eq!(cap("m"), Capability::None);
        assert_eq!(cap("quantile_over_time(0.5, m[5m])"), Capability::None);
        // a real PromQL fn the classifier does not list → unclassified → None.
        assert_eq!(cap("stddev_over_time(m[5m])"), Capability::None);
    }

    /// Build a `QueryEngine` whose `metrics` table also has a registered
    /// `metrics_5m` rollup tier — the minimal fixture the resolver consults via
    /// `has_table`. Mirrors `test_long_range_keeps_live_tail_when_tier_selected`.
    async fn engine_with_5m_tier() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        // dt=1970-01-01: matches the t=0 sample (the rollup file's FR1 pruning
        // interval derives from the `dt=` day).
        let dir = tmp.path().join("metrics").join("sum").join("dt=1970-01-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            crate::querier::udf::tests::attributes_map_field(),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let mk = || {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["s"])),
                    Arc::new(StringArray::from(vec!["reqs"])),
                    crate::querier::udf::tests::json_map_array(&vec![r#"{"sc":"a"}"#]),
                    Arc::new(TimestampNanosecondArray::from(vec![0i64]).with_timezone("UTC")),
                    Arc::new(Float64Array::from(vec![1.0])),
                    Arc::new(StringArray::from(vec!["reqs"])),
                ],
            )
            .unwrap()
        };
        for f in ["m.parquet", "rollup-5m.parquet"] {
            let file = std::fs::File::create(dir.join(f)).unwrap();
            let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
            w.write(&mk()).unwrap();
            w.close().unwrap();
        }
        let opts = QuerierOptions {
            storage: StorageConfig {
                path: tmp.path().into(),
                url: None,
            },
            ..QuerierOptions::default()
        };
        crate::querier::QueryEngine::new(&opts).await.unwrap()
    }

    /// Windows must be ordered, time-disjoint, and cover `[start, end]` exactly.
    fn assert_disjoint_covering(windows: &[MetricWindow], start: i64, end: i64) {
        assert_eq!(windows.first().unwrap().1, start, "first window starts at start");
        assert_eq!(windows.last().unwrap().2, end, "last window ends at end");
        for pair in windows.windows(2) {
            assert!(
                pair[0].2 < pair[1].1,
                "windows disjoint: {:?} then {:?}",
                pair[0],
                pair[1]
            );
            assert_eq!(pair[1].1, pair[0].2 + 1, "windows contiguous (no gap)");
        }
    }

    #[tokio::test]
    async fn test_resolve_windows_none_is_all_raw() {
        let engine = engine_with_5m_tier().await;
        const DAY_NS: i64 = 86_400_000_000_000;
        let w =
            resolve_metric_windows(&engine, 0, 2 * DAY_NS, 300_000_000_000, Capability::None, 2 * DAY_NS);
        assert_eq!(w, vec![("metrics".to_string(), 0, 2 * DAY_NS)]);
    }

    #[tokio::test]
    async fn test_resolve_windows_splits_sealed_and_trailing() {
        let engine = engine_with_5m_tier().await;
        const DAY_NS: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;
        let start = 0i64;
        let end = 2 * DAY_NS;
        let sealed = end - DAY_NS;
        // 2-day span, 5m resolution, non-None capability, metrics_5m present.
        // `now_ns = end` reproduces the historical `end - 1day` boundary.
        let w = resolve_metric_windows(&engine, start, end, M5, Capability::MinMax, end);
        assert_eq!(
            w,
            vec![
                ("metrics_5m".to_string(), start, sealed),
                ("metrics".to_string(), sealed + 1, end),
            ]
        );
        assert_disjoint_covering(&w, start, end);
    }

    #[tokio::test]
    async fn test_resolve_windows_short_span_all_live_is_raw() {
        // A span shorter than one day ends entirely inside the trailing live
        // window (`start_ns > sealed_ns`), so even a tier-eligible capability +
        // coarse resolution resolves to a single raw window — the sealed/live
        // early-return branch.
        let engine = engine_with_5m_tier().await;
        const M5: i64 = 300_000_000_000;
        let end = 10 * SEALED_OFFSET_NS; // arbitrary; span below is < 1 day
        let start = end - SEALED_OFFSET_NS / 2; // half a day → fully live
        let w = resolve_metric_windows(&engine, start, end, M5, Capability::MinMax, end);
        assert_eq!(w, vec![("metrics".to_string(), start, end)]);
        assert_disjoint_covering(&w, start, end);
    }

    #[tokio::test]
    async fn test_resolve_windows_fine_resolution_no_tier() {
        let engine = engine_with_5m_tier().await;
        const DAY_NS: i64 = 86_400_000_000_000;
        const M1: i64 = 60_000_000_000;
        // 1m resolution: no tier resolution ≤ 1m → single raw window even for a
        // non-None capability.
        let w = resolve_metric_windows(&engine, 0, 2 * DAY_NS, M1, Capability::SumCount, 2 * DAY_NS);
        assert_eq!(w, vec![("metrics".to_string(), 0, 2 * DAY_NS)]);
    }

    #[tokio::test]
    async fn test_resolve_windows_historical_end_uses_tier() {
        // Regression (live Sol↔Mimir): a historical dashboard view whose `start`
        // AND `end` are BOTH well in the past — over a day that sealed long ago.
        // The sealed/live boundary is wall-clock-relative (`now - 1day`), NOT
        // `end - 1day`, so the long-sealed span must route to `metrics_5m`. With
        // the old `end - 1day` logic the last day before `end` looked "live" and
        // the whole query went raw — the ~6× slowdown this fix removes.
        let engine = engine_with_5m_tier().await;
        const DAY_NS: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;
        let now_ns = 100 * DAY_NS; // wall-clock now
        let start = now_ns - 50 * DAY_NS / 24; // now - 50h
        let end = now_ns - 30 * DAY_NS / 24; // now - 30h (fully sealed: < now - 1d)
        let w = resolve_metric_windows(&engine, start, end, M5, Capability::MinMax, now_ns);
        assert_eq!(
            w,
            vec![("metrics_5m".to_string(), start, end)],
            "a fully-sealed historical span must route to the tier, not raw: {w:?}"
        );
        assert_disjoint_covering(&w, start, end);
        // Sanity: the OLD `end - 1day` boundary would have made this all-raw,
        // because `start (now-50h) > end - 1day (now-54h)`.
        let old_sealed = end - DAY_NS;
        assert!(start > old_sealed, "precondition: old end-relative logic went all-raw");
    }

    #[tokio::test]
    async fn test_resolve_windows_now_relative_boundary() {
        // The sealed/live split is at `now_ns - 1day`, independent of `end_ns`:
        // for the SAME `[start, end]`, two different `now_ns` give two different
        // sealed boundaries (and so different tier/raw splits).
        let engine = engine_with_5m_tier().await;
        const DAY_NS: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;
        let start = 0i64;
        let end = 3 * DAY_NS;
        // now == end: sealed boundary at end - 1day = 2*DAY_NS.
        let w_a = resolve_metric_windows(&engine, start, end, M5, Capability::MinMax, end);
        assert_eq!(
            w_a,
            vec![
                ("metrics_5m".to_string(), start, 2 * DAY_NS),
                ("metrics".to_string(), 2 * DAY_NS + 1, end),
            ],
            "now == end → sealed at end - 1day: {w_a:?}"
        );
        // now far in the future: the whole span is sealed → single tier window.
        let now_future = 10 * DAY_NS;
        let w_b = resolve_metric_windows(&engine, start, end, M5, Capability::MinMax, now_future);
        assert_eq!(
            w_b,
            vec![("metrics_5m".to_string(), start, end)],
            "now ≫ end → whole span sealed, single tier window: {w_b:?}"
        );
        assert_ne!(w_a, w_b, "the split must depend on now_ns, not end_ns");
    }

    // --- Task 3: capability-aware value selection on a tier (FR7) ---

    const DAY_NS: i64 = 86_400_000_000_000;
    const M5: i64 = 300_000_000_000;

    /// Build an engine whose **raw** `metrics` carries multi-sample 5m buckets on
    /// the sealed day 0 (the per-bucket peak is *not* the last sample, so a tier
    /// that only kept `last` would drop it) plus a live-day-1 sample, and whose
    /// `rollup-5m.parquet` tier carries the per-bucket `{last, min, max, sum,
    /// count}` aggregates. The tier's `last` (`double_value`) is deliberately a
    /// *wrong* value for max/avg — only `value_max`/`value_sum`/`value_count`
    /// match raw — so a test that reads the tier and matches raw proves FR7 (the
    /// per-op aggregate column, not a recompute over `last`, is used).
    async fn engine_with_rich_5m_tier() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        // dt=1970-01-01: matches the epoch-relative timestamps (the rollup
        // file's FR1 pruning interval derives from the `dt=` day).
        let dir = tmp.path().join("metrics").join("gauge").join("dt=1970-01-01");
        std::fs::create_dir_all(&dir).unwrap();

        // Raw schema (no value_* cols — the catalog adapter nulls them for raw).
        let raw_schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            crate::querier::udf::tests::attributes_map_field(),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        // Day-0 (sealed) bucket [0, 5m): samples 10, 99, 20 — peak 99 is NOT last.
        // Day-1 (live) sample at DAY+1m: 5.
        let n = 4usize;
        let raw = RecordBatch::try_new(
            Arc::clone(&raw_schema),
            vec![
                Arc::new(StringArray::from(vec!["s"; n])),
                Arc::new(StringArray::from(vec!["g"; n])),
                crate::querier::udf::tests::json_map_array(&vec![r#"{"sc":"a"}"#; n]),
                Arc::new(
                    TimestampNanosecondArray::from(vec![
                        60_000_000_000i64,  // t=1m  (bucket 0)
                        120_000_000_000,    // t=2m  (bucket 0)
                        180_000_000_000,    // t=3m  (bucket 0)
                        DAY_NS + 60_000_000_000, // live day 1
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(Float64Array::from(vec![10.0, 99.0, 20.0, 5.0])),
                Arc::new(StringArray::from(vec!["g"; n])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("m.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, raw_schema, None).unwrap();
        w.write(&raw).unwrap();
        w.close().unwrap();

        // Tier schema = raw + the four value_* aggregate columns (nullable).
        let tier_schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            crate::querier::udf::tests::attributes_map_field(),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
            Field::new("value_min", DataType::Float64, true),
            Field::new("value_max", DataType::Float64, true),
            Field::new("value_sum", DataType::Float64, true),
            Field::new("value_count", DataType::Float64, true),
        ]));
        // One sealed bucket row at t=3m. `double_value` (last) = 20 — a wrong value
        // for both max (99) and avg (129/3=43). value_* carry the truth.
        let tier = RecordBatch::try_new(
            Arc::clone(&tier_schema),
            vec![
                Arc::new(StringArray::from(vec!["s"])),
                Arc::new(StringArray::from(vec!["g"])),
                crate::querier::udf::tests::json_map_array(&[r#"{"sc":"a"}"#]),
                Arc::new(
                    TimestampNanosecondArray::from(vec![180_000_000_000i64]).with_timezone("UTC"),
                ),
                Arc::new(Float64Array::from(vec![20.0])),
                Arc::new(StringArray::from(vec!["g"])),
                Arc::new(Float64Array::from(vec![10.0])), // value_min
                Arc::new(Float64Array::from(vec![99.0])), // value_max
                Arc::new(Float64Array::from(vec![129.0])), // value_sum (10+99+20)
                Arc::new(Float64Array::from(vec![3.0])),  // value_count
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("rollup-5m.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, tier_schema, None).unwrap();
        w.write(&tier).unwrap();
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

    /// The first point's value for the (single-series) matrix response, parsed.
    fn first_point_value(resp: &PromMatrixResponse) -> f64 {
        assert_eq!(resp.data.result.len(), 1, "one series: {:?}", resp.data.result);
        resp.data.result[0]
            .values
            .first()
            .and_then(|(_, v)| v.parse::<f64>().ok())
            .expect("a numeric first point")
    }

    #[tokio::test]
    async fn test_range_max_over_time_uses_tier_and_matches_raw() {
        // `max(max_over_time(g[5m]))` over a sealed 2-day span at 5m step routes the
        // sealed day to the tier. The tier's per-bucket `value_max`=99 (the raw
        // peak) is used — NOT the last-valued `double_value`=20 a recompute would
        // see — so the result equals the raw max (99). Proves FR7 + tier routing.
        let engine = engine_with_rich_5m_tier().await;
        let resp = handle_range(&engine, "max(max_over_time(g[5m]))", 0, 2 * DAY_NS, M5, 2 * DAY_NS)
            .await
            .unwrap();
        let v = first_point_value(&resp);
        assert!(
            (v - 99.0).abs() < 1e-6,
            "max_over_time must use the tier's value_max (99) and match raw, got {v}"
        );
    }

    #[tokio::test]
    async fn test_range_min_over_time_uses_tier_and_matches_raw() {
        // `min(min_over_time(g[5m]))` over the sealed span routes to the tier and
        // reads `value_min`=10 (the raw bucket min) — NOT value_max(99)/last(20).
        let engine = engine_with_rich_5m_tier().await;
        let cap = op_capability(&parser::parse("min_over_time(g[5m])").unwrap());
        let w = resolve_metric_windows(&engine, 0, 2 * DAY_NS, M5, cap, 2 * DAY_NS);
        assert_eq!(w.first().unwrap().0, "metrics_5m", "sealed window → tier: {w:?}");
        let resp = handle_range(&engine, "min(min_over_time(g[5m]))", 0, 2 * DAY_NS, M5, 2 * DAY_NS)
            .await
            .unwrap();
        let v = first_point_value(&resp);
        assert!(
            (v - 10.0).abs() < 1e-6,
            "min_over_time must use the tier's value_min (10) and match raw, got {v}"
        );
    }

    #[tokio::test]
    async fn test_range_sum_over_time_uses_tier_and_matches_raw() {
        // `sum_over_time(g[5m])` over the tier = SUM(value_sum) = 129 — the raw sum
        // of the bucket samples (10+99+20) — NOT sum(last)=20.
        let engine = engine_with_rich_5m_tier().await;
        let cap = op_capability(&parser::parse("sum_over_time(g[5m])").unwrap());
        let w = resolve_metric_windows(&engine, 0, 2 * DAY_NS, M5, cap, 2 * DAY_NS);
        assert_eq!(w.first().unwrap().0, "metrics_5m", "sealed window → tier: {w:?}");
        let resp = handle_range(&engine, "sum(sum_over_time(g[5m]))", 0, 2 * DAY_NS, M5, 2 * DAY_NS)
            .await
            .unwrap();
        let v = first_point_value(&resp);
        assert!(
            (v - 129.0).abs() < 1e-6,
            "sum_over_time must be SUM(value_sum) = 129 and match raw, got {v}"
        );
    }

    #[tokio::test]
    async fn test_range_count_over_time_uses_tier_and_matches_raw() {
        // `count_over_time(g[5m])` over the tier = SUM(value_count) = 3 — the raw
        // sample count of the bucket — NOT a row count (1) of the single tier row.
        let engine = engine_with_rich_5m_tier().await;
        let cap = op_capability(&parser::parse("count_over_time(g[5m])").unwrap());
        let w = resolve_metric_windows(&engine, 0, 2 * DAY_NS, M5, cap, 2 * DAY_NS);
        assert_eq!(w.first().unwrap().0, "metrics_5m", "sealed window → tier: {w:?}");
        let resp = handle_range(&engine, "sum(count_over_time(g[5m]))", 0, 2 * DAY_NS, M5, 2 * DAY_NS)
            .await
            .unwrap();
        let v = first_point_value(&resp);
        assert!(
            (v - 3.0).abs() < 1e-6,
            "count_over_time must be SUM(value_count) = 3 and match raw, got {v}"
        );
    }

    #[tokio::test]
    async fn test_range_avg_over_time_uses_tier_and_matches_raw() {
        // `avg_over_time(g[5m])` over the sealed tier = Σvalue_sum/Σvalue_count =
        // 129/3 = 43 — the raw bucket average — NOT avg(last)=20.
        let engine = engine_with_rich_5m_tier().await;
        let resp = handle_range(&engine, "avg(avg_over_time(g[5m]))", 0, 2 * DAY_NS, M5, 2 * DAY_NS)
            .await
            .unwrap();
        let v = first_point_value(&resp);
        assert!(
            (v - 43.0).abs() < 1e-6,
            "avg_over_time must be Σvalue_sum/Σvalue_count = 43, got {v}"
        );
    }

    #[tokio::test]
    async fn test_range_rate_still_uses_tier() {
        // The existing rate routing stays green: a coarse-step rate over a sealed
        // window reads the tier (Capability::Last) — exercised end-to-end here.
        let engine = engine_with_rich_5m_tier().await;
        let resp = handle_range(&engine, "rate(g[5m])", 0, 2 * DAY_NS, M5, 2 * DAY_NS).await;
        // It must succeed (routing path intact) and yield a series from the tier.
        assert!(resp.is_ok(), "rate over a sealed window must route + evaluate");
    }

    #[tokio::test]
    async fn test_range_binary_rate_ratio_uses_tier() {
        // `rate(a[5m])/rate(b[5m])`: both operands are Capability::Last, so the
        // binary combine is Last and the whole query routes to the tier.
        let expr = parser::parse("rate(g[5m])/rate(g[5m])").unwrap();
        assert_eq!(op_capability(&expr), Capability::Last);
        let engine = engine_with_rich_5m_tier().await;
        let w = resolve_metric_windows(&engine, 0, 2 * DAY_NS, M5, op_capability(&expr), 2 * DAY_NS);
        assert_eq!(w.first().unwrap().0, "metrics_5m", "sealed window → tier: {w:?}");
        // And it evaluates (g/g = 1 on the overlapping series).
        let resp = handle_range(&engine, "rate(g[5m])/rate(g[5m])", 0, 2 * DAY_NS, M5, 2 * DAY_NS).await;
        assert!(resp.is_ok(), "binary rate ratio must route + evaluate");
    }

    #[test]
    fn test_op_capability_scalar_scaled_rate_is_last() {
        // A scalar operand (no selector) is capability-neutral — unit-scaling and
        // threshold panels over a rate stay tier-eligible (inherit Last).
        let cap = |q: &str| op_capability(&parser::parse(q).unwrap());
        assert_eq!(cap("rate(m[5m]) * 2"), Capability::Last);
        assert_eq!(cap("2 * rate(m[5m])"), Capability::Last);
        assert_eq!(cap("rate(m[5m]) / 1024"), Capability::Last);
        assert_eq!(cap("rate(m[5m]) > 5"), Capability::Last);
        // max_over_time scaled by a scalar keeps MinMax (the metric operand wins).
        assert_eq!(cap("max_over_time(m[5m]) / 100"), Capability::MinMax);
        // Two scalars (no metric) → None (never reaches the range tier path).
        assert_eq!(cap("1 + 2"), Capability::None);
    }

    /// The single instant sample's value for a one-series vector response.
    fn instant_value(resp: &PromResponse) -> f64 {
        assert_eq!(resp.data.result.len(), 1, "one series: {:?}", resp.data.result);
        resp.data.result[0]
            .value
            .1
            .parse::<f64>()
            .expect("a numeric instant value")
    }

    #[tokio::test]
    async fn test_instant_rate_long_window_uses_tier() {
        // An instant `sum(rate(g[3d]))` at t=2d: the selector window
        // [2d-3d, 2d] covers the sealed day-0 span, so the resolver (resolution =
        // the 3d selector window, Capability::Last) routes the sealed portion to
        // the 5m tier. It must route + evaluate.
        let engine = engine_with_rich_5m_tier().await;
        let range = parser::parse("sum(rate(g[3d]))").unwrap();
        let w =
            resolve_metric_windows(&engine, -DAY_NS, 2 * DAY_NS, 3 * DAY_NS, op_capability(&range), 2 * DAY_NS);
        assert_eq!(w.first().unwrap().0, "metrics_5m", "sealed window → tier: {w:?}");
        let resp = handle_instant(&engine, "sum(rate(g[3d]))", 2 * DAY_NS, i64::MAX).await;
        assert!(resp.is_ok(), "instant rate over a sealed window must route + evaluate");
    }

    #[tokio::test]
    async fn test_instant_max_over_time_uses_tier_and_matches_raw() {
        // Instant `max_over_time(g[3d])` at t=2d over the sealed span uses the
        // tier's per-bucket `value_max`=99 (the raw peak), NOT the last-valued
        // `double_value`=20 a recompute would see. The live tail sample is 5, so
        // the overall max equals the raw max (99) — peak preserved (FR7).
        let engine = engine_with_rich_5m_tier().await;
        let resp = handle_instant(&engine, "max_over_time(g[3d])", 2 * DAY_NS, i64::MAX)
            .await
            .unwrap();
        let v = instant_value(&resp);
        assert!(
            (v - 99.0).abs() < 1e-6,
            "instant max_over_time must use the tier's value_max (99) and match raw, got {v}"
        );
    }

    #[tokio::test]
    async fn test_instant_bare_selector_reads_raw() {
        // A bare instant vector `g` at t=2d: no matrix window (capability None) ⇒
        // resolves to a single raw window. The latest sample at/before 2d is the
        // live tail value 5.
        let engine = engine_with_rich_5m_tier().await;
        let bare = parser::parse("g").unwrap();
        assert_eq!(op_capability(&bare), Capability::None);
        let resp = handle_instant(&engine, "g", 2 * DAY_NS, i64::MAX).await.unwrap();
        let v = instant_value(&resp);
        assert!((v - 5.0).abs() < 1e-6, "bare selector reads raw latest = 5, got {v}");
    }

    #[test]
    fn test_instant_anchor() {
        // The sentinel i64::MAX ("latest") resolves to wall-clock now; a finite
        // explicit time passes through unchanged.
        assert_eq!(instant_anchor(i64::MAX, 1000), 1000);
        assert_eq!(instant_anchor(500, 1000), 500);
    }

    #[tokio::test]
    async fn test_instant_range_fn_with_omitted_time_anchors_to_now() {
        // REGRESSION (Sol↔Mimir parity): an omitted `time` arrives as i64::MAX.
        // A range-function instant builds a `[T-range, T]` window — with T=i64::MAX
        // that lands in the year 2262 (past all data) → empty. Anchoring T to a real
        // `now_ns` puts the window over real samples. The rich fixture's newest raw
        // sample is the live tail value 5 at DAY+1m; with now=DAY+1m, `m[5m]` covers
        // it → non-empty value 5. The old code returned empty here.
        let engine = engine_with_rich_5m_tier().await;
        let now = DAY_NS + 60_000_000_000; // the live tail sample's timestamp
        // max_over_time over the anchored 5m window.
        let resp = handle_instant(&engine, "max_over_time(g[5m])", i64::MAX, now)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1, "omitted-time must be non-empty: {:?}", resp.data.result);
        let v = instant_value(&resp);
        assert!((v - 5.0).abs() < 1e-6, "max over [now-5m, now] is the tail sample 5, got {v}");
        // rate over the anchored window must route + evaluate (it lands on real data,
        // not the year 2262); the fixture has a single sample in this 5m window so the
        // slope is empty — the point is that it no longer scans an empty future range.
        let rate = handle_instant(&engine, "rate(g[5m])", i64::MAX, now).await;
        assert!(rate.is_ok(), "omitted-time rate must route + evaluate over real data");
        // Sanity: with the un-anchored sentinel (now=i64::MAX), the window is in 2262
        // and the result is empty — the very regression the anchor fixes.
        let empty = handle_instant(&engine, "max_over_time(g[5m])", i64::MAX, i64::MAX)
            .await
            .unwrap();
        assert!(empty.data.result.is_empty(), "sentinel-now window is past all data → empty");
    }

    /// A monotonic counter sampled every 15s over 10m (41 samples on a single day),
    /// value = 100·k so the increment is 100 per 15s (rate = 100/15 ≈ 6.667/s). All
    /// samples are on the active (unsealed) day, so the resolver returns one raw
    /// window — isolating the rate/increase logic from tier routing.
    async fn monotonic_counter_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let n = 41usize; // 0..40 → 10m at 15s
        #[allow(clippy::cast_possible_wrap)] // small test-fixture index
        let times: Vec<i64> = (0..n).map(|k| (k as i64) * 15_000_000_000).collect();
        #[allow(clippy::cast_precision_loss)] // small test-fixture index
        let vals: Vec<f64> = (0..n).map(|k| 100.0 * k as f64).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["client"; n])),
                Arc::new(StringArray::from(vec!["c"; n])),
                Arc::new(TimestampNanosecondArray::from(times).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&vec!["{}"; n]),
                Arc::new(Float64Array::from(vals)),
                Arc::new(StringArray::from(vec!["c"; n])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("c.parquet")).unwrap();
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

    /// A steady counter (`c`, service=client) sampled every 5m over ~30h so it
    /// straddles a UTC midnight — the range path's per-day `frontend::split`
    /// produces ≥2 shards. `first_ns` is the timestamp of the first sample; each
    /// step adds a constant +300 so the steady-state rate is 300/300s = 1/s. Used
    /// to prove FR2's lookback keeps `rate` continuous across a shard boundary and
    /// emits no duplicate grid timestamps.
    async fn across_midnight_counter_engine(first_ns: i64) -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        const STEP_NS: i64 = 300_000_000_000; // 5m sample interval
        let n = 361usize; // 360 steps × 5m = 30h → crosses one UTC midnight
        #[allow(clippy::cast_possible_wrap)] // small test-fixture index
        let times: Vec<i64> = (0..n).map(|k| first_ns + (k as i64) * STEP_NS).collect();
        #[allow(clippy::cast_precision_loss)] // small test-fixture index
        let vals: Vec<f64> = (0..n).map(|k| 300.0 * k as f64).collect(); // +300/step → 1/s
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["client"; n])),
                Arc::new(StringArray::from(vec!["c"; n])),
                Arc::new(TimestampNanosecondArray::from(times).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&vec!["{}"; n]),
                Arc::new(Float64Array::from(vals)),
                Arc::new(StringArray::from(vec!["c"; n])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("c.parquet")).unwrap();
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
    async fn test_range_rate_no_left_edge_ramp() {
        // FR2: a steady counter starting at t=0; query a range that starts well
        // after the series start (T=300s ≫ 0) at a 15s step. The FIRST grid point's
        // rate must ≈ a mid-range point (steady state), not ramp up from ~0. Before
        // the lookback fix the range path scanned only `[300s, 600s]`, so the 300s
        // window had no earlier samples → a low, ramping value at the left edge.
        let engine = monotonic_counter_engine().await; // 100/15s → ~6.667/s
        const S15: i64 = 15_000_000_000;
        let start = 300_000_000_000i64; // 300s ≫ series start (0)
        let end = 600_000_000_000i64;
        let resp = handle_range(&engine, "rate(c[5m])", start, end, S15, end)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1, "one series: {:?}", resp.data.result);
        let vals: Vec<f64> = resp.data.result[0]
            .values
            .iter()
            .map(|(_, v)| v.parse::<f64>().unwrap())
            .collect();
        assert!(vals.len() >= 3, "several grid points: {vals:?}");
        let first = vals[0];
        let mid = vals[vals.len() / 2];
        assert!(
            (first - mid).abs() < 0.05 * mid,
            "first grid point ({first}) must be steady-state, not a left-edge ramp \
             (mid-range = {mid}); values = {vals:?}"
        );
    }

    #[tokio::test]
    async fn test_range_rate_continuous_across_shard_boundary() {
        // FR2: a range spanning a UTC midnight → `frontend::split` yields ≥2 shards.
        // The steady counter's rate (1/s) must be continuous across the boundary:
        // no dip at the first grid point of the second shard (which, pre-fix,
        // scanned only from midnight and had a truncated window).
        const DAY: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;
        // Series first sample 6h before a UTC midnight; query straddles midnight
        // over >1 day so the split fires.
        let midnight = 1_780_704_000_000_000_000i64; // 2026-05-30 00:00 UTC (÷ DAY == 0)
        let first_ns = midnight - 6 * 3_600_000_000_000; // 18:00 the previous day
        let engine = across_midnight_counter_engine(first_ns).await;
        let start = midnight - 3_600_000_000_000; // 23:00 prev day
        let end = start + DAY + 2 * 3_600_000_000_000; // > 1 day → forces the split
        let resp = handle_range(&engine, "rate(c[15m])", start, end, M5, end)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1, "one series: {:?}", resp.data.result);
        let pts = &resp.data.result[0].values;
        // Find the pair of adjacent grid points straddling midnight and assert the
        // rate barely changes (a boundary dip would be a sharp drop).
        #[allow(clippy::cast_precision_loss)] // ns→s for the test boundary timestamp
        let boundary_s = midnight as f64 / 1e9;
        let mut checked = false;
        for w in pts.windows(2) {
            let (t0, v0) = (w[0].0, w[0].1.parse::<f64>().unwrap());
            let (t1, v1) = (w[1].0, w[1].1.parse::<f64>().unwrap());
            if t0 < boundary_s && t1 >= boundary_s {
                assert!(
                    (v1 - v0).abs() < 0.05 * v0.max(1e-9),
                    "rate dips across the midnight shard boundary: {v0} → {v1}"
                );
                checked = true;
            }
        }
        assert!(checked, "expected a grid-point pair straddling midnight: {pts:?}");
    }

    #[tokio::test]
    async fn test_range_rate_no_duplicate_timestamps() {
        // FR2: the lookback region `[query_start, shard.start)` is scanned only to
        // seed the window/LAG — it must NOT be emitted, so grid timestamps stay
        // strictly increasing with no duplicates across shard boundaries.
        const DAY: i64 = 86_400_000_000_000;
        const M5: i64 = 300_000_000_000;
        let midnight = 1_780_704_000_000_000_000i64;
        let first_ns = midnight - 6 * 3_600_000_000_000;
        let engine = across_midnight_counter_engine(first_ns).await;
        let start = midnight - 3_600_000_000_000;
        let end = start + DAY + 2 * 3_600_000_000_000; // >1 day → ≥2 shards
        let resp = handle_range(&engine, "rate(c[15m])", start, end, M5, end)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1, "one series: {:?}", resp.data.result);
        let ts: Vec<f64> = resp.data.result[0].values.iter().map(|(t, _)| *t).collect();
        for w in ts.windows(2) {
            assert!(
                w[1] > w[0],
                "timestamps must be strictly increasing (no lookback double-emit): \
                 {:?}",
                ts
            );
        }
    }

    #[tokio::test]
    async fn test_rate_matches_prometheus_golden() {
        // GOLDEN PARITY ANCHOR (Sol↔Mimir): pin the range `rate` to the analytic
        // Prometheus `extrapolatedRate` (promql/functions.go) on a known steady
        // counter, asserted within 1e-6. This is the durable regression guard that
        // fixes Sol's rate math to Prometheus semantics.
        //
        // Fixture `monotonic_counter_engine`: perfectly steady counter, sample k at
        // t_k = 15k s, v_k = 100k (k = 0..40) → true slope 100/15 = 6.6667/s. We
        // evaluate `rate(c[5m])` at the grid point T = 300s (a 5m window, positioned
        // so its edges land exactly on sample boundaries — no partial-gap effects).
        //
        // Hand-derived extrapolatedRate at T = 300s, window (0, 300]:
        //   • in-window samples: k = 1..20 (t = 15s..300s), cnt = 20 (k=0 at t=0 is
        //     excluded by the half-open lower bound).
        //   • base reset-adjusted increase result = v_last − v_first = v_20 − v_1 =
        //     2000 − 100 = 1900 (each per-sample delta = 100; sum over k=1..20 = 2000,
        //     minus the leading first_delta = 100).
        //   • first_t = 15s, last_t = 300s; sampledInterval = 300 − 15 = 285s;
        //     avg_gap = 285/(20−1) = 15s.
        //   • window_start = last_t − range = 300 − 300 = 0; durationToStart_raw =
        //     first_t − window_start = 15s. durationToEnd = last_t − last_t = 0.
        //   • counter zero-clamp: durationToZero = sampledInterval·(first_value/result)
        //     = 285·(100/1900) = 15s. It is NOT < durationToStart_raw(15) (equal, and
        //     the clamp uses strict `<`), so durationToStart stays 15s. Boundary cap:
        //     15 is not ≥ 1.1·avg_gap (16.5), so no cap.
        //   • factor = (285 + 15 + 0)/285 = 300/285; extrapolated = 1900·300/285 = 2000;
        //     rate = extrapolated / range_s = 2000/300 = 20/3 = 6.6666…/s.
        // The extrapolation recovers the exact true slope (6.6667/s), as Prometheus
        // does for a steady counter whose window spans whole gaps.
        let engine = monotonic_counter_engine().await;
        let t = 300_000_000_000i64; // T = 300s
        let resp = handle_range(&engine, "rate(c[5m])", t, t, M5, t)
            .await
            .unwrap();
        assert_eq!(resp.data.result.len(), 1, "one series: {:?}", resp.data.result);
        let pts = &resp.data.result[0].values;
        let (grid_t, v_str) = pts
            .iter()
            .find(|(gt, _)| (*gt - 300.0).abs() < 1e-9)
            .expect("a grid point at T = 300s");
        assert!((grid_t - 300.0).abs() < 1e-9);
        let v = v_str.parse::<f64>().unwrap();
        let expected = 20.0_f64 / 3.0; // 2000/300 s⁻¹
        assert!(
            (v - expected).abs() < 1e-6,
            "range rate {v} must equal the analytic Prometheus extrapolatedRate \
             {expected} within 1e-6"
        );
    }

    #[tokio::test]
    async fn test_instant_rate_matches_range_rate() {
        // PARITY (Sol↔Mimir): the instant `rate(c[5m])` at T must equal the range
        // path's rate at the same grid point T — the range path is the verified-correct
        // reference (matches Mimir live). The bug was the instant returning ~½ the
        // range value because its base was scanned over exactly `[T-range, T]`, so the
        // window's leading sample had no LAG predecessor and its delta was dropped.
        let engine = monotonic_counter_engine().await;
        let t_last = 600_000_000_000i64;

        // Instant path (omitted time → anchored to T_last).
        let instant = handle_instant(&engine, "rate(c[5m])", i64::MAX, t_last)
            .await
            .unwrap();
        let iv = instant_value(&instant);

        // Range path as the reference: a range query over [0, T_last] at 5m step;
        // its last grid point (at T_last) is the verified-correct rate at T_last.
        let range = handle_range(&engine, "rate(c[5m])", 0, t_last, M5, t_last)
            .await
            .unwrap();
        assert_eq!(range.data.result.len(), 1, "one range series: {:?}", range.data.result);
        let rv = range.data.result[0]
            .values
            .last()
            .and_then(|(_, v)| v.parse::<f64>().ok())
            .expect("a numeric range rate at T_last");

        assert!(
            (iv - rv).abs() < 1e-6,
            "instant rate ({iv}) must equal the range rate ({rv}) at the same instant"
        );
    }

    #[tokio::test]
    async fn test_instant_increase_matches_range_increase() {
        // Same parity for `increase` (rate without the /window): instant must equal
        // the range path's increase at T_last.
        let engine = monotonic_counter_engine().await;
        let t_last = 600_000_000_000i64;

        let instant = handle_instant(&engine, "increase(c[5m])", i64::MAX, t_last)
            .await
            .unwrap();
        let iv = instant_value(&instant);

        let range = handle_range(&engine, "increase(c[5m])", 0, t_last, M5, t_last)
            .await
            .unwrap();
        let rv = range.data.result[0]
            .values
            .last()
            .and_then(|(_, v)| v.parse::<f64>().ok())
            .expect("a numeric range increase at T_last");

        assert!(
            (iv - rv).abs() < 1e-6,
            "instant increase ({iv}) must equal the range increase ({rv})"
        );
    }

    /// Three series (svc=a/b/c) of `m`, each a monotonic counter sampled every 15s
    /// over ~10m but **staggered** by 0/5/10s so their last samples land on DIFFERENT
    /// timestamps (a→600s, b→605s, c→610s). Distinct per-step increments (100/200/300)
    /// give distinct per-series rates, so a dropped series visibly changes any
    /// cross-series aggregate. All on the active (unsealed) day → one raw window.
    async fn offset_multiseries_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let n = 41usize; // 0..40 → 10m at 15s, per series
        let mut svc: Vec<&str> = Vec::new();
        let mut times: Vec<i64> = Vec::new();
        let mut vals: Vec<f64> = Vec::new();
        for (s, offset_s, inc) in [("a", 0i64, 100.0), ("b", 5, 200.0), ("c", 10, 300.0)] {
            for k in 0..n {
                svc.push(s);
                #[allow(clippy::cast_possible_wrap)] // small test-fixture index
                times.push((k as i64) * 15_000_000_000 + offset_s * 1_000_000_000);
                #[allow(clippy::cast_precision_loss)] // small test-fixture index
                vals.push(inc * k as f64);
            }
        }
        let total = svc.len();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(svc)),
                Arc::new(StringArray::from(vec!["m"; total])),
                Arc::new(TimestampNanosecondArray::from(times).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&vec!["{}"; total]),
                Arc::new(Float64Array::from(vals)),
                Arc::new(StringArray::from(vec!["m"; total])),
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
        crate::querier::QueryEngine::new(&opts).await.unwrap()
    }

    /// The single-series last-grid-point value of a range response (for a query that
    /// collapses to one output series).
    fn range_last_value(resp: &PromMatrixResponse) -> f64 {
        assert_eq!(resp.data.result.len(), 1, "one range series: {:?}", resp.data.result);
        resp.data.result[0]
            .values
            .last()
            .and_then(|(_, v)| v.parse::<f64>().ok())
            .expect("a numeric last range point")
    }

    // The global anchor across the staggered fixture (svc=c's last sample at 610s).
    const OFFSET_ANCHOR_NS: i64 = 600_000_000_000 + 10_000_000_000;

    #[tokio::test]
    async fn test_instant_sum_rate_matches_range_multiseries() {
        // THE failing case: `sum(rate(m[5m]))` instant must sum ALL three series.
        // The bug grouped by (key, time) then picked the global-latest timestamp,
        // dropping the series whose last sample wasn't on that max timestamp. Compare
        // against the range path's last grid point at the anchor (verified reference).
        let engine = offset_multiseries_engine().await;
        let instant = handle_instant(&engine, "sum(rate(m[5m]))", i64::MAX, OFFSET_ANCHOR_NS)
            .await
            .unwrap();
        let iv = instant_value(&instant);
        // Range reference evaluated AT the anchor: start so the step grid's last
        // point lands exactly on the anchor (610s − 2·5m = 10s ⇒ grid 10s,310s,610s).
        let range = handle_range(&engine, "sum(rate(m[5m]))", OFFSET_ANCHOR_NS - 2 * M5, OFFSET_ANCHOR_NS, M5, OFFSET_ANCHOR_NS)
            .await
            .unwrap();
        let rv = range_last_value(&range);
        // Sanity: the sum must include all three series — far above any single one.
        assert!(iv > 20.0, "sum must include all 3 staggered series, got {iv}");
        assert!(
            (iv - rv).abs() < 1e-6,
            "instant sum(rate) ({iv}) must equal range sum(rate) at the anchor ({rv})"
        );
    }

    #[tokio::test]
    async fn test_instant_sum_rate_by_label_matches_range() {
        // `sum by(svc)(rate(m[5m]))`: one output series per svc, each must equal the
        // range path's per-svc last grid point. (Here `service_name` is the grouping
        // label; the query groups on it.)
        let engine = offset_multiseries_engine().await;
        let instant =
            handle_instant(&engine, "sum by(service_name)(rate(m[5m]))", i64::MAX, OFFSET_ANCHOR_NS)
                .await
                .unwrap();
        let range =
            handle_range(&engine, "sum by(service_name)(rate(m[5m]))", OFFSET_ANCHOR_NS - 2 * M5, OFFSET_ANCHOR_NS, M5, OFFSET_ANCHOR_NS)
                .await
                .unwrap();
        assert_eq!(instant.data.result.len(), 3, "three svc groups: {:?}", instant.data.result);
        // Map each path's results by svc label, then compare per group.
        let by_svc = |label_get: &dyn Fn(usize) -> Option<String>, val: &dyn Fn(usize) -> f64, len: usize| {
            let mut m = std::collections::BTreeMap::new();
            for i in 0..len {
                if let Some(s) = label_get(i) {
                    m.insert(s, val(i));
                }
            }
            m
        };
        let iv = by_svc(
            &|i| instant.data.result[i].metric.get("service_name").cloned(),
            &|i| instant.data.result[i].value.1.parse::<f64>().unwrap(),
            instant.data.result.len(),
        );
        let rv = by_svc(
            &|i| range.data.result[i].metric.get("service_name").cloned(),
            &|i| range.data.result[i].values.last().unwrap().1.parse::<f64>().unwrap(),
            range.data.result.len(),
        );
        for (svc, v) in &iv {
            let r = rv.get(svc).copied().unwrap_or(f64::NAN);
            assert!(
                (v - r).abs() < 1e-6,
                "svc {svc}: instant {v} must equal range {r}"
            );
        }
    }

    #[tokio::test]
    async fn test_instant_avg_over_time_aggregate_matches_range() {
        // `avg(avg_over_time(m[5m]))` over the three staggered series: every series
        // must contribute its anchor-window mean. Window [310s, 610s] at the anchor:
        //   svc=a (inc 100): samples 315..600 → mean 3050
        //   svc=b (inc 200): samples 320..605 → mean 6100
        //   svc=c (inc 300): samples 310..610 → mean 9000
        // avg across the three = (3050+6100+9000)/3 = 6050.
        // (We assert the analytic value rather than range@T here: the range path
        // resamples each series to the step grid by carrying its *last sample's*
        // window value forward, so at a sub-step-offset anchor it evaluates svc=a/b
        // over a slightly different window than the exact-anchor instant — a query-type
        // difference, not a bug. The analytic value proves all 3 series contribute and
        // the instant value is exact, which is the regression this matrix locks down.)
        let engine = offset_multiseries_engine().await;
        let instant =
            handle_instant(&engine, "avg(avg_over_time(m[5m]))", i64::MAX, OFFSET_ANCHOR_NS)
                .await
                .unwrap();
        let iv = instant_value(&instant);
        assert!(
            (iv - 6050.0).abs() < 1e-6,
            "instant avg(avg_over_time) must be (3050+6100+9000)/3 = 6050 (all 3 series), got {iv}"
        );
    }

    #[tokio::test]
    async fn test_instant_max_over_time_aggregate_matches_range() {
        // `max(max_over_time(m[5m]))` multi-series: instant == range@anchor. The max
        // is dominated by svc=c (largest values); a dropped series would still need
        // the right global max — compare against the range reference.
        let engine = offset_multiseries_engine().await;
        let instant =
            handle_instant(&engine, "max(max_over_time(m[5m]))", i64::MAX, OFFSET_ANCHOR_NS)
                .await
                .unwrap();
        let iv = instant_value(&instant);
        let range = handle_range(&engine, "max(max_over_time(m[5m]))", OFFSET_ANCHOR_NS - 2 * M5, OFFSET_ANCHOR_NS, M5, OFFSET_ANCHOR_NS)
            .await
            .unwrap();
        let rv = range_last_value(&range);
        assert!(
            (iv - rv).abs() < 1e-6,
            "instant max(max_over_time) ({iv}) must equal range ({rv})"
        );
    }

    #[test]
    fn test_op_capability_binary_mixed_is_none() {
        // `max_over_time(a)/rate(b)`: operands need different value columns
        // (value_max vs last) → combine is None → forced raw.
        let expr = parser::parse("max_over_time(g[5m])/rate(g[5m])").unwrap();
        assert_eq!(op_capability(&expr), Capability::None);
        // A unary negation carries its operand's capability through.
        let neg = parser::parse("-rate(g[5m])").unwrap();
        assert_eq!(op_capability(&neg), Capability::Last);
    }

    // --- Task 6: metadata tier routing (FR5) ---

    #[tokio::test]
    async fn test_series_enumeration_matches_raw_via_tier() {
        // `/series` over a sealed 2-day span routes the sealed day to `metrics_5m`
        // (resolution = i64::MAX → coarsest tier, capability Last) and the trailing
        // live day to raw. The rollup tier carries the same `(name, service_name)`
        // series as raw for the sealed window, so the routed union MUST equal the
        // raw-only enumeration (no-range fallback → unfiltered raw `metrics`).
        let engine = engine_with_rich_5m_tier().await;
        // Routed (explicit range): sealed day-0 → tier, trailing day-1 → raw.
        let w = resolve_metric_windows(&engine, 0, 2 * DAY_NS, i64::MAX, Capability::Last, 2 * DAY_NS);
        assert_eq!(w.first().unwrap().0, "metrics_5m", "sealed window → tier: {w:?}");
        let routed = handle_series(&engine, None, Some((0, 2 * DAY_NS)), 2 * DAY_NS).await.unwrap();
        // Raw-only baseline: no time range → unfiltered raw `metrics` scan.
        let raw_only = handle_series(&engine, None, None, 2 * DAY_NS).await.unwrap();
        assert_eq!(
            routed["data"], raw_only["data"],
            "tier-routed /series must equal raw-only: routed={routed} raw={raw_only}"
        );
        assert_eq!(
            routed["data"],
            serde_json::json!([{ "__name__": "g", "service_name": "s" }]),
            "the fixture's single series: {routed}"
        );
    }

    #[tokio::test]
    async fn test_label_values_matches_raw_via_tier() {
        // `/label/__name__/values` over a sealed 2-day span routes the sealed day
        // to `metrics_5m` (coarsest tier, capability Last) and the trailing day to
        // raw. The tier preserves the full name set, so the routed value set MUST
        // equal the raw-only enumeration.
        let engine = engine_with_rich_5m_tier().await;
        let w = resolve_metric_windows(&engine, 0, 2 * DAY_NS, i64::MAX, Capability::Last, 2 * DAY_NS);
        assert_eq!(w.first().unwrap().0, "metrics_5m", "sealed window → tier: {w:?}");
        // Routed (explicit range).
        let routed = handle_label_values(&engine, "__name__", 0, 2 * DAY_NS, None, 2 * DAY_NS).await.unwrap();
        // Raw-only baseline: a span starting after the sealed boundary
        // (`start > sealed_ns`) resolves to a single raw `metrics` window (no
        // tier) yet still covers the live day-1 sample at `DAY_NS + 60s`.
        let raw_start = DAY_NS + 30_000_000_000; // just before the live sample
        let raw_only =
            handle_label_values(&engine, "__name__", raw_start, 2 * DAY_NS, None, 2 * DAY_NS).await.unwrap();
        assert_eq!(
            routed["data"], raw_only["data"],
            "tier-routed label values must equal raw-only: routed={routed} raw={raw_only}"
        );
        assert_eq!(routed["data"], serde_json::json!(["g"]), "the fixture's single name: {routed}");
    }

    // --- Task 7: NFR3 no-silent-bypass + NFR1 ---

    /// NFR1 — every operator the classifier maps to [`Capability::None`] must
    /// force a single raw `metrics` window, even over a multi-day sealed span
    /// where a tier is registered. This proves the safe-by-default path: an
    /// unsafe/unlisted operator can never be silently served from a coarse tier.
    ///
    /// Table-driven over `irate`, `quantile_over_time`, `stddev_over_time` (the
    /// documented `None` operators), a bare selector `m`, and a pure scalar
    /// `1+2`. For each we assert (a) `op_capability == None` and (b) the resolver
    /// returns exactly one window on the raw `metrics` table — never a tier — at a
    /// coarse resolution over a 2-day span with `metrics_5m` registered. We test
    /// the *classifier + resolver* (which parse/inspect the `Expr`), not query
    /// execution, so the operators need not be runtime-implemented.
    #[tokio::test]
    async fn test_none_capability_never_tiers() {
        let engine = engine_with_rich_5m_tier().await;
        // Coarse resolution + multi-day sealed span: the conditions under which a
        // non-None capability *would* route to `metrics_5m` (see
        // `test_resolve_windows_splits_sealed_and_trailing`). The point is that
        // None never does, regardless.
        let start = 0i64;
        let end = 2 * DAY_NS;
        for query in ["irate(m[5m])", "quantile_over_time(0.9, m[5m])", "stddev_over_time(m[5m])", "m", "1 + 2"] {
            let expr = parser::parse(query).unwrap();
            let cap = op_capability(&expr);
            assert_eq!(cap, Capability::None, "{query}: must classify as Capability::None");
            let w = resolve_metric_windows(&engine, start, end, i64::MAX, cap, end);
            assert_eq!(
                w,
                vec![("metrics".to_string(), start, end)],
                "{query}: None capability must yield a single raw window, never a tier"
            );
        }
    }

    // --- backend-metrics-perf task 3: per-query file pruning (FR1) ---

    /// Write an exact-bounds-named gauge file (`<min>-<max>-<uuid>.parquet`,
    /// task 1b sink naming) under `metrics/dt=<day>/`, one `m` sample per
    /// timestamp — so the file inventory parses exact per-file intervals and
    /// `table_scoped` can prune it in/out per query window (FR1 fixtures).
    fn write_bounded_gauge(root: &std::path::Path, day: &str, times_ns: &[i64]) {
        use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let (min, max) = (times_ns[0], times_ns[times_ns.len() - 1]);
        let dir = root.join("metrics").join(format!("dt={day}"));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "{min}-{max}-550e8400-e29b-41d4-a716-446655440000.parquet"
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let n = times_ns.len();
        #[allow(clippy::cast_precision_loss)]
        let values: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["svc"; n])),
                Arc::new(StringArray::from(vec!["m"; n])),
                Arc::new(TimestampNanosecondArray::from(times_ns.to_vec()).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&vec!["{}"; n]),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(vec!["m"; n])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    /// Write an exact-bounds-named OTLP array-histogram file (task 14b subtype
    /// layout: `metrics/histogram/dt=<day>/<min>-<max>-<uuid>.parquet`), one
    /// `hist_seconds` sample per timestamp. Per-bucket cumulative counts grow
    /// with `count_base + i` so consecutive files form a monotonic counter —
    /// the `histogram_quantile(φ, sum(rate(<base>_bucket[w])) by (le))` bench
    /// shape reads these via [`hist_source`].
    fn write_bounded_histogram(
        root: &std::path::Path,
        day: &str,
        times_ns: &[i64],
        count_base: u64,
    ) {
        use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let (min, max) = (times_ns[0], times_ns[times_ns.len() - 1]);
        let dir = root.join("metrics").join("histogram").join(format!("dt={day}"));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "{min}-{max}-550e8400-e29b-41d4-a716-446655440001.parquet"
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            crate::querier::udf::tests::attributes_map_field(),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let n = times_ns.len();
        let counts: Vec<String> = (0..n)
            .map(|i| {
                let c = count_base + i as u64;
                format!("[{c},{c},{c},{c},{c},{c}]")
            })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["svc"; n])),
                Arc::new(StringArray::from(vec!["hist"; n])),
                Arc::new(TimestampNanosecondArray::from(times_ns.to_vec()).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&vec!["{}"; n]),
                Arc::new(StringArray::from(counts)),
                Arc::new(StringArray::from(vec!["[10,20,30,40,50]"; n])),
                Arc::new(StringArray::from(vec!["hist_seconds"; n])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    fn pruning_opts(path: &std::path::Path) -> crate::config::querier::QuerierOptions {
        crate::config::querier::QuerierOptions {
            storage: crate::config::querier::StorageConfig {
                path: path.into(),
                url: None,
            },
            ..crate::config::querier::QuerierOptions::default()
        }
    }

    /// Drain the recorder snapshot **once**, returning (a) the per-stage
    /// plan-pipeline seconds (`querier_plan_stage_duration_seconds`, summed per
    /// `stage` label — one sample per executed scan/shard, so sums are
    /// per-interval totals) and (b) the `signal="metrics"` files-opened count,
    /// both from the same drained interval. `Snapshotter::snapshot()` drains
    /// histogram samples, so a single call must serve both readings — calling
    /// [`metrics_files_opened`] first would discard the stage samples.
    fn drain_plan_stages_and_files(
        snap: &metrics_util::debugging::Snapshotter,
    ) -> (BTreeMap<String, f64>, f64) {
        use metrics_util::MetricKind;
        use metrics_util::debugging::DebugValue;
        let mut stages: BTreeMap<String, f64> = BTreeMap::new();
        let mut files = 0.0;
        for (k, _, _, v) in snap.snapshot().into_vec() {
            if k.kind() != MetricKind::Histogram {
                continue;
            }
            let DebugValue::Histogram(samples) = v else {
                continue;
            };
            let sum: f64 = samples.iter().map(|h| h.into_inner()).sum();
            match k.key().name() {
                "querier_plan_stage_duration_seconds" => {
                    if let Some(stage) = k
                        .key()
                        .labels()
                        .find(|l| l.key() == "stage")
                        .map(|l| l.value().to_string())
                    {
                        *stages.entry(stage).or_insert(0.0) += sum;
                    }
                }
                "querier_files_opened"
                    if k.key()
                        .labels()
                        .any(|l| l.key() == "signal" && l.value() == "metrics") =>
                {
                    files += sum;
                }
                _ => {}
            }
        }
        (stages, files)
    }

    /// Profiling-seam sanity (promql-plan-cache task 1, FR1): on a tiny fixture
    /// a cold `rate()` range query records all five pipeline stages
    /// (parse/lower/optimize/physical/execute) with non-zero durations, and the
    /// stage sum never exceeds the end-to-end wall time — the stages nest
    /// inside the request without double-counting. The residual
    /// (merge/resample/cache bookkeeping) is deliberately not bounded here:
    /// on a µs-scale fixture its share is scheduling noise; the ±10 % check
    /// lives in the demo-scale bench table.
    #[test]
    fn test_plan_stage_seam_sums_within_total() {
        use metrics_util::debugging::DebuggingRecorder;

        const MINUTE_NS: i64 = 60_000_000_000;
        const HOUR_NS: i64 = 60 * MINUTE_NS;
        /// 2026-06-01T00:00:00Z (anchored: `date -u -d '2026-06-01' +%s`).
        const JUN01_NS: i64 = 1_780_272_000_000_000_000;

        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        let (stages, total_s) = std::thread::spawn(move || {
            metrics::with_local_recorder(&recorder, || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let tmp = tempfile::tempdir().unwrap();
                    let noon = JUN01_NS + 12 * HOUR_NS;
                    let times: Vec<i64> = (0..30).map(|i| noon + i * MINUTE_NS).collect();
                    write_bounded_gauge(tmp.path(), "2026-06-01", &times);
                    let engine = crate::querier::QueryEngine::new(&pruning_opts(tmp.path()))
                        .await
                        .unwrap();
                    let lo = noon + 10 * MINUTE_NS;
                    let hi = noon + 25 * MINUTE_NS;
                    let t = std::time::Instant::now();
                    let resp = handle_range(&engine, "rate(m[5m])", lo, hi, MINUTE_NS, hi)
                        .await
                        .unwrap();
                    let total_s = t.elapsed().as_secs_f64();
                    assert!(!resp.data.result.is_empty(), "query must return data");
                    let (stages, _files) = drain_plan_stages_and_files(&snap);
                    (stages, total_s)
                })
            })
        })
        .join()
        .unwrap();

        for stage in ["parse", "lower", "optimize", "physical", "execute"] {
            let d = stages.get(stage).copied().unwrap_or(0.0);
            assert!(
                d > 0.0,
                "stage `{stage}` must be recorded with a non-zero duration, got {stages:?}"
            );
        }
        let sum: f64 = stages.values().sum();
        assert!(
            sum <= total_s,
            "stage sum {sum:.6}s must not exceed the end-to-end total {total_s:.6}s \
             (stages nest inside the request): {stages:?}"
        );
    }

    /// Sum of the `querier_files_opened` histogram samples for
    /// `signal="metrics"` in the recorder's current snapshot — how many
    /// Parquet files the executed metric scans have opened so far (0 before
    /// any scan). One sample is recorded per executed scan
    /// (`execute_recording_scan`, 1 file group per file). NOTE:
    /// `Snapshotter::snapshot()` drains the histogram's samples, so every
    /// call returns the count accumulated SINCE THE PREVIOUS snapshot — a
    /// deterministic per-interval proxy for "footers opened".
    fn metrics_files_opened(snap: &metrics_util::debugging::Snapshotter) -> f64 {
        use metrics_util::MetricKind;
        use metrics_util::debugging::DebugValue;
        snap.snapshot()
            .into_vec()
            .into_iter()
            .find_map(|(k, _, _, v)| {
                (k.kind() == MetricKind::Histogram
                    && k.key().name() == "querier_files_opened"
                    && k.key()
                        .labels()
                        .any(|l| l.key() == "signal" && l.value() == "metrics"))
                .then_some(v)
            })
            .map_or(0.0, |v| match v {
                DebugValue::Histogram(samples) => samples.iter().map(|h| h.into_inner()).sum(),
                _ => 0.0,
            })
    }

    /// FR1/NFR1 (task 3): a 15-min range query over a 3-day store opens only
    /// the in-window file. Files-opened is observed via the `DebuggingRecorder`
    /// pattern (`catalog::test_query_records_real_bytes_scanned` precedent):
    /// see [`metrics_files_opened`]. Pre-pruning this scans all 3
    /// files; scoped, exactly the 1 in-window file.
    #[test]
    fn test_range_query_opens_only_window_files() {
        use metrics_util::debugging::DebuggingRecorder;

        const MINUTE_NS: i64 = 60_000_000_000;
        const HOUR_NS: i64 = 60 * MINUTE_NS;
        const DAY_NS: i64 = 24 * HOUR_NS;
        /// 2026-06-01T00:00:00Z (anchored: `date -u -d '2026-06-01' +%s`).
        const JUN01_NS: i64 = 1_780_272_000_000_000_000;

        // `with_local_recorder` installs a thread-local recorder, so run the
        // whole build+query on one dedicated thread whose own current-thread
        // runtime drives the async work.
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        std::thread::spawn(move || {
            metrics::with_local_recorder(&recorder, || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let tmp = tempfile::tempdir().unwrap();
                    // 3 days, one exact-bounds file each: 100 one-minute samples
                    // from noon (enough rows that the scan reports bytes > 0).
                    for (d, day) in [(0, "2026-06-01"), (1, "2026-06-02"), (2, "2026-06-03")] {
                        let noon = JUN01_NS + d * DAY_NS + 12 * HOUR_NS;
                        let times: Vec<i64> = (0..100).map(|i| noon + i * MINUTE_NS).collect();
                        write_bounded_gauge(tmp.path(), day, &times);
                    }
                    let engine = crate::querier::QueryEngine::new(&pruning_opts(tmp.path()))
                        .await
                        .unwrap();
                    // 15-min query at noon on day 2: the ±1 h pruning margin
                    // still reaches no other day's file.
                    let lo = JUN01_NS + DAY_NS + 12 * HOUR_NS;
                    let hi = lo + 15 * MINUTE_NS;
                    let resp = handle_range(&engine, "m", lo, hi, MINUTE_NS, hi)
                        .await
                        .unwrap();
                    assert!(
                        !resp.data.result.is_empty(),
                        "query must return the day-2 series"
                    );
                });
            });
        })
        .join()
        .unwrap();

        let total = metrics_files_opened(&snap);
        assert!(
            (total - 1.0).abs() < f64::EPSILON,
            "15-min query over the 3-day store must open only the 1 in-window file, \
             opened: {total}"
        );
    }

    /// FR1 result-equality (task 3): a window spanning a day boundary returns
    /// results identical to the unscoped path. The baseline engine's inventory
    /// is emptied so every `table_scoped` call falls back to the registered
    /// full table — byte-for-byte the pre-pruning behaviour — over the same
    /// store; the pruned engine must produce the identical matrix.
    #[tokio::test]
    async fn test_cross_day_query_correct() {
        const MINUTE_NS: i64 = 60_000_000_000;
        const HOUR_NS: i64 = 60 * MINUTE_NS;
        const DAY_NS: i64 = 24 * HOUR_NS;
        /// 2026-06-01T00:00:00Z (anchored: `date -u -d '2026-06-01' +%s`).
        const JUN01_NS: i64 = 1_780_272_000_000_000_000;

        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        // One exact-bounds file each side of midnight: day 1 23:00–23:49 and
        // day 2 00:05–00:44, one sample per minute.
        let d1: Vec<i64> = (0..50)
            .map(|i| JUN01_NS + 23 * HOUR_NS + i * MINUTE_NS)
            .collect();
        write_bounded_gauge(tmp.path(), "2026-06-01", &d1);
        let d2: Vec<i64> = (5..45).map(|i| JUN01_NS + DAY_NS + i * MINUTE_NS).collect();
        write_bounded_gauge(tmp.path(), "2026-06-02", &d2);

        let opts = pruning_opts(tmp.path());
        let pruned = crate::querier::QueryEngine::new(&opts).await.unwrap();
        let full = crate::querier::QueryEngine::new(&opts).await.unwrap();
        full.set_inventory_for_test(crate::querier::FileInventory::default());

        let lo = JUN01_NS + 23 * HOUR_NS;
        let hi = JUN01_NS + DAY_NS + 45 * MINUTE_NS;
        let step = 5 * MINUTE_NS;
        let a = handle_range(&pruned, "m", lo, hi, step, hi).await.unwrap();
        let b = handle_range(&full, "m", lo, hi, step, hi).await.unwrap();
        let (a, b) = (
            serde_json::to_value(&a).unwrap(),
            serde_json::to_value(&b).unwrap(),
        );
        assert_eq!(
            a, b,
            "pruned result must equal the unscoped full-scan result"
        );

        // Sanity: the (identical) result actually spans the midnight boundary.
        let points = a["data"]["result"][0]["values"].as_array().unwrap();
        #[allow(clippy::cast_precision_loss)]
        let midnight = (JUN01_NS + DAY_NS) as f64 / 1e9;
        assert!(
            points.iter().any(|p| p[0].as_f64().unwrap() < midnight),
            "points before midnight expected: {points:?}"
        );
        assert!(
            points.iter().any(|p| p[0].as_f64().unwrap() >= midnight),
            "points after midnight expected: {points:?}"
        );
    }

    /// Task 8 (NFR1 evidence): files opened scale with the QUERY WINDOW, not
    /// the store size. Same store, same PromQL, two windows: the 15-min
    /// window opens exactly the 1 in-window file; the 3-day window opens all
    /// 3 day files — deterministic counts from the fixture layout, observed
    /// as [`metrics_files_opened`] snapshot deltas (`DebuggingRecorder`
    /// pattern). Bare selector `m` classifies as `Capability::None`, so every
    /// window routes raw — no tier table can absorb part of the scan.
    #[test]
    fn test_files_opened_scales_with_window_not_store() {
        use metrics_util::debugging::DebuggingRecorder;

        const MINUTE_NS: i64 = 60_000_000_000;
        const HOUR_NS: i64 = 60 * MINUTE_NS;
        const DAY_NS: i64 = 24 * HOUR_NS;
        /// 2026-06-01T00:00:00Z (anchored: `date -u -d '2026-06-01' +%s`).
        const JUN01_NS: i64 = 1_780_272_000_000_000_000;

        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        let (files_15m, files_3d) = std::thread::spawn(move || {
            metrics::with_local_recorder(&recorder, || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let tmp = tempfile::tempdir().unwrap();
                    // 3 `dt=` days, one exact-bounds-named file each: 100
                    // one-minute samples from noon.
                    for (d, day) in [(0, "2026-06-01"), (1, "2026-06-02"), (2, "2026-06-03")] {
                        let noon = JUN01_NS + d * DAY_NS + 12 * HOUR_NS;
                        let times: Vec<i64> = (0..100).map(|i| noon + i * MINUTE_NS).collect();
                        write_bounded_gauge(tmp.path(), day, &times);
                    }
                    let engine = crate::querier::QueryEngine::new(&pruning_opts(tmp.path()))
                        .await
                        .unwrap();
                    // 15-min window at noon on day 2: the ±1 h pruning margin
                    // still reaches no other day's file.
                    let lo = JUN01_NS + DAY_NS + 12 * HOUR_NS;
                    let hi = lo + 15 * MINUTE_NS;
                    let resp = handle_range(&engine, "m", lo, hi, MINUTE_NS, hi).await.unwrap();
                    assert!(!resp.data.result.is_empty(), "15-min query must return data");
                    let files_15m = metrics_files_opened(&snap);
                    // Same PromQL over a 3-day window covering every day file.
                    let lo3 = JUN01_NS + 12 * HOUR_NS;
                    let hi3 = JUN01_NS + 2 * DAY_NS + 14 * HOUR_NS;
                    let resp = handle_range(&engine, "m", lo3, hi3, HOUR_NS, hi3)
                        .await
                        .unwrap();
                    assert!(!resp.data.result.is_empty(), "3-day query must return data");
                    // `Snapshotter::snapshot()` DRAINS histogram samples, so each
                    // read is already the delta since the previous snapshot — do
                    // not subtract `files_15m` again (empirically: 4 executions
                    // × 1 file = snapshots of 1 then 3).
                    (files_15m, metrics_files_opened(&snap))
                })
            })
        })
        .join()
        .unwrap();

        assert!(
            (files_15m - 1.0).abs() < f64::EPSILON,
            "15-min window must open exactly the 1 in-window file, opened: {files_15m}"
        );
        assert!(
            (files_3d - 3.0).abs() < f64::EPSILON,
            "3-day window must open all 3 day files, opened: {files_3d}"
        );
        assert!(
            files_15m < files_3d,
            "files opened must scale with the window: 15-min {files_15m} vs 3-day {files_3d}"
        );
    }

    /// Task 8 (NFR1/NFR2 evidence) + promql-plan-cache task 1 (FR1): demo-scale
    /// bench — 1,505 exact-bounds gauge Parquet files across 7 `dt=` days
    /// (matching the live store's ~1,529 files / 7 days), 3 tiny rows each,
    /// plus 40 histogram files on the last day (15:00–19:00 slots, ≥ 2 h away
    /// from the noon gauge window so the ±1 h pruning margin keeps the gauge
    /// probes' scoped file set unchanged). PRINTS a per-stage plan-pipeline
    /// table (parse/lower/optimize/physical/execute vs end-to-end total) for
    /// three query shapes — `rate()`, bare selector, `histogram_quantile` —
    /// 3 sliding runs each (run 0 = shape-cold, runs 1–2 = shape-warm; the
    /// window slides one step per run so the RESULT cache misses, isolating
    /// what a PLAN cache would save) plus one same-window repeat (result-cache
    /// hit). Deliberately asserts nothing about wall-clock (CI variance). Run:
    /// `cargo test --lib querier:: -- --ignored bench_cold_range_query_demo_scale --nocapture`
    #[test]
    #[ignore = "demo-scale bench: generates 1,545 Parquet files; prints measurements, no assertions"]
    #[allow(clippy::print_stderr)] // benchmark timing output (run with --nocapture)
    fn bench_cold_range_query_demo_scale() {
        use std::time::Instant;

        use metrics_util::debugging::DebuggingRecorder;

        const SEC_NS: i64 = 1_000_000_000;
        const MINUTE_NS: i64 = 60 * SEC_NS;
        const HOUR_NS: i64 = 60 * MINUTE_NS;
        const DAY_NS: i64 = 24 * HOUR_NS;
        /// 2026-06-01T00:00:00Z (anchored: `date -u -d '2026-06-01' +%s`).
        const JUN01_NS: i64 = 1_780_272_000_000_000_000;
        const DAYS: i64 = 7;
        const FILES_PER_DAY: i64 = 215; // 7 × 215 = 1,505 ≥ demo scale
        const HIST_FILES: i64 = 40; // last day, 6-min slots from 15:00

        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        std::thread::spawn(move || {
            metrics::with_local_recorder(&recorder, || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    let tmp = tempfile::tempdir().unwrap();
                    let t = Instant::now();
                    for d in 0..DAYS {
                        let day = format!("2026-06-{:02}", d + 1);
                        let day_ns = JUN01_NS + d * DAY_NS;
                        for i in 0..FILES_PER_DAY {
                            // One exact-bounds file per 6-min slot, 3 samples.
                            let start = day_ns + i * 6 * MINUTE_NS;
                            write_bounded_gauge(
                                tmp.path(),
                                &day,
                                &[start, start + 30 * SEC_NS, start + 60 * SEC_NS],
                            );
                        }
                    }
                    // Histogram series (promql-plan-cache task 1): 6-min slots
                    // on the last day from 15:00, mirroring the gauge slot
                    // shape; cumulative counts keep growing across files.
                    let last_day = format!("2026-06-{DAYS:02}");
                    let last_day_ns = JUN01_NS + (DAYS - 1) * DAY_NS;
                    for i in 0..HIST_FILES {
                        let start = last_day_ns + 15 * HOUR_NS + i * 6 * MINUTE_NS;
                        write_bounded_histogram(
                            tmp.path(),
                            &last_day,
                            &[start, start + 30 * SEC_NS, start + 60 * SEC_NS],
                            (i * 3).unsigned_abs() * 10,
                        );
                    }
                    eprintln!(
                        "bench_cold_range_query_demo_scale: store={} gauge + {HIST_FILES} \
                         histogram files / {DAYS} dt= days, generated in {:.1} s",
                        DAYS * FILES_PER_DAY,
                        t.elapsed().as_secs_f64()
                    );
                    let t = Instant::now();
                    let engine = crate::querier::QueryEngine::new(&pruning_opts(tmp.path()))
                        .await
                        .unwrap();
                    eprintln!(
                        "engine build (walk + register): {:.1} ms",
                        t.elapsed().as_secs_f64() * 1e3
                    );
                    // The live probes: 15-min windows on the last (active) day —
                    // gauge shapes end at noon, the histogram shape at 18:00
                    // (inside its 15:00–19:00 slot coverage).
                    let hi = JUN01_NS + (DAYS - 1) * DAY_NS + 12 * HOUR_NS;
                    let lo = hi - 15 * MINUTE_NS;
                    let hist_hi = last_day_ns + 18 * HOUR_NS;
                    let hist_lo = hist_hi - 15 * MINUTE_NS;
                    let shapes: [(&str, i64, i64); 3] = [
                        ("rate(m[5m])", lo, hi),
                        ("m", lo, hi),
                        (
                            "histogram_quantile(0.95, sum(rate(hist_seconds_bucket[5m])) by (le))",
                            hist_lo,
                            hist_hi,
                        ),
                    ];
                    eprintln!(
                        "{:<68} {:>10} {:>9} {:>8} {:>8} {:>9} {:>9} {:>8} {:>10} {:>9} {:>6}",
                        "shape", "run", "total_ms", "parse", "lower", "optimize", "physical",
                        "execute", "stage_sum", "residual", "files"
                    );
                    for (query, lo0, hi0) in shapes {
                        // 3 sliding runs (result-cache miss each time) + 1
                        // same-window repeat (result-cache hit).
                        for run in 0..4 {
                            let slide = i64::from(run.min(2)) * MINUTE_NS;
                            let (lo, hi) = (lo0 + slide, hi0 + slide);
                            let label = match run {
                                0 => "cold",
                                3 => "rcache",
                                _ => "warm",
                            };
                            // Drain the recorder so this run's samples stand alone.
                            let _ = drain_plan_stages_and_files(&snap);
                            let t = Instant::now();
                            let resp = handle_range(&engine, query, lo, hi, MINUTE_NS, hi)
                                .await
                                .unwrap();
                            let total_ms = t.elapsed().as_secs_f64() * 1e3;
                            assert!(
                                !resp.data.result.is_empty(),
                                "{query}: bench query must return data"
                            );
                            let (stages, files) = drain_plan_stages_and_files(&snap);
                            let ms = |s: &str| stages.get(s).copied().unwrap_or(0.0) * 1e3;
                            let stage_sum: f64 = stages.values().sum::<f64>() * 1e3;
                            eprintln!(
                                "{query:<68} {run}:{label:<7} {total_ms:>9.1} {:>8.2} {:>8.2} \
                                 {:>9.2} {:>9.2} {:>8.1} {stage_sum:>10.1} {:>9.1} {files:>6}",
                                ms("parse"),
                                ms("lower"),
                                ms("optimize"),
                                ms("physical"),
                                ms("execute"),
                                total_ms - stage_sum,
                            );
                        }
                    }
                });
            });
        })
        .join()
        .unwrap();
    }

    /// NFR3 (no silent bypass) — source guard, mirroring `no_sql_invariant_tests`
    /// in `mod.rs`: read this module's own source, drop the test region (split on
    /// the first `#[cfg(test)]`, keep the production prefix), strip line comments,
    /// and assert no handler hardcodes a rollup tier table. Tier table names are
    /// produced ONLY inside `resolve_metric_windows` via
    /// `format!("metrics_{}", tier.label())`, so no production line may contain a
    /// `metrics_5m`/`metrics_1h`/`metrics_1d` literal or a `.table("metrics_`
    /// occurrence. This test FAILS if a future handler hardcodes a tier table,
    /// bypassing the single tier-resolution choke point.
    ///
    /// Raw `.table("metrics")` (no trailing `_`) is intentionally allowed — it is
    /// the safe fallback `resolve_metric_windows` itself emits and the two
    /// documented no-time-range metadata fallbacks read.
    #[test]
    fn test_no_handler_hardcodes_tier_table() {
        let src = include_str!("prometheus.rs");
        // Production region only — drop everything from the first test module.
        let prod = src.split("#[cfg(test)]").next().unwrap();
        // Strip line comments so doc/comment mentions of a tier table don't trip
        // the gate (mirrors `no_sql_invariant_tests::test_no_format_sql_in_core`).
        let code: String = prod
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");

        for tier in ["metrics_5m", "metrics_1h", "metrics_1d"] {
            assert!(
                !code.contains(tier),
                "prometheus.rs hardcodes tier table `{tier}` outside the resolver — \
                 reach tier tables only via `resolve_metric_windows`'s `format!`"
            );
        }
        assert!(
            !code.contains(".table(\"metrics_"),
            "prometheus.rs scans a tier table directly via `.table(\"metrics_…\")` — \
             route every tier read through `resolve_metric_windows`"
        );
    }
}
