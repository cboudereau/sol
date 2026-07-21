// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! P5/P6/P7 — window-function primitives over `DataFrame`/`Expr`, reproducing the
//! SQL window lowerings: latest-per-series (`ROW_NUMBER … rn=1`), `rate` (`LAG` +
//! counter-reset), and `<agg>_over_time` (`RANGE … PRECEDING` frame).
//!
//! These are the only "hard" primitives; they are built and parity-tested here in
//! isolation against the known SQL outputs before any signal is rewired (the
//! de-risking gate). Time keys are canonical nanoseconds `i64` (FR7), so the RANGE
//! frame bound and the `ORDER BY` key share units.

use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_window::lead_lag::lag;
use datafusion::functions_window::nth_value::first_value_udwf;
use datafusion::functions_window::rank::dense_rank;
use datafusion::functions_window::row_number::row_number;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::{
    ExprFunctionExt, WindowFrame, WindowFrameBound, WindowFrameUnits, when,
};
use datafusion::prelude::{DataFrame, Expr, col, lit};
use datafusion::scalar::ScalarValue;

/// Sliding-window aggregate operator for `<agg>_over_time`.
#[derive(Debug, Clone, Copy)]
pub enum OverTimeAgg {
    /// `max_over_time`
    Max,
    /// `min_over_time`
    Min,
    /// `avg_over_time`
    Avg,
    /// `sum_over_time`
    Sum,
    /// `count_over_time`
    Count,
}

fn ns(time_col: &str) -> Expr {
    cast(col(time_col), DataType::Int64)
}

/// P5 — keep the latest row per series: a `row_number()` window partitioned by
/// `part`, ordered by `time_col` descending, filtered to `rn = 1`. The `rn`
/// column is left in place for the caller to project away.
///
/// # Errors
/// Propagates DataFusion plan-construction errors.
pub fn latest_per_series(
    df: DataFrame,
    part: Vec<Expr>,
    time_col: &str,
) -> crate::Result<DataFrame> {
    let rn = row_number()
        .partition_by(part)
        .order_by(vec![col(time_col).sort(false, false)])
        .build()?
        .alias("rn");
    Ok(df.window(vec![rn])?.filter(col("rn").eq(lit(1_u64)))?)
}

/// `topk(k, …)` / `bottomk(k, …)` lowered to a window plan, replacing the Rust
/// sort-and-truncate. PromQL `topk` keeps the top-`k` **whole series** (all their
/// points), ranked by each series' representative (peak) value — the semantics of
/// the superseded `topk_series`. We reproduce that relationally:
/// 1. `peak = MAX(v) OVER (PARTITION BY part)` — the per-series score on every row;
/// 2. `series_rank = DENSE_RANK() OVER (ORDER BY peak DESC[topk]/ASC[bottomk], part)`
///    — distinct series get distinct ranks (the full key is in the `ORDER BY`, so
///    every row of a series shares one rank); ties on peak break by key, stable;
/// 3. keep rows with `series_rank <= k`.
///
/// The `peak`/`series_rank` columns are left in place for the caller to project
/// away. `k <= 0` yields an empty result (matches `truncate(0)`).
///
/// # Errors
/// Propagates DataFusion plan-construction errors.
pub fn lower_topk(
    df: DataFrame,
    part: Vec<Expr>,
    v_col: &str,
    k: i64,
    is_topk: bool,
) -> crate::Result<DataFrame> {
    use datafusion::functions_aggregate::min_max::max_udaf;
    let max_win: Expr = WindowFunction::new(max_udaf(), vec![col(v_col)]).into();
    let peak = max_win.partition_by(part.clone()).build()?.alias("peak");
    let with_peak = df.window(vec![peak])?;
    // Order series by peak, breaking ties on the partition key so each distinct
    // series lands on its own dense rank (and all its rows share that rank).
    // `sort(asc, …)`: topk ranks highest-peak first → descending → asc = false.
    let mut order = vec![col("peak").sort(!is_topk, false)];
    order.extend(part.into_iter().map(|e| e.sort(true, false)));
    let rank = dense_rank().order_by(order).build()?.alias("series_rank");
    let kept = u64::try_from(k.max(0)).unwrap_or(u64::MAX);
    Ok(with_peak
        .window(vec![rank])?
        .filter(col("series_rank").lt_eq(lit(kept)))?)
}

/// P6 — `irate`: per-sample delta via `LAG` over the series window, counter-reset
/// aware (`v < prev_v` → use `v`), divided by the elapsed seconds — i.e. the
/// latest inter-sample slope. Drops the first sample of each series (no
/// predecessor) and zero-`dt` duplicate timestamps. Output columns:
/// `service_name`, `attributes`, `prom_series_key`, `time_unix_nano`, `v`.
///
/// # Errors
/// Propagates DataFusion plan-construction errors.
pub fn irate(
    df: DataFrame,
    part: Vec<Expr>,
    v_col: &str,
    time_col: &str,
) -> crate::Result<DataFrame> {
    let order = vec![col(time_col).sort(true, false)];
    let prev_v = lag(col(v_col), Some(1), None)
        .partition_by(part.clone())
        .order_by(order.clone())
        .build()?
        .alias("prev_v");
    let prev_t = lag(ns(time_col), Some(1), None)
        .partition_by(part)
        .order_by(order)
        .build()?
        .alias("prev_t");
    let win = df.window(vec![prev_v, prev_t])?;
    // counter-reset-aware delta / elapsed-seconds
    let delta =
        when(col(v_col).gt_eq(col("prev_v")), col(v_col) - col("prev_v")).otherwise(col(v_col))?;
    let dt_secs = cast(ns(time_col) - col("prev_t"), DataType::Float64) / lit(1e9);
    let rate = (delta / dt_secs).alias("v");
    Ok(win
        .filter(
            col("prev_t")
                .is_not_null()
                .and(ns(time_col).not_eq(col("prev_t"))),
        )?
        .select(vec![
            col("service_name"),
            col("attributes"),
            col("prom_series_key"),
            col(time_col),
            rate,
        ])?)
}

/// P6 — `rate(m[w])` / `increase(m[w])` with **windowed** Prometheus semantics
/// (Sol↔Mimir parity), NOT the per-sample slope (`irate`). At each sample time
/// `t` the value is Prometheus's `extrapolatedRate`: the reset-adjusted in-window
/// increase, **extrapolated to the window boundaries**, then (for `rate`) divided
/// by the window seconds (`increase` skips that division — `divide_by_window =
/// false`).
///
/// 1. per-sample reset-adjusted `delta` via `LAG` (`v − prev_v`, or `v` on a
///    counter reset where `v < prev_v`); the first sample of each series has no
///    predecessor → its delta is NULL so it never inflates a window sum;
/// 2. over the `RANGE BETWEEN range_ns PRECEDING AND CURRENT ROW` frame (the same
///    ns-based frame as [`over_time`]) gather: `sum_delta = SUM(delta)`,
///    `first_delta = FIRST_VALUE(delta)`, `first_value = FIRST_VALUE(v)`,
///    `first_t = FIRST_VALUE(t)` (frame's earliest sample time) and `cnt` (frame
///    row count). `last_t` (frame's latest time) is not a window — it equals the
///    current row's `t`, since the frame ends at CURRENT ROW;
/// 3. the base in-window increase is `result = sum_delta − first_delta`: dropping
///    the leading delta (which reaches back to the sample *before* the window)
///    yields the reset-adjusted increase between the first and last in-window
///    samples — Prometheus's base `resultValue`;
/// 4. extrapolate `result` to the window edges per Prometheus `extrapolatedRate`:
///    extend by up to half the average inter-sample gap on each side, capped at
///    the window boundary; for a counter, clamp a start extrapolation that would
///    imply a value below zero. A single in-window sample (`cnt < 2`) → 0;
/// 5. for `rate`, divide the extrapolated increase by the window seconds
///    (`range_ns / 1e9`).
///
/// Output columns: `service_name`, `attributes`, `prom_series_key`, `time_unix_nano`, `v` — the same
/// shape as [`irate`] so the downstream grid-align + resample are unaffected.
///
/// # Errors
/// Propagates DataFusion plan-construction errors.
#[allow(clippy::cast_precision_loss)]
pub fn rate(
    df: DataFrame,
    part: Vec<Expr>,
    v_col: &str,
    time_col: &str,
    range_ns: i64,
    divide_by_window: bool,
) -> crate::Result<DataFrame> {
    let order = vec![ns(time_col).sort(true, false)];
    let prev_v = lag(col(v_col), Some(1), None)
        .partition_by(part.clone())
        .order_by(order.clone())
        .build()?
        .alias("prev_v");
    // Per-sample reset-adjusted delta. The first sample of each series has a NULL
    // `prev_v`; we map that to a NULL delta (not the full value) so `SUM` over the
    // window skips it — otherwise the window's leading sample would be counted as
    // a spurious increase equal to its absolute value.
    let delta = when(col("prev_v").is_null(), lit(ScalarValue::Float64(None)))
        .when(col(v_col).gt_eq(col("prev_v")), col(v_col) - col("prev_v"))
        .otherwise(col(v_col))?;
    // `with_column` preserves every input column (the partition keys `name`/
    // `service_name`/`attributes` and the time key) and just appends `delta`, so
    // the window aggregates below can still `PARTITION BY part`.
    let win = df.window(vec![prev_v])?.with_column("delta", delta)?;
    // All the window aggregates below share ONE frame/partition/order — the same
    // ns-based `(t−range, t]` RANGE frame — so their per-row values are aligned.
    let frame = || {
        WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::Int64(Some(range_ns))),
            WindowFrameBound::CurrentRow,
        )
    };
    let over_frame = |f: Expr, alias: &str| -> crate::Result<Expr> {
        Ok(f.partition_by(part.clone())
            .order_by(vec![ns(time_col).sort(true, false)])
            .window_frame(frame())
            .build()?
            .alias(alias))
    };
    let sum_win: Expr = WindowFunction::new(sum_udaf(), vec![col("delta")]).into();
    // FIRST_VALUE as a true window UDWF (not the aggregate-as-window path, which
    // needs a sliding accumulator DataFusion 53 doesn't provide for first_value):
    // returns the value at the frame's first (earliest-time) row.
    let first_delta_win: Expr =
        WindowFunction::new(first_value_udwf(), vec![col("delta")]).into();
    let first_value_win: Expr = WindowFunction::new(first_value_udwf(), vec![col(v_col)]).into();
    // `first_t` (the window start) is the leading row's `t` on the ASC-time-ordered
    // frame → FIRST_VALUE(t), joining the leading-row family (delta, v, t) that all
    // share ONE partition/order/frame, so DataFusion co-locates them in a single
    // window node. `last_t` needs no window at all: the frame ends at CURRENT ROW,
    // so the frame's greatest time IS the current row's `t` — read straight off the
    // row. This drops the former MIN(t) and MAX(t) aggregate-window passes.
    let first_t_win: Expr = WindowFunction::new(first_value_udwf(), vec![ns(time_col)]).into();
    let cnt_win: Expr = WindowFunction::new(count_udaf(), vec![col(v_col)]).into();
    let windowed = win.window(vec![
        over_frame(sum_win, "sum_delta")?,
        over_frame(first_delta_win, "first_delta")?,
        over_frame(first_value_win, "first_value")?,
        over_frame(first_t_win, "first_t")?,
        over_frame(cnt_win, "cnt")?,
    ])?;
    // The frame's last time == the current row's time (frame ends at CURRENT ROW).
    let last_t = ns(time_col);

    // --- Prometheus `extrapolatedRate` (promql/functions.go) over the frame ---
    let secs = |ns_expr: Expr| cast(ns_expr, DataType::Float64) / lit(1e9);
    let cnt = cast(col("cnt"), DataType::Float64);
    // Base reset-adjusted in-window increase: drop the leading delta (which reaches
    // to the sample before the window) → increase between first & last in-window.
    let result = coalesce(vec![col("sum_delta"), lit(0.0_f64)])
        - coalesce(vec![col("first_delta"), lit(0.0_f64)]);
    // sampledInterval = (last_t − first_t) seconds; avg_gap = interval / (cnt−1).
    let sampled_interval = secs(last_t.clone() - col("first_t"));
    let avg_gap = sampled_interval.clone() / (cnt.clone() - lit(1.0_f64));
    // durationToStart = gap from the window start (last_t − range) to the first
    // sample. durationToEnd (gap from the last sample to the window end) is
    // provably 0 — the frame ends at CURRENT ROW so last_t IS the window end —
    // so the term is dropped from `factor` below rather than computed.
    let window_start = last_t - lit(ScalarValue::Int64(Some(range_ns)));
    let duration_to_start_raw = secs(col("first_t") - window_start);
    // Counter zero-clamp: if result>0 and first_value>=0, don't extrapolate the
    // start below the point where the counter would hit zero.
    let duration_to_zero = sampled_interval.clone() * (col("first_value") / result.clone());
    let clamp_zero = result
        .clone()
        .gt(lit(0.0_f64))
        .and(col("first_value").gt_eq(lit(0.0_f64)))
        .and(duration_to_zero.clone().lt(duration_to_start_raw.clone()));
    let duration_to_start_z = when(clamp_zero, duration_to_zero)
        .otherwise(duration_to_start_raw.clone())?;
    // Boundary cap: if the extrapolation reaches ≥1.1× the average gap past the
    // outermost sample, cap it at half the average gap (Prometheus's heuristic).
    let cap = |d: Expr| -> crate::Result<Expr> {
        Ok(
            when(d.clone().gt_eq(lit(1.1_f64) * avg_gap.clone()), avg_gap.clone() / lit(2.0_f64))
                .otherwise(d)?,
        )
    };
    let duration_to_start = cap(duration_to_start_z)?;
    // duration_to_end == 0 (proven above), so it contributes nothing to the factor.
    let factor = (sampled_interval.clone() + duration_to_start) / sampled_interval.clone();
    let extrapolated = result * factor;
    // A single in-window sample (cnt < 2) has no rate. We emit NULL (not 0) so the
    // downstream grid-align/resample drops the point — exactly as before, when the
    // first sample's SUM(delta) was NULL. NULL also guards the /(cnt−1) and
    // /sampledInterval divisions above (NaN/±inf for cnt<2) from leaking.
    let increase = when(cnt.lt(lit(2.0_f64)), lit(ScalarValue::Float64(None)))
        .otherwise(extrapolated)?
        .alias("increase");
    let windowed = windowed.with_column("increase", increase)?;
    // `rate` divides by the window seconds; `increase` keeps the raw increase.
    let v = if divide_by_window {
        (col("increase") / lit(range_ns as f64 / 1e9)).alias("v")
    } else {
        col("increase").alias("v")
    };
    Ok(windowed.select(vec![
        col("service_name"),
        col("attributes"),
        col("prom_series_key"),
        col(time_col),
        v,
    ])?)
}

/// P7 — `<agg>_over_time`: a sliding `agg(v)` over a `RANGE BETWEEN range_ns
/// PRECEDING AND CURRENT ROW` frame, partitioned by `part`, ordered by the ns
/// time key. Output columns: `service_name`, `attributes`, `prom_series_key`, `time_unix_nano`, `v`.
///
/// # Errors
/// Propagates DataFusion plan-construction errors.
pub fn over_time(
    df: DataFrame,
    part: Vec<Expr>,
    v_col: &str,
    time_col: &str,
    range_ns: i64,
    agg: OverTimeAgg,
) -> crate::Result<DataFrame> {
    // An aggregate used as a window function: build an Expr::WindowFunction from
    // the UDAF directly (ExprFunctionExt::partition_by only promotes an expr that
    // is already a WindowFunction, not a bare AggregateFunction).
    let udaf = match agg {
        OverTimeAgg::Max => max_udaf(),
        OverTimeAgg::Min => min_udaf(),
        OverTimeAgg::Avg => avg_udaf(),
        OverTimeAgg::Sum => sum_udaf(),
        OverTimeAgg::Count => count_udaf(),
    };
    let win: Expr = WindowFunction::new(udaf, vec![col(v_col)]).into();
    let frame = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::Preceding(ScalarValue::Int64(Some(range_ns))),
        WindowFrameBound::CurrentRow,
    );
    let windowed = win
        .partition_by(part)
        .order_by(vec![ns(time_col).sort(true, false)])
        .window_frame(frame)
        .build()?
        .alias("v");
    Ok(df.select(vec![
        col("service_name"),
        col("attributes"),
        col("prom_series_key"),
        col(time_col),
        windowed,
    ])?)
}

/// `avg_over_time` off a rollup tier: the windowed mean is
/// `SUM(value_sum) / SUM(value_count)` over the same `RANGE … PRECEDING` frame,
/// **not** `AVG(value_last)` — a single windowed column cannot recover the
/// per-bucket mean once the rollup has reduced each bucket to scalars. The two
/// windowed sums share one frame/partition, so the ratio equals the raw
/// `avg_over_time` over the bucket members. Output columns mirror [`over_time`].
///
/// # Errors
/// Propagates DataFusion plan-construction errors.
pub fn over_time_ratio(
    df: DataFrame,
    part: Vec<Expr>,
    num_col: &str,
    den_col: &str,
    time_col: &str,
    range_ns: i64,
) -> crate::Result<DataFrame> {
    let frame = || {
        WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::Int64(Some(range_ns))),
            WindowFrameBound::CurrentRow,
        )
    };
    let order = || vec![ns(time_col).sort(true, false)];
    let win_sum = |c: &str| -> crate::Result<Expr> {
        let w: Expr = WindowFunction::new(sum_udaf(), vec![col(c)]).into();
        Ok(w.partition_by(part.clone())
            .order_by(order())
            .window_frame(frame())
            .build()?)
    };
    let v = (win_sum(num_col)? / win_sum(den_col)?).alias("v");
    Ok(df.select(vec![
        col("service_name"),
        col("attributes"),
        col("prom_series_key"),
        col(time_col),
        v,
    ])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, AsArray, Float64Array, StringArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Float64Type, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    /// A 3-sample counter (http_total, service=client) at t=1,2,3s → 10,30,60.
    async fn counter_engine() -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            Field::new("prom_series_key", DataType::Utf8, false),
            Field::new("double_value", DataType::Float64, true),
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
                // series key for `{}` attributes == series_key_string("{}") == "".
                Arc::new(StringArray::from(vec!["", "", ""])),
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
        crate::querier::QueryEngine::new(&opts).await.unwrap()
    }

    async fn base(engine: &crate::querier::QueryEngine) -> DataFrame {
        engine
            .table("metrics")
            .await
            .unwrap()
            .filter(col("name").eq(lit("http_total")))
            .unwrap()
            .select(vec![
                col("service_name"),
                col("attributes"),
                col("prom_series_key"),
                col("time_unix_nano"),
                col("double_value").alias("v"),
            ])
            .unwrap()
    }

    /// Collect the `v` column sorted by time — the parity comparison vector.
    async fn values_by_time(engine: &crate::querier::QueryEngine, df: DataFrame) -> Vec<f64> {
        let df = df
            .sort(vec![col("time_unix_nano").sort(true, false)])
            .unwrap();
        let batches = engine.collect(df).await.unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let idx = b.schema().index_of("v").unwrap();
            let v = b.column(idx).as_primitive::<Float64Type>();
            for i in 0..b.num_rows() {
                // Skip NULL `v` (e.g. the first sample of a windowed rate, whose
                // increase is NULL) — downstream `group_range_series` drops these.
                if !v.is_null(i) {
                    out.push(v.value(i));
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn test_rate_is_windowed_average_over_the_range() {
        let engine = counter_engine().await;
        let part = vec![col("service_name"), col("attributes")];
        // Window 5m = 300s covers all preceding samples. `rate` now replicates
        // Prometheus `extrapolatedRate`: the reset-adjusted in-window increase is
        // extrapolated to the window boundaries, then divided by the full window.
        // Counter series t=1,2,3s → 10,30,60 (deltas: t=1 NULL, t=2 +20, t=3 +30).
        //
        //   t=1s: first sample, `result` NULL → SUM NULL → dropped downstream.
        //   t=2s: window={1,2}. result = sum_delta(20) - first_delta(NULL→0) = 20.
        //         cnt=2, first_t=1s, last_t=2s. sampledInterval=1s, avg_gap=1s.
        //         durationToStart = first_t-(last_t-range) = 1-(2-300) = 299s.
        //         durationToEnd = last_t-last_t = 0. first_value=10>0, result>0 →
        //         durationToZero = 1*(10/20)=0.5 < 299 → durationToStart=0.5.
        //         0.5 < 1.1*avg → no cap. factor=(1+0.5+0)/1=1.5.
        //         extrapolated=20*1.5=30 → rate=30/300=0.1.
        //   t=3s: window={1,2,3}. result = 50 - 0 = 50. cnt=3, first_t=1s,
        //         last_t=3s. sampledInterval=2s, avg_gap=1s. durationToStart=298s.
        //         durationToEnd=0. durationToZero=2*(10/50)=0.4 < 298 →
        //         durationToStart=0.4. no cap. factor=(2+0.4+0)/2=1.2.
        //         extrapolated=50*1.2=60 → rate=60/300=0.2.
        let df = rate(base(&engine).await, part, "v", "time_unix_nano", 300_000_000_000, true)
            .unwrap();
        let got = values_by_time(&engine, df).await;
        assert_eq!(got, vec![0.1, 0.2]);
    }

    #[tokio::test]
    async fn test_increase_is_windowed_sum_without_dividing() {
        let engine = counter_engine().await;
        let part = vec![col("service_name"), col("attributes")];
        // increase(m[5m]) = the extrapolated in-window increase, NOT divided by the
        // window. Same extrapolation as `test_rate_is_windowed_average_over_the_range`
        // → t=2s: 30, t=3s: 60 (the rate values above × 300).
        let df = rate(base(&engine).await, part, "v", "time_unix_nano", 300_000_000_000, false)
            .unwrap();
        assert_eq!(values_by_time(&engine, df).await, vec![30.0, 60.0]);
    }

    /// Sum the window expressions across every `Window` node in a logical plan.
    fn count_window_exprs(plan: &datafusion::logical_expr::LogicalPlan) -> usize {
        use datafusion::logical_expr::LogicalPlan;
        let here = match plan {
            LogicalPlan::Window(w) => w.window_expr.len(),
            _ => 0,
        };
        here + plan
            .inputs()
            .iter()
            .map(|p| count_window_exprs(p))
            .sum::<usize>()
    }

    #[tokio::test]
    async fn test_rate_plan_window_passes_are_reduced() {
        // FR1 regression guard: the reduced `rate()` lowering builds 6 window
        // expressions — LAG(prev_v) + {SUM(delta), FIRST_VALUE(delta),
        // FIRST_VALUE(v), FIRST_VALUE(t), COUNT(v)}. The pre-reduction plan built 7
        // (an extra MAX(t) pass, plus MIN(t) instead of FIRST_VALUE(t)); MAX(t) is
        // gone (last_t = current row t) and duration_to_end is dropped. If this
        // count rises, a window pass crept back in.
        let engine = counter_engine().await;
        let part = vec![col("service_name"), col("attributes")];
        let df = rate(base(&engine).await, part, "v", "time_unix_nano", 300_000_000_000, true)
            .unwrap();
        assert_eq!(
            count_window_exprs(df.logical_plan()),
            6,
            "reduced rate() must build 6 window exprs (was 7 before FR1)"
        );
    }

    /// A counter with a caller-supplied (time_ns, value) series for `client` /
    /// `http_total`, one Parquet row per sample.
    async fn series_engine(samples: &[(i64, f64)]) -> crate::querier::QueryEngine {
        use crate::config::querier::{QuerierOptions, StorageConfig};
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
            Field::new("prom_series_key", DataType::Utf8, false),
            Field::new("double_value", DataType::Float64, true),
        ]));
        let n = samples.len();
        let times: Vec<i64> = samples.iter().map(|(t, _)| *t).collect();
        let vals: Vec<f64> = samples.iter().map(|(_, v)| *v).collect();
        let attrs: Vec<&str> = vec!["{}"; n];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["client"; n])),
                Arc::new(StringArray::from(vec!["http_total"; n])),
                Arc::new(TimestampNanosecondArray::from(times).with_timezone("UTC")),
                crate::querier::udf::tests::json_map_array(&attrs),
                // series key for `{}` attributes == "".
                Arc::new(StringArray::from(vec![""; n])),
                Arc::new(Float64Array::from(vals)),
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
    async fn test_rate_extrapolates_to_window_edges() {
        // A counter sampled sparsely relative to the window: samples at
        // t=90,100,110,120s → 900,1000,1100,1200 (steady +100 per 10s). Window=60s,
        // evaluated at the last row t=120s. The first in-window sample (t=90) sits
        // well after the window start (t=60), so Prometheus extrapolates toward it.
        //
        // deltas: t=90 NULL (series first), t=100/110/120 = +100 each.
        // window (60,120] = {90,100,110,120}. sum_delta=300, first_delta=NULL→0 →
        // result=300. cnt=4, first_t=90s, last_t=120s.
        //   sampledInterval=(120-90)=30s, avg_gap=30/3=10s.
        //   durationToStart = first_t-(last_t-range) = 90-(120-60) = 30s.
        //   durationToEnd = last_t-last_t = 0.
        //   counter zero-clamp: first_value=900, durationToZero=30*(900/300)=90s;
        //     90 < 30? no → durationToStart unchanged (30s).
        //   cap: durationToStart(30) >= 1.1*avg(11)? yes → durationToStart=avg/2=5s.
        //     durationToEnd(0) >= 11? no.
        //   factor=(30+5+0)/30 = 35/30. extrapolated=300*35/30=350.
        //   rate = 350/60 ≈ 5.8333  (strictly > result/window = 300/60 = 5.0).
        let samples = [
            (90_000_000_000i64, 900.0),
            (100_000_000_000, 1000.0),
            (110_000_000_000, 1100.0),
            (120_000_000_000, 1200.0),
        ];
        let engine = series_engine(&samples).await;
        let part = vec![col("service_name"), col("attributes")];
        let df = rate(base(&engine).await, part, "v", "time_unix_nano", 60_000_000_000, true)
            .unwrap();
        let got = values_by_time(&engine, df).await;
        let last = *got.last().unwrap();
        let expected = 350.0 / 60.0;
        assert!(
            (last - expected).abs() < 1e-9,
            "extrapolated rate {last} != expected {expected}"
        );
        assert!(
            last > 300.0 / 60.0,
            "extrapolated rate {last} must exceed result/window {}",
            300.0 / 60.0
        );
    }

    #[tokio::test]
    async fn test_rate_is_smooth_across_grid() {
        // A steadily-increasing counter: 20 samples at a 15s cadence, +150 each
        // (→ 10/s). Window=60s. Once the window is full the extrapolated rate is a
        // near-constant 10/s across successive rows — the old SUM(delta)/window
        // oscillated as samples crossed the trailing edge; here the zigzag is gone.
        #[allow(clippy::cast_precision_loss)] // small integer test-fixture indices
        let samples: Vec<(i64, f64)> = (1..=20)
            .map(|i| (i * 15_000_000_000i64, (i as f64) * 150.0))
            .collect();
        let engine = series_engine(&samples).await;
        let part = vec![col("service_name"), col("attributes")];
        let df = rate(base(&engine).await, part, "v", "time_unix_nano", 60_000_000_000, true)
            .unwrap();
        let got = values_by_time(&engine, df).await;
        // Steady-state region: drop the initial window-fill ramp (first 3 points).
        let steady = &got[3..];
        let max_jump = steady
            .windows(2)
            .map(|w| (w[1] - w[0]).abs())
            .fold(0.0_f64, f64::max);
        assert!(
            max_jump < 1e-6,
            "rate must be smooth across the grid; max adjacent |Δ| = {max_jump}, series = {steady:?}"
        );
    }

    #[tokio::test]
    async fn test_irate_is_per_sample_slope_unchanged() {
        let engine = counter_engine().await;
        let part = vec![col("service_name"), col("attributes")];
        // irate keeps the latest inter-sample slope: first sample dropped, then
        // 20/1s and 30/1s. (Unchanged from the pre-windowing `rate`.)
        let df = irate(base(&engine).await, part, "v", "time_unix_nano").unwrap();
        assert_eq!(values_by_time(&engine, df).await, vec![20.0, 30.0]);
    }

    #[tokio::test]
    async fn test_over_time_max_matches_sql_semantics() {
        let engine = counter_engine().await;
        let part = vec![col("service_name"), col("attributes")];
        let df = over_time(
            base(&engine).await,
            part,
            "v",
            "time_unix_nano",
            300_000_000_000,
            OverTimeAgg::Max,
        )
        .unwrap();
        // sliding max up to each point within 5m: 10, 30, 60.
        assert_eq!(values_by_time(&engine, df).await, vec![10.0, 30.0, 60.0]);
    }

    #[tokio::test]
    async fn test_latest_per_series_keeps_one_row() {
        let engine = counter_engine().await;
        let part = vec![col("service_name"), col("attributes")];
        let df = latest_per_series(base(&engine).await, part, "time_unix_nano").unwrap();
        let batches = engine.collect(df).await.unwrap();
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 1, "one row per series (the latest)");
        // and it is the latest value, 60.
        let v: Vec<f64> = values_by_time(
            &engine,
            latest_per_series(
                base(&engine).await,
                vec![col("service_name"), col("attributes")],
                "time_unix_nano",
            )
            .unwrap(),
        )
        .await;
        assert_eq!(v, vec![60.0]);
    }
}
