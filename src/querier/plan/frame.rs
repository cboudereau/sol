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
use datafusion::functions_window::lead_lag::lag;
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

/// P6 — `rate`: per-sample delta via `LAG` over the series window, counter-reset
/// aware (`v < prev_v` → use `v`), divided by the elapsed seconds. Drops the first
/// sample of each series (no predecessor) and zero-`dt` duplicate timestamps.
/// Output columns: `service_name`, `attributes`, `time_unix_nano`, `v`.
///
/// # Errors
/// Propagates DataFusion plan-construction errors.
pub fn rate(
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
            col(time_col),
            rate,
        ])?)
}

/// P7 — `<agg>_over_time`: a sliding `agg(v)` over a `RANGE BETWEEN range_ns
/// PRECEDING AND CURRENT ROW` frame, partitioned by `part`, ordered by the ns
/// time key. Output columns: `service_name`, `attributes`, `time_unix_nano`, `v`.
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
        col(time_col),
        windowed,
    ])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{AsArray, Float64Array, StringArray, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{Field, Float64Type, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    /// A 3-sample counter (http_total, service=client) at t=1,2,3s → 10,30,60.
    async fn counter_engine() -> crate::querier::QueryEngine {
        use crate::config::query::{QuerierOptions, StorageConfig};
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
                out.push(v.value(i));
            }
        }
        out
    }

    #[tokio::test]
    async fn test_rate_matches_sql_semantics() {
        let engine = counter_engine().await;
        let part = vec![col("service_name"), col("attributes")];
        let df = rate(base(&engine).await, part, "v", "time_unix_nano").unwrap();
        // first sample dropped; rate at 2s,3s = 20,30 per second (parity with rate_sql).
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
