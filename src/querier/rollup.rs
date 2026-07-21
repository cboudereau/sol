// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Metric rollup tiers / downsampling (task 12).
//!
//! The compactor pre-aggregates the metrics cold tail into coarser resolutions
//! (5m / 1h / 1d) so 13-month-default / 2-year-opt-in ranges meet
//! [NFR6](../../../docs/workspace/parquet-backend/DESIGN.md#nfr6). Per the
//! [long-range-metrics ADR](../../../docs/workspace/parquet-backend/adrs/long-range-metrics-strategy.md),
//! a rollup keeps the **last raw sample per (series, time-bucket)** — preserving
//! actual `bucket_counts` and monotonic counter values rather than pre-computed
//! quantiles, so `histogram_quantile` / `rate` stay correct after rollup. The
//! frontend selects the coarsest tier whose resolution ≤ the query `step`,
//! falling back to raw. Rollups apply to **metric tables only**; traces/logs
//! are bounded-window (≤30d) and skip them.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::StreamExt;

use super::compaction::{
    MERGE_MEM_BUDGET_BYTES, finalize_writer, merge_ctx, open_staged_writer, resolve_files,
};

const M5_NS: i64 = 300_000_000_000;
const H1_NS: i64 = 3_600_000_000_000;
// Canonical day value (canonical-ns ADR — single source of truth, no duplicated literal).
const D1_NS: i64 = super::units::DurationNs::DAY.ns();

/// A downsampling resolution tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollupTier {
    /// Original samples.
    Raw,
    /// 5-minute resolution.
    M5,
    /// 1-hour resolution.
    H1,
    /// 1-day resolution.
    D1,
}

impl RollupTier {
    /// Bucket width in nanoseconds (`Raw` = 0).
    pub fn resolution_ns(self) -> i64 {
        match self {
            RollupTier::Raw => 0,
            RollupTier::M5 => M5_NS,
            RollupTier::H1 => H1_NS,
            RollupTier::D1 => D1_NS,
        }
    }

    /// Footer/file label.
    pub fn label(self) -> &'static str {
        match self {
            RollupTier::Raw => "raw",
            RollupTier::M5 => "5m",
            RollupTier::H1 => "1h",
            RollupTier::D1 => "1d",
        }
    }

    /// Rollup tiers, coarsest last.
    pub fn all() -> [RollupTier; 3] {
        [RollupTier::M5, RollupTier::H1, RollupTier::D1]
    }
}

/// Only metric tables get rollups (NFR7 per-signal scope).
pub fn is_rollup_eligible(signal: &str) -> bool {
    signal == "metrics"
}

/// Pick the coarsest **available** tier whose resolution ≤ `step_ns`; fall back
/// to `Raw` when none qualifies (e.g. a fine step, or the tier is absent).
pub fn select_tier(step_ns: i64, available: &[RollupTier]) -> RollupTier {
    available
        .iter()
        .copied()
        .filter(|t| *t != RollupTier::Raw && t.resolution_ns() > 0 && t.resolution_ns() <= step_ns)
        .max_by_key(|t| t.resolution_ns())
        .unwrap_or(RollupTier::Raw)
}

/// Downsample `batches` to `resolution_ns`: keep the last raw sample per
/// `(name, service_name, attributes, time-bucket)`. A `resolution_ns <= 0`
/// (i.e. `Raw`) returns the input unchanged.
pub async fn rollup_batches(
    batches: Vec<RecordBatch>,
    resolution_ns: i64,
) -> crate::Result<Vec<RecordBatch>> {
    if batches.is_empty() || resolution_ns <= 0 {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let ctx = SessionContext::new();
    ctx.register_table(
        "m",
        Arc::new(MemTable::try_new(Arc::clone(&schema), vec![batches])?),
    )?;
    let out = rollup_plan(ctx.table("m").await?, &schema, resolution_ns)?;
    Ok(out.collect().await?)
}

/// The downsample plan: keep the last raw sample per `(name, service_name,
/// prom_series_key, time-bucket)`, project the original columns back, and
/// sort `(service_name, prom_name, time)` so the tier files prune like raw ones.
/// `arrow_schema` provides the column projection and must carry the stored
/// `prom_series_key` column (FR2 — grouped on directly, no UDF).
fn rollup_plan(
    table: DataFrame,
    arrow_schema: &datafusion::arrow::datatypes::SchemaRef,
    resolution_ns: i64,
) -> crate::Result<DataFrame> {
    use std::collections::HashSet;

    use datafusion::arrow::datatypes::DataType::{Float64, Int64};
    use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
    use datafusion::functions_aggregate::first_last::last_value_udaf;
    use datafusion::logical_expr::{ExprFunctionExt, expr_fn::cast};
    use datafusion::prelude::{coalesce, col, lit};
    use datafusion::scalar::ScalarValue;

    // The four per-bucket scalar-value aggregates (FR6) are *computed* by this
    // plan over the coalesced scalar value — they are never `last_value`d, and
    // need not exist on the input (raw files lack them; the schema adapter nulls
    // them). Excluded from the last-valued set below; emitted explicitly after.
    const VALUE_AGG_COLS: [&str; 4] = ["value_min", "value_max", "value_sum", "value_count"];

    // Keep the last raw sample per (series, time-bucket) via a **hash
    // aggregation**, not a ROW_NUMBER window + sort. DataFusion spills hash
    // aggregates cleanly, so memory is bounded by the group count (cardinality ×
    // buckets) regardless of day size — the window+double-sort plan held ~the
    // whole pool and OOMed nondeterministically on full days. Group by the
    // stored `prom_series_key` column (FR2 — the attributes Map can't be a GROUP
    // BY key; the write side materializes the key so the read side needs no UDF)
    // plus the bucket index `time / resolution`; `last_value(col ORDER BY time)`
    // picks each column from the latest-timestamp row in the bucket.
    let bucket = (cast(col("time_unix_nano"), Int64) / lit(resolution_ns)).alias("__bucket");
    // `name`/`service_name` are real group keys (emitted directly); every other
    // column — including the `attributes` Map, which `last_value` carries by
    // position (it never compares the value, only the ORDER BY time) — is taken
    // from the bucket's latest sample. ASC order ⇒ last = max time.
    let order = vec![col("time_unix_nano").sort(true, false)];
    // Which input columns exist on the scanned schema. Raw metric files vary by
    // subtype (a gauge-only file has no `int_value`; none carry value_*), so the
    // plan must reference only columns that are actually present.
    let present: HashSet<&str> =
        arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
    // last_value every input column except the group keys and the value_*
    // aggregates (which this plan computes; raw files never carry them).
    let mut aggrs: Vec<_> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|n| {
            *n != "name"
                && *n != "service_name"
                && *n != "prom_series_key"
                && !VALUE_AGG_COLS.contains(n)
        })
        .map(|n| {
            last_value_udaf()
                .call(vec![col(n)])
                .order_by(order.clone())
                .build()
                .map(|e| e.alias(n))
        })
        .collect::<Result<_, _>>()?;
    // Per-bucket aggregates over the coalesced scalar value (FR6), built only from
    // the scalar columns the input actually has. Histogram rows (or files with no
    // scalar column at all) yield a null scalar → min/max/sum null and count 0,
    // which is correct (histograms use the Last capability, not these).
    let scalar = match (present.contains("double_value"), present.contains("int_value")) {
        (true, true) => coalesce(vec![col("double_value"), cast(col("int_value"), Float64)]),
        (true, false) => col("double_value"),
        (false, true) => cast(col("int_value"), Float64),
        (false, false) => lit(ScalarValue::Float64(None)),
    };
    aggrs.push(min(scalar.clone()).alias("value_min"));
    aggrs.push(max(scalar.clone()).alias("value_max"));
    aggrs.push(sum(scalar.clone()).alias("value_sum"));
    // count(scalar) excludes nulls and yields Int64; cast to Float64 happens in
    // the projection below (DataFusion rejects a cast wrapping an aggregate).
    aggrs.push(count(scalar).alias("value_count"));
    let agg = table.aggregate(
        vec![
            col("name"),
            col("service_name"),
            col("prom_series_key"),
            bucket,
        ],
        aggrs,
    )?;
    // Project: the input columns (minus any value_* the input happened to carry),
    // then the four value_* aggregates appended exactly once — so the rollup file
    // always carries them regardless of the scanned schema. Sorted
    // (service_name, prom_name, time) so tier files prune like raw ones.
    let mut cols: Vec<_> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|n| !VALUE_AGG_COLS.contains(n))
        .map(col)
        .collect();
    cols.push(col("value_min"));
    cols.push(col("value_max"));
    cols.push(col("value_sum"));
    // value_count is cast Int64→Float64 to match the shared schema column.
    cols.push(cast(col("value_count"), Float64).alias("value_count"));
    Ok(agg.select(cols)?.sort(vec![
        col("service_name").sort(true, false),
        col("prom_name").sort(true, false),
        col("time_unix_nano").sort(true, false),
    ])?)
}

/// Whether `out` already reflects `sources` — true when the rollup file exists
/// and is at least as new as every source. Lets a sealed partition (whose data
/// never changes) be rolled up once and skipped thereafter. Uses mtime, like
/// the compactor's GC; a re-seal that rewrites the daily bumps its mtime and
/// invalidates the rollup.
fn rollup_is_current(out: &Path, sources: &[PathBuf]) -> crate::Result<bool> {
    let Ok(out_meta) = fs::metadata(out) else {
        return Ok(false); // no rollup yet
    };
    let out_mtime = out_meta.modified()?;
    for src in sources {
        if fs::metadata(src)?.modified()? > out_mtime {
            return Ok(false); // a source changed since the rollup was written
        }
    }
    Ok(true)
}

/// Generate a rollup file for a metric partition dir: downsample the partition's
/// **surviving** samples (compacted + non-superseded raw, via [`resolve_files`])
/// to `tier` and write `rollup-<tier>.parquet` (level 2, resolution = tier).
/// Reading the survivors (not raw-only) means the rollup is independent of the
/// raw retention/GC lifecycle — it can always be (re)built from the compacted
/// daily. Returns the row count written, `None` if there is nothing to roll or
/// the existing rollup is already current.
pub async fn generate_rollup(dir: &Path, tier: RollupTier) -> crate::Result<Option<usize>> {
    let sources = resolve_files(dir)?; // compacted + non-superseded raw; rollups excluded
    if sources.is_empty() {
        return Ok(None);
    }
    let out = dir.join(format!("rollup-{}.parquet", tier.label()));
    if rollup_is_current(&out, &sources)? {
        return Ok(None); // sealed-day data unchanged — skip the rewrite
    }
    let resolution_ns = tier.resolution_ns();
    if resolution_ns <= 0 {
        return Ok(None); // Raw tier writes no rollup file
    }
    // Bounded, spilling, single-partition session (same budget as the seal) so a
    // large compacted daily is downsampled without materialising it in RAM —
    // the rollup-path counterpart of the seal/merge OOM fix. Read the survivors
    // as a disk-streaming scan and stream the aggregation output to the writer.
    // Single partition: the rollup is a real aggregation (window + sort), not a
    // pre-sorted merge, so keep one partition to bound the spill reservation.
    let ctx = merge_ctx(MERGE_MEM_BUDGET_BYTES, Some(1))?;
    let paths: Vec<String> = sources.iter().map(|p| p.to_string_lossy().to_string()).collect();
    let scan = ctx.read_parquet(paths, ParquetReadOptions::default()).await?;
    let arrow_schema: datafusion::arrow::datatypes::SchemaRef =
        Arc::new(scan.schema().as_arrow().clone());
    let df = rollup_plan(scan, &arrow_schema, resolution_ns)?;
    let mut stream = df.execute_stream().await?;

    let (mut writer, staging) = open_staged_writer(&out, stream.schema())?;
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        rows += batch.num_rows();
        writer.write(&batch)?;
    }
    if rows == 0 {
        drop(writer);
        let _ = fs::remove_file(&staging);
        return Ok(None);
    }
    finalize_writer(writer, &staging, &out, 2, "", tier.label())?;
    super::telemetry::record_compaction(0, 1, rows as u64, std::time::Duration::from_secs(0));
    Ok(Some(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::querier::compaction::write_with_provenance;
    use datafusion::arrow::array::{AsArray, Float64Array, StringArray, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Float64Type, Schema, TimeUnit};

    #[test]
    fn test_select_tier_by_range_and_step() {
        let all = RollupTier::all();
        // 1h step → coarsest tier with res ≤ 1h is H1 (D1=1d is too coarse)
        assert_eq!(select_tier(H1_NS, &all), RollupTier::H1);
        // 2h step → still H1 (D1 too coarse)
        assert_eq!(select_tier(2 * H1_NS, &all), RollupTier::H1);
        // 1d step → D1
        assert_eq!(select_tier(D1_NS, &all), RollupTier::D1);
        // 5m step → M5
        assert_eq!(select_tier(M5_NS, &all), RollupTier::M5);
    }

    #[test]
    fn test_missing_tier_falls_back_to_raw() {
        // fine step: no tier resolution ≤ 1m → Raw
        assert_eq!(
            select_tier(60_000_000_000, &RollupTier::all()),
            RollupTier::Raw
        );
        // tier absent entirely → Raw
        assert_eq!(select_tier(H1_NS, &[]), RollupTier::Raw);
        // only M5 available, 1m step → M5 too coarse → Raw
        assert_eq!(
            select_tier(60_000_000_000, &[RollupTier::M5]),
            RollupTier::Raw
        );
        assert!(is_rollup_eligible("metrics") && !is_rollup_eligible("logs"));
    }

    fn counter_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            crate::querier::udf::tests::attributes_map_field(),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("double_value", DataType::Float64, true),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("prom_name", DataType::Utf8, false),
            Field::new("prom_series_key", DataType::Utf8, false),
            // int_value placed after prom_name to keep the positional column
            // indices the existing tests rely on (time=3, double=4, bucket=5).
            // Realistic raw input: NO value_* columns — the rollup plan adds them.
            Field::new("int_value", DataType::Int64, true),
        ]))
    }

    fn batch(times: &[i64], vals: &[f64], buckets: &[&str]) -> RecordBatch {
        let n = times.len();
        RecordBatch::try_new(
            counter_schema(),
            vec![
                Arc::new(StringArray::from(vec!["m"; n])),
                Arc::new(StringArray::from(vec!["s"; n])),
                crate::querier::udf::tests::json_map_array(&vec!["{}"; n]),
                Arc::new(TimestampNanosecondArray::from(times.to_vec()).with_timezone("UTC")),
                Arc::new(Float64Array::from(vals.to_vec())),
                Arc::new(StringArray::from(buckets.to_vec())),
                Arc::new(StringArray::from(vec!["m"; n])),
                // series key for `{}` attributes == "".
                Arc::new(StringArray::from(vec![""; n])),
                Arc::new(datafusion::arrow::array::Int64Array::from(
                    vec![None::<i64>; n],
                )),
            ],
        )
        .unwrap()
    }

    /// A gauge-only-style raw schema with `double_value` but NO `int_value`,
    /// mirroring the per-subtype / compaction fixtures (locks in the BUG 1 fix).
    fn double_only_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            crate::querier::udf::tests::attributes_map_field(),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
            Field::new("prom_series_key", DataType::Utf8, false),
        ]))
    }

    fn double_only_batch(times: &[i64], vals: &[f64]) -> RecordBatch {
        let n = times.len();
        RecordBatch::try_new(
            double_only_schema(),
            vec![
                Arc::new(StringArray::from(vec!["m"; n])),
                Arc::new(StringArray::from(vec!["s"; n])),
                crate::querier::udf::tests::json_map_array(&vec!["{}"; n]),
                Arc::new(TimestampNanosecondArray::from(times.to_vec()).with_timezone("UTC")),
                Arc::new(Float64Array::from(vals.to_vec())),
                Arc::new(StringArray::from(vec!["m"; n])),
                // series key for `{}` attributes == "".
                Arc::new(StringArray::from(vec![""; n])),
            ],
        )
        .unwrap()
    }

    /// Column index of a named field in `counter_schema`.
    fn col_idx(b: &RecordBatch, name: &str) -> usize {
        b.schema().index_of(name).unwrap()
    }

    /// Read a Float64 column across all rolled-up batches as a flat Vec.
    fn f64_col(bs: &[RecordBatch], name: &str) -> Vec<f64> {
        let mut out = Vec::new();
        for b in bs {
            let a = b.column(col_idx(b, name)).as_primitive::<Float64Type>();
            for i in 0..b.num_rows() {
                out.push(a.value(i));
            }
        }
        out
    }

    #[tokio::test]
    async fn test_rate_over_rollup_matches_raw() {
        // monotonic counter, several samples per 5m bucket
        let times = [
            0i64,
            60_000_000_000,
            120_000_000_000,
            300_000_000_000,
            360_000_000_000,
            600_000_000_000,
        ];
        let vals = [0.0, 2.0, 4.0, 10.0, 12.0, 20.0];
        let raw = batch(&times, &vals, &["[]"; 6]);
        let rolled = rollup_batches(vec![raw.clone()], M5_NS).await.unwrap();

        // rollup reduces 6 raw samples to 3 (one per 5m bucket)
        let rolled_rows: usize = rolled.iter().map(RecordBatch::num_rows).sum();
        assert!(
            (1..6).contains(&rolled_rows),
            "downsampled to {rolled_rows} rows"
        );

        #[allow(clippy::cast_precision_loss)] // ns→s over test-fixture timestamps
        let rate = |bs: &[RecordBatch]| {
            let mut pts: Vec<(i64, f64)> = Vec::new();
            for b in bs {
                let t = b
                    .column(3)
                    .as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>();
                let v = b.column(4).as_primitive::<Float64Type>();
                for i in 0..b.num_rows() {
                    pts.push((t.value(i), v.value(i)));
                }
            }
            pts.sort_by_key(|p| p.0);
            let (t0, v0) = pts[0];
            let (t1, v1) = *pts.last().unwrap();
            (v1 - v0) / ((t1 - t0) as f64 / 1e9)
        };
        let raw_rate = rate(&[raw]);
        let rollup_rate = rate(&rolled);
        assert!(
            (raw_rate - rollup_rate).abs() < 1e-6,
            "raw {raw_rate} vs rollup {rollup_rate}"
        );
    }

    #[tokio::test]
    async fn test_rollup_emits_per_bucket_aggregates() {
        // One 5m bucket, scalar values [1, 9, 4] at ascending times → one row with
        // min=1, max=9, sum=14, count=3, and double_value (last) = 4.
        let raw = batch(
            &[0, 60_000_000_000, 120_000_000_000],
            &[1.0, 9.0, 4.0],
            &["[]"; 3],
        );
        let rolled = rollup_batches(vec![raw], M5_NS).await.unwrap();
        let rows: usize = rolled.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "three samples in one 5m bucket → one rollup row");

        assert_eq!(f64_col(&rolled, "value_min"), vec![1.0]);
        assert_eq!(f64_col(&rolled, "value_max"), vec![9.0]);
        assert_eq!(f64_col(&rolled, "value_sum"), vec![14.0]);
        assert_eq!(f64_col(&rolled, "value_count"), vec![3.0]);
        assert_eq!(f64_col(&rolled, "double_value"), vec![4.0], "last sample");
    }

    #[tokio::test]
    async fn test_rollup_handles_schema_without_int_value() {
        // BUG 1: a gauge-only raw schema (double_value, no int_value) must roll up
        // successfully and still emit the four value_* aggregates — mirrors the
        // per-subtype / compaction case where rollup_plan must not reference
        // columns absent from the scanned schema.
        let raw = double_only_batch(&[0, 60_000_000_000, 120_000_000_000], &[2.0, 8.0, 5.0]);
        let rolled = rollup_batches(vec![raw], M5_NS).await.unwrap();
        let rows: usize = rolled.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "one 5m bucket → one rollup row");

        assert_eq!(f64_col(&rolled, "value_min"), vec![2.0]);
        assert_eq!(f64_col(&rolled, "value_max"), vec![8.0]);
        assert_eq!(f64_col(&rolled, "value_sum"), vec![15.0]);
        assert_eq!(f64_col(&rolled, "value_count"), vec![3.0]);
        // schema gained exactly the four value_* columns (none duplicated).
        let schema = rolled[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        for n in ["value_min", "value_max", "value_sum", "value_count"] {
            assert_eq!(names.iter().filter(|x| **x == n).count(), 1, "{n} once");
        }
    }

    #[tokio::test]
    async fn test_max_over_rollup_matches_raw() {
        // Several samples across two 5m buckets; MAX(value_max) over the tier rows
        // equals the max of all raw samples (peaks preserved).
        let times = [
            0i64,
            60_000_000_000,
            120_000_000_000,
            300_000_000_000,
            360_000_000_000,
            420_000_000_000,
        ];
        let vals = [3.0, 7.0, 2.0, 5.0, 11.0, 1.0];
        let raw = batch(&times, &vals, &["[]"; 6]);
        let rolled = rollup_batches(vec![raw], M5_NS).await.unwrap();

        let tier_max = f64_col(&rolled, "value_max")
            .into_iter()
            .fold(f64::MIN, f64::max);
        let raw_max = vals.iter().copied().fold(f64::MIN, f64::max);
        assert!(
            (tier_max - raw_max).abs() < 1e-9,
            "tier max {tier_max} vs raw {raw_max}"
        );
    }

    #[tokio::test]
    async fn test_avg_over_rollup_matches_raw() {
        // sum(value_sum)/sum(value_count) over the tier == mean of all raw samples.
        let times = [
            0i64,
            60_000_000_000,
            120_000_000_000,
            300_000_000_000,
            360_000_000_000,
            420_000_000_000,
        ];
        let vals = [3.0, 7.0, 2.0, 5.0, 11.0, 1.0];
        let raw = batch(&times, &vals, &["[]"; 6]);
        let rolled = rollup_batches(vec![raw], M5_NS).await.unwrap();

        let sum: f64 = f64_col(&rolled, "value_sum").iter().sum();
        let count: f64 = f64_col(&rolled, "value_count").iter().sum();
        let tier_avg = sum / count;
        #[allow(clippy::cast_precision_loss)] // small test-fixture length
        let raw_avg = vals.iter().sum::<f64>() / vals.len() as f64;
        assert!(
            (tier_avg - raw_avg).abs() < 1e-9,
            "tier avg {tier_avg} vs raw {raw_avg}"
        );
    }

    #[tokio::test]
    async fn test_rollup_plan_is_hash_aggregation_not_window() {
        // The fix: rollup must lower to a spillable AggregateExec, NOT a
        // window (WindowAggExec) + sort — that's what bounds its memory and
        // stops the nondeterministic ExternalSorterMerge OOM on full days.
        let ctx = SessionContext::new();
        let schema = counter_schema();
        ctx.register_table(
            "m",
            Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch(&[0, 1], &[1.0, 2.0], &["[]", "[]"])]])
                    .unwrap(),
            ),
        )
        .unwrap();
        let df = rollup_plan(ctx.table("m").await.unwrap(), &schema, M5_NS).unwrap();
        let display = datafusion::physical_plan::displayable(
            df.create_physical_plan().await.unwrap().as_ref(),
        )
        .indent(true)
        .to_string();
        assert!(
            display.contains("AggregateExec"),
            "rollup must be a hash aggregation, got:\n{display}"
        );
        assert!(
            !display.contains("WindowAggExec") && !display.contains("BoundedWindowAggExec"),
            "rollup must not use a window operator (un-spillable), got:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_rollup_groups_on_stored_column() {
        // FR2: the rollup groups on the stored `prom_series_key` column, not the
        // per-row `prom_series_key(attributes)` UDF. The logical plan groups on the
        // column (no UDF call form) and rolls up correctly.
        let ctx = SessionContext::new();
        let schema = counter_schema();
        ctx.register_table(
            "m",
            Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch(&[0, 1], &[1.0, 2.0], &["[]", "[]"])]])
                    .unwrap(),
            ),
        )
        .unwrap();
        let df = rollup_plan(ctx.table("m").await.unwrap(), &schema, M5_NS).unwrap();
        let plan = format!("{}", df.logical_plan().display_indent());
        assert!(
            plan.contains("prom_series_key"),
            "rollup groups on the stored column: {plan}"
        );
        assert!(
            !plan.contains("prom_series_key("),
            "no per-row prom_series_key UDF call in the rollup plan: {plan}"
        );
    }

    #[tokio::test]
    async fn test_rollup_preserves_bucket_counts() {
        // two histogram snapshots in the same 1h bucket; rollup keeps the last
        let bounds = [10.0, 20.0, 30.0, 40.0, 50.0];
        let early = "[0,5,5,5,5,5]";
        let late = "[0,20,30,30,15,5]"; // the snapshot the rollup must keep
        let raw = batch(&[0, 120_000_000_000], &[1.0, 2.0], &[early, late]);
        let rolled = rollup_batches(vec![raw], H1_NS).await.unwrap();

        let total: usize = rolled.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1, "two snapshots in one 1h bucket → one rollup row");
        let bc = rolled[0].column(5).as_string::<i32>();
        let counts: Vec<f64> = serde_json::from_str(bc.value(0)).unwrap();
        let expected: Vec<f64> = serde_json::from_str(late).unwrap();
        assert_eq!(
            counts, expected,
            "rollup keeps the last snapshot's bucket counts"
        );

        let q_rollup =
            super::super::prometheus::histogram_quantile(0.95, &counts, &bounds).unwrap();
        let q_raw = super::super::prometheus::histogram_quantile(0.95, &expected, &bounds).unwrap();
        assert!((q_rollup - q_raw).abs() < 1e-9, "quantile preserved");
    }

    #[tokio::test]
    async fn test_rollup_reads_compacted_when_raw_gone() {
        // B4: after GC reclaims raw, the day's data lives only in the compacted
        // daily. generate_rollup must still build the tier from it (raw-only
        // would find nothing and silently produce no rollup).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        fs::create_dir_all(&dir).unwrap();
        let b = batch(&[0, 120_000_000_000], &[1.0, 2.0], &["[]", "[]"]);
        // Only a compacted daily on disk (raw already deleted); it supersedes the
        // raw names, which no longer exist.
        write_with_provenance(
            &dir.join("compacted-2026-05-30.parquet"),
            counter_schema(),
            &[b],
            2,
            "08-00-00.parquet,08-02-00.parquet",
            "raw",
        )
        .unwrap();

        let rows = generate_rollup(&dir, RollupTier::H1).await.unwrap();
        assert_eq!(
            rows,
            Some(1),
            "rollup built from the compacted daily, raw absent"
        );
        assert!(dir.join("rollup-1h.parquet").exists());
    }

    #[tokio::test]
    async fn test_rollup_skips_when_current() {
        // B4b: a sealed partition is rolled up once; a second pass with no source
        // change is a no-op (the rollup is newer than its source).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        fs::create_dir_all(&dir).unwrap();
        write_with_provenance(
            &dir.join("compacted-2026-05-30.parquet"),
            counter_schema(),
            &[batch(&[0, 120_000_000_000], &[1.0, 2.0], &["[]", "[]"])],
            2,
            "08-00-00.parquet",
            "raw",
        )
        .unwrap();

        assert_eq!(
            generate_rollup(&dir, RollupTier::H1).await.unwrap(),
            Some(1),
            "first build"
        );
        assert_eq!(
            generate_rollup(&dir, RollupTier::H1).await.unwrap(),
            None,
            "second pass skipped — source unchanged"
        );
        assert!(dir.join("rollup-1h.parquet").exists(), "rollup retained");
    }

    #[tokio::test]
    async fn test_rollup_generated_for_metrics_only() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        fs::create_dir_all(&dir).unwrap();
        // write a raw file
        let b = batch(&[0, 60_000_000_000], &[1.0, 2.0], &["[]", "[]"]);
        let f = fs::File::create(dir.join("raw.parquet")).unwrap();
        let mut w =
            datafusion::parquet::arrow::ArrowWriter::try_new(f, counter_schema(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();

        // eligible signal → rollup produced
        assert!(is_rollup_eligible("metrics"));
        let rows = generate_rollup(&dir, RollupTier::H1).await.unwrap();
        assert!(rows.is_some(), "rollup generated");
        assert!(
            dir.join("rollup-1h.parquet").exists(),
            "rollup file written"
        );
        // logs are not eligible — the caller skips
        assert!(!is_rollup_eligible("logs"));
    }
}
