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
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use super::compaction::{read_batches, write_with_provenance};

const M5_NS: i64 = 300_000_000_000;
const H1_NS: i64 = 3_600_000_000_000;
const D1_NS: i64 = 86_400_000_000_000;

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
    ctx.register_table("m", Arc::new(MemTable::try_new(schema, vec![batches])?))?;
    // Join each row to the max timestamp of its (series, bucket) → last sample.
    let sql = format!(
        "SELECT m.* FROM m JOIN (\
           SELECT name, service_name, attributes, \
             MAX(CAST(time_unix_nano AS BIGINT)) AS maxt \
           FROM m \
           GROUP BY name, service_name, attributes, CAST(time_unix_nano AS BIGINT) / {res}) g \
         ON m.name = g.name AND m.service_name = g.service_name \
         AND COALESCE(m.attributes, '') = COALESCE(g.attributes, '') \
         AND CAST(m.time_unix_nano AS BIGINT) = g.maxt \
         ORDER BY m.service_name, m.time_unix_nano",
        res = resolution_ns
    );
    let df = ctx.sql(&sql).await?;
    Ok(df.collect().await?)
}

/// Generate a rollup file for a metric partition dir: downsample its raw
/// samples to `tier` and write `rollup-<tier>.parquet` (level 2, resolution =
/// tier). Returns the row count written, or `None` if there is nothing to roll.
pub async fn generate_rollup(dir: &Path, tier: RollupTier) -> crate::Result<Option<usize>> {
    let mut batches = Vec::new();
    for entry in fs::read_dir(dir)?.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
        if name.ends_with(".parquet") && !name.starts_with("rollup-") && !name.starts_with("compacted-") {
            batches.extend(read_batches(&path)?);
        }
    }
    if batches.is_empty() {
        return Ok(None);
    }
    let rolled = rollup_batches(batches, tier.resolution_ns()).await?;
    if rolled.is_empty() {
        return Ok(None);
    }
    let rows: usize = rolled.iter().map(RecordBatch::num_rows).sum();
    let schema = rolled[0].schema();
    let out = dir.join(format!("rollup-{}.parquet", tier.label()));
    write_with_provenance(&out, schema, &rolled, 2, "", tier.label())?;
    super::telemetry::record_compaction(0, 1, rows as u64, std::time::Duration::from_secs(0));
    Ok(Some(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(select_tier(60_000_000_000, &RollupTier::all()), RollupTier::Raw);
        // tier absent entirely → Raw
        assert_eq!(select_tier(H1_NS, &[]), RollupTier::Raw);
        // only M5 available, 1m step → M5 too coarse → Raw
        assert_eq!(select_tier(60_000_000_000, &[RollupTier::M5]), RollupTier::Raw);
        assert!(is_rollup_eligible("metrics") && !is_rollup_eligible("logs"));
    }

    fn counter_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("double_value", DataType::Float64, true),
            Field::new("bucket_counts", DataType::Utf8, true),
        ]))
    }

    fn batch(times: &[i64], vals: &[f64], buckets: &[&str]) -> RecordBatch {
        let n = times.len();
        RecordBatch::try_new(
            counter_schema(),
            vec![
                Arc::new(StringArray::from(vec!["m"; n])),
                Arc::new(StringArray::from(vec!["s"; n])),
                Arc::new(StringArray::from(vec!["{}"; n])),
                Arc::new(TimestampNanosecondArray::from(times.to_vec()).with_timezone("UTC")),
                Arc::new(Float64Array::from(vals.to_vec())),
                Arc::new(StringArray::from(buckets.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_rate_over_rollup_matches_raw() {
        // monotonic counter, several samples per 5m bucket
        let times = [0i64, 60_000_000_000, 120_000_000_000, 300_000_000_000, 360_000_000_000, 600_000_000_000];
        let vals = [0.0, 2.0, 4.0, 10.0, 12.0, 20.0];
        let raw = batch(&times, &vals, &["[]"; 6]);
        let rolled = rollup_batches(vec![raw.clone()], M5_NS).await.unwrap();

        // rollup reduces 6 raw samples to 3 (one per 5m bucket)
        let rolled_rows: usize = rolled.iter().map(RecordBatch::num_rows).sum();
        assert!(rolled_rows < 6 && rolled_rows >= 1, "downsampled to {rolled_rows} rows");

        let rate = |bs: &[RecordBatch]| {
            let mut pts: Vec<(i64, f64)> = Vec::new();
            for b in bs {
                let t = b.column(3).as_primitive::<datafusion::arrow::datatypes::TimestampNanosecondType>();
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
        assert!((raw_rate - rollup_rate).abs() < 1e-6, "raw {raw_rate} vs rollup {rollup_rate}");
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
        assert_eq!(counts, expected, "rollup keeps the last snapshot's bucket counts");

        let q_rollup = super::super::prometheus::histogram_quantile(0.95, &counts, &bounds).unwrap();
        let q_raw = super::super::prometheus::histogram_quantile(0.95, &expected, &bounds).unwrap();
        assert!((q_rollup - q_raw).abs() < 1e-9, "quantile preserved");
    }

    #[tokio::test]
    async fn test_rollup_generated_for_metrics_only() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        fs::create_dir_all(&dir).unwrap();
        // write a raw file
        let b = batch(&[0, 60_000_000_000], &[1.0, 2.0], &["[]", "[]"]);
        let f = fs::File::create(dir.join("raw.parquet")).unwrap();
        let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(f, counter_schema(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();

        // eligible signal → rollup produced
        assert!(is_rollup_eligible("metrics"));
        let rows = generate_rollup(&dir, RollupTier::H1).await.unwrap();
        assert!(rows.is_some(), "rollup generated");
        assert!(dir.join("rollup-1h.parquet").exists(), "rollup file written");
        // logs are not eligible — the caller skips
        assert!(!is_rollup_eligible("logs"));
    }
}
