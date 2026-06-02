//! Standalone sealed-day compactor (task 10).
//!
//! A `Parquet → compacted Parquet` component (singleton role) that merges the
//! gateway's many small files into few large sorted files, bounding the
//! small-files problem ([FR7](../../../docs/workspace/parquet-backend/DESIGN.md#fr7)).
//!
//! Read/compact consistency is catalog-free ([compaction-consistency ADR](../../../docs/workspace/parquet-backend/adrs/compaction-consistency.md)):
//! - **Sealed-day cadence** — only partitions older than `grace` are compacted;
//!   the active day stays raw, so the compactor never races the gateway.
//! - **Footer provenance** — each compacted file records `sol.compaction.level`,
//!   `sol.compaction.supersedes` (the inputs it replaces) and
//!   `sol.compaction.resolution` in its Parquet footer, written atomically
//!   (staging `*.tmp` → `rename`). [`resolve_files`] prefers the highest level
//!   and skips superseded inputs, so a datum is read exactly once even while
//!   superseded inputs still exist — deleting them is pure GC.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use chrono::NaiveDate;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::prelude::SessionContext;

/// Footer key: compaction level (`0`=raw, `1`=day-merge, `2`=rollup…).
const LEVEL_KEY: &str = "sol.compaction.level";
/// Footer key: comma-separated input file names this file supersedes.
const SUPERSEDES_KEY: &str = "sol.compaction.supersedes";
/// Footer key: data resolution (`raw`/`5m`/`1h`/`1d`).
const RESOLUTION_KEY: &str = "sol.compaction.resolution";
/// Compacted-file name prefix.
const COMPACTED_PREFIX: &str = "compacted-";

fn err(msg: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::<dyn std::error::Error + Send + Sync>::from(msg.into())
}

/// Compactor policy.
#[derive(Debug, Clone, Copy)]
pub struct CompactorConfig {
    /// A partition is sealable once it is at least this many days old.
    pub grace_days: i64,
    /// Partitions older than this are deleted by retention GC.
    pub retention_days: i64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self { grace_days: 1, retention_days: 30 }
    }
}

/// Outcome of a seal run over one signal.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompactionReport {
    /// Partitions that were compacted this run.
    pub partitions_sealed: usize,
    /// Raw input files merged.
    pub files_input: usize,
    /// Compacted files written.
    pub files_output: usize,
    /// Rows written across the compacted files.
    pub rows: usize,
}

/// The standalone compactor over a storage root (`<root>/<signal>/dt=…/`).
pub struct Compactor {
    root: PathBuf,
    config: CompactorConfig,
}

impl Compactor {
    /// Create a compactor for `root` with the given policy.
    pub fn new(root: impl Into<PathBuf>, config: CompactorConfig) -> Self {
        Self { root: root.into(), config }
    }

    /// `dt=YYYY-MM-DD` partition dirs for a signal, with parsed dates.
    fn partition_dirs(&self, signal: &str) -> Vec<(NaiveDate, PathBuf)> {
        // Recursively find `dt=YYYY-MM-DD` partition dirs at any depth under the
        // signal root: `logs/dt=…`, `traces/dt=…`, and (task 14b) the nested
        // `metrics/<subtype>/dt=…`.
        let mut out = Vec::new();
        let mut stack = vec![self.root.join(signal)];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else { continue };
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
                if let Some(date_str) = name.strip_prefix("dt=")
                    && let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                {
                    out.push((date, path));
                } else {
                    stack.push(path); // descend (e.g. metrics/<subtype>/)
                }
            }
        }
        out.sort_by_key(|(d, _)| *d);
        out
    }

    /// Compact every sealable partition for `signal` (those at least
    /// `grace_days` old relative to `today`). The active day is left raw.
    pub async fn seal_signal(
        &self,
        signal: &str,
        today: NaiveDate,
    ) -> crate::Result<CompactionReport> {
        let start = Instant::now();
        let mut report = CompactionReport::default();
        for (date, dir) in self.partition_dirs(signal) {
            if (today - date).num_days() < self.config.grace_days {
                continue; // active / within-grace partition: untouched
            }
            if let Some((inputs, rows)) = self.seal_partition(&dir, date).await? {
                report.partitions_sealed += 1;
                report.files_input += inputs;
                report.files_output += 1;
                report.rows += rows;
            }
        }
        if report.partitions_sealed > 0 {
            super::telemetry::record_compaction(
                report.files_input as u64,
                report.files_output as u64,
                report.rows as u64,
                start.elapsed(),
            );
        }
        Ok(report)
    }

    /// Compact one partition dir. Returns `(inputs_merged, rows)` or `None` if
    /// there is nothing new to do (idempotent re-runs).
    async fn seal_partition(
        &self,
        dir: &Path,
        date: NaiveDate,
    ) -> crate::Result<Option<(usize, usize)>> {
        // Raw inputs not already superseded by an existing compacted file.
        let superseded = superseded_inputs(dir)?;
        let mut raw_inputs: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(dir)? .flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
            if name.ends_with(".parquet")
                && !name.starts_with(COMPACTED_PREFIX)
                && !superseded.contains(name)
            {
                raw_inputs.push(path);
            }
        }
        if raw_inputs.is_empty() {
            return Ok(None); // already compacted — idempotent
        }
        raw_inputs.sort();

        // Read + sort-merge all raw inputs.
        let mut batches: Vec<RecordBatch> = Vec::new();
        for path in &raw_inputs {
            batches.extend(read_batches(path)?);
        }
        if batches.is_empty() {
            return Ok(None);
        }
        let schema = batches[0].schema();
        let time_col = if schema.field_with_name("time_unix_nano").is_ok() {
            "time_unix_nano"
        } else {
            "start_time_unix_nano"
        };

        let ctx = SessionContext::new();
        let mem = MemTable::try_new(Arc::clone(&schema), vec![batches])?;
        ctx.register_table("t", Arc::new(mem))?;
        let df = ctx
            .sql(&format!("SELECT * FROM t ORDER BY service_name, {time_col}"))
            .await?;
        let sorted = df.collect().await?;
        let rows: usize = sorted.iter().map(RecordBatch::num_rows).sum();

        // Write to a staging file, then atomically rename into place.
        let input_names: Vec<String> = raw_inputs
            .iter()
            .filter_map(|p| p.file_name().and_then(|s| s.to_str()).map(String::from))
            .collect();
        let final_path = dir.join(format!("{COMPACTED_PREFIX}{date}.parquet"));
        write_with_provenance(&final_path, schema, &sorted, 1, &input_names.join(","), "raw")?;
        Ok(Some((raw_inputs.len(), rows)))
    }

    /// Delete partitions older than the retention policy. Returns files deleted.
    pub fn gc_retention(&self, signal: &str, today: NaiveDate) -> crate::Result<usize> {
        let mut deleted = 0;
        for (date, dir) in self.partition_dirs(signal) {
            if (today - date).num_days() > self.config.retention_days {
                for entry in fs::read_dir(&dir)?.flatten() {
                    if entry.path().is_file() {
                        fs::remove_file(entry.path())?;
                        deleted += 1;
                    }
                }
                fs::remove_dir_all(&dir).ok();
            }
        }
        if deleted > 0 {
            super::telemetry::record_retention_deleted(deleted as u64);
        }
        Ok(deleted)
    }

    /// Run one full compaction pass over all signals: seal sealed-day partitions,
    /// generate metric rollup tiers (when `rollups`), then retention GC. `today`
    /// is the current UTC date. This is the body the compactor daemon loops.
    pub async fn run_once(
        &self,
        today: NaiveDate,
        rollups: bool,
    ) -> crate::Result<CompactionReport> {
        let mut report = CompactionReport::default();
        for signal in ["logs", "traces", "metrics"] {
            let r = self.seal_signal(signal, today).await?;
            report.partitions_sealed += r.partitions_sealed;
            report.files_input += r.files_input;
            report.files_output += r.files_output;
            report.rows += r.rows;
        }
        if rollups {
            // Pre-aggregate sealed metric partitions into resolution tiers.
            for (date, dir) in self.partition_dirs("metrics") {
                if (today - date).num_days() < self.config.grace_days {
                    continue;
                }
                for tier in super::rollup::RollupTier::all() {
                    super::rollup::generate_rollup(&dir, tier).await?;
                }
            }
        }
        for signal in ["logs", "traces", "metrics"] {
            self.gc_retention(signal, today)?;
        }
        super::telemetry::set_compactor_lag(0.0); // caught up after a full pass
        Ok(report)
    }
}

/// Atomically write `batches` to `path` with compaction footer provenance:
/// stages to a hidden `.tmp` sibling and renames into place, so a crash
/// mid-write never leaves a visible partial file.
pub(crate) fn write_with_provenance(
    path: &Path,
    schema: Arc<datafusion::arrow::datatypes::Schema>,
    batches: &[RecordBatch],
    level: i32,
    supersedes: &str,
    resolution: &str,
) -> crate::Result<()> {
    let staging = path.with_file_name(format!(
        ".{}.tmp",
        path.file_name().and_then(|s| s.to_str()).unwrap_or("compacted.parquet")
    ));
    {
        let file = fs::File::create(&staging)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.append_key_value_metadata(KeyValue::new(LEVEL_KEY.into(), Some(level.to_string())));
        writer.append_key_value_metadata(KeyValue::new(
            SUPERSEDES_KEY.into(),
            Some(supersedes.to_string()),
        ));
        writer.append_key_value_metadata(KeyValue::new(
            RESOLUTION_KEY.into(),
            Some(resolution.to_string()),
        ));
        writer.close()?;
    }
    fs::rename(&staging, path)?;
    Ok(())
}

/// Read all record batches from a Parquet file.
pub(crate) fn read_batches(path: &Path) -> crate::Result<Vec<RecordBatch>> {
    let file = fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out = Vec::new();
    for batch in reader {
        out.push(batch.map_err(|e| err(e.to_string()))?);
    }
    Ok(out)
}

/// Read the `level`/`supersedes`/`resolution` footer metadata from a file.
fn read_provenance(path: &Path) -> crate::Result<(i32, Vec<String>)> {
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut level = 0;
    let mut supersedes = Vec::new();
    if let Some(kvs) = builder.metadata().file_metadata().key_value_metadata() {
        for kv in kvs {
            match kv.key.as_str() {
                LEVEL_KEY => level = kv.value.as_deref().and_then(|v| v.parse().ok()).unwrap_or(0),
                SUPERSEDES_KEY => {
                    if let Some(v) = &kv.value {
                        supersedes =
                            v.split(',').filter(|s| !s.is_empty()).map(String::from).collect();
                    }
                }
                _ => {}
            }
        }
    }
    Ok((level, supersedes))
}

/// The set of raw input names superseded by compacted files in `dir`.
fn superseded_inputs(dir: &Path) -> crate::Result<std::collections::HashSet<String>> {
    let mut set = std::collections::HashSet::new();
    for entry in fs::read_dir(dir)?.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
        if name.starts_with(COMPACTED_PREFIX) && name.ends_with(".parquet") {
            let (_, supersedes) = read_provenance(&path)?;
            set.extend(supersedes);
        }
    }
    Ok(set)
}

/// Querier-side file resolution for a partition dir (extends task 2): return
/// the compacted files plus the raw files **not** superseded by any of them, so
/// each datum is read exactly once. Highest level wins by construction (a
/// superseded raw input is dropped; recompaction supersedes lower levels).
pub fn resolve_files(dir: &Path) -> crate::Result<Vec<PathBuf>> {
    let superseded = superseded_inputs(dir)?;
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)?.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
        if !name.ends_with(".parquet") {
            continue; // skip staging *.tmp and anything else
        }
        if name.starts_with(COMPACTED_PREFIX) || !superseded.contains(name) {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array, StringArray, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("int_value", DataType::Int64, true),
        ]))
    }

    fn write_raw(dir: &Path, name: &str, svc: &[&str], ts: &[i64], vals: &[i64]) {
        fs::create_dir_all(dir).unwrap();
        let s = schema();
        let batch = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(StringArray::from(svc.to_vec())),
                Arc::new(TimestampNanosecondArray::from(ts.to_vec()).with_timezone("UTC")),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap();
        let f = fs::File::create(dir.join(name)).unwrap();
        let mut w = ArrowWriter::try_new(f, s, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    fn count_parquet(dir: &Path, compacted: bool) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|e| {
                let n = e.file_name().into_string().unwrap_or_default();
                n.ends_with(".parquet") && n.starts_with(COMPACTED_PREFIX) == compacted
            })
            .count()
    }

    #[tokio::test]
    async fn test_seal_only_compacts_partitions_older_than_grace() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let today = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        let active = root.join("metrics").join("dt=2026-06-01");
        let sealed = root.join("metrics").join("dt=2026-05-30");
        write_raw(&active, "a.parquet", &["s"], &[1], &[1]);
        write_raw(&sealed, "b.parquet", &["s"], &[1], &[1]);
        write_raw(&sealed, "c.parquet", &["s"], &[2], &[2]);

        let c = Compactor::new(root, CompactorConfig { grace_days: 1, retention_days: 30 });
        let report = c.seal_signal("metrics", today).await.unwrap();
        assert_eq!(report.partitions_sealed, 1, "only the sealed partition");
        assert_eq!(count_parquet(&active, true), 0, "active day not compacted");
        assert_eq!(count_parquet(&sealed, true), 1, "sealed day compacted");
    }

    #[tokio::test]
    async fn test_compacted_footer_records_level_and_supersedes() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "b.parquet", &["s"], &[2], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()).await.unwrap();

        let compacted = dir.join("compacted-2026-05-30.parquet");
        let (level, supersedes) = read_provenance(&compacted).unwrap();
        assert_eq!(level, 1);
        assert_eq!(supersedes, vec!["a.parquet".to_string(), "b.parquet".to_string()]);
    }

    #[tokio::test]
    async fn test_querier_skips_superseded_inputs() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "b.parquet", &["s"], &[2], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()).await.unwrap();

        // Raw inputs still on disk, but resolve_files returns only the compacted.
        let resolved = resolve_files(&dir).unwrap();
        let names: Vec<String> =
            resolved.iter().map(|p| p.file_name().unwrap().to_string_lossy().into()).collect();
        assert_eq!(names, vec!["compacted-2026-05-30.parquet".to_string()], "raw inputs skipped");
    }

    #[tokio::test]
    async fn test_staging_then_finalize_atomic() {
        // After a successful seal there is no leftover staging file.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["s"], &[1], &[1]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()).await.unwrap();
        let has_tmp = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .any(|e| e.file_name().to_string_lossy().ends_with(".tmp"));
        assert!(!has_tmp, "no leftover staging file after finalize");
        assert!(dir.join("compacted-2026-05-30.parquet").exists());
    }

    #[tokio::test]
    async fn test_compaction_merges_to_fewer_sorted_files_and_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        // unsorted across two files
        write_raw(&dir, "a.parquet", &["b", "a"], &[30, 10], &[3, 1]);
        write_raw(&dir, "b.parquet", &["a"], &[20], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        let today = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        c.seal_signal("metrics", today).await.unwrap();

        // 3 raw + 1 compacted on disk; querier sees only the 1 compacted.
        assert_eq!(resolve_files(&dir).unwrap().len(), 1, "merged to one file");
        let batches = read_batches(&dir.join("compacted-2026-05-30.parquet")).unwrap();
        let svc = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let services: Vec<&str> = (0..svc.len()).map(|i| svc.value(i)).collect();
        assert_eq!(services, vec!["a", "a", "b"], "globally sorted by service_name");

        // second run is a no-op (idempotent): inputs already superseded.
        let report2 = c.seal_signal("metrics", today).await.unwrap();
        assert_eq!(report2.partitions_sealed, 0, "idempotent re-run");
        assert_eq!(count_parquet(&dir, true), 1, "still exactly one compacted file");
    }

    #[tokio::test]
    async fn test_retention_gc_deletes_past_policy() {
        let tmp = tempfile::tempdir().unwrap();
        let old = tmp.path().join("metrics").join("dt=2026-01-01");
        let recent = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&old, "a.parquet", &["s"], &[1], &[1]);
        write_raw(&recent, "b.parquet", &["s"], &[1], &[1]);
        let c = Compactor::new(tmp.path(), CompactorConfig { grace_days: 1, retention_days: 30 });
        let today = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        let deleted = c.gc_retention("metrics", today).unwrap();
        assert_eq!(deleted, 1, "only the >30d partition deleted");
        assert!(!old.exists(), "old partition removed");
        assert!(recent.exists(), "recent partition kept");
    }

    #[tokio::test]
    async fn test_run_once_seals_all_signals_and_rolls_up_metrics() {
        let tmp = tempfile::tempdir().unwrap();
        // sealed-day raw across signals; metrics nested under a subtype dir (14b).
        let logs = tmp.path().join("logs").join("dt=2026-05-30");
        let metrics = tmp.path().join("metrics").join("gauge").join("dt=2026-05-30");
        write_raw(&logs, "a.parquet", &["s"], &[1], &[1]);
        // metrics fixture needs name + attributes (the rollup group-by key).
        {
            use datafusion::arrow::array::Float64Array;
            fs::create_dir_all(&metrics).unwrap();
            let s = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("service_name", DataType::Utf8, false),
                Field::new("attributes", DataType::Utf8, true),
                Field::new(
                    "time_unix_nano",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    false,
                ),
                Field::new("double_value", DataType::Float64, true),
            ]));
            let batch = RecordBatch::try_new(
                s.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["cpu", "cpu"])),
                    Arc::new(StringArray::from(vec!["s", "s"])),
                    Arc::new(StringArray::from(vec!["{}", "{}"])),
                    Arc::new(
                        TimestampNanosecondArray::from(vec![1000i64, 2000]).with_timezone("UTC"),
                    ),
                    Arc::new(Float64Array::from(vec![1.0, 2.0])),
                ],
            )
            .unwrap();
            let f = fs::File::create(metrics.join("m.parquet")).unwrap();
            let mut w = ArrowWriter::try_new(f, s, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let c = Compactor::new(tmp.path(), CompactorConfig { grace_days: 1, retention_days: 30 });
        let today = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        let report = c.run_once(today, true).await.unwrap();

        assert!(report.partitions_sealed >= 2, "logs + metrics sealed: {report:?}");
        assert!(logs.join("compacted-2026-05-30.parquet").exists(), "logs compacted");
        assert!(metrics.join("compacted-2026-05-30.parquet").exists(), "metrics compacted");
        // rollup tiers generated for the metric partition (14b nested dir reached)
        assert!(metrics.join("rollup-1h.parquet").exists(), "1h rollup generated");
    }
}
