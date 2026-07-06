// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
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

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
#[cfg(test)]
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::SortExpr;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::file::metadata::KeyValue;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use futures::StreamExt;

/// Footer key: compaction level (`0`=raw, `1`=day-merge, `2`=rollup…).
const LEVEL_KEY: &str = "sol.compaction.level";
/// Footer key: comma-separated input file names this file supersedes.
const SUPERSEDES_KEY: &str = "sol.compaction.supersedes";
/// Footer key: data resolution (`raw`/`5m`/`1h`/`1d`).
const RESOLUTION_KEY: &str = "sol.compaction.resolution";
/// Compacted-file name prefix.
const COMPACTED_PREFIX: &str = "compacted-";
/// Prefix of downsampled rollup-tier files. These back the separate
/// `metrics_5m/1h/1d` tables and are NOT part of the lossless union, so they
/// are excluded from [`resolve_files`] (and never fed into a seal merge).
const ROLLUP_PREFIX: &str = "rollup-";
/// Memory budget for the seal/merge sort. The merge streams batches from disk
/// and **spills sorted runs to disk** past this cap, so peak RAM is bounded
/// regardless of how large a sealed day is — the fix for the midnight day-seal
/// OOM. Sized well under NFR5's cache budget so the compactor co-exists with
/// the querier on one host.
pub(crate) const MERGE_MEM_BUDGET_BYTES: usize = 128 * 1024 * 1024;
/// Batch size for the merge. A `SortPreservingMerge` buffers one batch per input
/// file simultaneously and **cannot spill** them, so its peak RAM is
/// `fan_in × batch_size × row_width`. Intraday compaction merges a whole hour of
/// raw files (≈100s of small files) at once, so the default 8192-row batch blew
/// the pool; a small batch keeps `fan_in × batch` bounded even at high fan-in.
pub(crate) const MERGE_BATCH_SIZE: usize = 1024;
/// Fan-in (input file count) above which the seal merge switches from the
/// streaming SPM (memory ∝ fan-in, un-spillable) to the bounded spilling
/// full-sort (memory independent of fan-in — it spills). This makes peak RAM
/// bounded at *any* ingest rate, not just the common case. Sized so
/// `MAX_SPM_FANIN × MERGE_BATCH_SIZE × row_width` stays well under the pool.
pub(crate) const MAX_SPM_FANIN: usize = 256;

/// Use the streaming SPM while fan-in fits the pool; spill above it.
pub(crate) fn merge_uses_spm(fan_in: usize) -> bool {
    fan_in <= MAX_SPM_FANIN
}

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
    /// Compact completed hours within the active day (leveled compaction).
    pub intraday: bool,
    /// Grace before a completed hour is compacted, for late-arriving data.
    pub hour_grace_secs: i64,
    /// Delete inputs once superseded (after `delete_grace_secs`).
    pub delete_superseded: bool,
    /// How long a superseding file must exist before its inputs are deleted.
    pub delete_grace_secs: i64,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        Self {
            grace_days: 1,
            retention_days: 30,
            intraday: true,
            hour_grace_secs: 600,
            delete_superseded: true,
            delete_grace_secs: 60,
        }
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
    /// Superseded input files deleted (disk reclamation).
    pub files_deleted: usize,
    /// Per-signal/per-rollup steps that failed this pass (logged, non-fatal).
    pub failures: usize,
}

impl CompactionReport {
    /// Fold a per-signal seal/intraday report into the pass total.
    fn merge(&mut self, o: CompactionReport) {
        self.partitions_sealed += o.partitions_sealed;
        self.files_input += o.files_input;
        self.files_output += o.files_output;
        self.rows += o.rows;
        self.files_deleted += o.files_deleted;
        self.failures += o.failures;
    }
}

/// The standalone compactor over a storage root (`<root>/<signal>/dt=…/`).
pub struct Compactor {
    root: PathBuf,
    config: CompactorConfig,
}

impl Compactor {
    /// Create a compactor for `root` with the given policy.
    pub fn new(root: impl Into<PathBuf>, config: CompactorConfig) -> Self {
        Self {
            root: root.into(),
            config,
        }
    }

    /// `dt=YYYY-MM-DD` partition dirs for a signal, with parsed dates.
    fn partition_dirs(&self, signal: &str) -> Vec<(NaiveDate, PathBuf)> {
        // Recursively find `dt=YYYY-MM-DD` partition dirs at any depth under the
        // signal root: `logs/dt=…`, `traces/dt=…`, and (task 14b) the nested
        // `metrics/<subtype>/dt=…`.
        let mut out = Vec::new();
        let mut stack = vec![self.root.join(signal)];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
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

    /// Day-seal one partition into a single level-2 file. Merges every
    /// surviving file the querier would read (leftover raw plus the hourly
    /// level-1 files from intra-day compaction), superseding them all. Returns
    /// `(inputs_merged, rows)` or `None` when already sealed (idempotent).
    async fn seal_partition(
        &self,
        dir: &Path,
        date: NaiveDate,
    ) -> crate::Result<Option<(usize, usize)>> {
        let daily = format!("{COMPACTED_PREFIX}{date}.parquet");
        // Data to carry forward, each datum exactly once: the surviving files
        // (`resolve_files` excludes superseded inputs and rollup tiers). This
        // INCLUDES any existing daily file, so a re-seal triggered by a
        // late-arriving raw preserves the daily's data instead of overwriting it.
        let read: Vec<PathBuf> = resolve_files(dir)?;
        // Idempotent: if the only survivor is the daily itself, it already
        // covers the partition (superseded raw may still be on disk awaiting gc,
        // but `resolve_files` excludes them) — re-sealing would churn the mtime.
        let has_new = read
            .iter()
            .any(|p| p.file_name().and_then(|s| s.to_str()) != Some(daily.as_str()));
        if !has_new {
            return Ok(None);
        }
        // Supersede *every* physical raw/hourly file (not just those merged this
        // pass): the new daily carries the old daily's data forward, so it fully
        // covers the partition. Next `resolve_files` returns only the daily, and
        // gc can reclaim every raw input.
        let mut supersede: Vec<String> = Vec::new();
        for entry in fs::read_dir(dir)?.flatten() {
            let Some(name) = entry
                .path()
                .file_name()
                .and_then(|s| s.to_str())
                .map(String::from)
            else {
                continue;
            };
            if name.ends_with(".parquet") && name != daily && !name.starts_with(ROLLUP_PREFIX) {
                supersede.push(name);
            }
        }
        supersede.sort();
        let rows = self.merge_inputs(dir, &read, &supersede, &daily, 2).await?;
        Ok(rows.map(|r| (supersede.len(), r)))
    }

    /// Sort-merge `read` into `dir/out_name` at compaction `level`, recording
    /// `supersede` in the `supersedes` footer. `read` is the set of files whose
    /// data to carry forward (each datum once); `supersede` is the set of files
    /// the output replaces (which may differ — a re-seal reads the prior daily
    /// for its data but supersedes the raw inputs, not itself). Returns rows
    /// written, or `None` when `read` holds no rows. Atomic (staging → rename).
    async fn merge_inputs(
        &self,
        dir: &Path,
        read: &[PathBuf],
        supersede: &[String],
        out_name: &str,
        level: i32,
    ) -> crate::Result<Option<usize>> {
        if read.is_empty() {
            return Ok(None);
        }
        // Inputs are all pre-sorted by the read-side key (codec sort-on-write +
        // prior compaction). Strategy by fan-in so peak RAM is bounded at ANY
        // fan-in (not just the common case):
        //   ≤ MAX_SPM_FANIN → streaming SortPreservingMerge — zero-resort k-way
        //     merge, but holds one un-spillable batch per input file, so memory
        //     ∝ fan-in. `None` keeps per-file partitions so the planner picks it.
        //   >  MAX_SPM_FANIN → bounded spilling full-sort — re-sorts (slower) but
        //     spills to disk, so memory is the spillable sort buffer, independent
        //     of fan-in. The backstop that makes OOM impossible at any ingest.
        // Either way the result streams to the writer batch-by-batch.
        let schema = read_schema(&read[0])?;
        let sort_exprs = sort_exprs_for_schema(&schema);
        let df = if merge_uses_spm(read.len()) {
            let ctx = merge_ctx(MERGE_MEM_BUDGET_BYTES, None)?;
            build_merge_df(&ctx, read, &sort_exprs).await?
        } else {
            let ctx = merge_ctx(MERGE_MEM_BUDGET_BYTES, Some(1))?;
            build_spilling_sort_df(&ctx, read, &sort_exprs).await?
        };
        let mut stream = df.execute_stream().await?;
        let out_path = dir.join(out_name);
        let (mut writer, staging) = open_staged_writer(&out_path, stream.schema())?;
        let mut rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| err(e.to_string()))?;
            rows += batch.num_rows();
            writer.write(&batch)?;
        }
        if rows == 0 {
            // No rows to carry forward — discard the staged file, write nothing
            // (matches the prior `batches.is_empty()` behaviour).
            drop(writer);
            let _ = fs::remove_file(&staging);
            return Ok(None);
        }
        finalize_writer(
            writer,
            &staging,
            &out_path,
            level,
            &supersede.join(","),
            "raw",
        )?;
        Ok(Some(rows))
    }

    /// Intra-day compaction: within the active (current-day) partition, merge
    /// each *completed* hour's raw files into one level-1 file, leaving the
    /// in-progress hour raw. An hour `H` is eligible once
    /// `now > end(H) + hour_grace_secs` (late-data watermark). Bounds the active
    /// day's file count so queriers open few files. No-op when `intraday` is off.
    pub async fn compact_active_day(
        &self,
        signal: &str,
        now: DateTime<Utc>,
    ) -> crate::Result<CompactionReport> {
        let mut report = CompactionReport::default();
        if !self.config.intraday {
            return Ok(report);
        }
        let start = Instant::now();
        let today = now.date_naive();
        let now_ns = now.timestamp_nanos_opt().unwrap_or(i64::MAX);
        let grace_ns = self.config.hour_grace_secs.saturating_mul(1_000_000_000);

        for (date, dir) in self.partition_dirs(signal) {
            if date != today {
                continue; // sealed/past days are handled by seal_signal
            }
            let superseded = superseded_inputs(&dir)?;
            // Group the not-yet-compacted raw files by their hour-of-day.
            let mut by_hour: BTreeMap<u32, Vec<PathBuf>> = BTreeMap::new();
            for entry in fs::read_dir(&dir)?.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
                if !name.ends_with(".parquet") || name.starts_with(COMPACTED_PREFIX) {
                    continue;
                }
                if superseded.contains(name) {
                    continue;
                }
                if let Some(hour) = parse_hour(name) {
                    by_hour.entry(hour).or_default().push(path);
                }
            }

            for (hour, mut inputs) in by_hour {
                // Only a fully-elapsed hour past the watermark; a lone file
                // gains nothing from a rewrite (the day-seal sweeps it up).
                if now_ns < hour_end_ns(date, hour) + grace_ns || inputs.len() < 2 {
                    continue;
                }
                inputs.sort();
                let out = format!("{COMPACTED_PREFIX}h{hour:02}-{date}.parquet");
                // Hourly merge supersedes exactly the raw files it absorbs.
                let names: Vec<String> = inputs
                    .iter()
                    .filter_map(|p| p.file_name().and_then(|s| s.to_str()).map(String::from))
                    .collect();
                if let Some(rows) = self.merge_inputs(&dir, &inputs, &names, &out, 1).await? {
                    report.partitions_sealed += 1;
                    report.files_input += inputs.len();
                    report.files_output += 1;
                    report.rows += rows;
                }
            }
        }
        if report.files_output > 0 {
            super::telemetry::record_compaction(
                report.files_input as u64,
                report.files_output as u64,
                report.rows as u64,
                start.elapsed(),
            );
        }
        Ok(report)
    }

    /// Delete inputs that a compacted file supersedes, once that compacted file
    /// has existed at least `delete_grace_secs` — long enough for every querier
    /// (which re-registers its file list every `refresh_interval_secs` and
    /// excludes superseded files) to have stopped referencing them. Returns the
    /// number of files deleted.
    ///
    /// Orphan-free: a superseder is always newer than its inputs, so whenever a
    /// higher-level file is old enough to authorize deletion, the mid-level
    /// files it supersedes are older still and their own inputs are collected in
    /// the same pass (all supersedes lists are read before any file is removed).
    pub fn gc_superseded(&self, signal: &str, now: DateTime<Utc>) -> crate::Result<usize> {
        if !self.config.delete_superseded {
            return Ok(0);
        }
        let now_s = now.timestamp();
        let mut deleted = 0;
        for (_date, dir) in self.partition_dirs(signal) {
            // Collect the inputs of every *aged* compacted file first, so
            // deleting one compacted file never loses another's provenance.
            let mut to_delete: std::collections::HashSet<String> = std::collections::HashSet::new();
            for entry in fs::read_dir(&dir)?.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
                if !name.starts_with(COMPACTED_PREFIX) || !name.ends_with(".parquet") {
                    continue;
                }
                let modified = fs::metadata(&path)?
                    .modified()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()
                    .and_then(|d| i64::try_from(d.as_secs()).ok())
                    .unwrap_or(0);
                if now_s - modified < self.config.delete_grace_secs {
                    continue; // too fresh — a querier may still reference its inputs
                }
                let (_, supersedes) = read_provenance(&path)?;
                to_delete.extend(supersedes);
            }
            for name in &to_delete {
                let path = dir.join(name);
                if path.is_file() {
                    fs::remove_file(&path)?;
                    deleted += 1;
                }
            }
        }
        Ok(deleted)
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
    ///
    /// **Resilient**: a single signal/partition/tier failure is logged and
    /// skipped (counted in `report.failures`), never aborting the pass — so GC
    /// always runs (no raw pile-up) and one bad rollup can't block the rest. The
    /// failed unit is idempotently retried next pass.
    pub async fn run_once(
        &self,
        now: DateTime<Utc>,
        rollups: bool,
    ) -> crate::Result<CompactionReport> {
        let today = now.date_naive();
        let mut report = CompactionReport::default();
        for signal in ["logs", "traces", "metrics"] {
            // Intra-day hourly compaction of the active day, then seal the
            // sealed days into a single daily file (leveled compaction).
            match self.compact_active_day(signal, now).await {
                Ok(r) => report.merge(r),
                Err(e) => {
                    report.failures += 1;
                    tracing::warn!("compactor: intraday compaction failed for {signal}: {e}");
                }
            }
            match self.seal_signal(signal, today).await {
                Ok(r) => report.merge(r),
                Err(e) => {
                    report.failures += 1;
                    tracing::warn!("compactor: seal failed for {signal}: {e}");
                }
            }
        }
        if rollups {
            // Pre-aggregate sealed metric partitions into resolution tiers.
            for (date, dir) in self.partition_dirs("metrics") {
                if (today - date).num_days() < self.config.grace_days {
                    continue;
                }
                for tier in super::rollup::RollupTier::all() {
                    if let Err(e) = super::rollup::generate_rollup(&dir, tier).await {
                        report.failures += 1;
                        tracing::warn!(
                            "compactor: rollup {tier:?} failed for {}: {e}",
                            dir.display()
                        );
                    }
                }
            }
        }
        for signal in ["logs", "traces", "metrics"] {
            match self.gc_superseded(signal, now) {
                Ok(n) => report.files_deleted += n,
                Err(e) => {
                    report.failures += 1;
                    tracing::warn!("compactor: gc_superseded failed for {signal}: {e}");
                }
            }
            if let Err(e) = self.gc_retention(signal, today) {
                report.failures += 1;
                tracing::warn!("compactor: gc_retention failed for {signal}: {e}");
            }
        }
        super::telemetry::set_compactor_lag(0.0); // a pass completed
        Ok(report)
    }
}

/// Parse the hour-of-day from a raw filename `HH-MM-SS.parquet`. Returns `None`
/// for any name that doesn't start with a two-digit hour < 24.
fn parse_hour(name: &str) -> Option<u32> {
    let hour: u32 = name.split('-').next()?.parse().ok()?;
    (hour < 24).then_some(hour)
}

/// Nanoseconds at the *end* of hour `hour` on `date` (UTC) — i.e. the start of
/// the next hour. Used for the intra-day compaction watermark.
fn hour_end_ns(date: NaiveDate, hour: u32) -> i64 {
    date.and_hms_opt(hour, 0, 0)
        .map(|dt| dt + ChronoDuration::hours(1))
        .and_then(|dt| dt.and_utc().timestamp_nanos_opt())
        .unwrap_or(i64::MAX)
}

/// Open a ZSTD-9 `ArrowWriter` over a hidden `.tmp` staging sibling of `path`.
/// Compaction is background (not latency-sensitive) and its output is read
/// repeatedly, so compress at a high level. The default `WriterProperties` is
/// UNCOMPRESSED — merging zstd raw files into an uncompressed file *grew*
/// on-disk size (a 107 MB compacted log file 7z'd to 8 MB before this fix).
pub(crate) fn open_staged_writer(
    path: &Path,
    schema: Arc<datafusion::arrow::datatypes::Schema>,
) -> crate::Result<(ArrowWriter<fs::File>, PathBuf)> {
    let staging = path.with_file_name(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("compacted.parquet")
    ));
    let file = fs::File::create(&staging)?;
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_compression(datafusion::parquet::basic::Compression::ZSTD(
            datafusion::parquet::basic::ZstdLevel::try_new(9).map_err(|e| err(e.to_string()))?,
        ))
        .build();
    let writer = ArrowWriter::try_new(file, schema, Some(props))?;
    Ok((writer, staging))
}

/// Append the compaction footer provenance, close the writer, then durably
/// publish the staged file: a later pass deletes the inputs this file
/// supersedes, so it must survive a crash. fsync the file, rename, then fsync
/// the directory entry — a crash mid-write never leaves a visible partial file.
pub(crate) fn finalize_writer(
    mut writer: ArrowWriter<fs::File>,
    staging: &Path,
    path: &Path,
    level: i32,
    supersedes: &str,
    resolution: &str,
) -> crate::Result<()> {
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
    fs::File::open(staging)?.sync_all()?;
    fs::rename(staging, path)?;
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Atomically write `batches` to `path` with compaction footer provenance.
/// Test-only since the live seal/rollup paths stream via `open_staged_writer`
/// + `finalize_writer`; tests use it to synthesise fixture files.
#[cfg(test)]
pub(crate) fn write_with_provenance(
    path: &Path,
    schema: Arc<datafusion::arrow::datatypes::Schema>,
    batches: &[RecordBatch],
    level: i32,
    supersedes: &str,
    resolution: &str,
) -> crate::Result<()> {
    let (mut writer, staging) = open_staged_writer(path, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    finalize_writer(writer, &staging, path, level, supersedes, resolution)
}

/// Read just the Arrow schema from a Parquet file's footer (no row groups).
fn read_schema(path: &Path) -> crate::Result<datafusion::arrow::datatypes::SchemaRef> {
    let file = fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(Arc::clone(builder.schema()))
}

/// Session for the seal/merge: a bounded `FairSpillPool` + an on-OS-temp
/// `DiskManager`, so the sort spills to disk instead of OOMing on a large
/// sealed day. Utf8View coercion is disabled so the merged output keeps the
/// codec's exact schema (Utf8/Binary) — otherwise the compacted file diverges
/// from the raw inputs the querier unions it with.
pub(crate) fn merge_ctx(
    mem_budget_bytes: usize,
    target_partitions: Option<usize>,
) -> crate::Result<SessionContext> {
    let mut config = datafusion::prelude::SessionConfig::new();
    config.options_mut().execution.parquet.schema_force_view_types = false;
    // Keep DataFusion's *default* `sort_spill_reservation_bytes` (10 MB). An
    // inflated reservation (we briefly used mem/4 = 32 MB) is held aside per
    // SortExec and, in the rollup's sort-heavy pipeline within a tight pool,
    // becomes un-obtainable → `ExternalSorterMerge` ResourcesExhausted. The small
    // default is reliably obtainable and DataFusion multi-pass-merges if needed.
    // Cap the batch size so a high-fan-in SortPreservingMerge (one un-spillable
    // batch per input file) stays within the pool — see MERGE_BATCH_SIZE.
    config.options_mut().execution.batch_size = MERGE_BATCH_SIZE;
    // `Some(1)` forces a SINGLE partition — for a *full* sort (the rollup
    // aggregation) this avoids N concurrent partition-sorts each reserving spill
    // headroom up front (N × reservation blows the pool before anything spills).
    // The seal passes `None` (default ≈ #CPUs) so the planner keeps the per-file
    // scans as separate ordered partitions and lowers to a streaming
    // `SortPreservingMerge` rather than coalescing them into a buffering
    // `SortExec`.
    if let Some(n) = target_partitions {
        config.options_mut().execution.target_partitions = n;
    }
    let rt = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_disk_manager_builder(datafusion::execution::disk_manager::DiskManagerBuilder::default())
        .with_memory_pool(Arc::new(
            datafusion::execution::memory_pool::FairSpillPool::new(mem_budget_bytes),
        ))
        .build_arc()?;
    Ok(SessionContext::new_with_config_rt(config, rt))
}

/// The read-side sort key for a signal's schema: `(service_name, [prom_name],
/// time)` — `prom_name` only for metrics; time is `time_unix_nano` when present
/// (logs/metrics) else `start_time_unix_nano` (traces). All ascending,
/// nulls-last (`sort(true, false)`), byte-identical to what the codec writes.
pub(crate) fn sort_exprs_for_schema(
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Vec<SortExpr> {
    use datafusion::prelude::col;
    let time_col = if schema.field_with_name("time_unix_nano").is_ok() {
        "time_unix_nano"
    } else {
        "start_time_unix_nano"
    };
    let mut exprs = vec![col("service_name").sort(true, false)];
    if schema.field_with_name("prom_name").is_ok() {
        exprs.push(col("prom_name").sort(true, false));
    }
    exprs.push(col(time_col).sort(true, false));
    exprs
}

/// Build the **zero-resort k-way merge** plan over the input files. Every file
/// is already sorted by `sort_exprs` on write (`sort_dp_rows` for metrics,
/// `sort_logs`/`sort_spans` for logs/traces; compacted files by this same key) —
/// so each single-file scan declares its `file_sort_order`, and the final
/// `.sort()` lowers to a streaming `SortPreservingMergeExec` (zero re-sort,
/// `O(k·batch)` memory) rather than a buffering `SortExec`. Requires every input
/// to actually be sorted — the empty-state / sort-on-write cutover contract (see
/// the sort-on-write-cutover ADR). `merge_ctx(None)` keeps the per-file
/// partitions so the planner picks the SPM.
pub(crate) async fn build_merge_df(
    ctx: &SessionContext,
    read: &[PathBuf],
    sort_exprs: &[SortExpr],
) -> crate::Result<DataFrame> {
    let mut acc: Option<DataFrame> = None;
    for path in read {
        let opts = ParquetReadOptions::default().file_sort_order(vec![sort_exprs.to_vec()]);
        let df = ctx
            .read_parquet(path.to_string_lossy().to_string(), opts)
            .await?;
        acc = Some(match acc {
            Some(prev) => prev.union(df)?,
            None => df,
        });
    }
    let df = acc.ok_or_else(|| err("merge: no input files"))?;
    Ok(df.sort(sort_exprs.to_vec())?)
}

/// The bounded **spilling full-sort** fallback for high fan-in. Reads all inputs
/// as one sequential scan (`merge_ctx(Some(1))` → single partition, so the scan
/// reads files one at a time and the single `SortExec` doesn't fan out spill
/// reservations) and sorts with spill-to-disk. Inputs are already sorted, so
/// this re-sorts redundantly (slower) — but its peak RAM is the spillable sort
/// buffer, **independent of fan-in**, so it cannot OOM however many files it gets.
pub(crate) async fn build_spilling_sort_df(
    ctx: &SessionContext,
    read: &[PathBuf],
    sort_exprs: &[SortExpr],
) -> crate::Result<DataFrame> {
    if read.is_empty() {
        return Err(err("merge: no input files"));
    }
    let paths: Vec<String> = read
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    let df = ctx
        .read_parquet(paths, ParquetReadOptions::default())
        .await?;
    Ok(df.sort(sort_exprs.to_vec())?)
}

/// Read all record batches from a Parquet file. Test-only: the live paths
/// stream via `read_parquet` rather than buffering whole files.
#[cfg(test)]
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
                LEVEL_KEY => {
                    level = kv
                        .value
                        .as_deref()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0)
                }
                SUPERSEDES_KEY => {
                    if let Some(v) = &kv.value {
                        supersedes = v
                            .split(',')
                            .filter(|s| !s.is_empty())
                            .map(String::from)
                            .collect();
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
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
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
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.ends_with(".parquet") || name.starts_with(ROLLUP_PREFIX) {
            continue; // skip staging *.tmp, and rollup tiers (separate tables)
        }
        // Drop any superseded file regardless of level — with leveled
        // compaction a daily file supersedes the hourly files, which in turn
        // supersede the raw inputs, so supersession must be transitive.
        if !superseded.contains(name) {
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
            Arc::clone(&s),
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

        let c = Compactor::new(
            root,
            CompactorConfig {
                grace_days: 1,
                retention_days: 30,
                ..Default::default()
            },
        );
        let report = c.seal_signal("metrics", today).await.unwrap();
        assert_eq!(report.partitions_sealed, 1, "only the sealed partition");
        assert_eq!(count_parquet(&active, true), 0, "active day not compacted");
        assert_eq!(count_parquet(&sealed, true), 1, "sealed day compacted");
    }

    #[tokio::test]
    async fn test_seal_streaming_merge_globally_sorts_overlapping_inputs() {
        // Two pre-sorted files whose service ranges INTERLEAVE (a,c | b,d): a
        // naive concatenation is a,c,b,d (not sorted) — only a real merge yields
        // the globally sorted a,b,c,d. Proves the streaming merge is correct.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["a", "c"], &[1, 1], &[1, 3]);
        write_raw(&dir, "b.parquet", &["b", "d"], &[1, 1], &[2, 4]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
            .await
            .unwrap();

        let batches = read_batches(&dir.join("compacted-2026-05-30.parquet")).unwrap();
        let mut svcs = Vec::new();
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..col.len() {
                svcs.push(col.value(i).to_string());
            }
        }
        assert_eq!(
            svcs,
            vec!["a", "b", "c", "d"],
            "merge must globally sort by service_name across files, not concat"
        );
    }

    #[tokio::test]
    async fn test_merge_ctx_pool_is_bounded_so_sort_spills() {
        // The day-seal OOM came from an unbounded memory pool. merge_ctx must
        // cap the pool (paired with its DiskManager, that is what makes the
        // external sort spill to disk instead of buffering the whole partition).
        use datafusion::execution::memory_pool::MemoryConsumer;
        let ctx = merge_ctx(1 << 20, Some(1)).unwrap(); // 1 MiB budget
        let pool = Arc::clone(&ctx.task_ctx().runtime_env().memory_pool);
        let reservation = MemoryConsumer::new("test").register(&pool);
        assert!(
            reservation.try_grow(8 << 20).is_err(),
            "merge pool must be bounded to its budget; an unbounded pool OOMs the day-seal"
        );
        // Some(1) → single partition (the full-sort caller: rollup).
        assert_eq!(
            ctx.copied_config().options().execution.target_partitions,
            1,
            "Some(1) must force one partition so spill reservations don't multiply"
        );
        // Small batch caps a high-fan-in SortPreservingMerge's un-spillable
        // per-input buffers (fan_in × batch_size) within the pool.
        assert_eq!(
            ctx.copied_config().options().execution.batch_size,
            MERGE_BATCH_SIZE,
            "merge must use the capped batch size to bound SPM fan-in memory"
        );
    }

    #[tokio::test]
    async fn test_seal_merge_plan_is_sort_preserving_merge() {
        // Pre-sorted inputs (the cutover contract) must lower to a streaming
        // SortPreservingMerge — a zero-resort k-way merge — not a buffering
        // SortExec. merge_ctx(None) keeps per-file partitions for the SPM.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["a", "c"], &[1, 1], &[1, 3]);
        write_raw(&dir, "b.parquet", &["b", "d"], &[1, 1], &[2, 4]);
        let ctx = merge_ctx(MERGE_MEM_BUDGET_BYTES, None).unwrap();
        let read = vec![dir.join("a.parquet"), dir.join("b.parquet")];
        let schema = read_schema(&read[0]).unwrap();
        let df = build_merge_df(&ctx, &read, &sort_exprs_for_schema(&schema))
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let display = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(
            display.contains("SortPreservingMerge"),
            "expected a streaming SortPreservingMerge, got:\n{display}"
        );
        assert!(
            !display.contains("SortExec"),
            "must not buffer/full-sort pre-sorted inputs, got:\n{display}"
        );
    }

    #[test]
    fn test_merge_uses_spm_below_threshold_only() {
        assert!(merge_uses_spm(1), "tiny fan-in uses SPM");
        assert!(merge_uses_spm(MAX_SPM_FANIN), "at the cap, still SPM");
        assert!(
            !merge_uses_spm(MAX_SPM_FANIN + 1),
            "above the cap, fall back to the spilling sort"
        );
    }

    #[tokio::test]
    async fn test_spilling_fallback_plan_is_buffering_sort() {
        // The high-fan-in fallback must be a real (spilling) SortExec, NOT an
        // SPM — that's what makes its memory independent of fan-in.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["a", "c"], &[1, 1], &[1, 3]);
        write_raw(&dir, "b.parquet", &["b", "d"], &[1, 1], &[2, 4]);
        let ctx = merge_ctx(MERGE_MEM_BUDGET_BYTES, Some(1)).unwrap();
        let read = vec![dir.join("a.parquet"), dir.join("b.parquet")];
        let schema = read_schema(&read[0]).unwrap();
        let df = build_spilling_sort_df(&ctx, &read, &sort_exprs_for_schema(&schema))
            .await
            .unwrap();
        let display = datafusion::physical_plan::displayable(
            df.create_physical_plan().await.unwrap().as_ref(),
        )
        .indent(true)
        .to_string();
        assert!(
            display.contains("SortExec"),
            "fallback must be a spilling SortExec, got:\n{display}"
        );
        assert!(
            !display.contains("SortPreservingMerge"),
            "fallback must not be an SPM (whose memory grows with fan-in):\n{display}"
        );
    }

    #[tokio::test]
    async fn test_seal_high_fanin_falls_back_and_completes() {
        // Fan-in above MAX_SPM_FANIN takes the spilling path and still produces
        // a complete, globally-sorted output — the fan-in-independent guarantee.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        let n = MAX_SPM_FANIN + 1;
        for i in 0..n {
            // one row per file; services in reverse file order so a correct
            // merge/sort is required to produce ascending output.
            let svc = format!("s{:04}", n - 1 - i);
            #[allow(clippy::cast_possible_wrap)] // small loop index test fixture
            write_raw(
                &dir,
                &format!("{i:05}-00-00.parquet"),
                &[&svc],
                &[i as i64],
                &[i as i64],
            );
        }
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
            .await
            .unwrap();

        let batches = read_batches(&dir.join("compacted-2026-05-30.parquet")).unwrap();
        let mut svcs = Vec::new();
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..col.len() {
                svcs.push(col.value(i).to_string());
            }
        }
        assert_eq!(svcs.len(), n, "all rows preserved through the fallback");
        let mut sorted = svcs.clone();
        sorted.sort();
        assert_eq!(svcs, sorted, "fallback output is globally sorted");
    }

    #[tokio::test]
    async fn test_seal_merges_many_files_single_partition() {
        // The real failure was a many-file partition (logs: ~136 files): N
        // partition-sorts × per-sort spill reservation exhausted the pool. With
        // one partition this seals correctly to a sorted, complete output.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        let files = 12;
        for f in 0..files {
            // interleaved services across files so a correct merge is required
            write_raw(
                &dir,
                &format!("{f:02}-00-00.parquet"),
                &["a", "m", "z"],
                &[f, f, f],
                &[f, f, f],
            );
        }
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
            .await
            .unwrap();

        let batches = read_batches(&dir.join("compacted-2026-05-30.parquet")).unwrap();
        let mut svcs = Vec::new();
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..col.len() {
                svcs.push(col.value(i).to_string());
            }
        }
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)] // non-negative test count
        let expected_rows = (files as usize) * 3;
        assert_eq!(svcs.len(), expected_rows, "all rows preserved");
        let mut sorted = svcs.clone();
        sorted.sort();
        assert_eq!(svcs, sorted, "globally sorted across all files");
    }

    #[tokio::test]
    async fn test_merge_streams_large_input_correctly() {
        // A large pre-sorted input streams through (execute_stream, no collect)
        // to a complete, globally-sorted output.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        let n: i64 = 50_000;
        let svc: Vec<String> = (0..n).map(|i| format!("svc-{i:06}")).collect();
        let svc_ref: Vec<&str> = svc.iter().map(String::as_str).collect();
        let ts: Vec<i64> = (0..n).collect();
        let vals: Vec<i64> = (0..n).collect();
        write_raw(&dir, "a.parquet", &svc_ref, &ts, &vals);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
            .await
            .unwrap();

        let batches = read_batches(&dir.join("compacted-2026-05-30.parquet")).unwrap();
        let mut count = 0usize;
        let mut last: Option<String> = None;
        let mut sorted = true;
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..col.len() {
                let v = col.value(i).to_string();
                if last.as_ref().is_some_and(|p| &v < p) {
                    sorted = false;
                }
                last = Some(v);
                count += 1;
            }
        }
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)] // non-negative test count
        let expected_n = n as usize;
        assert_eq!(count, expected_n, "all rows preserved");
        assert!(sorted, "output globally sorted");
    }

    #[tokio::test]
    async fn test_compacted_footer_records_level_and_supersedes() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "b.parquet", &["s"], &[2], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
            .await
            .unwrap();

        let compacted = dir.join("compacted-2026-05-30.parquet");
        let (level, supersedes) = read_provenance(&compacted).unwrap();
        assert_eq!(level, 2, "day-seal is the level-2 tier");
        assert_eq!(
            supersedes,
            vec!["a.parquet".to_string(), "b.parquet".to_string()]
        );
        // Compacted output must be ZSTD, not the default UNCOMPRESSED (else
        // compaction grows on-disk size vs the zstd raw inputs).
        let f = fs::File::open(&compacted).unwrap();
        let md = Arc::clone(
            ParquetRecordBatchReaderBuilder::try_new(f)
                .unwrap()
                .metadata(),
        );
        let codec = md.row_group(0).column(0).compression();
        assert!(
            matches!(codec, datafusion::parquet::basic::Compression::ZSTD(_)),
            "compacted output must be ZSTD, got {codec:?}"
        );
    }

    #[tokio::test]
    async fn test_querier_skips_superseded_inputs() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "b.parquet", &["s"], &[2], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
            .await
            .unwrap();

        // Raw inputs still on disk, but resolve_files returns only the compacted.
        let resolved = resolve_files(&dir).unwrap();
        let names: Vec<String> = resolved
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into())
            .collect();
        assert_eq!(
            names,
            vec!["compacted-2026-05-30.parquet".to_string()],
            "raw inputs skipped"
        );
    }

    #[tokio::test]
    async fn test_staging_then_finalize_atomic() {
        // After a successful seal there is no leftover staging file.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&dir, "a.parquet", &["s"], &[1], &[1]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
            .await
            .unwrap();
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
        // two pre-sorted files (the codec's sort-on-write contract)
        write_raw(&dir, "a.parquet", &["a", "b"], &[10, 30], &[1, 3]);
        write_raw(&dir, "b.parquet", &["a"], &[20], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        let today = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        c.seal_signal("metrics", today).await.unwrap();

        // 3 raw + 1 compacted on disk; querier sees only the 1 compacted.
        assert_eq!(resolve_files(&dir).unwrap().len(), 1, "merged to one file");
        let batches = read_batches(&dir.join("compacted-2026-05-30.parquet")).unwrap();
        let svc = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let services: Vec<&str> = (0..svc.len()).map(|i| svc.value(i)).collect();
        assert_eq!(
            services,
            vec!["a", "a", "b"],
            "globally sorted by service_name"
        );

        // second run is a no-op (idempotent): inputs already superseded.
        let report2 = c.seal_signal("metrics", today).await.unwrap();
        assert_eq!(report2.partitions_sealed, 0, "idempotent re-run");
        assert_eq!(
            count_parquet(&dir, true),
            1,
            "still exactly one compacted file"
        );
    }

    #[tokio::test]
    async fn test_retention_gc_deletes_past_policy() {
        let tmp = tempfile::tempdir().unwrap();
        let old = tmp.path().join("metrics").join("dt=2026-01-01");
        let recent = tmp.path().join("metrics").join("dt=2026-05-30");
        write_raw(&old, "a.parquet", &["s"], &[1], &[1]);
        write_raw(&recent, "b.parquet", &["s"], &[1], &[1]);
        let c = Compactor::new(
            tmp.path(),
            CompactorConfig {
                grace_days: 1,
                retention_days: 30,
                ..Default::default()
            },
        );
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
        let metrics = tmp
            .path()
            .join("metrics")
            .join("gauge")
            .join("dt=2026-05-30");
        write_raw(&logs, "a.parquet", &["s"], &[1], &[1]);
        // metrics fixture needs name + attributes (the rollup group-by key).
        {
            use datafusion::arrow::array::Float64Array;
            fs::create_dir_all(&metrics).unwrap();
            let s = Arc::new(Schema::new(vec![
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
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&s),
                vec![
                    Arc::new(StringArray::from(vec!["cpu", "cpu"])),
                    Arc::new(StringArray::from(vec!["s", "s"])),
                    crate::querier::udf::tests::json_map_array(&["{}", "{}"]),
                    Arc::new(
                        TimestampNanosecondArray::from(vec![1000i64, 2000]).with_timezone("UTC"),
                    ),
                    Arc::new(Float64Array::from(vec![1.0, 2.0])),
                    Arc::new(StringArray::from(vec!["cpu", "cpu"])),
                ],
            )
            .unwrap();
            let f = fs::File::create(metrics.join("m.parquet")).unwrap();
            let mut w = ArrowWriter::try_new(f, s, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let c = Compactor::new(
            tmp.path(),
            CompactorConfig {
                grace_days: 1,
                retention_days: 30,
                ..Default::default()
            },
        );
        let today = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        let now = today.and_hms_opt(12, 0, 0).unwrap().and_utc();
        let report = c.run_once(now, true).await.unwrap();

        assert!(
            report.partitions_sealed >= 2,
            "logs + metrics sealed: {report:?}"
        );
        assert!(
            logs.join("compacted-2026-05-30.parquet").exists(),
            "logs compacted"
        );
        assert!(
            metrics.join("compacted-2026-05-30.parquet").exists(),
            "metrics compacted"
        );
        // rollup tiers generated for the metric partition (14b nested dir reached)
        assert!(
            metrics.join("rollup-1h.parquet").exists(),
            "1h rollup generated"
        );
    }

    #[tokio::test]
    async fn test_intraday_compacts_completed_hours_only() {
        let tmp = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-06-02");
        // hour 08: two completed raw files; hour 10: the in-progress hour.
        write_raw(&dir, "08-00-00.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "08-30-00.parquet", &["s"], &[2], &[2]);
        write_raw(&dir, "10-00-00.parquet", &["s"], &[3], &[3]);
        write_raw(&dir, "10-30-00.parquet", &["s"], &[4], &[4]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        // now = 10:30 same day: hour 08 is past the watermark, hour 10 is live.
        let now = date.and_hms_opt(10, 30, 0).unwrap().and_utc();
        let report = c.compact_active_day("metrics", now).await.unwrap();

        assert_eq!(report.files_output, 1, "only the completed hour compacted");
        assert!(dir.join("compacted-h08-2026-06-02.parquet").exists());
        let names: Vec<String> = resolve_files(&dir)
            .unwrap()
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into())
            .collect();
        assert!(
            names.contains(&"compacted-h08-2026-06-02.parquet".to_string()),
            "{names:?}"
        );
        assert!(
            names.contains(&"10-00-00.parquet".to_string()),
            "current hour raw kept: {names:?}"
        );
        assert!(
            !names.iter().any(|n| n.starts_with("08-")),
            "hour-08 raw superseded: {names:?}"
        );
    }

    #[tokio::test]
    async fn test_intraday_respects_watermark() {
        let tmp = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let dir = tmp.path().join("logs").join("dt=2026-06-02");
        write_raw(&dir, "09-00-00.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "09-30-00.parquet", &["s"], &[2], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        // 10:05 — hour 09 ended only 5 min ago (< 10-min grace): not yet.
        let early = date.and_hms_opt(10, 5, 0).unwrap().and_utc();
        assert_eq!(
            c.compact_active_day("logs", early)
                .await
                .unwrap()
                .files_output,
            0
        );
        // 10:11 — past the watermark: eligible.
        let late = date.and_hms_opt(10, 11, 0).unwrap().and_utc();
        assert_eq!(
            c.compact_active_day("logs", late)
                .await
                .unwrap()
                .files_output,
            1
        );
    }

    #[tokio::test]
    async fn test_day_seal_merges_hourly_into_single_daily() {
        // Leveled chain raw -> hourly (L1) -> daily (L2); querier sees only L2.
        let tmp = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-06-02");
        write_raw(&dir, "08-00-00.parquet", &["b"], &[10], &[1]);
        write_raw(&dir, "08-30-00.parquet", &["a"], &[20], &[2]);
        write_raw(&dir, "09-00-00.parquet", &["a"], &[30], &[3]);
        write_raw(&dir, "09-30-00.parquet", &["c"], &[40], &[4]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());

        let now = date.and_hms_opt(12, 0, 0).unwrap().and_utc();
        c.compact_active_day("metrics", now).await.unwrap();
        assert_eq!(count_parquet(&dir, true), 2, "two hourly level-1 files");

        // Next day the partition seals: hourly files merge into one daily file.
        let next_day = NaiveDate::from_ymd_opt(2026, 6, 3).unwrap();
        c.seal_signal("metrics", next_day).await.unwrap();
        let names: Vec<String> = resolve_files(&dir)
            .unwrap()
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into())
            .collect();
        assert_eq!(
            names,
            vec!["compacted-2026-06-02.parquet".to_string()],
            "transitive supersession: only the daily survives"
        );
        let (level, _) = read_provenance(&dir.join("compacted-2026-06-02.parquet")).unwrap();
        assert_eq!(level, 2);
        let batches = read_batches(&dir.join("compacted-2026-06-02.parquet")).unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4, "no data lost across the level chain");
    }

    #[tokio::test]
    async fn test_seal_idempotent_with_rollups_present_and_lossless_on_late_raw() {
        // Regression: rollup tiers live in the metric partition dir. They must
        // not be swept into the daily seal (which caused perpetual re-seal +
        // mtime churn for metrics, so gc never reclaimed raw). And a late raw
        // arriving after a seal must be absorbed without losing the daily's
        // existing data.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-05-30");
        let date = NaiveDate::from_ymd_opt(2026, 5, 30).unwrap();
        let today = NaiveDate::from_ymd_opt(2026, 6, 1).unwrap();
        write_raw(&dir, "08-00-00.parquet", &["a"], &[10], &[1]);
        write_raw(&dir, "08-30-00.parquet", &["b"], &[20], &[2]);
        // A rollup tier file sits in the same dir.
        write_raw(&dir, "rollup-1d.parquet", &["roll"], &[15], &[99]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());

        // First seal: merges the two raw, supersedes them, ignores the rollup.
        assert!(
            c.seal_signal("metrics", today)
                .await
                .unwrap()
                .partitions_sealed
                >= 1
        );
        let daily = dir.join("compacted-2026-05-30.parquet");
        assert_eq!(
            read_batches(&daily)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        // resolve_files excludes the rollup and the superseded raw → only daily.
        assert_eq!(
            resolve_files(&dir)
                .unwrap()
                .iter()
                .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
                .collect::<Vec<_>>(),
            vec!["compacted-2026-05-30.parquet".to_string()],
            "rollup excluded, raw superseded"
        );

        // Second seal with no new data is a true no-op (the churn bug).
        assert_eq!(
            c.seal_partition(&dir, date).await.unwrap(),
            None,
            "idempotent: no re-seal when only the daily + rollup remain"
        );

        // A late raw appears post-seal: re-seal must keep the daily's 2 rows AND
        // absorb the new one (3 total), not overwrite to just the late row.
        write_raw(&dir, "09-00-00.parquet", &["c"], &[30], &[3]);
        assert!(
            c.seal_partition(&dir, date).await.unwrap().is_some(),
            "late raw triggers re-seal"
        );
        assert_eq!(
            read_batches(&daily)
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3,
            "daily carries prior data forward + absorbs the late raw (no loss)"
        );

        // Aged GC now reclaims every raw input; rollup and daily remain.
        let later = chrono::Utc::now() + ChronoDuration::seconds(120);
        c.gc_superseded("metrics", later).unwrap();
        assert_eq!(
            count_parquet(&dir, false),
            1,
            "only the rollup remains as non-compacted"
        );
        assert!(
            dir.join("rollup-1d.parquet").exists(),
            "rollup untouched by gc"
        );
        assert_eq!(
            resolve_files(&dir).unwrap().len(),
            1,
            "querier reads just the daily"
        );
    }

    #[tokio::test]
    async fn test_gc_superseded_deletes_inputs_after_grace_not_before() {
        let tmp = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-06-02");
        write_raw(&dir, "08-00-00.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "08-30-00.parquet", &["s"], &[2], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.compact_active_day("metrics", date.and_hms_opt(10, 0, 0).unwrap().and_utc())
            .await
            .unwrap();
        assert_eq!(
            count_parquet(&dir, false),
            2,
            "raw inputs still on disk after merge"
        );

        // Within the delete grace (file just written): nothing removed.
        let deleted = c.gc_superseded("metrics", chrono::Utc::now()).unwrap();
        assert_eq!(deleted, 0, "fresh superseder: inputs kept for read safety");
        assert_eq!(count_parquet(&dir, false), 2);

        // Past the grace: superseded raw inputs are reclaimed, hourly kept.
        let later = chrono::Utc::now() + ChronoDuration::seconds(120);
        let deleted = c.gc_superseded("metrics", later).unwrap();
        assert_eq!(deleted, 2, "both superseded raw files deleted");
        assert_eq!(count_parquet(&dir, false), 0, "raw gone");
        assert_eq!(count_parquet(&dir, true), 1, "hourly file kept");
        assert_eq!(resolve_files(&dir).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_gc_superseded_transitive_cleanup_no_orphans() {
        // raw -> hourly -> daily; after seal + aged GC only the daily survives
        // on disk (raw and hourly both reclaimed, none orphaned).
        let tmp = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-06-02");
        write_raw(&dir, "08-00-00.parquet", &["a"], &[10], &[1]);
        write_raw(&dir, "08-30-00.parquet", &["b"], &[20], &[2]);
        let c = Compactor::new(tmp.path(), CompactorConfig::default());
        c.compact_active_day("metrics", date.and_hms_opt(10, 0, 0).unwrap().and_utc())
            .await
            .unwrap();
        c.seal_signal("metrics", NaiveDate::from_ymd_opt(2026, 6, 3).unwrap())
            .await
            .unwrap();

        let later = chrono::Utc::now() + ChronoDuration::seconds(120);
        c.gc_superseded("metrics", later).unwrap();
        let names: Vec<String> = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into())
            .filter(|n: &String| n.ends_with(".parquet"))
            .collect();
        assert_eq!(
            names,
            vec!["compacted-2026-06-02.parquet".to_string()],
            "only the daily file remains on disk: {names:?}"
        );
    }

    #[tokio::test]
    async fn test_gc_superseded_disabled_keeps_inputs() {
        let tmp = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-06-02");
        write_raw(&dir, "08-00-00.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "08-30-00.parquet", &["s"], &[2], &[2]);
        let c = Compactor::new(
            tmp.path(),
            CompactorConfig {
                delete_superseded: false,
                ..Default::default()
            },
        );
        c.compact_active_day("metrics", date.and_hms_opt(10, 0, 0).unwrap().and_utc())
            .await
            .unwrap();
        let later = chrono::Utc::now() + ChronoDuration::seconds(120);
        assert_eq!(
            c.gc_superseded("metrics", later).unwrap(),
            0,
            "deletion disabled"
        );
        assert_eq!(count_parquet(&dir, false), 2, "raw inputs retained");
    }

    #[tokio::test]
    async fn test_intraday_disabled_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let date = NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let dir = tmp.path().join("metrics").join("dt=2026-06-02");
        write_raw(&dir, "08-00-00.parquet", &["s"], &[1], &[1]);
        write_raw(&dir, "08-30-00.parquet", &["s"], &[2], &[2]);
        let c = Compactor::new(
            tmp.path(),
            CompactorConfig {
                intraday: false,
                ..Default::default()
            },
        );
        let now = date.and_hms_opt(12, 0, 0).unwrap().and_utc();
        assert_eq!(
            c.compact_active_day("metrics", now)
                .await
                .unwrap()
                .files_output,
            0
        );
        assert_eq!(
            count_parquet(&dir, true),
            0,
            "intraday off: nothing compacted"
        );
    }
}
