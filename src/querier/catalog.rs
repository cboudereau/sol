// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Parquet table catalog + DataFusion query engine (task 2).
//!
//! Registers one DataFusion table per signal directory written by the file sink
//! (`logs/`, `traces/`, and `metrics/`). With task 14b the gateway writes
//! metrics into per-subtype subdirs (`metrics/<subtype>/dt=…`); the `metrics`
//! table is a ListingTable over the `metrics/` prefix, so it recurses into
//! those subdirs and unions the narrow per-subtype files. Schemas are declared
//! explicitly here as the binding contract with the codec ([parquet-multisignal](../../../docs/designs/20260527_parquet-multisignal.md));
//! DataFusion's schema adapter fills columns missing from a given file with null.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::MemTable;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use crate::config::querier::QuerierOptions;

/// A logical table registered in the query engine, backed by one signal directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalTable {
    /// Logs table (`logs/`).
    Logs,
    /// Traces table (`traces/`).
    Traces,
    /// Union of all metric subtypes (until task 14b splits them into per-subtype dirs).
    Metrics,
}

impl SignalTable {
    /// All signal tables registered by the catalog.
    pub const ALL: [SignalTable; 3] =
        [SignalTable::Logs, SignalTable::Traces, SignalTable::Metrics];

    /// SQL table name.
    pub fn table_name(self) -> &'static str {
        match self {
            SignalTable::Logs => "logs",
            SignalTable::Traces => "traces",
            SignalTable::Metrics => "metrics",
        }
    }

    /// Directory (relative to the storage root) holding this signal's Parquet files.
    pub fn listing_dir(self) -> &'static str {
        self.table_name()
    }

    /// Explicit Arrow schema — must match the codec output (parquet-multisignal).
    pub fn arrow_schema(self) -> SchemaRef {
        match self {
            SignalTable::Logs => log_schema(),
            SignalTable::Traces => trace_schema(),
            SignalTable::Metrics => metric_union_schema(),
        }
    }
}

fn utf8(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}
fn i32(name: &str) -> Field {
    Field::new(name, DataType::Int32, true)
}
fn i64n(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}
fn f64n(name: &str) -> Field {
    Field::new(name, DataType::Float64, true)
}
fn ts(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        nullable,
    )
}

fn log_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        utf8("service_name", false),
        utf8("event_name", true),
        ts("time_unix_nano", true),
        ts("observed_time_unix_nano", true),
        i32("severity_number"),
        utf8("severity_text", true),
        utf8("body", true),
        utf8("attributes", true),
        i32("flags"),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        i32("dropped_attributes_count"),
        utf8("resource_attributes", true),
        utf8("resource_schema_url", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("scope_schema_url", true),
    ]))
}

fn trace_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        utf8("service_name", false),
        ts("start_time_unix_nano", false),
        Field::new("duration_nanos", DataType::Int64, false),
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("parent_span_id", DataType::FixedSizeBinary(8), true),
        utf8("trace_state", true),
        utf8("name", false),
        i32("kind"),
        i32("status_code"),
        utf8("status_message", true),
        utf8("attributes", true),
        utf8("events", true),
        utf8("links", true),
        i32("dropped_attributes_count"),
        i32("dropped_events_count"),
        i32("dropped_links_count"),
        i32("flags"),
        utf8("resource_attributes", true),
        utf8("resource_schema_url", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("scope_schema_url", true),
    ]))
}

/// Superset of all metric-subtype columns (common block + every subtype's extras),
/// all subtype-specific columns nullable. DataFusion's schema adapter fills the
/// columns a given subtype file lacks with null.
fn metric_union_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // common (16)
        utf8("service_name", false),
        utf8("name", false),
        utf8("description", true),
        utf8("unit", true),
        ts("time_unix_nano", false),
        ts("start_time_unix_nano", true),
        utf8("attributes", true),
        i32("flags"),
        utf8("exemplars", true),
        utf8("resource_attributes", true),
        utf8("resource_schema_url", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("scope_schema_url", true),
        // prom_name — normalized Prometheus name (read-side filter column);
        // REQUIRED (clean cutover — every metric file carries it).
        utf8("prom_name", false),
        // number (gauge/sum)
        i64n("int_value"),
        f64n("double_value"),
        i32("aggregation_temporality"),
        Field::new("is_monotonic", DataType::Boolean, true),
        // histogram / exp-histogram / summary
        i64n("count"),
        f64n("sum"),
        f64n("min"),
        f64n("max"),
        utf8("bucket_counts", true),
        utf8("explicit_bounds", true),
        i32("scale"),
        i64n("zero_count"),
        f64n("zero_threshold"),
        i32("positive_offset"),
        utf8("positive_bucket_counts", true),
        i32("negative_offset"),
        utf8("negative_bucket_counts", true),
        utf8("quantile_values", true),
    ]))
}

/// Discovers and registers the signal Parquet directories as DataFusion tables.
pub struct ParquetCatalog {
    root: PathBuf,
}

impl ParquetCatalog {
    /// Create a catalog rooted at the given storage directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Register every signal table in `ctx`. An absent or empty directory is not an
    /// error — it registers as an empty table (count == 0).
    pub async fn register(&self, ctx: &SessionContext) -> crate::Result<()> {
        for table in SignalTable::ALL {
            let dir = self.root.join(table.listing_dir());
            let schema = table.arrow_schema();
            // Enumerate the surviving files ourselves (recursive walk + per-partition
            // supersession via `resolve_files`) and register over that explicit list.
            // This finds files at any partition depth — `logs/dt=…/` and (task 14b)
            // `metrics/<subtype>/dt=…/` — and skips raw inputs already superseded by a
            // compacted file, so the querier never double-counts (compaction ADR).
            let files = if dir.is_dir() {
                resolve_signal_files(&dir)?
            } else {
                Vec::new()
            };
            if files.is_empty() {
                // Absent/empty → empty table with the declared schema
                // (one empty partition; MemTable requires ≥1 partition).
                let empty = MemTable::try_new(schema, vec![vec![]])?;
                ctx.register_table(table.table_name(), Arc::new(empty))?;
            } else {
                let paths = files
                    .iter()
                    .map(|f| ListingTableUrl::parse(format!("file://{}", f.display())))
                    .collect::<Result<Vec<_>, _>>()?;
                // We pass the schema explicitly, so don't let DataFusion open
                // every file's footer for stats at plan time — with thousands
                // of files that exhausts the fd limit (EMFILE).
                let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_collect_stat(false);
                let config = ListingTableConfig::new_with_multi_paths(paths)
                    .with_listing_options(options)
                    .with_schema(schema);
                let listing = ListingTable::try_new(config)?;
                ctx.register_table(table.table_name(), Arc::new(listing))?;
            }
        }

        // Rollup tier tables (FR6): metrics_5m / metrics_1h / metrics_1d over the
        // compactor's rollup-<tier>.parquet files. Registered only when present,
        // so the frontend can detect availability and fall back to raw.
        let metrics_root = self.root.join("metrics");
        let metric_schema = SignalTable::Metrics.arrow_schema();
        for tier in ROLLUP_TIERS {
            let files = rollup_tier_files(&metrics_root, tier);
            if files.is_empty() {
                continue;
            }
            let paths = files
                .iter()
                .map(|f| ListingTableUrl::parse(format!("file://{}", f.display())))
                .collect::<Result<Vec<_>, _>>()?;
            let options =
                ListingOptions::new(Arc::new(ParquetFormat::default())).with_collect_stat(false);
            let config = ListingTableConfig::new_with_multi_paths(paths)
                .with_listing_options(options)
                .with_schema(Arc::clone(&metric_schema));
            ctx.register_table(
                format!("metrics_{tier}"),
                Arc::new(ListingTable::try_new(config)?),
            )?;
        }
        Ok(())
    }

    /// Re-register tables to pick up newly-created directories / files.
    /// (ListingTable lists files at registration; this re-registers so newly
    /// written files — and new compacted/rollup files — become visible.)
    pub async fn refresh(&self, ctx: &SessionContext) -> crate::Result<()> {
        for table in SignalTable::ALL {
            let _ = ctx.deregister_table(table.table_name());
        }
        for tier in ROLLUP_TIERS {
            let _ = ctx.deregister_table(format!("metrics_{tier}"));
        }
        self.register(ctx).await
    }
}

/// Rollup tier labels matching [`super::rollup::RollupTier::label`].
const ROLLUP_TIERS: [&str; 3] = ["5m", "1h", "1d"];

/// Collect `rollup-<tier>.parquet` files at any depth under the metrics root.
fn rollup_tier_files(metrics_root: &std::path::Path, tier: &str) -> Vec<PathBuf> {
    let target = format!("rollup-{tier}.parquet");
    let mut out = Vec::new();
    let mut stack = vec![metrics_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().and_then(|s| s.to_str()) == Some(target.as_str()) {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Recursively collect the queryable Parquet files under a signal root: walks to
/// every partition directory (any depth — `dt=…/` and task-14b
/// `<subtype>/dt=…/`), applies per-directory supersession ([`super::compaction::resolve_files`]
/// — compacted files plus raw not yet superseded), and excludes `rollup-*` files
/// (those back separate tier tables, not the main union).
fn resolve_signal_files(root: &std::path::Path) -> crate::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut has_parquet = false;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                has_parquet = true;
            }
        }
        if has_parquet {
            for file in super::compaction::resolve_files(&dir)? {
                let name = file
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default();
                if !name.starts_with("rollup-") {
                    out.push(file);
                }
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Bytes read and file groups opened by an executed physical plan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ScanStats {
    bytes_scanned: u64,
    files_opened: u64,
}

/// Sum the Parquet scan metrics across an executed physical plan tree.
///
/// `bytes_scanned` is the `bytes_scanned` counter DataFusion's Parquet data
/// source records per node (`MetricsSet::sum_by_name("bytes_scanned")`). Only
/// the leaf scan nodes expose it, so summing over the whole tree double-counts
/// nothing. `files_opened` is approximated by the scan node's output partition
/// count (one partition per file group) — a robust proxy that avoids downcasting
/// the trait-object data source, which is fragile across DataFusion versions.
/// Must run **after** execution so the counters hold real values.
fn scan_stats_from_plan(
    plan: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> ScanStats {
    use datafusion::physical_plan::ExecutionPlanProperties;
    let mut stats = ScanStats::default();
    if let Some(value) = plan.metrics().and_then(|m| m.sum_by_name("bytes_scanned")) {
        let bytes = value.as_usize() as u64;
        if bytes > 0 {
            stats.bytes_scanned += bytes;
            // A node that reports bytes_scanned is a Parquet scan leaf; its
            // output partition count is the number of file groups opened.
            stats.files_opened += plan.output_partitioning().partition_count() as u64;
        }
    }
    for child in plan.children() {
        let child_stats = scan_stats_from_plan(child);
        stats.bytes_scanned += child_stats.bytes_scanned;
        stats.files_opened += child_stats.files_opened;
    }
    stats
}

/// Derive the dashboard `signal` label from a logical plan's table scans:
/// `logs` / `traces` / `metrics` (rollup tiers `metrics_5m|1h|1d` collapse to
/// `metrics`). A plan scanning a single signal is labelled with it; a
/// cross-signal SQL plan touching more than one distinct signal is labelled
/// `sql`; a plan with no recognised scan falls back to `metrics`.
fn signal_of_plan(plan: &datafusion::logical_expr::LogicalPlan) -> &'static str {
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut signals = std::collections::BTreeSet::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(sig) = signal_of_table(scan.table_name.table())
        {
            signals.insert(sig);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    match signals.len() {
        1 => signals.into_iter().next().unwrap_or("metrics"),
        n if n > 1 => "sql",
        _ => "metrics",
    }
}

/// Map a registered table name to its signal label, collapsing rollup tiers.
fn signal_of_table(name: &str) -> Option<&'static str> {
    match name {
        "logs" => Some("logs"),
        "traces" => Some("traces"),
        "metrics" | "metrics_5m" | "metrics_1h" | "metrics_1d" => Some("metrics"),
        _ => None,
    }
}

/// Thin wrapper over a DataFusion `SessionContext` with the signal catalog registered.
/// Sole query-engine dependency (NFR1); worker pool bounded so queries do not starve
/// ingestion (NFR5).
pub struct QueryEngine {
    ctx: SessionContext,
    catalog: ParquetCatalog,
    cache: super::cache::MokaQueryCache,
    storage_root: std::path::PathBuf,
    max_scan_bytes: u64,
}

impl QueryEngine {
    /// Build the engine from config: register the signal catalog in a DataFusion
    /// `SessionContext` with a bounded worker pool (NFR5).
    pub async fn new(opts: &QuerierOptions) -> crate::Result<Self> {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(4); // NFR5: bound the worker pool.
        let config = SessionConfig::new().with_target_partitions(parallelism);
        let mut ctx = SessionContext::new_with_config(config);
        // JSON extraction over the `attributes` string column (ADR 0039):
        // registers `json_get_str`/`json_get_*`, `->`/`->>`, `json_contains`, …
        datafusion_functions_json::register_all(&mut ctx)?;
        // prom_attr(attributes, 'name'): OTLP→Prometheus normalized attribute
        // lookup so the Prometheus API matches dashboards (ADR 0039 / query-side).
        ctx.register_udf(super::udf::prom_attr_udf());
        // prom_group_key / prom_group_key_reproject: canonical, reversible group
        // keys for aggregation pushdown (promql-pushdown T1). Called from plans.
        ctx.register_udf(super::group_key::prom_group_key_udf());
        ctx.register_udf(super::group_key::prom_group_key_reproject_udf());
        // Metric-name normalization is materialized into the `prom_name` column at
        // write time (codec), so no read-time `prom_metric_name` UDF is registered.
        let catalog = ParquetCatalog::new(opts.storage.path.clone());
        catalog.register(&ctx).await?;
        Ok(Self {
            ctx,
            catalog,
            cache: super::cache::MokaQueryCache::with_budget(
                opts.cache.max_bytes,
                std::time::Duration::from_secs(opts.cache.ttl_secs),
            ),
            storage_root: opts.storage.path.clone(),
            max_scan_bytes: opts.guardrails.max_bytes_scanned,
        })
    }

    /// Storage root (`<root>/<signal>/dt=…`), for scan-size guardrail estimates.
    pub fn storage_root(&self) -> &std::path::Path {
        &self.storage_root
    }

    /// Configured maximum bytes a single query may scan (NFR9).
    pub fn max_scan_bytes(&self) -> u64 {
        self.max_scan_bytes
    }

    /// Whether a table is registered (e.g. a rollup tier table `metrics_1h`).
    pub fn has_table(&self, name: &str) -> bool {
        self.ctx.table_exist(name).unwrap_or(false)
    }

    /// Run a SQL query, collecting all result batches. Results are cached
    /// (FR5/NFR6) keyed by the SQL text; a repeat query within the TTL is
    /// served from memory without re-hitting DataFusion.
    pub async fn sql(
        &self,
        query: &str,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        use super::cache::{CacheKey, QueryCache};
        let key = CacheKey::for_sql(query);
        if let Some(hit) = self.cache.get(&key) {
            super::telemetry::record_cache(true);
            return Ok((*hit).clone());
        }
        super::telemetry::record_cache(false);
        let df = self.ctx.sql(query).await?;
        let batches = self.execute_recording_scan(df).await?;
        self.cache.insert(key, std::sync::Arc::new(batches.clone()));
        super::telemetry::set_cache_memory(self.cache.weighted_size());
        Ok(batches)
    }

    /// Execute a `DataFrame` via its physical plan, recording the per-signal
    /// scan volume (bytes/files) observed from the executed plan metrics (NFR5).
    /// Going through the physical plan (rather than `DataFrame::collect`) keeps
    /// the executed plan in hand so its scan counters can be read afterwards.
    async fn execute_recording_scan(
        &self,
        df: datafusion::dataframe::DataFrame,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let signal = signal_of_plan(df.logical_plan());
        let plan = df.create_physical_plan().await?;
        let batches =
            datafusion::physical_plan::collect(std::sync::Arc::clone(&plan), self.ctx.task_ctx())
                .await?;
        let stats = scan_stats_from_plan(&plan);
        if stats.bytes_scanned > 0 || stats.files_opened > 0 {
            super::telemetry::record_scan(signal, stats.bytes_scanned, stats.files_opened);
        }
        Ok(batches)
    }

    /// A `DataFrame` over a registered signal table — the entry point for the
    /// `Expr`/plan-based lowering ([`super::plan`]). Signal modules build on this
    /// (`engine.table("traces")?.filter(pred)?…`) then run it via [`Self::collect`].
    pub async fn table(&self, name: &str) -> crate::Result<datafusion::dataframe::DataFrame> {
        Ok(self.ctx.table(name).await?)
    }

    /// Execute a built `DataFrame`, collecting Arrow batches — the plan-based twin
    /// of [`Self::sql`]. Cached on the plan's indented display (ADR: plan-cache-keying),
    /// reusing the same moka cache + telemetry contract.
    pub async fn collect(
        &self,
        df: datafusion::dataframe::DataFrame,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        use super::cache::{CacheKey, QueryCache};
        let key = CacheKey::for_sql(&df.logical_plan().display_indent().to_string());
        if let Some(hit) = self.cache.get(&key) {
            super::telemetry::record_cache(true);
            return Ok((*hit).clone());
        }
        super::telemetry::record_cache(false);
        let batches = self.execute_recording_scan(df).await?;
        self.cache.insert(key, std::sync::Arc::new(batches.clone()));
        super::telemetry::set_cache_memory(self.cache.weighted_size());
        Ok(batches)
    }

    /// Run **untrusted** user SQL (the cross-signal `/api/v1/sql` endpoint).
    /// Unlike [`Self::sql`] (used only for internally-built, trusted queries),
    /// this rejects DDL, DML, and non-query statements via DataFusion
    /// `SQLOptions`, so a client cannot `COPY … TO`, `CREATE EXTERNAL TABLE …
    /// LOCATION` (arbitrary file write/read), or mutate the catalog — only
    /// read-only `SELECT`s over the registered tables are allowed (NFR9).
    pub async fn sql_user(
        &self,
        query: &str,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        use super::cache::{CacheKey, QueryCache};
        let key = CacheKey::for_sql(query);
        if let Some(hit) = self.cache.get(&key) {
            super::telemetry::record_cache(true);
            return Ok((*hit).clone());
        }
        super::telemetry::record_cache(false);
        let options = datafusion::execution::context::SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = self.ctx.sql_with_options(query, options).await?;
        let batches = self.execute_recording_scan(df).await?;
        self.cache.insert(key, std::sync::Arc::new(batches.clone()));
        super::telemetry::set_cache_memory(self.cache.weighted_size());
        Ok(batches)
    }

    /// Re-list storage for newly written files (called periodically by the
    /// server). Invalidates the query cache so freshly discovered data is
    /// visible immediately rather than after the TTL.
    pub async fn refresh(&self) -> crate::Result<()> {
        use super::cache::QueryCache;
        self.catalog.refresh(&self.ctx).await?;
        self.cache.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;

    fn engine_opts(root: PathBuf) -> QuerierOptions {
        QuerierOptions {
            storage: crate::config::querier::StorageConfig {
                path: root,
                url: None,
            },
            ..QuerierOptions::default()
        }
    }

    async fn count(engine: &QueryEngine, table: &str) -> i64 {
        let batches = engine
            .sql(&format!("SELECT count(*) AS n FROM {table}"))
            .await
            .unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        arr.value(0)
    }

    #[test]
    fn test_signal_tables_map_to_directories() {
        let dirs: Vec<_> = SignalTable::ALL.iter().map(|t| t.listing_dir()).collect();
        assert_eq!(dirs, vec!["logs", "traces", "metrics"]);
    }

    #[test]
    fn test_logs_schema_matches_codec_columns() {
        let schema = SignalTable::Logs.arrow_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names[0], "service_name");
        assert!(names.contains(&"event_name"));
        assert!(names.contains(&"body"));
        assert!(names.contains(&"trace_id"));
        assert_eq!(schema.fields().len(), 18);
        // trace_id is fixed-size binary(16) per the codec.
        let tid = schema.field_with_name("trace_id").unwrap();
        assert_eq!(tid.data_type(), &DataType::FixedSizeBinary(16));
    }

    fn write_min_log_parquet(path: &std::path::Path, rows: usize) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            false,
        )]));
        let vals: Vec<&str> = std::iter::repeat_n("svc", rows).collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vals))]).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_catalog_refresh_picks_up_new_file() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(count(&engine, "logs").await, 0); // logs/ absent at register time

        // A logs directory + file appears after registration.
        let logs_dir = tmp.path().join("logs").join("dt=2026-06-01");
        std::fs::create_dir_all(&logs_dir).unwrap();
        write_min_log_parquet(&logs_dir.join("new.parquet"), 3);

        engine.refresh().await.unwrap();
        assert_eq!(count(&engine, "logs").await, 3);
        // Idempotent: a second refresh keeps the count.
        engine.refresh().await.unwrap();
        assert_eq!(count(&engine, "logs").await, 3);
    }

    #[tokio::test]
    async fn test_catalog_empty_dir_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(count(&engine, "logs").await, 0);
        assert_eq!(count(&engine, "traces").await, 0);
        assert_eq!(count(&engine, "metrics").await, 0);
    }

    #[tokio::test]
    async fn test_catalog_registers_tables_from_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let logs_dir = tmp.path().join("logs").join("dt=2026-06-01");
        std::fs::create_dir_all(&logs_dir).unwrap();

        // Minimal log Parquet fixture (2 cols); the schema adapter fills the rest with null.
        let fixture_schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            utf8("body", true),
        ]));
        let batch = RecordBatch::try_new(
            fixture_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["client", "client"])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(logs_dir.join("fixture.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, fixture_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(count(&engine, "logs").await, 2);

        // A SELECT of declared columns the fixture lacks returns null (adapter), not error.
        let rows = engine
            .sql("SELECT service_name, severity_text FROM logs ORDER BY body")
            .await
            .unwrap();
        assert_eq!(rows[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_metrics_union_recurses_into_per_subtype_dirs() {
        // task 14b: the gateway writes metrics/<subtype>/dt=…; the `metrics`
        // union ListingTable must recurse into those nested dirs and union the
        // narrow per-subtype files (adapter fills the missing columns).
        let tmp = tempfile::tempdir().unwrap();
        for subtype in ["gauge", "sum", "histogram"] {
            let dir = tmp
                .path()
                .join("metrics")
                .join(subtype)
                .join("dt=2026-06-01");
            std::fs::create_dir_all(&dir).unwrap();
            write_min_log_parquet(&dir.join("f.parquet"), 2); // 2 rows each
        }
        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        // 3 subtypes × 2 rows, all under metrics/ → one union table.
        assert_eq!(count(&engine, "metrics").await, 6);
    }

    #[tokio::test]
    async fn test_rollup_tier_tables_registered_separately() {
        // task 12/D: rollup-<tier>.parquet files back per-tier tables, excluded
        // from the main `metrics` union (no double count) but queryable directly.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp
            .path()
            .join("metrics")
            .join("gauge")
            .join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        write_min_log_parquet(&dir.join("m.parquet"), 2); // raw, 2 rows
        write_min_log_parquet(&dir.join("rollup-1h.parquet"), 1); // 1h rollup, 1 row
        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        // main union excludes the rollup file → only the 2 raw rows
        assert_eq!(count(&engine, "metrics").await, 2);
        // the 1h tier table is registered over the rollup file
        assert!(engine.has_table("metrics_1h"));
        assert!(
            !engine.has_table("metrics_5m"),
            "absent tier not registered"
        );
        assert_eq!(count(&engine, "metrics_1h").await, 1);
    }

    #[tokio::test]
    async fn test_querier_reads_each_datum_once_after_compaction() {
        // Compaction correctness (ADR): raw inputs and the compacted file that
        // supersedes them coexist on disk; the querier must read each datum
        // exactly once — count the compacted rows, NOT raw + compacted.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("logs").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        write_min_log_parquet(&dir.join("raw-a.parquet"), 2); // raw input, 2 rows

        // compacted file (3 rows) declaring it supersedes raw-a.parquet
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service_name",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["svc", "svc", "svc"]))],
        )
        .unwrap();
        crate::querier::compaction::write_with_provenance(
            &dir.join("compacted-2026-06-01.parquet"),
            schema,
            &[batch],
            1,
            "raw-a.parquet",
            "raw",
        )
        .unwrap();

        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        // raw-a.parquet is superseded → only the 3 compacted rows, not 5.
        assert_eq!(count(&engine, "logs").await, 3);
    }

    #[test]
    fn test_signal_of_table_collapses_rollup_tiers() {
        assert_eq!(signal_of_table("logs"), Some("logs"));
        assert_eq!(signal_of_table("traces"), Some("traces"));
        assert_eq!(signal_of_table("metrics"), Some("metrics"));
        assert_eq!(signal_of_table("metrics_1h"), Some("metrics"));
        assert_eq!(signal_of_table("metrics_1d"), Some("metrics"));
        assert_eq!(signal_of_table("unknown"), None);
    }

    #[test]
    fn test_query_records_real_bytes_scanned() {
        use metrics_util::MetricKind;
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        // `with_local_recorder` installs a thread-local recorder, so run the whole
        // build+query on one dedicated thread whose own current-thread runtime
        // drives the async work — nesting a runtime inside `#[tokio::test]` panics.
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
                    let logs_dir = tmp.path().join("logs").join("dt=2026-06-01");
                    std::fs::create_dir_all(&logs_dir).unwrap();
                    // Enough rows that the Parquet scan reports non-zero bytes.
                    write_min_log_parquet(&logs_dir.join("f.parquet"), 1000);
                    let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
                        .await
                        .unwrap();
                    engine.sql("SELECT service_name FROM logs").await.unwrap();
                });
            });
        })
        .join()
        .unwrap();

        let s = snap.snapshot().into_vec();
        let bytes = s.iter().find_map(|(k, _, _, v)| {
            (k.kind() == MetricKind::Histogram
                && k.key().name() == "querier_bytes_scanned"
                && k.key()
                    .labels()
                    .any(|l| l.key() == "signal" && l.value() == "logs"))
            .then_some(v)
        });
        let DebugValue::Histogram(samples) = bytes.expect("bytes_scanned histogram for logs signal")
        else {
            panic!("expected histogram value");
        };
        let total: f64 = samples.iter().map(|h| h.into_inner()).sum();
        assert!(total > 0.0, "bytes_scanned must be > 0, samples: {samples:?}");
    }
}
