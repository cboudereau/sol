// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Parquet table catalog + DataFusion query engine (task 2).
//!
//! Registers one DataFusion table per signal directory written by the file sink
//! (`logs/`, `traces/`, and `metrics/`). With task 14b the gateway writes
//! metrics into per-subtype subdirs (`metrics/<subtype>/dt=…`); the `metrics`
//! table is a ListingTable over the `metrics/` prefix, so it recurses into
//! those subdirs and unions the narrow per-subtype files. Schemas are declared
//! explicitly here as the binding contract with the codec ([parquet-multisignal](../../docs/20260527_parquet-multisignal/designs/20260527_parquet-multisignal.md));
//! DataFusion's schema adapter fills columns missing from a given file with null.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use super::inventory::{FileInventory, QueryScope};
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
        // attributes — columnar MAP<Utf8,Utf8> (read parse-free; promql-pushdown T6/T7).
        Field::new("attributes", super::udf::attributes_map_type(), true),
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
        // prom_series_key — canonical, groupable series key (== series_key_string);
        // REQUIRED (clean cutover). Window/aggregate/rollup partitions key on this
        // plain Utf8 column instead of the per-row UDF over the `attributes` MAP.
        utf8("prom_series_key", false),
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
        // per-bucket scalar-value aggregates (FR6 rollup-aggregate-schema ADR);
        // nullable so raw files null them via the schema adapter — only tier
        // files (written by the compactor rollup) populate them.
        f64n("value_min"),
        f64n("value_max"),
        f64n("value_sum"),
        f64n("value_count"),
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

    /// Build the `(table_name, provider)` set for every signal table + present
    /// rollup tier, doing the file-listing walk — plus the [`FileInventory`]
    /// over the **same** walked file lists (per-query file-pruning ADR: the
    /// inventory and the registered tables derive from one walk, so they
    /// cannot diverge). Pure (no `ctx` mutation), so a `refresh` can build the
    /// new providers while the old tables stay live, then swap them in (see
    /// [`refresh`]).
    async fn build_providers(
        &self,
    ) -> crate::Result<(Vec<(String, Arc<dyn TableProvider>)>, FileInventory)> {
        let mut out: Vec<(String, Arc<dyn TableProvider>)> = Vec::new();
        let mut inventory = FileInventory::default();
        for table in SignalTable::ALL {
            let dir = self.root.join(table.listing_dir());
            let schema = table.arrow_schema();
            // Enumerate the surviving files ourselves (recursive walk + per-partition
            // supersession via `resolve_files`) over that explicit list. Finds files
            // at any partition depth — `logs/dt=…/` and `metrics/<subtype>/dt=…/` —
            // and skips raw inputs already superseded by a compacted file (no double
            // count, compaction ADR).
            let files = if dir.is_dir() {
                resolve_signal_files(&dir)?
            } else {
                Vec::new()
            };
            let provider: Arc<dyn TableProvider> = if files.is_empty() {
                empty_provider(schema)?
            } else {
                listing_provider(&files, schema)?
            };
            inventory.insert_table(table.table_name(), &files);
            out.push((table.table_name().to_string(), provider));
        }
        // Rollup tier tables (FR6): metrics_5m / metrics_1h / metrics_1d over the
        // compactor's rollup-<tier>.parquet files. Built only when present, so the
        // frontend can detect availability and fall back to raw.
        let metrics_root = self.root.join("metrics");
        let metric_schema = SignalTable::Metrics.arrow_schema();
        for tier in ROLLUP_TIERS {
            let files = rollup_tier_files(&metrics_root, tier);
            if files.is_empty() {
                continue;
            }
            let name = format!("metrics_{tier}");
            inventory.insert_table(name.as_str(), &files);
            out.push((name, listing_provider(&files, Arc::clone(&metric_schema))?));
        }
        Ok((out, inventory))
    }

    /// Register every signal table in `ctx`. An absent or empty directory is not an
    /// error — it registers as an empty table (count == 0). Returns the
    /// [`FileInventory`] built from the same walk as the registered tables.
    pub async fn register(&self, ctx: &SessionContext) -> crate::Result<FileInventory> {
        let (providers, inventory) = self.build_providers().await?;
        for (name, provider) in providers {
            ctx.register_table(name, provider)?;
        }
        Ok(inventory)
    }

    /// Re-register tables to pick up newly-created directories / files
    /// (ListingTable lists files at registration), without a registration gap.
    ///
    /// The old code deregistered **every** table and *then* ran the file-listing
    /// walk to re-register — leaving a window (widened by the now-large store +
    /// rollup tiers) during which a concurrent query planned against a missing
    /// table ("Error during planning: No table named 'metrics'"). `register_table`
    /// errors on an existing name (no in-place replace), so a deregister is
    /// unavoidable — but it must not bracket the slow walk. Instead: build all
    /// the new providers first (walk runs while the old tables stay live), then
    /// swap each in with a tight `deregister`→`register` pair with **no `await`
    /// between** (catalog map ops), shrinking the unregistered window per table
    /// from the whole walk to effectively nothing.
    ///
    /// Returns the new [`FileInventory`] built from the same walk as the
    /// swapped-in providers; the caller ([`QueryEngine::refresh`]) swaps it in
    /// right after the table swap — build everything first, then swap both.
    pub async fn refresh(&self, ctx: &SessionContext) -> crate::Result<FileInventory> {
        let (providers, inventory) = self.build_providers().await?;
        let present: std::collections::HashSet<&str> =
            providers.iter().map(|(n, _)| n.as_str()).collect();
        for (name, provider) in &providers {
            let _ = ctx.deregister_table(name.as_str());
            ctx.register_table(name.as_str(), Arc::clone(provider))?;
        }
        // Drop any rollup tier whose files have all vanished (e.g. retention GC):
        // `build_providers` omits empty tiers, so a stale registration would linger.
        for tier in ROLLUP_TIERS {
            let name = format!("metrics_{tier}");
            if !present.contains(name.as_str()) {
                let _ = ctx.deregister_table(name.as_str());
            }
        }
        Ok(inventory)
    }
}

/// Rollup tier labels matching [`super::rollup::RollupTier::label`].
const ROLLUP_TIERS: [&str; 3] = ["5m", "1h", "1d"];

/// A `ListingTable` provider over an explicit file list with the declared
/// schema. Schema is explicit, so don't let DataFusion open every file's
/// footer for stats at plan time — with thousands of files that exhausts the
/// fd limit (EMFILE). Used both for the registered tables
/// ([`ParquetCatalog::build_providers`]) and for the per-query scoped,
/// **unregistered** providers ([`QueryEngine::table_scoped`]).
fn listing_provider(files: &[PathBuf], schema: SchemaRef) -> crate::Result<Arc<dyn TableProvider>> {
    let paths = files
        .iter()
        .map(|f| ListingTableUrl::parse(format!("file://{}", f.display())))
        .collect::<Result<Vec<_>, _>>()?;
    let options = ListingOptions::new(Arc::new(ParquetFormat::default())).with_collect_stat(false);
    let config = ListingTableConfig::new_with_multi_paths(paths)
        .with_listing_options(options)
        .with_schema(schema);
    Ok(Arc::new(ListingTable::try_new(config)?))
}

/// An empty table with the declared schema (one empty partition; `MemTable`
/// requires ≥ 1 partition) — for an absent/empty directory or an all-pruned
/// scoped file list.
fn empty_provider(schema: SchemaRef) -> crate::Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(MemTable::try_new(schema, vec![vec![]])?))
}

/// Declared schema of a registered table name (signal tables + rollup tiers,
/// which share the metric union schema); `None` for anything else.
fn table_schema(name: &str) -> Option<SchemaRef> {
    match name {
        "logs" => Some(SignalTable::Logs.arrow_schema()),
        "traces" => Some(SignalTable::Traces.arrow_schema()),
        "metrics" | "metrics_5m" | "metrics_1h" | "metrics_1d" => {
            Some(SignalTable::Metrics.arrow_schema())
        }
        _ => None,
    }
}

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
/// nothing. `files_opened` is the scan node's actual file count (every listed
/// file has its footer opened at execution, even when its row groups are then
/// stats-pruned — opening is the per-file cost FR1 eliminates), read from the
/// `DataSourceExec`'s `FileScanConfig` ([`scan_file_count`]); if that downcast
/// ever stops matching, it degrades to the output-partition-count proxy (one
/// partition per file *group*, which under-counts grouped files but stays > 0).
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
            // A node that reports bytes_scanned is a Parquet scan leaf.
            stats.files_opened += scan_file_count(plan.as_ref())
                .unwrap_or_else(|| plan.output_partitioning().partition_count() as u64);
        }
    }
    for child in plan.children() {
        let child_stats = scan_stats_from_plan(child);
        stats.bytes_scanned += child_stats.bytes_scanned;
        stats.files_opened += child_stats.files_opened;
    }
    stats
}

/// The number of files a scan leaf reads: the summed `FileScanConfig` group
/// sizes of a `DataSourceExec`. `None` when `plan` is not a file scan (e.g. a
/// `MemTable` leaf) or the concrete types change across a DataFusion upgrade —
/// the caller then falls back to the partition-count proxy.
fn scan_file_count(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> Option<u64> {
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::datasource::source::DataSourceExec;
    let exec = plan.as_any().downcast_ref::<DataSourceExec>()?;
    let config = exec
        .data_source()
        .as_any()
        .downcast_ref::<FileScanConfig>()?;
    Some(
        config
            .file_groups
            .iter()
            .map(|g| g.len() as u64)
            .sum::<u64>(),
    )
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

/// Marker string carried by every overload (shed) error — **the** contract
/// `routes::error_response` matches (by substring) to map overload onto
/// HTTP 503 + `Retry-After`. Substring matching is deliberate: single-flight
/// followers receive their leader's error *stringified*
/// ([`super::single_flight`] shares non-`Clone` errors as rendered messages),
/// so only the request that actually timed out still holds the typed
/// [`OverloadError`]; a follower whose leader shed sees this marker inside a
/// plain string error. Keep the text distinctive enough never to appear in an
/// engine or user-SQL error.
pub(super) const OVERLOAD_MARKER: &str = "querier overloaded: max_concurrent_queries reached";

/// Typed overload error (FR5,
/// [concurrency-guardrail ADR](../../docs/workspace/backend-metrics-perf/adrs/concurrency-guardrail.md)):
/// a query could not obtain an execution permit within the bounded wait.
/// Renders as [`OVERLOAD_MARKER`] so the HTTP layer — and single-flight
/// followers, who only see it stringified — can recognise it.
#[derive(Debug)]
pub struct OverloadError;

impl std::fmt::Display for OverloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(OVERLOAD_MARKER)
    }
}

impl std::error::Error for OverloadError {}

/// Bounded wait for an execution permit before shedding
/// ([ADR](../../docs/workspace/backend-metrics-perf/adrs/concurrency-guardrail.md)
/// option A: short wait, then 503 + `Retry-After`). Long enough to absorb a
/// dashboard burst draining, short enough that overload surfaces instead of
/// queueing unboundedly. Tests override it per engine (tiny, deterministic).
const PERMIT_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Thin wrapper over a DataFusion `SessionContext` with the signal catalog registered.
/// Sole query-engine dependency (NFR1); worker pool bounded so queries do not starve
/// ingestion (NFR5).
pub struct QueryEngine {
    ctx: SessionContext,
    catalog: ParquetCatalog,
    cache: super::cache::MokaQueryCache,
    /// Optimized-logical-plan cache (promql-plan-cache task 2a, ADR A′):
    /// repeated query shapes skip lower's re-optimize — the cached
    /// post-`optimize()` plan is rebound (window literals + scoped providers)
    /// to the current window and physically planned directly. Sits *behind*
    /// the result cache and single-flight: a result-cache hit never touches
    /// it.
    plan_cache: super::plan_cache::PlanCache,
    /// Request coalescing in front of the cache-backed execution (FR3): N
    /// concurrent identical cache misses execute the plan once; followers
    /// share the leader's result.
    single_flight: super::single_flight::SingleFlight,
    storage_root: std::path::PathBuf,
    max_scan_bytes: u64,
    metadata_default_range_secs: u64,
    /// Staleness lookback (seconds) bounding instant scans (promql-plan-cache
    /// FR3): an instant vector at `time` only reads `[time − this, time]`.
    instant_lookback_secs: u64,
    /// Per-table file inventory for per-query pruning (FR1) — always built
    /// from the same `build_providers` walk as the registered tables and
    /// swapped right after them at [`Self::refresh`]. The engine's single
    /// interior-mutability point: readers snapshot the `Arc` and drop the
    /// guard before any `await`.
    inventory: std::sync::RwLock<Arc<FileInventory>>,
    /// FR5 admission control
    /// ([ADR](../../docs/workspace/backend-metrics-perf/adrs/concurrency-guardrail.md)):
    /// `guardrails.max_concurrent_queries` permits bounding query *execution*
    /// — every Prometheus/Loki/Tempo/SQL path funnels into
    /// `sql`/`collect_scoped`/`sql_user`, where the single-flight **leader**
    /// acquires a permit; coalesced followers wait without consuming capacity.
    /// `Arc` so tests can hold an owned permit.
    query_permits: Arc<tokio::sync::Semaphore>,
    /// Bounded permit wait before shedding with [`OverloadError`]
    /// (default [`PERMIT_ACQUIRE_TIMEOUT`]; tiny override in tests).
    permit_acquire_timeout: std::time::Duration,
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
        // prom_series_key(attributes): a groupable Utf8 key derived from the
        // columnar `attributes` MAP — DataFusion cannot GROUP/PARTITION BY a Map,
        // so window/series plans key on this instead (promql-pushdown T7).
        ctx.register_udf(super::udf::prom_series_key_udf());
        // Metric-name normalization is materialized into the `prom_name` column at
        // write time (codec), so no read-time `prom_metric_name` UDF is registered.
        let catalog = ParquetCatalog::new(opts.storage.path.clone());
        let inventory = catalog.register(&ctx).await?;
        // FR5: size the execution-permit pool from the guardrail config
        // (default 16). An out-of-range value clamps to the semaphore maximum.
        let max_concurrent = usize::try_from(opts.guardrails.max_concurrent_queries)
            .unwrap_or(tokio::sync::Semaphore::MAX_PERMITS)
            .min(tokio::sync::Semaphore::MAX_PERMITS);
        Ok(Self {
            ctx,
            catalog,
            cache: super::cache::MokaQueryCache::with_budget(
                opts.cache.max_bytes,
                std::time::Duration::from_secs(opts.cache.ttl_secs),
            ),
            plan_cache: super::plan_cache::PlanCache::new(),
            single_flight: super::single_flight::SingleFlight::new(),
            storage_root: opts.storage.path.clone(),
            max_scan_bytes: opts.guardrails.max_bytes_scanned,
            metadata_default_range_secs: opts.metadata_default_range_secs,
            instant_lookback_secs: opts.instant_lookback_secs,
            inventory: std::sync::RwLock::new(Arc::new(inventory)),
            query_permits: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            permit_acquire_timeout: PERMIT_ACQUIRE_TIMEOUT,
        })
    }

    /// Acquire an execution permit (FR5) with the bounded wait; on timeout the
    /// query sheds — records `sol_querier_shed_total` and returns the typed
    /// [`OverloadError`] (mapped to HTTP 503 + `Retry-After` by the routes
    /// layer). The permit is RAII: dropped on every exit of the execution it
    /// guards (success, error, panic), so a failing query never leaks capacity.
    async fn acquire_query_permit(&self) -> crate::Result<tokio::sync::SemaphorePermit<'_>> {
        match tokio::time::timeout(self.permit_acquire_timeout, self.query_permits.acquire()).await
        {
            Ok(Ok(permit)) => Ok(permit),
            // Timed out — or the semaphore was closed, which this engine never
            // does; treat both as overload rather than panic.
            Ok(Err(_)) | Err(_) => {
                super::telemetry::record_shed();
                Err(OverloadError.into())
            }
        }
    }

    /// Override the bounded permit wait (tests only): deterministic shed
    /// without multi-second sleeps.
    #[cfg(test)]
    pub(crate) fn set_permit_acquire_timeout_for_test(&mut self, timeout: std::time::Duration) {
        self.permit_acquire_timeout = timeout;
    }

    /// Hold one execution permit (tests only): saturates
    /// `max_concurrent_queries` from outside the engine.
    #[cfg(test)]
    pub(crate) async fn hold_query_permit_for_test(&self) -> tokio::sync::OwnedSemaphorePermit {
        Arc::clone(&self.query_permits)
            .acquire_owned()
            .await
            .expect("query-permit semaphore is never closed")
    }

    /// Storage root (`<root>/<signal>/dt=…`), for scan-size guardrail estimates.
    pub fn storage_root(&self) -> &std::path::Path {
        &self.storage_root
    }

    /// Configured maximum bytes a single query may scan (NFR9).
    pub fn max_scan_bytes(&self) -> u64 {
        self.max_scan_bytes
    }

    /// Default `start` (unix ns) for a Prometheus metadata request without an
    /// explicit `start` (FR4): `now − metadata_default_range_secs`. A bounded
    /// default lets the metadata paths take the ranged branch — sealed-span
    /// tier routing plus FR1's scoped file listing — instead of scanning all
    /// history. An explicit client `start` (including `start=0`) always wins;
    /// this is only the absent-`start` fallback.
    pub fn metadata_default_start_ns(&self, now_ns: i64) -> i64 {
        let range_ns = i64::try_from(
            self.metadata_default_range_secs
                .saturating_mul(1_000_000_000),
        )
        .unwrap_or(i64::MAX);
        now_ns.saturating_sub(range_ns)
    }

    /// Staleness lookback for instant queries, in nanoseconds
    /// (promql-plan-cache FR3, Prometheus 5 m semantics by default): the
    /// instant scan's lower bound is `anchor − this`, so only files that can
    /// hold a sample inside the staleness window are opened, and series whose
    /// last sample is older correctly disappear.
    pub(crate) fn instant_lookback_ns(&self) -> i64 {
        i64::try_from(self.instant_lookback_secs.saturating_mul(1_000_000_000))
            .unwrap_or(i64::MAX)
    }

    /// Whether a table is registered (e.g. a rollup tier table `metrics_1h`).
    pub fn has_table(&self, name: &str) -> bool {
        self.ctx.table_exist(name).unwrap_or(false)
    }

    /// Run a SQL query, collecting all result batches. Results are cached
    /// (FR5/NFR6) keyed by the SQL text; a repeat query within the TTL is
    /// served from memory without re-hitting DataFusion. Concurrent identical
    /// misses coalesce onto one execution (FR3, single-flight).
    pub async fn sql(
        &self,
        query: &str,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        use super::cache::{CacheKey, QueryCache, TtlClass};
        let key = CacheKey::for_sql(query);
        if let Some(hit) = self.cache.get(&key) {
            super::telemetry::record_cache(true);
            return Ok((*hit).clone());
        }
        super::telemetry::record_cache(false);
        let shared = self
            .single_flight
            .run(key.clone(), move || async move {
                // FR5: the permit bounds execution, not idle waiting — only
                // the single-flight leader acquires one; followers coalesce
                // without consuming capacity. RAII: released on any exit.
                let _permit = self.acquire_query_permit().await?;
                let df = self.ctx.sql(query).await?;
                let batches = self.execute_recording_scan(df).await?;
                let shared: super::cache::CachedResult = std::sync::Arc::new(batches);
                // Raw SQL carries no window to classify → short TTL (FR2 safe
                // default). Inserted only on success — a failure is never cached.
                self.cache
                    .insert(key, std::sync::Arc::clone(&shared), TtlClass::Mutable);
                super::telemetry::set_cache_memory(self.cache.weighted_size());
                Ok(shared)
            })
            .await?;
        Ok((*shared).clone())
    }

    /// Execute a `DataFrame` via its physical plan, recording the per-signal
    /// scan volume (bytes/files) observed from the executed plan metrics (NFR5).
    /// Going through the physical plan (rather than `DataFrame::collect`) keeps
    /// the executed plan in hand so its scan counters can be read afterwards.
    ///
    /// Profiling seam (promql-plan-cache FR1): `DataFrame::create_physical_plan`
    /// bundles logical optimization + physical planning, so it is split here into
    /// its two constituent `SessionState` calls — `optimize` then the session's
    /// `QueryPlanner` on the *already-optimized* plan — which is byte-identical
    /// to the bundled call (it does exactly `optimize` → `query_planner
    /// .create_physical_plan`). Each stage (plus execution) records a
    /// [`super::telemetry::record_plan_stage`] duration; the seam only wraps
    /// timing around the existing steps and never changes a query result.
    async fn execute_recording_scan(
        &self,
        df: datafusion::dataframe::DataFrame,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        use std::time::Instant;
        let signal = signal_of_plan(df.logical_plan());
        let (state, logical) = df.into_parts();
        let t = Instant::now();
        let optimized = state.optimize(&logical)?;
        super::telemetry::record_plan_stage("optimize", t.elapsed());
        self.physical_and_collect(signal, &state, &optimized).await
    }

    /// [`Self::execute_recording_scan`] with the optimized-plan cache in front
    /// of the optimize stage (promql-plan-cache task 2a, ADR A′). On a shape
    /// hit the cached optimized plan is REBOUND — window literals rewritten,
    /// every `TableScan` swapped to the current window's scoped provider — and
    /// goes straight to physical planning; `optimize` is skipped (and its
    /// stage histogram not recorded, the deterministic hit proxy). On a miss
    /// the plan optimizes as usual and is inserted iff its rebind is provably
    /// total (identity self-check); otherwise the shape bypasses forever
    /// (correct-but-slow, never guessed). Used by the `DataFrame` paths
    /// ([`Self::collect_scoped`]); the raw-SQL paths keep
    /// [`Self::execute_recording_scan`].
    async fn execute_plan_cached(
        &self,
        df: datafusion::dataframe::DataFrame,
        step_ns: i64,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        use super::plan_cache::{self, CachedPlan, PlanCacheKey, PlanCacheOutcome};
        use std::time::Instant;
        let signal = signal_of_plan(df.logical_plan());
        let (state, logical) = df.into_parts();
        let shape = plan_cache::analyze(&logical);
        let optimized = match shape {
            Err(_) => {
                // Unanalyzable plan (never expected for our lowerings): bypass
                // the cache, optimize fresh — correctness over reuse.
                self.plan_cache.note(PlanCacheOutcome::Bypass);
                let t = Instant::now();
                let optimized = state.optimize(&logical)?;
                super::telemetry::record_plan_stage("optimize", t.elapsed());
                optimized
            }
            Ok(shape) => {
                let key = PlanCacheKey {
                    shape: shape.shape.clone(),
                    step_ns,
                    tables: shape.tables.clone(),
                    inventory_generation: self.inventory_snapshot().generation(),
                    lookback_cfg: (self.metadata_default_range_secs, self.instant_lookback_secs),
                };
                let cached = self.plan_cache.get(&key);
                let rebound = cached
                    .as_ref()
                    .and_then(|hit| plan_cache::rebind(hit, &shape));
                match (rebound, cached) {
                    // Hit: serve the rebound plan; optimize skipped entirely.
                    (Some(rebound), _) => {
                        self.plan_cache.note(PlanCacheOutcome::Hit);
                        rebound
                    }
                    // An entry existed but could not be rebound onto this
                    // window: bypass (fresh optimize), never guess.
                    (None, Some(_)) => {
                        self.plan_cache.note(PlanCacheOutcome::Bypass);
                        let t = Instant::now();
                        let optimized = state.optimize(&logical)?;
                        super::telemetry::record_plan_stage("optimize", t.elapsed());
                        optimized
                    }
                    // Miss: optimize fresh; insert only when the identity
                    // rebind round-trips (proves every window literal in the
                    // *optimized* plan is covered and every scan swappable).
                    (None, None) => {
                        self.plan_cache.note(PlanCacheOutcome::Miss);
                        let t = Instant::now();
                        let optimized = state.optimize(&logical)?;
                        super::telemetry::record_plan_stage("optimize", t.elapsed());
                        let candidate = CachedPlan {
                            optimized: optimized.clone(),
                            window_values: shape.window_values.clone(),
                        };
                        if plan_cache::rebind(&candidate, &shape).is_some() {
                            self.plan_cache.insert(key, std::sync::Arc::new(candidate));
                        }
                        optimized
                    }
                }
            }
        };
        self.physical_and_collect(signal, &state, &optimized).await
    }

    /// Shared tail of the execution pipeline: physical-plan the (already
    /// optimized or rebound) logical plan, collect, and record the
    /// `physical`/`execute` stages plus per-signal scan volume.
    async fn physical_and_collect(
        &self,
        signal: &'static str,
        state: &datafusion::execution::session_state::SessionState,
        optimized: &datafusion::logical_expr::LogicalPlan,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        use std::time::Instant;
        let t = Instant::now();
        let plan = state
            .query_planner()
            .create_physical_plan(optimized, state)
            .await?;
        super::telemetry::record_plan_stage("physical", t.elapsed());
        let t = Instant::now();
        let batches =
            datafusion::physical_plan::collect(std::sync::Arc::clone(&plan), self.ctx.task_ctx())
                .await?;
        super::telemetry::record_plan_stage("execute", t.elapsed());
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

    /// A `DataFrame` over `name` restricted to the files whose interval
    /// overlaps `scope` — the per-query file-pruning entry point (FR1,
    /// [ADR](../../docs/workspace/backend-metrics-perf/adrs/per-query-file-pruning.md);
    /// no query-time widening since promql-plan-cache FR4).
    ///
    /// The filtered list backs an **unregistered** `ListingTable`, so the
    /// registered tables (and the refresh swap protocol) are untouched.
    /// Superset guarantee ⇒ result equality: filtered to the same window, this
    /// returns identical rows to [`Self::table`] — pruning is invisible in
    /// results. An all-pruned list yields an empty `MemTable` with the table's
    /// schema; a name unknown to the inventory falls back to the registered
    /// full table.
    ///
    /// Note for callers caching the collected result: the scan is built with
    /// `name` as its plan-display table name (not `ctx.read_table`'s anonymous
    /// `?table?`), so cache keys derived from the plan display keep the table
    /// identity — two same-window scans of `metrics` vs a rollup tier must not
    /// collide (their values differ). The cache key must additionally carry
    /// the window itself, via the callers' usual time-filter literals in the
    /// plan; any residual same-display collision is then between scans of the
    /// same table over the same window, which result equality makes benign.
    pub async fn table_scoped(
        &self,
        name: &str,
        scope: QueryScope,
    ) -> crate::Result<datafusion::dataframe::DataFrame> {
        let files = self.inventory_snapshot().scoped_files(name, scope);
        let (Some(files), Some(schema)) = (files, table_schema(name)) else {
            return self.table(name).await;
        };
        let provider = if files.is_empty() {
            empty_provider(schema)?
        } else {
            listing_provider(&files, schema)?
        };
        let source = datafusion::datasource::provider_as_source(provider);
        let plan =
            datafusion::logical_expr::LogicalPlanBuilder::scan(name, source, None)?.build()?;
        Ok(datafusion::dataframe::DataFrame::new(
            self.ctx.state(),
            plan,
        ))
    }

    /// Snapshot of the current file inventory (cheap `Arc` clone; the read
    /// guard never crosses an `await`).
    pub(crate) fn inventory_snapshot(&self) -> Arc<FileInventory> {
        Arc::clone(
            &self
                .inventory
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Replace the inventory (tests only — e.g. to exercise the
    /// unknown-to-inventory fallback of [`Self::table_scoped`]).
    #[cfg(test)]
    pub(crate) fn set_inventory_for_test(&self, inventory: FileInventory) {
        *self
            .inventory
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(inventory);
    }

    /// Execute a built `DataFrame`, collecting Arrow batches — the plan-based twin
    /// of [`Self::sql`]. Equivalent to [`Self::collect_scoped`] with no window:
    /// the cached entry classifies as mutable → short TTL (FR2 safe default).
    pub async fn collect(
        &self,
        df: datafusion::dataframe::DataFrame,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        self.collect_scoped(df, None).await
    }

    /// [`Self::collect`] carrying the query's time window for cache TTL
    /// classification (FR2, [cache-invalidation-scope ADR](../../docs/workspace/backend-metrics-perf/adrs/cache-invalidation-scope.md)):
    /// a window entirely sealed against wall-clock now (`hi < now − 1 day`,
    /// [`QueryScope::is_sealed`] — the same rule as the tier boundary) caches
    /// under the long sealed TTL and so survives catalog refreshes; a mutable
    /// or absent window keeps the short TTL. Cached on the plan's indented
    /// display (ADR: plan-cache-keying), reusing the same moka cache +
    /// telemetry contract.
    pub async fn collect_scoped(
        &self,
        df: datafusion::dataframe::DataFrame,
        scope: Option<QueryScope>,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        self.collect_scoped_stepped(df, scope, 0).await
    }

    /// [`Self::collect_scoped`] carrying the query's `step_ns` (0 when the
    /// path has none) — a plan-cache key component (promql-plan-cache task 2a:
    /// the range paths pass their step so shapes never alias across steps).
    /// The plan cache runs *inside* the single-flight leader, so the result
    /// cache and request coalescing stay in front of it: a result-cache hit
    /// never touches the plan cache.
    pub async fn collect_scoped_stepped(
        &self,
        df: datafusion::dataframe::DataFrame,
        scope: Option<QueryScope>,
        step_ns: i64,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        use super::cache::{CacheKey, QueryCache, TtlClass};
        let key = CacheKey::for_sql(&df.logical_plan().display_indent().to_string());
        if let Some(hit) = self.cache.get(&key) {
            super::telemetry::record_cache(true);
            return Ok((*hit).clone());
        }
        super::telemetry::record_cache(false);
        let shared = self
            .single_flight
            .run(key.clone(), move || async move {
                // FR5: leader-only execution permit (see `Self::sql`).
                let _permit = self.acquire_query_permit().await?;
                let batches = self.execute_plan_cached(df, step_ns).await?;
                // Wall-clock now: TTL classification is inherently wall-clock,
                // like the moka TTL it selects (and the tier boundary it
                // mirrors).
                let class = TtlClass::classify(scope, super::now_unix_ns());
                let shared: super::cache::CachedResult = std::sync::Arc::new(batches);
                // Inserted only on success — a failure is never cached (FR3).
                self.cache
                    .insert(key, std::sync::Arc::clone(&shared), class);
                super::telemetry::set_cache_memory(self.cache.weighted_size());
                Ok(shared)
            })
            .await?;
        Ok((*shared).clone())
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
        use super::cache::{CacheKey, QueryCache, TtlClass};
        let key = CacheKey::for_sql(query);
        if let Some(hit) = self.cache.get(&key) {
            super::telemetry::record_cache(true);
            return Ok((*hit).clone());
        }
        super::telemetry::record_cache(false);
        let shared = self
            .single_flight
            .run(key.clone(), move || async move {
                // FR5: leader-only execution permit (see `Self::sql`).
                let _permit = self.acquire_query_permit().await?;
                let options = datafusion::execution::context::SQLOptions::new()
                    .with_allow_ddl(false)
                    .with_allow_dml(false)
                    .with_allow_statements(false);
                let df = self.ctx.sql_with_options(query, options).await?;
                let batches = self.execute_recording_scan(df).await?;
                let shared: super::cache::CachedResult = std::sync::Arc::new(batches);
                // Untrusted SQL carries no window to classify → short TTL (FR2
                // safe default). Inserted only on success — a failure is never
                // cached.
                self.cache
                    .insert(key, std::sync::Arc::clone(&shared), TtlClass::Mutable);
                super::telemetry::set_cache_memory(self.cache.weighted_size());
                Ok(shared)
            })
            .await?;
        Ok((*shared).clone())
    }

    /// Re-list storage for newly written files (called periodically by the
    /// server). Freshly discovered data becomes visible **within the cache
    /// TTL** — the same 15s bound the cache key's time bucketing already
    /// imposes. The cache is deliberately *not* cleared here (FR2,
    /// [cache-invalidation-scope ADR](../../docs/workspace/backend-metrics-perf/adrs/cache-invalidation-scope.md)):
    /// a blanket clear every refresh interval meant a dashboard never hit the
    /// cache; per-entry TTL now bounds staleness instead, and sealed-window
    /// entries ([`Self::collect_scoped`]) survive refreshes outright.
    ///
    /// The file inventory is swapped right after the tables, from the same
    /// `build_providers` walk (built fully before either swap) — the ADR
    /// invariant "inventory and registered tables derive from the same walk;
    /// replace both or neither".
    pub async fn refresh(&self) -> crate::Result<()> {
        let mut inventory = self.catalog.refresh(&self.ctx).await?;
        // Plan-cache generation (promql-plan-cache task 2a): the generation
        // identifies the inventory *content* — an unchanged walk keeps it (so
        // no-change refreshes don't evict rebindable plans), any file-set
        // change bumps it. The generation is a plan-cache key component, so a
        // bump makes every cached plan unreachable; drop them eagerly too.
        let previous = self.inventory_snapshot();
        if inventory.same_files(&previous) {
            inventory.set_generation(previous.generation());
        } else {
            inventory.set_generation(previous.generation().wrapping_add(1));
            self.plan_cache.invalidate_all();
        }
        *self
            .inventory
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(inventory);
        Ok(())
    }

    /// Plan-cache `(hits, misses, bypasses)` — test observability.
    #[cfg(test)]
    pub(crate) fn plan_cache_counts(&self) -> (u64, u64, u64) {
        self.plan_cache.counts()
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
    fn test_catalog_metric_schema_has_value_aggregate_cols() {
        // FR6: the shared metric schema (raw + tiers) carries the four per-bucket
        // scalar-value aggregates, each Float64 and nullable (raw files null them).
        let schema = metric_union_schema();
        for name in ["value_min", "value_max", "value_sum", "value_count"] {
            let f = schema
                .field_with_name(name)
                .unwrap_or_else(|_| panic!("missing {name}"));
            assert_eq!(f.data_type(), &DataType::Float64, "{name} must be Float64");
            assert!(f.is_nullable(), "{name} must be nullable");
        }
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
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(StringArray::from(vals))]).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_catalog_refresh_picks_up_new_file() {
        use crate::querier::cache::QueryCache;
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
        // refresh() no longer clears the cache (FR2) — new data is promised
        // "within the TTL"; model the TTL lapse deterministically.
        engine.cache.clear();
        assert_eq!(count(&engine, "logs").await, 3);
        // Idempotent: a second refresh keeps the count.
        engine.refresh().await.unwrap();
        engine.cache.clear();
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
            Arc::clone(&fixture_schema),
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
    async fn test_refresh_keeps_metrics_and_drops_vanished_tier() {
        // refresh() re-registers over the current files WITHOUT deregistering
        // first (the "No table named 'metrics'" race: the old code deregistered
        // every table, then ran the file-listing walk, leaving a window where a
        // concurrent query planned against a missing table). It must still drop a
        // rollup tier whose files have all gone (e.g. retention GC).
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp
            .path()
            .join("metrics")
            .join("gauge")
            .join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        write_min_log_parquet(&dir.join("m.parquet"), 2);
        write_min_log_parquet(&dir.join("rollup-5m.parquet"), 1);
        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        assert!(engine.has_table("metrics_5m"), "tier registered initially");
        assert!(engine.has_table("metrics"));

        // the tier's files vanish; a refresh must drop the now-stale tier table…
        std::fs::remove_file(dir.join("rollup-5m.parquet")).unwrap();
        engine.refresh().await.unwrap();
        assert!(
            !engine.has_table("metrics_5m"),
            "vanished tier must be deregistered"
        );
        // …while the main table stays continuously registered (no gap, no loss).
        assert!(engine.has_table("metrics"), "main table stays registered");
        assert_eq!(count(&engine, "metrics").await, 2);
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
            Arc::clone(&schema),
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

    #[tokio::test]
    async fn test_querier_reads_each_datum_once_across_chunk_level() {
        // write-side-small-files task 1: the supersession lattice gained the
        // open-hour chunk level (raw → chunk → hourly). With raw, chunk and
        // hourly files ALL coexisting on disk (no GC ran), the querier must
        // still read each datum exactly once.
        use crate::querier::compaction::{Compactor, CompactorConfig};
        let tmp = tempfile::tempdir().unwrap();
        let day2 = JUN01_NS + DAY_NS; // 2026-06-02T00:00:00Z
        let h8 = day2 + 8 * HOUR_NS;
        // Two exact-bounds raws in chunk [08:00, 08:05) + a leftover raw in
        // the hour's tail — one row each.
        write_timed_log(
            tmp.path(),
            "2026-06-02",
            h8 + 10 * NS_PER_SEC,
            h8 + 20 * NS_PER_SEC,
            &[h8 + 10 * NS_PER_SEC],
        );
        write_timed_log(
            tmp.path(),
            "2026-06-02",
            h8 + 60 * NS_PER_SEC,
            h8 + 70 * NS_PER_SEC,
            &[h8 + 60 * NS_PER_SEC],
        );
        write_timed_log(
            tmp.path(),
            "2026-06-02",
            h8 + 50 * MINUTE_NS,
            h8 + 50 * MINUTE_NS,
            &[h8 + 50 * MINUTE_NS],
        );
        let date = chrono::NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let compactor = Compactor::new(tmp.path(), CompactorConfig::default());
        // 08:08 — chunk pass merges the closed chunk; 10:30 — hourly pass
        // absorbs the chunk + leftover raw.
        compactor
            .compact_active_day("logs", date.and_hms_opt(8, 8, 0).unwrap().and_utc())
            .await
            .unwrap();
        compactor
            .compact_active_day("logs", date.and_hms_opt(10, 30, 0).unwrap().and_utc())
            .await
            .unwrap();
        // All levels coexist: 3 raw + 1 chunk + 1 hourly (5 files, 8 rows raw-summed).
        let dir = tmp.path().join("logs").join("dt=2026-06-02");
        let on_disk = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
            .count();
        assert_eq!(on_disk, 5, "raw ∪ chunk ∪ hourly all still on disk");

        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        assert_eq!(
            count(&engine, "logs").await,
            3,
            "each datum read exactly once across raw ∪ chunk ∪ hourly"
        );
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

    // ---- backend-metrics-perf task 2: retained inventory + table_scoped ----

    use crate::querier::inventory::{FileInventory, QueryScope};

    const NS_PER_SEC: i64 = 1_000_000_000;
    const MINUTE_NS: i64 = 60 * NS_PER_SEC;
    const HOUR_NS: i64 = 60 * MINUTE_NS;
    const DAY_NS: i64 = 24 * HOUR_NS;
    /// 2026-06-01T00:00:00Z (anchored: `date -u -d '2026-06-01' +%s`).
    const JUN01_NS: i64 = 1_780_272_000 * NS_PER_SEC;
    /// The whole timeline — every parsed interval overlaps it.
    const ALL_TIME: QueryScope = QueryScope {
        lo_ns: i64::MIN,
        hi_ns: i64::MAX,
    };

    /// Write a minimal log file under `logs/dt=<day>/` with the task-1b
    /// exact-bounds name `<min>-<max>-<uuid>.parquet` covering
    /// `[day+12:00, day+12:30]`, so the inventory parses an exact interval.
    fn write_bounded_log(root: &std::path::Path, day: &str, day_ns: i64, rows: i64) -> PathBuf {
        let min = day_ns + 12 * HOUR_NS;
        let max = min + 30 * MINUTE_NS;
        let times: Vec<i64> = (0..rows).map(|i| min + i * MINUTE_NS).collect();
        write_timed_log(root, day, min, max, &times)
    }

    /// Row-count helper for an unregistered (scoped) DataFrame.
    async fn rows(df: datafusion::dataframe::DataFrame) -> usize {
        df.collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    /// Write a log file under `logs/dt=<day>/` with the exact-bounds name
    /// `<min>-<max>-<uuid>.parquet` and one row per timestamp in `times_ns`
    /// (the sink invariant: the name carries the batch's true min/max).
    fn write_timed_log(
        root: &std::path::Path,
        day: &str,
        min_ns: i64,
        max_ns: i64,
        times_ns: &[i64],
    ) -> PathBuf {
        use datafusion::arrow::array::TimestampNanosecondArray;
        let dir = root.join("logs").join(format!("dt={day}"));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!(
            "{min_ns}-{max_ns}-550e8400-e29b-41d4-a716-446655440000.parquet"
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            ts("time_unix_nano", true),
        ]));
        let svc: Vec<&str> = std::iter::repeat_n("svc", times_ns.len()).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(svc)),
                Arc::new(TimestampNanosecondArray::from(times_ns.to_vec()).with_timezone("UTC")),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    /// `CAST(time_unix_nano AS BIGINT) BETWEEN lo AND hi` (mirrors
    /// `prometheus::prom_time_between`).
    fn window_filter(lo_ns: i64, hi_ns: i64) -> datafusion::logical_expr::Expr {
        use datafusion::logical_expr::expr_fn::cast;
        use datafusion::prelude::{col, lit};
        cast(col("time_unix_nano"), DataType::Int64).between(lit(lo_ns), lit(hi_ns))
    }

    #[tokio::test]
    async fn test_inventory_built_on_refresh() {
        // Inventory and registered tables derive from the same build_providers
        // walk — built at register time, replaced (both) at refresh.
        let tmp = tempfile::tempdir().unwrap();
        for (i, day) in [(0, "2026-06-01"), (1, "2026-06-02"), (2, "2026-06-03")] {
            write_bounded_log(tmp.path(), day, JUN01_NS + i * DAY_NS, 1);
        }
        // A rollup tier file is part of the same walk → scoped-capable too.
        let tier_dir = tmp
            .path()
            .join("metrics")
            .join("gauge")
            .join("dt=2026-06-01");
        std::fs::create_dir_all(&tier_dir).unwrap();
        write_min_log_parquet(&tier_dir.join("rollup-1h.parquet"), 1);

        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        let inv = engine.inventory_snapshot();
        assert_eq!(inv.scoped_files("logs", ALL_TIME).unwrap().len(), 3);
        assert_eq!(inv.scoped_files("traces", ALL_TIME).unwrap().len(), 0);
        assert_eq!(inv.scoped_files("metrics_1h", ALL_TIME).unwrap().len(), 1);
        assert!(inv.scoped_files("nope", ALL_TIME).is_none());

        // A 4th day appears; refresh swaps in a new inventory with it. The
        // file-set change bumps the snapshot generation (plan-cache key
        // component, promql-plan-cache task 2a)…
        assert_eq!(inv.generation(), 0);
        write_bounded_log(tmp.path(), "2026-06-04", JUN01_NS + 3 * DAY_NS, 1);
        engine.refresh().await.unwrap();
        let inv = engine.inventory_snapshot();
        assert_eq!(inv.scoped_files("logs", ALL_TIME).unwrap().len(), 4);
        assert_eq!(inv.generation(), 1, "changed file set bumps generation");
        // …while a no-change refresh keeps it (cached plans stay reachable).
        engine.refresh().await.unwrap();
        assert_eq!(
            engine.inventory_snapshot().generation(),
            1,
            "unchanged file set keeps the generation"
        );
    }

    #[tokio::test]
    async fn test_table_scoped_excludes_out_of_window_files() {
        // 15-min scope over a 3-day fixture: only the in-window file is listed
        // and scanned; the registered full table is untouched.
        let tmp = tempfile::tempdir().unwrap();
        let day2 = JUN01_NS + DAY_NS;
        write_bounded_log(tmp.path(), "2026-06-01", JUN01_NS, 1);
        let in_window = write_bounded_log(tmp.path(), "2026-06-02", day2, 2);
        write_bounded_log(tmp.path(), "2026-06-03", JUN01_NS + 2 * DAY_NS, 4);

        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        // 12:00–12:15 on day 2: only day 2's file truly overlaps.
        let scope = QueryScope {
            lo_ns: day2 + 12 * HOUR_NS,
            hi_ns: day2 + 12 * HOUR_NS + 15 * MINUTE_NS,
        };
        assert_eq!(
            engine
                .inventory_snapshot()
                .scoped_files("logs", scope)
                .unwrap(),
            vec![in_window.clone()]
        );
        let scoped = engine.table_scoped("logs", scope).await.unwrap();
        assert_eq!(rows(scoped).await, 2);
        // Empty filtered list → empty MemTable with the table's schema.
        let empty_scope = QueryScope {
            lo_ns: 0,
            hi_ns: 1_000,
        };
        let empty = engine.table_scoped("logs", empty_scope).await.unwrap();
        assert_eq!(empty.schema().fields().len(), 18);
        assert_eq!(rows(empty).await, 0);
        // Registered full table behaviour unchanged.
        assert_eq!(count(&engine, "logs").await, 7);
    }

    #[tokio::test]
    async fn test_table_scoped_equals_full_table_filtered() {
        // Result-equality invariant: the scoped table filtered to the window
        // returns identical rows to the full registered table under the same
        // filter — pruning is invisible in results. The scoped file list is a
        // subset of the full table's files, so scoped rows ⊆ full rows and
        // count equality ⇒ row equality.
        let tmp = tempfile::tempdir().unwrap();
        let day2 = JUN01_NS + DAY_NS;
        let noon = day2 + 12 * HOUR_NS;
        // A: straddles the scope's lower boundary (some rows in, some out).
        write_timed_log(
            tmp.path(),
            "2026-06-02",
            noon - 30 * MINUTE_NS,
            noon + 10 * MINUTE_NS,
            &[
                noon - 30 * MINUTE_NS,
                noon + 5 * MINUTE_NS,
                noon + 10 * MINUTE_NS,
            ],
        );
        // B: fully inside the scope.
        write_timed_log(
            tmp.path(),
            "2026-06-02",
            noon + MINUTE_NS,
            noon + 14 * MINUTE_NS,
            &[
                noon + MINUTE_NS,
                noon + 7 * MINUTE_NS,
                noon + 14 * MINUTE_NS,
            ],
        );
        // D: ends 30 min below the scope → no true overlap, EXCLUDED (FR4:
        // exact bounds are trusted, no query-time widening).
        let d = write_timed_log(
            tmp.path(),
            "2026-06-02",
            noon - 70 * MINUTE_NS,
            noon - 30 * MINUTE_NS,
            &[noon - 70 * MINUTE_NS, noon - 30 * MINUTE_NS],
        );
        // C: a day earlier, far outside the scope → excluded.
        write_timed_log(
            tmp.path(),
            "2026-06-01",
            JUN01_NS + 12 * HOUR_NS,
            JUN01_NS + 12 * HOUR_NS + 30 * MINUTE_NS,
            &[
                JUN01_NS + 12 * HOUR_NS,
                JUN01_NS + 12 * HOUR_NS + 10 * MINUTE_NS,
            ],
        );

        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        let scope = QueryScope {
            lo_ns: noon,
            hi_ns: noon + 15 * MINUTE_NS,
        };
        let files = engine
            .inventory_snapshot()
            .scoped_files("logs", scope)
            .unwrap();
        assert_eq!(files.len(), 2, "true-overlap files A and B only: {files:?}");
        assert!(
            !files.contains(&d),
            "no query-time widening: the 30-min-away file is excluded (FR4)"
        );

        let filter = window_filter(scope.lo_ns, scope.hi_ns);
        let full = engine
            .table("logs")
            .await
            .unwrap()
            .filter(filter.clone())
            .unwrap();
        let scoped = engine
            .table_scoped("logs", scope)
            .await
            .unwrap()
            .filter(filter)
            .unwrap();
        let full_rows = rows(full).await;
        assert_eq!(full_rows, 5, "A contributes 2, B contributes 3");
        assert_eq!(rows(scoped).await, full_rows);
    }

    #[tokio::test]
    async fn test_exact_bounds_files_no_query_margin() {
        // FR4 (promql-plan-cache task 3): exact-bounds intervals are trusted
        // as written — `scoped_files` applies NO query-time widening, so a
        // file whose interval ends 30 min before the scope (well inside the
        // old 1 h margin, which would have included it) is excluded; only
        // true-overlap files are listed.
        let tmp = tempfile::tempdir().unwrap();
        let noon = JUN01_NS + 12 * HOUR_NS;
        // A: straddles the scope's lower boundary → true overlap, included.
        let a = write_timed_log(
            tmp.path(),
            "2026-06-01",
            noon - 30 * MINUTE_NS,
            noon + 10 * MINUTE_NS,
            &[noon - 30 * MINUTE_NS, noon + 10 * MINUTE_NS],
        );
        // B: ends 30 min before the scope → no overlap, excluded.
        write_timed_log(
            tmp.path(),
            "2026-06-01",
            noon - 70 * MINUTE_NS,
            noon - 30 * MINUTE_NS,
            &[noon - 70 * MINUTE_NS, noon - 30 * MINUTE_NS],
        );
        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        let scope = QueryScope {
            lo_ns: noon,
            hi_ns: noon + 15 * MINUTE_NS,
        };
        assert_eq!(
            engine
                .inventory_snapshot()
                .scoped_files("logs", scope)
                .unwrap(),
            vec![a],
            "only the true-overlap file A survives — no 1 h widening"
        );
    }

    #[tokio::test]
    async fn test_table_scoped_unknown_table_falls_back() {
        let tmp = tempfile::tempdir().unwrap();
        write_bounded_log(tmp.path(), "2026-06-01", JUN01_NS, 3);
        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();
        let scope = QueryScope {
            lo_ns: 0,
            hi_ns: 1_000,
        };
        // A name unknown to both inventory and catalog behaves as engine.table.
        assert!(engine.table("nope").await.is_err());
        assert!(engine.table_scoped("nope", scope).await.is_err());
        // A table absent from the inventory falls back to the REGISTERED full
        // table — no pruning: the far-out-of-window scope still sees all rows.
        engine.set_inventory_for_test(FileInventory::default());
        let df = engine.table_scoped("logs", scope).await.unwrap();
        assert_eq!(rows(df).await, 3);
    }

    #[tokio::test]
    async fn test_sealed_entry_survives_refresh() {
        // FR2 (cache-invalidation-scope ADR, policy B+D): refresh() no longer
        // clears the cache — a sealed-classified entry is still a hit after it.
        use crate::querier::cache::{CacheKey, QueryCache, TtlClass};
        let tmp = tempfile::tempdir().unwrap();
        // The fixture covers [12:00, 12:30] of 2026-06-01 — entirely sealed
        // relative to any real wall clock this test can run under.
        write_bounded_log(tmp.path(), "2026-06-01", JUN01_NS, 2);
        let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
            .await
            .unwrap();

        // Directly inserted sealed entry: survives refresh (no blanket clear).
        let sealed_key = CacheKey::for_sql("sealed probe");
        engine
            .cache
            .insert(sealed_key.clone(), Arc::new(Vec::new()), TtlClass::Sealed);

        // End-to-end classification: a collect_scoped over a sealed window
        // inserts under the sealed TTL via the scope threading.
        let scope = QueryScope {
            lo_ns: JUN01_NS,
            hi_ns: JUN01_NS + DAY_NS,
        };
        let df = engine.table_scoped("logs", scope).await.unwrap();
        let plan_key = CacheKey::for_sql(&df.logical_plan().display_indent().to_string());
        let batches = engine.collect_scoped(df, Some(scope)).await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(engine.cache.get(&plan_key).is_some(), "inserted on miss");

        engine.refresh().await.unwrap();
        assert!(
            engine.cache.get(&sealed_key).is_some(),
            "sealed entry must survive a catalog refresh"
        );
        assert!(
            engine.cache.get(&plan_key).is_some(),
            "sealed-scoped collect result must survive a catalog refresh"
        );
    }

    /// promql-plan-cache task 2a: a second same-shape query over a slid
    /// window must HIT the plan cache and skip `state.optimize()` — proxied
    /// deterministically by the `optimize` stage histogram receiving exactly
    /// ONE sample across the two executions (`physical` gets two), plus the
    /// `sol_querier_plan_cache_requests_total{result=hit|miss}` counters. No
    /// wall-clock assertions. Local-recorder pattern as in
    /// [`test_query_records_real_bytes_scanned`].
    #[test]
    fn test_plan_cache_hit_skips_optimize() {
        use metrics_util::MetricKind;
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

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
                    // Exact-bounds fixture covering [12:00, 12:30].
                    write_bounded_log(tmp.path(), "2026-06-01", JUN01_NS, 3);
                    let engine = QueryEngine::new(&engine_opts(tmp.path().to_path_buf()))
                        .await
                        .unwrap();
                    let noon = JUN01_NS + 12 * HOUR_NS;
                    // Two same-shape windows (both overlap the fixture file, so
                    // both scope to the same ListingTable provider shape); the
                    // differing window literals defeat the result cache but
                    // not the plan cache.
                    let windows = [
                        (noon, noon + 5 * MINUTE_NS),
                        (noon + 10 * MINUTE_NS, noon + 15 * MINUTE_NS),
                    ];
                    for (lo, hi) in windows {
                        let scope = QueryScope {
                            lo_ns: lo,
                            hi_ns: hi,
                        };
                        let df = engine
                            .table_scoped("logs", scope)
                            .await
                            .unwrap()
                            .filter(window_filter(lo, hi))
                            .unwrap();
                        engine.collect_scoped(df, Some(scope)).await.unwrap();
                    }
                    assert_eq!(
                        engine.plan_cache_counts(),
                        (1, 1, 0),
                        "(hits, misses, bypasses): first window misses+inserts, second rebinds"
                    );
                });
            });
        })
        .join()
        .unwrap();

        let s = snap.snapshot().into_vec();
        let stage_samples = |stage: &str| {
            s.iter()
                .find_map(|(k, _, _, v)| {
                    (k.kind() == MetricKind::Histogram
                        && k.key().name() == "querier_plan_stage_duration_seconds"
                        && k.key()
                            .labels()
                            .any(|l| l.key() == "stage" && l.value() == stage))
                    .then_some(v)
                })
                .map_or(0, |v| {
                    let DebugValue::Histogram(samples) = v else {
                        panic!("expected histogram value for stage {stage}");
                    };
                    samples.len()
                })
        };
        assert_eq!(
            stage_samples("physical"),
            2,
            "both executions are physically planned"
        );
        assert_eq!(
            stage_samples("optimize"),
            1,
            "the plan-cache hit must skip (and not record) the optimize stage"
        );
        let plan_cache_count = |result: &str| {
            s.iter()
                .find_map(|(k, _, _, v)| {
                    (k.kind() == MetricKind::Counter
                        && k.key().name() == "querier_plan_cache_requests_total"
                        && k.key()
                            .labels()
                            .any(|l| l.key() == "result" && l.value() == result))
                    .then_some(v)
                })
                .map_or(0, |v| {
                    let DebugValue::Counter(n) = v else {
                        panic!("expected counter value for result {result}");
                    };
                    *n
                })
        };
        assert_eq!(plan_cache_count("miss"), 1, "first execution misses");
        assert_eq!(plan_cache_count("hit"), 1, "second execution hits");
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

    /// FR5 ([concurrency-guardrail ADR](../../docs/workspace/backend-metrics-perf/adrs/concurrency-guardrail.md)):
    /// with `max_concurrent_queries = 1` and the only permit held, a
    /// distinct-key query sheds after the (tiny, test-overridden) bounded wait
    /// with the typed overload error — and records `sol_querier_shed_total`.
    /// Local-recorder counters need the dedicated-thread current-thread-runtime
    /// pattern of `test_query_records_real_bytes_scanned`.
    #[test]
    fn test_semaphore_limits_concurrency() {
        use metrics_util::MetricKind;
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

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
                    let mut opts = engine_opts(tmp.path().to_path_buf());
                    opts.guardrails.max_concurrent_queries = 1;
                    let mut engine = QueryEngine::new(&opts).await.unwrap();
                    engine.set_permit_acquire_timeout_for_test(
                        std::time::Duration::from_millis(20),
                    );
                    // Saturate the guardrail: hold the only permit.
                    let held = engine.hold_query_permit_for_test().await;
                    let err = engine
                        .sql("SELECT 1")
                        .await
                        .expect_err("second query must shed, not wait unboundedly");
                    assert!(
                        err.to_string().contains(OVERLOAD_MARKER),
                        "typed overload error expected, got: {err}"
                    );
                    // Capacity restored once the permit is released. (The shed
                    // failure was not cached and its flight entry is gone, so
                    // this same-key call re-executes.)
                    drop(held);
                    engine
                        .sql("SELECT 1")
                        .await
                        .expect("permit available again after release");
                });
            });
        })
        .join()
        .unwrap();

        let s = snap.snapshot().into_vec();
        let shed = s.iter().find_map(|(k, _, _, v)| {
            (k.kind() == MetricKind::Counter && k.key().name() == "querier_shed_total")
                .then_some(v)
        });
        let DebugValue::Counter(n) = shed.expect("shed counter emitted") else {
            panic!("expected counter value");
        };
        assert_eq!(*n, 1, "exactly one shed recorded");
    }

    /// FR5: a failing query frees its permit (RAII `SemaphorePermit`) — with
    /// `max = 1` and a tiny bounded wait, a leaked permit would make the next
    /// call shed instead of succeeding.
    #[tokio::test]
    async fn test_permits_released_on_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut opts = engine_opts(tmp.path().to_path_buf());
        opts.guardrails.max_concurrent_queries = 1;
        let mut engine = QueryEngine::new(&opts).await.unwrap();
        engine.set_permit_acquire_timeout_for_test(std::time::Duration::from_millis(20));
        let err = engine
            .sql("SELECT no_such_col FROM no_such_table")
            .await
            .expect_err("query against a missing table fails");
        assert!(
            !err.to_string().contains(OVERLOAD_MARKER),
            "failure must be the query error, not overload: {err}"
        );
        engine
            .sql("SELECT 1")
            .await
            .expect("the failed query must have released its permit");
    }
}
