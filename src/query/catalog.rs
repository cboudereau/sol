//! Parquet table catalog + DataFusion query engine (task 2).
//!
//! Registers one DataFusion table per signal directory written by the file sink
//! (`logs/`, `traces/`, and `metrics/` as a single union table until task 14b
//! adds per-subtype directories). Schemas are declared explicitly here as the
//! binding contract with the codec ([parquet-multisignal](../../../docs/designs/20260527_parquet-multisignal.md));
//! DataFusion's schema adapter fills columns missing from a given file with null.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::MemTable;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use crate::config::query::Options;

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
    pub const ALL: [SignalTable; 3] = [SignalTable::Logs, SignalTable::Traces, SignalTable::Metrics];

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
        // common (15)
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
            if dir.is_dir() {
                let url = format!("file://{}/", dir.display());
                let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                    .with_file_extension(".parquet");
                ctx.register_listing_table(table.table_name(), &url, options, Some(schema), None)
                    .await?;
            } else {
                // Absent directory → empty table with the declared schema
                // (one empty partition; MemTable requires ≥1 partition).
                let empty = MemTable::try_new(schema, vec![vec![]])?;
                ctx.register_table(table.table_name(), Arc::new(empty))?;
            }
        }
        Ok(())
    }

    /// Re-register tables to pick up newly-created directories / files.
    /// (ListingTable re-lists files on each scan; this re-registers in case a
    /// previously-absent directory now exists.)
    pub async fn refresh(&self, ctx: &SessionContext) -> crate::Result<()> {
        for table in SignalTable::ALL {
            let _ = ctx.deregister_table(table.table_name());
        }
        self.register(ctx).await
    }
}

/// Thin wrapper over a DataFusion `SessionContext` with the signal catalog registered.
/// Sole query-engine dependency (NFR1); worker pool bounded so queries do not starve
/// ingestion (NFR5).
pub struct QueryEngine {
    ctx: SessionContext,
    catalog: ParquetCatalog,
}

impl QueryEngine {
    /// Build the engine from config: register the signal catalog in a DataFusion
    /// `SessionContext` with a bounded worker pool (NFR5).
    pub async fn new(opts: &Options) -> crate::Result<Self> {
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(4); // NFR5: bound the worker pool.
        let config = SessionConfig::new().with_target_partitions(parallelism);
        let mut ctx = SessionContext::new_with_config(config);
        // JSON extraction over the `attributes` string column (ADR 0039):
        // registers `json_get_str`/`json_get_*`, `->`/`->>`, `json_contains`, …
        datafusion_functions_json::register_all(&mut ctx)?;
        let catalog = ParquetCatalog::new(opts.storage.path.clone());
        catalog.register(&ctx).await?;
        Ok(Self { ctx, catalog })
    }

    /// Run a SQL query, collecting all result batches.
    pub async fn sql(
        &self,
        query: &str,
    ) -> crate::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        Ok(df.collect().await?)
    }

    /// Re-list storage for newly written files (called periodically by the server).
    pub async fn refresh(&self) -> crate::Result<()> {
        self.catalog.refresh(&self.ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;

    fn engine_opts(root: PathBuf) -> Options {
        Options {
            storage: crate::config::query::StorageConfig { path: root, url: None },
            ..Options::default()
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
        let schema = Arc::new(Schema::new(vec![Field::new("service_name", DataType::Utf8, false)]));
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
}
