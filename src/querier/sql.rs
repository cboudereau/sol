// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Cross-signal SQL endpoint (task 13).
//!
//! Exposes DataFusion SQL over the registered catalog — the differentiator the
//! three Grafana query languages can't offer (`logs ⨝ traces` on `trace_id`,
//! `metrics ⨝ traces` on `service_name` + time window). `POST /api/v1/sql`,
//! JSON results, stateless ([FR9](../../../docs/workspace/parquet-backend/DESIGN.md#fr9)).
//! Subject to the NFR9 max-bytes-scanned guardrail; reads compacted + rollup
//! files through the catalog.

use std::path::Path;

use serde_json::{Value, json};

/// The signal tables a query may touch (the catalog's registered tables).
const SIGNALS: [&str; 3] = ["logs", "traces", "metrics"];

fn err(msg: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    Box::<dyn std::error::Error + Send + Sync>::from(msg.into())
}

/// Conservative scan-size estimate (NFR9): sum the Parquet byte sizes of the
/// signal partitions the query references. Over-estimates (whole-table), which
/// is the safe direction for a guardrail.
fn estimate_scan_bytes(root: &Path, sql: &str) -> u64 {
    let lower = sql.to_lowercase();
    let mut total = 0u64;
    for signal in SIGNALS {
        if !lower.contains(signal) {
            continue;
        }
        total += dir_parquet_bytes(&root.join(signal));
    }
    total
}

fn dir_parquet_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            total += dir_parquet_bytes(&path);
        } else if path.extension().is_some_and(|e| e == "parquet")
            && let Ok(meta) = path.metadata()
        {
            total += meta.len();
        }
    }
    total
}

/// Serialise result batches to Grafana-consumable JSON: an array of row objects
/// plus a count, e.g. `{"rows": [{"col": v, …}, …], "count": n}`.
fn batches_to_json(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> crate::Result<Value> {
    use datafusion::arrow::json::ArrayWriter;
    let count: usize = batches
        .iter()
        .map(datafusion::arrow::record_batch::RecordBatch::num_rows)
        .sum();
    let mut buf = Vec::new();
    {
        let mut writer = ArrayWriter::new(&mut buf);
        for batch in batches {
            writer.write(batch).map_err(|e| err(e.to_string()))?;
        }
        writer.finish().map_err(|e| err(e.to_string()))?;
    }
    let rows: Value = serde_json::from_slice(&buf).unwrap_or_else(|_| json!([]));
    Ok(json!({ "rows": rows, "count": count }))
}

/// Run an ad-hoc SQL query, enforcing the scan guardrail (NFR9), and return
/// Grafana-consumable JSON. The `Err` message starts with `guardrail:` when the
/// query is rejected for exceeding the scan budget (mapped to HTTP 422).
pub async fn handle_sql(engine: &super::QueryEngine, sql: &str) -> crate::Result<Value> {
    let estimated = estimate_scan_bytes(engine.storage_root(), sql);
    if estimated > engine.max_scan_bytes() {
        super::telemetry::record_rejected("bytes");
        return Err(err(format!(
            "guardrail: query would scan ~{estimated} bytes, over the {} byte limit (NFR9); \
             narrow the time range or add a filter",
            engine.max_scan_bytes()
        )));
    }
    // Untrusted input: the restricted path rejects DDL/DML/statements, so a
    // client cannot read/write arbitrary files or mutate the catalog (NFR9).
    let batches = engine.sql_user(sql).await?;
    batches_to_json(&batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::querier::{GuardrailsConfig, QuerierOptions, StorageConfig};
    use datafusion::arrow::array::{
        FixedSizeBinaryArray, Int64Array, StringArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    fn hx(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn write(dir: &std::path::Path, schema: Arc<Schema>, batch: RecordBatch) {
        std::fs::create_dir_all(dir).unwrap();
        let f = std::fs::File::create(dir.join("f.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    // logs + traces + metrics sharing trace_id 3bc5…/service "client".
    async fn tri_engine(max_scan_bytes: u64) -> (crate::querier::QueryEngine, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let tid = hx("3bc59070ba6c121cad3d88a3f889b303");

        // logs
        let log_schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        ]));
        let log_batch = RecordBatch::try_new(
            Arc::clone(&log_schema),
            vec![
                Arc::new(StringArray::from(vec!["client"])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(vec![tid.clone()].into_iter()).unwrap(),
                ),
            ],
        )
        .unwrap();
        write(
            &root.join("logs").join("dt=2026-06-01"),
            log_schema,
            log_batch,
        );

        // traces
        let trace_schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "start_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("name", DataType::Utf8, false),
            Field::new("duration_nanos", DataType::Int64, false),
        ]));
        let trace_batch = RecordBatch::try_new(
            Arc::clone(&trace_schema),
            vec![
                Arc::new(StringArray::from(vec!["client"])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone("UTC"),
                ),
                Arc::new(FixedSizeBinaryArray::try_from_iter(vec![tid].into_iter()).unwrap()),
                Arc::new(StringArray::from(vec!["GET /x"])),
                Arc::new(Int64Array::from(vec![42_000_000i64])),
            ],
        )
        .unwrap();
        write(
            &root.join("traces").join("dt=2026-06-01"),
            trace_schema,
            trace_batch,
        );

        // metrics
        let metric_schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("double_value", DataType::Float64, true),
            Field::new("prom_name", DataType::Utf8, false),
        ]));
        let metric_batch = RecordBatch::try_new(
            Arc::clone(&metric_schema),
            vec![
                Arc::new(StringArray::from(vec!["client"])),
                Arc::new(StringArray::from(vec!["cpu"])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone("UTC"),
                ),
                Arc::new(datafusion::arrow::array::Float64Array::from(vec![0.5])),
                Arc::new(StringArray::from(vec!["cpu"])),
            ],
        )
        .unwrap();
        write(
            &root.join("metrics").join("dt=2026-06-01"),
            metric_schema,
            metric_batch,
        );

        let opts = QuerierOptions {
            storage: StorageConfig {
                path: root.into(),
                url: None,
            },
            guardrails: GuardrailsConfig {
                max_bytes_scanned: max_scan_bytes,
                ..Default::default()
            },
            ..QuerierOptions::default()
        };
        (crate::querier::QueryEngine::new(&opts).await.unwrap(), tmp)
    }

    #[tokio::test]
    async fn test_sql_select_over_each_signal_table() {
        let (engine, _tmp) = tri_engine(u64::MAX).await;
        for table in ["logs", "traces", "metrics"] {
            let v = handle_sql(&engine, &format!("SELECT count(*) AS n FROM {table}"))
                .await
                .unwrap();
            assert_eq!(v["count"], 1, "one count row for {table}: {v}");
            assert_eq!(v["rows"][0]["n"], 1, "one row in {table}: {v}");
        }
    }

    #[tokio::test]
    async fn test_join_logs_traces_on_trace_id() {
        let (engine, _tmp) = tri_engine(u64::MAX).await;
        let v = handle_sql(
            &engine,
            "SELECT l.body, t.name FROM logs l JOIN traces t ON l.trace_id = t.trace_id",
        )
        .await
        .unwrap();
        assert_eq!(v["count"], 1, "joined on shared trace_id: {v}");
        assert_eq!(v["rows"][0]["body"], "hello");
        assert_eq!(v["rows"][0]["name"], "GET /x");
    }

    #[tokio::test]
    async fn test_join_metrics_traces_on_service_and_time_window() {
        let (engine, _tmp) = tri_engine(u64::MAX).await;
        let v = handle_sql(
            &engine,
            "SELECT m.name AS metric, t.name AS span FROM metrics m JOIN traces t \
             ON m.service_name = t.service_name \
             AND CAST(m.time_unix_nano AS BIGINT) BETWEEN CAST(t.start_time_unix_nano AS BIGINT) - 1000000000 \
             AND CAST(t.start_time_unix_nano AS BIGINT) + 1000000000",
        )
        .await
        .unwrap();
        assert_eq!(v["count"], 1, "joined on service + time window: {v}");
        assert_eq!(v["rows"][0]["metric"], "cpu");
    }

    #[tokio::test]
    async fn test_sql_guardrail_rejects_oversize_scan() {
        // 1-byte budget → any real scan is rejected
        let (engine, _tmp) = tri_engine(1).await;
        let res = handle_sql(&engine, "SELECT * FROM logs").await;
        let e = res.unwrap_err().to_string();
        assert!(e.starts_with("guardrail:"), "clear guardrail error: {e}");
        assert!(e.contains("scan"), "{e}");
    }

    #[tokio::test]
    async fn test_sql_endpoint_rejects_ddl_dml_and_statements() {
        // NFR9 / B1: the untrusted endpoint must not allow arbitrary file
        // read/write or catalog mutation — only read-only SELECTs.
        let (engine, _tmp) = tri_engine(u64::MAX).await;
        for q in [
            "CREATE EXTERNAL TABLE evil STORED AS PARQUET LOCATION '/etc/hostname'",
            "COPY (SELECT * FROM logs) TO '/tmp/exfil.csv'",
            "DROP TABLE logs",
            "CREATE VIEW v AS SELECT * FROM logs",
            "INSERT INTO logs VALUES ('x')",
        ] {
            assert!(handle_sql(&engine, q).await.is_err(), "must reject: {q}");
        }
        // a read-only SELECT still works
        assert!(handle_sql(&engine, "SELECT 1 AS one").await.is_ok());
        assert!(
            handle_sql(&engine, "SELECT count(*) FROM logs")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_sql_result_json_consumable_by_grafana() {
        let (engine, _tmp) = tri_engine(u64::MAX).await;
        let v = handle_sql(&engine, "SELECT service_name, body FROM logs")
            .await
            .unwrap();
        // array of row objects with the selected columns
        let rows = v["rows"].as_array().expect("rows is an array");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["service_name"], "client");
        assert_eq!(rows[0]["body"], "hello");
    }
}
