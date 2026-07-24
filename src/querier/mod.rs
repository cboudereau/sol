// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Sol query backend — serves Prometheus/Tempo/Loki + SQL APIs over Parquet via DataFusion.
//!
//! Built incrementally per `docs/workspace/parquet-backend/TASKS.md`.
//! Gated behind the `querier-backend` feature; absent from default builds.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tracing::{Span, debug, error};

mod cache;
mod catalog;
pub mod compaction;
pub mod frontend;
mod group_key;
mod inventory;
pub mod logql;
pub mod loki;
pub mod plan;
mod plan_cache;
pub mod prometheus;
pub mod rollup;
mod routes;
mod single_flight;
pub mod sql;
pub mod telemetry;
pub mod tempo;
pub mod traceql;
mod udf;
pub mod units;
pub use catalog::{ParquetCatalog, QueryEngine, SignalTable};
pub use inventory::{FileInventory, QueryScope};

use crate::config::{compactor::CompactorOptions, querier::QuerierOptions};

/// Wall-clock now in unix nanoseconds. Captured at the request boundary by the
/// routes (the core query fns stay clock-free + testable — they take `now_ns`
/// explicitly): it anchors an omitted instant `time` (see `instant_anchor`)
/// and the wall-clock sealed/live tier boundary (see `resolve_metric_windows`)
/// so a historical-`end` query still routes long-sealed days to the rollup
/// tier. Also read by the cache's TTL classification
/// (`QueryEngine::collect_scoped`), which is inherently wall-clock like the
/// TTL it selects.
pub(crate) fn now_unix_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    )
    .unwrap_or(i64::MAX)
}

/// Handle to a running Sol querier-backend component (querier or compactor).
///
/// Gracefully shuts down when dropped — the `oneshot` sender closing ends the
/// server task. Mirrors [`crate::api::Server`].
pub struct Server {
    _shutdown: oneshot::Sender<()>,
}

impl Server {
    /// Spawn the periodic compactor loop (no HTTP): intraday → seal → rollup →
    /// GC every `interval_secs`, starting immediately.
    pub fn start_compactor(opts: &CompactorOptions, handle: &Handle) -> crate::Result<Self> {
        let (_shutdown, rx) = oneshot::channel::<()>();
        let _guard = handle.enter();
        let opts = opts.clone();
        handle.spawn(async move {
            let cfg = compaction::CompactorConfig {
                grace_days: opts.grace_days,
                retention_days: opts.retention_days,
                intraday: opts.intraday,
                hour_grace_secs: opts.hour_grace_secs,
                open_hour_chunks: opts.open_hour_chunks,
                chunk_secs: opts.chunk_secs,
                chunk_grace_secs: opts.chunk_grace_secs,
                delete_superseded: opts.delete_superseded,
                delete_grace_secs: opts.delete_grace_secs,
            };
            let compactor = compaction::Compactor::new(opts.storage.path.clone(), cfg);
            let mut tick = tokio::time::interval(Duration::from_secs(opts.interval_secs.max(1)));
            let shutdown = async {
                rx.await.ok();
            };
            tokio::pin!(shutdown);
            debug!(
                message = "Sol compactor started.",
                interval = opts.interval_secs
            );
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let now = chrono::Utc::now();
                        match compactor.run_once(now, opts.rollups).await {
                            Ok(report) => debug!(
                                message = "Sol compactor pass complete.",
                                partitions_sealed = report.partitions_sealed,
                                rows = report.rows,
                            ),
                            Err(error) => {
                                error!(message = "Sol compactor pass failed.", %error)
                            }
                        }
                    }
                    _ = &mut shutdown => break,
                }
            }
            debug!(message = "Sol compactor stopped.");
        });
        Ok(Server { _shutdown })
    }

    /// Bind and spawn the query backend HTTP server on `opts.address`.
    ///
    /// Builds the [`QueryEngine`] (catalog over the configured Parquet storage),
    /// mounts the warp routes ([`routes::make_routes`]), serves over a manual
    /// hyper accept loop (mirroring [`crate::api::Server`]), and periodically
    /// refreshes the catalog. Gracefully shuts down when dropped.
    pub fn start_querier(opts: &QuerierOptions, handle: &Handle) -> crate::Result<Self> {
        let (_shutdown, rx) = oneshot::channel::<()>();
        let _guard = handle.enter();
        let addr = opts.address;

        let std_listener = TcpListener::bind(addr)?;
        std_listener.set_nonblocking(true)?;

        let opts = opts.clone();
        let span = Span::current();
        handle.spawn(async move {
            let listener = tokio::net::TcpListener::from_std(std_listener)
                .expect("failed to create tokio TcpListener");

            let engine = match QueryEngine::new(&opts).await {
                Ok(engine) => Arc::new(engine),
                Err(error) => {
                    error!(message = "Sol query backend failed to build the query engine.", %error, %addr);
                    return;
                }
            };
            let routes = routes::make_routes(Arc::clone(&engine));
            debug!(message = "Sol query backend serving.", %addr);

            let mut refresh =
                tokio::time::interval(Duration::from_secs(opts.refresh_interval_secs.max(1)));
            refresh.tick().await; // consume the immediate first tick

            let shutdown = async {
                rx.await.ok();
            };
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (stream, _) = match result {
                            Ok(conn) => conn,
                            Err(err) => { error!("query backend accept error: {err:?}"); continue; }
                        };
                        let svc = tower::ServiceBuilder::new()
                            .layer(crate::http::build_http_trace_layer(span.clone()))
                            .service(warp::service(routes.clone()));
                        tokio::spawn(async move {
                            let io = hyper_util::rt::TokioIo::new(stream);
                            hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, hyper_util::service::TowerToHyperService::new(svc))
                                .await
                                .ok();
                        });
                    }
                    _ = refresh.tick() => { let _ = engine.refresh().await; }
                    _ = &mut shutdown => break,
                }
            }
            debug!(message = "Sol query backend stopped.", %addr);
        });

        Ok(Server { _shutdown })
    }
}

#[cfg(test)]
mod no_sql_invariant_tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                rs_files(&path, out);
            } else if path.extension().is_some_and(|x| x == "rs") {
                out.push(path);
            }
        }
    }

    /// FR6 — the "no SQL in core" invariant. Outside `sql.rs` (the user-SQL
    /// endpoint) and `#[cfg(test)]` fixtures, the query surface builds queries
    /// through the DataFusion `Expr`/`DataFrame` API only — no `format!`-built
    /// SQL strings. Both the read path (PromQL/LogQL/TraceQL lowering) and the
    /// write path (compaction sort-merge, rollup downsample) were migrated, so
    /// the only `.sql()` call left is `QueryEngine::sql` (a borrowed `&str`
    /// passthrough for the user endpoint), never a `format!` literal.
    #[test]
    fn test_no_format_sql_in_core() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/querier");
        let mut files = Vec::new();
        rs_files(&root, &mut files);
        assert!(!files.is_empty(), "expected to scan querier source files");

        for f in &files {
            if f.file_name().unwrap() == "sql.rs" {
                continue; // user-SQL endpoint: the one sanctioned SQL surface
            }
            let src = fs::read_to_string(f).unwrap();
            // Production region only — drop everything from the first test module.
            let prod = src.split("#[cfg(test)]").next().unwrap();
            // Strip line comments so SQL keywords in docs don't trip the gate.
            let code: String = prod
                .lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n");
            let rel = f.strip_prefix(&root).unwrap().display();

            assert!(
                !code.contains(".sql(&format!") && !code.contains(".sql(format!"),
                "{rel}: executes a `format!`-built SQL string — use the DataFrame API"
            );
            for kw in ["SELECT ", " FROM ", " WHERE ", " GROUP BY ", " JOIN "] {
                assert!(
                    !code.contains(kw),
                    "{rel}: contains a SQL-shaped string literal ({kw:?}) — \
                     query construction must go through `Expr`/`DataFrame`"
                );
            }
        }
    }

    /// promql-pushdown T7 — the metric-label read path extracts every metric
    /// label from the columnar `attributes` MAP, never by parsing a JSON string.
    /// `group_key.rs` (the `prom_group_key` grouping path) must be entirely
    /// JSON-free; in `udf.rs` only the `lookup_json` helper (the sanctioned path
    /// for the JSON `resource_attributes` / logs+traces `attributes` columns,
    /// which are out of scope) may parse JSON — the metric-MAP readers (`lookup_map`,
    /// `map_row_entries`, `map_row_normalized_labels`) must not. The histogram
    /// `bucket_counts`/`explicit_bounds` blobs in `prometheus.rs` also stay JSON.
    #[test]
    fn test_no_serde_json_in_label_path() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/querier");

        // group_key.rs: the metric grouping path is fully columnar — no JSON.
        let gk = fs::read_to_string(root.join("group_key.rs")).unwrap();
        let gk_prod = gk.split("#[cfg(test)]").next().unwrap();
        assert!(
            !gk_prod.contains("serde_json"),
            "group_key.rs: the prom_group_key path must read the columnar MAP, no JSON (T7)"
        );

        // udf.rs: the only sanctioned JSON parse is `lookup_json` (resource_attributes
        // / logs+traces). The metric-MAP read functions must not parse JSON.
        let udf = fs::read_to_string(root.join("udf.rs")).unwrap();
        let udf_prod = udf.split("#[cfg(test)]").next().unwrap();
        let from_str = udf_prod.matches("serde_json::from_str").count();
        assert_eq!(
            from_str, 1,
            "udf.rs: exactly one JSON parse expected (lookup_json for the JSON \
             attribute columns); the metric-MAP path must be parse-free (T7)"
        );
    }
}
