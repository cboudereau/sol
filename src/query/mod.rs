// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Sol query backend — serves Prometheus/Tempo/Loki + SQL APIs over Parquet via DataFusion.
//!
//! Built incrementally per `docs/workspace/parquet-backend/TASKS.md`.
//! Gated behind the `query-backend` feature; absent from default builds.

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
pub mod logql;
pub mod loki;
pub mod plan;
pub mod prometheus;
pub mod rollup;
mod routes;
pub mod sql;
pub mod telemetry;
pub mod tempo;
pub mod traceql;
pub mod units;
mod udf;
pub use catalog::{ParquetCatalog, QueryEngine, SignalTable};

use crate::config::query::{CompactorOptions, QuerierOptions};

/// Handle to a running Sol query-backend component (querier or compactor).
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
