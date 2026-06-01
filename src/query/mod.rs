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

mod catalog;
pub mod loki;
mod routes;
mod udf;
pub use catalog::{ParquetCatalog, QueryEngine, SignalTable};

use crate::config::query::Options;

/// Handle to the running Sol query backend.
///
/// Gracefully shuts down when dropped — the `oneshot` sender closing ends the
/// server task. Mirrors [`crate::api::Server`].
pub struct Server {
    _shutdown: oneshot::Sender<()>,
}

impl Server {
    /// Bind and spawn the query backend HTTP server on `opts.address`.
    ///
    /// Builds the [`QueryEngine`] (catalog over the configured Parquet storage),
    /// mounts the warp routes ([`routes::make_routes`]), serves over a manual
    /// hyper accept loop (mirroring [`crate::api::Server`]), and periodically
    /// refreshes the catalog. Gracefully shuts down when dropped.
    pub fn start(opts: &Options, handle: &Handle) -> crate::Result<Self> {
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
            let routes = routes::make_routes(engine.clone());
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
