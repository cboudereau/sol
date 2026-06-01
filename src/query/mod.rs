//! Sol query backend — serves Prometheus/Tempo/Loki + SQL APIs over Parquet via DataFusion.
//!
//! Built incrementally per `docs/workspace/parquet-backend/TASKS.md`.
//! Gated behind the `query-backend` feature; absent from default builds.

use std::net::TcpListener;

use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tracing::debug;

mod catalog;
pub mod loki;
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
    /// Skeleton: binds the listener and holds it until shutdown. The
    /// Prometheus/Tempo/Loki/SQL routers + DataFusion catalog are layered in by
    /// tasks 2–7 and 13. Mirrors [`crate::api::Server::start`].
    pub fn start(opts: &Options, handle: &Handle) -> crate::Result<Self> {
        let (_shutdown, rx) = oneshot::channel::<()>();
        let _guard = handle.enter();
        let addr = opts.address;

        let std_listener = TcpListener::bind(addr)?;
        std_listener.set_nonblocking(true)?;

        handle.spawn(async move {
            let _listener = tokio::net::TcpListener::from_std(std_listener)
                .expect("failed to create tokio TcpListener");
            debug!(message = "Sol query backend started (skeleton).", %addr);
            // TODO(tasks 2–7, 13): register the ParquetCatalog and mount the
            // PromQL / TraceQL / LogQL / SQL routers on `_listener`.
            rx.await.ok();
            debug!(message = "Sol query backend stopped.", %addr);
        });

        Ok(Server { _shutdown })
    }
}

/// Smoke reference ensuring the query-backend dependency tree links until the
/// real `QueryEngine`/`ParquetCatalog` (task 2) exercises these crates.
#[doc(hidden)]
pub fn _deps_smoke() {
    let _ctx = datafusion::prelude::SessionContext::new();
    let _path = object_store::path::Path::from("parquet");
    let _ = promql_parser::parser::parse("up");
}
