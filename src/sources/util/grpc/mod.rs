use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::FutureExt;
use tonic::{body::Body as TonicBody, server::NamedService, service::Routes, transport::server::Server};
use tower::{Layer, Service};
use tracing::Span;

use crate::{
    internal_events::{GrpcServerRequestReceived, GrpcServerResponseSent},
    shutdown::{ShutdownSignal, ShutdownSignalToken},
    tls::MaybeTlsSettings,
};

/// Macro to create a tonic 0.13-compatible adapter for a tonic 0.12 gRPC service.
///
/// This exists because opentelemetry-proto 0.27 depends on tonic 0.12 while
/// the rest of the crate uses tonic 0.13. The two versions have incompatible
/// body and trait types, so we bridge them by wrapping each tonic 0.12 service
/// in a newtype that implements tonic 0.13's `NamedService` and `Service` traits.
///
/// Usage:
/// ```ignore
/// tonic_0_12_adapter!(LogsAdapter, "opentelemetry.proto.collector.logs.v1.LogsService");
/// let adapted = LogsAdapter(log_service);
/// builder.add_service(adapted);
/// ```
#[cfg(any(feature = "sources-opentelemetry", feature = "component-validation-runner"))]
macro_rules! tonic_0_12_adapter {
    ($name:ident, $service_name:literal) => {
        #[derive(Clone)]
        pub(crate) struct $name<S>(pub S);

        impl<S> tonic::server::NamedService for $name<S> {
            const NAME: &'static str = $service_name;
        }

        impl<S> tower::Service<http::Request<tonic::body::Body>> for $name<S>
        where
            S: tower::Service<
                    http::Request<tonic_0_12::body::BoxBody>,
                    Response = http::Response<tonic_0_12::body::BoxBody>,
                    Error = std::convert::Infallible,
                > + Clone
                + Send
                + 'static,
            S::Future: Send + 'static,
        {
            type Response = http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
            >;

            fn poll_ready(
                &mut self,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                self.0.poll_ready(cx)
            }

            fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
                use http_body_util::BodyExt;
                // Convert request body: tonic 0.13 Body -> tonic 0.12 BoxBody
                let req = req.map(|body| {
                    body.map_err(|e| -> tonic_0_12::Status {
                        tonic_0_12::Status::internal(e.to_string())
                    })
                    .boxed_unsync()
                });
                let fut = self.0.call(req);
                Box::pin(async move {
                    let resp = fut.await?;
                    // Convert response body: tonic 0.12 BoxBody -> tonic 0.13 Body
                    Ok(resp.map(tonic::body::Body::new))
                })
            }
        }
    };
}

#[cfg(any(feature = "sources-opentelemetry", feature = "component-validation-runner"))]
pub(crate) use tonic_0_12_adapter;

fn grpc_server_builder() -> Server {
    Server::builder()
        .http2_adaptive_window(Some(true))
        .initial_stream_window_size(1024 * 1024)
        .initial_connection_window_size(2 * 1024 * 1024)
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(20)))
        .max_concurrent_streams(1024)
}

pub async fn run_grpc_server<S>(
    address: SocketAddr,
    tls_settings: MaybeTlsSettings,
    service: S,
    shutdown: ShutdownSignal,
) -> crate::Result<()>
where
    S: Service<http::Request<TonicBody>, Response = http::Response<TonicBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let span = Span::current();
    let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();
    let listener = tls_settings.bind(&address).await?;
    let stream = listener.accept_stream();

    info!(%address, "Building gRPC server.");

    grpc_server_builder()
        .layer(GrpcTraceLayer::new(span.clone()))
        .add_service(service)
        .serve_with_incoming_shutdown(stream, shutdown.map(|token| tx.send(token).unwrap()))
        .await?;

    drop(rx.await);

    Ok(())
}

pub async fn run_grpc_server_with_routes(
    address: SocketAddr,
    tls_settings: MaybeTlsSettings,
    routes: Routes,
    shutdown: ShutdownSignal,
) -> crate::Result<()> {
    let span = Span::current();
    let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();
    let listener = tls_settings.bind(&address).await?;
    let stream = listener.accept_stream();

    info!(%address, "Building gRPC server.");

    grpc_server_builder()
        .layer(GrpcTraceLayer::new(span.clone()))
        .add_routes(routes)
        .serve_with_incoming_shutdown(stream, shutdown.map(|token| tx.send(token).unwrap()))
        .await?;

    drop(rx.await);

    Ok(())
}

#[derive(Clone)]
struct GrpcTraceLayer {
    span: Span,
}

impl GrpcTraceLayer {
    const fn new(span: Span) -> Self {
        Self { span }
    }
}

impl<S> Layer<S> for GrpcTraceLayer {
    type Service = GrpcTraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcTraceService {
            inner,
            span: self.span.clone(),
        }
    }
}

#[derive(Clone)]
struct GrpcTraceService<S> {
    inner: S,
    span: Span,
}

impl<S> Service<http::Request<TonicBody>> for GrpcTraceService<S>
where
    S: Service<http::Request<TonicBody>, Response = http::Response<TonicBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let path = req.uri().path();
        let mut parts = path.split('/');
        let service = parts.nth(1).unwrap_or("_unknown");
        let method = parts.next().unwrap_or("_unknown");

        let request_span = error_span!(
            parent: &self.span,
            "grpc-request",
            grpc_service = %service,
            grpc_method = %method,
        );

        emit!(GrpcServerRequestReceived);

        let start = std::time::Instant::now();
        let fut = self.inner.call(req);

        let future = async move {
            let result = fut.await;
            let latency = start.elapsed();
            if let Ok(ref response) = result {
                emit!(GrpcServerResponseSent { response, latency });
            }
            result
        };

        Box::pin(tracing::Instrument::instrument(future, request_span))
    }
}
