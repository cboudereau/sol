use std::{
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::FutureExt;
use pin_project::pin_project;
use tonic::{
    body::Body as TonicBody, server::NamedService, service::Routes, transport::server::Server,
};
use tower::{Layer, Service};
use tracing::{Instrument, Span};

use crate::{
    internal_events::{GrpcServerRequestReceived, GrpcServerResponseSent},
    shutdown::{ShutdownSignal, ShutdownSignalToken},
    tls::MaybeTlsSettings,
};

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
    type Future = tracing::instrument::Instrumented<GrpcTraceResponseFuture<S::Future>>;

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

        let fut = self.inner.call(req);

        GrpcTraceResponseFuture {
            inner: fut,
            start: Instant::now(),
        }
        .instrument(request_span)
    }
}

#[pin_project]
struct GrpcTraceResponseFuture<F> {
    #[pin]
    inner: F,
    start: Instant,
}

impl<F, E> std::future::Future for GrpcTraceResponseFuture<F>
where
    F: std::future::Future<Output = Result<http::Response<TonicBody>, E>>,
{
    type Output = Result<http::Response<TonicBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(result) => {
                let latency = this.start.elapsed();
                if let Ok(ref response) = result {
                    emit!(GrpcServerResponseSent { response, latency });
                }
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
