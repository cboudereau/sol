use std::{
    convert::Infallible,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
use tonic::{body::BoxBody, server::NamedService, service::Routes, transport::server::Server};
use tower::{Layer, Service};
use tracing::Span;

use crate::{
    internal_events::{GrpcServerRequestReceived, GrpcServerResponseSent},
    shutdown::{ShutdownSignal, ShutdownSignalToken},
    tls::MaybeTlsSettings,
};


pub async fn run_grpc_server<S>(
    address: SocketAddr,
    tls_settings: MaybeTlsSettings,
    service: S,
    shutdown: ShutdownSignal,
) -> crate::Result<()>
where
    S: Service<http_1::Request<BoxBody>, Response = http_1::Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    let span = Span::current();
    let (tx, rx) = tokio::sync::oneshot::channel::<ShutdownSignalToken>();
    let listener = tls_settings.bind(&address).await?;
    let stream = listener.accept_stream();

    info!(%address, "Building gRPC server.");

    Server::builder()
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

    Server::builder()
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

impl<S> Service<http_1::Request<BoxBody>> for GrpcTraceService<S>
where
    S: Service<http_1::Request<BoxBody>, Response = http_1::Response<BoxBody>>
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

    fn call(&mut self, req: http_1::Request<BoxBody>) -> Self::Future {
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
