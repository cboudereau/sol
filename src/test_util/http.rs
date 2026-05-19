use std::{convert::Infallible, future::Future};

use bytes::Bytes;
use http::{Request, Response, Uri, uri::Scheme};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use super::{addr::next_addr, wait_for_tcp};

/// Spawns an HTTP server that uses the given `handler` to respond to requests.
///
/// A random local address is chosen for the HTTP server to listen on, and the function does not return until the server
/// is up and ready for requests. The returned `Uri` is configured for the appropriate address.
pub async fn spawn_blackhole_http_server<H, F>(handler: H) -> Uri
where
    H: Fn(Request<hyper::body::Incoming>) -> F + Clone + Send + 'static,
    F: Future<Output = std::result::Result<Response<Full<Bytes>>, Infallible>> + Send + 'static,
{
    let (_guard, address) = next_addr();

    let uri = Uri::builder()
        .scheme(Scheme::HTTP)
        .authority(address.to_string())
        .path_and_query("/")
        .build()
        .expect("URI should always be valid when starting from `SocketAddr`");

    let listener = TcpListener::bind(&address)
        .await
        .expect("Failed to bind TCP listener for blackhole HTTP server");

    tokio::spawn(async move {
        loop {
            let (stream, _peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(error) => {
                    error!(message = "Blackhole HTTP server accept error.", ?error);
                    continue;
                }
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                if let Err(error) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service_fn(handler))
                    .await
                {
                    error!(message = "Blackhole HTTP server connection error.", ?error);
                }
            });
        }
    });

    wait_for_tcp(address).await;

    uri
}

/// Responds to every request with a 200 OK response.
pub async fn always_200_response(
    _: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::new(Full::new(Bytes::new())))
}
