use std::{
    io::{BufRead, BufReader},
    net::SocketAddr,
};

use bytes::{Buf, Bytes};
use flate2::read::{MultiGzDecoder, ZlibDecoder};
use futures::{FutureExt, SinkExt, channel::mpsc, stream};
use futures_util::StreamExt;
use http::request::Parts;
use http_body::Body as HttpBody;
use http_body_util::Full;
use hyper::{Request, Response, StatusCode, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use stream_cancel::{Trigger, Tripwire};

use crate::config::{SinkConfig, SinkContext};

pub fn load_sink<T>(config: &str) -> crate::Result<(T, SinkContext)>
where
    for<'a> T: Deserialize<'a> + SinkConfig,
{
    let sink_config: T = toml::from_str(config)?;
    let cx = SinkContext::default();

    Ok((sink_config, cx))
}

pub fn load_sink_with_context<T>(config: &str, cx: SinkContext) -> crate::Result<(T, SinkContext)>
where
    for<'a> T: Deserialize<'a> + SinkConfig,
{
    let sink_config: T = toml::from_str(config)?;

    Ok((sink_config, cx))
}

pub fn build_test_server(
    addr: SocketAddr,
) -> (
    mpsc::Receiver<(http::request::Parts, Bytes)>,
    Trigger,
    impl std::future::Future<Output = Result<(), ()>>,
) {
    build_test_server_generic(addr, || Response::new(Full::new(Bytes::new())))
}

pub fn build_test_server_status(
    addr: SocketAddr,
    status: StatusCode,
) -> (
    mpsc::Receiver<(http::request::Parts, Bytes)>,
    Trigger,
    impl std::future::Future<Output = Result<(), ()>>,
) {
    build_test_server_generic(addr, move || {
        Response::builder()
            .status(status)
            .body(Full::new(Bytes::new()))
            .unwrap_or_else(|_| unreachable!())
    })
}

pub fn build_test_server_generic<B>(
    addr: SocketAddr,
    responder: impl Fn() -> Response<B> + Clone + Send + Sync + 'static,
) -> (
    mpsc::Receiver<(http::request::Parts, Bytes)>,
    Trigger,
    impl std::future::Future<Output = Result<(), ()>>,
)
where
    B: HttpBody + Send + 'static,
    <B as HttpBody>::Data: Send + Sync,
    <B as HttpBody>::Error: snafu::Error + Send + Sync,
{
    let (tx, rx) = mpsc::channel(100);
    let (trigger, tripwire) = Tripwire::new();

    let std_listener =
        std::net::TcpListener::bind(addr).expect("Failed to bind test server");
    std_listener
        .set_nonblocking(true)
        .expect("Failed to set non-blocking");

    let server = async move {
        let listener = tokio::net::TcpListener::from_std(std_listener)
            .expect("Failed to create tokio TcpListener");
        let mut tripwire = tripwire.fuse();
        loop {
            let result = tokio::select! {
                result = listener.accept() => result,
                _ = &mut tripwire => break,
            };
            let (stream, _) = result.expect("Failed to accept connection");
            let responder = responder.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                    let responder = responder.clone();
                    let mut tx = tx.clone();
                    async move {
                        let (parts, body) = req.into_parts();
                        let response = responder();
                        if response.status().is_success() {
                            tokio::spawn(async move {
                                let bytes = http_body_util::BodyExt::collect(body)
                                    .await
                                    .unwrap()
                                    .to_bytes();
                                tx.send((parts, bytes)).await.unwrap();
                            });
                        }

                        Ok::<_, std::convert::Infallible>(response)
                    }
                });
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                    .ok();
            });
        }
        Ok(())
    };

    (rx, trigger, server)
}

pub async fn get_received_gzip(
    rx: mpsc::Receiver<(Parts, Bytes)>,
    assert_parts: impl Fn(Parts),
) -> Vec<String> {
    get_received(rx, assert_parts, |body| MultiGzDecoder::new(body.reader())).await
}

pub async fn get_received_zlib(
    rx: mpsc::Receiver<(Parts, Bytes)>,
    assert_parts: impl Fn(Parts),
) -> Vec<String> {
    get_received(rx, assert_parts, |body| ZlibDecoder::new(body.reader())).await
}

async fn get_received<D>(
    rx: mpsc::Receiver<(Parts, Bytes)>,
    assert_parts: impl Fn(Parts),
    decoder_maker: impl Fn(Bytes) -> D,
) -> Vec<String>
where
    D: std::io::Read,
{
    rx.flat_map(|(parts, body)| {
        assert_parts(parts);
        let decoder = decoder_maker(body);
        let reader = BufReader::new(decoder);
        stream::iter(reader.lines())
    })
    .map(Result::unwrap)
    .map(|line| {
        let val: serde_json::Value = serde_json::from_str(&line).unwrap();
        let body = val.get("body").unwrap();
        // OTLP/JSON: body is {"stringValue": "..."} or a plain string (legacy)
        if let Some(sv) = body.get("stringValue") {
            sv.as_str().unwrap().to_owned()
        } else {
            body.as_str().unwrap().to_owned()
        }
    })
    .collect::<Vec<_>>()
    .await
}
