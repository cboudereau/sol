use std::{
    cmp,
    future::Future,
    io::Write,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::write::GzDecoder;
use futures_util::FutureExt;
use http_body_util::{BodyExt, StreamBody};
use sol_lib::internal_event::{
    ByteSize, BytesReceived, InternalEventHandle as _, Protocol, Registered,
};
use tokio::{pin, select};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, body::BoxBody, metadata::AsciiMetadataValue};
use tower::{Layer, Service};

use crate::internal_events::{GrpcError, GrpcInvalidCompressionSchemeError};

const GRPC_MESSAGE_HEADER_LEN: usize = mem::size_of::<u8>() + mem::size_of::<u32>();
const GRPC_ENCODING_HEADER: &str = "grpc-encoding";
const GRPC_ACCEPT_ENCODING_HEADER: &str = "grpc-accept-encoding";

enum CompressionScheme {
    Gzip,
}

impl CompressionScheme {
    fn from_encoding_header(req: &http_1::Request<BoxBody>) -> Result<Option<Self>, Status> {
        req.headers()
            .get(GRPC_ENCODING_HEADER)
            .map(|s| {
                s.to_str().map(|s| s.to_string()).map_err(|_| {
                    Status::unimplemented(format!(
                        "`{GRPC_ENCODING_HEADER}` contains non-visible characters and is not a valid encoding"
                    ))
                })
            })
            .transpose()
            .and_then(|value| match value {
                None => Ok(None),
                Some(scheme) => match scheme.as_str() {
                    "gzip" => Ok(Some(CompressionScheme::Gzip)),
                    other => Err(Status::unimplemented(format!(
                        "compression scheme `{other}` is not supported"
                    ))),
                },
            })
            .map_err(|mut status| {
                status.metadata_mut().insert(
                    GRPC_ACCEPT_ENCODING_HEADER,
                    AsciiMetadataValue::from_static("gzip,identity"),
                );
                status
            })
    }
}

#[derive(Default)]
enum State {
    #[default]
    WaitingForHeader,
    Forward {
        overall_len: usize,
    },
    Decompress {
        remaining: usize,
    },
}

fn new_decompressor() -> GzDecoder<Vec<u8>> {
    let buf = vec![0; GRPC_MESSAGE_HEADER_LEN];
    GzDecoder::new(buf)
}

type FrameResult = Result<http_body_1::Frame<Bytes>, Status>;
type FrameSender = tokio::sync::mpsc::Sender<FrameResult>;

async fn drive_body_decompression(
    mut source: BoxBody,
    destination: FrameSender,
) -> Result<usize, Status> {
    let mut state = State::default();
    let mut buf = BytesMut::new();
    let mut decompressor = None;
    let mut bytes_received = 0;

    while let Some(result) = source.frame().await {
        let frame = result.map_err(|e| Status::internal(format!("failed to read frame: {e}")))?;

        if let Ok(data) = frame.into_data() {
            buf.put(data);

            let maybe_message = loop {
                match state {
                    State::WaitingForHeader => {
                        if buf.len() < GRPC_MESSAGE_HEADER_LEN {
                            break None;
                        }

                        let (is_compressed, message_len) = {
                            let header = &buf[..GRPC_MESSAGE_HEADER_LEN];
                            let message_len_raw: u32 = header[1..]
                                .try_into()
                                .map(u32::from_be_bytes)
                                .expect("there must be four bytes remaining in the header slice");
                            let message_len = message_len_raw
                                .try_into()
                                .expect("Vector does not support 16-bit platforms");
                            (header[0] == 1, message_len)
                        };

                        if is_compressed {
                            buf.advance(GRPC_MESSAGE_HEADER_LEN);
                            state = State::Decompress {
                                remaining: message_len,
                            };
                        } else {
                            let overall_len = GRPC_MESSAGE_HEADER_LEN + message_len;
                            state = State::Forward { overall_len };
                        }
                    }
                    State::Forward { overall_len } => {
                        if buf.len() < overall_len {
                            break None;
                        }

                        let message = buf.split_to(overall_len).freeze();
                        state = State::WaitingForHeader;
                        bytes_received += overall_len;
                        break Some(message);
                    }
                    State::Decompress { ref mut remaining } => {
                        if *remaining > 0 {
                            let available = buf.len();
                            if available > 0 {
                                let to_take = cmp::min(available, *remaining);
                                let decompressor =
                                    decompressor.get_or_insert_with(new_decompressor);
                                if decompressor.write_all(&buf[..to_take]).is_err() {
                                    return Err(Status::internal(
                                        "failed to write to decompressor",
                                    ));
                                }
                                *remaining -= to_take;
                                buf.advance(to_take);
                            } else {
                                break None;
                            }
                        } else {
                            let result = decompressor
                                .take()
                                .expect("consumed decompressor when no decompressor was present")
                                .finish();

                            let mut buf = result.map_err(|_| {
                                Status::internal(
                                    "reached impossible error during decompressor finalization",
                                )
                            })?;
                            bytes_received += buf.len();

                            let message_len_actual = buf.len() - GRPC_MESSAGE_HEADER_LEN;
                            let message_len = u32::try_from(message_len_actual).map_err(|_| {
                                Status::out_of_range("messages greater than 4GB are not supported")
                            })?;

                            let message_len_bytes = message_len.to_be_bytes();
                            let message_len_slot = &mut buf[1..GRPC_MESSAGE_HEADER_LEN];
                            message_len_slot.copy_from_slice(&message_len_bytes[..]);

                            state = State::WaitingForHeader;
                            break Some(Bytes::from(buf));
                        }
                    }
                }
            };

            if let Some(message) = maybe_message
                && destination
                    .send(Ok(http_body_1::Frame::data(message)))
                    .await
                    .is_err()
            {
                return Err(Status::internal("destination body abnormally closed"));
            }
        }
    }

    Ok(bytes_received)
}

async fn drive_request<F, E>(
    source: BoxBody,
    destination: FrameSender,
    inner: F,
    bytes_received: Registered<BytesReceived>,
) -> Result<http_1::Response<BoxBody>, E>
where
    F: Future<Output = Result<http_1::Response<BoxBody>, E>>,
    E: std::fmt::Display,
{
    let body_decompression = drive_body_decompression(source, destination);

    pin!(inner);
    pin!(body_decompression);

    let mut body_eof = false;
    let mut body_bytes_received = 0;

    let result = loop {
        select! {
            biased;

            result = &mut inner => break result,

            result = &mut body_decompression, if !body_eof => match result {
                Err(e) => break Ok(e.into_http()),
                Ok(bytes_received) => {
                    body_bytes_received = bytes_received;
                    body_eof = true;
                },
            }
        }
    };

    match &result {
        Ok(res) if res.status().is_success() => {
            bytes_received.emit(ByteSize(body_bytes_received));
        }
        Ok(res) => {
            emit!(GrpcError {
                error: format!("Received {}", res.status())
            });
        }
        Err(error) => {
            emit!(GrpcError { error: &error });
        }
    };

    result
}

#[derive(Clone)]
pub struct DecompressionAndMetrics<S> {
    inner: S,
    bytes_received: Registered<BytesReceived>,
}

impl<S> Service<http_1::Request<BoxBody>> for DecompressionAndMetrics<S>
where
    S: Service<http_1::Request<BoxBody>, Response = http_1::Response<BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display,
{
    type Response = http_1::Response<BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http_1::Request<BoxBody>) -> Self::Future {
        match CompressionScheme::from_encoding_header(&req) {
            Err(status) => {
                emit!(GrpcInvalidCompressionSchemeError { status: &status });
                Box::pin(async move { Ok(status.into_http()) })
            }

            Ok(_) => {
                let (tx, rx) = tokio::sync::mpsc::channel::<FrameResult>(32);
                let stream = ReceiverStream::new(rx);
                let decompressed_body = BoxBody::new(StreamBody::new(stream));

                let (req_parts, req_body) = req.into_parts();
                let mapped_req = http_1::Request::from_parts(req_parts, decompressed_body);

                let inner = self.inner.call(mapped_req);

                drive_request(req_body, tx, inner, self.bytes_received.clone()).boxed()
            }
        }
    }
}

/// A layer for decompressing Tonic request payloads and emitting telemetry for the payload sizes.
///
/// In some cases, we configure `tonic` to use compression on requests to save CPU and throughput when sending those
/// large requests. In the case of Vector-to-Vector communication, this means the Vector v2 source may deal with
/// compressed requests. The code already transparently handles decompression, but as part of our component
/// specification, we have specific goals around what event representations we pay attention to.
///
/// In the case of tracking bytes sent/received, we always want to track the number of bytes received _after_
/// decompression to faithfully represent the amount of data being processed by Vector. This poses a problem with the
/// out-of-the-box `tonic` codegen as there is no hook whatsoever to inspect the raw request payload (after
/// decompression, if it was compressed at all) prior to the payload being decoded as a Protocol Buffers payload.
///
/// This layer wraps the incoming body in our own body type, which allows us to do two things: decompress the payload
/// before it enters the decoding phase, and emit metrics based on the decompressed payload.
///
/// Since we can see the decompressed bytes, and also know if the underlying service responded successfully -- i.e. the
/// request was valid, and was processed -- we can now report the number of bytes (after decompression) that were
/// received _and_ processed correctly.
///
/// The only supported compression scheme is gzip, which is also the only supported compression scheme in `tonic` itself.
#[derive(Clone, Default)]
pub struct DecompressionAndMetricsLayer;

impl<S> Layer<S> for DecompressionAndMetricsLayer {
    type Service = DecompressionAndMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        DecompressionAndMetrics {
            inner,
            bytes_received: register!(BytesReceived::from(Protocol::from("grpc"))),
        }
    }
}
