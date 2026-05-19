use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use futures_util::FutureExt;
use http::StatusCode;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use prost::Message;
use snafu::Snafu;
use sol_lib::{
    EstimatedJsonEncodedSizeOf,
    event::{BatchNotifier, BatchStatus},
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Registered,
    },
    opentelemetry::upstream_opentelemetry_proto::tonic::collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
};
use tower::ServiceBuilder;
use tracing::Span;
use warp::{
    Filter, Reply, filters::BoxedFilter, http::HeaderMap, reject::Rejection, reply::Response,
};

use super::{
    reply::{json, protobuf},
    status::Status,
};
use crate::{
    SourceSender,
    common::http::ErrorMessage,
    event::Event,
    http::{KeepaliveConfig, MaxConnectionAgeLayer, build_http_trace_layer},
    internal_events::{EventsReceived, HttpBadRequest, StreamClosedError},
    shutdown::ShutdownSignal,
    sources::{
        http_server::HttpConfigParamKind,
        opentelemetry::config::{LOGS, METRICS, OpentelemetryConfig, TRACES},
        util::{add_headers, decompress_body},
    },
    tls::MaybeTlsSettings,
};

#[derive(Clone, Copy, Debug, Snafu)]
pub(crate) enum ApiError {
    ServerShutdown,
}

impl warp::reject::Reject for ApiError {}

pub(crate) async fn run_http_server(
    address: SocketAddr,
    tls_settings: MaybeTlsSettings,
    filters: BoxedFilter<(Response,)>,
    shutdown: ShutdownSignal,
    keepalive_settings: KeepaliveConfig,
) -> crate::Result<()> {
    let mut listener = tls_settings.bind(&address).await?;
    let routes = filters.recover(handle_rejection);

    info!(message = "Building HTTP server.", address = %address);

    let span = Span::current();
    let shutdown = shutdown.map(|_| ());
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            result = listener.accept() => {
                let conn = match result {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("Error accepting connection: {:?}.", err);
                        continue;
                    }
                };
                let svc = ServiceBuilder::new()
                    .layer(build_http_trace_layer(span.clone()))
                    .option_layer(keepalive_settings.max_connection_age_secs.map(|secs| {
                        MaxConnectionAgeLayer::new(
                            Duration::from_secs(secs),
                            keepalive_settings.max_connection_age_jitter_factor,
                            conn.peer_addr(),
                        )
                    }))
                    .service(warp::service(routes.clone()));
                tokio::spawn(async move {
                    let io = TokioIo::new(conn);
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, TowerToHyperService::new(svc))
                        .with_upgrades()
                        .await
                        .ok();
                });
            }
            _ = &mut shutdown => break,
        }
    }

    Ok(())
}

pub(crate) fn build_warp_filter(
    acknowledgements: bool,
    out: SourceSender,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
    headers: Vec<HttpConfigParamKind>,
) -> BoxedFilter<(Response,)> {
    let log_filters = build_warp_log_filter(
        acknowledgements,
        out.clone(),
        bytes_received.clone(),
        events_received.clone(),
        headers.clone(),
    );
    let metrics_filters = build_warp_metrics_filter(
        acknowledgements,
        out.clone(),
        bytes_received.clone(),
        events_received.clone(),
    );
    let trace_filters = build_warp_trace_filter(
        acknowledgements,
        out.clone(),
        bytes_received,
        events_received,
    );
    log_filters
        .or(trace_filters)
        .unify()
        .or(metrics_filters)
        .unify()
        .boxed()
}

fn enrich_events(
    events: &mut [Event],
    headers_config: &[HttpConfigParamKind],
    headers: &HeaderMap,
) {
    add_headers(events, headers_config, headers, OpentelemetryConfig::NAME);
}

fn emit_decode_error(error: impl std::fmt::Display) -> ErrorMessage {
    let message = format!("Could not decode request: {error}");
    emit!(HttpBadRequest::new(
        StatusCode::BAD_REQUEST.as_u16(),
        &message
    ));
    ErrorMessage::new(StatusCode::BAD_REQUEST, message)
}

const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";
const CONTENT_TYPE_JSON: &str = "application/json";

fn build_ingest_filter<Resp, F>(
    telemetry_type: &'static str,
    acknowledgements: bool,
    out: SourceSender,
    make_events: F,
) -> BoxedFilter<(Response,)>
where
    Resp: prost::Message + Default + serde::Serialize + Send + 'static,
    F: Clone
        + Send
        + Sync
        + 'static
        + Fn(Option<String>, Option<String>, HeaderMap, Bytes) -> Result<Vec<Event>, ErrorMessage>,
{
    warp::post()
        .and(warp::path("v1"))
        .and(warp::path(telemetry_type))
        .and(warp::path::end())
        .and(warp::header::optional::<String>("content-type"))
        .and(warp::header::optional::<String>("content-encoding"))
        .and(warp::header::headers_cloned())
        .and(warp::body::bytes())
        .and_then(
            move |content_type: Option<String>,
                  encoding_header: Option<String>,
                  headers: HeaderMap,
                  body: Bytes| {
                let is_json = is_json_content_type(content_type.as_deref());
                let ct = content_type.as_deref().unwrap_or(CONTENT_TYPE_PROTOBUF);
                if !ct.starts_with(CONTENT_TYPE_PROTOBUF)
                    && !ct.starts_with(CONTENT_TYPE_JSON)
                {
                    let err = ErrorMessage::new(
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        format!(
                            "Unsupported content type: {ct}. Expected {CONTENT_TYPE_PROTOBUF} or {CONTENT_TYPE_JSON}"
                        ),
                    );
                    return handle_request(
                        Err(err),
                        acknowledgements,
                        out.clone(),
                        telemetry_type,
                        Resp::default(),
                        is_json,
                    );
                }
                let events = make_events(content_type, encoding_header, headers, body);
                handle_request(
                    events,
                    acknowledgements,
                    out.clone(),
                    telemetry_type,
                    Resp::default(),
                    is_json,
                )
            },
        )
        .boxed()
}

fn is_json_content_type(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|ct| ct.starts_with(CONTENT_TYPE_JSON))
}

fn build_warp_log_filter(
    acknowledgements: bool,
    source_sender: SourceSender,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
    headers_cfg: Vec<HttpConfigParamKind>,
) -> BoxedFilter<(Response,)> {
    let make_events = move |content_type: Option<String>,
                            encoding_header: Option<String>,
                            headers: HeaderMap,
                            body: Bytes| {
        decompress_body(encoding_header.as_deref(), body)
            .inspect_err(|err| {
                if err.status_code() == StatusCode::UNSUPPORTED_MEDIA_TYPE {
                    emit!(HttpBadRequest::new(
                        err.status_code().as_u16(),
                        err.message()
                    ));
                }
            })
            .and_then(|decoded_body| {
                bytes_received.emit(ByteSize(decoded_body.len()));
                decode_log_body(
                    decoded_body,
                    &events_received,
                    is_json_content_type(content_type.as_deref()),
                )
                .map(|mut events| {
                    enrich_events(&mut events, &headers_cfg, &headers);
                    events
                })
            })
    };

    build_ingest_filter::<otel::collector::logs::v1::ExportLogsServiceResponse, _>(
        LOGS,
        acknowledgements,
        source_sender,
        make_events,
    )
}

fn build_warp_metrics_filter(
    acknowledgements: bool,
    source_sender: SourceSender,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
) -> BoxedFilter<(Response,)> {
    let make_events = move |content_type: Option<String>,
                            encoding_header: Option<String>,
                            _headers: HeaderMap,
                            body: Bytes| {
        decompress_body(encoding_header.as_deref(), body)
            .inspect_err(|err| {
                if err.status_code() == StatusCode::UNSUPPORTED_MEDIA_TYPE {
                    emit!(HttpBadRequest::new(
                        err.status_code().as_u16(),
                        err.message()
                    ));
                }
            })
            .and_then(|decoded_body| {
                bytes_received.emit(ByteSize(decoded_body.len()));
                decode_metrics_body(
                    decoded_body,
                    &events_received,
                    is_json_content_type(content_type.as_deref()),
                )
            })
    };

    build_ingest_filter::<otel::collector::metrics::v1::ExportMetricsServiceResponse, _>(
        METRICS,
        acknowledgements,
        source_sender,
        make_events,
    )
}

fn build_warp_trace_filter(
    acknowledgements: bool,
    source_sender: SourceSender,
    bytes_received: Registered<BytesReceived>,
    events_received: Registered<EventsReceived>,
) -> BoxedFilter<(Response,)> {
    let make_events = move |content_type: Option<String>,
                            encoding_header: Option<String>,
                            _headers: HeaderMap,
                            body: Bytes| {
        decompress_body(encoding_header.as_deref(), body)
            .inspect_err(|err| {
                if err.status_code() == StatusCode::UNSUPPORTED_MEDIA_TYPE {
                    emit!(HttpBadRequest::new(
                        err.status_code().as_u16(),
                        err.message()
                    ));
                }
            })
            .and_then(|decoded_body| {
                bytes_received.emit(ByteSize(decoded_body.len()));
                decode_trace_body(
                    decoded_body,
                    &events_received,
                    is_json_content_type(content_type.as_deref()),
                )
            })
    };

    build_ingest_filter::<otel::collector::trace::v1::ExportTraceServiceResponse, _>(
        TRACES,
        acknowledgements,
        source_sender,
        make_events,
    )
}

use opentelemetry_proto::tonic as otel;

#[cfg(feature = "sources-opentelemetry")]
use sol_lib::event::{EventMetadata, OtelLog, OtelMetric, OtelSpan};

#[cfg(feature = "sources-opentelemetry")]
fn json_decode_logs(body: Bytes) -> Result<Vec<Event>, ErrorMessage> {
    let request: otel::collector::logs::v1::ExportLogsServiceRequest =
        serde_json::from_slice(&body).map_err(emit_decode_error)?;

    Ok(request
        .resource_logs
        .into_iter()
        .flat_map(|rl| {
            let resource = rl.resource;
            rl.scope_logs.into_iter().flat_map(move |sl| {
                let scope = sl.scope.clone();
                let resource = resource.clone();
                sl.log_records.into_iter().map(move |record| {
                    Event::Log(OtelLog::from_parts(
                        record,
                        resource.clone(),
                        scope.clone(),
                        EventMetadata::default(),
                    ))
                })
            })
        })
        .collect())
}

#[cfg(feature = "sources-opentelemetry")]
fn json_decode_metrics(body: Bytes) -> Result<Vec<Event>, ErrorMessage> {
    let request: otel::collector::metrics::v1::ExportMetricsServiceRequest =
        serde_json::from_slice(&body).map_err(emit_decode_error)?;

    Ok(request
        .resource_metrics
        .into_iter()
        .flat_map(|rm| {
            let resource = rm.resource;
            rm.scope_metrics.into_iter().flat_map(move |sm| {
                let scope = sm.scope.clone();
                let resource = resource.clone();
                sm.metrics.into_iter().map(move |metric| {
                    Event::Metric(OtelMetric::from_parts(
                        metric,
                        resource.clone(),
                        scope.clone(),
                        EventMetadata::default(),
                    ))
                })
            })
        })
        .collect())
}

#[cfg(feature = "sources-opentelemetry")]
fn json_decode_traces(body: Bytes) -> Result<Vec<Event>, ErrorMessage> {
    let request: otel::collector::trace::v1::ExportTraceServiceRequest =
        serde_json::from_slice(&body).map_err(emit_decode_error)?;

    Ok(request
        .resource_spans
        .into_iter()
        .flat_map(|rs| {
            let resource = rs.resource;
            rs.scope_spans.into_iter().flat_map(move |ss| {
                let scope = ss.scope.clone();
                let resource = resource.clone();
                ss.spans.into_iter().map(move |span| {
                    Event::Trace(OtelSpan::from_parts(
                        span,
                        resource.clone(),
                        scope.clone(),
                        EventMetadata::default(),
                    ))
                })
            })
        })
        .collect())
}

fn decode_trace_body(
    body: Bytes,
    events_received: &Registered<EventsReceived>,
    is_json: bool,
) -> Result<Vec<Event>, ErrorMessage> {
    let events: Vec<Event> = if is_json {
        json_decode_traces(body)?
    } else {
        let request = ExportTraceServiceRequest::decode(body).map_err(emit_decode_error)?;
        request
            .resource_spans
            .into_iter()
            .flat_map(sol_lib::opentelemetry::spans::resource_spans_into_events)
            .collect()
    };

    events_received.emit(CountByteSize(
        events.len(),
        events.estimated_json_encoded_size_of(),
    ));

    Ok(events)
}

fn decode_log_body(
    body: Bytes,
    events_received: &Registered<EventsReceived>,
    is_json: bool,
) -> Result<Vec<Event>, ErrorMessage> {
    let events: Vec<Event> = if is_json {
        json_decode_logs(body)?
    } else {
        let request = ExportLogsServiceRequest::decode(body).map_err(emit_decode_error)?;
        request
            .resource_logs
            .into_iter()
            .flat_map(sol_lib::opentelemetry::logs::resource_logs_into_events)
            .collect()
    };

    events_received.emit(CountByteSize(
        events.len(),
        events.estimated_json_encoded_size_of(),
    ));

    Ok(events)
}

fn decode_metrics_body(
    body: Bytes,
    events_received: &Registered<EventsReceived>,
    is_json: bool,
) -> Result<Vec<Event>, ErrorMessage> {
    let events: Vec<Event> = if is_json {
        json_decode_metrics(body)?
    } else {
        let request = ExportMetricsServiceRequest::decode(body).map_err(emit_decode_error)?;
        request
            .resource_metrics
            .into_iter()
            .flat_map(sol_lib::opentelemetry::metrics::resource_metrics_into_events)
            .collect()
    };

    events_received.emit(CountByteSize(
        events.len(),
        events.estimated_json_encoded_size_of(),
    ));

    Ok(events)
}

fn reply_for<T: Message + serde::Serialize>(resp: T, is_json: bool) -> Response {
    if is_json {
        json(resp).into_response()
    } else {
        protobuf(resp).into_response()
    }
}

async fn handle_request(
    events: Result<Vec<Event>, ErrorMessage>,
    acknowledgements: bool,
    mut out: SourceSender,
    output: &str,
    resp: impl Message + serde::Serialize,
    is_json: bool,
) -> Result<Response, Rejection> {
    match events {
        Ok(mut events) => {
            let receiver = BatchNotifier::maybe_apply_to(acknowledgements, &mut events);
            let count = events.len();

            out.send_batch_named(output, events).await.map_err(|_| {
                emit!(StreamClosedError { count });
                warp::reject::custom(ApiError::ServerShutdown)
            })?;

            match receiver {
                None => Ok(reply_for(resp, is_json)),
                Some(receiver) => match receiver.await {
                    BatchStatus::Delivered => Ok(reply_for(resp, is_json)),
                    BatchStatus::Errored => Err(warp::reject::custom(Status {
                        code: 2,
                        message: "Error delivering contents to sink".into(),
                        ..Default::default()
                    })),
                    BatchStatus::Rejected => Err(warp::reject::custom(Status {
                        code: 2,
                        message: "Contents failed to deliver to sink".into(),
                        ..Default::default()
                    })),
                },
            }
        }
        Err(err) => Err(warp::reject::custom(err)),
    }
}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, std::convert::Infallible> {
    if let Some(err_msg) = err.find::<ErrorMessage>() {
        let reply = protobuf(Status {
            code: 2,
            message: err_msg.message().into(),
            ..Default::default()
        });

        Ok(warp::reply::with_status(reply, err_msg.status_code()))
    } else {
        let reply = protobuf(Status {
            code: 2,
            message: format!("{err:?}"),
            ..Default::default()
        });

        Ok(warp::reply::with_status(
            reply,
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}
