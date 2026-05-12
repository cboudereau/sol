use std::{
    num::NonZeroUsize,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, future::BoxFuture, stream::BoxStream};
use http::{Method, Request, header};
use hyper::Body;
use sol_lib::{
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
    config::telemetry,
    configurable::configurable_component,
    request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata},
    stream::{BatcherSettings, DriverResponse, batcher::data::BatchReduce},
};
use tower::{Service, ServiceBuilder};

use snafu::Snafu;

use crate::{
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkContext},
    event::{Event, EventFinalizers, EventStatus, Finalizable},
    http::HttpClient,
    internal_events::EndpointBytesSent,
    sinks::{
        Healthcheck, VectorSink,
        util::{
            BatchConfig, RealtimeEventBasedDefaultBatchSettings, ServiceBuilderExt, SinkBuilderExt,
            StreamSink, TowerRequestConfig,
            metadata::RequestMetadataBuilder,
            retries::{RetryAction, RetryLogic},
        },
    },
    tls::TlsSettings,
};

use super::grpc::{
    otel_log_event_to_resource_logs, otel_metric_event_to_resource_metrics,
    otel_span_event_to_resource_spans,
};

#[derive(Debug, Snafu)]
pub enum OtlpHttpError {
    #[snafu(display("HTTP request failed: {message}"))]
    HttpRequest { message: String },
}

/// The encoding format for the OTLP/HTTP transport.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OtlpHttpEncoding {
    /// Protobuf binary encoding (`application/x-protobuf`).
    /// Most compatible and efficient. Default.
    #[default]
    Protobuf,
    /// JSON encoding (`application/json`).
    /// Useful for debugging and backends that prefer JSON.
    Json,
}

/// Configuration for the `opentelemetry` sink's OTLP/HTTP transport.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct OtlpHttpConfig {
    /// The base OTLP HTTP endpoint.
    ///
    /// Signal-specific paths (`/v1/logs`, `/v1/metrics`, `/v1/traces`) are
    /// appended automatically.
    #[configurable(metadata(docs::examples = "http://localhost:4318"))]
    #[serde(default = "default_endpoint")]
    pub endpoint: String,

    /// The encoding format for OTLP data.
    #[configurable(derived)]
    #[serde(default)]
    pub encoding: OtlpHttpEncoding,

    #[configurable(derived)]
    #[serde(default)]
    pub batch: BatchConfig<RealtimeEventBasedDefaultBatchSettings>,

    #[configurable(derived)]
    #[serde(default)]
    pub request: TowerRequestConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub tls: Option<sol_lib::tls::TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for OtlpHttpConfig {
    fn default() -> Self {
        Self {
            endpoint: default_endpoint(),
            encoding: Default::default(),
            batch: Default::default(),
            request: Default::default(),
            tls: Default::default(),
            acknowledgements: Default::default(),
        }
    }
}

fn default_endpoint() -> String {
    "http://localhost:4318".to_string()
}

impl GenerateConfig for OtlpHttpConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(r#"endpoint = "http://localhost:4318""#).unwrap()
    }
}

impl OtlpHttpConfig {
    pub async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let tls = TlsSettings::from_options(self.tls.as_ref())?;
        let client = HttpClient::new(tls, cx.proxy())?;

        let service = OtlpHttpService {
            client: client.clone(),
            endpoint: self.endpoint.trim_end_matches('/').to_string(),
            encoding: self.encoding,
        };

        let batch = self
            .batch
            .into_batcher_settings()
            .map_err(|e| format!("invalid batch settings: {e}"))?;

        let tower_svc = ServiceBuilder::new()
            .settings(self.request.into_settings(), OtlpHttpRetryLogic)
            .service(service);

        let encoding = self.encoding;
        let sink = OtlpHttpSink {
            batch_settings: batch,
            service: tower_svc,
            encoding,
        };

        let healthcheck: Healthcheck = Box::pin(async { Ok(()) });
        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    pub fn input(&self) -> Input {
        Input::all()
    }

    pub const fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

// ---------------------------------------------------------------------------
// Request / Response
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct OtlpHttpRequest {
    pub body: Bytes,
    pub signal_path: &'static str,
    pub finalizers: EventFinalizers,
    pub metadata: RequestMetadata,
}

impl Finalizable for OtlpHttpRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.finalizers.take_finalizers()
    }
}

impl MetaDescriptive for OtlpHttpRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

pub struct OtlpHttpResponse {
    status: http::StatusCode,
    events_byte_size: GroupedCountByteSize,
}

impl DriverResponse for OtlpHttpResponse {
    fn event_status(&self) -> EventStatus {
        if self.status.is_success() {
            EventStatus::Delivered
        } else if self.status.is_server_error() {
            EventStatus::Errored
        } else {
            EventStatus::Rejected
        }
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.events_byte_size
    }
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct OtlpHttpService {
    client: HttpClient,
    endpoint: String,
    encoding: OtlpHttpEncoding,
}

impl Service<OtlpHttpRequest> for OtlpHttpService {
    type Response = OtlpHttpResponse;
    type Error = OtlpHttpError;
    type Future = BoxFuture<'static, Result<OtlpHttpResponse, OtlpHttpError>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), OtlpHttpError>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: OtlpHttpRequest) -> Self::Future {
        let client = self.client.clone();
        let uri = format!("{}{}", self.endpoint, req.signal_path);
        let metadata = std::mem::take(req.metadata_mut());
        let events_byte_size = metadata.into_events_estimated_json_encoded_byte_size();
        let byte_size = req.body.len();
        let endpoint = self.endpoint.clone();
        let content_type = match self.encoding {
            OtlpHttpEncoding::Protobuf => "application/x-protobuf",
            OtlpHttpEncoding::Json => "application/json",
        };

        Box::pin(async move {
            let http_req = Request::builder()
                .method(Method::POST)
                .uri(&uri)
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(req.body))
                .map_err(|e| OtlpHttpError::HttpRequest {
                    message: e.to_string(),
                })?;

            let response = client
                .send(http_req)
                .await
                .map_err(|e| OtlpHttpError::HttpRequest {
                    message: e.to_string(),
                })?;

            let status = response.status();

            if status.is_success() {
                emit!(EndpointBytesSent {
                    byte_size,
                    protocol: "http",
                    endpoint: &endpoint,
                });
            }

            Ok(OtlpHttpResponse {
                status,
                events_byte_size,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// Retry logic
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct OtlpHttpRetryLogic;

impl RetryLogic for OtlpHttpRetryLogic {
    type Error = OtlpHttpError;
    type Request = OtlpHttpRequest;
    type Response = OtlpHttpResponse;

    fn is_retriable_error(&self, _error: &Self::Error) -> bool {
        true
    }

    fn should_retry_response(&self, response: &Self::Response) -> RetryAction<Self::Request> {
        let status = response.status;
        match status {
            http::StatusCode::TOO_MANY_REQUESTS => RetryAction::Retry("too many requests".into()),
            http::StatusCode::REQUEST_TIMEOUT => RetryAction::Retry("request timeout".into()),
            http::StatusCode::NOT_IMPLEMENTED => {
                RetryAction::DontRetry("endpoint not implemented".into())
            }
            _ if status.is_server_error() => {
                RetryAction::Retry(format!("server error: {status}").into())
            }
            _ if status.is_success() => RetryAction::Successful,
            _ => RetryAction::DontRetry(format!("response status: {status}").into()),
        }
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

struct OtlpHttpSink<S> {
    batch_settings: BatcherSettings,
    service: S,
    encoding: OtlpHttpEncoding,
}

#[derive(Clone)]
struct EventCollection {
    finalizers: EventFinalizers,
    events: Vec<Event>,
    byte_size: usize,
    json_byte_size: GroupedCountByteSize,
}

impl Default for EventCollection {
    fn default() -> Self {
        Self {
            finalizers: Default::default(),
            events: Default::default(),
            byte_size: Default::default(),
            json_byte_size: telemetry().create_request_count_byte_size(),
        }
    }
}

impl<S> OtlpHttpSink<S>
where
    S: Service<OtlpHttpRequest, Response = OtlpHttpResponse> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        input
            .map(|mut event| {
                let byte_size = event.size_of();
                let mut json_byte_size = telemetry().create_request_count_byte_size();
                json_byte_size.add_event(&event, event.estimated_json_encoded_size_of());
                let finalizers = event.take_finalizers();
                (event, finalizers, byte_size, json_byte_size)
            })
            .batched(self.batch_settings.as_reducer_config(
                |(event, _, _, _): &(Event, _, _, _)| event.size_of().max(1),
                BatchReduce::new(
                    |col: &mut EventCollection, (event, finalizers, byte_size, json_size)| {
                        col.finalizers.merge(finalizers);
                        col.events.push(event);
                        col.byte_size += byte_size;
                        col.json_byte_size += json_size;
                    },
                ),
            ))
            .flat_map({
                let encoding = self.encoding;
                move |col| futures::stream::iter(collection_into_http_requests(col, encoding))
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait]
impl<S> StreamSink<Event> for OtlpHttpSink<S>
where
    S: Service<OtlpHttpRequest, Response = OtlpHttpResponse> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

// ---------------------------------------------------------------------------
// Event → OtlpHttpRequest conversion
// ---------------------------------------------------------------------------

fn collection_into_http_requests(
    col: EventCollection,
    encoding: OtlpHttpEncoding,
) -> Vec<OtlpHttpRequest> {
    use sol_lib::opentelemetry::upstream_opentelemetry_proto::tonic::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        logs::v1::ResourceLogs,
        metrics::v1::ResourceMetrics,
        trace::v1::ResourceSpans,
    };

    let n = col.events.len();
    let mut log_resources: Vec<ResourceLogs> = vec![];
    let mut metric_resources: Vec<ResourceMetrics> = vec![];
    let mut trace_resources: Vec<ResourceSpans> = vec![];

    for event in &col.events {
        match event {
            Event::Log(log_event) => {
                log_resources.push(otel_log_event_to_resource_logs(log_event));
            }
            Event::Metric(metric_event) => {
                metric_resources.push(otel_metric_event_to_resource_metrics(metric_event));
            }
            Event::Trace(span_event) => {
                trace_resources.push(otel_span_event_to_resource_spans(span_event));
            }
        }
    }

    let mut requests = Vec::new();
    let builder = RequestMetadataBuilder::new(n, col.byte_size, col.json_byte_size);

    if !log_resources.is_empty() {
        let request = ExportLogsServiceRequest {
            resource_logs: log_resources,
        };
        let body = encode_otlp(&request, encoding, OtlpSignal::Logs);
        let bytes_len = NonZeroUsize::new(body.len().max(1)).unwrap();
        requests.push(OtlpHttpRequest {
            body,
            signal_path: "/v1/logs",
            finalizers: col.finalizers.clone(),
            metadata: builder.with_request_size(bytes_len),
        });
    }

    if !metric_resources.is_empty() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: metric_resources,
        };
        let body = encode_otlp(&request, encoding, OtlpSignal::Metrics);
        let bytes_len = NonZeroUsize::new(body.len().max(1)).unwrap();
        requests.push(OtlpHttpRequest {
            body,
            signal_path: "/v1/metrics",
            finalizers: col.finalizers.clone(),
            metadata: builder.with_request_size(bytes_len),
        });
    }

    if !trace_resources.is_empty() {
        let request = ExportTraceServiceRequest {
            resource_spans: trace_resources,
        };
        let body = encode_otlp(&request, encoding, OtlpSignal::Traces);
        let bytes_len = NonZeroUsize::new(body.len().max(1)).unwrap();
        requests.push(OtlpHttpRequest {
            body,
            signal_path: "/v1/traces",
            finalizers: col.finalizers.clone(),
            metadata: builder.with_request_size(bytes_len),
        });
    }

    requests
}

enum OtlpSignal {
    Logs,
    Metrics,
    Traces,
}

fn encode_otlp(
    msg: &(impl prost::Message + Default + serde::Serialize),
    encoding: OtlpHttpEncoding,
    signal: OtlpSignal,
) -> Bytes {
    match encoding {
        OtlpHttpEncoding::Protobuf => Bytes::from(msg.encode_to_vec()),
        OtlpHttpEncoding::Json => Bytes::from(proto_to_json(msg, signal)),
    }
}

fn proto_to_json(
    msg: &(impl serde::Serialize + prost::Message + Default),
    _signal: OtlpSignal,
) -> Vec<u8> {
    let mut value = serde_json::to_value(msg).expect("JSON serialization");
    strip_nulls(&mut value);
    serde_json::to_vec(&value).expect("JSON re-serialization")
}

fn strip_nulls(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            for v in map.values_mut() {
                strip_nulls(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                strip_nulls(v);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sol_lib::request_metadata::GroupedCountByteSize;
    use sol_lib::stream::DriverResponse;

    fn make_response(status: http::StatusCode) -> OtlpHttpResponse {
        OtlpHttpResponse {
            status,
            events_byte_size: GroupedCountByteSize::default(),
        }
    }

    #[test]
    fn test_otlp_http_response_event_status_2xx() {
        let resp = make_response(http::StatusCode::OK);
        assert_eq!(resp.event_status(), EventStatus::Delivered);
    }

    #[test]
    fn test_otlp_http_response_event_status_4xx() {
        let resp = make_response(http::StatusCode::BAD_REQUEST);
        assert_eq!(resp.event_status(), EventStatus::Rejected);
    }

    #[test]
    fn test_otlp_http_response_event_status_5xx() {
        let resp = make_response(http::StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(resp.event_status(), EventStatus::Errored);
    }

    #[test]
    fn test_otlp_http_response_event_status_3xx() {
        let resp = make_response(http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(resp.event_status(), EventStatus::Rejected);
    }

    #[test]
    fn test_otlp_http_retry_logic_302() {
        let logic = OtlpHttpRetryLogic;
        let resp = make_response(http::StatusCode::FOUND);
        assert!(matches!(
            logic.should_retry_response(&resp),
            RetryAction::DontRetry(_)
        ));
    }

    #[test]
    fn test_otlp_http_retry_logic_400() {
        let logic = OtlpHttpRetryLogic;
        let resp = make_response(http::StatusCode::BAD_REQUEST);
        assert!(matches!(
            logic.should_retry_response(&resp),
            RetryAction::DontRetry(_)
        ));
    }

    #[test]
    fn test_otlp_http_retry_logic_408() {
        let logic = OtlpHttpRetryLogic;
        let resp = make_response(http::StatusCode::REQUEST_TIMEOUT);
        assert!(matches!(
            logic.should_retry_response(&resp),
            RetryAction::Retry(_)
        ));
    }

    #[test]
    fn test_otlp_http_retry_logic_429() {
        let logic = OtlpHttpRetryLogic;
        let resp = make_response(http::StatusCode::TOO_MANY_REQUESTS);
        assert!(matches!(
            logic.should_retry_response(&resp),
            RetryAction::Retry(_)
        ));
    }

    #[test]
    fn test_otlp_http_retry_logic_500() {
        let logic = OtlpHttpRetryLogic;
        let resp = make_response(http::StatusCode::INTERNAL_SERVER_ERROR);
        assert!(matches!(
            logic.should_retry_response(&resp),
            RetryAction::Retry(_)
        ));
    }

    #[test]
    fn test_otlp_http_retry_logic_501() {
        let logic = OtlpHttpRetryLogic;
        let resp = make_response(http::StatusCode::NOT_IMPLEMENTED);
        assert!(matches!(
            logic.should_retry_response(&resp),
            RetryAction::DontRetry(_)
        ));
    }

    #[test]
    fn test_otlp_http_retry_logic_200() {
        let logic = OtlpHttpRetryLogic;
        let resp = make_response(http::StatusCode::OK);
        assert!(matches!(
            logic.should_retry_response(&resp),
            RetryAction::Successful
        ));
    }
}
