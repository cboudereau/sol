use std::{
    collections::{BTreeMap, HashMap},
    iter::FromIterator,
    net::SocketAddr,
    str,
    time::Duration,
};

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::{Stream, StreamExt};
use http::HeaderMap;
use indoc::indoc;
use opentelemetry_proto::tonic::common::v1::AnyValue;
use prost::Message;
use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
use similar_asserts::assert_eq;
use sol_lib::{
    codecs::{
        BytesDecoder, BytesDeserializer, CharacterDelimitedDecoderConfig,
        decoding::{
            BytesDeserializerConfig, CharacterDelimitedDecoderOptions, Deserializer,
            DeserializerConfig, Framer,
        },
    },
    config::DataType,
    event::OtelAttributes,
    lookup::owned_value_path,
    otel_tags,
};
use tokio::time::timeout;
use vrl::{compiler::value::Collection, value, value::Kind};

use crate::{
    SourceSender,
    common::datadog::{DatadogMetricType, DatadogPoint, DatadogSeriesMetric},
    components::validation::prelude::*,
    config::{SourceConfig, SourceContext},
    event::{
        Event, EventStatus, MetricView, OtelMetric, OtelSpan, Value, into_event_stream,
        metric::MetricKind,
    },
    schema,
    schema::Definition,
    serde::{default_decoding, default_framing_message_based},
    sources::datadog_agent::{
        DatadogAgentConfig, DatadogAgentSource, LOGS, LogMsg, METRICS, TRACES, ddmetric_proto,
        ddtrace_proto, logs::decode_log_body, metrics::DatadogSeriesRequest,
    },
    test_util::{
        addr::{PortGuard, next_addr},
        components::{HTTP_PUSH_SOURCE_TAGS, assert_source_compliance},
        spawn_collect_n, trace_init, wait_for_tcp,
    },
};

const DD_API_KEY: &str = "12345678abcdefgh12345678abcdefgh";
const DD_API_LOGS_V1_PATH: &str = "/v1/input/";
const DD_API_LOGS_V2_PATH: &str = "/api/v2/logs";
const DD_API_SERIES_V1_PATH: &str = "/api/v1/series";
const DD_API_SERIES_V2_PATH: &str = "/api/v2/series";
const DD_API_TRACES_PATH: &str = "/api/v0.2/traces";
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

fn test_logs_schema_definition() -> schema::Definition {
    schema::Definition::empty_definition().with_event_field(
        &owned_value_path!("a log field"),
        Kind::integer().or_bytes(),
        Some("log field"),
    )
}

impl Arbitrary for LogMsg {
    fn arbitrary(g: &mut Gen) -> Self {
        LogMsg {
            message: Bytes::from(String::arbitrary(g)),
            status: Bytes::from(String::arbitrary(g)),
            timestamp: Utc
                .timestamp_millis_opt(i64::from(u32::arbitrary(g)))
                .single()
                .expect("invalid timestamp"),
            hostname: Bytes::from(String::arbitrary(g)),
            service: Bytes::from(String::arbitrary(g)),
            ddsource: Bytes::from(String::arbitrary(g)),
            ddtags: Bytes::from(String::arbitrary(g)),
        }
    }
}

// We want to know that for any json payload that is a `Vec<LogMsg>` we can
// correctly decode it into a `Vec<OtelLog>`. For convenience we assume
// that order is preserved in the decoding step though this is not
// necessarily part of the contract of that function.
#[test]
fn test_decode_log_body() {
    fn inner(msgs: Vec<LogMsg>) -> TestResult {
        let body = Bytes::from(serde_json::to_string(&msgs).unwrap());
        let api_key = None;
        let decoder = sol_lib::codecs::Decoder::new(
            Framer::Bytes(BytesDecoder::new()),
            Deserializer::Bytes(BytesDeserializer),
        );

        let source = DatadogAgentSource::new(
            true,
            decoder,
            "http",
            Some(test_logs_schema_definition()),
            false,
            true,
        );

        let events = decode_log_body(body, api_key, &source).unwrap();
        assert_eq!(events.len(), msgs.len());
        for (msg, event) in msgs.into_iter().zip(events.into_iter()) {
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), Value::from(msg.message));
            assert_eq!(log.get("status").unwrap(), Value::from(msg.status));
            let expected_nanos = msg.timestamp.timestamp_nanos_opt().unwrap();
            if expected_nanos != 0 {
                assert_eq!(
                    log.get("time_unix_nano").unwrap(),
                    Value::Integer(expected_nanos)
                );
            } else {
                // When nanos == 0, time_unix_nano is stored as 0 on the proto record
                // but get("time_unix_nano") skips zero values; instead "timestamp"
                // is stored as a string attribute.
                assert!(log.get("time_unix_nano").is_none());
                assert!(log.get("timestamp").is_some());
            }
            assert_eq!(
                log.get("hostname").unwrap(),
                Value::from(String::from_utf8_lossy(&msg.hostname).into_owned())
            );
            assert_eq!(log.get("service").unwrap(), Value::from(msg.service));
            assert_eq!(log.get("ddsource").unwrap(), Value::from(msg.ddsource));
            assert_eq!(log.get("ddtags").unwrap(), Value::from(msg.ddtags));

            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }

        TestResult::passed()
    }

    QuickCheck::new().quickcheck(inner as fn(Vec<LogMsg>) -> TestResult);
}

#[test]
fn test_decode_log_body_parse_ddtags() {
    let log_msgs = [LogMsg {
        message: Bytes::from(String::from("message")),
        status: Bytes::from(String::from("status")),
        timestamp: Utc
            .timestamp_millis_opt(1234)
            .single()
            .expect("invalid timestamp"),
        hostname: Bytes::from(String::from("host")),
        service: Bytes::from(String::from("service")),
        ddsource: Bytes::from(String::from("ddsource")),
        ddtags: Bytes::from(String::from("wizard:the_grey,env:staging")),
    }];

    let body = Bytes::from(serde_json::to_string(&log_msgs).unwrap());
    let api_key = None;
    let decoder = sol_lib::codecs::Decoder::new(
        Framer::Bytes(BytesDecoder::new()),
        Deserializer::Bytes(BytesDeserializer),
    );

    let source = DatadogAgentSource::new(
        true,
        decoder,
        "http",
        Some(test_logs_schema_definition()),
        true,
        true,
    );

    let events = decode_log_body(body, api_key, &source).unwrap();

    assert_eq!(events.len(), 1);

    let event = events.first().unwrap();
    let log = event.as_log();
    let log_msg = log_msgs[0].clone();

    assert_eq!(log.get("body").unwrap(), Value::from(log_msg.message));
    assert_eq!(log.get("status").unwrap(), Value::from(log_msg.status));
    assert_eq!(
        log.get("time_unix_nano").unwrap(),
        Value::Integer(log_msg.timestamp.timestamp_nanos_opt().unwrap())
    );
    assert_eq!(
        log.get("hostname").unwrap(),
        Value::from(String::from_utf8_lossy(&log_msg.hostname).into_owned())
    );
    assert_eq!(log.get("service").unwrap(), Value::from(log_msg.service));
    assert_eq!(log.get("ddsource").unwrap(), Value::from(log_msg.ddsource));

    assert_eq!(
        log.get("ddtags").unwrap(),
        value!(["wizard:the_grey", "env:staging"])
    );
}

#[test]
fn test_decode_log_body_empty_object() {
    let body = Bytes::from("{}");
    let api_key = None;
    let decoder = sol_lib::codecs::Decoder::new(
        Framer::Bytes(BytesDecoder::new()),
        Deserializer::Bytes(BytesDeserializer),
    );

    let source = DatadogAgentSource::new(
        true,
        decoder,
        "http",
        Some(test_logs_schema_definition()),
        false,
        true,
    );

    let events = decode_log_body(body, api_key, &source).unwrap();
    assert_eq!(events.len(), 0);
}

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<DatadogAgentConfig>();
}

async fn source(
    status: EventStatus,
    acknowledgements: bool,
    store_api_key: bool,
    multiple_outputs: bool,
    split_metric_namespace: bool,
) -> (
    impl Stream<Item = Event> + Unpin,
    Option<impl Stream<Item = Event>>,
    Option<impl Stream<Item = Event>>,
    SocketAddr,
    PortGuard,
) {
    let (sender, recv) = SourceSender::new_test_finalize(status);
    let (logs_output, metrics_output, address, guard) = source_with_sender(
        sender,
        status,
        acknowledgements,
        store_api_key,
        multiple_outputs,
        split_metric_namespace,
    )
    .await;
    (recv, logs_output, metrics_output, address, guard)
}

async fn source_with_timeout(
    status: EventStatus,
    acknowledgements: bool,
    store_api_key: bool,
    multiple_outputs: bool,
    split_metric_namespace: bool,
    send_timeout: Duration,
) -> (
    impl Stream<Item = Event> + Unpin,
    Option<impl Stream<Item = Event>>,
    Option<impl Stream<Item = Event>>,
    SocketAddr,
    PortGuard,
) {
    let (sender, recv) = SourceSender::new_test_sender_with_options(1, Some(send_timeout));
    let (logs_output, metrics_output, address, guard) = source_with_sender(
        sender,
        status,
        acknowledgements,
        store_api_key,
        multiple_outputs,
        split_metric_namespace,
    )
    .await;
    let recv = recv.into_stream().flat_map(into_event_stream);
    (recv, logs_output, metrics_output, address, guard)
}

async fn source_with_sender(
    mut sender: SourceSender,
    status: EventStatus,
    acknowledgements: bool,
    store_api_key: bool,
    multiple_outputs: bool,
    split_metric_namespace: bool,
) -> (
    Option<impl Stream<Item = Event>>,
    Option<impl Stream<Item = Event>>,
    SocketAddr,
    PortGuard,
) {
    let mut logs_output = None;
    let mut metrics_output = None;
    if multiple_outputs {
        logs_output = Some(
            sender
                .add_outputs(status, "logs".to_string())
                .flat_map(into_event_stream),
        );
        metrics_output = Some(
            sender
                .add_outputs(status, "metrics".to_string())
                .flat_map(into_event_stream),
        );
    }
    let (guard, address) = next_addr();
    let config = toml::from_str::<DatadogAgentConfig>(&format!(
        indoc! { r#"
            address = "{}"
            compression = "none"
            store_api_key = {}
            acknowledgements = {}
            multiple_outputs = {}
            split_metric_namespace = {}
            trace_proto = "v1v2"
        "#},
        address, store_api_key, acknowledgements, multiple_outputs, split_metric_namespace
    ))
    .unwrap();
    let schema_definitions =
        HashMap::from([(Some(LOGS.to_owned()), test_logs_schema_definition())]);
    let context = SourceContext::new_test(sender, Some(schema_definitions));
    tokio::spawn(async move {
        config.build(context).await.unwrap().await.unwrap();
    });
    wait_for_tcp(address).await;
    (logs_output, metrics_output, address, guard)
}

async fn send_with_path(address: SocketAddr, body: &str, headers: HeaderMap, path: &str) -> u16 {
    timeout(
        HTTP_REQUEST_TIMEOUT,
        reqwest::Client::new()
            .post(format!("http://{address}{path}"))
            .headers(headers)
            .body(body.to_owned())
            .send(),
    )
    .await
    .expect("send_with_path request timed out")
    .unwrap()
    .status()
    .as_u16()
}

async fn send_and_collect(
    address: SocketAddr,
    body: String,
    headers: HeaderMap,
    path: &'static str,
    rx: impl Stream<Item = Event> + Unpin,
    expected_count: usize,
) -> Vec<Event> {
    spawn_collect_n(
        async move {
            assert_eq!(200, send_with_path(address, &body, headers, path).await);
        },
        rx,
        expected_count,
    )
    .await
}

fn dd_api_key_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("dd-api-key", DD_API_KEY.parse().unwrap());
    headers
}

#[tokio::test]
async fn full_payload_v1() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let mut events = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("foo"),
                timestamp: Utc
                    .timestamp_opt(123, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            HeaderMap::new(),
            DD_API_LOGS_V1_PATH,
            rx,
            1,
        )
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), "foo".into());
            assert_eq!(
                log.get("time_unix_nano").unwrap(),
                Value::Integer(123_000_000_000i64)
            );
            assert_eq!(log.get("hostname").unwrap(), "festeburg".into());
            assert_eq!(log.get("status").unwrap(), "notice".into());
            assert_eq!(log.get("service").unwrap(), "vector".into());
            assert_eq!(log.get("ddsource").unwrap(), "curl".into());
            assert_eq!(log.get("ddtags").unwrap(), "one,two,three".into());
            assert!(event.metadata().secrets().get("datadog_api_key").is_none());
            assert_eq!(log.get_source_type().unwrap(), "datadog_agent".into());
            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn full_payload_v2() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let mut events = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("foo"),
                timestamp: Utc
                    .timestamp_opt(123, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            HeaderMap::new(),
            DD_API_LOGS_V2_PATH,
            rx,
            1,
        )
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), "foo".into());
            assert_eq!(
                log.get("time_unix_nano").unwrap(),
                Value::Integer(123_000_000_000i64)
            );
            assert_eq!(log.get("hostname").unwrap(), "festeburg".into());
            assert_eq!(log.get("status").unwrap(), "notice".into());
            assert_eq!(log.get("service").unwrap(), "vector".into());
            assert_eq!(log.get("ddsource").unwrap(), "curl".into());
            assert_eq!(log.get("ddtags").unwrap(), "one,two,three".into());
            assert!(event.metadata().secrets().get("datadog_api_key").is_none());
            assert_eq!(log.get_source_type().unwrap(), "datadog_agent".into());
            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn no_api_key() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let mut events = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("foo"),
                timestamp: Utc
                    .timestamp_opt(123, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            HeaderMap::new(),
            DD_API_LOGS_V1_PATH,
            rx,
            1,
        )
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), "foo".into());
            assert_eq!(
                log.get("time_unix_nano").unwrap(),
                Value::Integer(123_000_000_000i64)
            );
            assert_eq!(log.get("hostname").unwrap(), "festeburg".into());
            assert_eq!(log.get("status").unwrap(), "notice".into());
            assert_eq!(log.get("service").unwrap(), "vector".into());
            assert_eq!(log.get("ddsource").unwrap(), "curl".into());
            assert_eq!(log.get("ddtags").unwrap(), "one,two,three".into());
            assert!(event.metadata().secrets().get("datadog_api_key").is_none());
            assert_eq!(log.get_source_type().unwrap(), "datadog_agent".into());
            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn api_key_in_url() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let mut events = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("bar"),
                timestamp: Utc
                    .timestamp_opt(456, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            HeaderMap::new(),
            "/v1/input/12345678abcdefgh12345678abcdefgh",
            rx,
            1,
        )
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), "bar".into());
            assert_eq!(
                log.get("time_unix_nano").unwrap(),
                Value::Integer(456_000_000_000i64)
            );
            assert_eq!(log.get("hostname").unwrap(), "festeburg".into());
            assert_eq!(log.get("status").unwrap(), "notice".into());
            assert_eq!(log.get("service").unwrap(), "vector".into());
            assert_eq!(log.get("ddsource").unwrap(), "curl".into());
            assert_eq!(log.get("ddtags").unwrap(), "one,two,three".into());
            assert_eq!(log.get_source_type().unwrap(), "datadog_agent".into());
            assert_eq!(
                &event.metadata().secrets().get("datadog_api_key").unwrap()[..],
                DD_API_KEY
            );
            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn api_key_in_query_params() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let mut events = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("bar"),
                timestamp: Utc
                    .timestamp_opt(456, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            HeaderMap::new(),
            "/api/v2/logs?dd-api-key=12345678abcdefgh12345678abcdefgh",
            rx,
            1,
        )
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), "bar".into());
            assert_eq!(
                log.get("time_unix_nano").unwrap(),
                Value::Integer(456_000_000_000i64)
            );
            assert_eq!(log.get("hostname").unwrap(), "festeburg".into());
            assert_eq!(log.get("status").unwrap(), "notice".into());
            assert_eq!(log.get("service").unwrap(), "vector".into());
            assert_eq!(log.get("ddsource").unwrap(), "curl".into());
            assert_eq!(log.get("ddtags").unwrap(), "one,two,three".into());
            assert_eq!(log.get_source_type().unwrap(), "datadog_agent".into());
            assert_eq!(
                &event.metadata().secrets().get("datadog_api_key").unwrap()[..],
                DD_API_KEY
            );
            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn api_key_in_header() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let mut events = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("baz"),
                timestamp: Utc
                    .timestamp_opt(789, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            dd_api_key_headers(),
            DD_API_LOGS_V1_PATH,
            rx,
            1,
        )
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), "baz".into());
            assert_eq!(
                log.get("time_unix_nano").unwrap(),
                Value::Integer(789_000_000_000i64)
            );
            assert_eq!(log.get("hostname").unwrap(), "festeburg".into());
            assert_eq!(log.get("status").unwrap(), "notice".into());
            assert_eq!(log.get("service").unwrap(), "vector".into());
            assert_eq!(log.get("ddsource").unwrap(), "curl".into());
            assert_eq!(log.get("ddtags").unwrap(), "one,two,three".into());
            assert_eq!(log.get_source_type().unwrap(), "datadog_agent".into());
            assert_eq!(
                &event.metadata().secrets().get("datadog_api_key").unwrap()[..],
                DD_API_KEY
            );
            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn delivery_failure() {
    trace_init();
    let (rx, _, _, addr, _guard) = source(EventStatus::Rejected, true, true, false, true).await;

    spawn_collect_n(
        async move {
            assert_eq!(
                400,
                send_with_path(
                    addr,
                    &serde_json::to_string(&[LogMsg {
                        message: Bytes::from("foo"),
                        timestamp: Utc
                            .timestamp_opt(123, 0)
                            .single()
                            .expect("invalid timestamp"),
                        hostname: Bytes::from("festeburg"),
                        status: Bytes::from("notice"),
                        service: Bytes::from("vector"),
                        ddsource: Bytes::from("curl"),
                        ddtags: Bytes::from("one,two,three"),
                    }])
                    .unwrap(),
                    HeaderMap::new(),
                    DD_API_LOGS_V1_PATH
                )
                .await
            );
        },
        rx,
        1,
    )
    .await;
}

#[tokio::test]
async fn send_timeout_returns_service_unavailable() {
    trace_init();
    let (rx, _, _, addr, _guard) = source_with_timeout(
        EventStatus::Delivered,
        false,
        true,
        false,
        true,
        Duration::from_millis(50),
    )
    .await;

    let body = serde_json::to_string(&[LogMsg {
        message: Bytes::from("foo"),
        timestamp: Utc
            .timestamp_opt(123, 0)
            .single()
            .expect("invalid timestamp"),
        hostname: Bytes::from("festeburg"),
        status: Bytes::from("notice"),
        service: Bytes::from("vector"),
        ddsource: Bytes::from("curl"),
        ddtags: Bytes::from("one,two,three"),
    }])
    .unwrap();

    assert_eq!(
        200,
        send_with_path(addr, &body, HeaderMap::new(), DD_API_LOGS_V1_PATH).await
    );

    assert_eq!(
        503,
        send_with_path(addr, &body, HeaderMap::new(), DD_API_LOGS_V1_PATH).await
    );
    drop(rx);
}

#[test]
fn parse_config_with_send_timeout_secs() {
    let config = toml::from_str::<DatadogAgentConfig>(indoc! { r#"
            address = "0.0.0.0:8012"
            send_timeout_secs = 1.5
        "#})
    .unwrap();

    assert_eq!(config.send_timeout_secs, Some(1.5));
    assert_eq!(config.send_timeout(), Some(Duration::from_secs_f64(1.5)));
}

#[test]
fn parse_config_without_send_timeout_secs() {
    let config = toml::from_str::<DatadogAgentConfig>(indoc! { r#"
            address = "0.0.0.0:8012"
        "#})
    .unwrap();

    assert_eq!(config.send_timeout_secs, None);
    assert_eq!(config.send_timeout(), None);
}

#[tokio::test]
async fn ignores_disabled_acknowledgements() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Rejected, false, true, false, true).await;

        let events = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("foo"),
                timestamp: Utc
                    .timestamp_opt(123, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            HeaderMap::new(),
            DD_API_LOGS_V1_PATH,
            rx,
            1,
        )
        .await;

        assert_eq!(events.len(), 1);
    })
    .await;
}

#[tokio::test]
async fn ignores_api_key() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, false, false, true).await;

        let mut events = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("baz"),
                timestamp: Utc
                    .timestamp_opt(789, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            dd_api_key_headers(),
            "/v1/input/12345678abcdefgh12345678abcdefgh",
            rx,
            1,
        )
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), "baz".into());
            assert_eq!(
                log.get("time_unix_nano").unwrap(),
                Value::Integer(789_000_000_000i64)
            );
            assert_eq!(log.get("hostname").unwrap(), "festeburg".into());
            assert_eq!(log.get("status").unwrap(), "notice".into());
            assert_eq!(log.get("service").unwrap(), "vector".into());
            assert_eq!(log.get("ddsource").unwrap(), "curl".into());
            assert_eq!(log.get("ddtags").unwrap(), "one,two,three".into());
            assert_eq!(log.get_source_type().unwrap(), "datadog_agent".into());
            assert!(event.metadata().secrets().get("datadog_api_key").is_none());
            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn decode_series_endpoint_v1() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let dd_metric_request = DatadogSeriesRequest {
            series: vec![
                DatadogSeriesMetric {
                    metric: "dd_gauge".to_string(),
                    r#type: DatadogMetricType::Gauge,
                    interval: None,
                    points: vec![
                        DatadogPoint(1542182950, 3.14),
                        DatadogPoint(1542182951, 3.1415),
                    ],
                    tags: Some(vec!["foo:bar".to_string()]),
                    host: Some("random_host".to_string()),
                    source_type_name: None,
                    device: None,
                    metadata: None,
                },
                DatadogSeriesMetric {
                    metric: "dd_rate".to_string(),
                    r#type: DatadogMetricType::Rate,
                    interval: Some(10),
                    points: vec![DatadogPoint(1542182950, 3.14)],
                    tags: Some(vec!["foo:bar:baz".to_string()]),
                    host: Some("another_random_host".to_string()),
                    source_type_name: None,
                    device: None,
                    metadata: None,
                },
                DatadogSeriesMetric {
                    metric: "dd_count".to_string(),
                    r#type: DatadogMetricType::Count,
                    interval: None,
                    points: vec![DatadogPoint(1542182955, 16777216_f64)],
                    tags: Some(vec!["foobar".to_string()]),
                    host: Some("a_host".to_string()),
                    source_type_name: None,
                    device: None,
                    metadata: None,
                },
                DatadogSeriesMetric {
                    metric: "system.disk.free".to_string(),
                    r#type: DatadogMetricType::Count,
                    interval: None,
                    points: vec![DatadogPoint(1542182955, 16777216_f64)],
                    tags: None,
                    host: None,
                    source_type_name: None,
                    device: None,
                    metadata: None,
                },
                DatadogSeriesMetric {
                    metric: "system.disk".to_string(),
                    r#type: DatadogMetricType::Count,
                    interval: None,
                    points: vec![DatadogPoint(1542182955, 16777216_f64)],
                    tags: None,
                    host: None,
                    source_type_name: None,
                    device: None,
                    metadata: None,
                },
            ],
        };
        let events = send_and_collect(
            addr,
            serde_json::to_string(&dd_metric_request).unwrap(),
            dd_api_key_headers(),
            DD_API_SERIES_V1_PATH,
            rx,
            6,
        )
        .await;

        {
            let mut metric = events[0].as_metric();
            assert_eq!(metric.name(), "dd_gauge");
            assert_eq!(metric.namespace(), None);
            assert_eq!(
                metric.timestamp(),
                Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                        .single()
                        .expect("invalid timestamp")
                )
            );
            assert_eq!(metric.kind(), MetricKind::Absolute);
            assert!(matches!(metric.view(), MetricView::Gauge { value: 3.14 }));
            assert_tags(
                &metric,
                otel_tags!(
                    "resource.host.name" => "random_host",
                    "resource.source_type" => "datadog_agent",
                    "foo" => "bar",
                ),
            );

            assert_eq!(
                &events[0]
                    .metadata()
                    .secrets()
                    .get("datadog_api_key")
                    .unwrap()[..],
                DD_API_KEY
            );

            metric = events[1].as_metric();
            assert_eq!(metric.name(), "dd_gauge");
            assert_eq!(metric.namespace(), None);
            assert_eq!(
                metric.timestamp(),
                Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 11)
                        .single()
                        .expect("invalid timestamp")
                )
            );
            assert_eq!(metric.kind(), MetricKind::Absolute);
            assert!(matches!(metric.view(), MetricView::Gauge { value: 3.1415 }));
            assert_tags(
                &metric,
                otel_tags!(
                    "resource.host.name" => "random_host",
                    "resource.source_type" => "datadog_agent",
                    "foo" => "bar",
                ),
            );

            assert_eq!(
                &events[1]
                    .metadata()
                    .secrets()
                    .get("datadog_api_key")
                    .unwrap()[..],
                DD_API_KEY
            );

            metric = events[2].as_metric();
            assert_eq!(metric.name(), "dd_rate");
            assert_eq!(metric.namespace(), None);
            assert_eq!(
                metric.timestamp(),
                Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                        .single()
                        .expect("invalid timestamp")
                )
            );
            assert_eq!(metric.kind(), MetricKind::Incremental);
            assert!(matches!(metric.view(), MetricView::Sum { value } if value == 3.14 * 10_f64));
            {
                let mut expected_tags = otel_tags!(
                    "resource.host.name" => "another_random_host",
                    "resource.source_type" => "datadog_agent",
                    "foo" => "bar:baz",
                );
                expected_tags.insert(
                    "interval_ms".to_string(),
                    AnyValue {
                        value: Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(
                                10000,
                            ),
                        ),
                    },
                );
                assert_tags(&metric, expected_tags);
            }

            assert_eq!(
                &events[2]
                    .metadata()
                    .secrets()
                    .get("datadog_api_key")
                    .unwrap()[..],
                DD_API_KEY
            );

            metric = events[3].as_metric();
            assert_eq!(metric.name(), "dd_count");
            assert_eq!(
                metric.timestamp(),
                Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 15)
                        .single()
                        .expect("invalid timestamp")
                )
            );
            assert_eq!(metric.kind(), MetricKind::Incremental);
            assert!(matches!(
                metric.view(),
                MetricView::Sum { value: 16777216.0 }
            ));
            {
                let mut expected_tags = otel_tags!(
                    "resource.host.name" => "a_host",
                    "resource.source_type" => "datadog_agent",
                );
                expected_tags.insert("foobar".to_string(), AnyValue { value: None });
                assert_tags(&metric, expected_tags);
            }

            metric = events[4].as_metric();
            assert_eq!(metric.name(), "disk.free");
            assert_eq!(metric.namespace(), Some("system"));

            metric = events[5].as_metric();
            assert_eq!(metric.name(), "disk");
            assert_eq!(metric.namespace(), Some("system"));

            assert_eq!(
                &events[3]
                    .metadata()
                    .secrets()
                    .get("datadog_api_key")
                    .unwrap()[..],
                DD_API_KEY
            );
        }
    })
    .await;
}

#[tokio::test]
async fn decode_traces() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let mut headers = dd_api_key_headers();
        headers.insert("X-Datadog-Reported-Languages", "ada".parse().unwrap());

        let mut buf_v1 = Vec::new();

        let span = ddtrace_proto::Span {
            service: "a_service".to_string(),
            name: "a_name".to_string(),
            resource: "a_resource".to_string(),
            trace_id: 123u64,
            span_id: 456u64,
            parent_id: 789u64,
            start: 1_431_648_000_000_001i64,
            duration: 1_000_000_000i64,
            error: 404i32,
            meta: BTreeMap::from_iter([("foo".to_string(), "bar".to_string())].into_iter()),
            metrics: BTreeMap::from_iter([("a_metrics".to_string(), 0.577f64)].into_iter()),
            r#type: "a_type".to_string(),
            meta_struct: BTreeMap::new(),
        };

        let trace = ddtrace_proto::ApiTrace {
            trace_id: 123u64,
            spans: vec![span.clone()],
            start_time: 1_431_648_000_000_001i64,
            end_time: 1_431_649_000_000_001i64,
        };

        let payload_v1 = ddtrace_proto::TracePayload {
            host_name: "a_hostname".to_string(),
            env: "an_environment".to_string(),
            traces: vec![trace],
            transactions: vec![span.clone()],
            // Other filea
            tracer_payloads: vec![],
            tags: BTreeMap::new(),
            agent_version: "".to_string(),
            target_tps: 0f64,
            error_tps: 0f64,
        };

        payload_v1.encode(&mut buf_v1).unwrap();

        let mut buf_v2 = Vec::new();

        let chunk = ddtrace_proto::TraceChunk {
            priority: 42i32,
            origin: "an_origin".to_string(),
            dropped_trace: false,
            spans: vec![span],
            tags: BTreeMap::from_iter([("a".to_string(), "tag".to_string())].into_iter()),
        };

        let tracer_payload = ddtrace_proto::TracerPayload {
            container_id: "an_id".to_string(),
            language_name: "plop".to_string(),
            language_version: "v33".to_string(),
            tracer_version: "v577".to_string(),
            runtime_id: "123abc".to_string(),
            chunks: vec![chunk],
            env: "env".to_string(),
            tags: BTreeMap::from_iter([("another".to_string(), "tag".to_string())].into_iter()),
            hostname: "hostname".to_string(),
            app_version: "v314".to_string(),
        };

        let payload_v2 = ddtrace_proto::TracePayload {
            host_name: "a_hostname".to_string(),
            env: "env".to_string(),
            traces: vec![],
            transactions: vec![],
            tracer_payloads: vec![tracer_payload],
            tags: BTreeMap::new(),
            agent_version: "v1.23456".to_string(),
            target_tps: 10f64,
            error_tps: 10f64,
        };

        payload_v2.encode(&mut buf_v2).unwrap();

        let events = spawn_collect_n(
            async move {
                assert_eq!(
                    200,
                    send_with_path(
                        addr,
                        unsafe { str::from_utf8_unchecked(&buf_v1) },
                        headers.clone(),
                        DD_API_TRACES_PATH
                    )
                    .await
                );
                assert_eq!(
                    200,
                    send_with_path(
                        addr,
                        unsafe { str::from_utf8_unchecked(&buf_v2) },
                        headers,
                        DD_API_TRACES_PATH
                    )
                    .await
                );
            },
            rx,
            3,
        )
        .await;

        // Helper to find an attribute value by key
        fn find_attr<'a>(
            attrs: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
            key: &str,
        ) -> Option<&'a opentelemetry_proto::tonic::common::v1::AnyValue> {
            attrs
                .iter()
                .find(|kv| kv.key == key)
                .and_then(|kv| kv.value.as_ref())
        }
        fn attr_str(
            attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
            key: &str,
        ) -> String {
            match find_attr(attrs, key).and_then(|v| v.value.as_ref()) {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                    s.clone()
                }
                other => panic!("expected string attribute for '{key}', got {other:?}"),
            }
        }
        fn attr_double(
            attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
            key: &str,
        ) -> f64 {
            match find_attr(attrs, key).and_then(|v| v.value.as_ref()) {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d)) => {
                    *d
                }
                other => panic!("expected double attribute for '{key}', got {other:?}"),
            }
        }
        fn attr_int(attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue], key: &str) -> i64 {
            match find_attr(attrs, key).and_then(|v| v.value.as_ref()) {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => *i,
                other => panic!("expected int attribute for '{key}', got {other:?}"),
            }
        }
        fn resource_attr_str(span: &OtelSpan, key: &str) -> String {
            let resource = span.resource_proto().expect("resource should be set");
            attr_str(&resource.attributes, key)
        }

        {
            // --- v0 trace span ---
            let span_v1 = events[0].as_trace();
            let otel_span = span_v1.span();

            // Resource attributes
            assert_eq!(resource_attr_str(span_v1, "host.name"), "a_hostname");
            assert_eq!(
                resource_attr_str(span_v1, "deployment.environment"),
                "an_environment"
            );

            // Scope (language from X-Datadog-Reported-Languages header)
            let scope = span_v1.scope().expect("scope should be set");
            assert_eq!(scope.name, "datadog.tracer.ada");

            // Span fields
            assert_eq!(otel_span.name, "a_name");
            assert_eq!(otel_span.trace_id, {
                let mut bytes = vec![0u8; 16];
                bytes[8..16].copy_from_slice(&123u64.to_be_bytes());
                bytes
            });
            assert_eq!(otel_span.span_id, 456u64.to_be_bytes().to_vec());
            assert_eq!(otel_span.parent_span_id, 789u64.to_be_bytes().to_vec());
            assert_eq!(otel_span.start_time_unix_nano, 1_431_648_000_000_001u64);
            assert_eq!(
                otel_span.end_time_unix_nano,
                1_431_648_000_000_001u64 + 1_000_000_000u64
            );

            // Status (error=404 → Error)
            let status = otel_span.status.as_ref().unwrap();
            assert_eq!(
                status.code,
                opentelemetry_proto::tonic::trace::v1::status::StatusCode::Error as i32
            );

            // Span attributes (from DD meta + metrics)
            assert_eq!(
                attr_str(&span_v1.attributes().to_key_values(), "foo"),
                "bar"
            );
            assert_eq!(
                attr_double(&span_v1.attributes().to_key_values(), "a_metrics"),
                0.577
            );
            assert_eq!(
                attr_str(&span_v1.attributes().to_key_values(), "dd.resource"),
                "a_resource"
            );

            assert_eq!(
                &events[0]
                    .metadata()
                    .secrets()
                    .get("datadog_api_key")
                    .unwrap()[..],
                DD_API_KEY
            );

            // --- v0 APM transaction span ---
            let apm_span = events[1].as_trace();
            let apm_otel = apm_span.span();
            assert_eq!(resource_attr_str(apm_span, "host.name"), "a_hostname");
            assert_eq!(
                resource_attr_str(apm_span, "deployment.environment"),
                "an_environment"
            );
            assert_eq!(apm_otel.name, "a_name");
            assert_eq!(
                attr_str(&apm_span.attributes().to_key_values(), "dd.resource"),
                "a_resource"
            );

            assert_eq!(
                &events[1]
                    .metadata()
                    .secrets()
                    .get("datadog_api_key")
                    .unwrap()[..],
                DD_API_KEY
            );

            // --- v1 trace chunk span ---
            let span_v2 = events[2].as_trace();
            let v2_otel = span_v2.span();
            assert_eq!(resource_attr_str(span_v2, "host.name"), "a_hostname");
            assert_eq!(resource_attr_str(span_v2, "deployment.environment"), "env");

            // Chunk-level tags as span attributes
            let v2_kvs = span_v2.attributes().to_key_values();
            assert_eq!(attr_str(&v2_kvs, "a"), "tag");
            assert_eq!(attr_str(&v2_kvs, "another"), "tag");

            // Scope from tracer payload
            let scope_v2 = span_v2.scope().expect("scope should be set");
            assert_eq!(scope_v2.name, "datadog.tracer.plop");
            assert_eq!(scope_v2.version, "v577");

            // Tracer-level attributes
            assert_eq!(attr_str(&v2_kvs, "dd.language_version"), "v33");
            assert_eq!(attr_str(&v2_kvs, "dd.container_id"), "an_id");
            assert_eq!(attr_str(&v2_kvs, "dd.origin"), "an_origin");
            assert_eq!(attr_str(&v2_kvs, "dd.runtime_id"), "123abc");
            assert_eq!(attr_str(&v2_kvs, "dd.app_version"), "v314");
            assert_eq!(attr_int(&v2_kvs, "dd.priority"), 42);
            assert_eq!(attr_double(&v2_kvs, "dd.target_tps"), 10.0);
            assert_eq!(attr_double(&v2_kvs, "dd.error_tps"), 10.0);

            // v2 span fields
            assert_eq!(v2_otel.name, "a_name");
            assert_eq!(attr_str(&v2_kvs, "dd.resource"), "a_resource");
            assert_eq!(v2_otel.trace_id, {
                let mut bytes = vec![0u8; 16];
                bytes[8..16].copy_from_slice(&123u64.to_be_bytes());
                bytes
            });
            assert_eq!(v2_otel.span_id, 456u64.to_be_bytes().to_vec());
            assert_eq!(v2_otel.parent_span_id, 789u64.to_be_bytes().to_vec());
            assert_eq!(v2_otel.start_time_unix_nano, 1_431_648_000_000_001u64);
            assert_eq!(
                v2_otel.end_time_unix_nano,
                1_431_648_000_000_001u64 + 1_000_000_000u64
            );

            // Status (error=404 → Error)
            let v2_status = v2_otel.status.as_ref().unwrap();
            assert_eq!(
                v2_status.code,
                opentelemetry_proto::tonic::trace::v1::status::StatusCode::Error as i32
            );

            // Meta + metrics as span attributes
            assert_eq!(attr_str(&v2_kvs, "foo"), "bar");
            assert_eq!(attr_double(&v2_kvs, "a_metrics"), 0.577);

            assert_eq!(
                &events[2]
                    .metadata()
                    .secrets()
                    .get("datadog_api_key")
                    .unwrap()[..],
                DD_API_KEY
            );
        }
    })
    .await;
}

#[tokio::test]
async fn split_outputs() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (_, rx_logs, rx_metrics, addr, _guard) =
            source(EventStatus::Delivered, true, true, true, true).await;

        let mut log_event = send_and_collect(
            addr,
            serde_json::to_string(&[LogMsg {
                message: Bytes::from("baz"),
                timestamp: Utc
                    .timestamp_opt(789, 0)
                    .single()
                    .expect("invalid timestamp"),
                hostname: Bytes::from("festeburg"),
                status: Bytes::from("notice"),
                service: Bytes::from("vector"),
                ddsource: Bytes::from("curl"),
                ddtags: Bytes::from("one,two,three"),
            }])
            .unwrap(),
            dd_api_key_headers(),
            DD_API_LOGS_V1_PATH,
            rx_logs.unwrap(),
            1,
        )
        .await;

        let mut headers_for_metric = HeaderMap::new();
        headers_for_metric.insert(
            "dd-api-key",
            "abcdefgh12345678abcdefgh12345678".parse().unwrap(),
        );
        let dd_metric_request = DatadogSeriesRequest {
            series: vec![DatadogSeriesMetric {
                metric: "dd_gauge".to_string(),
                r#type: DatadogMetricType::Gauge,
                interval: None,
                points: vec![
                    DatadogPoint(1542182950, 3.14),
                    DatadogPoint(1542182951, 3.1415),
                ],
                tags: Some(vec!["foo:bar".to_string()]),
                host: Some("random_host".to_string()),
                source_type_name: None,
                device: None,
                metadata: None,
            }],
        };
        let mut metric_event = send_and_collect(
            addr,
            serde_json::to_string(&dd_metric_request).unwrap(),
            headers_for_metric,
            DD_API_SERIES_V1_PATH,
            rx_metrics.unwrap(),
            1,
        )
        .await;

        {
            let event = metric_event.remove(0);
            let metric = event.as_metric();
            assert_eq!(metric.name(), "dd_gauge");
            assert_eq!(
                metric.timestamp(),
                Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                        .single()
                        .expect("invalid timestamp")
                )
            );
            assert_eq!(metric.kind(), MetricKind::Absolute);
            assert!(matches!(metric.view(), MetricView::Gauge { value: 3.14 }));
            assert_tags(
                &metric,
                otel_tags!(
                    "resource.host.name" => "random_host",
                    "resource.source_type" => "datadog_agent",
                    "foo" => "bar",
                ),
            );
            assert_eq!(
                &event.metadata().secrets().get("datadog_api_key").unwrap()[..],
                "abcdefgh12345678abcdefgh12345678"
            );
        }

        {
            let event = log_event.remove(0);
            let log = event.as_log();
            assert_eq!(log.get("body").unwrap(), "baz".into());
            assert_eq!(
                log.get("time_unix_nano").unwrap(),
                Value::Integer(789_000_000_000i64)
            );
            assert_eq!(log.get("hostname").unwrap(), "festeburg".into());
            assert_eq!(log.get("status").unwrap(), "notice".into());
            assert_eq!(log.get("service").unwrap(), "vector".into());
            assert_eq!(log.get("ddsource").unwrap(), "curl".into());
            assert_eq!(log.get("ddtags").unwrap(), "one,two,three".into());
            assert_eq!(log.get_source_type().unwrap(), "datadog_agent".into());
            assert_eq!(
                &event.metadata().secrets().get("datadog_api_key").unwrap()[..],
                DD_API_KEY
            );
            assert_eq!(
                event.metadata().schema_definition().as_ref(),
                &test_logs_schema_definition()
            );
        }
    })
    .await;
}

#[test]
fn test_config_outputs_with_disabled_data_types() {
    struct TestCase {
        multiple_outputs: bool,
        disable_logs: bool,
        disable_metrics: bool,
        disable_traces: bool,
    }

    for TestCase {
        multiple_outputs,
        disable_logs,
        disable_metrics,
        disable_traces,
    } in [
        TestCase {
            multiple_outputs: true,
            disable_logs: true,
            disable_metrics: true,
            disable_traces: true,
        },
        TestCase {
            multiple_outputs: true,
            disable_logs: true,
            disable_metrics: false,
            disable_traces: false,
        },
        TestCase {
            multiple_outputs: true,
            disable_logs: false,
            disable_metrics: true,
            disable_traces: false,
        },
        TestCase {
            multiple_outputs: true,
            disable_logs: false,
            disable_metrics: false,
            disable_traces: true,
        },
        TestCase {
            multiple_outputs: true,
            disable_logs: true,
            disable_metrics: true,
            disable_traces: false,
        },
        TestCase {
            multiple_outputs: true,
            disable_logs: false,
            disable_metrics: false,
            disable_traces: false,
        },
        TestCase {
            multiple_outputs: false,
            disable_logs: true,
            disable_metrics: true,
            disable_traces: true,
        },
    ] {
        let config = DatadogAgentConfig {
            address: "0.0.0.0:8080".parse().unwrap(),
            tls: None,
            store_api_key: true,
            framing: default_framing_message_based(),
            decoding: default_decoding(),
            acknowledgements: Default::default(),
            multiple_outputs,
            disable_logs,
            disable_metrics,
            disable_traces,
            parse_ddtags: false,
            split_metric_namespace: true,
            keepalive: Default::default(),
            send_timeout_secs: None,
            resource_attributes: Default::default(),
        };

        let outputs: Vec<DataType> = config
            .outputs()
            .into_iter()
            .map(|output| output.ty)
            .collect();
        if multiple_outputs {
            assert_eq!(outputs.contains(&DataType::Log), !disable_logs);
            assert_eq!(outputs.contains(&DataType::Trace), !disable_traces);
            assert_eq!(outputs.contains(&DataType::Metric), !disable_metrics);
        } else {
            assert!(outputs.contains(&DataType::all_bits()));
            assert!(outputs.len() == 1);
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn test_config_outputs() {
    struct TestCase {
        decoding: DeserializerConfig,
        multiple_outputs: bool,
        want: HashMap<Option<&'static str>, Option<schema::Definition>>,
    }

    for (
        title,
        TestCase {
            decoding,
            multiple_outputs,
            want,
        },
    ) in [
        (
            "default decoding",
            TestCase {
                decoding: default_decoding(),
                multiple_outputs: false,
                want: HashMap::from([(
                    None,
                    Some(
                        schema::Definition::empty_definition()
                            .with_event_field(
                                &owned_value_path!("body"),
                                Kind::bytes(),
                                Some("message"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("status"),
                                Kind::bytes(),
                                Some("severity"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("timestamp"),
                                Kind::timestamp(),
                                Some("timestamp"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("hostname"),
                                Kind::bytes(),
                                Some("host"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("service"),
                                Kind::bytes(),
                                Some("service"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("ddsource"),
                                Kind::bytes(),
                                Some("source"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("ddtags"),
                                Kind::bytes(),
                                Some("tags"),
                            )
                            .with_standard_vector_source_metadata(),
                    ),
                )]),
            },
        ),
        (
            "bytes / single output",
            TestCase {
                decoding: DeserializerConfig::Bytes,
                multiple_outputs: false,
                want: HashMap::from([(
                    None,
                    Some(
                        schema::Definition::empty_definition()
                            .with_event_field(
                                &owned_value_path!("body"),
                                Kind::bytes(),
                                Some("message"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("status"),
                                Kind::bytes(),
                                Some("severity"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("timestamp"),
                                Kind::timestamp(),
                                Some("timestamp"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("hostname"),
                                Kind::bytes(),
                                Some("host"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("service"),
                                Kind::bytes(),
                                Some("service"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("ddsource"),
                                Kind::bytes(),
                                Some("source"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("ddtags"),
                                Kind::bytes(),
                                Some("tags"),
                            )
                            .with_standard_vector_source_metadata(),
                    ),
                )]),
            },
        ),
        (
            "bytes / multiple output",
            TestCase {
                decoding: DeserializerConfig::Bytes,
                multiple_outputs: true,
                want: HashMap::from([
                    (
                        Some(LOGS),
                        Some(
                            schema::Definition::empty_definition()
                                .with_event_field(
                                    &owned_value_path!("body"),
                                    Kind::bytes(),
                                    Some("message"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("status"),
                                    Kind::bytes(),
                                    Some("severity"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("timestamp"),
                                    Kind::timestamp(),
                                    Some("timestamp"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("hostname"),
                                    Kind::bytes(),
                                    Some("host"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("service"),
                                    Kind::bytes(),
                                    Some("service"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("ddsource"),
                                    Kind::bytes(),
                                    Some("source"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("ddtags"),
                                    Kind::bytes(),
                                    Some("tags"),
                                )
                                .with_standard_vector_source_metadata(),
                        ),
                    ),
                    (Some(METRICS), None),
                    (Some(TRACES), None),
                ]),
            },
        ),
        (
            "json / single output",
            TestCase {
                decoding: DeserializerConfig::Json(Default::default()),
                multiple_outputs: false,
                want: HashMap::from([(
                    None,
                    Some(
                        DeserializerConfig::Json(Default::default())
                            .schema_definition()
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("status"),
                                Kind::bytes(),
                                Some("severity"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("timestamp"),
                                Kind::timestamp(),
                                Some("timestamp"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("hostname"),
                                Kind::bytes(),
                                Some("host"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("service"),
                                Kind::bytes(),
                                Some("service"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("ddsource"),
                                Kind::bytes(),
                                Some("source"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("ddtags"),
                                Kind::bytes(),
                                Some("tags"),
                            )
                            .with_standard_vector_source_metadata(),
                    ),
                )]),
            },
        ),
        (
            "json / multiple output",
            TestCase {
                decoding: DeserializerConfig::Json(Default::default()),
                multiple_outputs: true,
                want: HashMap::from([
                    (
                        Some(LOGS),
                        Some(
                            DeserializerConfig::Json(Default::default())
                                .schema_definition()
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("status"),
                                    Kind::bytes(),
                                    Some("severity"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("timestamp"),
                                    Kind::timestamp(),
                                    Some("timestamp"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("hostname"),
                                    Kind::bytes(),
                                    Some("host"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("service"),
                                    Kind::bytes(),
                                    Some("service"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("ddsource"),
                                    Kind::bytes(),
                                    Some("source"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("ddtags"),
                                    Kind::bytes(),
                                    Some("tags"),
                                )
                                .with_standard_vector_source_metadata(),
                        ),
                    ),
                    (Some(METRICS), None),
                    (Some(TRACES), None),
                ]),
            },
        ),
        #[cfg(feature = "codecs-syslog")]
        (
            "syslog / single output",
            TestCase {
                decoding: DeserializerConfig::Syslog(Default::default()),
                multiple_outputs: false,
                want: HashMap::from([(
                    None,
                    Some(
                        DeserializerConfig::Syslog(Default::default())
                            .schema_definition()
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("status"),
                                Kind::bytes(),
                                Some("severity"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("timestamp"),
                                Kind::timestamp(),
                                Some("timestamp"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("hostname"),
                                Kind::bytes(),
                                Some("host"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("service"),
                                Kind::bytes(),
                                Some("service"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("ddsource"),
                                Kind::bytes(),
                                Some("source"),
                            )
                            .with_source_metadata(
                                "datadog_agent",
                                &owned_value_path!("ddtags"),
                                Kind::bytes(),
                                Some("tags"),
                            )
                            .with_standard_vector_source_metadata(),
                    ),
                )]),
            },
        ),
        #[cfg(feature = "codecs-syslog")]
        (
            "syslog / multiple output",
            TestCase {
                decoding: DeserializerConfig::Syslog(Default::default()),
                multiple_outputs: true,
                want: HashMap::from([
                    (
                        Some(LOGS),
                        Some(
                            DeserializerConfig::Syslog(Default::default())
                                .schema_definition()
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("status"),
                                    Kind::bytes(),
                                    Some("severity"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("timestamp"),
                                    Kind::timestamp(),
                                    Some("timestamp"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("hostname"),
                                    Kind::bytes(),
                                    Some("host"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("service"),
                                    Kind::bytes(),
                                    Some("service"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("ddsource"),
                                    Kind::bytes(),
                                    Some("source"),
                                )
                                .with_source_metadata(
                                    "datadog_agent",
                                    &owned_value_path!("ddtags"),
                                    Kind::bytes(),
                                    Some("tags"),
                                )
                                .with_standard_vector_source_metadata(),
                        ),
                    ),
                    (Some(METRICS), None),
                    (Some(TRACES), None),
                ]),
            },
        ),
    ] {
        let config = DatadogAgentConfig {
            address: "0.0.0.0:8080".parse().unwrap(),
            tls: None,
            store_api_key: true,
            framing: default_framing_message_based(),
            decoding,
            acknowledgements: Default::default(),
            multiple_outputs,
            disable_logs: false,
            disable_metrics: false,
            disable_traces: false,
            parse_ddtags: false,
            split_metric_namespace: true,
            keepalive: Default::default(),
            send_timeout_secs: None,
            resource_attributes: Default::default(),
        };

        let mut outputs = config
            .outputs()
            .into_iter()
            .map(|output| (output.port.clone(), output.schema_definition(true)))
            .collect::<HashMap<_, _>>();

        for (name, want) in want {
            let got = outputs
                .remove(&name.map(ToOwned::to_owned))
                .expect("output exists");

            assert_eq!(got, want, "{}", title);
        }
    }
}

#[tokio::test]
async fn decode_series_endpoint_v2() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        let (rx, _, _, addr, _guard) =
            source(EventStatus::Delivered, true, true, false, true).await;

        let series = vec![
            ddmetric_proto::metric_payload::MetricSeries {
                resources: vec![ddmetric_proto::metric_payload::Resource {
                    r#type: "host".to_string(),
                    name: "random_host".to_string(),
                }],
                metric: "namespace.dd_gauge".to_string(),
                tags: vec!["foo:bar".to_string()],
                points: vec![
                    ddmetric_proto::metric_payload::MetricPoint {
                        value: 3.14,
                        timestamp: 1542182950,
                    },
                    ddmetric_proto::metric_payload::MetricPoint {
                        value: 3.1415,
                        timestamp: 1542182951,
                    },
                ],
                r#type: ddmetric_proto::metric_payload::MetricType::Gauge as i32,
                unit: "".to_string(),
                source_type_name: "a_random_source_type_name".to_string(),
                interval: 10, // Dogstatsd sets Gauge interval to 10 by default
                metadata: None,
            },
            ddmetric_proto::metric_payload::MetricSeries {
                resources: vec![ddmetric_proto::metric_payload::Resource {
                    r#type: "host".to_string(),
                    name: "another_random_host".to_string(),
                }],
                metric: "another_namespace.dd_rate".to_string(),
                tags: vec!["foo:bar:baz".to_string(), "foo:bizbaz".to_string()],
                points: vec![ddmetric_proto::metric_payload::MetricPoint {
                    value: 3.14,
                    timestamp: 1542182950,
                }],
                r#type: ddmetric_proto::metric_payload::MetricType::Rate as i32,
                unit: "".to_string(),
                source_type_name: "another_random_source_type_name".to_string(),
                interval: 10,
                metadata: None,
            },
            ddmetric_proto::metric_payload::MetricSeries {
                resources: vec![ddmetric_proto::metric_payload::Resource {
                    r#type: "host".to_string(),
                    name: "a_host".to_string(),
                }],
                metric: "dd_count".to_string(),
                tags: vec!["foobar".to_string()],
                points: vec![ddmetric_proto::metric_payload::MetricPoint {
                    value: 16777216_f64,
                    timestamp: 1542182955,
                }],
                r#type: ddmetric_proto::metric_payload::MetricType::Count as i32,
                unit: "".to_string(),
                source_type_name: "a_very_random_source_type_name".to_string(),
                interval: 0,
                metadata: Some(ddmetric_proto::Metadata {
                    origin: Some(ddmetric_proto::Origin {
                        origin_product: 10,
                        origin_category: 10,
                        origin_service: 42,
                    }),
                }),
            },
        ];

        let series_payload = ddmetric_proto::MetricPayload { series };

        let mut buf = Vec::new();
        series_payload.encode(&mut buf).unwrap();
        let body = unsafe { String::from_utf8_unchecked(buf) };
        let events = send_and_collect(
            addr,
            body,
            dd_api_key_headers(),
            DD_API_SERIES_V2_PATH,
            rx,
            4,
        )
        .await;

        {
            let mut metric = events[0].as_metric();
            assert_eq!(metric.name(), "dd_gauge");
            assert_eq!(
                metric.timestamp(),
                Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                        .single()
                        .expect("invalid timestamp")
                )
            );
            assert_eq!(metric.kind(), MetricKind::Absolute);
            assert!(matches!(metric.view(), MetricView::Gauge { value: 3.14 }));
            {
                let mut expected_tags = otel_tags!(
                    "resource.host.name" => "random_host",
                    "resource.source_type" => "datadog_agent",
                    "foo" => "bar",
                    "source_type_name" => "a_random_source_type_name",
                );
                expected_tags.insert(
                    "interval_ms".to_string(),
                    AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(10000)),
                    },
                );
                assert_tags(&metric, expected_tags);
            }
            assert_eq!(metric.namespace(), Some("namespace"));

            assert_eq!(
                &events[0].metadata().secrets().get("datadog_api_key").unwrap()[..],
                DD_API_KEY
            );

            metric = events[1].as_metric();
            assert_eq!(metric.name(), "dd_gauge");
            assert_eq!(
                metric.timestamp(),
                Some(Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 11).unwrap())
            );
            assert_eq!(metric.kind(), MetricKind::Absolute);
            assert!(matches!(metric.view(), MetricView::Gauge { value: 3.1415 }));
            {
                let mut expected_tags = otel_tags!(
                    "resource.host.name" => "random_host",
                    "resource.source_type" => "datadog_agent",
                    "foo" => "bar",
                    "source_type_name" => "a_random_source_type_name",
                );
                expected_tags.insert(
                    "interval_ms".to_string(),
                    AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(10000)),
                    },
                );
                assert_tags(&metric, expected_tags);
            }
            assert_eq!(metric.namespace(), Some("namespace"));

            assert_eq!(
                &events[1].metadata().secrets().get("datadog_api_key").unwrap()[..],
                DD_API_KEY
            );

            metric = events[2].as_metric();
            assert_eq!(metric.name(), "dd_rate");
            assert_eq!(
                metric.timestamp(),
                Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                        .single()
                        .expect("invalid timestamp")
                )
            );
            assert_eq!(metric.kind(), MetricKind::Incremental);
            assert!(matches!(metric.view(), MetricView::Sum { value } if value == 3.14 * 10_f64));
            {
                use opentelemetry_proto::tonic::common::v1::ArrayValue;
                let mut expected_tags = otel_tags!(
                    "resource.host.name" => "another_random_host",
                    "resource.source_type" => "datadog_agent",
                    "source_type_name" => "another_random_source_type_name",
                );
                expected_tags.insert(
                    "foo".to_string(),
                    AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::ArrayValue(ArrayValue {
                            values: vec![
                                AnyValue { value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("bar:baz".to_string())) },
                                AnyValue { value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue("bizbaz".to_string())) },
                            ],
                        })),
                    },
                );
                expected_tags.insert(
                    "interval_ms".to_string(),
                    AnyValue {
                        value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(10000)),
                    },
                );
                assert_tags(&metric, expected_tags);
            }
            assert_eq!(metric.namespace(), Some("another_namespace"));

            assert_eq!(
                &events[2].metadata().secrets().get("datadog_api_key").unwrap()[..],
                DD_API_KEY
            );

            metric = events[3].as_metric();
            assert_eq!(metric.name(), "dd_count");
            assert_eq!(
                metric.timestamp(),
                Some(
                    Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 15)
                        .single()
                        .expect("invalid timestamp")
                )
            );
            assert_eq!(metric.kind(), MetricKind::Incremental);
            assert!(matches!(metric.view(), MetricView::Sum { value: 16777216.0 }));
            {
                let mut expected_tags = otel_tags!(
                    "resource.host.name" => "a_host",
                    "resource.source_type" => "datadog_agent",
                    "source_type_name" => "a_very_random_source_type_name",
                );
                expected_tags.insert("foobar".to_string(), AnyValue { value: None });
                assert_tags(&metric, expected_tags);
            }
            assert_eq!(metric.namespace(), None);

            assert_eq!(
                &events[3].metadata().secrets().get("datadog_api_key").unwrap()[..],
                DD_API_KEY
            );
        }
    })
    .await;
}

#[test]
fn test_output_schema_definition_json_vector_namespace() {
    let definition = toml::from_str::<DatadogAgentConfig>(indoc! { r#"
            address = "0.0.0.0:8012"
            decoding.codec = "json"
        "#})
    .unwrap()
    .outputs()
    .remove(0)
    .schema_definition(true);

    assert_eq!(
        definition,
        Some(
            Definition::new_with_default_metadata(Kind::object(Collection::empty()))
                .unknown_fields(Kind::json())
                .with_event_field(&owned_value_path!("time_unix_nano"), Kind::json(), None)
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "ddsource"),
                    Kind::bytes(),
                    Some("source")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "ddtags"),
                    Kind::bytes(),
                    Some("tags")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "hostname"),
                    Kind::bytes(),
                    Some("host")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "service"),
                    Kind::bytes(),
                    Some("service")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "status"),
                    Kind::bytes(),
                    Some("severity")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "timestamp"),
                    Kind::timestamp(),
                    Some("timestamp")
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "ingest_timestamp"),
                    Kind::timestamp(),
                    None
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "source_type"),
                    Kind::bytes(),
                    None
                )
        )
    )
}

#[test]
fn test_output_schema_definition_bytes_vector_namespace() {
    let definition = toml::from_str::<DatadogAgentConfig>(indoc! { r#"
            address = "0.0.0.0:8012"
            decoding.codec = "bytes"
        "#})
    .unwrap()
    .outputs()
    .remove(0)
    .schema_definition(true);

    assert_eq!(
        definition,
        Some(
            Definition::new_with_default_metadata(Kind::object(Collection::empty()))
                .with_event_field(&owned_value_path!("body"), Kind::bytes(), Some("message"))
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "ddsource"),
                    Kind::bytes(),
                    Some("source")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "ddtags"),
                    Kind::bytes(),
                    Some("tags")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "hostname"),
                    Kind::bytes(),
                    Some("host")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "service"),
                    Kind::bytes(),
                    Some("service")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "status"),
                    Kind::bytes(),
                    Some("severity")
                )
                .with_metadata_field(
                    &owned_value_path!("datadog_agent", "timestamp"),
                    Kind::timestamp(),
                    Some("timestamp")
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "ingest_timestamp"),
                    Kind::timestamp(),
                    None
                )
                .with_metadata_field(
                    &owned_value_path!("vector", "source_type"),
                    Kind::bytes(),
                    None
                )
        )
    )
}

fn assert_tags(metric: &OtelMetric, expected: OtelAttributes) {
    let all = metric.all_tags_including_resource().expect("Missing tags");
    for (k, v) in expected.iter() {
        assert_eq!(all.get(k), Some(v), "Tag {k} mismatch");
    }
}

async fn test_series_v1_split_metric_namespace_impl(
    split: bool,
    expected_name: &str,
    expected_namespace: Option<&str>,
) {
    let (rx, _, _, addr, _guard) = source(EventStatus::Delivered, true, true, false, split).await;

    let dd_metric_request = DatadogSeriesRequest {
        series: vec![DatadogSeriesMetric {
            metric: "system.disk.free".to_string(),
            r#type: DatadogMetricType::Gauge,
            interval: None,
            points: vec![DatadogPoint(1542182950, 42.0)],
            tags: Some(vec!["foo:bar".to_string()]),
            host: Some("test_host".to_string()),
            source_type_name: None,
            device: None,
            metadata: None,
        }],
    };

    let events = send_and_collect(
        addr,
        serde_json::to_string(&dd_metric_request).unwrap(),
        dd_api_key_headers(),
        DD_API_SERIES_V1_PATH,
        rx,
        1,
    )
    .await;

    let metric = events[0].as_metric();
    assert_eq!(metric.name(), expected_name);
    assert_eq!(metric.namespace(), expected_namespace);
}

#[tokio::test]
async fn series_v1_split_metric_namespace_true() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        test_series_v1_split_metric_namespace_impl(true, "disk.free", Some("system")).await;
    })
    .await;
}

#[tokio::test]
async fn series_v1_split_metric_namespace_false() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        test_series_v1_split_metric_namespace_impl(false, "system.disk.free", None).await;
    })
    .await;
}

async fn test_series_v2_split_metric_namespace_impl(
    split: bool,
    expected_name: &str,
    expected_namespace: Option<&str>,
) {
    let (rx, _, _, addr, _guard) = source(EventStatus::Delivered, true, true, false, split).await;

    let series = vec![ddmetric_proto::metric_payload::MetricSeries {
        resources: vec![ddmetric_proto::metric_payload::Resource {
            r#type: "host".to_string(),
            name: "test_host".to_string(),
        }],
        metric: "system.disk.free".to_string(),
        tags: vec!["foo:bar".to_string()],
        points: vec![ddmetric_proto::metric_payload::MetricPoint {
            value: 42.0,
            timestamp: 1542182950,
        }],
        r#type: ddmetric_proto::metric_payload::MetricType::Gauge as i32,
        unit: "".to_string(),
        source_type_name: "".to_string(),
        interval: 10,
        metadata: None,
    }];

    let series_payload = ddmetric_proto::MetricPayload { series };

    let mut buf = Vec::new();
    series_payload.encode(&mut buf).unwrap();
    let body = unsafe { String::from_utf8_unchecked(buf) };
    let events = send_and_collect(
        addr,
        body,
        dd_api_key_headers(),
        DD_API_SERIES_V2_PATH,
        rx,
        1,
    )
    .await;

    let metric = events[0].as_metric();
    assert_eq!(metric.name(), expected_name);
    assert_eq!(metric.namespace(), expected_namespace);
}

#[tokio::test]
async fn series_v2_split_metric_namespace_true() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        test_series_v2_split_metric_namespace_impl(true, "disk.free", Some("system")).await;
    })
    .await;
}

#[tokio::test]
async fn series_v2_split_metric_namespace_false() {
    assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
        test_series_v2_split_metric_namespace_impl(false, "system.disk.free", None).await;
    })
    .await;
}

impl ValidatableComponent for DatadogAgentConfig {
    fn validation_configuration() -> ValidationConfiguration {
        use sol_lib::codecs::DecodingConfig;

        let config = DatadogAgentConfig {
            address: "0.0.0.0:9007".parse().unwrap(),
            tls: None,
            store_api_key: false,
            framing: CharacterDelimitedDecoderConfig {
                character_delimited: CharacterDelimitedDecoderOptions {
                    delimiter: b',',
                    max_length: Some(usize::MAX),
                },
            }
            .into(),
            decoding: BytesDeserializerConfig::new().into(),
            acknowledgements: Default::default(),
            multiple_outputs: false,
            disable_logs: false,
            disable_metrics: false,
            disable_traces: false,
            parse_ddtags: false,
            split_metric_namespace: true,
            keepalive: Default::default(),
            send_timeout_secs: None,
            resource_attributes: Default::default(),
        };

        // TODO set up separate test cases for metrics and traces endpoints

        let logs_addr = format!("http://{}/api/v2/logs", config.address);
        let uri = http::Uri::try_from(&logs_addr).expect("should not fail to parse URI");

        let decoder = DecodingConfig::new(
            config.framing.clone(),
            DeserializerConfig::Json(Default::default()),
        );

        let external_resource = ExternalResource::new(
            ResourceDirection::Push,
            HttpResourceConfig::from_parts(uri, None),
            decoder,
        );

        ValidationConfiguration::from_source(
            Self::NAME,
            vec![ComponentTestCaseConfig::from_source(
                config,
                None,
                Some(external_resource),
            )],
        )
    }
}

register_validatable_component!(DatadogAgentConfig);
