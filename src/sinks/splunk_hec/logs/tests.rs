use std::{collections::BTreeMap, sync::Arc};

use chrono::{TimeZone, Utc};
use futures_util::StreamExt;
use serde::{Deserialize, de};
use sol_lib::{
    codecs::{JsonSerializerConfig, TextSerializerConfig},
    event::{Event, EventMetadata, OtelLog, Value},
    lookup::lookup_v2::OptionalTargetPath,
    schema::{Definition, meaning},
};
use vrl::{event_path, metadata_path, owned_value_path, path::OwnedTargetPath, value::Kind};

use super::sink::{HecLogsProcessedEventMetadata, HecProcessedEvent};
use crate::{
    codecs::{Encoder, EncodingConfig},
    config::{SinkConfig, SinkContext},
    sinks::{
        splunk_hec::{
            common::EndpointTarget,
            logs::{config::HecLogsSinkConfig, encoder::HecLogsEncoder, sink::process_log},
        },
        util::{
            Compression, encoding::Encoder as _, processed_event::ProcessedEvent,
            test::build_test_server,
        },
    },
    template::Template,
    test_util::addr::next_addr,
};

#[derive(Deserialize, Debug)]
struct HecEventJson {
    time: Option<f64>,
    event: BTreeMap<String, serde_json::Value>,
    fields: serde_json::Value,
    source: Option<String>,
    sourcetype: Option<String>,
    index: Option<String>,
    host: Option<String>,
}

#[derive(Deserialize, Debug)]
struct HecEventText {
    time: Option<f64>,
    event: String,
    fields: serde_json::Value,
    source: Option<String>,
    sourcetype: Option<String>,
    index: Option<String>,
    host: Option<String>,
}

fn get_encoded_event<D: de::DeserializeOwned>(
    encoding: EncodingConfig,
    processed_event: ProcessedEvent<OtelLog, HecLogsProcessedEventMetadata>,
) -> D {
    let encoder = hec_encoder(encoding);
    let mut bytes = Vec::new();
    encoder
        .encode_input(vec![processed_event], &mut bytes)
        .unwrap();
    serde_json::from_slice::<D>(&bytes).unwrap()
}

fn get_processed_event_timestamp(
    timestamp: Option<Value>,
    timestamp_key: Option<OptionalTargetPath>,
    auto_extract_timestamp: bool,
) -> HecProcessedEvent {
    let mut event = Event::Log(OtelLog::from("hello world"));
    event
        .as_mut_log()
        .insert("event_sourcetype", "test_sourcetype");
    event.as_mut_log().insert("event_source", "test_source");
    event.as_mut_log().insert("event_index", "test_index");
    event.as_mut_log().insert("host_key", "test_host");
    event.as_mut_log().insert("event_field1", "test_value1");
    event.as_mut_log().insert("event_field2", "test_value2");
    event.as_mut_log().insert("key", "value");
    event.as_mut_log().insert("int_val", 123);

    if let Some(OptionalTargetPath {
        path: Some(ts_path),
    }) = &timestamp_key
    {
        if timestamp.is_some() {
            event
                .as_mut_log()
                .insert(&OwnedTargetPath::event(ts_path.path.clone()), timestamp);
        } else {
            event
                .as_mut_log()
                .remove(&OwnedTargetPath::event(ts_path.path.clone()));
        }
    } else if let Some(Value::Timestamp(ts)) = timestamp {
        event.as_mut_log().set_timestamp(ts);
    }

    let sourcetype = Template::try_from("{{ event_sourcetype }}".to_string()).ok();
    let source = Template::try_from("{{ event_source }}".to_string()).ok();
    let index = Template::try_from("{{ event_index }}".to_string()).ok();
    let indexed_fields = vec![
        owned_value_path!("event_field1"),
        owned_value_path!("event_field2"),
    ];
    let timestamp_nanos_key = Some(String::from("ts_nanos_key"));

    process_log(
        event,
        &super::sink::HecLogData {
            sourcetype: sourcetype.as_ref(),
            source: source.as_ref(),
            index: index.as_ref(),
            host_key: Some(OptionalTargetPath {
                path: Some(OwnedTargetPath::event(owned_value_path!("host_key"))),
            }),
            indexed_fields: indexed_fields.as_slice(),
            timestamp_nanos_key: timestamp_nanos_key.as_ref(),
            timestamp_key,
            endpoint_target: EndpointTarget::Event,
            auto_extract_timestamp,
        },
    )
}

fn get_processed_event() -> HecProcessedEvent {
    let mut event = Event::Log(OtelLog::from("hello world"));
    event
        .as_mut_log()
        .insert("event_sourcetype", "test_sourcetype");
    event.as_mut_log().insert("event_source", "test_source");
    event.as_mut_log().insert("event_index", "test_index");
    event.as_mut_log().insert("host_key", "test_host");
    event.as_mut_log().insert("event_field1", "test_value1");
    event.as_mut_log().insert("event_field2", "test_value2");
    event.as_mut_log().insert("key", "value");
    event.as_mut_log().insert("int_val", 123);
    event
        .as_mut_log()
        .set_timestamp(Utc.timestamp_nanos(1638366107111456123));

    let sourcetype = Template::try_from("{{ event_sourcetype }}".to_string()).ok();
    let source = Template::try_from("{{ event_source }}".to_string()).ok();
    let index = Template::try_from("{{ event_index }}".to_string()).ok();
    let indexed_fields = vec![
        owned_value_path!("event_field1"),
        owned_value_path!("event_field2"),
    ];
    let timestamp_nanos_key = Some(String::from("ts_nanos_key"));

    process_log(
        event,
        &super::sink::HecLogData {
            sourcetype: sourcetype.as_ref(),
            source: source.as_ref(),
            index: index.as_ref(),
            host_key: Some(OptionalTargetPath {
                path: Some(OwnedTargetPath::event(owned_value_path!("host_key"))),
            }),
            indexed_fields: indexed_fields.as_slice(),
            timestamp_nanos_key: timestamp_nanos_key.as_ref(),
            timestamp_key: None,
            endpoint_target: EndpointTarget::Event,
            auto_extract_timestamp: false,
        },
    )
}

fn get_event_with_token(msg: &str, token: &str) -> Event {
    let mut event = Event::Log(OtelLog::from(msg));
    event.metadata_mut().set_splunk_hec_token(Arc::from(token));
    event
}

#[test]
fn splunk_process_log_event() {
    let processed_event = get_processed_event();
    let metadata = processed_event.metadata;

    assert_eq!(metadata.sourcetype, Some("test_sourcetype".to_string()));
    assert_eq!(metadata.source, Some("test_source".to_string()));
    assert_eq!(metadata.index, Some("test_index".to_string()));
    assert_eq!(metadata.host, Some(Value::from("test_host")));
    assert!(metadata.fields.contains("event_field1"));
    assert!(metadata.fields.contains("event_field2"));
}

fn hec_encoder(encoding: EncodingConfig) -> HecLogsEncoder {
    let transformer = encoding.transformer();
    let serializer = encoding.build().unwrap();
    let encoder = Encoder::<()>::new(serializer);
    HecLogsEncoder {
        transformer,
        encoder,
        auto_extract_timestamp: false,
    }
}

#[test]
fn splunk_encode_log_event_json() {
    let processed_event = get_processed_event();
    let hec_data =
        get_encoded_event::<HecEventJson>(JsonSerializerConfig::default().into(), processed_event);
    let event = hec_data.event;

    // In OTLP/JSON format, body is wrapped: {"stringValue":"hello world"}
    assert_eq!(
        event.get("body").unwrap(),
        &serde_json::json!({"stringValue": "hello world"})
    );
    // Timestamp is removed from the event (extracted to hec_data.time)
    assert!(!event.contains_key("timeUnixNano"));

    // Attributes like "key", "int_val", "ts_nanos_key" are in the OTLP attributes array
    let attrs = event.get("attributes").unwrap().as_array().unwrap();
    let find_attr = |name: &str| -> Option<&serde_json::Value> {
        attrs.iter().find_map(|a| {
            if a["key"].as_str() == Some(name) {
                Some(&a["value"])
            } else {
                None
            }
        })
    };
    assert_eq!(
        find_attr("key").unwrap(),
        &serde_json::json!({"stringValue": "value"})
    );
    assert_eq!(
        find_attr("int_val").unwrap(),
        &serde_json::json!({"intValue": "123"})
    );

    assert_eq!(hec_data.source, Some("test_source".to_string()));
    assert_eq!(hec_data.sourcetype, Some("test_sourcetype".to_string()));
    assert_eq!(hec_data.index, Some("test_index".to_string()));
    assert_eq!(hec_data.host, Some("test_host".to_string()));

    // Fields are OTLP-serialized; check that the attributes array contains event_field1
    let fields_attrs = hec_data
        .fields
        .get("attributes")
        .unwrap()
        .as_array()
        .unwrap();
    let find_field = |name: &str| -> Option<String> {
        fields_attrs.iter().find_map(|a| {
            if a["key"].as_str() == Some(name) {
                a["value"]["stringValue"].as_str().map(|s| s.to_string())
            } else {
                None
            }
        })
    };
    assert_eq!(find_field("event_field1").unwrap(), "test_value1");

    assert_eq!(hec_data.time, Some(1638366107.111));
    assert_eq!(
        find_attr("ts_nanos_key").unwrap(),
        &serde_json::json!({"intValue": "456123"})
    );
}

#[test]
fn splunk_encode_log_event_text() {
    let processed_event = get_processed_event();
    let hec_data =
        get_encoded_event::<HecEventText>(TextSerializerConfig::default().into(), processed_event);

    assert_eq!(hec_data.event.as_str(), "hello world");

    assert_eq!(hec_data.source, Some("test_source".to_string()));
    assert_eq!(hec_data.sourcetype, Some("test_sourcetype".to_string()));
    assert_eq!(hec_data.index, Some("test_index".to_string()));
    assert_eq!(hec_data.host, Some("test_host".to_string()));

    // Fields are OTLP-serialized; check that the attributes array contains event_field1
    let fields_attrs = hec_data
        .fields
        .get("attributes")
        .unwrap()
        .as_array()
        .unwrap();
    let find_field = |name: &str| -> Option<String> {
        fields_attrs.iter().find_map(|a| {
            if a["key"].as_str() == Some(name) {
                a["value"]["stringValue"].as_str().map(|s| s.to_string())
            } else {
                None
            }
        })
    };
    assert_eq!(find_field("event_field1").unwrap(), "test_value1");

    assert_eq!(hec_data.time, Some(1638366107.111));
}

#[tokio::test]
async fn splunk_passthrough_token() {
    let (_guard, addr) = next_addr();
    let config = HecLogsSinkConfig {
        default_token: "token".to_string().into(),
        endpoint: format!("http://{addr}"),
        host_key: None,
        indexed_fields: Vec::new(),
        index: None,
        sourcetype: None,
        source: None,
        encoding: JsonSerializerConfig::default().into(),
        compression: Compression::None,
        batch: Default::default(),
        request: Default::default(),
        tls: None,
        acknowledgements: Default::default(),
        timestamp_nanos_key: None,
        timestamp_key: None,
        auto_extract_timestamp: None,
        endpoint_target: EndpointTarget::Event,
    };
    let cx = SinkContext::default();

    let (sink, _) = config.build(cx).await.unwrap();

    let (rx, _trigger, server) = build_test_server(addr);
    tokio::spawn(server);

    let events = vec![
        get_event_with_token("message-1", "passthrough-token-1"),
        get_event_with_token("message-2", "passthrough-token-2"),
        Event::Log(OtelLog::from("default token will be used")),
    ];

    sink.run_events(events).await.unwrap();

    let mut tokens = rx
        .take(3)
        .map(|r| r.0.headers.get("Authorization").unwrap().clone())
        .collect::<Vec<_>>()
        .await;

    tokens.sort();
    assert_eq!(
        tokens,
        vec![
            "Splunk passthrough-token-1",
            "Splunk passthrough-token-2",
            "Splunk token"
        ]
    )
}

#[test]
fn splunk_encode_log_event_json_timestamps() {
    crate::test_util::trace_init();

    fn get_hec_data_for_timestamp_test(
        timestamp: Option<Value>,
        timestamp_path: Option<OptionalTargetPath>,
        auto_extract_timestamp: bool,
    ) -> HecEventJson {
        let processed_event =
            get_processed_event_timestamp(timestamp, timestamp_path, auto_extract_timestamp);
        get_encoded_event::<HecEventJson>(JsonSerializerConfig::default().into(), processed_event)
    }

    let no_timestamp = Some(OptionalTargetPath::none());
    let dont_auto_extract = false;
    let do_auto_extract = true;

    // no timestamp_key → remove_timestamp() used, but no timestamp set → None
    let mut hec_data =
        get_hec_data_for_timestamp_test(None, no_timestamp.clone(), dont_auto_extract);
    assert_eq!(hec_data.time, None);

    // no timestamp_key, no timestamp in the event
    hec_data = get_hec_data_for_timestamp_test(None, None, dont_auto_extract);
    assert_eq!(hec_data.time, None);

    // default removal with set_timestamp: timestamp is present → extracted
    hec_data = get_hec_data_for_timestamp_test(
        Some(Value::Timestamp(Utc::now())),
        None,
        dont_auto_extract,
    );
    assert!(hec_data.time.is_some());

    // auto_extract_timestamp is set → timestamp not extracted
    hec_data =
        get_hec_data_for_timestamp_test(Some(Value::Timestamp(Utc::now())), None, do_auto_extract);
    assert_eq!(hec_data.time, None);
}

#[test]
fn splunk_encode_log_event_semantic_meanings() {
    let metadata = EventMetadata::default().with_schema_definition(&Arc::new(
        Definition::new_with_default_metadata(Kind::bytes())
            .with_source_metadata(
                "splunk_hec",
                &owned_value_path!("hostname"),
                Kind::bytes(),
                Some(meaning::HOST),
            )
            .with_source_metadata(
                "splunk_hec",
                &owned_value_path!("timestamp"),
                Kind::timestamp(),
                Some(meaning::TIMESTAMP),
            ),
    ));

    let mut log = OtelLog::new_with_metadata(metadata);
    log.insert(event_path!("message"), "the_message");

    // insert an arbitrary metadata field such that the log becomes Vector namespaced
    log.insert(metadata_path!("vector", "foo"), "bar");

    let og_time = Utc::now();

    // determine the time we expect to get after encoding
    #[allow(clippy::cast_precision_loss)]
    let expected_time = (og_time.timestamp_millis() as f64) / 1000f64;

    log.insert(metadata_path!("splunk_hec", "hostname"), "roast");
    log.insert(
        metadata_path!("splunk_hec", "timestamp"),
        Value::Timestamp(og_time),
    );

    let event = Event::from(log);

    let processed_event = process_log(
        event,
        &super::sink::HecLogData {
            sourcetype: None,
            source: None,
            index: None,
            host_key: None,
            indexed_fields: &[],
            timestamp_nanos_key: None,
            timestamp_key: None,
            endpoint_target: EndpointTarget::Event,
            auto_extract_timestamp: false,
        },
    );

    let hec_data =
        get_encoded_event::<HecEventJson>(JsonSerializerConfig::default().into(), processed_event);

    assert_eq!(hec_data.time, Some(expected_time));

    assert_eq!(hec_data.host, Some("roast".to_string()));
}
