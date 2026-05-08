use sol_lib::{
    codecs::{JsonSerializerConfig, TextSerializerConfig},
    event::{Event, MetricKind, OtelLog, OtelMetric},
    request_metadata::GroupedCountByteSize,
};

use super::{config::RedisSinkConfig, request_builder::encode_event};
use crate::codecs::{Encoder, Transformer};

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<RedisSinkConfig>();
}

#[test]
fn redis_log_event_json() {
    let msg = "hello_world".to_owned();
    let mut byte_size = GroupedCountByteSize::new_untagged();
    let mut evt = OtelLog::from(msg.clone());
    evt.insert("key", "value");
    let result = encode_event(
        evt.into(),
        "key".to_string(),
        None,
        &Default::default(),
        &mut Encoder::<()>::new(JsonSerializerConfig::default().build().into()),
        &mut byte_size,
    )
    .unwrap()
    .value;
    let json: serde_json::Value = serde_json::from_slice(&result[..]).unwrap();
    // OTLP JSON: body is {"stringValue":"..."}
    assert_eq!(
        json.get("body").unwrap(),
        &serde_json::json!({"stringValue": msg})
    );
}

#[test]
fn redis_log_event_text() {
    let msg = "hello_world".to_owned();
    let evt = OtelLog::from(msg.clone());
    let mut byte_size = GroupedCountByteSize::new_untagged();
    let event = encode_event(
        evt.into(),
        "key".to_string(),
        None,
        &Default::default(),
        &mut Encoder::<()>::new(TextSerializerConfig::default().build().into()),
        &mut byte_size,
    )
    .unwrap()
    .value;
    assert_eq!(event, Vec::from(msg));
}

#[test]
fn redis_log_encode_event() {
    let msg = "hello_world";
    let mut evt = OtelLog::from(msg);
    let mut byte_size = GroupedCountByteSize::new_untagged();
    evt.insert("key", "value");

    let result = encode_event(
        evt.into(),
        "key".to_string(),
        None,
        &Transformer::new(None, Some(vec!["key".into()]), None).unwrap(),
        &mut Encoder::<()>::new(JsonSerializerConfig::default().build().into()),
        &mut byte_size,
    )
    .unwrap()
    .value;

    let json: serde_json::Value = serde_json::from_slice(&result[..]).unwrap();
    // After removing "key", there should be no "key" attribute in OTLP JSON
    let has_key_attr = json
        .get("attributes")
        .and_then(|a| a.as_array())
        .map(|arr| arr.iter().any(|a| a["key"] == "key"))
        .unwrap_or(false);
    assert!(!has_key_attr);
}

#[test]
fn redis_metric_encode_event() {
    let mut byte_size = GroupedCountByteSize::new_untagged();
    let metric = OtelMetric::new_counter("test_counter", MetricKind::Absolute, 42.0);

    let result = encode_event(
        Event::Metric(metric),
        "metrics.counter".to_string(),
        None,
        &Default::default(),
        &mut Encoder::<()>::new(JsonSerializerConfig::default().build().into()),
        &mut byte_size,
    )
    .unwrap()
    .value;

    let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

    assert_eq!(json["name"], "test_counter");
    // OTLP-native JSON format: counter is Sum with isMonotonic=true
    assert!(json["sum"].is_object(), "expected sum field in OTLP format");
    assert_eq!(json["sum"]["dataPoints"][0]["asDouble"], 42.0);
}

#[test]
fn redis_log_scoring() {
    let msg = "hello_world";
    let mut evt = OtelLog::from(msg);
    let mut byte_size = GroupedCountByteSize::new_untagged();
    evt.insert("key", "value");

    let result = encode_event(
        evt.into(),
        "key".to_string(),
        Some(64),
        &Default::default(),
        &mut Encoder::<()>::new(JsonSerializerConfig::default().build().into()),
        &mut byte_size,
    )
    .unwrap()
    .score;

    assert_eq!(result, Some(64));
}
