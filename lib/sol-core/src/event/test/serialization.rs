use regex::Regex;
use similar_asserts::assert_eq;

use super::*;

#[test]
fn serialization() {
    let mut event = OtelLog::from("raw log line");
    event.insert(vrl::event_path!("foo"), "bar");
    event.insert(vrl::event_path!("bar"), "baz");

    // Convert Vec<(KeyString, Value)> to an ObjectMap for JSON object serialization
    let fields: vrl::value::ObjectMap = event.all_event_fields().unwrap().into_iter().collect();
    let actual_all = serde_json::to_value(&fields).unwrap();

    // OtelLog::from("...") sets body but does not auto-insert a timestamp
    // like LogEvent::from("...") did.  Verify the fields that are present.
    assert_eq!(actual_all["body"], serde_json::json!("raw log line"));
    assert_eq!(actual_all["foo"], serde_json::json!("bar"));
    assert_eq!(actual_all["bar"], serde_json::json!("baz"));

    // If a timestamp was populated, verify its format.
    if let Some(ts_str) = actual_all
        .pointer("/timestamp")
        .and_then(serde_json::Value::as_str)
    {
        let rfc3339_re = Regex::new(r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\z").unwrap();
        assert!(rfc3339_re.is_match(ts_str));
    }
}

#[test]
fn type_serialization() {
    use serde_json::json;

    let mut event = OtelLog::from("hello world");
    event.insert(vrl::event_path!("int"), 4);
    event.insert(vrl::event_path!("float"), 5.5);
    event.insert(vrl::event_path!("bool"), true);
    event.insert(vrl::event_path!("string"), "thisisastring");

    let fields: vrl::value::ObjectMap = event.all_event_fields().unwrap().into_iter().collect();
    let map = serde_json::to_value(&fields).unwrap();
    assert_eq!(map["float"], json!(5.5));
    assert_eq!(map["int"], json!(4));
    assert_eq!(map["bool"], json!(true));
    assert_eq!(map["string"], json!("thisisastring"));
}
