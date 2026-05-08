#![cfg(test)]
use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::common::v1::AnyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValueKind;
use similar_asserts::assert_eq;
use sol_lib::event;

use crate::{
    event::{Event, OtelLog, string_value},
    transforms::{FunctionTransform, OutputBuffer},
};

/// Build a log event for test purposes.
///
/// Parsers produce native OtelLog events regardless of namespace,
/// so this helper builds an OtelLog directly for both branches.
pub fn make_log_event(
    message: vrl::value::Value,
    timestamp: &str,
    stream: &str,
    is_partial: bool,
) -> Event {
    let timestamp = DateTime::parse_from_rfc3339(timestamp)
        .expect("invalid timestamp in test case")
        .with_timezone(&Utc);

    let msg_str = match &message {
        vrl::value::Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => other.to_string(),
    };
    let mut otel_log = OtelLog::from_bytes(msg_str.as_bytes());
    #[allow(clippy::cast_sign_loss)]
    {
        otel_log.record_mut().time_unix_nano = timestamp.timestamp_nanos_opt().unwrap_or(0) as u64;
    }
    otel_log.set_attribute("stream".to_string(), string_value(stream));
    if is_partial {
        otel_log.set_attribute(
            event::PARTIAL.to_string(),
            AnyValue {
                value: Some(OtelValueKind::BoolValue(true)),
            },
        );
    }
    Event::from(otel_log)
}

/// Normalize an OtelLog event for comparison: clear the source_event_id
/// (which is a random UUID). Attribute sorting is no longer needed because
/// `OtelAttributes` uses a `BTreeMap` (inherently sorted).
fn normalize_for_comparison(event: &mut Event, shared_metadata: &crate::event::EventMetadata) {
    if let Event::Log(otel_log) = event {
        *otel_log.metadata_mut() = shared_metadata.clone();
    }
}

/// Shared logic for testing parsers.
///
/// Takes a parser builder and a list of test cases.
pub fn test_parser<B, L, S, F>(builder: B, loader: L, cases: Vec<(S, Vec<Event>)>)
where
    B: Fn() -> F,
    F: FunctionTransform,
    L: Fn(S) -> Event,
{
    for (message, mut expected) in cases {
        let input = loader(message);
        let mut parser = (builder)();
        let mut output = OutputBuffer::default();
        parser.transform(&mut output, input);

        let mut actual = output.into_events().collect::<Vec<_>>();

        let shared_meta = crate::event::EventMetadata::default();
        for e in expected.iter_mut() {
            normalize_for_comparison(e, &shared_meta);
        }
        for e in actual.iter_mut() {
            normalize_for_comparison(e, &shared_meta);
        }

        assert_eq!(expected, actual, "expected left, actual right");
    }
}
