#![deny(missing_docs)]

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bytes::BytesMut;
use futures::{Stream, StreamExt};
use sol_lib::stream::expiration_map::{Emitter, map_with_expiration};

use crate::{
    event,
    event::{Event, OtelLog, Value},
    internal_events::KubernetesMergedLineTooBigError,
};

/// The key we use for `file` field.
const FILE_KEY: &str = "file";

const EXPIRATION_TIME: Duration = Duration::from_secs(30);

struct PartialEventMergeState {
    buckets: HashMap<String, Bucket>,
    maybe_max_merged_line_bytes: Option<usize>,
}

impl PartialEventMergeState {
    fn add_event(
        &mut self,
        event: OtelLog,
        file: &str,
        expiration_time: Duration,
    ) {
        let new_body = event.body_string();
        let new_body_bytes = bytes::Bytes::from(new_body);

        if let Some(bucket) = self.buckets.get_mut(file) {
            if bucket.exceeds_max_merged_line_limit {
                return;
            }

            let prev_body = bucket.event.body_string();
            let mut bytes_mut = BytesMut::new();
            bytes_mut.extend_from_slice(prev_body.as_bytes());
            bytes_mut.extend_from_slice(&new_body_bytes);

            if let Some(max_merged_line_bytes) = self.maybe_max_merged_line_bytes
                && bytes_mut.len() > max_merged_line_bytes
            {
                bucket.exceeds_max_merged_line_limit = true;
                emit!(KubernetesMergedLineTooBigError {
                    event: &Value::Bytes(new_body_bytes),
                    configured_limit: max_merged_line_bytes,
                    encountered_size_so_far: bytes_mut.len()
                });
            }

            bucket.event.set_body(crate::event::string_value(
                String::from_utf8_lossy(&bytes_mut),
            ));
        } else {
            let mut exceeds_max_merged_line_limit = false;

            if let Some(max_merged_line_bytes) = self.maybe_max_merged_line_bytes {
                exceeds_max_merged_line_limit = new_body_bytes.len() > max_merged_line_bytes;
                if exceeds_max_merged_line_limit {
                    emit!(KubernetesMergedLineTooBigError {
                        event: &Value::Bytes(new_body_bytes.clone()),
                        configured_limit: max_merged_line_bytes,
                        encountered_size_so_far: new_body_bytes.len()
                    });
                }
            }

            self.buckets.insert(
                file.to_owned(),
                Bucket {
                    event,
                    expiration: Instant::now() + expiration_time,
                    exceeds_max_merged_line_limit,
                },
            );
        }
    }

    fn remove_event(&mut self, file: &str) -> Option<OtelLog> {
        self.buckets
            .remove(file)
            .filter(|bucket| !bucket.exceeds_max_merged_line_limit)
            .map(|bucket| bucket.event)
    }

    fn emit_expired_events(&mut self, emitter: &mut Emitter<OtelLog>) {
        let now = Instant::now();
        self.buckets.retain(|_key, bucket| {
            let expired = now >= bucket.expiration;
            if expired && !bucket.exceeds_max_merged_line_limit {
                emitter.emit(bucket.event.clone());
            }
            !expired
        });
    }

    fn flush_events(&mut self, emitter: &mut Emitter<OtelLog>) {
        for (_, bucket) in self.buckets.drain() {
            if !bucket.exceeds_max_merged_line_limit {
                emitter.emit(bucket.event);
            }
        }
    }
}

struct Bucket {
    event: OtelLog,
    expiration: Instant,
    exceeds_max_merged_line_limit: bool,
}

pub fn merge_partial_events(
    stream: impl Stream<Item = Event> + 'static,
    maybe_max_merged_line_bytes: Option<usize>,
) -> impl Stream<Item = Event> {
    merge_partial_events_with_custom_expiration(
        stream,
        EXPIRATION_TIME,
        maybe_max_merged_line_bytes,
    )
}

// internal function that allows customizing the expiration time (for testing)
fn merge_partial_events_with_custom_expiration(
    stream: impl Stream<Item = Event> + 'static,
    expiration_time: Duration,
    maybe_max_merged_line_bytes: Option<usize>,
) -> impl Stream<Item = Event> {
    let state = PartialEventMergeState {
        buckets: HashMap::new(),
        maybe_max_merged_line_bytes,
    };

    map_with_expiration(
        state,
        stream.map(|e| e.into_log()),
        Duration::from_secs(1),
        move |state: &mut PartialEventMergeState,
              otel_log: OtelLog,
              emitter: &mut Emitter<OtelLog>| {
            use crate::event::OtelValueKind;
            let is_partial = otel_log.attribute(event::PARTIAL)
                .and_then(|av| av.value.as_ref())
                .map(|v| matches!(v, OtelValueKind::StringValue(s) if s == "true" || s == "1")
                    || matches!(v, OtelValueKind::BoolValue(true)))
                .unwrap_or(false);

            let file = otel_log.attribute(FILE_KEY)
                .and_then(|av| av.value.as_ref())
                .and_then(|v| if let OtelValueKind::StringValue(s) = v { Some(s.clone()) } else { None })
                .unwrap_or_default();

            state.add_event(otel_log, &file, expiration_time);
            if !is_partial && let Some(merged) = state.remove_event(&file) {
                emitter.emit(merged);
            }
        },
        |state: &mut PartialEventMergeState, emitter: &mut Emitter<OtelLog>| {
            state.emit_expired_events(emitter)
        },
        |state: &mut PartialEventMergeState, emitter: &mut Emitter<OtelLog>| {
            state.flush_events(emitter);
        },
    )
    // OtelLog -> Event
    .map(|e| e.into())
}

#[cfg(test)]
mod test {
    use sol_lib::event::OtelLog;
    use vrl::value;

    use super::*;

    #[tokio::test]
    async fn merge_single_event_legacy() {
        let mut e_1 = OtelLog::from("test message 1");
        e_1.insert("foo", 1);

        let input_stream = futures::stream::iter([e_1.into()]);
        let output_stream = merge_partial_events(input_stream, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].as_log().get(".body"),
            Some(value!("test message 1"))
        );
    }

    #[tokio::test]
    async fn merge_single_event_legacy_exceeds_max_merged_line_limit() {
        let mut e_1 = OtelLog::from("test message 1");
        e_1.insert("foo", 1);

        let input_stream = futures::stream::iter([e_1.into()]);
        let output_stream = merge_partial_events(input_stream, Some(1));

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn merge_multiple_events_legacy() {
        let mut e_1 = OtelLog::from("test message 1");
        e_1.insert("foo", 1);
        e_1.insert("_partial", true);

        let mut e_2 = OtelLog::from("test message 2");
        e_2.insert("foo2", 1);

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        let output_stream = merge_partial_events(input_stream, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].as_log().get(".body"),
            Some(value!("test message 1test message 2"))
        );
    }

    #[tokio::test]
    async fn merge_multiple_events_legacy_exceeds_max_merged_line_limit() {
        let mut e_1 = OtelLog::from("test message 1");
        e_1.insert("foo", 1);
        e_1.insert("_partial", true);

        let mut e_2 = OtelLog::from("test message 2");
        e_2.insert("foo2", 1);

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        // 24 > length of first message but less than the two combined
        let output_stream = merge_partial_events(input_stream, Some(24));

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn multiple_events_flush_legacy() {
        let mut e_1 = OtelLog::from("test message 1");
        e_1.insert("foo", 1);
        e_1.insert("_partial", true);

        let mut e_2 = OtelLog::from("test message 2");
        e_2.insert("foo2", 1);
        e_1.insert("_partial", true);

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        let output_stream = merge_partial_events(input_stream, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].as_log().get(".body"),
            Some(value!("test message 1test message 2"))
        );
    }

    #[tokio::test]
    async fn multiple_events_flush_legacy_exceeds_max_merged_line_limit() {
        let mut e_1 = OtelLog::from("test message 1");
        e_1.insert("foo", 1);
        e_1.insert("_partial", true);

        let mut e_2 = OtelLog::from("test message 2");
        e_2.insert("foo2", 1);
        e_1.insert("_partial", true);

        let input_stream = futures::stream::iter([e_1.into(), e_2.into()]);
        // 24 > length of first message but less than the two combined
        let output_stream = merge_partial_events(input_stream, Some(24));

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 0);
    }

    #[tokio::test]
    async fn multiple_events_expire_legacy() {
        let mut e_1 = OtelLog::from("test message");
        e_1.insert(FILE_KEY, "foo1");
        e_1.insert("_partial", true);

        let mut e_2 = OtelLog::from("test message");
        e_2.insert(FILE_KEY, "foo2");
        e_1.insert("_partial", true);

        // and input stream that never ends
        let input_stream =
            futures::stream::iter([e_1.into(), e_2.into()]).chain(futures::stream::pending());

        let output_stream = merge_partial_events_with_custom_expiration(
            input_stream,
            Duration::from_secs(1),
            None,
        );

        let output: Vec<Event> = output_stream.take(2).collect().await;
        assert_eq!(output.len(), 2);
        assert_eq!(
            output[0].as_log().get(".body"),
            Some(value!("test message"))
        );
        assert_eq!(
            output[1].as_log().get(".body"),
            Some(value!("test message"))
        );
    }

    #[tokio::test]
    async fn merge_single_event_vector_namespace() {
        use crate::event::string_value;

        let mut e_1 = OtelLog::from_bytes(b"test message 1");
        e_1.set_attribute(FILE_KEY.to_string(), string_value("foo1"));

        let input_stream = futures::stream::iter([Event::from(e_1)]);
        let output_stream = merge_partial_events(input_stream, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].as_log().body_string(), "test message 1");
    }

    #[tokio::test]
    async fn merge_multiple_events_vector_namespace() {
        use crate::event::string_value;
        use opentelemetry_proto::tonic::common::v1::AnyValue;
        use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValueKind;

        let mut e_1 = OtelLog::from_bytes(b"test message 1");
        e_1.set_attribute(FILE_KEY.to_string(), string_value("foo1"));
        e_1.set_attribute(
            event::PARTIAL.to_string(),
            AnyValue {
                value: Some(OtelValueKind::BoolValue(true)),
            },
        );

        let mut e_2 = OtelLog::from_bytes(b"test message 2");
        e_2.set_attribute(FILE_KEY.to_string(), string_value("foo1"));

        let input_stream = futures::stream::iter([Event::from(e_1), Event::from(e_2)]);
        let output_stream = merge_partial_events(input_stream, None);

        let output: Vec<Event> = output_stream.collect().await;
        assert_eq!(output.len(), 1);
        assert_eq!(
            output[0].as_log().body_string(),
            "test message 1test message 2"
        );
    }
}
