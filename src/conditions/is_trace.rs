use crate::event::Event;

pub(crate) const fn check_is_trace(e: Event) -> (bool, Event) {
    (matches!(e, Event::Trace(_)), e)
}

pub(crate) fn check_is_trace_with_context(e: Event) -> (Result<(), String>, Event) {
    let (result, event) = check_is_trace(e);
    if result {
        (Ok(()), event)
    } else {
        (Err("event is not a trace type".to_string()), event)
    }
}

#[cfg(test)]
mod test {
    use super::check_is_trace;
    use crate::event::{Event, EventMetadata, OtelMetric, OtelSpan, metric::MetricKind};
    use opentelemetry_proto::tonic::trace::v1::Span;
    use vrl::value::Value;

    #[test]
    fn is_trace_basic() {
        assert!(
            check_is_trace(Event::Trace(OtelSpan::from_value_map(
                Value::from("just a trace"),
                EventMetadata::default(),
            )))
            .0
        );
        assert!(
            !check_is_trace(Event::Metric(OtelMetric::new_counter(
                "test metric",
                MetricKind::Incremental,
                1.0
            )))
            .0,
        );
    }

    #[test]
    fn is_trace_matches_otel_span() {
        let event = Event::Trace(OtelSpan::new(Span {
            name: "my-span".to_string(),
            ..Default::default()
        }));
        assert!(check_is_trace(event).0);
    }
}
