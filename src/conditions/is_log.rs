use crate::event::Event;

pub(crate) const fn check_is_log(e: Event) -> (bool, Event) {
    (matches!(e, Event::Log(_)), e)
}

pub(crate) fn check_is_log_with_context(e: Event) -> (Result<(), String>, Event) {
    let (result, event) = check_is_log(e);
    if result {
        (Ok(()), event)
    } else {
        (Err("event is not a log type".to_string()), event)
    }
}

#[cfg(test)]
mod test {
    use super::check_is_log;
    use crate::event::{Event, OtelLog, OtelMetric, metric::MetricKind};
    use opentelemetry_proto::tonic::logs::v1::LogRecord;

    #[test]
    fn is_log_basic() {
        assert!(check_is_log(Event::Log(OtelLog::from("just a log"))).0);
        assert!(
            !check_is_log(Event::Metric(OtelMetric::new_counter(
                "test metric",
                MetricKind::Incremental,
                1.0
            )))
            .0,
        );
    }

    #[test]
    fn is_log_matches_otel_log() {
        let event = Event::Log(OtelLog::new(LogRecord {
            severity_text: "INFO".to_string(),
            ..Default::default()
        }));
        assert!(check_is_log(event).0);
    }
}
