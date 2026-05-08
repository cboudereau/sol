use crate::event::Event;

pub(crate) const fn check_is_metric(e: Event) -> (bool, Event) {
    (matches!(e, Event::Metric(_)), e)
}

pub(crate) fn check_is_metric_with_context(e: Event) -> (Result<(), String>, Event) {
    let (result, event) = check_is_metric(e);
    if result {
        (Ok(()), event)
    } else {
        (Err("event is not a metric type".to_string()), event)
    }
}

#[cfg(test)]
mod test {
    use super::check_is_metric;
    use crate::event::{Event, OtelLog, OtelMetric, metric::MetricKind};
    use opentelemetry_proto::tonic::metrics::v1::Metric as OtelMetricProto;

    #[test]
    fn is_metric_basic() {
        assert!(!check_is_metric(Event::Log(OtelLog::from("just a log"))).0);
        assert!(
            check_is_metric(Event::Metric(OtelMetric::new_counter(
                "test metric",
                MetricKind::Incremental,
                1.0
            )))
            .0,
        );
    }

    #[test]
    fn is_metric_matches_otel_metric() {
        let event = Event::Metric(OtelMetric::new(OtelMetricProto {
            name: "http.duration".to_string(),
            ..Default::default()
        }));
        assert!(check_is_metric(event).0);
    }
}
