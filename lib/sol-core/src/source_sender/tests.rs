use chrono::{DateTime, Duration, Utc};
use rand::{Rng, rng};
use std::time::{Duration as StdDuration, Instant};
use tokio::time::timeout;
use vrl::event_path;

use super::*;
use crate::{
    config::{DataType, SourceOutput},
    event::{Event, MetricView, OtelLog, OtelMetric, OtelSpan},
    metrics::{self, Controller},
};

#[tokio::test]
async fn emits_lag_time_for_log() {
    emit_and_test(|timestamp| {
        let mut log = OtelLog::from("Log message");
        log.set_timestamp(timestamp);
        Event::Log(log)
    })
    .await;
}

#[tokio::test]
async fn emits_lag_time_for_metric() {
    emit_and_test(|timestamp| {
        Event::Metric(OtelMetric::new_gauge("name", 123.4).with_timestamp(Some(timestamp)))
    })
    .await;
}

#[tokio::test]
async fn emits_lag_time_for_trace() {
    emit_and_test(|timestamp| {
        let mut trace = OtelSpan::new(Default::default());
        trace.insert(event_path!("start_time"), timestamp);
        Event::Trace(trace)
    })
    .await;
}

async fn emit_and_test(make_event: impl FnOnce(DateTime<Utc>) -> Event) {
    metrics::init_test();
    let (mut sender, _stream) = SourceSender::new_test();
    let millis = rng().random_range(10..10000);
    let timestamp = Utc::now() - Duration::milliseconds(millis);
    #[expect(clippy::cast_precision_loss)]
    let expected = millis as f64 / 1000.0;

    let event = make_event(timestamp);
    sender
        .send_event(event)
        .await
        .expect("Send should not fail");

    let lag_times = Controller::get()
        .expect("There must be a controller")
        .capture_metrics()
        .into_iter()
        .filter(|metric| metric.name() == "sol_source_lag_time_seconds")
        .collect::<Vec<_>>();
    assert_eq!(lag_times.len(), 1);

    let lag_time = &lag_times[0];
    match lag_time.view() {
        MetricView::Histogram {
            bounds,
            counts,
            count,
            sum,
        } => {
            let mut done = false;
            for (&upper_limit, &bucket_count) in bounds.iter().zip(counts.iter()) {
                if !done && upper_limit >= expected {
                    assert_eq!(bucket_count, 1);
                    done = true;
                } else {
                    assert_eq!(bucket_count, 0);
                }
            }
            assert_eq!(count, 1);
            assert!(
                (sum - expected).abs() <= 0.010,
                "Histogram sum does not match expected sum: {sum} vs {expected}",
            );
        }
        _ => panic!("source_lag_time_seconds has invalid type"),
    }
}

#[tokio::test]
async fn emits_component_discarded_events_total_for_send_event() {
    metrics::init_test();
    let (mut sender, _recv) = SourceSender::new_test_sender_with_options(1, None);

    let event = Event::Metric(OtelMetric::new_gauge("name", 123.4));

    // First send will succeed.
    sender
        .send_event(event.clone())
        .await
        .expect("First send should not fail");

    // Second send will timeout, so the future will not be polled to completion.
    let res = timeout(
        std::time::Duration::from_millis(100),
        sender.send_event(event.clone()),
    )
    .await;
    assert!(res.is_err(), "Send should have timed out.");

    let component_discarded_events_total = Controller::get()
        .expect("There must be a controller")
        .capture_metrics()
        .into_iter()
        .filter(|metric| metric.name() == "sol_component_discarded_events_total")
        .collect::<Vec<_>>();
    assert_eq!(component_discarded_events_total.len(), 1);

    let component_discarded_events_total = &component_discarded_events_total[0];
    let MetricView::Sum { value } = component_discarded_events_total.view() else {
        panic!("component_discarded_events_total has invalid type")
    };
    assert_eq!(value, 1.0);
}

#[tokio::test]
#[expect(clippy::cast_precision_loss)]
async fn emits_component_discarded_events_total_for_send_batch() {
    metrics::init_test();
    let (mut sender, _recv) = SourceSender::new_test_sender_with_options(1, None);

    let expected_drop = 100;
    let events: Vec<Event> = (0..(CHUNK_SIZE + expected_drop))
        .map(|_| Event::Metric(OtelMetric::new_gauge("name", 123.4)))
        .collect();

    // `CHUNK_SIZE` events will be sent into buffer but then the future will not be polled to completion.
    let res = timeout(
        std::time::Duration::from_millis(100),
        sender.send_batch(events),
    )
    .await;
    assert!(res.is_err(), "Send should have timed out.");

    let metrics = get_component_metrics();
    assert_no_metric(&metrics, "sol_component_timed_out_events_total");
    assert_no_metric(&metrics, "sol_component_timed_out_requests_total");
    assert_counter_metric(
        &metrics,
        "sol_component_discarded_events_total",
        expected_drop as f64,
    );
}

#[tokio::test]
async fn times_out_send_event_with_timeout() {
    metrics::init_test();

    let timeout_duration = StdDuration::from_millis(10);
    let (mut sender, _recv) = SourceSender::new_test_sender_with_options(1, Some(timeout_duration));

    let event = Event::Metric(OtelMetric::new_gauge("name", 123.4));

    sender
        .send_event(event.clone())
        .await
        .expect("First send should succeed");

    let start = Instant::now();
    let result = sender.send_event(event).await;
    let elapsed = start.elapsed();

    assert!(
        matches!(result, Err(SendError::Timeout)),
        "Send should return a timeout error."
    );
    assert!(
        elapsed >= timeout_duration,
        "Send did not wait for the configured timeout"
    );
    assert!(elapsed <= timeout_duration * 2, "Send waited too long");

    let metrics = get_component_metrics();
    assert_no_metric(&metrics, "sol_component_discarded_events_total");
    assert_counter_metric(&metrics, "sol_component_timed_out_events_total", 1.0);
    assert_counter_metric(&metrics, "sol_component_timed_out_requests_total", 1.0);
}

/// Verifies that backpressure on one signal type (metrics) does not block
/// other signal types (logs, traces) from flowing.
///
/// This guards the invariant provided by `SourceSender::named_outputs`:
/// each signal type has an independent channel so a slow consumer on one
/// cannot stall producers on another.
#[tokio::test]
async fn per_signal_backpressure_isolation() {
    const BUF: usize = 1;
    metrics::init_test();

    let component_key = "test-source".into();

    // Build a sender with three named outputs (capacity 1 each).
    let mut builder = SourceSender::builder().with_buffer(BUF);

    let mut log_rx = builder.add_source_output(
        SourceOutput {
            port: Some("logs".to_string()),
            ty: DataType::Log,
            schema_definition: None,
        },
        component_key,
    );
    let mut metric_rx = builder.add_source_output(
        SourceOutput {
            port: Some("metrics".to_string()),
            ty: DataType::Metric,
            schema_definition: None,
        },
        "test-source".into(),
    );
    let mut trace_rx = builder.add_source_output(
        SourceOutput {
            port: Some("traces".to_string()),
            ty: DataType::Trace,
            schema_definition: None,
        },
        "test-source".into(),
    );

    let mut sender = builder.build();

    // Fill the metrics channel to capacity (1 item).
    sender
        .send_batch_named(
            "metrics",
            vec![Event::Metric(OtelMetric::new_gauge("fill", 0.0))],
        )
        .await
        .expect("first metric send should succeed");

    // With the metrics channel full, logs and traces must still be sendable
    // without blocking.  We give each a 200 ms budget.
    let log_future = sender.send_batch_named("logs", vec![Event::Log(OtelLog::from("hello"))]);
    timeout(StdDuration::from_millis(200), log_future)
        .await
        .expect("log send must not block when metric channel is full")
        .expect("log send must succeed");

    let mut trace = OtelSpan::new(Default::default());
    trace.insert(event_path!("msg"), "hi");
    let trace_future = sender.send_batch_named("traces", vec![Event::Trace(trace)]);
    timeout(StdDuration::from_millis(200), trace_future)
        .await
        .expect("trace send must not block when metric channel is full")
        .expect("trace send must succeed");

    // Drain all channels so nothing leaks into other tests.
    assert!(log_rx.next().await.is_some(), "log item must be receivable");
    assert!(
        trace_rx.next().await.is_some(),
        "trace item must be receivable"
    );
    assert!(
        metric_rx.next().await.is_some(),
        "metric item must be receivable"
    );
}

fn get_component_metrics() -> Vec<OtelMetric> {
    Controller::get()
        .expect("There must be a controller")
        .capture_metrics()
        .into_iter()
        .filter(|metric| metric.name().starts_with("sol_component_"))
        .collect()
}

fn assert_no_metric(metrics: &[OtelMetric], name: &str) {
    assert!(
        !metrics.iter().any(|metric| metric.name() == name),
        "Metric {name} should not be present"
    );
}

fn assert_counter_metric(metrics: &[OtelMetric], name: &str, expected: f64) {
    let mut filter = metrics.iter().filter(|metric| metric.name() == name);
    let Some(metric) = filter.next() else {
        panic!("Metric {name} should be present");
    };
    let MetricView::Sum { value } = metric.view() else {
        panic!("Metric {name} should be a counter");
    };
    assert_eq!(value, expected);
    assert!(
        filter.next().is_none(),
        "Only one {name} metric should be present"
    );
}

#[tokio::test]
async fn emits_buffer_utilization_histogram_on_send_and_receive() {
    const BUFFER_SIZE: usize = 2;

    metrics::init_test();
    let (mut sender, mut recv) = SourceSender::new_test_sender_with_options(BUFFER_SIZE, None);

    let event = Event::Log(OtelLog::from("test event"));
    sender
        .send_event(event.clone())
        .await
        .expect("first send succeeds");
    sender
        .send_event(event)
        .await
        .expect("second send succeeds");

    assert_buffer_metrics(BUFFER_SIZE, 2);

    // Drain the channel so both the send and receive paths are exercised.
    assert!(recv.next().await.is_some());
    assert!(recv.next().await.is_some());

    assert_buffer_metrics(BUFFER_SIZE, 0);
}

#[expect(clippy::cast_precision_loss)]
fn assert_buffer_metrics(buffer_size: usize, level: usize) {
    let metrics: Vec<_> = Controller::get()
        .expect("metrics controller available")
        .capture_metrics()
        .into_iter()
        .filter(|metric| metric.name().starts_with("sol_source_buffer_"))
        .collect();
    assert_eq!(metrics.len(), 5, "expected 5 utilization metrics");

    let find_metric = |name: &str| {
        metrics
            .iter()
            .find(|m| m.name() == name)
            .unwrap_or_else(|| panic!("missing metric: {name}"))
    };

    let metric = find_metric("sol_source_buffer_utilization");
    let tags = metric.tags().expect("utilization histogram has tags");
    assert_eq!(tags.get_string("output"), Some("_default"));

    let metric = find_metric("sol_source_buffer_utilization_level");
    let MetricView::Gauge { value } = metric.view() else {
        panic!("source_buffer_utilization_level should be a gauge");
    };
    assert_eq!(value, level as f64);

    let metric = find_metric("sol_source_buffer_max_event_size");
    let MetricView::Gauge { value } = metric.view() else {
        panic!("source_buffer_max_event_size should be a gauge");
    };
    assert_eq!(value, buffer_size as f64);

    let metric = find_metric("sol_source_buffer_max_size_events");
    let MetricView::Gauge { value } = metric.view() else {
        panic!("source_buffer_max_size_events should be a gauge");
    };
    assert_eq!(value, buffer_size as f64);
}
