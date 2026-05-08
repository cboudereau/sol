use aws_smithy_types::DateTime;
use chrono::{Timelike, Utc, offset::TimeZone};
use similar_asserts::assert_eq;
use sol_lib::otel_tags;

use super::*;
use crate::event::OtelMetric;
use crate::event::metric::MetricKind;

fn timestamp(time: &str) -> DateTime {
    DateTime::from_millis(
        chrono::DateTime::parse_from_rfc3339(time)
            .unwrap()
            .timestamp_millis(),
    )
}

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<CloudWatchMetricsSinkConfig>();
}

fn config() -> CloudWatchMetricsSinkConfig {
    CloudWatchMetricsSinkConfig {
        default_namespace: "vector".into(),
        region: RegionOrEndpoint::with_region("us-east-1".to_owned()),
        storage_resolution: IndexMap::from([("bytes_out".to_owned(), 1)]),
        ..Default::default()
    }
}

async fn svc() -> CloudWatchMetricsSvc {
    let config = config();
    let client = config
        .create_client(&ProxyConfig::from_env())
        .await
        .unwrap();
    CloudWatchMetricsSvc {
        client,
        storage_resolution: config.storage_resolution,
    }
}

#[tokio::test]
async fn encode_events_basic_counter() {
    let events: Vec<OtelMetric> = vec![
        OtelMetric::new_counter("exception_total", MetricKind::Incremental, 1.0),
        OtelMetric::new_counter("bytes_out", MetricKind::Incremental, 2.5).with_timestamp(Some(
            Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                .single()
                .and_then(|t| t.with_nanosecond(123456789))
                .expect("invalid timestamp"),
        )),
        OtelMetric::new_counter("healthcheck", MetricKind::Incremental, 1.0)
            .with_tags(Some(otel_tags!("region" => "local")))
            .with_timestamp(Some(
                Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                    .single()
                    .and_then(|t| t.with_nanosecond(123456789))
                    .expect("invalid timestamp"),
            )),
    ];

    assert_eq!(
        svc().await.encode_events(events),
        vec![
            MetricDatum::builder()
                .metric_name("exception_total")
                .value(1.0)
                .build(),
            MetricDatum::builder()
                .metric_name("bytes_out")
                .value(2.5)
                .timestamp(timestamp("2018-11-14T08:09:10.123Z"))
                .storage_resolution(1)
                .build(),
            MetricDatum::builder()
                .metric_name("healthcheck")
                .value(1.0)
                .timestamp(timestamp("2018-11-14T08:09:10.123Z"))
                .dimensions(Dimension::builder().name("region").value("local").build())
                .build()
        ]
    );
}

#[tokio::test]
async fn encode_events_absolute_gauge() {
    let events: Vec<OtelMetric> = vec![OtelMetric::new_gauge("temperature", 10.0)];

    assert_eq!(
        svc().await.encode_events(events),
        vec![
            MetricDatum::builder()
                .metric_name("temperature")
                .value(10.0)
                .build()
        ]
    );
}

#[tokio::test]
async fn encode_events_distribution() {
    let samples = sol_lib::samples![11.0 => 100, 12.0 => 50];
    let events: Vec<OtelMetric> = vec![OtelMetric::new_histogram_from_samples(
        "latency",
        MetricKind::Incremental,
        &samples,
    )];

    assert_eq!(
        svc().await.encode_events(events),
        vec![
            MetricDatum::builder()
                .metric_name("latency")
                .set_values(Some(vec![11.0, 12.0]))
                .set_counts(Some(vec![100.0, 50.0]))
                .build()
        ]
    );
}

#[tokio::test]
async fn encode_events_set() {
    let events: Vec<OtelMetric> = vec![OtelMetric::new_set_from_values(
        "users",
        MetricKind::Incremental,
        vec!["alice", "bob"],
    )];

    assert_eq!(
        svc().await.encode_events(events),
        vec![
            MetricDatum::builder()
                .metric_name("users")
                .value(2.0)
                .build()
        ]
    );
}
