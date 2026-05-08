use chrono::{Timelike, Utc, offset::TimeZone};
use rand::seq::SliceRandom;
use sol_lib::otel_tags;

use super::*;
use crate::{
    event::{Event, MetricKind, OtelMetric},
    test_util::{
        components::{AWS_SINK_TAGS, run_and_assert_sink_compliance},
        random_string,
    },
};

fn cloudwatch_address() -> String {
    std::env::var("CLOUDWATCH_ADDRESS").unwrap_or_else(|_| "http://localhost:4566".into())
}

fn config() -> CloudWatchMetricsSinkConfig {
    CloudWatchMetricsSinkConfig {
        default_namespace: "vector".into(),
        region: RegionOrEndpoint::with_both("us-east-1", cloudwatch_address().as_str()),
        ..Default::default()
    }
}

#[tokio::test]
async fn cloudwatch_metrics_healthcheck() {
    let config = config();
    let client = config
        .create_client(&ProxyConfig::from_env())
        .await
        .unwrap();
    config.healthcheck(client).await.unwrap();
}

#[tokio::test]
async fn cloudwatch_metrics_put_data() {
    let cx = SinkContext::default();
    let config = config();
    let client = config.create_client(&cx.globals.proxy).await.unwrap();
    let sink = CloudWatchMetricsSvc::new(config, client).unwrap();

    let mut events = Vec::new();

    for i in 0..100 {
        let event = Event::Metric(
            OtelMetric::new_counter(
                format!("counter-{}", 0),
                MetricKind::Incremental,
                f64::from(i),
            )
            .with_tags(Some(otel_tags!(
                "region" => "us-west-1",
                "production" => "true",
                "e" => "",
            ))),
        );
        events.push(event);
    }

    let gauge_name = random_string(10);
    for i in 0..10 {
        let event = Event::Metric(OtelMetric::new_gauge(
            format!("gauge-{gauge_name}"),
            f64::from(i),
        ));
        events.push(event);
    }

    let distribution_name = random_string(10);
    for i in 0..10 {
        let samples = sol_lib::samples![f64::from(i) => 100];
        let event = Event::Metric(
            OtelMetric::new_histogram_from_samples(
                format!("distribution-{distribution_name}"),
                MetricKind::Incremental,
                &samples,
            )
            .with_timestamp(Some(
                Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                    .single()
                    .and_then(|t| t.with_nanosecond(123456789))
                    .expect("invalid timestamp"),
            )),
        );
        events.push(event);
    }

    run_and_assert_sink_compliance(sink, stream::iter(events), &AWS_SINK_TAGS).await;
}

#[tokio::test]
async fn cloudwatch_metrics_namespace_partitioning() {
    let cx = SinkContext::default();
    let config = config();
    let client = config.create_client(&cx.globals.proxy).await.unwrap();
    let sink = CloudWatchMetricsSvc::new(config, client).unwrap();

    let mut events = Vec::new();

    for namespace in ["ns1", "ns2", "ns3", "ns4"].iter() {
        for _ in 0..100 {
            let event = Event::Metric(
                OtelMetric::new_counter("counter", MetricKind::Incremental, 1.0)
                    .with_namespace(Some(*namespace)),
            );
            events.push(event);
        }
    }

    events.shuffle(&mut rand::rng());

    run_and_assert_sink_compliance(sink, stream::iter(events), &AWS_SINK_TAGS).await;
}
