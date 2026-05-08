use std::{collections::HashMap, ops::Range};

use serde_json::Value;

use super::tests::*;
use crate::{
    config::{SinkConfig, SinkContext},
    event::{Event, MetricView},
    sinks::{
        influxdb::test_util::{cleanup_v1, format_timestamp, onboarding_v1, query_v1},
        prometheus::remote_write::config::RemoteWriteConfig,
    },
    test_util::components::{HTTP_SINK_TAGS, assert_sink_compliance},
    tls::{self, TlsConfig},
};

const HTTP_URL: &str = "http://influxdb-v1:8086";
const HTTPS_URL: &str = "https://influxdb-v1-tls:8087";

#[tokio::test]
async fn insert_metrics_over_http() {
    insert_metrics(HTTP_URL).await;
}

#[tokio::test]
async fn insert_metrics_over_https() {
    insert_metrics(HTTPS_URL).await;
}

async fn insert_metrics(url: &str) {
    assert_sink_compliance(&HTTP_SINK_TAGS, async {
        let database = onboarding_v1(url).await;

        let cx = SinkContext::default();

        let config = RemoteWriteConfig {
            endpoint: format!("{url}/api/v1/prom/write?db={database}"),
            tls: Some(TlsConfig {
                ca_file: Some(tls::TEST_PEM_CA_PATH.into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let events = create_events(0..5, |n| n * 11.0);

        let (sink, _) = config.build(cx).await.expect("error building config");
        sink.run_events(events.clone()).await.unwrap();

        let result = query(url, &format!("show series on {database}")).await;

        let values = &result["results"][0]["series"][0]["values"];
        assert_eq!(values.as_array().unwrap().len(), 5);

        for event in events {
            let metric = event.into_otel_metric();
            let result = query(
                url,
                &format!(r#"SELECT * FROM "{}".."{}""#, database, metric.name()),
            )
            .await;

            let metrics = decode_metrics(&result["results"][0]["series"][0]);
            assert_eq!(metrics.len(), 1);
            let output = &metrics[0];

            match metric.view() {
                MetricView::Gauge { value } => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "test gauge value fits in u32"
                    )]
                    #[expect(clippy::cast_sign_loss, reason = "test gauge value is non-negative")]
                    let int_val = value as u32;
                    assert_eq!(output["value"], Value::Number(int_val.into()))
                }
                _ => panic!("Unhandled metric value, fix the test"),
            }
            let tags = metric.tags().unwrap();
            for (tag, value) in tags.iter() {
                if let Some(
                    opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                ) = &value.value
                {
                    assert_eq!(output[tag.as_str()], Value::String(s.clone()));
                }
            }
            let timestamp =
                format_timestamp(metric.timestamp().unwrap(), chrono::SecondsFormat::Millis);
            assert_eq!(output["time"], Value::String(timestamp));
        }

        cleanup_v1(url, &database).await;
    })
    .await
}

async fn query(url: &str, query: &str) -> Value {
    let result = query_v1(url, query).await;
    let text = result.text().await.unwrap();
    serde_json::from_str(&text).expect("error when parsing InfluxDB response JSON")
}

fn decode_metrics(data: &Value) -> Vec<HashMap<String, Value>> {
    let data = data.as_object().expect("Data is not an object");
    let columns = data["columns"].as_array().expect("Columns is not an array");
    data["values"]
        .as_array()
        .expect("Values is not an array")
        .iter()
        .map(|values| {
            columns
                .iter()
                .zip(values.as_array().unwrap().iter())
                .map(|(column, value)| (column.as_str().unwrap().to_owned(), value.clone()))
                .collect()
        })
        .collect()
}

fn create_events(name_range: Range<i32>, value: impl Fn(f64) -> f64) -> Vec<Event> {
    name_range
        .map(move |num| create_event(format!("metric_{num}"), value(f64::from(num))))
        .collect()
}
