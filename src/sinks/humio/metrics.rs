use async_trait::async_trait;
use futures::StreamExt;
use futures_util::stream::BoxStream;
use indoc::indoc;
use sol_lib::{
    codecs::JsonSerializerConfig,
    configurable::configurable_component,
    lookup::lookup_v2::{ConfigValuePath, OptionalTargetPath, OptionalValuePath},
    sensitive_string::SensitiveString,
    sink::StreamSink,
};

use super::{
    config_host_key,
    logs::{HOST, HumioLogsConfig},
};
use crate::{
    config::{
        AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext, TransformContext,
    },
    event::{Event, EventArray, EventContainer},
    sinks::{
        Healthcheck, VectorSink,
        splunk_hec::common::SplunkHecDefaultBatchSettings,
        util::{BatchConfig, Compression, TowerRequestConfig},
    },
    template::Template,
    tls::TlsConfig,
    transforms::{
        FunctionTransform, OutputBuffer,
        metric_to_log::{MetricToLog, MetricToLogConfig},
    },
};

/// Configuration for the `humio_metrics` sink.
//
// TODO: This sink overlaps almost entirely with the `humio_logs` sink except for the metric-to-log
// transform that it uses to get metrics into the shape of a log before sending to Humio. However,
// due to issues with aliased fields and flattened fields [1] in `serde`, we can't embed the
// `humio_logs` config here.
//
// [1]: https://github.com/serde-rs/serde/issues/1504
#[configurable_component(sink("humio_metrics", "Deliver metric event data to Humio."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct HumioMetricsConfig {
    #[serde(flatten)]
    transform: MetricToLogConfig,

    /// The Humio ingestion token.
    #[configurable(metadata(
        docs::examples = "${HUMIO_TOKEN}",
        docs::examples = "A94A8FE5CCB19BA61C4C08"
    ))]
    token: SensitiveString,

    /// The base URL of the Humio instance.
    ///
    /// The scheme (`http` or `https`) must be specified. No path should be included since the paths defined
    /// by the [`Splunk`][splunk] API are used.
    ///
    /// [splunk]: https://docs.splunk.com/Documentation/Splunk/8.0.0/Data/HECRESTendpoints
    #[serde(alias = "host")]
    #[serde(default = "default_endpoint")]
    #[configurable(metadata(
        docs::examples = "http://127.0.0.1",
        docs::examples = "https://example.com",
    ))]
    pub(super) endpoint: String,

    /// The source of events sent to this sink.
    ///
    /// Typically the filename the metrics originated from. Maps to `@source` in Humio.
    source: Option<Template>,

    /// The type of events sent to this sink. Humio uses this as the name of the parser to use to ingest the data.
    ///
    /// If unset, Humio defaults it to none.
    #[configurable(metadata(
        docs::examples = "json",
        docs::examples = "none",
        docs::examples = "{{ event_type }}"
    ))]
    event_type: Option<Template>,

    /// Overrides the name of the log field used to retrieve the hostname to send to Humio.
    ///
    /// By default, `host` is used if log
    /// events are Legacy namespaced, or the semantic meaning of "host" is used, if defined.
    #[serde(default = "config_host_key")]
    host_key: OptionalValuePath,

    /// Event fields to be added to Humio’s extra fields.
    ///
    /// Can be used to tag events by specifying fields starting with `#`.
    ///
    /// For more information, see [Humio’s Format of Data][humio_data_format].
    ///
    /// [humio_data_format]: https://docs.humio.com/integrations/data-shippers/hec/#format-of-data
    #[serde(default)]
    indexed_fields: Vec<ConfigValuePath>,

    /// Optional name of the repository to ingest into.
    ///
    /// In public-facing APIs, this must (if present) be equal to the repository used to create the ingest token used for authentication.
    ///
    /// In private cluster setups, Humio can be configured to allow these to be different.
    ///
    /// For more information, see [Humio’s Format of Data][humio_data_format].
    ///
    /// [humio_data_format]: https://docs.humio.com/integrations/data-shippers/hec/#format-of-data
    #[serde(default)]
    #[configurable(metadata(docs::examples = "{{ host }}", docs::examples = "custom_index"))]
    index: Option<Template>,

    #[configurable(derived)]
    #[serde(default)]
    compression: Compression,

    #[configurable(derived)]
    #[serde(default)]
    request: TowerRequestConfig,

    #[configurable(derived)]
    #[serde(default)]
    batch: BatchConfig<SplunkHecDefaultBatchSettings>,

    #[configurable(derived)]
    tls: Option<TlsConfig>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

fn default_endpoint() -> String {
    HOST.to_string()
}

impl GenerateConfig for HumioMetricsConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(indoc! {r#"
                host_key = "hostname"
                token = "${HUMIO_TOKEN}"
            "#})
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "humio_metrics")]
impl SinkConfig for HumioMetricsConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let transform = self
            .transform
            .build_transform(&TransformContext::new_with_globals(cx.globals.clone()));

        let sink = HumioLogsConfig {
            token: self.token.clone(),
            endpoint: self.endpoint.clone(),
            source: self.source.clone(),
            encoding: JsonSerializerConfig::default().into(),
            event_type: self.event_type.clone(),
            host_key: OptionalTargetPath::from(
                vrl::path::PathPrefix::Event,
                self.host_key.path.clone(),
            ),
            indexed_fields: self.indexed_fields.clone(),
            index: self.index.clone(),
            compression: self.compression,
            request: self.request,
            batch: self.batch,
            tls: self.tls.clone(),
            timestamp_nanos_key: None,
            acknowledgements: Default::default(),
            timestamp_key: OptionalTargetPath::none(),
        };

        let (sink, healthcheck) = sink.clone().build(cx).await?;

        let sink = HumioMetricsSink {
            inner: sink,
            transform,
        };

        Ok((VectorSink::Stream(Box::new(sink)), healthcheck))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

pub struct HumioMetricsSink {
    inner: VectorSink,
    transform: MetricToLog,
}

#[async_trait]
impl StreamSink<EventArray> for HumioMetricsSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, EventArray>) -> Result<(), ()> {
        let mut transform = self.transform;
        self.inner
            .run(input.map(move |events| {
                let mut buf = OutputBuffer::with_capacity(events.len());
                for event in events.into_events() {
                    transform.transform(&mut buf, event);
                }
                // Awkward but necessary for the `EventArray` type
                let events = buf.into_events().map(Event::into_log).collect::<Vec<_>>();
                events.into()
            }))
            .await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Utc, offset::TimeZone};
    use futures::stream;
    use indoc::indoc;
    use similar_asserts::assert_eq;
    use sol_lib::otel_tags;

    use super::*;
    use crate::{
        event::{Event, OtelMetric, metric::MetricKind},
        sinks::util::test::{build_test_server, load_sink},
        test_util::{
            self,
            components::{HTTP_SINK_TAGS, run_and_assert_sink_compliance},
        },
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<HumioMetricsConfig>();
    }

    #[test]
    fn test_endpoint_field() {
        let (config, _) = load_sink::<HumioMetricsConfig>(indoc! {r#"
            token = "atoken"
            batch.max_events = 1
            endpoint = "https://localhost:9200/"
        "#})
        .unwrap();

        assert_eq!("https://localhost:9200/".to_string(), config.endpoint);
        let (config, _) = load_sink::<HumioMetricsConfig>(indoc! {r#"
            token = "atoken"
            batch.max_events = 1
            host = "https://localhost:9200/"
        "#})
        .unwrap();

        assert_eq!("https://localhost:9200/".to_string(), config.endpoint);
    }

    #[tokio::test]
    async fn smoke_json() {
        let (mut config, cx) = load_sink::<HumioMetricsConfig>(indoc! {r#"
            token = "atoken"
            batch.max_events = 1
        "#})
        .unwrap();

        let (_guard, addr) = test_util::addr::next_addr();
        // Swap out the endpoint so we can force send it
        // to our local server
        config.endpoint = format!("http://{addr}");

        let (sink, _) = config.build(cx).await.unwrap();

        let (rx, _trigger, server) = build_test_server(addr);
        tokio::spawn(server);

        // Make our test metrics.
        let samples = sol_lib::samples![1.0 => 100, 2.0 => 200, 3.0 => 300];
        let metrics = vec![
            Event::Metric(
                OtelMetric::new_counter("metric1", MetricKind::Incremental, 42.0)
                    .with_tags(Some(otel_tags!("os.host" => "somehost")))
                    .with_timestamp(Some(
                        Utc.with_ymd_and_hms(2020, 8, 18, 21, 0, 1)
                            .single()
                            .expect("invalid timestamp"),
                    )),
            ),
            Event::Metric(
                OtelMetric::new_histogram_from_samples("metric2", MetricKind::Absolute, &samples)
                    .with_tags(Some(otel_tags!("os.host" => "somehost")))
                    .with_timestamp(Some(
                        Utc.with_ymd_and_hms(2020, 8, 18, 21, 0, 2)
                            .single()
                            .expect("invalid timestamp"),
                    )),
            ),
        ];

        let len = metrics.len();
        run_and_assert_sink_compliance(sink, stream::iter(metrics), &HTTP_SINK_TAGS).await;

        let output = rx.take(len).collect::<Vec<_>>().await;
        let s0 = std::str::from_utf8(&output[0].1).unwrap();
        let s1 = std::str::from_utf8(&output[1].1).unwrap();
        let m1: serde_json::Value = serde_json::from_str(s0).expect("valid JSON");
        // metric_to_log puts the full metric as the OtelLog body (KvlistValue)
        let find_body_kv = |event: &serde_json::Value, name: &str| -> serde_json::Value {
            event["body"]["kvlistValue"]["values"]
                .as_array()
                .and_then(|arr| {
                    arr.iter().find_map(|a| {
                        if a["key"].as_str() == Some(name) {
                            Some(a["value"].clone())
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or(serde_json::Value::Null)
        };
        assert_eq!(
            find_body_kv(&m1["event"], "name"),
            serde_json::json!({"stringValue": "metric1"})
        );
        assert!(
            !find_body_kv(&m1["event"], "sum").is_null(),
            "counter should be in body as OTLP sum"
        );
        assert_eq!(m1["time"], 1597784401.0);

        let m2: serde_json::Value = serde_json::from_str(s1).expect("valid JSON");
        assert_eq!(
            find_body_kv(&m2["event"], "name"),
            serde_json::json!({"stringValue": "metric2"})
        );
        assert!(
            !find_body_kv(&m2["event"], "histogram").is_null(),
            "distribution should be in body as OTLP histogram"
        );
        assert_eq!(m2["time"], 1597784402.0);
    }

    #[tokio::test]
    async fn multi_value_tags() {
        let (mut config, cx) = load_sink::<HumioMetricsConfig>(indoc! {r#"
            token = "atoken"
            batch.max_events = 1
            metric_tag_values = "full"
        "#})
        .unwrap();

        let (_guard, addr) = test_util::addr::next_addr();
        // Swap out the endpoint so we can force send it
        // to our local server
        config.endpoint = format!("http://{addr}");

        let (sink, _) = config.build(cx).await.unwrap();

        let (rx, _trigger, server) = build_test_server(addr);
        tokio::spawn(server);

        // Make our test metrics.
        let metrics = vec![Event::Metric(
            OtelMetric::new_counter("metric1", MetricKind::Incremental, 42.0)
                .with_tags(Some(otel_tags!(
                    "code" => "200",
                    "code" => "success"
                )))
                .with_timestamp(Some(
                    Utc.with_ymd_and_hms(2020, 8, 18, 21, 0, 1)
                        .single()
                        .expect("invalid timestamp"),
                )),
        )];

        let len = metrics.len();
        run_and_assert_sink_compliance(sink, stream::iter(metrics), &HTTP_SINK_TAGS).await;

        let output = rx.take(len).collect::<Vec<_>>().await;
        let s = std::str::from_utf8(&output[0].1).unwrap();
        let m: serde_json::Value = serde_json::from_str(s).expect("valid JSON");
        // metric_to_log puts the full metric as the OtelLog body (KvlistValue)
        let find_body_kv = |event: &serde_json::Value, name: &str| -> serde_json::Value {
            event["body"]["kvlistValue"]["values"]
                .as_array()
                .and_then(|arr| {
                    arr.iter().find_map(|a| {
                        if a["key"].as_str() == Some(name) {
                            Some(a["value"].clone())
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or(serde_json::Value::Null)
        };
        assert_eq!(
            find_body_kv(&m["event"], "name"),
            serde_json::json!({"stringValue": "metric1"})
        );
        assert!(
            !find_body_kv(&m["event"], "sum").is_null(),
            "counter should be in body as OTLP sum"
        );
        assert_eq!(m["time"], 1597784401.0);
        let sum_val = find_body_kv(&m["event"], "sum");
        assert!(!sum_val.is_null(), "sum body key should exist");
    }
}
