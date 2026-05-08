use std::collections::BTreeMap;

use chrono::Utc;
use sol_lib::{
    TimeZone,
    codecs::MetricTagValues,
    configurable::configurable_component,
    lookup::{owned_value_path, path},
};
use vrl::value::{Kind, kind::Collection};

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    event::{self, Event, OtelLog, OtelMetric},
    schema::Definition,
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

/// Configuration for the `metric_to_log` transform.
#[configurable_component(transform("metric_to_log", "Convert metric events to log events."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct MetricToLogConfig {
    /// Name of the tag in the metric to use for the source host.
    ///
    /// If present, the value of the tag is set on the generated log event in the `host` field,
    /// where the field key defaults to `host`.
    #[configurable(metadata(docs::examples = "host", docs::examples = "hostname"))]
    pub host_tag: Option<String>,

    /// The name of the time zone to apply to timestamp conversions that do not contain an explicit
    /// time zone.
    ///
    /// This overrides the [global `timezone`][global_timezone] option. The time zone name may be
    /// any name in the [TZ database][tz_database] or `local` to indicate system local time.
    ///
    /// [global_timezone]: https://vector.dev/docs/reference/configuration//global-options#timezone
    /// [tz_database]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    pub timezone: Option<TimeZone>,

    /// Controls how metric tag values are encoded.
    ///
    /// When set to `single`, only the last non-bare value of tags is displayed with the
    /// metric.  When set to `full`, all metric tags are exposed as separate assignments as
    /// described by [the `native_json` codec][vector_native_json].
    ///
    /// [vector_native_json]: https://github.com/vectordotdev/vector/blob/master/lib/codecs/
    #[serde(default)]
    pub metric_tag_values: MetricTagValues,
}

impl MetricToLogConfig {
    pub fn build_transform(&self, context: &TransformContext) -> MetricToLog {
        MetricToLog::new(
            self.host_tag.as_deref(),
            self.timezone.unwrap_or_else(|| context.globals.timezone()),
            self.metric_tag_values,
        )
    }
}

impl GenerateConfig for MetricToLogConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            host_tag: Some("host-tag".to_string()),
            timezone: None,
            metric_tag_values: MetricTagValues::Single,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "metric_to_log")]
impl TransformConfig for MetricToLogConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(self.build_transform(context)))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn outputs(
        &self,
        _context: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        let schema_definition = schema_definition();

        vec![TransformOutput::new(
            DataType::Log,
            input_definitions
                .iter()
                .map(|(output, _)| (output.clone(), schema_definition.clone()))
                .collect(),
        )]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

fn schema_definition() -> Definition {
    let mut schema_definition = Definition::default_definition()
        .with_event_field(&owned_value_path!("name"), Kind::bytes(), None)
        .with_event_field(
            &owned_value_path!("description"),
            Kind::bytes().or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("unit"),
            Kind::bytes().or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("sum"),
            Kind::any_object().or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("gauge"),
            Kind::any_object().or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("histogram"),
            Kind::any_object().or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("summary"),
            Kind::any_object().or_undefined(),
            None,
        )
        .with_event_field(
            &owned_value_path!("exponentialHistogram"),
            Kind::any_object().or_undefined(),
            None,
        );

    schema_definition = schema_definition.with_metadata_field(
        &owned_value_path!("vector"),
        Kind::object(Collection::empty()),
        None,
    );
    schema_definition
}

#[derive(Clone, Debug)]
pub struct MetricToLog {
    host_tag_key: Option<String>,
    #[allow(dead_code)]
    timezone: TimeZone,
    tag_values: MetricTagValues,
}

impl MetricToLog {
    pub fn new(host_tag: Option<&str>, timezone: TimeZone, tag_values: MetricTagValues) -> Self {
        Self {
            host_tag_key: Some(host_tag.unwrap_or("host").to_string()),
            timezone,
            tag_values,
        }
    }

    pub fn transform_one(&self, mut otel: OtelMetric) -> Option<OtelLog> {
        if self.tag_values == MetricTagValues::Single {
            otel.reduce_tags_to_single();
        }

        let timestamp = otel.timestamp().unwrap_or_else(Utc::now);
        let metadata = otel.metadata().clone();

        let host_value = self.host_tag_key.as_ref().and_then(|key| {
            let val = otel.tag_value(key);
            otel.remove_data_point_attribute(key);
            val
        });

        let body = otel.to_log_body();

        let mut log = OtelLog::new(Default::default());
        *log.metadata_mut() = metadata;
        log.set_body(body);
        log.set_timestamp(timestamp);

        if let Some(resource) = otel.resource_proto() {
            log.set_resource(resource);
        }
        if let Some(scope) = otel.scope_proto() {
            log.set_scope(scope);
        }

        if let Some(host_val) = host_value {
            log.set_host(event::Value::from(host_val));
        }

        log.metadata_mut()
            .value_mut()
            .insert(path!("vector"), vrl::value::Value::Object(BTreeMap::new()));
        Some(log)
    }
}

impl FunctionTransform for MetricToLog {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let otel = match event {
            Event::Metric(otel) => otel,
            other => {
                output.push(other);
                return;
            }
        };
        let retval: Option<Event> = self.transform_one(otel).map(Event::Log);
        output.extend(retval.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, Timelike, Utc, offset::TimeZone};
    use similar_asserts::assert_eq;
    use sol_lib::{config::ComponentKey, event::EventMetadata, otel_tags};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::*;
    use crate::{
        event::{KeyString, OtelAttributes, OtelLog, OtelMetric, Value, metric::MetricKind},
        test_util::components::assert_transform_compliance,
        transforms::test::create_topology,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<MetricToLogConfig>();
    }

    async fn do_transform(metric: OtelMetric) -> Option<OtelLog> {
        assert_transform_compliance(async move {
            let config = MetricToLogConfig {
                host_tag: Some("host".into()),
                timezone: None,
                ..Default::default()
            };
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            tx.send(Event::Metric(metric)).await.unwrap();

            let result = out.recv().await;

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);

            result
        })
        .await
        .map(|e| e.into_log())
    }

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
            .single()
            .and_then(|t| t.with_nanosecond(11))
            .expect("invalid timestamp")
    }

    fn tags() -> OtelAttributes {
        otel_tags! {
            "host" => "localhost",
            "some_tag" => "some_value",
        }
    }

    fn event_metadata() -> EventMetadata {
        EventMetadata::default().with_source_type("unit_test_stream")
    }

    #[tokio::test]
    async fn transform_counter() {
        let counter = OtelMetric::new_counter("counter", MetricKind::Absolute, 1.0)
            .with_metadata(event_metadata())
            .with_tags(Some(tags()))
            .with_timestamp(Some(ts()));
        let mut metadata = counter.metadata().clone();
        metadata.set_source_id(Arc::new(ComponentKey::from("in")));
        metadata.set_upstream_id(Arc::new(OutputId::from("transform")));
        metadata.set_schema_definition(&Arc::new(schema_definition()));
        metadata.value_mut().insert(
            vrl::path!("vector"),
            Value::Object(std::collections::BTreeMap::new()),
        );

        let log = do_transform(counter).await.unwrap();
        let collected: Vec<_> = log.all_event_fields().unwrap();

        assert_eq!(
            collected,
            vec![
                (KeyString::from("body.name"), Value::from("counter")),
                (
                    KeyString::from("body.sum.aggregationTemporality"),
                    Value::Integer(2)
                ),
                (
                    KeyString::from("body.sum.dataPoints[0].asDouble"),
                    Value::from(1.0)
                ),
                (
                    KeyString::from("body.sum.dataPoints[0].attributes[0].key"),
                    Value::from("some_tag")
                ),
                (
                    KeyString::from("body.sum.dataPoints[0].attributes[0].value"),
                    Value::from("some_value")
                ),
                (
                    KeyString::from("body.sum.dataPoints[0].timeUnixNano"),
                    Value::from(ts().timestamp_nanos_opt().unwrap().to_string())
                ),
                (
                    KeyString::from("body.sum.isMonotonic"),
                    Value::Boolean(true)
                ),
                (
                    KeyString::from("resource.\"host.name\""),
                    Value::from("localhost")
                ),
                (
                    KeyString::from("time_unix_nano"),
                    Value::Integer(ts().timestamp_nanos_opt().unwrap() as i64)
                ),
            ]
        );
        assert_eq!(log.metadata(), &metadata);
    }

    #[tokio::test]
    async fn transform_gauge() {
        let gauge = OtelMetric::new_gauge("gauge", 1.0)
            .with_metadata(event_metadata())
            .with_timestamp(Some(ts()));
        let mut metadata = gauge.metadata().clone();
        metadata.set_source_id(Arc::new(ComponentKey::from("in")));
        metadata.set_upstream_id(Arc::new(OutputId::from("transform")));
        metadata.set_schema_definition(&Arc::new(schema_definition()));
        metadata.value_mut().insert(
            vrl::path!("vector"),
            Value::Object(std::collections::BTreeMap::new()),
        );

        let log = do_transform(gauge).await.unwrap();
        let collected: Vec<_> = log.all_event_fields().unwrap();

        assert_eq!(
            collected,
            vec![
                (
                    KeyString::from("body.gauge.dataPoints[0].asDouble"),
                    Value::from(1.0)
                ),
                (
                    KeyString::from("body.gauge.dataPoints[0].timeUnixNano"),
                    Value::from(ts().timestamp_nanos_opt().unwrap().to_string())
                ),
                (KeyString::from("body.name"), Value::from("gauge")),
                (
                    KeyString::from("time_unix_nano"),
                    Value::Integer(ts().timestamp_nanos_opt().unwrap() as i64)
                ),
            ]
        );
        assert_eq!(log.metadata(), &metadata);
    }

    #[tokio::test]
    async fn transform_set() {
        let set = OtelMetric::new_set_from_values(
            "set",
            MetricKind::Absolute,
            vec![String::from("one"), String::from("two")],
        )
        .with_metadata(event_metadata())
        .with_timestamp(Some(ts()));
        let mut metadata = set.metadata().clone();
        metadata.set_source_id(Arc::new(ComponentKey::from("in")));
        metadata.set_upstream_id(Arc::new(OutputId::from("transform")));
        metadata.set_schema_definition(&Arc::new(schema_definition()));
        metadata.value_mut().insert(
            vrl::path!("vector"),
            Value::Object(std::collections::BTreeMap::new()),
        );

        let log = do_transform(set).await.unwrap();
        let collected: Vec<_> = log.all_event_fields().unwrap();

        assert!(collected.iter().any(|(k, _)| k == "body.name"));
        assert_eq!(
            collected.iter().find(|(k, _)| k == "body.name").unwrap().1,
            Value::from("set")
        );
        assert!(collected.iter().any(|(k, _)| k.starts_with("body.gauge.")));
        assert!(collected.iter().any(|(k, _)| k == "time_unix_nano"));
    }

    #[tokio::test]
    async fn transform_distribution() {
        let distro = OtelMetric::new_histogram_from_samples(
            "distro",
            MetricKind::Absolute,
            &sol_lib::samples![1.0 => 10, 2.0 => 20],
        )
        .with_metadata(event_metadata())
        .with_timestamp(Some(ts()));
        let mut metadata = distro.metadata().clone();
        metadata.set_source_id(Arc::new(ComponentKey::from("in")));
        metadata.set_upstream_id(Arc::new(OutputId::from("transform")));
        metadata.set_schema_definition(&Arc::new(schema_definition()));
        metadata.value_mut().insert(
            vrl::path!("vector"),
            Value::Object(std::collections::BTreeMap::new()),
        );

        let log = do_transform(distro).await.unwrap();
        let collected: Vec<_> = log.all_event_fields().unwrap();

        assert!(collected.iter().any(|(k, _)| k == "body.name"));
        assert_eq!(
            collected.iter().find(|(k, _)| k == "body.name").unwrap().1,
            Value::from("distro")
        );
        assert!(
            collected
                .iter()
                .any(|(k, _)| k.starts_with("body.histogram."))
        );
        assert!(collected.iter().any(|(k, _)| k == "time_unix_nano"));
        assert_eq!(log.metadata(), &metadata);
    }

    #[tokio::test]
    async fn transform_histogram() {
        let histo = OtelMetric::new_histogram(
            "histo",
            MetricKind::Absolute,
            &sol_lib::buckets![1.0 => 10, 2.0 => 20],
            30,
            50.0,
        )
        .with_metadata(event_metadata())
        .with_timestamp(Some(ts()));
        let mut metadata = histo.metadata().clone();
        metadata.set_source_id(Arc::new(ComponentKey::from("in")));
        metadata.set_upstream_id(Arc::new(OutputId::from("transform")));
        metadata.set_schema_definition(&Arc::new(schema_definition()));
        metadata.value_mut().insert(
            vrl::path!("vector"),
            Value::Object(std::collections::BTreeMap::new()),
        );

        let log = do_transform(histo).await.unwrap();
        let collected: Vec<_> = log.all_event_fields().unwrap();

        assert_eq!(
            collected,
            vec![
                (
                    KeyString::from("body.histogram.aggregationTemporality"),
                    Value::Integer(2)
                ),
                (
                    KeyString::from("body.histogram.dataPoints[0].bucketCounts[0]"),
                    Value::from("10")
                ),
                (
                    KeyString::from("body.histogram.dataPoints[0].bucketCounts[1]"),
                    Value::from("20")
                ),
                (
                    KeyString::from("body.histogram.dataPoints[0].bucketCounts[2]"),
                    Value::from("0")
                ),
                (
                    KeyString::from("body.histogram.dataPoints[0].count"),
                    Value::from("30")
                ),
                (
                    KeyString::from("body.histogram.dataPoints[0].explicitBounds[0]"),
                    Value::from(1.0)
                ),
                (
                    KeyString::from("body.histogram.dataPoints[0].explicitBounds[1]"),
                    Value::from(2.0)
                ),
                (
                    KeyString::from("body.histogram.dataPoints[0].sum"),
                    Value::from(50.0)
                ),
                (
                    KeyString::from("body.histogram.dataPoints[0].timeUnixNano"),
                    Value::from(ts().timestamp_nanos_opt().unwrap().to_string())
                ),
                (KeyString::from("body.name"), Value::from("histo")),
                (
                    KeyString::from("time_unix_nano"),
                    Value::Integer(ts().timestamp_nanos_opt().unwrap() as i64)
                ),
            ]
        );
        assert_eq!(log.metadata(), &metadata);
    }

    #[tokio::test]
    async fn transform_summary() {
        let summary = OtelMetric::new_summary(
            "summary",
            &sol_lib::quantiles![50.0 => 10.0, 90.0 => 20.0],
            30,
            50.0,
        )
        .with_metadata(event_metadata())
        .with_timestamp(Some(ts()));
        let mut metadata = summary.metadata().clone();
        metadata.set_source_id(Arc::new(ComponentKey::from("in")));
        metadata.set_upstream_id(Arc::new(OutputId::from("transform")));
        metadata.set_schema_definition(&Arc::new(schema_definition()));
        metadata.value_mut().insert(
            vrl::path!("vector"),
            Value::Object(std::collections::BTreeMap::new()),
        );

        let log = do_transform(summary).await.unwrap();
        let collected: Vec<_> = log.all_event_fields().unwrap();

        assert_eq!(
            collected,
            vec![
                (KeyString::from("body.name"), Value::from("summary")),
                (
                    KeyString::from("body.summary.dataPoints[0].count"),
                    Value::from("30")
                ),
                (
                    KeyString::from("body.summary.dataPoints[0].quantileValues[0].quantile"),
                    Value::from(50.0)
                ),
                (
                    KeyString::from("body.summary.dataPoints[0].quantileValues[0].value"),
                    Value::from(10.0)
                ),
                (
                    KeyString::from("body.summary.dataPoints[0].quantileValues[1].quantile"),
                    Value::from(90.0)
                ),
                (
                    KeyString::from("body.summary.dataPoints[0].quantileValues[1].value"),
                    Value::from(20.0)
                ),
                (
                    KeyString::from("body.summary.dataPoints[0].sum"),
                    Value::from(50.0)
                ),
                (
                    KeyString::from("body.summary.dataPoints[0].timeUnixNano"),
                    Value::from(ts().timestamp_nanos_opt().unwrap().to_string())
                ),
                (
                    KeyString::from("time_unix_nano"),
                    Value::Integer(ts().timestamp_nanos_opt().unwrap() as i64)
                ),
            ]
        );
        assert_eq!(log.metadata(), &metadata);
    }

    #[tokio::test]
    async fn transform_tag_single_encoding() {
        let tags = otel_tags! {
            "single" => "value",
        };
        let counter = OtelMetric::new_counter("counter", MetricKind::Absolute, 1.0)
            .with_tags(Some(tags))
            .with_timestamp(Some(ts()));

        let mut output = OutputBuffer::with_capacity(1);
        MetricToLogConfig {
            metric_tag_values: MetricTagValues::Single,
            ..Default::default()
        }
        .build_transform(&TransformContext::default())
        .transform(&mut output, Event::Metric(counter));

        assert_eq!(output.len(), 1);
        let log = output.into_events().next().unwrap().into_log();
        let collected: Vec<_> = log.all_event_fields().unwrap();
        let has_single_key = collected
            .iter()
            .any(|(k, v)| k.starts_with("body.") && *v == Value::from("single"));
        let has_single_val = collected
            .iter()
            .any(|(k, v)| k.starts_with("body.") && *v == Value::from("value"));
        assert!(
            has_single_key,
            "attribute key 'single' not found in OTLP body output"
        );
        assert!(
            has_single_val,
            "attribute value 'value' not found in OTLP body output"
        );
    }

    #[tokio::test]
    async fn transform_tag_full_encoding() {
        let tags = otel_tags! {
            "multi" => "a",
            "multi" => "b",
        };
        let counter = OtelMetric::new_counter("counter", MetricKind::Absolute, 1.0)
            .with_tags(Some(tags))
            .with_timestamp(Some(ts()));

        let mut output = OutputBuffer::with_capacity(1);
        MetricToLogConfig {
            metric_tag_values: MetricTagValues::Full,
            ..Default::default()
        }
        .build_transform(&TransformContext::default())
        .transform(&mut output, Event::Metric(counter));

        assert_eq!(output.len(), 1);
        let log = output.into_events().next().unwrap().into_log();
        let collected: Vec<_> = log.all_event_fields().unwrap();
        let has_multi_key = collected
            .iter()
            .any(|(k, v)| k.starts_with("body.") && *v == Value::from("multi"));
        assert!(
            has_multi_key,
            "attribute key 'multi' not found in OTLP body output"
        );
    }
}
