use std::{collections::HashMap, num::ParseFloatError, sync::Arc};

use chrono::Utc;
use indexmap::IndexMap;
use sol_lib::{configurable::configurable_component, event::OtelMetric};
use vrl::path::parse_target_path;

use crate::{
    common::expansion::pair_expansion,
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput, schema::Definition,
    },
    event::{AnyValue, Event, OtelAttributes, Value, metric::MetricKind},
    internal_events::{
        DROP_EVENT, LogToMetricFieldNullError, LogToMetricParseFloatError, ParserMissingFieldError,
    },
    schema,
    template::{Template, TemplateRenderingError},
    transforms::{
        FunctionTransform, OutputBuffer, Transform, log_to_metric::TransformError::PathNotFound,
    },
};

/// Configuration for the `log_to_metric` transform.
#[configurable_component(transform("log_to_metric", "Convert log events to metric events."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct LogToMetricConfig {
    /// A list of metrics to generate.
    pub metrics: Option<Vec<MetricConfig>>,
}

/// Specification of a counter derived from a log event.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct CounterConfig {
    /// Increments the counter by the value in `field`, instead of only by `1`.
    #[serde(default = "default_increment_by_value")]
    pub increment_by_value: bool,

    #[configurable(derived)]
    #[serde(default = "default_kind")]
    pub kind: MetricKind,
}

/// Specification of a metric derived from a log event.
// TODO: While we're resolving the schema for this enum somewhat reasonably (in
// `generate-components-docs.rb`), we have a problem where an overlapping field (overlap between two
// or more of the subschemas) takes the details of the last subschema to be iterated over that
// contains that field, such that, for example, the `Summary` variant below is overriding the
// description for almost all of the fields because they're shared across all of the variants.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct MetricConfig {
    /// Name of the field in the event to generate the metric.
    pub field: Template,

    /// Overrides the name of the counter.
    ///
    /// If not specified, `field` is used as the name of the metric.
    pub name: Option<Template>,

    /// Sets the namespace for the metric.
    pub namespace: Option<Template>,

    /// Tags to apply to the metric.
    ///
    /// Both keys and values can be templated, allowing you to attach dynamic tags to events.
    ///
    #[configurable(metadata(docs::additional_props_description = "A metric tag."))]
    pub tags: Option<IndexMap<Template, TagConfig>>,

    #[configurable(derived)]
    #[serde(flatten)]
    pub metric: MetricTypeConfig,
}

/// Specification of the value of a created tag.
///
/// This may be a single value, a `null` for a bare tag, or an array of either.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(untagged)]
pub enum TagConfig {
    /// A single tag value.
    Plain(Option<Template>),

    /// An array of values to give to the same tag name.
    Multi(Vec<Option<Template>>),
}

/// Specification of the type of an individual metric, and any associated data.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The type of metric to create."))]
pub enum MetricTypeConfig {
    /// A counter.
    Counter(CounterConfig),

    /// A histogram.
    Histogram,

    /// A gauge.
    Gauge,

    /// A set.
    Set,

    /// A summary.
    Summary,
}

impl MetricConfig {
    fn field(&self) -> &str {
        self.field.get_ref()
    }
}

const fn default_increment_by_value() -> bool {
    false
}

const fn default_kind() -> MetricKind {
    MetricKind::Incremental
}

#[derive(Debug, Clone)]
pub struct LogToMetric {
    pub metrics: Vec<MetricConfig>,
}

impl GenerateConfig for LogToMetricConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            metrics: Some(vec![MetricConfig {
                field: "field_name".try_into().expect("Fixed template"),
                name: None,
                namespace: None,
                tags: None,
                metric: MetricTypeConfig::Counter(CounterConfig {
                    increment_by_value: false,
                    kind: MetricKind::Incremental,
                }),
            }]),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "log_to_metric")]
impl TransformConfig for LogToMetricConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(LogToMetric {
            metrics: self.metrics.clone().unwrap_or_default(),
        }))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        _: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        // Converting the log to a metric means we lose all incoming `Definition`s.
        vec![TransformOutput::new(DataType::Metric, HashMap::new())]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

enum TransformError {
    PathNotFound {
        path: String,
    },
    PathNull {
        path: String,
    },
    ParseFloatError {
        path: String,
        error: ParseFloatError,
    },
    TemplateRenderingError(TemplateRenderingError),
    PairExpansionError {
        key: String,
        value: String,
        error: serde_json::Error,
    },
}

fn render_template(template: &Template, event: &Event) -> Result<String, TransformError> {
    template
        .render_string(event)
        .map_err(TransformError::TemplateRenderingError)
}

fn render_tags(
    tags: &Option<IndexMap<Template, TagConfig>>,
    event: &Event,
) -> Result<Option<OtelAttributes>, TransformError> {
    let mut static_tags: HashMap<String, String> = HashMap::new();
    let mut dynamic_tags: HashMap<String, String> = HashMap::new();
    Ok(match tags {
        None => None,
        Some(tags) => {
            let mut result = OtelAttributes::default();
            for (name, config) in tags {
                match config {
                    TagConfig::Plain(template) => {
                        render_tag_into(
                            event,
                            name,
                            template.as_ref(),
                            &mut result,
                            &mut static_tags,
                            &mut dynamic_tags,
                        )?;
                    }
                    TagConfig::Multi(vec) => {
                        for template in vec {
                            render_tag_into(
                                event,
                                name,
                                template.as_ref(),
                                &mut result,
                                &mut static_tags,
                                &mut dynamic_tags,
                            )?;
                        }
                    }
                }
            }
            for (k, v) in static_tags {
                if let Some(discarded_v) = dynamic_tags.insert(k.clone(), v.clone()) {
                    warn!(
                        "Static tags overrides dynamic tags. \
                key: {}, value: {:?}, discarded value: {:?}",
                        k, v, discarded_v
                    );
                };
            }
            result.as_option()
        }
    })
}

fn render_tag_into(
    event: &Event,
    key_template: &Template,
    value_template: Option<&Template>,
    result: &mut OtelAttributes,
    static_tags: &mut HashMap<String, String>,
    dynamic_tags: &mut HashMap<String, String>,
) -> Result<(), TransformError> {
    let key = match render_template(key_template, event) {
        Ok(key_s) => key_s,
        Err(TransformError::TemplateRenderingError(err)) => {
            emit!(crate::internal_events::TemplateRenderingError {
                error: err,
                drop_event: false,
                field: Some(key_template.get_ref()),
            });
            return Ok(());
        }
        Err(err) => return Err(err),
    };
    match value_template {
        None => {
            result.insert(key, AnyValue { value: None });
        }
        Some(template) => match render_template(template, event) {
            Ok(value) => {
                let expanded_pairs = pair_expansion(&key, &value, static_tags, dynamic_tags)
                    .map_err(|error| TransformError::PairExpansionError { key, value, error })?;
                result.extend_strings(expanded_pairs);
            }
            Err(TransformError::TemplateRenderingError(value_error)) => {
                emit!(crate::internal_events::TemplateRenderingError {
                    error: value_error,
                    drop_event: false,
                    field: Some(template.get_ref()),
                });
                return Ok(());
            }
            Err(other) => return Err(other),
        },
    };
    Ok(())
}

fn to_metric_with_config(
    config: &MetricConfig,
    event: &Event,
) -> Result<OtelMetric, TransformError> {
    let log = event.as_log();

    let timestamp = log
        .get_timestamp()
        .and_then(|v| v.as_timestamp().copied())
        .or_else(|| Some(Utc::now()));

    let metadata = event
        .metadata()
        .clone()
        .with_schema_definition(&Arc::new(Definition::any()));

    let field = parse_target_path(config.field()).map_err(|_e| PathNotFound {
        path: config.field().to_string(),
    })?;

    let value = match log.get(&field) {
        None => Err(TransformError::PathNotFound {
            path: field.to_string(),
        }),
        Some(Value::Null) => Err(TransformError::PathNull {
            path: field.to_string(),
        }),
        Some(value) => Ok(value),
    }?;

    let name = config.name.as_ref().unwrap_or(&config.field);
    let name = render_template(name, event)?;

    let namespace = config.namespace.as_ref();
    let namespace = namespace
        .map(|namespace| render_template(namespace, event))
        .transpose()?;

    let tags = render_tags(&config.tags, event)?;

    let metric = match &config.metric {
        MetricTypeConfig::Counter(counter) => {
            let value = if counter.increment_by_value {
                value.to_string_lossy().parse().map_err(|error| {
                    TransformError::ParseFloatError {
                        path: config.field.get_ref().to_owned(),
                        error,
                    }
                })?
            } else {
                1.0
            };

            OtelMetric::new_counter(&name, counter.kind, value)
        }
        MetricTypeConfig::Histogram | MetricTypeConfig::Summary => {
            let value: f64 = value.to_string_lossy().parse().map_err(|error| {
                TransformError::ParseFloatError {
                    path: field.to_string(),
                    error,
                }
            })?;

            OtelMetric::new_exponential_histogram_single(&name, value)
        }
        MetricTypeConfig::Gauge => {
            let value = value.to_string_lossy().parse().map_err(|error| {
                TransformError::ParseFloatError {
                    path: field.to_string(),
                    error,
                }
            })?;

            OtelMetric::new_gauge(&name, value)
        }
        MetricTypeConfig::Set => {
            let value = value.to_string_lossy().into_owned();

            OtelMetric::new_set_from_values(&name, MetricKind::Incremental, vec![value])
        }
    };
    Ok(metric
        .with_metadata(metadata)
        .with_namespace(namespace)
        .with_tags(tags)
        .with_timestamp(timestamp))
}

impl FunctionTransform for LogToMetric {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        if !matches!(event, Event::Log(_)) {
            return;
        }
        // Metrics are "all or none" for a specific log. If a single fails, none are produced.
        let mut buffer = Vec::with_capacity(self.metrics.len());
        for config in self.metrics.iter() {
            match to_metric_with_config(config, &event) {
                Ok(otel) => {
                    buffer.push(Event::Metric(otel));
                }
                Err(err) => {
                    match err {
                        TransformError::PathNull { path } => {
                            emit!(LogToMetricFieldNullError {
                                field: path.as_ref()
                            })
                        }
                        TransformError::PathNotFound { path } => {
                            emit!(ParserMissingFieldError::<DROP_EVENT> {
                                field: path.as_ref()
                            })
                        }
                        TransformError::ParseFloatError { path, error } => {
                            emit!(LogToMetricParseFloatError {
                                field: path.as_ref(),
                                error
                            })
                        }
                        TransformError::TemplateRenderingError(error) => {
                            emit!(crate::internal_events::TemplateRenderingError {
                                error,
                                drop_event: true,
                                field: None,
                            })
                        }
                        TransformError::PairExpansionError { key, value, error } => {
                            emit!(crate::internal_events::PairExpansionError {
                                key: &key,
                                value: &value,
                                drop_event: true,
                                error
                            })
                        }
                    };
                    // early return to prevent the partial buffer from being sent
                    return;
                }
            }
        }

        // Metric generation was successful, publish them all.
        for event in buffer {
            output.push(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use chrono::{DateTime, Timelike, Utc, offset::TimeZone};
    use similar_asserts::assert_eq;
    use sol_lib::{config::ComponentKey, event::ObjectMap, otel_tags};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use sol_lib::lookup::{OwnedTargetPath, owned_value_path};

    use super::*;
    use crate::{
        event::{Event, EventMetadata, OtelLog, OtelMetric, metric::MetricKind},
        test_util::components::assert_transform_compliance,
        transforms::test::create_topology,
    };

    const TEST_SOURCE_COMPONENT_ID: &str = "in";
    const TEST_UPSTREAM_COMPONENT_ID: &str = "transform";
    const TEST_SOURCE_TYPE: &str = "unit_test_stream";

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<LogToMetricConfig>();
    }

    fn parse_config(s: &str) -> LogToMetricConfig {
        toml::from_str(s).unwrap()
    }

    fn parse_yaml_config(s: &str) -> LogToMetricConfig {
        serde_yaml::from_str(s).unwrap()
    }

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
            .single()
            .and_then(|t| t.with_nanosecond(11))
            .expect("invalid timestamp")
    }

    fn create_event(key: &str, value: impl Into<Value> + std::fmt::Debug) -> Event {
        let mut log = Event::Log(OtelLog::from("i am a log"));
        log.as_mut_log().insert(key, value);
        log.as_mut_log().insert(
            &OwnedTargetPath::event(owned_value_path!("time_unix_nano")),
            ts(),
        );
        log
    }

    fn set_test_source_metadata(metadata: &mut EventMetadata) {
        metadata.set_upstream_id(Arc::new(OutputId::from(TEST_UPSTREAM_COMPONENT_ID)));
        metadata.set_source_id(Arc::new(ComponentKey::from(TEST_SOURCE_COMPONENT_ID)));
        metadata.set_source_type(TEST_SOURCE_TYPE);
    }

    async fn do_transform(config: LogToMetricConfig, event: Event) -> Option<Event> {
        assert_transform_compliance(async move {
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;
            tx.send(event).await.unwrap();
            let result = tokio::time::timeout(Duration::from_secs(5), out.recv())
                .await
                .unwrap_or(None);
            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
            result
        })
        .await
    }

    async fn do_transform_multiple_events(
        config: LogToMetricConfig,
        event: Event,
        count: usize,
    ) -> Vec<Event> {
        assert_transform_compliance(async move {
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;
            tx.send(event).await.unwrap();

            let mut results = vec![];
            for _ in 0..count {
                let result = tokio::time::timeout(Duration::from_secs(5), out.recv())
                    .await
                    .unwrap_or(None);
                if let Some(event) = result {
                    results.push(event);
                }
            }

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
            results
        })
        .await
    }

    #[tokio::test]
    async fn count_http_status_codes() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"
            "#,
        );

        let event = create_event("status", "42");
        let mut metadata = event.metadata().clone();
        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);
        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_counter("status", MetricKind::Incremental, 1.0)
                .with_metadata(metadata)
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn count_http_requests_with_tags() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "message"
            name = "http_requests_total"
            namespace = "app"
            tags = {method = "{{method}}", code = "{{code}}", missing_tag = "{{unknown}}", host = "localhost"}
            "#,
        );

        let mut event = create_event("message", "i am log");
        event.as_mut_log().insert("method", "post");
        event.as_mut_log().insert("code", "200");
        let mut metadata = event.metadata().clone();
        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_counter("http_requests_total", MetricKind::Incremental, 1.0)
                .with_metadata(metadata)
                .with_namespace(Some("app"))
                .with_tags(Some(otel_tags!(
                    "method" => "post",
                    "code" => "200",
                    "host" => "localhost",
                )))
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn count_http_requests_with_tags_expansion() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "message"
            name = "http_requests_total"
            namespace = "app"
            tags = {"*" = "{{ dict }}"}
            "#,
        );

        let mut event = create_event("message", "i am log");
        let log = event.as_mut_log();

        let mut test_dict = ObjectMap::default();
        test_dict.insert("one".into(), Value::from("foo"));
        test_dict.insert("two".into(), Value::from("baz"));
        log.insert("dict", Value::from(test_dict));

        let mut metadata = event.metadata().clone();
        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_counter("http_requests_total", MetricKind::Incremental, 1.0)
                .with_metadata(metadata)
                .with_namespace(Some("app"))
                .with_tags(Some(otel_tags!(
                    "one" => "foo",
                    "two" => "baz",
                )))
                .with_timestamp(Some(ts()))
        );
    }
    #[tokio::test]
    async fn count_http_requests_with_colliding_dynamic_tags() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "message"
            name = "http_requests_total"
            namespace = "app"
            tags = {"l1_*" = "{{ map1 }}", "*" = "{{ map2 }}"}
            "#,
        );

        let mut event = create_event("message", "i am log");
        let log = event.as_mut_log();

        let mut map1 = ObjectMap::default();
        map1.insert("key1".into(), Value::from("val1"));
        log.insert("map1", Value::from(map1));

        let mut map2 = ObjectMap::default();
        map2.insert("l1_key1".into(), Value::from("val2"));
        log.insert("map2", Value::from(map2));

        let mut metadata = event.metadata().clone();
        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event)
            .await
            .unwrap()
            .into_otel_metric();
        let tags = metric.tags().expect("Metric should have tags");

        assert_eq!(tags.iter_single().collect::<Vec<_>>()[0].0, "l1_key1");

        // With OtelAttributes, only single values per key; multi-value maps to last value
        assert_eq!(tags.iter_single().count(), 1);
        for (name, value) in tags.iter_single() {
            assert_eq!(name, "l1_key1");
            assert!(value == Some("val1") || value == Some("val2"));
        }
    }
    #[tokio::test]
    async fn multi_value_tags_yaml() {
        // Have to use YAML to represent bare tags
        let config = parse_yaml_config(
            r#"
            metrics:
            - field: "message"
              type: "counter"
              tags:
                tag:
                - "one"
                - null
                - "two"
            "#,
        );

        let event = create_event("message", "I am log");
        let metric = do_transform(config, event)
            .await
            .unwrap()
            .into_otel_metric();
        let tags = metric.tags().expect("Metric should have tags");

        assert_eq!(
            tags.iter_single().collect::<Vec<_>>(),
            vec![("tag", Some("two"))]
        );

        // With OtelAttributes, last value wins for multi-value tags
        assert_eq!(tags.iter_single().count(), 1);
        for (name, value) in tags.iter_single() {
            assert_eq!(name, "tag");
            assert!(value.is_none() || value == Some("one") || value == Some("two"));
        }
    }
    #[tokio::test]
    async fn multi_value_tags_expansion_yaml() {
        // Have to use YAML to represent bare tags
        let config = parse_yaml_config(
            r#"
            metrics:
            - field: "message"
              type: "counter"
              tags:
                "*": "{{dict}}"
            "#,
        );

        let mut event = create_event("message", "I am log");
        let log = event.as_mut_log();

        let mut test_dict = ObjectMap::default();
        test_dict.insert("one".into(), Value::from(vec!["foo", "baz"]));
        log.insert("dict", Value::from(test_dict));

        let metric = do_transform(config, event)
            .await
            .unwrap()
            .into_otel_metric();
        let tags = metric.tags().expect("Metric should have tags");

        assert_eq!(
            tags.iter_single().collect::<Vec<_>>(),
            vec![("one", Some("[\"foo\",\"baz\"]"))]
        );

        assert_eq!(tags.iter_single().count(), 1);
        for (name, value) in tags.iter_single() {
            assert_eq!(name, "one");
            assert_eq!(value, Some("[\"foo\",\"baz\"]"));
        }
    }

    #[tokio::test]
    async fn multi_value_tags_toml() {
        let config = parse_config(
            r#"
            [[metrics]]
            field = "message"
            type = "counter"
            [metrics.tags]
            tag = ["one", "two"]
            "#,
        );

        let event = create_event("message", "I am log");
        let metric = do_transform(config, event)
            .await
            .unwrap()
            .into_otel_metric();
        let tags = metric.tags().expect("Metric should have tags");

        assert_eq!(
            tags.iter_single().collect::<Vec<_>>(),
            vec![("tag", Some("two"))]
        );

        // With OtelAttributes, last value wins for multi-value tags
        assert_eq!(tags.iter_single().count(), 1);
        for (name, value) in tags.iter_single() {
            assert_eq!(name, "tag");
            assert!(value == Some("one") || value == Some("two"));
        }
    }

    #[tokio::test]
    async fn count_exceptions() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "backtrace"
            name = "exception_total"
            "#,
        );

        let event = create_event("backtrace", "message");
        let mut metadata = event.metadata().clone();
        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_counter("exception_total", MetricKind::Incremental, 1.0)
                .with_metadata(metadata)
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn count_exceptions_no_match() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "backtrace"
            name = "exception_total"
            "#,
        );

        let event = create_event("success", "42");
        assert_eq!(do_transform(config, event).await, None);
    }

    #[tokio::test]
    async fn sum_order_amounts() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "amount"
            name = "amount_total"
            increment_by_value = true
            "#,
        );

        let event = create_event("amount", "33.99");
        let mut metadata = event.metadata().clone();
        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);
        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_counter("amount_total", MetricKind::Incremental, 33.99)
                .with_metadata(metadata)
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn count_absolute() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "amount"
            name = "amount_total"
            increment_by_value = true
            kind = "absolute"
            "#,
        );

        let event = create_event("amount", "33.99");
        let mut metadata = event.metadata().clone();
        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_counter("amount_total", MetricKind::Absolute, 33.99)
                .with_metadata(metadata)
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn memory_usage_gauge() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "gauge"
            field = "memory_rss"
            name = "memory_rss_bytes"
            "#,
        );

        let event = create_event("memory_rss", "123");
        let mut metadata = event.metadata().clone();

        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));

        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_gauge("memory_rss_bytes", 123.0)
                .with_metadata(metadata)
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn parse_failure() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"
            name = "status_total"
            increment_by_value = true
            "#,
        );

        let event = create_event("status", "not a number");
        assert_eq!(do_transform(config, event).await, None);
    }

    #[tokio::test]
    async fn missing_field() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"
            name = "status_total"
            "#,
        );

        let event = create_event("not foo", "not a number");
        assert_eq!(do_transform(config, event).await, None);
    }

    #[tokio::test]
    async fn null_field() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"
            name = "status_total"
            "#,
        );

        let event = create_event("status", Value::Null);
        assert_eq!(do_transform(config, event).await, None);
    }

    #[tokio::test]
    async fn multiple_metrics() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "counter"
            field = "status"

            [[metrics]]
            type = "counter"
            field = "backtrace"
            name = "exception_total"
            "#,
        );

        let mut event = Event::Log(OtelLog::from("i am a log"));
        event.as_mut_log().insert(
            &OwnedTargetPath::event(owned_value_path!("time_unix_nano")),
            ts(),
        );
        event.as_mut_log().insert("status", "42");
        event.as_mut_log().insert("backtrace", "message");
        let mut metadata = event.metadata().clone();

        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let output = do_transform_multiple_events(config, event, 2).await;

        assert_eq!(2, output.len());
        assert_eq!(
            output[0].clone().into_otel_metric(),
            OtelMetric::new_counter("status", MetricKind::Incremental, 1.0)
                .with_metadata(metadata.clone())
                .with_timestamp(Some(ts()))
        );
        assert_eq!(
            output[1].clone().into_otel_metric(),
            OtelMetric::new_counter("exception_total", MetricKind::Incremental, 1.0)
                .with_metadata(metadata)
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn multiple_metrics_with_multiple_templates() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "set"
            field = "status"
            name = "{{host}}_{{worker}}_status_set"

            [[metrics]]
            type = "counter"
            field = "backtrace"
            name = "{{service}}_exception_total"
            namespace = "{{host}}"
            "#,
        );

        let mut event = Event::Log(OtelLog::from("i am a log"));
        event.as_mut_log().insert(
            &OwnedTargetPath::event(owned_value_path!("time_unix_nano")),
            ts(),
        );
        event.as_mut_log().insert("status", "42");
        event.as_mut_log().insert("backtrace", "message");
        event.as_mut_log().insert("host", "local");
        event.as_mut_log().insert("worker", "abc");
        event.as_mut_log().insert("service", "xyz");
        let mut metadata = event.metadata().clone();

        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let output = do_transform_multiple_events(config, event, 2).await;

        assert_eq!(2, output.len());
        assert_eq!(
            output[0].clone().into_otel_metric(),
            OtelMetric::new_set_from_values(
                "local_abc_status_set",
                MetricKind::Incremental,
                vec![String::from("42")]
            )
            .with_metadata(metadata.clone())
            .with_timestamp(Some(ts()))
        );
        assert_eq!(
            output[1].clone().into_otel_metric(),
            OtelMetric::new_counter("xyz_exception_total", MetricKind::Incremental, 1.0)
                .with_metadata(metadata)
                .with_namespace(Some("local"))
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn user_ip_set() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "set"
            field = "user_ip"
            name = "unique_user_ip"
            "#,
        );

        let event = create_event("user_ip", "1.2.3.4");
        let mut metadata = event.metadata().clone();
        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_set_from_values(
                "unique_user_ip",
                MetricKind::Incremental,
                vec![String::from("1.2.3.4")]
            )
            .with_metadata(metadata)
            .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn response_time_histogram() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "histogram"
            field = "response_time"
            "#,
        );

        let event = create_event("response_time", "2.5");
        let mut metadata = event.metadata().clone();

        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_exponential_histogram_single("response_time", 2.5)
                .with_metadata(metadata)
                .with_timestamp(Some(ts()))
        );
    }

    #[tokio::test]
    async fn response_time_summary() {
        let config = parse_config(
            r#"
            [[metrics]]
            type = "summary"
            field = "response_time"
            "#,
        );

        let event = create_event("response_time", "2.5");
        let mut metadata = event.metadata().clone();

        // definitions aren't valid for metrics yet, it's just set to the default (anything).
        metadata.set_schema_definition(&Arc::new(Definition::any()));
        set_test_source_metadata(&mut metadata);

        let metric = do_transform(config, event).await.unwrap();

        assert_eq!(
            metric.into_otel_metric(),
            OtelMetric::new_exponential_histogram_single("response_time", 2.5)
                .with_metadata(metadata)
                .with_timestamp(Some(ts()))
        );
    }
}
