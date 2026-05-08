use std::{collections::BTreeMap, time::Duration};

use chrono::Utc;
use futures::StreamExt;
use serde_with::serde_as;
use sol_lib::event::otel_metric::{InstrumentationScope, Resource};
use sol_lib::{
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
    configurable::configurable_component,
    internal_event::{ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol},
};
use tokio::time;
use tokio_stream::wrappers::IntervalStream;

use crate::{
    SourceSender,
    config::{SourceConfig, SourceContext, SourceOutput},
    event::{Event, MetricKind, OtelMetric},
    internal_events::{EventsReceived, StreamClosedError},
    shutdown::ShutdownSignal,
    sources::source_otel,
};

/// Configuration for the `static_metrics` source.
#[serde_as]
#[configurable_component(source(
    "static_metrics",
    "Produce static metrics defined in configuration."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct StaticMetricsConfig {
    /// The interval between metric emitting, in seconds.
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[serde(default = "default_interval")]
    #[configurable(metadata(docs::human_name = "Emitting interval"))]
    pub interval_secs: Duration,

    /// Overrides the default namespace for the metrics emitted by the source.
    #[serde(default = "default_namespace")]
    pub namespace: String,

    #[configurable(derived)]
    #[serde(default)]
    pub metrics: Vec<StaticMetricConfig>,

    /// Custom resource attributes for OTel Resource on emitted metrics.
    ///
    /// Defaults: `service.name = sol/static_metrics`, `host.name` = auto-detected hostname.
    #[serde(default)]
    pub resource_attributes: BTreeMap<String, String>,
}

impl Default for StaticMetricsConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_interval(),
            metrics: Vec::default(),
            namespace: default_namespace(),
            resource_attributes: BTreeMap::new(),
        }
    }
}

/// Supported metric value types for static metric configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum MetricValueConfig {
    /// A counter metric value.
    Counter {
        /// The counter value.
        value: f64,
    },
    /// A gauge metric value.
    Gauge {
        /// The gauge value.
        value: f64,
    },
}

/// Tag configuration for the `internal_metrics` source.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct StaticMetricConfig {
    /// Name of the static metric
    pub name: String,

    /// "Observed" value of the static metric
    pub value: MetricValueConfig,

    /// Kind of the static metric - either absolute or incremental
    pub kind: MetricKind,

    /// Key-value pairs representing tags and their values to add to the metric.
    #[configurable(metadata(
        docs::additional_props_description = "An individual tag - value pair."
    ))]
    pub tags: BTreeMap<String, String>,
}

fn default_interval() -> Duration {
    Duration::from_secs_f64(1.0)
}

fn default_namespace() -> String {
    "static".to_owned()
}

impl_generate_config_from_default!(StaticMetricsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "static_metrics")]
impl SourceConfig for StaticMetricsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        if self.interval_secs.is_zero() {
            warn!(
                "Interval set to 0 secs, this could result in high CPU utilization. It is suggested to use interval >= 1 secs.",
            );
        }
        let interval = self.interval_secs;

        let namespace = self.namespace.clone();

        let metrics = self.metrics.clone();
        let resource =
            source_otel::build_source_resource("static_metrics", &self.resource_attributes);
        let scope = source_otel::build_source_scope("static_metrics");

        Ok(Box::pin(
            StaticMetrics {
                namespace,
                metrics,
                interval,
                out: cx.out,
                shutdown: cx.shutdown,
                resource,
                scope,
            }
            .run(),
        ))
    }

    fn outputs(&self) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

struct StaticMetrics {
    namespace: String,
    metrics: Vec<StaticMetricConfig>,
    interval: time::Duration,
    out: SourceSender,
    shutdown: ShutdownSignal,
    resource: Resource,
    scope: InstrumentationScope,
}

impl StaticMetrics {
    async fn run(mut self) -> Result<(), ()> {
        let events_received = register!(EventsReceived);
        let bytes_received = register!(BytesReceived::from(Protocol::STATIC));
        let mut interval =
            IntervalStream::new(time::interval(self.interval)).take_until(self.shutdown);

        // Prepare metrics, since they are static and won't change
        let metrics: Vec<OtelMetric> = self
            .metrics
            .into_iter()
            .map(
                |StaticMetricConfig {
                     name,
                     value,
                     kind,
                     tags,
                 }| {
                    let metric = match value {
                        MetricValueConfig::Counter { value: v } => {
                            OtelMetric::new_counter(&name, kind, v)
                        }
                        MetricValueConfig::Gauge { value: v } => OtelMetric::new_gauge(&name, v),
                    };
                    let mut m = metric
                        .with_namespace(Some(self.namespace.clone()))
                        .with_tags(Some(tags.into()));
                    m.set_resource(self.resource.clone());
                    m.set_scope(self.scope.clone());
                    m
                },
            )
            .collect();

        while interval.next().await.is_some() {
            let count = metrics.len();
            let byte_size = metrics.size_of();
            let json_size = metrics.estimated_json_encoded_size_of();

            bytes_received.emit(ByteSize(byte_size));
            events_received.emit(CountByteSize(count, json_size));

            let batch = metrics
                .clone()
                .into_iter()
                .map(|metric| metric.with_timestamp(Some(Utc::now())));

            let events: Vec<Event> = batch.map(Event::Metric).collect();
            if (self.out.send_batch(events).await).is_err() {
                emit!(StreamClosedError { count });
                return Err(());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::{Event, MetricView},
        test_util::{
            self,
            components::{SOURCE_TAGS, run_and_assert_source_compliance},
        },
    };

    #[test]
    fn generate_config() {
        test_util::test_generate_config::<StaticMetricsConfig>();
    }

    async fn events_from_config(config: StaticMetricsConfig) -> Vec<Event> {
        run_and_assert_source_compliance(config, time::Duration::from_millis(100), &SOURCE_TAGS)
            .await
    }

    fn default_metric() -> StaticMetricConfig {
        StaticMetricConfig {
            name: "".to_string(),
            value: MetricValueConfig::Gauge { value: 0.0 },
            kind: MetricKind::Absolute,
            tags: BTreeMap::default(),
        }
    }

    #[tokio::test]
    async fn default_empty() {
        let events = events_from_config(StaticMetricsConfig::default()).await;

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn default_namespace() {
        let mut events = events_from_config(StaticMetricsConfig {
            metrics: vec![default_metric()],
            ..Default::default()
        })
        .await;

        assert!(!events.is_empty());
        let event = events.remove(0);
        assert_eq!(event.as_metric().namespace(), Some("static"));
    }

    #[tokio::test]
    async fn default_namespace_multiple_events() {
        let mut events = events_from_config(StaticMetricsConfig {
            metrics: vec![default_metric(), default_metric()],
            ..Default::default()
        })
        .await;

        assert!(!events.is_empty());
        let event = events.remove(0);
        assert_eq!(event.as_metric().namespace(), Some("static"));
        let event = events.remove(0);
        assert_eq!(event.as_metric().namespace(), Some("static"));
    }

    #[tokio::test]
    async fn namespace() {
        let namespace = "totally_custom";

        let config = StaticMetricsConfig {
            namespace: namespace.to_owned(),
            metrics: vec![default_metric()],
            ..StaticMetricsConfig::default()
        };

        let mut events = events_from_config(config).await;
        assert!(!events.is_empty());
        let event = events.remove(0);

        assert_eq!(event.as_metric().namespace(), Some(namespace));
    }

    #[tokio::test]
    async fn sets_custom_tags() {
        let mut events = events_from_config(StaticMetricsConfig {
            metrics: vec![StaticMetricConfig {
                name: "test".to_string(),
                value: MetricValueConfig::Gauge { value: 2.3 },
                kind: MetricKind::Absolute,
                tags: BTreeMap::from([("custom_tag".to_string(), "custom_tag_value".to_string())]),
            }],
            ..Default::default()
        })
        .await;

        assert!(!events.is_empty());
        let event = events.remove(0);
        let metric = event.as_metric();

        assert_eq!(metric.name(), "test");
        assert!(matches!(metric.view(), MetricView::Gauge { value: 2.3 }));
        assert_eq!(
            metric.tag_value("custom_tag"),
            Some("custom_tag_value".to_string())
        );
    }
}
