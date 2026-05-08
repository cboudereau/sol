use std::{collections::BTreeMap, time::Duration};

use futures::StreamExt;
use serde_with::serde_as;
use sol_lib::event::otel_metric::{InstrumentationScope, Resource};
use sol_lib::{
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
    configurable::configurable_component,
    internal_event::{ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol},
    lookup::{lookup_v2::OptionalValuePath, owned_value_path},
};
use tokio::time;
use tokio_stream::wrappers::IntervalStream;

use crate::{
    SourceSender,
    config::{SourceConfig, SourceContext, SourceOutput},
    event::Event,
    internal_events::{EventsReceived, StreamClosedError},
    metrics::Controller,
    shutdown::ShutdownSignal,
    sources::source_otel,
};

/// Configuration for the `internal_metrics` source.
#[serde_as]
#[configurable_component(source(
    "internal_metrics",
    "Expose internal metrics emitted by the running Vector instance."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields, default)]
pub struct InternalMetricsConfig {
    /// The interval between metric gathering, in seconds.
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    #[serde(default = "default_scrape_interval")]
    #[configurable(metadata(docs::human_name = "Scrape Interval"))]
    pub scrape_interval_secs: Duration,

    #[configurable(derived)]
    pub tags: TagsConfig,

    /// Overrides the default namespace for the metrics emitted by the source.
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Custom resource attributes for OTel Resource on emitted metrics.
    ///
    /// Default `service.name` is `sol` (not `sol/internal_metrics`).
    #[serde(default)]
    pub resource_attributes: BTreeMap<String, String>,
}

impl Default for InternalMetricsConfig {
    fn default() -> Self {
        Self {
            scrape_interval_secs: default_scrape_interval(),
            tags: TagsConfig::default(),
            namespace: default_namespace(),
            resource_attributes: BTreeMap::new(),
        }
    }
}

/// Tag configuration for the `internal_metrics` source.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields, default)]
pub struct TagsConfig {
    /// Overrides the name of the tag used to add the peer host to each metric.
    ///
    /// The value is the peer host's address, including the port. For example, `1.2.3.4:9000`.
    ///
    /// By default, `host` is used.
    ///
    /// Set to `""` to suppress this key.
    pub host_key: Option<OptionalValuePath>,

    /// Sets the name of the tag to use to add the current process ID to each metric.
    ///
    ///
    /// By default, this is not set and the tag is not automatically added.
    #[configurable(metadata(docs::examples = "pid"))]
    pub pid_key: Option<String>,
}

fn default_scrape_interval() -> Duration {
    Duration::from_secs_f64(1.0)
}

fn default_namespace() -> String {
    "sol".to_owned()
}

impl_generate_config_from_default!(InternalMetricsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "internal_metrics")]
impl SourceConfig for InternalMetricsConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        if self.scrape_interval_secs.is_zero() {
            warn!(
                "Interval set to 0 secs, this could result in high CPU utilization. It is suggested to use interval >= 1 secs.",
            );
        }
        let interval = self.scrape_interval_secs;

        // namespace for created metrics is already "sol" by default.
        let namespace = self.namespace.clone();

        let host_key = self
            .tags
            .host_key
            .clone()
            .unwrap_or(OptionalValuePath::from(owned_value_path!("host")));

        let pid_key = self
            .tags
            .pid_key
            .as_deref()
            .and_then(|tag| (!tag.is_empty()).then(|| tag.to_owned()));

        // For internal_metrics, default service.name is "sol" rather than "sol/internal_metrics".
        let mut ra = self.resource_attributes.clone();
        ra.entry("service.name".to_string())
            .or_insert_with(|| "sol".to_string());
        let resource = source_otel::build_source_resource("internal_metrics", &ra);
        let scope = source_otel::build_source_scope("internal_metrics");

        Ok(Box::pin(
            InternalMetrics {
                namespace,
                host_key,
                pid_key,
                controller: Controller::get()?,
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

struct InternalMetrics<'a> {
    namespace: String,
    host_key: OptionalValuePath,
    pid_key: Option<String>,
    controller: &'a Controller,
    interval: time::Duration,
    out: SourceSender,
    shutdown: ShutdownSignal,
    resource: Resource,
    scope: InstrumentationScope,
}

impl InternalMetrics<'_> {
    async fn run(mut self) -> Result<(), ()> {
        let events_received = register!(EventsReceived);
        let bytes_received = register!(BytesReceived::from(Protocol::INTERNAL));
        let mut interval =
            IntervalStream::new(time::interval(self.interval)).take_until(self.shutdown);
        while interval.next().await.is_some() {
            let hostname = crate::get_hostname();
            let pid = std::process::id().to_string();

            let metrics = self.controller.capture_metrics();
            let count = metrics.len();
            let byte_size = metrics.size_of();
            let json_size = metrics.estimated_json_encoded_size_of();

            bytes_received.emit(ByteSize(byte_size));
            events_received.emit(CountByteSize(count, json_size));

            let batch = metrics.into_iter().map(|mut metric| {
                if self.namespace != "sol" {
                    if let Some(stripped) = metric.name().strip_prefix("sol_") {
                        metric.metric_mut().name = format!("{}_{}", self.namespace, stripped);
                    }
                    metric = metric.with_namespace(Some(self.namespace.clone()));
                }

                if let Some(host_key) = &self.host_key.path
                    && let Ok(hostname) = &hostname
                {
                    metric.replace_tag(host_key.to_string(), hostname.to_owned());
                }
                if let Some(pid_key) = &self.pid_key {
                    metric.replace_tag(pid_key.to_owned(), pid.clone());
                }
                metric.set_resource(self.resource.clone());
                metric.set_scope(self.scope.clone());
                metric
            });

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
    use std::collections::BTreeMap;

    use metrics::{counter, gauge, histogram};
    use sol_lib::{metrics::Controller, otel_tags};

    use super::*;
    use crate::{
        event::{Event, MetricView, OtelMetric},
        test_util::{
            self,
            components::{SOURCE_TAGS, run_and_assert_source_compliance},
        },
    };

    #[test]
    fn generate_config() {
        test_util::test_generate_config::<InternalMetricsConfig>();
    }

    #[test]
    fn captures_internal_metrics() {
        test_util::trace_init();

        // There *seems* to be a race condition here (CI was flaky), so add a slight delay.
        std::thread::sleep(std::time::Duration::from_millis(300));

        gauge!("foo").set(1.0);
        gauge!("foo").set(2.0);
        counter!("bar").increment(3);
        counter!("bar").increment(4);
        histogram!("baz").record(5.0);
        histogram!("baz").record(6.0);
        histogram!("quux", "host" => "foo").record(8.0);
        histogram!("quux", "host" => "foo").record(8.1);

        let controller = Controller::get().expect("no controller");

        // There *seems* to be a race condition here (CI was flaky), so add a slight delay.
        std::thread::sleep(std::time::Duration::from_millis(300));

        let output = controller
            .capture_metrics()
            .into_iter()
            .map(|metric| (metric.name().to_string(), metric))
            .collect::<BTreeMap<String, OtelMetric>>();

        assert!(matches!(
            output["sol_foo"].view(),
            MetricView::Gauge { value: 2.0 }
        ));
        assert!(matches!(
            output["sol_bar"].view(),
            MetricView::Sum { value: 7.0 }
        ));

        match output["sol_baz"].view() {
            MetricView::Histogram {
                counts, count, sum, ..
            } => {
                // This index is _only_ stable so long as the offsets in
                // [`metrics::handle::Histogram::new`] are hard-coded. If this
                // check fails you might look there and see if we've allowed
                // users to set their own bucket widths.
                assert_eq!(counts[15], 2);
                assert_eq!(count, 2);
                assert_eq!(sum, 11.0);
            }
            _ => panic!("wrong type"),
        }

        match output["sol_quux"].view() {
            MetricView::Histogram {
                counts, count, sum, ..
            } => {
                // This index is _only_ stable so long as the offsets in
                // [`metrics::handle::Histogram::new`] are hard-coded. If this
                // check fails you might look there and see if we've allowed
                // users to set their own bucket widths.
                assert_eq!(counts[15], 1);
                assert_eq!(counts[16], 1);
                assert_eq!(count, 2);
                assert_eq!(sum, 16.1);
            }
            _ => panic!("wrong type"),
        }

        let labels = otel_tags!("host" => "foo");
        assert_eq!(Some(labels), output["sol_quux"].tags());
    }

    async fn event_from_config(config: InternalMetricsConfig) -> Event {
        let mut events = run_and_assert_source_compliance(
            config,
            time::Duration::from_millis(100),
            &SOURCE_TAGS,
        )
        .await;

        assert!(!events.is_empty());
        events.remove(0)
    }

    #[tokio::test]
    async fn default_namespace() {
        let event = event_from_config(InternalMetricsConfig::default()).await;

        assert_eq!(event.as_metric().namespace(), Some("sol"));
    }

    #[tokio::test]
    async fn sets_tags() {
        let event = event_from_config(InternalMetricsConfig {
            tags: TagsConfig {
                host_key: Some(OptionalValuePath::new("my_host_key")),
                pid_key: Some(String::from("my_pid_key")),
            },
            ..Default::default()
        })
        .await;

        let metric = event.as_metric();

        assert!(metric.tag_value("my_host_key").is_some());
        assert!(metric.tag_value("my_pid_key").is_some());
    }

    #[tokio::test]
    async fn only_host_tags_by_default() {
        let event = event_from_config(InternalMetricsConfig::default()).await;

        let metric = event.as_metric();

        assert!(metric.tag_value("host").is_some());
        assert!(metric.tag_value("pid").is_none());
    }

    #[tokio::test]
    async fn namespace() {
        let namespace = "totally_custom";

        let config = InternalMetricsConfig {
            namespace: namespace.to_owned(),
            ..InternalMetricsConfig::default()
        };

        let event = event_from_config(config).await;

        assert_eq!(event.as_metric().namespace(), Some(namespace));
    }
}
