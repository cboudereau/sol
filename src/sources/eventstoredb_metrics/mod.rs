use std::{collections::BTreeMap, time::Duration};

use futures::{FutureExt, StreamExt};
use bytes::Bytes;
use http::{Request, Uri};
use http_body_util::{BodyExt as _, Collected, Full};
use serde_with::serde_as;
use sol_lib::{
    EstimatedJsonEncodedSizeOf,
    configurable::configurable_component,
    internal_event::{ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol},
};
use tokio_stream::wrappers::IntervalStream;

use self::types::Stats;
use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::Event,
    http::HttpClient,
    internal_events::{
        EventStoreDbMetricsHttpError, EventStoreDbStatsParsingError, EventsReceived,
        StreamClosedError,
    },
    sources::source_otel,
    tls::TlsSettings,
};

pub mod types;

/// Configuration for the `eventstoredb_metrics` source.
#[serde_as]
#[configurable_component(source(
    "eventstoredb_metrics",
    "Receive metrics from collected by a EventStoreDB."
))]
#[derive(Clone, Debug, Default)]
pub struct EventStoreDbConfig {
    /// Endpoint to scrape stats from.
    #[serde(default = "default_endpoint")]
    #[configurable(metadata(docs::examples = "https://localhost:2113/stats"))]
    endpoint: String,

    /// The interval between scrapes, in seconds.
    #[serde(default = "default_scrape_interval_secs")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[configurable(metadata(docs::human_name = "Scrape Interval"))]
    scrape_interval_secs: Duration,

    /// Overrides the default namespace for the metrics emitted by the source.
    ///
    /// By default, `eventstoredb` is used.
    #[configurable(metadata(docs::examples = "eventstoredb"))]
    default_namespace: Option<String>,

    /// Custom resource attributes for OTel Resource on emitted metrics.
    ///
    /// Defaults: `service.name = sol/eventstoredb_metrics`, `host.name` = auto-detected hostname.
    #[serde(default)]
    resource_attributes: BTreeMap<String, String>,
}

const fn default_scrape_interval_secs() -> Duration {
    Duration::from_secs(15)
}

pub fn default_endpoint() -> String {
    "https://localhost:2113/stats".to_string()
}

impl_generate_config_from_default!(EventStoreDbConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "eventstoredb_metrics")]
impl SourceConfig for EventStoreDbConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let resource =
            source_otel::build_source_resource("eventstoredb_metrics", &self.resource_attributes);
        let scope = source_otel::build_source_scope("eventstoredb_metrics");
        eventstoredb(
            self.endpoint.clone(),
            self.scrape_interval_secs,
            self.default_namespace.clone(),
            cx,
            resource,
            scope,
        )
    }

    fn outputs(&self) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

fn eventstoredb(
    endpoint: String,
    interval: Duration,
    namespace: Option<String>,
    mut cx: SourceContext,
    resource: opentelemetry_proto::tonic::resource::v1::Resource,
    scope: opentelemetry_proto::tonic::common::v1::InstrumentationScope,
) -> crate::Result<super::Source> {
    let mut ticks = IntervalStream::new(tokio::time::interval(interval)).take_until(cx.shutdown);
    let tls_settings = TlsSettings::from_options(None)?;
    let client = HttpClient::new(tls_settings, &cx.proxy)?;
    let url: Uri = endpoint.as_str().parse()?;

    let bytes_received = register!(BytesReceived::from(Protocol::HTTP));
    let events_received = register!(EventsReceived);

    Ok(Box::pin(
        async move {
            while ticks.next().await.is_some() {
                let req = Request::get(&url)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::new()))
                    .expect("Building request should be infallible.");

                match client.send(req).await {
                    Err(error) => {
                        emit!(EventStoreDbMetricsHttpError {
                            error: error.into(),
                        });
                        continue;
                    }

                    Ok(resp) => {
                        let bytes = match resp
                            .into_body()
                            .collect()
                            .await
                            .map(Collected::to_bytes)
                        {
                            Ok(b) => b,
                            Err(error) => {
                                emit!(EventStoreDbMetricsHttpError {
                                    error: error.into(),
                                });
                                continue;
                            }
                        };
                        bytes_received.emit(ByteSize(bytes.len()));

                        match serde_json::from_slice::<Stats>(bytes.as_ref()) {
                            Err(error) => {
                                emit!(EventStoreDbStatsParsingError { error });
                                continue;
                            }

                            Ok(stats) => {
                                let metrics = stats
                                    .metrics(namespace.clone())
                                    .into_iter()
                                    .map(|mut m| {
                                        m.set_resource(resource.clone());
                                        m.set_scope(scope.clone());
                                        m
                                    })
                                    .collect::<Vec<_>>();
                                let count = metrics.len();
                                let byte_size = metrics.estimated_json_encoded_size_of();

                                events_received.emit(CountByteSize(count, byte_size));

                                let events: Vec<Event> =
                                    metrics.into_iter().map(Event::Metric).collect();
                                if (cx.out.send_batch(events).await).is_err() {
                                    emit!(StreamClosedError { count });
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        .map(Ok)
        .boxed(),
    ))
}

#[cfg(all(test, feature = "eventstoredb_metrics-integration-tests"))]
mod integration_tests {
    use tokio::time::Duration;

    use super::*;
    use crate::test_util::components::{SOURCE_TAGS, run_and_assert_source_compliance};

    const EVENTSTOREDB_SCRAPE_ADDRESS: &str = "http://eventstoredb:2113/stats";

    #[tokio::test]
    async fn scrape_something() {
        let config = EventStoreDbConfig {
            endpoint: EVENTSTOREDB_SCRAPE_ADDRESS.to_owned(),
            scrape_interval_secs: Duration::from_secs(1),
            default_namespace: None,
            resource_attributes: BTreeMap::new(),
        };

        let events =
            run_and_assert_source_compliance(config, Duration::from_secs(5), &SOURCE_TAGS).await;
        assert!(!events.is_empty());
    }
}
