use std::{
    collections::BTreeMap,
    convert::TryFrom,
    time::{Duration, Instant},
};

use bytes::Bytes;
use chrono::Utc;
use futures::{StreamExt, future::join_all};
use http::{Request, StatusCode};
use http_body_util::{BodyExt as _, Collected, Full};
use http::Uri;
use serde_with::serde_as;
use snafu::{ResultExt, Snafu};
use sol_lib::event::otel_metric::{InstrumentationScope, Resource};
use sol_lib::{EstimatedJsonEncodedSizeOf, configurable::configurable_component, otel_tags};
use tokio::time;
use tokio_stream::wrappers::IntervalStream;

use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::{Event, OtelAttributes, OtelMetric, metric::MetricKind},
    http::{Auth, HttpClient},
    internal_events::{
        CollectionCompleted, EndpointBytesReceived, NginxMetricsEventsReceived,
        NginxMetricsRequestError, NginxMetricsStubStatusParseError, StreamClosedError,
    },
    sources::source_otel,
    tls::{TlsConfig, TlsSettings},
};

pub mod parser;
use parser::NginxStubStatus;

#[derive(Debug, Snafu)]
enum NginxBuildError {
    #[snafu(display("Failed to parse endpoint: {}", source))]
    HostInvalidUri { source: http::uri::InvalidUri },
}

#[derive(Debug, Snafu)]
enum NginxError {
    #[snafu(display("Invalid response status: {}", status))]
    InvalidResponseStatus { status: StatusCode },
}

/// Configuration for the `nginx_metrics` source.
#[serde_as]
#[configurable_component(source("nginx_metrics", "Collect metrics from NGINX."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct NginxMetricsConfig {
    /// A list of NGINX instances to scrape.
    ///
    /// Each endpoint must be a valid HTTP/HTTPS URI pointing to an NGINX instance that has the
    /// `ngx_http_stub_status_module` module enabled.
    #[configurable(metadata(docs::examples = "http://localhost:8000/basic_status"))]
    endpoints: Vec<String>,

    /// The interval between scrapes.
    #[serde(default = "default_scrape_interval_secs")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[configurable(metadata(docs::human_name = "Scrape Interval"))]
    scrape_interval_secs: Duration,

    /// Overrides the default namespace for the metrics emitted by the source.
    ///
    /// If set to an empty string, no namespace is added to the metrics.
    ///
    /// By default, `nginx` is used.
    #[serde(default = "default_namespace")]
    namespace: String,

    #[configurable(derived)]
    tls: Option<TlsConfig>,

    #[configurable(derived)]
    auth: Option<Auth>,

    /// Custom resource attributes for OTel Resource on emitted metrics.
    ///
    /// Defaults: `service.name = sol/nginx_metrics`, `host.name` = auto-detected hostname.
    #[serde(default)]
    resource_attributes: BTreeMap<String, String>,
}

pub(super) const fn default_scrape_interval_secs() -> Duration {
    Duration::from_secs(15)
}

pub fn default_namespace() -> String {
    "nginx".to_string()
}

impl_generate_config_from_default!(NginxMetricsConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "nginx_metrics")]
impl SourceConfig for NginxMetricsConfig {
    async fn build(&self, mut cx: SourceContext) -> crate::Result<super::Source> {
        let tls = TlsSettings::from_options(self.tls.as_ref())?;
        let http_client = HttpClient::new(tls, &cx.proxy)?;

        let namespace = Some(self.namespace.clone()).filter(|namespace| !namespace.is_empty());
        let resource =
            source_otel::build_source_resource("nginx_metrics", &self.resource_attributes);
        let scope = source_otel::build_source_scope("nginx_metrics");
        let mut sources = Vec::with_capacity(self.endpoints.len());
        for endpoint in self.endpoints.iter() {
            sources.push(NginxMetrics::new(
                http_client.clone(),
                endpoint.clone(),
                self.auth.clone(),
                namespace.clone(),
                resource.clone(),
                scope.clone(),
            )?);
        }

        let duration = self.scrape_interval_secs;
        let shutdown = cx.shutdown;
        Ok(Box::pin(async move {
            let mut interval = IntervalStream::new(time::interval(duration)).take_until(shutdown);
            while interval.next().await.is_some() {
                let start = Instant::now();
                let metrics = join_all(sources.iter().map(|nginx| nginx.collect())).await;
                emit!(CollectionCompleted {
                    start,
                    end: Instant::now()
                });

                let metrics: Vec<OtelMetric> = metrics.into_iter().flatten().collect();
                let count = metrics.len();

                let events: Vec<Event> = metrics.into_iter().map(Event::Metric).collect();
                if (cx.out.send_batch(events).await).is_err() {
                    emit!(StreamClosedError { count });
                    return Err(());
                }
            }

            Ok(())
        }))
    }

    fn outputs(&self) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct NginxMetrics {
    http_client: HttpClient,
    endpoint: String,
    auth: Option<Auth>,
    namespace: Option<String>,
    tags: OtelAttributes,
    resource: Resource,
    scope: InstrumentationScope,
}

impl NginxMetrics {
    fn new(
        http_client: HttpClient,
        endpoint: String,
        auth: Option<Auth>,
        namespace: Option<String>,
        resource: Resource,
        scope: InstrumentationScope,
    ) -> crate::Result<Self> {
        let tags = otel_tags!(
            "endpoint" => endpoint.clone(),
            "host" => Self::get_endpoint_host(&endpoint)?,
        );

        Ok(Self {
            http_client,
            endpoint,
            auth,
            namespace,
            tags,
            resource,
            scope,
        })
    }

    fn get_endpoint_host(endpoint: &str) -> crate::Result<String> {
        let uri: Uri = endpoint.parse().context(HostInvalidUriSnafu)?;
        Ok(match (uri.host().unwrap_or(""), uri.port()) {
            (host, None) => host.to_owned(),
            (host, Some(port)) => format!("{host}:{port}"),
        })
    }

    async fn collect(&self) -> Vec<OtelMetric> {
        let (up_value, mut metrics) = match self.collect_metrics().await {
            Ok(metrics) => (1.0, metrics),
            Err(()) => (0.0, vec![]),
        };

        let byte_size = metrics.estimated_json_encoded_size_of();

        metrics.push(self.create_gauge("up", up_value));

        emit!(NginxMetricsEventsReceived {
            count: metrics.len(),
            byte_size,
            endpoint: &self.endpoint
        });

        metrics
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "nginx stub status counters (connections, requests); precise for |v| <= 2^53"
    )]
    async fn collect_metrics(&self) -> Result<Vec<OtelMetric>, ()> {
        let response = self.get_nginx_response().await.map_err(|error| {
            emit!(NginxMetricsRequestError {
                error,
                endpoint: &self.endpoint,
            })
        })?;
        emit!(EndpointBytesReceived {
            byte_size: response.len(),
            protocol: "http",
            endpoint: &self.endpoint,
        });

        let status = NginxStubStatus::try_from(String::from_utf8_lossy(&response).as_ref())
            .map_err(|error| {
                emit!(NginxMetricsStubStatusParseError {
                    error,
                    endpoint: &self.endpoint,
                })
            })?;

        Ok(vec![
            self.create_gauge("connections_active", status.active as f64),
            self.create_counter("connections_accepted_total", status.accepts as f64),
            self.create_counter("connections_handled_total", status.handled as f64),
            self.create_counter("http_requests_total", status.requests as f64),
            self.create_gauge("connections_reading", status.reading as f64),
            self.create_gauge("connections_writing", status.writing as f64),
            self.create_gauge("connections_waiting", status.waiting as f64),
        ])
    }

    async fn get_nginx_response(&self) -> crate::Result<Bytes> {
        let mut request = Request::get(&self.endpoint).body(Full::new(Bytes::new()))?;
        if let Some(auth) = &self.auth {
            auth.apply(&mut request);
        }

        let response = self.http_client.send(request).await?;
        let (parts, body) = response.into_parts();
        match parts.status {
            StatusCode::OK => body.collect().await.map(Collected::to_bytes).map_err(Into::into),
            status => Err(Box::new(NginxError::InvalidResponseStatus { status })),
        }
    }

    fn create_counter(&self, name: &str, value: f64) -> OtelMetric {
        let mut m = OtelMetric::new_counter(name, MetricKind::Absolute, value)
            .with_namespace(self.namespace.clone())
            .with_tags(Some(self.tags.clone()))
            .with_timestamp(Some(Utc::now()));
        m.set_resource(self.resource.clone());
        m.set_scope(self.scope.clone());
        m
    }

    fn create_gauge(&self, name: &str, value: f64) -> OtelMetric {
        let mut m = OtelMetric::new_gauge(name, value)
            .with_namespace(self.namespace.clone())
            .with_tags(Some(self.tags.clone()))
            .with_timestamp(Some(Utc::now()));
        m.set_resource(self.resource.clone());
        m.set_scope(self.scope.clone());
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<NginxMetricsConfig>();
    }
}

#[cfg(all(test, feature = "nginx-integration-tests"))]
mod integration_tests {
    use tokio::time::Duration;

    use super::*;
    use crate::{
        config::ProxyConfig,
        test_util::components::{HTTP_PULL_SOURCE_TAGS, run_and_assert_source_compliance_advanced},
    };

    fn nginx_proxy_address() -> String {
        std::env::var("NGINX_PROXY_ADDRESS").unwrap_or_else(|_| "http://nginx-proxy:8000".into())
    }

    fn nginx_address() -> String {
        std::env::var("NGINX_ADDRESS").unwrap_or_else(|_| "http://localhost:8000".into())
    }

    fn squid_address() -> String {
        std::env::var("SQUID_ADDRESS").unwrap_or_else(|_| "http://localhost:3128".into())
    }

    async fn test_nginx(endpoint: String, auth: Option<Auth>, proxy: ProxyConfig) {
        let config = NginxMetricsConfig {
            endpoints: vec![endpoint],
            scrape_interval_secs: Duration::from_secs(15),
            namespace: "vector_nginx".to_owned(),
            tls: None,
            auth,
            resource_attributes: BTreeMap::new(),
        };

        let events = run_and_assert_source_compliance_advanced(
            config,
            move |context: &mut SourceContext| {
                context.proxy = proxy;
            },
            Some(Duration::from_secs(3)),
            None,
            &HTTP_PULL_SOURCE_TAGS,
        )
        .await;
        assert_eq!(events.len(), 8);
    }

    #[tokio::test]
    async fn test_stub_status() {
        let url = format!("{}/basic_status", nginx_address());
        test_nginx(url, None, ProxyConfig::default()).await
    }

    #[tokio::test]
    async fn test_stub_status_auth() {
        let url = format!("{}/basic_status_auth", nginx_address());
        test_nginx(
            url,
            Some(Auth::Basic {
                user: "vector".to_owned(),
                password: "vector".to_owned().into(),
            }),
            ProxyConfig::default(),
        )
        .await
    }

    // This integration test verifies that proxy support is wired up correctly in Vector
    // It is the only test of its kind
    #[tokio::test]
    async fn test_stub_status_with_proxy() {
        let url = format!("{}/basic_status", nginx_proxy_address());
        test_nginx(
            url,
            None,
            ProxyConfig {
                http: Some(squid_address()),
                ..Default::default()
            },
        )
        .await
    }
}
