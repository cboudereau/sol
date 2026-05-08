use std::{
    convert::Infallible,
    hash::Hash,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use base64::prelude::{BASE64_STANDARD, Engine as _};
use futures::{FutureExt, StreamExt, future, stream::BoxStream};
use hyper::{
    Body, Method, Request, Response, Server, StatusCode,
    body::HttpBody,
    header::HeaderValue,
    service::{make_service_fn, service_fn},
};
use indexmap::{IndexMap, map::Entry};
use serde_with::serde_as;
use snafu::Snafu;
use sol_lib::{
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
    configurable::configurable_component,
    internal_event::{
        ByteSize, BytesSent, CountByteSize, EventsSent, InternalEventHandle as _, Output, Protocol,
        Registered,
    },
};
use stream_cancel::{Trigger, Tripwire};
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tracing::{Instrument, Span};

use super::collector::{MetricCollector, StringCollector};
use crate::{
    config::{AcknowledgementsConfig, GenerateConfig, Input, Resource, SinkConfig, SinkContext},
    event::{
        Event, EventStatus, Finalizable, MetricView, OtelMetric,
        metric::{MetricKind, MetricSeries},
    },
    http::{Auth, build_http_trace_layer},
    internal_events::PrometheusNormalizationError,
    sinks::{
        Healthcheck, VectorSink,
        util::{StreamSink, statistic::validate_quantiles},
    },
    tls::{MaybeTlsSettings, TlsEnableableConfig},
};

const MIN_FLUSH_PERIOD_SECS: u64 = 1;

const LOCK_FAILED: &str = "Prometheus exporter data lock is poisoned";

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Flush period for sets must be greater or equal to {} secs", min))]
    FlushPeriodTooShort { min: u64 },
}

/// Configuration for the `prometheus_exporter` sink.
#[serde_as]
#[configurable_component(sink(
    "prometheus_exporter",
    "Expose metric events on a Prometheus compatible endpoint."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct PrometheusExporterConfig {
    /// The default namespace for any metrics sent.
    ///
    /// This namespace is only used if a metric has no existing namespace. When a namespace is
    /// present, it is used as a prefix to the metric name, and separated with an underscore (`_`).
    ///
    /// It should follow the Prometheus [naming conventions][prom_naming_docs].
    ///
    /// [prom_naming_docs]: https://prometheus.io/docs/practices/naming/#metric-names
    #[serde(alias = "namespace")]
    #[configurable(metadata(docs::advanced))]
    pub default_namespace: Option<String>,

    /// The address to expose for scraping.
    ///
    /// The metrics are exposed at the typical Prometheus exporter path, `/metrics`.
    #[serde(default = "default_address")]
    #[configurable(metadata(docs::examples = "192.160.0.10:9598"))]
    pub address: SocketAddr,

    #[configurable(derived)]
    pub auth: Option<Auth>,

    #[configurable(derived)]
    pub tls: Option<TlsEnableableConfig>,

    /// Default buckets to use for aggregating [distribution][dist_metric_docs] metrics into histograms.
    ///
    /// [dist_metric_docs]: https://vector.dev/docs/architecture/data-model/metric/#distribution
    #[serde(default = "super::default_histogram_buckets")]
    #[configurable(metadata(docs::advanced))]
    pub buckets: Vec<f64>,

    /// Quantiles to use for aggregating [distribution][dist_metric_docs] metrics into a summary.
    ///
    /// [dist_metric_docs]: https://vector.dev/docs/architecture/data-model/metric/#distribution
    #[serde(default = "super::default_summary_quantiles")]
    #[configurable(metadata(docs::advanced))]
    pub quantiles: Vec<f64>,

    /// Whether or not to render [distributions][dist_metric_docs] as an [aggregated histogram][prom_agg_hist_docs] or  [aggregated summary][prom_agg_summ_docs].
    ///
    /// While distributions as a lossless way to represent a set of samples for a
    /// metric is supported, Prometheus clients (the application being scraped, which is this sink) must
    /// aggregate locally into either an aggregated histogram or aggregated summary.
    ///
    /// [dist_metric_docs]: https://vector.dev/docs/architecture/data-model/metric/#distribution
    /// [prom_agg_hist_docs]: https://prometheus.io/docs/concepts/metric_types/#histogram
    /// [prom_agg_summ_docs]: https://prometheus.io/docs/concepts/metric_types/#summary
    #[serde(default = "default_distributions_as_summaries")]
    #[configurable(metadata(docs::advanced))]
    pub distributions_as_summaries: bool,

    /// The interval, in seconds, on which metrics are flushed.
    ///
    /// On the flush interval, if a metric has not been seen since the last flush interval, it is
    /// considered expired and is removed.
    ///
    /// Be sure to configure this value higher than your client’s scrape interval.
    #[serde(default = "default_flush_period_secs")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[configurable(metadata(docs::advanced))]
    #[configurable(metadata(docs::human_name = "Flush Interval"))]
    pub flush_period_secs: Duration,

    /// Suppresses timestamps on the Prometheus output.
    ///
    /// This can sometimes be useful when the source of metrics leads to their timestamps being too
    /// far in the past for Prometheus to allow them, such as when aggregating metrics over long
    /// time periods, or when replaying old metrics from a disk buffer.
    #[serde(default)]
    #[configurable(metadata(docs::advanced))]
    pub suppress_timestamp: bool,

    /// Resource attribute keys to promote to metric labels.
    ///
    /// When set, the specified keys are extracted from the OTel Resource and
    /// injected as metric-level tags/labels. This allows non-OTLP sinks to
    /// propagate Resource information like `service.name` and `host.name`.
    #[serde(default = "default_resource_to_labels")]
    #[configurable(metadata(docs::advanced))]
    pub resource_to_labels: Vec<String>,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,
}

fn default_resource_to_labels() -> Vec<String> {
    vec!["service.name".into(), "host.name".into()]
}

impl Default for PrometheusExporterConfig {
    fn default() -> Self {
        Self {
            default_namespace: None,
            address: default_address(),
            auth: None,
            tls: None,
            buckets: super::default_histogram_buckets(),
            quantiles: super::default_summary_quantiles(),
            distributions_as_summaries: default_distributions_as_summaries(),
            flush_period_secs: default_flush_period_secs(),
            suppress_timestamp: default_suppress_timestamp(),
            resource_to_labels: default_resource_to_labels(),
            acknowledgements: Default::default(),
        }
    }
}

const fn default_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9598)
}

const fn default_distributions_as_summaries() -> bool {
    false
}

const fn default_flush_period_secs() -> Duration {
    Duration::from_secs(60)
}

const fn default_suppress_timestamp() -> bool {
    false
}

impl GenerateConfig for PrometheusExporterConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "prometheus_exporter")]
impl SinkConfig for PrometheusExporterConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        if self.flush_period_secs.as_secs() < MIN_FLUSH_PERIOD_SECS {
            return Err(Box::new(BuildError::FlushPeriodTooShort {
                min: MIN_FLUSH_PERIOD_SECS,
            }));
        }

        validate_quantiles(&self.quantiles)?;

        let sink = PrometheusExporter::new(self.clone());
        let healthcheck = future::ok(()).boxed();

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

struct PrometheusExporter {
    server_shutdown_trigger: Option<Trigger>,
    config: PrometheusExporterConfig,
    metrics: Arc<RwLock<IndexMap<MetricRef, (OtelMetric, MetricMetadata)>>>,
}

/// Expiration metadata for a metric.
#[derive(Clone, Copy, Debug)]
struct MetricMetadata {
    expiration_window: Duration,
    expires_at: Instant,
}

impl MetricMetadata {
    pub fn new(expiration_window: Duration) -> Self {
        Self {
            expiration_window,
            expires_at: Instant::now() + expiration_window,
        }
    }

    /// Resets the expiration deadline.
    pub fn refresh(&mut self) {
        self.expires_at = Instant::now() + self.expiration_window;
    }

    /// Whether or not the referenced metric has expired yet.
    pub fn has_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

// Composite identifier that uniquely represents a metric.
//
// Instead of simply working off of the name (series) alone, we include the metric kind as well as
// the type (counter, gauge, etc) and any subtype information like histogram buckets.
//
// Specifically, though, we do _not_ include the actual metric value.  This type is used
// specifically to look up the entry in a map for a metric in the sense of "get the metric whose
// name is X and type is Y and has these tags".
#[derive(Clone, Debug)]
struct MetricRef {
    series: MetricSeries,
    metric_type: &'static str,
    bounds: Option<Vec<f64>>,
}

const fn metric_type_tag(view: &MetricView<'_>) -> &'static str {
    match view {
        MetricView::Sum { .. } => "sum",
        MetricView::Gauge { .. } => "gauge",
        MetricView::Set { .. } => "set",
        MetricView::Histogram { .. } => "histogram",
        MetricView::Summary { .. } => "summary",
        MetricView::ExponentialHistogram { .. } => "exponential_histogram",
    }
}

impl MetricRef {
    pub fn from_otel_metric(metric: &OtelMetric) -> Self {
        let view = metric.view();
        let bounds = match &view {
            MetricView::Histogram { bounds, .. } => Some(bounds.to_vec()),
            MetricView::Summary { quantiles, .. } => {
                Some(quantiles.iter().map(|q| q.quantile).collect())
            }
            _ => None,
        };
        let mt = metric_type_tag(&view);
        let series = MetricSeries {
            name: metric.name().to_owned(),
            namespace: metric.namespace().map(str::to_owned),
            tags: metric.tags(),
        };
        Self {
            series,
            metric_type: mt,
            bounds,
        }
    }
}

impl PartialEq for MetricRef {
    fn eq(&self, other: &Self) -> bool {
        self.series == other.series
            && self.metric_type == other.metric_type
            && self.bounds == other.bounds
    }
}

impl Eq for MetricRef {}

impl Hash for MetricRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.series.hash(state);
        self.metric_type.hash(state);
        if let Some(bounds) = &self.bounds {
            for bound in bounds {
                bound.to_bits().hash(state);
            }
        }
    }
}

fn authorized<T: HttpBody>(req: &Request<T>, auth: &Option<Auth>) -> bool {
    if let Some(auth) = auth {
        let headers = req.headers();
        if let Some(auth_header) = headers.get(hyper::header::AUTHORIZATION) {
            let encoded_credentials = match auth {
                Auth::Basic { user, password } => Some(HeaderValue::from_str(
                    format!(
                        "Basic {}",
                        BASE64_STANDARD.encode(format!("{}:{}", user, password.inner()))
                    )
                    .as_str(),
                )),
                Auth::Bearer { token } => Some(HeaderValue::from_str(
                    format!("Bearer {}", token.inner()).as_str(),
                )),
                Auth::Custom { value } => Some(HeaderValue::from_str(value)),
                #[cfg(feature = "aws-core")]
                _ => None,
            };

            if let Some(Ok(encoded_credentials)) = encoded_credentials
                && auth_header == encoded_credentials
            {
                return true;
            }
        }
    } else {
        return true;
    }

    false
}

#[derive(Clone)]
struct Handler {
    auth: Option<Auth>,
    default_namespace: Option<String>,
    buckets: Box<[f64]>,
    quantiles: Box<[f64]>,
    bytes_sent: Registered<BytesSent>,
    events_sent: Registered<EventsSent>,
}

impl Handler {
    fn handle<T: HttpBody>(
        &self,
        req: Request<T>,
        metrics: &RwLock<IndexMap<MetricRef, (OtelMetric, MetricMetadata)>>,
    ) -> Response<Body> {
        let mut response = Response::new(Body::empty());

        match (authorized(&req, &self.auth), req.method(), req.uri().path()) {
            (false, _, _) => {
                *response.status_mut() = StatusCode::UNAUTHORIZED;
                response.headers_mut().insert(
                    http::header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Basic, Bearer"),
                );
            }

            (true, &Method::GET, "/metrics") => {
                let metrics = metrics.read().expect(LOCK_FAILED);

                let count = metrics.len();
                let byte_size = metrics
                    .iter()
                    .map(|(_, (otel, _))| otel.estimated_json_encoded_size_of())
                    .sum();

                let mut collector = StringCollector::new();

                for (_, (otel, _)) in metrics.iter() {
                    collector.encode_metric(
                        self.default_namespace.as_deref(),
                        &self.buckets,
                        &self.quantiles,
                        otel,
                    );
                }

                drop(metrics);

                let body = collector.finish();
                let body_size = body.size_of();

                *response.body_mut() = body.into();

                response.headers_mut().insert(
                    "Content-Type",
                    HeaderValue::from_static("text/plain; version=0.0.4"),
                );

                self.events_sent.emit(CountByteSize(count, byte_size));
                self.bytes_sent.emit(ByteSize(body_size));
            }

            (true, _, _) => {
                *response.status_mut() = StatusCode::NOT_FOUND;
            }
        }

        response
    }
}

impl PrometheusExporter {
    fn new(config: PrometheusExporterConfig) -> Self {
        Self {
            server_shutdown_trigger: None,
            config,
            metrics: Arc::new(RwLock::new(IndexMap::new())),
        }
    }

    async fn start_server_if_needed(&mut self) -> crate::Result<()> {
        if self.server_shutdown_trigger.is_some() {
            return Ok(());
        }

        let handler = Handler {
            bytes_sent: register!(BytesSent::from(Protocol::HTTP)),
            events_sent: register!(EventsSent::from(Output(None))),
            default_namespace: self.config.default_namespace.clone(),
            buckets: self.config.buckets.clone().into(),
            quantiles: self.config.quantiles.clone().into(),
            auth: self.config.auth.clone(),
        };

        let span = Span::current();
        let metrics = Arc::clone(&self.metrics);

        let new_service = make_service_fn(move |_| {
            let span = Span::current();
            let metrics = Arc::clone(&metrics);
            let handler = handler.clone();

            let inner = service_fn(move |req| {
                let response = handler.handle(req, &metrics);

                future::ok::<_, Infallible>(response)
            });

            let service = ServiceBuilder::new()
                .layer(build_http_trace_layer(span.clone()))
                .layer(CompressionLayer::new())
                .service(inner);

            async move { Ok::<_, Infallible>(service) }
        });

        let (trigger, tripwire) = Tripwire::new();

        let tls = self.config.tls.clone();
        let address = self.config.address;

        let tls = MaybeTlsSettings::from_config(tls.as_ref(), true)?;
        let listener = tls.bind(&address).await?;

        tokio::spawn(async move {
            info!(message = "Building HTTP server.", address = %address);

            Server::builder(hyper::server::accept::from_stream(listener.accept_stream()))
                .serve(new_service)
                .with_graceful_shutdown(tripwire.then(crate::shutdown::tripwire_handler))
                .instrument(span)
                .await
                .map_err(|error| error!("Server error: {}.", error))?;

            Ok::<(), ()>(())
        });

        self.server_shutdown_trigger = Some(trigger);
        Ok(())
    }

    fn normalize(&mut self, mut otel: OtelMetric) -> Option<OtelMetric> {
        otel.convert_exponential_to_histogram(&self.config.buckets);
        otel.flatten_resource_to_tags(&self.config.resource_to_labels);

        if otel.kind() == MetricKind::Incremental {
            let metrics = self.metrics.read().expect(LOCK_FAILED);
            let metric_ref = MetricRef::from_otel_metric(&otel);

            if let Some((existing, _)) = metrics.get(&metric_ref) {
                let mut accumulated = existing.clone();
                if accumulated.add(&otel) {
                    accumulated.set_kind(MetricKind::Absolute);
                    accumulated.set_timestamp(otel.timestamp());
                    return Some(accumulated);
                }
            }
        }

        otel.set_kind(MetricKind::Absolute);
        Some(otel)
    }
}

#[async_trait]
impl StreamSink<Event> for PrometheusExporter {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.start_server_if_needed()
            .await
            .map_err(|error| error!("Failed to start Prometheus exporter: {}.", error))?;

        let mut last_flush = Instant::now();
        let flush_period = self.config.flush_period_secs;

        while let Some(event) = input.next().await {
            // If we've exceed our flush interval, go through all of the metrics we're currently
            // tracking and remove any which have exceeded the flush interval in terms of not
            // having been updated within that long of a time.
            //
            // TODO: Can we be smarter about this? As is, we might wait up to 2x the flush period to
            // remove an expired metric depending on how things line up.  It'd be cool to _check_
            // for expired metrics more often, but we also don't want to check _way_ too often, like
            // every second, since then we're constantly iterating through every metric, etc etc.
            if last_flush.elapsed() > self.config.flush_period_secs {
                last_flush = Instant::now();

                let mut metrics = self.metrics.write().expect(LOCK_FAILED);

                metrics.retain(|_metric_ref, (_, metadata)| !metadata.has_expired(last_flush));
            }

            // Now process the metric we got.
            let Some(mut otel) = event.try_into_otel_metric() else {
                continue;
            };
            let finalizers = otel.take_finalizers();

            match self.normalize(otel) {
                Some(mut normalized) => {
                    if self.config.suppress_timestamp {
                        normalized.set_timestamp(None);
                    }

                    // We have a normalized metric, in absolute form.  If we're already aware of this
                    // metric, update its expiration deadline, otherwise, start tracking it.
                    let metric_ref = MetricRef::from_otel_metric(&normalized);
                    let mut metrics = self.metrics.write().expect(LOCK_FAILED);

                    match metrics.entry(metric_ref) {
                        Entry::Occupied(mut entry) => {
                            let (data, metadata) = entry.get_mut();
                            *data = normalized;
                            metadata.refresh();
                        }
                        Entry::Vacant(entry) => {
                            entry.insert((normalized, MetricMetadata::new(flush_period)));
                        }
                    }
                    finalizers.update_status(EventStatus::Delivered);
                }
                _ => {
                    emit!(PrometheusNormalizationError {});
                    finalizers.update_status(EventStatus::Errored);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use chrono::{Duration, Utc};
    use flate2::read::GzDecoder;
    use futures::stream;
    use indoc::indoc;
    use similar_asserts::assert_eq;
    use sol_lib::{
        event::OtelAttributes,
        finalization::{BatchNotifier, BatchStatus},
        otel_tags, samples,
        sensitive_string::SensitiveString,
    };
    use tokio::{sync::oneshot::error::TryRecvError, time};

    use super::*;
    use crate::{
        config::ProxyConfig,
        event::{OtelMetric, metric::MetricKind},
        http::HttpClient,
        test_util::{
            addr::next_addr,
            components::{SINK_TAGS, run_and_assert_sink_compliance},
            random_string, trace_init,
        },
        tls::MaybeTlsSettings,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<PrometheusExporterConfig>();
    }

    #[tokio::test]
    async fn prometheus_notls() {
        export_and_fetch_simple(None).await;
    }

    #[tokio::test]
    async fn prometheus_tls() {
        let mut tls_config = TlsEnableableConfig::test_config();
        tls_config.options.verify_hostname = Some(false);
        export_and_fetch_simple(Some(tls_config)).await;
    }

    #[tokio::test]
    async fn prometheus_noauth() {
        let (name1, event1) = create_metric_gauge(None, 123.4);
        let (name2, event2) = tests::create_metric_set(None, vec!["0", "1", "2"]);
        let events = vec![event1, event2];

        let response_result = export_and_fetch_with_auth(None, None, events, false).await;

        assert!(response_result.is_ok());

        let body = response_result.expect("Cannot extract body from the response");

        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{some_tag="some_value"}} 123.4
            "#},
            name = name1
        )));
        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{some_tag="some_value"}} 3
            "#},
            name = name2
        )));
    }

    #[tokio::test]
    async fn prometheus_successful_basic_auth() {
        let (name1, event1) = create_metric_gauge(None, 123.4);
        let (name2, event2) = tests::create_metric_set(None, vec!["0", "1", "2"]);
        let events = vec![event1, event2];

        let auth_config = Auth::Basic {
            user: "user".to_string(),
            password: SensitiveString::from("password".to_string()),
        };

        let response_result =
            export_and_fetch_with_auth(Some(auth_config.clone()), Some(auth_config), events, false)
                .await;

        assert!(response_result.is_ok());

        let body = response_result.expect("Cannot extract body from the response");

        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{some_tag="some_value"}} 123.4
            "#},
            name = name1
        )));
        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{some_tag="some_value"}} 3
            "#},
            name = name2
        )));
    }

    #[tokio::test]
    async fn prometheus_successful_token_auth() {
        let (name1, event1) = create_metric_gauge(None, 123.4);
        let (name2, event2) = tests::create_metric_set(None, vec!["0", "1", "2"]);
        let events = vec![event1, event2];

        let auth_config = Auth::Bearer {
            token: SensitiveString::from("token".to_string()),
        };

        let response_result =
            export_and_fetch_with_auth(Some(auth_config.clone()), Some(auth_config), events, false)
                .await;

        assert!(response_result.is_ok());

        let body = response_result.expect("Cannot extract body from the response");

        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{some_tag="some_value"}} 123.4
            "#},
            name = name1
        )));
        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{some_tag="some_value"}} 3
            "#},
            name = name2
        )));
    }

    #[tokio::test]
    async fn prometheus_missing_auth() {
        let (_, event1) = create_metric_gauge(None, 123.4);
        let (_, event2) = tests::create_metric_set(None, vec!["0", "1", "2"]);
        let events = vec![event1, event2];

        let server_auth_config = Auth::Bearer {
            token: SensitiveString::from("token".to_string()),
        };

        let response_result =
            export_and_fetch_with_auth(Some(server_auth_config), None, events, false).await;

        assert!(response_result.is_err());
        assert_eq!(response_result.unwrap_err(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn prometheus_wrong_auth() {
        let (_, event1) = create_metric_gauge(None, 123.4);
        let (_, event2) = tests::create_metric_set(None, vec!["0", "1", "2"]);
        let events = vec![event1, event2];

        let server_auth_config = Auth::Bearer {
            token: SensitiveString::from("token".to_string()),
        };

        let client_auth_config = Auth::Basic {
            user: "user".to_string(),
            password: SensitiveString::from("password".to_string()),
        };

        let response_result = export_and_fetch_with_auth(
            Some(server_auth_config),
            Some(client_auth_config),
            events,
            false,
        )
        .await;

        assert!(response_result.is_err());
        assert_eq!(response_result.unwrap_err(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn encoding_gzip() {
        let (name1, event1) = create_metric_gauge(None, 123.4);
        let events = vec![event1];

        let body_raw = export_and_fetch_raw(None, events, false, Some(String::from("gzip"))).await;
        let expected = format!(
            indoc! {r#"
                # HELP {name} {name}
                # TYPE {name} gauge
                {name}{{some_tag="some_value"}} 123.4
            "#},
            name = name1,
        );

        let mut gz = GzDecoder::new(&body_raw[..]);
        let mut body_decoded = String::new();
        let _ = gz.read_to_string(&mut body_decoded);

        assert!(body_raw.len() < expected.len());
        assert_eq!(body_decoded, expected);
    }

    #[tokio::test]
    async fn updates_timestamps() {
        let timestamp1 = Utc::now();
        let (name, event1) = create_metric_gauge(None, 123.4);
        let mut m1 = event1.into_otel_metric();
        m1.set_timestamp(Some(timestamp1));
        let event1 = Event::Metric(m1);
        let (_, event2) = create_metric_gauge(Some(name.clone()), 12.0);
        let timestamp2 = timestamp1 + Duration::seconds(1);
        let mut m2 = event2.into_otel_metric();
        m2.set_timestamp(Some(timestamp2));
        let event2 = Event::Metric(m2);
        let events = vec![event1, event2];

        let body = export_and_fetch(None, events, false).await;
        let timestamp = timestamp2.timestamp_millis();
        assert_eq!(
            body,
            format!(
                indoc! {r#"
                    # HELP {name} {name}
                    # TYPE {name} gauge
                    {name}{{some_tag="some_value"}} 135.4 {timestamp}
                "#},
                name = name,
                timestamp = timestamp
            )
        );
    }

    #[tokio::test]
    async fn suppress_timestamp() {
        let timestamp = Utc::now();
        let (name, event) = create_metric_gauge(None, 123.4);
        let mut m = event.into_otel_metric();
        m.set_timestamp(Some(timestamp));
        let events = vec![Event::Metric(m)];

        let body = export_and_fetch(None, events, true).await;
        assert_eq!(
            body,
            format!(
                indoc! {r#"
                    # HELP {name} {name}
                    # TYPE {name} gauge
                    {name}{{some_tag="some_value"}} 123.4
                "#},
                name = name,
            )
        );
    }

    /// According to the [spec](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md?plain=1#L115)
    /// > Label names MUST be unique within a LabelSet.
    /// Prometheus itself will reject the metric with an error. Largely to remain backward compatible with older versions of Vector,
    /// we only publish the last tag in the list.
    #[tokio::test]
    async fn prometheus_duplicate_labels() {
        let (name, event) = create_metric_with_tags(
            None,
            |n| OtelMetric::new_gauge_delta(n, 123.4),
            Some(otel_tags!("code" => "200", "code" => "success")),
        );
        let events = vec![event];

        let response_result = export_and_fetch_with_auth(None, None, events, false).await;

        assert!(response_result.is_ok());

        let body = response_result.expect("Cannot extract body from the response");

        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{code="success"}} 123.4
            "# },
            name = name
        )));
    }

    async fn export_and_fetch_raw(
        tls_config: Option<TlsEnableableConfig>,
        mut events: Vec<Event>,
        suppress_timestamp: bool,
        encoding: Option<String>,
    ) -> hyper::body::Bytes {
        trace_init();

        let client_settings = MaybeTlsSettings::from_config(tls_config.as_ref(), false).unwrap();
        let proto = client_settings.http_protocol_name();

        let (_guard, address) = next_addr();
        let config = PrometheusExporterConfig {
            address,
            tls: tls_config,
            suppress_timestamp,
            ..Default::default()
        };

        // Set up acknowledgement notification
        let mut receiver = BatchNotifier::apply_to(&mut events[..]);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        let (sink, _) = config.build(SinkContext::default()).await.unwrap();
        let (_, delayed_event) = create_metric_gauge(Some("delayed".to_string()), 123.4);
        let sink_handle = tokio::spawn(run_and_assert_sink_compliance(
            sink,
            stream::iter(events).chain(stream::once(async move {
                // Wait a bit to have time to scrape metrics
                time::sleep(time::Duration::from_millis(500)).await;
                delayed_event
            })),
            &SINK_TAGS,
        ));

        time::sleep(time::Duration::from_millis(100)).await;

        // Events are marked as delivered as soon as they are aggregated.
        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Delivered));

        let mut request = Request::get(format!("{proto}://{address}/metrics"))
            .body(Body::empty())
            .expect("Error creating request.");

        if let Some(ref encoding) = encoding {
            request.headers_mut().insert(
                http::header::ACCEPT_ENCODING,
                HeaderValue::from_str(encoding.as_str()).unwrap(),
            );
        }

        let proxy = ProxyConfig::default();
        let result = HttpClient::new(client_settings, &proxy)
            .unwrap()
            .send(request)
            .await
            .expect("Could not fetch query");

        assert!(result.status().is_success());

        if encoding.is_some() {
            assert!(
                result
                    .headers()
                    .contains_key(http::header::CONTENT_ENCODING)
            );
        }

        let body = result.into_body();
        let bytes = http_body::Body::collect(body)
            .await
            .expect("Reading body failed")
            .to_bytes();

        sink_handle.await.unwrap();

        bytes
    }

    async fn export_and_fetch(
        tls_config: Option<TlsEnableableConfig>,
        events: Vec<Event>,
        suppress_timestamp: bool,
    ) -> String {
        let bytes = export_and_fetch_raw(tls_config, events, suppress_timestamp, None);
        String::from_utf8(bytes.await.to_vec()).unwrap()
    }

    async fn export_and_fetch_with_auth(
        server_auth_config: Option<Auth>,
        client_auth_config: Option<Auth>,
        mut events: Vec<Event>,
        suppress_timestamp: bool,
    ) -> Result<String, http::status::StatusCode> {
        trace_init();

        let client_settings = MaybeTlsSettings::from_config(None, false).unwrap();
        let proto = client_settings.http_protocol_name();

        let (_guard, address) = next_addr();
        let config = PrometheusExporterConfig {
            address,
            auth: server_auth_config,
            tls: None,
            suppress_timestamp,
            ..Default::default()
        };

        // Set up acknowledgement notification
        let mut receiver = BatchNotifier::apply_to(&mut events[..]);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));

        let (sink, _) = config.build(SinkContext::default()).await.unwrap();
        let (_, delayed_event) = create_metric_gauge(Some("delayed".to_string()), 123.4);
        let sink_handle = tokio::spawn(run_and_assert_sink_compliance(
            sink,
            stream::iter(events).chain(stream::once(async move {
                // Wait a bit to have time to scrape metrics
                time::sleep(time::Duration::from_millis(500)).await;
                delayed_event
            })),
            &SINK_TAGS,
        ));

        time::sleep(time::Duration::from_millis(100)).await;

        // Events are marked as delivered as soon as they are aggregated.
        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Delivered));

        let mut request = Request::get(format!("{proto}://{address}/metrics"))
            .body(Body::empty())
            .expect("Error creating request.");

        if let Some(client_auth_config) = client_auth_config {
            client_auth_config.apply(&mut request);
        }

        let proxy = ProxyConfig::default();
        let result = HttpClient::new(client_settings, &proxy)
            .unwrap()
            .send(request)
            .await
            .expect("Could not fetch query");

        if !result.status().is_success() {
            return Err(result.status());
        }

        let body = result.into_body();
        let bytes = http_body::Body::collect(body)
            .await
            .expect("Reading body failed")
            .to_bytes();
        let result = String::from_utf8(bytes.to_vec()).unwrap();

        sink_handle.await.unwrap();

        Ok(result)
    }

    async fn export_and_fetch_simple(tls_config: Option<TlsEnableableConfig>) {
        let (name1, event1) = create_metric_gauge(None, 123.4);
        let (name2, event2) = tests::create_metric_set(None, vec!["0", "1", "2"]);
        let events = vec![event1, event2];

        let body = export_and_fetch(tls_config, events, false).await;

        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{some_tag="some_value"}} 123.4
            "#},
            name = name1
        )));
        assert!(body.contains(&format!(
            indoc! {r#"
               # HELP {name} {name}
               # TYPE {name} gauge
               {name}{{some_tag="some_value"}} 3
            "#},
            name = name2
        )));
    }

    pub fn create_metric_gauge(name: Option<String>, value: f64) -> (String, Event) {
        let name = name.unwrap_or_else(|| format!("vector_set_{}", random_string(16)));
        let otel = OtelMetric::new_gauge_delta(&name, value)
            .with_tags(Some(otel_tags!("some_tag" => "some_value")));
        (name, Event::Metric(otel))
    }

    pub fn create_metric_set(name: Option<String>, values: Vec<&'static str>) -> (String, Event) {
        let name = name.unwrap_or_else(|| format!("vector_set_{}", random_string(16)));
        let otel = OtelMetric::new_set_from_values(&name, MetricKind::Incremental, values)
            .with_tags(Some(otel_tags!("some_tag" => "some_value")));
        (name, Event::Metric(otel))
    }

    fn create_metric_with_tags(
        name: Option<String>,
        make_metric: impl FnOnce(&str) -> OtelMetric,
        tags: Option<OtelAttributes>,
    ) -> (String, Event) {
        let name = name.unwrap_or_else(|| format!("vector_set_{}", random_string(16)));
        let otel = make_metric(&name).with_tags(tags);
        (name, Event::Metric(otel))
    }

    #[tokio::test]
    async fn sink_absolute() {
        let (_guard, address) = next_addr();
        let config = PrometheusExporterConfig {
            address,
            tls: None,
            ..Default::default()
        };

        let sink = PrometheusExporter::new(config);

        let otel_m1 = OtelMetric::new_counter("absolute", MetricKind::Absolute, 32.)
            .with_tags(Some(otel_tags!("tag1" => "value1")));
        let otel_m2 = OtelMetric::new_counter("absolute", MetricKind::Absolute, 33.)
            .with_tags(Some(otel_tags!("tag1" => "value2")));

        let events = vec![
            Event::Metric(
                OtelMetric::new_counter("absolute", MetricKind::Absolute, 32.)
                    .with_tags(Some(otel_tags!("tag1" => "value1"))),
            ),
            Event::Metric(
                OtelMetric::new_counter("absolute", MetricKind::Absolute, 33.)
                    .with_tags(Some(otel_tags!("tag1" => "value2"))),
            ),
            Event::Metric(
                OtelMetric::new_counter("absolute", MetricKind::Absolute, 40.)
                    .with_tags(Some(otel_tags!("tag1" => "value1"))),
            ),
        ];

        let metrics_handle = Arc::clone(&sink.metrics);

        let sink = VectorSink::from_event_streamsink(sink);
        let input_events = stream::iter(events).map(Into::into);
        sink.run(input_events).await.unwrap();

        let metrics_after = metrics_handle.read().unwrap();

        let actual_m1 = metrics_after
            .get(&MetricRef::from_otel_metric(&otel_m1))
            .expect("m1 should exist");
        assert!(matches!(actual_m1.0.view(), MetricView::Sum { value } if value == 40.));

        let actual_m2 = metrics_after
            .get(&MetricRef::from_otel_metric(&otel_m2))
            .expect("m2 should exist");
        assert!(matches!(actual_m2.0.view(), MetricView::Sum { value } if value == 33.));
    }

    #[tokio::test]
    async fn sink_histograms_accumulate() {
        use crate::event::metric::{Sample, samples_to_buckets};

        let (_guard, address) = next_addr();
        let config = PrometheusExporterConfig {
            address,
            tls: None,
            ..Default::default()
        };
        let bucket_bounds = config.buckets.clone();

        let sink = PrometheusExporter::new(config);

        let samples_batches: Vec<Vec<Sample>> = vec![
            samples!(1.0 => 1, 3.0 => 2),
            samples!(1.0 => 2, 2.9 => 1),
            samples!(1.0 => 4, 3.2 => 1),
        ];

        let all_samples: Vec<Sample> = samples_batches.iter().flatten().cloned().collect();
        let (expected_buckets, expected_count, expected_sum) =
            samples_to_buckets(&all_samples, &bucket_bounds);

        let metrics_handle = Arc::clone(&sink.metrics);

        let events: Vec<Event> = samples_batches
            .iter()
            .map(|s| {
                let (buckets, count, sum) = samples_to_buckets(s, &bucket_bounds);
                Event::Metric(OtelMetric::new_histogram(
                    "my_histo",
                    MetricKind::Incremental,
                    &buckets,
                    count,
                    sum,
                ))
            })
            .collect();

        let sink = VectorSink::from_event_streamsink(sink);
        let input_events = stream::iter(events).map(Into::into);
        sink.run(input_events).await.unwrap();

        let metrics_after = metrics_handle.read().unwrap();
        assert_eq!(metrics_after.len(), 1);

        let expected_otel = OtelMetric::new_histogram(
            "my_histo",
            MetricKind::Absolute,
            &expected_buckets,
            expected_count,
            expected_sum,
        );
        let actual = metrics_after
            .get(&MetricRef::from_otel_metric(&expected_otel))
            .expect("histogram metric should exist");
        match actual.0.view() {
            MetricView::Histogram { count, sum, .. } => {
                assert_eq!(count, expected_count);
                assert!(
                    (sum - expected_sum).abs() < 1e-10,
                    "sum mismatch: {sum} vs {expected_sum}"
                );
            }
            other => panic!("expected Histogram view, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn sink_gauge_incremental_absolute_mix() {
        let (_guard, address) = next_addr();
        let config = PrometheusExporterConfig {
            address,
            tls: None,
            ..Default::default()
        };

        let sink = PrometheusExporter::new(config);

        let events = vec![
            Event::Metric(OtelMetric::new_gauge("gauge", 100.0)),
            Event::Metric(OtelMetric::new_gauge("gauge", 333.0)),
            Event::Metric(OtelMetric::new_gauge_delta("gauge", -10.0)),
            Event::Metric(OtelMetric::new_gauge_delta("gauge", 4.0)),
        ];

        let metrics_handle = Arc::clone(&sink.metrics);

        let sink = VectorSink::from_event_streamsink(sink);
        let input_events = stream::iter(events).map(Into::into);
        sink.run(input_events).await.unwrap();

        let metrics_after = metrics_handle.read().unwrap();
        assert_eq!(metrics_after.len(), 1);

        let expected_gauge = OtelMetric::new_gauge("gauge", 327.0);
        let actual_gauge = metrics_after
            .get(&MetricRef::from_otel_metric(&expected_gauge))
            .expect("gauge metric should exist");
        assert!(matches!(actual_gauge.0.view(), MetricView::Gauge { value } if value == 327.0));
    }
}

#[cfg(all(test, feature = "prometheus-integration-tests"))]
mod integration_tests {
    #![allow(clippy::print_stdout)] // tests
    #![allow(clippy::print_stderr)] // tests
    #![allow(clippy::dbg_macro)] // tests

    use chrono::Utc;
    use futures::{future::ready, stream};
    use serde_json::Value;
    use tokio::{sync::mpsc, time};
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use super::*;
    use crate::{
        config::ProxyConfig,
        http::HttpClient,
        test_util::{
            components::{SINK_TAGS, run_and_assert_sink_compliance},
            trace_init,
        },
    };

    fn sink_exporter_address() -> String {
        std::env::var("SINK_EXPORTER_ADDRESS").unwrap_or_else(|_| "127.0.0.1:9101".into())
    }

    fn prometheus_address() -> String {
        std::env::var("PROMETHEUS_ADDRESS").unwrap_or_else(|_| "localhost:9090".into())
    }

    async fn fetch_exporter_body() -> String {
        let url = format!("http://{}/metrics", sink_exporter_address());
        let request = Request::get(url)
            .body(Body::empty())
            .expect("Error creating request.");
        let proxy = ProxyConfig::default();
        let result = HttpClient::new(None, &proxy)
            .unwrap()
            .send(request)
            .await
            .expect("Could not send request");
        let result = http_body::Body::collect(result.into_body())
            .await
            .expect("Error fetching body")
            .to_bytes();
        String::from_utf8_lossy(&result).to_string()
    }

    async fn prometheus_query(query: &str) -> Value {
        let url = format!(
            "http://{}/api/v1/query?query={}",
            prometheus_address(),
            query
        );
        let request = Request::post(url)
            .body(Body::empty())
            .expect("Error creating request.");
        let proxy = ProxyConfig::default();
        let result = HttpClient::new(None, &proxy)
            .unwrap()
            .send(request)
            .await
            .expect("Could not fetch query");
        let result = http_body::Body::collect(result.into_body())
            .await
            .expect("Error fetching body")
            .to_bytes();
        let result = String::from_utf8_lossy(&result);
        serde_json::from_str(result.as_ref()).expect("Invalid JSON from prometheus")
    }

    #[tokio::test]
    async fn prometheus_metrics() {
        trace_init();

        prometheus_scrapes_metrics().await;
        time::sleep(time::Duration::from_millis(500)).await;
        reset_on_flush_period().await;
        expire_on_flush_period().await;
    }

    async fn prometheus_scrapes_metrics() {
        let start = Utc::now().timestamp();

        let config = PrometheusExporterConfig {
            address: sink_exporter_address().parse().unwrap(),
            flush_period_secs: Duration::from_secs(2),
            ..Default::default()
        };
        let (sink, _) = config.build(SinkContext::default()).await.unwrap();
        let (name, event) = tests::create_metric_gauge(None, 123.4);
        let (_, delayed_event) = tests::create_metric_gauge(Some("delayed".to_string()), 123.4);

        run_and_assert_sink_compliance(
            sink,
            stream::once(ready(event)).chain(stream::once(async move {
                // Wait a bit for the prometheus server to scrape the metrics
                time::sleep(time::Duration::from_secs(2)).await;
                delayed_event
            })),
            &SINK_TAGS,
        )
        .await;

        // Now try to download them from prometheus
        let result = prometheus_query(&name).await;

        let data = &result["data"]["result"][0];
        assert_eq!(data["metric"]["__name__"], Value::String(name));
        assert_eq!(
            data["metric"]["instance"],
            Value::String(sink_exporter_address())
        );
        assert_eq!(
            data["metric"]["some_tag"],
            Value::String("some_value".into())
        );
        #[allow(clippy::cast_precision_loss)]
        let start_f64 = start as f64;
        assert!(data["value"][0].as_f64().unwrap() >= start_f64);
        assert_eq!(data["value"][1], Value::String("123.4".into()));
    }

    async fn reset_on_flush_period() {
        let config = PrometheusExporterConfig {
            address: sink_exporter_address().parse().unwrap(),
            flush_period_secs: Duration::from_secs(3),
            ..Default::default()
        };
        let (sink, _) = config.build(SinkContext::default()).await.unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let input_events = UnboundedReceiverStream::new(rx);

        let input_events = input_events.map(Into::into);
        let sink_handle = tokio::spawn(async move { sink.run(input_events).await.unwrap() });

        // Create two sets with different names but the same size.
        let (name1, event) = tests::create_metric_set(None, vec!["0", "1", "2"]);
        tx.send(event).expect("Failed to send.");
        let (name2, event) = tests::create_metric_set(None, vec!["3", "4", "5"]);
        tx.send(event).expect("Failed to send.");

        // Wait for the Prometheus server to scrape them, and then query it to ensure both metrics
        // have their correct set size value.
        time::sleep(time::Duration::from_secs(2)).await;

        // Now query Prometheus to make sure we see them there.
        let result = prometheus_query(&name1).await;
        assert_eq!(
            result["data"]["result"][0]["value"][1],
            Value::String("3".into())
        );
        let result = prometheus_query(&name2).await;
        assert_eq!(
            result["data"]["result"][0]["value"][1],
            Value::String("3".into())
        );

        // Wait a few more seconds to ensure that the two original sets have logically expired.
        // We'll update `name2` but not `name1`, which should lead to both being expired, but
        // `name2` being recreated with two values only, while `name1` is entirely gone.
        time::sleep(time::Duration::from_secs(3)).await;

        let (name2, event) = tests::create_metric_set(Some(name2), vec!["8", "9"]);
        tx.send(event).expect("Failed to send.");

        // Again, wait for the Prometheus server to scrape the metrics, and then query it again.
        time::sleep(time::Duration::from_secs(2)).await;
        let result = prometheus_query(&name1).await;
        assert_eq!(result["data"]["result"][0]["value"][1], Value::Null);
        let result = prometheus_query(&name2).await;
        assert_eq!(
            result["data"]["result"][0]["value"][1],
            Value::String("2".into())
        );

        drop(tx);
        sink_handle.await.unwrap();
    }

    async fn expire_on_flush_period() {
        let config = PrometheusExporterConfig {
            address: sink_exporter_address().parse().unwrap(),
            flush_period_secs: Duration::from_secs(3),
            ..Default::default()
        };
        let (sink, _) = config.build(SinkContext::default()).await.unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let input_events = UnboundedReceiverStream::new(rx);

        let input_events = input_events.map(Into::into);
        let sink_handle = tokio::spawn(async move { sink.run(input_events).await.unwrap() });

        // metrics that will not be updated for a full flush period and therefore should expire
        let (name1, event) = tests::create_metric_set(None, vec!["42"]);
        tx.send(event).expect("Failed to send.");
        let (name2, event) = tests::create_metric_gauge(None, 100.0);
        tx.send(event).expect("Failed to send.");

        // Wait a bit for the sink to process the events
        time::sleep(time::Duration::from_secs(1)).await;

        // Exporter should present both metrics at first
        let body = fetch_exporter_body().await;
        assert!(body.contains(&name1));
        assert!(body.contains(&name2));

        // Wait long enough to put us past flush_period_secs for the metric that wasn't updated
        for _ in 0..7 {
            // Update the first metric, ensuring it doesn't expire
            let (_, event) = tests::create_metric_set(Some(name1.clone()), vec!["43"]);
            tx.send(event).expect("Failed to send.");

            // Wait a bit for time to pass
            time::sleep(time::Duration::from_secs(1)).await;
        }

        // Exporter should present only the one that got updated
        let body = fetch_exporter_body().await;
        assert!(body.contains(&name1));
        assert!(!body.contains(&name2));

        drop(tx);
        sink_handle.await.unwrap();
    }
}
