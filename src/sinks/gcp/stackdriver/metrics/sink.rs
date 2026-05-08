use sol_lib::event::{MetricView, OtelMetric};

use super::request_builder::StackdriverMetricsRequestBuilder;
use crate::sinks::{
    prelude::*,
    util::buffer::metrics::MetricNormalizer,
    util::{
        buffer::metrics::{MetricNormalize, MetricSet},
        http::HttpRequest,
    },
};

#[derive(Clone, Debug, Default)]
struct StackdriverMetricsNormalize;

impl MetricNormalize for StackdriverMetricsNormalize {
    fn normalize(&mut self, state: &mut MetricSet, metric: OtelMetric) -> Option<OtelMetric> {
        if metric.is_sum() || (metric.is_gauge() && !metric.is_set()) {
            state.make_absolute(metric)
        } else {
            // All others are left as-is
            Some(metric)
        }
    }

    fn exp_hist_bounds(&self) -> Option<&[f64]> {
        use crate::sinks::util::buffer::metrics::DEFAULT_HISTOGRAM_BOUNDS;
        Some(DEFAULT_HISTOGRAM_BOUNDS)
    }
}

pub(super) struct StackdriverMetricsSink<S> {
    service: S,
    batch_settings: BatcherSettings,
    request_builder: StackdriverMetricsRequestBuilder,
}

impl<S> StackdriverMetricsSink<S>
where
    S: Service<HttpRequest<()>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    /// Creates a new `StackdriverMetricsSink`.
    pub(super) const fn new(
        service: S,
        batch_settings: BatcherSettings,
        request_builder: StackdriverMetricsRequestBuilder,
    ) -> Self {
        Self {
            service,
            batch_settings,
            request_builder,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let mut normalizer = MetricNormalizer::<StackdriverMetricsNormalize>::default();
        input
            .filter_map(move |event| {
                let Some(otel) = event.try_into_otel_metric() else {
                    return future::ready(None);
                };

                // Filter unsupported types before normalization
                future::ready(match otel.view() {
                    MetricView::Sum { .. } | MetricView::Gauge { .. } => normalizer
                        .normalize_otel(otel)
                        .and_then(|e| e.try_into_otel_metric()),
                    not_supported => {
                        warn!("Unsupported metric type: {:?}.", not_supported);
                        None
                    }
                })
            })
            .batched(self.batch_settings.as_byte_size_config())
            .request_builder(
                default_request_builder_concurrency_limit(),
                self.request_builder,
            )
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait::async_trait]
impl<S> StreamSink<Event> for StackdriverMetricsSink<S>
where
    S: Service<HttpRequest<()>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: std::fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(
        self: Box<Self>,
        input: futures_util::stream::BoxStream<'_, Event>,
    ) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
