use std::{fmt, future::ready};

use async_trait::async_trait;
use futures_util::{
    StreamExt,
    stream::{self, BoxStream},
};
use sol_lib::{
    event::Event,
    internal_event::Protocol,
    sink::StreamSink,
    stream::{BatcherSettings, DriverResponse},
};
use tower::Service;

use super::{
    batch::StatsdBatchSizer, normalizer::StatsdNormalizer, request_builder::StatsdRequestBuilder,
    service::StatsdRequest,
};
use crate::sinks::util::{SinkBuilderExt, buffer::metrics::MetricNormalizer};

pub(crate) struct StatsdSink<S> {
    service: S,
    batch_settings: BatcherSettings,
    request_builder: StatsdRequestBuilder,
    protocol: Protocol,
    pub(crate) resource_to_tags: Vec<String>,
}

impl<S> StatsdSink<S>
where
    S: Service<StatsdRequest> + Send,
    S::Error: fmt::Debug + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse,
{
    /// Creates a new `StatsdSink`.
    pub const fn new(
        service: S,
        batch_settings: BatcherSettings,
        request_builder: StatsdRequestBuilder,
        protocol: Protocol,
        resource_to_tags: Vec<String>,
    ) -> Self {
        Self {
            service,
            batch_settings,
            request_builder,
            protocol,
            resource_to_tags,
        }
    }

    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let mut normalizer = MetricNormalizer::<StatsdNormalizer>::default();
        let resource_to_tags = self.resource_to_tags.clone();
        input
            .filter_map(move |event| {
                ready(match event {
                    Event::Metric(otel) => normalizer
                        .normalize_otel(otel)
                        .and_then(|e| e.try_into_otel_metric()),
                    _ => None,
                })
            })
            .map(move |mut otel| {
                otel.flatten_resource_to_tags(&resource_to_tags);
                otel
            })
            .batched(self.batch_settings.as_item_size_config(StatsdBatchSizer))
            // We build our requests "incrementally", which means that for a single batch of
            // metrics, we might generate N requests to represent all of the metrics in the batch.
            //
            // We do this as for different socket modes, there are optimal request sizes to use to
            // ensure the highest rate of delivery, such as staying within the MTU for UDP, etc.
            .incremental_request_builder(self.request_builder)
            // This unrolls the vector of request results that our request builder generates.
            .flat_map(stream::iter)
            // Generating requests _cannot_ fail, so we just unwrap our built requests.
            .unwrap_infallible()
            // Finally, we generate the driver which will take our requests, send them off, and appropriately handle
            // finalization of the events, and logging/metrics, as the requests are responded to.
            .into_driver(self.service)
            .protocol(self.protocol)
            .run()
            .await
    }
}

#[async_trait]
impl<S> StreamSink<Event> for StatsdSink<S>
where
    S: Service<StatsdRequest> + Send,
    S::Error: fmt::Debug + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        // Rust has issues with lifetimes and generics, which `async_trait` exacerbates, so we write
        // a normal async fn in `StatsdSink` itself, and then call out to it from this trait
        // implementation, which makes the compiler happy.
        self.run_inner(input).await
    }
}
