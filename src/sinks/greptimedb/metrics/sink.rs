use std::future::ready;

use async_trait::async_trait;
use futures::StreamExt;
use futures_util::stream::BoxStream;
use sol_lib::event::OtelMetric;

use crate::sinks::{
    greptimedb::metrics::{
        batch::GreptimeDBBatchSizer,
        request::{GreptimeDBGrpcRequest, GreptimeDBGrpcRetryLogic},
        request_builder::RequestBuilderOptions,
        service::GreptimeDBGrpcService,
    },
    prelude::*,
    util::buffer::metrics::{MetricNormalize, MetricNormalizer, MetricSet},
};

#[derive(Clone, Debug, Default)]
pub struct GreptimeDBMetricNormalize;

impl MetricNormalize for GreptimeDBMetricNormalize {
    fn normalize(&mut self, state: &mut MetricSet, metric: OtelMetric) -> Option<OtelMetric> {
        if metric.is_sum() || (metric.is_gauge() && !metric.is_set()) {
            state.make_absolute(metric)
        } else {
            // All others are left as-is
            Some(metric)
        }
    }
}

/// GreptimeDBGrpcSink is a sink that sends metrics to GreptimeDB via gRPC.
/// It uses the `GreptimeDBGrpcService` to send the metrics.
pub struct GreptimeDBGrpcSink {
    pub(super) service: Svc<GreptimeDBGrpcService, GreptimeDBGrpcRetryLogic>,
    pub(super) batch_settings: BatcherSettings,
    pub(super) request_builder_options: RequestBuilderOptions,
}

impl GreptimeDBGrpcSink {
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let mut normalizer = MetricNormalizer::<GreptimeDBMetricNormalize>::default();
        input
            .filter_map(move |event| {
                ready(
                    event
                        .try_into_otel_metric()
                        .and_then(|m| normalizer.normalize_otel(m))
                        .and_then(|e| e.try_into_otel_metric()),
                )
            })
            .batched(
                self.batch_settings
                    .as_item_size_config(GreptimeDBBatchSizer),
            )
            .map(|m| GreptimeDBGrpcRequest::from_metrics(m, &self.request_builder_options))
            .into_driver(self.service)
            .protocol("grpc")
            .run()
            .await
    }
}

#[async_trait]
impl StreamSink<Event> for GreptimeDBGrpcSink {
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
