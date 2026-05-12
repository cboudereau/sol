use futures::TryFutureExt;
use sol_lib::{
    EstimatedJsonEncodedSizeOf,
    event::{BatchNotifier, BatchStatus, BatchStatusReceiver, Event},
    internal_event::{ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Registered},
    opentelemetry::{
        logs::resource_logs_into_events, metrics::resource_metrics_into_events,
        spans::resource_spans_into_events,
        upstream_opentelemetry_proto::tonic::collector::{
            logs::v1::{
                ExportLogsServiceRequest, ExportLogsServiceResponse,
                logs_service_server::LogsService,
            },
            metrics::v1::{
                ExportMetricsServiceRequest, ExportMetricsServiceResponse,
                metrics_service_server::MetricsService,
            },
            trace::v1::{
                ExportTraceServiceRequest, ExportTraceServiceResponse,
                trace_service_server::TraceService,
            },
        },
    },
};
use tonic::{Request, Response, Status};

use crate::{
    SharedSourceSender,
    internal_events::{EventsReceived, StreamClosedError},
};

pub const LOGS: &str = "logs";
pub const METRICS: &str = "metrics";
pub const TRACES: &str = "traces";

#[derive(Clone)]
pub(crate) struct Service {
    pub pipeline: SharedSourceSender,
    pub acknowledgements: bool,
    pub events_received: Registered<EventsReceived>,
    pub bytes_received: Registered<BytesReceived>,
}

#[tonic::async_trait]
impl TraceService for Service {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let events: Vec<Event> = request
            .into_inner()
            .resource_spans
            .into_iter()
            .flat_map(resource_spans_into_events)
            .collect();
        self.handle_events(events, TRACES).await?;

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl LogsService for Service {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let events: Vec<Event> = request
            .into_inner()
            .resource_logs
            .into_iter()
            .flat_map(resource_logs_into_events)
            .collect();
        self.handle_events(events, LOGS).await?;

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for Service {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let events: Vec<Event> = request
            .into_inner()
            .resource_metrics
            .into_iter()
            .flat_map(resource_metrics_into_events)
            .collect();
        self.handle_events(events, METRICS).await?;

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

impl Service {
    async fn handle_events(
        &self,
        mut events: Vec<Event>,
        log_name: &'static str,
    ) -> Result<(), Status> {
        let count = events.len();
        let byte_size = events.estimated_json_encoded_size_of();
        self.events_received.emit(CountByteSize(count, byte_size));
        self.bytes_received.emit(ByteSize(byte_size.get()));

        let receiver = BatchNotifier::maybe_apply_to(self.acknowledgements, &mut events);

        self.pipeline
            .send_batch_named(log_name, events)
            .map_err(|error| {
                let message = error.to_string();
                emit!(StreamClosedError { count });
                Status::unavailable(message)
            })
            .and_then(|_| handle_batch_status(receiver))
            .await?;
        Ok(())
    }
}

async fn handle_batch_status(receiver: Option<BatchStatusReceiver>) -> Result<(), Status> {
    let status = match receiver {
        Some(receiver) => receiver.await,
        None => BatchStatus::Delivered,
    };

    match status {
        BatchStatus::Errored => Err(Status::internal("Delivery error")),
        BatchStatus::Rejected => Err(Status::data_loss("Delivery failed")),
        BatchStatus::Delivered => Ok(()),
    }
}
