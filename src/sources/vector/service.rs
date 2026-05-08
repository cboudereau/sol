//! gRPC service implementation for the native Vector protocol.
//!
//! This allows legacy Vector instances to push events to Sol using
//! the original `vector` sink / `vector` source protocol.

use futures::TryFutureExt;
use sol_lib::{
    EstimatedJsonEncodedSizeOf,
    event::{BatchNotifier, BatchStatus, BatchStatusReceiver, Event},
    internal_event::{CountByteSize, InternalEventHandle as _, Registered},
};
use tonic::{Request, Response, Status};

use crate::{
    SourceSender,
    internal_events::{EventsReceived, StreamClosedError},
};

use super::convert::convert_event;
use super::proto::vector::{
    HealthCheckRequest, HealthCheckResponse, PushEventsRequest, PushEventsResponse, ServingStatus,
    vector_server::Vector,
};

#[derive(Clone)]
pub struct NativeVectorService {
    pub pipeline: SourceSender,
    pub acknowledgements: bool,
    pub events_received: Registered<EventsReceived>,
}

#[tonic::async_trait]
impl Vector for NativeVectorService {
    async fn push_events(
        &self,
        request: Request<PushEventsRequest>,
    ) -> Result<Response<PushEventsResponse>, Status> {
        let mut events: Vec<Event> = request
            .into_inner()
            .events
            .into_iter()
            .filter_map(convert_event)
            .collect();

        let count = events.len();
        let byte_size = events.estimated_json_encoded_size_of();
        self.events_received.emit(CountByteSize(count, byte_size));

        let receiver = BatchNotifier::maybe_apply_to(self.acknowledgements, &mut events);

        self.pipeline
            .clone()
            .send_batch(events)
            .map_err(|error| {
                let message = error.to_string();
                emit!(StreamClosedError { count });
                Status::unavailable(message)
            })
            .and_then(|_| handle_batch_status(receiver))
            .await?;

        Ok(Response::new(PushEventsResponse {}))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
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
