use std::time::Duration;

use http::response::Response;
use metrics::{counter, histogram};
use sol_lib::NamedInternalEvent;
use sol_lib::internal_event::InternalEvent;
use tonic::Code;

const GRPC_STATUS_LABEL: &str = "grpc_status";

#[derive(Debug, NamedInternalEvent)]
pub struct GrpcServerRequestReceived;

impl InternalEvent for GrpcServerRequestReceived {
    fn emit(self) {
        counter!("grpc_server_messages_received_total").increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct GrpcServerResponseSent<'a, B> {
    pub response: &'a Response<B>,
    pub latency: Duration,
}

impl<B> InternalEvent for GrpcServerResponseSent<'_, B> {
    fn emit(self) {
        let grpc_code = self
            .response
            .headers()
            .get("grpc-status")
            // The header value is missing on success.
            .map_or(tonic::Code::Ok, |v| tonic::Code::from_bytes(v.as_bytes()));
        let grpc_code = grpc_code_to_name(grpc_code);

        let labels = &[(GRPC_STATUS_LABEL, grpc_code)];
        counter!("grpc_server_messages_sent_total", labels).increment(1);
        histogram!("grpc_server_handler_duration_seconds", labels).record(self.latency);
    }
}

const fn grpc_code_to_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "Ok",
        Code::Cancelled => "Cancelled",
        Code::Unknown => "Unknown",
        Code::InvalidArgument => "InvalidArgument",
        Code::DeadlineExceeded => "DeadlineExceeded",
        Code::NotFound => "NotFound",
        Code::AlreadyExists => "AlreadyExists",
        Code::PermissionDenied => "PermissionDenied",
        Code::ResourceExhausted => "ResourceExhausted",
        Code::FailedPrecondition => "FailedPrecondition",
        Code::Aborted => "Aborted",
        Code::OutOfRange => "OutOfRange",
        Code::Unimplemented => "Unimplemented",
        Code::Internal => "Internal",
        Code::Unavailable => "Unavailable",
        Code::DataLoss => "DataLoss",
        Code::Unauthenticated => "Unauthenticated",
    }
}
