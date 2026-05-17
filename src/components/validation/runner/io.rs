use sol_lib::{
    event::Event,
    opentelemetry::upstream_opentelemetry_proto::tonic::collector::{
        logs::v1::{
            ExportLogsServiceRequest, ExportLogsServiceResponse,
            logs_service_client::LogsServiceClient, logs_service_server::LogsService,
            logs_service_server::LogsServiceServer,
        },
        metrics::v1::{
            ExportMetricsServiceRequest, ExportMetricsServiceResponse,
            metrics_service_client::MetricsServiceClient, metrics_service_server::MetricsService,
            metrics_service_server::MetricsServiceServer,
        },
        trace::v1::{
            ExportTraceServiceRequest, ExportTraceServiceResponse,
            trace_service_client::TraceServiceClient, trace_service_server::TraceService,
            trace_service_server::TraceServiceServer,
        },
    },
    opentelemetry::{
        logs::resource_logs_into_events, metrics::resource_metrics_into_events,
        spans::resource_spans_into_events,
    },
    shutdown::ShutdownSignal,
    tls::MaybeTlsSettings,
};
use tokio::{pin, select, sync::mpsc};
use tonic_0_12::{
    Request, Response, Status,
    codec::CompressionEncoding,
    transport::{Channel, Endpoint},
};

use crate::{
    components::validation::{
        TestEvent,
        sync::{Configuring, TaskCoordinator},
        util::GrpcAddress,
    },
    sinks::opentelemetry::grpc::{
        otel_log_event_to_resource_logs, otel_metric_event_to_resource_metrics,
        otel_span_event_to_resource_spans,
    },
    sources::util::grpc::{run_grpc_server_with_routes, tonic_0_12_adapter},
};

tonic_0_12_adapter!(LogsServiceAdapter, "opentelemetry.proto.collector.logs.v1.LogsService");
tonic_0_12_adapter!(MetricsServiceAdapter, "opentelemetry.proto.collector.metrics.v1.MetricsService");
tonic_0_12_adapter!(TraceServiceAdapter, "opentelemetry.proto.collector.trace.v1.TraceService");

#[derive(Clone)]
pub struct EventForwardService {
    tx: mpsc::Sender<Vec<Event>>,
}

impl From<mpsc::Sender<Vec<Event>>> for EventForwardService {
    fn from(tx: mpsc::Sender<Vec<Event>>) -> Self {
        Self { tx }
    }
}

#[tonic_0_12::async_trait]
impl LogsService for EventForwardService {
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
        self.tx
            .send(events)
            .await
            .expect("event forward rx should not close first");
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic_0_12::async_trait]
impl MetricsService for EventForwardService {
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
        self.tx
            .send(events)
            .await
            .expect("event forward rx should not close first");
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic_0_12::async_trait]
impl TraceService for EventForwardService {
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
        self.tx
            .send(events)
            .await
            .expect("event forward rx should not close first");
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

pub struct InputEdge {
    logs_client: LogsServiceClient<Channel>,
    metrics_client: MetricsServiceClient<Channel>,
    trace_client: TraceServiceClient<Channel>,
}

pub struct OutputEdge {
    listen_addr: GrpcAddress,
    service: EventForwardService,
    rx: mpsc::Receiver<Vec<Event>>,
}

impl InputEdge {
    pub fn from_address(address: GrpcAddress) -> Self {
        let uri: http::Uri = address.as_uri().to_string().parse().expect("valid URI");
        let channel = Endpoint::from(uri).connect_lazy();
        Self {
            logs_client: LogsServiceClient::new(channel.clone()),
            metrics_client: MetricsServiceClient::new(channel.clone()),
            trace_client: TraceServiceClient::new(channel),
        }
    }

    pub fn spawn_input_client(
        self,
        task_coordinator: &TaskCoordinator<Configuring>,
    ) -> mpsc::Sender<TestEvent> {
        let (tx, mut rx) = mpsc::channel::<TestEvent>(1024);
        let started = task_coordinator.track_started();
        let completed = task_coordinator.track_completed();

        tokio::spawn(async move {
            let mut logs_client = self.logs_client;
            let mut metrics_client = self.metrics_client;
            let mut trace_client = self.trace_client;

            started.mark_as_done();

            while let Some(test_event) = rx.recv().await {
                let event = test_event.into_event();
                let result = match &event {
                    Event::Log(log) => {
                        let request = ExportLogsServiceRequest {
                            resource_logs: vec![otel_log_event_to_resource_logs(log)],
                        };
                        logs_client.export(request).await.map(|_| ())
                    }
                    Event::Metric(metric) => {
                        let request = ExportMetricsServiceRequest {
                            resource_metrics: vec![otel_metric_event_to_resource_metrics(metric)],
                        };
                        metrics_client.export(request).await.map(|_| ())
                    }
                    Event::Trace(span) => {
                        let request = ExportTraceServiceRequest {
                            resource_spans: vec![otel_span_event_to_resource_spans(span)],
                        };
                        trace_client.export(request).await.map(|_| ())
                    }
                };

                if let Err(e) = result {
                    error!(error = ?e, "Failed to send input event to controlled input edge.");
                }
            }

            completed.mark_as_done();
        });

        tx
    }
}

impl OutputEdge {
    pub fn from_address(listen_addr: GrpcAddress) -> Self {
        let (tx, rx) = mpsc::channel(1024);

        Self {
            listen_addr,
            service: EventForwardService::from(tx),
            rx,
        }
    }

    pub fn spawn_output_server(
        self,
        task_coordinator: &TaskCoordinator<Configuring>,
    ) -> mpsc::Receiver<Vec<Event>> {
        spawn_otlp_grpc_server(self.listen_addr, self.service, task_coordinator);
        self.rx
    }
}

pub fn spawn_otlp_grpc_server(
    listen_addr: GrpcAddress,
    service: EventForwardService,
    task_coordinator: &TaskCoordinator<Configuring>,
) {
    let started = task_coordinator.track_started();
    let completed = task_coordinator.track_completed();
    let mut shutdown_handle = task_coordinator.register_for_shutdown();

    tokio::spawn(async move {
        started.mark_as_done();

        let (trigger_shutdown, shutdown_signal, _) = ShutdownSignal::new_wired();
        let mut trigger_shutdown = Some(trigger_shutdown);
        let tls_settings = MaybeTlsSettings::from_config(None, true)
            .expect("should not fail to get empty TLS settings");

        let log_service = LogsServiceAdapter(LogsServiceServer::new(service.clone())
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(usize::MAX));
        let metrics_service = MetricsServiceAdapter(MetricsServiceServer::new(service.clone())
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(usize::MAX));
        let trace_service = TraceServiceAdapter(TraceServiceServer::new(service)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(usize::MAX));

        let mut builder = tonic::service::RoutesBuilder::default();
        builder
            .add_service(log_service)
            .add_service(metrics_service)
            .add_service(trace_service);

        let server = run_grpc_server_with_routes(
            listen_addr.as_socket_addr(),
            tls_settings,
            builder.routes(),
            shutdown_signal,
        );
        pin!(server);

        loop {
            select! {
                _ = shutdown_handle.wait(), if trigger_shutdown.is_some() => {
                    trigger_shutdown.take().unwrap().cancel();
                },
                _ = &mut server => break,
            }
        }

        completed.mark_as_done();
    });
}

pub struct ControlledEdges {
    pub input: Option<mpsc::Sender<TestEvent>>,
    pub output: Option<mpsc::Receiver<Vec<Event>>>,
}
