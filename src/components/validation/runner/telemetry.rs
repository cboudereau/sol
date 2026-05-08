use std::time::Duration;

use sol_lib::event::Event;
use tokio::{select, sync::mpsc, task::JoinHandle};

use super::io::{EventForwardService, spawn_otlp_grpc_server};
use crate::{
    components::validation::{
        sync::{Configuring, TaskCoordinator},
        util::GrpcAddress,
    },
    config::ConfigBuilder,
    sinks::opentelemetry::{GrpcConfig, OpenTelemetryConfig, Protocol},
    sources::{internal_logs::InternalLogsConfig, internal_metrics::InternalMetricsConfig},
    test_util::addr::next_addr,
};

const INTERNAL_LOGS_KEY: &str = "_telemetry_logs";
const INTERNAL_METRICS_KEY: &str = "_telemetry_metrics";
const VECTOR_SINK_KEY: &str = "_telemetry_out";

const SHUTDOWN_TICKS: u8 = 3;

/// Telemetry collector for a component under validation.
pub struct Telemetry {
    listen_addr: GrpcAddress,
    service: EventForwardService,
    rx: mpsc::Receiver<Vec<Event>>,
}

impl Telemetry {
    /// Creates a telemetry collector by attaching the relevant components to an existing `ConfigBuilder`.
    pub fn attach_to_config(config_builder: &mut ConfigBuilder) -> Self {
        let (_guard, addr) = next_addr();
        let listen_addr = GrpcAddress::from(addr);
        info!(%listen_addr, "Attaching telemetry components.");

        let internal_logs = InternalLogsConfig::default();
        let internal_metrics = InternalMetricsConfig {
            scrape_interval_secs: Duration::from_millis(100),
            ..Default::default()
        };
        let mut grpc_config = GrpcConfig {
            endpoint: listen_addr.as_uri().to_string(),
            load_balancing: None,
            compression: false,
            batch: Default::default(),
            request: Default::default(),
            tls: None,
            acknowledgements: Default::default(),
        };

        grpc_config.batch.timeout_secs = Some(0.1);
        grpc_config.request.retry_attempts = 0;
        let vector_sink = OpenTelemetryConfig::from_protocol(Protocol::Grpc(grpc_config));

        config_builder.add_source(INTERNAL_LOGS_KEY, internal_logs);
        config_builder.add_source(INTERNAL_METRICS_KEY, internal_metrics);
        config_builder.add_sink(
            VECTOR_SINK_KEY,
            &[INTERNAL_LOGS_KEY, INTERNAL_METRICS_KEY],
            vector_sink,
        );

        let (tx, rx) = mpsc::channel(1024);

        Self {
            listen_addr,
            service: EventForwardService::from(tx),
            rx,
        }
    }

    pub async fn into_collector(
        self,
        telemetry_task_coordinator: &TaskCoordinator<Configuring>,
    ) -> TelemetryCollector {
        let telemetry_started = telemetry_task_coordinator.track_started();
        let telemetry_completed = telemetry_task_coordinator.track_completed();
        let mut telemetry_shutdown_handle = telemetry_task_coordinator.register_for_shutdown();

        let grpc_task_coordinator = TaskCoordinator::new("gRPC");
        spawn_otlp_grpc_server(self.listen_addr, self.service, &grpc_task_coordinator);
        let mut grpc_task_coordinator = grpc_task_coordinator.started().await;
        info!("All gRPC task(s) started.");

        let mut rx = self.rx;
        let driver_handle = tokio::spawn(async move {
            telemetry_started.mark_as_done();

            let mut telemetry_events = Vec::new();
            'outer: loop {
                select! {
                    _ = telemetry_shutdown_handle.wait() => {
                        info!("Telemetry: waiting for final internal_metrics events before shutting down.");

                        let mut batches_received = 0;

                        let timeout = tokio::time::sleep(Duration::from_secs(5));
                        tokio::pin!(timeout);

                        loop {
                            select! {
                                d = rx.recv() => {
                                    match d {
                                        None => break,
                                        Some(telemetry_event_batch) => {
                                        telemetry_events.extend(telemetry_event_batch);
                                            info!("Telemetry: processed one batch of internal_metrics.");
                                            batches_received += 1;
                                            if batches_received == SHUTDOWN_TICKS {
                                                break;
                                            }
                                        }
                                    }
                                },
                                _ = &mut timeout => break,
                            }
                        }
                        if batches_received != SHUTDOWN_TICKS {
                            panic!("Did not receive {SHUTDOWN_TICKS} events while waiting for shutdown! Only received {batches_received}!");
                        }
                        break 'outer;
                    },
                    maybe_telemetry_event = rx.recv() => match maybe_telemetry_event {
                        None => break,
                        Some(telemetry_event_batch) => telemetry_events.extend(telemetry_event_batch),
                    },
                }
            }

            grpc_task_coordinator.shutdown().await;

            telemetry_completed.mark_as_done();

            telemetry_events
        });

        TelemetryCollector { driver_handle }
    }
}

pub struct TelemetryCollector {
    driver_handle: JoinHandle<Vec<Event>>,
}

impl TelemetryCollector {
    pub async fn collect(self) -> Vec<Event> {
        self.driver_handle
            .await
            .expect("telemetry collector task should not panic")
    }
}
