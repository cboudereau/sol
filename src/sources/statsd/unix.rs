use std::path::PathBuf;
use std::time::Duration;

use sol_lib::configurable::configurable_component;
use sol_lib::event::otel_metric::{InstrumentationScope, Resource};
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::net::UnixListener;

use super::{
    ConversionUnit,
    aggregator::{Aggregator, AggregatorConfig},
    default_convert_to, default_flush_interval_secs, default_gauge_ttl_secs, default_is_monotonic,
    default_sanitize, flush_aggregator,
    parser::Parser,
};
use crate::{
    SourceSender,
    internal_events::{EventsReceived, SocketBytesReceived, SocketMode, SocketReceiveError},
    shutdown::ShutdownSignal,
};

/// Unix domain socket configuration for the `statsd` source.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct UnixConfig {
    /// The Unix socket path.
    ///
    /// This should be an absolute path.
    #[configurable(metadata(docs::examples = "/path/to/socket"))]
    pub path: PathBuf,

    #[serde(default = "default_sanitize")]
    #[configurable(derived)]
    pub sanitize: bool,

    #[serde(default = "default_convert_to")]
    #[configurable(derived)]
    pub convert_to: ConversionUnit,

    /// The flush interval in seconds for aggregated metrics.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: f64,

    /// The time-to-live in seconds for gauge metrics that receive no updates.
    #[serde(default = "default_gauge_ttl_secs")]
    pub gauge_ttl_secs: f64,

    /// Whether emitted Sum metrics should be marked as monotonic.
    #[serde(default = "default_is_monotonic")]
    pub is_monotonic: bool,
}

pub async fn statsd_unix_aggregated(
    config: UnixConfig,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
    resource: Resource,
    scope: InstrumentationScope,
) -> Result<(), ()> {
    let listener = UnixListener::bind(&config.path).map_err(|error| {
        error!(message = "Failed to bind Unix socket.", path = ?config.path, %error);
    })?;

    info!(message = "Listening.", path = ?config.path, r#type = "unix");

    let parser = Parser::new(config.sanitize, config.convert_to);
    let timer_unit = match config.convert_to {
        super::ConversionUnit::Seconds => "s",
        super::ConversionUnit::Milliseconds => "ms",
    };
    let agg_config = AggregatorConfig {
        flush_interval: Duration::from_secs_f64(config.flush_interval_secs),
        gauge_ttl: Duration::from_secs_f64(config.gauge_ttl_secs),
        is_monotonic: config.is_monotonic,
        timer_unit: timer_unit.to_string(),
    };
    let mut aggregator = Aggregator::new(agg_config.clone());
    let mut flush_interval = tokio::time::interval(agg_config.flush_interval);
    flush_interval.tick().await;
    let events_received = register!(EventsReceived);

    let (line_tx, mut line_rx) = tokio::sync::mpsc::channel::<String>(4096);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                flush_aggregator(&mut aggregator, &resource, &scope, &events_received, &mut out).await;
                break;
            }
            _ = flush_interval.tick() => {
                if !flush_aggregator(&mut aggregator, &resource, &scope, &events_received, &mut out).await {
                    break;
                }
            }
            accept = listener.accept() => {
                match accept {
                    Ok((stream, _addr)) => {
                        let tx = line_tx.clone();
                        tokio::spawn(async move {
                            let reader = BufReader::new(stream);
                            let mut lines = reader.lines();
                            while let Ok(Some(line)) = lines.next_line().await {
                                emit!(SocketBytesReceived {
                                    mode: SocketMode::Unix,
                                    byte_size: line.len() + 1,
                                });
                                if tx.send(line).await.is_err() {
                                    break;
                                }
                            }
                        });
                    }
                    Err(error) => {
                        error!(message = "Failed to accept Unix connection.", %error);
                    }
                }
            }
            Some(line) = line_rx.recv() => {
                if !line.is_empty() {
                    match parser.parse_for_aggregation(&line) {
                        Ok(parsed) => aggregator.record(parsed),
                        Err(error) => {
                            emit!(SocketReceiveError {
                                mode: SocketMode::Unix,
                                error: std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
                            });
                        }
                    }
                }
            }
        }
    }

    // Clean up socket file
    let _ = std::fs::remove_file(&config.path);
    Ok(())
}
