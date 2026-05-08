use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use futures::TryFutureExt;
use listenfd::ListenFd;
use serde_with::serde_as;
use sol_lib::event::otel_metric::{InstrumentationScope, Resource};
use sol_lib::{
    EstimatedJsonEncodedSizeOf,
    configurable::configurable_component,
    internal_event::{CountByteSize, InternalEventHandle as _, Registered},
    ipallowlist::IpAllowlistConfig,
};
use tokio::io::AsyncBufReadExt;

use self::aggregator::{Aggregator, AggregatorConfig};
use super::util::net::{SocketListenAddr, try_bind_udp_socket};
use crate::{
    SourceSender,
    config::{GenerateConfig, Resource as CfgResource, SourceConfig, SourceContext, SourceOutput},
    event::Event,
    internal_events::{
        EventsReceived, SocketBindError, SocketBytesReceived, SocketMode, SocketReceiveError,
        StreamClosedError,
    },
    net,
    shutdown::ShutdownSignal,
    sources::source_otel,
    tcp::TcpKeepaliveConfig,
    tls::TlsSourceConfig,
};

pub mod aggregator;
pub mod parser;
#[cfg(unix)]
mod unix;

use parser::Parser;
#[cfg(unix)]
use unix::{UnixConfig, statsd_unix_aggregated};

/// Configuration for the `statsd` source.
#[configurable_component(source("statsd", "Collect metrics emitted by the StatsD aggregator."))]
#[derive(Clone, Debug)]
#[serde(tag = "mode", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The type of socket to use."))]
#[allow(clippy::large_enum_variant)] // just used for configuration
pub enum StatsdConfig {
    /// Listen on TCP.
    Tcp(TcpConfig),

    /// Listen on UDP.
    Udp(UdpConfig),

    /// Listen on a Unix domain Socket (UDS).
    #[cfg(unix)]
    Unix(UnixConfig),
}

/// Specifies the target unit for converting incoming StatsD timing values. When set to "seconds" (the default), timing values in milliseconds (`ms`) are converted to seconds (`s`). When set to "milliseconds", the original timing values are preserved.
#[configurable_component]
#[derive(Clone, Debug, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConversionUnit {
    /// Convert to seconds.
    #[default]
    Seconds,

    /// Convert to milliseconds.
    Milliseconds,
}

/// UDP configuration for the `statsd` source.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct UdpConfig {
    #[configurable(derived)]
    address: SocketListenAddr,

    /// The size of the receive buffer used for each connection.
    receive_buffer_bytes: Option<usize>,

    #[serde(default = "default_sanitize")]
    #[configurable(derived)]
    sanitize: bool,

    #[serde(default = "default_convert_to")]
    #[configurable(derived)]
    convert_to: ConversionUnit,

    /// The flush interval in seconds for aggregated metrics.
    #[serde(default = "default_flush_interval_secs")]
    flush_interval_secs: f64,

    /// The time-to-live in seconds for gauge metrics that receive no updates.
    #[serde(default = "default_gauge_ttl_secs")]
    gauge_ttl_secs: f64,

    /// Whether emitted Sum metrics should be marked as monotonic.
    #[serde(default = "default_is_monotonic")]
    is_monotonic: bool,
}

impl UdpConfig {
    pub const fn from_address(address: SocketListenAddr) -> Self {
        Self {
            address,
            receive_buffer_bytes: None,
            sanitize: default_sanitize(),
            convert_to: default_convert_to(),
            flush_interval_secs: default_flush_interval_secs(),
            gauge_ttl_secs: default_gauge_ttl_secs(),
            is_monotonic: default_is_monotonic(),
        }
    }
}

/// TCP configuration for the `statsd` source.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug)]
pub struct TcpConfig {
    #[configurable(derived)]
    address: SocketListenAddr,

    #[configurable(derived)]
    keepalive: Option<TcpKeepaliveConfig>,

    #[configurable(derived)]
    pub permit_origin: Option<IpAllowlistConfig>,

    #[configurable(derived)]
    #[serde(default)]
    tls: Option<TlsSourceConfig>,

    /// The timeout before a connection is forcefully closed during shutdown.
    #[serde(default = "default_shutdown_timeout_secs")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[configurable(metadata(docs::human_name = "Shutdown Timeout"))]
    shutdown_timeout_secs: Duration,

    /// The size of the receive buffer used for each connection.
    #[configurable(metadata(docs::type_unit = "bytes"))]
    receive_buffer_bytes: Option<usize>,

    /// The maximum number of TCP connections that are allowed at any given time.
    #[configurable(metadata(docs::type_unit = "connections"))]
    connection_limit: Option<u32>,

    ///	Whether or not to sanitize incoming statsd key names. When "true", keys are sanitized by:
    /// - "/" is replaced with "-"
    /// - All whitespace is replaced with "_"
    /// - All non alphanumeric characters (A-Z, a-z, 0-9, _, or -) are removed.
    #[serde(default = "default_sanitize")]
    #[configurable(derived)]
    sanitize: bool,

    #[serde(default = "default_convert_to")]
    #[configurable(derived)]
    convert_to: ConversionUnit,

    /// The flush interval in seconds for aggregated metrics.
    #[serde(default = "default_flush_interval_secs")]
    flush_interval_secs: f64,

    /// The time-to-live in seconds for gauge metrics that receive no updates.
    #[serde(default = "default_gauge_ttl_secs")]
    gauge_ttl_secs: f64,

    /// Whether emitted Sum metrics should be marked as monotonic.
    #[serde(default = "default_is_monotonic")]
    is_monotonic: bool,
}

impl TcpConfig {
    #[cfg(test)]
    pub const fn from_address(address: SocketListenAddr) -> Self {
        Self {
            address,
            keepalive: None,
            permit_origin: None,
            tls: None,
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            receive_buffer_bytes: None,
            connection_limit: None,
            sanitize: default_sanitize(),
            convert_to: default_convert_to(),
            flush_interval_secs: default_flush_interval_secs(),
            gauge_ttl_secs: default_gauge_ttl_secs(),
            is_monotonic: default_is_monotonic(),
        }
    }
}

const fn default_shutdown_timeout_secs() -> Duration {
    Duration::from_secs(30)
}

const fn default_sanitize() -> bool {
    true
}

const fn default_convert_to() -> ConversionUnit {
    ConversionUnit::Seconds
}

const fn default_flush_interval_secs() -> f64 {
    10.0
}

const fn default_gauge_ttl_secs() -> f64 {
    300.0
}

const fn default_is_monotonic() -> bool {
    true
}

impl GenerateConfig for StatsdConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::Udp(UdpConfig::from_address(
            SocketListenAddr::SocketAddr(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                8125,
            ))),
        )))
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "statsd")]
impl SourceConfig for StatsdConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        let empty_overrides = BTreeMap::new();
        let resource = source_otel::build_source_resource("statsd", &empty_overrides);
        let scope = source_otel::build_source_scope("statsd");

        match self {
            StatsdConfig::Udp(config) => Ok(Box::pin(statsd_udp(
                config.clone(),
                cx.shutdown,
                cx.out,
                resource,
                scope,
            ))),
            StatsdConfig::Tcp(config) => Ok(Box::pin(statsd_tcp(
                config.clone(),
                cx.shutdown,
                cx.out,
                resource,
                scope,
            ))),
            #[cfg(unix)]
            StatsdConfig::Unix(config) => Ok(Box::pin(statsd_unix_aggregated(
                config.clone(),
                cx.shutdown,
                cx.out,
                resource,
                scope,
            ))),
        }
    }

    fn outputs(&self) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }

    fn resources(&self) -> Vec<CfgResource> {
        match self.clone() {
            Self::Tcp(tcp) => vec![tcp.address.as_tcp_resource()],
            Self::Udp(udp) => vec![udp.address.as_udp_resource()],
            #[cfg(unix)]
            Self::Unix(_) => vec![],
        }
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

async fn statsd_udp(
    config: UdpConfig,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
    resource: Resource,
    scope: InstrumentationScope,
) -> Result<(), ()> {
    let listenfd = ListenFd::from_env();
    let socket = try_bind_udp_socket(config.address, listenfd)
        .map_err(|error| {
            emit!(SocketBindError {
                mode: SocketMode::Udp,
                error
            })
        })
        .await?;

    if let Some(receive_buffer_bytes) = config.receive_buffer_bytes
        && let Err(error) = net::set_receive_buffer_size(&socket, receive_buffer_bytes)
    {
        warn!(message = "Failed configuring receive buffer size on UDP socket.", %error);
    }

    info!(
        message = "Listening.",
        addr = %config.address,
        r#type = "udp"
    );

    let parser = Parser::new(config.sanitize, config.convert_to);
    let timer_unit = match config.convert_to {
        ConversionUnit::Seconds => "s",
        ConversionUnit::Milliseconds => "ms",
    };
    let agg_config = AggregatorConfig {
        flush_interval: Duration::from_secs_f64(config.flush_interval_secs),
        gauge_ttl: Duration::from_secs_f64(config.gauge_ttl_secs),
        is_monotonic: config.is_monotonic,
        timer_unit: timer_unit.to_string(),
    };
    let mut aggregator = Aggregator::new(agg_config.clone());
    let mut flush_interval = tokio::time::interval(agg_config.flush_interval);
    flush_interval.tick().await; // consume the immediate first tick
    let events_received = register!(EventsReceived);

    let mut buf = vec![0u8; 65535];

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
            recv = socket.recv_from(&mut buf) => {
                match recv {
                    Ok((n, _addr)) => {
                        emit!(SocketBytesReceived {
                            mode: SocketMode::Udp,
                            byte_size: n,
                        });
                        let data = &buf[..n];
                        if let Ok(s) = std::str::from_utf8(data) {
                            for line in s.lines() {
                                if line.is_empty() {
                                    continue;
                                }
                                match parser.parse_for_aggregation(line) {
                                    Ok(parsed) => aggregator.record(parsed),
                                    Err(error) => {
                                        debug!(message = "Failed to parse statsd line.", %error);
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        emit!(SocketReceiveError {
                            mode: SocketMode::Udp,
                            error
                        });
                    }
                }
            }
        }
    }

    Ok(())
}

async fn flush_aggregator(
    aggregator: &mut Aggregator,
    resource: &Resource,
    scope: &InstrumentationScope,
    events_received: &Registered<EventsReceived>,
    out: &mut SourceSender,
) -> bool {
    let metrics = aggregator.flush(resource, scope);
    if metrics.is_empty() {
        return true;
    }
    let events: Vec<Event> = metrics.into_iter().map(Event::Metric).collect();
    let count = events.len();
    let byte_size = events.estimated_json_encoded_size_of();
    events_received.emit(CountByteSize(count, byte_size));
    if (out.send_batch(events).await).is_err() {
        emit!(StreamClosedError { count });
        return false;
    }
    true
}

async fn statsd_tcp(
    config: TcpConfig,
    mut shutdown: ShutdownSignal,
    mut out: SourceSender,
    resource: Resource,
    scope: InstrumentationScope,
) -> Result<(), ()> {
    let addr = match config.address {
        SocketListenAddr::SocketAddr(addr) => addr,
        SocketListenAddr::SystemdFd(_) => {
            error!(message = "Aggregated TCP mode does not support systemd socket activation.");
            return Err(());
        }
    };
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|error| {
        emit!(SocketBindError {
            mode: SocketMode::Tcp,
            error,
        });
    })?;

    info!(
        message = "Listening.",
        addr = %config.address,
        r#type = "tcp"
    );

    let parser = Parser::new(config.sanitize, config.convert_to);
    let timer_unit = match config.convert_to {
        ConversionUnit::Seconds => "s",
        ConversionUnit::Milliseconds => "ms",
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
                            let reader = tokio::io::BufReader::new(stream);
                            let mut lines = reader.lines();
                            while let Ok(Some(line)) = lines.next_line().await {
                                emit!(SocketBytesReceived {
                                    mode: SocketMode::Tcp,
                                    byte_size: line.len() + 1,
                                });
                                if tx.send(line).await.is_err() {
                                    break;
                                }
                            }
                        });
                    }
                    Err(error) => {
                        emit!(SocketReceiveError {
                            mode: SocketMode::Tcp,
                            error,
                        });
                    }
                }
            }
            Some(line) = line_rx.recv() => {
                if !line.is_empty() {
                    match parser.parse_for_aggregation(&line) {
                        Ok(parsed) => aggregator.record(parsed),
                        Err(error) => {
                            emit!(SocketReceiveError {
                                mode: SocketMode::Tcp,
                                error: std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use futures::StreamExt;
    use futures::channel::mpsc;
    use futures_util::SinkExt;
    use sol_lib::{config::ComponentKey, event::EventContainer};
    use tokio::{
        io::AsyncWriteExt,
        net::UdpSocket,
        time::{Duration, Instant, sleep},
    };

    use super::*;
    use crate::{
        series,
        test_util::{
            addr::next_addr,
            collect_limited,
            components::{
                COMPONENT_ERROR_TAGS, SOCKET_PUSH_SOURCE_TAGS, assert_source_compliance,
                assert_source_error,
            },
            metrics::{
                AbsoluteMetricState, assert_counter, assert_exponential_histogram, assert_gauge,
            },
        },
    };

    fn statsd_series(name: &str, tags: &[(&str, &str)]) -> sol_lib::event::metric::MetricIdentity {
        use sol_lib::event::{OtelAttributes, string_value};

        let mut attrs = OtelAttributes::new();
        for &(k, v) in tags {
            attrs.insert(k.to_string(), string_value(v));
        }
        sol_lib::event::metric::MetricIdentity {
            name: name.into(),
            namespace: None,
            tags: Some(attrs),
        }
    }

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<StatsdConfig>();
    }

    #[tokio::test]
    async fn test_statsd_udp() {
        assert_source_compliance(&SOCKET_PUSH_SOURCE_TAGS, async move {
            let (_guard, in_addr) = next_addr();
            let config = StatsdConfig::Udp(UdpConfig::from_address(in_addr.into()));
            let (sender, mut receiver) = mpsc::channel(200);
            tokio::spawn(async move {
                let (_guard, bind_addr) = next_addr();
                let socket = UdpSocket::bind(bind_addr).await.unwrap();
                socket.connect(in_addr).await.unwrap();
                while let Some(bytes) = receiver.next().await {
                    socket.send(bytes).await.unwrap();
                }
            });
            test_statsd(config, sender).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_statsd_tcp() {
        assert_source_compliance(&SOCKET_PUSH_SOURCE_TAGS, async move {
            let (_guard, in_addr) = next_addr();
            let config = StatsdConfig::Tcp(TcpConfig::from_address(in_addr.into()));
            let (sender, mut receiver) = mpsc::channel(200);
            tokio::spawn(async move {
                while let Some(bytes) = receiver.next().await {
                    tokio::net::TcpStream::connect(in_addr)
                        .await
                        .unwrap()
                        .write_all(bytes)
                        .await
                        .unwrap();
                }
            });
            test_statsd(config, sender).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_statsd_error() {
        assert_source_error(&COMPONENT_ERROR_TAGS, async move {
            let (_guard, in_addr) = next_addr();
            let config = StatsdConfig::Tcp(TcpConfig::from_address(in_addr.into()));
            let (sender, mut receiver) = mpsc::channel(200);
            tokio::spawn(async move {
                while let Some(bytes) = receiver.next().await {
                    tokio::net::TcpStream::connect(in_addr)
                        .await
                        .unwrap()
                        .write_all(bytes)
                        .await
                        .unwrap();
                }
            });
            test_invalid_statsd(config, sender).await;
        })
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_statsd_unix() {
        assert_source_compliance(&SOCKET_PUSH_SOURCE_TAGS, async move {
            let in_path = tempfile::tempdir().unwrap().keep().join("unix_test");
            let config = StatsdConfig::Unix(UnixConfig {
                path: in_path.clone(),
                sanitize: true,
                convert_to: ConversionUnit::Seconds,
                flush_interval_secs: default_flush_interval_secs(),
                gauge_ttl_secs: default_gauge_ttl_secs(),
                is_monotonic: default_is_monotonic(),
            });
            let (sender, mut receiver) = mpsc::channel(200);
            tokio::spawn(async move {
                while let Some(bytes) = receiver.next().await {
                    tokio::net::UnixStream::connect(&in_path)
                        .await
                        .unwrap()
                        .write_all(bytes)
                        .await
                        .unwrap();
                }
            });
            test_statsd(config, sender).await;
        })
        .await;
    }

    #[tokio::test]
    async fn test_statsd_udp_conversion_disabled() {
        let (_guard, in_addr) = next_addr();
        let mut config = UdpConfig::from_address(in_addr.into());
        config.convert_to = ConversionUnit::Milliseconds;
        let statsd_config = StatsdConfig::Udp(config);
        let (mut sender, mut receiver) = mpsc::channel(200);

        tokio::spawn(async move {
            let (_guard, bind_addr) = next_addr();
            let socket = UdpSocket::bind(bind_addr).await.unwrap();
            socket.connect(in_addr).await.unwrap();
            while let Some(bytes) = receiver.next().await {
                socket.send(bytes).await.unwrap();
            }
        });

        let component_key = ComponentKey::from("statsd_conversion_disabled");
        let (tx, rx) = SourceSender::new_test_sender_with_options(4096, None);
        let (source_ctx, shutdown) = SourceContext::new_shutdown(&component_key, tx);
        let sink = statsd_config
            .build(source_ctx)
            .await
            .expect("failed to build source");

        tokio::spawn(async move {
            sink.await.expect("sink should not fail");
        });

        sleep(Duration::from_millis(250)).await;
        sender.send(b"timer:320|ms|@0.1\n").await.unwrap();
        sleep(Duration::from_millis(250)).await;
        shutdown
            .shutdown_all(Some(Instant::now() + Duration::from_millis(100)))
            .await;
        let state = collect_limited(rx)
            .await
            .into_iter()
            .flat_map(EventContainer::into_events)
            .collect::<AbsoluteMetricState>();
        let metrics = state.finish();
        assert_exponential_histogram(&metrics, series!("timer"), 10, 3200.0);
    }

    async fn test_statsd(statsd_config: StatsdConfig, mut sender: mpsc::Sender<&'static [u8]>) {
        // Build our statsd source and then spawn it.  We use a big pipeline buffer because each
        // packet we send has a lot of metrics per packet.  We could technically count them all up
        // and have a more accurate number here, but honestly, who cares?  This is big enough.
        let component_key = ComponentKey::from("statsd");
        let (tx, rx) = SourceSender::new_test_sender_with_options(4096, None);
        let (source_ctx, shutdown) = SourceContext::new_shutdown(&component_key, tx);
        let sink = statsd_config
            .build(source_ctx)
            .await
            .expect("failed to build statsd source");

        tokio::spawn(async move {
            sink.await.expect("sink should not fail");
        });

        // Wait like 250ms to give the sink time to start running and become ready to handle
        // traffic.
        //
        // TODO: It'd be neat if we could make `ShutdownSignal` track when it was polled at least once,
        // and then surface that (via one of the related types, maybe) somehow so we could use it as
        // a signal for "the sink is ready, it's polled the shutdown future at least once, which
        // means it's trying to accept connections, etc" and would be far more deterministic than this.
        sleep(Duration::from_millis(250)).await;

        // Send all of the messages.
        for _ in 0..100 {
            sender.send(
                b"foo:1|c|#a,b:b\nbar:42|g\nfoo:1|c|#a,b:c\nglork:3|h|@0.1\nmilliglork:3000|ms|@0.2\nset:0|s\nset:1|s\n"
            ).await.unwrap();

            // Space things out slightly to try to avoid dropped packets.
            sleep(Duration::from_millis(10)).await;
        }

        // Now wait for another small period of time to make sure we've processed the messages.
        // After that, trigger shutdown so our source closes and allows us to deterministically read
        // everything that was in up without having to know the exact count.
        sleep(Duration::from_millis(250)).await;
        shutdown
            .shutdown_all(Some(Instant::now() + Duration::from_millis(100)))
            .await;

        // Read all the events into a `MetricState`, which handles normalizing metrics and tracking
        // cumulative values for incremental metrics, etc.  This will represent the final/cumulative
        // values for each metric sent by the source into the pipeline.
        let state = collect_limited(rx)
            .await
            .into_iter()
            .flat_map(EventContainer::into_events)
            .collect::<AbsoluteMetricState>();
        let metrics = state.finish();

        assert_counter(
            &metrics,
            statsd_series("foo", &[("a", ""), ("b", "b")]),
            100.0,
        );

        assert_counter(
            &metrics,
            statsd_series("foo", &[("a", ""), ("b", "c")]),
            100.0,
        );

        assert_gauge(&metrics, series!("bar"), 42.0);
        assert_exponential_histogram(&metrics, series!("glork"), 1000, 3000.0);
        assert_exponential_histogram(&metrics, series!("milliglork"), 500, 1500.0);
        // Sets become gauge(cardinality)
        assert_gauge(&metrics, series!("set"), 2.0);
    }

    async fn test_invalid_statsd(
        statsd_config: StatsdConfig,
        mut sender: mpsc::Sender<&'static [u8]>,
    ) {
        // Build our statsd source and then spawn it.  We use a big pipeline buffer because each
        // packet we send has a lot of metrics per packet.  We could technically count them all up
        // and have a more accurate number here, but honestly, who cares?  This is big enough.
        let component_key = ComponentKey::from("statsd");
        let (tx, _rx) = SourceSender::new_test_sender_with_options(4096, None);
        let (source_ctx, shutdown) = SourceContext::new_shutdown(&component_key, tx);
        let sink = statsd_config
            .build(source_ctx)
            .await
            .expect("failed to build statsd source");

        tokio::spawn(async move {
            sink.await.expect("sink should not fail");
        });

        // Wait like 250ms to give the sink time to start running and become ready to handle
        // traffic.
        //
        // TODO: It'd be neat if we could make `ShutdownSignal` track when it was polled at least once,
        // and then surface that (via one of the related types, maybe) somehow so we could use it as
        // a signal for "the sink is ready, it's polled the shutdown future at least once, which
        // means it's trying to accept connections, etc" and would be far more deterministic than this.
        sleep(Duration::from_millis(250)).await;

        // Send 10 invalid statsd messages
        for _ in 0..10 {
            sender.send(b"invalid statsd message").await.unwrap();

            // Space things out slightly to try to avoid dropped packets.
            sleep(Duration::from_millis(10)).await;
        }

        // Now wait for another small period of time to make sure we've processed the messages.
        // After that, trigger shutdown so our source closes and allows us to deterministically read
        // everything that was in up without having to know the exact count.
        sleep(Duration::from_millis(250)).await;
        shutdown
            .shutdown_all(Some(Instant::now() + Duration::from_millis(100)))
            .await;
    }
}
