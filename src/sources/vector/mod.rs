//! The `vector` source. See [VectorConfig].
//!
//! This source speaks both OTLP (for native OTel clients) and the legacy
//! Vector protocol (for existing Vector instances using the `vector` sink).
use std::net::SocketAddr;

use futures::TryFutureExt;
use sol_lib::{
    codecs::BytesDeserializerConfig,
    configurable::configurable_component,
    internal_event::{BytesReceived, Protocol},
    opentelemetry::upstream_opentelemetry_proto::tonic::collector::{
        logs::v1::logs_service_server::LogsServiceServer,
        metrics::v1::metrics_service_server::MetricsServiceServer,
        trace::v1::trace_service_server::TraceServiceServer,
    },
};
use tonic::{codec::CompressionEncoding, service::RoutesBuilder};
use tonic_0_12::codec::CompressionEncoding as CompressionEncoding012;

use crate::{
    config::{
        DataType, GenerateConfig, Resource, SourceAcknowledgementsConfig, SourceConfig,
        SourceContext, SourceOutput,
    },
    internal_events::EventsReceived,
    serde::bool_or_struct,
    sources::{
        Source,
        opentelemetry::grpc::{LOGS, METRICS, Service, TRACES},
        util::grpc::run_grpc_server_with_routes,
    },
    tls::{MaybeTlsSettings, TlsEnableableConfig},
};

mod convert;
mod service;

// Include the generated protobuf modules.
#[allow(warnings, clippy::pedantic, clippy::nursery, clippy::clone_on_ref_ptr)]
pub(crate) mod proto {
    pub mod event {
        include!(concat!(env!("OUT_DIR"), "/event.rs"));
    }
    pub mod vector {
        include!(concat!(env!("OUT_DIR"), "/vector.rs"));
    }
}

use crate::sources::util::grpc::tonic_0_12_adapter;
use proto::vector::vector_server::VectorServer;
use service::NativeVectorService;

tonic_0_12_adapter!(LogsServiceAdapter, "opentelemetry.proto.collector.logs.v1.LogsService");
tonic_0_12_adapter!(MetricsServiceAdapter, "opentelemetry.proto.collector.metrics.v1.MetricsService");
tonic_0_12_adapter!(TraceServiceAdapter, "opentelemetry.proto.collector.trace.v1.TraceService");

/// Marker type for version two of the configuration for the `vector` source.
#[configurable_component]
#[derive(Clone, Debug)]
enum VectorConfigVersion {
    /// Marker value for version two.
    #[serde(rename = "2")]
    V2,
}

/// Configuration for the `vector` source.
#[configurable_component(source("vector", "Collect observability data from a Vector instance."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct VectorConfig {
    /// Version of the configuration.
    version: Option<VectorConfigVersion>,

    /// The socket address to listen for connections on.
    ///
    /// It _must_ include a port.
    pub address: SocketAddr,

    #[configurable(derived)]
    #[serde(default)]
    tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    #[serde(default, deserialize_with = "bool_or_struct")]
    acknowledgements: SourceAcknowledgementsConfig,
}

impl VectorConfig {
    /// Creates a `VectorConfig` with the given address.
    pub fn from_address(addr: SocketAddr) -> Self {
        Self {
            address: addr,
            ..Default::default()
        }
    }
}

impl Default for VectorConfig {
    fn default() -> Self {
        Self {
            version: None,
            address: "0.0.0.0:6000".parse().unwrap(),
            tls: None,
            acknowledgements: Default::default(),
        }
    }
}

impl GenerateConfig for VectorConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(VectorConfig::default()).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector")]
impl SourceConfig for VectorConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<Source> {
        let tls_settings = MaybeTlsSettings::from_config(self.tls.as_ref(), true)?;
        let acknowledgements = cx.do_acknowledgements(self.acknowledgements);
        let events_received = register!(EventsReceived);

        // OTLP service (for native OTel clients).
        let otlp_service = Service {
            pipeline: cx.out.clone().into_shared(),
            acknowledgements,
            events_received: events_received.clone(),
            bytes_received: register!(BytesReceived::from(Protocol::from("grpc"))),
        };

        let log_service = LogsServiceServer::new(otlp_service.clone())
            .accept_compressed(CompressionEncoding012::Gzip)
            .max_decoding_message_size(usize::MAX);

        let metrics_service = MetricsServiceServer::new(otlp_service.clone())
            .accept_compressed(CompressionEncoding012::Gzip)
            .max_decoding_message_size(usize::MAX);

        let trace_service = TraceServiceServer::new(otlp_service)
            .accept_compressed(CompressionEncoding012::Gzip)
            .max_decoding_message_size(usize::MAX);

        // Native Vector protocol service (for legacy Vector sinks).
        let native_service = NativeVectorService {
            pipeline: cx.out,
            acknowledgements,
            events_received,
        };

        let vector_service = VectorServer::new(native_service)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(usize::MAX);

        let mut builder = RoutesBuilder::default();
        builder
            .add_service(LogsServiceAdapter(log_service))
            .add_service(MetricsServiceAdapter(metrics_service))
            .add_service(TraceServiceAdapter(trace_service))
            .add_service(vector_service);

        let source =
            run_grpc_server_with_routes(self.address, tls_settings, builder.routes(), cx.shutdown)
                .map_err(|error| {
                    error!(message = "Source future failed.", %error);
                });

        Ok(Box::pin(source))
    }

    fn outputs(&self) -> Vec<SourceOutput> {
        let schema_definition = BytesDeserializerConfig
            .schema_definition()
            .with_standard_vector_source_metadata();

        vec![
            SourceOutput::new_maybe_logs(DataType::all_bits(), schema_definition).with_port(LOGS),
            SourceOutput::new_metrics().with_port(METRICS),
            SourceOutput::new_traces().with_port(TRACES),
        ]
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::VectorConfig>();
    }
}
