pub(crate) mod grpc;
pub(crate) mod http;
pub mod load_balancing;

use indoc::indoc;
use sol_config::component::GenerateConfig;
use sol_lib::configurable::configurable_component;

use crate::{
    config::{AcknowledgementsConfig, Input, SinkConfig, SinkContext},
    sinks::{Healthcheck, VectorSink},
};

pub use grpc::GrpcConfig;
pub use http::OtlpHttpConfig;

/// Configuration for the `OpenTelemetry` sink.
#[configurable_component(sink("opentelemetry", "Deliver OTLP data over HTTP or gRPC."))]
#[derive(Clone, Debug, Default)]
pub struct OpenTelemetryConfig {
    /// Protocol configuration.
    #[configurable(derived)]
    protocol: Protocol,
}

impl OpenTelemetryConfig {
    pub const fn from_protocol(protocol: Protocol) -> Self {
        Self { protocol }
    }
}

/// The transport protocol for the `opentelemetry` sink.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
#[configurable(metadata(docs::enum_tag_description = "The communication protocol."))]
pub enum Protocol {
    /// Send OTLP data over HTTP. Supports both protobuf (default) and JSON
    /// encoding. POSTed to per-signal endpoints (`/v1/logs`, `/v1/metrics`,
    /// `/v1/traces`).
    Http(OtlpHttpConfig),
    /// Send data over gRPC (OTLP/gRPC).
    Grpc(GrpcConfig),
}

impl Default for Protocol {
    fn default() -> Self {
        Protocol::Http(OtlpHttpConfig::default())
    }
}

impl GenerateConfig for OpenTelemetryConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(indoc! {r#"
            [protocol]
            type = "http"
            endpoint = "http://localhost:4318"
        "#})
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "opentelemetry")]
impl SinkConfig for OpenTelemetryConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        match &self.protocol {
            Protocol::Http(config) => config.build(cx).await,
            Protocol::Grpc(config) => config.build(cx).await,
        }
    }

    fn input(&self) -> Input {
        match &self.protocol {
            Protocol::Http(config) => config.input(),
            Protocol::Grpc(config) => config.input(),
        }
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        match &self.protocol {
            Protocol::Http(config) => config.acknowledgements(),
            Protocol::Grpc(config) => config.acknowledgements(),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::OpenTelemetryConfig>();
    }
}
