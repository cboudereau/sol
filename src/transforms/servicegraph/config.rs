use sol_lib::configurable::configurable_component;

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    schema,
    transforms::Transform,
};

use super::transform::ServiceGraph;

/// Configuration for the `servicegraph` transform.
///
/// Computes inter-service edge metrics from trace spans, emitting
/// them as OTel Metric events. Mirrors the OTel Collector Contrib
/// `servicegraphconnector`.
#[configurable_component(transform(
    "servicegraph",
    "Compute inter-service edge metrics from trace spans — servicegraphconnector equivalent.",
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ServiceGraphConfig {
    /// Store configuration for pending edge buffering.
    #[serde(default)]
    pub store: StoreConfig,

    /// Flush interval in seconds for emitting aggregated metrics.
    #[serde(default = "default_flush_interval")]
    pub metrics_flush_interval_secs: u64,

    /// Explicit histogram bucket boundaries for latency metrics (in seconds).
    #[serde(default = "default_buckets")]
    pub latency_histogram_buckets: Vec<f64>,

    /// Additional span/resource attributes to include as metric dimensions.
    #[serde(default)]
    pub dimensions: Vec<String>,
}

/// Store configuration for pending edge buffering.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Time-to-live in seconds for pending edges awaiting their pair.
    #[serde(default = "default_ttl")]
    pub ttl_secs: u64,

    /// Maximum number of pending edges in the store.
    #[serde(default = "default_max_items")]
    pub max_items: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_ttl(),
            max_items: default_max_items(),
        }
    }
}

const fn default_flush_interval() -> u64 {
    15
}
const fn default_ttl() -> u64 {
    2
}
const fn default_max_items() -> usize {
    1000
}
fn default_buckets() -> Vec<f64> {
    vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

impl GenerateConfig for ServiceGraphConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"
            metrics_flush_interval_secs = 15
            [store]
            ttl_secs = 2
            max_items = 1000
            "#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "servicegraph")]
impl TransformConfig for ServiceGraphConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::event_task(ServiceGraph::new(self.clone())))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Trace)
    }

    fn outputs(
        &self,
        _context: &TransformContext,
        input_definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Metric,
            input_definitions
                .iter()
                .map(|(output, definition)| (output.clone(), definition.clone()))
                .collect(),
        )]
    }
}
