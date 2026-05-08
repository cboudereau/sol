use sol_lib::configurable::configurable_component;

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    schema,
    transforms::Transform,
};

use super::policies::PolicyConfig;
use super::transform::TailSampling;

/// Configuration for the `tail_sampling` transform.
///
/// Buffers OTel spans by trace_id and evaluates sampling policies after a
/// configurable decision wait period. Mirrors the OTel Collector Contrib
/// `tailsamplingprocessor`.
#[configurable_component(transform(
    "tail_sampling",
    "Sample complete traces based on tail-sampling policies (errors, latency, probabilistic, etc.).",
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct TailSamplingConfig {
    /// Wall-clock seconds to wait after the first span of a trace before
    /// evaluating sampling policies.
    #[serde(default = "default_decision_wait_secs")]
    pub decision_wait_secs: u64,

    /// Maximum number of traces buffered in memory. When exceeded, the oldest
    /// incomplete trace is evicted.
    #[serde(default = "default_num_traces")]
    pub num_traces: usize,

    /// Maximum byte size per trace (protobuf-estimated). Traces exceeding this
    /// limit are dropped immediately.
    #[serde(default = "default_max_trace_size_bytes")]
    pub max_trace_size_bytes: usize,

    /// Decision cache configuration.
    #[serde(default)]
    pub decision_cache: DecisionCacheConfig,

    /// Sampling policies evaluated in order. First match wins.
    pub policies: Vec<PolicyConfig>,
}

/// Decision cache: LRU caches for sampled and non-sampled trace decisions.
/// Late-arriving spans inherit the cached decision instead of being rebuffered.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct DecisionCacheConfig {
    /// Number of sampled trace IDs to cache.
    #[serde(default = "default_sampled_cache_size")]
    pub sampled_cache_size: usize,
    /// Number of non-sampled trace IDs to cache.
    #[serde(default = "default_non_sampled_cache_size")]
    pub non_sampled_cache_size: usize,
}

impl Default for DecisionCacheConfig {
    fn default() -> Self {
        Self {
            sampled_cache_size: default_sampled_cache_size(),
            non_sampled_cache_size: default_non_sampled_cache_size(),
        }
    }
}

const fn default_decision_wait_secs() -> u64 {
    30
}
const fn default_num_traces() -> usize {
    50_000
}
const fn default_max_trace_size_bytes() -> usize {
    10 * 1024 * 1024
} // 10 MiB
const fn default_sampled_cache_size() -> usize {
    100_000
}
const fn default_non_sampled_cache_size() -> usize {
    100_000
}

impl GenerateConfig for TailSamplingConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"
            decision_wait_secs = 30
            num_traces = 50000
            [[policies]]
            type = "always_sample"
            name = "all"
            "#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "tail_sampling")]
impl TransformConfig for TailSamplingConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        let transform = TailSampling::new(self.clone())?;
        Ok(Transform::event_task(transform))
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
            DataType::Trace,
            input_definitions
                .iter()
                .map(|(output, definition)| (output.clone(), definition.clone()))
                .collect(),
        )]
    }
}
