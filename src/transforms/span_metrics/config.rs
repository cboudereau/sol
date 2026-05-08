use sol_lib::configurable::configurable_component;

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, TransformConfig, TransformContext,
        TransformOutput,
    },
    schema,
    transforms::Transform,
};

use super::transform::SpanMetrics;

/// Configuration for the `span_metrics` transform.
///
/// Computes RED (Rate, Errors, Duration) metrics from OTel spans, emitting
/// them as OTel Metric events. Mirrors the OTel Collector Contrib
/// `spanmetricsconnector`.
#[configurable_component(transform(
    "span_metrics",
    "Compute RED metrics (calls, duration) from trace spans — spanmetricsconnector equivalent.",
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SpanMetricsConfig {
    /// Metric name prefix.
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Aggregation temporality for emitted metrics.
    #[serde(default)]
    pub aggregation_temporality: Temporality,

    /// Flush interval in seconds.
    #[serde(default = "default_flush_interval")]
    pub metrics_flush_interval_secs: u64,

    /// Histogram configuration for duration metrics.
    #[serde(default)]
    pub histogram: HistogramConfig,

    /// Additional span/resource attributes to include as metric dimensions.
    #[serde(default)]
    pub dimensions: Vec<DimensionConfig>,

    /// Default dimensions to exclude (from the built-in set).
    #[serde(default)]
    pub exclude_dimensions: Vec<String>,
}

/// Aggregation temporality.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum Temporality {
    /// Cumulative: values accumulate over time.
    #[default]
    Cumulative,
    /// Delta: values reset each flush interval.
    Delta,
}

/// Histogram configuration.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct HistogramConfig {
    /// Duration unit for the histogram.
    #[serde(default = "default_unit")]
    pub unit: String,

    /// Explicit bucket boundaries. Used when type is "explicit".
    #[serde(default = "default_buckets")]
    pub buckets: Vec<f64>,
}

impl Default for HistogramConfig {
    fn default() -> Self {
        Self {
            unit: default_unit(),
            buckets: default_buckets(),
        }
    }
}

/// A user-configured dimension to extract from span attributes.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct DimensionConfig {
    /// Attribute key name.
    pub name: String,
    /// Default value if attribute is missing.
    #[serde(default)]
    pub default: Option<String>,
}

fn default_namespace() -> String {
    "traces.span.metrics".to_string()
}
const fn default_flush_interval() -> u64 {
    60
}
fn default_unit() -> String {
    "s".to_string()
}
fn default_buckets() -> Vec<f64> {
    vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

impl GenerateConfig for SpanMetricsConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"
            namespace = "traces.span.metrics"
            metrics_flush_interval_secs = 60
            "#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "span_metrics")]
impl TransformConfig for SpanMetricsConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::event_task(SpanMetrics::new(self.clone())))
    }

    fn input(&self) -> Input {
        Input::new(DataType::Trace)
    }

    fn outputs(
        &self,
        _context: &TransformContext,
        input_definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        // Outputs metric events derived from trace input.
        vec![TransformOutput::new(
            DataType::Metric,
            input_definitions
                .iter()
                .map(|(output, definition)| (output.clone(), definition.clone()))
                .collect(),
        )]
    }
}
