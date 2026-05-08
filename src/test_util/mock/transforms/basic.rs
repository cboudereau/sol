use async_trait::async_trait;
use sol_lib::{
    config::{DataType, Input, TransformOutput},
    configurable::configurable_component,
    event::Event,
    schema,
    transform::{FunctionTransform, OutputBuffer, Transform},
};
use vrl::value::Value;

use crate::config::{OutputId, TransformConfig, TransformContext};

/// Configuration for the `test_basic` transform.
#[configurable_component(transform("test_basic", "Test (basic)"))]
#[derive(Clone, Debug, Default)]
pub struct BasicTransformConfig {
    /// Suffix to add to the message of any log event.
    suffix: String,

    /// Amount to increase any metric by.
    increase: f64,
}

impl_generate_config_from_default!(BasicTransformConfig);

impl BasicTransformConfig {
    pub const fn new(suffix: String, increase: f64) -> Self {
        Self { suffix, increase }
    }
}

#[async_trait]
#[typetag::serde(name = "test_basic")]
impl TransformConfig for BasicTransformConfig {
    async fn build(&self, _globals: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(BasicTransform {
            suffix: self.suffix.clone(),
            increase: self.increase,
        }))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::all_bits(),
            definitions
                .iter()
                .map(|(output, definition)| (output.clone(), definition.clone()))
                .collect(),
        )]
    }
}

#[derive(Clone, Debug)]
struct BasicTransform {
    suffix: String,
    increase: f64,
}

impl FunctionTransform for BasicTransform {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        match &mut event {
            Event::Log(otel_log) => {
                let message_key = sol_lib::lookup::OwnedTargetPath::event(
                    sol_lib::lookup::owned_value_path!("body"),
                );
                let mut v = otel_log
                    .get(&message_key)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                v.push_str(&self.suffix);
                otel_log.insert(&message_key, Value::from(v));
            }
            Event::Metric(otel_metric) => {
                use opentelemetry_proto::tonic::metrics::v1::{
                    metric, number_data_point::Value as NDPValue,
                };
                // Modify the first data point value directly on the proto
                if let Some(data) = otel_metric.metric_mut().data.as_mut() {
                    match data {
                        metric::Data::Sum(sum) => {
                            if let Some(dp) = sum.data_points.first_mut() {
                                match &mut dp.value {
                                    Some(NDPValue::AsDouble(v)) => *v += self.increase,
                                    Some(NDPValue::AsInt(v)) => {
                                        #[expect(
                                            clippy::cast_possible_truncation,
                                            reason = "test transform f64 increase fits in i64"
                                        )]
                                        let inc = self.increase as i64;
                                        *v += inc;
                                    }
                                    None => dp.value = Some(NDPValue::AsDouble(self.increase)),
                                }
                            }
                        }
                        metric::Data::Gauge(gauge) => {
                            if let Some(dp) = gauge.data_points.first_mut() {
                                dp.value = Some(NDPValue::AsDouble(self.increase));
                            }
                        }
                        _ => {} // Histogram/Summary/Set — no increment in tests
                    }
                }
            }
            Event::Trace(otel_span) => {
                let message_key = sol_lib::lookup::OwnedTargetPath::event(
                    sol_lib::lookup::owned_value_path!("body"),
                );
                let mut v = otel_span
                    .get(&message_key)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                v.push_str(&self.suffix);
                otel_span.insert(&message_key, Value::from(v));
            }
        };
        output.push(event);
    }
}
