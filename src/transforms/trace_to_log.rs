use sol_lib::config::clone_input_definitions;
use sol_lib::configurable::configurable_component;

use crate::config::OutputId;
use crate::{
    config::{DataType, GenerateConfig, Input, TransformConfig, TransformContext, TransformOutput},
    event::Event,
    schema::Definition,
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

/// Configuration for the `trace_to_log` transform.
///
/// This is a naive implementation that simply converts an `OtelSpan` to an `OtelLog`.
/// The conversion preserves all trace attributes (span IDs, trace IDs, etc.) as log fields without modification.
/// This will need to be updated when Vector's trace data model is finalized to properly handle trace-specific semantics and field mappings.
#[configurable_component(transform("trace_to_log", "Convert trace events to log events."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct TraceToLogConfig {}

impl GenerateConfig for TraceToLogConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {}).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "trace_to_log")]
impl TransformConfig for TraceToLogConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(TraceToLog))
    }

    fn enable_concurrency(&self) -> bool {
        true
    }

    fn input(&self) -> Input {
        Input::trace()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        input_definitions: &[(OutputId, Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(input_definitions),
        )]
    }
}

#[derive(Clone, Debug)]
pub struct TraceToLog;

impl FunctionTransform for TraceToLog {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        if let Event::Trace(span) = event {
            // Convert span to log: span fields become log attributes,
            // resource and scope are preserved.
            let map = span.as_map().unwrap_or_default();
            let log = sol_lib::event::OtelLog::from_value_map(
                vrl::value::Value::Object(map),
                span.metadata().clone(),
            );
            output.push(Event::Log(log));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::components::assert_transform_compliance;
    use crate::transforms::test::create_topology;
    use sol_lib::event::{EventMetadata, OtelLog, OtelSpan};
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use vrl::value::Value;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<TraceToLogConfig>();
    }

    async fn do_transform(trace: OtelSpan) -> Option<OtelLog> {
        assert_transform_compliance(async move {
            let config = TraceToLogConfig {};
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            tx.send(Event::Trace(trace)).await.unwrap();

            let result = out.recv().await;

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);

            result
        })
        .await
        .map(|e| e.into_log())
    }

    #[tokio::test]
    async fn transform_trace() {
        let trace = OtelSpan::from_value_map(
            Value::Object(vrl::value::ObjectMap::from_iter([
                ("span_id".into(), Value::from("abc123")),
                ("trace_id".into(), Value::from("xyz789")),
                ("span_name".into(), Value::from("test-span")),
                ("service".into(), Value::from("my-service")),
            ])),
            EventMetadata::default(),
        );

        let expected_map = trace.as_map().unwrap();

        let log = do_transform(trace).await.unwrap();
        let actual_map = log.as_map().expect("log value should be an object");

        assert_eq!(
            actual_map, expected_map,
            "Trace data fields should be preserved"
        );
    }
}
