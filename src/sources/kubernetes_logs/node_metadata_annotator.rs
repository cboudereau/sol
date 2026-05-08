//! Annotates events with node metadata.

#![deny(missing_docs)]

use k8s_openapi::{api::core::v1::Node, apimachinery::pkg::apis::meta::v1::ObjectMeta};
use kube::runtime::reflector::{ObjectRef, store::Store};
use sol_lib::{
    config::insert_source_metadata,
    configurable::configurable_component,
    lookup::{OwnedTargetPath, lookup_v2::OptionalTargetPath, owned_value_path, path},
};

use super::Config;
use crate::event::{Event, OtelLog, string_value};

/// Configuration for how the events are enriched with Node metadata.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields, default)]
pub struct FieldsSpec {
    /// Event field for the Node's labels.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.node_labels"))]
    #[configurable(metadata(docs::examples = "k8s.node_labels"))]
    #[configurable(metadata(docs::examples = ""))]
    pub node_labels: OptionalTargetPath,
}

impl Default for FieldsSpec {
    fn default() -> Self {
        Self {
            node_labels: OwnedTargetPath::event(owned_value_path!("kubernetes", "node_labels"))
                .into(),
        }
    }
}

/// Annotate the event with node metadata.
pub struct NodeMetadataAnnotator {
    node_state_reader: Store<Node>,
    #[allow(dead_code)]
    fields_spec: FieldsSpec,
}

impl NodeMetadataAnnotator {
    /// Create a new [`NodeMetadataAnnotator`].
    pub const fn new(node_state_reader: Store<Node>, fields_spec: FieldsSpec) -> Self {
        Self {
            node_state_reader,
            fields_spec,
        }
    }
}

impl NodeMetadataAnnotator {
    /// Annotates an event with the information from the [`Node::metadata`].
    pub fn annotate(&self, event: &mut Event, node: &str) -> Option<()> {
        let obj = ObjectRef::<Node>::new(node);
        let resource = self.node_state_reader.get(&obj)?;
        let node: &Node = resource.as_ref();

        if let Event::Log(otel_log) = event {
            annotate_otel_from_metadata(otel_log, &node.metadata);
        }
        Some(())
    }
}

#[allow(dead_code)]
fn annotate_from_metadata(log: &mut OtelLog, fields_spec: &FieldsSpec, metadata: &ObjectMeta) {
    if let Some(labels) = &metadata.labels
        && fields_spec.node_labels.path.is_some()
    {
        for (key, value) in labels.iter() {
            insert_source_metadata(
                Config::NAME,
                log,
                path!("node_labels", key),
                value.to_owned(),
            )
        }
    }
}

fn annotate_otel_from_metadata(otel_log: &mut crate::event::OtelLog, metadata: &ObjectMeta) {
    if let Some(labels) = &metadata.labels {
        for (key, value) in labels.iter() {
            otel_log.set_resource_attribute(format!("k8s.node.labels.{key}"), string_value(value));
        }
    }
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;
    use sol_lib::lookup::{lookup_v2::parse_target_path, metadata_path};

    use super::*;
    use crate::event::OtelLog;

    #[test]
    fn test_annotate_from_metadata() {
        let cases = vec![
            (
                FieldsSpec::default(),
                ObjectMeta::default(),
                OtelLog::default(),
            ),
            (
                FieldsSpec::default(),
                ObjectMeta {
                    name: Some("sandbox0-name".to_owned()),
                    uid: Some("sandbox0-uid".to_owned()),
                    labels: Some(
                        vec![
                            ("sandbox0-label0".to_owned(), "val0".to_owned()),
                            ("sandbox0-label1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..ObjectMeta::default()
                },
                {
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "node_labels", "sandbox0-label0"),
                        "val0",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "node_labels", "sandbox0-label1"),
                        "val1",
                    );
                    log
                },
            ),
            (
                FieldsSpec::default(),
                ObjectMeta {
                    name: Some("sandbox0-name".to_owned()),
                    uid: Some("sandbox0-uid".to_owned()),
                    labels: Some(
                        vec![
                            ("sandbox0-label0".to_owned(), "val0".to_owned()),
                            ("sandbox0-label1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..ObjectMeta::default()
                },
                {
                    // annotate_from_metadata uses insert_source_metadata -> always metadata path
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "node_labels", "sandbox0-label0"),
                        "val0",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "node_labels", "sandbox0-label1"),
                        "val1",
                    );
                    log
                },
            ),
            (
                FieldsSpec {
                    node_labels: parse_target_path("node_labels").unwrap().into(),
                },
                ObjectMeta {
                    name: Some("sandbox0-name".to_owned()),
                    uid: Some("sandbox0-uid".to_owned()),
                    labels: Some(
                        vec![
                            ("sandbox0-label0".to_owned(), "val0".to_owned()),
                            ("sandbox0-label1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..ObjectMeta::default()
                },
                {
                    // annotate_from_metadata ignores FieldsSpec; always stores in metadata
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "node_labels", "sandbox0-label0"),
                        "val0",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "node_labels", "sandbox0-label1"),
                        "val1",
                    );
                    log
                },
            ),
        ];

        for (fields_spec, metadata, expected) in cases.into_iter() {
            let mut log = OtelLog::default();
            annotate_from_metadata(&mut log, &fields_spec, &metadata);
            let expected = expected;
            assert_eq!(log, expected);
        }
    }
}
