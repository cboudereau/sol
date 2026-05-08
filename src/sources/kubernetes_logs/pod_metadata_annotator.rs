//! Annotates events with pod metadata.

#![deny(missing_docs)]

use k8s_openapi::{
    api::core::v1::{Container, ContainerStatus, Pod, PodSpec, PodStatus},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::runtime::reflector::{ObjectRef, store::Store};
use sol_lib::{
    config::insert_source_metadata,
    configurable::configurable_component,
    lookup::{OwnedTargetPath, lookup_v2::OptionalTargetPath, owned_value_path, path},
};

use super::{
    Config,
    path_helpers::{LogFileInfo, parse_log_file_path},
};
use crate::event::{Event, OtelLog, string_value};

/// Configuration for how the events are enriched with Pod metadata.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields, default)]
pub struct FieldsSpec {
    /// Event field for the Pod's name.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_name"))]
    #[configurable(metadata(docs::examples = "k8s.pod_name"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_name: OptionalTargetPath,

    /// Event field for the Pod's namespace.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_ns"))]
    #[configurable(metadata(docs::examples = "k8s.pod_ns"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_namespace: OptionalTargetPath,

    /// Event field for the Pod's UID.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_uid"))]
    #[configurable(metadata(docs::examples = "k8s.pod_uid"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_uid: OptionalTargetPath,

    /// Event field for the Pod's IPv4 address.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_ip"))]
    #[configurable(metadata(docs::examples = "k8s.pod_ip"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_ip: OptionalTargetPath,

    /// Event field for the Pod's IPv4 and IPv6 addresses.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_ips"))]
    #[configurable(metadata(docs::examples = "k8s.pod_ips"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_ips: OptionalTargetPath,

    /// Event field for the `Pod`'s labels.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_labels"))]
    #[configurable(metadata(docs::examples = "k8s.pod_labels"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_labels: OptionalTargetPath,

    /// Event field for the Pod's annotations.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_annotations"))]
    #[configurable(metadata(docs::examples = "k8s.pod_annotations"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_annotations: OptionalTargetPath,

    /// Event field for the Pod's node_name.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_host"))]
    #[configurable(metadata(docs::examples = "k8s.pod_host"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_node_name: OptionalTargetPath,

    /// Event field for the Pod's owner reference.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.pod_owner"))]
    #[configurable(metadata(docs::examples = "k8s.pod_owner"))]
    #[configurable(metadata(docs::examples = ""))]
    pub pod_owner: OptionalTargetPath,

    /// Event field for the Container's name.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.container_name"))]
    #[configurable(metadata(docs::examples = "k8s.container_name"))]
    #[configurable(metadata(docs::examples = ""))]
    pub container_name: OptionalTargetPath,

    /// Event field for the Container's ID.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.container_id"))]
    #[configurable(metadata(docs::examples = "k8s.container_id"))]
    #[configurable(metadata(docs::examples = ""))]
    pub container_id: OptionalTargetPath,

    /// Event field for the Container's image.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.container_image"))]
    #[configurable(metadata(docs::examples = "k8s.container_image"))]
    #[configurable(metadata(docs::examples = ""))]
    pub container_image: OptionalTargetPath,

    /// Event field for the Container's image ID.
    ///
    /// Set to `""` to suppress this key.
    #[configurable(metadata(docs::examples = ".k8s.container_image_id"))]
    #[configurable(metadata(docs::examples = "k8s.container_image_id"))]
    #[configurable(metadata(docs::examples = ""))]
    pub container_image_id: OptionalTargetPath,
}

impl Default for FieldsSpec {
    fn default() -> Self {
        Self {
            pod_name: OwnedTargetPath::event(owned_value_path!("kubernetes", "pod_name")).into(),
            pod_namespace: OwnedTargetPath::event(owned_value_path!("kubernetes", "pod_namespace"))
                .into(),
            pod_uid: OwnedTargetPath::event(owned_value_path!("kubernetes", "pod_uid")).into(),
            pod_ip: OwnedTargetPath::event(owned_value_path!("kubernetes", "pod_ip")).into(),
            pod_ips: OwnedTargetPath::event(owned_value_path!("kubernetes", "pod_ips")).into(),
            pod_labels: OwnedTargetPath::event(owned_value_path!("kubernetes", "pod_labels"))
                .into(),
            pod_annotations: OwnedTargetPath::event(owned_value_path!(
                "kubernetes",
                "pod_annotations"
            ))
            .into(),
            pod_node_name: OwnedTargetPath::event(owned_value_path!("kubernetes", "pod_node_name"))
                .into(),
            pod_owner: OwnedTargetPath::event(owned_value_path!("kubernetes", "pod_owner")).into(),
            container_name: OwnedTargetPath::event(owned_value_path!(
                "kubernetes",
                "container_name"
            ))
            .into(),
            container_id: OwnedTargetPath::event(owned_value_path!("kubernetes", "container_id"))
                .into(),
            container_image: OwnedTargetPath::event(owned_value_path!(
                "kubernetes",
                "container_image"
            ))
            .into(),
            container_image_id: OwnedTargetPath::event(owned_value_path!(
                "kubernetes",
                "container_image_id"
            ))
            .into(),
        }
    }
}

/// Annotate the event with pod metadata.
pub struct PodMetadataAnnotator {
    pods_state_reader: Store<Pod>,
    #[allow(dead_code)]
    fields_spec: FieldsSpec,
}

impl PodMetadataAnnotator {
    /// Create a new [`PodMetadataAnnotator`].
    pub const fn new(pods_state_reader: Store<Pod>, fields_spec: FieldsSpec) -> Self {
        Self {
            pods_state_reader,
            fields_spec,
        }
    }
}

impl PodMetadataAnnotator {
    /// Annotates an event with the information from the [`Pod::metadata`].
    pub fn annotate<'a>(&self, event: &mut Event, file: &'a str) -> Option<LogFileInfo<'a>> {
        let file_info = parse_log_file_path(file)?;
        let obj = ObjectRef::<Pod>::new(file_info.pod_name).within(file_info.pod_namespace);
        let resource = self.pods_state_reader.get(&obj)?;
        let pod: &Pod = resource.as_ref();

        if let Event::Log(otel_log) = event {
            annotate_otel_from_file_info(otel_log, &file_info);
            annotate_otel_from_metadata(otel_log, &pod.metadata);
            if let Some(ref pod_spec) = pod.spec {
                annotate_otel_from_pod_spec(otel_log, pod_spec);
                if let Some(container) = pod_spec
                    .containers
                    .iter()
                    .find(|c| c.name == file_info.container_name)
                {
                    annotate_otel_from_container(otel_log, container);
                }
            }
            if let Some(ref pod_status) = pod.status {
                annotate_otel_from_pod_status(otel_log, pod_status);
                if let Some(ref container_statuses) = pod_status.container_statuses
                    && let Some(container_status) = container_statuses
                        .iter()
                        .find(|c| c.name == file_info.container_name)
                {
                    annotate_otel_from_container_status(otel_log, container_status);
                }
            }
        }
        Some(file_info)
    }
}

#[allow(dead_code)]
fn annotate_from_file_info(log: &mut OtelLog, file_info: &LogFileInfo<'_>) {
    insert_source_metadata(
        Config::NAME,
        log,
        path!("container_name"),
        file_info.container_name.to_owned(),
    );
}

#[allow(dead_code)]
fn annotate_from_metadata(log: &mut OtelLog, metadata: &ObjectMeta) {
    for (metadata_key, value) in [
        (path!("pod_name"), &metadata.name),
        (path!("pod_namespace"), &metadata.namespace),
        (path!("pod_uid"), &metadata.uid),
    ]
    .iter()
    {
        if let Some(value) = value {
            insert_source_metadata(Config::NAME, log, *metadata_key, value.to_owned());
        }
    }

    if let Some(owner_references) = &metadata.owner_references {
        insert_source_metadata(
            Config::NAME,
            log,
            path!("pod_owner"),
            format!("{}/{}", owner_references[0].kind, owner_references[0].name),
        )
    }

    if let Some(labels) = &metadata.labels {
        for (key, value) in labels.iter() {
            insert_source_metadata(
                Config::NAME,
                log,
                path!("pod_labels", key),
                value.to_owned(),
            )
        }
    }

    if let Some(annotations) = &metadata.annotations {
        for (key, value) in annotations.iter() {
            insert_source_metadata(
                Config::NAME,
                log,
                path!("pod_annotations", key),
                value.to_owned(),
            )
        }
    }
}

#[allow(dead_code)]
fn annotate_from_pod_spec(log: &mut OtelLog, pod_spec: &PodSpec) {
    if let Some(value) = &pod_spec.node_name {
        insert_source_metadata(Config::NAME, log, path!("pod_node_name"), value.to_owned())
    }
}

#[allow(dead_code)]
fn annotate_from_pod_status(log: &mut OtelLog, pod_status: &PodStatus) {
    if let Some(value) = &pod_status.pod_ip {
        insert_source_metadata(Config::NAME, log, path!("pod_ip"), value.to_owned())
    }

    if let Some(value) = &pod_status.pod_ips {
        let value = value
            .iter()
            .filter_map(|k| k.ip.clone())
            .collect::<Vec<String>>();

        insert_source_metadata(Config::NAME, log, path!("pod_ips"), value)
    }
}

#[allow(dead_code)]
fn annotate_from_container_status(log: &mut OtelLog, container_status: &ContainerStatus) {
    if let Some(value) = &container_status.container_id {
        insert_source_metadata(Config::NAME, log, path!("container_id"), value.to_owned())
    }

    insert_source_metadata(
        Config::NAME,
        log,
        path!("container_image_id"),
        container_status.image_id.to_owned(),
    )
}

#[allow(dead_code)]
fn annotate_from_container(log: &mut OtelLog, container: &Container) {
    if let Some(value) = &container.image {
        insert_source_metadata(
            Config::NAME,
            log,
            path!("container_image"),
            value.to_owned(),
        )
    }
}

fn annotate_otel_from_file_info(otel_log: &mut crate::event::OtelLog, file_info: &LogFileInfo<'_>) {
    otel_log.set_resource_attribute(
        "k8s.container.name".to_string(),
        string_value(file_info.container_name),
    );
}

fn annotate_otel_from_metadata(otel_log: &mut crate::event::OtelLog, metadata: &ObjectMeta) {
    if let Some(name) = &metadata.name {
        otel_log.set_resource_attribute("k8s.pod.name".to_string(), string_value(name));
    }
    if let Some(ns) = &metadata.namespace {
        otel_log.set_resource_attribute("k8s.namespace.name".to_string(), string_value(ns));
    }
    if let Some(uid) = &metadata.uid {
        otel_log.set_resource_attribute("k8s.pod.uid".to_string(), string_value(uid));
    }
    if let Some(owner_references) = &metadata.owner_references {
        otel_log.set_resource_attribute(
            "k8s.pod.owner".to_string(),
            string_value(format!(
                "{}/{}",
                owner_references[0].kind, owner_references[0].name
            )),
        );
    }
    if let Some(labels) = &metadata.labels {
        for (key, value) in labels.iter() {
            otel_log.set_resource_attribute(format!("k8s.pod.labels.{key}"), string_value(value));
        }
    }
    if let Some(annotations) = &metadata.annotations {
        for (key, value) in annotations.iter() {
            otel_log
                .set_resource_attribute(format!("k8s.pod.annotations.{key}"), string_value(value));
        }
    }
}

fn annotate_otel_from_pod_spec(otel_log: &mut crate::event::OtelLog, pod_spec: &PodSpec) {
    if let Some(node_name) = &pod_spec.node_name {
        otel_log.set_resource_attribute("k8s.node.name".to_string(), string_value(node_name));
    }
}

fn annotate_otel_from_pod_status(otel_log: &mut crate::event::OtelLog, pod_status: &PodStatus) {
    if let Some(pod_ip) = &pod_status.pod_ip {
        otel_log.set_resource_attribute("k8s.pod.ip".to_string(), string_value(pod_ip));
    }
    if let Some(pod_ips) = &pod_status.pod_ips {
        let ips: Vec<String> = pod_ips.iter().filter_map(|k| k.ip.clone()).collect();
        if !ips.is_empty() {
            otel_log.set_resource_attribute("k8s.pod.ips".to_string(), string_value(ips.join(",")));
        }
    }
}

fn annotate_otel_from_container_status(
    otel_log: &mut crate::event::OtelLog,
    container_status: &ContainerStatus,
) {
    if let Some(container_id) = &container_status.container_id {
        otel_log.set_resource_attribute("k8s.container.id".to_string(), string_value(container_id));
    }
    if !container_status.image_id.is_empty() {
        otel_log.set_resource_attribute(
            "k8s.container.image.id".to_string(),
            string_value(&container_status.image_id),
        );
    }
}

fn annotate_otel_from_container(otel_log: &mut crate::event::OtelLog, container: &Container) {
    if let Some(image) = &container.image {
        otel_log
            .set_resource_attribute("k8s.container.image.name".to_string(), string_value(image));
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use k8s_openapi::api::core::v1::PodIP;
    use similar_asserts::assert_eq;
    use sol_lib::lookup::metadata_path;

    use super::*;
    #[allow(unused_imports)]
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
                    namespace: Some("sandbox0-ns".to_owned()),
                    uid: Some("sandbox0-uid".to_owned()),
                    labels: Some(
                        vec![
                            ("sandbox0-label0".to_owned(), "val0".to_owned()),
                            ("sandbox0-label1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    annotations: Some(
                        vec![
                            ("sandbox0-annotation0".to_owned(), "val0".to_owned()),
                            ("sandbox0-annotation1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..ObjectMeta::default()
                },
                {
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_name"),
                        "sandbox0-name",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_namespace"),
                        "sandbox0-ns",
                    );
                    log.insert(metadata_path!("kubernetes_logs", "pod_uid"), "sandbox0-uid");
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "sandbox0-label0"),
                        "val0",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "sandbox0-label1"),
                        "val1",
                    );
                    log.insert(
                        metadata_path!(
                            "kubernetes_logs",
                            "pod_annotations",
                            "sandbox0-annotation0"
                        ),
                        "val0",
                    );
                    log.insert(
                        metadata_path!(
                            "kubernetes_logs",
                            "pod_annotations",
                            "sandbox0-annotation1"
                        ),
                        "val1",
                    );
                    log
                },
            ),
            (
                FieldsSpec::default(),
                ObjectMeta {
                    name: Some("sandbox0-name".to_owned()),
                    namespace: Some("sandbox0-ns".to_owned()),
                    uid: Some("sandbox0-uid".to_owned()),
                    labels: Some(
                        vec![
                            ("sandbox0-label0".to_owned(), "val0".to_owned()),
                            ("sandbox0-label1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    annotations: Some(
                        vec![
                            ("sandbox0-annotation0".to_owned(), "val0".to_owned()),
                            ("sandbox0-annotation1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..ObjectMeta::default()
                },
                {
                    // annotate_from_metadata uses insert_source_metadata
                    // which stores into EventMetadata["kubernetes_logs"][...].
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_name"),
                        "sandbox0-name",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_namespace"),
                        "sandbox0-ns",
                    );
                    log.insert(metadata_path!("kubernetes_logs", "pod_uid"), "sandbox0-uid");
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "sandbox0-label0"),
                        "val0",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "sandbox0-label1"),
                        "val1",
                    );
                    log.insert(
                        metadata_path!(
                            "kubernetes_logs",
                            "pod_annotations",
                            "sandbox0-annotation0"
                        ),
                        "val0",
                    );
                    log.insert(
                        metadata_path!(
                            "kubernetes_logs",
                            "pod_annotations",
                            "sandbox0-annotation1"
                        ),
                        "val1",
                    );
                    log
                },
            ),
            (
                FieldsSpec {
                    pod_name: OwnedTargetPath::event(owned_value_path!("name")).into(),
                    pod_namespace: OwnedTargetPath::event(owned_value_path!("ns")).into(),
                    pod_uid: OwnedTargetPath::event(owned_value_path!("uid")).into(),
                    pod_labels: OwnedTargetPath::event(owned_value_path!("labels")).into(),
                    // ensure we can disable fields
                    pod_annotations: OptionalTargetPath::none(),
                    ..Default::default()
                },
                ObjectMeta {
                    name: Some("sandbox0-name".to_owned()),
                    namespace: Some("sandbox0-ns".to_owned()),
                    uid: Some("sandbox0-uid".to_owned()),
                    labels: Some(
                        vec![
                            ("sandbox0-label0".to_owned(), "val0".to_owned()),
                            ("sandbox0-label1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    annotations: Some(
                        vec![
                            ("sandbox0-annotation0".to_owned(), "val0".to_owned()),
                            ("sandbox0-annotation1".to_owned(), "val1".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..ObjectMeta::default()
                },
                {
                    // annotate_from_metadata ignores FieldsSpec and always uses
                    // insert_source_metadata with fixed keys
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_name"),
                        "sandbox0-name",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_namespace"),
                        "sandbox0-ns",
                    );
                    log.insert(metadata_path!("kubernetes_logs", "pod_uid"), "sandbox0-uid");
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "sandbox0-label0"),
                        "val0",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "sandbox0-label1"),
                        "val1",
                    );
                    // annotations are also stored since annotate_from_metadata doesn't filter
                    log.insert(
                        metadata_path!(
                            "kubernetes_logs",
                            "pod_annotations",
                            "sandbox0-annotation0"
                        ),
                        "val0",
                    );
                    log.insert(
                        metadata_path!(
                            "kubernetes_logs",
                            "pod_annotations",
                            "sandbox0-annotation1"
                        ),
                        "val1",
                    );
                    log
                },
            ),
            // Ensure we properly handle labels with `.` as flat fields.
            (
                FieldsSpec::default(),
                ObjectMeta {
                    name: Some("sandbox0-name".to_owned()),
                    namespace: Some("sandbox0-ns".to_owned()),
                    uid: Some("sandbox0-uid".to_owned()),
                    labels: Some(
                        vec![
                            ("nested0.label0".to_owned(), "val0".to_owned()),
                            ("nested0.label1".to_owned(), "val1".to_owned()),
                            ("nested1.label0".to_owned(), "val2".to_owned()),
                            ("nested2.label0.deep0".to_owned(), "val3".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..ObjectMeta::default()
                },
                {
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_name"),
                        "sandbox0-name",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_namespace"),
                        "sandbox0-ns",
                    );
                    log.insert(metadata_path!("kubernetes_logs", "pod_uid"), "sandbox0-uid");
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "nested0.label0"),
                        "val0",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "nested0.label1"),
                        "val1",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "nested1.label0"),
                        "val2",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "nested2.label0.deep0"),
                        "val3",
                    );
                    log
                },
            ),
            (
                FieldsSpec::default(),
                ObjectMeta {
                    name: Some("sandbox0-name".to_owned()),
                    namespace: Some("sandbox0-ns".to_owned()),
                    uid: Some("sandbox0-uid".to_owned()),
                    labels: Some(
                        vec![
                            ("nested0.label0".to_owned(), "val0".to_owned()),
                            ("nested0.label1".to_owned(), "val1".to_owned()),
                            ("nested1.label0".to_owned(), "val2".to_owned()),
                            ("nested2.label0.deep0".to_owned(), "val3".to_owned()),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    ..ObjectMeta::default()
                },
                {
                    // annotate_from_metadata stores in EventMetadata.
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_name"),
                        "sandbox0-name",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_namespace"),
                        "sandbox0-ns",
                    );
                    log.insert(metadata_path!("kubernetes_logs", "pod_uid"), "sandbox0-uid");
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "nested0.label0"),
                        "val0",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "nested0.label1"),
                        "val1",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "nested1.label0"),
                        "val2",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_labels", "nested2.label0.deep0"),
                        "val3",
                    );
                    log
                },
            ),
        ];

        for (_fields_spec, metadata, expected) in cases.into_iter() {
            let mut log = OtelLog::default();
            annotate_from_metadata(&mut log, &metadata);
            let expected = expected;
            assert_eq!(log, expected);
        }
    }

    #[test]
    fn test_annotate_from_file_info() {
        let path = &format!(
            "{}{}",
            std::path::MAIN_SEPARATOR,
            [
                "var",
                "log",
                "pods",
                "sandbox0-ns_sandbox0-name_sandbox0-uid",
                "sandbox0-container0-name",
                "1.log",
            ]
            .iter()
            .collect::<PathBuf>()
            .into_os_string()
            .into_string()
            .unwrap()
        );
        let s_path = path.as_str();
        let cases = vec![
            (FieldsSpec::default(), s_path, {
                let mut log = OtelLog::default();
                log.insert(
                    metadata_path!("kubernetes_logs", "container_name"),
                    "sandbox0-container0-name",
                );
                log
            }),
            (
                FieldsSpec {
                    container_name: OwnedTargetPath::event(owned_value_path!("container_name"))
                        .into(),
                    ..Default::default()
                },
                s_path,
                {
                    // annotate_from_file_info ignores FieldsSpec; always stores in metadata
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "container_name"),
                        "sandbox0-container0-name",
                    );
                    log
                },
            ),
        ];

        for (_fields_spec, file, expected) in cases.into_iter() {
            let mut log = OtelLog::default();
            let file_info = parse_log_file_path(file).unwrap();
            annotate_from_file_info(&mut log, &file_info);
            let expected = expected;
            assert_eq!(log, expected);
        }
    }

    #[test]
    fn test_annotate_from_pod_spec() {
        let cases = vec![
            (
                FieldsSpec::default(),
                PodSpec::default(),
                OtelLog::default(),
            ),
            (
                FieldsSpec::default(),
                PodSpec {
                    node_name: Some("sandbox0-node-name".to_owned()),
                    ..Default::default()
                },
                {
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_node_name"),
                        "sandbox0-node-name",
                    );
                    log
                },
            ),
            (
                FieldsSpec {
                    pod_node_name: OwnedTargetPath::event(owned_value_path!("node_name")).into(),
                    ..Default::default()
                },
                PodSpec {
                    node_name: Some("sandbox0-node-name".to_owned()),
                    ..Default::default()
                },
                {
                    // annotate_from_pod_spec ignores FieldsSpec; always stores in metadata
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "pod_node_name"),
                        "sandbox0-node-name",
                    );
                    log
                },
            ),
        ];

        for (_fields_spec, pod_spec, expected) in cases.into_iter() {
            let mut log = OtelLog::default();
            annotate_from_pod_spec(&mut log, &pod_spec);
            let expected = expected;
            assert_eq!(log, expected);
        }
    }

    #[test]
    fn test_annotate_from_pod_status() {
        let cases = vec![
            (
                FieldsSpec::default(),
                PodStatus::default(),
                OtelLog::default(),
            ),
            (
                FieldsSpec::default(),
                PodStatus {
                    pod_ip: Some("192.168.1.2".to_owned()),
                    ..Default::default()
                },
                {
                    let mut log = OtelLog::default();
                    log.insert(metadata_path!("kubernetes_logs", "pod_ip"), "192.168.1.2");
                    log
                },
            ),
            (
                FieldsSpec::default(),
                PodStatus {
                    pod_ips: Some(vec![PodIP {
                        ip: Some("192.168.1.2".to_owned()),
                    }]),
                    ..Default::default()
                },
                {
                    let mut log = OtelLog::default();
                    let ips_vec = vec!["192.168.1.2"];
                    log.insert(metadata_path!("kubernetes_logs", "pod_ips"), ips_vec);
                    log
                },
            ),
            (
                FieldsSpec {
                    pod_ip: OwnedTargetPath::event(owned_value_path!(
                        "kubernetes",
                        "custom_pod_ip"
                    ))
                    .into(),
                    pod_ips: OwnedTargetPath::event(owned_value_path!(
                        "kubernetes",
                        "custom_pod_ips"
                    ))
                    .into(),
                    ..FieldsSpec::default()
                },
                PodStatus {
                    pod_ip: Some("192.168.1.2".to_owned()),
                    pod_ips: Some(vec![
                        PodIP {
                            ip: Some("192.168.1.2".to_owned()),
                        },
                        PodIP {
                            ip: Some("192.168.1.3".to_owned()),
                        },
                    ]),
                    ..Default::default()
                },
                {
                    // annotate_from_pod_status ignores FieldsSpec; always stores in metadata
                    let mut log = OtelLog::default();
                    log.insert(metadata_path!("kubernetes_logs", "pod_ip"), "192.168.1.2");
                    let ips_vec = vec!["192.168.1.2", "192.168.1.3"];
                    log.insert(metadata_path!("kubernetes_logs", "pod_ips"), ips_vec);
                    log
                },
            ),
            (
                FieldsSpec {
                    pod_node_name: OwnedTargetPath::event(owned_value_path!("node_name")).into(),
                    ..FieldsSpec::default()
                },
                PodStatus {
                    pod_ip: Some("192.168.1.2".to_owned()),
                    pod_ips: Some(vec![
                        PodIP {
                            ip: Some("192.168.1.2".to_owned()),
                        },
                        PodIP {
                            ip: Some("192.168.1.3".to_owned()),
                        },
                    ]),
                    ..Default::default()
                },
                {
                    // annotate_from_pod_status ignores FieldsSpec; always stores in metadata
                    let mut log = OtelLog::default();
                    log.insert(metadata_path!("kubernetes_logs", "pod_ip"), "192.168.1.2");
                    let ips_vec = vec!["192.168.1.2", "192.168.1.3"];
                    log.insert(metadata_path!("kubernetes_logs", "pod_ips"), ips_vec);
                    log
                },
            ),
        ];

        for (_fields_spec, pod_status, expected) in cases.into_iter() {
            let mut log = OtelLog::default();
            annotate_from_pod_status(&mut log, &pod_status);
            let expected = expected;
            assert_eq!(log, expected);
        }
    }

    #[test]
    fn test_annotate_from_container_status() {
        let cases = vec![
            (FieldsSpec::default(), ContainerStatus::default(), {
                let mut log = OtelLog::default();
                log.insert(metadata_path!("kubernetes_logs", "container_image_id"), "");
                log
            }),
            (
                FieldsSpec {
                    ..FieldsSpec::default()
                },
                ContainerStatus {
                    container_id: Some("container_id_foo".to_owned()),
                    image_id: "test_image_id".to_owned(),
                    ..ContainerStatus::default()
                },
                {
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "container_id"),
                        "container_id_foo",
                    );
                    log.insert(
                        metadata_path!("kubernetes_logs", "container_image_id"),
                        "test_image_id",
                    );
                    log
                },
            ),
        ];
        for (_fields_spec, container_status, expected) in cases.into_iter() {
            let mut log = OtelLog::default();
            annotate_from_container_status(&mut log, &container_status);
            let expected = expected;
            assert_eq!(log, expected);
        }
    }

    #[test]
    fn test_annotate_from_container() {
        let cases = vec![
            (
                FieldsSpec::default(),
                Container::default(),
                OtelLog::default(),
            ),
            (
                FieldsSpec::default(),
                Container {
                    image: Some("sandbox0-container-image".to_owned()),
                    ..Default::default()
                },
                {
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "container_image"),
                        "sandbox0-container-image",
                    );
                    log
                },
            ),
            (
                FieldsSpec {
                    container_image: OwnedTargetPath::event(owned_value_path!("container_image"))
                        .into(),
                    ..Default::default()
                },
                Container {
                    image: Some("sandbox0-container-image".to_owned()),
                    ..Default::default()
                },
                {
                    let mut log = OtelLog::default();
                    log.insert(
                        metadata_path!("kubernetes_logs", "container_image"),
                        "sandbox0-container-image",
                    );
                    log
                },
            ),
        ];

        for (_fields_spec, container, expected) in cases.into_iter() {
            let mut log = OtelLog::default();
            annotate_from_container(&mut log, &container);
            let expected = expected;
            assert_eq!(log, expected);
        }
    }
}
