use std::{collections::HashMap, fmt, num::NonZeroUsize, sync::Arc};

use bitmask_enum::bitmask;
use chrono::{DateTime, Utc};

mod global_options;
pub(crate) mod metrics_expiration;
pub mod output_id;
pub mod proxy;
mod telemetry;

pub use global_options::{GlobalOptions, WildcardMatching};
use lookup::{PathPrefix, lookup_v2::ValuePath, path};
pub use output_id::OutputId;
pub use sol_common::config::ComponentKey;
use sol_config::configurable_component;
pub use telemetry::{Tags, Telemetry, init_telemetry, telemetry};
use vrl::value::Value;

use crate::{
    event::{EventMetadata, OtelLog},
    schema,
};

/// Trait for event types that support the metadata insertion patterns used
/// by `insert_source_metadata` / `insert_vector_metadata`.
///
/// `OtelLog` implements this so that sources can produce `OtelLog` directly
/// without changing the insertion API.
pub trait MetadataInsertable {
    fn insert_by_path<'a>(&mut self, path: impl ValuePath<'a>, value: impl Into<Value>);
    fn try_insert_by_path<'a>(&mut self, path: impl ValuePath<'a>, value: impl Into<Value>);
    fn maybe_insert_by_path<'a>(
        &mut self,
        path: Option<impl ValuePath<'a>>,
        value: impl Into<Value>,
    );
    fn event_metadata_mut(&mut self) -> &mut EventMetadata;
    fn set_source_type(&mut self, value: impl Into<Value>);
    fn try_set_source_type(&mut self, value: impl Into<Value>);
    fn set_host(&mut self, value: impl Into<Value>);
    fn try_set_host(&mut self, value: impl Into<Value>);
}

impl MetadataInsertable for OtelLog {
    fn insert_by_path<'a>(&mut self, path: impl ValuePath<'a>, value: impl Into<Value>) {
        self.insert((PathPrefix::Event, path), value);
    }
    fn try_insert_by_path<'a>(&mut self, path: impl ValuePath<'a>, value: impl Into<Value>) {
        self.try_insert((PathPrefix::Event, path), value);
    }
    fn maybe_insert_by_path<'a>(
        &mut self,
        path: Option<impl ValuePath<'a>>,
        value: impl Into<Value>,
    ) {
        if let Some(path) = path {
            self.insert((PathPrefix::Event, path), value);
        }
    }
    fn event_metadata_mut(&mut self) -> &mut EventMetadata {
        self.metadata_mut()
    }
    fn set_source_type(&mut self, value: impl Into<Value>) {
        OtelLog::set_source_type(self, value);
    }
    fn try_set_source_type(&mut self, value: impl Into<Value>) {
        OtelLog::try_set_source_type(self, value);
    }
    fn set_host(&mut self, value: impl Into<Value>) {
        OtelLog::set_host(self, value);
    }
    fn try_set_host(&mut self, value: impl Into<Value>) {
        OtelLog::try_set_host(self, value);
    }
}

pub const MEMORY_BUFFER_DEFAULT_MAX_EVENTS: NonZeroUsize =
    sol_buffers::config::memory_buffer_default_max_events();

// This enum should be kept alphabetically sorted as the bitmask value is used when
// sorting sources by data type in the GraphQL API.
#[bitmask(u8)]
#[bitmask_config(flags_iter)]
pub enum DataType {
    Log,
    Metric,
    Trace,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list()
            .entries(
                Self::flags().filter_map(|&(name, value)| self.contains(value).then_some(name)),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Input {
    ty: DataType,
    log_schema_requirement: schema::Requirement,
}

impl Input {
    pub fn data_type(&self) -> DataType {
        self.ty
    }

    pub fn schema_requirement(&self) -> &schema::Requirement {
        &self.log_schema_requirement
    }

    pub fn new(ty: DataType) -> Self {
        Self {
            ty,
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    pub fn log() -> Self {
        Self {
            ty: DataType::Log,
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    pub fn metric() -> Self {
        Self {
            ty: DataType::Metric,
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    pub fn trace() -> Self {
        Self {
            ty: DataType::Trace,
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    pub fn all() -> Self {
        Self {
            ty: DataType::all_bits(),
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    /// Set the schema requirement for this output.
    #[must_use]
    pub fn with_schema_requirement(mut self, schema_requirement: schema::Requirement) -> Self {
        self.log_schema_requirement = schema_requirement;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceOutput {
    pub port: Option<String>,
    pub ty: DataType,

    // NOTE: schema definitions are only implemented/supported for log-type events. There is no
    // inherent blocker to support other types as well, but it'll require additional work to add
    // the relevant schemas, and store them separately in this type.
    pub schema_definition: Option<Arc<schema::Definition>>,
}

impl SourceOutput {
    /// Create a `SourceOutput` of the given data type that contains a single output `Definition`.
    /// If the data type does not contain logs, the schema definition will be ignored.
    /// Designed for use in log sources.
    #[must_use]
    pub fn new_maybe_logs(ty: DataType, schema_definition: schema::Definition) -> Self {
        let schema_definition = ty
            .contains(DataType::Log)
            .then(|| Arc::new(schema_definition));

        Self {
            port: None,
            ty,
            schema_definition,
        }
    }

    /// Create a `SourceOutput` of the given data type that contains no output `Definition`s.
    /// Designed for use in metrics sources.
    ///
    /// Sets the datatype to be [`DataType::Metric`].
    #[must_use]
    pub fn new_metrics() -> Self {
        Self {
            port: None,
            ty: DataType::Metric,
            schema_definition: None,
        }
    }

    /// Create a `SourceOutput` of the given data type that contains no output `Definition`s.
    /// Designed for use in trace sources.
    ///
    /// Sets the datatype to be [`DataType::Trace`].
    #[must_use]
    pub fn new_traces() -> Self {
        Self {
            port: None,
            ty: DataType::Trace,
            schema_definition: None,
        }
    }

    /// Return the schema [`schema::Definition`] from this output.
    ///
    /// Takes a `schema_enabled` flag to determine if the full definition including the fields
    /// and associated types should be returned, or if a simple definition should be returned.
    /// A simple definition is just the default for the namespace. For the Vector namespace the
    /// meanings are included.
    /// Schema enabled is set in the users configuration.
    #[must_use]
    pub fn schema_definition(&self, schema_enabled: bool) -> Option<schema::Definition> {
        use std::ops::Deref;

        self.schema_definition.as_ref().map(|definition| {
            if schema_enabled {
                definition.deref().clone()
            } else {
                let mut new_definition = schema::Definition::default_definition();
                new_definition.add_meanings(definition.meanings());
                new_definition
            }
        })
    }
}

impl SourceOutput {
    /// Set the port name for this `SourceOutput`.
    #[must_use]
    pub fn with_port(mut self, name: impl Into<String>) -> Self {
        self.port = Some(name.into());
        self
    }
}

fn fmt_helper(
    f: &mut fmt::Formatter<'_>,
    maybe_port: Option<&String>,
    data_type: DataType,
) -> fmt::Result {
    match maybe_port {
        Some(port) => write!(f, "port: \"{port}\",",),
        None => write!(f, "port: None,"),
    }?;
    write!(f, " types: {data_type}")
}

impl fmt::Display for SourceOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_helper(f, self.port.as_ref(), self.ty)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransformOutput {
    pub port: Option<String>,
    pub ty: DataType,

    /// For *transforms* if `Datatype` is [`DataType::Log`], if schema is
    /// enabled, at least one definition  should be output. If the transform
    /// has multiple connected sources, it is possible to have multiple output
    /// definitions - one for each input.
    pub log_schema_definitions: HashMap<OutputId, schema::Definition>,
}

impl fmt::Display for TransformOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_helper(f, self.port.as_ref(), self.ty)
    }
}

impl TransformOutput {
    /// Create a `TransformOutput` of the given data type that contains multiple [`schema::Definition`]s.
    /// Designed for use in transforms.
    #[must_use]
    pub fn new(ty: DataType, schema_definitions: HashMap<OutputId, schema::Definition>) -> Self {
        Self {
            port: None,
            ty,
            log_schema_definitions: schema_definitions,
        }
    }

    /// Set the port name for this `Output`.
    #[must_use]
    pub fn with_port(mut self, name: impl Into<String>) -> Self {
        self.port = Some(name.into());
        self
    }

    /// Return the schema [`schema::Definition`] from this output.
    ///
    /// Takes a `schema_enabled` flag to determine if the full definition including the fields
    /// and associated types should be returned, or if a simple definition should be returned.
    /// A simple definition is just the default for the namespace. For the Vector namespace the
    /// meanings are included.
    /// Schema enabled is set in the users configuration.
    #[must_use]
    pub fn schema_definitions(
        &self,
        schema_enabled: bool,
    ) -> HashMap<OutputId, schema::Definition> {
        if schema_enabled {
            self.log_schema_definitions.clone()
        } else {
            self.log_schema_definitions
                .iter()
                .map(|(output, definition)| {
                    let mut new_definition = schema::Definition::default_definition();
                    new_definition.add_meanings(definition.meanings());
                    (output.clone(), new_definition)
                })
                .collect()
        }
    }
}

/// Simple utility function that can be used by transforms that make no changes to
/// the schema definitions of events.
/// Takes a list of definitions with [`OutputId`] returns them as a [`HashMap`].
pub fn clone_input_definitions(
    input_definitions: &[(OutputId, schema::Definition)],
) -> HashMap<OutputId, schema::Definition> {
    input_definitions
        .iter()
        .map(|(output, definition)| (output.clone(), definition.clone()))
        .collect()
}

/// Source-specific end-to-end acknowledgements configuration.
///
/// This type exists solely to provide a source-specific description of the `acknowledgements`
/// setting, as it is deprecated, and we still need to maintain a way to expose it in the
/// documentation before it's removed while also making sure people know it shouldn't be used.
#[configurable_component]
#[configurable(deprecated)]
#[configurable(title = "Controls how acknowledgements are handled by this source.")]
#[configurable(
    description = "This setting is **deprecated** in favor of enabling `acknowledgements` at the [global][global_acks] or sink level.

Enabling or disabling acknowledgements at the source level has **no effect** on acknowledgement behavior.

See [End-to-end Acknowledgements][e2e_acks] for more information on how event acknowledgement is handled.

[global_acks]: https://vector.dev/docs/reference/configuration/global-options/#acknowledgements
[e2e_acks]: https://vector.dev/docs/architecture/end-to-end-acknowledgements/"
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceAcknowledgementsConfig {
    /// Whether or not end-to-end acknowledgements are enabled for this source.
    enabled: Option<bool>,
}

impl SourceAcknowledgementsConfig {
    pub const DEFAULT: Self = Self { enabled: None };

    #[must_use]
    pub fn merge_default(&self, other: &Self) -> Self {
        let enabled = self.enabled.or(other.enabled);
        Self { enabled }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }
}

impl From<Option<bool>> for SourceAcknowledgementsConfig {
    fn from(enabled: Option<bool>) -> Self {
        Self { enabled }
    }
}

impl From<bool> for SourceAcknowledgementsConfig {
    fn from(enabled: bool) -> Self {
        Some(enabled).into()
    }
}

impl From<SourceAcknowledgementsConfig> for AcknowledgementsConfig {
    fn from(config: SourceAcknowledgementsConfig) -> Self {
        Self {
            enabled: config.enabled,
        }
    }
}

/// End-to-end acknowledgements configuration.
#[configurable_component]
#[configurable(title = "Controls how acknowledgements are handled for this sink.")]
#[configurable(
    description = "See [End-to-end Acknowledgements][e2e_acks] for more information on how event acknowledgement is handled.

[e2e_acks]: https://vector.dev/docs/architecture/end-to-end-acknowledgements/"
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AcknowledgementsConfig {
    /// Controls whether or not end-to-end acknowledgements are enabled.
    ///
    /// When enabled for a sink, any source that supports end-to-end
    /// acknowledgements that is connected to that sink waits for events
    /// to be acknowledged by **all connected sinks** before acknowledging them at the source.
    ///
    /// Enabling or disabling acknowledgements at the sink level takes precedence over any global
    /// [`acknowledgements`][global_acks] configuration.
    ///
    /// [global_acks]: https://vector.dev/docs/reference/configuration/global-options/#acknowledgements
    enabled: Option<bool>,
}

impl AcknowledgementsConfig {
    pub const DEFAULT: Self = Self { enabled: None };

    #[must_use]
    pub fn merge_default(&self, other: &Self) -> Self {
        let enabled = self.enabled.or(other.enabled);
        Self { enabled }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.unwrap_or(false)
    }
}

impl From<Option<bool>> for AcknowledgementsConfig {
    fn from(enabled: Option<bool>) -> Self {
        Self { enabled }
    }
}

impl From<bool> for AcknowledgementsConfig {
    fn from(enabled: bool) -> Self {
        Some(enabled).into()
    }
}

/// Adds metadata to "event metadata", nested under the source name.
pub fn insert_source_metadata<'a>(
    source_name: &'a str,
    log: &mut impl MetadataInsertable,
    metadata_key: impl ValuePath<'a>,
    value: impl Into<Value>,
) {
    log.event_metadata_mut()
        .value_mut()
        .insert(path!(source_name).concat(metadata_key), value);
}

/// Retrieves metadata from "event metadata", nested under the source name.
pub fn get_source_metadata<'a>(
    source_name: &'a str,
    log: &OtelLog,
    metadata_key: impl ValuePath<'a>,
) -> Option<Value> {
    log.metadata()
        .value()
        .get(path!(source_name).concat(metadata_key))
        .cloned()
}

/// Adds `source_type` and `ingest_timestamp` to "event metadata" under "vector".
pub fn insert_standard_vector_source_metadata(
    log: &mut impl MetadataInsertable,
    source_name: &'static str,
    now: DateTime<Utc>,
) {
    log.event_metadata_mut()
        .value_mut()
        .insert(path!("vector", "source_type"), source_name);
    insert_vector_metadata(log, path!("ingest_timestamp"), now);
}

/// Adds metadata to "event metadata" under "vector".
pub fn insert_vector_metadata<'a>(
    log: &mut impl MetadataInsertable,
    metadata_key: impl ValuePath<'a>,
    value: impl Into<Value>,
) {
    log.event_metadata_mut()
        .value_mut()
        .insert(path!("vector").concat(metadata_key), value);
}

/// Retrieves metadata from "event metadata" under "vector".
pub fn get_vector_metadata<'a>(log: &OtelLog, metadata_key: impl ValuePath<'a>) -> Option<Value> {
    log.metadata()
        .value()
        .get(path!("vector").concat(metadata_key))
        .cloned()
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use super::*;
    use crate::event::OtelLog;

    #[test]
    fn test_insert_standard_vector_source_metadata() {
        let mut event = OtelLog::from("log");
        insert_standard_vector_source_metadata(&mut event, "source", Utc::now());

        let source_type = event
            .metadata()
            .value()
            .get(path!("vector", "source_type"))
            .cloned();
        assert!(source_type.is_some());
    }

    #[test]
    fn test_new_log_source_ignores_definition_with_metric_data_type() {
        let definition = schema::Definition::any();
        let output = SourceOutput::new_maybe_logs(DataType::Metric, definition);
        assert_eq!(output.schema_definition(true), None);
    }

    #[test]
    fn test_new_log_source_uses_definition_with_log_data_type() {
        let definition = schema::Definition::any();
        let output = SourceOutput::new_maybe_logs(DataType::Log, definition.clone());
        assert_eq!(output.schema_definition(true), Some(definition));
    }
}
