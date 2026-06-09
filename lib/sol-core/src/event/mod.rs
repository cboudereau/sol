use std::{convert::TryInto, fmt::Debug, sync::Arc};

pub use array::{
    EventArray, EventContainer, LogArray, MetricArray, OtelLogArray, OtelMetricArray,
    OtelSpanArray, TraceArray, into_event_stream,
};
pub use estimated_json_encoded_size_of::EstimatedJsonEncodedSizeOf;
pub use finalization::{
    BatchNotifier, BatchStatus, BatchStatusReceiver, EventFinalizer, EventFinalizers, EventStatus,
    Finalizable,
};
pub use metadata::{EventMetadata, WithMetadata};
pub use metric::MetricKind;
pub use r#ref::{EventMutRef, EventRef};
use serde::Serialize;
use sol_buffers::EventCount;
use sol_common::{
    EventDataEq, byte_size_of::ByteSizeOf, config::ComponentKey, finalization,
    internal_event::TaggedEventsSent, json_size::JsonSize, request_metadata::GetEventCountTags,
};
pub use vrl::value::{KeyString, ObjectMap, Value};
#[cfg(feature = "vrl")]
pub use vrl_target::{TargetEvents, VrlTarget};

use crate::config::OutputId;

pub mod array;
pub mod discriminant;
mod estimated_json_encoded_size_of;
#[cfg(feature = "lua")]
pub mod lua;
pub mod merge_state;
mod metadata;
pub mod metric;
pub mod otel_attributes;
pub(crate) mod otel_conv;
pub mod otel_event;
pub mod otel_fields;
pub mod otel_json;
pub mod otel_metric;
pub mod otlp;
pub mod prom_name;
mod r#ref;
mod ser;
pub use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValueKind;
pub use otel_attributes::OtelAttributes;
pub use otel_event::{
    AnyValue, OtelLog, OtelSpan, ValueAtQuantile, int_value, json_to_any_value, string_value,
    vrl_value_to_any_value,
};
pub use otel_metric::{MetricView, OtelMetric};

pub use otlp::{OtlpCodec, register_otlp_codec};
pub use ser::EventEncodableMetadata;
#[cfg(test)]
mod test;
pub mod util;
#[cfg(feature = "vrl")]
mod vrl_target;

pub const PARTIAL: &str = "_partial";

// Event is Serialize-only; no production path deserializes it (disk buffers
// use OtlpCodec, OTLP HTTP uses the otel_json module directly).
// Adding Deserialize required per-variant
// Deserialize impls on OtelLog/OtelSpan/OtelMetric which had zero live
// callers — removed for clarity.
#[derive(PartialEq, Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    Log(OtelLog),
    Metric(OtelMetric),
    Trace(OtelSpan),
}

impl ByteSizeOf for Event {
    fn allocated_bytes(&self) -> usize {
        match self {
            Event::Log(e) => e.allocated_bytes(),
            Event::Metric(e) => e.allocated_bytes(),
            Event::Trace(e) => e.allocated_bytes(),
        }
    }
}

impl EstimatedJsonEncodedSizeOf for Event {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        match self {
            Event::Log(e) => e.estimated_json_encoded_size_of(),
            Event::Metric(e) => e.estimated_json_encoded_size_of(),
            Event::Trace(e) => e.estimated_json_encoded_size_of(),
        }
    }
}

impl EventCount for Event {
    fn event_count(&self) -> usize {
        1
    }
}

impl Finalizable for Event {
    fn take_finalizers(&mut self) -> EventFinalizers {
        match self {
            Event::Log(e) => e.take_finalizers(),
            Event::Metric(e) => e.take_finalizers(),
            Event::Trace(e) => e.take_finalizers(),
        }
    }
}

impl GetEventCountTags for Event {
    fn get_tags(&self) -> TaggedEventsSent {
        match self {
            Event::Log(e) => e.get_tags(),
            Event::Metric(e) => e.get_tags(),
            Event::Trace(e) => e.get_tags(),
        }
    }
}

impl Event {
    /// Return self as an `OtelLog` reference.
    ///
    /// # Panics
    ///
    /// This function panics if self is not an `Event::Log`.
    pub fn as_log(&self) -> &OtelLog {
        match self {
            Event::Log(log) => log,
            _ => panic!("Failed type coercion, {self:?} is not a log event"),
        }
    }

    /// Return self as a mutable `OtelLog` reference.
    ///
    /// # Panics
    ///
    /// This function panics if self is not an `Event::Log`.
    pub fn as_mut_log(&mut self) -> &mut OtelLog {
        match self {
            Event::Log(log) => log,
            _ => panic!("Failed type coercion, {self:?} is not a log event"),
        }
    }

    /// Coerces self into an `OtelLog`.
    ///
    /// # Panics
    ///
    /// This function panics if self is not an `Event::Log`.
    pub fn into_log(self) -> OtelLog {
        match self {
            Event::Log(log) => log,
            _ => panic!("Failed type coercion, {self:?} is not a log event"),
        }
    }

    /// Fallibly coerces self into an `OtelLog`.
    pub fn try_into_log(self) -> Option<OtelLog> {
        match self {
            Event::Log(log) => Some(log),
            _ => None,
        }
    }

    /// Return self as an `OtelLog` if possible.
    pub fn maybe_as_log(&self) -> Option<&OtelLog> {
        match self {
            Event::Log(log) => Some(log),
            _ => None,
        }
    }

    /// Return self as an `OtelMetric` reference.
    ///
    /// # Panics
    ///
    /// This function panics if self is not an `Event::Metric`.
    pub fn as_metric(&self) -> &OtelMetric {
        match self {
            Event::Metric(metric) => metric,
            _ => panic!("Failed type coercion, {self:?} is not a metric"),
        }
    }

    /// Return self as a mutable `OtelMetric` reference.
    ///
    /// # Panics
    ///
    /// This function panics if self is not an `Event::Metric`.
    pub fn as_mut_metric(&mut self) -> &mut OtelMetric {
        match self {
            Event::Metric(metric) => metric,
            _ => panic!("Failed type coercion, {self:?} is not a metric"),
        }
    }

    /// Return self as an `OtelSpan` reference.
    ///
    /// # Panics
    ///
    /// This function panics if self is not an `Event::Trace`.
    pub fn as_trace(&self) -> &OtelSpan {
        match self {
            Event::Trace(trace) => trace,
            _ => panic!("Failed type coercion, {self:?} is not a trace event"),
        }
    }

    /// Return self as a mutable `OtelSpan` reference.
    ///
    /// # Panics
    ///
    /// This function panics if self is not an `Event::Trace`.
    pub fn as_mut_trace(&mut self) -> &mut OtelSpan {
        match self {
            Event::Trace(trace) => trace,
            _ => panic!("Failed type coercion, {self:?} is not a trace event"),
        }
    }

    /// Convert to a JSON value for serialization.
    /// Wraps the event in `{"log": ...}`, `{"metric": ...}`, or `{"trace": ...}`.
    pub fn to_legacy_json_value(self) -> serde_json::Value {
        match self {
            Event::Log(log) => {
                let v = serde_json::to_value(&log).unwrap_or(serde_json::Value::Null);
                serde_json::json!({"log": v})
            }
            Event::Metric(metric) => {
                let v = serde_json::to_value(&metric).unwrap_or(serde_json::Value::Null);
                serde_json::json!({"metric": v})
            }
            Event::Trace(trace) => {
                let v = serde_json::to_value(&trace).unwrap_or(serde_json::Value::Null);
                serde_json::json!({"trace": v})
            }
        }
    }

    /// Coerces self into an `OtelSpan`.
    ///
    /// # Panics
    ///
    /// This function panics if self is not an `Event::Trace`.
    pub fn into_trace(self) -> OtelSpan {
        match self {
            Event::Trace(trace) => trace,
            _ => panic!("Failed type coercion, {self:?} is not a trace event"),
        }
    }

    /// Fallibly coerces self into an `OtelSpan`.
    pub fn try_into_trace(self) -> Option<OtelSpan> {
        match self {
            Event::Trace(trace) => Some(trace),
            _ => None,
        }
    }

    pub fn metadata(&self) -> &EventMetadata {
        match self {
            Self::Log(e) => e.metadata(),
            Self::Metric(e) => e.metadata(),
            Self::Trace(e) => e.metadata(),
        }
    }

    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        match self {
            Self::Log(e) => e.metadata_mut(),
            Self::Metric(e) => e.metadata_mut(),
            Self::Trace(e) => e.metadata_mut(),
        }
    }

    /// Destroy the event and return the metadata.
    pub fn into_metadata(self) -> EventMetadata {
        match self {
            Self::Log(e) => e.into_parts().3,
            Self::Metric(e) => e.into_parts().3,
            Self::Trace(e) => e.into_parts().3,
        }
    }

    #[must_use]
    pub fn with_batch_notifier(self, batch: &BatchNotifier) -> Self {
        match self {
            Self::Log(e) => e.with_batch_notifier(batch).into(),
            Self::Metric(e) => e.with_batch_notifier(batch).into(),
            Self::Trace(e) => e.with_batch_notifier(batch).into(),
        }
    }

    #[must_use]
    pub fn with_batch_notifier_option(self, batch: &Option<BatchNotifier>) -> Self {
        match self {
            Self::Log(e) => e.with_batch_notifier_option(batch).into(),
            Self::Metric(e) => e.with_batch_notifier_option(batch).into(),
            Self::Trace(e) => e.with_batch_notifier_option(batch).into(),
        }
    }

    /// Backward-compat aliases — these just delegate to the renamed variants.
    pub fn as_otel_log(&self) -> &OtelLog {
        self.as_log()
    }
    pub fn as_mut_otel_log(&mut self) -> &mut OtelLog {
        self.as_mut_log()
    }
    pub fn into_otel_log(self) -> OtelLog {
        self.into_log()
    }
    pub fn try_into_otel_log(self) -> Option<OtelLog> {
        self.try_into_log()
    }
    pub fn maybe_as_otel_log(&self) -> Option<&OtelLog> {
        self.maybe_as_log()
    }
    pub fn as_otel_span(&self) -> &OtelSpan {
        self.as_trace()
    }
    pub fn as_mut_otel_span(&mut self) -> &mut OtelSpan {
        self.as_mut_trace()
    }
    pub fn into_otel_span(self) -> OtelSpan {
        self.into_trace()
    }
    pub fn try_into_otel_span(self) -> Option<OtelSpan> {
        self.try_into_trace()
    }
    pub fn as_otel_metric(&self) -> &OtelMetric {
        self.as_metric()
    }
    pub fn as_mut_otel_metric(&mut self) -> &mut OtelMetric {
        self.as_mut_metric()
    }
    /// # Panics
    /// Panics if the event is not a metric.
    pub fn into_otel_metric(self) -> OtelMetric {
        match self {
            Event::Metric(e) => e,
            _ => panic!("not a metric"),
        }
    }
    pub fn try_into_otel_metric(self) -> Option<OtelMetric> {
        match self {
            Event::Metric(e) => Some(e),
            _ => None,
        }
    }

    /// Returns a reference to the event metadata source.
    #[must_use]
    pub fn source_id(&self) -> Option<&Arc<ComponentKey>> {
        self.metadata().source_id()
    }

    /// Sets the `source_id` in the event metadata to the provided value.
    pub fn set_source_id(&mut self, source_id: Arc<ComponentKey>) {
        self.metadata_mut().set_source_id(source_id);
    }

    /// Sets the `upstream_id` in the event metadata to the provided value.
    pub fn set_upstream_id(&mut self, upstream_id: Arc<OutputId>) {
        self.metadata_mut().set_upstream_id(upstream_id);
    }

    /// Sets the `source_type` in the event metadata to the provided value.
    pub fn set_source_type(&mut self, source_type: &'static str) {
        self.metadata_mut().set_source_type(source_type);
    }

    /// Sets the `source_id` in the event metadata to the provided value.
    #[must_use]
    pub fn with_source_id(mut self, source_id: Arc<ComponentKey>) -> Self {
        self.metadata_mut().set_source_id(source_id);
        self
    }

    /// Sets the `source_type` in the event metadata to the provided value.
    #[must_use]
    pub fn with_source_type(mut self, source_type: &'static str) -> Self {
        self.metadata_mut().set_source_type(source_type);
        self
    }

    /// Sets the `upstream_id` in the event metadata to the provided value.
    #[must_use]
    pub fn with_upstream_id(mut self, upstream_id: Arc<OutputId>) -> Self {
        self.metadata_mut().set_upstream_id(upstream_id);
        self
    }

    /// Creates an Event from a JSON value.
    ///
    /// # Errors
    /// Returns an error if the JSON value cannot be converted.
    pub fn from_json_value(value: serde_json::Value) -> crate::Result<Self> {
        Ok(Event::Log(OtelLog::from(Value::from(value))))
    }
}

impl EventDataEq for Event {
    fn event_data_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Log(a), Self::Log(b)) => a.event_data_eq(b),
            (Self::Metric(a), Self::Metric(b)) => a.event_data_eq(b),
            (Self::Trace(a), Self::Trace(b)) => a.event_data_eq(b),
            _ => false,
        }
    }
}

impl finalization::AddBatchNotifier for Event {
    fn add_batch_notifier(&mut self, batch: BatchNotifier) {
        let finalizer = EventFinalizer::new(batch);
        match self {
            Self::Log(e) => e.add_finalizer(finalizer),
            Self::Metric(e) => e.add_finalizer(finalizer),
            Self::Trace(e) => e.add_finalizer(finalizer),
        }
    }
}

impl TryInto<serde_json::Value> for Event {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<serde_json::Value, Self::Error> {
        match self {
            Event::Log(e) => serde_json::to_value(e),
            Event::Metric(e) => serde_json::to_value(e),
            Event::Trace(e) => serde_json::to_value(e),
        }
    }
}

impl From<OtelLog> for Event {
    fn from(e: OtelLog) -> Self {
        Event::Log(e)
    }
}

impl From<OtelMetric> for Event {
    fn from(e: OtelMetric) -> Self {
        Event::Metric(e)
    }
}

impl From<OtelSpan> for Event {
    fn from(e: OtelSpan) -> Self {
        Event::Trace(e)
    }
}

pub trait MaybeAsLogMut {
    fn maybe_as_log_mut(&mut self) -> Option<&mut OtelLog>;
}

impl MaybeAsLogMut for Event {
    fn maybe_as_log_mut(&mut self) -> Option<&mut OtelLog> {
        match self {
            Event::Log(log) => Some(log),
            _ => None,
        }
    }
}
