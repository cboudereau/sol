#![deny(missing_docs)]

use sol_common::EventDataEq;

use super::{Event, EventMetadata, OtelLog, OtelMetric, OtelSpan};

/// A wrapper for references to inner event types, where reconstituting
/// a full `Event` from an `OtelLog` or `OtelMetric` might be inconvenient.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum EventRef<'a> {
    /// Reference to an `OtelLog`
    Log(&'a OtelLog),
    /// Reference to an `OtelMetric`
    Metric(&'a OtelMetric),
    /// Reference to an `OtelSpan`
    Trace(&'a OtelSpan),
}

impl<'a> EventRef<'a> {
    /// Extract the `OtelLog` reference in this.
    ///
    /// # Panics
    ///
    /// This will panic if this is not a `Log` reference.
    pub fn as_log(self) -> &'a OtelLog {
        match self {
            Self::Log(log) => log,
            _ => panic!("Failed type coercion, {self:?} is not a log reference"),
        }
    }

    /// Convert this reference into a new `OtelLog` by cloning.
    ///
    /// # Panics
    ///
    /// This will panic if this is not a `Log` reference.
    pub fn into_log(self) -> OtelLog {
        match self {
            Self::Log(log) => log.clone(),
            _ => panic!("Failed type coercion, {self:?} is not a log reference"),
        }
    }

    /// Extract the `OtelMetric` reference in this.
    ///
    /// # Panics
    ///
    /// This will panic if this is not a `Metric` reference.
    pub fn as_metric(self) -> &'a OtelMetric {
        match self {
            Self::Metric(metric) => metric,
            _ => panic!("Failed type coercion, {self:?} is not a metric reference"),
        }
    }

    /// Convert this reference into an `OtelMetric` by cloning.
    ///
    /// # Panics
    ///
    /// This will panic if this is not a `Metric` reference.
    pub fn into_otel_metric(self) -> OtelMetric {
        match self {
            Self::Metric(metric) => metric.clone(),
            _ => panic!("Failed type coercion, {self:?} is not a metric reference"),
        }
    }
}

impl<'a> From<&'a Event> for EventRef<'a> {
    fn from(event: &'a Event) -> Self {
        match event {
            Event::Log(e) => EventRef::Log(e),
            Event::Metric(e) => EventRef::Metric(e),
            Event::Trace(e) => EventRef::Trace(e),
        }
    }
}

impl<'a> From<&'a OtelLog> for EventRef<'a> {
    fn from(log: &'a OtelLog) -> Self {
        Self::Log(log)
    }
}

impl<'a> From<&'a OtelMetric> for EventRef<'a> {
    fn from(metric: &'a OtelMetric) -> Self {
        Self::Metric(metric)
    }
}

impl<'a> From<&'a OtelSpan> for EventRef<'a> {
    fn from(trace: &'a OtelSpan) -> Self {
        Self::Trace(trace)
    }
}

impl EventDataEq<Event> for EventRef<'_> {
    fn event_data_eq(&self, that: &Event) -> bool {
        match (self, that) {
            (Self::Log(a), Event::Log(b)) => a.event_data_eq(b),
            (Self::Metric(a), Event::Metric(b)) => a.event_data_eq(b),
            (Self::Trace(a), Event::Trace(b)) => a.event_data_eq(b),
            _ => false,
        }
    }
}

/// A wrapper for mutable references to inner event types, where reconstituting
/// a full `Event` from an `OtelLog` or `OtelMetric` might be inconvenient.
#[derive(Debug)]
pub enum EventMutRef<'a> {
    /// Reference to an `OtelLog`
    Log(&'a mut OtelLog),
    /// Reference to an `OtelMetric`
    Metric(&'a mut OtelMetric),
    /// Reference to an `OtelSpan`
    Trace(&'a mut OtelSpan),
}

impl<'a> EventMutRef<'a> {
    /// Extract the `OtelLog` reference in this.
    ///
    /// # Panics
    ///
    /// This will panic if this is not a `Log` reference.
    pub fn as_log(self) -> &'a OtelLog {
        match self {
            Self::Log(log) => log,
            _ => panic!("Failed type coercion, {self:?} is not a log reference"),
        }
    }

    /// Convert this reference into a new `OtelLog` by cloning.
    ///
    /// # Panics
    ///
    /// This will panic if this is not a `Log` reference.
    pub fn into_log(self) -> OtelLog {
        match self {
            Self::Log(log) => log.clone(),
            _ => panic!("Failed type coercion, {self:?} is not a log reference"),
        }
    }

    /// Extract the `OtelMetric` reference in this.
    ///
    /// # Panics
    ///
    /// This will panic if this is not a `Metric` reference.
    pub fn as_metric(self) -> &'a OtelMetric {
        match self {
            Self::Metric(metric) => metric,
            _ => panic!("Failed type coercion, {self:?} is not a metric reference"),
        }
    }

    /// Convert this reference into an `OtelMetric` by cloning.
    ///
    /// # Panics
    ///
    /// This will panic if this is not a `Metric` reference.
    pub fn into_otel_metric(self) -> OtelMetric {
        match self {
            Self::Metric(metric) => metric.clone(),
            _ => panic!("Failed type coercion, {self:?} is not a metric reference"),
        }
    }

    /// Access the metadata in this reference.
    pub fn metadata(&self) -> &EventMetadata {
        match self {
            Self::Log(event) => event.metadata(),
            Self::Metric(event) => event.metadata(),
            Self::Trace(event) => event.metadata(),
        }
    }

    /// Access the metadata mutably in this reference.
    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        match self {
            Self::Log(event) => event.metadata_mut(),
            Self::Metric(event) => event.metadata_mut(),
            Self::Trace(event) => event.metadata_mut(),
        }
    }
}

impl<'a> From<&'a mut Event> for EventMutRef<'a> {
    fn from(event: &'a mut Event) -> Self {
        match event {
            Event::Log(event) => EventMutRef::Log(event),
            Event::Metric(event) => EventMutRef::Metric(event),
            Event::Trace(event) => EventMutRef::Trace(event),
        }
    }
}

impl<'a> From<&'a mut OtelLog> for EventMutRef<'a> {
    fn from(log: &'a mut OtelLog) -> Self {
        Self::Log(log)
    }
}

impl<'a> From<&'a mut OtelMetric> for EventMutRef<'a> {
    fn from(metric: &'a mut OtelMetric) -> Self {
        Self::Metric(metric)
    }
}

impl<'a> From<&'a mut OtelSpan> for EventMutRef<'a> {
    fn from(trace: &'a mut OtelSpan) -> Self {
        Self::Trace(trace)
    }
}
