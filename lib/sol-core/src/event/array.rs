#![deny(missing_docs)]
//! This module contains the definitions and wrapper types for handling
//! arrays of type `Event`, in the various forms they may appear.

use std::{iter, slice, vec};

use futures::{Stream, stream};
#[cfg(test)]
use quickcheck::{Arbitrary, Gen};
use sol_buffers::EventCount;
use sol_common::{
    byte_size_of::ByteSizeOf,
    finalization::{AddBatchNotifier, BatchNotifier, EventFinalizers, Finalizable},
    json_size::JsonSize,
};

use super::{
    EstimatedJsonEncodedSizeOf, Event, EventDataEq, EventFinalizer, EventMetadata, EventMutRef,
    EventRef, OtelLog, OtelMetric, OtelSpan,
};

/// The type alias for an array of `OtelLog` elements.
pub type LogArray = Vec<OtelLog>;

/// The type alias for an array of `OtelMetric` elements.
pub type MetricArray = Vec<OtelMetric>;

/// The type alias for an array of `OtelSpan` elements.
pub type TraceArray = Vec<OtelSpan>;

/// Backward-compat aliases.
#[allow(missing_docs)]
pub type OtelLogArray = LogArray;
#[allow(missing_docs)]
pub type OtelMetricArray = MetricArray;
#[allow(missing_docs)]
pub type OtelSpanArray = TraceArray;

/// The core trait to abstract over any type that may work as an array
/// of events. This is effectively the same as the standard
/// `IntoIterator<Item = Event>` implementations, but that would
/// conflict with the base implementation for the type aliases below.
pub trait EventContainer: ByteSizeOf + EstimatedJsonEncodedSizeOf {
    /// The type of `Iterator` used to turn this container into events.
    type IntoIter: Iterator<Item = Event>;

    /// The number of events in this container.
    fn len(&self) -> usize;

    /// Is this container empty?
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Turn this container into an iterator over `Event`.
    fn into_events(self) -> Self::IntoIter;
}

/// Turn a container into a futures stream over the contained `Event`
/// type.  This would ideally be implemented as a default method on
/// `trait EventContainer`, but the required feature (associated type
/// defaults) is still unstable.
/// See <https://github.com/rust-lang/rust/issues/29661>
pub fn into_event_stream(container: impl EventContainer) -> impl Stream<Item = Event> + Unpin {
    stream::iter(container.into_events())
}

impl EventContainer for Event {
    type IntoIter = iter::Once<Event>;

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn into_events(self) -> Self::IntoIter {
        iter::once(self)
    }
}

impl EventContainer for OtelLog {
    type IntoIter = iter::Once<Event>;

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn into_events(self) -> Self::IntoIter {
        iter::once(self.into())
    }
}

impl EventContainer for LogArray {
    type IntoIter = iter::Map<vec::IntoIter<OtelLog>, fn(OtelLog) -> Event>;

    fn len(&self) -> usize {
        self.len()
    }

    fn into_events(self) -> Self::IntoIter {
        self.into_iter().map(Into::into)
    }
}

impl EventContainer for MetricArray {
    type IntoIter = iter::Map<vec::IntoIter<OtelMetric>, fn(OtelMetric) -> Event>;

    fn len(&self) -> usize {
        self.len()
    }

    fn into_events(self) -> Self::IntoIter {
        self.into_iter().map(Event::Metric)
    }
}

/// An array of one of the `Event` variants exclusively.
#[derive(Clone, Debug, PartialEq)]
pub enum EventArray {
    /// An array of type `OtelLog`
    Logs(LogArray),
    /// An array of type `OtelMetric`
    Metrics(MetricArray),
    /// An array of type `OtelSpan`
    Traces(TraceArray),
}

/// Backward-compat: these used to be separate variants.
impl EventArray {
    /// Backward-compat alias for `EventArray::Logs`.
    pub fn is_otel_logs(&self) -> bool {
        matches!(self, Self::Logs(_))
    }
    /// Backward-compat alias for `EventArray::Metrics`.
    pub fn is_otel_metrics(&self) -> bool {
        matches!(self, Self::Metrics(_))
    }
    /// Backward-compat alias for `EventArray::Traces`.
    pub fn is_otel_spans(&self) -> bool {
        matches!(self, Self::Traces(_))
    }
}

impl EventArray {
    /// Iterate over references to this array's events.
    pub fn iter_events(&self) -> impl Iterator<Item = EventRef<'_>> {
        match self {
            Self::Logs(array) => EventArrayIter::Logs(array.iter()),
            Self::Metrics(array) => EventArrayIter::Metrics(array.iter()),
            Self::Traces(array) => EventArrayIter::Traces(array.iter()),
        }
    }

    /// Iterate over mutable references to this array's events.
    pub fn iter_events_mut(&mut self) -> impl Iterator<Item = EventMutRef<'_>> {
        match self {
            Self::Logs(array) => EventArrayIterMut::Logs(array.iter_mut()),
            Self::Metrics(array) => EventArrayIterMut::Metrics(array.iter_mut()),
            Self::Traces(array) => EventArrayIterMut::Traces(array.iter_mut()),
        }
    }

    /// Iterate over references to the logs in this array.
    pub fn iter_logs_mut(&mut self) -> impl Iterator<Item = &mut OtelLog> {
        match self {
            Self::Logs(array) => TypedArrayIterMut(Some(array.iter_mut())),
            _ => TypedArrayIterMut(None),
        }
    }

    /// Applies a closure to each event's metadata in this array.
    pub fn for_each_metadata_mut(&mut self, mut f: impl FnMut(&mut EventMetadata)) {
        match self {
            Self::Logs(a) => a.iter_mut().for_each(|e| f(e.metadata_mut())),
            Self::Metrics(a) => a.iter_mut().for_each(|e| f(e.metadata_mut())),
            Self::Traces(a) => a.iter_mut().for_each(|e| f(e.metadata_mut())),
        }
    }
}

impl From<Event> for EventArray {
    fn from(event: Event) -> Self {
        match event {
            Event::Log(e) => Self::Logs(vec![e]),
            Event::Metric(e) => Self::Metrics(vec![e]),
            Event::Trace(e) => Self::Traces(vec![e]),
        }
    }
}

impl From<OtelLog> for EventArray {
    fn from(log: OtelLog) -> Self {
        Self::Logs(vec![log])
    }
}

impl From<OtelMetric> for EventArray {
    fn from(metric: OtelMetric) -> Self {
        Self::Metrics(vec![metric])
    }
}

impl From<OtelSpan> for EventArray {
    fn from(span: OtelSpan) -> Self {
        Self::Traces(vec![span])
    }
}

impl From<LogArray> for EventArray {
    fn from(array: LogArray) -> Self {
        Self::Logs(array)
    }
}

impl From<MetricArray> for EventArray {
    fn from(array: MetricArray) -> Self {
        Self::Metrics(array)
    }
}

impl AddBatchNotifier for EventArray {
    fn add_batch_notifier(&mut self, batch: BatchNotifier) {
        macro_rules! add_notifier {
            ($array:expr, $batch:expr) => {
                $array
                    .iter_mut()
                    .for_each(|item| item.add_finalizer(EventFinalizer::new($batch.clone())))
            };
        }
        match self {
            Self::Logs(a) => add_notifier!(a, batch),
            Self::Metrics(a) => add_notifier!(a, batch),
            Self::Traces(a) => add_notifier!(a, batch),
        }
    }
}

impl ByteSizeOf for EventArray {
    fn allocated_bytes(&self) -> usize {
        match self {
            Self::Logs(a) => a.allocated_bytes(),
            Self::Metrics(a) => a.allocated_bytes(),
            Self::Traces(a) => a.allocated_bytes(),
        }
    }
}

impl EstimatedJsonEncodedSizeOf for EventArray {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        match self {
            Self::Logs(v) => v.estimated_json_encoded_size_of(),
            Self::Metrics(v) => v.estimated_json_encoded_size_of(),
            Self::Traces(v) => v.estimated_json_encoded_size_of(),
        }
    }
}

impl EventCount for EventArray {
    fn event_count(&self) -> usize {
        match self {
            Self::Logs(a) => a.len(),
            Self::Metrics(a) => a.len(),
            Self::Traces(a) => a.len(),
        }
    }
}

impl EventContainer for EventArray {
    type IntoIter = EventArrayIntoIter;

    fn len(&self) -> usize {
        match self {
            Self::Logs(a) => a.len(),
            Self::Metrics(a) => a.len(),
            Self::Traces(a) => a.len(),
        }
    }

    fn into_events(self) -> Self::IntoIter {
        match self {
            Self::Logs(a) => EventArrayIntoIter::Logs(a.into_iter()),
            Self::Metrics(a) => EventArrayIntoIter::Metrics(a.into_iter()),
            Self::Traces(a) => EventArrayIntoIter::Traces(a.into_iter()),
        }
    }
}

impl EventDataEq for EventArray {
    fn event_data_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Logs(a), Self::Logs(b)) => a.event_data_eq(b),
            (Self::Metrics(a), Self::Metrics(b)) => a.event_data_eq(b),
            (Self::Traces(a), Self::Traces(b)) => a.event_data_eq(b),
            _ => false,
        }
    }
}

impl Finalizable for EventArray {
    fn take_finalizers(&mut self) -> EventFinalizers {
        match self {
            Self::Logs(a) => a.iter_mut().map(Finalizable::take_finalizers).collect(),
            Self::Metrics(a) => a.iter_mut().map(Finalizable::take_finalizers).collect(),
            Self::Traces(a) => a.iter_mut().map(Finalizable::take_finalizers).collect(),
        }
    }
}

#[cfg(test)]
impl Arbitrary for EventArray {
    fn arbitrary(g: &mut Gen) -> Self {
        let len = u8::arbitrary(g) as usize;
        let choice: u8 = u8::arbitrary(g);
        if choice.is_multiple_of(2) {
            let mut logs = Vec::new();
            for _ in 0..len {
                logs.push(OtelLog::arbitrary(g));
            }
            EventArray::Logs(logs)
        } else {
            let mut metrics = Vec::new();
            for _ in 0..len {
                metrics.push(OtelMetric::arbitrary(g));
            }
            EventArray::Metrics(metrics)
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        match self {
            EventArray::Logs(_) | EventArray::Metrics(_) | EventArray::Traces(_) => {
                Box::new(std::iter::empty())
            }
        }
    }
}

/// The iterator type for `EventArray::iter_events`.
#[derive(Debug)]
pub enum EventArrayIter<'a> {
    /// An iterator over type `OtelLog`.
    Logs(slice::Iter<'a, OtelLog>),
    /// An iterator over type `OtelMetric`.
    Metrics(slice::Iter<'a, OtelMetric>),
    /// An iterator over type `OtelSpan`.
    Traces(slice::Iter<'a, OtelSpan>),
}

impl<'a> Iterator for EventArrayIter<'a> {
    type Item = EventRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Logs(i) => i.next().map(EventRef::Log),
            Self::Metrics(i) => i.next().map(EventRef::Metric),
            Self::Traces(i) => i.next().map(EventRef::Trace),
        }
    }
}

/// The iterator type for `EventArray::iter_events_mut`.
#[derive(Debug)]
pub enum EventArrayIterMut<'a> {
    /// An iterator over type `OtelLog`.
    Logs(slice::IterMut<'a, OtelLog>),
    /// An iterator over type `OtelMetric`.
    Metrics(slice::IterMut<'a, OtelMetric>),
    /// An iterator over type `OtelSpan`.
    Traces(slice::IterMut<'a, OtelSpan>),
}

impl<'a> Iterator for EventArrayIterMut<'a> {
    type Item = EventMutRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Logs(i) => i.next().map(EventMutRef::Log),
            Self::Metrics(i) => i.next().map(EventMutRef::Metric),
            Self::Traces(i) => i.next().map(EventMutRef::Trace),
        }
    }
}

/// The iterator type for `EventArray::into_events`.
#[derive(Debug)]
pub enum EventArrayIntoIter {
    /// An iterator over type `OtelLog`.
    Logs(vec::IntoIter<OtelLog>),
    /// An iterator over type `OtelMetric`.
    Metrics(vec::IntoIter<OtelMetric>),
    /// An iterator over type `OtelSpan`.
    Traces(vec::IntoIter<OtelSpan>),
}

impl Iterator for EventArrayIntoIter {
    type Item = Event;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Logs(i) => i.next().map(Event::Log),
            Self::Metrics(i) => i.next().map(Event::Metric),
            Self::Traces(i) => i.next().map(Event::Trace),
        }
    }
}

struct TypedArrayIterMut<'a, T>(Option<slice::IterMut<'a, T>>);

impl<'a, T> Iterator for TypedArrayIterMut<'a, T> {
    type Item = &'a mut T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.as_mut().and_then(Iterator::next)
    }
}

/// Intermediate buffer for conversion of a sequence of individual
/// `Event`s into a sequence of `EventArray`s by coalescing contiguous
/// events of the same type into one array. This is used by
/// `events_into_array`.
#[derive(Debug, Default)]
pub struct EventArrayBuffer {
    buffer: Option<EventArray>,
    max_size: usize,
}

impl EventArrayBuffer {
    fn new(max_size: Option<usize>) -> Self {
        let max_size = max_size.unwrap_or(usize::MAX);
        let buffer = None;
        Self { buffer, max_size }
    }

    #[must_use]
    fn push(&mut self, event: Event) -> Option<EventArray> {
        match (event, &mut self.buffer) {
            (Event::Log(event), Some(EventArray::Logs(array))) if array.len() < self.max_size => {
                array.push(event);
                None
            }
            (Event::Metric(event), Some(EventArray::Metrics(array)))
                if array.len() < self.max_size =>
            {
                array.push(event);
                None
            }
            (Event::Trace(event), Some(EventArray::Traces(array)))
                if array.len() < self.max_size =>
            {
                array.push(event);
                None
            }
            (event, current) => current.replace(EventArray::from(event)),
        }
    }

    fn take(&mut self) -> Option<EventArray> {
        self.buffer.take()
    }
}

/// Convert the iterator over individual `Event`s into an iterator
/// over coalesced `EventArray`s.
pub fn events_into_arrays(
    events: impl IntoIterator<Item = Event>,
    max_size: Option<usize>,
) -> impl Iterator<Item = EventArray> {
    IntoEventArraysIter {
        inner: events.into_iter().fuse(),
        current: EventArrayBuffer::new(max_size),
    }
}

/// Iterator type implementing `into_arrays`
pub struct IntoEventArraysIter<I> {
    inner: iter::Fuse<I>,
    current: EventArrayBuffer,
}

impl<I: Iterator<Item = Event>> Iterator for IntoEventArraysIter<I> {
    type Item = EventArray;
    fn next(&mut self) -> Option<Self::Item> {
        for event in self.inner.by_ref() {
            if let Some(array) = self.current.push(event) {
                return Some(array);
            }
        }
        self.current.take()
    }
}
