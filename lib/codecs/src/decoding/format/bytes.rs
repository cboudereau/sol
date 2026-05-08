use bytes::Bytes;
use lookup::owned_value_path;
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};
use sol_core::{
    config::DataType,
    event::{Event, OtelLog},
    schema,
    schema::meaning,
};
use vrl::value::Kind;

use super::Deserializer;

/// Config used to build a `BytesDeserializer`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BytesDeserializerConfig;

impl BytesDeserializerConfig {
    /// Creates a new `BytesDeserializerConfig`.
    pub const fn new() -> Self {
        Self
    }

    /// Build the `BytesDeserializer` from this configuration.
    pub fn build(&self) -> BytesDeserializer {
        BytesDeserializer
    }

    /// Return the type of event build by this deserializer.
    pub fn output_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema produced by the deserializer.
    pub fn schema_definition(&self) -> schema::Definition {
        let definition = schema::Definition::empty_definition();
        let message_key = owned_value_path!("body");
        definition.with_event_field(
            &message_key,
            Kind::bytes(),
            Some(meaning::MESSAGE),
        )
    }
}

/// Deserializer that converts bytes to an `OtelLog` event.
///
/// This deserializer can be considered as the no-op action for input where no
/// further decoding has been specified.
#[derive(Debug, Clone)]
pub struct BytesDeserializer;

impl BytesDeserializer {
    /// Deserializes the given bytes, which will always produce a single `OtelLog`.
    pub fn parse_single(&self, bytes: Bytes) -> OtelLog {
        OtelLog::from_bytes(&bytes)
    }
}

impl Deserializer for BytesDeserializer {
    fn parse(
        &self,
        bytes: Bytes,
    ) -> sol_common::Result<SmallVec<[Event; 1]>> {
        let otel_log = self.parse_single(bytes);
        Ok(smallvec![Event::Log(otel_log)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_bytes_produces_otel_log() {
        let input = Bytes::from("foo");
        let deserializer = BytesDeserializer;

        let events = deserializer.parse(input).unwrap();
        assert_eq!(events.len(), 1);

        let event = &events[0];
        assert!(matches!(event, Event::Log(_)), "expected Log(OtelLog)");

        let otel_log = match event {
            Event::Log(log) => log,
            _ => panic!("expected Log(OtelLog)"),
        };
        assert_eq!(otel_log.body_string(), "foo");
    }
}
