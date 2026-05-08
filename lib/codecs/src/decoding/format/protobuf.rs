use std::path::PathBuf;

use bytes::Bytes;
use chrono::Utc;
use derivative::Derivative;
use lookup::owned_value_path;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use smallvec::{SmallVec, smallvec};
use sol_config::configurable_component;
use sol_core::{
    config::DataType,
    event::{Event, EventMetadata, OtelLog},
    schema,
};
use vrl::{
    protobuf::{
        descriptor::{get_message_descriptor, get_message_descriptor_from_bytes},
        parse::{Options, proto_to_value},
    },
    value::{Kind, Value},
};

use super::Deserializer;

/// Config used to build a `ProtobufDeserializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct ProtobufDeserializerConfig {
    /// Protobuf-specific decoding options.
    #[serde(default, skip_serializing_if = "sol_core::serde::is_default")]
    pub protobuf: ProtobufDeserializerOptions,
}

impl ProtobufDeserializerConfig {
    /// Build the `ProtobufDeserializer` from this configuration.
    pub fn build(&self) -> sol_common::Result<ProtobufDeserializer> {
        ProtobufDeserializer::try_from(self)
    }

    /// Return the type of event build by this deserializer.
    pub fn output_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema produced by the deserializer.
    pub fn schema_definition(&self) -> schema::Definition {
        let mut definition = schema::Definition::empty_definition().unknown_fields(Kind::any());

        {
            let timestamp_key = owned_value_path!("time_unix_nano");
            definition = definition.try_with_field(&timestamp_key, Kind::any(), Some("timestamp"));
        }
        definition
    }
}

/// Protobuf-specific decoding options.
#[configurable_component]
#[derive(Debug, Clone, PartialEq, Eq, Derivative)]
#[derivative(Default)]
pub struct ProtobufDeserializerOptions {
    /// The path to the protobuf descriptor set file.
    ///
    /// This file is the output of `protoc -I <include path> -o <desc output path> <proto>`.
    ///
    /// You can read more [here](https://buf.build/docs/reference/images/#how-buf-images-work).
    pub desc_file: PathBuf,

    /// The name of the message type to use for serializing.
    #[configurable(metadata(docs::examples = "package.Message"))]
    pub message_type: String,

    /// Use JSON field names (camelCase) instead of protobuf field names (snake_case).
    ///
    /// When enabled, the deserializer will output fields using their JSON names as defined
    /// in the `.proto` file (e.g., `jobDescription` instead of `job_description`).
    ///
    /// This is useful when working with data that needs to be converted to JSON or
    /// when interfacing with systems that use JSON naming conventions.
    #[serde(default, skip_serializing_if = "sol_core::serde::is_default")]
    pub use_json_names: bool,
}

/// Deserializer that builds `Event`s from a byte frame containing protobuf.
#[derive(Debug, Clone)]
pub struct ProtobufDeserializer {
    message_descriptor: MessageDescriptor,
    options: Options,
}

impl ProtobufDeserializer {
    /// Creates a new `ProtobufDeserializer`.
    pub fn new(message_descriptor: MessageDescriptor) -> Self {
        Self {
            message_descriptor,
            options: Default::default(),
        }
    }

    /// Creates a new deserializer instance using the descriptor bytes directly.
    pub fn new_from_bytes(
        desc_bytes: &[u8],
        message_type: &str,
        options: Options,
    ) -> sol_common::Result<Self> {
        let message_descriptor = get_message_descriptor_from_bytes(desc_bytes, message_type)?;
        Ok(Self {
            message_descriptor,
            options,
        })
    }
}

fn extract_vrl_value(
    bytes: Bytes,
    message_descriptor: &MessageDescriptor,
    options: &Options,
) -> sol_common::Result<Value> {
    let dynamic_message = DynamicMessage::decode(message_descriptor.clone(), bytes)
        .map_err(|error| format!("Error parsing protobuf: {error:?}"))?;

    Ok(proto_to_value(
        &prost_reflect::Value::Message(dynamic_message),
        None,
        options,
    )?)
}

impl Deserializer for ProtobufDeserializer {
    fn parse(&self, bytes: Bytes) -> sol_common::Result<SmallVec<[Event; 1]>> {
        let vrl_value = extract_vrl_value(bytes, &self.message_descriptor, &self.options)?;
        let mut log = OtelLog::from_value_map(vrl_value, EventMetadata::default());
        log.try_set_timestamp(Utc::now());

        Ok(smallvec![Event::Log(log)])
    }
}

impl TryFrom<&ProtobufDeserializerConfig> for ProtobufDeserializer {
    type Error = sol_common::Error;
    fn try_from(config: &ProtobufDeserializerConfig) -> sol_common::Result<Self> {
        let message_descriptor =
            get_message_descriptor(&config.protobuf.desc_file, &config.protobuf.message_type)?;
        Ok(Self {
            message_descriptor,
            options: Options {
                use_json_names: config.protobuf.use_json_names,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    // TODO: add test for bad file path & invalid message_type

    use std::{env, fs, path::PathBuf};

    use lookup::{OwnedTargetPath, owned_value_path};
    use sol_core::event::OtelLog;

    use super::*;

    fn test_data_dir() -> PathBuf {
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("tests/data/protobuf")
    }

    fn parse_and_validate(
        protobuf_bin_message: String,
        protobuf_desc_path: PathBuf,
        message_type: &str,
        validate_log: fn(&OtelLog),
    ) {
        let input = Bytes::from(protobuf_bin_message);
        let message_descriptor = get_message_descriptor(&protobuf_desc_path, message_type).unwrap();
        let deserializer = ProtobufDeserializer::new(message_descriptor);

        let events = deserializer.parse(input).unwrap();
        let mut events = events.into_iter();

        {
            let event = events.next().unwrap();
            let log = event.as_log();
            validate_log(log);
            assert!(
                log.get(&OwnedTargetPath::event(owned_value_path!("time_unix_nano")))
                    .is_some()
            );
        }

        assert_eq!(events.next(), None);
    }

    #[test]
    fn deserialize_protobuf() {
        let protobuf_bin_message_path = test_data_dir().join("pbs/person_someone.pb");
        let protobuf_desc_path = test_data_dir().join("protos/test_protobuf.desc");
        let message_type = "test_protobuf.Person";
        let validate_log = |log: &OtelLog| {
            assert_eq!(
                log.parse_path_and_get_value("name").ok().flatten().unwrap(),
                Value::from("someone")
            );
            let phones = log
                .parse_path_and_get_value("phones")
                .ok()
                .flatten()
                .unwrap();
            assert_eq!(
                phones.as_array().unwrap()[0].as_object().unwrap()["number"]
                    .as_str()
                    .unwrap(),
                "123456"
            );
        };

        parse_and_validate(
            fs::read_to_string(protobuf_bin_message_path).unwrap(),
            protobuf_desc_path,
            message_type,
            validate_log,
        );
    }

    #[test]
    fn deserialize_protobuf3() {
        let protobuf_bin_message_path = test_data_dir().join("pbs/person_someone3.pb");
        let protobuf_desc_path = test_data_dir().join("protos/test_protobuf3.desc");
        let message_type = "test_protobuf3.Person";
        let validate_log = |log: &OtelLog| {
            assert_eq!(
                log.parse_path_and_get_value("name").ok().flatten().unwrap(),
                Value::from("someone")
            );
            let phones = log
                .parse_path_and_get_value("phones")
                .ok()
                .flatten()
                .unwrap();
            assert_eq!(
                phones.as_array().unwrap()[0].as_object().unwrap()["number"]
                    .as_str()
                    .unwrap(),
                "1234"
            );
            let data = log.parse_path_and_get_value("data").ok().flatten().unwrap();
            assert_eq!(data.as_object().unwrap()["data_phone"], "HOME".into());
        };

        parse_and_validate(
            fs::read_to_string(protobuf_bin_message_path).unwrap(),
            protobuf_desc_path,
            message_type,
            validate_log,
        );
    }

    #[test]
    fn deserialize_empty_buffer() {
        let protobuf_bin_message = "".to_string();
        let protobuf_desc_path = test_data_dir().join("protos/test_protobuf.desc");
        let message_type = "test_protobuf.Person";
        let validate_log = |log: &OtelLog| {
            // No field will be set.
            assert!(
                log.parse_path_and_get_value("name")
                    .ok()
                    .flatten()
                    .is_none()
            );
            assert!(log.parse_path_and_get_value("id").ok().flatten().is_none());
            assert!(
                log.parse_path_and_get_value("email")
                    .ok()
                    .flatten()
                    .is_none()
            );
            assert!(
                log.parse_path_and_get_value("phones")
                    .ok()
                    .flatten()
                    .is_none()
            );
        };

        parse_and_validate(
            protobuf_bin_message,
            protobuf_desc_path,
            message_type,
            validate_log,
        );
    }

    #[test]
    fn deserialize_error_invalid_protobuf() {
        let input = Bytes::from("{ foo");
        let message_descriptor = get_message_descriptor(
            &test_data_dir().join("protos/test_protobuf.desc"),
            "test_protobuf.Person",
        )
        .unwrap();
        let deserializer = ProtobufDeserializer::new(message_descriptor);
        assert!(deserializer.parse(input).is_err());
    }
}
