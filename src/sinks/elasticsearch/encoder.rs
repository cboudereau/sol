use std::{io, io::Write};

use serde::Serialize;
use serde_json::json;
use sol_lib::{
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
    buffers::EventCount,
    config::telemetry,
    event::Event,
    internal_event::TaggedEventsSent,
    json_size::JsonSize,
    request_metadata::{GetEventCountTags, GroupedCountByteSize},
};

use crate::{
    codecs::Transformer,
    event::{EventFinalizers, Finalizable, OtelLog},
    sinks::{
        elasticsearch::{BulkAction, VersionType},
        util::encoding::{Encoder, as_tracked_write},
    },
};

#[derive(Serialize, Clone, Debug)]
pub enum DocumentVersionType {
    External,
    ExternalGte,
}

impl DocumentVersionType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            DocumentVersionType::External => VersionType::External.as_str(),
            DocumentVersionType::ExternalGte => VersionType::ExternalGte.as_str(),
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct DocumentVersion {
    pub kind: DocumentVersionType,
    pub value: u64,
}

#[derive(Serialize, Clone, Debug)]
pub enum DocumentMetadata {
    WithoutId,
    Id(String),
    IdAndVersion(String, DocumentVersion),
}

#[derive(Serialize, Clone, Debug)]
pub struct ProcessedEvent {
    pub index: String,
    pub bulk_action: BulkAction,
    pub log: OtelLog,
    pub document_metadata: DocumentMetadata,
}

impl Finalizable for ProcessedEvent {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.log.metadata_mut().take_finalizers()
    }
}

impl ByteSizeOf for ProcessedEvent {
    fn allocated_bytes(&self) -> usize {
        match &self.document_metadata {
            DocumentMetadata::WithoutId => {
                self.index.allocated_bytes() + self.log.allocated_bytes()
            }
            DocumentMetadata::Id(id) | DocumentMetadata::IdAndVersion(id, _) => {
                self.index.allocated_bytes() + self.log.allocated_bytes() + id.allocated_bytes()
            }
        }
    }
}

impl EstimatedJsonEncodedSizeOf for ProcessedEvent {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.log.estimated_json_encoded_size_of()
    }
}

impl EventCount for ProcessedEvent {
    fn event_count(&self) -> usize {
        // An Elasticsearch ProcessedEvent is mapped one-to-one with an event.
        1
    }
}

impl GetEventCountTags for ProcessedEvent {
    fn get_tags(&self) -> TaggedEventsSent {
        self.log.get_tags()
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ElasticsearchEncoder {
    pub transformer: Transformer,
    pub doc_type: String,
    pub suppress_type_name: bool,
    /// When true (datastream mode), rename "timestamp" to "@timestamp" in serialized output
    /// to comply with Elastic Common Schema.
    pub use_at_timestamp: bool,
}

impl Default for ElasticsearchEncoder {
    fn default() -> Self {
        Self {
            transformer: Transformer::default(),
            doc_type: "_doc".to_string(),
            suppress_type_name: false,
            use_at_timestamp: false,
        }
    }
}

impl Encoder<Vec<ProcessedEvent>> for ElasticsearchEncoder {
    fn encode_input(
        &self,
        input: Vec<ProcessedEvent>,
        writer: &mut dyn Write,
    ) -> std::io::Result<(usize, GroupedCountByteSize)> {
        let mut written_bytes = 0;
        let mut byte_size = telemetry().create_request_count_byte_size();
        for event in input {
            let log = {
                let mut event = Event::Log(event.log);
                self.transformer.transform(&mut event);
                byte_size.add_event(&event, event.estimated_json_encoded_size_of());

                event.into_log()
            };
            written_bytes += write_bulk_action(
                writer,
                event.bulk_action.as_str(),
                &event.index,
                &self.doc_type,
                self.suppress_type_name,
                &event.document_metadata,
            )?;
            written_bytes +=
                as_tracked_write::<_, _, io::Error>(writer, &log, |mut writer, log| {
                    writer.write_all(b"\n")?;
                    let mut value = serde_json::to_value(log)?;
                    if self.use_at_timestamp
                        && let Some(obj) = value.as_object_mut()
                        && let Some(nanos_str) = obj
                            .remove("timeUnixNano")
                            .and_then(|v| v.as_str().map(String::from))
                        && let Ok(nanos) = nanos_str.parse::<i64>()
                    {
                        let secs = nanos / 1_000_000_000;
                        #[expect(
                            clippy::cast_sign_loss,
                            reason = "nanos modulo 1e9 is always in 0..999_999_999"
                        )]
                        let nsecs = (nanos % 1_000_000_000) as u32;
                        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                            obj.insert(
                                "@timestamp".into(),
                                serde_json::Value::String(
                                    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                                ),
                            );
                        }
                    }
                    serde_json::to_writer(&mut writer, &value)?;
                    writer.write_all(b"\n")?;
                    Ok(())
                })?;
        }

        Ok((written_bytes, byte_size))
    }
}

fn write_bulk_action(
    writer: &mut dyn Write,
    bulk_action: &str,
    index: &str,
    doc_type: &str,
    suppress_type: bool,
    document: &DocumentMetadata,
) -> std::io::Result<usize> {
    as_tracked_write(
        writer,
        (bulk_action, index, doc_type, suppress_type, document),
        |writer, (bulk_action, index, doc_type, suppress_type, document)| match (
            suppress_type,
            document,
        ) {
            (true, DocumentMetadata::Id(id)) => {
                write!(
                    writer,
                    "{}",
                    json!({
                        bulk_action: {
                            "_index": index,
                            "_id": id,
                        }
                    }),
                )
            }
            (false, DocumentMetadata::Id(id)) => {
                write!(
                    writer,
                    "{}",
                    json!({
                        bulk_action: {
                            "_type": doc_type,
                            "_index": index,
                            "_id": id,
                        }
                    }),
                )
            }
            (true, DocumentMetadata::WithoutId) => {
                write!(
                    writer,
                    "{}",
                    json!({
                        bulk_action: {
                            "_index": index,
                        }
                    }),
                )
            }
            (false, DocumentMetadata::WithoutId) => {
                write!(
                    writer,
                    "{}",
                    json!({
                        bulk_action: {
                            "_type": doc_type,
                            "_index": index,
                        }
                    }),
                )
            }
            (true, DocumentMetadata::IdAndVersion(id, version)) => {
                write!(
                    writer,
                    "{}",
                    json!({
                        bulk_action: {
                            "_id": id,
                            "_index": index,
                            "version_type": version.kind.as_str(),
                            "version": version.value,
                        }
                    }),
                )
            }
            (false, DocumentMetadata::IdAndVersion(id, version)) => {
                write!(
                    writer,
                    "{}",
                    json!({
                        bulk_action: {
                            "_id": id,
                            "_type": doc_type,
                            "_index": index,
                            "version_type": version.kind.as_str(),
                            "version": version.value,
                        }
                    }),
                )
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suppress_type_with_id() {
        let mut writer = Vec::new();

        write_bulk_action(
            &mut writer,
            "ACTION",
            "INDEX",
            "TYPE",
            true,
            &DocumentMetadata::Id("ID".to_string()),
        )
        .unwrap();

        let value: serde_json::Value = serde_json::from_slice(&writer).unwrap();
        let value = value.as_object().unwrap();

        assert!(value.contains_key("ACTION"));

        let nested = value.get("ACTION").unwrap();
        let nested = nested.as_object().unwrap();

        assert!(nested.contains_key("_index"));
        assert_eq!(nested.get("_index").unwrap().as_str(), Some("INDEX"));
        assert!(nested.contains_key("_id"));
        assert_eq!(nested.get("_id").unwrap().as_str(), Some("ID"));
        assert!(!nested.contains_key("_type"));
    }

    #[test]
    fn suppress_type_without_id() {
        let mut writer = Vec::new();

        write_bulk_action(
            &mut writer,
            "ACTION",
            "INDEX",
            "TYPE",
            true,
            &DocumentMetadata::WithoutId,
        )
        .unwrap();

        let value: serde_json::Value = serde_json::from_slice(&writer).unwrap();
        let value = value.as_object().unwrap();

        assert!(value.contains_key("ACTION"));

        let nested = value.get("ACTION").unwrap();
        let nested = nested.as_object().unwrap();

        assert!(nested.contains_key("_index"));
        assert_eq!(nested.get("_index").unwrap().as_str(), Some("INDEX"));
        assert!(!nested.contains_key("_id"));
        assert!(!nested.contains_key("_type"));
    }

    #[test]
    fn type_with_id() {
        let mut writer = Vec::new();

        write_bulk_action(
            &mut writer,
            "ACTION",
            "INDEX",
            "TYPE",
            false,
            &DocumentMetadata::Id("ID".to_string()),
        )
        .unwrap();

        let value: serde_json::Value = serde_json::from_slice(&writer).unwrap();
        let value = value.as_object().unwrap();

        assert!(value.contains_key("ACTION"));

        let nested = value.get("ACTION").unwrap();
        let nested = nested.as_object().unwrap();

        assert!(nested.contains_key("_index"));
        assert_eq!(nested.get("_index").unwrap().as_str(), Some("INDEX"));
        assert!(nested.contains_key("_id"));
        assert_eq!(nested.get("_id").unwrap().as_str(), Some("ID"));
        assert!(nested.contains_key("_type"));
        assert_eq!(nested.get("_type").unwrap().as_str(), Some("TYPE"));
    }

    #[test]
    fn type_without_id() {
        let mut writer = Vec::new();

        write_bulk_action(
            &mut writer,
            "ACTION",
            "INDEX",
            "TYPE",
            false,
            &DocumentMetadata::WithoutId,
        )
        .unwrap();

        let value: serde_json::Value = serde_json::from_slice(&writer).unwrap();
        let value = value.as_object().unwrap();

        assert!(value.contains_key("ACTION"));

        let nested = value.get("ACTION").unwrap();
        let nested = nested.as_object().unwrap();

        assert!(nested.contains_key("_index"));
        assert_eq!(nested.get("_index").unwrap().as_str(), Some("INDEX"));
        assert!(!nested.contains_key("_id"));
        assert!(nested.contains_key("_type"));
        assert_eq!(nested.get("_type").unwrap().as_str(), Some("TYPE"));
    }

    #[test]
    fn encodes_fields_with_newlines() {
        let mut writer = Vec::new();

        write_bulk_action(
            &mut writer,
            "ACTION\n",
            "INDEX\n",
            "TYPE\n",
            false,
            &DocumentMetadata::Id("ID\n".to_string()),
        )
        .unwrap();

        let value: serde_json::Value = serde_json::from_slice(&writer).unwrap();
        let value = value.as_object().unwrap();

        assert!(value.contains_key("ACTION\n"));

        let nested = value.get("ACTION\n").unwrap();
        let nested = nested.as_object().unwrap();

        assert!(nested.contains_key("_index"));
        assert_eq!(nested.get("_index").unwrap().as_str(), Some("INDEX\n"));
        assert!(nested.contains_key("_id"));
        assert_eq!(nested.get("_id").unwrap().as_str(), Some("ID\n"));
        assert!(nested.contains_key("_type"));
        assert_eq!(nested.get("_type").unwrap().as_str(), Some("TYPE\n"));
    }
}
