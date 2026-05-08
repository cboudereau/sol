use std::{fmt, sync::Arc};

use sol_lib::{
    lookup::{OwnedValuePath, PathPrefix, event_path, lookup_v2::OptionalTargetPath},
    schema::meaning,
};
use vrl::value::{KeyString, ObjectMap};

use super::request_builder::HecLogsRequestBuilder;
use crate::{
    event::OtelLog,
    internal_events::{SplunkEventTimestampInvalidType, SplunkEventTimestampMissing},
    sinks::{
        prelude::*,
        splunk_hec::common::{
            EndpointTarget, INDEX_FIELD, SOURCE_FIELD, SOURCETYPE_FIELD, render_template_string,
            request::HecRequest,
        },
        util::processed_event::ProcessedEvent,
    },
};

// NOTE: The `OptionalTargetPath`s are wrapped in an `Option` in order to distinguish between a true
//       `None` type and an empty string. This is necessary because `OptionalTargetPath` deserializes an
//       empty string to a `None` path internally.
pub struct HecLogsSink<S> {
    pub service: S,
    pub request_builder: HecLogsRequestBuilder,
    pub batch_settings: BatcherSettings,
    pub sourcetype: Option<Template>,
    pub source: Option<Template>,
    pub index: Option<Template>,
    pub indexed_fields: Vec<OwnedValuePath>,
    pub host_key: Option<OptionalTargetPath>,
    pub timestamp_nanos_key: Option<String>,
    pub timestamp_key: Option<OptionalTargetPath>,
    pub endpoint_target: EndpointTarget,
    pub auto_extract_timestamp: bool,
}

pub struct HecLogData<'a> {
    pub sourcetype: Option<&'a Template>,
    pub source: Option<&'a Template>,
    pub index: Option<&'a Template>,
    pub indexed_fields: &'a [OwnedValuePath],
    pub host_key: Option<OptionalTargetPath>,
    pub timestamp_nanos_key: Option<&'a String>,
    pub timestamp_key: Option<OptionalTargetPath>,
    pub endpoint_target: EndpointTarget,
    pub auto_extract_timestamp: bool,
}

impl<S> HecLogsSink<S>
where
    S: Service<HecRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let data = HecLogData {
            sourcetype: self.sourcetype.as_ref(),
            source: self.source.as_ref(),
            index: self.index.as_ref(),
            indexed_fields: self.indexed_fields.as_slice(),
            host_key: self.host_key.clone(),
            timestamp_nanos_key: self.timestamp_nanos_key.as_ref(),
            timestamp_key: self.timestamp_key.clone(),
            endpoint_target: self.endpoint_target,
            auto_extract_timestamp: self.auto_extract_timestamp,
        };
        let batch_settings = self.batch_settings;

        input
            .map(move |event| process_log(event, &data))
            .batched_partitioned(
                if self.endpoint_target == EndpointTarget::Raw {
                    // We only need to partition by the metadata fields for the raw endpoint since those fields
                    // are sent via query parameters in the request.
                    EventPartitioner::new(
                        self.sourcetype.clone(),
                        self.source.clone(),
                        self.index.clone(),
                        self.host_key.clone(),
                    )
                } else {
                    EventPartitioner::new(None, None, None, None)
                },
                || batch_settings.as_byte_size_config(),
            )
            .request_builder(
                default_request_builder_concurrency_limit(),
                self.request_builder,
            )
            .filter_map(|request| async move {
                match request {
                    Err(e) => {
                        error!("Failed to build HEC Logs request: {:?}.", e);
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait]
impl<S> StreamSink<Event> for HecLogsSink<S>
where
    S: Service<HecRequest> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: fmt::Debug + Into<crate::Error> + Send,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}

#[derive(Clone, Debug, PartialEq, Hash, Eq)]
pub(super) struct Partitioned {
    pub(super) token: Option<Arc<str>>,
    pub(super) source: Option<String>,
    pub(super) sourcetype: Option<String>,
    pub(super) index: Option<String>,
    pub(super) host: Option<String>,
}

#[derive(Default)]
struct EventPartitioner {
    pub sourcetype: Option<Template>,
    pub source: Option<Template>,
    pub index: Option<Template>,
    pub host_key: Option<OptionalTargetPath>,
}

impl EventPartitioner {
    const fn new(
        sourcetype: Option<Template>,
        source: Option<Template>,
        index: Option<Template>,
        host_key: Option<OptionalTargetPath>,
    ) -> Self {
        Self {
            sourcetype,
            source,
            index,
            host_key,
        }
    }
}

impl Partitioner for EventPartitioner {
    type Item = HecProcessedEvent;
    type Key = Option<Partitioned>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        let emit_err = |error, field| {
            emit!(TemplateRenderingError {
                error,
                field: Some(field),
                drop_event: false,
            })
        };

        let source = self.source.as_ref().and_then(|source| {
            source
                .render_string(&item.event)
                .map_err(|error| emit_err(error, SOURCE_FIELD))
                .ok()
        });

        let sourcetype = self.sourcetype.as_ref().and_then(|sourcetype| {
            sourcetype
                .render_string(&item.event)
                .map_err(|error| emit_err(error, SOURCETYPE_FIELD))
                .ok()
        });

        let index = self.index.as_ref().and_then(|index| {
            index
                .render_string(&item.event)
                .map_err(|error| emit_err(error, INDEX_FIELD))
                .ok()
        });

        let host = match self.host_key.as_ref() {
            Some(maybe_key) => maybe_key
                .path
                .as_ref()
                .and_then(|path| item.event.get(path))
                .and_then(|value| value.as_str().map(|s| s.to_string())),
            None => item
                .event
                .find_key_by_meaning(meaning::HOST)
                .and_then(|path| item.event.get(path))
                .and_then(|value| value.as_str().map(|s| s.to_string())),
        };

        Some(Partitioned {
            token: item.event.metadata().splunk_hec_token(),
            source,
            sourcetype,
            index,
            host,
        })
    }
}

#[derive(PartialEq, Default, Clone, Debug)]
pub struct HecLogsProcessedEventMetadata {
    pub sourcetype: Option<String>,
    pub source: Option<String>,
    pub index: Option<String>,
    pub host: Option<Value>,
    pub timestamp: Option<f64>,
    pub fields: OtelLog,
    pub endpoint_target: EndpointTarget,
}

impl ByteSizeOf for HecLogsProcessedEventMetadata {
    fn allocated_bytes(&self) -> usize {
        self.sourcetype.allocated_bytes()
            + self.source.allocated_bytes()
            + self.index.allocated_bytes()
            + self.host.allocated_bytes()
            + self.fields.allocated_bytes()
    }
}

pub type HecProcessedEvent = ProcessedEvent<OtelLog, HecLogsProcessedEventMetadata>;

pub fn process_log(event: Event, data: &HecLogData) -> HecProcessedEvent {
    let mut log = event.into_log();

    let sourcetype = data
        .sourcetype
        .and_then(|sourcetype| render_template_string(sourcetype, &log, SOURCETYPE_FIELD));

    let source = data
        .source
        .and_then(|source| render_template_string(source, &log, SOURCE_FIELD));

    let index = data
        .index
        .and_then(|index| render_template_string(index, &log, INDEX_FIELD));

    let host = match data.host_key.as_ref() {
        Some(maybe_key) => maybe_key.path.as_ref().and_then(|path| log.get(path)),
        None => log
            .find_key_by_meaning(meaning::HOST)
            .and_then(|path| log.get(path)),
    };

    // only extract the timestamp if this is the Event endpoint, and if the setting
    // `auto_extract_timestamp` is false (because that indicates that we should leave
    // the timestamp in the event as-is, and let Splunk do the extraction).
    let timestamp = if EndpointTarget::Event == data.endpoint_target && !data.auto_extract_timestamp
    {
        let removed = match data.timestamp_key.as_ref().and_then(|k| k.path.as_ref()) {
            Some(user_path) => log.remove(user_path),
            None => log.remove_timestamp(),
        };
        let ts_opt = match removed {
            Some(Value::Timestamp(ts)) => Some(ts),
            Some(Value::Integer(nanos)) => {
                let secs = nanos / 1_000_000_000;
                #[expect(
                    clippy::cast_sign_loss,
                    reason = "nanos modulo 1e9 is always in 0..999_999_999"
                )]
                let nsecs = (nanos % 1_000_000_000) as u32;
                chrono::DateTime::from_timestamp(secs, nsecs)
            }
            Some(value) => {
                emit!(SplunkEventTimestampInvalidType {
                    r#type: value.kind_str()
                });
                None
            }
            None => {
                emit!(SplunkEventTimestampMissing {});
                None
            }
        };
        ts_opt.map(|ts| {
            if let Some(key) = data.timestamp_nanos_key {
                log.try_insert(event_path!(key), ts.timestamp_subsec_nanos() % 1_000_000);
            }
            {
                #[expect(clippy::cast_precision_loss, reason = "epoch millis to f64 for Splunk HEC timestamp; precise for dates until year 2255")]
                let millis = ts.timestamp_millis() as f64;
                millis / 1000f64
            }
        })
    } else {
        None
    };

    let fields = OtelLog::from(
        data.indexed_fields
            .iter()
            .filter_map(|field| {
                log.get((PathPrefix::Event, field))
                    .map(|value| (KeyString::from(field.to_string()), value))
            })
            .collect::<ObjectMap>(),
    );

    let metadata = HecLogsProcessedEventMetadata {
        sourcetype,
        source,
        index,
        host,
        timestamp,
        fields,
        endpoint_target: data.endpoint_target,
    };

    ProcessedEvent {
        event: log,
        metadata,
    }
}

impl EventCount for HecProcessedEvent {
    fn event_count(&self) -> usize {
        // A HecProcessedEvent is mapped one-to-one with an event.
        1
    }
}
