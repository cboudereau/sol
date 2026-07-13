use std::{
    convert::TryFrom,
    num::NonZeroU64,
    time::{Duration, Instant},
};

use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{
    FutureExt, future,
    stream::{BoxStream, StreamExt},
};
use serde_with::serde_as;
use sol_lib::{
    EstimatedJsonEncodedSizeOf, TimeZone,
    codecs::{
        TextSerializerConfig,
        encoding::{Framer, FramingConfig},
    },
    configurable::configurable_component,
    internal_event::{CountByteSize, EventsSent, InternalEventHandle as _, Output, Registered},
};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};
use tokio_util::{codec::Encoder as _, time::delay_queue::Expired};

use crate::{
    codecs::{Encoder, EncodingConfigWithFraming, SinkType, Transformer},
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext},
    event::{Event, EventStatus, Finalizable},
    expiring_hash_map::ExpiringHashMap,
    internal_events::{
        FileBytesSent, FileInternalMetricsConfig, FileIoError, FileOpen, TemplateRenderingError,
    },
    sinks::util::{StreamSink, timezone_to_offset},
    template::Template,
};
#[cfg(feature = "codecs-parquet")]
use sol_lib::{codecs::encoding::BatchSerializerConfig, json_size::JsonSize};

mod bytes_path;

use bytes_path::BytesPath;

/// Configuration for the `file` sink.
#[serde_as]
#[configurable_component(sink("file", "Output observability events into files."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileSinkConfig {
    /// File path to write events to.
    ///
    /// Compression format extension must be explicit.
    #[configurable(metadata(docs::examples = "/tmp/vector-%Y-%m-%d.log"))]
    #[configurable(metadata(
        docs::examples = "/tmp/application-{{ application_id }}-%Y-%m-%d.log"
    ))]
    #[configurable(metadata(docs::examples = "/tmp/vector-%Y-%m-%d.log.zst"))]
    pub path: Template,

    /// The amount of time that a file can be idle and stay open.
    ///
    /// After not receiving any events in this amount of time, the file is flushed and closed.
    #[serde(default = "default_idle_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "idle_timeout_secs")]
    #[configurable(metadata(docs::examples = 600))]
    #[configurable(metadata(docs::human_name = "Idle Timeout"))]
    pub idle_timeout: Duration,

    #[serde(flatten)]
    pub encoding: EncodingConfigWithFraming,

    /// Batch encoding for producing complete files per batch (e.g., Parquet).
    ///
    /// When set, events are buffered and encoded together. Each batch produces
    /// a self-contained file. Mutually exclusive with per-event `encoding`.
    #[cfg(feature = "codecs-parquet")]
    #[configurable(derived)]
    #[serde(default)]
    pub batch_encoding: Option<BatchSerializerConfig>,

    /// Batch settings controlling how many events are buffered before writing.
    ///
    /// Only used when `batch_encoding` is set.
    #[cfg(feature = "codecs-parquet")]
    #[configurable(derived)]
    #[serde(default)]
    pub batch: Option<FileBatchConfig>,

    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    pub compression: Compression,

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub timezone: Option<TimeZone>,

    #[configurable(derived)]
    #[serde(default)]
    pub internal_metrics: FileInternalMetricsConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub truncate: FileTruncateConfig,
}

/// Batch configuration for the Parquet file sink.
#[cfg(feature = "codecs-parquet")]
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileBatchConfig {
    /// Maximum number of events per batch.
    #[serde(default = "default_batch_max_events")]
    pub max_events: usize,

    /// Maximum time in seconds to wait before flushing a batch.
    #[serde(default = "default_batch_timeout_secs")]
    pub timeout_secs: f64,
}

#[cfg(feature = "codecs-parquet")]
impl Default for FileBatchConfig {
    fn default() -> Self {
        Self {
            max_events: default_batch_max_events(),
            timeout_secs: default_batch_timeout_secs(),
        }
    }
}

#[cfg(feature = "codecs-parquet")]
const fn default_batch_max_events() -> usize {
    10_000
}

#[cfg(feature = "codecs-parquet")]
const fn default_batch_timeout_secs() -> f64 {
    60.0
}

/// Configuration for truncating files.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct FileTruncateConfig {
    /// If this is set, files will be truncated after being closed for a set amount of seconds.
    #[serde(default)]
    pub after_close_time_secs: Option<NonZeroU64>,
    /// If this is set, files will be truncated after set amount of seconds of no modifications.
    #[serde(default)]
    pub after_modified_time_secs: Option<NonZeroU64>,
    /// If this is set, files will be truncated after set amount of seconds regardless of the state.
    #[serde(default)]
    pub after_secs: Option<NonZeroU64>,
}

impl GenerateConfig for FileSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            path: Template::try_from("/tmp/vector-%Y-%m-%d.log").unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Default::default(),
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: Default::default(),
            truncate: Default::default(),
        })
        .unwrap()
    }
}

const fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}

/// Compression configuration.
// TODO: Why doesn't this already use `crate::sinks::util::Compression`
// `crate::sinks::util::Compression` doesn't support zstd yet
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// [Gzip][gzip] compression.
    ///
    /// [gzip]: https://www.gzip.org/
    Gzip,

    /// [Zstandard][zstd] compression.
    ///
    /// [zstd]: https://facebook.github.io/zstd/
    Zstd,

    /// No compression.
    #[default]
    None,
}

struct OutFile {
    created_at: Instant,
    inner: OutFileInner,
}

enum OutFileInner {
    Regular(File),
    Gzip(GzipEncoder<File>),
    Zstd(ZstdEncoder<File>),
}

impl OutFile {
    fn new(file: File, compression: Compression) -> Self {
        Self {
            created_at: Instant::now(),
            inner: match compression {
                Compression::None => OutFileInner::Regular(file),
                Compression::Gzip => OutFileInner::Gzip(GzipEncoder::new(file)),
                Compression::Zstd => OutFileInner::Zstd(ZstdEncoder::new(file)),
            },
        }
    }

    async fn sync_all(&mut self) -> Result<(), std::io::Error> {
        match &mut self.inner {
            OutFileInner::Regular(file) => file.sync_all().await,
            OutFileInner::Gzip(gzip) => gzip.get_mut().sync_all().await,
            OutFileInner::Zstd(zstd) => zstd.get_mut().sync_all().await,
        }
    }

    async fn shutdown(&mut self) -> Result<(), std::io::Error> {
        match &mut self.inner {
            OutFileInner::Regular(file) => file.shutdown().await,
            OutFileInner::Gzip(gzip) => gzip.shutdown().await,
            OutFileInner::Zstd(zstd) => zstd.shutdown().await,
        }
    }

    async fn write_all(&mut self, src: &[u8]) -> Result<(), std::io::Error> {
        match &mut self.inner {
            OutFileInner::Regular(file) => file.write_all(src).await,
            OutFileInner::Gzip(gzip) => gzip.write_all(src).await,
            OutFileInner::Zstd(zstd) => zstd.write_all(src).await,
        }
    }

    const fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Shutdowns by flushing data, writing headers, and syncing all of that
    /// data and metadata to the filesystem.
    async fn close(&mut self) -> Result<(), std::io::Error> {
        self.shutdown().await?;
        self.sync_all().await
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "file")]
impl SinkConfig for FileSinkConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        #[cfg(feature = "codecs-parquet")]
        if let Some(ref batch_encoding) = self.batch_encoding {
            if self.compression != Compression::None {
                return Err("When using batch_encoding, set compression to 'none'. \
                     Parquet uses internal column-level compression."
                    .into());
            }
            let sink = BatchFileSink::new(self, batch_encoding, cx)?;
            return Ok((
                super::VectorSink::from_event_streamsink(sink),
                future::ok(()).boxed(),
            ));
        }

        let sink = FileSink::new(self, cx)?;
        Ok((
            super::VectorSink::from_event_streamsink(sink),
            future::ok(()).boxed(),
        ))
    }

    fn input(&self) -> Input {
        #[cfg(feature = "codecs-parquet")]
        if let Some(ref batch_encoding) = self.batch_encoding {
            return Input::new(batch_encoding.input_type());
        }
        Input::new(self.encoding.config().1.input_type())
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

pub struct FileSink {
    path: Template,
    transformer: Transformer,
    encoder: Encoder<Framer>,
    idle_timeout: Duration,
    files: ExpiringHashMap<Bytes, OutFile>,
    compression: Compression,
    events_sent: Registered<EventsSent>,
    include_file_metric_tag: bool,
    truncation_config: FileTruncateConfig,
}

impl FileSink {
    pub fn new(config: &FileSinkConfig, cx: SinkContext) -> crate::Result<Self> {
        let transformer = config.encoding.transformer();
        let (framer, serializer) = config.encoding.build(SinkType::StreamBased)?;
        let encoder = Encoder::<Framer>::new(framer, serializer);

        let offset = config
            .timezone
            .or(cx.globals.timezone)
            .and_then(timezone_to_offset);

        Ok(Self {
            path: config.path.clone().with_tz_offset(offset),
            transformer,
            encoder,
            idle_timeout: config.idle_timeout,
            files: ExpiringHashMap::default(),
            compression: config.compression,
            events_sent: register!(EventsSent::from(Output(None))),
            include_file_metric_tag: config.internal_metrics.include_file_tag,
            truncation_config: config.truncate.clone(),
        })
    }

    /// Uses pass the `event` to `self.path` template to obtain the file path
    /// to store the event as.
    fn partition_event(&mut self, event: &Event) -> Option<bytes::Bytes> {
        let bytes = match self.path.render(event) {
            Ok(b) => b,
            Err(error) => {
                emit!(TemplateRenderingError {
                    error,
                    field: Some("path"),
                    drop_event: true,
                });
                return None;
            }
        };

        Some(bytes)
    }

    fn deadline_at(&self) -> Instant {
        Instant::now()
            .checked_add(self.idle_timeout)
            .expect("unable to compute next deadline")
    }

    async fn run(&mut self, mut input: BoxStream<'_, Event>) -> crate::Result<()> {
        loop {
            tokio::select! {
                event = input.next() => {
                    match event {
                        Some(event) => self.process_event(event).await,
                        None => {
                            // If we got `None` - terminate the processing.
                            debug!(message = "Receiver exhausted, terminating the processing loop.");

                            // Close all the open files.
                            debug!(message = "Closing all the open files.");
                            for (path, file) in self.files.iter_mut() {
                                if let Err(error) = file.close().await {
                                    emit!(FileIoError {
                                        error,
                                        code: "failed_closing_file",
                                        message: "Failed to close file.",
                                        path,
                                        dropped_events: 0,
                                    });
                                } else{
                                    trace!(message = "Successfully closed file.", path = ?path);
                                }
                            }

                            emit!(FileOpen {
                                count: 0
                            });

                            break;
                        }
                    }
                }
                result = self.files.next_expired(), if !self.files.is_empty() => {
                    match result {
                        // We do not poll map when it's empty, so we should
                        // never reach this branch.
                        None => unreachable!(),
                        Some((expired_file, path)) => {
                            // We got an expired file. All we really want is to
                            // flush and close it.
                            self.close_file(expired_file, path).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_event(&mut self, mut event: Event) {
        let path = match self.partition_event(&event) {
            Some(path) => path,
            None => {
                // We weren't able to find the path to use for the
                // file.
                // The error is already handled at `partition_event`, so
                // here we just skip the event.
                event.metadata().update_status(EventStatus::Errored);
                return;
            }
        };

        let next_deadline = self.deadline_at();
        trace!(message = "Computed next deadline.", next_deadline = ?next_deadline, path = ?path);

        let bytes_path = BytesPath::new(path.clone());
        let truncate = self.should_truncate(&bytes_path, &path).await;
        let file = if !truncate && let Some(file) = self.files.reset_at(&path, next_deadline) {
            trace!(message = "Working with an already opened file.", path = ?path);
            file
        } else {
            trace!(message = "Opening new file.", ?path);
            let file = match open_file(bytes_path, truncate).await {
                Ok(file) => file,
                Err(error) => {
                    // We couldn't open the file for this event.
                    // Maybe other events will work though! Just log
                    // the error and skip this event.
                    emit!(FileIoError {
                        code: "failed_opening_file",
                        message: "Unable to open the file.",
                        error,
                        path: &path,
                        dropped_events: 1,
                    });
                    event.metadata().update_status(EventStatus::Errored);
                    return;
                }
            };

            let outfile = OutFile::new(file, self.compression);

            self.files.insert_at(path.clone(), outfile, next_deadline);
            emit!(FileOpen {
                count: self.files.len()
            });
            self.files.get_mut(&path).unwrap()
        };

        trace!(message = "Writing an event to file.", path = ?path);
        let event_size = event.estimated_json_encoded_size_of();
        let finalizers = event.take_finalizers();
        match write_event_to_file(file, event, &self.transformer, &mut self.encoder).await {
            Ok(byte_size) => {
                finalizers.update_status(EventStatus::Delivered);
                self.events_sent.emit(CountByteSize(1, event_size));
                emit!(FileBytesSent {
                    byte_size,
                    file: String::from_utf8_lossy(&path),
                    include_file_metric_tag: self.include_file_metric_tag,
                });
            }
            Err(error) => {
                finalizers.update_status(EventStatus::Errored);
                emit!(FileIoError {
                    code: "failed_writing_file",
                    message: "Failed to write the file.",
                    error,
                    path: &path,
                    dropped_events: 1,
                });
            }
        }
    }

    async fn should_truncate(&mut self, bytes_path: &BytesPath, path: &bytes::Bytes) -> bool {
        let mut truncate = false;

        if let Some(after_close_time_secs) = self.truncation_config.after_close_time_secs
            && self.files.get(path).is_none()
            && let Ok(metadata) = fs::metadata(bytes_path).await
            && let Ok(time) = metadata
                .modified()
                .map_err(|_| ())
                .and_then(|t| t.elapsed().map_err(|_| ()))
            && time.as_secs() > after_close_time_secs.into()
        {
            truncate = true;
        }

        if let Some(after_secs) = self.truncation_config.after_secs
            && let Some(file) = self.files.get(path)
            && (file.created_at().elapsed().as_secs() > after_secs.into())
        {
            truncate = true;
        }

        if let Some(after_modified_time_secs) = self.truncation_config.after_modified_time_secs
            && let Some(previous_modification) = self
                .files
                .get_with_deadline(path)
                .and_then(|(_, deadline)| deadline.checked_sub(self.idle_timeout))
            && previous_modification.elapsed().as_secs() > after_modified_time_secs.into()
        {
            truncate = true;
        }

        if truncate && let Some((file, path)) = self.files.remove(path) {
            self.close_file(file, path).await;
        }

        truncate
    }

    async fn close_file(&self, mut file: OutFile, path: Expired<Bytes>) {
        if let Err(error) = file.close().await {
            emit!(FileIoError {
                error,
                code: "failed_closing_file",
                message: "Failed to close file.",
                path: &path,
                dropped_events: 0,
            });
        }
        drop(file); // ignore close error
        emit!(FileOpen {
            count: self.files.len()
        });
    }
}

#[cfg(feature = "codecs-parquet")]
pub struct BatchFileSink {
    path: Template,
    transformer: Transformer,
    encoder: sol_lib::codecs::BatchEncoder,
    max_events: usize,
    timeout: Duration,
    events_sent: Registered<EventsSent>,
    include_file_metric_tag: bool,
}

#[cfg(feature = "codecs-parquet")]
impl BatchFileSink {
    pub fn new(
        config: &FileSinkConfig,
        batch_encoding: &BatchSerializerConfig,
        cx: SinkContext,
    ) -> crate::Result<Self> {
        let transformer = config.encoding.transformer();
        let batch_serializer = batch_encoding.build()?;
        let encoder = sol_lib::codecs::BatchEncoder::new(batch_serializer);

        let batch_config = config.batch.clone().unwrap_or_default();

        let offset = config
            .timezone
            .or(cx.globals.timezone)
            .and_then(timezone_to_offset);

        Ok(Self {
            path: config.path.clone().with_tz_offset(offset),
            transformer,
            encoder,
            max_events: batch_config.max_events,
            timeout: Duration::from_secs_f64(batch_config.timeout_secs),
            events_sent: register!(EventsSent::from(Output(None))),
            include_file_metric_tag: config.internal_metrics.include_file_tag,
        })
    }

    async fn run(&mut self, mut input: BoxStream<'_, Event>) -> crate::Result<()> {
        let mut buffer: Vec<Event> = Vec::with_capacity(self.max_events);
        let sleep = tokio::time::sleep(self.timeout);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                event = input.next() => {
                    match event {
                        Some(event) => {
                            buffer.push(event);
                            if buffer.len() >= self.max_events {
                                self.flush_batch(&mut buffer).await;
                                sleep.as_mut().reset(tokio::time::Instant::now() + self.timeout);
                            }
                        }
                        None => {
                            if !buffer.is_empty() {
                                self.flush_batch(&mut buffer).await;
                            }
                            break;
                        }
                    }
                }
                () = &mut sleep => {
                    if !buffer.is_empty() {
                        self.flush_batch(&mut buffer).await;
                    }
                    sleep.as_mut().reset(tokio::time::Instant::now() + self.timeout);
                }
            }
        }
        Ok(())
    }

    async fn flush_batch(&mut self, buffer: &mut Vec<Event>) {
        let mut events = std::mem::take(buffer);
        let n_events = events.len();

        let path = match events.first() {
            Some(first) => match self.path.render(first) {
                Ok(b) => b,
                Err(error) => {
                    emit!(TemplateRenderingError {
                        error,
                        field: Some("path"),
                        drop_event: true,
                    });
                    for event in &events {
                        event.metadata().update_status(EventStatus::Errored);
                    }
                    return;
                }
            },
            None => return,
        };

        let finalizers = events.take_finalizers();

        // Apply transformer to each event before encoding.
        for event in &mut events {
            self.transformer.transform(event);
        }

        match self.encoder.encode_files_with_bounds(events) {
            Ok(files) => {
                let total_byte_size: usize = files.iter().map(|f| f.data.len()).sum();
                // One uniqueness token per flush — distinct writers (and repeat
                // flushes inside the same second) never target the same file.
                let token = uuid::Uuid::new_v4().to_string();
                let mut failed = false;
                let multi = files.len() > 1;
                for (i, file) in files.iter().enumerate() {
                    let index = multi.then_some(i);
                    // Self-describing name (backend-metrics-perf FR1, ADR A′):
                    // when the codec computed the batch's exact event-time
                    // bounds, stamp them into the basename so the querier
                    // inventory prunes on exact per-file intervals. Otherwise
                    // (no timestamped rows, or a non-Parquet batch codec) keep
                    // the legacy timestamped-template name.
                    let file_path = match file.time_bounds {
                        Some((min_ns, max_ns)) => {
                            parquet_bounds_path(&path, min_ns, max_ns, &token, index)
                        }
                        None => parquet_batch_path(&path, &token, index),
                    };
                    let file_bytes = &file.data;
                    let bytes_path = BytesPath::new(file_path.clone());
                    match open_file(bytes_path, false).await {
                        Ok(mut file) => {
                            if let Err(error) = file.write_all(file_bytes).await {
                                failed = true;
                                emit!(FileIoError {
                                    error,
                                    code: "failed_writing_file",
                                    message: "Failed to write batch to file.",
                                    path: &file_path,
                                    dropped_events: 0,
                                });
                                break;
                            }
                            if let Err(error) = file.shutdown().await {
                                failed = true;
                                emit!(FileIoError {
                                    error,
                                    code: "failed_closing_file",
                                    message: "Failed to close batch file.",
                                    path: &file_path,
                                    dropped_events: 0,
                                });
                                break;
                            }
                            emit!(FileBytesSent {
                                byte_size: file_bytes.len(),
                                file: String::from_utf8_lossy(&file_path),
                                include_file_metric_tag: self.include_file_metric_tag,
                            });
                        }
                        Err(error) => {
                            failed = true;
                            emit!(FileIoError {
                                code: "failed_opening_file",
                                message: "Unable to open file for batch.",
                                error,
                                path: &file_path,
                                dropped_events: 0,
                            });
                            break;
                        }
                    }
                }
                if failed {
                    finalizers.update_status(EventStatus::Errored);
                } else {
                    finalizers.update_status(EventStatus::Delivered);
                    self.events_sent
                        .emit(CountByteSize(n_events, JsonSize::new(total_byte_size)));
                }
            }
            Err(error) => {
                finalizers.update_status(EventStatus::Errored);
                let io_error =
                    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string());
                emit!(FileIoError {
                    error: io_error,
                    code: "failed_encoding_batch",
                    message: "Failed to encode batch.",
                    path: &path,
                    dropped_events: n_events,
                });
            }
        }
    }
}

/// Build a unique batch filename by inserting a per-flush uniqueness `token`
/// (and, for multi-file batches, an `index`) before the `.parquet` extension:
/// `foo.parquet` → `foo-<token>.parquet` / `foo-<token>-<index>.parquet`.
///
/// The token is what prevents two writers that resolve the same timestamped
/// path — e.g. replicated collectors sharing a Parquet volume, or one writer
/// flushing twice within the same `%H-%M-%S` second — from opening the same
/// file in append mode and concatenating two complete Parquet bodies into one
/// unreadable file (the trailing footer's row-group offsets no longer match).
#[cfg(feature = "codecs-parquet")]
fn parquet_batch_path(path: &[u8], token: &str, index: Option<usize>) -> Bytes {
    let s = String::from_utf8_lossy(path);
    let (stem, ext) = match s.strip_suffix(".parquet") {
        Some(stem) => (stem, ".parquet"),
        None => (s.as_ref(), ""),
    };
    let suffixed = match index {
        Some(i) => format!("{stem}-{token}-{i}{ext}"),
        None => format!("{stem}-{token}{ext}"),
    };
    Bytes::from(suffixed)
}

/// Floor for a name-carried epoch-ns bound: a bound below this (< 10 decimal
/// digits, i.e. before 1970-01-01T00:00:01Z) would be indistinguishable from a
/// legacy `HH-MM-SS` field and would not round-trip through the querier's
/// exact-bounds parser (`crate::querier::inventory::EXACT_BOUNDS_MIN_DIGITS`).
/// Real event timestamps are ~19 digits, so this only guards pathological
/// inputs; when it trips we fall back to the timestamped-template name.
#[cfg(feature = "codecs-parquet")]
const EXACT_BOUNDS_MIN_NS: i64 = 1_000_000_000;

/// Compose a self-describing batch filename carrying the batch's **exact**
/// event-time bounds (backend-metrics-perf FR1, ADR A′): the timestamped
/// basename rendered from the path template is replaced with
/// `<min_ns>-<max_ns>-<token>[-<index>].parquet`, keeping the `dt=YYYY-MM-DD`
/// directory the template produced. The querier's file inventory parses these
/// bounds directly (`crate::querier::inventory::parse_file_interval`), pruning
/// on exact per-file intervals instead of a conservative day/flush estimate.
///
/// Falls back to [`parquet_batch_path`] (the legacy name) if the bounds cannot
/// round-trip through the parser, so a stamped name is always parseable.
#[cfg(feature = "codecs-parquet")]
fn parquet_bounds_path(
    path: &[u8],
    min_ns: i64,
    max_ns: i64,
    token: &str,
    index: Option<usize>,
) -> Bytes {
    if min_ns < EXACT_BOUNDS_MIN_NS || max_ns < min_ns {
        return parquet_batch_path(path, token, index);
    }
    let s = String::from_utf8_lossy(path);
    // Keep the directory the template rendered (e.g. `dt=2026-07-10/`); replace
    // only the basename with the exact-bounds name.
    let (dir, _basename) = match s.rfind('/') {
        Some(slash) => s.split_at(slash + 1),
        None => ("", s.as_ref()),
    };
    let name = match index {
        Some(i) => format!("{dir}{min_ns}-{max_ns}-{token}-{i}.parquet"),
        None => format!("{dir}{min_ns}-{max_ns}-{token}.parquet"),
    };
    Bytes::from(name)
}

#[cfg(feature = "codecs-parquet")]
#[async_trait]
impl StreamSink<Event> for BatchFileSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        BatchFileSink::run(&mut self, input)
            .await
            .expect("batch file sink error");
        Ok(())
    }
}

async fn open_file(path: impl AsRef<std::path::Path>, truncate: bool) -> std::io::Result<File> {
    let parent = path.as_ref().parent();

    if let Some(parent) = parent {
        fs::create_dir_all(parent).await?;
    }

    fs::OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .append(!truncate)
        .truncate(truncate)
        .open(path)
        .await
}

async fn write_event_to_file(
    file: &mut OutFile,
    mut event: Event,
    transformer: &Transformer,
    encoder: &mut Encoder<Framer>,
) -> Result<usize, std::io::Error> {
    transformer.transform(&mut event);
    let mut buffer = BytesMut::new();
    encoder
        .encode(event, &mut buffer)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    file.write_all(&buffer).await.map(|()| buffer.len())
}

#[async_trait]
impl StreamSink<Event> for FileSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        FileSink::run(&mut self, input)
            .await
            .expect("file sink error");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use chrono::{SubsecRound, Utc};
    use futures::{SinkExt, stream};
    use similar_asserts::assert_eq;
    use sol_lib::{
        codecs::JsonSerializerConfig,
        event::{EventMetadata, OtelLog, OtelSpan},
        sink::VectorSink,
    };
    use vrl::value::Value;

    use super::*;
    use crate::test_util::{
        components::{FILE_SINK_TAGS, assert_sink_compliance},
        lines_from_file, lines_from_gzip_file, lines_from_zstd_file, random_events_with_stream,
        random_lines_with_stream, random_metrics_with_stream, random_metrics_with_stream_timestamp,
        temp_dir, temp_file, trace_init,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<FileSinkConfig>();
    }

    #[tokio::test]
    async fn log_single_partition() {
        let template = temp_file();

        let config = FileSinkConfig {
            path: template.clone().try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Compression::None,
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: FileInternalMetricsConfig {
                include_file_tag: true,
            },
            truncate: Default::default(),
        };

        let (input, _events) = random_lines_with_stream(100, 64, None);

        run_assert_log_sink(&config, input.clone()).await;

        let output = lines_from_file(template);
        for (input, output) in input.into_iter().zip(output) {
            assert_eq!(input, output);
        }
    }

    #[tokio::test]
    async fn log_single_partition_gzip() {
        let template = temp_file();

        let config = FileSinkConfig {
            path: template.clone().try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Compression::Gzip,
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: FileInternalMetricsConfig {
                include_file_tag: true,
            },
            truncate: Default::default(),
        };

        let (input, _) = random_lines_with_stream(100, 64, None);

        run_assert_log_sink(&config, input.clone()).await;

        let output = lines_from_gzip_file(template);
        for (input, output) in input.into_iter().zip(output) {
            assert_eq!(input, output);
        }
    }

    #[tokio::test]
    async fn log_single_partition_zstd() {
        let template = temp_file();

        let config = FileSinkConfig {
            path: template.clone().try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Compression::Zstd,
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: FileInternalMetricsConfig {
                include_file_tag: true,
            },
            truncate: Default::default(),
        };

        let (input, _) = random_lines_with_stream(100, 64, None);

        run_assert_log_sink(&config, input.clone()).await;

        let output = lines_from_zstd_file(template);
        for (input, output) in input.into_iter().zip(output) {
            assert_eq!(input, output);
        }
    }

    #[tokio::test]
    async fn log_many_partitions() {
        let directory = temp_dir();

        let mut template = directory.to_string_lossy().to_string();
        template.push_str("/{{level}}s-{{date}}.log");

        trace!(message = "Template.", %template);

        let config = FileSinkConfig {
            path: template.try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Compression::None,
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: FileInternalMetricsConfig {
                include_file_tag: true,
            },
            truncate: Default::default(),
        };

        let (mut input, _events) = random_events_with_stream(32, 8, None);
        input[0].as_mut_log().insert("date", "2019-26-07");
        input[0].as_mut_log().insert("level", "warning");
        input[1].as_mut_log().insert("date", "2019-26-07");
        input[1].as_mut_log().insert("level", "error");
        input[2].as_mut_log().insert("date", "2019-26-07");
        input[2].as_mut_log().insert("level", "warning");
        input[3].as_mut_log().insert("date", "2019-27-07");
        input[3].as_mut_log().insert("level", "error");
        input[4].as_mut_log().insert("date", "2019-27-07");
        input[4].as_mut_log().insert("level", "warning");
        input[5].as_mut_log().insert("date", "2019-27-07");
        input[5].as_mut_log().insert("level", "warning");
        input[6].as_mut_log().insert("date", "2019-28-07");
        input[6].as_mut_log().insert("level", "warning");
        input[7].as_mut_log().insert("date", "2019-29-07");
        input[7].as_mut_log().insert("level", "error");

        run_assert_sink(&config, input.clone().into_iter()).await;

        let output = [
            lines_from_file(directory.join("warnings-2019-26-07.log")),
            lines_from_file(directory.join("errors-2019-26-07.log")),
            lines_from_file(directory.join("warnings-2019-27-07.log")),
            lines_from_file(directory.join("errors-2019-27-07.log")),
            lines_from_file(directory.join("warnings-2019-28-07.log")),
            lines_from_file(directory.join("errors-2019-29-07.log")),
        ];

        assert_eq!(
            input[0].as_log().get("body").unwrap(),
            Value::from(&output[0][0] as &str)
        );
        assert_eq!(
            input[1].as_log().get("body").unwrap(),
            Value::from(&output[1][0] as &str)
        );
        assert_eq!(
            input[2].as_log().get("body").unwrap(),
            Value::from(&output[0][1] as &str)
        );
        assert_eq!(
            input[3].as_log().get("body").unwrap(),
            Value::from(&output[3][0] as &str)
        );
        assert_eq!(
            input[4].as_log().get("body").unwrap(),
            Value::from(&output[2][0] as &str)
        );
        assert_eq!(
            input[5].as_log().get("body").unwrap(),
            Value::from(&output[2][1] as &str)
        );
        assert_eq!(
            input[6].as_log().get("body").unwrap(),
            Value::from(&output[4][0] as &str)
        );
        assert_eq!(
            input[7].as_log().get("body").unwrap(),
            Value::from(&output[5][0] as &str)
        );
    }

    #[tokio::test]
    async fn log_reopening() {
        trace_init();

        let template = temp_file();

        let config = FileSinkConfig {
            path: template.clone().try_into().unwrap(),
            idle_timeout: Duration::from_secs(1),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Compression::None,
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: FileInternalMetricsConfig {
                include_file_tag: true,
            },
            truncate: Default::default(),
        };

        let (mut input, _events) = random_lines_with_stream(10, 64, None);

        let (mut tx, rx) = futures::channel::mpsc::channel(0);

        let sink_handle = tokio::spawn(async move {
            assert_sink_compliance(&FILE_SINK_TAGS, async move {
                let sink = FileSink::new(&config, SinkContext::default()).unwrap();
                VectorSink::from_event_streamsink(sink)
                    .run(Box::pin(rx.map(Into::into)))
                    .await
                    .expect("Running sink failed");
            })
            .await
        });

        // send initial payload
        for line in input.clone() {
            tx.send(Event::Log(OtelLog::from(line))).await.unwrap();
        }

        // wait for file to go idle and be closed
        tokio::time::sleep(Duration::from_secs(2)).await;

        // trigger another write
        let last_line = "i should go at the end";
        tx.send(OtelLog::from(last_line).into()).await.unwrap();
        input.push(String::from(last_line));

        // wait for another flush
        tokio::time::sleep(Duration::from_secs(1)).await;

        // make sure we appended instead of overwriting
        let output = lines_from_file(template);
        assert_eq!(input, output);

        // make sure sink stops and that it did not panic
        drop(tx);
        sink_handle.await.unwrap();
    }

    #[tokio::test]
    async fn metric_single_partition() {
        let template = temp_file();

        let config = FileSinkConfig {
            path: template.clone().try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Compression::None,
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: FileInternalMetricsConfig {
                include_file_tag: true,
            },
            truncate: Default::default(),
        };

        let (input, _events) = random_metrics_with_stream(100, None, None);

        run_assert_sink(&config, input.clone().into_iter()).await;

        let output = lines_from_file(template);
        for (input, output) in input.into_iter().zip(output) {
            let metric_name = input.as_metric().name();
            assert!(output.contains(metric_name));
        }
    }

    #[tokio::test]
    async fn metric_many_partitions() {
        let directory = temp_dir();

        let format = "%Y-%m-%d-%H-%M-%S";
        let mut template = directory.to_string_lossy().to_string();
        template.push_str(&format!("/{format}.log"));

        let config = FileSinkConfig {
            path: template.try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Compression::None,
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: FileInternalMetricsConfig {
                include_file_tag: true,
            },
            truncate: Default::default(),
        };

        let metric_count = 3;
        let timestamp = Utc::now().trunc_subsecs(3);
        let timestamp_offset = Duration::from_secs(1);

        let (input, _events) = random_metrics_with_stream_timestamp(
            metric_count,
            None,
            None,
            timestamp,
            timestamp_offset,
        );

        run_assert_sink(&config, input.clone().into_iter()).await;

        let output = (0..metric_count).map(|index| {
            #[expect(clippy::cast_possible_truncation, reason = "test index fits in u32")]
            let idx = index as u32;
            let expected_timestamp = timestamp + (timestamp_offset * idx);
            let expected_filename =
                directory.join(format!("{}.log", expected_timestamp.format(format)));

            lines_from_file(expected_filename)
        });
        for (input, output) in input.iter().zip(output) {
            // The format will partition by second and metrics are a second apart.
            assert_eq!(
                output.len(),
                1,
                "Expected the output file to contain one metric"
            );
            let output = &output[0];

            let metric_name = input.as_metric().name();
            assert!(output.contains(metric_name));
        }
    }

    #[tokio::test]
    async fn trace_single_partition() {
        let template = temp_file();

        let config = FileSinkConfig {
            path: template.clone().try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, JsonSerializerConfig::default()).into(),
            #[cfg(feature = "codecs-parquet")]
            batch_encoding: None,
            #[cfg(feature = "codecs-parquet")]
            batch: None,
            compression: Compression::None,
            acknowledgements: Default::default(),
            timezone: Default::default(),
            internal_metrics: FileInternalMetricsConfig {
                include_file_tag: true,
            },
            truncate: Default::default(),
        };

        let (input, _events) = random_lines_with_stream(100, 64, None);

        run_assert_trace_sink(&config, input.clone()).await;

        let output = lines_from_file(template);
        for (input, output) in input.iter().zip(output) {
            assert!(output.contains(input));
        }
    }

    async fn run_assert_log_sink(config: &FileSinkConfig, events: Vec<String>) {
        run_assert_sink(
            config,
            events.into_iter().map(OtelLog::from).map(Event::from),
        )
        .await;
    }

    async fn run_assert_trace_sink(config: &FileSinkConfig, events: Vec<String>) {
        run_assert_sink(
            config,
            events.into_iter().map(|s| {
                // Build a trace event with the string as a named span
                // attribute so it survives serialization (OtelSpan
                // serializes via as_map which maps
                // attributes to top-level fields).
                let mut map = vrl::value::ObjectMap::new();
                map.insert("message".into(), Value::from(s));
                Event::Trace(OtelSpan::from_value_map(
                    Value::Object(map),
                    EventMetadata::default(),
                ))
            }),
        )
        .await;
    }

    async fn run_assert_sink(config: &FileSinkConfig, events: impl Iterator<Item = Event> + Send) {
        assert_sink_compliance(&FILE_SINK_TAGS, async move {
            let sink = FileSink::new(config, SinkContext::default()).unwrap();
            VectorSink::from_event_streamsink(sink)
                .run(Box::pin(stream::iter(events.map(Into::into))))
                .await
                .expect("Running sink failed")
        })
        .await;
    }

    #[cfg(feature = "codecs-parquet")]
    #[test]
    fn parquet_batch_config_deserializes() {
        let yaml = r#"
            path: "/data/parquet/logs/%Y-%m-%d-%H-%M-%S.parquet"
            batch_encoding:
              codec: parquet
              compression: zstd
            batch:
              max_events: 5000
              timeout_secs: 30
        "#;
        let config: FileSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.batch_encoding.is_some());
        assert!(config.batch.is_some());
        assert_eq!(config.batch.unwrap().max_events, 5000);
    }

    #[cfg(feature = "codecs-parquet")]
    #[test]
    fn parquet_batch_config_defaults() {
        let yaml = r#"
            path: "/data/parquet/traces/%Y-%m-%d-%H-%M-%S.parquet"
            batch_encoding:
              codec: parquet
        "#;
        let config: FileSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.batch_encoding.is_some());
        assert!(config.batch.is_none());
    }

    #[cfg(feature = "codecs-parquet")]
    #[test]
    fn batch_path_inserts_token_before_extension() {
        // Single-file batch: token goes right before `.parquet`.
        let path = super::parquet_batch_path(b"data/metrics/2026-01-01.parquet", "abc123", None);
        assert_eq!(&path[..], b"data/metrics/2026-01-01-abc123.parquet");
        // Multi-file batch: token then index.
        let path = super::parquet_batch_path(b"data/metrics/2026-01-01.parquet", "abc123", Some(2));
        assert_eq!(&path[..], b"data/metrics/2026-01-01-abc123-2.parquet");
    }

    #[cfg(feature = "codecs-parquet")]
    #[test]
    fn batch_path_without_extension() {
        let path = super::parquet_batch_path(b"data/metrics/file", "tok", Some(0));
        assert_eq!(&path[..], b"data/metrics/file-tok-0");
        let path = super::parquet_batch_path(b"data/metrics/file", "tok", None);
        assert_eq!(&path[..], b"data/metrics/file-tok");
    }

    /// A gauge metric event whose single data point is stamped at
    /// `time_unix_nano` — the value the Parquet codec writes into the
    /// `time_unix_nano` column.
    #[cfg(feature = "codecs-parquet")]
    fn gauge_event_at(time_unix_nano: u64) -> Event {
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric as MetricProto, NumberDataPoint, metric::Data,
            number_data_point::Value as NdpValue,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use sol_lib::event::{OtelMetric, string_value};

        let proto = MetricProto {
            name: "test.gauge".to_string(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano,
                    value: Some(NdpValue::AsInt(1)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };
        let resource = Resource {
            attributes: vec![opentelemetry_proto::tonic::common::v1::KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value("bounds-svc")),
            }],
            ..Default::default()
        };
        Event::Metric(OtelMetric::from_parts(
            proto,
            Some(resource),
            None,
            EventMetadata::default(),
        ))
    }

    /// Task 1b (per-query-file-pruning ADR, option A′): the Parquet batch
    /// file's name carries the batch's exact min/max `time_unix_nano` —
    /// `<min_ns>-<max_ns>-<uuid>.parquet` — so the querier's file inventory
    /// (`crate::querier::inventory`) can parse exact per-file time bounds.
    #[cfg(feature = "codecs-parquet")]
    #[tokio::test]
    async fn test_sink_filename_carries_batch_time_bounds() {
        let directory = temp_dir();
        let yaml = format!(
            r#"
path: "{}/dt=%Y-%m-%d/%H-%M-%S.parquet"
batch_encoding:
  codec: parquet
"#,
            directory.display()
        );
        let config: FileSinkConfig = serde_yaml::from_str(&yaml).unwrap();
        let batch_encoding = config.batch_encoding.clone().expect("parquet batch encoding");
        let mut sink =
            BatchFileSink::new(&config, &batch_encoding, SinkContext::default()).unwrap();

        // Deliberately put the LATEST event first: the dt= directory template
        // still renders from the first event, but the file name's bounds must
        // be the batch's true min/max — not the first event's stamp (the
        // residual first-event-stale risk task 1b eliminates).
        let min_ns: u64 = 1_783_652_400_000_000_000; // 2026-07-10T03:00:00Z
        let max_ns = min_ns + 30_000_000_000; // +30 s
        let mut buffer = vec![gauge_event_at(max_ns), gauge_event_at(min_ns)];
        sink.flush_batch(&mut buffer).await;

        // The dt= directory template is untouched (renders the first event's
        // event time); the basename carries the exact bounds + a uniqueness
        // token.
        let day_dir = directory.join("dt=2026-07-10");
        let names: Vec<String> = std::fs::read_dir(&day_dir)
            .expect("dt= day directory exists")
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 1, "one file per single-signal flush: {names:?}");
        let name = &names[0];
        assert!(
            name.starts_with(&format!("{min_ns}-{max_ns}-")),
            "file name must start with the batch's exact <min_ns>-<max_ns> bounds: {name}"
        );
        assert!(name.ends_with(".parquet"), "{name}");
    }

    #[cfg(feature = "codecs-parquet")]
    #[test]
    fn batch_path_tokens_differ_so_concurrent_writers_never_collide() {
        // Two flushes resolving the same timestamped path must produce distinct
        // files — the property that stops replicated collectors corrupting one
        // shared Parquet file via append-mode concatenation.
        let a = super::parquet_batch_path(
            b"traces/dt=2026-06-10/14-30-15.parquet",
            &uuid::Uuid::new_v4().to_string(),
            None,
        );
        let b = super::parquet_batch_path(
            b"traces/dt=2026-06-10/14-30-15.parquet",
            &uuid::Uuid::new_v4().to_string(),
            None,
        );
        assert_ne!(a, b);
    }
}
