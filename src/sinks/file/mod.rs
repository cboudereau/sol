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
                return Err(
                    "When using batch_encoding, set compression to 'none'. \
                     Parquet uses internal column-level compression."
                        .into(),
                );
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
        use crate::sinks::util::encoding::Encoder as SinkEncoder;

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

        let encoding = (self.transformer.clone(), self.encoder.clone());
        let mut writer: Vec<u8> = Vec::new();
        match encoding.encode_input(events, &mut writer) {
            Ok((byte_size, _)) => {
                let bytes_path = BytesPath::new(path.clone());
                match open_file(bytes_path, false).await {
                    Ok(mut file) => {
                        if let Err(error) = file.write_all(&writer).await {
                            finalizers.update_status(EventStatus::Errored);
                            emit!(FileIoError {
                                error,
                                code: "failed_writing_file",
                                message: "Failed to write batch to file.",
                                path: &path,
                                dropped_events: n_events,
                            });
                            return;
                        }
                        if let Err(error) = file.shutdown().await {
                            emit!(FileIoError {
                                error,
                                code: "failed_closing_file",
                                message: "Failed to close batch file.",
                                path: &path,
                                dropped_events: 0,
                            });
                        }
                        finalizers.update_status(EventStatus::Delivered);
                        self.events_sent.emit(CountByteSize(
                            n_events,
                            JsonSize::new(byte_size),
                        ));
                        emit!(FileBytesSent {
                            byte_size,
                            file: String::from_utf8_lossy(&path),
                            include_file_metric_tag: self.include_file_metric_tag,
                        });
                    }
                    Err(error) => {
                        finalizers.update_status(EventStatus::Errored);
                        emit!(FileIoError {
                            code: "failed_opening_file",
                            message: "Unable to open file for batch.",
                            error,
                            path: &path,
                            dropped_events: n_events,
                        });
                    }
                }
            }
            Err(error) => {
                finalizers.update_status(EventStatus::Errored);
                emit!(FileIoError {
                    error,
                    code: "failed_encoding_batch",
                    message: "Failed to encode batch.",
                    path: &path,
                    dropped_events: n_events,
                });
            }
        }
    }
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
            encoding:
              codec: text
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
            encoding:
              codec: text
            batch_encoding:
              codec: parquet
        "#;
        let config: FileSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.batch_encoding.is_some());
        assert!(config.batch.is_none());
    }
}
