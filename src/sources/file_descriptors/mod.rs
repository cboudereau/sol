use std::io;

use async_stream::stream;
use bytes::Bytes;
use chrono::Utc;
use futures::{SinkExt, StreamExt, channel::mpsc, executor};
use sol_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::{
        StreamDecodingError,
        decoding::{DeserializerConfig, FramingConfig},
    },
    configurable::NamedComponent,
    event::{Event, string_value},
    internal_event::{ByteSize, BytesReceived, CountByteSize, InternalEventHandle as _, Protocol},
    lookup::{lookup_v2::OptionalValuePath, owned_value_path},
};
use tokio_util::{codec::FramedRead, io::StreamReader};
use vrl::value::Kind;

use crate::{
    SourceSender,
    codecs::{Decoder, DecodingConfig},
    config::SourceOutput,
    internal_events::{EventsReceived, FileDescriptorReadError, StreamClosedError},
    shutdown::ShutdownSignal,
};

#[cfg(all(unix, feature = "sources-file_descriptor"))]
pub mod file_descriptor;
#[cfg(feature = "sources-stdin")]
pub mod stdin;

pub trait FileDescriptorConfig: NamedComponent {
    fn host_key(&self) -> Option<OptionalValuePath>;
    fn framing(&self) -> Option<FramingConfig>;
    fn decoding(&self) -> DeserializerConfig;
    fn description(&self) -> String;

    fn source<R>(
        &self,
        reader: R,
        shutdown: ShutdownSignal,
        out: SourceSender,
    ) -> crate::Result<crate::sources::Source>
    where
        R: Send + io::BufRead + 'static,
    {
        let hostname = crate::get_hostname().ok();

        let description = self.description();

        let decoding = self.decoding();
        let framing = self
            .framing()
            .unwrap_or_else(|| decoding.default_stream_framing());
        let decoder = DecodingConfig::new(framing, decoding).build()?;

        let (sender, receiver) = mpsc::channel(1024);

        // Spawn background thread with blocking I/O to process fd.
        //
        // This is recommended by Tokio, as otherwise the process will not shut down
        // until another newline is entered. See
        // https://github.com/tokio-rs/tokio/blob/a73428252b08bf1436f12e76287acbc4600ca0e5/tokio/src/io/stdin.rs#L33-L42
        std::thread::spawn(move || {
            info!("Capturing {}.", description);
            read_from_fd(reader, sender);
        });

        Ok(Box::pin(process_stream(
            receiver,
            decoder,
            out,
            shutdown,
            self.get_component_name(),
            hostname,
        )))
    }
}

type Sender = mpsc::Sender<Result<Bytes, io::Error>>;

fn read_from_fd<R>(mut reader: R, mut sender: Sender)
where
    R: Send + io::BufRead + 'static,
{
    loop {
        let (buffer, len) = match reader.fill_buf() {
            Ok([]) => break, // EOF.
            Ok(buffer) => (Ok(Bytes::copy_from_slice(buffer)), buffer.len()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => (Err(error), 0),
        };

        reader.consume(len);

        if executor::block_on(sender.send(buffer)).is_err() {
            // Receiver has closed so we should shutdown.
            break;
        }
    }
}

type Receiver = mpsc::Receiver<Result<Bytes, io::Error>>;

async fn process_stream(
    receiver: Receiver,
    decoder: Decoder,
    mut out: SourceSender,
    shutdown: ShutdownSignal,
    source_type: &'static str,
    hostname: Option<String>,
) -> Result<(), ()> {
    let bytes_received = register!(BytesReceived::from(Protocol::NONE));
    let events_received = register!(EventsReceived);
    let stream = receiver.inspect(|result| {
        if let Err(error) = result {
            emit!(FileDescriptorReadError { error: &error });
        }
    });
    let stream = StreamReader::new(stream);
    let mut stream = FramedRead::new(stream, decoder).take_until(shutdown);
    let mut stream = stream! {
        while let Some(result) = stream.next().await {
            match result {
                Ok((events, byte_size)) => {
                    bytes_received.emit(ByteSize(byte_size));
                    events_received.emit(CountByteSize(
                         events.len(),
                         events.estimated_json_encoded_size_of(),
                    ));

                    let now = Utc::now();

                    for mut event in events {
                        if let Event::Log(ref mut otel_log) = event {
                                otel_log.set_source_metadata_vector_ns(source_type, now);
                            if let Some(hostname) = &hostname {
                                otel_log.set_resource_attribute(
                                    "host.name".to_string(),
                                    string_value(hostname.clone()),
                                );
                            }
                        }
                        yield event;
                    }
                }
                Err(error) => {
                    // Error is logged by `sol_lib::codecs::Decoder`, no
                    // further handling is needed here.
                    if !error.can_continue() {
                        break;
                    }
                }
            }
        }
    }
    .boxed();

    match out.send_event_stream(&mut stream).await {
        Ok(()) => {
            debug!("Finished sending.");
            Ok(())
        }
        Err(_) => {
            let (count, _) = stream.size_hint();
            emit!(StreamClosedError { count });
            Err(())
        }
    }
}

/// Builds the `sol_lib::config::Outputs` for stdin and
/// file_descriptor sources.
fn outputs(decoding: &DeserializerConfig, source_name: &'static str) -> Vec<SourceOutput> {
    let schema_definition = decoding
        .schema_definition()
        .with_source_metadata(
            source_name,
            &owned_value_path!("host"),
            Kind::bytes(),
            Some("host"),
        )
        .with_standard_vector_source_metadata();

    vec![SourceOutput::new_maybe_logs(
        decoding.output_type(),
        schema_definition,
    )]
}
