use std::path::PathBuf;

use bytes::Bytes;
use chrono::Utc;
use sol_lib::{
    codecs::decoding::{DeserializerConfig, FramingConfig},
    configurable::configurable_component,
    lookup::{self, lookup_v2::OptionalValuePath},
    shutdown::ShutdownSignal,
};

use super::{SocketConfig, default_host_key};
use crate::{
    SourceSender,
    codecs::Decoder,
    event::Event,
    serde::default_decoding,
    sources::{
        Source,
        util::{build_unix_datagram_source, build_unix_stream_source},
    },
};

/// Unix domain socket configuration for the `socket` source.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct UnixConfig {
    /// The Unix socket path.
    ///
    /// This should be an absolute path.
    #[configurable(metadata(docs::examples = "/path/to/socket"))]
    pub path: PathBuf,

    /// Unix file mode bits to be applied to the unix socket file as its designated file permissions.
    ///
    /// Note: The file mode value can be specified in any numeric format supported by your configuration
    /// language, but it is most intuitive to use an octal number.
    #[configurable(metadata(docs::examples = 0o777))]
    #[configurable(metadata(docs::examples = 0o600))]
    #[configurable(metadata(docs::examples = 508))]
    pub socket_file_mode: Option<u32>,

    /// Overrides the name of the log field used to add the peer host to each event.
    ///
    /// The value will be the peer host's address, including the port i.e. `1.2.3.4:9000`.
    ///
    /// By default, `host` is used.
    ///
    /// Set to `""` to suppress this key.
    #[serde(default = "default_host_key")]
    pub host_key: OptionalValuePath,

    #[configurable(derived)]
    #[serde(default)]
    pub framing: Option<FramingConfig>,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    pub decoding: DeserializerConfig,
}

impl UnixConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            socket_file_mode: None,
            host_key: default_host_key(),
            framing: None,
            decoding: default_decoding(),
        }
    }

    pub const fn decoding(&self) -> &DeserializerConfig {
        &self.decoding
    }

    pub const fn host_key(&self) -> &OptionalValuePath {
        &self.host_key
    }
}

/// Function to pass to `build_unix_*_source`, specific to the basic unix source
/// Takes a single line of a received message and handles an `Event` object.
fn handle_events(
    events: &mut [Event],
    _host_key: &OptionalValuePath,
    received_from: Option<Bytes>,
) {
    let now = Utc::now();

    for event in events {
        if let Event::Log(otel_log) = event {
            otel_log.set_source_metadata_vector_ns(SocketConfig::NAME, now);
            if let Some(ref host) = received_from {
                otel_log.metadata_mut().value_mut().insert(
                    lookup::path!(SocketConfig::NAME, "host"),
                    String::from_utf8_lossy(host).into_owned(),
                );
            }
        }
    }
}

pub(super) fn unix_datagram(
    config: UnixConfig,
    decoder: Decoder,
    shutdown: ShutdownSignal,
    out: SourceSender,
) -> crate::Result<Source> {
    let max_length = config
        .framing
        .and_then(|framing| match framing {
            FramingConfig::CharacterDelimited(config) => config.character_delimited.max_length,
            FramingConfig::NewlineDelimited(config) => config.newline_delimited.max_length,
            FramingConfig::OctetCounting(config) => config.octet_counting.max_length,
            _ => None,
        })
        .unwrap_or_else(crate::serde::default_max_length);

    build_unix_datagram_source(
        config.path,
        config.socket_file_mode,
        max_length,
        decoder,
        move |events, received_from| handle_events(events, &config.host_key, received_from),
        shutdown,
        out,
    )
}

pub(super) fn unix_stream(
    config: UnixConfig,
    decoder: Decoder,
    shutdown: ShutdownSignal,
    out: SourceSender,
) -> crate::Result<Source> {
    build_unix_stream_source(
        config.path,
        config.socket_file_mode,
        decoder,
        move |events, received_from| handle_events(events, &config.host_key, received_from),
        shutdown,
        out,
    )
}
