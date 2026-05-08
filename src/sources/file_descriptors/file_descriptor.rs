use std::{
    fs::File,
    io,
    os::fd::{FromRawFd as _, IntoRawFd as _, RawFd},
};

use sol_lib::{
    codecs::decoding::{DeserializerConfig, FramingConfig},
    configurable::configurable_component,
    lookup::lookup_v2::OptionalValuePath,
};

use super::{FileDescriptorConfig, outputs};
use crate::{
    config::{GenerateConfig, Resource, SourceConfig, SourceContext, SourceOutput},
    serde::default_decoding,
};
/// Configuration for the `file_descriptor` source.
#[configurable_component(source("file_descriptor", "Collect logs from a file descriptor."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileDescriptorSourceConfig {
    /// The maximum buffer size, in bytes, of incoming messages.
    ///
    /// Messages larger than this are truncated.
    #[serde(default = "crate::serde::default_max_length")]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    pub max_length: usize,

    /// Overrides the name of the log field used to add the current hostname to each event.
    ///
    ///
    /// By default, `host` is used.
    pub host_key: Option<OptionalValuePath>,

    #[configurable(derived)]
    pub framing: Option<FramingConfig>,

    #[configurable(derived)]
    #[serde(default = "default_decoding")]
    pub decoding: DeserializerConfig,

    /// The file descriptor number to read from.
    #[configurable(metadata(docs::examples = 10))]
    #[configurable(metadata(docs::human_name = "File Descriptor Number"))]
    pub fd: u32,
}

impl FileDescriptorConfig for FileDescriptorSourceConfig {
    fn host_key(&self) -> Option<OptionalValuePath> {
        self.host_key.clone()
    }

    fn framing(&self) -> Option<FramingConfig> {
        self.framing.clone()
    }

    fn decoding(&self) -> DeserializerConfig {
        self.decoding.clone()
    }

    fn description(&self) -> String {
        format!("file descriptor {}", self.fd)
    }
}

impl GenerateConfig for FileDescriptorSourceConfig {
    fn generate_config() -> toml::Value {
        let fd = null_fd().unwrap();
        toml::from_str(&format!(
            r#"
            fd = {fd}
            "#
        ))
        .unwrap()
    }
}

pub(crate) fn null_fd() -> crate::Result<RawFd> {
    #[cfg(unix)]
    const FILENAME: &str = "/dev/null";
    #[cfg(windows)]
    const FILENAME: &str = "C:\\NUL";
    File::open(FILENAME)
        .map_err(|error| format!("Could not open dummy file at {FILENAME:?}: {error}").into())
        .map(|file| file.into_raw_fd())
}

#[async_trait::async_trait]
#[typetag::serde(name = "file_descriptor")]
impl SourceConfig for FileDescriptorSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<crate::sources::Source> {
        #[expect(
            clippy::cast_possible_wrap,
            reason = "file descriptor number fits i32 (Unix FDs are non-negative ints)"
        )]
        let raw_fd = self.fd as i32;
        let pipe = io::BufReader::new(unsafe { File::from_raw_fd(raw_fd) });

        self.source(pipe, cx.shutdown, cx.out)
    }

    fn outputs(&self) -> Vec<SourceOutput> {
        outputs(&self.decoding, Self::NAME)
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::Fd(self.fd)]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use nix::unistd::{close, pipe, write};
    use sol_lib::lookup::path;
    use vrl::value;

    use super::*;
    use crate::{
        SourceSender,
        test_util::components::{
            COMPONENT_ERROR_TAGS, SOURCE_TAGS, assert_source_compliance, assert_source_error,
        },
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<FileDescriptorSourceConfig>();
    }

    #[tokio::test]
    #[allow(clippy::cast_sign_loss)]
    async fn file_descriptor_decodes_line() {
        assert_source_compliance(&SOURCE_TAGS, async {
            let (tx, rx) = SourceSender::new_test();
            let (read_fd, write_fd) = pipe().unwrap();
            let config = FileDescriptorSourceConfig {
                max_length: crate::serde::default_max_length(),
                host_key: Default::default(),
                framing: None,
                decoding: default_decoding(),
                fd: read_fd as u32,
            };

            let mut stream = rx;

            write(write_fd, b"hello world\nhello world again\n").unwrap();
            close(write_fd).unwrap();

            let context = SourceContext::new_test(tx, None);
            config.build(context).await.unwrap().await.unwrap();

            let event = stream.next().await;
            assert_eq!(
                Some("hello world".into()),
                event.map(|event| event
                    .as_log()
                    .get("body")
                    .unwrap()
                    .to_string_lossy()
                    .into_owned())
            );

            let event = stream.next().await;
            assert_eq!(
                Some("hello world again".into()),
                event.map(|event| event
                    .as_log()
                    .get("body")
                    .unwrap()
                    .to_string_lossy()
                    .into_owned())
            );

            let event = stream.next().await;
            assert!(event.is_none());
        })
        .await;
    }

    #[tokio::test]
    #[allow(clippy::cast_sign_loss)]
    async fn file_descriptor_decodes_line_vector_namespace() {
        assert_source_compliance(&SOURCE_TAGS, async {
            let (tx, rx) = SourceSender::new_test();
            let (read_fd, write_fd) = pipe().unwrap();
            let config = FileDescriptorSourceConfig {
                max_length: crate::serde::default_max_length(),
                host_key: Default::default(),
                framing: None,
                decoding: default_decoding(),
                fd: read_fd as u32,
            };

            let mut stream = rx;

            write(write_fd, b"hello world\nhello world again\n").unwrap();
            close(write_fd).unwrap();

            let context = SourceContext::new_test(tx, None);
            config.build(context).await.unwrap().await.unwrap();

            let event = stream.next().await;
            let event = event.unwrap();
            let log = event.as_log();
            let meta = log.metadata().value();

            assert_eq!(value!("hello world"), log.value());
            assert_eq!(
                meta.get(path!("vector", "source_type")).unwrap(),
                &value!("file_descriptor")
            );
            assert!(
                meta.get(path!("vector", "ingest_timestamp"))
                    .unwrap()
                    .is_timestamp()
            );

            let event = stream.next().await;
            let event = event.unwrap();
            let log = event.as_log();

            assert_eq!(value!("hello world again"), log.value());

            let event = stream.next().await;
            assert!(event.is_none());
        })
        .await;
    }

    #[tokio::test]
    #[allow(clippy::cast_sign_loss)]
    async fn file_descriptor_handles_invalid_fd() {
        assert_source_error(&COMPONENT_ERROR_TAGS, async {
            let (tx, rx) = SourceSender::new_test();
            let (_read_fd, write_fd) = pipe().unwrap();
            let config = FileDescriptorSourceConfig {
                max_length: crate::serde::default_max_length(),
                host_key: Default::default(),
                framing: None,
                decoding: default_decoding(),
                fd: write_fd as u32, // intentionally giving the source a write-only fd
            };

            let mut stream = rx;

            write(write_fd, b"hello world\nhello world again\n").unwrap();

            let context = SourceContext::new_test(tx, None);
            config.build(context).await.unwrap().await.unwrap();

            // The error "Bad file descriptor" will be logged when the source attempts to read
            // for the first time.
            let event = stream.next().await;
            assert!(event.is_none());
        })
        .await;
    }
}
