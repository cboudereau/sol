use std::path::PathBuf;

use base64::prelude::{BASE64_STANDARD, Engine as _};
use dnsmsg_parser::dns_message_parser::DnsParserOptions;
use dnstap_parser::{
    parser::DnstapParser,
    schema::{DNSTAP_VALUE_PATHS, DnstapEventSchema},
};
use sol_lib::{
    configurable::configurable_component,
    event::{Event, EventMetadata, OtelLog},
    internal_event::{ByteSize, BytesReceived, InternalEventHandle, Protocol, Registered},
    lookup::{owned_value_path, path},
    tls::MaybeTlsSettings,
};
use vrl::{
    path::OwnedValuePath,
    value::{Kind, ObjectMap, Value},
};

use super::util::framestream::{
    FrameHandler, build_framestream_tcp_source, build_framestream_unix_source,
};
use crate::{
    Result,
    config::{DataType, SourceConfig, SourceContext, SourceOutput},
    internal_events::DnstapParseError,
};

pub mod tcp;
#[cfg(unix)]
pub mod unix;
use sol_lib::lookup::lookup_v2::OptionalValuePath;

/// Configuration for the `dnstap` source.
#[configurable_component(source("dnstap", "Collect DNS logs from a dnstap-compatible server."))]
#[derive(Clone, Debug)]
pub struct DnstapConfig {
    #[serde(flatten)]
    pub mode: Mode,

    /// Maximum DNSTAP frame length that the source accepts.
    ///
    /// If any frame is longer than this, it is discarded.
    #[serde(default = "default_max_frame_length")]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    pub max_frame_length: usize,

    /// Overrides the name of the log field used to add the source path to each event.
    ///
    /// The value is the socket path itself.
    ///
    /// By default, `host` is used.
    pub host_key: Option<OptionalValuePath>,

    /// Whether or not to skip parsing or decoding of DNSTAP frames.
    ///
    /// If set to `true`, frames are not parsed or decoded. The raw frame data is set as a field on the event
    /// (called `rawData`) and encoded as a base64 string.
    pub raw_data_only: Option<bool>,

    /// Whether or not to concurrently process DNSTAP frames.
    pub multithreaded: Option<bool>,

    /// Maximum number of frames that can be processed concurrently.
    pub max_frame_handling_tasks: Option<usize>,

    /// Whether to downcase all DNSTAP hostnames received for consistency
    #[serde(default = "crate::serde::default_false")]
    pub lowercase_hostnames: bool,
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "100 KiB constant fits usize on all supported platforms"
)]
fn default_max_frame_length() -> usize {
    bytesize::kib(100u64) as usize
}

/// Listening mode for the `dnstap` source.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "mode", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The type of dnstap socket to use."))]
#[allow(clippy::large_enum_variant)] // just used for configuration
pub enum Mode {
    /// Listen on TCP.
    Tcp(tcp::TcpConfig),

    /// Listen on a Unix domain socket
    #[cfg(unix)]
    Unix(unix::UnixConfig),
}

impl DnstapConfig {
    pub fn new(socket_path: PathBuf) -> Self {
        Self {
            mode: Mode::Unix(unix::UnixConfig::new(socket_path)),
            ..Default::default()
        }
    }

    fn raw_data_only(&self) -> bool {
        self.raw_data_only.unwrap_or(false)
    }

    pub fn schema_definition(&self) -> sol_lib::schema::Definition {
        let event_schema = DnstapEventSchema;

        let schema = sol_lib::schema::Definition::empty_definition();

        if self.raw_data_only() {
            return schema.with_event_field(
                &owned_value_path!("body"),
                Kind::bytes(),
                Some("message"),
            );
        }
        event_schema.schema_definition(schema)
    }
}

impl Default for DnstapConfig {
    fn default() -> Self {
        Self {
            #[cfg(unix)]
            mode: Mode::Unix(unix::UnixConfig::default()),
            #[cfg(not(unix))]
            mode: Mode::Tcp(tcp::TcpConfig::from_address(std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                9000,
            ))),
            max_frame_length: default_max_frame_length(),
            host_key: None,
            raw_data_only: None,
            multithreaded: None,
            max_frame_handling_tasks: None,
            lowercase_hostnames: false,
        }
    }
}

impl_generate_config_from_default!(DnstapConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "dnstap")]
impl SourceConfig for DnstapConfig {
    async fn build(&self, cx: SourceContext) -> Result<super::Source> {
        let common_frame_handler = CommonFrameHandler::new(self);
        match &self.mode {
            Mode::Tcp(config) => {
                let tls_config = config.tls().as_ref().map(|tls| tls.tls_config.clone());

                let tls = MaybeTlsSettings::from_config(tls_config.as_ref(), true)?;
                let frame_handler =
                    tcp::DnstapFrameHandler::new(config.clone(), tls, common_frame_handler);

                build_framestream_tcp_source(frame_handler, cx.shutdown, cx.out)
            }
            #[cfg(unix)]
            Mode::Unix(config) => {
                let frame_handler =
                    unix::DnstapFrameHandler::new(config.clone(), common_frame_handler);
                build_framestream_unix_source(frame_handler, cx.shutdown, cx.out)
            }
        }
    }

    fn outputs(&self) -> Vec<SourceOutput> {
        let schema_definition = self
            .schema_definition()
            .with_standard_vector_source_metadata();
        vec![SourceOutput::new_maybe_logs(
            DataType::Log,
            schema_definition,
        )]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

#[derive(Clone)]
struct CommonFrameHandler {
    max_frame_length: usize,
    content_type: String,
    raw_data_only: bool,
    multithreaded: bool,
    max_frame_handling_tasks: usize,
    host_key: Option<OwnedValuePath>,
    timestamp_key: Option<OwnedValuePath>,
    source_type_key: Option<OwnedValuePath>,
    bytes_received: Registered<BytesReceived>,
    lowercase_hostnames: bool,
}

impl CommonFrameHandler {
    pub fn new(config: &DnstapConfig) -> Self {
        Self {
            max_frame_length: config.max_frame_length,
            content_type: "protobuf:dnstap.Dnstap".to_string(),
            raw_data_only: config.raw_data_only.unwrap_or(false),
            multithreaded: config.multithreaded.unwrap_or(false),
            max_frame_handling_tasks: config.max_frame_handling_tasks.unwrap_or(1000),
            host_key: None,
            timestamp_key: None,
            source_type_key: None,
            bytes_received: register!(BytesReceived::from(Protocol::from("protobuf"))),
            lowercase_hostnames: config.lowercase_hostnames,
        }
    }
}

impl FrameHandler for CommonFrameHandler {
    fn content_type(&self) -> String {
        self.content_type.clone()
    }

    fn max_frame_length(&self) -> usize {
        self.max_frame_length
    }

    fn handle_event(
        &self,
        received_from: Option<vrl::prelude::Bytes>,
        frame: vrl::prelude::Bytes,
    ) -> Option<sol_lib::event::Event> {
        self.bytes_received.emit(ByteSize(frame.len()));

        let mut value = Value::Object(ObjectMap::new());
        let parse_result = if self.raw_data_only {
            value.insert(&DNSTAP_VALUE_PATHS.raw_data, BASE64_STANDARD.encode(&frame));
            Ok(())
        } else {
            DnstapParser::parse(
                &mut value,
                frame,
                DnsParserOptions {
                    lowercase_hostnames: self.lowercase_hostnames,
                },
            )
        };
        if let Err(err) = parse_result {
            emit!(DnstapParseError {
                error: format!("Dnstap protobuf decode error {err:?}.")
            });
            return None;
        }

        let mut log = OtelLog::from_value_map(value, EventMetadata::default());

        if let Some(host) = received_from {
            log.set_host(host);
        }

        log.metadata_mut()
            .value_mut()
            .insert(path!("vector", "ingest_timestamp"), chrono::Utc::now());

        log.set_source_type(DnstapConfig::NAME);

        Some(Event::Log(log))
    }

    fn multithreaded(&self) -> bool {
        self.multithreaded
    }

    fn max_frame_handling_tasks(&self) -> usize {
        self.max_frame_handling_tasks
    }

    fn host_key(&self) -> &Option<vrl::path::OwnedValuePath> {
        &self.host_key
    }

    fn timestamp_key(&self) -> Option<&vrl::path::OwnedValuePath> {
        self.timestamp_key.as_ref()
    }

    fn source_type_key(&self) -> Option<&vrl::path::OwnedValuePath> {
        self.source_type_key.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use sol_lib::event::{Event, OtelLog};

    use super::*;

    #[test]
    fn simple_matches_schema() {
        let record = r#"{"dataType":"Message",
                         "dataTypeId":1,
                         "messageType":"ClientQuery",
                         "messageTypeId":5,
                         "requestData":{
                           "fullRcode":0,
                           "header":{
                             "aa":false,
                             "ad":true,
                             "anCount":0,
                             "arCount":1,
                             "cd":false,
                             "id":38339,
                             "nsCount":0,
                             "opcode":0,
                             "qdCount":1,
                             "qr":0,
                             "ra":false,
                             "rcode":0,
                             "rd":true,
                             "tc":false},
                           "opt":{
                             "do":false,
                             "ednsVersion":0,
                             "extendedRcode":0,
                             "options":[{"optCode":10,
                                         "optName":"Cookie",
                                         "optValue":"5JiWq4VYa7U="}],
                             "udpPayloadSize":1232},
                           "question":[{"class":"IN","domainName":"whoami.example.org.","questionType":"A","questionTypeId":1}],
                           "rcodeName":"NoError",
                           "time":1667909880863224758,
                           "timePrecision":"ns"},
                         "serverId":"stephenwakely-Precision-5570",
                         "serverVersion":"CoreDNS-1.10.0",
                         "socketFamily":"INET",
                         "socketProtocol":"UDP",
                         "sourceAddress":"0.0.0.0",
                         "sourcePort":54782,
                         "source_type":"dnstap",
                         "time":1667909880863224758,
                         "timePrecision":"ns"
                         }"#;

        let json: serde_json::Value = serde_json::from_str(record).unwrap();
        let mut event = Event::Log(OtelLog::from(vrl::value::Value::from(json)));
        // Set the observed timestamp via OTLP-native API (stored as
        // observed_time_unix_nano in the canonical view, not "timestamp").
        event
            .as_mut_log()
            .set_observed_timestamp(chrono::Utc::now());

        let definition = DnstapEventSchema;
        // Build the schema without with_standard_vector_source_metadata:
        // OtelLog stores source_type as an attribute (already present in the
        // JSON fixture) and the timestamp as observed_time_unix_nano (integer),
        // not the legacy "timestamp" (Kind::timestamp) path.
        let schema = sol_lib::schema::Definition::empty_definition()
            .with_event_field(&owned_value_path!("source_type"), Kind::bytes(), None)
            .with_event_field(
                &owned_value_path!("observed_time_unix_nano"),
                Kind::integer(),
                None,
            );

        definition
            .schema_definition(schema)
            .assert_valid_for_event(&event)
    }
}

#[cfg(all(test, feature = "dnstap-integration-tests"))]
mod integration_tests {
    #![allow(clippy::print_stdout)] // tests

    use bollard::{
        Docker,
        exec::{CreateExecOptions, StartExecOptions},
    };
    use futures::StreamExt;
    use serde_json::json;
    use sol_lib::{event::Event, lookup::lookup_v2::OptionalValuePath};
    use tokio::time;

    use self::unix::UnixConfig;
    use super::*;
    use crate::{
        SourceSender,
        event::Value,
        test_util::{
            components::{SOURCE_TAGS, assert_source_compliance},
            wait_for,
        },
    };

    async fn test_dnstap(raw_data: bool, query_type: &'static str) {
        assert_source_compliance(&SOURCE_TAGS, async {
            let (sender, mut recv) = SourceSender::new_test();

            tokio::spawn(async move {
                let socket = get_socket(raw_data, query_type);

                DnstapConfig {
                    mode: Mode::Unix(UnixConfig {
                        socket_path: socket,
                        socket_file_mode: Some(511),
                        socket_receive_buffer_size: Some(10485760),
                        socket_send_buffer_size: Some(10485760),
                    }),
                    max_frame_length: 102400,
                    host_key: Some(OptionalValuePath::from(owned_value_path!("key"))),
                    raw_data_only: Some(raw_data),
                    multithreaded: Some(false),
                    max_frame_handling_tasks: Some(100000),
                    lowercase_hostnames: false,
                }
                .build(SourceContext::new_test(sender, None))
                .await
                .unwrap()
                .await
                .unwrap()
            });

            send_query(raw_data, query_type);

            let event = time::timeout(time::Duration::from_secs(10), recv.next())
                .await
                .expect("fetch dnstap source event timeout")
                .expect("failed to get dnstap source event from a stream");
            let mut events = vec![event];
            loop {
                match time::timeout(time::Duration::from_secs(1), recv.next()).await {
                    Ok(Some(event)) => events.push(event),
                    Ok(None) => {
                        println!("None: No event");
                        break;
                    }
                    Err(e) => {
                        println!("Error: {e}");
                        break;
                    }
                }
            }

            verify_events(raw_data, query_type, &events);
        })
        .await;
    }

    fn send_query(raw_data: bool, query_type: &'static str) {
        tokio::spawn(async move {
            let socket_path = get_socket(raw_data, query_type);
            let (query_port, control_port) = get_bind_ports(raw_data, query_type);

            // Wait for the source to create its respective socket before telling BIND to reload, causing it to open
            // that new socket file.
            wait_for(move || {
                let path = socket_path.clone();
                async move { path.exists() }
            })
            .await;

            // Now instruct BIND to reopen its DNSTAP socket file and execute the given query.
            reload_bind_dnstap_socket(control_port).await;

            match query_type {
                "query" => {
                    nslookup(query_port).await;
                }
                "update" => {
                    nsupdate().await;
                }
                _ => (),
            }
        });
    }

    fn verify_events(raw_data: bool, query_event: &'static str, events: &[Event]) {
        if raw_data {
            assert_eq!(events.len(), 2);
            assert!(
                events.iter().all(|v| v.as_log().get("rawData").is_some()),
                "No rawData field!"
            );
        } else if query_event == "query" {
            assert_eq!(events.len(), 2);
            assert!(
                events
                    .iter()
                    .any(|v| v.as_log().get("messageType")
                        == Some(Value::Bytes("ClientQuery".into()))),
                "No ClientQuery event!"
            );
            assert!(
                events.iter().any(|v| v.as_log().get("messageType")
                    == Some(Value::Bytes("ClientResponse".into()))),
                "No ClientResponse event!"
            );
        } else if query_event == "update" {
            assert_eq!(events.len(), 4);
            assert!(
                events
                    .iter()
                    .any(|v| v.as_log().get("messageType")
                        == Some(Value::Bytes("UpdateQuery".into()))),
                "No UpdateQuery event!"
            );
            assert!(
                events.iter().any(|v| v.as_log().get("messageType")
                    == Some(Value::Bytes("UpdateResponse".into()))),
                "No UpdateResponse event!"
            );
            assert!(
                events.iter().any(
                    |v| v.as_log().get("messageType") == Some(Value::Bytes("AuthQuery".into()))
                ),
                "No UpdateQuery event!"
            );
            assert!(
                events
                    .iter()
                    .any(|v| v.as_log().get("messageType")
                        == Some(Value::Bytes("AuthResponse".into()))),
                "No UpdateResponse event!"
            );
        }

        for event in events {
            let json = serde_json::to_value(event.as_log().all_event_fields().unwrap()).unwrap();
            match query_event {
                "query" => {
                    if json["messageType"] == json!("ClientQuery") {
                        assert_eq!(
                            json["requestData.question[0].domainName"],
                            json!("h1.example.com.")
                        );
                        assert_eq!(json["requestData.rcodeName"], json!("NoError"));
                    } else if json["messageType"] == json!("ClientResponse") {
                        assert_eq!(
                            json["responseData.answers[0].domainName"],
                            json!("h1.example.com.")
                        );
                        assert_eq!(json["responseData.answers[0].rData"], json!("10.0.0.11"));
                        assert_eq!(json["responseData.rcodeName"], json!("NoError"));
                    }
                }
                "update" => {
                    if json["messageType"] == json!("UpdateQuery") {
                        assert_eq!(
                            json["requestData.update[0].domainName"],
                            json!("dh1.example.com.")
                        );
                        assert_eq!(json["requestData.update[0].rData"], json!("10.0.0.21"));
                        assert_eq!(json["requestData.rcodeName"], json!("NoError"));
                    } else if json["messageType"] == json!("UpdateResponse") {
                        assert_eq!(json["responseData.rcodeName"], json!("NoError"));
                    }
                }
                _ => (),
            }
        }
    }

    fn get_container() -> String {
        std::env::var("CONTAINER_NAME").unwrap_or_else(|_| "vector_dnstap".into())
    }

    fn get_socket(raw_data: bool, query_type: &'static str) -> PathBuf {
        let socket_folder = std::env::var("BIND_SOCKET")
            .map(PathBuf::from)
            .expect("BIND socket directory must be specified via BIND_SOCKET");

        match query_type {
            "query" if raw_data => socket_folder.join("dnstap.sock1"),
            "query" => socket_folder.join("dnstap.sock2"),
            "update" => socket_folder.join("dnstap.sock3"),
            _ => unreachable!("no other test variants should exist"),
        }
    }

    fn get_bind_ports(raw_data: bool, query_type: &'static str) -> (&'static str, &'static str) {
        // Returns the query port and control port, respectively, for the given BIND instance.
        match query_type {
            "query" if raw_data => ("8001", "9001"),
            "query" => ("8002", "9002"),
            "update" => ("8003", "9003"),
            _ => unreachable!("no other test variants should exist"),
        }
    }

    async fn dnstap_exec(cmd: Vec<&str>) {
        let docker = Docker::connect_with_defaults().expect("failed binding to docker socket");
        let config = CreateExecOptions {
            cmd: Some(cmd),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };
        let result = docker
            .create_exec(get_container().as_str(), config)
            .await
            .expect("failed to execute command");
        docker
            .start_exec(&result.id, None::<StartExecOptions>)
            .await
            .expect("failed to execute command");
    }

    async fn reload_bind_dnstap_socket(control_port: &str) {
        dnstap_exec(vec![
            "/usr/sbin/rndc",
            "-p",
            control_port,
            "dnstap",
            "-reopen",
        ])
        .await
    }

    async fn nslookup(port: &str) {
        dnstap_exec(vec![
            "nslookup",
            "-type=A",
            format!("-port={port}").as_str(),
            "h1.example.com",
            "localhost",
        ])
        .await
    }

    async fn nsupdate() {
        dnstap_exec(vec!["nsupdate", "-v", "/bind3/etc/bind/nsupdate.txt"]).await
    }

    #[tokio::test]
    async fn test_dnstap_raw_event() {
        test_dnstap(true, "query").await;
    }

    #[tokio::test]
    async fn test_dnstap_query_event() {
        test_dnstap(false, "query").await;
    }

    #[tokio::test]
    async fn test_dnstap_update_event() {
        test_dnstap(false, "update").await;
    }
}
