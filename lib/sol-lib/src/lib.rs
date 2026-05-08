pub use codecs;
pub use enrichment;
#[cfg(feature = "file-source")]
pub use file_source;
#[cfg(feature = "file-source")]
pub use file_source_common;
#[cfg(feature = "api-client")]
pub use sol_api_client as api_client;
pub use sol_buffers as buffers;
#[cfg(feature = "test")]
pub use sol_common::event_test_util;
pub use sol_common::{
    Error, NamedInternalEvent, Result, TimeZone, assert_event_data_eq, atomic, btreemap,
    byte_size_of, byte_size_of::ByteSizeOf, conversion, encode_logfmt, finalization, finalizer, id,
    impl_event_data_eq, internal_event, json_size, registered_event, request_metadata,
    sensitive_string, shutdown, stats, trigger,
};
pub use sol_config as configurable;
pub use sol_config::impl_generate_config_from_default;
#[cfg(feature = "vrl")]
pub use sol_core::compile_vrl;
pub use sol_core::{
    EstimatedJsonEncodedSizeOf, buckets, default_data_dir, emit, event, fanout, ipallowlist,
    latency, metrics, otel_tags, partition, quantiles, register, samples, schema, serde, sink,
    source, source_sender, tcp, tls, transform,
};
pub use sol_lookup as lookup;
pub use sol_stream as stream;
pub use sol_tap as tap;
#[cfg(feature = "sol-top")]
pub use sol_top as top;
#[cfg(feature = "vrl")]
pub use vrl;

pub mod config {
    pub use sol_common::config::ComponentKey;
    pub use sol_core::config::{
        AcknowledgementsConfig, DataType, GlobalOptions, Input, MEMORY_BUFFER_DEFAULT_MAX_EVENTS,
        OutputId, SourceAcknowledgementsConfig, SourceOutput, Tags, Telemetry, TransformOutput,
        WildcardMatching, clone_input_definitions, get_source_metadata, get_vector_metadata,
        init_telemetry, insert_source_metadata, insert_standard_vector_source_metadata,
        insert_vector_metadata, proxy, telemetry,
    };
}

#[cfg(feature = "opentelemetry")]
pub mod opentelemetry {
    pub use sol_opentelemetry_proto::{buffer_codec, common, logs, metrics, proto, spans};
}

#[cfg(feature = "prometheus")]
pub mod prometheus {
    pub use prometheus_parser as parser;
}
