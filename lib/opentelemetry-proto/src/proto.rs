pub const LOGS_REQUEST_MESSAGE_TYPE: &str =
    "opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest";
pub const TRACES_REQUEST_MESSAGE_TYPE: &str =
    "opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest";
pub const METRICS_REQUEST_MESSAGE_TYPE: &str =
    "opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest";

pub const RESOURCE_LOGS_JSON_FIELD: &str = "resourceLogs";
pub const RESOURCE_METRICS_JSON_FIELD: &str = "resourceMetrics";
pub const RESOURCE_SPANS_JSON_FIELD: &str = "resourceSpans";

include!(concat!(env!("OUT_DIR"), "/opentelemetry-proto.rs"));
