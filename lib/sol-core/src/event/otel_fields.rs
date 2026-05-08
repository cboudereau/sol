// Canonical field names for OTel log records.
pub const BODY: &str = "body";
pub const TIME_UNIX_NANO: &str = "time_unix_nano";
pub const OBSERVED_TIME_UNIX_NANO: &str = "observed_time_unix_nano";
pub const SEVERITY_TEXT: &str = "severity_text";
pub const SEVERITY_NUMBER: &str = "severity_number";
pub const LOG_TRACE_ID: &str = "trace_id";
pub const LOG_SPAN_ID: &str = "span_id";
pub const LOG_FLAGS: &str = "flags";

// Canonical field names for OTel spans.
pub const START_TIME_UNIX_NANO: &str = "start_time_unix_nano";
pub const END_TIME_UNIX_NANO: &str = "end_time_unix_nano";
pub const PARENT_SPAN_ID: &str = "parent_span_id";
pub const SPAN_TRACE_ID: &str = "trace_id";
pub const SPAN_SPAN_ID: &str = "span_id";
pub const SPAN_STATUS: &str = "status";
pub const SPAN_KIND: &str = "kind";
pub const SPAN_EVENTS: &str = "events";
pub const SPAN_LINKS: &str = "links";
pub const SPAN_FLAGS: &str = "flags";

// Common OTel structure fields.
pub const ATTRIBUTES: &str = "attributes";
pub const DROPPED_ATTRIBUTES_COUNT: &str = "dropped_attributes_count";
pub const RESOURCE: &str = "resource";
pub const SCOPE: &str = "scope";
pub const NAME: &str = "name";
pub const VERSION: &str = "version";

// Span status sub-fields.
pub const STATUS_CODE: &str = "code";
pub const STATUS_MESSAGE: &str = "message";

// Metric-specific field names.
pub const DESCRIPTION: &str = "description";
pub const UNIT: &str = "unit";
pub const METRIC_NAMESPACE: &str = "metric.namespace";

// Metric data type names (used in MetricView::as_name and to_log_body).
pub const METRIC_TYPE_SUM: &str = "sum";
pub const METRIC_TYPE_GAUGE: &str = "gauge";
pub const METRIC_TYPE_HISTOGRAM: &str = "histogram";
pub const METRIC_TYPE_SUMMARY: &str = "summary";
pub const METRIC_TYPE_EXPONENTIAL_HISTOGRAM: &str = "exponential_histogram";
pub const METRIC_TYPE_COUNTER: &str = "counter";
pub const METRIC_TYPE_SET: &str = "set";

// Metric kind strings.
pub const METRIC_KIND_INCREMENTAL: &str = "incremental";
pub const METRIC_KIND_ABSOLUTE: &str = "absolute";

// Metric proto field names (camelCase as used in OTLP JSON).
pub const DATA_POINTS: &str = "dataPoints";
pub const AGGREGATION_TEMPORALITY: &str = "aggregationTemporality";
pub const IS_MONOTONIC: &str = "isMonotonic";
pub const BUCKET_COUNTS: &str = "bucketCounts";
pub const EXPLICIT_BOUNDS: &str = "explicitBounds";
pub const COUNT: &str = "count";
pub const SCALE: &str = "scale";
pub const ZERO_COUNT: &str = "zeroCount";
pub const QUANTILE: &str = "quantile";
pub const QUANTILE_VALUES: &str = "quantileValues";
pub const AS_DOUBLE: &str = "asDouble";
pub const AS_INT: &str = "asInt";
pub const POSITIVE: &str = "positive";
pub const NEGATIVE: &str = "negative";
pub const OFFSET: &str = "offset";

// Common field used in KV structures and data point fields.
pub const KEY: &str = "key";
pub const VALUE: &str = "value";
pub const VALUES: &str = "values";

// OTel span time aliases (used for get/insert/remove shortcuts).
pub const START_TIME: &str = "start_time";
pub const END_TIME: &str = "end_time";

// OTLP JSON camelCase field names (serialization format).
pub const TIME_UNIX_NANO_CC: &str = "timeUnixNano";
pub const OBSERVED_TIME_UNIX_NANO_CC: &str = "observedTimeUnixNano";
pub const SEVERITY_TEXT_CC: &str = "severityText";
pub const SEVERITY_NUMBER_CC: &str = "severityNumber";
pub const TRACE_ID_CC: &str = "traceId";
pub const SPAN_ID_CC: &str = "spanId";
pub const PARENT_SPAN_ID_CC: &str = "parentSpanId";
pub const START_TIME_UNIX_NANO_CC: &str = "startTimeUnixNano";
pub const END_TIME_UNIX_NANO_CC: &str = "endTimeUnixNano";
pub const EXPONENTIAL_HISTOGRAM_CC: &str = "exponentialHistogram";

// OTLP JSON AnyValue type wrappers.
pub const STRING_VALUE: &str = "stringValue";
pub const INT_VALUE: &str = "intValue";
pub const DOUBLE_VALUE: &str = "doubleValue";
pub const BOOL_VALUE: &str = "boolValue";
pub const BYTES_VALUE: &str = "bytesValue";
pub const ARRAY_VALUE: &str = "arrayValue";
pub const KVLIST_VALUE: &str = "kvlistValue";

// Resource attribute keys.
pub const SOURCE_TYPE: &str = "source_type";
pub const SERVICE_NAME: &str = "service.name";
pub const HOST_NAME: &str = "host.name";

// Tracing metadata field names.
pub const METADATA: &str = "metadata";
pub const LEVEL: &str = "level";
pub const MODULE_PATH: &str = "module_path";
pub const TARGET: &str = "target";
pub const EVENT: &str = "event";
pub const INGEST_TIMESTAMP: &str = "ingest_timestamp";
