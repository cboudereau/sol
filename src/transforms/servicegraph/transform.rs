use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use metrics::counter;
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    metrics::v1::{
        self as otel_metrics, AggregationTemporality, Metric as OtelMetricProto, metric,
        number_data_point::Value as NDPValue,
    },
    trace::v1::span::SpanKind,
    trace::v1::status::StatusCode as OtelStatusCode,
};
use sol_lib::transform::TaskTransform;

use crate::event::{Event, OtelMetric, OtelSpan, otel_event::string_value};
use crate::expiring_hash_map::ExpiringHashMap;

use super::config::ServiceGraphConfig;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgeKey {
    trace_id: [u8; 16],
    span_id: [u8; 8],
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub client_service: Option<String>,
    pub server_service: Option<String>,
    pub client_latency_sec: Option<f64>,
    pub server_latency_sec: Option<f64>,
    pub failed: bool,
    pub connection_type: String,
    pub client_dimensions: Vec<(String, String)>,
    pub server_dimensions: Vec<(String, String)>,
}

impl Edge {
    const fn new() -> Self {
        Self {
            client_service: None,
            server_service: None,
            client_latency_sec: None,
            server_latency_sec: None,
            failed: false,
            connection_type: String::new(),
            client_dimensions: Vec::new(),
            server_dimensions: Vec::new(),
        }
    }

    pub const fn is_complete(&self) -> bool {
        self.client_service.is_some() && self.server_service.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AggKey {
    client: String,
    server: String,
    connection_type: String,
    dimensions: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct AggBucket {
    request_count: u64,
    failed_count: u64,
    server_duration_sum: f64,
    server_bucket_counts: Vec<u64>,
    client_duration_sum: f64,
    client_bucket_counts: Vec<u64>,
}

impl AggBucket {
    fn new(num_buckets: usize) -> Self {
        Self {
            request_count: 0,
            failed_count: 0,
            server_duration_sum: 0.0,
            server_bucket_counts: vec![0; num_buckets + 1],
            client_duration_sum: 0.0,
            client_bucket_counts: vec![0; num_buckets + 1],
        }
    }
}

pub struct ServiceGraph {
    config: ServiceGraphConfig,
    store: ExpiringHashMap<EdgeKey, Edge>,
    insertion_order: VecDeque<EdgeKey>,
    aggregations: HashMap<AggKey, AggBucket>,
}

impl ServiceGraph {
    pub fn new(config: ServiceGraphConfig) -> Self {
        Self {
            config,
            store: ExpiringHashMap::default(),
            insertion_order: VecDeque::new(),
            aggregations: HashMap::new(),
        }
    }

    fn on_span(&mut self, event: &Event) {
        let otel_span = match event {
            Event::Trace(s) => s,
            _ => return,
        };
        let span = otel_span.span();

        let kind = match SpanKind::try_from(span.kind) {
            Ok(k) => k,
            Err(_) => return,
        };

        match kind {
            SpanKind::Client | SpanKind::Producer => {
                let key = EdgeKey {
                    trace_id: Self::to_trace_id(&span.trace_id),
                    span_id: Self::to_span_id(&span.span_id),
                };
                self.upsert_client(&key, otel_span);
            }
            SpanKind::Server | SpanKind::Consumer => {
                if span.parent_span_id.is_empty() {
                    return;
                }
                let key = EdgeKey {
                    trace_id: Self::to_trace_id(&span.trace_id),
                    span_id: Self::to_span_id(&span.parent_span_id),
                };
                self.upsert_server(&key, otel_span);
            }
            _ => (),
        }
    }

    fn upsert_client(&mut self, key: &EdgeKey, otel_span: &OtelSpan) {
        let span = otel_span.span();
        let service = Self::extract_service_name(otel_span);
        let latency = Self::extract_latency_sec(span);
        let failed = Self::is_failed(span);
        let conn_type = Self::detect_connection_type(otel_span);
        let dims = self.extract_custom_dimensions(otel_span);

        if let Some(edge) = self.store.get_mut(key) {
            edge.client_service = Some(service);
            edge.client_latency_sec = Some(latency);
            edge.connection_type = conn_type;
            edge.client_dimensions = dims;
            if failed {
                edge.failed = true;
            }
            if edge.is_complete() {
                let edge = self.store.remove(key).map(|(e, _)| e);
                if let Some(edge) = edge {
                    self.aggregate_edge(&edge);
                }
            }
        } else {
            let mut edge = Edge::new();
            edge.client_service = Some(service);
            edge.client_latency_sec = Some(latency);
            edge.failed = failed;
            edge.connection_type = conn_type;
            edge.client_dimensions = dims;
            self.insert_edge(key.clone(), edge);
        }
    }

    fn upsert_server(&mut self, key: &EdgeKey, otel_span: &OtelSpan) {
        let span = otel_span.span();
        let service = Self::extract_service_name(otel_span);
        let latency = Self::extract_latency_sec(span);
        let failed = Self::is_failed(span);
        let conn_type = Self::detect_connection_type(otel_span);
        let dims = self.extract_custom_dimensions(otel_span);

        if let Some(edge) = self.store.get_mut(key) {
            edge.server_service = Some(service);
            edge.server_latency_sec = Some(latency);
            if failed {
                edge.failed = true;
            }
            if edge.connection_type.is_empty() {
                edge.connection_type = conn_type;
            }
            edge.server_dimensions = dims;
            if edge.is_complete() {
                let edge = self.store.remove(key).map(|(e, _)| e);
                if let Some(edge) = edge {
                    self.aggregate_edge(&edge);
                }
            }
        } else {
            let mut edge = Edge::new();
            edge.server_service = Some(service);
            edge.server_latency_sec = Some(latency);
            edge.failed = failed;
            edge.connection_type = conn_type;
            edge.server_dimensions = dims;
            self.insert_edge(key.clone(), edge);
        }
    }

    fn insert_edge(&mut self, key: EdgeKey, edge: Edge) {
        let ttl = Duration::from_secs(self.config.store.ttl_secs);

        while self.store.len() >= self.config.store.max_items {
            if let Some(oldest_key) = self.insertion_order.pop_front() {
                if self.store.remove(&oldest_key).is_some() {
                    counter!("sol_servicegraph_dropped_spans_total").increment(1);
                }
            } else {
                break;
            }
        }

        self.insertion_order.push_back(key.clone());
        self.store.insert(key, edge, ttl);
    }

    fn on_expired(&mut self, _edge: Edge) {
        counter!("sol_servicegraph_unpaired_spans_total").increment(1);
    }

    fn aggregate_edge(&mut self, edge: &Edge) {
        counter!("sol_servicegraph_edges_total").increment(1);

        let mut dims = Vec::new();
        for (k, v) in &edge.client_dimensions {
            dims.push((format!("client_{k}"), v.clone()));
        }
        for (k, v) in &edge.server_dimensions {
            dims.push((format!("server_{k}"), v.clone()));
        }
        dims.sort_by(|a, b| a.0.cmp(&b.0));

        let agg_key = AggKey {
            client: edge.client_service.clone().unwrap_or_default(),
            server: edge.server_service.clone().unwrap_or_default(),
            connection_type: edge.connection_type.clone(),
            dimensions: dims,
        };

        let num_buckets = self.config.latency_histogram_buckets.len();
        let bucket = self
            .aggregations
            .entry(agg_key)
            .or_insert_with(|| AggBucket::new(num_buckets));

        bucket.request_count += 1;
        if edge.failed {
            bucket.failed_count += 1;
        }

        if let Some(server_lat) = edge.server_latency_sec {
            bucket.server_duration_sum += server_lat;
            Self::record_histogram(
                &mut bucket.server_bucket_counts,
                &self.config.latency_histogram_buckets,
                server_lat,
            );
        }
        if let Some(client_lat) = edge.client_latency_sec {
            bucket.client_duration_sum += client_lat;
            Self::record_histogram(
                &mut bucket.client_bucket_counts,
                &self.config.latency_histogram_buckets,
                client_lat,
            );
        }
    }

    fn record_histogram(bucket_counts: &mut [u64], bounds: &[f64], value: f64) {
        for (i, &bound) in bounds.iter().enumerate() {
            if value <= bound {
                bucket_counts[i] += 1;
                return;
            }
        }
        if let Some(last) = bucket_counts.last_mut() {
            *last += 1;
        }
    }

    fn flush(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        #[expect(
            clippy::cast_possible_truncation,
            reason = "nanosecond timestamp fits in u64 until year 2554"
        )]
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        for (agg_key, bucket) in self.aggregations.drain() {
            let mut attributes: Vec<KeyValue> = vec![
                KeyValue {
                    key: "client".to_string(),
                    value: Some(string_value(&agg_key.client)),
                },
                KeyValue {
                    key: "server".to_string(),
                    value: Some(string_value(&agg_key.server)),
                },
                KeyValue {
                    key: "connection_type".to_string(),
                    value: Some(string_value(&agg_key.connection_type)),
                },
            ];
            for (k, v) in &agg_key.dimensions {
                attributes.push(KeyValue {
                    key: k.clone(),
                    value: Some(string_value(v)),
                });
            }

            let temporality = AggregationTemporality::Cumulative as i32;

            // traces_service_graph_request (Mimir adds _total for monotonic sums)
            events.push(Event::Metric(OtelMetric::new(OtelMetricProto {
                name: "traces_service_graph_request".to_string(),
                description: "Total count of requests between two nodes".to_string(),
                unit: "{requests}".to_string(),
                metadata: vec![],
                data: Some(metric::Data::Sum(otel_metrics::Sum {
                    data_points: vec![otel_metrics::NumberDataPoint {
                        attributes: attributes.clone(),
                        start_time_unix_nano: 0,
                        time_unix_nano: now_nanos,
                        exemplars: vec![],
                        flags: 0,
                        value: Some(NDPValue::AsInt({
                            #[expect(
                                clippy::cast_possible_wrap,
                                reason = "request count fits in i64 for proto encoding"
                            )]
                            let v = bucket.request_count as i64;
                            v
                        })),
                    }],
                    aggregation_temporality: temporality,
                    is_monotonic: true,
                })),
            })));

            // traces_service_graph_request_failed (Mimir adds _total for monotonic sums)
            events.push(Event::Metric(OtelMetric::new(OtelMetricProto {
                name: "traces_service_graph_request_failed".to_string(),
                description: "Total count of failed requests between two nodes".to_string(),
                unit: "{requests}".to_string(),
                metadata: vec![],
                data: Some(metric::Data::Sum(otel_metrics::Sum {
                    data_points: vec![otel_metrics::NumberDataPoint {
                        attributes: attributes.clone(),
                        start_time_unix_nano: 0,
                        time_unix_nano: now_nanos,
                        exemplars: vec![],
                        flags: 0,
                        value: Some(NDPValue::AsInt({
                            #[expect(
                                clippy::cast_possible_wrap,
                                reason = "failed count fits in i64 for proto encoding"
                            )]
                            let v = bucket.failed_count as i64;
                            v
                        })),
                    }],
                    aggregation_temporality: temporality,
                    is_monotonic: true,
                })),
            })));

            // traces_service_graph_request_server (Mimir adds _seconds from unit "s")
            let server_bounds = self.config.latency_histogram_buckets.clone();
            events.push(Event::Metric(OtelMetric::new(OtelMetricProto {
                name: "traces_service_graph_request_server".to_string(),
                description: "Server-side latency".to_string(),
                unit: "s".to_string(),
                metadata: vec![],
                data: Some(metric::Data::Histogram(otel_metrics::Histogram {
                    data_points: vec![otel_metrics::HistogramDataPoint {
                        attributes: attributes.clone(),
                        start_time_unix_nano: 0,
                        time_unix_nano: now_nanos,
                        count: bucket.request_count,
                        sum: Some(bucket.server_duration_sum),
                        bucket_counts: bucket.server_bucket_counts,
                        explicit_bounds: server_bounds,
                        exemplars: vec![],
                        flags: 0,
                        min: None,
                        max: None,
                    }],
                    aggregation_temporality: temporality,
                })),
            })));

            // traces_service_graph_request_client (Mimir adds _seconds from unit "s")
            let client_bounds = self.config.latency_histogram_buckets.clone();
            events.push(Event::Metric(OtelMetric::new(OtelMetricProto {
                name: "traces_service_graph_request_client".to_string(),
                description: "Client-side latency".to_string(),
                unit: "s".to_string(),
                metadata: vec![],
                data: Some(metric::Data::Histogram(otel_metrics::Histogram {
                    data_points: vec![otel_metrics::HistogramDataPoint {
                        attributes: attributes.clone(),
                        start_time_unix_nano: 0,
                        time_unix_nano: now_nanos,
                        count: bucket.request_count,
                        sum: Some(bucket.client_duration_sum),
                        bucket_counts: bucket.client_bucket_counts,
                        explicit_bounds: client_bounds,
                        exemplars: vec![],
                        flags: 0,
                        min: None,
                        max: None,
                    }],
                    aggregation_temporality: temporality,
                })),
            })));
        }

        events
    }

    fn to_trace_id(bytes: &[u8]) -> [u8; 16] {
        let mut id = [0u8; 16];
        let len = bytes.len().min(16);
        id[..len].copy_from_slice(&bytes[..len]);
        id
    }

    fn to_span_id(bytes: &[u8]) -> [u8; 8] {
        let mut id = [0u8; 8];
        let len = bytes.len().min(8);
        id[..len].copy_from_slice(&bytes[..len]);
        id
    }

    fn extract_service_name(otel_span: &OtelSpan) -> String {
        otel_span
            .resource_attrs()
            .get("service.name")
            .and_then(|av| match &av.value {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                    Some(s.clone())
                }
                _ => None,
            })
            .unwrap_or_default()
    }

    fn extract_latency_sec(span: &opentelemetry_proto::tonic::trace::v1::Span) -> f64 {
        let nanos = span
            .end_time_unix_nano
            .saturating_sub(span.start_time_unix_nano);
        #[expect(
            clippy::cast_precision_loss,
            reason = "nanosecond span duration to seconds; precise for |v| <= 2^53"
        )]
        let nanos_f = nanos as f64;
        nanos_f / 1_000_000_000.0
    }

    fn is_failed(span: &opentelemetry_proto::tonic::trace::v1::Span) -> bool {
        span.status
            .as_ref()
            .map(|s| s.code == OtelStatusCode::Error as i32)
            .unwrap_or(false)
    }

    pub fn detect_connection_type(otel_span: &OtelSpan) -> String {
        if otel_span.attribute("messaging.system").is_some() {
            return "messaging_system".to_string();
        }
        if otel_span.attribute("db.system").is_some() {
            return "database".to_string();
        }
        String::new()
    }

    fn extract_custom_dimensions(&self, otel_span: &OtelSpan) -> Vec<(String, String)> {
        let resource_attrs = otel_span.resource_attrs();
        let mut dims = Vec::new();
        for dim_name in &self.config.dimensions {
            let value = otel_span
                .attribute(dim_name)
                .and_then(|av| match &av.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => Some(s.clone()),
                    _ => None,
                })
                .or_else(|| {
                    resource_attrs.get(dim_name).and_then(|av| match &av.value {
                        Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                s,
                            ),
                        ) => Some(s.clone()),
                        _ => None,
                    })
                })
                .unwrap_or_default();
            dims.push((dim_name.clone(), value));
        }
        dims
    }
}

impl TaskTransform<Event> for ServiceGraph {
    fn transform(
        mut self: Box<Self>,
        input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let mut input = input_rx.fuse();
        let flush_interval = Duration::from_secs(self.config.metrics_flush_interval_secs);

        Box::pin(async_stream::stream! {
            let mut tick = tokio::time::interval(flush_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    maybe_event = input.next() => {
                        match maybe_event {
                            Some(event) => {
                                self.on_span(&event);
                            }
                            None => {
                                for e in self.flush() {
                                    yield e;
                                }
                                break;
                            }
                        }
                    }
                    expired = self.store.next_expired(), if !self.store.is_empty() => {
                        if let Some((edge, _)) = expired {
                            self.on_expired(edge);
                        }
                    }
                    _ = tick.tick() => {
                        for e in self.flush() {
                            yield e;
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventMetadata, OtelSpan};
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, any_value::Value as OtelValueKind},
        resource::v1::Resource,
        trace::v1::{Span, Status, status::StatusCode as OtelStatusCode},
    };

    fn default_config() -> ServiceGraphConfig {
        ServiceGraphConfig {
            store: super::super::config::StoreConfig {
                ttl_secs: 2,
                max_items: 1000,
            },
            metrics_flush_interval_secs: 15,
            latency_histogram_buckets: vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
            dimensions: vec![],
        }
    }

    #[allow(clippy::too_many_arguments, clippy::trivially_copy_pass_by_ref)]
    fn make_span(
        service: &str,
        trace_id: &[u8; 16],
        span_id: &[u8; 8],
        parent_span_id: &[u8],
        kind: i32,
        start_ns: u64,
        end_ns: u64,
        status_code: i32,
        attrs: Vec<KeyValue>,
    ) -> Event {
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value(service)),
            }],
            dropped_attributes_count: 0,
        };
        let span = Span {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            parent_span_id: parent_span_id.to_vec(),
            name: "test-op".to_string(),
            kind,
            start_time_unix_nano: start_ns,
            end_time_unix_nano: end_ns,
            attributes: attrs,
            status: Some(Status {
                message: String::new(),
                code: status_code,
            }),
            trace_state: String::new(),
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            flags: 0,
        };
        Event::Trace(OtelSpan::from_parts(
            span,
            Some(resource),
            None,
            EventMetadata::default(),
        ))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn client_span(service: &str, trace_id: &[u8; 16], span_id: &[u8; 8]) -> Event {
        make_span(
            service,
            trace_id,
            span_id,
            &[],
            SpanKind::Client as i32,
            1_000_000_000,
            1_500_000_000,
            OtelStatusCode::Ok as i32,
            vec![],
        )
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    fn server_span(
        service: &str,
        trace_id: &[u8; 16],
        span_id: &[u8; 8],
        parent_span_id: &[u8; 8],
    ) -> Event {
        make_span(
            service,
            trace_id,
            span_id,
            parent_span_id,
            SpanKind::Server as i32,
            1_000_000_000,
            1_200_000_000,
            OtelStatusCode::Ok as i32,
            vec![],
        )
    }

    // --- Task 1: Config tests ---

    #[test]
    fn test_default_config() {
        let config = default_config();
        assert_eq!(config.store.ttl_secs, 2);
        assert_eq!(config.store.max_items, 1000);
        assert_eq!(config.metrics_flush_interval_secs, 15);
        assert_eq!(config.latency_histogram_buckets.len(), 11);
        assert!(config.dimensions.is_empty());
    }

    #[test]
    fn test_generate_config() {
        use crate::config::GenerateConfig;
        let value = ServiceGraphConfig::generate_config();
        assert!(value.get("metrics_flush_interval_secs").is_some());
        assert!(value.get("store").is_some());
    }

    // --- Task 2: Edge types and span pairing ---

    #[test]
    fn test_edge_key_hash_eq() {
        let k1 = EdgeKey {
            trace_id: [1; 16],
            span_id: [2; 8],
        };
        let k2 = EdgeKey {
            trace_id: [1; 16],
            span_id: [2; 8],
        };
        assert_eq!(k1, k2);

        let mut map = HashMap::new();
        map.insert(k1, "a");
        assert_eq!(map.get(&k2), Some(&"a"));
    }

    #[tokio::test]
    async fn test_client_span_creates_edge() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let span_id = [2u8; 8];

        sg.on_span(&client_span("svc-a", &trace_id, &span_id));

        let key = EdgeKey { trace_id, span_id };
        let edge = sg.store.get(&key).unwrap();
        assert_eq!(edge.client_service.as_deref(), Some("svc-a"));
        assert!(edge.server_service.is_none());
        assert!(!edge.is_complete());
    }

    #[tokio::test]
    async fn test_server_span_completes_edge() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let client_sid = [2u8; 8];
        let server_sid = [3u8; 8];

        sg.on_span(&client_span("svc-a", &trace_id, &client_sid));
        sg.on_span(&server_span("svc-b", &trace_id, &server_sid, &client_sid));

        // Edge should be removed from store (complete) and aggregated.
        let key = EdgeKey {
            trace_id,
            span_id: client_sid,
        };
        assert!(sg.store.get(&key).is_none());
        assert_eq!(sg.aggregations.len(), 1);
    }

    #[tokio::test]
    async fn test_unmatched_server_creates_new_edge() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let server_sid = [3u8; 8];
        let parent_sid = [99u8; 8];

        sg.on_span(&server_span("svc-b", &trace_id, &server_sid, &parent_sid));

        let key = EdgeKey {
            trace_id,
            span_id: parent_sid,
        };
        let edge = sg.store.get(&key).unwrap();
        assert!(edge.client_service.is_none());
        assert_eq!(edge.server_service.as_deref(), Some("svc-b"));
        assert!(!edge.is_complete());
    }

    #[tokio::test]
    async fn test_connection_type_messaging() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let span_id = [2u8; 8];

        let event = make_span(
            "svc-a",
            &trace_id,
            &span_id,
            &[],
            SpanKind::Client as i32,
            0,
            100_000_000,
            OtelStatusCode::Ok as i32,
            vec![KeyValue {
                key: "messaging.system".to_string(),
                value: Some(AnyValue {
                    value: Some(OtelValueKind::StringValue("kafka".to_string())),
                }),
            }],
        );
        sg.on_span(&event);

        let key = EdgeKey { trace_id, span_id };
        assert_eq!(
            sg.store.get(&key).unwrap().connection_type,
            "messaging_system"
        );
    }

    #[tokio::test]
    async fn test_connection_type_database() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let span_id = [2u8; 8];

        let event = make_span(
            "svc-a",
            &trace_id,
            &span_id,
            &[],
            SpanKind::Client as i32,
            0,
            100_000_000,
            OtelStatusCode::Ok as i32,
            vec![KeyValue {
                key: "db.system".to_string(),
                value: Some(AnyValue {
                    value: Some(OtelValueKind::StringValue("postgresql".to_string())),
                }),
            }],
        );
        sg.on_span(&event);

        let key = EdgeKey { trace_id, span_id };
        assert_eq!(sg.store.get(&key).unwrap().connection_type, "database");
    }

    #[tokio::test]
    async fn test_connection_type_default() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let span_id = [2u8; 8];

        sg.on_span(&client_span("svc-a", &trace_id, &span_id));

        let key = EdgeKey { trace_id, span_id };
        assert_eq!(sg.store.get(&key).unwrap().connection_type, "");
    }

    #[tokio::test]
    async fn test_failed_detection() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let span_id = [2u8; 8];

        let event = make_span(
            "svc-a",
            &trace_id,
            &span_id,
            &[],
            SpanKind::Client as i32,
            0,
            100_000_000,
            OtelStatusCode::Error as i32,
            vec![],
        );
        sg.on_span(&event);

        let key = EdgeKey { trace_id, span_id };
        assert!(sg.store.get(&key).unwrap().failed);
    }

    #[test]
    fn test_edge_is_complete() {
        let mut edge = Edge::new();
        assert!(!edge.is_complete());

        edge.client_service = Some("a".to_string());
        assert!(!edge.is_complete());

        edge.server_service = Some("b".to_string());
        assert!(edge.is_complete());
    }

    // --- Task 3: Store management ---

    #[tokio::test]
    async fn test_capacity_eviction() {
        let config = ServiceGraphConfig {
            store: super::super::config::StoreConfig {
                ttl_secs: 9999,
                max_items: 2,
            },
            ..default_config()
        };
        let mut sg = ServiceGraph::new(config);

        sg.on_span(&client_span("svc-a", &[1; 16], &[1; 8]));
        sg.on_span(&client_span("svc-b", &[2; 16], &[2; 8]));
        assert_eq!(sg.store.len(), 2);

        sg.on_span(&client_span("svc-c", &[3; 16], &[3; 8]));
        assert_eq!(sg.store.len(), 2);

        // Oldest (trace [1]) should be evicted.
        let key1 = EdgeKey {
            trace_id: [1; 16],
            span_id: [1; 8],
        };
        assert!(sg.store.get(&key1).is_none());
    }

    #[tokio::test]
    async fn test_expired_edge_does_not_emit_metrics() {
        let mut sg = ServiceGraph::new(default_config());
        let edge = Edge::new();

        sg.on_expired(edge);
        let events = sg.flush();
        assert!(events.is_empty());
    }

    // --- Task 4: Metric emission ---

    #[tokio::test]
    async fn test_flush_emits_four_metrics_per_edge() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let client_sid = [2u8; 8];
        let server_sid = [3u8; 8];

        sg.on_span(&client_span("svc-a", &trace_id, &client_sid));
        sg.on_span(&server_span("svc-b", &trace_id, &server_sid, &client_sid));

        let events = sg.flush();
        assert_eq!(events.len(), 4);
    }

    #[tokio::test]
    async fn test_flush_metric_names() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let client_sid = [2u8; 8];
        let server_sid = [3u8; 8];

        sg.on_span(&client_span("svc-a", &trace_id, &client_sid));
        sg.on_span(&server_span("svc-b", &trace_id, &server_sid, &client_sid));

        let events = sg.flush();
        let names: Vec<&str> = events.iter().map(|e| e.as_metric().name()).collect();
        assert_eq!(
            names,
            vec![
                "traces_service_graph_request",
                "traces_service_graph_request_failed",
                "traces_service_graph_request_server",
                "traces_service_graph_request_client",
            ]
        );
    }

    #[tokio::test]
    async fn test_flush_dimensions() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let client_sid = [2u8; 8];
        let server_sid = [3u8; 8];

        sg.on_span(&client_span("svc-a", &trace_id, &client_sid));
        sg.on_span(&server_span("svc-b", &trace_id, &server_sid, &client_sid));

        let events = sg.flush();
        // Check first metric (request_total) for dimensions.
        let metric = events[0].as_metric();
        let proto = metric.metric_proto();
        if let Some(metric::Data::Sum(sum)) = &proto.data {
            let attrs = &sum.data_points[0].attributes;
            let find = |key: &str| attrs.iter().find(|kv| kv.key == key);
            assert!(find("client").is_some());
            assert!(find("server").is_some());
            assert!(find("connection_type").is_some());
        } else {
            panic!("expected Sum");
        }
    }

    #[tokio::test]
    async fn test_flush_custom_dimensions_prefixed() {
        let config = ServiceGraphConfig {
            dimensions: vec!["http.method".to_string()],
            ..default_config()
        };
        let mut sg = ServiceGraph::new(config);
        let trace_id = [1u8; 16];
        let client_sid = [2u8; 8];
        let server_sid = [3u8; 8];

        let client_event = make_span(
            "svc-a",
            &trace_id,
            &client_sid,
            &[],
            SpanKind::Client as i32,
            1_000_000_000,
            1_500_000_000,
            OtelStatusCode::Ok as i32,
            vec![KeyValue {
                key: "http.method".to_string(),
                value: Some(AnyValue {
                    value: Some(OtelValueKind::StringValue("GET".to_string())),
                }),
            }],
        );
        let server_event = make_span(
            "svc-b",
            &trace_id,
            &server_sid,
            client_sid.as_slice(),
            SpanKind::Server as i32,
            1_000_000_000,
            1_200_000_000,
            OtelStatusCode::Ok as i32,
            vec![KeyValue {
                key: "http.method".to_string(),
                value: Some(AnyValue {
                    value: Some(OtelValueKind::StringValue("GET".to_string())),
                }),
            }],
        );

        sg.on_span(&client_event);
        sg.on_span(&server_event);

        let events = sg.flush();
        let metric = events[0].as_metric();
        let proto = metric.metric_proto();
        if let Some(metric::Data::Sum(sum)) = &proto.data {
            let attrs = &sum.data_points[0].attributes;
            let find = |key: &str| attrs.iter().find(|kv| kv.key == key);
            assert!(find("client_http.method").is_some(), "client-prefixed dim");
            assert!(find("server_http.method").is_some(), "server-prefixed dim");
        } else {
            panic!("expected Sum");
        }
    }

    #[tokio::test]
    async fn test_flush_failed_count() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let client_sid = [2u8; 8];
        let server_sid = [3u8; 8];

        let client_event = make_span(
            "svc-a",
            &trace_id,
            &client_sid,
            &[],
            SpanKind::Client as i32,
            0,
            100_000_000,
            OtelStatusCode::Error as i32,
            vec![],
        );
        sg.on_span(&client_event);
        sg.on_span(&server_span("svc-b", &trace_id, &server_sid, &client_sid));

        let events = sg.flush();
        // Second metric is request_failed_total.
        let metric = events[1].as_metric();
        let proto = metric.metric_proto();
        if let Some(metric::Data::Sum(sum)) = &proto.data {
            assert_eq!(sum.data_points[0].value, Some(NDPValue::AsInt(1)));
        } else {
            panic!("expected Sum");
        }
    }

    #[tokio::test]
    async fn test_flush_resets() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let client_sid = [2u8; 8];
        let server_sid = [3u8; 8];

        sg.on_span(&client_span("svc-a", &trace_id, &client_sid));
        sg.on_span(&server_span("svc-b", &trace_id, &server_sid, &client_sid));

        let events1 = sg.flush();
        assert_eq!(events1.len(), 4);

        let events2 = sg.flush();
        assert!(events2.is_empty());
    }

    #[tokio::test]
    async fn test_flush_temporality_is_cumulative() {
        let mut sg = ServiceGraph::new(default_config());
        sg.on_span(&client_span("svc-a", &[1; 16], &[2; 8]));
        sg.on_span(&server_span("svc-b", &[1; 16], &[3; 8], &[2; 8]));

        let events = sg.flush();
        for event in &events {
            let proto = event.as_metric().metric_proto();
            match &proto.data {
                Some(metric::Data::Sum(sum)) => {
                    assert_eq!(
                        sum.aggregation_temporality,
                        AggregationTemporality::Cumulative as i32,
                        "{}",
                        proto.name
                    );
                }
                Some(metric::Data::Histogram(h)) => {
                    assert_eq!(
                        h.aggregation_temporality,
                        AggregationTemporality::Cumulative as i32,
                        "{}",
                        proto.name
                    );
                }
                _ => panic!("unexpected metric data type"),
            }
        }
    }

    // --- Task 5: Integration ---

    #[tokio::test]
    async fn test_multiple_edges_aggregate() {
        let mut sg = ServiceGraph::new(default_config());

        // Two pairs with same (svc-a → svc-b).
        sg.on_span(&client_span("svc-a", &[1; 16], &[1; 8]));
        sg.on_span(&server_span("svc-b", &[1; 16], &[11; 8], &[1; 8]));

        sg.on_span(&client_span("svc-a", &[2; 16], &[2; 8]));
        sg.on_span(&server_span("svc-b", &[2; 16], &[12; 8], &[2; 8]));

        let events = sg.flush();
        assert_eq!(
            events.len(),
            4,
            "same edge key → aggregated into one set of 4 metrics"
        );

        // request_total should be 2.
        let metric = events[0].as_metric();
        let proto = metric.metric_proto();
        if let Some(metric::Data::Sum(sum)) = &proto.data {
            assert_eq!(sum.data_points[0].value, Some(NDPValue::AsInt(2)));
        } else {
            panic!("expected Sum");
        }
    }

    #[tokio::test]
    async fn test_different_services_separate() {
        let mut sg = ServiceGraph::new(default_config());

        sg.on_span(&client_span("svc-a", &[1; 16], &[1; 8]));
        sg.on_span(&server_span("svc-b", &[1; 16], &[11; 8], &[1; 8]));

        sg.on_span(&client_span("svc-x", &[2; 16], &[2; 8]));
        sg.on_span(&server_span("svc-y", &[2; 16], &[12; 8], &[2; 8]));

        let events = sg.flush();
        assert_eq!(events.len(), 8, "two different edge keys → 2 × 4 metrics");
    }

    #[tokio::test]
    async fn test_internal_spans_ignored() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let span_id = [2u8; 8];

        let event = make_span(
            "svc-a",
            &trace_id,
            &span_id,
            &[],
            SpanKind::Internal as i32,
            0,
            100_000_000,
            OtelStatusCode::Ok as i32,
            vec![],
        );
        sg.on_span(&event);

        assert!(sg.store.is_empty());
        assert!(sg.aggregations.is_empty());
    }

    #[tokio::test]
    async fn test_server_span_without_parent_ignored() {
        let mut sg = ServiceGraph::new(default_config());
        let trace_id = [1u8; 16];
        let span_id = [2u8; 8];

        let event = make_span(
            "svc-a",
            &trace_id,
            &span_id,
            &[],
            SpanKind::Server as i32,
            0,
            100_000_000,
            OtelStatusCode::Ok as i32,
            vec![],
        );
        sg.on_span(&event);

        assert!(sg.store.is_empty());
    }

    #[tokio::test]
    async fn test_flush_histogram_buckets() {
        let config = ServiceGraphConfig {
            latency_histogram_buckets: vec![0.1, 0.5, 1.0],
            ..default_config()
        };
        let mut sg = ServiceGraph::new(config);
        let trace_id = [1u8; 16];
        let client_sid = [2u8; 8];
        let server_sid = [3u8; 8];

        // Client: 500ms, Server: 200ms
        sg.on_span(&client_span("svc-a", &trace_id, &client_sid));
        sg.on_span(&server_span("svc-b", &trace_id, &server_sid, &client_sid));

        let events = sg.flush();

        // Server histogram (index 2)
        let server_hist = events[2].as_metric();
        let server_proto = server_hist.metric_proto();
        if let Some(metric::Data::Histogram(h)) = &server_proto.data {
            let dp = &h.data_points[0];
            assert_eq!(dp.explicit_bounds, vec![0.1, 0.5, 1.0]);
            // 200ms = 0.2s → falls in bucket [0.1, 0.5)
            assert_eq!(dp.bucket_counts, vec![0, 1, 0, 0]);
        } else {
            panic!("expected Histogram");
        }

        // Client histogram (index 3)
        let client_hist = events[3].as_metric();
        let client_proto = client_hist.metric_proto();
        if let Some(metric::Data::Histogram(h)) = &client_proto.data {
            let dp = &h.data_points[0];
            // 500ms = 0.5s → falls in bucket [0.1, 0.5] (≤ 0.5)
            assert_eq!(dp.bucket_counts, vec![0, 1, 0, 0]);
        } else {
            panic!("expected Histogram");
        }
    }
}
