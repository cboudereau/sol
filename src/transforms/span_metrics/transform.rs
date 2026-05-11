use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    metrics::v1::{
        self as otel_metrics, AggregationTemporality, Metric as OtelMetricProto, metric,
        number_data_point::Value as NDPValue,
    },
};
use sol_lib::transform::TaskTransform;

use crate::event::{Event, OtelMetric, otel_event::string_value};

use super::config::{SpanMetricsConfig, Temporality};

/// Default dimension keys extracted from every span.
const DEFAULT_DIMENSIONS: &[&str] = &["service.name", "span.name", "span.kind", "status.code"];

/// Aggregation bucket for one unique dimension set.
#[derive(Debug, Clone)]
struct AggBucket {
    calls: u64,
    duration_sum: f64,
    bucket_counts: Vec<u64>,
}

impl AggBucket {
    fn new(num_buckets: usize) -> Self {
        Self {
            calls: 0,
            duration_sum: 0.0,
            bucket_counts: vec![0; num_buckets + 1], // +1 for overflow
        }
    }
}

/// Sorted dimension key-value pairs used as aggregation map key.
type DimensionKey = Vec<(String, String)>;

pub struct SpanMetrics {
    config: SpanMetricsConfig,
    /// Aggregation map: dimension key → bucket.
    aggregations: HashMap<DimensionKey, AggBucket>,
}

impl SpanMetrics {
    pub fn new(config: SpanMetricsConfig) -> Self {
        Self {
            config,
            aggregations: HashMap::new(),
        }
    }

    /// Process a single span event.
    fn on_span(&mut self, event: &Event) {
        let otel_span = match event {
            Event::Trace(s) => s,
            _ => return,
        };
        let span = otel_span.span();

        // Extract duration in the configured unit.
        let duration_nanos = span
            .end_time_unix_nano
            .saturating_sub(span.start_time_unix_nano);
        #[expect(
            clippy::cast_precision_loss,
            reason = "nanosecond span duration to time unit; precise for |v| <= 2^53"
        )]
        let duration_nanos_f = duration_nanos as f64;
        let duration = match self.config.histogram.unit.as_str() {
            "ms" => duration_nanos_f / 1_000_000.0,
            _ => duration_nanos_f / 1_000_000_000.0, // seconds
        };

        // Build dimension key.
        let dims = self.extract_dimensions(otel_span);

        // Get or create aggregation bucket.
        let num_buckets = self.config.histogram.buckets.len();
        let bucket = self
            .aggregations
            .entry(dims)
            .or_insert_with(|| AggBucket::new(num_buckets));
        bucket.calls += 1;
        bucket.duration_sum += duration;

        // Record into histogram buckets.
        let mut placed = false;
        for (i, &bound) in self.config.histogram.buckets.iter().enumerate() {
            if duration <= bound {
                bucket.bucket_counts[i] += 1;
                placed = true;
                break;
            }
        }
        if !placed {
            // Overflow bucket.
            bucket.bucket_counts[num_buckets] += 1;
        }
    }

    /// Extract dimension values from an OtelSpan.
    fn extract_dimensions(&self, otel_span: &crate::event::OtelSpan) -> DimensionKey {
        use opentelemetry_proto::tonic::trace::v1::span::SpanKind;

        let span = otel_span.span();
        let resource_attrs = otel_span.resource_attrs();
        let mut dims = Vec::new();

        let excluded = &self.config.exclude_dimensions;

        // Default dimensions.
        for &dim_name in DEFAULT_DIMENSIONS {
            if excluded.iter().any(|e| e == dim_name) {
                continue;
            }
            let value = match dim_name {
                "service.name" => resource_attrs
                    .get("service.name")
                    .and_then(|av| match &av.value {
                        Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                s,
                            ),
                        ) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default(),
                "span.name" => span.name.clone(),
                "span.kind" => match SpanKind::try_from(span.kind) {
                    Ok(SpanKind::Client) => "SPAN_KIND_CLIENT".to_string(),
                    Ok(SpanKind::Server) => "SPAN_KIND_SERVER".to_string(),
                    Ok(SpanKind::Producer) => "SPAN_KIND_PRODUCER".to_string(),
                    Ok(SpanKind::Consumer) => "SPAN_KIND_CONSUMER".to_string(),
                    Ok(SpanKind::Internal) => "SPAN_KIND_INTERNAL".to_string(),
                    _ => "SPAN_KIND_UNSPECIFIED".to_string(),
                },
                "status.code" => span
                    .status
                    .as_ref()
                    .map(|s| match s.code {
                        1 => "OK".to_string(),
                        2 => "ERROR".to_string(),
                        _ => "UNSET".to_string(),
                    })
                    .unwrap_or_else(|| "UNSET".to_string()),
                _ => String::new(),
            };
            dims.push((dim_name.to_string(), value));
        }

        // User-configured extra dimensions.
        for dim_config in &self.config.dimensions {
            if excluded.iter().any(|e| e == &dim_config.name) {
                continue;
            }
            let value = otel_span.attribute(&dim_config.name)
                .and_then(|av| match &av.value {
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                })
                .or_else(|| {
                    resource_attrs.get(&dim_config.name)
                        .and_then(|av| match &av.value {
                            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => Some(s.clone()),
                            _ => None,
                        })
                })
                .or_else(|| dim_config.default.clone())
                .unwrap_or_default();
            dims.push((dim_config.name.clone(), value));
        }

        dims.sort_by(|a, b| a.0.cmp(&b.0));
        dims
    }

    /// Flush aggregated metrics as OtelMetric events.
    fn flush(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        let temporality = match self.config.aggregation_temporality {
            Temporality::Cumulative => AggregationTemporality::Cumulative as i32,
            Temporality::Delta => AggregationTemporality::Delta as i32,
        };
        #[expect(
            clippy::cast_possible_truncation,
            reason = "nanosecond timestamp fits in u64 until year 2554"
        )]
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        for (dims, bucket) in self.aggregations.drain() {
            let attributes: Vec<KeyValue> = dims
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.clone(),
                    value: Some(string_value(v)),
                })
                .collect();

            // --- calls metric (Sum) ---
            let calls_metric = OtelMetricProto {
                name: format!("{}.calls", self.config.namespace),
                description: "Number of spans".to_string(),
                unit: "{spans}".to_string(),
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
                                reason = "call count fits in i64 for proto encoding"
                            )]
                            let v = bucket.calls as i64;
                            v
                        })),
                    }],
                    aggregation_temporality: temporality,
                    is_monotonic: true,
                })),
            };
            events.push(Event::Metric(OtelMetric::new(calls_metric)));

            // --- duration metric (Histogram) ---
            let explicit_bounds = self.config.histogram.buckets.clone();
            let duration_metric = OtelMetricProto {
                name: format!("{}.duration", self.config.namespace),
                description: "Span duration".to_string(),
                unit: self.config.histogram.unit.clone(),
                metadata: vec![],
                data: Some(metric::Data::Histogram(otel_metrics::Histogram {
                    data_points: vec![otel_metrics::HistogramDataPoint {
                        attributes,
                        start_time_unix_nano: 0,
                        time_unix_nano: now_nanos,
                        count: bucket.calls,
                        sum: Some(bucket.duration_sum),
                        bucket_counts: bucket.bucket_counts,
                        explicit_bounds,
                        exemplars: vec![],
                        flags: 0,
                        min: None,
                        max: None,
                    }],
                    aggregation_temporality: temporality,
                })),
            };
            events.push(Event::Metric(OtelMetric::new(duration_metric)));
        }

        events
    }
}

impl TaskTransform<Event> for SpanMetrics {
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
                                // Flush remaining on shutdown.
                                for e in self.flush() {
                                    yield e;
                                }
                                break;
                            }
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
    use super::super::config::DimensionConfig;
    use super::*;
    use crate::event::{EventMetadata, OtelSpan};
    use opentelemetry_proto::tonic::{
        resource::v1::Resource,
        trace::v1::{Span, Status, status::StatusCode as OtelStatusCode},
    };

    fn make_test_span(
        service: &str,
        name: &str,
        status_code: i32,
        start_ns: u64,
        end_ns: u64,
        extra_attrs: Vec<KeyValue>,
    ) -> Event {
        let resource = Resource {
            attributes: vec![KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value(service)),
            }],
            dropped_attributes_count: 0,
        };
        let attributes = extra_attrs;
        let span = Span {
            trace_id: vec![0; 16],
            span_id: vec![0; 8],
            parent_span_id: vec![],
            name: name.to_string(),
            kind: 2, // SERVER
            start_time_unix_nano: start_ns,
            end_time_unix_nano: end_ns,
            attributes,
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

    fn default_config() -> SpanMetricsConfig {
        SpanMetricsConfig {
            namespace: "test.span.metrics".to_string(),
            aggregation_temporality: Temporality::Cumulative,
            metrics_flush_interval_secs: 60,
            histogram: super::super::config::HistogramConfig {
                unit: "s".to_string(),
                buckets: vec![0.1, 0.5, 1.0, 5.0],
            },
            dimensions: vec![],
            exclude_dimensions: vec![],
        }
    }

    #[test]
    fn single_span_produces_calls_and_duration() {
        let mut sm = SpanMetrics::new(default_config());
        let span = make_test_span(
            "my-svc",
            "GET /api",
            OtelStatusCode::Ok as i32,
            1_000_000_000,
            1_500_000_000,
            vec![],
        ); // 500ms

        sm.on_span(&span);
        let events = sm.flush();

        assert_eq!(events.len(), 2, "should emit calls + duration");

        // Check calls metric.
        let calls = events[0].as_metric();
        assert_eq!(calls.name(), "test.span.metrics.calls");

        // Check duration metric.
        let duration = events[1].as_metric();
        assert_eq!(duration.name(), "test.span.metrics.duration");
    }

    #[test]
    fn multiple_spans_aggregate() {
        let mut sm = SpanMetrics::new(default_config());

        // Two spans with same dimensions.
        sm.on_span(&make_test_span(
            "svc",
            "op",
            OtelStatusCode::Ok as i32,
            1_000_000_000,
            1_200_000_000,
            vec![],
        )); // 200ms
        sm.on_span(&make_test_span(
            "svc",
            "op",
            OtelStatusCode::Ok as i32,
            2_000_000_000,
            2_800_000_000,
            vec![],
        )); // 800ms

        let events = sm.flush();
        assert_eq!(events.len(), 2, "same dims → one calls + one duration");

        // Calls should be 2.
        if let Event::Metric(m) = &events[0] {
            if let Some(metric::Data::Sum(sum)) = &m.metric().data {
                let dp = &sum.data_points[0];
                assert_eq!(dp.value, Some(NDPValue::AsInt(2)));
            } else {
                panic!("expected Sum");
            }
        }

        // Duration sum should be 0.2 + 0.8 = 1.0.
        if let Event::Metric(m) = &events[1] {
            if let Some(metric::Data::Histogram(h)) = &m.metric().data {
                let dp = &h.data_points[0];
                assert_eq!(dp.count, 2);
                assert!((dp.sum.unwrap() - 1.0).abs() < 0.001);
            } else {
                panic!("expected Histogram");
            }
        }
    }

    #[test]
    fn different_dimensions_separate() {
        let mut sm = SpanMetrics::new(default_config());

        sm.on_span(&make_test_span(
            "svc-a",
            "op",
            OtelStatusCode::Ok as i32,
            0,
            100_000_000,
            vec![],
        ));
        sm.on_span(&make_test_span(
            "svc-b",
            "op",
            OtelStatusCode::Ok as i32,
            0,
            100_000_000,
            vec![],
        ));

        let events = sm.flush();
        assert_eq!(
            events.len(),
            4,
            "two different service.name → 2×(calls+duration)"
        );
    }

    #[test]
    fn custom_dimensions_extracted() {
        let config = SpanMetricsConfig {
            dimensions: vec![DimensionConfig {
                name: "http.method".to_string(),
                default: None,
            }],
            ..default_config()
        };
        let mut sm = SpanMetrics::new(config);

        let attr = KeyValue {
            key: "http.method".to_string(),
            value: Some(string_value("POST")),
        };
        sm.on_span(&make_test_span(
            "svc",
            "op",
            OtelStatusCode::Ok as i32,
            0,
            100_000_000,
            vec![attr],
        ));

        let events = sm.flush();
        assert_eq!(events.len(), 2);

        // Check that http.method is in the metric attributes.
        if let Event::Metric(m) = &events[0] {
            let attrs = m.first_dp_attrs().expect("should have data point attrs");
            assert!(
                attrs.get("http.method").is_some(),
                "http.method should be a dimension"
            );
        }
    }

    #[test]
    fn flush_resets_aggregations() {
        let mut sm = SpanMetrics::new(default_config());
        sm.on_span(&make_test_span(
            "svc",
            "op",
            OtelStatusCode::Ok as i32,
            0,
            100_000_000,
            vec![],
        ));

        let events1 = sm.flush();
        assert_eq!(events1.len(), 2);

        let events2 = sm.flush();
        assert_eq!(events2.len(), 0, "second flush should be empty after drain");
    }

    #[test]
    fn error_status_dimension() {
        let mut sm = SpanMetrics::new(default_config());
        sm.on_span(&make_test_span(
            "svc",
            "op",
            OtelStatusCode::Ok as i32,
            0,
            100_000_000,
            vec![],
        ));
        sm.on_span(&make_test_span(
            "svc",
            "op",
            OtelStatusCode::Error as i32,
            0,
            100_000_000,
            vec![],
        ));

        let events = sm.flush();
        // OK and ERROR are different dimension keys → 4 events (2 calls + 2 duration).
        assert_eq!(events.len(), 4);
    }
}
