use std::sync::Arc;

use bytes::Bytes;
use http::StatusCode;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        self as otel_metrics, AggregationTemporality, Metric as OtelMetricProto, metric,
        number_data_point::Value as NDPValue,
    },
    resource::v1::Resource,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use sol_lib::{
    EstimatedJsonEncodedSizeOf,
    event::EventMetadata,
    internal_event::{CountByteSize, InternalEventHandle as _, Registered},
};
use warp::{Filter, filters::BoxedFilter, path, path::FullPath, reply::Response};

use super::ddmetric_proto::{MetricPayload, SketchPayload, metric_payload};
use super::ddsketch::AgentDDSketch;
use super::{ApiKeyQueryParams, DatadogAgentSource, RequestHandler};
use crate::{
    common::{
        datadog::{DatadogMetricType, DatadogSeriesMetric},
        http::ErrorMessage,
    },
    event::{Event, OtelMetric, otel_event::string_value},
    internal_events::EventsReceived,
    sources::util::extract_tag_key_and_value,
};

#[derive(Deserialize, Serialize)]
pub(crate) struct DatadogSeriesRequest {
    pub(crate) series: Vec<DatadogSeriesMetric>,
}

pub(super) fn build_warp_filter(
    handler: RequestHandler,
    source: DatadogAgentSource,
) -> BoxedFilter<(Response,)> {
    let sketches_service = sketches_service(handler.clone(), source.clone());
    let series_v1_service = series_v1_service(handler.clone(), source.clone());
    let series_v2_service = series_v2_service(handler, source);
    sketches_service
        .or(series_v1_service)
        .unify()
        .or(series_v2_service)
        .unify()
        .boxed()
}

fn sketches_service(
    handler: RequestHandler,
    source: DatadogAgentSource,
) -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "beta" / "sketches" / ..))
        .and(warp::path::full())
        .and(warp::header::optional::<String>("content-encoding"))
        .and(warp::header::optional::<String>("dd-api-key"))
        .and(warp::query::<ApiKeyQueryParams>())
        .and(warp::body::bytes())
        .and_then({
            move |path: FullPath,
                  encoding_header: Option<String>,
                  api_token: Option<String>,
                  query_params: ApiKeyQueryParams,
                  body: Bytes| {
                let scope = source.scope.clone();
                let events = source
                    .decode(&encoding_header, body, path.as_str())
                    .and_then(|body| {
                        decode_datadog_sketches(
                            body,
                            source.api_key_extractor.extract(
                                path.as_str(),
                                api_token,
                                query_params.dd_api_key,
                            ),
                            source.split_metric_namespace,
                            &source.events_received,
                            &scope,
                        )
                    });
                handler.clone().handle_request(events, super::METRICS)
            }
        })
        .boxed()
}

fn series_v1_service(
    handler: RequestHandler,
    source: DatadogAgentSource,
) -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "v1" / "series" / ..))
        .and(warp::path::full())
        .and(warp::header::optional::<String>("content-encoding"))
        .and(warp::header::optional::<String>("dd-api-key"))
        .and(warp::query::<ApiKeyQueryParams>())
        .and(warp::body::bytes())
        .and_then({
            move |path: FullPath,
                  encoding_header: Option<String>,
                  api_token: Option<String>,
                  query_params: ApiKeyQueryParams,
                  body: Bytes| {
                let scope = source.scope.clone();
                let events = source
                    .decode(&encoding_header, body, path.as_str())
                    .and_then(|body| {
                        decode_datadog_series_v1(
                            body,
                            source.api_key_extractor.extract(
                                path.as_str(),
                                api_token,
                                query_params.dd_api_key,
                            ),
                            source.split_metric_namespace,
                            &source.events_received,
                            &scope,
                        )
                    });
                handler.clone().handle_request(events, super::METRICS)
            }
        })
        .boxed()
}

fn series_v2_service(
    handler: RequestHandler,
    source: DatadogAgentSource,
) -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "v2" / "series" / ..))
        .and(warp::path::full())
        .and(warp::header::optional::<String>("content-encoding"))
        .and(warp::header::optional::<String>("dd-api-key"))
        .and(warp::query::<ApiKeyQueryParams>())
        .and(warp::body::bytes())
        .and_then({
            move |path: FullPath,
                  encoding_header: Option<String>,
                  api_token: Option<String>,
                  query_params: ApiKeyQueryParams,
                  body: Bytes| {
                let scope = source.scope.clone();
                let events = source
                    .decode(&encoding_header, body, path.as_str())
                    .and_then(|body| {
                        decode_datadog_series_v2(
                            body,
                            source.api_key_extractor.extract(
                                path.as_str(),
                                api_token,
                                query_params.dd_api_key,
                            ),
                            source.split_metric_namespace,
                            &source.events_received,
                            &scope,
                        )
                    });
                handler.clone().handle_request(events, super::METRICS)
            }
        })
        .boxed()
}

fn decode_datadog_sketches(
    body: Bytes,
    api_key: Option<Arc<str>>,
    split_metric_namespace: bool,
    events_received: &Registered<EventsReceived>,
    scope: &InstrumentationScope,
) -> Result<Vec<Event>, ErrorMessage> {
    if body.is_empty() {
        // The datadog agent may send an empty payload as a keep alive
        debug!(message = "Empty payload ignored.");
        return Ok(Vec::new());
    }

    let metrics =
        decode_ddsketch(body, &api_key, split_metric_namespace, scope).map_err(|error| {
            ErrorMessage::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("Error decoding Datadog sketch: {error:?}"),
            )
        })?;

    events_received.emit(CountByteSize(
        metrics.len(),
        metrics.estimated_json_encoded_size_of(),
    ));

    Ok(metrics)
}

fn decode_datadog_series_v2(
    body: Bytes,
    api_key: Option<Arc<str>>,
    split_metric_namespace: bool,
    events_received: &Registered<EventsReceived>,
    scope: &InstrumentationScope,
) -> Result<Vec<Event>, ErrorMessage> {
    if body.is_empty() {
        // The datadog agent may send an empty payload as a keep alive
        debug!(message = "Empty payload ignored.");
        return Ok(Vec::new());
    }

    let metrics =
        decode_ddseries_v2(body, &api_key, split_metric_namespace, scope).map_err(|error| {
            ErrorMessage::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("Error decoding Datadog sketch: {error:?}"),
            )
        })?;

    events_received.emit(CountByteSize(
        metrics.len(),
        metrics.estimated_json_encoded_size_of(),
    ));

    Ok(metrics)
}

/// Converts DD tags (key:value strings) into OTel KeyValue attributes.
fn dd_tags_to_attributes(tags: Vec<String>) -> Vec<KeyValue> {
    tags.iter()
        .map(|tag| {
            let (key, tag_value) = extract_tag_key_and_value(tag);
            let value = tag_value.map(string_value);
            KeyValue { key, value }
        })
        .collect()
}

/// Builds resource attributes from DD metadata (host, namespace, origin).
fn build_resource(host: Option<&str>, namespace: Option<&str>) -> Resource {
    let mut attributes = Vec::new();
    if let Some(h) = host {
        attributes.push(KeyValue {
            key: "host.name".to_string(),
            value: Some(string_value(h)),
        });
    }
    if let Some(ns) = namespace {
        attributes.push(KeyValue {
            key: "metric.namespace".to_string(),
            value: Some(string_value(ns)),
        });
    }
    attributes.push(KeyValue {
        key: "source_type".to_string(),
        value: Some(string_value("datadog_agent")),
    });
    Resource {
        attributes,
        dropped_attributes_count: 0,
        ..Default::default()
    }
}

/// Creates an OtelMetric event with resource, scope, and metadata.
fn build_otel_metric_event(
    metric: OtelMetricProto,
    resource: Resource,
    api_key: &Option<Arc<str>>,
    scope: &InstrumentationScope,
) -> Event {
    let mut otel = OtelMetric::from_parts(metric, Some(resource), None, EventMetadata::default());
    otel.set_scope(scope.clone());
    if let Some(k) = api_key {
        otel.metadata_mut()
            .secrets_mut()
            .insert("datadog_api_key", Arc::clone(k));
    }
    Event::Metric(otel)
}

/// Converts a DD timestamp (seconds since epoch) to nanoseconds.
/// Negative (pre-epoch) timestamps are clamped to 0.
#[expect(
    clippy::cast_sign_loss,
    reason = "ts is guaranteed positive after the <= 0 early return"
)]
const fn dd_ts_to_nanos(ts: i64) -> u64 {
    if ts <= 0 {
        return 0;
    }
    (ts as u64).saturating_mul(1_000_000_000)
}

pub(crate) fn decode_ddseries_v2(
    frame: Bytes,
    api_key: &Option<Arc<str>>,
    split_metric_namespace: bool,
    scope: &InstrumentationScope,
) -> crate::Result<Vec<Event>> {
    let payload = MetricPayload::decode(frame)?;
    let decoded_metrics: Vec<Event> = payload
        .series
        .into_iter()
        .flat_map(|serie| {
            let (namespace, name) = if split_metric_namespace {
                namespace_name_from_dd_metric(&serie.metric)
            } else {
                (None, serie.metric.as_str())
            };
            let mut attributes = dd_tags_to_attributes(serie.tags);

            // Extract host from resources (the only resource type DD agents send)
            let mut host: Option<String> = None;
            for r in &serie.resources {
                if r.r#type == "host" {
                    host = Some(r.name.clone());
                } else {
                    attributes.push(KeyValue {
                        key: format!("resource.{}", r.r#type),
                        value: Some(string_value(&r.name)),
                    });
                }
            }
            if !serie.source_type_name.is_empty() {
                attributes.push(KeyValue {
                    key: "source_type_name".to_string(),
                    value: Some(string_value(&serie.source_type_name)),
                });
            }

            let resource = build_resource(host.as_deref(), namespace);

            // Interval handling for rate/gauge metrics (see DD agent DogStatsD interval logic)
            let interval_secs = if serie.interval > 0 {
                Some(serie.interval)
            } else {
                None
            };

            match metric_payload::MetricType::try_from(serie.r#type) {
                Ok(metric_payload::MetricType::Count) => serie
                    .points
                    .iter()
                    .map(|dd_point| {
                        let metric = OtelMetricProto {
                            name: name.to_string(),
                            description: String::new(),
                            unit: String::new(),
                            metadata: vec![],
                            data: Some(metric::Data::Sum(otel_metrics::Sum {
                                data_points: vec![otel_metrics::NumberDataPoint {
                                    attributes: attributes.clone(),
                                    start_time_unix_nano: 0,
                                    time_unix_nano: dd_ts_to_nanos(dd_point.timestamp),
                                    exemplars: vec![],
                                    flags: 0,
                                    value: Some(NDPValue::AsDouble(dd_point.value)),
                                }],
                                aggregation_temporality: AggregationTemporality::Delta as i32,
                                is_monotonic: true,
                            })),
                        };
                        build_otel_metric_event(metric, resource.clone(), api_key, scope)
                    })
                    .collect::<Vec<_>>(),
                Ok(metric_payload::MetricType::Gauge) => serie
                    .points
                    .iter()
                    .map(|dd_point| {
                        let mut dp_attrs = attributes.clone();
                        if let Some(interval) = interval_secs {
                            dp_attrs.push(KeyValue {
                                key: "interval_ms".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::IntValue(interval * 1000)),
                                }),
                            });
                        }
                        let metric = OtelMetricProto {
                            name: name.to_string(),
                            description: String::new(),
                            unit: String::new(),
                            metadata: vec![],
                            data: Some(metric::Data::Gauge(otel_metrics::Gauge {
                                data_points: vec![otel_metrics::NumberDataPoint {
                                    attributes: dp_attrs,
                                    start_time_unix_nano: 0,
                                    time_unix_nano: dd_ts_to_nanos(dd_point.timestamp),
                                    exemplars: vec![],
                                    flags: 0,
                                    value: Some(NDPValue::AsDouble(dd_point.value)),
                                }],
                            })),
                        };
                        build_otel_metric_event(metric, resource.clone(), api_key, scope)
                    })
                    .collect::<Vec<_>>(),
                Ok(metric_payload::MetricType::Rate) => serie
                    .points
                    .iter()
                    .map(|dd_point| {
                        #[expect(
                            clippy::cast_possible_truncation,
                            clippy::cast_sign_loss,
                            reason = "DD metric interval (i64) is a small positive seconds value"
                        )]
                        let i = Some(serie.interval)
                            .filter(|v| *v != 0)
                            .map(|v| v as u32)
                            .unwrap_or(1);
                        let mut dp_attrs = attributes.clone();
                        dp_attrs.push(KeyValue {
                            key: "interval_ms".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::IntValue(i64::from(i) * 1000)),
                            }),
                        });
                        let metric = OtelMetricProto {
                            name: name.to_string(),
                            description: String::new(),
                            unit: String::new(),
                            metadata: vec![],
                            data: Some(metric::Data::Sum(otel_metrics::Sum {
                                data_points: vec![otel_metrics::NumberDataPoint {
                                    attributes: dp_attrs,
                                    start_time_unix_nano: 0,
                                    time_unix_nano: dd_ts_to_nanos(dd_point.timestamp),
                                    exemplars: vec![],
                                    flags: 0,
                                    value: Some(NDPValue::AsDouble(dd_point.value * f64::from(i))),
                                }],
                                aggregation_temporality: AggregationTemporality::Delta as i32,
                                is_monotonic: true,
                            })),
                        };
                        build_otel_metric_event(metric, resource.clone(), api_key, scope)
                    })
                    .collect::<Vec<_>>(),
                Ok(metric_payload::MetricType::Unspecified) | Err(_) => {
                    warn!("Unspecified metric type ({}).", serie.r#type);
                    Vec::new()
                }
            }
        })
        .collect();

    Ok(decoded_metrics)
}

fn decode_datadog_series_v1(
    body: Bytes,
    api_key: Option<Arc<str>>,
    split_metric_namespace: bool,
    events_received: &Registered<EventsReceived>,
    scope: &InstrumentationScope,
) -> Result<Vec<Event>, ErrorMessage> {
    if body.is_empty() {
        // The datadog agent may send an empty payload as a keep alive
        debug!(message = "Empty payload ignored.");
        return Ok(Vec::new());
    }

    let metrics: DatadogSeriesRequest = serde_json::from_slice(&body).map_err(|error| {
        ErrorMessage::new(
            StatusCode::BAD_REQUEST,
            format!("Error parsing JSON: {error:?}"),
        )
    })?;

    let decoded_metrics: Vec<Event> = metrics
        .series
        .into_iter()
        .flat_map(|m| into_otel_metric(m, api_key.clone(), split_metric_namespace, scope))
        .collect();

    events_received.emit(CountByteSize(
        decoded_metrics.len(),
        decoded_metrics.estimated_json_encoded_size_of(),
    ));

    Ok(decoded_metrics)
}

fn into_otel_metric(
    dd_metric: DatadogSeriesMetric,
    api_key: Option<Arc<str>>,
    split_metric_namespace: bool,
    scope: &InstrumentationScope,
) -> Vec<Event> {
    let mut attributes = dd_tags_to_attributes(dd_metric.tags.unwrap_or_default());

    if let Some(source) = dd_metric.source_type_name {
        attributes.push(KeyValue {
            key: "source_type_name".to_string(),
            value: Some(string_value(source)),
        });
    }
    if let Some(dev) = dd_metric.device {
        attributes.push(KeyValue {
            key: "device".to_string(),
            value: Some(string_value(dev)),
        });
    }

    let (namespace, name) = if split_metric_namespace {
        namespace_name_from_dd_metric(&dd_metric.metric)
    } else {
        (None, dd_metric.metric.as_str())
    };

    let resource = build_resource(dd_metric.host.as_deref(), namespace);

    match dd_metric.r#type {
        DatadogMetricType::Count => dd_metric
            .points
            .iter()
            .map(|dd_point| {
                let metric = OtelMetricProto {
                    name: name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Sum(otel_metrics::Sum {
                        data_points: vec![otel_metrics::NumberDataPoint {
                            attributes: attributes.clone(),
                            start_time_unix_nano: 0,
                            time_unix_nano: dd_ts_to_nanos(dd_point.0),
                            exemplars: vec![],
                            flags: 0,
                            value: Some(NDPValue::AsDouble(dd_point.1)),
                        }],
                        aggregation_temporality: AggregationTemporality::Delta as i32,
                        is_monotonic: true,
                    })),
                };
                build_otel_metric_event(metric, resource.clone(), &api_key, scope)
            })
            .collect::<Vec<_>>(),
        DatadogMetricType::Gauge => dd_metric
            .points
            .iter()
            .map(|dd_point| {
                let metric = OtelMetricProto {
                    name: name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(otel_metrics::Gauge {
                        data_points: vec![otel_metrics::NumberDataPoint {
                            attributes: attributes.clone(),
                            start_time_unix_nano: 0,
                            time_unix_nano: dd_ts_to_nanos(dd_point.0),
                            exemplars: vec![],
                            flags: 0,
                            value: Some(NDPValue::AsDouble(dd_point.1)),
                        }],
                    })),
                };
                build_otel_metric_event(metric, resource.clone(), &api_key, scope)
            })
            .collect::<Vec<_>>(),
        // Agent sends rate only for dogstatsd counter
        // for consistency purpose they are turned back into counters with interval
        DatadogMetricType::Rate => dd_metric
            .points
            .iter()
            .map(|dd_point| {
                let i = dd_metric.interval.filter(|v| *v != 0).unwrap_or(1);
                let mut dp_attrs = attributes.clone();
                dp_attrs.push(KeyValue {
                    key: "interval_ms".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::IntValue(i64::from(i) * 1000)),
                    }),
                });
                let metric = OtelMetricProto {
                    name: name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Sum(otel_metrics::Sum {
                        data_points: vec![otel_metrics::NumberDataPoint {
                            attributes: dp_attrs,
                            start_time_unix_nano: 0,
                            time_unix_nano: dd_ts_to_nanos(dd_point.0),
                            exemplars: vec![],
                            flags: 0,
                            value: Some(NDPValue::AsDouble(dd_point.1 * f64::from(i))),
                        }],
                        aggregation_temporality: AggregationTemporality::Delta as i32,
                        is_monotonic: true,
                    })),
                };
                build_otel_metric_event(metric, resource.clone(), &api_key, scope)
            })
            .collect::<Vec<_>>(),
    }
}

/// Parses up to the first '.' of the input metric name into a namespace.
/// If no delimiter, the namespace is None type.
fn namespace_name_from_dd_metric(dd_metric_name: &str) -> (Option<&str>, &str) {
    // ex: "system.fs.util" -> ("system", "fs.util")
    match dd_metric_name.split_once('.') {
        Some((namespace, name)) => (Some(namespace), name),
        None => (None, dd_metric_name),
    }
}

pub(crate) fn decode_ddsketch(
    frame: Bytes,
    api_key: &Option<Arc<str>>,
    split_metric_namespace: bool,
    scope: &InstrumentationScope,
) -> crate::Result<Vec<Event>> {
    let payload = SketchPayload::decode(frame)?;
    // payload.metadata is always empty for payload coming from dd agents
    Ok(payload
        .sketches
        .into_iter()
        .flat_map(|sketch_series| {
            // sketch_series.distributions is also always empty from payload coming from dd agents
            let attributes = dd_tags_to_attributes(sketch_series.tags);
            let host = if sketch_series.host.is_empty() {
                None
            } else {
                Some(sketch_series.host.as_str())
            };

            let (namespace, name) = if split_metric_namespace {
                namespace_name_from_dd_metric(&sketch_series.metric)
            } else {
                (None, sketch_series.metric.as_str())
            };
            let resource = build_resource(host, namespace);

            let name = name.to_string();
            let resource = resource.clone();
            sketch_series.dogsketches.into_iter().map(move |sketch| {
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "DD sketch k/n/cnt fields are narrow proto values fitting i16/u16/u32"
                )]
                let k: Vec<i16> = sketch.k.iter().map(|k| *k as i16).collect();
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "DD sketch k/n/cnt fields are narrow proto values fitting i16/u16/u32"
                )]
                let n: Vec<u16> = sketch.n.iter().map(|n| *n as u16).collect();
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "DD sketch cnt is a non-negative count that fits u32"
                )]
                let cnt = sketch.cnt as u32;
                let ddsketch = AgentDDSketch::from_raw(
                    cnt, sketch.min, sketch.max, sketch.sum, sketch.avg, &k, &n,
                )
                .unwrap_or_else(AgentDDSketch::with_agent_defaults);

                let exp_histo_dp = ddsketch.to_exponential_histogram_data_point(
                    attributes.clone(),
                    dd_ts_to_nanos(sketch.ts),
                );

                let metric = OtelMetricProto {
                    name: name.clone(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::ExponentialHistogram(
                        otel_metrics::ExponentialHistogram {
                            data_points: vec![exp_histo_dp],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        },
                    )),
                };
                build_otel_metric_event(metric, resource.clone(), api_key, scope)
            })
        })
        .collect())
}
