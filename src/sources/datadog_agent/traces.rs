use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use futures::future;
use http::StatusCode;
use opentelemetry_proto::tonic::{
    common::v1::{InstrumentationScope, KeyValue},
    resource::v1::Resource,
    trace::v1::{Span, Status, status::StatusCode as OtelStatusCode},
};
use prost::Message;
use sol_lib::{
    EstimatedJsonEncodedSizeOf,
    internal_event::{CountByteSize, InternalEventHandle as _},
};
use warp::{Filter, Rejection, Reply, filters::BoxedFilter, path, path::FullPath, reply::Response};

use super::{ApiKeyQueryParams, DatadogAgentSource, RequestHandler, ddtrace_proto};
use crate::{
    common::http::ErrorMessage,
    event::{Event, OtelSpan, otel_event::string_value},
};

pub(super) fn build_warp_filter(
    handler: RequestHandler,
    source: DatadogAgentSource,
) -> BoxedFilter<(Response,)> {
    build_trace_filter(handler, source)
        .or(build_stats_filter())
        .unify()
        .boxed()
}

fn build_trace_filter(
    handler: RequestHandler,
    source: DatadogAgentSource,
) -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "v0.2" / "traces" / ..))
        .and(warp::path::full())
        .and(warp::header::optional::<String>("content-encoding"))
        .and(warp::header::optional::<String>("dd-api-key"))
        .and(warp::header::optional::<String>(
            "X-Datadog-Reported-Languages",
        ))
        .and(warp::query::<ApiKeyQueryParams>())
        .and(warp::body::bytes())
        .and_then({
            move |path: FullPath,
                  encoding_header: Option<String>,
                  api_token: Option<String>,
                  reported_language: Option<String>,
                  query_params: ApiKeyQueryParams,
                  body: Bytes| {
                let events = source
                    .decode(&encoding_header, body, path.as_str())
                    .and_then(|body| {
                        handle_dd_trace_payload(
                            body,
                            source.api_key_extractor.extract(
                                path.as_str(),
                                api_token,
                                query_params.dd_api_key,
                            ),
                            reported_language.as_ref(),
                            &source,
                        )
                        .map_err(|error| {
                            ErrorMessage::new(
                                StatusCode::UNPROCESSABLE_ENTITY,
                                format!("Error decoding Datadog traces: {error:?}"),
                            )
                        })
                    });
                handler.clone().handle_request(events, super::TRACES)
            }
        })
        .boxed()
}

fn build_stats_filter() -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "v0.2" / "stats" / ..))
        .and_then(|| {
            // APM stats are discarded on purpose, they will be computed in the `datadog_traces` sink
            // thus we simply reply with a 200/OK response.
            let response: Result<Response, Rejection> = Ok(warp::reply().into_response());
            future::ready(response)
        })
        .boxed()
}

fn handle_dd_trace_payload(
    frame: Bytes,
    api_key: Option<Arc<str>>,
    lang: Option<&String>,
    source: &DatadogAgentSource,
) -> crate::Result<Vec<Event>> {
    let decoded_payload = ddtrace_proto::TracePayload::decode(frame)?;
    if decoded_payload.tracer_payloads.is_empty() {
        debug!("Older trace payload decoded.");
        handle_dd_trace_payload_v0(decoded_payload, api_key, lang, source)
    } else {
        debug!("Newer trace payload decoded.");
        handle_dd_trace_payload_v1(decoded_payload, api_key, source)
    }
}

/// Convert a DD u64 trace_id to OTel 16-byte trace_id (zero-extended).
fn dd_trace_id_to_otel(id: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; 16];
    bytes[8..16].copy_from_slice(&id.to_be_bytes());
    bytes
}

/// Convert a DD u64 span_id to OTel 8-byte span_id.
fn dd_span_id_to_otel(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

/// Build an OTel Status from DD error code.
fn dd_error_to_otel_status(error: i32) -> Option<Status> {
    if error != 0 {
        Some(Status {
            message: format!("dd.error={error}"),
            code: OtelStatusCode::Error as i32,
        })
    } else {
        Some(Status {
            message: String::new(),
            code: OtelStatusCode::Ok as i32,
        })
    }
}

/// Convert DD span meta (string→string tags) to OTel KeyValue attributes.
fn dd_meta_to_attributes(meta: BTreeMap<String, String>) -> Vec<KeyValue> {
    meta.into_iter()
        .map(|(k, v)| KeyValue {
            key: k,
            value: Some(string_value(v)),
        })
        .collect()
}

/// Convert DD span metrics (string→f64 tags) to OTel KeyValue attributes.
fn dd_metrics_to_attributes(metrics: BTreeMap<String, f64>) -> Vec<KeyValue> {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
    metrics
        .into_iter()
        .map(|(k, v)| KeyValue {
            key: k,
            value: Some(AnyValue {
                value: Some(any_value::Value::DoubleValue(v)),
            }),
        })
        .collect()
}

/// Convert a single DD Span proto to an OtelSpan event.
fn dd_span_to_otel(
    dd_span: ddtrace_proto::Span,
    resource: &Resource,
    scope: &Option<InstrumentationScope>,
    api_key: &Option<Arc<str>>,
    extra_attributes: &[KeyValue],
) -> Event {
    // Set service.name on the resource from the DD span's service field.
    let mut span_resource = resource.clone();
    if !dd_span.service.is_empty() {
        // Replace or add service.name on the resource.
        if let Some(attr) = span_resource
            .attributes
            .iter_mut()
            .find(|a| a.key == "service.name")
        {
            attr.value = Some(string_value(&dd_span.service));
        } else {
            span_resource.attributes.push(KeyValue {
                key: "service.name".to_string(),
                value: Some(string_value(&dd_span.service)),
            });
        }
    }

    let mut attributes = dd_meta_to_attributes(dd_span.meta);
    attributes.extend(dd_metrics_to_attributes(dd_span.metrics));

    // DD-specific fields as attributes
    attributes.push(KeyValue {
        key: "dd.resource".to_string(),
        value: Some(string_value(&dd_span.resource)),
    });
    if !dd_span.r#type.is_empty() {
        attributes.push(KeyValue {
            key: "dd.span_type".to_string(),
            value: Some(string_value(&dd_span.r#type)),
        });
    }
    // Include extra trace-level attributes
    attributes.extend_from_slice(extra_attributes);

    #[expect(
        clippy::cast_sign_loss,
        reason = "DD span start/duration are non-negative nanosecond timestamps"
    )]
    let start_time_unix_nano = dd_span.start as u64;
    #[expect(
        clippy::cast_sign_loss,
        reason = "DD span start+duration is a non-negative nanosecond timestamp"
    )]
    let end_time_unix_nano = (dd_span.start + dd_span.duration) as u64;
    let span = Span {
        trace_id: dd_trace_id_to_otel(dd_span.trace_id),
        span_id: dd_span_id_to_otel(dd_span.span_id),
        parent_span_id: dd_span_id_to_otel(dd_span.parent_id),
        name: dd_span.name,
        kind: 0, // SPAN_KIND_UNSPECIFIED — DD doesn't distinguish client/server/etc.
        start_time_unix_nano,
        end_time_unix_nano,
        attributes,
        status: dd_error_to_otel_status(dd_span.error),
        trace_state: String::new(),
        dropped_attributes_count: 0,
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        flags: 0,
    };

    let mut otel_span =
        OtelSpan::from_parts(span, Some(span_resource), scope.clone(), Default::default());
    if let Some(k) = api_key {
        otel_span
            .metadata_mut()
            .secrets_mut()
            .insert("datadog_api_key", Arc::clone(k));
    }
    Event::Trace(otel_span)
}

/// Build a Resource from trace-level DD metadata.
fn build_trace_resource(hostname: &str, env: &str, service: Option<&str>) -> Resource {
    let mut attrs = vec![
        KeyValue {
            key: "host.name".to_string(),
            value: Some(string_value(hostname)),
        },
        KeyValue {
            key: "deployment.environment".to_string(),
            value: Some(string_value(env)),
        },
        KeyValue {
            key: "source_type".to_string(),
            value: Some(string_value("datadog_agent")),
        },
    ];
    if let Some(svc) = service {
        attrs.push(KeyValue {
            key: "service.name".to_string(),
            value: Some(string_value(svc)),
        });
    }
    Resource {
        attributes: attrs,
        dropped_attributes_count: 0,
    }
}

/// Convert DD tags (BTreeMap) to OTel KeyValue attributes.
fn dd_tags_to_attributes(tags: BTreeMap<String, String>) -> Vec<KeyValue> {
    tags.into_iter()
        .map(|(k, v)| KeyValue {
            key: k,
            value: Some(string_value(v)),
        })
        .collect()
}

/// Decode Datadog newer protobuf schema (v1 — with tracer_payloads)
fn handle_dd_trace_payload_v1(
    decoded_payload: ddtrace_proto::TracePayload,
    api_key: Option<Arc<str>>,
    source: &DatadogAgentSource,
) -> crate::Result<Vec<Event>> {
    let env = decoded_payload.env;
    let hostname = decoded_payload.host_name;
    let agent_version = decoded_payload.agent_version;
    let target_tps = decoded_payload.target_tps;
    let error_tps = decoded_payload.error_tps;
    let payload_tags = dd_tags_to_attributes(decoded_payload.tags);

    let events: Vec<Event> = decoded_payload
        .tracer_payloads
        .into_iter()
        .flat_map(|tracer| {
            let scope = Some(InstrumentationScope {
                name: format!("datadog.tracer.{}", tracer.language_name),
                version: tracer.tracer_version.clone(),
                attributes: vec![],
                dropped_attributes_count: 0,
            });

            let resource = build_trace_resource(&hostname, &env, None);

            // Tracer-level extra attributes
            let mut tracer_attrs = dd_tags_to_attributes(tracer.tags);
            tracer_attrs.extend(payload_tags.clone());
            tracer_attrs.push(KeyValue {
                key: "dd.payload_version".to_string(),
                value: Some(string_value("v2")),
            });
            if !tracer.container_id.is_empty() {
                tracer_attrs.push(KeyValue {
                    key: "dd.container_id".to_string(),
                    value: Some(string_value(&tracer.container_id)),
                });
            }
            if !tracer.runtime_id.is_empty() {
                tracer_attrs.push(KeyValue {
                    key: "dd.runtime_id".to_string(),
                    value: Some(string_value(&tracer.runtime_id)),
                });
            }
            if !tracer.app_version.is_empty() {
                tracer_attrs.push(KeyValue {
                    key: "dd.app_version".to_string(),
                    value: Some(string_value(&tracer.app_version)),
                });
            }
            if !tracer.language_version.is_empty() {
                tracer_attrs.push(KeyValue {
                    key: "dd.language_version".to_string(),
                    value: Some(string_value(&tracer.language_version)),
                });
            }
            if !agent_version.is_empty() {
                tracer_attrs.push(KeyValue {
                    key: "dd.agent_version".to_string(),
                    value: Some(string_value(&agent_version)),
                });
            }
            {
                use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
                tracer_attrs.push(KeyValue {
                    key: "dd.target_tps".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::DoubleValue(target_tps)),
                    }),
                });
                tracer_attrs.push(KeyValue {
                    key: "dd.error_tps".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::DoubleValue(error_tps)),
                    }),
                });
            }

            tracer
                .chunks
                .into_iter()
                .flat_map(|chunk| {
                    let mut chunk_attrs = tracer_attrs.clone();
                    if !chunk.origin.is_empty() {
                        chunk_attrs.push(KeyValue {
                            key: "dd.origin".to_string(),
                            value: Some(string_value(&chunk.origin)),
                        });
                    }
                    {
                        use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
                        chunk_attrs.push(KeyValue {
                            key: "dd.priority".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::IntValue(i64::from(chunk.priority))),
                            }),
                        });
                    }
                    if chunk.dropped_trace {
                        use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
                        chunk_attrs.push(KeyValue {
                            key: "dd.dropped".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::BoolValue(true)),
                            }),
                        });
                    }
                    chunk_attrs.extend(dd_tags_to_attributes(chunk.tags));

                    chunk
                        .spans
                        .into_iter()
                        .map(|span| {
                            dd_span_to_otel(span, &resource, &scope, &api_key, &chunk_attrs)
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
        .collect();

    source.events_received.emit(CountByteSize(
        events.len(),
        events.estimated_json_encoded_size_of(),
    ));

    Ok(events)
}

/// Decode Datadog older protobuf schema (v0 — with traces/transactions)
fn handle_dd_trace_payload_v0(
    decoded_payload: ddtrace_proto::TracePayload,
    api_key: Option<Arc<str>>,
    lang: Option<&String>,
    source: &DatadogAgentSource,
) -> crate::Result<Vec<Event>> {
    let env = decoded_payload.env;
    let hostname = decoded_payload.host_name;

    let scope = lang.map(|l| InstrumentationScope {
        name: format!("datadog.tracer.{l}"),
        version: String::new(),
        attributes: vec![],
        dropped_attributes_count: 0,
    });

    let resource = build_trace_resource(&hostname, &env, None);

    let payload_version_attr = KeyValue {
        key: "dd.payload_version".to_string(),
        value: Some(string_value("v1")),
    };

    let events: Vec<Event> = decoded_payload
        .traces
        .into_iter()
        .flat_map(|dd_trace| {
            dd_trace
                .spans
                .into_iter()
                .map(|span| {
                    dd_span_to_otel(
                        span,
                        &resource,
                        &scope,
                        &api_key,
                        std::slice::from_ref(&payload_version_attr),
                    )
                })
                .collect::<Vec<_>>()
        })
        // Each APM transaction span is also mapped into its own event
        .chain(decoded_payload.transactions.into_iter().map(|span| {
            let mut extra = vec![payload_version_attr.clone()];
            use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value};
            extra.push(KeyValue {
                key: "dd.dropped".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::BoolValue(true)),
                }),
            });
            dd_span_to_otel(span, &resource, &scope, &api_key, &extra)
        }))
        .collect();

    source.events_received.emit(CountByteSize(
        events.len(),
        events.estimated_json_encoded_size_of(),
    ));

    Ok(events)
}
