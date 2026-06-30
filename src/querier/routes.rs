// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! HTTP routing for the query backend (warp filters).
//!
//! Shared across the Grafana APIs — task 3 mounts Loki `query_range`; tasks
//! 4/5/7/13 add the Prometheus / Tempo / SQL filters here.

use std::convert::Infallible;
use std::sync::Arc;

use serde::Deserialize;
use warp::{Filter, Reply, filters::BoxedFilter};

use super::{QueryEngine, loki, prometheus, sql, tempo};

/// Inject the shared `QueryEngine` into a filter chain.
fn with_engine(
    engine: Arc<QueryEngine>,
) -> impl Filter<Extract = (Arc<QueryEngine>,), Error = Infallible> + Clone {
    warp::any().map(move || Arc::clone(&engine))
}

/// Wall-clock now in unix nanoseconds, captured at the request boundary. The
/// core query fns stay clock-free + testable (they take `now_ns` explicitly);
/// it anchors an omitted instant `time` (see `instant_anchor`) and the
/// wall-clock sealed/live tier boundary (see `resolve_metric_windows`) so a
/// historical-`end` query still routes long-sealed days to the rollup tier.
fn now_unix_ns() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    )
    .unwrap_or(i64::MAX)
}

#[derive(Debug, Deserialize)]
struct LokiQueryParams {
    query: String,
    start: Option<String>,
    end: Option<String>,
    limit: Option<u32>,
    direction: Option<String>,
    step: Option<String>,
}

fn parse_ns(s: &Option<String>, default: i64) -> i64 {
    s.as_ref()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default)
}

/// Parse a Loki `step` (seconds, possibly with a trailing duration unit Grafana
/// may send) to nanoseconds; fall back to ~1/100 of the range (min 1s).
fn loki_step_ns(step: &Option<String>, start: i64, end: i64) -> i64 {
    let parsed = step.as_ref().and_then(|s| {
        let digits: String = s
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        digits.parse::<f64>().ok()
    });
    if let Some(secs) = parsed.filter(|s| *s > 0.0) {
        return super::units::DurationNs::from_secs_f64(secs)
            .ns()
            .max(1_000_000_000);
    }
    ((end - start) / 100).max(1_000_000_000)
}

async fn loki_query_range(
    params: LokiQueryParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    let t = std::time::Instant::now();
    let start = parse_ns(&params.start, 0);
    let end = parse_ns(&params.end, i64::MAX);
    // Metric (volume / aggregation) queries → matrix; plain selectors → streams.
    let result = if loki::is_metric_query(&params.query) {
        let step = loki_step_ns(&params.step, start, end);
        loki::handle_volume(&engine, &params.query, start, end, step)
            .await
            .map(|v| warp::reply::json(&v).into_response())
    } else {
        let limit = params.limit.unwrap_or(100);
        let forward = params.direction.as_deref() == Some("forward");
        loki::handle_query_range(&engine, &params.query, start, end, limit, forward)
            .await
            .map(|resp| warp::reply::json(&resp).into_response())
    };
    rec("loki", "logs", t);
    match result {
        Ok(resp) => Ok(resp),
        Err(error) => {
            let body = serde_json::json!({"status": "error", "error": error.to_string()});
            Ok(warp::reply::with_status(
                warp::reply::json(&body),
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response())
        }
    }
}

#[derive(Debug, Deserialize)]
struct PromInstantParams {
    query: String,
    time: Option<String>,
}

/// Parse a Prometheus `time` (unix seconds, possibly fractional) to nanoseconds.
/// Absent/unparseable means "now" → the latest sample (`i64::MAX`).
fn parse_time_ns(s: &Option<String>) -> i64 {
    // Ingress boundary: Prometheus/Tempo `time` is unix seconds → canonical ns.
    s.as_ref()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|secs| super::units::TimeNs::from_unix_secs(secs).ns())
        .unwrap_or(i64::MAX)
}

/// Record a served query for the self-monitoring dashboard (NFR6): request
/// count + duration, labelled by `api`/`signal`. Scan volume (bytes/files) is
/// recorded separately, per executed physical plan, by `telemetry::record_scan`
/// from inside the query engine (it sees the plan metrics; a request may run
/// several plans). The latency/throughput panels + the dashboard's
/// `service_name` variable (keyed on `sol_querier_requests_total`) need this called.
fn rec(api: &str, signal: &str, t: std::time::Instant) {
    super::telemetry::record_request(api, signal, t.elapsed());
}

fn error_response(error: impl std::fmt::Display) -> warp::reply::Response {
    let body = serde_json::json!({"status": "error", "error": error.to_string()});
    warp::reply::with_status(
        warp::reply::json(&body),
        warp::http::StatusCode::BAD_REQUEST,
    )
    .into_response()
}

async fn prom_instant(
    params: PromInstantParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    let t = std::time::Instant::now();
    let time_ns = parse_time_ns(&params.time);
    let now_ns = now_unix_ns();
    let r = prometheus::handle_instant(&engine, &params.query, time_ns, now_ns).await;
    rec("prometheus", "metrics", t);
    match r {
        Ok(resp) => Ok(warp::reply::json(&resp).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

#[derive(Debug, Deserialize)]
struct PromRangeParams {
    query: String,
    start: Option<String>,
    end: Option<String>,
    step: Option<String>,
}

/// Parse a Prometheus `step` (seconds, possibly fractional) to nanoseconds.
/// Unparseable/absent → 0, which selects the raw `metrics` table (no rollup).
fn parse_step_ns(s: &Option<String>) -> i64 {
    s.as_ref()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|secs| super::units::DurationNs::from_secs_f64(secs).ns())
        .unwrap_or(0)
}

async fn prom_range(
    params: PromRangeParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    let t = std::time::Instant::now();
    // start defaults to 0 (not "now"), end to now/latest.
    let start_ns = params
        .start
        .as_ref()
        .map_or(0, |_| parse_time_ns(&params.start));
    let end_ns = parse_time_ns(&params.end);
    let step_ns = parse_step_ns(&params.step);
    let now_ns = now_unix_ns();
    let r = prometheus::handle_range(&engine, &params.query, start_ns, end_ns, step_ns, now_ns).await;
    rec("prometheus", "metrics", t);
    match r {
        Ok(resp) => Ok(warp::reply::json(&resp).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

#[derive(Debug, Deserialize)]
struct PromLabelParams {
    start: Option<String>,
    end: Option<String>,
    #[serde(rename = "match[]", default)]
    matcher: Option<String>,
}

async fn prom_label_values(
    label: String,
    params: PromLabelParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    // start defaults to 0 (all past), end to now/latest — same window convention
    // as the range query; an absent window means "all history" (unchanged).
    let start_ns = params.start.as_ref().map_or(0, |_| parse_time_ns(&params.start));
    let end_ns = parse_time_ns(&params.end);
    let now_ns = now_unix_ns();
    match prometheus::handle_label_values(&engine, &label, start_ns, end_ns, params.matcher.as_deref(), now_ns).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

#[derive(Debug, Deserialize)]
struct SeriesParams {
    start: Option<String>,
    end: Option<String>,
    #[serde(rename = "match[]", default)]
    matcher: Option<String>,
}

async fn prom_series(
    params: SeriesParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    // An explicit `[start, end]` range routes the sealed span to the rollup tier
    // (FR5); an absent start (no range) keeps the raw scan. Mirror the
    // label-values convention: start defaults to 0 only to bound the window when
    // end is given — `time_range` is `Some` only when at least `start` is present.
    let time_range = params.start.as_ref().map(|_| {
        (parse_time_ns(&params.start), parse_time_ns(&params.end))
    });
    match prometheus::handle_series(&engine, params.matcher.as_deref(), time_range, now_unix_ns()).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

async fn prom_labels(engine: Arc<QueryEngine>) -> Result<warp::reply::Response, Infallible> {
    match prometheus::handle_labels(&engine).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

async fn loki_labels(engine: Arc<QueryEngine>) -> Result<warp::reply::Response, Infallible> {
    match loki::handle_labels(&engine).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

async fn loki_label_values(
    label: String,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    match loki::handle_label_values(&engine, &label).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

#[derive(Debug, Deserialize)]
struct LokiSeriesParams {
    #[serde(rename = "match[]", default)]
    matcher: Option<String>,
    start: Option<String>,
    end: Option<String>,
}

async fn loki_series(
    params: LokiSeriesParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    let start = parse_ns(&params.start, 0);
    let end = parse_ns(&params.end, i64::MAX);
    match loki::handle_series(&engine, params.matcher.as_deref(), start, end).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

#[derive(Debug, Deserialize)]
struct LokiIndexParams {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    step: Option<String>,
}

async fn loki_index_stats(
    params: LokiIndexParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    let start = parse_ns(&params.start, 0);
    let end = parse_ns(&params.end, i64::MAX);
    let query = params.query.unwrap_or_else(|| "{}".to_string());
    match loki::handle_index_stats(&engine, &query, start, end).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

async fn loki_index_volume_impl(
    params: LokiIndexParams,
    engine: Arc<QueryEngine>,
    range: bool,
) -> Result<warp::reply::Response, Infallible> {
    let t = std::time::Instant::now();
    let start = parse_ns(&params.start, 0);
    let end = parse_ns(&params.end, i64::MAX);
    let step = loki_step_ns(&params.step, start, end);
    let query = params.query.unwrap_or_else(|| "{}".to_string());
    let r = loki::handle_index_volume(&engine, &query, start, end, step, range).await;
    rec("loki", "logs", t);
    match r {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

async fn loki_index_volume(
    params: LokiIndexParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    loki_index_volume_impl(params, engine, false).await
}

async fn loki_index_volume_range(
    params: LokiIndexParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    loki_index_volume_impl(params, engine, true).await
}

async fn tempo_tags(engine: Arc<QueryEngine>) -> Result<warp::reply::Response, Infallible> {
    match tempo::handle_tags(&engine).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

async fn tempo_tags_flat(engine: Arc<QueryEngine>) -> Result<warp::reply::Response, Infallible> {
    match tempo::handle_tags_flat(&engine).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

#[derive(Debug, Deserialize)]
struct TempoSearchParams {
    #[serde(default)]
    q: Option<String>,
    start: Option<String>,
    end: Option<String>,
    limit: Option<u32>,
}

async fn tempo_search(
    params: TempoSearchParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    // Tempo `start`/`end` are unix seconds; default to all-time.
    let start_ns = params
        .start
        .as_ref()
        .map_or(0, |_| parse_time_ns(&params.start));
    let end_ns = parse_time_ns(&params.end);
    let traceql = params.q.unwrap_or_else(|| "{}".to_string());
    let limit = params.limit.unwrap_or(20);
    let t = std::time::Instant::now();
    let r = tempo::handle_search(&engine, &traceql, start_ns, end_ns, limit).await;
    rec("tempo", "traces", t);
    match r {
        Ok(resp) => Ok(warp::reply::json(&resp).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

async fn tempo_trace_by_id(
    id: String,
    accept: Option<String>,
    v2: bool,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    let t = std::time::Instant::now();
    // Grafana fetches traces with `Accept: application/protobuf` and decodes the
    // body as OTLP protobuf regardless of Content-Type; serve that when asked.
    let wants_proto = accept
        .as_deref()
        .is_some_and(|a| a.contains("application/protobuf") || a.contains("application/x-protobuf"));
    let response = if wants_proto {
        tempo::handle_trace_by_id_otlp(&engine, &id).await.map(|bytes| {
            // V2 (/api/v2/traces) wraps the trace in TraceByIDResponse{trace=1};
            // V1 (/api/traces) returns the bare trace.
            let body = if v2 {
                tempo::wrap_trace_by_id_v2(&bytes)
            } else {
                bytes
            };
            warp::http::Response::builder()
                .header("content-type", "application/protobuf")
                .body(body.into())
                .unwrap_or_default()
        })
    } else {
        tempo::handle_trace_by_id(&engine, &id)
            .await
            .map(|body| warp::reply::json(&body).into_response())
    };
    rec("tempo", "traces", t);
    match response {
        Ok(resp) => Ok(resp),
        Err(error) => Ok(error_response(error)),
    }
}

async fn tempo_tag_values(
    tag: String,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    match tempo::handle_tag_values(&engine, &tag).await {
        Ok(body) => Ok(warp::reply::json(&body).into_response()),
        Err(error) => Ok(error_response(error)),
    }
}

#[derive(Debug, Deserialize)]
struct SqlBody {
    #[serde(alias = "query")]
    sql: String,
}

async fn sql_query(
    body: SqlBody,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    let t = std::time::Instant::now();
    let r = sql::handle_sql(&engine, &body.sql).await;
    rec("sql", "sql", t);
    match r {
        Ok(value) => Ok(warp::reply::json(&value).into_response()),
        Err(error) => {
            let msg = error.to_string();
            // NFR9 guardrail breaches surface as HTTP 422.
            let status = if msg.starts_with("guardrail:") {
                warp::http::StatusCode::UNPROCESSABLE_ENTITY
            } else {
                warp::http::StatusCode::BAD_REQUEST
            };
            let payload = serde_json::json!({"status": "error", "error": msg});
            Ok(warp::reply::with_status(warp::reply::json(&payload), status).into_response())
        }
    }
}

/// Build the query backend's warp routes against a shared engine.
pub fn make_routes(engine: Arc<QueryEngine>) -> BoxedFilter<(impl Reply,)> {
    let loki = warp::path!("loki" / "api" / "v1" / "query_range")
        .and(warp::get())
        .and(warp::query::<LokiQueryParams>())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(loki_query_range);

    // Prometheus instant query — accept params from GET query string or POST form.
    let prom_params = warp::get()
        .and(warp::query::<PromInstantParams>())
        .or(warp::post().and(warp::body::form::<PromInstantParams>()))
        .unify();
    let prom_query = warp::path!("prometheus" / "api" / "v1" / "query")
        .and(prom_params)
        .and(with_engine(Arc::clone(&engine)))
        .and_then(prom_instant);

    let prom_range_params = warp::get()
        .and(warp::query::<PromRangeParams>())
        .or(warp::post().and(warp::body::form::<PromRangeParams>()))
        .unify();
    let prom_range = warp::path!("prometheus" / "api" / "v1" / "query_range")
        .and(prom_range_params)
        .and(with_engine(Arc::clone(&engine)))
        .and_then(prom_range);

    let prom_label_values = warp::path!("prometheus" / "api" / "v1" / "label" / String / "values")
        .and(warp::get())
        .and(warp::query::<PromLabelParams>())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(prom_label_values);

    let prom_labels = warp::path!("prometheus" / "api" / "v1" / "labels")
        .and(warp::get().or(warp::post()).unify())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(prom_labels);

    // Grafana probes rules on datasource load; no rule storage -> empty groups.
    let prom_rules = warp::path!("prometheus" / "api" / "v1" / "rules")
        .and(warp::get())
        .map(|| {
            warp::reply::json(&serde_json::json!({ "status": "success", "data": { "groups": [] } }))
        });

    // Metric metadata (Grafana metric browser type/unit hints). Minimal: empty.
    let prom_metadata = warp::path!("prometheus" / "api" / "v1" / "metadata")
        .and(warp::get())
        .map(|| warp::reply::json(&serde_json::json!({ "status": "success", "data": {} })));

    // `series` takes `match[]` from the query string (GET) or form body (POST).
    let series_params = warp::get()
        .and(warp::query::<SeriesParams>())
        .or(warp::post().and(warp::body::form::<SeriesParams>()))
        .unify();
    let prom_series = warp::path!("prometheus" / "api" / "v1" / "series")
        .and(series_params)
        .and(with_engine(Arc::clone(&engine)))
        .and_then(prom_series);

    let loki_labels = warp::path!("loki" / "api" / "v1" / "labels")
        .and(warp::get())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(loki_labels);

    let loki_label_values = warp::path!("loki" / "api" / "v1" / "label" / String / "values")
        .and(warp::get())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(loki_label_values);

    // Loki series (C-L2) — match[] from query string (GET) or form body (POST).
    let loki_series_params = warp::get()
        .and(warp::query::<LokiSeriesParams>())
        .or(warp::post().and(warp::body::form::<LokiSeriesParams>()))
        .unify();
    let loki_series = warp::path!("loki" / "api" / "v1" / "series")
        .and(loki_series_params)
        .and(with_engine(Arc::clone(&engine)))
        .and_then(loki_series);

    // Loki index endpoints (C-L1 volume, C-L3 stats).
    let loki_index_stats = warp::path!("loki" / "api" / "v1" / "index" / "stats")
        .and(warp::get())
        .and(warp::query::<LokiIndexParams>())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(loki_index_stats);
    let loki_index_volume = warp::path!("loki" / "api" / "v1" / "index" / "volume")
        .and(warp::get())
        .and(warp::query::<LokiIndexParams>())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(loki_index_volume);
    let loki_index_volume_range = warp::path!("loki" / "api" / "v1" / "index" / "volume_range")
        .and(warp::get())
        .and(warp::query::<LokiIndexParams>())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(loki_index_volume_range);

    // Tempo: TraceQL search, trace-by-id, tag discovery.
    let tempo_search = warp::path!("tempo" / "api" / "search")
        .and(warp::get())
        .and(warp::query::<TempoSearchParams>())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(tempo_search);
    let tempo_trace_v2 = warp::path!("tempo" / "api" / "v2" / "traces" / String)
        .and(warp::get())
        .and(warp::header::optional::<String>("accept"))
        .and(warp::any().map(|| true))
        .and(with_engine(Arc::clone(&engine)))
        .and_then(tempo_trace_by_id);
    let tempo_trace_v1 = warp::path!("tempo" / "api" / "traces" / String)
        .and(warp::get())
        .and(warp::header::optional::<String>("accept"))
        .and(warp::any().map(|| false))
        .and(with_engine(Arc::clone(&engine)))
        .and_then(tempo_trace_by_id);
    let tempo_tags_v2 = warp::path!("tempo" / "api" / "v2" / "search" / "tags")
        .and(warp::get())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(tempo_tags);
    let tempo_tags_v1 = warp::path!("tempo" / "api" / "search" / "tags")
        .and(warp::get())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(tempo_tags_flat);
    let tempo_tag_values_v2 =
        warp::path!("tempo" / "api" / "v2" / "search" / "tag" / String / "values")
            .and(warp::get())
            .and(with_engine(Arc::clone(&engine)))
            .and_then(tempo_tag_values);
    let tempo_tag_values_v1 = warp::path!("tempo" / "api" / "search" / "tag" / String / "values")
        .and(warp::get())
        .and(with_engine(Arc::clone(&engine)))
        .and_then(tempo_tag_values);

    // Cross-signal SQL endpoint (FR9). Accepts JSON `{"sql"|"query": "…"}`.
    let sql = warp::path!("api" / "v1" / "sql")
        .and(warp::post())
        .and(warp::body::json::<SqlBody>())
        .and(with_engine(engine))
        .and_then(sql_query);
    // Tempo data-source health probe.
    let tempo_echo = warp::path!("tempo" / "api" / "echo")
        .and(warp::get())
        .map(|| "echo");

    // Discovery probe so Grafana data sources "Save & Test" passes.
    let ready = warp::path!("ready")
        .and(warp::get())
        .map(|| warp::reply::with_status("ready", warp::http::StatusCode::OK));

    loki.or(loki_labels)
        .or(loki_label_values)
        .or(loki_series)
        .or(loki_index_stats)
        .or(loki_index_volume_range)
        .or(loki_index_volume)
        .or(prom_query)
        .or(prom_range)
        .or(prom_label_values)
        .or(prom_labels)
        .or(prom_rules)
        .or(prom_metadata)
        .or(prom_series)
        .or(tempo_search)
        .or(tempo_trace_v2)
        .or(tempo_trace_v1)
        .or(tempo_tags_v2)
        .or(tempo_tags_v1)
        .or(tempo_tag_values_v2)
        .or(tempo_tag_values_v1)
        .or(tempo_echo)
        .or(sql)
        .or(ready)
        // Track concurrent requests: an RAII guard increments `query_inflight`
        // when the request is accepted and decrements it once the reply is built
        // (or the route rejects), so the gauge reflects live load.
        .with(warp::wrap_fn(|filter| {
            warp::any()
                .map(super::telemetry::InflightGuard::new)
                .and(filter)
                .map(|_guard: super::telemetry::InflightGuard, reply| reply)
        }))
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::querier::{QuerierOptions, StorageConfig};
    use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;

    async fn fixture_engine() -> Arc<QueryEngine> {
        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp.path().join("logs").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["client"])),
                Arc::new(TimestampNanosecondArray::from(vec![10i64]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec!["hello world"])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("f.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let opts = QuerierOptions {
            storage: StorageConfig {
                path: tmp.path().into(),
                url: None,
            },
            ..QuerierOptions::default()
        };
        Arc::new(QueryEngine::new(&opts).await.unwrap())
    }

    #[tokio::test]
    async fn test_loki_route_serves_streams_json() {
        let routes = make_routes(fixture_engine().await);
        let resp = warp::test::request()
            .method("GET")
            .path("/loki/api/v1/query_range?query=%7Bservice_name%3D%22client%22%7D&start=0&end=1000&limit=10")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), 200);
        let body = String::from_utf8(resp.body().to_vec()).unwrap();
        assert!(body.contains(r#""resultType":"streams""#), "body: {body}");
        assert!(body.contains("hello world"), "body: {body}");
    }

    #[tokio::test]
    async fn test_loki_series_route() {
        let routes = make_routes(fixture_engine().await);
        let resp = warp::test::request()
            .method("GET")
            .path("/loki/api/v1/series?match%5B%5D=%7Bservice_name%3D%22client%22%7D&start=0&end=1000")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), 200);
        let body = String::from_utf8(resp.body().to_vec()).unwrap();
        assert!(body.contains(r#""status":"success""#), "body: {body}");
        assert!(body.contains("client"), "body: {body}");
    }

    #[tokio::test]
    async fn test_loki_index_stats_route_is_flat() {
        let routes = make_routes(fixture_engine().await);
        let resp = warp::test::request()
            .method("GET")
            .path("/loki/api/v1/index/stats?query=%7B%7D&start=0&end=1000")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), 200);
        let body = String::from_utf8(resp.body().to_vec()).unwrap();
        // flat shape: top-level entries/streams/bytes, no status wrapper.
        assert!(body.contains(r#""entries":1"#), "body: {body}");
        assert!(!body.contains("status"), "flat, not wrapped: {body}");
    }

    #[tokio::test]
    async fn test_ready_probe() {
        let routes = make_routes(fixture_engine().await);
        let resp = warp::test::request()
            .method("GET")
            .path("/ready")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), 200);
    }
}
