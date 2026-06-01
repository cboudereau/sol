//! Querier-side telemetry (task 9).
//!
//! Emits the `sol_query_*` and `sol_objectstore_*` metric catalog from
//! [DESIGN.md §cross-cutting](../../../docs/workspace/parquet-backend/DESIGN.md#cross-cutting-concerns)
//! via Vector's `metrics` facility, so Sol monitors its own backend through the
//! `internal_metrics` source. Names and labels match the `SOL Query Backend`
//! dashboard. Histograms (`*_duration_seconds`, `*_bytes_scanned`,
//! `*_files_opened`) are exposed with Prometheus `_bucket`/`_sum`/`_count` by
//! `internal_metrics`, so `histogram_quantile` works in the dashboard.
//!
//! `sol_compactor_*` shares the namespace but is emitted by the compactor
//! (task 10); frontend shard metrics by task 11.

use std::time::Duration;

use metrics::{counter, gauge, histogram};

/// Record a served query: a request counter plus duration / bytes-scanned /
/// files-opened histograms, labelled by `api` (prometheus/loki/tempo/sql) and
/// `signal` (logs/metrics/traces).
#[allow(clippy::cast_precision_loss)] // metric magnitudes are well under 2^53
pub fn record_request(
    api: &str,
    signal: &str,
    duration: Duration,
    bytes_scanned: u64,
    files_opened: u64,
) {
    let api = api.to_string();
    let signal = signal.to_string();
    counter!("sol_query_requests_total", "api" => api.clone(), "signal" => signal.clone())
        .increment(1);
    histogram!("sol_query_request_duration_seconds", "api" => api.clone(), "signal" => signal.clone())
        .record(duration.as_secs_f64());
    histogram!("sol_query_bytes_scanned", "api" => api.clone(), "signal" => signal.clone())
        .record(bytes_scanned as f64);
    histogram!("sol_query_files_opened", "api" => api, "signal" => signal)
        .record(files_opened as f64);
}

/// Record a cache lookup outcome (`result=hit|miss`).
pub fn record_cache(hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    counter!("sol_query_cache_requests_total", "result" => result).increment(1);
}

/// Set the current cache memory footprint (bytes).
#[allow(clippy::cast_precision_loss)]
pub fn set_cache_memory(bytes: u64) {
    gauge!("sol_query_cache_memory_bytes").set(bytes as f64);
}

/// Increment / decrement the in-flight query gauge.
pub fn inc_inflight() {
    gauge!("sol_query_inflight").increment(1.0);
}

/// Decrement the in-flight query gauge.
pub fn dec_inflight() {
    gauge!("sol_query_inflight").decrement(1.0);
}

/// Record an object-store request, flagging throttles (HTTP 503, NFR10).
pub fn record_objectstore(duration: Duration, throttled: bool) {
    counter!("sol_objectstore_requests_total").increment(1);
    if throttled {
        counter!("sol_objectstore_throttled_total").increment(1);
    }
    histogram!("sol_objectstore_request_duration_seconds").record(duration.as_secs_f64());
}

/// Record a guardrail rejection (NFR9 — `reason` e.g. `range`/`bytes`).
pub fn record_rejected(reason: &str) {
    counter!("sol_query_rejected_total", "reason" => reason.to_string()).increment(1);
}

/// Record an unsupported query construct (`lang` e.g. promql, `construct`).
pub fn record_unsupported(lang: &str, construct: &str) {
    counter!("sol_query_unsupported_total", "lang" => lang.to_string(), "construct" => construct.to_string())
        .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::MetricKind;
    use metrics_util::debugging::DebuggingRecorder;

    fn has_metric(
        snapshot: &[(metrics_util::CompositeKey, Option<metrics::Unit>, Option<metrics::SharedString>, metrics_util::debugging::DebugValue)],
        kind: MetricKind,
        name: &str,
    ) -> Option<Vec<(String, String)>> {
        snapshot.iter().find(|(k, _, _, _)| k.kind() == kind && k.key().name() == name).map(
            |(k, _, _, _)| {
                k.key().labels().map(|l| (l.key().to_string(), l.value().to_string())).collect()
            },
        )
    }

    #[test]
    fn test_request_duration_histogram_emitted() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_request("prometheus", "metrics", Duration::from_millis(12), 4096, 3);
        });
        let s = snap.snapshot().into_vec();
        let labels = has_metric(&s, MetricKind::Histogram, "sol_query_request_duration_seconds")
            .expect("duration histogram emitted");
        assert!(labels.contains(&("api".to_string(), "prometheus".to_string())), "labels: {labels:?}");
        assert!(labels.contains(&("signal".to_string(), "metrics".to_string())), "labels: {labels:?}");
        assert!(has_metric(&s, MetricKind::Histogram, "sol_query_bytes_scanned").is_some());
        assert!(has_metric(&s, MetricKind::Histogram, "sol_query_files_opened").is_some());
        assert!(has_metric(&s, MetricKind::Counter, "sol_query_requests_total").is_some());
    }

    #[test]
    fn test_cache_hit_miss_counters() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_cache(true);
            record_cache(false);
        });
        let s = snap.snapshot().into_vec();
        // both hit and miss variants of the labelled counter are present
        let hits = s.iter().filter(|(k, _, _, _)| {
            k.kind() == MetricKind::Counter
                && k.key().name() == "sol_query_cache_requests_total"
                && k.key().labels().any(|l| l.key() == "result" && l.value() == "hit")
        });
        assert_eq!(hits.count(), 1, "hit counter emitted");
        let misses = s.iter().filter(|(k, _, _, _)| {
            k.kind() == MetricKind::Counter
                && k.key().name() == "sol_query_cache_requests_total"
                && k.key().labels().any(|l| l.key() == "result" && l.value() == "miss")
        });
        assert_eq!(misses.count(), 1, "miss counter emitted");
    }

    #[test]
    fn test_objectstore_throttle_counter() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_objectstore(Duration::from_millis(5), true);
        });
        let s = snap.snapshot().into_vec();
        assert!(
            has_metric(&s, MetricKind::Counter, "sol_objectstore_throttled_total").is_some(),
            "503 throttle counter emitted"
        );
        assert!(has_metric(&s, MetricKind::Counter, "sol_objectstore_requests_total").is_some());
        assert!(
            has_metric(&s, MetricKind::Histogram, "sol_objectstore_request_duration_seconds")
                .is_some()
        );
    }

    #[test]
    fn test_guardrail_reject_counter() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || record_rejected("range"));
        let s = snap.snapshot().into_vec();
        let labels = has_metric(&s, MetricKind::Counter, "sol_query_rejected_total")
            .expect("guardrail reject counter emitted");
        assert!(labels.contains(&("reason".to_string(), "range".to_string())), "labels: {labels:?}");
    }

    #[test]
    fn test_unsupported_construct_counter() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || record_unsupported("promql", "subquery"));
        let s = snap.snapshot().into_vec();
        let labels = has_metric(&s, MetricKind::Counter, "sol_query_unsupported_total")
            .expect("unsupported construct counter emitted");
        assert!(labels.contains(&("lang".to_string(), "promql".to_string())), "labels: {labels:?}");
        assert!(labels.contains(&("construct".to_string(), "subquery".to_string())), "labels: {labels:?}");
    }
}
