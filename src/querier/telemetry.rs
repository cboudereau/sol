// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Querier-side telemetry (task 9).
//!
//! Emits the `query_*` and `objectstore_*` metric catalog from
//! [DESIGN.md §cross-cutting](../../../docs/workspace/parquet-backend/DESIGN.md#cross-cutting-concerns)
//! via Vector's `metrics` facility, so Sol monitors its own backend through the
//! `internal_metrics` source. Names are emitted **unprefixed**: the
//! `internal_metrics` source prepends its namespace (default `sol`), so they
//! surface as `sol_querier_*` / `sol_objectstore_*` — prefixing here would
//! double up (`sol_sol_querier_*`). Surfaced names and labels match the
//! `SOL Query Backend` dashboard. Histograms (`*_duration_seconds`, `*_bytes_scanned`,
//! `*_files_opened`) are exposed with Prometheus `_bucket`/`_sum`/`_count` by
//! `internal_metrics`, so `histogram_quantile` works in the dashboard.
//!
//! `compactor_*` (surfaced `sol_compactor_*`) shares the namespace but is
//! emitted by the compactor (task 10); frontend shard metrics by task 11.

use std::time::Duration;

use metrics::{counter, gauge, histogram};

/// Record a served query: a request counter plus a duration histogram, labelled
/// by `api` (prometheus/loki/tempo/sql) and `signal` (logs/metrics/traces).
/// Scan volume (bytes/files) is recorded separately by [`record_scan`], keyed on
/// `signal` only — it is observed from the executed physical plan, which a single
/// served request may run several times (day-shard split, `histogram_quantile`).
pub fn record_request(api: &str, signal: &str, duration: Duration) {
    let api = api.to_string();
    let signal = signal.to_string();
    counter!("querier_requests_total", "api" => api.clone(), "signal" => signal.clone()).increment(1);
    histogram!("querier_request_duration_seconds", "api" => api, "signal" => signal)
        .record(duration.as_secs_f64());
}

/// Record the scan volume of one executed physical plan: bytes read from Parquet
/// and the number of file groups opened, labelled by `signal`
/// (logs/metrics/traces, or `sql` for a mixed cross-signal scan). Emitted per
/// `collect`/`sql` execution, so a single served request may produce several
/// observations (day-shard split, `histogram_quantile`); acceptable for the p95
/// scan panels. A cache hit performs no scan and records nothing.
#[allow(clippy::cast_precision_loss)] // metric magnitudes are well under 2^53
pub fn record_scan(signal: &str, bytes_scanned: u64, files_opened: u64) {
    let signal = signal.to_string();
    histogram!("querier_bytes_scanned", "signal" => signal.clone()).record(bytes_scanned as f64);
    histogram!("querier_files_opened", "signal" => signal).record(files_opened as f64);
}

/// Record a result-cache lookup outcome. The dashboard's hit-ratio panel filters
/// `sol_querier_cache_requests_total{cache="result", result="hit|miss"}`, so both
/// labels are required for it to match.
pub fn record_cache(hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    counter!("querier_cache_requests_total", "cache" => "result", "result" => result).increment(1);
}

/// Record a coalesced execution (FR3 single-flight): a concurrent identical
/// query awaited the in-flight leader's result instead of executing the plan
/// again. Mirrors [`record_cache`]'s counter with `result="coalesced"` —
/// surfaced as `sol_querier_cache_requests_total`; the dashboard's hit-ratio
/// panel filters `result="hit|miss"`, so this extra variant does not skew it.
pub fn record_coalesced() {
    counter!("querier_cache_requests_total", "cache" => "result", "result" => "coalesced")
        .increment(1);
}

/// Set the current cache memory footprint (bytes).
#[allow(clippy::cast_precision_loss)]
pub fn set_cache_memory(bytes: u64) {
    gauge!("querier_cache_memory_bytes").set(bytes as f64);
}

/// Increment / decrement the in-flight query gauge.
pub fn inc_inflight() {
    gauge!("querier_inflight").increment(1.0);
}

/// Decrement the in-flight query gauge.
pub fn dec_inflight() {
    gauge!("querier_inflight").decrement(1.0);
}

/// RAII guard tracking one in-flight request: increments `query_inflight` on
/// creation, decrements it on drop. Held for the lifetime of a request (via the
/// routes wrapper) so the gauge reflects concurrent load even when a handler
/// returns early or errors.
pub struct InflightGuard;

impl InflightGuard {
    /// Mark a request as in-flight (increments the gauge); the count is released
    /// when the returned guard is dropped.
    #[must_use]
    pub fn new() -> Self {
        inc_inflight();
        Self
    }
}

impl Default for InflightGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        dec_inflight();
    }
}

/// Record an object-store request, flagging throttles (HTTP 503, NFR10).
pub fn record_objectstore(duration: Duration, throttled: bool) {
    counter!("objectstore_requests_total").increment(1);
    if throttled {
        counter!("objectstore_throttled_total").increment(1);
    }
    histogram!("objectstore_request_duration_seconds").record(duration.as_secs_f64());
}

/// Record a shed query (FR5 concurrency guardrail): a query could not obtain
/// an execution permit within the bounded wait and was rejected with 503.
/// Surfaced as `sol_querier_shed_total`.
pub fn record_shed() {
    counter!("querier_shed_total").increment(1);
}

/// Record a guardrail rejection (NFR9 — `reason` e.g. `range`/`bytes`).
pub fn record_rejected(reason: &str) {
    counter!("querier_rejected_total", "reason" => reason.to_string()).increment(1);
}

/// Record an unsupported query construct (`lang` e.g. promql, `construct`).
pub fn record_unsupported(lang: &str, construct: &str) {
    counter!("querier_unsupported_total", "lang" => lang.to_string(), "construct" => construct.to_string())
        .increment(1);
}

/// Record a compaction run (task 10): input/output file counts, rows merged,
/// and wall-clock duration.
pub fn record_compaction(files_input: u64, files_output: u64, rows: u64, duration: Duration) {
    counter!("compactor_files_input_total").increment(files_input);
    counter!("compactor_files_output_total").increment(files_output);
    counter!("compactor_rollup_rows_total").increment(rows);
    histogram!("compactor_duration_seconds").record(duration.as_secs_f64());
}

/// Record retention GC deletions.
pub fn record_retention_deleted(files: u64) {
    counter!("compactor_retention_deleted_total").increment(files);
}

/// Set the compactor lag (seconds behind the active partition boundary).
pub fn set_compactor_lag(seconds: f64) {
    gauge!("compactor_lag_seconds").set(seconds);
}

/// Record a query-frontend split (task 11): one split, into `shards` shards.
#[allow(clippy::cast_precision_loss)]
pub fn record_shard_split(shards: u64) {
    counter!("querier_shard_splits_total").increment(1);
    histogram!("querier_shards_per_query").record(shards as f64);
}

/// Record a per-shard cache lookup outcome (`result=hit|miss`).
pub fn record_shard_cache(hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    counter!("querier_shard_cache_requests_total", "result" => result).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::MetricKind;
    use metrics_util::debugging::DebuggingRecorder;

    fn has_metric(
        snapshot: &[(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            metrics_util::debugging::DebugValue,
        )],
        kind: MetricKind,
        name: &str,
    ) -> Option<Vec<(String, String)>> {
        snapshot
            .iter()
            .find(|(k, _, _, _)| k.kind() == kind && k.key().name() == name)
            .map(|(k, _, _, _)| {
                k.key()
                    .labels()
                    .map(|l| (l.key().to_string(), l.value().to_string()))
                    .collect()
            })
    }

    #[test]
    fn test_request_duration_histogram_emitted() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_request("prometheus", "metrics", Duration::from_millis(12));
        });
        let s = snap.snapshot().into_vec();
        let labels = has_metric(&s, MetricKind::Histogram, "querier_request_duration_seconds")
            .expect("duration histogram emitted");
        assert!(
            labels.contains(&("api".to_string(), "prometheus".to_string())),
            "labels: {labels:?}"
        );
        assert!(
            labels.contains(&("signal".to_string(), "metrics".to_string())),
            "labels: {labels:?}"
        );
        assert!(has_metric(&s, MetricKind::Counter, "querier_requests_total").is_some());
    }

    #[test]
    fn test_record_scan_emits_bytes_and_files() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_scan("metrics", 8192, 2);
        });
        let s = snap.snapshot().into_vec();
        let bytes = has_metric(&s, MetricKind::Histogram, "querier_bytes_scanned")
            .expect("bytes_scanned histogram emitted");
        assert!(
            bytes.contains(&("signal".to_string(), "metrics".to_string())),
            "labels: {bytes:?}"
        );
        let files = has_metric(&s, MetricKind::Histogram, "querier_files_opened")
            .expect("files_opened histogram emitted");
        assert!(
            files.contains(&("signal".to_string(), "metrics".to_string())),
            "labels: {files:?}"
        );
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
                && k.key().name() == "querier_cache_requests_total"
                && k.key()
                    .labels()
                    .any(|l| l.key() == "result" && l.value() == "hit")
                // the dashboard filters on cache="result"; it must be present
                && k.key()
                    .labels()
                    .any(|l| l.key() == "cache" && l.value() == "result")
        });
        assert_eq!(hits.count(), 1, "hit counter emitted with cache=result");
        let misses = s.iter().filter(|(k, _, _, _)| {
            k.kind() == MetricKind::Counter
                && k.key().name() == "querier_cache_requests_total"
                && k.key()
                    .labels()
                    .any(|l| l.key() == "result" && l.value() == "miss")
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
            has_metric(&s, MetricKind::Counter, "objectstore_throttled_total").is_some(),
            "503 throttle counter emitted"
        );
        assert!(has_metric(&s, MetricKind::Counter, "objectstore_requests_total").is_some());
        assert!(
            has_metric(
                &s,
                MetricKind::Histogram,
                "objectstore_request_duration_seconds"
            )
            .is_some()
        );
    }

    #[test]
    fn test_guardrail_reject_counter() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || record_rejected("range"));
        let s = snap.snapshot().into_vec();
        let labels = has_metric(&s, MetricKind::Counter, "querier_rejected_total")
            .expect("guardrail reject counter emitted");
        assert!(
            labels.contains(&("reason".to_string(), "range".to_string())),
            "labels: {labels:?}"
        );
    }

    #[test]
    fn test_unsupported_construct_counter() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || record_unsupported("promql", "subquery"));
        let s = snap.snapshot().into_vec();
        let labels = has_metric(&s, MetricKind::Counter, "querier_unsupported_total")
            .expect("unsupported construct counter emitted");
        assert!(
            labels.contains(&("lang".to_string(), "promql".to_string())),
            "labels: {labels:?}"
        );
        assert!(
            labels.contains(&("construct".to_string(), "subquery".to_string())),
            "labels: {labels:?}"
        );
    }
}
