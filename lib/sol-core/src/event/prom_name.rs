// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! OTLP → Prometheus/Mimir metric-name normalization.
//!
//! Single source of truth shared by the write path (the Parquet codec
//! materializes a `prom_name` column from this) and — historically — the read
//! path. Mirrors Mimir's `-distributor.otel-metric-suffixes-enabled`: sanitize
//! the name, append the UCUM→Prometheus unit suffix, then `_total` for monotonic
//! counters. Token-aware dedup keeps it idempotent (a name already carrying its
//! unit/`_total`, e.g. node-exporter `cpu_seconds_total`, is left unchanged).

/// UCUM → Prometheus unit suffix (the subset Mimir's OTLP ingest applies).
/// `None` means no suffix (dimensionless `1`/empty, or an annotation `{…}`).
fn unit_suffix(unit: &str) -> Option<&'static str> {
    match unit.trim() {
        "" | "1" => None,
        u if u.starts_with('{') => None, // annotation-only unit, e.g. {thread}
        "s" => Some("seconds"),
        "ms" => Some("milliseconds"),
        "us" | "µs" => Some("microseconds"),
        "ns" => Some("nanoseconds"),
        "min" => Some("minutes"),
        "h" => Some("hours"),
        "d" => Some("days"),
        "By" | "bytes" => Some("bytes"),
        "KiBy" => Some("kibibytes"),
        "MiBy" => Some("mebibytes"),
        "GiBy" => Some("gibibytes"),
        "KBy" => Some("kilobytes"),
        "MBy" => Some("megabytes"),
        "%" => Some("percent"),
        "Cel" => Some("celsius"),
        "Hz" => Some("hertz"),
        "V" => Some("volts"),
        "A" => Some("amperes"),
        "W" => Some("watts"),
        "J" => Some("joules"),
        _ => None, // unknown unit → no suffix (conservative; avoids wrong names)
    }
}

/// Normalize an OTLP metric name to its Mimir/Prometheus form: sanitize
/// (`[^A-Za-z0-9_:]`→`_`), append the unit suffix, then `_total` for monotonic
/// counters — matching `-distributor.otel-metric-suffixes-enabled`.
#[must_use]
pub fn prom_metric_name(name: &str, unit: &str, is_monotonic: bool) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect();
    // Append the unit suffix unless the name already carries it as a `_`-delimited
    // token. A plain `ends_with` check is not enough: the unit token can sit
    // *before* a trailing `_total` (node-exporter style, e.g. `cpu_seconds_total`),
    // which `ends_with("seconds")` misses → it would double to
    // `cpu_seconds_total_seconds_total`. Token-presence dedup mirrors OTel's
    // `BuildCompliantName` (it skips a unit token already present anywhere) and is
    // idempotent regardless of where the existing suffix sits.
    if let Some(suffix) = unit_suffix(unit)
        && !out.split('_').any(|tok| tok == suffix)
    {
        out.push('_');
        out.push_str(suffix);
    }
    if is_monotonic && !out.ends_with("_total") {
        out.push_str("_total");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prom_metric_name_normalization() {
        // gauge with bytes unit
        assert_eq!(
            prom_metric_name("process.memory.usage", "By", false),
            "process_memory_usage_bytes"
        );
        // monotonic counter with time unit → _seconds_total
        assert_eq!(
            prom_metric_name("process.cpu.time", "s", true),
            "process_cpu_time_seconds_total"
        );
        // counter, annotation unit → just _total
        assert_eq!(
            prom_metric_name("dotnet.exceptions", "{exception}", true),
            "dotnet_exceptions_total"
        );
        // gauge, dimensionless → name only
        assert_eq!(
            prom_metric_name("process.thread.count", "1", false),
            "process_thread_count"
        );
        // histogram base (no _total; _bucket/_count/_sum added by the histogram path)
        assert_eq!(
            prom_metric_name("http.server.request.duration", "s", false),
            "http_server_request_duration_seconds"
        );
    }

    #[test]
    fn test_prom_metric_name_does_not_double_existing_unit_suffix() {
        // node-exporter-style names already embed unit + `_total`: the unit token
        // sits before the trailing `_total`, so it must not be re-appended.
        assert_eq!(
            prom_metric_name("node_cpu_seconds_total", "s", true),
            "node_cpu_seconds_total"
        );
        assert_eq!(
            prom_metric_name("node_disk_read_bytes_total", "By", true),
            "node_disk_read_bytes_total"
        );
        // unit token present mid-name on a gauge stays single, too.
        assert_eq!(
            prom_metric_name("node_memory_total_bytes", "By", false),
            "node_memory_total_bytes"
        );
        // idempotent: normalizing an already-normalized name is a no-op.
        assert_eq!(
            prom_metric_name("process_cpu_time_seconds_total", "s", true),
            "process_cpu_time_seconds_total"
        );
    }
}
