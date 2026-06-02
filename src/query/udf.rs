//! Query-side OTLP→Prometheus attribute lookup (`prom_attr`).
//!
//! Sol stores attributes with their raw OTLP keys (dotted, e.g. `http.route`,
//! `deployment.environment`). Grafana/Mimir query the **Prometheus-normalized**
//! name (`http_route`, `deployment_environment`) — Mimir normalizes on OTLP
//! ingest, Sol does not. `prom_attr(attributes_json, 'http_route')` bridges that
//! on the read side: it returns the value of the stored key whose normalized
//! form (`[^A-Za-z0-9_]` → `_`) equals the requested Prometheus name, so the
//! Prometheus API presents a normalized view over raw OTLP storage.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};

/// Normalize an OTLP attribute key to its Prometheus label name: every
/// character outside `[A-Za-z0-9_]` becomes `_` (matches the OTLP→Prometheus
/// normalization Mimir applies on ingest).
fn normalize(key: &str) -> String {
    key.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
}

/// Look up `prom_name` in a JSON attributes object by normalized key.
fn lookup(attributes_json: &str, prom_name: &str) -> Option<String> {
    let map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(attributes_json).ok()?;
    for (key, value) in &map {
        if normalize(key) == prom_name {
            return match value {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Null => None,
                other => Some(other.to_string()),
            };
        }
    }
    None
}

/// `prom_attr(attributes, prom_name) -> value | NULL`.
pub fn prom_attr_udf() -> ScalarUDF {
    let fun = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let attrs = arrays[0].as_any().downcast_ref::<StringArray>();
        let names = arrays[1].as_any().downcast_ref::<StringArray>();
        let (attrs, names) = match (attrs, names) {
            (Some(a), Some(n)) => (a, n),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "prom_attr expects (Utf8, Utf8) arguments".to_string(),
                ));
            }
        };
        let out: StringArray = (0..attrs.len())
            .map(|i| {
                if attrs.is_null(i) || names.is_null(i) {
                    None
                } else {
                    lookup(attrs.value(i), names.value(i))
                }
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    };
    create_udf(
        "prom_attr",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(fun),
    )
}

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
fn prom_metric_name(name: &str, unit: &str, is_monotonic: bool) -> String {
    let mut out: String =
        name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == ':' { c } else { '_' }).collect();
    if let Some(suffix) = unit_suffix(unit)
        && !out.ends_with(suffix)
    {
        out.push('_');
        out.push_str(suffix);
    }
    if is_monotonic && !out.ends_with("_total") {
        out.push_str("_total");
    }
    out
}

/// `prom_metric_name(name, unit, is_monotonic) -> normalized name`.
pub fn prom_metric_name_udf() -> ScalarUDF {
    let fun = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let names = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| datafusion::error::DataFusionError::Execution("name must be Utf8".into()))?;
        let units = arrays[1].as_any().downcast_ref::<StringArray>();
        let monotonic = arrays[2].as_any().downcast_ref::<BooleanArray>();
        let out: StringArray = (0..names.len())
            .map(|i| {
                if names.is_null(i) {
                    return None;
                }
                let unit = units.and_then(|u| (!u.is_null(i)).then(|| u.value(i))).unwrap_or("");
                let mono =
                    monotonic.and_then(|m| (!m.is_null(i)).then(|| m.value(i))).unwrap_or(false);
                Some(prom_metric_name(names.value(i), unit, mono))
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
    };
    create_udf(
        "prom_metric_name",
        vec![DataType::Utf8, DataType::Utf8, DataType::Boolean],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(fun),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prom_metric_name_normalization() {
        // gauge with bytes unit
        assert_eq!(prom_metric_name("process.memory.usage", "By", false), "process_memory_usage_bytes");
        // monotonic counter with time unit → _seconds_total
        assert_eq!(prom_metric_name("process.cpu.time", "s", true), "process_cpu_time_seconds_total");
        // counter, annotation unit → just _total
        assert_eq!(prom_metric_name("dotnet.exceptions", "{exception}", true), "dotnet_exceptions_total");
        // gauge, dimensionless → name only
        assert_eq!(prom_metric_name("process.thread.count", "1", false), "process_thread_count");
        // histogram base (no _total; _bucket/_count/_sum added by the histogram path)
        assert_eq!(
            prom_metric_name("http.server.request.duration", "s", false),
            "http_server_request_duration_seconds"
        );
    }

    #[test]
    fn test_normalize_otlp_to_prometheus() {
        assert_eq!(normalize("http.route"), "http_route");
        assert_eq!(normalize("deployment.environment"), "deployment_environment");
        assert_eq!(normalize("already_ok"), "already_ok");
        assert_eq!(normalize("url.scheme"), "url_scheme");
    }

    #[test]
    fn test_lookup_by_normalized_key() {
        let json = r#"{"http.route":"/x","deployment.environment":"dev","n":5}"#;
        assert_eq!(lookup(json, "http_route").as_deref(), Some("/x"));
        assert_eq!(lookup(json, "deployment_environment").as_deref(), Some("dev"));
        assert_eq!(lookup(json, "n").as_deref(), Some("5"));
        assert_eq!(lookup(json, "absent"), None);
        assert_eq!(lookup("not json", "x"), None);
    }
}
