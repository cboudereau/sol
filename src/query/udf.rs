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

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
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

#[cfg(test)]
mod tests {
    use super::*;

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
