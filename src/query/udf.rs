//! Custom scalar UDFs for the query engine.
//!
//! `json_get_str(json, key)` extracts a top-level string field from a JSON-string
//! column (the codec stores `attributes`/`resource_attributes`/`scope_attributes`
//! as JSON UTF8, ADR 0038). Backed by `serde_json` — DataFusion core has no JSON
//! extraction and we do not add a new crate (see task 3 discovery).

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};

/// Build the `json_get_str(json_utf8, key_utf8) -> utf8` scalar UDF.
pub fn json_get_str_udf() -> ScalarUDF {
    create_udf(
        "json_get_str",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(impl_json_get_str),
    )
}

fn impl_json_get_str(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let json = arrays[0].as_string::<i32>();
    let key = arrays[1].as_string::<i32>();
    let mut builder = StringBuilder::with_capacity(json.len(), json.len() * 8);
    for i in 0..json.len() {
        if json.is_null(i) || key.is_null(i) {
            builder.append_null();
            continue;
        }
        let extracted = serde_json::from_str::<serde_json::Value>(json.value(i))
            .ok()
            .and_then(|v| {
                v.get(key.value(i)).map(|field| match field {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            });
        match extracted {
            Some(s) => builder.append_value(s),
            None => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_json_get_str_extracts_field() {
        let ctx = SessionContext::new();
        ctx.register_udf(json_get_str_udf());
        let batches = ctx
            .sql(r#"SELECT json_get_str('{"service_version":"1.0.0","n":5}', 'service_version') AS a,
                           json_get_str('{"n":5}', 'service_version') AS b,
                           json_get_str('{"n":5}', 'n') AS c"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row = &batches[0];
        let a = row.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let b = row.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let c = row.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.value(0), "1.0.0");
        assert!(b.is_null(0)); // missing key → null
        assert_eq!(c.value(0), "5"); // non-string field stringified
    }
}
