use chrono::Utc;
use greptimedb_ingester::{api::v1::*, helpers::values::*};
use sol_lib::event::{MetricView, OtelMetric, ValueAtQuantile};

pub(super) struct RequestBuilderOptions {
    pub(super) use_new_naming: bool,
}

pub(super) const SUMMARY_STAT_FIELD_COUNT: usize = 2;
pub(super) const LEGACY_TIME_INDEX_COLUMN_NAME: &str = "ts";
pub(super) const TIME_INDEX_COLUMN_NAME: &str = "greptime_timestamp";
pub(super) const LEGACY_VALUE_COLUMN_NAME: &str = "val";
pub(super) const VALUE_COLUMN_NAME: &str = "greptime_value";

fn encode_f64_value(
    name: &str,
    value: f64,
    schema: &mut Vec<ColumnSchema>,
    columns: &mut Vec<Value>,
) {
    schema.push(f64_column(name));
    columns.push(f64_value(value));
}

pub fn metric_to_insert_request(
    metric: OtelMetric,
    options: &RequestBuilderOptions,
) -> RowInsertRequest {
    let ns = metric.namespace();
    let metric_name = metric.name();
    let table_name = if let Some(ns) = ns {
        format!("{ns}_{metric_name}")
    } else {
        metric_name.to_owned()
    };
    let mut schema = Vec::new();
    let mut columns = Vec::new();

    // timestamp
    let timestamp = metric
        .timestamp()
        .map(|t| t.timestamp_millis())
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    schema.push(ts_column(if options.use_new_naming {
        TIME_INDEX_COLUMN_NAME
    } else {
        LEGACY_TIME_INDEX_COLUMN_NAME
    }));
    columns.push(timestamp_millisecond_value(timestamp));

    // tags
    if let Some(tags) = metric.tags() {
        for (key, value) in tags.iter_single() {
            schema.push(tag_column(key));
            columns.push(string_value(value.unwrap_or("").to_owned()));
        }
    }

    // fields
    match metric.view() {
        MetricView::Sum { value } | MetricView::Gauge { value } => {
            encode_f64_value(
                if options.use_new_naming {
                    VALUE_COLUMN_NAME
                } else {
                    LEGACY_VALUE_COLUMN_NAME
                },
                value,
                &mut schema,
                &mut columns,
            );
        }
        MetricView::Set { values } => {
            encode_f64_value(
                if options.use_new_naming {
                    VALUE_COLUMN_NAME
                } else {
                    LEGACY_VALUE_COLUMN_NAME
                },
                {
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "set cardinality; precise for |v| <= 2^53"
                    )]
                    let v = values.len() as f64;
                    v
                },
                &mut schema,
                &mut columns,
            );
        }
        MetricView::Histogram {
            bounds,
            counts,
            count,
            sum,
        } => {
            encode_histogram(bounds, counts, &mut schema, &mut columns);
            #[expect(
                clippy::cast_precision_loss,
                reason = "metric count to f64; precise for |v| <= 2^53"
            )]
            let count_f64 = count as f64;
            encode_f64_value("count", count_f64, &mut schema, &mut columns);
            encode_f64_value("sum", sum, &mut schema, &mut columns);
        }
        MetricView::Summary {
            quantiles,
            count,
            sum,
        } => {
            encode_quantiles(quantiles, &mut schema, &mut columns);
            #[expect(
                clippy::cast_precision_loss,
                reason = "summary count to f64; precise for |v| <= 2^53"
            )]
            let count_f64 = count as f64;
            encode_f64_value("count", count_f64, &mut schema, &mut columns);
            encode_f64_value("sum", sum, &mut schema, &mut columns);
        }
        MetricView::ExponentialHistogram {
            count,
            sum,
            min,
            max,
            ..
        } => {
            #[expect(
                clippy::cast_precision_loss,
                reason = "exp-histogram count to f64; precise for |v| <= 2^53"
            )]
            let count_f64 = count as f64;
            encode_f64_value("count", count_f64, &mut schema, &mut columns);
            encode_f64_value("sum", sum, &mut schema, &mut columns);
            if let Some(mn) = min {
                encode_f64_value("min", mn, &mut schema, &mut columns);
            }
            if let Some(mx) = max {
                encode_f64_value("max", mx, &mut schema, &mut columns);
            }
        }
    }

    RowInsertRequest {
        table_name,
        rows: Some(Rows {
            schema,
            rows: vec![Row { values: columns }],
        }),
    }
}

fn encode_histogram(
    bounds: &[f64],
    counts: &[u64],
    schema: &mut Vec<ColumnSchema>,
    columns: &mut Vec<Value>,
) {
    for (&limit, &cnt) in bounds.iter().zip(counts.iter()) {
        let column_name = format!("b{limit}");
        #[expect(
            clippy::cast_precision_loss,
            reason = "bucket count to f64; precise for |v| <= 2^53"
        )]
        let cnt_f64 = cnt as f64;
        encode_f64_value(&column_name, cnt_f64, schema, columns);
    }
}

fn encode_quantiles(
    quantiles: &[ValueAtQuantile],
    schema: &mut Vec<ColumnSchema>,
    columns: &mut Vec<Value>,
) {
    for q in quantiles {
        let column_name = format!("p{:02}", q.quantile * 100f64);
        encode_f64_value(&column_name, q.value, schema, columns);
    }
}

fn f64_column(name: &str) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_owned(),
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::Float64 as i32,
        ..Default::default()
    }
}

fn ts_column(name: &str) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_owned(),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: ColumnDataType::TimestampMillisecond as i32,
        ..Default::default()
    }
}

fn tag_column(name: &str) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_owned(),
        semantic_type: SemanticType::Tag as i32,
        datatype: ColumnDataType::String as i32,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {

    use similar_asserts::assert_eq;

    use super::*;
    use crate::event::{OtelMetric, metric::MetricKind};

    fn get_column(rows: &Rows, name: &str) -> f64 {
        let (col_index, _) = rows
            .schema
            .iter()
            .enumerate()
            .find(|(_, c)| c.column_name == name)
            .unwrap();
        let value_data = rows.rows[0].values[col_index]
            .value_data
            .as_ref()
            .expect("null value");
        match value_data {
            value::ValueData::F64Value(v) => *v,
            _ => {
                unreachable!()
            }
        }
    }

    #[test]
    fn test_metric_data_to_insert_request() {
        let metric = OtelMetric::new_gauge("load1", 1.1)
            .with_namespace(Some("ns"))
            .with_tags(Some(
                vec![("host".to_owned(), "my_host".to_owned())]
                    .into_iter()
                    .collect(),
            ))
            .with_timestamp(Some(Utc::now()));

        let options = RequestBuilderOptions {
            use_new_naming: false,
        };

        let insert = metric_to_insert_request(metric, &options);

        assert_eq!(insert.table_name, "ns_load1");
        let rows = insert.rows.expect("Empty insert request");
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].values.len(), 3);

        let column_names = rows
            .schema
            .iter()
            .map(|c| c.column_name.as_ref())
            .collect::<Vec<&str>>();
        assert!(column_names.contains(&LEGACY_TIME_INDEX_COLUMN_NAME));
        assert!(column_names.contains(&"host"));
        assert!(column_names.contains(&LEGACY_VALUE_COLUMN_NAME));

        assert_eq!(get_column(&rows, LEGACY_VALUE_COLUMN_NAME), 1.1);

        let metric2 = OtelMetric::new_gauge("load1", 1.1);
        let insert2 = metric_to_insert_request(metric2, &options);
        assert_eq!(insert2.table_name, "load1");
    }

    #[test]
    fn test_metric_data_to_insert_request_new_naming() {
        let metric = OtelMetric::new_gauge("load1", 1.1)
            .with_namespace(Some("ns"))
            .with_tags(Some(
                vec![("host".to_owned(), "my_host".to_owned())]
                    .into_iter()
                    .collect(),
            ))
            .with_timestamp(Some(Utc::now()));

        let options = RequestBuilderOptions {
            use_new_naming: true,
        };

        let insert = metric_to_insert_request(metric, &options);

        assert_eq!(insert.table_name, "ns_load1");
        let rows = insert.rows.expect("Empty insert request");
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].values.len(), 3);

        let column_names = rows
            .schema
            .iter()
            .map(|c| c.column_name.as_ref())
            .collect::<Vec<&str>>();
        assert!(column_names.contains(&TIME_INDEX_COLUMN_NAME));
        assert!(column_names.contains(&"host"));
        assert!(column_names.contains(&VALUE_COLUMN_NAME));

        assert_eq!(get_column(&rows, VALUE_COLUMN_NAME), 1.1);

        let metric2 = OtelMetric::new_gauge("load1", 1.1);
        let insert2 = metric_to_insert_request(metric2, &options);
        assert_eq!(insert2.table_name, "load1");
    }

    #[test]
    fn test_counter() {
        let metric = OtelMetric::new_counter("cpu_seconds_total", MetricKind::Incremental, 1.1);
        let options = RequestBuilderOptions {
            use_new_naming: false,
        };

        let insert = metric_to_insert_request(metric, &options);
        let rows = insert.rows.expect("Empty insert request");
        assert_eq!(rows.rows[0].values.len(), 2);

        assert_eq!(get_column(&rows, LEGACY_VALUE_COLUMN_NAME), 1.1);
    }

    #[test]
    fn test_set() {
        let otel = OtelMetric::new_set_from_values(
            "cpu_seconds_total",
            MetricKind::Absolute,
            ["foo".to_owned(), "bar".to_owned()],
        );
        let options = RequestBuilderOptions {
            use_new_naming: false,
        };

        let insert = metric_to_insert_request(otel, &options);
        let rows = insert.rows.expect("Empty insert request");
        assert_eq!(rows.rows[0].values.len(), 2);

        assert_eq!(get_column(&rows, LEGACY_VALUE_COLUMN_NAME), 2.0);
    }

    #[test]
    fn test_distribution_as_histogram() {
        let samples = sol_lib::samples![1.0 => 2, 2.0 => 4, 3.0 => 2];
        let otel = OtelMetric::new_histogram_from_samples(
            "cpu_seconds_total",
            MetricKind::Incremental,
            &samples,
        );
        let options = RequestBuilderOptions {
            use_new_naming: false,
        };

        let insert = metric_to_insert_request(otel, &options);
        let rows = insert.rows.expect("Empty insert request");
        assert_eq!(rows.rows[0].values.len(), 1 + SUMMARY_STAT_FIELD_COUNT + 3);

        assert_eq!(get_column(&rows, "b1"), 2.0);
        assert_eq!(get_column(&rows, "b2"), 4.0);
        assert_eq!(get_column(&rows, "b3"), 2.0);
        assert_eq!(get_column(&rows, "count"), 8.0);
        assert_eq!(get_column(&rows, "sum"), 16.0);
    }

    #[test]
    fn test_histogram() {
        let buckets = sol_lib::buckets![1.0 => 1, 2.0 => 2, 3.0 => 1];
        let buckets_len = buckets.len();
        let otel = OtelMetric::new_histogram(
            "cpu_seconds_total",
            MetricKind::Incremental,
            &buckets,
            4,
            8.0,
        );
        let options = RequestBuilderOptions {
            use_new_naming: false,
        };

        let insert = metric_to_insert_request(otel, &options);
        let rows = insert.rows.expect("Empty insert request");
        assert_eq!(
            rows.rows[0].values.len(),
            1 + SUMMARY_STAT_FIELD_COUNT + buckets_len
        );

        assert_eq!(get_column(&rows, "b1"), 1.0);
        assert_eq!(get_column(&rows, "b2"), 2.0);
        assert_eq!(get_column(&rows, "b3"), 1.0);
        assert_eq!(get_column(&rows, "count"), 4.0);
        assert_eq!(get_column(&rows, "sum"), 8.0);
    }

    #[test]
    fn test_summary() {
        let quantiles = sol_lib::quantiles![0.01 => 1.5, 0.5 => 2.0, 0.99 => 3.0];
        let quantiles_len = quantiles.len();
        let otel = OtelMetric::new_summary("cpu_seconds_total", &quantiles, 6, 12.0);
        let options = RequestBuilderOptions {
            use_new_naming: false,
        };

        let insert = metric_to_insert_request(otel, &options);
        let rows = insert.rows.expect("Empty insert request");
        assert_eq!(
            rows.rows[0].values.len(),
            1 + SUMMARY_STAT_FIELD_COUNT + quantiles_len
        );

        assert_eq!(get_column(&rows, "p01"), 1.5);
        assert_eq!(get_column(&rows, "p50"), 2.0);
        assert_eq!(get_column(&rows, "p99"), 3.0);
        assert_eq!(get_column(&rows, "count"), 6.0);
        assert_eq!(get_column(&rows, "sum"), 12.0);
    }
}
