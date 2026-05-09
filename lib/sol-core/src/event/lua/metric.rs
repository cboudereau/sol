use mlua::prelude::*;

use super::{
    super::{
        MetricKind, MetricView, OtelAttributes, OtelMetric,
        metric::{self},
    },
    util::{table_to_timestamp, timestamp_to_table},
};

pub struct LuaMetric {
    pub otel: OtelMetric,
    pub multi_value_tags: bool,
}

pub struct LuaOtelAttributes {
    pub attrs: OtelAttributes,
}

impl IntoLua for MetricKind {
    #![expect(clippy::wrong_self_convention, reason = "trait method name defined by mlua")]
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        let kind = match self {
            MetricKind::Absolute => "absolute",
            MetricKind::Incremental => "incremental",
        };
        lua.create_string(kind).map(LuaValue::String)
    }
}

impl FromLua for MetricKind {
    fn from_lua(value: LuaValue, _: &Lua) -> LuaResult<Self> {
        match value {
            LuaValue::String(s) if s == "absolute" => Ok(MetricKind::Absolute),
            LuaValue::String(s) if s == "incremental" => Ok(MetricKind::Incremental),
            _ => Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("MetricKind"),
                message: Some(
                    "Metric kind should be either \"incremental\" or \"absolute\"".to_string(),
                ),
            }),
        }
    }
}

impl FromLua for OtelAttributes {
    fn from_lua(value: LuaValue, _: &Lua) -> LuaResult<Self> {
        let LuaValue::Table(table) = value else {
            return Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("OtelAttributes"),
                message: Some("Expected a table for metric tags".to_string()),
            });
        };
        let mut attrs = OtelAttributes::new();
        for pair in table.pairs::<String, LuaValue>() {
            let (key, val) = pair?;
            match val {
                LuaValue::String(s) => {
                    attrs.insert_string(key, s.to_string_lossy().clone());
                }
                LuaValue::Nil => {}
                _ => {
                    attrs.insert_string(key, format!("{val:?}"));
                }
            }
        }
        Ok(attrs)
    }
}

impl IntoLua for LuaOtelAttributes {
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        Ok(LuaValue::Table(
            lua.create_table_from(self.attrs.into_iter_single())?,
        ))
    }
}

impl IntoLua for LuaMetric {
    #![expect(clippy::wrong_self_convention, reason = "trait method name defined by mlua")]
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        let tbl = lua.create_table()?;

        tbl.raw_set("name", self.otel.name())?;
        if let Some(namespace) = self.otel.namespace() {
            tbl.raw_set("namespace", namespace)?;
        }
        if let Some(ts) = self.otel.timestamp() {
            tbl.raw_set("timestamp", timestamp_to_table(lua, ts)?)?;
        }
        if let Some(i) = self.otel.interval_ms() {
            tbl.raw_set("interval_ms", i.get())?;
        }
        if let Some(attrs) = self.otel.tags() {
            tbl.raw_set("tags", LuaOtelAttributes { attrs })?;
        }
        tbl.raw_set("kind", self.otel.kind())?;

        match self.otel.view() {
            MetricView::Sum { value } => {
                let counter = lua.create_table()?;
                counter.raw_set("value", value)?;
                tbl.raw_set("counter", counter)?;
            }
            MetricView::Gauge { value } => {
                let gauge = lua.create_table()?;
                gauge.raw_set("value", value)?;
                tbl.raw_set("gauge", gauge)?;
            }
            MetricView::Set { values } => {
                let set = lua.create_table()?;
                set.raw_set("values", lua.create_sequence_from(values.into_iter())?)?;
                tbl.raw_set("set", set)?;
            }
            MetricView::Histogram {
                bounds,
                counts,
                count,
                sum,
            } => {
                let aggregated_histogram = lua.create_table()?;
                let count_vec: Vec<u64> = counts.to_vec();
                let bucket_vec: Vec<f64> = bounds.to_vec();
                aggregated_histogram.raw_set("buckets", bucket_vec)?;
                aggregated_histogram.raw_set("counts", count_vec)?;
                aggregated_histogram.raw_set("count", count)?;
                aggregated_histogram.raw_set("sum", sum)?;
                tbl.raw_set("aggregated_histogram", aggregated_histogram)?;
            }
            MetricView::Summary {
                quantiles,
                count,
                sum,
            } => {
                let aggregated_summary = lua.create_table()?;
                let values: Vec<f64> = quantiles.iter().map(|q| q.value).collect();
                let quantile_vec: Vec<f64> = quantiles.iter().map(|q| q.quantile).collect();
                aggregated_summary.raw_set("quantiles", quantile_vec)?;
                aggregated_summary.raw_set("values", values)?;
                aggregated_summary.raw_set("count", count)?;
                aggregated_summary.raw_set("sum", sum)?;
                tbl.raw_set("aggregated_summary", aggregated_summary)?;
            }
            MetricView::ExponentialHistogram {
                scale,
                count,
                sum,
                zero_count,
                ..
            } => {
                let exp_hist = lua.create_table()?;
                exp_hist.raw_set("scale", scale)?;
                exp_hist.raw_set("count", count)?;
                exp_hist.raw_set("sum", sum)?;
                exp_hist.raw_set("zero_count", zero_count)?;
                tbl.raw_set("exponential_histogram", exp_hist)?;
            }
        }

        Ok(LuaValue::Table(tbl))
    }
}

impl FromLua for OtelMetric {
    #[expect(clippy::too_many_lines, reason = "handles all metric data variants")]
    fn from_lua(value: LuaValue, _: &Lua) -> LuaResult<Self> {
        let table = match &value {
            LuaValue::Table(table) => table,
            other => {
                return Err(LuaError::FromLuaConversionError {
                    from: other.type_name(),
                    to: String::from("Metric"),
                    message: Some("Metric should be a Lua table".to_string()),
                });
            }
        };

        let name: String = table.raw_get("name")?;
        let timestamp = table
            .raw_get::<Option<LuaTable>>("timestamp")?
            .map(table_to_timestamp)
            .transpose()?;
        let interval_ms: Option<u32> = table.raw_get("interval_ms")?;
        let namespace: Option<String> = table.raw_get("namespace")?;
        let tags: Option<OtelAttributes> = table.raw_get("tags")?;
        let kind = table
            .raw_get::<Option<MetricKind>>("kind")?
            .unwrap_or(MetricKind::Absolute);

        let otel = if let Some(counter) = table.raw_get::<Option<LuaTable>>("counter")? {
            OtelMetric::new_counter(&name, kind, counter.raw_get::<f64>("value")?)
        } else if let Some(gauge) = table.raw_get::<Option<LuaTable>>("gauge")? {
            match kind {
                MetricKind::Absolute => OtelMetric::new_gauge(&name, gauge.raw_get::<f64>("value")?),
                MetricKind::Incremental => OtelMetric::new_gauge_delta(&name, gauge.raw_get::<f64>("value")?),
            }
        } else if let Some(set) = table.raw_get::<Option<LuaTable>>("set")? {
            let values: std::collections::BTreeSet<String> = set.raw_get("values")?;
            OtelMetric::new_set_from_values(&name, kind, values.into_iter().collect::<Vec<_>>())
        } else if let Some(distribution) = table.raw_get::<Option<LuaTable>>("distribution")? {
            let values: Vec<f64> = distribution.raw_get("values")?;
            let rates: Vec<u32> = distribution.raw_get("sample_rates")?;
            let samples = metric::zip_samples(values, rates);
            let _statistic: String = distribution.raw_get("statistic").unwrap_or_else(|_| "histogram".to_string());
            OtelMetric::new_histogram_from_samples(&name, kind, &samples)
        } else if let Some(aggregated_histogram) =
            table.raw_get::<Option<LuaTable>>("aggregated_histogram")?
        {
            let counts: Vec<u64> = aggregated_histogram.raw_get("counts")?;
            let buckets: Vec<f64> = aggregated_histogram.raw_get("buckets")?;
            let count = counts.iter().sum();
            let sum: f64 = aggregated_histogram.raw_get("sum")?;
            OtelMetric::new_histogram(&name, kind, &metric::zip_buckets(buckets, counts), count, sum)
        } else if let Some(aggregated_summary) =
            table.raw_get::<Option<LuaTable>>("aggregated_summary")?
        {
            let quantiles_vals: Vec<f64> = aggregated_summary.raw_get("quantiles")?;
            let values: Vec<f64> = aggregated_summary.raw_get("values")?;
            let count: u64 = aggregated_summary.raw_get("count")?;
            let sum: f64 = aggregated_summary.raw_get("sum")?;
            OtelMetric::new_summary(&name, &metric::zip_quantiles(quantiles_vals, values), count, sum)
        } else {
            return Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("Metric"),
                message: Some("Cannot find metric value, expected presence one of \"counter\", \"gauge\", \"set\", \"distribution\", \"aggregated_histogram\", \"aggregated_summary\"".to_string()),
            });
        }
        .with_namespace(namespace)
        .with_tags(tags)
        .with_timestamp(timestamp)
        .with_interval_ms(interval_ms.and_then(std::num::NonZeroU32::new));

        Ok(otel)
    }
}

#[cfg(test)]
mod test {
    use chrono::{Timelike, Utc, offset::TimeZone};
    use sol_common::assert_event_data_eq;

    use super::*;
    use crate::event::OtelMetric;

    fn assert_metric(metric: OtelMetric, multi_value_tags: bool, assertions: Vec<&'static str>) {
        let lua = Lua::new();
        lua.globals()
            .set(
                "metric",
                LuaMetric {
                    otel: metric,
                    multi_value_tags,
                },
            )
            .unwrap();

        for assertion in assertions {
            assert!(
                lua.load(assertion).eval::<bool>().expect(assertion),
                "{}",
                assertion
            );
        }
    }

    #[test]
    fn into_lua_counter_full() {
        let metric = OtelMetric::new_counter("example counter", MetricKind::Incremental, 1.0)
            .with_namespace(Some("namespace_example"))
            .with_tags(Some(crate::otel_tags!("example tag" => "example value")))
            .with_timestamp(Some(
                Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                    .single()
                    .and_then(|t| t.with_nanosecond(11))
                    .expect("invalid timestamp"),
            ));

        assert_metric(
            metric.clone(),
            false,
            vec![
                "type(metric) == 'table'",
                "metric.name == 'example counter'",
                "metric.namespace == 'namespace_example'",
                "type(metric.timestamp) == 'table'",
                "metric.timestamp.year == 2018",
                "metric.timestamp.month == 11",
                "metric.timestamp.day == 14",
                "metric.timestamp.hour == 8",
                "metric.timestamp.min == 9",
                "metric.timestamp.sec == 10",
                "type(metric.tags) == 'table'",
                "metric.tags['example tag'] == 'example value'",
                "metric.kind == 'incremental'",
                "type(metric.counter) == 'table'",
                "metric.counter.value == 1",
            ],
        );
        assert_metric(
            metric,
            true,
            vec![
                "type(metric) == 'table'",
                "metric.name == 'example counter'",
                "metric.namespace == 'namespace_example'",
                "type(metric.timestamp) == 'table'",
                "metric.timestamp.year == 2018",
                "metric.timestamp.month == 11",
                "metric.timestamp.day == 14",
                "metric.timestamp.hour == 8",
                "metric.timestamp.min == 9",
                "metric.timestamp.sec == 10",
                "type(metric.tags) == 'table'",
                "metric.tags['example tag'][1] == 'example value'",
                "metric.kind == 'incremental'",
                "type(metric.counter) == 'table'",
                "metric.counter.value == 1",
            ],
        );
    }

    #[test]
    fn read_multi_value_tag() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, any_value};
        let mut tags = OtelAttributes::default();
        tags.insert(
            "example tag".to_string(),
            AnyValue {
                value: Some(any_value::Value::ArrayValue(ArrayValue {
                    values: vec![
                        AnyValue {
                            value: Some(any_value::Value::StringValue("a".into())),
                        },
                        AnyValue {
                            value: Some(any_value::Value::StringValue("b".into())),
                        },
                    ],
                })),
            },
        );
        let metric = OtelMetric::new_counter("example counter", MetricKind::Incremental, 1.0)
            .with_tags(Some(tags));

        assert_metric(
            metric,
            true,
            vec![
                "type(metric.tags) == 'table'",
                "metric.tags['example tag'][1] == 'a'",
                "metric.tags['example tag'][2] == 'b'",
            ],
        );
    }

    #[test]
    fn into_lua_counter_minimal() {
        let metric = OtelMetric::new_counter("example counter", MetricKind::Absolute, 0.577_215_66);

        for multi_value_tags in [false, true] {
            assert_metric(
                metric.clone(),
                multi_value_tags,
                vec![
                    "metric.timestamp == nil",
                    "metric.tags == nil",
                    "metric.kind == 'absolute'",
                    "metric.counter.value == 0.57721566",
                ],
            );
        }
    }

    #[test]
    fn into_lua_gauge() {
        let metric = OtelMetric::new_gauge("example gauge", 1.618_033_9);
        assert_metric(
            metric,
            false,
            vec!["metric.gauge.value == 1.6180339", "metric.counter == nil"],
        );
    }

    #[test]
    fn into_lua_set() {
        let metric = OtelMetric::new_set_from_values(
            "example set",
            MetricKind::Incremental,
            vec!["value", "another value"],
        );
        assert_metric(
            metric,
            false,
            vec![
                "type(metric.set) == 'table'",
                "type(metric.set.values) == 'table'",
                "#metric.set.values == 2",
                "metric.set.values[1] == 'another value'",
                "metric.set.values[2] == 'value'",
            ],
        );
    }

    #[expect(clippy::useless_vec, reason = "vec needed for slice comparison in test")]
    #[test]
    fn into_lua_distribution() {
        let metric = OtelMetric::new_histogram_from_samples(
            "example distribution",
            MetricKind::Incremental,
            &crate::samples![1.0 => 10, 1.0 => 20],
        );
        assert_metric(
            metric,
            false,
            vec![
                "type(metric.aggregated_histogram) == 'table'",
                "metric.aggregated_histogram.count == 30",
                "metric.aggregated_histogram.sum == 30",
            ],
        );
    }

    #[test]
    fn into_lua_aggregated_histogram() {
        let buckets = crate::buckets![1.0 => 20, 2.0 => 10, 4.0 => 45, 8.0 => 12];
        let metric = OtelMetric::new_histogram(
            "example histogram",
            MetricKind::Incremental,
            &buckets,
            87,
            975.2,
        );
        assert_metric(
            metric,
            false,
            vec![
                "type(metric.aggregated_histogram) == 'table'",
                "#metric.aggregated_histogram.buckets == 4",
                "metric.aggregated_histogram.buckets[1] == 1",
                "metric.aggregated_histogram.buckets[4] == 8",
                "#metric.aggregated_histogram.counts == 5",
                "metric.aggregated_histogram.counts[1] == 20",
                "metric.aggregated_histogram.counts[4] == 12",
                "metric.aggregated_histogram.counts[5] == 0",
                "metric.aggregated_histogram.count == 87",
                "metric.aggregated_histogram.sum == 975.2",
            ],
        );
    }

    #[test]
    fn into_lua_aggregated_summary() {
        let quantiles = crate::quantiles![
            0.1 => 2.0, 0.25 => 3.0, 0.5 => 5.0, 0.75 => 8.0, 0.9 => 7.0, 0.99 => 9.0, 1.0 => 10.0
        ];
        let metric = OtelMetric::new_summary("example summary", &quantiles, 197, 975.2);

        assert_metric(
            metric,
            false,
            vec![
                "type(metric.aggregated_summary) == 'table'",
                "#metric.aggregated_summary.quantiles == 7",
                "metric.aggregated_summary.quantiles[2] == 0.25",
                "#metric.aggregated_summary.values == 7",
                "metric.aggregated_summary.values[3] == 5",
                "metric.aggregated_summary.count == 197",
                "metric.aggregated_summary.sum == 975.2",
            ],
        );
    }

    #[test]
    fn from_lua_counter_minimal() {
        let value = r#"{
            name = "example counter",
            counter = {
                value = 0.57721566
            }
        }"#;
        let expected =
            OtelMetric::new_counter("example counter", MetricKind::Absolute, 0.577_215_66);
        assert_event_data_eq!(
            Lua::new().load(value).eval::<OtelMetric>().unwrap(),
            expected
        );
    }

    #[test]
    fn from_lua_counter_full() {
        let value = r#"{
            name = "example counter",
            namespace = "example_namespace",
            timestamp = {
                year = 2018,
                month = 11,
                day = 14,
                hour = 8,
                min = 9,
                sec = 10
            },
            tags = {
                ["example tag"] = "example value"
            },
            kind = "incremental",
            counter = {
                value = 1
            }
        }"#;
        let expected = OtelMetric::new_counter("example counter", MetricKind::Incremental, 1.0)
            .with_namespace(Some("example_namespace"))
            .with_tags(Some(crate::otel_tags!("example tag" => "example value")))
            .with_timestamp(Some(
                Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                    .single()
                    .expect("invalid timestamp"),
            ));
        assert_event_data_eq!(
            Lua::new().load(value).eval::<OtelMetric>().unwrap(),
            expected
        );
    }

    #[test]
    fn set_multi_valued_tags() {
        let value = r#"{
            name = "example counter",
            namespace = "example_namespace",
            timestamp = {
                year = 2018,
                month = 11,
                day = 14,
                hour = 8,
                min = 9,
                sec = 10
            },
            tags = {
                ["example tag"] = {"a", "b"}
            },
            kind = "incremental",
            counter = {
                value = 1
            }
        }"#;
        let mut tags = OtelAttributes::default();
        {
            use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, any_value};
            tags.insert(
                "example tag".to_string(),
                AnyValue {
                    value: Some(any_value::Value::ArrayValue(ArrayValue {
                        values: vec![
                            AnyValue {
                                value: Some(any_value::Value::StringValue("a".into())),
                            },
                            AnyValue {
                                value: Some(any_value::Value::StringValue("b".into())),
                            },
                        ],
                    })),
                },
            );
        }
        let expected = OtelMetric::new_counter("example counter", MetricKind::Incremental, 1.0)
            .with_namespace(Some("example_namespace"))
            .with_tags(Some(tags))
            .with_timestamp(Some(
                Utc.with_ymd_and_hms(2018, 11, 14, 8, 9, 10)
                    .single()
                    .expect("invalid timestamp"),
            ));
        assert_event_data_eq!(
            Lua::new().load(value).eval::<OtelMetric>().unwrap(),
            expected
        );
    }

    #[test]
    fn from_lua_gauge() {
        let value = r#"{
            name = "example gauge",
            gauge = {
                value = 1.6180339
            }
        }"#;
        let expected = OtelMetric::new_gauge("example gauge", 1.618_033_9);
        assert_event_data_eq!(
            Lua::new().load(value).eval::<OtelMetric>().unwrap(),
            expected
        );
    }

    #[test]
    fn from_lua_set() {
        let value = r#"{
            name = "example set",
            set = {
                values = { "value", "another value" }
            }
        }"#;
        let expected = OtelMetric::new_set_from_values(
            "example set",
            MetricKind::Absolute,
            vec!["value", "another value"],
        );
        assert_event_data_eq!(
            Lua::new().load(value).eval::<OtelMetric>().unwrap(),
            expected
        );
    }

    #[expect(clippy::useless_vec, reason = "vec needed for slice comparison in test")]
    #[test]
    fn from_lua_distribution() {
        let value = r#"{
            name = "example distribution",
            distribution = {
                values = { 1.0, 1.0 },
                sample_rates = { 10, 20 },
                statistic = "histogram"
            }
        }"#;
        let expected = OtelMetric::new_histogram_from_samples(
            "example distribution",
            MetricKind::Absolute,
            &crate::samples![1.0 => 10, 1.0 => 20],
        );
        assert_event_data_eq!(
            Lua::new().load(value).eval::<OtelMetric>().unwrap(),
            expected
        );
    }

    #[test]
    fn from_lua_aggregated_histogram() {
        let value = r#"{
            name = "example histogram",
            aggregated_histogram = {
                buckets = { 1, 2, 4, 8 },
                counts = { 20, 10, 45, 12 },
                sum = 975.2
            }
        }"#;
        let buckets = crate::buckets![1.0 => 20, 2.0 => 10, 4.0 => 45, 8.0 => 12];
        let expected = OtelMetric::new_histogram(
            "example histogram",
            MetricKind::Absolute,
            &buckets,
            87,
            975.2,
        );
        assert_event_data_eq!(
            Lua::new().load(value).eval::<OtelMetric>().unwrap(),
            expected
        );
    }

    #[test]
    fn from_lua_aggregated_summary() {
        let value = r#"{
            name = "example summary",
            aggregated_summary = {
                quantiles = { 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0 },
                values = { 2.0, 3.0, 5.0, 8.0, 7.0, 9.0, 10.0 },
                count = 197,
                sum = 975.2
            }
        }"#;
        let quantiles = crate::quantiles![
            0.1 => 2.0, 0.25 => 3.0, 0.5 => 5.0, 0.75 => 8.0, 0.9 => 7.0, 0.99 => 9.0, 1.0 => 10.0
        ];
        let expected = OtelMetric::new_summary("example summary", &quantiles, 197, 975.2);
        assert_event_data_eq!(
            Lua::new().load(value).eval::<OtelMetric>().unwrap(),
            expected
        );
    }
}
