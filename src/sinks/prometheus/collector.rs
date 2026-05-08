use std::{collections::BTreeMap, fmt::Write as _};

use chrono::Utc;
use indexmap::map::IndexMap;
use sol_lib::{
    event::OtelAttributes,
    prometheus::parser::{METRIC_NAME_LABEL, proto},
};

use crate::{
    event::{MetricView, OtelMetric, metric::MetricKind},
    sinks::util::encode_namespace,
};

pub(super) trait MetricCollector {
    type Output;

    fn new() -> Self;

    fn emit_metadata(
        &mut self,
        name: &str,
        fullname: &str,
        view: &MetricView<'_>,
        metric: &OtelMetric,
    );

    fn emit_value(
        &mut self,
        timestamp_millis: Option<i64>,
        name: &str,
        suffix: &str,
        value: f64,
        tags: Option<&OtelAttributes>,
        extra: Option<(&str, String)>,
    );

    fn finish(self) -> Self::Output;

    #[expect(
        clippy::cast_precision_loss,
        reason = "metric counter/bucket values to f64 for Prometheus exposition; precise for |v| <= 2^53"
    )]
    fn encode_metric(
        &mut self,
        default_namespace: Option<&str>,
        _buckets: &[f64],
        _quantiles: &[f64],
        metric: &OtelMetric,
    ) {
        let name = encode_namespace(metric.namespace().or(default_namespace), '_', metric.name());
        let name = &name;
        let timestamp = metric.timestamp().map(|t| t.timestamp_millis());
        let view = metric.view();

        if metric.kind() == MetricKind::Absolute {
            let event_tags = metric.tags();
            let tags = event_tags.as_ref();
            self.emit_metadata(metric.name(), name, &view, metric);

            match &view {
                MetricView::Sum { value } => {
                    self.emit_value(timestamp, name, "", *value, tags, None);
                }
                MetricView::Gauge { value } => {
                    self.emit_value(timestamp, name, "", *value, tags, None);
                }
                MetricView::Set { values } => {
                    self.emit_value(timestamp, name, "", values.len() as f64, tags, None);
                }
                MetricView::Histogram {
                    bounds,
                    counts,
                    count,
                    sum,
                } => {
                    let mut bucket_count = 0.0;
                    for (&limit, &cnt) in bounds.iter().zip(counts.iter()) {
                        if limit.is_infinite() {
                            continue;
                        }

                        bucket_count += cnt as f64;
                        self.emit_value(
                            timestamp,
                            name,
                            "_bucket",
                            bucket_count,
                            tags,
                            Some(("le", limit.to_string())),
                        );
                    }
                    self.emit_value(
                        timestamp,
                        name,
                        "_bucket",
                        *count as f64,
                        tags,
                        Some(("le", "+Inf".to_string())),
                    );
                    self.emit_value(timestamp, name, "_sum", *sum, tags, None);
                    self.emit_value(timestamp, name, "_count", *count as f64, tags, None);
                }
                MetricView::Summary {
                    quantiles: qs,
                    count,
                    sum,
                } => {
                    for q in *qs {
                        self.emit_value(
                            timestamp,
                            name,
                            "",
                            q.value,
                            tags,
                            Some(("quantile", q.quantile.to_string())),
                        );
                    }
                    self.emit_value(timestamp, name, "_sum", *sum, tags, None);
                    self.emit_value(timestamp, name, "_count", *count as f64, tags, None);
                }
                MetricView::ExponentialHistogram {
                    scale,
                    count,
                    sum,
                    zero_count,
                    positive,
                    negative,
                    ..
                } => {
                    let base = (2.0_f64).powf((2.0_f64).powi(-scale));
                    let mut explicit: Vec<(f64, f64)> = Vec::new();

                    if let Some(neg) = negative {
                        for (i, &c) in neg.bucket_counts.iter().enumerate().rev() {
                            #[expect(
                                clippy::cast_possible_wrap,
                                reason = "bucket index fits in i64"
                            )]
                            let idx = i64::from(neg.offset) + i as i64;
                            let upper = -base.powf(idx as f64);
                            explicit.push((upper, c as f64));
                        }
                    }

                    if *zero_count > 0 {
                        explicit.push((0.0, *zero_count as f64));
                    }

                    if let Some(pos) = positive {
                        for (i, &c) in pos.bucket_counts.iter().enumerate() {
                            #[expect(
                                clippy::cast_possible_wrap,
                                reason = "bucket index fits in i64"
                            )]
                            let idx = i64::from(pos.offset) + i as i64 + 1;
                            let upper = base.powf(idx as f64);
                            explicit.push((upper, c as f64));
                        }
                    }

                    let mut cumulative = 0.0;
                    for &(limit, cnt) in &explicit {
                        cumulative += cnt;
                        self.emit_value(
                            timestamp,
                            name,
                            "_bucket",
                            cumulative,
                            tags,
                            Some(("le", limit.to_string())),
                        );
                    }
                    self.emit_value(
                        timestamp,
                        name,
                        "_bucket",
                        *count as f64,
                        tags,
                        Some(("le", "+Inf".to_string())),
                    );
                    self.emit_value(timestamp, name, "_sum", *sum, tags, None);
                    self.emit_value(timestamp, name, "_count", *count as f64, tags, None);
                }
            }
        }
    }
}

pub(super) struct StringCollector {
    // BTreeMap ensures we get sorted output, which whilst not required is preferable
    processed: BTreeMap<String, String>,
}

impl MetricCollector for StringCollector {
    type Output = String;

    fn new() -> Self {
        let processed = BTreeMap::new();
        Self { processed }
    }

    fn emit_metadata(
        &mut self,
        name: &str,
        fullname: &str,
        view: &MetricView<'_>,
        metric: &OtelMetric,
    ) {
        if !self.processed.contains_key(fullname) {
            let header = Self::encode_header(name, fullname, view, metric);
            self.processed.insert(fullname.into(), header);
        }
    }

    fn emit_value(
        &mut self,
        timestamp_millis: Option<i64>,
        name: &str,
        suffix: &str,
        value: f64,
        tags: Option<&OtelAttributes>,
        extra: Option<(&str, String)>,
    ) {
        let result = self
            .processed
            .get_mut(name)
            .expect("metric metadata not encoded");

        result.push_str(name);
        result.push_str(suffix);
        Self::encode_tags(result, tags, extra);
        _ = match timestamp_millis {
            None => writeln!(result, " {value}"),
            Some(timestamp) => writeln!(result, " {value} {timestamp}"),
        };
    }

    fn finish(self) -> String {
        self.processed.into_values().collect()
    }
}

impl StringCollector {
    fn encode_tags(
        result: &mut String,
        tags: Option<&OtelAttributes>,
        extra: Option<(&str, String)>,
    ) {
        match (tags, extra) {
            (None, None) => Ok(()),
            (None, Some(tag)) => write!(result, "{{{}}}", Self::format_tag(tag.0, &tag.1)),
            (Some(tags), ref tag) => {
                let mut parts = tags
                    .iter_single()
                    .map(|(key, value)| Self::format_tag(key, value.unwrap_or("")))
                    .collect::<Vec<_>>();

                if let Some((key, value)) = tag {
                    parts.push(Self::format_tag(key, value))
                }

                parts.sort();
                write!(result, "{{{}}}", parts.join(","))
            }
        }
        .ok();
    }

    fn encode_header(
        name: &str,
        fullname: &str,
        view: &MetricView<'_>,
        metric: &OtelMetric,
    ) -> String {
        let r#type = prometheus_metric_type(view, metric).as_str();
        format!("# HELP {fullname} {name}\n# TYPE {fullname} {type}\n")
    }

    fn format_tag(key: &str, mut value: &str) -> String {
        // For most tags, this is just `{KEY}="{VALUE}"` so allocate optimistically
        let mut result = String::with_capacity(key.len() + value.len() + 3);
        result.push_str(key);
        result.push_str("=\"");
        while let Some(i) = value.find(['\\', '"']) {
            result.push_str(&value[..i]);
            result.push('\\');
            // Ugly but works because we know the character at `i` is ASCII
            result.push(value.as_bytes()[i] as char);
            value = &value[i + 1..];
        }
        result.push_str(value);
        result.push('"');
        result
    }
}

type Labels = Vec<proto::Label>;

pub(super) struct TimeSeries {
    buffer: IndexMap<Labels, Vec<proto::Sample>>,
    metadata: IndexMap<String, proto::MetricMetadata>,
    timestamp: Option<i64>,
}

impl TimeSeries {
    fn make_labels(
        tags: Option<&OtelAttributes>,
        name: &str,
        suffix: &str,
        extra: Option<(&str, String)>,
    ) -> Labels {
        // Each Prometheus metric is grouped by its labels, which
        // contains all the labels from the source metric, plus the name
        // label for the actual metric name. For convenience below, an
        // optional extra tag is added.
        let mut labels = tags.cloned().unwrap_or_default();
        labels.replace_string(METRIC_NAME_LABEL.into(), [name, suffix].join(""));
        if let Some((name, value)) = extra {
            labels.replace_string(name.into(), value);
        }

        // Extract the labels into a vec and sort to produce a
        // consistent key for the buffer.
        let mut labels = labels
            .into_iter_single()
            .map(|(name, value)| proto::Label { name, value })
            .collect::<Labels>();
        labels.sort();
        labels
    }

    fn default_timestamp(&mut self) -> i64 {
        *self
            .timestamp
            .get_or_insert_with(|| Utc::now().timestamp_millis())
    }
}

impl MetricCollector for TimeSeries {
    type Output = proto::WriteRequest;

    fn new() -> Self {
        Self {
            buffer: Default::default(),
            metadata: Default::default(),
            timestamp: None,
        }
    }

    fn emit_metadata(
        &mut self,
        name: &str,
        fullname: &str,
        view: &MetricView<'_>,
        metric: &OtelMetric,
    ) {
        if !self.metadata.contains_key(name) {
            let r#type = prometheus_metric_type(view, metric);
            let metadata = proto::MetricMetadata {
                r#type: r#type as i32,
                metric_family_name: fullname.into(),
                help: name.into(),
                unit: String::new(),
            };
            self.metadata.insert(name.into(), metadata);
        }
    }

    fn emit_value(
        &mut self,
        timestamp_millis: Option<i64>,
        name: &str,
        suffix: &str,
        value: f64,
        tags: Option<&OtelAttributes>,
        extra: Option<(&str, String)>,
    ) {
        let timestamp = timestamp_millis.unwrap_or_else(|| self.default_timestamp());
        self.buffer
            .entry(Self::make_labels(tags, name, suffix, extra))
            .or_default()
            .push(proto::Sample { value, timestamp });
    }

    fn finish(self) -> proto::WriteRequest {
        let timeseries = self
            .buffer
            .into_iter()
            .map(|(labels, samples)| proto::TimeSeries { labels, samples })
            .collect::<Vec<_>>();
        let metadata = self
            .metadata
            .into_iter()
            .map(|(_, metadata)| metadata)
            .collect();
        proto::WriteRequest {
            timeseries,
            metadata,
        }
    }
}

const fn prometheus_metric_type(view: &MetricView<'_>, _metric: &OtelMetric) -> proto::MetricType {
    use proto::MetricType;
    match view {
        MetricView::Sum { .. } => MetricType::Counter,
        MetricView::Gauge { .. } | MetricView::Set { .. } => MetricType::Gauge,
        MetricView::Histogram { .. } | MetricView::ExponentialHistogram { .. } => {
            MetricType::Histogram
        }
        MetricView::Summary { .. } => MetricType::Summary,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Timelike};
    use indoc::indoc;
    use similar_asserts::assert_eq;
    use sol_lib::otel_tags;

    use super::*;
    use crate::{
        event::OtelMetric, event::metric::MetricKind, test_util::stats::VariableHistogram,
    };

    fn encode_one<T: MetricCollector>(
        default_namespace: Option<&str>,
        buckets: &[f64],
        quantiles: &[f64],
        metric: OtelMetric,
    ) -> T::Output {
        let mut s = T::new();
        s.encode_metric(default_namespace, buckets, quantiles, &metric);
        s.finish()
    }

    fn tags() -> OtelAttributes {
        otel_tags!("code" => "200")
    }

    macro_rules! write_request {
        ( $name:literal, $help:literal, $type:ident
          [ $(
              $suffix:literal @ $timestamp:literal = $svalue:literal
                  [ $( $label:literal => $lvalue:literal ),* ]
          ),* ]
        ) => {
            proto::WriteRequest {
                timeseries: vec![
                    $(
                        proto::TimeSeries {
                            labels: vec![
                                proto::Label {
                                    name: "__name__".into(),
                                    value: format!("{}{}", $name, $suffix),
                                },
                                $(
                                    proto::Label {
                                        name: $label.into(),
                                        value: $lvalue.into(),
                                    },
                                )*
                            ],
                            samples: vec![ proto::Sample {
                                value: $svalue,
                                timestamp: $timestamp,
                            }],
                        },
                    )*
                ],
                metadata: vec![proto::MetricMetadata {
                    r#type: proto::metric_metadata::MetricType::$type as i32,
                    metric_family_name: $name.into(),
                    help: $help.into(),
                    unit: "".into(),
                }],
            }
        };
    }

    #[test]
    fn encodes_counter_text() {
        assert_eq!(
            encode_counter::<StringCollector>(),
            indoc! { r#"
                # HELP vector_hits hits
                # TYPE vector_hits counter
                vector_hits{code="200"} 10 1612325106789
            "#}
        );
    }

    #[test]
    fn encodes_counter_request() {
        assert_eq!(
            encode_counter::<TimeSeries>(),
            write_request!("vector_hits", "hits", Counter ["" @ 1612325106789 = 10.0 ["code" => "200"]])
        );
    }

    fn encode_counter<T: MetricCollector>() -> T::Output {
        let otel = OtelMetric::new_counter("hits", MetricKind::Absolute, 10.0)
            .with_tags(Some(tags()))
            .with_timestamp(Some(timestamp()));
        encode_one::<T>(Some("vector"), &[], &[], otel)
    }

    #[test]
    fn encodes_gauge_text() {
        assert_eq!(
            encode_gauge::<StringCollector>(),
            indoc! { r#"
                # HELP vector_temperature temperature
                # TYPE vector_temperature gauge
                vector_temperature{code="200"} -1.1 1612325106789
            "#}
        );
    }

    #[test]
    fn encodes_gauge_request() {
        assert_eq!(
            encode_gauge::<TimeSeries>(),
            write_request!("vector_temperature", "temperature", Gauge ["" @ 1612325106789 = -1.1 ["code" => "200"]])
        );
    }

    fn encode_gauge<T: MetricCollector>() -> T::Output {
        let otel = OtelMetric::new_gauge("temperature", -1.1)
            .with_tags(Some(tags()))
            .with_timestamp(Some(timestamp()));
        encode_one::<T>(Some("vector"), &[], &[], otel)
    }

    #[test]
    fn encodes_set_text() {
        assert_eq!(
            encode_set::<StringCollector>(),
            indoc! { r"
                # HELP vector_users users
                # TYPE vector_users gauge
                vector_users 1 1612325106789
            "}
        );
    }

    #[test]
    fn encodes_set_request() {
        assert_eq!(
            encode_set::<TimeSeries>(),
            write_request!("vector_users", "users", Gauge [ "" @ 1612325106789 = 1.0 []])
        );
    }

    fn encode_set<T: MetricCollector>() -> T::Output {
        let otel = OtelMetric::new_set_from_values("users", MetricKind::Absolute, vec!["foo"])
            .with_timestamp(Some(timestamp()));
        encode_one::<T>(Some("vector"), &[], &[], otel)
    }

    #[test]
    fn encodes_expired_set_text() {
        assert_eq!(
            encode_expired_set::<StringCollector>(),
            indoc! {r"
                # HELP vector_users users
                # TYPE vector_users gauge
                vector_users 0 1612325106789
            "}
        );
    }

    #[test]
    fn encodes_expired_set_request() {
        assert_eq!(
            encode_expired_set::<TimeSeries>(),
            write_request!("vector_users", "users", Gauge ["" @ 1612325106789 = 0.0 []])
        );
    }

    fn encode_expired_set<T: MetricCollector>() -> T::Output {
        let otel =
            OtelMetric::new_set_from_values("users", MetricKind::Absolute, Vec::<String>::new())
                .with_timestamp(Some(timestamp()));
        encode_one::<T>(Some("vector"), &[], &[], otel)
    }

    #[test]
    fn encodes_histogram_from_samples_text() {
        assert_eq!(
            encode_histogram_from_samples::<StringCollector>(),
            indoc! {r#"
                # HELP vector_requests requests
                # TYPE vector_requests histogram
                vector_requests_bucket{le="1"} 3 1612325106789
                vector_requests_bucket{le="2"} 6 1612325106789
                vector_requests_bucket{le="3"} 8 1612325106789
                vector_requests_bucket{le="+Inf"} 8 1612325106789
                vector_requests_sum 15 1612325106789
                vector_requests_count 8 1612325106789
            "#}
        );
    }

    #[test]
    fn encodes_histogram_from_samples_request() {
        assert_eq!(
            encode_histogram_from_samples::<TimeSeries>(),
            write_request!(
                "vector_requests", "requests", Histogram [
                        "_bucket" @ 1612325106789 = 3.0 ["le" => "1"],
                        "_bucket" @ 1612325106789 = 6.0 ["le" => "2"],
                        "_bucket" @ 1612325106789 = 8.0 ["le" => "3"],
                        "_bucket" @ 1612325106789 = 8.0 ["le" => "+Inf"],
                        "_sum" @ 1612325106789 = 15.0 [],
                        "_count" @ 1612325106789 = 8.0 []
                ]
            )
        );
    }

    fn encode_histogram_from_samples<T: MetricCollector>() -> T::Output {
        let samples = sol_lib::samples![1.0 => 3, 2.0 => 3, 3.0 => 2];
        let otel =
            OtelMetric::new_histogram_from_samples("requests", MetricKind::Absolute, &samples)
                .with_timestamp(Some(timestamp()));
        encode_one::<T>(Some("vector"), &[], &[], otel)
    }

    #[test]
    fn encodes_histogram_text() {
        assert_eq!(
            encode_histogram::<StringCollector>(false),
            indoc! {r#"
                # HELP vector_requests requests
                # TYPE vector_requests histogram
                vector_requests_bucket{le="1"} 1 1612325106789
                vector_requests_bucket{le="2.1"} 3 1612325106789
                vector_requests_bucket{le="3"} 6 1612325106789
                vector_requests_bucket{le="+Inf"} 6 1612325106789
                vector_requests_sum 11.5 1612325106789
                vector_requests_count 6 1612325106789
            "#}
        );
    }

    #[test]
    fn encodes_histogram_request() {
        assert_eq!(
            encode_histogram::<TimeSeries>(false),
            write_request!(
                "vector_requests", "requests", Histogram [
                        "_bucket" @ 1612325106789 = 1.0 ["le" => "1"],
                        "_bucket" @ 1612325106789 = 3.0 ["le" => "2.1"],
                        "_bucket" @ 1612325106789 = 6.0 ["le" => "3"],
                        "_bucket" @ 1612325106789 = 6.0 ["le" => "+Inf"],
                        "_sum" @ 1612325106789 = 11.5 [],
                        "_count" @ 1612325106789 = 6.0 []
                    ]
            )
        );
    }

    #[test]
    fn encodes_histogram_text_with_extra_infinity_bound() {
        assert_eq!(
            encode_histogram::<StringCollector>(true),
            indoc! {r#"
                # HELP vector_requests requests
                # TYPE vector_requests histogram
                vector_requests_bucket{le="1"} 1 1612325106789
                vector_requests_bucket{le="2.1"} 3 1612325106789
                vector_requests_bucket{le="3"} 6 1612325106789
                vector_requests_bucket{le="+Inf"} 6 1612325106789
                vector_requests_sum 11.5 1612325106789
                vector_requests_count 6 1612325106789
            "#}
        );
    }

    #[test]
    fn encodes_histogram_request_with_extra_infinity_bound() {
        assert_eq!(
            encode_histogram::<TimeSeries>(true),
            write_request!(
                "vector_requests", "requests", Histogram [
                        "_bucket" @ 1612325106789 = 1.0 ["le" => "1"],
                        "_bucket" @ 1612325106789 = 3.0 ["le" => "2.1"],
                        "_bucket" @ 1612325106789 = 6.0 ["le" => "3"],
                        "_bucket" @ 1612325106789 = 6.0 ["le" => "+Inf"],
                        "_sum" @ 1612325106789 = 11.5 [],
                        "_count" @ 1612325106789 = 6.0 []
                    ]
            )
        );
    }

    fn encode_histogram<T: MetricCollector>(add_inf_bound: bool) -> T::Output {
        let bounds = if add_inf_bound {
            &[1.0, 2.1, 3.0, f64::INFINITY][..]
        } else {
            &[1.0, 2.1, 3.0][..]
        };

        let mut histogram = VariableHistogram::new(bounds);
        histogram.record_many(&[0.4, 2.0, 1.75, 2.6, 2.25, 2.5][..]);

        let buckets = histogram.buckets();
        let otel = OtelMetric::new_histogram(
            "requests",
            MetricKind::Absolute,
            &buckets,
            histogram.count(),
            histogram.sum(),
        )
        .with_timestamp(Some(timestamp()));
        encode_one::<T>(Some("vector"), &[], &[], otel)
    }

    #[test]
    fn encodes_summary_text() {
        assert_eq!(
            encode_summary::<StringCollector>(),
            indoc! {r#"# HELP ns_requests requests
                # TYPE ns_requests summary
                ns_requests{code="200",quantile="0.01"} 1.5 1612325106789
                ns_requests{code="200",quantile="0.5"} 2 1612325106789
                ns_requests{code="200",quantile="0.99"} 3 1612325106789
                ns_requests_sum{code="200"} 12 1612325106789
                ns_requests_count{code="200"} 6 1612325106789
            "#}
        );
    }

    #[test]
    fn encodes_summary_request() {
        assert_eq!(
            encode_summary::<TimeSeries>(),
            write_request!(
                "ns_requests", "requests", Summary [
                    "" @ 1612325106789 = 1.5 ["code" => "200", "quantile" => "0.01"],
                    "" @ 1612325106789 = 2.0 ["code" => "200", "quantile" => "0.5"],
                    "" @ 1612325106789 = 3.0 ["code" => "200", "quantile" => "0.99"],
                    "_sum" @ 1612325106789 = 12.0 ["code" => "200"],
                    "_count" @ 1612325106789 = 6.0 ["code" => "200"]
                ]
            )
        );
    }

    fn encode_summary<T: MetricCollector>() -> T::Output {
        let quantiles = sol_lib::quantiles![0.01 => 1.5, 0.5 => 2.0, 0.99 => 3.0];
        let otel = OtelMetric::new_summary("requests", &quantiles, 6, 12.0)
            .with_tags(Some(tags()))
            .with_timestamp(Some(timestamp()));
        encode_one::<T>(Some("ns"), &[], &[], otel)
    }

    #[test]
    fn encodes_histogram_from_samples_with_tags_text() {
        let samples = sol_lib::samples![1.0 => 3, 2.0 => 3, 3.0 => 2];
        let otel =
            OtelMetric::new_histogram_from_samples("requests", MetricKind::Absolute, &samples)
                .with_tags(Some(tags()))
                .with_timestamp(Some(timestamp()));
        assert_eq!(
            encode_one::<StringCollector>(Some("ns"), &[], &[], otel),
            indoc! {r#"
                # HELP ns_requests requests
                # TYPE ns_requests histogram
                ns_requests_bucket{code="200",le="1"} 3 1612325106789
                ns_requests_bucket{code="200",le="2"} 6 1612325106789
                ns_requests_bucket{code="200",le="3"} 8 1612325106789
                ns_requests_bucket{code="200",le="+Inf"} 8 1612325106789
                ns_requests_sum{code="200"} 15 1612325106789
                ns_requests_count{code="200"} 8 1612325106789
            "#}
        );
    }

    #[test]
    fn encodes_histogram_from_samples_with_tags_request() {
        let samples = sol_lib::samples![1.0 => 3, 2.0 => 3, 3.0 => 2];
        let otel =
            OtelMetric::new_histogram_from_samples("requests", MetricKind::Absolute, &samples)
                .with_tags(Some(tags()))
                .with_timestamp(Some(timestamp()));
        assert_eq!(
            encode_one::<TimeSeries>(Some("ns"), &[], &[], otel),
            write_request!(
                "ns_requests", "requests", Histogram [
                    "_bucket" @ 1612325106789 = 3.0 ["code" => "200", "le" => "1"],
                    "_bucket" @ 1612325106789 = 6.0 ["code" => "200", "le" => "2"],
                    "_bucket" @ 1612325106789 = 8.0 ["code" => "200", "le" => "3"],
                    "_bucket" @ 1612325106789 = 8.0 ["code" => "200", "le" => "+Inf"],
                    "_sum" @ 1612325106789 = 15.0 ["code" => "200"],
                    "_count" @ 1612325106789 = 8.0 ["code" => "200"]
                ]
            )
        );
    }

    #[test]
    fn encodes_timestamp_text() {
        assert_eq!(
            encode_timestamp::<StringCollector>(),
            indoc! {r"
                # HELP temperature temperature
                # TYPE temperature counter
                temperature 2 1612325106789
            "}
        );
    }

    #[test]
    fn encodes_timestamp_request() {
        assert_eq!(
            encode_timestamp::<TimeSeries>(),
            write_request!("temperature", "temperature", Counter ["" @ 1612325106789 = 2.0 []])
        );
    }

    fn encode_timestamp<T: MetricCollector>() -> T::Output {
        let otel = OtelMetric::new_counter("temperature", MetricKind::Absolute, 2.0)
            .with_timestamp(Some(timestamp()));
        encode_one::<T>(None, &[], &[], otel)
    }

    #[test]
    fn adds_timestamp_request() {
        let now = Utc::now().timestamp_millis();
        let otel = OtelMetric::new_gauge("something", 1.0);
        let encoded = encode_one::<TimeSeries>(None, &[], &[], otel);
        assert!(encoded.timeseries[0].samples[0].timestamp >= now);
    }

    fn timestamp() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2021, 2, 3, 4, 5, 6)
            .single()
            .and_then(|t| t.with_nanosecond(789 * 1_000_000))
            .expect("invalid timestamp")
    }

    #[test]
    fn escapes_tags_text() {
        let tags = otel_tags!(
            "code" => "200",
            "quoted" => r#"host"1""#,
            "path" => r"c:\Windows",
        );
        let otel =
            OtelMetric::new_counter("something", MetricKind::Absolute, 1.0).with_tags(Some(tags));
        let encoded = encode_one::<StringCollector>(None, &[], &[], otel);
        assert_eq!(
            encoded,
            indoc! {r#"
                # HELP something something
                # TYPE something counter
                something{code="200",path="c:\\Windows",quoted="host\"1\""} 1
            "#}
        );
    }

    /// According to the [spec](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md?plain=1#L115)
    ///
    /// > Label names MUST be unique within a LabelSet.
    ///
    /// Prometheus itself will reject the metric with an error. Largely to remain backward
    /// compatible with older versions of Vector, we only publish the last tag in the list.
    #[test]
    fn encodes_duplicate_tags() {
        let tags = otel_tags!(
            "code" => "200",
            "code" => "success",
        );
        let otel =
            OtelMetric::new_counter("something", MetricKind::Absolute, 1.0).with_tags(Some(tags));
        let encoded = encode_one::<StringCollector>(None, &[], &[], otel);
        assert_eq!(
            encoded,
            indoc! {r#"
                # HELP something something
                # TYPE something counter
                something{code="success"} 1
            "#}
        );
    }
}
