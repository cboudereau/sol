use opentelemetry_proto::tonic::common::v1::AnyValue;
pub use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
pub use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValueKind;
use opentelemetry_proto::tonic::metrics::v1::Metric as OtelMetricProto;
pub use opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets as ExpBuckets;
pub use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
pub use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberDataPointValue;
pub use opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile;
pub use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message as _;
use serde::Serialize;
use sol_buffers::EventCount;
use sol_common::{
    EventDataEq,
    byte_size_of::ByteSizeOf,
    finalization::{EventFinalizers, Finalizable},
    internal_event::TaggedEventsSent,
    json_size::JsonSize,
    request_metadata::GetEventCountTags,
};
use std::collections::BTreeSet;

use super::otel_attributes::OtelAttributes;
use super::otel_conv::OtlpMetricInt;
use super::otel_event::{
    int_value, otel_value_to_tag_string, resource_to_proto, scope_to_proto, string_value,
};
use super::{
    BatchNotifier, EstimatedJsonEncodedSizeOf, EventFinalizer, EventMetadata, otel_fields as f,
};

// -- MetricView --

/// Zero-copy view into an `OtelMetric`'s data. Variant names follow OTLP types
/// (`Sum`, `Gauge`, `Histogram`, `Summary`, `ExponentialHistogram`) with
/// one Vector-specific extension (`Set`).
///
/// Scalars are copied (cheap). Histogram bounds/counts and summary quantiles
/// are borrowed from the proto. Set values are cloned from the struct field.
#[derive(Debug)]
pub enum MetricView<'a> {
    Sum {
        value: f64,
    },
    Gauge {
        value: f64,
    },
    Set {
        values: Vec<String>,
    },
    Histogram {
        bounds: &'a [f64],
        counts: &'a [u64],
        count: u64,
        sum: f64,
    },
    Summary {
        quantiles: &'a [ValueAtQuantile],
        count: u64,
        sum: f64,
    },
    ExponentialHistogram {
        scale: i32,
        count: u64,
        sum: f64,
        zero_count: u64,
        positive: Option<&'a ExpBuckets>,
        negative: Option<&'a ExpBuckets>,
        min: Option<f64>,
        max: Option<f64>,
    },
}

impl MetricView<'_> {
    pub fn as_name(&self) -> &'static str {
        match self {
            Self::Sum { .. } => f::METRIC_TYPE_COUNTER,
            Self::Gauge { .. } => f::METRIC_TYPE_GAUGE,
            Self::Set { .. } => f::METRIC_TYPE_SET,
            Self::Histogram { .. } => f::METRIC_TYPE_HISTOGRAM,
            Self::Summary { .. } => f::METRIC_TYPE_SUMMARY,
            Self::ExponentialHistogram { .. } => "exponential histogram",
        }
    }
}

impl std::fmt::Display for MetricView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sum { value } => write!(f, "counter: {value}"),
            Self::Gauge { value } => write!(f, "gauge: {value}"),
            Self::Set { values } => write!(f, "set: {} values", values.len()),
            Self::Histogram {
                bounds, count, sum, ..
            } => {
                write!(
                    f,
                    "histogram: {} buckets, count={count}, sum={sum}",
                    bounds.len()
                )
            }
            Self::Summary {
                quantiles,
                count,
                sum,
            } => {
                write!(
                    f,
                    "summary: {} quantiles, count={count}, sum={sum}",
                    quantiles.len()
                )
            }
            Self::ExponentialHistogram {
                scale, count, sum, ..
            } => {
                write!(
                    f,
                    "exponential histogram: scale={scale}, count={count}, sum={sum}"
                )
            }
        }
    }
}

// -- OtelMetric --

#[derive(Clone, Debug, PartialEq)]
pub struct OtelMetric {
    pub(crate) metric: OtelMetricProto,
    pub(crate) dp_attrs: Vec<OtelAttributes>,
    pub(crate) resource: Option<Resource>,
    pub(crate) resource_attrs: OtelAttributes,
    pub(crate) scope: Option<InstrumentationScope>,
    pub(crate) scope_attrs: OtelAttributes,
    pub(crate) metadata: EventMetadata,
    pub(crate) set_values: Option<BTreeSet<String>>,
    pub(crate) kind_override: Option<super::MetricKind>,
}

fn extract_dp_attrs(metric: &mut OtelMetricProto) -> Vec<OtelAttributes> {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
    match metric.data.as_mut() {
        Some(MetricData::Sum(s)) => s
            .data_points
            .iter_mut()
            .map(|dp| OtelAttributes::from_key_values(std::mem::take(&mut dp.attributes)))
            .collect(),
        Some(MetricData::Gauge(g)) => g
            .data_points
            .iter_mut()
            .map(|dp| OtelAttributes::from_key_values(std::mem::take(&mut dp.attributes)))
            .collect(),
        Some(MetricData::Histogram(h)) => h
            .data_points
            .iter_mut()
            .map(|dp| OtelAttributes::from_key_values(std::mem::take(&mut dp.attributes)))
            .collect(),
        Some(MetricData::Summary(s)) => s
            .data_points
            .iter_mut()
            .map(|dp| OtelAttributes::from_key_values(std::mem::take(&mut dp.attributes)))
            .collect(),
        Some(MetricData::ExponentialHistogram(e)) => e
            .data_points
            .iter_mut()
            .map(|dp| OtelAttributes::from_key_values(std::mem::take(&mut dp.attributes)))
            .collect(),
        None => vec![],
    }
}

fn populate_dp_attrs(metric: &mut OtelMetricProto, dp_attrs: &[OtelAttributes]) {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
    macro_rules! write_back {
        ($data_points:expr) => {
            for (dp, attrs) in $data_points.iter_mut().zip(dp_attrs.iter()) {
                dp.attributes = attrs.to_key_values();
            }
        };
    }
    if let Some(data) = metric.data.as_mut() {
        match data {
            MetricData::Sum(s) => write_back!(s.data_points),
            MetricData::Gauge(g) => write_back!(g.data_points),
            MetricData::Histogram(h) => write_back!(h.data_points),
            MetricData::Summary(s) => write_back!(s.data_points),
            MetricData::ExponentialHistogram(e) => write_back!(e.data_points),
        }
    }
}

impl OtelMetric {
    pub fn new(mut metric: OtelMetricProto) -> Self {
        let dp_attrs = extract_dp_attrs(&mut metric);
        Self {
            metric,
            dp_attrs,
            resource: None,
            resource_attrs: OtelAttributes::new(),
            scope: None,
            scope_attrs: OtelAttributes::new(),
            metadata: EventMetadata::default(),
            set_values: None,
            kind_override: None,
        }
    }

    /// Convenience constructor for a counter metric.
    /// Builds the OTLP proto directly without going through legacy Metric.
    pub fn new_counter(name: impl Into<String>, kind: super::MetricKind, value: f64) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::{
            self as otel_metrics, NumberDataPoint, Sum, metric::Data,
            number_data_point::Value as NDPValue,
        };
        let temporality = match kind {
            super::MetricKind::Incremental => otel_metrics::AggregationTemporality::Delta as i32,
            super::MetricKind::Absolute => otel_metrics::AggregationTemporality::Cumulative as i32,
        };
        let proto = OtelMetricProto {
            name: name.into(),
            data: Some(Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    value: Some(NDPValue::AsDouble(value)),
                    ..Default::default()
                }],
                aggregation_temporality: temporality,
                is_monotonic: true,
            })),
            ..Default::default()
        };
        Self::new(proto)
    }

    /// Convenience constructor for a gauge metric.
    /// Builds the OTLP proto directly without going through legacy Metric.
    pub fn new_gauge(name: impl Into<String>, value: f64) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, NumberDataPoint, metric::Data, number_data_point::Value as NDPValue,
        };
        let proto = OtelMetricProto {
            name: name.into(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    value: Some(NDPValue::AsDouble(value)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };
        Self::new(proto)
    }

    /// Convenience constructor for an aggregated histogram metric.
    pub fn new_histogram(
        name: impl Into<String>,
        kind: super::MetricKind,
        buckets: &[super::metric::Bucket],
        count: u64,
        sum: f64,
    ) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::{
            self as otel_metrics, Histogram, HistogramDataPoint, metric::Data,
        };
        let temporality = match kind {
            super::MetricKind::Incremental => otel_metrics::AggregationTemporality::Delta as i32,
            super::MetricKind::Absolute => otel_metrics::AggregationTemporality::Cumulative as i32,
        };
        let n = buckets.len();
        let mut explicit_bounds = Vec::with_capacity(n);
        let mut bucket_counts = Vec::with_capacity(n + 1);
        for b in buckets {
            if b.upper_limit == f64::INFINITY {
                bucket_counts.push(b.count);
            } else {
                explicit_bounds.push(b.upper_limit);
                bucket_counts.push(b.count);
            }
        }
        if buckets
            .last()
            .is_none_or(|b| b.upper_limit != f64::INFINITY)
        {
            bucket_counts.push(0);
        }
        let proto = OtelMetricProto {
            name: name.into(),
            data: Some(Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    count,
                    sum: Some(sum),
                    bucket_counts,
                    explicit_bounds,
                    ..Default::default()
                }],
                aggregation_temporality: temporality,
            })),
            ..Default::default()
        };
        Self::new(proto)
    }

    /// Convenience constructor for an aggregated summary metric.
    pub fn new_summary(
        name: impl Into<String>,
        quantiles: &[super::metric::Quantile],
        count: u64,
        sum: f64,
    ) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::{
            Summary, SummaryDataPoint, metric::Data, summary_data_point::ValueAtQuantile,
        };
        let proto = OtelMetricProto {
            name: name.into(),
            data: Some(Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    count,
                    sum,
                    quantile_values: quantiles
                        .iter()
                        .map(|q| ValueAtQuantile {
                            quantile: q.quantile,
                            value: q.value,
                        })
                        .collect(),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };
        Self::new(proto)
    }

    /// Convenience constructor for a set metric.
    /// `OTel` has no native Set type; represented as a Gauge whose value
    /// is the cardinality (number of unique values).
    #[expect(clippy::cast_precision_loss, reason = "precise for len ≤ 2^53")]
    pub fn new_set(name: impl Into<String>, cardinality: usize) -> Self {
        Self::new_gauge(name, cardinality as f64)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "OTLP exponential histogram has many fields"
    )]
    pub fn new_exponential_histogram(
        name: impl Into<String>,
        scale: i32,
        count: u64,
        sum: f64,
        zero_count: u64,
        positive: opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets,
        negative: opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets,
        min: Option<f64>,
        max: Option<f64>,
    ) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::{
            self as otel_metrics, ExponentialHistogram, ExponentialHistogramDataPoint, metric::Data,
        };
        let proto = OtelMetricProto {
            name: name.into(),
            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count,
                    sum: Some(sum),
                    scale,
                    zero_count,
                    positive: Some(positive),
                    negative: Some(negative),
                    min,
                    max,
                    ..Default::default()
                }],
                aggregation_temporality: otel_metrics::AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        };
        Self::new(proto)
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "IEEE 754 exponent extraction and index computation"
    )]
    #[expect(
        clippy::cast_precision_loss,
        reason = "IEEE 754 exponential histogram bucket index algorithm"
    )]
    pub fn new_exponential_histogram_single(name: impl Into<String>, value: f64) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::{
            self as otel_metrics, ExponentialHistogram, ExponentialHistogramDataPoint,
            exponential_histogram_data_point::Buckets, metric::Data,
        };
        fn frexp_local(x: f64) -> (f64, i32) {
            if x == 0.0 || x.is_nan() || x.is_infinite() {
                return (x, 0);
            }
            let bits = x.to_bits();
            let exp_raw = ((bits >> 52) & 0x7FF) as i32;
            let exp = exp_raw - 1022;
            let frac = f64::from_bits((bits & 0x800F_FFFF_FFFF_FFFF) | 0x3FE0_0000_0000_0000);
            (frac, exp)
        }

        let scale = 20i32;
        let (zero_count, positive, negative) = if value == 0.0 {
            (1u64, Buckets::default(), Buckets::default())
        } else {
            let abs_value = value.abs();
            let (frac, exp) = frexp_local(abs_value);
            let index = if frac == 0.5 {
                ((exp - 1) << scale) - 1
            } else {
                let scale_factor = std::f64::consts::LOG2_E * (1u64 << scale as u64) as f64;
                (abs_value.ln() * scale_factor).floor() as i32
            };
            let b = Buckets {
                offset: index,
                bucket_counts: vec![1],
            };
            if value > 0.0 {
                (0u64, b, Buckets::default())
            } else {
                (0u64, Buckets::default(), b)
            }
        };
        let proto = OtelMetricProto {
            name: name.into(),
            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 1,
                    sum: Some(value),
                    scale,
                    zero_count,
                    positive: Some(positive),
                    negative: Some(negative),
                    min: Some(value),
                    max: Some(value),
                    ..Default::default()
                }],
                aggregation_temporality: otel_metrics::AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        };
        Self::new(proto)
    }

    pub fn new_histogram_from_samples(
        name: impl Into<String>,
        kind: super::MetricKind,
        samples: &[super::metric::Sample],
    ) -> Self {
        let count = u64::from(samples.iter().map(|s| s.rate).sum::<u32>());
        let sum: f64 = samples.iter().map(|s| s.value * f64::from(s.rate)).sum();
        let buckets: Vec<super::metric::Bucket> = samples
            .iter()
            .map(|s| super::metric::Bucket {
                upper_limit: s.value,
                count: u64::from(s.rate),
            })
            .collect();
        Self::new_histogram(name, kind, &buckets, count, sum)
    }

    /// Convenience constructor for a set metric with its values.
    /// Represented as an OTLP Gauge with cardinality as the numeric value.
    /// Set values are stored in the `set_values` struct field, not in attributes.
    #[expect(clippy::cast_precision_loss, reason = "precise for len ≤ 2^53")]
    pub fn new_set_from_values(
        name: impl Into<String>,
        kind: super::MetricKind,
        values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let set: BTreeSet<String> = values.into_iter().map(Into::into).collect();
        let cardinality = set.len() as f64;
        let mut m = Self::new_gauge(name, cardinality);
        m.set_values = Some(set);
        m.kind_override = match kind {
            super::MetricKind::Incremental => Some(super::MetricKind::Incremental),
            super::MetricKind::Absolute => None,
        };
        m
    }

    /// Convenience constructor for a delta/signed gauge (e.g. statsd +/-).
    pub fn new_gauge_delta(name: impl Into<String>, value: f64) -> Self {
        let mut m = Self::new_gauge(name, value);
        m.kind_override = Some(super::MetricKind::Incremental);
        m
    }

    pub fn from_parts(
        mut metric: OtelMetricProto,
        mut resource: Option<Resource>,
        mut scope: Option<InstrumentationScope>,
        metadata: EventMetadata,
    ) -> Self {
        let dp_attrs = extract_dp_attrs(&mut metric);
        let resource_attrs = resource
            .as_mut()
            .map(|r| OtelAttributes::from_key_values(std::mem::take(&mut r.attributes)))
            .unwrap_or_default();
        let scope_attrs = scope
            .as_mut()
            .map(|s| OtelAttributes::from_key_values(std::mem::take(&mut s.attributes)))
            .unwrap_or_default();
        Self {
            metric,
            dp_attrs,
            resource,
            resource_attrs,
            scope,
            scope_attrs,
            metadata,
            set_values: None,
            kind_override: None,
        }
    }

    pub fn into_parts(
        mut self,
    ) -> (
        OtelMetricProto,
        Option<Resource>,
        Option<InstrumentationScope>,
        EventMetadata,
    ) {
        populate_dp_attrs(&mut self.metric, &self.dp_attrs);
        let resource = self.resource.map(|mut r| {
            r.attributes = self.resource_attrs.to_key_values();
            r
        });
        let scope = self.scope.map(|mut s| {
            s.attributes = self.scope_attrs.to_key_values();
            s
        });
        (self.metric, resource, scope, self.metadata)
    }

    pub fn metric_proto(&self) -> OtelMetricProto {
        let mut m = self.metric.clone();
        populate_dp_attrs(&mut m, &self.dp_attrs);
        m
    }

    pub fn metric(&self) -> &OtelMetricProto {
        &self.metric
    }

    pub fn metric_mut(&mut self) -> &mut OtelMetricProto {
        &mut self.metric
    }

    pub fn resource(&self) -> Option<&Resource> {
        self.resource.as_ref()
    }

    pub fn resource_proto(&self) -> Option<Resource> {
        resource_to_proto(self.resource.as_ref(), &self.resource_attrs)
    }

    pub fn scope_proto(&self) -> Option<InstrumentationScope> {
        scope_to_proto(self.scope.as_ref(), &self.scope_attrs)
    }

    pub fn set_resource(&mut self, mut resource: Resource) {
        let mut new_attrs =
            OtelAttributes::from_key_values(std::mem::take(&mut resource.attributes));
        for (k, v) in self.resource_attrs.iter() {
            new_attrs.insert(k.clone(), v.clone());
        }
        self.resource_attrs = new_attrs;
        self.resource = Some(resource);
    }

    pub fn resource_attrs(&self) -> &OtelAttributes {
        &self.resource_attrs
    }

    pub fn scope_attrs(&self) -> &OtelAttributes {
        &self.scope_attrs
    }

    pub fn scope(&self) -> Option<&InstrumentationScope> {
        self.scope.as_ref()
    }

    pub fn set_scope(&mut self, mut scope: InstrumentationScope) {
        self.scope_attrs = OtelAttributes::from_key_values(std::mem::take(&mut scope.attributes));
        self.scope = Some(scope);
    }

    pub fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        &mut self.metadata
    }

    pub fn name(&self) -> &str {
        &self.metric.name
    }

    pub fn description(&self) -> &str {
        &self.metric.description
    }

    pub fn unit(&self) -> &str {
        &self.metric.unit
    }

    #[must_use]
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.metric.unit = unit.into();
        self
    }

    pub fn first_dp_attrs(&self) -> Option<&OtelAttributes> {
        self.dp_attrs.first()
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "public API; callers pass owned values"
    )]
    pub fn set_data_point_attribute(&mut self, key: String, value: AnyValue) {
        for attrs in &mut self.dp_attrs {
            attrs.insert(key.clone(), value.clone());
        }
    }

    pub fn flatten_resource_to_tags(&mut self, keys: &[String]) {
        for key in keys {
            if let Some(value) = self.resource_attrs.get(key) {
                self.set_data_point_attribute(key.clone(), value.clone());
            }
        }
    }

    pub fn reduce_tags_to_single(&mut self) {
        for attrs in &mut self.dp_attrs {
            let updates: Vec<(String, AnyValue)> = attrs
                .iter()
                .filter_map(|(key, val)| {
                    if let Some(OtelValueKind::ArrayValue(arr)) = &val.value {
                        let last = arr.values.iter().rev().find_map(|v| match &v.value {
                            Some(OtelValueKind::StringValue(s)) => Some(s.clone()),
                            _ => None,
                        });
                        last.map(|s| {
                            (
                                key.clone(),
                                AnyValue {
                                    value: Some(OtelValueKind::StringValue(s)),
                                },
                            )
                        })
                    } else {
                        None
                    }
                })
                .collect();
            for (key, val) in updates {
                attrs.insert(key, val);
            }
        }
    }

    pub fn remove_data_point_attribute(&mut self, key: &str) -> Option<AnyValue> {
        let mut removed = None;
        for attrs in &mut self.dp_attrs {
            removed = removed.or(attrs.remove(key));
        }
        removed
    }

    /// Replace a tag: remove existing attribute then set new value.
    pub fn replace_tag(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        self.remove_data_point_attribute(&key);
        self.set_data_point_attribute(
            key,
            AnyValue {
                value: Some(OtelValueKind::StringValue(value.into())),
            },
        );
    }

    pub fn resource_attribute(&self, key: &str) -> Option<&AnyValue> {
        self.resource_attrs.get(key)
    }

    pub fn set_resource_attribute(&mut self, key: String, value: AnyValue) {
        if self.resource.is_none() {
            self.resource = Some(Resource {
                attributes: Vec::new(),
                dropped_attributes_count: 0,
            });
        }
        self.resource_attrs.insert(key, value);
    }

    // -----------------------------------------------------------------------
    // Metric accessors
    //
    // `value()`, `kind()`, `tag_value()` read proto directly via
    // `extract_metric_data()`. `timestamp()` and `namespace()` also read
    // proto directly. `tags()` is a broken stub — see its doc comment.
    // -----------------------------------------------------------------------

    /// Get the metric timestamp from the first data point.
    pub fn timestamp(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.to_legacy_metric_ref_timestamp()
    }

    /// Get the interval between `start_time` and `end_time` in milliseconds.
    pub fn interval_ms(&self) -> Option<std::num::NonZeroU32> {
        self.reconstruct_interval_ms()
    }

    fn to_legacy_metric_ref_timestamp(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        use opentelemetry_proto::tonic::metrics::v1::metric;
        let data = self.metric.data.as_ref()?;
        let nanos = match data {
            metric::Data::Gauge(g) => g.data_points.first().map(|dp| dp.time_unix_nano),
            metric::Data::Sum(s) => s.data_points.first().map(|dp| dp.time_unix_nano),
            metric::Data::Histogram(h) => h.data_points.first().map(|dp| dp.time_unix_nano),
            metric::Data::ExponentialHistogram(h) => {
                h.data_points.first().map(|dp| dp.time_unix_nano)
            }
            metric::Data::Summary(s) => s.data_points.first().map(|dp| dp.time_unix_nano),
        }?;
        if nanos == 0 {
            return None;
        }
        let secs = (nanos / 1_000_000_000) as i64;
        let nsecs = (nanos % 1_000_000_000) as u32;
        chrono::DateTime::from_timestamp(secs, nsecs)
    }

    /// Set the timestamp on all data points.
    ///
    /// Note: `None` sets `time_unix_nano` to 0, which `timestamp()` reads back
    /// as `None`. A `Some(ts)` at the Unix epoch (1970-01-01T00:00:00Z) also
    /// produces nanos == 0 and will round-trip as `None`.
    pub fn set_timestamp(&mut self, ts: Option<chrono::DateTime<chrono::Utc>>) {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
        let nanos = ts.map_or(0, |t| t.timestamp_nanos_opt().unwrap_or(0) as u64);
        if let Some(data) = self.metric.data.as_mut() {
            match data {
                MetricData::Sum(s) => {
                    for dp in &mut s.data_points {
                        dp.time_unix_nano = nanos;
                    }
                }
                MetricData::Gauge(g) => {
                    for dp in &mut g.data_points {
                        dp.time_unix_nano = nanos;
                    }
                }
                MetricData::Histogram(h) => {
                    for dp in &mut h.data_points {
                        dp.time_unix_nano = nanos;
                    }
                }
                MetricData::Summary(s) => {
                    for dp in &mut s.data_points {
                        dp.time_unix_nano = nanos;
                    }
                }
                MetricData::ExponentialHistogram(e) => {
                    for dp in &mut e.data_points {
                        dp.time_unix_nano = nanos;
                    }
                }
            }
        }
    }

    pub fn set_kind(&mut self, kind: super::MetricKind) {
        use opentelemetry_proto::tonic::metrics::v1::{
            AggregationTemporality, metric::Data as MetricData,
        };
        let temp = match kind {
            super::MetricKind::Incremental => AggregationTemporality::Delta as i32,
            super::MetricKind::Absolute => AggregationTemporality::Cumulative as i32,
        };
        if let Some(data) = self.metric.data.as_mut() {
            match data {
                MetricData::Sum(s) => s.aggregation_temporality = temp,
                MetricData::Histogram(h) => h.aggregation_temporality = temp,
                MetricData::ExponentialHistogram(e) => e.aggregation_temporality = temp,
                MetricData::Gauge(_) | MetricData::Summary(_) => {
                    self.kind_override = match kind {
                        super::MetricKind::Incremental => Some(super::MetricKind::Incremental),
                        super::MetricKind::Absolute => None,
                    };
                }
            }
        }
    }

    pub fn set_namespace(&mut self, namespace: impl Into<String>) {
        self.set_resource_attribute(
            f::METRIC_NAMESPACE.to_string(),
            string_value(namespace.into()),
        );
    }

    /// Builder-style: set the metric namespace (stored as `metric.namespace` resource attribute).
    #[must_use]
    pub fn with_namespace(mut self, namespace: Option<impl Into<String>>) -> Self {
        if let Some(ns) = namespace {
            if self.resource.is_none() {
                self.resource = Some(Resource {
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                });
            }
            self.resource_attrs
                .insert(f::METRIC_NAMESPACE.to_string(), string_value(ns.into()));
        }
        self
    }

    /// Builder-style: set the timestamp on all data points.
    #[must_use]
    pub fn with_timestamp(mut self, ts: Option<chrono::DateTime<chrono::Utc>>) -> Self {
        self.set_timestamp(ts);
        self
    }

    /// Builder-style: set `interval_ms` by adjusting `start_time_unix_nano` relative to `time_unix_nano`.
    #[must_use]
    pub fn with_interval_ms(mut self, interval: Option<std::num::NonZeroU32>) -> Self {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as ProtoMetricData;
        let Some(interval) = interval else {
            return self;
        };
        if let Some(data) = self.metric.data.as_mut() {
            let adjust = |start: &mut u64, end: u64| {
                if end > 0 {
                    *start = end.saturating_sub(u64::from(interval.get()) * 1_000_000);
                }
            };
            match data {
                ProtoMetricData::Sum(s) => {
                    if let Some(dp) = s.data_points.first_mut() {
                        adjust(&mut dp.start_time_unix_nano, dp.time_unix_nano);
                    }
                }
                ProtoMetricData::Gauge(g) => {
                    if let Some(dp) = g.data_points.first_mut() {
                        adjust(&mut dp.start_time_unix_nano, dp.time_unix_nano);
                    }
                }
                ProtoMetricData::Histogram(h) => {
                    if let Some(dp) = h.data_points.first_mut() {
                        adjust(&mut dp.start_time_unix_nano, dp.time_unix_nano);
                    }
                }
                ProtoMetricData::Summary(s) => {
                    if let Some(dp) = s.data_points.first_mut() {
                        adjust(&mut dp.start_time_unix_nano, dp.time_unix_nano);
                    }
                }
                ProtoMetricData::ExponentialHistogram(e) => {
                    if let Some(dp) = e.data_points.first_mut() {
                        adjust(&mut dp.start_time_unix_nano, dp.time_unix_nano);
                    }
                }
            }
        }
        self
    }

    /// Builder-style: set tags as data point attributes from `OtelAttributes`.
    #[must_use]
    pub fn with_tags(mut self, tags: Option<OtelAttributes>) -> Self {
        let Some(tags) = tags else { return self };
        for (key, value) in tags.inner {
            self.remove_data_point_attribute(&key);
            self.set_data_point_attribute(key, value);
        }
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: EventMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Build `OtelAttributes` from proto data point, resource, and scope attributes.
    ///
    /// Returns an owned `OtelAttributes` because the tags are assembled from
    /// multiple proto fields. Returns `None` if there are no tags at all.
    pub fn tags(&self) -> Option<OtelAttributes> {
        self.dp_attrs.first().and_then(|dp| dp.clone().as_option())
    }

    pub fn all_tags_including_resource(&self) -> Option<OtelAttributes> {
        let mut attrs = OtelAttributes::new();

        for (key, val) in self.resource_attrs.iter() {
            if key == f::METRIC_NAMESPACE {
                continue;
            }
            if val.value.is_some() {
                attrs.insert(format!("resource.{key}"), val.clone());
            }
        }

        if let Some(ref scope) = self.scope {
            if !scope.name.is_empty() {
                attrs.insert("scope.name".to_string(), string_value(&scope.name));
            }
            if !scope.version.is_empty() {
                attrs.insert("scope.version".to_string(), string_value(&scope.version));
            }
        }

        if let Some(dp) = self.dp_attrs.first() {
            for (key, val) in dp.iter() {
                attrs.insert(key.clone(), val.clone());
            }
        }

        attrs.as_option()
    }

    /// Get the metric namespace from the `metric.namespace` resource attribute.
    pub fn namespace(&self) -> Option<&str> {
        self.resource_attrs
            .get(f::METRIC_NAMESPACE)
            .and_then(|av| match &av.value {
                Some(OtelValueKind::StringValue(s)) => Some(s.as_str()),
                _ => None,
            })
    }

    /// Get the metric kind directly from proto (or `kind_override` for Gauge/Summary).
    pub fn kind(&self) -> super::MetricKind {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, metric};
        match self.metric.data.as_ref() {
            Some(metric::Data::Sum(sum)) => {
                if sum.aggregation_temporality == AggregationTemporality::Delta as i32 {
                    super::MetricKind::Incremental
                } else {
                    super::MetricKind::Absolute
                }
            }
            Some(metric::Data::Gauge(_)) | Some(metric::Data::Summary(_)) => {
                self.kind_override.unwrap_or(super::MetricKind::Absolute)
            }
            Some(metric::Data::Histogram(hist)) => {
                if hist.aggregation_temporality == AggregationTemporality::Delta as i32 {
                    super::MetricKind::Incremental
                } else {
                    super::MetricKind::Absolute
                }
            }
            Some(metric::Data::ExponentialHistogram(exp)) => {
                if exp.aggregation_temporality == AggregationTemporality::Delta as i32 {
                    super::MetricKind::Incremental
                } else {
                    super::MetricKind::Absolute
                }
            }
            None => super::MetricKind::Absolute,
        }
    }

    pub fn tag_value(&self, key: &str) -> Option<String> {
        if let Some(dp) = self.dp_attrs.first()
            && let Some(av) = dp.get(key)
            && let Some(ref v) = av.value
        {
            return Some(otel_value_to_tag_string(v));
        }
        // Check resource attributes (prefixed with "resource." in legacy)
        if let Some(stripped) = key.strip_prefix("resource.")
            && let Some(av) = self.resource_attrs.get(stripped)
            && let Some(ref v) = av.value
        {
            return Some(otel_value_to_tag_string(v));
        }
        // Check scope attributes
        if let Some(ref scope) = self.scope {
            match key {
                "scope.name" if !scope.name.is_empty() => return Some(scope.name.clone()),
                "scope.version" if !scope.version.is_empty() => return Some(scope.version.clone()),
                _ => {}
            }
        }
        None
    }

    /// Check whether a tag with the given name matches the given value.
    pub fn tag_matches(&self, name: &str, value: &str) -> bool {
        self.tag_value(name).filter(|v| v == value).is_some()
    }

    /// Decompose this `OtelMetric` into legacy metric parts without creating
    /// an intermediate Metric. Used by aggregate and other transforms that
    /// store MetricSeries/MetricData separately.
    /// Build a `MetricSeries` key for this metric (name + namespace + tags).
    /// This is the grouping key used by aggregate/normalization.
    pub fn metric_series(&self) -> super::metric::MetricIdentity {
        super::metric::MetricIdentity {
            name: self.metric.name.clone(),
            namespace: self.namespace().map(std::string::ToString::to_string),
            tags: self.tags(),
        }
    }

    fn reconstruct_interval_ms(&self) -> Option<std::num::NonZeroU32> {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
        let dp_times = match self.metric.data.as_ref()? {
            MetricData::Sum(s) => s
                .data_points
                .first()
                .map(|p| (p.start_time_unix_nano, p.time_unix_nano)),
            MetricData::Gauge(g) => g
                .data_points
                .first()
                .map(|p| (p.start_time_unix_nano, p.time_unix_nano)),
            MetricData::Histogram(h) => h
                .data_points
                .first()
                .map(|p| (p.start_time_unix_nano, p.time_unix_nano)),
            MetricData::Summary(s) => s
                .data_points
                .first()
                .map(|p| (p.start_time_unix_nano, p.time_unix_nano)),
            MetricData::ExponentialHistogram(e) => e
                .data_points
                .first()
                .map(|p| (p.start_time_unix_nano, p.time_unix_nano)),
        };
        dp_times.and_then(|(start, end)| {
            if start > 0 && end > start {
                let diff_ms = (end - start) / 1_000_000;
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "interval_ms fits in u32 for practical metric intervals"
                )]
                std::num::NonZeroU32::new(diff_ms as u32)
            } else {
                None
            }
        })
    }

    pub fn add_finalizer(&mut self, finalizer: EventFinalizer) {
        self.metadata.add_finalizer(finalizer);
    }

    #[must_use]
    pub fn with_batch_notifier(mut self, batch: &BatchNotifier) -> Self {
        self.metadata = self.metadata.with_batch_notifier(batch);
        self
    }

    #[must_use]
    pub fn with_batch_notifier_option(mut self, batch: &Option<BatchNotifier>) -> Self {
        self.metadata = self.metadata.with_batch_notifier_option(batch);
        self
    }

    /// Merge set values from `other` into this set metric.
    /// Unions the `set_values` `BTreeSets` and updates the gauge cardinality.
    #[expect(clippy::cast_precision_loss, reason = "precise for len ≤ 2^53")]
    fn merge_set_values(&mut self, other: &Self) {
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NDPValue;

        if let (Some(self_set), Some(other_set)) = (&mut self.set_values, &other.set_values) {
            self_set.extend(other_set.iter().cloned());
            let cardinality = self_set.len() as f64;
            if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(g)) =
                self.metric.data.as_mut()
                && let Some(dp) = g.data_points.first_mut()
            {
                dp.value = Some(NDPValue::AsDouble(cardinality));
            }
        }
    }

    #[expect(clippy::cast_precision_loss, reason = "precise for len ≤ 2^53")]
    fn subtract_set_values(&mut self, other: &Self) {
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NDPValue;

        if let (Some(self_set), Some(other_set)) = (&mut self.set_values, &other.set_values) {
            for item in other_set {
                self_set.remove(item);
            }
            let cardinality = self_set.len() as f64;
            if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(g)) =
                self.metric.data.as_mut()
                && let Some(dp) = g.data_points.first_mut()
            {
                dp.value = Some(NDPValue::AsDouble(cardinality));
            }
        }
    }

    /// Add the data from `other` to this metric.
    ///
    /// Both metrics must have the same data type (Sum+Sum, Gauge+Gauge, etc.).
    /// For Histogram, bucket layouts (`explicit_bounds`) must match.
    /// Returns `false` if the types are incompatible.
    #[must_use]
    pub fn add(&mut self, other: &Self) -> bool {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MD;
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NDPValue;

        if self.is_set() && other.is_set() {
            self.merge_set_values(other);
            return true;
        }

        match (self.metric.data.as_mut(), other.metric.data.as_ref()) {
            (Some(MD::Sum(s)), Some(MD::Sum(o))) => {
                for (dp, odp) in s.data_points.iter_mut().zip(o.data_points.iter()) {
                    match (&mut dp.value, &odp.value) {
                        (Some(NDPValue::AsDouble(v)), Some(NDPValue::AsDouble(ov))) => *v += ov,
                        (Some(NDPValue::AsInt(v)), Some(NDPValue::AsInt(ov))) => *v += ov,
                        _ => return false,
                    }
                }
                true
            }
            (Some(MD::Gauge(g)), Some(MD::Gauge(o))) => {
                for (dp, odp) in g.data_points.iter_mut().zip(o.data_points.iter()) {
                    match (&mut dp.value, &odp.value) {
                        (Some(NDPValue::AsDouble(v)), Some(NDPValue::AsDouble(ov))) => *v += ov,
                        (Some(NDPValue::AsInt(v)), Some(NDPValue::AsInt(ov))) => *v += ov,
                        _ => return false,
                    }
                }
                true
            }
            (Some(MD::Histogram(h)), Some(MD::Histogram(oh))) => {
                for (dp, odp) in h.data_points.iter_mut().zip(oh.data_points.iter()) {
                    if dp.explicit_bounds != odp.explicit_bounds
                        || dp.bucket_counts.len() != odp.bucket_counts.len()
                    {
                        return false;
                    }
                    for (bc, obc) in dp.bucket_counts.iter_mut().zip(odp.bucket_counts.iter()) {
                        *bc += obc;
                    }
                    dp.count += odp.count;
                    dp.sum = Some(dp.sum.unwrap_or(0.0) + odp.sum.unwrap_or(0.0));
                }
                true
            }
            (Some(MD::ExponentialHistogram(eh)), Some(MD::ExponentialHistogram(oeh))) => {
                for (dp, odp) in eh.data_points.iter_mut().zip(oeh.data_points.iter()) {
                    if dp.scale != odp.scale {
                        return false;
                    }
                    dp.count += odp.count;
                    dp.sum = Some(dp.sum.unwrap_or(0.0) + odp.sum.unwrap_or(0.0));
                    dp.zero_count += odp.zero_count;
                    if let (Some(pos), Some(opos)) = (&mut dp.positive, &odp.positive) {
                        if pos.offset == opos.offset
                            && pos.bucket_counts.len() == opos.bucket_counts.len()
                        {
                            for (bc, obc) in
                                pos.bucket_counts.iter_mut().zip(opos.bucket_counts.iter())
                            {
                                *bc += obc;
                            }
                        } else {
                            return false;
                        }
                    }
                    if let (Some(neg), Some(oneg)) = (&mut dp.negative, &odp.negative) {
                        if neg.offset == oneg.offset
                            && neg.bucket_counts.len() == oneg.bucket_counts.len()
                        {
                            for (bc, obc) in
                                neg.bucket_counts.iter_mut().zip(oneg.bucket_counts.iter())
                            {
                                *bc += obc;
                            }
                        } else {
                            return false;
                        }
                    }
                }
                true
            }
            _ => false,
        }
    }

    /// Subtract the data of `other` from this metric.
    ///
    /// Both metrics must have the same data type. For counters (Sum),
    /// this is monotonic: returns `false` if subtraction would go negative.
    #[must_use]
    #[expect(
        clippy::too_many_lines,
        reason = "handles all OTLP metric data variants"
    )]
    pub fn subtract(&mut self, other: &Self) -> bool {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MD;
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NDPValue;

        if self.is_set() && other.is_set() {
            self.subtract_set_values(other);
            return true;
        }
        if self.is_set() || other.is_set() {
            return false;
        }

        match (self.metric.data.as_mut(), other.metric.data.as_ref()) {
            (Some(MD::Sum(s)), Some(MD::Sum(o))) => {
                for (dp, odp) in s.data_points.iter_mut().zip(o.data_points.iter()) {
                    match (&mut dp.value, &odp.value) {
                        (Some(NDPValue::AsDouble(v)), Some(NDPValue::AsDouble(ov))) => {
                            if *v < *ov {
                                return false;
                            }
                            *v -= ov;
                        }
                        (Some(NDPValue::AsInt(v)), Some(NDPValue::AsInt(ov))) => {
                            if *v < *ov {
                                return false;
                            }
                            *v -= ov;
                        }
                        _ => return false,
                    }
                }
                true
            }
            (Some(MD::Gauge(g)), Some(MD::Gauge(o))) => {
                for (dp, odp) in g.data_points.iter_mut().zip(o.data_points.iter()) {
                    match (&mut dp.value, &odp.value) {
                        (Some(NDPValue::AsDouble(v)), Some(NDPValue::AsDouble(ov))) => *v -= ov,
                        (Some(NDPValue::AsInt(v)), Some(NDPValue::AsInt(ov))) => *v -= ov,
                        _ => return false,
                    }
                }
                true
            }
            (Some(MD::Histogram(h)), Some(MD::Histogram(oh))) => {
                for (dp, odp) in h.data_points.iter_mut().zip(oh.data_points.iter()) {
                    if dp.explicit_bounds != odp.explicit_bounds
                        || dp.bucket_counts.len() != odp.bucket_counts.len()
                        || dp.count < odp.count
                    {
                        return false;
                    }
                    for (bc, obc) in dp.bucket_counts.iter_mut().zip(odp.bucket_counts.iter()) {
                        if *bc < *obc {
                            return false;
                        }
                        *bc -= obc;
                    }
                    dp.count -= odp.count;
                    dp.sum = Some(dp.sum.unwrap_or(0.0) - odp.sum.unwrap_or(0.0));
                }
                true
            }
            (Some(MD::ExponentialHistogram(eh)), Some(MD::ExponentialHistogram(oeh))) => {
                for (dp, odp) in eh.data_points.iter_mut().zip(oeh.data_points.iter()) {
                    if dp.scale != odp.scale || dp.count < odp.count {
                        return false;
                    }
                    dp.count -= odp.count;
                    dp.sum = Some(dp.sum.unwrap_or(0.0) - odp.sum.unwrap_or(0.0));
                    if dp.zero_count < odp.zero_count {
                        return false;
                    }
                    dp.zero_count -= odp.zero_count;
                    if let (Some(pos), Some(opos)) = (&mut dp.positive, &odp.positive) {
                        if pos.offset != opos.offset
                            || pos.bucket_counts.len() != opos.bucket_counts.len()
                        {
                            return false;
                        }
                        for (bc, obc) in pos.bucket_counts.iter_mut().zip(opos.bucket_counts.iter())
                        {
                            if *bc < *obc {
                                return false;
                            }
                            *bc -= obc;
                        }
                    }
                    if let (Some(neg), Some(oneg)) = (&mut dp.negative, &odp.negative) {
                        if neg.offset != oneg.offset
                            || neg.bucket_counts.len() != oneg.bucket_counts.len()
                        {
                            return false;
                        }
                        for (bc, obc) in neg.bucket_counts.iter_mut().zip(oneg.bucket_counts.iter())
                        {
                            if *bc < *obc {
                                return false;
                            }
                            *bc -= obc;
                        }
                    }
                }
                true
            }
            _ => false,
        }
    }

    /// Zero out all data point values in this metric.
    pub fn zero(&mut self) {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MD;
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NDPValue;

        match self.metric.data.as_mut() {
            Some(MD::Sum(s)) => {
                for dp in &mut s.data_points {
                    match &mut dp.value {
                        Some(NDPValue::AsDouble(v)) => *v = 0.0,
                        Some(NDPValue::AsInt(v)) => *v = 0,
                        _ => {}
                    }
                }
            }
            Some(MD::Gauge(g)) => {
                for dp in &mut g.data_points {
                    match &mut dp.value {
                        Some(NDPValue::AsDouble(v)) => *v = 0.0,
                        Some(NDPValue::AsInt(v)) => *v = 0,
                        _ => {}
                    }
                }
            }
            Some(MD::Histogram(h)) => {
                for dp in &mut h.data_points {
                    for bc in &mut dp.bucket_counts {
                        *bc = 0;
                    }
                    dp.count = 0;
                    dp.sum = Some(0.0);
                }
            }
            Some(MD::Summary(s)) => {
                for dp in &mut s.data_points {
                    for qv in &mut dp.quantile_values {
                        qv.value = 0.0;
                    }
                    dp.count = 0;
                    dp.sum = 0.0;
                }
            }
            Some(MD::ExponentialHistogram(eh)) => {
                for dp in &mut eh.data_points {
                    dp.count = 0;
                    dp.sum = Some(0.0);
                    dp.zero_count = 0;
                    if let Some(ref mut pos) = dp.positive {
                        for bc in &mut pos.bucket_counts {
                            *bc = 0;
                        }
                    }
                    if let Some(ref mut neg) = dp.negative {
                        for bc in &mut neg.bucket_counts {
                            *bc = 0;
                        }
                    }
                }
            }
            None => {}
        }
    }

    /// Set the first data point value (Sum or Gauge only).
    pub fn set_first_value(&mut self, val: f64) {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MD;
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NDPValue;

        match self.metric.data.as_mut() {
            Some(MD::Sum(s)) => {
                if let Some(dp) = s.data_points.first_mut() {
                    dp.value = Some(NDPValue::AsDouble(val));
                }
            }
            Some(MD::Gauge(g)) => {
                if let Some(dp) = g.data_points.first_mut() {
                    dp.value = Some(NDPValue::AsDouble(val));
                }
            }
            _ => {}
        }
    }

    /// Get the first data point value as f64, if this is a Sum or Gauge.
    pub fn first_value_as_f64(&self) -> Option<f64> {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MD;
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NDPValue;

        match self.metric.data.as_ref() {
            Some(MD::Sum(s)) => s.data_points.first().and_then(|dp| match &dp.value {
                Some(NDPValue::AsDouble(v)) => Some(*v),
                Some(NDPValue::AsInt(v)) => Some(OtlpMetricInt::from_proto(*v).to_f64()),
                _ => None,
            }),
            Some(MD::Gauge(g)) => g.data_points.first().and_then(|dp| match &dp.value {
                Some(NDPValue::AsDouble(v)) => Some(*v),
                Some(NDPValue::AsInt(v)) => Some(OtlpMetricInt::from_proto(*v).to_f64()),
                _ => None,
            }),
            _ => None,
        }
    }

    /// Check if this metric is a delta (incremental) type.
    /// Only Sum, Histogram, and `ExponentialHistogram` have `AggregationTemporality`.
    /// Gauge and Summary are point-in-time and neither delta nor cumulative.
    pub fn is_delta(&self) -> bool {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, metric::Data as MD};
        match self.metric.data.as_ref() {
            Some(MD::Sum(s)) => s.aggregation_temporality == AggregationTemporality::Delta as i32,
            Some(MD::Histogram(h)) => {
                h.aggregation_temporality == AggregationTemporality::Delta as i32
            }
            Some(MD::ExponentialHistogram(eh)) => {
                eh.aggregation_temporality == AggregationTemporality::Delta as i32
            }
            _ => false,
        }
    }

    /// Check if this metric is cumulative. Gauge and Summary have no temporality
    /// and return `false` (per `OTel` spec and otelcol-contrib behavior).
    pub fn is_cumulative(&self) -> bool {
        use opentelemetry_proto::tonic::metrics::v1::{AggregationTemporality, metric::Data as MD};
        match self.metric.data.as_ref() {
            Some(MD::Sum(s)) => {
                s.aggregation_temporality == AggregationTemporality::Cumulative as i32
            }
            Some(MD::Histogram(h)) => {
                h.aggregation_temporality == AggregationTemporality::Cumulative as i32
            }
            Some(MD::ExponentialHistogram(eh)) => {
                eh.aggregation_temporality == AggregationTemporality::Cumulative as i32
            }
            _ => false,
        }
    }

    /// Check if this metric type carries an aggregation temporality field.
    /// Sum, Histogram, and `ExponentialHistogram` have temporality.
    /// Gauge and Summary do not.
    pub fn has_temporality(&self) -> bool {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MD;
        matches!(
            self.metric.data.as_ref(),
            Some(MD::Sum(_) | MD::Histogram(_) | MD::ExponentialHistogram(_))
        )
    }

    /// Check if this metric is a Gauge type.
    pub fn is_gauge(&self) -> bool {
        matches!(
            self.metric.data.as_ref(),
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(_))
        )
    }

    /// Check if this metric is a Sum type.
    pub fn is_sum(&self) -> bool {
        matches!(
            self.metric.data.as_ref(),
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(
                _
            ))
        )
    }

    /// Check if this metric is a Set (stored as Gauge with `set_values` field populated).
    pub fn is_set(&self) -> bool {
        self.set_values.is_some()
    }

    pub fn set_values(&self) -> Option<&std::collections::BTreeSet<String>> {
        self.set_values.as_ref()
    }

    pub fn set_set_values(&mut self, values: std::collections::BTreeSet<String>) {
        self.set_values = Some(values);
    }

    pub fn kind_override(&self) -> Option<super::MetricKind> {
        self.kind_override
    }

    pub fn set_kind_override(&mut self, kind: Option<super::MetricKind>) {
        self.kind_override = kind;
    }

    /// Returns a zero-copy view of this metric's data, borrowing from the
    /// underlying proto where possible.
    pub fn view(&self) -> MetricView<'_> {
        use opentelemetry_proto::tonic::metrics::v1::{
            metric::Data, number_data_point::Value as NDPValue,
        };

        match self.metric.data.as_ref() {
            Some(Data::Sum(sum)) => {
                let val = sum
                    .data_points
                    .first()
                    .and_then(|p| p.value.as_ref())
                    .map_or(0.0, |v| match v {
                        NDPValue::AsDouble(f) => *f,
                        NDPValue::AsInt(i) => OtlpMetricInt::from_proto(*i).to_f64(),
                    });
                if sum.is_monotonic {
                    MetricView::Sum { value: val }
                } else {
                    MetricView::Gauge { value: val }
                }
            }
            Some(Data::Gauge(gauge)) => {
                if let Some(ref sv) = self.set_values {
                    MetricView::Set {
                        values: sv.iter().cloned().collect(),
                    }
                } else {
                    let val = gauge
                        .data_points
                        .first()
                        .and_then(|p| p.value.as_ref())
                        .map_or(0.0, |v| match v {
                            NDPValue::AsDouble(f) => *f,
                            NDPValue::AsInt(i) => OtlpMetricInt::from_proto(*i).to_f64(),
                        });
                    MetricView::Gauge { value: val }
                }
            }
            Some(Data::Histogram(hist)) => match hist.data_points.first() {
                Some(p) => MetricView::Histogram {
                    bounds: &p.explicit_bounds,
                    counts: &p.bucket_counts,
                    count: p.count,
                    sum: p.sum.unwrap_or(0.0),
                },
                None => MetricView::Histogram {
                    bounds: &[],
                    counts: &[],
                    count: 0,
                    sum: 0.0,
                },
            },
            Some(Data::Summary(summary)) => match summary.data_points.first() {
                Some(p) => MetricView::Summary {
                    quantiles: &p.quantile_values,
                    count: p.count,
                    sum: p.sum,
                },
                None => MetricView::Summary {
                    quantiles: &[],
                    count: 0,
                    sum: 0.0,
                },
            },
            Some(Data::ExponentialHistogram(exp)) => match exp.data_points.first() {
                Some(p) => MetricView::ExponentialHistogram {
                    scale: p.scale,
                    count: p.count,
                    sum: p.sum.unwrap_or(0.0),
                    zero_count: p.zero_count,
                    positive: p.positive.as_ref(),
                    negative: p.negative.as_ref(),
                    min: p.min,
                    max: p.max,
                },
                None => MetricView::ExponentialHistogram {
                    scale: 0,
                    count: 0,
                    sum: 0.0,
                    zero_count: 0,
                    positive: None,
                    negative: None,
                    min: None,
                    max: None,
                },
            },
            None => MetricView::Gauge { value: 0.0 },
        }
    }

    /// If this metric is an `ExponentialHistogram`, convert it in-place to an
    /// explicit-bounds Histogram using the given target bucket boundaries.
    /// Returns true if conversion happened, false if the metric was not an
    /// `ExponentialHistogram`.
    pub fn convert_exponential_to_histogram(&mut self, target_bounds: &[f64]) -> bool {
        use opentelemetry_proto::tonic::metrics::v1::{
            Histogram, HistogramDataPoint, metric::Data,
        };

        let (exp_hist_temporality, dp) = match self.metric.data.as_ref() {
            Some(Data::ExponentialHistogram(exp)) => {
                (exp.aggregation_temporality, exp.data_points.first())
            }
            _ => return false,
        };

        let Some(dp) = dp else {
            self.metric.data = Some(Data::Histogram(Histogram {
                data_points: vec![],
                aggregation_temporality: exp_hist_temporality,
            }));
            return true;
        };

        let scale = dp.scale;
        let base: f64 = 2.0_f64.powf(2.0_f64.powi(-scale));

        let n = target_bounds.len();
        let mut bucket_counts = vec![0u64; n + 1];

        let mut place_value = |value: f64, count: u64| {
            let pos = target_bounds.partition_point(|&b| b < value);
            bucket_counts[pos] += count;
        };

        if dp.zero_count > 0 {
            place_value(0.0, dp.zero_count);
        }

        if let Some(ref pos) = dp.positive {
            #[expect(clippy::cast_possible_truncation, reason = "bucket index fits in i32")]
            for (i, &c) in pos.bucket_counts.iter().enumerate() {
                if c == 0 {
                    continue;
                }
                let idx = pos.offset + i as i32;
                let lower = base.powf(f64::from(idx));
                let upper = base.powf(f64::from(idx + 1));
                let midpoint = f64::midpoint(lower, upper);
                place_value(midpoint, c);
            }
        }

        if let Some(ref neg) = dp.negative {
            #[expect(clippy::cast_possible_truncation, reason = "bucket index fits in i32")]
            for (i, &c) in neg.bucket_counts.iter().enumerate() {
                if c == 0 {
                    continue;
                }
                let idx = neg.offset + i as i32;
                let lower = base.powf(f64::from(idx));
                let upper = base.powf(f64::from(idx + 1));
                let midpoint = -f64::midpoint(lower, upper);
                place_value(midpoint, c);
            }
        }

        let new_dp = HistogramDataPoint {
            attributes: vec![],
            start_time_unix_nano: dp.start_time_unix_nano,
            time_unix_nano: dp.time_unix_nano,
            count: dp.count,
            sum: dp.sum,
            bucket_counts,
            explicit_bounds: target_bounds.to_vec(),
            exemplars: dp.exemplars.clone(),
            flags: dp.flags,
            min: dp.min,
            max: dp.max,
        };

        self.metric.data = Some(Data::Histogram(Histogram {
            data_points: vec![new_dp],
            aggregation_temporality: exp_hist_temporality,
        }));

        true
    }

    /// Convert this metric to an `AnyValue::KvlistValue` suitable for use as
    /// an `OtelLog` body. Includes name, description, unit, and data (sum/gauge/
    /// histogram/summary/exponentialHistogram). Does NOT include resource/scope
    /// — those should be transferred directly to the `OtelLog`.
    pub fn to_log_body(&self) -> AnyValue {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;

        let metric = self.metric_proto();
        let mut kvs: Vec<KeyValue> = Vec::new();

        kvs.push(KeyValue {
            key: f::NAME.into(),
            value: Some(string_value(&metric.name)),
        });
        if !metric.description.is_empty() {
            kvs.push(KeyValue {
                key: f::DESCRIPTION.into(),
                value: Some(string_value(&metric.description)),
            });
        }
        if !metric.unit.is_empty() {
            kvs.push(KeyValue {
                key: f::UNIT.into(),
                value: Some(string_value(&metric.unit)),
            });
        }

        if let Some(ref data) = metric.data {
            match data {
                MetricData::Sum(sum) => {
                    kvs.push(KeyValue {
                        key: f::METRIC_TYPE_SUM.into(),
                        value: Some(sum_to_any_value(sum)),
                    });
                }
                MetricData::Gauge(gauge) => {
                    kvs.push(KeyValue {
                        key: f::METRIC_TYPE_GAUGE.into(),
                        value: Some(gauge_to_any_value(gauge)),
                    });
                }
                MetricData::Histogram(hist) => {
                    kvs.push(KeyValue {
                        key: f::METRIC_TYPE_HISTOGRAM.into(),
                        value: Some(histogram_to_any_value(hist)),
                    });
                }
                MetricData::Summary(summary) => {
                    kvs.push(KeyValue {
                        key: f::METRIC_TYPE_SUMMARY.into(),
                        value: Some(summary_to_any_value(summary)),
                    });
                }
                MetricData::ExponentialHistogram(exp) => {
                    kvs.push(KeyValue {
                        key: f::EXPONENTIAL_HISTOGRAM_CC.into(),
                        value: Some(exp_histogram_to_any_value(exp)),
                    });
                }
            }
        }

        AnyValue {
            value: Some(OtelValueKind::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList { values: kvs },
            )),
        }
    }
}

// -- Helper functions for to_log_body --

fn double_value(d: f64) -> AnyValue {
    AnyValue {
        value: Some(OtelValueKind::DoubleValue(d)),
    }
}

fn bool_value(b: bool) -> AnyValue {
    AnyValue {
        value: Some(OtelValueKind::BoolValue(b)),
    }
}

fn kvlist_any_value(kvs: Vec<KeyValue>) -> AnyValue {
    AnyValue {
        value: Some(OtelValueKind::KvlistValue(
            opentelemetry_proto::tonic::common::v1::KeyValueList { values: kvs },
        )),
    }
}

fn array_any_value(values: Vec<AnyValue>) -> AnyValue {
    AnyValue {
        value: Some(OtelValueKind::ArrayValue(
            opentelemetry_proto::tonic::common::v1::ArrayValue { values },
        )),
    }
}

fn attrs_to_any_value(attrs: &[KeyValue]) -> AnyValue {
    array_any_value(
        attrs
            .iter()
            .map(|kv| {
                let mut inner = vec![KeyValue {
                    key: f::KEY.into(),
                    value: Some(string_value(&kv.key)),
                }];
                if let Some(ref v) = kv.value {
                    inner.push(KeyValue {
                        key: f::VALUE.into(),
                        value: Some(v.clone()),
                    });
                }
                kvlist_any_value(inner)
            })
            .collect(),
    )
}

fn number_dp_to_any_value(
    dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
) -> AnyValue {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NDPValue;
    let mut kvs = Vec::new();
    if let Some(ref v) = dp.value {
        match v {
            NDPValue::AsDouble(d) => kvs.push(KeyValue {
                key: f::AS_DOUBLE.into(),
                value: Some(double_value(*d)),
            }),
            NDPValue::AsInt(i) => kvs.push(KeyValue {
                key: f::AS_INT.into(),
                value: Some(int_value(*i)),
            }),
        }
    }
    if !dp.attributes.is_empty() {
        kvs.push(KeyValue {
            key: f::ATTRIBUTES.into(),
            value: Some(attrs_to_any_value(&dp.attributes)),
        });
    }
    if dp.time_unix_nano != 0 {
        kvs.push(KeyValue {
            key: f::TIME_UNIX_NANO_CC.into(),
            value: Some(string_value(dp.time_unix_nano.to_string())),
        });
    }
    if dp.start_time_unix_nano != 0 {
        kvs.push(KeyValue {
            key: f::START_TIME_UNIX_NANO_CC.into(),
            value: Some(string_value(dp.start_time_unix_nano.to_string())),
        });
    }
    kvlist_any_value(kvs)
}

fn sum_to_any_value(sum: &opentelemetry_proto::tonic::metrics::v1::Sum) -> AnyValue {
    let mut kvs = Vec::new();
    let dps: Vec<AnyValue> = sum.data_points.iter().map(number_dp_to_any_value).collect();
    kvs.push(KeyValue {
        key: f::DATA_POINTS.into(),
        value: Some(array_any_value(dps)),
    });
    kvs.push(KeyValue {
        key: f::AGGREGATION_TEMPORALITY.into(),
        value: Some(int_value(i64::from(sum.aggregation_temporality))),
    });
    kvs.push(KeyValue {
        key: f::IS_MONOTONIC.into(),
        value: Some(bool_value(sum.is_monotonic)),
    });
    kvlist_any_value(kvs)
}

fn gauge_to_any_value(gauge: &opentelemetry_proto::tonic::metrics::v1::Gauge) -> AnyValue {
    let mut kvs = Vec::new();
    let dps: Vec<AnyValue> = gauge
        .data_points
        .iter()
        .map(number_dp_to_any_value)
        .collect();
    kvs.push(KeyValue {
        key: f::DATA_POINTS.into(),
        value: Some(array_any_value(dps)),
    });
    kvlist_any_value(kvs)
}

fn histogram_to_any_value(hist: &opentelemetry_proto::tonic::metrics::v1::Histogram) -> AnyValue {
    let mut kvs = Vec::new();
    let dps: Vec<AnyValue> = hist
        .data_points
        .iter()
        .map(|dp| {
            let mut m = Vec::new();
            if !dp.attributes.is_empty() {
                m.push(KeyValue {
                    key: f::ATTRIBUTES.into(),
                    value: Some(attrs_to_any_value(&dp.attributes)),
                });
            }
            if dp.time_unix_nano != 0 {
                m.push(KeyValue {
                    key: f::TIME_UNIX_NANO_CC.into(),
                    value: Some(string_value(dp.time_unix_nano.to_string())),
                });
            }
            m.push(KeyValue {
                key: f::COUNT.into(),
                value: Some(string_value(dp.count.to_string())),
            });
            if let Some(sum) = dp.sum {
                m.push(KeyValue {
                    key: f::METRIC_TYPE_SUM.into(),
                    value: Some(double_value(sum)),
                });
            }
            if !dp.bucket_counts.is_empty() {
                m.push(KeyValue {
                    key: f::BUCKET_COUNTS.into(),
                    value: Some(array_any_value(
                        dp.bucket_counts
                            .iter()
                            .map(|c| string_value(c.to_string()))
                            .collect(),
                    )),
                });
            }
            if !dp.explicit_bounds.is_empty() {
                m.push(KeyValue {
                    key: f::EXPLICIT_BOUNDS.into(),
                    value: Some(array_any_value(
                        dp.explicit_bounds
                            .iter()
                            .map(|b| double_value(*b))
                            .collect(),
                    )),
                });
            }
            kvlist_any_value(m)
        })
        .collect();
    kvs.push(KeyValue {
        key: f::DATA_POINTS.into(),
        value: Some(array_any_value(dps)),
    });
    kvs.push(KeyValue {
        key: f::AGGREGATION_TEMPORALITY.into(),
        value: Some(int_value(i64::from(hist.aggregation_temporality))),
    });
    kvlist_any_value(kvs)
}

fn summary_to_any_value(summary: &opentelemetry_proto::tonic::metrics::v1::Summary) -> AnyValue {
    let mut kvs = Vec::new();
    let dps: Vec<AnyValue> = summary
        .data_points
        .iter()
        .map(|dp| {
            let mut m = Vec::new();
            if !dp.attributes.is_empty() {
                m.push(KeyValue {
                    key: f::ATTRIBUTES.into(),
                    value: Some(attrs_to_any_value(&dp.attributes)),
                });
            }
            if dp.time_unix_nano != 0 {
                m.push(KeyValue {
                    key: f::TIME_UNIX_NANO_CC.into(),
                    value: Some(string_value(dp.time_unix_nano.to_string())),
                });
            }
            m.push(KeyValue {
                key: f::COUNT.into(),
                value: Some(string_value(dp.count.to_string())),
            });
            m.push(KeyValue {
                key: f::METRIC_TYPE_SUM.into(),
                value: Some(double_value(dp.sum)),
            });
            let qvs: Vec<AnyValue> = dp
                .quantile_values
                .iter()
                .map(|q| {
                    kvlist_any_value(vec![
                        KeyValue {
                            key: f::QUANTILE.into(),
                            value: Some(double_value(q.quantile)),
                        },
                        KeyValue {
                            key: f::VALUE.into(),
                            value: Some(double_value(q.value)),
                        },
                    ])
                })
                .collect();
            m.push(KeyValue {
                key: f::QUANTILE_VALUES.into(),
                value: Some(array_any_value(qvs)),
            });
            kvlist_any_value(m)
        })
        .collect();
    kvs.push(KeyValue {
        key: f::DATA_POINTS.into(),
        value: Some(array_any_value(dps)),
    });
    kvlist_any_value(kvs)
}

fn exp_histogram_to_any_value(
    exp: &opentelemetry_proto::tonic::metrics::v1::ExponentialHistogram,
) -> AnyValue {
    let mut kvs = Vec::new();
    let dps: Vec<AnyValue> = exp
        .data_points
        .iter()
        .map(|dp| {
            let mut m = Vec::new();
            if !dp.attributes.is_empty() {
                m.push(KeyValue {
                    key: f::ATTRIBUTES.into(),
                    value: Some(attrs_to_any_value(&dp.attributes)),
                });
            }
            if dp.time_unix_nano != 0 {
                m.push(KeyValue {
                    key: f::TIME_UNIX_NANO_CC.into(),
                    value: Some(string_value(dp.time_unix_nano.to_string())),
                });
            }
            m.push(KeyValue {
                key: f::COUNT.into(),
                value: Some(string_value(dp.count.to_string())),
            });
            if let Some(sum) = dp.sum {
                m.push(KeyValue {
                    key: f::METRIC_TYPE_SUM.into(),
                    value: Some(double_value(sum)),
                });
            }
            m.push(KeyValue {
                key: f::SCALE.into(),
                value: Some(int_value(i64::from(dp.scale))),
            });
            m.push(KeyValue {
                key: f::ZERO_COUNT.into(),
                value: Some(string_value(dp.zero_count.to_string())),
            });
            if let Some(ref pos) = dp.positive {
                m.push(KeyValue {
                    key: f::POSITIVE.into(),
                    value: Some(kvlist_any_value(vec![
                        KeyValue {
                            key: f::OFFSET.into(),
                            value: Some(int_value(i64::from(pos.offset))),
                        },
                        KeyValue {
                            key: f::BUCKET_COUNTS.into(),
                            value: Some(array_any_value(
                                pos.bucket_counts
                                    .iter()
                                    .map(|c| string_value(c.to_string()))
                                    .collect(),
                            )),
                        },
                    ])),
                });
            }
            if let Some(ref neg) = dp.negative {
                m.push(KeyValue {
                    key: f::NEGATIVE.into(),
                    value: Some(kvlist_any_value(vec![
                        KeyValue {
                            key: f::OFFSET.into(),
                            value: Some(int_value(i64::from(neg.offset))),
                        },
                        KeyValue {
                            key: f::BUCKET_COUNTS.into(),
                            value: Some(array_any_value(
                                neg.bucket_counts
                                    .iter()
                                    .map(|c| string_value(c.to_string()))
                                    .collect(),
                            )),
                        },
                    ])),
                });
            }
            kvlist_any_value(m)
        })
        .collect();
    kvs.push(KeyValue {
        key: f::DATA_POINTS.into(),
        value: Some(array_any_value(dps)),
    });
    kvs.push(KeyValue {
        key: f::AGGREGATION_TEMPORALITY.into(),
        value: Some(int_value(i64::from(exp.aggregation_temporality))),
    });
    kvlist_any_value(kvs)
}

// -- Trait implementations --

impl ByteSizeOf for OtelMetric {
    fn allocated_bytes(&self) -> usize {
        self.metric.encoded_len()
            + self
                .resource
                .as_ref()
                .map_or(0, prost::Message::encoded_len)
            + self.scope.as_ref().map_or(0, prost::Message::encoded_len)
            + self.metadata.allocated_bytes()
    }
}

impl EstimatedJsonEncodedSizeOf for OtelMetric {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        // Approximate: proto encoded_len * 3 accounts for JSON overhead
        // (field names, quoting, braces). For OtelLog/OtelSpan this should
        // closely match `as_map().estimated_json_encoded_size_of()`.
        JsonSize::new(self.metric.encoded_len() * 3)
    }
}

impl EventCount for OtelMetric {
    fn event_count(&self) -> usize {
        1
    }
}

impl Finalizable for OtelMetric {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.metadata.take_finalizers()
    }
}

// Override GetEventCountTags for OtelMetric with proper source/service extraction.
impl GetEventCountTags for OtelMetric {
    fn get_tags(&self) -> TaggedEventsSent {
        use crate::config::telemetry;
        use sol_common::internal_event::OptionalTag;

        let source = if telemetry().tags().emit_source {
            self.metadata().source_id().cloned().into()
        } else {
            OptionalTag::Ignored
        };

        let service = if telemetry().tags().emit_service {
            self.resource_attribute(f::SERVICE_NAME)
                .and_then(|av| match &av.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => Some(s.clone()),
                    _ => None,
                })
                .into()
        } else {
            OptionalTag::Ignored
        };

        TaggedEventsSent { source, service }
    }
}

impl EventDataEq for OtelMetric {
    fn event_data_eq(&self, other: &Self) -> bool {
        self.metric == other.metric
            && self.dp_attrs == other.dp_attrs
            && self.resource == other.resource
            && self.resource_attrs == other.resource_attrs
            && self.scope == other.scope
            && self.scope_attrs == other.scope_attrs
    }
}

impl Serialize for OtelMetric {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use super::otel_json::{
            SerializableExpHistogram, SerializableGauge, SerializableHistogram,
            SerializableResource, SerializableScope, SerializableSum, SerializableSummary,
        };
        use opentelemetry_proto::tonic::metrics::v1::metric;
        use serde::ser::SerializeMap;

        let metric_with_attrs = self.metric_proto();

        let mut len = 1;
        if !metric_with_attrs.description.is_empty() {
            len += 1;
        }
        if !metric_with_attrs.unit.is_empty() {
            len += 1;
        }
        if metric_with_attrs.data.is_some() {
            len += 1;
        }
        if self.resource.is_some() {
            len += 1;
        }
        if self.scope.is_some() {
            len += 1;
        }

        let mut map = serializer.serialize_map(Some(len))?;
        map.serialize_entry(f::NAME, &metric_with_attrs.name)?;
        if !metric_with_attrs.description.is_empty() {
            map.serialize_entry(f::DESCRIPTION, &metric_with_attrs.description)?;
        }
        if !metric_with_attrs.unit.is_empty() {
            map.serialize_entry(f::UNIT, &metric_with_attrs.unit)?;
        }

        if let Some(ref data) = metric_with_attrs.data {
            match data {
                metric::Data::Sum(sum) => {
                    map.serialize_entry(f::METRIC_TYPE_SUM, &SerializableSum(sum))?;
                }
                metric::Data::Gauge(gauge) => {
                    map.serialize_entry(f::METRIC_TYPE_GAUGE, &SerializableGauge(gauge))?;
                }
                metric::Data::Histogram(hist) => {
                    map.serialize_entry(f::METRIC_TYPE_HISTOGRAM, &SerializableHistogram(hist))?;
                }
                metric::Data::Summary(summary) => {
                    map.serialize_entry(f::METRIC_TYPE_SUMMARY, &SerializableSummary(summary))?;
                }
                metric::Data::ExponentialHistogram(exp) => {
                    map.serialize_entry(
                        f::EXPONENTIAL_HISTOGRAM_CC,
                        &SerializableExpHistogram(exp),
                    )?;
                }
            }
        }

        if self.resource.is_some() || !self.resource_attrs.is_empty() {
            let mut res = self.resource.clone().unwrap_or_default();
            res.attributes = self.resource_attrs.to_key_values();
            map.serialize_entry(f::RESOURCE, &SerializableResource(&res))?;
        }
        if self.scope.is_some() || !self.scope_attrs.is_empty() {
            let mut scope = self.scope.clone().unwrap_or_default();
            scope.attributes = self.scope_attrs.to_key_values();
            map.serialize_entry(f::SCOPE, &SerializableScope(&scope))?;
        }
        map.end()
    }
}

impl std::fmt::Display for OtelMetric {
    /// Display in Prometheus-like text format:
    /// `TIMESTAMP NAMESPACE_NAME{TAGS} KIND VALUE`
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ts) = self.timestamp() {
            write!(fmt, "{ts:?} ")?;
        }
        if let Some(ns) = self.namespace() {
            write!(fmt, "{ns}_")?;
        }
        write!(fmt, "{}", self.name())?;
        write!(fmt, "{{")?;
        if let Some(tags) = self.tags() {
            let mut first = true;
            for (tag, value) in tags.iter_single() {
                if !first {
                    write!(fmt, ",")?;
                }
                first = false;
                match value {
                    Some(v) => write!(fmt, "{tag}={v:?}")?,
                    None => write!(fmt, "{tag}")?,
                }
            }
        }
        write!(fmt, "}}")?;
        let kind_char = match self.kind() {
            super::MetricKind::Absolute => '=',
            super::MetricKind::Incremental => '+',
        };
        write!(fmt, " {kind_char} {}", self.view())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::metrics::v1::{
        ExponentialHistogram, ExponentialHistogramDataPoint,
        exponential_histogram_data_point::Buckets, metric::Data,
    };

    fn make_exp_hist(
        scale: i32,
        positive_offset: i32,
        positive_counts: Vec<u64>,
        zero_count: u64,
        count: u64,
        sum: f64,
    ) -> OtelMetric {
        let proto = OtelMetricProto {
            name: "test_exp_hist".into(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 100,
                    time_unix_nano: 200,
                    count,
                    sum: Some(sum),
                    scale,
                    zero_count,
                    positive: Some(Buckets {
                        offset: positive_offset,
                        bucket_counts: positive_counts,
                    }),
                    negative: None,
                    flags: 0,
                    exemplars: vec![],
                    min: Some(0.001),
                    max: Some(10.0),
                    zero_threshold: 0.0,
                }],
                aggregation_temporality: 2, // CUMULATIVE
            })),
        };
        OtelMetric::new(proto)
    }

    #[test]
    fn convert_exp_hist_to_histogram_basic() {
        let bounds = vec![
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];
        let mut metric = make_exp_hist(0, 0, vec![5, 3, 2], 1, 11, 7.5);

        assert!(metric.convert_exponential_to_histogram(&bounds));

        match metric.view() {
            MetricView::Histogram {
                bounds: b,
                counts,
                count,
                sum,
            } => {
                assert_eq!(b.len(), bounds.len());
                assert_eq!(counts.len(), bounds.len() + 1);
                assert_eq!(count, 11);
                assert!((sum - 7.5).abs() < f64::EPSILON);
                let total: u64 = counts.iter().sum();
                assert_eq!(total, 11);
            }
            other => panic!("Expected Histogram, got: {other}"),
        }
    }

    #[test]
    fn convert_exp_hist_preserves_timestamps() {
        let bounds = vec![1.0, 10.0];
        let mut metric = make_exp_hist(0, 0, vec![5], 0, 5, 5.0);

        metric.convert_exponential_to_histogram(&bounds);

        if let Some(Data::Histogram(h)) = metric.metric_proto().data.as_ref() {
            let dp = &h.data_points[0];
            assert_eq!(dp.start_time_unix_nano, 100);
            assert_eq!(dp.time_unix_nano, 200);
        } else {
            panic!("Expected Histogram data after conversion");
        }
    }

    #[test]
    fn convert_exp_hist_preserves_temporality() {
        let bounds = vec![1.0];
        let mut metric = make_exp_hist(0, 0, vec![1], 0, 1, 1.0);

        metric.convert_exponential_to_histogram(&bounds);

        assert!(metric.is_cumulative());
    }

    #[test]
    fn convert_noop_on_non_exp_hist() {
        let mut metric = OtelMetric::new_gauge("test_gauge", 42.0);
        assert!(!metric.convert_exponential_to_histogram(&[1.0, 10.0]));
        match metric.view() {
            MetricView::Gauge { .. } => {}
            other => panic!("Expected Gauge, got: {other}"),
        }
    }

    #[test]
    fn convert_exp_hist_empty_data_points() {
        let proto = OtelMetricProto {
            name: "empty_exp".into(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![],
            data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![],
                aggregation_temporality: 1, // DELTA
            })),
        };
        let mut metric = OtelMetric::new(proto);

        assert!(metric.convert_exponential_to_histogram(&[1.0, 10.0]));

        assert!(metric.is_delta());
    }

    #[test]
    fn convert_exp_hist_zero_count_placed_correctly() {
        let bounds = vec![0.005, 0.01];
        let mut metric = make_exp_hist(0, 5, vec![], 10, 10, 0.0);

        metric.convert_exponential_to_histogram(&bounds);

        match metric.view() {
            MetricView::Histogram { counts, .. } => {
                assert_eq!(
                    counts[0], 10,
                    "zero_count should land in first bucket (<=0.005)"
                );
            }
            other => panic!("Expected Histogram, got: {other}"),
        }
    }

    #[test]
    fn new_histogram_bucket_counts_has_overflow() {
        let buckets = vec![
            super::super::metric::Bucket {
                upper_limit: 1.0,
                count: 5,
            },
            super::super::metric::Bucket {
                upper_limit: 5.0,
                count: 3,
            },
            super::super::metric::Bucket {
                upper_limit: 10.0,
                count: 2,
            },
        ];
        let metric = OtelMetric::new_histogram(
            "test",
            super::super::MetricKind::Incremental,
            &buckets,
            10,
            25.0,
        );
        match metric.view() {
            MetricView::Histogram {
                bounds,
                counts,
                count,
                sum,
            } => {
                assert_eq!(bounds, &[1.0, 5.0, 10.0]);
                assert_eq!(
                    counts.len(),
                    bounds.len() + 1,
                    "OTLP requires overflow bucket"
                );
                assert_eq!(
                    counts,
                    &[5, 3, 2, 0],
                    "overflow bucket should be 0 when no infinity"
                );
                assert_eq!(count, 10);
                assert!((sum - 25.0).abs() < f64::EPSILON);
            }
            other => panic!("Expected Histogram, got: {other}"),
        }
    }

    #[test]
    fn new_histogram_strips_infinity_bound() {
        let buckets = vec![
            super::super::metric::Bucket {
                upper_limit: 1.0,
                count: 5,
            },
            super::super::metric::Bucket {
                upper_limit: 5.0,
                count: 3,
            },
            super::super::metric::Bucket {
                upper_limit: f64::INFINITY,
                count: 2,
            },
        ];
        let metric = OtelMetric::new_histogram(
            "test",
            super::super::MetricKind::Absolute,
            &buckets,
            10,
            15.0,
        );
        match metric.view() {
            MetricView::Histogram { bounds, counts, .. } => {
                assert_eq!(
                    bounds,
                    &[1.0, 5.0],
                    "infinity should be stripped from bounds"
                );
                assert_eq!(counts.len(), bounds.len() + 1);
                assert_eq!(counts, &[5, 3, 2], "infinity bucket count becomes overflow");
            }
            other => panic!("Expected Histogram, got: {other}"),
        }
    }

    #[test]
    fn new_histogram_from_samples_has_overflow() {
        let metric = OtelMetric::new_histogram_from_samples(
            "test_samples",
            super::super::MetricKind::Incremental,
            &crate::samples![1.0 => 3, 2.0 => 7],
        );
        match metric.view() {
            MetricView::Histogram {
                bounds,
                counts,
                count,
                ..
            } => {
                assert_eq!(
                    counts.len(),
                    bounds.len() + 1,
                    "samples histogram must have overflow bucket"
                );
                assert_eq!(count, 10);
            }
            other => panic!("Expected Histogram, got: {other}"),
        }
    }

    #[test]
    fn subtract_exponential_histogram() {
        let mut a = make_exp_hist(0, 0, vec![10, 6, 4], 2, 22, 15.0);
        let b = make_exp_hist(0, 0, vec![3, 2, 1], 1, 7, 5.0);

        assert!(a.subtract(&b), "subtract matching ExpHists should succeed");

        if let Some(Data::ExponentialHistogram(eh)) = a.metric().data.as_ref() {
            let dp = &eh.data_points[0];
            assert_eq!(dp.count, 15);
            assert!((dp.sum.unwrap() - 10.0).abs() < f64::EPSILON);
            assert_eq!(dp.zero_count, 1);
            let pos = dp.positive.as_ref().unwrap();
            assert_eq!(pos.bucket_counts, vec![7, 4, 3]);
        } else {
            panic!("expected ExponentialHistogram");
        }
    }

    #[test]
    fn subtract_exponential_histogram_mismatched_scale() {
        let mut a = make_exp_hist(0, 0, vec![5], 0, 5, 5.0);
        let b = make_exp_hist(1, 0, vec![5], 0, 5, 5.0);

        assert!(!a.subtract(&b), "mismatched scale should fail");
    }

    #[test]
    fn subtract_exponential_histogram_underflow() {
        let mut a = make_exp_hist(0, 0, vec![2], 0, 2, 2.0);
        let b = make_exp_hist(0, 0, vec![5], 0, 5, 5.0);

        assert!(!a.subtract(&b), "underflow should fail");
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "reference implementation mirrors production IEEE 754 algorithm"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "reference implementation mirrors production IEEE 754 algorithm"
    )]
    #[test]
    fn exp_hist_single_index_matches_statsd() {
        fn map_to_index_reference(value: f64, scale: i32) -> i32 {
            fn frexp_ref(x: f64) -> (f64, i32) {
                if x == 0.0 {
                    return (0.0, 0);
                }
                let bits = x.to_bits();
                let exp_raw = ((bits >> 52) & 0x7FF) as i32;
                let exp = exp_raw - 1022;
                let frac = f64::from_bits((bits & 0x800F_FFFF_FFFF_FFFF) | 0x3FE0_0000_0000_0000);
                (frac, exp)
            }
            let (frac, exp) = frexp_ref(value);
            if frac == 0.5 {
                ((exp - 1) << scale) - 1
            } else {
                let scale_factor = std::f64::consts::LOG2_E * (1u64 << scale as u64) as f64;
                (value.ln() * scale_factor).floor() as i32
            }
        }

        let test_values = [0.001, 0.5, 1.0, 2.0, 2.5, 3.125, 10.0, 100.0, 1000.0];
        for &val in &test_values {
            let metric = OtelMetric::new_exponential_histogram_single("test", val);
            if let Some(Data::ExponentialHistogram(eh)) = metric.metric().data.as_ref() {
                let dp = &eh.data_points[0];
                let actual_index = dp.positive.as_ref().unwrap().offset;
                let expected = map_to_index_reference(val, 20);
                assert_eq!(actual_index, expected, "index mismatch for value {val}");
            }
        }
    }

    #[test]
    fn summary_kind_reads_kind_override() {
        use opentelemetry_proto::tonic::metrics::v1::{Summary, SummaryDataPoint};
        let proto = OtelMetricProto {
            name: "test_summary".into(),
            data: Some(Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    count: 10,
                    sum: 50.0,
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };
        let mut metric = OtelMetric::new(proto);
        assert_eq!(metric.kind(), super::super::MetricKind::Absolute);

        metric.set_kind(super::super::MetricKind::Incremental);
        assert_eq!(
            metric.kind(),
            super::super::MetricKind::Incremental,
            "Summary should read kind_override"
        );
    }

    #[test]
    fn exp_hist_min_max_none_preserved() {
        let metric = OtelMetric::new_exponential_histogram(
            "test",
            20, 10, 50.0, 0,
            opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![10],
            },
            opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets::default(),
            None,
            None,
        );
        if let Some(Data::ExponentialHistogram(eh)) = metric.metric().data.as_ref() {
            let dp = &eh.data_points[0];
            assert!(dp.min.is_none(), "min should be None, not Some(0.0)");
            assert!(dp.max.is_none(), "max should be None, not Some(0.0)");
        } else {
            panic!("expected ExponentialHistogram");
        }
    }
}
