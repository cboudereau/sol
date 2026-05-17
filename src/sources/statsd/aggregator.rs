use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use sol_lib::event::otel_metric::{
    ExpBuckets as ProtoBuckets, InstrumentationScope, MetricData, Resource,
};

use crate::event::OtelAttributes;
use crate::event::OtelMetric;

#[derive(Clone, Debug)]
pub struct AggregatorConfig {
    pub flush_interval: Duration,
    pub gauge_ttl: Duration,
    pub is_monotonic: bool,
    pub timer_unit: String,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(10),
            gauge_ttl: Duration::from_secs(300),
            is_monotonic: true,
            timer_unit: "s".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetricKey {
    pub name: String,
    pub tags: Option<OtelAttributes>,
}

pub enum ParsedMetric {
    Counter {
        key: MetricKey,
        value: f64,
    },
    Gauge {
        key: MetricKey,
        value: f64,
        direction: Option<f64>,
    },
    Timer {
        key: MetricKey,
        value: f64,
        sample_rate: f64,
    },
    Set {
        key: MetricKey,
        value: String,
    },
}

impl ParsedMetric {
    pub const fn key(&self) -> &MetricKey {
        match self {
            Self::Counter { key, .. }
            | Self::Gauge { key, .. }
            | Self::Timer { key, .. }
            | Self::Set { key, .. } => key,
        }
    }
}

struct AggCounter {
    value: f64,
}

struct AggGauge {
    value: f64,
    last_update: Instant,
}

struct AggSet {
    values: BTreeSet<String>,
}

pub struct Aggregator {
    config: AggregatorConfig,
    counters: HashMap<MetricKey, AggCounter>,
    gauges: HashMap<MetricKey, AggGauge>,
    sets: HashMap<MetricKey, AggSet>,
    histograms: HashMap<MetricKey, ExpoHistogram>,
    last_flush_ns: u64,
}

impl Aggregator {
    pub fn new(config: AggregatorConfig) -> Self {
        Self {
            config,
            counters: HashMap::new(),
            gauges: HashMap::new(),
            sets: HashMap::new(),
            histograms: HashMap::new(),
            last_flush_ns: now_nanos(),
        }
    }

    pub fn record(&mut self, metric: ParsedMetric) {
        match metric {
            ParsedMetric::Counter { key, value } => {
                self.counters
                    .entry(key)
                    .and_modify(|c| c.value += value)
                    .or_insert(AggCounter { value });
            }
            ParsedMetric::Gauge {
                key,
                value,
                direction,
            } => {
                let now = Instant::now();
                match direction {
                    None => {
                        self.gauges.insert(
                            key,
                            AggGauge {
                                value,
                                last_update: now,
                            },
                        );
                    }
                    Some(sign) => {
                        self.gauges
                            .entry(key)
                            .and_modify(|g| {
                                g.value += value * sign;
                                g.last_update = now;
                            })
                            .or_insert(AggGauge {
                                value: value * sign,
                                last_update: now,
                            });
                    }
                }
            }
            ParsedMetric::Set { key, value } => {
                self.sets
                    .entry(key)
                    .and_modify(|s| {
                        s.values.insert(value.clone());
                    })
                    .or_insert(AggSet {
                        values: BTreeSet::from([value]),
                    });
            }
            ParsedMetric::Timer {
                key,
                value,
                sample_rate,
            } => {
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "inverse sample rate is a small positive integer (e.g. 1/0.1 = 10)"
                )]
                let count = if sample_rate > 0.0 && sample_rate < 1.0 {
                    (1.0 / sample_rate).round() as u64
                } else {
                    1
                };
                self.histograms
                    .entry(key)
                    .and_modify(|h| h.record(value, count))
                    .or_insert_with(|| {
                        let mut h = ExpoHistogram::new(EXPO_MAX_SIZE, EXPO_START_SCALE);
                        h.record(value, count);
                        h
                    });
            }
        }
    }

    pub fn flush(&mut self, resource: &Resource, scope: &InstrumentationScope) -> Vec<OtelMetric> {
        let flush_time_ns = now_nanos();
        let start_time_ns = self.last_flush_ns;
        self.last_flush_ns = flush_time_ns;

        let mut metrics = Vec::new();

        // Counters → Sum(Delta, is_monotonic from config), unit="1"
        for (key, counter) in self.counters.drain() {
            let mut m = OtelMetric::new_counter(
                &key.name,
                crate::event::metric::MetricKind::Incremental,
                counter.value,
            )
            .with_unit("1");
            if let Some(tags) = key.tags {
                m = m.with_tags(Some(tags));
            }
            m.set_resource(resource.clone());
            m.set_scope(scope.clone());
            set_timestamps(&mut m, start_time_ns, flush_time_ns);
            set_is_monotonic(&mut m, self.config.is_monotonic);
            metrics.push(m);
        }

        // Gauges → Gauge(absolute), with TTL eviction
        let ttl = self.config.gauge_ttl;
        let now = Instant::now();
        self.gauges
            .retain(|_, g| now.duration_since(g.last_update) < ttl);
        for (key, gauge) in &self.gauges {
            let mut m = OtelMetric::new_gauge(&key.name, gauge.value);
            if let Some(ref tags) = key.tags {
                m = m.with_tags(Some(tags.clone()));
            }
            m.set_resource(resource.clone());
            m.set_scope(scope.clone());
            set_gauge_timestamp(&mut m, flush_time_ns);
            metrics.push(m);
        }

        // Sets → Gauge(cardinality), unit="1"
        for (key, set) in self.sets.drain() {
            #[expect(
                clippy::cast_precision_loss,
                reason = "set cardinality; precise for |v| <= 2^53"
            )]
            let cardinality = set.values.len() as f64;
            let mut m = OtelMetric::new_gauge(&key.name, cardinality).with_unit("1");
            if let Some(tags) = key.tags {
                m = m.with_tags(Some(tags));
            }
            m.set_resource(resource.clone());
            m.set_scope(scope.clone());
            set_gauge_timestamp(&mut m, flush_time_ns);
            metrics.push(m);
        }

        // Histograms → ExponentialHistogram(Delta), unit from config
        for (key, hist) in self.histograms.drain() {
            let mut m = hist
                .to_otel_metric(&key.name)
                .with_unit(&self.config.timer_unit);
            if let Some(tags) = key.tags {
                m = m.with_tags(Some(tags));
            }
            m.set_resource(resource.clone());
            m.set_scope(scope.clone());
            set_timestamps(&mut m, start_time_ns, flush_time_ns);
            metrics.push(m);
        }

        metrics
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "system clock nanoseconds since epoch fit u64 until year 2554"
)]
fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn set_timestamps(metric: &mut OtelMetric, start_ns: u64, end_ns: u64) {
    if let Some(data) = metric.metric_mut().data.as_mut() {
        match data {
            MetricData::Sum(sum) => {
                for dp in &mut sum.data_points {
                    dp.start_time_unix_nano = start_ns;
                    dp.time_unix_nano = end_ns;
                }
            }
            MetricData::ExponentialHistogram(eh) => {
                for dp in &mut eh.data_points {
                    dp.start_time_unix_nano = start_ns;
                    dp.time_unix_nano = end_ns;
                }
            }
            _ => {}
        }
    }
}

fn set_gauge_timestamp(metric: &mut OtelMetric, time_ns: u64) {
    if let Some(data) = metric.metric_mut().data.as_mut()
        && let MetricData::Gauge(gauge) = data
    {
        for dp in &mut gauge.data_points {
            dp.time_unix_nano = time_ns;
        }
    }
}

fn set_is_monotonic(metric: &mut OtelMetric, is_monotonic: bool) {
    if let Some(data) = metric.metric_mut().data.as_mut()
        && let MetricData::Sum(sum) = data
    {
        sum.is_monotonic = is_monotonic;
    }
}

// --- ExponentialHistogram Engine ---

const EXPO_MAX_SIZE: i32 = 160;
const EXPO_START_SCALE: i32 = 20;

pub struct ExpoHistogram {
    max_size: i32,
    scale: i32,
    count: u64,
    zero_count: u64,
    sum: f64,
    min: f64,
    max: f64,
    positive: ExpoBuckets,
    negative: ExpoBuckets,
}

struct ExpoBuckets {
    counts: Vec<u64>,
    index_start: i32,
}

impl ExpoBuckets {
    const fn new() -> Self {
        Self {
            counts: Vec::new(),
            index_start: 0,
        }
    }

    const fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "histogram bucket count bounded by max_size which fits i32"
    )]
    const fn index_end(&self) -> i32 {
        self.index_start + self.counts.len() as i32 - 1
    }

    #[cfg(test)]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "histogram bucket count bounded by max_size which fits i32"
    )]
    const fn span(&self) -> i32 {
        self.counts.len() as i32
    }

    #[expect(
        clippy::cast_sign_loss,
        reason = "index differences are non-negative: guards ensure index >= index_start"
    )]
    fn increment(&mut self, index: i32, count: u64) -> bool {
        if self.counts.is_empty() {
            self.index_start = index;
            self.counts.push(count);
            return true;
        }

        if index < self.index_start {
            let prepend = (self.index_start - index) as usize;
            let mut new_counts = vec![0u64; prepend + self.counts.len()];
            new_counts[prepend..].copy_from_slice(&self.counts);
            self.counts = new_counts;
            self.index_start = index;
        } else if index > self.index_end() {
            let new_len = (index - self.index_start + 1) as usize;
            self.counts.resize(new_len, 0);
        }

        let offset = (index - self.index_start) as usize;
        self.counts[offset] += count;
        true
    }

    fn would_exceed(&self, index: i32, max_size: i32) -> bool {
        if self.counts.is_empty() {
            return false;
        }
        let lo = self.index_start.min(index);
        let hi = self.index_end().max(index);
        (hi - lo + 1) > max_size
    }

    #[expect(
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "histogram bin index math: differences are non-negative, lengths bounded by max_size"
    )]
    fn downscale(&mut self, by: i32) {
        if self.counts.len() <= 1 || by < 1 {
            self.index_start >>= by;
            return;
        }

        let old_start = self.index_start;
        let old_end = self.index_end();
        let new_start = old_start >> by;
        let new_end = old_end >> by;
        let new_len = (new_end - new_start + 1) as usize;
        let mut new_counts = vec![0u64; new_len];

        for (i, &count) in self.counts.iter().enumerate() {
            if count == 0 {
                continue;
            }
            let old_idx = old_start + i as i32;
            let new_idx = old_idx >> by;
            new_counts[(new_idx - new_start) as usize] += count;
        }

        self.counts = new_counts;
        self.index_start = new_start;
    }

    fn to_proto(&self) -> ProtoBuckets {
        ProtoBuckets {
            offset: self.index_start,
            bucket_counts: self.counts.clone(),
        }
    }
}

impl ExpoHistogram {
    pub const fn new(max_size: i32, scale: i32) -> Self {
        Self {
            max_size,
            scale,
            count: 0,
            zero_count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            positive: ExpoBuckets::new(),
            negative: ExpoBuckets::new(),
        }
    }

    pub fn record(&mut self, value: f64, count: u64) {
        if count == 0 {
            return;
        }

        self.count += count;
        #[expect(
            clippy::cast_precision_loss,
            reason = "histogram sample count; precise for |v| <= 2^53"
        )]
        let count_f = count as f64;
        self.sum += value * count_f;

        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }

        if value == 0.0 {
            self.zero_count += count;
            return;
        }

        let (buckets, abs_value) = if value > 0.0 {
            (&mut self.positive, value)
        } else {
            (&mut self.negative, -value)
        };

        let index = map_to_index(abs_value, self.scale);

        if buckets.would_exceed(index, self.max_size) {
            let change = self.scale_change_needed(index);
            self.downscale(change);
            let new_index = map_to_index(abs_value, self.scale);
            let buckets = if value > 0.0 {
                &mut self.positive
            } else {
                &mut self.negative
            };
            buckets.increment(new_index, count);
        } else {
            buckets.increment(index, count);
        }
    }

    fn scale_change_needed(&self, new_index: i32) -> i32 {
        let mut change = 0i32;

        let pos_lo = if self.positive.is_empty() {
            new_index
        } else {
            self.positive.index_start.min(new_index)
        };
        let pos_hi = if self.positive.is_empty() {
            new_index
        } else {
            self.positive.index_end().max(new_index)
        };

        let neg_lo = if self.negative.is_empty() {
            new_index
        } else {
            self.negative.index_start.min(new_index)
        };
        let neg_hi = if self.negative.is_empty() {
            new_index
        } else {
            self.negative.index_end().max(new_index)
        };

        let mut plo = pos_lo;
        let mut phi = pos_hi;
        let mut nlo = neg_lo;
        let mut nhi = neg_hi;

        loop {
            let pos_ok = (phi - plo + 1) <= self.max_size || self.positive.is_empty();
            let neg_ok = (nhi - nlo + 1) <= self.max_size || self.negative.is_empty();
            if pos_ok && neg_ok {
                break;
            }
            change += 1;
            plo >>= 1;
            phi >>= 1;
            nlo >>= 1;
            nhi >>= 1;
        }

        change
    }

    fn downscale(&mut self, change: i32) {
        if change <= 0 {
            return;
        }
        self.positive.downscale(change);
        self.negative.downscale(change);
        self.scale -= change;
    }

    pub const fn scale(&self) -> i32 {
        self.scale
    }

    pub const fn count(&self) -> u64 {
        self.count
    }

    pub const fn sum(&self) -> f64 {
        self.sum
    }

    pub fn to_otel_metric(&self, name: &str) -> OtelMetric {
        OtelMetric::new_exponential_histogram(
            name,
            self.scale,
            self.count,
            self.sum,
            self.zero_count,
            self.positive.to_proto(),
            self.negative.to_proto(),
            if self.min == f64::INFINITY {
                None
            } else {
                Some(self.min)
            },
            if self.max == f64::NEG_INFINITY {
                None
            } else {
                Some(self.max)
            },
        )
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "histogram bin index math; power-of-two shift is exact in f64"
)]
#[expect(
    clippy::cast_sign_loss,
    reason = "scale is non-negative in the branch where it is cast to u64"
)]
#[expect(
    clippy::cast_possible_truncation,
    reason = "logarithmic bin index result fits i32 by OTel expo histogram spec"
)]
fn map_to_index(value: f64, scale: i32) -> i32 {
    debug_assert!(value > 0.0, "map_to_index requires positive value");

    let (frac, exp) = frexp(value);
    if scale <= 0 {
        let correction: i32 = if frac == 0.5 { 2 } else { 1 };
        (exp - correction) >> (-scale)
    } else if frac == 0.5 {
        // Exact power of two: use exact integer formula.
        // frexp exp is IEEE exponent + 1, so normal_base2 = exp - 1.
        ((exp - 1) << scale) - 1
    } else {
        let scale_factor = std::f64::consts::LOG2_E * (1u64 << scale as u64) as f64;
        (value.ln() * scale_factor).floor() as i32
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "histogram bin index math; power-of-two shift is exact in f64"
)]
fn frexp(value: f64) -> (f64, i32) {
    if value == 0.0 {
        return (0.0, 0);
    }
    let bits = value.to_bits();
    let biased_exp = ((bits >> 52) & 0x7ff) as i32;
    if biased_exp == 0 {
        // Subnormal: normalize by multiplying by 2^64, then adjust
        let normalized = value * (1u64 << 63) as f64 * 2.0;
        let (frac, exp) = frexp(normalized);
        return (frac, exp - 64);
    }
    let exp = biased_exp - 1022;
    let frac_bits = (bits & 0x800f_ffff_ffff_ffff) | 0x3fe0_0000_0000_0000;
    (f64::from_bits(frac_bits), exp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sol_lib::event::otel_metric::NumberDataPointValue;

    // --- ExpoHistogram unit tests ---

    #[test]
    fn map_to_index_scale_zero() {
        // At scale 0, base = 2^(2^0) = 2. Buckets: (1,2], (2,4], (4,8], ...
        // index 0 = (1, 2], index 1 = (2, 4], index -1 = (0.5, 1]
        assert_eq!(map_to_index(1.5, 0), 0);
        assert_eq!(map_to_index(2.0, 0), 0);
        assert_eq!(map_to_index(3.0, 0), 1);
        assert_eq!(map_to_index(4.0, 0), 1);
        assert_eq!(map_to_index(0.75, 0), -1);
        assert_eq!(map_to_index(1.0, 0), -1);
    }

    #[test]
    fn map_to_index_scale_one() {
        // At scale 1, base = 2^(2^-1) = sqrt(2) ≈ 1.4142
        // index 0 = (1, sqrt(2)], index 1 = (sqrt(2), 2], index 2 = (2, 2*sqrt(2)]
        assert_eq!(map_to_index(1.2, 1), 0);
        assert_eq!(map_to_index(1.5, 1), 1);
        // 2.0 = base^2, so it's the upper bound of bucket 1
        assert_eq!(map_to_index(2.0, 1), 1);
        assert_eq!(map_to_index(2.1, 1), 2);
    }

    #[test]
    fn map_to_index_scale_twenty() {
        // At scale 20, very fine resolution. Nearby values should get different indices.
        let i1 = map_to_index(1.0, 20);
        let i2 = map_to_index(1.0001, 20);
        assert!(
            i2 > i1,
            "nearby values should map to different indices at scale 20"
        );
    }

    #[test]
    fn expo_histogram_single_value() {
        let mut h = ExpoHistogram::new(160, 20);
        h.record(5.0, 1);
        assert_eq!(h.count(), 1);
        assert_eq!(h.sum(), 5.0);
        assert_eq!(h.min, 5.0);
        assert_eq!(h.max, 5.0);
        assert_eq!(h.positive.counts.iter().sum::<u64>(), 1);
        assert!(h.negative.is_empty());
    }

    #[test]
    fn expo_histogram_zero_value() {
        let mut h = ExpoHistogram::new(160, 20);
        h.record(0.0, 3);
        assert_eq!(h.count(), 3);
        assert_eq!(h.zero_count, 3);
        assert_eq!(h.sum(), 0.0);
        assert!(h.positive.is_empty());
        assert!(h.negative.is_empty());
    }

    #[test]
    fn expo_histogram_negative_values() {
        let mut h = ExpoHistogram::new(160, 20);
        h.record(-2.5, 1);
        h.record(-7.0, 1);
        assert_eq!(h.count(), 2);
        assert_eq!(h.sum(), -9.5);
        assert!(h.positive.is_empty());
        assert!(!h.negative.is_empty());
        assert_eq!(h.negative.counts.iter().sum::<u64>(), 2);
    }

    #[test]
    fn expo_histogram_mixed_values() {
        let mut h = ExpoHistogram::new(160, 20);
        h.record(1.0, 1);
        h.record(-1.0, 1);
        h.record(0.0, 1);
        assert_eq!(h.count(), 3);
        assert_eq!(h.sum(), 0.0);
        assert_eq!(h.zero_count, 1);
        assert_eq!(h.positive.counts.iter().sum::<u64>(), 1);
        assert_eq!(h.negative.counts.iter().sum::<u64>(), 1);
    }

    #[test]
    fn expo_histogram_weighted_insert() {
        let mut h = ExpoHistogram::new(160, 20);
        h.record(5.0, 10);
        assert_eq!(h.count(), 10);
        assert_eq!(h.sum(), 50.0);
        assert_eq!(h.positive.counts.iter().sum::<u64>(), 10);
    }

    #[test]
    fn expo_histogram_downscale_on_overflow() {
        // With max_size=4, inserting values with very different magnitudes should trigger downscale
        let mut h = ExpoHistogram::new(4, 20);
        h.record(1.0, 1);
        h.record(1000.0, 1);
        h.record(0.001, 1);
        h.record(1_000_000.0, 1);
        h.record(0.000001, 1);

        assert_eq!(h.count(), 5);
        assert!(
            h.scale() < 20,
            "scale should have decreased from 20, got {}",
            h.scale()
        );
        assert!(
            h.positive.span() <= 4,
            "positive span {} should be <= max_size 4",
            h.positive.span()
        );
    }

    #[test]
    fn expo_histogram_many_values_bounded() {
        let mut h = ExpoHistogram::new(160, 20);
        for i in 1..=10000 {
            h.record(f64::from(i), 1);
        }
        assert_eq!(h.count(), 10000);
        assert!(
            h.positive.span() <= 160,
            "positive span {} should be <= 160",
            h.positive.span()
        );
        let bucket_sum: u64 = h.positive.counts.iter().sum();
        assert_eq!(bucket_sum, 10000);
    }

    #[test]
    fn expo_histogram_sum_accuracy() {
        let mut h = ExpoHistogram::new(160, 20);
        for _ in 0..100 {
            h.record(0.1, 1);
        }
        assert_eq!(h.count(), 100);
        assert!((h.sum() - 10.0).abs() < 1e-10);
    }

    #[test]
    fn expo_histogram_to_otel_metric() {
        let mut h = ExpoHistogram::new(160, 20);
        h.record(1.0, 1);
        h.record(2.0, 1);
        h.record(3.0, 1);

        let m = h.to_otel_metric("test_histogram");
        assert_eq!(m.name(), "test_histogram");

        let proto = m.metric_proto();
        let eh = proto.data.as_ref().unwrap();
        if let MetricData::ExponentialHistogram(eh) = eh {
            assert_eq!(eh.data_points.len(), 1);
            let dp = &eh.data_points[0];
            assert_eq!(dp.count, 3);
            assert!((dp.sum.unwrap() - 6.0).abs() < 1e-10);
            assert!(dp.scale > 0, "scale should be positive, got {}", dp.scale);
            assert_eq!(dp.zero_count, 0);
            assert!(dp.positive.is_some());
            let pos = dp.positive.as_ref().unwrap();
            let bucket_sum: u64 = pos.bucket_counts.iter().sum();
            assert_eq!(bucket_sum, 3);
        } else {
            panic!("expected ExponentialHistogram data");
        }
    }

    #[test]
    fn frexp_basic() {
        let (frac, exp) = frexp(1.0);
        assert_eq!(frac, 0.5);
        assert_eq!(exp, 1);

        let (frac, exp) = frexp(2.0);
        assert_eq!(frac, 0.5);
        assert_eq!(exp, 2);

        let (frac, exp) = frexp(3.0);
        assert_eq!(frac, 0.75);
        assert_eq!(exp, 2);

        let (frac, exp) = frexp(0.5);
        assert_eq!(frac, 0.5);
        assert_eq!(exp, 0);
    }

    #[test]
    fn downscale_preserves_total_count() {
        let mut b = ExpoBuckets::new();
        b.increment(0, 5);
        b.increment(1, 3);
        b.increment(2, 7);
        b.increment(3, 2);
        let total_before: u64 = b.counts.iter().sum();

        b.downscale(1);
        let total_after: u64 = b.counts.iter().sum();
        assert_eq!(total_before, total_after);
        assert!(b.counts.len() <= 2);
    }

    #[test]
    fn downscale_negative_indices() {
        let mut b = ExpoBuckets::new();
        b.increment(-4, 10);
        b.increment(-3, 20);
        b.increment(-2, 30);
        b.increment(-1, 40);
        let total_before: u64 = b.counts.iter().sum();

        b.downscale(1);
        let total_after: u64 = b.counts.iter().sum();
        assert_eq!(total_before, total_after);
    }

    // --- Aggregator unit tests ---

    fn test_resource() -> Resource {
        Resource {
            attributes: vec![],
            dropped_attributes_count: 0,
            ..Default::default()
        }
    }

    fn test_scope() -> InstrumentationScope {
        InstrumentationScope {
            name: "test".into(),
            version: "0.1".into(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }
    }

    #[test]
    fn aggregator_counter_accumulation() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        let key = MetricKey {
            name: "requests".into(),
            tags: None,
        };

        agg.record(ParsedMetric::Counter {
            key: key.clone(),
            value: 1.0,
        });
        agg.record(ParsedMetric::Counter {
            key: key.clone(),
            value: 3.0,
        });
        agg.record(ParsedMetric::Counter {
            key: key.clone(),
            value: 0.5,
        });

        let metrics = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics.len(), 1);

        let m = &metrics[0];
        assert_eq!(m.name(), "requests");
        let proto = m.metric_proto();
        if let Some(MetricData::Sum(sum)) = &proto.data {
            assert_eq!(sum.data_points.len(), 1);
            assert!(
                (*sum.data_points[0].value.as_ref().unwrap()
                    == NumberDataPointValue::AsDouble(4.5))
            );
        } else {
            panic!("expected Sum");
        }
    }

    #[test]
    fn aggregator_gauge_absolute() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        let key = MetricKey {
            name: "cpu".into(),
            tags: None,
        };

        agg.record(ParsedMetric::Gauge {
            key: key.clone(),
            value: 30.0,
            direction: None,
        });
        agg.record(ParsedMetric::Gauge {
            key: key.clone(),
            value: 70.0,
            direction: None,
        });

        let metrics = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics.len(), 1);
        let proto = metrics[0].metric_proto();
        if let Some(MetricData::Gauge(g)) = &proto.data {
            assert_eq!(g.data_points.len(), 1);
            assert!(g.data_points[0].value == Some(NumberDataPointValue::AsDouble(70.0)));
        } else {
            panic!("expected Gauge");
        }
    }

    #[test]
    fn aggregator_gauge_delta_accumulation() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        let key = MetricKey {
            name: "cpu".into(),
            tags: None,
        };

        agg.record(ParsedMetric::Gauge {
            key: key.clone(),
            value: 100.0,
            direction: None,
        });
        agg.record(ParsedMetric::Gauge {
            key: key.clone(),
            value: 5.0,
            direction: Some(1.0),
        });
        agg.record(ParsedMetric::Gauge {
            key: key.clone(),
            value: 3.0,
            direction: Some(-1.0),
        });

        let metrics = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics.len(), 1);
        let proto = metrics[0].metric_proto();
        if let Some(MetricData::Gauge(g)) = &proto.data {
            let val = match &g.data_points[0].value {
                Some(NumberDataPointValue::AsDouble(v)) => *v,
                _ => panic!("expected double"),
            };
            assert!((val - 102.0).abs() < 1e-10);
        } else {
            panic!("expected Gauge");
        }
    }

    #[test]
    fn aggregator_gauge_persists_across_flushes() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        let key = MetricKey {
            name: "cpu".into(),
            tags: None,
        };

        agg.record(ParsedMetric::Gauge {
            key: key.clone(),
            value: 100.0,
            direction: None,
        });

        let metrics1 = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics1.len(), 1);

        // Second flush without new data — gauge should still emit
        let metrics2 = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics2.len(), 1);

        // Delta should accumulate on persisted value
        agg.record(ParsedMetric::Gauge {
            key: key.clone(),
            value: 5.0,
            direction: Some(1.0),
        });
        let metrics3 = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics3.len(), 1);
        let proto = metrics3[0].metric_proto();
        if let Some(MetricData::Gauge(g)) = &proto.data {
            let val = match &g.data_points[0].value {
                Some(NumberDataPointValue::AsDouble(v)) => *v,
                _ => panic!("expected double"),
            };
            assert!((val - 105.0).abs() < 1e-10);
        } else {
            panic!("expected Gauge");
        }
    }

    #[test]
    fn aggregator_set_deduplication() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        let key = MetricKey {
            name: "users".into(),
            tags: None,
        };

        for _ in 0..100 {
            agg.record(ParsedMetric::Set {
                key: key.clone(),
                value: "user123".into(),
            });
        }
        agg.record(ParsedMetric::Set {
            key: key.clone(),
            value: "user456".into(),
        });

        let metrics = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics.len(), 1);
        let proto = metrics[0].metric_proto();
        if let Some(MetricData::Gauge(g)) = &proto.data {
            let val = match &g.data_points[0].value {
                Some(NumberDataPointValue::AsDouble(v)) => *v,
                _ => panic!("expected double"),
            };
            assert!((val - 2.0).abs() < 1e-10);
        } else {
            panic!("expected Gauge");
        }
    }

    #[test]
    fn aggregator_timer_to_exponential_histogram() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        let key = MetricKey {
            name: "latency".into(),
            tags: None,
        };

        for i in 1..=100 {
            agg.record(ParsedMetric::Timer {
                key: key.clone(),
                value: f64::from(i),
                sample_rate: 1.0,
            });
        }

        let metrics = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics.len(), 1);
        let proto = metrics[0].metric_proto();
        if let Some(MetricData::ExponentialHistogram(eh)) = &proto.data {
            let dp = &eh.data_points[0];
            assert_eq!(dp.count, 100);
            assert!((dp.sum.unwrap() - 5050.0).abs() < 1e-10);
            assert!(dp.scale > 0);
            assert!(dp.positive.is_some());
            let pos = dp.positive.as_ref().unwrap();
            let bucket_sum: u64 = pos.bucket_counts.iter().sum();
            assert_eq!(bucket_sum, 100);
        } else {
            panic!("expected ExponentialHistogram");
        }
    }

    #[test]
    fn aggregator_timer_sample_rate() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        let key = MetricKey {
            name: "latency".into(),
            tags: None,
        };

        // sample_rate=0.1 means each observation represents 10 actual observations
        agg.record(ParsedMetric::Timer {
            key: key.clone(),
            value: 5.0,
            sample_rate: 0.1,
        });

        let metrics = agg.flush(&test_resource(), &test_scope());
        let proto = metrics[0].metric_proto();
        if let Some(MetricData::ExponentialHistogram(eh)) = &proto.data {
            assert_eq!(eh.data_points[0].count, 10);
            assert!((eh.data_points[0].sum.unwrap() - 50.0).abs() < 1e-10);
        } else {
            panic!("expected ExponentialHistogram");
        }
    }

    #[test]
    fn aggregator_counters_reset_after_flush() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        let key = MetricKey {
            name: "requests".into(),
            tags: None,
        };

        agg.record(ParsedMetric::Counter {
            key: key.clone(),
            value: 10.0,
        });
        let m1 = agg.flush(&test_resource(), &test_scope());
        assert_eq!(m1.len(), 1);

        // After flush, counter is reset — no metrics if nothing recorded
        let m2 = agg.flush(&test_resource(), &test_scope());
        assert_eq!(m2.len(), 0);
    }

    #[test]
    fn aggregator_timestamps_set() {
        let mut agg = Aggregator::new(AggregatorConfig::default());
        agg.record(ParsedMetric::Counter {
            key: MetricKey {
                name: "test".into(),
                tags: None,
            },
            value: 1.0,
        });

        let metrics = agg.flush(&test_resource(), &test_scope());
        let proto = metrics[0].metric_proto();
        if let Some(MetricData::Sum(sum)) = &proto.data {
            let dp = &sum.data_points[0];
            assert!(dp.start_time_unix_nano > 0);
            assert!(dp.time_unix_nano > 0);
            assert!(dp.time_unix_nano >= dp.start_time_unix_nano);
        } else {
            panic!("expected Sum");
        }
    }

    #[test]
    fn aggregator_different_tags_separate_series() {
        let mut agg = Aggregator::new(AggregatorConfig::default());

        let mut tags_a = OtelAttributes::new();
        tags_a.insert("env".into(), crate::event::string_value("prod"));

        let mut tags_b = OtelAttributes::new();
        tags_b.insert("env".into(), crate::event::string_value("staging"));

        agg.record(ParsedMetric::Counter {
            key: MetricKey {
                name: "requests".into(),
                tags: Some(tags_a),
            },
            value: 1.0,
        });
        agg.record(ParsedMetric::Counter {
            key: MetricKey {
                name: "requests".into(),
                tags: Some(tags_b),
            },
            value: 2.0,
        });

        let metrics = agg.flush(&test_resource(), &test_scope());
        assert_eq!(metrics.len(), 2);
    }
}
