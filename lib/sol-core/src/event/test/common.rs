use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use quickcheck::{Arbitrary, Gen, empty_shrinker};
use vrl::value::{ObjectMap, Value};

use super::super::{
    Event, EventMetadata, MetricKind, OtelAttributes, OtelLog, OtelMetric, OtelSpan,
    metric::{Bucket, Quantile, Sample},
};

const MAX_F64_SIZE: f64 = 1_000_000.0;
const MAX_MAP_SIZE: usize = 4;
const MAX_STR_SIZE: usize = 16;
const ALPHABET: [&str; 27] = [
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
    "t", "u", "v", "w", "x", "y", "z", "_",
];

#[derive(Debug, Clone)]
pub struct Name {
    inner: String,
}

impl Arbitrary for Name {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut name = String::with_capacity(MAX_STR_SIZE);
        for _ in 0..(g.size() % MAX_STR_SIZE) {
            let idx: usize = usize::arbitrary(g) % ALPHABET.len();
            name.push_str(ALPHABET[idx]);
        }

        Name { inner: name }
    }
}

impl From<Name> for String {
    fn from(name: Name) -> String {
        name.inner
    }
}

fn datetime(g: &mut Gen) -> DateTime<Utc> {
    // chrono documents that there is an out-of-range for both second and
    // nanosecond values but doesn't actually document what the valid ranges
    // are. We just sort of arbitrarily restrict things.
    let secs = i64::arbitrary(g) % 32_000;
    let nanosecs = u32::arbitrary(g) % 32_000;
    DateTime::from_timestamp(secs, nanosecs).expect("invalid timestamp")
}

impl Arbitrary for Event {
    fn arbitrary(g: &mut Gen) -> Self {
        let choice: u8 = u8::arbitrary(g);
        // Quickcheck can't derive Arbitrary for enums, see
        // https://github.com/BurntSushi/quickcheck/issues/98
        if choice.is_multiple_of(2) {
            Event::Log(OtelLog::arbitrary(g))
        } else {
            Event::Metric(OtelMetric::arbitrary(g))
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        empty_shrinker()
    }
}

impl Arbitrary for OtelLog {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut generator = Gen::new(MAX_MAP_SIZE);
        let map: ObjectMap = ObjectMap::arbitrary(&mut generator);
        let metadata: EventMetadata = EventMetadata::arbitrary(g);
        OtelLog::from_value_map(Value::Object(map), metadata)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let map = self.as_map().unwrap_or_default();
        let metadata = self.metadata().clone();

        Box::new(
            map.shrink()
                .map(move |x| OtelLog::from_value_map(Value::Object(x), metadata.clone())),
        )
    }
}

impl Arbitrary for OtelSpan {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut generator = Gen::new(MAX_MAP_SIZE);
        let map: ObjectMap = ObjectMap::arbitrary(&mut generator);
        let metadata: EventMetadata = EventMetadata::arbitrary(g);
        OtelSpan::from_value_map(Value::Object(map), metadata)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let map = self.as_map().unwrap_or_default();
        let metadata = self.metadata().clone();

        Box::new(
            map.shrink()
                .map(move |x| OtelSpan::from_value_map(Value::Object(x), metadata.clone())),
        )
    }
}

impl Arbitrary for OtelMetric {
    fn arbitrary(g: &mut Gen) -> Self {
        let kind = MetricKind::arbitrary(g);
        let name: Name = Name::arbitrary(g);
        let namespace: Option<Name> = Arbitrary::arbitrary(g);
        let tags: Option<OtelAttributes> = if bool::arbitrary(g) {
            let count = usize::arbitrary(g) % 4;
            let mut attrs = OtelAttributes::default();
            for _ in 0..count {
                let key: Name = Name::arbitrary(g);
                let val: Name = Name::arbitrary(g);
                attrs.insert(
                    String::from(key),
                    super::super::string_value(String::from(val)),
                );
            }
            if attrs.is_empty() { None } else { Some(attrs) }
        } else {
            None
        };
        let timestamp = if bool::arbitrary(g) {
            Some(datetime(g))
        } else {
            None
        };
        let metadata = EventMetadata::arbitrary(g);

        let otel = match u8::arbitrary(g) % 6 {
            0 => {
                let value = f64::arbitrary(g) % MAX_F64_SIZE;
                OtelMetric::new_counter(String::from(name), kind, value)
            }
            1 => {
                let value = f64::arbitrary(g) % MAX_F64_SIZE;
                match kind {
                    MetricKind::Absolute => OtelMetric::new_gauge(String::from(name), value),
                    MetricKind::Incremental => {
                        OtelMetric::new_gauge_delta(String::from(name), value)
                    }
                }
            }
            2 => {
                let values: BTreeSet<String> = BTreeSet::arbitrary(g);
                OtelMetric::new_set_from_values(
                    String::from(name),
                    kind,
                    values.into_iter().collect::<Vec<_>>(),
                )
            }
            3 => {
                let samples: Vec<Sample> = Vec::arbitrary(g);
                OtelMetric::new_histogram_from_samples(String::from(name), kind, &samples)
            }
            4 => {
                let buckets: Vec<Bucket> = Vec::arbitrary(g);
                let count = u64::arbitrary(g);
                let sum = f64::arbitrary(g) % MAX_F64_SIZE;
                OtelMetric::new_histogram(String::from(name), kind, &buckets, count, sum)
            }
            5 => {
                let quantiles: Vec<Quantile> = Vec::arbitrary(g);
                let count = u64::arbitrary(g);
                let sum = f64::arbitrary(g) % MAX_F64_SIZE;
                OtelMetric::new_summary(String::from(name), &quantiles, count, sum)
            }
            _ => unreachable!(),
        };
        otel.with_namespace(namespace.map(String::from))
            .with_tags(tags)
            .with_timestamp(timestamp)
            .with_metadata(metadata)
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        empty_shrinker()
    }
}

impl Arbitrary for MetricKind {
    fn arbitrary(g: &mut Gen) -> Self {
        let choice: u8 = u8::arbitrary(g);
        // Quickcheck can't derive Arbitrary for enums, see
        // https://github.com/BurntSushi/quickcheck/issues/98
        if choice.is_multiple_of(2) {
            MetricKind::Incremental
        } else {
            MetricKind::Absolute
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        empty_shrinker()
    }
}

impl Arbitrary for Sample {
    fn arbitrary(g: &mut Gen) -> Self {
        Sample {
            value: f64::arbitrary(g) % MAX_F64_SIZE,
            rate: u32::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let base = *self;

        Box::new(
            base.value
                .shrink()
                .map(move |value| {
                    let mut sample = base;
                    sample.value = value;
                    sample
                })
                .flat_map(|sample| {
                    sample.rate.shrink().map(move |rate| {
                        let mut ns = sample;
                        ns.rate = rate;
                        ns
                    })
                }),
        )
    }
}

impl Arbitrary for Quantile {
    fn arbitrary(g: &mut Gen) -> Self {
        Quantile {
            quantile: f64::arbitrary(g) % MAX_F64_SIZE,
            value: f64::arbitrary(g) % MAX_F64_SIZE,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let base = *self;

        Box::new(
            base.quantile
                .shrink()
                .map(move |upper_limit| {
                    let mut quantile = base;
                    quantile.quantile = upper_limit;
                    quantile
                })
                .flat_map(|quantile| {
                    quantile.value.shrink().map(move |value| {
                        let mut nq = quantile;
                        nq.value = value;
                        nq
                    })
                }),
        )
    }
}

impl Arbitrary for Bucket {
    fn arbitrary(g: &mut Gen) -> Self {
        Bucket {
            upper_limit: f64::arbitrary(g) % MAX_F64_SIZE,
            count: u64::arbitrary(g),
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let base = *self;

        Box::new(
            base.upper_limit
                .shrink()
                .map(move |upper_limit| {
                    let mut nb = base;
                    nb.upper_limit = upper_limit;
                    nb
                })
                .flat_map(|bucket| {
                    bucket.count.shrink().map(move |count| {
                        let mut nb = bucket;
                        nb.count = count;
                        nb
                    })
                }),
        )
    }
}

impl Arbitrary for EventMetadata {
    fn arbitrary(g: &mut Gen) -> Self {
        let mut metadata = EventMetadata::default();
        *metadata.value_mut() = Value::arbitrary(g);
        metadata
    }
}
