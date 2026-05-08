use core::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sol_common::byte_size_of::ByteSizeOf;
use sol_config::configurable_component;

use crate::float_eq;

const INFINITY: &str = "inf";
const NEG_INFINITY: &str = "-inf";
const NAN: &str = "NaN";

/// A single observation.
#[configurable_component]
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    /// The value of the observation.
    pub value: f64,

    /// The rate at which the value was observed.
    pub rate: u32,
}

impl PartialEq for Sample {
    fn eq(&self, other: &Self) -> bool {
        self.rate == other.rate && float_eq(self.value, other.value)
    }
}

impl ByteSizeOf for Sample {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

/// Custom serialization function which converts special `f64` values to strings.
/// Non-special values are serialized as numbers.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_f64<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if value.is_infinite() {
        serializer.serialize_str(if *value > 0.0 { INFINITY } else { NEG_INFINITY })
    } else if value.is_nan() {
        serializer.serialize_str(NAN)
    } else {
        serializer.serialize_f64(*value)
    }
}

/// Custom deserialization function for handling special f64 values.
fn deserialize_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    struct UpperLimitVisitor;

    impl de::Visitor<'_> for UpperLimitVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a number or a special string value")
        }

        fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            match value {
                NAN => Ok(f64::NAN),
                INFINITY => Ok(f64::INFINITY),
                NEG_INFINITY => Ok(f64::NEG_INFINITY),
                _ => Err(E::custom("unsupported string value")),
            }
        }
    }

    deserializer.deserialize_any(UpperLimitVisitor)
}

/// A histogram bucket.
///
/// Histogram buckets represent the `count` of observations where the value of the observations does
/// not exceed the specified `upper_limit`.
#[configurable_component(no_deser, no_ser)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Bucket {
    /// The upper limit of values in the bucket.
    #[serde(serialize_with = "serialize_f64", deserialize_with = "deserialize_f64")]
    pub upper_limit: f64,

    /// The number of values tracked in this bucket.
    pub count: u64,
}

impl PartialEq for Bucket {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && float_eq(self.upper_limit, other.upper_limit)
    }
}

impl ByteSizeOf for Bucket {
    fn allocated_bytes(&self) -> usize {
        0
    }
}

/// A single quantile observation.
///
/// Quantiles themselves are "cut points dividing the range of a probability distribution into
/// continuous intervals with equal probabilities". [[1][quantiles_wikipedia]].
///
/// We use quantiles to measure the value along these probability distributions for representing
/// client-side aggregations of distributions, which represent a collection of observations over a
/// specific time window.
///
/// In general, we typically use the term "quantile" to represent the concept of _percentiles_,
/// which deal with whole integers -- 0, 1, 2, .., 99, 100 -- even though quantiles are
/// floating-point numbers and can represent higher-precision cut points, such as 0.9999, or the
/// 99.99th percentile.
///
/// [quantiles_wikipedia]: https://en.wikipedia.org/wiki/Quantile
#[configurable_component]
#[derive(Clone, Copy, Debug)]
pub struct Quantile {
    /// The value of the quantile.
    ///
    /// This value must be between 0.0 and 1.0, inclusive.
    pub quantile: f64,

    /// The estimated value of the given quantile within the probability distribution.
    pub value: f64,
}

impl PartialEq for Quantile {
    fn eq(&self, other: &Self) -> bool {
        float_eq(self.quantile, other.quantile) && float_eq(self.value, other.value)
    }
}

impl Quantile {
    /// Renders this quantile as a string, scaled to be a percentile.
    ///
    /// Up to four significant digits are maintained, but the resulting string will be without a decimal point.
    ///
    /// For example, a quantile of 0.25, which represents a percentile of 25, will be rendered as "25" and a quantile of
    /// 0.9999, which represents a percentile of 99.99, will be rendered as "9999". A quantile of 0.99999, which
    /// represents a percentile of 99.999, would also be rendered as "9999", though.
    pub fn to_percentile_string(&self) -> String {
        let clamped = self.quantile.clamp(0.0, 1.0) * 100.0;
        clamped
            .to_string()
            .chars()
            .take(5)
            .filter(|c| *c != '.')
            .collect()
    }

    /// Renders this quantile as a string.
    ///
    /// Up to four significant digits are maintained.
    ///
    /// For example, a quantile of 0.25 will be rendered as "0.25", and a quantile of 0.9999 will be rendered as
    /// "0.9999", but a quantile of 0.99999 will be rendered as "0.9999".
    pub fn to_quantile_string(&self) -> String {
        let clamped = self.quantile.clamp(0.0, 1.0);
        clamped.to_string().chars().take(6).collect()
    }
}

impl ByteSizeOf for Quantile {
    fn allocated_bytes(&self) -> usize {
        0
    }
}
