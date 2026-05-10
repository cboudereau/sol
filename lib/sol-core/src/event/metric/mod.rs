#[cfg(feature = "vrl")]
use std::convert::TryFrom;

use sol_config::configurable_component;
#[cfg(feature = "vrl")]
use vrl::compiler::value::VrlValueConvert;

#[cfg(any(test, feature = "test"))]
mod arbitrary;

mod series;
pub use self::series::*;

mod tags;
pub use self::tags::*;

mod value;
pub use self::value::*;

#[macro_export]
macro_rules! otel_tags {
    () => { $crate::event::OtelAttributes::default() };

    ($($key:expr => $value:expr,)+) => { $crate::otel_tags!($($key => $value),+) };

    ($($key:expr => $value:expr),*) => {
        [
            $( (String::from($key), String::from($value)), )*
        ].into_iter().collect::<$crate::event::OtelAttributes>()
    };
}

/// Metric kind.
///
/// Metrics can be either absolute or incremental. Absolute metrics represent a sort of "last write wins" scenario,
/// where the latest absolute value seen is meant to be the actual metric value.  In contrast, and perhaps intuitively,
/// incremental metrics are meant to be additive, such that we don't know what total value of the metric is, but we know
/// that we'll be adding or subtracting the given value from it.
///
/// Generally speaking, most metrics storage systems deal with incremental updates. A notable exception is Prometheus,
/// which deals with, and expects, absolute values from clients.
#[configurable_component]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    /// Incremental metric.
    Incremental,

    /// Absolute metric.
    Absolute,
}

#[cfg(feature = "vrl")]
impl TryFrom<vrl::value::Value> for MetricKind {
    type Error = String;

    fn try_from(value: vrl::value::Value) -> Result<Self, Self::Error> {
        let value = value.try_bytes().map_err(|e| e.to_string())?;
        match std::str::from_utf8(&value).map_err(|e| e.to_string())? {
            "incremental" => Ok(Self::Incremental),
            "absolute" => Ok(Self::Absolute),
            value => Err(format!(
                "invalid metric kind {value}, metric kind must be `absolute` or `incremental`"
            )),
        }
    }
}

#[cfg(feature = "vrl")]
impl From<MetricKind> for vrl::value::Value {
    fn from(kind: MetricKind) -> Self {
        match kind {
            MetricKind::Incremental => "incremental".into(),
            MetricKind::Absolute => "absolute".into(),
        }
    }
}

#[macro_export]
macro_rules! samples {
    ( $( $value:expr => $rate:expr ),* ) => {
        [ $( $crate::event::metric::Sample { value: $value, rate: $rate }, )* ]
    }
}

#[macro_export]
macro_rules! buckets {
    ( $( $limit:expr => $count:expr ),* ) => {
        [ $( $crate::event::metric::Bucket { upper_limit: $limit, count: $count }, )* ]
    }
}

#[macro_export]
macro_rules! quantiles {
    ( $( $q:expr => $value:expr ),* ) => {
        [ $( $crate::event::metric::Quantile { quantile: $q, value: $value }, )* ]
    }
}

#[cfg(feature = "lua")]
#[inline]
pub(crate) fn zip_samples(
    values: impl IntoIterator<Item = f64>,
    rates: impl IntoIterator<Item = u32>,
) -> Vec<Sample> {
    values
        .into_iter()
        .zip(rates)
        .map(|(value, rate)| Sample { value, rate })
        .collect()
}

#[cfg(feature = "lua")]
#[inline]
pub(crate) fn zip_buckets(
    limits: impl IntoIterator<Item = f64>,
    counts: impl IntoIterator<Item = u64>,
) -> Vec<Bucket> {
    limits
        .into_iter()
        .zip(counts)
        .map(|(upper_limit, count)| Bucket { upper_limit, count })
        .collect()
}

#[cfg(feature = "lua")]
#[inline]
pub(crate) fn zip_quantiles(
    quantiles: impl IntoIterator<Item = f64>,
    values: impl IntoIterator<Item = f64>,
) -> Vec<Quantile> {
    quantiles
        .into_iter()
        .zip(values)
        .map(|(quantile, value)| Quantile { quantile, value })
        .collect()
}

pub fn samples_to_buckets(samples: &[Sample], buckets: &[f64]) -> (Vec<Bucket>, u64, f64) {
    let mut counts = vec![0; buckets.len()];
    let mut sum = 0.0;
    let mut count = 0;
    for sample in samples {
        let rate = u64::from(sample.rate);

        if let Some((i, _)) = buckets
            .iter()
            .enumerate()
            .find(|&(_, b)| *b >= sample.value)
        {
            counts[i] += rate;
        }

        sum += sample.value * f64::from(sample.rate);
        count += rate;
    }

    let buckets = buckets
        .iter()
        .zip(counts.iter())
        .map(|(b, c)| Bucket {
            upper_limit: *b,
            count: *c,
        })
        .collect();

    (buckets, count, sum)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn quantile_to_percentile_string() {
        let quantiles = [
            (-1.0, "0"),
            (0.0, "0"),
            (0.25, "25"),
            (0.50, "50"),
            (0.999, "999"),
            (0.9999, "9999"),
            (0.99999, "9999"),
            (1.0, "100"),
            (3.0, "100"),
        ];

        for (quantile, expected) in quantiles {
            let quantile = Quantile {
                quantile,
                value: 1.0,
            };
            let result = quantile.to_percentile_string();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn quantile_to_string() {
        let quantiles = [
            (-1.0, "0"),
            (0.0, "0"),
            (0.25, "0.25"),
            (0.50, "0.5"),
            (0.999, "0.999"),
            (0.9999, "0.9999"),
            (0.99999, "0.9999"),
            (1.0, "1"),
            (3.0, "1"),
        ];

        for (quantile, expected) in quantiles {
            let quantile = Quantile {
                quantile,
                value: 1.0,
            };
            let result = quantile.to_quantile_string();
            assert_eq!(result, expected);
        }
    }
}
