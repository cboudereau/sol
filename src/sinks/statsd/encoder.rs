use std::{
    fmt::Display,
    io::{self, Write},
};

use bytes::{BufMut, BytesMut};
use sol_lib::event::{MetricKind, MetricView, OtelAttributes, OtelMetric};
use tokio_util::codec::Encoder;

use crate::{internal_events::StatsdInvalidMetricError, sinks::util::encode_namespace};

/// Error type for errors that can never happen, but for use with `Encoder`.
///
/// For the StatsD encoder, the encoding operation is infallible. However, as `Encoder<T>` requires
/// that the associated error type can be created by `From<io::Error>`, we can't simply use
/// `Infallible`. This type exists to bridge that gap, acting as a marker type for "we emit no
/// errors" while supporting the trait bounds on `Encoder<T>::Error`.
#[derive(Debug)]
pub struct InfallibleIo;

impl From<io::Error> for InfallibleIo {
    fn from(_: io::Error) -> Self {
        Self
    }
}

#[derive(Debug, Clone)]
pub(super) struct StatsdEncoder {
    default_namespace: Option<String>,
}

impl StatsdEncoder {
    /// Creates a new `StatsdEncoder` with the given default namespace, if any.
    pub const fn new(default_namespace: Option<String>) -> Self {
        Self { default_namespace }
    }
}

impl<'a> Encoder<&'a OtelMetric> for StatsdEncoder {
    type Error = InfallibleIo;

    fn encode(&mut self, metric: &'a OtelMetric, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let namespace = metric.namespace().or(self.default_namespace.as_deref());
        let name = encode_namespace(namespace, '.', metric.name());
        let tags = metric.tags().as_ref().map(encode_tags);

        match metric.view() {
            MetricView::Sum { value } => {
                encode_and_write_single_event(buf, &name, tags.as_deref(), value, "c", None);
            }
            MetricView::Gauge { value } => {
                match metric.kind() {
                    MetricKind::Incremental => encode_and_write_single_event(
                        buf,
                        &name,
                        tags.as_deref(),
                        format!("{value:+}"),
                        "g",
                        None,
                    ),
                    MetricKind::Absolute => {
                        encode_and_write_single_event(buf, &name, tags.as_deref(), value, "g", None)
                    }
                };
            }
            MetricView::Histogram { bounds, counts, .. } => {
                for (&val, &cnt) in bounds.iter().zip(counts.iter()) {
                    encode_and_write_single_event(
                        buf,
                        &name,
                        tags.as_deref(),
                        val,
                        "h",
                        Some({
                            #[expect(
                                clippy::cast_possible_truncation,
                                reason = "value is clamped to u32::MAX"
                            )]
                            let sample_count = cnt.min(u64::from(u32::MAX)) as u32;
                            sample_count
                        }),
                    );
                }
            }
            MetricView::Set { values } => {
                for val in values {
                    encode_and_write_single_event(buf, &name, tags.as_deref(), val, "s", None);
                }
            }
            _ => {
                emit!(StatsdInvalidMetricError {
                    error: "Unsupported metric type for StatsD.".into(),
                });

                return Ok(());
            }
        };

        Ok(())
    }
}

// Note that if multi-valued tags are present, this encoding may change the order from the input
// event, since the tags with multiple values may not have been grouped together.
// This is not an issue, but noting as it may be an observed behavior.
fn encode_tags(tags: &OtelAttributes) -> String {
    let parts: Vec<_> = tags
        .iter_all()
        .map(|(name, tag_value)| match tag_value {
            Some(value) => format!("{name}:{value}"),
            None => name.to_owned(),
        })
        .collect();

    // `parts` is already sorted by key because of BTreeMap
    parts.join(",")
}

fn encode_and_write_single_event<V: Display>(
    buf: &mut BytesMut,
    metric_name: &str,
    otel_tags: Option<&str>,
    val: V,
    metric_type: &str,
    sample_rate: Option<u32>,
) {
    let mut writer = buf.writer();

    write!(&mut writer, "{metric_name}:{val}|{metric_type}").unwrap();

    if let Some(sample_rate) = sample_rate
        && sample_rate != 1
    {
        write!(&mut writer, "|@{}", 1.0 / f64::from(sample_rate)).unwrap();
    };

    if let Some(t) = otel_tags {
        write!(&mut writer, "|#{t}").unwrap();
    };

    writeln!(&mut writer).unwrap();
}

#[cfg(test)]
mod tests {
    use sol_lib::event::OtelAttributes;
    #[cfg(feature = "sources-statsd")]
    use sol_lib::event::{MetricKind, OtelMetric};

    use super::encode_tags;

    #[cfg(feature = "sources-statsd")]
    fn encode_metric(metric: &OtelMetric) -> bytes::BytesMut {
        use tokio_util::codec::Encoder;

        let mut encoder = super::StatsdEncoder {
            default_namespace: None,
        };
        let mut frame = bytes::BytesMut::new();
        encoder.encode(metric, &mut frame).unwrap();
        frame
    }

    fn tags() -> OtelAttributes {
        sol_lib::otel_tags!(
            "normal_tag" => "value",
            "multi_value" => "true",
            "bare_tag" => "",
        )
    }

    #[test]
    fn test_encode_tags() {
        let actual = encode_tags(&tags());
        let mut actual = actual.split(',').collect::<Vec<_>>();
        actual.sort();

        let mut expected = "bare_tag:,multi_value:true,normal_tag:value"
            .split(',')
            .collect::<Vec<_>>();
        expected.sort();

        assert_eq!(actual, expected);
    }

    #[test]
    fn tags_order() {
        assert_eq!(
            &encode_tags(
                &vec![
                    ("a", "value"),
                    ("b", "value"),
                    ("c", "value"),
                    ("d", "value"),
                    ("e", "value"),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect::<OtelAttributes>()
            ),
            "a:value,b:value,c:value,d:value,e:value"
        );
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_counter() {
        let input = OtelMetric::new_counter("counter", MetricKind::Incremental, 1.5)
            .with_tags(Some(tags()));
        let frame = encode_metric(&input);
        assert_eq!(
            "counter:1.5|c|#bare_tag:,multi_value:true,normal_tag:value\n",
            std::str::from_utf8(&frame).unwrap()
        );
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_absolute_counter() {
        let input = OtelMetric::new_counter("counter", MetricKind::Absolute, 1.5);
        let frame = encode_metric(&input);
        assert_eq!("counter:1.5|c\n", std::str::from_utf8(&frame).unwrap());
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_gauge() {
        let input = OtelMetric::new_gauge_delta("gauge", -1.5).with_tags(Some(tags()));
        let frame = encode_metric(&input);
        assert_eq!(
            "gauge:-1.5|g|#bare_tag:,multi_value:true,normal_tag:value\n",
            std::str::from_utf8(&frame).unwrap()
        );
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_absolute_gauge() {
        let input = OtelMetric::new_gauge("gauge", 1.5).with_tags(Some(tags()));
        let frame = encode_metric(&input);
        assert_eq!(
            "gauge:1.5|g|#bare_tag:,multi_value:true,normal_tag:value\n",
            std::str::from_utf8(&frame).unwrap()
        );
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_histogram() {
        let samples = sol_lib::samples![1.5 => 1, 1.5 => 1];
        let input =
            OtelMetric::new_histogram_from_samples("histo", MetricKind::Incremental, &samples)
                .with_tags(Some(tags()));

        let frame = encode_metric(&input);
        let output = std::str::from_utf8(&frame).unwrap();
        let lines: Vec<&str> = output.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("histo:1.5|h"));
        assert!(lines[1].starts_with("histo:1.5|h"));
    }

    #[cfg(feature = "sources-statsd")]
    #[test]
    fn test_encode_set() {
        let input =
            OtelMetric::new_set_from_values("set", MetricKind::Incremental, vec!["abc".to_owned()])
                .with_tags(Some(tags()));

        let frame = encode_metric(&input);
        assert_eq!(
            "set:abc|s|#bare_tag:,multi_value:true,normal_tag:value\n",
            std::str::from_utf8(&frame).unwrap()
        );
    }
}
