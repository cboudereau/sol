use std::collections::{BTreeMap, HashSet};

use async_stream::stream;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt};

use super::{
    OutputThroughput, ReceivedBytesTotal, ReceivedEventsTotal, SentBytesTotal, SentEventsTotal,
    filter_output_metric,
};
use crate::{
    config::ComponentKey,
    event::{MetricView, OtelMetric},
    metrics::Controller,
};

fn get_controller() -> &'static Controller {
    Controller::get().expect("Metrics system not initialized. Please report.")
}

/// Capture internal metrics as `Vec<OtelMetric>` for the GraphQL API layer.
fn capture_metrics() -> Vec<OtelMetric> {
    get_controller().capture_metrics()
}

/// Sums an iterable of `&OtelMetric`, by folding metric values. Convenience function typically
/// used to get aggregate metrics.
pub fn sum_metrics<'a, I: IntoIterator<Item = &'a OtelMetric>>(metrics: I) -> Option<OtelMetric> {
    let mut iter = metrics.into_iter();
    let mut result = iter.next()?.clone();
    for m in iter {
        let _ = result.add(m);
    }
    Some(result)
}

/// Sums an iterable of `OtelMetric`, by folding metric values. Convenience function typically
/// used to get aggregate metrics.
fn sum_metrics_owned<I: IntoIterator<Item = OtelMetric>>(metrics: I) -> Option<OtelMetric> {
    let mut iter = metrics.into_iter();
    let mut result = iter.next()?;
    for m in iter {
        let _ = result.add(&m);
    }
    Some(result)
}

pub trait MetricsFilter<'a> {
    fn received_bytes_total(&self) -> Option<ReceivedBytesTotal>;
    fn received_events_total(&self) -> Option<ReceivedEventsTotal>;
    fn sent_bytes_total(&self) -> Option<SentBytesTotal>;
    fn sent_events_total(&self) -> Option<SentEventsTotal>;
}

impl MetricsFilter<'_> for Vec<OtelMetric> {
    fn received_bytes_total(&self) -> Option<ReceivedBytesTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "sol_component_received_bytes_total"),
        )?;

        Some(ReceivedBytesTotal::new(sum))
    }

    fn received_events_total(&self) -> Option<ReceivedEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "sol_component_received_events_total"),
        )?;

        Some(ReceivedEventsTotal::new(sum))
    }

    fn sent_bytes_total(&self) -> Option<SentBytesTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "sol_component_sent_bytes_total"),
        )?;

        Some(SentBytesTotal::new(sum))
    }

    fn sent_events_total(&self) -> Option<SentEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "sol_component_sent_events_total"),
        )?;

        Some(SentEventsTotal::new(sum))
    }
}

impl<'a> MetricsFilter<'a> for Vec<&'a OtelMetric> {
    fn received_bytes_total(&self) -> Option<ReceivedBytesTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "sol_component_received_bytes_total")
                .copied(),
        )?;

        Some(ReceivedBytesTotal::new(sum))
    }

    fn received_events_total(&self) -> Option<ReceivedEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "sol_component_received_events_total")
                .copied(),
        )?;

        Some(ReceivedEventsTotal::new(sum))
    }

    fn sent_bytes_total(&self) -> Option<SentBytesTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "sol_component_sent_bytes_total")
                .copied(),
        )?;

        Some(SentBytesTotal::new(sum))
    }

    fn sent_events_total(&self) -> Option<SentEventsTotal> {
        let sum = sum_metrics(
            self.iter()
                .filter(|m| m.name() == "sol_component_sent_events_total")
                .copied(),
        )?;

        Some(SentEventsTotal::new(sum))
    }
}

/// Returns a stream of `OtelMetric`s, collected at the provided millisecond interval.
pub fn get_metrics(interval: i32) -> impl Stream<Item = OtelMetric> {
    #[expect(
        clippy::cast_sign_loss,
        reason = "GraphQL interval validated to [10, 60_000]"
    )]
    let interval_ms = interval as u64;
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));

    stream! {
        loop {
            interval.tick().await;
            for m in capture_metrics() {
                yield m;
            }
        }
    }
}

pub fn get_all_metrics(interval: i32) -> impl Stream<Item = Vec<OtelMetric>> {
    #[expect(
        clippy::cast_sign_loss,
        reason = "GraphQL interval validated to [10, 60_000]"
    )]
    let interval_ms = interval as u64;
    let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));

    stream! {
        loop {
            interval.tick().await;
            yield capture_metrics()
        }
    }
}

/// Return [`Vec<OtelMetric>`] based on a component id tag.
pub fn by_component_key(component_key: &ComponentKey) -> Vec<OtelMetric> {
    capture_metrics()
        .into_iter()
        .filter_map(|m| {
            m.tag_matches("component_id", component_key.id())
                .then_some(m)
        })
        .collect()
}

type MetricFilterFn = dyn Fn(&OtelMetric) -> bool + Send + Sync;

/// Returns a stream of `Vec<OtelMetric>`, where `metric_name` matches the name of the metric
/// (e.g. "sol_component_sent_events_total"), and the value is derived from `MetricView::Sum`. Uses a
/// local cache to match against the `component_id` of a metric, to return results only when
/// the value of a current iteration is greater than the previous. This is useful for the client
/// to be notified as metrics increase without returning 'empty' or identical results.
pub fn component_counter_metrics(
    interval: i32,
    filter_fn: &'static MetricFilterFn,
) -> impl Stream<Item = Vec<OtelMetric>> {
    let mut cache = BTreeMap::new();

    component_to_filtered_metrics(interval, filter_fn).map(move |map| {
        map.into_iter()
            .filter_map(|(id, metrics)| {
                let m = sum_metrics_owned(metrics)?;
                match m.view() {
                    MetricView::Sum { value }
                        if cache.insert(id, value).unwrap_or(0.00) < value =>
                    {
                        Some(m)
                    }
                    _ => None,
                }
            })
            .collect()
    })
}

/// Returns a stream of `Vec<OtelMetric>`, where `metric_name` matches the name of the metric
/// (e.g. "sol_component_sent_events_total"), and the value is derived from `MetricView::Gauge`. Uses a
/// local cache to match against the `component_id` of a metric, to return results only when
/// the value of a current iteration is greater than the previous. This is useful for the client
/// to be notified as metrics increase without returning 'empty' or identical results.
pub fn component_gauge_metrics(
    interval: i32,
    filter_fn: &'static MetricFilterFn,
) -> impl Stream<Item = Vec<OtelMetric>> {
    let mut cache = BTreeMap::new();

    component_to_filtered_metrics(interval, filter_fn).map(move |map| {
        map.into_iter()
            .filter_map(|(id, metrics)| {
                let m = sum_metrics_owned(metrics)?;
                match m.view() {
                    MetricView::Gauge { value }
                        if cache.insert(id, value).unwrap_or(0.00) < value =>
                    {
                        Some(m)
                    }
                    _ => None,
                }
            })
            .collect()
    })
}

/// Returns the throughput of a 'counter' metric, sampled over `interval` milliseconds
/// and filtered by the provided `filter_fn`.
pub fn counter_throughput(
    interval: i32,
    filter_fn: &'static MetricFilterFn,
) -> impl Stream<Item = (OtelMetric, f64)> {
    let mut last = 0.00;

    get_metrics(interval)
        .filter(filter_fn)
        .filter_map(move |m| match m.view() {
            MetricView::Sum { value } if value > last => {
                let throughput = value - last;
                last = value;
                Some((m, throughput))
            }
            _ => None,
        })
        // Ignore the first, since we only care about sampling between `interval`
        .skip(1)
}

/// Returns the throughput of a 'counter' metric, sampled over `interval` milliseconds
/// and filtered by the provided `filter_fn`, aggregated against each component.
pub fn component_counter_throughputs(
    interval: i32,
    filter_fn: &'static MetricFilterFn,
) -> impl Stream<Item = Vec<(OtelMetric, f64)>> {
    let mut cache = BTreeMap::new();

    component_to_filtered_metrics(interval, filter_fn)
        .map(move |map| {
            map.into_iter()
                .filter_map(|(id, metrics)| {
                    let m = sum_metrics_owned(metrics)?;
                    match m.view() {
                        MetricView::Sum { value } => {
                            let last = cache.insert(id, value).unwrap_or(0.00);
                            let throughput = value - last;
                            Some((m, throughput))
                        }
                        _ => None,
                    }
                })
                .collect()
        })
        // Ignore the first, since we only care about sampling between `interval`
        .skip(1)
}

/// Returns a stream of `Vec<(OtelMetric, Vec<OtelMetric>)>`, where `OtelMetric` is the
/// total `component_sent_events_total` metric for a component and `Vec<OtelMetric>`
/// is the `component_sent_events_total` metric split by output
pub fn component_sent_events_totals_metrics_with_outputs(
    interval: i32,
) -> impl Stream<Item = Vec<(OtelMetric, Vec<OtelMetric>)>> {
    let mut cache = BTreeMap::new();

    component_to_filtered_metrics(interval, &|m| m.name() == "sol_component_sent_events_total").map(
        move |map| {
            map.into_iter()
                .filter_map(|(id, metrics)| {
                    let outputs = metrics
                        .iter()
                        .filter_map(|m| m.tag_value("output"))
                        .collect::<HashSet<_>>();

                    let metric_by_outputs = outputs
                        .iter()
                        .filter_map(|output| {
                            let m = filter_output_metric(metrics.as_ref(), output.as_ref())?;
                            match m.view() {
                                MetricView::Sum { value }
                                    if cache
                                        .insert(format!("{id}.{output}"), value)
                                        .unwrap_or(0.00)
                                        < value =>
                                {
                                    Some(m)
                                }
                                _ => None,
                            }
                        })
                        .collect();

                    let sum = sum_metrics_owned(metrics)?;
                    match sum.view() {
                        MetricView::Sum { value }
                            if cache.insert(id, value).unwrap_or(0.00) < value =>
                        {
                            Some((sum, metric_by_outputs))
                        }
                        _ => None,
                    }
                })
                .collect()
        },
    )
}

/// Returns the throughput of the 'component_sent_events_total' metric, sampled over `interval` milliseconds,
/// for each component. Within a particular component, throughput per output stream is also included.
pub fn component_sent_events_total_throughputs_with_outputs(
    interval: i32,
) -> impl Stream<Item = Vec<(ComponentKey, i64, Vec<OutputThroughput>)>> {
    let mut cache = BTreeMap::new();

    component_to_filtered_metrics(interval, &|m| m.name() == "sol_component_sent_events_total")
        .map(move |map| {
            map.into_iter()
                .filter_map(|(id, metrics)| {
                    let outputs = metrics
                        .iter()
                        .filter_map(|m| m.tag_value("output"))
                        .collect::<HashSet<_>>();

                    let throughput_by_outputs = outputs
                        .iter()
                        .filter_map(|output| {
                            let m = filter_output_metric(metrics.as_ref(), output.as_ref())?;
                            let throughput = throughput(&m, format!("{id}.{output}"), &mut cache)?;
                            #[expect(
                                clippy::cast_possible_truncation,
                                reason = "f64 throughput → i64 for GraphQL API"
                            )]
                            let throughput_i = throughput as i64;
                            Some(OutputThroughput::new(output.clone(), throughput_i))
                        })
                        .collect::<Vec<_>>();

                    let sum = sum_metrics_owned(metrics)?;
                    let total_throughput = throughput(&sum, id.clone(), &mut cache)?;
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "f64 throughput → i64 for GraphQL API"
                    )]
                    let total_throughput_i = total_throughput as i64;
                    Some((
                        ComponentKey::from(id),
                        total_throughput_i,
                        throughput_by_outputs,
                    ))
                })
                .collect()
        })
        // Ignore the first, since we only care about sampling between `interval`
        .skip(1)
}

/// Returns a map of Component ID to list of metrics where metrics have been
/// filtered by `filter_fn`
fn component_to_filtered_metrics(
    interval: i32,
    filter_fn: &'static MetricFilterFn,
) -> impl Stream<Item = BTreeMap<String, Vec<OtelMetric>>> {
    get_all_metrics(interval).map(move |m| {
        m.into_iter()
            .filter(filter_fn)
            .filter_map(|m| m.tag_value("component_id").map(|id| (id, m)))
            .fold(
                BTreeMap::new(),
                |mut map: BTreeMap<String, Vec<OtelMetric>>, (id, m)| {
                    map.entry(id).or_default().push(m);
                    map
                },
            )
    })
}

/// Returns throughput based on a metric and provided `cache` of previous values
fn throughput(metric: &OtelMetric, id: String, cache: &mut BTreeMap<String, f64>) -> Option<f64> {
    match metric.view() {
        MetricView::Sum { value } => {
            let last = cache.insert(id, value).unwrap_or(0.00);
            let throughput = value - last;
            Some(throughput)
        }
        _ => None,
    }
}
