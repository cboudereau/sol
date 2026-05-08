use std::{
    collections::{HashMap, hash_map::Entry},
    pin::Pin,
    time::Duration,
};

use async_stream::stream;
use futures::{Stream, StreamExt};
use sol_lib::{configurable::configurable_component, event::metric::MetricSeries};

use crate::{
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    event::{Event, OtelMetric},
    internal_events::{AggregateEventRecorded, AggregateFlushed, AggregateUpdateFailed},
    schema,
    transforms::{TaskTransform, Transform},
};

/// Configuration for the `aggregate` transform.
#[configurable_component(transform("aggregate", "Aggregate metrics passing through a topology."))]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct AggregateConfig {
    /// The interval between flushes, in milliseconds.
    ///
    /// During this time frame, metrics (beta) with the same series data (name, namespace, tags, and so on) are aggregated.
    #[serde(default = "default_interval_ms")]
    #[configurable(metadata(docs::human_name = "Flush Interval"))]
    pub interval_ms: u64,
    /// Function to use for aggregation.
    ///
    /// Some of the functions may only function on incremental and some only on absolute metrics.
    #[serde(default = "default_mode")]
    #[configurable(derived)]
    pub mode: AggregationMode,
}

#[configurable_component]
#[derive(Clone, Debug, Default)]
#[configurable(description = "The aggregation mode to use.")]
pub enum AggregationMode {
    /// Default mode. Sums incremental metrics and uses the latest value for absolute metrics.
    #[default]
    Auto,

    /// Sums incremental metrics, ignores absolute
    Sum,

    /// Returns the latest value for absolute metrics, ignores incremental
    Latest,

    /// Counts metrics for incremental and absolute metrics
    Count,

    /// Returns difference between latest value for absolute, ignores incremental
    Diff,

    /// Max value of absolute metric, ignores incremental
    Max,

    /// Min value of absolute metric, ignores incremental
    Min,

    /// Mean value of absolute metric, ignores incremental
    Mean,

    /// Stdev value of absolute metric, ignores incremental
    Stdev,
}

const fn default_mode() -> AggregationMode {
    AggregationMode::Auto
}

const fn default_interval_ms() -> u64 {
    10 * 1000
}

impl_generate_config_from_default!(AggregateConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "aggregate")]
impl TransformConfig for AggregateConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        Aggregate::new(self).map(Transform::event_task)
    }

    fn input(&self) -> Input {
        Input::metric()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        _: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(DataType::Metric, HashMap::new())]
    }
}

#[derive(Debug)]
pub struct Aggregate {
    interval: Duration,
    map: HashMap<MetricSeries, OtelMetric>,
    prev_map: HashMap<MetricSeries, OtelMetric>,
    multi_map: HashMap<MetricSeries, Vec<OtelMetric>>,
    mode: AggregationMode,
}

impl Aggregate {
    pub fn new(config: &AggregateConfig) -> crate::Result<Self> {
        Ok(Self {
            interval: Duration::from_millis(config.interval_ms),
            map: Default::default(),
            prev_map: Default::default(),
            multi_map: Default::default(),
            mode: config.mode.clone(),
        })
    }

    fn record(&mut self, event: Event) {
        let metric = match event {
            Event::Metric(m) => m,
            _ => return,
        };

        let series = metric.metric_series();

        match self.mode {
            AggregationMode::Auto => {
                if metric.is_delta() {
                    self.record_sum(series, metric);
                } else {
                    self.map.insert(series, metric);
                }
            }
            AggregationMode::Sum => self.record_sum(series, metric),
            AggregationMode::Latest | AggregationMode::Diff => {
                if metric.is_cumulative() || metric.is_gauge() {
                    self.map.insert(series, metric);
                }
            }
            AggregationMode::Count => self.record_count(series, metric),
            AggregationMode::Max | AggregationMode::Min => self.record_comparison(series, metric),
            AggregationMode::Mean | AggregationMode::Stdev => {
                if metric.is_gauge() {
                    match self.multi_map.entry(series) {
                        Entry::Occupied(mut entry) => entry.get_mut().push(metric),
                        Entry::Vacant(entry) => {
                            entry.insert(vec![metric]);
                        }
                    }
                }
            }
        }

        emit!(AggregateEventRecorded);
    }

    fn record_count(&mut self, series: MetricSeries, metric: OtelMetric) {
        match self.map.entry(series) {
            Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                let current = existing.first_value_as_f64().unwrap_or(0.0);
                existing.set_first_value(current + 1.0);
                existing.metadata_mut().merge(metric.metadata().clone());
            }
            Entry::Vacant(entry) => {
                use crate::event::metric::MetricKind;
                let mut counter = OtelMetric::new_counter(metric.name(), MetricKind::Absolute, 1.0);
                *counter.metadata_mut() = metric.metadata().clone();
                entry.insert(counter);
            }
        }
    }

    fn record_sum(&mut self, series: MetricSeries, metric: OtelMetric) {
        if !metric.is_delta() {
            return;
        }
        match self.map.entry(series) {
            Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                if existing.is_delta() && existing.add(&metric) {
                    existing.metadata_mut().merge(metric.metadata().clone());
                } else {
                    emit!(AggregateUpdateFailed);
                    *existing = metric;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(metric);
            }
        }
    }

    fn record_comparison(&mut self, series: MetricSeries, metric: OtelMetric) {
        if !metric.is_cumulative() && !metric.is_gauge() {
            return;
        }
        match self.map.entry(series) {
            Entry::Occupied(mut entry) => {
                let existing_val = entry.get().first_value_as_f64();
                let new_val = metric.first_value_as_f64();
                if let (Some(ev), Some(nv)) = (existing_val, new_val) {
                    let should_update = match self.mode {
                        AggregationMode::Max => nv > ev,
                        AggregationMode::Min => nv < ev,
                        _ => false,
                    };
                    if should_update {
                        *entry.get_mut() = metric;
                    }
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(metric);
            }
        }
    }

    fn flush_into(&mut self, output: &mut Vec<Event>) {
        let map = std::mem::take(&mut self.map);
        for (series, mut metric) in map.clone().into_iter() {
            if matches!(self.mode, AggregationMode::Diff)
                && let Some(prev) = self.prev_map.get(&series)
                && !metric.subtract(prev)
            {
                emit!(AggregateUpdateFailed);
            }
            output.push(Event::Metric(metric));
        }

        let multi_map = std::mem::take(&mut self.multi_map);
        'outer: for (_series, entries) in multi_map.into_iter() {
            if entries.is_empty() {
                continue;
            }

            let mut combined = entries[0].clone();
            for m in entries.iter().skip(1) {
                if !combined.add(m) {
                    emit!(AggregateUpdateFailed);
                    continue 'outer;
                }
                combined.metadata_mut().merge(m.metadata().clone());
            }

            #[expect(
                clippy::cast_precision_loss,
                reason = "metric aggregation entry count; precise for |v| <= 2^53"
            )]
            let count = entries.len() as f64;
            let mean_value = combined.first_value_as_f64().unwrap_or(0.0) / count;

            match self.mode {
                AggregationMode::Mean => {
                    combined.set_first_value(mean_value);
                    output.push(Event::Metric(combined));
                }
                AggregationMode::Stdev => {
                    let variance = entries
                        .iter()
                        .filter_map(|m| {
                            let v = m.first_value_as_f64()?;
                            let diff = mean_value - v;
                            Some(diff * diff)
                        })
                        .sum::<f64>()
                        / count;
                    combined.set_first_value(variance.sqrt());
                    output.push(Event::Metric(combined));
                }
                _ => (),
            }
        }

        self.prev_map = map;
        emit!(AggregateFlushed);
    }
}

impl TaskTransform<Event> for Aggregate {
    fn transform(
        mut self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let mut flush_stream = tokio::time::interval(self.interval);

        Box::pin(stream! {
            let mut output = Vec::new();
            let mut done = false;
            while !done {
                tokio::select! {
                    _ = flush_stream.tick() => {
                        self.flush_into(&mut output);
                    },
                    maybe_event = input_rx.next() => {
                        match maybe_event {
                            None => {
                                self.flush_into(&mut output);
                                done = true;
                            }
                            Some(event) => {
                                if matches!(&event, Event::Metric(_)) {
                                    self.record(event);
                                } else {
                                    output.push(event);
                                }
                            }
                        }
                    }
                };
                for event in output.drain(..) {
                    yield event;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc, task::Poll};

    use futures::stream;
    use sol_lib::config::ComponentKey;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use vrl::value::Kind;

    use super::*;
    use crate::{
        event::{Event, OtelMetric, metric::MetricKind},
        schema::Definition,
        test_util::components::assert_transform_compliance,
        transforms::test::create_topology,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<AggregateConfig>();
    }

    fn make_metric(_name: &'static str, otel: OtelMetric) -> Event {
        let mut event = Event::Metric(otel)
            .with_source_id(Arc::new(ComponentKey::from("in")))
            .with_upstream_id(Arc::new(OutputId::from("transform")));
        event.metadata_mut().set_schema_definition(&Arc::new(
            Definition::new_with_default_metadata(Kind::any()),
        ));

        event.metadata_mut().set_source_type("unit_test_stream");

        event
    }

    #[test]
    fn incremental_auto() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Auto,
        })
        .unwrap();

        let counter_a_1 = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 42.0),
        );
        let counter_a_2 = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 43.0),
        );
        let counter_a_summed = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 85.0),
        );

        // Single item, just stored regardless of kind
        agg.record(counter_a_1.clone());
        let mut out = vec![];
        // We should flush 1 item counter_a_1
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&counter_a_1, &out[0]);

        // A subsequent flush doesn't send out anything
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // One more just to make sure that we don't re-see from the other buffer
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // Two increments with the same series, should sum into 1
        agg.record(counter_a_1.clone());
        agg.record(counter_a_2);
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(
            counter_a_summed.clone().into_otel_metric(),
            out[0].clone().into_otel_metric()
        );

        let counter_b_1 = make_metric(
            "counter_b",
            OtelMetric::new_counter("counter_b", MetricKind::Incremental, 44.0),
        );
        // Two increments with the different series, should get each back as-is
        agg.record(counter_a_1.clone());
        agg.record(counter_b_1.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(2, out.len());
        // B/c we don't know the order they'll come back
        for event in out {
            let metric = event.clone().into_otel_metric();
            match metric.name() {
                "counter_a" => assert_eq!(counter_a_1.clone().into_otel_metric(), metric),
                "counter_b" => assert_eq!(counter_b_1.clone().into_otel_metric(), metric),
                _ => panic!("Unexpected metric name in aggregate output"),
            }
        }
    }

    #[test]
    fn absolute_auto() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Auto,
        })
        .unwrap();

        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 42.0));
        let gauge_a_2 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 43.0));

        // Single item, just stored regardless of kind
        agg.record(gauge_a_1.clone());
        let mut out = vec![];
        // We should flush 1 item gauge_a_1
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_1, &out[0]);

        // A subsequent flush doesn't send out anything
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // One more just to make sure that we don't re-see from the other buffer
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // Two absolutes with the same series, should get the 2nd (last) back.
        agg.record(gauge_a_1.clone());
        agg.record(gauge_a_2.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_2, &out[0]);

        let gauge_b_1 = make_metric("gauge_b", OtelMetric::new_gauge("gauge_b", 44.0));
        // Two increments with the different series, should get each back as-is
        agg.record(gauge_a_1.clone());
        agg.record(gauge_b_1.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(2, out.len());
        // B/c we don't know the order they'll come back
        for event in out {
            let metric = event.clone().into_otel_metric();
            match metric.name() {
                "gauge_a" => assert_eq!(gauge_a_1.clone().into_otel_metric(), metric),
                "gauge_b" => assert_eq!(gauge_b_1.clone().into_otel_metric(), metric),
                _ => panic!("Unexpected metric name in aggregate output"),
            }
        }
    }

    #[test]
    fn count_agg() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Count,
        })
        .unwrap();

        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 42.0));
        let gauge_a_2 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 43.0));
        let result_count = make_metric(
            "gauge_a",
            OtelMetric::new_counter("gauge_a", MetricKind::Absolute, 1.0),
        );
        let result_count_2 = make_metric(
            "gauge_a",
            OtelMetric::new_counter("gauge_a", MetricKind::Absolute, 2.0),
        );

        // Single item, counter should be 1
        agg.record(gauge_a_1.clone());
        let mut out = vec![];
        // We should flush 1 item gauge_a_1
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&result_count, &out[0]);

        // A subsequent flush doesn't send out anything
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // One more just to make sure that we don't re-see from the other buffer
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // Two absolutes with the same series, counter should be 2
        agg.record(gauge_a_1.clone());
        agg.record(gauge_a_2.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&result_count_2, &out[0]);
    }

    #[test]
    fn absolute_max() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Max,
        })
        .unwrap();

        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 112.0));
        let gauge_a_2 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 89.0));

        // Single item, it should be returned as is
        agg.record(gauge_a_2.clone());
        let mut out = vec![];
        // We should flush 1 item gauge_a_2
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_2, &out[0]);

        // A subsequent flush doesn't send out anything
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // One more just to make sure that we don't re-see from the other buffer
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // Two absolutes, result should be higher of the 2
        agg.record(gauge_a_1.clone());
        agg.record(gauge_a_2.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_1, &out[0]);
    }

    #[test]
    fn absolute_min() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Min,
        })
        .unwrap();

        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 32.0));
        let gauge_a_2 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 89.0));

        // Single item, it should be returned as is
        agg.record(gauge_a_2.clone());
        let mut out = vec![];
        // We should flush 1 item gauge_a_2
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_2, &out[0]);

        // A subsequent flush doesn't send out anything
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // One more just to make sure that we don't re-see from the other buffer
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // Two absolutes, result should be lower of the 2
        agg.record(gauge_a_1.clone());
        agg.record(gauge_a_2.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_1, &out[0]);
    }

    #[test]
    fn absolute_diff() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Diff,
        })
        .unwrap();

        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 32.0));
        let gauge_a_2 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 82.0));
        let result = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 50.0));

        // Single item, it should be returned as is
        agg.record(gauge_a_2.clone());
        let mut out = vec![];
        // We should flush 1 item gauge_a_2
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_2, &out[0]);

        // A subsequent flush doesn't send out anything
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // One more just to make sure that we don't re-see from the other buffer
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // Two absolutes in 2 separate flushes, result should be diff between the 2
        agg.record(gauge_a_1.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_1, &out[0]);

        agg.record(gauge_a_2.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&result, &out[0]);
    }

    #[test]
    fn absolute_diff_conflicting_type() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Diff,
        })
        .unwrap();

        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 32.0));
        let gauge_a_2 = make_metric(
            "gauge_a",
            OtelMetric::new_counter("gauge_a", MetricKind::Absolute, 1.0),
        );

        let mut out = vec![];
        // Two absolutes in 2 separate flushes, result should be second one due to different types
        agg.record(gauge_a_1.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_1, &out[0]);

        agg.record(gauge_a_2.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        // Due to incompatible results, the new value just overwrites the old one
        assert_eq!(&gauge_a_2, &out[0]);
    }

    #[test]
    fn absolute_mean() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Mean,
        })
        .unwrap();

        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 32.0));
        let gauge_a_2 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 82.0));
        let gauge_a_3 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 51.0));
        let mean_result = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 55.0));

        // Single item, it should be returned as is
        agg.record(gauge_a_2.clone());
        let mut out = vec![];
        // We should flush 1 item gauge_a_2
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&gauge_a_2, &out[0]);

        // A subsequent flush doesn't send out anything
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // One more just to make sure that we don't re-see from the other buffer
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(0, out.len());

        // Three absolutes, result should be mean
        agg.record(gauge_a_1.clone());
        agg.record(gauge_a_2.clone());
        agg.record(gauge_a_3.clone());
        out.clear();
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&mean_result, &out[0]);
    }

    #[test]
    fn absolute_stdev() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Stdev,
        })
        .unwrap();

        let gauges = vec![
            make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 25.0)),
            make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 30.0)),
            make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 35.0)),
            make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 40.0)),
            make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 45.0)),
            make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 50.0)),
            make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 55.0)),
        ];
        let stdev_result = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 10.0));

        for gauge in gauges {
            agg.record(gauge);
        }
        let mut out = vec![];
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&stdev_result, &out[0]);
    }

    #[test]
    fn conflicting_value_type() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Auto,
        })
        .unwrap();

        let counter = make_metric(
            "the-thing",
            OtelMetric::new_counter("the-thing", MetricKind::Incremental, 42.0),
        );
        let mut values = BTreeSet::<String>::new();
        values.insert("a".into());
        values.insert("b".into());
        let set = make_metric(
            "the-thing",
            OtelMetric::new_set_from_values("the-thing", MetricKind::Incremental, values),
        );
        let summed = make_metric(
            "the-thing",
            OtelMetric::new_counter("the-thing", MetricKind::Incremental, 84.0),
        );

        // when types conflict the new values replaces whatever is there

        // Start with an counter
        agg.record(counter.clone());
        // Another will "add" to it
        agg.record(counter.clone());
        // Then an set will replace it due to a failed update
        agg.record(set.clone());
        // Then a set union would be a noop
        agg.record(set.clone());
        let mut out = vec![];
        // We should flush 1 item counter
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&set, &out[0]);

        // Start out with an set
        agg.record(set.clone());
        // Union with itself, a noop
        agg.record(set);
        // Send an counter with the same name, will replace due to a failed update
        agg.record(counter.clone());
        // Send another counter will "add"
        agg.record(counter);
        let mut out = vec![];
        // We should flush 1 item counter
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(summed.into_otel_metric(), out[0].clone().into_otel_metric());
    }

    #[test]
    fn conflicting_kinds() {
        let mut agg = Aggregate::new(&AggregateConfig {
            interval_ms: 1000_u64,
            mode: AggregationMode::Auto,
        })
        .unwrap();

        let incremental = make_metric(
            "the-thing",
            OtelMetric::new_counter("the-thing", MetricKind::Incremental, 42.0),
        );
        let absolute = make_metric(
            "the-thing",
            OtelMetric::new_counter("the-thing", MetricKind::Absolute, 43.0),
        );
        let summed = make_metric(
            "the-thing",
            OtelMetric::new_counter("the-thing", MetricKind::Incremental, 84.0),
        );

        // when types conflict the new values replaces whatever is there

        // Start with an incremental
        agg.record(incremental.clone());
        // Another will "add" to it
        agg.record(incremental.clone());
        // Then an absolute will replace it with a failed update
        agg.record(absolute.clone());
        // Then another absolute will replace it normally
        agg.record(absolute.clone());
        let mut out = vec![];
        // We should flush 1 item incremental
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(&absolute, &out[0]);

        // Start out with an absolute
        agg.record(absolute.clone());
        // Replace it normally
        agg.record(absolute);
        // Send an incremental with the same name, will replace due to a failed update
        agg.record(incremental.clone());
        // Send another incremental will "add"
        agg.record(incremental);
        let mut out = vec![];
        // We should flush 1 item incremental
        agg.flush_into(&mut out);
        assert_eq!(1, out.len());
        assert_eq!(summed.into_otel_metric(), out[0].clone().into_otel_metric());
    }

    #[tokio::test]
    async fn transform_shutdown() {
        let agg = toml::from_str::<AggregateConfig>(
            r"
interval_ms = 999999
",
        )
        .unwrap()
        .build(&TransformContext::default())
        .await
        .unwrap();

        let agg = agg.into_task();

        let counter_a_1 = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 42.0),
        );
        let counter_a_2 = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 43.0),
        );
        let counter_a_summed = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 85.0),
        );
        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 42.0));
        let gauge_a_2 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 43.0));
        let inputs = vec![counter_a_1, counter_a_2, gauge_a_1, gauge_a_2.clone()];

        // Queue up some events to be consumed & recorded
        let in_stream = Box::pin(stream::iter(inputs));
        // Kick off the transform process which should consume & record them
        let mut out_stream = agg.transform_events(in_stream);

        // B/c the input stream has ended we will have gone through the `input_rx.next() => None`
        // part of the loop and do the shutting down final flush immediately. We'll already be able
        // to read our expected bits on the output.
        let mut count = 0_u8;
        while let Some(event) = out_stream.next().await {
            count += 1;
            let metric = event.clone().into_otel_metric();
            match metric.name() {
                "counter_a" => assert_eq!(counter_a_summed.clone().into_otel_metric(), metric),
                "gauge_a" => assert_eq!(gauge_a_2.clone().into_otel_metric(), metric),
                _ => panic!("Unexpected metric name in aggregate output"),
            };
        }
        // There were only 2
        assert_eq!(2, count);
    }

    #[tokio::test]
    async fn transform_interval() {
        let transform_config = toml::from_str::<AggregateConfig>("").unwrap();

        let counter_a_1 = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 42.0),
        );
        let counter_a_2 = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 43.0),
        );
        let counter_a_summed = make_metric(
            "counter_a",
            OtelMetric::new_counter("counter_a", MetricKind::Incremental, 85.0),
        );
        let gauge_a_1 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 42.0));
        let gauge_a_2 = make_metric("gauge_a", OtelMetric::new_gauge("gauge_a", 43.0));

        assert_transform_compliance(async {
            let (tx, rx) = mpsc::channel(10);
            let (topology, out) = create_topology(ReceiverStream::new(rx), transform_config).await;
            let mut out = ReceiverStream::new(out);

            tokio::time::pause();

            // tokio interval is always immediately ready, so we poll once to make sure
            // we trip it/set the interval in the future
            assert_eq!(Poll::Pending, futures::poll!(out.next()));

            // Now send our events
            tx.send(counter_a_1).await.unwrap();
            tx.send(counter_a_2).await.unwrap();
            tx.send(gauge_a_1).await.unwrap();
            tx.send(gauge_a_2.clone()).await.unwrap();
            // We won't have flushed yet b/c the interval hasn't elapsed, so no outputs
            assert_eq!(Poll::Pending, futures::poll!(out.next()));
            // Now fast forward time enough that our flush should trigger.
            tokio::time::advance(Duration::from_secs(11)).await;
            // We should have had an interval fire now and our output aggregate events should be
            // available.
            let mut count = 0_u8;
            while count < 2 {
                match out.next().await {
                    Some(event) => {
                        let metric = event.clone().into_otel_metric();
                        match metric.name() {
                            "counter_a" => {
                                assert_eq!(counter_a_summed.clone().into_otel_metric(), metric)
                            }
                            "gauge_a" => assert_eq!(gauge_a_2.clone().into_otel_metric(), metric),
                            _ => panic!("Unexpected metric name in aggregate output"),
                        };
                        count += 1;
                    }
                    _ => {
                        panic!("Unexpectedly received None in output stream");
                    }
                }
            }
            // We should be back to pending, having nothing waiting for us
            assert_eq!(Poll::Pending, futures::poll!(out.next()));

            drop(tx);
            topology.stop().await;
            assert_eq!(out.next().await, None);
        })
        .await;
    }
}
