use sol_lib::event::OtelMetric;

use crate::sinks::util::buffer::metrics::{MetricNormalize, MetricSet};

#[derive(Default)]
pub(crate) struct AppsignalMetricsNormalizer;

impl MetricNormalize for AppsignalMetricsNormalizer {
    fn normalize(&mut self, state: &mut MetricSet, metric: OtelMetric) -> Option<OtelMetric> {
        if metric.is_sum() {
            state.make_incremental(metric)
        } else if metric.is_gauge() && !metric.is_set() {
            state.make_absolute(metric)
        } else {
            Some(metric)
        }
    }

    fn exp_hist_bounds(&self) -> Option<&[f64]> {
        use crate::sinks::util::buffer::metrics::DEFAULT_HISTOGRAM_BOUNDS;
        Some(DEFAULT_HISTOGRAM_BOUNDS)
    }
}

#[cfg(test)]
mod tests {
    use super::AppsignalMetricsNormalizer;
    use crate::{
        event::{MetricKind, OtelMetric},
        test_util::metrics::{assert_normalize, tests},
    };

    #[test]
    fn absolute_counter() {
        tests::absolute_counter_normalize_to_incremental(AppsignalMetricsNormalizer);
    }

    #[test]
    fn incremental_counter() {
        tests::incremental_counter_normalize_to_incremental(AppsignalMetricsNormalizer);
    }

    #[test]
    fn mixed_counter() {
        tests::mixed_counter_normalize_to_incremental(AppsignalMetricsNormalizer);
    }

    #[test]
    fn absolute_gauge() {
        tests::absolute_gauge_normalize_to_absolute(AppsignalMetricsNormalizer);
    }

    #[test]
    fn incremental_gauge() {
        tests::incremental_gauge_normalize_to_absolute(AppsignalMetricsNormalizer);
    }

    #[test]
    fn mixed_gauge() {
        tests::mixed_gauge_normalize_to_absolute(AppsignalMetricsNormalizer);
    }

    #[test]
    fn other_metrics() {
        let metric =
            OtelMetric::new_set_from_values("set", MetricKind::Incremental, Vec::<String>::new());

        assert_normalize(
            AppsignalMetricsNormalizer,
            vec![metric.clone()],
            vec![Some(metric)],
        );
    }
}
