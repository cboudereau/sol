use sol_core::event::{OtelAttributes, OtelMetric};
use sol_vrl_metrics::MetricsStorage;

pub(crate) fn test_vrl_metrics_storage() -> MetricsStorage {
    let storage = MetricsStorage::default();
    let metric = OtelMetric::new_gauge("utilization", 0.5).with_tags(Some(
        OtelAttributes::from_iter([("component_id".to_string(), "test".to_string())]),
    ));
    storage.cache.store(vec![metric].into());
    storage
}
