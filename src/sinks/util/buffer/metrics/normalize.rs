use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

use lru::LruCache;
use serde_with::serde_as;
use snafu::Snafu;
use sol_config_macros::configurable_component;
use sol_lib::{
    ByteSizeOf,
    event::{Event, MetricKind, OtelMetric, metric::MetricSeries},
};

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum NormalizerError {
    #[snafu(display("`max_bytes` must be greater than zero"))]
    InvalidMaxBytes,
    #[snafu(display("`max_events` must be greater than zero"))]
    InvalidMaxEvents,
    #[snafu(display("`time_to_live` must be greater than zero"))]
    InvalidTimeToLive,
}

/// Defines behavior for creating the MetricNormalizer
#[serde_as]
#[configurable_component]
#[configurable(metadata(docs::advanced))]
#[derive(Clone, Copy, Debug, Default)]
pub struct NormalizerConfig<D: NormalizerSettings + Clone> {
    /// The maximum size in bytes of the events in the metrics normalizer cache, excluding cache overhead.
    #[serde(default = "default_max_bytes::<D>")]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    pub max_bytes: Option<usize>,

    /// The maximum number of events of the metrics normalizer cache
    #[serde(default = "default_max_events::<D>")]
    #[configurable(metadata(docs::type_unit = "events"))]
    pub max_events: Option<usize>,

    /// The maximum age of a metric not being updated before it is evicted from the metrics normalizer cache.
    #[serde(default = "default_time_to_live::<D>")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::human_name = "Time To Live"))]
    pub time_to_live: Option<u64>,

    #[serde(skip)]
    pub _d: PhantomData<D>,
}

const fn default_max_bytes<D: NormalizerSettings>() -> Option<usize> {
    D::MAX_BYTES
}

const fn default_max_events<D: NormalizerSettings>() -> Option<usize> {
    D::MAX_EVENTS
}

const fn default_time_to_live<D: NormalizerSettings>() -> Option<u64> {
    D::TIME_TO_LIVE
}

impl<D: NormalizerSettings + Clone> NormalizerConfig<D> {
    pub fn validate(&self) -> Result<NormalizerConfig<D>, NormalizerError> {
        let config = NormalizerConfig::<D> {
            max_bytes: self.max_bytes.or(D::MAX_BYTES),
            max_events: self.max_events.or(D::MAX_EVENTS),
            time_to_live: self.time_to_live.or(D::TIME_TO_LIVE),
            _d: Default::default(),
        };
        match (config.max_bytes, config.max_events, config.time_to_live) {
            (Some(0), _, _) => Err(NormalizerError::InvalidMaxBytes),
            (_, Some(0), _) => Err(NormalizerError::InvalidMaxEvents),
            (_, _, Some(0)) => Err(NormalizerError::InvalidTimeToLive),
            _ => Ok(config),
        }
    }

    pub const fn into_settings(self) -> MetricSetSettings {
        MetricSetSettings {
            max_bytes: self.max_bytes,
            max_events: self.max_events,
            time_to_live: self.time_to_live,
        }
    }
}

pub trait NormalizerSettings {
    const MAX_EVENTS: Option<usize>;
    const MAX_BYTES: Option<usize>;
    const TIME_TO_LIVE: Option<u64>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultNormalizerSettings;

impl NormalizerSettings for DefaultNormalizerSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = None;
    const TIME_TO_LIVE: Option<u64> = None;
}

/// Normalizes metrics according to a set of rules.
///
/// Depending on the system in which they are being sent to, metrics may have to be modified in order to fit the data
/// model or constraints placed on that system.  Typically, this boils down to whether or not the system can accept
/// absolute metrics or incremental metrics: the latest value of a metric, or the delta between the last time the
/// metric was observed and now, respective. Other rules may need to be applied, such as dropping metrics of a specific
/// type that the system does not support.
///
/// The trait provides a simple interface to apply this logic uniformly, given a reference to a simple state container
/// that allows tracking the necessary information of a given metric over time. As well, given the optional return, it
/// composes nicely with iterators (i.e. using `filter_map`) in order to filter metrics within existing
/// iterator/stream-based approaches.
pub trait MetricNormalize {
    /// Normalizes the metric against the given state.
    ///
    /// If the metric was normalized successfully, `Some(metric)` will be returned. Otherwise, `None` is returned.
    ///
    /// In some cases, a metric may be successfully added/tracked within the given state, but due to the normalization
    /// logic, it cannot yet be emitted. An example of this is normalizing all metrics to be incremental.
    ///
    /// In this example, if an incoming metric is already incremental, it can be passed through unchanged.  If the
    /// incoming metric is absolute, however, we need to see it at least twice in order to calculate the incremental
    /// delta necessary to emit an incremental version. This means that the first time an absolute metric is seen,
    /// `normalize` would return `None`, and the subsequent calls would return `Some(metric)`.
    ///
    /// However, a metric may simply not be supported by a normalization implementation, and so `None` may or may not be
    /// a common return value. This behavior is, thus, implementation defined.
    fn normalize(&mut self, state: &mut MetricSet, metric: OtelMetric) -> Option<OtelMetric>;

    /// If `Some`, ExponentialHistograms are converted to explicit-bounds Histograms
    /// using the given bounds before normalization. Sinks with native ExponentialHistogram
    /// support return `None` (the default) to pass them through unchanged.
    fn exp_hist_bounds(&self) -> Option<&[f64]> {
        None
    }
}

/// A self-contained metric normalizer.
///
/// The normalization state is stored internally, and it can only be created from a normalizer implementation that is
/// either `Default` or is constructed ahead of time, so it is primarily useful for constructing a usable normalizer
/// via implicit conversion methods or when no special parameters are required for configuring the underlying normalizer.
pub struct MetricNormalizer<N> {
    state: MetricSet,
    normalizer: N,
}

impl<N> MetricNormalizer<N> {
    /// Creates a new normalizer with the given configuration.
    pub fn with_config<D: NormalizerSettings + Clone>(
        normalizer: N,
        config: NormalizerConfig<D>,
    ) -> Self {
        let settings = config
            .validate()
            .unwrap_or_else(|e| panic!("Invalid cache settings: {e:?}"))
            .into_settings();
        Self {
            state: MetricSet::new(settings),
            normalizer,
        }
    }

    /// Gets a mutable reference to the current metric state for this normalizer.
    pub const fn get_state_mut(&mut self) -> &mut MetricSet {
        &mut self.state
    }
}

pub const DEFAULT_HISTOGRAM_BOUNDS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

impl<N: MetricNormalize> MetricNormalizer<N> {
    /// Normalizes the metric against the internal normalization state.
    ///
    /// ExponentialHistogram metrics are converted to explicit-bounds Histogram
    /// before per-sink normalization when the sink's normalizer returns
    /// `Some(bounds)` from `exp_hist_bounds()`.
    pub fn normalize(&mut self, mut metric: OtelMetric) -> Option<OtelMetric> {
        if let Some(bounds) = self.normalizer.exp_hist_bounds() {
            metric.convert_exponential_to_histogram(bounds);
        }
        self.normalizer.normalize(&mut self.state, metric)
    }

    /// Normalize and wrap as Event.
    pub fn normalize_otel(&mut self, otel: OtelMetric) -> Option<Event> {
        self.normalize(otel).map(Event::Metric)
    }
}

impl<N: Default> Default for MetricNormalizer<N> {
    fn default() -> Self {
        Self {
            state: MetricSet::default(),
            normalizer: N::default(),
        }
    }
}

impl<N> From<N> for MetricNormalizer<N> {
    fn from(normalizer: N) -> Self {
        Self {
            state: MetricSet::default(),
            normalizer,
        }
    }
}

/// A cached metric with its last-seen timestamp for TTL tracking.
#[derive(Clone, Debug)]
struct CachedMetric {
    metric: OtelMetric,
    last_seen: Option<Instant>,
}

impl ByteSizeOf for CachedMetric {
    fn allocated_bytes(&self) -> usize {
        self.metric.allocated_bytes()
    }
}

impl CachedMetric {
    const fn new(metric: OtelMetric, last_seen: Option<Instant>) -> Self {
        Self { metric, last_seen }
    }

    fn is_expired(&self, ttl: Duration, reference_time: Instant) -> bool {
        match self.last_seen {
            Some(ts) => reference_time.duration_since(ts) >= ttl,
            None => false,
        }
    }
}

/// Configuration for capacity-based eviction (memory and/or entry count limits).
#[derive(Clone, Debug)]
pub struct CapacityPolicy {
    /// Maximum memory usage in bytes
    pub max_bytes: Option<usize>,
    /// Maximum number of entries
    pub max_events: Option<usize>,
    /// Current memory usage tracking
    current_memory: usize,
}

impl CapacityPolicy {
    /// Creates a new capacity policy with both memory and entry limits.
    pub const fn new(max_bytes: Option<usize>, max_events: Option<usize>) -> Self {
        Self {
            max_bytes,
            max_events,
            current_memory: 0,
        }
    }

    /// Gets the current memory usage.
    pub const fn current_memory(&self) -> usize {
        self.current_memory
    }

    /// Updates memory tracking when an entry is removed.
    const fn remove_memory(&mut self, bytes: usize) {
        self.current_memory = self.current_memory.saturating_sub(bytes);
    }

    /// Frees the memory for an item if max_bytes is set.
    /// Only calculates and tracks memory when max_bytes is specified.
    fn free_item(&mut self, series: &MetricSeries, entry: &CachedMetric) {
        if self.max_bytes.is_some() {
            let freed_memory = self.item_size(series, entry);
            self.remove_memory(freed_memory);
        }
    }

    /// Updates memory tracking.
    const fn replace_memory(&mut self, old_bytes: usize, new_bytes: usize) {
        self.current_memory = self
            .current_memory
            .saturating_sub(old_bytes)
            .saturating_add(new_bytes);
    }

    /// Checks if the current state exceeds memory limits.
    const fn exceeds_memory_limit(&self) -> bool {
        if let Some(max_bytes) = self.max_bytes {
            self.current_memory > max_bytes
        } else {
            false
        }
    }

    /// Checks if the given entry count exceeds entry limits.
    const fn exceeds_entry_limit(&self, entry_count: usize) -> bool {
        if let Some(max_events) = self.max_events {
            entry_count > max_events
        } else {
            false
        }
    }

    /// Returns true if any limits are currently exceeded.
    const fn needs_eviction(&self, entry_count: usize) -> bool {
        self.exceeds_memory_limit() || self.exceeds_entry_limit(entry_count)
    }

    /// Gets the total memory size of entry/series, excluding LRU cache overhead.
    fn item_size(&self, series: &MetricSeries, entry: &CachedMetric) -> usize {
        entry.allocated_bytes() + series.allocated_bytes()
    }
}

#[derive(Clone, Debug)]
pub struct TtlPolicy {
    /// Time-to-live for entries
    pub ttl: Duration,
    /// How often to run cleanup
    pub cleanup_interval: Duration,
    /// Last time cleanup was performed
    pub(crate) last_cleanup: Instant,
}

/// Configuration for automatic cleanup of expired entries.
impl TtlPolicy {
    /// Creates a new TTL policy with the given duration.
    /// Cleanup interval defaults to TTL/10 with a 10-second minimum.
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            cleanup_interval: ttl.div_f32(10.0).max(Duration::from_secs(10)),
            last_cleanup: Instant::now(),
        }
    }

    /// Checks if it's time to run cleanup.
    ///
    /// Returns Some(current_time) if cleanup should be performed, None otherwise.
    pub fn should_cleanup(&self) -> Option<Instant> {
        let now = Instant::now();
        if now.duration_since(self.last_cleanup) >= self.cleanup_interval {
            Some(now)
        } else {
            None
        }
    }

    /// Marks cleanup as having been performed with the provided timestamp.
    pub const fn mark_cleanup_done(&mut self, now: Instant) {
        self.last_cleanup = now;
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricSetSettings {
    pub max_bytes: Option<usize>,
    pub max_events: Option<usize>,
    pub time_to_live: Option<u64>,
}

/// Dual-limit cache using standard LRU with optional capacity and TTL policies.
///
/// This implementation uses the standard LRU crate with optional enforcement of both
/// memory and entry count limits via CapacityPolicy, plus optional TTL via TtlPolicy.
#[derive(Clone, Debug)]
pub struct MetricSet {
    /// LRU cache for storing metrics
    inner: LruCache<MetricSeries, CachedMetric>,
    /// Optional capacity policy for memory and/or entry count limits
    capacity_policy: Option<CapacityPolicy>,
    /// Optional TTL policy for time-based expiration
    ttl_policy: Option<TtlPolicy>,
}

impl MetricSet {
    /// Creates a new MetricSet with the given settings.
    pub fn new(settings: MetricSetSettings) -> Self {
        // Create capacity policy if any capacity limit is set
        let capacity_policy = match (settings.max_bytes, settings.max_events) {
            (None, None) => None,
            (max_bytes, max_events) => Some(CapacityPolicy::new(max_bytes, max_events)),
        };

        // Create TTL policy if time-to-live is set
        let ttl_policy = settings
            .time_to_live
            .map(|ttl| TtlPolicy::new(Duration::from_secs(ttl)));

        Self::with_policies(capacity_policy, ttl_policy)
    }

    /// Creates a new MetricSet with the given policies.
    pub fn with_policies(
        capacity_policy: Option<CapacityPolicy>,
        ttl_policy: Option<TtlPolicy>,
    ) -> Self {
        // Always use an unbounded cache since we manually track limits
        // This ensures our capacity policy can properly track memory for all evicted entries
        Self {
            inner: LruCache::unbounded(),
            capacity_policy,
            ttl_policy,
        }
    }

    /// Gets the current capacity policy.
    pub const fn capacity_policy(&self) -> Option<&CapacityPolicy> {
        self.capacity_policy.as_ref()
    }

    /// Gets the current TTL policy.
    pub const fn ttl_policy(&self) -> Option<&TtlPolicy> {
        self.ttl_policy.as_ref()
    }

    /// Gets a mutable reference to the TTL policy configuration.
    pub const fn ttl_policy_mut(&mut self) -> Option<&mut TtlPolicy> {
        self.ttl_policy.as_mut()
    }

    /// Gets the current number of entries in the cache.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Gets the current memory usage in bytes.
    pub fn weighted_size(&self) -> u64 {
        self.capacity_policy
            .as_ref()
            .map_or(0, |cp| cp.current_memory() as u64)
    }

    /// Creates a timestamp if TTL is enabled.
    fn create_timestamp(&self) -> Option<Instant> {
        self.ttl_policy.as_ref().map(|_| Instant::now())
    }

    /// Enforce memory and entry limits by evicting LRU entries.
    fn enforce_capacity_policy(&mut self) {
        let Some(ref mut capacity_policy) = self.capacity_policy else {
            return; // No capacity limits configured
        };

        // Keep evicting until we're within limits
        while capacity_policy.needs_eviction(self.inner.len()) {
            if let Some((series, entry)) = self.inner.pop_lru() {
                capacity_policy.free_item(&series, &entry);
            } else {
                break; // No more entries to evict
            }
        }
    }

    /// Perform TTL cleanup if configured and needed.
    fn maybe_cleanup(&mut self) {
        // Check if cleanup is needed and get the current timestamp in one operation
        let now = match self.ttl_policy().and_then(|config| config.should_cleanup()) {
            Some(timestamp) => timestamp,
            None => return, // No cleanup needed
        };

        // Perform the cleanup using the same timestamp
        self.cleanup_expired(now);

        // Mark cleanup as done with the same timestamp
        if let Some(config) = self.ttl_policy_mut() {
            config.mark_cleanup_done(now);
        }
    }

    /// Remove expired entries based on TTL using the provided timestamp.
    fn cleanup_expired(&mut self, now: Instant) {
        // Get the TTL from the policy
        let Some(ttl) = self.ttl_policy().map(|policy| policy.ttl) else {
            return; // No TTL policy, nothing to do
        };

        let mut expired_keys = Vec::new();

        // Collect expired keys using the provided timestamp
        for (series, entry) in self.inner.iter() {
            if entry.is_expired(ttl, now) {
                expired_keys.push(series.clone());
            }
        }

        // Remove expired entries and update memory tracking (if max_bytes is set)
        for series in expired_keys {
            if let Some(entry) = self.inner.pop(&series)
                && let Some(ref mut capacity_policy) = self.capacity_policy
            {
                capacity_policy.free_item(&series, &entry);
            }
        }
    }

    /// Internal insert that updates memory tracking and enforces limits.
    fn insert_with_tracking(&mut self, series: MetricSeries, cached: CachedMetric) {
        let Some(ref mut capacity_policy) = self.capacity_policy else {
            self.inner.put(series, cached);
            return;
        };

        if capacity_policy.max_bytes.is_some() {
            let new_size = capacity_policy.item_size(&series, &cached);
            if let Some(existing) = self.inner.put(series.clone(), cached) {
                let old_size = capacity_policy.item_size(&series, &existing);
                capacity_policy.replace_memory(old_size, new_size);
            } else {
                capacity_policy.replace_memory(0, new_size);
            }
        } else {
            self.inner.put(series, cached);
        }

        self.enforce_capacity_policy();
    }

    fn store(&mut self, series: MetricSeries, metric: OtelMetric, timestamp: Option<Instant>) {
        self.insert_with_tracking(series, CachedMetric::new(metric, timestamp));
    }

    /// Consumes this MetricSet and returns a vector of OtelMetric.
    pub fn into_metrics(mut self) -> Vec<OtelMetric> {
        self.cleanup_expired(Instant::now());
        let mut metrics = Vec::new();
        while let Some((_series, cached)) = self.inner.pop_lru() {
            metrics.push(cached.metric);
        }
        metrics
    }

    /// Either pass the metric through as-is if absolute, or convert it
    /// to absolute if incremental.
    pub fn make_absolute(&mut self, metric: OtelMetric) -> Option<OtelMetric> {
        self.maybe_cleanup();
        match metric.kind() {
            MetricKind::Absolute => Some(metric),
            MetricKind::Incremental => Some(self.incremental_to_absolute(metric)),
        }
    }

    /// Either convert the metric to incremental if absolute, or
    /// aggregate it with any previous value if already incremental.
    pub fn make_incremental(&mut self, metric: OtelMetric) -> Option<OtelMetric> {
        self.maybe_cleanup();
        match metric.kind() {
            MetricKind::Absolute => self.absolute_to_incremental(metric),
            MetricKind::Incremental => Some(metric),
        }
    }

    /// Convert the incremental metric into an absolute one, using the
    /// state buffer to keep track of the value throughout the entire
    /// application uptime.
    fn incremental_to_absolute(&mut self, metric: OtelMetric) -> OtelMetric {
        let timestamp = self.create_timestamp();
        let series = metric.metric_series();
        let mut accumulated = match self.inner.get(&series) {
            Some(cached) => {
                let mut acc = cached.metric.clone();
                if !acc.add(&metric) { metric } else { acc }
            }
            None => metric,
        };
        self.store(series.clone(), accumulated.clone(), timestamp);
        accumulated.set_kind(MetricKind::Absolute);
        accumulated
    }

    /// Convert the absolute metric into an incremental by calculating
    /// the increment from the last saved absolute state.
    fn absolute_to_incremental(&mut self, metric: OtelMetric) -> Option<OtelMetric> {
        // We only emit a metric when we've calculated an actual delta for it.
        // The first time an absolute metric is seen, we store it and return None.
        // This avoids massive counter spikes on Vector restart.
        let timestamp = self.create_timestamp();
        let series = metric.metric_series();
        match self.inner.get(&series) {
            Some(cached) => {
                let mut delta = metric.clone();
                if delta.subtract(&cached.metric) {
                    self.store(series.clone(), metric, timestamp);
                    delta.set_kind(MetricKind::Incremental);
                    Some(delta)
                } else {
                    self.store(series.clone(), metric, timestamp);
                    None
                }
            }
            None => {
                self.store(series, metric, timestamp);
                None
            }
        }
    }

    pub fn insert_update(&mut self, metric: OtelMetric) {
        self.maybe_cleanup();
        let timestamp = self.create_timestamp();
        let series = metric.metric_series();
        if metric.kind() == MetricKind::Incremental
            && let Some(cached) = self.inner.get(&series)
        {
            let mut accumulated = cached.metric.clone();
            if accumulated.add(&metric) {
                accumulated.metadata_mut().merge(metric.metadata().clone());
                self.store(series, accumulated, timestamp);
                return;
            }
            warn!(message = "Metric changed type, dropping old value.", %series);
        }
        self.store(series, metric, timestamp);
    }

    /// Removes a series from the cache.
    ///
    /// If the series existed and was removed, returns true.  Otherwise, false.
    pub fn remove(&mut self, series: &MetricSeries) -> bool {
        if let Some(cached) = self.inner.pop(series) {
            if let Some(ref mut capacity_policy) = self.capacity_policy {
                capacity_policy.free_item(series, &cached);
            }
            return true;
        }
        false
    }
}

impl Default for MetricSet {
    fn default() -> Self {
        Self::new(MetricSetSettings::default())
    }
}
