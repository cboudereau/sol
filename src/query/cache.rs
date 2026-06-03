//! Query result cache (task 8).
//!
//! A [`QueryCache`] trait with an in-memory `moka` default ([caching ADR](../../../docs/workspace/parquet-backend/adrs/query-caching-strategy.md)):
//! TTL 15s, max 1000 entries, TinyLFU eviction, no active invalidation. The
//! trait lets a future Redis backend slot in without touching the query path.
//!
//! [`CacheKey`] floors the query time range to a 15s bucket so adjacent
//! dashboard refreshes (which move the range by a few seconds) collide on the
//! same entry instead of each missing the cache.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;

/// Time-bucket width: 15 seconds, in nanoseconds.
const BUCKET_NS: i64 = 15_000_000_000;
/// Default max entries (caching ADR).
const DEFAULT_CAPACITY: u64 = 1000;
/// Default time-to-live (caching ADR).
const DEFAULT_TTL: Duration = Duration::from_secs(15);

/// A cached result: the batches produced by a query, shared cheaply.
pub type CachedResult = Arc<Vec<RecordBatch>>;

/// Cache key: the query text plus its start/end floored to a 15s bucket.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    query: String,
    start_bucket: i64,
    end_bucket: i64,
}

impl CacheKey {
    /// Build a key, flooring `start_ns`/`end_ns` to the 15s bucket.
    pub fn new(query: &str, start_ns: i64, end_ns: i64) -> Self {
        Self {
            query: query.to_string(),
            start_bucket: start_ns.div_euclid(BUCKET_NS),
            end_bucket: end_ns.div_euclid(BUCKET_NS),
        }
    }

    /// Key for a query that already embeds its time range in the SQL text.
    pub fn for_sql(sql: &str) -> Self {
        Self::new(sql, 0, 0)
    }
}

/// A query result cache. Implementors must be cheap to share across the
/// stateless querier's request handlers.
pub trait QueryCache: Send + Sync {
    /// Look up a cached result.
    fn get(&self, key: &CacheKey) -> Option<CachedResult>;
    /// Insert (or overwrite) a result.
    fn insert(&self, key: CacheKey, value: CachedResult);
    /// Drop all entries — called when the catalog refreshes (new files make
    /// cached results stale).
    fn clear(&self);
}

/// In-memory `moka` cache — the default [`QueryCache`].
pub struct MokaQueryCache {
    inner: moka::sync::Cache<CacheKey, CachedResult>,
}

impl MokaQueryCache {
    /// Default cache: capacity 1000, TTL 15s.
    pub fn new() -> Self {
        Self::with_params(DEFAULT_CAPACITY, DEFAULT_TTL)
    }

    /// Cache with explicit capacity and TTL (used in tests).
    pub fn with_params(max_capacity: u64, ttl: Duration) -> Self {
        Self {
            inner: moka::sync::Cache::builder()
                .max_capacity(max_capacity)
                .time_to_live(ttl)
                .build(),
        }
    }
}

impl Default for MokaQueryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryCache for MokaQueryCache {
    fn get(&self, key: &CacheKey) -> Option<CachedResult> {
        self.inner.get(key)
    }

    fn insert(&self, key: CacheKey, value: CachedResult) {
        self.inner.insert(key, value);
    }

    fn clear(&self) {
        self.inner.invalidate_all();
        self.inner.run_pending_tasks();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_cache_key_buckets_to_15s() {
        // 1s and 14s fall in the same 15s start-bucket; 2s and 3s in the same
        // end-bucket → identical keys.
        let a = CacheKey::new("q", 1_000_000_000, 2_000_000_000);
        let b = CacheKey::new("q", 14_000_000_000, 3_000_000_000);
        assert_eq!(a, b, "adjacent refreshes within a 15s bucket must collide");
        // crossing the bucket boundary (16s → bucket 1) must differ
        let c = CacheKey::new("q", 16_000_000_000, 2_000_000_000);
        assert_ne!(a, c);
        // different query text differs
        assert_ne!(a, CacheKey::new("other", 1_000_000_000, 2_000_000_000));
    }

    #[test]
    fn test_cache_hit_returns_without_executing() {
        let cache = MokaQueryCache::new();
        let key = CacheKey::for_sql("SELECT 1");
        let calls = AtomicUsize::new(0);
        // get-or-compute closure: only runs on a miss.
        let run = || {
            if let Some(v) = cache.get(&key) {
                return v;
            }
            calls.fetch_add(1, Ordering::SeqCst);
            let v: CachedResult = Arc::new(Vec::new());
            cache.insert(key.clone(), Arc::clone(&v));
            v
        };
        let _ = run();
        let _ = run();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second identical query must hit cache"
        );
    }

    #[test]
    fn test_cache_ttl_expiry() {
        let cache = MokaQueryCache::with_params(10, Duration::from_millis(40));
        let key = CacheKey::for_sql("SELECT 1");
        cache.insert(key.clone(), Arc::new(Vec::new()));
        assert!(cache.get(&key).is_some(), "fresh entry present");
        std::thread::sleep(Duration::from_millis(80));
        cache.inner.run_pending_tasks();
        assert!(cache.get(&key).is_none(), "entry expired after TTL");
    }

    #[test]
    fn test_cache_clear_invalidates_all() {
        let cache = MokaQueryCache::new();
        let key = CacheKey::for_sql("SELECT 1");
        cache.insert(key.clone(), Arc::new(Vec::new()));
        assert!(cache.get(&key).is_some());
        cache.clear(); // simulates a catalog refresh
        assert!(cache.get(&key).is_none(), "refresh must drop stale entries");
    }

    #[test]
    fn test_cache_lru_eviction_at_capacity() {
        let cache = MokaQueryCache::with_params(2, Duration::from_secs(60));
        for i in 0..3 {
            cache.insert(CacheKey::for_sql(&format!("q{i}")), Arc::new(Vec::new()));
        }
        cache.inner.run_pending_tasks();
        assert!(
            cache.inner.entry_count() <= 2,
            "capacity bound enforced (eviction)"
        );
    }
}
