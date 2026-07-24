// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Query result cache (task 8).
//!
//! A [`QueryCache`] trait with an in-memory `moka` default ([caching ADR](../../../docs/workspace/parquet-backend/adrs/query-caching-strategy.md)):
//! bounded by a **byte budget** (NFR5 memory ceiling, via a per-entry weigher
//! over the cached batches' memory size), TinyLFU eviction, no active
//! invalidation. The trait lets a future Redis backend slot in without touching
//! the query path.
//!
//! Per-entry TTL ([FR2](../../../docs/workspace/backend-metrics-perf/DESIGN.md#fr2),
//! [cache-invalidation-scope ADR](../../../docs/workspace/backend-metrics-perf/adrs/cache-invalidation-scope.md)):
//! each insert carries a [`TtlClass`] derived from the query's window — a
//! window entirely sealed (`hi < now − 1 day`, [`QueryScope::is_sealed`])
//! gets [`SEALED_TTL`]; a mutable-window or unclassified entry keeps the
//! short configured TTL (15s). [`ScopedExpiry`] applies the class via moka's
//! `Expiry`; the byte budget bounds memory regardless of TTL.
//!
//! [`CacheKey`] floors the query time range to a 15s bucket so adjacent
//! dashboard refreshes (which move the range by a few seconds) collide on the
//! same entry instead of each missing the cache.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;

use super::inventory::QueryScope;

/// Time-bucket width: 15 seconds, in nanoseconds.
const BUCKET_NS: i64 = 15_000_000_000;
/// Default cache memory budget in bytes (NFR5; 256 MB).
const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Default time-to-live (caching ADR) — the **short** TTL: mutable-window and
/// unclassified entries.
const DEFAULT_TTL: Duration = Duration::from_secs(15);
/// TTL for entries whose query window is entirely sealed (FR2, ADR: 15 min).
/// A sealed window's data no longer changes, so the entry could in principle
/// live forever; 15 min keeps the eviction pressure low while the byte-budget
/// weigher (NFR5) still bounds total memory.
const SEALED_TTL: Duration = Duration::from_secs(15 * 60);

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

/// Per-entry TTL classification (FR2, [cache-invalidation-scope ADR](../../../docs/workspace/backend-metrics-perf/adrs/cache-invalidation-scope.md)).
///
/// New invariant (ADR consequences): every insert supplies its entry's window
/// classification; an unclassifiable entry (no window at the call site — raw
/// SQL, unbounded metadata) defaults to [`TtlClass::Mutable`], the safe
/// direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TtlClass {
    /// Window overlaps the mutable trailing day, or the caller had no window
    /// to classify → short TTL (the configured 15s staleness bound).
    Mutable,
    /// Window entirely sealed ([`QueryScope::is_sealed`]) → [`SEALED_TTL`]:
    /// the result cannot change, so it survives catalog refreshes.
    Sealed,
}

impl TtlClass {
    /// Classify a query window against wall-clock `now_ns`: entirely sealed →
    /// [`TtlClass::Sealed`]; mutable or absent (`None`) → [`TtlClass::Mutable`].
    pub fn classify(scope: Option<QueryScope>, now_ns: i64) -> Self {
        match scope {
            Some(s) if s.is_sealed(now_ns) => TtlClass::Sealed,
            _ => TtlClass::Mutable,
        }
    }
}

/// One stored cache entry: the result plus the [`TtlClass`] fixed at insert,
/// which [`ScopedExpiry`] reads back to pick the entry's TTL.
#[derive(Clone)]
struct CachedEntry {
    result: CachedResult,
    class: TtlClass,
}

/// moka `Expiry` applying the per-entry TTL policy (FR2): [`TtlClass::Sealed`]
/// → [`SEALED_TTL`], [`TtlClass::Mutable`] → the short configured TTL.
/// Replaces the builder-level `time_to_live` (which would cap every entry at
/// the short TTL, defeating the sealed class).
struct ScopedExpiry {
    /// The short TTL for mutable/unclassified entries (`cache.ttl_secs`).
    mutable_ttl: Duration,
}

impl ScopedExpiry {
    fn ttl_of(&self, class: TtlClass) -> Duration {
        match class {
            TtlClass::Mutable => self.mutable_ttl,
            TtlClass::Sealed => SEALED_TTL,
        }
    }
}

impl moka::Expiry<CacheKey, CachedEntry> for ScopedExpiry {
    fn expire_after_create(
        &self,
        _key: &CacheKey,
        value: &CachedEntry,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        Some(self.ttl_of(value.class))
    }

    fn expire_after_update(
        &self,
        _key: &CacheKey,
        value: &CachedEntry,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        // An overwrite restarts the entry's TTL under its (possibly new) class,
        // matching what builder-level `time_to_live` did for updates.
        Some(self.ttl_of(value.class))
    }
}

/// A query result cache. Implementors must be cheap to share across the
/// stateless querier's request handlers.
pub trait QueryCache: Send + Sync {
    /// Look up a cached result.
    fn get(&self, key: &CacheKey) -> Option<CachedResult>;
    /// Insert (or overwrite) a result under its window classification (FR2):
    /// the class picks the entry's TTL — see [`TtlClass`].
    fn insert(&self, key: CacheKey, value: CachedResult, class: TtlClass);
    /// Drop all entries. Kept on the trait for tests and future operational
    /// hooks; the catalog refresh no longer calls it — per-entry TTL bounds
    /// staleness instead (FR2, cache-invalidation-scope ADR).
    #[allow(dead_code)]
    fn clear(&self);
}

/// In-memory `moka` cache — the default [`QueryCache`].
pub struct MokaQueryCache {
    inner: moka::sync::Cache<CacheKey, CachedEntry>,
}

impl MokaQueryCache {
    /// Default cache: 256 MB budget, short TTL 15s.
    pub fn new() -> Self {
        Self::with_budget(DEFAULT_MAX_BYTES, DEFAULT_TTL)
    }

    /// Cache bounded by total result **bytes** (NFR5 memory ceiling) with a
    /// per-entry TTL: `ttl` for mutable/unclassified entries, [`SEALED_TTL`]
    /// for sealed-window entries ([`ScopedExpiry`], FR2). Each entry weighs
    /// the in-memory size of its `RecordBatch`es, so the cache holds at most
    /// ~`max_bytes` of results regardless of entry count or TTL class.
    pub fn with_budget(max_bytes: u64, ttl: Duration) -> Self {
        Self {
            inner: moka::sync::Cache::builder()
                .max_capacity(max_bytes)
                .weigher(|_k, v: &CachedEntry| {
                    let bytes: usize = v
                        .result
                        .iter()
                        .map(RecordBatch::get_array_memory_size)
                        .sum();
                    u32::try_from(bytes).unwrap_or(u32::MAX)
                })
                .expire_after(ScopedExpiry { mutable_ttl: ttl })
                .build(),
        }
    }

    /// Approximate total bytes currently cached (NFR5 gauge).
    pub fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }
}

impl Default for MokaQueryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryCache for MokaQueryCache {
    fn get(&self, key: &CacheKey) -> Option<CachedResult> {
        self.inner.get(key).map(|e| e.result)
    }

    fn insert(&self, key: CacheKey, value: CachedResult, class: TtlClass) {
        self.inner.insert(
            key,
            CachedEntry {
                result: value,
                class,
            },
        );
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
            cache.insert(key.clone(), Arc::clone(&v), TtlClass::Mutable);
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
        let cache = MokaQueryCache::with_budget(1_000_000, Duration::from_millis(40));
        let key = CacheKey::for_sql("SELECT 1");
        cache.insert(key.clone(), Arc::new(Vec::new()), TtlClass::Mutable);
        assert!(cache.get(&key).is_some(), "fresh entry present");
        std::thread::sleep(Duration::from_millis(80));
        cache.inner.run_pending_tasks();
        assert!(cache.get(&key).is_none(), "entry expired after TTL");
    }

    #[test]
    fn test_byte_budget_bounds_the_cache() {
        // B3 / NFR5: the cache is bounded by result *bytes*, not entry count.
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let val: CachedResult = Arc::new(vec![batch]);

        // Budget smaller than one entry → not admitted — even a Sealed entry:
        // no entry outlives the byte budget (FR2 invariant, weigher unchanged).
        let tiny = MokaQueryCache::with_budget(1, Duration::from_secs(60));
        tiny.insert(CacheKey::for_sql("q"), Arc::clone(&val), TtlClass::Sealed);
        tiny.inner.run_pending_tasks();
        assert!(
            tiny.get(&CacheKey::for_sql("q")).is_none(),
            "oversized entry rejected by budget"
        );

        // Ample budget → admitted, and weighted_size tracks the cached bytes.
        let big = MokaQueryCache::with_budget(10_000_000, Duration::from_secs(60));
        big.insert(CacheKey::for_sql("q"), val, TtlClass::Mutable);
        big.inner.run_pending_tasks();
        assert!(big.get(&CacheKey::for_sql("q")).is_some());
        assert!(big.weighted_size() > 0, "weighted size tracks cached bytes");
    }

    #[test]
    fn test_cache_clear_invalidates_all() {
        let cache = MokaQueryCache::new();
        let key = CacheKey::for_sql("SELECT 1");
        cache.insert(key.clone(), Arc::new(Vec::new()), TtlClass::Sealed);
        assert!(cache.get(&key).is_some());
        cache.clear(); // explicit clear drops everything, sealed included
        assert!(cache.get(&key).is_none(), "clear must drop all entries");
    }

    /// One day in ns — the sealed boundary offset (`SEALED_OFFSET_NS`).
    const DAY_NS: i64 = 24 * 3_600 * 1_000_000_000;

    #[test]
    fn test_live_entry_short_ttl_classification() {
        // is_sealed boundary (FR2): sealed ⇔ hi < now − 1 day, strict.
        let now_ns = 100 * DAY_NS;
        let scope = |hi_ns: i64| {
            Some(QueryScope {
                lo_ns: hi_ns - DAY_NS,
                hi_ns,
            })
        };
        // hi just below the boundary → entirely sealed → long TTL class.
        assert_eq!(
            TtlClass::classify(scope(now_ns - DAY_NS - 1), now_ns),
            TtlClass::Sealed
        );
        // hi exactly at now − 1 day → NOT sealed (strict <) → short TTL.
        assert_eq!(
            TtlClass::classify(scope(now_ns - DAY_NS), now_ns),
            TtlClass::Mutable
        );
        // hi just above the boundary, and a live window ending at now.
        assert_eq!(
            TtlClass::classify(scope(now_ns - DAY_NS + 1), now_ns),
            TtlClass::Mutable
        );
        assert_eq!(TtlClass::classify(scope(now_ns), now_ns), TtlClass::Mutable);
    }

    #[test]
    fn test_unscoped_insert_defaults_to_short_ttl() {
        // Windowless/unknown → Mutable, the safe direction (ADR invariant).
        let now_ns = 100 * DAY_NS;
        assert_eq!(TtlClass::classify(None, now_ns), TtlClass::Mutable);

        // Expiry duration choice per class (deterministic — no wall-clock
        // sleeps): Mutable → the configured short TTL; Sealed → SEALED_TTL.
        let short = Duration::from_secs(15);
        let expiry = ScopedExpiry { mutable_ttl: short };
        let entry = |class: TtlClass| CachedEntry {
            result: Arc::new(Vec::new()),
            class,
        };
        let key = CacheKey::for_sql("q");
        let at = std::time::Instant::now();
        use moka::Expiry;
        assert_eq!(
            expiry.expire_after_create(&key, &entry(TtlClass::Mutable), at),
            Some(short)
        );
        assert_eq!(
            expiry.expire_after_create(&key, &entry(TtlClass::Sealed), at),
            Some(SEALED_TTL)
        );
        assert_eq!(SEALED_TTL, Duration::from_secs(900), "ADR: 15 min");
        // An overwrite restarts the TTL under the new class.
        assert_eq!(
            expiry.expire_after_update(&key, &entry(TtlClass::Sealed), at, Some(short)),
            Some(SEALED_TTL)
        );
    }
}
