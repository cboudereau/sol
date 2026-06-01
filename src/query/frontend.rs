//! Query-frontend: time-range splitting + merge + per-shard immutable cache (task 11).
//!
//! Long metric ranges are split into per-day shards aligned to UTC midnight
//! ([long-range-metrics ADR](../../../docs/workspace/parquet-backend/adrs/long-range-metrics-strategy.md)).
//! Completed historical shards (those ending at/before the sealed-day boundary)
//! are **immutable** and cached permanently; only the in-progress shard is
//! recomputed on each refresh — this fixes the whole-range cache-key defect.
//!
//! Merge rules (per the ADR): range-vector shards overlap by the lookback
//! window so a boundary `rate()` matches the unsplit result; `topk` is
//! partial-then-merge; `histogram_quantile` sums bucket counts across shards
//! *then* computes (never averages quantiles).

use std::collections::BTreeMap;

/// One nanosecond day.
const DAY_NS: i64 = 86_400_000_000_000;

/// A per-day shard of a split range query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shard {
    /// Where the underlying scan starts — `start_ns - lookback` so range
    /// functions have their window at the left edge (overlap-by-lookback).
    pub query_start_ns: i64,
    /// First emitted timestamp (inclusive), aligned to a day boundary except
    /// the first shard which clamps to the query's start.
    pub start_ns: i64,
    /// End timestamp (exclusive), a day boundary except the last shard.
    pub end_ns: i64,
    /// Historical shards (end ≤ sealed boundary) are immutable → cacheable.
    pub cacheable: bool,
}

/// Split `[start_ns, end_ns)` into day-aligned shards. `lookback_ns` is the
/// range-function window (overlap); `sealed_ns` is the sealed-day boundary
/// (`now − grace`) — shards ending at/before it are cacheable.
pub fn split(start_ns: i64, end_ns: i64, lookback_ns: i64, sealed_ns: i64) -> Vec<Shard> {
    let mut shards = Vec::new();
    if end_ns <= start_ns {
        return shards;
    }
    let mut cursor = start_ns;
    while cursor < end_ns {
        // next UTC midnight strictly after `cursor`
        let next_midnight = (cursor / DAY_NS + 1) * DAY_NS;
        let shard_end = next_midnight.min(end_ns);
        shards.push(Shard {
            query_start_ns: cursor - lookback_ns,
            start_ns: cursor,
            end_ns: shard_end,
            cacheable: shard_end <= sealed_ns,
        });
        cursor = shard_end;
    }
    super::telemetry::record_shard_split(shards.len() as u64);
    shards
}

/// Short non-metric queries (traces/logs) should not be split.
pub fn should_split(start_ns: i64, end_ns: i64) -> bool {
    end_ns.saturating_sub(start_ns) > DAY_NS
}

/// A range series: label set → time-ordered `(ts, value)` points.
pub type Series = BTreeMap<String, Vec<(f64, f64)>>;

/// Merge per-shard matrices into one, concatenating each series' points in
/// time order (shards cover disjoint emit ranges, so no double-count).
pub fn merge_series(shards: Vec<Series>) -> Series {
    let mut out: Series = BTreeMap::new();
    for shard in shards {
        for (label, points) in shard {
            out.entry(label).or_default().extend(points);
        }
    }
    for points in out.values_mut() {
        points.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        points.dedup_by(|a, b| a.0 == b.0); // drop boundary-overlap duplicates
    }
    out
}

/// `topk` partial-then-merge: each shard contributes its own top-k series; the
/// global top-k is the k highest across the union.
pub fn merge_topk(k: usize, partials: Vec<Vec<(String, f64)>>) -> Vec<(String, f64)> {
    let mut all: Vec<(String, f64)> = partials.into_iter().flatten().collect();
    all.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    all.truncate(k);
    all
}

/// `histogram_quantile` across shards: sum the per-shard bucket counts
/// element-wise, *then* compute the quantile (never average quantiles).
pub fn merge_histogram_quantile(
    phi: f64,
    per_shard_counts: &[Vec<f64>],
    bounds: &[f64],
) -> Option<f64> {
    let width = per_shard_counts.iter().map(Vec::len).max()?;
    let mut summed = vec![0.0; width];
    for counts in per_shard_counts {
        for (i, c) in counts.iter().enumerate() {
            summed[i] += c;
        }
    }
    super::prometheus::histogram_quantile(phi, &summed, bounds)
}

#[cfg(test)]
mod tests {
    use super::*;

    // 2026-05-30 00:00:00 UTC in ns (a day boundary: divisible by DAY_NS).
    const D0: i64 = 1_780_704_000_000_000_000;

    #[test]
    fn test_split_aligns_to_utc_midnight_and_step() {
        // start mid-day-0, end mid-day-2 → 3 shards, inner boundaries at midnight
        let start = D0 + DAY_NS / 2;
        let end = D0 + 2 * DAY_NS + DAY_NS / 4;
        let shards = split(start, end, 0, i64::MAX);
        assert_eq!(shards.len(), 3, "{shards:?}");
        assert_eq!(shards[0].start_ns, start, "first clamps to query start");
        assert_eq!(shards[0].end_ns, D0 + DAY_NS, "first ends at midnight");
        assert_eq!(shards[1].start_ns, D0 + DAY_NS, "middle aligned to midnight");
        assert_eq!(shards[1].end_ns, D0 + 2 * DAY_NS);
        assert_eq!(shards[2].end_ns, end, "last clamps to query end");
        // every inner boundary is a midnight multiple
        assert_eq!((D0 + DAY_NS) % DAY_NS, 0);
    }

    #[test]
    fn test_rate_shards_overlap_by_lookback() {
        let lookback = 300_000_000_000; // 5m
        let shards = split(D0, D0 + 2 * DAY_NS, lookback, i64::MAX);
        for s in &shards {
            // each shard scans from start - lookback so rate() has its window
            assert_eq!(s.query_start_ns, s.start_ns - lookback, "{s:?}");
        }
        // merging two adjacent shards' points (with an overlapping boundary
        // sample) dedups → equals the unsplit point set
        let mut a: Series = BTreeMap::new();
        a.insert("x".into(), vec![(1.0, 10.0), (2.0, 20.0)]);
        let mut b: Series = BTreeMap::new();
        b.insert("x".into(), vec![(2.0, 20.0), (3.0, 30.0)]); // (2.0) overlaps
        let merged = merge_series(vec![a, b]);
        assert_eq!(merged["x"], vec![(1.0, 10.0), (2.0, 20.0), (3.0, 30.0)]);
    }

    #[test]
    fn test_histogram_quantile_merge_sums_buckets() {
        let bounds = [10.0, 20.0, 30.0, 40.0, 50.0];
        // unsplit counts
        let whole = [0.0, 20.0, 30.0, 30.0, 15.0, 5.0];
        let unsplit = super::super::prometheus::histogram_quantile(0.95, &whole, &bounds).unwrap();
        // same totals split across two shards (element-wise halves)
        let s1 = vec![0.0, 12.0, 18.0, 20.0, 10.0, 5.0];
        let s2 = vec![0.0, 8.0, 12.0, 10.0, 5.0, 0.0];
        let merged = merge_histogram_quantile(0.95, &[s1, s2], &bounds).unwrap();
        assert!((merged - unsplit).abs() < 1e-9, "merged {merged} vs unsplit {unsplit}");
    }

    #[test]
    fn test_topk_partial_merge() {
        let shard_a = vec![("a".to_string(), 5.0), ("b".to_string(), 9.0)];
        let shard_b = vec![("c".to_string(), 7.0), ("d".to_string(), 3.0)];
        let top2 = merge_topk(2, vec![shard_a, shard_b]);
        assert_eq!(top2, vec![("b".to_string(), 9.0), ("c".to_string(), 7.0)]);
    }

    #[test]
    fn test_historical_shard_cached_permanently() {
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let sealed = D0 + 2 * DAY_NS; // days 0,1 sealed; day 2 live
        let computed = AtomicUsize::new(0);
        let mut cache: HashMap<(i64, i64), ()> = HashMap::new();

        let run = |start: i64, end: i64, cache: &mut HashMap<(i64, i64), ()>| {
            for s in split(start, end, 0, sealed) {
                let key = (s.start_ns, s.end_ns);
                if s.cacheable && cache.contains_key(&key) {
                    continue; // historical shard: cache hit, no compute
                }
                computed.fetch_add(1, Ordering::SeqCst);
                if s.cacheable {
                    cache.insert(key, ());
                }
            }
        };

        // first query over days 0..2 (+ a bit into day 2)
        run(D0, D0 + 2 * DAY_NS + DAY_NS / 2, &mut cache);
        let first = computed.swap(0, Ordering::SeqCst);
        assert_eq!(first, 3, "2 historical + 1 live computed");

        // re-query with end advanced further into the live day
        run(D0, D0 + 2 * DAY_NS + 3 * DAY_NS / 4, &mut cache);
        let second = computed.load(Ordering::SeqCst);
        assert_eq!(second, 1, "only the live shard recomputed; historical cached");
    }

    #[test]
    fn test_short_query_not_split() {
        assert!(!should_split(D0, D0 + DAY_NS / 2), "sub-day query bypasses splitting");
        assert!(should_split(D0, D0 + 3 * DAY_NS), "multi-day query splits");
    }

    #[test]
    fn test_split_emits_frontend_metrics() {
        use metrics_util::MetricKind;
        use metrics_util::debugging::DebuggingRecorder;
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            split(D0, D0 + 3 * DAY_NS, 0, i64::MAX);
            super::super::telemetry::record_shard_cache(true);
        });
        let s = snap.snapshot().into_vec();
        assert!(
            s.iter().any(|(k, _, _, _)| k.kind() == MetricKind::Counter
                && k.key().name() == "sol_query_shard_splits_total"),
            "split count emitted"
        );
        assert!(
            s.iter().any(|(k, _, _, _)| k.kind() == MetricKind::Counter
                && k.key().name() == "sol_query_shard_cache_requests_total"),
            "shard-cache counter emitted"
        );
    }
}
