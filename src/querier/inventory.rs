// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Per-file time-interval inventory (backend-metrics-perf task 1).
//!
//! Pure parsing of a store Parquet file path into conservative time bounds
//! ([FR1](../../../docs/workspace/backend-metrics-perf/DESIGN.md#fr1),
//! [per-query-file-pruning ADR](../../../docs/workspace/backend-metrics-perf/adrs/per-query-file-pruning.md)).
//! The parser is **total**: any name it cannot understand maps to the
//! unbounded interval, so an unknown file is always included — pruning can
//! only ever skip files proven out-of-window, never lose data.

use std::path::Path;

use chrono::NaiveDate;

use super::compaction::{COMPACTED_PREFIX, ROLLUP_PREFIX, hour_end_ns};

/// Lateness/skew allowance, in nanoseconds (1 h wall-clock).
///
/// Generous relative to the ~30 s gateway flush cadence and consistent with
/// the 24 h-margin philosophy of `SEALED_OFFSET_NS`
/// (`src/querier/prometheus.rs`). One documented constant — both the parse-time
/// widening of hour-compacted intervals and the query-time widening in
/// [`FileInterval::overlaps`] use it.
pub(crate) const INTERVAL_MARGIN_NS: i64 = 60 * 60 * 1_000_000_000;

/// Conservative `[lo_ns, hi_ns]` event-time bounds for one store file,
/// parsed from its path alone (no I/O). Closed interval, UTC epoch ns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileInterval {
    /// Earliest event-time nanosecond the file may contain.
    pub(crate) lo_ns: i64,
    /// Latest event-time nanosecond the file may contain.
    pub(crate) hi_ns: i64,
}

impl FileInterval {
    /// `(-∞, +∞)` — the safety default: always overlaps every query window.
    pub(crate) const UNBOUNDED: Self = Self {
        lo_ns: i64::MIN,
        hi_ns: i64::MAX,
    };

    /// Include-this-file test: does the query window `[lo_ns, hi_ns]`, widened
    /// by `margin_ns` on both sides, overlap this interval? (ADR rule:
    /// include iff `[lo − margin, hi + margin]` overlaps.)
    pub(crate) fn overlaps(&self, lo_ns: i64, hi_ns: i64, margin_ns: i64) -> bool {
        let query_lo = lo_ns.saturating_sub(margin_ns);
        let query_hi = hi_ns.saturating_add(margin_ns);
        self.lo_ns <= query_hi && query_lo <= self.hi_ns
    }
}

/// Parse a store file path into its conservative [`FileInterval`].
///
/// Interval rules (ADR, A baseline — task 1b tightens the raw-file rule to
/// exact name-carried bounds):
/// - `dt=YYYY-MM-DD` ancestor dir → base interval `[day_start, day_end)`;
/// - raw `HH-MM-SS-<uuid>[-<i>].parquet` → `[day_start, flush + margin]`;
/// - `compacted-hHH-*` → that hour widened by the margin (reuses the
///   [`hour_end_ns`] convention);
/// - `compacted-<date>` / `rollup-<tier>` → the full day;
/// - anything else → [`FileInterval::UNBOUNDED`] (always included).
///
/// Total: never errors, never excludes by default.
pub(crate) fn parse_file_interval(path: &Path) -> FileInterval {
    interval_of(path).unwrap_or(FileInterval::UNBOUNDED)
}

/// The fallible core of [`parse_file_interval`]; `None` means "unparseable",
/// which the caller maps to [`FileInterval::UNBOUNDED`].
fn interval_of(path: &Path) -> Option<FileInterval> {
    let name = path.file_name()?.to_str()?;
    let date = dt_day(path)?;
    let day_lo = day_start_ns(date)?;
    let day_hi = hour_end_ns(date, 23); // start of the next day

    if name.starts_with(ROLLUP_PREFIX) {
        // rollup-<tier>.parquet: downsampled over the whole sealed day.
        return Some(FileInterval {
            lo_ns: day_lo,
            hi_ns: day_hi,
        });
    }
    if let Some(rest) = name.strip_prefix(COMPACTED_PREFIX) {
        if let Some(hour_part) = rest.strip_prefix('h') {
            // compacted-hHH-<date>.parquet: that hour ± margin. The margin is
            // baked in at parse time (ADR: "widened by margin") because an
            // hour merge may carry late events stamped just outside the hour.
            let hour: u32 = hour_part.split('-').next()?.parse().ok()?;
            let lo = date
                .and_hms_opt(hour, 0, 0)? // also validates hour < 24
                .and_utc()
                .timestamp_nanos_opt()?;
            return Some(FileInterval {
                lo_ns: lo.saturating_sub(INTERVAL_MARGIN_NS),
                hi_ns: hour_end_ns(date, hour).saturating_add(INTERVAL_MARGIN_NS),
            });
        }
        // compacted-<date>.parquet: lossless merge of the whole sealed day.
        return Some(FileInterval {
            lo_ns: day_lo,
            hi_ns: day_hi,
        });
    }

    // Raw gateway file `HH-MM-SS-<uuid>[-<i>].parquet`.
    //
    // Verified (task 1 acceptance criterion 2): the sink's `%H-%M-%S` path
    // template stamps **event time**, not write time — `Template::render` →
    // `render_timestamp` (`src/template.rs:579-594`) formats the event's own
    // timestamp (log timestamp / `Metric::timestamp()` / trace
    // `time_unix_nano`), falling back to `Utc::now()` only when the event has
    // none; the Parquet batch sink renders the path from the **first event of
    // the flushed batch** (`src/sinks/file/mod.rs:639-640`, `flush_batch`).
    // So the name's stamp is the first batched event's event time. Later
    // events in the same batch can post-date it by at most the batch flush
    // window (~30 s cadence, ≪ the 1 h margin), so `hi = stamp + margin`
    // holds for in-order traffic; the residual risk — a batch whose *first*
    // event is stamped far in the past while later events are current — is
    // exactly what task 1b's exact-bounds naming (ADR A′) eliminates. The
    // lower bound stays `day_start`: a batch may carry late events from
    // earlier in the day.
    let flush_ns = parse_flush_ns(name, date)?;
    Some(FileInterval {
        lo_ns: day_lo,
        hi_ns: flush_ns.saturating_add(INTERVAL_MARGIN_NS),
    })
}

/// The `dt=YYYY-MM-DD` component from the path's ancestors, parsed as a date.
/// Mirrors `Compactor::partition_dirs`' `dt=` parsing (`compaction.rs`).
fn dt_day(path: &Path) -> Option<NaiveDate> {
    path.ancestors().find_map(|dir| {
        let name = dir.file_name()?.to_str()?;
        let date = name.strip_prefix("dt=")?;
        NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
    })
}

/// UTC nanoseconds at midnight starting `date`.
fn day_start_ns(date: NaiveDate) -> Option<i64> {
    date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_nanos_opt()
}

/// Wall-clock nanoseconds of the `HH-MM-SS` stamp of a raw file name on
/// `date`. `None` unless the name starts with a valid `HH-MM-SS` triple
/// (`and_hms_opt` rejects out-of-range fields).
fn parse_flush_ns(name: &str, date: NaiveDate) -> Option<i64> {
    let stem = name.strip_suffix(".parquet").unwrap_or(name);
    let mut parts = stem.split('-');
    let hour: u32 = parts.next()?.parse().ok()?;
    let minute: u32 = parts.next()?.parse().ok()?;
    let second: u32 = parts.next()?.parse().ok()?;
    date.and_hms_opt(hour, minute, second)?
        .and_utc()
        .timestamp_nanos_opt()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    const NS_PER_SEC: i64 = 1_000_000_000;
    const HOUR_NS: i64 = 3_600 * NS_PER_SEC;
    const DAY_NS: i64 = 24 * HOUR_NS;
    /// 2026-07-10T00:00:00Z (anchored: `date -u -d '2026-07-10' +%s`).
    const JUL10_NS: i64 = 1_783_641_600 * NS_PER_SEC;
    /// 2026-06-02T00:00:00Z.
    const JUN02_NS: i64 = 1_780_358_400 * NS_PER_SEC;

    fn interval(path: &str) -> FileInterval {
        parse_file_interval(&PathBuf::from(path))
    }

    #[test]
    fn test_interval_raw_file_bounds() {
        // Raw gateway flush: lower bound = day start (late events from earlier
        // in the day), upper bound = the HH-MM-SS stamp + margin.
        let got = interval(
            "/store/metrics/gauge/dt=2026-07-10/12-18-28-550e8400-e29b-41d4-a716-446655440000.parquet",
        );
        let stamp = JUL10_NS + 12 * HOUR_NS + (18 * 60 + 28) * NS_PER_SEC;
        assert_eq!(got.lo_ns, JUL10_NS);
        assert_eq!(got.hi_ns, stamp + INTERVAL_MARGIN_NS);

        // Multi-part batch suffix `-<i>` parses the same way.
        let multi = interval(
            "/store/logs/dt=2026-07-10/00-00-05-550e8400-e29b-41d4-a716-446655440000-3.parquet",
        );
        assert_eq!(multi.lo_ns, JUL10_NS);
        assert_eq!(multi.hi_ns, JUL10_NS + 5 * NS_PER_SEC + INTERVAL_MARGIN_NS);
    }

    #[test]
    fn test_interval_compacted_hour() {
        // compacted-h07 → [07:00 − margin, 08:00 + margin] of that day
        // (hour_end_ns convention: the end is the start of the next hour).
        let got = interval("/store/logs/dt=2026-06-02/compacted-h07-2026-06-02.parquet");
        assert_eq!(got.lo_ns, JUN02_NS + 7 * HOUR_NS - INTERVAL_MARGIN_NS);
        assert_eq!(got.hi_ns, JUN02_NS + 8 * HOUR_NS + INTERVAL_MARGIN_NS);
    }

    #[test]
    fn test_interval_compacted_day_and_rollup() {
        // Both a sealed-day merge and a rollup tier cover exactly their day.
        let day = FileInterval {
            lo_ns: JUN02_NS,
            hi_ns: JUN02_NS + DAY_NS,
        };
        assert_eq!(
            interval("/store/traces/dt=2026-06-02/compacted-2026-06-02.parquet"),
            day
        );
        assert_eq!(
            interval("/store/metrics/sum/dt=2026-06-02/rollup-1h.parquet"),
            day
        );
        assert_eq!(
            interval("/store/metrics/sum/dt=2026-06-02/rollup-5m.parquet"),
            day
        );
    }

    #[test]
    fn test_interval_unparseable_is_unbounded() {
        for path in [
            // No dt= day context at all.
            "/store/metrics/garbage.parquet",
            "/store/metrics/12-18-28-uuid.parquet",
            // dt= present but the name matches no known shape.
            "/store/logs/dt=2026-07-10/README.txt",
            "/store/logs/dt=2026-07-10/notes.parquet",
            // Out-of-range HH-MM-SS fields are not a valid raw stamp.
            "/store/logs/dt=2026-07-10/25-00-00-uuid.parquet",
            "/store/logs/dt=2026-07-10/12-61-00-uuid.parquet",
            // Bad hour digits after compacted-h.
            "/store/logs/dt=2026-07-10/compacted-hxx-2026-07-10.parquet",
            "/store/logs/dt=2026-07-10/compacted-h99-2026-07-10.parquet",
            // Malformed dt= dir.
            "/store/logs/dt=not-a-date/compacted-2026-07-10.parquet",
        ] {
            let got = interval(path);
            assert_eq!(got, FileInterval::UNBOUNDED, "{path}");
            // Always included, whatever the window.
            assert!(got.overlaps(0, 0, 0), "{path}");
            assert!(got.overlaps(i64::MIN, i64::MAX, INTERVAL_MARGIN_NS), "{path}");
        }
    }

    #[test]
    fn test_interval_overlap_semantics() {
        let iv = FileInterval {
            lo_ns: 1_000,
            hi_ns: 2_000,
        };
        // Zero margin: closed-interval touch counts as overlap.
        assert!(iv.overlaps(2_000, 3_000, 0));
        assert!(iv.overlaps(0, 1_000, 0));
        assert!(iv.overlaps(1_200, 1_800, 0)); // fully inside
        assert!(iv.overlaps(0, 5_000, 0)); // fully covering
        assert!(!iv.overlaps(2_001, 3_000, 0));
        assert!(!iv.overlaps(0, 999, 0));
        // Margin widens the query window on both sides.
        assert!(iv.overlaps(2_100, 3_000, 100)); // 2_100 − 100 touches hi
        assert!(!iv.overlaps(2_101, 3_000, 100));
        assert!(iv.overlaps(0, 900, 100)); // 900 + 100 touches lo
        assert!(!iv.overlaps(0, 899, 100));
        // Saturation: extreme windows + margin must not wrap.
        assert!(FileInterval::UNBOUNDED.overlaps(i64::MIN, i64::MAX, i64::MAX));
        assert!(!iv.overlaps(i64::MIN, i64::MIN, 100));
        assert!(!iv.overlaps(i64::MAX, i64::MAX, 100));
    }
}
