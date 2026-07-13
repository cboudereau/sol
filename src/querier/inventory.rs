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

use std::collections::HashMap;
use std::path::{Path, PathBuf};

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

/// Clock-skew allowance for exact name-carried bounds, in nanoseconds (5 s).
///
/// Task 1b (ADR A′): the gateway Parquet batch sink stamps each file name with
/// the batch's **exact** min/max `time_unix_nano`
/// (`src/sinks/file/mod.rs::parquet_bounds_path`), so no lateness margin is
/// needed — a file provably contains nothing outside its named bounds. This
/// small constant only cushions cross-host wall-clock skew between the
/// stamping gateway and whatever clock anchors a query window; it is
/// deliberately distinct from (and much smaller than) the 1 h
/// [`INTERVAL_MARGIN_NS`] lateness allowance that legacy `HH-MM-SS-*` names
/// still require.
pub(crate) const EXACT_BOUNDS_SKEW_NS: i64 = 5 * 1_000_000_000;

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

/// One store file retained by the inventory: its path plus the interval
/// parsed from that path at refresh time (task 2, FR1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileEntry {
    /// Absolute path of the Parquet file, as walked by `build_providers`.
    pub(crate) path: PathBuf,
    /// Conservative event-time bounds parsed from [`Self::path`].
    pub(crate) interval: FileInterval,
}

/// A query's time window `[lo_ns, hi_ns]` (closed, UTC epoch ns), threaded
/// from the handlers into file pruning (and, task 4, cache classification).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryScope {
    /// Window start (inclusive).
    pub lo_ns: i64,
    /// Window end (inclusive).
    pub hi_ns: i64,
}

/// Per-table file inventory, retained at refresh from the **same**
/// `build_providers` walk that backs the registered tables (per-query
/// file-pruning ADR invariant: the two derive from one walk — a refresh
/// replaces both or neither, so they cannot diverge).
#[derive(Debug, Default)]
pub struct FileInventory {
    /// Registered table name → its surviving files with parsed intervals.
    tables: HashMap<String, Vec<FileEntry>>,
}

impl FileInventory {
    /// Record `files` (already walked + supersession-resolved) as the
    /// inventory of table `name`, parsing each path's interval once here.
    pub(crate) fn insert_table(&mut self, name: impl Into<String>, files: &[PathBuf]) {
        let entries = files
            .iter()
            .map(|path| FileEntry {
                path: path.clone(),
                interval: parse_file_interval(path),
            })
            .collect();
        self.tables.insert(name.into(), entries);
    }

    /// The paths of `table`'s files whose interval overlaps `scope` widened by
    /// [`INTERVAL_MARGIN_NS`] — the ADR's **superset guarantee**: every file
    /// that could hold an in-window row (including unparseable → unbounded
    /// ones) is returned; only files provably out of window are skipped.
    ///
    /// `None` ⇔ `table` is unknown to the inventory; the caller falls back to
    /// the registered full table.
    pub(crate) fn scoped_files(&self, table: &str, scope: QueryScope) -> Option<Vec<PathBuf>> {
        let entries = self.tables.get(table)?;
        Some(
            entries
                .iter()
                .filter(|e| {
                    e.interval
                        .overlaps(scope.lo_ns, scope.hi_ns, INTERVAL_MARGIN_NS)
                })
                .map(|e| e.path.clone())
                .collect(),
        )
    }
}

/// Parse a store file path into its [`FileInterval`].
///
/// Interval rules (ADR A′, ratified):
/// - exact-bounds `<min_ns>-<max_ns>-<uuid>[-<i>].parquet` (task 1b, written
///   by the gateway Parquet batch sink) → `[min, max + skew]` — see
///   [`parse_exact_bounds`];
/// - `dt=YYYY-MM-DD` ancestor dir → base interval `[day_start, day_end)`;
/// - legacy raw `HH-MM-SS-<uuid>[-<i>].parquet` → `[day_start, flush + margin]`;
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
    // Exact name-carried bounds are self-describing: no dt= day context
    // needed, and they take precedence over every conservative rule.
    if let Some(exact) = parse_exact_bounds(name) {
        return Some(exact);
    }
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

    // LEGACY raw gateway file `HH-MM-SS-<uuid>[-<i>].parquet` (pre-cutover;
    // since task 1b the Parquet batch sink writes exact-bounds names instead —
    // this rule survives only for files written before the store wipe).
    //
    // Verified (task 1 acceptance criterion 2): the sink's `%H-%M-%S` path
    // template stamps **event time**, not write time — `Template::render` →
    // `render_timestamp` (`src/template.rs:579-594`) formats the event's own
    // timestamp (log timestamp / `Metric::timestamp()` / trace
    // `time_unix_nano`), falling back to `Utc::now()` only when the event has
    // none; the Parquet batch sink renders the path from the **first event of
    // the flushed batch** (`src/sinks/file/mod.rs`, `flush_batch`).
    // So the name's stamp is the first batched event's event time. Later
    // events in the same batch can post-date it by at most the batch flush
    // window (~30 s cadence, ≪ the 1 h margin), so `hi = stamp + margin`
    // holds for in-order traffic; the residual risk — a batch whose *first*
    // event is stamped far in the past while later events are current — is
    // exactly what the exact-bounds naming ([`parse_exact_bounds`], ADR A′)
    // eliminates for newly written files. The lower bound stays `day_start`:
    // a batch may carry late events from earlier in the day.
    let flush_ns = parse_flush_ns(name, date)?;
    Some(FileInterval {
        lo_ns: day_lo,
        hi_ns: flush_ns.saturating_add(INTERVAL_MARGIN_NS),
    })
}

/// Fewest decimal digits a name-carried epoch-ns bound may have (10 digits =
/// 1 s past the epoch; any modern stamp has 19). This is what disambiguates
/// the exact-bounds shape from legacy `HH-MM-SS-<uuid>` raw names, whose
/// leading fields have exactly 2 digits. The sink honours the same floor
/// (`EXACT_BOUNDS_MIN_NS` in `src/sinks/file/mod.rs`) so every name it stamps
/// round-trips through this parser.
const EXACT_BOUNDS_MIN_DIGITS: usize = 10;

/// Exact name-carried bounds `<min_ns>-<max_ns>-<uuid>[-<i>].parquet`
/// (task 1b, ADR A′): the gateway Parquet batch sink stamps each file with
/// the true min/max `time_unix_nano` of its rows
/// (`src/sinks/file/mod.rs::parquet_bounds_path`), so the interval is
/// `[min, max + skew]` — exact bounds need no lateness allowance, only the
/// small [`EXACT_BOUNDS_SKEW_NS`] clock-skew cushion.
///
/// `None` (→ fall through to the conservative rules) unless the name is
/// `.parquet`, both bounds are plain decimals of at least
/// [`EXACT_BOUNDS_MIN_DIGITS`] digits, `min ≤ max`, and a uniqueness token
/// follows the bounds.
fn parse_exact_bounds(name: &str) -> Option<FileInterval> {
    let stem = name.strip_suffix(".parquet")?;
    let mut parts = stem.split('-');
    let min_ns = parse_bound(parts.next()?)?;
    let max_ns = parse_bound(parts.next()?)?;
    if min_ns > max_ns {
        return None;
    }
    // The sink always appends a uniqueness token after the bounds.
    parts.next()?;
    Some(FileInterval {
        lo_ns: min_ns,
        hi_ns: max_ns.saturating_add(EXACT_BOUNDS_SKEW_NS),
    })
}

/// One decimal epoch-ns bound of the exact shape; `None` when the field is
/// too short to be one (e.g. a legacy 2-digit `HH`) or not a plain number.
fn parse_bound(field: &str) -> Option<i64> {
    if field.len() < EXACT_BOUNDS_MIN_DIGITS {
        return None;
    }
    field.parse().ok()
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
    fn test_interval_exact_bounds_name() {
        // Task 1b (ADR A′): the sink names each Parquet batch file with the
        // batch's exact min/max time_unix_nano — the parser returns
        // [min, max + skew], no lateness margin.
        let min = JUL10_NS + 3 * HOUR_NS;
        let max = min + 30 * NS_PER_SEC;
        let got = interval(&format!(
            "/store/metrics/gauge/dt=2026-07-10/{min}-{max}-550e8400-e29b-41d4-a716-446655440000.parquet",
        ));
        assert_eq!(got.lo_ns, min);
        assert_eq!(got.hi_ns, max + EXACT_BOUNDS_SKEW_NS);

        // Multi-file batch suffix `-<i>` parses the same way.
        let multi = interval(&format!(
            "/store/logs/dt=2026-07-10/{min}-{max}-550e8400-e29b-41d4-a716-446655440000-3.parquet",
        ));
        assert_eq!(multi.lo_ns, min);
        assert_eq!(multi.hi_ns, max + EXACT_BOUNDS_SKEW_NS);

        // A single-point batch (min == max) is valid.
        let point = interval(&format!(
            "/store/traces/dt=2026-07-10/{min}-{min}-550e8400-e29b-41d4-a716-446655440000.parquet",
        ));
        assert_eq!(point.lo_ns, min);
        assert_eq!(point.hi_ns, min + EXACT_BOUNDS_SKEW_NS);

        // The name is self-describing — no dt= day context needed.
        let no_dt = interval(&format!(
            "/anywhere/{min}-{max}-550e8400-e29b-41d4-a716-446655440000.parquet",
        ));
        assert_eq!(no_dt.lo_ns, min);
        assert_eq!(no_dt.hi_ns, max + EXACT_BOUNDS_SKEW_NS);

        // Corrupt bounds (min > max) are NOT exact bounds → safety fallback
        // (unbounded: neither a valid raw stamp nor a compacted shape).
        assert_eq!(
            interval(&format!(
                "/store/logs/dt=2026-07-10/{max}-{min}-550e8400-e29b-41d4-a716-446655440000.parquet",
            )),
            FileInterval::UNBOUNDED
        );
        // A missing uniqueness token after the bounds is not the sink's shape.
        assert_eq!(
            interval(&format!("/store/logs/dt=2026-07-10/{min}-{max}.parquet")),
            FileInterval::UNBOUNDED
        );
        // Legacy raw names keep task 1's conservative rule (regression pin;
        // full coverage in test_interval_raw_file_bounds).
        let legacy = interval(
            "/store/metrics/gauge/dt=2026-07-10/12-18-28-550e8400-e29b-41d4-a716-446655440000.parquet",
        );
        assert_eq!(legacy.lo_ns, JUL10_NS);
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
