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

/// Clock-skew allowance for exact name-carried bounds, in nanoseconds (5 s).
///
/// Task 1b (ADR A′): the gateway Parquet batch sink stamps each file name with
/// the batch's **exact** min/max `time_unix_nano`
/// (`src/sinks/file/mod.rs::parquet_bounds_path`), so no lateness margin is
/// needed — a file provably contains nothing outside its named bounds. This
/// small constant only cushions cross-host wall-clock skew between the
/// stamping gateway and whatever clock anchors a query window. It is the only
/// lateness-style allowance left: the legacy 1 h `INTERVAL_MARGIN_NS`
/// query-time widening was deleted with the `HH-MM-SS-*` rule it served
/// (promql-plan-cache FR4, no retro-compat).
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

    /// Include-this-file test: does the query window `[lo_ns, hi_ns]` overlap
    /// this interval? No query-time widening (promql-plan-cache FR4): every
    /// remaining margin is baked into the interval at parse time
    /// ([`EXACT_BOUNDS_SKEW_NS`], the `hour_end_ns`/full-day conventions).
    pub(crate) fn overlaps(&self, lo_ns: i64, hi_ns: i64) -> bool {
        self.lo_ns <= hi_ns && lo_ns <= self.hi_ns
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

impl QueryScope {
    /// Whether this window is **entirely sealed**: `hi_ns < now_ns − 1 day`,
    /// the exact same wall-clock rule as `resolve_metric_windows`' sealed/live
    /// tier boundary ([`super::prometheus::SEALED_OFFSET_NS`]). A sealed
    /// window's data can no longer change (the gateway only appends to the
    /// trailing day; compaction/rollups preserve values), so a query result
    /// over it stays valid across catalog refreshes — the FR2 cache
    /// classification ([cache-invalidation-scope ADR](../../../docs/workspace/backend-metrics-perf/adrs/cache-invalidation-scope.md)).
    pub fn is_sealed(self, now_ns: i64) -> bool {
        self.hi_ns < now_ns.saturating_sub(super::prometheus::SEALED_OFFSET_NS)
    }
}

/// Per-table file inventory, retained at refresh from the **same**
/// `build_providers` walk that backs the registered tables (per-query
/// file-pruning ADR invariant: the two derive from one walk — a refresh
/// replaces both or neither, so they cannot diverge).
#[derive(Debug, Default)]
pub struct FileInventory {
    /// Registered table name → its surviving files with parsed intervals.
    tables: HashMap<String, Vec<FileEntry>>,
    /// Snapshot generation (promql-plan-cache task 2a): identifies the
    /// inventory **content** across swaps — [`QueryEngine::refresh`] carries
    /// the previous generation over when [`Self::same_files`] holds and bumps
    /// it otherwise, so a no-change refresh does not invalidate plan-cache
    /// keys while any real store change does.
    ///
    /// [`QueryEngine::refresh`]: super::QueryEngine::refresh
    generation: u64,
}

impl FileInventory {
    /// Snapshot generation — a component of the plan-cache key.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Stamp this snapshot's generation (set once at the refresh swap).
    pub(crate) fn set_generation(&mut self, generation: u64) {
        self.generation = generation;
    }

    /// Whether both snapshots hold exactly the same files (paths + parsed
    /// intervals) for the same tables — the "content unchanged" test deciding
    /// whether a refresh keeps or bumps the generation.
    pub(crate) fn same_files(&self, other: &FileInventory) -> bool {
        self.tables == other.tables
    }

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

    /// The paths of `table`'s files whose interval overlaps `scope` — the
    /// ADR's **superset guarantee**: every file that could hold an in-window
    /// row (including unparseable → unbounded ones) is returned; only files
    /// provably out of window are skipped. The scope is used as-is: parse-time
    /// bounds already carry every needed allowance (promql-plan-cache FR4 —
    /// no query-time widening).
    ///
    /// `None` ⇔ `table` is unknown to the inventory; the caller falls back to
    /// the registered full table.
    pub(crate) fn scoped_files(&self, table: &str, scope: QueryScope) -> Option<Vec<PathBuf>> {
        let entries = self.tables.get(table)?;
        Some(
            entries
                .iter()
                .filter(|e| e.interval.overlaps(scope.lo_ns, scope.hi_ns))
                .map(|e| e.path.clone())
                .collect(),
        )
    }
}

/// Parse a store file path into its [`FileInterval`].
///
/// Interval rules (ADR A′ + promql-plan-cache FR4 — no legacy raw rule):
/// - exact-bounds `<min_ns>-<max_ns>-<uuid>[-<i>].parquet` (task 1b, written
///   by the gateway Parquet batch sink) → `[min, max + skew]` — see
///   [`parse_exact_bounds`];
/// - `compacted-hHH-*` under a `dt=YYYY-MM-DD` dir → `[hour_start, next_hour]`
///   (the [`hour_end_ns`] convention);
/// - `compacted-<date>` / `rollup-<tier>` under a `dt=` dir → the full day;
/// - anything else — including pre-cutover `HH-MM-SS-<uuid>` raw names —
///   → [`FileInterval::UNBOUNDED`] (always included).
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
            // compacted-hHH-<date>.parquet: exactly that hour, widened only by
            // the hour_end_ns convention (the end is the start of the next
            // hour). Its inputs carry exact bounds, so no lateness margin
            // (promql-plan-cache FR4 deleted the legacy ±1 h).
            let hour: u32 = hour_part.split('-').next()?.parse().ok()?;
            let lo = date
                .and_hms_opt(hour, 0, 0)? // also validates hour < 24
                .and_utc()
                .timestamp_nanos_opt()?;
            return Some(FileInterval {
                lo_ns: lo,
                hi_ns: hour_end_ns(date, hour),
            });
        }
        // compacted-<date>.parquet: lossless merge of the whole sealed day.
        return Some(FileInterval {
            lo_ns: day_lo,
            hi_ns: day_hi,
        });
    }

    // No other shape is recognised (the pre-cutover `HH-MM-SS-<uuid>` raw rule
    // was deleted — promql-plan-cache FR4, no retro-compat): unparseable, so
    // the caller's unbounded fallback keeps the file always included.
    None
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
    let (min_ns, max_ns, _) = exact_bounds_fields(name)?;
    Some(FileInterval {
        lo_ns: min_ns,
        hi_ns: max_ns.saturating_add(EXACT_BOUNDS_SKEW_NS),
    })
}

/// The raw name-carried fields of the exact-bounds shape: the true
/// `(min_ns, max_ns)` (no skew cushion) plus the first uniqueness-token field
/// (`"chunk"` for the compactor's open-hour chunk outputs — see
/// `super::compaction::CHUNK_TOKEN`). Shared with the compactor, which groups
/// exact-bounds raws by these bounds and recognises its own chunk outputs by
/// the token. Same acceptance rules as [`parse_exact_bounds`].
pub(crate) fn exact_bounds_fields(name: &str) -> Option<(i64, i64, &str)> {
    let stem = name.strip_suffix(".parquet")?;
    let mut parts = stem.split('-');
    let min_ns = parse_bound(parts.next()?)?;
    let max_ns = parse_bound(parts.next()?)?;
    if min_ns > max_ns {
        return None;
    }
    // The sink always appends a uniqueness token after the bounds.
    let token = parts.next()?;
    Some((min_ns, max_ns, token))
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
    fn test_interval_compacted_hour() {
        // compacted-h07 → [07:00, 08:00] of that day (hour_end_ns convention:
        // the end is the start of the next hour) — no lateness margin (FR4).
        let got = interval("/store/logs/dt=2026-06-02/compacted-h07-2026-06-02.parquet");
        assert_eq!(got.lo_ns, JUN02_NS + 7 * HOUR_NS);
        assert_eq!(got.hi_ns, JUN02_NS + 8 * HOUR_NS);
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
            // HH-MM-SS-ish fields are not a recognised shape (FR4: the legacy
            // raw rule is gone), whatever their ranges.
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
            assert!(got.overlaps(0, 0), "{path}");
            assert!(got.overlaps(i64::MIN, i64::MAX), "{path}");
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
    }

    #[test]
    fn test_legacy_raw_name_falls_back_unbounded() {
        // FR4 (promql-plan-cache task 3, no retro-compat): the pre-cutover
        // `HH-MM-SS-<uuid>` raw naming has no special interval rule any more —
        // it is simply unparseable, so the total parser's unbounded fallback
        // keeps such a file always included (never a widened stamp interval).
        for path in [
            "/store/metrics/gauge/dt=2026-07-10/12-18-28-550e8400-e29b-41d4-a716-446655440000.parquet",
            "/store/logs/dt=2026-07-10/00-00-05-550e8400-e29b-41d4-a716-446655440000-3.parquet",
        ] {
            let got = interval(path);
            assert_eq!(got, FileInterval::UNBOUNDED, "{path}");
            assert!(got.overlaps(0, 0), "{path}: always included");
        }
    }

    #[test]
    fn test_interval_overlap_semantics() {
        let iv = FileInterval {
            lo_ns: 1_000,
            hi_ns: 2_000,
        };
        // Closed-interval touch counts as overlap; no query-time widening (FR4).
        assert!(iv.overlaps(2_000, 3_000));
        assert!(iv.overlaps(0, 1_000));
        assert!(iv.overlaps(1_200, 1_800)); // fully inside
        assert!(iv.overlaps(0, 5_000)); // fully covering
        assert!(!iv.overlaps(2_001, 3_000));
        assert!(!iv.overlaps(0, 999));
        // Extreme windows must not wrap.
        assert!(FileInterval::UNBOUNDED.overlaps(i64::MIN, i64::MAX));
        assert!(!iv.overlaps(i64::MIN, i64::MIN));
        assert!(!iv.overlaps(i64::MAX, i64::MAX));
    }
}
