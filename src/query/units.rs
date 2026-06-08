// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Canonical time/duration units for the query backend.
//!
//! Per [ADR canonical-nanoseconds](../../docs/workspace/expr-lowering/adrs/canonical-nanoseconds.md):
//! internal time and duration are **nanoseconds `i64`**, wrapped in [`TimeNs`] /
//! [`DurationNs`] so the core cannot mix sec/ms/ns. Conversions live **only** at
//! the HTTP boundary — ingress param parsing (sec→ns) and egress response
//! serialization (ns→sec for Prometheus). Sample values stay `f64`.

/// A point in time: nanoseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimeNs(pub i64);

/// A duration in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurationNs(pub i64);

impl TimeNs {
    /// Epoch (lower bound for "all time" queries).
    pub const MIN: TimeNs = TimeNs(0);
    /// Upper bound ("now"/latest) sentinel.
    pub const MAX: TimeNs = TimeNs(i64::MAX);

    /// The underlying nanosecond count.
    #[must_use]
    pub const fn ns(self) -> i64 {
        self.0
    }

    /// Egress (Prometheus): nanoseconds → fractional Unix seconds.
    #[must_use]
    #[allow(clippy::cast_precision_loss)] // ns→s; sub-ms precision is irrelevant on the wire
    pub fn as_unix_secs(self) -> f64 {
        self.0 as f64 / 1e9
    }

    /// Ingress (Prometheus/Tempo): fractional Unix seconds → nanoseconds.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // unix-seconds*1e9 fits i64 well past year 2200
    pub fn from_unix_secs(secs: f64) -> TimeNs {
        TimeNs((secs * 1e9) as i64)
    }
}

impl DurationNs {
    /// Zero duration.
    pub const ZERO: DurationNs = DurationNs(0);

    /// The underlying nanosecond count.
    #[must_use]
    pub const fn ns(self) -> i64 {
        self.0
    }

    /// Duration as fractional seconds (for `RANGE`/`step` math at the boundary).
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1e9
    }

    /// Ingress: fractional seconds → duration ns (Prometheus/Loki `step`).
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn from_secs_f64(secs: f64) -> DurationNs {
        DurationNs((secs * 1e9) as i64)
    }
}

/// Nanosecond multiplier for a duration unit suffix (`ns us µs ms s m h d w`).
fn unit_ns(unit: &str) -> Option<f64> {
    Some(match unit {
        "ns" => 1.0,
        "us" | "µs" => 1e3,
        "ms" => 1e6,
        "s" => 1e9,
        "m" => 60e9,
        "h" => 3_600e9,
        "d" => 86_400e9,
        "w" => 604_800e9,
        _ => return None,
    })
}

/// Parse a duration literal shared by PromQL `[5m]`, TraceQL `1.5s`, and LogQL
/// `[5m]`/`offset` — the single duration parser ([FR7](../../docs/workspace/expr-lowering/DESIGN.md#fr7)).
///
/// Accepts a fractional magnitude (`1.5s`) and compound forms (`1h30m`); units
/// `ns us µs ms s m h d w`. Returns `None` on malformed input (never panics).
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn parse_duration_ns(s: &str) -> Option<DurationNs> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut total = 0.0_f64;
    let mut rest = s;
    let mut matched_any = false;
    while !rest.is_empty() {
        // magnitude: digits with an optional single decimal point
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if num_end == 0 {
            return None;
        }
        let mag: f64 = rest[..num_end].parse().ok()?;
        rest = &rest[num_end..];
        // unit: the following ascii-alphabetic run (plus the µ micro sign)
        let unit_end = rest
            .find(|c: char| !(c.is_ascii_alphabetic() || c == 'µ'))
            .unwrap_or(rest.len());
        if unit_end == 0 {
            return None; // a magnitude with no unit
        }
        total += mag * unit_ns(&rest[..unit_end])?;
        rest = &rest[unit_end..];
        matched_any = true;
    }
    if !matched_any {
        return None;
    }
    Some(DurationNs(total as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_simple_units() {
        assert_eq!(parse_duration_ns("5m"), Some(DurationNs(300_000_000_000)));
        assert_eq!(parse_duration_ns("200ms"), Some(DurationNs(200_000_000)));
        assert_eq!(parse_duration_ns("90s"), Some(DurationNs(90_000_000_000)));
        assert_eq!(parse_duration_ns("1h"), Some(DurationNs(3_600_000_000_000)));
        assert_eq!(parse_duration_ns("500ns"), Some(DurationNs(500)));
    }

    #[test]
    fn test_parse_duration_fractional_and_compound() {
        // TraceQL fractional duration
        assert_eq!(parse_duration_ns("1.5s"), Some(DurationNs(1_500_000_000)));
        // Prometheus/LogQL compound
        assert_eq!(
            parse_duration_ns("1h30m"),
            Some(DurationNs(5_400_000_000_000))
        );
        assert_eq!(
            parse_duration_ns("2h45m30s"),
            Some(DurationNs(9_930_000_000_000))
        );
    }

    #[test]
    fn test_parse_duration_rejects_malformed() {
        assert_eq!(parse_duration_ns(""), None);
        assert_eq!(parse_duration_ns("abc"), None);
        assert_eq!(parse_duration_ns("5"), None); // no unit
        assert_eq!(parse_duration_ns("5x"), None); // bad unit
        assert_eq!(parse_duration_ns("m5"), None);
    }

    #[test]
    fn test_time_ns_boundary_conversions_roundtrip() {
        // ingress sec→ns, egress ns→sec
        assert_eq!(TimeNs::from_unix_secs(1.5).ns(), 1_500_000_000);
        assert!((TimeNs(1_500_000_000).as_unix_secs() - 1.5).abs() < 1e-9);
        // whole seconds are exact
        assert_eq!(
            TimeNs::from_unix_secs(1_700_000_000.0).ns(),
            1_700_000_000_000_000_000
        );
    }
}
