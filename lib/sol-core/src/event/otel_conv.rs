#![deny(clippy::cast_possible_wrap)]
#![deny(clippy::cast_sign_loss)]

use chrono::{DateTime, TimeZone, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub(crate) struct OtlpTimestamp(u64);

impl OtlpTimestamp {
    pub(crate) fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    pub(crate) fn as_nanos(self) -> u64 {
        self.0
    }

    #[expect(
        clippy::cast_possible_wrap,
        reason = "nanos fit in i64 until year 2262"
    )]
    pub(crate) fn to_vrl(self) -> i64 {
        self.0 as i64
    }

    #[expect(clippy::cast_sign_loss, reason = "clamped to non-negative")]
    pub(crate) fn from_vrl(v: i64) -> Self {
        Self(v.max(0) as u64)
    }

    #[allow(dead_code)]
    #[expect(
        clippy::cast_possible_wrap,
        reason = "seconds fit in i64 until year 2262"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "modulo 10^9 < 2^30, fits u32"
    )]
    pub(crate) fn to_chrono(self) -> DateTime<Utc> {
        let secs = (self.0 / 1_000_000_000) as i64;
        let nsecs = (self.0 % 1_000_000_000) as u32;
        Utc.timestamp_opt(secs, nsecs).single().unwrap_or_default()
    }

    #[allow(dead_code)]
    #[expect(clippy::cast_sign_loss, reason = "timestamps are non-negative")]
    pub(crate) fn from_chrono(ts: DateTime<Utc>) -> Self {
        Self(ts.timestamp_nanos_opt().unwrap_or(0).max(0) as u64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct OtlpCount(u32);

impl OtlpCount {
    #[allow(dead_code)]
    pub(crate) fn from_proto(v: u32) -> Self {
        Self(v)
    }

    pub(crate) fn as_proto(self) -> u32 {
        self.0
    }

    #[allow(dead_code)]
    pub(crate) fn to_vrl(self) -> i64 {
        i64::from(self.0)
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "proto field is u32; value round-trips through i64"
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "proto field is u32; value round-trips through i64"
    )]
    pub(crate) fn from_vrl(v: i64) -> Self {
        Self(v as u32)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct OtlpEnumField(i32);

impl OtlpEnumField {
    #[allow(dead_code)]
    pub(crate) fn from_proto(v: i32) -> Self {
        Self(v)
    }

    pub(crate) fn as_proto(self) -> i32 {
        self.0
    }

    #[allow(dead_code)]
    pub(crate) fn to_vrl(self) -> i64 {
        i64::from(self.0)
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "proto field is i32; value round-trips through i64"
    )]
    pub(crate) fn from_vrl(v: i64) -> Self {
        Self(v as i32)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct OtlpMetricInt(i64);

impl OtlpMetricInt {
    pub(crate) fn from_proto(v: i64) -> Self {
        Self(v)
    }

    #[allow(dead_code)]
    pub(crate) fn as_proto(self) -> i64 {
        self.0
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "precise for |v| <= 2^53; OTLP metric values"
    )]
    pub(crate) fn to_f64(self) -> f64 {
        self.0 as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otlp_timestamp_roundtrip() {
        let nanos = 1_700_000_000_000_000_000_u64;
        let vrl = OtlpTimestamp::from_nanos(nanos).to_vrl();
        assert_eq!(OtlpTimestamp::from_vrl(vrl).as_nanos(), nanos);
    }

    #[test]
    fn test_otlp_timestamp_negative_clamps() {
        assert_eq!(OtlpTimestamp::from_vrl(-1).as_nanos(), 0);
    }

    #[test]
    fn test_otlp_timestamp_chrono_roundtrip() {
        let ts = DateTime::parse_from_rfc3339("2024-01-15T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let rt = OtlpTimestamp::from_chrono(ts).to_chrono();
        assert_eq!(rt, ts);
    }

    #[test]
    fn test_otlp_timestamp_epoch() {
        assert_eq!(OtlpTimestamp::from_nanos(0).to_vrl(), 0);
        assert_eq!(OtlpTimestamp::from_vrl(0).as_nanos(), 0);
        let epoch = DateTime::UNIX_EPOCH;
        assert_eq!(OtlpTimestamp::from_chrono(epoch).as_nanos(), 0);
        assert_eq!(OtlpTimestamp::from_nanos(0).to_chrono(), epoch);
    }

    #[test]
    fn test_otlp_timestamp_max_wraps() {
        let vrl = OtlpTimestamp::from_nanos(u64::MAX).to_vrl();
        assert!(vrl < 0);
        assert_eq!(OtlpTimestamp::from_vrl(vrl).as_nanos(), 0);
    }

    #[test]
    fn test_otlp_count_roundtrip() {
        let proto = 42_u32;
        let vrl = OtlpCount::from_proto(proto).to_vrl();
        assert_eq!(OtlpCount::from_vrl(vrl).as_proto(), proto);
    }

    #[test]
    fn test_otlp_count_truncation() {
        let truncated = OtlpCount::from_vrl(i64::MAX).as_proto();
        assert_eq!(truncated, u32::MAX);
    }

    #[test]
    fn test_otlp_enum_roundtrip() {
        let proto = 5_i32;
        let vrl = OtlpEnumField::from_proto(proto).to_vrl();
        assert_eq!(OtlpEnumField::from_vrl(vrl).as_proto(), proto);
    }

    #[test]
    fn test_otlp_enum_truncation() {
        let truncated = OtlpEnumField::from_vrl(i64::MAX).as_proto();
        assert_eq!(truncated, -1);
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "test verifies precision boundary behavior"
    )]
    #[test]
    fn test_otlp_metric_int_to_f64() {
        assert!((OtlpMetricInt::from_proto(42).to_f64() - 42.0).abs() < f64::EPSILON);
        assert!(
            (OtlpMetricInt::from_proto(1_i64 << 53).to_f64() - (1_i64 << 53) as f64).abs()
                < f64::EPSILON
        );
        let large = (1_i64 << 53) + 1;
        let converted = OtlpMetricInt::from_proto(large).to_f64();
        assert!((converted - large as f64).abs() <= 1.0);
    }
}
