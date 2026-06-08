// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! P1/P2 — the shared predicate builder: *(lhs, op, value) → `Expr`*, reused by
//! PromQL matchers, LogQL label/line filters, and TraceQL field comparisons.
//!
//! Values enter as `lit(value)` — a bound literal in the plan — so no query value
//! can alter plan structure (the `esc()` injection surface disappears, FR2). Regex
//! is anchored (`^(?:…)$`) and absent labels behave like the empty string, matching
//! the previous SQL semantics (FR5 parity).

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::Expr;
use datafusion::prelude::{col, lit};

/// Comparison operator (the union over the three signals' matcher ops).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchKind {
    /// `=`
    Eq,
    /// `!=`
    Neq,
    /// `=~` (anchored regex)
    Re,
    /// `!~` (negated anchored regex)
    Nre,
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,
}

/// P2 — `prom_attr(column, key)` as an `Expr`: the normalized-attribute LHS used
/// by the Prometheus/Loki JSON columns (`attributes` / `resource_attributes`).
#[must_use]
pub fn prom_attr(column: &str, key: &str) -> Expr {
    super::super::udf::prom_attr_udf().call(vec![col(column), lit(key)])
}

/// Anchored regex predicate `regexp_like(coalesce(lhs, ''), '^(?:pat)$')`,
/// optionally negated — mirrors the SQL anchoring + null-as-empty semantics.
#[must_use]
pub fn anchored_regex(lhs: Expr, pattern: &str, negated: bool) -> Expr {
    let e = regexp_like(
        coalesce(vec![lhs, lit("")]),
        lit(format!("^(?:{pattern})$")),
        None,
    );
    if negated { !e } else { e }
}

/// P1 — build a comparison predicate. `value` is always a bound literal. For the
/// ordering ops, `numeric` casts the LHS to `Float64` (JSON/text columns) and
/// binds a numeric literal.
#[must_use]
pub fn cmp(lhs: Expr, op: MatchKind, value: &str, numeric: bool) -> Expr {
    match op {
        MatchKind::Eq if value.is_empty() => lhs.clone().is_null().or(lhs.eq(lit(""))),
        MatchKind::Eq => lhs.eq(lit(value)),
        MatchKind::Neq if value.is_empty() => lhs.clone().is_not_null().and(lhs.not_eq(lit(""))),
        MatchKind::Neq => lhs.clone().is_null().or(lhs.not_eq(lit(value))),
        MatchKind::Re => anchored_regex(lhs, value, false),
        MatchKind::Nre => anchored_regex(lhs, value, true),
        MatchKind::Gt | MatchKind::Gte | MatchKind::Lt | MatchKind::Lte => {
            let (l, r) = if numeric {
                (cast(lhs, DataType::Float64), lit(value.parse::<f64>().unwrap_or(f64::NAN)))
            } else {
                (lhs, lit(value))
            };
            match op {
                MatchKind::Gt => l.gt(r),
                MatchKind::Gte => l.gt_eq(r),
                MatchKind::Lt => l.lt(r),
                MatchKind::Lte => l.lt_eq(r),
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmp_eq_binds_literal_not_interpolated() {
        // A value with a quote / `&&` must be a bound literal, never text.
        let e = cmp(col("service_name"), MatchKind::Eq, "a'b && c", false);
        let s = format!("{e}");
        assert!(s.contains("a'b && c"), "value bound as a literal: {s}");
        assert!(s.contains("service_name"), "{s}");
    }

    #[test]
    fn test_anchored_regex_form() {
        let e = cmp(col("pod"), MatchKind::Re, "web", false);
        let s = format!("{e}");
        assert!(s.contains("^(?:web)$"), "anchored: {s}");
        let neg = cmp(col("pod"), MatchKind::Nre, "web", false);
        assert!(format!("{neg}").contains("^(?:web)$"), "{neg}");
    }

    #[test]
    fn test_eq_empty_is_absent_aware() {
        let e = cmp(col("x"), MatchKind::Eq, "", false);
        let s = format!("{e}");
        assert!(s.to_uppercase().contains("IS NULL"), "absent≡empty: {s}");
    }

    #[test]
    fn test_numeric_cmp_casts_and_binds() {
        let e = cmp(prom_attr("attributes", "status"), MatchKind::Gte, "500", true);
        let s = format!("{e}");
        assert!(s.contains("500"), "numeric literal bound: {s}");
        assert!(s.to_uppercase().contains("CAST") || s.contains("Float64"), "lhs cast: {s}");
    }

    #[test]
    fn test_prom_attr_is_udf_call() {
        let s = format!("{}", prom_attr("resource_attributes", "deployment.environment"));
        assert!(s.contains("prom_attr"), "{s}");
        assert!(s.contains("deployment.environment"), "{s}");
    }
}
