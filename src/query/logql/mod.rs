// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! LogQL parser built on grmtools (`lrlex` + `lrpar`).
//!
//! The grammar (`logql.y`) and lexer (`logql.l`) are ports of Loki's
//! `pkg/logql/syntax/expr.y` + `lex.go`; codegen runs in `build.rs`. This keeps
//! Sol's LogQL surface diffable against upstream (ADR: parser-strategy). The AST
//! lives in [`ast`]; lowering to SQL is a separate pass ([`super::loki`]).
//!
//! ## Re-sync procedure (NFR1)
//! 1. Diff the upstream `expr.y` (pin: `grafana/loki` `pkg/logql/syntax/expr.y`).
//! 2. Mirror rule/precedence changes in `logql.y`; mirror token changes in `logql.l`.
//! 3. Re-express any new semantic actions in Rust; extend [`ast`].

pub mod ast;

// grmtools-generated lexer + parser. The generated code is not held to Sol's
// lints, so it is isolated in this module behind a broad allow.
#[allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    dead_code,
    unused_imports,
    unreachable_pub,
    non_snake_case,
    unused_parens
)]
mod grammar {
    lrlex::lrlex_mod!("query/logql/logql.l");
    lrpar::lrpar_mod!("query/logql/logql.y");
    // Re-export just the entry points (the generated modules are private).
    pub(super) use logql_l::lexerdef;
    pub(super) use logql_y::parse;
}

use ast::{LogPipeline, LogQlExpr, Selector};

/// A LogQL parse failure. Surfaced as HTTP 400 by the route handlers (FR7).
#[derive(Debug)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

/// Parse any LogQL query — a log query or a metric (sample) query — into a
/// [`LogQlExpr`]. Never panics on any input (NFR3); malformed input yields a
/// [`ParseError`].
pub fn parse(input: &str) -> Result<LogQlExpr, ParseError> {
    let lexerdef = grammar::lexerdef();
    let lexer = lexerdef.lexer(input);
    let (res, errs) = grammar::parse(&lexer);
    if !errs.is_empty() {
        return Err(ParseError(format!(
            "invalid LogQL: {} parse error(s)",
            errs.len()
        )));
    }
    match res {
        Some(Ok(expr)) => Ok(expr),
        _ => Err(ParseError("invalid LogQL query".to_string())),
    }
}

/// Parse a LogQL log query (selector + pipeline). Errors if the input is a
/// metric query.
pub fn parse_pipeline(input: &str) -> Result<LogPipeline, ParseError> {
    match parse(input)? {
        LogQlExpr::Log(p) => Ok(p),
        LogQlExpr::Sample(_) => {
            Err(ParseError("expected a log query, got a metric query".to_string()))
        }
    }
}

/// Parse just the stream selector of a LogQL log query.
pub fn parse_selector(input: &str) -> Result<Selector, ParseError> {
    parse_pipeline(input).map(|p| p.selector)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::MatchOp;

    #[test]
    fn test_parse_selector_all_match_ops() {
        let sel = parse_selector(r#"{service_name="client", a!="b", c=~"d", e!~"f"}"#).unwrap();
        assert_eq!(sel.matchers.len(), 4);
        assert_eq!(sel.matchers[0].name, "service_name");
        assert_eq!(sel.matchers[0].op, MatchOp::Eq);
        assert_eq!(sel.matchers[0].value, "client");
        assert_eq!(sel.matchers[1].op, MatchOp::Neq);
        assert_eq!(sel.matchers[2].op, MatchOp::Re);
        assert_eq!(sel.matchers[3].op, MatchOp::Nre);
    }

    #[test]
    fn test_parse_selector_unescapes_double_backslash() {
        // Grafana sends `1\\.0\\.0` on the wire → regex `1\.0\.0`.
        let sel = parse_selector(r#"{v=~"1\\.0\\.0"}"#).unwrap();
        assert_eq!(sel.matchers[0].value, r"1\.0\.0");
    }

    #[test]
    fn test_parse_empty_selector() {
        assert_eq!(parse_selector("{}").unwrap().matchers.len(), 0);
    }

    #[test]
    fn test_parse_rejects_malformed_without_panic() {
        assert!(parse_selector("{not a selector").is_err());
        assert!(parse_selector("garbage").is_err());
        assert!(parse_selector("").is_err());
        assert!(parse_selector("{=\"x\"}").is_err());
    }

    #[test]
    fn test_parse_pipeline_line_filters() {
        use ast::{LineOp, Stage};
        let p = parse_pipeline(r#"{app="a"} |= "err" != "noise" |~ "re" !~ "nre""#).unwrap();
        assert_eq!(p.selector.matchers.len(), 1);
        assert_eq!(
            p.stages,
            vec![
                Stage::Line { op: LineOp::Contains, value: "err".into() },
                Stage::Line { op: LineOp::NotContains, value: "noise".into() },
                Stage::Line { op: LineOp::Re, value: "re".into() },
                Stage::Line { op: LineOp::Nre, value: "nre".into() },
            ]
        );
    }

    #[test]
    fn test_parse_pipeline_parsers_and_formats() {
        use ast::Stage;
        let p = parse_pipeline(
            r#"{app="a"} | json | logfmt | regexp "(?P<x>.*)" | line_format "{{.x}}" | drop a, b | keep c"#,
        )
        .unwrap();
        assert_eq!(
            p.stages,
            vec![
                Stage::Json,
                Stage::Logfmt,
                Stage::Regexp("(?P<x>.*)".into()),
                Stage::LineFormat("{{.x}}".into()),
                Stage::Drop(vec!["a".into(), "b".into()]),
                Stage::Keep(vec!["c".into()]),
            ]
        );
    }

    #[test]
    fn test_parse_pipeline_label_filter_and_format() {
        use ast::{CmpOp, LabelFilter, Stage};
        let p = parse_pipeline(r#"{app="a"} | status>=500 | label_format dst=src, t="v""#).unwrap();
        assert_eq!(
            p.stages[0],
            Stage::LabelFilter(LabelFilter { name: "status".into(), op: CmpOp::Gte, value: "500".into() })
        );
        assert_eq!(
            p.stages[1],
            Stage::LabelFormat(vec![("dst".into(), "src".into()), ("t".into(), "v".into())])
        );
    }

    #[test]
    fn test_parse_pipeline_empty_backtick_line_filter() {
        use ast::{LineOp, Stage};
        // Grafana Explore sends `|= \`\`` — must parse as an empty (no-op) filter.
        let p = parse_pipeline("{app=\"a\"} |= ``").unwrap();
        assert_eq!(p.stages, vec![Stage::Line { op: LineOp::Contains, value: String::new() }]);
    }

    #[test]
    fn test_parse_range_aggregation_volume_shape() {
        use ast::{LogQlExpr, SampleExpr};
        // The demo's log-volume query.
        let e = parse(r#"sum by (level) (count_over_time({app="a"}[5m]))"#).unwrap();
        let LogQlExpr::Sample(SampleExpr::VectorAgg { op, grouping, inner, .. }) = e else {
            panic!("expected vector agg: {e:?}");
        };
        assert_eq!(op, "sum");
        let g = grouping.unwrap();
        assert!(!g.without);
        assert_eq!(g.labels, vec!["level".to_string()]);
        let SampleExpr::RangeAgg { op, range, .. } = *inner else {
            panic!("expected inner range agg");
        };
        assert_eq!(op, "count_over_time");
        assert_eq!(range.interval, "5m");
        assert_eq!(range.pipeline.selector.matchers.len(), 1);
    }

    #[test]
    fn test_parse_binary_ratio_with_precedence() {
        use ast::{BinOp, LogQlExpr, SampleExpr};
        // a / b + c  parses as  (a / b) + c
        let e = parse(
            r#"sum(rate({a="x"}[1m])) / sum(rate({b="y"}[1m])) + count_over_time({c="z"}[1m])"#,
        )
        .unwrap();
        let LogQlExpr::Sample(SampleExpr::Binary { op: BinOp::Add, lhs, .. }) = e else {
            panic!("top should be +: {e:?}");
        };
        assert!(matches!(*lhs, SampleExpr::Binary { op: BinOp::Div, .. }), "lhs should be /");
    }

    #[test]
    fn test_parse_topk_param_and_quantile_over_time() {
        use ast::{LogQlExpr, SampleExpr};
        let e = parse(r#"topk(5, sum by (x) (rate({a="b"}[1m])))"#).unwrap();
        let LogQlExpr::Sample(SampleExpr::VectorAgg { op, param, .. }) = e else {
            panic!("expected topk vector agg");
        };
        assert_eq!(op, "topk");
        assert_eq!(param.as_deref(), Some("5"));

        let q = parse(r#"quantile_over_time(0.95, {a="b"} | unwrap latency [5m])"#).unwrap();
        let LogQlExpr::Sample(SampleExpr::RangeAgg { op, param, range, .. }) = q else {
            panic!("expected quantile range agg");
        };
        assert_eq!(op, "quantile_over_time");
        assert_eq!(param.as_deref(), Some("0.95"));
        assert!(
            range.pipeline.stages.iter().any(|s| matches!(s, ast::Stage::Unwrap(l) if l == "latency")),
            "unwrap stage present: {:?}",
            range.pipeline.stages
        );
    }
}
