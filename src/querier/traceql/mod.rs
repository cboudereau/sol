// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! TraceQL parser built on grmtools (`lrlex` + `lrpar`).
//!
//! The grammar (`traceql.y`) and lexer (`traceql.l`) are ports of Tempo's
//! `pkg/traceql/expr.y` + `lexer.go`; codegen runs in `build.rs`. The AST lives
//! in [`ast`]; lowering to SQL is a separate pass ([`super::tempo`]).
//!
//! Scope: spanset filters `{ … }` combined by `&& || >> <<`, with field
//! expressions (comparisons, `&&`/`||`, parens, scoped attributes, intrinsics,
//! literals). Pipeline/aggregate/metrics parsing is deferred (a later slice).
//!
//! ## Re-sync procedure (NFR1)
//! Diff upstream `pkg/traceql/expr.y` (pin: `grafana/tempo`); mirror rule/
//! precedence changes in `traceql.y`, token changes in `traceql.l`.

pub mod ast;

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
    lrlex::lrlex_mod!("querier/traceql/traceql.l");
    lrpar::lrpar_mod!("querier/traceql/traceql.y");
    pub(super) use traceql_l::lexerdef;
    pub(super) use traceql_y::parse;
}

use ast::SpansetExpr;

/// A TraceQL parse failure. Surfaced as HTTP 400 by the route handlers (FR7).
#[derive(Debug)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

/// Parse a TraceQL query into a [`SpansetExpr`]. Never panics on any input
/// (NFR3); malformed input yields a [`ParseError`].
pub fn parse(input: &str) -> Result<SpansetExpr, ParseError> {
    let lexerdef = grammar::lexerdef();
    let lexer = lexerdef.lexer(input);
    let (res, errs) = grammar::parse(&lexer);
    if !errs.is_empty() {
        return Err(ParseError(format!(
            "invalid TraceQL: {} parse error(s)",
            errs.len()
        )));
    }
    match res {
        Some(Ok(expr)) => Ok(expr),
        _ => Err(ParseError("invalid TraceQL query".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::{AttrScope, Field, FieldExpr, FieldOp, SpansetExpr, SpansetOp};

    fn filter(input: &str) -> FieldExpr {
        match parse(input).unwrap() {
            SpansetExpr::Filter(Some(fe)) => fe,
            other => panic!("expected a single filter, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_bare_dotted_attr_is_unscoped() {
        // Grafana's Tempo search emits a bare `service.name` (no leading dot or
        // scope); it must parse as an unscoped attribute, not a parse error.
        let fe = filter(r#"{ service.name = "client" }"#);
        let FieldExpr::Cmp { lhs, rhs, .. } = fe else {
            panic!("expected a comparison")
        };
        assert_eq!(
            *lhs,
            Field::Attr {
                scope: AttrScope::Unscoped,
                path: "service.name".into()
            }
        );
        assert_eq!(*rhs, Field::Str("client".into()));
        // A bare single identifier stays an intrinsic.
        let fe = filter(r#"{ name = "x" }"#);
        let FieldExpr::Cmp { lhs, .. } = fe else {
            panic!("expected a comparison")
        };
        assert_eq!(*lhs, Field::Intrinsic("name".into()));
    }

    #[test]
    fn test_parse_parity_cases() {
        // The current ad-hoc surface: { a="x" && b!="y" } with intrinsics + attrs.
        let fe = filter(r#"{ name="GET" && span.http.status_code != "500" }"#);
        let FieldExpr::And(l, r) = fe else {
            panic!("expected &&: {fe:?}")
        };
        assert_eq!(
            *l,
            FieldExpr::Cmp {
                lhs: Box::new(Field::Intrinsic("name".into())),
                op: FieldOp::Eq,
                rhs: Box::new(Field::Str("GET".into())),
            }
        );
        let FieldExpr::Cmp { lhs, op, rhs } = *r else {
            panic!("rhs cmp")
        };
        assert_eq!(
            *lhs,
            Field::Attr {
                scope: AttrScope::Span,
                path: "http.status_code".into()
            }
        );
        assert_eq!(op, FieldOp::Neq);
        assert_eq!(*rhs, Field::Str("500".into()));
    }

    #[test]
    fn test_parse_empty_set() {
        assert_eq!(parse("{}").unwrap(), SpansetExpr::Filter(None));
    }

    #[test]
    fn test_parse_comparisons_and_literals() {
        let fe = filter(r#"{ duration > 1.5s }"#);
        let FieldExpr::Cmp { lhs, op, rhs } = fe else {
            panic!()
        };
        assert_eq!(*lhs, Field::Intrinsic("duration".into()));
        assert_eq!(op, FieldOp::Gt);
        assert_eq!(*rhs, Field::Duration("1.5s".into()));

        let fe = filter(r#"{ .ok = true }"#);
        let FieldExpr::Cmp { lhs, rhs, .. } = fe else {
            panic!()
        };
        assert_eq!(
            *lhs,
            Field::Attr {
                scope: AttrScope::Unscoped,
                path: "ok".into()
            }
        );
        assert_eq!(*rhs, Field::Bool(true));
    }

    #[test]
    fn test_parse_or_and_spanset_operators() {
        // field-level || inside a set
        let fe = filter(r#"{ resource.service.name="a" || name=~"GET.*" }"#);
        assert!(matches!(fe, FieldExpr::Or(_, _)), "{fe:?}");

        // spanset-level && and descendant >>
        let e = parse(r#"{ name="a" } >> { name="b" }"#).unwrap();
        let SpansetExpr::Op { op, .. } = e else {
            panic!("expected spanset op: {e:?}")
        };
        assert_eq!(op, SpansetOp::Descendant);

        let e = parse(r#"{ name="a" } && { resource.service.name="s" }"#).unwrap();
        assert!(
            matches!(
                e,
                SpansetExpr::Op {
                    op: SpansetOp::And,
                    ..
                }
            ),
            "{e:?}"
        );
    }

    #[test]
    fn test_parse_never_panics_on_adversarial_input() {
        // NFR3: no input may panic.
        let inputs = [
            "",
            "{",
            "}",
            "{}{}",
            "{ a }",
            "{ =\"b\" }",
            "{ a = }",
            "{ a == }",
            "{ name }",
            "span.",
            ".",
            "{ name=\"a\" &&",
            "<<",
            "{ a } >>",
            "(((",
            "{ a=~\"(unclosed\" }",
            "{ duration > }",
            "\u{1f600}{a=\"b\"}",
        ];
        for q in inputs {
            let _ = parse(q);
        }
    }
}
