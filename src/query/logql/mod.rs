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

use ast::Selector;

/// A LogQL parse failure. Surfaced as HTTP 400 by the route handlers (FR7).
#[derive(Debug)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseError {}

/// Parse a LogQL stream selector (`{ … }`) into a [`Selector`]. Never panics on
/// any input (NFR3); malformed input yields a [`ParseError`].
///
/// Task 1 scope: the stream selector only. Pipeline + metric queries follow.
pub fn parse_selector(input: &str) -> Result<Selector, ParseError> {
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
        Some(Ok(sel)) => Ok(sel),
        _ => Err(ParseError("invalid LogQL selector".to_string())),
    }
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
}
