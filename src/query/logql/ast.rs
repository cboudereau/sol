// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! LogQL AST. Node names mirror Loki's `pkg/logql/syntax` so the grammar and
//! this AST stay diffable against upstream (see
//! [QUERY-PARSING.md](../../../docs/workspace/parquet-backend/QUERY-PARSING.md)).
//!
//! Ported from `grafana/loki` `pkg/logql/syntax/expr.y`. Grown per task: this
//! file currently models the stream selector (Task 1); pipeline stages and
//! `SampleExpr` follow.

/// Label-matcher operator (`=`, `!=`, `=~`, `!~`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchOp {
    /// `=`
    Eq,
    /// `!=`
    Neq,
    /// `=~` (regex)
    Re,
    /// `!~` (negated regex)
    Nre,
}

/// A single `name op "value"` stream-selector matcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    /// Label name (left-hand side).
    pub name: String,
    /// Matcher operator.
    pub op: MatchOp,
    /// Unquoted, unescaped value.
    pub value: String,
}

/// A `{ … }` stream selector.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Selector {
    /// The label matchers inside the braces (possibly empty for `{}`).
    pub matchers: Vec<LabelMatcher>,
}
