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

/// A log query: a stream selector followed by a left-to-right pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPipeline {
    /// The `{ … }` stream selector.
    pub selector: Selector,
    /// Pipeline stages applied in order.
    pub stages: Vec<Stage>,
}

/// Line-filter operator. `|=`/`!=` are substring; `|~`/`!~` are regex; `|>`/`!>`
/// are pattern filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineOp {
    /// `|=` — line contains.
    Contains,
    /// `!=` — line does not contain.
    NotContains,
    /// `|~` — line matches regex.
    Re,
    /// `!~` — line does not match regex.
    Nre,
    /// `|>` — line matches pattern.
    Pattern,
    /// `!>` — line does not match pattern.
    NotPattern,
}

/// Label-filter comparison operator (on extracted/stored labels).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CmpOp {
    /// `=`
    Eq,
    /// `==`
    EqEq,
    /// `!=`
    Neq,
    /// `=~`
    Re,
    /// `!~`
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

/// A single label-filter predicate (`name op value`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelFilter {
    /// Label name.
    pub name: String,
    /// Comparison operator.
    pub op: CmpOp,
    /// Right-hand value (string, number, or duration/bytes literal — raw).
    pub value: String,
}

/// A pipeline stage. Node set mirrors Loki's pipeline stages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stage {
    /// Line filter (`|=`, `!=`, `|~`, `!~`, `|>`, `!>`).
    Line {
        /// The line-filter operator.
        op: LineOp,
        /// The (unquoted) filter value; empty = no-op.
        value: String,
    },
    /// `| json` (no explicit field map).
    Json,
    /// `| logfmt`.
    Logfmt,
    /// `| unpack`.
    Unpack,
    /// `| decolorize`.
    Decolorize,
    /// `| regexp "…"`.
    Regexp(String),
    /// `| pattern "…"`.
    Pattern(String),
    /// `| line_format "…"`.
    LineFormat(String),
    /// `| drop a, b`.
    Drop(Vec<String>),
    /// `| keep a, b`.
    Keep(Vec<String>),
    /// `| label_format dst=src|"template", …` (rhs captured raw).
    LabelFormat(Vec<(String, String)>),
    /// `| name op value` label filter.
    LabelFilter(LabelFilter),
}
