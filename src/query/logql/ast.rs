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
    /// `| unwrap <label>` — extract a numeric value from a label (range metric).
    Unwrap(String),
}

/// Top-level LogQL expression: a log query or a metric (sample) query.
#[derive(Debug, Clone, PartialEq)]
pub enum LogQlExpr {
    /// A log query (`{ … } | …`).
    Log(LogPipeline),
    /// A metric query (`rate(…)`, `sum(…)`, `a/b`, …).
    Sample(SampleExpr),
}

/// `by (…)` / `without (…)` aggregation grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grouping {
    /// `true` for `without`, `false` for `by`.
    pub without: bool,
    /// Grouping label names.
    pub labels: Vec<String>,
}

/// A log range: a log pipeline plus the `[interval]` and optional `offset`.
/// (`| unwrap label` is carried as a [`Stage::Unwrap`] inside `pipeline`.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRange {
    /// The inner log pipeline.
    pub pipeline: LogPipeline,
    /// Raw range interval (e.g. `5m`).
    pub interval: String,
    /// Raw offset (e.g. `1h`), if any.
    pub offset: Option<String>,
}

/// Binary operator on metric expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `^`
    Pow,
    /// `==`
    Eq,
    /// `!=`
    Neq,
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// `and`
    And,
    /// `or`
    Or,
    /// `unless`
    Unless,
}

/// A LogQL metric (sample) expression.
#[derive(Debug, Clone, PartialEq)]
pub enum SampleExpr {
    /// Range aggregation over a log range (`rate`, `count_over_time`, …).
    RangeAgg {
        /// Operator name (raw, e.g. `count_over_time`).
        op: String,
        /// Optional scalar parameter (e.g. the φ of `quantile_over_time`).
        param: Option<String>,
        /// The log range argument.
        range: Box<LogRange>,
        /// Optional grouping.
        grouping: Option<Grouping>,
    },
    /// Vector aggregation over a metric expression (`sum`, `topk`, …).
    VectorAgg {
        /// Operator name (raw, e.g. `sum`).
        op: String,
        /// Optional scalar parameter (e.g. the `k` of `topk`).
        param: Option<String>,
        /// Inner metric expression.
        inner: Box<SampleExpr>,
        /// Optional grouping.
        grouping: Option<Grouping>,
    },
    /// Binary operation between two metric expressions.
    ///
    /// `bool`/`on`/`ignoring`/`group_left`/`group_right` modifiers are not yet
    /// parsed (a later slice); bare operators only.
    Binary {
        /// The operator.
        op: BinOp,
        /// Left operand.
        lhs: Box<SampleExpr>,
        /// Right operand.
        rhs: Box<SampleExpr>,
    },
    /// A scalar number literal.
    Number(f64),
}
