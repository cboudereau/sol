// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! TraceQL AST. Node names mirror Tempo's `pkg/traceql` so the grammar stays
//! diffable against upstream (see
//! [QUERY-PARSING.md](../../../docs/workspace/parquet-backend/QUERY-PARSING.md)).
//!
//! Ported from `grafana/tempo` `pkg/traceql/expr.y` + `lexer.go`. Scope: spanset
//! filters and field expressions; pipeline/aggregate/metrics parsing is deferred.

/// Field comparison operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldOp {
    /// `=`
    Eq,
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

/// Attribute scope. `parent.*` is parsed but not lowered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttrScope {
    /// Bare `.attr` (resource-or-span, unscoped).
    Unscoped,
    /// `span.`
    Span,
    /// `resource.`
    Resource,
    /// `event.`
    Event,
    /// `instrumentation.`
    Instrumentation,
    /// `link.`
    Link,
    /// `parent.`
    Parent,
}

/// A field: an intrinsic, a scoped attribute, or a literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    /// Intrinsic (`name`, `duration`, `status`, `kind`, …).
    Intrinsic(String),
    /// Scoped/unscoped attribute reference.
    Attr {
        /// The scope.
        scope: AttrScope,
        /// Dotted attribute path (raw OTLP key, dots preserved).
        path: String,
    },
    /// String literal.
    Str(String),
    /// Numeric literal.
    Num(f64),
    /// Boolean literal.
    Bool(bool),
    /// Duration literal (raw, e.g. `5ms`).
    Duration(String),
}

/// A field expression inside a spanset `{ … }`.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldExpr {
    /// `lhs op rhs`.
    Cmp {
        /// Left field.
        lhs: Box<Field>,
        /// Operator.
        op: FieldOp,
        /// Right field (usually a literal).
        rhs: Box<Field>,
    },
    /// `a && b`.
    And(Box<FieldExpr>, Box<FieldExpr>),
    /// `a || b`.
    Or(Box<FieldExpr>, Box<FieldExpr>),
    /// A bare field (existence / truthiness).
    Field(Box<Field>),
}

/// Spanset-combining operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpansetOp {
    /// `&&`
    And,
    /// `||`
    Or,
    /// `>>` (descendant).
    Descendant,
    /// `<<` (ancestor).
    Ancestor,
}

/// A spanset expression: a `{ … }` filter, or two combined by a spanset op.
#[derive(Debug, Clone, PartialEq)]
pub enum SpansetExpr {
    /// `{ field-expr }` or `{}` (None = match all).
    Filter(Option<FieldExpr>),
    /// `lhs op rhs`.
    Op {
        /// Left spanset.
        lhs: Box<SpansetExpr>,
        /// Operator.
        op: SpansetOp,
        /// Right spanset.
        rhs: Box<SpansetExpr>,
    },
}
