// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Shared query-plan primitives over the DataFusion logical layer.
//!
//! Per the [expr-lowering design](../../../docs/20260608_expr-lowering/designs/20260608_expr-lowering.md),
//! the SQL surface of all three signals reduces to a small set of reusable
//! primitives built on `Expr`/`DataFrame` (no `format!` SQL). This module hosts
//! them; signal modules (`prometheus`/`loki`/`tempo`) compose them.
//!
//! - [`predicate`] — P1 (label/field comparison → `Expr`) + P2 (LHS resolver).
//!
//! Window primitives (P5/P6/P7), aggregation (P4/P8), and id encode/lookup (P9)
//! land in follow-up tasks.

pub mod frame;
pub mod ids;
pub mod predicate;
