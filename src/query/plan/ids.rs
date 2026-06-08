// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! P9 — binary-id encoding for trace/span ids: `encode(col, 'hex'|'base64')` as
//! an `Expr`, replacing the SQL `encode(_, '…')` projections.

use datafusion::functions::encoding::encode;
use datafusion::prelude::{lit, Expr};

/// `encode(e, fmt)` where `fmt` is `"hex"` or `"base64"`.
#[must_use]
pub fn encode_as(e: Expr, fmt: &str) -> Expr {
    encode().call(vec![e, lit(fmt)])
}
