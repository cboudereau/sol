// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Canonical, groupable series-key string for a data point's label set.
//!
//! Single source of truth shared by the write path (the Parquet codec
//! materializes a `prom_series_key` column from this) and the read path (the
//! querier's `prom_series_key` UDF / stored-column grouping), so the two cannot
//! diverge. The key is the sorted, escaped raw `k=v` join of the attribute
//! entries — raw keys keep it injective; normalization for display happens in
//! the materialization path. An empty entry set yields the empty string.

/// Build the canonical series key from raw `(key, value)` attribute entries.
/// Entries are sorted, each `k`/`v` is backslash-escaped (`\`, `=`, `\x1f`), a
/// `=` joins each pair, and `\x1f` joins the pairs (matching the `GroupKey`
/// escaping scheme). Order-independent (sorted) and injective on the raw entries.
#[must_use]
pub fn series_key(mut entries: Vec<(String, String)>) -> String {
    entries.sort();
    let mut out = String::new();
    for (k, v) in entries {
        if !out.is_empty() {
            out.push('\u{1f}');
        }
        push_escaped(&mut out, &k);
        out.push('=');
        push_escaped(&mut out, &v);
    }
    out
}

/// Append `s` escaping `\`, `=`, `\x1f` so the `k=v\x1f…` encoding is unambiguous.
fn push_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        if c == '\\' || c == '=' || c == '\u{1f}' {
            out.push('\\');
        }
        out.push(c);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_entries_yield_empty_key() {
        assert_eq!(series_key(vec![]), "");
    }

    #[test]
    fn sorted_and_injective() {
        let a = series_key(vec![
            ("cpu".to_string(), "0".to_string()),
            ("mode".to_string(), "idle".to_string()),
        ]);
        let b = series_key(vec![
            ("mode".to_string(), "idle".to_string()),
            ("cpu".to_string(), "0".to_string()),
        ]);
        assert_eq!(a, b, "order-independent");
        assert!(a.contains("cpu=0"));
        assert_eq!(a, "cpu=0\u{1f}mode=idle");
    }

    #[test]
    fn escapes_delimiters() {
        let k = series_key(vec![("a=b".to_string(), "c\u{1f}d".to_string())]);
        assert_eq!(k, "a\\=b=c\\\u{1f}d");
    }
}
