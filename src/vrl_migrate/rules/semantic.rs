use std::sync::LazyLock;

use regex::Regex;

use super::{RewriteResult, Rule, RuleId, replace_outside_strings};

/// Semantic rules run AFTER structural Pass 1.
/// At this point, `.message` has already been rewritten to `.body` by LOG-01.
/// These rules handle `.` (root) usage patterns that may still exist.
pub static RULES: [Rule; 7] = [
    Rule {
        id: RuleId::Sem03,
        apply: apply_sem03,
    },
    Rule {
        id: RuleId::Sem02,
        apply: apply_sem02,
    },
    Rule {
        id: RuleId::Sem04,
        apply: apply_sem04,
    },
    Rule {
        id: RuleId::Sem05,
        apply: apply_sem05,
    },
    Rule {
        id: RuleId::Sem06,
        apply: apply_sem06,
    },
    Rule {
        id: RuleId::Sem07,
        apply: apply_sem07,
    },
    Rule {
        id: RuleId::Sem01,
        apply: apply_sem01,
    },
];

// SEM-01: get_field!(., "message") → string!(.)
// After LOG-01, the pattern is get_field!(., ".") or similar.
// We match the original pattern since it may not have been in the LOG-01 rewrite scope.
static RE_GET_FIELD_MESSAGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"get_field!\(\s*\.\s*,\s*"message"\s*\)"#).unwrap());

fn apply_sem01(line: &str) -> RewriteResult {
    match replace_outside_strings(line, &RE_GET_FIELD_MESSAGE, "string!(.)") {
        Some(new) => RewriteResult::Rewritten(
            new,
            r#"get_field!(., "message") → string!(.) [SEM-01]"#.into(),
        ),
        None => RewriteResult::NoMatch,
    }
}

// SEM-02: exists(.) → true (root always exists)
// Note: .message is now rewritten to .body by LOG-01, so exists(.body) is a normal check.
static RE_EXISTS_ROOT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"exists\(\s*\.\s*\)").unwrap());

fn apply_sem02(line: &str) -> RewriteResult {
    if let Some(new) = replace_outside_strings(line, &RE_EXISTS_ROOT, "true") {
        return RewriteResult::Rewritten(
            new,
            "exists(.) → true (root always exists) [SEM-02]".into(),
        );
    }
    RewriteResult::NoMatch
}

// SEM-03: del(.) → REVIEW (deletes entire event)
// Note: del(.body) is valid (clears log body) — only del(.) (root) is suspicious.
static RE_DEL_ROOT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"del\(\s*\.\s*\)").unwrap());

fn apply_sem03(line: &str) -> RewriteResult {
    if RE_DEL_ROOT.is_match(line)
        && !super::in_string_or_comment(line, RE_DEL_ROOT.find(line).unwrap().start())
    {
        return RewriteResult::NeedsReview(
            "del(.) would delete the entire event — did you mean del(.body)? [SEM-03]".into(),
        );
    }
    RewriteResult::NoMatch
}

// SEM-04: encode_json(.message) → encode_json(.)
// After LOG-01 this is already encode_json(.), but if the original wasn't caught, handle it.
static RE_ENCODE_JSON_MESSAGE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"encode_json\(\s*\.message\s*\)").unwrap());

fn apply_sem04(line: &str) -> RewriteResult {
    match replace_outside_strings(line, &RE_ENCODE_JSON_MESSAGE, "encode_json(.)") {
        Some(new) => RewriteResult::Rewritten(
            new,
            "encode_json(.message) → encode_json(.) [SEM-04]".into(),
        ),
        None => RewriteResult::NoMatch,
    }
}

// SEM-05: parse_json(.) → parse_json(string!(.))
// Root needs string! wrapping. parse_json(.body) is fine as-is.
static RE_PARSE_JSON_ROOT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"parse_json\(\s*\.\s*\)").unwrap());

fn apply_sem05(line: &str) -> RewriteResult {
    if let Some(new) = replace_outside_strings(line, &RE_PARSE_JSON_ROOT, "parse_json(string!(.))")
    {
        return RewriteResult::Rewritten(
            new,
            "parse_json(.) → parse_json(string!(.)) [SEM-05]".into(),
        );
    }
    RewriteResult::NoMatch
}

// SEM-06: No-op. After LOG-01, .message → .body. is_string(.body) is valid as-is.
const fn apply_sem06(_line: &str) -> RewriteResult {
    RewriteResult::NoMatch
}

// SEM-07: assert_eq!(., ...) → assert_eq!(string!(.), ...)
// Only root needs string! wrapping. assert_eq!(.body, ...) is fine as-is.
static RE_ASSERT_EQ_ROOT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"assert_eq!\(\s*\.\s*,").unwrap());

fn apply_sem07(line: &str) -> RewriteResult {
    if let Some(new) = replace_outside_strings(line, &RE_ASSERT_EQ_ROOT, "assert_eq!(string!(.),") {
        return RewriteResult::Rewritten(
            new,
            "assert_eq!(., ...) → assert_eq!(string!(.), ...) [SEM-07]".into(),
        );
    }
    RewriteResult::NoMatch
}
