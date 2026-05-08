use std::sync::LazyLock;

use regex::Regex;

use super::{RewriteResult, Rule, RuleId};

pub static RULES: [Rule; 10] = [
    Rule {
        id: RuleId::Log07,
        apply: apply_log07,
    },
    Rule {
        id: RuleId::Log04,
        apply: apply_log04,
    },
    Rule {
        id: RuleId::Log03,
        apply: apply_log03,
    },
    Rule {
        id: RuleId::Log06,
        apply: apply_log06,
    },
    Rule {
        id: RuleId::Log05,
        apply: apply_log05,
    },
    Rule {
        id: RuleId::Log02,
        apply: apply_log02,
    },
    Rule {
        id: RuleId::Log01,
        apply: apply_log01,
    },
    Rule {
        id: RuleId::Meta01,
        apply: apply_meta01,
    },
    Rule {
        id: RuleId::Meta02,
        apply: apply_meta02,
    },
    Rule {
        id: RuleId::Trc01,
        apply: apply_trc01,
    },
];

pub(super) const fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Returns true if the character after the match continues an identifier
/// (i.e., the match is a prefix of a longer field name).
pub(super) fn followed_by_ident_or_dot(line: &str, end: usize) -> bool {
    if let Some(&b) = line.as_bytes().get(end) {
        is_ident_char(b) || b == b'.' || b == b'['
    } else {
        false
    }
}

/// Replace field references that are NOT a prefix of a longer identifier.
/// Returns true if the match is part of a metadata path (%vector.source_type).
pub(super) fn preceded_by_ident(line: &str, start: usize) -> bool {
    if start > 0 {
        let b = line.as_bytes()[start - 1];
        is_ident_char(b) || b == b'%'
    } else {
        false
    }
}

pub(super) fn replace_field(line: &str, re: &Regex, replacement: &str) -> Option<String> {
    let mut result = String::with_capacity(line.len());
    let mut last = 0;
    let mut changed = false;

    for m in re.find_iter(line) {
        if super::in_string_or_comment(line, m.start()) {
            continue;
        }
        if followed_by_ident_or_dot(line, m.end()) {
            continue;
        }
        if preceded_by_ident(line, m.start()) {
            continue;
        }
        result.push_str(&line[last..m.start()]);
        result.push_str(replacement);
        last = m.end();
        changed = true;
    }

    if changed {
        result.push_str(&line[last..]);
        Some(result)
    } else {
        None
    }
}

// LOG-01: .message → .body
static RE_MESSAGE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\.message\b").unwrap());

fn apply_log01(line: &str) -> RewriteResult {
    match replace_field(line, &RE_MESSAGE, ".body") {
        Some(new) => RewriteResult::Rewritten(new, ".message → .body [LOG-01]".into()),
        None => RewriteResult::NoMatch,
    }
}

// LOG-02: .timestamp → .time_unix_nano
static RE_TIMESTAMP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\.timestamp\b").unwrap());

fn apply_log02(line: &str) -> RewriteResult {
    match replace_field(line, &RE_TIMESTAMP, ".time_unix_nano") {
        Some(new) => RewriteResult::Rewritten(new, ".timestamp → .time_unix_nano [LOG-02]".into()),
        None => RewriteResult::NoMatch,
    }
}

// LOG-03: .source_type → .attributes."pipeline.source_type"
static RE_SOURCE_TYPE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\.source_type\b").unwrap());

fn apply_log03(line: &str) -> RewriteResult {
    match replace_field(
        line,
        &RE_SOURCE_TYPE,
        r#".attributes."pipeline.source_type""#,
    ) {
        Some(new) => RewriteResult::Rewritten(
            new,
            r#".source_type → .attributes."pipeline.source_type" [LOG-03]"#.into(),
        ),
        None => RewriteResult::NoMatch,
    }
}

// LOG-04: .host → .resource.attributes."host.name"
static RE_HOST: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\.host\b").unwrap());

fn apply_log04(line: &str) -> RewriteResult {
    match replace_field(line, &RE_HOST, r#".resource.attributes."host.name""#) {
        Some(new) => RewriteResult::Rewritten(
            new,
            r#".host → .resource.attributes."host.name" [LOG-04]"#.into(),
        ),
        None => RewriteResult::NoMatch,
    }
}

// LOG-05: .tags (standalone) → .attributes
static RE_TAGS_STANDALONE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\.tags\b").unwrap());

fn apply_log05(line: &str) -> RewriteResult {
    match replace_field(line, &RE_TAGS_STANDALONE, ".attributes") {
        Some(new) => RewriteResult::Rewritten(new, ".tags → .attributes [LOG-05]".into()),
        None => RewriteResult::NoMatch,
    }
}

// LOG-06: .tags.<key> → .attributes."<key>"
// Must run before LOG-05 to avoid double-rewriting.
static RE_TAGS_KEY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\.tags\.([a-zA-Z_][a-zA-Z0-9_]*)").unwrap());

fn apply_log06(line: &str) -> RewriteResult {
    let mut result = String::with_capacity(line.len());
    let mut last = 0;
    let mut changed = false;

    for caps in RE_TAGS_KEY.captures_iter(line) {
        let m = caps.get(0).unwrap();
        if super::in_string_or_comment(line, m.start()) {
            continue;
        }
        let key = &caps[1];
        result.push_str(&line[last..m.start()]);
        result.push_str(&format!(".attributes.\"{key}\""));
        last = m.end();
        changed = true;
    }

    if changed {
        result.push_str(&line[last..]);
        RewriteResult::Rewritten(
            result,
            ".tags.<key> → .attributes.\"<key>\" [LOG-06]".into(),
        )
    } else {
        RewriteResult::NoMatch
    }
}

// LOG-07: .level / .severity → .severity_text
static RE_LEVEL: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\.(level|severity)\b").unwrap());

fn apply_log07(line: &str) -> RewriteResult {
    match replace_field(line, &RE_LEVEL, ".severity_text") {
        Some(new) => {
            RewriteResult::Rewritten(new, ".level/.severity → .severity_text [LOG-07]".into())
        }
        None => RewriteResult::NoMatch,
    }
}

// META-01: %vector.source_type → %pipeline.source_type
static RE_META_SOURCE_TYPE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"%vector\.source_type").unwrap());

fn apply_meta01(line: &str) -> RewriteResult {
    match super::replace_outside_strings(line, &RE_META_SOURCE_TYPE, "%pipeline.source_type") {
        Some(new) => RewriteResult::Rewritten(
            new,
            "%vector.source_type → %pipeline.source_type [META-01]".into(),
        ),
        None => RewriteResult::NoMatch,
    }
}

// META-02: %vector.source_id → %pipeline.source_id
static RE_META_SOURCE_ID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"%vector\.source_id").unwrap());

fn apply_meta02(line: &str) -> RewriteResult {
    match super::replace_outside_strings(line, &RE_META_SOURCE_ID, "%pipeline.source_id") {
        Some(new) => RewriteResult::Rewritten(
            new,
            "%vector.source_id → %pipeline.source_id [META-02]".into(),
        ),
        None => RewriteResult::NoMatch,
    }
}

// TRC-01/02/03: .span_id, .trace_id, .parent_span_id — unchanged in OTel. No-op.
const fn apply_trc01(_line: &str) -> RewriteResult {
    RewriteResult::NoMatch
}
