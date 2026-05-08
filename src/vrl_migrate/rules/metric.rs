use std::sync::LazyLock;

use regex::Regex;

use super::{RewriteResult, Rule, RuleId};

pub static RULES: [Rule; 5] = [
    Rule {
        id: RuleId::Met05,
        apply: apply_met05,
    },
    Rule {
        id: RuleId::Met04,
        apply: apply_met04,
    },
    Rule {
        id: RuleId::Met02,
        apply: apply_met02,
    },
    Rule {
        id: RuleId::Met06,
        apply: apply_met06,
    },
    Rule {
        id: RuleId::Met07,
        apply: apply_met07,
    },
];

// MET-01: .name → .name (unchanged). No rewrite needed.
// MET-03: .tags → .attributes (handled by LOG-05 in structural pass).

fn followed_by_ident_or_dot(line: &str, end: usize) -> bool {
    if let Some(&b) = line.as_bytes().get(end) {
        b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'['
    } else {
        false
    }
}

fn replace_field(line: &str, re: &Regex, replacement: &str) -> Option<String> {
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

// MET-02: .namespace → .attributes."metric.namespace"
static RE_NAMESPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\.namespace\b").unwrap());

fn apply_met02(line: &str) -> RewriteResult {
    match replace_field(line, &RE_NAMESPACE, r#".attributes."metric.namespace""#) {
        Some(new) => RewriteResult::Rewritten(
            new,
            r#".namespace → .attributes."metric.namespace" [MET-02]"#.into(),
        ),
        None => RewriteResult::NoMatch,
    }
}

// MET-04: .tags.<key> → .attributes."<key>" (already handled by LOG-06). No-op.
const fn apply_met04(_line: &str) -> RewriteResult {
    RewriteResult::NoMatch
}

// MET-05: .kind → REVIEW
static RE_KIND: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\.kind\b").unwrap());

fn apply_met05(line: &str) -> RewriteResult {
    for m in RE_KIND.find_iter(line) {
        if !super::in_string_or_comment(line, m.start()) && !followed_by_ident_or_dot(line, m.end())
        {
            return RewriteResult::NeedsReview(
                ".kind maps to OTel AggregationTemporality — requires manual review [MET-05]"
                    .into(),
            );
        }
    }
    RewriteResult::NoMatch
}

// MET-06: .value.counter.value → .data.sum.data_points[0].value
static RE_COUNTER_VALUE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\.value\.counter\.value\b").unwrap());

fn apply_met06(line: &str) -> RewriteResult {
    match super::replace_outside_strings(line, &RE_COUNTER_VALUE, ".data.sum.data_points[0].value")
    {
        Some(new) => RewriteResult::Rewritten(
            new,
            ".value.counter.value → .data.sum.data_points[0].value [MET-06]".into(),
        ),
        None => RewriteResult::NoMatch,
    }
}

// MET-07: .value.gauge.value → .data.gauge.data_points[0].value
static RE_GAUGE_VALUE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\.value\.gauge\.value\b").unwrap());

fn apply_met07(line: &str) -> RewriteResult {
    match super::replace_outside_strings(line, &RE_GAUGE_VALUE, ".data.gauge.data_points[0].value")
    {
        Some(new) => RewriteResult::Rewritten(
            new,
            ".value.gauge.value → .data.gauge.data_points[0].value [MET-07]".into(),
        ),
        None => RewriteResult::NoMatch,
    }
}
