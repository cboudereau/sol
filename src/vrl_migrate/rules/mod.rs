pub mod log_schema;
pub mod metric;
pub mod semantic;
pub mod structural;

use regex::Regex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleId {
    Log01,
    Log02,
    Log03,
    Log04,
    Log05,
    Log06,
    Log07,
    Trc01,
    Trc02,
    Trc03,
    Meta01,
    Meta02,
    Sem01,
    Sem02,
    Sem03,
    Sem04,
    Sem05,
    Sem06,
    Sem07,
    Sem08,
    Sem09,
    Met01,
    Met02,
    Met03,
    Met04,
    Met05,
    Met06,
    Met07,
    Ls01,
    Ls02,
    Ls03,
    Ls04,
    Ls05,
}

impl std::fmt::Display for RuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Log01 => "LOG-01",
            Self::Log02 => "LOG-02",
            Self::Log03 => "LOG-03",
            Self::Log04 => "LOG-04",
            Self::Log05 => "LOG-05",
            Self::Log06 => "LOG-06",
            Self::Log07 => "LOG-07",
            Self::Trc01 => "TRC-01",
            Self::Trc02 => "TRC-02",
            Self::Trc03 => "TRC-03",
            Self::Meta01 => "META-01",
            Self::Meta02 => "META-02",
            Self::Sem01 => "SEM-01",
            Self::Sem02 => "SEM-02",
            Self::Sem03 => "SEM-03",
            Self::Sem04 => "SEM-04",
            Self::Sem05 => "SEM-05",
            Self::Sem06 => "SEM-06",
            Self::Sem07 => "SEM-07",
            Self::Sem08 => "SEM-08",
            Self::Sem09 => "SEM-09",
            Self::Met01 => "MET-01",
            Self::Met02 => "MET-02",
            Self::Met03 => "MET-03",
            Self::Met04 => "MET-04",
            Self::Met05 => "MET-05",
            Self::Met06 => "MET-06",
            Self::Met07 => "MET-07",
            Self::Ls01 => "LS-01",
            Self::Ls02 => "LS-02",
            Self::Ls03 => "LS-03",
            Self::Ls04 => "LS-04",
            Self::Ls05 => "LS-05",
        };
        f.write_str(s)
    }
}

pub enum RewriteResult {
    NoMatch,
    Rewritten(String, String),
    NeedsReview(String),
}

pub struct Rule {
    pub id: RuleId,
    pub apply: fn(&str) -> RewriteResult,
}

impl Rule {
    pub fn apply(&self, line: &str) -> RewriteResult {
        (self.apply)(line)
    }
}

/// Returns true if `pos` is inside a VRL string literal or comment in `line`.
fn in_string_or_comment(line: &str, pos: usize) -> bool {
    let bytes = line.as_bytes();
    let mut in_double = false;
    let mut in_single = false;
    let mut i = 0;
    while i < bytes.len() && i < pos {
        match bytes[i] {
            b'#' if !in_double && !in_single => return true,
            b'"' if !in_single => {
                if i > 0 && bytes[i - 1] == b'\\' {
                    // escaped
                } else {
                    in_double = !in_double;
                }
            }
            b'\'' if !in_double => {
                if i > 0 && bytes[i - 1] == b'\\' {
                    // escaped
                } else {
                    in_single = !in_single;
                }
            }
            _ => {}
        }
        i += 1;
    }
    in_double || in_single
}

/// Replace all non-overlapping matches of `re` in `line` that are not inside strings/comments.
fn replace_outside_strings(line: &str, re: &Regex, replacement: &str) -> Option<String> {
    let mut result = String::with_capacity(line.len());
    let mut last = 0;
    let mut changed = false;

    for m in re.find_iter(line) {
        if in_string_or_comment(line, m.start()) {
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
