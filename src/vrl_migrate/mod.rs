pub mod cmd;
mod rules;

use std::fmt;
use std::path::Path;

use rules::log_schema::LogSchemaRules;
pub use rules::log_schema::MigrateLogSchema;
pub use rules::{RewriteResult, Rule, RuleId};

/// Migrates a VRL program from Vector field semantics to OTel field semantics.
///
/// Applies three passes:
/// 1. Structural rewrites (LOG-*, META-*, TRC-*) — mechanical, always safe
/// 2. Semantic rewrites (SEM-*) — context-sensitive `.message`/`.` patterns
/// 3. Metric field rewrites (MET-*) — metric-specific paths
pub fn migrate(source: &str) -> MigrationOutput {
    let mut output = MigrationOutput::new(source);
    output.apply_pass_1_structural();
    output.apply_pass_2_semantic();
    output.apply_pass_3_metric();
    output
}

/// Migrates VRL with log_schema-aware rewriting (Pass 0) before standard passes.
pub fn migrate_with_log_schema(source: &str, schema: &MigrateLogSchema) -> MigrationOutput {
    let schema_rules = LogSchemaRules::from_schema(schema);
    let mut output = MigrationOutput::new(source);
    schema_rules.apply_to_output(&mut output);
    output.apply_pass_1_structural();
    output.apply_pass_2_semantic();
    output.apply_pass_3_metric();
    output
}

/// Produces a unified diff between original and migrated VRL.
pub fn diff(source: &str, path: Option<&Path>) -> String {
    let output = migrate(source);
    let label = path.map_or("input.vrl", |p| p.to_str().unwrap_or("input.vrl"));
    unified_diff(source, &output.text, label)
}

/// Produces a unified diff with log_schema-aware rewriting.
pub fn diff_with_log_schema(
    source: &str,
    path: Option<&Path>,
    schema: &MigrateLogSchema,
) -> String {
    let output = migrate_with_log_schema(source, schema);
    let label = path.map_or("input.vrl", |p| p.to_str().unwrap_or("input.vrl"));
    unified_diff(source, &output.text, label)
}

#[derive(Debug, Clone)]
pub struct MigrationOutput {
    pub text: String,
    pub applied: Vec<AppliedRule>,
    pub reviews: Vec<ReviewItem>,
}

#[derive(Debug, Clone)]
pub struct AppliedRule {
    pub rule_id: RuleId,
    pub line: usize,
    pub original: String,
    pub rewritten: String,
}

#[derive(Debug, Clone)]
pub struct ReviewItem {
    pub rule_id: RuleId,
    pub line: usize,
    pub reason: String,
}

impl MigrationOutput {
    fn new(source: &str) -> Self {
        Self {
            text: source.to_owned(),
            applied: Vec::new(),
            reviews: Vec::new(),
        }
    }

    fn apply_pass_1_structural(&mut self) {
        use rules::structural::RULES;
        self.apply_rules(&RULES);
    }

    fn apply_pass_2_semantic(&mut self) {
        use rules::semantic::RULES;
        self.apply_rules(&RULES);
    }

    fn apply_pass_3_metric(&mut self) {
        use rules::metric::RULES;
        self.apply_rules(&RULES);
    }

    fn apply_rules(&mut self, rules: &[Rule]) {
        let source = std::mem::take(&mut self.text);
        let lines: Vec<&str> = source.lines().collect();
        let mut result_lines: Vec<String> = Vec::with_capacity(lines.len());

        for (line_idx, line) in lines.iter().enumerate() {
            let mut current = line.to_string();
            let mut annotations: Vec<String> = Vec::new();

            for rule in rules {
                match rule.apply(&current) {
                    RewriteResult::NoMatch => {}
                    RewriteResult::Rewritten(new_text, annotation) => {
                        self.applied.push(AppliedRule {
                            rule_id: rule.id,
                            line: line_idx + 1,
                            original: current.clone(),
                            rewritten: new_text.clone(),
                        });
                        current = new_text;
                        annotations.push(format!("# MIGRATED: {annotation}"));
                    }
                    RewriteResult::NeedsReview(reason) => {
                        self.reviews.push(ReviewItem {
                            rule_id: rule.id,
                            line: line_idx + 1,
                            reason: reason.clone(),
                        });
                        annotations.push(format!("# REVIEW: {reason}"));
                    }
                }
            }

            if annotations.is_empty() {
                result_lines.push(current);
            } else {
                let indent = leading_whitespace(&current);
                for ann in &annotations {
                    result_lines.push(format!("{indent}{ann}"));
                }
                result_lines.push(current);
            }
        }

        self.text = result_lines.join("\n");
        if self.text.is_empty() && !lines.is_empty() {
            self.text.push('\n');
        }
    }
}

impl fmt::Display for MigrationOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.text)
    }
}

pub(crate) fn leading_whitespace(s: &str) -> &str {
    let trimmed = s.trim_start();
    &s[..s.len() - trimmed.len()]
}

fn unified_diff(original: &str, modified: &str, label: &str) -> String {
    let orig_lines: Vec<&str> = original.lines().collect();
    let mod_lines: Vec<&str> = modified.lines().collect();

    if orig_lines == mod_lines {
        return String::new();
    }

    let mut out = String::new();
    out.push_str(&format!("--- a/{label}\n"));
    out.push_str(&format!("+++ b/{label}\n"));

    let mut i = 0;
    let mut j = 0;

    while i < orig_lines.len() || j < mod_lines.len() {
        if i < orig_lines.len() && j < mod_lines.len() && orig_lines[i] == mod_lines[j] {
            i += 1;
            j += 1;
            continue;
        }

        let ctx_start_i = i.saturating_sub(3);
        let ctx_start_j = j.saturating_sub(3);
        let mut end_i = i;
        let mut end_j = j;

        while end_i < orig_lines.len() || end_j < mod_lines.len() {
            if end_i < orig_lines.len()
                && end_j < mod_lines.len()
                && orig_lines[end_i] == mod_lines[end_j]
            {
                let lookahead = (1..=3).all(|k| {
                    let oi = end_i + k;
                    let oj = end_j + k;
                    oi < orig_lines.len() && oj < mod_lines.len() && orig_lines[oi] == mod_lines[oj]
                });
                if lookahead || (end_i + 1 >= orig_lines.len() && end_j + 1 >= mod_lines.len()) {
                    break;
                }
            }
            if end_i < orig_lines.len() {
                end_i += 1;
            }
            if end_j < mod_lines.len() {
                end_j += 1;
            }
        }

        let ctx_end_i = (end_i + 3).min(orig_lines.len());
        let ctx_end_j = (end_j + 3).min(mod_lines.len());

        out.push_str(&format!(
            "@@ -{},{} +{},{} @@\n",
            ctx_start_i + 1,
            ctx_end_i - ctx_start_i,
            ctx_start_j + 1,
            ctx_end_j - ctx_start_j
        ));

        for k in ctx_start_i..i {
            out.push_str(&format!(" {}\n", orig_lines[k]));
        }
        for k in i..end_i {
            out.push_str(&format!("-{}\n", orig_lines[k]));
        }
        for k in j..end_j {
            out.push_str(&format!("+{}\n", mod_lines[k]));
        }
        for k in end_i..ctx_end_i {
            out.push_str(&format!(" {}\n", orig_lines[k]));
        }

        i = ctx_end_i;
        j = ctx_end_j;
    }

    out
}

#[cfg(test)]
mod tests;
