use regex::Regex;
use serde::Deserialize;

use super::RuleId;
use super::structural::replace_field;

struct FieldMapping {
    rule_id: RuleId,
    custom_name: String,
    canonical_path: &'static str,
    annotation: String,
}

const DEFAULT_NAMES: &[&str] = &["body", "time_unix_nano", "host", "source_type", "metadata"];
const STRUCTURAL_DEFAULTS: &[&str] = &["message", "timestamp", "host", "source_type"];

/// Minimal log_schema representation for the VRL migration tool.
/// This replaces the dependency on `sol_lib::config::LogSchema`.
#[derive(Deserialize, Default, Clone, Debug)]
#[serde(default)]
pub struct MigrateLogSchema {
    #[serde(default = "default_message_key")]
    pub message_key: String,
    #[serde(default = "default_timestamp_key")]
    pub timestamp_key: String,
    #[serde(default = "default_host_key")]
    pub host_key: String,
    #[serde(default = "default_source_type_key")]
    pub source_type_key: String,
    #[serde(default = "default_metadata_key")]
    pub metadata_key: String,
}

fn default_message_key() -> String {
    "body".into()
}
fn default_timestamp_key() -> String {
    "time_unix_nano".into()
}
fn default_host_key() -> String {
    "host".into()
}
fn default_source_type_key() -> String {
    "source_type".into()
}
fn default_metadata_key() -> String {
    "metadata".into()
}

impl MigrateLogSchema {
    pub fn non_default_fields(&self) -> Vec<(&'static str, String, &'static str)> {
        let mut out = Vec::new();

        if self.message_key != "body" {
            out.push(("message_key", self.message_key.clone(), ".body"));
        }
        if self.timestamp_key != "time_unix_nano" {
            out.push((
                "timestamp_key",
                self.timestamp_key.clone(),
                ".time_unix_nano",
            ));
        }
        if self.host_key != "host" {
            out.push((
                "host_key",
                self.host_key.clone(),
                r#".resource.attributes."host.name""#,
            ));
        }
        if self.source_type_key != "source_type" {
            out.push((
                "source_type_key",
                self.source_type_key.clone(),
                r#".attributes."pipeline.source_type""#,
            ));
        }
        if self.metadata_key != "metadata" {
            out.push(("metadata_key", self.metadata_key.clone(), ""));
        }

        out
    }
}

pub struct LogSchemaRules {
    mappings: Vec<FieldMapping>,
}

impl LogSchemaRules {
    pub fn from_schema(schema: &MigrateLogSchema) -> Self {
        let mut mappings = Vec::new();

        for (field_name, custom_value, canonical_path) in schema.non_default_fields() {
            if custom_value.is_empty() {
                continue;
            }
            if DEFAULT_NAMES.contains(&custom_value.as_str()) {
                continue;
            }
            if STRUCTURAL_DEFAULTS.contains(&custom_value.as_str()) {
                continue;
            }

            let rule_id = match field_name {
                "message_key" => RuleId::Ls01,
                "timestamp_key" => RuleId::Ls02,
                "host_key" => RuleId::Ls03,
                "source_type_key" => RuleId::Ls04,
                "metadata_key" => RuleId::Ls05,
                _ => continue,
            };

            let annotation = if canonical_path.is_empty() {
                format!(
                    ".{custom_value} → REVIEW (metadata_key has no standard OTLP mapping) [{rule_id}]"
                )
            } else {
                format!(".{custom_value} → {canonical_path} [{rule_id}]")
            };

            mappings.push(FieldMapping {
                rule_id,
                custom_name: custom_value,
                canonical_path,
                annotation,
            });
        }

        Self { mappings }
    }

    pub fn apply_to_output(&self, output: &mut super::super::MigrationOutput) {
        if self.mappings.is_empty() {
            return;
        }

        let source = std::mem::take(&mut output.text);
        let lines: Vec<&str> = source.lines().collect();
        let mut result_lines: Vec<String> = Vec::with_capacity(lines.len());

        for (line_idx, line) in lines.iter().enumerate() {
            let mut current = line.to_string();
            let mut annotations: Vec<String> = Vec::new();

            for mapping in &self.mappings {
                let pattern = format!(r"\.{}\b", regex::escape(&mapping.custom_name));
                let re = Regex::new(&pattern).unwrap();

                if mapping.canonical_path.is_empty() {
                    for m in re.find_iter(&current) {
                        if !super::in_string_or_comment(&current, m.start()) {
                            output.reviews.push(super::super::ReviewItem {
                                rule_id: mapping.rule_id,
                                line: line_idx + 1,
                                reason: mapping.annotation.clone(),
                            });
                            annotations.push(format!("# REVIEW: {}", mapping.annotation));
                            break;
                        }
                    }
                } else if let Some(new_text) = replace_field(&current, &re, mapping.canonical_path)
                {
                    output.applied.push(super::super::AppliedRule {
                        rule_id: mapping.rule_id,
                        line: line_idx + 1,
                        original: current.clone(),
                        rewritten: new_text.clone(),
                    });
                    current = new_text;
                    annotations.push(format!("# MIGRATED: {}", mapping.annotation));
                }
            }

            if annotations.is_empty() {
                result_lines.push(current);
            } else {
                let indent = super::super::leading_whitespace(&current);
                for ann in &annotations {
                    result_lines.push(format!("{indent}{ann}"));
                }
                result_lines.push(current);
            }
        }

        output.text = result_lines.join("\n");
        if output.text.is_empty() && !lines.is_empty() {
            output.text.push('\n');
        }
    }
}
