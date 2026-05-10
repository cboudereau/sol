#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use serde::Deserialize;

use super::{MigrateLogSchema, diff, diff_with_log_schema, migrate, migrate_with_log_schema};

#[derive(Parser, Debug, Clone)]
#[command(rename_all = "kebab-case")]
pub struct Opts {
    /// VRL file(s) to migrate.
    #[arg(required_unless_present = "config")]
    pub files: Vec<PathBuf>,

    /// Rewrite file(s) in-place.
    #[arg(long)]
    pub in_place: bool,

    /// Show a unified diff without modifying files.
    #[arg(long)]
    pub diff: bool,

    /// Migrate all inline VRL in a Vector config file (TOML/YAML/JSON).
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Read log_schema overrides from a Vector config file to generate
    /// additional rewrite rules for custom field names.
    #[arg(long)]
    pub log_schema: Option<PathBuf>,
}

pub fn cmd(opts: &Opts) -> exitcode::ExitCode {
    if let Some(config_path) = &opts.config {
        return migrate_config(config_path, opts.diff, opts.in_place);
    }

    let schema = match resolve_log_schema(opts.log_schema.as_deref()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading log_schema: {e}");
            return exitcode::IOERR;
        }
    };

    let mut any_changed = false;

    for path in &opts.files {
        let source = match fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Error reading {}: {e}", path.display());
                return exitcode::IOERR;
            }
        };

        if opts.diff {
            let d = if let Some(ref schema) = schema {
                diff_with_log_schema(&source, Some(path), schema)
            } else {
                diff(&source, Some(path))
            };
            if !d.is_empty() {
                print!("{d}");
                any_changed = true;
            }
        } else if opts.in_place {
            let output = if let Some(ref schema) = schema {
                migrate_with_log_schema(&source, schema)
            } else {
                migrate(&source)
            };
            if output.text != source {
                if let Err(e) = fs::write(path, &output.text) {
                    eprintln!("Error writing {}: {e}", path.display());
                    return exitcode::IOERR;
                }
                any_changed = true;
                eprintln!(
                    "{}: {} rules applied, {} items need review",
                    path.display(),
                    output.applied.len(),
                    output.reviews.len()
                );
            } else {
                eprintln!("{}: no changes", path.display());
            }
        } else {
            let output = if let Some(ref schema) = schema {
                migrate_with_log_schema(&source, schema)
            } else {
                migrate(&source)
            };
            print!("{output}");
            if output.text != source {
                any_changed = true;
            }
        }
    }

    if opts.diff && !any_changed {
        eprintln!("No changes needed.");
    }

    exitcode::OK
}

fn migrate_config(path: &PathBuf, show_diff: bool, in_place: bool) -> exitcode::ExitCode {
    let source = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Error reading {}: {e}", path.display());
            return exitcode::IOERR;
        }
    };

    let schema = parse_log_schema_from_toml(&source);

    let mut output = source.clone();
    let mut total_applied = 0;
    let mut total_reviews = 0;

    // Extract inline VRL from `source` fields in TOML/YAML config.
    // Strategy: find `source = """..."""` or `source: |` blocks and migrate them.
    let re_toml_vrl = regex::Regex::new(r#"(?m)(source\s*=\s*"""\n)([\s\S]*?)(""")"#).unwrap();

    let result = re_toml_vrl.replace_all(&output, |caps: &regex::Captures| {
        let prefix = &caps[1];
        let vrl_body = &caps[2];
        let suffix = &caps[3];

        let migrated = if let Some(ref schema) = schema {
            migrate_with_log_schema(vrl_body, schema)
        } else {
            migrate(vrl_body)
        };
        total_applied += migrated.applied.len();
        total_reviews += migrated.reviews.len();

        format!("{prefix}{}{suffix}", migrated.text)
    });
    output = result.into_owned();

    // Also handle single-line source = "..." patterns
    let re_toml_inline = regex::Regex::new(r#"(?m)(source\s*=\s*")((?:[^"\\]|\\.)*)(")"#).unwrap();

    let result = re_toml_inline.replace_all(&output, |caps: &regex::Captures| {
        let prefix = &caps[1];
        let vrl_body = &caps[2];
        let suffix = &caps[3];

        let migrated = if let Some(ref schema) = schema {
            migrate_with_log_schema(vrl_body, schema)
        } else {
            migrate(vrl_body)
        };
        total_applied += migrated.applied.len();
        total_reviews += migrated.reviews.len();

        format!("{prefix}{}{suffix}", migrated.text)
    });
    output = result.into_owned();

    // Strip [log_schema] section from config if present
    if schema.is_some() {
        output = strip_log_schema_section(&output);
    }

    if show_diff {
        let d = super::unified_diff(&source, &output, path.to_str().unwrap_or("config"));
        if d.is_empty() {
            eprintln!("No changes needed.");
        } else {
            print!("{d}");
        }
    } else if in_place {
        if output != source {
            if let Err(e) = fs::write(path, &output) {
                eprintln!("Error writing {}: {e}", path.display());
                return exitcode::IOERR;
            }
            eprintln!(
                "{}: {total_applied} rules applied, {total_reviews} items need review",
                path.display()
            );
            if schema.is_some() {
                eprintln!("{}: [log_schema] section removed", path.display());
            }
        } else {
            eprintln!("{}: no changes", path.display());
        }
    } else {
        print!("{output}");
    }

    exitcode::OK
}

#[derive(Deserialize, Default)]
struct PartialConfig {
    #[serde(default)]
    log_schema: MigrateLogSchema,
}

fn parse_log_schema_from_toml(toml_source: &str) -> Option<MigrateLogSchema> {
    let config: PartialConfig = toml::from_str(toml_source).ok()?;
    let non_defaults = config.log_schema.non_default_fields();
    if non_defaults.is_empty() {
        None
    } else {
        Some(config.log_schema)
    }
}

fn resolve_log_schema(path: Option<&Path>) -> Result<Option<MigrateLogSchema>, String> {
    let Some(path) = path else { return Ok(None) };
    let source = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(parse_log_schema_from_toml(&source))
}

fn strip_log_schema_section(source: &str) -> String {
    // Handle standalone [log_schema] section
    let re_section = regex::Regex::new(r"(?m)^\[log_schema\]\s*\n(?:[^\[\n][^\n]*\n|\n)*").unwrap();
    let output = re_section.replace_all(source, "");

    // Handle dot-key form: log_schema.field = value
    let re_dotkey = regex::Regex::new(r"(?m)^log_schema\.[a-z_]+\s*=\s*[^\n]*\n?").unwrap();
    let output = re_dotkey.replace_all(&output, "");

    // Clean up resulting double blank lines
    let re_blank = regex::Regex::new(r"\n{3,}").unwrap();
    re_blank.replace_all(&output, "\n\n").into_owned()
}
