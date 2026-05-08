use super::MigrateLogSchema;
use super::*;

fn assert_migrates(input: &str, expected: &str) {
    let output = migrate(input);
    assert_eq!(
        output.text, expected,
        "\n--- input ---\n{input}\n--- expected ---\n{expected}\n--- got ---\n{}",
        output.text
    );
}

fn assert_unchanged(input: &str) {
    let output = migrate(input);
    assert_eq!(
        output.text, input,
        "\n--- input ---\n{input}\n--- should be unchanged but got ---\n{}",
        output.text
    );
}

fn assert_has_review(input: &str, rule: RuleId) {
    let output = migrate(input);
    assert!(
        output.reviews.iter().any(|r| r.rule_id == rule),
        "Expected REVIEW for {rule} in:\n{input}\nGot reviews: {:?}",
        output.reviews
    );
}

// ═══════════════════════════════════════════════════════════════
// Pass 1: Structural rewrites
// ═══════════════════════════════════════════════════════════════

#[test]
fn log01_message_standalone() {
    assert_migrates(
        ".foo = .message",
        "# MIGRATED: .message → .body [LOG-01]\n.foo = .body",
    );
}

#[test]
fn log01_message_in_function() {
    assert_migrates(
        "to_string(.message)",
        "# MIGRATED: .message → .body [LOG-01]\nto_string(.body)",
    );
}

#[test]
fn log01_message_not_in_string() {
    assert_unchanged(r#".foo = "has .message in string""#);
}

#[test]
fn log01_message_not_prefix() {
    assert_unchanged(".message_type = 1");
}

#[test]
fn log02_timestamp() {
    assert_migrates(
        ".ts = .timestamp",
        "# MIGRATED: .timestamp → .time_unix_nano [LOG-02]\n.ts = .time_unix_nano",
    );
}

#[test]
fn log02_timestamp_not_prefix() {
    assert_unchanged(".timestamp_format = \"rfc3339\"");
}

#[test]
fn log03_source_type() {
    assert_migrates(
        "x = .source_type",
        "# MIGRATED: .source_type → .attributes.\"pipeline.source_type\" [LOG-03]\nx = .attributes.\"pipeline.source_type\"",
    );
}

#[test]
fn log04_host() {
    assert_migrates(
        ".h = .host",
        "# MIGRATED: .host → .resource.attributes.\"host.name\" [LOG-04]\n.h = .resource.attributes.\"host.name\"",
    );
}

#[test]
fn log04_host_not_prefix() {
    assert_unchanged(".hostname = \"foo\"");
}

#[test]
fn log05_tags_standalone() {
    assert_migrates(
        "t = .tags",
        "# MIGRATED: .tags → .attributes [LOG-05]\nt = .attributes",
    );
}

#[test]
fn log06_tags_key() {
    assert_migrates(
        "env = .tags.environment",
        "# MIGRATED: .tags.<key> → .attributes.\"<key>\" [LOG-06]\nenv = .attributes.\"environment\"",
    );
}

#[test]
fn log07_level() {
    assert_migrates(
        ".severity_text = .level",
        "# MIGRATED: .level/.severity → .severity_text [LOG-07]\n.severity_text = .severity_text",
    );
}

#[test]
fn log07_severity() {
    assert_migrates(
        "s = .severity",
        "# MIGRATED: .level/.severity → .severity_text [LOG-07]\ns = .severity_text",
    );
}

#[test]
fn meta01_source_type() {
    assert_migrates(
        "src = %vector.source_type",
        "# MIGRATED: %vector.source_type → %pipeline.source_type [META-01]\nsrc = %pipeline.source_type",
    );
}

#[test]
fn meta02_source_id() {
    assert_migrates(
        "id = %vector.source_id",
        "# MIGRATED: %vector.source_id → %pipeline.source_id [META-02]\nid = %pipeline.source_id",
    );
}

// ═══════════════════════════════════════════════════════════════
// Pass 2: Semantic rewrites
// ═══════════════════════════════════════════════════════════════

#[test]
fn sem02_exists_message() {
    assert_migrates(
        "if exists(.message) {",
        "# MIGRATED: .message → .body [LOG-01]\nif exists(.body) {",
    );
}

#[test]
fn sem03_del_message() {
    let output = migrate("del(.message)");
    assert!(output.text.contains("MIGRATED: .message → .body [LOG-01]"));
    // del(.body) is a valid operation — no longer triggers "delete entire event" review
}

#[test]
fn sem05_parse_json_message() {
    let output = migrate("parsed = parse_json(.message)");
    // .message → .body, parse_json(.body) is valid as-is
    assert!(
        output.text.contains("parse_json(.body)"),
        "Expected parse_json(.body) in:\n{}",
        output.text
    );
}

#[test]
fn sem06_is_string_message() {
    let output = migrate("if is_string(.message) {");
    // .message → .body, is_string(.body) is valid as-is
    assert!(
        output.text.contains("is_string(.body)"),
        "Expected is_string(.body) in:\n{}",
        output.text
    );
}

#[test]
fn sem07_assert_eq_message() {
    let output = migrate(r#"assert_eq!(.message, "hello")"#);
    // .message → .body, assert_eq!(.body, "hello") is valid as-is
    assert!(
        output.text.contains(r#"assert_eq!(.body,"#),
        "Expected assert_eq!(.body, ...) in:\n{}",
        output.text
    );
}

// ═══════════════════════════════════════════════════════════════
// Pass 3: Metric rewrites
// ═══════════════════════════════════════════════════════════════

#[test]
fn met02_namespace() {
    assert_migrates(
        "ns = .namespace",
        "# MIGRATED: .namespace → .attributes.\"metric.namespace\" [MET-02]\nns = .attributes.\"metric.namespace\"",
    );
}

#[test]
fn met05_kind_review() {
    assert_has_review(".kind == \"incremental\"", RuleId::Met05);
}

#[test]
fn met06_counter_value() {
    assert_migrates(
        "v = .value.counter.value",
        "# MIGRATED: .value.counter.value → .data.sum.data_points[0].value [MET-06]\nv = .data.sum.data_points[0].value",
    );
}

#[test]
fn met07_gauge_value() {
    assert_migrates(
        "v = .value.gauge.value",
        "# MIGRATED: .value.gauge.value → .data.gauge.data_points[0].value [MET-07]\nv = .data.gauge.data_points[0].value",
    );
}

// ═══════════════════════════════════════════════════════════════
// Multi-line programs
// ═══════════════════════════════════════════════════════════════

#[test]
fn multi_line_program() {
    let input = r#".parsed = parse_json(.message)
.level = "info"
.host = "myhost"
.tags.env = "prod""#;

    let output = migrate(input);
    assert!(
        output.text.contains("parse_json(.body)"),
        "Expected parse_json(.body) in:\n{}",
        output.text
    );
    assert!(output.text.contains(".severity_text"));
    assert!(output.text.contains(r#".resource.attributes."host.name""#));
    assert!(output.text.contains(r#".attributes."env""#));
}

#[test]
fn preserves_comments() {
    assert_unchanged("# this is a comment with .message in it");
}

#[test]
fn preserves_strings() {
    assert_unchanged(r#".foo = "the .message field""#);
}

// ═══════════════════════════════════════════════════════════════
// Diff output
// ═══════════════════════════════════════════════════════════════

#[test]
fn diff_shows_changes() {
    let input = ".foo = .message";
    let d = diff(input, None);
    assert!(d.contains("-.foo = .message"));
    assert!(d.contains("+.foo = .body"));
}

#[test]
fn diff_empty_when_unchanged() {
    let input = ".foo = .bar";
    let d = diff(input, None);
    assert!(d.is_empty());
}

// ═══════════════════════════════════════════════════════════════
// Pass 0: Log schema rewrites
// ═══════════════════════════════════════════════════════════════

fn schema_with(field: &str, value: &str) -> MigrateLogSchema {
    let toml = format!("{field} = \"{value}\"");
    toml::from_str(&toml).unwrap()
}

fn assert_migrates_with_schema(input: &str, schema: &MigrateLogSchema, expected_fragment: &str) {
    let output = migrate_with_log_schema(input, schema);
    assert!(
        output.text.contains(expected_fragment),
        "\n--- input ---\n{input}\n--- expected fragment ---\n{expected_fragment}\n--- got ---\n{}",
        output.text
    );
}

#[test]
fn ls01_custom_message_key() {
    let schema = schema_with("message_key", "msg");
    assert_migrates_with_schema(".foo = .msg", &schema, ".foo = .body");
}

#[test]
fn ls02_custom_timestamp_key() {
    let schema = schema_with("timestamp_key", "ts");
    assert_migrates_with_schema(".t = .ts", &schema, ".t = .time_unix_nano");
}

#[test]
fn ls03_custom_host_key() {
    let schema = schema_with("host_key", "hostname");
    assert_migrates_with_schema(
        ".h = .hostname",
        &schema,
        r#".h = .resource.attributes."host.name""#,
    );
}

#[test]
fn ls04_custom_source_type_key() {
    let schema = schema_with("source_type_key", "type");
    assert_migrates_with_schema(
        ".st = .type",
        &schema,
        r#".st = .attributes."pipeline.source_type""#,
    );
}

#[test]
fn ls05_custom_metadata_key_review() {
    let schema = schema_with("metadata_key", "meta");
    let output = migrate_with_log_schema(".m = .meta", &schema);
    assert!(
        output.reviews.iter().any(|r| r.rule_id == RuleId::Ls05),
        "Expected REVIEW for LS-05, got: {:?}",
        output.reviews
    );
}

#[test]
fn ls_no_rewrite_when_custom_equals_default() {
    // "message" is the old default handled by LOG-01, not LS-*
    let schema = schema_with("message_key", "message");
    let output = migrate_with_log_schema(".foo = .message", &schema);
    assert!(
        output.applied.iter().all(|a| a.rule_id != RuleId::Ls01),
        "LS-01 should not fire when custom matches structural default"
    );
    // LOG-01 should still fire
    assert!(output.text.contains(".body"));
}

#[test]
fn ls_no_rewrite_when_custom_equals_canonical() {
    // "body" is already the canonical name
    let schema = schema_with("message_key", "body");
    let output = migrate_with_log_schema(".foo = .body", &schema);
    assert!(
        output.applied.iter().all(|a| a.rule_id != RuleId::Ls01),
        "LS-01 should not fire when custom matches canonical"
    );
}

#[test]
fn ls_custom_not_prefix_of_longer_ident() {
    let schema = schema_with("message_key", "msg");
    let output = migrate_with_log_schema(".msg_type = 1", &schema);
    assert!(
        !output.text.contains(".body"),
        ".msg should not match .msg_type"
    );
}

#[test]
fn ls_combined_custom_and_structural() {
    let toml = r#"
        message_key = "payload"
        host_key = "server"
    "#;
    let schema: MigrateLogSchema = toml::from_str(toml).unwrap();
    let input = r#".out = .payload
.h = .server
.ts = .timestamp"#;
    let output = migrate_with_log_schema(input, &schema);
    // Pass 0: .payload → .body, .server → .resource.attributes."host.name"
    // Pass 1: .timestamp → .time_unix_nano
    assert!(
        output.text.contains(".out = .body"),
        "payload→body:\n{}",
        output.text
    );
    assert!(
        output.text.contains(r#".resource.attributes."host.name""#),
        "server→host.name:\n{}",
        output.text
    );
    assert!(
        output.text.contains(".time_unix_nano"),
        "timestamp→time_unix_nano:\n{}",
        output.text
    );
}
