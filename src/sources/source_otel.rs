use std::collections::BTreeMap;

use sol_lib::event::string_value;

pub fn build_source_resource(
    source_type: &str,
    config_overrides: &BTreeMap<String, String>,
) -> sol_lib::event::otel_metric::Resource {
    use sol_lib::event::otel_metric::KeyValue;

    let mut attrs = Vec::new();

    let service_name = config_overrides
        .get("service.name")
        .cloned()
        .unwrap_or_else(|| format!("sol/{source_type}"));
    attrs.push(KeyValue {
        key: "service.name".into(),
        value: Some(string_value(service_name)),
    });

    let host_name = config_overrides
        .get("host.name")
        .cloned()
        .unwrap_or_else(|| crate::get_hostname().unwrap_or_default());
    if !host_name.is_empty() {
        attrs.push(KeyValue {
            key: "host.name".into(),
            value: Some(string_value(host_name)),
        });
    }

    for (k, v) in config_overrides {
        if k == "service.name" || k == "host.name" {
            continue;
        }
        attrs.push(KeyValue {
            key: k.clone(),
            value: Some(string_value(v.clone())),
        });
    }

    sol_lib::event::otel_metric::Resource {
        attributes: attrs,
        dropped_attributes_count: 0,
        ..Default::default()
    }
}

pub fn build_source_scope(source_type: &str) -> sol_lib::event::otel_metric::InstrumentationScope {
    sol_lib::event::otel_metric::InstrumentationScope {
        name: format!("sol/{source_type}"),
        version: crate::built_info::PKG_VERSION.to_string(),
        attributes: vec![],
        dropped_attributes_count: 0,
    }
}
