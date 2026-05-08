use std::collections::HashMap;

use opentelemetry_proto::tonic::common::v1::AnyValue;
use sol_lib::event::Event;

use crate::{event::string_value, sources::http_server::HttpConfigParamKind};

pub fn add_query_parameters(
    events: &mut [Event],
    query_parameters_config: &[HttpConfigParamKind],
    query_parameters: &HashMap<String, String>,
    _source_name: &'static str,
) {
    for qp in query_parameters_config {
        match qp {
            // Add each non-wildcard containing query_parameter that was specified
            // in the `query_parameters` config option to the event if an exact match
            // is found.
            HttpConfigParamKind::Exact(query_parameter_name) => {
                let value = query_parameters.get(query_parameter_name);

                for event in events.iter_mut() {
                    if let Event::Log(otel_log) = event {
                        match value {
                            Some(v) => {
                                otel_log.set_attribute(
                                    query_parameter_name.to_string(),
                                    string_value(v),
                                );
                            }
                            None => {
                                otel_log.set_attribute(
                                    query_parameter_name.to_string(),
                                    AnyValue { value: None },
                                );
                            }
                        }
                    }
                }
            }
            HttpConfigParamKind::Glob(query_parameter_pattern) => {
                for query_parameter_name in query_parameters.keys() {
                    if query_parameter_pattern
                        .matches_with(query_parameter_name.as_str(), glob::MatchOptions::default())
                    {
                        let value = query_parameters.get(query_parameter_name);

                        for event in events.iter_mut() {
                            if let Event::Log(otel_log) = event
                                && let Some(v) = value
                            {
                                otel_log.set_attribute(
                                    query_parameter_name.to_string(),
                                    string_value(v),
                                );
                            }
                        }
                    }
                }
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use vrl::value;

    use crate::{
        event::OtelLog,
        sources::{http_server::HttpConfigParamKind, util::add_query_parameters},
    };

    #[test]
    fn multiple_query_params() {
        let query_params_names = [
            HttpConfigParamKind::Exact("param1".into()),
            HttpConfigParamKind::Exact("param2".into()),
        ];
        let query_params = [
            ("param1".into(), "value1".into()),
            ("param2".into(), "value2".into()),
            ("param3".into(), "value3".into()),
        ]
        .into();

        let mut events = [OtelLog::from(value!({})).into()];
        add_query_parameters(&mut events, &query_params_names, &query_params, "test");

        let log = events[0].as_log();
        assert_eq!(log.get("param1").unwrap(), "value1".into());
        assert_eq!(log.get("param2").unwrap(), "value2".into());
        assert!(log.get("param3").is_none());
    }

    #[test]
    fn multiple_query_params_wildcard() {
        let query_params_names = [HttpConfigParamKind::Glob(glob::Pattern::new("*").unwrap())];
        let query_params = [
            ("param1".into(), "value1".into()),
            ("param2".into(), "value2".into()),
            ("param3".into(), "value3".into()),
        ]
        .into();

        let mut events = [OtelLog::from(value!({})).into()];
        add_query_parameters(&mut events, &query_params_names, &query_params, "test");

        let log = events[0].as_log();
        assert_eq!(
            log.get("param1").unwrap(),
            "value1".into(),
            "Checking log contains first query parameter"
        );
        assert_eq!(
            log.get("param2").unwrap(),
            "value2".into(),
            "Checking log contains second query parameter"
        );
        assert_eq!(
            log.get("param3").unwrap(),
            "value3".into(),
            "Checking log contains third query parameter"
        );
    }
}
