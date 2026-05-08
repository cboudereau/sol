use opentelemetry_proto::tonic::common::v1::AnyValue;
use sol_lib::event::Event;
use warp::http::{HeaderMap, HeaderValue};

use crate::{event::string_value, sources::http_server::HttpConfigParamKind};

pub fn add_headers(
    events: &mut [Event],
    headers_config: &[HttpConfigParamKind],
    headers: &HeaderMap,
    _source_name: &'static str,
) {
    for h in headers_config {
        match h {
            // Add each non-wildcard containing header that was specified
            // in the `headers` config option to the event if an exact match
            // is found.
            HttpConfigParamKind::Exact(header_name) => {
                let value = headers.get(header_name).map(HeaderValue::as_bytes);

                for event in events.iter_mut() {
                    if let Event::Log(otel_log) = event {
                        match value {
                            Some(v) => {
                                otel_log.set_attribute(
                                    header_name.to_string(),
                                    string_value(String::from_utf8_lossy(v).into_owned()),
                                );
                            }
                            None => {
                                otel_log.set_attribute(
                                    header_name.to_string(),
                                    AnyValue { value: None },
                                );
                            }
                        }
                    }
                }
            }
            HttpConfigParamKind::Glob(header_pattern) => {
                for header_name in headers.keys() {
                    if header_pattern
                        .matches_with(header_name.as_str(), glob::MatchOptions::default())
                    {
                        let value = headers.get(header_name).map(HeaderValue::as_bytes);

                        for event in events.iter_mut() {
                            if let Event::Log(otel_log) = event
                                && let Some(v) = value
                            {
                                let key = header_name.as_str().to_string();
                                // Don't overwrite body fields — body values take precedence over headers.
                                if !otel_log.has_field(&key) {
                                    otel_log.set_attribute(
                                        key,
                                        string_value(String::from_utf8_lossy(v).into_owned()),
                                    );
                                }
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
    use warp::http::HeaderMap;

    use crate::{
        event::OtelLog,
        sources::{http_server::HttpConfigParamKind, util::add_headers},
    };

    #[test]
    fn multiple_headers() {
        let header_names = [
            HttpConfigParamKind::Exact("Content-Type".into()),
            HttpConfigParamKind::Exact("User-Agent".into()),
        ];
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/x-protobuf".parse().unwrap());
        headers.insert("User-Agent", "Test".parse().unwrap());
        headers.insert("Content-Encoding", "gzip".parse().unwrap());

        let mut events = [OtelLog::from(value!({})).into()];
        add_headers(&mut events, &header_names, &headers, "test");

        let log = events[0].as_log();
        assert_eq!(
            log.get("Content-Type").unwrap(),
            "application/x-protobuf".into()
        );
        assert_eq!(log.get("User-Agent").unwrap(), "Test".into());
        assert!(log.get("Content-Encoding").is_none());
    }

    #[test]
    fn multiple_headers_wildcard() {
        let header_names = [HttpConfigParamKind::Glob(
            glob::Pattern::new("Content-*").unwrap(),
        )];
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/x-protobuf".parse().unwrap());
        headers.insert("User-Agent", "Test".parse().unwrap());
        headers.insert("Content-Encoding", "gzip".parse().unwrap());

        let mut events = [OtelLog::from(value!({})).into()];
        add_headers(&mut events, &header_names, &headers, "test");

        let log = events[0].as_log();
        assert_eq!(
            log.get("content-type").unwrap(),
            "application/x-protobuf".into(),
            "Checking log contains Content-Type header"
        );
        assert!(
            !log.contains("user-agent"),
            "Checking log does not contain User-Agent header"
        );
        assert_eq!(
            log.get("content-encoding").unwrap(),
            "gzip".into(),
            "Checking log contains Content-Encoding header"
        );
    }
}
