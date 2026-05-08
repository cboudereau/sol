use chrono::{DateTime, Utc};
use derivative::Derivative;
use sol_lib::conversion;

use crate::{
    event::{self, Event},
    internal_events::{
        DROP_EVENT, ParserConversionError, ParserMatchError, ParserMissingFieldError,
    },
    transforms::{FunctionTransform, OutputBuffer},
};

const STREAM_KEY: &str = "stream";
const TIMESTAMP_KEY: &str = "timestamp";

/// Parser for the CRI log format.
///
/// Expects logs to arrive in a CRI log format.
///
/// CRI log format ([documentation][cri_log_format]) is a simple
/// newline-separated text format. We rely on regular expressions to parse it.
///
/// Normalizes parsed data for consistency.
///
/// [cri_log_format]: https://github.com/kubernetes/community/blob/ee2abbf9dbfa4523b414f99a04ddc97bd38c74b2/contributors/design-proposals/node/kubelet-cri-logging.md
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub(super) struct Cri;

impl Cri {
    pub const fn new() -> Self {
        Self
    }
}

impl FunctionTransform for Cri {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        if let Event::Log(ref mut otel_log) = event {
            let body = otel_log.body_string();
            if body.is_empty() {
                emit!(ParserMissingFieldError::<DROP_EVENT> { field: "body" });
                return;
            }
            let s = bytes::Bytes::from(body);
            match parse_log_line(&s) {
                None => {
                    emit!(ParserMatchError { value: &s[..] });
                    return;
                }
                Some(parsed_log) => {
                    otel_log.set_body(crate::event::string_value(String::from_utf8_lossy(
                        parsed_log.message,
                    )));

                    if parsed_log.multiline_tag[0] == b'P' {
                        otel_log.set_attribute(
                            event::PARTIAL.to_string(),
                            opentelemetry_proto::tonic::common::v1::AnyValue {
                                value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::BoolValue(true)),
                            },
                        );
                    }

                    let ds = String::from_utf8_lossy(parsed_log.timestamp);
                    match DateTime::parse_from_str(&ds, "%+") {
                        Ok(dt) => {
                            let ts = dt.with_timezone(&Utc);
                            #[expect(
                                clippy::cast_sign_loss,
                                reason = "timestamp nanos are non-negative for post-epoch dates; 0 fallback is safe"
                            )]
                            let nanos = ts.timestamp_nanos_opt().unwrap_or(0) as u64;
                            otel_log.record_mut().time_unix_nano = nanos;
                        }
                        Err(e) => {
                            emit!(ParserConversionError {
                                name: TIMESTAMP_KEY,
                                error: conversion::Error::TimestampParse {
                                    s: ds.to_string(),
                                    source: e,
                                },
                            });
                        }
                    }

                    otel_log.set_attribute(
                        STREAM_KEY.to_string(),
                        crate::event::string_value(String::from_utf8_lossy(parsed_log.stream)),
                    );
                }
            }
        }

        output.push(event);
    }
}

struct ParsedLog<'a> {
    timestamp: &'a [u8],
    stream: &'a [u8],
    multiline_tag: &'a [u8],
    message: &'a [u8],
}

#[allow(clippy::trivially_copy_pass_by_ref)]
#[inline]
const fn is_delimiter(c: &u8) -> bool {
    *c == b' '
}

/// Parses a CRI log line.
///
/// Equivalent to regex: `(?-u)^(?P<timestamp>.*) (?P<stream>(stdout|stderr)) (?P<multiline_tag>(P|F)) (?P<message>.*)(?P<new_line_tag>\n?)$`
#[inline]
fn parse_log_line(line: &[u8]) -> Option<ParsedLog<'_>> {
    let rest = line;

    let after_timestamp_pos = rest.iter().position(is_delimiter)?;
    let (timestamp, rest) = rest.split_at(after_timestamp_pos + 1);
    let timestamp = timestamp.split_last()?.1; // Trim the delimiter

    let after_stream_pos = rest.iter().position(is_delimiter)?;
    let (stream, rest) = rest.split_at(after_stream_pos + 1);
    let stream = stream.split_last()?.1;
    if stream != b"stdout".as_ref() && stream != b"stderr".as_ref() {
        return None;
    }

    let after_multiline_tag_pos = rest.iter().position(is_delimiter)?;
    let (multiline_tag, rest) = rest.split_at(after_multiline_tag_pos + 1);
    let multiline_tag = multiline_tag.split_last()?.1;
    if multiline_tag != b"F".as_ref() && multiline_tag != b"P".as_ref() {
        return None;
    }

    let has_new_line_tag = !rest.is_empty() && *rest.last()? == b'\n';
    let message = if has_new_line_tag {
        // Remove the newline tag field, if it exists.
        // For additional details, see https://github.com/vectordotdev/vector/issues/8606.
        rest.split_last()?.1
    } else {
        rest
    };

    Some(ParsedLog {
        timestamp,
        stream,
        multiline_tag,
        message,
    })
}

#[cfg(test)]
pub mod tests {
    use bytes::Bytes;
    use vrl::value;

    use super::{super::test_util, *};
    use crate::{event::OtelLog, test_util::trace_init};

    fn make_long_string(base: &str, len: usize) -> String {
        base.chars().cycle().take(len).collect()
    }

    /// Shared test cases.
    pub fn valid_cases() -> Vec<(Bytes, Vec<Event>)> {
        vec![
            (
                Bytes::from(
                    "2016-10-06T00:17:09.669794202Z stdout F The content of the log entry 1",
                ),
                vec![test_util::make_log_event(
                    value!("The content of the log entry 1"),
                    "2016-10-06T00:17:09.669794202Z",
                    "stdout",
                    false,
                )],
            ),
            (
                Bytes::from("2016-10-06T00:17:09.669794202Z stdout P First line of log entry 2"),
                vec![test_util::make_log_event(
                    value!("First line of log entry 2"),
                    "2016-10-06T00:17:09.669794202Z",
                    "stdout",
                    true,
                )],
            ),
            (
                Bytes::from(
                    "2016-10-06T00:17:09.669794202Z stdout P Second line of the log entry 2",
                ),
                vec![test_util::make_log_event(
                    value!("Second line of the log entry 2"),
                    "2016-10-06T00:17:09.669794202Z",
                    "stdout",
                    true,
                )],
            ),
            (
                Bytes::from("2016-10-06T00:17:10.113242941Z stderr F Last line of the log entry 2"),
                vec![test_util::make_log_event(
                    value!("Last line of the log entry 2"),
                    "2016-10-06T00:17:10.113242941Z",
                    "stderr",
                    false,
                )],
            ),
            // A part of the partial message with a realistic length.
            (
                Bytes::from(
                    [
                        r"2016-10-06T00:17:10.113242941Z stdout P ",
                        make_long_string("very long message ", 16 * 1024).as_str(),
                    ]
                    .join(""),
                ),
                vec![test_util::make_log_event(
                    value!(make_long_string("very long message ", 16 * 1024)),
                    "2016-10-06T00:17:10.113242941Z",
                    "stdout",
                    true,
                )],
            ),
            (
                // This is not valid UTF-8 string, ends with \n
                // 2021-08-05T17:35:26.640507539Z stdout P Hello World Привет Ми\xd1\n
                Bytes::from(vec![
                    50, 48, 50, 49, 45, 48, 56, 45, 48, 53, 84, 49, 55, 58, 51, 53, 58, 50, 54, 46,
                    54, 52, 48, 53, 48, 55, 53, 51, 57, 90, 32, 115, 116, 100, 111, 117, 116, 32,
                    80, 32, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 32, 208, 159, 209,
                    128, 208, 184, 208, 178, 208, 181, 209, 130, 32, 208, 156, 208, 184, 209, 10,
                ]),
                vec![test_util::make_log_event(
                    value!(Bytes::from(vec![
                        72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 32, 208, 159, 209, 128,
                        208, 184, 208, 178, 208, 181, 209, 130, 32, 208, 156, 208, 184, 209,
                    ])),
                    "2021-08-05T17:35:26.640507539Z",
                    "stdout",
                    true,
                )],
            ),
        ]
    }

    #[test]
    fn test_parsing_valid_vector_namespace() {
        trace_init();
        test_util::test_parser(
            Cri::new,
            |bytes| Event::Log(OtelLog::from(value!(bytes))),
            valid_cases(),
        );
    }

    #[test]
    fn test_parsing_valid_legacy_namespace() {
        trace_init();
        test_util::test_parser(
            Cri::new,
            |bytes| Event::Log(OtelLog::from(bytes)),
            valid_cases(),
        );
    }
}
