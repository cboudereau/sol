use std::num::ParseFloatError;

use metrics::counter;
use sol_lib::NamedInternalEvent;
use sol_lib::internal_event::{
    ComponentEventsDropped, InternalEvent, UNINTENTIONAL, error_stage, error_type,
};

#[derive(NamedInternalEvent)]
pub struct LogToMetricFieldNullError<'a> {
    pub field: &'a str,
}

impl InternalEvent for LogToMetricFieldNullError<'_> {
    fn emit(self) {
        let reason = "Unable to convert null field.";
        error!(
            message = reason,
            error_code = "field_null",
            error_type = error_type::CONDITION_FAILED,
            stage = error_stage::PROCESSING,
            null_field = %self.field
        );
        counter!(
            "component_errors_total",
            "error_code" => "field_null",
            "error_type" => error_type::CONDITION_FAILED,
            "stage" => error_stage::PROCESSING,
            "null_field" => self.field.to_string(),
        )
        .increment(1);

        emit!(ComponentEventsDropped::<UNINTENTIONAL> { count: 1, reason })
    }
}

#[derive(NamedInternalEvent)]
pub struct LogToMetricParseFloatError<'a> {
    pub field: &'a str,
    pub error: ParseFloatError,
}

impl InternalEvent for LogToMetricParseFloatError<'_> {
    fn emit(self) {
        let reason = "Failed to parse field as float.";
        error!(
            message = reason,
            error = ?self.error,
            field = %self.field,
            error_code = "failed_parsing_float",
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::PROCESSING
        );
        counter!(
            "component_errors_total",
            "error_code" => "failed_parsing_float",
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::PROCESSING,
            "field" => self.field.to_string(),
        )
        .increment(1);

        emit!(ComponentEventsDropped::<UNINTENTIONAL> { count: 1, reason })
    }
}
