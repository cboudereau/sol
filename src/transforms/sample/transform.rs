use std::{collections::HashMap, fmt};

use sol_lib::lookup::lookup_v2::OptionalValuePath;

use crate::{
    conditions::Condition,
    event::{Event, Value, string_value},
    internal_events::SampleEventDiscarded,
    sinks::prelude::TemplateRenderingError,
    template::Template,
    transforms::{FunctionTransform, OutputBuffer},
};

/// Exists only for backwards compatability purposes so that the value of sample_rate_key is
/// consistent after the internal implementation of the Sample class was modified to work in terms
/// of percentages
#[derive(Clone, Debug)]
pub enum SampleMode {
    Rate {
        rate: u64,
        counters: HashMap<Option<String>, u64>,
    },
    Ratio {
        ratio: f64,
        values: HashMap<Option<String>, f64>,
        hash_ratio_threshold: u64,
    },
}

impl SampleMode {
    pub fn new_rate(rate: u64) -> Self {
        Self::Rate {
            rate,
            counters: HashMap::default(),
        }
    }

    pub fn new_ratio(ratio: f64) -> Self {
        Self::Ratio {
            ratio,
            values: HashMap::default(),
            // Supports the 'key_field' option, assuming an equal distribution of values for a given
            // field, hashing its contents this component should output events according to the
            // configured ratio.
            //
            // To do one option would be to convert the hash to a number between 0 and 1 and compare
            // to the ratio. However to address issues with precision, here the ratio is scaled to
            // meet the width of the type of the hash.
            hash_ratio_threshold: {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "sampling hash threshold; u64::MAX as f64 is the closest representable value"
                )]
                let max_f = u128::from(u64::MAX) as f64;
                #[expect(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    reason = "sampling threshold: ratio in [0,1] × u64::MAX is non-negative and fits in u64"
                )]
                let threshold = (ratio * max_f) as u64;
                threshold
            },
        }
    }

    fn increment(&mut self, group_by_key: Option<String>, value: Option<&Value>) -> bool {
        let threshold_exceeded = match self {
            Self::Rate { rate, counters } => {
                let counter_value = counters.entry(group_by_key).or_default();
                let old_counter_value = *counter_value;
                *counter_value += 1;
                old_counter_value % *rate == 0
            }
            Self::Ratio { ratio, values, .. } => {
                let value = values.entry(group_by_key).or_insert(1.0 - *ratio);
                let increment: f64 = *value + *ratio;
                *value = if increment >= 1.0 {
                    increment - 1.0
                } else {
                    increment
                };
                increment >= 1.0
            }
        };
        if let Some(value) = value {
            self.hash_within_ratio(value.to_string_lossy().as_bytes())
        } else {
            threshold_exceeded
        }
    }

    fn hash_within_ratio(&self, value: &[u8]) -> bool {
        let hash = seahash::hash(value);
        match self {
            Self::Rate { rate, .. } => hash.is_multiple_of(*rate),
            Self::Ratio {
                hash_ratio_threshold,
                ..
            } => hash <= *hash_ratio_threshold,
        }
    }
}

impl fmt::Display for SampleMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Avoids the print of an additional '.0' which was not performed in the previous
        // implementation
        match self {
            Self::Rate { rate, .. } => write!(f, "{rate}"),
            Self::Ratio { ratio, .. } => write!(f, "{ratio}"),
        }
    }
}

#[derive(Clone)]
pub struct Sample {
    #[allow(dead_code)]
    name: String,
    rate: SampleMode,
    key_field: Option<String>,
    group_by: Option<Template>,
    exclude: Option<Condition>,
    sample_rate_key: OptionalValuePath,
}

impl Sample {
    // This function is dead code when the feature flag `transforms-impl-sample` is specified but not
    // `transforms-sample`.
    #![allow(dead_code)]
    pub const fn new(
        name: String,
        rate: SampleMode,
        key_field: Option<String>,
        group_by: Option<Template>,
        exclude: Option<Condition>,
        sample_rate_key: OptionalValuePath,
    ) -> Self {
        Self {
            name,
            rate,
            key_field,
            group_by,
            exclude,
            sample_rate_key,
        }
    }

    #[cfg(test)]
    #[expect(
        clippy::cast_precision_loss,
        reason = "test-only helper; sample rates are small enough for f64"
    )]
    pub fn ratio(&self) -> f64 {
        match self.rate {
            SampleMode::Rate { rate, .. } => 1.0f64 / rate as f64,
            SampleMode::Ratio { ratio, .. } => ratio,
        }
    }
}

impl FunctionTransform for Sample {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let mut event = {
            if let Some(condition) = self.exclude.as_ref() {
                let (result, event) = condition.check(event);
                if result {
                    output.push(event);
                    return;
                } else {
                    event
                }
            } else {
                event
            }
        };

        let value = self.key_field.as_ref().and_then(|key_field| match &event {
            Event::Log(otel_log) => otel_log
                .parse_path_and_get_value(key_field.as_str())
                .ok()
                .flatten(),
            Event::Trace(otel_span) => otel_span
                .parse_path_and_get_value(key_field.as_str())
                .ok()
                .flatten(),
            Event::Metric(_) => {
                panic!("component can never receive metric events")
            }
        });

        // Fetch actual field value if group_by option is set.
        let group_by_key = self.group_by.as_ref().and_then(|group_by| {
            match &event {
                Event::Log(otel_log) => group_by.render_string(otel_log),
                Event::Trace(otel_span) => group_by.render_string(otel_span),
                Event::Metric(_) => {
                    panic!("component can never receive metric events")
                }
            }
            .map_err(|error| {
                emit!(TemplateRenderingError {
                    error,
                    field: Some("group_by"),
                    drop_event: false,
                })
            })
            .ok()
        });

        if self.rate.increment(group_by_key, value.as_ref()) {
            if let Some(path) = &self.sample_rate_key.path {
                match event {
                    Event::Log(ref mut otel_log) => {
                        otel_log
                            .set_attribute(path.to_string(), string_value(self.rate.to_string()));
                    }
                    Event::Trace(ref mut otel_span) => {
                        otel_span
                            .set_attribute(path.to_string(), string_value(self.rate.to_string()));
                    }
                    Event::Metric(_) => {
                        panic!("component can never receive metric events")
                    }
                };
            }
            output.push(event);
        } else {
            emit!(SampleEventDiscarded);
        }
    }
}
