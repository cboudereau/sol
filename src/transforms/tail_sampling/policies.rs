use sol_lib::configurable::configurable_component;

use super::transform::BufferedTrace;

/// Sampling decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Keep the trace.
    Sample,
    /// Drop the trace.
    Drop,
    /// No decision yet (policy doesn't apply).
    Pending,
}

/// A sampling policy that evaluates a buffered trace.
pub trait SamplingPolicy: Send + Sync {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision;
    fn name(&self) -> &str;
}

/// Policy configuration — deserialized from TOML.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PolicyConfig {
    /// Sample all traces unconditionally.
    AlwaysSample(AlwaysSampleConfig),
    /// Sample traces containing spans with matching status codes.
    StatusCode(StatusCodeConfig),
    /// Sample traces whose duration exceeds a threshold.
    Latency(LatencyConfig),
    /// Hash-based probabilistic sampling.
    Probabilistic(ProbabilisticConfig),
    /// Token-bucket rate limiting.
    RateLimiting(RateLimitingConfig),
    /// Sample by span count in trace.
    SpanCount(SpanCountConfig),
    /// Sample by string attribute match.
    StringAttribute(StringAttributeConfig),
    /// Sample by numeric attribute range.
    NumericAttribute(NumericAttributeConfig),
    /// Composite AND: all sub-policies must return Sample.
    And(AndConfig),
}

impl PolicyConfig {
    /// Build a boxed `SamplingPolicy` from this config.
    pub fn build(&self) -> Box<dyn SamplingPolicy> {
        match self {
            PolicyConfig::AlwaysSample(c) => Box::new(AlwaysSample(c.name.clone())),
            PolicyConfig::StatusCode(c) => Box::new(StatusCode {
                name: c.name.clone(),
                status_codes: c.status_codes.clone(),
            }),
            PolicyConfig::Latency(c) => Box::new(Latency {
                name: c.name.clone(),
                threshold_ms: c.threshold_ms,
                upper_threshold_ms: c.upper_threshold_ms,
            }),
            PolicyConfig::Probabilistic(c) => Box::new(Probabilistic {
                name: c.name.clone(),
                sampling_percentage: c.sampling_percentage,
            }),
            PolicyConfig::RateLimiting(c) => Box::new(RateLimiting {
                name: c.name.clone(),
                spans_per_second: c.spans_per_second,
                state: std::sync::Mutex::new(RateLimitState {
                    tokens: c.spans_per_second,
                    last_refill: std::time::Instant::now(),
                }),
            }),
            PolicyConfig::SpanCount(c) => Box::new(SpanCount {
                name: c.name.clone(),
                min_spans: c.min_spans,
                max_spans: c.max_spans,
            }),
            PolicyConfig::StringAttribute(c) => {
                let compiled_regexes = if c.enabled_regex_matching {
                    Some(
                        c.values
                            .iter()
                            .map(|v| {
                                regex::Regex::new(v)
                                    .expect("invalid regex in string_attribute policy")
                            })
                            .collect(),
                    )
                } else {
                    None
                };
                Box::new(StringAttribute {
                    name: c.name.clone(),
                    key: c.key.clone(),
                    values: c.values.clone(),
                    compiled_regexes,
                    invert_match: c.invert_match,
                })
            }
            PolicyConfig::NumericAttribute(c) => Box::new(NumericAttribute {
                name: c.name.clone(),
                key: c.key.clone(),
                min_value: c.min_value,
                max_value: c.max_value,
            }),
            PolicyConfig::And(c) => Box::new(And {
                name: c.name.clone(),
                sub_policies: c.sub_policies.iter().map(|p| p.build()).collect(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Config structs (serde)
// ---------------------------------------------------------------------------

/// Always sample config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct AlwaysSampleConfig {
    /// Policy name for metrics.
    pub name: String,
}

/// Status code policy config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct StatusCodeConfig {
    /// Policy name for metrics.
    pub name: String,
    /// Status codes to match (e.g. "ERROR", "OK", "UNSET").
    pub status_codes: Vec<String>,
}

/// Latency policy config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct LatencyConfig {
    /// Policy name for metrics.
    pub name: String,
    /// Minimum trace duration in ms to trigger sampling.
    pub threshold_ms: u64,
    /// Optional upper bound — only sample traces shorter than this.
    #[serde(default)]
    pub upper_threshold_ms: Option<u64>,
}

/// Probabilistic policy config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct ProbabilisticConfig {
    /// Policy name for metrics.
    pub name: String,
    /// Percentage of traces to sample (0.0 – 100.0).
    pub sampling_percentage: f64,
}

/// Rate limiting policy config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct RateLimitingConfig {
    /// Policy name for metrics.
    pub name: String,
    /// Maximum spans per second.
    pub spans_per_second: f64,
}

/// Span count policy config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct SpanCountConfig {
    /// Policy name for metrics.
    pub name: String,
    /// Minimum span count.
    #[serde(default)]
    pub min_spans: Option<usize>,
    /// Maximum span count.
    #[serde(default)]
    pub max_spans: Option<usize>,
}

/// String attribute policy config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct StringAttributeConfig {
    /// Policy name for metrics.
    pub name: String,
    /// Attribute key to match.
    pub key: String,
    /// Values to match against (exact strings or regex patterns).
    pub values: Vec<String>,
    /// Treat values as regex patterns.
    #[serde(default)]
    pub enabled_regex_matching: bool,
    /// Invert the match result (Sample becomes Pending and vice versa).
    #[serde(default)]
    pub invert_match: bool,
}

/// AND composite policy config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct AndConfig {
    /// Policy name for metrics.
    pub name: String,
    /// Sub-policies: all must return Sample for the AND to Sample.
    pub sub_policies: Vec<PolicyConfig>,
}

/// Numeric attribute policy config.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct NumericAttributeConfig {
    /// Policy name for metrics.
    pub name: String,
    /// Attribute key to match.
    pub key: String,
    /// Minimum value (inclusive).
    pub min_value: f64,
    /// Maximum value (inclusive).
    pub max_value: f64,
}

// ---------------------------------------------------------------------------
// Policy implementations
// ---------------------------------------------------------------------------

use opentelemetry_proto::tonic::trace::v1::status::StatusCode as OtelStatusCode;

/// Always sample.
struct AlwaysSample(String);

impl SamplingPolicy for AlwaysSample {
    fn evaluate(&self, _trace: &BufferedTrace) -> Decision {
        Decision::Sample
    }
    fn name(&self) -> &str {
        &self.0
    }
}

/// Match span status codes.
struct StatusCode {
    name: String,
    status_codes: Vec<String>,
}

impl SamplingPolicy for StatusCode {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision {
        for span_event in &trace.spans {
            if let crate::event::Event::Trace(otel_span) = span_event
                && let Some(status) = otel_span.span().status.as_ref()
            {
                let code_str = match status.code {
                    c if c == OtelStatusCode::Ok as i32 => "OK",
                    c if c == OtelStatusCode::Error as i32 => "ERROR",
                    _ => "UNSET",
                };
                if self.status_codes.iter().any(|s| s == code_str) {
                    return Decision::Sample;
                }
            }
        }
        Decision::Pending
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Trace duration exceeds threshold.
struct Latency {
    name: String,
    threshold_ms: u64,
    upper_threshold_ms: Option<u64>,
}

impl SamplingPolicy for Latency {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision {
        let (mut min_start, mut max_end) = (u64::MAX, 0u64);
        for span_event in &trace.spans {
            if let crate::event::Event::Trace(otel_span) = span_event {
                let span = otel_span.span();
                min_start = min_start.min(span.start_time_unix_nano);
                max_end = max_end.max(span.end_time_unix_nano);
            }
        }
        if min_start == u64::MAX {
            return Decision::Pending;
        }
        let duration_ms = (max_end.saturating_sub(min_start)) / 1_000_000;
        if duration_ms >= self.threshold_ms {
            if let Some(upper) = self.upper_threshold_ms
                && duration_ms >= upper
            {
                return Decision::Pending;
            }
            Decision::Sample
        } else {
            Decision::Pending
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Hash-based probabilistic sampling.
struct Probabilistic {
    name: String,
    sampling_percentage: f64,
}

impl SamplingPolicy for Probabilistic {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision {
        // Hash trace_id to get deterministic sampling.
        let hash = crc32fast::hash(&trace.trace_id);
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "sampling percentage in [0,100] scaled to u32 threshold"
        )]
        let threshold = (self.sampling_percentage / 100.0 * f64::from(u32::MAX)) as u32;
        if hash <= threshold {
            Decision::Sample
        } else {
            Decision::Pending
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Token-bucket rate limiting.
/// Uses Mutex for interior mutability because SamplingPolicy requires &self.
/// The transform is single-threaded so the lock is always uncontended (~20ns).
struct RateLimiting {
    name: String,
    spans_per_second: f64,
    state: std::sync::Mutex<RateLimitState>,
}

struct RateLimitState {
    tokens: f64,
    last_refill: std::time::Instant,
}

impl SamplingPolicy for RateLimiting {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision {
        let now = std::time::Instant::now();
        let mut state = self.state.lock().unwrap();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        state.tokens = (state.tokens + elapsed * self.spans_per_second).min(self.spans_per_second);
        state.last_refill = now;

        #[expect(
            clippy::cast_precision_loss,
            reason = "trace span count for rate-limit token bucket; precise for |v| <= 2^53"
        )]
        let span_count = trace.spans.len() as f64;
        if state.tokens >= span_count {
            state.tokens -= span_count;
            Decision::Sample
        } else {
            Decision::Pending
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Sample by span count.
struct SpanCount {
    name: String,
    min_spans: Option<usize>,
    max_spans: Option<usize>,
}

impl SamplingPolicy for SpanCount {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision {
        let count = trace.spans.len();
        let above_min = self.min_spans.is_none_or(|min| count >= min);
        let below_max = self.max_spans.is_none_or(|max| count <= max);
        if above_min && below_max {
            Decision::Sample
        } else {
            Decision::Pending
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Match string attribute value on any span.
struct StringAttribute {
    name: String,
    key: String,
    values: Vec<String>,
    compiled_regexes: Option<Vec<regex::Regex>>,
    invert_match: bool,
}

impl SamplingPolicy for StringAttribute {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision {
        use opentelemetry_proto::tonic::common::v1::any_value::Value as OtelValue;

        let matched = 'outer: {
            for span_event in &trace.spans {
                if let crate::event::Event::Trace(otel_span) = span_event
                    && let Some(v) = otel_span.attribute(&self.key)
                {
                    let s_owned: String;
                    let s: &str = match &v.value {
                        Some(OtelValue::StringValue(s)) => s.as_str(),
                        Some(OtelValue::IntValue(i)) => {
                            s_owned = i.to_string();
                            &s_owned
                        }
                        Some(OtelValue::DoubleValue(d)) => {
                            s_owned = d.to_string();
                            &s_owned
                        }
                        Some(OtelValue::BoolValue(b)) => {
                            s_owned = b.to_string();
                            &s_owned
                        }
                        _ => continue,
                    };
                    let hit = if let Some(regexes) = &self.compiled_regexes {
                        regexes.iter().any(|re| re.is_match(s))
                    } else {
                        self.values.iter().any(|val| val.as_str() == s)
                    };
                    if hit {
                        break 'outer true;
                    }
                }
            }
            false
        };
        let decision = if matched {
            Decision::Sample
        } else {
            Decision::Pending
        };
        if self.invert_match {
            match decision {
                Decision::Sample => Decision::Pending,
                Decision::Pending => Decision::Sample,
                other => other,
            }
        } else {
            decision
        }
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Match numeric attribute in range on any span.
struct NumericAttribute {
    name: String,
    key: String,
    min_value: f64,
    max_value: f64,
}

impl SamplingPolicy for NumericAttribute {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision {
        for span_event in &trace.spans {
            if let crate::event::Event::Trace(otel_span) = span_event
                && let Some(v) = otel_span.attribute(&self.key)
            {
                let num = match &v.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::DoubleValue(d),
                    ) => Some(*d),
                    Some(opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(i)) => {
                        #[expect(
                            clippy::cast_precision_loss,
                            reason = "span attribute int-to-float for numeric range comparison; precise for |v| <= 2^53"
                        )]
                        let v = *i as f64;
                        Some(v)
                    }
                    _ => None,
                };
                if let Some(n) = num
                    && n >= self.min_value
                    && n <= self.max_value
                {
                    return Decision::Sample;
                }
            }
        }
        Decision::Pending
    }
    fn name(&self) -> &str {
        &self.name
    }
}

/// Composite AND: all sub-policies must return Sample.
struct And {
    name: String,
    sub_policies: Vec<Box<dyn SamplingPolicy>>,
}

impl SamplingPolicy for And {
    fn evaluate(&self, trace: &BufferedTrace) -> Decision {
        if self.sub_policies.is_empty() {
            return Decision::Pending;
        }
        for policy in &self.sub_policies {
            match policy.evaluate(trace) {
                Decision::Sample => continue,
                Decision::Drop => return Decision::Drop,
                Decision::Pending => return Decision::Pending,
            }
        }
        Decision::Sample
    }
    fn name(&self) -> &str {
        &self.name
    }
}
