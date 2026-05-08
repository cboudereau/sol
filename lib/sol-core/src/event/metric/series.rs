use core::fmt;

use sol_common::byte_size_of::ByteSizeOf;

use crate::event::OtelAttributes;

/// Metric identity — the grouping key for metric aggregation.
#[derive(
    Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MetricIdentity {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<OtelAttributes>,
}

impl MetricIdentity {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn name_mut(&mut self) -> &mut String {
        &mut self.name
    }

    pub fn namespace(&self) -> Option<&String> {
        self.namespace.as_ref()
    }

    pub fn namespace_mut(&mut self) -> &mut Option<String> {
        &mut self.namespace
    }

    pub fn tags(&self) -> Option<&OtelAttributes> {
        self.tags.as_ref()
    }

    pub fn tags_mut(&mut self) -> &mut Option<OtelAttributes> {
        &mut self.tags
    }

    pub fn replace_tag(&mut self, key: String, value: impl Into<String>) -> Option<String> {
        let attrs = self.tags.get_or_insert_with(Default::default);
        attrs.replace_string(key, value.into())
    }

    pub fn remove_tags(&mut self) {
        self.tags = None;
    }

    pub fn remove_tag(&mut self, key: &str) -> Option<String> {
        match &mut self.tags {
            None => None,
            Some(attrs) => {
                let old = attrs.remove(key);
                if attrs.is_empty() {
                    self.tags = None;
                }
                old.and_then(|av| match av.value {
                    Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) => Some(s),
                    _ => None,
                })
            }
        }
    }
}

impl ByteSizeOf for MetricIdentity {
    fn allocated_bytes(&self) -> usize {
        self.name.allocated_bytes()
            + self.namespace.allocated_bytes()
            + self
                .tags
                .as_ref()
                .map_or(0, sol_common::byte_size_of::ByteSizeOf::allocated_bytes)
    }
}

impl fmt::Display for MetricIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(namespace) = &self.namespace {
            write!(f, "{namespace}_")?;
        }
        write!(f, "{}", self.name)?;
        write!(f, "{{")?;
        if let Some(tags) = &self.tags {
            let mut first = true;
            for (key, value) in tags.iter_single() {
                if !first {
                    write!(f, ",")?;
                }
                first = false;
                write!(f, "{key}")?;
                if let Some(value) = value {
                    write!(f, "={value:?}")?;
                }
            }
        }
        write!(f, "}}")
    }
}

pub type MetricSeries = MetricIdentity;
