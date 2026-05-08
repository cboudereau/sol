use metrics::{counter, gauge};
use sol_lib::{
    NamedInternalEvent, configurable::configurable_component, internal_event::InternalEvent,
};

/// Configuration of internal metrics for enrichment memory table.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct InternalMetricsConfig {
    /// Determines whether to include the key tag on internal metrics.
    ///
    /// This is useful for distinguishing between different keys while monitoring. However, the tag's
    /// cardinality is unbounded.
    #[serde(default = "crate::serde::default_false")]
    pub include_key_tag: bool,
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct MemoryEnrichmentTableRead<'a> {
    pub key: &'a str,
    pub include_key_metric_tag: bool,
}

impl InternalEvent for MemoryEnrichmentTableRead<'_> {
    fn emit(self) {
        if self.include_key_metric_tag {
            counter!(
                "memory_enrichment_table_reads_total",
                "key" => self.key.to_owned()
            )
            .increment(1);
        } else {
            counter!("memory_enrichment_table_reads_total",).increment(1);
        }
    }
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct MemoryEnrichmentTableInserted<'a> {
    pub key: &'a str,
    pub include_key_metric_tag: bool,
}

impl InternalEvent for MemoryEnrichmentTableInserted<'_> {
    fn emit(self) {
        if self.include_key_metric_tag {
            counter!(
                "memory_enrichment_table_insertions_total",
                "key" => self.key.to_owned()
            )
            .increment(1);
        } else {
            counter!("memory_enrichment_table_insertions_total",).increment(1);
        }
    }
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct MemoryEnrichmentTableFlushed {
    pub new_objects_count: usize,
    pub new_byte_size: usize,
}

impl InternalEvent for MemoryEnrichmentTableFlushed {
    fn emit(self) {
        counter!("memory_enrichment_table_flushes_total",).increment(1);
        #[expect(
            clippy::cast_precision_loss,
            reason = "enrichment table gauge; precise for |v| <= 2^53"
        )]
        let objects_count = self.new_objects_count as f64;
        gauge!("memory_enrichment_table_objects_count",).set(objects_count);
        #[expect(
            clippy::cast_precision_loss,
            reason = "enrichment table gauge; precise for |v| <= 2^53"
        )]
        let byte_size = self.new_byte_size as f64;
        gauge!("memory_enrichment_table_byte_size",).set(byte_size);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct MemoryEnrichmentTableTtlExpired<'a> {
    pub key: &'a str,
    pub include_key_metric_tag: bool,
}

impl InternalEvent for MemoryEnrichmentTableTtlExpired<'_> {
    fn emit(self) {
        if self.include_key_metric_tag {
            counter!(
                "memory_enrichment_table_ttl_expirations",
                "key" => self.key.to_owned()
            )
            .increment(1);
        } else {
            counter!("memory_enrichment_table_ttl_expirations",).increment(1);
        }
    }
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct MemoryEnrichmentTableReadFailed<'a> {
    pub key: &'a str,
    pub include_key_metric_tag: bool,
}

impl InternalEvent for MemoryEnrichmentTableReadFailed<'_> {
    fn emit(self) {
        if self.include_key_metric_tag {
            counter!(
                "memory_enrichment_table_failed_reads",
                "key" => self.key.to_owned()
            )
            .increment(1);
        } else {
            counter!("memory_enrichment_table_failed_reads",).increment(1);
        }
    }
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct MemoryEnrichmentTableInsertFailed<'a> {
    pub key: &'a str,
    pub include_key_metric_tag: bool,
}

impl InternalEvent for MemoryEnrichmentTableInsertFailed<'_> {
    fn emit(self) {
        if self.include_key_metric_tag {
            counter!(
                "memory_enrichment_table_failed_insertions",
                "key" => self.key.to_owned()
            )
            .increment(1);
        } else {
            counter!("memory_enrichment_table_failed_insertions",).increment(1);
        }
    }
}
