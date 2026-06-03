//! Query backend configuration — the top-level `query:` block.
//!
//! Serves Prometheus/Tempo/Loki + SQL APIs over Parquet via DataFusion.
//! Mirrors [`super::api::Options`] / [`super::HealthcheckOptions`]; gated behind
//! the `query-backend` feature. See `docs/workspace/parquet-backend/`.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use sol_lib::configurable::configurable_component;

/// Query backend options.
#[configurable_component(global_option("query"))]
#[derive(Clone, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    /// Whether the query backend is enabled.
    pub enabled: bool,

    /// Network address the query backend binds to.
    #[configurable(metadata(docs::examples = "0.0.0.0:9009"))]
    pub address: SocketAddr,

    /// Parquet storage discovery (local filesystem and/or S3-compatible object store).
    pub storage: StorageConfig,

    /// Query result / metadata cache budget.
    pub cache: CacheConfig,

    /// Interval (seconds) at which the catalog re-lists storage for newly written files.
    pub refresh_interval_secs: u64,

    /// Per-signal query guardrails (max range, max bytes scanned, max concurrency).
    pub guardrails: GuardrailsConfig,

    /// Which role this instance runs: stateless `querier` (HTTP APIs) or the
    /// singleton `compactor` (seal → rollup → retention loop). See the
    /// deployment-roles ADR.
    pub role: QueryRole,

    /// Compactor settings (used when `role = compactor`).
    pub compaction: CompactionConfig,
}

/// The deployment role an instance runs.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueryRole {
    /// Stateless read-only querier serving the HTTP APIs (default).
    #[default]
    Querier,
    /// Singleton compactor: periodically seals sealed-day partitions, generates
    /// metric rollups, and runs retention GC. No HTTP server.
    Compactor,
}

/// Compactor loop settings (NFR5/NFR6; FR6/FR7).
#[configurable_component]
#[derive(Clone, Copy, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct CompactionConfig {
    /// How often (seconds) the compactor runs a seal → rollup → GC pass.
    pub interval_secs: u64,

    /// A partition is sealable once it is at least this many days old (the
    /// active day is never compacted).
    pub grace_days: i64,

    /// Partitions older than this are deleted by retention GC.
    pub retention_days: i64,

    /// Whether to generate metric rollup tiers (5m / 1h / 1d).
    pub rollups: bool,

    /// Whether to compact completed hours within the active (unsealed) day, so
    /// the current day never accumulates thousands of small raw files.
    pub intraday: bool,

    /// Grace before a completed hour is compacted, for late-arriving data. An
    /// hour H is compacted once `now > end(H) + this`.
    pub hour_grace_secs: i64,

    /// Whether to delete raw/lower-level inputs once a compacted file
    /// supersedes them (reclaims disk + inodes intra-day, not just at
    /// retention). Deletion is deferred by `delete_grace_secs` for read safety.
    pub delete_superseded: bool,

    /// How long a superseding compacted file must exist before its inputs are
    /// deleted. MUST exceed the querier `refresh_interval_secs` so no querier
    /// still references the inputs in a registered table.
    pub delete_grace_secs: i64,
}

/// Where the query backend discovers Parquet files written by the codec.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    /// Local filesystem root containing the per-signal Parquet directories.
    pub path: PathBuf,

    /// Optional `object_store` URL (e.g. `s3://bucket/prefix`) overriding `path`.
    #[configurable(metadata(docs::examples = "s3://my-bucket/parquet"))]
    pub url: Option<String>,
}

/// Bounded cache budget (NFR5): result cache + Parquet metadata cache share this ceiling.
#[configurable_component]
#[derive(Clone, Copy, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Total cache memory ceiling, in bytes.
    pub max_bytes: u64,

    /// Result-cache TTL, in seconds (one dashboard refresh cycle by default).
    pub ttl_secs: u64,

    /// Maximum number of cached query results.
    pub max_entries: u64,
}

/// Per-signal query guardrails (NFR9): reject queries beyond these bounds.
#[configurable_component]
#[derive(Clone, Copy, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct GuardrailsConfig {
    /// Max query range for traces, in seconds (default 30d).
    pub traces_max_range_secs: u64,

    /// Max query range for logs, in seconds (default 30d).
    pub logs_max_range_secs: u64,

    /// Max query range for metrics, in seconds (default 13 months).
    pub metrics_max_range_secs: u64,

    /// Max bytes scanned per query before rejection.
    pub max_bytes_scanned: u64,

    /// Max concurrent in-flight queries.
    pub max_concurrent_queries: u64,
}

impl_generate_config_from_default!(Options);

const DAY: u64 = 86_400;

impl Default for Options {
    fn default() -> Self {
        Self {
            enabled: false,
            address: SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 9009),
            storage: StorageConfig::default(),
            cache: CacheConfig::default(),
            refresh_interval_secs: 15,
            guardrails: GuardrailsConfig::default(),
            role: QueryRole::default(),
            compaction: CompactionConfig::default(),
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            interval_secs: 3600, // hourly
            grace_days: 1,       // seal everything before today
            retention_days: 30,
            rollups: true,
            intraday: true,
            hour_grace_secs: 600,    // 10 min for late-arriving data
            delete_superseded: true, // reclaim disk once safely superseded
            delete_grace_secs: 60,   // > querier refresh_interval_secs (15s)
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("/var/lib/sol/parquet"),
            url: None,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024, // 256 MB (NFR5)
            ttl_secs: 15,
            max_entries: 1000,
        }
    }
}

impl Default for GuardrailsConfig {
    fn default() -> Self {
        Self {
            traces_max_range_secs: 30 * DAY,
            logs_max_range_secs: 30 * DAY,
            metrics_max_range_secs: 395 * DAY, // 13 months default
            max_bytes_scanned: 1024 * 1024 * 1024, // 1 GB
            max_concurrent_queries: 16,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_options_deserializes_from_yaml() {
        let yaml = r#"
address: "0.0.0.0:9009"
storage:
  path: "/data/parquet"
"#;
        let opts: Options = serde_yaml::from_str(yaml).expect("query options should parse");
        assert_eq!(opts.address.port(), 9009);
        assert_eq!(opts.storage.path, PathBuf::from("/data/parquet"));
        // Unspecified fields fall back to defaults.
        assert!(!opts.enabled);
        assert_eq!(opts.refresh_interval_secs, 15);
        assert_eq!(opts.cache.max_bytes, 256 * 1024 * 1024);
        assert_eq!(opts.guardrails.metrics_max_range_secs, 395 * DAY);
    }
}
