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
