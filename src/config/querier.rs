//! Querier configuration — the top-level `querier:` block.
//!
//! The querier is the stateless read server: it serves the Prometheus/Tempo/
//! Loki + SQL APIs over Parquet via DataFusion. Presence of the `querier:`
//! section starts it (no `role` switch); the compactor is configured
//! separately via [`super::compactor`]. Gated behind the `querier-backend`
//! feature. See `docs/workspace/parquet-backend/`.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use sol_lib::configurable::configurable_component;

/// Querier options (`querier:`) — the read-only HTTP query server.
#[configurable_component(global_option("querier"))]
#[derive(Clone, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct QuerierOptions {
    /// Network address the querier binds to.
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

    /// Default lookback window (seconds) for Prometheus metadata endpoints
    /// (`/labels`, `/label/:name/values`, `/series`) when the request carries no
    /// explicit `start`: the default `start` becomes `now − this` instead of 0
    /// (all history). An explicit client `start` — including `start=0` — always
    /// wins. Default 3 days.
    pub metadata_default_range_secs: u64,

    /// Staleness lookback (seconds) for Prometheus instant queries: an instant
    /// vector at `time` only includes series with a sample in
    /// `[time − this, time]`, and the scan is bounded to that window instead of
    /// all history. Matches Prometheus's 5-minute staleness default (300).
    pub instant_lookback_secs: u64,
}

/// Where the query backend discovers Parquet files written by the codec. Shared
/// by the querier (read) and the [compactor](super::compactor) (read-write).
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

impl_generate_config_from_default!(QuerierOptions);

const DAY: u64 = 86_400;

impl Default for QuerierOptions {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 9009),
            storage: StorageConfig::default(),
            cache: CacheConfig::default(),
            refresh_interval_secs: 15,
            guardrails: GuardrailsConfig::default(),
            metadata_default_range_secs: 3 * DAY,
            instant_lookback_secs: 300, // Prometheus's 5 m staleness window
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
    fn test_querier_options_deserializes_from_yaml() {
        let yaml = r#"
address: "0.0.0.0:9009"
storage:
  path: "/data/parquet"
"#;
        let opts: QuerierOptions =
            serde_yaml::from_str(yaml).expect("querier options should parse");
        assert_eq!(opts.address.port(), 9009);
        assert_eq!(opts.storage.path, PathBuf::from("/data/parquet"));
        assert_eq!(opts.refresh_interval_secs, 15);
        assert_eq!(opts.instant_lookback_secs, 300, "Prometheus 5 m staleness");
    }
}
