//! Query-backend configuration — the top-level `querier:` and `compactor:`
//! blocks.
//!
//! The query backend serves Prometheus/Tempo/Loki + SQL APIs over Parquet via
//! DataFusion. It runs as one of two components, selected by *which section is
//! present* (no `role` switch):
//!
//! - [`QuerierOptions`] (`querier:`) — the stateless read server (HTTP APIs).
//! - [`CompactorOptions`] (`compactor:`) — the singleton seal → rollup →
//!   retention loop. No HTTP server.
//!
//! An instance may configure either or both. Gated behind the `query-backend`
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
}

/// Compactor options (`compactor:`) — the singleton seal → rollup → retention
/// loop (NFR5/NFR6; FR6/FR7). No HTTP server.
#[configurable_component(global_option("compactor"))]
#[derive(Clone, Debug)]
#[serde(default, deny_unknown_fields)]
pub struct CompactorOptions {
    /// Parquet storage root the compactor reads and writes (read-write).
    pub storage: StorageConfig,

    /// How often (seconds) the compactor runs an intraday → seal → rollup → GC pass.
    pub interval_secs: u64,

    /// A partition is sealable once it is at least this many days old (the
    /// active day is hourly-compacted instead).
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

impl_generate_config_from_default!(QuerierOptions);
impl_generate_config_from_default!(CompactorOptions);

const DAY: u64 = 86_400;

impl Default for QuerierOptions {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 9009),
            storage: StorageConfig::default(),
            cache: CacheConfig::default(),
            refresh_interval_secs: 15,
            guardrails: GuardrailsConfig::default(),
        }
    }
}

impl Default for CompactorOptions {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
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
    fn test_querier_options_deserializes_from_yaml() {
        let yaml = r#"
address: "0.0.0.0:9009"
storage:
  path: "/data/parquet"
"#;
        let opts: QuerierOptions = serde_yaml::from_str(yaml).expect("querier options should parse");
        assert_eq!(opts.address.port(), 9009);
        assert_eq!(opts.storage.path, PathBuf::from("/data/parquet"));
        assert_eq!(opts.refresh_interval_secs, 15);
    }

    #[test]
    fn test_compactor_options_deserializes_from_yaml() {
        let yaml = r#"
storage:
  path: "/data/parquet"
interval_secs: 300
intraday: true
"#;
        let opts: CompactorOptions =
            serde_yaml::from_str(yaml).expect("compactor options should parse");
        assert_eq!(opts.storage.path, PathBuf::from("/data/parquet"));
        assert_eq!(opts.interval_secs, 300);
        assert!(opts.intraday);
        assert!(opts.delete_superseded, "default carried");
    }
}
