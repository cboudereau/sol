//! Compactor configuration — the top-level `compactor:` block.
//!
//! The compactor is the singleton seal → rollup → retention loop (NFR5/NFR6;
//! FR6/FR7). It has no HTTP server. Presence of the `compactor:` section starts
//! it (no `role` switch); the read server is configured separately via
//! [`super::querier`]. Gated behind the `querier-backend` feature. See
//! `docs/workspace/parquet-backend/`.

use sol_lib::configurable::configurable_component;

use super::querier::StorageConfig;

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

impl_generate_config_from_default!(CompactorOptions);

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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
