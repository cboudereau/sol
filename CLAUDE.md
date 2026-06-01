# CLAUDE.md

## Active workspaces
- [parquet-backend](docs/workspace/parquet-backend/TASKS.md) — read-side query backend (Prometheus/Tempo/Loki + SQL APIs over Parquet via DataFusion) incl. compaction, rollups, telemetry, and end-to-end demo integration. Phase 5 — Session 1 in progress. Front-load gate cleared (datafusion 53 / object_store 0.13 / promql-parser 0.9 pinned; parquet read-interop low-risk). Next: task 1 feature/module/config scaffold + task 14 file-sink layout + task 2 catalog.
