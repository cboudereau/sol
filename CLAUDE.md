# CLAUDE.md

## Active workspaces
- [parquet-backend](docs/workspace/parquet-backend/TASKS.md) — read-side query backend (Prometheus/Tempo/Loki + SQL APIs over Parquet via DataFusion) incl. compaction, rollups, telemetry, and end-to-end demo integration. Phase 5 — Session 1 in progress. ✅ Task 1 (feature/config/server/app wiring), ✅ Task 14a (dt= per-signal Parquet layout in demo gateway; 14b codec sort/per-subtype-dir deferred). Next: task 2 (catalog — uses metrics/ union fallback per ADR), then Session-1 checkpoint.
