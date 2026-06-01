# CLAUDE.md

## Active workspaces
- [parquet-backend](docs/workspace/parquet-backend/TASKS.md) — read-side query backend (Prometheus/Tempo/Loki + SQL APIs over Parquet via DataFusion) incl. compaction, rollups, telemetry, and end-to-end demo integration. Phase 5 — ✅ **Session 1 complete** (tasks 1, 14a, 2; checkpoint green: catalog 5/5, file-sink 13/13, default build OK). Catalog registers logs/traces/metrics(union) over DataFusion; codec↔DataFusion read-back validated. Deferred: 14b (codec per-subtype dirs + sort). **Next: Session 2** — task 3 (LogQL→SQL + Loki query_range), task 4 (Prometheus instant + label/series).
