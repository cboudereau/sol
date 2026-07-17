# CLAUDE.md

## Standing directives
- **No parquet/rollup retro-compat, ever**: any storage layout/schema change ships as a clean cutover (demo store wipe); never write dual-format read paths or migration code.

## Active workspaces
- [promql-plan-cache](docs/workspace/promql-plan-cache/TASKS.md) — **Phase 5, task 2/5 — S2: T2a DONE (A′ plan cache landed), next T2b re-profile (E-if-needed), then T3.** Follow-up to docs/20260716_backend-metrics-perf (integrated): inherits NFR1 (cold repeated-shape rate() ≤ 80 ms) + NFR2 (20-query burst ≤ 0.5 s). Measured: ~190 ms/query = rate() window-fn PLAN constant (bare range 58 ms vs rate() 250 ms, flat in window width; warm 5 ms). FR1 profiles parse/lower/optimize/physical/execute split (the unknown gating the mechanism); ADR plan-cache-mechanism goes proposed with that data — DELIBERATE session gate after S1 T1 for ratification. FR3 bounds instant scans (staleness lookback; instant selector 385 ms → ≤ 90 ms), FR4 removes the double margin for exact-bounds files. Baseline: querier:: 244/0/2, make check-clippy green @ b92b2e624.
  RESUME: load the `my-plan` skill, then read TASKS.md (checked = done) + `git log --oneline`; continue at first unchecked task; re-run the session checkpoint before trusting state.