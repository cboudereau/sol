# promql-plan-cache — A′ optimized-plan cache + instant lookback + margin deletion

Follow-up to [backend-metrics-perf](../20260716_backend-metrics-perf/README.md), owning its two inherited NFRs. Measurement-first: task 1 profiled the plan pipeline (parse/lower/optimize/physical/execute — the seam ships as `sol_querier_plan_stage_duration_seconds`), and the [mechanism ADR](./adrs/plan-cache-mechanism.md) was ratified on that data (A′+E sequenced; E later skipped-with-numbers on the fixture, then re-fired by live data — see below).

## Delivered (5 tasks, 3 sessions; commits `ca8e03ec0`…`35199163b`)

- **A′ plan cache** (`plan_cache.rs`): caches the post-optimize logical plan keyed by (masked plan shape, step, table set, inventory content-generation, lookback config); on hit, rewrites the structurally-identified window literals AND swaps every `TableScan` to the current scoped provider, then physical-plans directly. Bypass-over-guess with an insert-time identity-rebind self-check. `sol_querier_plan_cache_requests_total{hit|miss|bypass}`. **Verified live: the optimize stage is eliminated** (0.00 on hits; 10.6 ms live mean mixed).
- **FR3 — instant staleness lookback** (`instant_lookback_secs`, default 5 m): instant scans stopped reading the whole store. **Live: 385 ms → 84.5 ms — target met.**
- **FR4 — legacy margin machinery deleted** (standing no-retro-compat directive): `INTERVAL_MARGIN_NS`, the `HH-MM-SS` raw rule, and all query-time widening removed (−100 lines); parse-time bounds only.
- Suite: 244 → 254 passed / 0 failed / 2 ignored; `make check-clippy` green throughout.

## Live verdict ([VERIFY.md](./VERIFY.md))

FR3 met; plan cache + result cache (9 ms sustained) verified live. **NFR1/NFR2 remain unmet and re-fire the deferred levers**: live stage means are `execute` 835 ms / `physical` 122 ms / `optimize` 10.6 ms — execution-bound over **~240 files inside every 15-min window** (the demo now flushes ~14 files/min; the earlier 58 ms floor belonged to a ~90-file store — the floor moved with the store, not the code). The fixture that justified skipping E used 2–5-row files and under-represented execution.

**Next levers, in causal order** (for a future workspace; not opened unilaterally):
1. Write-side small files (original recommendation item 6, deferred twice — now the top lever): gateway flush cadence and/or intra-day compaction of closed hours → ~5–10× fewer in-window files.
2. E — smaller `rate()` lowering (ADR trigger formally re-fired) and the write-side `prom_series_key` column (item 7) — per-row execution cost.
