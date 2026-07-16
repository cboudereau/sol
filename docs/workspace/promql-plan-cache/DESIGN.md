# promql-plan-cache — Design Doc

Amends: [backend-metrics-perf](../../20260716_backend-metrics-perf/designs/backend-metrics-perf.md) — inherits its two unmet NFRs.

## Context

backend-metrics-perf delivered FR1–FR5 (file pruning, cache scoping, single-flight, bounded metadata, concurrency guardrail) and live verification decomposed the residual cold-query cost ([VERIFY](../../20260716_backend-metrics-perf/VERIFY.md)): a bare selector range query costs **58 ms** while the same window through `rate()` costs **~250 ms** — so **~190 ms per query is the PromQL window-function plan path** (LAG + six RANGE-frame aggregates, per-row `prom_series_key` UDF partition key, sorts, extrapolation arithmetic — `src/querier/plan/frame.rs`), constant in window width, paid cold on every dashboard refresh. The warm (result-cache) path is 5 ms, proving nothing above the plan is expensive. The RED dashboard burst = 20 × this constant → ~1.4 s wall.

The declared revisit trigger from backend-metrics-perf ("rate() plan cost dominates profiles after FR1") fired; this workspace owns the inherited targets.

**Unknown that gates the mechanism choice**: the ~190 ms is not yet split between (i) logical lowering (Expr/DataFrame construction), (ii) DataFusion optimizer passes, (iii) physical planning, (iv) execution of the window operators. The remedy differs radically per bucket — hence a measurement-first plan (FR1) feeding an ADR, before any caching is built.

## Functional Requirements

### <a id="fr1"></a>FR1 — Plan-pipeline cost profile
Instrumented, reproducible measurement (demo-scale fixture, plus the live demo) splitting the cold `rate()` range-query cost into: PromQL parse, logical-plan lowering, optimizer, physical planning, execution — for `rate()`, a bare selector, and one `histogram_quantile` query. Output: a table in the workspace + the decision data for the [mechanism ADR](./adrs/plan-cache-mechanism.md).

### <a id="fr2"></a>FR2 — Reuse the expensive plan stage across repeated query shapes
Whatever stage(s) FR1 convicts, repeated executions of the same *query shape* (same PromQL expression, step, and table routing — only the time window sliding) must not re-pay it. Candidate mechanisms (ADR decides after FR1): logical-plan template cache with literal rebinding; cached optimized plan with parameter placeholders; optimizer-pass trimming for our plan shapes; pre-built plan fragments for the `rate()` lowering. Correctness bar: results byte-identical to the uncached path; the cache key must capture everything that changes the plan (expression text, step/window shape, resolved table set + inventory snapshot generation, config knobs).

### <a id="fr3"></a>FR3 — Instant-path scan bound
`selector_base_df` / `hist_instant_scan` currently scope `[i64::MIN, time]` — a full-store scan per instant query (measured 385 ms). Bound the lower edge with a staleness lookback (Prometheus semantics: 5 m default), configurable, preserving latest-≤ correctness for series with samples inside the lookback (Prometheus itself returns nothing for staler series).

### <a id="fr4"></a>FR4 — Delete the legacy raw-file margin machinery
No parquet retro-compat (standing directive: clean cutover + store wipe is always sanctioned). Delete the legacy `HH-MM-SS-*` conservative interval rule and the 1 h `INTERVAL_MARGIN_NS` query-time widening outright: intervals are exact-bounds names, `compacted-*`/`rollup-*` (current compactor output), or the unbounded safety fallback; `scoped_files` widens by nothing. Simpler parser, tighter pruning, no dual-path.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Inherited: cold repeated-shape `rate()` query ≤ 80 ms (p95, demo scale)
Relaxed from the predecessor's 50 ms to the measured scan+execute floor (58 ms bare + margin); the first-ever occurrence of a shape after restart may still pay full cost.

### <a id="nfr2"></a>NFR2 — Inherited: 20-query dashboard burst ≤ 0.5 s wall, ≤ 2 core-s (cold, demo hardware)

### <a id="nfr3"></a>NFR3 — No correctness regression
`querier::` suite green (baseline 244/0/2); instant==range parity tests hold; plan-cache hits produce byte-identical responses to misses (dedicated equality tests); Sol↔Mimir live parity unchanged.

## Non-goals

- **In-memory recent-samples buffer** — unchanged from predecessor (architecture change; 60–80 ms cold is indistinguishable for dashboard users, and within ~3× of Mimir's corrected real latency of ~25 ms).
- **Simplified `rate()` lowering (lever b) and write-side `prom_series_key` column (lever c)** — deferred unless FR1's profile shows execution (not planning) dominates; revisit trigger: post-FR2 profile still > NFR1 with planning removed.
- **Loki/Tempo plan caching** — same mechanism would apply, but metrics own the fired trigger; extend later by analogy.
- **Parquet/rollup retro-compat** — permanently out of scope (standing directive): any layout/schema change ships as a clean cutover with a store wipe; no dual-format read paths, no migration code.

## Rabbit holes

- **DataFusion plan-with-placeholders**: `LogicalPlan` supports `$n` placeholders but the optimizer typically runs after binding; do not fight the optimizer to make placeholder plans fully optimizable. Cap: if placeholder binding forces re-optimization, choose a different lever (that's what FR1's data is for).
- **Physical-plan literal rewriting**: rewriting time literals inside an optimized `ExecutionPlan` is version-fragile. Cap: only consider if FR1 shows physical planning (not the optimizer) dominates AND DataFusion 53 exposes a supported rewrite.
- **Cache-key completeness**: an incomplete key serves a stale/wrong plan (worse than slow). Cap: the key components are enumerated in the ADR and each has a test that changing it misses the cache.
- **Profiling scope creep**: FR1 measures five named stages for three query shapes — not a general profiler integration.

## Design

FR1 instruments the existing path (timing spans around parse/lower/optimize/physical/execute in `handle_range`'s pipeline) behind a test/bench seam — no runtime overhead in release paths beyond cheap `Instant` reads. Its output ratifies the [plan-cache mechanism ADR](./adrs/plan-cache-mechanism.md) (drafted with the option space now, decided after FR1 data). FR2 implements the ratified option inside `QueryEngine` beside the existing result cache + single-flight (same `CacheKey` discipline, separate keyspace). FR3/FR4 are small scoped changes in `prometheus.rs` / `inventory.rs` with their own tests, independent of the ADR.

Decisions:
- [Plan-cache mechanism](./adrs/plan-cache-mechanism.md) — `draft` until FR1 data lands, then `proposed` for ratification.

## Cross-cutting Concerns

- **Observability**: `sol_querier_plan_cache_*` hit/miss counters mirroring the result-cache telemetry; the profile table lands in the workspace and the durable docs.
- **Rollback**: FR2 behind the engine boundary — revertable commit; FR3/FR4 independent commits.
- **Verification**: re-run the backend-metrics-perf VERIFY probe set live (same commands) — targets: cold repeated-shape `rate()` ≤ 80 ms, burst ≤ 0.5 s, instant selector ≤ 90 ms.
