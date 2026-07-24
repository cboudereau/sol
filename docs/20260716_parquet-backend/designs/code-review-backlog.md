# Parquet-backend — code-review backlog

Findings from the deep review of `git diff main...HEAD` (query backend) + the
uncommitted `querier:`/`compactor:` config refactor. Severity is reviewer's
judgement. "Verified" = confirmed by reading the code (not just flagged).

Status legend: ⬜ open · ✅ fixed · 🔁 partially addressed.

## Status summary (after the fix pass)

| Item | Status | Where |
|---|---|---|
| B1 SQL endpoint lockdown | ✅ fixed | `bcd2ccf95` — `sql_user()` w/ `SQLOptions` (no DDL/DML/stmts) |
| B2 NFR9 byte guardrail | 🔁 mitigated | arbitrary file read/write **closed by B1**; byte-estimate still coarse (DoS-class, not exfil). Real fix = DataFusion memory pool — open |
| B3 cache wiring + NFR5 byte ceiling | ✅ fixed | `6d6f06f0b` — byte weigher, wired from config, `max_entries` dropped |
| B4 / B4b rollup from compacted + idempotent | ✅ fixed | `0f3d87367` |
| H1 regex matchers unanchored | ✅ fixed | `e5ee66178` — `^(?:RE)$` (PromQL + LogQL labels) |
| H2 gc relies on fs mtime | ⬜ deferred | needs a compactor-written marker (larger); mtime caveat documented |
| H3 frontend lookback=0 + dead merge_* | ⬜ deferred | larger refactor; impact narrow (multi-day ranges crossing UTC midnight) |
| H4 querier-backend config not hot-reloaded | ⬜ documented | restart-required noted in deployment-roles ADR; full reload deferred |
| M1 rate/increase/irate ignore `[d]` window | ⬜ deferred | Mimir-parity, larger |
| M2 rate ÷0 on duplicate timestamps | ✅ fixed | `e5ee66178` |
| M3 distinct_json_keys unbounded | ✅ fixed | `bcd2ccf95` — `LIMIT 10_000` |
| M4 `__name__` matchers dropped | ⬜ non-issue | analysis: `{__name__=~…}` already rejected (needs literal name); implicit drop is correct dedup; only nonsensical `foo{__name__=~"other"}` over-broad |
| M5 seal supersede-set TOCTOU | ⬜ deferred | microsecond window, sealed days get no concurrent writes |
| M6 error responses leak engine internals | ⬜ deferred | single-tenant; genericizing would hide useful translation/guardrail errors |
| L2 non-integer topk truncation | ✅ fixed | `e5ee66178` |
| L1/L3/L4/L5 | ⬜ minor | as noted below |

**Datasource-contract fixes (found while polishing the demo, not in the original review):**

| Item | Status | Where |
|---|---|---|
| Tempo search missing `spanSets` (plural) → Grafana 13 Search crash | ✅ fixed | `11c64a8c6` |
| Loki "Logs volume" metric query rejected (→ matrix) | ✅ fixed | `11c64a8c6` |

## BLOCKER

- ✅ **B1 — SQL endpoint runs arbitrary statements.** `handle_sql` → `engine.sql` →
  `ctx.sql()` with no `SQLOptions`. `COPY … TO '/path'` writes any file;
  `CREATE EXTERNAL TABLE … LOCATION '…'` reads any file (bypassing catalog +
  guardrail); `DROP`/`CREATE VIEW` mutate the catalog. `src/querier/sql.rs:85`,
  `src/querier/catalog.rs` (`ctx.sql`). Fix: user path via
  `ctx.sql_with_options(sql, SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false))`.
- 🔁 **B2 — NFR9 byte guardrail unsound/bypassable.** `estimate_scan_bytes` =
  `sql.to_lowercase().contains(signal)` × whole-dir size: 0 bytes for any query
  without those substrings (B1's external-table read is "free"), ignores
  `WHERE`/`LIMIT`/pruning, false-positives on column names. `src/querier/sql.rs:24-34`.
  Fix: enforce limits in DataFusion (memory pool / inspect `ExecutionPlan`); pair
  with B1 to restrict reachable tables.
- ✅ **B3 — Cache config is dead + NFR5 byte ceiling unenforced.**
  `QueryEngine::new` hardcodes `MokaQueryCache::new()`; `ttl_secs`/`max_entries`/
  `max_bytes` inert. No byte weigher → entry-count cap only; `set_cache_memory`
  never called. `src/querier/catalog.rs:354`, `src/querier/cache.rs:74`. Fix:
  `with_params(max_entries, ttl)` + a `.weigher(bytes)` bounded by `max_bytes`.
  (Planned.)
- ✅ **B4 — Rollup is coupled to raw lifecycle (rollup ≠ compactor rule).** FIXED:
  `generate_rollup` now reads `resolve_files(dir)` survivors (compacted +
  non-superseded raw, rollups excluded), so it can always (re)build a day's
  rollup from the compacted daily — independent of raw GC. `src/querier/rollup.rs`.
  Was: raw-only (`!rollup- && !compacted-`), unbuildable once raw was gone.
  **Not** a steady-state bug: `run_once` orders seal → rollup → gc, so a
  continuously-running compactor regenerates the rollup from raw in the same pass
  that later reclaims it. It triggers only when raw is already absent at rollup
  time — rollups enabled *after* a day was sealed+reclaimed, backfill/recovery,
  object-store restore keeping only compacted files, or any change to the
  ordering / `interval` vs `delete_grace_secs` relationship. Symptom: silent gaps
  in coarse-`step` long-range queries (the day is missing from the `metrics_*`
  tier). Severity HIGH (robustness/coupling, narrow trigger), not steady-state
  data loss. Fix: read `resolve_files(dir)` survivors (compacted + non-superseded
  raw) — decouples rollup from raw and enables the idempotency skip below. NB the
  *earlier* facet (rollups swept into the seal → churn/corruption) was fixed in
  `a57ad3b82`; this raw-only-input facet was not.

- ✅ **B4b — Rollup regenerates every sealed day on every pass (no idempotency).**
  FIXED: `generate_rollup` skips when `rollup-<tier>` is newer than every source
  (`rollup_is_current`, mtime-based); a re-seal bumps the daily's mtime and
  invalidates it. Sealed days are now rolled up once, then skipped.
  `run_once` calls `generate_rollup` for every sealed metric partition × tier
  each pass, unconditionally re-reading + overwriting — ~`retention_days × tiers`
  (≈90) full regenerations every interval, though sealed days never change. Seal
  has a `has_new` guard; rollup has none. `src/querier/rollup.rs`,
  `src/querier/compaction.rs` `run_once`. Fix: skip a partition whose
  `rollup-<tier>` is newer than its source (pairs with B4 — read survivors, then
  compare mtimes). NB rollup does NOT need leveled/multi-pass compaction: one
  file per tier per sealed day, bounded by `retention_days`; no small-file
  accumulation.

## HIGH

- ⬜ **H1 — Regex matchers not anchored.** `=~`/`!~` emit bare
  `regexp_like(x,'<v>')` (DataFusion = substring) while Prometheus/Loki fully
  anchor `^(?:…)$`; e.g. `service_name=~"prod"` wrongly matches `prod-1`.
  `src/querier/prometheus.rs:75-76`, `src/querier/loki.rs:84-85`. (Loki `|~` line
  filters are correctly unanchored — leave them.) Fix: wrap label-matcher regex
  in `^(?:…)$`.
- ⬜ **H2 — `gc_superseded` trusts filesystem mtime.** Orphan-free guarantee
  assumes superseder newer than inputs, but `cp -p`/backup-restore/clock-skew/
  NTP-step break it → premature raw deletion. Also `delete_grace_secs=60` should
  be ≥ `refresh_interval + max_query_secs` (long scans open files lazily at
  execution). `src/querier/compaction.rs` `gc_superseded`. Fix: gate on a
  compactor-written marker, not raw mtime; raise/document the grace bound.
- ⬜ **H3 — Frontend sharding half-wired.** `handle_range` calls
  `split(start,end,0,…)` (lookback hardcoded **0**) → `rate`/`increase`/
  `*_over_time` under-compute at every UTC midnight on multi-day queries; and
  `merge_series`/`merge_topk`/`merge_histogram_quantile` + per-shard `cacheable`
  cache are dead code (handle_range re-merges inline) → historical-shard cache
  never runs, tests give false confidence. `src/querier/prometheus.rs:1037-1082`,
  `src/querier/frontend.rs`. Fix: route through frontend (real lookback + cache),
  or delete the unused pieces and fix lookback inline.
- ⬜ **H4 — Config reload ignores query backend.** `controller.reload()` only
  re-creates `api_server`; `querier`/`compactor` changes are a silent no-op until
  restart. `src/topology/controller.rs`. Fix: respawn `query_servers` on reload,
  or log "restart required".

## MEDIUM

- ⬜ **M1 — `rate`/`increase`/`irate` ignore `[d]`.** All map to one per-sample-
  delta SQL → not Mimir-equivalent (per-second avg over window); `increase`
  returns a rate, not a count. `src/querier/prometheus.rs` `rate_sql`/`lower_call`.
- ⬜ **M2 — `rate_sql` ÷0 on duplicate timestamps** → `inf`/`NaN` into JSON (the
  heatmap path guards `dt>0`, rate doesn't). `src/querier/prometheus.rs:430-433`.
- ⬜ **M3 — `distinct_json_keys` unbounded** on high-cardinality span
  `attributes` (label/tag discovery DoS). `src/querier/prometheus.rs:343`,
  `src/querier/tempo.rs` tags. Fix: `LIMIT` the distinct scan.
- ⬜ **M4 — `__name__` regex/`!=` matchers silently dropped** (`matcher_pred`
  returns `None` for `__name__`). `src/querier/prometheus.rs:60-63`.
- ⬜ **M5 — `seal_partition` supersede-set from a second dir scan** (TOCTOU vs the
  read-set): a late raw could be marked superseded but never merged → data loss
  (sealed-day, narrow window). `src/querier/compaction.rs` `seal_partition`.
- ⬜ **M6 — Error responses leak raw engine internals** to clients.
  `src/querier/routes.rs:45,75-78`. Fix: log full error, return generic message.

## LOW

- ⬜ **L1 — bare `querier:` (null) → `None` but `querier: {}` → server on default
  port** (presence footgun). Doc + maybe warn. `src/config/builder.rs`.
- ⬜ **L2 — non-integer `topk(2.9,…)` truncates silently** (Prometheus errors).
  `src/querier/prometheus.rs:472-480`.
- ⬜ **L3 — `histogram_quantile` first-bucket lower bound hardcoded `0.0`** (wrong
  for negative observations). `src/querier/prometheus.rs:1137`.
- ⬜ **L4 — `run_once` `?` aborts the whole pass on one corrupt partition**
  (self-heals next interval but can wedge GC → unbounded disk).
  `src/querier/compaction.rs` `run_once`. Fix: per-partition error isolation.
- ⬜ **L5 — `gc_retention` leaves empty `<subtype>/` dirs** after leaf removal
  (cosmetic). `src/querier/compaction.rs` `gc_retention`.

## Refuted (investigated, NOT a bug)

- ❌ **Label-name SQL injection via `/label/:name/values`.** `label_lhs` applies
  `esc()` (doubles single quotes); DataFusion SQL string literals don't treat
  backslash as an escape → no breakout. Identifier contexts go through
  `sql_ident` (non-alphanumeric → `_`). Not exploitable. (Label-name validation
  is still nice-to-have hygiene, not a vuln.)

## Already fixed (context)

- ✅ Rollups swept into the daily seal → churn + corrupted metric data
  (`a57ad3b82`).
- ✅ LogQL/TraceQL quoted-string unescape (`d0cbf810f`).
- ✅ Telemetry `sol_` double-prefix (`ada2e9ed9`).

## Verified-correct (looked risky, are fine)

`write_with_provenance` crash-safety (write→fsync→rename→dir-fsync);
`resolve_files` transitive supersession; `collect_stat(false)` fd bound;
`detected_level` OTLP ranges; cache `Arc` sharing + `refresh()` invalidation;
telemetry names unprefixed; `trace_by_id` hex validation; `esc()` value-injection
defense.

## Docs drift (Phase 6 reconcile)

- Stale: the `role:` config mechanism (DESIGN.md, deployment-roles ADR, …) —
  now presence-based `querier:`/`compactor:` sections.
- Missing entirely: intraday leveled compaction (hourly tier),
  `delete_superseded`/disk-reclaim + `delete_grace_secs`, `detected_level`,
  `hour_grace_secs`. Added after the plan was written.
- New decisions to record as ADR/constraint: SQL-endpoint lockdown (`SQLOptions`),
  regex anchoring.
