---
status: accepted
---
# Stored prom_series_key column + write-sort pushdown

Addresses: [FR2](../designs/rate-row-work.md#fr2), [FR3](../designs/rate-row-work.md#fr3), [NFR1](../designs/rate-row-work.md#nfr1), [NFR2](../designs/rate-row-work.md#nfr2)

## Problem

`prom_series_key` is a per-row scalar UDF over the `attributes` MAP (`src/querier/udf.rs:97-131`), evaluated once per scanned row and re-evaluated in every plan node referencing it — the window PARTITION BY (LAG + 6 frame windows), sum-by/topk grouping, `*_over_time`, and the rollup downsample (`rollup.rs:147-149`). DataFusion cannot PARTITION BY a Map, hence the UDF. Separately, the metric scan declares no ordering (`catalog.rs:314-323`), so each window inserts a `SortExec`; the on-disk sort `(service_name, prom_name, time)` has no series-key component to elide it.

## Options

| Option | Removes | Cost |
|---|---|---|
| A. Stored column only | the per-row UDF (all paths) | schema change + write-compute at 5 sites + store wipe |
| B. Stored column **+** write-sort `(service_name, prom_name, prom_series_key, time)` + `with_file_sort_order` | the per-row UDF **and** the per-window SortExec | A's cost + a write-sort change + a declared-ordering-correctness risk |
| C. Do neither; rely on FR1 | — | zero, but leaves the UDF + sort if FR1 misses |

## Decision

**B, gated on FR1's re-profile** — ship only if FR1 alone misses NFR1. The column and the write-sort are one clean cutover (both need the wipe; the sort elision needs the column as a sort-key prefix), so splitting them wastes a wipe. Column value = the exact `series_key_string` output (udf.rs:118-131) so read-side grouping is byte-identical to today. `with_file_sort_order` MUST match the codec write sort exactly (a mismatch silently corrupts results — DataFusion trusts the declaration); a test asserts equality and the reads-each-datum-once + parity suites gate it.

Rejected: A alone (wastes the wipe by leaving the sort); C is the "FR1 sufficed" branch, decided by data not fiat.

## Outcome (2026-07-21, task 4) — FR3 sort-elision BLOCKED by a DF-53 limitation

FR2 (stored column, UDF off all partition paths) landed. **FR3's SortExec elision did NOT** — proven empirically, not assumed: the scan advertises the declared order `(name, service_name, prom_series_key, time)` and the window's partition prefix is satisfied, but a `SortExec` survives keyed on `CAST(time_unix_nano AS Int64)` — DataFusion 53 does not treat the Timestamp→Int64 cast (required because the RANGE frame bound is ns/Int64) as order-preserving vs the declared Timestamp ordering. Control: the same window with raw-`time_unix_nano` ORDER BY elides to 0 SortExec — the cast is the sole blocker.

What shipped is the safe, useful subset: the correct `with_file_sort_order` declaration on the metric tables + a drift guard (`test_declared_sort_matches_write_sort` asserts declaration == the authoritative write sort; a false declaration would silently corrupt results, so this is load-bearing). No write-sort change was made (it would only pay off after the cast is removed, and changing it without a matching declaration corrupts).

**Follow-up to elide the sort (deferred, needs its own decision):** materialise `time_unix_nano` as a stored Int64 ns column used by BOTH the declared file sort key and the window ORDER BY (removing the cast), then align the write sort to the partition prefix `(name/service_name, prom_series_key, time_int64)`. That is a further clean-cutover schema change; gated on whether FR2 alone moves live latency enough (task 5).

## Consequences

- Read paths partition on a plain Utf8 column — UDF gone from rate/sum-by/topk/over_time; rollup groups on the column too (`rollup.rs` UDF call sites drop).
- The declared sort order lets DataFusion elide the window `SortExec` for the canonical partition; other orderings (unusual group-bys) still sort — acceptable.
- Schema cutover: new required column in `metric_union_schema()` + codec schema; store wipe; plan cache drops stale plans on the schema/generation change (no dual-format hazard, `plan_cache.rs:316-326`). Logs/traces untouched.
- Write cost: one more computed column + a wider sort key at ingest — the write-side trade the original 7-item analysis (item 7) always anticipated.
