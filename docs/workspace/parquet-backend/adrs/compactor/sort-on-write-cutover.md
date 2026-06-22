---
status: draft
---
# Sort-on-write cutover: zero-resort seal merge

Addresses: [FR7](../../DESIGN.md#fr7), [NFR5](../../DESIGN.md#nfr5)

Realises the "Future optimisation" of [bounded-memory-seal-merge](./bounded-memory-seal-merge.md): now that the codec sorts **every** signal on write — metrics via `sort_dp_rows`, logs/traces via `sort_logs`/`sort_spans` (commit `86c5a764b`) — the seal can stop re-sorting.

## Problem

The seal currently does a full (spilling) sort of its inputs, serialised to one
partition to bound the spill reservation. That's correct but slow (~16 min/pass
observed live). Every input is *already* sorted by the read-side key, so the
sort is wasted work — a streaming k-way merge would suffice. But trusting input
order is a cutover: any unsorted file silently mis-merges.

## Decision

1. **Seal lowers to `SortPreservingMergeExec`.** `build_merge_df` reads each
   input as its own scan declaring `file_sort_order`, unions them, and the final
   `.sort()` lowers to a streaming SPM (zero re-sort, `O(k·batch)` memory). The
   sort key is the existing `(service_name, [prom_name], time)` (`prom_name`
   metrics-only; `time` = `time_unix_nano` else `start_time_unix_nano`), all
   `sort(true, false)` = ascending nulls-last — byte-identical to what the codec
   writes (logs' absent time sorts last).
2. **Split `merge_ctx` by partitioning.** Seal uses **default** `target_partitions`
   so the per-file partitions survive for the SPM. The **rollup** keeps
   `target_partitions = 1` (it's a real aggregation/window, not a pre-sorted
   merge — single partition bounds its spill reservation, the earlier OOM fix).
   The bounded `FairSpillPool` + `DiskManager` stay for both (SPM needs little,
   but it's a cheap safety net).
3. **No backward compat, empty-state start.** The seal *trusts* `file_sort_order`;
   there is no defensive re-sort and **no conversion of existing files**. The
   store is started **empty** under the new image, so every file is written by
   the sorting codec from the start — metric raw via `sort_dp_rows`, logs/traces
   via `sort_logs`/`sort_spans`, compacted/rollup via their producers. There are
   no pre-cutover unsorted files to migrate. (A runtime `docker exec` of a
   `cargo run --example` could not work anyway — the slim runtime image carries
   only the `sol` binary, no toolchain/source.)

## Consequences

- **Fast passes** — the seal merge stops buffering/re-sorting; the big logs seal
  becomes a streaming merge. (Rollup cost is unchanged — it's a separate
  aggregation, not addressed here.)
- **SPM memory ∝ fan-in — bounded by a fallback, not just batch size.** A
  `SortPreservingMerge` holds **one batch per input file** at once and **cannot
  spill** those buffers, so its peak RAM ≈ `fan_in × batch_size × row_width`,
  which grows with ingest. Intraday compaction merges a whole hour of raw files
  (≈100s) at once — at the default 8192-row batch it blew the 128 MB pool live
  (2026-06-17 fresh start: `SortPreservingMergeExec` hit ~123 MB and failed).
  A smaller batch (`MERGE_BATCH_SIZE = 1024`) only *raises the ceiling* — it
  doesn't remove the fan-in dependency, so it's not a general bound. The actual
  fix makes peak RAM **independent of fan-in**: `merge_inputs` switches strategy
  at `MAX_SPM_FANIN` (256) — SPM at or below it (fast), and a **bounded spilling
  full-sort** above it (re-sorts, slower, but spills to disk so memory is the
  spillable sort buffer regardless of fan-in). So the merge cannot OOM at any
  ingest rate. The day-seal is low fan-in (~24 hourly L1 files) → always SPM.
- **Rollup spill reservation** — the seal work had inflated
  `sort_spill_reservation_bytes` to `mem/4` (32 MB). The rollup's sort-heavy
  pipeline (sort → window → sort) in the tight pool then couldn't *obtain* that
  reservation → `ExternalSorterMerge` ResourcesExhausted on the largest metric
  daily (2026-06-19 live: `sum` rollup failed, "wants 32 MB, 24 MB free").
  Fixed by reverting to DataFusion's **default reservation (10 MB)** — reliably
  obtainable; DataFusion multi-pass-merges if a single pass is short. (This is
  exactly what DataFusion's own error message advises: *decrease
  `sort_spill_reservation_bytes`*.)
- **Rollup memory — the real fix: hash aggregation, not window+sort.** The
  reservation revert was a band-aid: it still failed live (2026-06-22) on
  `gauge/dt=2026-06-20`, and the evidence proved the plan, not the data, was the
  problem — that partition (6.3M rows, 294 series) failed while the *larger*
  `gauge/dt=2026-06-19` (9.3M rows, same cardinality) had succeeded, and it
  failed even with the system idle over a paused weekend (no external memory
  pressure; a fresh 128 MB pool showed only 5.3 MB free → the rollup's own
  pipeline held ~122 MB). The cause: "last sample per (series, bucket)" was a
  `ROW_NUMBER` window + **two sorts**, which hold ~the whole pool and don't spill
  cleanly. Rewrote `rollup_plan` as a **spillable hash aggregation** —
  `GROUP BY (name, service_name, prom_series_key(attributes), time/resolution)`
  with `last_value(col ORDER BY time)` per remaining column (incl. the
  `attributes` Map, which `last_value` carries by position — never compared, so
  it needs no GROUP BY/Map ordering). DataFusion spills hash aggregates, so
  memory is bounded by group count (cardinality × buckets), not by buffering the
  sorted day. A plan-shape test asserts `AggregateExec` and **not** a window
  operator.
- **Pass resilience** — that one rollup failure aborted the *whole* pass, so GC
  never ran and raw piled up (670+ files/partition). `run_once` is now resilient:
  each per-signal seal/intraday, each rollup tier, and each GC step is logged +
  skipped on error (counted in `report.failures`), never aborting the pass — so
  GC always runs and one bad unit is retried next pass instead of cascading.
- **Cutover risk** — if any input is not actually sorted by the exact key, SPM
  mis-orders **silently**. Mitigated by: codec sort-on-write (all signals), the
  empty-state start (no pre-cutover unsorted files), and a plan-shape test
  asserting SPM (not SortExec). Querier correctness is unaffected regardless (it
  filters/sorts in
  queries); only on-disk pruning would degrade.
- **Test contract change** — seal tests that fed *deliberately unsorted* fixtures
  to prove "the seal sorts" must switch to pre-sorted fixtures: the seal no longer
  sorts arbitrary input (that guarantee moves to the codec).
