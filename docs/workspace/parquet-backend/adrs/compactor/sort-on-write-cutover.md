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
- **Cutover risk** — if any input is not actually sorted by the exact key, SPM
  mis-orders **silently**. Mitigated by: codec sort-on-write (all signals), the
  one-shot example for pre-existing raw, and a plan-shape test asserting SPM (not
  SortExec). Querier correctness is unaffected regardless (it filters/sorts in
  queries); only on-disk pruning would degrade.
- **Test contract change** — seal tests that fed *deliberately unsorted* fixtures
  to prove "the seal sorts" must switch to pre-sorted fixtures: the seal no longer
  sorts arbitrary input (that guarantee moves to the codec).
