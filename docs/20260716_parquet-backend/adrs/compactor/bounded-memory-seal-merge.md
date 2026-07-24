---
status: accepted
---
# Bounded-memory seal/merge (streaming, spilling sort)

Addresses: [FR7](../../designs/parquet-backend.md#fr7), [NFR5](../../designs/parquet-backend.md#nfr5)

Extends [file-layout-and-compaction-strategy](./file-layout-and-compaction-strategy.md) and [compaction-consistency](./compaction-consistency.md).

## Problem

The demo compactor was **OOM-killed** (`OOMKilled=true`, exit 137) ~19 min after
midnight — the first sealed-day pass after the previous day completed. The day
seal's `merge_inputs` loaded the **entire partition into RAM twice**:

1. `read_batches` decoded every input file into one `Vec<RecordBatch>`
   (uncompressed Arrow), wrapped in an in-memory `MemTable`;
2. `df.sort(...).collect()` produced a **second** full copy (the sorted result);
3. the `SessionContext` used DataFusion's **default unbounded memory pool with
   no `DiskManager`** — so nothing spilled.

For the demo's largest partition (logs, 164 MB ZSTD-9 → ~1–2.5 GB decoded, ×2)
this exceeded the shared WSL/Docker host budget → the cgroup killed the
compactor. This is the [NFR3](../../designs/parquet-backend.md#nfr3)/no-spill risk made real.

## Options

| Option | Bounded memory? | Correct for unsorted input? | Notes |
|---|---|---|---|
| A. Load-all → sort → collect (status quo) | No | Yes | The OOM. |
| B. True k-way merge over inputs (trust `file_sort_order`) | Yes — `O(k·batch)` | **No** | Requires every input pre-sorted by the merge key. **Only metrics are sorted on write** (`sort_dp_rows` = `(service_name, prom_name, time)`); **logs/traces are not**. A false ordering hint silently mis-orders them, and the file-layout ADR requires the seal to sort *any* input. |
| C. Streaming, **disk-spilling** sort, output streamed to the writer | Yes — capped pool + spill | Yes | DataFusion's external sort spills sorted runs to disk past a memory budget, then k-way merges the runs internally; we consume that merged stream batch-by-batch. |

## Decision

**Option C.** `merge_inputs` now:

- reads each input as a **disk-streaming** `read_parquet` scan (no up-front
  `read_batches` into a `Vec`), unions them, and applies one global `.sort()`;
- runs on a `merge_ctx` `SessionContext` configured with a bounded
  **`FairSpillPool`** (`MERGE_MEM_BUDGET_BYTES`, 128 MB — well under
  [NFR5](../../designs/parquet-backend.md#nfr5)'s cache budget) **plus a `DiskManager`**, so the
  sort spills to disk instead of OOMing. `sort_spill_reservation_bytes` is tied
  to the budget so a small budget can still merge its spilled runs;
- reads all inputs in **one scan as a single partition** (`target_partitions =
  1`). With N input files DataFusion otherwise runs up to `target_partitions`
  (≈ #CPUs) concurrent partition-sorts, **each reserving `sort_spill_reservation_bytes`
  up front** — N × reservation exhausts the pool *before anything can spill*
  (`ResourcesExhausted` on a many-file partition like logs, ~136 files). Serial
  merge is fine for background compaction;
- **streams the sorted output to the `ArrowWriter` batch-by-batch**
  (`execute_stream`), never `collect()`-ing the full result.

Peak RAM is therefore bounded by the budget regardless of how large a sealed day
is. The write path (atomic staging → fsync → rename) is unchanged
(`open_staged_writer`/`finalize_writer`, shared with `write_with_provenance`).

**Why not the true zero-resort k-way merge (Option B):** it needs every input
sorted by the merge key, but only metrics are sorted on write — logs and traces
have no row sort (`parquet.rs` has no `sort_*_rows` for them), and the
file-layout ADR guarantees the seal sorts unsorted input. Trusting a
`file_sort_order` hint there would silently mis-order the output.

**Schema fidelity:** `merge_ctx` sets
`execution.parquet.schema_force_view_types = false`. DataFusion 53 otherwise
coerces `Utf8`→`Utf8View` on read, which would make the compacted file's schema
diverge from the raw files the querier unions it with.

## Rollup path (same fix)

The metric **rollup** (`generate_rollup`) had the identical anti-pattern —
`read_batches` the whole compacted daily into a `Vec`, `MemTable`, downsample,
`collect()` on a default unbounded `SessionContext` — and OOM-killed the demo
compactor a second time (00:18 UTC, *after* the large-logs seal succeeded, while
rolling up the 64 MB `sum`/`gauge` dailies). It now reuses `merge_ctx`
(bounded `FairSpillPool` + `DiskManager` + single partition), reads the
survivors as a streaming `read_parquet` scan, and streams the downsample output
to the writer via `execute_stream` — no `Vec`, no `collect()`. The downsample
plan is shared with the in-memory `rollup_batches` test helper (`rollup_plan`).

## Consequences

- **No more day-seal OOM** — the seal of an arbitrarily large partition stays
  within the budget by spilling; the demo compactor survives the midnight seal.
- **Spill I/O cost** — large seals now write/read temporary spill files. This is
  background work (not latency-sensitive) and far cheaper than crashing; the
  budget knob trades RAM for spill volume.
- **Metrics are re-sorted even though already sorted** — a CPU cost we accept to
  keep one correct path for all signals and honour the sort-any-input guarantee.
- **Passes are slow (memory-for-throughput trade-off).** `target_partitions = 1`
  serialises the merge and every rollup, and spilling adds disk I/O. Verified
  live (2026-06-17 demo seal): a full pass over the sealed day took **~16 min**
  — each rollup tier ~2–3 min — to seal all signals, generate the 5m/1h/1d
  tiers for `gauge`/`sum`/`histogram`, and GC each partition to one file (logs
  142→1, traces 262→1). No OOM, compactor stayed up throughout. This is fine for
  background compaction at the demo cadence (`interval_secs = 300`, work is
  idempotent so a pass simply resumes), but it does **not** scale to high
  ingest. Levers if throughput matters: raise `MERGE_MEM_BUDGET_BYTES` and allow
  `target_partitions > 1` (more concurrent sorts × spill reservation — size the
  budget accordingly), and/or the zero-resort k-way merge below.
- **Future optimisation (now realised):** the codec sorts logs/traces on write
  (`sort_logs`/`sort_spans`), so the seal declares `file_sort_order` per input
  and lowers to a streaming `SortPreservingMergeExec` — a true zero-resort k-way
  merge for *all* signals, dropping the re-sort cost (the seal goes back to
  default `target_partitions`; the rollup keeps `Some(1)`). See
  [sort-on-write-cutover](./sort-on-write-cutover.md) — it trusts input ordering,
  so it's gated on an empty-state start (no backward compat).
- The `merge_ctx` budget and spill reservation are currently module constants;
  promote to `compactor.*` config if deployments need to tune them.
