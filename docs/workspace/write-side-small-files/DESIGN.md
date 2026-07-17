# write-side-small-files — Design Doc

Amends: [promql-plan-cache](../../20260717_promql-plan-cache/designs/promql-plan-cache.md) — lever 1 of its "Next levers"; shares ownership of the inherited latency NFRs with the row-work levers (E / series-key), which stay out of scope here.

## Context

Live measurement ([VERIFY](../../20260717_promql-plan-cache/VERIFY.md)): every 15-min metrics window contains **~240 files** (`files_opened` p95 = 237) → bare-range floor 304 ms and execute-dominated `rate()` at 370–420 ms. The gateway's six Parquet sinks flush every 30 s (`sol-gateway.yaml`, `batch: {max_events: 5000, timeout_secs: 30}`), ~12–14 files/min.

**Premise correction from exploration**: active-day *closed-hour* compaction already exists and is enabled in the demo — `compact_active_day` (`src/querier/compaction.rs:330-397`) produces `compacted-hHH-<date>.parquet` for hours past `hour_end + hour_grace_secs` (600 s demo), on every compactor tick (`interval_secs: 300` demo). So closed hours already collapse to ~6 files each. The ~240 in-window files are the **current hour's raw tail** — a "last 15 minutes" dashboard window almost never leaves the open hour, and nothing compacts inside it.

Facts the design builds on (all explorer-verified, file:line in the ADR):
- Consistency is name-based, not quiescence-based: closed-window selection by parsed time, atomic staged rename, footer provenance + `resolve_files` supersession, GC only after `delete_grace_secs` (60 s) > querier refresh (15 s). Compacting a dir the gateway is writing is already routine.
- The querier's inventory parser accepts exact-bounds names from *any* writer — a new sub-hour compacted file named `<min_ns>-<max_ns>-<uuid>.parquet` with a level-1 provenance footer is pruning-ready with **zero parser changes**.
- Rollups only run on sealed days (`run_once` grace-gate) and read via `resolve_files` — indifferent to new intra-hour levels.
- Raising the gateway `timeout_secs` is correctness-neutral (bounds are folded from actual rows) — but it directly delays data visibility in Sol, which the side-by-side demo against Mimir would show as a flat tail.

## Functional Requirements

### <a id="fr1"></a>FR1 — Sub-hour chunk compaction of the open hour
The compactor compacts each **closed chunk** of the current hour (chunk length configurable, default 300 s; closed = `now ≥ chunk_end + chunk_grace`) into one exact-bounds-named, level-1, provenance-footered file superseding the raws it absorbs — same machinery, one level below `compacted-hHH`. The later hourly pass absorbs chunk files + leftover raws exactly as it absorbs raws today (supersession is transitive). Chunks are compacted once and never rewritten (write amplification ≈ 2× on hour data, vs ~30× for a rolling partial — see ADR).

### <a id="fr2"></a>FR2 — Compactor cadence fit
The demo compactor tick drops to 60 s (`interval_secs: 300 → 60`) so chunks close promptly; `run_once` stays cheap between events (hourly/seal/GC passes are no-ops most ticks). Config docs updated; `delete_grace_secs` > querier refresh invariant restated where it's defined.

### <a id="fr3"></a>FR3 — Deterministic in-window file-count evidence
A fixture test proves the mechanism arithmetic: with 30 s raw cadence and 300 s chunks, a 15-min window over a compacted current hour resolves to **≤ ~20 files** (3 chunk files + the tail of ≤ chunk+grace raws), vs ~40 uncompacted. Live target after rebuild: `files_opened` p95 ≤ 40 (from 237).

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — In-window file count
Live 15-min metrics window: `files_opened` p95 **≤ 40** (from 237). This is the metric this workspace owns outright.

### <a id="nfr2"></a>NFR2 — Latency movement, honestly shared
Bare-range floor ≤ 150 ms and repeated-shape `rate()` measurably improved under demo load; the inherited ≤ 80 ms / ≤ 0.5 s targets are **jointly owned** with the row-work levers (out of scope here) — this workspace reports its contribution without claiming their share.

### <a id="nfr3"></a>NFR3 — No correctness or freshness regression
`querier::` suite green (baseline 254/0/2); reads-each-datum-once invariant holds across the new level (chunk ∪ raw ∪ hourly); **data visibility latency unchanged** (flush cadence untouched); GC orphan-freedom preserved.

## Non-goals

- **Gateway flush-cadence increase** — evaluated and rejected as the primary lever (ADR): it trades the demo's visible freshness (30 s → 120 s lag vs Mimir side-by-side) for a smaller reduction than chunk compaction achieves freshness-free. Remains a documented knob for deployments that prefer it.
- **Row-work levers** (E — smaller `rate()` lowering; write-side series-key column): separately owned ([promql-plan-cache README](../../20260717_promql-plan-cache/README.md)).
- **Retro-compat**: standing directive — no dual-format paths; chunk files use the existing exact-bounds shape + provenance footers, so no layout change and **no store wipe needed**.
- **Sealed-day/rollup changes**: untouched; rollups never see the active day.

## Rabbit holes

- **Chunk-boundary tuning**: chunk length and grace are two constants; do not build adaptive sizing. Cap: defaults 300 s / 120 s, config-overridable, done.
- **Rewriting chunks as late raws arrive**: don't. A raw landing after its chunk closed just stays raw until the hourly pass absorbs it (exact same lateness story hours already have). Cap: chunks are write-once.
- **Compactor/querier race re-derivations**: the name-based consistency ADR already covers concurrent writers/readers; reuse its arguments, add tests only for the new level's transitivity.

## Design

One new pass inside `compact_active_day`, running before the hourly grouping: group not-yet-superseded raw files of the **current hour** by chunk index (from their exact-bounds `max_ns`), compact each closed chunk with ≥ 2 inputs via the existing `merge_inputs` → staged write → `finalize_writer` (level 1, supersedes list), output name = exact-bounds shape from the merged rows. Hourly and daily passes are untouched — they operate on `resolve_files` survivors, which now include chunk files. Querier side: zero changes (parser already prunes exact-bounds names; supersession already dedups; inventory generation bump on churn is the designed path).

Decisions:
- [Open-hour compaction strategy](./adrs/open-hour-chunk-compaction.md) — chunked vs rolling-partial vs cadence-only.

## Cross-cutting Concerns

- **Observability**: existing `sol_compactor_*` metrics cover the new pass (it's the same merge machinery); `files_opened` p95 is the acceptance signal on the querier dashboard.
- **Rollback**: the pass is config-gated (reuse `intraday` or a sibling flag — implementer's choice); disabling it reverts to today's behaviour; chunk files remain valid survivors either way.
- **Verification**: fixture arithmetic test (FR3), then live re-measurement after the user rebuild: files_opened p95, bare floor, rate() probes — same probe set as the two predecessor VERIFYs.
