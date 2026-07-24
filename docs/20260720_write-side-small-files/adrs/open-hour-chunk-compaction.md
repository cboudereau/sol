---
status: accepted
---
# Open-hour compaction: chunked write-once, not rolling-partial, not cadence

Addresses: [FR1](../designs/write-side-small-files.md#fr1), [FR2](../designs/write-side-small-files.md#fr2), [NFR1](../designs/write-side-small-files.md#nfr1), [NFR3](../designs/write-side-small-files.md#nfr3)

## Problem

A "last 15 minutes" window lives in the open hour, where only raw ~30 s flush files exist (~240 in-window live). Closed hours already compact (`compact_active_day`, `src/querier/compaction.rs:330-397`); the open hour has no mechanism. Reduce its file count without regressing freshness (data visibility = raw-file landing, flush-driven) or correctness (reads-each-datum-once via footer supersession, `compaction.rs:794-833`).

## Options

| Option | In-window files (15 m, demo cadence) | Write amplification (hour data) | Freshness | Notes |
|---|---|---|---|---|
| A. **Chunked write-once**: compact each closed chunk (default 300 s, grace 120 s) of the open hour; chunk files exact-bounds-named, level 1, superseding their raws; hourly pass later absorbs chunks + leftover raws (transitive supersession) | ~3 chunks + ≤ 7 min raw tail ≈ **15–20** | ≈ **2×** (each datum: raw → chunk → hourly) | unchanged | Write-once chunks; idempotent (a chunk with < 2 non-superseded inputs is skipped; a compacted chunk's inputs are superseded so it never re-forms) |
| B. Rolling partial: every tick, merge the whole hour-so-far (prior partial + new raws) into one new partial | **~1 + tick-tail ≈ 5–8** | ≈ **30×** (hour data rewritten every tick) | unchanged | Fewest files, but pathological write amp and constant churn of a growing file; supersession chains grow per tick |
| C. Cadence-only: gateway `timeout_secs` 30 → 120 | ~60 | 1× | **degraded**: data visible up to 120 s late — visible as a flat tail in the Sol-vs-Mimir side-by-side | Correctness-neutral (bounds folded from rows, `lib/codecs/.../parquet.rs:3718-3777`) but fails the demo's freshness bar |
| D. Status quo | ~240 | 1× | unchanged | The measured problem |

Supporting facts (explorer-verified): closed-window selection is name-based (`compaction.rs:370` watermark pattern; exact-bounds names carry `max_ns`); staged `.tmp` → atomic rename (`:560-608`); GC deletes only footer-listed inputs after `delete_grace_secs` 60 s > querier refresh 15 s (`config/compactor.rs:48-67`); the querier parser prunes any exact-bounds name with zero changes (`src/querier/inventory.rs:246-260`) and inventory churn is the designed refresh path (`catalog.rs:1068-1087`); rollups never touch the active day (`compaction.rs:506`).

## Decision

**Option A (chunked write-once), with the demo compactor tick at 60 s** (`interval_secs` 300 → 60; `run_once` is cheap between events — hourly/seal/GC are no-ops most ticks). Chunk constants: `chunk_secs = 300`, `chunk_grace_secs = 120`, config-overridable, no adaptive sizing (rabbit-hole cap). Chunk outputs use the **existing exact-bounds name shape** (`<min_ns>-<max_ns>-<uuid>.parquet`) + level-1 provenance footer — no new parse rule, no layout change, no store wipe (standing no-retro-compat directive is satisfied vacuously).

B rejected on write amplification and churn; C rejected as *primary* lever on freshness (stays documented as a deployment knob); expected landing per the arithmetic: ~240 → ~15–20 in-window files, `files_opened` p95 ≤ 40 with margin.

## Consequences

- The supersession lattice gains one level (raw → chunk → hourly → daily); transitivity is already the mechanism (`superseded_inputs` unions all levels) — tests must pin reads-each-datum-once across all four.
- Compactor runs 5× more often in the demo; each open-hour pass merges ≤ ~12 small files — negligible cost, but `sol_compactor_*` counters will show the new cadence.
- A raw landing after its chunk closed stays raw until the hourly pass (bounded by the existing lateness story); chunks never rewrite.
- If a deployment disables the pass (config gate), behaviour reverts to today's; chunk files already written remain valid survivors.
