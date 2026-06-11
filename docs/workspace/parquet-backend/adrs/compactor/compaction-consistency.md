---
status: draft
---
# Compaction consistency: standalone compactor, sealed days, footer provenance

Addresses: [FR7](../../DESIGN.md#fr7), [NFR6](../../DESIGN.md#nfr6), [NFR8](../../DESIGN.md#nfr8)

Refines: [file-layout-and-compaction-strategy](./file-layout-and-compaction-strategy.md), [deployment-roles-and-read-scaling](../shared/deployment-roles-and-read-scaling.md)

## Problem

The gateway flushes one small Parquet file per batch per signal (the demo: a file every ~30s → ~2,880/day/signal). Compaction must merge these into few large sorted files (+ rollups) so queries meet [NFR6](../../DESIGN.md#nfr6). The hard part is **consistency**: while the compactor merges, a querier must read each datum **exactly once** — never double-count (raw inputs *and* their compacted output) and never miss (inputs deleted before output visible) — and we have rejected a transactional catalog (Iceberg/Delta) as a rabbit hole.

## Options

### Where does compaction run?
| Option | Pros | Cons |
|---|---|---|
| A. Background task inside the querier (`src/querier/`) | Fewer moving parts | Couples compaction to query lifecycle; complicates the stateless-querier rule ([NFR8](../../DESIGN.md#nfr8)) |
| B. Make the gateway write big sorted files | "No compaction" | Forces gateway buffering → memory + freshness regression; can't global-sort or roll up; multi-gateway still fragments |
| C. **Standalone Parquet→Parquet compactor component** (DataFusion), singleton role | Gateway stays dumb/low-latency; clean role isolation; reuses file sink + querier catalog; schedulable | One more deployable (mitigated: same binary, a role/cron) |

### How is read/compact consistency achieved (no catalog)?
| Option | Pros | Cons |
|---|---|---|
| D. Mutate input files' state (tag `superseded`) | Intuitive | **Parquet footers are immutable** → rewrite; flipping N tags is not atomic; per-file tag reads at query time |
| E. Sidecar `_sealed` marker + atomic directory swap | Simple | Relies on swap ordering; external convention; rename non-atomic on S3 |
| F. **Supersession metadata in the compacted output's footer** (`level` + covered inputs), written atomically at close | Atomic by construction; self-describing/portable; correctness decoupled from input deletion; generalises to rollup levels | Querier must read compacted footers before pruning raw (cheap — few compacted files) |

## Decision

**Option C + Option F, on a sealed-day cadence.**

1. **Standalone compactor component** (`Parquet in → compacted Parquet out`), DataFusion sort-merge, sharing the querier's table schemas/catalog, run as the singleton compactor role.

2. **Sealed-day cadence**: compact only partitions older than `now − grace` (grace ≥ a few gateway flush intervals, default e.g. 1h). The **current** day stays raw and is scanned directly. This single date boundary also defines the immutable-cache line ([FR8](../../DESIGN.md#fr8)) and tier selection ([FR6](../../DESIGN.md#fr6)) — the compactor never races the gateway because it never touches the active partition.

3. **Footer supersession metadata**: each compacted/rollup file carries, in its Parquet footer key-value metadata, written atomically when the file closes:
   - `sol.compaction.level` — `0`=raw, `1`=day-merge, `2`=week/rollup… (LSM-style)
   - `sol.compaction.supersedes` — the input provenance it replaces (input file ids / partition)
   - `sol.compaction.resolution` — `raw | 5m | 1h | 1d`

   **Querier rule**: read the (few) compacted footers first; for each sub-range pick the **highest-level** file and **skip the inputs it supersedes**. Because the output declares what it supersedes, queriers are correct *even while superseded inputs still exist* — so deleting inputs is pure **GC**, not a consistency-critical step.

4. **Atomic make-visible**: write to a staging name (`*.tmp` / staging dir); the file only becomes a real compacted file once fully written with its footer. On S3 the object PUT is itself the atomic step (strong read-after-write); no rename needed.

5. **Coverage is by provenance, not event-time range**: the marker means "I supersede these inputs", not "I contain all events for [t0,t1)". This keeps late data orthogonal — a late event in a later partition is simply queried there (bounded lateness, hot data being a [non-goal](../../DESIGN.md#non-goals)).

## Consequences

- The compactor is a distinct component/role; queriers stay stateless pure readers ([NFR8](../../DESIGN.md#nfr8)).
- The "catalog" shrinks to per-file footer metadata — no external DB, no Iceberg.
- Recompaction and rollups generalise via `level`: a level-2 file supersedes the level-1 files it merged; the querier always prefers the highest level covering a sub-range.
- Querier opens compacted footers before pruning raw — bounded and cheap (the whole point is that compacted files are few).
- Late data beyond the grace window is not back-filled into sealed partitions in v1 (accepted; an occasional re-seal job is out of scope).
- The gateway is unchanged beyond the optional day-partition path hint ([FR7](../../DESIGN.md#fr7)).
