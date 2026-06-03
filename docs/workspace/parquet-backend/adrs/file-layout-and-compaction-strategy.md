---
status: draft
---
# File layout and compaction strategy

Addresses: [FR7](../DESIGN.md#fr7), [NFR5](../DESIGN.md#nfr5), [NFR6](../DESIGN.md#nfr6)

## Problem

Sol's Parquet codec flushes **one small file per batch per signal type**. At ~100 events/s with a few-second batch interval, this produces hundreds of small files per hour per signal. DataFusion's `ListingTable` must list, open, and parse the footer of every matching file on each query that is not fully pruned. This "small-files problem" is the dominant CPU and latency cost (the [InfluxDB comparison](../DESIGN.md#influxdb-30-iox--fdap-stack--the-reference-use-case) shows InfluxDB built an entire compactor component to solve exactly this; fragmented deployments hit a 432-file query limit).

We must bound file count and per-query scan cost to meet NFR5 (resource budget) and NFR6 (response time), **without** importing InfluxDB's distributed compactor/catalog/GC complexity — Sol is single-node, ~100 events/s.

The decision is fundamentally a set of **cost/latency trade-offs**.

## Options

| Option | Query latency | CPU/memory cost | Complexity |
|---|---|---|---|
| A. No compaction, re-list every query | Poor + degrades unboundedly with file count | Low background, high per-query | Lowest |
| B. Sort-on-write only (no compaction) | Better pruning, but file count still grows | Negligible | Low |
| C. Sort-on-write + lightweight in-process compaction + bounded caches | Good and **bounded** as data grows | Modest background CPU/IO; bounded memory | Medium |
| D. Full InfluxDB-style compactor + catalog + GC services | Best at scale | High; separate services | High — rejected (over-engineered for target) |

## Decision

**Option C.** Three cooperating, individually-configurable mechanisms:

1. **Sort order on write** — rows are written sorted by `service_name`, then `name` (metrics), then `time_unix_nano`. Low-cardinality-first ordering maximises row-group min/max pruning and columnar compression (the InfluxDB 10–100× lever). This is a write-side hint applied by the file sink / codec; the query backend assumes (does not enforce) it.

2. **Lightweight compaction** — *(NB: the "in-process background task" framing below is **superseded** by [compaction-consistency](./compaction-consistency.md) — compaction is now a standalone **singleton** Parquet→Parquet component on a sealed-day cadence. The merge/sort/retention mechanics here still hold; only the "in-process" packaging changed.)* merges small files within a time window into fewer larger sorted files (target file size and trigger thresholds configurable; sensible defaults: merge when >N small files accumulate in a window, target ~one file per signal per window), then deletes the superseded inputs. Implemented with DataFusion (read inputs → sort/merge → write one Parquet) — no new query logic. **Not** a distributed service, catalog, or dedup engine (Sol output is append-only, nothing to dedupe).

3. **Bounded caches** — a Parquet metadata/data cache and the query-result cache ([caching ADR](./query-caching-strategy.md)) share a total memory budget capped by NFR5 (default ≤256 MB). This is the memory⇄latency knob: raise the budget for lower latency, lower it to protect ingestion.

Retention pruning (delete files past a configured age) runs in the same background task.

> **Extended for long-range metrics + standalone compaction** (see [NFR7](../DESIGN.md#nfr7), [long-range-metrics-strategy](./long-range-metrics-strategy.md), [compaction-consistency](./compaction-consistency.md)):
> - Files are written under per-signal/subtype directories with a **time-partitioned sub-path** (`…/logs/dt=YYYY-MM-DD/*.parquet`, `…/metrics/gauge/dt=…/`) so the catalog prunes whole days by path. (Current sink writes flat `…/logs/%Y-%m-%d-%H-%M-%S.parquet`; `dt=` + per-subtype dirs are the proposed hint — see [datafusion-table-discovery](./datafusion-table-discovery.md).)
> - The numbers traces 30d (7d opt-in) / logs 30d / metrics 13mo (2y opt-in) are **query intervals** ([NFR7](../DESIGN.md#nfr7)), *not* retention TTLs. **Retention** (deletion policy) is a separate, configurable per-signal knob enforced by the compactor's GC, ≥ the query interval.
> - For metrics, compaction additionally produces **rollup tiers** (5m/1h/1d, [FR6](../DESIGN.md#fr6)) for the cold tail, storing bucket counts / counter values (not pre-computed quantiles) to keep `histogram_quantile`/`rate` correct.
> - Compaction is a **standalone Parquet→Parquet component**, run as the **singleton** compactor role on a **sealed-day cadence** with footer-provenance consistency — see [compaction-consistency](./compaction-consistency.md), which supersedes the embedded-background-task framing above.

## Consequences

- **The cost/latency balance is explicit and tunable**: compaction interval + target file size + cache budget + refresh interval are the four knobs. Defaults favour co-existing with ingestion (NFR5) over absolute minimum latency; all are configurable per deployment.
- **Bounded query cost as data grows**: file count per query is held roughly constant by compaction, so NFR6 targets do not degrade over time (the failure mode InfluxDB documents).
- **Write amplification**: compaction re-writes data once per merge — modest background CPU/IO, cheap at the demo target, disable-able for write-heavy/low-query deployments.
- **Freshness unchanged**: compaction operates only on finalized files; the flush + refresh interval still defines freshness (hot data remains a [non-goal](../DESIGN.md#non-goals)).
- **Coupling**: sort order is a contract between the write side (codec/sink) and read side. If the sink cannot sort cheaply, compaction still produces sorted output, so pruning benefits are recovered after the first compaction pass even on unsorted input.
- This ADR makes compaction part of the read-backend workspace's scope (FR7). It is sequenced **after** the read path works (so its benefit can be measured), and **before** FR6 pre-computation (which is only justified if compaction + caching still miss NFR6).

## Implementation note (reconciliation with what shipped)

The "lightweight compaction" of Decision §2 landed as **time-tiered leveled
compaction**, not the count-triggered "merge when >N files" sketch:

- **Levels** L0 raw → **L1 hourly** → **L2 daily**, recorded in the footer
  `level`/`supersedes`; `resolve_files` dedups transitively. See [FR7](../DESIGN.md#fr7)
  and the [signal lifecycle](../DESIGN.md#signal-lifecycle) sequence diagram.
- **Intra-day** compaction was added because leaving the **active day** fully raw
  (the original sealed-day-only design) let it accumulate thousands of files and
  exhaust the querier's **file descriptors** (EMFILE). Each *completed* hour of
  the active day is merged into one L1 file (`hour_grace_secs` watermark for late
  data); the in-progress hour stays raw. The querier also registers
  `ListingTable`s with `collect_stat(false)` so it doesn't open every footer at
  plan time.
- **Disk reclaim** is **deferred GC**: superseded inputs are deleted once their
  superseder is older than `delete_grace_secs` (> querier `refresh_interval_secs`,
  so no live registration still points at them), reclaiming disk intra-day rather
  than only at retention. Writes are crash-safe (stage → fsync → rename → dir
  fsync). This realises the "then deletes the superseded inputs" clause safely.
- **Config**: knobs are `compactor.{intraday, grace_days, hour_grace_secs,
  delete_superseded, delete_grace_secs, retention_days, rollups, interval_secs}`.
