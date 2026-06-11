---
status: draft
---
# Deployment roles and horizontal read scaling

Addresses: [NFR8](../../DESIGN.md#nfr8), [NFR5](../../DESIGN.md#nfr5), [FR5](../../DESIGN.md#fr5)

## Problem

The read path must scale with query concurrency: many dashboards, ~130 queries each per 15s refresh. The earlier draft framed the backend as "single-node, Ballista deferred", which wrongly implied reads do not scale. We must define how the backend scales **out** without rewriting it, while keeping a simple single-node default and without letting queries starve ingestion ([NFR5](../../DESIGN.md#nfr5)).

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Single process only, scale up | Simplest | Cannot meet concurrency demand; one OOM query takes down ingestion |
| B. Ballista (distribute one query across nodes) | Scales a single huge query | Wrong axis — workload is many *small* queries; heavy dependency; deferred |
| C. Stateless querier replicas + optional query-frontend + singleton compactor (the `mimir -target` / InfluxDB-querier model) | Reads scale by replication over shared object storage; roles isolate failure; same binary, config-selected | Must keep queriers stateless and make the compactor a strict singleton |

## Decision

**Option C.** State lives in shared object storage (storage/compute separation), so read scaling is by stateless replication. Three roles, all the same binary selected by config:

- **Querier** — stateless: API translation + DataFusion over shared object storage. Horizontally scalable behind a load balancer. Holds only a per-process cache (best-effort) and discovers files via `resolve_files` (footer `level`/`supersedes`, per [compaction-consistency](../compactor/compaction-consistency.md)) — **not** a blind directory listing (which would double-count raw + compacted).
- **Query-frontend** (optional) — fronts the queriers: time-range splitting ([FR8](../../DESIGN.md#fr8)) and a **shared** result cache (the multi-node form of [FR5](../../DESIGN.md#fr5)); routes shards to queriers and merges.
- **Compactor** — **singleton** standalone `Parquet → compacted Parquet` component (DataFusion): the only writer of compacted/rollup files; seals past partitions, builds rollups, enforces the retention policy ([FR7](../../DESIGN.md#fr7), [compaction-consistency](../compactor/compaction-consistency.md)). Replicating it would race the merges and corrupt output.

Default deployment is **all roles in one process** (single-node): per-process LRU cache, in-process compactor, queries served locally. Scaling out is config, not a rewrite.

**Resource isolation (dual runtime):** ingestion/compaction and query run on separate Tokio runtimes with separate thread/memory budgets. Rule: **ingestion always wins; queries are best-effort** (can queue, time out, or spill). DataFusion per-query memory limit + spill-to-disk + bounded `target_partitions` enforce the query side ([NFR5](../../DESIGN.md#nfr5)).

## Consequences

- Queriers must hold **no authoritative state** — anything cached is reconstructible from object storage. This is a design constraint on every querier component.
- The compactor needs single-owner guarantees (run exactly one; in multi-node, a lease/leader or a dedicated deployment). Out of scope to build leader-election now, but the role boundary must exist so it can be added.
- The result cache is behind the [QueryCache trait](../querier/query-caching-strategy.md): per-process LRU for single-node, shared (Redis/object-store) when a query-frontend is present.
- Single-node simplicity (the "one binary" story) is preserved; horizontal scale is available when needed. Ballista-style single-query distribution remains out of scope.

## Implementation note (reconciliation with what shipped)

"Same binary selected by config" landed as **presence-based config sections**,
not a `role:` field: a process runs a **querier** if `querier:` is present and a
**compactor** if `compactor:` is present (it may run both). There is no role
enum — invalid combinations (a compactor with a query cache, a querier with a
retention policy) are unrepresentable because each section only carries its own
fields. The query-frontend is not yet a separately-deployable section (its
splitting/merge logic lives in the querier path). Config changes to either
section currently require a process restart (reload only re-applies `api:`).
