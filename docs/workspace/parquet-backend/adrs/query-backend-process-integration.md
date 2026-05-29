---
status: draft
---
# Query backend process integration

Addresses: [FR1](../DESIGN.md#fr1), [FR2](../DESIGN.md#fr2), [FR3](../DESIGN.md#fr3), [NFR2](../DESIGN.md#nfr2)

## Problem

The query backend is a long-running HTTP server that serves Prometheus, Tempo, and Loki APIs by reading Parquet files. It does **not** emit events into the Sol pipeline — it answers queries. This breaks Sol's three component archetypes:

- A **source** runs a server but its purpose is to *emit events* into the topology. The query backend emits nothing.
- A **sink** consumes events. The query backend consumes nothing from the pipeline.
- A **transform** maps events to events.

So the query backend does not fit `SourceConfig` / `SinkConfig` / `TransformConfig`. How does it embed in the Sol process?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Model as a `source` with empty/no outputs | Reuses `SourceConfig`, `HttpSource`, registration machinery | Semantically wrong (a source that emits nothing); confuses topology graph, acknowledgements, `outputs()` contract; pollutes `sources::` namespace |
| B. Dedicated top-level config block + embedded server, mirroring `src/api/` | Matches the existing precedent exactly (`api: api::Options` in [config/mod.rs:149](../../../../src/config/mod.rs), started in [app.rs:134 `setup_api`](../../../../src/app.rs)); clean separation from the event pipeline; independent lifecycle | New subsystem to wire (config field, startup, shutdown) — but the `api` server is a copy-paste-shaped template |
| C. Separate binary / subcommand (`sol query`) | Full process isolation | Cannot share the running pipeline's Parquet sink output path config, in-process metrics, or config loading; operationally heavier; duplicates bootstrap |

## Decision

**Option B — a dedicated top-level config block with an embedded server, modelled on `src/api/`.**

Concretely:
- A new module `src/query/` (feature-gated, see [datafusion-table-discovery](./datafusion-table-discovery.md) for the feature flag).
- A `query::Options` config struct, added as `#[cfg(feature = "query-backend")] pub query: query::Options` on the top-level `Config` (mirrors `api: api::Options` at [config/mod.rs:149](../../../../src/config/mod.rs)).
- A `query::Server` with a `start(opts, handle) -> Result<Server>` constructor, launched from a `setup_query(&self, handle)` method on `ApplicationConfig` in [app.rs](../../../../src/app.rs), exactly as `setup_api` launches `api::Server` ([app.rs:134](../../../../src/app.rs)).
- The server holds the Tokio task handle and a shutdown trigger; it is stored on the topology controller alongside `api_server` ([topology/controller.rs:42](../../../../src/topology/controller.rs)) and torn down the same way.

Rationale:
- The `api` GraphQL server is the **exact same shape**: an embedded, optional, config-block-driven HTTP server with its own lifecycle, independent of the event pipeline. Reusing that pattern means zero new architectural concepts.
- The query backend serving Grafana is conceptually a peer of the GraphQL admin API — both are read-only HTTP surfaces over Sol's state.

## Consequences

- The query backend is configured under a `query:` block, e.g. `query: { address: "0.0.0.0:9009", storage: { path: "/var/lib/sol/parquet" } }`.
- Startup/shutdown plumbing is added in `app.rs` and `topology/controller.rs` parallel to the `api` server — these are the only two pipeline-integration touch points.
- HTTP routing uses **warp** filters (the framework already used by `src/api/server.rs` and the OTLP HTTP source), so no new HTTP framework dependency.
- Because it is decoupled from the topology graph, the query backend has no acknowledgement or backpressure contract to satisfy — it only reads finalized Parquet files (consistent with the write-ahead-log non-goal in [DESIGN.md](../DESIGN.md#non-goals)).
- The three APIs (Prometheus/Tempo/Loki) are mounted as sub-routers on the single `query::Server` listener, distinguished by path prefix (`/prometheus`, `/api`, `/loki`).
