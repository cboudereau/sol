---
status: draft
---
# Proto type canonical source

Addresses: [FR1](../DESIGN.md#fr1), [FR2](../DESIGN.md#fr2), [FR3](../DESIGN.md#fr3)

## Problem

Sol has two sets of OTLP proto types generated from identical `.proto` files:
- **Local** (`sol-opentelemetry-proto::proto::*`) — compiled by `build.rs` with `tonic_build`
- **Upstream** (`opentelemetry-proto v0.27`) — from crates.io, aliased as `upstream-opentelemetry-proto`

Converting between them requires encode→bytes→decode per type (3 round-trips per event). Which set should be the single canonical source?

## Options

| Option | Pros | Cons |
|---|---|---|
| A: Upstream as canonical | Zero-change to sol-core event types (already stores upstream); upstream has serde support; tonic 0.12 compatible; eliminates local Rust type generation | Couples tonic version to upstream crate releases; local build.rs still needed for DESCRIPTOR_BYTES |
| B: Local as canonical | Full control over type generation; no external crate dependency coupling | Must add serde derives to local build.rs; must change all sol-core event types from upstream to local; larger refactor; still need upstream crate or manual serde |
| C: Unsafe transmute | No code changes beyond conversion sites; fastest to implement | Fragile — depends on prost generating identical memory layouts; undefined behavior risk; blocks future proto schema divergence |

## Decision

**Option A: Upstream proto types as the single canonical source.**

The decisive factors:
1. `OtelLog`/`OtelSpan`/`OtelMetric` **already store upstream types** — zero changes to sol-core event model
2. The upstream crate **already generates tonic 0.12 service stubs** via `features = ["full"]` — gRPC services can implement upstream traits directly
3. Serde support is **already provided** by the upstream crate — no need to add derives
4. `DESCRIPTOR_BYTES` can still be generated from `.proto` files by a trimmed build.rs (`build_server(false).build_client(false)`)

Option B requires changing the internal type in every event wrapper, every accessor, every transform that pattern-matches on proto fields, and adding serde support manually. Option A requires changing the gRPC service trait implementations and sink export functions — a much smaller surface.

## Consequences

**Easier:**
- Source ingestion becomes zero-copy (proto types from tonic are already the right type)
- Sink export becomes zero-copy (event types are already the right type)
- Buffer codec can use the canonical types directly
- Less code overall (delete proto_convert_* functions, simplify sink export)

**Harder:**
- Tonic version is coupled to the upstream crate's tonic dependency (both 0.12 today)
- Upgrading tonic requires upstream crate to publish a compatible version first
- `DESCRIPTOR_BYTES` still requires a local build.rs (but no Rust type generation)
