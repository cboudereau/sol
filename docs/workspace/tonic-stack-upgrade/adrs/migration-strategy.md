---
status: draft
---
# Migration Strategy

Addresses: [FR1](../DESIGN.md#fr1), [FR2](../DESIGN.md#fr2), [FR3](../DESIGN.md#fr3), [FR4](../DESIGN.md#fr4), [FR5](../DESIGN.md#fr5), [FR6](../DESIGN.md#fr6), [FR7](../DESIGN.md#fr7), [FR8](../DESIGN.md#fr8), [FR9](../DESIGN.md#fr9), [FR10](../DESIGN.md#fr10)

## Problem

The codebase has ~100 files depending on the hyper 0.14 ecosystem (hyper, http 0.2, http-body 0.4, warp 0.3, tower-http 0.4, hyper-openssl 0.9, hyper-proxy 0.9). How should the migration to hyper 1.x be executed?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Big-bang Cargo.toml, incremental per-area fixes | Fastest path to single ecosystem; no dual-version maintenance; clear "broken → fixed" progression | Non-compiling state during migration; hard to bisect regressions |
| B. Gradual per-crate migration (keep both ecosystems) | Always compiles; bisectable | Longer dual-version maintenance; complex conditional compilation; already in this state and it's painful |
| C. Replace warp with axum first, then migrate hyper | Eliminates warp dependency risk | Massive scope increase (29 files rewritten); two sequential large migrations instead of one |

## Decision

**Option A — Big-bang Cargo.toml with incremental per-area fixes.**

Rationale:
1. The project is *already* in dual-version state (http 0.2 + `http-1` alias, http-body 0.4 + `http-body-1` alias). This is the pain that motivates the upgrade. Option B extends this pain.
2. The migration is on a branch. If it stalls, the branch is abandoned — no production risk.
3. All target versions are available: warp 0.4.3, hyper-openssl 0.10.2, hyper-proxy2 0.1.0, aws-smithy-http-client 1.1+ with `default-client`.
4. tonic `NamedService` is already imported from `tonic::server::NamedService` — no change needed.

### Sub-decisions

**warp**: upgrade to 0.4.3 (not replace with axum). warp 0.4 uses hyper 1.x natively. Replacing warp with axum would affect 29+ files for no functional gain.

**async-graphql**: upgrade to 8.0.0-rc.5 (both `async-graphql` and `async-graphql-warp`). This is the only path to warp 0.4 compatibility. The RC.5 status indicates the API is stabilizing. Fallback: if RC proves unstable, replace `async-graphql-warp` with `async-graphql-axum` and migrate only the API server (2 files) to axum.

**hyper-proxy**: replace with `hyper-proxy2` 0.1.0 (same `ProxyConnector` API, different crate name).

**AWS SDK connector**: switch from `aws-smithy-runtime` `connector-hyper-0-14-x` to `aws-smithy-http-client` `default-client` feature (uses hyper 1.x + hyper-util). Update to latest aws-smithy-runtime (1.11+) which delegates to aws-smithy-http-client.

**Body type**: replace `hyper::Body` with `http_body_util::Full<Bytes>` for outgoing requests, `BoxBody<Bytes, hyper::Error>` for polymorphic bodies. Use `http_body_util::BodyExt` for body consumption.

## Consequences

- **Easier**: single http/http-body version, no more `http_1::` / `http_body_1::` aliases, stays on maintained crate versions.
- **Harder**: large single PR (~100 files), async-graphql RC dependency until 8.0 stable releases, hyper-proxy2 is a less mature crate than hyper-proxy.
- **Risk**: warp 0.4 or async-graphql 8.0-rc may have undiscovered API changes that increase effort. Mitigated by rabbit hole time-caps in DESIGN.md.
