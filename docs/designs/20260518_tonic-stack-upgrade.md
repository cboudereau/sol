# Tonic Stack Upgrade — Design Doc

**Status: EXPERIMENTAL** — research found no documented performance improvement, but no benchmark was run to verify. The upgrade could reveal undocumented gains — validate with `demo/benchmark`. See [Research findings](#research-findings).

## Context

Amends: [designs/20260513_grpc-stack-tuning.md](../../designs/20260513_grpc-stack-tuning.md)

The [gRPC stack tuning](../../designs/20260513_grpc-stack-tuning.md) work tuned client-side and server-side H2 parameters but could not close the **noop-traces-grpc-50k gap** (Sol 80,451/s vs otelcol 92,135/s = 87%). Post-implementation analysis confirmed the bottleneck is tonic 0.12's server-side H2 throughput ceiling — at 50k+ spans/s over a single H2 connection, Go's gRPC outperforms Rust's tonic 0.12 / hyper 0.14 / h2 0.4.

The project is in a **hybrid dependency state**:

| Layer | Current (hyper 0.14 ecosystem) | Target (hyper 1.x ecosystem) |
|---|---|---|
| tonic | 0.12.3 | 0.13+ |
| hyper | 0.14.32 (pinned, `backports` feature) | 1.x |
| http | 0.2.9 (pinned) + `http-1` alias for 1.3 | 1.x only |
| http-body | 0.4.6 + `http-body-1` alias for 1.0 | 1.0 only |
| h2 | 0.4.11 | 0.4.x (unchanged) |
| tower | 0.5.2 | 0.5.x (unchanged) |
| tower-http | 0.4.4 | 0.6+ |
| axum | 0.6.20 (pinned) | 0.7+ |
| hyper-openssl | 0.9.x | 0.10+ |
| hyper-proxy | 0.9.1 | hyper-proxy2 0.1.0 |
| warp | 0.3.7 | 0.4.3 |
| async-graphql | 7.0.17 | 8.0+ (8.0.0-rc.5 available) |
| async-graphql-warp | 7.0.17 | 8.0+ (8.0.0-rc.5 available) |
| aws-smithy-runtime | 1.8.3 (`connector-hyper-0-14-x`) | 1.11+ via `aws-smithy-http-client` `default-client` |

**Key finding**: tonic 0.12 already uses hyper 1 / http 1 / http-body 1 internally. The tonic 0.12 → 0.13 change is small (tower 0.4 → 0.5, `NamedService` import path). The **real migration** is Sol's direct dependencies on hyper 0.14, http 0.2, http-body 0.4, tower-http 0.4, warp 0.3, and hyper-openssl 0.9.

### Scope assessment (updated with codebase inventory)

| Area | Files affected | Complexity |
|---|---|---|
| tonic gRPC sources/sinks | ~3 | Low — `NamedService` already correct, only `http_1::` alias removal |
| HTTP sinks (`hyper::Body`, `http::Request`) | ~31 | Medium — Body type + http 0.2 → 1.x replacement |
| HTTP client (`src/http.rs`) | 1 | High — core infrastructure, hyper Client + proxy + TLS |
| warp HTTP sources | ~29 | Medium — warp 0.4 API changes, `hyper::Server` removal |
| API server (GraphQL) | ~2 | Medium — async-graphql 8.0 + warp 0.4 |
| tower-http layers | ~3 | Low — API changes |
| axum (validation) | ~1 | Low |
| TLS (hyper-openssl) | ~1 | Medium — 0.9 → 0.10 API change |
| Proxy (hyper-proxy → hyper-proxy2) | ~2 | Medium — crate replacement |
| AWS sinks (smithy) | ~2 | Medium — connector feature + import changes |
| http 0.2 types across codebase | ~34 | Low-Medium — mechanical `http::` → `http::` (version swap) |
| http-body 0.4 usage | ~12 | Medium — trait changes |
| DNS connector | ~1 | Low — `hyper::client::connect::dns::Name` path change |

## Functional Requirements

### <a id="fr1"></a>FR1 — Upgrade tonic to 0.13+

Update tonic dependency and fix breaking changes:
- `NamedService` moved from `tonic::transport` to `tonic::server`
- Interceptor API changes
- tower 0.5 (already used — no change needed)

### <a id="fr2"></a>FR2 — Migrate hyper 0.14 → 1.x

Replace all direct hyper 0.14 usage:
- `hyper::Body` → `http-body-util` types or `BoxBody`
- `hyper::Client` → `hyper_util::client::legacy::Client`
- `hyper::Server` → removed (use `hyper_util::server`)
- Response/Request types use `http 1.x`

### <a id="fr3"></a>FR3 — Unify http/http-body to single version

Remove the dual-version aliases (`http` 0.2 + `http-1` 1.x, `http-body` 0.4 + `http-body-1` 1.0). All code uses http 1.x and http-body 1.0 only.

### <a id="fr4"></a>FR4 — Upgrade tower-http to 0.6+

tower-http 0.4 depends on http 0.2. Upgrade to 0.6+ which supports http 1.x.

### <a id="fr5"></a>FR5 — Upgrade TLS connector

Replace hyper-openssl 0.9 (hyper 0.14) with hyper-openssl 0.10+ (hyper 1.x), or an equivalent TLS connector.

### <a id="fr6"></a>FR6 — Upgrade axum to 0.7+

axum 0.6 depends on hyper 0.14. Upgrade to 0.7+ for hyper 1.x support.

### <a id="fr7"></a>FR7 — Upgrade warp 0.3 → 0.4

warp 0.3.7 depends on hyper 0.14. warp 0.4.3 uses hyper 1.x / http 1.x / http-body 1.x. All HTTP sources built on the warp framework (~29 files) must be migrated. The `hyper::Server` usage in `src/sources/util/http/prelude.rs` is replaced by warp 0.4's built-in server or `hyper-util::server`.

### <a id="fr8"></a>FR8 — Upgrade async-graphql for warp 0.4

async-graphql-warp 7.0.17 depends on warp 0.3. To use warp 0.4, upgrade to async-graphql-warp 8.0+ (currently 8.0.0-rc.5). This cascades to upgrading async-graphql 7.0.17 → 8.0+.

### <a id="fr9"></a>FR9 — Update AWS SDK connector to hyper 1.x

Replace `aws-smithy-runtime` `connector-hyper-0-14-x` feature with `aws-smithy-http-client` `default-client` feature, which uses hyper 1.x + hyper-util. Update `HyperClientBuilder` imports in `src/aws/mod.rs`.

### <a id="fr10"></a>FR10 — Replace hyper-proxy with hyper-proxy2

`hyper-proxy` 0.9.1 has no hyper 1.x support. Replace with `hyper-proxy2` 0.1.0, which provides the same `ProxyConnector` API for hyper 1.x. Update `src/http.rs` and `lib/sol-core/src/config/proxy.rs`.

## Non-Functional Requirements

### <a id="nfr1"></a>~~NFR1 — Close 50k traces gap~~ → moved to Non-goals

~~noop-traces-grpc-50k throughput must reach ≥95% of otelcol (currently 87%). Target: ≥86,000 spans/s.~~ **Invalidated**: [arc-zero-copy gap analysis](../../designs/20260514_arc-zero-copy-optimization.md#noop-traces-grpc-50k-gap-analysis) confirmed the gap is fundamental to h2/tonic's HTTP/2 implementation, not a version issue. Research found no throughput improvement from hyper 1.x or tonic 0.13.

### <a id="nfr2"></a>NFR2 — No regression on existing scenarios

All scenarios currently at ≥95% of otelcol must remain at ≥95%.

### <a id="nfr3"></a>NFR3 — All existing tests pass

All CI checks must pass:
- `cargo fmt --all --check` (fix formatting issues with `cargo fmt`)
- `make check-clippy`
- `make test`
- `make test-component-validation`

### <a id="nfr4"></a>NFR4 — No new dependencies

The upgrade should replace existing crates with their successors (hyper-openssl 0.9 → 0.10), not add new ones beyond what's needed for the migration (e.g., `hyper-util`, `http-body-util` are expected additions).

## Non-goals

- **Rewriting the HTTP client abstraction**: the `HttpClient` wrapper in `src/http.rs` can be updated to use hyper 1.x APIs while keeping the same external interface. No architectural redesign.
- **async-tungstenite / websocket upgrades**: if websocket dependencies pin hyper 0.14, they can temporarily coexist via the existing dual-version pattern until their own upgrade is released.
- **Replacing warp with axum**: warp 0.4.3 supports hyper 1.x. The HTTP source framework stays on warp unless warp 0.4 migration proves unworkable (see Rabbit holes).
- **Closing the noop-traces-grpc-50k gap**: research confirmed the gap is fundamental to h2/tonic vs Go gRPC, not a version issue. See [arc-zero-copy gap analysis](../../designs/20260514_arc-zero-copy-optimization.md#noop-traces-grpc-50k-gap-analysis).
- **Performance tuning beyond the upgrade**: the upgrade is not expected to improve H2 throughput. Connection pooling or upstream h2 changes are separate work.

## Rabbit holes

- ~~**hyper-proxy replacement**~~: **Resolved** — `hyper-proxy2` 0.1.0 exists with `openssl-tls` feature. Drop-in replacement for `hyper-proxy` 0.9.
- ~~**AWS SDK compatibility**~~: **Resolved** — `aws-smithy-http-client` 1.1+ provides `default-client` feature using hyper 1.x + hyper-util. The `hyper-014` feature is the legacy path. Migration: replace `aws-smithy-runtime`'s `connector-hyper-0-14-x` with `aws-smithy-http-client`'s `default-client` + TLS feature.
- **async-graphql 8.0 RC**: `async-graphql-warp` 8.0.0-rc.5 is needed for warp 0.4. No stable release yet. Cap: if RC is too unstable, keep the API server on warp 0.3 (requires allowing warp 0.3 as transitive dep) or replace the GraphQL API integration with async-graphql-axum. Don't rewrite the entire API server.
- **warp 0.4 API changes**: warp 0.4 is a major rewrite using hyper 1.x. The Filter API may have breaking changes beyond simple import updates. Cap: if warp 0.4 requires extensive API rewrites in the HTTP source framework (~29 files), evaluate whether replacing warp with axum is less effort. Don't spend more than 4H on warp 0.4 migration before reassessing.
- **Compile-time explosion**: having both hyper 0.14 and 1.x during migration doubles compile units. Cap: migrate in one pass, don't leave both versions in Cargo.toml longer than needed.

## Design

### Migration strategy

**Big-bang in workspace Cargo.toml, incremental per-crate fixes.** Update the root `Cargo.toml` dependency versions first, then fix compilation errors crate by crate. This is the standard Rust approach for ecosystem-wide version bumps.

### Migration order

1. **Cargo.toml**: bump all dependencies, replace hyper-proxy → hyper-proxy2, remove http/http-body dual aliases, add hyper-util + http-body-util
2. **HTTP client core** (`src/http.rs`): migrate `hyper::Client` → `hyper_util::client::legacy::Client`, `hyper::Body` → `http_body_util` types, `hyper-openssl` 0.10, `hyper-proxy2`
3. **Proxy config** (`lib/sol-core/src/config/proxy.rs`): update hyper-proxy → hyper-proxy2 imports
4. **tonic gRPC code** (~3 files): remove `http_1::` alias usage, update to `http::` directly
5. **warp HTTP sources** (~29 files): warp 0.4 API changes, remove direct `hyper::Server` usage
6. **API server** (~2 files): async-graphql 8.0 + warp 0.4
7. **HTTP sinks** (~31 files): update `hyper::Body` → new body types, `http 0.2` → `http 1.x`
8. **http-body trait usage** (~12 files): `http_body 0.4` → `http_body 1.0` + `http-body-util`
9. **tower-http layers** (~3 files): upgrade to 0.6 API
10. **axum** (~1 file): 0.7 API changes
11. **AWS** (~2 files): `aws-smithy-http-client` `default-client` connector
12. **Cleanup**: remove `http-1`, `http-body-1` aliases, verify no hyper 0.14 transitive deps remain

### Decisions

- [Migration strategy](../adrs/0035-migration-strategy.md)

## Cross-cutting Concerns

- **Feature flags**: some sinks are behind feature flags. Migration must cover ALL enabled features, not just the default build.
- **CI**: the existing CI pipeline (`cargo check`, `cargo clippy`, `cargo test`) validates correctness. No new CI steps needed.
- **Rollback**: if the migration stalls, the workspace can be abandoned — all changes are on a branch. The current hyper 0.14 stack is functional.

## <a id="research-findings"></a>Research findings — upgrade deferred

Research into the upgrade path found **no documented performance improvement** that would justify the migration effort (~60 files, multi-day work):

- **hyper 1.x**: an API redesign, not a performance release. [Issue #3164](https://github.com/hyperium/hyper/issues/3164) reported hyper 1.x being **1.8x slower** than 0.14 in a gateway proxy benchmark. No performance claims in the release notes.
- **h2 0.4.x**: incremental correctness fixes (header decoding, flow control padding, capacity reclamation). Useful but no step-change in throughput. No benchmark numbers cited.
- **tonic 0.13**: primarily a prost 0.13 update. The one meaningful optimization (buffer amortization, PR #1423) is already in tonic 0.12.
- **TechEmpower**: no version-to-version comparison available for hyper.

### Conclusion

The 50k noop-traces gap (87% of otelcol) is not caused by outdated crate versions. It is a fundamental characteristic of the h2/tonic server-side flow control implementation vs Go's gRPC. Upgrading the stack would be a large effort with no expected throughput payoff.

The upgrade remains valuable for **ecosystem hygiene** (removing dual http/http-body versions, staying on supported crates). If attempted, the `demo/benchmark` suite provides a concrete before/after validation — run `bash run.sh` with the current image as baseline, then with the upgraded image to measure actual impact.

### When to revisit

- A new h2 or tonic release cites HTTP/2 throughput improvements with benchmarks
- A dependency (e.g., AWS SDK, tower-http) drops hyper 0.14 support
- The dual http/http-body version situation causes maintainability issues
