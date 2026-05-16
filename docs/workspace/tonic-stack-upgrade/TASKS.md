# Tonic Stack Upgrade — Tasks

Design: [DESIGN.md](./DESIGN.md)

## Analysis

Build: `cargo check --all-features` — verified green (pre-migration baseline)
Test: `make test` — verified green (pre-migration baseline)
Lint: `make check-clippy` — verified green (pre-migration baseline)
Format: `cargo fmt --all --check` — verified green

### Known-failing tests

| Test | Reason | Action |
|---|---|---|
| (none pre-migration) | | |

### Codebase context

- **Rust edition**: 2024, **MSRV**: 1.88, **toolchain**: 1.92
- **Dual-version aliases exist**: `http` (0.2) + `http-1` (1.x), `http-body` (0.4) + `http-body-1` (1.0)
- **tonic `NamedService`**: already imported from `tonic::server::NamedService` — no change needed
- **`http_1::` alias usage**: only 3 files (`internal_events/grpc.rs`, `sinks/opentelemetry/grpc.rs`, `sources/kubernetes_logs/mod.rs`) — rename to `http::`

### Dependency migration map

| Current | Target | Crate change |
|---|---|---|
| `hyper` 0.14.32 | `hyper` 1.x + `hyper-util` 0.1 | Split: hyper is minimal, hyper-util has Client/Server |
| `http` 0.2.9 | `http` 1.x | Same crate, version bump |
| `http-1` alias | (removed) | Alias consumers switch to `http` |
| `http-body` 0.4.6 | `http-body` 1.0 | Same crate, version bump |
| `http-body-1` alias | (removed) | Alias consumers switch to `http-body` |
| `http-body-util` 0.1 | `http-body-util` 0.1 | Already present, no change |
| `tonic` 0.12 | `tonic` 0.13+ | Minor: tower 0.4→0.5 (already 0.5) |
| `tonic-build` 0.12 | `tonic-build` 0.13+ | Matches tonic |
| `tower-http` 0.4.4 | `tower-http` 0.6+ | API changes in trace/classify |
| `axum` 0.6.20 | `axum` 0.7+ | Uses hyper 1.x |
| `warp` 0.3.7 | `warp` 0.4.3 | Uses hyper 1.x |
| `async-graphql` 7.0.17 | `async-graphql` 8.0+ | Required for async-graphql-warp 8.0 |
| `async-graphql-warp` 7.0.17 | `async-graphql-warp` 8.0+ | Required for warp 0.4 |
| `hyper-openssl` 0.9.2 | `hyper-openssl` 0.10.2 | hyper 1.x connector |
| `hyper-proxy` 0.9.1 | `hyper-proxy2` 0.1.0 | Different crate, same API |
| `aws-smithy-runtime` 1.8.3 | `aws-smithy-runtime` 1.11+ | Via aws-smithy-http-client |

### Type migration patterns

| Old (hyper 0.14) | New (hyper 1.x) | Notes |
|---|---|---|
| `hyper::Body` | `http_body_util::Full<Bytes>` | For outgoing request bodies |
| `hyper::Body` (response) | `hyper::body::Incoming` | Incoming response bodies |
| `hyper::body::HttpBody` | `http_body::Body` | Trait, from http-body 1.0 |
| `hyper::body::to_bytes(b)` | `BodyExt::collect(b).await?.to_bytes()` | Body consumption |
| `Body::from(bytes)` | `Full::new(Bytes::from(bytes))` | Body creation |
| `Body::empty()` | `Full::new(Bytes::new())` or `Empty::new()` | Empty body |
| `hyper::Client<C, B>` | `hyper_util::client::legacy::Client<C, B>` | Client type |
| `hyper::client::Builder` | `hyper_util::client::legacy::Builder` | Client builder |
| `Client::builder().build(conn)` | `Client::builder(TokioExecutor::new()).build(conn)` | Requires executor |
| `hyper::Server` | removed — use `hyper_util::server` or warp's server | Server |
| `hyper::server::accept::from_stream` | removed — use `TcpListener` + `hyper_util::server::conn::auto` | Accept stream |
| `make_service_fn` | removed — manual service construction | Service factory |
| `hyper::Error` | `hyper_util::client::legacy::Error` (client) | Error type split |
| `hyper::client::connect::dns::Name` | `tower::util::BoxService` or custom | DNS connector |
| `ProxyConnector<C>` | `hyper_proxy2::ProxyConnector<C>` | Crate rename |
| `HttpsConnector<HttpConnector>` | `hyper_openssl::HttpsConnector<HttpConnector>` (0.10) | Version bump |
| `tower_http::trace::TraceLayer` | same, but 0.6 API | Minor signature changes |
| `warp::Filter` (0.3) | `warp::Filter` (0.4) | API changes TBD |
| `aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder` | `aws_smithy_http_client::HyperClientBuilder` (default-client) | New crate path |

### Requirement traceability

| Area / File | Addresses | Notes |
|---|---|---|
| `Cargo.toml` | All FRs | Dependency version hub |
| `src/http.rs` — `HttpClient<B>` | [FR2](./DESIGN.md#fr2), [FR5](./DESIGN.md#fr5), [FR10](./DESIGN.md#fr10) | Core HTTP client — hyper, openssl, proxy |
| `lib/sol-core/src/config/proxy.rs` | [FR10](./DESIGN.md#fr10) | Proxy config — hyper-proxy → hyper-proxy2 |
| `src/dns.rs` | [FR2](./DESIGN.md#fr2) | DNS connector — hyper client DNS types |
| `src/sources/util/http/prelude.rs` | [FR2](./DESIGN.md#fr2), [FR7](./DESIGN.md#fr7) | HTTP source framework — warp + hyper::Server |
| `src/sources/util/http/*.rs` | [FR7](./DESIGN.md#fr7) | HTTP source utilities — warp types |
| `src/sources/*/` (HTTP sources) | [FR7](./DESIGN.md#fr7) | ~20 source files using warp |
| `src/api/server.rs`, `src/api/handler.rs` | [FR8](./DESIGN.md#fr8) | GraphQL API — async-graphql + warp |
| `src/sinks/util/http.rs` | [FR2](./DESIGN.md#fr2), [FR3](./DESIGN.md#fr3) | Sink HTTP utilities — Body types |
| `src/sinks/*/` (HTTP sinks) | [FR2](./DESIGN.md#fr2), [FR3](./DESIGN.md#fr3) | ~30 sink files using hyper::Body + http 0.2 |
| `src/aws/mod.rs` | [FR9](./DESIGN.md#fr9) | AWS SDK connector — smithy runtime |
| `src/components/validation/resources/http.rs` | [FR6](./DESIGN.md#fr6) | Validation — axum 0.7 |
| `src/internal_events/grpc.rs` | [FR3](./DESIGN.md#fr3) | Remove `http_1::` alias |
| `src/sinks/opentelemetry/grpc.rs` | [FR3](./DESIGN.md#fr3) | Remove `http_1::` alias |
| `src/sources/kubernetes_logs/mod.rs` | [FR3](./DESIGN.md#fr3) | Remove `http_1::` alias |

### Transformations

| Function / Type | Input → Output | Invariant |
|---|---|---|
| `HttpClient::new()` | `(TlsSettings, ProxyConfig) → HttpClient` | Must build `hyper_util::client::legacy::Client` with `TokioExecutor` and `hyper_proxy2::ProxyConnector` |
| `HttpClient::send()` | `Request<B> → Result<Response<Incoming>, HttpError>` | Response body type changes from `hyper::Body` to `hyper::body::Incoming` |
| `build_proxy_connector()` | `(MaybeTlsSettings, ProxyConfig) → ProxyConnector` | Uses `hyper_proxy2` API instead of `hyper_proxy` |
| `HttpSource::run()` | `(addr, tls, filters) → Source` | warp 0.4 server startup replaces `hyper::Server::builder` + `make_service_fn` |
| `create_client()` in `aws/mod.rs` | `(ProxyConfig, TlsSettings) → SharedHttpClient` | Uses `aws_smithy_http_client` default-client instead of `hyper_014::HyperClientBuilder` |
| Body consumption (sinks) | `Response<Body> → Bytes` | `body.collect().await?.to_bytes()` via `BodyExt` instead of `hyper::body::to_bytes()` |

### External dependencies (post-migration)

| Crate | Version | Purpose |
|---|---|---|
| `hyper` | 1.x | Core HTTP types (minimal) |
| `hyper-util` | 0.1 | Client, server utilities, TokioExecutor |
| `http` | 1.x | Request, Response, StatusCode, HeaderMap |
| `http-body` | 1.0 | Body trait |
| `http-body-util` | 0.1 | Full, Empty, BodyExt, BoxBody |
| `tonic` | 0.13+ | gRPC framework |
| `tonic-build` | 0.13+ | gRPC codegen |
| `tower-http` | 0.6+ | HTTP middleware layers |
| `axum` | 0.7+ | HTTP framework (validation component) |
| `warp` | 0.4.3 | HTTP framework (sources, API) |
| `async-graphql` | 8.0+ | GraphQL engine |
| `async-graphql-warp` | 8.0+ | GraphQL warp integration |
| `hyper-openssl` | 0.10.2 | TLS connector |
| `hyper-proxy2` | 0.1.0 | Proxy connector |
| `aws-smithy-runtime` | 1.11+ | AWS SDK runtime |
| `aws-smithy-http-client` | 1.1+ | AWS HTTP client (hyper 1.x) |

## Tasks

### 1. Update Cargo.toml dependencies (All FRs)

**Goal**: Bump all dependency versions in one pass — the big-bang that breaks compilation.
**Constraints**:
- [ADR: migration-strategy](./adrs/migration-strategy.md) — big-bang approach
- Remove `http` 0.2, `http-1` alias, `http-body` 0.4, `http-body-1` alias
- Replace `hyper-proxy` with `hyper-proxy2`
- Add `hyper-util` as workspace dependency
- Update `aws-smithy-runtime` features, add `aws-smithy-http-client`
- All version changes in `[workspace.dependencies]` section
**Tests**: no compilation expected after this task — validation is `cargo metadata` succeeds (dependency resolution)
- `test_cargo_metadata_resolves` — `cargo metadata --format-version 1` exits 0
**Verify**: `cargo metadata --format-version 1 > /dev/null`
**Acceptance criteria**:
- [ ] All dependency versions updated per migration map
- [ ] `http` 0.2 and `http-body` 0.4 removed (not just aliased)
- [ ] `http-1` and `http-body-1` aliases removed from Cargo.toml
- [ ] `hyper-proxy` replaced with `hyper-proxy2`
- [ ] `cargo metadata` resolves successfully
**Depends on**: (none)
**Time-box**: ~30 min

### 2. Migrate core HTTP client — `src/http.rs` ([FR2](./DESIGN.md#fr2), [FR5](./DESIGN.md#fr5), [FR10](./DESIGN.md#fr10))

**Goal**: Migrate the central HTTP client infrastructure that all HTTP sinks depend on.
**Types**: `HttpClient<B>`, `HttpProxyConnector`, `HttpClientFuture` — see type migration patterns
**Constraints**:
- `hyper::Client` → `hyper_util::client::legacy::Client` with `TokioExecutor::new()`
- `hyper::body::Body` / `HttpBody` → `http_body::Body` trait from 1.0
- `hyper_proxy::ProxyConnector` → `hyper_proxy2::ProxyConnector`
- `hyper_openssl::HttpsConnector` 0.9 → 0.10 API
- `hyper::Error` → `hyper_util::client::legacy::Error` for client errors
- `tower_http` 0.4 → 0.6 (TraceLayer, classify imports)
- Response type: `Response<hyper::Body>` → `Response<hyper::body::Incoming>`
- `HttpClient::send()` return type must propagate the Incoming body type change
**Tests**: compilation of `src/http.rs` and dependent modules
- Existing HTTP client tests must pass after migration
**Verify**: `cargo check -p sol --lib 2>&1 | grep -c 'error' | grep -q '^0'` (may still have errors in other files)
**Acceptance criteria**:
- [ ] `HttpClient` uses `hyper_util::client::legacy::Client`
- [ ] `HttpProxyConnector` uses `hyper_proxy2::ProxyConnector`
- [ ] `HttpsConnector` uses `hyper_openssl` 0.10
- [ ] `tower_http` imports updated to 0.6 API
- [ ] No `hyper::Body` imports remain in `src/http.rs`
**Depends on**: task 1
**Time-box**: ~90 min

### 3. Migrate proxy config — `lib/sol-core/src/config/proxy.rs` ([FR10](./DESIGN.md#fr10))

**Goal**: Update proxy configuration to use hyper-proxy2 types.
**Types**: `ProxyConnector`, `Proxy`, `Intercept`, `Custom` from `hyper_proxy2`
**Constraints**:
- Same API surface expected — hyper-proxy2 is a fork with hyper 1.x support
- If API differs, adapt minimally
**Tests**: proxy config unit tests
**Verify**: `cargo check -p sol-core 2>&1 | grep -c 'error'`
**Acceptance criteria**:
- [ ] All `hyper_proxy::` imports replaced with `hyper_proxy2::`
- [ ] Proxy config builds and tests pass
**Depends on**: task 1
**Time-box**: ~20 min

### 4. Migrate DNS connector — `src/dns.rs` ([FR2](./DESIGN.md#fr2))

**Goal**: Update DNS connector that uses `hyper::client::connect::dns::Name`.
**Constraints**:
- In hyper 1.x, the DNS resolver API moved to `hyper-util` or was removed
- May need `tower::Service` based DNS resolver instead
**Tests**: DNS resolution tests if they exist
**Verify**: `cargo check -p sol --lib 2>&1 | grep 'dns.rs' | grep -c 'error'`
**Acceptance criteria**:
- [ ] `src/dns.rs` compiles with hyper 1.x types
- [ ] DNS resolution behavior unchanged
**Depends on**: task 1
**Time-box**: ~30 min

### 5. Remove `http_1::` / `http_body_1::` aliases ([FR3](./DESIGN.md#fr3))

**Goal**: Replace alias imports with direct `http::` / `http_body::` imports now that only v1 exists.
**Files**:
- `src/internal_events/grpc.rs` — `http_1::response::Response` → `http::response::Response`
- `src/sinks/opentelemetry/grpc.rs` — `http_1::Uri` → `http::Uri`
- `src/sources/kubernetes_logs/mod.rs` — `http_1::{HeaderName, HeaderValue}` → `http::{HeaderName, HeaderValue}`
- All other files using `http_1::` or `http_body_1::`
**Constraints**:
- Mechanical find-and-replace
- `http_1` alias no longer exists in Cargo.toml (removed in task 1)
**Tests**: compilation
**Verify**: `grep -rn 'http_1::' src/ | wc -l` returns 0
**Acceptance criteria**:
- [ ] Zero occurrences of `http_1::` in source code
- [ ] Zero occurrences of `http_body_1::` in source code
**Depends on**: task 1
**Time-box**: ~15 min

### 6. Migrate tonic gRPC code ([FR1](./DESIGN.md#fr1))

**Goal**: Update tonic 0.12 → 0.13 breaking changes.
**Constraints**:
- `tonic::server::NamedService` — already correct, no change needed
- Check for interceptor API changes
- Check for `tonic::transport::Channel` API changes (may affect gRPC client construction in `src/sinks/opentelemetry/grpc.rs`)
- tower 0.5 already in use — no tower migration needed
**Tests**: gRPC source and sink tests
**Verify**: `cargo check -p sol --features sources-opentelemetry,sinks-opentelemetry 2>&1 | grep 'tonic' | grep -c 'error'`
**Acceptance criteria**:
- [ ] tonic 0.13 compiles without errors
- [ ] gRPC sources and sinks functional
**Depends on**: task 1, task 5
**Time-box**: ~30 min

### 7. Migrate warp HTTP source framework ([FR2](./DESIGN.md#fr2), [FR7](./DESIGN.md#fr7))

**Goal**: Migrate the core HTTP source framework from warp 0.3 + hyper::Server to warp 0.4.
**Files**: `src/sources/util/http/prelude.rs`, `src/sources/util/http/encoding.rs`, `src/sources/util/http/headers.rs`
**Constraints**:
- `hyper::Server::builder(accept_stream).serve(make_svc)` must be replaced with warp 0.4's server mechanism or `hyper-util` server
- `make_service_fn` removed in hyper 1.x — need alternative service construction
- `warp::service(routes)` — check if warp 0.4 still supports this
- `warp::http::StatusCode` — may change to re-export from http 1.x (should be transparent)
- `warp::http::HeaderMap` — same
- The `MaybeTlsIncomingStream` / `PeerAddr` pattern must still work
- `MaxConnectionAgeLayer` and `build_http_trace_layer` tower layers must compose with new server
**Tests**: any HTTP source that uses the framework
**Verify**: `cargo check -p sol --features sources-http_server 2>&1 | grep 'prelude.rs' | grep -c 'error'`
**Acceptance criteria**:
- [ ] `HttpSource::run()` compiles with warp 0.4 + hyper 1.x
- [ ] No `hyper::Server` or `make_service_fn` usage remains
- [ ] TLS, keepalive, and peer address extraction still work
**Depends on**: task 1, task 2
**Time-box**: ~90 min

### 8. Migrate HTTP source implementations ([FR7](./DESIGN.md#fr7))

**Goal**: Update individual HTTP sources that use warp 0.3 APIs.
**Files** (~20 sources):
- `src/sources/http_server.rs`
- `src/sources/heroku_logs.rs`
- `src/sources/splunk_hec/mod.rs`, `acknowledgements.rs`
- `src/sources/datadog_agent/mod.rs`, `logs.rs`, `metrics.rs`, `traces.rs`
- `src/sources/aws_kinesis_firehose/mod.rs`, `filters.rs`, `handlers.rs`, `errors.rs`
- `src/sources/opentelemetry/http.rs`, `reply.rs`, `status.rs`
- `src/sources/prometheus/pushgateway.rs`, `remote_write.rs`, `scrape.rs`
**Constraints**:
- Most changes are mechanical: warp 0.3 → 0.4 Filter API
- `warp::http::*` re-exports should be transparent (same http 1.x types)
- `warp::reject::Rejection` may have API changes
- `hyper::Request` in `prelude.rs:202` → check warp 0.4 equivalent
**Tests**: HTTP source integration tests
**Verify**: `cargo check -p sol --features sources-http_server,sources-splunk_hec,sources-datadog_agent 2>&1 | grep -c 'error'`
**Acceptance criteria**:
- [ ] All warp-based HTTP sources compile
- [ ] No `warp 0.3` API patterns remain
**Depends on**: task 7
**Time-box**: ~60 min

### 9. Migrate API server — async-graphql + warp ([FR8](./DESIGN.md#fr8))

**Goal**: Update the GraphQL API server to async-graphql 8.0 + warp 0.4.
**Files**: `src/api/server.rs`, `src/api/handler.rs`
**Constraints**:
- `async-graphql` 7 → 8 may have breaking API changes
- `async-graphql-warp` 7 → 8 warp integration may change
- `warp::ws::Ws` (WebSocket) — check warp 0.4 WebSocket API
- `warp::http::Response` — should be transparent
**Tests**: API server tests if they exist
**Verify**: `cargo check -p sol --features api 2>&1 | grep 'api/' | grep -c 'error'`
**Acceptance criteria**:
- [ ] API server compiles with async-graphql 8.0 + warp 0.4
- [ ] GraphQL queries and WebSocket subscriptions compile
**Depends on**: task 1, task 7
**Time-box**: ~45 min

### 10. Migrate HTTP sink utilities ([FR2](./DESIGN.md#fr2), [FR3](./DESIGN.md#fr3), [FR4](./DESIGN.md#fr4))

**Goal**: Update the HTTP sink utility layer that all HTTP sinks depend on.
**Files**: `src/sinks/util/http.rs`
**Constraints**:
- `hyper::Body` → appropriate body type
- `http::Request` / `http::Response` — now http 1.x (should be mostly transparent since http 0.2 and 1.x have similar APIs)
- `http_body::Body` trait — from 1.0 now
- `tower_http` 0.6 API for trace/classify layers
**Tests**: sink utility tests
**Verify**: `cargo check -p sol --features sinks-http 2>&1 | grep 'sinks/util/http' | grep -c 'error'`
**Acceptance criteria**:
- [ ] `src/sinks/util/http.rs` compiles with hyper 1.x types
- [ ] No `hyper::Body` imports remain
**Depends on**: task 2
**Time-box**: ~45 min

### 11. Migrate HTTP sinks ([FR2](./DESIGN.md#fr2), [FR3](./DESIGN.md#fr3))

**Goal**: Update all individual HTTP sink files that use `hyper::Body` and `http 0.2` types.
**Files** (~30 sinks):
- `src/sinks/appsignal/{config,service}.rs`
- `src/sinks/azure_monitor_logs/{service,tests}.rs`
- `src/sinks/clickhouse/{arrow/schema,config}.rs`
- `src/sinks/elasticsearch/{common,config,retry,service}.rs`
- `src/sinks/gcp/{pubsub,stackdriver/logs/config}.rs`
- `src/sinks/gcp_chronicle/chronicle_unstructured.rs`
- `src/sinks/gcs_common/{config,service}.rs`
- `src/sinks/greptimedb/logs/http_request_builder.rs`
- `src/sinks/honeycomb/config.rs`
- `src/sinks/http/{config,service}.rs`
- `src/sinks/influxdb/{mod,metrics}.rs`
- `src/sinks/keep/config.rs`
- `src/sinks/loki/{healthcheck,service}.rs`
- `src/sinks/mezmo.rs`
- `src/sinks/new_relic/{healthcheck,service}.rs`
- `src/sinks/opentelemetry/http.rs`
- `src/sinks/prometheus/{exporter,remote_write/service}.rs`
- `src/sinks/splunk_hec/common/{acknowledgements,util,service}.rs`
- `src/sinks/doris/{service,client}.rs`
- `src/sinks/websocket_server/sink.rs`
**Constraints**:
- Mechanical: `hyper::Body` → new body types
- `hyper::body::to_bytes()` → `BodyExt::collect().await?.to_bytes()`
- `Body::from()` → `Full::from()` or `Full::new()`
- `http::Request` / `http::Response` — version swap (mostly transparent)
- Some sinks use `http_body::Body` trait bounds — update to 1.0
**Tests**: individual sink tests
**Verify**: `cargo check -p sol --all-features 2>&1 | grep 'sinks/' | grep -c 'error'`
**Acceptance criteria**:
- [ ] All HTTP sinks compile
- [ ] No `hyper::Body` imports remain in sink files
- [ ] No `http_body 0.4` trait usage remains
**Depends on**: task 10
**Time-box**: ~90 min

### 12. Migrate AWS connector ([FR9](./DESIGN.md#fr9))

**Goal**: Switch AWS SDK from hyper 0.14 connector to hyper 1.x.
**Files**: `src/aws/mod.rs`, `Cargo.toml` (features already updated in task 1)
**Constraints**:
- `aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder` → new path via `aws-smithy-http-client`
- `http_body::Body` / `http_body::combinators::BoxBody` → http-body 1.0 equivalents
- `http::HeaderMap` — now http 1.x
- `aws_smithy_types::body::SdkBody` — check if API changed
- The `AwsHttpClient` custom wrapper must compile with new types
**Tests**: AWS integration tests (may require credentials — unit tests only in CI)
**Verify**: `cargo check -p sol --features aws-core 2>&1 | grep 'aws/' | grep -c 'error'`
**Acceptance criteria**:
- [ ] `src/aws/mod.rs` compiles with aws-smithy-http-client default-client
- [ ] No `hyper_014` references remain
- [ ] AWS credential provider and signing still work
**Depends on**: task 1, task 2
**Time-box**: ~60 min

### 13. Migrate remaining files ([FR2](./DESIGN.md#fr2), [FR3](./DESIGN.md#fr3), [FR6](./DESIGN.md#fr6))

**Goal**: Fix all remaining compilation errors across scattered files.
**Files**:
- `src/gcp.rs` — `hyper::Body`, `http_body` usage
- `src/providers/http.rs` — `hyper::Body`
- `src/internal_events/http.rs` — http types
- `src/internal_events/http_client.rs` — http types
- `src/common/http/server_auth.rs` — http types
- `src/test_util/http.rs` — http types
- `src/transforms/aws_ec2_metadata.rs` — `hyper::Body`, warp (test only)
- `src/docker.rs` — `http::uri::Uri`
- `src/sources/gcp_pubsub.rs` — `http::uri::*`
- `src/sources/okta/client.rs` — http types
- `src/sources/aws_ecs_metrics/mod.rs` — `http_body`
- `src/sources/eventstoredb_metrics/mod.rs` — `http_body`
- `src/sources/nginx_metrics/mod.rs` — `http_body`
- `lib/sol-config/src/http.rs` — `http::StatusCode`
- `src/components/validation/resources/http.rs` — axum 0.6 → 0.7, `http_body`
- `src/components/validation/util.rs` — `http::Uri`
**Constraints**:
- Most are mechanical http 0.2 → 1.x type changes
- axum 0.6 → 0.7 in validation component
- Some files use `http_body 0.4` trait bounds
**Tests**: `cargo check --all-features` clean
**Verify**: `cargo check --all-features 2>&1 | grep -c 'error'` returns 0
**Acceptance criteria**:
- [ ] `cargo check --all-features` succeeds with zero errors
- [ ] No `hyper 0.14`, `http 0.2`, or `http-body 0.4` direct usage remains
**Depends on**: tasks 2, 7, 10, 11, 12
**Time-box**: ~60 min

### 14. Cleanup and verification ([NFR2](./DESIGN.md#nfr2), [NFR3](./DESIGN.md#nfr3), [NFR4](./DESIGN.md#nfr4))

**Goal**: Ensure the entire codebase is clean — formatting, linting, tests pass.
**Constraints**:
- `cargo fmt --all` — fix any formatting issues
- `make check-clippy` — fix all clippy warnings
- `make test` — all unit tests pass
- Verify no hyper 0.14 in direct dependencies: `cargo tree -i hyper@0.14` should show only transitive deps (if any)
**Tests**:
- `test_no_hyper_014_direct` — `cargo tree -d 2>&1 | grep 'hyper v0.14'` shows zero direct deps
- `test_no_http_02_direct` — similar for http 0.2
**Verify**: `cargo fmt --all --check && make check-clippy && make test`
**Acceptance criteria**:
- [ ] `cargo fmt --all --check` passes
- [ ] `make check-clippy` passes
- [ ] `make test` passes — all existing tests green
- [ ] No direct dependency on hyper 0.14, http 0.2, http-body 0.4
**Depends on**: task 13
**Time-box**: ~90 min

### 15. Benchmark validation ([NFR2](./DESIGN.md#nfr2))

**Goal**: Run `demo/benchmark` to validate no performance regression and check for undocumented gains.
**Constraints**:
- Build baseline image with current code
- Build upgraded image with migration branch
- Run `bash run.sh` for both
- Compare results — all scenarios at ≥95% of otelcol must remain ≥95%
- Document any throughput changes in DESIGN.md findings
**Tests**: benchmark comparison
**Verify**: benchmark results show no regression
**Acceptance criteria**:
- [ ] All scenarios previously ≥95% of otelcol remain ≥95%
- [ ] Results documented in DESIGN.md
- [ ] If noop-traces-grpc-50k improves, update research findings
**Depends on**: task 14
**Time-box**: ~60 min (execution) + ~30 min (analysis)

## Sessions

### Session 1 — Foundation + Core Infrastructure (~3.5H)
Tasks: 1, 2, 3, 4, 5, 6
**Skills**: `rust-software-engineer`, `tdd`
**Checkpoint**: `cargo check -p sol-core && cargo check -p sol --lib --features sources-opentelemetry,sinks-opentelemetry 2>&1 | grep -c '^error' | head -1` — core crate and gRPC features compile
**Commit point**: no — code won't fully compile yet, but core infrastructure is migrated

### Session 2 — HTTP Sources + API Server (~3H)
Tasks: 7, 8, 9
**Skills**: `rust-software-engineer`, `tdd`
**Checkpoint**: `cargo check -p sol --features sources-http_server,sources-splunk_hec,sources-datadog_agent,api 2>&1 | grep -c '^error'` — HTTP sources and API compile
**Commit point**: no — sinks still broken

### Session 3 — HTTP Sinks + AWS + Remaining (~3.5H)
Tasks: 10, 11, 12, 13
**Skills**: `rust-software-engineer`, `tdd`
**Checkpoint**: `cargo check --all-features` — full compilation succeeds
**Commit point**: yes — commit "feat: migrate hyper 0.14 → 1.x ecosystem"

### Session 4 — Cleanup + Validation (~2.5H)
Tasks: 14, 15
**Skills**: `rust-software-engineer`, `code-quality`
**Checkpoint**: `cargo fmt --all --check && make check-clippy && make test` — all green
**Commit point**: yes — commit "chore: fix clippy + formatting after hyper migration"

## Quality gates (post-session review)

- [ ] Acceptance criteria: all green above
- [ ] Code review: implementation matches [DESIGN.md](./DESIGN.md) intent
- [ ] Code organization: file placement, module structure, naming conventions (refactoring pass)
- [ ] Code quality: no new complexity, clean types, no duplication
- [ ] Security review: TLS connector still enforces certificate validation, proxy auth unchanged
- [ ] Observability: existing metrics and internal events unchanged
- [ ] Performance: [NFR2](./DESIGN.md#nfr2) — benchmark validation confirms no regression
