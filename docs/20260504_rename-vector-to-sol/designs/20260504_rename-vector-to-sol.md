# rename-vector-to-sol — Design Doc

## Context

This project is a fork of [Datadog Vector](https://github.com/vectordotdev/vector), licensed under MPL-2.0. As described in [MARKET.md Section 20](../../otlp-as-core-protocol-plan/MARKET.md), **"Vector" is a trademark of Datadog / Timber Technologies** and the fork must be renamed before any public release or marketing. The chosen name is **Sol** (**S**ingle **O**bservability **L**ayer).

This is a mechanical refactoring: find-and-replace across the codebase, update Cargo.toml manifests, regenerate configs. No functional changes.

### Scope summary (from codebase analysis, updated 2026-05-04)

| Category | Count | Status | Notes |
|----------|-------|--------|-------|
| Cargo.toml packages with "vector" in name | 21 | **DONE** | Renamed in [d1b942da](../../../) |
| Binary targets | 1 | **DONE** | `vector` → `sol` in [d1b942da](../../../) |
| `VECTOR_*` environment variable references | ~325 | **DONE** | All renamed to `SOL_*` in [d1b942da](../../../) |
| `vector_*` internal metrics strings | ~90 | **DONE** | Renamed in [22198c3e](../../../) — registry prepends `sol_`, custom metrics stripped of `vector_` prefix |
| Default config paths (`/etc/vector/`) | ~40 files | **DONE** | All updated to `/etc/sol/` in [d1b942da](../../../) |
| Systemd service files | 3 | **DONE** | Renamed and updated in [d1b942da](../../../) |
| Product name strings ("Vector has started", etc.) | ~10 | **DONE** | All "Sol" in [d1b942da](../../../) |
| `use vector::` / `extern crate vector` imports | ~29 | **DONE** | All `sol`/`sol_*` in [d1b942da](../../../) |
| Lines with "vector" in .rs files | ~2,097 | **DONE** | Crate refs, strings, env vars all renamed in [d1b942da](../../../) |
| Workspace members total | 28 | **DONE** | All paths updated in [d1b942da](../../../) |
| K8s e2e metric names (`vector_started`, function names) | ~20 | **DONE** | Renamed in [d1b942da](../../../) |
| Demo configs | 3 | **DONE** | `demo/otel-sol-grafana-dotnet/sol/` already uses `sol-*` naming |
| Project docs and metadata | 20 files | **DONE** | README, NOTICE, SECURITY, PRIVACY, CONTRIBUTING, templates in [56ea5f85](../../../) |

## Functional Requirements

### <a id="fr1"></a>FR1 — Rename Cargo package names

All Cargo.toml `name` fields containing "vector" must be renamed to use "sol":
- Root: `vector` → `sol`
- Libs: `vector-core` → `sol-core`, `vector-lib` → `sol-lib`, etc.
- VRL crates: `vector-vrl-functions` → `sol-vrl-functions`, etc.
- K8s test binaries: `vector-agent` → `sol-agent`, etc.

### <a id="fr2"></a>FR2 — Rename binary target

The `[[bin]] name = "vector"` entry and `default-run = "vector"` in root Cargo.toml must become `"sol"`.

### <a id="fr3"></a>FR3 — Rename library directory paths

Workspace member paths `lib/vector-*` must be renamed to `lib/sol-*`:
- `lib/vector-core/` → `lib/sol-core/`
- `lib/vector-lib/` → `lib/sol-lib/`
- `lib/vector-vrl/` → `lib/sol-vrl/`
- etc. (15 directories total)

The workspace member list in root Cargo.toml must be updated to reference the new paths.

### <a id="fr4"></a>FR4 — Update all Rust import paths

All `use vector::`, `use vector_core::`, `use vector_lib::`, `extern crate vector`, and similar import statements must reference the renamed crate names (`sol`, `sol_core`, `sol_lib`, etc.).

### <a id="fr5"></a>FR5 — Rename environment variable prefix

All `VECTOR_*` environment variables must be renamed to `SOL_*`:
- `VECTOR_LOG` → `SOL_LOG`
- `VECTOR_LOG_FORMAT` → `SOL_LOG_FORMAT`
- `VECTOR_DATA_DIR` → `SOL_DATA_DIR`
- `VECTOR_APP_NAME` → `SOL_APP_NAME`
- `VECTOR_BUILD_DESC` → `SOL_BUILD_DESC`
- And all test/CI environment variables following the same pattern

### <a id="fr6"></a>FR6 — Rename internal metrics prefix (**DONE**)

Completed in commit [22198c3e](../../../). The metrics registry in `recorder.rs` now prepends `sol_` to all metric names, and custom metrics in `tail_sampling`, `load_balancing` had their `vector_` prefix stripped (the registry adds `sol_` automatically). See [ADR: metrics-namespace-renaming](../adrs/20260504_metrics-namespace-renaming.md).

**Remaining**: `lib/k8s-e2e-tests/src/metrics.rs` still has `vector_started` function names and test strings (`extract_vector_started`, `assert_vector_started`, `wait_for_vector_started`) — these will be renamed as part of [FR4](#fr4) (Rust import/symbol rename).

### <a id="fr7"></a>FR7 — Rename product name strings

All user-visible strings referencing "Vector" as a product name must say "Sol":
- `"Vector has started."` → `"Sol has started."`
- `"Vector has stopped."` → `"Sol has stopped."`
- `"Vector Service"` → `"Sol Service"`
- `"Vector Unit Tests"` → `"Sol Unit Tests"`
- `VECTOR_APP_NAME` default: `"Vector"` → `"Sol"`
- HTTP header `"X-Powered-By": "Vector"` → `"X-Powered-By": "Sol"`

### <a id="fr8"></a>FR8 — Rename config file paths and defaults

- Default config: `/etc/vector/vector.yaml` → `/etc/sol/sol.yaml`
- Config dir: `/etc/vector/` → `/etc/sol/`
- Data dir references: `/var/lib/vector/` → `/var/lib/sol/`
- Config file names in `config/`: `vector.yaml` → `sol.yaml`
- Regression test configs: rename `vector.yaml` files

### <a id="fr9"></a>FR9 — Rename systemd service files

- `distribution/systemd/vector.service` → `distribution/systemd/sol.service`
- `distribution/systemd/hardened-vector.service` → `distribution/systemd/hardened-sol.service`
- `distribution/systemd/vector.default` → `distribution/systemd/sol.default`
- Update all internal references (`ExecStart=/usr/bin/vector` → `ExecStart=/usr/bin/sol`, etc.)

### <a id="fr10"></a>FR10 — Rename Docker and distribution files

- Dockerfile references: `timberio/vector` → appropriate Sol image name
- Docker image names and labels
- Debian package metadata in root Cargo.toml (`[package.metadata.deb]`)
- Install script (`distribution/install.sh`)

### <a id="fr11"></a>FR11 — Update CI/CD workflow files

- GitHub Actions workflow references to "vector" binary, image names, cache keys
- `VECTOR_LOG=vector=debug` → `SOL_LOG=sol=debug` in workflow env vars
- Docker container/volume names in CI

### <a id="fr12"></a>FR12 — Add attribution notice

Per MPL-2.0 best practices, add a clear attribution line:
> "Built on technology originally from the Vector project (MPL-2.0), Copyright Datadog, Inc."

Keep the existing LICENSE file (MPL-2.0) and all copyright headers intact.

### <a id="fr13"></a>FR13 — Update project documentation and metadata

All top-level project documents must reflect the Sol identity:
- **Cargo.toml**: authors, homepage, description (remove Datadog signing keys dep)
- **README.md**: replace Vector marketing copy with Sol project description, keep upstream attribution
- **NOTICE**: add Sol copyright, preserve original Datadog/Vector copyright
- **SECURITY.md**: replace Datadog-specific security policy (email, issue URLs) with Sol's own
- **PRIVACY.md**: replace Vector privacy policy with Sol's data handling stance (no sensitive data: GDPR, PCI-DSS, etc.)
- **CONTRIBUTING.md**: replace Team workflow with Sol contributing guide
- **RELEASES.md / VERSIONING.md**: simplify, remove Vector-specific content

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Build must pass after rename

`cargo build` and `cargo test -p sol` (formerly `cargo test -p vector`) must succeed after the rename. This is the primary validation gate.

### <a id="nfr2"></a>NFR2 — No false renames

The word "vector" as a Rust data structure concept (e.g., `Vec<T>`, "vector of bytes", variable names like `vectors`) must NOT be renamed. Only "vector" as a product/project/crate name is in scope.

### <a id="nfr3"></a>NFR3 — Preserve MPL-2.0 compliance

All existing copyright headers, license notices, and the LICENSE file must remain intact. Only product branding changes — legal notices stay.

### <a id="nfr4"></a>NFR4 — Git history preserved

The rename should be done via `git mv` for directory renames so git tracks file moves.

## Non-goals

- **Functional changes**: No behavior changes, no new features, no refactoring beyond the rename.
- **Website rebuild**: The `website/` directory content is not in scope (it references vector.dev extensively and is a separate effort).
- **Domain/GitHub org registration**: External brand assets are out of scope.
- **Upstream compatibility layer**: No backwards-compat shim for old "vector" names.
- **RFC/design doc content**: Historical documents in `rfcs/` and `docs/` that discuss "Vector" as a project are left as-is (they are historical records, not branding).

## Rabbit holes

### RH1 — False positive renames
The word "vector" appears in many contexts that are NOT the product name: Rust `Vec` discussions, mathematical vectors, variable names. A naive find-and-replace will break code.
**Constraint**: Each rename category must use targeted patterns, not blind replacement. Test compilation after each session.

### RH2 — Cargo dependency resolution after mass rename
Renaming 21 crates simultaneously may cause circular or broken dependency resolution.
**Constraint**: Rename all Cargo.toml files and directory paths in a single atomic session, then fix compilation. Do not attempt incremental crate-by-crate renaming.

### RH3 — CI workflow breakage
GitHub Actions workflows reference specific binary names and Docker images. These may break in ways that are only visible when CI runs.
**Constraint**: Update workflows mechanically but accept that full CI validation requires a push. Local build + test is the gate.

## Design

This is a mechanical refactoring with no architectural decisions. The approach is:

1. **Directory renames** (`git mv`) for `lib/vector-*` → `lib/sol-*`
2. **Cargo.toml updates** — package names, paths, dependencies, metadata
3. **Source code updates** — crate imports, env vars, metrics, product strings
4. **Config/distribution updates** — systemd, Docker, default configs
5. **CI updates** — workflow files
6. **Attribution** — add fork notice

### Decisions

- [Rename strategy](../adrs/20260504_rename-strategy.md)

## Cross-cutting Concerns

### Migration
This is a one-time rename. No migration path from "vector" to "sol" is needed for end users since the product hasn't been publicly released under either name yet.

### Rollback
Standard `git revert` of the rename commit(s). No special rollback procedure.

### Observability
Internal metrics already use `sol_*` prefix (done in 22198c3e). A Grafana dashboard for SOL Pipeline monitoring was added in the same commit. No further observability work needed for the rename.
