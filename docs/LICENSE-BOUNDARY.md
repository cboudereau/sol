# License boundary: Sol vs Vector

Sol is a fork of the [Vector project](https://github.com/vectordotdev/vector)
(MPL-2.0, © Datadog). Sol adds an original observability **backend** (query
engine, OTLP event model, new sources/sinks/transforms) licensed under
**AGPL-3.0-only**. This document records where the boundary sits and how it was
derived, so the dual license is auditable.

The authoritative, machine-readable boundary is [`REUSE.toml`](../REUSE.toml)
(validated with `reuse lint`). This document is the human-readable rationale.

## TL;DR

| Bucket | License | Where |
|--------|---------|-------|
| Sol-original backend & OTLP code | **AGPL-3.0-only** | the 45 files listed below |
| Sol-original docs (ADRs, designs, plans, architecture) | **AGPL-3.0-only** | `docs/{adrs,designs,workspace,otlp-as-core-protocol-plan}/**`, `docs/DEV_ENVIRONMENT.md`, `docs/LICENSE-BOUNDARY.md` |
| Vendored OpenTelemetry protobufs | **Apache-2.0** | `lib/otel-proto-types/src/proto/**` |
| Everything else (Vector-derived) | **MPL-2.0** | the rest of the tree (incl. Vector's own `docs/ARCHITECTURE.md`, `docs/specs/**`, `docs/tutorials/**`, …) |

> Docs note: AGPL is a software license but is a valid copyright license for any
> work, so it keeps Sol's IP under one rule. If you want a documentation-native
> non-commercial license for the prose instead, **CC-BY-NC-4.0** is the usual
> choice — say the word and I'll switch the docs block.

## Why a git diff is NOT the boundary

Comparing `feat/backend` against the Vector fork point
(`git merge-base feat/backend upstream/master` =
`e487d6ed6fa413f2ced27780227424f02472c799`) reports:

```
   409 added      11109 deleted      848 modified      ~280 renamed
```

Those numbers are dominated by a **mass rename**: Sol renamed the entire
`lib/vector-*` crate tree to `lib/sol-*`. Git's rename detection caps out at
11k files, so ~369 of the "added" files are actually **moved Vector code**, not
new Sol code. Example: `lib/sol-core/src/event/array.rs` shows as "added" but
existed verbatim at `lib/vector-core/src/event/array.rs` in Vector → it stays
**MPL-2.0**.

Therefore the boundary is defined by **content provenance**, not by git add
time. A git diff is evidence; `REUSE.toml` is the contract.

## Folder-level map

```
lib/vector-*   →  lib/sol-*      RENAMED Vector crates           → MPL-2.0
lib/codecs, lib/opentelemetry-proto, lib/loki-logproto, ...      → MPL-2.0
                                  (+ a few Sol-original files, see below)
lib/otel-proto-types/            NEW crate
  ├─ src/proto/**                vendored OpenTelemetry protos    → Apache-2.0
  └─ src/lib.rs, build.rs        Sol wrapper                      → MPL-2.0 *
docs/{adrs,designs,workspace,        Sol-original documentation     → AGPL-3.0-only
  otlp-as-core-protocol-plan}/
docs/ARCHITECTURE.md, specs/,        Vector-inherited docs          → MPL-2.0
  tutorials/, README.md, ...
demo/, .claude/, .devcontainer/  Sol-authored tooling             → MPL-2.0 (fallback)
src/querier/, src/vrl_migrate/, src/transforms/{servicegraph,
  span_metrics,tail_sampling}/, src/sinks/opentelemetry/{grpc,
  http,load_balancing}.rs, src/sources/source_otel.rs            → AGPL-3.0-only
lib/sol-core/src/event/otel_*.rs, otlp.rs                        → AGPL-3.0-only
lib/codecs/src/encoding/format/parquet.rs                        → AGPL-3.0-only
lib/opentelemetry-proto/src/buffer_codec.rs                      → AGPL-3.0-only
```

\* `lib/otel-proto-types/src/lib.rs` and `build.rs` are thin generated-proto
wrappers; left MPL for now. Promote to AGPL if they grow original logic.

## The AGPL-3.0 set (45 files)

```
lib/codecs/src/encoding/format/parquet.rs
lib/opentelemetry-proto/src/buffer_codec.rs
lib/sol-core/src/event/otel_attributes.rs
lib/sol-core/src/event/otel_conv.rs
lib/sol-core/src/event/otel_event.rs
lib/sol-core/src/event/otel_fields.rs
lib/sol-core/src/event/otel_json.rs
lib/sol-core/src/event/otel_metric.rs
lib/sol-core/src/event/otlp.rs
src/config/querier.rs
src/config/compactor.rs
src/querier/*.rs                 (13 files)
src/sinks/opentelemetry/grpc.rs
src/sinks/opentelemetry/http.rs
src/sinks/opentelemetry/load_balancing.rs
src/sources/source_otel.rs
src/transforms/servicegraph/*.rs      (3 files)
src/transforms/span_metrics/*.rs      (3 files)
src/transforms/tail_sampling/*.rs     (4 files)
src/vrl_migrate/*.rs                   (8 files)
```

## How to reproduce / audit

```bash
git remote add upstream https://github.com/vectordotdev/vector.git
git fetch --filter=blob:none upstream master
MB=$(git merge-base feat/backend upstream/master)

# files Sol added with no Vector predecessor (candidates — still needs provenance review)
git diff --name-status -M --diff-filter=A "$MB" feat/backend

# confirm an "added" file is actually a renamed Vector file (→ MPL, not Sol IP)
git ls-tree -r --name-only "$MB" -- lib/vector-core/src/event/

# validate the declared boundary
pipx run reuse lint     # or: pip install reuse && reuse lint
```

## Stronger boundary (recommended next step)

`REUSE.toml` + SPDX headers make the boundary explicit and CI-checkable. The
*ideal* boundary is also **structural**: move all AGPL code into dedicated
crates that contain **only** Sol-original code (e.g. a `lib/sol-backend` crate),
so no file mixes MPL and AGPL provenance. Today `lib/sol-core` mixes renamed
Vector code (MPL) with new `otel_*`/`otlp` files (AGPL) in one crate — correct
but harder to reason about. Extracting the OTLP event model into its own crate
would make the boundary a directory line, not a per-file annotation.

> NOT LEGAL ADVICE. Have an IP lawyer confirm the AGPL set before relying on it
> commercially — especially any file that adapts Vector patterns.
