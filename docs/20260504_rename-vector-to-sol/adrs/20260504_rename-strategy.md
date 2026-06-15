---
status: accepted
---
# Rename strategy: atomic single-commit vs incremental

Addresses: [FR1](../designs/20260504_rename-vector-to-sol.md#fr1), [FR3](../designs/20260504_rename-vector-to-sol.md#fr3), [NFR1](../designs/20260504_rename-vector-to-sol.md#nfr1)

## Problem

Renaming 21 crates, their directory paths, and all cross-references can be done in one large commit or spread across multiple smaller commits. Which approach is safer?

## Options

| Option | Pros | Cons |
|---|---|---|
| **A — Single atomic commit** | One `git bisect` point; no intermediate broken states; Cargo dependency graph is always consistent | Large diff; harder to review; one mistake blocks everything |
| **B — Incremental per-crate** | Smaller diffs; easier to review | Intermediate commits won't compile (crate X renamed but crate Y still references old name); broken git bisect; complex dependency ordering |
| **C — Layered sessions** | Logical grouping (dirs → Cargo → source → config → CI); each session compiles; reviewable chunks | Slightly more commits but each is coherent |

## Decision

**Option C — Layered sessions**. Group the work into logical layers:

1. **Session 1**: Directory renames (`git mv`) + all Cargo.toml updates (package names, workspace paths, dependency references) + all Rust import path updates → must compile
2. **Session 2**: Environment variables, product name strings, config paths → must compile + tests pass
3. **Session 3**: Systemd, Docker, CI workflows, attribution → must compile

**Note** (updated 2026-05-04): Metrics prefix rename (originally planned for Session 2) is already done — commit 22198c3e changed the registry default namespace from `vector` to `sol` and stripped `vector_` prefix from custom metrics. See [ADR: metrics-namespace-renaming](20260504_metrics-namespace-renaming.md).

Each session produces a commit that compiles. This avoids the "big bang" risk of Option A while avoiding the broken-intermediate-state problem of Option B.

## Consequences

- **Easier**: Each session can be validated independently (`cargo build` / `cargo test`)
- **Easier**: Code review can focus on one layer at a time
- **Harder**: Three commits instead of one, but each is self-contained
- **Risk**: Session 1 is still large (all Cargo.toml + imports), but this is unavoidable — Cargo requires consistency
