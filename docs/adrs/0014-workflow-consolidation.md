---
status: accepted
---
# Workflow consolidation strategy

Addresses: [FR1](../designs/20260505_github-actions.md#fr1), [FR2](../designs/20260505_github-actions.md#fr2), [FR3](../designs/20260505_github-actions.md#fr3), [FR4](../designs/20260505_github-actions.md#fr4), [NFR1](../designs/20260505_github-actions.md#nfr1), [NFR2](../designs/20260505_github-actions.md#nfr2)

## Problem

Sol inherits 42 GitHub Actions workflows from Vector. Most reference infrastructure, services, and targets Sol does not use. We need to decide how to reduce this to what Sol actually needs.

## Options

| Option | Pros | Cons |
|---|---|---|
| **A: Delete all, write from scratch** | Clean slate, no dead code | Loses proven patterns (caching, setup action, path filtering) |
| **B: Keep useful workflows, delete the rest** | Reuses battle-tested setup action and path filtering | May inherit subtle upstream assumptions |
| **C: Consolidate into 2-3 new workflows, reuse setup action** | Clean structure + proven infrastructure | Slightly more work than option B |

## Decision

**Option C** — Write 3 new workflow files (`ci.yml`, `build.yml`, `nightly.yml`). Reuse the `.github/actions/setup` composite action as-is. Delete all 42 existing workflow files, the `changes.yml` path-filter workflow, and the unused custom actions.

All CI checks run unconditionally on every PR (no path-based filtering). This gives us a clean, auditable CI surface while keeping the setup action that handles Rust toolchain installation, Cargo caching, mold linker, and tool installation.

## Consequences

- All 42 existing workflow files and `changes.yml` deleted.
- Custom actions `pull-test-runner` and `spelling/` deleted.
- The `setup` action is retained as the only custom action (`install-vdev` kept as its dependency).
- No path-based filtering — every check runs on every PR. Simpler to maintain than conditional skipping.
- Future upstream syncs of workflows are no longer possible (intentional — Sol's CI diverges from Vector).
