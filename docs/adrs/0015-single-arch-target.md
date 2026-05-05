---
status: accepted
---
# Single architecture target

Addresses: [FR2](../designs/20260505_github-actions.md#fr2), [FR3](../designs/20260505_github-actions.md#fr3), [NFR1](../designs/20260505_github-actions.md#nfr1)

## Problem

The upstream build matrix targets 8 Linux architectures (x86_64/aarch64 × gnu/musl, armv7, arm), macOS, and Windows. Sol needs to decide which targets to build.

## Options

| Option | Pros | Cons |
|---|---|---|
| **A: Keep full matrix** | Maximum platform coverage | Massive CI cost, needs cross-compilation tooling, many targets unused |
| **B: x86_64-unknown-linux-gnu only** | Minimal CI cost, matches deployment target | No ARM support |
| **C: x86_64 + aarch64 (gnu)** | Covers common server architectures | Doubles build time, needs cross or QEMU |

## Decision

**Option B** — Build only `x86_64-unknown-linux-gnu`. This is Sol's deployment target. The `.deb` package and Docker image are both for this architecture.

If ARM support is needed later, add `aarch64-unknown-linux-gnu` as a second matrix entry.

## Consequences

- Build time reduced from ~8 parallel architecture builds to 1.
- No need for `cross` tool or QEMU setup.
- Docker images are `linux/amd64` only (no multi-platform manifest).
- The `Cross.toml` file and `scripts/cross/` directory become unused but can stay for future use.
