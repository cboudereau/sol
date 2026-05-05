---
status: accepted
---
# Single architecture packaging, multi-platform CI

Addresses: [FR1](../designs/20260505_github-actions.md#fr1), [FR2](../designs/20260505_github-actions.md#fr2), [FR3](../designs/20260505_github-actions.md#fr3), [NFR1](../designs/20260505_github-actions.md#nfr1)

## Problem

The upstream build matrix targets 8 Linux architectures (x86_64/aarch64 × gnu/musl, armv7, arm), macOS, and Windows. Sol needs to decide which targets to build and test.

## Options

| Option | Pros | Cons |
|---|---|---|
| **A: Keep full matrix** | Maximum platform coverage | Massive CI cost, needs cross-compilation tooling, many targets unused |
| **B: x86_64-unknown-linux-gnu only (build + test)** | Minimal CI cost, matches deployment target | No Windows test coverage, regressions caught late |
| **C: x86_64 + aarch64 (gnu)** | Covers common server architectures | Doubles build time, needs cross or QEMU |
| **D: x86_64 packaging + Linux and Windows CI tests** | Catches platform-specific issues early, single packaging target | Windows CI adds ~15 min |

## Decision

**Option D** — Package only `x86_64-unknown-linux-gnu` (`.deb` + Docker image), but run unit tests on both Linux (`ubuntu-24.04-8core`) and Windows (`windows-2025-8core`).

Windows test coverage catches platform-specific issues (path handling, line endings, conditional compilation) without the cost of Windows packaging.

If ARM support is needed later, add `aarch64-unknown-linux-gnu` as a second build matrix entry.

## Consequences

- Packaging reduced from ~8 architecture builds to 1 (`x86_64-unknown-linux-gnu`).
- No need for `cross` tool or QEMU setup.
- Docker images are `linux/amd64` only (no multi-platform manifest).
- Windows unit tests run on every PR, catching platform-specific regressions.
- The `Cross.toml` file and `scripts/cross/` directory become unused but can stay for future use.
