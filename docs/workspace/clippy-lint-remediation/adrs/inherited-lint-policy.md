---
status: draft
---
# Inherited lint policy for upstream Vector code

Addresses: [FR6](../DESIGN.md#fr6), [NFR1](../DESIGN.md#nfr1)

## Problem

Sol inherits code from upstream Vector (commit `692704adc`). After fixing all
Sol-authored lint violations, 13 errors remain in inherited files. These are
from newer clippy lints that did not exist when upstream Vector ran CI.

## Analysis

| File | Errors | Lints | Notes |
|---|---|---|---|
| `lua/event.rs` | 3 | `semicolon_if_nothing_returned` | Newer lint, trivial fix |
| `lua/metric.rs` | 2 | `useless_vec` | Macro-caused (`samples!`/`buckets!`) |
| `lua/metric.rs` | 1 | `implicit_clone` | Newer lint |
| `event/mod.rs` | 1 | `missing_panics_doc` | Newer lint |
| `event/mod.rs` | 1 | `missing_errors_doc` | Newer lint |
| `metric/series.rs` | 1 | `redundant_closure` | Newer lint |
| `test/serialization.rs` | 1 | `collapsible_if` | Newer lint |
| `source_sender/output.rs` | 1 | `cast_precision_loss` | Upstream had `#[expect]` annotation; lost during fork |
| `source_sender/tests.rs` | 1 | `uninlined_format_args` | Newer lint |
| `source_sender/tests.rs` | 1 | `items_after_statements` | Newer lint |

Total: 13 errors in 8 files.

## Options

| Option | Pros | Cons |
|---|---|---|
| A: Fix all 13 individually | Zero crate-wide allows needed | 13 small changes to inherited files |
| B: Add crate-wide allows for affected lint categories | No changes to inherited files | Suppresses these lints for ALL files, including new Sol code |
| C: Add local `#[allow]` on each inherited violation | Suppression is targeted, doesn't affect new code | 13 annotations in inherited files |

## Decision

**Option A**: Fix all 13 inherited errors directly.

Rationale:
- 13 errors is a small number — not the "hundreds" initially assumed
- All fixes are mechanical (add `;`, add doc section, simplify closure, restore
  upstream annotation)
- No crate-wide allows needed — keeps all lints enforced for new Sol code
- The `useless_vec` in `lua/metric.rs` is macro-caused; use local `#[allow]`
  for those 2 instances only
- `source_sender/output.rs` should restore upstream's original `#[expect]`
  annotation that was lost during the fork

## Consequences

- Zero new crate-wide `#![allow(...)]` entries in `lib.rs`
- All pedantic + all lints enforced for all code, inherited and new
- The 10 original upstream allows in `lib.rs` remain unchanged
- `useless_vec` gets 2 local `#[allow]` annotations (macro-inherent, cannot fix)
- Future Sol code must pass all clippy lints with no blanket suppression
