# clippy-lint-remediation

Sol forked from Vector at commit `692704adc`. The upstream `vector-core` crate enforced `#![deny(clippy::pedantic)]` with only 10 specific `#![allow(...)]` entries — notably, `cast_precision_loss` and `cast_possible_truncation` were **not** allowed, meaning upstream enforced them.

## Design
- [20260510_clippy-lint-remediation](./designs/20260510_clippy-lint-remediation.md)

## ADRs
- [20260510_inherited-lint-policy](./adrs/20260510_inherited-lint-policy.md) — Inherited lint policy for upstream Vector code
- [20260510_otlp-boundary-types](./adrs/20260510_otlp-boundary-types.md) — Typed boundary conversions for VRL ↔ OTLP
