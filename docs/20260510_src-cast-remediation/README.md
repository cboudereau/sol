# src-cast-remediation

The clippy-lint-remediation workspace fixes all 400 clippy errors in `sol-core` (the library crate, which enforces `deny(clippy::pedantic)`). This workspace addresses the **other side** of the codebase: `src/` — the main crate containing sinks, sources, and transforms.

## Design
- [20260510_src-cast-remediation](./designs/20260510_src-cast-remediation.md)

## ADRs
- [20260510_cast-safety-strategy](./adrs/20260510_cast-safety-strategy.md) — VRL ↔ OTLP cast safety strategy
- [20260510_numeric-conversion-conventions](./adrs/20260510_numeric-conversion-conventions.md) — Numeric conversion conventions for sinks, sources, and transforms
