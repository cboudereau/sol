---
status: accepted
---
# JSON attribute extraction via the datafusion-functions-json extension

Addresses: [FR3](../DESIGN.md#fr3), [FR1](../DESIGN.md#fr1), [NFR1](../DESIGN.md#nfr1), [NFR5](../DESIGN.md#nfr5)

## Problem

OTLP attributes are stored as a JSON UTF8 string column (`attributes` for
metrics/traces, `resource_attributes` for logs) per the codec write side. Every
query that filters or groups on a non-promoted label needs to extract a key from
that JSON at query time. DataFusion core ships **no** JSON extraction function,
so we must provide `json_get_str(json, 'key')` (and ideally typed/nested
variants) ourselves.

Two ways to provide it:
- **A — custom `serde_json` UDF**: one scalar UDF backed by `serde_json` (already
  in the tree). No new dependency.
- **B — `datafusion-functions-json` crate**: the de-facto JSON extension for
  DataFusion (datafusion-contrib), version-aligned to DataFusion 53 (`0.53.1`).

## Options

| Option | Pros | Cons |
|---|---|---|
| A — custom serde_json UDF | No new crate; full control; minimal code | `serde_json` is a *document* parser: parses the **entire** JSON into an owned `Value` tree **per row, per query**, then keeps one key. Returns `String` only (caller must `CAST`). No nested paths, no operators, no `StringView` support. We hand-roll null/edge-case handling and tests. |
| B — datafusion-functions-json | Built on **`jiter`** (lazy/iterative parser — scans to the key and stops, no full-tree alloc → lower CPU/allocations). Vectorized Arrow kernels over `Utf8`/`LargeUtf8`/`StringView`. Typed getters (`json_get_int/float/bool/str`, `json_get`), nested paths + array indices, `->`/`->>` operators wired into the planner, `json_contains`. Maintained & tested. Same `json_get_str` name our translators already emit → drop-in. | One new crate (+ its tree); must stay version-aligned to DataFusion. |

## Decision

**Option B** — depend on `datafusion-functions-json` `0.53.1` (optional, gated by
the `query-backend` feature) and register it via
`datafusion_functions_json::register_all(&mut ctx)` in `QueryEngine::new`. The
custom UDF (`src/query/udf.rs`) is removed.

This is consistent with [NFR1](../DESIGN.md#nfr1): the crate is a **DataFusion
extension** from the DataFusion ecosystem (datafusion-contrib), not a separate
query engine, JVM, or embedded database. NFR1's intent — DataFusion as the sole
engine — is preserved. The crate is added to the pinned dependency set
(datafusion / **datafusion-functions-json** / object_store / promql-parser).

## Consequences

- **Easier**: typed and nested attribute extraction, `->>` operator syntax,
  `StringView` columns, and lower per-row parse cost — directly serving the
  attribute-filtering paths of FR3/FR1 and the cost budget (NFR5).
- **Easier**: translators (`loki.rs`, `prometheus.rs`) are unchanged — they keep
  emitting `json_get_str(<col>, '<key>')`, now resolved by the crate.
- **Harder**: one more crate to keep version-aligned on each DataFusion bump
  (mitigated: it tracks DataFusion's version line, `0.53.x` ↔ DataFusion 53).
- **Out of scope (see rabbit hole #4)**: this still parses JSON at query time.
  The genuinely state-of-the-art fix is *not* storing attributes as a string at
  all — see the ClickHouse / Parquet-Variant note — which would supersede the
  JSON-string design and warrants its own ADR.
