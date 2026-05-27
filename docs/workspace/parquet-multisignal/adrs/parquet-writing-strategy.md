---
status: draft
---
# Parquet writing strategy: native column writers vs Arrow intermediary

Addresses: [FR1](../DESIGN.md#fr1), [FR2](../DESIGN.md#fr2), [FR3](../DESIGN.md#fr3), [FR4](../DESIGN.md#fr4), [NFR1](../DESIGN.md#nfr1)

## Problem

The current log-only Parquet implementation uses Arrow as an intermediary:

```
OtelLog → as_map() → ObjectMap → serde_arrow → Arrow RecordBatch → ArrowWriter → Parquet
```

This approach has several issues:
1. `serde_arrow` infers Arrow types from the ObjectMap, often producing types that don't match the target schema (e.g., `Int64` instead of `Int32`), requiring a `cast_with_options` correction pass
2. `as_map()` flattens proto fields into a dynamic `ObjectMap`, losing compile-time type safety
3. `OtelMetric` has no `as_map()` method — the approach cannot work for metrics at all
4. The `arrow` crate (~large dependency surface) is only used as a pass-through to the `parquet` crate's `ArrowWriter`

The question: how should the Parquet codec write files for all three signal types?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Native parquet column writers (`SerializedFileWriter` + `SerializedColumnWriter`) | No arrow/serde_arrow dependency. Direct proto→column extraction. Uniform for all signals. Compile-time type safety per column. No cast correction hacks. | Lower-level API — explicit definition levels, column-by-column writing. More verbose per-signal implementation. |
| B. Arrow RecordBatch via array builders (no serde_arrow) | Keep ArrowWriter. Type-safe array construction. Works for metrics. | Still requires arrow dependency. Array builder API is verbose. Unnecessary intermediary — Arrow RecordBatch is immediately consumed by ArrowWriter and discarded. |
| C. Extend as_map() to all types + keep serde_arrow | Minimal code change from current implementation. | Propagates the fundamental problem: dynamic ObjectMap loses type safety, requires cast corrections, adds runtime overhead. Requires adding as_map() to OtelMetric (complex — metric subtypes have different field sets). |

## Decision

**Option A — Native parquet column writers.**

Rationale:
- **Removes unnecessary intermediary**: Arrow RecordBatch serves no purpose in the write path — it's immediately serialized to Parquet and discarded. The `parquet` crate can write files directly without it.
- **Uniform approach for all signals**: logs, traces, and all metric subtypes use the same pattern: extract column vectors from proto structs, write columns with typed writers. No special handling needed for metrics.
- **Dependency reduction**: the `parquet` feature drops `dep:arrow` and `dep:serde_arrow`. The `parquet` crate (v56) without the `arrow` feature still provides `SerializedFileWriter`, `SerializedColumnWriter`, schema types, and compression codecs.
- **Compile-time type safety**: each column extraction produces a concrete type (`Vec<i64>`, `Vec<ByteArray>`, `Vec<bool>`). Type mismatches are caught at compile time, not at runtime via cast corrections.
- **No serde round-trip**: proto fields are extracted directly into column vectors. No serialization to ObjectMap, no deserialization by serde_arrow, no type inference surprises.

Option B is rejected because it retains the arrow dependency for no benefit — Arrow RecordBatch is an intermediary without a consumer. If the Parquet files were later read back as Arrow tables (e.g., for DataFusion queries), the reader would reconstruct the RecordBatch from Parquet anyway.

Option C is rejected because it propagates a fundamentally flawed approach (dynamic ObjectMap + type inference + cast corrections) to two more signal types. Adding `as_map()` to `OtelMetric` is particularly problematic because metric subtypes (gauge, sum, histogram, etc.) have different field sets, making a flat ObjectMap representation lossy.

## Consequences

- The current `parquet.rs` is rewritten — the `build_record_batch`, `convert_timestamps_in_map`, and `timestamp_to_nanos` functions are replaced by direct column writing functions.
- The `parquet` feature in `lib/codecs/Cargo.toml` changes from `["dep:parquet", "dep:arrow", "dep:serde_arrow"]` to `["dep:parquet"]`.
- Each signal type needs a column extraction implementation (more upfront code), but each is straightforward and type-safe.
- Tests must verify Parquet output by reading back with the `parquet` crate's native reader (or the arrow-based reader in dev-dependencies only).
- The `arrow` feature (for ClickHouse ArrowStream codec) is completely independent and unaffected.
