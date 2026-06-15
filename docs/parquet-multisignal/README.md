# parquet-multisignal

The Parquet codec originally encoded only OTLP logs via an `arrow` + `serde_arrow` intermediary (`OtelLog → as_map() → ObjectMap → serde_arrow → RecordBatch → ArrowWriter`), which lost type safety and had no path for metrics (`OtelMetric` has no `as_map()`). It was **replaced** with direct native column writing — `proto struct fields → Vec<T> per column → SerializedColumnWriter → Parquet bytes` — 

## Design
- [20260527_parquet-multisignal](./designs/20260527_parquet-multisignal.md)

## ADRs
- [20260527_attributes-serialization-strategy](./adrs/20260527_attributes-serialization-strategy.md) — Attributes serialization strategy for Parquet
- [20260527_batch-per-file-semantics](./adrs/20260527_batch-per-file-semantics.md) — Batch-per-file semantics
- [20260527_mixed-signal-batch-handling](./adrs/20260527_mixed-signal-batch-handling.md) — Mixed-signal batch handling
- [20260527_parquet-compression-codec](./adrs/20260527_parquet-compression-codec.md) — Parquet compression codec selection
- [20260527_parquet-writing-strategy](./adrs/20260527_parquet-writing-strategy.md) — Parquet writing strategy: native column writers vs Arrow intermediary
- [20260527_storage-format-selection](./adrs/20260527_storage-format-selection.md) — Storage format selection: Parquet vs Avro
