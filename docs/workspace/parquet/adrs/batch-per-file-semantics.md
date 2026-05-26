---
status: draft
---
# Batch-per-file semantics

Addresses: [FR1](../DESIGN.md#fr1), [FR4](../DESIGN.md#fr4), [FR5](../DESIGN.md#fr5)

## Problem

Parquet files require a footer containing row group metadata. Unlike Arrow IPC streaming (which can append record batches indefinitely), a Parquet file must be finalized with a footer before it can be read.

How does the Parquet serializer interact with Sol's batching and file rotation?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. One Parquet file per batch (batch = file) | Simple, each `encode(Vec<Event>)` call produces a complete file. Works with S3 (one PUT per batch) and file sink (one file per batch). | Small batches → many small files (the "small files problem") |
| B. Multi-batch Parquet writer (append row groups to open file) | Fewer, larger files. Multiple row groups per file. | Complex lifecycle: need to track open writers, handle rotation, flush on shutdown. Breaks the `Encoder<Vec<Event>>` trait contract (which expects a complete output per call). |
| C. Buffer batches in memory, write single large file on rotation | Optimal file size control | Unbounded memory growth, data loss risk on crash |

## Decision

**Option A — One Parquet file per batch.**

Rationale:
- Matches the existing `BatchEncoder` / `Encoder<Vec<Event>>` contract: each `encode()` call writes a complete, self-contained Parquet file to the output buffer.
- The S3 sink already operates in batch-per-object mode — one S3 PUT per batch. A complete Parquet file per PUT is the natural fit.
- The file sink rotates files based on idle timeout, size, or time — each rotated file should be a complete Parquet file.
- The "small files problem" is mitigated by Sol's batch configuration: `max_events`, `max_bytes`, and `timeout` control batch size. For S3 use cases, the recommended batch config would be larger (e.g., `max_bytes: 50MB`, `timeout: 300s`) to produce reasonably-sized Parquet files.
- Downstream compaction tools (Spark, Athena CTAS, Iceberg compaction) handle small file consolidation — this is standard in data lake architectures.

Option B is rejected because it would require a fundamentally different serializer interface (stateful, with explicit flush/close lifecycle) that doesn't fit Sol's existing batch codec abstraction.

## Consequences

- Each `encode(Vec<Event>, &mut BytesMut)` call produces a complete Parquet file (header + row groups + footer) in the buffer.
- Sink-level batch configuration (`batch.max_bytes`, `batch.max_events`, `batch.timeout_secs`) controls Parquet file size.
- Documentation should recommend batch sizes appropriate for Parquet (larger than streaming defaults).
- File extension: `.parquet` (auto-set when no override).
