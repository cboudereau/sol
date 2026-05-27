---
status: draft
---
# Mixed-signal batch handling

Addresses: [FR5](../DESIGN.md#fr5)

## Problem

Sol's `Event` enum has three variants: `Log`, `Trace`, `Metric`. A batch (`Vec<Event>`) passed to `ParquetSerializer::encode()` may contain a mix of signal types. Each signal type has a different Parquet schema (logs, traces, gauge, sum, histogram, exp_histogram, summary).

A single Parquet file has one schema. Mixed signals cannot be written to a single file.

How should the serializer handle mixed-signal batches?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Reject mixed-signal batches with an error | Simple, explicit. Forces upstream to partition. Clear contract. | Breaks if any pipeline path produces mixed batches. Requires sink-level partitioning (not all sinks partition by signal type). |
| B. Group by signal type, produce multiple Parquet files per encode() call | Handles any input. No upstream requirements. Each output is a valid, self-contained Parquet file. | `encode()` must return multiple outputs. Caller must handle multiple files (naming, routing). Slightly more complex serializer. |
| C. Silently drop non-primary signals (e.g., keep only logs) | Simple implementation. | Data loss. Unacceptable. |

## Decision

**Option B — Group by signal type, produce multiple Parquet files per encode() call.**

Rationale:
- **Robustness**: the codec should handle any valid `Vec<Event>` input without requiring upstream guarantees about signal homogeneity. Defensive coding at the boundary.
- **Already needed for metrics**: even a "metrics-only" batch can contain multiple metric subtypes (gauge + histogram). The serializer must split by subtype regardless. Splitting by signal type is the same pattern at a higher level.
- **Practical**: Sol's OTLP source already groups events by signal type (separate gRPC endpoints for logs, traces, metrics). Mixed batches are unlikely in normal operation but possible through VRL transforms or test scenarios. The codec should not silently fail in these cases.

The `encode()` method appends one Parquet file per signal group to the output buffer. Each file is self-contained (header + row group + footer). The caller can either:
- Use the buffer as-is (concatenated files — valid for blob storage where one PUT = one batch)
- Split on Parquet magic bytes if separate files are needed

For the typical case (homogeneous batch), `encode()` produces exactly one Parquet file — no overhead.

## Consequences

- `encode()` may produce multiple concatenated Parquet files in the output buffer. The buffer contains `N` complete Parquet files where `N` = number of distinct signal types + metric subtypes in the batch.
- Sinks that produce one file per batch (S3, file sink) will get concatenated Parquet files if the batch is mixed. This is acceptable for S3 (one object per batch). For the file sink, each rotation produces one file — if mixed, the file contains multiple Parquet files back-to-back (not a standard pattern but readable by iterating).
- Sink-level partitioning by signal type (upstream of the codec) is the recommended configuration for clean file separation, but it is not required.
- Metric batches always produce one file per subtype present — this is inherent to the separate-schema design and is not a mixed-signal concern.
