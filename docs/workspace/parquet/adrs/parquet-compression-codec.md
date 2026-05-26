---
status: draft
---
# Parquet compression codec selection

Addresses: [FR3](../DESIGN.md#fr3), [NFR1](../DESIGN.md#nfr1)

## Problem

Parquet supports column-level compression codecs built into the file format. Unlike sink-level compression (gzip wrapping the entire payload), Parquet compression is per-column-chunk and encoded in the file metadata — readers decompress transparently.

Which compression codecs should Sol expose, and what should the default be?

## Options

| Option | Pros | Cons |
|---|---|---|
| Expose all Parquet codecs (uncompressed, snappy, gzip, brotli, zstd, lz4_raw) | Maximum flexibility | Larger dependency surface, some codecs rarely used |
| Expose subset: snappy, gzip, zstd, uncompressed | Covers 99% of use cases, matches Sol's existing compression support | Excludes brotli and lz4 |
| Single default (zstd), no configuration | Simplest config | Too restrictive for users with legacy tooling |

## Decision

**Option 2 — Expose snappy, gzip, zstd, uncompressed.** Default: **zstd**.

Rationale:
- **zstd** provides the best compression ratio / speed tradeoff for observability data (highly repetitive text with structural patterns). It is the modern default for Parquet in the ecosystem (Spark 3.x, DuckDB, Athena).
- **snappy** is the legacy Parquet default — needed for compatibility with older Hadoop/Spark clusters.
- **gzip** is universally supported — needed for maximum compatibility.
- **uncompressed** is useful for debugging and for pipelines where CPU is the bottleneck.
- **brotli** and **lz4_raw** are excluded: brotli is HTTP-oriented (not columnar-optimized), lz4_raw has Hadoop-compatibility issues. Both can be added later without breaking changes.

The `parquet` crate v56 supports all these via feature flags: `snap`, `flate2`, `zstd`, `lz4`. Sol already depends on `flate2` and `zstd` — only `snap` is new (but Sol already has snappy support in `src/sinks/util/snappy.rs` for Kafka).

## Consequences

- Parquet compression replaces sink-level compression: when using the Parquet codec, the sink should not apply additional gzip/zstd wrapping (double compression wastes CPU and hurts ratio).
- Configuration surface: `compression: zstd` (or `snappy`, `gzip`, `none`) at the codec level.
- The `parquet` crate feature flags needed: `snap`, `flate2`, `zstd`.
