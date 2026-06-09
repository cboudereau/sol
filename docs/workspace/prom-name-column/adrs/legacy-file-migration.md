---
status: draft
---
# Legacy-file migration via dual-predicate fallback

Addresses: [FR6](../DESIGN.md#fr6), [NFR2](../DESIGN.md#nfr2)

## Problem

Parquet files written before this change have no `prom_name` column. Once the
metrics schema declares `prom_name`, a query filtering `prom_name = 'x'` would
match **nothing** in old files (the column reads as NULL there) — silently
dropping all pre-change metrics until they age out or are rewritten. How do we
stay correct on a mixed dataset while still pruning new files?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Eager backfill (rewrite every file) | clean cutover; all files prunable | expensive one-off; touches retention/GC; risky on a live store |
| B. Dual predicate: `prom_name = x OR (prom_name IS NULL AND udf(...) = x)` | correct on mixed data; new files prune; zero migration step; converges as compaction rewrites | old files still full-scan (UDF) until rewritten; predicate slightly more complex |
| C. Hard cutover (ignore old files) | simplest | data loss for the retention window — unacceptable |

## Decision

**Option B — dual-predicate fallback.** The read filter is
`prom_name = name OR (prom_name IS NULL AND prom_metric_name(name,unit,is_monotonic) = name)`
(plus the histogram OR-branch). DataFusion prunes new (`prom_name`-bearing) files
on the column predicate; old files (where `prom_name` is NULL) fall back to the
UDF and remain correct. As the compactor/rollup rewrites sealed days
([FR5](../DESIGN.md#fr5) sort included), old files gain `prom_name` and become
prunable — the dataset converges within the retention window with no explicit
migration job.

## Consequences

- Correctness on mixed old/new data (NFR2 parity) with no migration step.
- Transitional: queries touching not-yet-rewritten old files still pay the UDF
  full-scan **for those files only**; the active/new tail prunes immediately.
- The `prom_metric_name` UDF must remain registered (it is the fallback).
- If `prom_name` is declared REQUIRED in the schema, DataFusion may not NULL-fill
  it for files lacking the column; the implementation must declare `prom_name`
  **OPTIONAL/nullable** in the catalog read schema so the `IS NULL` fallback is
  reachable. (New files always write a non-null value.)
