---
status: draft
---
# Clean cutover — no backward compatibility

Addresses: [FR6](../DESIGN.md#fr6), [NFR2](../DESIGN.md#nfr2)

## Problem

Parquet files written before this change have no `prom_name` column. Once the
metrics schema declares `prom_name` and the read filter uses it, those files
won't match. Do we support a mixed old/new dataset, or cut over cleanly?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Eager backfill (rewrite every file) | all files prunable; no read-time complexity | expensive one-off; touches retention/GC; risky on a live store |
| B. Dual predicate `prom_name = x OR (prom_name IS NULL AND udf = x)` | correct on mixed data; converges as compaction rewrites | keeps the UDF; read predicate more complex; old files still full-scan |
| C. Clean cutover — regenerate the store, no fallback | simplest read path (plain column equality); lets the UDF be **removed**; smallest code | drops pre-change data (must wipe/regenerate) |

## Decision

**Option C — clean cutover.** Per the explicit call ("we don't care about
retro-compatibility right now"), we regenerate the Parquet store so every metric
file carries `prom_name`, and the read path assumes the column is present. No
fallback predicate, no mixed-data handling, no backfill job. This is what lets
the DataFusion `prom_metric_name_udf` be **removed** entirely
([prom-name-materialization](./prom-name-materialization.md),
[normalizer-canonical-location](./normalizer-canonical-location.md)) rather than
retained as a fallback — the read filter is a plain `col("prom_name") = lit(x)`.

`prom_name` is declared **REQUIRED** (non-null) in both codec and catalog
schemas (no nullable-for-fallback needed).

## Consequences

- Simplest possible read path; the UDF and its registration are deleted.
- **Pre-change Parquet files are unreadable** — the store must be wiped and
  regenerated before deploying. (Acceptable: the demo/dev data is disposable.)
- If an in-place migration is ever needed later, revisit (Option A backfill or
  re-introduce the Option B dual predicate) — recorded here so the trade-off is
  not silently lost.
