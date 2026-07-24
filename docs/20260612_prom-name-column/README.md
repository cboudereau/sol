# prom-name-column

Materialize a `prom_name` column at write time so metric-name filtering is a prunable column equality, not the per-row `prom_metric_name` UDF (a full scan of ~14M rows → 1.8s vs 0.1s). The pure normalizer lives in `sol-core` (codec write path); the codec writes + sorts by `prom_name`; the catalog declares it REQUIRED; read filters use a plain `col("prom_name")` equality and the DataFusion `prom_metric_name_udf` + its registration are **deleted**. **No backward compatibility** — clean cutover, the Parquet store is regenerated (old files lack the column and are not read).

Status: **shipped** (Phase 5 complete — commits `894809c9d`, `62c0334b4`; querier + codec tests green, clippy `-D warnings` clean). Verified live: a `prom_name`-filtered scan prunes to ~3.7ms over a multi-million-row store.

## Design
- [2026-06-12_prom-name-column](./designs/2026-06-12_prom-name-column.md)

## ADRs (accepted)
- [prom-name-materialization](./adrs/2026-06-12_prom-name-materialization.md) — materialize the normalized name as a column vs the read-time UDF
- [normalizer-canonical-location](./adrs/2026-06-12_normalizer-canonical-location.md) — the pure normalizer canonical home in `sol-core` (codec write path)
- [legacy-file-migration](./adrs/2026-06-12_legacy-file-migration.md) — clean cutover (regenerate the store, no fallback/backfill)
