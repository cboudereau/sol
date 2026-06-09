# prom-name-column — Design Doc

Builds on: [designs/20260527_parquet-multisignal.md](../../designs/20260527_parquet-multisignal.md) ·
relates to [parquet-backend](../parquet-backend/DESIGN.md)

## Context

Metric-name filtering in the Parquet query backend is done with the
`prom_metric_name(name, unit, is_monotonic)` **scalar UDF** (`src/query/udf.rs`,
called via `prom_name_expr()` in `src/query/prometheus.rs`). Because the filter
predicate is a UDF over raw columns, DataFusion cannot derive Parquet row-group
min/max pruning from it: every metric query decodes **all rows of all metric
files** and evaluates the UDF per row.

Measured on the live demo (14,026,388 metric rows):

| Filter | Plan | Time |
|---|---|---|
| `prom_metric_name(name,unit,is_monotonic) = 'x'` (current) | post-scan `FilterExec`, no pruning | **1.81 s** |
| `name = 'x'` (raw column) | `predicate=name_min <= x <= name_max` row-group pruning + name-sort | **0.11 s** |

The conversion is *many-to-one and lossy* (every non-alphanumeric collapses to
`_`; unit/`_total` suffixes are appended conditionally), so it **cannot be
inverted** at query time to a raw-`name` equality. The fix is to run the
normalization **once at write time** and store the result as a real, sorted
`prom_name` column, so the read filter becomes a prunable column equality.

## Functional Requirements

### <a id="fr1"></a>FR1 — Materialize `prom_name` at write time
The Parquet codec writes a `prom_name` column on every metric row, equal to
`prom_metric_name(name, unit, is_monotonic)` for that row (the normalized
Prometheus/Mimir name). The raw OTLP `name`/`unit`/`is_monotonic` columns are
**retained unchanged** (storage fidelity).

### <a id="fr2"></a>FR2 — Single canonical normalizer shared by write and read
The normalization function (`prom_metric_name` + `unit_suffix`) lives in one
shared location reachable by both `lib/codecs` (write) and `src/query` (read).
The DataFusion UDF wrapper stays in `src/query/udf.rs` but delegates to the
shared function — no duplicated logic, no drift between stored and queried names.

### <a id="fr3"></a>FR3 — Read filters use the `prom_name` column
The metrics-table schema (`src/query/catalog.rs`) declares `prom_name`, and the
read path filters on it as a **column equality** (`col("prom_name") = lit(name)`),
not via the UDF. Affected sites: `prom_name_expr`, `name_pred_expr`, the
`__name__` `label_values` path, `build_series`, the histogram/bucket scans, and
range/instant base selectors.

### <a id="fr4"></a>FR4 — Histogram component names stay correct
The synthesized histogram component series (`<base>_count`, `<base>_sum`,
`<base>_bucket`) continue to resolve against the stored histogram row, expressed
as a predicate on `prom_name` (`prom_name = '<base>_count' OR (prom_name =
'<base>' AND bucket_counts IS NOT NULL)`), preserving current behavior.

### <a id="fr5"></a>FR5 — Sort metric files by `prom_name`
The codec's `sort_dp_rows` and the compactor's sort-merge order rows by
`(service_name, prom_name, time_unix_nano)` so row-group min/max on `prom_name`
is tight and pruning is maximal. (Today they sort by raw `name`.)

### <a id="fr6"></a>FR6 — Legacy files without `prom_name` stay queryable
Parquet files written before this change have no `prom_name` column. Queries
against a mixed dataset must remain **correct** (no missing series) while new
files prune. Old files fall back to the UDF predicate; the column predicate
prunes the new ones. Compaction/rollup rewrites backfill `prom_name` over time.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Selective metric queries prune, no full scan
An exact metric-name query over `prom_name`-bearing files must use row-group
pruning (no per-row UDF over the whole table). Target: the prunable-column
latency class (~0.1 s for the 14 M-row dataset), not the full-scan class (~1.8 s).

### <a id="nfr2"></a>NFR2 — Result parity
Query results (instant, range, `label_values(__name__)`, `series`, histogram
quantiles) are identical to the pre-change UDF behavior, including for the
synthesized `_count`/`_sum`/`_bucket` component series. `query::` stays green.

### <a id="nfr3"></a>NFR3 — Schema is the codec↔catalog contract
The `prom_name` column must be added to **both** the codec schemas
(per-subtype) and `metric_union_schema` in the catalog, at matching positions
— the existing binding-contract invariant (parquet-multisignal).

### <a id="nfr4"></a>NFR4 — No new external dependency
Pinned crate set unchanged (datafusion / datafusion-functions-json /
object_store / promql-parser / moka).

## Non-goals

- **Backfilling existing Parquet files eagerly.** Excluded for cost/complexity;
  FR6's fallback + natural compaction rewrite covers correctness and converges
  pruning over the retention window. Revisit only if cold-tail pruning matters
  before compaction catches up.
- **Removing the `prom_metric_name` UDF.** It is retained — it is the FR6
  fallback for legacy files and remains available to the ad-hoc SQL endpoint
  (`sql.rs`). This work changes the *default metric read filter*, not the UDF.
- **Changing the normalization rules.** The OTLP→Prometheus mapping is unchanged
  (already fixed in the token-dedup work, `udf.rs`); this only moves *where/when*
  it runs.
- **Label-key normalization (`prom_attr`).** Out of scope — `prom_attr` is a
  JSON-extraction UDF on the attributes column, a separate concern with its own
  cardinality; not a metric-name pruning problem.

## Rabbit holes

- **Schema evolution reading old files.** A ListingTable whose schema has
  `prom_name` reading a file lacking it: confirm DataFusion fills the missing
  column with NULL (vs erroring). Cap: if NULL-fill is not automatic, the read
  path must tolerate absent `prom_name` via the FR6 fallback; do **not** attempt
  per-file schema negotiation or backfill in this work.
- **Moving the normalizer across crates.** `lib/codecs` cannot depend on the
  `sol` crate. Cap: move only the pure `prom_metric_name`/`unit_suffix` fns to a
  crate both can use (`lib/sol-core`, which already owns `OtelMetric`); leave the
  DataFusion UDF wrapper in `src/query/udf.rs`. Do not refactor unrelated udf code.
- **Compaction/rollup carrying the new column.** Rollup/compaction read→rewrite
  metric batches; `prom_name` must survive the round-trip and the re-sort. Cap:
  it is just another column in the Arrow batch — verify it round-trips; only the
  sort key (FR5) changes.

## Design

```mermaid
flowchart LR
  subgraph write [Write path - lib/codecs]
    OM[OtelMetric name,unit,is_monotonic] --> N[shared prom_metric_name]
    N --> PN[prom_name column]
    OM --> RAW[raw name,unit,is_monotonic columns]
    PN & RAW --> SORT[sort by service_name, prom_name, time]
    SORT --> PARQUET[(metrics/subtype/dt=.../*.parquet)]
  end
  subgraph read [Read path - src/query]
    Q[PromQL name X] --> F["col prom_name = X (prunable)\nOR UDF fallback on NULL prom_name (FR6)"]
    F --> SCAN[ParquetExec row-group pruning]
    PARQUET --> SCAN
  end
  N -. same fn .-> UDFW[prom_metric_name_udf wrapper]
  UDFW -.-> F
```

**Data model.** One added column `prom_name` (REQUIRED UTF8) on every metric
subtype schema, populated at write from the shared normalizer. Raw columns kept.

**Read filter.** `name_pred_expr(name)` becomes, conceptually:
`prom_name = name` (+ histogram `_count`/`_sum` OR-branch, FR4), with the FR6
fallback `OR (prom_name IS NULL AND <udf> = name)` so legacy files stay correct.
New files prune on the column predicate; old files fall back to the UDF.

**Normalizer location.** Move `prom_metric_name` + `unit_suffix` to
`lib/sol-core` (alongside `OtelMetric`); `src/query/udf.rs` and the codec both
call it. The DataFusion `ScalarUDF` wrapper stays in `src/query/udf.rs`.

Decisions:
- [Materialize prom_name at write time](./adrs/prom-name-materialization.md)
- [Canonical normalizer location](./adrs/normalizer-canonical-location.md)
- [Legacy-file migration via dual-predicate fallback](./adrs/legacy-file-migration.md)

## Cross-cutting Concerns

- **Observability.** `sol_query_*` scan-bytes/latency already emitted; expect a
  drop for selective metric queries. No new metrics required.
- **Migration.** FR6 fallback makes the change backward-compatible at read; no
  data migration step. Compaction/rollup rewrites converge old files to
  `prom_name`-bearing over the retention window.
- **Rollback.** Read path can revert to `prom_name_expr` = UDF (the current
  behavior) without touching stored data; the extra column is inert if unused.
