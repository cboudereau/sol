---
status: proposed
---
# Per-query file pruning: retained inventory + time-scoped ephemeral providers

Addresses: [FR1](../DESIGN.md#fr1), [NFR1](../DESIGN.md#nfr1), [NFR3](../DESIGN.md#nfr3)

## Problem

Every query executes over a `ListingTable` holding **all** surviving files of its signal, all days (`src/querier/catalog.rs:229-249`, `with_collect_stat(false)` at `:216-217`), so execution-time stats pruning opens every file's footer: measured `files_ranges_pruned_statistics=1.35 K` per 15-minute query on the demo store — the ~0.25 s fixed cost behind slow dashboards. The file list is computed in `build_providers` and dropped into the `ListingTable`; **nothing retains it** (`catalog.rs:210-267`), so per-query pruning has no index to consult today.

Facts that shape the options (explorer-verified):
- Every metrics `.table()` call site already has a time window in scope (`metric_base_df` `prometheus.rs:203`, `hist_scan` `:2131`, `build_label_values` `:1375`, resolver windows `(table, lo, hi)` from `resolve_metric_windows` `prometheus.rs:2087-2118`); Loki range paths and Tempo search do too. Metadata no-range branches, Tempo tag/trace-by-id, and raw SQL do **not**.
- Path-time parsers already exist for `dt=YYYY-MM-DD` (`Compactor::partition_dirs`, `compaction.rs:150-180`) and bare hour (`parse_hour` `:538-541`, `hour_end_ns` `:545-550`); none for full `HH-MM-SS-<uuid>` flush times.
- DataFusion 53.1 partition columns (`table_partition_cols`) are entirely unused today.
- Providers are swapped by deregister/register pairs with no await between (race note `catalog.rs:284-290`); the engine is a lock-free `Arc<QueryEngine>`.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. **Retained inventory + scoped ephemeral provider**: refresh keeps a per-table `Vec<FileEntry{path, conservative [lo,hi]}>` (swap via `arc_swap`-style `RwLock<Arc<…>>`); new `engine.table_scoped(name, lo, hi)` filters the inventory and builds an unregistered `ListingTable` consumed via `ctx.read_table(provider)`; windowless callers keep the registered full table | Surgical: no schema change, no predicate-generation change, SQL endpoint untouched; pruning granularity as fine as the path encodes (day + intra-day flush time); fallback is the existing behaviour | New engine state to keep in sync with the registered tables (single `build_providers` source makes this atomic); per-query provider construction (no I/O, µs-scale) |
| B. **DataFusion `dt` partition column** + synthesized `dt` predicate in `prom_time_between` | Native mechanism; plan-time file pruning | Day granularity only (no intra-day pruning — active-day queries still open all ~250–460 of the day's footers, likely missing [NFR1](../DESIGN.md#nfr1)); adds a visible `dt` column to every table (leaks into SQL results and `SELECT *`); every query path must emit the extra predicate; reworks `new_with_multi_paths` registration |
| C. **Per-day table registration** (`metrics_2026_07_10`, …) with query-time union | Plan-time pruning | Explodes table names and breaks `resolve_metric_windows`' `(table, lo, hi)` contract; large blast radius |

## Decision

**Option A.**

Conservative per-file interval, parsed once at refresh:
- `dt=YYYY-MM-DD` dir → base interval `[day_start, day_end)`.
- Raw `HH-MM-SS-<uuid>.parquet`: flush wall-clock time tightens the **upper** bound to `flush_time + margin` (a file cannot contain events received after it was flushed; event time ≤ receive time + skew). The lower bound stays `day_start` (a batch may carry late events from earlier in the day).
- `compacted-hHH-*` → `[day_start + HH h, day_start + (HH+1) h)` widened by `margin` (reuse `hour_end_ns`).
- `compacted-<date>` / `rollup-<tier>` → the full day.
- **Unparseable name → interval `(-∞, +∞)` (always included).** Safety default over cleverness.
- `margin`: one documented constant, `1 h` wall-clock skew/lateness allowance — generous relative to the 30 s gateway flush cadence and consistent with the 24 h-margin philosophy of `sealed_ns` (`prometheus.rs:2069`). ⚠️ Needs verification during implementation: whether the sink's `%H-%M-%S` path template stamps write time or event time (`sol-gateway.yaml` file sink) — the interval rule above is safe for write-time stamping; if it is event-time stamping the bounds only get tighter.
- Include file iff `[lo − margin, hi + margin]` overlaps its interval. Callers without a window use the registered full table — unchanged.

Scoped DataFrames come from `ctx.read_table` on an **unregistered** provider, so the registered tables (and the deregister/register refresh race protocol) are untouched. The inventory and the registered providers are built from the same `build_providers` walk, so they cannot diverge.

## Consequences

- A 15-minute query touches only the active day's recent files (tens) instead of ~1,400 footers; sealed-day queries touch only their days.
- New invariant: **inventory and registered tables derive from the same walk** — refresh replaces both or neither.
- The engine gains one interior-mutability point (the inventory swap); everything else stays lock-free.
- `test_no_format_sql_in_core` (`src/querier/mod.rs:184`) still holds — all new code uses the `Expr`/`DataFrame` API.
- Rejected-option B's `dt` partition column remains available later if the inventory approach ever proves insufficient; nothing here forecloses it.
