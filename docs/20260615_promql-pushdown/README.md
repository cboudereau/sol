# promql-pushdown

Push the **relational core** of PromQL evaluation (scan → label/group-key extraction → grouping → aggregation → arithmetic → topk) into DataFusion logical plans so it inherits vectorised, parallel, spillable execution; keep a **thin Rust shell** only for the genuinely non-relational PromQL tail. The keystone is an in-plan **group-key column** that turns `by`, `without`, and nested aggregation into native DataFusion `GROUP BY` / chained `.aggregate()`. The endgame stores metric `attributes` as a dictionary-encoded Arrow `MAP<Utf8,Utf8>` (read columnar, no per-row JSON parse). **No backward compatibility** — clean cutover, the Parquet store is regenerated (old JSON-`attributes` files lack the MAP and are not read).

Status: **shipped** (all code tasks complete; querier tests green, clippy `-D warnings` clean). Verified live: multi-series `sum(rate)` reaches Sol↔Mimir parity (7.12% vs 6.97%).

## Design
- [2026-06-15_promql-pushdown](./designs/2026-06-15_promql-pushdown.md)

## ADRs (accepted)
- [aggregation-pushdown](./adrs/2026-06-15_aggregation-pushdown.md) — push the relational core into DataFusion; thin Rust shell for the non-relational tail. Supersedes the parquet-backend `promql-aggregate-evaluation` ADR.
- [group-key-format](./adrs/2026-06-15_group-key-format.md) — canonical sorted `k=v` group key (`\x1f`-joined, escaped) computed in-plan; `by`/`without`/all via `prom_group_key`.
- [relational-nonrelational-boundary](./adrs/2026-06-15_relational-nonrelational-boundary.md) — what stays relational vs. what the Rust shell evaluates.
- [materialized-label-columns](./adrs/2026-06-15_materialized-label-columns.md) — store `attributes` as Arrow `MAP` (general); per-key materialized columns deferred.
- [precomputed-counter-delta](./adrs/2026-06-15_precomputed-counter-delta.md) — windowed-rate semantics (RANGE-frame SUM of reset-adjusted deltas); precomputed delta column **declined** (LAG measured ~25% over scan).
