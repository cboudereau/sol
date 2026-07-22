# rate-row-work — rate() execution-cost reduction (frame shrink + stored series-key)

Final follow-up of the querier-perf arc (after [promql-plan-cache](../20260717_promql-plan-cache/README.md) and [write-side-small-files](../20260720_write-side-small-files/README.md)). After file count and plan cost were handled, `rate()` was execution-bound (~835 ms live execute); this workspace attacked the row-level cost.

## Delivered (5 tasks, 3 sessions; commits `eeb97e12a`…`dc0a56933`)

- **FR1 — rate() frame reduction** ([ADR](./adrs/rate-frame-reduction.md)): `frame.rs::rate` from 6 window passes to 5 — dropped `MAX(t)` (= current-row t on a CURRENT ROW frame), `MIN(t)`→`FIRST_VALUE(t)` fused into the leading-row family, dropped `duration_to_end` (≡ 0). Bit-identical within the 1e-6 extrapolation goldens + instant==range parity. No schema change.
- **FR2 — stored `prom_series_key` column** ([ADR](./adrs/series-key-column.md)): a REQUIRED metric-schema column computed at write time via a **shared `sol-core` `series_key` function** (write and read call the same code — cannot diverge). Read/rollup partition on the plain column; the per-row UDF is gone from every metric window/aggregate/rollup path. Clean cutover + store wipe; logs/traces untouched.
- **FR3 — declared sort order (partial)**: `with_file_sort_order` on the metric tables + a drift guard (declaration == authoritative write sort). The window `SortExec` **elision is blocked** by a DataFusion-53 limitation — see below.
- Suite 261 → 266 / 0 / 2 across the arc; `make check-clippy` green throughout.

## Live verdict ([VERIFY.md](./VERIFY.md), clean quiet-box)

**execute 835 → 35 ms (~24×)**; repeated-shape `rate()` **~300 → 74–113 ms** (NFR1 ≤ 80 ms met best-case, mean ~99 ms); 20-query burst **~2.3 s → ~0.5–0.6 s** (NFR2 borderline); warm path 5.5 ms now **beats Mimir** (23 ms). Both inherited NFRs are **at target**. Measurement caveat learned the hard way: only measure on a quiet host — a WSL2 loadavg artifact (5.58 with idle containers) inflated every timing ~6× including the querier's own stage spans, discarding a full round.

## Remaining lever (documented, not opened)

Physical planning (62 ms) is now the dominant stage — that is what FR3's SortExec elision would cut. It is blocked because the window ORDER BY's `CAST(time_unix_nano AS Int64)` (needed for the ns RANGE frame) isn't treated as order-preserving vs the declared Timestamp order (control: raw-time ORDER BY elides to 0 SortExec). **Unblocking needs a stored Int64 ns time column** used by both the declared file sort and the window ORDER BY — a further clean-cutover, the next follow-up if NFR1/NFR2 must be cleared comfortably rather than at-target.

## Arc close

This closes the querier-perf arc opened from the demo analysis: per-query fixed cost went from "opens the whole store, wipes its cache every 15 s, no coalescing, unbounded metadata, dead guardrail, per-row UDF over every scanned row, 6-window rate plan" to time-scoped + plan-cached + single-flighted + bounded + write-compacted + a 5-window rate over a stored partition key — a dashboard `rate()` panel from ~370 ms to ~75 ms cold-repeat / 5.5 ms warm.
