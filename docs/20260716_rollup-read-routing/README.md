# rollup-read-routing — one tier-resolution choke point + operator→capability routing

Every metric read path (range-agg, range-histogram, instant, instant-histogram, metadata) routes through one `resolve_metric_windows` choke point with an **operator→capability** model (`Last`/`MinMax`/`SumCount`/`None`) selecting the coarsest rollup tier that can answer *correctly* — replacing step-only routing that silently dropped peaks (`max_over_time`) and forced instant/metadata to raw. The write side was included deliberately (FR6/FR7): the rollup carries per-bucket `{last,min,max,sum,count}` so `max/min/avg/sum/count_over_time` use a tier **and** match raw.

- **Design**: [designs/rollup-read-routing.md](./designs/rollup-read-routing.md)
- **ADRs** (accepted): [tier-resolution-choke-point](./adrs/tier-resolution-choke-point.md), [operator-safety-allowlist](./adrs/operator-safety-allowlist.md), [rollup-aggregate-schema](./adrs/rollup-aggregate-schema.md), [instant-and-metadata-routing](./adrs/instant-and-metadata-routing.md)
- **Implemented**: S1 `cab4128d3`, S2 `74baff9ce`, S3 `b6a81b3b5`; review fixes `bea64417d`; live-parity fixes `9e60ac764`, `d8d25be16`, `d1305ef82` (instant-path regressions caught by Sol↔Mimir comparison); tier under-use fix `bfc273f91` (`sealed_ns` wall-clock).
- **Verified live** (pre-close): exact Sol↔Mimir parity for gauges/max/avg/count + instant per-series rate; tier scans 15–54× fewer rows, ~6× faster cold (sealed day: 268k/88k/74k vs 4.03M raw rows; 90 ms vs 533 ms), tier values exact vs Mimir.

## Open / deferred at close (not blocking; each has an owner or trigger)

- avg/sum/count_over_time over a tier are exact only for bucket-aligned windows (≤1-bucket edge approximation otherwise) — the ADR "exact" claim is qualified accordingly.
- Day-aligned sealed boundary (capture the last sealed day; needs a tier-coverage cap).
- Broader post-rebuild Sol↔Mimir parity sweep + 7-day CPU check require sealed days + regenerated rollups on the wiped store — re-run once days seal (probe set: [backend-metrics-perf VERIFY](../20260716_backend-metrics-perf/VERIFY.md)).
- RANGE-rate zigzag and instant `histogram_quantile` dispatch were split out (zigzag closed by [range-rate-parity](../20260716_range-rate-parity/README.md); `histogram_quantile` instant dispatch still open, unowned).
