# Parquet Backend — Complexity & Cost Model

> Gate: this model must be validated **before** implementation ([TASKS.md](./TASKS.md) Phase 4a gate). It is how the once-uphill tasks (6, 11, 12) were de-risked analytically rather than by spike (now downhill).
> Design: [DESIGN.md](./DESIGN.md). Logs are modelled first (highest volume; the wedge to beat Loki/Grafana Cloud — [MARKET §9.1](../../otlp-as-core-protocol-plan/MARKET.md)).

## 1. Why a model before code

The architecture (compaction [FR7](./DESIGN.md#fr7), splitting [FR8](./DESIGN.md#fr8), rollups [FR6](./DESIGN.md#fr6)) is justified only if the **cost/latency** numbers hold at scale. We validate analytically against three real-world points and AWS pricing, then let the model tell us *where* each mechanism becomes necessary. No mechanism is built before the model shows it pays.

## 2. Parameters

| Symbol | Meaning | Demo | Midpoint | Ceiling |
|---|---|---|---|---|
| `I` | log ingest (GB/day) | ~1 | **500** (Datadog-typical org) | **30 000** (≈1 PB/mo, Loki large cluster) |
| `b` | avg bytes/log event (on wire) | ~500 | ~500 | ~500 |
| `E` | events/day = `I·1e9/b` | ~2 M | ~1.0 B | ~60 B |
| `evt/s` | mean event rate | ~25 | ~11.6 k | ~700 k |
| `K` | active streams (label-set cardinality) | ~50 | ~50 k | ~1–5 M |
| `Fʟ` | gateway flush interval (s) | 30 | 30 | 30 |
| `cmp` | log compression (zstd, JSON-ish) | ~10× | ~10× | ~10× |
| `Rᵢ` | retention (days, ≥ query interval) | 30 | 30 | 30 |
| `Qᵢ` | **query interval** (LogQL, [NFR7](./DESIGN.md#nfr7)) | ≤30d | ≤30d | ≤30d |

Reference sources: [Loki prod 1.16 TB/day @ 34k lines/s](https://dev.to/sriramrajendran/running-grafana-loki-in-production-what-we-actually-learned-d9g), [Loki scale/sizing ~1 PB/mo](https://grafana.com/docs/loki/latest/setup/size/), [AWS S3 pricing](https://aws.amazon.com/s3/pricing/), [Datadog ~500 GB/day org](https://www.datadoghq.com/blog/observability-pipelines-log-volume-control/).

## 3. The cost driver: file count & S3 requests (the small-files problem, quantified)

Raw files per day per signal: `Nʀ = 86 400 / Fʟ = 2 880` (at 30 s flush) — **independent of volume** (volume grows file *size*, not count… until a single flush exceeds a target size, then count grows with volume too).

A LogQL range query over `D` days touches, **before** compaction:
```
files_opened ≈ D · Nʀ           # 7-day query → 20 160 files
S3 GETs      ≈ files_opened · (1 + cols_scanned_chunks)
S3 GET $     ≈ GETs · $0.0004/1e3
latency      ∝ files_opened       (per-file footer fetch = a round-trip)
```

After compaction to `Nᴄ` files/day (e.g. 1/day, or 24 hourly):
```
files_opened ≈ D · Nᴄ           # 7-day query → 7 files
```

| Query | Raw (no compaction) | Compacted (1/day) | Improvement |
|---|---|---|---|
| 7-day log range — files opened | 20 160 | 7 | **~2 880×** |
| 7-day — S3 GET requests (footers only) | ~20 160 | ~7 | ~2 880× |
| 7-day — request $ (footers) | ~$0.008 | ~$0.000003 | — |
| 7-day — **latency from round-trips** (S3 first-byte ≈10–100 ms/GET; local FS sub-ms; partially parallel) | many seconds | ms | **the real win** |

**Conclusion C1 — compaction is mandatory, and it's a latency argument, not a storage one.** S3 GET $ is tiny either way; the killer is **round-trips** (`files_opened`), and S3 first-byte latency is **tens of ms, not sub-ms** — 20 160 footer fetches even at 100-way parallelism ≈ 20 160/100 × ~30 ms ≈ **~6 s** just to open files. This holds at *every* scale, even the demo. → [FR7](./DESIGN.md#fr7) is not optional.

### 3a. S3 request-rate limits (not just $ and latency)

S3 enforces **~5 500 GET/HEAD per second and ~3 500 PUT/POST/DELETE per second _per prefix_** ([S3 performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)); exceeding a prefix's rate returns **`503 SlowDown`**. The small-files problem pushes against this ceiling:

- **Reads**: post-compaction a 30-day log query ≈ 30 files × a few GETs ≈ ~100 GETs; a 130-query dashboard refresh ≈ **~13 000 GETs in a ~2 s burst ≈ 6 500 GET/s** — *at the per-prefix limit*. **Pre**-compaction (thousands of files/query) it is **100×+ over** → guaranteed throttling. So compaction is also an S3-**rate** mitigation, not only latency/$.
- **Mitigations** (all already in the design): (a) **prefix sharding** — `dt=YYYY-MM-DD/` + per-signal/subtype dirs ([FR7](./DESIGN.md#fr7)) spread load across prefixes, and S3 scales rate *per prefix*, so aggregate ceiling rises with prefix count; (b) **caching** — the query-result + per-day immutable cache ([FR5](./DESIGN.md#fr5)/[FR8](./DESIGN.md#fr8)) serve repeat refreshes with **zero** GETs (the 15 s-refresh pattern would otherwise hammer one prefix); (c) **retry with exponential backoff** on `503` (the `object_store` crate does this — configure it); (d) **bounded LIST** — LIST is paginated (1 000 keys/page) and rate-limited, so re-listing a prefix cost scales with object count → yet another compaction argument.
- This is formalised as [NFR10](./DESIGN.md#nfr10).

## 4. Bytes scanned (the second driver) & where splitting/rollups kick in

With day-partitioning + predicate pushdown + column projection, a selective LogQL query scans only matching row-groups of the needed columns:
```
bytes_scanned ≈ (stored bytes in [Q] days)                       # worst case: no selectivity
              · sel_time   (row-group pruning by dt + time)
              · sel_label  (row-group stats / bloom on service_name)
              · proj       (only body + queried label columns, not all 18)
```
Stored bytes/day (logs) = `I / cmp` → midpoint **50 GB/day stored**, ceiling **3 TB/day stored**.

- **Logs (Qᵢ ≤ 30d)**: a 30-day midpoint query worst-case scans ~1.5 TB; with `proj`≈0.3 and `sel`≈0.01–0.1 (label/time selective) → 5–150 GB scanned. DataFusion at ~1–2 GB/s/core over S3 → sub-second to seconds. **Splitting helps (parallelism + per-day cache) but logs do not need rollups** — the window is short. ✔ matches [NFR7](./DESIGN.md#nfr7).
- **Metrics (Qᵢ 13 mo default, 2 y opt-in)**: worst-case scan over the long tail is infeasible (`bytes ∝ Q`). This is where **splitting + per-day immutable cache** (refresh re-reads 1 day, not ~395/730) and **rollups** (vs 15 s raw: 5m ≈ 20×, 1h ≈ 240×, 1d ≈ 5760×) become necessary. → [FR6](./DESIGN.md#fr6)/[FR8](./DESIGN.md#fr8) justified *only* for metrics.

**Conclusion C2 — splitting/rollups are a metrics-only requirement driven by query interval, not volume.** Logs (short interval) need compaction + pushdown + bloom, not rollups.

## 5. Storage cost (AWS), per scale point — logs

`storage_GB_resident ≈ (I / cmp) · Rᵢ`; `$/mo = GB · tier_rate`.

| | Demo | Midpoint (500 GB/day) | Ceiling (30 TB/day) |
|---|---|---|---|
| Stored/day (zstd 10×) | 0.1 GB | 50 GB | 3 TB |
| Resident @30d | 3 GB | 1.5 TB | 90 TB |
| S3 Standard $/mo | ~$0.07 | ~$35 | ~$2 070 |
| + Glacier-IA cold tail (>Qᵢ) | — | optional | large saving (×0.04) |
| Write PUT $/mo (raw `Nʀ`·30d) | ~$0.4/signal | ~$0.4/signal | ~$0.4/signal — **volume-independent** (≈$3/mo all 7 signals; file count set by flush interval) |

> **$/mo basis**: S3 Standard at a flat **$0.023/GB·mo** (first-tier, a conservative upper bound — real tiered pricing drops to $0.022/$0.021 above 50/500 TB, so the multi-PB metric figures below are ~7% high). Excludes request charges (modelled in §3) and Glacier savings. `GB = 1e9 bytes`.

**Conclusion C3 — storage is cheap and comparable to Loki** (both compressed on S3; Loki chunks 70–80% + index 10–20%). Sol's edge is *not* storage price — it's (a) **SQL-queryable** open Parquet vs Loki's opaque chunks, and (b) **fewer always-on components** (see §6).

## 6. Compute cost & the "beat Loki" thesis (logs)

Loki runs an always-on fleet: distributors, ingesters, queriers, query-frontend, **index-gateway**, compactor. Sol's read path is **stateless queriers + one compactor** over the same S3:

| Dimension | Loki | Sol (this design) | Winner (logs) |
|---|---|---|---|
| Storage | chunks + index on S3, ~10× compress | Parquet+zstd on S3, ~10× | ~tie |
| Always-on components | distributor/ingester/querier/frontend/index-gateway/compactor | stateless querier (scale-to-low) + singleton compactor | **Sol** (fewer, can idle) |
| Selective label lookup | index → fast | row-group stats + **bloom on `service_name`** (mapping decision §7) | Loki, unless Sol adds bloom → tie |
| Full-text / `\|~` regex | scans chunks | scans `body` column (projected) | ~tie (both scan) |
| **Analytical / aggregate log queries (SQL)** | LogQL metric queries, limited | full SQL, JOINs, any agg | **Sol** |
| Open format / own-your-data | opaque chunks | standard Parquet, any engine | **Sol** ([MARKET §7.3](../../otlp-as-core-protocol-plan/MARKET.md)) |

**Conclusion C4 — to beat Loki on logs we must (1) compact (C1), (2) add a bloom filter on `service_name` (and promote 1–2 hot labels) so selective lookups match Loki's index, and (3) lean on SQL analytics + open format as the differentiators.** Storage parity is assumed; the win is fewer components + queryability.

## 7. Metrics estimation

Driver = **active series `Kₘ`** (cardinality), not byte volume. samples/day = `Kₘ · 86 400/scrape`; rows/day in Sol = samples/day (one Parquet row per datapoint).

| Param | Demo | Midpoint | Ceiling |
|---|---|---|---|
| `Kₘ` active series | ~5 k | **1 M** | **100 M** (≈10% of [Mimir's 1 B max](https://grafana.com/blog/2022/04/08/how-we-scaled-our-new-prometheus-tsdb-grafana-mimir-to-1-billion-active-series/)) |
| scrape (s) | 15 | 15 | 15 |
| samples/s | ~330 | ~67 k | ~6.7 M |
| rows/day | ~28 M | ~5.8 B | ~576 B |

**Key finding M1 — Sol's denormalised row-per-datapoint Parquet is storage-heavy for metrics vs a purpose-built TSDB.** Mimir stores ~**2 bytes/sample** (delta+XOR compacted). A Sol metric row carries `service_name`, `name`, JSON `attributes`, timestamps, value, resource/scope. The mitigation is Parquet **dictionary + RLE on the low-cardinality repeated columns** (maximised by the [FR7](./DESIGN.md#fr7) sort order) + zstd — realistically **~15–30 bytes/datapoint effective**, i.e. **~10× Mimir**. This is Sol's weakest signal on storage price.

| | Demo | Midpoint (1 M series) | Ceiling (100 M series) |
|---|---|---|---|
| Stored/day (Sol, ~20 B/dp) | ~0.5 GB | ~115 GB | ~11.5 TB |
| Resident — **13 mo default** | ~0.2 TB | **~46 TB** | **~4.6 PB** |
| Resident — 2 y opt-in ceiling | ~0.4 TB | ~84 TB | ~8.4 PB (impractical) |
| AWS S3 $/mo (raw, 13 mo default) | ~$5 | ~$1 040 | ~$105 k |
| Same in Mimir (2 B/sample, 13 mo) | — | ~$100 | ~$10 k |
| Raw rows for one full-range `histogram_quantile` (2 y) | — | ~4.2 T | ~420 T (infeasible) |

**Conclusion M2 — metrics are the highest *query* complexity (windowed `rate`/`histogram_quantile` over billions of rows) AND storage-inefficient vs a TSDB. Both are fixed by the same lever:** the cold tail (beyond a configurable recent window) must be served **rollup-only** ([FR6](./DESIGN.md#fr6)) — vs 15 s raw, 1h rollup cuts rows ~240×, 1d ~5760× — with raw aged out to Glacier or dropped. This is what makes even the 13 mo default affordable and the 2 y opt-in feasible. Splitting + immutable per-day cache ([FR8](./DESIGN.md#fr8)) handle the 15s-refresh repetition. **Sol does not beat Mimir on metric storage $; it wins on unified SQL + cross-signal + open format** — accept the trade-off, or run rollup-only retention for very-high-cardinality shops.

## 8. Traces estimation

Driver = **point lookup** (trace-by-id) + bounded search over the window (`Qᵢ 30 d default, matching Grafana Cloud; 7 d opt-in for cost`). [Tempo itself stores Parquet + bloom filters](https://grafana.com/docs/tempo/latest/) — so Sol is **architecturally identical** here; parity, not a leap.

| Param | Demo | Midpoint | Ceiling |
|---|---|---|---|
| spans/s (post-sampling) | ~tens | **50 k** | **1 M** ([Grafana Labs: 2.2 M/s](https://grafana.com/docs/tempo/latest/set-up-for-tracing/instrument-send/best-practices/)) |
| bytes/span (raw, Tempo ref) | ~300 | ~300 | ~300 |
| spans/day | ~1 M | ~4.3 B | ~86 B |
| Stored/day (Sol row, dict+zstd ~150 B) | ~0.15 GB | ~650 GB | ~13 TB |
| Resident @ 30 d (default) | ~4.5 GB | ~19.5 TB | ~390 TB |
| AWS S3 $/mo (30 d) | ~$0.10 | ~$450 | ~$8 900 |
| (7 d opt-in) | ~1 GB / ~$0.02 | ~4.5 TB / ~$100 | ~90 TB / ~$2 100 |

**Conclusion T1 — traces are still the cheapest signal to get right.** At the 30 d default the resident set is ~4× the old 7 d figure but bounded; the verdict holds. **No rollups; splitting optional** (a 30 d trace search benefits from the same per-day splitting as logs, but the dominant path is the point lookup). The decisive lever is the **`trace_id` bloom filter** ([FR4](./DESIGN.md#fr4)) for sub-150ms point lookups (without it, trace-by-id is a full scan). TraceQL search is a pushdown scan over ≤30 d with `json_extract` on attributes (cost-flagged, §9). Sol matches Tempo architecturally (same Parquet + bloom, same 30 d retention); the differentiator is unified SQL + cross-signal JOINs, not trace storage.

## 9. Mapping trade-offs surfaced by the model (feeds [QUERY-MAPPING.md](./QUERY-MAPPING.md))

The model flags LogQL/PromQL constructs whose naive translation breaks NFR5/NFR6 — these get a *restricted/costly* decision rather than blind support:

| Construct | Cost risk | Decision |
|---|---|---|
| LogQL live tail (`/tail` WS) | needs hot/unflushed data | ⛔ unsupported (hot data = [non-goal](./DESIGN.md#non-goals)) |
| LogQL `\|~` unbounded regex | full `body` scan | ⚠️ supported, cost-flagged; pushdown `\|=` substring first |
| LogQL query-time `json`/`logfmt` parse + high-card `sum by` | per-row parse + cardinality blowup | ⚠️ supported, bounded by `limit`/series cap |
| PromQL `histogram_quantile` over raw long-range | UNNEST over huge scan | ✅ via rollups (FR6) + splitting (FR8); raw only for recent |
| PromQL subqueries / `predict_linear`/`holt_winters` | unbounded inner range eval | ⛔ deferred (NFR) |
| PromQL `absent` / `absent_over_time` | needs full series catalog | ⛔ deferred (consistent with [QUERY-MAPPING §2.3](./QUERY-MAPPING.md)) |
| Selective `{service_name=…}` | — | ✅ bloom + row-group pruning (C4) |

## 10. What this model proves (and the validation method)

- **Validated analytically** (no spike): C1 compaction mandatory (round-trips); C2 rollups/splitting metrics-only; C3 storage cheap; C4 beat-Loki = fewer components + bloom + SQL.
- **Open quantities to confirm during build** (record measured vs modelled):
  1. DataFusion scan throughput GB/s/core over S3 (assumed 1–2) → sets the bytes-scanned latency.
  2. UNNEST cost for `histogram_quantile` (rabbit hole 5) — modelled as rows×buckets; if it exceeds budget, the Rust-native fallback path is taken (already the escape hatch).
  3. Bloom-filter false-positive rate on `service_name` at cardinality `K`.
- **Drives uphill → downhill**: tasks 6/11/12 now have explicit cost models and fallback paths, so their *approach* is settled; only constants remain (measured during the task, not blocking the plan).

## 11. Per-signal summary

| Signal | Qᵢ | Compaction | Bloom | Splitting | Rollups | Dominant cost | vs incumbent |
|---|---|---|---|---|---|---|---|
| **Logs** | ≤30d | **required** (C1) | **required** on `service_name` (C4) | helpful | no | round-trips + `body` scan | **beat Loki** (fewer components + SQL + open format) |
| **Traces** | 30d (7d opt-in) | required | **on `trace_id`** (FR4) | optional | no | point-lookup | **parity with Tempo** (same Parquet+bloom, same 30d) |
| **Metrics** | 13mo (2y opt-in) | required | — | **required** (FR8) | **required** (FR6) | windowed scan over billions of rows | **lose to Mimir on storage $**, win on SQL/unification → rollup-only cold tail |

**Net:** logs are the wedge (highest volume, clearest win); traces are easy parity; metrics are the hardest (query complexity + storage inefficiency) and need the full FR6+FR7+FR8 machinery, with rollup-only cold retention as the escape valve at high cardinality.
