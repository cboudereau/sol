---
status: accepted
---
# Tail sampling policy equivalence

Addresses: [FR7](../DESIGN.md#fr7), [NFR1](../DESIGN.md#nfr1)

## Problem

The o11y-weekly otelcontribcol pipeline and Sol demo use different tail sampling configurations that evolved independently. For a fair benchmark, both must evaluate equivalent sampling logic. However, the two systems have different architectural constraints that make exact equivalence impossible.

### otelcontribcol architecture (two sequential processors)

```
otlp receiver
  → tail_sampling/latency-error (decision_wait: 10s)
      policies:
        1. latency: threshold_ms 100      (keep traces > 100ms)
        2. AND(status_code ERROR, string_attribute error.type !~ /4../)
                                          (keep server errors, not 4xx)
  → tail_sampling/probabilistic
      policies:
        1. probabilistic: 10%             (keep 10% of survivors)
  → nop exporter
```

Traces are buffered TWICE (once per processor). `decision_wait` applies twice. A trace that passes the latency-error processor waits another `decision_wait` in the probabilistic processor before being forwarded.

### Sol architecture (single transform, first-match-wins)

```
otlp source
  → tail_sampling (decision_wait: 10s)
      policies (first match wins):
        1. AND(latency > 100ms, probabilistic 10%)
        2. latency > 500ms
        3. AND(status_code ERROR, string_attribute http.response.status_code !~ /4../)
  → blackhole sink
```

Traces are buffered ONCE. One `decision_wait`. First matching policy decides.

### Key differences

| Aspect | otelcontribcol | Sol |
|---|---|---|
| Processors/transforms | 2 sequential | 1 |
| Trace buffers | 2 (double memory) | 1 |
| Decision wait cycles | 2 × 10s = 20s total | 1 × 10s |
| Probabilistic applied to | ALL survivors of latency-error | Only traces matching latency > 100ms (policy 1) |
| Error traces | 100% kept (no probabilistic filter on errors) | 100% kept (policy 3 is not inside probabilistic) |
| Traces > 500ms | Not special-cased (pass latency > 100ms, then 10% survive) | 100% kept (policy 2) |
| Error attribute key | `error.type` | `http.response.status_code` |

## Options

| Option | Pros | Cons |
|---|---|---|
| **A. Align Sol to match otelcol exactly** — remove policy 2 (>500ms), apply probabilistic to all survivors | Identical sampling decisions | Loses Sol's expressiveness advantage; doesn't match what Sol actually does in production |
| **B. Align otelcol to match Sol** — use single processor with AND policies | Impossible — otelcol can't express first-match-wins + AND + probabilistic-within-AND in a single processor | Not feasible |
| **C. Use equivalent but not identical policies** — same intent, different structure | Both get their natural/idiomatic config. Report documents the differences. | Sampling rates may differ slightly (e.g., Sol keeps all >500ms traces, otelcol keeps 10% of them) |
| **D. Use simplified equivalent policies** — just probabilistic 10% + status_code ERROR | Easy to make identical. Exercises the tail sampling machinery. | Doesn't showcase Sol's AND composition advantage |

## Decision

**Option C — equivalent intent, idiomatic configs.** Each system gets its natural configuration for the same use case: "keep errors (excluding 4xx), keep slow traces, sample everything else at 10%."

The benchmark report documents:
1. The architectural difference (1 processor vs 2) and its impact on memory (double buffering)
2. The policy differences (Sol keeps all >500ms; otelcol keeps 10% of them)
3. That the throughput comparison is valid regardless — both systems do tail sampling work (buffer traces, evaluate policies, make decisions)

To align the error attribute key, Sol's config uses `error.type` (same as otelcol) instead of `http.response.status_code`.

Additionally, both configs use:
- `decision_wait`: 10s
- `num_traces`: 50000
- `decision_cache.sampled_cache_size`: 100000
- `decision_cache.non_sampled_cache_size`: 100000

## Consequences

- The report must include a "Fairness notes" section explaining the architectural difference.
- Sampling rates may differ by a few percent — this is acceptable because the benchmark measures pipeline throughput and resource usage, not sampling accuracy.
- The double-buffer architecture of otelcontribcol is a legitimate disadvantage that the benchmark reveals (higher memory, longer total decision latency). This is a real operational difference, not a testing artifact.
