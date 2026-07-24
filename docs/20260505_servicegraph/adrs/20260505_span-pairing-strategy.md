---
status: draft
---
# Span pairing strategy for servicegraph

Addresses: [FR3](../designs/20260505_servicegraph.md#fr3), [NFR2](../designs/20260505_servicegraph.md#nfr2)

## Problem

The servicegraph transform needs to pair client spans with server spans to compute edge metrics. There are multiple ways to identify which spans form a client→server pair.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Parent span ID matching | Precise: client span_id == server parent_span_id. Standard OTel semantic. | Requires both spans in the same trace to arrive at the servicegraph instance. Only works if load balancing routes by traceID (which we do). |
| B. Same trace, opposite kind | Simpler: any CLIENT+SERVER in same trace form a pair | False positives in complex traces with multiple hops |
| C. Hybrid: parent_span_id first, same-trace fallback | Best coverage | More complex, two code paths |

## Decision

Option A — parent span ID matching. The load balancer already routes by traceID, so all spans of a trace arrive at the same servicegraph instance. Parent span ID matching is the standard approach used by OTel Collector Contrib's servicegraph connector.

## Consequences

- Simpler implementation (single matching strategy)
- Requires that servicegraph runs downstream of the load balancer (which it does — it's in sol-collector)
- Spans without a matching parent will expire as unpaired (counted in `unpaired_spans_total`)
