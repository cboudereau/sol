---
status: accepted
---
# Resource limits

Addresses: [FR2](../designs/20260513_benchmark-sol-vs-otelcol.md#fr2), [NFR1](../designs/20260513_benchmark-sol-vs-otelcol.md#nfr1), [NFR2](../designs/20260513_benchmark-sol-vs-otelcol.md#nfr2)

## Problem

Without resource limits, results depend on what else is running on the host. Docker Compose `deploy.resources.limits` ensures both systems get equal, capped resources — making results comparable across machines.

## Options

| Option | Pros | Cons |
|---|---|---|
| 2 CPU / 2 GB per system | Fits on laptops (8 core). Constrains enough to show efficiency differences. | May saturate at 50k/s scenarios, measuring generator not pipeline. |
| 4 CPU / 4 GB per system | More headroom for high-rate scenarios. | Needs 16+ cores to also run load gen + Prometheus + cAdvisor. |
| No limits | Simpler config. Shows "real" performance on the test machine. | Results not reproducible across machines. One system could monopolize resources. |

## Decision

**2 CPU / 2 GB memory per system.** This is the most portable constraint — works on an 8-core laptop with headroom for the load generator and monitoring. At 50k/s, if both systems saturate equally, the comparison is still valid (same ceiling, different resource usage to get there).

The load generator gets 2 CPU / 1 GB. Prometheus + cAdvisor share the remaining cores without limits (low overhead).

## Consequences

- Results header must state the resource limits used.
- High-rate scenarios (50k/s) may be generator-bottlenecked rather than pipeline-bottlenecked. The report must note this if achieved rate < 90% of target.
- Users with more cores can override limits via environment variables.
