---
status: accepted
---
# Measurement source

Addresses: [FR3](../designs/20260513_benchmark-sol-vs-otelcol.md#fr3), [FR6](../designs/20260513_benchmark-sol-vs-otelcol.md#fr6)

## Problem

We need to measure throughput, CPU, and memory for both systems. Multiple approaches exist — internal metrics, cAdvisor, `docker stats`, or external instrumentation. We need a single consistent approach.

## Options

| Option | Pros | Cons |
|---|---|---|
| **Prometheus + cAdvisor + internal metrics** | Standard stack. Container-level CPU/mem from cAdvisor. Throughput from each system's own counters. Queryable post-run. | cAdvisor may not work on WSL2. Three moving parts. |
| **`docker stats` polling** | Zero dependencies. Works everywhere. | Coarse granularity (1s). No queryable history. Must parse text output. |
| **Internal metrics only** (each system's Prometheus endpoint) | Simpler — no cAdvisor needed. Sol exposes `component_*` metrics, otelcol exposes `otelcol_*` metrics. | CPU/memory not available from internal metrics — need another source for resource usage. |
| **Hybrid: internal metrics for throughput, `docker stats` for resources** | Works everywhere. No cAdvisor dependency. Throughput from accurate internal counters. | Slightly more complex script. `docker stats` granularity is 1s. |

## Decision

**Hybrid approach.** Use each system's internal Prometheus metrics for throughput (most accurate — they count actual events processed). Use `docker stats --no-stream` in a polling loop for CPU and memory (works everywhere, including WSL2, no cAdvisor dependency).

Prometheus is still used to scrape internal metrics for throughput data — it provides a queryable time-series we can extract post-run. Resource metrics (CPU/mem) come from `docker stats` logged to a CSV file.

If cAdvisor is available (Linux with cgroup v2), it can be enabled optionally for higher-fidelity resource data. But the default path must not depend on it.

## Consequences

- `run.sh` starts a `docker stats` polling loop as a background process, writing to `results/raw/docker-stats.csv`.
- Prometheus scrapes Sol on `:9090/metrics` (internal metrics endpoint) and otelcol on `:8888/metrics` (default Prometheus endpoint).
- Post-run query uses `curl` against Prometheus HTTP API to extract throughput rates.
- CPU/memory peaks are derived from the CSV file via `sort`/`awk`.
