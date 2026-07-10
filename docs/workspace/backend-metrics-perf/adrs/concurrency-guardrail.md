---
status: proposed
---
# Concurrency guardrail: enforce `max_concurrent_queries` or remove it

Addresses: [FR5](../DESIGN.md#fr5), [NFR2](../DESIGN.md#nfr2)

## Problem

`guardrails.max_concurrent_queries` is parsed with default 16 (`src/config/querier.rs:80,124`) and set to 16 in the demo config (`demo/otel-sol-grafana-dotnet/sol/sol-querier.yaml:22`), but no code in `src/` references it — there is no semaphore, no admission control. Measured: a 20-query dashboard burst runs fully unthrottled at ~968 % CPU. A configured guardrail that does nothing misleads operators and leaves the querier open to CPU collapse under fan-out (the original incident was 225 % querier CPU on 7-day dashboards).

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Enforce: `tokio::sync::Semaphore(max)` around query execution; bounded wait (short timeout) then 503 + `Retry-After` | Config becomes truthful; protects the node under overload; trivially small | Under sustained overload some panels see errors (Grafana retries); timeout constant to choose |
| B. Enforce: unbounded FIFO wait (pure queueing, no shed) | No user-visible errors; simplest semantics | Queue can grow without bound; latency collapse hides overload instead of surfacing it |
| C. Remove the field from config schema | Honest; zero code | Loses a needed protection (overload is real and measured); breaks existing configs referencing it |
| D. Status quo (parsed, unenforced) | — | Rejected: silently lying config is worse than either alternative |

## Decision

**Recommendation: A.** Acquire with a short bounded wait (e.g. 5 s, constant documented in code); on timeout return 503 with `Retry-After`. Placement (explorer-verified): an `Arc<tokio::sync::Semaphore>` on `QueryEngine`, acquired inside the execution entry points `sql`/`collect`/`sql_user` (`src/querier/catalog.rs:519, 566, 591`) — this covers every query path (Prometheus/Loki/Tempo/SQL) at the choke point where work actually starts, mirrors how `max_bytes_scanned` already lands on the engine (`catalog.rs:492`), and leaves health/static routes untouched. The existing `InflightGuard` `warp::wrap_fn` (`src/querier/routes.rs:636-641`) stays as the per-request gauge; the semaphore is not a warp filter so internal callers are bounded too. With [FR1](../DESIGN.md#fr1)+[FR3](../DESIGN.md#fr3) landed, 16 in-flight queries are far below saturation, so the limit should be invisible in normal operation and bite only under genuine overload.

## Consequences

- Overload becomes observable (shed counter + 503s) instead of manifesting as node-wide latency collapse.
- The demo config value (16) becomes meaningful; docs/config comments must state the semantics (bounded wait, then shed).
- A new failure mode (503 under burst) that Grafana handles by design; the shed path needs a test and a `sol_querier_*` counter.
