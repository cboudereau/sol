---
status: draft
---
# Channel tuning strategy: match server defaults vs independent values

Addresses: [FR1](../DESIGN.md#fr1), [FR3](../DESIGN.md#fr3), [NFR1](../DESIGN.md#nfr1)

## Problem

The OTLP gRPC client (sink) uses all tonic `Endpoint` defaults. The server side was already tuned with specific H2 window sizes, adaptive window, and keepalive. The client needs tuning — but should client values mirror the server, or be independently chosen?

## Options

| Option | Pros | Cons |
|---|---|---|
| **A. Mirror server values** | Symmetric config — easy to reason about. Both sides start with the same window sizes; BDP estimation adjusts from there. One set of constants to maintain. | Client and server have different flow control roles; mirroring may not be optimal in all topologies. |
| **B. Independent client values** | Can optimize for client-specific patterns (e.g., larger send windows for high-throughput sinks). | Two sets of magic numbers to maintain. Risk of drift. Harder to explain why they differ. |

## Decision

**Option A — mirror server values.** The Sol OTLP gRPC client channel uses the same H2 window sizes (1 MB stream / 2 MB connection), adaptive window, and keepalive intervals as the server.

Rationale:
- In the LB topology, Sol talks to Sol — symmetric config is correct by construction.
- With adaptive window enabled, initial values are just starting points; BDP estimation converges to the right size regardless.
- The 50k traces gap is caused by the client using 64 KB defaults vs the server's 1 MB — matching eliminates the mismatch. Further per-side tuning is premature optimization.
- A shared helper function (`build_otlp_channel`) enforces consistency and prevents drift.

## Consequences

- All three `Channel::builder` call sites use identical tuning via a shared helper.
- If future profiling shows the client needs different values (e.g., for cross-datacenter forwarding), the helper can be parameterized — but not until there's evidence.
- `tcp_nodelay(true)` and `connect_timeout(5s)` are client-only additions (server already sets TCP_NODELAY in `incoming.rs`; connect timeout is client-only by nature).
