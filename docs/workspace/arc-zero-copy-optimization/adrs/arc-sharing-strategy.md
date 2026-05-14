---
status: accepted
---
# Arc sharing strategy for Resource, Scope, and attributes

Addresses: [FR1](../DESIGN.md#fr1), [FR2](../DESIGN.md#fr2), [FR3](../DESIGN.md#fr3), [FR4](../DESIGN.md#fr4), [NFR1](../DESIGN.md#nfr1), [NFR2](../DESIGN.md#nfr2)

## Problem

`resource_spans_into_events()` deep-clones `Resource` and `InstrumentationScope` for every span. This causes:
1. ~100 MiB extra memory in tail sampling buffers (1000 identical copies of the same Resource attributes)
2. Allocator contention at 50k spans/s reducing throughput

Three sharing strategies were considered.

## Options

| Option | Pros | Cons |
|---|---|---|
| A: Arc Resource + Arc Scope + Arc OtelAttributes | Maximum sharing; eliminates all per-span cloning for resource/scope data; copy-on-write preserves mutation semantics | Arc overhead (16 bytes per Arc); `Arc::make_mut` adds complexity to mutation paths |
| B: Arc Resource + Arc Scope only (not attributes) | Simpler; fewer mutation-path changes | OtelAttributes (BTreeMap) is the actual memory cost — Resource shell is tiny after attribute extraction. Saves little memory. |
| C: Zero-copy protobuf buffering (store raw bytes) | Maximum memory savings; lazy deserialization | Massive refactor; changes Event model fundamentally; policy evaluation needs deserialization; complex error handling |

## Decision

**Option A** — Arc all four fields (resource, resource_attrs, scope, scope_attrs).

- Option B was ruled out because Resource/Scope shells are ~32 bytes after attribute extraction — the BTreeMap (OtelAttributes) is where the memory lives. Arc-ing only the shells saves negligible memory.
- Option C was deferred as a non-goal. Arc sharing achieves sufficient improvement (1.62x → 1.2x) without the complexity of raw byte buffering.

## Consequences

- **Easier**: tail sampling memory within 1.2-1.3x of otelcol; logs/metrics throughput now exceeds otelcol; simpler mental model (shared-by-default, copy-on-write).
- **Harder**: every mutation of resource/scope attributes must go through `Arc::make_mut`; forgetting this causes a compile error (Arc is immutable), so correctness is enforced.
- **Validation**: `demo/benchmark` confirmed -28 to -31% memory reduction on tail sampling, zero throughput regressions, and significant throughput improvements on logs/metrics.
