---
status: accepted
---
# Shared EventMetadata across batch

Addresses: [FR1](../designs/20260516_tail-sampling-slim-buffer.md#fr1), [FR2](../designs/20260516_tail-sampling-slim-buffer.md#fr2), [NFR1](../designs/20260516_tail-sampling-slim-buffer.md#nfr1)

## Problem

Each span in a batch gets `EventMetadata::default()` — a fresh `Arc::new(Inner::default())` heap allocation (~216 bytes) containing empty collections and a random UUID. At 300k buffered spans, this wastes ~65 MiB.

## Options

| Option | Pros | Cons |
|---|---|---|
| **Share one EventMetadata per batch** | Eliminates ~216 bytes/span heap. Same Arc pattern as resource/scope. One-line change at ingestion boundary. | Per-span UUID lost (all spans in batch share same UUID). |
| **Lazy Inner allocation** | Only allocate Inner when first mutated. | Requires Option\<Arc\<Inner\>\> or sentinel value. Complicates every metadata access. |
| **Pool/intern EventMetadata** | Reuse across batches. | Complex lifecycle management. Over-engineering. |

## Decision

**Share one EventMetadata per batch.** The Arc + `make_mut` copy-on-write pattern is already proven for resource and scope. EventMetadata.clone() is already just an Arc refcount bump.

The per-span UUID (`source_event_id`) becomes per-batch. This is acceptable because:
1. trace_id + span_id already uniquely identify a span
2. `source_event_id` is not used for deduplication in any sink (grep confirms no reads outside metadata accessors and test assertions)

## Consequences

- **Easier**: no code changes needed in transforms or sinks — they already handle shared Arc metadata via `make_mut`.
- **Harder**: if a future feature requires per-span unique IDs from metadata, it would need to use trace_id+span_id instead of source_event_id.
