---
status: accepted
---
# Store implementation for servicegraph edge buffering

Addresses: [FR1](../designs/20260505_servicegraph.md#fr1), [FR5](../designs/20260505_servicegraph.md#fr5), [NFR1](../designs/20260505_servicegraph.md#nfr1), [NFR2](../designs/20260505_servicegraph.md#nfr2)

## Problem

The servicegraph transform needs a store to hold pending edges (spans waiting for their pair) with TTL-based expiration and a capacity limit. Sol provides two relevant primitives:

1. `ExpiringHashMap<K, V>` — wraps `HashMap` + tokio `DelayQueue`, provides `insert(key, value, ttl)` and `next_expired()` async.
2. Manual `HashMap` + `BTreeMap<Instant, Vec<K>>` + `VecDeque<K>` — the pattern used by `tail_sampling` for buffered traces.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. ExpiringHashMap + VecDeque for eviction | Reuses existing Sol primitive for TTL. Cleaner async expiration via `next_expired()`. Less manual bookkeeping for deadlines. | No built-in capacity limit — need a VecDeque wrapper for LRU eviction. Two data structures to coordinate. |
| B. HashMap + BTreeMap deadlines + VecDeque (tail_sampling pattern) | Proven pattern in the codebase. Full control over eviction and expiration. Single owner of all state. | More manual code for deadline management. Reimplements what `ExpiringHashMap` already provides. |

## Decision

Option A — `ExpiringHashMap` with a `VecDeque` for capacity-based eviction. The `ExpiringHashMap` handles TTL expiration cleanly via its `next_expired()` async method, which integrates naturally into the `tokio::select!` loop. The `VecDeque` tracks insertion order for LRU eviction when `max_items` is exceeded — same approach as `tail_sampling`'s `insertion_order` but simpler because we only need to call `ExpiringHashMap::remove()` on eviction (no BTreeMap cleanup needed).

The `tokio::select!` loop becomes:
```
select! {
    event = input.next() => { on_span(event) }
    expired = store.next_expired(), if !store.is_empty() => { on_expired(expired) }
    _ = flush_tick.tick() => { flush_metrics() }
}
```

## Consequences

- Simpler TTL management than the tail_sampling pattern (no manual BTreeMap)
- `ExpiringHashMap::remove()` correctly cleans up both the HashMap and the DelayQueue
- Capacity eviction is explicit: when `store.len() >= max_items`, pop from VecDeque and remove from store
- The `if !store.is_empty()` guard on `next_expired()` prevents spinlock (documented in ExpiringHashMap)
- Known limitation: VecDeque retains stale keys for completed edges — negligible at demo scale, worth addressing for production use
