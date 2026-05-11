---
status: draft
---
# SourceSender mutability in gRPC handlers

Addresses: [FR2](../DESIGN.md#fr2), [NFR1](../DESIGN.md#nfr1)

## Problem

Tonic service traits require `&self` on handler methods. `SourceSender::send_batch_named` requires `&mut self`. The current solution clones the entire `SourceSender` (including `HashMap<String, Output>`) per request.

`Output` contains: `LimitedSender` (channel sender clone), `Histogram` (metrics handle), `Registered<EventsSent>`, `Arc<Definition>`, `Arc<OutputId>`, `Option<Duration>`.

How should the gRPC handler get mutable access to `SourceSender` without cloning per request?

## Options

| Option | Pros | Cons |
|---|---|---|
| A: `Arc<Mutex<SourceSender>>` | Simple. One lock per request. | Lock contention under high concurrency. All signals (logs, traces, metrics) contend on one mutex. |
| B: Per-output `Arc<Mutex<Output>>` | No cross-signal contention. Logs, traces, metrics lock independently. | Requires changing `SourceSender` internals or building a wrapper. `send_batch_named` API would need to change. |
| C: `Arc<Mutex<SourceSender>>` with `try_lock` + clone fallback | Low contention: fast path uses lock, slow path clones. | Complex. Two code paths. |

## Decision

**Option B**: Per-output `Arc<Mutex<Output>>` — each named output (logs, traces, metrics) gets its own lock.

Rationale:
- In production, collectors receive all three signals simultaneously on the same gRPC endpoint. Cross-signal contention on a single mutex is a real concern, not a theoretical one.
- `Output::send_batch` is async (channel sends are await points), so the lock is held for the entire batch send duration. With a single `Arc<Mutex<SourceSender>>`, a log request blocks trace and metric requests during its batch send — unnecessary since they target different channels.
- Per-output locking gives full parallelism across signal types while still serializing same-signal requests (which is correct — they write to the same channel).
- `Output` is `pub(super)` in `source_sender` — add a `SharedSourceSender` wrapper type in that module that stores `HashMap<String, Arc<tokio::sync::Mutex<Output>>>` and exposes `send_batch_named(&self, ...)` (note: `&self`, not `&mut self`).
- Uses `tokio::sync::Mutex` (not `std::sync::Mutex`) because `send_batch` is async and the lock is held across await points.

### Implementation sketch

```rust
// In lib/sol-core/src/source_sender/sender.rs
pub struct SharedSourceSender {
    named_outputs: HashMap<String, Arc<tokio::sync::Mutex<Output>>>,
}

impl SharedSourceSender {
    pub fn from_sender(sender: SourceSender) -> Self {
        let named_outputs = sender.named_outputs
            .into_iter()
            .map(|(k, v)| (k, Arc::new(tokio::sync::Mutex::new(v))))
            .collect();
        Self { named_outputs }
    }

    pub async fn send_batch_named<I, E>(&self, name: &str, events: I) -> Result<(), SendError>
    where
        E: Into<Event> + ByteSizeOf,
        I: IntoIterator<Item = E>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        let output = self.named_outputs.get(name).expect("unknown output");
        output.lock().await.send_batch(events).await
    }
}
```

```rust
// In src/sources/opentelemetry/grpc.rs
pub(crate) struct Service {
    pub pipeline: SharedSourceSender,  // was: SourceSender
    // ...
}
```

## Consequences

- New `SharedSourceSender` type in `source_sender` module — thin wrapper over per-output locks.
- `SourceSender::into_shared(self) -> SharedSourceSender` conversion (consumes the sender, no clone).
- `Service` struct changes from `pipeline: SourceSender` to `pipeline: SharedSourceSender`.
- `handle_events` calls `self.pipeline.send_batch_named(...)` directly — no clone, no explicit lock acquisition in caller code.
- Logs, traces, and metrics are fully parallel — only same-signal requests serialize (correct behavior).
- `tokio::sync::Mutex` is used because `Output::send_batch` is async. This is the same mutex tonic already uses internally — no new dependency.
- `Output` visibility stays `pub(super)` — `SharedSourceSender` encapsulates it.
