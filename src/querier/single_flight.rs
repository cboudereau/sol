// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Single-flight query execution ([FR3](../../docs/workspace/backend-metrics-perf/DESIGN.md#fr3),
//! [cache-invalidation-scope ADR](../../docs/workspace/backend-metrics-perf/adrs/cache-invalidation-scope.md)
//! option F).
//!
//! N concurrent callers with the same [`CacheKey`] execute the underlying
//! plan **once**: the first caller (the leader) runs the query — including
//! the cache insert its closure performs — while the others (followers)
//! await the leader's shared result over a `tokio::sync::watch` channel.
//!
//! Hand-rolled rather than `moka` `get_with` because the cache is compiled
//! `sync`-only: its blocking coalescing would stall the async executor (ADR).
//!
//! Invariants (ADR):
//! - the in-flight map's mutex is never held across an `await`, so distinct
//!   keys never serialise;
//! - errors propagate to every waiter and are **never** cached (the leader's
//!   closure only inserts on success);
//! - the in-flight entry is always removed when the leader finishes —
//!   including on panic or cancellation, via an RAII guard — so a later call
//!   re-executes instead of waiting forever.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::watch;

use super::cache::{CacheKey, CachedResult};

/// The outcome a leader shares with its followers. `crate::Error` is a
/// non-`Clone` boxed error, so followers receive its rendered message; the
/// leader itself keeps (and returns) the original typed error.
type SharedOutcome = std::result::Result<CachedResult, String>;

/// One in-flight execution: followers subscribe to `rx` and wait for the
/// leader to publish `Some(outcome)`. The channel retains its last value, so
/// a follower that subscribes before the entry is removed can never miss the
/// result, even if it only polls after the leader completed.
struct InFlight {
    rx: watch::Receiver<Option<SharedOutcome>>,
    /// Followers coalesced onto this flight — observed by tests to sequence
    /// deterministically (incremented under the map lock).
    followers: Arc<std::sync::atomic::AtomicUsize>,
}

/// Request coalescing keyed by [`CacheKey`] (FR3): sits in front of the
/// cache-backed execution in `QueryEngine::{sql, collect_scoped, sql_user}`.
pub struct SingleFlight {
    inflight: Mutex<HashMap<CacheKey, InFlight>>,
}

/// RAII removal of the leader's in-flight entry: runs however the leader
/// finishes — return, error, panic, or future cancellation — so the map
/// cannot leak an entry that would make later same-key calls wait forever.
struct RemoveOnDrop<'a> {
    flights: &'a SingleFlight,
    key: CacheKey,
}

impl Drop for RemoveOnDrop<'_> {
    fn drop(&mut self) {
        self.flights.lock().remove(&self.key);
    }
}

impl SingleFlight {
    /// An empty single-flight table.
    pub fn new() -> Self {
        Self {
            inflight: Mutex::new(HashMap::new()),
        }
    }

    /// Lock the in-flight map (poison-tolerant, matching the engine's mutex
    /// convention). Held only for map operations — never across an `await`.
    fn lock(&self) -> MutexGuard<'_, HashMap<CacheKey, InFlight>> {
        self.inflight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Execute `execute` under single-flight semantics for `key`: if no
    /// identical query is in flight, the caller leads and runs it; otherwise
    /// the caller awaits the leader's shared result (recording a coalesced
    /// hit). If the leader vanishes without publishing (panic/cancellation),
    /// its RAII guard has removed the entry and the follower falls back to
    /// executing itself — no deadlock, and a failure is never cached (the
    /// closure inserts into the cache only on success).
    pub async fn run<F, Fut>(&self, key: CacheKey, execute: F) -> crate::Result<CachedResult>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::Result<CachedResult>>,
    {
        enum Role {
            Leader(watch::Sender<Option<SharedOutcome>>),
            Follower(watch::Receiver<Option<SharedOutcome>>),
        }
        // Claim leadership or subscribe — the lock is released before any
        // await, so distinct keys never serialise.
        let role = {
            let mut map = self.lock();
            match map.get(&key) {
                Some(flight) => {
                    flight
                        .followers
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Role::Follower(flight.rx.clone())
                }
                None => {
                    let (tx, rx) = watch::channel(None);
                    map.insert(
                        key.clone(),
                        InFlight {
                            rx,
                            followers: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                        },
                    );
                    Role::Leader(tx)
                }
            }
        };
        match role {
            Role::Leader(tx) => {
                // Removed however this future finishes (see `RemoveOnDrop`).
                let _guard = RemoveOnDrop { flights: self, key };
                let result = execute().await;
                let shared = match &result {
                    Ok(v) => Ok(Arc::clone(v)),
                    Err(e) => Err(e.to_string()),
                };
                // Publish before the guard removes the entry: a caller that
                // subscribed while we ran always finds the retained value.
                let _ = tx.send(Some(shared));
                result
            }
            Role::Follower(mut rx) => {
                super::telemetry::record_coalesced();
                let received = rx
                    .wait_for(|outcome| outcome.is_some())
                    .await
                    .ok()
                    .and_then(|guard| guard.clone());
                match received {
                    Some(Ok(result)) => Ok(result),
                    Some(Err(message)) => Err(message.into()),
                    // The leader vanished without publishing (panic or
                    // cancellation): its guard removed the entry, so execute
                    // ourselves — never a deadlock, and nothing was cached.
                    None => execute().await,
                }
            }
        }
    }

    /// Followers currently coalesced onto `key`'s in-flight execution
    /// (0 when none) — the deterministic sequencing seam for tests.
    #[cfg(test)]
    fn waiters(&self, key: &CacheKey) -> usize {
        self.lock()
            .get(key)
            .map_or(0, |f| f.followers.load(std::sync::atomic::Ordering::SeqCst))
    }
}

impl Default for SingleFlight {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn empty_result() -> CachedResult {
        Arc::new(Vec::new())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_single_flight_coalesces_concurrent_identical() {
        let sf = Arc::new(SingleFlight::new());
        let key = CacheKey::for_sql("SELECT 1");
        let execs = Arc::new(AtomicUsize::new(0));

        // Leader: signals once executing, then blocks on the gate so the
        // followers can pile up deterministically.
        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let leader = {
            let sf = Arc::clone(&sf);
            let key = key.clone();
            let execs = Arc::clone(&execs);
            tokio::spawn(async move {
                sf.run(key, move || async move {
                    execs.fetch_add(1, Ordering::SeqCst);
                    let _ = started_tx.send(());
                    let _ = gate_rx.await;
                    Ok(empty_result())
                })
                .await
            })
        };
        started_rx.await.expect("leader executing");

        // Four followers on the same key while the leader is in flight.
        let followers: Vec<_> = (0..4)
            .map(|_| {
                let sf = Arc::clone(&sf);
                let key = key.clone();
                let execs = Arc::clone(&execs);
                tokio::spawn(async move {
                    sf.run(key, move || async move {
                        execs.fetch_add(1, Ordering::SeqCst);
                        Ok(empty_result())
                    })
                    .await
                })
            })
            .collect();
        // Sequence: all four registered as followers before the gate opens.
        while sf.waiters(&key) < 4 {
            tokio::task::yield_now().await;
        }
        gate_tx.send(()).expect("leader awaits the gate");

        let lead = leader.await.expect("join").expect("leader result");
        for f in followers {
            let got = f.await.expect("join").expect("follower result");
            assert!(
                Arc::ptr_eq(&lead, &got),
                "followers must share the leader's Arc'd result"
            );
        }
        assert_eq!(
            execs.load(Ordering::SeqCst),
            1,
            "5 concurrent identical calls must execute exactly once"
        );
        assert_eq!(sf.waiters(&key), 0, "in-flight entry removed on completion");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_single_flight_error_propagates_and_not_cached() {
        let sf = Arc::new(SingleFlight::new());
        let key = CacheKey::for_sql("SELECT boom");
        let follower_execs = Arc::new(AtomicUsize::new(0));

        let (started_tx, started_rx) = oneshot::channel::<()>();
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let leader = {
            let sf = Arc::clone(&sf);
            let key = key.clone();
            tokio::spawn(async move {
                sf.run(key, move || async move {
                    let _ = started_tx.send(());
                    let _ = gate_rx.await;
                    Err(crate::Error::from("boom"))
                })
                .await
            })
        };
        started_rx.await.expect("leader executing");

        let followers: Vec<_> = (0..2)
            .map(|_| {
                let sf = Arc::clone(&sf);
                let key = key.clone();
                let execs = Arc::clone(&follower_execs);
                tokio::spawn(async move {
                    sf.run(key, move || async move {
                        execs.fetch_add(1, Ordering::SeqCst);
                        Ok(empty_result())
                    })
                    .await
                })
            })
            .collect();
        while sf.waiters(&key) < 2 {
            tokio::task::yield_now().await;
        }
        gate_tx.send(()).expect("leader awaits the gate");

        let lead_err = leader.await.expect("join").expect_err("leader fails");
        assert!(lead_err.to_string().contains("boom"), "err: {lead_err}");
        for f in followers {
            let err = f.await.expect("join").expect_err("followers share the failure");
            assert!(err.to_string().contains("boom"), "err: {err}");
        }
        assert_eq!(
            follower_execs.load(Ordering::SeqCst),
            0,
            "followers must not have executed during the failed flight"
        );

        // Nothing cached, entry removed: the next same-key call re-executes.
        let reruns = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&reruns);
        let ok = sf
            .run(key.clone(), move || async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(empty_result())
            })
            .await;
        assert!(ok.is_ok(), "call after a failure succeeds");
        assert_eq!(
            reruns.load(Ordering::SeqCst),
            1,
            "a failure is never cached: the next call re-executes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_single_flight_distinct_keys_parallel() {
        // Key A's leader can only finish after key B's leader has executed:
        // if distinct keys serialised (a lock held across an await), this
        // would deadlock — the timeout turns that into a failure.
        let sf = Arc::new(SingleFlight::new());
        let key_a = CacheKey::for_sql("SELECT a");
        let key_b = CacheKey::for_sql("SELECT b");

        let (a_started_tx, a_started_rx) = oneshot::channel::<()>();
        let (b_done_tx, b_done_rx) = oneshot::channel::<()>();
        let a = {
            let sf = Arc::clone(&sf);
            tokio::spawn(async move {
                sf.run(key_a, move || async move {
                    let _ = a_started_tx.send(());
                    b_done_rx.await.map_err(|_| crate::Error::from("b never ran"))?;
                    Ok(empty_result())
                })
                .await
            })
        };
        a_started_rx.await.expect("A executing");
        // B starts strictly inside A's execution window.
        let b = {
            let sf = Arc::clone(&sf);
            tokio::spawn(async move {
                sf.run(key_b, move || async move {
                    let _ = b_done_tx.send(());
                    Ok(empty_result())
                })
                .await
            })
        };

        let both = async {
            b.await.expect("join").expect("B result");
            a.await.expect("join").expect("A result");
        };
        tokio::time::timeout(Duration::from_secs(5), both)
            .await
            .expect("distinct keys must proceed concurrently (no serialisation)");
    }
}
