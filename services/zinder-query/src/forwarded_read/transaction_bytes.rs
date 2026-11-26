//! Verified transaction bytes forwarded reads share: a byte-bounded LRU
//! cache and one node fetch per transaction, digest, and admission kind at a
//! time.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use futures_util::{
    FutureExt as _,
    future::{BoxFuture, Shared, WeakShared},
};
use parking_lot::Mutex;
use tokio::sync::Notify;
use zinder_core::{SerializedBytesDigest, TransactionId};

use super::{CACHE_LOOKUPS_COUNTER, ForwardedReadKind, ForwardedTransactionError};
use crate::MinedTransactionDigest;

type CacheKey = (TransactionId, SerializedBytesDigest);
type FlightKey = (TransactionId, SerializedBytesDigest, ForwardedReadKind);
type FetchOutcome = Result<Arc<[u8]>, ForwardedTransactionError>;
pub(super) type Fetch = Shared<BoxFuture<'static, FetchOutcome>>;

/// The outcome of looking a transaction up before contacting the node.
pub(super) enum Lookup {
    Cached(Arc<[u8]>),
    /// A node fetch to await, either just started or already running for
    /// another caller of the same admission kind.
    Fetch(Fetch),
}

pub(super) struct TransactionBytes {
    state: Arc<Mutex<State>>,
    cache_populated: Arc<Notify>,
    capacity: usize,
}

#[derive(Default)]
struct State {
    cache: TransactionCache,
    /// Fetches in progress, keyed by admission kind so a queued `History`
    /// lookup never blocks a `Transaction` point lookup with reserved slots.
    flights: HashMap<FlightKey, (u64, WeakShared<BoxFuture<'static, FetchOutcome>>)>,
    next_flight: u64,
}

impl TransactionBytes {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            state: Arc::default(),
            cache_populated: Arc::default(),
            capacity,
        }
    }

    pub(super) fn cached(&self, mined: &MinedTransactionDigest) -> Option<Arc<[u8]>> {
        let bytes = self.cached_without_metric(mined)?;
        record_lookup("hit");
        Some(bytes)
    }

    pub(super) fn cached_without_metric(
        &self,
        mined: &MinedTransactionDigest,
    ) -> Option<Arc<[u8]>> {
        self.state.lock().cache.get(key_of(mined))
    }

    pub(super) fn cache_populated(&self) -> &Notify {
        &self.cache_populated
    }

    /// Returns the cached bytes of `mined`, joins a fetch of `kind` already
    /// running for them, or starts `fetch`.
    ///
    /// The started fetch caches verified bytes across all kinds and leaves the
    /// running set before it reports its outcome, so a later lookup either hits
    /// the cache or starts a new fetch; a failure is never reused.
    pub(super) fn lookup<Run, Started>(
        &self,
        mined: MinedTransactionDigest,
        kind: ForwardedReadKind,
        fetch: Run,
    ) -> Lookup
    where
        Run: FnOnce() -> Started,
        Started: Future<Output = FetchOutcome> + Send + 'static,
    {
        let cache_key = key_of(&mined);
        let flight_key = (cache_key.0, cache_key.1, kind);
        let mut state = self.state.lock();
        if let Some(bytes) = state.cache.get(cache_key) {
            drop(state);
            record_lookup("hit");
            return Lookup::Cached(bytes);
        }
        if let Some(running) = state
            .flights
            .get(&flight_key)
            .and_then(|(_, weak)| weak.upgrade())
        {
            drop(state);
            record_lookup("coalesced");
            return Lookup::Fetch(running);
        }
        let flight = state.next_flight;
        state.next_flight += 1;
        let registry = Arc::clone(&self.state);
        let cache_notify = Arc::clone(&self.cache_populated);
        let capacity = self.capacity;
        let started = fetch();
        let shared = async move {
            let outcome = started.await;
            let inserted = {
                let mut state = registry.lock();
                let cached_now = outcome.as_ref().is_ok_and(|bytes| {
                    state.cache.insert(cache_key, Arc::clone(bytes), capacity);
                    capacity > 0 && bytes.len() <= capacity
                });
                if state
                    .flights
                    .get(&flight_key)
                    .is_some_and(|(running, _)| *running == flight)
                {
                    state.flights.remove(&flight_key);
                }
                cached_now
            };
            if inserted {
                cache_notify.notify_waiters();
            }
            outcome
        }
        .boxed()
        .shared();
        state
            .flights
            .retain(|_, (_, weak)| weak.upgrade().is_some());
        if let Some(weak) = shared.downgrade() {
            state.flights.insert(flight_key, (flight, weak));
        }
        drop(state);
        record_lookup("miss");
        Lookup::Fetch(shared)
    }

    #[cfg(test)]
    pub(super) fn running(&self) -> usize {
        self.state
            .lock()
            .flights
            .values()
            .filter(|(_, weak)| weak.upgrade().is_some())
            .count()
    }
}

fn key_of(mined: &MinedTransactionDigest) -> CacheKey {
    (mined.location.transaction_id, mined.serialized_bytes_digest)
}

fn record_lookup(outcome: &'static str) {
    metrics::counter!(CACHE_LOOKUPS_COUNTER, "outcome" => outcome).increment(1);
}

/// Verified bytes by transaction and digest, evicted least recently used
/// first once their total exceeds the capacity.
#[derive(Default)]
pub(super) struct TransactionCache {
    entries: HashMap<CacheKey, (u64, Arc<[u8]>)>,
    lru: BTreeMap<u64, CacheKey>,
    clock: u64,
    bytes: usize,
}

impl TransactionCache {
    pub(super) fn get(&mut self, key: CacheKey) -> Option<Arc<[u8]>> {
        let (tick, bytes) = self.entries.get_mut(&key)?;
        self.lru.remove(tick);
        self.clock += 1;
        *tick = self.clock;
        self.lru.insert(*tick, key);
        Some(Arc::clone(bytes))
    }

    pub(super) fn insert(&mut self, key: CacheKey, bytes: Arc<[u8]>, capacity: usize) {
        if bytes.len() > capacity || self.get(key).is_some() {
            return;
        }
        while self.bytes + bytes.len() > capacity {
            let Some((_, oldest)) = self.lru.pop_first() else {
                break;
            };
            if let Some((_, removed)) = self.entries.remove(&oldest) {
                self.bytes -= removed.len();
            }
        }
        self.clock += 1;
        self.bytes += bytes.len();
        self.lru.insert(self.clock, key);
        self.entries.insert(key, (self.clock, bytes));
    }

    #[cfg(test)]
    pub(super) const fn bytes(&self) -> usize {
        self.bytes
    }
}
