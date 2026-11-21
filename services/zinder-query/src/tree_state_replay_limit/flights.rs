use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::{
    FutureExt as _,
    future::{BoxFuture, Shared},
};
use parking_lot::Mutex;
use tokio::{
    sync::{OwnedSemaphorePermit, watch},
    time::Instant,
};
use tracing::Instrument as _;
use zinder_core::{
    BlockHeight, CanonicalBlockFactsSequenceDigest, CanonicalHistoryBounds, ChainEpoch,
};
use zinder_store::{ServedTreeState, TreeStateLookup, TreeStateReplayOutcome};

use super::{ReplayRefusal, TreeStateReplayLimit, WaitingPlace, replay_holding_slot};
use crate::{CanonicalReader, QueryError, StorageReadError};

const SETTLED_ENTRIES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReplayKey {
    height: BlockHeight,
    epoch: ChainEpoch,
    sequence: CanonicalBlockFactsSequenceDigest,
    schedule_revision: Option<u64>,
    retained: CanonicalHistoryBounds,
}

impl ReplayKey {
    pub(crate) fn new(height: BlockHeight, reader: &dyn CanonicalReader) -> Self {
        Self {
            height,
            epoch: reader.chain_epoch(),
            sequence: reader.event_fence().sequence_digest(),
            schedule_revision: reader
                .network_upgrade_schedule()
                .map(zinder_store::CanonicalActivationSchedule::revision),
            retained: reader.history_bounds(),
        }
    }
}

#[derive(Default, Debug)]
pub(super) struct Flights {
    running: HashMap<(ReplayKey, usize), Weak<Flight>>,
    settled: Arc<Mutex<SettledStates>>,
}

#[derive(Debug, Default)]
struct SettledStates {
    settled: VecDeque<(ReplayKey, ServedTreeState)>,
}

impl SettledStates {
    fn cached(&self, key: ReplayKey) -> Option<ServedTreeState> {
        self.settled
            .iter()
            .find(|(entry, _)| *entry == key)
            .map(|(_, state)| state.clone())
    }

    fn remember(&mut self, key: ReplayKey, state: &ServedTreeState) {
        if key.height > key.epoch.settled_tip_height
            || self.settled.iter().any(|(entry, _)| *entry == key)
        {
            return;
        }
        if self.settled.len() == SETTLED_ENTRIES {
            self.settled.pop_front();
        }
        self.settled.push_back((key, state.clone()));
    }
}

pub(crate) enum ReadyState {
    Cached(ServedTreeState),
    Stored(ServedTreeState),
}

impl ReadyState {
    pub(crate) fn serve(self) -> ServedTreeState {
        let (source, state) = match self {
            Self::Cached(state) => ("cached", state),
            Self::Stored(state) => ("stored", state),
        };
        super::record_tree_state_read(state.block_id().height, source);
        state
    }
}

impl TreeStateReplayLimit {
    pub(crate) fn cached(&self, key: ReplayKey) -> Option<ServedTreeState> {
        self.flights.lock().settled.lock().cached(key)
    }

    pub(crate) async fn ready_state(
        &self,
        key: ReplayKey,
        reader: &dyn CanonicalReader,
    ) -> Result<Option<ReadyState>, QueryError> {
        if key.height > key.epoch.visible_tip_height
            || key
                .retained
                .preceding_checkpoint()
                .is_some_and(|block| key.height < block.height)
        {
            return Err(QueryError::BlockNotInBestChain);
        }
        if let Some(state) = self.cached(key) {
            return Ok(Some(ReadyState::Cached(state)));
        }
        let stored = reader
            .stored_tree_state_at(key.height)
            .await
            .map_err(QueryError::canonical_read)?;
        Ok(stored.map(ReadyState::Stored))
    }

    pub(crate) fn subscribe<T: Send + Sync + 'static>(
        &self,
        key: ReplayKey,
        generation: &Weak<T>,
        deadline: Instant,
    ) -> Result<Arc<Subscription>, QueryError> {
        let id = (key, generation.as_ptr().addr());
        let mut flights = self.flights.lock();
        if let Some(flight) = flights.running.get(&id).and_then(Weak::upgrade)
            && flight.joinable()
        {
            let place = WaitingPlace::take(&self.waiters, self.max_waiters)
                .ok_or_else(|| self.refused(ReplayRefusal::WaitersFull))?;
            return Ok(Arc::new(Subscription {
                flight,
                _place: Some(place),
            }));
        }
        flights.running.retain(|_, old| old.strong_count() > 0);
        let admission = self
            .admit_until(deadline)?
            .map(|admission| {
                admission.map(|permit| {
                    let slot = Mutex::new(Some(permit));
                    Arc::new(move || slot.lock().take()) as TakeReplayPermit
                })
            })
            .boxed()
            .shared();
        let flight = Arc::new(Flight {
            key,
            _generation: generation.clone(),
            cache: flights.settled.clone(),
            cancelled: Arc::default(),
            admission,
            read: Mutex::default(),
        });
        flights.running.insert(id, Arc::downgrade(&flight));
        drop(flights);
        Ok(Arc::new(Subscription {
            flight,
            _place: None,
        }))
    }

    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> u32 {
        self.waiters.load(Ordering::Acquire)
    }
}

type TakeReplayPermit = Arc<dyn Fn() -> Option<OwnedSemaphorePermit> + Send + Sync>;
type ReplayRead =
    watch::Receiver<Option<Result<Option<(ServedTreeState, bool)>, Arc<StorageReadError>>>>;

struct Flight {
    key: ReplayKey,
    // Keep the allocation address unique without pinning its storage reader.
    _generation: Weak<dyn Send + Sync>,
    cache: Arc<Mutex<SettledStates>>,
    cancelled: Arc<AtomicBool>,
    admission: Shared<BoxFuture<'static, Result<TakeReplayPermit, ReplayRefusal>>>,
    read: Mutex<Option<ReplayRead>>,
}

impl Flight {
    fn joinable(&self) -> bool {
        !matches!(self.admission.peek(), Some(Err(_)))
            && !self
                .read
                .lock()
                .as_ref()
                .is_some_and(|rx| matches!(rx.borrow().as_ref(), Some(Err(_))))
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

pub(crate) struct Subscription {
    flight: Arc<Flight>,
    _place: Option<WaitingPlace>,
}

impl Subscription {
    pub(crate) async fn admit(
        &self,
        limit: &TreeStateReplayLimit,
        deadline: Instant,
    ) -> Result<TakeReplayPermit, QueryError> {
        tokio::time::timeout_at(deadline, self.flight.admission.clone())
            .await
            .map_err(|_| limit.refused(ReplayRefusal::WaitExpired))?
            .map_err(|reason| limit.refused(reason))
    }

    pub(crate) async fn replay<Read, Reading>(
        &self,
        take_permit: TakeReplayPermit,
        read: Read,
    ) -> Result<ServedTreeState, QueryError>
    where
        Read: FnOnce(Arc<AtomicBool>) -> Reading,
        Reading: Future<Output = Result<Option<TreeStateReplayOutcome>, StorageReadError>>
            + Send
            + 'static,
    {
        let flight = &self.flight;
        let (mut pending, joined) = {
            let mut current = flight.read.lock();
            if let Some(pending) = current.as_ref() {
                (pending.clone(), true)
            } else {
                let (send, pending) = watch::channel(None);
                let work = replay_holding_slot(
                    flight.key.height,
                    take_permit().ok_or_else(|| QueryError::BlockingTaskFailed {
                        reason: "tree-state replay lost its slot".to_owned(),
                    })?,
                    read(flight.cancelled.clone()),
                );
                tokio::spawn(
                    async move {
                        send.send_replace(Some(work.await.map_err(Arc::new)));
                    }
                    .in_current_span(),
                );
                *current = Some(pending.clone());
                drop(current);
                (pending, false)
            }
        };
        let (state, reconstructed) = pending
            .wait_for(Option::is_some)
            .await
            .map_err(|failure| QueryError::BlockingTaskFailed {
                reason: failure.to_string(),
            })?
            .clone()
            .ok_or(QueryError::BlockNotInBestChain)?
            .map_err(shared_read_error)?
            .ok_or(QueryError::BlockNotInBestChain)?;
        let source = if joined {
            "coalesced"
        } else if reconstructed {
            "replayed"
        } else {
            "stored"
        };
        super::record_tree_state_read(flight.key.height, source);
        flight.cache.lock().remember(flight.key, &state);
        Ok(state)
    }
}

pub(crate) async fn read_missing_height(
    reader: &dyn CanonicalReader,
    height: BlockHeight,
    cancelled: Arc<AtomicBool>,
) -> Result<Option<TreeStateReplayOutcome>, StorageReadError> {
    match reader.tree_state_lookup(height).await? {
        Some(TreeStateLookup::Replay(replay)) => {
            reader
                .replay_tree_state_until_cancelled(replay, cancelled)
                .await
        }
        Some(TreeStateLookup::Stored(state)) => Ok(Some(TreeStateReplayOutcome::Stored(state))),
        None => Ok(None),
    }
}

fn shared_read_error(error: Arc<StorageReadError>) -> QueryError {
    use StorageReadError as E;
    let copy = match *error {
        E::ReaderInvalidated { .. } => E::ReaderInvalidated {
            source: Box::new(error),
        },
        E::Unavailable { .. } => E::Unavailable {
            source: Box::new(error),
        },
        E::ReadTaskFailed { ref reason } => E::ReadTaskFailed {
            reason: reason.clone(),
        },
        E::ArtifactUnavailable { family, key } => E::ArtifactUnavailable { family, key },
        E::CanonicalHistoryUnavailable {
            first_available_height,
        } => E::CanonicalHistoryUnavailable {
            first_available_height,
        },
        E::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        } => E::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        },
        E::ChainEventCursorInvalid { reason } => E::ChainEventCursorInvalid { reason },
    };
    QueryError::canonical_read(copy)
}

#[cfg(test)]
mod tests;
