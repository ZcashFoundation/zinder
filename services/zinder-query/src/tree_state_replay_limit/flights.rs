use std::{
    collections::HashMap,
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
    BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest, CanonicalHistoryBounds, ChainEpoch,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
};
use zinder_store::{ServedTreeState, TreeStateLookup, TreeStateReplayOutcome};

use super::{
    ReplayRefusal, TreeStateReplayLimit, WaitingPlace, replay_holding_slot,
    settled_cache::{LookupStep, ReaderGeneration, SettledChainAnchor, SettledStates},
};
use crate::{CanonicalReader, QueryError, StorageReadError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReplayKey {
    height: BlockHeight,
    epoch: ChainEpoch,
    sequence: CanonicalBlockFactsSequenceDigest,
    schedule_revision: Option<u64>,
    activations_fingerprint: Option<NetworkUpgradeActivationsFingerprint>,
    retained: CanonicalHistoryBounds,
}

impl ReplayKey {
    pub(crate) fn new(height: BlockHeight, reader: &dyn CanonicalReader) -> Self {
        let schedule = reader.network_upgrade_schedule();
        Self {
            height,
            epoch: reader.chain_epoch(),
            sequence: reader.event_fence().sequence_digest(),
            schedule_revision: schedule.map(zinder_store::CanonicalActivationSchedule::revision),
            activations_fingerprint: schedule.map(|table| {
                table
                    .activations()
                    .fingerprint(NetworkUpgradeActivationsFingerprintVersion::CURRENT)
            }),
            retained: reader.history_bounds(),
        }
    }

    pub(super) fn settled_anchor(&self) -> SettledChainAnchor {
        SettledChainAnchor {
            network: self.epoch.network,
            artifact_schema: self.epoch.artifact_schema_version,
            retained: self.retained,
            schedule_revision: self.schedule_revision,
            activations_fingerprint: self.activations_fingerprint,
            settled_tip: BlockId::new(self.epoch.settled_tip_height, self.epoch.settled_tip_hash),
        }
    }
}

#[derive(Default, Debug)]
pub(super) struct Flights {
    running: HashMap<(ReplayKey, usize), Weak<Flight>>,
    settled: SettledStates,
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
    async fn acquire_anchor_verify(
        &self,
        deadline: Instant,
    ) -> Result<(tokio::sync::MutexGuard<'_, ()>, bool), QueryError> {
        if let Ok(guard) = self.anchor_verify.try_lock() {
            return Ok((guard, false));
        }
        if Instant::now() >= deadline {
            return Err(self.refused(ReplayRefusal::WaitExpired));
        }
        let _place = WaitingPlace::take(&self.waiters, self.max_waiters)
            .ok_or_else(|| self.refused(ReplayRefusal::WaitersFull))?;
        let guard = tokio::time::timeout_at(deadline, self.anchor_verify.lock())
            .await
            .map_err(|_| self.refused(ReplayRefusal::WaitExpired))?;
        Ok((guard, true))
    }

    async fn lookup_settled(
        &self,
        key: ReplayKey,
        generation: &ReaderGeneration,
        reader: &dyn CanonicalReader,
        deadline: Instant,
    ) -> Result<Option<ServedTreeState>, QueryError> {
        let target = key.settled_anchor();
        let initial_step = self
            .flights
            .lock()
            .settled
            .classify_lookup(target, generation, key.height);
        match initial_step {
            LookupStep::Hit(state) => return Ok(Some(state)),
            LookupStep::Miss => return Ok(None),
            LookupStep::VerifyExtension(_) => {}
        }
        let (_verify_guard, waited) = self.acquire_anchor_verify(deadline).await?;
        let locked_step = self
            .flights
            .lock()
            .settled
            .classify_lookup(target, generation, key.height);
        let prior = match locked_step {
            LookupStep::Hit(state) => return Ok(Some(state)),
            LookupStep::Miss => return Ok(None),
            LookupStep::VerifyExtension(prior) => prior,
        };
        let header = if !self.wait.is_zero() || waited {
            tokio::time::timeout_at(deadline, reader.block_header_at(prior.settled_tip.height))
                .await
                .map_err(|_| self.refused(ReplayRefusal::WaitExpired))?
                .map_err(QueryError::canonical_read)?
        } else {
            reader
                .block_header_at(prior.settled_tip.height)
                .await
                .map_err(QueryError::canonical_read)?
        };
        let extends_prior =
            header.is_some_and(|stored| stored.block_hash == prior.settled_tip.hash);
        Ok(self.flights.lock().settled.complete_extension_lookup(
            (prior, target),
            generation,
            key.height,
            extends_prior,
        ))
    }

    pub(crate) async fn ready_state(
        &self,
        key: ReplayKey,
        generation: &ReaderGeneration,
        reader: &dyn CanonicalReader,
        deadline: Instant,
    ) -> Result<Option<ReadyState>, QueryError> {
        if key.height > key.epoch.visible_tip_height
            || key
                .retained
                .preceding_checkpoint()
                .is_some_and(|block| key.height < block.height)
        {
            return Err(QueryError::BlockNotInBestChain);
        }
        if let Some(state) = self
            .lookup_settled(key, generation, reader, deadline)
            .await?
        {
            return Ok(Some(ReadyState::Cached(state)));
        }
        let stored = reader
            .stored_tree_state_at(key.height)
            .await
            .map_err(QueryError::canonical_read)?;
        Ok(stored.map(ReadyState::Stored))
    }

    pub(crate) fn select(
        &self,
        key: ReplayKey,
        generation: &ReaderGeneration,
        deadline: Instant,
        mut carried: Option<TakeReplayPermit>,
    ) -> Result<Selection, QueryError> {
        let id = (key, generation.addr());
        let mut flights = self.flights.lock();
        if let Some(state) = flights
            .settled
            .cached_at_anchor(key.settled_anchor(), key.height)
        {
            return Ok(Selection::Ready(ReadyState::Cached(state)));
        }
        if let Some(flight) = flights.running.get(&id).and_then(Weak::upgrade)
            && flight.try_join(self, &mut carried)?
        {
            return Ok(Selection::Waiting(Subscription::new(flight, deadline)));
        }
        flights.running.retain(|_, old| old.strong_count() > 0);
        let (admission, place) = Admission::new(self, deadline, carried)?;
        let flight = Arc::new(Flight {
            key,
            generation: generation.clone(),
            registry: self.flights.clone(),
            cancelled: Arc::default(),
            admission,
            state: Mutex::new(FlightState {
                subscribers: SubscriberPlaces::first(place),
                work: Work::Waiting,
            }),
        });
        flights.running.insert(id, Arc::downgrade(&flight));
        drop(flights);
        Ok(Selection::Waiting(Subscription::new(flight, deadline)))
    }

    #[cfg(test)]
    fn subscribe<T: Send + Sync + 'static>(
        &self,
        key: ReplayKey,
        generation: &Weak<T>,
        deadline: Instant,
    ) -> Result<Arc<Subscription>, QueryError> {
        let cache_generation = ReaderGeneration::new(generation, true);
        match self.select(key, &cache_generation, deadline, None)? {
            Selection::Waiting(subscription) => Ok(subscription),
            Selection::Ready(_) => Err(QueryError::BlockNotInBestChain),
        }
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_waiters(
        &self,
        count: u32,
    ) -> Result<(), tokio::time::error::Elapsed> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while self.waiter_count() != count {
                tokio::task::yield_now().await;
            }
        })
        .await
    }

    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> u32 {
        self.waiters.load(Ordering::Acquire)
    }
}

pub(crate) type TakeReplayPermit = Arc<dyn Fn() -> Option<OwnedSemaphorePermit> + Send + Sync>;
type ReplayRead =
    watch::Receiver<Option<Result<Option<(ServedTreeState, bool)>, Arc<StorageReadError>>>>;

pub(crate) enum Selection {
    Ready(ReadyState),
    Waiting(Arc<Subscription>),
}

fn transferable(permit: OwnedSemaphorePermit) -> TakeReplayPermit {
    let slot = Mutex::new(Some(permit));
    Arc::new(move || slot.lock().take())
}

type SlotDecision = Result<TakeReplayPermit, ReplayRefusal>;

struct Admission {
    slot: Shared<BoxFuture<'static, SlotDecision>>,
    decision: Option<watch::Sender<Option<SlotDecision>>>,
}

impl Admission {
    fn new(
        limit: &TreeStateReplayLimit,
        deadline: Instant,
        carried: Option<TakeReplayPermit>,
    ) -> Result<(Self, Option<WaitingPlace>), QueryError> {
        if let Some(slot) = carried {
            return Ok((
                Self {
                    slot: futures_util::future::ready(Ok(slot)).boxed().shared(),
                    decision: None,
                },
                None,
            ));
        }
        let (queued, place) = limit.admit_until(deadline)?;
        let admitted = queued.map(|admission| admission.map(transferable));
        if place.is_none() {
            return Ok((
                Self {
                    slot: admitted.boxed().shared(),
                    decision: None,
                },
                None,
            ));
        }
        Ok((Self::queued(admitted), place))
    }

    fn queued(admitted: impl Future<Output = SlotDecision> + Send + 'static) -> Self {
        let (decision, mut accepted) = watch::channel(None::<SlotDecision>);
        let publish = decision.clone();
        let slot = async move {
            tokio::select! {
                biased;
                Some(slot) = async {
                    accepted.changed().await.ok()?;
                    accepted.borrow_and_update().clone()
                } => slot,
                mut slot = admitted => {
                    // The queued acquisition and a carried permit can both complete in the same poll.
                    publish.send_if_modified(|chosen| {
                        if let Some(first) = chosen {
                            slot.clone_from(first);
                            false
                        } else {
                            *chosen = Some(slot.clone());
                            true
                        }
                    });
                    slot
                }
            }
        };
        Self {
            slot: slot.boxed().shared(),
            decision: Some(decision),
        }
    }

    /// A refusal leaves the incoming permit available for a fresh flight.
    fn accept_carried(&self, carried: &mut Option<TakeReplayPermit>) -> bool {
        let Some(decision) = &self.decision else {
            return true;
        };
        decision.send_if_modified(|chosen| {
            if chosen.is_none()
                && let Some(slot) = carried.take()
            {
                *chosen = Some(Ok(slot));
                true
            } else {
                false
            }
        });
        decision.borrow().as_ref().is_some_and(Result::is_ok)
    }

    fn refused(&self) -> bool {
        self.decision
            .as_ref()
            .is_some_and(|decision| decision.borrow().as_ref().is_some_and(Result::is_err))
    }
}

// A queued flight needs one waiting place per caller. After admission the slot
// covers one live caller, regardless of which subscriber leaves next.
struct SubscriberPlaces {
    count: usize,
    admitted: bool,
    waiting: Vec<WaitingPlace>,
}

impl SubscriberPlaces {
    fn first(place: Option<WaitingPlace>) -> Self {
        Self {
            count: 1,
            admitted: place.is_none(),
            waiting: place.into_iter().collect(),
        }
    }

    fn leave(&mut self, moved: bool) {
        self.count -= 1;
        if moved && self.admitted {
            self.admitted = false;
        } else {
            self.waiting.pop();
        }
    }
}

struct Flight {
    key: ReplayKey,
    // Keep the allocation address and ownership context without pinning its storage reader.
    generation: ReaderGeneration,
    registry: Arc<Mutex<Flights>>,
    cancelled: Arc<AtomicBool>,
    admission: Admission,
    state: Mutex<FlightState>,
}

struct FlightState {
    subscribers: SubscriberPlaces,
    work: Work,
}

enum Work {
    Waiting,
    Running(ReplayRead),
    Moved,
}

impl Flight {
    fn try_join(
        &self,
        limit: &TreeStateReplayLimit,
        carried: &mut Option<TakeReplayPermit>,
    ) -> Result<bool, QueryError> {
        let mut state = self.state.lock();
        match &state.work {
            Work::Moved => return Ok(false),
            Work::Running(rx) if matches!(rx.borrow().as_ref(), Some(Err(_))) => return Ok(false),
            Work::Waiting if self.admission.refused() => return Ok(false),
            Work::Waiting | Work::Running(_) => {}
        }
        let places = &mut state.subscribers;
        if carried.is_some() && !places.admitted {
            if !self.admission.accept_carried(carried) {
                return Ok(false);
            }
            places.admitted = true;
        } else if places.count != 0 || !places.admitted {
            let place = WaitingPlace::take(&limit.waiters, limit.max_waiters)
                .ok_or_else(|| limit.refused(ReplayRefusal::WaitersFull))?;
            places.waiting.push(place);
        }
        places.count += 1;
        drop(state);
        Ok(true)
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

pub(crate) struct Subscription {
    flight: Arc<Flight>,
    deadline: Instant,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut state = self.flight.state.lock();
        let moved = matches!(state.work, Work::Moved);
        state.subscribers.leave(moved);
    }
}

impl Subscription {
    fn new(flight: Arc<Flight>, deadline: Instant) -> Arc<Self> {
        Arc::new(Self { flight, deadline })
    }

    /// Transfers unstarted work's slot. Subscribers that still need the old generation retry there.
    pub(crate) fn carry(&self, slot: &TakeReplayPermit) -> Option<TakeReplayPermit> {
        let mut state = self.flight.state.lock();
        if !matches!(state.work, Work::Waiting) {
            return None;
        }
        let permit = slot()?;
        state.work = Work::Moved;
        drop(state);
        Some(transferable(permit))
    }

    pub(crate) async fn admit(
        &self,
        limit: &TreeStateReplayLimit,
    ) -> Result<TakeReplayPermit, QueryError> {
        let admission = &self.flight.admission;
        let slot = tokio::time::timeout_at(self.deadline, admission.slot.clone())
            .await
            .map_err(|_| limit.refused(ReplayRefusal::WaitExpired))?
            .map_err(|reason| limit.refused(reason))?;
        let mut state = self.flight.state.lock();
        if !state.subscribers.admitted && !matches!(state.work, Work::Moved) {
            state.subscribers.admitted = true;
            state.subscribers.waiting.pop();
        }
        drop(state);
        Ok(slot)
    }

    pub(crate) async fn replay<Read, Reading>(
        &self,
        take_permit: TakeReplayPermit,
        read: Read,
    ) -> Result<Option<ServedTreeState>, QueryError>
    where
        Read: FnOnce(Arc<AtomicBool>) -> Reading,
        Reading: Future<Output = Result<Option<TreeStateReplayOutcome>, StorageReadError>>
            + Send
            + 'static,
    {
        let flight = &self.flight;
        let (mut pending, joined) = {
            let mut state = flight.state.lock();
            match &state.work {
                Work::Moved => return Ok(None),
                Work::Running(pending) => (pending.clone(), true),
                Work::Waiting => {
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
                    state.work = Work::Running(pending.clone());
                    drop(state);
                    (pending, false)
                }
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
        if state.block_id().height == flight.key.height {
            flight.registry.lock().settled.remember(
                flight.key.settled_anchor(),
                &flight.generation,
                &state,
            );
        }
        Ok(Some(state))
    }
}

pub(crate) async fn read_missing_height(
    reader: &dyn CanonicalReader,
    height: BlockHeight,
    cancelled: Arc<AtomicBool>,
) -> Result<Option<TreeStateReplayOutcome>, StorageReadError> {
    if cancelled.load(Ordering::Relaxed) {
        return Ok(None);
    }
    match reader
        .tree_state_lookup_until_cancelled(height, cancelled.clone())
        .await?
    {
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
