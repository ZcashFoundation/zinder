use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::sync::{OnceCell, watch};
use tracing::Instrument as _;
use zinder_core::{
    BlockHeight, CanonicalBlockFactsSequenceDigest, CanonicalHistoryBounds, ChainEpoch,
};
use zinder_store::{ServedTreeState, TreeStateReplayOutcome};

use super::{TreeStateReplayLimit, replay_holding_slot};
use crate::{CanonicalReader, QueryError, StorageReadError};

const SETTLED_ENTRIES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReplayKey {
    height: BlockHeight,
    epoch: ChainEpoch,
    sequence: CanonicalBlockFactsSequenceDigest,
    retained: CanonicalHistoryBounds,
}

impl ReplayKey {
    pub(crate) fn new(height: BlockHeight, reader: &dyn CanonicalReader) -> Self {
        Self {
            height,
            epoch: reader.chain_epoch(),
            sequence: reader.event_fence().sequence_digest(),
            retained: reader.history_bounds(),
        }
    }
}

#[derive(Default, Debug)]
pub(super) struct Flights {
    running: HashMap<ReplayKey, Weak<Flight>>,
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

impl TreeStateReplayLimit {
    pub(crate) fn flight(&self, key: ReplayKey) -> Arc<Flight> {
        let mut flights = self.flights.lock();
        if let Some(flight) = flights.running.get(&key).and_then(Weak::upgrade) {
            return flight;
        }
        flights
            .running
            .retain(|_, flight| flight.strong_count() > 0);
        let flight = Arc::new(Flight {
            key,
            cache: flights.settled.clone(),
            cancelled: Arc::default(),
            permit: OnceCell::new(),
            read: Mutex::default(),
        });
        flights.running.insert(key, Arc::downgrade(&flight));
        flight
    }
}

type ReplayRead = watch::Receiver<Option<Result<Option<ServedTreeState>, Arc<StorageReadError>>>>;

pub(crate) struct Flight {
    key: ReplayKey,
    cache: Arc<Mutex<SettledStates>>,
    cancelled: Arc<AtomicBool>,
    permit: OnceCell<Option<Arc<dyn Send + Sync>>>,
    read: Mutex<Option<ReplayRead>>,
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

impl Flight {
    pub(crate) fn cached(&self) -> Option<ServedTreeState> {
        self.cache.lock().cached(self.key)
    }

    pub(crate) async fn admit(
        &self,
        limit: &TreeStateReplayLimit,
    ) -> Result<Arc<dyn Send + Sync>, QueryError> {
        self.permit
            .get_or_init(|| async {
                limit
                    .admit()
                    .await
                    .ok()
                    .map(|permit| Arc::new(permit) as Arc<dyn Send + Sync>)
            })
            .await
            .clone()
            .ok_or(QueryError::TreeStateReplayLimitReached {
                max_concurrent_replays: limit.max_replays,
            })
    }

    pub(crate) async fn replay<Read, Reading>(
        &self,
        permit: Arc<dyn Send + Sync>,
        read: Read,
    ) -> Result<ServedTreeState, QueryError>
    where
        Read: FnOnce(Arc<AtomicBool>) -> Reading,
        Reading: Future<Output = Result<Option<TreeStateReplayOutcome>, StorageReadError>>
            + Send
            + 'static,
    {
        let mut joined = true;
        let mut pending = self
            .read
            .lock()
            .get_or_insert_with(|| {
                joined = false;
                let (send, receive) = watch::channel(None);
                let work =
                    replay_holding_slot(self.key.height, permit, read(self.cancelled.clone()));
                tokio::spawn(
                    async move {
                        send.send_replace(Some(work.await.map_err(Arc::new)));
                    }
                    .in_current_span(),
                );
                receive
            })
            .clone();
        let answer = pending
            .wait_for(Option::is_some)
            .await
            .map_err(|failure| QueryError::BlockingTaskFailed {
                reason: failure.to_string(),
            })?
            .clone()
            .ok_or(QueryError::BlockNotInBestChain)?;
        if let Ok(Some(state)) = &answer {
            if joined {
                let trees = state.tip_metadata();
                super::record_tree_state_read(self.key.height, "coalesced", 0, trees, trees);
            }
            self.cache.lock().remember(self.key, state);
        }
        if answer.is_err() {
            let mut current = self.read.lock();
            if current
                .as_ref()
                .is_some_and(|read| read.same_channel(&pending))
            {
                *current = None;
            }
        }
        answer
            .map_err(shared_read_error)?
            .ok_or(QueryError::BlockNotInBestChain)
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
mod tests {
    use super::{ReplayKey, SETTLED_ENTRIES, SettledStates};
    use crate::{CanonicalReader as _, RocksDbCanonicalReader};
    use zinder_core::{BlockHash, BlockHeight, CanonicalHistoryBounds, Network};
    use zinder_store::TreeStateLookup;
    use zinder_testkit::{
        ChainFixture, WalletServingStoreFixture, sample_regtest_upgrade_activations,
    };

    #[tokio::test]
    async fn only_settled_rows_at_the_same_retained_fence_survive_eviction() -> eyre::Result<()> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(3);
        let mut store =
            WalletServingStoreFixture::from_chain(&chain, &sample_regtest_upgrade_activations())?;
        let reader = RocksDbCanonicalReader::new(store.take_readers()?.0)?;
        let height = reader.chain_epoch().visible_tip_height;
        let Some(TreeStateLookup::Stored(state)) = reader.tree_state_lookup(height).await? else {
            return Err(eyre::eyre!("missing tip checkpoint"));
        };
        let mut key = ReplayKey::new(height, &reader);
        key.epoch.settled_tip_height = BlockHeight::new(u32::try_from(SETTLED_ENTRIES)?);
        let mut cache = SettledStates::default();
        for number in 0..=key.epoch.settled_tip_height.value() {
            key.height = BlockHeight::new(number);
            cache.remember(key, &state);
        }
        assert_eq!(cache.cached(key), Some(state.clone()));
        assert_eq!(
            cache.cached(ReplayKey {
                height: BlockHeight::new(0),
                ..key
            }),
            None
        );
        let mut fork = key;
        fork.epoch.visible_tip_hash = BlockHash::from_bytes([23; 32]);
        assert!(cache.cached(fork).is_none());
        let pruned = ReplayKey {
            retained: CanonicalHistoryBounds::checkpointed(state.block_id())?,
            ..key
        };
        assert!(cache.cached(pruned).is_none());
        key.height = BlockHeight::new(key.epoch.settled_tip_height.value() + 1);
        cache.remember(key, &state);
        assert!(cache.cached(key).is_none());
        Ok(())
    }
}
