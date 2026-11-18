//! Bound on concurrent tree-state reconstructions.

use std::{num::NonZeroU32, sync::Arc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::QueryError;

/// Counter of tree-state requests refused because every replay slot was taken.
const REPLAY_REFUSALS_COUNTER: &str = "zinder_query_tree_state_replay_refusals_total";

/// Histogram of the compact blocks one tree-state reconstruction replayed.
const REPLAYED_BLOCKS_HISTOGRAM: &str = "zinder_query_tree_state_replayed_blocks";

/// Limit on the tree-state reconstructions one wallet query runs at once.
///
/// A height without a stored checkpoint is reconstructed by replaying up to a
/// checkpoint stride of compact blocks on the blocking pool, so concurrent
/// reconstructions of dense history could otherwise occupy every blocking
/// thread. Clones share one limit, so the clones of a runtime's query share
/// it. A request that finds every slot taken is refused at once rather than
/// queued.
#[derive(Clone, Debug)]
pub(crate) struct TreeStateReplayLimit {
    slots: Arc<Semaphore>,
    max_replays: NonZeroU32,
}

impl TreeStateReplayLimit {
    /// Admits one reconstruction per available CPU.
    pub(crate) fn for_available_parallelism() -> Self {
        let max_replays = std::thread::available_parallelism()
            .ok()
            .and_then(|parallelism| u32::try_from(parallelism.get()).ok())
            .and_then(NonZeroU32::new)
            .unwrap_or(NonZeroU32::MIN);
        Self::new(max_replays)
    }

    /// Admits at most `max_replays` reconstructions at once.
    pub(crate) fn new(max_replays: NonZeroU32) -> Self {
        let permits = usize::try_from(max_replays.get())
            .unwrap_or(Semaphore::MAX_PERMITS)
            .min(Semaphore::MAX_PERMITS);
        Self {
            slots: Arc::new(Semaphore::new(permits)),
            max_replays,
        }
    }

    /// Takes a slot for one reconstruction, held until the returned permit
    /// is dropped.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::TreeStateReplayLimitReached`] when every slot is
    /// taken, and counts the refusal in
    /// `zinder_query_tree_state_replay_refusals_total`.
    pub(crate) fn try_admit(&self) -> Result<OwnedSemaphorePermit, QueryError> {
        Arc::clone(&self.slots).try_acquire_owned().map_err(|_| {
            metrics::counter!(REPLAY_REFUSALS_COUNTER).increment(1);
            QueryError::TreeStateReplayLimitReached {
                max_concurrent_replays: self.max_replays,
            }
        })
    }
}

/// Records how many compact blocks one reconstruction replayed.
pub(crate) fn record_replayed_blocks(replayed_blocks: u32) {
    metrics::histogram!(REPLAYED_BLOCKS_HISTOGRAM).record(f64::from(replayed_blocks));
}

/// Registers `# HELP` and `# TYPE` text for the tree-state rebuild metrics.
pub(crate) fn describe_metrics() {
    metrics::describe_counter!(
        REPLAY_REFUSALS_COUNTER,
        metrics::Unit::Count,
        "Tree-state requests refused because the query already runs its limit of \
         rebuilds. Sustained growth means wallets request heights without a stored \
         checkpoint faster than the server rebuilds them."
    );
    metrics::describe_histogram!(
        REPLAYED_BLOCKS_HISTOGRAM,
        metrics::Unit::Count,
        "Compact blocks replayed by one tree-state rebuild, from the nearest stored \
         checkpoint to the requested height."
    );
}
