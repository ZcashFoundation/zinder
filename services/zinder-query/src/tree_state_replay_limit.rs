//! Bound on concurrent tree-state reconstructions, and the metrics of every
//! tree-state read.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zinder_core::ChainTipMetadata;

use crate::QueryError;

/// Counter of reconstructions that took a replay slot.
const REPLAY_ADMISSIONS_COUNTER: &str = "zinder_query_tree_state_replay_admissions_total";

/// Counter of tree-state requests refused because every replay slot was taken.
const REPLAY_REFUSALS_COUNTER: &str = "zinder_query_tree_state_replay_refusals_total";

/// Histogram of the compact blocks one tree-state read replayed.
const REPLAYED_BLOCKS_HISTOGRAM: &str = "zinder_query_tree_state_replayed_blocks";

/// Histogram of the note commitments one tree-state read appended.
const REPLAYED_COMMITMENTS_HISTOGRAM: &str = "zinder_query_tree_state_replayed_commitments";

/// Histogram of the time a reconstruction waited for a replay slot.
const REPLAY_WAIT_HISTOGRAM: &str = "zinder_query_tree_state_replay_wait_duration_seconds";

/// Histogram of the time one reconstruction spent replaying.
const REPLAY_DURATION_HISTOGRAM: &str = "zinder_query_tree_state_replay_duration_seconds";

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
    /// is dropped, and records the admission or refusal.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::TreeStateReplayLimitReached`] when every slot is
    /// taken.
    pub(crate) fn try_admit(&self) -> Result<OwnedSemaphorePermit, QueryError> {
        metrics::histogram!(REPLAY_WAIT_HISTOGRAM).record(Duration::ZERO.as_secs_f64());
        let permit = Arc::clone(&self.slots).try_acquire_owned().map_err(|_| {
            metrics::counter!(REPLAY_REFUSALS_COUNTER).increment(1);
            QueryError::TreeStateReplayLimitReached {
                max_concurrent_replays: self.max_replays,
            }
        })?;
        metrics::counter!(REPLAY_ADMISSIONS_COUNTER).increment(1);
        Ok(permit)
    }
}

/// Records one served tree-state read whose trees after its seed hold
/// `seed_trees`, its answer holds `served_trees`, and its seed sits
/// `replayed_blocks` below the requested height.
///
/// A stored checkpoint is its own seed and records zero blocks and zero
/// commitments, so the samples show how often requests avoid a replay.
pub(crate) fn record_tree_state_depth(
    replayed_blocks: u32,
    seed_trees: ChainTipMetadata,
    served_trees: ChainTipMetadata,
) {
    metrics::histogram!(REPLAYED_BLOCKS_HISTOGRAM).record(f64::from(replayed_blocks));
    // u64 note-commitment counts stay exact in an f64 below 2^53.
    #[allow(
        clippy::cast_precision_loss,
        reason = "a replay appends far fewer than 2^53 commitments"
    )]
    metrics::histogram!(REPLAYED_COMMITMENTS_HISTOGRAM)
        .record(appended_commitments(seed_trees, served_trees) as f64);
}

/// Records how long one completed reconstruction spent replaying.
pub(crate) fn record_replay_duration(replay_duration: Duration) {
    metrics::histogram!(REPLAY_DURATION_HISTOGRAM).record(replay_duration.as_secs_f64());
}

/// Note commitments appended across all pools to grow `seed_trees` into
/// `served_trees`.
fn appended_commitments(seed_trees: ChainTipMetadata, served_trees: ChainTipMetadata) -> u64 {
    let appended = |seed: u32, served: u32| u64::from(served.saturating_sub(seed));
    appended(
        seed_trees.sapling_commitment_tree_size,
        served_trees.sapling_commitment_tree_size,
    ) + appended(
        seed_trees.orchard_commitment_tree_size,
        served_trees.orchard_commitment_tree_size,
    ) + appended(
        seed_trees.ironwood_commitment_tree_size,
        served_trees.ironwood_commitment_tree_size,
    )
}

/// Registers `# HELP` and `# TYPE` text for the tree-state read metrics.
pub(crate) fn describe_metrics() {
    metrics::describe_counter!(
        REPLAY_ADMISSIONS_COUNTER,
        metrics::Unit::Count,
        "Tree-state rebuilds that took a rebuild slot."
    );
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
        "Compact blocks replayed to answer one tree-state request, from the nearest \
         stored checkpoint to the requested height. A stored checkpoint records 0."
    );
    metrics::describe_histogram!(
        REPLAYED_COMMITMENTS_HISTOGRAM,
        metrics::Unit::Count,
        "Note commitments appended across all pools to answer one tree-state request. \
         A stored checkpoint records 0."
    );
    metrics::describe_histogram!(
        REPLAY_WAIT_HISTOGRAM,
        metrics::Unit::Seconds,
        "Time a tree-state rebuild waited for a rebuild slot, sampled for admitted and \
         refused rebuilds."
    );
    metrics::describe_histogram!(
        REPLAY_DURATION_HISTOGRAM,
        metrics::Unit::Seconds,
        "Time one completed tree-state rebuild spent reading and replaying compact blocks."
    );
}

#[cfg(test)]
mod tests {
    use zinder_core::ChainTipMetadata;

    use super::appended_commitments;

    #[test]
    fn appended_commitments_sum_every_pool_growth() {
        assert_eq!(
            appended_commitments(
                ChainTipMetadata::new(10, 0, 4),
                ChainTipMetadata::new(13, 5, 4)
            ),
            8
        );
        assert_eq!(
            appended_commitments(ChainTipMetadata::empty(), ChainTipMetadata::empty()),
            0
        );
    }
}
