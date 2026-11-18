//! Bound on concurrent tree-state reconstructions, and the metrics of every
//! tree-state read.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zinder_core::{BlockHeight, ChainTipMetadata};

use crate::QueryError;

/// Blocks in one `height_band` label value.
///
/// A coarse band keeps the label set small, about 15 values on Mainnet, while
/// still separating dense eras such as the 2022 spam range from the rest.
const HEIGHT_BAND_BLOCKS: u32 = 250_000;

/// Counter of served tree-state reads, by whether a checkpoint was stored at
/// the height.
const TREE_STATE_READS_COUNTER: &str = "zinder_query_tree_state_reads_total";

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

/// Records one served tree-state read at `height` whose seed sits
/// `replayed_blocks` below it, whose trees after that seed hold `seed_trees`,
/// and whose answer holds `served_trees`.
///
/// A stored checkpoint is its own seed, so it counts as a `stored` read and
/// records zero blocks and zero commitments in every pool. Every other read
/// replays at least one block and counts as `replayed`.
pub(crate) fn record_tree_state_read(
    height: BlockHeight,
    replayed_blocks: u32,
    seed_trees: ChainTipMetadata,
    served_trees: ChainTipMetadata,
) {
    let band = height_band(height);
    let source = if replayed_blocks == 0 {
        "stored"
    } else {
        "replayed"
    };
    metrics::counter!(TREE_STATE_READS_COUNTER, "source" => source, "height_band" => band.clone())
        .increment(1);
    metrics::histogram!(REPLAYED_BLOCKS_HISTOGRAM, "height_band" => band.clone())
        .record(f64::from(replayed_blocks));
    for (pool, appended) in appended_commitments(seed_trees, served_trees) {
        metrics::histogram!(
            REPLAYED_COMMITMENTS_HISTOGRAM,
            "pool" => pool,
            "height_band" => band.clone()
        )
        .record(f64::from(appended));
    }
}

/// Records how long one completed reconstruction of `height` spent replaying.
pub(crate) fn record_replay_duration(height: BlockHeight, replay_duration: Duration) {
    metrics::histogram!(REPLAY_DURATION_HISTOGRAM, "height_band" => height_band(height))
        .record(replay_duration.as_secs_f64());
}

/// The `height_band` label of `height`: the first height of its
/// [`HEIGHT_BAND_BLOCKS`]-block band.
fn height_band(height: BlockHeight) -> String {
    (height.value() - height.value() % HEIGHT_BAND_BLOCKS).to_string()
}

/// Note commitments appended to each pool to grow `seed_trees` into
/// `served_trees`, labelled by pool.
fn appended_commitments(
    seed_trees: ChainTipMetadata,
    served_trees: ChainTipMetadata,
) -> [(&'static str, u32); 3] {
    [
        (
            "sapling",
            served_trees
                .sapling_commitment_tree_size
                .saturating_sub(seed_trees.sapling_commitment_tree_size),
        ),
        (
            "orchard",
            served_trees
                .orchard_commitment_tree_size
                .saturating_sub(seed_trees.orchard_commitment_tree_size),
        ),
        (
            "ironwood",
            served_trees
                .ironwood_commitment_tree_size
                .saturating_sub(seed_trees.ironwood_commitment_tree_size),
        ),
    ]
}

/// Registers `# HELP` and `# TYPE` text for the tree-state read metrics.
pub(crate) fn describe_metrics() {
    metrics::describe_counter!(
        TREE_STATE_READS_COUNTER,
        metrics::Unit::Count,
        "Served tree-state reads. `source` is `stored` when a checkpoint is stored at the \
         height and `replayed` when it was rebuilt; `height_band` is the first height of \
         the read's 250,000-block band."
    );
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
         stored checkpoint to the requested height, by height band. A stored \
         checkpoint records 0."
    );
    metrics::describe_histogram!(
        REPLAYED_COMMITMENTS_HISTOGRAM,
        metrics::Unit::Count,
        "Note commitments appended to one pool to answer one tree-state request, by \
         pool and height band. A stored checkpoint records 0."
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
        "Time one completed tree-state rebuild spent reading and replaying compact blocks, \
         by height band."
    );
}

#[cfg(test)]
mod tests {
    use zinder_core::{BlockHeight, ChainTipMetadata};

    use super::{appended_commitments, height_band};

    #[test]
    fn appended_commitments_count_each_pool_growth() {
        assert_eq!(
            appended_commitments(
                ChainTipMetadata::new(10, 0, 4),
                ChainTipMetadata::new(13, 5, 4)
            ),
            [("sapling", 3), ("orchard", 5), ("ironwood", 0)]
        );
    }

    #[test]
    fn height_bands_start_at_each_250_000_block_boundary() {
        assert_eq!(height_band(BlockHeight::new(0)), "0");
        assert_eq!(height_band(BlockHeight::new(249_999)), "0");
        assert_eq!(height_band(BlockHeight::new(1_812_345)), "1750000");
    }
}
