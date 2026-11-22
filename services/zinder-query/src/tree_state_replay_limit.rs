//! Bound on concurrent tree-state reconstructions, and the metrics of every
//! tree-state read.

mod flights;
#[cfg(feature = "light-wallet-qualification")]
pub(crate) use flights::ReadyState;
pub(crate) use flights::{ReplayKey, Selection, read_missing_height};

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use zinder_core::{BlockHeight, ChainTipMetadata};
use zinder_store::{ServedTreeState, TreeStateReplayOutcome};

use crate::{QueryError, StorageReadError};

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

/// Default time a reconstruction waits for a replay slot before it is
/// refused.
pub const DEFAULT_TREE_STATE_REPLAY_WAIT: Duration = Duration::from_secs(2);

/// Longest replay-slot wait a query accepts.
///
/// The wait and the reconstruction that follows it share the client's
/// deadline. A full reconstruction in the densest history takes seconds, so
/// a longer wait would let admitted requests time out at a 10-second client
/// deadline.
pub const MAX_TREE_STATE_REPLAY_WAIT: Duration = Duration::from_secs(5);

/// Default number of requests that may wait for a replay slot, per slot.
///
/// At a 30 ms rebuild, about the 99th percentile measured on Testnet, one
/// slot serves 64 queued requests in 1.92 s, inside
/// [`DEFAULT_TREE_STATE_REPLAY_WAIT`], so a burst of a few hundred wallets
/// queues instead of being refused.
pub const DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(63);

/// Most requests a query lets wait for a replay slot, per slot.
///
/// The cap keeps the waiting requests a bounded number of small futures.
pub const MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT: NonZeroU32 = NonZeroU32::MIN.saturating_add(255);

/// Limit on the tree-state reconstructions one wallet query runs at once.
///
/// A height without a stored checkpoint is reconstructed by replaying up to a
/// checkpoint stride of compact blocks on the blocking pool, so concurrent
/// reconstructions of dense history could otherwise occupy every blocking
/// thread. Clones share one limit, so the clones of a runtime's query share
/// it. A request that finds every slot taken waits for one up to the
/// configured wait, alongside at most the configured number of other
/// requests per slot, [`DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT`] by
/// default, and is refused when the wait ends or the waiting places are
/// full. A waiter that is dropped gives up its place at once.
#[derive(Clone, Debug)]
pub(crate) struct TreeStateReplayLimit {
    flights: Arc<parking_lot::Mutex<flights::Flights>>,
    slots: Arc<Semaphore>,
    max_replays: NonZeroU32,
    waiters: Arc<AtomicU32>,
    max_waiters: u32,
    wait: Duration,
}

impl TreeStateReplayLimit {
    /// Admits one reconstruction per available CPU, each waiting up to
    /// `wait` for a slot.
    pub(crate) fn for_available_parallelism(wait: Duration) -> Self {
        let max_replays = std::thread::available_parallelism()
            .ok()
            .and_then(|parallelism| u32::try_from(parallelism.get()).ok())
            .and_then(NonZeroU32::new)
            .unwrap_or(NonZeroU32::MIN);
        Self::new(max_replays, wait)
    }

    /// Admits at most `max_replays` reconstructions at once, each waiting up
    /// to `wait`, capped at [`MAX_TREE_STATE_REPLAY_WAIT`], for a slot.
    pub(crate) fn new(max_replays: NonZeroU32, wait: Duration) -> Self {
        let permits = usize::try_from(max_replays.get())
            .unwrap_or(Semaphore::MAX_PERMITS)
            .min(Semaphore::MAX_PERMITS);
        Self {
            flights: Arc::default(),
            slots: Arc::new(Semaphore::new(permits)),
            max_replays,
            waiters: Arc::new(AtomicU32::new(0)),
            max_waiters: max_replays
                .get()
                .saturating_mul(DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT.get()),
            wait: wait.min(MAX_TREE_STATE_REPLAY_WAIT),
        }
    }

    /// Returns this limit with every reconstruction waiting up to `wait`,
    /// capped at [`MAX_TREE_STATE_REPLAY_WAIT`], for a slot.
    pub(crate) fn with_wait(self, wait: Duration) -> Self {
        Self {
            wait: wait.min(MAX_TREE_STATE_REPLAY_WAIT),
            ..self
        }
    }

    /// Returns this limit with at most `waiters_per_slot` requests, capped at
    /// [`MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT`], waiting for each slot.
    pub(crate) fn with_waiters_per_slot(self, waiters_per_slot: NonZeroU32) -> Self {
        Self {
            max_waiters: self.max_replays.get().saturating_mul(
                waiters_per_slot
                    .min(MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT)
                    .get(),
            ),
            ..self
        }
    }

    /// Takes a slot for one reconstruction, held until the returned permit
    /// is dropped, and records the wait and the admission or refusal.
    ///
    /// Dropping the returned future gives up its waiting place and records
    /// nothing.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::TreeStateReplayLimitReached`] when no slot frees
    /// within the wait, or when every waiting place is taken.
    #[cfg(test)]
    pub(crate) async fn admit(&self) -> Result<OwnedSemaphorePermit, QueryError> {
        let (admission, _waiting) = self.admit_until(self.deadline())?;
        admission.await.map_err(|reason| self.refused(reason))
    }

    pub(crate) fn deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.wait
    }

    fn refused(&self, refusal: ReplayRefusal) -> QueryError {
        metrics::counter!(REPLAY_REFUSALS_COUNTER, "reason" => refusal.label()).increment(1);
        QueryError::TreeStateReplayLimitReached {
            max_concurrent_replays: self.max_replays,
        }
    }

    fn admit_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<
        (
            impl Future<Output = Result<OwnedSemaphorePermit, ReplayRefusal>> + use<>,
            Option<WaitingPlace>,
        ),
        QueryError,
    > {
        let started_at = Instant::now();
        let (admission, waiting) = self.acquire(deadline).map_err(|reason| {
            metrics::histogram!(REPLAY_WAIT_HISTOGRAM).record(started_at.elapsed().as_secs_f64());
            self.refused(reason)
        })?;
        Ok((
            async move {
                let slot = admission.await;
                metrics::histogram!(REPLAY_WAIT_HISTOGRAM)
                    .record(started_at.elapsed().as_secs_f64());
                if slot.is_ok() {
                    metrics::counter!(REPLAY_ADMISSIONS_COUNTER).increment(1);
                }
                slot
            },
            waiting,
        ))
    }

    fn acquire(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<
        (
            impl Future<Output = Result<OwnedSemaphorePermit, ReplayRefusal>> + use<>,
            Option<WaitingPlace>,
        ),
        ReplayRefusal,
    > {
        use futures_util::future::{Either, ready};
        let slots = Arc::clone(&self.slots);
        if let Ok(permit) = slots.clone().try_acquire_owned() {
            return Ok((Either::Left(ready(Ok(permit))), None));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ReplayRefusal::WaitExpired);
        }
        let Some(place) = WaitingPlace::take(&self.waiters, self.max_waiters) else {
            return Err(ReplayRefusal::WaitersFull);
        };
        Ok((
            Either::Right(async move {
                match tokio::time::timeout_at(deadline, slots.acquire_owned()).await {
                    Ok(Ok(permit)) => Ok(permit),
                    Ok(Err(_)) | Err(_) => Err(ReplayRefusal::WaitExpired),
                }
            }),
            Some(place),
        ))
    }
}

/// Why a reconstruction was refused a replay slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplayRefusal {
    /// No slot freed within the wait.
    WaitExpired,
    /// Every waiting place was taken.
    WaitersFull,
}

impl ReplayRefusal {
    const fn label(self) -> &'static str {
        match self {
            Self::WaitExpired => "wait_expired",
            Self::WaitersFull => "waiters_full",
        }
    }
}

/// One of the places a request waiting for a replay slot holds, released
/// when dropped.
#[derive(Debug)]
struct WaitingPlace {
    waiters: Arc<AtomicU32>,
}

impl WaitingPlace {
    fn take(waiters: &Arc<AtomicU32>, max_waiters: u32) -> Option<Self> {
        waiters
            .try_update(Ordering::AcqRel, Ordering::Acquire, |waiting| {
                (waiting < max_waiters).then_some(waiting + 1)
            })
            .ok()
            .map(|_| Self {
                waiters: waiters.clone(),
            })
    }
}

impl Drop for WaitingPlace {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Holds the slot on the replay task so cancelling the request cannot release
/// capacity while its storage read is still running.
async fn replay_holding_slot<Read>(
    height: BlockHeight,
    replay_slot: OwnedSemaphorePermit,
    read: Read,
) -> Result<Option<(ServedTreeState, bool)>, StorageReadError>
where
    Read: Future<Output = Result<Option<TreeStateReplayOutcome>, StorageReadError>>,
{
    let started_at = Instant::now();
    let outcome = read.await;
    let duration = started_at.elapsed();
    drop(replay_slot);
    Ok(outcome?.map(|outcome| match outcome {
        TreeStateReplayOutcome::Stored(stored) => (stored, false),
        TreeStateReplayOutcome::Replayed { replay, tree_state } => {
            record_replay_duration(height, duration);
            record_replay_work(
                &height_band(height),
                replay.replayed_blocks(),
                replay.seed().tip_metadata(),
                tree_state.tip_metadata(),
            );
            (tree_state, true)
        }
    }))
}

fn record_tree_state_read(height: BlockHeight, source: &'static str) {
    metrics::counter!(TREE_STATE_READS_COUNTER, "source" => source, "height_band" => height_band(height))
        .increment(1);
}

fn record_replay_work(
    band: &str,
    replayed_blocks: u32,
    seed_trees: ChainTipMetadata,
    served_trees: ChainTipMetadata,
) {
    metrics::histogram!(REPLAYED_BLOCKS_HISTOGRAM, "height_band" => band.to_owned())
        .record(f64::from(replayed_blocks));
    for (pool, appended) in appended_commitments(seed_trees, served_trees) {
        metrics::histogram!(
            REPLAYED_COMMITMENTS_HISTOGRAM,
            "pool" => pool,
            "height_band" => band.to_owned()
        )
        .record(f64::from(appended));
    }
}

/// Records how long one completed reconstruction of `height` spent replaying.
fn record_replay_duration(height: BlockHeight, replay_duration: Duration) {
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
        "Served tree-state reads by source (stored, cached, replayed, or coalesced) and \
         250,000-block height band. Each caller receiving a state counts once."
    );
    metrics::describe_counter!(
        REPLAY_ADMISSIONS_COUNTER,
        metrics::Unit::Count,
        "Tree-state rebuilds that took a rebuild slot."
    );
    metrics::describe_counter!(
        REPLAY_REFUSALS_COUNTER,
        metrics::Unit::Count,
        "Tree-state requests refused a rebuild slot. `reason` is `wait_expired` when no \
         slot freed within the wait and `waiters_full` when every waiting place was \
         taken. Sustained growth means wallets request heights without a stored \
         checkpoint faster than the server rebuilds them."
    );
    metrics::describe_histogram!(
        REPLAYED_BLOCKS_HISTOGRAM,
        metrics::Unit::Count,
        "Compact blocks replayed per completed reconstruction, by height band."
    );
    metrics::describe_histogram!(
        REPLAYED_COMMITMENTS_HISTOGRAM,
        metrics::Unit::Count,
        "Note commitments appended per completed reconstruction, by pool and height band."
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
    use std::{error::Error, num::NonZeroU32, sync::atomic::Ordering, time::Duration};

    use zinder_core::{BlockHeight, ChainTipMetadata};

    use super::{
        DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT, MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
        ReplayRefusal, TreeStateReplayLimit, appended_commitments, height_band,
    };
    use crate::QueryError;

    const WAIT: Duration = Duration::from_secs(2);

    fn one_slot() -> TreeStateReplayLimit {
        TreeStateReplayLimit::new(NonZeroU32::MIN, WAIT)
    }

    fn is_refused<T>(admission: &Result<T, QueryError>) -> bool {
        matches!(
            admission,
            Err(QueryError::TreeStateReplayLimitReached { .. })
        )
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiter_takes_a_slot_that_frees_within_the_wait() -> Result<(), Box<dyn Error>> {
        let limit = one_slot();
        let held = limit.admit().await?;
        let waiter = tokio::spawn({
            let limit = limit.clone();
            async move { limit.admit().await.map(drop) }
        });
        tokio::time::sleep(WAIT / 2).await;
        drop(held);
        waiter.await??;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiter_is_refused_when_the_wait_ends() -> Result<(), Box<dyn Error>> {
        let limit = one_slot();
        let _held = limit.admit().await?;
        let started_at = tokio::time::Instant::now();
        assert!(is_refused(&limit.admit().await));
        assert_eq!(started_at.elapsed(), WAIT);
        Ok(())
    }

    /// Spawns `count` requests that each take a slot through `limit`, hold
    /// it for `rebuild`, and release it.
    fn spawn_rebuilds(
        limit: &TreeStateReplayLimit,
        count: u32,
        rebuild: Duration,
    ) -> Vec<tokio::task::JoinHandle<Result<(), QueryError>>> {
        (0..count)
            .map(|_| {
                let limit = limit.clone();
                tokio::spawn(async move {
                    let permit = limit.admit().await?;
                    tokio::time::sleep(rebuild).await;
                    drop(permit);
                    Ok(())
                })
            })
            .collect()
    }

    /// Lets spawned requests run until `waiting` of them wait for a slot.
    async fn until_waiting(limit: &TreeStateReplayLimit, waiting: u32) -> Result<(), String> {
        for _ in 0..10_000 {
            if limit.waiters.load(Ordering::Acquire) == waiting {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
        Err(format!(
            "{} requests waited, not {waiting}",
            limit.waiters.load(Ordering::Acquire)
        ))
    }

    /// With the default 64 waiting places per slot and 30 ms rebuilds, about
    /// the measured 99th percentile, every request of a burst far larger
    /// than the slots is served within the 2-second wait.
    #[tokio::test(start_paused = true)]
    async fn a_burst_of_fast_rebuilds_waits_instead_of_being_refused() -> Result<(), Box<dyn Error>>
    {
        const SLOTS: u32 = 2;
        const REBUILD: Duration = Duration::from_millis(30);
        let limit = TreeStateReplayLimit::new(NonZeroU32::MIN.saturating_add(SLOTS - 1), WAIT);
        let burst = SLOTS * (1 + DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT.get());
        let started_at = tokio::time::Instant::now();

        for rebuild in spawn_rebuilds(&limit, burst, REBUILD) {
            rebuild.await??;
        }
        assert!(started_at.elapsed() < WAIT, "{:?}", started_at.elapsed());
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_past_the_waiting_places_is_refused_at_once() -> Result<(), Box<dyn Error>> {
        let limit = one_slot();
        let _held = limit.admit().await?;
        let waiting = DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT.get();
        let waiters = spawn_rebuilds(&limit, waiting, Duration::ZERO);
        until_waiting(&limit, waiting).await?;

        let started_at = tokio::time::Instant::now();
        assert_eq!(
            limit.acquire(limit.deadline()).err(),
            Some(ReplayRefusal::WaitersFull)
        );
        assert!(is_refused(&limit.admit().await));
        assert_eq!(started_at.elapsed(), Duration::ZERO);
        for waiter in waiters {
            assert!(is_refused(&waiter.await?));
        }
        Ok(())
    }

    /// A configured count replaces the default, and the next request past it
    /// is refused for full waiting places.
    #[tokio::test(start_paused = true)]
    async fn the_configured_waiters_per_slot_bound_the_waiting_places() -> Result<(), Box<dyn Error>>
    {
        let limit = one_slot().with_waiters_per_slot(NonZeroU32::MIN.saturating_add(2));
        let _held = limit.admit().await?;
        let waiters = spawn_rebuilds(&limit, 3, Duration::ZERO);
        until_waiting(&limit, 3).await?;

        assert_eq!(
            limit.acquire(limit.deadline()).err(),
            Some(ReplayRefusal::WaitersFull)
        );
        for waiter in waiters {
            waiter.abort();
        }
        Ok(())
    }

    /// Dropping waiting requests, as a server does when their clients go
    /// away, frees their places at once, so a new request waits again.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_waiter_gives_up_its_waiting_place() -> Result<(), Box<dyn Error>> {
        let limit = one_slot();
        let held = limit.admit().await?;
        let waiting = DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT.get();
        let waiters = spawn_rebuilds(&limit, waiting, Duration::ZERO);
        until_waiting(&limit, waiting).await?;

        let cancelled_at = tokio::time::Instant::now();
        for waiter in waiters {
            waiter.abort();
            assert!(waiter.await.is_err_and(|error| error.is_cancelled()));
        }
        assert_eq!(limit.waiters.load(Ordering::Acquire), 0);
        assert_eq!(cancelled_at.elapsed(), Duration::ZERO);

        let waiter = spawn_rebuilds(&limit, 1, Duration::ZERO);
        until_waiting(&limit, 1).await?;
        drop(held);
        for waiter in waiter {
            waiter.await??;
        }
        Ok(())
    }

    #[test]
    fn waiters_per_slot_default_to_64_and_are_capped_at_256() {
        let two_slots = NonZeroU32::MIN.saturating_add(1);
        let limit = TreeStateReplayLimit::new(two_slots, WAIT);
        assert_eq!(limit.max_waiters, 2 * 64);
        assert_eq!(
            limit.with_waiters_per_slot(NonZeroU32::MAX).max_waiters,
            2 * MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT.get()
        );
    }

    #[test]
    fn a_wait_is_capped_at_the_longest_the_client_deadline_allows() {
        let limit = TreeStateReplayLimit::new(NonZeroU32::MIN, Duration::from_mins(1));
        assert_eq!(limit.wait, super::MAX_TREE_STATE_REPLAY_WAIT);
    }

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
