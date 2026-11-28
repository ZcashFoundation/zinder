//! Commitment-tree worker pool and bounded SST-stage handoff queue for the
//! ordered canonical loader.
//!
//! With more than one `tree_hashing_workers` thread, a dedicated tree-staging
//! thread drains bounded batches of prepared blocks, appends their commitments
//! across all worker threads, positions them in height order, and cross-checks
//! distinct pool frontiers in parallel across the batch. Completed
//! [`CanonicalBuildBlock`] values — still holding their preparation memory
//! permits — stream through a bounded queue to the `RocksDB` SST stager thread,
//! so batch `N`'s SST encoding, hashing, and disk writes overlap with batch
//! `N + 1`'s commitment-tree hashing while preserving height order and bounded
//! in-flight memory.

use std::{
    collections::VecDeque,
    num::NonZeroU32,
    sync::Arc,
    thread::{self, JoinHandle},
};

use parking_lot::{Condvar, Mutex};
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;
use zinder_core::BlockHeight;
use zinder_store::{CanonicalBuildBlock, CanonicalBuildSubtreeRoot};

use super::{CanonicalConstructionError, progress::ConstructionProgress};

/// Unified Rayon worker pool for parallel commitment-tree appends and
/// checkpoint cross-checks.
pub(super) struct TreeHashers {
    workers: rayon::ThreadPool,
}

impl TreeHashers {
    /// Starts `workers` commitment-tree hashing threads, or returns `None` for
    /// one worker, which hashes on the loader thread.
    pub(super) fn start(workers: NonZeroU32) -> Result<Option<Self>, CanonicalConstructionError> {
        let threads = usize::try_from(workers.get()).unwrap_or(usize::MAX);
        if threads <= 1 {
            return Ok(None);
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("zinder-tree-hash-{index}"))
            .panic_handler(|_| {
                tracing::error!(
                    target: "zinder::ingest",
                    event = "commitment_tree_hashing_worker_panicked",
                    "a commitment-tree hashing worker panicked"
                );
            })
            .build()
            .map_err(
                |source| CanonicalConstructionError::TreeHashingWorkersUnavailable {
                    workers: workers.get(),
                    reason: source.to_string(),
                },
            )?;
        Ok(Some(Self { workers: pool }))
    }

    /// Runs `hash` across the commitment-tree worker pool.
    pub(super) fn install<T: Send>(&self, hash: impl FnOnce() -> T + Send) -> T {
        self.workers.install(hash)
    }
}

/// One finished canonical build block paired with its preparation memory permit.
pub(super) struct GuardedBuildBlock {
    pub(super) block: CanonicalBuildBlock,
    pub(super) memory_permit: OwnedSemaphorePermit,
}

pub(super) type QueuedBuildBlock = Result<Box<GuardedBuildBlock>, CanonicalConstructionError>;

pub(super) enum PopBuildBlock {
    Ready(QueuedBuildBlock),
    Empty,
    Closed,
}

struct BuildQueueState {
    entries: VecDeque<QueuedBuildBlock>,
    producer_done: bool,
    consumer_closed: bool,
}

/// Bounded handoff queue between the tree-hashing stage and the SST stager.
pub(super) struct BoundedBuildBlockQueue {
    capacity: usize,
    state: Mutex<BuildQueueState>,
    not_empty: Condvar,
    not_full: Condvar,
}

impl BoundedBuildBlockQueue {
    pub(super) fn new(capacity: usize) -> Arc<Self> {
        let bounded_capacity = capacity.max(1);
        Arc::new(Self {
            capacity: bounded_capacity,
            state: Mutex::new(BuildQueueState {
                entries: VecDeque::with_capacity(bounded_capacity),
                producer_done: false,
                consumer_closed: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        })
    }

    /// Enqueues one ordered block or error, returning `false` when the consumer
    /// has already closed the queue.
    pub(super) fn push(&self, entry: QueuedBuildBlock) -> bool {
        let mut guard = self.state.lock();
        while !guard.consumer_closed && guard.entries.len() >= self.capacity {
            self.not_full.wait(&mut guard);
        }
        if guard.consumer_closed {
            return false;
        }
        guard.entries.push_back(entry);
        drop(guard);
        self.not_empty.notify_one();
        true
    }

    pub(super) fn finish_producer(&self) {
        let mut guard = self.state.lock();
        guard.producer_done = true;
        drop(guard);
        self.not_empty.notify_all();
    }

    pub(super) fn close_consumer(&self) {
        let mut guard = self.state.lock();
        guard.consumer_closed = true;
        guard.entries.clear();
        drop(guard);
        self.not_full.notify_all();
        self.not_empty.notify_all();
    }

    pub(super) fn try_pop(&self) -> PopBuildBlock {
        let mut guard = self.state.lock();
        if let Some(entry) = guard.entries.pop_front() {
            drop(guard);
            self.not_full.notify_one();
            return PopBuildBlock::Ready(entry);
        }
        if guard.producer_done || guard.consumer_closed {
            return PopBuildBlock::Closed;
        }
        PopBuildBlock::Empty
    }

    pub(super) fn pop_blocking(&self) -> Option<QueuedBuildBlock> {
        let mut guard = self.state.lock();
        loop {
            if let Some(entry) = guard.entries.pop_front() {
                drop(guard);
                self.not_full.notify_one();
                return Some(entry);
            }
            if guard.producer_done || guard.consumer_closed {
                return None;
            }
            self.not_empty.wait(&mut guard);
        }
    }
}

struct ProducerFinishGuard {
    queue: Arc<BoundedBuildBlockQueue>,
}

impl Drop for ProducerFinishGuard {
    fn drop(&mut self) {
        self.queue.finish_producer();
    }
}

/// Consumer-side iterator that stages finished blocks into `RocksDB` SSTs while
/// the producer thread hashes the next batch's commitment trees.
pub(super) struct PipelinedBuildBlocks {
    queue: Arc<BoundedBuildBlockQueue>,
    active_memory_permit: Option<OwnedSemaphorePermit>,
    in_flight_store_height: Option<BlockHeight>,
    progress: ConstructionProgress,
    cancel: CancellationToken,
    producer_stop: CancellationToken,
    producer_handle: Option<JoinHandle<Vec<CanonicalBuildSubtreeRoot>>>,
}

impl PipelinedBuildBlocks {
    pub(super) fn spawn(
        mut receiver: super::CanonicalBuildBlockReceiver,
        progress: ConstructionProgress,
        cancel: CancellationToken,
        producer_stop: CancellationToken,
        queue_capacity: usize,
    ) -> Result<Self, CanonicalConstructionError> {
        let queue = BoundedBuildBlockQueue::new(queue_capacity);
        let producer_queue = Arc::clone(&queue);
        let handle = thread::Builder::new()
            .name("zinder-tree-stage".to_owned())
            .spawn(move || {
                let _finish_guard = ProducerFinishGuard {
                    queue: Arc::clone(&producer_queue),
                };
                while let Some(entry) = receiver.next_guarded() {
                    let is_err = entry.is_err();
                    if !producer_queue.push(entry.map(Box::new)) || is_err {
                        break;
                    }
                }
                std::mem::take(&mut receiver.derived_subtree_roots)
            })
            .map_err(|source| CanonicalConstructionError::LoaderTaskFailed {
                reason: format!("could not start canonical tree-stage thread: {source}"),
            })?;
        Ok(Self {
            queue,
            active_memory_permit: None,
            in_flight_store_height: None,
            progress,
            cancel,
            producer_stop,
            producer_handle: Some(handle),
        })
    }

    fn record_staged_block(&mut self) {
        if let Some(height) = self.in_flight_store_height.take()
            && !self.cancel.is_cancelled()
        {
            self.progress.record_loaded(height);
        }
    }

    pub(super) fn finish_and_join(
        mut self,
        load_succeeded: bool,
    ) -> Result<(ConstructionProgress, Vec<CanonicalBuildSubtreeRoot>), CanonicalConstructionError>
    {
        self.active_memory_permit = None;
        if load_succeeded {
            self.record_staged_block();
        } else {
            self.in_flight_store_height = None;
            self.producer_stop.cancel();
            self.queue.close_consumer();
        }
        let Some(handle) = self.producer_handle.take() else {
            return Err(CanonicalConstructionError::LoaderTaskFailed {
                reason: "canonical tree-stage thread handle missing".to_owned(),
            });
        };
        let derived_subtree_roots =
            handle
                .join()
                .map_err(|_| CanonicalConstructionError::LoaderTaskFailed {
                    reason: "canonical tree-stage thread panicked".to_owned(),
                })?;
        // Move progress out before Drop runs (producer_handle is now None).
        let progress = self.progress.take_finished_handle();
        Ok((progress, derived_subtree_roots))
    }
}

impl Iterator for PipelinedBuildBlocks {
    type Item = Result<CanonicalBuildBlock, CanonicalConstructionError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.active_memory_permit = None;
        self.record_staged_block();
        if self.cancel.is_cancelled() {
            self.producer_stop.cancel();
            self.queue.close_consumer();
            return Some(Err(CanonicalConstructionError::Cancelled));
        }
        let popped = match self.queue.try_pop() {
            PopBuildBlock::Ready(entry) => Some(entry),
            PopBuildBlock::Closed => None,
            PopBuildBlock::Empty => {
                if !self.cancel.is_cancelled() {
                    self.progress.flush_pending();
                }
                self.queue.pop_blocking()
            }
        };
        match popped {
            Some(Ok(guarded)) => {
                let GuardedBuildBlock {
                    block,
                    memory_permit,
                } = *guarded;
                self.active_memory_permit = Some(memory_permit);
                self.in_flight_store_height = Some(block.facts.block_header.height);
                Some(Ok(block))
            }
            Some(Err(error)) => Some(Err(error)),
            None => self
                .cancel
                .is_cancelled()
                .then_some(Err(CanonicalConstructionError::Cancelled)),
        }
    }
}

impl Drop for PipelinedBuildBlocks {
    fn drop(&mut self) {
        self.active_memory_permit = None;
        self.producer_stop.cancel();
        self.queue.close_consumer();
        if let Some(handle) = self.producer_handle.take() {
            let _joined = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroU32,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };

    use tokio_util::sync::CancellationToken;
    use zinder_core::{BlockHash, BlockHeight, BlockId};
    use zinder_runtime::{ComposedReadiness, Readiness, ReadinessCause};
    use zinder_store::{CanonicalReorgPolicy, CanonicalStoreBuildPlan, RawBlobRetention};
    use zinder_testkit::sample_regtest_upgrade_activations;

    use super::{
        BoundedBuildBlockQueue, CanonicalConstructionError, ConstructionProgress,
        PipelinedBuildBlocks, PopBuildBlock, ProducerFinishGuard, TreeHashers,
    };

    #[test]
    fn one_worker_hashes_on_the_loader_thread() -> Result<(), Box<dyn std::error::Error>> {
        assert!(TreeHashers::start(NonZeroU32::MIN)?.is_none());
        Ok(())
    }

    #[test]
    fn unified_worker_pool_uses_all_configured_threads() -> Result<(), Box<dyn std::error::Error>> {
        for count in [2_u32, 4, 8] {
            let workers = NonZeroU32::new(count).ok_or("workers must be non-zero")?;
            let hashers = TreeHashers::start(workers)?.ok_or("expected worker pool")?;
            let threads = hashers.install(rayon::current_num_threads);
            assert_eq!(threads, usize::try_from(count)?);
        }
        Ok(())
    }

    #[test]
    #[allow(
        clippy::significant_drop_tightening,
        reason = "queue stays alive through the final try_pop assertion"
    )]
    fn bounded_build_queue_unblocks_blocked_producer_when_consumer_closes()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = BoundedBuildBlockQueue::new(1);
        assert!(queue.push(Err(CanonicalConstructionError::Cancelled)));

        let producer_queue = Arc::clone(&queue);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let producer = thread::spawn(move || {
            let _ = entered_tx.send(());
            producer_queue.push(Err(CanonicalConstructionError::Cancelled))
        });
        entered_rx.recv()?;
        thread::sleep(Duration::from_millis(20));

        let started_at = Instant::now();
        queue.close_consumer();
        let accepted = producer.join().map_err(|_| "producer panicked")?;
        assert!(!accepted);
        assert!(started_at.elapsed() < Duration::from_secs(1));
        assert!(matches!(queue.try_pop(), PopBuildBlock::Closed));
        Ok(())
    }

    #[test]
    #[allow(
        clippy::panic,
        reason = "verifies unwind safety of ProducerFinishGuard"
    )]
    fn producer_finish_guard_wakes_consumer_on_unwind() {
        let queue = BoundedBuildBlockQueue::new(2);
        let producer_queue = Arc::clone(&queue);
        let producer = thread::spawn(move || {
            let _guard = ProducerFinishGuard {
                queue: producer_queue,
            };
            std::panic::panic_any("injected producer unwind");
        });
        assert!(producer.join().is_err());
        assert!(queue.pop_blocking().is_none());
    }

    #[test]
    fn finish_and_join_skips_recording_failed_in_flight_block_height()
    -> Result<(), Box<dyn std::error::Error>> {
        let activations = sample_regtest_upgrade_activations();
        let build_plan = CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(BlockHeight::new(5), BlockHash::from_bytes([5; 32])),
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(100)?,
        )?;
        for (succeeded, expected_height, expected_lag) in
            [(false, Some(0), Some(5)), (true, Some(5), Some(0))]
        {
            let readiness = ComposedReadiness::new(Readiness::default(), &[]);
            let (progress, subtrees) = (PipelinedBuildBlocks {
                queue: BoundedBuildBlockQueue::new(1),
                active_memory_permit: None,
                in_flight_store_height: Some(BlockHeight::new(5)),
                progress: ConstructionProgress::start(&build_plan, Some(readiness.clone())),
                cancel: CancellationToken::new(),
                producer_stop: CancellationToken::new(),
                producer_handle: Some(thread::spawn(Vec::new)),
            })
            .finish_and_join(succeeded)?;
            drop(progress);
            let report = readiness.runtime_readiness().report();
            drop(readiness);
            assert_eq!(subtrees.len(), 0);
            assert_eq!(report.current_height, expected_height);
            assert_eq!(
                report.cause,
                ReadinessCause::Syncing {
                    lag_blocks: expected_lag,
                    wait_reason: None,
                }
            );
        }
        Ok(())
    }
}
