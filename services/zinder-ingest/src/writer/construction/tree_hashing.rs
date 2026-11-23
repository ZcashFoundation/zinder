//! Commitment-tree hashing threads for the ordered canonical loader.
//!
//! The loader still appends every block to the trees and positions it in
//! height order. With more than one worker, each append hashes on the workers,
//! and the block's checkpoint and final roots are derived on them from a
//! snapshot of the trees while the loader appends and writes the blocks around
//! it. A block is handed to the store only after its own tree work finishes,
//! so blocks reach the store in height order with the trees and checkpoints a
//! serial loader produces.

use std::{collections::VecDeque, num::NonZeroU32};

use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, oneshot},
};
use tokio_util::sync::CancellationToken;
use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, BlockId, CommitmentTreeAccumulatorError,
    CommitmentTreeCheckpoint, CommitmentTreeSnapshot,
};

use super::CanonicalConstructionError;
use crate::artifact_builder::PositionedCanonicalBlock;

/// Most blocks whose tree work can be outstanding, whatever the worker count.
const MAX_PENDING_BLOCKS: usize = 64;

/// Worker threads and the blocks whose checkpoints they are still deriving.
pub(super) struct TreeHashers {
    workers: rayon::ThreadPool,
    /// The runtime the loader's blocking thread waits on, so a wait for a
    /// worker also ends when construction is cancelled.
    runtime: Handle,
    /// Appended blocks in height order. An entry that failed to append ends
    /// the queue, so the blocks before it still reach the store first.
    pending: VecDeque<Result<PendingBuildBlock, CanonicalConstructionError>>,
    max_pending: usize,
}

impl TreeHashers {
    /// Starts `workers` hashing threads, or returns `None` for one worker,
    /// which hashes on the loader thread.
    pub(super) fn start(
        workers: NonZeroU32,
        runtime: Handle,
    ) -> Result<Option<Self>, CanonicalConstructionError> {
        let threads = usize::try_from(workers.get()).unwrap_or(usize::MAX);
        if threads == 1 {
            return Ok(None);
        }
        let workers = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("zinder-tree-hash-{index}"))
            // A panicking job drops its result sender, which the loader
            // reports as a failed block instead of aborting the process.
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
        Ok(Some(Self {
            workers,
            runtime,
            pending: VecDeque::new(),
            max_pending: threads.saturating_mul(2).min(MAX_PENDING_BLOCKS),
        }))
    }

    /// Runs `hash` on the workers, which it may split further.
    pub(super) fn install<T: Send>(&self, hash: impl FnOnce() -> T + Send) -> T {
        self.workers.install(hash)
    }

    /// Whether the loader should take another block before handing one to
    /// the store.
    pub(super) fn wants_more(&self) -> bool {
        self.pending.len() < self.max_pending && !self.pending.back().is_some_and(Result::is_err)
    }

    pub(super) fn is_idle(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn push(&mut self, block: Result<PendingBuildBlock, CanonicalConstructionError>) {
        self.pending.push_back(block);
    }

    pub(super) fn pop(&mut self) -> Option<Result<PendingBuildBlock, CanonicalConstructionError>> {
        self.pending.pop_front()
    }

    /// Derives the block's checkpoint, when `checkpoint_required`, and its
    /// final roots, when `final_roots_required`, from `snapshot` on a worker.
    pub(super) fn spawn_tree_work(
        &self,
        snapshot: CommitmentTreeSnapshot,
        block: BlockId,
        block_time_seconds: u32,
        work: TreeWork,
    ) -> PendingTreeWork {
        self.spawn_job(move || {
            let checkpoint = work.checkpoint_required.then(|| {
                snapshot.cross_checked_frontiers().map(|frontiers| {
                    CommitmentTreeCheckpoint::new(block, block_time_seconds, frontiers)
                })
            });
            let final_roots = work
                .final_roots_required
                .then(|| final_roots(&snapshot, block.hash))
                .flatten();
            TreeWorkOutput {
                checkpoint,
                final_roots,
            }
        })
    }

    fn spawn_job(&self, job: impl FnOnce() -> TreeWorkOutput + Send + 'static) -> PendingTreeWork {
        let (sender, receiver) = oneshot::channel();
        self.workers.spawn(move || {
            // The loader drops the receiver only when construction stops.
            let _loader_stopped = sender.send(job());
        });
        PendingTreeWork {
            receiver,
            runtime: self.runtime.clone(),
        }
    }
}

/// Which of a block's checkpoint and final roots the store needs.
#[derive(Clone, Copy)]
pub(super) struct TreeWork {
    pub(super) checkpoint_required: bool,
    pub(super) final_roots_required: bool,
}

/// A block appended to the trees, waiting for its tree work.
pub(super) struct PendingBuildBlock {
    pub(super) height: BlockHeight,
    pub(super) positioned: PositionedCanonicalBlock,
    pub(super) memory_permit: OwnedSemaphorePermit,
    pub(super) work: TreeWork,
    pub(super) tree_work: Option<PendingTreeWork>,
}

pub(super) struct PendingTreeWork {
    receiver: oneshot::Receiver<TreeWorkOutput>,
    runtime: Handle,
}

/// A block's checkpoint and final roots, derived on a worker.
#[derive(Default)]
pub(super) struct TreeWorkOutput {
    pub(super) checkpoint: Option<Result<CommitmentTreeCheckpoint, CommitmentTreeAccumulatorError>>,
    pub(super) final_roots: Option<BlockFinalNoteCommitmentRoots>,
}

impl PendingTreeWork {
    /// Waits for the worker deriving this block's tree work, or until
    /// `cancel` fires.
    ///
    /// Call it only from a blocking thread, never from async code.
    pub(super) fn wait(
        self,
        cancel: &CancellationToken,
    ) -> Result<TreeWorkOutput, CanonicalConstructionError> {
        let Self { receiver, runtime } = self;
        runtime.block_on(async {
            tokio::select! {
                biased;
                () = cancel.cancelled() => Err(CanonicalConstructionError::Cancelled),
                output = receiver => output.map_err(|_| CanonicalConstructionError::LoaderTaskFailed {
                    reason: "a commitment-tree hashing worker stopped before finishing a block"
                        .to_owned(),
                }),
            }
        })
    }
}

/// Derives the explorer workload's final roots, omitting a block before any
/// pool activates, as the serial loader does.
pub(super) fn final_roots(
    snapshot: &CommitmentTreeSnapshot,
    block_hash: BlockHash,
) -> Option<BlockFinalNoteCommitmentRoots> {
    let roots = snapshot.final_note_commitment_roots(block_hash);
    (roots.sapling.is_some() || roots.orchard.is_some() || roots.ironwood.is_some())
        .then_some(roots)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        time::{Duration, Instant},
    };

    use tokio_util::sync::CancellationToken;

    use super::{CanonicalConstructionError, NonZeroU32, TreeHashers, TreeWorkOutput};

    /// Starts two workers whose waits run on `runtime`.
    fn two_workers(
        runtime: &tokio::runtime::Runtime,
    ) -> Result<TreeHashers, Box<dyn std::error::Error>> {
        TreeHashers::start(
            NonZeroU32::new(2).ok_or("two is non-zero")?,
            runtime.handle().clone(),
        )?
        .ok_or_else(|| "two workers start a pool".into())
    }

    #[test]
    fn one_worker_hashes_on_the_loader_thread() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Runtime::new()?;
        assert!(TreeHashers::start(NonZeroU32::MIN, runtime.handle().clone())?.is_none());
        Ok(())
    }

    #[test]
    #[allow(clippy::panic, reason = "the test injects a worker panic")]
    fn a_panicking_worker_fails_its_block_instead_of_the_process()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let tree_hashers = two_workers(&runtime)?;
        let cancel = CancellationToken::new();
        assert!(matches!(
            tree_hashers
                .spawn_job(|| std::panic::panic_any("injected worker failure"))
                .wait(&cancel),
            Err(CanonicalConstructionError::LoaderTaskFailed { .. })
        ));
        // The pool keeps running the next block's work.
        let next = tree_hashers.spawn_job(TreeWorkOutput::default);
        drop(tree_hashers);
        assert!(next.wait(&cancel).is_ok());
        Ok(())
    }

    #[test]
    fn stopping_while_a_worker_hashes_returns_without_waiting_for_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let tree_hashers = two_workers(&runtime)?;
        let inside_job = Arc::new(Barrier::new(2));
        let release_job = Arc::new(Barrier::new(2));
        let pending = {
            let inside_job = Arc::clone(&inside_job);
            let release_job = Arc::clone(&release_job);
            tree_hashers.spawn_job(move || {
                inside_job.wait();
                release_job.wait();
                TreeWorkOutput::default()
            })
        };
        inside_job.wait();
        // Construction stops with this block's work still running: the loader
        // drops its pending blocks and the pool without joining the worker.
        let stopped_at = Instant::now();
        drop(pending);
        drop(tree_hashers);
        assert!(stopped_at.elapsed() < Duration::from_secs(1));
        release_job.wait();
        Ok(())
    }

    #[test]
    fn a_loader_waiting_on_a_held_worker_stops_when_construction_is_cancelled()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let tree_hashers = two_workers(&runtime)?;
        let inside_job = Arc::new(Barrier::new(2));
        let release_job = Arc::new(Barrier::new(2));
        let oldest = {
            let inside_job = Arc::clone(&inside_job);
            let release_job = Arc::clone(&release_job);
            tree_hashers.spawn_job(move || {
                inside_job.wait();
                release_job.wait();
                TreeWorkOutput::default()
            })
        };
        inside_job.wait();
        let cancel = CancellationToken::new();
        let (waiting_sender, waiting) = std::sync::mpsc::channel();
        let loader = std::thread::spawn({
            let cancel = cancel.clone();
            move || {
                let _test_listening = waiting_sender.send(());
                oldest.wait(&cancel)
            }
        });
        waiting.recv()?;
        // The loader is in, or about to enter, its wait for the held job.
        std::thread::sleep(Duration::from_millis(50));
        let cancelled_at = Instant::now();
        cancel.cancel();
        let waited = loader.join().map_err(|_| "loader thread panicked")?;
        assert!(
            matches!(waited, Err(CanonicalConstructionError::Cancelled)),
            "a cancelled wait must report cancellation"
        );
        assert!(cancelled_at.elapsed() < Duration::from_secs(1));
        // The worker still holds the oldest job, and finishing it after the
        // loader stopped is harmless.
        release_job.wait();
        drop(tree_hashers);
        Ok(())
    }
}
