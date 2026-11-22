//! Commitment-tree hashing threads for the ordered canonical loader.
//!
//! The loader still appends every block to the trees and positions it in
//! height order. With more than one worker, each append hashes on the workers,
//! and the block's checkpoint and final roots are derived on them from a
//! snapshot of the trees while the loader appends and writes the blocks around
//! it. A block is handed to the store only after its own tree work finishes,
//! so blocks reach the store in height order with the trees and checkpoints a
//! serial loader produces.

use std::{
    collections::VecDeque,
    num::NonZeroU32,
    sync::mpsc::{Receiver, sync_channel},
};

use tokio::sync::OwnedSemaphorePermit;
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
    /// Appended blocks in height order. An entry that failed to append ends
    /// the queue, so the blocks before it still reach the store first.
    pending: VecDeque<Result<PendingBuildBlock, CanonicalConstructionError>>,
    max_pending: usize,
}

impl TreeHashers {
    /// Starts `workers` hashing threads, or returns `None` for one worker,
    /// which hashes on the loader thread.
    pub(super) fn start(workers: NonZeroU32) -> Result<Option<Self>, CanonicalConstructionError> {
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
        let (sender, receiver) = sync_channel(1);
        self.workers.spawn(move || {
            let checkpoint = work.checkpoint_required.then(|| {
                snapshot.cross_checked_frontiers().map(|frontiers| {
                    CommitmentTreeCheckpoint::new(block, block_time_seconds, frontiers)
                })
            });
            let final_roots = work
                .final_roots_required
                .then(|| final_roots(&snapshot, block.hash))
                .flatten();
            // The loader drops the receiver only when construction stops.
            let _loader_stopped = sender.send(TreeWorkOutput {
                checkpoint,
                final_roots,
            });
        });
        PendingTreeWork { receiver }
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
    receiver: Receiver<TreeWorkOutput>,
}

/// A block's checkpoint and final roots, derived on a worker.
#[derive(Default)]
pub(super) struct TreeWorkOutput {
    pub(super) checkpoint: Option<Result<CommitmentTreeCheckpoint, CommitmentTreeAccumulatorError>>,
    pub(super) final_roots: Option<BlockFinalNoteCommitmentRoots>,
}

impl PendingTreeWork {
    /// Waits for the worker deriving this block's tree work.
    pub(super) fn wait(self) -> Result<TreeWorkOutput, CanonicalConstructionError> {
        self.receiver
            .recv()
            .map_err(|_| CanonicalConstructionError::LoaderTaskFailed {
                reason: "a commitment-tree hashing worker stopped before finishing a block"
                    .to_owned(),
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
