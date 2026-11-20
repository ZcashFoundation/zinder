//! A canonical reader for tests that delegates to the `RocksDB` reader,
//! counts the storage reads made through it, and can hold its tree-state
//! replays until the test releases them.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use tokio::sync::{Notify, Semaphore};
use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange,
    CanonicalHistoryBounds, ChainEpoch, CompactBlockArtifact, Network, SubtreeRootArtifact,
    SubtreeRootRange, TransactionBlobArtifact, TransactionId,
};
use zinder_store::{
    BlockHashLookup, CanonicalActivationSchedule, CanonicalEventFence, ChainEventEnvelope,
    ChainEventStreamFamily, ChainEventStreamResume, EventStreamStartPosition, HeightOrder,
    RawBlobRetention, ServedTreeState, StreamCursorTokenV1, TreeStateLookup, TreeStateReplay,
    TreeStateReplayOutcome,
};

use crate::{CanonicalReader, IndexedTransaction, RocksDbCanonicalReader, StorageReadError};

/// A [`CanonicalReader`] that counts its storage reads and can hold replays.
pub(crate) struct ObservedCanonicalReader {
    inner: RocksDbCanonicalReader,
    reads: AtomicUsize,
    replays: Arc<ReplayGate>,
}

/// Holds every tree-state replay of an [`ObservedCanonicalReader`] until
/// [`ReplayGate::release`].
pub(crate) struct ReplayGate {
    open: Semaphore,
    started: Notify,
}

impl ReplayGate {
    /// Waits until a replay has started and is held.
    pub(crate) async fn replay_started(&self) {
        self.started.notified().await;
    }

    /// Lets every held and later replay run.
    pub(crate) fn release(&self) {
        self.open.close();
    }
}

impl ObservedCanonicalReader {
    /// Observes `inner`, letting its replays run.
    pub(crate) fn new(inner: RocksDbCanonicalReader) -> Self {
        let replays = ReplayGate {
            open: Semaphore::new(0),
            started: Notify::new(),
        };
        replays.release();
        Self {
            inner,
            reads: AtomicUsize::new(0),
            replays: Arc::new(replays),
        }
    }

    /// Observes `inner`, holding every replay until the returned gate
    /// releases it.
    pub(crate) fn holding_replays(inner: RocksDbCanonicalReader) -> (Self, Arc<ReplayGate>) {
        let replays = Arc::new(ReplayGate {
            open: Semaphore::new(0),
            started: Notify::new(),
        });
        (
            Self {
                inner,
                reads: AtomicUsize::new(0),
                replays: Arc::clone(&replays),
            },
            replays,
        )
    }

    /// Returns how many storage reads were made through this reader.
    pub(crate) fn reads(&self) -> usize {
        self.reads.load(Ordering::Acquire)
    }

    fn count(&self) -> &RocksDbCanonicalReader {
        self.reads.fetch_add(1, Ordering::AcqRel);
        &self.inner
    }
}

#[async_trait]
impl CanonicalReader for ObservedCanonicalReader {
    fn network(&self) -> Network {
        self.inner.network()
    }

    fn network_upgrade_schedule(&self) -> Option<&CanonicalActivationSchedule> {
        self.inner.network_upgrade_schedule()
    }

    fn raw_blob_retention(&self) -> RawBlobRetention {
        self.inner.raw_blob_retention()
    }

    fn event_fence(&self) -> CanonicalEventFence {
        self.inner.event_fence()
    }

    fn chain_epoch(&self) -> ChainEpoch {
        self.inner.chain_epoch()
    }

    fn history_bounds(&self) -> CanonicalHistoryBounds {
        self.inner.history_bounds()
    }

    async fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        self.count().block_header_at(height).await
    }

    async fn block_hash_lookup(
        &self,
        block_hash: BlockHash,
    ) -> Result<BlockHashLookup, StorageReadError> {
        self.count().block_hash_lookup(block_hash).await
    }

    async fn block_header_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        self.count().block_header_by_hash(block_hash).await
    }

    async fn compact_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        self.count().compact_block_by_hash(block_hash).await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        self.count().compact_block_at(height).await
    }

    async fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        self.count().compact_blocks_in_range(range).await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        self.count()
            .compact_blocks_in_range_up_to(range, order, max_bytes)
            .await
    }

    async fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, StorageReadError> {
        self.count().block_blob_at(height).await
    }

    async fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, StorageReadError> {
        self.count().block_blobs_in_range(range).await
    }

    async fn mined_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<IndexedTransaction>, StorageReadError> {
        self.count().mined_transaction(transaction_id).await
    }

    async fn transaction_blob_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<TransactionBlobArtifact, StorageReadError> {
        self.count().transaction_blob_by_id(transaction_id).await
    }

    async fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ServedTreeState>, StorageReadError> {
        self.count()
            .tree_state_checkpoint_at_or_before(height)
            .await
    }

    async fn tree_state_lookup(
        &self,
        height: BlockHeight,
    ) -> Result<Option<TreeStateLookup>, StorageReadError> {
        self.count().tree_state_lookup(height).await
    }

    async fn replay_tree_state(
        &self,
        replay: TreeStateReplay,
    ) -> Result<Option<TreeStateReplayOutcome>, StorageReadError> {
        self.replays.started.notify_one();
        // A released gate is closed, so it refuses the permit at once.
        let _released = self.replays.open.acquire().await;
        self.count().replay_tree_state(replay).await
    }

    async fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, StorageReadError> {
        self.count().subtree_roots(range).await
    }

    async fn wallet_chain_event_history(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Result<Vec<ChainEventEnvelope>, StorageReadError> {
        self.count()
            .wallet_chain_event_history(from_cursor, family, max_events)
            .await
    }

    async fn resolve_wallet_chain_event_stream_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StorageReadError> {
        self.count()
            .resolve_wallet_chain_event_stream_start(start, requested_family)
            .await
    }
}
