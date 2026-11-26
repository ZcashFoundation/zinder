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
use zinder_query::{CanonicalReader, IndexedTransaction, RocksDbCanonicalReader, StorageReadError};
use zinder_store::{
    BlockHashLookup, CanonicalActivationSchedule, CanonicalEventFence, ChainEventEnvelope,
    ChainEventStreamFamily, ChainEventStreamResume, EventStreamStartPosition, HeightOrder,
    RawBlobRetention, ServedTreeState, StreamCursorTokenV1, TreeStateLookup, TreeStateReplay,
    TreeStateReplayOutcome,
};

pub(super) struct BatchGate {
    hold_call: usize,
    calls: AtomicUsize,
    started: Notify,
    open: Semaphore,
}

impl BatchGate {
    pub(super) fn holding_call(hold_call: usize) -> Arc<Self> {
        Arc::new(Self {
            hold_call,
            calls: AtomicUsize::new(0),
            started: Notify::new(),
            open: Semaphore::new(0),
        })
    }

    pub(super) fn started(&self) -> tokio::sync::futures::Notified<'_> {
        self.started.notified()
    }

    pub(super) fn release(&self) {
        self.open.close();
    }
}

pub(super) struct GatedBatchCanonicalReader {
    inner: RocksDbCanonicalReader,
    gate: Arc<BatchGate>,
}

impl GatedBatchCanonicalReader {
    pub(super) fn new(inner: RocksDbCanonicalReader, gate: Arc<BatchGate>) -> Self {
        Self { inner, gate }
    }
}

#[async_trait]
impl CanonicalReader for GatedBatchCanonicalReader {
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

    fn history_predecessor_digest(&self) -> Option<[u8; 32]> {
        self.inner.history_predecessor_digest()
    }

    async fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        self.inner.block_header_at(height).await
    }

    async fn block_hash_lookup(
        &self,
        block_hash: BlockHash,
    ) -> Result<BlockHashLookup, StorageReadError> {
        self.inner.block_hash_lookup(block_hash).await
    }

    async fn block_header_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError> {
        self.inner.block_header_by_hash(block_hash).await
    }

    async fn compact_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        self.inner.compact_block_by_hash(block_hash).await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError> {
        self.inner.compact_block_at(height).await
    }

    async fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        self.inner.compact_blocks_in_range(range).await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError> {
        self.inner
            .compact_blocks_in_range_up_to(range, order, max_bytes)
            .await
    }

    async fn compact_block_rows_with_lengths(
        &self,
        range: BlockHeightRange,
        max_bytes: usize,
    ) -> Result<Vec<(CompactBlockArtifact, usize)>, StorageReadError> {
        self.inner
            .compact_block_rows_with_lengths(range, max_bytes)
            .await
    }

    async fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, StorageReadError> {
        self.inner.block_blob_at(height).await
    }

    async fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, StorageReadError> {
        self.inner.block_blobs_in_range(range).await
    }

    async fn mined_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<IndexedTransaction>, StorageReadError> {
        self.inner.mined_transaction(transaction_id).await
    }

    async fn mined_transactions(
        &self,
        transaction_ids: &[TransactionId],
    ) -> Result<Vec<Option<IndexedTransaction>>, StorageReadError> {
        let call_index = self.gate.calls.fetch_add(1, Ordering::SeqCst);
        if call_index == self.gate.hold_call {
            self.gate.started.notify_waiters();
            let _ = self.gate.open.acquire().await;
        }
        self.inner.mined_transactions(transaction_ids).await
    }

    async fn transaction_blob_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<TransactionBlobArtifact, StorageReadError> {
        self.inner.transaction_blob_by_id(transaction_id).await
    }

    async fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ServedTreeState>, StorageReadError> {
        self.inner.tree_state_checkpoint_at_or_before(height).await
    }

    async fn stored_tree_state_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ServedTreeState>, StorageReadError> {
        self.inner.stored_tree_state_at(height).await
    }

    async fn tree_state_lookup(
        &self,
        height: BlockHeight,
    ) -> Result<Option<TreeStateLookup>, StorageReadError> {
        self.inner.tree_state_lookup(height).await
    }

    async fn replay_tree_state(
        &self,
        replay: TreeStateReplay,
    ) -> Result<Option<TreeStateReplayOutcome>, StorageReadError> {
        self.inner.replay_tree_state(replay).await
    }

    async fn replay_tree_state_until_cancelled(
        &self,
        replay: TreeStateReplay,
        cancelled: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<Option<TreeStateReplayOutcome>, StorageReadError> {
        self.inner
            .replay_tree_state_until_cancelled(replay, cancelled)
            .await
    }

    async fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, StorageReadError> {
        self.inner.subtree_roots(range).await
    }

    async fn wallet_chain_event_history(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Result<Vec<ChainEventEnvelope>, StorageReadError> {
        self.inner
            .wallet_chain_event_history(from_cursor, family, max_events)
            .await
    }

    async fn resolve_wallet_chain_event_stream_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StorageReadError> {
        self.inner
            .resolve_wallet_chain_event_stream_start(start, requested_family)
            .await
    }
}
