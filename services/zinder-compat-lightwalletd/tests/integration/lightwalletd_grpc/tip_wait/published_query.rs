//! A wallet-serving query whose publications the test drives.
//!
//! Production replaces the pair behind a `WalletServingPairSlot` through its
//! publisher, which needs a running writer status endpoint. This query
//! answers from one `WalletServingQuery` until the test publishes the next
//! one, and reports each publication as the slot does, so the wallet adapter
//! runs its real tip wait against it.

use std::{num::NonZeroU16, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::watch;
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockSelector, ChainEpoch, ChainEpochId, ChainValuePoolsAtTip,
    CompactBlockArtifact, NetworkUpgradeActivations, RawTransactionBytes, StandardFee,
    SubtreeRootRange, TransactionBroadcastOutcome, TransactionId, TransparentAddressBalance,
    TransparentAddressScriptHash, TransparentOutPoint,
};
use zinder_query::{
    BlockHeaderAtEpoch, BlockIdAtEpoch, ChainEvents, CompactBlock, CompactBlockFence,
    CompactBlockRange, CompactBlockStream, FullBlock, FullBlockStream,
    NativeWalletEndpointCapabilities, QueryError, RawTransaction, ServedTipSnapshot,
    SettledTipBlock, SubtreeRoots, Transaction, TransactionStatus, TransparentAddressTxIds,
    TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputs,
    TransparentAddressUnspentOutputsRequest, TransparentOutpointSpendStatuses, TreeState,
    UpstreamNodeCapabilities, VisibleTipBlock, WalletQueryApi, WalletServingQuery,
};
use zinder_store::{
    ChainEventStreamFamily, ChainEventStreamResume, EventStreamStartPosition, HeightOrder,
    StreamCursorTokenV1,
};

#[derive(Clone)]
pub(super) struct PublishedQuery {
    /// The first query, whose structural capabilities every later one shares.
    first: WalletServingQuery<()>,
    current: Arc<Mutex<WalletServingQuery<()>>>,
    publications: Arc<watch::Sender<()>>,
}

impl PublishedQuery {
    pub(super) fn new(first: WalletServingQuery<()>) -> Self {
        Self {
            current: Arc::new(Mutex::new(first.clone())),
            first,
            publications: Arc::new(watch::Sender::new(())),
        }
    }

    /// Serves `next` from now on and wakes every subscriber.
    pub(super) fn publish(&self, next: WalletServingQuery<()>) {
        *self.current.lock() = next;
        self.publications.send_replace(());
    }

    fn now(&self) -> WalletServingQuery<()> {
        self.current.lock().clone()
    }
}

#[async_trait]
impl WalletQueryApi for PublishedQuery {
    fn compact_block_fence(&self, epoch: ChainEpoch) -> Option<CompactBlockFence> {
        self.now().compact_block_fence(epoch)
    }

    fn subscribe_publications(&self) -> Option<watch::Receiver<()>> {
        Some(self.publications.subscribe())
    }

    fn native_endpoint_capabilities(&self) -> &NativeWalletEndpointCapabilities {
        self.first.native_endpoint_capabilities()
    }

    fn upstream_node_capabilities(&self) -> Option<UpstreamNodeCapabilities> {
        self.now().upstream_node_capabilities()
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError> {
        self.now().network_upgrade_activations().await
    }

    async fn chain_value_pools_at_tip(&self) -> Result<ChainValuePoolsAtTip, QueryError> {
        self.now().chain_value_pools_at_tip().await
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError> {
        self.now().visible_tip_block(at_epoch_id).await
    }

    async fn served_tip_snapshot(&self) -> Result<ServedTipSnapshot, QueryError> {
        self.now().served_tip_snapshot().await
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError> {
        self.now().settled_tip_block(at_epoch_id).await
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        self.now().block_id_by_selector(selector, at_epoch_id).await
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError> {
        self.now()
            .block_header_by_selector(selector, at_epoch_id)
            .await
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError> {
        self.now().compact_block_at(height, at_epoch_id).await
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        self.now()
            .compact_blocks_in_range(block_range, at_epoch_id)
            .await
    }

    async fn compact_block_stream(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockStream, QueryError> {
        self.now()
            .compact_block_stream(block_range, at_epoch_id)
            .await
    }

    async fn compact_blocks_in_range_up_to(
        &self,
        block_range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        self.now()
            .compact_blocks_in_range_up_to(block_range, order, max_bytes, at_epoch_id)
            .await
    }

    async fn compact_block_rows_with_lengths(
        &self,
        block_range: BlockHeightRange,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, QueryError> {
        self.now()
            .compact_block_rows_with_lengths(block_range, max_bytes, at_epoch_id)
            .await
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError> {
        self.now().full_block_at(height, at_epoch_id).await
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError> {
        self.now()
            .full_blocks_in_range(block_range, at_epoch_id)
            .await
    }

    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError> {
        self.now().transaction(transaction_id, at_epoch_id).await
    }

    async fn transaction_at_block_index(
        &self,
        height: BlockHeight,
        tx_index: u64,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError> {
        self.now()
            .transaction_at_block_index(height, tx_index, at_epoch_id)
            .await
    }

    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError> {
        self.now()
            .raw_transaction(transaction_id, at_epoch_id)
            .await
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        self.now()
            .transparent_address_unspent_outputs(request, at_epoch_id)
            .await
    }

    async fn transparent_address_unspent_outputs_on_demand(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        first_page_size: NonZeroU16,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        self.now()
            .transparent_address_unspent_outputs_on_demand(request, first_page_size, at_epoch_id)
            .await
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressTxIds, QueryError> {
        self.now()
            .transparent_address_tx_ids_in_range(request, at_epoch_id)
            .await
    }

    async fn transparent_outpoint_spend_status(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutpointSpendStatuses, QueryError> {
        self.now()
            .transparent_outpoint_spend_status(outpoints, at_epoch_id)
            .await
    }

    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError> {
        self.now()
            .transparent_address_balance(addresses, at_epoch_id)
            .await
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.now().tree_state_at(height, at_epoch_id).await
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        self.now().latest_tree_state_checkpoint(at_epoch_id).await
    }

    async fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError> {
        self.now()
            .subtree_roots(subtree_root_range, at_epoch_id)
            .await
    }

    async fn chain_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEvents, QueryError> {
        self.now().chain_events(from_cursor, family).await
    }

    async fn resolve_chain_events_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, QueryError> {
        self.now()
            .resolve_chain_events_start(start, requested_family)
            .await
    }

    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError> {
        self.now().broadcast_transaction(raw_transaction).await
    }

    async fn standard_fee(&self) -> Result<StandardFee, QueryError> {
        self.now().standard_fee().await
    }
}
