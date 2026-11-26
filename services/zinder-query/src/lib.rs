//! Wallet and application query boundary for Zinder.
//!
//! This crate serves indexed artifacts from an admitted wallet-serving pair
//! through [`WalletServingQuery`] without mutating canonical storage or using
//! upstream nodes as a fallback for indexed history. Only transaction
//! broadcast and the recommended-fee read contact an upstream node.

use std::{
    num::{NonZeroU16, NonZeroU32},
    time::Instant,
};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;
use zinder_core::{
    BlockBlobArtifact, BlockHeader, BlockHeight, BlockHeightRange, BlockId, BlockSelector,
    ChainEpoch, ChainEpochId, ChainValuePoolsAtTip, CompactBlockArtifact,
    MAX_SUBTREE_ROOTS_PER_REQUEST, NetworkUpgradeActivations, RawTransactionBytes,
    ShieldedProtocol, StandardFee, SubtreeRootArtifact, SubtreeRootIndex, SubtreeRootRange,
    TransactionBlobArtifact, TransactionBroadcastOutcome, TransactionId, TransactionLocation,
    TransparentAddressBalance, TransparentAddressScriptHash, TransparentAddressTxIndexArtifact,
    TransparentOutPoint, TransparentOutpointSpendStatus, TxStatus,
};
use zinder_proto::capabilities::WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1;
use zinder_source::SourceError;
use zinder_store::{
    ArtifactFamily, ChainEventEnvelope, ChainEventStreamFamily, ChainEventStreamResume,
    DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS, EventStreamStartPosition, HeightOrder, StoreError,
    StreamCursorTokenV1,
};

mod broadcast_byte_budget;
mod cold_transaction;
mod compact_block_fence;
mod forwarded_read;
mod grpc;
mod ingest_control;
mod light_wallet;
mod native_wallet_endpoint_capabilities;
mod node_broadcast;
#[cfg(test)]
mod observed_canonical_reader;
mod ordered_transactions;
pub use light_wallet::{LightWalletNodeQuery, VerifiedNodeTarget};
mod rocksdb_serving_readers;
mod serving_read;
mod serving_readers;
mod transparent_unspent_output_stream;
mod tree_state_replay_limit;
mod wallet_serving_pair;
mod wallet_serving_pair_publisher;
mod wallet_serving_query;

pub use broadcast_byte_budget::BROADCAST_BYTE_BUDGET;
pub use compact_block_fence::{CompactBlockFence, CompactBlockSource};
pub use forwarded_read::{
    DEFAULT_FORWARDED_READ_MAX_CONCURRENT, DEFAULT_FORWARDED_READ_WAIT,
    DEFAULT_FORWARDED_READ_WAITERS_PER_SLOT, DEFAULT_FORWARDED_TRANSACTION_CACHE_BYTES,
    FORWARDED_HISTORY_DEADLINE, ForwardedReadConfig, ForwardedReadConfigError, ForwardedReadKind,
    ForwardedReadLimit, ForwardedReadRefused, ForwardedResponseKind, ForwardedResponsePermit,
    ForwardedTransactionError, MAX_FORWARDED_READ_MAX_CONCURRENT, MAX_FORWARDED_READ_WAIT,
    MAX_FORWARDED_READ_WAITERS_PER_SLOT, MAX_FORWARDED_TRANSACTION_CACHE_BYTES,
};
pub use grpc::{
    DEFAULT_MAX_CONCURRENT_COMPACT_BLOCK_STREAMS, TransactionStatusLookup,
    WALLET_QUERY_FIELD_LIMITS, WalletEndpointMetadata, WalletQueryGrpcAdapter,
    address_lookup_to_script_hash, block_header_by_selector_response,
    block_id_by_selector_response, broadcast_transaction_response,
    build_transparent_address_tx_ids_chunk, build_transparent_address_tx_ids_header,
    build_transparent_unspent_output_message, build_transparent_unspent_outputs_header,
    build_wallet_server_info, chain_events_response, chain_value_pools_at_tip_response,
    compact_block_response, compact_blocks_in_range_header, compact_blocks_in_range_item,
    full_block_response, latest_tree_state_checkpoint_response,
    network_upgrade_activations_response, parse_receivable_transparent_address,
    status_from_query_error, subtree_roots_response, transaction_response,
    transparent_address_tx_ids_response, transparent_address_unspent_outputs_response,
    transparent_outpoint_spend_status_response, tree_state_at_response, visible_tip_block_response,
};
pub use ingest_control::{
    AdmittedIngestControl, IngestControlAdmissionError, WALLET_INGEST_CONTROL_REQUEST_TIMEOUT,
    wallet_ingest_control_request, wallet_mempool_events_request,
};
pub use native_wallet_endpoint_capabilities::{
    NativeWalletEndpointCapabilities, UpstreamNodeCapabilities,
};
pub use node_broadcast::OrderedBroadcaster;
pub use ordered_transactions::{
    CanonicalRefill, CompletedCanonicalRefill, PendingTransaction, TransactionsInOrder,
};
pub use rocksdb_serving_readers::MinedTransactionDigest;
pub use rocksdb_serving_readers::{RocksDbCanonicalReader, RocksDbWalletReader};
pub use serving_readers::{
    CanonicalReader, IndexedTransaction, SpendStatusReadEnd, StorageReadError,
    WalletProjectionReader, read_on_blocking_thread,
};
pub use transparent_unspent_output_stream::{
    TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE, TransparentAddressUnspentOutputs,
};
pub use tree_state_replay_limit::{
    DEFAULT_TREE_STATE_REPLAY_WAIT, DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
    MAX_TREE_STATE_REPLAY_WAIT, MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT, ServedTreeStateHex,
};
pub use wallet_serving_pair::{WalletServingAdmissionError, WalletServingReadPair};
pub use wallet_serving_pair_publisher::{
    WalletServingConvergence, WalletServingPairConfig, WalletServingPairError,
    WalletServingPairPublisher, WalletServingPairSlot, WalletServingPairTeardownError,
    WalletServingReadiness, spawn_wallet_ingest_control_readiness_probe,
    spawn_wallet_node_readiness_probe,
};
pub use wallet_serving_query::WalletServingQuery;
/// Wallet-facing read API backed by epoch-bound canonical reads.
///
/// Canonical reads take `at_epoch_id: Option<ChainEpochId>`. `None` resolves to
/// the visible chain epoch at call time; `Some(id)` pins the read to that epoch.
/// Current materialized-view reads expose their chain epoch in the response
/// instead of accepting a pin.
#[async_trait]
pub trait WalletQueryApi: Send + Sync + 'static {
    /// Returns the canonical fence, retention bounds, and source of a locally
    /// admitted epoch. Adapters can use this identity to reuse derived
    /// compact-block bytes.
    fn compact_block_fence(&self, _epoch: ChainEpoch) -> Option<CompactBlockFence> {
        None
    }

    /// Subscribes to replacements of the serving data this query reads, so a
    /// caller can wait for a newer visible tip without polling.
    ///
    /// The receiver reports a change for every replacement made after this
    /// call. `None` means the query publishes no replacements, and a caller
    /// must not wait for one.
    fn subscribe_publications(&self) -> Option<tokio::sync::watch::Receiver<()>> {
        None
    }

    /// Returns the visible tip height of the serving data current now,
    /// without keeping that data readable for epoch pins.
    ///
    /// A caller that only compares heights, such as one waiting for a newer
    /// tip, reads it this way so its checks never retain a replaced
    /// generation. `None` means the query has no such read, and the caller
    /// reads the tip through [`Self::visible_tip_block`].
    fn current_visible_tip_height(&self) -> Option<BlockHeight> {
        None
    }

    /// Returns the chain epoch of the serving data current now, without
    /// keeping that data readable for epoch pins.
    ///
    /// A probe that exposes no epoch, such as a tip or staleness check, reads
    /// it this way. The caller must not pin a read to the returned epoch.
    /// `None` means the query has no such read, and the caller reads the
    /// epoch through [`Self::visible_tip_block`].
    fn current_chain_epoch(&self) -> Option<ChainEpoch> {
        None
    }

    /// Returns the immutable structural capability set derived when this query
    /// was composed and admitted.
    fn native_endpoint_capabilities(&self) -> &NativeWalletEndpointCapabilities;

    /// Returns the diagnostic capability snapshot of the first verified node
    /// endpoint, in broadcast preference order, that this query broadcasts
    /// through, or `None` while no endpoint has passed first-contact
    /// verification.
    fn upstream_node_capabilities(&self) -> Option<UpstreamNodeCapabilities>;

    /// Returns the network-upgrade activation table the canonical writer
    /// admitted with the current serving pair.
    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError>;

    /// Reads chain-wide value-pool totals from the exact admitted upstream
    /// source and binds them to the query's current chain epoch.
    async fn chain_value_pools_at_tip(&self) -> Result<ChainValuePoolsAtTip, QueryError> {
        Err(QueryError::EndpointCapabilityUnavailable {
            capability: WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
        })
    }

    /// Reads the visible-tip block identity.
    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError>;

    /// Reads the visible tip and its block time for one single-shot answer,
    /// such as a latest-block or server-info response.
    ///
    /// The caller must not read again by the returned epoch. A query that
    /// serves from a pair slot answers from one pair without keeping that
    /// pair readable for pins, so frequent tip calls never hold a serving
    /// generation. The default composes the pinned reads.
    async fn served_tip_snapshot(&self) -> Result<ServedTipSnapshot, QueryError> {
        let tip = self.visible_tip_block(None).await?;
        let header = self
            .block_header_by_selector(BlockSelector::Height(tip.height), Some(tip.chain_epoch.id))
            .await?;
        Ok(ServedTipSnapshot {
            chain_epoch: tip.chain_epoch,
            block_time_unix_seconds: header.block_header.block_time,
        })
    }

    /// Reads the block at the chain epoch's settled finality watermark.
    /// Mirrors `visible_tip_block` but resolves to
    /// `chain_epoch.settled_tip_height` rather than `chain_epoch.visible_tip_height`.
    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError>;

    /// Resolves a typed block selector against the canonical best chain.
    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError>;

    /// Reads the typed block-header read model at a typed block selector.
    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError>;

    /// Reads one compact block artifact at a given height.
    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError>;

    /// Reads compact block artifacts for an inclusive height range.
    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError>;

    /// Streams compact block artifacts for an inclusive height range under
    /// one chain epoch.
    ///
    /// Validation and errors are those of [`Self::compact_blocks_in_range`],
    /// and they surface as the returned error, before any block is
    /// delivered. Blocks arrive ascending and contiguous; a store failure
    /// after the first block ends the stream with one terminal `Err`.
    ///
    /// The default reads the whole range with
    /// [`Self::compact_blocks_in_range`] and queues every block.
    async fn compact_block_stream(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockStream, QueryError> {
        let compact_block_range = self
            .compact_blocks_in_range(block_range, at_epoch_id)
            .await?;
        let (blocks, receiver) = mpsc::channel(compact_block_range.compact_blocks.len().max(1));
        for compact_block in compact_block_range.compact_blocks {
            if blocks.try_send(Ok(compact_block)).is_err() {
                break;
            }
        }
        Ok(CompactBlockStream {
            chain_epoch: compact_block_range.chain_epoch,
            blocks: receiver,
        })
    }

    /// Reads compact block artifacts from one end of an inclusive height
    /// range, up to a byte budget.
    ///
    /// The read starts at the end of `block_range` that `order` visits first
    /// and stops before the block that would take the stored bytes of the
    /// blocks it keeps past `max_bytes`. It always keeps one block, so a
    /// block larger than `max_bytes` is returned alone. The response's
    /// `block_range` is the heights kept, and its blocks are in ascending
    /// height order.
    ///
    /// Validation and errors are those of
    /// [`Self::compact_blocks_in_range`] for the whole of `block_range`: a
    /// range that reaches outside retained history fails even where the
    /// budget would stop the read first.
    async fn compact_blocks_in_range_up_to(
        &self,
        block_range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError>;

    /// Reads ascending compact blocks with their original stored byte lengths
    /// up to `max_bytes`.
    ///
    /// Local canonical query implementations override this to supply exact
    /// stored byte lengths for live-tail caching; implementations without
    /// stored-length metadata may keep the default `Ok(None)`.
    async fn compact_block_rows_with_lengths(
        &self,
        _block_range: BlockHeightRange,
        _max_bytes: usize,
        _at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Option<Vec<(CompactBlockArtifact, usize)>>, QueryError> {
        Ok(None)
    }

    /// Reads one full serialized block at a given height.
    ///
    /// Served from the stored block blob, present only when the writer
    /// deployment retains block blobs (`raw_blob_policy = "all"`). Heights with
    /// no retained blob return [`QueryError::ArtifactUnavailable`].
    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError>;

    /// Streams full serialized blocks for an inclusive height range under one
    /// pinned chain epoch.
    ///
    /// Served from stored block blobs. The whole stream reads one epoch,
    /// resolved before this call returns, so an epoch-pin failure surfaces as
    /// the returned error rather than mid-stream. Blocks arrive ascending and
    /// contiguous; the first height with no retained blob terminates the
    /// stream with [`QueryError::ArtifactUnavailable`] after the blocks
    /// already delivered.
    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError>;

    /// Reads typed transaction status by transaction id.
    ///
    /// Returns [`TxStatus::Mined`] for mined transactions, with epoch-bound
    /// [`zinder_core::MinedTransactionChainContext`] enrichment, and
    /// [`TxStatus::NotFound`] only for a proven absence from the canonical
    /// chain of the returned epoch. A store whose history starts after a
    /// checkpoint cannot prove an absence and returns
    /// [`QueryError::CanonicalHistoryUnavailable`] on a miss; storage faults,
    /// a missing epoch, and a pin this store cannot serve are other
    /// [`QueryError`] values. This method never consults live mempool state;
    /// mempool fall-through is the caller's responsibility on this trait.
    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError>;

    /// Opens an ordered reader for `transaction_ids` at `at_epoch_id`.
    ///
    /// Canonical locations, block headers, and retained bodies are read in
    /// bounded multi-get batches, and uncached cold bodies are fetched as
    /// independent [`PendingTransaction`] futures charged to
    /// [`ForwardedReadKind::History`].
    ///
    /// The default implementation resolves the current visible epoch once when
    /// `at_epoch_id` is `None` and starts [`Self::transaction`] futures pinned
    /// to that single epoch on demand as the caller polls
    /// [`TransactionsInOrder`], never materializing more transaction bodies than
    /// the caller's batch capacity. An implementation that wraps another should
    /// forward this method to keep batched canonical reads active.
    async fn transactions_in_order(
        &self,
        transaction_ids: Vec<TransactionId>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionsInOrder, QueryError>
    where
        Self: Sized + Clone,
    {
        let resolved_epoch_id = match at_epoch_id {
            Some(epoch_id) => epoch_id,
            None => self.visible_tip_block(None).await?.chain_epoch.id,
        };
        Ok(TransactionsInOrder::from_lazy_query(
            self.clone(),
            transaction_ids,
            resolved_epoch_id,
        ))
    }

    /// Verifies the node that supplies this query's transaction bytes against
    /// the served tip of `at_epoch_id`, or of the current epoch when `None`.
    ///
    /// A caller consults the live mempool after a canonical miss only once
    /// this succeeds, so a lookup never reports a pending answer or an
    /// absence while the forwarding node is unreachable, behind the served
    /// tip, or on a different chain from the served tip. A node that holds
    /// the served tip on its best chain passes even when it has mined further.
    /// A query whose store retains transaction bodies, or that
    /// has no forwarding node, has nothing to verify and succeeds.
    async fn verify_forwarding_node(
        &self,
        _at_epoch_id: Option<ChainEpochId>,
    ) -> Result<(), QueryError> {
        Ok(())
    }

    /// Reads the indexed transaction at `(height, tx_index)`.
    ///
    /// `tx_index` is the transaction's position within the full block, as
    /// produced by ingestion. The lookup decodes the indexed compact block
    /// at `height` and matches on the per-transaction index recorded there.
    async fn transaction_at_block_index(
        &self,
        height: BlockHeight,
        tx_index: u64,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError>;

    /// Reads an optional raw transaction blob by transaction id.
    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError>;

    /// Streams the complete unspent transparent output set for one
    /// transparent address script at a single pinned chain epoch.
    ///
    /// The epoch is resolved before this call returns, so an epoch-pin
    /// failure surfaces as the returned error. Outputs are then read in pages
    /// of at most [`TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE`] as the caller
    /// consumes them.
    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError>;

    /// Streams the outputs [`Self::transparent_address_unspent_outputs`]
    /// streams, in the same order and at the same pinned epoch, reading
    /// storage only on demand.
    ///
    /// The stream reads nothing until it is first polled and never reads
    /// ahead: it reads a page only when the caller polls it with no output
    /// left. The first page holds `first_page_size` outputs, at most
    /// [`TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE`], and each later page doubles
    /// up to that size as the caller consumes them, so a caller that merges
    /// many of these streams holds at most one page per stream. Use it when
    /// one request opens many streams at once, and size the first page from
    /// how many streams the request holds.
    ///
    /// The default opens the eager stream, so an implementation that wraps
    /// another must forward this method to keep its reads on demand.
    async fn transparent_address_unspent_outputs_on_demand(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        first_page_size: NonZeroU16,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        let _ = first_page_size;
        self.transparent_address_unspent_outputs(request, at_epoch_id)
            .await
    }

    /// Reads a bounded page of transparent-address tx-history index
    /// artifacts.
    ///
    /// A continuation resumes at the epoch named by its cursor. A non-`None`
    /// `at_epoch_id` must name that same epoch.
    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressTxIds, QueryError>;

    /// Classifies each transparent outpoint as unspent, spent by a mined
    /// input, or not found, reading every outpoint at one chain epoch.
    ///
    /// The returned statuses follow `outpoints` in order and length,
    /// including duplicates. The read covers mined transactions only.
    ///
    /// Rejects an empty list and any list above
    /// [`zinder_core::MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST`] with
    /// [`QueryError::TransparentOutpointCountOutOfRange`] before reading
    /// storage. `NotFound` is a proven absence from height 1 through the
    /// visible tip; a store whose history starts after a checkpoint answers a
    /// miss with [`QueryError::CanonicalHistoryUnavailable`] instead.
    async fn transparent_outpoint_spend_status(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutpointSpendStatuses, QueryError>;

    /// Sums the canonical confirmed balance across one or more transparent
    /// addresses at a single pinned chain epoch.
    ///
    /// Folds the complete unspent transparent output set of every requested
    /// address into one saturating `confirmed_zat` total. The lightwalletd
    /// compatibility adapter is the only consumer; the native endpoint does
    /// not serve a balance.
    ///
    /// Rejects an empty address list and any list above
    /// [`MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES`] with
    /// [`QueryError::TransparentBalanceAddressCountExceeded`] so one request
    /// cannot fan out into an unbounded number of unspent-set reads.
    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError>;

    /// Reads the tree state at exactly `height`.
    ///
    /// Served from a stored checkpoint when one exists at `height`, otherwise
    /// rebuilt from the nearest stored checkpoint below it and the stored
    /// compact blocks, without contacting the node. The returned height always
    /// equals `height`.
    ///
    /// A height above the visible tip or below retained history fails with
    /// [`QueryError::BlockNotInBestChain`]. A rebuild requested while the
    /// query already runs its limit of them waits a bounded time for a slot
    /// and then fails with [`QueryError::TreeStateReplayLimitReached`].
    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError>;

    /// Reads the tree state at exactly `height`, pre-encoded in lowercase
    /// hexadecimal for lightwalletd `GetTreeState` serving.
    ///
    /// Validation and errors match [`Self::tree_state_at`]. Because the
    /// returned value exposes no chain epoch, an unpinned (`None`) read need
    /// not retain its serving generation for future epoch pins. The default
    /// encodes [`Self::tree_state_at`].
    async fn tree_state_hex_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<ServedTreeStateHex, QueryError> {
        let tree_state = self.tree_state_at(height, at_epoch_id).await?;
        Ok(ServedTreeStateHex::from_tree_state(&tree_state))
    }

    /// Reads the latest tree-state checkpoint at the visible chain epoch tip.
    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError>;

    /// Reads the latest tree-state checkpoint at the visible chain epoch tip,
    /// pre-encoded in lowercase hexadecimal for lightwalletd
    /// `GetLatestTreeState` serving.
    ///
    /// Validation and errors match [`Self::latest_tree_state_checkpoint`].
    /// Because the returned value exposes no chain epoch, an unpinned (`None`)
    /// read need not retain its serving generation for future epoch pins. The
    /// default encodes [`Self::latest_tree_state_checkpoint`].
    async fn latest_tree_state_checkpoint_hex(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<ServedTreeStateHex, QueryError> {
        let tree_state = self.latest_tree_state_checkpoint(at_epoch_id).await?;
        Ok(ServedTreeStateHex::from_tree_state(&tree_state))
    }

    /// Reads subtree-root artifacts for a bounded subtree range.
    ///
    /// Rejects ranges above [`MAX_SUBTREE_ROOTS_PER_REQUEST`] with
    /// [`QueryError::SubtreeRootRangeTooLarge`] before reading storage.
    async fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError>;

    /// Reads a bounded page of replayable chain events.
    async fn chain_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEvents, QueryError>;

    /// Resolves an event-stream start position for the chain-event family
    /// once at subscribe time.
    async fn resolve_chain_events_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, QueryError>;

    /// Broadcasts a raw transaction without mutating canonical storage.
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError>;

    /// Reads the fee the verified broadcast node recommends for a
    /// transaction to be mined in the next block.
    ///
    /// Fails with [`QueryError::Node`] holding
    /// [`SourceError::NodeCapabilityMissing`] when no verified node
    /// implements `getstandardfee`,
    /// [`SourceError::TransactionBroadcastDisabled`] when no node is
    /// configured, and [`SourceError::SourceProtocolMismatch`] when the node
    /// answers with a zero fee or a reply of another shape.
    async fn standard_fee(&self) -> Result<StandardFee, QueryError>;

    /// Returns the retained canonical history bounds of the current serving
    /// view without observing or pinning it, when available synchronously.
    fn canonical_history_bounds(&self) -> Option<zinder_core::CanonicalHistoryBounds> {
        None
    }
}

/// Default maximum compact-block count returned by one range call.
pub const DEFAULT_MAX_COMPACT_BLOCK_RANGE: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);

/// Stored compact-block bytes one read of a compact-block stream keeps.
///
/// Each read stops before the block that would take its stored bytes past
/// this budget, and always keeps one block, so a stream holds about one
/// budget of blocks plus its channel no matter how wide its range is.
pub const COMPACT_BLOCK_STREAM_SUB_READ_BYTES: usize = 2 * 1024 * 1024;

/// Bounded depth, in blocks, of the compact-block stream channel.
pub const COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY: usize = 16;

/// Default maximum full-block count returned by one range call.
///
/// Sized to one wallet initial-scan batch so a 1000-block window is a single
/// stream. The range is served demand-driven, so this bounds request width,
/// not memory.
pub const DEFAULT_MAX_FULL_BLOCK_RANGE: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);

/// Blocks read per store multi-get while streaming a full-block range.
///
/// Caps the producer's working set to one sub-read regardless of window width.
pub(crate) const FULL_BLOCK_STREAM_SUB_READ_BLOCKS: u32 = 16;

/// Bounded depth of the full-block range channel.
///
/// In-flight blocks a slow consumer can stall behind stay near this many blobs
/// plus one sub-read, so per-stream memory never scales with the window.
pub const FULL_BLOCK_STREAM_CHANNEL_CAPACITY: usize = 4;

/// Hard cap on the number of addresses one balance request may sum across.
///
/// Each address fans out into one complete unspent-output read (and, in the
/// gRPC adapter, one mempool point lookup), so an unbounded list would let one
/// request issue thousands of reads. `u32` matches the `address_count` field
/// on the response so the bound check happens in the wire type's native width.
pub const MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES: u32 = 256;

/// Metric pair the `WalletQuery` adapter emits per request.
const QUERY_RPC_METRICS: zinder_runtime::RpcMetricNames =
    zinder_runtime::RpcMetricNames::for_service(
        "zinder_query_request_duration_seconds",
        "zinder_query_request_total",
    );

/// Registers `# HELP` and `# TYPE` text for every metric this crate emits.
///
/// Call once at startup, after `install_metrics_recorder` returns and before
/// the gRPC server records its first request. Delegates the RPC metric pair
/// to [`zinder_runtime::describe_rpc_metrics`] so every Zinder service
/// shares one description template; the compact-block-range, tree-state
/// rebuild, and broadcast budget metrics are `WalletQuery`-specific and stay
/// registered here.
pub fn describe_request_metrics() {
    zinder_runtime::describe_rpc_metrics(QUERY_RPC_METRICS, "WalletQuery");
    metrics::describe_histogram!(
        "zinder_query_compact_block_range_block_count",
        metrics::Unit::Count,
        "Number of blocks returned by a single CompactBlockRange response, labelled \
         by status. Used to monitor request fan-out and page sizing."
    );
    tree_state_replay_limit::describe_metrics();
    grpc::describe_compact_block_stream_metrics();
    broadcast_byte_budget::describe_metrics();
    forwarded_read::describe_metrics();
}

pub(crate) fn record_wallet_query_outcome<Response>(
    operation: &'static str,
    started_at: Instant,
    query_outcome: &Result<Response, QueryError>,
    block_count: Option<usize>,
) {
    let outcome = outcome_from_query_result(query_outcome);
    zinder_runtime::record_rpc_request(QUERY_RPC_METRICS, operation, started_at.elapsed(), outcome);

    if let Some(block_count) = block_count {
        metrics::histogram!(
            "zinder_query_compact_block_range_block_count",
            "status" => outcome.status_label()
        )
        .record(usize_to_u32_saturating(block_count));
    }
}

fn outcome_from_query_result<Response>(
    query_outcome: &Result<Response, QueryError>,
) -> zinder_runtime::RpcOutcome {
    match query_outcome {
        Ok(_) => zinder_runtime::RpcOutcome::Ok,
        Err(error) => zinder_runtime::RpcOutcome::Error {
            class: query_error_class(Some(error)),
        },
    }
}

fn query_error_class(error: Option<&QueryError>) -> &'static str {
    match error {
        None => "none",
        Some(QueryError::InvalidBlockRange { .. }) => "invalid_block_range",
        Some(QueryError::BlockRangeTooLarge { .. }) => "block_range_too_large",
        Some(QueryError::SubtreeRootRangeTooLarge { .. }) => "subtree_root_range_too_large",
        Some(QueryError::TransparentBalanceAddressCountExceeded { .. }) => {
            "transparent_balance_address_count_exceeded"
        }
        Some(QueryError::TransparentOutpointCountOutOfRange { .. }) => {
            "transparent_outpoint_count_out_of_range"
        }
        Some(QueryError::ArtifactUnavailable { .. }) => "artifact_unavailable",
        Some(QueryError::CompactBlockPayloadMalformed { .. }) => "compact_block_payload_malformed",
        Some(QueryError::UnsupportedShieldedProtocol { .. }) => "unsupported_shielded_protocol",
        Some(QueryError::ChainEventCursorInvalid { .. }) => "chain_event_cursor_invalid",
        Some(QueryError::ChainEventCursorExpired { .. }) => "chain_event_cursor_expired",
        Some(QueryError::TransparentHistoryCursorInvalid { .. }) => {
            "transparent_history_cursor_invalid"
        }
        Some(QueryError::InvalidAddress { .. }) => "invalid_address",
        Some(QueryError::ChainEpochPinUnavailable { .. }) => "chain_epoch_pin_unavailable",
        Some(QueryError::CanonicalHistoryUnavailable { .. }) => "canonical_history_unavailable",
        Some(QueryError::UnsupportedChainEvent { .. }) => "unsupported_chain_event",
        Some(QueryError::UnsupportedBlockSelector { .. }) => "unsupported_block_selector",
        Some(QueryError::UnsupportedTransactionStatus { .. }) => "unsupported_transaction_status",
        Some(QueryError::UnsupportedWalletEncoding { .. }) => "unsupported_wallet_encoding",
        Some(QueryError::BroadcastTransactionTooLarge { .. }) => "broadcast_transaction_too_large",
        Some(QueryError::BroadcastBudgetExhausted { .. }) => "broadcast_budget_exhausted",
        Some(QueryError::MaterializedViewUnavailable { .. }) => "materialized_view_unavailable",
        Some(QueryError::EndpointCapabilityUnavailable { .. }) => "endpoint_capability_unavailable",
        Some(QueryError::TreeStateReplayLimitReached { .. }) => "tree_state_replay_limit_reached",
        Some(QueryError::CompactBlockStreamLimitReached { .. }) => {
            "compact_block_stream_limit_reached"
        }
        Some(QueryError::ForwardedReadLimitReached { .. }) => "forwarded_read_limit_reached",
        Some(QueryError::BlockingTaskFailed { .. }) => "blocking_task_failed",
        Some(QueryError::ArtifactCorrupt { .. }) => "artifact_corrupt",
        Some(QueryError::BlockNotInBestChain) => "block_not_in_best_chain",
        Some(QueryError::Store(_)) => "store",
        Some(QueryError::WalletProjectionRead { .. }) => "wallet_projection_read",
        Some(QueryError::CanonicalStore(_)) => "canonical_store",
        Some(QueryError::WalletStore(_)) => "wallet_store",
        Some(QueryError::Node(_)) => "node",
    }
}

fn usize_to_u32_saturating(amount: usize) -> u32 {
    u32::try_from(amount).unwrap_or(u32::MAX)
}

/// Records duration + outcome for a `WalletQuery` RPC that the gRPC adapter
/// proxies to the colocated ingest-control writer.
///
/// Shares the `zinder_query_request_*` metric names with
/// [`record_wallet_query_outcome`] so dashboards see one series per
/// operation regardless of which layer handled the request. The
/// `error_class` label carries the `tonic::Code` name rather than a typed
/// `QueryError` variant, because the gRPC adapter handles the response as
/// an opaque `Status`.
pub(crate) fn record_proxy_outcome<ResponseT>(
    operation: &'static str,
    started_at: Instant,
    proxy_outcome: &Result<tonic::Response<ResponseT>, tonic::Status>,
) {
    let outcome = match proxy_outcome {
        Ok(_) => zinder_runtime::RpcOutcome::Ok,
        Err(status) => zinder_runtime::RpcOutcome::Error {
            class: proxy_error_class(Some(status)),
        },
    };
    zinder_runtime::record_rpc_request(QUERY_RPC_METRICS, operation, started_at.elapsed(), outcome);
}

fn proxy_error_class(error: Option<&tonic::Status>) -> &'static str {
    let Some(status) = error else {
        return "none";
    };
    match status.code() {
        tonic::Code::Ok => "none",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::NotFound => "not_found",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::Aborted => "aborted",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::Unauthenticated => "unauthenticated",
        tonic::Code::Unknown => "unknown",
    }
}

/// Visible-tip block identity bound to one chain epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VisibleTipBlock {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Visible-tip block height.
    pub height: BlockHeight,
    /// Visible-tip block hash.
    pub block_hash: zinder_core::BlockHash,
}

/// The visible tip and the Unix time in its header, read from one pair for a
/// single-shot answer.
///
/// Its epoch identifies the tip for caching. Reads by that epoch are not
/// guaranteed to be served.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServedTipSnapshot {
    /// Chain epoch of the pair that answered.
    pub chain_epoch: ChainEpoch,
    /// Unix time in the visible-tip block header.
    pub block_time_unix_seconds: i64,
}

/// Settled-tip block metadata bound to one chain epoch. The block sits at
/// `chain_epoch.settled_tip_height` and marks the reorg-window finality boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SettledTipBlock {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Settled tip height (`chain_epoch.settled_tip_height`).
    pub height: BlockHeight,
    /// Block hash at `settled_tip_height`.
    pub block_hash: zinder_core::BlockHash,
}

/// Block-identity resolver response bound to one chain epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockIdAtEpoch {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Resolved block identity in the canonical best chain.
    pub block_id: BlockId,
}

/// Block-header read response bound to one chain epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockHeaderAtEpoch {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Block-header read-model value at the resolved selector.
    pub block_header: BlockHeader,
}

/// Single compact block response bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactBlock {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Compact block artifact at the requested height.
    pub compact_block: CompactBlockArtifact,
}

/// Compact block range response bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactBlockRange {
    /// Chain epoch used for every compact block in this range.
    pub chain_epoch: ChainEpoch,
    /// Inclusive height range the compact blocks cover: the requested range,
    /// or the part of it a byte-bounded read kept.
    pub block_range: BlockHeightRange,
    /// Compact block artifacts in ascending height order.
    pub compact_blocks: Vec<CompactBlockArtifact>,
}

/// Full serialized block response bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullBlock {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Raw block blob at the requested height.
    pub block_blob: BlockBlobArtifact,
}

/// Demand-driven full-block range stream bound to one pinned chain epoch.
///
/// [`chain_epoch`](Self::chain_epoch) is resolved once and describes every
/// delivered block. [`blocks`](Self::blocks) yields raw block blobs in
/// ascending, contiguous height order until the range is exhausted; a missing
/// blob or store failure arrives as a terminal `Err` after the blocks already
/// delivered, and dropping the receiver stops the backing producer.
#[derive(Debug)]
pub struct FullBlockStream {
    /// Chain epoch every delivered block is read under.
    pub chain_epoch: ChainEpoch,
    /// Ascending block blobs, then optionally one terminal error.
    pub blocks: mpsc::Receiver<Result<BlockBlobArtifact, QueryError>>,
}

/// Demand-driven compact-block range stream bound to one chain epoch.
///
/// [`chain_epoch`](Self::chain_epoch) is resolved once and describes every
/// delivered block. [`blocks`](Self::blocks) yields compact blocks in
/// ascending, contiguous height order until the range is exhausted; a store
/// failure arrives as a terminal `Err` after the blocks already delivered,
/// and dropping the receiver stops the backing producer.
#[derive(Debug)]
pub struct CompactBlockStream {
    /// Chain epoch every delivered block is read under.
    pub chain_epoch: ChainEpoch,
    /// Ascending compact blocks, then optionally one terminal error.
    pub blocks: mpsc::Receiver<Result<CompactBlockArtifact, QueryError>>,
}

/// Single mined-transaction response bound to one chain epoch. Used by
/// [`WalletQueryApi::transaction_at_block_index`], which only resolves
/// mined transactions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transaction {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Transaction location at the requested block-local index.
    pub transaction: TransactionLocation,
}

/// Raw transaction blob response bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawTransaction {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Optional raw transaction blob.
    pub transaction: TransactionBlobArtifact,
}

/// Typed transaction-status response bound to one chain epoch.
///
/// Returned by [`WalletQueryApi::transaction`]. Carries the typed
/// [`TxStatus`] (`Mined`/`InMempool`/`NotFound`) and
/// the epoch used to answer the read; the wire-side adapter maps
/// `TxStatus::NotFound` to gRPC `NOT_FOUND`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionStatus {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Typed transaction status.
    pub status: TxStatus,
}

/// Transparent-address unspent output request.
///
/// Address inputs are typed: the wire boundary parses string addresses and
/// SHA-256-hashes them to `address_script_hash` before constructing this
/// request. The native API never carries a `String` form on the read path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressUnspentOutputsRequest {
    /// SHA-256 of the transparent address scriptPubKey.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Wallet-birthday floor: minimum mined height to include.
    /// `BlockHeight::new(0)` means scan from genesis.
    pub start_height: BlockHeight,
}

/// Spend status of a batch of transparent outpoints, read at one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentOutpointSpendStatuses {
    /// Chain epoch every status was read at.
    pub chain_epoch: ChainEpoch,
    /// One status per requested outpoint, in request order.
    pub statuses: Vec<TransparentOutpointSpendStatus>,
}

/// Transparent-address tx-history range request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressTxIdsInRangeRequest {
    /// SHA-256 of the transparent address scriptPubKey.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Inclusive minimum block height. Ignored when `from_cursor` is `Some`.
    pub start_height: BlockHeight,
    /// Inclusive maximum block height.
    pub end_height: BlockHeight,
    /// Server-bounded maximum entries per page.
    pub max_entries: NonZeroU32,
    /// Optional cursor returned by a previous response. The cursor is bound
    /// to the response's visible-chain event fence and is rejected after a
    /// reorg.
    pub from_cursor: Option<StreamCursorTokenV1>,
}

/// Transparent-address tx-history page response bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressTxIds {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Tx-history artifacts in ascending mined order.
    pub artifacts: Vec<TransparentAddressTxIndexArtifact>,
    /// Resume cursor when more entries may be available.
    pub next_cursor: Option<StreamCursorTokenV1>,
}

/// Typed canonical `z_gettreestate` `finalState` frontier bytes per pool.
///
/// `None` marks a pool that is not active at the tree state's height;
/// `Some(bytes)` holds the raw canonical legacy `CommitmentTree` encoding.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TreeStateFrontiers {
    /// Raw Sapling `finalState` bytes, or `None` before Sapling activation.
    pub sapling_final_state: Option<Vec<u8>>,
    /// Raw Orchard `finalState` bytes, or `None` before NU5 activation.
    pub orchard_final_state: Option<Vec<u8>>,
    /// Raw Ironwood `finalState` bytes, or `None` before NU6.3 activation.
    pub ironwood_final_state: Option<Vec<u8>>,
}

/// Tree-state response bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeState {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Height the tree state belongs to.
    pub height: BlockHeight,
    /// Hash of the block this tree state belongs to.
    pub block_hash: zinder_core::BlockHash,
    /// Block timestamp in Unix seconds.
    pub block_time_seconds: u32,
    /// Typed per-pool canonical `finalState` frontier bytes.
    pub frontiers: TreeStateFrontiers,
}

impl TreeState {
    /// Encodes the native `z_gettreestate`-compatible JSON payload bytes.
    pub fn encode_json_payload(&self) -> Result<Vec<u8>, QueryError> {
        let mut payload = serde_json::Map::new();
        for (name, bytes) in [
            ("sapling", self.frontiers.sapling_final_state.as_deref()),
            ("orchard", self.frontiers.orchard_final_state.as_deref()),
            ("ironwood", self.frontiers.ironwood_final_state.as_deref()),
        ] {
            if let Some(bytes) = bytes {
                payload.insert(
                    name.to_owned(),
                    serde_json::json!({
                        "commitments": {
                            "finalState": hex::encode(bytes)
                        }
                    }),
                );
            }
        }
        serde_json::to_vec(&serde_json::Value::Object(payload)).map_err(|source| {
            QueryError::ArtifactCorrupt {
                family: ArtifactFamily::TreeState,
                reason: source.to_string(),
            }
        })
    }
}

/// Subtree-root response bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubtreeRoots {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Shielded protocol returned.
    pub protocol: ShieldedProtocol,
    /// First requested subtree-root index.
    pub start_index: SubtreeRootIndex,
    /// Subtree-root artifacts in ascending index order.
    pub subtree_roots: Vec<SubtreeRootArtifact>,
}

/// Bounded page of chain-event envelopes in stream order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainEvents {
    /// Chain-event envelopes returned for this page.
    pub event_envelopes: Vec<ChainEventEnvelope>,
}

/// Artifact lookup key used in unavailable-artifact errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArtifactKey {
    /// Artifact keyed by block height.
    BlockHeight(BlockHeight),
    /// Artifact keyed by transaction id.
    TransactionId(TransactionId),
    /// Subtree-root artifact keyed by shielded protocol and subtree index.
    SubtreeRootIndex {
        /// Shielded protocol requested.
        protocol: ShieldedProtocol,
        /// Requested subtree-root index.
        index: SubtreeRootIndex,
    },
    /// Transaction lookup keyed by a transaction index inside a block.
    BlockTransactionIndex {
        /// Requested block height.
        height: BlockHeight,
        /// Requested transaction index within the block.
        tx_index: u64,
    },
}

impl std::fmt::Display for ArtifactKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockHeight(height) => write!(formatter, "height {}", height.value()),
            Self::TransactionId(transaction_id) => {
                write!(formatter, "transaction id {transaction_id:?}")
            }
            Self::SubtreeRootIndex { protocol, index } => {
                write!(formatter, "{protocol:?} subtree index {}", index.value())
            }
            Self::BlockTransactionIndex { height, tx_index } => write!(
                formatter,
                "height {} transaction index {tx_index}",
                height.value()
            ),
        }
    }
}

/// Error returned by wallet query operations.
#[derive(Debug, Error)]
pub enum QueryError {
    /// Requested block range has an invalid shape.
    #[error("invalid block range: start {start_height:?} exceeds end {end_height:?}")]
    InvalidBlockRange {
        /// First requested height.
        start_height: BlockHeight,
        /// Last requested height.
        end_height: BlockHeight,
    },

    /// Requested block range exceeds the configured response bound.
    #[error("block range is too large: requested {requested}, maximum {maximum}")]
    BlockRangeTooLarge {
        /// Requested block count.
        requested: usize,
        /// Maximum allowed block count.
        maximum: usize,
    },

    /// Requested subtree-root range exceeds the public per-request bound.
    #[error("subtree-root range is too large: requested {requested}, maximum {maximum}")]
    SubtreeRootRangeTooLarge {
        /// Requested subtree-root count.
        requested: u32,
        /// Maximum allowed subtree-root count.
        maximum: u32,
    },

    /// Balance request named an empty address list or more addresses than the
    /// per-request cap.
    #[error(
        "transparent balance address count is out of range: requested {requested}, maximum {maximum}"
    )]
    TransparentBalanceAddressCountExceeded {
        /// Requested address count.
        requested: usize,
        /// Maximum allowed address count.
        maximum: u32,
    },

    /// Outpoint spend-status request named no outpoints or more than the
    /// per-request cap.
    #[error(
        "transparent outpoint count is out of range: requested {requested}, allowed 1 to {maximum}"
    )]
    TransparentOutpointCountOutOfRange {
        /// Requested outpoint count.
        requested: usize,
        /// Maximum allowed outpoint count.
        maximum: usize,
    },

    /// Indexed artifact is unavailable for the requested key.
    #[error("{family:?} artifact is unavailable for {key}")]
    ArtifactUnavailable {
        /// Artifact family requested.
        family: ArtifactFamily,
        /// Requested lookup key.
        key: ArtifactKey,
    },

    /// Indexed compact block payload could not be decoded.
    #[error("compact block payload at height {height:?} is malformed: {reason}")]
    CompactBlockPayloadMalformed {
        /// Requested block height.
        height: BlockHeight,
        /// Decoder failure reason.
        reason: String,
    },

    /// Stored artifact bytes failed Zcash protocol decoding at request time.
    #[error("{family:?} artifact is corrupt: {reason}")]
    ArtifactCorrupt {
        /// Artifact family that failed to decode.
        family: ArtifactFamily,
        /// Decoder failure reason.
        reason: String,
    },

    /// Block selector resolved to no visible block in the canonical best chain.
    #[error("block selector resolved to no visible block in the canonical best chain")]
    BlockNotInBestChain,

    /// Shielded protocol cannot be represented on the native wallet protocol.
    #[error("{protocol:?} is not supported by the native wallet protocol")]
    UnsupportedShieldedProtocol {
        /// Shielded protocol that cannot be encoded.
        protocol: ShieldedProtocol,
    },

    /// Transparent address tx-history cursor failed validation.
    #[error("transparent history cursor is invalid: {reason}")]
    TransparentHistoryCursorInvalid {
        /// Cursor validation failure reason.
        reason: &'static str,
    },

    /// Transparent address selector failed validation.
    #[error("invalid transparent address: {reason}")]
    InvalidAddress {
        /// Validation failure reason.
        reason: &'static str,
    },

    /// Chain-event cursor failed validation.
    #[error("chain-event cursor is invalid: {reason}")]
    ChainEventCursorInvalid {
        /// Cursor validation failure reason.
        reason: &'static str,
    },

    /// Chain-event cursor points before retained event history.
    #[error(
        "chain-event cursor expired: event sequence {event_sequence}, oldest retained {oldest_retained_sequence}"
    )]
    ChainEventCursorExpired {
        /// Cursor event sequence.
        event_sequence: u64,
        /// Oldest retained event sequence.
        oldest_retained_sequence: u64,
    },

    /// Requested chain epoch is no longer available.
    #[error("chain-epoch pin is unavailable: {chain_epoch_id:?}")]
    ChainEpochPinUnavailable {
        /// Requested chain epoch id.
        chain_epoch_id: ChainEpochId,
    },

    /// The store's retained canonical history starts after a checkpoint, so a
    /// transaction or transparent outpoint lookup that missed cannot prove
    /// its subject absent.
    #[error(
        "canonical history starts at height {}; absence cannot be proven",
        .first_available_height.value()
    )]
    CanonicalHistoryUnavailable {
        /// First height the store retains.
        first_available_height: BlockHeight,
    },

    /// Store returned a chain-event variant unsupported by the native protocol.
    #[error("unsupported chain event: {event}")]
    UnsupportedChainEvent {
        /// Unsupported event description.
        event: &'static str,
    },

    /// Block selector variant cannot be resolved by this query implementation.
    #[error("unsupported block selector: {reason}")]
    UnsupportedBlockSelector {
        /// Stable diagnostic reason.
        reason: &'static str,
    },

    /// Typed transaction status cannot be encoded on the native wire protocol.
    #[error("unsupported transaction status: {reason}")]
    UnsupportedTransactionStatus {
        /// Stable diagnostic reason.
        reason: &'static str,
    },

    /// A native domain value has no representation in the wallet protocol.
    #[error("unsupported wallet encoding: {value_kind}")]
    UnsupportedWalletEncoding {
        /// Native value family that cannot be represented.
        value_kind: &'static str,
    },

    /// Raw transaction exceeds the maximum serialized size accepted for broadcast.
    #[error("raw transaction is too large: {actual} bytes exceeds maximum {maximum}")]
    BroadcastTransactionTooLarge {
        /// Serialized length of the submitted transaction.
        actual: usize,
        /// Maximum accepted serialized length.
        maximum: usize,
    },

    /// The process is already broadcasting its budget of transaction bytes.
    #[error(
        "broadcast budget exhausted: this server already broadcasts {budget_bytes} bytes of transactions at once; retry with backoff"
    )]
    BroadcastBudgetExhausted {
        /// Transaction bytes the process broadcasts at once.
        budget_bytes: usize,
    },

    /// Materialized-view-owned wallet projection is not configured for this query handle.
    #[error("materialized view is unavailable for {capability}")]
    MaterializedViewUnavailable {
        /// Capability that requires the materialized view.
        capability: &'static str,
    },

    /// The composed endpoint does not structurally implement a capability.
    #[error("endpoint capability is unavailable: {capability}")]
    EndpointCapabilityUnavailable {
        /// Exact capability identifier required by the operation.
        capability: &'static str,
    },

    /// A tree state at a height without a stored checkpoint needs a
    /// reconstruction, the query already runs its limit of them, and either
    /// no slot freed within the wait or every waiting place was taken.
    #[error(
        "tree-state reconstruction limit reached: this server already reconstructs {max_concurrent_replays} tree states at once"
    )]
    TreeStateReplayLimitReached {
        /// Most reconstructions the query runs at once.
        max_concurrent_replays: NonZeroU32,
    },

    /// The adapter already serves its limit of open native
    /// `CompactBlocksInRange` streams.
    #[error(
        "this server already serves {max_streams} CompactBlocksInRange streams; retry after a short delay"
    )]
    CompactBlockStreamLimitReached {
        /// Most streams the adapter serves at once.
        max_streams: NonZeroU32,
    },

    /// A read forwarded to the node found every slot of its kind taken, and
    /// either no slot freed within the wait or every waiting place was taken.
    #[error(
        "forwarded {kind} reads are at their limit of {max_concurrent} at once; retry with backoff"
    )]
    ForwardedReadLimitReached {
        /// The refused kind, as the `kind` metric label names it.
        kind: &'static str,
        /// Most reads of the kind that run at once.
        max_concurrent: u32,
    },

    /// A blocking read task failed unexpectedly (panic or runtime shutdown).
    #[error("query read task failed: {reason}")]
    BlockingTaskFailed {
        /// Operator-facing failure reason.
        reason: String,
    },

    /// Canonical read API returned a storage error.
    #[error(transparent)]
    Store(#[from] StoreError),

    /// Typed wallet-projection backend returned a storage failure.
    #[error("wallet projection read failed: {source}")]
    WalletProjectionRead {
        /// Backend-specific source retained without leaking its API contract.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A canonical read found that storage could not answer.
    ///
    /// [`QueryError::canonical_read`] routes every outcome a request can
    /// cause to its own variant, such as [`QueryError::ArtifactUnavailable`],
    /// so this variant carries only storage failures.
    #[error(transparent)]
    CanonicalStore(StorageReadError),

    /// A wallet projection read found that storage could not answer.
    ///
    /// [`QueryError::wallet_read`] routes every outcome a request can cause
    /// to its own variant, so this variant carries only storage failures.
    #[error(transparent)]
    WalletStore(StorageReadError),

    /// Upstream node operation failed.
    #[error(transparent)]
    Node(#[from] SourceError),
}

impl QueryError {
    /// Converts the outcome of a failed [`CanonicalReader`] read, keeping a
    /// storage failure as [`QueryError::CanonicalStore`].
    #[must_use]
    pub fn canonical_read(error: StorageReadError) -> Self {
        Self::from_storage_read(error, Self::CanonicalStore)
    }

    /// Converts the outcome of a failed [`WalletProjectionReader`] read,
    /// keeping a storage failure as [`QueryError::WalletStore`].
    #[must_use]
    pub fn wallet_read(error: StorageReadError) -> Self {
        Self::from_storage_read(error, Self::WalletStore)
    }

    /// Reports whether this read failed only because the reader it read
    /// through was invalidated, so a reader admitted afresh can answer it.
    pub(crate) const fn invalidates_reader(&self) -> bool {
        matches!(
            self,
            Self::CanonicalStore(StorageReadError::ReaderInvalidated { .. })
                | Self::WalletStore(StorageReadError::ReaderInvalidated { .. })
        )
    }

    fn from_storage_read(
        error: StorageReadError,
        storage_failure: fn(StorageReadError) -> Self,
    ) -> Self {
        match error {
            StorageReadError::ArtifactUnavailable { family, key } => {
                Self::ArtifactUnavailable { family, key }
            }
            StorageReadError::CanonicalHistoryUnavailable {
                first_available_height,
            } => Self::CanonicalHistoryUnavailable {
                first_available_height,
            },
            StorageReadError::ChainEventCursorExpired {
                event_sequence,
                oldest_retained_sequence,
            } => Self::ChainEventCursorExpired {
                event_sequence,
                oldest_retained_sequence,
            },
            StorageReadError::ChainEventCursorInvalid { reason } => {
                Self::ChainEventCursorInvalid { reason }
            }
            StorageReadError::ReadTaskFailed { reason } => Self::BlockingTaskFailed { reason },
            failure @ (StorageReadError::ReaderInvalidated { .. }
            | StorageReadError::Unavailable { .. }) => storage_failure(failure),
        }
    }
}

impl zinder_proto::BoundaryError for QueryError {
    /// Maps each [`QueryError`] variant to its stable
    /// [`ErrorReason`](zinder_proto::v1::ops::ErrorReason).
    ///
    /// `Store` failures route through `StoreError::error_reason` at the gRPC
    /// adapter and never reach this match in practice; the arm keeps the match
    /// total and fails closed as storage-unavailable.
    fn error_reason(&self) -> zinder_proto::v1::ops::ErrorReason {
        use zinder_proto::v1::ops::ErrorReason;
        match self {
            Self::InvalidBlockRange { .. } => ErrorReason::InvalidBlockRange,
            Self::BlockRangeTooLarge { .. } => ErrorReason::BlockRangeTooLarge,
            Self::SubtreeRootRangeTooLarge { .. } => ErrorReason::SubtreeRootRangeTooLarge,
            Self::TransparentBalanceAddressCountExceeded { .. } => {
                ErrorReason::TransparentBalanceAddressCountExceeded
            }
            Self::TransparentOutpointCountOutOfRange { .. } => {
                ErrorReason::TransparentOutpointCountOutOfRange
            }
            Self::ChainEventCursorInvalid { .. } => ErrorReason::ChainEventCursorInvalid,
            Self::TransparentHistoryCursorInvalid { .. } => {
                ErrorReason::TransparentHistoryCursorInvalid
            }
            Self::InvalidAddress { .. } => ErrorReason::InvalidAddress,
            Self::UnsupportedShieldedProtocol { .. } => ErrorReason::UnsupportedShieldedProtocol,
            Self::BroadcastTransactionTooLarge { .. } => ErrorReason::BroadcastTransactionTooLarge,
            Self::BroadcastBudgetExhausted { .. } => ErrorReason::BroadcastBudgetExhausted,
            Self::MaterializedViewUnavailable { .. } => ErrorReason::MaterializedViewUnavailable,
            Self::EndpointCapabilityUnavailable { .. } => {
                ErrorReason::EndpointCapabilityUnavailable
            }
            Self::ChainEventCursorExpired { .. } => ErrorReason::ChainEventCursorExpired,
            Self::ChainEpochPinUnavailable { .. } => ErrorReason::ChainEpochPinUnavailable,
            Self::CanonicalHistoryUnavailable { .. } => ErrorReason::CanonicalHistoryUnavailable,
            Self::ArtifactUnavailable { .. } => ErrorReason::ArtifactUnavailable,
            Self::CompactBlockPayloadMalformed { .. } => ErrorReason::CompactBlockPayloadMalformed,
            Self::ArtifactCorrupt { .. } => ErrorReason::ArtifactCorrupt,
            Self::BlockNotInBestChain => ErrorReason::BlockNotInBestChain,
            Self::UnsupportedChainEvent { .. } => ErrorReason::UnsupportedChainEvent,
            Self::UnsupportedBlockSelector { .. } => ErrorReason::UnsupportedBlockSelector,
            Self::UnsupportedTransactionStatus { .. } => ErrorReason::UnsupportedTransactionStatus,
            Self::UnsupportedWalletEncoding { .. } => ErrorReason::UnsupportedWalletEncoding,
            Self::TreeStateReplayLimitReached { .. } => ErrorReason::TreeStateReplayLimitReached,
            Self::CompactBlockStreamLimitReached { .. } => {
                ErrorReason::CompactBlockStreamLimitReached
            }
            Self::ForwardedReadLimitReached { .. } => ErrorReason::ForwardedReadLimitReached,
            Self::BlockingTaskFailed { .. } => ErrorReason::BlockingTaskFailed,
            Self::Node(source_error) if source_error.is_node_capability_missing() => {
                ErrorReason::NodeCapabilityMissing
            }
            Self::Node(SourceError::NodeRateLimited { .. }) => ErrorReason::NodeRateLimited,
            Self::Node(_) => ErrorReason::NodeUnavailable,
            Self::Store(_)
            | Self::WalletProjectionRead { .. }
            | Self::CanonicalStore(_)
            | Self::WalletStore(_) => ErrorReason::StorageUnavailable,
        }
    }
}

pub(crate) fn validate_block_range(
    block_range: BlockHeightRange,
    max_blocks: NonZeroU32,
) -> Result<(), QueryError> {
    if block_range.start > block_range.end {
        return Err(QueryError::InvalidBlockRange {
            start_height: block_range.start,
            end_height: block_range.end,
        });
    }

    let requested = block_range.into_iter().len();
    let maximum = u32_to_usize(max_blocks.get());
    if requested > maximum {
        return Err(QueryError::BlockRangeTooLarge { requested, maximum });
    }

    Ok(())
}

fn validate_subtree_root_range(subtree_root_range: SubtreeRootRange) -> Result<(), QueryError> {
    let requested = subtree_root_range.max_entries.get();
    if requested > MAX_SUBTREE_ROOTS_PER_REQUEST {
        return Err(QueryError::SubtreeRootRangeTooLarge {
            requested,
            maximum: MAX_SUBTREE_ROOTS_PER_REQUEST,
        });
    }
    Ok(())
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "zinder-core rejects targets with pointer widths below 32 bits"
)]
const fn u32_to_usize(count: u32) -> usize {
    count as usize
}

#[cfg(test)]
mod error_reason_tests {
    use zinder_core::MAX_RAW_TRANSACTION_BYTES;
    use zinder_proto::{BoundaryError, v1::ops::ErrorReason};

    use super::*;

    /// One representative of every [`QueryError`] variant.
    ///
    /// The list is exhaustive, so a new variant fails to compile until it is
    /// listed; [`no_query_error_variant_maps_to_unspecified`] then asserts the
    /// new variant carries a real reason.
    #[allow(
        clippy::too_many_lines,
        reason = "One literal per QueryError variant; the length tracks the enum, not branching complexity."
    )]
    fn one_of_each_variant() -> Vec<QueryError> {
        vec![
            QueryError::InvalidBlockRange {
                start_height: BlockHeight::new(2),
                end_height: BlockHeight::new(1),
            },
            QueryError::BlockRangeTooLarge {
                requested: 2,
                maximum: 1,
            },
            QueryError::SubtreeRootRangeTooLarge {
                requested: MAX_SUBTREE_ROOTS_PER_REQUEST.saturating_add(1),
                maximum: MAX_SUBTREE_ROOTS_PER_REQUEST,
            },
            QueryError::TransparentBalanceAddressCountExceeded {
                requested: 257,
                maximum: MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
            },
            QueryError::TransparentOutpointCountOutOfRange {
                requested: 0,
                maximum: zinder_core::MAX_OUTPOINTS_PER_SPEND_STATUS_REQUEST,
            },
            QueryError::ArtifactUnavailable {
                family: ArtifactFamily::CompactBlock,
                key: ArtifactKey::BlockHeight(BlockHeight::new(1)),
            },
            QueryError::CompactBlockPayloadMalformed {
                height: BlockHeight::new(1),
                reason: "probe".to_owned(),
            },
            QueryError::ArtifactCorrupt {
                family: ArtifactFamily::CompactBlock,
                reason: "probe".to_owned(),
            },
            QueryError::BlockNotInBestChain,
            QueryError::UnsupportedShieldedProtocol {
                protocol: ShieldedProtocol::Sapling,
            },
            QueryError::TransparentHistoryCursorInvalid { reason: "probe" },
            QueryError::InvalidAddress { reason: "probe" },
            QueryError::ChainEventCursorInvalid { reason: "probe" },
            QueryError::ChainEventCursorExpired {
                event_sequence: 1,
                oldest_retained_sequence: 2,
            },
            QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: ChainEpochId::new(1),
            },
            QueryError::CanonicalHistoryUnavailable {
                first_available_height: BlockHeight::new(101),
            },
            QueryError::UnsupportedChainEvent { event: "probe" },
            QueryError::UnsupportedBlockSelector { reason: "probe" },
            QueryError::UnsupportedTransactionStatus { reason: "probe" },
            QueryError::UnsupportedWalletEncoding {
                value_kind: "probe",
            },
            QueryError::BroadcastTransactionTooLarge {
                actual: MAX_RAW_TRANSACTION_BYTES + 1,
                maximum: MAX_RAW_TRANSACTION_BYTES,
            },
            QueryError::BroadcastBudgetExhausted {
                budget_bytes: crate::BROADCAST_BYTE_BUDGET,
            },
            QueryError::MaterializedViewUnavailable {
                capability: "probe",
            },
            QueryError::EndpointCapabilityUnavailable {
                capability: "probe",
            },
            QueryError::TreeStateReplayLimitReached {
                max_concurrent_replays: NonZeroU32::MIN,
            },
            QueryError::CompactBlockStreamLimitReached {
                max_streams: NonZeroU32::MIN,
            },
            QueryError::ForwardedReadLimitReached {
                kind: "transaction",
                max_concurrent: 24,
            },
            QueryError::BlockingTaskFailed {
                reason: "probe".to_owned(),
            },
            QueryError::Store(StoreError::InvalidChainEpochArtifacts { reason: "probe" }),
            QueryError::WalletProjectionRead {
                source: Box::new(std::io::Error::other("probe")),
            },
            QueryError::CanonicalStore(StorageReadError::Unavailable {
                source: Box::new(std::io::Error::other("probe")),
            }),
            QueryError::WalletStore(StorageReadError::Unavailable {
                source: Box::new(std::io::Error::other("probe")),
            }),
            QueryError::Node(SourceError::InvalidBlockHashHex {
                reason: "probe".to_owned(),
            }),
        ]
    }

    /// A storage failure keeps the `error_class` of the store that failed.
    ///
    /// Dashboards and alerts that select `canonical_store` or `wallet_store`
    /// keep matching, and an outcome the request caused keeps its own class
    /// from either store.
    #[test]
    fn storage_failures_keep_the_error_class_of_the_store_that_failed() {
        let unavailable = || StorageReadError::Unavailable {
            source: Box::new(std::io::Error::other("probe")),
        };
        let invalidated = || StorageReadError::ReaderInvalidated {
            source: Box::new(std::io::Error::other("probe")),
        };
        for (error, class) in [
            (QueryError::canonical_read(unavailable()), "canonical_store"),
            (QueryError::canonical_read(invalidated()), "canonical_store"),
            (QueryError::wallet_read(unavailable()), "wallet_store"),
            (QueryError::wallet_read(invalidated()), "wallet_store"),
            (
                QueryError::wallet_read(StorageReadError::CanonicalHistoryUnavailable {
                    first_available_height: BlockHeight::new(3),
                }),
                "canonical_history_unavailable",
            ),
            (
                QueryError::canonical_read(StorageReadError::ReadTaskFailed {
                    reason: "probe".to_owned(),
                }),
                "blocking_task_failed",
            ),
        ] {
            assert_eq!(query_error_class(Some(&error)), class, "{error:?}");
            assert_eq!(
                error.error_reason() == ErrorReason::StorageUnavailable,
                matches!(class, "canonical_store" | "wallet_store"),
                "{error:?}"
            );
        }
    }

    #[test]
    fn history_floor_miss_carries_the_first_retained_height_on_the_wire()
    -> Result<(), Box<dyn std::error::Error>> {
        let error = QueryError::canonical_read(StorageReadError::CanonicalHistoryUnavailable {
            first_available_height: BlockHeight::new(101),
        });
        assert!(matches!(
            error,
            QueryError::CanonicalHistoryUnavailable {
                first_available_height
            } if first_available_height == BlockHeight::new(101)
        ));
        let status = crate::status_from_query_error(&error);
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            zinder_proto::wire::canonical_history_first_available_height(&status)?,
            Some(BlockHeight::new(101))
        );
        Ok(())
    }

    #[test]
    fn tree_state_replay_refusal_is_resource_exhausted_on_the_wire() {
        let status = crate::status_from_query_error(&QueryError::TreeStateReplayLimitReached {
            max_concurrent_replays: NonZeroU32::MIN,
        });
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            tonic_types::StatusExt::get_details_error_info(&status)
                .map(|error_info| error_info.reason),
            Some("TREE_STATE_REPLAY_LIMIT_REACHED".to_owned())
        );
    }

    #[test]
    fn compact_block_stream_refusal_is_resource_exhausted_on_the_wire() {
        let status = crate::status_from_query_error(&QueryError::CompactBlockStreamLimitReached {
            max_streams: NonZeroU32::MIN,
        });
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            tonic_types::StatusExt::get_details_error_info(&status)
                .map(|error_info| error_info.reason),
            Some("COMPACT_BLOCK_STREAM_LIMIT_REACHED".to_owned())
        );
    }

    #[test]
    fn forwarded_read_refusal_is_resource_exhausted_on_the_wire() {
        let status = crate::status_from_query_error(&QueryError::ForwardedReadLimitReached {
            kind: "transaction",
            max_concurrent: 24,
        });
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            tonic_types::StatusExt::get_details_error_info(&status)
                .map(|error_info| error_info.reason),
            Some("FORWARDED_READ_LIMIT_REACHED".to_owned())
        );
    }

    #[test]
    fn broadcast_budget_refusal_is_unavailable_on_the_wire() {
        let status = crate::status_from_query_error(&QueryError::BroadcastBudgetExhausted {
            budget_bytes: crate::BROADCAST_BYTE_BUDGET,
        });
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            tonic_types::StatusExt::get_details_error_info(&status)
                .map(|error_info| error_info.reason),
            Some("BROADCAST_BUDGET_EXHAUSTED".to_owned())
        );
    }

    #[test]
    fn no_query_error_variant_maps_to_unspecified() {
        for error in one_of_each_variant() {
            assert_ne!(
                error.error_reason(),
                ErrorReason::Unspecified,
                "QueryError variant {error:?} mapped to ERROR_REASON_UNSPECIFIED"
            );
        }
    }

    #[test]
    fn permanent_historical_node_refusals_map_to_node_capability_missing() {
        for source_error in [
            SourceError::HistoricalTreeStateUnavailable {
                height: BlockHeight::new(1_000_000),
                reason: "probe".to_owned(),
            },
            SourceError::HistoricalBlockBodyUnavailable {
                height: BlockHeight::new(1_000_000),
                reason: "probe".to_owned(),
            },
            SourceError::HistoricalSubtreeRootsUnavailable {
                protocol: ShieldedProtocol::Sapling,
                start_index: zinder_core::SubtreeRootIndex::new(0),
                reason: "probe".to_owned(),
            },
        ] {
            let error = QueryError::Node(source_error);
            assert_eq!(error.error_reason(), ErrorReason::NodeCapabilityMissing);
            assert_eq!(
                crate::status_from_query_error(&error).code(),
                tonic::Code::FailedPrecondition
            );
        }

        let transient = QueryError::Node(SourceError::BlockUnavailable {
            height: BlockHeight::new(1),
            reason: "probe".to_owned(),
        });
        assert_eq!(transient.error_reason(), ErrorReason::NodeUnavailable);
    }

    #[test]
    fn node_rate_limited_maps_to_retryable_unavailable_with_its_own_reason() {
        let error = QueryError::Node(SourceError::NodeRateLimited {
            operation: "getrawtransaction",
        });
        assert_eq!(error.error_reason(), ErrorReason::NodeRateLimited);
        let status = crate::status_from_query_error(&error);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            tonic_types::StatusExt::get_details_error_info(&status)
                .map(|error_info| error_info.reason),
            Some("NODE_RATE_LIMITED".to_owned())
        );
    }
}

#[cfg(test)]
mod tree_state_payload_tests {
    use serde_json::{Map, Value, json};
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, ChainEpoch, ChainEpochId, ChainTipMetadata,
        Network, UnixTimestampMillis,
    };

    use super::{TreeState, TreeStateFrontiers};

    /// The payload encoding `WalletServingQuery` produced before frontiers
    /// reached the adapters as typed bytes.
    fn reference_payload(pools: [(&'static str, Option<&[u8]>); 3]) -> Vec<u8> {
        let mut payload = Map::new();
        for (name, bytes) in pools {
            if let Some(bytes) = bytes {
                payload.insert(
                    name.to_owned(),
                    json!({ "commitments": { "finalState": hex::encode(bytes) } }),
                );
            }
        }
        serde_json::to_vec(&Value::Object(payload)).unwrap_or_default()
    }

    fn tree_state(frontiers: TreeStateFrontiers) -> TreeState {
        TreeState {
            chain_epoch: ChainEpoch {
                id: ChainEpochId::new(1),
                network: Network::ZcashRegtest,
                visible_tip_height: BlockHeight::new(1),
                visible_tip_hash: BlockHash::from_bytes([1; 32]),
                settled_tip_height: BlockHeight::new(1),
                settled_tip_hash: BlockHash::from_bytes([1; 32]),
                artifact_schema_version: ArtifactSchemaVersion::new(20),
                tip_metadata: ChainTipMetadata::new(0, 0, 0),
                created_at: UnixTimestampMillis::new(0),
            },
            height: BlockHeight::new(1),
            block_hash: BlockHash::from_bytes([2; 32]),
            block_time_seconds: 1_774_668_401,
            frontiers,
        }
    }

    #[test]
    fn encode_json_payload_is_byte_identical_to_the_previous_encoding_for_every_pool_subset()
    -> Result<(), super::QueryError> {
        let sapling = vec![0x01, 0xaa, 0x00];
        let orchard = vec![0xbb];
        let ironwood = vec![0xcc, 0xdd];

        for subset in 0_u8..8 {
            let pick = |bit: u8, bytes: &[u8]| (subset & bit != 0).then(|| bytes.to_vec());
            let frontiers = TreeStateFrontiers {
                sapling_final_state: pick(1, &sapling),
                orchard_final_state: pick(2, &orchard),
                ironwood_final_state: pick(4, &ironwood),
            };
            let expected = reference_payload([
                ("sapling", frontiers.sapling_final_state.as_deref()),
                ("orchard", frontiers.orchard_final_state.as_deref()),
                ("ironwood", frontiers.ironwood_final_state.as_deref()),
            ]);

            assert_eq!(
                tree_state(frontiers).encode_json_payload()?,
                expected,
                "pool subset {subset:#05b}"
            );
        }
        Ok(())
    }

    /// The wire bytes the base revision emits for three active pools.
    ///
    /// Pools appear in ascending name order (`ironwood`, `orchard`,
    /// `sapling`) with no whitespace. Enabling `serde_json/preserve_order`
    /// would reorder them to match insertion order, so the literal pins the
    /// order independently of the production serializer.
    const THREE_POOL_PAYLOAD: &[u8] = concat!(
        r#"{"ironwood":{"commitments":{"finalState":"cc"}},"#,
        r#""orchard":{"commitments":{"finalState":"bb"}},"#,
        r#""sapling":{"commitments":{"finalState":"aa"}}}"#,
    )
    .as_bytes();

    #[test]
    fn encode_json_payload_writes_three_pools_in_the_literal_wire_order()
    -> Result<(), super::QueryError> {
        let all = tree_state(TreeStateFrontiers {
            sapling_final_state: Some(vec![0xaa]),
            orchard_final_state: Some(vec![0xbb]),
            ironwood_final_state: Some(vec![0xcc]),
        });
        assert_eq!(all.encode_json_payload()?, THREE_POOL_PAYLOAD);

        let orchard_and_sapling = tree_state(TreeStateFrontiers {
            sapling_final_state: Some(vec![0xaa]),
            orchard_final_state: Some(vec![0xbb]),
            ironwood_final_state: None,
        });
        assert_eq!(
            orchard_and_sapling.encode_json_payload()?,
            concat!(
                r#"{"orchard":{"commitments":{"finalState":"bb"}},"#,
                r#""sapling":{"commitments":{"finalState":"aa"}}}"#,
            )
            .as_bytes()
        );
        Ok(())
    }

    #[test]
    fn encode_json_payload_writes_an_empty_object_for_no_active_pool()
    -> Result<(), super::QueryError> {
        let empty = tree_state(TreeStateFrontiers::default());
        assert_eq!(empty.encode_json_payload()?, b"{}");
        Ok(())
    }

    #[test]
    fn encode_json_payload_writes_the_documented_shape_for_all_pools()
    -> Result<(), super::QueryError> {
        let all = tree_state(TreeStateFrontiers {
            sapling_final_state: Some(vec![0xaa]),
            orchard_final_state: Some(vec![0xbb]),
            ironwood_final_state: Some(vec![0xcc]),
        });
        let payload: Value =
            serde_json::from_slice(&all.encode_json_payload()?).map_err(|source| {
                super::QueryError::ArtifactCorrupt {
                    family: super::ArtifactFamily::TreeState,
                    reason: source.to_string(),
                }
            })?;
        assert_eq!(
            payload,
            json!({
                "sapling": { "commitments": { "finalState": "aa" } },
                "orchard": { "commitments": { "finalState": "bb" } },
                "ironwood": { "commitments": { "finalState": "cc" } }
            })
        );
        Ok(())
    }
}
