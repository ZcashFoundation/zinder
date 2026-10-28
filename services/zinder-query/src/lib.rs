//! Wallet and application query boundary for Zinder.
//!
//! This crate serves indexed artifacts through [`ChainEpochReadApi`] without
//! mutating canonical storage or using upstream nodes as a fallback for
//! indexed history. Upstream access is limited to explicitly delegated sparse
//! tree-state fill and transaction broadcast.

use std::{collections::HashSet, fmt, num::NonZeroU32, sync::Arc, time::Instant};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{OnceCell, mpsc};
use zinder_core::{
    BlockBlobArtifact, BlockHeader, BlockHeight, BlockHeightRange, BlockId, BlockSelector,
    CanonicalHistoryBounds, ChainEpoch, ChainEpochId, ChainValuePoolsAtTip, CompactBlockArtifact,
    MAX_RAW_TRANSACTION_BYTES, MAX_SUBTREE_ROOTS_PER_REQUEST, MinedTransaction,
    MinedTransactionChainContext, NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootArtifact, SubtreeRootIndex, SubtreeRootRange, TransactionBlobArtifact,
    TransactionBroadcastOutcome, TransactionId, TransactionLocation, TransparentAddressBalance,
    TransparentAddressScriptHash, TransparentAddressTxIndexArtifact, TransparentOutPoint,
    TransparentOutputEntry, TransparentOutputsByOutpointResponse, TransparentSpendEntry,
    TransparentSpendsByOutpointResponse, TransparentUnspentOutput,
    TransparentUnspentOutputsByOutpointResponse, TransparentUtxoSetSummary, TxStatus,
};
use zinder_materialized_views::{
    MaterializedViewState, MaterializedViewStore, MaterializedViewStoreReadSnapshot,
    TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_CONSUMER_NAME,
    TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME, TransparentAddressTransactionHistoryConsumer,
    TransparentAddressTransactionHistoryPageRequest, TransparentOutpointSpendConsumer,
};
use zinder_proto::capabilities::{
    WALLET_ADDRESS_TRANSPARENT_HISTORY_V1, WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1,
    WALLET_READ_TRANSPARENT_SPENDS_V1,
};
use zinder_runtime::AuthenticatedChannel;
use zinder_source::{SourceError, TransactionBroadcaster, TreeStateUpstream};
use zinder_store::{
    AddressOutputIndexPageRequest, ArtifactFamily, BlockHashLookup, ChainEpochReadApi,
    ChainEventEnvelope, ChainEventHistoryRequest, ChainEventStreamFamily, ChainEventStreamResume,
    DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS, EventStreamStartPosition, StoreError,
    StreamCursorTokenV1,
};

mod grpc;
mod ingest_control;
mod native_wallet_endpoint_capabilities;
mod wallet_serving_pair;
mod wallet_serving_pair_publisher;
mod wallet_serving_query;

pub use grpc::{
    TransactionStatusLookup, WalletEndpointMetadata, WalletQueryGrpcAdapter,
    address_lookup_to_script_hash, block_header_by_selector_response,
    block_id_by_selector_response, broadcast_transaction_response,
    build_transparent_address_tx_ids_chunk, build_transparent_address_tx_ids_header,
    build_transparent_unspent_output_message, build_transparent_unspent_outputs_header,
    build_wallet_server_info, chain_events_response, chain_value_pools_at_tip_response,
    compact_block_response, full_block_response, latest_tree_state_checkpoint_response,
    network_upgrade_activations_response, status_from_query_error, subtree_roots_response,
    transaction_response, transparent_address_tx_ids_response,
    transparent_address_unspent_outputs_response, transparent_outputs_by_outpoint_response,
    transparent_spends_by_outpoint_response, transparent_unspent_outputs_by_outpoint_response,
    tree_state_at_response, visible_tip_block_response,
};
pub use ingest_control::{AdmittedIngestControl, IngestControlAdmissionError};
pub use native_wallet_endpoint_capabilities::{
    NativeWalletEndpointCapabilities, UpstreamNodeCapabilities,
};
pub use wallet_serving_pair::{
    CanonicalReader, WalletProjectionReader, WalletServingAdmissionError, WalletServingReadPair,
};
pub use wallet_serving_pair_publisher::{
    WalletServingConvergence, WalletServingPairConfig, WalletServingPairError,
    WalletServingPairPublisher, WalletServingPairSlot, WalletServingReadiness,
    spawn_wallet_ingest_control_readiness_probe, spawn_wallet_node_readiness_probe,
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
    /// Returns the immutable structural capability set derived when this query
    /// was composed and admitted.
    fn native_endpoint_capabilities(&self) -> &NativeWalletEndpointCapabilities;

    /// Returns the diagnostic capability snapshot from the exact upstream
    /// source handle installed in this query, when one exists.
    fn upstream_node_capabilities(&self) -> Option<&UpstreamNodeCapabilities>;

    /// Returns the network-upgrade activation table advertised by the
    /// configured upstream node.
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
    /// [`MinedTransactionChainContext`] enrichment, and [`TxStatus::NotFound`]
    /// only for a proven absence from the canonical chain of the returned
    /// epoch. A store whose history starts after a checkpoint cannot prove an
    /// absence and returns [`QueryError::CanonicalHistoryUnavailable`] on a
    /// miss; storage faults, a missing epoch, and a pin this store cannot
    /// serve are other [`QueryError`] values. This method never consults live
    /// mempool state; mempool fall-through is the caller's responsibility on
    /// this trait.
    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError>;

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

    /// Resolves a batch of canonical-chain transparent outpoints to their
    /// referenced outputs.
    ///
    /// Reads each unique outpoint from the canonical transparent-output
    /// index. Outpoints that do not resolve at the response's [`ChainEpoch`]
    /// return an entry with `prevout = None`. The response preserves input
    /// order; duplicate outpoints emit duplicate entries.
    ///
    async fn transparent_outputs_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutputsByOutpointResponse, QueryError>;

    /// Resolves a batch of canonical-chain transparent outpoints to where each
    /// was spent on the canonical chain.
    ///
    /// Reads the canonical spend-fact index. Outpoints unspent at the
    /// response's [`ChainEpoch`] produce no entry; consumers key results by
    /// `spent_outpoint`. Coinbase inputs spend no prevout and never appear.
    async fn transparent_spends_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentSpendsByOutpointResponse, QueryError>;

    /// Resolves a batch of transparent outpoints to their referenced output,
    /// keeping each only while it is unspent on the canonical chain at the
    /// response's [`ChainEpoch`] (gettxout-equivalent, null-if-spent).
    ///
    /// Composes the canonical output resolver and the canonical spend-fact
    /// reader at one pinned epoch. An outpoint emits an entry only when the
    /// output is present and carries no canonical spend; spent or never-existed
    /// outpoints produce no entry, so every entry's `output` is present.
    /// Consumers key results by `outpoint`; duplicate outpoints collapse to one
    /// entry. The read is canonical-only: mempool-aware unspent-ness composes
    /// with [`Self::transparent_spends_by_outpoint`]'s mempool counterpart.
    async fn transparent_unspent_outputs_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUnspentOutputsByOutpointResponse, QueryError>;

    /// Reads the complete unspent transparent output set for one transparent
    /// address script at a single pinned chain epoch.
    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError>;

    /// Reads a bounded page of transparent-address tx-history index
    /// artifacts.
    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
    ) -> Result<TransparentAddressTxIds, QueryError>;

    /// Sums the canonical confirmed balance across one or more transparent
    /// addresses at a single pinned chain epoch.
    ///
    /// Folds the complete unspent transparent output set of every requested
    /// address into one saturating `confirmed_zat` total. The returned
    /// [`TransparentAddressBalance`] carries `unconfirmed_delta_zat = 0`: the
    /// canonical read knows nothing about live mempool state. The wallet-side
    /// gRPC adapter overlays the signed mempool delta on top of this total
    /// when an ingest-control endpoint is wired.
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

    /// Aggregates the chain-wide transparent UTXO set at the settled tip.
    ///
    /// Folds the canonical current-UTXO projection into an unspent count and a
    /// total value (gettxoutsetinfo-equivalent) by a request-time full scan.
    /// The aggregate is taken at the resolved epoch's settled tip, where the
    /// projection is the settled-tip unspent set under the configured reorg
    /// policy; a deeper reorg fails closed. When `commitment_enabled`
    /// is set the same scan also folds the `LtHash16` homomorphic commitment.
    async fn transparent_utxo_set_summary(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUtxoSetSummary, QueryError>;

    /// Reads the tree state at exactly `height`.
    ///
    /// Served from a stored checkpoint when one exists at `height`, otherwise
    /// filled from the configured upstream node. The returned height always
    /// equals `height`.
    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError>;

    /// Reads the latest tree-state checkpoint at the visible chain epoch tip.
    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError>;

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
}

/// Query boundary backed by a [`ChainEpochReadApi`] implementation.
///
/// Pass `()` as the broadcaster to disable transaction broadcast.
#[derive(Clone)]
pub struct WalletQuery<ReadApi, Broadcaster = ()> {
    read_api: ReadApi,
    materialized_view_store: Option<MaterializedViewStore>,
    transaction_broadcaster: Broadcaster,
    options: WalletQueryOptions,
    network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    tree_state_upstream: Option<Arc<dyn TreeStateUpstream>>,
    native_endpoint_capabilities: NativeWalletEndpointCapabilities,
    // Temporary primary-store test compositions may defer this fixture
    // connection. Release composition admits `AdmittedIngestControl` first.
    legacy_ingest_control_proxy_endpoint: Option<String>,
    legacy_ingest_control_channel: Arc<OnceCell<AuthenticatedChannel>>,
}

impl<ReadApi: fmt::Debug, Broadcaster: fmt::Debug> fmt::Debug
    for WalletQuery<ReadApi, Broadcaster>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalletQuery")
            .field("read_api", &self.read_api)
            .field(
                "materialized_view_store",
                &self.materialized_view_store.is_some(),
            )
            .field("transaction_broadcaster", &self.transaction_broadcaster)
            .field("options", &self.options)
            .field(
                "network_upgrade_activations",
                &self.network_upgrade_activations,
            )
            .field("tree_state_upstream", &self.tree_state_upstream.is_some())
            .field(
                "native_endpoint_capabilities",
                &self.native_endpoint_capabilities,
            )
            .field(
                "legacy_ingest_control_proxy_configured",
                &self.legacy_ingest_control_proxy_endpoint.is_some(),
            )
            .field(
                "legacy_ingest_control_channel_initialized",
                &self.legacy_ingest_control_channel.get().is_some(),
            )
            .finish()
    }
}

/// Default maximum compact-block count returned by one range call.
pub const DEFAULT_MAX_COMPACT_BLOCK_RANGE: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);

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

/// Runtime options for [`WalletQuery`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletQueryOptions {
    /// Maximum compact-block count returned by one range call.
    pub max_compact_block_range: NonZeroU32,
    /// Maximum full-block count returned by one range call.
    pub max_full_block_range: NonZeroU32,
}

impl Default for WalletQueryOptions {
    fn default() -> Self {
        Self {
            max_compact_block_range: DEFAULT_MAX_COMPACT_BLOCK_RANGE,
            max_full_block_range: DEFAULT_MAX_FULL_BLOCK_RANGE,
        }
    }
}

impl<ReadApi, Broadcaster> WalletQuery<ReadApi, Broadcaster> {
    /// Creates a wallet query boundary backed by `read_api` and
    /// `transaction_broadcaster`.
    ///
    /// `network_upgrade_activations` is the node-discovered upgrade table
    /// that populates `MinedTransactionChainContext.consensus_branch_id` for mined
    /// transactions with the value actually active at the mined height on
    /// the configured network. In production it is shared via
    /// `ZebraJsonRpcSource::discover_network_upgrade_activations`; tests use
    /// `zinder_testkit::sample_regtest_upgrade_activations`.
    #[must_use]
    pub fn new(
        read_api: ReadApi,
        transaction_broadcaster: Broadcaster,
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
    ) -> Self {
        Self::with_options(
            read_api,
            transaction_broadcaster,
            network_upgrade_activations,
            WalletQueryOptions::default(),
        )
    }

    /// Creates a wallet query boundary with explicit runtime options.
    #[must_use]
    pub fn with_options(
        read_api: ReadApi,
        transaction_broadcaster: Broadcaster,
        network_upgrade_activations: Arc<NetworkUpgradeActivations>,
        options: WalletQueryOptions,
    ) -> Self {
        Self {
            read_api,
            materialized_view_store: None,
            transaction_broadcaster,
            options,
            network_upgrade_activations,
            tree_state_upstream: None,
            native_endpoint_capabilities:
                NativeWalletEndpointCapabilities::for_chain_epoch_read_api(),
            legacy_ingest_control_proxy_endpoint: None,
            legacy_ingest_control_channel: Arc::new(OnceCell::new()),
        }
    }

    /// Attaches the materialized-view projections used by wallet queries.
    #[must_use]
    pub fn with_materialized_view_store(
        mut self,
        materialized_view_store: MaterializedViewStore,
    ) -> Self {
        self.materialized_view_store = Some(materialized_view_store);
        self
    }

    /// Attaches the upstream node used to fill tree states at heights without a
    /// stored checkpoint. Without it, `tree_state_at` serves only stored
    /// checkpoint heights and returns `ArtifactUnavailable` for the gaps.
    #[must_use]
    pub fn with_tree_state_upstream(mut self, source: Arc<dyn TreeStateUpstream>) -> Self {
        self.tree_state_upstream = Some(source);
        self
    }
}

/// Outcome of the synchronous store probe in [`WalletQuery::tree_state_at`].
enum TreeStateProbe {
    /// A stored checkpoint sits exactly at the requested height.
    Stored(TreeState),
    /// No stored checkpoint at the requested height; fill from the upstream node.
    Fill {
        chain_epoch: ChainEpoch,
        block_id: BlockId,
        block_time_seconds: u32,
    },
}

#[allow(
    clippy::too_many_lines,
    reason = "The query implementation mirrors the WalletQueryApi contract one method at a time."
)]
#[async_trait]
impl<ReadApi, Broadcaster> WalletQueryApi for WalletQuery<ReadApi, Broadcaster>
where
    ReadApi: ChainEpochReadApi + Clone + Send + Sync + 'static,
    Broadcaster: TransactionBroadcaster + Clone,
{
    fn native_endpoint_capabilities(&self) -> &NativeWalletEndpointCapabilities {
        &self.native_endpoint_capabilities
    }

    fn upstream_node_capabilities(&self) -> Option<&UpstreamNodeCapabilities> {
        None
    }

    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, QueryError> {
        Ok((*self.network_upgrade_activations).clone())
    }

    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<VisibleTipBlock, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            Ok(VisibleTipBlock {
                chain_epoch,
                height: chain_epoch.visible_tip_height,
                block_hash: chain_epoch.visible_tip_hash,
            })
        }))
        .await;
        record_wallet_query_outcome("visible_tip_block", started_at, &query_outcome, None);
        query_outcome
    }

    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SettledTipBlock, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            Ok(SettledTipBlock {
                chain_epoch,
                height: chain_epoch.settled_tip_height,
                block_hash: chain_epoch.settled_tip_hash,
            })
        }))
        .await;
        record_wallet_query_outcome("settled_tip_block", started_at, &query_outcome, None);
        query_outcome
    }

    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockIdAtEpoch, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let block_id = resolve_block_selector(&reader, selector)?;
            Ok(BlockIdAtEpoch {
                chain_epoch,
                block_id,
            })
        }))
        .await;
        record_wallet_query_outcome("block_id_by_selector", started_at, &query_outcome, None);
        query_outcome
    }

    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeaderAtEpoch, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let block_id = resolve_block_selector(&reader, selector)?;
            let block = reader.block_header_at(block_id.height)?.ok_or_else(|| {
                block_height_artifact_unavailable(ArtifactFamily::BlockHeader, block_id.height)
            })?;
            Ok(BlockHeaderAtEpoch {
                chain_epoch,
                block_header: block.into_header(),
            })
        }))
        .await;
        record_wallet_query_outcome("block_header_by_selector", started_at, &query_outcome, None);
        query_outcome
    }

    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlock, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let compact_block = match reader.compact_block_at(height) {
                Ok(Some(compact_block)) => compact_block,
                Ok(None)
                | Err(
                    StoreError::ArtifactMissing {
                        family: ArtifactFamily::CompactBlock,
                        ..
                    }
                    | StoreError::CanonicalHistoryUnavailable { .. },
                ) => {
                    return Err(block_height_artifact_unavailable(
                        ArtifactFamily::CompactBlock,
                        height,
                    ));
                }
                Err(error) => return Err(QueryError::Store(error)),
            };

            Ok(CompactBlock {
                chain_epoch,
                compact_block,
            })
        }))
        .await;
        record_wallet_query_outcome("compact_block_at", started_at, &query_outcome, None);
        query_outcome
    }

    async fn transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransactionStatus, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let activations = self.network_upgrade_activations.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let Some(artifact) = reader
                .transaction_facts_or_proven_absence(transaction_id)
                .map_err(transaction_lookup_store_error)?
            else {
                return Ok(TransactionStatus {
                    chain_epoch,
                    status: TxStatus::NotFound,
                });
            };
            let block_time = reader
                .block_header_at(artifact.location.block_height)?
                .map(|block| block.block_time)
                .unwrap_or_default();
            let consensus_branch_id =
                activations.consensus_branch_id_at(artifact.location.block_height);
            let chain_context = MinedTransactionChainContext::from_response_epoch(
                &chain_epoch,
                artifact.location.block_height,
                consensus_branch_id,
                block_time,
            );
            let raw_transaction_bytes = reader
                .transaction_blob_by_id(transaction_id)?
                .map(|blob| blob.raw_transaction_bytes);
            Ok(TransactionStatus {
                chain_epoch,
                status: TxStatus::Mined(MinedTransaction::new(
                    artifact.location,
                    chain_context,
                    raw_transaction_bytes,
                )),
            })
        }))
        .await;
        record_wallet_query_outcome("transaction_by_id", started_at, &query_outcome, None);
        query_outcome
    }

    async fn transaction_at_block_index(
        &self,
        height: BlockHeight,
        tx_index: u64,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Transaction, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let tx_index_in_block = u32::try_from(tx_index).map_err(|_| {
                artifact_unavailable(
                    ArtifactFamily::BlockTransactionIndex,
                    ArtifactKey::BlockTransactionIndex { height, tx_index },
                )
            })?;
            let transaction_id = reader
                .transaction_id_at_block_index(height, tx_index_in_block)?
                .ok_or_else(|| {
                    artifact_unavailable(
                        ArtifactFamily::BlockTransactionIndex,
                        ArtifactKey::BlockTransactionIndex { height, tx_index },
                    )
                })?;
            let transaction = reader
                .transaction_location_by_id(transaction_id)?
                .ok_or_else(|| {
                    artifact_unavailable(
                        ArtifactFamily::TransactionLocation,
                        ArtifactKey::TransactionId(transaction_id),
                    )
                })?;

            Ok(Transaction {
                chain_epoch,
                transaction,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "transaction_at_block_index",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn raw_transaction(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<RawTransaction, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let transaction = reader
                .transaction_blob_by_id(transaction_id)?
                .ok_or_else(|| {
                    artifact_unavailable(
                        ArtifactFamily::TransactionBlob,
                        ArtifactKey::TransactionId(transaction_id),
                    )
                })?;

            Ok(RawTransaction {
                chain_epoch,
                transaction,
            })
        }))
        .await;
        record_wallet_query_outcome("raw_transaction", started_at, &query_outcome, None);
        query_outcome
    }

    async fn transparent_outputs_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutputsByOutpointResponse, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();

            let prevouts_by_outpoint = reader.transparent_outputs_by_outpoints(&outpoints)?;

            let mut entries = Vec::with_capacity(outpoints.len());
            for outpoint in outpoints {
                let prevout = prevouts_by_outpoint
                    .get(&outpoint)
                    .cloned()
                    .map(zinder_core::TransparentOutputArtifact::into_output);
                entries.push(TransparentOutputEntry {
                    outpoint,
                    output: prevout,
                });
            }

            Ok(TransparentOutputsByOutpointResponse {
                chain_epoch,
                entries,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "transparent_outputs_by_outpoint",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn transparent_spends_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentSpendsByOutpointResponse, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let materialized_view_store = self.materialized_view_store.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();

            let canonical_spends = reader.transparent_spend_facts_by_outpoints(&outpoints)?;

            let mut spends = Vec::with_capacity(outpoints.len());
            let mut seen_outpoints = HashSet::with_capacity(outpoints.len());
            let mut unresolved_outpoints = Vec::new();
            for outpoint in &outpoints {
                if let Some(spend) = canonical_spends.get(outpoint) {
                    if seen_outpoints.insert(*outpoint) {
                        spends.push(TransparentSpendEntry::from_spend_fact(spend));
                    }
                } else {
                    unresolved_outpoints.push(*outpoint);
                }
            }

            if !unresolved_outpoints.is_empty()
                && let Some(deleted_through_height) = reader
                    .transparent_retention_deleted_through_height()
                    .map_err(QueryError::Store)?
            {
                let materialized_view_store =
                    materialized_view_store.ok_or(QueryError::MaterializedViewUnavailable {
                        capability: WALLET_READ_TRANSPARENT_SPENDS_V1,
                    })?;
                materialized_view_store.try_catch_up()?;
                let snapshot = materialized_view_store.read_snapshot();
                for materialized_spend in resolve_materialized_transparent_spends(
                    &snapshot,
                    &reader,
                    deleted_through_height,
                    &unresolved_outpoints,
                )? {
                    if seen_outpoints.insert(materialized_spend.spent_outpoint) {
                        spends.push(materialized_spend);
                    }
                }
            }

            Ok(TransparentSpendsByOutpointResponse {
                chain_epoch,
                spends,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "transparent_spends_by_outpoint",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn transparent_unspent_outputs_by_outpoint(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUnspentOutputsByOutpointResponse, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let entries = reader.transparent_unspent_outputs_by_outpoints(&outpoints)?;

            Ok(TransparentUnspentOutputsByOutpointResponse {
                chain_epoch,
                entries,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "transparent_unspent_outputs_by_outpoint",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockRange, QueryError> {
        let started_at = Instant::now();
        let requested_blocks = block_range.into_iter().len();
        if let Err(error) = validate_block_range(block_range, self.options.max_compact_block_range)
        {
            let query_outcome = Err(error);
            record_wallet_query_outcome(
                "compact_blocks_in_range",
                started_at,
                &query_outcome,
                Some(requested_blocks),
            );
            return query_outcome;
        }

        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let compact_blocks = reader
                .compact_blocks_in_range(block_range)
                .map_err(|error| {
                    map_height_artifact_store_error(
                        error,
                        ArtifactFamily::CompactBlock,
                        block_range.start,
                    )
                })?;
            let mut available_compact_blocks = Vec::with_capacity(compact_blocks.len());

            for (height, compact_block) in block_range.into_iter().zip(compact_blocks) {
                let Some(compact_block) = compact_block else {
                    return Err(block_height_artifact_unavailable(
                        ArtifactFamily::CompactBlock,
                        height,
                    ));
                };

                available_compact_blocks.push(compact_block);
            }

            Ok(CompactBlockRange {
                chain_epoch,
                block_range,
                compact_blocks: available_compact_blocks,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "compact_blocks_in_range",
            started_at,
            &query_outcome,
            Some(requested_blocks),
        );
        query_outcome
    }

    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlock, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let block_blob = reader.block_blob_at(height)?.ok_or_else(|| {
                block_height_artifact_unavailable(ArtifactFamily::BlockBlob, height)
            })?;

            Ok(FullBlock {
                chain_epoch,
                block_blob,
            })
        }))
        .await;
        record_wallet_query_outcome("full_block_at", started_at, &query_outcome, None);
        query_outcome
    }

    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<FullBlockStream, QueryError> {
        let started_at = Instant::now();
        let requested_blocks = block_range.into_iter().len();
        let query_outcome = spawn_full_block_stream(
            self.read_api.clone(),
            self.options.max_full_block_range,
            block_range,
            at_epoch_id,
        )
        .await;
        record_wallet_query_outcome(
            "full_blocks_in_range",
            started_at,
            &query_outcome,
            Some(requested_blocks),
        );
        query_outcome
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: TransparentAddressUnspentOutputsRequest,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressUnspentOutputs, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let at_epoch = match at_epoch_id {
                Some(_) => Some(open_chain_epoch_reader(&read_api, at_epoch_id)?.chain_epoch()),
                None => None,
            };
            let page = read_api
                .address_output_index_page(AddressOutputIndexPageRequest {
                    at_epoch,
                    address_script_hash: request.address_script_hash,
                    start_height: request.start_height,
                    max_entries: NonZeroU32::MAX,
                    from_cursor: None,
                })
                .map_err(QueryError::Store)?;

            Ok(TransparentAddressUnspentOutputs {
                chain_epoch: page.chain_epoch,
                outputs: page.outputs,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "transparent_address_unspent_outputs",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: TransparentAddressTxIdsInRangeRequest,
    ) -> Result<TransparentAddressTxIds, QueryError> {
        let started_at = Instant::now();
        if request.start_height > request.end_height {
            let outcome = Err(QueryError::InvalidBlockRange {
                start_height: request.start_height,
                end_height: request.end_height,
            });
            record_wallet_query_outcome(
                "transparent_address_tx_ids_in_range",
                started_at,
                &outcome,
                None,
            );
            return outcome;
        }

        let Some(materialized_view_store) = self.materialized_view_store.clone() else {
            let outcome = Err(QueryError::MaterializedViewUnavailable {
                capability: WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
            });
            record_wallet_query_outcome(
                "transparent_address_tx_ids_in_range",
                started_at,
                &outcome,
                None,
            );
            return outcome;
        };
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let chain_epoch = read_api
                .current_chain_epoch_reader()
                .map_err(QueryError::Store)?
                .chain_epoch();
            let canonical_fence =
                current_visible_chain_event_cursor_for_epoch(&read_api, chain_epoch)?;
            materialized_view_store.try_catch_up()?;
            let snapshot = materialized_view_store.read_snapshot();
            let materialized_fence = snapshot
                .get_chain_event_cursor(TRANSPARENT_ADDRESS_TRANSACTION_HISTORY_CONSUMER_NAME)?;
            if materialized_fence.as_deref()
                != canonical_fence
                    .as_ref()
                    .map(zinder_store::StreamCursorTokenV1::as_bytes)
            {
                return Err(QueryError::MaterializedViewUnavailable {
                    capability: WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
                });
            }
            let page = TransparentAddressTransactionHistoryConsumer::read_page_snapshot(
                &snapshot,
                TransparentAddressTransactionHistoryPageRequest {
                    address_script_hash: request.address_script_hash,
                    start_height: request.start_height,
                    end_height: request.end_height,
                    max_entries: request.max_entries,
                    from_cursor: request.from_cursor.as_ref(),
                    chain_event_fence: canonical_fence.as_ref(),
                    descending: request.descending,
                },
            )
            .map_err(map_transparent_history_page_error)?;
            drop(snapshot);
            Ok(TransparentAddressTxIds {
                chain_epoch,
                artifacts: page.artifacts,
                next_cursor: page.next_cursor,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "transparent_address_tx_ids_in_range",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn transparent_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentAddressBalance, QueryError> {
        let started_at = Instant::now();
        let address_count = u32::try_from(addresses.len())
            .ok()
            .filter(|count| (1..=MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES).contains(count));
        let Some(address_count) = address_count else {
            let outcome = Err(QueryError::TransparentBalanceAddressCountExceeded {
                requested: addresses.len(),
                maximum: MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
            });
            record_wallet_query_outcome("transparent_address_balance", started_at, &outcome, None);
            return outcome;
        };
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let at_epoch = at_epoch_id.is_some().then_some(chain_epoch);
            let mut confirmed_zat: u64 = 0;
            for address_script_hash in addresses {
                let page = read_api
                    .address_output_index_page(AddressOutputIndexPageRequest {
                        at_epoch,
                        address_script_hash,
                        start_height: BlockHeight::new(0),
                        max_entries: NonZeroU32::MAX,
                        from_cursor: None,
                    })
                    .map_err(QueryError::Store)?;
                for output in &page.outputs {
                    confirmed_zat = confirmed_zat.saturating_add(output.value_zat);
                }
            }
            Ok(TransparentAddressBalance {
                confirmed_zat,
                unconfirmed_delta_zat: 0,
                address_count,
                chain_epoch,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "transparent_address_balance",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn transparent_utxo_set_summary(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUtxoSetSummary, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            reader
                .transparent_utxo_set_summary(false)
                .map_err(QueryError::Store)
        }))
        .await;
        record_wallet_query_outcome(
            "transparent_utxo_set_summary",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let probe = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            if height > chain_epoch.visible_tip_height {
                return Err(block_height_artifact_unavailable(
                    ArtifactFamily::TreeState,
                    height,
                ));
            }

            let stored = match reader.tree_state_checkpoint_at_or_before(height) {
                Ok(stored) => stored,
                Err(StoreError::ArtifactMissing {
                    family: ArtifactFamily::TreeState,
                    ..
                }) => None,
                Err(error @ StoreError::CanonicalHistoryUnavailable { .. }) => {
                    return Err(map_height_artifact_store_error(
                        error,
                        ArtifactFamily::TreeState,
                        height,
                    ));
                }
                Err(error) => return Err(QueryError::Store(error)),
            };
            if let Some(stored) = stored
                && stored.height == height
            {
                return Ok(TreeStateProbe::Stored(TreeState {
                    chain_epoch,
                    height: stored.height,
                    block_hash: stored.block_hash,
                    block_time_seconds: stored.block_time_seconds,
                    payload_bytes: stored.payload_bytes,
                }));
            }

            let block = reader
                .block_header_at(height)
                .map_err(|error| {
                    map_height_artifact_store_error(error, ArtifactFamily::TreeState, height)
                })?
                .ok_or_else(|| {
                    block_height_artifact_unavailable(ArtifactFamily::TreeState, height)
                })?;
            Ok(TreeStateProbe::Fill {
                chain_epoch,
                block_id: BlockId::new(height, block.block_hash),
                block_time_seconds: u32::try_from(block.block_time).map_err(|_| {
                    QueryError::ArtifactCorrupt {
                        family: ArtifactFamily::BlockHeader,
                        reason: "canonical block time is outside the u32 range".to_owned(),
                    }
                })?,
            })
        }))
        .await;

        let query_outcome = match probe {
            Ok(TreeStateProbe::Stored(tree_state)) => Ok(tree_state),
            Ok(TreeStateProbe::Fill {
                chain_epoch,
                block_id,
                block_time_seconds,
            }) => match self.tree_state_upstream.as_ref() {
                Some(source) => {
                    let height = block_id.height;
                    let block_hash = block_id.hash;
                    source
                        .fetch_tree_state_for_block(block_id)
                        .await
                        .map_err(QueryError::Node)
                        .and_then(|source_tree_state| {
                            if source_tree_state.block_id != block_id {
                                return Err(QueryError::Node(
                                    SourceError::SourceProtocolMismatch {
                                        reason: "tree-state source identity does not match the canonical block",
                                    },
                                ));
                            }
                            if source_tree_state.block_time_seconds != block_time_seconds {
                                return Err(QueryError::Node(
                                    SourceError::SourceProtocolMismatch {
                                        reason: "tree-state source time does not match the canonical block",
                                    },
                                ));
                            }
                            Ok(TreeState {
                                chain_epoch,
                                height,
                                block_hash,
                                block_time_seconds,
                                payload_bytes: source_tree_state.payload_bytes,
                            })
                        })
                }
                None => Err(block_height_artifact_unavailable(
                    ArtifactFamily::TreeState,
                    block_id.height,
                )),
            },
            Err(error) => Err(error),
        };
        record_wallet_query_outcome("tree_state_at", started_at, &query_outcome, None);
        query_outcome
    }

    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeState, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();

            let tree_state = match reader.latest_tree_state_checkpoint() {
                Ok(Some(tree_state)) => tree_state,
                Ok(None)
                | Err(StoreError::ArtifactMissing {
                    family: ArtifactFamily::TreeState,
                    ..
                }) => {
                    return Err(block_height_artifact_unavailable(
                        ArtifactFamily::TreeState,
                        chain_epoch.visible_tip_height,
                    ));
                }
                Err(error) => return Err(QueryError::Store(error)),
            };

            Ok(TreeState {
                chain_epoch,
                height: tree_state.height,
                block_hash: tree_state.block_hash,
                block_time_seconds: tree_state.block_time_seconds,
                payload_bytes: tree_state.payload_bytes,
            })
        }))
        .await;
        record_wallet_query_outcome(
            "latest_tree_state_checkpoint",
            started_at,
            &query_outcome,
            None,
        );
        query_outcome
    }

    async fn subtree_roots(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<SubtreeRoots, QueryError> {
        let started_at = Instant::now();
        if let Err(error) = validate_subtree_root_range(subtree_root_range) {
            let query_outcome = Err(error);
            record_wallet_query_outcome("subtree_roots_in_range", started_at, &query_outcome, None);
            return query_outcome;
        }
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let reader = open_chain_epoch_reader(&read_api, at_epoch_id)?;
            let chain_epoch = reader.chain_epoch();
            let completed_subtree_count =
                completed_subtree_count(chain_epoch, subtree_root_range.protocol);

            if subtree_root_range.start_index.value() >= completed_subtree_count {
                return Ok(SubtreeRoots {
                    chain_epoch,
                    protocol: subtree_root_range.protocol,
                    start_index: subtree_root_range.start_index,
                    subtree_roots: Vec::new(),
                });
            }

            let available_entries = completed_subtree_count
                .saturating_sub(subtree_root_range.start_index.value())
                .min(subtree_root_range.max_entries.get());
            let available_entries = NonZeroU32::new(available_entries).ok_or_else(|| {
                subtree_root_artifact_unavailable(
                    subtree_root_range.protocol,
                    subtree_root_range.start_index,
                )
            })?;
            let available_range = SubtreeRootRange::new(
                subtree_root_range.protocol,
                subtree_root_range.start_index,
                available_entries,
            );
            let subtree_roots = reader.subtree_roots(available_range)?;
            let mut available_subtree_roots = Vec::with_capacity(subtree_roots.len());

            for (subtree_index, subtree_root) in available_range.into_iter().zip(subtree_roots) {
                let Some(subtree_root) = subtree_root else {
                    return Err(subtree_root_artifact_unavailable(
                        subtree_root_range.protocol,
                        subtree_index,
                    ));
                };

                available_subtree_roots.push(subtree_root);
            }

            Ok(SubtreeRoots {
                chain_epoch,
                protocol: subtree_root_range.protocol,
                start_index: subtree_root_range.start_index,
                subtree_roots: available_subtree_roots,
            })
        }))
        .await;
        record_wallet_query_outcome("subtree_roots_in_range", started_at, &query_outcome, None);
        query_outcome
    }

    async fn chain_events(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEvents, QueryError> {
        let started_at = Instant::now();
        let read_api = self.read_api.clone();
        let query_outcome = join_blocking(tokio::task::spawn_blocking(move || {
            let event_envelopes = read_api
                .chain_event_history(ChainEventHistoryRequest::new_for_family(
                    from_cursor.as_ref(),
                    family,
                    DEFAULT_MAX_CHAIN_EVENT_HISTORY_EVENTS,
                ))
                .map_err(map_chain_event_store_error)?;

            Ok(ChainEvents { event_envelopes })
        }))
        .await;
        record_wallet_query_outcome("chain_events", started_at, &query_outcome, None);
        query_outcome
    }

    async fn resolve_chain_events_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, QueryError> {
        let read_api = self.read_api.clone();
        join_blocking(tokio::task::spawn_blocking(move || {
            read_api
                .resolve_chain_event_stream_start(&start, requested_family)
                .map_err(map_chain_event_store_error)
        }))
        .await
    }

    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, QueryError> {
        let started_at = Instant::now();
        let broadcast_outcome = async {
            guard_broadcast_payload_size(&raw_transaction)?;
            self.transaction_broadcaster
                .broadcast_transaction(raw_transaction)
                .await
                .map_err(map_broadcast_source_error)
        }
        .await;
        record_wallet_query_outcome(
            "broadcast_transaction",
            started_at,
            &broadcast_outcome,
            None,
        );
        broadcast_outcome
    }
}

/// Resolves canonical misses from one verified materialized-view snapshot.
fn resolve_materialized_transparent_spends(
    snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    reader: &zinder_store::ChainEpochReader<'_>,
    deleted_through_height: BlockHeight,
    unresolved_outpoints: &[TransparentOutPoint],
) -> Result<Vec<TransparentSpendEntry>, QueryError> {
    let materialized_view_state = snapshot
        .consumer_state(TRANSPARENT_OUTPOINT_SPEND_CONSUMER_NAME)?
        .ok_or(QueryError::MaterializedViewUnavailable {
            capability: WALLET_READ_TRANSPARENT_SPENDS_V1,
        })?;
    validate_transparent_spend_materialized_view_coverage(
        reader,
        materialized_view_state,
        deleted_through_height,
    )?;
    let projected_spends = TransparentOutpointSpendConsumer::read_spends_by_outpoints_snapshot(
        snapshot,
        unresolved_outpoints,
    )?;
    let settled_tip_height = reader.chain_epoch().settled_tip_height;
    let mut resolved_spends = Vec::with_capacity(projected_spends.len());
    for outpoint in unresolved_outpoints {
        let Some(spend) = projected_spends.get(outpoint) else {
            continue;
        };
        if spend.spending_block_height <= settled_tip_height
            && transparent_spend_matches_canonical_header(reader, spend)?
        {
            resolved_spends.push(spend.clone());
        }
    }
    Ok(resolved_spends)
}

/// Requires verified spender coverage through every canonical deletion.
fn validate_transparent_spend_materialized_view_coverage(
    reader: &zinder_store::ChainEpochReader<'_>,
    materialized_view_state: MaterializedViewState,
    deleted_through_height: BlockHeight,
) -> Result<(), QueryError> {
    let Some(coverage) = materialized_view_state.coverage else {
        return Err(QueryError::MaterializedViewUnavailable {
            capability: WALLET_READ_TRANSPARENT_SPENDS_V1,
        });
    };
    let first_available_height = reader.canonical_history_bounds().first_available_height();
    let covers_deleted_facts = coverage.complete_from_height <= first_available_height
        && coverage.complete_through_height >= deleted_through_height;
    let materialized_view_tip_is_canonical = canonical_block_hash_matches(
        reader,
        materialized_view_state.tip_height,
        materialized_view_state.tip_hash,
    )?;
    let coverage_tip_is_canonical = canonical_block_hash_matches(
        reader,
        coverage.complete_through_height,
        coverage.complete_through_hash,
    )?;
    if !covers_deleted_facts || !materialized_view_tip_is_canonical || !coverage_tip_is_canonical {
        return Err(QueryError::MaterializedViewUnavailable {
            capability: WALLET_READ_TRANSPARENT_SPENDS_V1,
        });
    }
    Ok(())
}

fn transparent_spend_matches_canonical_header(
    reader: &zinder_store::ChainEpochReader<'_>,
    spend: &TransparentSpendEntry,
) -> Result<bool, QueryError> {
    canonical_block_hash_matches(
        reader,
        spend.spending_block_height,
        spend.spending_block_hash,
    )
}

fn canonical_block_hash_matches(
    reader: &zinder_store::ChainEpochReader<'_>,
    height: BlockHeight,
    expected_hash: zinder_core::BlockHash,
) -> Result<bool, QueryError> {
    Ok(reader
        .block_header_at(height)
        .map_err(QueryError::Store)?
        .is_some_and(|header| header.block_hash == expected_hash))
}

/// Resolves the authenticated visible-chain event fence for `expected_chain_epoch`.
///
/// The chain store can advance between the epoch read and `LiveTail` resolution.
/// Re-reading the epoch makes that race fail closed instead of pairing a history
/// projection from one chain branch with another branch's response epoch.
fn current_visible_chain_event_cursor_for_epoch(
    read_api: &(impl ChainEpochReadApi + ?Sized),
    expected_chain_epoch: ChainEpoch,
) -> Result<Option<StreamCursorTokenV1>, QueryError> {
    let fence = read_api
        .resolve_chain_event_stream_start(
            &EventStreamStartPosition::LiveTail,
            ChainEventStreamFamily::Visible,
        )
        .map_err(map_chain_event_store_error)?
        .cursor;
    let current_chain_epoch = read_api
        .current_chain_epoch_reader()
        .map_err(QueryError::Store)?
        .chain_epoch();
    if current_chain_epoch != expected_chain_epoch {
        return Err(QueryError::MaterializedViewUnavailable {
            capability: WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
        });
    }
    Ok(fence)
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Unknown materialized-view failures retain the query storage-error mapping."
)]
fn map_transparent_history_page_error(
    error: zinder_materialized_views::MaterializedViewStoreError,
) -> QueryError {
    match error {
        zinder_materialized_views::MaterializedViewStoreError::ConsumerCursorInvalid {
            reason: "cursor is bound to a different visible chain fence",
            ..
        } => QueryError::MaterializedViewUnavailable {
            capability: WALLET_ADDRESS_TRANSPARENT_HISTORY_V1,
        },
        zinder_materialized_views::MaterializedViewStoreError::ConsumerCursorInvalid {
            reason,
            ..
        } => QueryError::TransparentHistoryCursorInvalid { reason },
        error => QueryError::MaterializedViewStore(error),
    }
}

fn open_chain_epoch_reader<ReadApi>(
    read_api: &ReadApi,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<zinder_store::ChainEpochReader<'_>, QueryError>
where
    ReadApi: ChainEpochReadApi,
{
    let Some(requested_epoch_id) = at_epoch_id else {
        return read_api
            .current_chain_epoch_reader()
            .map_err(QueryError::Store);
    };

    read_api
        .chain_epoch_reader_at(requested_epoch_id)
        .map_err(|error| map_epoch_pin_store_error(error, requested_epoch_id))
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Only a missing pinned epoch changes category; all other storage failures keep the shared query storage mapping."
)]
fn map_epoch_pin_store_error(error: StoreError, chain_epoch_id: ChainEpochId) -> QueryError {
    match error {
        StoreError::ChainEpochMissing { .. } => {
            QueryError::ChainEpochPinUnavailable { chain_epoch_id }
        }
        _ => QueryError::Store(error),
    }
}

/// Awaits a `spawn_blocking` task and flattens the join error into a
/// `QueryError` so callers see one consistent error vocabulary.
///
/// Every query composition in this crate routes its storage reads through
/// this helper, so no `RocksDB` read ever runs on a reactor thread.
pub(crate) async fn join_blocking<Output>(
    handle: tokio::task::JoinHandle<Result<Output, QueryError>>,
) -> Result<Output, QueryError> {
    match handle.await {
        Ok(blocking_outcome) => blocking_outcome,
        Err(join_error) => Err(QueryError::BlockingTaskFailed {
            reason: join_error.to_string(),
        }),
    }
}

/// Validates the range, resolves the pinning epoch, then spawns the async
/// driver that streams it.
///
/// The chain epoch resolves before returning, so an over-cap request or an
/// epoch-pin failure is the returned error rather than a mid-stream one. The
/// driver holds no blocking thread across the client drain: each sub-read is
/// its own short blocking task and blocks flow to the sink with async sends.
async fn spawn_full_block_stream<ReadApi>(
    read_api: ReadApi,
    max_full_block_range: NonZeroU32,
    block_range: BlockHeightRange,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<FullBlockStream, QueryError>
where
    ReadApi: ChainEpochReadApi + Clone + Send + 'static,
{
    validate_block_range(block_range, max_full_block_range)?;

    let chain_epoch = resolve_full_block_epoch(read_api.clone(), at_epoch_id).await?;

    let (block_sender, block_receiver) = mpsc::channel(FULL_BLOCK_STREAM_CHANNEL_CAPACITY);
    tokio::spawn(drive_full_block_range(
        read_api,
        chain_epoch.id,
        block_range,
        block_sender,
    ));

    Ok(FullBlockStream {
        chain_epoch,
        blocks: block_receiver,
    })
}

/// Resolves the chain epoch that pins the whole full-block stream.
///
/// Runs in a short blocking task so the pin failure or store error surfaces as
/// the returned error before any block is streamed.
async fn resolve_full_block_epoch<ReadApi>(
    read_api: ReadApi,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<ChainEpoch, QueryError>
where
    ReadApi: ChainEpochReadApi + Send + 'static,
{
    join_blocking(tokio::task::spawn_blocking(move || {
        open_chain_epoch_reader(&read_api, at_epoch_id).map(|reader| reader.chain_epoch())
    }))
    .await
}

/// Async driver that streams one full-block range from a pinned epoch.
///
/// Walks the range in [`FULL_BLOCK_STREAM_SUB_READ_BLOCKS`] sub-reads, each in
/// its own short blocking task re-pinned to `chain_epoch_id`, and forwards each
/// blob to the sink with async sends. The first missing blob, store error,
/// blocking-task failure, or mid-stream epoch sweep ends the stream with a
/// terminal error after the blocks already sent; a dropped receiver stops the
/// walk on the next send.
async fn drive_full_block_range<ReadApi>(
    read_api: ReadApi,
    chain_epoch_id: ChainEpochId,
    block_range: BlockHeightRange,
    block_sender: mpsc::Sender<Result<BlockBlobArtifact, QueryError>>,
) where
    ReadApi: ChainEpochReadApi + Clone + Send + 'static,
{
    let last_height = block_range.end.value();
    let mut sub_read_start = block_range.start.value();
    loop {
        let sub_read_end = sub_read_start
            .saturating_add(FULL_BLOCK_STREAM_SUB_READ_BLOCKS - 1)
            .min(last_height);
        let sub_range = BlockHeightRange::inclusive(
            BlockHeight::new(sub_read_start),
            BlockHeight::new(sub_read_end),
        );
        let block_blobs =
            match read_full_block_sub_range(read_api.clone(), chain_epoch_id, sub_range).await {
                Ok(block_blobs) => block_blobs,
                Err(error) => {
                    let _ = block_sender.send(Err(error)).await;
                    return;
                }
            };
        if !forward_full_block_sub_range(&block_sender, sub_range, block_blobs).await {
            return;
        }
        if sub_read_end == last_height {
            return;
        }
        sub_read_start = sub_read_end.saturating_add(1);
    }
}

/// Reads one epoch-pinned sub-range of block blobs in a short blocking task.
///
/// Re-pins `chain_epoch_id` per call so a mid-stream sweep of the epoch fails
/// this read with [`QueryError::ChainEpochPinUnavailable`]; a panic in the walk
/// surfaces as [`QueryError::BlockingTaskFailed`] rather than silent truncation.
async fn read_full_block_sub_range<ReadApi>(
    read_api: ReadApi,
    chain_epoch_id: ChainEpochId,
    sub_range: BlockHeightRange,
) -> Result<Vec<Option<BlockBlobArtifact>>, QueryError>
where
    ReadApi: ChainEpochReadApi + Send + 'static,
{
    join_blocking(tokio::task::spawn_blocking(move || {
        read_api
            .chain_epoch_reader_at(chain_epoch_id)
            .map_err(|error| map_epoch_pin_store_error(error, chain_epoch_id))?
            .block_blobs_in_range(sub_range)
            .map_err(|error| {
                map_height_artifact_store_error(error, ArtifactFamily::BlockBlob, sub_range.start)
            })
    }))
    .await
}

/// Forwards one sub-read's blobs to the sink in ascending height order.
///
/// Returns `true` when every blob was delivered and the walk should continue. A
/// missing blob is sent as a terminal [`ArtifactFamily::BlockBlob`] unavailable
/// error and returns `false`; a dropped receiver also returns `false`.
async fn forward_full_block_sub_range(
    block_sender: &mpsc::Sender<Result<BlockBlobArtifact, QueryError>>,
    sub_range: BlockHeightRange,
    block_blobs: Vec<Option<BlockBlobArtifact>>,
) -> bool {
    for (height, block_blob) in sub_range.into_iter().zip(block_blobs) {
        let Some(block_blob) = block_blob else {
            let _ = block_sender
                .send(Err(block_height_artifact_unavailable(
                    ArtifactFamily::BlockBlob,
                    height,
                )))
                .await;
            return false;
        };
        if block_sender.send(Ok(block_blob)).await.is_err() {
            return false;
        }
    }
    true
}

fn map_broadcast_source_error(error: SourceError) -> QueryError {
    if matches!(error, SourceError::TransactionBroadcastDisabled) {
        QueryError::TransactionBroadcastDisabled
    } else {
        QueryError::Node(error)
    }
}

fn guard_broadcast_payload_size(raw_transaction: &RawTransactionBytes) -> Result<(), QueryError> {
    let actual = raw_transaction.len();
    if actual > MAX_RAW_TRANSACTION_BYTES {
        return Err(QueryError::BroadcastTransactionTooLarge {
            actual,
            maximum: MAX_RAW_TRANSACTION_BYTES,
        });
    }
    Ok(())
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Only cursor-specific store errors become query cursor errors; all other current and future storage failures remain storage failures."
)]
fn map_chain_event_store_error(error: StoreError) -> QueryError {
    match error {
        StoreError::ChainEventCursorInvalid { reason } => {
            QueryError::ChainEventCursorInvalid { reason }
        }
        StoreError::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        } => QueryError::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        },
        _ => QueryError::Store(error),
    }
}

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
/// shares one description template; the compact-block-range histogram is
/// `WalletQuery`-specific and stays registered here.
pub fn describe_request_metrics() {
    zinder_runtime::describe_rpc_metrics(QUERY_RPC_METRICS, "WalletQuery");
    metrics::describe_histogram!(
        "zinder_query_compact_block_range_block_count",
        metrics::Unit::Count,
        "Number of blocks returned by a single CompactBlockRange response, labelled \
         by status. Used to monitor request fan-out and page sizing."
    );
}

fn record_wallet_query_outcome<Response>(
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
        Some(QueryError::TransactionBroadcastDisabled) => "transaction_broadcast_disabled",
        Some(QueryError::BroadcastTransactionTooLarge { .. }) => "broadcast_transaction_too_large",
        Some(QueryError::MaterializedViewUnavailable { .. }) => "materialized_view_unavailable",
        Some(QueryError::EndpointCapabilityUnavailable { .. }) => "endpoint_capability_unavailable",
        Some(QueryError::BlockingTaskFailed { .. }) => "blocking_task_failed",
        Some(QueryError::ArtifactCorrupt { .. }) => "artifact_corrupt",
        Some(QueryError::BlockNotInBestChain) => "block_not_in_best_chain",
        Some(QueryError::Store(_)) => "store",
        Some(QueryError::MaterializedViewStore(_)) => "materialized_view_store",
        Some(QueryError::WalletProjectionRead { .. }) => "wallet_projection_read",
        Some(QueryError::CanonicalStore(_)) => "canonical_store",
        Some(QueryError::WalletStore(_)) => "wallet_store",
        Some(QueryError::Node(_)) => "node",
    }
}

fn usize_to_u32_saturating(amount: usize) -> u32 {
    u32::try_from(amount).map_or(u32::MAX, |converted| converted)
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
    /// Inclusive height range requested.
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

/// Complete unspent transparent output set bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressUnspentOutputs {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Unspent outputs in ascending `(block_height, outpoint)` order.
    pub outputs: Vec<TransparentUnspentOutput>,
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
    /// Iterate newest-first when true.
    pub descending: bool,
}

/// Transparent-address tx-history page response bound to one chain epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressTxIds {
    /// Chain epoch used to answer the query.
    pub chain_epoch: ChainEpoch,
    /// Tx-history artifacts in the requested order.
    pub artifacts: Vec<TransparentAddressTxIndexArtifact>,
    /// Resume cursor when more entries may be available.
    pub next_cursor: Option<StreamCursorTokenV1>,
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
    /// Encoded tree-state payload bytes.
    pub payload_bytes: Vec<u8>,
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
    /// transaction lookup that missed cannot prove the transaction absent.
    #[error(
        "canonical history starts at height {first_available_height:?}; transaction absence cannot be proven"
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

    /// Transaction broadcast is disabled for this query handle.
    #[error("transaction broadcast is disabled")]
    TransactionBroadcastDisabled,

    /// Raw transaction exceeds the maximum serialized size accepted for broadcast.
    #[error("raw transaction is too large: {actual} bytes exceeds maximum {maximum}")]
    BroadcastTransactionTooLarge {
        /// Serialized length of the submitted transaction.
        actual: usize,
        /// Maximum accepted serialized length.
        maximum: usize,
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

    /// A blocking read task failed unexpectedly (panic or runtime shutdown).
    #[error("query read task failed: {reason}")]
    BlockingTaskFailed {
        /// Operator-facing failure reason.
        reason: String,
    },

    /// Canonical read API returned a storage error.
    #[error(transparent)]
    Store(#[from] StoreError),

    /// Materialized-view store returned a storage error.
    #[error(transparent)]
    MaterializedViewStore(#[from] zinder_materialized_views::MaterializedViewStoreError),

    /// Typed wallet-projection backend returned a storage failure.
    #[error("wallet projection read failed: {source}")]
    WalletProjectionRead {
        /// Backend-specific source retained without leaking its API contract.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Canonical store returned a storage or admission failure.
    #[error(transparent)]
    CanonicalStore(#[from] zinder_store::CanonicalStoreError),

    /// Wallet store returned a storage or admission failure.
    #[error(transparent)]
    WalletStore(#[from] zinder_wallet_rocksdb::RocksDbWalletError),

    /// Upstream node operation failed.
    #[error(transparent)]
    Node(#[from] SourceError),
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
            Self::ChainEventCursorInvalid { .. } => ErrorReason::ChainEventCursorInvalid,
            Self::TransparentHistoryCursorInvalid { .. } => {
                ErrorReason::TransparentHistoryCursorInvalid
            }
            Self::InvalidAddress { .. } => ErrorReason::InvalidAddress,
            Self::UnsupportedShieldedProtocol { .. } => ErrorReason::UnsupportedShieldedProtocol,
            Self::TransactionBroadcastDisabled => ErrorReason::BroadcastDisabled,
            Self::BroadcastTransactionTooLarge { .. } => ErrorReason::BroadcastTransactionTooLarge,
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
            Self::BlockingTaskFailed { .. } => ErrorReason::BlockingTaskFailed,
            Self::Node(source_error) if source_error.is_node_capability_missing() => {
                ErrorReason::NodeCapabilityMissing
            }
            Self::Node(_) => ErrorReason::NodeUnavailable,
            Self::MaterializedViewStore(_)
            | Self::Store(_)
            | Self::WalletProjectionRead { .. }
            | Self::CanonicalStore(_)
            | Self::WalletStore(_) => ErrorReason::StorageUnavailable,
        }
    }
}

/// Requires complete canonical history before a transaction lookup miss can
/// be reported as an absence.
pub(crate) fn require_complete_transaction_history(
    history_bounds: CanonicalHistoryBounds,
) -> Result<(), QueryError> {
    if history_bounds.intentionally_excludes(BlockHeight::new(1)) {
        return Err(QueryError::CanonicalHistoryUnavailable {
            first_available_height: history_bounds.first_available_height(),
        });
    }
    Ok(())
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Only the history floor changes category on a transaction lookup; every other storage failure keeps the shared mapping."
)]
fn transaction_lookup_store_error(error: StoreError) -> QueryError {
    match error {
        StoreError::CanonicalHistoryUnavailable {
            first_available_height,
            ..
        } => QueryError::CanonicalHistoryUnavailable {
            first_available_height,
        },
        _ => QueryError::Store(error),
    }
}

fn block_height_artifact_unavailable(family: ArtifactFamily, height: BlockHeight) -> QueryError {
    artifact_unavailable(family, ArtifactKey::BlockHeight(height))
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "BlockSelector is #[non_exhaustive]; new selector variants are a deliberate decision per the gap doc, not a default fall-through"
)]
fn resolve_block_selector(
    reader: &zinder_store::ChainEpochReader<'_>,
    selector: BlockSelector,
) -> Result<BlockId, QueryError> {
    match selector {
        BlockSelector::Height(height) => {
            let chain_epoch = reader.chain_epoch();
            if height > chain_epoch.visible_tip_height {
                return Err(QueryError::BlockNotInBestChain);
            }
            let block = reader
                .block_header_at(height)?
                .ok_or(QueryError::BlockNotInBestChain)?;
            Ok(BlockId::new(height, block.block_hash))
        }
        BlockSelector::Hash(hash) => match reader.block_hash_lookup(hash)? {
            BlockHashLookup::Resolved(block_id) => Ok(block_id),
            BlockHashLookup::NotInBestChain | BlockHashLookup::NotIndexed => {
                Err(QueryError::BlockNotInBestChain)
            }
        },
        _ => Err(QueryError::UnsupportedBlockSelector {
            reason: "selector variant has no canonical resolver",
        }),
    }
}

fn subtree_root_artifact_unavailable(
    protocol: ShieldedProtocol,
    index: SubtreeRootIndex,
) -> QueryError {
    artifact_unavailable(
        ArtifactFamily::SubtreeRoot,
        ArtifactKey::SubtreeRootIndex { protocol, index },
    )
}

fn artifact_unavailable(family: ArtifactFamily, key: ArtifactKey) -> QueryError {
    QueryError::ArtifactUnavailable { family, key }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Only intentional history absence and a matching missing artifact change category; every other current and future store failure remains a storage error."
)]
fn map_height_artifact_store_error(
    error: StoreError,
    family: ArtifactFamily,
    requested_height: BlockHeight,
) -> QueryError {
    match error {
        StoreError::CanonicalHistoryUnavailable { .. } => {
            block_height_artifact_unavailable(family, requested_height)
        }
        StoreError::ArtifactMissing {
            family: missing_family,
            ..
        } if missing_family == family => {
            block_height_artifact_unavailable(family, requested_height)
        }
        _ => QueryError::Store(error),
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

fn completed_subtree_count(chain_epoch: ChainEpoch, protocol: ShieldedProtocol) -> u32 {
    chain_epoch.tip_metadata.completed_subtree_count(protocol)
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
            QueryError::CanonicalHistoryUnavailable {
                first_available_height: BlockHeight::new(101),
            },
            QueryError::ChainEpochPinUnavailable {
                chain_epoch_id: ChainEpochId::new(1),
            },
            QueryError::UnsupportedChainEvent { event: "probe" },
            QueryError::UnsupportedBlockSelector { reason: "probe" },
            QueryError::UnsupportedTransactionStatus { reason: "probe" },
            QueryError::UnsupportedWalletEncoding {
                value_kind: "probe",
            },
            QueryError::TransactionBroadcastDisabled,
            QueryError::BroadcastTransactionTooLarge {
                actual: MAX_RAW_TRANSACTION_BYTES + 1,
                maximum: MAX_RAW_TRANSACTION_BYTES,
            },
            QueryError::MaterializedViewUnavailable {
                capability: "probe",
            },
            QueryError::EndpointCapabilityUnavailable {
                capability: "probe",
            },
            QueryError::BlockingTaskFailed {
                reason: "probe".to_owned(),
            },
            QueryError::Store(StoreError::NoVisibleChainEpoch),
            QueryError::MaterializedViewStore(
                zinder_materialized_views::MaterializedViewStoreError::InvalidOptions {
                    reason: "probe",
                },
            ),
            QueryError::WalletProjectionRead {
                source: Box::new(std::io::Error::other("probe")),
            },
            QueryError::Node(SourceError::InvalidBlockHashHex {
                reason: "probe".to_owned(),
            }),
        ]
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
    fn transaction_absence_requires_complete_history() -> Result<(), Box<dyn std::error::Error>> {
        assert!(require_complete_transaction_history(CanonicalHistoryBounds::complete()).is_ok());
        let checkpointed = CanonicalHistoryBounds::checkpointed(BlockId::new(
            BlockHeight::new(100),
            zinder_core::BlockHash::from_bytes([1; 32]),
        ))?;
        assert!(matches!(
            require_complete_transaction_history(checkpointed),
            Err(QueryError::CanonicalHistoryUnavailable {
                first_available_height
            }) if first_available_height == BlockHeight::new(101)
        ));
        let status = crate::status_from_query_error(&QueryError::CanonicalHistoryUnavailable {
            first_available_height: BlockHeight::new(101),
        });
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            zinder_proto::wire::canonical_history_first_available_height(&status)?,
            Some(BlockHeight::new(101))
        );
        Ok(())
    }

    #[test]
    fn subtree_root_range_limit_has_stable_metrics_classification() {
        let error = QueryError::SubtreeRootRangeTooLarge {
            requested: MAX_SUBTREE_ROOTS_PER_REQUEST.saturating_add(1),
            maximum: MAX_SUBTREE_ROOTS_PER_REQUEST,
        };

        assert_eq!(
            query_error_class(Some(&error)),
            "subtree_root_range_too_large"
        );
    }
}
