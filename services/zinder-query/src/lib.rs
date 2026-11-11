//! Wallet and application query boundary for Zinder.
//!
//! This crate serves indexed artifacts from an admitted wallet-serving pair
//! through [`WalletServingQuery`] without mutating canonical storage or using
//! upstream nodes as a fallback for indexed history. Upstream access is limited to explicitly delegated sparse
//! tree-state fill and transaction broadcast.

use std::{num::NonZeroU32, time::Instant};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::mpsc;
use zinder_core::{
    BlockBlobArtifact, BlockHeader, BlockHeight, BlockHeightRange, BlockId, BlockSelector,
    ChainEpoch, ChainEpochId, ChainValuePoolsAtTip, CompactBlockArtifact,
    MAX_SUBTREE_ROOTS_PER_REQUEST, NetworkUpgradeActivations, RawTransactionBytes,
    ShieldedProtocol, SubtreeRootArtifact, SubtreeRootIndex, SubtreeRootRange,
    TransactionBlobArtifact, TransactionBroadcastOutcome, TransactionId, TransactionLocation,
    TransparentAddressBalance, TransparentAddressScriptHash, TransparentAddressTxIndexArtifact,
    TransparentOutPoint, TransparentOutputsByOutpointResponse, TransparentSpendsByOutpointResponse,
    TransparentUnspentOutput, TransparentUnspentOutputsByOutpointResponse,
    TransparentUtxoSetSummary, TxStatus,
};
use zinder_proto::capabilities::WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1;
use zinder_source::SourceError;
use zinder_store::{
    ArtifactFamily, ChainEventEnvelope, ChainEventStreamFamily, ChainEventStreamResume,
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
    WalletEndpointMetadata, WalletQueryGrpcAdapter, address_lookup_to_script_hash,
    block_header_by_selector_response, block_id_by_selector_response,
    broadcast_transaction_response, build_transparent_address_tx_ids_chunk,
    build_transparent_address_tx_ids_header, build_transparent_unspent_output_message,
    build_transparent_unspent_outputs_header, build_wallet_server_info, chain_events_response,
    chain_value_pools_at_tip_response, compact_block_response, full_block_response,
    latest_tree_state_checkpoint_response, network_upgrade_activations_response,
    status_from_query_error, subtree_roots_response, transaction_response,
    transparent_address_tx_ids_response, transparent_address_unspent_outputs_response,
    transparent_outputs_by_outpoint_response, transparent_spends_by_outpoint_response,
    transparent_unspent_outputs_by_outpoint_response, tree_state_at_response,
    visible_tip_block_response,
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
    /// [`zinder_core::MinedTransactionChainContext`] enrichment, [`TxStatus::NotFound`] when the
    /// transaction is not visible in the canonical chain, and
    /// [`QueryError`] for storage/upstream failures. An `at_epoch_id` pin is
    /// a canonical-chain read and never consults live mempool state;
    /// mempool fall-through is the caller's responsibility on this trait.
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
            QueryError::Store(StoreError::InvalidChainEpochArtifacts { reason: "probe" }),
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
}
