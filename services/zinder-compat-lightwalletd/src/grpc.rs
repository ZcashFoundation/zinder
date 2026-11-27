//! gRPC implementation for the vendored lightwalletd protocol.

pub(crate) mod light_wallet;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashSet, binary_heap::PeekMut},
    num::{NonZeroU16, NonZeroU32},
    pin::Pin,
    time::{Duration, Instant},
};

use prost::Message as _;
use thiserror::Error;
use tokio_stream as stream;
use tokio_stream::StreamExt as _;
use tonic::{Request, Response, Status};
use zinder_core::wire::WireDecodeError;
use zinder_core::{
    BlockHeight, BlockSelector, BroadcastAccepted, BroadcastDuplicate, BroadcastInvalidEncoding,
    BroadcastQueued, BroadcastRejected, BroadcastUnknown, ChainEpoch, ChainEpochId,
    CompactBlockArtifact, CompactTransaction, MAX_SUBTREE_ROOTS_PER_REQUEST, MinedTransaction,
    Network, NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol, SubtreeRootIndex,
    SubtreeRootRange, TransactionBroadcastOutcome, TransactionId, TransparentAddressScriptHash,
    TransparentAddressTxIndexArtifact, TransparentUnspentOutput, TxStatus,
    wire::{
        decode_internal_transaction_id, decode_rpc_block_hash_bytes, encode_bip70_chain_name,
        encode_internal_transaction_id, encode_rpc_block_hash_bytes, encode_rpc_block_hash_hex,
        encode_rpc_transaction_id_hex,
    },
};
use zinder_proto::compat::lightwalletd::{self, EncodedBlock, compact_tx_streamer_server};
use zinder_proto::v1::wallet::{self as wallet_proto, address_lookup};
use zinder_query::{
    ArtifactKey, MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES, PendingTransaction, QueryError,
    StorageReadError, SubtreeRoots, TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE, TransactionsInOrder,
    TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputs,
    TransparentAddressUnspentOutputsRequest, TreeState, WalletQueryApi, WalletServingQuery,
    address_lookup_to_script_hash, status_from_query_error,
};
use zinder_runtime::RepeatedFieldGuard;
use zinder_source::{NodeCapability, SourceError};
use zinder_source::{parse_transparent_address, transparent_address_matches_network};
use zinder_store::{
    ArtifactFamily, CompactBlockSpliceSelection, EncodedCompactBlockRangeRequest, MempoolEvent,
    RawBlobRetention,
};

use crate::block_methods::{
    self, BlockMethods, ChunkRead, ChunkReadFailure, ChunkReadOutcome, ChunkView,
    CompactBlockReads, EncodedChunkReadOutcome, RequestedBlock, TipCapture, requested_block,
    view_reaches,
};
use crate::compact_block_messages::CompactBlockShape;
use crate::mempool::{
    MempoolExclusions, MempoolSnapshotPage, MempoolSurfaceError, MempoolTransactionLookup,
    SharedMempoolSurface, SharedTipChangeWatcher, TipChangeWatcherError,
};
use crate::node_tip_witness::NodeTipWitness;
use crate::ping::LightwalletdPing;
use crate::request_limits::COMPACT_TX_STREAMER_FIELD_LIMITS;
use crate::stale_tip::StaleTipLimit;
use crate::tip_wait::TipWait;

/// A server-streaming response of the `CompactTxStreamer` adapters.
pub(crate) type GrpcStream<T> =
    Pin<Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Default number of subtree roots one native read returns while `GetSubtreeRoots` streams.
pub const DEFAULT_MAX_LIGHTWALLETD_SUBTREE_ROOTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);
/// Default maximum transparent UTXOs in one compatibility response.
pub const DEFAULT_MAX_LIGHTWALLETD_ADDRESS_UTXOS: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(99_999);
/// Maximum encoded reply-list size, applied to both UTXO methods.
const ADDRESS_UTXO_RESPONSE_MAX_BYTES: usize = 16 * 1024 * 1024;
/// Native per-request subtree-root cap expressed as a `NonZeroU32`.
const MAX_SUBTREE_ROOT_PAGE_SIZE: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(MAX_SUBTREE_ROOTS_PER_REQUEST - 1);
/// Page size used to drain the native mempool snapshot for lightwalletd.
const LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE: u32 = 1024;
/// Transactions a `GetTaddressTransactions` or `GetTaddressTxids` response
/// reads ahead of its client.
const TRANSPARENT_HISTORY_STREAM_CHANNEL_CAPACITY: usize = 8;
/// Subtree roots a `GetSubtreeRoots` response reads ahead of its client.
const SUBTREE_ROOT_STREAM_CHANNEL_CAPACITY: usize = 64;
/// Rows a `GetAddressUtxosStream` response merges ahead of its client.
const ADDRESS_UTXO_STREAM_CHANNEL_CAPACITY: usize = 64;
/// Outputs the first pages of one UTXO request's address streams share, so
/// up to eight distinct addresses read full 1,024-output first pages.
const ADDRESS_UTXO_FIRST_PAGE_BUDGET: usize = 8 * 1_024;
/// Fewest outputs the first page of one address's UTXO stream reads.
const ADDRESS_UTXO_MIN_FIRST_PAGE_SIZE: NonZeroU16 = NonZeroU16::MIN.saturating_add(15);
/// Maximum number of transaction-id suffixes accepted by one `GetMempoolTx` request.
pub(crate) const MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST: usize = 20_000;
/// Most distinct transactions one `GetMempoolStream` sends between tip changes.
///
/// The stream remembers each id it sent so it can skip repeats, and that set
/// only resets when the stream closes on a tip change. The bound keeps a
/// stalled tip from growing it without limit; it is far above what a Zcash
/// mempool turns over in one block interval.
const MAX_MEMPOOL_STREAM_TRANSACTIONS: usize = 100_000;
/// Outbound `RawTransaction` messages one `GetMempoolStream` task buffers ahead
/// of its gRPC transport before backpressuring its upstream subscription.
const MEMPOOL_STREAM_CHANNEL_CAPACITY: usize = 16;
/// Entries a mempool snapshot walk buffers ahead of its consumer before
/// backpressuring the next snapshot page read.
const MEMPOOL_SNAPSHOT_ENTRY_CHANNEL_CAPACITY: usize = 16;

/// Response bounds for [`LightwalletdGrpcAdapter`].
///
/// These bounds limit response materialization. They do not control advertised
/// compatibility support, which is derived only by
/// [`LightwalletdGrpcAdapter::from_admitted_compatibility_query`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LightwalletdResponseLimits {
    /// Roots one native read returns while `GetSubtreeRoots` streams, clamped to
    /// [`MAX_SUBTREE_ROOTS_PER_REQUEST`].
    ///
    /// It bounds memory per read, not the response: `GetSubtreeRoots` pages
    /// through every requested root, and `maxEntries = 0` means every
    /// remaining root, as in upstream lightwalletd.
    pub max_subtree_roots: NonZeroU32,
    /// Most UTXOs one `GetAddressUtxos` or `GetAddressUtxosStream` response
    /// returns.
    ///
    /// A positive `maxEntries` finishes the height containing that entry.
    /// Zero requests every matching UTXO. A page that exceeds this bound
    /// fails with `RESOURCE_EXHAUSTED`.
    pub max_address_utxos: NonZeroU32,
}

impl Default for LightwalletdResponseLimits {
    fn default() -> Self {
        Self {
            max_subtree_roots: DEFAULT_MAX_LIGHTWALLETD_SUBTREE_ROOTS,
            max_address_utxos: DEFAULT_MAX_LIGHTWALLETD_ADDRESS_UTXOS,
        }
    }
}

/// Structural failure while composing the compatibility transparent-address claim.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LightwalletdAdmissionError {
    /// The admitted query has neither retained transaction bytes nor a verified node byte source.
    #[error(
        "lightwalletd wallet compatibility requires retained transaction bytes or a verified node byte source; admitted retention is {retention}"
    )]
    TransactionRetentionUnavailable {
        /// Retention authenticated by the admitted canonical serving pair.
        retention: RawBlobRetention,
    },
}

/// gRPC adapter from [`WalletQueryApi`] to lightwalletd `CompactTxStreamer`.
#[derive(Clone)]
pub struct LightwalletdGrpcAdapter<QueryApi> {
    query_api: QueryApi,
    response_limits: LightwalletdResponseLimits,
    block_methods: BlockMethods,
    transparent_address_support: bool,
    mempool_surface: Option<SharedMempoolSurface>,
    tip_change_watcher: Option<SharedTipChangeWatcher>,
    ping: LightwalletdPing,
}

impl<QueryApi: std::fmt::Debug> std::fmt::Debug for LightwalletdGrpcAdapter<QueryApi> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LightwalletdGrpcAdapter")
            .field("query_api", &self.query_api)
            .field("response_limits", &self.response_limits)
            .field("block_methods", &self.block_methods)
            .field(
                "transparent_address_support",
                &self.transparent_address_support,
            )
            .field("mempool_surface", &self.mempool_surface.is_some())
            .field("tip_change_watcher", &self.tip_change_watcher.is_some())
            .field("ping_enabled", &self.ping.is_enabled())
            .finish_non_exhaustive()
    }
}

impl<QueryApi> LightwalletdGrpcAdapter<QueryApi> {
    /// Creates a lightwalletd-compatible gRPC adapter.
    ///
    /// `GetLightdInfo` derives `consensusBranchId`, `upgradeName`,
    /// `upgradeHeight`, and `saplingActivationHeight` from
    /// [`WalletQueryApi::network_upgrade_activations`] on each request.
    ///
    /// The returned adapter does not advertise mempool surfaces. Pair the
    /// adapter with [`LightwalletdGrpcAdapter::with_mempool_surface`] to
    /// expose `GetMempoolStream` and `GetMempoolTx`; without one, both
    /// methods return `Status::unavailable`.
    #[must_use]
    pub fn new(query_api: QueryApi) -> Self {
        Self::with_response_limits(query_api, LightwalletdResponseLimits::default())
    }

    /// Creates a lightwalletd-compatible gRPC adapter with explicit response limits.
    ///
    /// The generic adapter does not advertise transparent-address support.
    /// Production compatibility composition must use
    /// [`LightwalletdGrpcAdapter::from_admitted_compatibility_query`].
    #[must_use]
    pub fn with_response_limits(
        query_api: QueryApi,
        response_limits: LightwalletdResponseLimits,
    ) -> Self {
        Self {
            query_api,
            response_limits,
            block_methods: BlockMethods::default(),
            transparent_address_support: false,
            mempool_surface: None,
            tip_change_watcher: None,
            ping: LightwalletdPing::default(),
        }
    }

    /// Limits how many `GetBlockRange` and `GetBlockRangeNullifiers` streams
    /// this adapter and its clones serve at once.
    ///
    /// A request past the limit waits for a slot before any read, and fails
    /// with `UNAVAILABLE` when none returns in time. The default is
    /// [`crate::DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS`].
    #[must_use]
    pub fn with_max_concurrent_block_range_streams(mut self, max_streams: NonZeroU32) -> Self {
        self.block_methods = self
            .block_methods
            .with_max_concurrent_block_range_streams(max_streams);
        self
    }

    /// Sets how long a block-range request past the limit waits for a slot
    /// before it fails with `UNAVAILABLE`. The default is
    /// [`crate::DEFAULT_BLOCK_RANGE_ADMISSION_WAIT`].
    #[must_use]
    pub fn with_block_range_admission_wait(mut self, max_wait: Duration) -> Self {
        self.block_methods = self.block_methods.with_block_range_admission_wait(max_wait);
        self
    }

    /// Sets how old the served tip may be before `GetLatestBlock` and
    /// `GetLightdInfo` fail with `UNAVAILABLE`.
    ///
    /// The adapter defaults to [`StaleTipLimit::DISABLED`]; the
    /// `zinder-compat-lightwalletd` binary sets the configured limit.
    #[must_use]
    pub const fn with_stale_tip_limit(mut self, stale_tip_limit: StaleTipLimit) -> Self {
        self.block_methods.stale_tip_limit = stale_tip_limit;
        self
    }

    /// Sets the witness whose report of the writer's node decides whether an
    /// old served tip is the chain tip or a stale one.
    ///
    /// Without one, no tip is vouched for, so the stale-tip limit refuses
    /// every tip older than the limit.
    #[must_use]
    pub fn with_node_tip_witness(mut self, node_tip_witness: NodeTipWitness) -> Self {
        self.block_methods = self.block_methods.with_node_tip_witness(node_tip_witness);
        self
    }

    /// Lets a block read that names a height just past the visible tip wait
    /// briefly for that height instead of failing at once.
    ///
    /// Applies to `GetBlock`, `GetBlockNullifiers`, `GetBlockRange`,
    /// `GetBlockRangeNullifiers`, the encoded range methods, and, where this
    /// adapter serves it, `GetTreeState`, when they select by height. In
    /// light-wallet serving, it also bounds the wait when forwarded UTXO and
    /// transparent balance reads observe a node tip ahead of the served tip.
    /// The adapter defaults to [`TipWait::disabled`]; the
    /// `zinder-compat-lightwalletd` binary sets the configured wait.
    #[must_use]
    pub fn with_tip_wait(mut self, tip_wait: TipWait) -> Self {
        self.block_methods = self.block_methods.with_tip_wait(tip_wait);
        self
    }

    /// Wires a mempool read surface into the adapter.
    #[must_use]
    pub fn with_mempool_surface(mut self, mempool_surface: SharedMempoolSurface) -> Self {
        self.mempool_surface = Some(mempool_surface);
        self
    }

    /// Wires a tip-change watcher into the adapter so `GetMempoolStream`
    /// closes cleanly on each best-chain tip change, preserving the
    /// lightwalletd Go server's de-facto contract.
    #[must_use]
    pub fn with_tip_change_watcher(mut self, watcher: SharedTipChangeWatcher) -> Self {
        self.tip_change_watcher = Some(watcher);
        self
    }

    /// Sets how this adapter answers `Ping`. The default refuses it with
    /// `FAILED_PRECONDITION`, as lightwalletd does without
    /// `--ping-very-insecure`.
    #[must_use]
    pub fn with_ping(mut self, ping: LightwalletdPing) -> Self {
        self.ping = ping;
        self
    }

    fn into_field_guarded_server(
        self,
    ) -> RepeatedFieldGuard<compact_tx_streamer_server::CompactTxStreamerServer<Self>>
    where
        Self: compact_tx_streamer_server::CompactTxStreamer,
    {
        RepeatedFieldGuard::new(
            compact_tx_streamer_server::CompactTxStreamerServer::new(self)
                .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES),
            COMPACT_TX_STREAMER_FIELD_LIMITS,
        )
    }
}

impl<QueryApi> LightwalletdGrpcAdapter<QueryApi>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    /// Wraps this adapter in the generated tonic server type, behind the
    /// pre-decode bounds of [`COMPACT_TX_STREAMER_FIELD_LIMITS`].
    #[must_use]
    pub fn into_server(
        self,
    ) -> RepeatedFieldGuard<compact_tx_streamer_server::CompactTxStreamerServer<Self>> {
        self.into_field_guarded_server()
    }
}

impl<Broadcaster> LightwalletdGrpcAdapter<WalletServingQuery<Broadcaster>>
where
    Broadcaster: zinder_source::TransactionBroadcaster + Clone,
{
    /// Creates a compatibility adapter from an admitted canonical and wallet
    /// serving pair with retained transaction bytes or a verified node byte source.
    ///
    /// The resulting `taddrSupport` claim is fixed for this process. Later
    /// serving-pair lag or node outage is represented by the runtime readiness
    /// gate and never changes `GetLightdInfo`.
    pub fn from_admitted_compatibility_query(
        query_api: WalletServingQuery<Broadcaster>,
    ) -> Result<Self, LightwalletdAdmissionError> {
        let raw_blob_retention = query_api.admitted_raw_blob_retention();
        if !raw_blob_retention.retains_transaction_blobs()
            && !query_api.has_verified_cold_tx_source()
        {
            return Err(
                LightwalletdAdmissionError::TransactionRetentionUnavailable {
                    retention: raw_blob_retention,
                },
            );
        }
        Ok(Self {
            transparent_address_support: true,
            ..Self::new(query_api)
        })
    }
}

impl<QueryApi: Sync> LightwalletdGrpcAdapter<QueryApi> {
    /// Answers a `GetTransaction` whose canonical read missed from the live
    /// mempool, as lightwalletd does through Zebra's `getrawtransaction`.
    ///
    /// A pending transaction is returned at height 0, the protocol's
    /// "in the mempool" sentinel. A deployment without a mempool surface keeps
    /// `canonical_miss`, and so does a mempool miss after a canonical read
    /// that searched the chain of the same epoch. A mempool miss at another
    /// epoch is `UNAVAILABLE`, because the transaction may have been mined
    /// into a block this replica has not applied yet. A mempool failure is
    /// returned instead of `canonical_miss`, because the transaction may be
    /// pending.
    async fn pending_transaction(
        &self,
        transaction_id: TransactionId,
        canonical_miss: CanonicalTransactionMiss,
    ) -> Result<lightwalletd::RawTransaction, Status> {
        let Some(mempool_surface) = self.mempool_surface.as_ref() else {
            return Err(canonical_miss.into_status());
        };
        let lookup = mempool_surface
            .mempool_transaction(transaction_id)
            .await
            .map_err(status_from_mempool_surface_error)?;
        match (lookup, canonical_miss) {
            (MempoolTransactionLookup::Pending(entry), _) => Ok(mempool_raw_transaction(&entry)),
            (
                MempoolTransactionLookup::Absent { chain_epoch_id },
                CanonicalTransactionMiss::NotInChain { at_epoch_id },
            ) if chain_epoch_id == at_epoch_id => {
                Err(CanonicalTransactionMiss::NotInChain { at_epoch_id }.into_status())
            }
            (
                MempoolTransactionLookup::Absent { chain_epoch_id },
                CanonicalTransactionMiss::NotInChain { at_epoch_id },
            ) => Err(Status::unavailable(format!(
                "the live mempool answered at chain epoch {} but this replica read chain epoch {}; retry",
                chain_epoch_id.value(),
                at_epoch_id.value(),
            ))),
            (
                MempoolTransactionLookup::Absent { .. },
                CanonicalTransactionMiss::BelowHistoryFloor(history_floor),
            ) => Err(history_floor),
        }
    }

    async fn mempool_transactions(
        &self,
        request: Request<lightwalletd::GetMempoolTxRequest>,
    ) -> Result<Response<GrpcStream<lightwalletd::CompactTx>>, Status> {
        let request = request.into_inner();
        validate_excluded_txid_suffixes(&request.exclude_txid_suffixes)?;
        let shape = CompactBlockShape::for_mempool(&request.pool_types)?;
        let mempool_surface = self
            .mempool_surface
            .as_ref()
            .ok_or_else(|| Status::unavailable("mempool surface is not configured"))?
            .clone();
        let first_page = mempool_surface
            .mempool_snapshot_page(LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE, None)
            .await
            .map_err(status_from_mempool_surface_error)?;
        if request.exclude_txid_suffixes.is_empty() {
            return Ok(Response::new(stream_mempool_compact_transactions(
                mempool_surface,
                first_page,
                shape,
            )));
        }
        buffered_mempool_transactions(
            mempool_surface,
            first_page,
            shape,
            request.exclude_txid_suffixes,
        )
        .await
        .map(Response::new)
    }

    async fn mempool_stream(
        &self,
    ) -> Result<Response<GrpcStream<lightwalletd::RawTransaction>>, Status> {
        let mempool_surface = self
            .mempool_surface
            .as_ref()
            .ok_or_else(|| Status::unavailable("mempool surface is not configured"))?
            .clone();
        // Deliver the current mempool contents from the snapshot walk, then
        // continue with events strictly after the walk's resume anchor. The
        // anchor hands off at-least-once: a transaction admitted mid-walk may
        // appear both in a later page and as an event, and an evicted
        // transaction may be added again. lightwalletd sends each txid once
        // per block interval, so the stream drops txids it already sent.
        let first_page = mempool_surface
            .mempool_snapshot_page(LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE, None)
            .await
            .map_err(status_from_mempool_surface_error)?;
        let snapshot_chain_epoch_id = first_page.chain_epoch_id;
        let event_stream = mempool_surface
            .mempool_events(first_page.events_resume_cursor.clone())
            .await
            .map_err(status_from_mempool_surface_error)?;
        let entry_stream = mempool_snapshot_entries(mempool_surface, first_page).chain(
            stream::StreamExt::filter_map(event_stream, added_mempool_entry),
        );
        let raw_transaction_stream = each_once_up_to(
            entry_stream,
            zinder_core::MempoolEntry::transaction_id,
            MAX_MEMPOOL_STREAM_TRANSACTIONS,
        )
        .map(|entry_outcome| entry_outcome.map(|entry| mempool_raw_transaction(&entry)));

        let bounded_stream: GrpcStream<lightwalletd::RawTransaction> =
            if let Some(watcher) = self.tip_change_watcher.clone() {
                Box::pin(close_mempool_stream_on_tip_change(
                    raw_transaction_stream,
                    watcher,
                    snapshot_chain_epoch_id,
                ))
            } else {
                Box::pin(raw_transaction_stream)
            };
        Ok(Response::new(bounded_stream))
    }
}

/// Wallet serving reads through its query at the serving pair the query
/// captures.
///
/// A view is the chain epoch of the visible tip, and every read of a view is
/// pinned to it, except the read of a later block-range chunk, which reads
/// unpinned at the pair current when it runs.
impl<QueryApi> CompactBlockReads for QueryApi
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    type View = ChainEpoch;

    fn subscribe_publications(&self) -> Option<tokio::sync::watch::Receiver<()>> {
        WalletQueryApi::subscribe_publications(self)
    }

    async fn peek_visible_tip_height(&self) -> Result<zinder_core::BlockHeight, Status> {
        match self.current_visible_tip_height() {
            Some(height) => Ok(height),
            None => self
                .capture_view()
                .await
                .map(|view| view.visible_tip_height),
        }
    }

    async fn capture_ephemeral_view(&self) -> Result<ChainEpoch, Status> {
        match self.current_chain_epoch() {
            Some(chain_epoch) => Ok(chain_epoch),
            None => self.capture_view().await,
        }
    }

    async fn capture_view(&self) -> Result<ChainEpoch, Status> {
        self.visible_tip_block(None)
            .await
            .map(|visible_tip_block| visible_tip_block.chain_epoch)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn capture_tip(&self) -> Result<TipCapture<ChainEpoch>, Status> {
        self.served_tip_snapshot()
            .await
            .map(|snapshot| TipCapture {
                view: snapshot.chain_epoch,
                block_time_unix_seconds: Some(snapshot.block_time_unix_seconds),
            })
            .map_err(|error| status_from_query_error(&error))
    }

    fn view_chain_epoch(view: &ChainEpoch) -> ChainEpoch {
        *view
    }

    async fn view_activations(
        &self,
        _view: &ChainEpoch,
    ) -> Result<NetworkUpgradeActivations, Status> {
        self.network_upgrade_activations()
            .await
            .map_err(|error| status_from_query_error(&error))
    }

    async fn tip_block_time(&self, view: &ChainEpoch) -> Result<i64, Status> {
        self.block_header_by_selector(
            BlockSelector::Height(view.visible_tip_height),
            Some(view.id),
        )
        .await
        .map(|tip_header| tip_header.block_header.block_time)
        .map_err(|error| status_from_query_error(&error))
    }

    async fn read_compact_block(
        &self,
        view: &ChainEpoch,
        block: RequestedBlock,
    ) -> Result<CompactBlockArtifact, Status> {
        let height = resolve_block_height(self, view, block)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        self.compact_block_at(height, Some(view.id))
            .await
            .map(|compact_block| compact_block.compact_block)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn read_encoded_compact_block(
        &self,
        view: &ChainEpoch,
        block: RequestedBlock,
        selection: CompactBlockSpliceSelection,
    ) -> Result<Option<EncodedBlock>, Status> {
        let height = resolve_block_height(self, view, block)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        self.encoded_compact_block_at(height, selection, Some(view.id))
            .await
            .map(|encoded| encoded.map(|response| response.compact_block))
            .map_err(|error| status_from_query_error(&error))
    }

    async fn read_encoded_compact_block_chunk(
        &self,
        view: ChunkView<Self::View>,
        request: EncodedCompactBlockRangeRequest,
        cancel: Arc<AtomicBool>,
    ) -> EncodedChunkReadOutcome<Self::View> {
        if cancel.load(Ordering::Acquire) {
            return EncodedChunkReadOutcome::Failed {
                read_duration: None,
                failure: ChunkReadFailure::Failed(Status::cancelled(
                    "block-range stream cancelled",
                )),
            };
        }
        let (read_at, at_epoch_id, fallback_view) = match view {
            ChunkView::Captured(epoch) => (epoch, Some(epoch.id), ChunkView::Captured(epoch)),
            ChunkView::Current => {
                let epoch = match self.capture_ephemeral_view().await {
                    Ok(epoch) => epoch,
                    Err(status) => {
                        return EncodedChunkReadOutcome::Failed {
                            read_duration: None,
                            failure: ChunkReadFailure::Failed(status),
                        };
                    }
                };
                if !view_reaches(&epoch, request.range) {
                    return EncodedChunkReadOutcome::Failed {
                        read_duration: None,
                        failure: ChunkReadFailure::TipBelowChunk(epoch),
                    };
                }
                (epoch, None, ChunkView::Current)
            }
        };
        let started_at = Instant::now();
        let outcome = self
            .encoded_compact_block_range_up_to(request, at_epoch_id, Some(Arc::clone(&cancel)))
            .await;
        let read_duration = started_at.elapsed();
        match outcome {
            Ok(Some(range)) => {
                if cancel.load(Ordering::Acquire) {
                    EncodedChunkReadOutcome::Failed {
                        read_duration: Some(read_duration),
                        failure: ChunkReadFailure::Failed(Status::cancelled(
                            "block-range stream cancelled",
                        )),
                    }
                } else {
                    EncodedChunkReadOutcome::Direct {
                        read_at,
                        range,
                        read_duration,
                    }
                }
            }
            Ok(None) => EncodedChunkReadOutcome::Fallback(fallback_view),
            Err(error) => {
                let status = if matches!(
                    error,
                    QueryError::CanonicalStore(StorageReadError::Cancelled)
                ) {
                    Status::cancelled("block-range stream cancelled")
                } else {
                    status_from_query_error(&error)
                };
                EncodedChunkReadOutcome::Failed {
                    read_duration: Some(read_duration),
                    failure: ChunkReadFailure::Failed(status),
                }
            }
        }
    }

    async fn read_compact_block_chunk<Output, Convert>(
        &self,
        read: ChunkRead<ChainEpoch>,
        convert: Convert,
    ) -> ChunkReadOutcome<Output>
    where
        Output: Send + 'static,
        Convert: FnOnce(ChainEpoch, Vec<CompactBlockArtifact>) -> Output + Send + 'static,
    {
        if read.cancel.is_cancelled() {
            return ChunkReadOutcome::unread(ChunkReadFailure::Failed(Status::cancelled(
                "block-range stream cancelled",
            )));
        }
        let at_epoch_id = match read.view {
            ChunkView::Captured(view) => Some(view.id),
            ChunkView::Current => {
                let view = match self.capture_ephemeral_view().await {
                    Ok(view) => view,
                    Err(status) => {
                        return ChunkReadOutcome::unread(ChunkReadFailure::Failed(status));
                    }
                };
                if !view_reaches(&view, read.chunk) {
                    return ChunkReadOutcome::unread(ChunkReadFailure::TipBelowChunk(view));
                }
                // The read is unpinned, so it answers at the pair current
                // when it runs.
                None
            }
        };
        let started_at = Instant::now();
        let compact_block_range = self
            .compact_blocks_in_range_up_to(read.chunk, read.order, read.max_bytes, at_epoch_id)
            .await;
        // The read stage ends when the query returns, so it leaves out the
        // conversion's wait for a blocking thread.
        let read_duration = Some(started_at.elapsed());
        let compact_block_range = match compact_block_range {
            Ok(compact_block_range) => compact_block_range,
            Err(error) => {
                return ChunkReadOutcome {
                    read_duration,
                    converted: Err(ChunkReadFailure::Failed(status_from_query_error(&error))),
                };
            }
        };
        let cancel = read.cancel;
        if cancel.is_cancelled() {
            return ChunkReadOutcome {
                read_duration,
                converted: Err(ChunkReadFailure::Failed(Status::cancelled(
                    "block-range stream cancelled",
                ))),
            };
        }
        // The query hands the blocks back on this task, so the conversion
        // moves to the blocking pool, inside the caller's span.
        let span = tracing::Span::current();
        let converted = tokio::task::spawn_blocking(move || {
            if cancel.is_cancelled() {
                return Err(ChunkReadFailure::Failed(Status::cancelled(
                    "block-range stream cancelled",
                )));
            }
            span.in_scope(|| {
                Ok(convert(
                    compact_block_range.chain_epoch,
                    compact_block_range.compact_blocks,
                ))
            })
        })
        .await
        .unwrap_or_else(|error| {
            Err(ChunkReadFailure::Failed(status_from_query_error(
                &QueryError::BlockingTaskFailed {
                    reason: error.to_string(),
                },
            )))
        });
        ChunkReadOutcome {
            read_duration,
            converted,
        }
    }
}

/// Resolves `block` to a height at `view`, which a hash takes one read for.
async fn resolve_block_height<QueryApi>(
    query_api: &QueryApi,
    view: &ChainEpoch,
    block: RequestedBlock,
) -> Result<BlockHeight, QueryError>
where
    QueryApi: WalletQueryApi + Sync,
{
    match block {
        RequestedBlock::Height(height) => Ok(height),
        RequestedBlock::Hash(block_hash) => query_api
            .block_id_by_selector(BlockSelector::Hash(block_hash), Some(view.id))
            .await
            .map(|resolved| resolved.block_id.height),
    }
}

fn reject_genesis_tree_state(
    block: RequestedBlock,
    network: zinder_core::Network,
) -> Result<(), Status> {
    if block == RequestedBlock::Hash(network.genesis_hash()) {
        return Err(pre_sapling_tree_state());
    }
    Ok(())
}

fn tree_state_block_lookup_status(error: &QueryError) -> Status {
    if matches!(error, QueryError::BlockNotInBestChain) {
        return Status::invalid_argument("block hash is not in the best chain");
    }
    status_from_query_error(error)
}

impl<QueryApi> LightwalletdGrpcAdapter<QueryApi>
where
    QueryApi: WalletQueryApi + Send + Sync + 'static,
{
    /// Opens the merge of every requested address's unspent outputs at the
    /// visible chain epoch, and reads the first output of each, so a request
    /// that cannot be served fails before any row is sent.
    ///
    /// Each distinct address is read once and its rows are sent once, however
    /// often the request lists it, as lightwalletd drops a repeated address
    /// before it queries the node.
    async fn address_utxo_merge(
        &self,
        request: lightwalletd::GetAddressUtxosArg,
    ) -> Result<AddressUtxoMerge, Status> {
        let response_limit = u32_to_usize(self.response_limits.max_address_utxos.get());
        let max_entries =
            NonZeroU32::new(request.max_entries).map(|max_entries| u32_to_usize(max_entries.get()));
        let mut merge = AddressUtxoMerge::new(max_entries, response_limit);
        if request.addresses.is_empty() {
            return Ok(merge);
        }

        let start_height = u32::try_from(request.start_height)
            .map_err(|_| Status::invalid_argument("startHeight exceeds u32"))?;
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let mut seen_addresses = HashSet::new();
        let distinct_addresses = request
            .addresses
            .into_iter()
            .filter(|address| seen_addresses.insert(address.clone()))
            .map(|address| {
                transparent_address_unspent_outputs_request(
                    &address,
                    visible_tip_block.chain_epoch.network,
                    BlockHeight::new(start_height),
                )
                .map(|query_request| (address, query_request))
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let first_page_size = address_utxo_first_page_size(
            distinct_addresses.len(),
            max_entries.map(|target| target.min(response_limit)),
        );
        for (address, query_request) in distinct_addresses {
            let outputs = self
                .query_api
                .transparent_address_unspent_outputs_on_demand(
                    query_request,
                    first_page_size,
                    Some(visible_tip_block.chain_epoch.id),
                )
                .await
                .map_err(|error| status_from_query_error(&error))?;
            merge.add_address(address, outputs).await?;
        }
        Ok(merge)
    }

    /// Starts reading the mined history `filter` selects at the visible
    /// chain epoch, and reads its first page, so a request that cannot be
    /// served fails before any transaction is sent.
    async fn transparent_history(
        &self,
        filter: &lightwalletd::TransparentAddressBlockFilter,
    ) -> Result<TransparentHistory<QueryApi>, Status>
    where
        QueryApi: Clone,
    {
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let request = transparent_address_tx_history_request(
            filter,
            visible_tip_block.chain_epoch.network,
            visible_tip_block.height,
        )?;
        TransparentHistory::start(
            self.query_api.clone(),
            request,
            visible_tip_block.chain_epoch.id,
        )
        .await
    }

    /// Returns lightwalletd `Balance { value_zat: int64 }` from the wallet
    /// plane's transparent-address balance primitive.
    ///
    /// The legacy proto carries one signed `value_zat` field that wallets
    /// interpret as confirmed balance. The compat shim reports the canonical
    /// confirmed total and rejects a total above the `int64` wire ceiling.
    async fn compat_balance_response(
        &self,
        script_hashes: Vec<TransparentAddressScriptHash>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        if script_hashes.is_empty() {
            return Err(Status::invalid_argument(EMPTY_BALANCE_ADDRESSES));
        }
        let visible_tip_block = self.compat_visible_tip_block().await?;
        let balance = self
            .query_api
            .transparent_address_balance(script_hashes, Some(visible_tip_block.chain_epoch.id))
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let value_zat = lightwalletd_balance_value_zat(balance)?;
        Ok(Response::new(lightwalletd::Balance { value_zat }))
    }

    async fn compat_visible_tip_block(&self) -> Result<zinder_query::VisibleTipBlock, Status> {
        self.query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))
    }

    async fn tree_state_hex_for_height(
        &self,
        height: BlockHeight,
    ) -> Result<zinder_query::ServedTreeStateHex, Status>
    where
        QueryApi: Clone,
    {
        self.block_methods
            .await_tip(&self.query_api, "get_tree_state", height)
            .await?;
        let view = self.query_api.capture_ephemeral_view().await?;
        reject_future_tree_state_height(height, view.visible_tip_height)?;
        match self.query_api.tree_state_hex_at(height, None).await {
            Ok(tree_state) => Ok(tree_state),
            Err(QueryError::BlockNotInBestChain) => {
                if self
                    .query_api
                    .canonical_history_bounds()
                    .and_then(zinder_core::CanonicalHistoryBounds::preceding_checkpoint)
                    .is_some_and(|checkpoint| height < checkpoint.height)
                {
                    return Err(tree_state_status(&QueryError::BlockNotInBestChain));
                }
                let latest_view = self.query_api.capture_ephemeral_view().await?;
                reject_future_tree_state_height(height, latest_view.visible_tip_height)?;
                let pinned_view = self.query_api.capture_view().await?;
                reject_future_tree_state_height(height, pinned_view.visible_tip_height)?;
                self.query_api
                    .tree_state_hex_at(height, Some(pinned_view.id))
                    .await
                    .map_err(|error| tree_state_status(&error))
            }
            Err(error) => Err(tree_state_status(&error)),
        }
    }
}

const EMPTY_BALANCE_ADDRESSES: &str = "addresses list must not be empty";
const UNRECEIVABLE_ADDRESS: &str = "transparent address does not produce a receivable script";

/// The distinct script hashes of one balance request, in request order.
///
/// A request may repeat an address, so it is counted once; the request is
/// refused at the first distinct address over
/// [`MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES`], before anything past the cap
/// is stored.
#[derive(Default)]
struct DistinctBalanceAddresses {
    script_hashes: Vec<TransparentAddressScriptHash>,
    seen: HashSet<TransparentAddressScriptHash>,
}

impl DistinctBalanceAddresses {
    fn admit(&mut self, script_hash: TransparentAddressScriptHash) -> Result<(), Status> {
        if self.seen.contains(&script_hash) {
            return Ok(());
        }
        if self.script_hashes.len() >= u32_to_usize(MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES) {
            return Err(status_from_query_error(
                &QueryError::TransparentBalanceAddressCountExceeded {
                    requested: self.script_hashes.len().saturating_add(1),
                    maximum: MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES,
                },
            ));
        }
        self.seen.insert(script_hash);
        self.script_hashes.push(script_hash);
        Ok(())
    }
}

/// Parses a streamed or listed lightwalletd address into its script hash on
/// `network`, so callers keep 32 bytes per address instead of the raw string.
fn compat_address_script_hash(
    address: String,
    network: Network,
) -> Result<TransparentAddressScriptHash, Status> {
    address_lookup_to_script_hash(
        Some(wallet_proto::AddressLookup {
            selector: Some(address_lookup::Selector::Address(address)),
        }),
        network,
    )
    .map_err(|error| status_from_query_error(&error))
}

fn lightwalletd_balance_value_zat(
    balance: zinder_core::TransparentAddressBalance,
) -> Result<i64, Status> {
    i64::try_from(balance.confirmed_zat).map_err(|_| {
        Status::out_of_range(
            "transparent address balance exceeds the lightwalletd int64 balance field",
        )
    })
}

#[tonic::async_trait]
impl<QueryApi> lightwalletd::EncodedBlockRange for LightwalletdGrpcAdapter<QueryApi>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    async fn get_encoded_block_range(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<lightwalletd::EncodedBlockRangeStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(&self.query_api, "get_encoded_block_range", &request)
            .await?;
        self.block_methods
            .encoded_block_range(&self.query_api, &request, CompactBlockShape::for_range)
            .await
            .map(Response::new)
    }

    fn supports_encoded_frames(&self) -> bool {
        true
    }

    async fn get_encoded_block_range_frames(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<lightwalletd::EncodedBlockFrameStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(&self.query_api, "get_encoded_block_range", &request)
            .await?;
        self.block_methods
            .encoded_block_range_frames(&self.query_api, &request, CompactBlockShape::for_range)
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl<QueryApi> lightwalletd::EncodedBlockRangeNullifiers for LightwalletdGrpcAdapter<QueryApi>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    async fn get_encoded_block_range_nullifiers(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<lightwalletd::EncodedBlockRangeStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(
                &self.query_api,
                "get_encoded_block_range_nullifiers",
                &request,
            )
            .await?;
        self.block_methods
            .encoded_block_range(
                &self.query_api,
                &request,
                CompactBlockShape::for_nullifier_range,
            )
            .await
            .map(Response::new)
    }

    fn supports_encoded_frames(&self) -> bool {
        true
    }

    async fn get_encoded_block_range_nullifier_frames(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<lightwalletd::EncodedBlockFrameStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(
                &self.query_api,
                "get_encoded_block_range_nullifiers",
                &request,
            )
            .await?;
        self.block_methods
            .encoded_block_range_frames(
                &self.query_api,
                &request,
                CompactBlockShape::for_nullifier_range,
            )
            .await
            .map(Response::new)
    }
}

#[tonic::async_trait]
impl<QueryApi> compact_tx_streamer_server::CompactTxStreamer for LightwalletdGrpcAdapter<QueryApi>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    fn encoded_block_range(&self) -> Option<&dyn lightwalletd::EncodedBlockRange> {
        Some(self)
    }

    fn encoded_block_range_nullifiers(
        &self,
    ) -> Option<&dyn lightwalletd::EncodedBlockRangeNullifiers> {
        Some(self)
    }

    async fn get_latest_block(
        &self,
        _request: Request<lightwalletd::ChainSpec>,
    ) -> Result<Response<lightwalletd::BlockId>, Status> {
        self.block_methods
            .latest_block(&self.query_api)
            .await
            .map(Response::new)
    }

    async fn get_block(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        let block_id = request.into_inner();
        self.block_methods
            .await_block_tip(&self.query_api, "get_block", &block_id)
            .await?;
        block_methods::compact_block(&self.query_api, &block_id, CompactBlockShape::FULL_BLOCK)
            .await
            .map(Response::new)
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        let block_id = request.into_inner();
        self.block_methods
            .await_block_tip(&self.query_api, "get_block_nullifiers", &block_id)
            .await?;
        block_methods::compact_block(
            &self.query_api,
            &block_id,
            CompactBlockShape::BLOCK_NULLIFIERS,
        )
        .await
        .map(Response::new)
    }

    type GetBlockRangeStream = GrpcStream<lightwalletd::CompactBlock>;

    async fn get_block_range(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(&self.query_api, "get_block_range", &request)
            .await?;
        self.block_methods
            .block_range(&self.query_api, &request, CompactBlockShape::for_range)
            .await
            .map(Response::new)
    }

    type GetBlockRangeNullifiersStream = GrpcStream<lightwalletd::CompactBlock>;

    async fn get_block_range_nullifiers(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        let request = request.into_inner();
        self.block_methods
            .await_range_tip(&self.query_api, "get_block_range_nullifiers", &request)
            .await?;
        self.block_methods
            .block_range(
                &self.query_api,
                &request,
                CompactBlockShape::for_nullifier_range,
            )
            .await
            .map(Response::new)
    }

    async fn get_transaction(
        &self,
        request: Request<lightwalletd::TxFilter>,
    ) -> Result<Response<lightwalletd::RawTransaction>, Status> {
        let at_epoch_id = self.query_api.capture_view().await?.id;
        let filter = request.into_inner();

        if filter.hash.is_empty() {
            return Err(Status::invalid_argument("GetTransaction: specify a txid"));
        }
        let transaction_id = decode_internal_transaction_id(&filter.hash)
            .map_err(|error| wire_decode_error_to_status(&error))?;
        let lookup = self
            .query_api
            .transaction(transaction_id, Some(at_epoch_id))
            .await;
        #[allow(
            clippy::wildcard_enum_match_arm,
            reason = "TxStatus is #[non_exhaustive]; a status added later is not mined, so it falls back to the mempool like NotFound."
        )]
        let canonical_miss = match lookup {
            Ok(response) => match response.status {
                TxStatus::Mined(mined) => {
                    return Ok(Response::new(raw_transaction_from_mined(mined)?));
                }
                TxStatus::InMempool(entry) => {
                    return Ok(Response::new(mempool_raw_transaction(&entry)));
                }
                _ => CanonicalTransactionMiss::NotInChain { at_epoch_id },
            },
            Err(error @ QueryError::CanonicalHistoryUnavailable { .. }) => {
                CanonicalTransactionMiss::BelowHistoryFloor(status_from_query_error(&error))
            }
            Err(error) => return Err(forwarded_read_status(&error)),
        };

        // A store without transaction bodies verifies its forwarding node
        // against the captured tip before the mempool answers or an absence
        // is reported.
        self.query_api
            .verify_forwarding_node(Some(at_epoch_id))
            .await
            .map_err(|error| forwarded_read_status(&error))?;
        self.pending_transaction(transaction_id, canonical_miss)
            .await
            .map(Response::new)
    }

    async fn send_transaction(
        &self,
        request: Request<lightwalletd::RawTransaction>,
    ) -> Result<Response<lightwalletd::SendResponse>, Status> {
        let raw_transaction = RawTransactionBytes::new(request.into_inner().data);
        let broadcast_outcome = self
            .query_api
            .broadcast_transaction(raw_transaction)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        Ok(Response::new(send_response_from_broadcast_outcome(
            broadcast_outcome,
        )))
    }

    type GetTaddressTxidsStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_taddress_txids(
        &self,
        request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTxidsStream>, Status> {
        let history = self.transparent_history(&request.into_inner()).await?;
        Ok(Response::new(history.into_stream()))
    }

    type GetTaddressTransactionsStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_taddress_transactions(
        &self,
        request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        let history = self.transparent_history(&request.into_inner()).await?;
        Ok(Response::new(history.into_stream()))
    }

    async fn get_taddress_balance(
        &self,
        request: Request<lightwalletd::AddressList>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        let addresses = request.into_inner().addresses;
        let network = self.compat_visible_tip_block().await?.chain_epoch.network;
        let mut balance_addresses = DistinctBalanceAddresses::default();
        for address in addresses {
            balance_addresses.admit(compat_address_script_hash(address, network)?)?;
        }
        self.compat_balance_response(balance_addresses.script_hashes)
            .await
    }

    async fn get_taddress_balance_stream(
        &self,
        request: Request<tonic::Streaming<lightwalletd::Address>>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        let mut stream = request.into_inner();
        // The network is fixed for the runtime, so reading it here pins no
        // epoch for the duration of the client's stream.
        let network = self.compat_visible_tip_block().await?.chain_epoch.network;
        let mut balance_addresses = DistinctBalanceAddresses::default();
        while let Some(received) = stream.next().await {
            balance_addresses.admit(compat_address_script_hash(received?.address, network)?)?;
        }
        self.compat_balance_response(balance_addresses.script_hashes)
            .await
    }

    type GetMempoolTxStream = GrpcStream<lightwalletd::CompactTx>;

    async fn get_mempool_tx(
        &self,
        request: Request<lightwalletd::GetMempoolTxRequest>,
    ) -> Result<Response<Self::GetMempoolTxStream>, Status> {
        self.mempool_transactions(request).await
    }

    type GetMempoolStreamStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_mempool_stream(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
        self.mempool_stream().await
    }

    async fn get_tree_state(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        let block = requested_block(&request.into_inner(), decode_rpc_block_hash_bytes)?;
        let tree_state = match block {
            RequestedBlock::Height(height) => self.tree_state_hex_for_height(height).await?,
            RequestedBlock::Hash(_) => {
                let view = self.query_api.capture_view().await?;
                reject_genesis_tree_state(block, view.network)?;
                let height = resolve_block_height(&self.query_api, &view, block)
                    .await
                    .map_err(|error| tree_state_block_lookup_status(&error))?;
                reject_future_tree_state_height(height, view.visible_tip_height)?;
                self.query_api
                    .tree_state_hex_at(height, Some(view.id))
                    .await
                    .map_err(|error| tree_state_status(&error))?
            }
        };

        lightwalletd_tree_state_from_hex(tree_state).map(Response::new)
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        let tree_state = self
            .query_api
            .latest_tree_state_checkpoint_hex(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;

        lightwalletd_tree_state_from_hex(tree_state).map(Response::new)
    }

    type GetSubtreeRootsStream = GrpcStream<lightwalletd::SubtreeRoot>;

    async fn get_subtree_roots(
        &self,
        request: Request<lightwalletd::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        let epoch = self.query_api.capture_view().await?;
        let request = request.into_inner();
        let protocol = shielded_protocol_from_request(request.shielded_protocol)?;
        let source = SubtreeRootsStream::start(
            self.query_api.clone(),
            protocol,
            &request,
            self.response_limits.max_subtree_roots,
            epoch,
        )
        .await?;

        Ok(Response::new(spawn_response_stream(
            source,
            SUBTREE_ROOT_STREAM_CHANNEL_CAPACITY,
        )))
    }

    async fn get_address_utxos(
        &self,
        request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<lightwalletd::GetAddressUtxosReplyList>, Status> {
        let mut merge = self.address_utxo_merge(request.into_inner()).await?;
        let mut replies = Vec::new();
        while let Some(reply) = merge.next().await {
            replies.push(reply?);
        }
        Ok(Response::new(lightwalletd::GetAddressUtxosReplyList {
            address_utxos: replies,
        }))
    }

    type GetAddressUtxosStreamStream = GrpcStream<lightwalletd::GetAddressUtxosReply>;

    async fn get_address_utxos_stream(
        &self,
        request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        let merge = self.address_utxo_merge(request.into_inner()).await?;
        Ok(Response::new(spawn_response_stream(
            merge,
            ADDRESS_UTXO_STREAM_CHANNEL_CAPACITY,
        )))
    }

    async fn get_lightd_info(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::LightdInfo>, Status> {
        self.block_methods
            .lightd_info(&self.query_api, self.transparent_address_support)
            .await
            .map(Response::new)
    }

    async fn get_standard_fee(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::StandardFee>, Status> {
        standard_fee_response(self.query_api.standard_fee().await)
    }

    async fn ping(
        &self,
        request: Request<lightwalletd::Duration>,
    ) -> Result<Response<lightwalletd::PingResponse>, Status> {
        self.ping
            .respond(request.into_inner())
            .await
            .map(Response::new)
    }
}

/// Where a server-streaming response produced by a reader task gets its
/// items, in response order.
trait ResponseSource: Send + 'static {
    /// One response message.
    type Message: Send + 'static;

    /// Reads the next message, or the status that ends the response, or
    /// `None` once the response is complete.
    fn next(&mut self) -> impl Future<Output = Option<Result<Self::Message, Status>>> + Send;
}

/// Returns a response that sends what `source` reads, at most `capacity`
/// messages ahead of the client, and spawns the task that reads it.
///
/// The response ends after the first status `source` returns, so a client
/// that received rows before a failure sees the failure as the response's
/// status. The task stops and drops `source` once the client drops the
/// response, even while a read is in progress.
fn spawn_response_stream<Source: ResponseSource>(
    mut source: Source,
    capacity: usize,
) -> GrpcStream<Source::Message> {
    let (messages, queued_messages) = tokio::sync::mpsc::channel(capacity);
    tokio::spawn(async move {
        // Reserving the slot before reading keeps the messages read but not
        // yet received within `capacity`.
        while let Ok(slot) = messages.reserve().await {
            let message = tokio::select! {
                biased;
                () = messages.closed() => return,
                message = source.next() => message,
            };
            let Some(message) = message else {
                return;
            };
            let is_terminal = message.is_err();
            slot.send(message);
            if is_terminal {
                return;
            }
        }
    });
    Box::pin(tokio_stream::wrappers::ReceiverStream::new(queued_messages))
}

/// One transparent address's mined transactions at one chain epoch.
///
/// Pages of history entries are read one at a time, with up to
/// [`TRANSPARENT_HISTORY_STREAM_CHANNEL_CAPACITY`] transaction reads in flight
/// or buffered ahead of the client.
struct TransparentHistory<QueryApi> {
    query_api: QueryApi,
    request: TransparentAddressTxIdsInRangeRequest,
    at_epoch_id: ChainEpochId,
    transactions: TransactionsInOrder,
    has_later_pages: bool,
}

type HistoryPermit = tokio::sync::mpsc::OwnedPermit<Result<lightwalletd::RawTransaction, Status>>;

struct HistoryPipeline {
    in_flight: futures_util::stream::FuturesOrdered<PendingHistoryRow>,
    refill: Option<zinder_query::CanonicalRefill>,
    permits: Vec<HistoryPermit>,
}

struct PendingHistoryRow {
    permit: Option<HistoryPermit>,
    pending: PendingTransaction,
}

impl PendingHistoryRow {
    const fn new(permit: HistoryPermit, pending: PendingTransaction) -> Self {
        Self {
            permit: Some(permit),
            pending,
        }
    }
}

impl Future for PendingHistoryRow {
    type Output = (HistoryPermit, Result<lightwalletd::RawTransaction, Status>);

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let std::task::Poll::Ready(outcome) = Pin::new(&mut self.pending).poll(cx) else {
            return std::task::Poll::Pending;
        };
        let message = outcome
            .map_err(|error| forwarded_read_status(&error))
            .and_then(|response| {
                raw_transaction_from_mined(mined_transaction_from_status(response.status)?)
            });
        let Some(permit) = self.permit.take() else {
            return std::task::Poll::Pending;
        };
        std::task::Poll::Ready((permit, message))
    }
}

impl<QueryApi> TransparentHistory<QueryApi>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    async fn start(
        query_api: QueryApi,
        request: TransparentAddressTxIdsInRangeRequest,
        at_epoch_id: ChainEpochId,
    ) -> Result<Self, Status> {
        let mut history = Self {
            query_api,
            request,
            at_epoch_id,
            transactions: TransactionsInOrder::empty(),
            has_later_pages: true,
        };
        history.read_page().await?;
        Ok(history)
    }

    async fn read_page(&mut self) -> Result<(), Status> {
        let response = self
            .query_api
            .transparent_address_tx_ids_in_range(self.request.clone(), Some(self.at_epoch_id))
            .await
            .map_err(|error| status_from_query_error(&error))?;
        self.has_later_pages = response.next_cursor.is_some();
        self.request.from_cursor = response.next_cursor;
        if response.artifacts.is_empty() {
            self.transactions = TransactionsInOrder::empty();
            return Ok(());
        }
        let transaction_ids: Vec<TransactionId> = response
            .artifacts
            .into_iter()
            .map(|artifact: TransparentAddressTxIndexArtifact| artifact.transaction_id)
            .collect();
        self.transactions = self
            .query_api
            .transactions_in_order(transaction_ids, Some(self.at_epoch_id))
            .await
            .map_err(|error| status_from_query_error(&error))?;
        Ok(())
    }

    fn into_stream(mut self) -> GrpcStream<lightwalletd::RawTransaction> {
        if !self.transactions.has_remaining() && !self.has_later_pages {
            return Box::pin(stream::empty());
        }
        let (messages, queued_messages) =
            tokio::sync::mpsc::channel(TRANSPARENT_HISTORY_STREAM_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            self.stream_into(messages).await;
        });
        Box::pin(tokio_stream::wrappers::ReceiverStream::new(queued_messages))
    }

    async fn stream_into(
        &mut self,
        messages: tokio::sync::mpsc::Sender<Result<lightwalletd::RawTransaction, Status>>,
    ) {
        let mut pipeline = HistoryPipeline {
            in_flight: futures_util::stream::FuturesOrdered::new(),
            refill: None,
            permits: Vec::with_capacity(TRANSPARENT_HISTORY_STREAM_CHANNEL_CAPACITY),
        };
        loop {
            let window_cap = self.max_window();
            self.fill_window(&messages, window_cap, &mut pipeline);
            if pipeline.in_flight.is_empty()
                && pipeline.refill.is_none()
                && !self.transactions.has_remaining()
            {
                if !self.has_later_pages {
                    return;
                }
                if !self.advance_page(&messages, &mut pipeline.permits).await {
                    return;
                }
                continue;
            }
            let need_permit = pipeline.in_flight.len() + pipeline.permits.len() < window_cap
                && pipeline.permits.is_empty()
                && (self.transactions.can_take_ready()
                    || (pipeline.refill.is_none() && self.transactions.needs_refill()));
            tokio::select! {
                biased;
                () = messages.closed() => return,
                completed = pipeline.in_flight.next(), if !pipeline.in_flight.is_empty() => {
                    if let Some((permit, message)) = completed {
                        let is_terminal = message.is_err();
                        permit.send(message);
                        if is_terminal {
                            return;
                        }
                    }
                }
                completed_refill = poll_optional_refill(&mut pipeline.refill), if pipeline.refill.is_some() => {
                    self.transactions.apply_refill(completed_refill);
                }
                reserved = messages.clone().reserve_owned(), if need_permit => {
                    let Ok(permit) = reserved else {
                        return;
                    };
                    pipeline.permits.push(permit);
                }
            }
        }
    }

    fn max_window(&self) -> usize {
        if !self.transactions.has_remaining() {
            return 0;
        }
        self.transactions
            .max_in_flight()
            .clamp(1, TRANSPARENT_HISTORY_STREAM_CHANNEL_CAPACITY)
    }

    fn fill_window(
        &mut self,
        messages: &tokio::sync::mpsc::Sender<Result<lightwalletd::RawTransaction, Status>>,
        window_cap: usize,
        pipeline: &mut HistoryPipeline,
    ) {
        while pipeline.in_flight.len() < window_cap && self.transactions.has_remaining() {
            while pipeline.in_flight.len() + pipeline.permits.len() < window_cap {
                let Ok(permit) = messages.clone().try_reserve_owned() else {
                    break;
                };
                pipeline.permits.push(permit);
            }
            if pipeline.permits.is_empty() {
                break;
            }
            let ready = self.transactions.take_ready(pipeline.permits.len());
            if !ready.is_empty() {
                let started = ready.len();
                for (permit, pending) in pipeline.permits.drain(..started).zip(ready) {
                    pipeline
                        .in_flight
                        .push_back(PendingHistoryRow::new(permit, pending));
                }
                continue;
            }
            if pipeline.refill.is_none() && self.transactions.needs_refill() {
                pipeline.refill = self.transactions.start_refill(pipeline.permits.len());
            }
            break;
        }
        if !self.transactions.has_remaining() {
            pipeline.permits.truncate(usize::from(self.has_later_pages));
        }
    }

    async fn advance_page(
        &mut self,
        messages: &tokio::sync::mpsc::Sender<Result<lightwalletd::RawTransaction, Status>>,
        permits: &mut Vec<HistoryPermit>,
    ) -> bool {
        let permit = if let Some(permit) = permits.pop() {
            permit
        } else {
            let Ok(permit) = messages.clone().reserve_owned().await else {
                return false;
            };
            permit
        };
        let page_outcome = tokio::select! {
            biased;
            () = messages.closed() => return false,
            outcome = self.read_page() => outcome,
        };
        match page_outcome {
            Ok(()) => {
                permits.push(permit);
                true
            }
            Err(status) => {
                self.has_later_pages = false;
                permit.send(Err(status));
                false
            }
        }
    }
}

async fn poll_optional_refill(
    refill: &mut Option<zinder_query::CanonicalRefill>,
) -> zinder_query::CompletedCanonicalRefill {
    let Some(active) = refill.as_mut() else {
        return std::future::pending().await;
    };
    let completed = active.await;
    *refill = None;
    completed
}

trait SubtreeRootReads: Send + Sync + 'static {
    fn read_subtree_roots(
        &self,
        range: SubtreeRootRange,
        epoch: ChainEpoch,
    ) -> impl Future<Output = Result<SubtreeRoots, QueryError>> + Send;
}

impl<Query: WalletQueryApi> SubtreeRootReads for Query {
    async fn read_subtree_roots(
        &self,
        range: SubtreeRootRange,
        epoch: ChainEpoch,
    ) -> Result<SubtreeRoots, QueryError> {
        self.subtree_roots(range, Some(epoch.id)).await
    }
}

/// One protocol's subtree roots at one chain epoch, read one bounded native
/// page at a time until `max_entries` is reached or the store runs out.
///
/// `start` reads the first page, so argument and retained-history-floor
/// failures surface before the response stream opens.
struct SubtreeRootsStream<QueryApi> {
    query_api: QueryApi,
    protocol: ShieldedProtocol,
    epoch: ChainEpoch,
    /// Index of the next root to read, or `None` once the stream is complete.
    next_index: Option<u32>,
    /// Roots still to return. `maxEntries = 0` ("all") starts at `u32::MAX`,
    /// which no store reaches because subtree indexes are `u32`.
    remaining: u32,
    page_limit: NonZeroU32,
    page: std::vec::IntoIter<lightwalletd::SubtreeRoot>,
}

impl<QueryApi> SubtreeRootsStream<QueryApi>
where
    QueryApi: SubtreeRootReads,
{
    async fn start(
        query_api: QueryApi,
        protocol: ShieldedProtocol,
        request: &lightwalletd::GetSubtreeRootsArg,
        page_limit: NonZeroU32,
        epoch: ChainEpoch,
    ) -> Result<Self, Status> {
        let mut stream = Self {
            query_api,
            protocol,
            epoch,
            next_index: Some(request.start_index),
            remaining: if request.max_entries == 0 {
                u32::MAX
            } else {
                request.max_entries
            },
            page_limit: page_limit.min(MAX_SUBTREE_ROOT_PAGE_SIZE),
            page: Vec::new().into_iter(),
        };
        stream.read_next_page().await?;
        Ok(stream)
    }

    async fn read_next_page(&mut self) -> Result<(), Status> {
        let (Some(start_index), Some(remaining)) =
            (self.next_index, NonZeroU32::new(self.remaining))
        else {
            self.next_index = None;
            return Ok(());
        };
        let page_entries = remaining.min(self.page_limit);
        let subtree_roots = self
            .query_api
            .read_subtree_roots(
                SubtreeRootRange::new(
                    self.protocol,
                    SubtreeRootIndex::new(start_index),
                    page_entries,
                ),
                self.epoch,
            )
            .await
            .map_err(|error| subtree_roots_status(&error))?;
        let received = u32::try_from(subtree_roots.subtree_roots.len())
            .map_err(|_| Status::internal("native subtree_roots page length exceeds u32"))?;

        let completed_subtree_count = subtree_roots
            .chain_epoch
            .tip_metadata
            .completed_subtree_count(self.protocol);

        self.remaining = self.remaining.saturating_sub(received);
        // The page's own epoch says where the roots end, so a full page that
        // reaches the last completed root ends the stream without another
        // read that a retired epoch would turn into `UNAVAILABLE`.
        self.next_index = start_index.checked_add(received).filter(|next_index| {
            received >= page_entries.get() && *next_index < completed_subtree_count
        });
        self.page = lightwalletd_subtree_roots(&subtree_roots).into_iter();
        Ok(())
    }
}

impl<QueryApi> ResponseSource for SubtreeRootsStream<QueryApi>
where
    QueryApi: SubtreeRootReads,
{
    type Message = lightwalletd::SubtreeRoot;

    async fn next(&mut self) -> Option<Result<Self::Message, Status>> {
        loop {
            if let Some(root) = self.page.next() {
                return Some(Ok(root));
            }
            self.next_index?;
            if let Err(status) = self.read_next_page().await {
                self.next_index = None;
                return Some(Err(status));
            }
        }
    }
}

/// The first page size of every address stream of a UTXO request that lists
/// `distinct_addresses` distinct addresses and targets `max_entries` rows.
///
/// Each address gets an even share of [`ADDRESS_UTXO_FIRST_PAGE_BUDGET`],
/// capped at the 1,024-output page and at `max_entries`, but never fewer than
/// [`ADDRESS_UTXO_MIN_FIRST_PAGE_SIZE`] outputs. A request of up to eight
/// addresses with at most 1,024 outputs each then reads each address in one
/// page, and a request of many addresses keeps small first pages.
fn address_utxo_first_page_size(
    distinct_addresses: usize,
    max_entries: Option<usize>,
) -> NonZeroU16 {
    let share = ADDRESS_UTXO_FIRST_PAGE_BUDGET / distinct_addresses.max(1);
    let wanted = max_entries.map_or(share, |max_entries| share.min(max_entries));
    NonZeroU16::new(u16::try_from(wanted).unwrap_or(u16::MAX))
        .unwrap_or(NonZeroU16::MIN)
        .clamp(
            ADDRESS_UTXO_MIN_FIRST_PAGE_SIZE,
            TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE,
        )
}

/// An epoch-bound merge of on-demand address streams.
///
/// The storage index orders by height and outpoint. The merge stages one
/// height within the response budget and sorts it by transaction position.
struct AddressUtxoMerge {
    addresses: Vec<MergedAddress>,
    next_outputs: BinaryHeap<Reverse<NextAddressUtxo>>,
    current_height: std::vec::IntoIter<NextAddressUtxo>,
    max_entries: Option<usize>,
    budget: AddressUtxoResponseBudget,
    ended: bool,
}

/// One distinct address of an [`AddressUtxoMerge`].
struct MergedAddress {
    address: String,
    outputs: TransparentAddressUnspentOutputs,
}

/// The next output of one address in an [`AddressUtxoMerge`].
struct NextAddressUtxo {
    reply: lightwalletd::GetAddressUtxosReply,
    address_position: usize,
    tx_index: u32,
}

impl NextAddressUtxo {
    const fn merge_key(&self) -> (u64, usize) {
        (self.reply.height, self.address_position)
    }

    fn block_order_key(&self) -> (u32, i32, &[u8]) {
        (self.tx_index, self.reply.index, self.reply.txid.as_slice())
    }
}

impl PartialEq for NextAddressUtxo {
    fn eq(&self, other: &Self) -> bool {
        self.merge_key() == other.merge_key()
    }
}

impl Eq for NextAddressUtxo {}

impl PartialOrd for NextAddressUtxo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NextAddressUtxo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.merge_key().cmp(&other.merge_key())
    }
}

impl AddressUtxoMerge {
    fn new(max_entries: Option<usize>, response_limit: usize) -> Self {
        Self {
            addresses: Vec::new(),
            next_outputs: BinaryHeap::new(),
            current_height: Vec::new().into_iter(),
            max_entries,
            budget: AddressUtxoResponseBudget::new(response_limit),
            ended: false,
        }
    }

    /// Adds one distinct address and reads its first output.
    async fn add_address(
        &mut self,
        address: String,
        outputs: TransparentAddressUnspentOutputs,
    ) -> Result<(), Status> {
        self.addresses.push(MergedAddress { address, outputs });
        self.read_next_output(self.addresses.len() - 1).await
    }

    async fn read_next_output(&mut self, address_position: usize) -> Result<(), Status> {
        let Some(merged) = self.addresses.get_mut(address_position) else {
            return Ok(());
        };
        match merged.outputs.next_with_tx_index().await {
            Some(Ok((tx_index, utxo))) => {
                let reply = lightwalletd_address_utxo(&merged.address, utxo)?;
                self.next_outputs.push(Reverse(NextAddressUtxo {
                    reply,
                    address_position,
                    tx_index,
                }));
                Ok(())
            }
            Some(Err(error)) => Err(status_from_query_error(&error)),
            None => Ok(()),
        }
    }

    async fn read_height(&mut self) -> Result<Vec<NextAddressUtxo>, Status> {
        let Some(Reverse(first)) = self.next_outputs.peek() else {
            return Ok(Vec::new());
        };
        let height = first.reply.height;
        let mut outputs = Vec::new();
        while let Some(Reverse(next_output)) = self
            .next_outputs
            .peek_mut()
            .filter(|next_output| next_output.0.reply.height == height)
            .map(PeekMut::pop)
        {
            self.budget.record(&next_output.reply)?;
            self.read_next_output(next_output.address_position).await?;
            outputs.push(next_output);
        }
        outputs
            .sort_unstable_by(|left, right| left.block_order_key().cmp(&right.block_order_key()));
        Ok(outputs)
    }
}

impl ResponseSource for AddressUtxoMerge {
    type Message = lightwalletd::GetAddressUtxosReply;

    async fn next(&mut self) -> Option<Result<Self::Message, Status>> {
        if let Some(output) = self.current_height.next() {
            return Some(Ok(output.reply));
        }
        if self.ended
            || self
                .max_entries
                .is_some_and(|target| self.budget.entries >= target)
        {
            return None;
        }
        match self.read_height().await {
            Ok(outputs) => {
                self.current_height = outputs.into_iter();
                self.current_height.next().map(|output| Ok(output.reply))
            }
            Err(status) => {
                self.ended = true;
                Some(Err(status))
            }
        }
    }
}

/// Counts each row as a field of the unary reply, including its protobuf
/// tag and length prefix, so the same byte bound applies to both RPCs.
struct AddressUtxoResponseBudget {
    entry_limit: usize,
    entries: usize,
    bytes: usize,
}

impl AddressUtxoResponseBudget {
    const fn new(entry_limit: usize) -> Self {
        Self {
            entry_limit,
            entries: 0,
            bytes: 0,
        }
    }

    fn record(&mut self, reply: &lightwalletd::GetAddressUtxosReply) -> Result<(), Status> {
        if self.entries == self.entry_limit {
            return Err(Status::resource_exhausted(format!(
                "more unspent outputs match than the {} one response returns",
                self.entry_limit
            )));
        }
        let bytes = reply.encoded_len();
        self.bytes = self
            .bytes
            .saturating_add(1 + prost::length_delimiter_len(bytes) + bytes);
        if self.bytes > ADDRESS_UTXO_RESPONSE_MAX_BYTES {
            return Err(Status::resource_exhausted(format!(
                "unspent-output response exceeds {ADDRESS_UTXO_RESPONSE_MAX_BYTES} encoded bytes"
            )));
        }
        self.entries += 1;
        Ok(())
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "TxStatus is #[non_exhaustive]; non-mined statuses map to gRPC NOT_FOUND on the lightwalletd wire because lightwalletd's RawTransaction shape is mined-only."
)]
fn mined_transaction_from_status(status: TxStatus) -> Result<MinedTransaction, Status> {
    match status {
        TxStatus::Mined(mined) => Ok(mined),
        TxStatus::NotFound | TxStatus::InMempool(_) => Err(Status::not_found(
            "transaction is not mined in the canonical chain",
        )),
        _ => Err(Status::not_found(
            "transaction status is not representable on the lightwalletd wire",
        )),
    }
}

/// Returns a mined transaction's bytes as lightwalletd sends them, from the
/// read that located it.
fn raw_transaction_from_mined(
    mined: MinedTransaction,
) -> Result<lightwalletd::RawTransaction, Status> {
    let location = mined.location;
    let raw_transaction_bytes = mined.raw_transaction_bytes.ok_or_else(|| {
        status_from_query_error(&QueryError::ArtifactUnavailable {
            family: ArtifactFamily::TransactionBlob,
            key: ArtifactKey::TransactionId(location.transaction_id),
        })
    })?;
    Ok(lightwalletd::RawTransaction {
        data: raw_transaction_bytes,
        height: u64::from(location.block_height.value()),
    })
}

fn reject_future_tree_state_height(
    requested_height: BlockHeight,
    visible_tip_height: BlockHeight,
) -> Result<(), Status> {
    if requested_height > visible_tip_height {
        return Err(Status::invalid_argument(format!(
            "tree state height {} is newer than latest block {}",
            requested_height.value(),
            visible_tip_height.value(),
        )));
    }
    Ok(())
}

/// Convert a [`WireDecodeError`] into a tonic [`Status`] for the
/// lightwalletd-compatibility surface.
///
/// Length and hex-decoding failures map to `INVALID_ARGUMENT` (the caller
/// supplied a malformed byte or string field). Enum and dialect-string
/// failures use the same code: the caller's wire input did not match any
/// known value for the surface they targeted.
fn wire_decode_error_to_status(error: &WireDecodeError) -> Status {
    Status::invalid_argument(error.to_string())
}

fn shielded_protocol_from_request(protocol: i32) -> Result<ShieldedProtocol, Status> {
    match lightwalletd::ShieldedProtocol::try_from(protocol) {
        Ok(lightwalletd::ShieldedProtocol::Sapling) => Ok(ShieldedProtocol::Sapling),
        Ok(lightwalletd::ShieldedProtocol::Orchard) => Ok(ShieldedProtocol::Orchard),
        Ok(lightwalletd::ShieldedProtocol::Ironwood) => Ok(ShieldedProtocol::Ironwood),
        Err(_) => Err(Status::invalid_argument("shieldedProtocol is unknown")),
    }
}

/// Builds the native history request for a lightwalletd address filter.
///
/// The range resolves as lightwalletd resolves it on Zebra: an absent or zero
/// `range.end` means the visible tip, and both bounds are clamped to the
/// visible tip, so an end above the tip, even above `u32`, reads to the tip.
fn transparent_address_tx_history_request(
    filter: &lightwalletd::TransparentAddressBlockFilter,
    network: Network,
    visible_tip_height: BlockHeight,
) -> Result<TransparentAddressTxIdsInRangeRequest, Status> {
    let address_text = filter.address.as_str();
    let zebra_address = parse_transparent_address(address_text)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if !transparent_address_matches_network(zebra_address.network_kind(), network) {
        return Err(Status::invalid_argument(
            "transparent address network does not match server network",
        ));
    }
    let script_pub_key = zebra_address.script().as_raw_bytes().to_vec();
    if script_pub_key.is_empty() {
        return Err(Status::invalid_argument(UNRECEIVABLE_ADDRESS));
    }
    let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script_pub_key);
    let range = filter
        .range
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("range is required"))?;
    let start_block = range
        .start
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("range.start is required"))?;
    let start_height = height_clamped_to_tip(start_block.height, visible_tip_height);
    let end_height = match range.end.as_ref().map(|end_block| end_block.height) {
        None | Some(0) => visible_tip_height,
        Some(end_height) => height_clamped_to_tip(end_height, visible_tip_height),
    };
    if start_height > end_height {
        return Err(Status::invalid_argument(
            "range.start.height must not exceed range.end.height",
        ));
    }
    Ok(TransparentAddressTxIdsInRangeRequest {
        address_script_hash,
        start_height,
        end_height,
        max_entries: NonZeroU32::MIN.saturating_add(999),
        from_cursor: None,
    })
}

fn height_clamped_to_tip(height: u64, visible_tip_height: BlockHeight) -> BlockHeight {
    u32::try_from(height).map_or(visible_tip_height, |height| {
        BlockHeight::new(height).min(visible_tip_height)
    })
}

fn transparent_address_unspent_outputs_request(
    address: &str,
    network: Network,
    start_height: BlockHeight,
) -> Result<TransparentAddressUnspentOutputsRequest, Status> {
    let transparent_address = parse_transparent_address(address)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if !transparent_address_matches_network(transparent_address.network_kind(), network) {
        return Err(Status::invalid_argument(
            "transparent address network does not match server network",
        ));
    }

    let script_pub_key = transparent_address.script().as_raw_bytes().to_vec();
    if script_pub_key.is_empty() {
        return Err(Status::invalid_argument(UNRECEIVABLE_ADDRESS));
    }

    Ok(TransparentAddressUnspentOutputsRequest {
        address_script_hash: TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
        start_height,
    })
}

fn lightwalletd_address_utxo(
    address: &str,
    utxo: TransparentUnspentOutput,
) -> Result<lightwalletd::GetAddressUtxosReply, Status> {
    Ok(lightwalletd::GetAddressUtxosReply {
        address: address.to_owned(),
        txid: encode_internal_transaction_id(utxo.outpoint.transaction_id).to_vec(),
        index: i32::try_from(utxo.outpoint.output_index)
            .map_err(|_| Status::data_loss("transparent output index exceeds i32"))?,
        script: utxo.script_pub_key,
        value_zat: i64::try_from(utxo.value_zat)
            .map_err(|_| Status::data_loss("transparent output value exceeds i64"))?,
        height: u64::from(utxo.block_height.value()),
    })
}

fn standard_fee_response(
    fee: Result<zinder_core::StandardFee, QueryError>,
) -> Result<Response<lightwalletd::StandardFee>, Status> {
    match fee {
        Ok(fee) => Ok(Response::new(lightwalletd::StandardFee {
            fee_zat: fee.fee_zat(),
            estimator_version: fee.estimator_version(),
        })),
        Err(QueryError::Node(
            SourceError::NodeCapabilityMissing {
                capability: NodeCapability::StandardFee,
            }
            | SourceError::TransactionBroadcastDisabled,
        )) => Err(Status::unimplemented(
            "GetStandardFee is not implemented: no configured node implements the getstandardfee RPC",
        )),
        Err(QueryError::Node(error @ SourceError::SourceProtocolMismatch { .. })) => {
            Err(Status::internal(format!(
                "GetStandardFee: the node's getstandardfee reply is unusable: {error}"
            )))
        }
        Err(error) => Err(status_from_query_error(&error)),
    }
}

/// Maps a tree-state read failure to the lightwalletd status.
///
/// A rebuild refused for want of a rebuild slot is `UNAVAILABLE`, as a full
/// block-range limit is, so a wallet retries both admission limits the same
/// way. Native `TreeStateAtHeight` keeps `RESOURCE_EXHAUSTED` with its typed
/// reason.
fn tree_state_status(error: &QueryError) -> Status {
    if matches!(error, QueryError::TreeStateReplayLimitReached { .. }) {
        return Status::unavailable(format!("{error}; retry shortly"));
    }
    status_from_query_error(error)
}

/// Maps a read that may be forwarded to the node to the lightwalletd status.
///
/// A read refused for want of a forwarded-read slot is `UNAVAILABLE`, as a
/// full block-range or tree-state rebuild limit is, so a wallet retries every
/// admission limit the same way. Native `WalletQuery` keeps
/// `RESOURCE_EXHAUSTED` with its typed reason.
fn forwarded_read_status(error: &QueryError) -> Status {
    if matches!(error, QueryError::ForwardedReadLimitReached { .. }) {
        return Status::unavailable(error.to_string());
    }
    status_from_query_error(error)
}

/// Maps a subtree-root read failure to the lightwalletd status.
///
/// A page read after the entry epoch is retired fails with `UNAVAILABLE`, so a
/// wallet retries the request rather than treating the cut-off stream as a
/// permanent precondition failure.
fn subtree_roots_status(error: &QueryError) -> Status {
    if matches!(error, QueryError::ChainEpochPinUnavailable { .. }) {
        return Status::unavailable(format!("{error}; retry shortly"));
    }
    status_from_query_error(error)
}

fn pre_sapling_tree_state() -> Status {
    Status::invalid_argument("GetTreeState: z_gettreestate did not return treestate")
}

fn require_sapling_tree_state(tree_state: &TreeState) -> Result<(), Status> {
    if tree_state.frontiers.sapling_final_state.is_none() {
        return Err(pre_sapling_tree_state());
    }
    Ok(())
}

fn lightwalletd_tree_state(tree_state: &TreeState) -> lightwalletd::TreeState {
    lightwalletd::TreeState {
        network: encode_bip70_chain_name(tree_state.chain_epoch.network).to_owned(),
        height: u64::from(tree_state.height.value()),
        hash: encode_rpc_block_hash_hex(tree_state.block_hash),
        time: tree_state.block_time_seconds,
        sapling_tree: tree_state
            .frontiers
            .sapling_final_state
            .as_deref()
            .map_or_default(hex::encode),
        orchard_tree: tree_state
            .frontiers
            .orchard_final_state
            .as_deref()
            .map_or_default(hex::encode),
        ironwood_tree: tree_state
            .frontiers
            .ironwood_final_state
            .as_deref()
            .map_or_default(hex::encode),
    }
}

fn lightwalletd_tree_state_from_hex(
    tree_state: zinder_query::ServedTreeStateHex,
) -> Result<lightwalletd::TreeState, Status> {
    if !tree_state.has_sapling {
        return Err(pre_sapling_tree_state());
    }
    Ok(lightwalletd::TreeState {
        network: encode_bip70_chain_name(tree_state.network).to_owned(),
        height: u64::from(tree_state.height.value()),
        hash: tree_state.hash,
        time: tree_state.block_time_seconds,
        sapling_tree: tree_state.sapling_tree,
        orchard_tree: tree_state.orchard_tree,
        ironwood_tree: tree_state.ironwood_tree,
    })
}

fn lightwalletd_subtree_roots(subtree_roots: &SubtreeRoots) -> Vec<lightwalletd::SubtreeRoot> {
    subtree_roots
        .subtree_roots
        .iter()
        .map(|subtree_root| lightwalletd::SubtreeRoot {
            root_hash: subtree_root.root_hash.as_bytes().to_vec(),
            completing_block_hash: encode_rpc_block_hash_bytes(subtree_root.completing_block_hash)
                .to_vec(),
            completing_block_height: u64::from(subtree_root.completing_block_height.value()),
        })
        .collect()
}

/// Maps a typed broadcast outcome to the lightwalletd `SendResponse` shape.
///
/// Stable error-code scheme so wallet clients can pattern-match without parsing
/// the message string:
///
/// * `0` accepted; `errorMessage` carries the node-reported transaction id hex.
/// * `-22` invalid encoding (Bitcoin/Zcash convention).
/// * `-26` rejected by node policy.
/// * `-27` already in mempool or chain (duplicate).
/// * `-1` unclassified node response.
///
/// When the node reports its own `error_code`, that code is forwarded
/// instead of the default; clients that already track node codes do not
/// need a Zinder-specific table.
///
/// `SendResponse` carries only a code and a message, so the typed
/// `BroadcastRejectionReason` has no legacy field. Every rejection reason,
/// including `UnsupportedTransactionVersion` and `ExceedsBlockActionLimit`,
/// maps through the rejected arm with the node message forwarded verbatim.
#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "non-exhaustive broadcast outcomes from zinder-core must degrade conservatively"
)]
fn send_response_from_broadcast_outcome(
    broadcast_outcome: TransactionBroadcastOutcome,
) -> lightwalletd::SendResponse {
    match broadcast_outcome {
        TransactionBroadcastOutcome::Accepted(BroadcastAccepted { transaction_id }) => {
            lightwalletd::SendResponse {
                error_code: 0,
                error_message: encode_rpc_transaction_id_hex(transaction_id),
            }
        }
        TransactionBroadcastOutcome::InvalidEncoding(BroadcastInvalidEncoding {
            error_code,
            message,
        }) => lightwalletd::SendResponse {
            error_code: classified_send_error_code(error_code, -22),
            error_message: message,
        },
        TransactionBroadcastOutcome::Rejected(BroadcastRejected {
            error_code,
            message,
            kind: _,
        }) => lightwalletd::SendResponse {
            error_code: classified_send_error_code(error_code, -26),
            error_message: message,
        },
        TransactionBroadcastOutcome::Duplicate(BroadcastDuplicate {
            error_code,
            message,
        }) => lightwalletd::SendResponse {
            error_code: classified_send_error_code(error_code, -27),
            error_message: message,
        },
        TransactionBroadcastOutcome::Queued(BroadcastQueued { message }) => {
            // lightwalletd's SendResponse has no queued concept; surface
            // Zebra's underlying -25 Verify code so legacy wallets see the
            // same error code they would have received from zcashd while the
            // download queue drains.
            lightwalletd::SendResponse {
                error_code: -25,
                error_message: message,
            }
        }
        TransactionBroadcastOutcome::Unknown(BroadcastUnknown {
            error_code,
            message,
        }) => lightwalletd::SendResponse {
            error_code: classified_send_error_code(error_code, -1),
            error_message: message,
        },
        _ => lightwalletd::SendResponse {
            error_code: -1,
            error_message: "unclassified transaction broadcast response".to_owned(),
        },
    }
}

fn classified_send_error_code(reported_code: Option<i64>, default_code: i32) -> i32 {
    reported_code
        .and_then(|code| i32::try_from(code).ok())
        .unwrap_or(default_code)
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "zinder-core rejects targets with pointer widths below 32 bits"
)]
pub(crate) const fn u32_to_usize(count: u32) -> usize {
    count as usize
}

fn close_mempool_stream_on_tip_change<S>(
    raw_transaction_stream: S,
    watcher: SharedTipChangeWatcher,
    snapshot_chain_epoch_id: ChainEpochId,
) -> tokio_stream::wrappers::ReceiverStream<Result<lightwalletd::RawTransaction, Status>>
where
    S: tonic::codegen::tokio_stream::Stream<Item = Result<lightwalletd::RawTransaction, Status>>
        + Send
        + 'static,
{
    let (output_sender, output_receiver) =
        tokio::sync::mpsc::channel(MEMPOOL_STREAM_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let mut raw_transaction_stream = Box::pin(raw_transaction_stream);
        let mut tip_change_signal =
            Box::pin(watcher.await_tip_change_after(snapshot_chain_epoch_id));
        loop {
            let permit = tokio::select! {
                biased;
                () = output_sender.closed() => return,
                tip_change_outcome = &mut tip_change_signal => {
                    drop(raw_transaction_stream);
                    if let Err(error) = record_mempool_stream_tip_signal(tip_change_outcome) {
                        // Ending with OK would tell the client a new block
                        // arrived when no tip change was observed.
                        let _ = output_sender
                            .send(Err(Status::unavailable(error.to_string())))
                            .await;
                    }
                    return;
                }
                permit_outcome = output_sender.reserve() => match permit_outcome {
                    Ok(permit) => permit,
                    Err(_) => return,
                },
            };
            tokio::select! {
                biased;
                () = output_sender.closed() => return,
                tip_change_outcome = &mut tip_change_signal => {
                    drop(raw_transaction_stream);
                    if let Err(error) = record_mempool_stream_tip_signal(tip_change_outcome) {
                        permit.send(Err(Status::unavailable(error.to_string())));
                    }
                    return;
                }
                outcome = tonic::codegen::tokio_stream::StreamExt::next(&mut raw_transaction_stream) => {
                    match outcome {
                        Some(raw_transaction_outcome) => permit.send(raw_transaction_outcome),
                        None => {
                            // Underlying mempool source ended; let the
                            // gRPC stream end naturally.
                            return;
                        }
                    }
                }
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(output_receiver)
}

fn record_mempool_stream_tip_signal(
    tip_change_outcome: Result<(), TipChangeWatcherError>,
) -> Result<(), TipChangeWatcherError> {
    match tip_change_outcome {
        Ok(()) => {
            tracing::debug!(
                target: "zinder::compat_lightwalletd",
                event = "mempool_stream_close_on_tip_change",
                "GetMempoolStream closing on observed best-chain tip change"
            );
            Ok(())
        }
        Err(error @ TipChangeWatcherError::SignalClosed) => {
            tracing::warn!(
                target: "zinder::compat_lightwalletd",
                event = "mempool_stream_tip_signal_closed",
                error = %error,
                "tip-change signal source closed; GetMempoolStream will end"
            );
            Err(error)
        }
    }
}

/// Passes each item whose `key` it has not yielded before, and errors are
/// passed through unchanged.
///
/// After `max_distinct` distinct keys, the next new key ends the stream with
/// `UNAVAILABLE`, which lightwalletd clients retry, instead of letting the
/// seen set grow or letting a repeat through.
fn each_once_up_to<S, KeyFn, K>(
    streamed: S,
    key: KeyFn,
    max_distinct: usize,
) -> EachOnceUpTo<S, KeyFn, K> {
    EachOnceUpTo {
        upstream: Some(Box::pin(streamed)),
        key,
        seen: HashSet::new(),
        max_distinct,
    }
}

/// The stream [`each_once_up_to`] returns.
///
/// On exhaustion it drops its upstream and its seen set before yielding the
/// error, so it ends without polling the upstream again.
struct EachOnceUpTo<S, KeyFn, K> {
    upstream: Option<Pin<Box<S>>>,
    key: KeyFn,
    seen: HashSet<K>,
    max_distinct: usize,
}

impl<S, KeyFn, K, T> tonic::codegen::tokio_stream::Stream for EachOnceUpTo<S, KeyFn, K>
where
    S: tonic::codegen::tokio_stream::Stream<Item = Result<T, Status>>,
    KeyFn: Fn(&T) -> K + Unpin,
    K: Eq + std::hash::Hash + Unpin,
{
    type Item = Result<T, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let Some(upstream) = this.upstream.as_mut() else {
                return std::task::Poll::Ready(None);
            };
            let outcome = match upstream.as_mut().poll_next(context) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(None) => {
                    this.upstream = None;
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Ready(Some(outcome)) => outcome,
            };
            let streamed = match outcome {
                Ok(streamed) => streamed,
                Err(status) => return std::task::Poll::Ready(Some(Err(status))),
            };
            let streamed_key = (this.key)(&streamed);
            if this.seen.contains(&streamed_key) {
                continue;
            }
            if this.seen.len() >= this.max_distinct {
                this.upstream = None;
                this.seen = HashSet::new();
                return std::task::Poll::Ready(Some(Err(Status::unavailable(format!(
                    "stream sent {} distinct transactions without a tip change; reconnect",
                    this.max_distinct
                )))));
            }
            this.seen.insert(streamed_key);
            return std::task::Poll::Ready(Some(Ok(streamed)));
        }
    }
}

fn stream_mempool_compact_transactions(
    mempool_surface: SharedMempoolSurface,
    first_page: MempoolSnapshotPage,
    shape: CompactBlockShape,
) -> GrpcStream<lightwalletd::CompactTx> {
    if first_page.entries.is_empty() && first_page.next_cursor.is_none() {
        return Box::pin(stream::empty());
    }
    if first_page.next_cursor.is_none() {
        return Box::pin(MempoolCompactTxStream::new(
            stream::iter(first_page.entries.into_iter().map(Ok)),
            shape,
        ));
    }
    Box::pin(MempoolCompactTxStream::new(
        mempool_snapshot_entries(mempool_surface, first_page),
        shape,
    ))
}

async fn buffered_mempool_transactions(
    mempool_surface: SharedMempoolSurface,
    first_page: MempoolSnapshotPage,
    shape: CompactBlockShape,
    exclude_txid_suffixes: Vec<Vec<u8>>,
) -> Result<GrpcStream<lightwalletd::CompactTx>, Status> {
    let mut entries = mempool_snapshot_entries(mempool_surface, first_page);
    let mut exclusions = MempoolExclusions::new(exclude_txid_suffixes);
    let mut compact_messages = Vec::new();
    while let Some(entry_outcome) = entries.next().await {
        let entry = entry_outcome?;
        exclusions.observe(entry.transaction_id());
        if let Some(message) = encode_mempool_transaction(shape, entry)? {
            compact_messages.push(message);
        }
    }
    let messages = compact_messages
        .into_iter()
        .filter(move |message| !exclusions.uniquely_matches(&message.txid))
        .map(Ok);
    Ok(Box::pin(stream::iter(messages)))
}

fn encode_mempool_transaction(
    shape: CompactBlockShape,
    entry: zinder_core::MempoolEntry,
) -> Result<Option<lightwalletd::CompactTx>, Status> {
    let entry = entry.into_parts();
    shape.encode_transaction(CompactTransaction {
        index: 0,
        transaction_id: entry.transaction_id,
        data: entry.compact_transaction_data,
    })
}

/// Streams `CompactTx` messages encoded lazily from a mempool snapshot walk.
///
/// Entries excluded by `shape` are skipped. On the first upstream or encoding
/// error, the upstream entry stream is dropped before yielding the error so
/// subsequent polls end the stream without reading further pages.
struct MempoolCompactTxStream<S> {
    entries: Option<S>,
    shape: CompactBlockShape,
}

impl<S> MempoolCompactTxStream<S> {
    const fn new(entries: S, shape: CompactBlockShape) -> Self {
        Self {
            entries: Some(entries),
            shape,
        }
    }
}

impl<S> tonic::codegen::tokio_stream::Stream for MempoolCompactTxStream<S>
where
    S: tonic::codegen::tokio_stream::Stream<Item = Result<zinder_core::MempoolEntry, Status>>
        + Unpin,
{
    type Item = Result<lightwalletd::CompactTx, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let Some(entries) = this.entries.as_mut() else {
                return std::task::Poll::Ready(None);
            };
            let outcome = match Pin::new(entries).poll_next(context) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(None) => {
                    this.entries = None;
                    return std::task::Poll::Ready(None);
                }
                std::task::Poll::Ready(Some(outcome)) => outcome,
            };
            let entry = match outcome {
                Ok(entry) => entry,
                Err(status) => {
                    this.entries = None;
                    return std::task::Poll::Ready(Some(Err(status)));
                }
            };
            match encode_mempool_transaction(this.shape, entry) {
                Ok(Some(message)) => return std::task::Poll::Ready(Some(Ok(message))),
                Ok(None) => {}
                Err(status) => {
                    this.entries = None;
                    return std::task::Poll::Ready(Some(Err(status)));
                }
            }
        }
    }
}

async fn take_or_reserve_mempool_entry_slot<'a>(
    reserved_slot: &mut Option<
        tokio::sync::mpsc::Permit<'a, Result<zinder_core::MempoolEntry, Status>>,
    >,
    entry_sender: &'a tokio::sync::mpsc::Sender<Result<zinder_core::MempoolEntry, Status>>,
) -> Option<tokio::sync::mpsc::Permit<'a, Result<zinder_core::MempoolEntry, Status>>> {
    if let Some(slot) = reserved_slot.take() {
        return Some(slot);
    }
    entry_sender.reserve().await.ok()
}

/// Streams live-mempool entries page by page, starting with the
/// already-fetched first page and fetching later pages only as the consumer
/// pulls, so the walk never materializes the whole mempool.
fn mempool_snapshot_entries(
    mempool_surface: SharedMempoolSurface,
    first_page: MempoolSnapshotPage,
) -> tokio_stream::wrappers::ReceiverStream<Result<zinder_core::MempoolEntry, Status>> {
    let (entry_sender, entry_receiver) =
        tokio::sync::mpsc::channel(MEMPOOL_SNAPSHOT_ENTRY_CHANNEL_CAPACITY);
    tokio::spawn(async move {
        let mut page = first_page;
        let mut reserved_slot = None;
        loop {
            for entry in page.entries {
                let Some(slot) =
                    take_or_reserve_mempool_entry_slot(&mut reserved_slot, &entry_sender).await
                else {
                    return;
                };
                slot.send(Ok(entry));
            }
            let Some(cursor) = page.next_cursor else {
                return;
            };
            let Some(slot) =
                take_or_reserve_mempool_entry_slot(&mut reserved_slot, &entry_sender).await
            else {
                return;
            };
            if entry_sender.is_closed() {
                return;
            }
            let next_page = tokio::select! {
                biased;
                () = entry_sender.closed() => return,
                outcome = mempool_surface.mempool_snapshot_page(
                    LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE,
                    Some(cursor),
                ) => outcome,
            };
            page = match next_page {
                Ok(next_page) => {
                    reserved_slot = Some(slot);
                    next_page
                }
                Err(error) => {
                    slot.send(Err(status_from_mempool_surface_error(error)));
                    return;
                }
            };
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(entry_receiver)
}

fn status_from_mempool_surface_error(error: MempoolSurfaceError) -> Status {
    match error {
        MempoolSurfaceError::Unavailable { reason } => Status::unavailable(reason),
        MempoolSurfaceError::UnsupportedEncoding { reason } => zinder_proto::status_for_reason(
            zinder_proto::v1::ops::ErrorReason::UnsupportedWalletEncoding,
            reason,
        ),
        MempoolSurfaceError::CursorInvalid => Status::invalid_argument("mempool cursor is invalid"),
        MempoolSurfaceError::CursorExpired => Status::failed_precondition("mempool cursor expired"),
    }
}

fn validate_excluded_txid_suffixes(exclude_suffixes: &[Vec<u8>]) -> Result<(), Status> {
    if exclude_suffixes.len() > MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST {
        return Err(Status::invalid_argument(format!(
            "exclude_txid_suffixes contains {} entries; at most {MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST} are allowed",
            exclude_suffixes.len(),
        )));
    }
    for (index, suffix) in exclude_suffixes.iter().enumerate() {
        if suffix.len() > 32 {
            return Err(Status::invalid_argument(format!(
                "exclude txid {index} is larger than 32 bytes",
            )));
        }
    }
    Ok(())
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "MempoolEvent is #[non_exhaustive]; the lightwalletd compat shim only projects Added events into RawTransaction, so every non-Added variant is filtered out of the GetMempoolStream projection."
)]
fn added_mempool_entry(
    event_outcome: Result<zinder_store::MempoolEventEnvelope, MempoolSurfaceError>,
) -> Option<Result<zinder_core::MempoolEntry, Status>> {
    match event_outcome {
        Ok(envelope) => match envelope.event {
            MempoolEvent::Added { entry } => Some(Ok(entry)),
            _ => None,
        },
        Err(error) => Some(Err(status_from_mempool_surface_error(error))),
    }
}

/// A `GetTransaction` canonical read that did not find the transaction.
enum CanonicalTransactionMiss {
    /// The canonical chain of `at_epoch_id` does not hold the transaction.
    NotInChain {
        /// Chain epoch the canonical read searched.
        at_epoch_id: ChainEpochId,
    },
    /// The store's history starts after a checkpoint, so the transaction may
    /// be mined below the floor this status names.
    BelowHistoryFloor(Status),
}

impl CanonicalTransactionMiss {
    fn into_status(self) -> Status {
        match self {
            Self::NotInChain { .. } => {
                Status::not_found("transaction is not in the canonical chain or the live mempool")
            }
            Self::BelowHistoryFloor(history_floor) => history_floor,
        }
    }
}

fn mempool_raw_transaction(entry: &zinder_core::MempoolEntry) -> lightwalletd::RawTransaction {
    lightwalletd::RawTransaction {
        data: entry.raw_transaction_bytes().as_slice().to_vec(),
        // Reference lightwalletd reports pending mempool transactions with
        // height 0. The observed chain epoch stays native-only metadata.
        height: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use zinder_core::BlockHash;
    use zinder_core::{
        ArtifactSchemaVersion, ChainTipMetadata, TransparentAddressBalance, UnixTimestampMillis,
    };

    #[test]
    fn a_forwarded_read_refusal_is_unavailable_and_other_errors_keep_their_status() {
        let refused = forwarded_read_status(&QueryError::ForwardedReadLimitReached {
            kind: "transaction",
            max_concurrent: 24,
        });
        assert_eq!(refused.code(), tonic::Code::Unavailable);
        assert!(refused.message().contains("retry with backoff"));
        let corrupt = forwarded_read_status(&QueryError::ArtifactCorrupt {
            family: zinder_store::ArtifactFamily::TransactionLocation,
            reason: "probe".to_owned(),
        });
        assert_eq!(corrupt.code(), tonic::Code::DataLoss);
    }

    fn test_chain_epoch() -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(1),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(1),
            visible_tip_hash: BlockHash::from_bytes([1; 32]),
            settled_tip_height: BlockHeight::new(1),
            settled_tip_hash: BlockHash::from_bytes([1; 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(0),
        }
    }

    fn transparent_address_balance(confirmed_zat: u64) -> TransparentAddressBalance {
        TransparentAddressBalance {
            confirmed_zat,
            address_count: 1,
            chain_epoch: test_chain_epoch(),
        }
    }

    #[test]
    fn address_utxo_budget_counts_the_reply_list_encoding() -> Result<(), Status> {
        let reply = lightwalletd::GetAddressUtxosReply {
            script: vec![0; 128],
            height: 1,
            ..Default::default()
        };
        let encoded_bytes = lightwalletd::GetAddressUtxosReplyList {
            address_utxos: vec![reply.clone()],
        }
        .encoded_len();
        let mut budget = AddressUtxoResponseBudget::new(2);
        budget.bytes = ADDRESS_UTXO_RESPONSE_MAX_BYTES - encoded_bytes;
        budget.record(&reply)?;
        assert_eq!(budget.bytes, ADDRESS_UTXO_RESPONSE_MAX_BYTES);
        assert!(matches!(
            budget.record(&reply),
            Err(status) if status.code() == tonic::Code::ResourceExhausted
        ));
        Ok(())
    }

    /// Up to eight addresses read full first pages, more addresses share the
    /// budget down to 16 outputs each, and `maxEntries` caps the share.
    #[test]
    fn address_utxo_first_pages_share_the_request_budget() {
        let cases = [
            (0, None, 1_024),
            (1, None, 1_024),
            (8, None, 1_024),
            (9, None, 910),
            (60, None, 136),
            (60, Some(30), 30),
            (1, Some(3), 16),
            (1, Some(100_000), 1_024),
            (512, None, 16),
            (513, None, 16),
            (10_000, None, 16),
        ];
        for (distinct_addresses, max_entries, expected) in cases {
            assert_eq!(
                address_utxo_first_page_size(distinct_addresses, max_entries).get(),
                expected,
                "{distinct_addresses} addresses, maxEntries {max_entries:?}"
            );
        }
    }

    /// The first pages of one request hold at most 8,192 outputs together,
    /// or 16 for each distinct address past 512, the bound the merge states.
    #[test]
    fn address_utxo_first_pages_stay_within_the_stated_bound() {
        for distinct_addresses in 1..=10_000 {
            let first_page =
                usize::from(address_utxo_first_page_size(distinct_addresses, None).get());
            assert!(
                distinct_addresses * first_page
                    <= ADDRESS_UTXO_FIRST_PAGE_BUDGET.max(16 * distinct_addresses),
                "{distinct_addresses} addresses read first pages of {first_page} outputs"
            );
        }
    }

    /// A response source that counts its reads, can stop answering after a
    /// number of reads, and reports when it is dropped.
    struct ObservedSource {
        reads: Arc<AtomicUsize>,
        answered_reads: usize,
        dropped: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl ObservedSource {
        fn new(
            answered_reads: usize,
        ) -> (Self, Arc<AtomicUsize>, tokio::sync::oneshot::Receiver<()>) {
            let reads = Arc::new(AtomicUsize::new(0));
            let (dropped, on_drop) = tokio::sync::oneshot::channel();
            let source = Self {
                reads: Arc::clone(&reads),
                answered_reads,
                dropped: Some(dropped),
            };
            (source, reads, on_drop)
        }
    }

    impl ResponseSource for ObservedSource {
        type Message = usize;

        async fn next(&mut self) -> Option<Result<usize, Status>> {
            let read = self.reads.fetch_add(1, Ordering::SeqCst);
            if read >= self.answered_reads {
                std::future::pending::<()>().await;
            }
            Some(Ok(read))
        }
    }

    impl Drop for ObservedSource {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    #[tokio::test]
    async fn response_stream_reads_no_further_ahead_of_an_unread_client_than_its_capacity() {
        const CAPACITY: usize = 8;
        let (source, reads, _on_drop) = ObservedSource::new(usize::MAX);

        let _response = spawn_response_stream(source, CAPACITY);
        // The source answers at once, so the reader task reaches its bound
        // during this sleep.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(reads.load(Ordering::SeqCst), CAPACITY);
    }

    #[tokio::test]
    async fn response_stream_drops_its_source_when_the_client_leaves_during_a_read()
    -> Result<(), Status> {
        let (source, reads, on_drop) = ObservedSource::new(1);
        let mut response = spawn_response_stream(source, 8);
        assert_eq!(response.next().await.transpose()?, Some(0));
        while reads.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }

        drop(response);

        tokio::time::timeout(std::time::Duration::from_secs(5), on_drop)
            .await
            .map_err(|_| Status::deadline_exceeded("the source outlived its client"))?
            .map_err(|_| Status::internal("the source must report its drop"))
    }

    #[tokio::test]
    async fn each_once_up_to_skips_repeats_and_ends_unavailable_past_the_bound()
    -> Result<(), tokio::time::error::Elapsed> {
        // The upstream never ends, so only the bound itself can end the stream.
        let streamed = tokio_stream::iter([1, 2, 1, 2, 3, 4].map(Ok::<u8, Status>))
            .chain(tokio_stream::pending());
        let outcomes: Vec<Result<u8, Status>> = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            each_once_up_to(streamed, |id: &u8| *id, 2).collect(),
        )
        .await?;

        let codes: Vec<Result<u8, tonic::Code>> = outcomes
            .into_iter()
            .map(|outcome| outcome.map_err(|status| status.code()))
            .collect();
        assert_eq!(codes, vec![Ok(1), Ok(2), Err(tonic::Code::Unavailable)]);
        Ok(())
    }

    fn tree_state_with_frontiers(frontiers: zinder_query::TreeStateFrontiers) -> TreeState {
        TreeState {
            chain_epoch: test_chain_epoch(),
            height: BlockHeight::new(1),
            block_hash: BlockHash::from_bytes([1; 32]),
            block_time_seconds: 1_774_668_401,
            frontiers,
        }
    }

    #[test]
    fn tree_state_maps_every_pool_final_state_and_the_typed_identity() {
        let tree_state = lightwalletd_tree_state(&tree_state_with_frontiers(
            zinder_query::TreeStateFrontiers {
                sapling_final_state: Some(vec![0xaa]),
                orchard_final_state: Some(vec![0xbb]),
                ironwood_final_state: Some(vec![0xcc]),
            },
        ));

        assert_eq!(
            tree_state.network,
            encode_bip70_chain_name(Network::ZcashRegtest)
        );
        assert_eq!(tree_state.height, 1);
        assert_eq!(
            tree_state.hash,
            encode_rpc_block_hash_hex(BlockHash::from_bytes([1; 32]))
        );
        assert_eq!(tree_state.time, 1_774_668_401);
        assert_eq!(tree_state.sapling_tree, "aa");
        assert_eq!(tree_state.orchard_tree, "bb");
        assert_eq!(tree_state.ironwood_tree, "cc");
    }

    #[test]
    fn tree_state_reports_an_inactive_pool_as_an_empty_tree() {
        let tree_state = lightwalletd_tree_state(&tree_state_with_frontiers(
            zinder_query::TreeStateFrontiers {
                sapling_final_state: Some(vec![0x00, 0x00, 0x00]),
                orchard_final_state: None,
                ironwood_final_state: None,
            },
        ));

        assert_eq!(tree_state.sapling_tree, "000000");
        assert_eq!(tree_state.orchard_tree, "");
        assert_eq!(tree_state.ironwood_tree, "");
    }

    #[test]
    fn lightwalletd_balance_projection_reports_the_confirmed_total() -> Result<(), Status> {
        let value_zat = lightwalletd_balance_value_zat(transparent_address_balance(100))?;

        assert_eq!(value_zat, 100);
        Ok(())
    }

    #[test]
    fn lightwalletd_balance_projection_rejects_a_total_above_the_int64_field() {
        let outcome = lightwalletd_balance_value_zat(transparent_address_balance(u64::MAX));

        assert!(matches!(
            outcome,
            Err(status) if status.code() == tonic::Code::OutOfRange
        ));
    }

    struct PendingDropStream {
        remaining_messages: u8,
        dropped: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl PendingDropStream {
        fn new(remaining_messages: u8) -> (Self, tokio::sync::oneshot::Receiver<()>) {
            let (dropped, on_drop) = tokio::sync::oneshot::channel();
            (
                Self {
                    remaining_messages,
                    dropped: Some(dropped),
                },
                on_drop,
            )
        }
    }

    impl tonic::codegen::tokio_stream::Stream for PendingDropStream {
        type Item = Result<lightwalletd::RawTransaction, Status>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            if self.remaining_messages == 0 {
                return std::task::Poll::Pending;
            }
            let next_byte = self.remaining_messages;
            self.remaining_messages -= 1;
            std::task::Poll::Ready(Some(Ok(lightwalletd::RawTransaction {
                data: vec![next_byte],
                height: 0,
            })))
        }
    }

    impl Drop for PendingDropStream {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    #[tokio::test]
    async fn mempool_stream_wrapper_drops_idle_upstream_when_receiver_drops() -> Result<(), Status>
    {
        let (_tip_sender, tip_receiver) = tokio::sync::watch::channel(7_u64);
        let watcher: SharedTipChangeWatcher =
            Arc::new(crate::mempool::WatchTipChangeWatcher::new(tip_receiver));
        let (upstream, on_drop) = PendingDropStream::new(0);
        let response = close_mempool_stream_on_tip_change(upstream, watcher, ChainEpochId::new(7));
        tokio::task::yield_now().await;

        drop(response);

        tokio::time::timeout(Duration::from_millis(500), on_drop)
            .await
            .map_err(|_| Status::deadline_exceeded("idle upstream outlived dropped receiver"))?
            .map_err(|_| Status::internal("upstream drop sender disappeared"))
    }

    #[tokio::test]
    async fn mempool_stream_wrapper_releases_upstream_on_closed_tip_signal_with_full_queue()
    -> Result<(), Status> {
        let (tip_sender, tip_receiver) = tokio::sync::watch::channel(7_u64);
        let watcher: SharedTipChangeWatcher =
            Arc::new(crate::mempool::WatchTipChangeWatcher::new(tip_receiver));
        let (upstream, on_drop) = PendingDropStream::new(17);
        let mut response =
            close_mempool_stream_on_tip_change(upstream, watcher, ChainEpochId::new(7));
        tokio::time::sleep(Duration::from_millis(20)).await;

        drop(tip_sender);

        tokio::time::timeout(Duration::from_millis(500), on_drop)
            .await
            .map_err(|_| Status::deadline_exceeded("upstream stayed open on closed tip signal"))?
            .map_err(|_| Status::internal("upstream drop sender disappeared"))?;
        for _ in 0..16 {
            let buffered = response
                .next()
                .await
                .ok_or_else(|| Status::internal("missing buffered message"))?;
            assert!(buffered.is_ok());
        }
        let terminal = response
            .next()
            .await
            .ok_or_else(|| Status::internal("missing UNAVAILABLE status"))?;
        let Err(status) = terminal else {
            return Err(Status::internal("expected UNAVAILABLE status after queue"));
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);
        Ok(())
    }

    fn unit_mempool_entry(
        txid_byte: u8,
        fee_zat: Option<u64>,
    ) -> Result<zinder_core::MempoolEntry, zinder_core::MempoolEntryBuildError> {
        let compact_transaction_data = zinder_core::CompactTransactionData {
            fee_zat,
            sapling_outputs: vec![zinder_core::CompactSaplingOutput {
                commitment: [txid_byte; 32],
                ephemeral_key: [txid_byte; 32],
                ciphertext: [txid_byte; 52],
            }],
            ..zinder_core::CompactTransactionData::default()
        };
        zinder_core::MempoolEntry::new(
            TransactionId::from_bytes([txid_byte; 32]),
            None,
            RawTransactionBytes::new(vec![txid_byte; 16]),
            compact_transaction_data,
            zinder_core::MempoolObservation {
                first_seen_unix_millis: UnixTimestampMillis::new(1_700_000_000_000),
                first_seen_chain_epoch: test_chain_epoch(),
            },
        )
    }

    struct DelayedUnitMempoolSurface {
        fetched_pages: Arc<AtomicUsize>,
        page_two_reached: Arc<tokio::sync::Notify>,
        page_two_dropped: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    struct UnitDropGuard(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for UnitDropGuard {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::mempool::MempoolSurface for DelayedUnitMempoolSurface {
        async fn mempool_snapshot_page(
            &self,
            _max_entries: u32,
            _from_cursor: Option<Vec<u8>>,
        ) -> Result<MempoolSnapshotPage, MempoolSurfaceError> {
            self.fetched_pages.fetch_add(1, Ordering::SeqCst);
            let _drop_guard = UnitDropGuard(self.page_two_dropped.lock().take());
            self.page_two_reached.notify_waiters();
            std::future::pending::<()>().await;
            Err(MempoolSurfaceError::Unavailable {
                reason: "unreachable".to_owned(),
            })
        }

        async fn mempool_events(
            &self,
            _from_cursor: Option<zinder_store::StreamCursorTokenV1>,
        ) -> Result<crate::mempool::MempoolEventEnvelopeStream, MempoolSurfaceError> {
            Err(MempoolSurfaceError::Unavailable {
                reason: "unused in unit test".to_owned(),
            })
        }

        async fn mempool_transaction(
            &self,
            _transaction_id: TransactionId,
        ) -> Result<MempoolTransactionLookup, MempoolSurfaceError> {
            Err(MempoolSurfaceError::Unavailable {
                reason: "unused in unit test".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn mempool_snapshot_entries_waits_for_channel_capacity_before_fetching_next_page()
    -> Result<(), Box<dyn std::error::Error>> {
        let fetched_pages = Arc::new(AtomicUsize::new(0));
        let page_two_reached = Arc::new(tokio::sync::Notify::new());
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let surface: SharedMempoolSurface = Arc::new(DelayedUnitMempoolSurface {
            fetched_pages: Arc::clone(&fetched_pages),
            page_two_reached: Arc::clone(&page_two_reached),
            page_two_dropped: parking_lot::Mutex::new(Some(dropped_tx)),
        });
        let first_entries = (0..MEMPOOL_SNAPSHOT_ENTRY_CHANNEL_CAPACITY)
            .map(|idx| unit_mempool_entry(u8::try_from(idx + 1).unwrap_or(u8::MAX), Some(10)))
            .collect::<Result<Vec<_>, _>>()?;
        let first_page = MempoolSnapshotPage {
            chain_epoch_id: ChainEpochId::new(1),
            events_resume_cursor: None,
            entries: first_entries,
            next_cursor: Some(vec![1]),
        };

        let mut entry_stream = mempool_snapshot_entries(surface, first_page);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(fetched_pages.load(Ordering::SeqCst), 0);

        let page_two_wait = page_two_reached.notified();
        let first_entry = entry_stream
            .next()
            .await
            .ok_or_else(|| Status::internal("missing first entry"))??;
        assert_eq!(
            first_entry.transaction_id(),
            TransactionId::from_bytes([1; 32])
        );
        tokio::time::timeout(Duration::from_secs(2), page_two_wait).await?;
        assert_eq!(fetched_pages.load(Ordering::SeqCst), 1);

        drop(entry_stream);
        tokio::time::timeout(Duration::from_secs(2), dropped_rx).await??;
        Ok(())
    }

    struct CountingEntryStream {
        entries: std::vec::IntoIter<Result<zinder_core::MempoolEntry, Status>>,
        dropped: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl tonic::codegen::tokio_stream::Stream for CountingEntryStream {
        type Item = Result<zinder_core::MempoolEntry, Status>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            std::task::Poll::Ready(self.entries.next())
        }
    }

    impl Drop for CountingEntryStream {
        fn drop(&mut self) {
            if let Some(sender) = self.dropped.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn mempool_compact_tx_stream_drops_upstream_and_terminates_on_encode_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let upstream = CountingEntryStream {
            entries: vec![
                Ok(unit_mempool_entry(0x11, Some(10))?),
                Ok(unit_mempool_entry(0x22, Some(u64::from(u32::MAX) + 1))?),
                Ok(unit_mempool_entry(0x33, Some(10))?),
            ]
            .into_iter(),
            dropped: Some(dropped_tx),
        };
        let shape = CompactBlockShape::for_mempool(&[])?;
        let mut tx_stream = MempoolCompactTxStream::new(upstream, shape);

        let first = tx_stream
            .next()
            .await
            .ok_or_else(|| Status::internal("missing first compact tx"))??;
        assert_eq!(first.txid, vec![0x11; 32]);

        let second = tx_stream
            .next()
            .await
            .ok_or_else(|| Status::internal("missing encode error"))?;
        let Err(status) = second else {
            return Err("expected DataLoss error on oversized fee".into());
        };
        assert_eq!(status.code(), tonic::Code::DataLoss);
        tokio::time::timeout(Duration::from_secs(2), dropped_rx).await??;
        assert!(tx_stream.next().await.is_none());
        Ok(())
    }
}
