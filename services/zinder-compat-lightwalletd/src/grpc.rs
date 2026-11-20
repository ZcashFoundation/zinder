//! gRPC implementation for the vendored lightwalletd protocol.

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashSet},
    num::{NonZeroU16, NonZeroU32},
    pin::Pin,
    time::Duration,
};

use thiserror::Error;
use tokio_stream as stream;
use tokio_stream::StreamExt as _;
use tonic::{Request, Response, Status};
use zinder_core::wire::WireDecodeError;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockSelector, BroadcastAccepted, BroadcastDuplicate,
    BroadcastInvalidEncoding, BroadcastQueued, BroadcastRejected, BroadcastUnknown, ChainEpochId,
    CompactBlockArtifact, CompactTransaction, CompactTransactionData,
    MAX_SUBTREE_ROOTS_PER_REQUEST, MinedTransaction, Network, NetworkUpgradeActivations,
    RawTransactionBytes, ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange,
    TransactionBroadcastOutcome, TransactionId, TransparentAddressScriptHash,
    TransparentAddressTxIndexArtifact, TransparentUnspentOutput, TxStatus,
    wire::{
        decode_internal_transaction_id, encode_bip70_chain_name, encode_branch_id_hex,
        encode_internal_block_hash, encode_internal_transaction_id, encode_rpc_block_hash_bytes,
        encode_rpc_block_hash_hex, encode_rpc_transaction_id_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    self, LIGHTWALLETD_PROTOCOL_VERSION, compact_tx_streamer_server,
};
use zinder_proto::v1::wallet::{self as wallet_proto, address_lookup};
use zinder_query::{
    ArtifactKey, CompactBlockRange, MAX_TRANSPARENT_ADDRESS_BALANCE_ADDRESSES, QueryError,
    SubtreeRoots, TRANSPARENT_UNSPENT_OUTPUT_PAGE_SIZE, TransparentAddressTxIdsInRangeRequest,
    TransparentAddressUnspentOutputs, TransparentAddressUnspentOutputsRequest, TreeState,
    WalletQueryApi, WalletServingQuery, address_lookup_to_script_hash, status_from_query_error,
};
use zinder_runtime::RepeatedFieldGuard;
use zinder_source::{NodeCapability, SourceError};
use zinder_source::{parse_transparent_address, transparent_address_matches_network};
use zinder_store::{ArtifactFamily, MempoolEvent, RawBlobRetention};

use crate::block_range::{
    BLOCK_RANGE_CHUNK_MAX_BYTES, BlockRangeChunks, BlockRangeContinuity, BlockRangeSink,
    BlockRangeStage, BlockRangeStreamLimit, BlockRangeStreamSlot,
    DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS,
};
use crate::mempool::{
    MempoolSnapshotPage, MempoolSurfaceError, MempoolTransactionLookup, SharedMempoolSurface,
    SharedTipChangeWatcher, TipChangeWatcherError,
};
use crate::network_tip_estimate::estimated_network_height;
use crate::node_tip_witness::NodeTipWitness;
use crate::ping::LightwalletdPing;
use crate::request_limits::COMPACT_TX_STREAMER_FIELD_LIMITS;
use crate::stale_tip::{StaleTipLimit, TipBlockTime, TipBlockTimeCache, unix_now_seconds};

type GrpcStream<T> =
    Pin<Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// The visible tip a tip call answers with, the Unix time of its block, and
/// the time it was read.
struct ServedTip {
    block: zinder_query::VisibleTipBlock,
    times: TipBlockTime,
    now_unix_seconds: i64,
}

#[derive(Clone, Copy)]
struct EntryChainView {
    epoch_id: ChainEpochId,
    visible_tip_height: BlockHeight,
}

/// Default number of subtree roots one native read returns while `GetSubtreeRoots` streams.
pub const DEFAULT_MAX_LIGHTWALLETD_SUBTREE_ROOTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);
/// Default maximum transparent UTXOs one `GetAddressUtxos` response returns.
///
/// It equals Zebra's `getaddressutxos` result cap, which bounds lightwalletd
/// on Zebra the same way.
pub const DEFAULT_MAX_LIGHTWALLETD_ADDRESS_UTXOS: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(99_999);
/// Native per-request subtree-root cap expressed as a `NonZeroU32`.
const MAX_SUBTREE_ROOT_PAGE_SIZE: NonZeroU32 =
    NonZeroU32::MIN.saturating_add(MAX_SUBTREE_ROOTS_PER_REQUEST - 1);
/// Page size used to drain the native mempool snapshot for lightwalletd.
const LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE: u32 = 1024;
/// Blocks a slow `GetBlockRange` client can stall behind, on top of the chunk
/// the producer is currently sending.
const COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY: usize = 16;
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
pub(crate) const MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST: usize = 1024;
/// Most distinct transactions one `GetMempoolStream` sends between tip changes.
///
/// The stream remembers each id it sent so it can skip repeats, and that set
/// only resets when the stream closes on a tip change. The bound keeps a
/// stalled tip from growing it without limit; it is far above what a Zcash
/// mempool turns over in one block interval.
const MAX_MEMPOOL_STREAM_TRANSACTIONS: usize = 100_000;

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
    /// `maxEntries = 0` means every matching UTXO, as in lightwalletd. A
    /// request that would return more than this bound fails with
    /// `RESOURCE_EXHAUSTED` instead of dropping rows, so a wallet never sums
    /// a partial UTXO set as its balance.
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
    /// The admitted canonical store does not retain transaction bytes.
    #[error(
        "lightwalletd transparent compatibility requires transaction retention, but the admitted canonical store retains {retention}"
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
    block_range_streams: BlockRangeStreamLimit,
    stale_tip_limit: StaleTipLimit,
    tip_block_times: TipBlockTimeCache,
    node_tip_witness: NodeTipWitness,
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
            .field(
                "max_concurrent_block_range_streams",
                &self.block_range_streams.max_streams(),
            )
            .field("stale_tip_limit", &self.stale_tip_limit.limit())
            .field("tip_block_times", &self.tip_block_times)
            .field("node_tip_witness", &self.node_tip_witness)
            .field(
                "transparent_address_support",
                &self.transparent_address_support,
            )
            .field("mempool_surface", &self.mempool_surface.is_some())
            .field("tip_change_watcher", &self.tip_change_watcher.is_some())
            .field("ping_enabled", &self.ping.is_enabled())
            .finish()
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
            block_range_streams: BlockRangeStreamLimit::new(
                DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS,
            ),
            stale_tip_limit: StaleTipLimit::DISABLED,
            tip_block_times: TipBlockTimeCache::default(),
            node_tip_witness: NodeTipWitness::default(),
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
    /// [`DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS`].
    #[must_use]
    pub fn with_max_concurrent_block_range_streams(mut self, max_streams: NonZeroU32) -> Self {
        self.block_range_streams = BlockRangeStreamLimit::new(max_streams)
            .with_max_wait(self.block_range_streams.max_wait());
        self
    }

    /// Sets how long a block-range request past the limit waits for a slot
    /// before it fails with `UNAVAILABLE`. The default is
    /// [`crate::DEFAULT_BLOCK_RANGE_ADMISSION_WAIT`].
    #[must_use]
    pub fn with_block_range_admission_wait(mut self, max_wait: Duration) -> Self {
        self.block_range_streams = self.block_range_streams.with_max_wait(max_wait);
        self
    }

    /// Sets how old the served tip may be before `GetLatestBlock` and
    /// `GetLightdInfo` fail with `UNAVAILABLE`.
    ///
    /// The adapter defaults to [`StaleTipLimit::DISABLED`]; the
    /// `zinder-compat-lightwalletd` binary sets the configured limit.
    #[must_use]
    pub const fn with_stale_tip_limit(mut self, stale_tip_limit: StaleTipLimit) -> Self {
        self.stale_tip_limit = stale_tip_limit;
        self
    }

    /// Sets the witness whose report of the writer's node decides whether an
    /// old served tip is the chain tip or a stale one.
    ///
    /// Without one, no tip is vouched for, so the stale-tip limit refuses
    /// every tip older than the limit.
    #[must_use]
    pub fn with_node_tip_witness(mut self, node_tip_witness: NodeTipWitness) -> Self {
        self.node_tip_witness = node_tip_witness;
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

    /// Wraps this adapter in the generated tonic server type, behind the
    /// pre-decode bounds of [`COMPACT_TX_STREAMER_FIELD_LIMITS`].
    #[must_use]
    pub fn into_server(
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

impl<Broadcaster> LightwalletdGrpcAdapter<WalletServingQuery<Broadcaster>>
where
    Broadcaster: zinder_source::TransactionBroadcaster + Clone,
{
    /// Creates a compatibility adapter from an admitted canonical and wallet
    /// serving pair with retained transaction bytes.
    ///
    /// The resulting `taddrSupport` claim is fixed for this process. Later
    /// serving-pair lag or node outage is represented by the runtime readiness
    /// gate and never changes `GetLightdInfo`.
    pub fn from_admitted_compatibility_query(
        query_api: WalletServingQuery<Broadcaster>,
    ) -> Result<Self, LightwalletdAdmissionError> {
        let raw_blob_retention = query_api.admitted_raw_blob_retention();
        if !raw_blob_retention.retains_transaction_blobs() {
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

impl<QueryApi> LightwalletdGrpcAdapter<QueryApi>
where
    QueryApi: WalletQueryApi + Send + Sync + 'static,
{
    /// Resolves the visible chain epoch id at call time.
    ///
    /// Every lightwalletd handler pins all of its downstream canonical reads
    /// to one epoch so a single logical response cannot straddle a reorg.
    /// This resolves that epoch from the visible tip once at handler entry.
    async fn entry_chain_view(&self) -> Result<EntryChainView, Status> {
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        Ok(EntryChainView {
            epoch_id: visible_tip_block.chain_epoch.id,
            visible_tip_height: visible_tip_block.height,
        })
    }

    /// Reads the visible tip and its times for the tip call `method`, and
    /// refuses it when the tip is older than the stale-tip limit.
    async fn served_tip(&self, method: &'static str) -> Result<ServedTip, Status> {
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let times = self
            .tip_block_times
            .block_time(
                visible_tip_block.chain_epoch.id,
                visible_tip_block.height,
                || async {
                    self.query_api
                        .block_header_by_selector(
                            BlockSelector::Height(visible_tip_block.height),
                            Some(visible_tip_block.chain_epoch.id),
                        )
                        .await
                        .map(|tip_header| tip_header.block_header.block_time)
                        .map_err(|error| status_from_query_error(&error))
                },
            )
            .await?;
        let served_tip = ServedTip {
            block: visible_tip_block,
            times,
            now_unix_seconds: unix_now_seconds(),
        };
        self.stale_tip_limit.check(
            method,
            served_tip.times.aged_from_unix_seconds,
            served_tip.now_unix_seconds,
            self.node_tip_witness.vouches_for(served_tip.block.height),
        )?;
        Ok(served_tip)
    }

    async fn entry_chain_epoch_id(&self) -> Result<ChainEpochId, Status> {
        Ok(self.entry_chain_view().await?.epoch_id)
    }

    /// Resolves a lightwalletd `BlockId` to a typed [`BlockHeight`].
    ///
    /// `BlockId { height, hash }` accepts three legacy shapes: height-only
    /// (`hash` empty), hash-only (`height = 0` with `hash` populated), and
    /// height-with-hash (both populated; the hash is verified after the read).
    /// Hash-only resolves through the canonical best-chain
    /// [`BlockSelector`] resolver, pinned to `at_epoch_id`; height-zero with
    /// empty hash returns `Status::invalid_argument`, matching reference
    /// lightwalletd's "unspecified identifier" behavior for unary block
    /// lookups.
    async fn resolve_block_height(
        &self,
        block_id: &lightwalletd::BlockId,
        at_epoch_id: ChainEpochId,
    ) -> Result<BlockHeight, Status> {
        let height_zero = block_id.height == 0;
        if height_zero && block_id.hash.is_empty() {
            return Err(Status::invalid_argument("block identifier is unspecified"));
        }
        if !block_id.hash.is_empty() && height_zero {
            let hash_bytes: [u8; 32] = block_id
                .hash
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("block hash must be 32 bytes"))?;
            let resolved = self
                .query_api
                .block_id_by_selector(
                    BlockSelector::Hash(BlockHash::from_bytes(hash_bytes)),
                    Some(at_epoch_id),
                )
                .await
                .map_err(|error| status_from_query_error(&error))?;
            return Ok(resolved.block_id.height);
        }
        block_height_from_id(block_id)
    }

    /// Reads one compact block for `block_id`, pinning the height resolution
    /// and the block read to a single chain epoch.
    async fn block_at_epoch(
        &self,
        block_id: &lightwalletd::BlockId,
        chain_view: EntryChainView,
    ) -> Result<lightwalletd::CompactBlock, Status> {
        let height = self
            .resolve_block_height(block_id, chain_view.epoch_id)
            .await?;
        reject_future_block_height(height, chain_view.visible_tip_height)?;
        let compact_block = self
            .query_api
            .compact_block_at(height, Some(chain_view.epoch_id))
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let compact_block = compact_block_to_lightwalletd(&compact_block.compact_block)?;

        if !block_id.hash.is_empty() && block_id.hash != compact_block.hash {
            return Err(Status::not_found(
                "requested block hash does not match indexed block",
            ));
        }

        Ok(compact_block)
    }

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
        let first_page_size = address_utxo_first_page_size(distinct_addresses.len(), max_entries);
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
            return Err(Status::invalid_argument("addresses list must not be empty"));
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
}

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
impl<QueryApi> compact_tx_streamer_server::CompactTxStreamer for LightwalletdGrpcAdapter<QueryApi>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    async fn get_latest_block(
        &self,
        _request: Request<lightwalletd::ChainSpec>,
    ) -> Result<Response<lightwalletd::BlockId>, Status> {
        let served_tip = self.served_tip("get_latest_block").await?;
        Ok(Response::new(lightwalletd::BlockId {
            height: u64::from(served_tip.block.height.value()),
            hash: encode_internal_block_hash(served_tip.block.block_hash).to_vec(),
        }))
    }

    async fn get_block(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        let chain_view = self.entry_chain_view().await?;
        let block_id = request.into_inner();
        Ok(Response::new(
            self.block_at_epoch(&block_id, chain_view).await?,
        ))
    }

    async fn get_block_nullifiers(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::CompactBlock>, Status> {
        let chain_view = self.entry_chain_view().await?;
        let block_id = request.into_inner();
        let compact_block = self.block_at_epoch(&block_id, chain_view).await?;
        Ok(Response::new(prune_compact_block(
            compact_block,
            CompactBlockPoolSelection::shielded(),
            CompactBlockPayloadMode::NullifiersOnly,
        )))
    }

    type GetBlockRangeStream = GrpcStream<lightwalletd::CompactBlock>;

    async fn get_block_range(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeStream>, Status> {
        let block_range_request = request.into_inner();
        let pool_selection = pool_selection_from_request(&block_range_request.pool_types)?;
        let (block_range, is_descending) = block_range_from_request(&block_range_request)?;
        let stream_slot = self.block_range_streams.open().await?;
        let chain_view = self.entry_chain_view().await?;
        reject_future_block_height(block_range.end, chain_view.visible_tip_height)?;
        Ok(Response::new(
            stream_compact_block_range(
                self.query_api.clone(),
                block_range,
                stream_slot,
                CompactBlockWindow {
                    entry_epoch_id: chain_view.epoch_id,
                    is_descending,
                    pool_selection,
                    payload_mode: CompactBlockPayloadMode::Full,
                },
            )
            .await?,
        ))
    }

    type GetBlockRangeNullifiersStream = GrpcStream<lightwalletd::CompactBlock>;

    async fn get_block_range_nullifiers(
        &self,
        request: Request<lightwalletd::BlockRange>,
    ) -> Result<Response<Self::GetBlockRangeNullifiersStream>, Status> {
        let block_range_request = request.into_inner();
        let (block_range, is_descending) = block_range_from_request(&block_range_request)?;
        let stream_slot = self.block_range_streams.open().await?;
        let chain_view = self.entry_chain_view().await?;
        reject_future_block_height(block_range.end, chain_view.visible_tip_height)?;
        Ok(Response::new(
            stream_compact_block_range(
                self.query_api.clone(),
                block_range,
                stream_slot,
                CompactBlockWindow {
                    entry_epoch_id: chain_view.epoch_id,
                    is_descending,
                    pool_selection: CompactBlockPoolSelection::shielded(),
                    payload_mode: CompactBlockPayloadMode::NullifiersOnly,
                },
            )
            .await?,
        ))
    }

    async fn get_transaction(
        &self,
        request: Request<lightwalletd::TxFilter>,
    ) -> Result<Response<lightwalletd::RawTransaction>, Status> {
        let at_epoch_id = self.entry_chain_epoch_id().await?;
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
            Err(error) => return Err(status_from_query_error(&error)),
        };

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
        Ok(Response::new(spawn_response_stream(
            history,
            TRANSPARENT_HISTORY_STREAM_CHANNEL_CAPACITY,
        )))
    }

    type GetTaddressTransactionsStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_taddress_transactions(
        &self,
        request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        let history = self.transparent_history(&request.into_inner()).await?;
        Ok(Response::new(spawn_response_stream(
            history,
            TRANSPARENT_HISTORY_STREAM_CHANNEL_CAPACITY,
        )))
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
        let request = request.into_inner();
        validate_excluded_txid_suffixes(&request.exclude_txid_suffixes)?;
        let pool_selection = pool_selection_from_request(&request.pool_types)?;
        let exclude_txid_suffixes = request.exclude_txid_suffixes;
        let mempool_surface = self
            .mempool_surface
            .as_ref()
            .ok_or_else(|| Status::unavailable("mempool surface is not configured"))?
            .clone();
        let first_page = mempool_surface
            .mempool_snapshot_page(LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE, None)
            .await
            .map_err(status_from_mempool_surface_error)?;
        let mut entries = mempool_snapshot_entries(mempool_surface, first_page);
        let mut exclude_suffix_match_counts = vec![0_usize; exclude_txid_suffixes.len()];
        let mut compact_messages = Vec::new();
        while let Some(entry_outcome) = entries.next().await {
            let entry = entry_outcome?;
            let transaction_id = encode_internal_transaction_id(entry.transaction_id());
            let compact_message = compact_transaction_data_to_lightwalletd(
                0,
                entry.transaction_id(),
                entry.compact_transaction_data(),
            )?;
            let pruned = prune_mempool_compact_transaction(
                compact_message,
                pool_selection,
                CompactBlockPayloadMode::Full,
            );
            if !compact_transaction_has_payload(&pruned) {
                continue;
            }
            for (exclude_suffix, match_count) in exclude_txid_suffixes
                .iter()
                .zip(&mut exclude_suffix_match_counts)
            {
                if !exclude_suffix.is_empty() && transaction_id.ends_with(exclude_suffix) {
                    *match_count = match_count.saturating_add(1);
                }
            }
            compact_messages.push((transaction_id, pruned));
        }
        let filtered_messages =
            compact_messages
                .into_iter()
                .filter_map(move |(transaction_id, compact_message)| {
                    (!transaction_id_is_uniquely_excluded(
                        &transaction_id,
                        &exclude_txid_suffixes,
                        &exclude_suffix_match_counts,
                    ))
                    .then_some(Ok(compact_message))
                });
        Ok(Response::new(Box::pin(stream::iter(filtered_messages))))
    }

    type GetMempoolStreamStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_mempool_stream(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<Self::GetMempoolStreamStream>, Status> {
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

    async fn get_tree_state(
        &self,
        request: Request<lightwalletd::BlockId>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        let chain_view = self.entry_chain_view().await?;
        let block_id = request.into_inner();
        let height = self
            .resolve_block_height(&block_id, chain_view.epoch_id)
            .await?;
        reject_future_tree_state_height(height, chain_view.visible_tip_height)?;
        let tree_state = self
            .query_api
            .tree_state_at(height, Some(chain_view.epoch_id))
            .await
            .map_err(|error| tree_state_status(&error))?;

        Ok(Response::new(lightwalletd_tree_state(&tree_state)))
    }

    async fn get_latest_tree_state(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::TreeState>, Status> {
        let tree_state = self
            .query_api
            .latest_tree_state_checkpoint(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;

        Ok(Response::new(lightwalletd_tree_state(&tree_state)))
    }

    type GetSubtreeRootsStream = GrpcStream<lightwalletd::SubtreeRoot>;

    async fn get_subtree_roots(
        &self,
        request: Request<lightwalletd::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        let at_epoch_id = self.entry_chain_epoch_id().await?;
        let request = request.into_inner();
        let protocol = shielded_protocol_from_request(request.shielded_protocol)?;
        let source = SubtreeRootsStream::start(
            self.query_api.clone(),
            protocol,
            &request,
            self.response_limits.max_subtree_roots,
            at_epoch_id,
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
        let served_tip = self.served_tip("get_lightd_info").await?;
        let visible_tip_block = served_tip.block;
        // Read after the tip: a later serving pair can only carry the same or
        // a newer admitted schedule, and revisions never change activations
        // at or below the tip they were admitted at.
        let activations = self
            .query_api
            .network_upgrade_activations()
            .await
            .map_err(|error| status_from_query_error(&error))?;
        if visible_tip_block.chain_epoch.network != activations.network() {
            return Err(Status::failed_precondition(
                "network upgrade activations do not match the chain epoch network",
            ));
        }

        let estimated_height = estimated_network_height(
            &activations,
            visible_tip_block.height,
            served_tip.times.block_time_unix_seconds,
            served_tip.now_unix_seconds,
        );
        Ok(Response::new(lightd_info(
            &activations,
            visible_tip_block.height,
            estimated_height,
            self.transparent_address_support,
        )))
    }

    async fn get_standard_fee(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::StandardFee>, Status> {
        match self.query_api.standard_fee().await {
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

/// One transparent address's mined transactions at one chain epoch, read a
/// page of history entries at a time and one transaction at a time.
struct TransparentHistory<QueryApi> {
    query_api: QueryApi,
    request: TransparentAddressTxIdsInRangeRequest,
    at_epoch_id: ChainEpochId,
    page: std::vec::IntoIter<TransparentAddressTxIndexArtifact>,
    has_later_pages: bool,
}

impl<QueryApi> TransparentHistory<QueryApi>
where
    QueryApi: WalletQueryApi + Send + Sync + 'static,
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
            page: Vec::new().into_iter(),
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
        self.page = response.artifacts.into_iter();
        self.has_later_pages = response.next_cursor.is_some();
        self.request.from_cursor = response.next_cursor;
        Ok(())
    }

    async fn read_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<lightwalletd::RawTransaction, Status> {
        let response = self
            .query_api
            .transaction(transaction_id, Some(self.at_epoch_id))
            .await
            .map_err(|error| status_from_query_error(&error))?;
        raw_transaction_from_mined(mined_transaction_from_status(response.status)?)
    }
}

impl<QueryApi> ResponseSource for TransparentHistory<QueryApi>
where
    QueryApi: WalletQueryApi + Send + Sync + 'static,
{
    type Message = lightwalletd::RawTransaction;

    async fn next(&mut self) -> Option<Result<Self::Message, Status>> {
        loop {
            if let Some(artifact) = self.page.next() {
                return Some(self.read_transaction(artifact.transaction_id).await);
            }
            if !self.has_later_pages {
                return None;
            }
            if let Err(status) = self.read_page().await {
                self.has_later_pages = false;
                return Some(Err(status));
            }
        }
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
    at_epoch_id: ChainEpochId,
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
    QueryApi: WalletQueryApi + Send + Sync + 'static,
{
    async fn start(
        query_api: QueryApi,
        protocol: ShieldedProtocol,
        request: &lightwalletd::GetSubtreeRootsArg,
        page_limit: NonZeroU32,
        at_epoch_id: ChainEpochId,
    ) -> Result<Self, Status> {
        let mut stream = Self {
            query_api,
            protocol,
            at_epoch_id,
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
            .subtree_roots(
                SubtreeRootRange::new(
                    self.protocol,
                    SubtreeRootIndex::new(start_index),
                    page_entries,
                ),
                Some(self.at_epoch_id),
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
    QueryApi: WalletQueryApi + Send + Sync + 'static,
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
/// `distinct_addresses` distinct addresses and returns at most `max_entries`
/// rows.
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

/// The unspent outputs of several addresses, merged into the order of one
/// lightwalletd UTXO response: height, txid, output index, then address.
///
/// Each address's stream is already in that order for its own outputs, so
/// the merge holds only the next output of each address. The streams read
/// on demand at the one chain epoch the request resolved, starting at the
/// first page [`address_utxo_first_page_size`] chooses. A later page holds at
/// most the outputs the merge has taken from that address plus one first
/// page, and every output the merge takes is sent as one row. So the outputs
/// read stay within about twice the rows sent, plus 2 outputs and one first
/// page for each distinct address, and the first pages together hold at most
/// 8,192 outputs, or 16 for each distinct address past 512.
struct AddressUtxoMerge {
    addresses: Vec<MergedAddress>,
    next_outputs: BinaryHeap<Reverse<NextAddressUtxo>>,
    /// The address whose last row was taken and whose next output is read
    /// only when the response needs another row, so a response never reads
    /// past the last row it sends.
    awaiting_next_output: Option<usize>,
    max_entries: Option<usize>,
    response_limit: usize,
    sent: usize,
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
}

impl NextAddressUtxo {
    fn order_key(&self) -> (u64, &[u8], i32, &str, usize) {
        (
            self.reply.height,
            self.reply.txid.as_slice(),
            self.reply.index,
            self.reply.address.as_str(),
            self.address_position,
        )
    }
}

impl PartialEq for NextAddressUtxo {
    fn eq(&self, other: &Self) -> bool {
        self.order_key() == other.order_key()
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
        self.order_key().cmp(&other.order_key())
    }
}

impl AddressUtxoMerge {
    const fn new(max_entries: Option<usize>, response_limit: usize) -> Self {
        Self {
            addresses: Vec::new(),
            next_outputs: BinaryHeap::new(),
            awaiting_next_output: None,
            max_entries,
            response_limit,
            sent: 0,
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
        match merged.outputs.next().await {
            Some(Ok(utxo)) => {
                let reply = lightwalletd_address_utxo(&merged.address, &utxo)?;
                self.next_outputs.push(Reverse(NextAddressUtxo {
                    reply,
                    address_position,
                }));
                Ok(())
            }
            Some(Err(error)) => Err(status_from_query_error(&error)),
            None => Ok(()),
        }
    }

    /// Reads the next output of the address whose last row was taken.
    async fn read_awaited_output(&mut self) -> Result<(), Status> {
        match self.awaiting_next_output.take() {
            Some(address_position) => self.read_next_output(address_position).await,
            None => Ok(()),
        }
    }

    /// Takes the lowest next output as the next row to send.
    fn take_next_row(&mut self) -> Option<lightwalletd::GetAddressUtxosReply> {
        let Reverse(next_output) = self.next_outputs.pop()?;
        self.awaiting_next_output = Some(next_output.address_position);
        Some(next_output.reply)
    }
}

impl ResponseSource for AddressUtxoMerge {
    type Message = lightwalletd::GetAddressUtxosReply;

    /// Sends at most `maxEntries` rows. Without a `maxEntries` within the
    /// response limit, a row past the limit fails the response with
    /// `RESOURCE_EXHAUSTED` instead of ending it as if it were complete.
    async fn next(&mut self) -> Option<Result<Self::Message, Status>> {
        if self.ended
            || self
                .max_entries
                .is_some_and(|max_entries| self.sent >= max_entries)
        {
            return None;
        }
        if let Err(status) = self.read_awaited_output().await {
            self.ended = true;
            return Some(Err(status));
        }
        if self.next_outputs.is_empty() {
            return None;
        }
        if self.sent >= self.response_limit {
            self.ended = true;
            return Some(Err(address_utxo_limit_exceeded(self.response_limit)));
        }
        let row = self.take_next_row()?;
        self.sent += 1;
        Some(Ok(row))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "Each bool is an independent per-pool inclusion gate parsed from a request's poolTypes, not a state machine."
)]
struct CompactBlockPoolSelection {
    sapling: bool,
    orchard: bool,
    ironwood: bool,
    transparent: bool,
}

impl CompactBlockPoolSelection {
    const fn shielded() -> Self {
        Self {
            sapling: true,
            orchard: true,
            ironwood: true,
            transparent: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactBlockPayloadMode {
    Full,
    NullifiersOnly,
}

/// How one `BlockRange` handler reads and shapes its window.
#[derive(Clone, Copy)]
struct CompactBlockWindow {
    /// Chain epoch the request's bounds were checked against. Only the first
    /// chunk is pinned to it; each later chunk reads the view current when
    /// it is read.
    entry_epoch_id: ChainEpochId,
    /// Whether the client asked for heights high to low.
    is_descending: bool,
    /// Pools the emitted blocks retain.
    pool_selection: CompactBlockPoolSelection,
    /// Payload depth the emitted blocks retain.
    payload_mode: CompactBlockPayloadMode,
}

/// Streams a compact-block window the client asked for in one call.
///
/// lightwalletd puts no width bound on `BlockRange`, while each store read is
/// capped at 1,000 blocks. [`BlockRangeChunks`] splits the window into chunks
/// read in wire order, and each read keeps at most
/// [`BLOCK_RANGE_CHUNK_MAX_BYTES`] of blocks, so an arbitrarily wide request
/// of blocks of any size streams while at most one bounded chunk is
/// materialized at a time.
///
/// The first chunk is read at the entry epoch before the response, so an
/// unreadable window fails the call rather than a stream the client has
/// already opened. Later chunks follow [`BlockRangeContinuity`]. The read of
/// the first chunk, the response, and the reader of later chunks each hold
/// `stream_slot` until they finish, even when the client cancels the call.
async fn stream_compact_block_range<QueryApi>(
    query_api: QueryApi,
    block_range: BlockHeightRange,
    stream_slot: BlockRangeStreamSlot,
    window: CompactBlockWindow,
) -> Result<GrpcStream<lightwalletd::CompactBlock>, Status>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    let mut chunks = BlockRangeChunks::new(block_range, window.is_descending);
    let Some(first_chunk) = chunks.next_chunk() else {
        return Ok(Box::pin(stream::empty()));
    };
    let first_range = stream_slot
        .hold_during({
            let query_api = query_api.clone();
            async move {
                read_compact_block_chunk(
                    &query_api,
                    first_chunk,
                    &chunks,
                    Some(window.entry_epoch_id),
                )
                .await
            }
        })
        .await?;
    chunks.advance(first_range.compact_blocks.len());
    if !chunks.has_remaining() {
        let first_messages = encode_compact_block_chunk(first_range.compact_blocks, window);
        return Ok(stream_slot.hold_for(first_messages));
    }
    let continuity = BlockRangeContinuity::after_first_chunk(
        &first_range.compact_blocks,
        first_range.chain_epoch.id,
        window.is_descending,
    )?;
    let first_messages = encode_compact_block_chunk(first_range.compact_blocks, window);
    Ok(stream_slot.hold_with_reader(
        first_messages,
        COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY,
        |sink| drive_compact_block_range(query_api, chunks, window, continuity, sink),
    ))
}

/// Reads every chunk after the first and forwards its blocks to the sink.
///
/// A read failure, a reorg ending, or an unencodable block ends the stream
/// with that status; a client that stops draining ends the walk on the next
/// send.
async fn drive_compact_block_range<QueryApi>(
    query_api: QueryApi,
    mut chunks: BlockRangeChunks,
    window: CompactBlockWindow,
    mut continuity: BlockRangeContinuity,
    sink: BlockRangeSink<Result<lightwalletd::CompactBlock, Status>>,
) where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    while let Some(chunk) = chunks.next_chunk() {
        let compact_blocks =
            match read_continuation_chunk(&query_api, chunk, &chunks, &mut continuity).await {
                Ok(compact_blocks) => compact_blocks,
                Err(status) => {
                    let _ = sink.send(Err(status)).await;
                    return;
                }
            };
        chunks.advance(compact_blocks.len());
        let messages = encode_compact_block_chunk(compact_blocks, window);
        let sent_chunk = BlockRangeStage::Send
            .time_async(async {
                for message in messages {
                    let is_terminal = message.is_err();
                    if sink.send(message).await.is_err() || is_terminal {
                        return false;
                    }
                }
                true
            })
            .await;
        if !sent_chunk {
            return;
        }
    }
}

/// Reads a chunk after the first at the view current when it is read.
///
/// The read is unpinned: the hash link that `continuity` checks, not a pin,
/// proves the chunk continues the chain already sent. The tip check before
/// the read only tells a reorg that lowered the tip apart from a read of an
/// unretained height.
async fn read_continuation_chunk<QueryApi>(
    query_api: &QueryApi,
    chunk: BlockHeightRange,
    chunks: &BlockRangeChunks,
    continuity: &mut BlockRangeContinuity,
) -> Result<Vec<CompactBlockArtifact>, Status>
where
    QueryApi: WalletQueryApi + Send + Sync,
{
    let visible_tip_block = query_api
        .visible_tip_block(None)
        .await
        .map_err(|error| status_from_query_error(&error))?;
    continuity.check_view_reaches(chunk, &visible_tip_block.chain_epoch)?;
    let compact_block_range = read_compact_block_chunk(query_api, chunk, chunks, None).await?;
    continuity.advance(
        chunk,
        &compact_block_range.chain_epoch,
        &compact_block_range.compact_blocks,
    )?;
    Ok(compact_block_range.compact_blocks)
}

/// Reads the blocks of `chunk` that fit one chunk's byte budget, from the
/// end the stream sends first.
async fn read_compact_block_chunk<QueryApi>(
    query_api: &QueryApi,
    chunk: BlockHeightRange,
    chunks: &BlockRangeChunks,
    at_epoch_id: Option<ChainEpochId>,
) -> Result<CompactBlockRange, Status>
where
    QueryApi: WalletQueryApi + Send + Sync,
{
    BlockRangeStage::Read
        .time_async(query_api.compact_blocks_in_range_up_to(
            chunk,
            chunks.order(),
            BLOCK_RANGE_CHUNK_MAX_BYTES,
            at_epoch_id,
        ))
        .await
        .map_err(|error| status_from_query_error(&error))
}

fn encode_compact_block_chunk(
    mut compact_blocks: Vec<CompactBlockArtifact>,
    window: CompactBlockWindow,
) -> Vec<Result<lightwalletd::CompactBlock, Status>> {
    BlockRangeStage::Convert.time(|| {
        if window.is_descending {
            compact_blocks.reverse();
        }
        compact_blocks
            .iter()
            .map(|compact_block| {
                compact_block_to_lightwalletd(compact_block).map(|compact_block| {
                    prune_compact_block(compact_block, window.pool_selection, window.payload_mode)
                })
            })
            .collect()
    })
}

fn compact_block_to_lightwalletd(
    compact_block: &CompactBlockArtifact,
) -> Result<lightwalletd::CompactBlock, Status> {
    let transactions = compact_block
        .transactions()
        .iter()
        .map(compact_transaction_to_lightwalletd)
        .collect::<Result<Vec<_>, _>>()?;
    let metadata = compact_block.chain_metadata();
    Ok(lightwalletd::CompactBlock {
        height: u64::from(compact_block.height().value()),
        hash: encode_internal_block_hash(compact_block.block_hash()).to_vec(),
        prev_hash: encode_internal_block_hash(compact_block.previous_block_hash()).to_vec(),
        time: compact_block.time(),
        header: Vec::new(),
        vtx: transactions,
        chain_metadata: Some(lightwalletd::ChainMetadata {
            sapling_commitment_tree_size: metadata.sapling_commitment_tree_size,
            orchard_commitment_tree_size: metadata.orchard_commitment_tree_size,
            ironwood_commitment_tree_size: metadata.ironwood_commitment_tree_size,
        }),
    })
}

fn compact_transaction_to_lightwalletd(
    transaction: &CompactTransaction,
) -> Result<lightwalletd::CompactTx, Status> {
    compact_transaction_data_to_lightwalletd(
        transaction.index,
        transaction.transaction_id,
        &transaction.data,
    )
}

fn compact_transaction_data_to_lightwalletd(
    index: u64,
    transaction_id: zinder_core::TransactionId,
    transaction_data: &CompactTransactionData,
) -> Result<lightwalletd::CompactTx, Status> {
    let fee = transaction_data
        .fee_zat
        .map(u32::try_from)
        .transpose()
        .map_err(|_| Status::data_loss("compact transaction fee exceeds u32"))?
        .unwrap_or_default();
    Ok(lightwalletd::CompactTx {
        index,
        txid: encode_internal_transaction_id(transaction_id).to_vec(),
        fee,
        spends: transaction_data
            .sapling_spends
            .iter()
            .map(|spend| lightwalletd::CompactSaplingSpend {
                nf: spend.nullifier.to_vec(),
            })
            .collect(),
        outputs: transaction_data
            .sapling_outputs
            .iter()
            .map(|output| lightwalletd::CompactSaplingOutput {
                cmu: output.commitment.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect(),
        actions: transaction_data
            .orchard_actions
            .iter()
            .map(compact_shielded_action_to_lightwalletd)
            .collect(),
        vin: transaction_data
            .transparent_inputs
            .iter()
            .map(|input| lightwalletd::CompactTxIn {
                prevout_txid: encode_internal_transaction_id(input.previous_transaction_id)
                    .to_vec(),
                prevout_index: input.previous_output_index,
            })
            .collect(),
        vout: transaction_data
            .transparent_outputs
            .iter()
            .map(|output| lightwalletd::TxOut {
                value: output.value_zat,
                script_pub_key: output.script_pub_key.clone(),
            })
            .collect(),
        ironwood_actions: transaction_data
            .ironwood_actions
            .iter()
            .map(compact_shielded_action_to_lightwalletd)
            .collect(),
    })
}

fn compact_shielded_action_to_lightwalletd(
    action: &zinder_core::CompactShieldedAction,
) -> lightwalletd::CompactOrchardAction {
    lightwalletd::CompactOrchardAction {
        nullifier: action.nullifier.to_vec(),
        cmx: action.commitment.to_vec(),
        ephemeral_key: action.ephemeral_key.to_vec(),
        ciphertext: action.ciphertext.to_vec(),
    }
}

fn prune_compact_transaction(
    mut transaction: lightwalletd::CompactTx,
    pool_selection: CompactBlockPoolSelection,
    payload_mode: CompactBlockPayloadMode,
) -> lightwalletd::CompactTx {
    if !pool_selection.sapling || payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        transaction.outputs.clear();
    }
    if !pool_selection.sapling {
        transaction.spends.clear();
    }
    if !pool_selection.orchard {
        transaction.actions.clear();
    } else if payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        for action in &mut transaction.actions {
            action.cmx.clear();
            action.ephemeral_key.clear();
            action.ciphertext.clear();
        }
    }
    if !pool_selection.ironwood {
        transaction.ironwood_actions.clear();
    } else if payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        for action in &mut transaction.ironwood_actions {
            action.cmx.clear();
            action.ephemeral_key.clear();
            action.ciphertext.clear();
        }
    }
    if !pool_selection.transparent || payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        transaction.vin.clear();
        transaction.vout.clear();
    }
    transaction
}

fn prune_mempool_compact_transaction(
    transaction: lightwalletd::CompactTx,
    pool_selection: CompactBlockPoolSelection,
    payload_mode: CompactBlockPayloadMode,
) -> lightwalletd::CompactTx {
    let mut transaction = prune_compact_transaction(transaction, pool_selection, payload_mode);
    // Reference lightwalletd exposes transparent outputs for pending mempool
    // transactions but omits transparent inputs from GetMempoolTx.
    transaction.vin.clear();
    transaction
}

fn prune_compact_block(
    mut compact_block: lightwalletd::CompactBlock,
    pool_selection: CompactBlockPoolSelection,
    payload_mode: CompactBlockPayloadMode,
) -> lightwalletd::CompactBlock {
    // The nullifiers-only contract excludes commitment tree sizes: the
    // GetBlockNullifiers / GetBlockRangeNullifiers responses must not leak the
    // witness-construction tree sizes carried in chain_metadata.
    if payload_mode == CompactBlockPayloadMode::NullifiersOnly {
        compact_block.chain_metadata = None;
    }
    compact_block.vtx = compact_block
        .vtx
        .into_iter()
        .map(|transaction| prune_compact_transaction(transaction, pool_selection, payload_mode))
        .filter(compact_transaction_has_payload)
        .collect();
    compact_block
}

/// Returns whether a pruned compact transaction still carries wallet-relevant
/// data.
///
/// Both serving planes drop transactions that this rejects, as lightwalletd
/// does, and keep the original `CompactTx.index` of the transactions that
/// remain.
#[must_use]
pub fn compact_transaction_has_payload(transaction: &lightwalletd::CompactTx) -> bool {
    !transaction.spends.is_empty()
        || !transaction.outputs.is_empty()
        || !transaction.actions.is_empty()
        || !transaction.ironwood_actions.is_empty()
        || !transaction.vin.is_empty()
        || !transaction.vout.is_empty()
}

fn pool_selection_from_request(pool_types: &[i32]) -> Result<CompactBlockPoolSelection, Status> {
    if pool_types.is_empty() {
        return Ok(CompactBlockPoolSelection::shielded());
    }

    let mut pool_selection = CompactBlockPoolSelection {
        sapling: false,
        orchard: false,
        ironwood: false,
        transparent: false,
    };
    for pool_type in pool_types {
        match lightwalletd::PoolType::try_from(*pool_type) {
            Ok(lightwalletd::PoolType::Sapling) => pool_selection.sapling = true,
            Ok(lightwalletd::PoolType::Orchard) => pool_selection.orchard = true,
            Ok(lightwalletd::PoolType::Ironwood) => pool_selection.ironwood = true,
            Ok(lightwalletd::PoolType::Transparent) => pool_selection.transparent = true,
            Ok(lightwalletd::PoolType::Invalid) | Err(_) => {
                return Err(Status::invalid_argument("invalid pool type requested"));
            }
        }
    }

    Ok(pool_selection)
}

fn block_range_from_request(
    request: &lightwalletd::BlockRange,
) -> Result<(BlockHeightRange, bool), Status> {
    let start = request
        .start
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("range.start is required"))
        .and_then(block_range_height_from_id)?;
    let end = request
        .end
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("range.end is required"))
        .and_then(block_range_height_from_id)?;
    let is_descending = start > end;
    let block_range = if is_descending {
        BlockHeightRange::inclusive(end, start)
    } else {
        BlockHeightRange::inclusive(start, end)
    };

    Ok((block_range, is_descending))
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

fn block_range_height_from_id(block_id: &lightwalletd::BlockId) -> Result<BlockHeight, Status> {
    let height = u32::try_from(block_id.height)
        .map_err(|_| Status::invalid_argument("block height exceeds u32"))?;
    Ok(BlockHeight::new(height))
}

fn block_height_from_id(block_id: &lightwalletd::BlockId) -> Result<BlockHeight, Status> {
    let height = u32::try_from(block_id.height)
        .map_err(|_| Status::invalid_argument("block height exceeds u32"))?;
    if height == 0 {
        if !block_id.hash.is_empty() {
            return Err(Status::invalid_argument(
                "hash-only block lookups must be resolved through resolve_block_height",
            ));
        }
        return Err(Status::not_found("block height 0 is not indexed"));
    }

    Ok(BlockHeight::new(height))
}

fn reject_future_block_height(
    requested_height: BlockHeight,
    visible_tip_height: BlockHeight,
) -> Result<(), Status> {
    if requested_height > visible_tip_height {
        return Err(Status::out_of_range(format!(
            "block {} is newer than latest block {}",
            requested_height.value(),
            visible_tip_height.value(),
        )));
    }
    Ok(())
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
        return Err(Status::invalid_argument(
            "transparent address does not produce a receivable script",
        ));
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
        return Err(Status::invalid_argument(
            "transparent address does not produce a receivable script",
        ));
    }

    Ok(TransparentAddressUnspentOutputsRequest {
        address_script_hash: TransparentAddressScriptHash::of_script_pub_key(&script_pub_key),
        start_height,
    })
}

fn lightwalletd_address_utxo(
    address: &str,
    utxo: &TransparentUnspentOutput,
) -> Result<lightwalletd::GetAddressUtxosReply, Status> {
    Ok(lightwalletd::GetAddressUtxosReply {
        address: address.to_owned(),
        txid: encode_internal_transaction_id(utxo.outpoint.transaction_id).to_vec(),
        index: i32::try_from(utxo.outpoint.output_index)
            .map_err(|_| Status::data_loss("transparent output index exceeds i32"))?,
        script: utxo.script_pub_key.clone(),
        value_zat: i64::try_from(utxo.value_zat)
            .map_err(|_| Status::data_loss("transparent output value exceeds i64"))?,
        height: u64::from(utxo.block_height.value()),
    })
}

fn address_utxo_limit_exceeded(response_limit: usize) -> Status {
    Status::resource_exhausted(format!(
        "more unspent outputs match than the {response_limit} one response returns"
    ))
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

/// Builds `LightdInfo` for a served tip at `tip_height` while the network is
/// estimated to be at `estimated_height`.
fn lightd_info(
    activations: &NetworkUpgradeActivations,
    tip_height: BlockHeight,
    estimated_height: BlockHeight,
    transparent_address_support: bool,
) -> lightwalletd::LightdInfo {
    let consensus_branch_id = activations.active_at(tip_height).map_or_else(
        || "00000000".to_owned(),
        |activation| encode_branch_id_hex(activation.branch_id),
    );
    // The protocol defines `upgradeName` and `upgradeHeight` as the next
    // pending upgrade, not the active one.
    let next_pending = activations
        .activations()
        .iter()
        .find(|activation| activation.activation_height > tip_height);
    let upgrade_name = next_pending.map_or_default(|activation| activation.name.clone());
    let upgrade_height = next_pending.map_or(0, |activation| {
        u64::from(activation.activation_height.value())
    });
    let sapling_activation_height = activations
        .activation_height_by_name("Sapling")
        .map_or(0, |height| u64::from(height.value()));

    lightwalletd::LightdInfo {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        vendor: "Zinder".to_owned(),
        taddr_support: transparent_address_support,
        chain_name: encode_bip70_chain_name(activations.network()).to_owned(),
        sapling_activation_height,
        consensus_branch_id,
        block_height: u64::from(tip_height.value()),
        git_commit: zinder_runtime::BUILD_GIT_COMMIT.to_owned(),
        branch: String::new(),
        build_date: String::new(),
        build_user: String::new(),
        estimated_height: u64::from(estimated_height.max(tip_height).value()),
        // Deliberately empty: Zinder is not zcashd; impersonating a build
        // version misleads operators inspecting the field.
        zcashd_build: String::new(),
        zcashd_subversion: String::new(),
        // Operator-configured in lightwalletd-go. Zinder has no donation
        // address config, so the empty string preserves the unset convention.
        donation_address: String::new(),
        upgrade_name,
        upgrade_height,
        lightwallet_protocol_version: LIGHTWALLETD_PROTOCOL_VERSION.to_owned(),
    }
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

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "non-exhaustive core networks must fail closed until lightwalletd mapping exists"
)]
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
    let (output_sender, output_receiver) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        tokio::pin!(raw_transaction_stream);
        let mut tip_change_signal =
            Box::pin(watcher.await_tip_change_after(snapshot_chain_epoch_id));
        loop {
            tokio::select! {
                outcome = tonic::codegen::tokio_stream::StreamExt::next(&mut raw_transaction_stream) => {
                    match outcome {
                        Some(raw_transaction_outcome) => {
                            if output_sender.send(raw_transaction_outcome).await.is_err() {
                                return;
                            }
                        }
                        None => {
                            // Underlying mempool source ended; let the
                            // gRPC stream end naturally.
                            return;
                        }
                    }
                }
                tip_change_outcome = &mut tip_change_signal => {
                    match tip_change_outcome {
                        Ok(()) => {
                            tracing::debug!(
                                target: "zinder::compat_lightwalletd",
                                event = "mempool_stream_close_on_tip_change",
                                "GetMempoolStream closing on observed best-chain tip change"
                            );
                        }
                        Err(error @ TipChangeWatcherError::SignalClosed) => {
                            tracing::warn!(
                                target: "zinder::compat_lightwalletd",
                                event = "mempool_stream_tip_signal_closed",
                                error = %error,
                                "tip-change signal source closed; GetMempoolStream will end"
                            );
                            // Ending with OK would tell the client a new block
                            // arrived when no tip change was observed.
                            let _ = output_sender
                                .send(Err(Status::unavailable(error.to_string())))
                                .await;
                        }
                    }
                    return;
                }
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(output_receiver)
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

/// Streams live-mempool entries page by page, starting with the
/// already-fetched first page and fetching later pages only as the consumer
/// pulls, so the walk never materializes the whole mempool.
fn mempool_snapshot_entries(
    mempool_surface: SharedMempoolSurface,
    first_page: MempoolSnapshotPage,
) -> tokio_stream::wrappers::ReceiverStream<Result<zinder_core::MempoolEntry, Status>> {
    let (entry_sender, entry_receiver) = tokio::sync::mpsc::channel(16);
    tokio::spawn(async move {
        let mut page = first_page;
        loop {
            for entry in page.entries {
                if entry_sender.send(Ok(entry)).await.is_err() {
                    return;
                }
            }
            let Some(cursor) = page.next_cursor else {
                return;
            };
            page = match mempool_surface
                .mempool_snapshot_page(LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE, Some(cursor))
                .await
            {
                Ok(next_page) => next_page,
                Err(error) => {
                    let _ = entry_sender
                        .send(Err(status_from_mempool_surface_error(error)))
                        .await;
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

fn transaction_id_is_uniquely_excluded(
    transaction_id: &[u8; 32],
    exclude_suffixes: &[Vec<u8>],
    exclude_suffix_match_counts: &[usize],
) -> bool {
    exclude_suffixes
        .iter()
        .zip(exclude_suffix_match_counts)
        .any(|(suffix, match_count)| {
            *match_count == 1 && !suffix.is_empty() && transaction_id.ends_with(suffix)
        })
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
    use zinder_core::{
        ArtifactSchemaVersion, ChainEpoch, ChainTipMetadata, CompactSaplingOutput,
        CompactSaplingSpend, CompactShieldedAction, CompactTransparentInput,
        CompactTransparentOutput, TransparentAddressBalance, UnixTimestampMillis,
    };

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

    #[test]
    fn lightd_info_reports_the_active_branch_and_the_next_pending_upgrade() {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let branch_hex = |name: &str| {
            activations
                .activations()
                .iter()
                .find(|activation| activation.name == name)
                .map(|activation| encode_branch_id_hex(activation.branch_id))
        };

        let before_nu5 = lightd_info(&activations, BlockHeight::new(1), BlockHeight::new(1), true);
        assert_eq!(Some(before_nu5.consensus_branch_id), branch_hex("Canopy"));
        assert_eq!(before_nu5.upgrade_name, "NU5");
        assert_eq!(before_nu5.upgrade_height, 2);

        let before_nu6_3 = lightd_info(
            &activations,
            BlockHeight::new(602),
            BlockHeight::new(602),
            true,
        );
        assert_eq!(Some(before_nu6_3.consensus_branch_id), branch_hex("NU6.2"));
        assert_eq!(before_nu6_3.upgrade_name, "NU6.3");
        assert_eq!(before_nu6_3.upgrade_height, 603);

        let before_nu7 = lightd_info(
            &activations,
            BlockHeight::new(603),
            BlockHeight::new(603),
            true,
        );
        assert_eq!(Some(before_nu7.consensus_branch_id), branch_hex("NU6.3"));
        assert_eq!(before_nu7.upgrade_name, "NU7");
        assert_eq!(before_nu7.upgrade_height, 703);

        let none_pending = lightd_info(
            &activations,
            BlockHeight::new(703),
            BlockHeight::new(703),
            true,
        );
        assert_eq!(Some(none_pending.consensus_branch_id), branch_hex("NU7"));
        assert_eq!(none_pending.upgrade_name, "");
        assert_eq!(none_pending.upgrade_height, 0);
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
    fn lightd_info_reports_the_shared_build_identity() {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let info = lightd_info(&activations, BlockHeight::new(1), BlockHeight::new(1), true);

        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(info.git_commit, zinder_runtime::BUILD_GIT_COMMIT);
    }

    fn compact_tx_with_ironwood() -> lightwalletd::CompactTx {
        lightwalletd::CompactTx {
            index: 0,
            txid: vec![1; 32],
            fee: 0,
            spends: Vec::new(),
            outputs: Vec::new(),
            actions: vec![lightwalletd::CompactOrchardAction {
                nullifier: vec![9; 32],
                cmx: vec![10; 32],
                ephemeral_key: vec![11; 32],
                ciphertext: vec![12; 52],
            }],
            ironwood_actions: vec![lightwalletd::CompactOrchardAction {
                nullifier: vec![20; 32],
                cmx: vec![21; 32],
                ephemeral_key: vec![22; 32],
                ciphertext: vec![23; 52],
            }],
            vin: Vec::new(),
            vout: Vec::new(),
        }
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

    #[test]
    fn nullifiers_only_keeps_the_ironwood_nullifier_and_clears_the_rest() {
        let pruned = prune_compact_transaction(
            compact_tx_with_ironwood(),
            CompactBlockPoolSelection::shielded(),
            CompactBlockPayloadMode::NullifiersOnly,
        );
        assert_eq!(pruned.ironwood_actions.len(), 1);
        let action = &pruned.ironwood_actions[0];
        assert_eq!(action.nullifier, vec![20; 32]);
        assert_eq!(action.cmx, [] as [u8; 0]);
        assert_eq!(action.ephemeral_key, [] as [u8; 0]);
        assert_eq!(action.ciphertext, [] as [u8; 0]);
    }

    #[test]
    fn deselecting_the_ironwood_pool_drops_ironwood_actions() {
        let pruned = prune_compact_transaction(
            compact_tx_with_ironwood(),
            CompactBlockPoolSelection {
                sapling: true,
                orchard: true,
                ironwood: false,
                transparent: true,
            },
            CompactBlockPayloadMode::Full,
        );
        assert_eq!(
            pruned.ironwood_actions,
            [] as [zinder_proto::compat::lightwalletd::CompactOrchardAction; 0]
        );
        assert_eq!(pruned.actions.len(), 1);
    }

    #[test]
    fn structured_scan_data_maps_every_pool_exactly() -> Result<(), Status> {
        let transaction_id = zinder_core::TransactionId::from_bytes([0x41; 32]);
        let previous_transaction_id = zinder_core::TransactionId::from_bytes([0x42; 32]);
        let data = CompactTransactionData {
            fee_zat: Some(23),
            sapling_spends: vec![CompactSaplingSpend {
                nullifier: [0x11; 32],
            }],
            sapling_outputs: vec![CompactSaplingOutput {
                commitment: [0x12; 32],
                ephemeral_key: [0x13; 32],
                ciphertext: [0x14; 52],
            }],
            orchard_actions: vec![CompactShieldedAction {
                nullifier: [0x21; 32],
                commitment: [0x22; 32],
                ephemeral_key: [0x23; 32],
                ciphertext: [0x24; 52],
            }],
            ironwood_actions: vec![CompactShieldedAction {
                nullifier: [0x31; 32],
                commitment: [0x32; 32],
                ephemeral_key: [0x33; 32],
                ciphertext: [0x34; 52],
            }],
            transparent_inputs: vec![CompactTransparentInput {
                previous_transaction_id,
                previous_output_index: 7,
            }],
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 99,
                script_pub_key: vec![0x51, 0x21],
            }],
        };

        let mapped = compact_transaction_data_to_lightwalletd(5, transaction_id, &data)?;

        assert_eq!(mapped.index, 5);
        assert_eq!(mapped.txid, encode_internal_transaction_id(transaction_id));
        assert_eq!(mapped.fee, 23);
        assert_eq!(mapped.spends[0].nf, vec![0x11; 32]);
        assert_eq!(mapped.outputs[0].cmu, vec![0x12; 32]);
        assert_eq!(mapped.outputs[0].ephemeral_key, vec![0x13; 32]);
        assert_eq!(mapped.outputs[0].ciphertext, vec![0x14; 52]);
        assert_eq!(mapped.actions[0].nullifier, vec![0x21; 32]);
        assert_eq!(mapped.actions[0].cmx, vec![0x22; 32]);
        assert_eq!(mapped.actions[0].ephemeral_key, vec![0x23; 32]);
        assert_eq!(mapped.actions[0].ciphertext, vec![0x24; 52]);
        assert_eq!(mapped.ironwood_actions[0].nullifier, vec![0x31; 32]);
        assert_eq!(mapped.ironwood_actions[0].cmx, vec![0x32; 32]);
        assert_eq!(mapped.ironwood_actions[0].ephemeral_key, vec![0x33; 32]);
        assert_eq!(mapped.ironwood_actions[0].ciphertext, vec![0x34; 52]);
        assert_eq!(
            mapped.vin[0].prevout_txid,
            encode_internal_transaction_id(previous_transaction_id)
        );
        assert_eq!(mapped.vin[0].prevout_index, 7);
        assert_eq!(mapped.vout[0].value, 99);
        assert_eq!(mapped.vout[0].script_pub_key, vec![0x51, 0x21]);
        Ok(())
    }
}
