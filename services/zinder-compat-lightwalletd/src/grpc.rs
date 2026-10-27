//! gRPC implementation for the vendored lightwalletd protocol.

use std::{num::NonZeroU32, pin::Pin};

use serde_json::Value;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;
use tokio_stream::{self as stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use zebra_chain::transparent::Address as ZebraTransparentAddress;
use zinder_core::wire::WireDecodeError;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockSelector, BroadcastAccepted, BroadcastDuplicate,
    BroadcastInvalidEncoding, BroadcastQueued, BroadcastRejected, BroadcastUnknown, ChainEpochId,
    CompactBlockArtifact, CompactTransaction, CompactTransactionData, Network,
    NetworkUpgradeActivations, RawTransactionBytes, ShieldedProtocol, SubtreeRootIndex,
    SubtreeRootRange, TransactionBroadcastOutcome, TransactionLocation,
    TransparentAddressScriptHash, TransparentAddressTxIndexArtifact, TxStatus,
    wire::{
        decode_internal_transaction_id, encode_bip70_chain_name, encode_branch_id_hex,
        encode_internal_block_hash, encode_internal_transaction_id, encode_rpc_block_hash_hex,
        encode_rpc_transaction_id_hex,
    },
};
use zinder_proto::compat::lightwalletd::{
    self, LIGHTWALLETD_PROTOCOL_COMMIT, compact_tx_streamer_server,
};
use zinder_proto::v1::wallet::{self as wallet_proto, address_lookup};
use zinder_query::{
    DEFAULT_MAX_COMPACT_BLOCK_RANGE, SubtreeRoots, TransparentAddressTxIdsInRangeRequest,
    TransparentAddressUnspentOutputs, TransparentAddressUnspentOutputsRequest, TreeState,
    WalletQueryApi, WalletServingQuery, address_lookup_to_script_hash, status_from_query_error,
};
use zinder_source::transparent_address_matches_network;
use zinder_store::{MempoolEvent, RawBlobRetention};

use crate::mempool::{
    MempoolSnapshotPage, MempoolSurfaceError, SharedMempoolSurface, SharedTipChangeWatcher,
    TipChangeWatcherError,
};

type GrpcStream<T> =
    Pin<Box<dyn tonic::codegen::tokio_stream::Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone, Copy)]
struct EntryChainView {
    epoch_id: ChainEpochId,
    visible_tip_height: BlockHeight,
}

/// Default maximum subtree roots returned when lightwalletd requests "all entries".
pub const DEFAULT_MAX_LIGHTWALLETD_SUBTREE_ROOTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);
/// Default maximum transparent UTXOs returned when lightwalletd requests "all entries".
pub const DEFAULT_MAX_LIGHTWALLETD_ADDRESS_UTXOS: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);
/// Page size used to drain the native mempool snapshot for lightwalletd.
const LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE: u32 = 1024;
/// Blocks a slow `GetBlockRange` client can stall behind, on top of the chunk
/// the producer is currently walking.
const COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY: usize = 16;
/// Maximum number of transaction-id suffixes accepted by one `GetMempoolTx` request.
const MAX_EXCLUDED_TXID_SUFFIXES_PER_REQUEST: usize = 1024;

/// Response bounds for [`LightwalletdGrpcAdapter`].
///
/// These bounds limit response materialization. They do not control advertised
/// compatibility support, which is derived only by
/// [`LightwalletdGrpcAdapter::from_admitted_compatibility_query`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LightwalletdResponseLimits {
    /// Bound used when `GetSubtreeRoots.maxEntries` is zero.
    ///
    /// Upstream lightwalletd defines zero as "all entries". Zinder keeps the
    /// response bounded so one compatibility request cannot materialize
    /// unbounded history.
    pub max_subtree_roots: NonZeroU32,
    /// Bound used when `GetAddressUtxos.maxEntries` is zero.
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
    transparent_address_support: bool,
    mempool_surface: Option<SharedMempoolSurface>,
    tip_change_watcher: Option<SharedTipChangeWatcher>,
}

impl<QueryApi: std::fmt::Debug> std::fmt::Debug for LightwalletdGrpcAdapter<QueryApi> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LightwalletdGrpcAdapter")
            .field("query_api", &self.query_api)
            .field("response_limits", &self.response_limits)
            .field(
                "transparent_address_support",
                &self.transparent_address_support,
            )
            .field("mempool_surface", &self.mempool_surface.is_some())
            .field("tip_change_watcher", &self.tip_change_watcher.is_some())
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
    pub const fn with_response_limits(
        query_api: QueryApi,
        response_limits: LightwalletdResponseLimits,
    ) -> Self {
        Self {
            query_api,
            response_limits,
            transparent_address_support: false,
            mempool_surface: None,
            tip_change_watcher: None,
        }
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

    /// Wraps this adapter in the generated tonic server type.
    #[must_use]
    pub fn into_server(self) -> compact_tx_streamer_server::CompactTxStreamerServer<Self>
    where
        Self: compact_tx_streamer_server::CompactTxStreamer,
    {
        compact_tx_streamer_server::CompactTxStreamerServer::new(self)
            .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES)
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
            query_api,
            response_limits: LightwalletdResponseLimits::default(),
            transparent_address_support: true,
            mempool_surface: None,
            tip_change_watcher: None,
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

    async fn address_utxos(
        &self,
        request: lightwalletd::GetAddressUtxosArg,
    ) -> Result<Vec<lightwalletd::GetAddressUtxosReply>, Status> {
        if request.addresses.is_empty() {
            return Ok(Vec::new());
        }

        let start_height = u32::try_from(request.start_height)
            .map_err(|_| Status::invalid_argument("startHeight exceeds u32"))?;
        let max_entries =
            NonZeroU32::new(request.max_entries).unwrap_or(self.response_limits.max_address_utxos);
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let mut replies = Vec::new();

        for address in request.addresses {
            let query_request = transparent_address_unspent_outputs_request(
                &address,
                visible_tip_block.chain_epoch.network,
                BlockHeight::new(start_height),
            )?;
            let address_utxos = self
                .query_api
                .transparent_address_unspent_outputs(
                    query_request,
                    Some(visible_tip_block.chain_epoch.id),
                )
                .await
                .map_err(|error| status_from_query_error(&error))?;
            replies.extend(lightwalletd_address_utxos(&address, &address_utxos)?);
        }

        replies.sort_by(|left, right| {
            left.height
                .cmp(&right.height)
                .then_with(|| left.txid.cmp(&right.txid))
                .then_with(|| left.index.cmp(&right.index))
                .then_with(|| left.address.cmp(&right.address))
        });
        replies.truncate(u32_to_usize(max_entries.get()));
        Ok(replies)
    }

    async fn transparent_address_raw_transactions(
        &self,
        filter: &lightwalletd::TransparentAddressBlockFilter,
    ) -> Result<Vec<Result<lightwalletd::RawTransaction, Status>>, Status> {
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let at_epoch_id = visible_tip_block.chain_epoch.id;
        let typed_request =
            transparent_address_tx_history_request(filter, visible_tip_block.chain_epoch.network)?;
        let history = transparent_address_tx_history(&self.query_api, typed_request).await?;
        let mut raw_transactions = Vec::with_capacity(history.len());

        for artifact in history {
            let response = self
                .query_api
                .transaction(artifact.transaction_id, Some(at_epoch_id))
                .await
                .map_err(|error| status_from_query_error(&error))?;
            let location = mined_location_from_status(response.status)?;
            raw_transactions
                .push(raw_transaction_from_location(&self.query_api, at_epoch_id, location).await);
        }

        Ok(raw_transactions)
    }

    /// Returns lightwalletd `Balance { value_zat: int64 }` from the wallet
    /// plane's transparent-address balance primitive.
    ///
    /// The legacy proto carries one signed `value_zat` field that wallets
    /// interpret as confirmed balance. The compat shim projects the native
    /// balance into that field by ignoring pending inflows and subtracting
    /// pending outflows from the confirmed total, saturating to zero and
    /// capping at the `int64` wire ceiling.
    async fn compat_balance_response(
        &self,
        addresses: Vec<String>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        if addresses.is_empty() {
            return Err(Status::invalid_argument("addresses list must not be empty"));
        }
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let mut script_hashes = Vec::with_capacity(addresses.len());
        for address in addresses {
            let address_script_hash = address_lookup_to_script_hash(
                Some(wallet_proto::AddressLookup {
                    selector: Some(address_lookup::Selector::Address(address)),
                }),
                visible_tip_block.chain_epoch.network,
            )
            .map_err(|error| status_from_query_error(&error))?;
            script_hashes.push(address_script_hash);
        }
        let balance = self
            .query_api
            .transparent_address_balance(script_hashes, Some(visible_tip_block.chain_epoch.id))
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let value_zat = lightwalletd_balance_value_zat(balance)?;
        Ok(Response::new(lightwalletd::Balance { value_zat }))
    }
}

fn lightwalletd_balance_value_zat(
    balance: zinder_core::TransparentAddressBalance,
) -> Result<i64, Status> {
    let value_zat = if balance.unconfirmed_delta_zat.is_negative() {
        balance
            .confirmed_zat
            .saturating_sub(balance.unconfirmed_delta_zat.unsigned_abs())
    } else {
        balance.confirmed_zat
    };
    i64::try_from(value_zat).map_err(|_| {
        Status::out_of_range(
            "transparent address balance exceeds i64::MAX; \
             use the native WalletQuery.TransparentAddressBalance surface",
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
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;

        Ok(Response::new(lightwalletd::BlockId {
            height: u64::from(visible_tip_block.height.value()),
            hash: encode_internal_block_hash(visible_tip_block.block_hash).to_vec(),
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
        let chain_view = self.entry_chain_view().await?;
        let block_range_request = request.into_inner();
        let pool_selection = pool_selection_from_request(&block_range_request.pool_types)?;
        let (block_range, is_descending) = block_range_from_request(&block_range_request)?;
        reject_future_block_height(block_range.end, chain_view.visible_tip_height)?;
        Ok(Response::new(
            stream_compact_block_range(
                self.query_api.clone(),
                block_range,
                CompactBlockWindow {
                    at_epoch_id: chain_view.epoch_id,
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
        let chain_view = self.entry_chain_view().await?;
        let block_range_request = request.into_inner();
        let (block_range, is_descending) = block_range_from_request(&block_range_request)?;
        reject_future_block_height(block_range.end, chain_view.visible_tip_height)?;
        Ok(Response::new(
            stream_compact_block_range(
                self.query_api.clone(),
                block_range,
                CompactBlockWindow {
                    at_epoch_id: chain_view.epoch_id,
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
        let response = self
            .query_api
            .transaction(transaction_id, Some(at_epoch_id))
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let location = mined_location_from_status(response.status)?;

        Ok(Response::new(
            raw_transaction_from_location(&self.query_api, at_epoch_id, location).await?,
        ))
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
        let filter = request.into_inner();
        let raw_transactions = self.transparent_address_raw_transactions(&filter).await?;
        Ok(Response::new(Box::pin(stream::iter(raw_transactions))))
    }

    type GetTaddressTransactionsStream = GrpcStream<lightwalletd::RawTransaction>;

    async fn get_taddress_transactions(
        &self,
        request: Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<Response<Self::GetTaddressTransactionsStream>, Status> {
        let filter = request.into_inner();
        let raw_transactions = self.transparent_address_raw_transactions(&filter).await?;
        Ok(Response::new(Box::pin(stream::iter(raw_transactions))))
    }

    async fn get_taddress_balance(
        &self,
        request: Request<lightwalletd::AddressList>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        let addresses = request.into_inner().addresses;
        self.compat_balance_response(addresses).await
    }

    async fn get_taddress_balance_stream(
        &self,
        request: Request<tonic::Streaming<lightwalletd::Address>>,
    ) -> Result<Response<lightwalletd::Balance>, Status> {
        let mut stream = request.into_inner();
        let mut addresses: Vec<String> = Vec::new();
        while let Some(received) = stream.next().await {
            let address = received?;
            addresses.push(address.address);
        }
        self.compat_balance_response(addresses).await
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
        // anchor guarantees at-least-once delivery; a transaction admitted
        // mid-walk may appear both in a later page and as an event.
        let first_page = mempool_surface
            .mempool_snapshot_page(LIGHTWALLETD_MEMPOOL_SNAPSHOT_PAGE_SIZE, None)
            .await
            .map_err(status_from_mempool_surface_error)?;
        let snapshot_chain_epoch_id = first_page.chain_epoch_id;
        let event_stream = mempool_surface
            .mempool_events(first_page.events_resume_cursor.clone())
            .await
            .map_err(status_from_mempool_surface_error)?;
        let snapshot_raw_transactions = mempool_snapshot_entries(mempool_surface, first_page)
            .map(|entry_outcome| entry_outcome.map(|entry| mempool_raw_transaction(&entry)));
        let raw_transaction_stream = snapshot_raw_transactions.chain(
            stream::StreamExt::filter_map(event_stream, project_added_to_raw_transaction),
        );

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
            .map_err(|error| status_from_query_error(&error))?;

        Ok(Response::new(lightwalletd_tree_state(&tree_state)?))
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

        Ok(Response::new(lightwalletd_tree_state(&tree_state)?))
    }

    type GetSubtreeRootsStream = GrpcStream<lightwalletd::SubtreeRoot>;

    async fn get_subtree_roots(
        &self,
        request: Request<lightwalletd::GetSubtreeRootsArg>,
    ) -> Result<Response<Self::GetSubtreeRootsStream>, Status> {
        let at_epoch_id = self.entry_chain_epoch_id().await?;
        let request = request.into_inner();
        let protocol = shielded_protocol_from_request(request.shielded_protocol)?;
        let max_entries =
            NonZeroU32::new(request.max_entries).unwrap_or(self.response_limits.max_subtree_roots);
        let subtree_roots = self
            .query_api
            .subtree_roots(
                SubtreeRootRange::new(
                    protocol,
                    SubtreeRootIndex::new(request.start_index),
                    max_entries,
                ),
                Some(at_epoch_id),
            )
            .await
            .map_err(|error| status_from_query_error(&error))?;

        Ok(Response::new(stream_items(lightwalletd_subtree_roots(
            &subtree_roots,
        ))))
    }

    async fn get_address_utxos(
        &self,
        request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<lightwalletd::GetAddressUtxosReplyList>, Status> {
        let replies = self.address_utxos(request.into_inner()).await?;
        Ok(Response::new(lightwalletd::GetAddressUtxosReplyList {
            address_utxos: replies,
        }))
    }

    type GetAddressUtxosStreamStream = GrpcStream<lightwalletd::GetAddressUtxosReply>;

    async fn get_address_utxos_stream(
        &self,
        request: Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<Response<Self::GetAddressUtxosStreamStream>, Status> {
        let replies = self.address_utxos(request.into_inner()).await?;
        Ok(Response::new(stream_items(replies)))
    }

    async fn get_lightd_info(
        &self,
        _request: Request<lightwalletd::Empty>,
    ) -> Result<Response<lightwalletd::LightdInfo>, Status> {
        let visible_tip_block = self
            .query_api
            .visible_tip_block(None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
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

        Ok(Response::new(lightd_info(
            &activations,
            visible_tip_block.height,
            self.transparent_address_support,
        )))
    }

    async fn ping(
        &self,
        _request: Request<lightwalletd::Duration>,
    ) -> Result<Response<lightwalletd::PingResponse>, Status> {
        Ok(Response::new(lightwalletd::PingResponse {
            entry: 0,
            exit: 0,
        }))
    }
}

async fn transparent_address_tx_history<QueryApi>(
    query_api: &QueryApi,
    mut request: TransparentAddressTxIdsInRangeRequest,
) -> Result<Vec<TransparentAddressTxIndexArtifact>, Status>
where
    QueryApi: WalletQueryApi + ?Sized,
{
    let mut artifacts = Vec::new();
    loop {
        let response = query_api
            .transparent_address_tx_ids_in_range(request.clone())
            .await
            .map_err(|error| status_from_query_error(&error))?;
        artifacts.extend(response.artifacts);
        let Some(next_cursor) = response.next_cursor else {
            return Ok(artifacts);
        };
        request.from_cursor = Some(next_cursor);
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

fn stream_items<T: Send + 'static>(items: Vec<T>) -> GrpcStream<T> {
    Box::pin(stream::iter(items.into_iter().map(Ok)))
}

/// How one `BlockRange` handler reads and shapes its window.
#[derive(Clone, Copy)]
struct CompactBlockWindow {
    /// Chain epoch every chunk read is pinned to.
    at_epoch_id: ChainEpochId,
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
/// capped at [`DEFAULT_MAX_COMPACT_BLOCK_RANGE`]. The window is split into
/// capped chunks read in wire order, so an arbitrarily wide request streams
/// while at most one chunk is materialized at a time.
///
/// The first chunk is read before the response, so an unreadable window fails
/// the call rather than a stream the client has already opened.
async fn stream_compact_block_range<QueryApi>(
    query_api: QueryApi,
    block_range: BlockHeightRange,
    window: CompactBlockWindow,
) -> Result<GrpcStream<lightwalletd::CompactBlock>, Status>
where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    let chunks = compact_block_range_chunks(block_range, window.is_descending);
    let Some((first_chunk, remaining_chunks)) = chunks.split_first() else {
        return Ok(Box::pin(stream::empty()));
    };
    let first_blocks =
        read_compact_block_chunk(&query_api, *first_chunk, window.at_epoch_id).await?;
    let first_messages = encode_compact_block_chunk(first_blocks, window);
    if remaining_chunks.is_empty() {
        return Ok(Box::pin(stream::iter(first_messages)));
    }

    let (message_sender, message_receiver) = mpsc::channel(COMPACT_BLOCK_STREAM_CHANNEL_CAPACITY);
    tokio::spawn(drive_compact_block_range(
        query_api,
        remaining_chunks.to_vec(),
        window,
        message_sender,
    ));
    Ok(Box::pin(
        stream::iter(first_messages).chain(ReceiverStream::new(message_receiver)),
    ))
}

/// Splits a compact-block window into capped chunks ordered as the wire
/// expects them, so the emitted heights stay monotonic across chunk edges.
fn compact_block_range_chunks(
    block_range: BlockHeightRange,
    is_descending: bool,
) -> Vec<BlockHeightRange> {
    let last_height = block_range.end.value();
    let mut chunks = Vec::new();
    let mut chunk_start = block_range.start.value();
    loop {
        let chunk_end = chunk_start
            .saturating_add(DEFAULT_MAX_COMPACT_BLOCK_RANGE.get().saturating_sub(1))
            .min(last_height);
        chunks.push(BlockHeightRange::inclusive(
            BlockHeight::new(chunk_start),
            BlockHeight::new(chunk_end),
        ));
        if chunk_end >= last_height {
            break;
        }
        chunk_start = chunk_end.saturating_add(1);
    }
    if is_descending {
        chunks.reverse();
    }
    chunks
}

/// Reads every chunk after the first and forwards its blocks to the sink.
///
/// A read failure or an unencodable block ends the stream with that status;
/// a client that stops draining ends the walk on the next send.
async fn drive_compact_block_range<QueryApi>(
    query_api: QueryApi,
    chunks: Vec<BlockHeightRange>,
    window: CompactBlockWindow,
    message_sender: mpsc::Sender<Result<lightwalletd::CompactBlock, Status>>,
) where
    QueryApi: WalletQueryApi + Clone + Send + Sync + 'static,
{
    for chunk in chunks {
        let compact_blocks =
            match read_compact_block_chunk(&query_api, chunk, window.at_epoch_id).await {
                Ok(compact_blocks) => compact_blocks,
                Err(status) => {
                    let _ = message_sender.send(Err(status)).await;
                    return;
                }
            };
        for message in encode_compact_block_chunk(compact_blocks, window) {
            let is_terminal = message.is_err();
            if message_sender.send(message).await.is_err() || is_terminal {
                return;
            }
        }
    }
}

async fn read_compact_block_chunk<QueryApi>(
    query_api: &QueryApi,
    chunk: BlockHeightRange,
    at_epoch_id: ChainEpochId,
) -> Result<Vec<CompactBlockArtifact>, Status>
where
    QueryApi: WalletQueryApi + Send + Sync,
{
    query_api
        .compact_blocks_in_range(chunk, Some(at_epoch_id))
        .await
        .map(|compact_block_range| compact_block_range.compact_blocks)
        .map_err(|error| status_from_query_error(&error))
}

fn encode_compact_block_chunk(
    mut compact_blocks: Vec<CompactBlockArtifact>,
    window: CompactBlockWindow,
) -> Vec<Result<lightwalletd::CompactBlock, Status>> {
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

fn compact_transaction_has_payload(transaction: &lightwalletd::CompactTx) -> bool {
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
fn mined_location_from_status(status: TxStatus) -> Result<TransactionLocation, Status> {
    match status {
        TxStatus::Mined(mined) => Ok(mined.location),
        TxStatus::NotFound | TxStatus::InMempool(_) => Err(Status::not_found(
            "transaction is not mined in the canonical chain",
        )),
        _ => Err(Status::not_found(
            "transaction status is not representable on the lightwalletd wire",
        )),
    }
}

async fn raw_transaction_from_location<Q: WalletQueryApi + ?Sized>(
    query_api: &Q,
    chain_epoch_id: ChainEpochId,
    location: TransactionLocation,
) -> Result<lightwalletd::RawTransaction, Status> {
    let raw_transaction = query_api
        .raw_transaction(location.transaction_id, Some(chain_epoch_id))
        .await
        .map_err(|error| status_from_query_error(&error))?;
    if raw_transaction.transaction.location != location {
        return Err(Status::data_loss(
            "stored raw transaction location does not match the indexed transaction location",
        ));
    }
    Ok(lightwalletd::RawTransaction {
        data: raw_transaction.transaction.raw_transaction_bytes,
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

fn transparent_address_tx_history_request(
    filter: &lightwalletd::TransparentAddressBlockFilter,
    network: Network,
) -> Result<TransparentAddressTxIdsInRangeRequest, Status> {
    let address_text = filter.address.as_str();
    let zebra_address: ZebraTransparentAddress = address_text.parse().map_err(|source| {
        Status::invalid_argument(format!("transparent address is invalid: {source}"))
    })?;
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
    let end_block = range
        .end
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("range.end is required"))?;
    let start_height = block_height_from_id(start_block)?;
    let end_height = block_height_from_id(end_block)?;
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
        descending: false,
    })
}

fn transparent_address_unspent_outputs_request(
    address: &str,
    network: Network,
    start_height: BlockHeight,
) -> Result<TransparentAddressUnspentOutputsRequest, Status> {
    let transparent_address = address
        .parse::<ZebraTransparentAddress>()
        .map_err(|source| {
            Status::invalid_argument(format!("transparent address is invalid: {source}"))
        })?;
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

fn lightwalletd_address_utxos(
    address: &str,
    address_utxos: &TransparentAddressUnspentOutputs,
) -> Result<Vec<lightwalletd::GetAddressUtxosReply>, Status> {
    address_utxos
        .outputs
        .iter()
        .map(|utxo| {
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
        })
        .collect()
}

fn lightwalletd_tree_state(tree_state: &TreeState) -> Result<lightwalletd::TreeState, Status> {
    let payload: Value = serde_json::from_slice(&tree_state.payload_bytes).map_err(|source| {
        Status::data_loss(format!("indexed tree-state payload is not JSON: {source}"))
    })?;

    Ok(lightwalletd::TreeState {
        network: encode_bip70_chain_name(tree_state.chain_epoch.network).to_owned(),
        height: u64::from(tree_state.height.value()),
        hash: encode_rpc_block_hash_hex(tree_state.block_hash),
        time: tree_state.block_time_seconds,
        sapling_tree: tree_state_pool_final_state(&payload, "sapling")?,
        orchard_tree: tree_state_pool_final_state(&payload, "orchard")?,
        ironwood_tree: tree_state_pool_final_state(&payload, "ironwood")?,
    })
}

fn tree_state_pool_final_state(payload: &Value, pool_name: &'static str) -> Result<String, Status> {
    let Some(pool) = payload.get(pool_name) else {
        return Ok(String::new());
    };
    let Some(pool_fields) = pool.as_object() else {
        return Err(Status::data_loss(format!(
            "{pool_name} tree-state pool must be a JSON object"
        )));
    };
    let Some(commitments) = pool_fields.get("commitments") else {
        return Ok(String::new());
    };

    if let Some(final_state) = commitments.get("finalState").and_then(Value::as_str) {
        return Ok(final_state.to_owned());
    }

    match commitments {
        Value::Object(fields) if fields.is_empty() => Ok(String::new()),
        Value::Object(_) => Err(Status::data_loss(format!(
            "{pool_name} tree-state commitments are missing finalState"
        ))),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Err(Status::data_loss(format!(
                "{pool_name} tree-state commitments must be a JSON object"
            )))
        }
    }
}

fn lightwalletd_subtree_roots(subtree_roots: &SubtreeRoots) -> Vec<lightwalletd::SubtreeRoot> {
    subtree_roots
        .subtree_roots
        .iter()
        .map(|subtree_root| lightwalletd::SubtreeRoot {
            root_hash: subtree_root.root_hash.as_bytes().to_vec(),
            completing_block_hash: encode_internal_block_hash(subtree_root.completing_block_hash)
                .to_vec(),
            completing_block_height: u64::from(subtree_root.completing_block_height.value()),
        })
        .collect()
}

fn lightd_info(
    activations: &NetworkUpgradeActivations,
    tip_height: BlockHeight,
    transparent_address_support: bool,
) -> lightwalletd::LightdInfo {
    let current = activations.active_at(tip_height);
    let consensus_branch_id = current.map_or_else(
        || "00000000".to_owned(),
        |activation| encode_branch_id_hex(activation.branch_id),
    );
    let upgrade_name = current
        .map(|activation| activation.name.clone())
        .unwrap_or_default();
    let upgrade_height = current.map_or(0, |activation| {
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
        estimated_height: u64::from(tip_height.value()),
        // Deliberately empty: Zinder is not zcashd; impersonating a build
        // version misleads operators inspecting the field.
        zcashd_build: String::new(),
        zcashd_subversion: String::new(),
        // Operator-configured in lightwalletd-go. Zinder has no donation
        // address config, so the empty string preserves the unset convention.
        donation_address: String::new(),
        upgrade_name,
        upgrade_height,
        lightwallet_protocol_version: LIGHTWALLETD_PROTOCOL_COMMIT.to_owned(),
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
const fn u32_to_usize(count: u32) -> usize {
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
                        }
                    }
                    return;
                }
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(output_receiver)
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
fn project_added_to_raw_transaction(
    event_outcome: Result<zinder_store::MempoolEventEnvelope, MempoolSurfaceError>,
) -> Option<Result<lightwalletd::RawTransaction, Status>> {
    match event_outcome {
        Ok(envelope) => match envelope.event {
            MempoolEvent::Added { entry } => Some(Ok(mempool_raw_transaction(&entry))),
            _ => None,
        },
        Err(error) => Some(Err(status_from_mempool_surface_error(error))),
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

    fn transparent_address_balance(
        confirmed_zat: u64,
        unconfirmed_delta_zat: i64,
    ) -> TransparentAddressBalance {
        TransparentAddressBalance {
            confirmed_zat,
            unconfirmed_delta_zat,
            address_count: 1,
            chain_epoch: test_chain_epoch(),
        }
    }

    #[test]
    fn lightd_info_reports_the_shared_build_identity() {
        let activations = zinder_testkit::sample_regtest_upgrade_activations();
        let info = lightd_info(&activations, BlockHeight::new(1), true);

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
    fn lightwalletd_balance_projection_ignores_pending_inflows() -> Result<(), Status> {
        let balance = transparent_address_balance(100, 25);

        let value_zat = lightwalletd_balance_value_zat(balance)?;

        assert_eq!(value_zat, 100);
        Ok(())
    }

    #[test]
    fn lightwalletd_balance_projection_subtracts_pending_outflows() -> Result<(), Status> {
        let balance = transparent_address_balance(100, -25);

        let value_zat = lightwalletd_balance_value_zat(balance)?;

        assert_eq!(value_zat, 75);
        Ok(())
    }

    #[test]
    fn lightwalletd_balance_projection_saturates_pending_outflows_to_zero() -> Result<(), Status> {
        let balance = transparent_address_balance(100, -250);

        let value_zat = lightwalletd_balance_value_zat(balance)?;

        assert_eq!(value_zat, 0);
        Ok(())
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
        assert!(action.cmx.is_empty());
        assert!(action.ephemeral_key.is_empty());
        assert!(action.ciphertext.is_empty());
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
        assert!(pruned.ironwood_actions.is_empty());
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
