//! Native gRPC adapter for wallet query reads.

use std::{num::NonZeroU32, pin::Pin, time::Instant};

use tokio_stream::{self as stream, Stream, StreamExt as _, wrappers::ReceiverStream};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status, codec::CompressionEncoding};
use tonic_types::ErrorDetails;
use zinder_core::wire::{
    decode_rpc_block_hash_hex, decode_rpc_transaction_id_hex, decode_zinder_native_chain_name,
    encode_rpc_transaction_id_hex,
};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockSelector, ChainEpoch, ChainEpochId, CompactPool,
    MAX_TRANSPARENT_ADDRESSES_PER_BATCH, Network, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootIndex, SubtreeRootRange, TransactionId, TransparentAddressScriptHash,
};
use zinder_proto::{
    capabilities, status_for_reason, status_with_reason,
    v1::{
        ingest::{MempoolTransactionRequest, ingest_control_client::IngestControlClient},
        ops::ErrorReason,
        wallet::{self, wallet_query_server},
    },
    wire::{
        CHAIN_EVENT_FAMILY_HEADER, TransactionAbsence, compact_block_message_for_pools,
        compact_block_nullifiers_message, compact_pool_from_message, outpoint_from_message,
        standard_fee_message,
    },
};
use zinder_runtime::{AuthenticatedChannel, Readiness, RepeatedFieldGuard};

use crate::record_proxy_outcome;
use zinder_store::{
    ChainEventStreamFamily, HeightOrder, StreamCursorTokenV1,
    chain_event_stream_family_from_request, event_stream_start_from_request,
};

type AuthenticatedIngestControlClient = IngestControlClient<AuthenticatedChannel>;

use crate::{
    QueryError, TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputs,
    TransparentAddressUnspentOutputsRequest, WalletQueryApi, WalletServingQuery,
    ingest_control::{
        WALLET_INGEST_CONTROL_REQUEST_TIMEOUT, typed_ingest_control_status,
        wallet_ingest_control_request,
    },
};

use super::chain_events::{chain_event_stream, decode_address_filter};
use super::compact_block_stream_limit::{
    CompactBlockStreamLimit, DEFAULT_MAX_CONCURRENT_COMPACT_BLOCK_STREAMS,
};
use super::native::{
    TransactionStatusLookup, WalletEndpointMetadata, address_lookup_to_script_hash,
    block_header_by_selector_response, block_id_by_selector_response,
    broadcast_transaction_response, build_chain_view_message, build_full_block_message,
    build_node_capabilities_descriptor, build_transparent_address_tx_ids_chunk,
    build_transparent_address_tx_ids_header, build_transparent_unspent_output_message,
    build_transparent_unspent_outputs_header, build_wallet_server_info,
    chain_value_pools_at_tip_response, compact_block_response, compact_blocks_in_range_header,
    compact_blocks_in_range_item, full_block_response, latest_tree_state_checkpoint_response,
    network_upgrade_activations_response, settled_tip_block_response, subtree_roots_response,
    transaction_response, transparent_address_unspent_outputs_response,
    transparent_outpoint_spend_status_response, tree_state_at_response, visible_tip_block_response,
};
use super::request_limits::MAX_COMPACT_RANGE_POOLS;
use super::shutdown_stream::end_on_shutdown;
use super::status_from_query_error;

type WalletGrpcStream<Message> = Pin<Box<dyn Stream<Item = Result<Message, Status>> + Send>>;
type ChainEventsStream = WalletGrpcStream<wallet::ChainEventEnvelope>;
type MempoolEventsStream = WalletGrpcStream<wallet::MempoolEventEnvelope>;
type TransparentUnspentOutputsStream = WalletGrpcStream<wallet::TransparentUnspentOutputsChunk>;
type TransparentAddressTxIdsStream = WalletGrpcStream<wallet::TransparentAddressTxIdsChunk>;
type CompactBlocksInRangeStream = WalletGrpcStream<wallet::CompactBlocksInRangeChunk>;
type FullBlocksInRangeStream = WalletGrpcStream<wallet::FullBlocksInRangeChunk>;

const TRANSACTION_NOT_FOUND_IN_CHAIN: &str =
    "transaction is not in the canonical chain of the requested epoch";
const TRANSACTION_NOT_FOUND_IN_CHAIN_OR_MEMPOOL: &str =
    "transaction is not in the canonical chain or the live mempool";
const INGEST_CONTROL_NOT_COMPOSED: &str =
    "query composition does not include admitted ingest control";

/// Looks `transaction_id` up in the writer's live mempool.
async fn mempool_transaction(
    mut client: AuthenticatedIngestControlClient,
    transaction_id: TransactionId,
) -> Result<wallet::TransactionStatusResponse, Status> {
    let started_at = Instant::now();
    let proxy_outcome = client
        .mempool_transaction(wallet_ingest_control_request(MempoolTransactionRequest {
            transaction_id: encode_rpc_transaction_id_hex(transaction_id),
        }))
        .await
        .map_err(typed_ingest_control_status);
    record_proxy_outcome("mempool_transaction", started_at, &proxy_outcome);
    proxy_outcome.map(Response::into_inner)
}

/// Completes an unpinned transaction lookup whose canonical read missed at
/// `chain_epoch` by consulting the writer's live mempool.
///
/// The answer is an absence only when the mempool view proves the miss at the
/// same epoch. A mempool view at a different epoch may have dropped the
/// transaction into a block this replica has not caught up to, so that case
/// fails with `CHAIN_EPOCH_MISSING` for the writer's epoch and the caller
/// retries. A writer that answers a miss without the typed absence keeps the
/// legacy ambiguous `ARTIFACT_UNAVAILABLE` shape, never an absence.
async fn mempool_transaction_after_canonical_miss(
    client: AuthenticatedIngestControlClient,
    transaction_id: TransactionId,
    chain_epoch: ChainEpoch,
) -> Result<wallet::TransactionStatusResponse, Status> {
    let status = match mempool_transaction(client, transaction_id).await {
        Ok(response) => return Ok(response),
        Err(status) => status,
    };
    match TransactionAbsence::from_status(&status) {
        Ok(Some(mempool_absence)) if mempool_absence.chain_epoch_id == chain_epoch.id => {
            Err(TransactionAbsence::at_epoch(chain_epoch, true)
                .into_status(TRANSACTION_NOT_FOUND_IN_CHAIN_OR_MEMPOOL))
        }
        Ok(Some(mempool_absence)) => Err(status_with_reason(
            ErrorReason::ChainEpochMissing,
            format!(
                "the live mempool answered at chain epoch {} but this replica read chain epoch {}; retry",
                mempool_absence.chain_epoch_id.value(),
                chain_epoch.id.value()
            ),
            ErrorDetails::with_resource_info(
                zinder_core::artifact_family::CHAIN_EPOCH,
                format!("chain_epoch:{}", mempool_absence.chain_epoch_id.value()),
                "zinder-query",
                "the writer's mempool epoch is not visible to this replica",
            ),
        )),
        Ok(None) if status.code() == Code::NotFound => Err(status_for_reason(
            ErrorReason::ArtifactUnavailable,
            "the live mempool answered without a typed absence; transaction absence cannot be proven",
        )),
        Ok(None) => Err(status),
        Err(metadata_error) => Err(status_for_reason(
            ErrorReason::ArtifactCorrupt,
            format!("the live mempool returned a malformed transaction absence: {metadata_error}"),
        )),
    }
}

/// Completes an unpinned transaction lookup whose canonical read missed on a
/// store whose history starts after a checkpoint.
///
/// A pending transaction needs no proof of absence, so a mempool hit answers
/// the lookup as it would on a store with complete history. A mempool miss
/// leaves `history_floor` standing, because the transaction may be mined below
/// the floor. Any other mempool failure keeps its own status, so a caller that
/// retries a transient one can still learn that the transaction is pending.
async fn mempool_transaction_below_history_floor(
    client: AuthenticatedIngestControlClient,
    transaction_id: TransactionId,
    history_floor: Status,
) -> Result<wallet::TransactionStatusResponse, Status> {
    match mempool_transaction(client, transaction_id).await {
        Err(status) if status.code() == Code::NotFound => Err(history_floor),
        outcome => outcome,
    }
}

/// gRPC adapter for a [`WalletQueryApi`] implementation.
///
/// The adapter owns no live-provider configuration or connection state. Its
/// query was fully composed before this protocol boundary was constructed.
#[derive(Clone, Debug)]
pub struct WalletQueryGrpcAdapter<QueryApi> {
    query_api: QueryApi,
    server_info: wallet::WalletServerInfo,
    traffic_readiness: Option<Readiness>,
    shutdown: Option<CancellationToken>,
    compact_block_streams: CompactBlockStreamLimit,
}

impl<QueryApi: WalletQueryApi> WalletQueryGrpcAdapter<QueryApi> {
    /// Creates a gRPC adapter over a wallet query API with the deployment's
    /// `ServerCapabilities` descriptor.
    #[must_use]
    pub fn new(query_api: QueryApi, metadata: WalletEndpointMetadata) -> Self {
        let server_info = build_wallet_server_info(
            metadata,
            query_api.native_endpoint_capabilities(),
            query_api.upstream_node_capabilities().as_ref(),
        );
        Self {
            query_api,
            server_info,
            traffic_readiness: None,
            shutdown: None,
            compact_block_streams: CompactBlockStreamLimit::new(
                DEFAULT_MAX_CONCURRENT_COMPACT_BLOCK_STREAMS,
            ),
        }
    }

    /// Admits up to `max_streams` active compact-block and nullifier ranges.
    /// Additional streams fail with `RESOURCE_EXHAUSTED`.
    ///
    /// The default is
    /// [`DEFAULT_MAX_CONCURRENT_COMPACT_BLOCK_STREAMS`](crate::DEFAULT_MAX_CONCURRENT_COMPACT_BLOCK_STREAMS).
    #[must_use]
    pub fn with_max_concurrent_compact_block_streams(mut self, max_streams: NonZeroU32) -> Self {
        self.compact_block_streams = CompactBlockStreamLimit::new(max_streams);
        self
    }

    /// Shares the readiness behind the traffic gate with open chain-event
    /// streams.
    ///
    /// The gate only rejects new requests. With this readiness, a stream that
    /// is already open ends with the same typed `REORG_WINDOW_EXCEEDED`
    /// status once the canonical writer parks, instead of idling on a chain
    /// that no longer advances.
    #[must_use]
    pub fn with_traffic_readiness(mut self, readiness: Readiness) -> Self {
        self.traffic_readiness = Some(readiness);
        self
    }

    /// Ends open server streams once `shutdown` is cancelled.
    ///
    /// Graceful server shutdown waits for every open call, and `ChainEvents`
    /// and `MempoolEvents` never end on their own. With this token, every
    /// server stream ends with `UNAVAILABLE` and readiness cause
    /// `shutting_down` when shutdown starts, so the server can drain.
    #[must_use]
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    fn server_stream<Message: Send + 'static>(
        &self,
        stream: WalletGrpcStream<Message>,
    ) -> Response<WalletGrpcStream<Message>> {
        Response::new(end_on_shutdown(stream, self.shutdown.as_ref()))
    }

    /// Wraps this adapter in the generated tonic server type, behind the
    /// pre-decode bounds of [`WALLET_QUERY_FIELD_LIMITS`](super::WALLET_QUERY_FIELD_LIMITS).
    #[must_use]
    pub fn into_server(self) -> RepeatedFieldGuard<wallet_query_server::WalletQueryServer<Self>>
    where
        Self: wallet_query_server::WalletQuery,
    {
        RepeatedFieldGuard::new(
            wallet_query_server::WalletQueryServer::new(self)
                .accept_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Zstd)
                .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES),
            super::WALLET_QUERY_FIELD_LIMITS,
        )
        .with_request_compression()
    }

    fn decode_address_batch(
        &self,
        addresses: Vec<wallet::AddressLookup>,
    ) -> Result<Vec<TransparentAddressScriptHash>, Status> {
        if !(1..=MAX_TRANSPARENT_ADDRESSES_PER_BATCH).contains(&addresses.len()) {
            return Err(Status::invalid_argument(format!(
                "addresses must contain between 1 and {MAX_TRANSPARENT_ADDRESSES_PER_BATCH} selectors",
            )));
        }
        let network = self.server_info_network()?;
        addresses
            .into_iter()
            .map(|address| {
                address_lookup_to_script_hash(Some(address), network)
                    .map_err(|error| status_from_query_error(&error))
            })
            .collect()
    }

    fn unspent_outputs_stream(
        &self,
        response: TransparentAddressUnspentOutputs,
    ) -> Response<TransparentUnspentOutputsStream> {
        let header = Ok(build_transparent_unspent_outputs_header(
            response.chain_epoch,
        ));
        let outputs = response.map(|output| {
            output
                .map(|output| build_transparent_unspent_output_message(&output))
                .map_err(|error| status_from_query_error(&error))
        });
        self.server_stream(Box::pin(stream::once(header).chain(outputs)))
    }

    fn require_endpoint_capability(&self, capability: &'static str) -> Result<(), Status> {
        if self
            .query_api
            .native_endpoint_capabilities()
            .contains(capability)
        {
            Ok(())
        } else {
            Err(status_from_query_error(
                &crate::QueryError::EndpointCapabilityUnavailable { capability },
            ))
        }
    }
}

#[tonic::async_trait]
trait IngestControlClientAccess {
    async fn ingest_control_client(
        &self,
    ) -> Result<Option<AuthenticatedIngestControlClient>, Status>;
}

#[tonic::async_trait]
impl<Broadcaster> IngestControlClientAccess
    for WalletServingQuery<Broadcaster, crate::AdmittedIngestControl>
where
    Self: WalletQueryApi,
{
    async fn ingest_control_client(
        &self,
    ) -> Result<Option<AuthenticatedIngestControlClient>, Status> {
        Ok(Some(self.admitted_native_ingest().client()))
    }
}

#[tonic::async_trait]
impl<QueryApi> wallet_query_server::WalletQuery for WalletQueryGrpcAdapter<QueryApi>
where
    QueryApi: Clone + WalletQueryApi + IngestControlClientAccess + Send + Sync + 'static,
{
    type CompactNullifiersInRangeStream = WalletGrpcStream<wallet::CompactNullifiersInRangeChunk>;
    type TransparentAddressesUnspentOutputsStream = Self::TransparentAddressUnspentOutputsStream;

    #[allow(
        clippy::significant_drop_tightening,
        reason = "The stream closure holds its admission slot until the client drops the stream."
    )]
    async fn compact_nullifiers_in_range(
        &self,
        request: Request<wallet::CompactNullifiersInRangeRequest>,
    ) -> Result<Response<Self::CompactNullifiersInRangeStream>, Status> {
        use wallet::compact_nullifiers_in_range_chunk::Body;

        self.require_endpoint_capability(capabilities::WALLET_READ_COMPACT_NULLIFIER_RANGE_V1)?;
        let request = request.into_inner();
        let pools = decode_compact_pools(request.pool_types)?;
        if pools.contains(&CompactPool::Transparent) {
            return Err(Status::invalid_argument(
                "nullifier ranges accept shielded pools only",
            ));
        }
        let (range, order) = ordered_block_range(request.start_height, request.end_height);
        crate::validate_block_range(range, crate::DEFAULT_MAX_COMPACT_BLOCK_RANGE)
            .map_err(|error| status_from_query_error(&error))?;
        let slot = self
            .compact_block_streams
            .try_open()
            .map_err(|error| status_from_query_error(&error))?;
        let open = self.query_api.compact_nullifier_stream(
            range,
            order,
            pools,
            chain_epoch_id_from_request(request.at_epoch_id),
        );
        let response = slot
            .clone()
            .hold_during(open)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let header = stream::once(Ok(wallet::CompactNullifiersInRangeChunk {
            body: Some(Body::Header(build_chain_view_message(response.chain_epoch))),
        }));
        let blocks = ReceiverStream::new(response.blocks).map(move |block| {
            let _stream_slot = &slot;
            block
                .map(|block| wallet::CompactNullifiersInRangeChunk {
                    body: Some(Body::Block(compact_block_nullifiers_message(&block))),
                })
                .map_err(|error| status_from_query_error(&error))
        });
        Ok(self.server_stream(Box::pin(header.chain(blocks))))
    }

    async fn standard_fee(
        &self,
        _: Request<wallet::StandardFeeRequest>,
    ) -> Result<Response<wallet::StandardFeeResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_STANDARD_FEE_V1)?;
        let fee = self
            .query_api
            .standard_fee()
            .await
            .map_err(|error| status_from_query_error(&error))?;
        Ok(Response::new(standard_fee_message(fee)))
    }

    async fn transparent_address_balances(
        &self,
        request: Request<wallet::TransparentAddressBalancesRequest>,
    ) -> Result<Response<wallet::TransparentAddressBalancesResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_ADDRESS_TRANSPARENT_BALANCES_V1)?;
        let request = request.into_inner();
        let addresses = self.decode_address_batch(request.addresses)?;
        let balance = self
            .query_api
            .transparent_address_balance(
                addresses,
                chain_epoch_id_from_request(request.at_epoch_id),
            )
            .await
            .map_err(|error| status_from_query_error(&error))?;
        Ok(Response::new(wallet::TransparentAddressBalancesResponse {
            chain_view: Some(build_chain_view_message(balance.chain_epoch)),
            confirmed_zat: balance.confirmed_zat,
            address_count: balance.address_count,
        }))
    }

    async fn transparent_addresses_unspent_outputs(
        &self,
        request: Request<wallet::TransparentAddressesUnspentOutputsRequest>,
    ) -> Result<Response<Self::TransparentAddressesUnspentOutputsStream>, Status> {
        self.require_endpoint_capability(
            capabilities::WALLET_ADDRESS_TRANSPARENT_BATCH_UNSPENT_OUTPUTS_V1,
        )?;
        let request = request.into_inner();
        let addresses = self.decode_address_batch(request.addresses)?;
        let response = self
            .query_api
            .transparent_addresses_unspent_outputs(
                addresses,
                BlockHeight::new(request.start_height),
                chain_epoch_id_from_request(request.at_epoch_id),
            )
            .await
            .map_err(|error| status_from_query_error(&error))?;
        Ok(self.unspent_outputs_stream(response))
    }

    type CompactBlocksInRangeStream = CompactBlocksInRangeStream;
    type FullBlocksInRangeStream = FullBlocksInRangeStream;
    type ChainEventsStream = ChainEventsStream;
    type MempoolEventsStream = MempoolEventsStream;
    type TransparentAddressUnspentOutputsStream = TransparentUnspentOutputsStream;
    type TransparentAddressTxIdsInRangeStream = TransparentAddressTxIdsStream;

    async fn visible_tip_block(
        &self,
        request: Request<wallet::VisibleTipBlockRequest>,
    ) -> Result<Response<wallet::VisibleTipBlockResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_VISIBLE_TIP_BLOCK_V1)?;
        visible_tip_block_response(
            &self.query_api,
            chain_epoch_id_from_request(request.into_inner().at_epoch_id),
        )
        .await
        .map(Response::new)
        .map_err(|error| status_from_query_error(&error))
    }

    async fn settled_tip_block(
        &self,
        request: Request<wallet::SettledTipBlockRequest>,
    ) -> Result<Response<wallet::SettledTipBlockResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_SETTLED_TIP_BLOCK_V1)?;
        settled_tip_block_response(
            &self.query_api,
            chain_epoch_id_from_request(request.into_inner().at_epoch_id),
        )
        .await
        .map(Response::new)
        .map_err(|error| status_from_query_error(&error))
    }

    async fn compact_block(
        &self,
        request: Request<wallet::CompactBlockRequest>,
    ) -> Result<Response<wallet::CompactBlockResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_COMPACT_BLOCK_AT_V2)?;
        let request = request.into_inner();
        compact_block_response(
            &self.query_api,
            BlockHeight::new(request.height),
            chain_epoch_id_from_request(request.at_epoch_id),
        )
        .await
        .map(Response::new)
        .map_err(|error| status_from_query_error(&error))
    }

    async fn full_block(
        &self,
        request: Request<wallet::FullBlockRequest>,
    ) -> Result<Response<wallet::FullBlockResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_FULL_BLOCK_AT_V1)?;
        let request = request.into_inner();
        full_block_response(
            &self.query_api,
            BlockHeight::new(request.height),
            chain_epoch_id_from_request(request.at_epoch_id),
        )
        .await
        .map(Response::new)
        .map_err(|error| status_from_query_error(&error))
    }

    async fn block_id_by_selector(
        &self,
        request: Request<wallet::BlockSelectorRequest>,
    ) -> Result<Response<wallet::BlockIdResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_BLOCK_ID_BY_SELECTOR_V1)?;
        let request = request.into_inner();
        let selector = block_selector_from_request(request.selector)?;
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);
        block_id_by_selector_response(&self.query_api, selector, at_epoch)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn block_header_by_selector(
        &self,
        request: Request<wallet::BlockSelectorRequest>,
    ) -> Result<Response<wallet::BlockHeaderResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_BLOCK_HEADER_BY_SELECTOR_V1)?;
        let request = request.into_inner();
        let selector = block_selector_from_request(request.selector)?;
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);
        block_header_by_selector_response(&self.query_api, selector, at_epoch)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn transaction(
        &self,
        request: Request<wallet::TransactionRequest>,
    ) -> Result<Response<wallet::TransactionStatusResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_TRANSACTION_BY_ID_V2)?;
        let request = request.into_inner();
        let transaction_id = transaction_id_from_request(&request.transaction_id)?;
        let at_epoch_id = chain_epoch_id_from_request(request.at_epoch_id);
        let canonical_lookup =
            transaction_response(&self.query_api, transaction_id, at_epoch_id).await;

        let chain_epoch = match canonical_lookup {
            Ok(TransactionStatusLookup::Located(response)) => return Ok(Response::new(*response)),
            Ok(TransactionStatusLookup::Absent { chain_epoch }) => chain_epoch,
            Err(error @ QueryError::CanonicalHistoryUnavailable { .. })
                if at_epoch_id.is_none() =>
            {
                self.query_api
                    .verify_forwarding_node(None)
                    .await
                    .map_err(|error| status_from_query_error(&error))?;
                let client = self
                    .ingest_control_client(INGEST_CONTROL_NOT_COMPOSED)
                    .await?;
                return mempool_transaction_below_history_floor(
                    client,
                    transaction_id,
                    status_from_query_error(&error),
                )
                .await
                .map(Response::new);
            }
            Err(error) => return Err(status_from_query_error(&error)),
        };
        if at_epoch_id.is_some() {
            // A pinned lookup is a canonical read; it never consults the
            // mempool, and its absence says so.
            return Err(TransactionAbsence::at_epoch(chain_epoch, false)
                .into_status(TRANSACTION_NOT_FOUND_IN_CHAIN));
        }

        self.query_api
            .verify_forwarding_node(Some(chain_epoch.id))
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let client = self
            .ingest_control_client(INGEST_CONTROL_NOT_COMPOSED)
            .await?;
        mempool_transaction_after_canonical_miss(client, transaction_id, chain_epoch)
            .await
            .map(Response::new)
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "the item closure keeps the stream slot until the client drops the stream"
    )]
    async fn compact_blocks_in_range(
        &self,
        request: Request<wallet::CompactBlocksInRangeRequest>,
    ) -> Result<Response<Self::CompactBlocksInRangeStream>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_COMPACT_BLOCK_RANGE_V2)?;
        let request = request.into_inner();
        let (block_range, order) = ordered_block_range(request.start_height, request.end_height);
        if order == HeightOrder::Descending || !request.pool_types.is_empty() {
            self.require_endpoint_capability(capabilities::WALLET_READ_COMPACT_BLOCK_RANGE_V3)?;
        }
        let pools = decode_compact_pools(request.pool_types)?;
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);
        // Only a valid range takes a slot. The query rejects any other range
        // before it reads, and records that rejection with its own error.
        let stream_slot =
            if crate::validate_block_range(block_range, crate::DEFAULT_MAX_COMPACT_BLOCK_RANGE)
                .is_ok()
            {
                Some(
                    self.compact_block_streams
                        .try_open()
                        .map_err(|error| status_from_query_error(&error))?,
                )
            } else {
                None
            };

        // The query's blocking reads and the task issuing them take clones of
        // the slot, so a cancelled request or a dropped stream keeps it until
        // the read already on the blocking pool ends.
        let open = self
            .query_api
            .ordered_compact_block_stream(block_range, order, at_epoch);
        let compact_block_stream = match stream_slot.clone() {
            Some(slot) => slot.hold_during(open).await,
            None => open.await,
        }
        .map_err(|error| status_from_query_error(&error))?;
        let header = stream::once(Ok(compact_blocks_in_range_header(
            compact_block_stream.chain_epoch,
        )));
        // The item closure keeps the slot while the client holds the stream,
        // so blocks queued for it count against the limit.
        let items = ReceiverStream::new(compact_block_stream.blocks).map(move |block| {
            let _stream_slot = &stream_slot;
            block
                .map(|compact_block| {
                    if pools.is_empty() {
                        return compact_blocks_in_range_item(&compact_block);
                    }
                    wallet::CompactBlocksInRangeChunk {
                        body: Some(wallet::compact_blocks_in_range_chunk::Body::Item(
                            compact_block_message_for_pools(&compact_block, &pools),
                        )),
                    }
                })
                .map_err(|error| status_from_query_error(&error))
        });

        Ok(self.server_stream(Box::pin(header.chain(items))))
    }

    async fn full_blocks_in_range(
        &self,
        request: Request<wallet::FullBlocksInRangeRequest>,
    ) -> Result<Response<Self::FullBlocksInRangeStream>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_FULL_BLOCK_RANGE_V1)?;
        let request = request.into_inner();
        let block_range = BlockHeightRange::inclusive(
            BlockHeight::new(request.start_height),
            BlockHeight::new(request.end_height),
        );
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);

        let full_block_stream = self
            .query_api
            .full_blocks_in_range(block_range, at_epoch)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let chain_view = build_chain_view_message(full_block_stream.chain_epoch);
        let full_block_chunks = ReceiverStream::new(full_block_stream.blocks).map(move |block| {
            block
                .map(|block_blob| wallet::FullBlocksInRangeChunk {
                    chain_view: Some(chain_view.clone()),
                    full_block: Some(build_full_block_message(block_blob)),
                })
                .map_err(|error| status_from_query_error(&error))
        });

        Ok(self.server_stream(Box::pin(full_block_chunks)))
    }

    async fn tree_state_at_height(
        &self,
        request: Request<wallet::TreeStateAtHeightRequest>,
    ) -> Result<Response<wallet::TreeStateResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_TREE_STATE_AT_HEIGHT_V2)?;
        let request = request.into_inner();
        tree_state_at_response(
            &self.query_api,
            BlockHeight::new(request.height),
            chain_epoch_id_from_request(request.at_epoch_id),
        )
        .await
        .map(Response::new)
        .map_err(|error| status_from_query_error(&error))
    }

    async fn latest_tree_state_checkpoint(
        &self,
        request: Request<wallet::LatestTreeStateCheckpointRequest>,
    ) -> Result<Response<wallet::TreeStateResponse>, Status> {
        self.require_endpoint_capability(
            capabilities::WALLET_READ_LATEST_TREE_STATE_CHECKPOINT_V2,
        )?;
        latest_tree_state_checkpoint_response(
            &self.query_api,
            chain_epoch_id_from_request(request.into_inner().at_epoch_id),
        )
        .await
        .map(Response::new)
        .map_err(|error| status_from_query_error(&error))
    }

    async fn subtree_roots(
        &self,
        request: Request<wallet::SubtreeRootsRequest>,
    ) -> Result<Response<wallet::SubtreeRootsResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_SUBTREE_ROOTS_IN_RANGE_V1)?;
        let request = request.into_inner();
        let protocol = shielded_protocol_from_request(request.shielded_protocol)?;
        if protocol == ShieldedProtocol::Ironwood {
            self.require_endpoint_capability(capabilities::WALLET_READ_SUBTREE_ROOTS_IRONWOOD_V1)?;
        }
        let max_entries = NonZeroU32::new(request.max_entries)
            .ok_or_else(|| Status::invalid_argument("max_entries must be non-zero"))?;
        let subtree_root_range = SubtreeRootRange::new(
            protocol,
            SubtreeRootIndex::new(request.start_index),
            max_entries,
        );
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);

        subtree_roots_response(&self.query_api, subtree_root_range, at_epoch)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn broadcast_transaction(
        &self,
        request: Request<wallet::BroadcastTransactionRequest>,
    ) -> Result<Response<wallet::BroadcastTransactionResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_BROADCAST_TRANSACTION_V1)?;
        let raw_transaction = RawTransactionBytes::new(request.into_inner().raw_transaction);

        broadcast_transaction_response(&self.query_api, raw_transaction)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn chain_events(
        &self,
        request: Request<wallet::ChainEventsRequest>,
    ) -> Result<Response<Self::ChainEventsStream>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_EVENTS_CHAIN_V1)?;
        let started_at = Instant::now();
        let outcome: Result<Response<ChainEventsStream>, Status> = async {
            let request = request.into_inner();
            let start = event_stream_start_from_request(request.start)?;
            let requested_family = chain_event_stream_family_from_request(request.family)?;
            let network = self.server_info_network()?;
            let address_filter = decode_address_filter(request.address_filter, network)
                .map_err(|error| status_from_query_error(&error))?;
            let resume = self
                .query_api
                .resolve_chain_events_start(start, requested_family)
                .await
                .map_err(|error| status_from_query_error(&error))?;
            let family = resume.family;
            let stream = chain_event_stream(
                self.query_api.clone(),
                resume,
                address_filter,
                self.traffic_readiness.clone(),
            );
            let mut response = self.server_stream(Box::pin(stream));
            if family == ChainEventStreamFamily::Settled {
                response.metadata_mut().insert(
                    CHAIN_EVENT_FAMILY_HEADER,
                    tonic::metadata::MetadataValue::from_static("settled"),
                );
            }
            Ok(response)
        }
        .await;
        record_proxy_outcome("chain_events", started_at, &outcome);
        outcome
    }

    async fn mempool_snapshot(
        &self,
        request: Request<wallet::MempoolSnapshotRequest>,
    ) -> Result<Response<wallet::MempoolSnapshotResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_SNAPSHOT_MEMPOOL_V3)?;
        let started_at = Instant::now();
        let outcome = async {
            let mut client = self
                .ingest_control_client(INGEST_CONTROL_NOT_COMPOSED)
                .await?;
            client
                .mempool_snapshot(wallet_ingest_control_request(request.into_inner()))
                .await
                .map_err(typed_ingest_control_status)
        }
        .await;
        record_proxy_outcome("mempool_snapshot", started_at, &outcome);
        outcome
    }

    async fn mempool_events(
        &self,
        request: Request<wallet::MempoolEventsRequest>,
    ) -> Result<Response<Self::MempoolEventsStream>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_EVENTS_MEMPOOL_V2)?;
        let started_at = Instant::now();
        let outcome: Result<Response<MempoolEventsStream>, Status> = async {
            let mut client = self
                .ingest_control_client(INGEST_CONTROL_NOT_COMPOSED)
                .await?;
            // A gRPC deadline applies to the entire server stream. Bound only
            // response establishment here so a healthy long-lived event
            // stream is not cancelled after the unary request timeout.
            let response = tokio::time::timeout(
                WALLET_INGEST_CONTROL_REQUEST_TIMEOUT,
                client.mempool_events(crate::wallet_mempool_events_request(request.into_inner())),
            )
            .await
            .map_err(|_| {
                status_for_reason(
                    ErrorReason::UpstreamUnreachable,
                    "ingest-control mempool event stream establishment timed out",
                )
            })?
            .map_err(typed_ingest_control_status)?;
            let events = response
                .into_inner()
                .map(|event| event.map_err(typed_ingest_control_status));
            Ok(self.server_stream(Box::pin(events)))
        }
        .await;
        record_proxy_outcome("mempool_events", started_at, &outcome);
        outcome
    }

    async fn chain_value_pools_at_tip(
        &self,
        _request: Request<wallet::ChainValuePoolsAtTipRequest>,
    ) -> Result<Response<wallet::ChainValuePoolsAtTipResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_CHAIN_VALUE_POOLS_AT_TIP_V1)?;
        chain_value_pools_at_tip_response(&self.query_api)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn transparent_address_unspent_outputs(
        &self,
        request: Request<wallet::TransparentAddressUnspentOutputsRequest>,
    ) -> Result<Response<Self::TransparentAddressUnspentOutputsStream>, Status> {
        self.require_endpoint_capability(
            capabilities::WALLET_ADDRESS_TRANSPARENT_UNSPENT_OUTPUTS_V1,
        )?;
        let request = request.into_inner();
        let at_epoch_id = chain_epoch_id_from_request(request.at_epoch_id);
        let address_script_hash =
            address_lookup_to_script_hash(request.address, self.server_info_network()?)
                .map_err(|error| status_from_query_error(&error))?;
        let typed_request = TransparentAddressUnspentOutputsRequest {
            address_script_hash,
            start_height: BlockHeight::new(request.start_height),
        };
        let response = transparent_address_unspent_outputs_response(
            &self.query_api,
            typed_request,
            at_epoch_id,
        )
        .await
        .map_err(|error| status_from_query_error(&error))?;
        Ok(self.unspent_outputs_stream(response))
    }

    async fn transparent_address_tx_ids_in_range(
        &self,
        request: Request<wallet::TransparentAddressTxIdsInRangeRequest>,
    ) -> Result<Response<Self::TransparentAddressTxIdsInRangeStream>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_ADDRESS_TRANSPARENT_HISTORY_V1)?;
        let request = request.into_inner();
        let typed_request = transparent_address_tx_ids_in_range_request_from_message(
            request,
            self.server_info_network()?,
        )?;
        let response = self
            .query_api
            .transparent_address_tx_ids_in_range(typed_request, None)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let header = Ok(build_transparent_address_tx_ids_header(
            response.chain_epoch,
        ));
        let next_cursor_bytes = response
            .next_cursor
            .map_or_default(|cursor| cursor.as_bytes().to_vec());
        let last_index = response.artifacts.len().saturating_sub(1);
        let items = response
            .artifacts
            .into_iter()
            .enumerate()
            .map(move |(index, artifact)| {
                let cursor = if index == last_index {
                    next_cursor_bytes.clone()
                } else {
                    Vec::new()
                };
                Ok(build_transparent_address_tx_ids_chunk(&artifact, cursor))
            });
        let chunks = stream::once(header).chain(stream::iter(items));
        Ok(self.server_stream(Box::pin(chunks)))
    }

    async fn transparent_outpoint_spend_status(
        &self,
        request: Request<wallet::TransparentOutpointSpendStatusRequest>,
    ) -> Result<Response<wallet::TransparentOutpointSpendStatusResponse>, Status> {
        self.require_endpoint_capability(
            capabilities::WALLET_READ_TRANSPARENT_OUTPOINT_SPEND_STATUS_V1,
        )?;
        let request = request.into_inner();
        let outpoints = request
            .outpoints
            .iter()
            .map(|outpoint| outpoint_from_message("outpoints.transaction_id", outpoint))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        transparent_outpoint_spend_status_response(
            &self.query_api,
            outpoints,
            chain_epoch_id_from_request(request.at_epoch_id),
        )
        .await
        .map(Response::new)
        .map_err(|error| status_from_query_error(&error))
    }

    async fn server_info(
        &self,
        _request: Request<wallet::ServerInfoRequest>,
    ) -> Result<Response<wallet::ServerInfoResponse>, Status> {
        let mut server_info = self.server_info.clone();
        // The node endpoint is verified after startup, so its capabilities
        // are read per request.
        server_info.node = Some(build_node_capabilities_descriptor(
            self.query_api.upstream_node_capabilities().as_ref(),
        ));
        Ok(Response::new(wallet::ServerInfoResponse {
            info: Some(server_info),
        }))
    }

    async fn network_upgrade_activations(
        &self,
        _request: Request<wallet::NetworkUpgradeActivationsRequest>,
    ) -> Result<Response<wallet::NetworkUpgradeActivationsResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_NETWORK_UPGRADE_ACTIVATIONS_V1)?;
        network_upgrade_activations_response(&self.query_api)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }
}

fn decode_compact_pools(pools: Vec<i32>) -> Result<Vec<CompactPool>, Status> {
    if pools.len() > MAX_COMPACT_RANGE_POOLS {
        return Err(Status::invalid_argument(
            "pool_types may contain at most four entries",
        ));
    }
    pools
        .into_iter()
        .map(|pool| {
            compact_pool_from_message(pool).ok_or_else(|| {
                Status::invalid_argument("pool_types contains an unknown or unspecified pool")
            })
        })
        .collect()
}

fn ordered_block_range(start: u32, end: u32) -> (BlockHeightRange, HeightOrder) {
    let (first, last, order) = if start > end {
        (end, start, HeightOrder::Descending)
    } else {
        (start, end, HeightOrder::Ascending)
    };
    (
        BlockHeightRange::inclusive(BlockHeight::new(first), BlockHeight::new(last)),
        order,
    )
}

fn transparent_address_tx_ids_in_range_request_from_message(
    request: wallet::TransparentAddressTxIdsInRangeRequest,
    network: Network,
) -> Result<TransparentAddressTxIdsInRangeRequest, Status> {
    let address_script_hash = address_lookup_to_script_hash(request.address, network)
        .map_err(|error| status_from_query_error(&error))?;
    let max_entries =
        max_entries_from_u32(request.max_entries, DEFAULT_MAX_TRANSPARENT_HISTORY_ENTRIES);
    let from_cursor = if request.from_cursor.is_empty() {
        None
    } else {
        Some(StreamCursorTokenV1::from_bytes(request.from_cursor))
    };

    Ok(TransparentAddressTxIdsInRangeRequest {
        address_script_hash,
        start_height: BlockHeight::new(request.start_height),
        end_height: BlockHeight::new(request.end_height),
        max_entries,
        from_cursor,
    })
}

const DEFAULT_MAX_TRANSPARENT_HISTORY_ENTRIES: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);

fn max_entries_from_u32(requested: u32, cap: NonZeroU32) -> NonZeroU32 {
    NonZeroU32::new(requested).map_or(cap, |max_entries| clamp_max_entries(max_entries, cap))
}

fn clamp_max_entries(requested: NonZeroU32, cap: NonZeroU32) -> NonZeroU32 {
    if requested > cap { cap } else { requested }
}

impl<QueryApi> WalletQueryGrpcAdapter<QueryApi> {
    fn server_info_network(&self) -> Result<Network, Status> {
        let network = self
            .server_info
            .common
            .as_ref()
            .map_or_default(|common| common.network.as_str());
        decode_zinder_native_chain_name(network)
            .ok()
            .ok_or_else(|| {
                Status::internal(format!(
                    "server network identifier {network} is not recognized"
                ))
            })
    }

    /// Returns the client owned by the already-composed query.
    async fn ingest_control_client(
        &self,
        unconfigured_message: &'static str,
    ) -> Result<AuthenticatedIngestControlClient, Status>
    where
        QueryApi: IngestControlClientAccess + Sync,
    {
        IngestControlClientAccess::ingest_control_client(&self.query_api)
            .await?
            .ok_or_else(|| Status::unavailable(unconfigured_message))
    }
}

fn transaction_id_from_request(transaction_id_rpc_hex: &str) -> Result<TransactionId, Status> {
    decode_rpc_transaction_id_hex(transaction_id_rpc_hex)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn chain_epoch_id_from_request(at_epoch_id: Option<u64>) -> Option<ChainEpochId> {
    at_epoch_id.map(ChainEpochId::new)
}

fn block_selector_from_request(
    selector: Option<wallet::BlockSelector>,
) -> Result<BlockSelector, Status> {
    let inner = selector
        .and_then(|message| message.selector)
        .ok_or_else(|| Status::invalid_argument("block selector must be specified"))?;
    match inner {
        wallet::block_selector::Selector::Height(height) => {
            Ok(BlockSelector::Height(BlockHeight::new(height)))
        }
        wallet::block_selector::Selector::Hash(hash_rpc_hex) => {
            let block_hash = decode_rpc_block_hash_hex(&hash_rpc_hex)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok(BlockSelector::Hash(block_hash))
        }
    }
}

fn shielded_protocol_from_request(protocol: i32) -> Result<ShieldedProtocol, Status> {
    match wallet::ShieldedProtocol::try_from(protocol) {
        Ok(wallet::ShieldedProtocol::Sapling) => Ok(ShieldedProtocol::Sapling),
        Ok(wallet::ShieldedProtocol::Orchard) => Ok(ShieldedProtocol::Orchard),
        Ok(wallet::ShieldedProtocol::Ironwood) => Ok(ShieldedProtocol::Ironwood),
        Ok(wallet::ShieldedProtocol::Unspecified) => Err(Status::invalid_argument(
            "shielded_protocol must be specified",
        )),
        Err(_) => Err(Status::invalid_argument("shielded_protocol is unknown")),
    }
}
