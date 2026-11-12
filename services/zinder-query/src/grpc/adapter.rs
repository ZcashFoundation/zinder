//! Native gRPC adapter for wallet query reads.

use std::{num::NonZeroU32, pin::Pin, time::Instant};

use tokio::sync::mpsc;
use tokio_stream::{self as stream, Stream, StreamExt as _, wrappers::ReceiverStream};
use tonic::{Code, Request, Response, Status};
use tonic_types::ErrorDetails;
use zinder_core::wire::{
    decode_rpc_block_hash_hex, decode_rpc_transaction_id_hex, decode_zinder_native_chain_name,
    encode_rpc_transaction_id_hex,
};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockSelector, ChainEpoch, ChainEpochId, Network,
    RawTransactionBytes, ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange, TransactionId,
};
use zinder_proto::{
    capabilities, status_for_reason, status_with_reason,
    v1::{
        ingest::{MempoolTransactionRequest, ingest_control_client::IngestControlClient},
        ops::ErrorReason,
        wallet::{self, wallet_query_server},
    },
    wire::{TransactionAbsence, outpoint_from_message},
};
use zinder_runtime::{AuthenticatedChannel, Readiness};

use crate::record_proxy_outcome;
use zinder_store::{
    StreamCursorTokenV1, chain_event_stream_family_from_request, event_stream_start_from_request,
};

type AuthenticatedIngestControlClient = IngestControlClient<AuthenticatedChannel>;

use crate::{
    QueryError, TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputsRequest,
    WalletQueryApi, WalletServingQuery,
    ingest_control::{WALLET_INGEST_CONTROL_REQUEST_TIMEOUT, wallet_ingest_control_request},
};

use super::chain_events::{decode_address_filter, spawn_filtered_stream};
use super::native::{
    TransactionStatusLookup, WalletEndpointMetadata, address_lookup_to_script_hash,
    block_header_by_selector_response, block_id_by_selector_response,
    broadcast_transaction_response, build_chain_view_message, build_full_block_message,
    build_transparent_address_tx_ids_chunk, build_transparent_address_tx_ids_header,
    build_transparent_unspent_output_message, build_transparent_unspent_outputs_header,
    build_wallet_server_info, chain_value_pools_at_tip_response, compact_block_response,
    compact_blocks_in_range_chunks, full_block_response, latest_tree_state_checkpoint_response,
    network_upgrade_activations_response, settled_tip_block_response, subtree_roots_response,
    transaction_response, transparent_address_unspent_outputs_response,
    transparent_outpoint_spend_status_response, tree_state_at_response, visible_tip_block_response,
};
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
        .await;
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
}

impl<QueryApi: WalletQueryApi> WalletQueryGrpcAdapter<QueryApi> {
    /// Creates a gRPC adapter over a wallet query API with the deployment's
    /// `ServerCapabilities` descriptor.
    #[must_use]
    pub fn new(query_api: QueryApi, metadata: WalletEndpointMetadata) -> Self {
        let server_info = build_wallet_server_info(
            metadata,
            query_api.native_endpoint_capabilities(),
            query_api.upstream_node_capabilities(),
        );
        Self {
            query_api,
            server_info,
            traffic_readiness: None,
        }
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

    /// Wraps this adapter in the generated tonic server type.
    #[must_use]
    pub fn into_server(self) -> wallet_query_server::WalletQueryServer<Self>
    where
        Self: wallet_query_server::WalletQuery,
    {
        wallet_query_server::WalletQueryServer::new(self)
            .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES)
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

        let client = self
            .ingest_control_client(INGEST_CONTROL_NOT_COMPOSED)
            .await?;
        mempool_transaction_after_canonical_miss(client, transaction_id, chain_epoch)
            .await
            .map(Response::new)
    }

    async fn compact_blocks_in_range(
        &self,
        request: Request<wallet::CompactBlocksInRangeRequest>,
    ) -> Result<Response<Self::CompactBlocksInRangeStream>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_COMPACT_BLOCK_RANGE_V2)?;
        let request = request.into_inner();
        let block_range = BlockHeightRange::inclusive(
            BlockHeight::new(request.start_height),
            BlockHeight::new(request.end_height),
        );
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);

        let compact_block_range = self
            .query_api
            .compact_blocks_in_range(block_range, at_epoch)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let chunks = compact_blocks_in_range_chunks(compact_block_range).map(Ok);

        Ok(Response::new(Box::pin(stream::iter(chunks))))
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

        Ok(Response::new(Box::pin(full_block_chunks)))
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
            let query_api = self.query_api.clone();
            let (event_sender, event_receiver) = mpsc::channel(16);
            spawn_filtered_stream(
                query_api,
                resume,
                address_filter,
                self.traffic_readiness.clone(),
                event_sender,
            );

            let stream: ChainEventsStream = Box::pin(ReceiverStream::new(event_receiver));
            Ok(Response::new(stream))
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
                client.mempool_events(Request::new(request.into_inner())),
            )
            .await
            .map_err(|_| {
                status_for_reason(
                    ErrorReason::UpstreamUnreachable,
                    "ingest-control mempool event stream establishment timed out",
                )
            })??;
            let stream: MempoolEventsStream = Box::pin(response.into_inner());
            Ok(Response::new(stream))
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
        let header = Ok(build_transparent_unspent_outputs_header(
            response.chain_epoch,
        ));
        let items = response
            .outputs
            .into_iter()
            .map(|output| Ok(build_transparent_unspent_output_message(&output)));
        let messages = stream::once(header).chain(stream::iter(items));

        Ok(Response::new(Box::pin(messages)))
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
            .transparent_address_tx_ids_in_range(typed_request)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let header = Ok(build_transparent_address_tx_ids_header(
            response.chain_epoch,
        ));
        let next_cursor_bytes = response
            .next_cursor
            .map(|cursor| cursor.as_bytes().to_vec())
            .unwrap_or_default();
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
        Ok(Response::new(Box::pin(chunks)))
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
        Ok(Response::new(wallet::ServerInfoResponse {
            info: Some(self.server_info.clone()),
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
            .map(|common| common.network.as_str())
            .unwrap_or_default();
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
