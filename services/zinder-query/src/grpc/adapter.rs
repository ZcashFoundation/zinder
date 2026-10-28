//! Native gRPC adapter for wallet query reads.

use std::{collections::HashMap, num::NonZeroU32, pin::Pin, time::Instant};

use tokio::sync::mpsc;
use tokio_stream::{self as stream, Stream, StreamExt as _, wrappers::ReceiverStream};
use tonic::{Code, Request, Response, Status};
use tonic_types::ErrorDetails;
use zinder_core::wire::{
    decode_rpc_block_hash_hex, decode_rpc_transaction_id_hex, decode_zinder_native_chain_name,
    encode_rpc_transaction_id_hex,
};
use zinder_core::{
    BlockHeight, BlockHeightRange, BlockSelector, ChainEpoch, ChainEpochId,
    MAX_TRANSPARENT_OUTPUTS_PER_REQUEST, Network, RawTransactionBytes, ShieldedProtocol,
    SubtreeRootIndex, SubtreeRootRange, TransactionId, TransparentOutPoint,
};
use zinder_proto::{
    capabilities, status_for_reason, status_with_reason,
    v1::{
        ingest::{MempoolTransactionRequest, ingest_control_client::IngestControlClient},
        ops::ErrorReason,
        wallet::{self, wallet_query_server},
    },
    wire::TransactionAbsence,
};
use zinder_runtime::{AuthenticatedChannel, connect_zinder_grpc};

use crate::record_proxy_outcome;
use zinder_store::{
    ArtifactFamily, StreamCursorTokenV1, chain_event_stream_family_from_request,
    event_stream_start_from_request,
};

type AuthenticatedIngestControlClient = IngestControlClient<AuthenticatedChannel>;

use crate::{
    TransparentAddressTxIdsInRangeRequest, TransparentAddressUnspentOutputsRequest, WalletQuery,
    WalletQueryApi, WalletServingQuery,
    ingest_control::{WALLET_INGEST_CONTROL_REQUEST_TIMEOUT, wallet_ingest_control_request},
};

use super::chain_events::{decode_address_filter, spawn_filtered_stream};
use super::native::{
    TransactionStatusLookup, WalletEndpointMetadata, address_lookup_to_script_hash,
    block_header_by_selector_response, block_id_by_selector_response,
    broadcast_transaction_response, build_chain_view_message, build_compact_block_message,
    build_full_block_message, build_transparent_address_tx_ids_chunk,
    build_transparent_address_tx_ids_header, build_transparent_unspent_output_message,
    build_transparent_unspent_outputs_header, build_wallet_server_info,
    chain_value_pools_at_tip_response, compact_block_response, full_block_response,
    latest_tree_state_checkpoint_response, network_upgrade_activations_response,
    settled_tip_block_response, subtree_roots_response, transaction_response,
    transparent_address_unspent_outputs_response, transparent_outputs_by_outpoint_response,
    transparent_spends_by_outpoint_response, transparent_unspent_outputs_by_outpoint_response,
    transparent_utxo_set_summary_response, tree_state_at_response, visible_tip_block_response,
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
    mut client: AuthenticatedIngestControlClient,
    transaction_id: TransactionId,
    chain_epoch: ChainEpoch,
) -> Result<wallet::TransactionStatusResponse, Status> {
    let started_at = Instant::now();
    let proxy_outcome = client
        .mempool_transaction(wallet_ingest_control_request(MempoolTransactionRequest {
            transaction_id: encode_rpc_transaction_id_hex(transaction_id),
        }))
        .await;
    record_proxy_outcome("mempool_transaction", started_at, &proxy_outcome);

    let status = match proxy_outcome {
        Ok(response) => return Ok(response.into_inner()),
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
                ArtifactFamily::ChainEpoch.wire_label(),
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

/// gRPC adapter for a [`WalletQueryApi`] implementation.
///
/// The adapter owns no live-provider configuration or connection state. Its
/// query was fully composed before this protocol boundary was constructed.
#[derive(Clone, Debug)]
pub struct WalletQueryGrpcAdapter<QueryApi> {
    query_api: QueryApi,
    server_info: wallet::WalletServerInfo,
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
        }
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

impl<ReadApi, Broadcaster> WalletQueryGrpcAdapter<WalletQuery<ReadApi, Broadcaster>>
where
    WalletQuery<ReadApi, Broadcaster>: WalletQueryApi,
{
    /// Connects the temporary generic primary-store composition to its
    /// ingest-control endpoint.
    ///
    /// Release runtimes must admit `AdmittedIngestControl` before constructing
    /// `WalletServingQuery`; this compatibility constructor is not a release
    /// composition path.
    #[doc(hidden)]
    #[must_use]
    pub fn with_ingest_control_proxy(
        mut query_api: WalletQuery<ReadApi, Broadcaster>,
        metadata: WalletEndpointMetadata,
        legacy_ingest_control_proxy_endpoint: String,
    ) -> Self {
        query_api.legacy_ingest_control_proxy_endpoint = Some(legacy_ingest_control_proxy_endpoint);
        Self::new(query_api, metadata)
    }
}

#[tonic::async_trait]
trait IngestControlClientAccess {
    async fn ingest_control_client(
        &self,
    ) -> Result<Option<AuthenticatedIngestControlClient>, Status>;
}

#[tonic::async_trait]
impl<ReadApi, Broadcaster> IngestControlClientAccess for WalletQuery<ReadApi, Broadcaster>
where
    Self: WalletQueryApi,
{
    async fn ingest_control_client(
        &self,
    ) -> Result<Option<AuthenticatedIngestControlClient>, Status> {
        let Some(endpoint) = self.legacy_ingest_control_proxy_endpoint.clone() else {
            return Ok(None);
        };
        let channel = self
            .legacy_ingest_control_channel
            .get_or_try_init(|| async move {
                connect_zinder_grpc(&endpoint, None)
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))
            })
            .await?;
        Ok(Some(IngestControlClient::new(channel.clone())))
    }
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
        let canonical_lookup = transaction_response(&self.query_api, transaction_id, at_epoch_id)
            .await
            .map_err(|error| status_from_query_error(&error))?;

        let chain_epoch = match canonical_lookup {
            TransactionStatusLookup::Located(response) => return Ok(Response::new(*response)),
            TransactionStatusLookup::Absent { chain_epoch } => chain_epoch,
        };
        if at_epoch_id.is_some() {
            // A pinned lookup is a canonical read; it never consults the
            // mempool, and its absence says so.
            return Err(TransactionAbsence::at_epoch(chain_epoch, false)
                .into_status(TRANSACTION_NOT_FOUND_IN_CHAIN));
        }

        let Some(client) =
            IngestControlClientAccess::ingest_control_client(&self.query_api).await?
        else {
            return Err(status_for_reason(
                ErrorReason::DependencyNotConfigured,
                "the live mempool view is not configured, so transaction absence cannot be proven",
            ));
        };
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

        let compact_blocks_in_range = self
            .query_api
            .compact_blocks_in_range(block_range, at_epoch)
            .await
            .map_err(|error| status_from_query_error(&error))?;
        let chain_view = build_chain_view_message(compact_blocks_in_range.chain_epoch);
        let compact_block_chunks =
            compact_blocks_in_range
                .compact_blocks
                .into_iter()
                .map(move |compact_block| {
                    Ok(wallet::CompactBlocksInRangeChunk {
                        chain_view: Some(chain_view.clone()),
                        compact_block: Some(build_compact_block_message(&compact_block)),
                    })
                });

        Ok(Response::new(Box::pin(stream::iter(compact_block_chunks))))
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
                resume.cursor,
                resume.family,
                address_filter,
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
                .ingest_control_client("query composition does not include admitted ingest control")
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
                .ingest_control_client("query composition does not include admitted ingest control")
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

    async fn transparent_mempool_outputs_by_address(
        &self,
        request: Request<wallet::TransparentMempoolOutputsByAddressRequest>,
    ) -> Result<Response<wallet::TransparentMempoolOutputsByAddressResponse>, Status> {
        self.require_endpoint_capability(
            capabilities::WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_BY_ADDRESS_V1,
        )?;
        let started_at = Instant::now();
        let outcome = async {
            let request = request.into_inner();
            let address_script_hash =
                address_lookup_to_script_hash(request.address.clone(), self.server_info_network()?)
                    .map_err(|error| status_from_query_error(&error))?;
            let normalized_request = wallet::TransparentMempoolOutputsByAddressRequest {
                address: Some(typed_script_hash_address_lookup(
                    &address_script_hash.as_bytes(),
                )),
                max_entries: request.max_entries,
            };
            let mut client = self
                .ingest_control_client("query composition does not include admitted ingest control")
                .await?;
            client
                .transparent_mempool_outputs_by_address(wallet_ingest_control_request(
                    normalized_request,
                ))
                .await
        }
        .await;
        record_proxy_outcome(
            "transparent_mempool_outputs_by_address",
            started_at,
            &outcome,
        );
        outcome
    }

    async fn transparent_mempool_spends_by_outpoint(
        &self,
        request: Request<wallet::TransparentMempoolSpendsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentMempoolSpendsByOutpointResponse>, Status> {
        self.require_endpoint_capability(
            capabilities::WALLET_MEMPOOL_TRANSPARENT_SPENDS_BY_OUTPOINT_V1,
        )?;
        let started_at = Instant::now();
        let outcome = async {
            let request = request.into_inner();
            reject_coinbase_sentinels(&request.outpoints)?;
            let mut client = self
                .ingest_control_client("query composition does not include admitted ingest control")
                .await?;
            client
                .transparent_mempool_spends_by_outpoint(wallet_ingest_control_request(request))
                .await
        }
        .await;
        record_proxy_outcome(
            "transparent_mempool_spends_by_outpoint",
            started_at,
            &outcome,
        );
        outcome
    }

    async fn transparent_outputs_by_outpoint(
        &self,
        request: Request<wallet::TransparentOutputsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentOutputsByOutpointResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_TRANSPARENT_OUTPUTS_V1)?;
        let request = request.into_inner();
        reject_coinbase_sentinels(&request.outpoints)?;
        let outpoints = transparent_outpoints_from_request(request.outpoints)?;
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);
        transparent_outputs_by_outpoint_response(&self.query_api, outpoints, at_epoch)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn transparent_spends_by_outpoint(
        &self,
        request: Request<wallet::TransparentSpendsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentSpendsByOutpointResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_TRANSPARENT_SPENDS_V1)?;
        let request = request.into_inner();
        reject_coinbase_sentinels(&request.outpoints)?;
        let outpoints = transparent_outpoints_from_request(request.outpoints)?;
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);
        transparent_spends_by_outpoint_response(&self.query_api, outpoints, at_epoch)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn transparent_unspent_outputs_by_outpoint(
        &self,
        request: Request<wallet::TransparentUnspentOutputsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentUnspentOutputsByOutpointResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_READ_TRANSPARENT_UNSPENT_OUTPUTS_V1)?;
        let request = request.into_inner();
        reject_coinbase_sentinels(&request.outpoints)?;
        let outpoints = transparent_outpoints_from_request(request.outpoints)?;
        let at_epoch = chain_epoch_id_from_request(request.at_epoch_id);
        transparent_unspent_outputs_by_outpoint_response(&self.query_api, outpoints, at_epoch)
            .await
            .map(Response::new)
            .map_err(|error| status_from_query_error(&error))
    }

    async fn transparent_mempool_outputs_by_outpoint(
        &self,
        request: Request<wallet::TransparentMempoolOutputsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentOutputsByOutpointResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_MEMPOOL_TRANSPARENT_OUTPUTS_V1)?;
        let started_at = Instant::now();
        let outcome = async {
            let request_inner = request.get_ref();
            reject_coinbase_sentinels(&request_inner.outpoints)?;
            let mut client = self
                .ingest_control_client("query composition does not include admitted ingest control")
                .await?;
            client
                .transparent_mempool_outputs_by_outpoint(wallet_ingest_control_request(
                    request.into_inner(),
                ))
                .await
        }
        .await;
        record_proxy_outcome(
            "transparent_mempool_outputs_by_outpoint",
            started_at,
            &outcome,
        );
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

    async fn transparent_address_balance(
        &self,
        request: Request<wallet::TransparentAddressBalanceRequest>,
    ) -> Result<Response<wallet::TransparentAddressBalanceResponse>, Status> {
        self.require_endpoint_capability(capabilities::WALLET_ADDRESS_TRANSPARENT_BALANCE_V1)?;
        let started_at = Instant::now();
        let outcome = self.compute_transparent_address_balance(request).await;
        record_proxy_outcome("transparent_address_balance", started_at, &outcome);
        outcome
    }

    async fn transparent_utxo_set_summary(
        &self,
        request: Request<wallet::TransparentUtxoSetSummaryRequest>,
    ) -> Result<Response<wallet::TransparentUtxoSetSummaryResponse>, Status> {
        self.require_endpoint_capability(
            capabilities::WALLET_READ_TRANSPARENT_UTXO_SET_SUMMARY_V1,
        )?;
        transparent_utxo_set_summary_response(
            &self.query_api,
            chain_epoch_id_from_request(request.into_inner().at_epoch_id),
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
        descending: request.descending,
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

    /// Computes the transparent-address balance: the canonical confirmed total
    /// summed in-process plus the live mempool overlay.
    ///
    /// The confirmed total, address cap, and chain-epoch pin are owned by
    /// [`WalletQueryApi::transparent_address_balance`]. The signed
    /// `unconfirmed_delta_zat` overlay is composed here from the live mempool
    /// surfaces reached through the temporary generic query's legacy
    /// ingest-control seam.
    async fn compute_transparent_address_balance(
        &self,
        request: Request<wallet::TransparentAddressBalanceRequest>,
    ) -> Result<Response<wallet::TransparentAddressBalanceResponse>, Status>
    where
        QueryApi: Clone + WalletQueryApi + IngestControlClientAccess + Send + Sync + 'static,
    {
        let request = request.into_inner();
        if request.addresses.is_empty() {
            return Err(Status::invalid_argument("addresses list must not be empty"));
        }
        let network = self.server_info_network()?;
        let at_epoch_id = chain_epoch_id_from_request(request.at_epoch_id);
        let mut script_hashes = Vec::with_capacity(request.addresses.len());
        for address_lookup in request.addresses {
            let address_script_hash = address_lookup_to_script_hash(Some(address_lookup), network)
                .map_err(|error| status_from_query_error(&error))?;
            script_hashes.push(address_script_hash);
        }

        let confirmed = self
            .query_api
            .transparent_address_balance(script_hashes.clone(), at_epoch_id)
            .await
            .map_err(|error| status_from_query_error(&error))?;

        let unconfirmed_delta_zat = self
            .mempool_balance_overlay(&script_hashes, confirmed.chain_epoch.id)
            .await?;

        Ok(Response::new(wallet::TransparentAddressBalanceResponse {
            chain_view: Some(build_chain_view_message(confirmed.chain_epoch)),
            confirmed_zat: confirmed.confirmed_zat,
            unconfirmed_delta_zat,
            address_count: confirmed.address_count,
        }))
    }

    /// Sums the signed mempool delta for the requested addresses.
    ///
    /// Adds pending inflows (mempool outputs paid to the addresses) and
    /// subtracts pending outflows (mempool spends of the addresses' confirmed
    /// unspent set), both read from the live mempool index. This composite
    /// remains only for the temporary generic primary-store seam: the release
    /// native endpoint omits transparent balance until P2b can supply one
    /// coherent canonical-and-mempool snapshot.
    async fn mempool_balance_overlay(
        &self,
        script_hashes: &[zinder_core::TransparentAddressScriptHash],
        chain_epoch_id: ChainEpochId,
    ) -> Result<i64, Status>
    where
        QueryApi: Clone + WalletQueryApi + IngestControlClientAccess + Send + Sync + 'static,
    {
        let Some(mut client) =
            IngestControlClientAccess::ingest_control_client(&self.query_api).await?
        else {
            return Ok(0);
        };

        let mut unconfirmed_delta_zat: i64 = 0;
        let mut spendable_value_by_outpoint: HashMap<(String, u32), u64> = HashMap::new();
        for address_script_hash in script_hashes {
            let mempool_outputs = client
                .transparent_mempool_outputs_by_address(wallet_ingest_control_request(
                    wallet::TransparentMempoolOutputsByAddressRequest {
                        address: Some(typed_script_hash_address_lookup(
                            &address_script_hash.as_bytes(),
                        )),
                        max_entries: None,
                    },
                ))
                .await?
                .into_inner();
            for output in &mempool_outputs.outputs {
                unconfirmed_delta_zat =
                    unconfirmed_delta_zat.saturating_add(value_zat_to_signed(output.value_zat)?);
            }

            let unspent_outputs = self
                .query_api
                .transparent_address_unspent_outputs(
                    TransparentAddressUnspentOutputsRequest {
                        address_script_hash: *address_script_hash,
                        start_height: BlockHeight::new(0),
                    },
                    Some(chain_epoch_id),
                )
                .await
                .map_err(|error| status_from_query_error(&error))?;
            for output in &unspent_outputs.outputs {
                spendable_value_by_outpoint.insert(
                    (
                        encode_rpc_transaction_id_hex(output.outpoint.transaction_id),
                        output.outpoint.output_index,
                    ),
                    output.value_zat,
                );
            }
        }

        let pending_outflow_zat =
            mempool_pending_outflow_zat(&mut client, &spendable_value_by_outpoint).await?;
        Ok(unconfirmed_delta_zat.saturating_sub(pending_outflow_zat))
    }
}

/// Sums the value of confirmed unspent outputs that the live mempool already
/// spends, batched at the per-request outpoint cap.
async fn mempool_pending_outflow_zat(
    client: &mut AuthenticatedIngestControlClient,
    spendable_value_by_outpoint: &HashMap<(String, u32), u64>,
) -> Result<i64, Status> {
    let spendable_outpoints = spendable_value_by_outpoint
        .keys()
        .map(|(transaction_id, output_index)| wallet::OutPoint {
            transaction_id: transaction_id.clone(),
            output_index: *output_index,
        })
        .collect::<Vec<_>>();
    let mut pending_outflow_zat: i64 = 0;
    for outpoint_batch in spendable_outpoints.chunks(MAX_TRANSPARENT_OUTPUTS_PER_REQUEST) {
        let spends = client
            .transparent_mempool_spends_by_outpoint(wallet_ingest_control_request(
                wallet::TransparentMempoolSpendsByOutpointRequest {
                    outpoints: outpoint_batch.to_vec(),
                },
            ))
            .await?
            .into_inner();
        for spend in spends.spends {
            let spent_outpoint = spend.spent_outpoint.ok_or_else(|| {
                Status::data_loss(
                    "TransparentMempoolSpend.spent_outpoint missing in IngestControl response",
                )
            })?;
            if let Some(value_zat) = spendable_value_by_outpoint
                .get(&(spent_outpoint.transaction_id, spent_outpoint.output_index))
            {
                pending_outflow_zat =
                    pending_outflow_zat.saturating_add(value_zat_to_signed(*value_zat)?);
            }
        }
    }
    Ok(pending_outflow_zat)
}

/// Builds an `AddressLookup` whose only populated arm is the typed
/// script-hash bytes.
///
/// The native adapter parses public `AddressLookup` selectors at the
/// public boundary; this helper produces the normalized shape the
/// private ingest-control surface accepts.
fn typed_script_hash_address_lookup(script_hash_bytes: &[u8]) -> wallet::AddressLookup {
    wallet::AddressLookup {
        selector: Some(wallet::address_lookup::Selector::ScriptHash(
            script_hash_bytes.to_vec(),
        )),
    }
}

fn transaction_id_from_request(transaction_id_rpc_hex: &str) -> Result<TransactionId, Status> {
    decode_rpc_transaction_id_hex(transaction_id_rpc_hex)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

/// Rejects the coinbase sentinel outpoint with `INVALID_ARGUMENT` and a
/// `BadRequest`-shaped diagnostic naming the offending request index.
///
fn reject_coinbase_sentinels(outpoints: &[wallet::OutPoint]) -> Result<(), Status> {
    let sentinel = TransparentOutPoint::COINBASE_SENTINEL;
    let sentinel_transaction_id_rpc_hex = encode_rpc_transaction_id_hex(sentinel.transaction_id);
    for (request_index, outpoint) in outpoints.iter().enumerate() {
        if outpoint.transaction_id == sentinel_transaction_id_rpc_hex
            && outpoint.output_index == sentinel.output_index
        {
            return Err(Status::invalid_argument(format!(
                "outpoints[{request_index}] is the coinbase sentinel \
                 (transaction_id is the all-zero RPC-form hash, output_index == 0xFFFFFFFF); \
                 filter coinbase inputs at the request boundary",
            )));
        }
    }
    Ok(())
}

/// Translates a wire outpoint list into typed `TransparentOutPoint`s,
/// silently truncating to [`MAX_TRANSPARENT_OUTPUTS_PER_REQUEST`].
fn transparent_outpoints_from_request(
    mut outpoints: Vec<wallet::OutPoint>,
) -> Result<Vec<TransparentOutPoint>, Status> {
    outpoints.truncate(MAX_TRANSPARENT_OUTPUTS_PER_REQUEST);
    outpoints
        .into_iter()
        .map(|outpoint| {
            let transaction_id = transaction_id_from_request(&outpoint.transaction_id)?;
            Ok(TransparentOutPoint::new(
                transaction_id,
                outpoint.output_index,
            ))
        })
        .collect()
}

fn chain_epoch_id_from_request(at_epoch_id: Option<u64>) -> Option<ChainEpochId> {
    at_epoch_id.map(ChainEpochId::new)
}

/// Converts a wire `u64` Zatoshi value to the signed delta-accumulator width.
///
/// Zcash's hardcoded supply cap (`MAX_MONEY = 21,000,000 * 10^8` zat) fits
/// well inside `i64::MAX`, so a `u64` value that does not fit is upstream data
/// corruption and surfaces as `data_loss` rather than silent saturation.
fn value_zat_to_signed(value_zat: u64) -> Result<i64, Status> {
    i64::try_from(value_zat).map_err(|_| {
        Status::data_loss(format!(
            "mempool overlay value_zat {value_zat} exceeds i64::MAX"
        ))
    })
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
