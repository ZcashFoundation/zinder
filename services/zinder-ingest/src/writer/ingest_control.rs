//! Version-1 `IngestControl` composition over the canonical writer channel.
//!
//! This adapter never opens a canonical primary or a materialized-view store. Exact
//! writer status and tip-event replay are serviced by the follower that owns
//! the admitted `RocksDbCanonicalStore`; resumable mempool history and cursor
//! retention are also owned by that canonical `RocksDB` event log. Only the
//! current low-latency mempool index is process-local, and it remains hidden
//! until its source snapshot completes.

use std::{num::NonZeroU32, pin::Pin, sync::Arc, time::Duration};

use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, service::interceptor::InterceptedService};
use zinder_core::{
    ChainEpoch, ChainValuePools, MAX_TRANSPARENT_OUTPUTS_PER_REQUEST, Network, TransactionId,
    TransparentAddressScriptHash, TransparentOutPoint,
    wire::{decode_rpc_transaction_id_hex, encode_zinder_native_chain_name},
};
use zinder_proto::{
    capabilities::{CapabilitySurface, capabilities_for_surface},
    v1::{
        ingest::{
            MempoolTransactionRequest, ServerInfoRequest, ServerInfoResponse, WriterPhase,
            WriterStatusRequest, WriterStatusResponse,
            ingest_control_server::{IngestControl, IngestControlServer},
        },
        ops, wallet,
    },
};
use zinder_runtime::{
    BearerToken, BearerTokenServerInterceptor, Readiness, ReadinessCause, ReadinessReport,
};
use zinder_source::{NodeCapability, NodeSource, SourceError};
use zinder_store::{
    CanonicalEventKind, block_tip_message, chain_epoch_message, chain_view_message,
    event_stream_start_from_message, mempool_entry_message, transparent_mempool_output_message,
    transparent_mempool_spend_message, transparent_output_entry_message,
};

use crate::{
    CanonicalControlHandle, LiveMempoolOwner,
    writer::control::{CanonicalIngestEvent, CanonicalWriterSnapshot},
};

type IngestControlStream<Message> = Pin<Box<dyn Stream<Item = Result<Message, Status>> + Send>>;
type VisibleChainEventsStream = IngestControlStream<wallet::ChainEventEnvelope>;
type MempoolEventsStream = IngestControlStream<wallet::MempoolEventEnvelope>;

const DEFAULT_MEMPOOL_SNAPSHOT_PAGE_SIZE: u32 = 256;
/// Hard response bound shared by the private writer control APIs.
pub const MAX_MEMPOOL_SNAPSHOT_PAGE_SIZE: u32 = 1_024;
const DEFAULT_TRANSPARENT_MEMPOOL_POINT_LOOKUP_MAX_ENTRIES: u32 = 256;
const MAX_TRANSPARENT_MEMPOOL_POINT_LOOKUP_MAX_ENTRIES: u32 = 1_024;
const CANONICAL_EVENT_PAGE_SIZE: NonZeroU32 = NonZeroU32::MIN.saturating_add(63);
const EVENT_STREAM_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Version-1 adapter for query and compatibility clients that consume the
/// ingest writer's control plane.
#[derive(Clone)]
pub struct CanonicalIngestControlGrpcAdapter {
    network: Network,
    canonical: CanonicalControlHandle,
    mempool: LiveMempoolOwner,
    node_source: Arc<dyn NodeSource>,
    bearer_token: Option<BearerToken>,
    readiness: Readiness,
}

impl CanonicalIngestControlGrpcAdapter {
    /// Binds one canonical writer command channel, one live mempool owner,
    /// and the source handle already owned by the ingest runtime.
    #[must_use]
    pub fn new(
        network: Network,
        canonical: CanonicalControlHandle,
        mempool: LiveMempoolOwner,
        node_source: Arc<dyn NodeSource>,
        readiness: Readiness,
    ) -> Self {
        Self {
            network,
            canonical,
            mempool,
            node_source,
            bearer_token: None,
            readiness,
        }
    }

    /// Applies the optional private-control bearer token to every RPC.
    #[must_use]
    pub fn with_bearer_token(mut self, bearer_token: Option<BearerToken>) -> Self {
        self.bearer_token = bearer_token;
        self
    }

    /// Builds the authenticated tonic service for the same listener as
    /// `CanonicalControl`.
    #[must_use]
    pub fn into_server(
        self,
    ) -> InterceptedService<IngestControlServer<Self>, BearerTokenServerInterceptor> {
        let interceptor = BearerTokenServerInterceptor::new(self.bearer_token.clone());
        let server = IngestControlServer::new(self)
            .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES);
        InterceptedService::new(server, interceptor)
    }

    fn advertised_capabilities(&self) -> Vec<String> {
        let chain_value_pools_supported = self
            .node_source
            .capabilities()
            .supports(NodeCapability::ChainValuePools);
        capabilities_for_surface(CapabilitySurface::Ingest)
            .filter(|spec| spec.policy.ingest_satisfied(chain_value_pools_supported))
            .map(|spec| spec.string.to_owned())
            .collect()
    }

    async fn current_chain_epoch(&self) -> Result<CanonicalWriterSnapshot, Status> {
        self.canonical.chain_epoch().await
    }
}

#[tonic::async_trait]
impl IngestControl for CanonicalIngestControlGrpcAdapter {
    type VisibleChainEventsStream = VisibleChainEventsStream;
    type MempoolEventsStream = MempoolEventsStream;

    async fn server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        Ok(Response::new(ServerInfoResponse {
            server_info: Some(ops::ServerInfo {
                network: encode_zinder_native_chain_name(self.network).to_owned(),
                service_name: env!("CARGO_PKG_NAME").to_owned(),
                service_version: env!("CARGO_PKG_VERSION").to_owned(),
                build_git_commit: zinder_runtime::BUILD_GIT_COMMIT.to_owned(),
                capabilities: self.advertised_capabilities(),
                contract_revision: zinder_proto::CONTRACT_REVISION,
                materialized_view_preset: String::new(),
                materialized_view_identities: Vec::new(),
            }),
        }))
    }

    async fn writer_status(
        &self,
        _request: Request<WriterStatusRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        let readiness_report = self.readiness.report();
        // A parked writer no longer owns a live fence, but readers still need
        // its typed halt rather than an unavailable control channel.
        let chain_epoch = if matches!(
            readiness_report.cause,
            ReadinessCause::ReorgWindowExceeded { .. }
        ) {
            None
        } else {
            Some(self.current_chain_epoch().await?.chain_epoch)
        };
        Ok(Response::new(writer_status_response(
            self.network,
            chain_epoch,
            &readiness_report,
        )))
    }

    async fn visible_chain_events(
        &self,
        request: Request<wallet::EventStreamStart>,
    ) -> Result<Response<Self::VisibleChainEventsStream>, Status> {
        let after_cursor = match request
            .into_inner()
            .position
            .ok_or_else(|| Status::invalid_argument("event stream start position is required"))?
        {
            wallet::event_stream_start::Position::AfterCursor(cursor) => Some(cursor),
            wallet::event_stream_start::Position::EarliestRetained(_) => None,
            wallet::event_stream_start::Position::LiveTail(_) => {
                let snapshot = self.current_chain_epoch().await?;
                (snapshot.fence.chain_event_sequence() != 0)
                    .then(|| {
                        zinder_store::CanonicalEventCursor::at(
                            snapshot.fence.chain_event_sequence(),
                        )
                        .map(|cursor| cursor.as_bytes().to_vec())
                        .map_err(|_| {
                            Status::internal("canonical writer returned an invalid event sequence")
                        })
                    })
                    .transpose()?
            }
        };
        Ok(Response::new(spawn_visible_chain_event_stream(
            self.canonical.clone(),
            after_cursor,
        )))
    }

    async fn mempool_snapshot(
        &self,
        request: Request<wallet::MempoolSnapshotRequest>,
    ) -> Result<Response<wallet::MempoolSnapshotResponse>, Status> {
        let request = request.into_inner();
        // Capture the canonical fence before touching the mempool page. A tip
        // committed during or after the page read is therefore observable as
        // a strictly newer chain event by tip-coherent consumers.
        let snapshot = self.current_chain_epoch().await?;
        let page = self
            .mempool
            .snapshot_page(
                &self.canonical,
                snapshot.chain_epoch,
                bounded_snapshot_page_size(request.max_entries),
                request.from_cursor,
            )
            .await?;
        Ok(Response::new(wallet::MempoolSnapshotResponse {
            chain_view: Some(chain_view_message(snapshot.chain_epoch)),
            events_resume_cursor: page.events_resume_cursor,
            snapshot_age_millis: page.snapshot_age_millis,
            entries: page
                .entries
                .iter()
                .map(|entry| mempool_entry_message(entry.as_ref()))
                .collect(),
            next_cursor: page.next_cursor,
            source_tip: Some(block_tip_message(
                page.source_tip.height,
                page.source_tip.hash,
            )),
        }))
    }

    async fn mempool_transaction(
        &self,
        request: Request<MempoolTransactionRequest>,
    ) -> Result<Response<wallet::TransactionStatusResponse>, Status> {
        let transaction_id = transaction_id_from_rpc_hex(&request.into_inner().transaction_id)?;
        let snapshot = self.current_chain_epoch().await?;
        let entry = self
            .mempool
            .entry_for(snapshot.chain_epoch, transaction_id)
            .await?
            .ok_or_else(|| {
                Status::not_found("transaction is not visible in the live mempool index")
            })?;
        Ok(Response::new(wallet::TransactionStatusResponse {
            chain_view: Some(chain_view_message(snapshot.chain_epoch)),
            location: Some(wallet::TransactionLocation {
                location: Some(wallet::transaction_location::Location::InMempool(
                    zinder_proto::wire::mempool_entry_message(&entry),
                )),
            }),
        }))
    }

    async fn mempool_events(
        &self,
        request: Request<wallet::MempoolEventsRequest>,
    ) -> Result<Response<Self::MempoolEventsStream>, Status> {
        let request = request.into_inner();
        let start = event_stream_start_from_message(request.start)
            .ok_or_else(|| Status::invalid_argument("event stream start is required"))?;
        let after_cursor = self
            .mempool
            .resolve_event_start(&self.canonical, start)
            .await?;
        Ok(Response::new(spawn_mempool_event_stream(
            self.mempool.clone(),
            self.canonical.clone(),
            after_cursor,
        )))
    }

    async fn transparent_mempool_outputs_by_address(
        &self,
        request: Request<wallet::TransparentMempoolOutputsByAddressRequest>,
    ) -> Result<Response<wallet::TransparentMempoolOutputsByAddressResponse>, Status> {
        let request = request.into_inner();
        let address_script_hash = script_hash_from_lookup(request.address)?;
        let snapshot = self.current_chain_epoch().await?;
        let outputs = self
            .mempool
            .transparent_outputs_by_address(
                snapshot.chain_epoch,
                address_script_hash,
                bounded_point_lookup_max_entries(request.max_entries),
            )
            .await?
            .iter()
            .map(transparent_mempool_output_message)
            .collect();
        Ok(Response::new(
            wallet::TransparentMempoolOutputsByAddressResponse {
                chain_view: Some(chain_view_message(snapshot.chain_epoch)),
                outputs,
            },
        ))
    }

    async fn transparent_mempool_spends_by_outpoint(
        &self,
        request: Request<wallet::TransparentMempoolSpendsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentMempoolSpendsByOutpointResponse>, Status> {
        let mut outpoints = request.into_inner().outpoints;
        outpoints.truncate(MAX_TRANSPARENT_OUTPUTS_PER_REQUEST);
        let outpoints = outpoints
            .into_iter()
            .map(|outpoint| outpoint_from_request_message(Some(outpoint)))
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = self.current_chain_epoch().await?;
        let spends = self
            .mempool
            .transparent_spends_by_outpoint(snapshot.chain_epoch, outpoints)
            .await?
            .iter()
            .map(transparent_mempool_spend_message)
            .collect();
        Ok(Response::new(
            wallet::TransparentMempoolSpendsByOutpointResponse {
                chain_view: Some(chain_view_message(snapshot.chain_epoch)),
                spends,
            },
        ))
    }

    async fn transparent_mempool_outputs_by_outpoint(
        &self,
        request: Request<wallet::TransparentMempoolOutputsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentOutputsByOutpointResponse>, Status> {
        let mut outpoints = request.into_inner().outpoints;
        outpoints.truncate(MAX_TRANSPARENT_OUTPUTS_PER_REQUEST);
        let outpoints = outpoints
            .into_iter()
            .map(|outpoint| outpoint_from_request_message(Some(outpoint)))
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = self.current_chain_epoch().await?;
        let entries = self
            .mempool
            .transparent_outputs_by_outpoints(snapshot.chain_epoch, &outpoints)
            .await?
            .into_iter()
            .map(transparent_output_entry_message)
            .collect();
        Ok(Response::new(
            wallet::TransparentOutputsByOutpointResponse {
                chain_view: Some(chain_view_message(snapshot.chain_epoch)),
                entries,
            },
        ))
    }

    async fn chain_value_pools_at_tip(
        &self,
        _request: Request<wallet::ChainValuePoolsAtTipRequest>,
    ) -> Result<Response<wallet::ChainValuePoolsAtTipResponse>, Status> {
        if !self
            .node_source
            .capabilities()
            .supports(NodeCapability::ChainValuePools)
        {
            return Err(Status::unimplemented(
                "upstream node does not advertise chain_value_pools",
            ));
        }
        let snapshot = self.current_chain_epoch().await?;
        let value_pools = self
            .node_source
            .fetch_chain_value_pools_at_tip()
            .await
            .map_err(|error| status_from_source_error(&error))?;
        Ok(Response::new(chain_value_pools_response(
            snapshot.chain_epoch,
            value_pools,
        )))
    }
}

fn spawn_visible_chain_event_stream(
    canonical: CanonicalControlHandle,
    mut after_cursor: Option<Vec<u8>>,
) -> VisibleChainEventsStream {
    let (event_sender, event_receiver) = mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            match canonical
                .ingest_event_page(after_cursor.clone(), CANONICAL_EVENT_PAGE_SIZE)
                .await
            {
                Ok(page) if page.events.is_empty() => {
                    tokio::select! {
                        () = tokio::time::sleep(EVENT_STREAM_IDLE_POLL_INTERVAL) => {}
                        () = event_sender.closed() => return,
                    }
                }
                Ok(page) => {
                    for event in page.events {
                        after_cursor = Some(event.cursor.as_bytes().to_vec());
                        let message = canonical_event_message(event);
                        if event_sender.send(message).await.is_err() {
                            return;
                        }
                    }
                }
                Err(status) => {
                    let _send_result = event_sender.send(Err(status)).await;
                    return;
                }
            }
        }
    });
    Box::pin(ReceiverStream::new(event_receiver))
}

fn spawn_mempool_event_stream(
    mempool: LiveMempoolOwner,
    canonical: CanonicalControlHandle,
    mut after_cursor: Option<zinder_store::StreamCursorTokenV1>,
) -> MempoolEventsStream {
    let (event_sender, event_receiver) = mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            match mempool.event_page(&canonical, after_cursor.clone()).await {
                Ok(events) if events.is_empty() => {
                    tokio::select! {
                        () = tokio::time::sleep(EVENT_STREAM_IDLE_POLL_INTERVAL) => {}
                        () = event_sender.closed() => return,
                    }
                }
                Ok(events) => {
                    for event in events {
                        after_cursor = Some(zinder_store::StreamCursorTokenV1::from_bytes(
                            event.cursor.clone(),
                        ));
                        if event_sender.send(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(status) => {
                    let _send_result = event_sender.send(Err(status)).await;
                    return;
                }
            }
        }
    });
    Box::pin(ReceiverStream::new(event_receiver))
}

fn canonical_event_message(
    event: CanonicalIngestEvent,
) -> Result<wallet::ChainEventEnvelope, Status> {
    let event_body = match event.kind {
        CanonicalEventKind::Committed => {
            wallet::chain_event_envelope::Event::ChainCommitted(wallet::ChainCommitted {
                committed: Some(chain_epoch_committed_message(
                    event.resulting_epoch,
                    event.committed_range,
                )),
            })
        }
        CanonicalEventKind::Reorged => {
            let previous_epoch = event.previous_epoch.ok_or_else(|| {
                Status::failed_precondition("retained reorg event has no previous canonical epoch")
            })?;
            let reverted_range = event.reverted_range.ok_or_else(|| {
                Status::failed_precondition("retained reorg event has no reverted range")
            })?;
            wallet::chain_event_envelope::Event::ChainReorged(wallet::ChainReorged {
                reverted: Some(wallet::ChainRangeReverted {
                    chain_epoch: Some(chain_epoch_message(previous_epoch)),
                    start_height: reverted_range.start.value(),
                    end_height: reverted_range.end.value(),
                }),
                committed: Some(chain_epoch_committed_message(
                    event.resulting_epoch,
                    event.committed_range,
                )),
            })
        }
    };
    Ok(wallet::ChainEventEnvelope {
        cursor: event.cursor.as_bytes().to_vec(),
        event_sequence: event.cursor.event_sequence(),
        chain_view: Some(chain_view_message(event.resulting_epoch)),
        event: Some(event_body),
    })
}

fn chain_epoch_committed_message(
    chain_epoch: ChainEpoch,
    range: zinder_core::BlockHeightRange,
) -> wallet::ChainEpochCommitted {
    wallet::ChainEpochCommitted {
        chain_epoch: Some(chain_epoch_message(chain_epoch)),
        start_height: range.start.value(),
        end_height: range.end.value(),
    }
}

fn writer_status_response(
    network: Network,
    chain_epoch: Option<ChainEpoch>,
    readiness_report: &ReadinessReport,
) -> WriterStatusResponse {
    let upstream_tip = upstream_tip_from_readiness(readiness_report);
    let gap_blocks = upstream_tip
        .as_ref()
        .and_then(|upstream_tip| upstream_tip.committed_height)
        .zip(chain_epoch)
        .map(|(upstream_height, chain_epoch)| {
            upstream_height.saturating_sub(chain_epoch.visible_tip_height.value())
        });
    let chain_view = chain_epoch.map(|chain_epoch| {
        let mut chain_view = chain_view_message(chain_epoch);
        chain_view.upstream_tip = upstream_tip;
        chain_view
    });
    let upstream_not_ready =
        if let ReadinessCause::UpstreamNotReady(detail) = &readiness_report.cause {
            Some(ops::UpstreamNotReadyDetail::from(detail))
        } else {
            None
        };
    let reorg_window_exceeded =
        if let ReadinessCause::ReorgWindowExceeded { depth, configured } = readiness_report.cause {
            Some(ops::ReorgWindowExceededDetail { depth, configured })
        } else {
            None
        };
    WriterStatusResponse {
        chain_view,
        network_name: encode_zinder_native_chain_name(network).to_owned(),
        phase: readiness_report
            .phase
            .map_or(WriterPhase::Unspecified, WriterPhase::from)
            .into(),
        gap_blocks,
        upstream_not_ready,
        reorg_window_exceeded,
    }
}

fn upstream_tip_from_readiness(readiness_report: &ReadinessReport) -> Option<wallet::UpstreamTip> {
    if let ReadinessCause::UpstreamNotReady(detail) = &readiness_report.cause {
        (detail.upstream_committed_height.is_some() || detail.upstream_estimated_height.is_some())
            .then_some(wallet::UpstreamTip {
                committed_height: detail.upstream_committed_height,
                estimated_height: detail.upstream_estimated_height,
            })
    } else {
        readiness_report
            .target_height
            .map(|committed_height| wallet::UpstreamTip {
                committed_height: Some(committed_height),
                estimated_height: None,
            })
    }
}

fn chain_value_pools_response(
    chain_epoch: ChainEpoch,
    value_pools: ChainValuePools,
) -> wallet::ChainValuePoolsAtTipResponse {
    let source_tip = value_pools.source_tip;
    wallet::ChainValuePoolsAtTipResponse {
        chain_view: Some(chain_view_message(chain_epoch)),
        pools: value_pools
            .pools
            .into_iter()
            .map(|pool| wallet::ChainValuePool {
                id: pool.id,
                monitored: pool.monitored,
                chain_value_zat: pool.chain_value_zat,
            })
            .collect(),
        source_tip: Some(block_tip_message(source_tip.height, source_tip.hash)),
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Only source errors with public gRPC semantics need distinct status codes."
)]
fn status_from_source_error(error: &SourceError) -> Status {
    match error {
        SourceError::NodeUnavailable { reason } => Status::unavailable(reason.clone()),
        SourceError::NodeCapabilityMissing { capability } => {
            Status::failed_precondition(format!("upstream node is missing {capability}"))
        }
        SourceError::SourceProtocolMismatch { reason } => Status::data_loss(*reason),
        _ => Status::internal("upstream source operation failed"),
    }
}

const fn bounded_snapshot_page_size(requested: u32) -> u32 {
    if requested == 0 {
        DEFAULT_MEMPOOL_SNAPSHOT_PAGE_SIZE
    } else if requested > MAX_MEMPOOL_SNAPSHOT_PAGE_SIZE {
        MAX_MEMPOOL_SNAPSHOT_PAGE_SIZE
    } else {
        requested
    }
}

const fn bounded_point_lookup_max_entries(requested: Option<u32>) -> u32 {
    let Some(requested) = requested else {
        return DEFAULT_TRANSPARENT_MEMPOOL_POINT_LOOKUP_MAX_ENTRIES;
    };
    if requested == 0 {
        DEFAULT_TRANSPARENT_MEMPOOL_POINT_LOOKUP_MAX_ENTRIES
    } else if requested > MAX_TRANSPARENT_MEMPOOL_POINT_LOOKUP_MAX_ENTRIES {
        MAX_TRANSPARENT_MEMPOOL_POINT_LOOKUP_MAX_ENTRIES
    } else {
        requested
    }
}

fn script_hash_from_lookup(
    address: Option<wallet::AddressLookup>,
) -> Result<TransparentAddressScriptHash, Status> {
    let lookup = address.ok_or_else(|| Status::invalid_argument("address selector is required"))?;
    let selector = lookup
        .selector
        .ok_or_else(|| Status::invalid_argument("address selector is empty"))?;
    match selector {
        wallet::address_lookup::Selector::ScriptHash(bytes) => {
            let hash_bytes: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("address.script_hash must be 32 bytes"))?;
            Ok(TransparentAddressScriptHash::from_bytes(hash_bytes))
        }
        wallet::address_lookup::Selector::Address(_) => Err(Status::unimplemented(
            "version-1 ingest control accepts only the script_hash address selector",
        )),
    }
}

fn transaction_id_from_rpc_hex(rpc_hex: &str) -> Result<TransactionId, Status> {
    decode_rpc_transaction_id_hex(rpc_hex)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn outpoint_from_request_message(
    message: Option<wallet::OutPoint>,
) -> Result<TransparentOutPoint, Status> {
    let message = message.ok_or_else(|| Status::invalid_argument("outpoint is required"))?;
    Ok(TransparentOutPoint::new(
        transaction_id_from_rpc_hex(&message.transaction_id)?,
        message.output_index,
    ))
}

#[cfg(test)]
mod tests {
    use std::{fs, num::NonZeroU32, str::FromStr as _, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use tokio::net::TcpListener;
    use tokio_stream::{StreamExt as _, wrappers::TcpListenerStream};
    use tokio_util::sync::CancellationToken;
    use tonic::{Code, Request, transport::Server};
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpoch,
        ChainEpochId, ChainTipMetadata, ChainValuePool, ChainValuePools, CompactTransactionData,
        CompactTransparentInput, CompactTransparentOutput, MempoolEntry, MempoolObservation,
        RawTransactionBytes, ShieldedProtocol, SubtreeRootIndex, TransactionId,
        UnixTimestampMillis, wire::encode_rpc_transaction_id_hex,
    };
    use zinder_proto::{
        capabilities::INGEST_CONTROL_VISIBLE_CHAIN_EVENTS_V1,
        v1::{
            ingest::{
                CanonicalWriterStatusRequest, MempoolTransactionRequest, ServerInfoRequest,
                WriterPhase, WriterStatusRequest, canonical_control_client::CanonicalControlClient,
                ingest_control_client::IngestControlClient, ingest_control_server::IngestControl,
            },
            wallet::{
                AddressLookup, ChainEventEnvelope, MempoolEventEnvelope, MempoolEventsRequest,
                MempoolSnapshotRequest, OutPoint, TransparentMempoolOutputsByAddressRequest,
                TransparentMempoolOutputsByOutpointRequest,
                TransparentMempoolSpendsByOutpointRequest, address_lookup, chain_event_envelope,
                mempool_event_envelope, transaction_location,
            },
        },
    };
    use zinder_runtime::{BearerToken, IngestPhase, Readiness, ReadinessState};
    use zinder_source::{
        NodeAuth, NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceError,
        SourceSubtreeRoots, ZebraJsonRpcSource,
    };
    use zinder_store::{
        CanonicalEventCursor, CanonicalEventKind, EventStreamStartPosition, MempoolEvent,
        MempoolEventRetentionConfig, RocksDbResourceBudget, event_stream_start_message,
    };

    use crate::{
        CanonicalCheckpointStagingRoot, CanonicalControlGrpcAdapter, LiveMempoolOwner,
        writer::control::{
            apply_canonical_control_command, canonical_control_channel,
            test_support::published_fixture_store,
        },
    };

    use super::{CanonicalIngestControlGrpcAdapter, canonical_event_message};

    #[test]
    fn tip_reorg_wire_event_names_the_exact_previous_epoch()
    -> Result<(), Box<dyn std::error::Error>> {
        let previous_epoch = fixture_epoch(7, 100, 0x71);
        let resulting_epoch = fixture_epoch(8, 101, 0x81);
        let event = crate::writer::control::CanonicalIngestEvent {
            cursor: CanonicalEventCursor::at(23)?,
            kind: CanonicalEventKind::Reorged,
            resulting_epoch,
            previous_epoch: Some(previous_epoch),
            reverted_range: Some(BlockHeightRange::inclusive(
                BlockHeight::new(99),
                BlockHeight::new(100),
            )),
            committed_range: BlockHeightRange::inclusive(
                BlockHeight::new(99),
                BlockHeight::new(101),
            ),
        };

        let envelope = canonical_event_message(event)?;
        let Some(chain_event_envelope::Event::ChainReorged(reorg)) = envelope.event else {
            return Err("expected a reorg envelope".into());
        };
        assert_eq!(
            reorg
                .reverted
                .and_then(|reverted| reverted.chain_epoch)
                .map(|epoch| epoch.chain_epoch_id),
            Some(previous_epoch.id.value()),
            "the reverted range must retain the epoch that was displaced"
        );
        assert_eq!(
            reorg
                .committed
                .and_then(|committed| committed.chain_epoch)
                .map(|epoch| epoch.chain_epoch_id),
            Some(resulting_epoch.id.value())
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shared_listener_authenticates_both_control_surfaces_and_serves_history()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = shared_control_fixture().await?;
        assert_control_authentication(&mut fixture.canonical_client, &mut fixture.ingest_client)
            .await?;
        let chain_events = assert_visible_chain_events_contract(&mut fixture.ingest_client).await?;
        let mempool_events = assert_mempool_contract(&mut fixture.ingest_client).await?;

        drop(mempool_events);
        drop(chain_events);
        fixture.shutdown().await
    }

    /// The retained adapter must bind snapshot paging, transaction lookups, and
    /// durable event replay to the same live mempool owner.
    #[tokio::test(flavor = "multi_thread")]
    #[allow(
        clippy::too_many_lines,
        reason = "The snapshot anchor, post-anchor mutation, page resume, point lookup, and replay assertion form one end-to-end adapter contract."
    )]
    async fn shared_listener_pages_snapshots_and_replays_only_post_anchor_events()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = shared_control_fixture().await?;
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        for transaction_id_byte in [0xB2, 0xC3] {
            fixture
                .mempool
                .apply_event(
                    &fixture.canonical,
                    MempoolEvent::Added {
                        entry: fixture_mempool_entry_with_id(transaction_id_byte, chain_epoch)?,
                    },
                    UnixTimestampMillis::new(1_750_000_000_200),
                )
                .await?;
        }

        let first_page = fixture
            .ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: Vec::new(),
            }))
            .await?
            .into_inner();
        assert_eq!(first_page.entries.len(), 1);
        assert!(!first_page.next_cursor.is_empty());
        let resume_cursor = first_page.events_resume_cursor.clone();

        let late_entry = fixture_mempool_entry_with_id(0xD4, chain_epoch)?;
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Added {
                    entry: late_entry.clone(),
                },
                UnixTimestampMillis::new(1_750_000_000_300),
            )
            .await?;

        let mut next_cursor = first_page.next_cursor;
        while !next_cursor.is_empty() {
            let later_page = fixture
                .ingest_client
                .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                    max_entries: 1,
                    from_cursor: next_cursor,
                }))
                .await?
                .into_inner();
            assert_eq!(later_page.events_resume_cursor, resume_cursor);
            next_cursor = later_page.next_cursor;
        }

        let transaction = fixture
            .ingest_client
            .mempool_transaction(authenticated(MempoolTransactionRequest {
                transaction_id: encode_rpc_transaction_id_hex(late_entry.transaction_id()),
            }))
            .await?
            .into_inner();
        let Some(transaction_location::Location::InMempool(transaction)) =
            transaction.location.and_then(|location| location.location)
        else {
            return Err("expected the late transaction in the live mempool".into());
        };
        assert_eq!(
            transaction.raw_transaction_bytes,
            late_entry.raw_transaction_bytes().as_slice()
        );

        let mut events = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::AfterCursor(
                        zinder_store::StreamCursorTokenV1::from_bytes(resume_cursor),
                    ),
                )),
            }))
            .await?
            .into_inner();
        let event = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await?
            .ok_or("mempool event stream ended before the post-anchor entry")??;
        assert_eq!(event.event_sequence, 4);
        assert!(matches!(
            event.event,
            Some(mempool_event_envelope::Event::Added(_))
        ));

        drop(events);
        fixture.shutdown().await
    }

    /// All three transparent point lookup RPCs resolve the same current
    /// in-memory entry and preserve absent entries as omissions/empty slots.
    #[tokio::test(flavor = "multi_thread")]
    async fn shared_listener_serves_transparent_mempool_lookups()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = shared_control_fixture().await?;
        let transaction_id = TransactionId::from_bytes([0xA1; 32]);
        let known_outpoint = OutPoint {
            transaction_id: encode_rpc_transaction_id_hex(transaction_id),
            output_index: 0,
        };

        let by_address = fixture
            .ingest_client
            .transparent_mempool_outputs_by_address(authenticated(
                TransparentMempoolOutputsByAddressRequest {
                    address: Some(AddressLookup {
                        selector: Some(address_lookup::Selector::ScriptHash(
                            zinder_core::TransparentAddressScriptHash::of_script_pub_key(
                                &[0xA1; 25],
                            )
                            .as_bytes()
                            .to_vec(),
                        )),
                    }),
                    max_entries: None,
                },
            ))
            .await?
            .into_inner();
        assert_eq!(by_address.outputs.len(), 1);
        assert_eq!(by_address.outputs[0].value_zat, 1_000);

        let spends = fixture
            .ingest_client
            .transparent_mempool_spends_by_outpoint(authenticated(
                TransparentMempoolSpendsByOutpointRequest {
                    outpoints: vec![
                        OutPoint {
                            transaction_id: "55".repeat(32),
                            output_index: 0,
                        },
                        OutPoint {
                            transaction_id: "ff".repeat(32),
                            output_index: 7,
                        },
                    ],
                },
            ))
            .await?
            .into_inner();
        assert_eq!(spends.spends.len(), 1);
        assert_eq!(
            spends.spends[0].spending_transaction_id,
            encode_rpc_transaction_id_hex(transaction_id)
        );

        let outputs = fixture
            .ingest_client
            .transparent_mempool_outputs_by_outpoint(authenticated(
                TransparentMempoolOutputsByOutpointRequest {
                    outpoints: vec![
                        known_outpoint,
                        OutPoint {
                            transaction_id: "ff".repeat(32),
                            output_index: 0,
                        },
                    ],
                },
            ))
            .await?
            .into_inner();
        assert_eq!(outputs.entries.len(), 2);
        assert_eq!(
            outputs.entries[0]
                .output
                .as_ref()
                .map(|output| output.value_zat),
            Some(1_000)
        );
        assert!(outputs.entries[1].output.is_none());

        fixture.shutdown().await
    }

    /// Retention is owned by the canonical writer: once a mined envelope is
    /// pruned, its cursor must be rejected at the public replay boundary.
    #[tokio::test(flavor = "multi_thread")]
    #[allow(
        clippy::too_many_lines,
        reason = "The retention setup, stream request, and failed-precondition assertion describe one public replay contract."
    )]
    async fn shared_listener_rejects_replay_from_a_pruned_mined_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = shared_control_fixture().await?;
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        let observed_at = UnixTimestampMillis::new(1_750_000_000_400);
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Mined {
                    transaction_id: TransactionId::from_bytes([0xA1; 32]),
                    mined_height: BlockHeight::new(41),
                    block_hash: BlockHash::from_bytes([0xA1; 32]),
                },
                observed_at,
            )
            .await?;
        let entry = fixture_mempool_entry_with_id(0xE5, chain_epoch)?;
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Added {
                    entry: entry.clone(),
                },
                observed_at,
            )
            .await?;
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Mined {
                    transaction_id: entry.transaction_id(),
                    mined_height: BlockHeight::new(42),
                    block_hash: BlockHash::from_bytes([0xE5; 32]),
                },
                observed_at,
            )
            .await?;

        let mut events = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::EarliestRetained,
                )),
            }))
            .await?
            .into_inner();
        let mut mined_cursor = None;
        for _ in 0..4 {
            let event = tokio::time::timeout(Duration::from_secs(1), events.next())
                .await?
                .ok_or("mempool event stream ended before the mined event")??;
            if mined_cursor.is_none()
                && matches!(event.event, Some(mempool_event_envelope::Event::Mined(_)))
            {
                mined_cursor = Some(event.cursor);
            }
        }
        drop(events);
        let mined_cursor = mined_cursor.ok_or("fixture did not emit a mined event")?;

        let retention = MempoolEventRetentionConfig::new(Some(Duration::from_millis(1)), None);
        let budget = zinder_store::MempoolEventRetentionStepBudget::new(
            std::num::NonZeroU32::new(8).ok_or("retention event budget must be nonzero")?,
            std::num::NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
        );
        let mut pruned_mined_count = 0_u64;
        for _step in 0..4 {
            let outcome = fixture
                .mempool
                .prune_events(
                    &fixture.canonical,
                    UnixTimestampMillis::now(),
                    retention,
                    budget,
                )
                .await?;
            pruned_mined_count =
                pruned_mined_count.saturating_add(outcome.report.pruned_mined_count);
            if !outcome.has_immediate_work() {
                break;
            }
        }
        assert!(pruned_mined_count >= 1);

        let status = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::AfterCursor(
                        zinder_store::StreamCursorTokenV1::from_bytes(mined_cursor),
                    ),
                )),
            }))
            .await
            .err()
            .ok_or("pruned mempool cursor unexpectedly remained replayable")?;
        assert_eq!(status.code(), Code::FailedPrecondition);

        fixture.shutdown().await
    }

    /// Value-pool capability discovery and its response remain coupled to the
    /// configured upstream source, while the chain view remains writer-owned.
    #[tokio::test(flavor = "multi_thread")]
    async fn adapter_advertises_and_reads_value_pools_from_the_configured_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = shared_control_fixture().await?;
        let expected_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        let source = StaticValuePoolSource {
            capabilities: NodeCapabilities::new([NodeCapability::ChainValuePools])?,
            value_pools: ChainValuePools::new(
                BlockId::new(BlockHeight::new(42), BlockHash::from_bytes([0x42; 32])),
                vec![ChainValuePool::new("transparent", true, Some(1_000))],
            ),
        };
        let adapter = CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            fixture.canonical.clone(),
            fixture.mempool.clone(),
            Arc::new(source),
            Readiness::default(),
        );

        let server_info = IngestControl::server_info(&adapter, Request::new(ServerInfoRequest {}))
            .await?
            .into_inner()
            .server_info
            .ok_or("ingest server info was absent")?;
        assert!(server_info.capabilities.iter().any(|capability| {
            capability == zinder_proto::capabilities::INGEST_CONTROL_CHAIN_VALUE_POOLS_AT_TIP_V1
        }));
        assert_eq!(
            server_info.build_git_commit,
            zinder_runtime::BUILD_GIT_COMMIT
        );
        assert!(server_info.materialized_view_preset.is_empty());
        assert!(server_info.materialized_view_identities.is_empty());

        let response = IngestControl::chain_value_pools_at_tip(
            &adapter,
            Request::new(zinder_proto::v1::wallet::ChainValuePoolsAtTipRequest {}),
        )
        .await?
        .into_inner();
        assert_eq!(response.source_tip.map(|tip| tip.height), Some(42));
        assert_eq!(response.pools[0].chain_value_zat, Some(1_000));
        assert_eq!(
            response
                .chain_view
                .and_then(|view| view.chain_epoch)
                .map(|epoch| epoch.chain_epoch_id),
            Some(expected_epoch.id.value())
        );

        fixture.shutdown().await
    }

    /// Writer status derives its epoch from the canonical owner and its phase
    /// and upstream gap from readiness, without opening a second store.
    #[tokio::test(flavor = "multi_thread")]
    async fn adapter_combines_canonical_epoch_with_readiness_status()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = shared_control_fixture().await?;
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        let visible_height = chain_epoch.visible_tip_height.value();
        let upstream_height = visible_height.saturating_add(2);
        let readiness = Readiness::new(
            ReadinessState::syncing(Some(2), Some(visible_height), Some(upstream_height))
                .with_phase(IngestPhase::BulkCatchup),
        );
        let source: Arc<dyn NodeSource> = Arc::new(ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashTestnet,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?);
        let adapter = CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            fixture.canonical.clone(),
            fixture.mempool.clone(),
            source,
            readiness,
        );

        let response = IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(response.phase(), WriterPhase::BulkCatchup);
        assert_eq!(response.gap_blocks, Some(2));
        assert_eq!(
            response
                .chain_view
                .and_then(|view| view.chain_epoch)
                .map(|epoch| epoch.chain_epoch_id),
            Some(chain_epoch.id.value())
        );

        fixture.shutdown().await
    }

    /// A parked writer has dropped its command queue, so `WriterStatus`
    /// answers from readiness alone and carries the typed halt.
    #[tokio::test(flavor = "multi_thread")]
    async fn parked_writer_status_reports_the_reorg_halt_without_a_live_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let (canonical, commands) = canonical_control_channel();
        drop(commands);
        let readiness = Readiness::new(
            ReadinessState::reorg_window_exceeded(1_001, 1_000, Some(4_200))
                .with_phase(IngestPhase::FollowingTip),
        );
        let source: Arc<dyn NodeSource> = Arc::new(ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashTestnet,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?);
        let adapter = CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            canonical,
            LiveMempoolOwner::default(),
            source,
            readiness.clone(),
        );

        let response = IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(response.phase(), WriterPhase::FollowingTip);
        assert_eq!(response.chain_view, None);
        assert_eq!(response.gap_blocks, None);
        assert_eq!(response.upstream_not_ready, None);
        assert_eq!(
            response.reorg_window_exceeded,
            Some(zinder_proto::v1::ops::ReorgWindowExceededDetail {
                depth: 1_001,
                configured: 1_000,
            })
        );

        readiness.set(ReadinessState::ready(Some(4_200)));
        let unavailable =
            IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
                .await
                .err()
                .ok_or("a writer that is not parked must answer from its live fence")?;
        assert_eq!(unavailable.code(), Code::Unavailable);
        Ok(())
    }

    #[derive(Clone)]
    struct StaticValuePoolSource {
        capabilities: NodeCapabilities,
        value_pools: ChainValuePools,
    }

    #[async_trait]
    impl NodeSource for StaticValuePoolSource {
        fn capabilities(&self) -> NodeCapabilities {
            self.capabilities
        }

        async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
            Err(SourceError::BlockUnavailable {
                height,
                reason: "value-pool test source does not serve blocks".to_owned(),
            })
        }

        async fn tip_id(&self) -> Result<BlockId, SourceError> {
            Err(SourceError::NodeUnavailable {
                reason: "value-pool test source does not serve tips".to_owned(),
            })
        }

        async fn fetch_subtree_roots(
            &self,
            protocol: ShieldedProtocol,
            start_index: SubtreeRootIndex,
            max_entries: NonZeroU32,
        ) -> Result<SourceSubtreeRoots, SourceError> {
            let _ignored = (protocol, start_index, max_entries);
            Err(SourceError::NodeCapabilityMissing {
                capability: NodeCapability::SubtreeRoots,
            })
        }

        async fn fetch_chain_value_pools_at_tip(&self) -> Result<ChainValuePools, SourceError> {
            Ok(self.value_pools.clone())
        }
    }

    struct SharedControlFixture {
        temporary: tempfile::TempDir,
        canonical_client: CanonicalControlClient<tonic::transport::Channel>,
        ingest_client: IngestControlClient<tonic::transport::Channel>,
        canonical: crate::CanonicalControlHandle,
        mempool: LiveMempoolOwner,
        cancel: CancellationToken,
        server_task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        command_task: tokio::task::JoinHandle<()>,
    }

    impl SharedControlFixture {
        async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
            drop(self.ingest_client);
            drop(self.canonical_client);
            self.cancel.cancel();
            self.server_task.await??;
            self.command_task.abort();
            let _ = self.command_task.await;
            drop(self.temporary);
            Ok(())
        }
    }

    async fn shared_control_fixture() -> Result<SharedControlFixture, Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let checkpoint_staging_root = temporary.path().join("checkpoint-staging");
        fs::create_dir(&checkpoint_staging_root)?;
        let mut store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, mut commands) = canonical_control_channel();
        let command_task = tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                apply_canonical_control_command(&mut store, command);
            }
        });

        let owner = LiveMempoolOwner::default();
        let chain_epoch = canonical.chain_epoch().await?.chain_epoch;
        owner
            .apply_event(
                &canonical,
                MempoolEvent::Added {
                    entry: fixture_mempool_entry(chain_epoch)?,
                },
                UnixTimestampMillis::new(1_750_000_000_100),
            )
            .await?;
        let source_tip =
            zinder_core::BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash);
        let _certification = owner.try_complete_hydration(&canonical, source_tip).await?;

        let bearer_token = BearerToken::from_str("fixture-control-token")?;
        let node_source: Arc<dyn NodeSource> = Arc::new(ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashTestnet,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?);
        let readiness = Readiness::new(ReadinessState::ready(Some(1)));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let canonical_adapter = CanonicalControlGrpcAdapter::new(
            canonical.clone(),
            CanonicalCheckpointStagingRoot::new(checkpoint_staging_root),
            RocksDbResourceBudget::for_local_tests(),
        )
        .with_bearer_token(Some(bearer_token.clone()));
        let ingest_adapter = CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            canonical.clone(),
            owner.clone(),
            node_source,
            readiness,
        )
        .with_bearer_token(Some(bearer_token));
        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(canonical_adapter.into_server())
                .add_service(ingest_adapter.into_server())
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_cancel.cancelled_owned(),
                )
                .await
        });

        let endpoint = format!("http://{listen_addr}");
        let canonical_client = CanonicalControlClient::connect(endpoint.clone()).await?;
        let ingest_client = IngestControlClient::connect(endpoint).await?;
        Ok(SharedControlFixture {
            temporary,
            canonical_client,
            ingest_client,
            canonical,
            mempool: owner,
            cancel,
            server_task,
            command_task,
        })
    }

    async fn assert_control_authentication(
        canonical_client: &mut CanonicalControlClient<tonic::transport::Channel>,
        ingest_client: &mut IngestControlClient<tonic::transport::Channel>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            canonical_client
                .writer_status(CanonicalWriterStatusRequest {})
                .await
                .err()
                .map(|status| status.code()),
            Some(Code::Unauthenticated)
        );
        assert_eq!(
            ingest_client
                .writer_status(WriterStatusRequest {})
                .await
                .err()
                .map(|status| status.code()),
            Some(Code::Unauthenticated)
        );

        let canonical_status = canonical_client
            .writer_status(authenticated(CanonicalWriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(
            canonical_status.fence.map(|fence| fence.event_sequence),
            Some(1)
        );
        let ingest_status = ingest_client
            .writer_status(authenticated(WriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(ingest_status.network_name, "zcash-testnet");
        Ok(())
    }

    async fn assert_visible_chain_events_contract(
        ingest_client: &mut IngestControlClient<tonic::transport::Channel>,
    ) -> Result<tonic::Streaming<ChainEventEnvelope>, Box<dyn std::error::Error>> {
        let server_info = ingest_client
            .server_info(authenticated(ServerInfoRequest {}))
            .await?
            .into_inner()
            .server_info
            .ok_or("ingest server info was absent")?;
        assert!(
            server_info
                .capabilities
                .iter()
                .any(|capability| capability == INGEST_CONTROL_VISIBLE_CHAIN_EVENTS_V1),
            "the implemented visible chain-event control stream must be advertised"
        );
        assert_eq!(
            server_info.build_git_commit,
            zinder_runtime::BUILD_GIT_COMMIT
        );
        assert!(server_info.materialized_view_preset.is_empty());
        assert!(server_info.materialized_view_identities.is_empty());

        let mut chain_events = ingest_client
            .visible_chain_events(authenticated(event_stream_start_message(
                &EventStreamStartPosition::EarliestRetained,
            )))
            .await?
            .into_inner();
        let first_chain_event = tokio::time::timeout(Duration::from_secs(1), chain_events.next())
            .await?
            .ok_or("visible event stream ended before the fixture event")??;
        assert_eq!(first_chain_event.event_sequence, 1);
        assert!(matches!(
            first_chain_event.event,
            Some(chain_event_envelope::Event::ChainCommitted(_))
        ));

        Ok(chain_events)
    }

    async fn assert_mempool_contract(
        ingest_client: &mut IngestControlClient<tonic::transport::Channel>,
    ) -> Result<tonic::Streaming<MempoolEventEnvelope>, Box<dyn std::error::Error>> {
        let snapshot = ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 10,
                from_cursor: Vec::new(),
            }))
            .await?
            .into_inner();
        assert_eq!(snapshot.entries.len(), 1);
        let mut mempool_events = ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::EarliestRetained,
                )),
            }))
            .await?
            .into_inner();
        let first_mempool_event =
            tokio::time::timeout(Duration::from_secs(1), mempool_events.next())
                .await?
                .ok_or("mempool event stream ended before the fixture event")??;
        assert!(matches!(
            first_mempool_event.event,
            Some(mempool_event_envelope::Event::Added(_))
        ));
        Ok(mempool_events)
    }

    fn authenticated<Message>(message: Message) -> Request<Message> {
        let mut request = Request::new(message);
        request.metadata_mut().insert(
            "authorization",
            tonic::metadata::MetadataValue::from_static("Bearer fixture-control-token"),
        );
        request
    }

    fn fixture_mempool_entry(
        chain_epoch: zinder_core::ChainEpoch,
    ) -> Result<MempoolEntry, zinder_core::MempoolEntryBuildError> {
        fixture_mempool_entry_with_id(0xA1, chain_epoch)
    }

    fn fixture_mempool_entry_with_id(
        transaction_id_byte: u8,
        chain_epoch: ChainEpoch,
    ) -> Result<MempoolEntry, zinder_core::MempoolEntryBuildError> {
        let transaction_id = TransactionId::from_bytes([transaction_id_byte; 32]);
        MempoolEntry::new(
            transaction_id,
            None,
            RawTransactionBytes::new(vec![transaction_id_byte; 8]),
            CompactTransactionData {
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 1_000,
                    script_pub_key: vec![0xA1; 25],
                }],
                transparent_inputs: vec![CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes(
                        [if transaction_id_byte == 0xA1 {
                            0x55
                        } else {
                            transaction_id_byte
                        }; 32],
                    ),
                    previous_output_index: 0,
                }],
                ..CompactTransactionData::default()
            },
            MempoolObservation {
                first_seen_unix_millis: UnixTimestampMillis::new(1_750_000_000_100),
                first_seen_chain_epoch: chain_epoch,
            },
        )
    }

    fn fixture_epoch(id: u64, height: u32, hash_byte: u8) -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(id),
            network: zinder_core::Network::ZcashTestnet,
            visible_tip_height: BlockHeight::new(height),
            visible_tip_hash: BlockHash::from_bytes([hash_byte; 32]),
            settled_tip_height: BlockHeight::new(height.saturating_sub(1)),
            settled_tip_hash: BlockHash::from_bytes([hash_byte.saturating_sub(1); 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(4),
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(1_750_000_000_000),
        }
    }
}
