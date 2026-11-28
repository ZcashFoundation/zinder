//! Version-1 `IngestControl` composition over the canonical writer channel.
//!
//! This adapter never opens a canonical primary or a materialized-view store. Exact
//! writer status and tip-event replay are serviced by the follower that owns
//! the admitted `RocksDbCanonicalStore`; resumable mempool history and cursor
//! retention are also owned by that canonical `RocksDB` event log. Only the
//! current low-latency mempool index is process-local, and it remains hidden
//! until its source snapshot completes.

use std::{num::NonZeroU32, pin::Pin, sync::Arc, time::Duration};

use prost::Message as _;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, service::interceptor::InterceptedService};
use zinder_core::{
    ChainEpoch, ChainValuePools, Network, TransactionId,
    wire::{decode_rpc_transaction_id_hex, encode_zinder_native_chain_name},
};
use zinder_proto::{
    capabilities::{CapabilitySurface, capabilities_for_surface},
    status_for_reason,
    v1::{
        ingest::{
            MempoolTransactionRequest, ServerInfoRequest, ServerInfoResponse, WriterPhase,
            WriterStatusRequest, WriterStatusResponse,
            ingest_control_server::{IngestControl, IngestControlServer},
        },
        ops, wallet,
    },
    wire::TransactionAbsence,
};
use zinder_runtime::{
    BearerToken, BearerTokenServerInterceptor, ComposedReadiness, IngestPhase, ReadinessCause,
    ReadinessInput, ReadinessReport, RetryPushback, UpstreamNotReadyDetail,
};
use zinder_source::{NodeCapability, NodeSource, SourceError};
use zinder_store::{
    CanonicalEventKind, block_tip_message, chain_epoch_message, chain_view_message,
    event_stream_start_from_message,
};

use crate::{
    CanonicalControlHandle, LiveMempoolOwner,
    writer::control::{CanonicalIngestEvent, CanonicalReadDeadline, CanonicalWriterSnapshot},
};

type IngestControlStream<Message> = Pin<Box<dyn Stream<Item = Result<Message, Status>> + Send>>;
type VisibleChainEventsStream = IngestControlStream<wallet::ChainEventEnvelope>;
type MempoolEventsStream = IngestControlStream<wallet::MempoolEventEnvelope>;

const DEFAULT_MEMPOOL_SNAPSHOT_PAGE_SIZE: u32 = 256;
/// Hard response entry-count bound shared by the private writer control APIs.
pub const MAX_MEMPOOL_SNAPSHOT_PAGE_SIZE: u32 = 1_024;
/// Maximum encoded protobuf size of a `MempoolSnapshotResponse` page.
///
/// Leaves 512 KiB of headroom below tonic's 4 MiB default client decode limit
/// while comfortably fitting a single consensus-maximum (~2 MiB) Zcash
/// transaction with its compact and transparent overlays.
pub const MAX_MEMPOOL_SNAPSHOT_RESPONSE_BYTES: usize = 3_584 * 1_024;
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
    readiness: ComposedReadiness,
}

impl CanonicalIngestControlGrpcAdapter {
    /// Binds one canonical writer command channel, one live mempool owner,
    /// the source handle already owned by the ingest runtime, and the
    /// writer's readiness composition.
    ///
    /// `WriterStatus` reports the phase and heights from the projected
    /// readiness, and upstream health from the
    /// [`ReadinessInput::NodeSource`] input even while a refusing admission
    /// state, such as `syncing`, supplies the projected cause. During fresh
    /// construction it answers from readiness alone, without a chain view.
    #[must_use]
    pub fn new(
        network: Network,
        canonical: CanonicalControlHandle,
        mempool: LiveMempoolOwner,
        node_source: Arc<dyn NodeSource>,
        readiness: ComposedReadiness,
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
            }),
        }))
    }

    async fn writer_status(
        &self,
        request: Request<WriterStatusRequest>,
    ) -> Result<Response<WriterStatusResponse>, Status> {
        let readiness_report = self.readiness.runtime_readiness().report();
        // Fresh construction has no published epoch, and the writer services
        // its command queue only once it follows a published store.
        if readiness_report.phase == Some(IngestPhase::BulkCatchup) {
            return Ok(Response::new(writer_status_response(
                self.network,
                None,
                &readiness_report,
                &self.readiness,
            )));
        }
        // A parked writer reports its frozen fence while it still answers;
        // once its owner loop has stopped, readers still need the typed halt
        // rather than an unavailable control channel.
        let chain_epoch = match CanonicalReadDeadline::from_request(&request)
            .wait(self.current_chain_epoch())
            .await
        {
            Ok(snapshot) => Some(snapshot.chain_epoch),
            Err(_)
                if matches!(
                    readiness_report.cause,
                    ReadinessCause::ReorgWindowExceeded { .. }
                ) =>
            {
                None
            }
            Err(status) => return Err(status),
        };
        Ok(Response::new(writer_status_response(
            self.network,
            chain_epoch,
            &readiness_report,
            &self.readiness,
        )))
    }

    async fn visible_chain_events(
        &self,
        request: Request<wallet::EventStreamStart>,
    ) -> Result<Response<Self::VisibleChainEventsStream>, Status> {
        let deadline = CanonicalReadDeadline::from_request(&request);
        let after_cursor = match request
            .into_inner()
            .position
            .ok_or_else(|| Status::invalid_argument("event stream start position is required"))?
        {
            wallet::event_stream_start::Position::AfterCursor(cursor) => Some(cursor),
            wallet::event_stream_start::Position::EarliestRetained(_) => None,
            wallet::event_stream_start::Position::LiveTail(_) => {
                let snapshot = deadline.wait(self.current_chain_epoch()).await?;
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
        let deadline = CanonicalReadDeadline::from_request(&request);
        let request = request.into_inner();
        // The owner captures the canonical fence before touching the mempool
        // page. A tip committed during or after the page read is therefore
        // observable as a strictly newer chain event by tip-coherent consumers.
        let (chain_epoch, page) = deadline
            .wait(self.mempool.snapshot_page_at_canonical_tip(
                &self.canonical,
                bounded_snapshot_page_size(request.max_entries),
                request.from_cursor,
            ))
            .await?;
        let response = wallet::MempoolSnapshotResponse {
            chain_view: Some(chain_view_message(chain_epoch)),
            events_resume_cursor: page.events_resume_cursor,
            snapshot_age_millis: page.snapshot_age_millis,
            entries: page.entries,
            next_cursor: page.next_cursor,
            source_tip: Some(block_tip_message(
                page.source_tip.height,
                page.source_tip.hash,
            )),
        };
        if response.encoded_len() > MAX_MEMPOOL_SNAPSHOT_RESPONSE_BYTES {
            return Err(status_for_reason(
                ops::ErrorReason::UnsupportedWalletEncoding,
                format!(
                    "mempool snapshot response ({} bytes) exceeds the {MAX_MEMPOOL_SNAPSHOT_RESPONSE_BYTES}-byte response budget",
                    response.encoded_len(),
                ),
            ));
        }
        Ok(Response::new(response))
    }

    async fn mempool_transaction(
        &self,
        request: Request<MempoolTransactionRequest>,
    ) -> Result<Response<wallet::TransactionStatusResponse>, Status> {
        let deadline = CanonicalReadDeadline::from_request(&request);
        let transaction_id = transaction_id_from_rpc_hex(&request.into_inner().transaction_id)?;
        let (chain_epoch, entry) = deadline
            .wait(
                self.mempool
                    .entry_at_canonical_tip(&self.canonical, transaction_id),
            )
            .await?;
        let entry = entry.ok_or_else(|| {
            TransactionAbsence::at_epoch(chain_epoch, true)
                .into_status("transaction is not visible in the live mempool index")
        })?;
        Ok(Response::new(wallet::TransactionStatusResponse {
            chain_view: Some(chain_view_message(chain_epoch)),
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
        let deadline = CanonicalReadDeadline::for_mempool_events(&request);
        let request = request.into_inner();
        let start = event_stream_start_from_message(request.start)
            .ok_or_else(|| Status::invalid_argument("event stream start is required"))?;
        let stream_start = deadline
            .wait(self.mempool.resolve_event_start(&self.canonical, start))
            .await?;
        Ok(Response::new(spawn_mempool_event_stream(
            self.mempool.clone(),
            self.canonical.clone(),
            stream_start,
        )))
    }

    async fn chain_value_pools_at_tip(
        &self,
        request: Request<wallet::ChainValuePoolsAtTipRequest>,
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
        let snapshot = CanonicalReadDeadline::from_request(&request)
            .wait(self.current_chain_epoch())
            .await?;
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
    stream_start: zinder_store::CanonicalMempoolEventStreamStart,
) -> MempoolEventsStream {
    let (event_sender, event_receiver) = mpsc::channel(16);
    tokio::spawn(async move {
        let (mut after_cursor, mut last_sequence, replay_through_sequence, mut head_subscription) =
            stream_start.into_parts();
        let mut at_live_tail = last_sequence >= replay_through_sequence;
        let mut empty_at_head = last_sequence;
        let mut withdrawal = std::pin::pin!(mempool.live_tail_withdrawn());
        loop {
            let wait_target = last_sequence.max(empty_at_head);
            let mut notifications_closed = false;
            if head_subscription.current_head_sequence() <= wait_target {
                tokio::select! {
                    biased;
                    () = event_sender.closed() => return,
                    status = &mut withdrawal, if at_live_tail => {
                        let _sent = event_sender.send(Err(status)).await;
                        return;
                    }
                    advanced = head_subscription.wait_for_advance_past(wait_target) => {
                        notifications_closed = advanced.is_none();
                    }
                }
            }
            let observed_head = head_subscription.current_head_sequence();
            let page = tokio::select! {
                biased;
                () = event_sender.closed() => return,
                status = &mut withdrawal, if at_live_tail => Err(status),
                page = mempool.event_page(&canonical, after_cursor.clone()) => page,
            };
            let events = match page {
                Ok(events) => events,
                Err(status) => {
                    let _sent = event_sender.send(Err(status)).await;
                    return;
                }
            };
            if events.is_empty() {
                if notifications_closed {
                    let _sent = event_sender
                        .send(Err(Status::unavailable(
                            "mempool event head notifications closed before delivering further events",
                        )))
                        .await;
                    return;
                }
                empty_at_head = empty_at_head.max(observed_head);
                continue;
            }
            for event in events {
                last_sequence = event.event_sequence;
                after_cursor = Some(zinder_store::StreamCursorTokenV1::from_bytes(
                    event.cursor.clone(),
                ));
                // Replay everything retained at establishment before relying on live observation.
                if !at_live_tail && last_sequence >= replay_through_sequence {
                    withdrawal.set(mempool.live_tail_withdrawn());
                    at_live_tail = true;
                }
                if event_sender.send(Ok(event)).await.is_err() {
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

/// What the follower's own admission state says about the node, which the
/// projected readiness report hides while a required node-health input
/// refuses or the writer drains for shutdown.
#[derive(Clone, Copy, Debug)]
struct FollowedNode {
    /// The node tip the follower last observed.
    target_height: Option<u32>,
    /// Whether the follower reports the node unreachable.
    is_unavailable: bool,
}

impl FollowedNode {
    fn of(readiness: &ComposedReadiness) -> Self {
        let admission = readiness.admission_state();
        Self {
            target_height: admission.target_height,
            is_unavailable: matches!(admission.cause, ReadinessCause::NodeUnavailable(_)),
        }
    }
}

fn writer_status_response(
    network: Network,
    chain_epoch: Option<ChainEpoch>,
    readiness_report: &ReadinessReport,
    readiness: &ComposedReadiness,
) -> WriterStatusResponse {
    let followed_node = FollowedNode::of(readiness);
    let node_source_observation = readiness.input_cause(ReadinessInput::NodeSource);
    let upstream_not_ready = if let ReadinessCause::UpstreamNotReady(detail) =
        node_source_observation
            .as_ref()
            .unwrap_or(&readiness_report.cause)
    {
        Some(detail)
    } else {
        None
    };
    let upstream_tip = if followed_node.is_unavailable
        || matches!(readiness_report.cause, ReadinessCause::NodeUnavailable(_))
    {
        None
    } else {
        upstream_tip_from_readiness(followed_node.target_height, upstream_not_ready)
    };
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
    WriterStatusResponse {
        chain_view,
        network_name: encode_zinder_native_chain_name(network).to_owned(),
        phase: readiness_report
            .phase
            .map_or(WriterPhase::Unspecified, WriterPhase::from)
            .into(),
        gap_blocks,
        upstream_not_ready: upstream_not_ready.map(ops::UpstreamNotReadyDetail::from),
        reorg_window_exceeded: readiness_report.cause.reorg_window_exceeded_detail(),
    }
}

/// Returns the node tip to report from the follower's observed target and
/// the node-health probe's not-ready report.
///
/// The follower can observe a newer tip between health probes. Each height
/// is the larger of the two, and a heightless `/ready` report leaves the
/// follower's target.
fn upstream_tip_from_readiness(
    followed_target: Option<u32>,
    upstream_not_ready: Option<&UpstreamNotReadyDetail>,
) -> Option<wallet::UpstreamTip> {
    let committed_height = upstream_not_ready
        .and_then(|detail| detail.upstream_committed_height)
        .max(followed_target);
    let estimated_height = upstream_not_ready
        .and_then(|detail| detail.upstream_estimated_height)
        .map(|estimated| followed_target.map_or(estimated, |target| estimated.max(target)));
    (committed_height.is_some() || estimated_height.is_some()).then_some(wallet::UpstreamTip {
        committed_height,
        estimated_height,
    })
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
        SourceError::NodeRateLimited { .. } => RetryPushback::Backoff.apply(status_for_reason(
            ops::ErrorReason::NodeRateLimited,
            error.to_string(),
        )),
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

fn transaction_id_from_rpc_hex(rpc_hex: &str) -> Result<TransactionId, Status> {
    decode_rpc_transaction_id_hex(rpc_hex)
        .map_err(|error| Status::invalid_argument(error.to_string()))
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
    use zinder_proto::wire::TransactionAbsence;
    use zinder_proto::{
        capabilities::INGEST_CONTROL_VISIBLE_CHAIN_EVENTS_V1,
        v1::{
            ingest::{
                CanonicalWriterStatusRequest, MempoolTransactionRequest, ServerInfoRequest,
                WriterPhase, WriterStatusRequest, canonical_control_client::CanonicalControlClient,
                ingest_control_client::IngestControlClient, ingest_control_server::IngestControl,
            },
            wallet::{
                ChainEventEnvelope, MempoolEventEnvelope, MempoolEventsRequest,
                MempoolSnapshotRequest, chain_event_envelope, mempool_event_envelope,
                transaction_location,
            },
        },
    };
    use zinder_runtime::{
        BearerToken, ComposedReadiness, IngestPhase, Readiness, ReadinessCause, ReadinessInput,
        ReadinessState, UpstreamHealth, UpstreamNotReadyDetail,
    };
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
            ParkedWriterHalt, apply_canonical_control_command,
            apply_parked_canonical_control_command, canonical_control_channel,
            test_support::published_fixture_store,
        },
    };

    use super::{CanonicalIngestControlGrpcAdapter, canonical_event_message};

    fn assert_reorg_halt(status: &tonic::Status) {
        use tonic_types::StatusExt as _;
        assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
        assert_eq!(
            status
                .get_error_details()
                .error_info()
                .map(|error_info| error_info.reason.clone()),
            Some(
                zinder_proto::v1::ops::ErrorReason::ReorgWindowExceeded
                    .as_str_name()
                    .to_owned()
            ),
            "{status:?}"
        );
    }

    /// A park ends open mempool event streams and refuses every mempool read
    /// and write with the typed halt, while canonical writer status carries
    /// the halt beside the frozen fence.
    #[tokio::test(flavor = "multi_thread")]
    #[allow(
        clippy::too_many_lines,
        reason = "the open stream, new reads, owner-bypassing writes, and writer status form one parked-writer contract"
    )]
    async fn parked_writer_ends_mempool_streams_and_refuses_mempool_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = shared_control_fixture().await?;
        let mut events = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::EarliestRetained,
                )),
            }))
            .await?
            .into_inner();
        let first = tokio::time::timeout(Duration::from_secs(1), events.next())
            .await?
            .ok_or("mempool event stream ended before its retained event")??;
        assert_eq!(first.event_sequence, 1);

        let halt = ParkedWriterHalt::new(zinder_proto::v1::ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        });
        *fixture.parked.lock() = Some(halt);
        fixture.mempool.halt(halt).await;

        let ended = tokio::time::timeout(Duration::from_secs(2), events.next())
            .await?
            .ok_or("an open mempool event stream must end with the halt, not close")?;
        let Err(status) = ended else {
            return Err("an open mempool event stream must end with the halt".into());
        };
        assert_reorg_halt(&status);

        let refused_stream = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::EarliestRetained,
                )),
            }))
            .await;
        let Err(status) = refused_stream else {
            return Err("a new mempool event stream must be refused".into());
        };
        assert_reorg_halt(&status);
        let Err(status) = fixture
            .ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: Vec::new(),
            }))
            .await
        else {
            return Err("a mempool snapshot must be refused".into());
        };
        assert_reorg_halt(&status);

        // The parked writer records no mempool event even from a caller that
        // bypasses the halted owner.
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        let Err(status) = fixture
            .canonical
            .append_mempool_event(
                MempoolEvent::Added {
                    entry: fixture_mempool_entry_with_id(0xE5, chain_epoch)?,
                },
                UnixTimestampMillis::new(1_750_000_000_400),
            )
            .await
        else {
            return Err("a parked writer must refuse mempool appends".into());
        };
        assert_reorg_halt(&status);
        let Err(status) = fixture
            .canonical
            .prune_mempool_events(
                UnixTimestampMillis::new(1_750_000_000_500),
                MempoolEventRetentionConfig::new(Some(Duration::from_millis(1)), None),
                zinder_store::MempoolEventRetentionStepBudget::new(
                    std::num::NonZeroU32::new(8).ok_or("retention event budget must be nonzero")?,
                    std::num::NonZeroU64::new(1_000_000)
                        .ok_or("retention byte budget must be nonzero")?,
                ),
            )
            .await
        else {
            return Err("a parked writer must refuse mempool pruning".into());
        };
        assert_reorg_halt(&status);

        let writer_status = fixture
            .canonical_client
            .writer_status(authenticated(CanonicalWriterStatusRequest {}))
            .await?
            .into_inner();
        assert!(writer_status.fence.is_some());
        assert_eq!(
            writer_status
                .reorg_window_exceeded
                .map(|detail| (detail.depth, detail.configured)),
            Some((1_001, 1_000))
        );

        drop(events);
        fixture.shutdown().await
    }

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
        assert_ne!(first_page.next_cursor, [] as [u8; 0]);
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

    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_walk_pins_point_in_time_membership_across_same_epoch_mutations()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = shared_control_fixture().await?;
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        // `shared_control_fixture` seeds `0xA1`; adding `0xB2` forms `{0xA1, 0xB2}`.
        let entry_a = fixture_mempool_entry(chain_epoch)?;
        let entry_b = fixture_mempool_entry_with_id(0xB2, chain_epoch)?;
        let entry_c = fixture_mempool_entry_with_id(0xC3, chain_epoch)?;
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Added {
                    entry: entry_b.clone(),
                },
                UnixTimestampMillis::new(1_750_000_000_150),
            )
            .await?;

        let first_page = fixture
            .ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: Vec::new(),
            }))
            .await?
            .into_inner();
        assert_eq!(first_page.entries.len(), 1);
        assert_eq!(
            first_page.entries[0].transaction_id,
            encode_rpc_transaction_id_hex(entry_a.transaction_id())
        );

        // Evict `A` and add `C` before page 2 runs at the same `ChainEpoch`.
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Invalidated {
                    transaction_id: entry_a.transaction_id(),
                    reason: zinder_core::MempoolEvictionReason::Expired,
                },
                UnixTimestampMillis::new(1_750_000_000_200),
            )
            .await?;
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Added { entry: entry_c },
                UnixTimestampMillis::new(1_750_000_000_250),
            )
            .await?;

        let second_page = fixture
            .ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 10,
                from_cursor: first_page.next_cursor,
            }))
            .await?
            .into_inner();
        assert_eq!(second_page.entries.len(), 1);
        assert_eq!(
            second_page.entries[0].transaction_id,
            encode_rpc_transaction_id_hex(entry_b.transaction_id())
        );
        assert_eq!(second_page.next_cursor, [] as [u8; 0]);

        fixture.shutdown().await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_walk_expires_cursor_once_lru_pin_is_evicted_and_index_advances()
    -> Result<(), Box<dyn std::error::Error>> {
        use tonic_types::StatusExt as _;

        let mut fixture = shared_control_fixture().await?;
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        for tx_byte in [0x10_u8, 0x20_u8] {
            fixture
                .mempool
                .apply_event(
                    &fixture.canonical,
                    MempoolEvent::Added {
                        entry: fixture_mempool_entry_with_id(tx_byte, chain_epoch)?,
                    },
                    UnixTimestampMillis::new(1_750_000_000_100),
                )
                .await?;
        }

        let oldest_walk = fixture
            .ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: Vec::new(),
            }))
            .await?
            .into_inner();

        let eviction_steps = u8::try_from(crate::mempool::MAX_PINNED_SNAPSHOT_WALKS)?;
        for step in 0_u8..=eviction_steps {
            fixture
                .mempool
                .apply_event(
                    &fixture.canonical,
                    MempoolEvent::Added {
                        entry: fixture_mempool_entry_with_id(0x60 + step, chain_epoch)?,
                    },
                    UnixTimestampMillis::new(1_750_000_000_200 + u64::from(step)),
                )
                .await?;
            let _newer_walk = fixture
                .ingest_client
                .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                    max_entries: 1,
                    from_cursor: Vec::new(),
                }))
                .await?;
        }

        let Err(status) = fixture
            .ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: oldest_walk.next_cursor,
            }))
            .await
        else {
            return Err("evicted snapshot walk cursor must expire after index updates".into());
        };
        assert_eq!(status.code(), Code::FailedPrecondition, "{status:?}");
        assert_eq!(
            status
                .get_error_details()
                .error_info()
                .map(|detail| (detail.domain.clone(), detail.reason.clone())),
            Some((
                zinder_proto::ZINDER_ERROR_DOMAIN.to_owned(),
                zinder_proto::v1::ops::ErrorReason::SnapshotPageCursorExpired
                    .as_str_name()
                    .to_owned(),
            )),
        );

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
                    mined_height: chain_epoch.visible_tip_height,
                    block_hash: chain_epoch.visible_tip_hash,
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
                    mined_height: chain_epoch.visible_tip_height,
                    block_hash: chain_epoch.visible_tip_hash,
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
            rate_limited: false,
        };
        let adapter = CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            fixture.canonical.clone(),
            fixture.mempool.clone(),
            Arc::new(source),
            ComposedReadiness::new(Readiness::default(), &[]),
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

    /// A node that refuses the value-pool read with HTTP 429 reaches the
    /// client as `UNAVAILABLE` with `NODE_RATE_LIMITED` and retry pushback.
    #[tokio::test(flavor = "multi_thread")]
    async fn rate_limited_value_pool_read_is_unavailable_with_its_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = shared_control_fixture().await?;
        let source = StaticValuePoolSource {
            capabilities: NodeCapabilities::new([NodeCapability::ChainValuePools])?,
            value_pools: ChainValuePools::new(
                BlockId::new(BlockHeight::new(42), BlockHash::from_bytes([0x42; 32])),
                Vec::new(),
            ),
            rate_limited: true,
        };
        let adapter = CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            fixture.canonical.clone(),
            fixture.mempool.clone(),
            Arc::new(source),
            ComposedReadiness::new(Readiness::default(), &[]),
        );

        let status = IngestControl::chain_value_pools_at_tip(
            &adapter,
            Request::new(zinder_proto::v1::wallet::ChainValuePoolsAtTipRequest {}),
        )
        .await
        .err()
        .ok_or("a rate-limited node must fail the value-pool read")?;

        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(
            tonic_types::StatusExt::get_details_error_info(&status)
                .map(|error_info| error_info.reason),
            Some("NODE_RATE_LIMITED".to_owned())
        );
        let delay_ms = status
            .metadata()
            .get("grpc-retry-pushback-ms")
            .ok_or("missing retry pushback")?
            .to_str()?
            .parse::<i32>()?;
        assert!((500..=1500).contains(&delay_ms));
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
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        readiness.publish_admission(
            ReadinessState::syncing(Some(2), Some(visible_height), Some(upstream_height))
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
            fixture.canonical.clone(),
            fixture.mempool.clone(),
            source,
            readiness,
        );

        let response = IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(response.phase(), WriterPhase::FollowingTip);
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
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        composed.publish_admission(
            ReadinessState::reorg_window_exceeded(1_001, 1_000, Some(1_000), Some(4_200))
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
            composed.clone(),
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
                covered: Some(1_000),
            })
        );

        composed.publish_admission(ReadinessState::ready(Some(4_200)));
        let unavailable =
            IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
                .await
                .err()
                .ok_or("a writer that is not parked must answer from its live fence")?;
        assert_eq!(unavailable.code(), Code::Unavailable);
        Ok(())
    }

    /// A refusing admission state hides the probe from `/readyz`, but writer
    /// status still reports the probe's upstream-health observation.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_status_reports_upstream_not_ready_while_admission_is_syncing()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = shared_control_fixture().await?;
        let visible_height = fixture
            .canonical
            .chain_epoch()
            .await?
            .chain_epoch
            .visible_tip_height
            .value();
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        composed.publish_admission(
            ReadinessState::syncing(Some(1), Some(visible_height), None)
                .with_phase(IngestPhase::FollowingTip),
        );
        composed.publish_input(
            ReadinessInput::NodeSource,
            ReadinessCause::UpstreamNotReady(UpstreamNotReadyDetail {
                upstream_committed_height: Some(visible_height.saturating_add(5)),
                upstream_estimated_height: Some(visible_height.saturating_add(900)),
                upstream_verification_progress: Some(0.4),
                upstream_health: UpstreamHealth {
                    source: std::borrow::Cow::Borrowed("zebra_ready_endpoint"),
                    reason: std::borrow::Cow::Borrowed("syncing"),
                },
            }),
        );
        assert!(matches!(
            readiness.report().cause,
            ReadinessCause::Syncing { .. }
        ));
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
            composed,
        );

        let response = IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(
            response
                .upstream_not_ready
                .as_ref()
                .and_then(|detail| detail.upstream_committed_height),
            Some(visible_height.saturating_add(5)),
            "writer status omitted the probe's upstream_not_ready observation"
        );
        assert_eq!(response.phase(), WriterPhase::FollowingTip);
        assert_eq!(response.gap_blocks, Some(5));
        assert_eq!(
            response
                .chain_view
                .and_then(|view| view.upstream_tip)
                .and_then(|tip| tip.estimated_height),
            Some(visible_height.saturating_add(900))
        );

        fixture.shutdown().await
    }

    /// A `/ready` not-ready report carries no heights, so writer status
    /// reports the node tip the follower observed instead of no upstream tip.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_status_reports_the_followed_node_tip_beside_a_heightless_probe_report()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = shared_control_fixture().await?;
        let visible_height = fixture
            .canonical
            .chain_epoch()
            .await?
            .chain_epoch
            .visible_tip_height
            .value();
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        composed.publish_admission(
            ReadinessState::ready_with_target(Some(visible_height), Some(visible_height))
                .with_phase(IngestPhase::FollowingTip),
        );
        composed.publish_input(
            ReadinessInput::NodeSource,
            ReadinessCause::UpstreamNotReady(UpstreamNotReadyDetail {
                upstream_committed_height: None,
                upstream_estimated_height: None,
                upstream_verification_progress: None,
                upstream_health: UpstreamHealth {
                    source: std::borrow::Cow::Borrowed("zebra_ready_endpoint"),
                    reason: std::borrow::Cow::Borrowed("tip_age=1800s"),
                },
            }),
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
            composed,
        );

        let response = IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(
            response
                .upstream_not_ready
                .as_ref()
                .map(|detail| detail.upstream_health_reason.as_str()),
            Some("tip_age=1800s")
        );
        assert_eq!(
            response
                .chain_view
                .and_then(|view| view.upstream_tip)
                .and_then(|tip| tip.committed_height),
            Some(visible_height),
            "the follower's observed node tip must stand in for a heightless report"
        );
        assert_eq!(response.gap_blocks, Some(0));

        fixture.shutdown().await
    }

    /// Builds the ingest-control adapter over `composed` for a writer-status
    /// test.
    fn writer_status_adapter(
        fixture: &SharedControlFixture,
        composed: ComposedReadiness,
    ) -> Result<CanonicalIngestControlGrpcAdapter, Box<dyn std::error::Error>> {
        let source: Arc<dyn NodeSource> = Arc::new(ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashTestnet,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?);
        Ok(CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            fixture.canonical.clone(),
            fixture.mempool.clone(),
            source,
            composed,
        ))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_health_observation_withdraws_the_writer_sync_report()
    -> Result<(), Box<dyn std::error::Error>> {
        use zinder_testkit::{JsonRpcTestServer, RpcReply, method};

        let (progress, reports) = tokio::sync::watch::channel(Some(0.5));
        let (requests, mut seen_requests) = tokio::sync::watch::channel(0_u64);
        let server =
            JsonRpcTestServer::start([method("getblockchaininfo").reply_with(move |_| {
                let progress = reports.borrow();
                requests.send_modify(|count| *count += 1);
                progress.map_or_else(
                    || RpcReply::error("node temporarily unavailable"),
                    |progress| {
                        RpcReply::result(serde_json::json!({
                            "blocks": 100, "estimatedheight": 100, "verificationprogress": progress,
                        }))
                    },
                )
            })])?;
        let source = ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashTestnet,
            server.url(),
            NodeAuth::None,
            Duration::from_secs(1),
        )?;
        let fixture = shared_control_fixture().await?;
        let composed = ComposedReadiness::new(Readiness::default(), &[ReadinessInput::NodeSource]);
        composed.publish_admission(
            ReadinessState::syncing(Some(5), Some(95), Some(100))
                .with_phase(IngestPhase::FollowingTip),
        );
        let adapter = writer_status_adapter(&fixture, composed.clone())?;
        let cancel = CancellationToken::new();
        let probe = crate::spawn_upstream_health_probe_task(
            Arc::new(source),
            composed.clone(),
            Duration::from_millis(5),
            None,
            cancel.clone(),
        );
        for (next_progress, expected_input) in [
            (Some(0.5), "upstream_not_ready"),
            (None, "node_unavailable"),
            (Some(0.5), "upstream_not_ready"),
            (Some(1.0), "ready"),
        ] {
            progress.send(next_progress)?;
            // The second request starts only after the probe published the first reply.
            let second_request = *seen_requests.borrow() + 2;
            tokio::time::timeout(
                Duration::from_secs(2),
                seen_requests.wait_for(|n| *n >= second_request),
            )
            .await??;
            let status =
                IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
                    .await?
                    .into_inner();
            assert_eq!(
                status.upstream_not_ready.is_some(),
                expected_input == "upstream_not_ready"
            );
            let report = composed.runtime_readiness().report();
            assert_eq!(
                report.cause,
                ReadinessCause::Syncing {
                    lag_blocks: Some(5),
                    wait_reason: None,
                }
            );
            assert_eq!(
                (report.current_height, report.target_height),
                (Some(95), Some(100))
            );
            assert_eq!(
                composed
                    .input_cause(ReadinessInput::NodeSource)
                    .map(|cause| cause.metric_label()),
                Some(expected_input)
            );
        }
        cancel.cancel();
        probe.await?;
        fixture.shutdown().await
    }

    /// Writer status reports the newer of the probe's and the follower's tip.
    ///
    /// The follower can observe a newer tip after an outage and before the
    /// next health probe completes.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_status_never_reports_a_probe_tip_below_the_followed_one()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = shared_control_fixture().await?;
        let visible_height = fixture
            .canonical
            .chain_epoch()
            .await?
            .chain_epoch
            .visible_tip_height
            .value();
        // `[node.health]` makes the probe a required input.
        let composed = ComposedReadiness::new(Readiness::default(), &[ReadinessInput::NodeSource]);
        // A quiet chain: the probe's last report before the outage names the
        // served tip.
        composed.publish_input(
            ReadinessInput::NodeSource,
            ReadinessCause::UpstreamNotReady(UpstreamNotReadyDetail {
                upstream_committed_height: Some(visible_height),
                upstream_estimated_height: Some(visible_height.saturating_add(20)),
                upstream_verification_progress: Some(0.99),
                upstream_health: UpstreamHealth {
                    source: std::borrow::Cow::Borrowed("verification_progress_fallback"),
                    reason: std::borrow::Cow::Borrowed("estimated_gap_above_floor"),
                },
            }),
        );
        let adapter = writer_status_adapter(&fixture, composed.clone())?;
        let upstream_tip = async || -> Result<_, Box<dyn std::error::Error>> {
            let response =
                IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
                    .await?
                    .into_inner();
            Ok((
                response.chain_view.and_then(|view| view.upstream_tip),
                response.gap_blocks,
            ))
        };

        // The node goes down, and the failed probes keep that report.
        composed.publish_admission(
            ReadinessState::node_unavailable_with_detail(
                zinder_runtime::NodeUnavailableDetail::first_iteration(
                    "connection",
                    "connection refused",
                ),
                Some(visible_height),
            )
            .with_phase(IngestPhase::FollowingTip),
        );
        assert_eq!(upstream_tip().await?.0, None);

        // The node comes back 90 blocks ahead of the served tip before the
        // probe's next report.
        composed.publish_admission(
            ReadinessState::syncing(
                Some(90),
                Some(visible_height),
                Some(visible_height.saturating_add(90)),
            )
            .with_phase(IngestPhase::FollowingTip),
        );
        let (upstream_tip, gap_blocks) = upstream_tip().await?;
        assert_eq!(
            upstream_tip.and_then(|tip| tip.committed_height),
            Some(visible_height.saturating_add(90))
        );
        assert_eq!(
            upstream_tip.and_then(|tip| tip.estimated_height),
            Some(visible_height.saturating_add(90))
        );
        assert_eq!(gap_blocks, Some(90));

        fixture.shutdown().await
    }

    /// A shutdown drain that starts during a node outage replaces the
    /// projected cause, and writer status still names no upstream tip.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_status_reports_no_upstream_tip_while_draining_through_an_outage()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = shared_control_fixture().await?;
        let visible_height = fixture
            .canonical
            .chain_epoch()
            .await?
            .chain_epoch
            .visible_tip_height
            .value();
        let composed = ComposedReadiness::new(Readiness::default(), &[]);
        composed.publish_input(
            ReadinessInput::NodeSource,
            ReadinessCause::UpstreamNotReady(UpstreamNotReadyDetail {
                upstream_committed_height: Some(visible_height),
                upstream_estimated_height: Some(visible_height.saturating_add(20)),
                upstream_verification_progress: Some(0.99),
                upstream_health: UpstreamHealth {
                    source: std::borrow::Cow::Borrowed("verification_progress_fallback"),
                    reason: std::borrow::Cow::Borrowed("estimated_gap_above_floor"),
                },
            }),
        );
        composed.publish_admission(
            ReadinessState::node_unavailable_with_detail(
                zinder_runtime::NodeUnavailableDetail::first_iteration(
                    "connection",
                    "connection refused",
                ),
                Some(visible_height),
            )
            .with_phase(IngestPhase::FollowingTip),
        );
        composed.publish_shutting_down();
        let adapter = writer_status_adapter(&fixture, composed)?;

        let response = IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(response.chain_view.and_then(|view| view.upstream_tip), None);

        fixture.shutdown().await
    }

    /// While the follower reports its node unavailable, writer status names
    /// no upstream tip, even though the health probe still holds its last
    /// `upstream_not_ready` observation from before the outage.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_status_reports_no_upstream_tip_while_the_node_is_unavailable()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = shared_control_fixture().await?;
        let visible_height = fixture
            .canonical
            .chain_epoch()
            .await?
            .chain_epoch
            .visible_tip_height
            .value();
        let readiness = Readiness::default();
        let composed = ComposedReadiness::new(readiness.clone(), &[]);
        composed.publish_input(
            ReadinessInput::NodeSource,
            ReadinessCause::UpstreamNotReady(UpstreamNotReadyDetail {
                upstream_committed_height: Some(visible_height),
                upstream_estimated_height: Some(visible_height.saturating_add(900)),
                upstream_verification_progress: Some(0.4),
                upstream_health: UpstreamHealth {
                    source: std::borrow::Cow::Borrowed("zebra_ready_endpoint"),
                    reason: std::borrow::Cow::Borrowed("syncing"),
                },
            }),
        );
        composed.publish_admission(
            ReadinessState::node_unavailable_with_detail(
                zinder_runtime::NodeUnavailableDetail::first_iteration(
                    "connection",
                    "connection refused",
                ),
                Some(visible_height),
            )
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
            fixture.canonical.clone(),
            fixture.mempool.clone(),
            source,
            composed,
        );

        let response = IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {}))
            .await?
            .into_inner();
        assert_eq!(
            response.chain_view.and_then(|view| view.upstream_tip),
            None,
            "an unreachable node has no tip to report"
        );
        assert_eq!(response.gap_blocks, None);

        fixture.shutdown().await
    }

    /// Fresh construction has no published epoch and does not service the
    /// command queue, so `WriterStatus` answers promptly from readiness with
    /// the phase and the probe's observation and without a chain view.
    #[tokio::test(flavor = "multi_thread")]
    async fn writer_status_answers_from_readiness_during_fresh_construction()
    -> Result<(), Box<dyn std::error::Error>> {
        let (canonical, _unserviced_commands) = canonical_control_channel();
        let composed = ComposedReadiness::new(Readiness::default(), &[]);
        composed.publish_admission(
            ReadinessState::syncing(Some(4_000), None, Some(4_000))
                .with_phase(IngestPhase::BulkCatchup),
        );
        composed.publish_input(
            ReadinessInput::NodeSource,
            ReadinessCause::UpstreamNotReady(UpstreamNotReadyDetail {
                upstream_committed_height: Some(4_000),
                upstream_estimated_height: Some(9_000),
                upstream_verification_progress: Some(0.4),
                upstream_health: UpstreamHealth {
                    source: std::borrow::Cow::Borrowed("zebra_ready_endpoint"),
                    reason: std::borrow::Cow::Borrowed("syncing"),
                },
            }),
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
            composed,
        );

        let response = tokio::time::timeout(
            Duration::from_secs(5),
            IngestControl::writer_status(&adapter, Request::new(WriterStatusRequest {})),
        )
        .await
        .map_err(|_| "writer status waited on the unserviced construction queue")??
        .into_inner();
        assert_eq!(response.phase(), WriterPhase::BulkCatchup);
        assert_eq!(response.chain_view, None);
        assert_eq!(response.gap_blocks, None);
        assert_eq!(
            response.upstream_not_ready.and_then(|detail| detail
                .upstream_estimated_height
                .zip(detail.upstream_committed_height)),
            Some((9_000, 4_000))
        );
        Ok(())
    }

    #[derive(Clone)]
    struct StaticValuePoolSource {
        capabilities: NodeCapabilities,
        value_pools: ChainValuePools,
        rate_limited: bool,
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
            if self.rate_limited {
                return Err(SourceError::NodeRateLimited {
                    operation: "getblockchaininfo",
                });
            }
            Ok(self.value_pools.clone())
        }
    }

    /// Regression: a `Mined` change for the next block that reaches the owner
    /// before the tip change must not turn into a proven absence through
    /// `MempoolTransaction`.
    #[tokio::test(flavor = "multi_thread")]
    async fn mined_change_beyond_the_tip_is_never_a_proven_absence()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut fixture = shared_control_fixture().await?;
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;

        // A miss in a coherent generation is a typed absence at the writer epoch.
        let coherent_miss = fixture
            .ingest_client
            .mempool_transaction(authenticated(MempoolTransactionRequest {
                transaction_id: encode_rpc_transaction_id_hex(TransactionId::from_bytes(
                    [0xF5; 32],
                )),
            }))
            .await
            .err()
            .ok_or("an unknown transaction must miss the live mempool")?;
        assert_eq!(
            TransactionAbsence::from_status(&coherent_miss)?,
            Some(TransactionAbsence::at_epoch(chain_epoch, true))
        );

        let entry = fixture_mempool_entry_with_id(0xF6, chain_epoch)?;
        let transaction_id = entry.transaction_id();
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Added { entry },
                UnixTimestampMillis::new(1_750_000_000_500),
            )
            .await?;
        let next_height = BlockHeight::new(chain_epoch.visible_tip_height.value() + 1);
        let outcome = fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Mined {
                    transaction_id,
                    mined_height: next_height,
                    block_hash: BlockHash::from_bytes([0xF7; 32]),
                },
                UnixTimestampMillis::new(1_750_000_000_600),
            )
            .await?;
        assert!(
            outcome.is_some(),
            "a Mined change beyond the tip ends the source generation"
        );

        // The removal is not applied, so the serving generation keeps the
        // entry at its own tip instead of claiming an absence.
        let writer_hit = fixture
            .ingest_client
            .mempool_transaction(authenticated(MempoolTransactionRequest {
                transaction_id: encode_rpc_transaction_id_hex(transaction_id),
            }))
            .await?
            .into_inner();
        assert!(matches!(
            writer_hit.location.and_then(|location| location.location),
            Some(zinder_proto::v1::wallet::transaction_location::Location::InMempool(_))
        ));
        fixture.shutdown().await
    }

    /// Oversized raw entries fail deterministically with `UNSUPPORTED_WALLET_ENCODING`.
    ///
    /// Preceding entries that fit within the page budget still stream before
    /// the oversized page is reached over the default 4 MiB tonic channel.
    #[tokio::test(flavor = "multi_thread")]
    async fn mempool_snapshot_rejects_oversized_raw_entry_with_typed_reason()
    -> Result<(), Box<dyn std::error::Error>> {
        use prost::Message as _;
        use tonic_types::StatusExt as _;

        let mut fixture = shared_control_fixture().await?;
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        let oversized = MempoolEntry::new(
            TransactionId::from_bytes([0xFE; 32]),
            None,
            RawTransactionBytes::new(vec![0xAB; 4 * 1_024 * 1_024 + 65_536]),
            CompactTransactionData::default(),
            MempoolObservation {
                first_seen_unix_millis: UnixTimestampMillis::new(1_750_000_000_900),
                first_seen_chain_epoch: chain_epoch,
            },
        )?;
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Added { entry: oversized },
                UnixTimestampMillis::new(1_750_000_000_900),
            )
            .await?;

        let first_page = fixture
            .ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 1_024,
                from_cursor: Vec::new(),
            }))
            .await?
            .into_inner();
        assert_eq!(first_page.entries.len(), 1);
        assert!(first_page.encoded_len() <= super::MAX_MEMPOOL_SNAPSHOT_RESPONSE_BYTES);
        assert_ne!(first_page.next_cursor, [] as [u8; 0]);

        let second_outcome = fixture
            .ingest_client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 1_024,
                from_cursor: first_page.next_cursor,
            }))
            .await;
        let Err(status) = second_outcome else {
            return Err("oversized >4 MiB entry must fail deterministically".into());
        };
        assert_eq!(status.code(), Code::Unavailable, "{status:?}");
        let details = status.get_error_details();
        let error_detail = details
            .error_info()
            .ok_or("oversized mempool entry status must carry google.rpc.ErrorInfo")?;
        assert_eq!(error_detail.domain, zinder_proto::ZINDER_ERROR_DOMAIN);
        assert_eq!(
            error_detail.reason,
            zinder_proto::v1::ops::ErrorReason::UnsupportedWalletEncoding.as_str_name()
        );
        fixture.shutdown().await
    }

    /// Restored owners advance their applied anchor across pruned additions.
    ///
    /// When retention prunes an `Added` event before a retained `Mined` event,
    /// reconciling unchanged active entries must still pin multi-page walks.
    #[tokio::test(flavor = "multi_thread")]
    async fn restored_owner_after_pruned_add_serves_multi_page_snapshots_without_cursor_expiry()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, commands) = canonical_control_channel();
        let parked = Arc::new(parking_lot::Mutex::new(None));
        let command_task = spawn_fixture_owner_loop(store, commands, parked);
        let chain_epoch = canonical.chain_epoch().await?.chain_epoch;
        let source_tip = BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash);

        let (entry_b, entry_c) = seed_pruned_mempool_history(&canonical, source_tip).await?;
        let mut expected_ids = [entry_b.transaction_id, entry_c.transaction_id];
        expected_ids.sort();

        let (second_source, second_feed) = zinder_testkit::MockMempoolSource::streaming();
        let restored_owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = crate::mempool_ready_channel();
        let owner_cancel = CancellationToken::new();
        let owner_task = tokio::spawn(crate::run_live_mempool_owner(
            Arc::new(second_source),
            canonical.clone(),
            restored_owner.clone(),
            ready_signal,
            owner_cancel.clone(),
        ));
        wait_for_condition(|| second_feed.open_count() >= 1).await?;
        second_feed.push_added(entry_b)?;
        second_feed.push_added(entry_c)?;
        second_feed.complete_initial_snapshot(source_tip)?;
        wait_for_condition(|| ready_gate.admits_canonical_tip(source_tip)).await?;

        let (mut client, server_cancel, server_task) =
            serve_ingest_control_for_owner(canonical.clone(), restored_owner).await?;
        let first_page = client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: Vec::new(),
            }))
            .await?
            .into_inner();
        assert_eq!(first_page.entries.len(), 1);
        assert_eq!(
            first_page.entries[0].transaction_id,
            encode_rpc_transaction_id_hex(expected_ids[0])
        );
        assert_ne!(first_page.next_cursor, [] as [u8; 0]);

        let late_entry = synthetic_mempool_source_entry(4, 1_750_000_000_500)?;
        let late_tx_id = late_entry.transaction_id;
        second_feed.push_added(late_entry)?;
        wait_for_live_mempool_entry(&mut client, late_tx_id).await?;

        let second_page = client
            .mempool_snapshot(authenticated(MempoolSnapshotRequest {
                max_entries: 10,
                from_cursor: first_page.next_cursor,
            }))
            .await?
            .into_inner();
        assert_eq!(second_page.entries.len(), 1);
        assert_eq!(
            second_page.entries[0].transaction_id,
            encode_rpc_transaction_id_hex(expected_ids[1])
        );
        assert_eq!(second_page.next_cursor, [] as [u8; 0]);

        drop(client);
        server_cancel.cancel();
        server_task.await??;
        owner_cancel.cancel();
        owner_task.await?;
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    async fn seed_pruned_mempool_history(
        canonical: &crate::CanonicalControlHandle,
        source_tip: BlockId,
    ) -> Result<
        (
            zinder_source::MempoolSourceEntry,
            zinder_source::MempoolSourceEntry,
        ),
        Box<dyn std::error::Error>,
    > {
        let (first_source, first_feed) = zinder_testkit::MockMempoolSource::streaming();
        let first_owner = LiveMempoolOwner::default();
        let (ready_signal, ready_gate) = crate::mempool_ready_channel();
        let first_cancel = CancellationToken::new();
        let first_task = tokio::spawn(crate::run_live_mempool_owner(
            Arc::new(first_source),
            canonical.clone(),
            first_owner.clone(),
            ready_signal,
            first_cancel.clone(),
        ));
        wait_for_condition(|| first_feed.open_count() >= 1).await?;
        first_feed.complete_initial_snapshot(source_tip)?;
        wait_for_condition(|| ready_gate.admits_canonical_tip(source_tip)).await?;

        let entry_a = synthetic_mempool_source_entry(1, 1_750_000_000_100)?;
        let entry_b = synthetic_mempool_source_entry(2, 1_750_000_000_200)?;
        let entry_c = synthetic_mempool_source_entry(3, 1_750_000_000_300)?;
        let mined_tx_id = entry_a.transaction_id;
        first_feed.push_added(entry_a)?;
        first_feed.push_added(entry_b.clone())?;
        first_feed.push_added(entry_c.clone())?;
        first_feed.push_mined(mined_tx_id, source_tip.height, source_tip.hash)?;
        wait_for_durable_sequence(canonical, 4).await?;

        let retention = MempoolEventRetentionConfig::new(Some(Duration::from_millis(1)), None);
        let budget = zinder_store::MempoolEventRetentionStepBudget::new(
            NonZeroU32::new(8).ok_or("retention event budget must be nonzero")?,
            std::num::NonZeroU64::new(1_000_000).ok_or("retention byte budget must be nonzero")?,
        );
        let prune_now =
            UnixTimestampMillis::new(UnixTimestampMillis::now().value().saturating_add(10_000));
        let prune_outcome = first_owner
            .prune_events(canonical, prune_now, retention, budget)
            .await?;
        assert_eq!(prune_outcome.report.oldest_retained_sequence, Some(2));
        assert_eq!(prune_outcome.report.pruned_added_count, 1);

        first_cancel.cancel();
        first_task.await?;
        Ok((entry_b, entry_c))
    }

    async fn serve_ingest_control_for_owner(
        canonical: crate::CanonicalControlHandle,
        owner: LiveMempoolOwner,
    ) -> Result<
        (
            IngestControlClient<tonic::transport::Channel>,
            CancellationToken,
            tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let bearer_token = BearerToken::from_str("fixture-control-token")?;
        let node_source: Arc<dyn NodeSource> = Arc::new(ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashTestnet,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?);
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        readiness.publish_admission(ReadinessState::ready(Some(1)));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?;
        let server_cancel = CancellationToken::new();
        let shutdown_token = server_cancel.clone();
        let adapter = CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            canonical,
            owner,
            node_source,
            readiness,
        )
        .with_bearer_token(Some(bearer_token));
        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(adapter.into_server())
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown_token.cancelled_owned(),
                )
                .await
        });
        let client = IngestControlClient::connect(format!("http://{listen_addr}")).await?;
        Ok((client, server_cancel, server_task))
    }

    fn synthetic_mempool_source_entry(
        transaction_nonce: u32,
        observed_at_millis: u64,
    ) -> Result<zinder_source::MempoolSourceEntry, Box<dyn std::error::Error>> {
        use zebra_chain::{
            serialization::ZcashDeserializeInto as _, transaction::Transaction as ZebraTransaction,
        };

        let mut raw_bytes = Vec::with_capacity(27);
        raw_bytes.extend_from_slice(&0x8000_0004_u32.to_le_bytes());
        raw_bytes.extend_from_slice(&0x892F_2085_u32.to_le_bytes());
        raw_bytes.extend_from_slice(&[0, 0]);
        raw_bytes.extend_from_slice(&transaction_nonce.to_le_bytes());
        raw_bytes.extend_from_slice(&0_u32.to_le_bytes());
        raw_bytes.extend_from_slice(&0_i64.to_le_bytes());
        raw_bytes.extend_from_slice(&[0, 0, 0]);
        let parsed: ZebraTransaction = raw_bytes.as_slice().zcash_deserialize_into()?;
        Ok(zinder_source::MempoolSourceEntry {
            transaction_id: TransactionId::from_bytes(parsed.hash().0),
            auth_digest: parsed
                .auth_digest()
                .map(|digest| zinder_core::AuthDigest::from_bytes(digest.0)),
            raw_transaction_bytes: RawTransactionBytes::new(raw_bytes),
            observed_at_unix_millis: UnixTimestampMillis::new(observed_at_millis),
        })
    }

    async fn wait_for_durable_sequence(
        canonical: &crate::CanonicalControlHandle,
        expected_sequence: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let page_size = NonZeroU32::new(16).ok_or("mempool page size must be nonzero")?;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let page = canonical.mempool_event_page(None, page_size).await?;
            if page
                .last()
                .is_some_and(|envelope| envelope.event_sequence >= expected_sequence)
            {
                return Ok(());
            }
            if std::time::Instant::now() > deadline {
                return Err("timed out waiting for durable mempool event sequence".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_live_mempool_entry(
        client: &mut IngestControlClient<tonic::transport::Channel>,
        expected_tx_id: TransactionId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tx_hex = encode_rpc_transaction_id_hex(expected_tx_id);
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if client
                .mempool_transaction(authenticated(MempoolTransactionRequest {
                    transaction_id: tx_hex.clone(),
                }))
                .await
                .is_ok()
            {
                return Ok(());
            }
            if std::time::Instant::now() > deadline {
                return Err("timed out waiting for late entry in live mempool index".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_condition(
        mut predicate: impl FnMut() -> bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !predicate() {
            if std::time::Instant::now() > deadline {
                return Err("timed out waiting for mempool owner state".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    struct SharedControlFixture {
        temporary: tempfile::TempDir,
        canonical_client: CanonicalControlClient<tonic::transport::Channel>,
        ingest_client: IngestControlClient<tonic::transport::Channel>,
        canonical: crate::CanonicalControlHandle,
        mempool: LiveMempoolOwner,
        /// Switches the fixture's owner loop to the parked writer's dispatch.
        parked: Arc<parking_lot::Mutex<Option<ParkedWriterHalt>>>,
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

    /// Serves owner commands like the follower, switching to the parked
    /// writer's dispatch once `parked` holds a halt.
    fn spawn_fixture_owner_loop(
        mut store: zinder_store::RocksDbCanonicalStore,
        mut commands: tokio::sync::mpsc::Receiver<crate::writer::control::CanonicalControlCommand>,
        parked: Arc<parking_lot::Mutex<Option<ParkedWriterHalt>>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(command) = commands.recv().await {
                let halt = *parked.lock();
                match halt {
                    Some(halt) => apply_parked_canonical_control_command(&mut store, command, halt),
                    None => {
                        let _scheduling = apply_canonical_control_command(&mut store, command);
                    }
                }
            }
        })
    }

    async fn shared_control_fixture() -> Result<SharedControlFixture, Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let checkpoint_staging_root = temporary.path().join("checkpoint-staging");
        fs::create_dir(&checkpoint_staging_root)?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, commands) = canonical_control_channel();
        let parked = Arc::new(parking_lot::Mutex::new(None));
        let command_task = spawn_fixture_owner_loop(store, commands, Arc::clone(&parked));

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
        let endpoint = crate::mempool_ready_channel().0.observe_node_endpoint();
        let _certification = owner
            .try_complete_hydration(&canonical, source_tip, &endpoint)
            .await?;

        let bearer_token = BearerToken::from_str("fixture-control-token")?;
        let node_source: Arc<dyn NodeSource> = Arc::new(ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashTestnet,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?);
        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        readiness.publish_admission(ReadinessState::ready(Some(1)));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let canonical_adapter = CanonicalControlGrpcAdapter::new(
            canonical.clone(),
            CanonicalCheckpointStagingRoot::new(checkpoint_staging_root),
            RocksDbResourceBudget::for_local_tests(),
            crate::CanonicalStoreDamageSignal::default(),
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
            parked,
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

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct IdleMempoolStats {
        mempool_event_page_calls: u64,
        event_row_reads: u64,
        envelope_decodes: u64,
        rocksdb_keys_read: u64,
        rocksdb_bytes_read: u64,
    }

    async fn read_idle_mempool_stats(
        stats_tx: &tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<IdleMempoolStats>>,
    ) -> Result<IdleMempoolStats, Box<dyn std::error::Error>> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        stats_tx.send(reply_tx).await?;
        Ok(reply_rx.await?)
    }

    fn spawn_instrumented_owner_loop(
        mut store: zinder_store::RocksDbCanonicalStore,
        mut commands: tokio::sync::mpsc::Receiver<crate::writer::control::CanonicalControlCommand>,
        mut stats_rx: tokio::sync::mpsc::Receiver<tokio::sync::oneshot::Sender<IdleMempoolStats>>,
    ) -> tokio::task::JoinHandle<()> {
        use rust_rocksdb::statistics::Ticker;

        tokio::spawn(async move {
            let mut mempool_event_page_calls = 0_u64;
            loop {
                tokio::select! {
                    maybe_command = commands.recv() => {
                        let Some(command) = maybe_command else {
                            return;
                        };
                        if matches!(
                            &command,
                            crate::writer::control::CanonicalControlCommand::MempoolEventPage { .. }
                        ) {
                            mempool_event_page_calls = mempool_event_page_calls.saturating_add(1);
                        }
                        let _scheduling = apply_canonical_control_command(&mut store, command);
                    }
                    maybe_stats = stats_rx.recv() => {
                        let Some(reply_tx) = maybe_stats else {
                            return;
                        };
                        let counts = store.mempool_event_read_counts();
                        let _sent = reply_tx.send(IdleMempoolStats {
                            mempool_event_page_calls,
                            event_row_reads: counts.event_row_reads,
                            envelope_decodes: counts.envelope_decodes,
                            rocksdb_keys_read: store.rocksdb_ticker_count(Ticker::NumberKeysRead),
                            rocksdb_bytes_read: store.rocksdb_ticker_count(Ticker::BytesRead),
                        });
                    }
                }
            }
        })
    }

    async fn advance_idle_virtual_time(duration: Duration) {
        let step = Duration::from_millis(250);
        let steps = duration.as_millis() / step.as_millis();
        for _ in 0..steps {
            tokio::time::advance(step).await;
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    #[allow(
        clippy::too_many_lines,
        reason = "verifies 1 and multiple caught-up subscribers before and after head advances"
    )]
    async fn caught_up_idle_mempool_subscribers_perform_zero_durable_reads_or_decodes()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::TempDir::new()?;
        let store = published_fixture_store(&temporary.path().join("canonical"))?;
        let (canonical, commands) = canonical_control_channel();
        let (stats_tx, stats_rx) = tokio::sync::mpsc::channel(8);
        let command_task = spawn_instrumented_owner_loop(store, commands, stats_rx);
        let owner = LiveMempoolOwner::default();
        let chain_epoch = canonical.chain_epoch().await?.chain_epoch;

        let large_entry = MempoolEntry::new(
            TransactionId::from_bytes([0x99; 32]),
            None,
            RawTransactionBytes::new(vec![0x5A; 256 * 1_024]),
            CompactTransactionData::default(),
            MempoolObservation {
                first_seen_unix_millis: UnixTimestampMillis::new(1_750_000_000_100),
                first_seen_chain_epoch: chain_epoch,
            },
        )?;
        owner
            .apply_event(
                &canonical,
                MempoolEvent::Added { entry: large_entry },
                UnixTimestampMillis::new(1_750_000_000_100),
            )
            .await?;
        let source_tip = BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash);
        let endpoint = crate::mempool_ready_channel().0.observe_node_endpoint();
        owner
            .try_complete_hydration(&canonical, source_tip, &endpoint)
            .await?;

        let readiness = ComposedReadiness::new(Readiness::default(), &[]);
        readiness.publish_admission(ReadinessState::ready(Some(1)));
        let node_source: Arc<dyn NodeSource> = Arc::new(ZebraJsonRpcSource::new(
            zinder_core::Network::ZcashTestnet,
            "http://127.0.0.1:1",
            NodeAuth::None,
            Duration::from_secs(1),
        )?);
        let adapter = CanonicalIngestControlGrpcAdapter::new(
            zinder_core::Network::ZcashTestnet,
            canonical.clone(),
            owner.clone(),
            node_source,
            readiness,
        );

        let subscribe = |position| {
            IngestControl::mempool_events(
                &adapter,
                Request::new(MempoolEventsRequest {
                    start: Some(event_stream_start_message(&position)),
                }),
            )
        };

        // 1 caught-up idle subscriber incurs zero reads/decodes while idle.
        let single_tail = subscribe(EventStreamStartPosition::LiveTail)
            .await?
            .into_inner();
        tokio::task::yield_now().await;
        let single_baseline = read_idle_mempool_stats(&stats_tx).await?;
        advance_idle_virtual_time(Duration::from_secs(5)).await;
        assert_eq!(read_idle_mempool_stats(&stats_tx).await?, single_baseline);

        // Scale to 8 concurrent caught-up subscribers (LiveTail, AfterCursor, drained EarliestRetained).
        let mut drained = subscribe(EventStreamStartPosition::EarliestRetained)
            .await?
            .into_inner();
        let first_event = drained
            .next()
            .await
            .ok_or("EarliestRetained omitted initial event")??;
        let head_cursor = zinder_store::StreamCursorTokenV1::from_bytes(first_event.cursor);
        let mut subscribers = vec![single_tail, drained];
        for _ in 0..3 {
            subscribers.push(
                subscribe(EventStreamStartPosition::LiveTail)
                    .await?
                    .into_inner(),
            );
        }
        for _ in 0..3 {
            subscribers.push(
                subscribe(EventStreamStartPosition::AfterCursor(head_cursor.clone()))
                    .await?
                    .into_inner(),
            );
        }
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        let multi_baseline = read_idle_mempool_stats(&stats_tx).await?;
        advance_idle_virtual_time(Duration::from_secs(5)).await;
        assert_eq!(read_idle_mempool_stats(&stats_tx).await?, multi_baseline);

        // Advancing the head wakes all 8 subscribers immediately with strict order.
        let next_entry = fixture_mempool_entry_with_id(0xB7, chain_epoch)?;
        let next_txid = next_entry.transaction_id();
        owner
            .apply_event(
                &canonical,
                MempoolEvent::Added { entry: next_entry },
                UnixTimestampMillis::new(1_750_000_000_200),
            )
            .await?;
        owner
            .apply_event(
                &canonical,
                MempoolEvent::Mined {
                    transaction_id: next_txid,
                    mined_height: chain_epoch.visible_tip_height,
                    block_hash: chain_epoch.visible_tip_hash,
                },
                UnixTimestampMillis::new(1_750_000_000_300),
            )
            .await?;
        owner
            .apply_event(
                &canonical,
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([0x99; 32]),
                    reason: zinder_core::MempoolEvictionReason::Expired,
                },
                UnixTimestampMillis::new(1_750_000_000_400),
            )
            .await?;

        for subscriber in &mut subscribers {
            let mut observed_sequences = Vec::with_capacity(3);
            for _ in 0..3 {
                let envelope = tokio::time::timeout(Duration::from_millis(200), subscriber.next())
                    .await?
                    .ok_or("caught-up subscriber ended prematurely")??;
                observed_sequences.push(envelope.event_sequence);
            }
            assert_eq!(observed_sequences, vec![2, 3, 4]);
        }

        // Remaining idle subscribers stay at zero reads/decodes after catch-up.
        subscribers.truncate(6);
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        let post_advance_baseline = read_idle_mempool_stats(&stats_tx).await?;
        advance_idle_virtual_time(Duration::from_secs(5)).await;
        assert_eq!(
            read_idle_mempool_stats(&stats_tx).await?,
            post_advance_baseline
        );

        drop(subscribers);
        command_task.abort();
        let _ = command_task.await;
        Ok(())
    }

    fn unique_mempool_entry(
        nonce: u32,
        raw_bytes_len: usize,
        chain_epoch: ChainEpoch,
    ) -> Result<MempoolEntry, zinder_core::MempoolEntryBuildError> {
        let mut txid_bytes = [0_u8; 32];
        txid_bytes[..4].copy_from_slice(&nonce.to_le_bytes());
        txid_bytes[31] = 0x7E;
        MempoolEntry::new(
            TransactionId::from_bytes(txid_bytes),
            None,
            RawTransactionBytes::new(vec![0x55; raw_bytes_len]),
            CompactTransactionData::default(),
            MempoolObservation {
                first_seen_unix_millis: UnixTimestampMillis::new(1_750_000_000_100),
                first_seen_chain_epoch: chain_epoch,
            },
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    #[allow(
        clippy::too_many_lines,
        reason = "exercises replay-to-live ordering, slow consumers, expiry, and owner withdrawal"
    )]
    async fn mempool_stream_handles_replay_to_live_ordering_slow_consumers_expiry_and_withdrawal()
    -> Result<(), Box<dyn std::error::Error>> {
        use tonic_types::StatusExt as _;

        let mut fixture = shared_control_fixture().await?;
        let chain_epoch = fixture.canonical.chain_epoch().await?.chain_epoch;
        let expired_at = UnixTimestampMillis::new(1_750_000_000_100);

        // Seed 69 more durable events (70 total, spanning >1 page of 64 events)
        // using 8 KiB Added payloads so paused HTTP/2 consumers hit backpressure
        // inside page 1.
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Invalidated {
                    transaction_id: TransactionId::from_bytes([0xA1; 32]),
                    reason: zinder_core::MempoolEvictionReason::Expired,
                },
                expired_at,
            )
            .await?;
        for idx in 1_u32..=34 {
            let entry = unique_mempool_entry(idx, 8 * 1_024, chain_epoch)?;
            fixture
                .mempool
                .apply_event(
                    &fixture.canonical,
                    MempoolEvent::Added { entry },
                    expired_at,
                )
                .await?;
        }
        for idx in 1_u32..=34 {
            let entry = unique_mempool_entry(idx, 8, chain_epoch)?;
            let event = if idx % 2 == 0 {
                MempoolEvent::Invalidated {
                    transaction_id: entry.transaction_id(),
                    reason: zinder_core::MempoolEvictionReason::Expired,
                }
            } else {
                MempoolEvent::Mined {
                    transaction_id: entry.transaction_id(),
                    mined_height: chain_epoch.visible_tip_height,
                    block_hash: chain_epoch.visible_tip_hash,
                }
            };
            fixture
                .mempool
                .apply_event(&fixture.canonical, event, expired_at)
                .await?;
        }

        let mut replay_to_live = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::EarliestRetained,
                )),
            }))
            .await?
            .into_inner();
        let mut fast_live = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::LiveTail,
                )),
            }))
            .await?
            .into_inner();
        let mut slow_live = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::LiveTail,
                )),
            }))
            .await?
            .into_inner();
        // Direct adapter streams hit the bounded mpsc::channel(16) directly without
        // HTTP/2 or loopback socket buffering.
        let direct_adapter =
            writer_status_adapter(&fixture, ComposedReadiness::new(Readiness::default(), &[]))?;
        let mut channel_backpressured_slow_live = IngestControl::mempool_events(
            &direct_adapter,
            Request::new(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::LiveTail,
                )),
            }),
        )
        .await?
        .into_inner();
        // Paused at EarliestRetained so mpsc::channel(16) stops it inside page 1
        // (seqs 1..=64) before requesting page 2 (after_cursor = seq 64).
        let mut paused_for_expiry = IngestControl::mempool_events(
            &direct_adapter,
            Request::new(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::EarliestRetained,
                )),
            }),
        )
        .await?
        .into_inner();

        // Advance the head across replay-to-live (events 71..=100, >16 channel capacity).
        for idx in 101_u32..=114 {
            let entry = unique_mempool_entry(idx, 8 * 1_024, chain_epoch)?;
            fixture
                .mempool
                .apply_event(
                    &fixture.canonical,
                    MempoolEvent::Added { entry },
                    expired_at,
                )
                .await?;
        }
        for idx in 101_u32..=114 {
            let entry = unique_mempool_entry(idx, 8, chain_epoch)?;
            let event = if idx % 2 == 0 {
                MempoolEvent::Invalidated {
                    transaction_id: entry.transaction_id(),
                    reason: zinder_core::MempoolEvictionReason::Expired,
                }
            } else {
                MempoolEvent::Mined {
                    transaction_id: entry.transaction_id(),
                    mined_height: chain_epoch.visible_tip_height,
                    block_hash: chain_epoch.visible_tip_hash,
                }
            };
            fixture
                .mempool
                .apply_event(&fixture.canonical, event, expired_at)
                .await?;
        }
        let unexpired_entry = unique_mempool_entry(200, 8 * 1_024, chain_epoch)?;
        let unexpired_txid = unexpired_entry.transaction_id();
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Added {
                    entry: unexpired_entry,
                },
                UnixTimestampMillis::now(),
            )
            .await?;
        fixture
            .mempool
            .apply_event(
                &fixture.canonical,
                MempoolEvent::Invalidated {
                    transaction_id: unexpired_txid,
                    reason: zinder_core::MempoolEvictionReason::Expired,
                },
                UnixTimestampMillis::now(),
            )
            .await?;

        let mut cursor_1_bytes = Vec::new();
        let mut replay_sequences = Vec::with_capacity(100);
        for _ in 0..100 {
            let envelope = tokio::time::timeout(Duration::from_secs(2), replay_to_live.next())
                .await?
                .ok_or("replay_to_live ended early")??;
            if envelope.event_sequence == 1 {
                cursor_1_bytes = envelope.cursor.clone();
            }
            replay_sequences.push(envelope.event_sequence);
        }
        assert_eq!(replay_sequences, (1_u64..=100).collect::<Vec<_>>());

        let mut fast_sequences = Vec::with_capacity(30);
        for _ in 0..30 {
            let envelope = tokio::time::timeout(Duration::from_secs(2), fast_live.next())
                .await?
                .ok_or("fast_live ended early")??;
            fast_sequences.push(envelope.event_sequence);
        }
        assert_eq!(fast_sequences, (71_u64..=100).collect::<Vec<_>>());

        // Slow consumers recover independently after head moved past channel & HTTP/2 buffers.
        let mut slow_sequences = Vec::with_capacity(30);
        for _ in 0..30 {
            let envelope = tokio::time::timeout(Duration::from_secs(2), slow_live.next())
                .await?
                .ok_or("slow_live ended early")??;
            slow_sequences.push(envelope.event_sequence);
        }
        assert_eq!(slow_sequences, (71_u64..=100).collect::<Vec<_>>());
        let mut direct_slow_sequences = Vec::with_capacity(30);
        for _ in 0..30 {
            let envelope = tokio::time::timeout(
                Duration::from_secs(2),
                channel_backpressured_slow_live.next(),
            )
            .await?
            .ok_or("channel_backpressured_slow_live ended early")??;
            direct_slow_sequences.push(envelope.event_sequence);
        }
        assert_eq!(direct_slow_sequences, (71_u64..=100).collect::<Vec<_>>());

        // Prune 1..=98 so floor reaches 99 while paused_for_expiry still has seq 64 as page-2 cursor.
        let retention = MempoolEventRetentionConfig::new(
            Some(Duration::from_millis(1)),
            Some(Duration::from_millis(1)),
        );
        let budget = zinder_store::MempoolEventRetentionStepBudget::new(
            NonZeroU32::new(64).ok_or("nonzero budget")?,
            std::num::NonZeroU64::new(1_000_000).ok_or("nonzero bytes")?,
        );
        let prune_now =
            UnixTimestampMillis::new(UnixTimestampMillis::now().value().saturating_sub(500));
        loop {
            let outcome = fixture
                .mempool
                .prune_events(&fixture.canonical, prune_now, retention, budget)
                .await?;
            if !outcome.has_immediate_work() {
                assert_eq!(outcome.report.oldest_retained_sequence, Some(99));
                break;
            }
        }

        // Open-time expiry check on pruned cursor 1.
        let expired_attach = fixture
            .ingest_client
            .mempool_events(authenticated(MempoolEventsRequest {
                start: Some(event_stream_start_message(
                    &EventStreamStartPosition::AfterCursor(
                        zinder_store::StreamCursorTokenV1::from_bytes(cursor_1_bytes),
                    ),
                )),
            }))
            .await
            .err()
            .ok_or("pruned cursor attached unexpectedly")?;
        assert_eq!(expired_attach.code(), Code::FailedPrecondition);
        assert_eq!(
            expired_attach
                .get_error_details()
                .error_info()
                .map(|detail| detail.reason.as_str()),
            Some(zinder_proto::v1::ops::ErrorReason::MempoolEventCursorExpired.as_str_name())
        );

        // Mid-stream expiry once paused_for_expiry drains page 1 and advances to page 2.
        let mut saw_expiry = false;
        for _ in 0..70 {
            match tokio::time::timeout(Duration::from_secs(2), paused_for_expiry.next()).await? {
                Some(Ok(_)) => {}
                Some(Err(status)) => {
                    assert_eq!(status.code(), Code::FailedPrecondition);
                    assert_eq!(
                        status
                            .get_error_details()
                            .error_info()
                            .map(|detail| detail.reason.as_str()),
                        Some(
                            zinder_proto::v1::ops::ErrorReason::MempoolEventCursorExpired
                                .as_str_name()
                        )
                    );
                    saw_expiry = true;
                    break;
                }
                None => return Err("paused stream closed without cursor expiry".into()),
            }
        }
        assert!(saw_expiry, "expected MempoolEventCursorExpired mid-stream");

        // Owner withdrawal ends caught-up streams with NodeUnavailable.
        fixture.mempool.withdraw_for_rebuild().await;
        for stream in [&mut replay_to_live, &mut fast_live, &mut slow_live] {
            let terminal = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await?
                .ok_or("caught-up stream closed instead of reporting withdrawal")?;
            let Err(status) = terminal else {
                return Err("caught-up stream stayed open after owner withdrawal".into());
            };
            assert_eq!(status.code(), Code::Unavailable);
            assert_eq!(
                status
                    .get_error_details()
                    .error_info()
                    .map(|detail| detail.reason.as_str()),
                Some(zinder_proto::v1::ops::ErrorReason::NodeUnavailable.as_str_name())
            );
        }

        drop(replay_to_live);
        drop(fast_live);
        drop(slow_live);
        drop(channel_backpressured_slow_live);
        drop(paused_for_expiry);
        fixture.shutdown().await
    }
}
