//! In-process ingest-control identity for query-composition tests.

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio_stream::{Stream, wrappers::TcpListenerStream};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};
use zinder_core::{Network, wire::encode_zinder_native_chain_name};
use zinder_proto::{
    capabilities::{
        INGEST_CONTROL_MEMPOOL_EVENTS_V2, INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
        INGEST_CONTROL_MEMPOOL_TRANSACTION_V2, INGEST_CONTROL_SERVER_INFO_V1,
        INGEST_CONTROL_TRANSPARENT_MEMPOOL_OUTPUTS_BY_ADDRESS_V1,
        INGEST_CONTROL_TRANSPARENT_MEMPOOL_SPENDS_BY_OUTPOINT_V1, INGEST_CONTROL_WRITER_STATUS_V1,
    },
    v1::{
        ingest::{
            MempoolTransactionRequest, ServerInfoRequest, ServerInfoResponse, WriterPhase,
            WriterStatusRequest, WriterStatusResponse,
            ingest_control_server::{IngestControl, IngestControlServer},
        },
        ops, wallet,
    },
};

type VisibleChainEventsStream =
    Pin<Box<dyn Stream<Item = Result<wallet::ChainEventEnvelope, Status>> + Send>>;
type MempoolEventsStream =
    Pin<Box<dyn Stream<Item = Result<wallet::MempoolEventEnvelope, Status>> + Send>>;

/// Running ingest-control endpoint with the structural contract required by wallet queries.
pub struct IngestControlFixture {
    endpoint: String,
    service: IngestControlFixtureService,
    cancel: CancellationToken,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl IngestControlFixture {
    /// Binds an ephemeral loopback endpoint and serves the required structural identity.
    pub async fn spawn(network: Network) -> eyre::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let service = IngestControlFixtureService::new(network);
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(IngestControlServer::new(server_service))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_cancel.cancelled_owned(),
                )
                .await
        });
        Ok(Self {
            endpoint: format!("http://{listen_addr}"),
            service,
            cancel,
            server,
        })
    }

    /// Returns the authenticated-transport endpoint URL.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Controls whether both health RPCs succeed with coherent evidence.
    pub fn set_health_available(&self, health_available: bool) {
        self.service.set_health_available(health_available);
    }

    /// Controls whether the writer-status health RPC stalls without replying.
    pub fn set_health_stalled(&self, health_stalled: bool) {
        self.service.set_health_stalled(health_stalled);
    }

    /// Controls a deterministic error returned by the mempool snapshot RPC.
    pub fn set_mempool_snapshot_error(&self, snapshot_error: Option<Status>) {
        self.service.set_mempool_snapshot_error(snapshot_error);
    }

    /// Reports a canonical writer parked by a reorg deeper than its window,
    /// or clears the halt with `None`.
    pub fn set_reorg_window_exceeded(&self, halt: Option<ops::ReorgWindowExceededDetail>) {
        self.service.set_reorg_window_exceeded(halt);
    }

    /// Stops the fixture and waits for its server task.
    pub async fn shutdown(mut self) -> eyre::Result<()> {
        self.cancel.cancel();
        (&mut self.server).await??;
        Ok(())
    }
}

impl Drop for IngestControlFixture {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Cloneable tonic service for tests that compose ingest control with another service.
#[derive(Clone)]
pub struct IngestControlFixtureService {
    network: Network,
    health_available: Arc<AtomicBool>,
    health_stalled: Arc<AtomicBool>,
    snapshot_error: Arc<Mutex<Option<Status>>>,
    reorg_window_exceeded: Arc<Mutex<Option<ops::ReorgWindowExceededDetail>>>,
}

impl IngestControlFixtureService {
    /// Creates a structurally valid ingest-control service for `network`.
    #[must_use]
    pub fn new(network: Network) -> Self {
        Self {
            network,
            health_available: Arc::new(AtomicBool::new(true)),
            health_stalled: Arc::new(AtomicBool::new(false)),
            snapshot_error: Arc::new(Mutex::new(None)),
            reorg_window_exceeded: Arc::new(Mutex::new(None)),
        }
    }

    /// Controls whether both health RPCs succeed with coherent evidence.
    pub fn set_health_available(&self, health_available: bool) {
        self.health_available
            .store(health_available, Ordering::SeqCst);
    }

    /// Controls whether the writer-status health RPC stalls without replying.
    pub fn set_health_stalled(&self, health_stalled: bool) {
        self.health_stalled.store(health_stalled, Ordering::SeqCst);
    }

    /// Controls a deterministic error returned by the mempool snapshot RPC.
    pub fn set_mempool_snapshot_error(&self, snapshot_error: Option<Status>) {
        *self.snapshot_error.lock() = snapshot_error;
    }

    /// Reports a canonical writer parked by a reorg deeper than its window,
    /// or clears the halt with `None`.
    pub fn set_reorg_window_exceeded(&self, halt: Option<ops::ReorgWindowExceededDetail>) {
        *self.reorg_window_exceeded.lock() = halt;
    }
}

#[tonic::async_trait]
impl IngestControl for IngestControlFixtureService {
    type VisibleChainEventsStream = VisibleChainEventsStream;
    type MempoolEventsStream = MempoolEventsStream;

    async fn server_info(
        &self,
        _request: Request<ServerInfoRequest>,
    ) -> Result<Response<ServerInfoResponse>, Status> {
        Ok(Response::new(ServerInfoResponse {
            server_info: Some(ops::ServerInfo {
                network: encode_zinder_native_chain_name(self.network).to_owned(),
                service_name: "zinder-ingest".to_owned(),
                service_version: "test".to_owned(),
                build_git_commit: "test".to_owned(),
                capabilities: [
                    INGEST_CONTROL_SERVER_INFO_V1,
                    INGEST_CONTROL_WRITER_STATUS_V1,
                    INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
                    INGEST_CONTROL_MEMPOOL_TRANSACTION_V2,
                    INGEST_CONTROL_MEMPOOL_EVENTS_V2,
                    INGEST_CONTROL_TRANSPARENT_MEMPOOL_OUTPUTS_BY_ADDRESS_V1,
                    INGEST_CONTROL_TRANSPARENT_MEMPOOL_SPENDS_BY_OUTPOINT_V1,
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
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
        if self.health_stalled.load(Ordering::SeqCst) {
            return std::future::pending().await;
        }
        if !self.health_available.load(Ordering::SeqCst) {
            return Err(Status::unavailable(
                "fixture ingest-control health is unavailable",
            ));
        }
        let reorg_window_exceeded = *self.reorg_window_exceeded.lock();
        Ok(Response::new(WriterStatusResponse {
            chain_view: reorg_window_exceeded
                .is_none()
                .then(|| fixture_chain_view(self.network)),
            network_name: encode_zinder_native_chain_name(self.network).to_owned(),
            phase: WriterPhase::FollowingTip.into(),
            gap_blocks: reorg_window_exceeded.is_none().then_some(0),
            upstream_not_ready: None,
            reorg_window_exceeded,
        }))
    }

    async fn visible_chain_events(
        &self,
        _request: Request<wallet::EventStreamStart>,
    ) -> Result<Response<Self::VisibleChainEventsStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    async fn mempool_snapshot(
        &self,
        _request: Request<wallet::MempoolSnapshotRequest>,
    ) -> Result<Response<wallet::MempoolSnapshotResponse>, Status> {
        let snapshot_error = self.snapshot_error.lock().clone();
        if let Some(error) = snapshot_error {
            return Err(error);
        }
        if !self.health_available.load(Ordering::SeqCst) {
            return Err(Status::unavailable(
                "fixture ingest-control health is unavailable",
            ));
        }
        let chain_view = fixture_chain_view(self.network);
        let source_tip = chain_view
            .chain_epoch
            .as_ref()
            .and_then(|epoch| epoch.visible_tip.clone());
        Ok(Response::new(wallet::MempoolSnapshotResponse {
            chain_view: Some(chain_view),
            events_resume_cursor: Vec::new(),
            snapshot_age_millis: 0,
            entries: Vec::new(),
            next_cursor: Vec::new(),
            source_tip,
        }))
    }

    async fn mempool_transaction(
        &self,
        _request: Request<MempoolTransactionRequest>,
    ) -> Result<Response<wallet::TransactionStatusResponse>, Status> {
        Err(Status::not_found("fixture mempool is empty"))
    }

    async fn mempool_events(
        &self,
        _request: Request<wallet::MempoolEventsRequest>,
    ) -> Result<Response<Self::MempoolEventsStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::pending())))
    }

    async fn transparent_mempool_outputs_by_address(
        &self,
        _request: Request<wallet::TransparentMempoolOutputsByAddressRequest>,
    ) -> Result<Response<wallet::TransparentMempoolOutputsByAddressResponse>, Status> {
        Ok(Response::new(
            wallet::TransparentMempoolOutputsByAddressResponse {
                chain_view: Some(fixture_chain_view(self.network)),
                outputs: Vec::new(),
            },
        ))
    }

    async fn transparent_mempool_spends_by_outpoint(
        &self,
        _request: Request<wallet::TransparentMempoolSpendsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentMempoolSpendsByOutpointResponse>, Status> {
        Ok(Response::new(
            wallet::TransparentMempoolSpendsByOutpointResponse {
                chain_view: Some(fixture_chain_view(self.network)),
                spends: Vec::new(),
            },
        ))
    }

    async fn transparent_mempool_outputs_by_outpoint(
        &self,
        _request: Request<wallet::TransparentMempoolOutputsByOutpointRequest>,
    ) -> Result<Response<wallet::TransparentOutputsByOutpointResponse>, Status> {
        Err(Status::unimplemented(
            "fixture does not implement transparent mempool outputs by outpoint",
        ))
    }

    async fn chain_value_pools_at_tip(
        &self,
        _request: Request<wallet::ChainValuePoolsAtTipRequest>,
    ) -> Result<Response<wallet::ChainValuePoolsAtTipResponse>, Status> {
        Err(Status::unimplemented(
            "fixture does not implement chain value pools",
        ))
    }
}

fn fixture_chain_view(network: Network) -> wallet::ChainView {
    wallet::ChainView {
        chain_epoch: Some(wallet::ChainEpoch {
            chain_epoch_id: 1,
            network_name: encode_zinder_native_chain_name(network).to_owned(),
            artifact_schema_version: 1,
            created_at_millis: 1,
            visible_tip: Some(wallet::BlockTip {
                height: 1,
                hash: "01".repeat(32),
            }),
            settled_tip: Some(wallet::BlockTip {
                height: 1,
                hash: "01".repeat(32),
            }),
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        }),
        indexed_tip: None,
        upstream_tip: None,
        materialized_views: None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::Code;
    use zinder_proto::v1::ingest::{
        MempoolTransactionRequest, ServerInfoRequest, WriterStatusRequest,
        ingest_control_client::IngestControlClient,
    };
    use zinder_proto::v1::wallet;

    use super::IngestControlFixture;

    #[tokio::test]
    async fn advertised_ingest_control_methods_have_valid_empty_semantics() -> eyre::Result<()> {
        let fixture = IngestControlFixture::spawn(zinder_core::Network::ZcashRegtest).await?;
        let mut client = IngestControlClient::connect(fixture.endpoint().to_owned()).await?;

        let server_info = client
            .server_info(ServerInfoRequest {})
            .await?
            .into_inner()
            .server_info
            .ok_or_else(|| eyre::eyre!("fixture ServerInfo omitted its payload"))?;
        assert_eq!(server_info.capabilities.len(), 7);

        let writer_status = client
            .writer_status(WriterStatusRequest {})
            .await?
            .into_inner();
        assert!(writer_status.chain_view.is_some());
        let snapshot = client
            .mempool_snapshot(wallet::MempoolSnapshotRequest {
                max_entries: 1,
                from_cursor: Vec::new(),
            })
            .await?
            .into_inner();
        assert!(snapshot.entries.is_empty());
        assert!(snapshot.chain_view.is_some());
        assert_eq!(
            snapshot.source_tip,
            snapshot
                .chain_view
                .as_ref()
                .and_then(|view| view.chain_epoch.as_ref())
                .and_then(|epoch| epoch.visible_tip.clone())
        );

        let transaction_error = client
            .mempool_transaction(MempoolTransactionRequest {
                transaction_id: "00".repeat(32),
            })
            .await
            .err()
            .ok_or_else(|| eyre::eyre!("empty fixture mempool returned a transaction"))?;
        assert_eq!(transaction_error.code(), Code::NotFound);

        let outputs = client
            .transparent_mempool_outputs_by_address(
                wallet::TransparentMempoolOutputsByAddressRequest {
                    address: None,
                    max_entries: Some(1),
                },
            )
            .await?
            .into_inner();
        assert!(outputs.chain_view.is_some());
        assert!(outputs.outputs.is_empty());
        let spends = client
            .transparent_mempool_spends_by_outpoint(
                wallet::TransparentMempoolSpendsByOutpointRequest {
                    outpoints: Vec::new(),
                },
            )
            .await?
            .into_inner();
        assert!(spends.chain_view.is_some());
        assert!(spends.spends.is_empty());

        let mut events = client
            .mempool_events(wallet::MempoolEventsRequest { start: None })
            .await?
            .into_inner();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), events.message())
                .await
                .is_err(),
            "a quiet fixture event stream must stay open"
        );
        drop(events);
        fixture.shutdown().await?;
        Ok(())
    }
}
