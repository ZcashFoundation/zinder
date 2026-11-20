use parking_lot::Mutex;
use tonic::{Request, Response, Status};
use zinder_core::Network;
use zinder_proto::v1::ingest::{
    AcquireCanonicalProjectionBuildLeaseRequest, CanonicalEventPageRequest,
    CanonicalEventPageResponse, CanonicalProjectionBuildLeaseResponse, CanonicalWriterFence,
    CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
    CreateCanonicalOwnerCheckpointRequest, CreateCanonicalOwnerCheckpointResponse,
    ReadmitCanonicalOwnerCheckpointRequest, ReleaseCanonicalProjectionBuildLeaseRequest,
    ReleaseCanonicalProjectionBuildLeaseResponse, RenewCanonicalProjectionBuildLeaseRequest,
    canonical_control_server::CanonicalControl,
};
use zinder_store::CanonicalEventFence;

#[derive(Clone)]
pub(crate) struct MutableCanonicalControl {
    pub(crate) status: std::sync::Arc<Mutex<CanonicalWriterStatusResponse>>,
}

#[tonic::async_trait]
impl CanonicalControl for MutableCanonicalControl {
    async fn writer_status(
        &self,
        _request: Request<CanonicalWriterStatusRequest>,
    ) -> Result<Response<CanonicalWriterStatusResponse>, Status> {
        Ok(Response::new(self.status.lock().clone()))
    }

    async fn event_page(
        &self,
        _request: Request<CanonicalEventPageRequest>,
    ) -> Result<Response<CanonicalEventPageResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn create_owner_checkpoint(
        &self,
        _request: Request<CreateCanonicalOwnerCheckpointRequest>,
    ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn readmit_owner_checkpoint(
        &self,
        _request: Request<ReadmitCanonicalOwnerCheckpointRequest>,
    ) -> Result<Response<CreateCanonicalOwnerCheckpointResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn acquire_projection_build_lease(
        &self,
        _request: Request<AcquireCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn renew_projection_build_lease(
        &self,
        _request: Request<RenewCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<CanonicalProjectionBuildLeaseResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }

    async fn release_projection_build_lease(
        &self,
        _request: Request<ReleaseCanonicalProjectionBuildLeaseRequest>,
    ) -> Result<Response<ReleaseCanonicalProjectionBuildLeaseResponse>, Status> {
        Err(Status::unimplemented(
            "bounded process fixture only serves writer status",
        ))
    }
}

pub(crate) fn writer_status_for_fence(
    fence: CanonicalEventFence,
    network: Network,
    schedule: Option<&zinder_store::CanonicalActivationSchedule>,
) -> CanonicalWriterStatusResponse {
    CanonicalWriterStatusResponse {
        network_name: zinder_core::wire::encode_zinder_native_chain_name(network).to_owned(),
        fence: Some(CanonicalWriterFence {
            chain_epoch_id: fence.chain_epoch_id().value(),
            event_sequence: fence.chain_event_sequence(),
            visible_tip_height: fence.visible_tip().height.value(),
            visible_tip_hash: fence.visible_tip().hash.as_bytes().to_vec(),
            visible_block_count: fence.sequence_digest().block_count(),
            canonical_sequence_digest: fence.sequence_digest().as_bytes().to_vec(),
        }),
        oldest_retained_event_sequence: 1,
        network_upgrade_schedule: schedule.map(|schedule| {
            zinder_proto::wire::encode_canonical_network_upgrade_schedule(
                schedule.activations(),
                schedule.revision(),
            )
        }),
        reorg_window_exceeded: None,
        tip_unconfirmed: false,
        canonical_store_damaged: None,
    }
}
