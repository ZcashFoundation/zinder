//! Private authenticated command surface for the running wallet owner.

use std::path::Path;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, service::interceptor::InterceptedService};
use zinder_core::{Network, wire::encode_zinder_native_chain_name};
use zinder_projector::state_bundle::{StateBundleError, StateBundleManifest};
use zinder_proto::v1::ingest::{
    CreateStateBundleCaptureRequest, CreateStateBundleCaptureResponse,
    projector_control_server::{ProjectorControl, ProjectorControlServer},
};
use zinder_runtime::{BearerToken, BearerTokenServerInterceptor};

/// Bounded owner-command channel capacity.
///
/// Capture is serialized by the sole
/// wallet primary, so callers receive resource exhaustion instead of piling up
/// unbounded work while a cold checkpoint is in progress.
pub(crate) const PROJECTOR_CONTROL_COMMAND_CAPACITY: usize = 1;

/// One command delegated to the wallet-owning following loop.
pub(crate) enum ProjectorControlCommand {
    /// Captures one coherent canonical-and-wallet state bundle.
    CreateStateBundleCapture {
        /// Opaque candidate identifier and completed-capture reuse policy.
        request: CreateStateBundleCaptureRequest,
        /// Reply sent only after manifest-last publication succeeds.
        reply: oneshot::Sender<Result<CreateStateBundleCaptureResponse, Status>>,
    },
}

/// Sender held by the authenticated gRPC adapter.
#[derive(Clone)]
pub(crate) struct ProjectorControlHandle {
    sender: mpsc::Sender<ProjectorControlCommand>,
}

impl ProjectorControlHandle {
    async fn create_state_bundle_capture(
        &self,
        request: CreateStateBundleCaptureRequest,
    ) -> Result<CreateStateBundleCaptureResponse, Status> {
        let (reply, receiver) = oneshot::channel();
        self.sender
            .try_send(ProjectorControlCommand::CreateStateBundleCapture { request, reply })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Status::resource_exhausted(
                    "projector owner is already capturing a state bundle",
                ),
                mpsc::error::TrySendError::Closed(_) => {
                    Status::unavailable("projector wallet owner is not available for capture")
                }
            })?;
        receiver.await.map_err(|_| {
            Status::unavailable("projector wallet owner stopped before capture completed")
        })?
    }
}

/// Creates the one private command channel consumed by the wallet owner.
pub(crate) fn projector_control_channel() -> (
    ProjectorControlHandle,
    mpsc::Receiver<ProjectorControlCommand>,
) {
    let (sender, receiver) = mpsc::channel(PROJECTOR_CONTROL_COMMAND_CAPACITY);
    (ProjectorControlHandle { sender }, receiver)
}

/// Authenticated adapter that never opens or owns the wallet store.
#[derive(Clone)]
pub(crate) struct ProjectorControlGrpcAdapter {
    handle: ProjectorControlHandle,
    bearer_token: BearerToken,
}

impl ProjectorControlGrpcAdapter {
    /// Creates an adapter whose every method requires the dedicated token.
    #[must_use]
    pub(crate) fn new(handle: ProjectorControlHandle, bearer_token: BearerToken) -> Self {
        Self {
            handle,
            bearer_token,
        }
    }

    /// Builds the authenticated bounded tonic service.
    #[must_use]
    pub(crate) fn into_server(
        self,
    ) -> InterceptedService<ProjectorControlServer<Self>, BearerTokenServerInterceptor> {
        let interceptor = BearerTokenServerInterceptor::new(Some(self.bearer_token.clone()));
        let server = ProjectorControlServer::new(self)
            .max_decoding_message_size(zinder_runtime::MAX_DECODING_MESSAGE_BYTES);
        InterceptedService::new(server, interceptor)
    }
}

#[tonic::async_trait]
impl ProjectorControl for ProjectorControlGrpcAdapter {
    async fn create_state_bundle_capture(
        &self,
        request: Request<CreateStateBundleCaptureRequest>,
    ) -> Result<Response<CreateStateBundleCaptureResponse>, Status> {
        self.handle
            .create_state_bundle_capture(request.into_inner())
            .await
            .map(Response::new)
    }
}

#[derive(Debug, Error)]
pub(super) enum CaptureReceiptError {
    #[error(transparent)]
    StateBundle(#[from] StateBundleError),
    #[error("the staged manifest differs from the requested capture")]
    CaptureMismatch,
}

pub(super) fn read_capture_receipt(
    root: &Path,
    candidate: &str,
    network: Network,
    fresh_capture: Option<&StateBundleManifest>,
) -> Result<(CreateStateBundleCaptureResponse, [u8; 32]), CaptureReceiptError> {
    let (manifest, digest) = StateBundleManifest::read_with_sha256(root, network)?;
    if manifest.candidate_id() != candidate || fresh_capture.is_some_and(|fresh| fresh != &manifest)
    {
        return Err(CaptureReceiptError::CaptureMismatch);
    }
    let receipt = CreateStateBundleCaptureResponse {
        candidate_id: candidate.to_owned(),
        state_bundle_identity: manifest.identity().to_owned(),
        state_bundle_format_version: u32::from(manifest.format_version()),
        topology: manifest.topology().to_owned(),
        network_name: encode_zinder_native_chain_name(network).to_owned(),
        chain_epoch_id: manifest.fence().chain_epoch_id(),
        chain_event_sequence: manifest.fence().chain_event_sequence(),
        visible_tip_height: manifest.fence().visible_tip_height(),
        state_bundle_manifest_sha256: digest.to_vec(),
    };
    Ok((receipt, digest))
}
