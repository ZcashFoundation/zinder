//! Authenticated ingest-control admission for the release wallet query.

use std::{sync::Arc, time::Duration};

use thiserror::Error;
use tonic::{Code, Request};
use tonic_types::StatusExt as _;
use zinder_core::{
    ChainEpoch, Network,
    wire::{decode_rpc_block_hash_hex, encode_zinder_native_chain_name},
};
use zinder_proto::{
    CONTRACT_REVISION, ZINDER_ERROR_DOMAIN,
    capabilities::{
        INGEST_CONTROL_MEMPOOL_EVENTS_V2, INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
        INGEST_CONTROL_MEMPOOL_TRANSACTION_V2, INGEST_CONTROL_SERVER_INFO_V1,
        INGEST_CONTROL_WRITER_STATUS_V1,
    },
    status_for_reason,
    v1::ingest::{
        ServerInfoRequest, WriterStatusRequest, ingest_control_client::IngestControlClient,
    },
    v1::{
        ops::{ErrorReason, ReorgWindowExceededDetail, UpstreamNotReadyDetail},
        wallet,
    },
    wire::chain_epoch_from_message,
};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, BearerTokenConnectError, NodeUnavailableDetail,
    ReadinessCause, RuntimeService, connect_zinder_grpc,
};
use zinder_source::SourceFailureClass;
/// Bound applied to native wallet unary ingest-control calls and stream establishment.
pub const WALLET_INGEST_CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const REQUIRED_CAPABILITIES: [&str; 5] = [
    INGEST_CONTROL_SERVER_INFO_V1,
    INGEST_CONTROL_WRITER_STATUS_V1,
    INGEST_CONTROL_MEMPOOL_SNAPSHOT_V3,
    INGEST_CONTROL_MEMPOOL_TRANSACTION_V2,
    INGEST_CONTROL_MEMPOOL_EVENTS_V2,
];

/// One authenticated ingest-control identity admitted before the wallet query binds.
#[derive(Clone)]
pub struct AdmittedIngestControl {
    channel: AuthenticatedChannel,
    capabilities: Arc<[String]>,
    network_name: &'static str,
}

impl std::fmt::Debug for AdmittedIngestControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdmittedIngestControl")
            .field("capabilities", &self.capabilities)
            .field("network_name", &self.network_name)
            .finish_non_exhaustive()
    }
}

impl AdmittedIngestControl {
    /// Connects once and admits the exact ingest service identity and structural contract.
    pub async fn connect(
        endpoint: &str,
        bearer_token: Option<&BearerToken>,
        expected_network: Network,
    ) -> Result<Self, IngestControlAdmissionError> {
        let channel = connect_zinder_grpc(endpoint, bearer_token).await?;
        let server_info = IngestControlClient::new(channel.clone())
            .server_info(wallet_ingest_control_request(ServerInfoRequest {}))
            .await
            .map_err(IngestControlAdmissionError::ServerInfoRpc)?
            .into_inner()
            .server_info
            .ok_or(IngestControlAdmissionError::ServerInfoMissing)?;
        let expected_network = encode_zinder_native_chain_name(expected_network);
        let expected_service_name = RuntimeService::Ingest.binary_name();
        if server_info.service_name != expected_service_name {
            return Err(IngestControlAdmissionError::ServiceNameMismatch {
                expected: expected_service_name,
                actual: server_info.service_name,
            });
        }
        if server_info.network != expected_network {
            return Err(IngestControlAdmissionError::NetworkMismatch {
                expected: expected_network,
                actual: server_info.network,
            });
        }
        if server_info.contract_revision != CONTRACT_REVISION {
            return Err(IngestControlAdmissionError::ContractRevisionMismatch {
                expected: CONTRACT_REVISION,
                actual: server_info.contract_revision,
            });
        }
        let missing = REQUIRED_CAPABILITIES
            .into_iter()
            .filter(|required| {
                !server_info
                    .capabilities
                    .iter()
                    .any(|advertised| advertised == required)
            })
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(IngestControlAdmissionError::CapabilitiesMissing { missing });
        }

        Ok(Self {
            channel,
            capabilities: server_info.capabilities.into(),
            network_name: expected_network,
        })
    }

    pub(crate) fn client(&self) -> IngestControlClient<AuthenticatedChannel> {
        IngestControlClient::new(self.channel.clone())
    }

    pub(crate) fn channel(&self) -> AuthenticatedChannel {
        self.channel.clone()
    }

    pub(crate) fn supports(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|advertised| advertised == capability)
    }

    pub(crate) async fn probe_health(&self) -> Result<(), IngestControlHealthError> {
        let writer_status = self
            .client()
            .writer_status(wallet_ingest_control_request(WriterStatusRequest {}))
            .await
            .map_err(IngestControlHealthError::WriterStatusRpc)?
            .into_inner();
        let writer_epoch = validate_writer_status(&writer_status, self.network_name)?;
        let snapshot = match self
            .client()
            .mempool_snapshot(wallet_ingest_control_request(
                wallet::MempoolSnapshotRequest {
                    max_entries: 1,
                    from_cursor: Vec::new(),
                },
            ))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status)
                if is_writer_refusal(
                    &status,
                    Code::FailedPrecondition,
                    ErrorReason::ChainEpochPinUnavailable,
                ) =>
            {
                return Ok(());
            }
            Err(status)
                if is_writer_refusal(&status, Code::Unavailable, ErrorReason::NodeUnavailable) =>
            {
                return Err(IngestControlHealthError::WriterMempoolNodeUnavailable(
                    status,
                ));
            }
            Err(status) => return Err(IngestControlHealthError::MempoolSnapshotRpc(status)),
        };
        validate_snapshot_observation(&writer_epoch, &snapshot, self.network_name)
    }
}

/// Wraps `message` in a request bounded by the wallet ingest-control timeout.
///
/// The bound rides as the `grpc-timeout` header, which both the tonic client
/// channel and the writer enforce, so a stalled writer cannot hold a wallet
/// read open indefinitely.
#[must_use]
pub fn wallet_ingest_control_request<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(WALLET_INGEST_CONTROL_REQUEST_TIMEOUT);
    request
}

#[cfg(test)]
fn validate_health_observation(
    writer_status: &zinder_proto::v1::ingest::WriterStatusResponse,
    snapshot: &wallet::MempoolSnapshotResponse,
    expected_network: &str,
) -> Result<(), IngestControlHealthError> {
    let writer_epoch = validate_writer_status(writer_status, expected_network)?;
    validate_snapshot_observation(&writer_epoch, snapshot, expected_network)
}

fn validate_writer_status(
    writer_status: &zinder_proto::v1::ingest::WriterStatusResponse,
    expected_network: &str,
) -> Result<ChainEpoch, IngestControlHealthError> {
    // A parked writer has no live fence, so its halt precedes fence checks.
    if let Some(halt) = writer_status
        .reorg_window_exceeded
        .filter(|_| writer_status.network_name == expected_network)
    {
        return Err(IngestControlHealthError::WriterReorgWindowExceeded(halt));
    }
    let writer_epoch_message = writer_status
        .chain_view
        .as_ref()
        .and_then(|view| view.chain_epoch.as_ref())
        .filter(|epoch| {
            writer_status.network_name == expected_network && epoch.network_name == expected_network
        })
        .ok_or(IngestControlHealthError::WriterStatusInvalid)?;
    if let Some(upstream_not_ready) = &writer_status.upstream_not_ready {
        return Err(IngestControlHealthError::WriterUpstreamNotReady(
            upstream_not_ready.clone(),
        ));
    }
    let writer_epoch = chain_epoch_from_message(writer_epoch_message.clone())
        .map_err(|_| IngestControlHealthError::WriterStatusInvalid)?;
    if writer_epoch.id.value() == 0 || writer_epoch.artifact_schema_version.value() == 0 {
        return Err(IngestControlHealthError::WriterStatusInvalid);
    }
    Ok(writer_epoch)
}

fn validate_snapshot_observation(
    writer_epoch: &ChainEpoch,
    snapshot: &wallet::MempoolSnapshotResponse,
    expected_network: &str,
) -> Result<(), IngestControlHealthError> {
    let snapshot_epoch_message = snapshot
        .chain_view
        .as_ref()
        .and_then(|view| view.chain_epoch.as_ref())
        .filter(|epoch| epoch.network_name == expected_network)
        .ok_or(IngestControlHealthError::MempoolSnapshotInvalid)?;
    let snapshot_epoch = chain_epoch_from_message(snapshot_epoch_message.clone())
        .map_err(|_| IngestControlHealthError::MempoolSnapshotInvalid)?;
    if snapshot_epoch.id.value() == 0 || snapshot_epoch.artifact_schema_version.value() == 0 {
        return Err(IngestControlHealthError::MempoolSnapshotInvalid);
    }
    let source_tip = snapshot
        .source_tip
        .as_ref()
        .ok_or(IngestControlHealthError::MempoolSnapshotInvalid)?;
    let source_tip_hash = decode_rpc_block_hash_hex(&source_tip.hash)
        .map_err(|_| IngestControlHealthError::MempoolSnapshotInvalid)?;
    if source_tip.height != snapshot_epoch.visible_tip_height.value()
        || source_tip_hash != snapshot_epoch.visible_tip_hash
    {
        return Err(IngestControlHealthError::MempoolSnapshotInvalid);
    }

    if snapshot_epoch.id < writer_epoch.id {
        return Err(IngestControlHealthError::MempoolSnapshotOlderThanWriter);
    }
    if snapshot_epoch.id == writer_epoch.id
        && (snapshot_epoch.visible_tip_height != writer_epoch.visible_tip_height
            || snapshot_epoch.visible_tip_hash != writer_epoch.visible_tip_hash)
    {
        return Err(IngestControlHealthError::MempoolSnapshotWriterFenceMismatch);
    }
    Ok(())
}

/// Types a failed ingest-control call that the writer never answered.
///
/// A status the writer built carries a `zinder.dev` `ErrorInfo` and passes
/// through unchanged. A transport failure, such as a refused connection or an
/// expired deadline, carries none, so it becomes `UPSTREAM_UNREACHABLE` and a
/// wallet client can tell an ingest-control outage from a writer answer.
pub(crate) fn typed_ingest_control_status(status: tonic::Status) -> tonic::Status {
    let is_transport_code = matches!(
        status.code(),
        Code::Unavailable | Code::Unknown | Code::DeadlineExceeded | Code::Cancelled
    );
    let carries_zinder_reason = status
        .get_error_details()
        .error_info()
        .is_some_and(|error_info| error_info.domain == ZINDER_ERROR_DOMAIN);
    if !is_transport_code || carries_zinder_reason {
        return status;
    }
    status_for_reason(
        ErrorReason::UpstreamUnreachable,
        format!("ingest control is unreachable: {}", status.message()),
    )
}

/// Whether `status` is a refusal the writer itself built with `code` and
/// `reason`, rather than a failure of the call or of another server.
///
/// A `MempoolSnapshot` that is not yet coherent at a new tip returns
/// `CHAIN_EPOCH_PIN_UNAVAILABLE`, which leaves ingest control healthy. A live
/// mempool that is hydrating or rebuilding from the node returns
/// `NODE_UNAVAILABLE`, which the probe relays as the writer's node outage
/// rather than an ingest-control failure.
fn is_writer_refusal(status: &tonic::Status, code: Code, reason: ErrorReason) -> bool {
    status.code() == code
        && status
            .get_error_details()
            .error_info()
            .is_some_and(|error_info| {
                error_info.domain == ZINDER_ERROR_DOMAIN
                    && error_info.reason == reason.as_str_name()
            })
}

#[derive(Debug, Error)]
pub(crate) enum IngestControlHealthError {
    #[error("ingest-control WriterStatus failed: {0}")]
    WriterStatusRpc(tonic::Status),
    #[error("ingest-control WriterStatus returned incoherent network or chain evidence")]
    WriterStatusInvalid,
    #[error("ingest-control WriterStatus reports that its upstream is not ready")]
    WriterUpstreamNotReady(UpstreamNotReadyDetail),
    #[error(
        "canonical writer is parked: a {}-block replacement exceeds its {}-block reorg window",
        .0.depth,
        .0.configured
    )]
    WriterReorgWindowExceeded(ReorgWindowExceededDetail),
    #[error("ingest-control MempoolSnapshot failed: {0}")]
    MempoolSnapshotRpc(tonic::Status),
    #[error("the writer's live mempool waits on its node: {}", .0.message())]
    WriterMempoolNodeUnavailable(tonic::Status),
    #[error("ingest-control MempoolSnapshot returned incoherent chain evidence")]
    MempoolSnapshotInvalid,
    #[error("ingest-control MempoolSnapshot is older than the preceding WriterStatus")]
    MempoolSnapshotOlderThanWriter,
    #[error("ingest-control MempoolSnapshot disagrees with same-epoch WriterStatus")]
    MempoolSnapshotWriterFenceMismatch,
}

impl IngestControlHealthError {
    /// Returns the cause this health failure publishes as the ingest-control
    /// dependency. A parked writer's halt refuses wallet traffic. A writer
    /// whose node is not ready, or whose live mempool waits on its node,
    /// relays that observation; every other failure is a recoverable
    /// control-channel outage.
    pub(crate) fn readiness_cause(&self) -> ReadinessCause {
        match self {
            Self::WriterReorgWindowExceeded(halt) => ReadinessCause::from(*halt),
            Self::WriterUpstreamNotReady(detail) => ReadinessCause::UpstreamNotReady(
                zinder_runtime::UpstreamNotReadyDetail::from(detail),
            ),
            Self::WriterMempoolNodeUnavailable(status) => {
                ReadinessCause::NodeUnavailable(NodeUnavailableDetail::first_iteration(
                    SourceFailureClass::StreamDisconnected.label(),
                    status.message().to_owned(),
                ))
            }
            Self::WriterStatusRpc(_)
            | Self::WriterStatusInvalid
            | Self::MempoolSnapshotRpc(_)
            | Self::MempoolSnapshotInvalid
            | Self::MempoolSnapshotOlderThanWriter
            | Self::MempoolSnapshotWriterFenceMismatch => ReadinessCause::IngestControlUnavailable,
        }
    }

    pub(crate) const fn class(&self) -> &'static str {
        match self {
            Self::WriterStatusRpc(_) => "writer_status_rpc",
            Self::WriterStatusInvalid => "writer_status_invalid",
            Self::WriterUpstreamNotReady(_) => "writer_upstream_not_ready",
            Self::WriterReorgWindowExceeded(_) => "writer_reorg_window_exceeded",
            Self::MempoolSnapshotRpc(_) => "mempool_snapshot_rpc",
            Self::WriterMempoolNodeUnavailable(_) => "writer_mempool_node_unavailable",
            Self::MempoolSnapshotInvalid => "mempool_snapshot_invalid",
            Self::MempoolSnapshotOlderThanWriter => "mempool_snapshot_older_than_writer",
            Self::MempoolSnapshotWriterFenceMismatch => "mempool_snapshot_writer_fence_mismatch",
        }
    }
}

/// Startup rejection from authenticating the release ingest-control dependency.
#[derive(Debug, Error)]
pub enum IngestControlAdmissionError {
    /// The authenticated HTTP/2 channel could not be opened.
    #[error("could not connect to ingest control: {0}")]
    Connect(#[from] BearerTokenConnectError),
    /// The ingest service identity RPC failed.
    #[error("ingest-control ServerInfo failed: {0}")]
    ServerInfoRpc(tonic::Status),
    /// The identity response omitted its required payload.
    #[error("ingest-control ServerInfo omitted server_info")]
    ServerInfoMissing,
    /// The endpoint is not the expected ingest service.
    #[error("ingest-control service mismatch: expected {expected}, got {actual}")]
    ServiceNameMismatch {
        /// Required stable service name.
        expected: &'static str,
        /// Advertised service name.
        actual: String,
    },
    /// The endpoint serves a different network.
    #[error("ingest-control network mismatch: expected {expected}, got {actual}")]
    NetworkMismatch {
        /// Required native network name.
        expected: &'static str,
        /// Advertised native network name.
        actual: String,
    },
    /// The endpoint implements a different internal protocol revision.
    #[error("ingest-control contract revision mismatch: expected {expected}, got {actual}")]
    ContractRevisionMismatch {
        /// Required contract revision.
        expected: u32,
        /// Advertised contract revision.
        actual: u32,
    },
    /// The endpoint omits one or more operations required by the release query.
    #[error("ingest-control capabilities missing: {missing:?}")]
    CapabilitiesMissing {
        /// Required capability identifiers absent from `ServerInfo`.
        missing: Vec<&'static str>,
    },
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::{Code, Status};
    use tonic_types::{ErrorDetails, StatusExt as _};
    use zinder_core::Network;
    use zinder_proto::v1::{
        ingest::{WriterPhase, WriterStatusResponse},
        ops::UpstreamNotReadyDetail,
        wallet,
    };
    use zinder_proto::{ZINDER_ERROR_DOMAIN, status_for_reason};
    use zinder_testkit::IngestControlFixture;

    use super::{
        AdmittedIngestControl, IngestControlHealthError, WALLET_INGEST_CONTROL_REQUEST_TIMEOUT,
        is_writer_refusal, typed_ingest_control_status, validate_health_observation,
    };

    const NETWORK: &str = "zcash-regtest";

    #[test]
    fn health_rejects_writer_status_without_a_chain_epoch() {
        let mut writer = writer_status(1, 1, "01");
        writer.chain_view = None;
        let outcome = validate_health_observation(&writer, &snapshot(1, 1, "01"), NETWORK);
        assert!(matches!(
            outcome,
            Err(IngestControlHealthError::WriterStatusInvalid)
        ));
    }

    #[test]
    fn health_rejects_zero_writer_epoch_or_schema_identity() {
        let zero_epoch =
            validate_health_observation(&writer_status(0, 1, "01"), &snapshot(1, 1, "01"), NETWORK);
        assert!(matches!(
            zero_epoch,
            Err(IngestControlHealthError::WriterStatusInvalid)
        ));

        let mut zero_schema = writer_status(1, 1, "01");
        if let Some(epoch) = zero_schema
            .chain_view
            .as_mut()
            .and_then(|view| view.chain_epoch.as_mut())
        {
            epoch.artifact_schema_version = 0;
        }
        let zero_schema_outcome =
            validate_health_observation(&zero_schema, &snapshot(1, 1, "01"), NETWORK);
        assert!(matches!(
            zero_schema_outcome,
            Err(IngestControlHealthError::WriterStatusInvalid)
        ));
    }

    #[test]
    fn health_rejects_malformed_writer_and_snapshot_hashes() {
        let malformed_writer = validate_health_observation(
            &writer_status(1, 1, "not-a-hash"),
            &snapshot(1, 1, "01"),
            NETWORK,
        );
        assert!(matches!(
            malformed_writer,
            Err(IngestControlHealthError::WriterStatusInvalid)
        ));

        let mut malformed_snapshot = snapshot(1, 1, "01");
        if let Some(source_tip) = malformed_snapshot.source_tip.as_mut() {
            source_tip.hash.clear();
        }
        let malformed_snapshot_outcome =
            validate_health_observation(&writer_status(1, 1, "01"), &malformed_snapshot, NETWORK);
        assert!(matches!(
            malformed_snapshot_outcome,
            Err(IngestControlHealthError::MempoolSnapshotInvalid)
        ));
    }

    #[test]
    fn health_relays_the_writer_upstream_not_ready_observation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = writer_status(1, 1, "01");
        writer.upstream_not_ready = Some(UpstreamNotReadyDetail {
            upstream_committed_height: Some(1),
            upstream_estimated_height: Some(9),
            upstream_verification_progress: Some(0.5),
            upstream_health_source: "zebra_ready_endpoint".to_owned(),
            upstream_health_reason: "syncing".to_owned(),
        });
        let error = validate_health_observation(&writer, &snapshot(1, 1, "01"), NETWORK)
            .err()
            .ok_or("a writer whose node is not ready must fail health")?;
        assert_eq!(error.class(), "writer_upstream_not_ready");
        let zinder_runtime::ReadinessCause::UpstreamNotReady(detail) = error.readiness_cause()
        else {
            return Err(format!("unexpected cause {:?}", error.readiness_cause()).into());
        };
        assert_eq!(detail.upstream_committed_height, Some(1));
        assert_eq!(detail.upstream_estimated_height, Some(9));
        assert_eq!(detail.upstream_health.source, "zebra_ready_endpoint");
        assert_eq!(detail.upstream_health.reason, "syncing");
        Ok(())
    }

    #[test]
    fn health_reports_a_parked_writer_as_a_typed_reorg_halt()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = writer_status(1, 1, "01");
        writer.chain_view = None;
        writer.reorg_window_exceeded = Some(zinder_proto::v1::ops::ReorgWindowExceededDetail {
            depth: 1_001,
            configured: 1_000,
            covered: Some(1_000),
        });
        let error = validate_health_observation(&writer, &snapshot(1, 1, "01"), NETWORK)
            .err()
            .ok_or("a parked writer must fail health")?;
        assert!(matches!(
            error.readiness_cause(),
            zinder_runtime::ReadinessCause::ReorgWindowExceeded {
                depth: 1_001,
                configured: 1_000,
                covered: Some(1_000),
            }
        ));
        assert_eq!(error.class(), "writer_reorg_window_exceeded");

        writer.network_name = "zcash-testnet".to_owned();
        assert!(matches!(
            validate_health_observation(&writer, &snapshot(1, 1, "01"), NETWORK),
            Err(IngestControlHealthError::WriterStatusInvalid)
        ));
        assert!(matches!(
            IngestControlHealthError::WriterStatusInvalid.readiness_cause(),
            zinder_runtime::ReadinessCause::IngestControlUnavailable
        ));
        Ok(())
    }

    #[test]
    fn health_rejects_a_snapshot_older_than_writer_status() {
        let outcome =
            validate_health_observation(&writer_status(2, 2, "02"), &snapshot(1, 1, "01"), NETWORK);
        assert!(matches!(
            outcome,
            Err(IngestControlHealthError::MempoolSnapshotOlderThanWriter)
        ));
    }

    #[test]
    fn health_rejects_a_same_epoch_snapshot_with_a_different_tip() {
        let outcome =
            validate_health_observation(&writer_status(2, 2, "02"), &snapshot(2, 3, "03"), NETWORK);
        assert!(matches!(
            outcome,
            Err(IngestControlHealthError::MempoolSnapshotWriterFenceMismatch)
        ));
    }

    #[test]
    fn health_accepts_a_newer_snapshot_observed_after_writer_status() {
        assert!(
            validate_health_observation(
                &writer_status(2, 2, "02"),
                &snapshot(3, 1, "a1"),
                NETWORK,
            )
            .is_ok(),
            "a newer epoch is a valid ordered observation"
        );
    }

    #[test]
    fn health_rejects_snapshot_source_tip_mismatch() {
        let mut snapshot = snapshot(2, 2, "02");
        snapshot.source_tip = Some(block_tip(3, "03"));
        let outcome = validate_health_observation(&writer_status(2, 2, "02"), &snapshot, NETWORK);
        assert!(matches!(
            outcome,
            Err(IngestControlHealthError::MempoolSnapshotInvalid)
        ));
    }

    #[test]
    fn writer_refusal_predicate_requires_the_exact_structured_status() {
        use zinder_proto::v1::ops::ErrorReason;

        let is_stale_view = |status: &Status| {
            is_writer_refusal(
                status,
                Code::FailedPrecondition,
                ErrorReason::ChainEpochPinUnavailable,
            )
        };
        let exact = status_for_reason(
            ErrorReason::ChainEpochPinUnavailable,
            "requested chain epoch is no longer available",
        );
        assert!(is_stale_view(&exact));

        let hydrating = status_for_reason(
            ErrorReason::NodeUnavailable,
            "live mempool index is hydrating from an upstream snapshot",
        );
        assert!(is_writer_refusal(
            &hydrating,
            Code::Unavailable,
            ErrorReason::NodeUnavailable
        ));
        assert!(!is_stale_view(&hydrating));
        let passed = typed_ingest_control_status(hydrating.clone());
        assert_eq!(
            passed
                .get_error_details()
                .error_info()
                .map(|info| info.reason.clone()),
            hydrating
                .get_error_details()
                .error_info()
                .map(|info| info.reason.clone())
        );

        let different_reason =
            status_for_reason(ErrorReason::ServiceNotReady, "mempool is not ready");
        assert!(!is_stale_view(&different_reason));

        let foreign_domain = Status::with_error_details(
            Code::FailedPrecondition,
            "foreign stale view",
            ErrorDetails::with_error_info("CHAIN_EPOCH_PIN_UNAVAILABLE", "other.example", []),
        );
        assert!(!is_stale_view(&foreign_domain));

        let wrong_code = Status::with_error_details(
            Code::Unavailable,
            "stale view with the wrong code",
            ErrorDetails::with_error_info("CHAIN_EPOCH_PIN_UNAVAILABLE", ZINDER_ERROR_DOMAIN, []),
        );
        assert!(!is_stale_view(&wrong_code));

        let absent_details = Status::failed_precondition("missing error details");
        assert!(!is_stale_view(&absent_details));

        let malformed_details = Status::with_error_details(
            Code::FailedPrecondition,
            "missing ErrorInfo",
            ErrorDetails::new(),
        );
        assert!(!is_stale_view(&malformed_details));
    }

    /// A writer whose live mempool waits on its node relays that outage as a
    /// node outage, not as an ingest-control failure.
    #[test]
    fn writer_mempool_node_refusal_reports_the_node_outage() -> Result<(), String> {
        let error = IngestControlHealthError::WriterMempoolNodeUnavailable(status_for_reason(
            zinder_proto::v1::ops::ErrorReason::NodeUnavailable,
            "live mempool index is hydrating from an upstream snapshot",
        ));
        let zinder_runtime::ReadinessCause::NodeUnavailable(detail) = error.readiness_cause()
        else {
            return Err(format!("unexpected cause {:?}", error.readiness_cause()));
        };
        assert_eq!(detail.failure_class, "stream_disconnected");
        assert_eq!(
            detail.last_reason,
            "live mempool index is hydrating from an upstream snapshot"
        );
        assert_eq!(error.class(), "writer_mempool_node_unavailable");
        Ok(())
    }

    #[test]
    fn untyped_transport_failures_become_upstream_unreachable() {
        let reason_of = |status: &Status| {
            status
                .get_error_details()
                .error_info()
                .map(|error_info| (error_info.domain.clone(), error_info.reason.clone()))
        };
        let unreachable = Some((
            ZINDER_ERROR_DOMAIN.to_owned(),
            zinder_proto::v1::ops::ErrorReason::UpstreamUnreachable
                .as_str_name()
                .to_owned(),
        ));
        for transport_status in [
            Status::unavailable("tcp connect error"),
            Status::unknown("transport error"),
            Status::deadline_exceeded("deadline expired"),
            Status::cancelled("timeout expired"),
        ] {
            let typed = typed_ingest_control_status(transport_status);
            assert_eq!(typed.code(), Code::Unavailable);
            assert_eq!(reason_of(&typed), unreachable);
        }

        let writer_answer = status_for_reason(
            zinder_proto::v1::ops::ErrorReason::ServiceNotReady,
            "mempool is not ready",
        );
        let passed = typed_ingest_control_status(writer_answer.clone());
        assert_eq!(passed.code(), writer_answer.code());
        assert_eq!(reason_of(&passed), reason_of(&writer_answer));

        let untyped_miss = typed_ingest_control_status(Status::not_found("no such transaction"));
        assert_eq!(untyped_miss.code(), Code::NotFound);
        assert_eq!(reason_of(&untyped_miss), None);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_health_handler_is_bounded_by_the_internal_request_timeout()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = IngestControlFixture::spawn(Network::ZcashRegtest).await?;
        let admitted =
            AdmittedIngestControl::connect(fixture.endpoint(), None, Network::ZcashRegtest).await?;
        fixture.set_health_stalled(true);
        let probe = tokio::spawn(async move { admitted.probe_health().await });
        tokio::task::yield_now().await;
        tokio::time::advance(WALLET_INGEST_CONTROL_REQUEST_TIMEOUT + Duration::from_millis(1))
            .await;
        let outcome = probe.await?;
        assert!(matches!(
            outcome,
            Err(IngestControlHealthError::WriterStatusRpc(_))
        ));
        fixture.shutdown().await?;
        Ok(())
    }

    fn writer_status(epoch_id: u64, height: u32, hash_byte: &str) -> WriterStatusResponse {
        WriterStatusResponse {
            chain_view: Some(chain_view(epoch_id, height, hash_byte)),
            network_name: NETWORK.to_owned(),
            phase: WriterPhase::FollowingTip.into(),
            gap_blocks: Some(0),
            upstream_not_ready: None,
            reorg_window_exceeded: None,
        }
    }

    fn snapshot(epoch_id: u64, height: u32, hash_byte: &str) -> wallet::MempoolSnapshotResponse {
        wallet::MempoolSnapshotResponse {
            chain_view: Some(chain_view(epoch_id, height, hash_byte)),
            source_tip: Some(block_tip(height, hash_byte)),
            ..wallet::MempoolSnapshotResponse::default()
        }
    }

    fn chain_view(epoch_id: u64, height: u32, hash_byte: &str) -> wallet::ChainView {
        wallet::ChainView {
            chain_epoch: Some(wallet::ChainEpoch {
                chain_epoch_id: epoch_id,
                network_name: NETWORK.to_owned(),
                artifact_schema_version: 1,
                visible_tip: Some(block_tip(height, hash_byte)),
                settled_tip: Some(block_tip(height, hash_byte)),
                ..wallet::ChainEpoch::default()
            }),
            ..wallet::ChainView::default()
        }
    }

    fn block_tip(height: u32, hash_byte: &str) -> wallet::BlockTip {
        wallet::BlockTip {
            height,
            hash: hash_byte.repeat(32),
        }
    }
}
