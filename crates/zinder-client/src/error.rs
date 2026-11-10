//! Public client error vocabulary.

use thiserror::Error;
#[cfg(feature = "remote")]
use tonic::Code;
#[cfg(feature = "remote")]
use tonic_types::StatusExt;
use zinder_core::Network;
#[cfg(feature = "remote")]
use zinder_proto::wire::decode_reorg_window_exceeded_metadata;

use crate::ErrorReason;

/// Renders [`IndexerError::ReorgWindowExceeded`] from whichever details the
/// server reported; servers that predate a field omit it.
fn reorg_window_exceeded_message(
    replacement_depth_blocks: Option<u64>,
    reorg_window_blocks: Option<u64>,
    covered_depth_blocks: Option<u64>,
) -> String {
    let replacement = replacement_depth_blocks.map_or_else(
        || "a replacement".to_owned(),
        |depth| format!("a replacement of {depth} blocks"),
    );
    let window = reorg_window_blocks.map_or_else(
        || "its reorg window".to_owned(),
        |window| format!("its {window}-block reorg window"),
    );
    // The covered depth only adds information while a raised window is still
    // growing toward its configured size.
    let bound = match covered_depth_blocks {
        Some(covered) if Some(covered) != reorg_window_blocks => {
            format!("the {covered} replaceable blocks of {window}")
        }
        Some(_) | None => window,
    };
    format!("indexer halted: {replacement} exceeds {bound}")
}

/// Domain Zinder services set on every `google.rpc.ErrorInfo`.
///
/// Matches the error vocabulary reference; duplicated here so the client does
/// not need to depend on a service crate.
#[cfg(feature = "remote")]
pub(crate) const ZINDER_ERROR_DOMAIN: &str = "zinder.dev";

/// Suggested retry policy attached to every [`IndexerError`].
///
/// Clients consult this to decide whether to retry, surface to the operator,
/// or fail the caller. The policy is derived from the gRPC code and the
/// typed remote error reason, so it is stable across Zinder releases.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RetryPolicy {
    /// Acquire a fresh chain epoch and restart the whole epoch-bound operation.
    /// Retrying the same pinned request cannot succeed.
    RefreshChainEpoch,
    /// Restart the chain-event stream from the earliest retained event and
    /// rebuild derived state from that replay boundary.
    RestartFromEarliestRetained,
    /// Retry with exponential backoff. The remote service or upstream node
    /// is transiently unavailable; the request shape is correct.
    RetryWithBackoff,
    /// An operator must intervene before the request can succeed. Examples:
    /// reorg window exceeded, schema mismatch, broadcast disabled. Clients
    /// must not retry without manual reconfiguration.
    OperatorActionRequired,
    /// The request itself is malformed or out of bounds. Clients fix the
    /// request and re-issue; retrying the same input will fail again.
    ClientError,
}

/// Recovery position for an expired chain-event cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChainEventCursorRecovery {
    /// Discard the expired cursor and subscribe from the earliest retained event.
    EarliestRetained,
}

/// Error returned by [`crate::ChainIndex`] implementations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IndexerError {
    /// No visible chain epoch has been committed yet.
    #[error("no visible chain epoch has been committed")]
    NoVisibleChainEpoch,

    /// Requested chain epoch is no longer retained by this serving pair.
    #[error("requested chain epoch pin is unavailable")]
    ChainEpochPinUnavailable,

    /// A chain-event cursor points before the retained event window.
    #[error("chain event cursor expired; restart from the earliest retained event")]
    ChainEventCursorExpired {
        /// Safe non-operator recovery action.
        recovery: ChainEventCursorRecovery,
    },

    /// The indexer's canonical writer stopped following its node because a
    /// replacement crossed the configured reorg window.
    ///
    /// The node can only present such a reorg after a failure such as a
    /// replaced or resynced node, so the indexer cannot recover on its own.
    /// Reads would come from a chain that no longer advances: treat the
    /// indexer as halted, stop creating spends against it, and alert an
    /// operator. Unary calls and open chain-event and mempool-event streams
    /// end with this error until the operator reconciles the indexer.
    #[error(
        "{}",
        reorg_window_exceeded_message(
            *.replacement_depth_blocks,
            *.reorg_window_blocks,
            *.covered_depth_blocks
        )
    )]
    ReorgWindowExceeded {
        /// Blocks the refused replacement would have replaced, when the
        /// server reports it.
        replacement_depth_blocks: Option<u64>,
        /// The indexer's reorg window in blocks, when the server reports it.
        reorg_window_blocks: Option<u64>,
        /// Replaceable depth the window covered when the replacement was
        /// refused, when the server reports it. It is below
        /// `reorg_window_blocks` while a raised window is still growing.
        covered_depth_blocks: Option<u64>,
    },

    /// Requested data is not indexed in the visible chain.
    #[error("{resource} was not found")]
    NotFound {
        /// Resource kind.
        resource: &'static str,
    },

    /// Requested artifact key is valid but absent from the named family.
    ///
    /// This is the canonical per-artifact unavailability signal; future
    /// per-artifact "not present" cases use this variant rather than adding
    /// a new top-level variant. `family` is the on-wire family label the
    /// server emits in `google.rpc.ResourceInfo.resource_type`; its values are
    /// the [`zinder_core::artifact_family`] constants.
    #[error("artifact unavailable in family {family}: key {key}")]
    ArtifactUnavailable {
        /// On-wire family label (see [`zinder_core::artifact_family`]).
        family: String,
        /// Diagnostic representation of the missing key.
        key: String,
    },

    /// Request failed local validation or remote argument validation.
    #[error("invalid request: {reason}")]
    InvalidRequest {
        /// Stable diagnostic reason.
        reason: String,
    },

    /// Requested operation is unavailable until the deployment is reconfigured.
    #[error("operation failed precondition: {reason}")]
    FailedPrecondition {
        /// Stable diagnostic reason.
        reason: String,
    },

    /// Stored or transmitted data could not be decoded.
    #[error("data loss: {reason}")]
    DataLoss {
        /// Stable diagnostic reason.
        reason: String,
    },

    /// Storage could not serve the request.
    #[error("storage is unavailable: {reason}")]
    StorageUnavailable {
        /// Stable diagnostic reason.
        reason: String,
    },

    /// Remote service could not serve the request.
    #[error("remote service is unavailable: {reason}")]
    ServiceUnavailable {
        /// Stable diagnostic reason.
        reason: String,
    },

    /// A response was missing a required field or carried invalid bytes.
    #[error("malformed response field {field}: {reason}")]
    MalformedResponse {
        /// Field path.
        field: &'static str,
        /// Stable diagnostic reason.
        reason: String,
    },

    /// Remote service returned data for a different network.
    #[error("network mismatch: expected {expected:?}, actual {actual}")]
    NetworkMismatch {
        /// Expected network.
        expected: Network,
        /// Remote network name.
        actual: String,
    },

    /// A blocking local read task failed unexpectedly.
    #[error("blocking task failed: {reason}")]
    BlockingTaskFailed {
        /// Stable diagnostic reason.
        reason: String,
    },

    /// A remote service returned a typed reason without a richer local recovery variant.
    #[error("remote service returned {reason:?}: {message}")]
    RemoteFailure {
        /// Exact known or additive reason returned by the server.
        reason: ErrorReason,
        /// Human-readable gRPC status message.
        message: String,
        /// Retry policy derived conservatively from the canonical gRPC code.
        retry_policy: RetryPolicy,
    },
}

impl IndexerError {
    #[cfg(feature = "remote")]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "tonic::Status is consumed through map_err adapters at gRPC boundaries"
    )]
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let message = status.message().to_owned();
        let details = status.get_error_details();
        let Some(zinder_reason) = details.error_info().and_then(|error_info| {
            if error_info.domain == ZINDER_ERROR_DOMAIN {
                Some(ErrorReason::from_wire_name(&error_info.reason))
            } else {
                None
            }
        }) else {
            return Self::ServiceUnavailable {
                reason: format!("missing zinder.dev ErrorInfo: {message}"),
            };
        };

        if status.code() == Code::NotFound
            && matches!(&zinder_reason, ErrorReason::BlockNotInBestChain)
        {
            return Self::NotFound { resource: "block" };
        }

        if status.code() == Code::NotFound
            && matches!(&zinder_reason, ErrorReason::ArtifactUnavailable)
            && let Some(resource_info) = details.resource_info()
        {
            return Self::ArtifactUnavailable {
                family: resource_info.resource_type.clone(),
                key: resource_info.resource_name.clone(),
            };
        }

        if status.code() == Code::FailedPrecondition
            && matches!(&zinder_reason, ErrorReason::ChainEventCursorExpired)
        {
            return Self::ChainEventCursorExpired {
                recovery: ChainEventCursorRecovery::EarliestRetained,
            };
        }

        if matches!(&zinder_reason, ErrorReason::ChainEpochPinUnavailable) {
            return Self::ChainEpochPinUnavailable;
        }

        if matches!(&zinder_reason, ErrorReason::ReorgWindowExceeded) {
            let halt = details
                .error_info()
                .map(|error_info| decode_reorg_window_exceeded_metadata(&error_info.metadata))
                .unwrap_or_default();
            return Self::ReorgWindowExceeded {
                replacement_depth_blocks: halt.replacement_depth_blocks,
                reorg_window_blocks: halt.reorg_window_blocks,
                covered_depth_blocks: halt.covered_depth_blocks,
            };
        }

        Self::RemoteFailure {
            retry_policy: retry_policy_for_remote(&zinder_reason, status.code()),
            reason: zinder_reason,
            message,
        }
    }

    /// Returns the typed [`ErrorReason`] attached to the failure, when known.
    ///
    /// Returns `Some` when this variant preserves one exact wire reason.
    /// Returns `None` for client-side failures and for deliberately coarser
    /// domain variants such as [`IndexerError::NotFound`], which do not retain
    /// the originating wire reason.
    #[must_use]
    #[cfg(feature = "remote")]
    pub fn reason(&self) -> Option<ErrorReason> {
        // Rich recovery variants have a one-to-one wire reason. Generic remote
        // failures retain the exact parsed reason; coarser domain variants do
        // not infer or reconstruct a reason from their local shape.
        match self {
            Self::ChainEpochPinUnavailable => Some(ErrorReason::ChainEpochPinUnavailable),
            Self::ChainEventCursorExpired { .. } => Some(ErrorReason::ChainEventCursorExpired),
            Self::ReorgWindowExceeded { .. } => Some(ErrorReason::ReorgWindowExceeded),
            Self::ArtifactUnavailable { .. } => Some(ErrorReason::ArtifactUnavailable),
            Self::RemoteFailure { reason, .. } => Some(reason.clone()),
            Self::NoVisibleChainEpoch
            | Self::NotFound { .. }
            | Self::InvalidRequest { .. }
            | Self::FailedPrecondition { .. }
            | Self::DataLoss { .. }
            | Self::StorageUnavailable { .. }
            | Self::ServiceUnavailable { .. }
            | Self::BlockingTaskFailed { .. }
            | Self::MalformedResponse { .. }
            | Self::NetworkMismatch { .. } => None,
        }
    }

    /// Suggested retry policy for this error.
    ///
    /// Returns a stable [`RetryPolicy`] derived from the variant tag.
    /// Consumers map this to local retry, alerting, and operator-action
    /// decisions without parsing message strings.
    #[must_use]
    pub fn retry_policy(&self) -> RetryPolicy {
        match self {
            Self::ChainEpochPinUnavailable => RetryPolicy::RefreshChainEpoch,
            Self::ChainEventCursorExpired { .. } => RetryPolicy::RestartFromEarliestRetained,
            Self::NoVisibleChainEpoch
            | Self::NotFound { .. }
            | Self::ArtifactUnavailable { .. }
            | Self::ServiceUnavailable { .. }
            | Self::StorageUnavailable { .. }
            | Self::BlockingTaskFailed { .. } => RetryPolicy::RetryWithBackoff,
            Self::RemoteFailure { retry_policy, .. } => *retry_policy,
            Self::ReorgWindowExceeded { .. }
            | Self::FailedPrecondition { .. }
            | Self::DataLoss { .. }
            | Self::NetworkMismatch { .. } => RetryPolicy::OperatorActionRequired,
            Self::InvalidRequest { .. } | Self::MalformedResponse { .. } => {
                RetryPolicy::ClientError
            }
        }
    }

    #[cfg(feature = "remote")]
    pub(crate) fn malformed(field: &'static str, reason: impl Into<String>) -> Self {
        Self::MalformedResponse {
            field,
            reason: reason.into(),
        }
    }

    #[cfg(feature = "remote")]
    pub(crate) fn invalid_request(reason: impl Into<String>) -> Self {
        Self::InvalidRequest {
            reason: reason.into(),
        }
    }
}

#[cfg(feature = "remote")]
const fn retry_policy_for_status(code: Code) -> RetryPolicy {
    match code {
        Code::InvalidArgument | Code::OutOfRange => RetryPolicy::ClientError,
        Code::FailedPrecondition
        | Code::PermissionDenied
        | Code::Unauthenticated
        | Code::Unimplemented => RetryPolicy::OperatorActionRequired,
        Code::Ok
        | Code::Cancelled
        | Code::Unknown
        | Code::DeadlineExceeded
        | Code::NotFound
        | Code::AlreadyExists
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::Internal
        | Code::Unavailable
        | Code::DataLoss => RetryPolicy::RetryWithBackoff,
    }
}

#[cfg(feature = "remote")]
const fn retry_policy_for_remote(reason: &ErrorReason, code: Code) -> RetryPolicy {
    match reason {
        ErrorReason::ChainEpochPinUnavailable => RetryPolicy::RefreshChainEpoch,
        ErrorReason::ChainEventCursorExpired => RetryPolicy::RestartFromEarliestRetained,
        ErrorReason::InvalidBlockRange
        | ErrorReason::BlockRangeTooLarge
        | ErrorReason::SubtreeRootRangeTooLarge
        | ErrorReason::ChainEventCursorInvalid
        | ErrorReason::AddressOutputCursorInvalid
        | ErrorReason::TransparentHistoryCursorInvalid
        | ErrorReason::InvalidAddress
        | ErrorReason::UnsupportedShieldedProtocol
        | ErrorReason::InvalidChainStoreOptions
        | ErrorReason::ArtifactPayloadTooLarge
        | ErrorReason::InvalidChainEpochArtifacts
        | ErrorReason::TransparentBalanceAddressCountExceeded
        | ErrorReason::SnapshotPageCursorInvalid
        | ErrorReason::BroadcastTransactionTooLarge => RetryPolicy::ClientError,
        ErrorReason::BroadcastDisabled
        | ErrorReason::MempoolEventCursorExpired
        | ErrorReason::SnapshotPageCursorExpired
        | ErrorReason::SchemaMismatch
        | ErrorReason::SchemaTooNew
        | ErrorReason::ReorgWindowExceeded
        | ErrorReason::ChainEpochConflict
        | ErrorReason::ChainEpochNetworkMismatch
        | ErrorReason::CompactBlockPayloadMalformed
        | ErrorReason::ArtifactCorrupt
        | ErrorReason::EntropyUnavailable
        | ErrorReason::ExplorerInternal
        | ErrorReason::MaterializedViewUnavailable
        | ErrorReason::EndpointCapabilityUnavailable
        | ErrorReason::NodeCapabilityMissing
        | ErrorReason::ExplorerPreconditionUnsatisfied
        | ErrorReason::ExplorerMethodDisabled
        | ErrorReason::DependencyNotConfigured
        | ErrorReason::Unspecified => RetryPolicy::OperatorActionRequired,
        ErrorReason::ArtifactUnavailable
        | ErrorReason::ChainEpochMissing
        | ErrorReason::BlockNotInBestChain
        | ErrorReason::UnsupportedChainEvent
        | ErrorReason::UnsupportedBlockSelector
        | ErrorReason::UnsupportedTransactionStatus
        | ErrorReason::BlockingTaskFailed
        | ErrorReason::NodeUnavailable
        | ErrorReason::StorageUnavailable
        | ErrorReason::UnsupportedWalletEncoding
        | ErrorReason::NoVisibleChainEpoch
        | ErrorReason::UpstreamUnreachable
        | ErrorReason::ServiceNotReady => RetryPolicy::RetryWithBackoff,
        ErrorReason::Unknown(_) => retry_policy_for_status(code),
    }
}

#[cfg(all(test, feature = "remote"))]
mod tests {
    use tonic_types::ErrorDetails;

    use super::*;

    #[test]
    fn stale_chain_epoch_pin_round_trips_as_typed_retryable_error() {
        let status = tonic::Status::with_error_details(
            Code::FailedPrecondition,
            "requested chain epoch is not retained",
            ErrorDetails::with_error_info(
                ErrorReason::ChainEpochPinUnavailable.as_str(),
                ZINDER_ERROR_DOMAIN,
                [],
            ),
        );

        let error = IndexerError::from_status(status);

        assert!(matches!(error, IndexerError::ChainEpochPinUnavailable));
        assert_eq!(error.reason(), Some(ErrorReason::ChainEpochPinUnavailable));
        assert_eq!(error.retry_policy(), RetryPolicy::RefreshChainEpoch);
    }

    #[test]
    fn reorg_window_halt_renders_only_the_details_the_server_reported() {
        let halt = |replacement_depth_blocks, reorg_window_blocks, covered_depth_blocks| {
            IndexerError::ReorgWindowExceeded {
                replacement_depth_blocks,
                reorg_window_blocks,
                covered_depth_blocks,
            }
            .to_string()
        };
        assert_eq!(
            halt(Some(1_001), Some(1_000), Some(1_000)),
            "indexer halted: a replacement of 1001 blocks exceeds its 1000-block reorg window"
        );
        assert_eq!(
            halt(Some(400), Some(1_000), Some(300)),
            "indexer halted: a replacement of 400 blocks exceeds the 300 replaceable blocks of its 1000-block reorg window"
        );
        assert_eq!(
            halt(Some(1_001), Some(1_000), None),
            "indexer halted: a replacement of 1001 blocks exceeds its 1000-block reorg window"
        );
        assert_eq!(
            halt(None, None, None),
            "indexer halted: a replacement exceeds its reorg window"
        );
        for rendered in [halt(None, Some(1_000), None), halt(Some(5), None, Some(3))] {
            assert!(
                !rendered.contains("None") && !rendered.contains("Some("),
                "{rendered}"
            );
        }
    }

    #[test]
    fn reorg_window_halt_decodes_as_a_typed_operator_action() {
        let halted = tonic::Status::with_error_details(
            Code::FailedPrecondition,
            "the canonical writer is parked",
            ErrorDetails::with_error_info(
                ErrorReason::ReorgWindowExceeded.as_str(),
                ZINDER_ERROR_DOMAIN,
                [
                    (
                        "readiness_cause".to_owned(),
                        "reorg_window_exceeded".to_owned(),
                    ),
                    ("replacement_depth_blocks".to_owned(), "1001".to_owned()),
                    ("reorg_window_blocks".to_owned(), "1000".to_owned()),
                    ("covered_depth_blocks".to_owned(), "1000".to_owned()),
                ],
            ),
        );
        let error = IndexerError::from_status(halted);
        assert!(matches!(
            error,
            IndexerError::ReorgWindowExceeded {
                replacement_depth_blocks: Some(1_001),
                reorg_window_blocks: Some(1_000),
                covered_depth_blocks: Some(1_000),
            }
        ));
        assert_eq!(error.reason(), Some(ErrorReason::ReorgWindowExceeded));
        assert_eq!(error.retry_policy(), RetryPolicy::OperatorActionRequired);

        let without_detail = tonic::Status::with_error_details(
            Code::FailedPrecondition,
            "reorg window exceeded",
            ErrorDetails::with_error_info(
                ErrorReason::ReorgWindowExceeded.as_str(),
                ZINDER_ERROR_DOMAIN,
                [],
            ),
        );
        assert!(matches!(
            IndexerError::from_status(without_detail),
            IndexerError::ReorgWindowExceeded {
                replacement_depth_blocks: None,
                reorg_window_blocks: None,
                covered_depth_blocks: None,
            }
        ));
    }

    #[test]
    fn non_best_chain_block_maps_only_not_found_status_to_block_absence() {
        let not_found =
            zinder_query::status_from_query_error(&zinder_query::QueryError::BlockNotInBestChain);
        let unavailable = tonic::Status::with_error_details(
            Code::Unavailable,
            "block lookup is unavailable",
            ErrorDetails::with_error_info(
                ErrorReason::BlockNotInBestChain.as_str(),
                ZINDER_ERROR_DOMAIN,
                [],
            ),
        );

        let block_not_found = IndexerError::from_status(not_found);
        assert_eq!(block_not_found.reason(), None);
        assert!(matches!(
            block_not_found,
            IndexerError::NotFound { resource: "block" }
        ));
        assert!(matches!(
            IndexerError::from_status(unavailable),
            IndexerError::RemoteFailure {
                reason: ErrorReason::BlockNotInBestChain,
                ..
            }
        ));
    }

    #[test]
    fn expired_chain_event_cursor_round_trips_with_replay_recovery() {
        let status = zinder_query::status_from_query_error(
            &zinder_query::QueryError::ChainEventCursorExpired {
                event_sequence: 4,
                oldest_retained_sequence: 9,
            },
        );

        let error = IndexerError::from_status(status);

        assert!(matches!(
            error,
            IndexerError::ChainEventCursorExpired {
                recovery: ChainEventCursorRecovery::EarliestRetained,
            }
        ));
        assert_eq!(error.reason(), Some(ErrorReason::ChainEventCursorExpired));
        assert_eq!(
            error.retry_policy(),
            RetryPolicy::RestartFromEarliestRetained
        );
    }

    #[test]
    fn generic_known_remote_reason_is_preserved() {
        let status = tonic::Status::with_error_details(
            Code::InvalidArgument,
            "invalid transparent address",
            ErrorDetails::with_error_info(
                ErrorReason::InvalidAddress.as_str(),
                ZINDER_ERROR_DOMAIN,
                [],
            ),
        );

        let error = IndexerError::from_status(status);

        assert_eq!(error.reason(), Some(ErrorReason::InvalidAddress));
        assert_eq!(error.retry_policy(), RetryPolicy::ClientError);
        assert!(matches!(error, IndexerError::RemoteFailure { .. }));
    }

    #[test]
    fn subtree_root_range_limit_is_a_client_error() {
        let status = zinder_query::status_from_query_error(
            &zinder_query::QueryError::SubtreeRootRangeTooLarge {
                requested: zinder_core::MAX_SUBTREE_ROOTS_PER_REQUEST.saturating_add(1),
                maximum: zinder_core::MAX_SUBTREE_ROOTS_PER_REQUEST,
            },
        );

        let error = IndexerError::from_status(status);

        assert_eq!(error.reason(), Some(ErrorReason::SubtreeRootRangeTooLarge));
        assert_eq!(error.retry_policy(), RetryPolicy::ClientError);
        assert!(matches!(error, IndexerError::RemoteFailure { .. }));
    }

    #[test]
    fn unknown_remote_reason_is_preserved_with_status_retry_policy() {
        let status = tonic::Status::with_error_details(
            Code::Unavailable,
            "new server failure",
            ErrorDetails::with_error_info("FUTURE_SERVER_REASON", ZINDER_ERROR_DOMAIN, []),
        );

        let error = IndexerError::from_status(status);

        assert_eq!(
            error.reason(),
            Some(ErrorReason::Unknown("FUTURE_SERVER_REASON".to_owned()))
        );
        assert_eq!(error.retry_policy(), RetryPolicy::RetryWithBackoff);
        assert!(matches!(error, IndexerError::RemoteFailure { .. }));
    }

    #[test]
    fn data_loss_and_internal_reasons_are_not_retried() {
        for (code, reason) in [
            (Code::DataLoss, ErrorReason::ArtifactCorrupt),
            (Code::Internal, ErrorReason::EntropyUnavailable),
        ] {
            let status = tonic::Status::with_error_details(
                code,
                "operator intervention required",
                ErrorDetails::with_error_info(reason.as_str(), ZINDER_ERROR_DOMAIN, []),
            );

            let error = IndexerError::from_status(status);

            assert_eq!(error.reason(), Some(reason));
            assert_eq!(error.retry_policy(), RetryPolicy::OperatorActionRequired);
        }
    }
}
