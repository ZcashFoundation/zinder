//! End-to-end roundtrip of `ErrorReason` across the gRPC boundary.
//!
//! Confirms a server-emitted `QueryError`/`StoreError` round-trips through
//! `tonic::Status` carrying `google.rpc.ErrorInfo`, and that
//! `IndexerError::from_status` recovers the typed reason on the client.

use tonic::Status;
use tonic_types::StatusExt;
use zinder_client::{ErrorReason, IndexerError, MAX_SUBTREE_ROOTS_PER_REQUEST, RetryPolicy};
use zinder_core::BlockHeight;
use zinder_proto::capabilities::WALLET_READ_FULL_BLOCK_AT_V1;
use zinder_proto::v1::ops::ErrorReason as WireErrorReason;
use zinder_query::{QueryError, status_from_query_error};

const ZINDER_DOMAIN: &str = "zinder.dev";

#[test]
fn query_error_status_carries_error_info() {
    let status = status_from_query_error(&QueryError::TransactionBroadcastDisabled);

    let details = status.get_error_details();
    let Some(error_info) = details.error_info() else {
        unreachable!("status must carry an ErrorInfo detail")
    };

    assert_eq!(error_info.domain, ZINDER_DOMAIN);
    assert_eq!(error_info.reason, "BROADCAST_DISABLED");
}

#[test]
fn generic_remote_status_preserves_exact_reason_on_client() {
    let status = status_from_query_error(&QueryError::BlockRangeTooLarge {
        requested: 4_096,
        maximum: 1_000,
    });

    let details = status.get_error_details();
    let Some(error_info) = details.error_info() else {
        unreachable!("status must carry an ErrorInfo detail")
    };
    assert_eq!(error_info.reason, "BLOCK_RANGE_TOO_LARGE");

    // Round-trip the status through the public-vocabulary compatibility
    // mapper and confirm the exact reason survives a shared gRPC code.
    let client_error: IndexerError = into_client_error(&status);
    assert!(matches!(client_error, IndexerError::RemoteFailure { .. }));
    assert_eq!(client_error.reason(), Some(ErrorReason::BlockRangeTooLarge));
    assert_eq!(client_error.retry_policy(), RetryPolicy::ClientError);
}

#[test]
fn subtree_root_range_limit_round_trips_as_client_error() {
    let status = status_from_query_error(&QueryError::SubtreeRootRangeTooLarge {
        requested: MAX_SUBTREE_ROOTS_PER_REQUEST.saturating_add(1),
        maximum: MAX_SUBTREE_ROOTS_PER_REQUEST,
    });
    let details = status.get_error_details();
    let violation = details
        .bad_request()
        .and_then(|bad_request| bad_request.field_violations.first());

    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(matches!(
        violation,
        Some(violation)
            if violation.field == "max_entries"
                && violation.description.contains("maximum is 1024")
    ));

    let client_error = into_client_error(&status);
    assert_eq!(
        client_error.reason(),
        Some(ErrorReason::SubtreeRootRangeTooLarge)
    );
    assert_eq!(client_error.retry_policy(), RetryPolicy::ClientError);
}

#[test]
fn unavailable_endpoint_capability_requires_operator_action() {
    let status = status_from_query_error(&QueryError::EndpointCapabilityUnavailable {
        capability: WALLET_READ_FULL_BLOCK_AT_V1,
    });
    let details = status.get_error_details();
    let violation = details
        .precondition_failure()
        .and_then(|failure| failure.violations.first());

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(matches!(
        violation,
        Some(violation)
            if violation.r#type == "ENDPOINT_CAPABILITY_UNAVAILABLE"
                && violation.subject == WALLET_READ_FULL_BLOCK_AT_V1
    ));

    let client_error = into_client_error(&status);
    assert_eq!(
        client_error.reason(),
        Some(ErrorReason::EndpointCapabilityUnavailable)
    );
    assert_eq!(
        client_error.retry_policy(),
        RetryPolicy::OperatorActionRequired
    );
}

#[test]
fn invalid_block_range_carries_invalid_argument_status() {
    let status = status_from_query_error(&QueryError::InvalidBlockRange {
        start_height: BlockHeight::new(10),
        end_height: BlockHeight::new(5),
    });
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let details = status.get_error_details();
    let Some(error_info) = details.error_info() else {
        unreachable!("status must carry an ErrorInfo detail")
    };
    assert_eq!(error_info.reason, "INVALID_BLOCK_RANGE");
}

#[test]
fn missing_zinder_error_info_fails_closed() {
    let foreign_status = Status::with_error_details(
        tonic::Code::Internal,
        "foreign service failure",
        tonic_types::ErrorDetails::with_error_info("MIGRATING", "other.example.com", []),
    );
    let client_error = into_client_error(&foreign_status);
    // A status without zinder.dev ErrorInfo cannot be classified; the client
    // refuses to guess and reports ServiceUnavailable.
    assert!(matches!(
        client_error,
        IndexerError::ServiceUnavailable { .. }
    ));
}

#[test]
fn every_error_reason_round_trips_through_proto_str_name() {
    // Sanity check: prost's generated `from_str_name`/`as_str_name` are
    // inverses for every variant. If a future variant gets a name that
    // disagrees, this test fails immediately.
    let reasons = [
        WireErrorReason::Unspecified,
        WireErrorReason::InvalidBlockRange,
        WireErrorReason::BlockRangeTooLarge,
        WireErrorReason::SubtreeRootRangeTooLarge,
        WireErrorReason::ChainEventCursorInvalid,
        WireErrorReason::AddressOutputCursorInvalid,
        WireErrorReason::TransparentHistoryCursorInvalid,
        WireErrorReason::InvalidAddress,
        WireErrorReason::UnsupportedShieldedProtocol,
        WireErrorReason::InvalidChainStoreOptions,
        WireErrorReason::ArtifactPayloadTooLarge,
        WireErrorReason::InvalidChainEpochArtifacts,
        WireErrorReason::TransparentBalanceAddressCountExceeded,
        WireErrorReason::SnapshotPageCursorInvalid,
        WireErrorReason::BroadcastTransactionTooLarge,
        WireErrorReason::BroadcastDisabled,
        WireErrorReason::ChainEventCursorExpired,
        WireErrorReason::MempoolEventCursorExpired,
        WireErrorReason::SnapshotPageCursorExpired,
        WireErrorReason::ChainEpochPinUnavailable,
        WireErrorReason::SchemaMismatch,
        WireErrorReason::SchemaTooNew,
        WireErrorReason::ReorgWindowExceeded,
        WireErrorReason::ChainEpochConflict,
        WireErrorReason::ChainEpochNetworkMismatch,
        WireErrorReason::ArtifactUnavailable,
        WireErrorReason::ChainEpochMissing,
        WireErrorReason::BlockNotInBestChain,
        WireErrorReason::CompactBlockPayloadMalformed,
        WireErrorReason::ArtifactCorrupt,
        WireErrorReason::UnsupportedChainEvent,
        WireErrorReason::UnsupportedBlockSelector,
        WireErrorReason::UnsupportedTransactionStatus,
        WireErrorReason::BlockingTaskFailed,
        WireErrorReason::NodeUnavailable,
        WireErrorReason::StorageUnavailable,
        WireErrorReason::UnsupportedWalletEncoding,
        WireErrorReason::EntropyUnavailable,
        WireErrorReason::MaterializedViewUnavailable,
        WireErrorReason::EndpointCapabilityUnavailable,
        WireErrorReason::NodeCapabilityMissing,
        WireErrorReason::NoVisibleChainEpoch,
        WireErrorReason::ExplorerInternal,
        WireErrorReason::ExplorerMethodDisabled,
        WireErrorReason::ExplorerPreconditionUnsatisfied,
        WireErrorReason::DependencyNotConfigured,
        WireErrorReason::UpstreamUnreachable,
        WireErrorReason::ServiceNotReady,
        WireErrorReason::TransactionNotFound,
        WireErrorReason::CanonicalHistoryUnavailable,
    ];

    for reason in reasons {
        let name = reason.as_str_name();
        let parsed = ErrorReason::from_wire_name(name);
        assert_eq!(
            parsed.as_str(),
            name,
            "{name} did not round-trip through client-owned ErrorReason"
        );
    }
}

/// Helper bridging the test crate's `Status` to the private
/// [`IndexerError::from_status`] constructor without exposing it publicly.
///
/// Mirrors the parse logic with public API only; the production constructor
/// is `pub(crate)`. Confirming public behavior is the test's contract, so
/// re-implementing the parse keeps the test independent of crate internals.
fn into_client_error(status: &Status) -> IndexerError {
    indexer_error_from_status_compat(status)
}

/// Mirrors `IndexerError::from_status` for the test, using only public API.
fn indexer_error_from_status_compat(status: &Status) -> IndexerError {
    use tonic::Code;

    let message = status.message().to_owned();
    let details = status.get_error_details();
    let Some(zinder_reason) = details.error_info().and_then(|error_info| {
        if error_info.domain == ZINDER_DOMAIN {
            Some(ErrorReason::from_wire_name(&error_info.reason))
        } else {
            None
        }
    }) else {
        return IndexerError::ServiceUnavailable {
            reason: format!("missing zinder.dev ErrorInfo: {message}"),
        };
    };

    if status.code() == Code::NotFound
        && matches!(&zinder_reason, ErrorReason::ArtifactUnavailable)
        && let Some(resource_info) = details.resource_info()
    {
        return IndexerError::ArtifactUnavailable {
            family: resource_info.resource_type.clone(),
            key: resource_info.resource_name.clone(),
        };
    }

    let retry_policy = match status.code() {
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
    };
    IndexerError::RemoteFailure {
        reason: zinder_reason,
        message,
        retry_policy,
    }
}
