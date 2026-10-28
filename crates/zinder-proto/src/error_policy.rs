//! The single authored error-reason policy.
//!
//! Every Zinder gRPC failure carries a stable [`ErrorReason`] as
//! `google.rpc.ErrorInfo{domain = "zinder.dev", reason = NAME}`. This module
//! owns the one table that maps each [`ErrorReason`] to its canonical gRPC
//! [`Code`] and retry disposition, and the one [`status_with_reason`]
//! constructor every surface calls to build a `Status`. Boundary error enums
//! (`QueryError`, `StoreError`, `ExplorerError`) keep a `fn error_reason(&self)
//! -> ErrorReason` next to their own definition and route their `Status`
//! through this seam; the proto enum is the vocabulary and this table is the
//! code-plus-retry authority.
//!
//! The outer `Status` code stays the canonical gRPC retry signal; the reason
//! rides alongside as the typed key a client pins to. `BroadcastRejectionReason`
//! stays a separate payload verdict per ADR-0023 and is never folded into
//! [`ErrorReason`].
//!
//! See [`docs/reference/error-vocabulary.md`](../../docs/reference/error-vocabulary.md).

use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt as _};

use crate::v1::ops::ErrorReason;

/// Domain set on every `google.rpc.ErrorInfo` returned by a Zinder service.
///
/// A client matches on this domain before trusting the `reason` field, so a
/// foreign service's `ErrorInfo` is never mistaken for a Zinder one.
pub const ZINDER_ERROR_DOMAIN: &str = "zinder.dev";

/// Retry disposition a client derives from a failure without parsing prose.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RetryDisposition {
    /// Retry immediately is futile; the request shape or deployment state must
    /// change first (malformed request, schema mismatch, broadcast disabled).
    NonRetryable,
    /// Retry with exponential backoff; a transient dependency is unavailable
    /// or the resource may appear after a future commit.
    RetryAfterBackoff,
    /// Retry is safe without backoff; the failure is a momentary contention
    /// signal rather than a sustained outage.
    Retryable,
}

/// Canonical gRPC code and retry disposition for one [`ErrorReason`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReasonPolicy {
    /// gRPC status code carried as the outer retry signal.
    pub code: Code,
    /// Retry disposition a client maps to local handling.
    pub retry: RetryDisposition,
}

impl ReasonPolicy {
    const fn new(code: Code, retry: RetryDisposition) -> Self {
        Self { code, retry }
    }
}

/// Maps an [`ErrorReason`] to its canonical gRPC code and retry disposition.
///
/// This is the single source for code and retry; the match is exhaustive so a
/// new reason cannot be added without a policy entry. `ERROR_REASON_UNSPECIFIED`
/// is never produced by a boundary error and is mapped here only so the match
/// stays total; a client that observes it has hit a server bug.
#[must_use]
pub const fn reason_policy(reason: ErrorReason) -> ReasonPolicy {
    use RetryDisposition::{NonRetryable, RetryAfterBackoff};

    match reason {
        ErrorReason::Unspecified => ReasonPolicy::new(Code::Internal, NonRetryable),

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
        | ErrorReason::BroadcastTransactionTooLarge
        | ErrorReason::SnapshotPageCursorInvalid => {
            ReasonPolicy::new(Code::InvalidArgument, NonRetryable)
        }

        ErrorReason::BroadcastDisabled
        | ErrorReason::ChainEventCursorExpired
        | ErrorReason::MempoolEventCursorExpired
        | ErrorReason::SnapshotPageCursorExpired
        | ErrorReason::ChainEpochPinUnavailable
        | ErrorReason::SchemaMismatch
        | ErrorReason::SchemaTooNew
        | ErrorReason::ReorgWindowExceeded
        | ErrorReason::ChainEpochNetworkMismatch
        | ErrorReason::MaterializedViewUnavailable
        | ErrorReason::EndpointCapabilityUnavailable
        | ErrorReason::DependencyNotConfigured
        | ErrorReason::NodeCapabilityMissing
        | ErrorReason::ExplorerPreconditionUnsatisfied
        | ErrorReason::CanonicalHistoryUnavailable => {
            ReasonPolicy::new(Code::FailedPrecondition, NonRetryable)
        }

        ErrorReason::ArtifactUnavailable
        | ErrorReason::ChainEpochMissing
        | ErrorReason::BlockNotInBestChain
        | ErrorReason::TransactionNotFound => ReasonPolicy::new(Code::NotFound, RetryAfterBackoff),

        ErrorReason::CompactBlockPayloadMalformed | ErrorReason::ArtifactCorrupt => {
            ReasonPolicy::new(Code::DataLoss, NonRetryable)
        }

        ErrorReason::UnsupportedChainEvent
        | ErrorReason::UnsupportedBlockSelector
        | ErrorReason::UnsupportedTransactionStatus
        | ErrorReason::UnsupportedWalletEncoding
        | ErrorReason::BlockingTaskFailed
        | ErrorReason::NodeUnavailable
        | ErrorReason::StorageUnavailable
        | ErrorReason::UpstreamUnreachable
        | ErrorReason::ServiceNotReady
        | ErrorReason::NoVisibleChainEpoch => {
            ReasonPolicy::new(Code::Unavailable, RetryAfterBackoff)
        }

        ErrorReason::EntropyUnavailable | ErrorReason::ExplorerInternal => {
            ReasonPolicy::new(Code::Internal, NonRetryable)
        }

        ErrorReason::ExplorerMethodDisabled => ReasonPolicy::new(Code::Unimplemented, NonRetryable),

        // A reader moved off its pinned epoch, or two commits raced for the
        // next epoch. Re-reading at a fresh epoch clears it.
        ErrorReason::ChainEpochConflict => ReasonPolicy::new(Code::Aborted, RetryAfterBackoff),
    }
}

/// Builds a `Status` for `reason` with `message` and structured `details`.
///
/// The code is read from [`reason_policy`] so the wire code and the typed
/// reason can never disagree, and the reason rides as
/// `google.rpc.ErrorInfo{domain = "zinder.dev", reason = NAME}`. Surfaces that
/// attach typed `BadRequest`/`PreconditionFailure`/`ResourceInfo` detail pass
/// it in `details`; the rest pass [`ErrorDetails::new`].
#[must_use]
pub fn status_with_reason(
    reason: ErrorReason,
    message: impl Into<String>,
    details: ErrorDetails,
) -> Status {
    status_with_reason_and_metadata(reason, message, details, std::collections::HashMap::new())
}

/// Builds a `Status` for `reason` with `message`, structured `details`, and
/// stable automation metadata attached to `google.rpc.ErrorInfo`.
///
/// Use this only when a reason has a documented metadata contract. Free-form
/// diagnostics belong in the status message or a typed detail message.
#[must_use]
pub fn status_with_reason_and_metadata(
    reason: ErrorReason,
    message: impl Into<String>,
    mut details: ErrorDetails,
    metadata: impl IntoIterator<Item = (String, String)>,
) -> Status {
    details.set_error_info(
        reason.as_str_name(),
        ZINDER_ERROR_DOMAIN,
        metadata
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>(),
    );
    Status::with_error_details(reason_policy(reason).code, message.into(), details)
}

/// Builds a `Status` for `reason` with `message` and no structured detail.
///
/// The detail-free companion of [`status_with_reason`] for surfaces whose
/// failures carry only the typed reason. The code still comes from
/// [`reason_policy`].
#[must_use]
pub fn status_for_reason(reason: ErrorReason, message: impl Into<String>) -> Status {
    status_with_reason(reason, message, ErrorDetails::new())
}

/// A boundary error enum that names its stable [`ErrorReason`].
///
/// Each library boundary (`QueryError`, `StoreError`, `ExplorerError`)
/// implements this next to its definition so the reason lives with the variant
/// it describes, while code and retry stay centralized in [`reason_policy`].
pub trait BoundaryError {
    /// Returns the stable reason for this failure.
    fn error_reason(&self) -> ErrorReason;
}
