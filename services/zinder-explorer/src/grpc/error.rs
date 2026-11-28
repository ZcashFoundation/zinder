//! Typed failure vocabulary for the explorer plane.
//!
//! Every `ExplorerQuery` handler builds its `tonic::Status` from an
//! [`ExplorerError`] so each failure carries a stable
//! [`ErrorReason`](zinder_proto::v1::ops::ErrorReason) attached as
//! `google.rpc.ErrorInfo`. The variants are the failure categories the plane
//! actually expresses; the gRPC code each yields is fixed by the shared reason
//! policy in `zinder-proto`, so a handler never chooses a code directly.

use thiserror::Error;
use tonic::Status;
use zinder_proto::v1::ops::ErrorReason;
use zinder_proto::{BoundaryError, status_for_reason};
use zinder_runtime::RetryPushback;

/// Explorer-plane failure category.
///
/// Each variant maps to one [`ErrorReason`] (and thus one gRPC code) through
/// the shared reason policy: `InvalidRequest` to `INVALID_ARGUMENT`,
/// `DependencyNotConfigured` to `FAILED_PRECONDITION`, `UpstreamUnreachable`
/// to `UNAVAILABLE`, `NotMaterialized` to `NOT_FOUND`,
/// `UnsatisfiedPrecondition` to `FAILED_PRECONDITION`, `Unsupported` to
/// `UNIMPLEMENTED`, and `Internal` to `INTERNAL`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub(crate) enum ExplorerError {
    /// Request shape failed validation (bad selector, cursor length, or range).
    #[error("{0}")]
    InvalidRequest(String),

    /// A federated dependency the request needs is not wired on this
    /// deployment. The operator must configure it; waiting never clears it.
    #[error("{0}")]
    DependencyNotConfigured(String),

    /// A configured federated endpoint is temporarily unreachable. A
    /// retry with backoff may succeed once the endpoint recovers.
    #[error("{0}")]
    UpstreamUnreachable(String),

    /// The requested resource is not materialized in the explorer's materialized view.
    #[error("{0}")]
    NotMaterialized(String),

    /// The deployment state cannot serve the request without reconfiguration.
    #[error("{0}")]
    UnsatisfiedPrecondition(String),

    /// The method is disabled on this server.
    #[error("{0}")]
    Unsupported(String),

    /// The explorer hit an unexpected internal condition (decode failure,
    /// missing required field on a federated response, key-shape mismatch).
    #[error("{0}")]
    Internal(String),
}

impl ExplorerError {
    /// Constructs an [`ExplorerError::InvalidRequest`].
    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest(message.into())
    }

    /// Constructs an [`ExplorerError::DependencyNotConfigured`].
    pub(crate) fn dependency_not_configured(message: impl Into<String>) -> Self {
        Self::DependencyNotConfigured(message.into())
    }

    /// Constructs an [`ExplorerError::UpstreamUnreachable`].
    pub(crate) fn upstream_unreachable(message: impl Into<String>) -> Self {
        Self::UpstreamUnreachable(message.into())
    }

    /// Constructs an [`ExplorerError::NotMaterialized`].
    pub(crate) fn not_materialized(message: impl Into<String>) -> Self {
        Self::NotMaterialized(message.into())
    }

    /// Constructs an [`ExplorerError::UnsatisfiedPrecondition`].
    pub(crate) fn unsatisfied_precondition(message: impl Into<String>) -> Self {
        Self::UnsatisfiedPrecondition(message.into())
    }

    /// Constructs an [`ExplorerError::Unsupported`].
    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }

    /// Constructs an [`ExplorerError::Internal`].
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}

impl BoundaryError for ExplorerError {
    fn error_reason(&self) -> ErrorReason {
        match self {
            Self::InvalidRequest(_) => ErrorReason::InvalidAddress,
            Self::DependencyNotConfigured(_) => ErrorReason::DependencyNotConfigured,
            Self::UpstreamUnreachable(_) => ErrorReason::UpstreamUnreachable,
            Self::NotMaterialized(_) => ErrorReason::ArtifactUnavailable,
            Self::UnsatisfiedPrecondition(_) => ErrorReason::ExplorerPreconditionUnsatisfied,
            Self::Unsupported(_) => ErrorReason::ExplorerMethodDisabled,
            Self::Internal(_) => ErrorReason::ExplorerInternal,
        }
    }
}

impl From<ExplorerError> for Status {
    fn from(error: ExplorerError) -> Self {
        let status = status_for_reason(error.error_reason(), error.to_string());
        if matches!(error, ExplorerError::UpstreamUnreachable(_)) {
            RetryPushback::Backoff.apply(status)
        } else {
            status
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One representative of every [`ExplorerError`] variant.
    ///
    /// The match is exhaustive, so a new variant fails to compile until it is
    /// listed; [`no_explorer_error_variant_maps_to_unspecified`] then asserts
    /// the new variant carries a real reason.
    fn one_of_each_variant() -> Vec<ExplorerError> {
        vec![
            ExplorerError::invalid_request("probe"),
            ExplorerError::dependency_not_configured("probe"),
            ExplorerError::upstream_unreachable("probe"),
            ExplorerError::not_materialized("probe"),
            ExplorerError::unsatisfied_precondition("probe"),
            ExplorerError::unsupported("probe"),
            ExplorerError::internal("probe"),
        ]
    }

    #[test]
    fn no_explorer_error_variant_maps_to_unspecified() {
        for error in one_of_each_variant() {
            assert_ne!(
                error.error_reason(),
                ErrorReason::Unspecified,
                "ExplorerError variant {error:?} mapped to ERROR_REASON_UNSPECIFIED"
            );
        }
    }
}
