//! gRPC status mapping for storage-boundary errors.

use tonic::Status;
use tonic_types::ErrorDetails;
use zinder_proto::v1::ops::ErrorReason;
use zinder_proto::v1::wallet;
use zinder_proto::{BoundaryError, status_with_reason};

use crate::proto_codec::{chain_event_stream_family_from_message, event_stream_start_from_message};
use crate::{ChainEventStreamFamily, EventStreamStartPosition, StoreError};

/// Decodes the wire `EventStreamStart` carried by a stream-subscribe request,
/// mapping an unset or empty start position to `INVALID_ARGUMENT`.
pub fn event_stream_start_from_request(
    start: Option<wallet::EventStreamStart>,
) -> Result<EventStreamStartPosition, Status> {
    event_stream_start_from_message(start)
        .ok_or_else(|| Status::invalid_argument("event-stream start position is required"))
}

/// Decodes the wire chain-event stream family carried by a stream-subscribe
/// request, mapping unknown integers to `INVALID_ARGUMENT`.
pub fn chain_event_stream_family_from_request(
    family: i32,
) -> Result<ChainEventStreamFamily, Status> {
    chain_event_stream_family_from_message(family)
        .ok_or_else(|| Status::invalid_argument("chain-event stream family is unknown"))
}

/// Maps a [`StoreError`] to the canonical gRPC status used by all services.
///
/// The reason comes from [`StoreError::error_reason`] and the code from the
/// shared reason policy. Storage-envelope failures carry their reason through
/// `ErrorInfo` alone and attach no structured detail.
#[must_use]
pub fn status_from_store_error(error: &StoreError) -> Status {
    status_with_reason(error.error_reason(), error.to_string(), ErrorDetails::new())
}

impl BoundaryError for StoreError {
    /// Maps each [`StoreError`] variant to its stable [`ErrorReason`].
    fn error_reason(&self) -> ErrorReason {
        match self {
            Self::InvalidChainEpochArtifacts { .. } => ErrorReason::InvalidChainEpochArtifacts,
            Self::ArtifactCorrupt { .. } => ErrorReason::ArtifactCorrupt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ArtifactFamily;
    use crate::store_error::StorageKey;

    /// One representative of every [`StoreError`] variant.
    fn one_of_each_variant() -> Vec<StoreError> {
        vec![
            StoreError::InvalidChainEpochArtifacts { reason: "probe" },
            StoreError::ArtifactCorrupt {
                family: ArtifactFamily::MempoolEvent,
                key: StorageKey::from(crate::format::StoreKey::mempool_event(1)),
                reason: "probe",
            },
        ]
    }

    #[test]
    fn no_store_error_variant_maps_to_unspecified() {
        for error in one_of_each_variant() {
            assert_ne!(
                error.error_reason(),
                ErrorReason::Unspecified,
                "StoreError variant {error:?} mapped to ERROR_REASON_UNSPECIFIED"
            );
        }
    }
}
