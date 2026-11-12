//! Native gRPC adapter and protobuf encoders for the wallet query plane.

mod adapter;
mod chain_events;
mod native;

use tonic::Status;
use tonic_types::{ErrorDetails, FieldViolation, PreconditionViolation};
use zinder_proto::capabilities::WALLET_BROADCAST_TRANSACTION_V1;
use zinder_proto::wire::canonical_history_unavailable_status;
use zinder_proto::{BoundaryError, status_with_reason};
use zinder_store::status_from_store_error;

use crate::QueryError;

pub use adapter::WalletQueryGrpcAdapter;
pub use native::{
    TransactionStatusLookup, WalletEndpointMetadata, address_lookup_to_script_hash,
    block_header_by_selector_response, block_id_by_selector_response,
    broadcast_transaction_response, build_transparent_address_tx_ids_chunk,
    build_transparent_address_tx_ids_header, build_transparent_unspent_output_message,
    build_transparent_unspent_outputs_header, build_wallet_server_info, chain_events_response,
    chain_value_pools_at_tip_response, compact_block_response, full_block_response,
    latest_tree_state_checkpoint_response, network_upgrade_activations_response,
    subtree_roots_response, transaction_response, transparent_address_tx_ids_response,
    transparent_address_unspent_outputs_response, transparent_outputs_by_outpoint_response,
    transparent_spends_by_outpoint_response, transparent_unspent_outputs_by_outpoint_response,
    tree_state_at_response, visible_tip_block_response,
};

/// Maps a [`QueryError`] to a tonic [`Status`] using the canonical mapping
/// from [`Public Interfaces §Error Conventions`](../../../docs/architecture/public-interfaces.md#error-conventions).
///
/// This is the single source of truth for `QueryError` to gRPC translation.
/// The native [`WalletQueryGrpcAdapter`] calls this function instead of
/// duplicating the mapping. The reason comes from [`QueryError::error_reason`] and the code
/// from the shared reason policy; this function attaches the typed
/// `BadRequest`/`PreconditionFailure`/`ResourceInfo` detail per variant.
#[must_use]
pub fn status_from_query_error(error: &QueryError) -> Status {
    if let QueryError::Store(store_error) = error {
        return status_from_store_error(store_error);
    }
    if let QueryError::CanonicalHistoryUnavailable {
        first_available_height,
    } = error
    {
        return canonical_history_unavailable_status(
            *first_available_height,
            error.to_string(),
            precondition_failure_details(error),
        );
    }
    status_with_reason(
        error.error_reason(),
        error.to_string(),
        typed_detail_for(error),
    )
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Only request-validation and precondition variants carry structured detail; every other query error rides its reason via ErrorInfo with an empty detail set."
)]
fn typed_detail_for(error: &QueryError) -> ErrorDetails {
    match error {
        QueryError::InvalidBlockRange { .. }
        | QueryError::BlockRangeTooLarge { .. }
        | QueryError::SubtreeRootRangeTooLarge { .. }
        | QueryError::ChainEventCursorInvalid { .. }
        | QueryError::TransparentHistoryCursorInvalid { .. }
        | QueryError::InvalidAddress { .. }
        | QueryError::UnsupportedShieldedProtocol { .. }
        | QueryError::BroadcastTransactionTooLarge { .. } => bad_request_details(error),
        QueryError::TransactionBroadcastDisabled
        | QueryError::MaterializedViewUnavailable { .. }
        | QueryError::EndpointCapabilityUnavailable { .. }
        | QueryError::ChainEventCursorExpired { .. }
        | QueryError::ChainEpochPinUnavailable { .. } => precondition_failure_details(error),
        QueryError::ArtifactUnavailable { family, key } => ErrorDetails::with_resource_info(
            family.wire_label(),
            key.to_string(),
            "zinder-query",
            "artifact is not available in the selected chain epoch",
        ),
        _ => ErrorDetails::new(),
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Only invalid-request variants carry BadRequest details; all other query errors intentionally return empty detail sets here."
)]
fn bad_request_details(error: &QueryError) -> ErrorDetails {
    match error {
        QueryError::InvalidBlockRange {
            start_height,
            end_height,
        } => ErrorDetails::with_bad_request(vec![
            FieldViolation::new(
                "start_height",
                format!(
                    "start height {} exceeds end height {}",
                    start_height.value(),
                    end_height.value()
                ),
            ),
            FieldViolation::new(
                "end_height",
                format!(
                    "end height {} is below start height {}",
                    end_height.value(),
                    start_height.value()
                ),
            ),
        ]),
        QueryError::BlockRangeTooLarge { requested, maximum } => {
            ErrorDetails::with_bad_request_violation(
                "end_height",
                format!("requested {requested} blocks; maximum is {maximum}"),
            )
        }
        QueryError::SubtreeRootRangeTooLarge { requested, maximum } => {
            ErrorDetails::with_bad_request_violation(
                "max_entries",
                format!("requested {requested} subtree roots; maximum is {maximum}"),
            )
        }
        QueryError::ChainEventCursorInvalid { reason }
        | QueryError::TransparentHistoryCursorInvalid { reason } => {
            ErrorDetails::with_bad_request_violation("from_cursor", *reason)
        }
        QueryError::InvalidAddress { reason } => {
            ErrorDetails::with_bad_request_violation("address", *reason)
        }
        QueryError::UnsupportedShieldedProtocol { protocol } => {
            ErrorDetails::with_bad_request_violation(
                "shielded_protocol",
                format!("{protocol:?} is not supported by the native wallet protocol"),
            )
        }
        QueryError::BroadcastTransactionTooLarge { actual, maximum } => {
            ErrorDetails::with_bad_request_violation(
                "raw_transaction",
                format!("transaction is {actual} bytes; maximum is {maximum}"),
            )
        }
        _ => ErrorDetails::new(),
    }
}

#[allow(
    clippy::wildcard_enum_match_arm,
    reason = "Only failed-precondition variants carry PreconditionFailure details; all other query errors intentionally return empty detail sets here."
)]
fn precondition_failure_details(error: &QueryError) -> ErrorDetails {
    match error {
        QueryError::TransactionBroadcastDisabled => {
            ErrorDetails::with_precondition_failure_violation(
                "TRANSACTION_BROADCAST_DISABLED",
                WALLET_BROADCAST_TRANSACTION_V1,
                "transaction broadcast is not configured for this deployment",
            )
        }
        QueryError::MaterializedViewUnavailable { capability } => {
            ErrorDetails::with_precondition_failure_violation(
                "MATERIALIZED_VIEW_UNAVAILABLE",
                *capability,
                "materialized view is not configured for this deployment",
            )
        }
        QueryError::EndpointCapabilityUnavailable { capability } => {
            ErrorDetails::with_precondition_failure_violation(
                "ENDPOINT_CAPABILITY_UNAVAILABLE",
                *capability,
                "the composed endpoint does not structurally implement this capability",
            )
        }
        QueryError::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence,
        } => ErrorDetails::with_precondition_failure(vec![PreconditionViolation::new(
            "CHAIN_EVENT_CURSOR_EXPIRED",
            format!("chain_event:{event_sequence}"),
            format!("oldest retained chain event sequence is {oldest_retained_sequence}"),
        )]),
        QueryError::ChainEpochPinUnavailable { chain_epoch_id } => {
            ErrorDetails::with_precondition_failure_violation(
                "CHAIN_EPOCH_PIN_UNAVAILABLE",
                format!("chain_epoch:{}", chain_epoch_id.value()),
                "requested chain epoch is not retained",
            )
        }
        QueryError::CanonicalHistoryUnavailable {
            first_available_height,
        } => ErrorDetails::with_precondition_failure_violation(
            "CANONICAL_HISTORY_UNAVAILABLE",
            format!("canonical_history:{}", first_available_height.value()),
            "the store retains canonical history only from this height",
        ),
        _ => ErrorDetails::new(),
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;
    use tonic_types::StatusExt as _;
    use zinder_core::{BlockHeight, MAX_SUBTREE_ROOTS_PER_REQUEST};

    use super::*;

    #[test]
    fn expired_cursor_status_carries_precondition_failure_detail() {
        let status = status_from_query_error(&QueryError::ChainEventCursorExpired {
            event_sequence: 4,
            oldest_retained_sequence: 9,
        });
        let details = status.get_error_details();
        let violation = details
            .precondition_failure()
            .and_then(|failure| failure.violations.first())
            .cloned();

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert!(matches!(
            violation,
            Some(violation)
                if violation.r#type == "CHAIN_EVENT_CURSOR_EXPIRED"
                    && violation.subject == "chain_event:4"
                    && violation.description.contains('9')
        ));
    }

    #[test]
    fn invalid_block_range_status_carries_bad_request_detail() {
        let status = status_from_query_error(&QueryError::InvalidBlockRange {
            start_height: BlockHeight::new(10),
            end_height: BlockHeight::new(5),
        });
        let details = status.get_error_details();
        let fields = details
            .bad_request()
            .map(|bad_request| {
                bad_request
                    .field_violations
                    .iter()
                    .map(|violation| violation.field.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(fields, vec!["start_height", "end_height"]);
    }

    #[test]
    fn oversized_subtree_root_range_status_names_max_entries() {
        let status = status_from_query_error(&QueryError::SubtreeRootRangeTooLarge {
            requested: MAX_SUBTREE_ROOTS_PER_REQUEST.saturating_add(1),
            maximum: MAX_SUBTREE_ROOTS_PER_REQUEST,
        });
        let details = status.get_error_details();
        let violation = details
            .bad_request()
            .and_then(|bad_request| bad_request.field_violations.first());

        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(matches!(
            violation,
            Some(violation)
                if violation.field == "max_entries"
                    && violation.description.contains("maximum is 1024")
        ));
    }
}
