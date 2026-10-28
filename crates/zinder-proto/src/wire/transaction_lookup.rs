//! Typed `ErrorInfo` metadata for transaction-lookup outcomes.
//!
//! A proven transaction absence and an incomplete-history refusal both travel
//! as gRPC statuses whose `google.rpc.ErrorInfo` metadata carries the facts a
//! caller needs to judge the answer. This module owns the metadata keys and the
//! encode/decode pair, so the query plane, the ingest control plane, and the
//! Rust client agree on one shape.

use std::collections::HashMap;

use thiserror::Error;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt as _};
use zinder_core::{BlockHeight, ChainEpoch, ChainEpochId};

use crate::error_policy::{ZINDER_ERROR_DOMAIN, status_with_reason_and_metadata};
use crate::v1::ops::ErrorReason;

/// `TRANSACTION_NOT_FOUND` metadata key naming the answering chain epoch.
pub const TRANSACTION_ABSENCE_CHAIN_EPOCH_ID_KEY: &str = "chain_epoch_id";
/// `TRANSACTION_NOT_FOUND` metadata key naming that epoch's visible tip height.
pub const TRANSACTION_ABSENCE_VISIBLE_TIP_HEIGHT_KEY: &str = "visible_tip_height";
/// `TRANSACTION_NOT_FOUND` metadata key stating whether the live mempool was searched.
pub const TRANSACTION_ABSENCE_COVERS_MEMPOOL_KEY: &str = "covers_mempool";
/// `CANONICAL_HISTORY_UNAVAILABLE` metadata key naming the first retained height.
pub const CANONICAL_HISTORY_FIRST_AVAILABLE_HEIGHT_KEY: &str = "first_available_height";

/// Proven transaction absence carried by a `TRANSACTION_NOT_FOUND` status.
///
/// The transaction is absent from the canonical chain of `chain_epoch_id`,
/// whose retained history is complete through `visible_tip_height`. When
/// `covers_mempool` is true, the writer's live mempool, coherent with the
/// same epoch, did not hold it either.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionAbsence {
    /// Chain epoch whose canonical chain was searched.
    pub chain_epoch_id: ChainEpochId,
    /// Visible tip height of that epoch; the searched canonical range ends here.
    pub visible_tip_height: BlockHeight,
    /// Whether the writer's live mempool at the same epoch was searched too.
    pub covers_mempool: bool,
}

impl TransactionAbsence {
    /// Builds the absence verdict for `chain_epoch`.
    #[must_use]
    pub const fn at_epoch(chain_epoch: ChainEpoch, covers_mempool: bool) -> Self {
        Self {
            chain_epoch_id: chain_epoch.id,
            visible_tip_height: chain_epoch.visible_tip_height,
            covers_mempool,
        }
    }

    /// Encodes this verdict as a `NOT_FOUND` status with reason
    /// `TRANSACTION_NOT_FOUND`.
    #[must_use]
    pub fn into_status(self, message: impl Into<String>) -> Status {
        status_with_reason_and_metadata(
            ErrorReason::TransactionNotFound,
            message,
            ErrorDetails::new(),
            [
                (
                    TRANSACTION_ABSENCE_CHAIN_EPOCH_ID_KEY.to_owned(),
                    self.chain_epoch_id.value().to_string(),
                ),
                (
                    TRANSACTION_ABSENCE_VISIBLE_TIP_HEIGHT_KEY.to_owned(),
                    self.visible_tip_height.value().to_string(),
                ),
                (
                    TRANSACTION_ABSENCE_COVERS_MEMPOOL_KEY.to_owned(),
                    self.covers_mempool.to_string(),
                ),
            ],
        )
    }

    /// Decodes a `TRANSACTION_NOT_FOUND` status.
    ///
    /// Returns `Ok(None)` when `status` is not a Zinder `TRANSACTION_NOT_FOUND`
    /// status, including every other `NOT_FOUND`. Returns an error when the
    /// status claims the reason but its metadata is missing or malformed.
    pub fn from_status(status: &Status) -> Result<Option<Self>, TransactionLookupMetadataError> {
        let Some(metadata) = zinder_metadata_for(status, ErrorReason::TransactionNotFound) else {
            return Ok(None);
        };
        if status.code() != Code::NotFound {
            return Err(TransactionLookupMetadataError::UnexpectedCode {
                code: status.code(),
            });
        }
        let chain_epoch_id = parse_metadata(&metadata, TRANSACTION_ABSENCE_CHAIN_EPOCH_ID_KEY)?;
        let visible_tip_height =
            parse_metadata(&metadata, TRANSACTION_ABSENCE_VISIBLE_TIP_HEIGHT_KEY)?;
        let covers_mempool = parse_metadata(&metadata, TRANSACTION_ABSENCE_COVERS_MEMPOOL_KEY)?;
        Ok(Some(Self {
            chain_epoch_id: ChainEpochId::new(chain_epoch_id),
            visible_tip_height: BlockHeight::new(visible_tip_height),
            covers_mempool,
        }))
    }
}

/// Builds the `CANONICAL_HISTORY_UNAVAILABLE` status for a store whose
/// retained history starts at `first_available_height`.
#[must_use]
pub fn canonical_history_unavailable_status(
    first_available_height: BlockHeight,
    message: impl Into<String>,
    details: ErrorDetails,
) -> Status {
    status_with_reason_and_metadata(
        ErrorReason::CanonicalHistoryUnavailable,
        message,
        details,
        [(
            CANONICAL_HISTORY_FIRST_AVAILABLE_HEIGHT_KEY.to_owned(),
            first_available_height.value().to_string(),
        )],
    )
}

/// Decodes the first retained height from a `CANONICAL_HISTORY_UNAVAILABLE`
/// status.
///
/// Returns `Ok(None)` when `status` carries a different reason.
pub fn canonical_history_first_available_height(
    status: &Status,
) -> Result<Option<BlockHeight>, TransactionLookupMetadataError> {
    let Some(metadata) = zinder_metadata_for(status, ErrorReason::CanonicalHistoryUnavailable)
    else {
        return Ok(None);
    };
    parse_metadata(&metadata, CANONICAL_HISTORY_FIRST_AVAILABLE_HEIGHT_KEY)
        .map(|height| Some(BlockHeight::new(height)))
}

/// A transaction-lookup status claimed a reason but carried unusable metadata.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum TransactionLookupMetadataError {
    /// A required metadata key is absent.
    #[error("ErrorInfo metadata {key} is missing")]
    MissingKey {
        /// Metadata key.
        key: &'static str,
    },
    /// A metadata value does not parse as the documented type.
    #[error("ErrorInfo metadata {key} is malformed")]
    MalformedValue {
        /// Metadata key.
        key: &'static str,
    },
    /// The status code disagrees with the reason's canonical code.
    #[error("TRANSACTION_NOT_FOUND arrived with status code {code:?}")]
    UnexpectedCode {
        /// Received gRPC status code.
        code: Code,
    },
}

fn zinder_metadata_for(status: &Status, reason: ErrorReason) -> Option<HashMap<String, String>> {
    status
        .get_error_details()
        .error_info()
        .filter(|error_info| {
            error_info.domain == ZINDER_ERROR_DOMAIN && error_info.reason == reason.as_str_name()
        })
        .map(|error_info| error_info.metadata.clone())
}

fn parse_metadata<T: std::str::FromStr>(
    metadata: &HashMap<String, String>,
    key: &'static str,
) -> Result<T, TransactionLookupMetadataError> {
    metadata
        .get(key)
        .ok_or(TransactionLookupMetadataError::MissingKey { key })?
        .parse()
        .map_err(|_| TransactionLookupMetadataError::MalformedValue { key })
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        ArtifactSchemaVersion, BlockHash, ChainTipMetadata, Network, UnixTimestampMillis,
    };

    use super::*;
    use crate::status_for_reason;

    fn chain_epoch() -> ChainEpoch {
        ChainEpoch {
            id: ChainEpochId::new(7),
            network: Network::ZcashRegtest,
            visible_tip_height: BlockHeight::new(120),
            visible_tip_hash: BlockHash::from_bytes([1; 32]),
            settled_tip_height: BlockHeight::new(20),
            settled_tip_hash: BlockHash::from_bytes([2; 32]),
            artifact_schema_version: ArtifactSchemaVersion::new(1),
            tip_metadata: ChainTipMetadata::empty(),
            created_at: UnixTimestampMillis::new(1),
        }
    }

    #[test]
    fn absence_round_trips_through_status() -> Result<(), TransactionLookupMetadataError> {
        let absence = TransactionAbsence::at_epoch(chain_epoch(), true);
        let status = absence.into_status("transaction not found");

        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(TransactionAbsence::from_status(&status)?, Some(absence));
        Ok(())
    }

    #[test]
    fn other_not_found_reasons_are_not_absences() -> Result<(), TransactionLookupMetadataError> {
        for reason in [
            ErrorReason::ArtifactUnavailable,
            ErrorReason::ChainEpochMissing,
            ErrorReason::BlockNotInBestChain,
        ] {
            let status = status_for_reason(reason, "not an absence");
            assert_eq!(TransactionAbsence::from_status(&status)?, None);
        }
        assert_eq!(
            TransactionAbsence::from_status(&Status::not_found("bare"))?,
            None
        );
        Ok(())
    }

    #[test]
    fn absence_without_metadata_is_malformed() {
        let status = status_for_reason(ErrorReason::TransactionNotFound, "no metadata");

        assert_eq!(
            TransactionAbsence::from_status(&status),
            Err(TransactionLookupMetadataError::MissingKey {
                key: TRANSACTION_ABSENCE_CHAIN_EPOCH_ID_KEY
            })
        );
    }

    #[test]
    fn history_floor_round_trips_through_status() -> Result<(), TransactionLookupMetadataError> {
        let status = canonical_history_unavailable_status(
            BlockHeight::new(101),
            "history starts after a checkpoint",
            ErrorDetails::new(),
        );

        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(
            canonical_history_first_available_height(&status)?,
            Some(BlockHeight::new(101))
        );
        Ok(())
    }
}
