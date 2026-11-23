//! Canonical v7 physical inventory and retained block data.

use std::path::Path;

use rust_rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use zinder_core::BlockHeight;

use super::{
    CanonicalStoreError, CanonicalStoreWorkload,
    block_replay::BLOCK_REPLAY_COLUMN_FAMILY,
    rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY,
        DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
        MEMPOOL_EVENT_COLUMN_FAMILY, SUBTREE_ROOT_COLUMN_FAMILY, TRANSACTION_BLOB_COLUMN_FAMILY,
        TRANSACTION_LOCATION_COLUMN_FAMILY, TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    },
};
use crate::RawBlobRetention;

pub(super) const CANONICAL_DATA_COLUMN_FAMILIES: [&str; 15] = [
    BLOCK_HEADER_COLUMN_FAMILY,
    BLOCK_HASH_INDEX_COLUMN_FAMILY,
    BLOCK_REPLAY_COLUMN_FAMILY,
    DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY,
    TRANSACTION_LOCATION_COLUMN_FAMILY,
    COMPACT_BLOCK_COLUMN_FAMILY,
    TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
    SUBTREE_ROOT_COLUMN_FAMILY,
    CHAIN_EPOCH_COLUMN_FAMILY,
    CHAIN_EVENT_COLUMN_FAMILY,
    MEMPOOL_EVENT_COLUMN_FAMILY,
    DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
    BLOCK_BLOB_COLUMN_FAMILY,
    TRANSACTION_BLOB_COLUMN_FAMILY,
];

pub(super) const CANONICAL_STAGED_SST_FAMILIES: [&str; 9] = [
    BLOCK_BLOB_COLUMN_FAMILY,
    BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
    BLOCK_HASH_INDEX_COLUMN_FAMILY,
    BLOCK_HEADER_COLUMN_FAMILY,
    BLOCK_REPLAY_COLUMN_FAMILY,
    COMPACT_BLOCK_COLUMN_FAMILY,
    TRANSACTION_BLOB_COLUMN_FAMILY,
    TRANSACTION_LOCATION_COLUMN_FAMILY,
    TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
];

pub(super) const CANONICAL_PRE_PUBLICATION_EMPTY_FAMILIES: [&str; 5] = [
    DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY,
    CHAIN_EPOCH_COLUMN_FAMILY,
    CHAIN_EVENT_COLUMN_FAMILY,
    MEMPOOL_EVENT_COLUMN_FAMILY,
    DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
];

/// Canonical block data a reader can require at admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CanonicalStoredData {
    /// Headers, hash and transaction locations, compact blocks, and semantic replay.
    BlockHistory,
    /// Serialized transactions at their canonical locations.
    TransactionBodies,
    /// Serialized blocks.
    BlockBodies,
    /// Per-block final note-commitment roots where shielded pools are active.
    FinalNoteCommitmentRoots,
}

/// Minimum retained block data and historical coverage required by a reader.
///
/// The default accepts block history at any retained starting height.
#[derive(Clone, Debug, Default)]
pub struct CanonicalStoreRequirements {
    additional_data: Vec<CanonicalStoredData>,
    first_required_height: Option<BlockHeight>,
}

impl CanonicalStoreRequirements {
    /// Requires each data kind in addition to canonical block history.
    #[must_use]
    pub fn new(additional_data: &[CanonicalStoredData]) -> Self {
        Self {
            additional_data: additional_data.to_vec(),
            first_required_height: None,
        }
    }

    /// Requires the admitted history to start at or before this block height.
    #[must_use]
    pub const fn from_height(mut self, height: BlockHeight) -> Self {
        self.first_required_height = Some(height);
        self
    }

    pub(super) fn from_legacy(
        workload: CanonicalStoreWorkload,
        retention: RawBlobRetention,
    ) -> Self {
        let additional_data = [
            CanonicalStoredData::TransactionBodies,
            CanonicalStoredData::BlockBodies,
            CanonicalStoredData::FinalNoteCommitmentRoots,
        ]
        .into_iter()
        .filter(|&kind| retains(workload, retention, kind))
        .collect();
        Self {
            additional_data,
            first_required_height: None,
        }
    }
}

/// Retention policy and height floor derived from a canonical build plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalStoredSet {
    workload: CanonicalStoreWorkload,
    retention: RawBlobRetention,
    first_height: BlockHeight,
}

impl CanonicalStoredSet {
    pub(super) const fn new(
        workload: CanonicalStoreWorkload,
        retention: RawBlobRetention,
        first_height: BlockHeight,
    ) -> Self {
        Self {
            workload,
            retention,
            first_height,
        }
    }

    /// Returns the historical floor for a retained data kind.
    ///
    /// Sparse artifacts such as final roots follow their own activation rules
    /// within retained history. This describes coverage policy.
    #[must_use]
    pub const fn first_height(self, kind: CanonicalStoredData) -> Option<BlockHeight> {
        if retains(self.workload, self.retention, kind) {
            Some(self.first_height)
        } else {
            None
        }
    }

    /// Reports whether the store retains this data kind.
    #[must_use]
    pub const fn contains(self, kind: CanonicalStoredData) -> bool {
        self.first_height(kind).is_some()
    }

    pub(super) fn require(
        self,
        path: &Path,
        requirements: &CanonicalStoreRequirements,
    ) -> Result<(), CanonicalStoreError> {
        for kind in std::iter::once(CanonicalStoredData::BlockHistory)
            .chain(requirements.additional_data.iter().copied())
        {
            let available_from = self.first_height(kind);
            if available_from.is_none()
                || requirements
                    .first_required_height
                    .zip(available_from)
                    .is_some_and(|(required, available)| required < available)
            {
                return Err(CanonicalStoreError::MissingStoredData {
                    path: path.to_path_buf(),
                    kind,
                    required_from: requirements.first_required_height,
                    available_from,
                });
            }
        }
        Ok(())
    }

    pub(super) const fn raw_blob_retention(self) -> RawBlobRetention {
        self.retention
    }

    pub(super) const fn requires_daily_value_pool_evidence(self) -> bool {
        matches!(self.workload, CanonicalStoreWorkload::Explorer)
    }
}

const fn retains(
    workload: CanonicalStoreWorkload,
    retention: RawBlobRetention,
    kind: CanonicalStoredData,
) -> bool {
    match kind {
        CanonicalStoredData::BlockHistory => true,
        CanonicalStoredData::TransactionBodies => retention.retains_transaction_blobs(),
        CanonicalStoredData::BlockBodies => retention.retains_block_blobs(),
        CanonicalStoredData::FinalNoteCommitmentRoots => workload.derives_final_roots(),
    }
}

impl CanonicalStoreWorkload {
    /// Reports whether construction derives per-block final note-commitment roots.
    #[must_use]
    pub const fn derives_final_roots(self) -> bool {
        matches!(self, Self::Explorer)
    }
}

impl RawBlobRetention {
    /// Whether full block blobs are retained.
    #[must_use]
    pub const fn retains_block_blobs(self) -> bool {
        matches!(self, Self::All)
    }

    /// Whether transaction blobs are retained.
    #[must_use]
    pub const fn retains_transaction_blobs(self) -> bool {
        matches!(self, Self::Transactions | Self::All)
    }
}

pub(super) fn validate_exact_column_families(
    path: &Path,
    observed: &[String],
) -> Result<(), CanonicalStoreError> {
    let mut observed = observed.iter().map(String::as_str).collect::<Vec<_>>();
    observed.sort_unstable();
    let mut expected = Vec::from(CANONICAL_DATA_COLUMN_FAMILIES);
    expected.push(DEFAULT_COLUMN_FAMILY_NAME);
    expected.sort_unstable();
    if observed != expected {
        return Err(CanonicalStoreError::admission(
            path,
            format!(
                "column families {observed:?} do not exactly match required canonical set {expected:?}"
            ),
        ));
    }
    Ok(())
}
