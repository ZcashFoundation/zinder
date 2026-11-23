//! Canonical v7 physical inventory and retained block data.

use std::{num::NonZeroU32, path::Path};

use rust_rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use zinder_core::{
    BlockHeight, CanonicalHistoryBounds, ChainTipMetadata, ShieldedProtocol, SubtreeRootIndex,
    SubtreeRootRange,
};

use super::{
    CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreWorkload,
    CanonicalSubtreeRootLoadCoverage,
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
    complete_subtree_root_prefix: bool,
}

impl CanonicalStoreRequirements {
    /// Requires each data kind in addition to canonical block history.
    #[must_use]
    pub fn new(additional_data: &[CanonicalStoredData]) -> Self {
        Self {
            additional_data: additional_data.to_vec(),
            first_required_height: None,
            complete_subtree_root_prefix: false,
        }
    }

    /// Requires the admitted history to start at or before this block height.
    #[must_use]
    pub const fn from_height(mut self, height: BlockHeight) -> Self {
        self.first_required_height = Some(height);
        self
    }

    /// Requires every completed subtree root from index zero in each pool.
    #[must_use]
    pub const fn with_complete_subtree_root_prefix(mut self) -> Self {
        self.complete_subtree_root_prefix = true;
        self
    }

    /// Requires canonical history and the specified raw-blob coverage.
    #[must_use]
    pub fn for_blob_retention(retention: RawBlobRetention) -> Self {
        Self::default().with_blob_retention(retention)
    }

    /// Adds the specified raw-blob coverage to the reader's existing requirements.
    #[must_use]
    pub fn with_blob_retention(mut self, retention: RawBlobRetention) -> Self {
        for kind in [
            CanonicalStoredData::TransactionBodies,
            CanonicalStoredData::BlockBodies,
        ] {
            if retains(CanonicalStoreWorkload::Wallet, retention, kind)
                && !self.additional_data.contains(&kind)
            {
                self.additional_data.push(kind);
            }
        }
        self
    }

    pub(super) fn from_legacy(
        workload: CanonicalStoreWorkload,
        retention: RawBlobRetention,
    ) -> Self {
        let mut requirements = Self::for_blob_retention(retention);
        if retains(
            workload,
            retention,
            CanonicalStoredData::FinalNoteCommitmentRoots,
        ) {
            requirements
                .additional_data
                .push(CanonicalStoredData::FinalNoteCommitmentRoots);
        }
        requirements
    }
}

impl CanonicalStoreBuildPlan {
    /// Returns the stored data policy for this construction plan and workload.
    #[must_use]
    pub const fn stored_set(&self, workload: CanonicalStoreWorkload) -> CanonicalStoredSet {
        CanonicalStoredSet::for_history(workload, self.raw_blob_retention(), self.history_bounds())
    }

    pub(super) const fn first_retained_height(&self) -> BlockHeight {
        CanonicalStoredSet::history_floor(self.history_bounds())
    }

    pub(super) fn subtree_root_ranges(
        &self,
        coverage: CanonicalSubtreeRootLoadCoverage,
        tip: ChainTipMetadata,
    ) -> Result<Vec<SubtreeRootRange>, CanonicalStoreError> {
        subtree_root_ranges(coverage, self.history_predecessor().tip_metadata(), tip)
    }
}

/// Retention, historical floor, and any admitted subtree-root evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalStoredSet {
    workload: CanonicalStoreWorkload,
    retention: RawBlobRetention,
    first_height: BlockHeight,
    subtree_root_coverage: Option<CanonicalSubtreeRootLoadCoverage>,
}

impl CanonicalStoredSet {
    /// Describes the retained data a writer builds for its workload and history.
    ///
    /// Coverage authenticated by a published manifest is available after admission.
    #[must_use]
    pub const fn for_history(
        workload: CanonicalStoreWorkload,
        retention: RawBlobRetention,
        bounds: CanonicalHistoryBounds,
    ) -> Self {
        Self::new(workload, retention, Self::history_floor(bounds))
    }

    pub(super) const fn new(
        workload: CanonicalStoreWorkload,
        retention: RawBlobRetention,
        first_height: BlockHeight,
    ) -> Self {
        Self {
            workload,
            retention,
            first_height,
            subtree_root_coverage: None,
        }
    }

    pub(super) const fn history_floor(bounds: CanonicalHistoryBounds) -> BlockHeight {
        bounds.first_available_height()
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

    /// Returns the authenticated subtree-root coverage of an admitted store.
    ///
    /// A set derived from a build plan alone has no manifest evidence.
    #[must_use]
    pub const fn subtree_root_coverage(self) -> Option<CanonicalSubtreeRootLoadCoverage> {
        self.subtree_root_coverage
    }

    pub(super) const fn with_subtree_root_coverage(
        mut self,
        coverage: CanonicalSubtreeRootLoadCoverage,
    ) -> Self {
        self.subtree_root_coverage = Some(coverage);
        self
    }

    pub(super) fn require(
        self,
        path: &Path,
        requirements: &CanonicalStoreRequirements,
        predecessor_tip_metadata: ChainTipMetadata,
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
        if requirements.complete_subtree_root_prefix
            && [
                ShieldedProtocol::Sapling,
                ShieldedProtocol::Orchard,
                ShieldedProtocol::Ironwood,
            ]
            .into_iter()
            .any(|pool| {
                self.subtree_root_coverage.map(|coverage| {
                    first_subtree_root_index(coverage, predecessor_tip_metadata, pool)
                }) != Some(0)
            })
        {
            return Err(CanonicalStoreError::IncompleteSubtreeRootPrefix {
                path: path.to_path_buf(),
            });
        }
        Ok(())
    }

    /// Returns the raw-blob policy fixed by the canonical writer.
    #[must_use]
    pub const fn raw_blob_retention(self) -> RawBlobRetention {
        self.retention
    }

    pub(super) const fn requires_daily_value_pool_evidence(self) -> bool {
        matches!(self.workload, CanonicalStoreWorkload::Explorer)
    }
}

impl CanonicalSubtreeRootLoadCoverage {
    pub(super) const fn includes_preceding_roots(self) -> bool {
        matches!(self, Self::CompletePrefix)
    }
}

pub(super) fn required_subtree_root_ranges(
    previous_tip: ChainTipMetadata,
    next_tip: ChainTipMetadata,
) -> Result<Vec<SubtreeRootRange>, CanonicalStoreError> {
    subtree_root_ranges(
        CanonicalSubtreeRootLoadCoverage::RetainedRange,
        previous_tip,
        next_tip,
    )
}

fn subtree_root_ranges(
    coverage: CanonicalSubtreeRootLoadCoverage,
    predecessor_tip_metadata: ChainTipMetadata,
    build_tip_metadata: ChainTipMetadata,
) -> Result<Vec<SubtreeRootRange>, CanonicalStoreError> {
    let mut ranges = Vec::with_capacity(3);
    for protocol in [
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
        ShieldedProtocol::Ironwood,
    ] {
        let start_index = first_subtree_root_index(coverage, predecessor_tip_metadata, protocol);
        let completed_at_tip = build_tip_metadata.completed_subtree_count(protocol);
        let root_count = completed_at_tip.checked_sub(start_index).ok_or_else(|| {
            CanonicalStoreError::subtree_root_sequence(format!(
                "{protocol:?} completed-subtree count regressed from {start_index} to {completed_at_tip}"
            ))
        })?;
        if let Some(max_entries) = NonZeroU32::new(root_count) {
            ranges.push(SubtreeRootRange::new(
                protocol,
                SubtreeRootIndex::new(start_index),
                max_entries,
            ));
        }
    }
    Ok(ranges)
}

pub(super) fn first_subtree_root_index(
    coverage: CanonicalSubtreeRootLoadCoverage,
    predecessor_tip_metadata: ChainTipMetadata,
    protocol: ShieldedProtocol,
) -> u32 {
    match coverage {
        CanonicalSubtreeRootLoadCoverage::CompletePrefix => 0,
        CanonicalSubtreeRootLoadCoverage::RetainedRange => {
            predecessor_tip_metadata.completed_subtree_count(protocol)
        }
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
