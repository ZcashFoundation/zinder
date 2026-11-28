//! Canonical v2 physical inventory and retained block data.

use std::{num::NonZeroU32, path::Path};

use rust_rocksdb::DEFAULT_COLUMN_FAMILY_NAME;
use zinder_core::{
    BlockHeight, CanonicalHistoryBounds, ChainTipMetadata, ShieldedProtocol, SubtreeRootIndex,
    SubtreeRootRange,
};

use super::{
    CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalSubtreeRootLoadCoverage,
    rocksdb::{
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
        LIVE_TAIL_BLOCK_COLUMN_FAMILY, LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
        MEMPOOL_EVENT_COLUMN_FAMILY, SETTLED_SEGMENT_INDEX_COLUMN_FAMILY,
        SUBTREE_ROOT_COLUMN_FAMILY, TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    },
};
use crate::RawBlobRetention;

pub(super) const CANONICAL_DATA_COLUMN_FAMILIES: [&str; 12] = [
    BLOCK_HEADER_COLUMN_FAMILY,
    BLOCK_HASH_INDEX_COLUMN_FAMILY,
    TRANSACTION_LOCATION_COLUMN_FAMILY,
    LIVE_TAIL_BLOCK_COLUMN_FAMILY,
    LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
    SETTLED_SEGMENT_INDEX_COLUMN_FAMILY,
    TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    SUBTREE_ROOT_COLUMN_FAMILY,
    CHAIN_EPOCH_COLUMN_FAMILY,
    CHAIN_EVENT_COLUMN_FAMILY,
    MEMPOOL_EVENT_COLUMN_FAMILY,
    DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
];

pub(super) const CANONICAL_STAGED_SST_FAMILIES: [&str; 7] = [
    BLOCK_HASH_INDEX_COLUMN_FAMILY,
    BLOCK_HEADER_COLUMN_FAMILY,
    LIVE_TAIL_BLOCK_COLUMN_FAMILY,
    LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
    SETTLED_SEGMENT_INDEX_COLUMN_FAMILY,
    TRANSACTION_LOCATION_COLUMN_FAMILY,
    TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
];

pub(super) const CANONICAL_PRE_PUBLICATION_EMPTY_FAMILIES: [&str; 4] = [
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
            if retains(retention, kind) && !self.additional_data.contains(&kind) {
                self.additional_data.push(kind);
            }
        }
        self
    }
}

impl CanonicalStoreBuildPlan {
    /// Returns the stored data policy for this construction plan.
    #[must_use]
    pub const fn stored_set(&self) -> CanonicalStoredSet {
        CanonicalStoredSet::for_history(self.raw_blob_retention(), self.history_bounds())
    }

    pub(super) const fn first_retained_height(&self) -> BlockHeight {
        CanonicalStoredSet::history_floor(self.history_bounds())
    }

    pub(super) fn admitted_stored_set(
        &self,
        coverage: CanonicalSubtreeRootLoadCoverage,
    ) -> CanonicalStoredSet {
        let mut stored_set = self.stored_set();
        let predecessor = self.subtree_predecessor_metadata(coverage);
        stored_set.subtree_root_coverage = Some(coverage);
        stored_set.first_subtree_root_indices =
            SHIELDED_PROTOCOLS.map(|protocol| predecessor.completed_subtree_count(protocol));
        stored_set
    }

    pub(super) fn subtree_root_ranges(
        &self,
        coverage: CanonicalSubtreeRootLoadCoverage,
        tip: ChainTipMetadata,
    ) -> Result<Vec<SubtreeRootRange>, CanonicalStoreError> {
        required_subtree_root_ranges(self.subtree_predecessor_metadata(coverage), tip)
    }

    fn subtree_predecessor_metadata(
        &self,
        coverage: CanonicalSubtreeRootLoadCoverage,
    ) -> ChainTipMetadata {
        match coverage {
            CanonicalSubtreeRootLoadCoverage::RetainedRange => {
                self.history_predecessor().tip_metadata()
            }
            CanonicalSubtreeRootLoadCoverage::CompletePrefix => ChainTipMetadata::empty(),
        }
    }
}

const SHIELDED_PROTOCOLS: [ShieldedProtocol; 3] = [
    ShieldedProtocol::Sapling,
    ShieldedProtocol::Orchard,
    ShieldedProtocol::Ironwood,
];

/// Retention, historical floor, and any admitted subtree-root evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalStoredSet {
    retention: RawBlobRetention,
    first_height: BlockHeight,
    subtree_root_coverage: Option<CanonicalSubtreeRootLoadCoverage>,
    first_subtree_root_indices: [u32; 3],
}

impl CanonicalStoredSet {
    /// Describes the retained data a writer builds for its retention and history.
    ///
    /// Coverage authenticated by a published manifest is available after admission.
    #[must_use]
    pub const fn for_history(retention: RawBlobRetention, bounds: CanonicalHistoryBounds) -> Self {
        Self::new(retention, Self::history_floor(bounds))
    }

    pub(super) const fn new(retention: RawBlobRetention, first_height: BlockHeight) -> Self {
        Self {
            retention,
            first_height,
            subtree_root_coverage: None,
            first_subtree_root_indices: [0; 3],
        }
    }

    pub(super) const fn history_floor(bounds: CanonicalHistoryBounds) -> BlockHeight {
        bounds.first_available_height()
    }

    /// Returns the historical floor for a retained data kind.
    #[must_use]
    pub const fn first_height(self, kind: CanonicalStoredData) -> Option<BlockHeight> {
        if self.contains(kind) {
            Some(self.first_height)
        } else {
            None
        }
    }

    /// Reports whether the store retains this data kind.
    #[must_use]
    pub const fn contains(&self, kind: CanonicalStoredData) -> bool {
        retains(self.retention, kind)
    }

    /// Returns the authenticated subtree-root coverage of an admitted store.
    ///
    /// A set derived from a build plan alone has no manifest evidence.
    #[must_use]
    pub const fn subtree_root_coverage(self) -> Option<CanonicalSubtreeRootLoadCoverage> {
        self.subtree_root_coverage
    }

    pub(super) fn first_subtree_root_index(&self, protocol: ShieldedProtocol) -> u32 {
        self.first_subtree_root_indices[usize::from(protocol.id()) - 1]
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
        if requirements.complete_subtree_root_prefix
            && (self.subtree_root_coverage.is_none() || self.first_subtree_root_indices != [0; 3])
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

    pub(super) fn permits_construction_rows(self, family: &str) -> bool {
        match family {
            LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY => self.contains(CanonicalStoredData::BlockBodies),
            _ => !CANONICAL_PRE_PUBLICATION_EMPTY_FAMILIES.contains(&family),
        }
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
    let mut ranges = Vec::with_capacity(3);
    for protocol in SHIELDED_PROTOCOLS {
        let start_index = previous_tip.completed_subtree_count(protocol);
        let completed_at_tip = next_tip.completed_subtree_count(protocol);
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

const fn retains(retention: RawBlobRetention, kind: CanonicalStoredData) -> bool {
    match kind {
        CanonicalStoredData::BlockHistory => true,
        CanonicalStoredData::TransactionBodies => retention.retains_transaction_blobs(),
        CanonicalStoredData::BlockBodies => retention.retains_block_blobs(),
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
        matches!(self, Self::All)
    }
}

pub(super) fn validate_exact_column_families(
    path: &Path,
    observed: &mut [String],
) -> Result<(), CanonicalStoreError> {
    observed.sort_unstable();
    let mut expected = [DEFAULT_COLUMN_FAMILY_NAME; CANONICAL_DATA_COLUMN_FAMILIES.len() + 1];
    expected[..CANONICAL_DATA_COLUMN_FAMILIES.len()]
        .copy_from_slice(&CANONICAL_DATA_COLUMN_FAMILIES);
    expected.sort_unstable();
    if !observed.iter().map(String::as_str).eq(expected) {
        return Err(CanonicalStoreError::admission(
            path,
            format!(
                "column families {observed:?} do not exactly match required canonical set {expected:?}"
            ),
        ));
    }
    Ok(())
}
