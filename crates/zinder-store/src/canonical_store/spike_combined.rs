//! Spike-only hooks for writing wallet rows in the canonical commit batch.
//!
//! Throwaway code for the combined-commit benchmark. It widens the canonical
//! writer so a caller can (1) open the canonical database with extra column
//! families, (2) prepare a live append or replacement without writing it,
//! (3) add its own rows to the same `WriteBatch`, and (4) commit that batch
//! through the unchanged synced write and readback path.

use std::sync::Arc;

use parking_lot::RwLock;
use rust_rocksdb::{Cache, ColumnFamilyDescriptor, WriteBatch};
use zinder_core::{
    BlockHeightRange, BlockId, ChainEpochId, NetworkUpgradeActivations,
    ValidatedCanonicalBlockReplay,
};

use super::{
    CanonicalEventFence, CanonicalLiveAppend, CanonicalLiveReplacement, CanonicalRetainedEvent,
    CanonicalStoreError, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    block_replay::{BLOCK_REPLAY_COLUMN_FAMILY, decode_persisted_replay},
    event_lifecycle::decode_retained_event,
    live_commit::{PreparedLiveAppendCommit, PreparedLiveBlockRows},
    live_replacement::PreparedLiveReplacement,
    rocksdb::DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
};
use crate::{BoundedRocksDbOpen, RocksDbResourceBudget};

/// Spike: wall time of the synced write and of the canonical readback.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpikeCommitTiming {
    /// `write_opt` with `sync = true`.
    pub write: std::time::Duration,
    /// Canonical readback and replay-tail advance after the write.
    pub readback: std::time::Duration,
}

type ExtraFamilyBuilder =
    Arc<dyn Fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor> + Send + Sync>;

struct SpikeExtraColumnFamilies {
    names: Vec<String>,
    build: ExtraFamilyBuilder,
}

static SPIKE_EXTRA_COLUMN_FAMILIES: RwLock<Option<Arc<SpikeExtraColumnFamilies>>> =
    RwLock::new(None);

/// Spike: registers column families that canonical opens add and admission accepts.
///
/// `build` must return one descriptor for every name in `names`. The
/// registration is process-wide and replaces any earlier one.
pub fn spike_set_extra_column_families(
    names: Vec<String>,
    build: impl Fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor>
    + Send
    + Sync
    + 'static,
) {
    *SPIKE_EXTRA_COLUMN_FAMILIES.write() = Some(Arc::new(SpikeExtraColumnFamilies {
        names,
        build: Arc::new(build),
    }));
}

/// Spike: removes the extra column-family registration.
pub fn spike_clear_extra_column_families() {
    *SPIKE_EXTRA_COLUMN_FAMILIES.write() = None;
}

pub(super) fn spike_extra_column_family_names() -> Vec<String> {
    SPIKE_EXTRA_COLUMN_FAMILIES
        .read()
        .as_ref()
        .map(|extra| extra.names.clone())
        .unwrap_or_default()
}

pub(super) fn spike_extra_column_family_descriptors(
    block_cache: &Cache,
    resource_budget: RocksDbResourceBudget,
) -> Vec<ColumnFamilyDescriptor> {
    let extra = SPIKE_EXTRA_COLUMN_FAMILIES.read().clone();
    extra.map_or_else(Vec::new, |extra| {
        (extra.build)(block_cache, resource_budget)
    })
}

/// Spike: one fully validated live append whose rows are not yet written.
pub struct SpikePreparedLiveAppend {
    pub(super) inner: PreparedLiveAppendCommit,
}

impl SpikePreparedLiveAppend {
    /// Canonical fence published by this append.
    #[must_use]
    pub const fn resulting_fence(&self) -> CanonicalEventFence {
        self.inner.outcome
    }

    /// Epoch replaced by this append.
    #[must_use]
    pub const fn previous_epoch_id(&self) -> ChainEpochId {
        self.inner.previous_epoch_id
    }

    /// Settled tip paired with the resulting epoch.
    #[must_use]
    pub const fn settled_tip(&self) -> BlockId {
        self.inner.settled_tip
    }

    /// Decodes the retained event row this append writes.
    pub fn retained_event(&self) -> Result<CanonicalRetainedEvent, CanonicalStoreError> {
        decode_retained_event(
            self.inner.outcome.chain_event_sequence(),
            &self.inner.encoded_event,
        )
    }

    /// Decodes the replay rows this append writes, in height order.
    pub fn committed_replays(
        &self,
    ) -> Result<Vec<ValidatedCanonicalBlockReplay>, CanonicalStoreError> {
        decode_block_rows_replays(std::slice::from_ref(&self.inner.block_rows))
    }

    /// Visits every canonical block-family row (family, key, value).
    pub fn for_each_block_row(&self, mut visit: impl FnMut(&'static str, &[u8], &[u8])) {
        for row in &self.inner.block_rows.rows {
            visit(row.family, &row.key, &row.encoded_value);
        }
    }
}

/// Spike: one fully validated replacement whose rows are not yet written.
pub struct SpikePreparedLiveReplacement {
    pub(super) inner: PreparedLiveReplacement,
}

impl SpikePreparedLiveReplacement {
    /// Canonical fence published by this replacement.
    #[must_use]
    pub const fn resulting_fence(&self) -> CanonicalEventFence {
        self.inner.outcome
    }

    /// Settled tip kept by this replacement.
    #[must_use]
    pub const fn settled_tip(&self) -> BlockId {
        self.inner.settled_tip
    }

    /// Displaced canonical heights.
    #[must_use]
    pub const fn reverted_range(&self) -> BlockHeightRange {
        self.inner.reverted_range
    }

    /// Replacement canonical heights.
    #[must_use]
    pub const fn committed_range(&self) -> BlockHeightRange {
        self.inner.committed_range
    }

    /// Decodes the retained reorg event row this replacement writes.
    pub fn retained_event(&self) -> Result<CanonicalRetainedEvent, CanonicalStoreError> {
        decode_retained_event(
            self.inner.outcome.chain_event_sequence(),
            &self.inner.encoded_event,
        )
    }

    /// Decodes the replacement replay rows, in height order.
    pub fn committed_replays(
        &self,
    ) -> Result<Vec<ValidatedCanonicalBlockReplay>, CanonicalStoreError> {
        decode_block_rows_replays(&self.inner.replacement_rows)
    }

    /// Visits every per-block canonical operation of this replacement: the
    /// displaced-row deletions (`None`), the replacement block rows, and the
    /// permanent archive rows. The single epoch, event, and control records
    /// are excluded because they do not scale with block density.
    pub fn for_each_block_scaled_op(
        &self,
        mut visit: impl FnMut(&'static str, &[u8], Option<&[u8]>),
    ) {
        for (family, key) in &self.inner.deletions.rows {
            visit(family, key, None);
        }
        for block_rows in &self.inner.replacement_rows {
            for row in &block_rows.rows {
                visit(row.family, &row.key, Some(&row.encoded_value));
            }
        }
        for (key, encoded) in &self.inner.archive.rows {
            visit(DISPLACED_BLOCK_FACTS_COLUMN_FAMILY, key, Some(encoded));
        }
    }
}

fn decode_block_rows_replays(
    block_rows: &[PreparedLiveBlockRows],
) -> Result<Vec<ValidatedCanonicalBlockReplay>, CanonicalStoreError> {
    let mut replays = Vec::with_capacity(block_rows.len());
    for rows in block_rows {
        let row = rows
            .rows
            .iter()
            .find(|row| row.family == BLOCK_REPLAY_COLUMN_FAMILY)
            .ok_or_else(|| CanonicalStoreError::live_commit("prepared block has no replay row"))?;
        let (_, replay) = decode_persisted_replay(&row.key, &row.encoded_value)?;
        replays.push(replay);
    }
    Ok(replays)
}

impl RocksDbCanonicalStore {
    /// Spike: the shared database handle.
    #[must_use]
    pub const fn spike_bounded_open(&self) -> &BoundedRocksDbOpen {
        &self.bounded_open
    }

    /// Spike: validates and prepares one live append without writing it.
    pub fn spike_prepare_live_append(
        &self,
        append: CanonicalLiveAppend,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<SpikePreparedLiveAppend, CanonicalStoreError> {
        Ok(SpikePreparedLiveAppend {
            inner: PreparedLiveAppendCommit::new(self, append, network_upgrade_activations)?,
        })
    }

    /// Spike: adds every canonical row of a prepared append to `batch`.
    pub fn spike_put_prepared_live_append(
        &self,
        prepared: &SpikePreparedLiveAppend,
        batch: &mut WriteBatch,
    ) -> Result<(), CanonicalStoreError> {
        prepared.inner.put_into(&self.bounded_open.db, batch)
    }

    /// Spike: writes `batch` (which must hold the prepared rows) synced, then
    /// runs the unchanged canonical readback.
    pub fn spike_commit_prepared_live_append(
        self,
        prepared: SpikePreparedLiveAppend,
        batch: WriteBatch,
    ) -> Result<(Self, CanonicalEventFence, SpikeCommitTiming), CanonicalStoreError> {
        self.write_prepared_live_append(prepared.inner, &batch)
    }

    /// Spike: validates and prepares one live replacement without writing it.
    pub fn spike_prepare_live_replacement(
        &self,
        replacement: CanonicalLiveReplacement,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<SpikePreparedLiveReplacement, CanonicalStoreError> {
        Ok(SpikePreparedLiveReplacement {
            inner: PreparedLiveReplacement::new(self, replacement, network_upgrade_activations)?,
        })
    }

    /// Spike: adds every canonical deletion, row, and archive entry of a prepared replacement.
    pub fn spike_put_prepared_live_replacement(
        &self,
        prepared: &SpikePreparedLiveReplacement,
        batch: &mut WriteBatch,
    ) -> Result<(), CanonicalStoreError> {
        prepared.inner.put_into(self, batch)
    }

    /// Spike: writes `batch` (which must hold the prepared replacement) synced,
    /// then runs the unchanged canonical readback.
    pub fn spike_commit_prepared_live_replacement(
        self,
        prepared: SpikePreparedLiveReplacement,
        batch: WriteBatch,
    ) -> Result<(Self, CanonicalEventFence, SpikeCommitTiming), CanonicalStoreError> {
        self.write_prepared_live_replacement(prepared.inner, &batch)
    }
}

impl RocksDbCanonicalSecondary {
    /// Spike: the secondary's database handle.
    #[must_use]
    pub const fn spike_bounded_open(&self) -> &BoundedRocksDbOpen {
        &self.bounded_open
    }
}
