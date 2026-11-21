//! Densifying the tree-state checkpoints of settled history.
//!
//! Sparse construction stores a tree-state checkpoint at its history
//! predecessor, every [`TREE_STATE_CHECKPOINT_STRIDE`] blocks, and at its
//! build tip, so a read between two of them replays stored compact blocks;
//! dense construction stores one for every loaded block directly.
//! Densification fills any incomplete stride, from one stored checkpoint to
//! the next cadence checkpoint, with the checkpoints a
//! [`TreeStateCheckpointSelection`] picks. It replays the stride with the
//! per-block step a tree-state reconstruction runs, and accepts the result
//! only when the replay reproduces every checkpoint already stored in the
//! stride, including the one that ends it, byte for byte. Each stride commits
//! in one batch, so a stride is either complete or untouched, and only settled
//! strides are densified, so no reorg can remove a densified row.

use std::{collections::BTreeMap, num::NonZeroU32};

use rust_rocksdb::{Direction, IteratorMode, WriteBatch, WriteOptions};
use zinder_core::{
    BlockHeight, BlockId, ChainTipMetadata, NetworkUpgradeActivations,
    wire::decode_height_key_ascending,
};

use super::{
    CanonicalStoreError, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    TREE_STATE_CHECKPOINT_STRIDE,
    block_load::{encode_block_position, encode_tree_state_checkpoint},
    publication::column_family,
    reader::{
        CanonicalServingRead, read_block_header_at, read_compact_block_at,
        read_stored_tree_state_checkpoint_at,
    },
    rocksdb::TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    tree_state_replay::{recorded_tree_sizes, replay_compact_blocks},
};

/// Most reads one stride adds to a scan.
///
/// A stride costs a read for each checkpoint row it holds and two more for each
/// gap between rows that its selection checks. A gap needs at least one block
/// of its own and a row fills one block, so a stride that alternates a row and
/// a gap, one block each, reads the most: half a stride of rows and half a
/// stride of gaps.
pub const MAX_TREE_STATE_STRIDE_SCAN_READS: u32 =
    TREE_STATE_CHECKPOINT_STRIDE + TREE_STATE_CHECKPOINT_STRIDE / 2;

/// Which blocks of a stride receive a stored tree-state checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TreeStateCheckpointSelection {
    /// Every block, so a tree-state read at any settled height is a stored
    /// checkpoint read.
    EveryBlock,
    /// The first block at which at least this many note commitments, summed
    /// over every pool, have been appended since the previous checkpoint.
    CommitmentBudget(NonZeroU32),
}

impl TreeStateCheckpointSelection {
    /// Returns whether a gap that starts after `committed_since_checkpoint`
    /// commitments still needs a checkpoint before its next stored one.
    fn requires_checkpoint(self, committed_since_checkpoint: u64) -> bool {
        match self {
            Self::EveryBlock => true,
            Self::CommitmentBudget(budget) => committed_since_checkpoint >= u64::from(budget.get()),
        }
    }
}

/// One settled run of blocks between two stored tree-state checkpoints.
///
/// The seed is the history predecessor or a cadence height, and the end is
/// the next cadence height above it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreeStateStride {
    pub(super) seed_height: BlockHeight,
    pub(super) end_height: BlockHeight,
}

impl TreeStateStride {
    /// Height of the stored checkpoint the stride replays from.
    #[must_use]
    pub const fn seed_height(self) -> BlockHeight {
        self.seed_height
    }

    /// Height of the stored cadence checkpoint the replay must reproduce.
    #[must_use]
    pub const fn end_height(self) -> BlockHeight {
        self.end_height
    }

    fn after_seed(seed_height: BlockHeight) -> Result<Self, CanonicalStoreError> {
        let end_height = seed_height
            .value()
            .checked_add(
                TREE_STATE_CHECKPOINT_STRIDE - seed_height.value() % TREE_STATE_CHECKPOINT_STRIDE,
            )
            .map(BlockHeight::new)
            .ok_or_else(|| {
                CanonicalStoreError::publication("tree-state stride height exceeds u32::MAX")
            })?;
        Ok(Self {
            seed_height,
            end_height,
        })
    }
}

/// Checkpoint rows a replay of one stride produced, ready for the writer.
///
/// Only [`RocksDbCanonicalSecondary::densify_tree_state_stride`] and
/// [`RocksDbCanonicalStore::densify_tree_state_stride`] create this value, so
/// the writer commits only rows a checked replay produced.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DensifiedTreeStateStride {
    stride: TreeStateStride,
    /// Block the replay ended at, which binds every replayed block to it
    /// through the parent links the replay checked.
    end_block: BlockId,
    seed_row: Vec<u8>,
    end_row: Vec<u8>,
    rows: Vec<(BlockHeight, Vec<u8>)>,
    replayed_commitment_count: u64,
}

impl DensifiedTreeStateStride {
    /// The stride this replay covered.
    #[must_use]
    pub const fn stride(&self) -> TreeStateStride {
        self.stride
    }

    /// Number of checkpoint rows the stride is missing and the commit adds.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Note commitments, over every pool, the replay appended.
    #[must_use]
    pub const fn replayed_commitment_count(&self) -> u64 {
        self.replayed_commitment_count
    }

    /// Encoded bytes of the rows the commit adds.
    #[must_use]
    pub fn row_bytes(&self) -> usize {
        self.rows.iter().map(|(_, row)| row.len()).sum()
    }

    fn mismatch(&self, reason: impl Into<String>) -> CanonicalStoreError {
        stride_mismatch(self.stride, reason)
    }
}

/// What one bounded scan of settled strides found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeStateStrideScan {
    /// Strides whose stored checkpoints do not satisfy the selection.
    pub incomplete: Vec<TreeStateStride>,
    /// Strides whose stored rows or blocks are damaged, with the check that
    /// failed.
    pub damaged: Vec<(TreeStateStride, String)>,
    /// Number of settled strides the scan examined.
    pub examined: u32,
    /// Reads the scan made: each checkpoint row it visited, at least one per
    /// examined stride, and each compact block it read.
    pub reads: u32,
    /// Height the next scan continues from.
    pub next_from: BlockHeight,
    /// Whether the scan stopped at the first stride above the settled tip
    /// rather than at its read budget.
    pub reached_unsettled: bool,
}

/// What committing one densified stride changed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreeStateStrideCommit {
    /// Rows written by this commit.
    pub rows_written: u32,
    /// Rows another commit had already written with identical bytes.
    pub rows_already_present: u32,
}

impl RocksDbCanonicalSecondary {
    /// Examines settled strides from `from` against `selection` until the
    /// reads the scan has made reach `read_budget`, so a caller can pace a
    /// scan of a long history by the reads it makes.
    ///
    /// The scan always examines at least one stride and stops after the
    /// stride that reaches the budget, so it makes fewer than `read_budget`
    /// plus [`MAX_TREE_STATE_STRIDE_SCAN_READS`] reads.
    pub fn scan_tree_state_strides(
        &self,
        from: BlockHeight,
        selection: TreeStateCheckpointSelection,
        read_budget: NonZeroU32,
    ) -> Result<TreeStateStrideScan, CanonicalStoreError> {
        scan_tree_state_strides(self, from, selection, read_budget)
    }

    /// Returns whether the settled stride that ends at `end` holds the
    /// checkpoints `selection` asks for.
    ///
    /// A stride above the settled tip, or one whose stored rows or blocks are
    /// damaged, is not satisfied. `end` must be a cadence height above the
    /// history predecessor.
    pub fn tree_state_stride_satisfied(
        &self,
        end: BlockHeight,
        selection: TreeStateCheckpointSelection,
    ) -> Result<bool, CanonicalStoreError> {
        stride_ending_at_satisfied(self, end, selection)
    }

    /// Returns the lowest settled stride at or above `from` whose stored
    /// checkpoints do not yet satisfy `selection`, or `None` when every
    /// settled stride from `from` does.
    pub fn next_tree_state_stride(
        &self,
        from: BlockHeight,
        selection: TreeStateCheckpointSelection,
    ) -> Result<Option<TreeStateStride>, CanonicalStoreError> {
        next_tree_state_stride(self, from, selection)
    }

    /// Replays `stride` and returns the checkpoint rows `selection` adds.
    ///
    /// Fails with [`CanonicalStoreError::TreeStateStrideMismatch`] when the
    /// replay does not link, does not reach the recorded tree sizes, replays a
    /// block that is not the canonical block at its height, or does not
    /// reproduce a checkpoint already stored in the stride, including the
    /// cadence checkpoint that ends it.
    pub fn densify_tree_state_stride(
        &self,
        stride: TreeStateStride,
        selection: TreeStateCheckpointSelection,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<DensifiedTreeStateStride, CanonicalStoreError> {
        densify_tree_state_stride(self, stride, selection, network_upgrade_activations)
    }
}

impl RocksDbCanonicalStore {
    /// Examines settled strides from `from` until the scan's reads reach
    /// `read_budget`; see [`RocksDbCanonicalSecondary::scan_tree_state_strides`].
    pub fn scan_tree_state_strides(
        &self,
        from: BlockHeight,
        selection: TreeStateCheckpointSelection,
        read_budget: NonZeroU32,
    ) -> Result<TreeStateStrideScan, CanonicalStoreError> {
        scan_tree_state_strides(self, from, selection, read_budget)
    }

    /// Returns whether the settled stride that ends at `end` holds the
    /// checkpoints `selection` asks for; see
    /// [`RocksDbCanonicalSecondary::tree_state_stride_satisfied`].
    pub fn tree_state_stride_satisfied(
        &self,
        end: BlockHeight,
        selection: TreeStateCheckpointSelection,
    ) -> Result<bool, CanonicalStoreError> {
        stride_ending_at_satisfied(self, end, selection)
    }

    /// Returns the lowest settled stride at or above `from` whose stored
    /// checkpoints do not yet satisfy `selection`.
    pub fn next_tree_state_stride(
        &self,
        from: BlockHeight,
        selection: TreeStateCheckpointSelection,
    ) -> Result<Option<TreeStateStride>, CanonicalStoreError> {
        next_tree_state_stride(self, from, selection)
    }

    /// Replays `stride` against this primary; see
    /// [`RocksDbCanonicalSecondary::densify_tree_state_stride`].
    pub fn densify_tree_state_stride(
        &self,
        stride: TreeStateStride,
        selection: TreeStateCheckpointSelection,
        network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<DensifiedTreeStateStride, CanonicalStoreError> {
        densify_tree_state_stride(self, stride, selection, network_upgrade_activations)
    }

    /// Adds a densified stride's checkpoint rows in one batch.
    ///
    /// The stride must still end at or below the settled tip, at the
    /// canonical block the replay ended at, its seed and end checkpoints must
    /// still hold the bytes the replay started from and reproduced, and a row
    /// already stored at one of its heights must hold identical bytes.
    /// Otherwise nothing is written.
    pub fn commit_tree_state_stride(
        &self,
        densified: &DensifiedTreeStateStride,
    ) -> Result<TreeStateStrideCommit, CanonicalStoreError> {
        let settled_height = self.ready_evidence.sequence_checkpoint.through().height;
        if densified.stride.end_height > settled_height {
            return Err(densified.mismatch("the stride ends above the settled tip"));
        }
        // Checkpoint rows hold no block hash, so only the end block binds the
        // replayed blocks to this store's chain.
        let canonical_end = read_block_header_at(self, densified.stride.end_height)
            .map_err(|error| within_stride(densified.stride, error))?
            .map(|header| header.block_hash);
        if canonical_end != Some(densified.end_block.hash) {
            return Err(densified.mismatch("the stride was replayed on another chain"));
        }
        let db = &self.bounded_open.db;
        let family = column_family(db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
        let stored = |height: BlockHeight| {
            db.get_cf(&family, encode_block_position(height))
                .map_err(|source| CanonicalStoreError::RocksDbOperation {
                    operation: "tree-state densification row read",
                    source,
                })
        };
        if stored(densified.stride.seed_height)?.as_deref() != Some(densified.seed_row.as_slice())
            || stored(densified.stride.end_height)?.as_deref() != Some(densified.end_row.as_slice())
        {
            return Err(densified.mismatch("a stored checkpoint bounding the stride changed"));
        }
        let mut batch = WriteBatch::default();
        let mut commit = TreeStateStrideCommit {
            rows_written: 0,
            rows_already_present: 0,
        };
        for (height, row) in &densified.rows {
            match stored(*height)? {
                None => {
                    batch.put_cf(&family, encode_block_position(*height), row);
                    commit.rows_written += 1;
                }
                Some(existing) if existing == *row => commit.rows_already_present += 1,
                Some(_) => {
                    return Err(densified.mismatch(format!(
                        "the checkpoint stored at height {} differs from the replay",
                        height.value()
                    )));
                }
            }
        }
        if commit.rows_written > 0 {
            // The pass records its progress only after this returns, so the
            // rows must be durable first.
            let mut options = WriteOptions::default();
            options.set_sync(true);
            db.write_opt(&batch, &options).map_err(|source| {
                CanonicalStoreError::RocksDbOperation {
                    operation: "tree-state densification commit",
                    source,
                }
            })?;
        }
        Ok(commit)
    }
}

fn scan_tree_state_strides(
    store: &impl CanonicalServingRead,
    from: BlockHeight,
    selection: TreeStateCheckpointSelection,
    read_budget: NonZeroU32,
) -> Result<TreeStateStrideScan, CanonicalStoreError> {
    let predecessor_height = store
        .serving_build_plan()
        .history_predecessor()
        .block_id
        .height;
    let settled_height = store
        .serving_ready_evidence()
        .sequence_checkpoint
        .through()
        .height;
    let mut seed_height = first_seed_at_or_above(from.max(predecessor_height), predecessor_height);
    let mut scan = TreeStateStrideScan {
        incomplete: Vec::new(),
        damaged: Vec::new(),
        examined: 0,
        reads: 0,
        next_from: seed_height,
        reached_unsettled: false,
    };
    while scan.examined == 0 || scan.reads < read_budget.get() {
        let stride = TreeStateStride::after_seed(seed_height)?;
        if stride.end_height > settled_height {
            scan.reached_unsettled = true;
            break;
        }
        match stride_satisfies_counting_reads(store, stride, selection, &mut scan.reads)
            .map_err(|error| within_stride(stride, error))
        {
            Ok(true) => {}
            Ok(false) => scan.incomplete.push(stride),
            Err(CanonicalStoreError::TreeStateStrideMismatch { reason, .. }) => {
                scan.damaged.push((stride, reason));
            }
            Err(error) => return Err(error),
        }
        scan.examined += 1;
        seed_height = stride.end_height;
        scan.next_from = seed_height;
    }
    Ok(scan)
}

fn stride_ending_at_satisfied(
    store: &impl CanonicalServingRead,
    end: BlockHeight,
    selection: TreeStateCheckpointSelection,
) -> Result<bool, CanonicalStoreError> {
    let predecessor_height = store
        .serving_build_plan()
        .history_predecessor()
        .block_id
        .height;
    if !end.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE) || end <= predecessor_height {
        return Err(CanonicalStoreError::publication(format!(
            "height {} does not end a tree-state stride",
            end.value()
        )));
    }
    let settled_height = store
        .serving_ready_evidence()
        .sequence_checkpoint
        .through()
        .height;
    if end > settled_height {
        return Ok(false);
    }
    let stride = TreeStateStride {
        seed_height: BlockHeight::new(
            (end.value() - TREE_STATE_CHECKPOINT_STRIDE).max(predecessor_height.value()),
        ),
        end_height: end,
    };
    match stride_satisfies(store, stride, selection).map_err(|error| within_stride(stride, error)) {
        Ok(satisfied) => Ok(satisfied),
        Err(CanonicalStoreError::TreeStateStrideMismatch { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

fn next_tree_state_stride(
    store: &impl CanonicalServingRead,
    from: BlockHeight,
    selection: TreeStateCheckpointSelection,
) -> Result<Option<TreeStateStride>, CanonicalStoreError> {
    let predecessor_height = store
        .serving_build_plan()
        .history_predecessor()
        .block_id
        .height;
    let settled_height = store
        .serving_ready_evidence()
        .sequence_checkpoint
        .through()
        .height;
    let mut seed_height = first_seed_at_or_above(from.max(predecessor_height), predecessor_height);
    loop {
        let stride = TreeStateStride::after_seed(seed_height)?;
        if stride.end_height > settled_height {
            return Ok(None);
        }
        if !stride_satisfies(store, stride, selection)
            .map_err(|error| within_stride(stride, error))?
        {
            return Ok(Some(stride));
        }
        seed_height = stride.end_height;
    }
}

/// Returns the lowest stride seed at or above `height`.
fn first_seed_at_or_above(height: BlockHeight, predecessor_height: BlockHeight) -> BlockHeight {
    if height == predecessor_height || height.value().is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE) {
        return height;
    }
    let below = height.value() - height.value() % TREE_STATE_CHECKPOINT_STRIDE;
    BlockHeight::new(below.saturating_add(TREE_STATE_CHECKPOINT_STRIDE))
}

/// Returns whether every gap between the stored checkpoints from the stride's
/// seed through its end satisfies `selection`.
///
/// Every gap is checked, not only the first, because a stride committed under
/// a looser selection can satisfy it near its seed and not further on.
fn stride_satisfies(
    store: &impl CanonicalServingRead,
    stride: TreeStateStride,
    selection: TreeStateCheckpointSelection,
) -> Result<bool, CanonicalStoreError> {
    stride_satisfies_counting_reads(store, stride, selection, &mut 0)
}

/// [`stride_satisfies`], adding to `reads` the checkpoint rows it visited, at
/// least one, and the compact blocks it read.
fn stride_satisfies_counting_reads(
    store: &impl CanonicalServingRead,
    stride: TreeStateStride,
    selection: TreeStateCheckpointSelection,
    reads: &mut u32,
) -> Result<bool, CanonicalStoreError> {
    let stored_heights = stored_heights_through_end(store, stride)?;
    *reads = reads.saturating_add(
        u32::try_from(stored_heights.len())
            .unwrap_or(u32::MAX)
            .max(1),
    );
    if stored_heights.last() != Some(&stride.end_height) {
        return Err(stride_mismatch(
            stride,
            "the cadence checkpoint that ends the stride is absent",
        ));
    }
    let mut previous = stride.seed_height;
    for height in stored_heights {
        let gap_end = height.value() - 1;
        if gap_end > previous.value() {
            if selection == TreeStateCheckpointSelection::EveryBlock {
                return Ok(false);
            }
            *reads = reads.saturating_add(2);
            let anchored = commitment_count(stored_tree_sizes(store, stride, previous)?);
            let reached =
                commitment_count(stored_tree_sizes(store, stride, BlockHeight::new(gap_end))?);
            if selection.requires_checkpoint(reached.saturating_sub(anchored)) {
                return Ok(false);
            }
        }
        previous = height;
    }
    Ok(true)
}

/// Returns the heights of the checkpoints stored above the stride's seed
/// through its end, in ascending order.
fn stored_heights_through_end(
    store: &impl CanonicalServingRead,
    stride: TreeStateStride,
) -> Result<Vec<BlockHeight>, CanonicalStoreError> {
    let db = &store.serving_open().db;
    let family = column_family(db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    let Some(start) = stride.seed_height.next() else {
        return Ok(Vec::new());
    };
    let start = encode_block_position(start);
    let mut heights = Vec::new();
    for row in db.iterator_cf(&family, IteratorMode::From(&start, Direction::Forward)) {
        let (key, _) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "tree-state densification scan",
            source,
        })?;
        let height = decode_height_key_ascending(&key).map_err(|source| {
            CanonicalStoreError::publication(format!(
                "tree_state_checkpoint has an invalid height key: {source}"
            ))
        })?;
        if height > stride.end_height {
            break;
        }
        heights.push(height);
    }
    Ok(heights)
}

/// Reads the tree sizes after `height` from its compact block, or from the
/// history predecessor, which has no retained block.
fn stored_tree_sizes(
    store: &impl CanonicalServingRead,
    stride: TreeStateStride,
    height: BlockHeight,
) -> Result<ChainTipMetadata, CanonicalStoreError> {
    let predecessor = store.serving_build_plan().history_predecessor();
    if height == predecessor.block_id.height {
        return Ok(predecessor.tip_metadata());
    }
    let compact_block = read_compact_block_at(store, height)?.ok_or_else(|| {
        stride_mismatch(
            stride,
            format!("compact block at height {} is absent", height.value()),
        )
    })?;
    Ok(recorded_tree_sizes(&compact_block))
}

fn densify_tree_state_stride(
    store: &impl CanonicalServingRead,
    stride: TreeStateStride,
    selection: TreeStateCheckpointSelection,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<DensifiedTreeStateStride, CanonicalStoreError> {
    replay_stride(store, stride, selection, network_upgrade_activations)
        .map_err(|error| within_stride(stride, error))
}

fn replay_stride(
    store: &impl CanonicalServingRead,
    stride: TreeStateStride,
    selection: TreeStateCheckpointSelection,
    network_upgrade_activations: &NetworkUpgradeActivations,
) -> Result<DensifiedTreeStateStride, CanonicalStoreError> {
    if !store.serving_admits_activations_through(network_upgrade_activations, stride.end_height) {
        return Err(CanonicalStoreError::admission(
            store.serving_open().db.path(),
            "tree-state densification activation table differs from the admitted schedule at or below the stride",
        ));
    }
    let settled_height = store
        .serving_ready_evidence()
        .sequence_checkpoint
        .through()
        .height;
    if stride.end_height > settled_height {
        return Err(stride_mismatch(
            stride,
            "the stride ends above the settled tip",
        ));
    }
    let stored_rows = StoredStrideRows::read(store, stride)?;
    let seed = read_stored_tree_state_checkpoint_at(store, stride.seed_height)?
        .ok_or_else(|| stride_mismatch(stride, "the seed checkpoint is absent"))?;
    let seed_commitments = commitment_count(seed.tip_metadata());
    let mut checkpoint_commitments = seed_commitments;
    let mut rows = Vec::new();
    let trees = replay_compact_blocks(
        store,
        &seed,
        stride.end_height,
        network_upgrade_activations,
        |trees| {
            // Every replayed block must be the canonical block at its height;
            // only the costlier frontier roots wait for a row the stride
            // compares or writes.
            trees.canonical_block_time(store)?;
            let height = trees.tip().height;
            let commitments = commitment_count(trees.tree_sizes());
            let stored = stored_rows.at(height);
            // The end row is always stored, so the end height is replayed and
            // compared whether or not the selection picks it.
            let selected =
                selection.requires_checkpoint(commitments.saturating_sub(checkpoint_commitments));
            if !selected && stored.is_none() {
                return Ok(());
            }
            let checkpoint = trees.checkpoint(store)?;
            let row =
                encode_tree_state_checkpoint(checkpoint.block_time_seconds, &checkpoint.frontiers);
            checkpoint_commitments = commitments;
            match stored {
                Some(stored) if stored != row.as_slice() => Err(stride_mismatch(
                    stride,
                    format!(
                        "the replay does not reproduce the checkpoint stored at height {}",
                        height.value()
                    ),
                )),
                Some(_) => Ok(()),
                None => {
                    rows.push((height, row));
                    Ok(())
                }
            }
        },
    )?;
    Ok(DensifiedTreeStateStride {
        stride,
        end_block: trees.tip(),
        seed_row: stored_rows.seed_row,
        end_row: stored_rows.end_row,
        rows,
        replayed_commitment_count: commitment_count(trees.tree_sizes())
            .saturating_sub(seed_commitments),
    })
}

/// The raw checkpoint rows stored from a stride's seed through its end.
struct StoredStrideRows {
    seed_row: Vec<u8>,
    end_row: Vec<u8>,
    /// Rows strictly inside the stride, and the end row, by height.
    rows: BTreeMap<BlockHeight, Vec<u8>>,
}

impl StoredStrideRows {
    fn read(
        store: &impl CanonicalServingRead,
        stride: TreeStateStride,
    ) -> Result<Self, CanonicalStoreError> {
        let db = &store.serving_open().db;
        let family = column_family(db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
        let start = encode_block_position(stride.seed_height);
        let mut seed_row = None;
        let mut rows = BTreeMap::new();
        for row in db.iterator_cf(&family, IteratorMode::From(&start, Direction::Forward)) {
            let (key, row) = row.map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "tree-state densification stride read",
                source,
            })?;
            let height = decode_height_key_ascending(&key).map_err(|source| {
                CanonicalStoreError::publication(format!(
                    "tree_state_checkpoint has an invalid height key: {source}"
                ))
            })?;
            if height > stride.end_height {
                break;
            }
            if height == stride.seed_height {
                seed_row = Some(row.into_vec());
            } else {
                rows.insert(height, row.into_vec());
            }
        }
        let end_row = rows.get(&stride.end_height).cloned().ok_or_else(|| {
            stride_mismatch(
                stride,
                "the cadence checkpoint that ends the stride is absent",
            )
        })?;
        Ok(Self {
            seed_row: seed_row
                .ok_or_else(|| stride_mismatch(stride, "the seed checkpoint is absent"))?,
            end_row,
            rows,
        })
    }

    /// Returns the row stored at `height`, including the stride's end.
    fn at(&self, height: BlockHeight) -> Option<&[u8]> {
        self.rows.get(&height).map(Vec::as_slice)
    }
}

fn commitment_count(tree_sizes: ChainTipMetadata) -> u64 {
    u64::from(tree_sizes.sapling_commitment_tree_size)
        + u64::from(tree_sizes.orchard_commitment_tree_size)
        + u64::from(tree_sizes.ironwood_commitment_tree_size)
}

/// Attributes store damage found while reading a stride, such as a row that
/// does not decode, to that stride, so the pass can skip it.
fn within_stride(stride: TreeStateStride, error: CanonicalStoreError) -> CanonicalStoreError {
    if let CanonicalStoreError::PublicationRefused { reason } = error {
        return stride_mismatch(stride, reason);
    }
    error
}

fn stride_mismatch(stride: TreeStateStride, reason: impl Into<String>) -> CanonicalStoreError {
    CanonicalStoreError::TreeStateStrideMismatch {
        seed_height: stride.seed_height,
        end_height: stride.end_height,
        reason: reason.into(),
    }
}
