//! Commitment-tree checkpoints at any retained height.
//!
//! Construction stores a checkpoint at its history predecessor, every
//! [`TREE_STATE_CHECKPOINT_STRIDE`] blocks, and at its build tip, and live
//! following and replacement store one for every block. Any other retained
//! height is answered by seeding the trees from the newest checkpoint below it
//! and appending each stored compact block's note commitments, the derivation
//! construction and live following check against the node's tree state before
//! they store a block. A seed below `required_seed_height` means a checkpoint
//! is missing, so a reconstruction replays fewer than
//! [`TREE_STATE_CHECKPOINT_STRIDE`] blocks or fails as store damage.

use zinder_core::{
    BlockHeight, BlockHeightRange, BlockId, ChainEpochId, ChainTipMetadata,
    CommitmentTreeAccumulator, CommitmentTreeAccumulatorError, CommitmentTreeCheckpoint,
    CompactBlockArtifact, NetworkUpgradeActivations,
};

use super::{
    CanonicalStoreError, CanonicalStoreReadyEvidence, TREE_STATE_CHECKPOINT_STRIDE,
    reader::{
        CanonicalServingRead, decode_served_tree_state, decode_stored_tree_state_checkpoint,
        read_block_header_at, read_tree_state_checkpoint_row_at_or_before,
        visit_compact_blocks_in_range,
    },
    served_tree_state::ServedTreeState,
};

/// How the store answers the tree state at one retained height.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreeStateLookup {
    /// A checkpoint is stored at the height, read as a wallet read serves it.
    Stored(ServedTreeState),
    /// No checkpoint is stored at the height, so it must be reconstructed.
    Replay(TreeStateReplay),
}

/// A pending reconstruction of the tree state at one height from the seed
/// checkpoint the store found below it.
///
/// A store's lookup issues one from a validated stored checkpoint and
/// remembers the visible chain of the store that read it, so a store showing
/// that chain replays it without reading the seed again. A store showing
/// another chain, and every store given a replay built with [`Self::new`],
/// reads its own seed before it replays.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeStateReplay {
    seed: CommitmentTreeCheckpoint,
    height: BlockHeight,
    /// The visible chain of the store lookup that read and revalidated
    /// `seed`, or `None` when nothing proves where `seed` came from.
    seed_read_at: Option<VisibleChain>,
}

/// How a store answered a [`TreeStateReplay`].
///
/// A store showing another visible chain than the one that issued the replay
/// looks the height up again, so it can answer from a checkpoint it stores
/// there or replay from another seed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreeStateReplayOutcome {
    /// The store holds a checkpoint at the height, so nothing was replayed.
    Stored(ServedTreeState),
    /// The store rebuilt the tree state from the seed `replay` names.
    Replayed {
        /// The reconstruction the store ran.
        replay: Box<TreeStateReplay>,
        /// The tree state it produced.
        tree_state: ServedTreeState,
    },
}

impl TreeStateReplayOutcome {
    /// Returns the tree state at the replayed height.
    #[must_use]
    pub const fn tree_state(&self) -> &ServedTreeState {
        match self {
            Self::Stored(tree_state) | Self::Replayed { tree_state, .. } => tree_state,
        }
    }
}

/// The visible chain a store's READY evidence names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct VisibleChain {
    epoch: ChainEpochId,
    tip: BlockId,
    sequence_digest: [u8; 32],
}

impl VisibleChain {
    fn of(store: &impl CanonicalServingRead) -> Self {
        let evidence: CanonicalStoreReadyEvidence = store.serving_ready_evidence();
        Self {
            epoch: evidence.visible_epoch,
            tip: evidence.visible_tip,
            sequence_digest: evidence.visible_sequence_digest,
        }
    }
}

impl TreeStateReplay {
    /// Creates the reconstruction of the tree state at `height` from `seed`,
    /// a checkpoint stored below it, as a canonical reader of another storage
    /// engine answers a lookup.
    ///
    /// Nothing records where `seed` came from, so a reader replays such a
    /// reconstruction only from the seed it reads itself, never from `seed`.
    ///
    /// Returns `None` when `seed` is not below `height`.
    #[must_use]
    pub fn new(seed: CommitmentTreeCheckpoint, height: BlockHeight) -> Option<Self> {
        (seed.block_id.height < height).then_some(Self {
            seed,
            height,
            seed_read_at: None,
        })
    }

    /// Returns the stored checkpoint the reconstruction starts from.
    #[must_use]
    pub const fn seed(&self) -> &CommitmentTreeCheckpoint {
        &self.seed
    }

    /// Returns the height whose tree state the reconstruction produces.
    #[must_use]
    pub const fn height(&self) -> BlockHeight {
        self.height
    }

    /// Returns how many compact blocks the reconstruction replays.
    #[must_use]
    pub const fn replayed_blocks(&self) -> u32 {
        self.height.value() - self.seed.block_id.height.value()
    }
}

/// Reads the validated tree state at `height`, which can seed further tree
/// state.
///
/// A checkpoint stored at `height` is revalidated in full. Any other height
/// is reconstructed from the newest checkpoint below it and the stored
/// compact blocks above that one.
pub(super) fn read_tree_state_checkpoint_at(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
    match find_tree_state(store, height, activations, || Ok(()))? {
        None => Ok(None),
        Some(TreeStateSource::StoredRow(encoded)) => {
            decode_stored_tree_state_checkpoint(store, height, &encoded).map(Some)
        }
        Some(TreeStateSource::Seed(seed)) => {
            replay_compact_blocks(store, &seed, height, activations, |_| Ok(()))?
                .checkpoint(store)
                .map(Some)
        }
    }
}

/// Finds the checkpoint stored at `height`, read as a wallet read serves it,
/// or the revalidated seed a reconstruction of `height` starts from.
pub(super) fn lookup_tree_state(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> Result<Option<TreeStateLookup>, CanonicalStoreError> {
    lookup_until_cancelled(store, height, activations, || false)
}

pub(super) fn lookup_until_cancelled(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
    cancelled: impl Fn() -> bool,
) -> Result<Option<TreeStateLookup>, CanonicalStoreError> {
    let check = || {
        if cancelled() {
            Err(CanonicalStoreError::ReplayCancelled)
        } else {
            Ok(())
        }
    };
    Ok(match find_tree_state(store, height, activations, check)? {
        None => None,
        Some(TreeStateSource::StoredRow(encoded)) => Some(TreeStateLookup::Stored(
            decode_served_tree_state(store, height, &encoded)?,
        )),
        Some(TreeStateSource::Seed(seed)) => Some(TreeStateLookup::Replay(TreeStateReplay {
            seed,
            height,
            seed_read_at: Some(VisibleChain::of(store)),
        })),
    })
}

/// Where the tree state at one height comes from.
enum TreeStateSource {
    /// The checkpoint row stored at the height, not yet decoded.
    StoredRow(Box<[u8]>),
    /// The revalidated checkpoint a reconstruction of the height starts from.
    Seed(CommitmentTreeCheckpoint),
}

/// Finds the checkpoint row stored at `height`, or reads and revalidates the
/// seed a reconstruction of `height` starts from.
///
/// One seek reads the newest row at or below `height`, and only a row below
/// `height` is decoded here, so the caller decodes a stored row as its use
/// requires. Returns `Ok(None)` for a height above the visible tip or below
/// the history predecessor. A seed below [`required_seed_height`] is store
/// damage.
fn find_tree_state(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
    check: impl Fn() -> Result<(), CanonicalStoreError>,
) -> Result<Option<TreeStateSource>, CanonicalStoreError> {
    check()?;
    refuse_unadmitted_activations(store, height, activations)?;
    let history_predecessor_height = store
        .serving_build_plan()
        .history_predecessor()
        .block_id
        .height;
    if height > store.serving_ready_evidence().visible_tip.height
        || height < history_predecessor_height
    {
        return Ok(None);
    }
    let required_seed = required_seed_height(height, history_predecessor_height);
    let row = read_tree_state_checkpoint_row_at_or_before(store, height)?
        .filter(|row| row.height >= required_seed)
        .ok_or_else(|| {
            CanonicalStoreError::publication(format!(
                "tree-state checkpoint at height {} is absent",
                required_seed.value()
            ))
        })?;
    check()?;
    if row.height == height {
        return Ok(Some(TreeStateSource::StoredRow(row.encoded)));
    }
    decode_stored_tree_state_checkpoint(store, row.height, &row.encoded)
        .map(|seed| Some(TreeStateSource::Seed(seed)))
}

/// Reconstructs the tree state `replay` names, reporting how it was
/// answered.
///
/// Only a seed a lookup read on a store showing this store's visible chain
/// is replayed as given. For any other `replay` the store looks the height up
/// again, serving a checkpoint it stores there as stored, so `Ok(None)` means
/// the height left that store's retained history.
pub(super) fn replay_tree_state(
    store: &impl CanonicalServingRead,
    replay: &TreeStateReplay,
    activations: &NetworkUpgradeActivations,
) -> Result<Option<TreeStateReplayOutcome>, CanonicalStoreError> {
    replay_until_cancelled(store, replay, activations, || false)
}

pub(super) fn replay_until_cancelled(
    store: &impl CanonicalServingRead,
    replay: &TreeStateReplay,
    activations: &NetworkUpgradeActivations,
    cancelled: impl Fn() -> bool,
) -> Result<Option<TreeStateReplayOutcome>, CanonicalStoreError> {
    let check = || {
        if cancelled() {
            Err(CanonicalStoreError::ReplayCancelled)
        } else {
            Ok(())
        }
    };
    check()?;
    let replay = if replay.seed_read_at == Some(VisibleChain::of(store)) {
        refuse_unadmitted_activations(store, replay.height, activations)?;
        replay.clone()
    } else {
        match lookup_until_cancelled(store, replay.height, activations, &cancelled)? {
            None => return Ok(None),
            Some(TreeStateLookup::Stored(tree_state)) => {
                return Ok(Some(TreeStateReplayOutcome::Stored(tree_state)));
            }
            Some(TreeStateLookup::Replay(reread)) => reread,
        }
    };
    let mut trees =
        replay_compact_blocks(store, &replay.seed, replay.height, activations, |_| check())?;
    let block_time_seconds = trees.canonical_block_time(store)?;
    let tree_state = ServedTreeState::from_replayed_accumulator(
        trees.tip,
        block_time_seconds,
        &trees.accumulator,
    )
    .map_err(|source| replay_refused(trees.tip.height, &source))?;
    Ok(Some(TreeStateReplayOutcome::Replayed {
        replay: Box::new(replay),
        tree_state,
    }))
}

pub(super) fn refuse_unadmitted_activations(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> Result<(), CanonicalStoreError> {
    if store.serving_admits_activations_through(activations, height) {
        Ok(())
    } else {
        Err(CanonicalStoreError::admission(
            store.serving_open().db.path(),
            "tree-state reconstruction activation table differs from the admitted schedule at or below the requested height",
        ))
    }
}

/// Returns the lowest checkpoint height a reconstruction of `height` may seed
/// from.
///
/// Every store holds a checkpoint at its history predecessor, at each cadence
/// height construction wrote, and at every block live following or a
/// replacement wrote, and it never drops retained blocks. The newest
/// checkpoint at or below `height` is therefore no lower than the newest
/// cadence height at or below it, or the history predecessor when that is
/// newer. A lower one means a checkpoint is missing.
fn required_seed_height(height: BlockHeight, history_predecessor: BlockHeight) -> BlockHeight {
    let cadence_height = height.value() - height.value() % TREE_STATE_CHECKPOINT_STRIDE;
    BlockHeight::new(cadence_height.max(history_predecessor.value()))
}

/// Seeds the trees from `seed` and appends each stored compact block through
/// `height` in ascending order, reading the blocks in multi-get batches.
///
/// Each block is appended with [`ReplayedTrees::append_block`] and the trees
/// are then passed to `after_block`. Returns the trees after the block at
/// `height`.
pub(super) fn replay_compact_blocks(
    store: &impl CanonicalServingRead,
    seed: &CommitmentTreeCheckpoint,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
    mut after_block: impl FnMut(&mut ReplayedTrees) -> Result<(), CanonicalStoreError>,
) -> Result<ReplayedTrees, CanonicalStoreError> {
    let mut trees = ReplayedTrees::seed(seed, activations)?;
    let Some(first_height) = trees.tip.height.next() else {
        return Err(CanonicalStoreError::publication(
            "tree-state reconstruction height exceeds u32::MAX",
        ));
    };
    visit_compact_blocks_in_range(
        store,
        BlockHeightRange::inclusive(first_height, height),
        |compact_block| {
            trees.append_block(&compact_block)?;
            after_block(&mut trees)
        },
    )?;
    Ok(trees)
}

/// Commitment trees replayed from a stored checkpoint, and the block they
/// end at.
///
/// Tree-state reads and the densification of settled history both append
/// stored compact blocks through [`Self::append_block`], so a checkpoint the
/// densification stores is the one a read would reconstruct.
pub(super) struct ReplayedTrees {
    accumulator: CommitmentTreeAccumulator,
    tip: BlockId,
    /// Block time of the tip, once its canonical header has been checked.
    canonical_tip_time: Option<u32>,
}

impl ReplayedTrees {
    /// Seeds the trees from a validated stored checkpoint.
    fn seed(
        seed: &CommitmentTreeCheckpoint,
        activations: &NetworkUpgradeActivations,
    ) -> Result<Self, CanonicalStoreError> {
        let accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            seed.block_id.height,
            &seed.frontiers,
            activations,
        )
        .map_err(|source| replay_refused(seed.block_id.height, &source))?;
        Ok(Self {
            accumulator,
            tip: seed.block_id,
            canonical_tip_time: None,
        })
    }

    /// Appends the note commitments of the compact block above the tip.
    ///
    /// The block must extend the tip and reach the tree sizes it records.
    fn append_block(
        &mut self,
        compact_block: &CompactBlockArtifact,
    ) -> Result<(), CanonicalStoreError> {
        let height = compact_block.height();
        if compact_block.previous_block_hash() != self.tip.hash {
            return Err(CanonicalStoreError::publication(format!(
                "compact block at height {} does not extend the block below it",
                height.value()
            )));
        }
        append_compact_block_commitments(&mut self.accumulator, compact_block)
            .map_err(|source| replay_refused(height, &source))?;
        if self.accumulator.tip_metadata() != recorded_tree_sizes(compact_block) {
            return Err(CanonicalStoreError::publication(format!(
                "compact block at height {} records tree sizes its note commitments do not reach",
                height.value()
            )));
        }
        self.tip = BlockId::new(height, compact_block.block_hash());
        self.canonical_tip_time = None;
        Ok(())
    }

    /// Returns the block the trees end at.
    pub(super) const fn tip(&self) -> BlockId {
        self.tip
    }

    /// Returns the tree sizes after the tip.
    pub(super) fn tree_sizes(&self) -> ChainTipMetadata {
        self.accumulator.tip_metadata()
    }

    /// Checks that the tip is the canonical block at its height and returns
    /// its block time.
    pub(super) fn canonical_block_time(
        &mut self,
        store: &impl CanonicalServingRead,
    ) -> Result<u32, CanonicalStoreError> {
        if let Some(block_time_seconds) = self.canonical_tip_time {
            return Ok(block_time_seconds);
        }
        let height = self.tip.height;
        let header = read_block_header_at(store, height)?.ok_or_else(|| {
            CanonicalStoreError::publication(format!(
                "block header at height {} is absent",
                height.value()
            ))
        })?;
        if header.block_hash != self.tip.hash {
            return Err(CanonicalStoreError::publication(format!(
                "replayed compact block at height {} is not the canonical block",
                height.value()
            )));
        }
        let block_time_seconds = u32::try_from(header.block_time).map_err(|_| {
            CanonicalStoreError::publication(format!(
                "canonical block time at height {} is outside u32",
                height.value()
            ))
        })?;
        self.canonical_tip_time = Some(block_time_seconds);
        Ok(block_time_seconds)
    }

    /// Returns the checkpoint at the tip, which must be the canonical block
    /// at its height.
    pub(super) fn checkpoint(
        &mut self,
        store: &impl CanonicalServingRead,
    ) -> Result<CommitmentTreeCheckpoint, CanonicalStoreError> {
        let block_time_seconds = self.canonical_block_time(store)?;
        let frontiers = self
            .accumulator
            .cross_checked_frontiers()
            .map_err(|source| replay_refused(self.tip.height, &source))?;
        Ok(CommitmentTreeCheckpoint::new(
            self.tip,
            block_time_seconds,
            frontiers,
        ))
    }
}

/// Returns the tree sizes `compact_block` records after its commitments.
pub(super) fn recorded_tree_sizes(compact_block: &CompactBlockArtifact) -> ChainTipMetadata {
    let recorded = compact_block.chain_metadata();
    ChainTipMetadata::new(
        recorded.sapling_commitment_tree_size,
        recorded.orchard_commitment_tree_size,
        recorded.ironwood_commitment_tree_size,
    )
}

/// Appends one compact block's note commitments to `accumulator` in wire
/// order.
pub(super) fn append_compact_block_commitments(
    accumulator: &mut CommitmentTreeAccumulator,
    compact_block: &CompactBlockArtifact,
) -> Result<(), CommitmentTreeAccumulatorError> {
    let mut sapling = Vec::new();
    let mut orchard = Vec::new();
    let mut ironwood = Vec::new();
    for transaction in compact_block.transactions() {
        sapling.extend(
            transaction
                .data
                .sapling_outputs
                .iter()
                .map(|output| output.commitment),
        );
        orchard.extend(
            transaction
                .data
                .orchard_actions
                .iter()
                .map(|action| action.commitment),
        );
        ironwood.extend(
            transaction
                .data
                .ironwood_actions
                .iter()
                .map(|action| action.commitment),
        );
    }
    accumulator.append_block_commitments(compact_block.height(), &sapling, &orchard, &ironwood)
}

fn replay_refused(
    height: BlockHeight,
    source: &CommitmentTreeAccumulatorError,
) -> CanonicalStoreError {
    CanonicalStoreError::publication(format!(
        "commitment trees at height {} cannot be reconstructed: {source}",
        height.value()
    ))
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        BlockHash, BlockHeight, BlockId, CommitmentTreeCheckpoint, CommitmentTreeFrontiers,
    };

    use super::TreeStateReplay;

    fn seed(height: u32) -> CommitmentTreeCheckpoint {
        CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(height), BlockHash::from_bytes([1; 32])),
            0,
            CommitmentTreeFrontiers::default(),
        )
    }

    /// A reconstruction built outside a store lookup names a seed below its
    /// height and records no store that read it.
    #[test]
    fn a_replay_built_outside_a_lookup_starts_below_its_height_unread() {
        let built = TreeStateReplay::new(seed(4), BlockHeight::new(7));
        assert_eq!(
            built.as_ref().map(TreeStateReplay::replayed_blocks),
            Some(3)
        );
        assert!(built.is_some_and(|replay| replay.seed_read_at.is_none()));
        assert_eq!(TreeStateReplay::new(seed(7), BlockHeight::new(7)), None);
    }
}
