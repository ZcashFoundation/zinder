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
/// Only a store lookup issues one, so its seed is a validated stored
/// checkpoint. It remembers the visible chain of the store that read the
/// seed, and a store showing another chain reads the seed again before it
/// replays.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeStateReplay {
    seed: CommitmentTreeCheckpoint,
    height: BlockHeight,
    seed_chain: VisibleChain,
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
    match find_tree_state(store, height, activations)? {
        None => Ok(None),
        Some(TreeStateSource::StoredRow(encoded)) => {
            decode_stored_tree_state_checkpoint(store, height, &encoded).map(Some)
        }
        Some(TreeStateSource::Seed(seed)) => {
            reconstruct(store, &seed, height, activations).map(Some)
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
    Ok(match find_tree_state(store, height, activations)? {
        None => None,
        Some(TreeStateSource::StoredRow(encoded)) => Some(TreeStateLookup::Stored(
            decode_served_tree_state(store, height, &encoded)?,
        )),
        Some(TreeStateSource::Seed(seed)) => Some(TreeStateLookup::Replay(TreeStateReplay {
            seed,
            height,
            seed_chain: VisibleChain::of(store),
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
) -> Result<Option<TreeStateSource>, CanonicalStoreError> {
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
    if row.height == height {
        return Ok(Some(TreeStateSource::StoredRow(row.encoded)));
    }
    decode_stored_tree_state_checkpoint(store, row.height, &row.encoded)
        .map(|seed| Some(TreeStateSource::Seed(seed)))
}

/// Reconstructs the tree state `replay` names, reporting how it was
/// answered.
///
/// A store showing another visible chain than the one that issued `replay`
/// looks the height up again, serving a checkpoint it stores there as stored,
/// so `Ok(None)` means the height left that store's retained history.
pub(super) fn replay_tree_state(
    store: &impl CanonicalServingRead,
    replay: &TreeStateReplay,
    activations: &NetworkUpgradeActivations,
) -> Result<Option<TreeStateReplayOutcome>, CanonicalStoreError> {
    let replay = if replay.seed_chain == VisibleChain::of(store) {
        refuse_unadmitted_activations(store, replay.height, activations)?;
        replay.clone()
    } else {
        match lookup_tree_state(store, replay.height, activations)? {
            None => return Ok(None),
            Some(TreeStateLookup::Stored(tree_state)) => {
                return Ok(Some(TreeStateReplayOutcome::Stored(tree_state)));
            }
            Some(TreeStateLookup::Replay(reread)) => reread,
        }
    };
    let checkpoint = reconstruct(store, &replay.seed, replay.height, activations)?;
    Ok(Some(TreeStateReplayOutcome::Replayed {
        replay: Box::new(replay),
        tree_state: ServedTreeState::from(checkpoint),
    }))
}

fn refuse_unadmitted_activations(
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

/// Replays the stored compact blocks above `seed` through `height` and checks
/// the result against the canonical block at `height`.
fn reconstruct(
    store: &impl CanonicalServingRead,
    seed: &CommitmentTreeCheckpoint,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> Result<CommitmentTreeCheckpoint, CanonicalStoreError> {
    let (mut accumulator, tip) = replay_compact_blocks(store, seed, height, activations)?;
    let header = read_block_header_at(store, height)?.ok_or_else(|| {
        CanonicalStoreError::publication(format!(
            "tree-state reconstruction header at height {} is absent",
            height.value()
        ))
    })?;
    if header.block_hash != tip.hash {
        return Err(CanonicalStoreError::publication(format!(
            "replayed compact blocks do not end at the canonical block at height {}",
            height.value()
        )));
    }
    let block_time_seconds = u32::try_from(header.block_time).map_err(|_| {
        CanonicalStoreError::publication(format!(
            "canonical block time at height {} is outside u32",
            height.value()
        ))
    })?;
    let frontiers = accumulator
        .cross_checked_frontiers()
        .map_err(|source| replay_refused(height, &source))?;
    Ok(CommitmentTreeCheckpoint::new(
        tip,
        block_time_seconds,
        frontiers,
    ))
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
/// Every block must extend the one before it, starting from `seed`'s block,
/// and reach the tree sizes its compact block records. Returns the trees and
/// the identity of the last replayed block.
fn replay_compact_blocks(
    store: &impl CanonicalServingRead,
    seed: &CommitmentTreeCheckpoint,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> Result<(CommitmentTreeAccumulator, BlockId), CanonicalStoreError> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        seed.block_id.height,
        &seed.frontiers,
        activations,
    )
    .map_err(|source| replay_refused(seed.block_id.height, &source))?;
    let mut tip = seed.block_id;
    let Some(first_height) = tip.height.next() else {
        return Err(CanonicalStoreError::publication(
            "tree-state reconstruction height exceeds u32::MAX",
        ));
    };
    visit_compact_blocks_in_range(
        store,
        BlockHeightRange::inclusive(first_height, height),
        |compact_block| {
            let next_height = compact_block.height();
            if compact_block.previous_block_hash() != tip.hash {
                return Err(CanonicalStoreError::publication(format!(
                    "compact block at height {} does not extend the block below it",
                    next_height.value()
                )));
            }
            append_compact_block_commitments(&mut accumulator, &compact_block)
                .map_err(|source| replay_refused(next_height, &source))?;
            let recorded = compact_block.chain_metadata();
            if accumulator.tip_metadata()
                != ChainTipMetadata::new(
                    recorded.sapling_commitment_tree_size,
                    recorded.orchard_commitment_tree_size,
                    recorded.ironwood_commitment_tree_size,
                )
            {
                return Err(CanonicalStoreError::publication(format!(
                    "compact block at height {} records tree sizes its note commitments do not reach",
                    next_height.value()
                )));
            }
            tip = BlockId::new(next_height, compact_block.block_hash());
            Ok(())
        },
    )?;
    Ok((accumulator, tip))
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
