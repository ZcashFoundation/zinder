//! Commitment-tree checkpoints at any retained height.
//!
//! Construction stores a checkpoint every [`super::TREE_STATE_CHECKPOINT_STRIDE`]
//! blocks and at its build tip, and live following stores one for every
//! block. Any other retained height is answered by seeding the trees from the
//! newest checkpoint below it and appending each stored compact block's note
//! commitments, the derivation construction and live following check against
//! the node's tree state before they store a block.

use zinder_core::{
    BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeAccumulator,
    CommitmentTreeAccumulatorError, CommitmentTreeCheckpoint, CompactBlockArtifact,
    NetworkUpgradeActivations,
};

use super::{
    CanonicalStoreError,
    reader::{
        CanonicalServingRead, read_block_header_at, read_compact_block_at,
        read_tree_state_checkpoint_at_or_before,
    },
};

/// Reads the stored checkpoint at `height`, or reconstructs it from the newest
/// checkpoint below `height` and the stored compact blocks above that one.
pub(super) fn read_tree_state_checkpoint_at(
    store: &impl CanonicalServingRead,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> Result<Option<CommitmentTreeCheckpoint>, CanonicalStoreError> {
    if !store.serving_admits_activations(activations) {
        return Err(CanonicalStoreError::admission(
            store.serving_open().db.path(),
            "tree-state reconstruction activation table differs from the admitted store identity",
        ));
    }
    if height > store.serving_ready_evidence().visible_tip.height {
        return Ok(None);
    }
    let Some(seed) = read_tree_state_checkpoint_at_or_before(store, height)? else {
        return Ok(None);
    };
    if seed.block_id.height == height {
        return Ok(Some(seed));
    }
    let (mut accumulator, tip) = replay_compact_blocks(store, &seed, height, activations)?;
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
    Ok(Some(CommitmentTreeCheckpoint::new(
        tip,
        block_time_seconds,
        frontiers,
    )))
}

/// Seeds the trees from `seed` and appends each stored compact block through
/// `height`, one height at a time.
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
    while tip.height < height {
        let next_height = tip.height.next().ok_or_else(|| {
            CanonicalStoreError::publication("tree-state reconstruction height exceeds u32::MAX")
        })?;
        let compact_block = read_compact_block_at(store, next_height)?.ok_or_else(|| {
            CanonicalStoreError::publication(format!(
                "compact block at height {} is absent",
                next_height.value()
            ))
        })?;
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
    }
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
