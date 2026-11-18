//! Chain fixtures whose compact blocks carry valid note commitments.
//!
//! [`with_note_commitments`] gives every block of a [`ChainFixture`] a
//! coinbase whose compact form carries Sapling outputs, Orchard actions, and
//! Ironwood actions in each pool the activation table makes active, and
//! records the resulting tree sizes as the block's chain metadata. A
//! production-store fixture built from such a chain derives each checkpoint
//! from those commitments, so a reader that replays compact blocks from a
//! checkpoint must reach the same trees. [`commitment_tree_checkpoints`]
//! derives the expected trees at chosen heights independently, one block at a
//! time from genesis.

use std::collections::BTreeMap;

use eyre::{Result, ensure, eyre};
use zinder_core::{
    BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput,
    CompactShieldedAction, CompactTransaction, CompactTransactionData, NetworkUpgradeActivations,
    ShieldedProtocol, TransactionId,
};

use crate::{ChainFixture, FixtureTransactionRows};

const SAPLING_POOL_TAG: u8 = 1;
const ORCHARD_POOL_TAG: u8 = 2;
const IRONWOOD_POOL_TAG: u8 = 3;

/// Returns `chain` with note commitments in every block.
///
/// Each block's commitments are derived from its hash, so a fork built with
/// [`ChainFixture::fork_at`] keeps its prefix's commitments and carries
/// different ones above the fork. The coinbase that carries them is the
/// block's transaction at index 0, so `chain` must not already carry
/// transaction rows. Apply this once, after the last
/// [`ChainFixture::extend_blocks`].
///
/// # Errors
///
/// Returns an error when `chain` and `activations` name different networks or
/// a block's tree sizes overflow `u32`.
pub fn with_note_commitments(
    chain: &ChainFixture,
    activations: &NetworkUpgradeActivations,
) -> Result<ChainFixture> {
    ensure!(
        chain.network() == activations.network(),
        "chain fixture and network-upgrade activations must use the same network"
    );
    let mut annotated = chain.clone();
    let mut tree_sizes = ChainTipMetadata::new(0, 0, 0);
    for block in chain.blocks() {
        let hash_bytes = block.hash.as_bytes();
        let commitment_counts = BlockCommitmentCounts::at(block.height, activations);
        let scan_data = CompactTransactionData {
            sapling_outputs: (0..commitment_counts.sapling)
                .map(|index| CompactSaplingOutput {
                    commitment: note_commitment(SAPLING_POOL_TAG, hash_bytes, index),
                    ephemeral_key: [0; 32],
                    ciphertext: [0; 52],
                })
                .collect(),
            orchard_actions: shielded_actions(
                ORCHARD_POOL_TAG,
                hash_bytes,
                commitment_counts.orchard,
            ),
            ironwood_actions: shielded_actions(
                IRONWOOD_POOL_TAG,
                hash_bytes,
                commitment_counts.ironwood,
            ),
            ..CompactTransactionData::default()
        };
        tree_sizes = commitment_counts.grow(tree_sizes)?;
        let mut transaction_id_bytes = hash_bytes;
        transaction_id_bytes[0] ^= 0x5a;
        let transaction_id = TransactionId::from_bytes(transaction_id_bytes);
        let previous_block_hash = if block.height == BlockHeight::new(1) {
            chain.network().genesis_hash()
        } else {
            block.parent_hash
        };
        let transactions = if commitment_counts.is_empty() {
            Vec::new()
        } else {
            vec![CompactTransaction {
                index: 0,
                transaction_id,
                data: scan_data,
            }]
        };
        annotated = annotated
            .with_transaction_rows(FixtureTransactionRows::coinbase(
                transaction_id,
                block.height,
                block.hash,
                format!("note-commitment-coinbase-{}", block.height.value()).into_bytes(),
            ))
            .with_compact_block_artifact(CompactBlockArtifact::new(
                BlockId::new(block.height, block.hash),
                previous_block_hash,
                block.block_time_seconds,
                transactions,
                CompactChainMetadata {
                    sapling_commitment_tree_size: tree_sizes.sapling_commitment_tree_size,
                    orchard_commitment_tree_size: tree_sizes.orchard_commitment_tree_size,
                    ironwood_commitment_tree_size: tree_sizes.ironwood_commitment_tree_size,
                },
            )?);
    }
    Ok(annotated)
}

/// Derives the commitment-tree checkpoint after the block of `chain` at each
/// of `heights`, in the order given.
///
/// The trees start empty at genesis and append each compact block's note
/// commitments in wire order, so the result does not depend on any stored
/// checkpoint. Only the requested heights are encoded: encoding a checkpoint
/// derives every pool's root, which costs far more than appending a block.
///
/// # Errors
///
/// Returns an error when a compact block's commitments cannot be appended or
/// do not reach the tree sizes its chain metadata records, or when `chain` has
/// no block at one of `heights`.
pub fn commitment_tree_checkpoints(
    chain: &ChainFixture,
    activations: &NetworkUpgradeActivations,
    heights: &[BlockHeight],
) -> Result<Vec<CommitmentTreeCheckpoint>> {
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        BlockHeight::new(0),
        &CommitmentTreeFrontiers::default(),
        activations,
    )?;
    let Some(last_height) = heights.iter().max().copied() else {
        return Ok(Vec::new());
    };
    let mut checkpoints = BTreeMap::new();
    for compact_block in chain.compact_block_artifacts() {
        if compact_block.height() > last_height {
            break;
        }
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
        accumulator.append_block_commitments(
            compact_block.height(),
            &sapling,
            &orchard,
            &ironwood,
        )?;
        let metadata = compact_block.chain_metadata();
        ensure!(
            accumulator.tip_metadata()
                == ChainTipMetadata::new(
                    metadata.sapling_commitment_tree_size,
                    metadata.orchard_commitment_tree_size,
                    metadata.ironwood_commitment_tree_size,
                ),
            "compact block {} records tree sizes its commitments do not reach",
            compact_block.height().value()
        );
        if heights.contains(&compact_block.height()) {
            checkpoints.insert(
                compact_block.height(),
                CommitmentTreeCheckpoint::new(
                    BlockId::new(compact_block.height(), compact_block.block_hash()),
                    compact_block.time(),
                    accumulator.validated_frontiers()?,
                ),
            );
        }
    }
    heights
        .iter()
        .map(|height| {
            checkpoints
                .get(height)
                .cloned()
                .ok_or_else(|| eyre!("chain fixture has no block at height {}", height.value()))
        })
        .collect()
}

/// Note commitments one block adds to each pool.
#[derive(Clone, Copy)]
struct BlockCommitmentCounts {
    sapling: u8,
    orchard: u8,
    ironwood: u8,
}

impl BlockCommitmentCounts {
    /// Varies the counts with the height, and adds none to an inactive pool.
    fn at(height: BlockHeight, activations: &NetworkUpgradeActivations) -> Self {
        let count = |protocol: ShieldedProtocol, modulus: u32, offset: u32| {
            let is_active = activations
                .activation_height_by_name(protocol.activation_upgrade_name())
                .is_some_and(|activation_height| activation_height <= height);
            if is_active {
                u8::try_from(height.value().wrapping_add(offset) % modulus).unwrap_or(0)
            } else {
                0
            }
        };
        Self {
            sapling: count(ShieldedProtocol::Sapling, 3, 0),
            orchard: count(ShieldedProtocol::Orchard, 3, 1),
            ironwood: count(ShieldedProtocol::Ironwood, 2, 0),
        }
    }

    const fn is_empty(self) -> bool {
        self.sapling == 0 && self.orchard == 0 && self.ironwood == 0
    }

    fn grow(self, sizes: ChainTipMetadata) -> Result<ChainTipMetadata> {
        let grow = |size: u32, count: u8| {
            size.checked_add(u32::from(count))
                .ok_or_else(|| eyre!("fixture tree size overflowed u32"))
        };
        Ok(ChainTipMetadata::new(
            grow(sizes.sapling_commitment_tree_size, self.sapling)?,
            grow(sizes.orchard_commitment_tree_size, self.orchard)?,
            grow(sizes.ironwood_commitment_tree_size, self.ironwood)?,
        ))
    }
}

fn shielded_actions(pool_tag: u8, hash_bytes: [u8; 32], count: u8) -> Vec<CompactShieldedAction> {
    (0..count)
        .map(|index| CompactShieldedAction {
            nullifier: [0; 32],
            commitment: note_commitment(pool_tag, hash_bytes, index),
            ephemeral_key: [0; 32],
            ciphertext: [0; 52],
        })
        .collect()
}

/// Encodes a small integer from the block hash, index, and pool.
///
/// A synthetic block hash repeats one four-byte word, and a fork's salt is
/// mixed into that word's low bytes, so all four bytes are kept. Every value
/// below 2^48 is a canonical field element in both the Sapling and the Pallas
/// base fields.
const fn note_commitment(pool_tag: u8, hash_bytes: [u8; 32], index: u8) -> [u8; 32] {
    let mut commitment = [0; 32];
    commitment[0] = hash_bytes[0];
    commitment[1] = hash_bytes[1];
    commitment[2] = hash_bytes[2];
    commitment[3] = hash_bytes[3];
    commitment[4] = index;
    commitment[5] = pool_tag;
    commitment
}
