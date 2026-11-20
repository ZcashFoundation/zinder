//! Note commitments for the published test chain.
//!
//! The published test chain's blocks carry no shielded data. [`NoteCommitmentTrees`]
//! gives each block a coinbase whose compact form carries Sapling outputs,
//! Orchard actions, and Ironwood actions in every pool the activation table
//! makes active, and derives the tree sizes and checkpoint frontiers those
//! commitments reach, so a replay crosses a nonzero commitment count in every
//! pool.

use zinder_core::{
    BlockId, ChainTipMetadata, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata, CompactSaplingOutput,
    CompactShieldedAction, CompactTransaction, CompactTransactionData, NetworkUpgradeActivations,
    ShieldedProtocol,
};

use super::CanonicalBuildBlock;

type FixtureResult<T> = Result<T, Box<dyn std::error::Error>>;

/// The commitment trees after the blocks appended so far.
pub(super) struct NoteCommitmentTrees {
    accumulator: CommitmentTreeAccumulator,
    activations: NetworkUpgradeActivations,
}

impl NoteCommitmentTrees {
    /// Starts from the empty trees at genesis.
    pub(super) fn new(activations: &NetworkUpgradeActivations) -> FixtureResult<Self> {
        Ok(Self {
            accumulator: CommitmentTreeAccumulator::from_validated_frontiers(
                zinder_core::BlockHeight::new(0),
                &CommitmentTreeFrontiers::default(),
                activations,
            )?,
            activations: activations.clone(),
        })
    }

    /// Appends the commitments of `block`, which must extend the last appended
    /// block, and records them and the resulting tree sizes in `block`.
    pub(super) fn append_block(&mut self, block: &mut CanonicalBuildBlock) -> FixtureResult<()> {
        let header = &block.facts.block_header;
        let height = header.height;
        let sapling = commitments(
            self.commitment_count(ShieldedProtocol::Sapling, 3, 0, height.value()),
            SAPLING_TAG,
            height.value(),
        );
        let orchard = commitments(
            self.commitment_count(ShieldedProtocol::Orchard, 3, 1, height.value()),
            ORCHARD_TAG,
            height.value(),
        );
        let ironwood = commitments(
            self.commitment_count(ShieldedProtocol::Ironwood, 2, 0, height.value()),
            IRONWOOD_TAG,
            height.value(),
        );
        self.accumulator
            .append_block_commitments(height, &sapling, &orchard, &ironwood)?;

        let transaction_id = block
            .facts
            .transactions
            .first()
            .ok_or("the published test block has no coinbase")?
            .public_facts
            .transaction_id;
        let transactions = if sapling.is_empty() && orchard.is_empty() && ironwood.is_empty() {
            Vec::new()
        } else {
            vec![CompactTransaction {
                index: 0,
                transaction_id,
                data: CompactTransactionData {
                    sapling_outputs: sapling
                        .iter()
                        .map(|&commitment| CompactSaplingOutput {
                            commitment,
                            ephemeral_key: [0; 32],
                            ciphertext: [0; 52],
                        })
                        .collect(),
                    orchard_actions: shielded_actions(&orchard),
                    ironwood_actions: shielded_actions(&ironwood),
                    ..CompactTransactionData::default()
                },
            }]
        };
        let sizes = self.accumulator.tip_metadata();
        block.compact_block = CompactBlockArtifact::new(
            BlockId::new(height, header.block_hash),
            header.parent_hash,
            height.value(),
            transactions,
            chain_metadata(sizes),
        )?;
        block.tip_metadata = sizes;
        Ok(())
    }

    /// Returns the checkpoint after the last appended block, taking its
    /// identity and time from `block`.
    pub(super) fn checkpoint(
        &mut self,
        block: &CanonicalBuildBlock,
    ) -> FixtureResult<CommitmentTreeCheckpoint> {
        let header = &block.facts.block_header;
        Ok(CommitmentTreeCheckpoint::new(
            BlockId::new(header.height, header.block_hash),
            u32::try_from(header.block_time)?,
            self.accumulator.validated_frontiers()?,
        ))
    }

    /// Varies the count with the height, and adds none to an inactive pool.
    fn commitment_count(
        &self,
        protocol: ShieldedProtocol,
        modulus: u32,
        offset: u32,
        height: u32,
    ) -> u8 {
        let is_active = self
            .activations
            .activation_height_by_name(protocol.activation_upgrade_name())
            .is_some_and(|activation_height| activation_height.value() <= height);
        if is_active {
            u8::try_from((height + offset) % modulus).unwrap_or(0)
        } else {
            0
        }
    }
}

const SAPLING_TAG: u8 = 1;
const ORCHARD_TAG: u8 = 2;
const IRONWOOD_TAG: u8 = 3;

/// Encodes small integers, which are canonical field elements in every pool.
fn commitments(count: u8, pool_tag: u8, height: u32) -> Vec<[u8; 32]> {
    (0..count)
        .map(|index| {
            let mut commitment = [0; 32];
            commitment[..4].copy_from_slice(&height.to_le_bytes());
            commitment[4] = index;
            commitment[5] = pool_tag;
            commitment
        })
        .collect()
}

fn shielded_actions(commitments: &[[u8; 32]]) -> Vec<CompactShieldedAction> {
    commitments
        .iter()
        .map(|&commitment| CompactShieldedAction {
            nullifier: [0; 32],
            commitment,
            ephemeral_key: [0; 32],
            ciphertext: [0; 52],
        })
        .collect()
}

fn chain_metadata(sizes: ChainTipMetadata) -> CompactChainMetadata {
    CompactChainMetadata {
        sapling_commitment_tree_size: sizes.sapling_commitment_tree_size,
        orchard_commitment_tree_size: sizes.orchard_commitment_tree_size,
        ironwood_commitment_tree_size: sizes.ironwood_commitment_tree_size,
    }
}
