//! Intrinsically valid commitment-tree frontier fixtures.
//!
//! Every fixture leaf is the canonical field element one, so a frontier built
//! here equals the frontier a commitment-tree accumulator derives after
//! appending the same number of `[1, 0, ..., 0]` note commitments.

use eyre::{Result, eyre};
use incrementalmerkletree::{
    Position,
    frontier::{CommitmentTree, Frontier},
};
use orchard::tree::MerkleHashOrchard;
use sapling::Node as SaplingNode;
use zcash_primitives::merkle_tree::{HashSer, write_commitment_tree};
use zinder_core::{
    CommitmentTreeFrontier, FinalNoteCommitmentRoot, SUBTREE_LEAF_COUNT, ShieldedProtocol,
};

/// Note commitment bytes of every fixture leaf: the canonical field element one.
pub const FIXTURE_NOTE_COMMITMENT: [u8; 32] = {
    let mut commitment = [0; 32];
    commitment[0] = 1;
    commitment
};

/// Returns a canonical Sapling frontier containing one note commitment.
pub fn one_leaf_sapling_frontier() -> Result<CommitmentTreeFrontier> {
    one_leaf_frontier(ShieldedProtocol::Sapling)
}

/// Returns a canonical Sapling frontier after exactly one completed subtree.
pub fn completed_sapling_subtree_frontier() -> Result<CommitmentTreeFrontier> {
    completed_subtree_frontier(ShieldedProtocol::Sapling)
}

/// Returns a canonical `protocol` frontier containing one note commitment.
///
/// Orchard and Ironwood share the Orchard note-commitment tree encoding.
pub fn one_leaf_frontier(protocol: ShieldedProtocol) -> Result<CommitmentTreeFrontier> {
    frontier_of_size(protocol, 1)
}

/// Returns a canonical `protocol` frontier after exactly one completed subtree.
///
/// Building this tree by appending 2^16 commitments is too slow for a test, so
/// the frontier is assembled from its position, leaf, and ommers directly.
pub fn completed_subtree_frontier(protocol: ShieldedProtocol) -> Result<CommitmentTreeFrontier> {
    frontier_of_size(protocol, SUBTREE_LEAF_COUNT)
}

fn frontier_of_size(protocol: ShieldedProtocol, tree_size: u32) -> Result<CommitmentTreeFrontier> {
    let position = Position::from(
        u64::from(tree_size)
            .checked_sub(1)
            .ok_or_else(|| eyre!("a non-empty frontier fixture needs at least one leaf"))?,
    );
    if protocol == ShieldedProtocol::Sapling {
        let leaf = Option::<SaplingNode>::from(SaplingNode::from_bytes(FIXTURE_NOTE_COMMITMENT))
            .ok_or_else(|| eyre!("one must be a canonical Sapling field element"))?;
        return frontier_from_parts(protocol, position, leaf, |root| {
            let mut root_bytes = root.to_bytes();
            root_bytes.reverse();
            root_bytes
        });
    }
    let leaf =
        Option::<MerkleHashOrchard>::from(MerkleHashOrchard::from_bytes(&FIXTURE_NOTE_COMMITMENT))
            .ok_or_else(|| eyre!("one must be a canonical Pallas base field element"))?;
    frontier_from_parts(protocol, position, leaf, MerkleHashOrchard::to_bytes)
}

/// Builds a frontier whose every ommer equals `leaf`, then validates it
/// through the production frontier codec.
fn frontier_from_parts<Node>(
    protocol: ShieldedProtocol,
    position: Position,
    leaf: Node,
    root_bytes_in_rpc_order: impl Fn(&Node) -> [u8; 32],
) -> Result<CommitmentTreeFrontier>
where
    Node: Clone + incrementalmerkletree::Hashable + HashSer,
{
    let ommer_count = usize::from(position.past_ommer_count());
    let frontier: Frontier<Node, 32> =
        Frontier::from_parts(position, leaf.clone(), vec![leaf; ommer_count])
            .map_err(|error| eyre!("valid {protocol:?} frontier fixture rejected: {error:?}"))?;
    let tree = CommitmentTree::from_frontier(&frontier);
    let mut final_state_bytes = Vec::new();
    write_commitment_tree(&tree, &mut final_state_bytes)?;
    Ok(CommitmentTreeFrontier::from_canonical_final_state(
        protocol,
        FinalNoteCommitmentRoot::from_bytes(root_bytes_in_rpc_order(&frontier.root())),
        final_state_bytes,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_have_exact_nonzero_sizes() -> Result<()> {
        for protocol in [
            ShieldedProtocol::Sapling,
            ShieldedProtocol::Orchard,
            ShieldedProtocol::Ironwood,
        ] {
            assert_eq!(one_leaf_frontier(protocol)?.tree_size(), 1);
            assert_eq!(
                completed_subtree_frontier(protocol)?.tree_size(),
                SUBTREE_LEAF_COUNT
            );
        }
        Ok(())
    }
}
