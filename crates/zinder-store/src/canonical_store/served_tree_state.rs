//! Tree state in the form a wallet read serves it.
//!
//! Every stored checkpoint row is validated in full when it is written. A
//! read that only serves a row checks the framing of each frontier and skips
//! its root, without decoding or hashing tree nodes. Its result is a separate
//! type from [`CommitmentTreeCheckpoint`], so nothing that writes, loads, or
//! replays tree state can take it as input.

use zinder_core::{
    BlockId, ChainTipMetadata, CommitmentTreeAccumulator, CommitmentTreeAccumulatorError,
    CommitmentTreeCheckpoint, CommitmentTreeFrontier, CommitmentTreeFrontierValidationError,
    ShieldedProtocol,
};

/// Parent slots in the canonical encoding of a nonempty note commitment
/// tree: one for each level of the depth-32 tree above its leaves.
const NONEMPTY_TREE_PARENT_SLOTS: u8 = 31;

/// One pool's `finalState` bytes, or `None` before that pool activates.
type FinalStateBytes = Option<Box<[u8]>>;

/// Bytes of one present node in a `finalState` encoding.
const NODE_BYTES: usize = 32;

/// The tree state after one block, in the form a wallet read serves it: the
/// block, its time, and each active pool's `finalState` bytes and tree size.
///
/// A store reads a stored checkpoint into this form after checking only the
/// framing of each frontier, without decoding or hashing tree nodes. It
/// carries no roots, and nothing that writes, loads, or replays tree state
/// accepts it. [`CommitmentTreeCheckpoint`] is the validated form those paths
/// take, and converts into this one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServedTreeState {
    block_id: BlockId,
    block_time_seconds: u32,
    frontiers: ServedFrontiers,
}

impl ServedTreeState {
    pub(in crate::canonical_store) const fn new(
        block_id: BlockId,
        block_time_seconds: u32,
        frontiers: ServedFrontiers,
    ) -> Self {
        Self {
            block_id,
            block_time_seconds,
            frontiers,
        }
    }

    pub(super) fn from_replayed_accumulator(
        block_id: BlockId,
        block_time_seconds: u32,
        accumulator: &CommitmentTreeAccumulator,
    ) -> Result<Self, CommitmentTreeAccumulatorError> {
        let sizes = accumulator.tip_metadata();
        let frontier = |protocol| {
            accumulator.final_state_bytes(protocol).map(|bytes| {
                bytes.map(|final_state_bytes| ServedFrontier {
                    tree_size: sizes.commitment_tree_size(protocol),
                    final_state_bytes,
                })
            })
        };
        Ok(Self::new(
            block_id,
            block_time_seconds,
            ServedFrontiers::new(
                frontier(ShieldedProtocol::Sapling)?,
                frontier(ShieldedProtocol::Orchard)?,
                frontier(ShieldedProtocol::Ironwood)?,
            ),
        ))
    }

    /// Returns the block this tree state follows.
    #[must_use]
    pub const fn block_id(&self) -> BlockId {
        self.block_id
    }

    /// Returns the block timestamp in Unix seconds.
    #[must_use]
    pub const fn block_time_seconds(&self) -> u32 {
        self.block_time_seconds
    }

    /// Returns the `finalState` bytes of the `protocol` tree, or `None`
    /// before that pool activates.
    #[must_use]
    pub fn final_state_bytes(&self, protocol: ShieldedProtocol) -> Option<&[u8]> {
        self.frontiers
            .get(protocol)
            .map(|frontier| &*frontier.final_state_bytes)
    }

    /// Consumes the tree state and returns the Sapling, Orchard, and Ironwood
    /// `finalState` bytes in that order, without copying them.
    #[must_use]
    pub fn into_final_state_bytes(self) -> (FinalStateBytes, FinalStateBytes, FinalStateBytes) {
        let ServedFrontiers {
            sapling,
            orchard,
            ironwood,
        } = self.frontiers;
        let final_state_bytes = |frontier: ServedFrontier| frontier.final_state_bytes;
        (
            sapling.map(final_state_bytes),
            orchard.map(final_state_bytes),
            ironwood.map(final_state_bytes),
        )
    }

    /// Returns the commitment-tree sizes after the block.
    #[must_use]
    pub fn tip_metadata(&self) -> ChainTipMetadata {
        let tree_size = |protocol| {
            self.frontiers
                .get(protocol)
                .map_or(0, |frontier| frontier.tree_size)
        };
        ChainTipMetadata::new(
            tree_size(ShieldedProtocol::Sapling),
            tree_size(ShieldedProtocol::Orchard),
            tree_size(ShieldedProtocol::Ironwood),
        )
    }
}

impl From<&CommitmentTreeCheckpoint> for ServedTreeState {
    fn from(checkpoint: &CommitmentTreeCheckpoint) -> Self {
        let frontier = |protocol| checkpoint.frontiers.get(protocol).map(ServedFrontier::from);
        Self::new(
            checkpoint.block_id,
            checkpoint.block_time_seconds,
            ServedFrontiers::new(
                frontier(ShieldedProtocol::Sapling),
                frontier(ShieldedProtocol::Orchard),
                frontier(ShieldedProtocol::Ironwood),
            ),
        )
    }
}

impl From<CommitmentTreeCheckpoint> for ServedTreeState {
    fn from(checkpoint: CommitmentTreeCheckpoint) -> Self {
        let (sapling, orchard, ironwood) = checkpoint.frontiers.into_parts();
        Self::new(
            checkpoint.block_id,
            checkpoint.block_time_seconds,
            ServedFrontiers::new(
                sapling.map(ServedFrontier::from),
                orchard.map(ServedFrontier::from),
                ironwood.map(ServedFrontier::from),
            ),
        )
    }
}

/// The frontiers of a [`ServedTreeState`], one for each active pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::canonical_store) struct ServedFrontiers {
    sapling: Option<ServedFrontier>,
    orchard: Option<ServedFrontier>,
    ironwood: Option<ServedFrontier>,
}

impl ServedFrontiers {
    pub(in crate::canonical_store) const fn new(
        sapling: Option<ServedFrontier>,
        orchard: Option<ServedFrontier>,
        ironwood: Option<ServedFrontier>,
    ) -> Self {
        Self {
            sapling,
            orchard,
            ironwood,
        }
    }

    const fn get(&self, protocol: ShieldedProtocol) -> Option<&ServedFrontier> {
        match protocol {
            ShieldedProtocol::Sapling => self.sapling.as_ref(),
            ShieldedProtocol::Orchard => self.orchard.as_ref(),
            ShieldedProtocol::Ironwood => self.ironwood.as_ref(),
            // A store admits only these three pools.
            _ => None,
        }
    }
}

/// The tree size and canonical frontier bytes a wallet read returns for one pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::canonical_store) struct ServedFrontier {
    tree_size: u32,
    final_state_bytes: Box<[u8]>,
}

impl ServedFrontier {
    /// Checks the framing of stored `finalState` bytes, as
    /// [`framed_tree_size`] describes, and keeps them with the tree size.
    pub(in crate::canonical_store) fn from_stored_final_state(
        final_state_bytes: Vec<u8>,
    ) -> Result<Self, CommitmentTreeFrontierValidationError> {
        let tree_size = framed_tree_size(&final_state_bytes)?;
        Ok(Self {
            tree_size,
            final_state_bytes: final_state_bytes.into_boxed_slice(),
        })
    }
}

impl From<&CommitmentTreeFrontier> for ServedFrontier {
    fn from(frontier: &CommitmentTreeFrontier) -> Self {
        Self {
            tree_size: frontier.tree_size(),
            final_state_bytes: Box::from(frontier.final_state_bytes()),
        }
    }
}

impl From<CommitmentTreeFrontier> for ServedFrontier {
    fn from(frontier: CommitmentTreeFrontier) -> Self {
        Self {
            tree_size: frontier.tree_size(),
            final_state_bytes: frontier.into_final_state_bytes(),
        }
    }
}

/// Returns the size of the tree a stored `finalState` encoding holds,
/// checking only that it is laid out as a canonical encoding.
///
/// The canonical encoding of a depth-32 legacy `CommitmentTree` is a left
/// leaf, a right leaf, and a parent vector, each node a presence flag of 0 or
/// 1 followed by 32 node bytes when present. The empty tree is three zero
/// bytes, and every other tree has a left leaf and exactly
/// [`NONEMPTY_TREE_PARENT_SLOTS`] parent slots. The size counts the present
/// nodes as the official codec does. Node bytes are skipped without being
/// decoded, so neither a node that is not a canonical field element nor a
/// stored root that the nodes do not derive is detected here.
fn framed_tree_size(
    final_state_bytes: &[u8],
) -> Result<u32, CommitmentTreeFrontierValidationError> {
    let invalid = |reason| CommitmentTreeFrontierValidationError::InvalidEncoding { reason };
    let mut remaining = final_state_bytes;
    let left = skip_framed_node(&mut remaining)?;
    let right = skip_framed_node(&mut remaining)?;
    // Both canonical parent counts are one-byte CompactSize values, so a
    // longer CompactSize fails the count check below.
    let (&parent_slots, parents) = remaining
        .split_first()
        .ok_or_else(|| invalid("finalState ends before its parent count"))?;
    let (mut tree_size, required_parent_slots) = match (left, right) {
        (false, false) => (0_u64, 0),
        (false, true) => return Err(invalid("right leaf is present without a left leaf")),
        (true, false) => (1, NONEMPTY_TREE_PARENT_SLOTS),
        (true, true) => (2, NONEMPTY_TREE_PARENT_SLOTS),
    };
    if parent_slots != required_parent_slots {
        return Err(invalid(if left {
            "nonempty tree does not have one parent slot per level above its leaves"
        } else {
            "empty tree has parent slots"
        }));
    }
    remaining = parents;
    for level in 1..=parent_slots {
        if skip_framed_node(&mut remaining)? {
            tree_size += 1_u64 << level;
        }
    }
    if !remaining.is_empty() {
        return Err(invalid("finalState contains trailing bytes"));
    }
    u32::try_from(tree_size)
        .map_err(|_| CommitmentTreeFrontierValidationError::TreeSizeOutOfRange { tree_size })
}

/// Advances `remaining` past one optional node and returns whether the node
/// is present.
fn skip_framed_node(remaining: &mut &[u8]) -> Result<bool, CommitmentTreeFrontierValidationError> {
    let invalid = |reason| CommitmentTreeFrontierValidationError::InvalidEncoding { reason };
    let (&presence, rest) = remaining
        .split_first()
        .ok_or_else(|| invalid("finalState ends before a node presence flag"))?;
    let (present, rest) = match presence {
        0 => (false, rest),
        1 => (
            true,
            rest.get(NODE_BYTES..)
                .ok_or_else(|| invalid("finalState ends inside a node"))?,
        ),
        _ => return Err(invalid("node presence flag is neither 0 nor 1")),
    };
    *remaining = rest;
    Ok(present)
}

#[cfg(test)]
mod tests {
    use incrementalmerkletree::{
        Position,
        frontier::{CommitmentTree, Frontier},
    };
    use sapling::Node as SaplingNode;
    use zcash_primitives::merkle_tree::write_commitment_tree;
    use zinder_core::{BlockHash, BlockHeight, CommitmentTreeFrontiers, SUBTREE_LEAF_COUNT};

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    const DEPTH: u8 = 32;

    /// The canonical `finalState` encoding of a tree of `tree_size` leaves
    /// whose tip leaf and every ommer are the same node.
    fn canonical_encoding(tree_size: u64) -> TestResult<Vec<u8>> {
        let mut leaf = [0; 32];
        leaf[0] = 1;
        let node = Option::<SaplingNode>::from(SaplingNode::from_bytes(leaf))
            .ok_or("one is a canonical Sapling field element")?;
        let tree = match tree_size.checked_sub(1) {
            None => CommitmentTree::<SaplingNode, DEPTH>::empty(),
            Some(position) => {
                let position = Position::from(position);
                let ommers = vec![node; usize::from(position.past_ommer_count())];
                CommitmentTree::from_frontier(
                    &Frontier::<SaplingNode, DEPTH>::from_parts(position, node, ommers)
                        .map_err(|error| format!("frontier rejected: {error:?}"))?,
                )
            }
        };
        let mut encoded = Vec::new();
        write_commitment_tree(&tree, &mut encoded)?;
        Ok(encoded)
    }

    /// One encoded optional node.
    fn node(present: bool) -> Vec<u8> {
        if present {
            [&[1][..], &[7; NODE_BYTES][..]].concat()
        } else {
            vec![0]
        }
    }

    fn invalid(reason: &'static str) -> CommitmentTreeFrontierValidationError {
        CommitmentTreeFrontierValidationError::InvalidEncoding { reason }
    }

    #[test]
    fn framing_counts_the_tree_size_of_every_canonical_encoding() -> TestResult {
        let subtree = u64::from(SUBTREE_LEAF_COUNT);
        for tree_size in [0, 1, 2, 3, 6, 7, 8, subtree - 1, subtree, subtree + 5] {
            let encoded = canonical_encoding(tree_size)?;
            assert_eq!(
                framed_tree_size(&encoded).map(u64::from),
                Ok(tree_size),
                "tree of {tree_size} leaves"
            );
        }
        let largest = u64::from(u32::MAX);
        assert_eq!(
            framed_tree_size(&canonical_encoding(largest)?).map(u64::from),
            Ok(largest)
        );
        Ok(())
    }

    #[test]
    fn framing_rejects_an_encoding_the_canonical_encoder_cannot_write() {
        let nonempty_parents = usize::from(NONEMPTY_TREE_PARENT_SLOTS);
        for (encoded, rejection) in [
            (
                vec![],
                invalid("finalState ends before a node presence flag"),
            ),
            (
                vec![2, 0, 0],
                invalid("node presence flag is neither 0 nor 1"),
            ),
            (vec![1, 7, 7], invalid("finalState ends inside a node")),
            (
                node(true),
                invalid("finalState ends before a node presence flag"),
            ),
            (
                vec![0, 0],
                invalid("finalState ends before its parent count"),
            ),
            (
                [node(false), node(true), vec![0]].concat(),
                invalid("right leaf is present without a left leaf"),
            ),
            (vec![0, 0, 1, 0], invalid("empty tree has parent slots")),
            (
                [vec![0, 0, 1], node(true)].concat(),
                invalid("empty tree has parent slots"),
            ),
            (
                [node(true), node(false), vec![0]].concat(),
                invalid("nonempty tree does not have one parent slot per level above its leaves"),
            ),
            (
                [node(true), node(true), vec![30], node(false).repeat(30)].concat(),
                invalid("nonempty tree does not have one parent slot per level above its leaves"),
            ),
            (
                [node(true), node(false), vec![32], node(false).repeat(32)].concat(),
                invalid("nonempty tree does not have one parent slot per level above its leaves"),
            ),
            (
                [node(true), node(false), vec![0xfd, 31, 0]].concat(),
                invalid("nonempty tree does not have one parent slot per level above its leaves"),
            ),
            (
                [node(true), node(false), vec![31], node(false).repeat(30)].concat(),
                invalid("finalState ends before a node presence flag"),
            ),
            (
                vec![0, 0, 0, 0],
                invalid("finalState contains trailing bytes"),
            ),
            (
                [
                    node(true),
                    node(false),
                    vec![31],
                    node(false).repeat(nonempty_parents),
                    vec![0],
                ]
                .concat(),
                invalid("finalState contains trailing bytes"),
            ),
            (
                [
                    node(true),
                    node(true),
                    vec![31],
                    node(true).repeat(nonempty_parents),
                ]
                .concat(),
                CommitmentTreeFrontierValidationError::TreeSizeOutOfRange { tree_size: 1 << 32 },
            ),
        ] {
            assert_eq!(
                framed_tree_size(&encoded),
                Err(rejection),
                "framing of {encoded:?}"
            );
        }
    }

    #[test]
    fn a_validated_checkpoint_serves_its_bytes_and_tree_sizes() -> TestResult {
        let sapling = CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling);
        let checkpoint = CommitmentTreeCheckpoint::new(
            BlockId::new(BlockHeight::new(7), BlockHash::from_bytes([7; 32])),
            70,
            CommitmentTreeFrontiers::from_validated_parts(Some(sapling.clone()), None, None)?,
        );

        let served = ServedTreeState::from(&checkpoint);

        assert_eq!(served.block_id(), checkpoint.block_id);
        assert_eq!(served.block_time_seconds(), 70);
        assert_eq!(
            served.final_state_bytes(ShieldedProtocol::Sapling),
            Some(sapling.final_state_bytes())
        );
        assert_eq!(served.final_state_bytes(ShieldedProtocol::Orchard), None);
        assert_eq!(served.tip_metadata(), checkpoint.tip_metadata());
        let stored = ServedFrontier::from_stored_final_state(canonical_encoding(6)?)?;
        let stored_state = ServedTreeState::new(
            checkpoint.block_id,
            70,
            ServedFrontiers::new(None, None, Some(stored)),
        );
        assert_eq!(stored_state.tip_metadata(), ChainTipMetadata::new(0, 0, 6));
        Ok(())
    }
}
