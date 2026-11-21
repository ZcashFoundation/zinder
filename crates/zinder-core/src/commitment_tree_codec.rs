//! Durable commitment tree-state artifact values.

#[cfg(feature = "commitment-tree-codec")]
use std::{
    collections::VecDeque,
    io::{self, Cursor},
};

#[cfg(feature = "commitment-tree-codec")]
use incrementalmerkletree::{
    Hashable, Level, Position,
    frontier::{CommitmentTree, Frontier},
};
#[cfg(feature = "commitment-tree-codec")]
use orchard::tree::MerkleHashOrchard;
#[cfg(feature = "commitment-tree-codec")]
use sapling::Node as SaplingNode;
use thiserror::Error;
#[cfg(feature = "commitment-tree-codec")]
use zcash_primitives::merkle_tree::{HashSer, read_commitment_tree, write_commitment_tree};

use crate::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, BlockId, ChainTipMetadata,
    CompletedSubtreeRoot, FinalNoteCommitmentRoot, NetworkUpgradeActivations, SUBTREE_LEAF_COUNT,
    ShieldedProtocol, SubtreeRootHash, SubtreeRootIndex,
};

/// Maximum canonical Zebra `finalState` size admitted for one commitment tree.
pub const MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES: usize = 1_090;
#[cfg(feature = "commitment-tree-codec")]
const NOTE_COMMITMENT_TREE_DEPTH: u8 = 32;
/// Tree level of a subtree root, whose subtree holds [`SUBTREE_LEAF_COUNT`]
/// leaves.
#[cfg(feature = "commitment-tree-codec")]
const SUBTREE_ROOT_LEVEL: u8 = 16;
#[cfg(feature = "commitment-tree-codec")]
const _: () = assert!(1_u32 << SUBTREE_ROOT_LEVEL == SUBTREE_LEAF_COUNT);
/// Number of recently derived frontier roots kept for reuse.
///
/// Following one block validates the visible-tip frontier and the next
/// frontier of each of the three pools, so the bound covers that working set
/// with room for a replacement parent.
#[cfg(feature = "commitment-tree-codec")]
const FRONTIER_ROOT_MEMO_CAPACITY: usize = 16;

/// A validated note-commitment-tree frontier of one shielded pool.
///
/// `final_state_bytes` contains the canonical legacy `CommitmentTree`
/// encoding used by `z_gettreestate`. `final_root` preserves Zebra's RPC
/// display byte order, which is reversed for Sapling and direct for Orchard
/// and Ironwood.
///
/// The frontier keeps the pool it was validated for, because neither its
/// bytes nor its root identify that pool: every empty tree encodes as the
/// same bytes, and Orchard and Ironwood trees share a hash function.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CommitmentTreeFrontier {
    /// Shielded pool whose tree this frontier encodes.
    protocol: ShieldedProtocol,
    /// Number of note commitments represented by this frontier.
    tree_size: u32,
    /// Root computed from `final_state_bytes`, in Zebra RPC display order.
    final_root: FinalNoteCommitmentRoot,
    /// Canonical `z_gettreestate` `finalState` bytes.
    final_state_bytes: Box<[u8]>,
}

impl CommitmentTreeFrontier {
    /// Constructs the canonical empty frontier for an active shielded pool.
    #[cfg(feature = "commitment-tree-codec")]
    #[must_use]
    pub fn empty(protocol: ShieldedProtocol) -> Self {
        match protocol {
            ShieldedProtocol::Sapling => {
                empty_frontier::<SaplingNode>(protocol, sapling_root_bytes_in_rpc_order)
            }
            ShieldedProtocol::Orchard | ShieldedProtocol::Ironwood => {
                empty_frontier::<MerkleHashOrchard>(protocol, orchard_root_bytes_in_rpc_order)
            }
        }
    }

    /// Validates and constructs one canonical Zebra commitment-tree frontier.
    ///
    /// The official Zcash codecs must accept the legacy `finalState` bytes,
    /// their canonical re-encoding must be byte-identical, and the derived
    /// root must equal `final_root` in Zebra RPC display order.
    ///
    /// The encoding checks and the root comparison run on every call. The
    /// root derivation itself is reused from a small process-wide memo when
    /// the same protocol and exact bytes were derived recently, because the
    /// root is a pure function of them.
    #[cfg(feature = "commitment-tree-codec")]
    pub fn from_canonical_final_state(
        protocol: ShieldedProtocol,
        final_root: FinalNoteCommitmentRoot,
        final_state_bytes: impl Into<Box<[u8]>>,
    ) -> Result<Self, CommitmentTreeFrontierValidationError> {
        let frontier = Self::from_canonical_encoding(protocol, final_state_bytes.into())?;
        if frontier.final_root != final_root {
            return Err(CommitmentTreeFrontierValidationError::RootMismatch);
        }
        Ok(frontier)
    }

    /// Validates canonical `finalState` bytes and derives their root.
    #[cfg(feature = "commitment-tree-codec")]
    fn from_canonical_encoding(
        protocol: ShieldedProtocol,
        final_state_bytes: Box<[u8]>,
    ) -> Result<Self, CommitmentTreeFrontierValidationError> {
        match protocol {
            ShieldedProtocol::Sapling => canonical_frontier::<SaplingNode>(
                protocol,
                final_state_bytes,
                sapling_root_bytes_in_rpc_order,
            ),
            ShieldedProtocol::Orchard | ShieldedProtocol::Ironwood => {
                canonical_frontier::<MerkleHashOrchard>(
                    protocol,
                    final_state_bytes,
                    orchard_root_bytes_in_rpc_order,
                )
            }
        }
    }

    /// Returns the shielded pool whose tree this frontier encodes.
    #[must_use]
    pub const fn protocol(&self) -> ShieldedProtocol {
        self.protocol
    }

    /// Returns the number of note commitments represented by this frontier.
    #[must_use]
    pub const fn tree_size(&self) -> u32 {
        self.tree_size
    }

    /// Returns the validated root in Zebra RPC display order.
    #[must_use]
    pub const fn final_root(&self) -> FinalNoteCommitmentRoot {
        self.final_root
    }

    /// Returns the canonical `z_gettreestate` `finalState` bytes.
    #[must_use]
    pub fn final_state_bytes(&self) -> &[u8] {
        &self.final_state_bytes
    }

    /// Consumes the frontier and returns its `finalState` bytes without copying.
    #[must_use]
    pub fn into_final_state_bytes(self) -> Box<[u8]> {
        self.final_state_bytes
    }
}

#[cfg(feature = "commitment-tree-codec")]
fn sapling_root_bytes_in_rpc_order(root: &SaplingNode) -> [u8; 32] {
    let mut root_bytes = root.to_bytes();
    root_bytes.reverse();
    root_bytes
}

#[cfg(feature = "commitment-tree-codec")]
fn orchard_root_bytes_in_rpc_order(root: &MerkleHashOrchard) -> [u8; 32] {
    root.to_bytes()
}

#[cfg(feature = "commitment-tree-codec")]
fn empty_frontier<Node>(
    protocol: ShieldedProtocol,
    root_bytes_in_rpc_order: impl Fn(&Node) -> [u8; 32],
) -> CommitmentTreeFrontier
where
    Node: Clone + Hashable,
{
    let root = frontier_root(&Frontier::<Node, NOTE_COMMITMENT_TREE_DEPTH>::empty());
    CommitmentTreeFrontier {
        protocol,
        tree_size: 0,
        final_root: FinalNoteCommitmentRoot::from_bytes(root_bytes_in_rpc_order(&root)),
        final_state_bytes: Box::from([0_u8, 0, 0]),
    }
}

/// Invalid canonical Zebra commitment-tree frontier.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CommitmentTreeFrontierValidationError {
    /// The encoded frontier exceeds the version-1 admission bound.
    #[error("frontier is {byte_count} bytes; maximum is {max_byte_count}")]
    TooLarge {
        /// Observed `finalState` length.
        byte_count: usize,
        /// Maximum accepted `finalState` length.
        max_byte_count: usize,
    },
    /// The bytes are not one exact canonical legacy `CommitmentTree` encoding.
    #[error("invalid commitment-tree frontier encoding: {reason}")]
    InvalidEncoding {
        /// Stable validation reason.
        reason: &'static str,
    },
    /// The official frontier position cannot be represented by the v1 domain size.
    #[error("commitment-tree size {tree_size} does not fit u32")]
    TreeSizeOutOfRange {
        /// Size derived from the canonical frontier.
        tree_size: u64,
    },
    /// The canonical frontier derives a different Zebra-order root.
    #[error("commitment-tree frontier root does not match finalRoot")]
    RootMismatch,
}

#[cfg(feature = "commitment-tree-codec")]
fn canonical_frontier<Node>(
    protocol: ShieldedProtocol,
    final_state_bytes: Box<[u8]>,
    root_bytes_in_rpc_order: impl Fn(&Node) -> [u8; 32],
) -> Result<CommitmentTreeFrontier, CommitmentTreeFrontierValidationError>
where
    Node: Clone + Hashable + HashSer,
{
    let (frontier, tree_size) = decode_canonical_frontier(&final_state_bytes)?;
    let final_root = memoized_frontier_root(protocol, &final_state_bytes, || {
        root_bytes_in_rpc_order(&frontier_root(&frontier))
    });
    Ok(CommitmentTreeFrontier {
        protocol,
        tree_size,
        final_root,
        final_state_bytes,
    })
}

#[cfg(feature = "commitment-tree-codec")]
fn decode_canonical_frontier<Node>(
    final_state_bytes: &[u8],
) -> Result<(Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>, u32), CommitmentTreeFrontierValidationError>
where
    Node: Clone + Hashable + HashSer,
{
    if final_state_bytes.len() > MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES {
        return Err(CommitmentTreeFrontierValidationError::TooLarge {
            byte_count: final_state_bytes.len(),
            max_byte_count: MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES,
        });
    }
    let mut reader = Cursor::new(final_state_bytes);
    let legacy_tree = read_commitment_tree::<Node, _, NOTE_COMMITMENT_TREE_DEPTH>(&mut reader)
        .map_err(|_| CommitmentTreeFrontierValidationError::InvalidEncoding {
            reason: "finalState is not a valid legacy CommitmentTree encoding",
        })?;
    if reader.position() != u64::try_from(final_state_bytes.len()).unwrap_or(u64::MAX) {
        return Err(CommitmentTreeFrontierValidationError::InvalidEncoding {
            reason: "finalState contains trailing bytes",
        });
    }

    let frontier = frontier_from_legacy_tree(&legacy_tree)?;
    let tree_size = frontier.tree_size();
    let tree_size = u32::try_from(tree_size)
        .map_err(|_| CommitmentTreeFrontierValidationError::TreeSizeOutOfRange { tree_size })?;
    let canonical_tree = CommitmentTree::from_frontier(&frontier);
    let mut canonical_bytes = Vec::new();
    write_commitment_tree(&canonical_tree, &mut canonical_bytes).map_err(|_| {
        CommitmentTreeFrontierValidationError::InvalidEncoding {
            reason: "validated frontier could not be canonically encoded",
        }
    })?;
    if canonical_bytes.as_slice() != final_state_bytes {
        return Err(CommitmentTreeFrontierValidationError::InvalidEncoding {
            reason: "finalState is not the canonical Zebra RPC encoding",
        });
    }
    Ok((frontier, tree_size))
}

#[cfg(test)]
thread_local! {
    static ROOT_COMPUTATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Bytes the next accumulator snapshot on this thread encodes in place of
    /// its frontier, so a test can model a codec whose round trip changes the
    /// tree.
    static SNAPSHOT_ENCODING_OVERRIDE: std::cell::Cell<Option<Vec<u8>>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(feature = "commitment-tree-codec")]
fn frontier_root<Node>(frontier: &Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>) -> Node
where
    Node: Clone + Hashable,
{
    #[cfg(test)]
    ROOT_COMPUTATIONS.set(ROOT_COMPUTATIONS.get().saturating_add(1));
    frontier.root()
}

/// Returns the root of canonical `final_state_bytes`, deriving it only when
/// the memo does not hold it.
///
/// `derive_root` must compute the root of the frontier decoded from exactly
/// `final_state_bytes`, so every memo entry is that pure function's value.
#[cfg(feature = "commitment-tree-codec")]
fn memoized_frontier_root(
    protocol: ShieldedProtocol,
    final_state_bytes: &[u8],
    derive_root: impl FnOnce() -> [u8; 32],
) -> FinalNoteCommitmentRoot {
    if let Some(final_root) = with_frontier_root_memo(|memo| memo.get(protocol, final_state_bytes))
    {
        return final_root;
    }
    let final_root = FinalNoteCommitmentRoot::from_bytes(derive_root());
    with_frontier_root_memo(|memo| memo.insert(protocol, final_state_bytes, final_root));
    final_root
}

#[cfg(all(feature = "commitment-tree-codec", not(test)))]
fn with_frontier_root_memo<T>(access: impl FnOnce(&mut FrontierRootMemo) -> T) -> T {
    static FRONTIER_ROOT_MEMO: parking_lot::Mutex<FrontierRootMemo> =
        parking_lot::Mutex::new(FrontierRootMemo::new());
    access(&mut FRONTIER_ROOT_MEMO.lock())
}

// Unit tests run concurrently and count the roots their own thread derives,
// so each test thread keeps a private memo that other tests cannot evict.
#[cfg(all(feature = "commitment-tree-codec", test))]
fn with_frontier_root_memo<T>(access: impl FnOnce(&mut FrontierRootMemo) -> T) -> T {
    thread_local! {
        static FRONTIER_ROOT_MEMO: std::cell::RefCell<FrontierRootMemo> =
            const { std::cell::RefCell::new(FrontierRootMemo::new()) };
    }
    FRONTIER_ROOT_MEMO.with_borrow_mut(access)
}

/// Recently derived roots of canonical `finalState` encodings, least recently
/// used first.
///
/// Entries match on the protocol and the complete byte string. The memo only
/// replaces a root derivation; callers still compare the root they were given.
#[cfg(feature = "commitment-tree-codec")]
struct FrontierRootMemo {
    entries: VecDeque<FrontierRootMemoEntry>,
}

#[cfg(feature = "commitment-tree-codec")]
struct FrontierRootMemoEntry {
    protocol: ShieldedProtocol,
    final_state_bytes: Box<[u8]>,
    final_root: FinalNoteCommitmentRoot,
}

#[cfg(feature = "commitment-tree-codec")]
impl FrontierRootMemo {
    const fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    fn get(
        &mut self,
        protocol: ShieldedProtocol,
        final_state_bytes: &[u8],
    ) -> Option<FinalNoteCommitmentRoot> {
        let index = self.entries.iter().position(|entry| {
            entry.protocol == protocol && *entry.final_state_bytes == *final_state_bytes
        })?;
        let entry = self.entries.remove(index)?;
        let final_root = entry.final_root;
        self.entries.push_back(entry);
        Some(final_root)
    }

    fn insert(
        &mut self,
        protocol: ShieldedProtocol,
        final_state_bytes: &[u8],
        final_root: FinalNoteCommitmentRoot,
    ) {
        if self.get(protocol, final_state_bytes).is_some() {
            return;
        }
        if self.entries.len() == FRONTIER_ROOT_MEMO_CAPACITY {
            self.entries.pop_front();
        }
        self.entries.push_back(FrontierRootMemoEntry {
            protocol,
            final_state_bytes: Box::from(final_state_bytes),
            final_root,
        });
    }
}

#[cfg(feature = "commitment-tree-codec")]
fn frontier_from_legacy_tree<Node>(
    legacy_tree: &CommitmentTree<Node, NOTE_COMMITMENT_TREE_DEPTH>,
) -> Result<Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>, CommitmentTreeFrontierValidationError>
where
    Node: Clone,
{
    let (leaf, mut ommers, mut tree_size) = match (legacy_tree.left(), legacy_tree.right()) {
        (None, None) => {
            if legacy_tree.parents().iter().any(Option::is_some) {
                return Err(CommitmentTreeFrontierValidationError::InvalidEncoding {
                    reason: "empty tree contains parent nodes",
                });
            }
            return Ok(Frontier::empty());
        }
        (None, Some(_)) => {
            return Err(CommitmentTreeFrontierValidationError::InvalidEncoding {
                reason: "right leaf is present without a left leaf",
            });
        }
        (Some(left), None) => (left.clone(), Vec::new(), 1_u64),
        (Some(left), Some(right)) => (right.clone(), vec![left.clone()], 2_u64),
    };

    for (parent_index, parent) in legacy_tree.parents().iter().enumerate() {
        if let Some(parent) = parent {
            ommers.push(parent.clone());
            tree_size = tree_size.checked_add(1_u64 << (parent_index + 1)).ok_or(
                CommitmentTreeFrontierValidationError::InvalidEncoding {
                    reason: "legacy tree size overflowed u64",
                },
            )?;
        }
    }
    Frontier::from_parts(Position::from(tree_size - 1), leaf, ommers).map_err(|_| {
        CommitmentTreeFrontierValidationError::InvalidEncoding {
            reason: "legacy tree occupancy does not form a valid frontier",
        }
    })
}

/// Validated note-commitment-tree frontiers after one block.
///
/// A pool is absent only before its node-advertised activation height. Once a
/// pool is active, its frontier is present even when the tree is empty.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct CommitmentTreeFrontiers {
    /// Sapling frontier, or `None` before Sapling activation.
    sapling: Option<CommitmentTreeFrontier>,
    /// Orchard frontier, or `None` before NU5 activation.
    orchard: Option<CommitmentTreeFrontier>,
    /// Ironwood frontier, or `None` before NU6.3 activation.
    ironwood: Option<CommitmentTreeFrontier>,
}

impl CommitmentTreeFrontiers {
    /// Creates the frontiers associated with one block.
    ///
    /// Each frontier must have been validated for the pool whose position it
    /// takes, or this fails with [`CommitmentTreeFrontiersError::PoolMismatch`].
    /// Nothing that later checks the frontier's bytes or root can detect the
    /// misplacement, so another pool's tree state would otherwise be stored
    /// and served as this pool's.
    pub fn from_validated_parts(
        sapling: Option<CommitmentTreeFrontier>,
        orchard: Option<CommitmentTreeFrontier>,
        ironwood: Option<CommitmentTreeFrontier>,
    ) -> Result<Self, CommitmentTreeFrontiersError> {
        require_frontier_pool(ShieldedProtocol::Sapling, sapling.as_ref())?;
        require_frontier_pool(ShieldedProtocol::Orchard, orchard.as_ref())?;
        require_frontier_pool(ShieldedProtocol::Ironwood, ironwood.as_ref())?;
        Ok(Self {
            sapling,
            orchard,
            ironwood,
        })
    }

    /// Returns the frontier for `protocol`, if that pool is active.
    #[must_use]
    pub const fn get(&self, protocol: ShieldedProtocol) -> Option<&CommitmentTreeFrontier> {
        match protocol {
            ShieldedProtocol::Sapling => self.sapling.as_ref(),
            ShieldedProtocol::Orchard => self.orchard.as_ref(),
            ShieldedProtocol::Ironwood => self.ironwood.as_ref(),
        }
    }

    /// Returns the Sapling frontier, if Sapling is active.
    #[must_use]
    pub const fn sapling(&self) -> Option<&CommitmentTreeFrontier> {
        self.sapling.as_ref()
    }

    /// Returns the Orchard frontier, if Orchard is active.
    #[must_use]
    pub const fn orchard(&self) -> Option<&CommitmentTreeFrontier> {
        self.orchard.as_ref()
    }

    /// Returns the Ironwood frontier, if Ironwood is active.
    #[must_use]
    pub const fn ironwood(&self) -> Option<&CommitmentTreeFrontier> {
        self.ironwood.as_ref()
    }

    /// Consumes the frontiers and returns the Sapling, Orchard, and Ironwood
    /// frontiers in that order.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Option<CommitmentTreeFrontier>,
        Option<CommitmentTreeFrontier>,
        Option<CommitmentTreeFrontier>,
    ) {
        (self.sapling, self.orchard, self.ironwood)
    }

    /// Derives chain-tip metadata from the admitted frontier sizes.
    #[must_use]
    pub fn tip_metadata(&self) -> ChainTipMetadata {
        ChainTipMetadata::new(
            self.sapling
                .as_ref()
                .map_or(0, CommitmentTreeFrontier::tree_size),
            self.orchard
                .as_ref()
                .map_or(0, CommitmentTreeFrontier::tree_size),
            self.ironwood
                .as_ref()
                .map_or(0, CommitmentTreeFrontier::tree_size),
        )
    }

    /// Derives the final roots associated with `block_id`.
    #[must_use]
    pub fn final_note_commitment_roots(&self, block_id: BlockId) -> BlockFinalNoteCommitmentRoots {
        BlockFinalNoteCommitmentRoots::new(
            block_id.height,
            block_id.hash,
            self.sapling
                .as_ref()
                .map(CommitmentTreeFrontier::final_root),
            self.orchard
                .as_ref()
                .map(CommitmentTreeFrontier::final_root),
            self.ironwood
                .as_ref()
                .map(CommitmentTreeFrontier::final_root),
        )
    }
}

/// Invalid assembly of one block's commitment-tree frontiers.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CommitmentTreeFrontiersError {
    /// A frontier validated for one pool was given for another pool.
    #[error("{frontier_protocol:?} commitment-tree frontier cannot be the {pool:?} frontier")]
    PoolMismatch {
        /// Pool whose frontier was being set.
        pool: ShieldedProtocol,
        /// Pool the frontier was validated for.
        frontier_protocol: ShieldedProtocol,
    },
}

fn require_frontier_pool(
    pool: ShieldedProtocol,
    frontier: Option<&CommitmentTreeFrontier>,
) -> Result<(), CommitmentTreeFrontiersError> {
    match frontier {
        Some(frontier) if frontier.protocol != pool => {
            Err(CommitmentTreeFrontiersError::PoolMismatch {
                pool,
                frontier_protocol: frontier.protocol,
            })
        }
        Some(_) | None => Ok(()),
    }
}

/// Exact block and commitment-tree state used to resume canonical history.
///
/// The block time is retained with the validated frontiers so a checkpoint can
/// serve the complete typed tree-state contract without consulting its source
/// node again.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CommitmentTreeCheckpoint {
    /// Exact canonical block at which these frontiers were observed.
    pub block_id: BlockId,
    /// Checkpoint block timestamp in Unix seconds.
    pub block_time_seconds: u32,
    /// Validated frontiers after applying `block_id`.
    pub frontiers: CommitmentTreeFrontiers,
}

impl CommitmentTreeCheckpoint {
    /// Creates one typed commitment-tree checkpoint.
    #[must_use]
    pub const fn new(
        block_id: BlockId,
        block_time_seconds: u32,
        frontiers: CommitmentTreeFrontiers,
    ) -> Self {
        Self {
            block_id,
            block_time_seconds,
            frontiers,
        }
    }

    /// Derives the commitment-tree sizes represented by this checkpoint.
    #[must_use]
    pub fn tip_metadata(&self) -> ChainTipMetadata {
        self.frontiers.tip_metadata()
    }
}

/// In-memory note-commitment-tree state used while applying canonical blocks.
///
/// The accumulator is initialized from one validated predecessor checkpoint,
/// retains only the official incremental frontiers, and never persists an
/// alternate tree representation. Pool presence is derived from the immutable
/// node-advertised activation table captured at construction.
#[derive(Clone, Debug)]
pub struct CommitmentTreeAccumulator {
    tip_height: BlockHeight,
    activations: NetworkUpgradeActivations,
    sapling: Option<PoolAccumulator<SaplingNode>>,
    orchard: Option<PoolAccumulator<MerkleHashOrchard>>,
    ironwood: Option<PoolAccumulator<MerkleHashOrchard>>,
    /// Subtrees the most recently appended block completed, in completion
    /// order.
    completed_subtree_roots: Vec<CompletedSubtreeRoot>,
}

#[derive(Clone, Debug)]
struct PoolAccumulator<Node> {
    frontier: Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
    tree_size: u32,
    /// Root of `frontier` in Zebra RPC display order, or `None` until it is
    /// derived after the pool activates or receives commitments.
    ///
    /// It comes from the validated seed checkpoint or from `frontier` itself,
    /// never from a snapshot's encoding, so a snapshot can be checked against
    /// it.
    root: Option<FinalNoteCommitmentRoot>,
}

impl CommitmentTreeAccumulator {
    /// Seeds a transient accumulator from the validated frontiers after
    /// `tip_height`.
    ///
    /// Active pools must have a frontier, including an empty frontier at their
    /// activation height. Inactive pools must not have one. A mismatch is
    /// rejected instead of silently inventing or discarding tree state.
    pub fn from_validated_frontiers(
        tip_height: BlockHeight,
        frontiers: &CommitmentTreeFrontiers,
        activations: &NetworkUpgradeActivations,
    ) -> Result<Self, CommitmentTreeAccumulatorError> {
        validate_pool_activation_frontier(
            ShieldedProtocol::Sapling,
            tip_height,
            frontiers.sapling(),
            activations,
        )?;
        validate_pool_activation_frontier(
            ShieldedProtocol::Orchard,
            tip_height,
            frontiers.orchard(),
            activations,
        )?;
        validate_pool_activation_frontier(
            ShieldedProtocol::Ironwood,
            tip_height,
            frontiers.ironwood(),
            activations,
        )?;

        Ok(Self {
            tip_height,
            activations: activations.clone(),
            sapling: frontiers
                .sapling()
                .map(|frontier| seed_pool(ShieldedProtocol::Sapling, frontier))
                .transpose()?,
            orchard: frontiers
                .orchard()
                .map(|frontier| seed_pool(ShieldedProtocol::Orchard, frontier))
                .transpose()?,
            ironwood: frontiers
                .ironwood()
                .map(|frontier| seed_pool(ShieldedProtocol::Ironwood, frontier))
                .transpose()?,
            completed_subtree_roots: Vec::new(),
        })
    }

    /// Returns the height of the latest applied canonical block.
    #[must_use]
    pub const fn tip_height(&self) -> BlockHeight {
        self.tip_height
    }

    /// Returns the current commitment-tree sizes without serializing frontiers.
    #[must_use]
    pub fn tip_metadata(&self) -> ChainTipMetadata {
        ChainTipMetadata::new(
            self.sapling.as_ref().map_or(0, |pool| pool.tree_size),
            self.orchard.as_ref().map_or(0, |pool| pool.tree_size),
            self.ironwood.as_ref().map_or(0, |pool| pool.tree_size),
        )
    }

    /// Applies one contiguous canonical block's note commitments in wire order.
    ///
    /// The three pool updates are atomic: malformed encodings, activation
    /// violations, or capacity failures leave this accumulator unchanged.
    pub fn append_block_commitments(
        &mut self,
        block_height: BlockHeight,
        sapling_cmus: &[[u8; 32]],
        orchard_cmxs: &[[u8; 32]],
        ironwood_cmxs: &[[u8; 32]],
    ) -> Result<(), CommitmentTreeAccumulatorError> {
        let expected_height =
            self.tip_height
                .next()
                .ok_or(CommitmentTreeAccumulatorError::BlockHeightExhausted {
                    tip_height: self.tip_height,
                })?;
        if block_height != expected_height {
            return Err(CommitmentTreeAccumulatorError::NonContiguousBlockHeight {
                expected_height,
                block_height,
            });
        }

        let sapling_update = prepare_pool_update(
            self.sapling.as_ref(),
            PoolUpdateContext::new(ShieldedProtocol::Sapling, block_height, &self.activations),
            sapling_cmus,
            |bytes| Option::<SaplingNode>::from(SaplingNode::from_bytes(bytes)),
        )?;
        let orchard_update = prepare_pool_update(
            self.orchard.as_ref(),
            PoolUpdateContext::new(ShieldedProtocol::Orchard, block_height, &self.activations),
            orchard_cmxs,
            |bytes| Option::<MerkleHashOrchard>::from(MerkleHashOrchard::from_bytes(&bytes)),
        )?;
        let ironwood_update = prepare_pool_update(
            self.ironwood.as_ref(),
            PoolUpdateContext::new(ShieldedProtocol::Ironwood, block_height, &self.activations),
            ironwood_cmxs,
            |bytes| Option::<MerkleHashOrchard>::from(MerkleHashOrchard::from_bytes(&bytes)),
        )?;

        let mut completed_subtree_roots = Vec::new();
        apply_pool_update(
            &mut self.sapling,
            sapling_update,
            SaplingNode::to_bytes,
            &mut completed_subtree_roots,
        );
        apply_pool_update(
            &mut self.orchard,
            orchard_update,
            MerkleHashOrchard::to_bytes,
            &mut completed_subtree_roots,
        );
        apply_pool_update(
            &mut self.ironwood,
            ironwood_update,
            MerkleHashOrchard::to_bytes,
            &mut completed_subtree_roots,
        );
        self.completed_subtree_roots = completed_subtree_roots;
        self.tip_height = block_height;
        Ok(())
    }

    /// Returns the roots of the subtrees that the most recently appended block
    /// completed, at that block's height.
    ///
    /// A root is taken when the commitment that fills its subtree's last leaf
    /// is appended, so a subtree that a block completes partway through its
    /// commitments is included. Roots are in the order the block completed
    /// them, and each is in the byte order that `z_getsubtreesbyindex`
    /// reports, which for Sapling is not the reversed order of `finalRoot`.
    /// An accumulator that has not appended a block since it was seeded
    /// returns none, and a rejected append keeps the previous block's roots.
    #[must_use]
    pub fn completed_subtree_roots(&self) -> &[CompletedSubtreeRoot] {
        &self.completed_subtree_roots
    }

    /// Encodes the active pool's Zebra `finalState` for a wallet read, or
    /// returns `None` before the pool activates. Checks that the canonical
    /// bytes decode to the accumulated frontier without deriving a root.
    pub fn final_state_bytes(
        &self,
        protocol: ShieldedProtocol,
    ) -> Result<Option<Box<[u8]>>, CommitmentTreeAccumulatorError> {
        match protocol {
            ShieldedProtocol::Sapling => self
                .sapling
                .as_ref()
                .map(|pool| served_pool_bytes(protocol, pool)),
            ShieldedProtocol::Orchard => self
                .orchard
                .as_ref()
                .map(|pool| served_pool_bytes(protocol, pool)),
            ShieldedProtocol::Ironwood => self
                .ironwood
                .as_ref()
                .map(|pool| served_pool_bytes(protocol, pool)),
        }
        .transpose()
    }

    /// Produces canonical, revalidated Zebra `finalState` checkpoints.
    ///
    /// Each pool's frontier is encoded with the official codec and validated
    /// again, and its root is derived from those canonical bytes. That root is
    /// compared with the in-memory frontier's root only when the accumulator
    /// already holds it, as it does for a pool that received no commitments
    /// since seeding. Use this when the caller compares the result with the
    /// node's tree state, as live following does, and
    /// [`Self::cross_checked_frontiers`] when nothing else checks it.
    pub fn validated_frontiers(
        &mut self,
    ) -> Result<CommitmentTreeFrontiers, CommitmentTreeAccumulatorError> {
        // Each snapshot is encoded and validated for the pool it fills.
        Ok(CommitmentTreeFrontiers {
            sapling: self
                .sapling
                .as_ref()
                .map(|pool| {
                    snapshot_pool(
                        ShieldedProtocol::Sapling,
                        pool,
                        sapling_root_bytes_in_rpc_order,
                    )
                })
                .transpose()?,
            orchard: self
                .orchard
                .as_ref()
                .map(|pool| {
                    snapshot_pool(
                        ShieldedProtocol::Orchard,
                        pool,
                        orchard_root_bytes_in_rpc_order,
                    )
                })
                .transpose()?,
            ironwood: self
                .ironwood
                .as_ref()
                .map(|pool| {
                    snapshot_pool(
                        ShieldedProtocol::Ironwood,
                        pool,
                        orchard_root_bytes_in_rpc_order,
                    )
                })
                .transpose()?,
        })
    }

    /// Produces the checkpoints of [`Self::validated_frontiers`] after
    /// requiring each pool's canonical encoding to have the same root as its
    /// in-memory frontier.
    ///
    /// The comparison rejects an encoding that does not round-trip to the
    /// appended tree. Use it when no node tree state is compared with the
    /// result, as for the checkpoints that canonical construction persists. It
    /// costs one more root derivation for each pool that received commitments
    /// since its root was last derived.
    ///
    /// A mismatch returns
    /// [`CommitmentTreeAccumulatorError::FrontierSnapshotValidationFailed`]
    /// with [`CommitmentTreeFrontierValidationError::RootMismatch`].
    pub fn cross_checked_frontiers(
        &mut self,
    ) -> Result<CommitmentTreeFrontiers, CommitmentTreeAccumulatorError> {
        if let Some(pool) = self.sapling.as_mut() {
            current_pool_root(pool, sapling_root_bytes_in_rpc_order);
        }
        if let Some(pool) = self.orchard.as_mut() {
            current_pool_root(pool, orchard_root_bytes_in_rpc_order);
        }
        if let Some(pool) = self.ironwood.as_mut() {
            current_pool_root(pool, orchard_root_bytes_in_rpc_order);
        }
        self.validated_frontiers()
    }

    /// Produces final roots for the current accumulator tip.
    #[must_use]
    pub fn final_note_commitment_roots(
        &mut self,
        block_hash: BlockHash,
    ) -> BlockFinalNoteCommitmentRoots {
        BlockFinalNoteCommitmentRoots::new(
            self.tip_height,
            block_hash,
            self.sapling
                .as_mut()
                .map(|pool| current_pool_root(pool, sapling_root_bytes_in_rpc_order)),
            self.orchard
                .as_mut()
                .map(|pool| current_pool_root(pool, orchard_root_bytes_in_rpc_order)),
            self.ironwood
                .as_mut()
                .map(|pool| current_pool_root(pool, orchard_root_bytes_in_rpc_order)),
        )
    }
}

/// Failure to seed, advance, or checkpoint a commitment-tree accumulator.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CommitmentTreeAccumulatorError {
    /// The predecessor checkpoint disagrees with the advertised activation table.
    #[error(
        "{protocol:?} pool activation/frontier mismatch at height {height:?}: active={pool_active}, frontier_present={frontier_present}"
    )]
    PoolActivationFrontierMismatch {
        /// Shielded pool with inconsistent state.
        protocol: ShieldedProtocol,
        /// Height at which the mismatch was observed.
        height: BlockHeight,
        /// Whether the activation table marks the pool active.
        pool_active: bool,
        /// Whether a frontier is present.
        frontier_present: bool,
    },
    /// The validated checkpoint could not be decoded into its official frontier.
    #[error("validated {protocol:?} frontier could not seed the accumulator")]
    ValidatedFrontierDecodeFailed {
        /// Shielded pool whose checkpoint failed to decode.
        protocol: ShieldedProtocol,
    },
    /// The validated checkpoint's recorded and decoded sizes disagree.
    #[error(
        "validated {protocol:?} frontier records size {recorded_tree_size}, but decodes to {decoded_tree_size}"
    )]
    ValidatedFrontierSizeMismatch {
        /// Shielded pool whose sizes disagree.
        protocol: ShieldedProtocol,
        /// Size stored in the validated checkpoint.
        recorded_tree_size: u32,
        /// Size reported by the official frontier.
        decoded_tree_size: u64,
    },
    /// The caller attempted to advance beyond the block-height domain.
    #[error("commitment-tree accumulator cannot advance beyond height {tip_height:?}")]
    BlockHeightExhausted {
        /// Current maximum-height tip.
        tip_height: BlockHeight,
    },
    /// Blocks must be applied exactly once and in ascending height order.
    #[error("expected commitment-tree block height {expected_height:?}, received {block_height:?}")]
    NonContiguousBlockHeight {
        /// Only height accepted for the next update.
        expected_height: BlockHeight,
        /// Height supplied by the caller.
        block_height: BlockHeight,
    },
    /// A block contains commitments for a pool that is not active yet.
    #[error(
        "block {block_height:?} contains {commitment_count} {protocol:?} commitments before activation"
    )]
    UnexpectedCommitmentsBeforeActivation {
        /// Inactive shielded pool.
        protocol: ShieldedProtocol,
        /// Block containing the unexpected commitments.
        block_height: BlockHeight,
        /// Number of unexpected commitments.
        commitment_count: usize,
    },
    /// One canonical commitment is not a valid field encoding for its pool.
    #[error(
        "block {block_height:?} {protocol:?} commitment {commitment_index} has a malformed encoding"
    )]
    MalformedCommitmentEncoding {
        /// Shielded pool whose commitment is malformed.
        protocol: ShieldedProtocol,
        /// Block containing the malformed commitment.
        block_height: BlockHeight,
        /// Zero-based commitment index within the block and pool.
        commitment_index: usize,
    },
    /// Appending the block would exceed the version-1 tree-size domain.
    #[error(
        "block {block_height:?} cannot append {commitment_count} {protocol:?} commitments to tree size {tree_size}"
    )]
    TreeFull {
        /// Shielded pool at capacity.
        protocol: ShieldedProtocol,
        /// Block that would exceed capacity.
        block_height: BlockHeight,
        /// Tree size before this block.
        tree_size: u32,
        /// Number of commitments requested by this block.
        commitment_count: usize,
    },
    /// An in-memory frontier could not be written with the official codec.
    #[error("failed to encode {protocol:?} commitment-tree frontier")]
    FrontierSnapshotEncodingFailed {
        /// Shielded pool whose frontier failed to encode.
        protocol: ShieldedProtocol,
        /// Official codec failure.
        #[source]
        source: io::Error,
    },
    /// A freshly encoded snapshot failed canonical frontier validation.
    #[error("encoded {protocol:?} commitment-tree frontier failed validation")]
    FrontierSnapshotValidationFailed {
        /// Shielded pool whose snapshot failed validation.
        protocol: ShieldedProtocol,
        /// Canonical validation failure.
        #[source]
        source: CommitmentTreeFrontierValidationError,
    },
}

fn pool_is_active(
    protocol: ShieldedProtocol,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> bool {
    activations
        .activation_height_by_name(protocol.activation_upgrade_name())
        .is_some_and(|activation_height| activation_height <= height)
}

fn validate_pool_activation_frontier(
    protocol: ShieldedProtocol,
    height: BlockHeight,
    frontier: Option<&CommitmentTreeFrontier>,
    activations: &NetworkUpgradeActivations,
) -> Result<(), CommitmentTreeAccumulatorError> {
    let pool_active = pool_is_active(protocol, height, activations);
    let frontier_present = frontier.is_some();
    if pool_active != frontier_present {
        return Err(
            CommitmentTreeAccumulatorError::PoolActivationFrontierMismatch {
                protocol,
                height,
                pool_active,
                frontier_present,
            },
        );
    }
    Ok(())
}

fn seed_pool<Node>(
    protocol: ShieldedProtocol,
    validated_frontier: &CommitmentTreeFrontier,
) -> Result<PoolAccumulator<Node>, CommitmentTreeAccumulatorError>
where
    Node: Clone + Hashable + HashSer,
{
    let mut reader = Cursor::new(validated_frontier.final_state_bytes());
    let legacy_tree = read_commitment_tree::<Node, _, NOTE_COMMITMENT_TREE_DEPTH>(&mut reader)
        .map_err(|_| CommitmentTreeAccumulatorError::ValidatedFrontierDecodeFailed { protocol })?;
    let frontier = frontier_from_legacy_tree(&legacy_tree)
        .map_err(|_| CommitmentTreeAccumulatorError::ValidatedFrontierDecodeFailed { protocol })?;
    if frontier.tree_size() != u64::from(validated_frontier.tree_size()) {
        return Err(
            CommitmentTreeAccumulatorError::ValidatedFrontierSizeMismatch {
                protocol,
                recorded_tree_size: validated_frontier.tree_size(),
                decoded_tree_size: frontier.tree_size(),
            },
        );
    }
    Ok(PoolAccumulator {
        frontier,
        tree_size: validated_frontier.tree_size(),
        root: Some(validated_frontier.final_root()),
    })
}

struct PreparedPoolUpdate<Node> {
    protocol: ShieldedProtocol,
    block_height: BlockHeight,
    pool_active: bool,
    commitments: Vec<Node>,
    next_tree_size: u32,
}

#[derive(Clone, Copy)]
struct PoolUpdateContext {
    protocol: ShieldedProtocol,
    block_height: BlockHeight,
    pool_active: bool,
}

impl PoolUpdateContext {
    fn new(
        protocol: ShieldedProtocol,
        block_height: BlockHeight,
        activations: &NetworkUpgradeActivations,
    ) -> Self {
        Self {
            protocol,
            block_height,
            pool_active: pool_is_active(protocol, block_height, activations),
        }
    }
}

fn prepare_pool_update<Node>(
    current: Option<&PoolAccumulator<Node>>,
    context: PoolUpdateContext,
    commitment_bytes: &[[u8; 32]],
    decode_commitment: impl Fn([u8; 32]) -> Option<Node>,
) -> Result<PreparedPoolUpdate<Node>, CommitmentTreeAccumulatorError> {
    let PoolUpdateContext {
        protocol,
        block_height,
        pool_active,
    } = context;
    if !pool_active {
        if !commitment_bytes.is_empty() {
            return Err(
                CommitmentTreeAccumulatorError::UnexpectedCommitmentsBeforeActivation {
                    protocol,
                    block_height,
                    commitment_count: commitment_bytes.len(),
                },
            );
        }
        if current.is_some() {
            return Err(
                CommitmentTreeAccumulatorError::PoolActivationFrontierMismatch {
                    protocol,
                    height: block_height,
                    pool_active,
                    frontier_present: true,
                },
            );
        }
        return Ok(PreparedPoolUpdate {
            protocol,
            block_height,
            pool_active,
            commitments: Vec::new(),
            next_tree_size: 0,
        });
    }

    let tree_size = current.map_or(0, |pool| pool.tree_size);
    let commitment_count = u32::try_from(commitment_bytes.len()).map_err(|_| {
        CommitmentTreeAccumulatorError::TreeFull {
            protocol,
            block_height,
            tree_size,
            commitment_count: commitment_bytes.len(),
        }
    })?;
    let next_tree_size = tree_size.checked_add(commitment_count).ok_or(
        CommitmentTreeAccumulatorError::TreeFull {
            protocol,
            block_height,
            tree_size,
            commitment_count: commitment_bytes.len(),
        },
    )?;

    let mut commitments = Vec::with_capacity(commitment_bytes.len());
    for (commitment_index, encoded_commitment) in commitment_bytes.iter().copied().enumerate() {
        let commitment = decode_commitment(encoded_commitment).ok_or(
            CommitmentTreeAccumulatorError::MalformedCommitmentEncoding {
                protocol,
                block_height,
                commitment_index,
            },
        )?;
        commitments.push(commitment);
    }
    Ok(PreparedPoolUpdate {
        protocol,
        block_height,
        pool_active,
        commitments,
        next_tree_size,
    })
}

/// Appends a prepared pool update and adds each subtree it completes to
/// `completed_subtree_roots`, encoding roots with `subtree_root_bytes`.
fn apply_pool_update<Node>(
    current: &mut Option<PoolAccumulator<Node>>,
    update: PreparedPoolUpdate<Node>,
    subtree_root_bytes: impl Fn(&Node) -> [u8; 32],
    completed_subtree_roots: &mut Vec<CompletedSubtreeRoot>,
) where
    Node: Clone + Hashable,
{
    if !update.pool_active {
        return;
    }
    let pool = current.get_or_insert_with(|| PoolAccumulator {
        frontier: Frontier::empty(),
        tree_size: 0,
        root: None,
    });
    let has_commitments = !update.commitments.is_empty();
    for commitment in update.commitments {
        // Capacity was checked for all pools before any mutation. A version-1
        // size is strictly below the official depth-32 frontier's 2^32 limit.
        let _appended_within_prevalidated_capacity = pool.frontier.append(commitment);
        if let Some((subtree_index, root)) = completed_subtree_root(&pool.frontier) {
            completed_subtree_roots.push(CompletedSubtreeRoot {
                protocol: update.protocol,
                subtree_index,
                root_hash: SubtreeRootHash::from_bytes(subtree_root_bytes(&root)),
                completing_block_height: update.block_height,
            });
        }
    }
    if has_commitments {
        pool.root = None;
    }
    pool.tree_size = update.next_tree_size;
}

/// Returns the index and root of the subtree whose last leaf `frontier` holds
/// at its tip, or `None` when its tip leaf completes no subtree.
///
/// The frontier still holds the whole subtree's path only right after that
/// leaf is appended, so this runs after each commitment.
fn completed_subtree_root<Node>(
    frontier: &Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
) -> Option<(SubtreeRootIndex, Node)>
where
    Node: Clone + Hashable,
{
    let frontier = frontier.value()?;
    let level = Level::from(SUBTREE_ROOT_LEVEL);
    let position = frontier.position();
    if !position.is_complete_subtree(level) {
        return None;
    }
    // A depth-32 position shifted down by 16 levels always fits.
    let subtree_index = u32::try_from(u64::from(position) >> SUBTREE_ROOT_LEVEL).ok()?;
    Some((
        SubtreeRootIndex::new(subtree_index),
        frontier.root(Some(level)),
    ))
}

fn current_pool_root<Node>(
    pool: &mut PoolAccumulator<Node>,
    root_bytes_in_rpc_order: impl Fn(&Node) -> [u8; 32],
) -> FinalNoteCommitmentRoot
where
    Node: Clone + Hashable,
{
    *pool.root.get_or_insert_with(|| {
        FinalNoteCommitmentRoot::from_bytes(root_bytes_in_rpc_order(&frontier_root(&pool.frontier)))
    })
}

fn encode_pool<Node>(
    protocol: ShieldedProtocol,
    pool: &PoolAccumulator<Node>,
) -> Result<Box<[u8]>, CommitmentTreeAccumulatorError>
where
    Node: Clone + Hashable + HashSer,
{
    let legacy_tree = CommitmentTree::from_frontier(&pool.frontier);
    let mut final_state_bytes = Vec::new();
    write_commitment_tree(&legacy_tree, &mut final_state_bytes).map_err(|source| {
        CommitmentTreeAccumulatorError::FrontierSnapshotEncodingFailed { protocol, source }
    })?;
    #[cfg(test)]
    let final_state_bytes = SNAPSHOT_ENCODING_OVERRIDE
        .take()
        .unwrap_or(final_state_bytes);
    Ok(final_state_bytes.into_boxed_slice())
}

fn served_pool_bytes<Node>(
    protocol: ShieldedProtocol,
    pool: &PoolAccumulator<Node>,
) -> Result<Box<[u8]>, CommitmentTreeAccumulatorError>
where
    Node: Clone + Hashable + HashSer + PartialEq,
{
    let bytes = encode_pool(protocol, pool)?;
    let (decoded, tree_size) = decode_canonical_frontier(&bytes).map_err(|source| {
        CommitmentTreeAccumulatorError::FrontierSnapshotValidationFailed { protocol, source }
    })?;
    if decoded != pool.frontier || tree_size != pool.tree_size {
        return Err(
            CommitmentTreeAccumulatorError::FrontierSnapshotValidationFailed {
                protocol,
                source: CommitmentTreeFrontierValidationError::InvalidEncoding {
                    reason: "encoded frontier differs from the accumulated tree",
                },
            },
        );
    }
    Ok(bytes)
}

fn snapshot_pool<Node>(
    protocol: ShieldedProtocol,
    pool: &PoolAccumulator<Node>,
    root_bytes_in_rpc_order: impl Fn(&Node) -> [u8; 32],
) -> Result<CommitmentTreeFrontier, CommitmentTreeAccumulatorError>
where
    Node: Clone + Hashable + HashSer,
{
    let final_state_bytes = encode_pool(protocol, pool)?;
    let validation_failed = |source| {
        CommitmentTreeAccumulatorError::FrontierSnapshotValidationFailed { protocol, source }
    };
    let snapshot = canonical_frontier(protocol, final_state_bytes, root_bytes_in_rpc_order)
        .map_err(validation_failed)?;
    // A root already derived from the in-memory frontier must agree with the
    // root of its canonical encoding.
    if pool.root.is_some_and(|root| root != snapshot.final_root()) {
        return Err(validation_failed(
            CommitmentTreeFrontierValidationError::RootMismatch,
        ));
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConsensusBranchId, Network, NetworkUpgradeActivation, NetworkUpgradeActivationsError,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn commitment(value: u8) -> [u8; 32] {
        let mut bytes = [0; 32];
        bytes[0] = value;
        bytes
    }

    fn pool_activations(
        sapling_height: u32,
        orchard_height: u32,
        ironwood_height: u32,
    ) -> Result<NetworkUpgradeActivations, NetworkUpgradeActivationsError> {
        NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(1),
                    activation_height: BlockHeight::new(sapling_height),
                    name: "Sapling".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(2),
                    activation_height: BlockHeight::new(orchard_height),
                    name: "NU5".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(3),
                    activation_height: BlockHeight::new(ironwood_height),
                    name: "NU6.3".to_owned(),
                },
            ],
        )
    }

    fn full_version_one_sapling_frontier() -> TestResultWith<CommitmentTreeFrontier> {
        let leaf = Option::<SaplingNode>::from(SaplingNode::from_bytes(commitment(1)))
            .ok_or("one is a canonical Sapling field element")?;
        let legacy_tree = CommitmentTree::<SaplingNode, NOTE_COMMITMENT_TREE_DEPTH>::from_parts(
            Some(leaf),
            None,
            vec![Some(leaf); 31],
        )
        .map_err(|()| "depth-32 tree must accept 31 parent slots")?;
        let frontier = frontier_from_legacy_tree(&legacy_tree)?;
        let mut final_state_bytes = Vec::new();
        write_commitment_tree(&legacy_tree, &mut final_state_bytes)?;
        Ok(CommitmentTreeFrontier::from_canonical_final_state(
            ShieldedProtocol::Sapling,
            FinalNoteCommitmentRoot::from_bytes(sapling_root_bytes_in_rpc_order(&frontier.root())),
            final_state_bytes,
        )?)
    }

    type TestResultWith<T> = Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn active_empty_frontiers_round_trip_through_official_codecs() {
        for protocol in [
            ShieldedProtocol::Sapling,
            ShieldedProtocol::Orchard,
            ShieldedProtocol::Ironwood,
        ] {
            let frontier = CommitmentTreeFrontier::empty(protocol);
            let decoded = CommitmentTreeFrontier::from_canonical_final_state(
                protocol,
                frontier.final_root(),
                frontier.final_state_bytes(),
            );
            assert_eq!(decoded, Ok(frontier));
        }
    }

    /// Neither the bytes nor the root of a frontier identify its pool, so a
    /// frontier is kept out of another pool's position by the pool it was
    /// validated for.
    #[test]
    fn frontiers_reject_a_frontier_validated_for_another_pool() {
        let pools = [
            ShieldedProtocol::Sapling,
            ShieldedProtocol::Orchard,
            ShieldedProtocol::Ironwood,
        ];
        let [sapling, orchard, ironwood] = pools.map(CommitmentTreeFrontier::empty);
        assert_eq!(sapling.final_state_bytes(), orchard.final_state_bytes());
        assert_ne!(sapling.final_root(), orchard.final_root());
        assert_eq!(orchard.final_state_bytes(), ironwood.final_state_bytes());
        assert_eq!(orchard.final_root(), ironwood.final_root());

        for pool in pools {
            for frontier_protocol in pools {
                let frontier = |position| {
                    Some(CommitmentTreeFrontier::empty(if position == pool {
                        frontier_protocol
                    } else {
                        position
                    }))
                };
                let frontiers = CommitmentTreeFrontiers::from_validated_parts(
                    frontier(ShieldedProtocol::Sapling),
                    frontier(ShieldedProtocol::Orchard),
                    frontier(ShieldedProtocol::Ironwood),
                );
                if pool == frontier_protocol {
                    let protocols = frontiers.map(|frontiers| {
                        pools.map(|position| {
                            frontiers
                                .get(position)
                                .map(CommitmentTreeFrontier::protocol)
                        })
                    });
                    assert_eq!(protocols, Ok(pools.map(Some)));
                } else {
                    assert_eq!(
                        frontiers,
                        Err(CommitmentTreeFrontiersError::PoolMismatch {
                            pool,
                            frontier_protocol,
                        }),
                        "{frontier_protocol:?} frontier in the {pool:?} position"
                    );
                }
            }
        }
    }

    #[test]
    fn a_decoded_frontier_keeps_the_pool_it_was_validated_for() -> TestResult {
        let orchard = CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard);
        let ironwood = CommitmentTreeFrontier::from_canonical_final_state(
            ShieldedProtocol::Ironwood,
            orchard.final_root(),
            orchard.final_state_bytes(),
        )?;

        assert_eq!(ironwood.protocol(), ShieldedProtocol::Ironwood);
        assert_ne!(ironwood, orchard);
        assert_eq!(
            CommitmentTreeFrontiers::from_validated_parts(None, None, Some(orchard)),
            Err(CommitmentTreeFrontiersError::PoolMismatch {
                pool: ShieldedProtocol::Ironwood,
                frontier_protocol: ShieldedProtocol::Orchard,
            })
        );
        Ok(())
    }

    #[test]
    fn frontier_validation_rejects_wrong_root_trailing_bytes_and_oversize_state() {
        let frontier = CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard);
        assert_eq!(
            CommitmentTreeFrontier::from_canonical_final_state(
                ShieldedProtocol::Orchard,
                FinalNoteCommitmentRoot::from_bytes([0xff; 32]),
                frontier.final_state_bytes(),
            ),
            Err(CommitmentTreeFrontierValidationError::RootMismatch)
        );

        let mut trailing = frontier.final_state_bytes().to_vec();
        trailing.push(0);
        assert!(matches!(
            CommitmentTreeFrontier::from_canonical_final_state(
                ShieldedProtocol::Orchard,
                frontier.final_root(),
                trailing,
            ),
            Err(CommitmentTreeFrontierValidationError::InvalidEncoding { .. })
        ));

        let oversized = vec![0; MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES + 1];
        assert_eq!(
            CommitmentTreeFrontier::from_canonical_final_state(
                ShieldedProtocol::Orchard,
                frontier.final_root(),
                oversized,
            ),
            Err(CommitmentTreeFrontierValidationError::TooLarge {
                byte_count: MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES + 1,
                max_byte_count: MAX_COMMITMENT_TREE_FRONTIER_FINAL_STATE_BYTES,
            })
        );
    }

    #[test]
    fn accumulator_appends_known_commitments_with_protocol_root_order() -> TestResult {
        if let Some(expected) = std::env::var_os("ZINDER_TEST_EXPECT_PASTA_BACKEND") {
            assert_eq!(expected, pasta_curves::BACKEND);
        }
        let activations = pool_activations(1, 1, 1)?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            &activations,
        )?;

        accumulator.append_block_commitments(
            BlockHeight::new(1),
            &[commitment(1)],
            &[commitment(2)],
            &[commitment(3)],
        )?;

        assert_eq!(accumulator.tip_height(), BlockHeight::new(1));
        assert_eq!(accumulator.tip_metadata(), ChainTipMetadata::new(1, 1, 1));
        let frontiers = accumulator.validated_frontiers()?;
        assert_eq!(
            frontiers
                .sapling()
                .ok_or("Sapling must be active")?
                .final_root(),
            FinalNoteCommitmentRoot::from_bytes([
                62, 73, 181, 249, 84, 170, 157, 53, 69, 188, 108, 55, 116, 70, 97, 238, 164, 141,
                124, 52, 227, 0, 13, 130, 183, 240, 1, 12, 48, 244, 194, 251,
            ])
        );
        assert_eq!(
            frontiers
                .orchard()
                .ok_or("Orchard must be active")?
                .final_root(),
            FinalNoteCommitmentRoot::from_bytes([
                174, 41, 53, 241, 223, 216, 162, 74, 237, 124, 112, 223, 125, 227, 166, 104, 235,
                122, 73, 177, 49, 152, 128, 221, 226, 187, 217, 3, 26, 229, 216, 47,
            ])
        );
        assert_eq!(
            frontiers
                .ironwood()
                .ok_or("Ironwood must be active")?
                .final_root(),
            FinalNoteCommitmentRoot::from_bytes([
                126, 86, 86, 235, 116, 117, 167, 178, 9, 252, 248, 164, 31, 140, 202, 188, 14, 200,
                50, 77, 159, 227, 195, 86, 125, 69, 107, 79, 141, 244, 94, 39,
            ])
        );
        let mut one_leaf_final_state = vec![1];
        one_leaf_final_state.extend_from_slice(&commitment(1));
        one_leaf_final_state.extend_from_slice(&[0, 31]);
        one_leaf_final_state.extend_from_slice(&[0; 31]);
        assert_eq!(
            frontiers
                .sapling()
                .ok_or("Sapling must be active")?
                .final_state_bytes(),
            one_leaf_final_state
        );
        let roots = accumulator.final_note_commitment_roots(BlockHash::from_bytes([7; 32]));
        assert_eq!(
            roots,
            frontiers.final_note_commitment_roots(BlockId::new(
                BlockHeight::new(1),
                BlockHash::from_bytes([7; 32]),
            ))
        );
        Ok(())
    }

    #[test]
    fn accumulator_resumes_from_its_validated_checkpoint() -> TestResult {
        let activations = pool_activations(1, 1, 1)?;
        let mut uninterrupted = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            &activations,
        )?;
        uninterrupted.append_block_commitments(
            BlockHeight::new(1),
            &[commitment(1), commitment(2)],
            &[commitment(3)],
            &[],
        )?;
        let checkpoint = uninterrupted.validated_frontiers()?;
        let mut resumed = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(1),
            &checkpoint,
            &activations,
        )?;

        uninterrupted.append_block_commitments(
            BlockHeight::new(2),
            &[commitment(4)],
            &[commitment(5), commitment(6)],
            &[commitment(7)],
        )?;
        resumed.append_block_commitments(
            BlockHeight::new(2),
            &[commitment(4)],
            &[commitment(5), commitment(6)],
            &[commitment(7)],
        )?;

        assert_eq!(resumed.tip_metadata(), uninterrupted.tip_metadata());
        assert_eq!(
            resumed.validated_frontiers()?,
            uninterrupted.validated_frontiers()?
        );
        assert_eq!(
            resumed.final_note_commitment_roots(BlockHash::from_bytes([9; 32])),
            uninterrupted.final_note_commitment_roots(BlockHash::from_bytes([9; 32]))
        );
        Ok(())
    }

    #[test]
    fn accumulator_initializes_pools_only_at_activation() -> TestResult {
        let activations = pool_activations(2, 2, 2)?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            &activations,
        )?;

        assert!(matches!(
            accumulator.append_block_commitments(BlockHeight::new(1), &[commitment(1)], &[], &[],),
            Err(
                CommitmentTreeAccumulatorError::UnexpectedCommitmentsBeforeActivation {
                    protocol: ShieldedProtocol::Sapling,
                    ..
                }
            )
        ));
        assert_eq!(accumulator.tip_height(), BlockHeight::new(0));
        assert_eq!(
            accumulator.validated_frontiers()?,
            CommitmentTreeFrontiers::default()
        );

        accumulator.append_block_commitments(BlockHeight::new(1), &[], &[], &[])?;
        accumulator.append_block_commitments(
            BlockHeight::new(2),
            &[commitment(1)],
            &[commitment(2)],
            &[commitment(3)],
        )?;
        let frontiers = accumulator.validated_frontiers()?;
        assert!(frontiers.sapling().is_some());
        assert!(frontiers.orchard().is_some());
        assert!(frontiers.ironwood().is_some());
        Ok(())
    }

    #[test]
    fn accumulator_rejects_activation_frontier_mismatches() -> TestResult {
        let activations = pool_activations(1, 2, 3)?;
        assert!(matches!(
            CommitmentTreeAccumulator::from_validated_frontiers(
                BlockHeight::new(1),
                &CommitmentTreeFrontiers::default(),
                &activations,
            ),
            Err(
                CommitmentTreeAccumulatorError::PoolActivationFrontierMismatch {
                    protocol: ShieldedProtocol::Sapling,
                    pool_active: true,
                    frontier_present: false,
                    ..
                }
            )
        ));

        let premature = CommitmentTreeFrontiers::from_validated_parts(
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
            None,
            None,
        )?;
        assert!(matches!(
            CommitmentTreeAccumulator::from_validated_frontiers(
                BlockHeight::new(0),
                &premature,
                &activations,
            ),
            Err(
                CommitmentTreeAccumulatorError::PoolActivationFrontierMismatch {
                    protocol: ShieldedProtocol::Sapling,
                    pool_active: false,
                    frontier_present: true,
                    ..
                }
            )
        ));
        Ok(())
    }

    #[test]
    fn accumulator_rejects_malformed_commitments_atomically() -> TestResult {
        let activations = pool_activations(1, 1, 1)?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            &activations,
        )?;

        assert!(matches!(
            accumulator.append_block_commitments(
                BlockHeight::new(1),
                &[commitment(1)],
                &[[0xff; 32]],
                &[],
            ),
            Err(
                CommitmentTreeAccumulatorError::MalformedCommitmentEncoding {
                    protocol: ShieldedProtocol::Orchard,
                    commitment_index: 0,
                    ..
                }
            )
        ));
        assert_eq!(accumulator.tip_height(), BlockHeight::new(0));
        assert_eq!(
            accumulator.validated_frontiers()?,
            CommitmentTreeFrontiers::default()
        );
        Ok(())
    }

    #[test]
    fn accumulator_rejects_appends_beyond_version_one_tree_size() -> TestResult {
        let activations = pool_activations(0, 20, 20)?;
        let full_frontier = full_version_one_sapling_frontier()?;
        assert_eq!(full_frontier.tree_size(), u32::MAX);
        let frontiers =
            CommitmentTreeFrontiers::from_validated_parts(Some(full_frontier), None, None)?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(10),
            &frontiers,
            &activations,
        )?;

        assert!(matches!(
            accumulator.append_block_commitments(BlockHeight::new(11), &[commitment(1)], &[], &[],),
            Err(CommitmentTreeAccumulatorError::TreeFull {
                protocol: ShieldedProtocol::Sapling,
                tree_size: u32::MAX,
                commitment_count: 1,
                ..
            })
        ));
        assert_eq!(accumulator.tip_height(), BlockHeight::new(10));
        assert_eq!(
            accumulator.tip_metadata().sapling_commitment_tree_size,
            u32::MAX
        );
        Ok(())
    }

    fn root_computations() -> u64 {
        ROOT_COMPUTATIONS.get()
    }

    /// Empties this thread's memo, as a restarted process starts without one.
    fn forget_derived_roots() {
        with_frontier_root_memo(|memo| *memo = FrontierRootMemo::new());
    }

    /// Note commitments of one block, in wire order.
    #[derive(Default)]
    struct BlockCommitments {
        sapling: Vec<[u8; 32]>,
        orchard: Vec<[u8; 32]>,
        ironwood: Vec<[u8; 32]>,
    }

    /// The node's note-commitment trees. Their roots are computed directly,
    /// as the node computes them, outside the counted and memoized path.
    struct NodeTrees {
        sapling: Frontier<SaplingNode, NOTE_COMMITMENT_TREE_DEPTH>,
        orchard: Frontier<MerkleHashOrchard, NOTE_COMMITMENT_TREE_DEPTH>,
        ironwood: Frontier<MerkleHashOrchard, NOTE_COMMITMENT_TREE_DEPTH>,
    }

    impl NodeTrees {
        fn after(block: &BlockCommitments) -> TestResultWith<Self> {
            let mut trees = Self {
                sapling: Frontier::empty(),
                orchard: Frontier::empty(),
                ironwood: Frontier::empty(),
            };
            trees.append(block)?;
            Ok(trees)
        }

        fn append(&mut self, block: &BlockCommitments) -> TestResult {
            for bytes in &block.sapling {
                let node = Option::<SaplingNode>::from(SaplingNode::from_bytes(*bytes))
                    .ok_or("fixture Sapling commitment must be canonical")?;
                if !self.sapling.append(node) {
                    return Err("fixture Sapling tree must have capacity".into());
                }
            }
            for (frontier, commitments) in [
                (&mut self.orchard, &block.orchard),
                (&mut self.ironwood, &block.ironwood),
            ] {
                for bytes in commitments {
                    let node =
                        Option::<MerkleHashOrchard>::from(MerkleHashOrchard::from_bytes(bytes))
                            .ok_or("fixture Orchard commitment must be canonical")?;
                    if !frontier.append(node) {
                        return Err("fixture Orchard tree must have capacity".into());
                    }
                }
            }
            Ok(())
        }

        /// Decodes the node's tree state as the JSON-RPC source decodes
        /// `z_gettreestate`.
        fn checkpoint(&self) -> TestResultWith<CommitmentTreeFrontiers> {
            Ok(CommitmentTreeFrontiers::from_validated_parts(
                Some(node_frontier(
                    ShieldedProtocol::Sapling,
                    &self.sapling,
                    sapling_root_bytes_in_rpc_order,
                )?),
                Some(node_frontier(
                    ShieldedProtocol::Orchard,
                    &self.orchard,
                    orchard_root_bytes_in_rpc_order,
                )?),
                Some(node_frontier(
                    ShieldedProtocol::Ironwood,
                    &self.ironwood,
                    orchard_root_bytes_in_rpc_order,
                )?),
            )?)
        }
    }

    fn node_frontier<Node>(
        protocol: ShieldedProtocol,
        frontier: &Frontier<Node, NOTE_COMMITMENT_TREE_DEPTH>,
        root_bytes_in_rpc_order: impl Fn(&Node) -> [u8; 32],
    ) -> TestResultWith<CommitmentTreeFrontier>
    where
        Node: Clone + Hashable + HashSer,
    {
        let mut final_state_bytes = Vec::new();
        write_commitment_tree(
            &CommitmentTree::from_frontier(frontier),
            &mut final_state_bytes,
        )?;
        Ok(CommitmentTreeFrontier::from_canonical_final_state(
            protocol,
            FinalNoteCommitmentRoot::from_bytes(root_bytes_in_rpc_order(&frontier.root())),
            final_state_bytes,
        )?)
    }

    /// Decodes persisted frontiers as the canonical store reads its
    /// visible-tip tree-state checkpoint row.
    fn reread(persisted: &CommitmentTreeFrontiers) -> TestResultWith<CommitmentTreeFrontiers> {
        let reread = |protocol| {
            persisted
                .get(protocol)
                .map(|frontier| {
                    CommitmentTreeFrontier::from_canonical_final_state(
                        protocol,
                        frontier.final_root(),
                        frontier.final_state_bytes(),
                    )
                })
                .transpose()
        };
        Ok(CommitmentTreeFrontiers::from_validated_parts(
            reread(ShieldedProtocol::Sapling)?,
            reread(ShieldedProtocol::Orchard)?,
            reread(ShieldedProtocol::Ironwood)?,
        )?)
    }

    fn derive(
        tip_height: BlockHeight,
        anchor: &CommitmentTreeFrontiers,
        block: &BlockCommitments,
        activations: &NetworkUpgradeActivations,
    ) -> TestResultWith<CommitmentTreeFrontiers> {
        let mut accumulator =
            CommitmentTreeAccumulator::from_validated_frontiers(tip_height, anchor, activations)?;
        accumulator.append_block_commitments(
            tip_height.next().ok_or("fixture height must advance")?,
            &block.sapling,
            &block.orchard,
            &block.ironwood,
        )?;
        Ok(accumulator.validated_frontiers()?)
    }

    /// A writer following a node, reduced to its commitment-tree work.
    struct FollowedChain {
        activations: NetworkUpgradeActivations,
        tip_height: BlockHeight,
        persisted: CommitmentTreeFrontiers,
        node: NodeTrees,
    }

    impl FollowedChain {
        /// Appends one block in production order and returns the roots it
        /// derived. The follow iteration reads the store's append anchor,
        /// decodes the node's tree state for the next block, and derives the
        /// next frontiers from the anchor. The store then reads its anchor
        /// again and re-derives the frontiers before it commits them.
        fn append(&mut self, block: &BlockCommitments) -> TestResultWith<u64> {
            let before = root_computations();
            let follow_anchor = reread(&self.persisted)?;
            self.node.append(block)?;
            let source = self.node.checkpoint()?;
            let derived = derive(self.tip_height, &follow_anchor, block, &self.activations)?;
            assert_eq!(derived, source, "ingest derives the node's frontiers");
            let store_anchor = reread(&self.persisted)?;
            let committed = derive(self.tip_height, &store_anchor, block, &self.activations)?;
            assert_eq!(
                committed, derived,
                "the store re-derives the same frontiers"
            );
            self.persisted = committed;
            self.tip_height = self
                .tip_height
                .next()
                .ok_or("fixture height must advance")?;
            Ok(root_computations() - before)
        }
    }

    #[test]
    fn following_a_block_derives_one_root_per_changed_pool() -> TestResult {
        let first_leaves = BlockCommitments {
            sapling: vec![commitment(1)],
            orchard: vec![commitment(2)],
            ironwood: vec![commitment(3)],
        };
        let node = NodeTrees::after(&first_leaves)?;
        let mut chain = FollowedChain {
            activations: pool_activations(1, 1, 1)?,
            tip_height: BlockHeight::new(1),
            persisted: node.checkpoint()?,
            node,
        };
        forget_derived_roots();

        // The first block derives the persisted root of each pool once.
        assert_eq!(chain.append(&BlockCommitments::default())?, 3);
        assert_eq!(chain.append(&BlockCommitments::default())?, 0);
        let sapling_only = BlockCommitments {
            sapling: vec![commitment(4), commitment(5)],
            ..BlockCommitments::default()
        };
        assert_eq!(chain.append(&sapling_only)?, 1);
        let orchard_only = BlockCommitments {
            orchard: vec![commitment(6)],
            ..BlockCommitments::default()
        };
        assert_eq!(chain.append(&orchard_only)?, 1);
        let sapling_and_ironwood = BlockCommitments {
            sapling: vec![commitment(7)],
            ironwood: vec![commitment(8)],
            ..BlockCommitments::default()
        };
        assert_eq!(chain.append(&sapling_and_ironwood)?, 2);
        // Unchanged pools stay memoized while another pool changes every block
        // for longer than the memo holds entries.
        for value in (9_u8..).take(2 * FRONTIER_ROOT_MEMO_CAPACITY) {
            let sapling_only = BlockCommitments {
                sapling: vec![commitment(value)],
                ..BlockCommitments::default()
            };
            assert_eq!(chain.append(&sapling_only)?, 1);
        }
        assert_eq!(chain.append(&BlockCommitments::default())?, 0);
        Ok(())
    }

    #[test]
    fn a_derived_frontier_that_differs_from_the_node_keeps_its_own_root() -> TestResult {
        let activations = pool_activations(1, 1, 1)?;
        let first_leaves = BlockCommitments {
            sapling: vec![commitment(1)],
            ..BlockCommitments::default()
        };
        let mut node = NodeTrees::after(&first_leaves)?;
        let persisted = node.checkpoint()?;
        node.append(&BlockCommitments {
            sapling: vec![commitment(9)],
            ..BlockCommitments::default()
        })?;
        let source = node.checkpoint()?;

        let derived = derive(
            BlockHeight::new(1),
            &reread(&persisted)?,
            &BlockCommitments {
                sapling: vec![commitment(8)],
                ..BlockCommitments::default()
            },
            &activations,
        )?;

        let derived_sapling = derived.sapling().ok_or("Sapling must be active")?;
        let source_sapling = source.sapling().ok_or("Sapling must be active")?;
        assert_ne!(
            derived_sapling.final_state_bytes(),
            source_sapling.final_state_bytes()
        );
        assert_ne!(derived_sapling.final_root(), source_sapling.final_root());
        assert_ne!(derived, source);
        Ok(())
    }

    /// Makes the next accumulator snapshot on this thread encode `frontier`
    /// in place of its own tree.
    fn encode_next_snapshot_as(frontier: &CommitmentTreeFrontier) {
        SNAPSHOT_ENCODING_OVERRIDE.set(Some(frontier.final_state_bytes().to_vec()));
    }

    fn append_block(
        accumulator: &mut CommitmentTreeAccumulator,
        block: &BlockCommitments,
    ) -> TestResult {
        let height = accumulator
            .tip_height()
            .next()
            .ok_or("fixture height must advance")?;
        accumulator.append_block_commitments(
            height,
            &block.sapling,
            &block.orchard,
            &block.ironwood,
        )?;
        Ok(())
    }

    #[test]
    fn wallet_frontier_bytes_round_trip_without_deriving_roots() -> TestResult {
        let block = BlockCommitments {
            sapling: vec![commitment(1), commitment(2), commitment(3)],
            orchard: vec![commitment(4)],
            ironwood: vec![],
        };
        let expected = NodeTrees::after(&block)?.checkpoint()?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            &pool_activations(1, 1, 1)?,
        )?;
        assert_eq!(
            accumulator.final_state_bytes(ShieldedProtocol::Sapling)?,
            None
        );
        append_block(&mut accumulator, &block)?;
        forget_derived_roots();
        let before = root_computations();
        for protocol in [
            ShieldedProtocol::Sapling,
            ShieldedProtocol::Orchard,
            ShieldedProtocol::Ironwood,
        ] {
            assert_eq!(
                accumulator.final_state_bytes(protocol)?.as_deref(),
                expected
                    .get(protocol)
                    .map(CommitmentTreeFrontier::final_state_bytes)
            );
        }
        assert_eq!(root_computations(), before);

        let wrong = NodeTrees::after(&BlockCommitments {
            sapling: vec![commitment(9), commitment(2), commitment(3)],
            ..BlockCommitments::default()
        })?
        .checkpoint()?;
        encode_next_snapshot_as(wrong.sapling().ok_or("Sapling must be active")?);
        assert!(matches!(
            accumulator.final_state_bytes(ShieldedProtocol::Sapling),
            Err(
                CommitmentTreeAccumulatorError::FrontierSnapshotValidationFailed {
                    protocol: ShieldedProtocol::Sapling,
                    ..
                }
            )
        ));
        Ok(())
    }

    #[test]
    fn a_construction_checkpoint_rejects_an_encoding_that_differs_from_its_frontier() -> TestResult
    {
        let first_leaves = BlockCommitments {
            sapling: vec![commitment(1)],
            ..BlockCommitments::default()
        };
        let mut node = NodeTrees::after(&first_leaves)?;
        let predecessor = node.checkpoint()?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(1),
            &predecessor,
            &pool_activations(1, 1, 1)?,
        )?;
        let block = BlockCommitments {
            sapling: vec![commitment(2)],
            ..BlockCommitments::default()
        };
        append_block(&mut accumulator, &block)?;
        node.append(&block)?;
        let source = node.checkpoint()?;
        // A codec that lost the block's commitment would encode the
        // predecessor tree, which is itself a valid canonical encoding.
        let lost_commitment = predecessor.sapling().ok_or("Sapling must be active")?;

        encode_next_snapshot_as(lost_commitment);
        let unchecked = accumulator.validated_frontiers()?;
        assert_eq!(unchecked.sapling(), Some(lost_commitment));
        assert_ne!(unchecked, source, "live following compares with the node");

        encode_next_snapshot_as(lost_commitment);
        assert!(matches!(
            accumulator.cross_checked_frontiers(),
            Err(
                CommitmentTreeAccumulatorError::FrontierSnapshotValidationFailed {
                    protocol: ShieldedProtocol::Sapling,
                    source: CommitmentTreeFrontierValidationError::RootMismatch,
                }
            )
        ));
        assert_eq!(accumulator.cross_checked_frontiers()?, source);
        Ok(())
    }

    #[test]
    fn a_construction_checkpoint_rejects_a_memoized_root_that_differs_from_its_frontier()
    -> TestResult {
        let block = BlockCommitments {
            sapling: vec![commitment(1)],
            ..BlockCommitments::default()
        };
        let source = NodeTrees::after(&block)?.checkpoint()?;
        let source_sapling = source.sapling().ok_or("Sapling must be active")?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            &pool_activations(1, 1, 1)?,
        )?;
        append_block(&mut accumulator, &block)?;
        forget_derived_roots();
        let wrong_root = FinalNoteCommitmentRoot::from_bytes([0xff; 32]);
        with_frontier_root_memo(|memo| {
            memo.insert(
                ShieldedProtocol::Sapling,
                source_sapling.final_state_bytes(),
                wrong_root,
            );
        });

        assert_eq!(
            accumulator
                .validated_frontiers()?
                .sapling()
                .map(CommitmentTreeFrontier::final_root),
            Some(wrong_root)
        );
        assert!(matches!(
            accumulator.cross_checked_frontiers(),
            Err(
                CommitmentTreeAccumulatorError::FrontierSnapshotValidationFailed {
                    protocol: ShieldedProtocol::Sapling,
                    source: CommitmentTreeFrontierValidationError::RootMismatch,
                }
            )
        ));
        Ok(())
    }

    #[test]
    fn a_construction_checkpoint_derives_two_roots_per_changed_pool() -> TestResult {
        forget_derived_roots();
        let first_leaves = BlockCommitments {
            sapling: vec![commitment(1)],
            orchard: vec![commitment(2)],
            ironwood: vec![commitment(3)],
        };
        let mut node = NodeTrees::after(&first_leaves)?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(1),
            &node.checkpoint()?,
            &pool_activations(1, 1, 1)?,
        )?;
        let before = root_computations();

        for value in 4_u8..14 {
            let block = BlockCommitments {
                sapling: vec![commitment(value)],
                ..BlockCommitments::default()
            };
            append_block(&mut accumulator, &block)?;
            node.append(&block)?;
        }
        assert_eq!(root_computations() - before, 0);
        // The changed pool derives its root from its frontier and from its
        // encoding, and the unchanged pools derive none.
        let checkpoint = accumulator.cross_checked_frontiers()?;
        assert_eq!(root_computations() - before, 2);
        // Explorer final roots and an unchanged checkpoint reuse those roots.
        assert_eq!(
            accumulator.final_note_commitment_roots(BlockHash::from_bytes([7; 32])),
            checkpoint.final_note_commitment_roots(BlockId::new(
                accumulator.tip_height(),
                BlockHash::from_bytes([7; 32]),
            ))
        );
        assert_eq!(accumulator.cross_checked_frontiers()?, checkpoint);
        assert_eq!(root_computations() - before, 2);
        assert_eq!(checkpoint, node.checkpoint()?);
        Ok(())
    }

    #[test]
    fn frontier_validation_reuses_a_derived_root_but_checks_every_input() -> TestResult {
        // Orchard's uncommitted leaf is two, so appending another two would
        // leave the root unchanged.
        let leaf = Option::<MerkleHashOrchard>::from(MerkleHashOrchard::from_bytes(&commitment(5)))
            .ok_or("five is a canonical Pallas base field element")?;
        let mut frontier = Frontier::<MerkleHashOrchard, NOTE_COMMITMENT_TREE_DEPTH>::empty();
        if !frontier.append(leaf) {
            return Err("an empty tree must accept a leaf".into());
        }
        let validated = node_frontier(
            ShieldedProtocol::Orchard,
            &frontier,
            orchard_root_bytes_in_rpc_order,
        )?;
        let final_root = validated.final_root();
        let final_state_bytes = validated.final_state_bytes().to_vec();
        forget_derived_roots();
        let before = root_computations();

        assert_eq!(
            CommitmentTreeFrontier::from_canonical_final_state(
                ShieldedProtocol::Orchard,
                final_root,
                final_state_bytes.clone(),
            )?,
            validated
        );
        assert_eq!(root_computations() - before, 1);
        assert_eq!(
            CommitmentTreeFrontier::from_canonical_final_state(
                ShieldedProtocol::Orchard,
                final_root,
                final_state_bytes.clone(),
            )?,
            validated
        );
        assert_eq!(root_computations() - before, 1);

        // A memoized root still has to match the claimed root.
        assert_eq!(
            CommitmentTreeFrontier::from_canonical_final_state(
                ShieldedProtocol::Orchard,
                FinalNoteCommitmentRoot::from_bytes([0xff; 32]),
                final_state_bytes.clone(),
            ),
            Err(CommitmentTreeFrontierValidationError::RootMismatch)
        );
        let mut trailing = final_state_bytes.clone();
        trailing.push(0);
        assert!(matches!(
            CommitmentTreeFrontier::from_canonical_final_state(
                ShieldedProtocol::Orchard,
                final_root,
                trailing,
            ),
            Err(CommitmentTreeFrontierValidationError::InvalidEncoding { .. })
        ));
        assert_eq!(root_computations() - before, 1);

        // The same bytes under another pool are derived independently.
        CommitmentTreeFrontier::from_canonical_final_state(
            ShieldedProtocol::Ironwood,
            final_root,
            final_state_bytes,
        )?;
        assert_eq!(root_computations() - before, 2);

        // Unseen bytes are derived in full, and a stale root is rejected.
        if !frontier.append(leaf) {
            return Err("a one-leaf tree must accept a leaf".into());
        }
        let mut grown_bytes = Vec::new();
        write_commitment_tree(&CommitmentTree::from_frontier(&frontier), &mut grown_bytes)?;
        assert_eq!(
            CommitmentTreeFrontier::from_canonical_final_state(
                ShieldedProtocol::Orchard,
                final_root,
                grown_bytes,
            ),
            Err(CommitmentTreeFrontierValidationError::RootMismatch)
        );
        assert_eq!(root_computations() - before, 3);
        Ok(())
    }

    #[test]
    fn frontier_root_memo_keeps_only_its_most_recently_used_roots() -> TestResult {
        let root = |tag: u8| FinalNoteCommitmentRoot::from_bytes([tag; 32]);
        let mut memo = FrontierRootMemo::new();
        for tag in 0..=u8::try_from(FRONTIER_ROOT_MEMO_CAPACITY)? {
            memo.insert(ShieldedProtocol::Sapling, &[tag], root(tag));
        }

        assert_eq!(memo.entries.len(), FRONTIER_ROOT_MEMO_CAPACITY);
        assert_eq!(memo.get(ShieldedProtocol::Sapling, &[0]), None);
        assert_eq!(memo.get(ShieldedProtocol::Sapling, &[1]), Some(root(1)));
        memo.insert(ShieldedProtocol::Sapling, &[0xfe], root(0xfe));
        assert_eq!(
            memo.get(ShieldedProtocol::Sapling, &[1]),
            Some(root(1)),
            "a hit keeps its entry"
        );
        assert_eq!(memo.get(ShieldedProtocol::Sapling, &[2]), None);
        assert_eq!(memo.get(ShieldedProtocol::Orchard, &[1]), None);
        assert_eq!(memo.get(ShieldedProtocol::Sapling, &[1, 0]), None);
        assert_eq!(memo.entries.len(), FRONTIER_ROOT_MEMO_CAPACITY);
        Ok(())
    }

    fn sapling_node(value: u8) -> TestResultWith<SaplingNode> {
        Ok(
            Option::<SaplingNode>::from(SaplingNode::from_bytes(commitment(value)))
                .ok_or("small values are canonical Sapling field elements")?,
        )
    }

    fn orchard_node(value: u8) -> TestResultWith<MerkleHashOrchard> {
        Ok(
            Option::<MerkleHashOrchard>::from(MerkleHashOrchard::from_bytes(&commitment(value)))
                .ok_or("small values are canonical Pallas base field elements")?,
        )
    }

    /// Returns a canonical frontier of `tree_size` leaves whose tip leaf and
    /// every ommer are `leaf`.
    fn uniform_frontier<Node>(
        protocol: ShieldedProtocol,
        tree_size: u32,
        leaf: &Node,
        root_bytes_in_rpc_order: impl Fn(&Node) -> [u8; 32],
    ) -> TestResultWith<CommitmentTreeFrontier>
    where
        Node: Clone + Hashable + HashSer,
    {
        let position = Position::from(u64::from(
            tree_size.checked_sub(1).ok_or("a frontier needs a leaf")?,
        ));
        let ommer_count = usize::from(position.past_ommer_count());
        let frontier = Frontier::<Node, NOTE_COMMITMENT_TREE_DEPTH>::from_parts(
            position,
            leaf.clone(),
            vec![leaf.clone(); ommer_count],
        )
        .map_err(|error| format!("uniform frontier rejected: {error:?}"))?;
        let mut final_state_bytes = Vec::new();
        write_commitment_tree(
            &CommitmentTree::from_frontier(&frontier),
            &mut final_state_bytes,
        )?;
        Ok(CommitmentTreeFrontier::from_canonical_final_state(
            protocol,
            FinalNoteCommitmentRoot::from_bytes(root_bytes_in_rpc_order(&frontier.root())),
            final_state_bytes,
        )?)
    }

    /// Hashes `last_leaf` up to the subtree root level, with `left` as the
    /// left sibling at every level, as a uniform frontier one leaf short of a
    /// subtree boundary supplies.
    fn expected_subtree_root<Node: Hashable>(left: &Node, last_leaf: Node) -> Node {
        (0..SUBTREE_ROOT_LEVEL).fold(last_leaf, |digest, level| {
            Node::combine(Level::from(level), left, &digest)
        })
    }

    #[test]
    fn accumulator_reports_the_subtrees_each_block_completes() -> TestResult {
        let activations = pool_activations(1, 1, 1)?;
        let sapling_leaf = sapling_node(1)?;
        let orchard_leaf = orchard_node(1)?;
        let one_leaf_short = SUBTREE_LEAF_COUNT - 1;
        let frontiers = CommitmentTreeFrontiers::from_validated_parts(
            Some(uniform_frontier(
                ShieldedProtocol::Sapling,
                one_leaf_short,
                &sapling_leaf,
                sapling_root_bytes_in_rpc_order,
            )?),
            Some(uniform_frontier(
                ShieldedProtocol::Orchard,
                one_leaf_short,
                &orchard_leaf,
                orchard_root_bytes_in_rpc_order,
            )?),
            Some(uniform_frontier(
                ShieldedProtocol::Ironwood,
                2 * SUBTREE_LEAF_COUNT - 1,
                &orchard_leaf,
                orchard_root_bytes_in_rpc_order,
            )?),
        )?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(9),
            &frontiers,
            &activations,
        )?;
        assert_eq!(accumulator.completed_subtree_roots(), []);

        // Sapling completes subtree 0 with its last commitment, Orchard
        // completes subtree 0 with the first of two, and Ironwood completes
        // subtree 1.
        accumulator.append_block_commitments(
            BlockHeight::new(10),
            &[commitment(2)],
            &[commitment(3), commitment(4)],
            &[commitment(5)],
        )?;
        let completed = |protocol, index, root: [u8; 32]| CompletedSubtreeRoot {
            protocol,
            subtree_index: SubtreeRootIndex::new(index),
            root_hash: SubtreeRootHash::from_bytes(root),
            completing_block_height: BlockHeight::new(10),
        };
        let sapling_root = expected_subtree_root(&sapling_leaf, sapling_node(2)?).to_bytes();
        let orchard_root = expected_subtree_root(&orchard_leaf, orchard_node(3)?).to_bytes();
        let ironwood_root = expected_subtree_root(&orchard_leaf, orchard_node(5)?).to_bytes();
        assert_eq!(
            accumulator.completed_subtree_roots(),
            [
                completed(ShieldedProtocol::Sapling, 0, sapling_root),
                completed(ShieldedProtocol::Orchard, 0, orchard_root),
                completed(ShieldedProtocol::Ironwood, 1, ironwood_root),
            ]
        );
        assert_eq!(
            accumulator.tip_metadata(),
            ChainTipMetadata::new(
                SUBTREE_LEAF_COUNT,
                SUBTREE_LEAF_COUNT + 1,
                2 * SUBTREE_LEAF_COUNT
            )
        );

        accumulator.append_block_commitments(BlockHeight::new(11), &[commitment(6)], &[], &[])?;
        assert_eq!(accumulator.completed_subtree_roots(), []);
        Ok(())
    }

    #[test]
    fn a_rejected_block_keeps_the_previous_blocks_completed_subtrees() -> TestResult {
        let activations = pool_activations(1, 1, 1)?;
        let sapling_leaf = sapling_node(1)?;
        let frontiers = CommitmentTreeFrontiers::from_validated_parts(
            Some(uniform_frontier(
                ShieldedProtocol::Sapling,
                SUBTREE_LEAF_COUNT - 1,
                &sapling_leaf,
                sapling_root_bytes_in_rpc_order,
            )?),
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Ironwood)),
        )?;
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(9),
            &frontiers,
            &activations,
        )?;
        accumulator.append_block_commitments(BlockHeight::new(10), &[commitment(2)], &[], &[])?;
        let completed = accumulator.completed_subtree_roots().to_vec();
        assert_eq!(completed.len(), 1);

        assert!(
            accumulator
                .append_block_commitments(BlockHeight::new(11), &[[0xff; 32]], &[], &[])
                .is_err()
        );
        assert_eq!(accumulator.completed_subtree_roots(), completed);
        Ok(())
    }
}
