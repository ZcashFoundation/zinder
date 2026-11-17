//! Durable subtree-root artifact values.

use std::num::NonZeroU32;

use crate::{BlockHash, BlockHeight};

/// Number of note commitments in one shielded subtree-root shard.
///
/// Lightwalletd's `GetSubtreeRoots` surface shards Sapling and Orchard note
/// commitment trees at height 16, half of their 32-level note commitment tree
/// depth. A height-16 subtree contains 2^16 leaves.
pub const SUBTREE_LEAF_COUNT: u32 = 1 << 16;

/// Maximum number of subtree roots returned by one wallet query request.
///
/// The bound prevents one request from forcing an unbounded storage scan and
/// response allocation.
pub const MAX_SUBTREE_ROOTS_PER_REQUEST: u32 = 1_024;

/// Shielded note commitment protocol.
///
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ShieldedProtocol {
    /// Sapling note commitment tree.
    Sapling,
    /// Orchard note commitment tree.
    Orchard,
    /// Ironwood note commitment tree.
    Ironwood,
}

impl ShieldedProtocol {
    /// Returns the stable storage key identifier for this shielded protocol.
    #[must_use]
    pub const fn id(self) -> u8 {
        match self {
            Self::Sapling => 1,
            Self::Orchard => 2,
            Self::Ironwood => 3,
        }
    }

    /// Resolves a storage key protocol identifier into a known protocol.
    #[must_use]
    pub const fn from_id(protocol_id: u8) -> Option<Self> {
        match protocol_id {
            1 => Some(Self::Sapling),
            2 => Some(Self::Orchard),
            3 => Some(Self::Ironwood),
            _ => None,
        }
    }

    /// Returns the node RPC pool name for this shielded protocol.
    #[must_use]
    pub const fn rpc_pool_name(self) -> &'static str {
        match self {
            Self::Sapling => "sapling",
            Self::Orchard => "orchard",
            Self::Ironwood => "ironwood",
        }
    }

    /// Returns the node-advertised upgrade name that activates this pool.
    #[must_use]
    pub const fn activation_upgrade_name(self) -> &'static str {
        match self {
            Self::Sapling => "Sapling",
            Self::Orchard => "NU5",
            Self::Ironwood => "NU6.3",
        }
    }
}

/// Index of a 2^16-leaf shielded note commitment subtree.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SubtreeRootIndex(u32);

impl SubtreeRootIndex {
    /// Creates a subtree-root index from its numeric value.
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    /// Returns the numeric subtree-root index.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }

    /// Returns the next subtree-root index.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Inclusive-start bounded subtree-root range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubtreeRootRange {
    /// Shielded protocol requested.
    pub protocol: ShieldedProtocol,
    /// First requested subtree-root index.
    pub start_index: SubtreeRootIndex,
    /// Maximum number of subtree roots to return.
    ///
    /// Wallet query boundaries reject values above
    /// [`MAX_SUBTREE_ROOTS_PER_REQUEST`] before reading storage.
    pub max_entries: NonZeroU32,
}

impl SubtreeRootRange {
    /// Creates a bounded subtree-root range.
    ///
    /// This domain constructor does not apply a consumer-specific limit.
    /// Wallet query boundaries enforce [`MAX_SUBTREE_ROOTS_PER_REQUEST`].
    #[must_use]
    pub const fn new(
        protocol: ShieldedProtocol,
        start_index: SubtreeRootIndex,
        max_entries: NonZeroU32,
    ) -> Self {
        Self {
            protocol,
            start_index,
            max_entries,
        }
    }
}

impl IntoIterator for SubtreeRootRange {
    type Item = SubtreeRootIndex;
    type IntoIter = SubtreeRootRangeIter;

    fn into_iter(self) -> Self::IntoIter {
        SubtreeRootRangeIter {
            next: self.start_index.value(),
            remaining: self.max_entries.get(),
        }
    }
}

/// Iterator over a bounded subtree-root range.
#[derive(Clone, Debug)]
pub struct SubtreeRootRangeIter {
    next: u32,
    remaining: u32,
}

impl Iterator for SubtreeRootRangeIter {
    type Item = SubtreeRootIndex;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let current = SubtreeRootIndex::new(self.next);
        self.next = self.next.saturating_add(1);
        self.remaining = self.remaining.saturating_sub(1);
        Some(current)
    }
}

impl ExactSizeIterator for SubtreeRootRangeIter {
    fn len(&self) -> usize {
        u32_to_usize(self.remaining)
    }
}

impl std::iter::FusedIterator for SubtreeRootRangeIter {}

#[allow(
    clippy::cast_possible_truncation,
    reason = "zinder-core rejects targets with pointer widths below 32 bits"
)]
const fn u32_to_usize(count: u32) -> usize {
    count as usize
}

/// Merkle root hash of a complete 2^16-leaf shielded subtree.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SubtreeRootHash([u8; 32]);

impl SubtreeRootHash {
    /// Creates a subtree-root hash from 32-byte hash material.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the subtree-root hash bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Root of a subtree that one block completed, before that block's hash is
/// attached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletedSubtreeRoot {
    /// Shielded protocol this subtree root belongs to.
    pub protocol: ShieldedProtocol,
    /// Subtree index within `protocol`.
    pub subtree_index: SubtreeRootIndex,
    /// Merkle root of the complete subtree.
    pub root_hash: SubtreeRootHash,
    /// Height of the block that completed this subtree.
    pub completing_block_height: BlockHeight,
}

/// Wallet-oriented subtree-root artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubtreeRootArtifact {
    /// Shielded protocol this subtree root belongs to.
    pub protocol: ShieldedProtocol,
    /// Subtree index within `protocol`.
    pub subtree_index: SubtreeRootIndex,
    /// Merkle root of the complete subtree.
    pub root_hash: SubtreeRootHash,
    /// Height of the block that completed this subtree.
    pub completing_block_height: BlockHeight,
    /// Hash of the block that completed this subtree.
    pub completing_block_hash: BlockHash,
}

impl SubtreeRootArtifact {
    /// Creates a subtree-root artifact.
    #[must_use]
    pub const fn new(
        protocol: ShieldedProtocol,
        subtree_index: SubtreeRootIndex,
        root_hash: SubtreeRootHash,
        completing_block_height: BlockHeight,
        completing_block_hash: BlockHash,
    ) -> Self {
        Self {
            protocol,
            subtree_index,
            root_hash,
            completing_block_height,
            completing_block_hash,
        }
    }
}
