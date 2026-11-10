//! Storage error vocabulary.

use std::fmt;

use thiserror::Error;

use crate::format::StoreKey;

/// Artifact family used in storage errors and key diagnostics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ArtifactFamily {
    /// Canonical block-header facts.
    BlockHeader,
    /// Optional raw block blob.
    BlockBlob,
    /// Compact block artifact.
    CompactBlock,
    /// Mined transaction location.
    TransactionLocation,
    /// Optional raw transaction blob.
    TransactionBlob,
    /// Commitment tree-state artifact.
    TreeState,
    /// Commitment subtree-root artifact.
    SubtreeRoot,
    /// Mempool event envelope.
    MempoolEvent,
}

impl ArtifactFamily {
    /// Returns the canonical on-wire family label.
    ///
    /// The label is the value emitted in `google.rpc.ResourceInfo.resource_type`
    /// for an unavailable artifact, and the value a client reads back. Labels
    /// come from [`zinder_core::artifact_family`] so producers and consumers
    /// share one string per family.
    #[must_use]
    pub const fn wire_label(self) -> &'static str {
        use zinder_core::artifact_family as family;
        match self {
            Self::BlockHeader => family::BLOCK_HEADER_ARTIFACT,
            Self::BlockBlob => family::BLOCK_BLOB,
            Self::CompactBlock => family::COMPACT_BLOCK,
            Self::TransactionLocation => family::TRANSACTION_LOCATION,
            Self::TransactionBlob => family::TRANSACTION_BLOB,
            Self::TreeState => family::TREE_STATE,
            Self::SubtreeRoot => family::SUBTREE_ROOT,
            Self::MempoolEvent => family::MEMPOOL_EVENT,
        }
    }
}

/// Opaque storage-key bytes included in diagnostic storage errors.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct StorageKey(Vec<u8>);

impl StorageKey {
    /// Returns the encoded storage-key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for StorageKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("StorageKey").field(&self.0).finish()
    }
}

impl From<&StoreKey> for StorageKey {
    fn from(key: &StoreKey) -> Self {
        Self(key.as_bytes().to_vec())
    }
}

impl From<StoreKey> for StorageKey {
    fn from(key: StoreKey) -> Self {
        Self(key.into_bytes())
    }
}

/// Error returned by the storage-envelope codecs.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Commit value failed domain validation before a durable write.
    #[error("invalid chain epoch artifacts: {reason}")]
    InvalidChainEpochArtifacts {
        /// Validation failure reason.
        reason: &'static str,
    },

    /// Artifact exists but cannot be decoded or validated.
    #[error("corrupt artifact in {family:?} for key {key:?}: {reason}")]
    ArtifactCorrupt {
        /// Corrupt artifact family.
        family: ArtifactFamily,
        /// Corrupt artifact key.
        key: StorageKey,
        /// Corruption reason.
        reason: &'static str,
    },
}
