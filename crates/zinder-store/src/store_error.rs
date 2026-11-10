//! Storage error vocabulary.

use std::{error::Error, fmt, num::NonZeroU32, path::PathBuf};

use thiserror::Error;

use crate::{RawBlobRetention, format::StoreKey};
use zinder_core::{BlockHeight, BlockId, ChainEpochId, Network};

/// Stable storage-engine failure category for operator diagnostics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum StorageErrorKind {
    /// `RocksDB` returned an error.
    RocksDb,
}

/// Artifact family used in storage errors and key diagnostics.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ArtifactFamily {
    /// Chain epoch metadata.
    ChainEpoch,
    /// Chain event envelope.
    ChainEvent,
    /// Canonical block-header facts.
    BlockHeader,
    /// Complete canonical block replay envelopes.
    BlockReplay,
    /// Optional raw block blob.
    BlockBlob,
    /// Compact block artifact.
    CompactBlock,
    /// Block-local transaction index row.
    BlockTransactionIndex,
    /// Mined transaction location.
    TransactionLocation,
    /// Canonical transaction facts.
    TransactionFacts,
    /// Optional transaction-intrinsic shielded value balances.
    TransactionIntrinsicValueBalances,
    /// Optional raw transaction blob.
    TransactionBlob,
    /// Commitment tree-state artifact.
    TreeState,
    /// Final note-commitment roots after a canonical block.
    FinalNoteCommitmentRoots,
    /// Optional cumulative value-pool balances after a canonical block.
    BlockValuePoolBalances,
    /// Commitment subtree-root artifact.
    SubtreeRoot,
    /// Transparent address output artifact.
    AddressOutputIndex,
    /// Transparent-output artifact keyed by outpoint.
    TransparentOutput,
    /// Resolved transparent spend fact.
    TransparentSpendFact,
    /// Best-chain block-hash to height index entry.
    BlockHashIndex,
    /// Block displaced from the canonical branch.
    DisplacedBlock,
    /// Mempool event envelope.
    MempoolEvent,
}

impl ArtifactFamily {
    /// Returns the canonical on-wire family label.
    ///
    /// The label is the value emitted in `google.rpc.ResourceInfo.resource_type`
    /// for [`StoreError::ArtifactMissing`] and the matching query error, and the
    /// value a client reads back. Labels come from
    /// [`zinder_core::artifact_family`] so producers and consumers share one
    /// string per family.
    #[must_use]
    pub const fn wire_label(self) -> &'static str {
        use zinder_core::artifact_family as family;
        match self {
            Self::ChainEpoch => family::CHAIN_EPOCH,
            Self::ChainEvent => family::CHAIN_EVENT,
            Self::BlockHeader => family::BLOCK_HEADER_ARTIFACT,
            Self::BlockReplay => family::BLOCK_REPLAY,
            Self::BlockBlob => family::BLOCK_BLOB,
            Self::CompactBlock => family::COMPACT_BLOCK,
            Self::BlockTransactionIndex => family::BLOCK_TRANSACTION_INDEX,
            Self::TransactionLocation => family::TRANSACTION_LOCATION,
            Self::TransactionFacts => family::TRANSACTION_FACTS,
            Self::TransactionIntrinsicValueBalances => family::TRANSACTION_INTRINSIC_VALUE_BALANCES,
            Self::TransactionBlob => family::TRANSACTION_BLOB,
            Self::TreeState => family::TREE_STATE,
            Self::FinalNoteCommitmentRoots => "zinder.block_final_note_commitment_roots",
            Self::BlockValuePoolBalances => family::BLOCK_VALUE_POOL_BALANCES,
            Self::SubtreeRoot => family::SUBTREE_ROOT,
            Self::AddressOutputIndex => family::ADDRESS_OUTPUT_INDEX,
            Self::TransparentOutput => family::TRANSPARENT_OUTPUT,
            Self::TransparentSpendFact => family::TRANSPARENT_SPEND_FACT,
            Self::BlockHashIndex => family::BLOCK_HASH_INDEX,
            Self::DisplacedBlock => family::DISPLACED_BLOCK,
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

/// Error returned by canonical storage operations.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Storage engine or filesystem access failed.
    #[error("storage is unavailable: {kind:?}")]
    StorageUnavailable {
        /// Stable failure category.
        kind: StorageErrorKind,
        /// Underlying storage-engine error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },

    /// Operating-system entropy was unavailable while creating store secrets.
    #[error("entropy is unavailable")]
    EntropyUnavailable {
        /// Underlying entropy failure.
        #[source]
        source: getrandom::Error,
    },

    /// Requested chain epoch does not exist.
    #[error("chain epoch {chain_epoch:?} was not found")]
    ChainEpochMissing {
        /// Missing chain epoch id.
        chain_epoch: ChainEpochId,
    },

    /// No visible chain epoch has been committed yet.
    #[error("no visible chain epoch has been committed")]
    NoVisibleChainEpoch,

    /// A non-empty store has no trustworthy canonical-history bounds.
    #[error("canonical history bounds are missing from a non-empty store")]
    CanonicalHistoryBoundsMissing,

    /// Requested history was intentionally omitted by checkpoint bootstrap.
    #[error(
        "canonical history at height {requested_height:?} is unavailable before retained height {first_available_height:?}"
    )]
    CanonicalHistoryUnavailable {
        /// Requested canonical block height.
        requested_height: BlockHeight,
        /// First height at which canonical artifacts are retained.
        first_available_height: BlockHeight,
        /// Checkpoint immediately preceding retained history.
        checkpoint: BlockId,
    },

    /// A bounded artifact range exceeds the store's block-count limit.
    #[error(
        "{family:?} range requests {requested_block_count} blocks, exceeding the maximum {maximum_block_count}"
    )]
    ArtifactRangeTooLarge {
        /// Artifact family requested by the caller.
        family: ArtifactFamily,
        /// Block count requested by the caller.
        requested_block_count: NonZeroU32,
        /// Maximum block count accepted by the store.
        maximum_block_count: NonZeroU32,
    },

    /// Attempted chain epoch conflicts with the current visible epoch.
    #[error("chain epoch conflict: current {current:?}, attempted {attempted:?}")]
    ChainEpochConflict {
        /// Current visible chain epoch id.
        current: ChainEpochId,
        /// Attempted chain epoch id.
        attempted: ChainEpochId,
    },

    /// Attempted commit belongs to a different network than the current store.
    #[error("chain epoch network mismatch: current {current:?}, attempted {attempted:?}")]
    ChainEpochNetworkMismatch {
        /// Current store network.
        current: Network,
        /// Attempted commit network.
        attempted: Network,
    },

    /// Displaced-block history cursor does not identify an archived ordering row.
    #[error("displaced block cursor is invalid")]
    DisplacedBlockCursorInvalid,

    /// Persisted store schema version does not match the binary's expected version.
    ///
    /// Operators must run an explicit migration that produces a store at the
    /// expected schema version before retrying. The binary refuses to silently
    /// upgrade or downgrade canonical state.
    #[error(
        "store schema mismatch: persisted version {persisted_version}, expected {expected_version}"
    )]
    SchemaMismatch {
        /// Schema version recorded on disk.
        persisted_version: u16,
        /// Schema version expected by the running binary.
        expected_version: u16,
    },

    /// A populated canonical store has no durable schema and network metadata.
    #[error(
        "canonical store metadata is missing from a non-empty store; use a fresh store path and rebuild from a certified recovery source"
    )]
    StoreMetadataMissing,

    /// Persisted metadata declares the current schema but a required column family is absent.
    #[error("store schema is incomplete: missing column family {missing_column_family}")]
    StoreSchemaIncomplete {
        /// Required column family not present in the `RocksDB` manifest.
        missing_column_family: &'static str,
    },

    /// Persisted artifact schema is newer than this binary supports.
    #[error(
        "store artifact schema is too new: persisted {persisted_version}, supported {supported_version}"
    )]
    SchemaTooNew {
        /// Artifact schema version recorded on disk.
        persisted_version: u16,
        /// Highest artifact schema version supported by the running binary.
        supported_version: u16,
    },

    /// Persisted artifact schema is older than this binary requires.
    ///
    /// Wipe the store and resync with the schema required by this binary.
    #[error(
        "store artifact schema is too old: persisted {persisted_version}, required {required_version}; wipe the store and resync"
    )]
    SchemaTooOld {
        /// Artifact schema version recorded on disk.
        persisted_version: u16,
        /// Artifact schema version this binary requires.
        required_version: u16,
    },

    /// Another primary process already owns the `RocksDB` lock.
    #[error("primary store is already open: {lock_path:?}")]
    PrimaryAlreadyOpen {
        /// `RocksDB` lock path for operator diagnostics.
        lock_path: PathBuf,
    },

    /// A `RocksDB` secondary failed to catch up with its primary.
    #[error("secondary catchup failed")]
    SecondaryCatchupFailed {
        /// Underlying `RocksDB` error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },

    /// `RocksDB` checkpoint creation failed.
    #[error("checkpoint at {path:?} is unavailable")]
    CheckpointUnavailable {
        /// Requested checkpoint path.
        path: PathBuf,
        /// Underlying `RocksDB` or filesystem error.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },

    /// Attempted replacement crossed the configured reorg boundary.
    #[error(
        "reorg window exceeded: attempted from {attempted_from_height:?}, minimum allowed {minimum_reorg_height:?}, settled tip {settled_tip_height:?}"
    )]
    ReorgWindowExceeded {
        /// First height requested for replacement.
        attempted_from_height: BlockHeight,
        /// Earliest height that may be replaced.
        minimum_reorg_height: BlockHeight,
        /// Current settled tip height (boundary above which reorgs are still possible).
        settled_tip_height: BlockHeight,
    },

    /// Chain event cursor points before retained chain-event history.
    #[error(
        "chain event cursor expired: event sequence {event_sequence}, oldest retained {oldest_retained_sequence}"
    )]
    ChainEventCursorExpired {
        /// Cursor event sequence.
        event_sequence: u64,
        /// Oldest retained chain event sequence.
        oldest_retained_sequence: u64,
    },

    /// Chain event cursor failed validation.
    #[error("chain event cursor is invalid: {reason}")]
    ChainEventCursorInvalid {
        /// Cursor validation failure reason.
        reason: &'static str,
    },

    /// Transparent output cursor failed validation.
    #[error("transparent output cursor is invalid: {reason}")]
    AddressOutputCursorInvalid {
        /// Cursor validation failure reason.
        reason: &'static str,
    },

    /// Mempool event cursor points before retained mempool-event history.
    #[error(
        "mempool event cursor expired: event sequence {event_sequence}, oldest retained {oldest_retained_sequence}"
    )]
    MempoolEventCursorExpired {
        /// Cursor event sequence.
        event_sequence: u64,
        /// Oldest retained mempool event sequence.
        oldest_retained_sequence: u64,
    },

    /// Mempool event cursor failed validation.
    #[error("mempool event cursor is invalid: {reason}")]
    MempoolEventCursorInvalid {
        /// Cursor validation failure reason.
        reason: &'static str,
    },

    /// Mempool snapshot paging cursor failed validation.
    #[error("mempool snapshot page cursor is invalid: {reason}")]
    SnapshotPageCursorInvalid {
        /// Cursor validation failure reason.
        reason: &'static str,
    },

    /// Mempool snapshot paging cursor is anchored ahead of the mempool-event
    /// sequence the writer has applied.
    #[error(
        "mempool snapshot page cursor expired: cursor anchor sequence {anchor_event_sequence}, current {current_event_sequence}"
    )]
    SnapshotPageCursorExpired {
        /// Anchor mempool-event sequence carried by the cursor.
        anchor_event_sequence: u64,
        /// Mempool-event sequence the writer has applied.
        current_event_sequence: u64,
    },

    /// Chain event sequence reached the maximum representable value.
    #[error("chain event sequence overflow")]
    ChainEventSequenceOverflow,

    /// Mempool event sequence reached the maximum representable value.
    #[error("mempool event sequence overflow")]
    MempoolEventSequenceOverflow,

    /// Chain epoch id reached the maximum representable value.
    #[error("chain epoch sequence overflow")]
    ChainEpochSequenceOverflow,

    /// Commit value failed domain validation before a durable write.
    #[error("invalid chain epoch artifacts: {reason}")]
    InvalidChainEpochArtifacts {
        /// Validation failure reason.
        reason: &'static str,
    },

    /// Artifact payload is too large for the v1 storage envelope.
    #[error("artifact payload in {family:?} is too large: {payload_len} bytes")]
    ArtifactPayloadTooLarge {
        /// Artifact family that could not be encoded.
        family: ArtifactFamily,
        /// Payload byte length.
        payload_len: usize,
    },

    /// Store options are invalid.
    #[error("invalid chain store options: {reason}")]
    InvalidChainStoreOptions {
        /// Validation failure reason.
        reason: &'static str,
    },

    /// Configured raw-blob retention differs from a non-empty store's contract.
    #[error(
        "raw blob retention mismatch: store is {persisted}, configured {configured}; rebuild the canonical store to change retention"
    )]
    RawBlobRetentionMismatch {
        /// Retention contract persisted before the first canonical commit.
        persisted: RawBlobRetention,
        /// Retention requested by the current primary writer.
        configured: RawBlobRetention,
    },

    /// Artifact required by an epoch-bound read is missing.
    #[error("missing artifact in {family:?} for key {key:?}")]
    ArtifactMissing {
        /// Missing artifact family.
        family: ArtifactFamily,
        /// Missing artifact key.
        key: StorageKey,
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

    /// Requested feature is not implemented by this storage backend.
    #[error("unsupported storage feature: {feature}")]
    Unsupported {
        /// Unsupported feature name.
        feature: &'static str,
    },
}
