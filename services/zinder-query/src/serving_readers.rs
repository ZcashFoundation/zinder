//! Engine-neutral canonical and wallet reads at one admitted fence.
//!
//! A serving runtime reads stored chain data only through [`CanonicalReader`]
//! and [`WalletProjectionReader`]. Their methods answer the questions the
//! serving runtimes ask, such as a byte-bounded compact-block range or the
//! spend state of a batch of outpoints, so a request that needs several
//! lookups makes one call. No signature names a storage engine: reads fail
//! with [`StorageReadError`], and an implementation keeps blocking storage
//! work off the async runtime itself.
//!
//! Every value the interfaces carry can be built by any engine from what its
//! writer stored, such as a fence with [`CanonicalEventFence::new`], a
//! schedule with [`CanonicalActivationSchedule::first_revision`], and a
//! pending reconstruction with [`TreeStateReplay::new`].
//!
//! A caller that already holds a blocking thread, and chooses what to read
//! only once it does, drives a read on that thread with
//! [`read_on_blocking_thread`].

use std::{cell::Cell, error::Error as StdError, num::NonZeroU16, num::NonZeroU32};

use async_trait::async_trait;
use thiserror::Error;
use zinder_core::{
    BlockBlobArtifact, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange,
    CanonicalHistoryBounds, ChainEpoch, CompactBlockArtifact, Network, SubtreeRootArtifact,
    SubtreeRootRange, TransactionBlobArtifact, TransactionId, TransactionLocation,
    TransparentAddressScriptHash, TransparentOutPoint, TransparentOutpointSpendStatus,
};
use zinder_store::{
    ArtifactFamily, BlockHashLookup, CanonicalActivationSchedule, CanonicalEventFence,
    ChainEventEnvelope, ChainEventStreamFamily, ChainEventStreamResume, EventStreamStartPosition,
    HeightOrder, RawBlobRetention, ServedTreeState, StreamCursorTokenV1, TreeStateLookup,
    TreeStateReplay, TreeStateReplayOutcome,
};
use zinder_wallet_projection::{
    WalletAddressTransactionHistoryPage, WalletAddressTransactionKey,
    WalletAddressUnspentOutputKey, WalletAddressUnspentOutputsPage, WalletProjectionReadyEvidence,
};

use crate::ArtifactKey;

/// Canonical chain data read at one immutable admitted fence.
///
/// The accessors return what the reader admitted with its fence and never
/// read storage. Every other method reads storage and keeps that work off
/// the async runtime. An implementation never moves its fence: a caller that
/// holds the reader reads the same chain epoch for as long as it holds it.
#[async_trait]
pub trait CanonicalReader: Send + Sync + 'static {
    /// Returns the network admitted with this reader.
    fn network(&self) -> Network;

    /// Returns the network-upgrade schedule the canonical writer admitted
    /// with this fence, or `None` before the writer's first admission.
    fn network_upgrade_schedule(&self) -> Option<&CanonicalActivationSchedule>;

    /// Returns the raw-blob retention the store was built with.
    fn raw_blob_retention(&self) -> RawBlobRetention;

    /// Returns the exact admitted canonical event fence.
    fn event_fence(&self) -> CanonicalEventFence;

    /// Returns the chain epoch visible at the admitted fence.
    fn chain_epoch(&self) -> ChainEpoch;

    /// Returns the canonical history range the store retains.
    fn history_bounds(&self) -> CanonicalHistoryBounds;

    /// Reads the canonical header at `height`, or `None` above the visible
    /// tip or below retained history.
    async fn block_header_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError>;

    /// Resolves `block_hash` through the canonical best-chain index.
    async fn block_hash_lookup(
        &self,
        block_hash: BlockHash,
    ) -> Result<BlockHashLookup, StorageReadError>;

    /// Reads the header of the visible best-chain block named by
    /// `block_hash`, resolving the hash and reading the header in one read.
    ///
    /// `None` when no block of the visible best chain has that hash, such as
    /// a block a reorg displaced.
    async fn block_header_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<BlockHeaderArtifact>, StorageReadError>;

    /// Reads the compact block of the visible best-chain block named by
    /// `block_hash`, resolving the hash and reading the block in one read.
    ///
    /// `None` when no block of the visible best chain has that hash. A
    /// best-chain block whose compact block the store does not hold fails
    /// with [`StorageReadError::ArtifactUnavailable`] for
    /// [`ArtifactFamily::CompactBlock`] at its height, so a caller can tell
    /// a missing block from an unknown hash.
    async fn compact_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError>;

    /// Reads the compact block at `height`, or `None` above the visible tip
    /// or below retained history.
    async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CompactBlockArtifact>, StorageReadError>;

    /// Reads every compact block of an inclusive range in ascending height
    /// order.
    ///
    /// A range that reaches below retained history or above the visible tip
    /// fails with [`StorageReadError::ArtifactUnavailable`] naming the
    /// lowest such height.
    async fn compact_blocks_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError>;

    /// Reads compact blocks from the end of `range` that `order` visits
    /// first, until the next block would take their stored bytes past
    /// `max_bytes`.
    ///
    /// The read always keeps one block, so a block larger than `max_bytes`
    /// comes back alone. The blocks come back in ascending height order and
    /// cover consecutive heights at the starting end. The whole of `range`
    /// is checked as [`Self::compact_blocks_in_range`] checks it, even where
    /// the budget would stop the read first.
    async fn compact_blocks_in_range_up_to(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
    ) -> Result<Vec<CompactBlockArtifact>, StorageReadError>;

    /// Reads the retained raw block at `height`, or `None` when the store
    /// does not hold it.
    async fn block_blob_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<BlockBlobArtifact>, StorageReadError>;

    /// Reads the retained raw blocks of an inclusive range, one entry per
    /// height in ascending order, `None` where the store does not hold it.
    async fn block_blobs_in_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Vec<Option<BlockBlobArtifact>>, StorageReadError>;

    /// Reads the mined transaction `transaction_id` names, with the header
    /// of its block and its raw bytes when the store retains them.
    ///
    /// `None` is a proven absence from the canonical chain from height 1
    /// through the visible tip. A store whose retained history starts after
    /// a checkpoint cannot prove an absence, so it answers a miss with
    /// [`StorageReadError::CanonicalHistoryUnavailable`].
    async fn mined_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Option<IndexedTransaction>, StorageReadError>;

    /// Reads the raw bytes of the mined transaction `transaction_id` names.
    ///
    /// Fails with [`StorageReadError::ArtifactUnavailable`] for
    /// [`ArtifactFamily::TransactionLocation`] when no canonical location
    /// names the transaction, and for [`ArtifactFamily::TransactionBlob`]
    /// when the store does not retain its bytes.
    async fn transaction_blob_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<TransactionBlobArtifact, StorageReadError>;

    /// Reads the newest tree-state checkpoint stored at or below `height`.
    async fn tree_state_checkpoint_at_or_before(
        &self,
        height: BlockHeight,
    ) -> Result<Option<ServedTreeState>, StorageReadError>;

    /// Finds how the tree state at `height` is answered under the admitted
    /// network-upgrade schedule: by the checkpoint stored there, or by a
    /// reconstruction [`Self::replay_tree_state`] runs.
    ///
    /// `None` for a height below retained history or above the visible tip.
    async fn tree_state_lookup(
        &self,
        height: BlockHeight,
    ) -> Result<Option<TreeStateLookup>, StorageReadError>;

    /// Reconstructs the tree state `replay` names from stored compact
    /// blocks, or answers from a checkpoint this reader stores at its
    /// height, and reports which.
    ///
    /// A reader replays from a seed it read itself. It replays a seed as
    /// given only when its own engine's lookup issued `replay` and recorded
    /// the seed's provenance, and reads its own seed at the height for any
    /// other, such as one built with [`TreeStateReplay::new`], which proves
    /// nothing about its seed. The outcome names the seed it replayed.
    async fn replay_tree_state(
        &self,
        replay: TreeStateReplay,
    ) -> Result<Option<TreeStateReplayOutcome>, StorageReadError>;

    /// Reads a contiguous range of the subtree roots the visible chain has
    /// completed, which its chain epoch's tip metadata counts.
    ///
    /// A completed root the store does not hold, because its retained
    /// history starts after the block that completed it, fails with
    /// [`StorageReadError::ArtifactUnavailable`] for
    /// [`ArtifactFamily::SubtreeRoot`].
    async fn subtree_roots(
        &self,
        range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, StorageReadError>;

    /// Reads at most `max_events` retained wallet chain events after
    /// `from_cursor`, or from the oldest retained event of `family`.
    ///
    /// A cursor records its family, which applies to the events after it
    /// whatever `family` names. [`ChainEventStreamFamily::Settled`] carries
    /// only a `ChainCommitted` event after which the visible tip is at or
    /// below the settled tip.
    async fn wallet_chain_event_history(
        &self,
        from_cursor: Option<StreamCursorTokenV1>,
        family: ChainEventStreamFamily,
        max_events: NonZeroU32,
    ) -> Result<Vec<ChainEventEnvelope>, StorageReadError>;

    /// Resolves where a wallet chain-event subscription starting at `start`
    /// begins.
    ///
    /// After a cursor the subscription reads the cursor's family. The
    /// default `requested_family`, [`ChainEventStreamFamily::Visible`],
    /// resumes a cursor of either family, and any other must match the
    /// cursor's or fails with [`StorageReadError::ChainEventCursorInvalid`].
    async fn resolve_wallet_chain_event_stream_start(
        &self,
        start: EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StorageReadError>;
}

/// Wallet projection state read at one immutable READY source identity.
///
/// The accessors return what the reader admitted and never read storage.
/// Every other method reads storage and keeps that work off the async
/// runtime. An implementation never follows the projector: a caller that
/// holds the reader reads the same source for as long as it holds it.
#[async_trait]
pub trait WalletProjectionReader: Send + Sync + 'static {
    /// Returns the network admitted with this reader.
    fn network(&self) -> Network;

    /// Returns the READY evidence this reader admitted.
    fn ready_evidence(&self) -> &WalletProjectionReadyEvidence;

    /// Reads at most `page_size` of the address's unspent outputs created at
    /// or above `start_height`, after the `after` key, in ascending
    /// `(height, outpoint)` order.
    ///
    /// `next_page_after` is set exactly when more outputs may follow.
    async fn address_unspent_outputs_page_from_height(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
        after: Option<WalletAddressUnspentOutputKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressUnspentOutputsPage, StorageReadError>;

    /// Reads at most `page_size` of the transactions touching the address
    /// within an inclusive height range, after the `after` key, in ascending
    /// `(height, transaction index)` order.
    ///
    /// `next_page_after` is set exactly when more transactions may follow.
    async fn address_transaction_history_range_page(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        height_range: BlockHeightRange,
        after: Option<WalletAddressTransactionKey>,
        page_size: NonZeroU16,
    ) -> Result<WalletAddressTransactionHistoryPage, StorageReadError>;

    /// Sums the confirmed balances of `addresses`, counting an address with
    /// no balance as zero and saturating at `u64::MAX`.
    ///
    /// The caller removes duplicates: an address listed twice counts twice.
    async fn total_address_balance(
        &self,
        addresses: Vec<TransparentAddressScriptHash>,
    ) -> Result<u64, StorageReadError>;

    /// Reads how the projection holds each outpoint, one status per outpoint
    /// in request order: unspent, spent by a mined input, or `NotFound` when
    /// the projection holds no output there.
    ///
    /// The projection moves an output from unspent to spent in the
    /// transition that mines its spend, and back only when a reorg reverts
    /// that spend, so each outpoint has exactly one status. `NotFound`
    /// proves an absence only when the canonical history the projection
    /// covers starts at height 1, which the caller checks. A caller that
    /// cannot prove an absence fails the request at the first `NotFound`, so
    /// it passes [`SpendStatusReadEnd::FirstNotFound`] and the read reads no
    /// outpoint after it.
    async fn outpoint_spend_statuses(
        &self,
        outpoints: Vec<TransparentOutPoint>,
        end: SpendStatusReadEnd,
    ) -> Result<Vec<TransparentOutpointSpendStatus>, StorageReadError>;
}

/// Where a [`WalletProjectionReader::outpoint_spend_statuses`] read ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpendStatusReadEnd {
    /// The read classifies every requested outpoint.
    AllOutpoints,
    /// The read ends with the first `NotFound` status, which is then its
    /// last status, and reads no outpoint after it.
    FirstNotFound,
}

/// A mined transaction as the canonical store indexes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedTransaction {
    /// Canonical location of the transaction.
    pub location: TransactionLocation,
    /// Header of the block that mines the transaction. `None` only when the
    /// store holds the location without its block's header.
    pub block_header: Option<BlockHeaderArtifact>,
    /// Raw transaction bytes, or `None` when the store does not retain them.
    pub transaction_blob: Option<TransactionBlobArtifact>,
}

/// Why a canonical or wallet read returned no value.
///
/// The first variants are outcomes a request can cause, and each keeps the
/// public status of the matching [`QueryError`](crate::QueryError) variant.
/// The others report that storage could not answer, which serving reports
/// as `STORAGE_UNAVAILABLE`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageReadError {
    /// The read named an artifact the reader does not hold.
    #[error("{family:?} artifact is unavailable for {key}")]
    ArtifactUnavailable {
        /// Artifact family requested.
        family: ArtifactFamily,
        /// Requested lookup key.
        key: ArtifactKey,
    },

    /// The reader's retained history starts after a checkpoint, so a lookup
    /// that missed cannot prove its subject absent.
    #[error(
        "canonical history starts at height {}; absence cannot be proven",
        .first_available_height.value()
    )]
    CanonicalHistoryUnavailable {
        /// First height the store retains.
        first_available_height: BlockHeight,
    },

    /// A chain-event cursor points before retained event history.
    #[error(
        "chain-event cursor expired: event sequence {event_sequence}, oldest retained {oldest_retained_sequence}"
    )]
    ChainEventCursorExpired {
        /// Cursor event sequence.
        event_sequence: u64,
        /// Oldest retained event sequence.
        oldest_retained_sequence: u64,
    },

    /// A chain-event cursor failed validation.
    #[error("chain-event cursor is invalid: {reason}")]
    ChainEventCursorInvalid {
        /// Cursor validation failure reason.
        reason: &'static str,
    },

    /// The reader's view of storage was invalidated under it, so this reader
    /// cannot answer, while a reader admitted afresh at the same fence can.
    ///
    /// The `RocksDB` reader reports a table file that the writer's compaction
    /// deleted under its secondary.
    #[error(transparent)]
    ReaderInvalidated {
        /// Engine-specific failure.
        source: Box<dyn StdError + Send + Sync>,
    },

    /// The task that ran the read failed before it answered, such as by
    /// panicking or because the runtime shut down.
    #[error("query read task failed: {reason}")]
    ReadTaskFailed {
        /// Operator-facing failure reason.
        reason: String,
    },

    /// Storage failed to answer.
    #[error(transparent)]
    Unavailable {
        /// Engine-specific failure.
        source: Box<dyn StdError + Send + Sync>,
    },
}

thread_local! {
    /// Whether [`read_on_blocking_thread`] drives a read on this thread.
    static DRIVING_BLOCKING_READ: Cell<bool> = const { Cell::new(false) };
}

/// Runs `read`, a read of a [`CanonicalReader`] or a
/// [`WalletProjectionReader`], to completion on the calling thread.
///
/// The calling thread must be one that may block inside a Tokio runtime,
/// such as one running a `tokio::task::spawn_blocking` closure. A reader that
/// moves its blocking storage work off the async runtime runs that work on
/// this thread instead, so the read waits for no other thread. A caller that
/// decides what to read only once it holds a blocking thread reads this way,
/// such as a compact-blocks range chunk that reads whichever reader is
/// current when its thread starts and releases it before it encodes.
///
/// # Panics
///
/// Panics outside a Tokio runtime or on a thread that runs asynchronous
/// tasks, as [`tokio::runtime::Handle::block_on`] does.
pub fn read_on_blocking_thread<Read: Future>(read: Read) -> Read::Output {
    let _driving = DrivingBlockingRead::enter();
    tokio::runtime::Handle::current().block_on(read)
}

/// Reports whether [`read_on_blocking_thread`] drives the read polling on
/// this thread, so a reader runs its blocking storage work here.
pub(crate) fn drives_blocking_read() -> bool {
    DRIVING_BLOCKING_READ.get()
}

/// Marks the thread as driving a read until dropped, then restores the mark
/// it found, including when the read panics.
struct DrivingBlockingRead {
    previous: bool,
}

impl DrivingBlockingRead {
    fn enter() -> Self {
        Self {
            previous: DRIVING_BLOCKING_READ.replace(true),
        }
    }
}

impl Drop for DrivingBlockingRead {
    fn drop(&mut self) {
        DRIVING_BLOCKING_READ.set(self.previous);
    }
}
