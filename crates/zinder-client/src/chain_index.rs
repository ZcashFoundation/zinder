//! Public chain-index trait and consumer-facing domain types.

use std::{num::NonZeroU32, pin::Pin, sync::Arc};

use crate::IndexerError;
#[cfg(feature = "remote")]
use crate::ServerInfo;
use async_trait::async_trait;
use tokio_stream::Stream;
use zinder_core::{
    BlockBlobArtifact, BlockHeader, BlockHeight, BlockHeightRange, BlockId, BlockSelector,
    ChainEpoch, ChainEpochId, CompactBlockArtifact, NetworkUpgradeActivations, SubtreeRootArtifact,
    SubtreeRootRange, TransactionId, TransparentAddressBalance, TransparentAddressScriptHash,
    TransparentAddressTxIndexArtifact, TransparentOutPoint, TransparentOutputsByOutpointResponse,
    TransparentSpendsByOutpointResponse, TransparentUnspentOutput,
    TransparentUnspentOutputsByOutpointResponse, TransparentUtxoSetCommitment, TreeStateArtifact,
    TxStatus,
};
#[cfg(feature = "remote")]
use zinder_core::{
    BlockHash, ChainValuePoolsAtTip, MempoolEntry, MempoolEvictionReason, RawTransactionBytes,
    TransactionBroadcastOutcome, TransparentMempoolOutput, TransparentMempoolOutputsRequest,
    TransparentMempoolSpend,
};

/// Chain-event stream selected by a subscription.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(feature = "remote")]
pub enum ChainEventStreamFamily {
    /// Every visible chain transition, including reorgs.
    Visible,
    /// Non-reorg commits that are entirely at or below the settled tip.
    Settled,
}

/// Typed stream returned by chain-index methods.
pub type IndexStream<T> = Pin<Box<dyn Stream<Item = Result<T, IndexerError>> + Send + 'static>>;

/// Chain-event stream returned by [`EndpointBackedIndex::chain_events`].
#[cfg(feature = "remote")]
pub type ChainEventStream = IndexStream<ChainEventEnvelope>;

/// Explicit start position for a resumable event-stream subscription.
///
/// Mirrors the wire `EventStreamStart` oneof. `AfterCursor` resumes strictly
/// after an opaque cursor from a previously delivered envelope: it is the
/// reconnect path once at least one event has been applied. A fresh
/// subscription chooses `EarliestRetained` to replay the retention window or
/// `LiveTail` to resolve once at subscribe time to the current stream head,
/// receiving only events applied after subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "remote")]
pub enum EventStreamStart<Cursor> {
    /// Resume strictly after this cursor; its encoded family is
    /// authoritative.
    AfterCursor(Cursor),
    /// Replay from the earliest retained event.
    EarliestRetained,
    /// Start at the stream head resolved at subscribe time.
    LiveTail,
}

/// Opaque chain-event cursor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg(feature = "remote")]
pub struct ChainEventCursor(Vec<u8>);

#[cfg(feature = "remote")]
impl ChainEventCursor {
    /// Creates a cursor from bytes previously returned by Zinder.
    #[must_use]
    pub fn from_bytes(cursor_bytes: impl Into<Vec<u8>>) -> Self {
        Self(cursor_bytes.into())
    }

    /// Returns the opaque cursor bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Cursor-bound chain event returned to Rust consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "remote")]
pub struct ChainEventEnvelope {
    /// Cursor for resuming strictly after this event.
    pub cursor: ChainEventCursor,
    /// Monotonic sequence in this event stream.
    pub event_sequence: u64,
    /// Chain epoch visible after this event.
    pub chain_epoch: ChainEpoch,
    /// Settled tip height reported with this event.
    pub settled_tip_height: BlockHeight,
    /// Canonical chain transition.
    pub event: ChainEvent,
}

/// Canonical chain transition carried by [`ChainEventEnvelope`].
///
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[cfg(feature = "remote")]
pub enum ChainEvent {
    /// A non-reorg commit advanced the canonical tip or settled-tip prefix.
    ChainCommitted {
        /// Committed epoch payload.
        committed: ChainEpochCommitted,
    },
    /// A range that had not yet reached the settled tip was replaced.
    ChainReorged {
        /// Previously visible range invalidated by this transition.
        reverted: ChainRangeReverted,
        /// Replacement range committed by this transition.
        committed: ChainEpochCommitted,
    },
}

#[cfg(feature = "remote")]
impl ChainEvent {
    /// Returns `true` when this event is the [`Self::ChainCommitted`] variant.
    ///
    /// Convenience for consumers that need a single boolean for tip-change
    /// notifications without `match`-ing on every variant.
    #[must_use]
    pub const fn is_chain_committed(&self) -> bool {
        matches!(self, Self::ChainCommitted { .. })
    }
}

/// Durable range committed by one chain event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(feature = "remote")]
pub struct ChainEpochCommitted {
    /// Chain epoch visible after the commit.
    pub chain_epoch: ChainEpoch,
    /// Inclusive block range included in the commit.
    pub block_range: BlockHeightRange,
}

/// Durable range reverted by one chain event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg(feature = "remote")]
pub struct ChainRangeReverted {
    /// Chain epoch that contained the reverted range.
    pub chain_epoch: ChainEpoch,
    /// Inclusive block range invalidated by this transition.
    pub block_range: BlockHeightRange,
}

/// Opaque mempool-event cursor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg(feature = "remote")]
pub struct MempoolEventCursor(Vec<u8>);

#[cfg(feature = "remote")]
impl MempoolEventCursor {
    /// Creates a cursor from bytes previously returned by Zinder.
    #[must_use]
    pub fn from_bytes(cursor_bytes: impl Into<Vec<u8>>) -> Self {
        Self(cursor_bytes.into())
    }

    /// Returns the opaque cursor bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Bounded snapshot request for [`EndpointBackedIndex::mempool_snapshot`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg(feature = "remote")]
pub struct MempoolSnapshotRequest {
    /// Server-enforced maximum entry count.
    pub max_entries: u32,
    /// Optional next-page cursor returned by a previous paged snapshot.
    pub from_cursor: Option<MempoolSnapshotCursor>,
}

/// Opaque next-page cursor for paged mempool snapshots.
///
/// Pass the cursor returned in one snapshot page to the next request. The
/// cursor binds the durable event-resume anchor and the last transaction id in
/// that page; callers must treat its bytes as opaque.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg(feature = "remote")]
pub struct MempoolSnapshotCursor(Vec<u8>);

#[cfg(feature = "remote")]
impl MempoolSnapshotCursor {
    /// Creates a snapshot cursor from bytes returned by Zinder.
    #[must_use]
    pub fn from_bytes(cursor_bytes: impl Into<Vec<u8>>) -> Self {
        Self(cursor_bytes.into())
    }

    /// Returns the opaque cursor bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Snapshot view returned by [`EndpointBackedIndex::mempool_snapshot`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "remote")]
pub struct MempoolSnapshotView {
    /// Chain epoch visible at snapshot time.
    pub chain_epoch: ChainEpoch,
    /// Upstream source tip under which the mempool generation was certified.
    pub source_tip: BlockId,
    /// `MempoolEvents` after-cursor anchored at the moment the snapshot walk
    /// began; identical on every page of one paged walk. `None` when the
    /// server had applied no mempool event yet, in which case a consumer
    /// subscribes with [`EventStreamStart::EarliestRetained`]. Replaying from
    /// it yields at-least-once delivery; consumers apply events idempotently.
    pub events_resume_cursor: Option<MempoolEventCursor>,
    /// Milliseconds since the current source generation's snapshot was certified.
    pub snapshot_age_millis: u64,
    /// Hydrated mempool entries.
    pub entries: Vec<MempoolEntry>,
    /// Next-page cursor when the response was truncated.
    pub next_cursor: Option<MempoolSnapshotCursor>,
}

/// Cursor-bound mempool source-event delivered to resumable consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "remote")]
pub struct MempoolEventEnvelope {
    /// Cursor for resuming strictly after this event.
    pub cursor: MempoolEventCursor,
    /// Monotonic sequence in the mempool-event stream. Independent from
    /// the chain-event sequence space.
    pub event_sequence: u64,
    /// Wall-clock time when the indexer observed the source change.
    pub source_observed_unix_millis: u64,
    /// Source transition observed by the indexer.
    pub event: MempoolEvent,
}

/// Mempool source transition delivered to consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[allow(
    clippy::large_enum_variant,
    reason = "Added carries the full hydrated MempoolEntry by design; consumers replay state from the event log without a follow-up snapshot call."
)]
#[cfg(feature = "remote")]
pub enum MempoolEvent {
    /// Mempool transaction observed.
    Added {
        /// Hydrated entry observed by the indexer.
        entry: MempoolEntry,
    },
    /// Mempool transaction removed without being mined.
    Invalidated {
        /// Identifier of the invalidated transaction.
        transaction_id: TransactionId,
        /// Source-classified eviction reason.
        reason: MempoolEvictionReason,
    },
    /// Mempool transaction observed mined into a block.
    Mined {
        /// Identifier of the mined transaction.
        transaction_id: TransactionId,
        /// Height at which the source observed the mining.
        mined_height: BlockHeight,
        /// Hash of the block that mined the transaction, as observed by the
        /// source. Lifecycle consumers can resolve mined block identity
        /// without a follow-up tip read.
        block_hash: BlockHash,
    },
}

/// Mempool-event stream returned by [`EndpointBackedIndex::mempool_events`].
#[cfg(feature = "remote")]
pub type MempoolEventStream = IndexStream<MempoolEventEnvelope>;

/// Read parameters for [`ChainIndex::transparent_address_unspent_outputs`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressUnspentOutputsQuery {
    /// SHA-256 of the transparent address scriptPubKey.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Wallet-birthday floor: minimum mined height to include.
    pub start_height: BlockHeight,
    /// Optional chain-epoch pin. `None` resolves against the live visible
    /// tip; `Some(id)` pins the unspent read to that epoch.
    pub at_epoch_id: Option<ChainEpochId>,
}

/// Stream of unspent transparent outputs returned by
/// [`ChainIndex::transparent_address_unspent_outputs`]. The stream always
/// carries the complete unspent set at one pinned chain epoch.
pub type TransparentAddressUnspentOutputsStream = IndexStream<TransparentUnspentOutputChunk>;

/// Opaque transparent-address tx-history cursor bound to one visible-chain fence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TransparentHistoryCursor(Vec<u8>);

impl TransparentHistoryCursor {
    /// Creates a cursor from bytes previously returned by Zinder.
    #[must_use]
    pub fn from_bytes(cursor_bytes: impl Into<Vec<u8>>) -> Self {
        Self(cursor_bytes.into())
    }

    /// Returns the opaque cursor bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Read parameters for [`ChainIndex::transparent_address_tx_ids_in_range`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressTxIdsQuery {
    /// SHA-256 of the transparent address scriptPubKey.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Inclusive minimum block height.
    pub start_height: BlockHeight,
    /// Inclusive maximum block height.
    pub end_height: BlockHeight,
    /// Server-bounded entry cap. `None` defers to the server default.
    pub max_entries: Option<NonZeroU32>,
    /// Optional cursor returned by a previous read. The cursor is valid only
    /// while the visible-chain event fence is unchanged.
    pub from_cursor: Option<TransparentHistoryCursor>,
    /// Iterate newest-first when true.
    pub descending: bool,
    /// Optional expected response epoch. `None` accepts the server's current
    /// epoch; `Some(id)` rejects a response header from any other epoch.
    pub at_epoch_id: Option<ChainEpochId>,
}

/// One streamed tx-history chunk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressTransactionChunk {
    /// Chain epoch used to answer this chunk.
    pub chain_epoch: ChainEpoch,
    /// Indexed tx-history artifact.
    pub artifact: TransparentAddressTxIndexArtifact,
    /// Resume cursor on the last chunk when more entries may be available.
    pub cursor: Option<TransparentHistoryCursor>,
}

/// Stream of tx-history chunks returned by
/// [`ChainIndex::transparent_address_tx_ids_in_range`].
pub type TransparentAddressTxIdsStream = IndexStream<TransparentAddressTransactionChunk>;

/// One streamed unspent transparent output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentUnspentOutputChunk {
    /// Chain epoch pinned for the entire stream.
    pub chain_epoch: ChainEpoch,
    /// Single unspent output.
    pub output: TransparentUnspentOutput,
}

/// Chain-wide transparent UTXO-set summary returned to a client.
///
/// `utxo_count` and `total_value_zat` are the order-independent set totals at
/// `summarized_height`. `commitment` is present only when the serving
/// deployment advertises `wallet.read.transparent_utxo_set_commitment_v1`;
/// absence is `None`, not an error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentUtxoSetSummaryView {
    /// Chain epoch the summary was taken at.
    pub chain_epoch: ChainEpoch,
    /// Settled tip height the aggregate was taken at.
    pub summarized_height: BlockHeight,
    /// Number of unspent transparent outputs at the settled tip.
    pub utxo_count: u64,
    /// Sum of the values of every unspent transparent output, in zatoshi.
    pub total_value_zat: u64,
    /// Homomorphic commitment to the full unspent set, when advertised.
    pub commitment: Option<TransparentUtxoSetCommitment>,
}

impl TransparentUtxoSetSummaryView {
    /// Returns true only when both summaries carry a commitment under the same
    /// scheme.
    ///
    /// Two commitments are comparable only when their schemes match: a scheme
    /// mismatch (or either side absent) means not-comparable, never diverged.
    /// Callers that get `true` may then compare the commitment bytes to detect
    /// genuine divergence at the same scheme and epoch.
    #[must_use]
    pub fn comparable_with(&self, other: &Self) -> bool {
        match (self.commitment.as_ref(), other.commitment.as_ref()) {
            (Some(left), Some(right)) => left.scheme() == right.scheme(),
            _ => false,
        }
    }
}

/// Borrowed chain view pinned to one [`ChainEpoch`].
///
/// Capture resolves [`ChainIndex::current_epoch`] exactly once. Canonical
/// requests forward that epoch id; address-history streams validate their
/// mandatory response header against it. A multi-call operation either
/// remains on one chain view or receives
/// [`IndexerError::ChainEpochPinUnavailable`] after the serving implementation
/// stops retaining that epoch.
pub struct ChainSnapshot<'a, I: ChainIndex + ?Sized> {
    chain_index: &'a I,
    chain_epoch: ChainEpoch,
}

impl<I: ChainIndex + ?Sized> Copy for ChainSnapshot<'_, I> {}

impl<I: ChainIndex + ?Sized> Clone for ChainSnapshot<'_, I> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, I: ChainIndex + ?Sized> ChainSnapshot<'a, I> {
    /// Captures the chain index's current visible epoch exactly once.
    pub async fn capture(chain_index: &'a I) -> Result<Self, IndexerError> {
        let chain_epoch = chain_index.current_epoch().await?;
        Ok(Self {
            chain_index,
            chain_epoch,
        })
    }

    /// Returns the epoch used by every canonical read on this snapshot.
    #[must_use]
    pub const fn chain_epoch(&self) -> ChainEpoch {
        self.chain_epoch
    }

    /// Returns the captured epoch's visible-tip block identity.
    pub async fn visible_tip_block(&self) -> Result<BlockId, IndexerError> {
        self.chain_index
            .visible_tip_block(Some(self.chain_epoch.id))
            .await
    }

    /// Returns the captured epoch's settled-tip block identity.
    pub async fn settled_tip_block(&self) -> Result<BlockId, IndexerError> {
        self.chain_index
            .settled_tip_block(Some(self.chain_epoch.id))
            .await
    }

    /// Resolves a block selector against the captured canonical epoch.
    pub async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
    ) -> Result<BlockId, IndexerError> {
        self.chain_index
            .block_id_by_selector(selector, Some(self.chain_epoch.id))
            .await
    }

    /// Returns a typed block header from the captured canonical epoch.
    pub async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
    ) -> Result<BlockHeader, IndexerError> {
        self.chain_index
            .block_header_by_selector(selector, Some(self.chain_epoch.id))
            .await
    }

    /// Reads one compact block from the captured canonical epoch.
    pub async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<CompactBlockArtifact, IndexerError> {
        self.chain_index
            .compact_block_at(height, Some(self.chain_epoch.id))
            .await
    }

    /// Streams compact blocks from the captured canonical epoch.
    pub async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<IndexStream<CompactBlockArtifact>, IndexerError> {
        self.chain_index
            .compact_blocks_in_range(block_range, Some(self.chain_epoch.id))
            .await
    }

    /// Requests one retained full block from the captured canonical epoch.
    ///
    /// Availability depends on the implementation's advertised full-block
    /// capability.
    pub async fn full_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<BlockBlobArtifact, IndexerError> {
        self.chain_index
            .full_block_at(height, Some(self.chain_epoch.id))
            .await
    }

    /// Requests retained full blocks from the captured canonical epoch.
    ///
    /// Availability depends on the implementation's advertised full-block
    /// range capability.
    pub async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<IndexStream<BlockBlobArtifact>, IndexerError> {
        self.chain_index
            .full_blocks_in_range(block_range, Some(self.chain_epoch.id))
            .await
    }

    /// Reads the tree state at one height from the captured canonical epoch.
    pub async fn tree_state_at(
        &self,
        height: BlockHeight,
    ) -> Result<TreeStateArtifact, IndexerError> {
        self.chain_index
            .tree_state_at(height, Some(self.chain_epoch.id))
            .await
    }

    /// Reads the latest tree-state checkpoint from the captured canonical
    /// epoch.
    pub async fn latest_tree_state_checkpoint(&self) -> Result<TreeStateArtifact, IndexerError> {
        self.chain_index
            .latest_tree_state_checkpoint(Some(self.chain_epoch.id))
            .await
    }

    /// Reads a bounded subtree-root range from the captured canonical epoch.
    pub async fn subtree_roots_in_range(
        &self,
        subtree_root_range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, IndexerError> {
        self.chain_index
            .subtree_roots_in_range(subtree_root_range, Some(self.chain_epoch.id))
            .await
    }

    /// Looks up a canonical transaction without consulting live mempool state.
    ///
    /// [`TxStatus::NotFound`] means the transaction is absent from the
    /// captured epoch's canonical chain, from height 1 through its visible
    /// tip. See [`ChainIndex::transaction_by_id`] for the errors that are
    /// never reported as an absence.
    pub async fn transaction_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<TxStatus, IndexerError> {
        self.chain_index
            .transaction_by_id(transaction_id, Some(self.chain_epoch.id))
            .await
    }

    /// Streams one address's unspent outputs from the captured canonical
    /// epoch.
    pub async fn transparent_address_unspent_outputs(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
    ) -> Result<TransparentAddressUnspentOutputsStream, IndexerError> {
        self.chain_index
            .transparent_address_unspent_outputs(TransparentAddressUnspentOutputsQuery {
                address_script_hash,
                start_height,
                at_epoch_id: Some(self.chain_epoch.id),
            })
            .await
    }

    /// Streams address transaction history from the captured epoch.
    ///
    /// The snapshot replaces `query.at_epoch_id` with its captured epoch so
    /// the mandatory stream header makes a replaced serving pair fail closed.
    pub async fn transparent_address_tx_ids_in_range(
        &self,
        mut query: TransparentAddressTxIdsQuery,
    ) -> Result<TransparentAddressTxIdsStream, IndexerError> {
        query.at_epoch_id = Some(self.chain_epoch.id);
        self.chain_index
            .transparent_address_tx_ids_in_range(query)
            .await
    }

    /// Requests canonical transparent outputs from the captured epoch when the
    /// implementation advertises the corresponding capability.
    pub async fn transparent_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<TransparentOutputsByOutpointResponse, IndexerError> {
        self.chain_index
            .transparent_outputs_by_outpoint(outpoints, Some(self.chain_epoch.id))
            .await
    }

    /// Requests canonical transparent spends from the captured epoch when the
    /// implementation advertises the corresponding capability.
    pub async fn transparent_spends_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<TransparentSpendsByOutpointResponse, IndexerError> {
        self.chain_index
            .transparent_spends_by_outpoint(outpoints, Some(self.chain_epoch.id))
            .await
    }

    /// Requests canonical unspent transparent outputs from the captured epoch
    /// when the implementation advertises the corresponding capability.
    pub async fn transparent_unspent_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<TransparentUnspentOutputsByOutpointResponse, IndexerError> {
        self.chain_index
            .transparent_unspent_outputs_by_outpoint(outpoints, Some(self.chain_epoch.id))
            .await
    }

    /// Returns the transparent UTXO-set summary from the captured epoch.
    pub async fn transparent_utxo_set_summary(
        &self,
    ) -> Result<TransparentUtxoSetSummaryView, IndexerError> {
        self.chain_index
            .transparent_utxo_set_summary(Some(self.chain_epoch.id))
            .await
    }
}

/// Owned chain view pinned to one [`ChainEpoch`].
///
/// This companion to [`ChainSnapshot`] is suitable for consumer APIs that
/// must retain a cloneable, `'static` chain view. It owns an [`Arc`] to the
/// index while preserving the same canonical-only epoch-pinning behavior.
pub struct OwnedChainSnapshot<I: ChainIndex + ?Sized> {
    chain_index: Arc<I>,
    chain_epoch: ChainEpoch,
}

impl<I: ChainIndex + ?Sized> Clone for OwnedChainSnapshot<I> {
    fn clone(&self) -> Self {
        Self {
            chain_index: Arc::clone(&self.chain_index),
            chain_epoch: self.chain_epoch,
        }
    }
}

impl<I: ChainIndex + ?Sized> OwnedChainSnapshot<I> {
    /// Captures the shared chain index's current visible epoch exactly once.
    pub async fn capture(chain_index: Arc<I>) -> Result<Self, IndexerError> {
        let chain_epoch = chain_index.current_epoch().await?;
        Ok(Self {
            chain_index,
            chain_epoch,
        })
    }

    /// Returns the epoch used by every canonical read on this snapshot.
    #[must_use]
    pub const fn chain_epoch(&self) -> ChainEpoch {
        self.chain_epoch
    }

    fn borrowed(&self) -> ChainSnapshot<'_, I> {
        ChainSnapshot {
            chain_index: self.chain_index.as_ref(),
            chain_epoch: self.chain_epoch,
        }
    }

    /// Returns the captured epoch's visible-tip block identity.
    pub async fn visible_tip_block(&self) -> Result<BlockId, IndexerError> {
        self.borrowed().visible_tip_block().await
    }

    /// Returns the captured epoch's settled-tip block identity.
    pub async fn settled_tip_block(&self) -> Result<BlockId, IndexerError> {
        self.borrowed().settled_tip_block().await
    }

    /// Resolves a block selector against the captured canonical epoch.
    pub async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
    ) -> Result<BlockId, IndexerError> {
        self.borrowed().block_id_by_selector(selector).await
    }

    /// Returns a typed block header from the captured canonical epoch.
    pub async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
    ) -> Result<BlockHeader, IndexerError> {
        self.borrowed().block_header_by_selector(selector).await
    }

    /// Reads one compact block from the captured canonical epoch.
    pub async fn compact_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<CompactBlockArtifact, IndexerError> {
        self.borrowed().compact_block_at(height).await
    }

    /// Streams compact blocks from the captured canonical epoch.
    pub async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<IndexStream<CompactBlockArtifact>, IndexerError> {
        self.borrowed().compact_blocks_in_range(block_range).await
    }

    /// Requests one retained full block from the captured canonical epoch.
    ///
    /// Availability depends on the implementation's advertised full-block
    /// capability.
    pub async fn full_block_at(
        &self,
        height: BlockHeight,
    ) -> Result<BlockBlobArtifact, IndexerError> {
        self.borrowed().full_block_at(height).await
    }

    /// Requests retained full blocks from the captured canonical epoch.
    ///
    /// Availability depends on the implementation's advertised full-block
    /// range capability.
    pub async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
    ) -> Result<IndexStream<BlockBlobArtifact>, IndexerError> {
        self.borrowed().full_blocks_in_range(block_range).await
    }

    /// Reads the tree state at one height from the captured canonical epoch.
    pub async fn tree_state_at(
        &self,
        height: BlockHeight,
    ) -> Result<TreeStateArtifact, IndexerError> {
        self.borrowed().tree_state_at(height).await
    }

    /// Reads the latest tree-state checkpoint from the captured epoch.
    pub async fn latest_tree_state_checkpoint(&self) -> Result<TreeStateArtifact, IndexerError> {
        self.borrowed().latest_tree_state_checkpoint().await
    }

    /// Reads a bounded subtree-root range from the captured canonical epoch.
    pub async fn subtree_roots_in_range(
        &self,
        subtree_root_range: SubtreeRootRange,
    ) -> Result<Vec<SubtreeRootArtifact>, IndexerError> {
        self.borrowed()
            .subtree_roots_in_range(subtree_root_range)
            .await
    }

    /// Looks up a canonical transaction without consulting live mempool state.
    pub async fn transaction_by_id(
        &self,
        transaction_id: TransactionId,
    ) -> Result<TxStatus, IndexerError> {
        self.borrowed().transaction_by_id(transaction_id).await
    }

    /// Streams one address's unspent outputs from the captured epoch.
    pub async fn transparent_address_unspent_outputs(
        &self,
        address_script_hash: TransparentAddressScriptHash,
        start_height: BlockHeight,
    ) -> Result<TransparentAddressUnspentOutputsStream, IndexerError> {
        self.borrowed()
            .transparent_address_unspent_outputs(address_script_hash, start_height)
            .await
    }

    /// Streams address transaction history from the captured epoch.
    pub async fn transparent_address_tx_ids_in_range(
        &self,
        query: TransparentAddressTxIdsQuery,
    ) -> Result<TransparentAddressTxIdsStream, IndexerError> {
        self.borrowed()
            .transparent_address_tx_ids_in_range(query)
            .await
    }

    /// Requests canonical transparent outputs from the captured epoch when the
    /// implementation advertises the corresponding capability.
    pub async fn transparent_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<TransparentOutputsByOutpointResponse, IndexerError> {
        self.borrowed()
            .transparent_outputs_by_outpoint(outpoints)
            .await
    }

    /// Requests canonical transparent spends from the captured epoch when the
    /// implementation advertises the corresponding capability.
    pub async fn transparent_spends_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<TransparentSpendsByOutpointResponse, IndexerError> {
        self.borrowed()
            .transparent_spends_by_outpoint(outpoints)
            .await
    }

    /// Requests canonical unspent transparent outputs from the captured epoch
    /// when the implementation advertises the corresponding capability.
    pub async fn transparent_unspent_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<TransparentUnspentOutputsByOutpointResponse, IndexerError> {
        self.borrowed()
            .transparent_unspent_outputs_by_outpoint(outpoints)
            .await
    }

    /// Returns the transparent UTXO-set summary from the captured epoch.
    pub async fn transparent_utxo_set_summary(
        &self,
    ) -> Result<TransparentUtxoSetSummaryView, IndexerError> {
        self.borrowed().transparent_utxo_set_summary().await
    }
}

/// Typed network metadata plus canonical and wallet-projection reads.
///
/// The public [`crate::RemoteChainIndex`] implements this trait over the native
/// `WalletQuery` endpoint. Callers preflight the deployment's advertised
/// capabilities before depending on optional reads.
///
/// Methods that require live endpoint-owned collaborators (broadcast,
/// live-mempool reads, the chain-event stream, source-backed chain value-pools,
/// and the wallet-plane server descriptor) live on the separate
/// `EndpointBackedIndex` trait, which only the remote client
/// implements. A caller that needs one of those methods adds an
/// `EndpointBackedIndex` bound, so a handle without an endpoint fails to
/// compile instead of failing at runtime.
///
/// Canonical reads take `at_epoch_id: Option<ChainEpochId>`. `None` resolves
/// to the visible chain epoch at call time; `Some(id)` pins the read to that
/// epoch. Address history uses the field as an expected response epoch and
/// fails when the mandatory stream header names another epoch.
///
/// All trait methods take and return `zinder-core` types; generated
/// `zinder_proto::*` types appear only in adapter modules, never on this
/// public Rust API.
#[async_trait]
pub trait ChainIndex: Send + Sync + 'static {
    /// Returns the current visible chain epoch.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let epoch = client.current_epoch().await?;
    /// # let _ = epoch; Ok(()) }
    /// ```
    async fn current_epoch(&self) -> Result<ChainEpoch, IndexerError>;

    /// Returns the immutable network-upgrade activation table advertised by
    /// this index.
    ///
    /// This metadata describes the configured network rather than a specific
    /// chain epoch, so callers discover it directly from the index instead of
    /// through a snapshot.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let activations = client.network_upgrade_activations().await?;
    /// # let _ = activations; Ok(()) }
    /// ```
    async fn network_upgrade_activations(&self) -> Result<NetworkUpgradeActivations, IndexerError>;

    /// Captures a borrowed canonical-chain view pinned to the current epoch.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let snapshot = client.snapshot().await?;
    /// let tip = snapshot.visible_tip_block().await?;
    /// # let _ = tip; Ok(()) }
    /// ```
    async fn snapshot(&self) -> Result<ChainSnapshot<'_, Self>, IndexerError>
    where
        Self: Sized,
    {
        ChainSnapshot::capture(self).await
    }

    /// Returns the visible-tip block identity.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let block = client.visible_tip_block(None).await?;
    /// # let _ = block; Ok(()) }
    /// ```
    async fn visible_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockId, IndexerError>;

    /// Resolves the block at the chain epoch's settled finality watermark.
    /// Wallets still scan through the pinned epoch's visible tip so their
    /// commitment-tree state matches the chain tip passed to the wallet database.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let block = client.settled_tip_block(None).await?;
    /// # let _ = block; Ok(()) }
    /// ```
    async fn settled_tip_block(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockId, IndexerError>;

    /// Resolves a block selector against the canonical best chain.
    ///
    /// Hash-only callers pass the `Hash` arm; height-only callers pass the
    /// `Height` arm and receive a normalized [`BlockId`] with the resolved
    /// hash. Returns [`IndexerError::NotFound`] when the selector addresses
    /// a block that is not visible at the request's chain epoch (reorged
    /// out or never indexed).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{BlockHeight, BlockSelector, ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let selector = BlockSelector::from_height(BlockHeight::new(0));
    /// let block = client.block_id_by_selector(selector, None).await?;
    /// # let _ = block; Ok(()) }
    /// ```
    async fn block_id_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockId, IndexerError>;

    /// Returns the typed block-header read model for a block selector.
    ///
    /// The Zinder header shape is independent of the lightwalletd compact
    /// header and the upstream node's JSON-RPC `getblockheader` shape.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{BlockHeight, BlockSelector, ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let selector = BlockSelector::from_height(BlockHeight::new(0));
    /// let header = client.block_header_by_selector(selector, None).await?;
    /// # let _ = header; Ok(()) }
    /// ```
    async fn block_header_by_selector(
        &self,
        selector: BlockSelector,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockHeader, IndexerError>;

    /// Reads one compact block artifact.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{BlockHeight, ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let block = client.compact_block_at(BlockHeight::new(0), None).await?;
    /// # let _ = block; Ok(()) }
    /// ```
    async fn compact_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<CompactBlockArtifact, IndexerError>;

    /// Streams compact block artifacts for an inclusive range.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{BlockHeight, BlockHeightRange, ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let range = BlockHeightRange::inclusive(BlockHeight::new(0), BlockHeight::new(0));
    /// let stream = client.compact_blocks_in_range(range, None).await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    async fn compact_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<IndexStream<CompactBlockArtifact>, IndexerError>;

    /// Reads one full serialized block.
    ///
    /// This is an optional protocol method. A serving implementation also
    /// requires retained block blobs (`raw_blob_policy = "all"`); heights with
    /// no retained blob return [`IndexerError::ArtifactUnavailable`]. Remote
    /// callers must preflight `wallet.read.full_block_at_v1`; an admitted
    /// wallet-serving endpoint advertises it only for `all` retention.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{BlockHeight, ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let block = client.full_block_at(BlockHeight::new(0), None).await?;
    /// # let _ = block; Ok(()) }
    /// ```
    async fn full_block_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<BlockBlobArtifact, IndexerError>;

    /// Streams full serialized blocks for an inclusive range.
    ///
    /// This is an optional protocol method. A serving implementation also
    /// requires retained block blobs (`raw_blob_policy = "all"`); the stream
    /// errors on the first height with no retained blob. Remote callers must
    /// preflight `wallet.read.full_block_range_v1`; an admitted
    /// wallet-serving endpoint advertises it only for `all` retention.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{BlockHeight, BlockHeightRange, ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let range = BlockHeightRange::inclusive(BlockHeight::new(0), BlockHeight::new(0));
    /// let stream = client.full_blocks_in_range(range, None).await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    async fn full_blocks_in_range(
        &self,
        block_range: BlockHeightRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<IndexStream<BlockBlobArtifact>, IndexerError>;

    /// Reads the tree-state artifact at exactly `height`.
    ///
    /// `at_epoch_id = None` resolves to the live tip; `Some(id)` pins the
    /// read to that chain epoch. The returned artifact's height always equals
    /// `height`. `RemoteChainIndex` asks the query plane, which may fill a
    /// non-checkpoint height from its configured upstream node.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{BlockHeight, ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let tree = client.tree_state_at(BlockHeight::new(0), None).await?;
    /// # let _ = tree; Ok(()) }
    /// ```
    async fn tree_state_at(
        &self,
        height: BlockHeight,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeStateArtifact, IndexerError>;

    /// Reads the tree-state artifact at the visible tip.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let tree = client.latest_tree_state_checkpoint(None).await?;
    /// # let _ = tree; Ok(()) }
    /// ```
    async fn latest_tree_state_checkpoint(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TreeStateArtifact, IndexerError>;

    /// Reads subtree roots for a bounded range.
    ///
    /// `max_entries` must not exceed
    /// [`MAX_SUBTREE_ROOTS_PER_REQUEST`](crate::MAX_SUBTREE_ROOTS_PER_REQUEST).
    /// A remote endpoint reports larger requests as
    /// [`ErrorReason::SubtreeRootRangeTooLarge`](crate::ErrorReason::SubtreeRootRangeTooLarge)
    /// with [`RetryPolicy::ClientError`](crate::RetryPolicy::ClientError).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::num::NonZeroU32;
    /// # use zinder_client::{
    /// #     ChainIndex, IndexerError, ShieldedProtocol, SubtreeRootIndex, SubtreeRootRange,
    /// # };
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// # let Some(max_entries) = NonZeroU32::new(1) else { return Ok(()) };
    /// let range = SubtreeRootRange::new(
    ///     ShieldedProtocol::Sapling,
    ///     SubtreeRootIndex::new(0),
    ///     max_entries,
    /// );
    /// let roots = client.subtree_roots_in_range(range, None).await?;
    /// # let _ = roots; Ok(()) }
    /// ```
    async fn subtree_roots_in_range(
        &self,
        subtree_root_range: SubtreeRootRange,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<Vec<SubtreeRootArtifact>, IndexerError>;

    /// Looks up a transaction by id.
    ///
    /// `Some(id)` pins the read to that epoch's canonical chain and never
    /// consults mempool state. `None` reads the visible epoch's canonical
    /// chain and, on a miss, the writer's live mempool at that same epoch.
    ///
    /// [`TxStatus::NotFound`] is a proven absence: the server searched the
    /// canonical chain of the answering epoch from height 1 through its
    /// visible tip (and, without a pin, the live mempool at that epoch) and
    /// found nothing. A pinned absence is checked against the pin. Every
    /// other outcome is an error, never `NotFound`:
    ///
    /// - [`IndexerError::CanonicalHistoryUnavailable`]: the indexer's history
    ///   starts after a checkpoint, so it cannot prove an absence.
    /// - [`IndexerError::TransactionAbsenceUnverified`]: the server predates
    ///   the typed absence signal and its `NOT_FOUND` is ambiguous.
    /// - [`IndexerError::ChainEpochPinUnavailable`]: the pinned epoch is not
    ///   served.
    /// - Storage, missing-epoch, and dependency failures keep their own typed
    ///   variants, including [`IndexerError::ArtifactUnavailable`],
    ///   [`IndexerError::NotFound`] for a block outside the best chain, and
    ///   [`IndexerError::RemoteFailure`].
    ///
    /// To judge a transaction against a height, take a
    /// [`ChainSnapshot`](crate::ChainSnapshot) whose epoch's visible tip
    /// covers that height and look the transaction up through it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError, TransactionId};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let status = client
    ///     .transaction_by_id(TransactionId::from_bytes([0u8; 32]), None)
    ///     .await?;
    /// # let _ = status; Ok(()) }
    /// ```
    async fn transaction_by_id(
        &self,
        transaction_id: TransactionId,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TxStatus, IndexerError>;

    /// Streams the complete unspent transparent output set for one
    /// transparent address at a single pinned chain epoch.
    ///
    /// The stream cannot be truncated: there is no cursor and no entry
    /// cap. Client memory for a drained stream is proportional to the
    /// address's unspent set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{
    /// #     BlockHeight, ChainIndex, IndexerError, TransparentAddressScriptHash,
    /// #     TransparentAddressUnspentOutputsQuery,
    /// # };
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let query = TransparentAddressUnspentOutputsQuery {
    ///     address_script_hash: TransparentAddressScriptHash::from_bytes([0u8; 32]),
    ///     start_height: BlockHeight::new(0),
    ///     at_epoch_id: None,
    /// };
    /// let stream = client.transparent_address_unspent_outputs(query).await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    async fn transparent_address_unspent_outputs(
        &self,
        query: TransparentAddressUnspentOutputsQuery,
    ) -> Result<TransparentAddressUnspentOutputsStream, IndexerError>;

    /// Streams ascending transparent-address tx-history index entries.
    ///
    /// The returned rows and any resume cursor are bound to the response's
    /// visible chain epoch. `query.at_epoch_id` validates that header without
    /// adding a wire field. A reorg invalidates a prior resume cursor rather
    /// than resuming it against a different branch.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{
    /// #     BlockHeight, ChainIndex, IndexerError, TransparentAddressScriptHash,
    /// #     TransparentAddressTxIdsQuery,
    /// # };
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let query = TransparentAddressTxIdsQuery {
    ///     address_script_hash: TransparentAddressScriptHash::from_bytes([0u8; 32]),
    ///     start_height: BlockHeight::new(0),
    ///     end_height: BlockHeight::new(0),
    ///     max_entries: None,
    ///     from_cursor: None,
    ///     descending: false,
    ///     at_epoch_id: None,
    /// };
    /// let stream = client.transparent_address_tx_ids_in_range(query).await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    async fn transparent_address_tx_ids_in_range(
        &self,
        query: TransparentAddressTxIdsQuery,
    ) -> Result<TransparentAddressTxIdsStream, IndexerError>;

    /// Returns the transparent-address balance summed across `addresses`.
    ///
    /// The confirmed total is summed from the canonical unspent-output index.
    /// The signed `unconfirmed_delta_zat` overlays the live mempool state
    /// available to the serving endpoint.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError, TransparentAddressScriptHash};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let addresses = [TransparentAddressScriptHash::from_bytes([0u8; 32])];
    /// let balance = client.transparent_address_balance(&addresses).await?;
    /// # let _ = balance; Ok(()) }
    /// ```
    async fn transparent_address_balance(
        &self,
        addresses: &[TransparentAddressScriptHash],
    ) -> Result<TransparentAddressBalance, IndexerError>;

    /// Resolves a batch of canonical-chain transparent outpoints to their
    /// referenced outputs, in input order. Each entry's `prevout` is
    /// `None` when the canonical chain at the response's epoch does not
    /// have the referenced output.
    ///
    /// Implementations reject the coinbase sentinel and silently truncate
    /// requests above
    /// [`zinder_core::MAX_TRANSPARENT_OUTPUTS_PER_REQUEST`]. This is an
    /// optional protocol method; remote callers must preflight
    /// `wallet.read.transparent_outputs_by_outpoint_v1`. The current
    /// wallet-serving query does not advertise it until its admitted serving pair
    /// resolver is implemented.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError, TransactionId, TransparentOutPoint};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let outpoints = [TransparentOutPoint::new(TransactionId::from_bytes([0u8; 32]), 0)];
    /// let prevouts = client.transparent_outputs_by_outpoint(&outpoints, None).await?;
    /// # let _ = prevouts; Ok(()) }
    /// ```
    async fn transparent_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentOutputsByOutpointResponse, IndexerError>;

    /// Resolves a batch of canonical-chain transparent outpoints to where each
    /// was spent: the spending transaction, input index, and the block that
    /// mined the spend.
    ///
    /// Outpoints unspent on the canonical chain at the response's epoch produce
    /// no entry; consumers key results by `spent_outpoint`. Implementations
    /// reject the coinbase sentinel and silently truncate requests above
    /// [`zinder_core::MAX_TRANSPARENT_OUTPUTS_PER_REQUEST`]. This is the
    /// canonical half of a getspentinfo-equivalent lookup; the unmined half is
    /// the endpoint-backed `transparent_mempool_spends_by_outpoint` method.
    /// This is an optional protocol method; remote callers must preflight
    /// `wallet.read.transparent_spends_by_outpoint_v1`. The current
    /// wallet-serving query does not advertise it until its admitted serving pair
    /// resolver is implemented.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError, TransactionId, TransparentOutPoint};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let outpoints = [TransparentOutPoint::new(TransactionId::from_bytes([0u8; 32]), 0)];
    /// let spends = client.transparent_spends_by_outpoint(&outpoints, None).await?;
    /// # let _ = spends; Ok(()) }
    /// ```
    async fn transparent_spends_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentSpendsByOutpointResponse, IndexerError>;

    /// Resolves a batch of transparent outpoints to their referenced output,
    /// returning each only while it is unspent on the canonical chain at the
    /// response's epoch (gettxout-equivalent, null-if-spent).
    ///
    /// An outpoint produces an entry only when the canonical chain at that
    /// epoch has the output and it carries no canonical spend; spent or
    /// never-existed outpoints produce no entry, so every entry's `output` is
    /// present. Consumers key results by `outpoint`. Implementations reject the
    /// coinbase sentinel and silently truncate requests above
    /// [`zinder_core::MAX_TRANSPARENT_OUTPUTS_PER_REQUEST`]. The read is
    /// canonical-only: a mempool-aware caller subtracts the spends returned by
    /// the endpoint-backed `transparent_mempool_spends_by_outpoint` method.
    /// This is an optional protocol method; remote callers must preflight
    /// `wallet.read.transparent_unspent_outputs_by_outpoint_v1`. The current
    /// wallet-serving query does not advertise it until its admitted serving pair
    /// resolver is implemented.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError, TransactionId, TransparentOutPoint};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let outpoints = [TransparentOutPoint::new(TransactionId::from_bytes([0u8; 32]), 0)];
    /// let unspent = client.transparent_unspent_outputs_by_outpoint(&outpoints, None).await?;
    /// # let _ = unspent; Ok(()) }
    /// ```
    async fn transparent_unspent_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUnspentOutputsByOutpointResponse, IndexerError>;

    /// Aggregates the chain-wide transparent UTXO set at the settled tip.
    ///
    /// Returns the order-independent count and total value (the
    /// `gettxoutsetinfo`-equivalent), plus the homomorphic commitment when the
    /// serving deployment advertises
    /// `wallet.read.transparent_utxo_set_commitment_v1`. Absence of the
    /// commitment is `None`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{ChainIndex, IndexerError};
    /// # async fn demo<T: ChainIndex>(client: &T) -> Result<(), IndexerError> {
    /// let summary = client.transparent_utxo_set_summary(None).await?;
    /// # let _ = summary; Ok(()) }
    /// ```
    async fn transparent_utxo_set_summary(
        &self,
        at_epoch_id: Option<ChainEpochId>,
    ) -> Result<TransparentUtxoSetSummaryView, IndexerError>;
}

/// Live endpoint-backed operations layered on top of [`ChainIndex`].
///
/// Only [`crate::RemoteChainIndex`] implements this trait: every method needs a
/// live collaborator that the canonical and materialized-view stores cannot
/// stand in for. Broadcast and chain value-pools use the query's admitted
/// upstream source; the mempool reads observe the writer's in-process mempool
/// index through its private control endpoint; the chain-event stream comes
/// from the writer; and the wallet-plane server descriptor is the endpoint's
/// own advertisement.
///
/// A consumer that needs any of these methods bounds its handle as
/// `T: ChainIndex + EndpointBackedIndex`, so a handle whose type does not
/// provide endpoint operations is rejected at compile time rather than at
/// call time.
#[async_trait]
#[cfg(feature = "remote")]
pub trait EndpointBackedIndex: ChainIndex {
    /// Returns the wallet-plane server descriptor advertised by the endpoint.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{EndpointBackedIndex, IndexerError};
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let descriptor = client.server_info().await?;
    /// # let _ = descriptor; Ok(()) }
    /// ```
    async fn server_info(&self) -> Result<ServerInfo, IndexerError>;

    /// Reads chain-wide value-pool totals at the upstream node's current tip.
    ///
    /// The response is bound to the Zinder chain epoch visible when the writer
    /// answered the proxied source read. The response carries the source tip's
    /// height and hash so callers can verify the snapshot against a canonical
    /// chain identity. The upstream value-pool list is preserved as entries
    /// rather than projected into fixed pool names.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{EndpointBackedIndex, IndexerError};
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let pools = client.chain_value_pools_at_tip().await?;
    /// # let _ = pools; Ok(()) }
    /// ```
    async fn chain_value_pools_at_tip(&self) -> Result<ChainValuePoolsAtTip, IndexerError>;

    /// Broadcasts raw transaction bytes without mutating canonical storage.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{EndpointBackedIndex, IndexerError, RawTransactionBytes};
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let outcome = client
    ///     .broadcast_transaction(RawTransactionBytes::new(Vec::<u8>::new()))
    ///     .await?;
    /// # let _ = outcome; Ok(()) }
    /// ```
    async fn broadcast_transaction(
        &self,
        raw_transaction: RawTransactionBytes,
    ) -> Result<TransactionBroadcastOutcome, IndexerError>;

    /// Streams visible-family chain events.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{EndpointBackedIndex, EventStreamStart, IndexerError};
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let stream = client.chain_events(EventStreamStart::EarliestRetained).await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    async fn chain_events(
        &self,
        start: EventStreamStart<ChainEventCursor>,
    ) -> Result<ChainEventStream, IndexerError> {
        self.chain_events_for_family(start, ChainEventStreamFamily::Visible)
            .await
    }

    /// Streams chain events for the requested family.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{
    /// #     ChainEventStreamFamily, EndpointBackedIndex, EventStreamStart, IndexerError,
    /// # };
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let stream = client
    ///     .chain_events_for_family(EventStreamStart::LiveTail, ChainEventStreamFamily::Visible)
    ///     .await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    async fn chain_events_for_family(
        &self,
        start: EventStreamStart<ChainEventCursor>,
        family: ChainEventStreamFamily,
    ) -> Result<ChainEventStream, IndexerError>;

    /// Streams chain events with a server-side address invalidation filter
    /// applied.
    ///
    /// `address_filter` is a list of transparent t-addresses (canonical
    /// Base58 form). An empty list disables filtering and delivers every
    /// envelope. A non-empty list narrows commit envelopes to those whose
    /// block range touches at least one of the supplied addresses; reorgs
    /// always pass through regardless of filter. The cursor remains opaque;
    /// clients re-derive per-address state from `compact_block_at` after
    /// each received envelope.
    ///
    /// The default implementation falls back to
    /// [`Self::chain_events_for_family`] (filter ignored); the remote backend
    /// overrides it to push the filter through to the wire.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{
    /// #     ChainEventStreamFamily, EndpointBackedIndex, EventStreamStart, IndexerError,
    /// # };
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let stream = client
    ///     .chain_events_with_filter(
    ///         EventStreamStart::EarliestRetained,
    ///         ChainEventStreamFamily::Visible,
    ///         Vec::new(),
    ///     )
    ///     .await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    async fn chain_events_with_filter(
        &self,
        start: EventStreamStart<ChainEventCursor>,
        family: ChainEventStreamFamily,
        address_filter: Vec<String>,
    ) -> Result<ChainEventStream, IndexerError> {
        let _ = address_filter;
        self.chain_events_for_family(start, family).await
    }

    /// Returns a bounded snapshot of the live mempool index.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{EndpointBackedIndex, IndexerError, MempoolSnapshotRequest};
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let snapshot = client
    ///     .mempool_snapshot(MempoolSnapshotRequest::default())
    ///     .await?;
    /// # let _ = snapshot; Ok(()) }
    /// ```
    async fn mempool_snapshot(
        &self,
        request: MempoolSnapshotRequest,
    ) -> Result<MempoolSnapshotView, IndexerError>;

    /// Streams replayable mempool events.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{EndpointBackedIndex, EventStreamStart, IndexerError};
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let stream = client.mempool_events(EventStreamStart::LiveTail).await?;
    /// # let _ = stream; Ok(()) }
    /// ```
    async fn mempool_events(
        &self,
        start: EventStreamStart<MempoolEventCursor>,
    ) -> Result<MempoolEventStream, IndexerError>;

    /// Returns whether `transaction_id` is currently visible in the live
    /// mempool index.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{EndpointBackedIndex, IndexerError, TransactionId};
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let present = client
    ///     .is_in_mempool(TransactionId::from_bytes([0u8; 32]))
    ///     .await?;
    /// # let _ = present; Ok(()) }
    /// ```
    async fn is_in_mempool(&self, transaction_id: TransactionId) -> Result<bool, IndexerError>;

    /// Returns transparent mempool outputs tied to one address.
    ///
    /// Bounded by the request's `max_entries`; values larger than the
    /// server's configured cap are silently clamped to that cap.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{
    /// #     EndpointBackedIndex, IndexerError, TransparentAddressScriptHash,
    /// #     TransparentMempoolOutputsRequest,
    /// # };
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let request = TransparentMempoolOutputsRequest {
    ///     address_script_hash: TransparentAddressScriptHash::from_bytes([0u8; 32]),
    ///     max_entries: 64,
    /// };
    /// let outputs = client.transparent_mempool_outputs_by_address(request).await?;
    /// # let _ = outputs; Ok(()) }
    /// ```
    async fn transparent_mempool_outputs_by_address(
        &self,
        request: TransparentMempoolOutputsRequest,
    ) -> Result<Vec<TransparentMempoolOutput>, IndexerError>;

    /// Returns the mempool spends that consume any of `outpoints`.
    ///
    /// Outpoints with no unmined spend produce no entry; callers key
    /// results by `spent_outpoint`. Implementations silently truncate
    /// requests above
    /// [`zinder_core::MAX_TRANSPARENT_OUTPUTS_PER_REQUEST`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{
    /// #     EndpointBackedIndex, IndexerError, TransactionId, TransparentOutPoint,
    /// # };
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let outpoints = [TransparentOutPoint::new(TransactionId::from_bytes([0u8; 32]), 0)];
    /// let spends = client.transparent_mempool_spends_by_outpoint(&outpoints).await?;
    /// # let _ = spends; Ok(()) }
    /// ```
    async fn transparent_mempool_spends_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<Vec<TransparentMempoolSpend>, IndexerError>;

    /// Resolves a batch of outpoints against the live mempool index. Used
    /// when an outpoint references an output of an unconfirmed mempool
    /// transaction (chained-mempool flows).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use zinder_client::{
    /// #     EndpointBackedIndex, IndexerError, TransactionId, TransparentOutPoint,
    /// # };
    /// # async fn demo<T: EndpointBackedIndex>(client: &T) -> Result<(), IndexerError> {
    /// let outpoints = [TransparentOutPoint::new(TransactionId::from_bytes([0u8; 32]), 0)];
    /// let prevouts = client.transparent_mempool_outputs_by_outpoint(&outpoints).await?;
    /// # let _ = prevouts; Ok(()) }
    /// ```
    async fn transparent_mempool_outputs_by_outpoint(
        &self,
        outpoints: &[TransparentOutPoint],
    ) -> Result<TransparentOutputsByOutpointResponse, IndexerError>;
}
