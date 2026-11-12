//! Materialized-view consumer traits, typed event wrappers, and in-process dispatch helpers.
//!
//! Every materialized-view consumer implements [`MaterializedViewConsumer`]. The trait is the seam
//! between the consumer-agnostic infrastructure in this crate (store and
//! typed event wrappers) and the consumer-specific aggregation logic that
//! lives in each consumer module.
//!
//! A consumer that needs the parsed block in `apply_block` does NOT fetch
//! raw block data itself. `zinder-ingest` constructs a shared
//! [`BlockCommitContext`] from the canonical commit and passes it to every
//! consumer observing that height.

pub(crate) mod address_value_event;
pub(crate) mod block_commit_context;
pub(crate) mod block_production_time;
pub(crate) mod block_summary;
pub(crate) mod commitment_root_search;
pub(crate) mod conventional_fee_distribution;
pub(crate) mod ironwood_migration;
pub(crate) mod mempool_event_counts;
pub(crate) mod paid_fee_distribution;
pub(crate) mod recent_transactions;
pub(crate) mod reorg_incidents;
pub(crate) mod transaction_component_summary;
pub(crate) mod transaction_fees;
pub(crate) mod transaction_history;
pub(crate) mod transparent_address_activity;
pub(crate) mod transparent_address_deltas;
pub(crate) mod transparent_address_ranking;
pub(crate) mod transparent_outpoint_spend;
pub(crate) mod value_pool_balance_history;
pub(crate) mod value_pool_flow_history;

use std::collections::HashMap;

use rust_rocksdb::WriteBatch;
use zinder_core::{BlockHash, BlockHeight, ChainEpoch};
use zinder_store::ChainEvent;

pub use block_commit_context::{
    BlockCommitContext, BlockCommitInput, BlockValuePoolBalanceFacts,
    TransactionIntrinsicValueBalanceFacts, TransparentSpendFacts,
};
pub use commitment_root_search::{
    COMMITMENT_ROOT_SEARCH_COLUMN_FAMILIES, COMMITMENT_ROOT_SEARCH_COLUMN_FAMILY,
    COMMITMENT_ROOT_SEARCH_CONSUMER_NAME, COMMITMENT_ROOT_SEARCH_COVERAGE_COLUMN_FAMILY,
    COMMITMENT_ROOT_SEARCH_INDEX_COLUMN_FAMILY, COMMITMENT_ROOT_SEARCH_SCHEMA,
    CommitmentRootBackfillCoverage, CommitmentRootIndexEntry, CommitmentRootSearchConsumer,
    CommitmentRootSearchConsumerError,
};

use crate::store::{MaterializedViewCoverage, MaterializedViewStore};

/// Stable name of a materialized-view consumer used to scope cursor and metadata rows.
///
/// The name is part of the on-disk key in the materialized-view cursor column families;
/// renaming a consumer between releases is a schema migration, not a config
/// change. Names are short, lowercase, snake-case, and stable across binary
/// versions.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MaterializedViewConsumerName(&'static str);

impl MaterializedViewConsumerName {
    /// Creates a materialized-view consumer name from a static string.
    ///
    /// The caller must ensure the name is stable across releases; renaming a
    /// consumer between deployments orphans its persisted cursor.
    #[must_use]
    pub const fn from_static(name: &'static str) -> Self {
        Self(name)
    }

    /// Returns the underlying string value used in cursor and metadata keys.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl AsRef<[u8]> for MaterializedViewConsumerName {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// A materialized-view consumer's on-disk schema declaration.
///
/// One declaration binds a consumer's stable [`MaterializedViewConsumerName`] to the
/// version of its persisted row contract and the set of column families it
/// owns. The materialized-view store admits only the exact persisted
/// declaration set.
///
/// A version or column-family change requires a fresh materialized-view store
/// rebuilt from a certified recovery source. This keeps every reader on one
/// row encoding without mutating an existing store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MaterializedViewConsumerSchema {
    /// Stable consumer identity, shared with the consumer's cursor rows.
    pub name: MaterializedViewConsumerName,
    /// Version of this consumer's persisted row contract.
    pub schema_version: u16,
    /// Column families this consumer reads and writes.
    pub column_families: &'static [&'static str],
}

impl MaterializedViewConsumerSchema {
    /// Declares a consumer's schema from its name, version, and owned column
    /// families. Requiring the version here makes it impossible to register a
    /// column family without one.
    #[must_use]
    pub const fn new(
        name: MaterializedViewConsumerName,
        schema_version: u16,
        column_families: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            schema_version,
            column_families,
        }
    }
}

/// Boxed application error returned by consumer apply methods.
///
/// The SDK does not constrain how consumers report failures inside their
/// `apply_*` hooks: a consumer that fails to write to its own column family
/// returns a typed error whose shape it controls. The boxed form lets the
/// SDK surface that failure verbatim through [`crate::MaterializedViewError::Consumer`]
/// without coupling the SDK to any one consumer's error enum.
pub type MaterializedViewConsumerError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Per-event consumer context.
///
/// Carries a borrow of the [`MaterializedViewStore`] (for read-only lookups during
/// apply) and a borrow of the [`WriteBatch`] the SDK will commit. Consumers
/// stage their writes into the batch; the SDK appends the cursor advance to
/// the same batch and commits atomically. A crash between
/// `apply_chain_committed` and the commit therefore replays the event on next
/// startup; a crash after the commit advances both cursor and consumer state
/// together.
pub struct MaterializedViewConsumerCtx<'a> {
    /// Store the consumer reads from while applying events.
    pub store: &'a MaterializedViewStore,
    /// Write batch the consumer stages its data writes into.
    pub batch: &'a mut WriteBatch,
}

/// Materialized-view checkpoint derived from one block-consumer dispatch batch.
#[derive(Clone, Copy, Debug)]
pub struct MaterializedViewBlockCheckpoint<'event> {
    /// Canonical epoch carried by the chain event.
    pub chain_epoch: ChainEpoch,
    /// Event or replay chunk whose rows were staged.
    pub chain_event: &'event ChainEvent,
    /// Highest staged canonical height when every required block context was present.
    pub tip_height: Option<BlockHeight>,
    /// Canonical hash at [`Self::tip_height`].
    pub tip_hash: Option<BlockHash>,
}

/// Advances one consumer's verified contiguous coverage through a checkpoint.
///
/// `initial_complete_from` is supplied only by consumers whose event replay
/// itself proves their first covered height. Consumers that require a
/// separate historical verifier leave it `None` until that verifier seeds the
/// first range.
pub(crate) fn advance_verified_materialized_view_coverage(
    coverage: Option<MaterializedViewCoverage>,
    checkpoint: MaterializedViewBlockCheckpoint<'_>,
    tip_height: BlockHeight,
    tip_hash: BlockHash,
    initial_complete_from: Option<BlockHeight>,
) -> Option<MaterializedViewCoverage> {
    let Some(coverage) = coverage else {
        return initial_complete_from.map(|complete_from_height| MaterializedViewCoverage {
            complete_from_height,
            complete_through_height: tip_height,
            complete_through_hash: tip_hash,
        });
    };
    match checkpoint.chain_event {
        ChainEvent::ChainCommitted { committed } => {
            let range = committed.block_range;
            if range.start > range.end {
                return Some(coverage);
            }
            if coverage.complete_through_height.next() == Some(range.start) {
                return Some(MaterializedViewCoverage {
                    complete_from_height: coverage.complete_from_height,
                    complete_through_height: tip_height,
                    complete_through_hash: tip_hash,
                });
            }
            Some(coverage)
        }
        ChainEvent::ChainReorged {
            reverted,
            committed,
        } => {
            let replacement = committed.block_range;
            let reverted_start = reverted.block_range.start;
            let covers_reverted_boundary = coverage.complete_through_height >= reverted_start;
            let replacement_starts_at_reverted_boundary = replacement.start == reverted_start;
            if covers_reverted_boundary && replacement_starts_at_reverted_boundary {
                return Some(MaterializedViewCoverage {
                    complete_from_height: coverage.complete_from_height,
                    complete_through_height: tip_height,
                    complete_through_hash: tip_hash,
                });
            }
            if coverage.complete_through_height < reverted_start {
                return Some(coverage);
            }
            None
        }
        _ => Some(coverage),
    }
}

/// Typed wrapper for a `ChainCommitted` chain event delivered to a consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ChainCommittedEvent {
    /// Monotonic event sequence the SDK uses for cursor accounting.
    pub event_sequence: u64,
    /// Chain epoch visible after the commit.
    pub chain_epoch: ChainEpoch,
    /// Settled tip block height that was true at delivery time.
    pub settled_tip_height: BlockHeight,
    /// First committed block height (inclusive).
    pub start_height: BlockHeight,
    /// Last committed block height (inclusive).
    pub end_height: BlockHeight,
}

impl ChainCommittedEvent {
    /// Builds a chain-committed event from its component fields.
    #[must_use]
    pub const fn new(
        event_sequence: u64,
        chain_epoch: ChainEpoch,
        settled_tip_height: BlockHeight,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Self {
        Self {
            event_sequence,
            chain_epoch,
            settled_tip_height,
            start_height,
            end_height,
        }
    }
}

/// Typed wrapper for a `ChainReorged` chain event delivered to a consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ChainReorgedEvent {
    /// Monotonic event sequence the SDK uses for cursor accounting.
    pub event_sequence: u64,
    /// Chain epoch visible after the reorg replacement commits.
    pub chain_epoch: ChainEpoch,
    /// Settled tip block height that was true at delivery time.
    pub settled_tip_height: BlockHeight,
    /// Range invalidated by the reorg.
    pub reverted: RevertedRange,
    /// Replacement range committed by the reorg.
    pub replacement: CommittedRange,
}

impl ChainReorgedEvent {
    /// Builds a chain-reorged event from its component fields.
    #[must_use]
    pub const fn new(
        event_sequence: u64,
        chain_epoch: ChainEpoch,
        settled_tip_height: BlockHeight,
        reverted: RevertedRange,
        replacement: CommittedRange,
    ) -> Self {
        Self {
            event_sequence,
            chain_epoch,
            settled_tip_height,
            reverted,
            replacement,
        }
    }
}

/// Reverted block range carried by a [`ChainReorgedEvent`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RevertedRange {
    /// Chain epoch that contained the reverted range.
    pub chain_epoch: ChainEpoch,
    /// First reverted block height (inclusive).
    pub start_height: BlockHeight,
    /// Last reverted block height (inclusive).
    pub end_height: BlockHeight,
}

impl RevertedRange {
    /// Builds a reverted range from its component fields.
    #[must_use]
    pub const fn new(
        chain_epoch: ChainEpoch,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Self {
        Self {
            chain_epoch,
            start_height,
            end_height,
        }
    }
}

/// Committed block range carried by a [`ChainReorgedEvent`] or
/// [`ChainCommittedEvent`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CommittedRange {
    /// Chain epoch that contains the committed range.
    pub chain_epoch: ChainEpoch,
    /// First committed block height (inclusive).
    pub start_height: BlockHeight,
    /// Last committed block height (inclusive).
    pub end_height: BlockHeight,
}

impl CommittedRange {
    /// Builds a committed range from its component fields.
    #[must_use]
    pub const fn new(
        chain_epoch: ChainEpoch,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Self {
        Self {
            chain_epoch,
            start_height,
            end_height,
        }
    }
}

/// Trait every chain-events materialized-view consumer implements.
///
/// The SDK dispatcher calls [`apply_chain_committed`](Self::apply_chain_committed)
/// and [`apply_chain_reorged`](Self::apply_chain_reorged) per envelope.
/// Consumers stage their state writes through the
/// [`MaterializedViewConsumerCtx::batch`] handle so the SDK can commit consumer
/// writes and the cursor advance atomically.
///
/// Most production consumers implement [`BlockKeyedConsumer`] instead;
/// a blanket impl gives them the per-height range-loop on top of this
/// trait so they only write per-block logic, never the range scaffolding.
pub trait MaterializedViewConsumer: Send + Sync {
    /// Stable consumer identity used for cursor and metadata key prefixes.
    fn name(&self) -> MaterializedViewConsumerName;

    /// Apply a committed range. Stage state writes into `ctx.batch`; the SDK
    /// adds the cursor advance and commits atomically.
    fn apply_chain_committed(
        &mut self,
        event: &ChainCommittedEvent,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError>;

    /// Apply a reorged event. Stage state writes into `ctx.batch`; the SDK
    /// adds the cursor advance and commits atomically. Implementations
    /// decide how to revert their derived state for the reverted range and
    /// how to fold in the replacement range.
    fn apply_chain_reorged(
        &mut self,
        event: &ChainReorgedEvent,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError>;
}

/// Per-block materialized-view consumer.
///
/// The convention every production chain-events consumer follows. The
/// in-process dispatchers
/// ([`apply_chain_committed_in_memory`], [`apply_chain_reorged_in_memory`])
/// walk a height range and call [`apply_block`](Self::apply_block) /
/// [`revert_block`](Self::revert_block) per height with already-parsed
/// [`BlockCommitContext`] inputs the writer holds in memory.
///
/// A consumer that observes range boundaries (or implements something
/// other than "one block in, some rows out") implements [`MaterializedViewConsumer`]
/// directly instead.
pub trait BlockKeyedConsumer: Send + Sync {
    /// Stable consumer identity used for cursor and metadata key prefixes.
    fn name(&self) -> MaterializedViewConsumerName;

    /// Starts one atomic block batch.
    ///
    /// Most consumers stage independent per-block rows and use this default
    /// no-op. Consumers that maintain shared aggregate rows can reset their
    /// batch-local overlay here before the dispatcher invokes `apply_block`
    /// or `revert_block` more than once against the same `WriteBatch`.
    fn begin_batch(
        &mut self,
        _ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        Ok(())
    }

    /// Stages per-height writes derived from `block`. Implementations write
    /// into `ctx.batch`; the SDK appends the cursor advance and commits
    /// atomically.
    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError>;

    /// Stages per-height deletes to revert state previously written for
    /// `height`. Called once per reverted height by
    /// [`apply_chain_reorged_in_memory`].
    fn revert_block(
        &mut self,
        height: BlockHeight,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError>;

    /// Finishes one atomic block batch.
    ///
    /// Aggregate consumers stage their final shared rows here after every
    /// per-block mutation is known. The default is a no-op.
    fn finish_batch(
        &mut self,
        _ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        Ok(())
    }

    /// Stages event-aware materialized-view metadata in the same batch as rows.
    ///
    /// The default is a no-op. Consumers that publish coverage or read fences
    /// use this hook because [`Self::finish_batch`] deliberately has no chain
    /// event semantics.
    fn stage_chain_event_checkpoint(
        &mut self,
        _checkpoint: MaterializedViewBlockCheckpoint<'_>,
        _ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        Ok(())
    }
}

/// Dispatches a committed range against a [`BlockKeyedConsumer`] from
/// already-parsed block contexts.
///
/// Used when the consumer runs colocated with the writer that already has
/// the parsed block in memory. Each height in `event` must have a matching
/// entry in `blocks`; heights without entries are skipped so callers can
/// reuse the helper while assembling partial in-memory windows.
///
/// # Errors
///
/// Surfaces any [`MaterializedViewConsumerError`] the consumer's `apply_block`
/// returns. The caller bundles the SDK's cursor advance into `ctx.batch`
/// and commits the batch atomically.
pub fn apply_chain_committed_in_memory<C, S>(
    consumer: &mut C,
    event: &ChainCommittedEvent,
    ctx: &mut MaterializedViewConsumerCtx<'_>,
    blocks: &HashMap<BlockHeight, std::sync::Arc<BlockCommitContext>, S>,
) -> Result<(), MaterializedViewConsumerError>
where
    C: BlockKeyedConsumer + ?Sized,
    S: std::hash::BuildHasher,
{
    for raw_height in event.start_height.value()..=event.end_height.value() {
        let height = BlockHeight::new(raw_height);
        if let Some(context) = blocks.get(&height) {
            consumer.apply_block(context.as_ref(), ctx)?;
        }
    }
    Ok(())
}

/// Reverts a reorged range, then applies the replacement range from
/// already-parsed contexts; in-process counterpart of the gRPC blanket impl.
///
/// # Errors
///
/// Surfaces any [`MaterializedViewConsumerError`] the consumer's `revert_block` or
/// `apply_block` returns.
pub fn apply_chain_reorged_in_memory<C, S>(
    consumer: &mut C,
    event: &ChainReorgedEvent,
    ctx: &mut MaterializedViewConsumerCtx<'_>,
    replacement_blocks: &HashMap<BlockHeight, std::sync::Arc<BlockCommitContext>, S>,
) -> Result<(), MaterializedViewConsumerError>
where
    C: BlockKeyedConsumer + ?Sized,
    S: std::hash::BuildHasher,
{
    for raw_height in event.reverted.start_height.value()..=event.reverted.end_height.value() {
        consumer.revert_block(BlockHeight::new(raw_height), ctx)?;
    }
    for raw_height in event.replacement.start_height.value()..=event.replacement.end_height.value()
    {
        let height = BlockHeight::new(raw_height);
        if let Some(context) = replacement_blocks.get(&height) {
            consumer.apply_block(context.as_ref(), ctx)?;
        }
    }
    Ok(())
}

/// Mempool-event consumer trait.
///
/// Separate from [`MaterializedViewConsumer`] because mempool events have different
/// retention, ordering, and semantic content than chain events. A consumer
/// can implement both traits if it observes both streams; the explorer
/// transparent-balance handler implements neither because it reads canonical
/// UTXOs and live mempool point lookups at request time.
pub trait MaterializedViewMempoolConsumer: Send + Sync {
    /// Stable consumer identity used for cursor and metadata key prefixes.
    fn name(&self) -> MaterializedViewConsumerName;

    /// Apply a mempool event. Stage state writes into `ctx.batch`; the SDK
    /// adds the cursor advance and commits atomically.
    fn apply_mempool_event(
        &mut self,
        event: &MempoolConsumerEvent<'_>,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError>;
}

/// Typed wrapper for a single `MempoolEventEnvelope` delivered to a consumer.
#[derive(Debug)]
#[non_exhaustive]
pub struct MempoolConsumerEvent<'a> {
    /// Monotonic mempool-event sequence.
    pub event_sequence: u64,
    /// Wall-clock observation timestamp from the source.
    pub source_observed_unix_millis: u64,
    /// Variant payload borrowed from the wire envelope.
    pub variant: MempoolConsumerEventVariant<'a>,
}

impl<'a> MempoolConsumerEvent<'a> {
    /// Builds a mempool-consumer event from its sequence, timestamp, and
    /// typed payload.
    #[must_use]
    pub const fn new(
        event_sequence: u64,
        source_observed_unix_millis: u64,
        variant: MempoolConsumerEventVariant<'a>,
    ) -> Self {
        Self {
            event_sequence,
            source_observed_unix_millis,
            variant,
        }
    }
}

/// Typed payload variant carried by [`MempoolConsumerEvent`].
#[derive(Debug)]
#[non_exhaustive]
pub enum MempoolConsumerEventVariant<'a> {
    /// New mempool transaction observed by the source.
    Added {
        /// Transaction id observed by the source.
        transaction_id: &'a [u8],
        /// Hydrated raw transaction bytes (when the source provides them).
        raw_transaction_bytes: &'a [u8],
    },
    /// Mempool transaction removed without being mined.
    Invalidated {
        /// Transaction id of the invalidated transaction.
        transaction_id: &'a [u8],
    },
    /// Mempool transaction observed mined into a block.
    Mined {
        /// Transaction id of the mined transaction.
        transaction_id: &'a [u8],
        /// Height of the mining block.
        mined_height: BlockHeight,
        /// Hash of the mining block (32 bytes).
        block_hash: &'a [u8],
    },
}
