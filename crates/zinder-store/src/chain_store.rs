//! Chain store facade.

mod schema_admission;
mod validation;

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use parking_lot::RwLock;
use zinder_core::{
    BlockBlobArtifact, BlockFinalNoteCommitmentRoots, BlockHash, BlockHeaderArtifact, BlockHeight,
    BlockHeightRange, BlockId, BlockTransactionIndexArtifact, BlockValuePoolBalances,
    CanonicalHistoryBounds, ChainEpoch, ChainEpochId, CompactBlockArtifact, DisplacedBlock,
    DisplacedBlockArchiveCoverage, Network, SubtreeRootArtifact, TransactionBlobArtifact,
    TransactionFactsArtifact, TransactionId, TransactionIntrinsicValueBalancesArtifact,
    TransactionLocation, TransparentAddressScriptHash, TransparentOutPoint,
    TransparentOutputArtifact, TransparentSpendFact, TransparentUnspentOutput, TreeStateArtifact,
    UnixTimestampMillis,
};

use crate::displaced_block::{
    build_displaced_block_archive_puts_for_change, read_displaced_block_archive_coverage,
    read_displaced_block_by_hash, read_displaced_block_count, read_displaced_block_page,
    read_displaced_blocks_for_event, read_newest_displaced_blocks,
};
use crate::{
    ArtifactFamily, CURRENT_ARTIFACT_SCHEMA_VERSION, ChainEpochArtifacts, ChainEpochCommitOutcome,
    ChainEpochCommitted, ChainEpochReader, ChainEvent, ChainEventEnvelope,
    ChainEventHistoryRequest, ChainEventStreamResume, ChainRangeReverted, EventStreamStartPosition,
    MempoolEvent, MempoolEventEnvelope, MempoolEventHistoryRequest, MempoolEventPosition,
    MempoolEventRetentionConfig, MempoolEventRetentionReport, RawBlobRetention, ReorgWindowChange,
    RocksDbResourceBudget, StoreError, StreamCursorTokenV1,
    block_artifact::read_block_header_artifact,
    block_hash_index::block_hash_index_put,
    block_value_pool_balances::read_block_value_pool_balances,
    format::{
        CHAIN_EVENT_LOCATOR_MAX, ChainEventCursorAnchor, ChainEventLocator, ChainEventStreamFamily,
        MempoolEventKind, SnapshotPageCursorAnchor, SnapshotPageCursorPayload, StoreKey,
        decode_block_value_pool_balances, decode_canonical_history_bounds, decode_chain_epoch,
        decode_chain_event_envelope, decode_final_note_commitment_roots,
        decode_mempool_event_envelope, decode_mempool_event_kind, decode_mempool_event_observed_at,
        decode_mempool_event_position, decode_transaction_intrinsic_value_balances,
        decode_transparent_output_block_index, encode_address_output_index_artifact,
        encode_block_blob_artifact, encode_block_header_artifact,
        encode_block_transaction_index_artifact, encode_block_value_pool_balances,
        encode_canonical_history_bounds, encode_chain_epoch, encode_chain_event_envelope,
        encode_compact_block_artifact, encode_final_note_commitment_roots,
        encode_mempool_event_envelope, encode_subtree_root_artifact,
        encode_transaction_blob_artifact, encode_transaction_facts_artifact,
        encode_transaction_intrinsic_value_balances, encode_transaction_location_artifact,
        encode_transparent_output_artifact, encode_transparent_output_block_index,
        encode_transparent_spend_fact, encode_transparent_spend_fact_block_index,
        encode_tree_state_artifact,
    },
    kv::{
        PrefixScanControl, RocksChainStore, RocksChainStoreRead, StorageDelete, StoragePut,
        StorageTable, StoreReadCaller,
    },
    transaction_artifact::{
        read_transaction_intrinsic_value_balances, read_transaction_location,
        visible_transaction_source_epoch,
    },
    transparent_output::read_current_transparent_outputs_by_outpoints,
    transparent_spend_fact::{
        read_current_transparent_spend_fact_block_facts,
        read_visible_transparent_spend_fact_block_outpoints,
    },
};

use schema_admission::{preflight_primary_store_schema, validate_primary_store_schema};
use validation::{
    ValidatedBlockReplayOrder, committed_block_range, validate_chain_epoch_artifacts,
    validate_chain_store_options, validate_reorg_window_change, validate_retained_blob_artifacts,
    validate_value_pool_entries, validate_visible_chain_commit,
};

/// Runtime options for [`PrimaryChainStore`] and [`SecondaryChainStore`].
///
/// Construct one with [`ChainStoreOptions::for_network`] for production use, or
/// [`ChainStoreOptions::for_local_tests`] for throwaway test stores. The struct
/// has no `Default` so callers must pick a posture explicitly. The
/// `rocksdb_resource_budget` carries the bounded `RocksDB` resource budget
/// described in [ADR-0020](../../../docs/adrs/0020-bounded-rocksdb-resource-budget.md).
/// Displaced-block archive rows are retained permanently. This release has no
/// archive-retention option; callers must capacity-plan for monotonic growth.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainStoreOptions {
    /// Number of near-tip blocks that may be replaced by a reorg.
    pub reorg_window_blocks: u32,
    /// Whether each write batch asks `RocksDB` to fsync before returning.
    pub sync_writes: bool,
    /// Expected network for this store, used to persist and validate store metadata.
    pub network: Option<Network>,
    /// Bounded `RocksDB` resource budget applied at open time.
    pub rocksdb_resource_budget: RocksDbResourceBudget,
    /// Raw-blob retention the primary writer persists on open.
    pub raw_blob_retention: RawBlobRetention,
    /// Maximum heights one maintenance pass sweeps when the settled-tip retention floor
    /// jumps far ahead of the swept marker. Bounds the scan through sparse
    /// eras where few outpoints ever meet the outpoint budget.
    pub retention_sweep_max_heights_per_pass: u32,
    /// Maximum outpoints one maintenance pass sweeps. Bounds the delete batch held in
    /// memory through transaction-dense eras where a few heights carry
    /// millions of spent outpoints. A single height is never split across
    /// passes, so one pass may exceed the budget by at most the densest
    /// height in its range.
    pub retention_sweep_max_outpoints_per_pass: u64,
}

/// Default per-pass ceiling on transparent retention sweep heights.
const DEFAULT_RETENTION_SWEEP_MAX_HEIGHTS_PER_PASS: u32 = 1_000;

/// Default per-pass ceiling on transparent retention sweep outpoints.
const DEFAULT_RETENTION_SWEEP_MAX_OUTPOINTS_PER_PASS: u64 = 10_000;
const SECONDARY_CATCHUP_MISSING_SST_ATTEMPTS: u32 = 3;

impl ChainStoreOptions {
    /// Returns durable production options anchored to `network` with fsync writes.
    #[must_use]
    pub const fn for_network(network: Network) -> Self {
        Self {
            reorg_window_blocks: 100,
            sync_writes: true,
            network: Some(network),
            rocksdb_resource_budget: RocksDbResourceBudget::canonical_writer_defaults(),
            raw_blob_retention: RawBlobRetention::None,
            retention_sweep_max_heights_per_pass: DEFAULT_RETENTION_SWEEP_MAX_HEIGHTS_PER_PASS,
            retention_sweep_max_outpoints_per_pass: DEFAULT_RETENTION_SWEEP_MAX_OUTPOINTS_PER_PASS,
        }
    }

    /// Returns options suitable for throwaway local test stores.
    ///
    /// Uses unsynchronized writes, a regtest network anchor, and the
    /// tighter [`RocksDbResourceBudget::for_local_tests`] budget. Production code
    /// must use [`Self::for_network`] instead.
    #[must_use]
    pub const fn for_local_tests() -> Self {
        Self {
            reorg_window_blocks: 100,
            sync_writes: false,
            network: Some(Network::ZcashRegtest),
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            raw_blob_retention: RawBlobRetention::None,
            retention_sweep_max_heights_per_pass: DEFAULT_RETENTION_SWEEP_MAX_HEIGHTS_PER_PASS,
            retention_sweep_max_outpoints_per_pass: DEFAULT_RETENTION_SWEEP_MAX_OUTPOINTS_PER_PASS,
        }
    }
}

/// Durable canonical store schema version written by this binary.
///
/// The current layout persists structured compact transaction data and derives
/// mempool transparent indexes from that data. Stores written with any earlier
/// schema are refused at open and must be rebuilt from the source chain.
pub const CURRENT_STORE_SCHEMA_VERSION: u16 = 16;
/// Oldest durable artifact schema version this binary can read.
///
/// Version 20 is the only accepted schema and requires the structured compact
/// block and mempool scan-data records.
pub const MIN_SUPPORTED_ARTIFACT_SCHEMA_VERSION: u16 = 20;
/// Highest durable artifact schema version this binary can read.
pub const MAX_SUPPORTED_ARTIFACT_SCHEMA_VERSION: u16 = CURRENT_ARTIFACT_SCHEMA_VERSION.value();

/// Maximum final-root artifacts accepted by one historical enrichment write.
pub const MAX_FINAL_NOTE_COMMITMENT_ROOT_ENRICHMENT_BATCH: usize = 10_000;

/// Maximum block value-pool balance artifacts accepted by one enrichment write.
pub const MAX_BLOCK_VALUE_POOL_BALANCE_ENRICHMENT_BATCH: usize = 10_000;

/// Maximum transaction intrinsic-balance artifacts accepted by one enrichment write.
pub const MAX_TRANSACTION_INTRINSIC_VALUE_BALANCE_ENRICHMENT_BATCH: usize = 10_000;

/// Result of enriching final-root artifacts without publishing a new chain epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalNoteCommitmentRootEnrichmentOutcome {
    /// Canonical epoch against which every artifact was validated and written.
    pub chain_epoch: ChainEpoch,
    /// Number of distinct artifacts supplied by the caller.
    pub artifact_count: usize,
}

/// Result of enriching block value-pool balances without publishing a new chain epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockValuePoolBalanceEnrichmentOutcome {
    /// Canonical epoch against which every artifact was validated and written.
    pub chain_epoch: ChainEpoch,
    /// Number of distinct artifacts supplied by the caller.
    pub artifact_count: usize,
}

/// Result of enriching transaction-intrinsic value balances without publishing an epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionIntrinsicValueBalanceEnrichmentOutcome {
    /// Canonical epoch against which every artifact was validated and written.
    pub chain_epoch: ChainEpoch,
    /// Number of distinct artifacts supplied by the caller.
    pub artifact_count: usize,
}

/// Chain-event retention state observed after a pruning or inspection pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainEventRetentionReport {
    /// Latest chain-event sequence written to the store.
    pub current_event_sequence: u64,
    /// Oldest retained sequence, or `None` when the store has no chain events.
    pub oldest_retained_sequence: Option<u64>,
    /// Creation time for [`Self::oldest_retained_sequence`], when retained.
    pub oldest_retained_created_at: Option<UnixTimestampMillis>,
    /// Number of event rows retained after the pass.
    pub retained_event_count: u64,
    /// Number of event rows deleted by this pass.
    pub pruned_event_count: u64,
}

/// Bounded transparent-address output read request.
///
/// Cursor handling lives entirely inside the store: the cursor token is
/// HMAC-authenticated against the store's per-instance auth key and decoded
/// into a typed position in `read_address_output_index_rows_paged`.
#[derive(Clone, Copy, Debug)]
pub struct AddressOutputIndexPageRequest<'cursor> {
    /// Optional chain epoch to pin the read to. `None` reads at the
    /// currently visible epoch.
    pub at_epoch: Option<ChainEpoch>,
    /// SHA-256 of the transparent address scriptPubKey.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Wallet-birthday optimization: skip outputs mined below this height.
    /// Ignored when `from_cursor` is `Some`.
    pub start_height: BlockHeight,
    /// Server-bounded maximum entries per page.
    pub max_entries: NonZeroU32,
    /// Optional cursor to resume strictly after. When present,
    /// `start_height` is ignored.
    pub from_cursor: Option<&'cursor StreamCursorTokenV1>,
}

/// Bounded transparent-address output page response.
#[derive(Clone, Debug)]
pub struct AddressOutputIndexPage {
    /// Chain epoch used to answer this page.
    pub chain_epoch: ChainEpoch,
    /// outputs in ascending `(block_height, outpoint)` order.
    pub outputs: Vec<TransparentUnspentOutput>,
    /// Resume cursor when the page reached `max_entries`. `None` when the
    /// scan was fully drained.
    pub next_cursor: Option<StreamCursorTokenV1>,
}

/// Outcome returned after a `RocksDB` secondary catchup attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecondaryCatchupOutcome {
    /// Visible epoch before catchup.
    pub before: Option<ChainEpochId>,
    /// Visible epoch after catchup.
    pub after: Option<ChainEpochId>,
}

impl SecondaryCatchupOutcome {
    const fn new(before: Option<ChainEpochId>, after: Option<ChainEpochId>) -> Self {
        Self { before, after }
    }
}

/// Result of one bounded transparent-projection retention maintenance pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransparentRetentionSweepOutcome {
    swept_heights: u32,
    swept_outpoints: u64,
    backlog_heights: u32,
}

impl TransparentRetentionSweepOutcome {
    const fn new(swept_heights: u32, swept_outpoints: u64, backlog_heights: u32) -> Self {
        Self {
            swept_heights,
            swept_outpoints,
            backlog_heights,
        }
    }

    /// Number of complete block heights advanced by this pass.
    #[must_use]
    pub const fn swept_heights(self) -> u32 {
        self.swept_heights
    }

    /// Number of settled spent outpoints deleted by this pass.
    #[must_use]
    pub const fn swept_outpoints(self) -> u64 {
        self.swept_outpoints
    }

    /// Remaining complete block heights below the current settled release ceiling.
    #[must_use]
    pub const fn backlog_heights(self) -> u32 {
        self.backlog_heights
    }

    /// Whether this pass advanced the durable retention cursor.
    #[must_use]
    pub const fn made_progress(self) -> bool {
        self.swept_heights > 0
    }
}

/// Canonical chain store opened as the single primary writer.
#[derive(Clone)]
pub struct PrimaryChainStore {
    store: ChainStoreInner,
    /// Cache of the currently visible chain epoch, populated on first read
    /// and refreshed on every successful [`commit_chain_epoch`]. Lets the
    /// live mempool owner stamp per-event `first_seen_chain_epoch` without
    /// touching `RocksDB` on each observation.
    cached_visible_chain_epoch: Arc<RwLock<Option<ChainEpoch>>>,
}

/// Canonical chain store opened as a `RocksDB` secondary reader.
#[derive(Clone)]
pub struct SecondaryChainStore {
    store: ChainStoreInner,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChainStoreReadPosture {
    Snapshot,
    Direct,
}

/// In-process facade shared by primary and secondary role handles.
#[derive(Clone)]
struct ChainStoreInner {
    inner: Arc<RocksChainStore>,
    options: ChainStoreOptions,
    cursor_auth_key: [u8; 32],
    read_posture: ChainStoreReadPosture,
    secondary_visible_epoch: Option<Arc<AtomicU64>>,
}

#[derive(Clone, Copy)]
struct ChainEventHistoryBounds {
    current_event_sequence: u64,
    oldest_retained_sequence: u64,
}

/// Retained-sequence window for a mempool-event history read.
#[derive(Clone, Copy)]
struct MempoolEventHistoryBounds {
    current_event_sequence: u64,
    oldest_retained_sequence: u64,
}

/// Resolved resume position for a chain-event history read.
struct ChainEventResume {
    /// First retained event sequence to read forward from.
    start_sequence: u64,
    /// Stream family the page reads under.
    family: ChainEventStreamFamily,
    /// Synthetic `ChainReorged` envelope to deliver ahead of the page when the
    /// cursor's branch was reorged out and the real reorg event was pruned.
    synthetic_reorg: Option<ChainEventEnvelope>,
}

/// Resolves the resume position for one event-history read, dispatching on
/// whether the caller supplied a cursor.
///
/// Every cursor-bound event family (chain events, mempool events) shares this
/// skeleton: with no cursor, the read starts at the retention floor; with a
/// cursor, a family-specific position-check resolves where retained delivery
/// resumes. The check owns cursor authentication, sequence-bound validation,
/// and any family-specific fork or expiry handling, so the typed error
/// vocabulary stays inside the family that owns it.
fn resolve_event_history_start_sequence<Resume>(
    from_cursor: Option<&StreamCursorTokenV1>,
    floor_resume: impl FnOnce() -> Resume,
    position_check: impl FnOnce(&StreamCursorTokenV1) -> Result<Resume, StoreError>,
) -> Result<Resume, StoreError> {
    from_cursor.map_or_else(|| Ok(floor_resume()), position_check)
}

/// Inputs for resolving the resume position of a reorged-out cursor.
#[derive(Clone, Copy)]
struct ReorgedCursorResume {
    current_chain_epoch: ChainEpoch,
    family: ChainEventStreamFamily,
    fork_point: ChainEventCursorAnchor,
    reverted_tip: ChainEventCursorAnchor,
    event_sequence: u64,
    cursor_auth_key: [u8; 32],
    bounds: ChainEventHistoryBounds,
}

/// Public Rust API used by `zinder-query` to read epoch-bound canonical data.
pub trait ChainEpochReadApi {
    /// Reads the durable boundary of intentionally retained canonical history.
    fn canonical_history_bounds(&self) -> Result<Option<CanonicalHistoryBounds>, StoreError>;

    /// Opens a reader pinned to the currently visible chain epoch.
    fn current_chain_epoch_reader(&self) -> Result<ChainEpochReader<'_>, StoreError>;

    /// Opens a reader pinned to a specific chain epoch.
    fn chain_epoch_reader_at(
        &self,
        chain_epoch: ChainEpochId,
    ) -> Result<ChainEpochReader<'_>, StoreError>;

    /// Reads a bounded page of retained chain events strictly after the request cursor.
    fn chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, StoreError>;

    /// Resolves an event-stream start position for the chain-event family.
    fn resolve_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StoreError>;

    /// Reads a bounded page of unspent transparent outputs.
    ///
    /// Decodes any supplied cursor against the store's per-instance auth key
    /// and resumes scanning strictly after the cursor position. Emits a
    /// `next_cursor` only when the page reached `max_entries`, signalling
    /// that more outputs may be available.
    fn address_output_index_page(
        &self,
        request: AddressOutputIndexPageRequest<'_>,
    ) -> Result<AddressOutputIndexPage, StoreError>;
}

impl PrimaryChainStore {
    /// Opens or creates the canonical chain store as the primary writer.
    pub fn open(path: impl AsRef<Path>, options: ChainStoreOptions) -> Result<Self, StoreError> {
        validate_chain_store_options(options)?;
        let initialized_from_empty_store = preflight_primary_store_schema(path.as_ref())?;
        let inner = Arc::new(RocksChainStore::open_primary(
            path.as_ref(),
            options.sync_writes,
            options.rocksdb_resource_budget,
        )?);
        validate_primary_store_schema(&inner, initialized_from_empty_store)?;
        inner.ensure_all_storage_tables_are_present()?;
        let store =
            ChainStoreInner::from_primary_inner(inner, options, ChainStoreReadPosture::Snapshot)?;

        Ok(Self {
            store,
            cached_visible_chain_epoch: Arc::new(RwLock::new(None)),
        })
    }

    /// Flushes every column family's active memtable to `SST` and
    /// truncates the WAL.
    ///
    /// Called by `zinder-ingest` between `BulkCatchup` batches to bound
    /// the live WAL by writer cadence rather than `RocksDB`'s own
    /// WAL-size trigger. See [the OOM-recovery
    /// runbook](../../../docs/runbooks/bulk-catchup-resource-tuning.md) for
    /// the rationale.
    pub fn flush(&self) -> Result<(), StoreError> {
        self.store.inner.flush()
    }

    /// Reads the persisted raw-blob retention signal.
    ///
    /// Returns [`RawBlobRetention::None`] only for an empty store without a
    /// signal. A non-empty store with no signal is corrupt and fails closed.
    pub fn raw_blob_retention(&self) -> Result<RawBlobRetention, StoreError> {
        read_raw_blob_retention_signal(self.store.inner.as_ref())
    }

    /// Returns the reorg window configured on this primary writer handle.
    #[must_use]
    pub const fn reorg_window_blocks(&self) -> u32 {
        self.store.options.reorg_window_blocks
    }

    /// Reads the durable boundary of intentionally retained canonical history.
    pub fn canonical_history_bounds(&self) -> Result<Option<CanonicalHistoryBounds>, StoreError> {
        read_canonical_history_bounds(self.store.inner.as_ref())
    }

    /// Reads the height through which transparent-retention maintenance has deleted
    /// transparent spend facts and their spent-output rows, or `None` before
    /// the first sweep.
    ///
    /// Below this height the canonical store holds no transparent spend fact:
    /// a durable projection is the only source of spender identity.
    pub fn transparent_retention_swept_height(&self) -> Result<Option<BlockHeight>, StoreError> {
        read_transparent_retention_swept_height(self.store.inner.as_ref())
    }

    /// Reads the height through which the settled-tip sweep has actually deleted
    /// transparent spend facts, or `None` before any real deletion.
    ///
    /// Unlike the swept-through cursor, this marker advances only in a batch
    /// that deletes spend facts, so it names exactly the highest settled height
    /// whose spender identity survives only in the durable projection. The
    /// startup guard reads it to decide whether the projection can still resolve
    /// every swept spend.
    pub fn transparent_retention_deleted_through_height(
        &self,
    ) -> Result<Option<BlockHeight>, StoreError> {
        read_transparent_retention_deleted_through_height(self.store.inner.as_ref())
    }

    /// Publishes the durable-consumer retention release height.
    ///
    /// `zinder-ingest` calls this from the materialized-view tailer as the durable
    /// transparent-outpoint-spend projection advances. The settled-tip sweep never
    /// deletes a spend fact above this height, so canonical retention releases
    /// only what the durable projection has already recorded.
    pub fn set_transparent_retention_release_height(
        &self,
        height: BlockHeight,
    ) -> Result<(), StoreError> {
        self.store
            .inner
            .write(vec![transparent_retention_release_height_put(height)])
    }

    /// Runs one bounded transparent-projection retention maintenance pass.
    ///
    /// The pass deletes only settled spends already released by the durable
    /// materialized view. It is deliberately separate from
    /// [`Self::commit_chain_epoch`] so a large historical retention backlog
    /// cannot delay canonical chain advancement. The ingest scheduler calls
    /// this only after canonical ingest and materialized-view replay have both caught up.
    pub fn sweep_transparent_retention_once(
        &self,
    ) -> Result<TransparentRetentionSweepOutcome, StoreError> {
        self.store.sweep_transparent_retention_once()
    }

    /// Reads the currently visible chain epoch.
    pub fn current_chain_epoch(&self) -> Result<Option<ChainEpoch>, StoreError> {
        let cached = *self.cached_visible_chain_epoch.read();
        if cached.is_some() {
            return Ok(cached);
        }
        let fetched = self.store.current_chain_epoch()?;
        if let Some(chain_epoch) = fetched {
            *self.cached_visible_chain_epoch.write() = Some(chain_epoch);
        }
        Ok(fetched)
    }

    /// Opens a reader pinned to the currently visible chain epoch.
    pub fn current_chain_epoch_reader(&self) -> Result<ChainEpochReader<'_>, StoreError> {
        self.store.current_chain_epoch_reader()
    }

    /// Opens a reader pinned to a specific chain epoch.
    pub fn chain_epoch_reader_at(
        &self,
        chain_epoch: ChainEpochId,
    ) -> Result<ChainEpochReader<'_>, StoreError> {
        self.store.chain_epoch_reader_at(chain_epoch)
    }

    /// Opens a reader pinned to a specific chain epoch, attributing its reads
    /// to `caller`.
    pub fn chain_epoch_reader_at_for(
        &self,
        caller: StoreReadCaller,
        chain_epoch: ChainEpochId,
    ) -> Result<ChainEpochReader<'_>, StoreError> {
        self.store.chain_epoch_reader_at_for(caller, chain_epoch)
    }

    /// Resolves transparent outputs on the primary writer's direct read path.
    ///
    /// This skips snapshot pinning and visibility filtering because the writer
    /// calls it while deriving a node-validated commit against the current
    /// visible epoch. External readers must use [`ChainEpochReader`] instead.
    pub fn transparent_outputs_by_outpoints_for_writer_commit(
        &self,
        caller: StoreReadCaller,
        chain_epoch: ChainEpoch,
        outpoints: &[TransparentOutPoint],
    ) -> Result<HashMap<TransparentOutPoint, TransparentOutputArtifact>, StoreError> {
        read_current_transparent_outputs_by_outpoints(
            &self.store.inner.direct_read_view_for(caller),
            chain_epoch,
            outpoints,
        )
    }

    /// Atomically commits artifacts for one chain epoch and advances the visible pointer.
    pub fn commit_chain_epoch(
        &self,
        artifacts: ChainEpochArtifacts,
    ) -> Result<ChainEpochCommitOutcome, StoreError> {
        let outcome = self.store.commit_chain_epoch(artifacts)?;
        *self.cached_visible_chain_epoch.write() = Some(outcome.chain_epoch);
        Ok(outcome)
    }

    /// Atomically bootstraps an empty store from an artifactless checkpoint epoch.
    pub fn commit_artifactless_checkpoint(
        &self,
        chain_epoch: ChainEpoch,
    ) -> Result<ChainEpochCommitOutcome, StoreError> {
        let outcome = self.store.commit_artifactless_checkpoint(chain_epoch)?;
        *self.cached_visible_chain_epoch.write() = Some(outcome.chain_epoch);
        Ok(outcome)
    }

    /// Enriches final note-commitment roots for currently canonical blocks.
    ///
    /// This ingest-writer operation serializes with canonical commits, writes
    /// no chain event, and leaves the visible chain epoch unchanged.
    pub fn enrich_final_note_commitment_roots(
        &self,
        roots_by_block: &[BlockFinalNoteCommitmentRoots],
    ) -> Result<FinalNoteCommitmentRootEnrichmentOutcome, StoreError> {
        self.store
            .enrich_final_note_commitment_roots(roots_by_block)
    }

    /// Enriches cumulative value-pool balances for currently canonical blocks.
    ///
    /// This ingest-writer operation serializes with canonical commits, writes
    /// no chain event, and leaves the visible chain epoch unchanged.
    pub fn enrich_block_value_pool_balances(
        &self,
        balances_by_block: &[BlockValuePoolBalances],
    ) -> Result<BlockValuePoolBalanceEnrichmentOutcome, StoreError> {
        self.store
            .enrich_block_value_pool_balances(balances_by_block)
    }

    /// Enriches intrinsic value balances for settled canonical transactions.
    ///
    /// This ingest-writer operation serializes with canonical commits, writes
    /// no chain event, and leaves the visible chain epoch unchanged.
    pub fn enrich_transaction_intrinsic_value_balances(
        &self,
        artifacts: &[TransactionIntrinsicValueBalancesArtifact],
    ) -> Result<TransactionIntrinsicValueBalanceEnrichmentOutcome, StoreError> {
        self.store
            .enrich_transaction_intrinsic_value_balances(artifacts)
    }

    /// Reads a bounded page of retained chain events strictly after the request cursor.
    pub fn chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, StoreError> {
        self.store.chain_event_history(request)
    }

    /// Reads a bounded page of unspent transparent outputs.
    pub fn address_output_index_page(
        &self,
        request: AddressOutputIndexPageRequest<'_>,
    ) -> Result<AddressOutputIndexPage, StoreError> {
        self.store.address_output_index_page(request)
    }

    /// Builds an HMAC-authenticated next-page cursor for a `MempoolSnapshot`
    /// walk, bookmarking the paging position plus the walk's events-resume
    /// anchor.
    pub fn encode_snapshot_page_cursor(
        &self,
        events_resume_anchor: Option<MempoolEventPosition>,
        after_transaction_id: TransactionId,
    ) -> Result<StreamCursorTokenV1, StoreError> {
        let network = self
            .network()
            .ok_or(StoreError::SnapshotPageCursorInvalid {
                reason: "store has no network",
            })?;
        StreamCursorTokenV1::snapshot_page(
            SnapshotPageCursorAnchor {
                network,
                events_resume_anchor,
                after_transaction_id,
            },
            self.store.cursor_auth_key,
        )
        .map_err(|_| StoreError::SnapshotPageCursorInvalid {
            reason: "cursor authentication key could not initialize the MAC",
        })
    }

    /// Decodes a `MempoolSnapshot` next-page cursor, verifying its HMAC,
    /// network, and stream family.
    ///
    /// Callers bound the decoded events-resume anchor against the
    /// mempool-event sequence the writer has applied and reject a cursor
    /// anchored ahead of it as [`StoreError::SnapshotPageCursorExpired`].
    pub fn decode_snapshot_page_cursor(
        &self,
        cursor: &StreamCursorTokenV1,
    ) -> Result<SnapshotPageCursorPayload, StoreError> {
        let network = self
            .network()
            .ok_or(StoreError::SnapshotPageCursorInvalid {
                reason: "store has no network",
            })?;
        cursor
            .decode_snapshot_page(network, self.store.cursor_auth_key)
            .map_err(|_| StoreError::SnapshotPageCursorInvalid {
                reason: "cursor failed authentication, network, or stream-family validation",
            })
    }

    /// Mints the `MempoolEvents` resume cursor for a snapshot walk anchored
    /// at `anchor`.
    ///
    /// Byte-identical to the cursor carried by the anchor event's envelope:
    /// the mempool cursor body encodes exactly the anchor pair.
    pub fn encode_mempool_events_resume_cursor(
        &self,
        anchor: MempoolEventPosition,
    ) -> Result<StreamCursorTokenV1, StoreError> {
        let network = self
            .network()
            .ok_or(StoreError::SnapshotPageCursorInvalid {
                reason: "store has no network",
            })?;
        StreamCursorTokenV1::mempool_event(
            network,
            anchor.event_sequence,
            anchor.transaction_id,
            self.store.cursor_auth_key,
        )
        .map_err(|_| StoreError::SnapshotPageCursorInvalid {
            reason: "cursor authentication key could not initialize the MAC",
        })
    }

    /// Resolves an event-stream start position for the chain-event family.
    pub fn resolve_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StoreError> {
        self.store
            .resolve_chain_event_stream_start(start, requested_family)
    }

    /// Resolves an event-stream start position for the mempool-event family
    /// to the cursor the page loop resumes strictly after.
    pub fn resolve_mempool_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
    ) -> Result<Option<StreamCursorTokenV1>, StoreError> {
        self.store.resolve_mempool_event_stream_start(start)
    }

    /// Deletes retained chain-event rows older than `cutoff_created_at`.
    ///
    /// The newest event is always retained, preserving a replay anchor even
    /// when the entire event log falls outside the configured time window.
    pub fn prune_chain_events_before(
        &self,
        cutoff_created_at: UnixTimestampMillis,
    ) -> Result<ChainEventRetentionReport, StoreError> {
        self.store.prune_chain_events_before(cutoff_created_at)
    }

    /// Reads the current chain-event retention floor without pruning.
    pub fn chain_event_retention_report(&self) -> Result<ChainEventRetentionReport, StoreError> {
        self.store.chain_event_retention_report()
    }

    /// Appends a single mempool event to the persistent log.
    pub fn append_mempool_event(
        &self,
        event: MempoolEvent,
        source_observed_at: UnixTimestampMillis,
    ) -> Result<MempoolEventEnvelope, StoreError> {
        self.store.append_mempool_event(event, source_observed_at)
    }

    /// Reads a bounded page of retained mempool events strictly after the
    /// request cursor.
    pub fn mempool_event_history(
        &self,
        request: MempoolEventHistoryRequest<'_>,
    ) -> Result<Vec<MempoolEventEnvelope>, StoreError> {
        self.store.mempool_event_history(request)
    }

    /// Deletes retained mempool-event rows older than the per-variant
    /// retention windows.
    pub fn prune_mempool_events_before(
        &self,
        now: UnixTimestampMillis,
        retention: MempoolEventRetentionConfig,
    ) -> Result<MempoolEventRetentionReport, StoreError> {
        self.store.prune_mempool_events_before(now, retention)
    }

    /// Reads the current mempool-event retention floor without pruning.
    pub fn mempool_event_retention_report(
        &self,
    ) -> Result<MempoolEventRetentionReport, StoreError> {
        self.store.mempool_event_retention_report()
    }

    /// Returns the network bound to this store, used by mempool cursor
    /// encoding.
    #[must_use]
    pub fn network(&self) -> Option<Network> {
        self.store.options.network
    }

    /// Creates a `RocksDB` checkpoint for backup or fixture capture.
    pub fn create_checkpoint(&self, path: impl AsRef<Path>) -> Result<(), StoreError> {
        self.store.inner.create_checkpoint(path)
    }
}

impl SecondaryChainStore {
    /// Opens the canonical chain store as a `RocksDB` secondary reader.
    pub fn open(
        primary_path: impl AsRef<Path>,
        secondary_path: impl AsRef<Path>,
        options: ChainStoreOptions,
    ) -> Result<Self, StoreError> {
        validate_chain_store_options(options)?;
        let inner = Arc::new(RocksChainStore::open_secondary(
            primary_path,
            secondary_path,
            options.sync_writes,
            options.rocksdb_resource_budget,
        )?);
        let store =
            ChainStoreInner::from_secondary_inner(inner, options, ChainStoreReadPosture::Direct)?;

        Ok(Self { store })
    }

    /// Reads the persisted raw-blob retention signal.
    ///
    /// Returns [`RawBlobRetention::None`] only for an empty store without a
    /// signal. A non-empty store with no signal is corrupt and fails closed.
    pub fn raw_blob_retention(&self) -> Result<RawBlobRetention, StoreError> {
        read_raw_blob_retention_signal(self.store.inner.as_ref())
    }

    /// Reads the durable boundary of intentionally retained canonical history.
    pub fn canonical_history_bounds(&self) -> Result<Option<CanonicalHistoryBounds>, StoreError> {
        read_canonical_history_bounds(self.store.inner.as_ref())
    }

    /// Reads the currently visible chain epoch.
    pub fn current_chain_epoch(&self) -> Result<Option<ChainEpoch>, StoreError> {
        self.store.current_chain_epoch()
    }

    /// Opens a reader pinned to the currently visible chain epoch.
    pub fn current_chain_epoch_reader(&self) -> Result<ChainEpochReader<'_>, StoreError> {
        self.store.current_chain_epoch_reader()
    }

    /// Opens a reader pinned to a specific chain epoch.
    pub fn chain_epoch_reader_at(
        &self,
        chain_epoch: ChainEpochId,
    ) -> Result<ChainEpochReader<'_>, StoreError> {
        self.store.chain_epoch_reader_at(chain_epoch)
    }

    /// Reads a bounded page of retained chain events strictly after the request cursor.
    pub fn chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, StoreError> {
        self.store.chain_event_history(request)
    }

    /// Reads a bounded page of unspent transparent outputs.
    pub fn address_output_index_page(
        &self,
        request: AddressOutputIndexPageRequest<'_>,
    ) -> Result<AddressOutputIndexPage, StoreError> {
        self.store.address_output_index_page(request)
    }

    /// Reads the current chain-event retention floor without pruning.
    pub fn chain_event_retention_report(&self) -> Result<ChainEventRetentionReport, StoreError> {
        self.store.chain_event_retention_report()
    }

    /// Resolves an event-stream start position for the chain-event family.
    pub fn resolve_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StoreError> {
        self.store
            .resolve_chain_event_stream_start(start, requested_family)
    }

    /// Reads a bounded page of retained mempool events strictly after the
    /// request cursor.
    pub fn mempool_event_history(
        &self,
        request: MempoolEventHistoryRequest<'_>,
    ) -> Result<Vec<MempoolEventEnvelope>, StoreError> {
        self.store.mempool_event_history(request)
    }

    /// Reads the current mempool-event retention floor without pruning.
    pub fn mempool_event_retention_report(
        &self,
    ) -> Result<MempoolEventRetentionReport, StoreError> {
        self.store.mempool_event_retention_report()
    }

    /// Replays available WAL and manifest state from the primary store.
    pub fn try_catch_up(&self) -> Result<SecondaryCatchupOutcome, StoreError> {
        let mut attempt = 1;
        loop {
            let outcome = self.try_catch_up_once();
            if outcome
                .as_ref()
                .is_err_and(is_transient_secondary_missing_sst)
                && attempt < SECONDARY_CATCHUP_MISSING_SST_ATTEMPTS
            {
                metrics::counter!("zinder_store_secondary_catchup_retries_total").increment(1);
                tracing::debug!(
                    target: "zinder::store",
                    event = "secondary_catchup_missing_sst_retry",
                    attempt,
                    max_attempts = SECONDARY_CATCHUP_MISSING_SST_ATTEMPTS,
                    "canonical secondary crossed a primary-compaction file race; retrying catchup"
                );
                std::thread::yield_now();
                attempt += 1;
                continue;
            }
            return outcome;
        }
    }

    fn try_catch_up_once(&self) -> Result<SecondaryCatchupOutcome, StoreError> {
        let before = self.store.current_chain_epoch_id()?;
        self.store.inner.try_catch_up_with_primary()?;
        let after = self.store.current_chain_epoch_id()?;
        if let Some(visible_epoch) = &self.store.secondary_visible_epoch {
            visible_epoch.store(after.map_or(0, ChainEpochId::value), Ordering::Release);
        }

        Ok(SecondaryCatchupOutcome::new(before, after))
    }
}

fn is_transient_secondary_missing_sst(error: &StoreError) -> bool {
    let Some(source) = std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<rust_rocksdb::Error>())
    else {
        return false;
    };
    is_missing_sst_error(&source.kind(), source.as_ref())
}

fn is_missing_sst_error(kind: &rust_rocksdb::ErrorKind, message: &str) -> bool {
    matches!(
        kind,
        rust_rocksdb::ErrorKind::IOError | rust_rocksdb::ErrorKind::NotFound
    ) && message.contains("No such file or directory")
        && message.contains(".sst")
}

impl ChainStoreInner {
    fn from_primary_inner(
        inner: Arc<RocksChainStore>,
        options: ChainStoreOptions,
        read_posture: ChainStoreReadPosture,
    ) -> Result<Self, StoreError> {
        let cursor_auth_key = {
            let _control_guard = inner.lock_control();
            if let Some(network) = options.network {
                ensure_store_metadata(&inner, network)?;
            }
            ensure_supported_artifact_schema(inner.as_ref())?;
            admit_raw_blob_retention(&inner, options.raw_blob_retention)?;
            ensure_cursor_auth_key(&inner)?
        };

        Ok(Self {
            inner,
            options,
            cursor_auth_key,
            read_posture,
            secondary_visible_epoch: None,
        })
    }

    fn from_secondary_inner(
        inner: Arc<RocksChainStore>,
        options: ChainStoreOptions,
        read_posture: ChainStoreReadPosture,
    ) -> Result<Self, StoreError> {
        let cursor_auth_key = {
            if let Some(network) = options.network {
                validate_store_metadata(inner.as_ref(), network)?;
            }
            inner.ensure_all_storage_tables_are_present()?;
            ensure_supported_artifact_schema(inner.as_ref())?;
            read_cursor_auth_key(inner.as_ref())?
        };
        let visible_epoch =
            read_current_chain_epoch_id(inner.as_ref())?.map_or(0, ChainEpochId::value);

        Ok(Self {
            inner,
            options,
            cursor_auth_key,
            read_posture,
            secondary_visible_epoch: Some(Arc::new(AtomicU64::new(visible_epoch))),
        })
    }

    /// Reads the currently visible chain epoch.
    fn current_chain_epoch(&self) -> Result<Option<ChainEpoch>, StoreError> {
        let read_view = self.read_view();
        read_current_chain_epoch_id(&read_view)?
            .map(|chain_epoch_id| read_chain_epoch(&read_view, chain_epoch_id))
            .transpose()
    }

    /// Opens a reader pinned to the currently visible chain epoch.
    fn current_chain_epoch_reader(&self) -> Result<ChainEpochReader<'_>, StoreError> {
        let read_view = self.read_view();
        let chain_epoch = require_current_chain_epoch(&read_view)?;
        let canonical_history_bounds = require_canonical_history_bounds(&read_view)?;
        Ok(ChainEpochReader::current(
            chain_epoch,
            canonical_history_bounds,
            read_view,
            self.secondary_visible_epoch.clone(),
        ))
    }

    /// Opens a reader pinned to a specific chain epoch.
    fn chain_epoch_reader_at(
        &self,
        chain_epoch: ChainEpochId,
    ) -> Result<ChainEpochReader<'_>, StoreError> {
        self.chain_epoch_reader_at_for(StoreReadCaller::Query, chain_epoch)
    }

    /// Opens a reader pinned to a specific chain epoch, attributing its reads
    /// to `caller`.
    fn chain_epoch_reader_at_for(
        &self,
        caller: StoreReadCaller,
        chain_epoch: ChainEpochId,
    ) -> Result<ChainEpochReader<'_>, StoreError> {
        let read_view = self.read_view_for(caller);
        let chain_epoch = read_chain_epoch(&read_view, chain_epoch)?;
        let canonical_history_bounds = require_canonical_history_bounds(&read_view)?;
        Ok(ChainEpochReader::at_epoch(
            chain_epoch,
            canonical_history_bounds,
            read_view,
            self.secondary_visible_epoch.clone(),
        ))
    }

    /// Atomically commits artifacts for one chain epoch and advances the visible pointer.
    fn commit_chain_epoch(
        &self,
        artifacts: ChainEpochArtifacts,
    ) -> Result<ChainEpochCommitOutcome, StoreError> {
        self.commit_chain_epoch_with_initial_bounds(artifacts, None)
    }

    fn commit_artifactless_checkpoint(
        &self,
        chain_epoch: ChainEpoch,
    ) -> Result<ChainEpochCommitOutcome, StoreError> {
        if chain_epoch.visible_tip_height != chain_epoch.settled_tip_height
            || chain_epoch.visible_tip_hash != chain_epoch.settled_tip_hash
        {
            return Err(StoreError::InvalidChainEpochArtifacts {
                reason: "artifactless checkpoint visible and settled tips must match",
            });
        }
        let checkpoint = BlockId::new(chain_epoch.visible_tip_height, chain_epoch.visible_tip_hash);
        let bounds = CanonicalHistoryBounds::checkpointed(checkpoint).map_err(|_| {
            StoreError::InvalidChainEpochArtifacts {
                reason: "artifactless checkpoint height must have a successor",
            }
        })?;
        let checkpoint_height = chain_epoch.visible_tip_height;
        let artifacts = ChainEpochArtifacts::new(chain_epoch, Vec::new(), Vec::new(), Vec::new())
            .with_reorg_window_change(ReorgWindowChange::AdvanceSettledTipTo {
                height: checkpoint_height,
            });
        self.commit_chain_epoch_with_initial_bounds(artifacts, Some(bounds))
    }

    fn commit_chain_epoch_with_initial_bounds(
        &self,
        artifacts: ChainEpochArtifacts,
        initial_bounds: Option<CanonicalHistoryBounds>,
    ) -> Result<ChainEpochCommitOutcome, StoreError> {
        // Artifact consistency is independent of current store state. Keep the
        // replay decode and exact row-parity work outside the writer control
        // section so pure CPU work does not lengthen the exclusive writer
        // critical section.
        let validated_block_replay_order = validate_chain_epoch_artifacts(&artifacts)?;
        validate_retained_blob_artifacts(&artifacts, self.options.raw_blob_retention)?;

        let _control_guard = self.inner.lock_control();
        let commit_read_view = self
            .inner
            .direct_read_view_for(StoreReadCaller::CommitFallback);
        let store_metadata_put =
            validate_store_metadata_for_commit(&self.inner, artifacts.chain_epoch.network)?;
        let current_chain_epoch = self.validate_commit_order(&artifacts.chain_epoch)?;
        validate_initial_canonical_history_commit(&artifacts, current_chain_epoch, initial_bounds)?;
        validate_reorg_window_change(&artifacts, current_chain_epoch, self.options)?;
        validate_visible_chain_commit(&self.inner, &artifacts, current_chain_epoch)?;
        let event_sequence = self
            .current_chain_event_sequence()?
            .checked_add(1)
            .ok_or(StoreError::ChainEventSequenceOverflow)?;

        let chain_epoch = artifacts.chain_epoch;
        let block_range = committed_block_range(&artifacts, current_chain_epoch)?;
        let reorg_window_change = artifacts.reorg_window_change.clone();
        let initializes_retention_cursor = current_chain_epoch.is_none()
            && artifacts.block_headers.is_empty()
            && matches!(
                &reorg_window_change,
                ReorgWindowChange::AdvanceSettledTipTo { .. }
            );
        let (current_projection_protected_outpoints, current_spend_projection_protected_outpoints) =
            protected_transparent_outpoints(&artifacts);
        let committed = ChainEpochCommitted::new(chain_epoch, block_range);
        let event_envelope = build_chain_event_envelope(&ChainEventEnvelopeInputs {
            inner: &commit_read_view,
            event_sequence,
            committed,
            previous_chain_epoch: current_chain_epoch,
            reorg_window_change: &reorg_window_change,
            cursor_auth_key: self.cursor_auth_key,
        })?;
        let mut puts =
            build_chain_epoch_puts(artifacts, validated_block_replay_order, &event_envelope)?;
        if let Some(bounds_put) =
            initial_canonical_history_bounds_put(current_chain_epoch, initial_bounds)?
        {
            puts.push(bounds_put);
        }
        puts.extend(build_displaced_block_archive_puts_for_change(
            self.inner.as_ref(),
            current_chain_epoch,
            chain_epoch,
            event_sequence,
            &reorg_window_change,
        )?);
        if let Some(store_metadata_put) = store_metadata_put {
            puts.push(store_metadata_put);
        }
        if initializes_retention_cursor {
            // A checkpoint bootstrap stores no canonical rows below the
            // supplied settled tip. Start maintenance at that boundary so a
            // later worker does not scan millions of intentionally absent
            // heights.
            puts.push(transparent_retention_swept_height_put(
                chain_epoch.settled_tip_height,
            ));
        }
        let projection_repairs = build_reorg_window_projection_repairs(
            &commit_read_view,
            TransparentOutputProjectionRepairInputs {
                previous_chain_epoch: current_chain_epoch,
                chain_epoch,
                reorg_window_change: &reorg_window_change,
                protected_outpoints: &current_projection_protected_outpoints,
            },
        )?;
        let spend_projection_repairs = build_reorg_window_spend_fact_projection_repairs(
            &commit_read_view,
            TransparentSpendFactProjectionRepairInputs {
                previous_chain_epoch: current_chain_epoch,
                chain_epoch,
                reorg_window_change: &reorg_window_change,
                protected_outpoints: &current_spend_projection_protected_outpoints,
            },
        )?;
        let mut deletes = projection_repairs.deletes;
        deletes.extend(spend_projection_repairs.deletes);

        self.inner.write_batch(puts, deletes)?;

        Ok(ChainEpochCommitOutcome::new(committed, event_envelope))
    }

    fn enrich_final_note_commitment_roots(
        &self,
        roots_by_block: &[BlockFinalNoteCommitmentRoots],
    ) -> Result<FinalNoteCommitmentRootEnrichmentOutcome, StoreError> {
        if roots_by_block.len() > MAX_FINAL_NOTE_COMMITMENT_ROOT_ENRICHMENT_BATCH {
            return Err(StoreError::InvalidChainEpochArtifacts {
                reason: "final note-commitment root enrichment batch exceeds the store limit",
            });
        }

        let _control_guard = self.inner.lock_control();
        let chain_epoch = require_current_chain_epoch(self.inner.as_ref())?;
        let read_view = self
            .inner
            .direct_read_view_for(StoreReadCaller::CommitFallback);
        let mut heights = HashSet::with_capacity(roots_by_block.len());
        let mut puts = Vec::with_capacity(roots_by_block.len().saturating_mul(2));

        for roots in roots_by_block {
            if !heights.insert(roots.height) {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "final note-commitment root enrichment cannot repeat a block height",
                });
            }
            let Some(block) = read_block_header_artifact(&read_view, chain_epoch, roots.height)?
            else {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "final note-commitment root enrichment height is not canonical",
                });
            };
            if block.block_hash != roots.block_hash {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "final note-commitment root enrichment block hash is stale",
                });
            }
            if let Some(existing) =
                crate::final_note_commitment_roots::read_final_note_commitment_roots(
                    &read_view,
                    chain_epoch,
                    roots.height,
                )?
            {
                if existing != *roots {
                    return Err(StoreError::InvalidChainEpochArtifacts {
                        reason: "final note-commitment root enrichment conflicts with visible roots",
                    });
                }
                continue;
            }

            let artifact_key = StoreKey::final_note_commitment_roots(
                chain_epoch.network,
                chain_epoch.id,
                roots.height,
            );
            if let Some(existing_bytes) = self.inner.get(
                StoreReadCaller::CommitFallback,
                StorageTable::FinalNoteCommitmentRoots,
                &artifact_key,
            )? {
                let existing = decode_final_note_commitment_roots(&artifact_key, &existing_bytes)?;
                if existing != *roots {
                    return Err(StoreError::InvalidChainEpochArtifacts {
                        reason: "final note-commitment root enrichment conflicts with stored roots",
                    });
                }
            }

            puts.push(StoragePut {
                table: StorageTable::FinalNoteCommitmentRoots,
                key: artifact_key,
                value: encode_final_note_commitment_roots(*roots)?,
            });
            puts.push(StoragePut {
                table: StorageTable::ReorgWindow,
                key: StoreKey::visible_final_note_commitment_roots_epoch(
                    chain_epoch.network,
                    roots.height,
                    chain_epoch.id,
                ),
                value: visibility_value(chain_epoch),
            });
        }

        self.inner.write_batch(puts, Vec::new())?;
        Ok(FinalNoteCommitmentRootEnrichmentOutcome {
            chain_epoch,
            artifact_count: roots_by_block.len(),
        })
    }

    fn enrich_block_value_pool_balances(
        &self,
        balances_by_block: &[BlockValuePoolBalances],
    ) -> Result<BlockValuePoolBalanceEnrichmentOutcome, StoreError> {
        if balances_by_block.len() > MAX_BLOCK_VALUE_POOL_BALANCE_ENRICHMENT_BATCH {
            return Err(StoreError::InvalidChainEpochArtifacts {
                reason: "block value-pool balance enrichment batch exceeds the store limit",
            });
        }

        let _control_guard = self.inner.lock_control();
        let chain_epoch = require_current_chain_epoch(self.inner.as_ref())?;
        let read_view = self
            .inner
            .direct_read_view_for(StoreReadCaller::CommitFallback);
        let mut heights = HashSet::with_capacity(balances_by_block.len());
        let mut puts = Vec::with_capacity(balances_by_block.len().saturating_mul(2));

        for balances in balances_by_block {
            validate_value_pool_entries(balances)?;
            if !heights.insert(balances.block_id.height) {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "block value-pool balance enrichment cannot repeat a block height",
                });
            }
            let Some(block) =
                read_block_header_artifact(&read_view, chain_epoch, balances.block_id.height)?
            else {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "block value-pool balance enrichment height is not canonical",
                });
            };
            if block.block_hash != balances.block_id.hash {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "block value-pool balance enrichment block hash is stale",
                });
            }
            if block.block_time != balances.block_time_seconds {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "block value-pool balance enrichment block time is stale",
                });
            }
            if let Some(existing) =
                read_block_value_pool_balances(&read_view, chain_epoch, balances.block_id.height)?
            {
                if existing != *balances {
                    return Err(StoreError::InvalidChainEpochArtifacts {
                        reason: "block value-pool balance enrichment conflicts with visible balances",
                    });
                }
                continue;
            }

            let artifact_key = StoreKey::block_value_pool_balances(
                chain_epoch.network,
                chain_epoch.id,
                balances.block_id.height,
            );
            validate_stored_block_value_pool_balances(
                self.inner.as_ref(),
                &artifact_key,
                balances,
            )?;

            puts.push(StoragePut {
                table: StorageTable::BlockValuePoolBalances,
                key: artifact_key,
                value: encode_block_value_pool_balances(balances)?,
            });
            puts.push(StoragePut {
                table: StorageTable::ReorgWindow,
                key: StoreKey::visible_block_value_pool_balances_epoch(
                    chain_epoch.network,
                    balances.block_id.height,
                    chain_epoch.id,
                ),
                value: visibility_value(chain_epoch),
            });
        }

        self.inner.write_batch(puts, Vec::new())?;
        Ok(BlockValuePoolBalanceEnrichmentOutcome {
            chain_epoch,
            artifact_count: balances_by_block.len(),
        })
    }

    fn enrich_transaction_intrinsic_value_balances(
        &self,
        artifacts: &[TransactionIntrinsicValueBalancesArtifact],
    ) -> Result<TransactionIntrinsicValueBalanceEnrichmentOutcome, StoreError> {
        if artifacts.len() > MAX_TRANSACTION_INTRINSIC_VALUE_BALANCE_ENRICHMENT_BATCH {
            return Err(StoreError::InvalidChainEpochArtifacts {
                reason: "transaction intrinsic value-balance enrichment batch exceeds the store limit",
            });
        }

        let _control_guard = self.inner.lock_control();
        let chain_epoch = require_current_chain_epoch(self.inner.as_ref())?;
        let read_view = self
            .inner
            .direct_read_view_for(StoreReadCaller::CommitFallback);
        let mut transaction_ids = HashSet::with_capacity(artifacts.len());
        let mut puts = Vec::with_capacity(artifacts.len());

        for artifact in artifacts {
            let transaction_id = artifact.location.transaction_id;
            if !transaction_ids.insert(transaction_id) {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "transaction intrinsic value-balance enrichment cannot repeat a transaction id",
                });
            }
            if artifact.location.block_height > chain_epoch.settled_tip_height {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "transaction intrinsic value-balance enrichment requires a settled transaction",
                });
            }
            let canonical_location =
                read_transaction_location(&read_view, chain_epoch, transaction_id)?;
            if canonical_location != Some(artifact.location) {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "transaction intrinsic value-balance enrichment location is stale or not canonical",
                });
            }
            if let Some(existing) =
                read_transaction_intrinsic_value_balances(&read_view, chain_epoch, transaction_id)?
            {
                if existing != *artifact {
                    return Err(StoreError::InvalidChainEpochArtifacts {
                        reason: "transaction intrinsic value-balance enrichment conflicts with visible balances",
                    });
                }
                continue;
            }

            let Some((source_epoch, _seek_key)) =
                visible_transaction_source_epoch(&read_view, chain_epoch, transaction_id)?
            else {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "transaction intrinsic value-balance enrichment transaction is not visible",
                });
            };
            let artifact_key = StoreKey::transaction_intrinsic_value_balances(
                chain_epoch.network,
                source_epoch,
                transaction_id,
            );
            if let Some(existing_bytes) = self.inner.get(
                StoreReadCaller::CommitFallback,
                StorageTable::TransactionIntrinsicValueBalances,
                &artifact_key,
            )? {
                let existing =
                    decode_transaction_intrinsic_value_balances(&artifact_key, &existing_bytes)?;
                if existing != *artifact {
                    return Err(StoreError::InvalidChainEpochArtifacts {
                        reason: "transaction intrinsic value-balance enrichment conflicts with stored balances",
                    });
                }
            }
            puts.push(StoragePut {
                table: StorageTable::TransactionIntrinsicValueBalances,
                key: artifact_key,
                value: encode_transaction_intrinsic_value_balances(*artifact)?,
            });
        }

        self.inner.write_batch(puts, Vec::new())?;
        Ok(TransactionIntrinsicValueBalanceEnrichmentOutcome {
            chain_epoch,
            artifact_count: artifacts.len(),
        })
    }

    /// Reads a bounded page of retained chain events strictly after the request cursor.
    fn chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, StoreError> {
        let read_view = self.read_view();
        let current_event_sequence = read_current_chain_event_sequence(&read_view)?;
        if current_event_sequence == 0 {
            if request.from_cursor.is_some() {
                return Err(StoreError::ChainEventCursorInvalid {
                    reason: "cursor sequence is ahead of retained history",
                });
            }

            return Ok(Vec::new());
        }

        let current_chain_epoch = require_current_chain_epoch(&read_view)?;
        let oldest_retained_sequence =
            read_oldest_retained_chain_event_sequence(&read_view, current_event_sequence)?
                .unwrap_or(1);
        let resume = self.chain_event_history_start_sequence(
            &read_view,
            request,
            &current_chain_epoch,
            ChainEventHistoryBounds {
                current_event_sequence,
                oldest_retained_sequence,
            },
        )?;
        let ChainEventResume {
            start_sequence,
            family,
            synthetic_reorg,
        } = resume;

        let max_events = u64::from(request.max_events.get());
        let mut event_envelopes = Vec::with_capacity(u32_to_usize(request.max_events.get()));
        if let Some(synthetic_reorg) = synthetic_reorg {
            event_envelopes.push(synthetic_reorg);
        }

        let mut event_sequence = start_sequence;
        while event_sequence <= current_event_sequence
            && u64::try_from(event_envelopes.len()).map_or(true, |count| count < max_events)
        {
            let key = StoreKey::chain_event(event_sequence);
            let Some(record_bytes) = read_view.get(StorageTable::ChainEvent, &key)? else {
                return Err(StoreError::ChainEventCursorExpired {
                    event_sequence: start_sequence.saturating_sub(1),
                    oldest_retained_sequence: event_sequence,
                });
            };
            let mut event_envelope =
                decode_chain_event_envelope(&key, &record_bytes, family, self.cursor_auth_key)?;
            if chain_event_matches_family(&event_envelope, family) {
                enrich_chain_event_cursor(
                    &read_view,
                    &mut event_envelope,
                    family,
                    self.cursor_auth_key,
                )?;
                event_envelopes.push(event_envelope);
            }
            event_sequence = event_sequence
                .checked_add(1)
                .ok_or(StoreError::ChainEventSequenceOverflow)?;
        }

        Ok(event_envelopes)
    }

    /// Reads a bounded page of unspent transparent outputs.
    fn address_output_index_page(
        &self,
        request: AddressOutputIndexPageRequest<'_>,
    ) -> Result<AddressOutputIndexPage, StoreError> {
        let read_view = self.read_view();
        let chain_epoch = match request.at_epoch {
            Some(at_epoch) => {
                let stored = read_chain_epoch(&read_view, at_epoch.id)?;
                if stored != at_epoch {
                    return Err(StoreError::ChainEpochMissing {
                        chain_epoch: at_epoch.id,
                    });
                }
                stored
            }
            None => require_current_chain_epoch(&read_view)?,
        };

        let resume_after = match request.from_cursor {
            Some(cursor) => Some(
                cursor
                    .decode_address_output(chain_epoch.network, self.cursor_auth_key)
                    .map_err(|_| StoreError::AddressOutputCursorInvalid {
                        reason: "cursor token failed validation",
                    })?,
            ),
            None => None,
        };

        let outputs = crate::address_output_index::read_address_output_index_rows_paged(
            &read_view,
            crate::address_output_index::AddressOutputIndexRowsScan {
                chain_epoch,
                address_script_hash: request.address_script_hash,
                start_height: request.start_height,
                max_entries: request.max_entries,
                resume_after,
            },
        )?;

        let next_cursor = if outputs.len() >= u32_to_usize(request.max_entries.get()) {
            match outputs.last() {
                Some(output) => Some(
                    StreamCursorTokenV1::address_output(
                        chain_epoch.network,
                        output.block_height,
                        output.outpoint,
                        self.cursor_auth_key,
                    )
                    .map_err(|_| StoreError::AddressOutputCursorInvalid {
                        reason: "next-cursor encoding failed",
                    })?,
                ),
                None => None,
            }
        } else {
            None
        };

        Ok(AddressOutputIndexPage {
            chain_epoch,
            outputs,
            next_cursor,
        })
    }

    fn chain_event_history_start_sequence(
        &self,
        inner: &impl RocksChainStoreRead,
        request: ChainEventHistoryRequest<'_>,
        current_chain_epoch: &ChainEpoch,
        bounds: ChainEventHistoryBounds,
    ) -> Result<ChainEventResume, StoreError> {
        resolve_event_history_start_sequence(
            request.from_cursor,
            || ChainEventResume {
                start_sequence: bounds.oldest_retained_sequence,
                family: request.family,
                synthetic_reorg: None,
            },
            |cursor| self.chain_event_cursor_resume(inner, cursor, current_chain_epoch, bounds),
        )
    }

    /// Resolves the resume position for a chain-event cursor: the
    /// position-check hook for the chain-event family.
    ///
    /// Authenticates the cursor, rejects forged sequences as
    /// `ChainEventCursorInvalid`, then resolves the locator fork point. A
    /// cursor still on the canonical chain resumes from the next event; one
    /// reorged out below its tip routes through
    /// [`resolve_reorged_cursor_resume`], which replays the retained reorg or
    /// synthesizes a `ChainReorged` ahead of the page.
    fn chain_event_cursor_resume(
        &self,
        inner: &impl RocksChainStoreRead,
        cursor: &StreamCursorTokenV1,
        current_chain_epoch: &ChainEpoch,
        bounds: ChainEventHistoryBounds,
    ) -> Result<ChainEventResume, StoreError> {
        let cursor_payload = cursor
            .decode_chain_event(self.stream_network()?, self.cursor_auth_key)
            .map_err(|_| StoreError::ChainEventCursorInvalid {
                reason: "cursor token failed validation",
            })?;
        let family = cursor_payload.family;

        // Genuine forgery: a zero or ahead-of-history sequence cannot name a
        // real delivered event.
        if cursor_payload.event_sequence == 0 {
            return Err(StoreError::ChainEventCursorInvalid {
                reason: "cursor sequence is before retained history",
            });
        }
        if cursor_payload.event_sequence > bounds.current_event_sequence {
            return Err(StoreError::ChainEventCursorInvalid {
                reason: "cursor sequence is ahead of retained history",
            });
        }

        let Some(fork_point) =
            resolve_locator_fork_point(inner, *current_chain_epoch, &cursor_payload.locator)?
        else {
            if retained_cursor_event_is_artifactless_checkpoint(
                inner,
                ArtifactlessCheckpointCursorInput {
                    event_sequence: cursor_payload.event_sequence,
                    oldest_retained_sequence: bounds.oldest_retained_sequence,
                    cursor_locator_tip: cursor_payload.locator.tip(),
                    family,
                    cursor_auth_key: self.cursor_auth_key,
                },
            )? {
                return resume_after_retained_cursor_event(family, cursor_payload.event_sequence);
            }
            // No locator entry sits on the canonical chain: the divergence is
            // deeper than the cap or its block is unresolvable. The consumer
            // must re-derive from canonical artifacts.
            return Err(StoreError::ChainEventCursorExpired {
                event_sequence: cursor_payload.event_sequence,
                oldest_retained_sequence: bounds.oldest_retained_sequence,
            });
        };

        let locator_tip = cursor_payload.locator.tip();
        let cursor_branch_on_chain = fork_point == locator_tip;

        if cursor_branch_on_chain {
            // No reorg. The cursor's tip is still canonical. Resume from the
            // next event; expire only when that next event is itself below the
            // retention floor, which means events between the cursor and the
            // floor were pruned.
            let next_sequence = cursor_payload
                .event_sequence
                .checked_add(1)
                .ok_or(StoreError::ChainEventSequenceOverflow)?;
            if next_sequence < bounds.oldest_retained_sequence {
                return Err(StoreError::ChainEventCursorExpired {
                    event_sequence: cursor_payload.event_sequence,
                    oldest_retained_sequence: bounds.oldest_retained_sequence,
                });
            }
            return Ok(ChainEventResume {
                start_sequence: next_sequence.max(bounds.oldest_retained_sequence),
                family,
                synthetic_reorg: None,
            });
        }

        // The cursor's branch was reorged out below its tip.
        resolve_reorged_cursor_resume(
            inner,
            ReorgedCursorResume {
                current_chain_epoch: *current_chain_epoch,
                family,
                fork_point,
                reverted_tip: locator_tip,
                event_sequence: cursor_payload.event_sequence,
                cursor_auth_key: self.cursor_auth_key,
                bounds,
            },
        )
    }

    fn prune_chain_events_before(
        &self,
        cutoff_created_at: UnixTimestampMillis,
    ) -> Result<ChainEventRetentionReport, StoreError> {
        let started_at = Instant::now();
        let prune_outcome = (|| {
            let _control_guard = self.inner.lock_control();
            let current_event_sequence = read_current_chain_event_sequence(self.inner.as_ref())?;
            let Some(oldest_retained_sequence) = read_oldest_retained_chain_event_sequence(
                self.inner.as_ref(),
                current_event_sequence,
            )?
            else {
                return Ok(ChainEventRetentionReport::empty());
            };

            let new_oldest_retained_sequence = self.oldest_retained_sequence_for_cutoff(
                oldest_retained_sequence,
                current_event_sequence,
                cutoff_created_at,
            )?;
            let pruned_event_count =
                new_oldest_retained_sequence.saturating_sub(oldest_retained_sequence);

            if pruned_event_count > 0 {
                let deletes = (oldest_retained_sequence..new_oldest_retained_sequence)
                    .map(|event_sequence| StorageDelete {
                        table: StorageTable::ChainEvent,
                        key: StoreKey::chain_event(event_sequence),
                    })
                    .collect();
                self.inner.write_batch(
                    vec![StoragePut {
                        table: StorageTable::StorageControl,
                        key: StoreKey::oldest_retained_chain_event_sequence(),
                        value: new_oldest_retained_sequence.to_be_bytes().to_vec(),
                    }],
                    deletes,
                )?;
            }

            self.chain_event_retention_report_locked()
                .map(|report| ChainEventRetentionReport {
                    pruned_event_count,
                    ..report
                })
        })();
        record_chain_event_prune_outcome(started_at, &prune_outcome);
        if let Ok(report) = prune_outcome {
            record_chain_event_retention_report(report);
            return Ok(report);
        }

        prune_outcome
    }

    fn oldest_retained_sequence_for_cutoff(
        &self,
        oldest_retained_sequence: u64,
        current_event_sequence: u64,
        cutoff_created_at: UnixTimestampMillis,
    ) -> Result<u64, StoreError> {
        let mut event_sequence = oldest_retained_sequence;
        while event_sequence < current_event_sequence {
            let key = StoreKey::chain_event(event_sequence);
            let Some(record_bytes) =
                self.inner
                    .get(StoreReadCaller::Query, StorageTable::ChainEvent, &key)?
            else {
                event_sequence = event_sequence
                    .checked_add(1)
                    .ok_or(StoreError::ChainEventSequenceOverflow)?;
                continue;
            };
            let event_envelope = decode_chain_event_envelope(
                &key,
                &record_bytes,
                ChainEventStreamFamily::Visible,
                self.cursor_auth_key,
            )?;
            if event_envelope.chain_epoch.created_at >= cutoff_created_at {
                break;
            }
            event_sequence = event_sequence
                .checked_add(1)
                .ok_or(StoreError::ChainEventSequenceOverflow)?;
        }

        Ok(event_sequence)
    }

    fn chain_event_retention_report(&self) -> Result<ChainEventRetentionReport, StoreError> {
        self.chain_event_retention_report_via(&self.read_view())
    }

    fn chain_event_retention_report_locked(&self) -> Result<ChainEventRetentionReport, StoreError> {
        self.chain_event_retention_report_via(self.inner.as_ref())
    }

    fn chain_event_retention_report_via<R: RocksChainStoreRead>(
        &self,
        read_source: &R,
    ) -> Result<ChainEventRetentionReport, StoreError> {
        let current_event_sequence = read_current_chain_event_sequence(read_source)?;
        let Some(oldest_retained_sequence) =
            read_oldest_retained_chain_event_sequence(read_source, current_event_sequence)?
        else {
            return Ok(ChainEventRetentionReport::empty());
        };
        build_chain_event_retention_report(
            read_source,
            oldest_retained_sequence,
            current_event_sequence,
            self.cursor_auth_key,
            0,
        )
    }

    fn current_chain_epoch_id(&self) -> Result<Option<ChainEpochId>, StoreError> {
        let read_view = self.read_view();
        read_current_chain_epoch_id(&read_view)
    }

    fn current_chain_event_sequence(&self) -> Result<u64, StoreError> {
        read_current_chain_event_sequence(self.inner.as_ref())
    }

    fn append_mempool_event(
        &self,
        event: MempoolEvent,
        source_observed_at: UnixTimestampMillis,
    ) -> Result<MempoolEventEnvelope, StoreError> {
        let _control_guard = self.inner.lock_control();
        let network = self.mempool_network()?;
        let event_sequence = read_current_mempool_event_sequence(self.inner.as_ref())?
            .checked_add(1)
            .ok_or(StoreError::MempoolEventSequenceOverflow)?;
        let cursor = StreamCursorTokenV1::mempool_event(
            network,
            event_sequence,
            event.transaction_id(),
            self.cursor_auth_key,
        )
        .map_err(|_| StoreError::InvalidChainEpochArtifacts {
            reason: "cursor authentication key could not initialize the MAC",
        })?;
        let envelope = MempoolEventEnvelope {
            cursor,
            event_sequence,
            source_observed_unix_millis: source_observed_at.value(),
            event,
        };

        let mut puts = vec![
            StoragePut {
                table: StorageTable::MempoolEvent,
                key: StoreKey::mempool_event(event_sequence),
                value: encode_mempool_event_envelope(&envelope)?,
            },
            StoragePut {
                table: StorageTable::StorageControl,
                key: StoreKey::mempool_event_sequence_pointer(),
                value: event_sequence.to_be_bytes().to_vec(),
            },
        ];
        if event_sequence == 1 {
            puts.push(StoragePut {
                table: StorageTable::StorageControl,
                key: StoreKey::oldest_retained_mempool_event_sequence(),
                value: event_sequence.to_be_bytes().to_vec(),
            });
        }
        self.inner.write_batch(puts, Vec::new())?;

        Ok(envelope)
    }

    fn mempool_event_history(
        &self,
        request: MempoolEventHistoryRequest<'_>,
    ) -> Result<Vec<MempoolEventEnvelope>, StoreError> {
        let network = self.mempool_network()?;
        let read_view = self.read_view();
        let current_event_sequence = read_current_mempool_event_sequence(&read_view)?;
        if current_event_sequence == 0 {
            if request.from_cursor.is_some() {
                return Err(StoreError::MempoolEventCursorInvalid {
                    reason: "cursor sequence is ahead of retained history",
                });
            }
            return Ok(Vec::new());
        }

        let oldest_retained_sequence =
            read_oldest_retained_mempool_event_sequence(&read_view, current_event_sequence)?
                .unwrap_or(1);
        let start_sequence = self.mempool_event_history_start_sequence(
            &read_view,
            request,
            network,
            MempoolEventHistoryBounds {
                current_event_sequence,
                oldest_retained_sequence,
            },
        )?;

        if start_sequence > current_event_sequence {
            return Ok(Vec::new());
        }

        let max_events = u32_to_usize(request.max_events.get());
        let mut event_envelopes = Vec::with_capacity(max_events);
        let mut decode_error: Option<StoreError> = None;
        let cursor_auth_key = self.cursor_auth_key;
        read_view.scan_forward(
            StorageTable::MempoolEvent,
            &StoreKey::mempool_event(start_sequence),
            &mut |key_bytes, record_bytes| {
                if event_envelopes.len() >= max_events {
                    return Ok(PrefixScanControl::Stop);
                }
                let key = StoreKey::from_raw_bytes(key_bytes);
                match decode_mempool_event_envelope(&key, record_bytes, network, cursor_auth_key) {
                    Ok(envelope) => {
                        if envelope.event_sequence > current_event_sequence {
                            return Ok(PrefixScanControl::Stop);
                        }
                        event_envelopes.push(envelope);
                        Ok(PrefixScanControl::Continue)
                    }
                    Err(error) => {
                        decode_error = Some(error);
                        Ok(PrefixScanControl::Stop)
                    }
                }
            },
        )?;
        if let Some(error) = decode_error {
            return Err(error);
        }

        // If the first observed sequence is past `start_sequence`, the
        // requested cursor's resume point was pruned between the bounds read
        // and the iteration. Surface this as `MempoolEventCursorExpired` so
        // callers resubscribe to the new oldest-retained boundary instead
        // of silently skipping events.
        if let Some(first_envelope) = event_envelopes.first()
            && first_envelope.event_sequence > start_sequence
        {
            return Err(StoreError::MempoolEventCursorExpired {
                event_sequence: start_sequence.saturating_sub(1),
                oldest_retained_sequence: first_envelope.event_sequence,
            });
        }

        Ok(event_envelopes)
    }

    /// Resolves an event-stream start position for the chain-event family.
    ///
    /// `AfterCursor` authenticates the cursor and takes its encoded family as
    /// authoritative, rejecting a non-default request family that disagrees.
    /// `LiveTail` mints a cursor at the current head so the page loop resumes
    /// strictly after it, delivering only events applied after subscription.
    fn resolve_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StoreError> {
        match start {
            EventStreamStartPosition::AfterCursor(cursor) => {
                let payload = cursor
                    .decode_chain_event(self.stream_network()?, self.cursor_auth_key)
                    .map_err(|_| StoreError::ChainEventCursorInvalid {
                        reason: "cursor token failed validation",
                    })?;
                if requested_family != ChainEventStreamFamily::Visible
                    && requested_family != payload.family
                {
                    return Err(StoreError::ChainEventCursorInvalid {
                        reason: "request family does not match the cursor's encoded family",
                    });
                }
                Ok(ChainEventStreamResume {
                    cursor: Some(cursor.clone()),
                    family: payload.family,
                })
            }
            EventStreamStartPosition::EarliestRetained => Ok(ChainEventStreamResume {
                cursor: None,
                family: requested_family,
            }),
            EventStreamStartPosition::LiveTail => {
                let read_view = self.read_view();
                let current_event_sequence = read_current_chain_event_sequence(&read_view)?;
                if current_event_sequence == 0 {
                    return Ok(ChainEventStreamResume {
                        cursor: None,
                        family: requested_family,
                    });
                }
                let chain_epoch = require_current_chain_epoch(&read_view)?;
                let cursor = mint_visible_chain_event_cursor(
                    &read_view,
                    chain_epoch,
                    requested_family,
                    current_event_sequence,
                    self.cursor_auth_key,
                )?;
                Ok(ChainEventStreamResume {
                    cursor: Some(cursor),
                    family: requested_family,
                })
            }
        }
    }

    /// Resolves an event-stream start position for the mempool-event family.
    ///
    /// `LiveTail` returns the newest retained envelope's own cursor; a
    /// missing newest row degrades to the retention floor, which only
    /// widens delivery (at-least-once, never loss).
    fn resolve_mempool_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
    ) -> Result<Option<StreamCursorTokenV1>, StoreError> {
        let network = self.mempool_network()?;
        match start {
            EventStreamStartPosition::AfterCursor(cursor) => {
                cursor
                    .decode_mempool_event(network, self.cursor_auth_key)
                    .map_err(|_| StoreError::MempoolEventCursorInvalid {
                        reason: "cursor token failed validation",
                    })?;
                Ok(Some(cursor.clone()))
            }
            EventStreamStartPosition::EarliestRetained => Ok(None),
            EventStreamStartPosition::LiveTail => {
                let read_view = self.read_view();
                let current_event_sequence = read_current_mempool_event_sequence(&read_view)?;
                if current_event_sequence == 0 {
                    return Ok(None);
                }
                let key = StoreKey::mempool_event(current_event_sequence);
                let Some(record_bytes) = read_view.get(StorageTable::MempoolEvent, &key)? else {
                    return Ok(None);
                };
                let position = decode_mempool_event_position(&key, &record_bytes)?;
                let cursor = StreamCursorTokenV1::mempool_event(
                    network,
                    position.event_sequence,
                    position.transaction_id,
                    self.cursor_auth_key,
                )
                .map_err(|_| StoreError::InvalidChainEpochArtifacts {
                    reason: "cursor authentication key could not initialize the MAC",
                })?;
                Ok(Some(cursor))
            }
        }
    }

    fn stream_network(&self) -> Result<Network, StoreError> {
        match self.options.network {
            Some(network) => Ok(network),
            None => Ok(require_current_chain_epoch(&self.read_view())?.network),
        }
    }

    fn mempool_network(&self) -> Result<Network, StoreError> {
        self.options
            .network
            .ok_or(StoreError::InvalidChainStoreOptions {
                reason: "mempool events require a network-bound store",
            })
    }

    fn mempool_event_history_start_sequence(
        &self,
        inner: &impl RocksChainStoreRead,
        request: MempoolEventHistoryRequest<'_>,
        network: Network,
        bounds: MempoolEventHistoryBounds,
    ) -> Result<u64, StoreError> {
        resolve_event_history_start_sequence(
            request.from_cursor,
            || bounds.oldest_retained_sequence,
            |cursor| self.mempool_event_cursor_resume(inner, cursor, network, bounds),
        )
    }

    /// Resolves the resume sequence for a mempool-event cursor: the
    /// position-check hook for the mempool family.
    ///
    /// Authenticates the cursor, rejects an ahead-of-history sequence as
    /// `MempoolEventCursorInvalid` and a below-floor sequence as
    /// `MempoolEventCursorExpired`, then verifies the cursor's claimed
    /// position against the stored event before trusting it. The mempool
    /// cursor bookmarks a `(sequence, transaction_id)` pair rather than a
    /// `(height, hash)` fork anchor, so this check confirms the retained
    /// event at the cursor's sequence still carries the bookmarked
    /// transaction id; a mismatch means a stale or forged cursor and yields
    /// `MempoolEventCursorInvalid`.
    fn mempool_event_cursor_resume(
        &self,
        inner: &impl RocksChainStoreRead,
        cursor: &StreamCursorTokenV1,
        network: Network,
        bounds: MempoolEventHistoryBounds,
    ) -> Result<u64, StoreError> {
        let cursor_payload = cursor
            .decode_mempool_event(network, self.cursor_auth_key)
            .map_err(|_| StoreError::MempoolEventCursorInvalid {
                reason: "cursor token failed validation",
            })?;
        if cursor_payload.event_sequence > bounds.current_event_sequence {
            return Err(StoreError::MempoolEventCursorInvalid {
                reason: "cursor sequence is ahead of retained history",
            });
        }
        if cursor_payload.event_sequence < bounds.oldest_retained_sequence {
            return Err(StoreError::MempoolEventCursorExpired {
                event_sequence: cursor_payload.event_sequence,
                oldest_retained_sequence: bounds.oldest_retained_sequence,
            });
        }

        let key = StoreKey::mempool_event(cursor_payload.event_sequence);
        if let Some(record_bytes) = inner.get(StorageTable::MempoolEvent, &key)? {
            let bookmarked =
                decode_mempool_event_envelope(&key, &record_bytes, network, self.cursor_auth_key)?;
            if bookmarked.transaction_id() != cursor_payload.last_transaction_id {
                return Err(StoreError::MempoolEventCursorInvalid {
                    reason: "cursor transaction id does not match the bookmarked event",
                });
            }
        }

        cursor_payload
            .event_sequence
            .checked_add(1)
            .ok_or(StoreError::MempoolEventSequenceOverflow)
    }

    fn prune_mempool_events_before(
        &self,
        now: UnixTimestampMillis,
        retention: MempoolEventRetentionConfig,
    ) -> Result<MempoolEventRetentionReport, StoreError> {
        let started_at = Instant::now();
        let prune_outcome = self.prune_mempool_events_locked(now, retention);
        record_mempool_event_prune_outcome(started_at, &prune_outcome);
        if let Ok(report) = prune_outcome {
            record_mempool_event_retention_report(report);
            return Ok(report);
        }
        prune_outcome
    }

    fn prune_mempool_events_locked(
        &self,
        now: UnixTimestampMillis,
        retention: MempoolEventRetentionConfig,
    ) -> Result<MempoolEventRetentionReport, StoreError> {
        // Phase 1: read bounds + scan against a snapshot view, lock-free, so
        // the per-event `append_mempool_event` path is not blocked during the
        // potentially-large iteration. The snapshot freezes the input the
        // delete batch is computed from; concurrent appends after the
        // snapshot only widen the retained window and remain safe to leave
        // in place.
        let read_view = self.read_view();
        let current_event_sequence = read_current_mempool_event_sequence(&read_view)?;
        let Some(oldest_retained_sequence) =
            read_oldest_retained_mempool_event_sequence(&read_view, current_event_sequence)?
        else {
            return Ok(MempoolEventRetentionReport::default());
        };
        let scan = scan_mempool_events_for_pruning(
            &read_view,
            now,
            retention,
            oldest_retained_sequence,
            current_event_sequence,
        )?;
        let new_oldest_retained = scan.new_oldest_retained.unwrap_or(current_event_sequence);
        let observed_at = read_mempool_event_observed_at(&read_view, new_oldest_retained)?;
        drop(read_view);

        // Phase 2: acquire the control lock only to apply the writes.
        //
        // The floor must advance whenever the scan computed a higher
        // `new_oldest_retained`, regardless of whether `scan.deletes` is
        // empty. A previous prune that crashed mid-batch can leave the
        // column family with gaps where the deletes physically already
        // happened but the floor never advanced; without this branch, the
        // floor would stay stuck and readers could observe a partially
        // pruned tail. Every prune call updates
        // `oldest_retained_mempool_event_sequence` atomically with the
        // column-family delete batch.
        let floor_advances = new_oldest_retained != oldest_retained_sequence;
        if floor_advances || !scan.deletes.is_empty() {
            let _control_guard = self.inner.lock_control();
            let puts = if floor_advances {
                vec![StoragePut {
                    table: StorageTable::StorageControl,
                    key: StoreKey::oldest_retained_mempool_event_sequence(),
                    value: new_oldest_retained.to_be_bytes().to_vec(),
                }]
            } else {
                Vec::new()
            };
            self.inner.write_batch(puts, scan.deletes)?;
        }

        let retained_event_count = current_event_sequence
            .saturating_sub(new_oldest_retained)
            .saturating_add(1);
        Ok(MempoolEventRetentionReport {
            current_event_sequence,
            oldest_retained_sequence: Some(new_oldest_retained),
            oldest_retained_observed_at: observed_at,
            retained_event_count,
            pruned_added_count: scan.pruned_added,
            pruned_mined_count: scan.pruned_mined,
            pruned_invalidated_count: scan.pruned_invalidated,
        })
    }

    fn mempool_event_retention_report(&self) -> Result<MempoolEventRetentionReport, StoreError> {
        let read_view = self.read_view();
        let current_event_sequence = read_current_mempool_event_sequence(&read_view)?;
        let Some(oldest_retained_sequence) =
            read_oldest_retained_mempool_event_sequence(&read_view, current_event_sequence)?
        else {
            return Ok(MempoolEventRetentionReport::default());
        };
        let observed_at = read_mempool_event_observed_at(&read_view, oldest_retained_sequence)?;
        let retained_event_count = current_event_sequence
            .saturating_sub(oldest_retained_sequence)
            .saturating_add(1);
        Ok(MempoolEventRetentionReport {
            current_event_sequence,
            oldest_retained_sequence: Some(oldest_retained_sequence),
            oldest_retained_observed_at: observed_at,
            retained_event_count,
            pruned_added_count: 0,
            pruned_mined_count: 0,
            pruned_invalidated_count: 0,
        })
    }

    fn read_view(&self) -> crate::kv::RocksChainStoreReadView<'_> {
        self.read_view_for(StoreReadCaller::Query)
    }

    fn read_view_for(&self, caller: StoreReadCaller) -> crate::kv::RocksChainStoreReadView<'_> {
        match self.read_posture {
            ChainStoreReadPosture::Snapshot => self.inner.snapshot_read_view_for(caller),
            ChainStoreReadPosture::Direct => self.inner.direct_read_view_for(caller),
        }
    }

    /// Runs one bounded transparent retention pass independently of canonical
    /// chain advancement.
    fn sweep_transparent_retention_once(
        &self,
    ) -> Result<TransparentRetentionSweepOutcome, StoreError> {
        let started_at = Instant::now();
        let sweep_outcome = (|| {
            let sweep_read_view = self
                .inner
                .direct_read_view_for(StoreReadCaller::RetentionSweep);
            let Some(chain_epoch_id) = self.current_chain_epoch_id()? else {
                return Ok(TransparentRetentionSweepOutcome::default());
            };
            let chain_epoch = read_chain_epoch(&sweep_read_view, chain_epoch_id)?;
            let retention_sweep = build_transparent_retention_sweep(
                &sweep_read_view,
                chain_epoch,
                self.options.retention_sweep_max_heights_per_pass,
                self.options.retention_sweep_max_outpoints_per_pass,
            )?;
            let outcome = retention_sweep.outcome();
            if retention_sweep.puts.is_empty() && retention_sweep.deletes.is_empty() {
                return Ok(outcome);
            }

            let _control_guard = self.inner.lock_control();
            let commit_read_view = self
                .inner
                .direct_read_view_for(StoreReadCaller::CommitFallback);
            if !retention_sweep.swept_marker_unchanged(&commit_read_view)? {
                return Ok(TransparentRetentionSweepOutcome::default());
            }

            let swept_outpoints = retention_sweep.swept_outpoints;
            let retention_sweep_advance = retention_sweep.advance;
            self.inner
                .write_batch(retention_sweep.puts, retention_sweep.deletes)?;
            record_transparent_retention_sweep(swept_outpoints);
            log_transparent_retention_sweep(retention_sweep_advance.as_ref());
            Ok(outcome)
        })();
        record_transparent_retention_sweep_outcome(started_at, &sweep_outcome);
        sweep_outcome
    }

    fn validate_commit_order(
        &self,
        chain_epoch: &ChainEpoch,
    ) -> Result<Option<ChainEpoch>, StoreError> {
        let Some(current_chain_epoch) = self.current_chain_epoch_id()? else {
            if chain_epoch.id != ChainEpochId::new(1) {
                return Err(StoreError::InvalidChainEpochArtifacts {
                    reason: "first chain epoch id must be 1",
                });
            }
            return Ok(None);
        };

        if chain_epoch.id <= current_chain_epoch {
            return Err(StoreError::ChainEpochConflict {
                current: current_chain_epoch,
                attempted: chain_epoch.id,
            });
        }
        if current_chain_epoch
            .value()
            .checked_add(1)
            .is_none_or(|expected| chain_epoch.id != ChainEpochId::new(expected))
        {
            return Err(StoreError::InvalidChainEpochArtifacts {
                reason: "chain epoch id must increase by exactly one",
            });
        }

        let current_epoch = read_chain_epoch(self.inner.as_ref(), current_chain_epoch)?;
        if current_epoch.network != chain_epoch.network {
            return Err(StoreError::ChainEpochNetworkMismatch {
                current: current_epoch.network,
                attempted: chain_epoch.network,
            });
        }

        Ok(Some(current_epoch))
    }
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "zinder-core rejects targets with pointer widths below 32 bits"
)]
const fn u32_to_usize(count: u32) -> usize {
    count as usize
}

fn protected_transparent_outpoints(
    artifacts: &ChainEpochArtifacts,
) -> (
    HashMap<TransparentOutPoint, BlockHeight>,
    HashSet<TransparentOutPoint>,
) {
    let outputs = artifacts
        .transparent_outputs_by_outpoint
        .iter()
        .map(|output| (output.outpoint, output.block_height))
        .collect();
    let spends = artifacts
        .transparent_spend_facts
        .iter()
        .map(|spend| spend.spent_outpoint)
        .collect();
    (outputs, spends)
}

fn validate_stored_block_value_pool_balances(
    store: &RocksChainStore,
    artifact_key: &StoreKey,
    balances: &BlockValuePoolBalances,
) -> Result<(), StoreError> {
    let Some(existing_bytes) = store.get(
        StoreReadCaller::CommitFallback,
        StorageTable::BlockValuePoolBalances,
        artifact_key,
    )?
    else {
        return Ok(());
    };
    let existing = decode_block_value_pool_balances(artifact_key, &existing_bytes)?;
    if existing != *balances {
        return Err(StoreError::InvalidChainEpochArtifacts {
            reason: "block value-pool balance enrichment conflicts with stored balances",
        });
    }
    Ok(())
}

impl ChainEpochReadApi for PrimaryChainStore {
    fn canonical_history_bounds(&self) -> Result<Option<CanonicalHistoryBounds>, StoreError> {
        self.canonical_history_bounds()
    }

    fn current_chain_epoch_reader(&self) -> Result<ChainEpochReader<'_>, StoreError> {
        self.current_chain_epoch_reader()
    }

    fn chain_epoch_reader_at(
        &self,
        chain_epoch: ChainEpochId,
    ) -> Result<ChainEpochReader<'_>, StoreError> {
        self.chain_epoch_reader_at(chain_epoch)
    }

    fn chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, StoreError> {
        self.chain_event_history(request)
    }

    fn resolve_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StoreError> {
        self.resolve_chain_event_stream_start(start, requested_family)
    }

    fn address_output_index_page(
        &self,
        request: AddressOutputIndexPageRequest<'_>,
    ) -> Result<AddressOutputIndexPage, StoreError> {
        self.store.address_output_index_page(request)
    }
}

impl ChainEpochReadApi for SecondaryChainStore {
    fn canonical_history_bounds(&self) -> Result<Option<CanonicalHistoryBounds>, StoreError> {
        self.canonical_history_bounds()
    }

    fn current_chain_epoch_reader(&self) -> Result<ChainEpochReader<'_>, StoreError> {
        self.current_chain_epoch_reader()
    }

    fn chain_epoch_reader_at(
        &self,
        chain_epoch: ChainEpochId,
    ) -> Result<ChainEpochReader<'_>, StoreError> {
        self.chain_epoch_reader_at(chain_epoch)
    }

    fn chain_event_history(
        &self,
        request: ChainEventHistoryRequest<'_>,
    ) -> Result<Vec<ChainEventEnvelope>, StoreError> {
        self.chain_event_history(request)
    }

    fn resolve_chain_event_stream_start(
        &self,
        start: &EventStreamStartPosition,
        requested_family: ChainEventStreamFamily,
    ) -> Result<ChainEventStreamResume, StoreError> {
        self.resolve_chain_event_stream_start(start, requested_family)
    }

    fn address_output_index_page(
        &self,
        request: AddressOutputIndexPageRequest<'_>,
    ) -> Result<AddressOutputIndexPage, StoreError> {
        self.store.address_output_index_page(request)
    }
}

impl crate::DisplacedBlockStore for PrimaryChainStore {
    fn displaced_block_page(
        &self,
        after: Option<&crate::DisplacedBlockCursor>,
        limit: NonZeroU32,
    ) -> Result<crate::DisplacedBlockPage, StoreError> {
        let network = self
            .store
            .options
            .network
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced block archive reads require an explicit network",
            })?;
        read_displaced_block_page(&self.store.read_view(), network, after, limit)
    }

    fn newest_displaced_blocks(
        &self,
        limit: NonZeroU32,
    ) -> Result<Vec<DisplacedBlock>, StoreError> {
        let network = self
            .store
            .options
            .network
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced block archive reads require an explicit network",
            })?;
        read_newest_displaced_blocks(&self.store.read_view(), network, limit)
    }

    fn displaced_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<DisplacedBlock>, StoreError> {
        let network = self
            .store
            .options
            .network
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced block archive reads require an explicit network",
            })?;
        read_displaced_block_by_hash(&self.store.read_view(), network, block_hash)
    }

    fn displaced_blocks_for_event(
        &self,
        event_sequence: u64,
        limit: NonZeroU32,
    ) -> Result<Vec<DisplacedBlock>, StoreError> {
        let network = self
            .store
            .options
            .network
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced block archive reads require an explicit network",
            })?;
        read_displaced_blocks_for_event(&self.store.read_view(), network, event_sequence, limit)
    }

    fn displaced_block_count(&self) -> Result<u64, StoreError> {
        read_displaced_block_count(&self.store.read_view())
    }

    fn displaced_block_archive_coverage(
        &self,
    ) -> Result<Option<DisplacedBlockArchiveCoverage>, StoreError> {
        read_displaced_block_archive_coverage(&self.store.read_view())
    }
}

impl crate::DisplacedBlockStore for SecondaryChainStore {
    fn displaced_block_page(
        &self,
        after: Option<&crate::DisplacedBlockCursor>,
        limit: NonZeroU32,
    ) -> Result<crate::DisplacedBlockPage, StoreError> {
        let network = self
            .store
            .options
            .network
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced block archive reads require an explicit network",
            })?;
        read_displaced_block_page(&self.store.read_view(), network, after, limit)
    }

    fn newest_displaced_blocks(
        &self,
        limit: NonZeroU32,
    ) -> Result<Vec<DisplacedBlock>, StoreError> {
        let network = self
            .store
            .options
            .network
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced block archive reads require an explicit network",
            })?;
        read_newest_displaced_blocks(&self.store.read_view(), network, limit)
    }

    fn displaced_block_by_hash(
        &self,
        block_hash: BlockHash,
    ) -> Result<Option<DisplacedBlock>, StoreError> {
        let network = self
            .store
            .options
            .network
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced block archive reads require an explicit network",
            })?;
        read_displaced_block_by_hash(&self.store.read_view(), network, block_hash)
    }

    fn displaced_blocks_for_event(
        &self,
        event_sequence: u64,
        limit: NonZeroU32,
    ) -> Result<Vec<DisplacedBlock>, StoreError> {
        let network = self
            .store
            .options
            .network
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "displaced block archive reads require an explicit network",
            })?;
        read_displaced_blocks_for_event(&self.store.read_view(), network, event_sequence, limit)
    }

    fn displaced_block_count(&self) -> Result<u64, StoreError> {
        read_displaced_block_count(&self.store.read_view())
    }

    fn displaced_block_archive_coverage(
        &self,
    ) -> Result<Option<DisplacedBlockArchiveCoverage>, StoreError> {
        read_displaced_block_archive_coverage(&self.store.read_view())
    }
}

fn ensure_store_metadata(
    inner: &RocksChainStore,
    expected_network: Network,
) -> Result<(), StoreError> {
    if let Some(store_metadata_put) = validate_store_metadata_for_commit(inner, expected_network)? {
        inner.write(vec![store_metadata_put])?;
    }

    Ok(())
}

fn validate_store_metadata(
    inner: &impl RocksChainStoreRead,
    expected_network: Network,
) -> Result<(), StoreError> {
    let key = StoreKey::store_metadata();
    let Some(metadata_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Err(StoreError::ArtifactMissing {
            family: ArtifactFamily::ChainEpoch,
            key: key.into(),
        });
    };
    let store_metadata = decode_current_store_metadata(&key, &metadata_bytes)?;
    if store_metadata.network != expected_network {
        return Err(StoreError::ChainEpochNetworkMismatch {
            current: store_metadata.network,
            attempted: expected_network,
        });
    }

    Ok(())
}

fn ensure_supported_artifact_schema(inner: &impl RocksChainStoreRead) -> Result<(), StoreError> {
    let Some(current_chain_epoch_id) = read_current_chain_epoch_id(inner)? else {
        return Ok(());
    };
    let current_chain_epoch = read_chain_epoch(inner, current_chain_epoch_id)?;
    let persisted_version = current_chain_epoch.artifact_schema_version.value();
    if persisted_version > MAX_SUPPORTED_ARTIFACT_SCHEMA_VERSION {
        return Err(StoreError::SchemaTooNew {
            persisted_version,
            supported_version: MAX_SUPPORTED_ARTIFACT_SCHEMA_VERSION,
        });
    }
    if persisted_version < MIN_SUPPORTED_ARTIFACT_SCHEMA_VERSION {
        return Err(StoreError::SchemaTooOld {
            persisted_version,
            required_version: MIN_SUPPORTED_ARTIFACT_SCHEMA_VERSION,
        });
    }

    Ok(())
}

fn validate_store_metadata_for_commit(
    inner: &RocksChainStore,
    expected_network: Network,
) -> Result<Option<StoragePut>, StoreError> {
    let key = StoreKey::store_metadata();
    if let Some(metadata_bytes) =
        inner.get(StoreReadCaller::Query, StorageTable::StorageControl, &key)?
    {
        let store_metadata = decode_current_store_metadata(&key, &metadata_bytes)?;
        if store_metadata.network != expected_network {
            return Err(StoreError::ChainEpochNetworkMismatch {
                current: store_metadata.network,
                attempted: expected_network,
            });
        }

        return Ok(None);
    }

    if let Some(current_chain_epoch_id) = read_current_chain_epoch_id(inner)? {
        let current_chain_epoch = read_chain_epoch(inner, current_chain_epoch_id)?;
        if current_chain_epoch.network != expected_network {
            return Err(StoreError::ChainEpochNetworkMismatch {
                current: current_chain_epoch.network,
                attempted: expected_network,
            });
        }
    }

    Ok(Some(StoragePut {
        table: StorageTable::StorageControl,
        key,
        value: encode_store_metadata(expected_network),
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoreMetadata {
    schema_version: u16,
    network: Network,
}

fn encode_store_metadata(network: Network) -> Vec<u8> {
    let mut metadata = Vec::with_capacity(6);
    metadata.extend_from_slice(&CURRENT_STORE_SCHEMA_VERSION.to_be_bytes());
    metadata.extend_from_slice(&network.id().to_be_bytes());
    metadata
}

fn decode_current_store_metadata(
    key: &StoreKey,
    metadata_bytes: &[u8],
) -> Result<StoreMetadata, StoreError> {
    let store_metadata = decode_store_metadata(key, metadata_bytes)?;
    if store_metadata.schema_version != CURRENT_STORE_SCHEMA_VERSION {
        return Err(StoreError::SchemaMismatch {
            persisted_version: store_metadata.schema_version,
            expected_version: CURRENT_STORE_SCHEMA_VERSION,
        });
    }

    Ok(store_metadata)
}

fn decode_store_metadata(
    key: &StoreKey,
    metadata_bytes: &[u8],
) -> Result<StoreMetadata, StoreError> {
    if metadata_bytes.len() != 6 {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::ChainEpoch,
            key: key.clone().into(),
            reason: "store metadata must be 6 bytes",
        });
    }

    let schema_version_bytes =
        <[u8; 2]>::try_from(&metadata_bytes[0..2]).map_err(|_| StoreError::ArtifactCorrupt {
            family: ArtifactFamily::ChainEpoch,
            key: key.clone().into(),
            reason: "store metadata schema version must be 2 bytes",
        })?;
    let schema_version = u16::from_be_bytes(schema_version_bytes);

    let network_id_bytes =
        <[u8; 4]>::try_from(&metadata_bytes[2..6]).map_err(|_| StoreError::ArtifactCorrupt {
            family: ArtifactFamily::ChainEpoch,
            key: key.clone().into(),
            reason: "store metadata network id must be 4 bytes",
        })?;
    let network_id = u32::from_be_bytes(network_id_bytes);
    let network = Network::from_id(network_id).ok_or(StoreError::ArtifactCorrupt {
        family: ArtifactFamily::ChainEpoch,
        key: key.clone().into(),
        reason: "store metadata has an unknown network id",
    })?;

    Ok(StoreMetadata {
        schema_version,
        network,
    })
}

/// Heights a locator bookmarks, tip-first and exponentially back-spaced.
///
/// Yields `tip`, `tip-1`, `tip-2`, `tip-4`, `tip-8`, ... doubling the gap each
/// step, deduplicated and clamped at genesis, capped at
/// [`CHAIN_EVENT_LOCATOR_MAX`] entries.
fn locator_heights(tip_height: BlockHeight) -> Vec<BlockHeight> {
    let mut heights = Vec::with_capacity(CHAIN_EVENT_LOCATOR_MAX);
    let mut current = tip_height.value();
    let mut step = 1u32;
    loop {
        heights.push(BlockHeight::new(current));
        if heights.len() >= CHAIN_EVENT_LOCATOR_MAX || current == 0 {
            break;
        }
        current = current.saturating_sub(step);
        step = step.saturating_mul(2);
    }
    heights
}

/// Builds a chain-event locator by resolving the canonical block hash at each
/// back-spaced height from the block index.
///
/// The block index outlives the pruned event-log window, so the locator
/// resolves a fork point even when the divergence is no longer in retained
/// event history. Always returns at least the tip entry.
fn build_chain_event_locator(
    inner: &impl RocksChainStoreRead,
    chain_epoch: ChainEpoch,
    tip: ChainEventCursorAnchor,
) -> Result<ChainEventLocator, StoreError> {
    let mut entries = vec![tip];
    for height in locator_heights(tip.height).into_iter().skip(1) {
        match read_block_header_artifact(inner, chain_epoch, height) {
            Ok(Some(block)) => entries.push(ChainEventCursorAnchor {
                height,
                hash: block.block_hash,
            }),
            Ok(None) | Err(StoreError::ArtifactMissing { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    ChainEventLocator::new(entries).map_err(|_| StoreError::InvalidChainEpochArtifacts {
        reason: "chain-event locator exceeded its bound",
    })
}

/// Finds the fork point: the most recent locator entry whose hash equals the
/// canonical block hash at that height.
///
/// Returns `None` when no locator entry is on the canonical chain, which means
/// the divergence is deeper than the cap or the entry's block is unresolvable.
fn resolve_locator_fork_point(
    inner: &impl RocksChainStoreRead,
    chain_epoch: ChainEpoch,
    locator: &ChainEventLocator,
) -> Result<Option<ChainEventCursorAnchor>, StoreError> {
    for entry in locator.entries() {
        let canonical_hash = match read_block_header_artifact(inner, chain_epoch, entry.height) {
            Ok(Some(block)) => block.block_hash,
            Ok(None) | Err(StoreError::ArtifactMissing { .. }) => continue,
            Err(error) => return Err(error),
        };
        if canonical_hash == entry.hash {
            return Ok(Some(*entry));
        }
    }
    Ok(None)
}

fn cursor_event_is_retained(
    inner: &impl RocksChainStoreRead,
    event_sequence: u64,
    oldest_retained_sequence: u64,
) -> Result<bool, StoreError> {
    Ok(event_sequence >= oldest_retained_sequence
        && inner
            .get(
                StorageTable::ChainEvent,
                &StoreKey::chain_event(event_sequence),
            )?
            .is_some())
}

fn retained_cursor_event_is_artifactless_checkpoint(
    inner: &impl RocksChainStoreRead,
    input: ArtifactlessCheckpointCursorInput,
) -> Result<bool, StoreError> {
    if input.event_sequence < input.oldest_retained_sequence {
        return Ok(false);
    }

    let key = StoreKey::chain_event(input.event_sequence);
    let Some(record_bytes) = inner.get(StorageTable::ChainEvent, &key)? else {
        return Ok(false);
    };
    let event_envelope =
        decode_chain_event_envelope(&key, &record_bytes, input.family, input.cursor_auth_key)?;
    if !matches!(&event_envelope.event, ChainEvent::ChainCommitted { .. }) {
        return Ok(false);
    }

    let event_tip = ChainEventCursorAnchor {
        height: event_envelope.chain_epoch.visible_tip_height,
        hash: event_envelope.chain_epoch.visible_tip_hash,
    };
    if input.cursor_locator_tip != event_tip {
        return Ok(false);
    }

    match read_block_header_artifact(inner, event_envelope.chain_epoch, event_tip.height) {
        Ok(Some(_)) => Ok(false),
        Ok(None) | Err(StoreError::ArtifactMissing { .. }) => Ok(true),
        Err(error) => Err(error),
    }
}

#[derive(Clone, Copy, Debug)]
struct ArtifactlessCheckpointCursorInput {
    event_sequence: u64,
    oldest_retained_sequence: u64,
    cursor_locator_tip: ChainEventCursorAnchor,
    family: ChainEventStreamFamily,
    cursor_auth_key: [u8; 32],
}

fn resume_after_retained_cursor_event(
    family: ChainEventStreamFamily,
    event_sequence: u64,
) -> Result<ChainEventResume, StoreError> {
    Ok(ChainEventResume {
        start_sequence: event_sequence
            .checked_add(1)
            .ok_or(StoreError::ChainEventSequenceOverflow)?,
        family,
        synthetic_reorg: None,
    })
}

/// Resolves the resume position for a cursor whose branch was reorged out below
/// its tip, following the reconnect-reorg rule.
fn resolve_reorged_cursor_resume(
    inner: &impl RocksChainStoreRead,
    resume: ReorgedCursorResume,
) -> Result<ChainEventResume, StoreError> {
    let ReorgedCursorResume {
        current_chain_epoch,
        family,
        fork_point,
        reverted_tip,
        event_sequence,
        cursor_auth_key,
        bounds,
    } = resume;

    if matches!(family, ChainEventStreamFamily::Settled) {
        // A Settled cursor cannot be reorged out below the settled tip by
        // definition; a locator miss is an expiry, never a synthesized reorg,
        // and the Settled family never carries ChainReorged.
        return Err(StoreError::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence: bounds.oldest_retained_sequence,
        });
    }

    if cursor_event_is_retained(inner, event_sequence, bounds.oldest_retained_sequence)? {
        // The real ChainReorged event still sits at or after the cursor;
        // replaying it from the next sequence delivers the reorg.
        return resume_after_retained_cursor_event(family, event_sequence);
    }

    // The reorg event was pruned. Synthesize a ChainReorged reverted from the
    // locator-resolved fork point, then resume from the retention floor so the
    // retained events re-commit the post-fork canonical chain.
    let synthetic_reorg = build_synthetic_reorg_envelope(
        inner,
        SyntheticReorgInputs {
            current_chain_epoch,
            family,
            fork_point,
            reverted_tip,
            historical_event_sequence: event_sequence,
            cursor_auth_key,
            oldest_retained_sequence: bounds.oldest_retained_sequence,
        },
    )?;
    Ok(ChainEventResume {
        start_sequence: bounds.oldest_retained_sequence,
        family,
        synthetic_reorg: Some(synthetic_reorg),
    })
}

/// Inputs for [`build_synthetic_reorg_envelope`].
#[derive(Clone, Copy)]
struct SyntheticReorgInputs {
    current_chain_epoch: ChainEpoch,
    family: ChainEventStreamFamily,
    fork_point: ChainEventCursorAnchor,
    reverted_tip: ChainEventCursorAnchor,
    historical_event_sequence: u64,
    cursor_auth_key: [u8; 32],
    oldest_retained_sequence: u64,
}

/// Builds the synthetic `ChainReorged` envelope delivered ahead of the page
/// when a reconnecting cursor's branch was reorged out and the real reorg event
/// has been pruned.
///
/// The envelope reverts `(fork_point, reverted_tip_height]` and re-commits the
/// canonical range above the fork point. Its cursor bookmarks the on-chain fork
/// point one sequence below the retention floor, so a reconnect that has not
/// yet applied the reorg resumes from the retained events that re-commit the
/// canonical chain: recovery is idempotent and always makes forward progress.
fn build_synthetic_reorg_envelope(
    inner: &impl RocksChainStoreRead,
    inputs: SyntheticReorgInputs,
) -> Result<ChainEventEnvelope, StoreError> {
    let SyntheticReorgInputs {
        current_chain_epoch,
        family,
        fork_point,
        reverted_tip,
        historical_event_sequence,
        cursor_auth_key,
        oldest_retained_sequence,
    } = inputs;
    let reverted_start = fork_point
        .height
        .next()
        .ok_or(StoreError::ChainEventSequenceOverflow)?;
    let historical_epoch = read_chain_epoch(inner, ChainEpochId::new(historical_event_sequence))?;
    if reverted_tip
        != (ChainEventCursorAnchor {
            height: historical_epoch.visible_tip_height,
            hash: historical_epoch.visible_tip_hash,
        })
    {
        return Err(StoreError::ChainEventCursorInvalid {
            reason: "cursor tip does not match its historical chain epoch",
        });
    }
    let reverted = ChainRangeReverted::new(
        historical_epoch,
        BlockHeightRange::inclusive(reverted_start, reverted_tip.height),
    );
    let committed = ChainEpochCommitted::new(
        current_chain_epoch,
        BlockHeightRange::inclusive(reverted_start, current_chain_epoch.visible_tip_height),
    );
    let fork_locator = build_chain_event_locator(inner, current_chain_epoch, fork_point)?;
    let cursor_event_sequence = oldest_retained_sequence.saturating_sub(1);
    let cursor = StreamCursorTokenV1::chain_event(
        current_chain_epoch.network,
        family,
        cursor_event_sequence,
        &fork_locator,
        cursor_auth_key,
    )
    .map_err(|_| StoreError::InvalidChainEpochArtifacts {
        reason: "cursor authentication key could not initialize the MAC",
    })?;
    Ok(ChainEventEnvelope::new(
        cursor,
        cursor_event_sequence,
        current_chain_epoch,
        current_chain_epoch.settled_tip_height,
        ChainEvent::ChainReorged {
            reverted,
            committed,
        },
    ))
}

/// Mints a chain-event cursor anchored at the epoch's visible tip, carrying
/// the full back-spaced locator.
fn mint_visible_chain_event_cursor(
    inner: &impl RocksChainStoreRead,
    chain_epoch: ChainEpoch,
    family: ChainEventStreamFamily,
    event_sequence: u64,
    cursor_auth_key: [u8; 32],
) -> Result<StreamCursorTokenV1, StoreError> {
    let locator = build_chain_event_locator(
        inner,
        chain_epoch,
        ChainEventCursorAnchor {
            height: chain_epoch.visible_tip_height,
            hash: chain_epoch.visible_tip_hash,
        },
    )?;
    StreamCursorTokenV1::chain_event(
        chain_epoch.network,
        family,
        event_sequence,
        &locator,
        cursor_auth_key,
    )
    .map_err(|_| StoreError::InvalidChainEpochArtifacts {
        reason: "cursor authentication key could not initialize the MAC",
    })
}

/// Rebuilds an envelope's resume cursor so it carries the full back-spaced
/// locator rather than the tip-only locator the pure decoder reconstructs.
fn enrich_chain_event_cursor(
    inner: &impl RocksChainStoreRead,
    event_envelope: &mut ChainEventEnvelope,
    family: ChainEventStreamFamily,
    cursor_auth_key: [u8; 32],
) -> Result<(), StoreError> {
    event_envelope.cursor = mint_visible_chain_event_cursor(
        inner,
        event_envelope.chain_epoch,
        family,
        event_envelope.event_sequence,
        cursor_auth_key,
    )?;
    Ok(())
}

fn build_chain_event(
    committed: ChainEpochCommitted,
    previous_chain_epoch: Option<ChainEpoch>,
    reorg_window_change: &ReorgWindowChange,
) -> Result<ChainEvent, StoreError> {
    let event = match *reorg_window_change {
        ReorgWindowChange::Replace { from_height } => {
            let previous_chain_epoch =
                previous_chain_epoch.ok_or(StoreError::InvalidChainEpochArtifacts {
                    reason: "replacement requires an existing chain epoch",
                })?;
            let reverted = ChainRangeReverted::new(
                previous_chain_epoch,
                BlockHeightRange::inclusive(from_height, previous_chain_epoch.visible_tip_height),
            );

            ChainEvent::ChainReorged {
                reverted,
                committed,
            }
        }
        ReorgWindowChange::Unchanged
        | ReorgWindowChange::Extend { .. }
        | ReorgWindowChange::AdvanceSettledTipTo { .. } => ChainEvent::ChainCommitted { committed },
    };

    Ok(event)
}

/// Inputs for [`build_chain_event_envelope`].
struct ChainEventEnvelopeInputs<'change, R> {
    inner: &'change R,
    event_sequence: u64,
    committed: ChainEpochCommitted,
    previous_chain_epoch: Option<ChainEpoch>,
    reorg_window_change: &'change ReorgWindowChange,
    cursor_auth_key: [u8; 32],
}

fn build_chain_event_envelope<R: RocksChainStoreRead>(
    inputs: &ChainEventEnvelopeInputs<'_, R>,
) -> Result<ChainEventEnvelope, StoreError> {
    let &ChainEventEnvelopeInputs {
        inner,
        event_sequence,
        committed,
        previous_chain_epoch,
        reorg_window_change,
        cursor_auth_key,
    } = inputs;
    let event = build_chain_event(committed, previous_chain_epoch, reorg_window_change)?;
    let chain_epoch = committed.chain_epoch;
    // The just-committed visible-tip block is not yet readable through the index, so
    // the visible-tip entry is taken from the epoch directly; the back-spaced
    // ancestors resolve against already-committed blocks.
    let cursor = mint_visible_chain_event_cursor(
        inner,
        chain_epoch,
        ChainEventStreamFamily::Visible,
        event_sequence,
        cursor_auth_key,
    )?;

    Ok(ChainEventEnvelope::new(
        cursor,
        event_sequence,
        chain_epoch,
        chain_epoch.settled_tip_height,
        event,
    ))
}

fn chain_event_matches_family(
    event_envelope: &ChainEventEnvelope,
    family: ChainEventStreamFamily,
) -> bool {
    match family {
        ChainEventStreamFamily::Visible => true,
        ChainEventStreamFamily::Settled => {
            event_envelope.chain_epoch.visible_tip_height <= event_envelope.settled_tip_height
                && matches!(&event_envelope.event, ChainEvent::ChainCommitted { .. })
        }
    }
}

fn build_chain_epoch_puts(
    artifacts: ChainEpochArtifacts,
    validated_block_replay_order: ValidatedBlockReplayOrder,
    event_envelope: &ChainEventEnvelope,
) -> Result<Vec<StoragePut>, StoreError> {
    let ChainEpochArtifacts {
        chain_epoch,
        block_headers,
        block_replay_envelopes,
        block_blobs,
        compact_blocks,
        block_transaction_index,
        transaction_locations,
        transaction_facts,
        transaction_intrinsic_value_balances,
        transaction_blobs,
        tree_states,
        final_note_commitment_roots,
        block_value_pool_balances,
        subtree_roots,
        transparent_outputs_by_outpoint,
        transparent_spend_facts,
        reorg_window_change: _,
    } = artifacts;
    let transparent_spend_inputs = transparent_spend_inputs_by_block(&transaction_facts);

    let mut puts = Vec::new();
    puts.push(StoragePut {
        table: StorageTable::ChainEpoch,
        key: StoreKey::chain_epoch(chain_epoch.id),
        value: encode_chain_epoch(&chain_epoch),
    });
    push_block_header_artifact_puts(&mut puts, chain_epoch, block_headers)?;
    push_block_replay_envelopes_puts(
        &mut puts,
        chain_epoch,
        validated_block_replay_order,
        block_replay_envelopes,
    )?;
    push_block_blob_artifact_puts(&mut puts, chain_epoch, block_blobs)?;
    push_compact_block_artifact_puts(&mut puts, chain_epoch, compact_blocks)?;
    push_block_transaction_index_artifact_puts(&mut puts, chain_epoch, block_transaction_index)?;
    push_transaction_location_puts(&mut puts, chain_epoch, transaction_locations)?;
    push_transaction_facts_artifact_puts(&mut puts, chain_epoch, transaction_facts)?;
    push_transaction_intrinsic_value_balance_puts(
        &mut puts,
        chain_epoch,
        transaction_intrinsic_value_balances,
    )?;
    push_transaction_blob_artifact_puts(&mut puts, chain_epoch, transaction_blobs)?;
    push_tree_state_artifact_puts(&mut puts, chain_epoch, tree_states)?;
    push_final_note_commitment_roots_puts(&mut puts, chain_epoch, final_note_commitment_roots)?;
    push_block_value_pool_balance_puts(&mut puts, chain_epoch, block_value_pool_balances)?;
    push_subtree_root_artifact_puts(&mut puts, chain_epoch, subtree_roots)?;
    push_transparent_output_artifact_puts(&mut puts, chain_epoch, transparent_outputs_by_outpoint)?;
    push_transparent_spend_fact_puts(
        &mut puts,
        chain_epoch,
        transparent_spend_inputs,
        transparent_spend_facts,
    )?;
    push_commit_control_puts(&mut puts, chain_epoch, event_envelope);

    Ok(puts)
}

struct TransparentOutputProjectionRepairs {
    deletes: Vec<StorageDelete>,
}

#[derive(Clone, Copy)]
struct TransparentOutputProjectionRepairInputs<'input> {
    previous_chain_epoch: Option<ChainEpoch>,
    chain_epoch: ChainEpoch,
    reorg_window_change: &'input ReorgWindowChange,
    /// Outpoints re-created by this commit, with their new creation height.
    protected_outpoints: &'input HashMap<TransparentOutPoint, BlockHeight>,
}

/// Builds the `Replace` repair for the transparent-output projection and the
/// address-output projection it derives.
///
/// Both projections key creation facts, so both repair from the same set of
/// reverted creation outpoints. The address row carries the creation height
/// in its key: a protected outpoint re-created at a different height still
/// needs its old address row deleted, while the outpoint-keyed
/// `transparent_output` row is simply overwritten by this commit's put.
fn build_reorg_window_projection_repairs(
    inner: &impl RocksChainStoreRead,
    inputs: TransparentOutputProjectionRepairInputs<'_>,
) -> Result<TransparentOutputProjectionRepairs, StoreError> {
    let ReorgWindowChange::Replace { from_height } = inputs.reorg_window_change else {
        return Ok(TransparentOutputProjectionRepairs {
            deletes: Vec::new(),
        });
    };

    let mut deletes = Vec::new();
    let previous_chain_epoch =
        inputs
            .previous_chain_epoch
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "reorg replacement requires an existing previous chain epoch",
            })?;
    let reverted_outpoints =
        read_reverted_transparent_output_outpoints(inner, previous_chain_epoch, *from_height)?;
    let reverted_outputs = read_current_transparent_outputs_by_outpoints(
        inner,
        previous_chain_epoch,
        &reverted_outpoints,
    )?;
    for outpoint in reverted_outpoints {
        if let Some(reverted_output) = reverted_outputs.get(&outpoint)
            && inputs.protected_outpoints.get(&outpoint) != Some(&reverted_output.block_height)
        {
            deletes.push(StorageDelete {
                table: StorageTable::AddressOutputIndex,
                key: StoreKey::address_output_index(
                    inputs.chain_epoch.network,
                    reverted_output.address_script_hash,
                    reverted_output.block_height,
                    outpoint,
                ),
            });
        }
        if inputs.protected_outpoints.contains_key(&outpoint) {
            continue;
        }

        deletes.push(StorageDelete {
            table: StorageTable::TransparentOutput,
            key: StoreKey::transparent_output(inputs.chain_epoch.network, outpoint),
        });
    }

    Ok(TransparentOutputProjectionRepairs { deletes })
}

struct TransparentSpendFactProjectionRepairs {
    deletes: Vec<StorageDelete>,
}

#[derive(Clone, Copy)]
struct TransparentSpendFactProjectionRepairInputs<'input> {
    previous_chain_epoch: Option<ChainEpoch>,
    chain_epoch: ChainEpoch,
    reorg_window_change: &'input ReorgWindowChange,
    protected_outpoints: &'input HashSet<TransparentOutPoint>,
}

fn build_reorg_window_spend_fact_projection_repairs(
    inner: &impl RocksChainStoreRead,
    inputs: TransparentSpendFactProjectionRepairInputs<'_>,
) -> Result<TransparentSpendFactProjectionRepairs, StoreError> {
    let ReorgWindowChange::Replace { from_height } = inputs.reorg_window_change else {
        return Ok(TransparentSpendFactProjectionRepairs {
            deletes: Vec::new(),
        });
    };

    let mut deletes = Vec::new();
    let previous_chain_epoch =
        inputs
            .previous_chain_epoch
            .ok_or(StoreError::InvalidChainEpochArtifacts {
                reason: "reorg replacement requires an existing previous chain epoch",
            })?;
    let reverted_outpoints =
        read_reverted_transparent_spend_fact_outpoints(inner, previous_chain_epoch, *from_height)?;
    for outpoint in reverted_outpoints {
        if inputs.protected_outpoints.contains(&outpoint) {
            continue;
        }

        let current_key = StoreKey::transparent_spend_fact(inputs.chain_epoch.network, outpoint);
        deletes.push(StorageDelete {
            table: StorageTable::TransparentSpendFact,
            key: current_key,
        });
    }

    Ok(TransparentSpendFactProjectionRepairs { deletes })
}

struct TransparentRetentionSweep {
    puts: Vec<StoragePut>,
    deletes: Vec<StorageDelete>,
    swept_outpoints: u64,
    advance: Option<RetentionSweepAdvance>,
    /// Swept marker the scan started from (0 when absent). The locked writer
    /// re-reads the marker and discards the sweep when they differ.
    observed_swept_height: BlockHeight,
}

impl TransparentRetentionSweep {
    const fn empty() -> Self {
        Self {
            puts: Vec::new(),
            deletes: Vec::new(),
            swept_outpoints: 0,
            advance: None,
            observed_swept_height: BlockHeight::new(0),
        }
    }

    /// Whether the persisted swept marker still matches the value the scan
    /// started from, so the precomputed puts and deletes may be written.
    ///
    /// Always true for a sweep with nothing to write.
    fn swept_marker_unchanged(&self, inner: &impl RocksChainStoreRead) -> Result<bool, StoreError> {
        if self.puts.is_empty() && self.deletes.is_empty() {
            return Ok(true);
        }
        let swept_height =
            read_transparent_retention_swept_height(inner)?.unwrap_or(BlockHeight::new(0));
        Ok(swept_height == self.observed_swept_height)
    }

    fn outcome(&self) -> TransparentRetentionSweepOutcome {
        self.advance
            .as_ref()
            .map_or_else(TransparentRetentionSweepOutcome::default, |advance| {
                TransparentRetentionSweepOutcome::new(
                    advance
                        .swept_through
                        .value()
                        .saturating_sub(advance.swept_from.value())
                        .saturating_add(1),
                    advance.swept_outpoints,
                    advance.backlog_heights,
                )
            })
    }
}

struct RetentionSweepAdvance {
    swept_from: BlockHeight,
    swept_through: BlockHeight,
    sweep_ceiling: BlockHeight,
    swept_outpoints: u64,
    backlog_heights: u32,
}

/// Builds one transparent-projection retention maintenance pass.
///
/// A projection row may be physically deleted only when no commit the store
/// will ever accept can make it live again. `validate_reorg_window_change`
/// floors every `Replace` at `settled_tip + 1`, so a spend at or below
/// `settled_tip_height` is irreversible: the spent output's rows are deleted
/// from `address_output_index`, `transparent_output`, and
/// `transparent_spend_fact` in one maintenance batch.
///
/// The sweep covers heights from the persisted swept-through marker up to
/// `min(current settled tip, retention release height)`. The retention
/// release height is the durable-consumer floor: `zinder-ingest` publishes it
/// through [`PrimaryChainStore::set_transparent_retention_release_height`] as the
/// durable transparent-outpoint-spend projection advances, so a spend fact is
/// deleted only once that projection has recorded its spender identity. A
/// release height below the swept marker leaves the marker and the projections
/// untouched. A pass that deletes at least one fact also advances the
/// `transparent_retention_deleted_through_height` marker to the same ceiling, so
/// the startup guard can tell a real deletion apart from a checkpoint-bootstrap
/// swept marker that deleted nothing.
///
/// A single pass sweeps at most `max_heights_per_pass` heights and stops
/// after the first fully-swept height that reaches
/// `max_outpoints_per_pass`, whichever budget hits first. When the release
/// floor jumps far ahead of the swept marker (a store rebuilt with materialized views
/// paused, then un-paused at tip), the marker advances only to the last
/// fully-swept height and the remaining backlog drains across later passes;
/// the outpoint budget bounds the delete batch through transaction-dense
/// eras, and the height cap bounds the scan through sparse ones.
///
/// The scan runs outside the control lock so readiness and event-history
/// reads stay responsive through a bounded chunk. This is sound because
/// every row it reads is settled: the range ends at or below the current
/// settled tip, `validate_reorg_window_change` floors every `Replace` at
/// `settled_tip + 1`, and the swept marker is written only by this sweep inside
/// the serialized writer. The locked write still re-checks the marker via
/// [`TransparentRetentionSweep::swept_marker_unchanged`] and discards the sweep on
/// skew rather than writing markers derived from stale state.
fn build_transparent_retention_sweep(
    inner: &impl RocksChainStoreRead,
    chain_epoch: ChainEpoch,
    max_heights_per_pass: u32,
    max_outpoints_per_pass: u64,
) -> Result<TransparentRetentionSweep, StoreError> {
    let swept_through =
        read_transparent_retention_swept_height(inner)?.unwrap_or(BlockHeight::new(0));
    let retention_release_height =
        read_transparent_retention_release_height(inner)?.unwrap_or(BlockHeight::new(0));
    let sweep_ceiling = chain_epoch.settled_tip_height.min(retention_release_height);
    if sweep_ceiling <= swept_through {
        return Ok(TransparentRetentionSweep::empty());
    }

    let capped_ceiling = BlockHeight::new(
        swept_through
            .value()
            .saturating_add(max_heights_per_pass)
            .min(sweep_ceiling.value()),
    );
    let swept_from = BlockHeight::new(swept_through.value().saturating_add(1));
    let sweep_range = BlockHeightRange::inclusive(swept_from, capped_ceiling);
    let settled_sweep = collect_settled_spend_sweep_deletes(
        inner,
        chain_epoch,
        sweep_range,
        max_outpoints_per_pass,
    )?;
    let swept_outpoints = settled_sweep.swept_outpoints;

    let mut puts = vec![transparent_retention_swept_height_put(
        settled_sweep.swept_through,
    )];
    if swept_outpoints > 0 {
        puts.push(transparent_retention_deleted_through_height_put(
            settled_sweep.swept_through,
        ));
    }
    let backlog_heights = sweep_ceiling
        .value()
        .saturating_sub(settled_sweep.swept_through.value());
    Ok(TransparentRetentionSweep {
        puts,
        deletes: settled_sweep.deletes,
        swept_outpoints,
        advance: Some(RetentionSweepAdvance {
            swept_from,
            swept_through: settled_sweep.swept_through,
            sweep_ceiling,
            swept_outpoints,
            backlog_heights,
        }),
        observed_swept_height: swept_through,
    })
}

struct SettledSpendSweepDeletes {
    deletes: Vec<StorageDelete>,
    swept_outpoints: u64,
    /// Last height whose spends are fully covered by `deletes`.
    swept_through: BlockHeight,
}

/// Collects the deletes for spend facts and spent-output rows settled within
/// `sweep_range`, stopping after the first fully-swept height that reaches
/// `max_outpoints`.
///
/// A height is never split across passes: the budget is checked only after
/// every spend at a height is collected, so `swept_through` always names a
/// height whose deletes are complete and the marker may safely advance to it.
fn collect_settled_spend_sweep_deletes(
    inner: &impl RocksChainStoreRead,
    chain_epoch: ChainEpoch,
    sweep_range: BlockHeightRange,
    max_outpoints: u64,
) -> Result<SettledSpendSweepDeletes, StoreError> {
    let mut deletes = Vec::new();
    let mut swept_outpoints = 0_u64;
    let mut swept_through = sweep_range.start;
    for height in sweep_range {
        swept_through = height;
        let spends = read_current_transparent_spend_fact_block_facts(inner, chain_epoch, height)?;
        for spend in spends {
            if spend.block_height != height {
                continue;
            }
            let outpoint = spend.spent_outpoint;
            deletes.push(StorageDelete {
                table: StorageTable::AddressOutputIndex,
                key: StoreKey::address_output_index(
                    chain_epoch.network,
                    spend.spent_address_script_hash,
                    spend.spent_block_height,
                    outpoint,
                ),
            });
            deletes.push(StorageDelete {
                table: StorageTable::TransparentOutput,
                key: StoreKey::transparent_output(chain_epoch.network, outpoint),
            });
            deletes.push(StorageDelete {
                table: StorageTable::TransparentSpendFact,
                key: StoreKey::transparent_spend_fact(chain_epoch.network, outpoint),
            });
            swept_outpoints = swept_outpoints.saturating_add(1);
        }
        if swept_outpoints >= max_outpoints {
            break;
        }
    }
    Ok(SettledSpendSweepDeletes {
        deletes,
        swept_outpoints,
        swept_through,
    })
}

fn canonical_history_bounds_put(bounds: CanonicalHistoryBounds) -> StoragePut {
    StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::canonical_history_bounds(),
        value: encode_canonical_history_bounds(bounds),
    }
}

fn initial_canonical_history_bounds_put(
    current_chain_epoch: Option<ChainEpoch>,
    initial_bounds: Option<CanonicalHistoryBounds>,
) -> Result<Option<StoragePut>, StoreError> {
    if current_chain_epoch.is_none() {
        let bounds = initial_bounds.unwrap_or_else(CanonicalHistoryBounds::complete);
        return Ok(Some(canonical_history_bounds_put(bounds)));
    }
    if initial_bounds.is_some() {
        return Err(StoreError::InvalidChainEpochArtifacts {
            reason: "artifactless checkpoint requires an empty canonical store",
        });
    }
    Ok(None)
}

fn validate_initial_canonical_history_commit(
    artifacts: &ChainEpochArtifacts,
    current_chain_epoch: Option<ChainEpoch>,
    initial_bounds: Option<CanonicalHistoryBounds>,
) -> Result<(), StoreError> {
    if current_chain_epoch.is_some() || initial_bounds.is_some() {
        return Ok(());
    }
    let first_required_height = if artifacts.chain_epoch.visible_tip_height == BlockHeight::new(0) {
        BlockHeight::new(0)
    } else {
        BlockHeight::new(1)
    };
    if artifacts
        .block_headers
        .iter()
        .any(|header| header.height == first_required_height)
    {
        return Ok(());
    }
    Err(StoreError::InvalidChainEpochArtifacts {
        reason: "first complete-history commit must include the first canonical block header",
    })
}

fn read_persisted_canonical_history_bounds(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<CanonicalHistoryBounds>, StoreError> {
    let key = StoreKey::canonical_history_bounds();
    inner
        .get(StorageTable::StorageControl, &key)?
        .map(|bytes| decode_canonical_history_bounds(&bytes))
        .transpose()
}

pub(crate) fn read_canonical_history_bounds(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<CanonicalHistoryBounds>, StoreError> {
    if let Some(bounds) = read_persisted_canonical_history_bounds(inner)? {
        return Ok(Some(bounds));
    }
    let Some(current_epoch_id) = read_current_chain_epoch_id(inner)? else {
        return Ok(None);
    };
    let current_epoch = read_chain_epoch(inner, current_epoch_id)?;
    match read_block_header_artifact(inner, current_epoch, BlockHeight::new(1)) {
        Ok(Some(_)) => Ok(Some(CanonicalHistoryBounds::complete())),
        Ok(None) | Err(StoreError::ArtifactMissing { .. }) => {
            Err(StoreError::CanonicalHistoryBoundsMissing)
        }
        Err(error) => Err(error),
    }
}

fn require_canonical_history_bounds(
    inner: &impl RocksChainStoreRead,
) -> Result<CanonicalHistoryBounds, StoreError> {
    read_canonical_history_bounds(inner)?.ok_or(StoreError::CanonicalHistoryBoundsMissing)
}

fn encode_raw_blob_retention_signal(retention: RawBlobRetention) -> Vec<u8> {
    let discriminant = match retention {
        RawBlobRetention::None => 0,
        RawBlobRetention::Transactions => 1,
        RawBlobRetention::All => 2,
    };
    vec![discriminant]
}

fn decode_raw_blob_retention_signal(signal_bytes: &[u8]) -> Result<RawBlobRetention, StoreError> {
    let corrupt = |reason: &'static str| StoreError::ArtifactCorrupt {
        family: ArtifactFamily::ChainEpoch,
        key: StoreKey::raw_blob_retention().into(),
        reason,
    };
    let [discriminant] = signal_bytes else {
        return Err(corrupt("raw blob retention signal must be 1 byte"));
    };
    match discriminant {
        0 => Ok(RawBlobRetention::None),
        1 => Ok(RawBlobRetention::Transactions),
        2 => Ok(RawBlobRetention::All),
        _ => Err(corrupt(
            "raw blob retention signal has unknown discriminant",
        )),
    }
}

fn persist_raw_blob_retention(
    inner: &RocksChainStore,
    retention: RawBlobRetention,
) -> Result<(), StoreError> {
    inner.write(vec![StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::raw_blob_retention(),
        value: encode_raw_blob_retention_signal(retention),
    }])
}

fn admit_raw_blob_retention(
    inner: &RocksChainStore,
    configured_retention: RawBlobRetention,
) -> Result<(), StoreError> {
    let persisted_retention = read_raw_blob_retention_signal_if_present(inner)?;
    if read_current_chain_epoch_id(inner)?.is_none() {
        return persist_raw_blob_retention(inner, configured_retention);
    }

    let persisted_retention = persisted_retention.ok_or_else(missing_raw_blob_retention_error)?;
    if persisted_retention != configured_retention {
        return Err(StoreError::RawBlobRetentionMismatch {
            persisted: persisted_retention,
            configured: configured_retention,
        });
    }
    Ok(())
}

fn read_raw_blob_retention_signal(
    inner: &impl RocksChainStoreRead,
) -> Result<RawBlobRetention, StoreError> {
    let retention = read_raw_blob_retention_signal_if_present(inner)?;
    if let Some(retention) = retention {
        return Ok(retention);
    }
    if read_current_chain_epoch_id(inner)?.is_some() {
        return Err(missing_raw_blob_retention_error());
    }
    Ok(RawBlobRetention::None)
}

fn read_raw_blob_retention_signal_if_present(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<RawBlobRetention>, StoreError> {
    let key = StoreKey::raw_blob_retention();
    let Some(signal_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Ok(None);
    };
    decode_raw_blob_retention_signal(&signal_bytes).map(Some)
}

fn missing_raw_blob_retention_error() -> StoreError {
    StoreError::ArtifactCorrupt {
        family: ArtifactFamily::ChainEpoch,
        key: StoreKey::raw_blob_retention().into(),
        reason: "non-empty canonical store is missing its raw blob retention contract",
    }
}

fn transparent_retention_swept_height_put(height: BlockHeight) -> StoragePut {
    StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::transparent_retention_swept_height(),
        value: height.value().to_be_bytes().to_vec(),
    }
}

fn transparent_retention_release_height_put(height: BlockHeight) -> StoragePut {
    StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::transparent_retention_release_height(),
        value: height.value().to_be_bytes().to_vec(),
    }
}

pub(super) fn transparent_retention_deleted_through_height_put(height: BlockHeight) -> StoragePut {
    StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::transparent_retention_deleted_through_height(),
        value: height.value().to_be_bytes().to_vec(),
    }
}

pub(crate) fn read_transparent_retention_swept_height(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<BlockHeight>, StoreError> {
    read_storage_control_height(
        inner,
        StoreKey::transparent_retention_swept_height(),
        "transparent retention swept height must be 4 bytes",
    )
}

pub(crate) fn read_transparent_retention_release_height(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<BlockHeight>, StoreError> {
    read_storage_control_height(
        inner,
        StoreKey::transparent_retention_release_height(),
        "transparent retention release height must be 4 bytes",
    )
}

pub(crate) fn read_transparent_retention_deleted_through_height(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<BlockHeight>, StoreError> {
    read_storage_control_height(
        inner,
        StoreKey::transparent_retention_deleted_through_height(),
        "transparent retention deleted-through height must be 4 bytes",
    )
}

fn read_storage_control_height(
    inner: &impl RocksChainStoreRead,
    key: StoreKey,
    corrupt_reason: &'static str,
) -> Result<Option<BlockHeight>, StoreError> {
    let Some(height_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Ok(None);
    };

    let height_bytes =
        <[u8; 4]>::try_from(height_bytes.as_slice()).map_err(|_| StoreError::ArtifactCorrupt {
            family: ArtifactFamily::TransparentSpendFact,
            key: key.into(),
            reason: corrupt_reason,
        })?;
    Ok(Some(BlockHeight::new(u32::from_be_bytes(height_bytes))))
}

fn record_transparent_retention_sweep(swept_outpoints: u64) {
    if swept_outpoints == 0 {
        return;
    }
    metrics::counter!("zinder_store_retention_swept_outpoints_total").increment(swept_outpoints);
}

fn record_transparent_retention_sweep_outcome(
    started_at: Instant,
    outcome: &Result<TransparentRetentionSweepOutcome, StoreError>,
) {
    metrics::histogram!(
        "zinder_store_retention_sweep_duration_seconds",
        "status" => outcome_status(outcome)
    )
    .record(started_at.elapsed());
    if let Ok(outcome) = outcome {
        metrics::gauge!("zinder_store_retention_backlog_heights")
            .set(f64::from(outcome.backlog_heights()));
    }
}

fn log_transparent_retention_sweep(advance: Option<&RetentionSweepAdvance>) {
    let Some(advance) = advance else {
        return;
    };
    if advance.swept_outpoints == 0 && advance.backlog_heights == 0 {
        return;
    }
    tracing::info!(
        target: "zinder::store",
        event = "retention_sweep_advanced",
        swept_from = advance.swept_from.value(),
        swept_through = advance.swept_through.value(),
        sweep_ceiling = advance.sweep_ceiling.value(),
        swept_outpoints = advance.swept_outpoints,
        backlog_heights = advance.backlog_heights,
        "advanced transparent retention maintenance"
    );
}

fn address_output_row(artifact: &TransparentOutputArtifact) -> TransparentUnspentOutput {
    TransparentUnspentOutput::new(
        artifact.address_script_hash,
        artifact.script_pub_key.clone(),
        artifact.outpoint,
        artifact.value_zat,
        artifact.block_height,
        artifact.block_hash,
    )
}

fn read_reverted_transparent_spend_fact_outpoints(
    inner: &impl RocksChainStoreRead,
    previous_chain_epoch: ChainEpoch,
    from_height: BlockHeight,
) -> Result<Vec<TransparentOutPoint>, StoreError> {
    let mut outpoints = HashSet::new();
    for height in BlockHeightRange::inclusive(from_height, previous_chain_epoch.visible_tip_height)
    {
        outpoints.extend(read_visible_transparent_spend_fact_block_outpoints(
            inner,
            previous_chain_epoch,
            height,
        )?);
    }

    let mut outpoints = outpoints.into_iter().collect::<Vec<_>>();
    sort_transparent_outpoints(&mut outpoints);
    Ok(outpoints)
}

fn read_reverted_transparent_output_outpoints(
    inner: &impl RocksChainStoreRead,
    previous_chain_epoch: ChainEpoch,
    from_height: BlockHeight,
) -> Result<Vec<TransparentOutPoint>, StoreError> {
    let mut outpoints = HashSet::new();
    for height in BlockHeightRange::inclusive(from_height, previous_chain_epoch.visible_tip_height)
    {
        outpoints.extend(read_visible_transparent_output_block_outpoints(
            inner,
            previous_chain_epoch,
            height,
        )?);
    }

    let mut outpoints = outpoints.into_iter().collect::<Vec<_>>();
    sort_transparent_outpoints(&mut outpoints);
    Ok(outpoints)
}

pub(crate) fn read_visible_transparent_output_block_outpoints(
    inner: &impl RocksChainStoreRead,
    chain_epoch: ChainEpoch,
    height: BlockHeight,
) -> Result<Vec<TransparentOutPoint>, StoreError> {
    let Some(block) = read_block_header_artifact(inner, chain_epoch, height)? else {
        return Ok(Vec::new());
    };

    let prefix = StoreKey::transparent_output_block_index_prefix(chain_epoch.network, height);
    let mut outpoints = None;
    let mut scan_error = None;
    inner.scan_prefix_reverse(
        StorageTable::TransparentOutputBlockIndex,
        &prefix,
        &mut |key_bytes, envelope_bytes| {
            let key = StoreKey::from_raw_bytes(key_bytes);
            let Some(source_epoch) = StoreKey::transparent_artifact_chain_epoch_id(key_bytes)
            else {
                scan_error = Some(StoreError::ArtifactCorrupt {
                    family: ArtifactFamily::TransparentOutput,
                    key: key.into(),
                    reason: "transparent spend index key is malformed",
                });
                return Ok(PrefixScanControl::Stop);
            };
            if source_epoch > chain_epoch.id {
                return Ok(PrefixScanControl::Continue);
            }

            match decode_transparent_output_block_index(&key, envelope_bytes) {
                Ok((block_hash, block_outpoints)) if block_hash == block.block_hash => {
                    outpoints = Some(block_outpoints);
                    Ok(PrefixScanControl::Stop)
                }
                Ok(_) => Ok(PrefixScanControl::Continue),
                Err(error) => {
                    scan_error = Some(error);
                    Ok(PrefixScanControl::Stop)
                }
            }
        },
    )?;

    if let Some(error) = scan_error {
        return Err(error);
    }

    Ok(outpoints.unwrap_or_default())
}

fn sort_transparent_outpoints(outpoints: &mut [TransparentOutPoint]) {
    outpoints.sort_by(|left, right| {
        left.transaction_id
            .as_bytes()
            .cmp(&right.transaction_id.as_bytes())
            .then(left.output_index.cmp(&right.output_index))
    });
}

fn push_block_header_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    block_headers: Vec<BlockHeaderArtifact>,
) -> Result<(), StoreError> {
    for block in block_headers {
        let height = block.height;
        let block_hash = block.block_hash;
        let encoded_block = encode_block_header_artifact(&block)?;
        puts.push(StoragePut {
            table: StorageTable::BlockHeader,
            key: StoreKey::block_header(chain_epoch.network, chain_epoch.id, height),
            value: encoded_block,
        });
        puts.push(StoragePut {
            table: StorageTable::ReorgWindow,
            key: StoreKey::visible_block_epoch(chain_epoch.network, height, chain_epoch.id),
            value: visibility_value(chain_epoch),
        });
        puts.push(block_hash_index_put(
            chain_epoch.network,
            chain_epoch.id,
            height,
            block_hash,
        ));
    }

    Ok(())
}

fn push_block_replay_envelopes_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    validated_order: ValidatedBlockReplayOrder,
    replay_envelopes: Vec<zinder_core::CanonicalBlockReplayEnvelope>,
) -> Result<(), StoreError> {
    if validated_order.block_heights.len() != replay_envelopes.len() {
        return Err(StoreError::InvalidChainEpochArtifacts {
            reason: "validated block replay order does not match the supplied replay envelopes",
        });
    }

    for (block_height, replay_envelope) in validated_order
        .block_heights
        .into_iter()
        .zip(replay_envelopes)
    {
        puts.push(StoragePut {
            table: StorageTable::BlockReplay,
            key: StoreKey::block_replay(chain_epoch.network, chain_epoch.id, block_height),
            value: replay_envelope.into_bytes(),
        });
    }

    Ok(())
}

fn push_block_blob_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    block_blobs: Vec<BlockBlobArtifact>,
) -> Result<(), StoreError> {
    for block_blob in block_blobs {
        let height = block_blob.height;
        let encoded_blob = encode_block_blob_artifact(block_blob)?;
        puts.push(StoragePut {
            table: StorageTable::BlockBlob,
            key: StoreKey::block_blob(chain_epoch.network, chain_epoch.id, height),
            value: encoded_blob,
        });
    }

    Ok(())
}

fn push_compact_block_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    compact_blocks: Vec<CompactBlockArtifact>,
) -> Result<(), StoreError> {
    for compact_block in compact_blocks {
        let height = compact_block.height();
        let encoded_compact_block = encode_compact_block_artifact(compact_block)?;
        puts.push(StoragePut {
            table: StorageTable::CompactBlock,
            key: StoreKey::compact_block(chain_epoch.network, chain_epoch.id, height),
            value: encoded_compact_block,
        });
        puts.push(StoragePut {
            table: StorageTable::ReorgWindow,
            key: StoreKey::visible_compact_block_epoch(chain_epoch.network, height, chain_epoch.id),
            value: visibility_value(chain_epoch),
        });
    }

    Ok(())
}

fn push_block_transaction_index_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    artifacts: Vec<BlockTransactionIndexArtifact>,
) -> Result<(), StoreError> {
    for artifact in artifacts {
        let block_height = artifact.block_height;
        let tx_index_in_block = artifact.tx_index_in_block;
        let encoded_artifact = encode_block_transaction_index_artifact(artifact)?;
        puts.push(StoragePut {
            table: StorageTable::BlockTransactionIndex,
            key: StoreKey::block_transaction_index(
                chain_epoch.network,
                chain_epoch.id,
                block_height,
                tx_index_in_block,
            ),
            value: encoded_artifact,
        });
    }

    Ok(())
}

fn push_transaction_location_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    locations: Vec<TransactionLocation>,
) -> Result<(), StoreError> {
    for location in locations {
        let transaction_id = location.transaction_id;
        let encoded_location = encode_transaction_location_artifact(location)?;
        puts.push(StoragePut {
            table: StorageTable::TransactionLocation,
            key: StoreKey::transaction_location(
                chain_epoch.network,
                chain_epoch.id,
                transaction_id,
            ),
            value: encoded_location,
        });
    }

    Ok(())
}

fn push_transaction_facts_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    transaction_facts: Vec<TransactionFactsArtifact>,
) -> Result<(), StoreError> {
    for transaction in transaction_facts {
        let transaction_id = transaction.location.transaction_id;
        let encoded_transaction = encode_transaction_facts_artifact(transaction)?;
        puts.push(StoragePut {
            table: StorageTable::TransactionFacts,
            key: StoreKey::transaction_facts(chain_epoch.network, chain_epoch.id, transaction_id),
            value: encoded_transaction,
        });
        puts.push(StoragePut {
            table: StorageTable::ReorgWindow,
            key: StoreKey::visible_transaction_epoch(
                chain_epoch.network,
                transaction_id,
                chain_epoch.id,
            ),
            value: visibility_value(chain_epoch),
        });
    }

    Ok(())
}

fn push_transaction_intrinsic_value_balance_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    artifacts: Vec<TransactionIntrinsicValueBalancesArtifact>,
) -> Result<(), StoreError> {
    for artifact in artifacts {
        let transaction_id = artifact.location.transaction_id;
        puts.push(StoragePut {
            table: StorageTable::TransactionIntrinsicValueBalances,
            key: StoreKey::transaction_intrinsic_value_balances(
                chain_epoch.network,
                chain_epoch.id,
                transaction_id,
            ),
            value: encode_transaction_intrinsic_value_balances(artifact)?,
        });
    }

    Ok(())
}

fn push_transaction_blob_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    transaction_blobs: Vec<TransactionBlobArtifact>,
) -> Result<(), StoreError> {
    for transaction_blob in transaction_blobs {
        let transaction_id = transaction_blob.location.transaction_id;
        let encoded_blob = encode_transaction_blob_artifact(transaction_blob)?;
        puts.push(StoragePut {
            table: StorageTable::TransactionBlob,
            key: StoreKey::transaction_blob(chain_epoch.network, chain_epoch.id, transaction_id),
            value: encoded_blob,
        });
    }

    Ok(())
}

fn push_tree_state_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    tree_states: Vec<TreeStateArtifact>,
) -> Result<(), StoreError> {
    for tree_state in tree_states {
        let height = tree_state.height;
        let encoded_tree_state = encode_tree_state_artifact(tree_state)?;
        puts.push(StoragePut {
            table: StorageTable::TreeState,
            key: StoreKey::tree_state(chain_epoch.network, chain_epoch.id, height),
            value: encoded_tree_state,
        });
        puts.push(StoragePut {
            table: StorageTable::ReorgWindow,
            key: StoreKey::visible_tree_state_epoch(chain_epoch.network, height, chain_epoch.id),
            value: visibility_value(chain_epoch),
        });
    }

    Ok(())
}

fn push_final_note_commitment_roots_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    roots_by_block: Vec<BlockFinalNoteCommitmentRoots>,
) -> Result<(), StoreError> {
    for roots in roots_by_block {
        let height = roots.height;
        puts.push(StoragePut {
            table: StorageTable::FinalNoteCommitmentRoots,
            key: StoreKey::final_note_commitment_roots(chain_epoch.network, chain_epoch.id, height),
            value: encode_final_note_commitment_roots(roots)?,
        });
        puts.push(StoragePut {
            table: StorageTable::ReorgWindow,
            key: StoreKey::visible_final_note_commitment_roots_epoch(
                chain_epoch.network,
                height,
                chain_epoch.id,
            ),
            value: visibility_value(chain_epoch),
        });
    }

    Ok(())
}

fn push_block_value_pool_balance_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    balances_by_block: Vec<BlockValuePoolBalances>,
) -> Result<(), StoreError> {
    for balances in balances_by_block {
        let height = balances.block_id.height;
        puts.push(StoragePut {
            table: StorageTable::BlockValuePoolBalances,
            key: StoreKey::block_value_pool_balances(chain_epoch.network, chain_epoch.id, height),
            value: encode_block_value_pool_balances(&balances)?,
        });
        puts.push(StoragePut {
            table: StorageTable::ReorgWindow,
            key: StoreKey::visible_block_value_pool_balances_epoch(
                chain_epoch.network,
                height,
                chain_epoch.id,
            ),
            value: visibility_value(chain_epoch),
        });
    }

    Ok(())
}

fn push_subtree_root_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    subtree_roots: Vec<SubtreeRootArtifact>,
) -> Result<(), StoreError> {
    for subtree_root in subtree_roots {
        let protocol = subtree_root.protocol;
        let subtree_index = subtree_root.subtree_index;
        let encoded_subtree_root = encode_subtree_root_artifact(&subtree_root)?;
        puts.push(StoragePut {
            table: StorageTable::SubtreeRoot,
            key: StoreKey::subtree_root(
                chain_epoch.network,
                chain_epoch.id,
                protocol,
                subtree_index,
            ),
            value: encoded_subtree_root,
        });
        puts.push(StoragePut {
            table: StorageTable::ReorgWindow,
            key: StoreKey::visible_subtree_root_epoch(
                chain_epoch.network,
                protocol,
                subtree_index,
                chain_epoch.id,
            ),
            value: visibility_value(chain_epoch),
        });
    }

    Ok(())
}

fn push_transparent_output_artifact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    transparent_outputs_by_outpoint: Vec<TransparentOutputArtifact>,
) -> Result<(), StoreError> {
    let mut block_outpoints = HashMap::<(BlockHeight, BlockHash), Vec<TransparentOutPoint>>::new();
    for artifact in transparent_outputs_by_outpoint {
        let current_key = StoreKey::transparent_output(chain_epoch.network, artifact.outpoint);
        block_outpoints
            .entry((artifact.block_height, artifact.block_hash))
            .or_default()
            .push(artifact.outpoint);
        puts.push(StoragePut {
            table: StorageTable::AddressOutputIndex,
            key: StoreKey::address_output_index(
                chain_epoch.network,
                artifact.address_script_hash,
                artifact.block_height,
                artifact.outpoint,
            ),
            value: encode_address_output_index_artifact(address_output_row(&artifact))?,
        });
        let encoded = encode_transparent_output_artifact(artifact)?;
        puts.push(StoragePut {
            table: StorageTable::TransparentOutput,
            key: current_key,
            value: encoded,
        });
    }

    let mut block_outpoints = block_outpoints.into_iter().collect::<Vec<_>>();
    block_outpoints.sort_by(
        |((left_height, left_hash), _), ((right_height, right_hash), _)| {
            left_height
                .cmp(right_height)
                .then(left_hash.as_bytes().cmp(&right_hash.as_bytes()))
        },
    );
    for ((block_height, block_hash), mut outpoints) in block_outpoints {
        sort_transparent_outpoints(&mut outpoints);
        outpoints.dedup();
        puts.push(StoragePut {
            table: StorageTable::TransparentOutputBlockIndex,
            key: StoreKey::transparent_output_block_index(
                chain_epoch.network,
                block_height,
                chain_epoch.id,
            ),
            value: encode_transparent_output_block_index(block_hash, &outpoints)?,
        });
    }

    Ok(())
}

fn push_transparent_spend_fact_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    mut block_spend_inputs: HashMap<(BlockHeight, BlockHash), Vec<TransparentOutPoint>>,
    transparent_spend_facts: Vec<TransparentSpendFact>,
) -> Result<(), StoreError> {
    let mut block_spend_facts =
        HashMap::<(BlockHeight, BlockHash), Vec<TransparentSpendFact>>::new();
    for spend in transparent_spend_facts {
        let current_key =
            StoreKey::transparent_spend_fact(chain_epoch.network, spend.spent_outpoint);
        let encoded_spend = encode_transparent_spend_fact(&spend)?;
        puts.push(StoragePut {
            table: StorageTable::TransparentSpendFact,
            key: current_key,
            value: encoded_spend,
        });
        block_spend_facts
            .entry((spend.block_height, spend.block_hash))
            .or_default()
            .push(spend.clone());
        block_spend_inputs
            .entry((spend.block_height, spend.block_hash))
            .or_default()
            .push(spend.spent_outpoint);
    }

    let mut block_spend_inputs = block_spend_inputs.into_iter().collect::<Vec<_>>();
    block_spend_inputs.sort_by(
        |((left_height, left_hash), _), ((right_height, right_hash), _)| {
            left_height
                .cmp(right_height)
                .then(left_hash.as_bytes().cmp(&right_hash.as_bytes()))
        },
    );
    for ((block_height, block_hash), mut input_outpoints) in block_spend_inputs {
        sort_transparent_outpoints(&mut input_outpoints);
        input_outpoints.dedup();
        let mut spend_facts = block_spend_facts
            .remove(&(block_height, block_hash))
            .unwrap_or_default();
        spend_facts.sort_by(|left, right| {
            left.spent_outpoint
                .transaction_id
                .as_bytes()
                .cmp(&right.spent_outpoint.transaction_id.as_bytes())
                .then(
                    left.spent_outpoint
                        .output_index
                        .cmp(&right.spent_outpoint.output_index),
                )
        });
        spend_facts.dedup_by_key(|spend| spend.spent_outpoint);
        puts.push(StoragePut {
            table: StorageTable::TransparentSpendFactBlockIndex,
            key: StoreKey::transparent_spend_fact_block_index(
                chain_epoch.network,
                block_height,
                chain_epoch.id,
            ),
            value: encode_transparent_spend_fact_block_index(
                block_hash,
                &input_outpoints,
                &spend_facts,
            )?,
        });
    }

    Ok(())
}

fn transparent_spend_inputs_by_block(
    transaction_facts: &[TransactionFactsArtifact],
) -> HashMap<(BlockHeight, BlockHash), Vec<TransparentOutPoint>> {
    let mut inputs_by_block = HashMap::new();
    for transaction in transaction_facts {
        for input in &transaction.transparent_inputs {
            if input.spent_outpoint.is_coinbase_sentinel() {
                continue;
            }
            inputs_by_block
                .entry((
                    transaction.location.block_height,
                    transaction.location.block_hash,
                ))
                .or_insert_with(Vec::new)
                .push(input.spent_outpoint);
        }
    }
    inputs_by_block
}

fn visibility_value(chain_epoch: ChainEpoch) -> Vec<u8> {
    chain_epoch.id.value().to_be_bytes().to_vec()
}

fn push_commit_control_puts(
    puts: &mut Vec<StoragePut>,
    chain_epoch: ChainEpoch,
    event_envelope: &ChainEventEnvelope,
) {
    puts.push(StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::visible_chain_epoch_pointer(),
        value: visibility_value(chain_epoch),
    });
    puts.push(StoragePut {
        table: StorageTable::ChainEvent,
        key: StoreKey::chain_event(event_envelope.event_sequence),
        value: encode_chain_event_envelope(event_envelope),
    });
    puts.push(StoragePut {
        table: StorageTable::StorageControl,
        key: StoreKey::chain_event_sequence_pointer(),
        value: event_envelope.event_sequence.to_be_bytes().to_vec(),
    });
    if event_envelope.event_sequence == 1 {
        puts.push(StoragePut {
            table: StorageTable::StorageControl,
            key: StoreKey::oldest_retained_chain_event_sequence(),
            value: event_envelope.event_sequence.to_be_bytes().to_vec(),
        });
    }
}

fn require_current_chain_epoch(inner: &impl RocksChainStoreRead) -> Result<ChainEpoch, StoreError> {
    read_current_chain_epoch_id(inner)?
        .map(|chain_epoch_id| read_chain_epoch(inner, chain_epoch_id))
        .transpose()?
        .ok_or(StoreError::NoVisibleChainEpoch)
}

fn read_current_chain_epoch_id(
    inner: &impl RocksChainStoreRead,
) -> Result<Option<ChainEpochId>, StoreError> {
    let key = StoreKey::visible_chain_epoch_pointer();
    let Some(chain_epoch_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Ok(None);
    };

    if chain_epoch_bytes.len() != 8 {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::ChainEpoch,
            key: key.into(),
            reason: "visible chain epoch pointer must be 8 bytes",
        });
    }

    let chain_epoch_bytes = <[u8; 8]>::try_from(chain_epoch_bytes.as_slice()).map_err(|_| {
        StoreError::ArtifactCorrupt {
            family: ArtifactFamily::ChainEpoch,
            key: key.clone().into(),
            reason: "visible chain epoch pointer must be 8 bytes",
        }
    })?;
    Ok(Some(ChainEpochId::new(u64::from_be_bytes(
        chain_epoch_bytes,
    ))))
}

fn read_current_chain_event_sequence(inner: &impl RocksChainStoreRead) -> Result<u64, StoreError> {
    let key = StoreKey::chain_event_sequence_pointer();
    let Some(event_sequence_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Ok(0);
    };

    if event_sequence_bytes.len() != 8 {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::ChainEvent,
            key: key.into(),
            reason: "chain event sequence pointer must be 8 bytes",
        });
    }

    let event_sequence_bytes =
        <[u8; 8]>::try_from(event_sequence_bytes.as_slice()).map_err(|_| {
            StoreError::ArtifactCorrupt {
                family: ArtifactFamily::ChainEvent,
                key: key.clone().into(),
                reason: "chain event sequence pointer must be 8 bytes",
            }
        })?;
    Ok(u64::from_be_bytes(event_sequence_bytes))
}

fn read_oldest_retained_chain_event_sequence(
    inner: &impl RocksChainStoreRead,
    current_event_sequence: u64,
) -> Result<Option<u64>, StoreError> {
    if current_event_sequence == 0 {
        return Ok(None);
    }

    let key = StoreKey::oldest_retained_chain_event_sequence();
    let Some(event_sequence_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Ok(Some(1));
    };

    if event_sequence_bytes.len() != 8 {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::ChainEvent,
            key: key.into(),
            reason: "oldest retained chain event sequence must be 8 bytes",
        });
    }

    let event_sequence_bytes =
        <[u8; 8]>::try_from(event_sequence_bytes.as_slice()).map_err(|_| {
            StoreError::ArtifactCorrupt {
                family: ArtifactFamily::ChainEvent,
                key: key.clone().into(),
                reason: "oldest retained chain event sequence must be 8 bytes",
            }
        })?;
    let oldest_retained_sequence = u64::from_be_bytes(event_sequence_bytes);
    if oldest_retained_sequence == 0 || oldest_retained_sequence > current_event_sequence {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::ChainEvent,
            key: key.into(),
            reason: "oldest retained chain event sequence is outside retained history",
        });
    }

    Ok(Some(oldest_retained_sequence))
}

impl ChainEventRetentionReport {
    const fn empty() -> Self {
        Self {
            current_event_sequence: 0,
            oldest_retained_sequence: None,
            oldest_retained_created_at: None,
            retained_event_count: 0,
            pruned_event_count: 0,
        }
    }
}

fn build_chain_event_retention_report(
    inner: &impl RocksChainStoreRead,
    oldest_retained_sequence: u64,
    current_event_sequence: u64,
    cursor_auth_key: [u8; 32],
    pruned_event_count: u64,
) -> Result<ChainEventRetentionReport, StoreError> {
    let oldest_retained_created_at =
        read_chain_event_created_at(inner, oldest_retained_sequence, cursor_auth_key)?;
    let retained_event_count = current_event_sequence
        .saturating_sub(oldest_retained_sequence)
        .saturating_add(1);

    Ok(ChainEventRetentionReport {
        current_event_sequence,
        oldest_retained_sequence: Some(oldest_retained_sequence),
        oldest_retained_created_at: Some(oldest_retained_created_at),
        retained_event_count,
        pruned_event_count,
    })
}

fn read_chain_event_created_at(
    inner: &impl RocksChainStoreRead,
    event_sequence: u64,
    cursor_auth_key: [u8; 32],
) -> Result<UnixTimestampMillis, StoreError> {
    let key = StoreKey::chain_event(event_sequence);
    let Some(record_bytes) = inner.get(StorageTable::ChainEvent, &key)? else {
        return Err(StoreError::ChainEventCursorExpired {
            event_sequence,
            oldest_retained_sequence: event_sequence.saturating_add(1),
        });
    };
    let event_envelope = decode_chain_event_envelope(
        &key,
        &record_bytes,
        ChainEventStreamFamily::Visible,
        cursor_auth_key,
    )?;

    Ok(event_envelope.chain_epoch.created_at)
}

fn record_chain_event_prune_outcome(
    started_at: Instant,
    prune_outcome: &Result<ChainEventRetentionReport, StoreError>,
) {
    metrics::histogram!(
        "zinder_chain_event_pruning_duration_seconds",
        "status" => outcome_status(prune_outcome)
    )
    .record(started_at.elapsed());
    if let Ok(report) = prune_outcome {
        metrics::counter!("zinder_chain_event_pruned_total").increment(report.pruned_event_count);
    }
}

fn record_chain_event_retention_report(report: ChainEventRetentionReport) {
    metrics::gauge!("zinder_chain_event_retained").set(u64_to_f64(report.retained_event_count));
    metrics::gauge!("zinder_chain_event_retention_oldest_sequence")
        .set(u64_to_f64(report.oldest_retained_sequence.unwrap_or(0)));
}

const fn outcome_status<T, E>(outcome: &Result<T, E>) -> &'static str {
    if outcome.is_ok() { "ok" } else { "error" }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus gauges use f64 samples; chain-event retention values are diagnostic"
)]
fn u64_to_f64(sample: u64) -> f64 {
    sample as f64
}

fn read_cursor_auth_key(inner: &impl RocksChainStoreRead) -> Result<[u8; 32], StoreError> {
    let key = StoreKey::cursor_auth_key();
    if let Some(cursor_auth_key_bytes) = inner.get(StorageTable::StorageControl, &key)? {
        let cursor_auth_key =
            <[u8; 32]>::try_from(cursor_auth_key_bytes.as_slice()).map_err(|_| {
                StoreError::ArtifactCorrupt {
                    family: ArtifactFamily::ChainEvent,
                    key: key.clone().into(),
                    reason: "cursor authentication key must be 32 bytes",
                }
            })?;

        return Ok(cursor_auth_key);
    }

    Err(StoreError::ArtifactMissing {
        family: ArtifactFamily::ChainEvent,
        key: key.into(),
    })
}

fn ensure_cursor_auth_key(inner: &RocksChainStore) -> Result<[u8; 32], StoreError> {
    let key = StoreKey::cursor_auth_key();
    match read_cursor_auth_key(inner) {
        Ok(cursor_auth_key) => return Ok(cursor_auth_key),
        Err(StoreError::ArtifactMissing { .. }) => {}
        Err(error) => return Err(error),
    }

    let mut cursor_auth_key = [0; 32];
    getrandom::fill(&mut cursor_auth_key)
        .map_err(|source| StoreError::EntropyUnavailable { source })?;
    inner.write(vec![StoragePut {
        table: StorageTable::StorageControl,
        key,
        value: cursor_auth_key.to_vec(),
    }])?;

    Ok(cursor_auth_key)
}

fn read_chain_epoch(
    inner: &impl RocksChainStoreRead,
    chain_epoch: ChainEpochId,
) -> Result<ChainEpoch, StoreError> {
    let key = StoreKey::chain_epoch(chain_epoch);
    let Some(record_bytes) = inner.get(StorageTable::ChainEpoch, &key)? else {
        return Err(StoreError::ChainEpochMissing { chain_epoch });
    };

    decode_chain_epoch(&key, &record_bytes)
}

fn read_current_mempool_event_sequence(
    inner: &impl RocksChainStoreRead,
) -> Result<u64, StoreError> {
    let key = StoreKey::mempool_event_sequence_pointer();
    let Some(event_sequence_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Ok(0);
    };
    if event_sequence_bytes.len() != 8 {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::MempoolEvent,
            key: key.into(),
            reason: "mempool event sequence pointer must be 8 bytes",
        });
    }
    let event_sequence_bytes =
        <[u8; 8]>::try_from(event_sequence_bytes.as_slice()).map_err(|_| {
            StoreError::ArtifactCorrupt {
                family: ArtifactFamily::MempoolEvent,
                key: key.clone().into(),
                reason: "mempool event sequence pointer must be 8 bytes",
            }
        })?;
    Ok(u64::from_be_bytes(event_sequence_bytes))
}

fn read_oldest_retained_mempool_event_sequence(
    inner: &impl RocksChainStoreRead,
    current_event_sequence: u64,
) -> Result<Option<u64>, StoreError> {
    if current_event_sequence == 0 {
        return Ok(None);
    }
    let key = StoreKey::oldest_retained_mempool_event_sequence();
    let Some(event_sequence_bytes) = inner.get(StorageTable::StorageControl, &key)? else {
        return Ok(Some(1));
    };
    if event_sequence_bytes.len() != 8 {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::MempoolEvent,
            key: key.into(),
            reason: "oldest retained mempool event sequence must be 8 bytes",
        });
    }
    let event_sequence_bytes =
        <[u8; 8]>::try_from(event_sequence_bytes.as_slice()).map_err(|_| {
            StoreError::ArtifactCorrupt {
                family: ArtifactFamily::MempoolEvent,
                key: key.clone().into(),
                reason: "oldest retained mempool event sequence must be 8 bytes",
            }
        })?;
    let oldest_retained_sequence = u64::from_be_bytes(event_sequence_bytes);
    if oldest_retained_sequence == 0 || oldest_retained_sequence > current_event_sequence {
        return Err(StoreError::ArtifactCorrupt {
            family: ArtifactFamily::MempoolEvent,
            key: key.into(),
            reason: "oldest retained mempool event sequence is outside retained history",
        });
    }
    Ok(Some(oldest_retained_sequence))
}

fn read_mempool_event_observed_at(
    inner: &impl RocksChainStoreRead,
    event_sequence: u64,
) -> Result<Option<UnixTimestampMillis>, StoreError> {
    let key = StoreKey::mempool_event(event_sequence);
    let Some(record_bytes) = inner.get(StorageTable::MempoolEvent, &key)? else {
        return Ok(None);
    };
    Ok(Some(decode_mempool_event_observed_at(&key, &record_bytes)?))
}

struct MempoolPruneScan {
    deletes: Vec<StorageDelete>,
    pruned_added: u64,
    pruned_mined: u64,
    pruned_invalidated: u64,
    new_oldest_retained: Option<u64>,
}

fn scan_mempool_events_for_pruning(
    inner: &impl RocksChainStoreRead,
    now: UnixTimestampMillis,
    retention: MempoolEventRetentionConfig,
    oldest_retained_sequence: u64,
    current_event_sequence: u64,
) -> Result<MempoolPruneScan, StoreError> {
    let mut deletes: Vec<StorageDelete> = Vec::new();
    let mut pruned_added = 0_u64;
    let mut pruned_mined = 0_u64;
    let mut pruned_invalidated = 0_u64;
    let mut new_oldest_retained: Option<u64> = None;
    let mut iter_error: Option<StoreError> = None;

    inner.scan_forward(
        StorageTable::MempoolEvent,
        &StoreKey::mempool_event(oldest_retained_sequence),
        &mut |key_bytes, record_bytes| {
            let key = StoreKey::from_raw_bytes(key_bytes);
            let Some(event_sequence) = StoreKey::mempool_event_sequence_from_key(key_bytes) else {
                iter_error = Some(StoreError::ArtifactCorrupt {
                    family: ArtifactFamily::MempoolEvent,
                    key: key.into(),
                    reason: "mempool event key has malformed length",
                });
                return Ok(PrefixScanControl::Stop);
            };
            if event_sequence >= current_event_sequence {
                return Ok(PrefixScanControl::Stop);
            }

            let observed_at = match decode_mempool_event_observed_at(&key, record_bytes) {
                Ok(observed_at) => observed_at,
                Err(error) => {
                    iter_error = Some(error);
                    return Ok(PrefixScanControl::Stop);
                }
            };
            let kind = match decode_mempool_event_kind(&key, record_bytes) {
                Ok(kind) => kind,
                Err(error) => {
                    iter_error = Some(error);
                    return Ok(PrefixScanControl::Stop);
                }
            };
            let retention_window = match kind {
                MempoolEventKind::Added => retention.added_retention,
                MempoolEventKind::Mined => retention.mined_retention,
                MempoolEventKind::Invalidated => retention.invalidated_retention,
            };
            let should_prune =
                retention_window.is_some_and(|window| age_exceeds_window(now, observed_at, window));
            if should_prune {
                deletes.push(StorageDelete {
                    table: StorageTable::MempoolEvent,
                    key: StoreKey::mempool_event(event_sequence),
                });
                match kind {
                    MempoolEventKind::Added => pruned_added = pruned_added.saturating_add(1),
                    MempoolEventKind::Mined => pruned_mined = pruned_mined.saturating_add(1),
                    MempoolEventKind::Invalidated => {
                        pruned_invalidated = pruned_invalidated.saturating_add(1);
                    }
                }
            } else if new_oldest_retained.is_none() {
                new_oldest_retained = Some(event_sequence);
            }
            Ok(PrefixScanControl::Continue)
        },
    )?;
    if let Some(error) = iter_error {
        return Err(error);
    }

    Ok(MempoolPruneScan {
        deletes,
        pruned_added,
        pruned_mined,
        pruned_invalidated,
        new_oldest_retained,
    })
}

fn age_exceeds_window(
    now: UnixTimestampMillis,
    observed_at: UnixTimestampMillis,
    window: std::time::Duration,
) -> bool {
    let age_millis = now.value().saturating_sub(observed_at.value());
    let window_millis = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
    age_millis > window_millis
}

fn record_mempool_event_prune_outcome(
    started_at: Instant,
    prune_outcome: &Result<MempoolEventRetentionReport, StoreError>,
) {
    metrics::histogram!(
        "zinder_mempool_event_pruning_duration_seconds",
        "status" => outcome_status(prune_outcome)
    )
    .record(started_at.elapsed());
    if let Ok(report) = prune_outcome {
        metrics::counter!(
            "zinder_mempool_events_pruned_total",
            "kind" => "added"
        )
        .increment(report.pruned_added_count);
        metrics::counter!(
            "zinder_mempool_events_pruned_total",
            "kind" => "mined"
        )
        .increment(report.pruned_mined_count);
        metrics::counter!(
            "zinder_mempool_events_pruned_total",
            "kind" => "invalidated"
        )
        .increment(report.pruned_invalidated_count);
    }
}

fn record_mempool_event_retention_report(report: MempoolEventRetentionReport) {
    metrics::gauge!("zinder_mempool_events_retained").set(u64_to_f64(report.retained_event_count));
    metrics::gauge!("zinder_mempool_event_retention_oldest_sequence")
        .set(u64_to_f64(report.oldest_retained_sequence.unwrap_or(0)));
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rust_rocksdb::{DB, Options};
    use tempfile::tempdir;
    use zinder_core::{
        BlockHash, BlockHeaderArtifact, BlockHeight, CanonicalBlockFacts,
        CanonicalBlockFactsDigestVersion, CanonicalBlockReplayFormatVersion, ChainTipMetadata,
        CompactBlockArtifact, SerializedBytesDigest, TreeStateArtifact, UnixTimestampMillis,
        encode_canonical_block_replay,
    };

    use super::*;

    fn synthetic_chain_epoch_artifacts(
        chain_epoch: ChainEpoch,
        block_headers: Vec<BlockHeaderArtifact>,
        compact_blocks: Vec<CompactBlockArtifact>,
    ) -> ChainEpochArtifacts {
        let block_replay_envelopes = block_headers
            .iter()
            .map(|block_header| {
                encode_canonical_block_replay(
                    &CanonicalBlockFacts {
                        block_header: block_header.clone(),
                        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[]),
                        transactions: Vec::new(),
                    },
                    CanonicalBlockReplayFormatVersion::CURRENT,
                    CanonicalBlockFactsDigestVersion::CURRENT,
                )
            })
            .collect();
        ChainEpochArtifacts::new(
            chain_epoch,
            block_headers,
            block_replay_envelopes,
            compact_blocks,
        )
    }

    #[test]
    fn current_artifact_schema_version_matches_supported_guard() {
        assert_eq!(CURRENT_ARTIFACT_SCHEMA_VERSION.value(), 20);
        assert_eq!(MIN_SUPPORTED_ARTIFACT_SCHEMA_VERSION, 20);
        assert_eq!(
            MAX_SUPPORTED_ARTIFACT_SCHEMA_VERSION,
            CURRENT_ARTIFACT_SCHEMA_VERSION.value()
        );
    }

    #[test]
    fn canonical_secondary_retries_only_missing_sst_file_races() {
        assert!(is_missing_sst_error(
            &rust_rocksdb::ErrorKind::IOError,
            "IO error: No such file or directory: /canonical/123456.sst"
        ));
        assert!(!is_missing_sst_error(
            &rust_rocksdb::ErrorKind::IOError,
            "IO error: No such file or directory: /canonical/CURRENT"
        ));
        assert!(!is_missing_sst_error(
            &rust_rocksdb::ErrorKind::Corruption,
            "Corruption: No such file or directory: /canonical/123456.sst"
        ));
    }

    #[test]
    fn raw_blob_retention_signal_round_trips_every_retention() -> Result<(), Box<dyn Error>> {
        for retention in [
            RawBlobRetention::None,
            RawBlobRetention::Transactions,
            RawBlobRetention::All,
        ] {
            let encoded = encode_raw_blob_retention_signal(retention);
            assert_eq!(encoded.len(), 1);
            assert_eq!(decode_raw_blob_retention_signal(&encoded)?, retention);
        }
        Ok(())
    }

    #[test]
    fn raw_blob_retention_signal_rejects_malformed_inputs() {
        for malformed in [vec![], vec![0, 0], vec![3], vec![255]] {
            assert!(matches!(
                decode_raw_blob_retention_signal(&malformed),
                Err(StoreError::ArtifactCorrupt { .. })
            ));
        }
    }

    #[test]
    fn missing_retained_full_history_row_is_not_treated_as_intentional_truncation()
    -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
        let (chain_epoch, block, compact_block) = synthetic_epoch(1, 1);
        store.commit_chain_epoch(synthetic_chain_epoch_artifacts(
            chain_epoch,
            vec![block],
            vec![compact_block],
        ))?;
        store.store.inner.delete(
            StorageTable::BlockHeader,
            &StoreKey::block_header(
                Network::ZcashRegtest,
                ChainEpochId::new(1),
                BlockHeight::new(1),
            ),
        )?;

        let reader = store.current_chain_epoch_reader()?;
        assert!(matches!(
            reader.block_header_at(BlockHeight::new(1)),
            Err(StoreError::ArtifactMissing {
                family: ArtifactFamily::BlockHeader,
                ..
            })
        ));

        Ok(())
    }

    #[test]
    fn chain_event_history_reports_expired_cursor_when_retained_row_is_missing()
    -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
        let (first_epoch, first_block, first_compact_block) = synthetic_epoch(1, 1);
        let (second_epoch, second_block, second_compact_block) = synthetic_epoch(2, 2);

        let first_commit = store.commit_chain_epoch(synthetic_chain_epoch_artifacts(
            first_epoch,
            vec![first_block],
            vec![first_compact_block],
        ))?;
        store.commit_chain_epoch(synthetic_chain_epoch_artifacts(
            second_epoch,
            vec![second_block],
            vec![second_compact_block],
        ))?;

        store
            .store
            .inner
            .delete(StorageTable::ChainEvent, &StoreKey::chain_event(2))?;

        let error = match store.chain_event_history(ChainEventHistoryRequest::with_default_limit(
            Some(&first_commit.event_envelope.cursor),
        )) {
            Ok(event_history) => {
                return Err(format!("expected expired cursor, got {event_history:?}").into());
            }
            Err(error) => error,
        };

        assert!(matches!(
            error,
            StoreError::ChainEventCursorExpired {
                event_sequence: 1,
                oldest_retained_sequence: 2,
            }
        ));

        Ok(())
    }

    #[test]
    fn unresolvable_locator_fork_point_expires_the_cursor() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
        let (chain_epoch, block, compact_block) = synthetic_epoch(1, 1);
        store.commit_chain_epoch(synthetic_chain_epoch_artifacts(
            chain_epoch,
            vec![block],
            vec![compact_block],
        ))?;

        // A well-authenticated cursor whose locator entries name a branch with
        // no canonical block at any of its heights (a divergence past the
        // recoverable cap). The fork point is unresolvable, so the cursor
        // expires with re-derive guidance rather than synthesizing a reorg.
        let locator = ChainEventLocator::new(vec![ChainEventCursorAnchor {
            height: BlockHeight::new(1),
            hash: block_hash(900),
        }])?;
        let cursor = StreamCursorTokenV1::chain_event(
            Network::ZcashRegtest,
            ChainEventStreamFamily::Visible,
            1,
            &locator,
            store.store.cursor_auth_key,
        )?;

        let error = match store
            .chain_event_history(ChainEventHistoryRequest::with_default_limit(Some(&cursor)))
        {
            Ok(event_history) => {
                return Err(format!("expected expired cursor, got {event_history:?}").into());
            }
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::ChainEventCursorExpired { .. }));

        Ok(())
    }

    #[test]
    fn replacement_commit_retains_superseded_height_visibility_rows() -> Result<(), Box<dyn Error>>
    {
        let tempdir = tempdir()?;
        let store = PrimaryChainStore::open(tempdir.path(), ChainStoreOptions::for_local_tests())?;
        let (first_epoch, first_block, first_compact_block) = synthetic_epoch(1, 1);
        let (mut second_epoch, second_block, second_compact_block) =
            synthetic_epoch_with_hash_seed(2, 2, 2, 1);
        second_epoch.settled_tip_height = first_epoch.visible_tip_height;
        second_epoch.settled_tip_hash = first_epoch.visible_tip_hash;
        let second_tree_state = TreeStateArtifact::new(
            second_block.height,
            second_block.block_hash,
            u32::try_from(second_block.block_time)?,
            b"tree-state-2".to_vec(),
        );
        let (mut replacement_epoch, replacement_block, replacement_compact_block) =
            synthetic_epoch_with_hash_seed(3, 2, 200, 1);
        replacement_epoch.settled_tip_height = first_epoch.visible_tip_height;
        replacement_epoch.settled_tip_hash = first_epoch.visible_tip_hash;
        let replacement_tree_state = TreeStateArtifact::new(
            replacement_block.height,
            replacement_block.block_hash,
            u32::try_from(replacement_block.block_time)?,
            b"replacement-tree-state-2".to_vec(),
        );

        store.commit_chain_epoch(synthetic_chain_epoch_artifacts(
            first_epoch,
            vec![first_block],
            vec![first_compact_block],
        ))?;
        store.commit_chain_epoch(
            synthetic_chain_epoch_artifacts(
                second_epoch,
                vec![second_block],
                vec![second_compact_block],
            )
            .with_tree_states(vec![second_tree_state]),
        )?;

        let stale_visibility_keys = height_visibility_keys(second_epoch, BlockHeight::new(2));
        assert_reorg_window_visibility(&store, &stale_visibility_keys, true)?;

        store.commit_chain_epoch(
            synthetic_chain_epoch_artifacts(
                replacement_epoch,
                vec![replacement_block],
                vec![replacement_compact_block],
            )
            .with_tree_states(vec![replacement_tree_state])
            .with_reorg_window_change(ReorgWindowChange::Replace {
                from_height: BlockHeight::new(2),
            }),
        )?;

        assert_reorg_window_visibility(&store, &stale_visibility_keys, true)?;

        Ok(())
    }

    fn height_visibility_keys(chain_epoch: ChainEpoch, height: BlockHeight) -> [StoreKey; 3] {
        [
            StoreKey::visible_block_epoch(chain_epoch.network, height, chain_epoch.id),
            StoreKey::visible_compact_block_epoch(chain_epoch.network, height, chain_epoch.id),
            StoreKey::visible_tree_state_epoch(chain_epoch.network, height, chain_epoch.id),
        ]
    }

    fn assert_reorg_window_visibility(
        store: &PrimaryChainStore,
        keys: &[StoreKey],
        expected_present: bool,
    ) -> Result<(), StoreError> {
        for key in keys {
            let present = store
                .store
                .inner
                .get(StoreReadCaller::Query, StorageTable::ReorgWindow, key)?
                .is_some();
            assert_eq!(present, expected_present);
        }

        Ok(())
    }

    #[test]
    fn opening_noncurrent_schema_refuses_before_decoding_unsupported_event_rows()
    -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let storage_path = tempdir.path().join("unsupported-event-row-store");
        let unsupported_event_key = StoreKey::mempool_event(1);
        // Schema 14 encoded event kind 4 with a transaction ID in nested field
        // 5. The current schema intentionally has no decoder for that event
        // kind.
        let mut unsupported_event_record = vec![
            0x08, 0x01, // event_sequence = 1
            0x10, 0x01, // source_observed_unix_millis = 1
            0x1a, 0x26, // event record, 38 bytes
            0x08, 0x04, // unsupported event kind
            0x2a, 0x22, // unsupported event payload, 34 bytes
            0x0a, 0x20, // transaction_id, 32 bytes
        ];
        unsupported_event_record.extend_from_slice(&[7; 32]);

        {
            let store =
                PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())?;
            let mut metadata_bytes = Vec::with_capacity(6);
            metadata_bytes.extend_from_slice(&14_u16.to_be_bytes());
            metadata_bytes.extend_from_slice(&Network::ZcashRegtest.id().to_be_bytes());
            store.store.inner.write(vec![
                StoragePut {
                    table: StorageTable::MempoolEvent,
                    key: unsupported_event_key.clone(),
                    value: unsupported_event_record.clone(),
                },
                StoragePut {
                    table: StorageTable::StorageControl,
                    key: StoreKey::store_metadata(),
                    value: metadata_bytes,
                },
            ])?;
        }

        let Err(error) =
            PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())
        else {
            return Err("expected schema-14 rejection on reopen".into());
        };
        assert!(
            matches!(
                error,
                StoreError::SchemaMismatch {
                    persisted_version: 14,
                    expected_version: CURRENT_STORE_SCHEMA_VERSION,
                }
            ),
            "unexpected error: {error:?}"
        );

        let column_families = DB::list_cf(&Options::default(), &storage_path)?;
        let database = DB::open_cf(&Options::default(), &storage_path, column_families)?;
        let mempool_events = database
            .cf_handle(StorageTable::MempoolEvent.column_family_name())
            .ok_or("mempool event column family must remain present")?;
        assert_eq!(
            database.get_cf(&mempool_events, unsupported_event_key.as_bytes())?,
            Some(unsupported_event_record),
            "failed admission must not mutate unsupported event rows"
        );

        Ok(())
    }

    #[test]
    fn opening_populated_store_without_metadata_refuses_without_mutation()
    -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let storage_path = tempdir.path().join("missing-metadata-store");
        let mempool_event_key = StoreKey::mempool_event(1);
        let mempool_event_value = vec![7, 11, 19];
        {
            let store =
                PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())?;
            store.store.inner.write(vec![StoragePut {
                table: StorageTable::MempoolEvent,
                key: mempool_event_key.clone(),
                value: mempool_event_value.clone(),
            }])?;
        }

        let column_families = DB::list_cf(&Options::default(), &storage_path)?;
        let database = DB::open_cf(&Options::default(), &storage_path, &column_families)?;
        let storage_control = database
            .cf_handle(StorageTable::StorageControl.column_family_name())
            .ok_or("storage-control column family must exist")?;
        database.delete_cf(&storage_control, StoreKey::store_metadata().as_bytes())?;
        drop(storage_control);
        drop(database);

        let Err(error) =
            PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())
        else {
            return Err("expected missing-metadata rejection on reopen".into());
        };
        assert!(
            matches!(error, StoreError::StoreMetadataMissing),
            "unexpected error: {error:?}"
        );

        let database = DB::open_cf(&Options::default(), &storage_path, &column_families)?;
        let mempool_events = database
            .cf_handle(StorageTable::MempoolEvent.column_family_name())
            .ok_or("mempool-event column family must remain present")?;
        let storage_control = database
            .cf_handle(StorageTable::StorageControl.column_family_name())
            .ok_or("storage-control column family must remain present")?;
        assert_eq!(
            database.get_cf(&mempool_events, mempool_event_key.as_bytes())?,
            Some(mempool_event_value),
            "failed admission must not mutate persisted rows"
        );
        assert_eq!(
            database.get_cf(&storage_control, StoreKey::store_metadata().as_bytes())?,
            None,
            "failed admission must not recreate metadata"
        );

        Ok(())
    }

    #[test]
    fn opening_fresh_store_initializes_current_metadata() -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let storage_path = tempdir.path().join("fresh-store");
        assert!(!storage_path.exists());

        let store = PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())?;
        let metadata_key = StoreKey::store_metadata();
        let metadata_bytes = store
            .store
            .inner
            .get(
                StoreReadCaller::Query,
                StorageTable::StorageControl,
                &metadata_key,
            )?
            .ok_or("fresh store metadata must be initialized")?;
        let metadata = decode_store_metadata(&metadata_key, &metadata_bytes)?;
        assert_eq!(metadata.schema_version, CURRENT_STORE_SCHEMA_VERSION);
        assert_eq!(metadata.network, Network::ZcashRegtest);

        Ok(())
    }

    #[test]
    fn opening_schema_13_refuses_without_creating_block_replay_column_family()
    -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let storage_path = tempdir.path().join("schema-mismatch-store");
        {
            let store =
                PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())?;
            let mut metadata_bytes = Vec::with_capacity(6);
            metadata_bytes.extend_from_slice(&13_u16.to_be_bytes());
            metadata_bytes.extend_from_slice(&Network::ZcashRegtest.id().to_be_bytes());
            store.store.inner.write(vec![StoragePut {
                table: StorageTable::StorageControl,
                key: StoreKey::store_metadata(),
                value: metadata_bytes,
            }])?;
        }

        let column_families = DB::list_cf(&Options::default(), &storage_path)?;
        let database = DB::open_cf(&Options::default(), &storage_path, column_families)?;
        database.drop_cf(StorageTable::BlockReplay.column_family_name())?;
        drop(database);
        let before_open = DB::list_cf(&Options::default(), &storage_path)?;
        assert!(!before_open.iter().any(|column_family| {
            column_family == StorageTable::BlockReplay.column_family_name()
        }));

        let Err(error) =
            PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())
        else {
            return Err("expected schema-mismatch rejection on reopen".into());
        };

        assert!(
            matches!(
                error,
                StoreError::SchemaMismatch {
                    persisted_version,
                    expected_version: CURRENT_STORE_SCHEMA_VERSION,
                } if persisted_version == 13
            ),
            "unexpected error: {error:?}"
        );
        let after_open = DB::list_cf(&Options::default(), &storage_path)?;
        assert_eq!(after_open, before_open);

        Ok(())
    }

    #[test]
    fn opening_schema_nineteen_with_missing_block_replay_envelopes_refuses_without_mutating_manifest()
    -> Result<(), Box<dyn Error>> {
        let tempdir = tempdir()?;
        let storage_path = tempdir.path().join("incomplete-schema-19-store");
        {
            let _store =
                PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())?;
        }

        let column_families = DB::list_cf(&Options::default(), &storage_path)?;
        let database = DB::open_cf(&Options::default(), &storage_path, column_families)?;
        database.drop_cf(StorageTable::BlockReplay.column_family_name())?;
        drop(database);
        let before_open = DB::list_cf(&Options::default(), &storage_path)?;

        let Err(error) =
            PrimaryChainStore::open(&storage_path, ChainStoreOptions::for_local_tests())
        else {
            return Err("expected incomplete schema-19 rejection on reopen".into());
        };
        assert!(
            matches!(
                error,
                StoreError::StoreSchemaIncomplete {
                    missing_column_family,
                } if missing_column_family == StorageTable::BlockReplay.column_family_name()
            ),
            "unexpected error: {error:?}"
        );
        let after_open = DB::list_cf(&Options::default(), &storage_path)?;
        assert_eq!(after_open, before_open);

        Ok(())
    }

    fn synthetic_epoch(
        chain_epoch_id: u64,
        height: u32,
    ) -> (ChainEpoch, BlockHeaderArtifact, CompactBlockArtifact) {
        synthetic_epoch_with_hash_seed(chain_epoch_id, height, height, height.saturating_sub(1))
    }

    fn synthetic_epoch_with_hash_seed(
        chain_epoch_id: u64,
        height: u32,
        hash_seed: u32,
        parent_hash_seed: u32,
    ) -> (ChainEpoch, BlockHeaderArtifact, CompactBlockArtifact) {
        let source_hash = block_hash(hash_seed);
        let parent_hash = block_hash(parent_hash_seed);
        let block_height = BlockHeight::new(height);

        (
            ChainEpoch {
                id: ChainEpochId::new(chain_epoch_id),
                network: Network::ZcashRegtest,
                visible_tip_height: block_height,
                visible_tip_hash: source_hash,
                settled_tip_height: block_height,
                settled_tip_hash: source_hash,
                artifact_schema_version: CURRENT_ARTIFACT_SCHEMA_VERSION,
                tip_metadata: ChainTipMetadata::empty(),
                created_at: UnixTimestampMillis::new(1_774_668_500_000 + u64::from(height)),
            },
            synthetic_block_header(
                block_height,
                source_hash,
                parent_hash,
                format!("raw-block-{chain_epoch_id}-{height}").as_bytes(),
            ),
            CompactBlockArtifact::empty(
                BlockId::new(block_height, source_hash),
                parent_hash,
                0,
                zinder_core::CompactChainMetadata {
                    sapling_commitment_tree_size: 0,
                    orchard_commitment_tree_size: 0,
                    ironwood_commitment_tree_size: 0,
                },
            ),
        )
    }

    fn block_hash(seed: u32) -> BlockHash {
        let mut bytes = [0; 32];
        for chunk in bytes.chunks_exact_mut(4) {
            chunk.copy_from_slice(&seed.to_be_bytes());
        }
        BlockHash::from_bytes(bytes)
    }

    fn synthetic_block_header(
        height: BlockHeight,
        block_hash: BlockHash,
        parent_hash: BlockHash,
        raw_block_bytes: &[u8],
    ) -> BlockHeaderArtifact {
        BlockHeaderArtifact::new(
            height,
            block_hash,
            parent_hash,
            [0; 32],
            [0; 32],
            0,
            0,
            [0; 32],
            0,
            u64::try_from(raw_block_bytes.len()).unwrap_or(u64::MAX),
        )
    }
}
