//! Generation-scoped transparent-address summaries and balance ranking.
//!
//! Snapshot bootstrap writes a new generation without touching the shared
//! chain-event cursor, then activates that generation in one metadata write.
//! Steady-state blocks mutate only the active generation and persist complete
//! before-images by height, allowing a reorg batch to restore summaries,
//! ranking keys, aggregate statistics, and coverage atomically.

use std::collections::{HashMap, HashSet};

use rust_rocksdb::WriteBatch;
use zinder_core::wire::{encode_address_script_hash, encode_height_key_ascending};
use zinder_core::{BlockHash, BlockHeight, TransparentAddressScriptHash};

use crate::consumer::address_value_event::{AddressValueEventKind, address_value_events};
use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewConsumerError, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
};
use crate::error::{MaterializedViewStoreColumnFamily, MaterializedViewStoreError};
use crate::store::MaterializedViewStore;

/// Generation-prefixed per-address summary rows.
pub const TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY: &str =
    "transparent_address_ranking_summary";
/// Generation-prefixed `(balance descending, script hash ascending)` rows.
pub const TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY: &str =
    "transparent_address_ranking_index";
/// Per-height before-image journals used by steady-state reorg rollback.
pub const TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY: &str = "transparent_address_ranking_undo";
/// Active-generation and in-progress snapshot-build metadata.
pub const TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY: &str =
    "transparent_address_ranking_metadata";

/// Column families owned by the ranking consumer.
pub const TRANSPARENT_ADDRESS_RANKING_COLUMN_FAMILIES: &[&str] = &[
    TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY,
    TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY,
];

/// Stable consumer identity used by materialized-view cursor persistence.
pub const TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("transparent_address_ranking");

/// Second on-disk contract for transparent-address ranking.
///
/// Version 2 adds incrementally maintained P2PKH and P2SH aggregate totals.
pub const TRANSPARENT_ADDRESS_RANKING_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME,
        2,
        TRANSPARENT_ADDRESS_RANKING_COLUMN_FAMILIES,
    );

const ACTIVE_METADATA_KEY: &[u8] = b"active";
const BUILD_METADATA_KEY: &[u8] = b"build";
const BUILD_MANIFEST_KEY: &[u8] = b"build_manifest";
const FORMAT_VERSION: u8 = 2;
const GENERATION_LEN: usize = 8;
const ADDRESS_HASH_LEN: usize = 32;
const SUMMARY_KEY_LEN: usize = GENERATION_LEN + ADDRESS_HASH_LEN;
const RANKING_KEY_LEN: usize = GENERATION_LEN + 8 + ADDRESS_HASH_LEN;
const SUMMARY_FIXED_LEN: usize = 2 + (4 * 8) + (4 * 8) + 4;
const METADATA_LEN: usize = 1 + (9 * 8) + 4 + 1 + 4 + 4 + 1;
const SNAPSHOT_BUILD_MANIFEST_LEN: usize = 1 + 8 + 4 + 32 + 1 + 4 + 4 + 1 + 4 + 32 + 8 + 8 + 1;
const UNDO_KEY_LEN: usize = GENERATION_LEN + 4;
const UNDO_HEADER_LEN: usize = 1 + 8 + 32 + 4 + METADATA_LEN + 4;
const ABSENT_SUMMARY_LEN: u32 = u32::MAX;
const TOP_TEN: usize = 10;
const TOP_ONE_HUNDRED: usize = 100;
/// Hard native bound for one ranking page.
pub const TRANSPARENT_ADDRESS_RANKING_MAX_PAGE_SIZE: usize = 500;

/// Lifetime facts and current balance for one transparent script hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressSummary {
    /// Raw `scriptPubKey`, when a snapshot or received output supplied it.
    pub script_pub_key: Option<Vec<u8>>,
    /// Current confirmed balance in zatoshis.
    pub balance_zat: u64,
    /// Confirmed value ever received over covered history.
    pub total_received_zat: u64,
    /// Confirmed value ever spent over covered history.
    pub total_sent_zat: u64,
    /// Distinct confirmed transactions touching this address.
    pub distinct_transaction_count: u64,
    /// Earliest covered transaction time.
    pub first_seen_unix_seconds: Option<i64>,
    /// Latest covered transaction time.
    pub last_seen_unix_seconds: Option<i64>,
    /// Snapshot-provided lower-extrema fallback retained across tail updates.
    pub snapshot_first_seen_unix_seconds: Option<i64>,
    /// Snapshot-provided upper-extrema fallback retained across tail updates.
    pub snapshot_last_seen_unix_seconds: Option<i64>,
}

impl TransparentAddressSummary {
    fn effective_first_seen_unix_seconds(&self) -> Option<i64> {
        minimum_optional(
            self.first_seen_unix_seconds,
            self.snapshot_first_seen_unix_seconds,
        )
    }

    fn effective_last_seen_unix_seconds(&self) -> Option<i64> {
        maximum_optional(
            self.last_seen_unix_seconds,
            self.snapshot_last_seen_unix_seconds,
        )
    }
}

/// One summary row supplied by an ingest-owned snapshot scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressRankingSnapshotRow {
    /// SHA-256 of the row's raw transparent script.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Snapshot summary to persist in the target generation.
    pub summary: TransparentAddressSummary,
}

/// Canonical boundaries and row cardinality pinned before a snapshot build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransparentAddressRankingSnapshotPlan {
    /// New generation prefix. Generations increase monotonically.
    pub generation: u64,
    /// Settled base height represented by the initial snapshot rows.
    pub base_height: BlockHeight,
    /// Canonical block hash at `base_height`.
    pub base_block_hash: BlockHash,
    /// Visible height reached by cursor-neutral tail seeding before activation.
    pub target_height: BlockHeight,
    /// Canonical block hash at `target_height`.
    pub target_block_hash: BlockHash,
    /// Exact number of summary rows expected in the settled snapshot.
    pub expected_summary_count: u64,
    /// Completeness of the settled base rows.
    pub base_coverage: TransparentAddressRankingCoverage,
}

/// Completeness claims attached to one active or building generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransparentAddressRankingCoverage {
    /// Height through which current balances are complete.
    pub balance_complete_through_height: BlockHeight,
    /// First height included in lifetime statistics, when known.
    pub history_complete_from_height: Option<BlockHeight>,
    /// Last contiguous height included in lifetime statistics, when known.
    pub history_complete_through_height: Option<BlockHeight>,
    /// Whether lifetime totals and extrema cover the canonical history.
    pub lifetime_statistics_complete: bool,
}

/// Positive-address count and balance for one standard script template.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransparentAddressScriptTypeTotals {
    /// Number of positive-balance addresses using this script template.
    pub positive_address_count: u64,
    /// Balance sum over those addresses.
    pub total_positive_balance_zat: u64,
}

/// Durable aggregate and coverage metadata for a generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransparentAddressRankingMetadata {
    /// Generation prefix used by summary and ranking keys.
    pub generation: u64,
    /// Number of positive-balance standard P2PKH/P2SH summaries.
    pub positive_address_count: u64,
    /// Balance sum over those positive standard summaries.
    pub total_positive_balance_zat: u64,
    /// Balance sum over the first ten ranked summaries.
    pub top_10_balance_zat: u64,
    /// Balance sum over the first one hundred ranked summaries.
    pub top_100_balance_zat: u64,
    /// Aggregate for positive exact P2PKH scripts.
    pub p2pkh: TransparentAddressScriptTypeTotals,
    /// Aggregate for positive exact P2SH scripts.
    pub p2sh: TransparentAddressScriptTypeTotals,
    /// Balance and lifetime-history completeness.
    pub coverage: TransparentAddressRankingCoverage,
}

impl TransparentAddressRankingMetadata {
    fn empty(generation: u64, coverage: TransparentAddressRankingCoverage) -> Self {
        Self {
            generation,
            positive_address_count: 0,
            total_positive_balance_zat: 0,
            top_10_balance_zat: 0,
            top_100_balance_zat: 0,
            p2pkh: TransparentAddressScriptTypeTotals::default(),
            p2sh: TransparentAddressScriptTypeTotals::default(),
            coverage,
        }
    }
}

/// One row returned by a ranking page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressRankingEntry {
    /// One-based rank in deterministic ranking order.
    pub rank: u64,
    /// Address script hash identifying the summary.
    pub address_script_hash: TransparentAddressScriptHash,
    /// Full summary associated with the rank.
    pub summary: TransparentAddressSummary,
}

/// A bounded offset page plus generation-wide aggregate metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransparentAddressRankingPage {
    /// Ranked rows after the requested offset.
    pub entries: Vec<TransparentAddressRankingEntry>,
    /// Metadata for the generation read by this page.
    pub metadata: TransparentAddressRankingMetadata,
}

#[derive(Clone, Debug, Default)]
struct AddressBlockDelta {
    received_zat: u64,
    sent_zat: u64,
    transaction_positions: HashSet<u32>,
    script_pub_key: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct UndoJournal {
    generation: u64,
    block_hash: BlockHash,
    metadata_before: TransparentAddressRankingMetadata,
    summaries_before: Vec<(
        TransparentAddressScriptHash,
        Option<TransparentAddressSummary>,
    )>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SnapshotBuildManifest {
    plan: TransparentAddressRankingSnapshotPlan,
    written_summary_count: u64,
    base_rows_complete: bool,
}

struct SummaryTransition<'summary> {
    address_script_hash: TransparentAddressScriptHash,
    before: Option<&'summary TransparentAddressSummary>,
    after: Option<TransparentAddressSummary>,
}

/// Materializes generation-scoped transparent-address ranking state.
#[derive(Default)]
pub struct TransparentAddressRankingConsumer {
    pending_summaries: HashMap<TransparentAddressScriptHash, Option<TransparentAddressSummary>>,
    pending_metadata: Option<TransparentAddressRankingMetadata>,
    reverted_addresses: HashSet<TransparentAddressScriptHash>,
    restored_revert_metadata: bool,
}

impl TransparentAddressRankingConsumer {
    /// Builds an empty consumer with no pending batch overlay.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true only for exact P2PKH or P2SH `scriptPubKey` templates.
    #[must_use]
    pub fn is_standard_transparent_script(script_pub_key: &[u8]) -> bool {
        is_p2pkh_script(script_pub_key) || is_p2sh_script(script_pub_key)
    }

    /// Reads metadata for the atomically active generation.
    pub fn active_metadata(
        store: &MaterializedViewStore,
    ) -> Result<Option<TransparentAddressRankingMetadata>, MaterializedViewStoreError> {
        read_metadata(store, ACTIVE_METADATA_KEY)
    }

    /// Reads metadata for an in-progress snapshot generation.
    pub fn build_metadata(
        store: &MaterializedViewStore,
    ) -> Result<Option<TransparentAddressRankingMetadata>, MaterializedViewStoreError> {
        read_metadata(store, BUILD_METADATA_KEY)
    }

    /// Initializes or resumes one canonically pinned inactive snapshot build.
    ///
    /// A different stale build is cleared before the new plan is persisted, so
    /// rows absent from a restarted canonical snapshot cannot survive.
    pub fn initialize_snapshot_generation(
        store: &MaterializedViewStore,
        plan: TransparentAddressRankingSnapshotPlan,
    ) -> Result<(), TransparentAddressRankingConsumerError> {
        validate_snapshot_plan(plan)?;
        if plan.generation == 0 || plan.generation == u64::MAX {
            return Err(TransparentAddressRankingConsumerError::InvalidGeneration {
                generation: plan.generation,
            });
        }
        if Self::active_metadata(store)?.is_some_and(|active| active.generation >= plan.generation)
        {
            return Err(TransparentAddressRankingConsumerError::InvalidGeneration {
                generation: plan.generation,
            });
        }
        let requested = SnapshotBuildManifest {
            plan,
            written_summary_count: 0,
            base_rows_complete: false,
        };
        let existing_build = read_snapshot_build_manifest(store)?;
        if existing_build.is_some_and(|existing| existing.plan == plan) {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        if let Some(existing) = existing_build {
            stage_generation_clear(store, &mut batch, existing.plan.generation)?;
        }
        stage_generation_clear(store, &mut batch, plan.generation)?;
        let metadata_cf =
            store.consumer_column_family(TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY)?;
        batch.put_cf(
            &metadata_cf,
            BUILD_METADATA_KEY,
            encode_metadata(TransparentAddressRankingMetadata::empty(
                plan.generation,
                plan.base_coverage,
            )),
        );
        batch.put_cf(
            &metadata_cf,
            BUILD_MANIFEST_KEY,
            encode_snapshot_build_manifest(requested),
        );
        store.write_consumer_batch(TRANSPARENT_ADDRESS_RANKING_SCHEMA.name, &batch)?;
        Ok(())
    }

    /// Atomically writes one idempotent snapshot batch into an inactive generation.
    ///
    /// This API does not read or advance the chain-event cursor. Repeating rows
    /// replaces their previous generation-local values and updates build
    /// aggregates by checked subtraction and addition.
    pub fn write_snapshot_batch(
        store: &MaterializedViewStore,
        generation: u64,
        rows: &[TransparentAddressRankingSnapshotRow],
    ) -> Result<(), TransparentAddressRankingConsumerError> {
        let mut metadata = Self::build_metadata(store)?
            .ok_or(TransparentAddressRankingConsumerError::SnapshotBuildMissing { generation })?;
        if metadata.generation != generation {
            return Err(
                TransparentAddressRankingConsumerError::SnapshotBuildConflict {
                    requested_generation: generation,
                    existing_generation: metadata.generation,
                },
            );
        }
        let mut manifest = read_snapshot_build_manifest(store)?
            .filter(|manifest| manifest.plan.generation == generation)
            .ok_or(TransparentAddressRankingConsumerError::SnapshotBuildMissing { generation })?;
        if manifest.base_rows_complete {
            return Err(TransparentAddressRankingConsumerError::SnapshotBaseAlreadyComplete);
        }
        let mut seen = HashSet::with_capacity(rows.len());
        let mut batch = WriteBatch::default();
        let summary_cf =
            store.consumer_column_family(TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY)?;
        let ranking_cf =
            store.consumer_column_family(TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY)?;
        for row in rows {
            if !seen.insert(row.address_script_hash) {
                return Err(TransparentAddressRankingConsumerError::DuplicateSnapshotAddress);
            }
            validate_summary(row.address_script_hash, &row.summary)?;
            let key = summary_key(generation, row.address_script_hash);
            let existing = store
                .get_consumer(TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY, &key)?
                .map(|payload| decode_summary(&payload))
                .transpose()?;
            if existing.is_none() {
                manifest.written_summary_count = manifest
                    .written_summary_count
                    .checked_add(1)
                    .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
            }
            if let Some(existing) = &existing {
                stage_ranking_delete(
                    &mut batch,
                    &ranking_cf,
                    generation,
                    row.address_script_hash,
                    existing,
                );
                remove_from_aggregate(&mut metadata, existing)?;
            }
            let encoded = encode_summary(&row.summary)?;
            batch.put_cf(&summary_cf, key, encoded.as_slice());
            stage_ranking_put(
                &mut batch,
                &ranking_cf,
                generation,
                row.address_script_hash,
                &row.summary,
            )?;
            add_to_aggregate(&mut metadata, &row.summary)?;
        }
        let metadata_cf =
            store.consumer_column_family(TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY)?;
        batch.put_cf(&metadata_cf, BUILD_METADATA_KEY, encode_metadata(metadata));
        batch.put_cf(
            &metadata_cf,
            BUILD_MANIFEST_KEY,
            encode_snapshot_build_manifest(manifest),
        );
        store.write_consumer_batch(TRANSPARENT_ADDRESS_RANKING_SCHEMA.name, &batch)?;
        Ok(())
    }

    /// Seals the settled snapshot rows before cursor-neutral tail seeding.
    pub fn finalize_snapshot_base(
        store: &MaterializedViewStore,
        generation: u64,
    ) -> Result<(), TransparentAddressRankingConsumerError> {
        let mut manifest = read_snapshot_build_manifest(store)?
            .filter(|manifest| manifest.plan.generation == generation)
            .ok_or(TransparentAddressRankingConsumerError::SnapshotBuildMissing { generation })?;
        if manifest.written_summary_count != manifest.plan.expected_summary_count {
            return Err(
                TransparentAddressRankingConsumerError::SnapshotRowCountMismatch {
                    expected: manifest.plan.expected_summary_count,
                    actual: manifest.written_summary_count,
                },
            );
        }
        manifest.base_rows_complete = true;
        store.put_consumer(
            TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY,
            BUILD_MANIFEST_KEY,
            &encode_snapshot_build_manifest(manifest),
        )?;
        Ok(())
    }

    /// Applies one canonical unsettled block to an inactive settled base.
    ///
    /// The write is cursor-neutral and persists the same undo journal normal
    /// live dispatch would need if a later reorg reaches this height.
    pub fn write_snapshot_tail_block(
        store: &MaterializedViewStore,
        generation: u64,
        block: &BlockCommitContext,
    ) -> Result<(), TransparentAddressRankingConsumerError> {
        let manifest = read_snapshot_build_manifest(store)?
            .filter(|manifest| manifest.plan.generation == generation)
            .ok_or(TransparentAddressRankingConsumerError::SnapshotBuildMissing { generation })?;
        if !manifest.base_rows_complete {
            return Err(TransparentAddressRankingConsumerError::SnapshotBaseIncomplete);
        }
        let metadata = Self::build_metadata(store)?
            .filter(|metadata| metadata.generation == generation)
            .ok_or(TransparentAddressRankingConsumerError::SnapshotBuildMissing { generation })?;
        if block.height > manifest.plan.target_height {
            return Err(TransparentAddressRankingConsumerError::SnapshotTailPastTarget);
        }
        if block.height <= metadata.coverage.balance_complete_through_height {
            return verify_applied_block(store, generation, block);
        }

        let mut consumer = Self::new();
        consumer.pending_metadata = Some(metadata);
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.apply_block(block, &mut ctx).map_err(|error| {
            TransparentAddressRankingConsumerError::SnapshotTail(error.to_string())
        })?;
        let mut updated = consumer
            .pending_metadata
            .ok_or(TransparentAddressRankingConsumerError::SnapshotBaseIncomplete)?;
        let (top_ten_sum, top_hundred_sum) =
            top_balance_sums_with_overlay(store, generation, &consumer.pending_summaries)?;
        updated.top_10_balance_zat = top_ten_sum;
        updated.top_100_balance_zat = top_hundred_sum;
        let metadata_cf =
            store.consumer_column_family(TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY)?;
        batch.put_cf(&metadata_cf, BUILD_METADATA_KEY, encode_metadata(updated));
        store.write_consumer_batch(TRANSPARENT_ADDRESS_RANKING_SCHEMA.name, &batch)?;
        Ok(())
    }

    /// Atomically activates a built generation at one chain-event boundary.
    pub fn activate_snapshot_generation_at_cursor(
        store: &MaterializedViewStore,
        generation: u64,
        cursor_bytes: &[u8],
    ) -> Result<TransparentAddressRankingMetadata, TransparentAddressRankingConsumerError> {
        let Some(mut metadata) = Self::build_metadata(store)? else {
            let active = Self::active_metadata(store)?
                .filter(|active| active.generation == generation)
                .ok_or(
                    TransparentAddressRankingConsumerError::SnapshotBuildMissing { generation },
                )?;
            let persisted_cursor =
                store.get_chain_event_cursor(TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME)?;
            if persisted_cursor.as_deref() != Some(cursor_bytes) {
                return Err(TransparentAddressRankingConsumerError::ActiveCursorMismatch);
            }
            return Ok(active);
        };
        if metadata.generation != generation {
            return Err(
                TransparentAddressRankingConsumerError::SnapshotBuildConflict {
                    requested_generation: generation,
                    existing_generation: metadata.generation,
                },
            );
        }
        let manifest = read_snapshot_build_manifest(store)?
            .filter(|manifest| manifest.plan.generation == generation)
            .ok_or(TransparentAddressRankingConsumerError::SnapshotBuildMissing { generation })?;
        if !manifest.base_rows_complete
            || metadata.coverage.balance_complete_through_height != manifest.plan.target_height
            || metadata.coverage.history_complete_through_height
                != Some(manifest.plan.target_height)
        {
            return Err(TransparentAddressRankingConsumerError::SnapshotTailIncomplete);
        }
        verify_snapshot_target(store, &manifest)?;
        let (top_ten_sum, top_hundred_sum) = top_balance_sums_for_generation(store, generation)?;
        metadata.top_10_balance_zat = top_ten_sum;
        metadata.top_100_balance_zat = top_hundred_sum;
        let metadata_cf =
            store.consumer_column_family(TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&metadata_cf, ACTIVE_METADATA_KEY, encode_metadata(metadata));
        batch.delete_cf(&metadata_cf, BUILD_METADATA_KEY);
        batch.delete_cf(&metadata_cf, BUILD_MANIFEST_KEY);
        store.stage_chain_event_cursor(
            &mut batch,
            TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME,
            cursor_bytes,
        )?;
        store.write_consumer_batch(TRANSPARENT_ADDRESS_RANKING_SCHEMA.name, &batch)?;
        Ok(metadata)
    }

    /// Reads one summary from the active generation.
    pub fn summary(
        store: &MaterializedViewStore,
        address_script_hash: TransparentAddressScriptHash,
    ) -> Result<Option<TransparentAddressSummary>, MaterializedViewStoreError> {
        let Some(metadata) = Self::active_metadata(store)? else {
            return Ok(None);
        };
        let key = summary_key(metadata.generation, address_script_hash);
        store
            .get_consumer(TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY, &key)?
            .map(|payload| decode_summary(&payload).map_err(store_decode_error))
            .transpose()
    }

    /// Reads a bounded page from the active generation.
    ///
    /// `limit` is intentionally trusted as the server-enforced page bound.
    /// The implementation still checks offset and rank arithmetic.
    pub fn page(
        store: &MaterializedViewStore,
        offset: u64,
        limit: usize,
    ) -> Result<Option<TransparentAddressRankingPage>, MaterializedViewStoreError> {
        let Some(metadata) = Self::active_metadata(store)? else {
            return Ok(None);
        };
        if limit > TRANSPARENT_ADDRESS_RANKING_MAX_PAGE_SIZE {
            return Err(store_decode_error(
                TransparentAddressRankingConsumerError::PageBounds,
            ));
        }
        let (start, end) = generation_range(metadata.generation);
        let rows = store.page_consumer_range(
            TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY,
            start.as_slice()..=end.as_slice(),
            offset,
            limit,
        )?;
        let mut entries = Vec::with_capacity(rows.len());
        for (index, (key, payload)) in rows.into_iter().enumerate() {
            let (generation, balance_zat, address_script_hash) =
                decode_ranking_key(&key).map_err(store_decode_error)?;
            if generation != metadata.generation {
                return Err(store_decode_error(
                    TransparentAddressRankingConsumerError::MalformedRankingKey,
                ));
            }
            let summary = decode_summary(&payload).map_err(store_decode_error)?;
            if summary.balance_zat != balance_zat || !is_ranked_summary(&summary) {
                return Err(store_decode_error(
                    TransparentAddressRankingConsumerError::RankingSummaryMismatch,
                ));
            }
            let rank = offset
                .checked_add(u64::try_from(index).map_err(|_| {
                    store_decode_error(TransparentAddressRankingConsumerError::PageBounds)
                })?)
                .and_then(|rank| rank.checked_add(1))
                .ok_or_else(|| {
                    store_decode_error(TransparentAddressRankingConsumerError::PageBounds)
                })?;
            entries.push(TransparentAddressRankingEntry {
                rank,
                address_script_hash,
                summary,
            });
        }
        Ok(Some(TransparentAddressRankingPage { entries, metadata }))
    }

    fn current_metadata(
        &self,
        store: &MaterializedViewStore,
    ) -> Result<TransparentAddressRankingMetadata, TransparentAddressRankingConsumerError> {
        if let Some(metadata) = self.pending_metadata {
            return Ok(metadata);
        }
        Self::active_metadata(store)?
            .ok_or(TransparentAddressRankingConsumerError::ActiveGenerationMissing)
    }

    fn current_summary(
        &self,
        store: &MaterializedViewStore,
        generation: u64,
        address_script_hash: TransparentAddressScriptHash,
    ) -> Result<Option<TransparentAddressSummary>, TransparentAddressRankingConsumerError> {
        if let Some(summary) = self.pending_summaries.get(&address_script_hash) {
            return Ok(summary.clone());
        }
        store
            .get_consumer(
                TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY,
                &summary_key(generation, address_script_hash),
            )?
            .map(|payload| decode_summary(&payload))
            .transpose()
    }

    fn stage_summary_transition(
        &mut self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
        generation: u64,
        transition: SummaryTransition<'_>,
    ) -> Result<(), TransparentAddressRankingConsumerError> {
        let summary_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY)?;
        let ranking_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY)?;
        if let Some(before) = transition.before {
            stage_ranking_delete(
                ctx.batch,
                &ranking_cf,
                generation,
                transition.address_script_hash,
                before,
            );
        }
        let key = summary_key(generation, transition.address_script_hash);
        match &transition.after {
            Some(summary) => {
                let encoded = encode_summary(summary)?;
                ctx.batch.put_cf(&summary_cf, key, encoded.as_slice());
                stage_ranking_put(
                    ctx.batch,
                    &ranking_cf,
                    generation,
                    transition.address_script_hash,
                    summary,
                )?;
            }
            None => ctx.batch.delete_cf(&summary_cf, key),
        }
        self.pending_summaries
            .insert(transition.address_script_hash, transition.after);
        Ok(())
    }
}

impl BlockKeyedConsumer for TransparentAddressRankingConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME
    }

    fn begin_batch(
        &mut self,
        _ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        self.pending_summaries.clear();
        self.pending_metadata = None;
        self.reverted_addresses.clear();
        self.restored_revert_metadata = false;
        Ok(())
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let spends = block.transparent_spends();
        validate_transparent_spends(block, spends.as_deref())?;

        let mut metadata = self.current_metadata(ctx.store)?;
        if block.height <= metadata.coverage.balance_complete_through_height {
            verify_applied_block(ctx.store, metadata.generation, block)?;
            return Ok(());
        }
        let expected_height = metadata
            .coverage
            .balance_complete_through_height
            .next()
            .ok_or(TransparentAddressRankingConsumerError::CoverageOverflow)?;
        if expected_height != block.height {
            return Err(Box::new(
                TransparentAddressRankingConsumerError::NonContiguousTail {
                    expected_height: expected_height.value(),
                    actual_height: block.height.value(),
                },
            ));
        }
        let mut deltas = aggregate_block_deltas(block, spends.as_deref())?
            .into_iter()
            .collect::<Vec<_>>();
        deltas.sort_unstable_by_key(|(address_script_hash, _delta)| address_script_hash.as_bytes());
        let mut before_images = Vec::with_capacity(deltas.len());
        for (address_script_hash, delta) in deltas {
            let before =
                self.current_summary(ctx.store, metadata.generation, address_script_hash)?;
            before_images.push((address_script_hash, before.clone()));
            let after = apply_delta(before.as_ref(), &delta, block.block_time_unix_seconds)?;
            if let Some(before) = &before {
                remove_from_aggregate(&mut metadata, before)?;
            }
            add_to_aggregate(&mut metadata, &after)?;
            self.stage_summary_transition(
                ctx,
                metadata.generation,
                SummaryTransition {
                    address_script_hash,
                    before: before.as_ref(),
                    after: Some(after),
                },
            )?;
        }
        // Top-balance sums are recomputed by `finish_batch` after every
        // batch, so a pre-image carries none; a mid-batch value would make the
        // journal depend on how blocks were grouped into batches.
        let metadata_before = TransparentAddressRankingMetadata {
            top_10_balance_zat: 0,
            top_100_balance_zat: 0,
            ..self.current_metadata(ctx.store)?
        };
        let journal = UndoJournal {
            generation: metadata.generation,
            block_hash: block.block_hash,
            metadata_before,
            summaries_before: before_images,
        };
        let undo_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY)?;
        ctx.batch.put_cf(
            &undo_cf,
            undo_key(metadata.generation, block.height),
            encode_undo_journal(&journal)?,
        );
        metadata.coverage.balance_complete_through_height = block.height;
        if metadata.coverage.history_complete_through_height
            == Some(metadata_before.coverage.balance_complete_through_height)
        {
            metadata.coverage.history_complete_through_height = Some(block.height);
        }
        self.pending_metadata = Some(metadata);
        Ok(())
    }

    fn revert_block(
        &mut self,
        height: BlockHeight,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let active = Self::active_metadata(ctx.store)?
            .ok_or(TransparentAddressRankingConsumerError::ActiveGenerationMissing)?;
        let key = undo_key(active.generation, height);
        let payload = ctx
            .store
            .get_consumer(TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY, &key)?
            .ok_or_else(
                || TransparentAddressRankingConsumerError::UndoJournalMissing {
                    height: height.value(),
                },
            )?;
        let journal = decode_undo_journal(&payload)?;
        if journal.generation != active.generation {
            return Err(Box::new(
                TransparentAddressRankingConsumerError::UndoGenerationMismatch {
                    height: height.value(),
                    active_generation: active.generation,
                    journal_generation: journal.generation,
                },
            ));
        }
        if !self.restored_revert_metadata {
            self.pending_metadata = Some(journal.metadata_before);
            self.restored_revert_metadata = true;
        }
        for (address_script_hash, before) in journal.summaries_before {
            if !self.reverted_addresses.insert(address_script_hash) {
                continue;
            }
            let current =
                self.current_summary(ctx.store, journal.generation, address_script_hash)?;
            self.stage_summary_transition(
                ctx,
                journal.generation,
                SummaryTransition {
                    address_script_hash,
                    before: current.as_ref(),
                    after: before,
                },
            )?;
        }
        let undo_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY)?;
        ctx.batch.delete_cf(&undo_cf, key);
        Ok(())
    }

    fn finish_batch(
        &mut self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let Some(mut metadata) = self.pending_metadata else {
            return Ok(());
        };
        let (top_ten_sum, top_hundred_sum) =
            top_balance_sums_with_overlay(ctx.store, metadata.generation, &self.pending_summaries)?;
        metadata.top_10_balance_zat = top_ten_sum;
        metadata.top_100_balance_zat = top_hundred_sum;
        let metadata_cf = ctx
            .store
            .consumer_column_family(TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY)?;
        ctx.batch
            .put_cf(&metadata_cf, ACTIVE_METADATA_KEY, encode_metadata(metadata));
        self.pending_metadata = None;
        self.pending_summaries.clear();
        self.reverted_addresses.clear();
        self.restored_revert_metadata = false;
        Ok(())
    }
}

fn validate_transparent_spends(
    block: &BlockCommitContext,
    spends: Option<&HashMap<zinder_core::TransparentOutPoint, zinder_core::TransparentSpendFact>>,
) -> Result<(), TransparentAddressRankingConsumerError> {
    for transaction in &block.transactions {
        if transaction.public_facts.is_coinbase {
            continue;
        }
        for input in &transaction.transparent_inputs {
            let spends = spends.ok_or_else(|| {
                TransparentAddressRankingConsumerError::TransparentSpendsUnavailable {
                    height: block.height.value(),
                }
            })?;
            if !spends.contains_key(&input.spent_outpoint) {
                return Err(
                    TransparentAddressRankingConsumerError::TransparentSpendUnresolved {
                        height: block.height.value(),
                        input_index: input.input_index,
                    },
                );
            }
        }
    }
    Ok(())
}

fn aggregate_block_deltas(
    block: &BlockCommitContext,
    spends: Option<&HashMap<zinder_core::TransparentOutPoint, zinder_core::TransparentSpendFact>>,
) -> Result<
    HashMap<TransparentAddressScriptHash, AddressBlockDelta>,
    TransparentAddressRankingConsumerError,
> {
    let mut deltas = HashMap::<TransparentAddressScriptHash, AddressBlockDelta>::new();
    for event in address_value_events(block, spends) {
        let delta = deltas.entry(event.address_script_hash).or_default();
        delta.transaction_positions.insert(event.in_block_position);
        match event.kind {
            AddressValueEventKind::Received => {
                delta.received_zat = delta
                    .received_zat
                    .checked_add(event.value_zat)
                    .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
            }
            AddressValueEventKind::Spent => {
                delta.sent_zat = delta
                    .sent_zat
                    .checked_add(event.value_zat)
                    .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
            }
        }
    }
    for transaction in &block.transactions {
        for output in &transaction.transparent_outputs {
            if TransparentAddressScriptHash::of_script_pub_key(&output.script_pub_key)
                != output.address_script_hash
            {
                return Err(TransparentAddressRankingConsumerError::ScriptHashMismatch);
            }
            let delta = deltas.entry(output.address_script_hash).or_default();
            match &delta.script_pub_key {
                Some(existing) if existing != &output.script_pub_key => {
                    return Err(TransparentAddressRankingConsumerError::ConflictingScript);
                }
                Some(_) => {}
                None => delta.script_pub_key = Some(output.script_pub_key.clone()),
            }
        }
    }
    Ok(deltas)
}

fn apply_delta(
    before: Option<&TransparentAddressSummary>,
    delta: &AddressBlockDelta,
    block_time_unix_seconds: i64,
) -> Result<TransparentAddressSummary, TransparentAddressRankingConsumerError> {
    let mut summary = before.cloned().unwrap_or(TransparentAddressSummary {
        script_pub_key: None,
        balance_zat: 0,
        total_received_zat: 0,
        total_sent_zat: 0,
        distinct_transaction_count: 0,
        first_seen_unix_seconds: None,
        last_seen_unix_seconds: None,
        snapshot_first_seen_unix_seconds: None,
        snapshot_last_seen_unix_seconds: None,
    });
    if let Some(script_pub_key) = &delta.script_pub_key {
        match &summary.script_pub_key {
            Some(existing) if existing != script_pub_key => {
                return Err(TransparentAddressRankingConsumerError::ConflictingScript);
            }
            Some(_) => {}
            None => summary.script_pub_key = Some(script_pub_key.clone()),
        }
    }
    summary.balance_zat = summary
        .balance_zat
        .checked_add(delta.received_zat)
        .and_then(|balance| balance.checked_sub(delta.sent_zat))
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    summary.total_received_zat = summary
        .total_received_zat
        .checked_add(delta.received_zat)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    summary.total_sent_zat = summary
        .total_sent_zat
        .checked_add(delta.sent_zat)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    summary.distinct_transaction_count = summary
        .distinct_transaction_count
        .checked_add(
            u64::try_from(delta.transaction_positions.len())
                .map_err(|_| TransparentAddressRankingConsumerError::ArithmeticOverflow)?,
        )
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    summary.first_seen_unix_seconds = Some(
        summary
            .effective_first_seen_unix_seconds()
            .map_or(block_time_unix_seconds, |first| {
                first.min(block_time_unix_seconds)
            }),
    );
    summary.last_seen_unix_seconds = Some(
        summary
            .effective_last_seen_unix_seconds()
            .map_or(block_time_unix_seconds, |last| {
                last.max(block_time_unix_seconds)
            }),
    );
    Ok(summary)
}

fn is_ranked_summary(summary: &TransparentAddressSummary) -> bool {
    summary.balance_zat > 0
        && summary
            .script_pub_key
            .as_deref()
            .is_some_and(TransparentAddressRankingConsumer::is_standard_transparent_script)
}

fn validate_summary(
    address_script_hash: TransparentAddressScriptHash,
    summary: &TransparentAddressSummary,
) -> Result<(), TransparentAddressRankingConsumerError> {
    if summary.script_pub_key.as_deref().is_some_and(|script| {
        TransparentAddressScriptHash::of_script_pub_key(script) != address_script_hash
    }) {
        return Err(TransparentAddressRankingConsumerError::ScriptHashMismatch);
    }
    Ok(())
}

fn validate_coverage(
    coverage: TransparentAddressRankingCoverage,
) -> Result<(), TransparentAddressRankingConsumerError> {
    match (
        coverage.history_complete_from_height,
        coverage.history_complete_through_height,
    ) {
        (Some(from), Some(through)) if from <= through => {}
        (None, None) if !coverage.lifetime_statistics_complete => {}
        _ => return Err(TransparentAddressRankingConsumerError::InvalidCoverage),
    }
    if coverage
        .history_complete_through_height
        .is_some_and(|through| through > coverage.balance_complete_through_height)
    {
        return Err(TransparentAddressRankingConsumerError::InvalidCoverage);
    }
    if coverage.lifetime_statistics_complete
        && (coverage.history_complete_from_height != Some(BlockHeight::new(1))
            || coverage.history_complete_through_height
                != Some(coverage.balance_complete_through_height))
    {
        return Err(TransparentAddressRankingConsumerError::InvalidCoverage);
    }
    Ok(())
}

fn validate_metadata(
    metadata: TransparentAddressRankingMetadata,
) -> Result<(), TransparentAddressRankingConsumerError> {
    let script_address_count = metadata
        .p2pkh
        .positive_address_count
        .checked_add(metadata.p2sh.positive_address_count)
        .ok_or(TransparentAddressRankingConsumerError::MalformedMetadata)?;
    let script_balance_zat = metadata
        .p2pkh
        .total_positive_balance_zat
        .checked_add(metadata.p2sh.total_positive_balance_zat)
        .ok_or(TransparentAddressRankingConsumerError::MalformedMetadata)?;
    if script_address_count != metadata.positive_address_count
        || script_balance_zat != metadata.total_positive_balance_zat
    {
        return Err(TransparentAddressRankingConsumerError::MalformedMetadata);
    }
    Ok(())
}

fn validate_snapshot_plan(
    plan: TransparentAddressRankingSnapshotPlan,
) -> Result<(), TransparentAddressRankingConsumerError> {
    validate_coverage(plan.base_coverage)?;
    if plan.base_coverage.balance_complete_through_height != plan.base_height
        || plan.target_height < plan.base_height
    {
        return Err(TransparentAddressRankingConsumerError::InvalidSnapshotPlan);
    }
    Ok(())
}

fn add_to_aggregate(
    metadata: &mut TransparentAddressRankingMetadata,
    summary: &TransparentAddressSummary,
) -> Result<(), TransparentAddressRankingConsumerError> {
    if !is_ranked_summary(summary) {
        return Ok(());
    }
    metadata.positive_address_count = metadata
        .positive_address_count
        .checked_add(1)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    metadata.total_positive_balance_zat = metadata
        .total_positive_balance_zat
        .checked_add(summary.balance_zat)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    let script_type_totals = script_type_totals_mut(metadata, summary)?;
    script_type_totals.positive_address_count = script_type_totals
        .positive_address_count
        .checked_add(1)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    script_type_totals.total_positive_balance_zat = script_type_totals
        .total_positive_balance_zat
        .checked_add(summary.balance_zat)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    Ok(())
}

fn remove_from_aggregate(
    metadata: &mut TransparentAddressRankingMetadata,
    summary: &TransparentAddressSummary,
) -> Result<(), TransparentAddressRankingConsumerError> {
    if !is_ranked_summary(summary) {
        return Ok(());
    }
    metadata.positive_address_count = metadata
        .positive_address_count
        .checked_sub(1)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    metadata.total_positive_balance_zat = metadata
        .total_positive_balance_zat
        .checked_sub(summary.balance_zat)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    let script_type_totals = script_type_totals_mut(metadata, summary)?;
    script_type_totals.positive_address_count = script_type_totals
        .positive_address_count
        .checked_sub(1)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    script_type_totals.total_positive_balance_zat = script_type_totals
        .total_positive_balance_zat
        .checked_sub(summary.balance_zat)
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    Ok(())
}

fn script_type_totals_mut<'metadata>(
    metadata: &'metadata mut TransparentAddressRankingMetadata,
    summary: &TransparentAddressSummary,
) -> Result<&'metadata mut TransparentAddressScriptTypeTotals, TransparentAddressRankingConsumerError>
{
    let script_pub_key = summary
        .script_pub_key
        .as_deref()
        .ok_or(TransparentAddressRankingConsumerError::MalformedSummary)?;
    if is_p2pkh_script(script_pub_key) {
        Ok(&mut metadata.p2pkh)
    } else if is_p2sh_script(script_pub_key) {
        Ok(&mut metadata.p2sh)
    } else {
        Err(TransparentAddressRankingConsumerError::MalformedSummary)
    }
}

fn is_p2pkh_script(script_pub_key: &[u8]) -> bool {
    script_pub_key.len() == 25
        && script_pub_key[0..3] == [0x76, 0xa9, 0x14]
        && script_pub_key[23..25] == [0x88, 0xac]
}

fn is_p2sh_script(script_pub_key: &[u8]) -> bool {
    script_pub_key.len() == 23 && script_pub_key[0..2] == [0xa9, 0x14] && script_pub_key[22] == 0x87
}

fn summary_key(
    generation: u64,
    address_script_hash: TransparentAddressScriptHash,
) -> [u8; SUMMARY_KEY_LEN] {
    let mut key = [0u8; SUMMARY_KEY_LEN];
    key[..GENERATION_LEN].copy_from_slice(&generation.to_be_bytes());
    key[GENERATION_LEN..].copy_from_slice(&encode_address_script_hash(address_script_hash));
    key
}

fn undo_key(generation: u64, height: BlockHeight) -> [u8; UNDO_KEY_LEN] {
    let mut key = [0u8; UNDO_KEY_LEN];
    key[..GENERATION_LEN].copy_from_slice(&generation.to_be_bytes());
    key[GENERATION_LEN..].copy_from_slice(&encode_height_key_ascending(height));
    key
}

fn verify_applied_block(
    store: &MaterializedViewStore,
    generation: u64,
    block: &BlockCommitContext,
) -> Result<(), TransparentAddressRankingConsumerError> {
    let payload = store
        .get_consumer(
            TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY,
            &undo_key(generation, block.height),
        )?
        .ok_or_else(
            || TransparentAddressRankingConsumerError::AppliedBlockJournalMissing {
                height: block.height.value(),
            },
        )?;
    let journal = decode_undo_journal(&payload)?;
    if journal.generation != generation || journal.block_hash != block.block_hash {
        return Err(
            TransparentAddressRankingConsumerError::AppliedBlockMismatch {
                height: block.height.value(),
            },
        );
    }
    Ok(())
}

fn verify_snapshot_target(
    store: &MaterializedViewStore,
    manifest: &SnapshotBuildManifest,
) -> Result<(), TransparentAddressRankingConsumerError> {
    if manifest.plan.target_height == manifest.plan.base_height {
        if manifest.plan.target_block_hash != manifest.plan.base_block_hash {
            return Err(TransparentAddressRankingConsumerError::SnapshotTargetMismatch);
        }
        return Ok(());
    }
    let payload = store
        .get_consumer(
            TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY,
            &undo_key(manifest.plan.generation, manifest.plan.target_height),
        )?
        .ok_or(TransparentAddressRankingConsumerError::SnapshotTargetMismatch)?;
    let journal = decode_undo_journal(&payload)?;
    if journal.generation != manifest.plan.generation
        || journal.block_hash != manifest.plan.target_block_hash
    {
        return Err(TransparentAddressRankingConsumerError::SnapshotTargetMismatch);
    }
    Ok(())
}

fn stage_generation_clear(
    store: &MaterializedViewStore,
    batch: &mut WriteBatch,
    generation: u64,
) -> Result<(), TransparentAddressRankingConsumerError> {
    let next_generation = generation
        .checked_add(1)
        .ok_or(TransparentAddressRankingConsumerError::InvalidGeneration { generation })?;
    for (column_family, suffix_len) in [
        (
            TRANSPARENT_ADDRESS_RANKING_SUMMARY_COLUMN_FAMILY,
            ADDRESS_HASH_LEN,
        ),
        (
            TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY,
            8 + ADDRESS_HASH_LEN,
        ),
        (TRANSPARENT_ADDRESS_RANKING_UNDO_COLUMN_FAMILY, 4),
    ] {
        let handle = store.consumer_column_family(column_family)?;
        let mut start = vec![0; GENERATION_LEN + suffix_len];
        let mut end = vec![0; GENERATION_LEN + suffix_len];
        start[..GENERATION_LEN].copy_from_slice(&generation.to_be_bytes());
        end[..GENERATION_LEN].copy_from_slice(&next_generation.to_be_bytes());
        batch.delete_range_cf(&handle, start, end);
    }
    Ok(())
}

fn ranking_key(
    generation: u64,
    balance_zat: u64,
    address_script_hash: TransparentAddressScriptHash,
) -> [u8; RANKING_KEY_LEN] {
    let mut key = [0u8; RANKING_KEY_LEN];
    key[..GENERATION_LEN].copy_from_slice(&generation.to_be_bytes());
    key[GENERATION_LEN..GENERATION_LEN + 8]
        .copy_from_slice(&(u64::MAX - balance_zat).to_be_bytes());
    key[GENERATION_LEN + 8..].copy_from_slice(&encode_address_script_hash(address_script_hash));
    key
}

fn generation_range(generation: u64) -> ([u8; RANKING_KEY_LEN], [u8; RANKING_KEY_LEN]) {
    let mut start = [0u8; RANKING_KEY_LEN];
    let mut end = [0xff; RANKING_KEY_LEN];
    start[..GENERATION_LEN].copy_from_slice(&generation.to_be_bytes());
    end[..GENERATION_LEN].copy_from_slice(&generation.to_be_bytes());
    (start, end)
}

fn decode_ranking_key(
    key: &[u8],
) -> Result<(u64, u64, TransparentAddressScriptHash), TransparentAddressRankingConsumerError> {
    if key.len() != RANKING_KEY_LEN {
        return Err(TransparentAddressRankingConsumerError::MalformedRankingKey);
    }
    let generation = u64::from_be_bytes(
        key[..GENERATION_LEN]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedRankingKey)?,
    );
    let inverted_balance = u64::from_be_bytes(
        key[GENERATION_LEN..GENERATION_LEN + 8]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedRankingKey)?,
    );
    let address_script_hash = TransparentAddressScriptHash::from_bytes(
        key[GENERATION_LEN + 8..]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedRankingKey)?,
    );
    Ok((generation, u64::MAX - inverted_balance, address_script_hash))
}

fn stage_ranking_delete(
    batch: &mut WriteBatch,
    ranking_cf: &std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>>,
    generation: u64,
    address_script_hash: TransparentAddressScriptHash,
    summary: &TransparentAddressSummary,
) {
    if is_ranked_summary(summary) {
        batch.delete_cf(
            ranking_cf,
            ranking_key(generation, summary.balance_zat, address_script_hash),
        );
    }
}

fn stage_ranking_put(
    batch: &mut WriteBatch,
    ranking_cf: &std::sync::Arc<rust_rocksdb::BoundColumnFamily<'_>>,
    generation: u64,
    address_script_hash: TransparentAddressScriptHash,
    summary: &TransparentAddressSummary,
) -> Result<(), TransparentAddressRankingConsumerError> {
    if is_ranked_summary(summary) {
        let encoded_summary = encode_summary(summary)?;
        batch.put_cf(
            ranking_cf,
            ranking_key(generation, summary.balance_zat, address_script_hash),
            encoded_summary.as_slice(),
        );
    }
    Ok(())
}

fn top_balance_sums_for_generation(
    store: &MaterializedViewStore,
    generation: u64,
) -> Result<(u64, u64), TransparentAddressRankingConsumerError> {
    let (start, end) = generation_range(generation);
    let rows = store.range_iterate_consumer(
        TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY,
        &start,
        &end,
        TOP_ONE_HUNDRED,
    )?;
    top_balance_sums(rows.into_iter().map(|(key, _)| key))
}

fn top_balance_sums_with_overlay(
    store: &MaterializedViewStore,
    generation: u64,
    pending_summaries: &HashMap<TransparentAddressScriptHash, Option<TransparentAddressSummary>>,
) -> Result<(u64, u64), TransparentAddressRankingConsumerError> {
    let cap = TOP_ONE_HUNDRED
        .checked_add(pending_summaries.len())
        .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
    let (start, end) = generation_range(generation);
    let persisted = store.range_iterate_consumer(
        TRANSPARENT_ADDRESS_RANKING_INDEX_COLUMN_FAMILY,
        &start,
        &end,
        cap,
    )?;
    let mut keys = Vec::with_capacity(cap);
    for (key, _) in persisted {
        let (_, _, address_script_hash) = decode_ranking_key(&key)?;
        if !pending_summaries.contains_key(&address_script_hash) {
            keys.push(key);
        }
    }
    for (address_script_hash, summary) in pending_summaries {
        if let Some(summary) = summary
            .as_ref()
            .filter(|summary| is_ranked_summary(summary))
        {
            keys.push(ranking_key(generation, summary.balance_zat, *address_script_hash).to_vec());
        }
    }
    keys.sort_unstable();
    top_balance_sums(keys.into_iter().take(TOP_ONE_HUNDRED))
}

fn top_balance_sums(
    keys: impl IntoIterator<Item = Vec<u8>>,
) -> Result<(u64, u64), TransparentAddressRankingConsumerError> {
    let mut top_ten_sum = 0u64;
    let mut top_hundred_sum = 0u64;
    for (index, key) in keys.into_iter().enumerate() {
        let (_, balance_zat, _) = decode_ranking_key(&key)?;
        top_hundred_sum = top_hundred_sum
            .checked_add(balance_zat)
            .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
        if index < TOP_TEN {
            top_ten_sum = top_ten_sum
                .checked_add(balance_zat)
                .ok_or(TransparentAddressRankingConsumerError::ArithmeticOverflow)?;
        }
    }
    Ok((top_ten_sum, top_hundred_sum))
}

fn encode_summary(
    summary: &TransparentAddressSummary,
) -> Result<Vec<u8>, TransparentAddressRankingConsumerError> {
    let script_len = summary.script_pub_key.as_ref().map_or(Ok(0u32), |script| {
        u32::try_from(script.len())
            .map_err(|_| TransparentAddressRankingConsumerError::SummaryTooLarge)
    })?;
    let mut flags = 0u8;
    flags |= u8::from(summary.script_pub_key.is_some());
    flags |= u8::from(summary.first_seen_unix_seconds.is_some()) << 1;
    flags |= u8::from(summary.last_seen_unix_seconds.is_some()) << 2;
    flags |= u8::from(summary.snapshot_first_seen_unix_seconds.is_some()) << 3;
    flags |= u8::from(summary.snapshot_last_seen_unix_seconds.is_some()) << 4;
    let capacity = SUMMARY_FIXED_LEN
        .checked_add(
            usize::try_from(script_len)
                .map_err(|_| TransparentAddressRankingConsumerError::SummaryTooLarge)?,
        )
        .ok_or(TransparentAddressRankingConsumerError::SummaryTooLarge)?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.push(FORMAT_VERSION);
    bytes.push(flags);
    for statistic in [
        summary.balance_zat,
        summary.total_received_zat,
        summary.total_sent_zat,
        summary.distinct_transaction_count,
    ] {
        bytes.extend_from_slice(&statistic.to_be_bytes());
    }
    for timestamp in [
        summary.first_seen_unix_seconds,
        summary.last_seen_unix_seconds,
        summary.snapshot_first_seen_unix_seconds,
        summary.snapshot_last_seen_unix_seconds,
    ] {
        bytes.extend_from_slice(&timestamp.unwrap_or_default().to_be_bytes());
    }
    bytes.extend_from_slice(&script_len.to_be_bytes());
    if let Some(script) = &summary.script_pub_key {
        bytes.extend_from_slice(script);
    }
    Ok(bytes)
}

fn decode_summary(
    bytes: &[u8],
) -> Result<TransparentAddressSummary, TransparentAddressRankingConsumerError> {
    if bytes.len() < SUMMARY_FIXED_LEN || bytes[0] != FORMAT_VERSION || bytes[1] & !0x1f != 0 {
        return Err(TransparentAddressRankingConsumerError::MalformedSummary);
    }
    let flags = bytes[1];
    let mut offset = 2;
    let balance_zat = take_u64(bytes, &mut offset)?;
    let total_received_zat = take_u64(bytes, &mut offset)?;
    let total_sent_zat = take_u64(bytes, &mut offset)?;
    let distinct_transaction_count = take_u64(bytes, &mut offset)?;
    let first_seen = take_i64(bytes, &mut offset)?;
    let last_seen = take_i64(bytes, &mut offset)?;
    let snapshot_first_seen = take_i64(bytes, &mut offset)?;
    let snapshot_last_seen = take_i64(bytes, &mut offset)?;
    let script_len = usize::try_from(take_u32(bytes, &mut offset)?)
        .map_err(|_| TransparentAddressRankingConsumerError::MalformedSummary)?;
    if bytes.len() != offset.saturating_add(script_len) {
        return Err(TransparentAddressRankingConsumerError::MalformedSummary);
    }
    let script_pub_key = if flags & 1 != 0 {
        Some(bytes[offset..].to_vec())
    } else if script_len == 0 {
        None
    } else {
        return Err(TransparentAddressRankingConsumerError::MalformedSummary);
    };
    Ok(TransparentAddressSummary {
        script_pub_key,
        balance_zat,
        total_received_zat,
        total_sent_zat,
        distinct_transaction_count,
        first_seen_unix_seconds: optional_i64(flags, 1, first_seen),
        last_seen_unix_seconds: optional_i64(flags, 2, last_seen),
        snapshot_first_seen_unix_seconds: optional_i64(flags, 3, snapshot_first_seen),
        snapshot_last_seen_unix_seconds: optional_i64(flags, 4, snapshot_last_seen),
    })
}

fn encode_metadata(metadata: TransparentAddressRankingMetadata) -> [u8; METADATA_LEN] {
    let mut bytes = [0u8; METADATA_LEN];
    let mut offset = 0;
    bytes[offset] = FORMAT_VERSION;
    offset += 1;
    for statistic in [
        metadata.generation,
        metadata.positive_address_count,
        metadata.total_positive_balance_zat,
        metadata.top_10_balance_zat,
        metadata.top_100_balance_zat,
        metadata.p2pkh.positive_address_count,
        metadata.p2pkh.total_positive_balance_zat,
        metadata.p2sh.positive_address_count,
        metadata.p2sh.total_positive_balance_zat,
    ] {
        bytes[offset..offset + 8].copy_from_slice(&statistic.to_be_bytes());
        offset += 8;
    }
    bytes[offset..offset + 4].copy_from_slice(
        &metadata
            .coverage
            .balance_complete_through_height
            .value()
            .to_be_bytes(),
    );
    offset += 4;
    let history_present = metadata.coverage.history_complete_from_height.is_some()
        && metadata.coverage.history_complete_through_height.is_some();
    bytes[offset] = u8::from(history_present);
    offset += 1;
    bytes[offset..offset + 4].copy_from_slice(
        &metadata
            .coverage
            .history_complete_from_height
            .map_or(0, BlockHeight::value)
            .to_be_bytes(),
    );
    offset += 4;
    bytes[offset..offset + 4].copy_from_slice(
        &metadata
            .coverage
            .history_complete_through_height
            .map_or(0, BlockHeight::value)
            .to_be_bytes(),
    );
    offset += 4;
    bytes[offset] = u8::from(metadata.coverage.lifetime_statistics_complete);
    bytes
}

fn decode_metadata(
    bytes: &[u8],
) -> Result<TransparentAddressRankingMetadata, TransparentAddressRankingConsumerError> {
    if bytes.len() != METADATA_LEN || bytes[0] != FORMAT_VERSION {
        return Err(TransparentAddressRankingConsumerError::MalformedMetadata);
    }
    let mut offset = 1;
    let generation = take_u64(bytes, &mut offset)?;
    let positive_address_count = take_u64(bytes, &mut offset)?;
    let total_positive_balance_zat = take_u64(bytes, &mut offset)?;
    let top_ten_sum = take_u64(bytes, &mut offset)?;
    let top_hundred_sum = take_u64(bytes, &mut offset)?;
    let p2pkh = TransparentAddressScriptTypeTotals {
        positive_address_count: take_u64(bytes, &mut offset)?,
        total_positive_balance_zat: take_u64(bytes, &mut offset)?,
    };
    let p2sh = TransparentAddressScriptTypeTotals {
        positive_address_count: take_u64(bytes, &mut offset)?,
        total_positive_balance_zat: take_u64(bytes, &mut offset)?,
    };
    let balance_complete_through_height = BlockHeight::new(take_u32(bytes, &mut offset)?);
    let history_present = take_u8(bytes, &mut offset)?;
    if history_present > 1 {
        return Err(TransparentAddressRankingConsumerError::MalformedMetadata);
    }
    let history_from = BlockHeight::new(take_u32(bytes, &mut offset)?);
    let history_through = BlockHeight::new(take_u32(bytes, &mut offset)?);
    let lifetime_statistics_complete = take_u8(bytes, &mut offset)?;
    if lifetime_statistics_complete > 1 {
        return Err(TransparentAddressRankingConsumerError::MalformedMetadata);
    }
    let coverage = TransparentAddressRankingCoverage {
        balance_complete_through_height,
        history_complete_from_height: (history_present == 1).then_some(history_from),
        history_complete_through_height: (history_present == 1).then_some(history_through),
        lifetime_statistics_complete: lifetime_statistics_complete == 1,
    };
    validate_coverage(coverage)?;
    let metadata = TransparentAddressRankingMetadata {
        generation,
        positive_address_count,
        total_positive_balance_zat,
        top_10_balance_zat: top_ten_sum,
        top_100_balance_zat: top_hundred_sum,
        p2pkh,
        p2sh,
        coverage,
    };
    validate_metadata(metadata)?;
    Ok(metadata)
}

fn read_metadata(
    store: &MaterializedViewStore,
    key: &[u8],
) -> Result<Option<TransparentAddressRankingMetadata>, MaterializedViewStoreError> {
    store
        .get_consumer(TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY, key)?
        .map(|payload| decode_metadata(&payload).map_err(store_decode_error))
        .transpose()
}

fn encode_snapshot_build_manifest(
    manifest: SnapshotBuildManifest,
) -> [u8; SNAPSHOT_BUILD_MANIFEST_LEN] {
    let mut bytes = [0u8; SNAPSHOT_BUILD_MANIFEST_LEN];
    let mut offset = 0;
    bytes[offset] = FORMAT_VERSION;
    offset += 1;
    bytes[offset..offset + 8].copy_from_slice(&manifest.plan.generation.to_be_bytes());
    offset += 8;
    bytes[offset..offset + 4].copy_from_slice(&manifest.plan.base_height.value().to_be_bytes());
    offset += 4;
    bytes[offset..offset + 32].copy_from_slice(&manifest.plan.base_block_hash.as_bytes());
    offset += 32;
    let history_present = manifest
        .plan
        .base_coverage
        .history_complete_from_height
        .is_some()
        && manifest
            .plan
            .base_coverage
            .history_complete_through_height
            .is_some();
    bytes[offset] = u8::from(history_present);
    offset += 1;
    bytes[offset..offset + 4].copy_from_slice(
        &manifest
            .plan
            .base_coverage
            .history_complete_from_height
            .map_or(0, BlockHeight::value)
            .to_be_bytes(),
    );
    offset += 4;
    bytes[offset..offset + 4].copy_from_slice(
        &manifest
            .plan
            .base_coverage
            .history_complete_through_height
            .map_or(0, BlockHeight::value)
            .to_be_bytes(),
    );
    offset += 4;
    bytes[offset] = u8::from(manifest.plan.base_coverage.lifetime_statistics_complete);
    offset += 1;
    bytes[offset..offset + 4].copy_from_slice(&manifest.plan.target_height.value().to_be_bytes());
    offset += 4;
    bytes[offset..offset + 32].copy_from_slice(&manifest.plan.target_block_hash.as_bytes());
    offset += 32;
    bytes[offset..offset + 8].copy_from_slice(&manifest.plan.expected_summary_count.to_be_bytes());
    offset += 8;
    bytes[offset..offset + 8].copy_from_slice(&manifest.written_summary_count.to_be_bytes());
    offset += 8;
    bytes[offset] = u8::from(manifest.base_rows_complete);
    bytes
}

fn decode_snapshot_build_manifest(
    bytes: &[u8],
) -> Result<SnapshotBuildManifest, TransparentAddressRankingConsumerError> {
    if bytes.len() != SNAPSHOT_BUILD_MANIFEST_LEN || bytes[0] != FORMAT_VERSION {
        return Err(TransparentAddressRankingConsumerError::MalformedSnapshotBuildManifest);
    }
    let mut offset = 1;
    let generation = take_u64(bytes, &mut offset)?;
    let base_height = BlockHeight::new(take_u32(bytes, &mut offset)?);
    let base_hash_end = offset + 32;
    let base_block_hash = BlockHash::from_bytes(
        bytes[offset..base_hash_end]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedSnapshotBuildManifest)?,
    );
    offset = base_hash_end;
    let history_present = take_u8(bytes, &mut offset)?;
    let history_from = BlockHeight::new(take_u32(bytes, &mut offset)?);
    let history_through = BlockHeight::new(take_u32(bytes, &mut offset)?);
    let lifetime_statistics_complete = take_u8(bytes, &mut offset)?;
    if history_present > 1 || lifetime_statistics_complete > 1 {
        return Err(TransparentAddressRankingConsumerError::MalformedSnapshotBuildManifest);
    }
    let base_coverage = TransparentAddressRankingCoverage {
        balance_complete_through_height: base_height,
        history_complete_from_height: (history_present == 1).then_some(history_from),
        history_complete_through_height: (history_present == 1).then_some(history_through),
        lifetime_statistics_complete: lifetime_statistics_complete == 1,
    };
    let target_height = BlockHeight::new(take_u32(bytes, &mut offset)?);
    let target_hash_end = offset + 32;
    let target_block_hash = BlockHash::from_bytes(
        bytes[offset..target_hash_end]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedSnapshotBuildManifest)?,
    );
    offset = target_hash_end;
    let expected_summary_count = take_u64(bytes, &mut offset)?;
    let written_summary_count = take_u64(bytes, &mut offset)?;
    let base_rows_complete = take_u8(bytes, &mut offset)?;
    if base_rows_complete > 1 || offset != bytes.len() {
        return Err(TransparentAddressRankingConsumerError::MalformedSnapshotBuildManifest);
    }
    let plan = TransparentAddressRankingSnapshotPlan {
        generation,
        base_height,
        base_block_hash,
        target_height,
        target_block_hash,
        expected_summary_count,
        base_coverage,
    };
    validate_snapshot_plan(plan)?;
    if written_summary_count > expected_summary_count {
        return Err(TransparentAddressRankingConsumerError::MalformedSnapshotBuildManifest);
    }
    Ok(SnapshotBuildManifest {
        plan,
        written_summary_count,
        base_rows_complete: base_rows_complete == 1,
    })
}

fn read_snapshot_build_manifest(
    store: &MaterializedViewStore,
) -> Result<Option<SnapshotBuildManifest>, MaterializedViewStoreError> {
    let Some(payload) = store.get_consumer(
        TRANSPARENT_ADDRESS_RANKING_METADATA_COLUMN_FAMILY,
        BUILD_MANIFEST_KEY,
    )?
    else {
        return Ok(None);
    };
    decode_snapshot_build_manifest(&payload)
        .map(Some)
        .map_err(store_decode_error)
}

fn encode_undo_journal(
    journal: &UndoJournal,
) -> Result<Vec<u8>, TransparentAddressRankingConsumerError> {
    let entry_count = u32::try_from(journal.summaries_before.len())
        .map_err(|_| TransparentAddressRankingConsumerError::UndoJournalTooLarge)?;
    let mut bytes = Vec::with_capacity(UNDO_HEADER_LEN);
    bytes.push(FORMAT_VERSION);
    bytes.extend_from_slice(&journal.generation.to_be_bytes());
    bytes.extend_from_slice(&journal.block_hash.as_bytes());
    bytes.extend_from_slice(
        &u32::try_from(METADATA_LEN)
            .map_err(|_| TransparentAddressRankingConsumerError::UndoJournalTooLarge)?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&encode_metadata(journal.metadata_before));
    bytes.extend_from_slice(&entry_count.to_be_bytes());
    for (address_script_hash, summary) in &journal.summaries_before {
        bytes.extend_from_slice(&encode_address_script_hash(*address_script_hash));
        match summary {
            Some(summary) => {
                let encoded = encode_summary(summary)?;
                bytes.extend_from_slice(
                    &u32::try_from(encoded.len())
                        .map_err(|_| TransparentAddressRankingConsumerError::UndoJournalTooLarge)?
                        .to_be_bytes(),
                );
                bytes.extend_from_slice(&encoded);
            }
            None => bytes.extend_from_slice(&ABSENT_SUMMARY_LEN.to_be_bytes()),
        }
    }
    Ok(bytes)
}

fn decode_undo_journal(
    bytes: &[u8],
) -> Result<UndoJournal, TransparentAddressRankingConsumerError> {
    if bytes.len() < UNDO_HEADER_LEN || bytes[0] != FORMAT_VERSION {
        return Err(TransparentAddressRankingConsumerError::MalformedUndoJournal);
    }
    let mut offset = 1;
    let generation = take_u64(bytes, &mut offset)?;
    let block_hash_end = offset
        .checked_add(32)
        .filter(|end| *end <= bytes.len())
        .ok_or(TransparentAddressRankingConsumerError::MalformedUndoJournal)?;
    let block_hash = BlockHash::from_bytes(
        bytes[offset..block_hash_end]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedUndoJournal)?,
    );
    offset = block_hash_end;
    let metadata_len = usize::try_from(take_u32(bytes, &mut offset)?)
        .map_err(|_| TransparentAddressRankingConsumerError::MalformedUndoJournal)?;
    let metadata_end = offset
        .checked_add(metadata_len)
        .filter(|end| *end <= bytes.len())
        .ok_or(TransparentAddressRankingConsumerError::MalformedUndoJournal)?;
    let metadata_before = decode_metadata(&bytes[offset..metadata_end])?;
    offset = metadata_end;
    let entry_count = usize::try_from(take_u32(bytes, &mut offset)?)
        .map_err(|_| TransparentAddressRankingConsumerError::MalformedUndoJournal)?;
    let mut summaries_before = Vec::with_capacity(entry_count.min(64));
    for _ in 0..entry_count {
        let address_end = offset
            .checked_add(ADDRESS_HASH_LEN)
            .filter(|end| *end <= bytes.len())
            .ok_or(TransparentAddressRankingConsumerError::MalformedUndoJournal)?;
        let address_script_hash = TransparentAddressScriptHash::from_bytes(
            bytes[offset..address_end]
                .try_into()
                .map_err(|_| TransparentAddressRankingConsumerError::MalformedUndoJournal)?,
        );
        offset = address_end;
        let summary_len = take_u32(bytes, &mut offset)?;
        let summary = if summary_len == ABSENT_SUMMARY_LEN {
            None
        } else {
            let summary_end =
                offset
                    .checked_add(usize::try_from(summary_len).map_err(|_| {
                        TransparentAddressRankingConsumerError::MalformedUndoJournal
                    })?)
                    .filter(|end| *end <= bytes.len())
                    .ok_or(TransparentAddressRankingConsumerError::MalformedUndoJournal)?;
            let summary = decode_summary(&bytes[offset..summary_end])?;
            offset = summary_end;
            Some(summary)
        };
        summaries_before.push((address_script_hash, summary));
    }
    if offset != bytes.len() || metadata_before.generation != generation {
        return Err(TransparentAddressRankingConsumerError::MalformedUndoJournal);
    }
    Ok(UndoJournal {
        generation,
        block_hash,
        metadata_before,
        summaries_before,
    })
}

fn take_u8(bytes: &[u8], offset: &mut usize) -> Result<u8, TransparentAddressRankingConsumerError> {
    let decoded = bytes
        .get(*offset)
        .copied()
        .ok_or(TransparentAddressRankingConsumerError::MalformedEncoding)?;
    *offset = offset
        .checked_add(1)
        .ok_or(TransparentAddressRankingConsumerError::MalformedEncoding)?;
    Ok(decoded)
}

fn take_u32(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u32, TransparentAddressRankingConsumerError> {
    let end = offset
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or(TransparentAddressRankingConsumerError::MalformedEncoding)?;
    let decoded = u32::from_be_bytes(
        bytes[*offset..end]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedEncoding)?,
    );
    *offset = end;
    Ok(decoded)
}

fn take_u64(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<u64, TransparentAddressRankingConsumerError> {
    let end = offset
        .checked_add(8)
        .filter(|end| *end <= bytes.len())
        .ok_or(TransparentAddressRankingConsumerError::MalformedEncoding)?;
    let decoded = u64::from_be_bytes(
        bytes[*offset..end]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedEncoding)?,
    );
    *offset = end;
    Ok(decoded)
}

fn take_i64(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<i64, TransparentAddressRankingConsumerError> {
    let end = offset
        .checked_add(8)
        .filter(|end| *end <= bytes.len())
        .ok_or(TransparentAddressRankingConsumerError::MalformedEncoding)?;
    let decoded = i64::from_be_bytes(
        bytes[*offset..end]
            .try_into()
            .map_err(|_| TransparentAddressRankingConsumerError::MalformedEncoding)?,
    );
    *offset = end;
    Ok(decoded)
}

fn optional_i64(flags: u8, bit: u8, timestamp: i64) -> Option<i64> {
    (flags & (1 << bit) != 0).then_some(timestamp)
}

fn minimum_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(timestamp), None) | (None, Some(timestamp)) => Some(timestamp),
        (None, None) => None,
    }
}

fn maximum_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(timestamp), None) | (None, Some(timestamp)) => Some(timestamp),
        (None, None) => None,
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "the signature is used directly by Result::map_err"
)]
fn store_decode_error(error: impl ToString) -> MaterializedViewStoreError {
    MaterializedViewStoreError::Decode {
        column_family: MaterializedViewStoreColumnFamily::ConsumerMetadata,
        reason: error.to_string(),
    }
}

/// Ranking materialized-view failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransparentAddressRankingConsumerError {
    /// Materialized-view store access failed.
    #[error(transparent)]
    Store(#[from] MaterializedViewStoreError),
    /// No active snapshot generation exists for steady-state application.
    #[error("transparent-address ranking has no active generation")]
    ActiveGenerationMissing,
    /// Snapshot generation zero or a non-increasing generation was requested.
    #[error("transparent-address ranking generation {generation} is invalid")]
    InvalidGeneration {
        /// Rejected generation.
        generation: u64,
    },
    /// Another snapshot generation is already building.
    #[error(
        "transparent-address ranking generation {requested_generation} conflicts with building generation {existing_generation}"
    )]
    SnapshotBuildConflict {
        /// Requested generation.
        requested_generation: u64,
        /// Existing build generation.
        existing_generation: u64,
    },
    /// Snapshot rows were supplied without the matching initialized build.
    #[error("transparent-address ranking generation {generation} is not building")]
    SnapshotBuildMissing {
        /// Requested generation.
        generation: u64,
    },
    /// Snapshot row writes were attempted after the settled base was sealed.
    #[error("transparent-address ranking snapshot base is already complete")]
    SnapshotBaseAlreadyComplete,
    /// Snapshot tail seeding started before the settled base was sealed.
    #[error("transparent-address ranking snapshot base is incomplete")]
    SnapshotBaseIncomplete,
    /// Snapshot base cardinality differs from its pinned plan.
    #[error("transparent-address ranking snapshot expected {expected} rows, wrote {actual}")]
    SnapshotRowCountMismatch {
        /// Exact row count pinned before the scan.
        expected: u64,
        /// Distinct rows durably written.
        actual: u64,
    },
    /// Tail seeding attempted to advance beyond the pinned visible target.
    #[error("transparent-address ranking snapshot tail exceeds its target")]
    SnapshotTailPastTarget,
    /// Tail seeding did not reach the pinned visible target.
    #[error("transparent-address ranking snapshot tail is incomplete")]
    SnapshotTailIncomplete,
    /// The target journal does not prove the pinned visible block identity.
    #[error("transparent-address ranking snapshot target does not match its journal")]
    SnapshotTargetMismatch,
    /// Cursor-neutral tail application failed.
    #[error("transparent-address ranking snapshot tail failed: {0}")]
    SnapshotTail(String),
    /// An already active generation does not own the requested event cursor.
    #[error("transparent-address ranking active generation cursor does not match startup boundary")]
    ActiveCursorMismatch,
    /// One snapshot batch repeated an address hash.
    #[error("transparent-address ranking snapshot batch contains a duplicate address")]
    DuplicateSnapshotAddress,
    /// Snapshot coverage fields are inconsistent.
    #[error("transparent-address ranking coverage is inconsistent")]
    InvalidCoverage,
    /// Snapshot plan boundaries disagree with its base coverage.
    #[error("transparent-address ranking snapshot plan is inconsistent")]
    InvalidSnapshotPlan,
    /// A raw script did not hash to the row's address script hash.
    #[error("transparent-address ranking script does not match its script hash")]
    ScriptHashMismatch,
    /// Two outputs for one script hash supplied different raw scripts.
    #[error("transparent-address ranking observed conflicting scripts for one script hash")]
    ConflictingScript,
    /// Transparent spend hydration was disabled for a required block.
    #[error("transparent spends are unavailable at height {height}")]
    TransparentSpendsUnavailable {
        /// Block height requiring spend facts.
        height: u32,
    },
    /// A non-coinbase transparent input did not resolve.
    #[error("transparent input {input_index} at height {height} did not resolve")]
    TransparentSpendUnresolved {
        /// Block height containing the input.
        height: u32,
        /// Input index within its transaction.
        input_index: u32,
    },
    /// The steady-state tail skipped or repeated a height.
    #[error(
        "transparent-address ranking tail expected height {expected_height}, got {actual_height}"
    )]
    NonContiguousTail {
        /// Next expected height.
        expected_height: u32,
        /// Supplied height.
        actual_height: u32,
    },
    /// An idempotent chunk replay had no persisted journal for its block.
    #[error("transparent-address ranking applied-block journal is missing at height {height}")]
    AppliedBlockJournalMissing {
        /// Height replayed after state had already advanced.
        height: u32,
    },
    /// An idempotent chunk replay supplied a different block at one height.
    #[error("transparent-address ranking applied block differs at height {height}")]
    AppliedBlockMismatch {
        /// Height whose canonical identity changed unexpectedly.
        height: u32,
    },
    /// Height successor could not be represented.
    #[error("transparent-address ranking coverage height overflowed")]
    CoverageOverflow,
    /// A checked value, count, offset, or aggregate operation failed.
    #[error("transparent-address ranking arithmetic overflowed or underflowed")]
    ArithmeticOverflow,
    /// Offset or rank arithmetic exceeded the platform or wire width.
    #[error("transparent-address ranking page bounds are invalid")]
    PageBounds,
    /// An apply record had no corresponding per-height undo journal.
    #[error("transparent-address ranking undo journal is missing at height {height}")]
    UndoJournalMissing {
        /// Height whose journal was required.
        height: u32,
    },
    /// An undo journal belongs to an inactive generation.
    #[error(
        "transparent-address ranking undo generation {journal_generation} at height {height} does not match active generation {active_generation}"
    )]
    UndoGenerationMismatch {
        /// Journal height.
        height: u32,
        /// Current active generation.
        active_generation: u64,
        /// Journal generation.
        journal_generation: u64,
    },
    /// Summary payload exceeded its length encoding.
    #[error("transparent-address ranking summary is too large")]
    SummaryTooLarge,
    /// Undo payload exceeded its length encoding.
    #[error("transparent-address ranking undo journal is too large")]
    UndoJournalTooLarge,
    /// A summary payload failed structural validation.
    #[error("transparent-address ranking summary payload is malformed")]
    MalformedSummary,
    /// A metadata payload failed structural validation.
    #[error("transparent-address ranking metadata payload is malformed")]
    MalformedMetadata,
    /// Snapshot-build manifest failed structural validation.
    #[error("transparent-address ranking snapshot-build manifest is malformed")]
    MalformedSnapshotBuildManifest,
    /// A ranking key failed structural validation.
    #[error("transparent-address ranking key is malformed")]
    MalformedRankingKey,
    /// A ranking row's key and summary disagree.
    #[error("transparent-address ranking key and summary disagree")]
    RankingSummaryMismatch,
    /// An undo payload failed structural validation.
    #[error("transparent-address ranking undo journal is malformed")]
    MalformedUndoJournal,
    /// A primitive decoder reached invalid or truncated bytes.
    #[error("transparent-address ranking encoding is malformed")]
    MalformedEncoding,
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error, sync::Arc};

    use super::{
        AddressBlockDelta, TRANSPARENT_ADDRESS_RANKING_SCHEMA, TransparentAddressRankingConsumer,
        TransparentAddressRankingCoverage, TransparentAddressRankingSnapshotPlan,
        TransparentAddressRankingSnapshotRow, TransparentAddressScriptTypeTotals,
        TransparentAddressSummary, apply_delta, decode_summary, encode_summary, ranking_key,
    };
    use crate::consumer::{
        BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, MaterializedViewConsumerCtx,
        TransparentSpendFacts,
    };
    use crate::{MaterializedViewStore, MaterializedViewStoreOptions};
    use rust_rocksdb::WriteBatch;
    use tempfile::tempdir;
    use zinder_core::{
        BlockHash, BlockHeight, LockTime, PrivacyShape, TransactionComponentCounts,
        TransactionFactsArtifact, TransactionId, TransactionLocation, TransactionPublicFacts,
        TransactionVersion, TransparentAddressScriptHash, TransparentInputFact,
        TransparentOutPoint, TransparentOutputFact,
    };
    use zinder_store::RocksDbResourceBudget;

    type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

    fn p2pkh(seed: u8) -> Vec<u8> {
        let mut script = vec![0x76, 0xa9, 0x14];
        script.extend_from_slice(&[seed; 20]);
        script.extend_from_slice(&[0x88, 0xac]);
        script
    }

    fn p2sh(seed: u8) -> Vec<u8> {
        let mut script = vec![0xa9, 0x14];
        script.extend_from_slice(&[seed; 20]);
        script.push(0x87);
        script
    }

    fn summary(script_pub_key: Option<Vec<u8>>, balance_zat: u64) -> TransparentAddressSummary {
        TransparentAddressSummary {
            script_pub_key,
            balance_zat,
            total_received_zat: balance_zat,
            total_sent_zat: 0,
            distinct_transaction_count: 1,
            first_seen_unix_seconds: Some(100),
            last_seen_unix_seconds: Some(100),
            snapshot_first_seen_unix_seconds: Some(90),
            snapshot_last_seen_unix_seconds: Some(110),
        }
    }

    fn coverage(height: u32) -> TransparentAddressRankingCoverage {
        TransparentAddressRankingCoverage {
            balance_complete_through_height: BlockHeight::new(height),
            history_complete_from_height: Some(BlockHeight::new(1)),
            history_complete_through_height: Some(BlockHeight::new(height)),
            lifetime_statistics_complete: true,
        }
    }

    fn open_store() -> TestResult<(tempfile::TempDir, MaterializedViewStore)> {
        let tempdir = tempdir()?;
        let store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[TRANSPARENT_ADDRESS_RANKING_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
            },
        )?;
        Ok((tempdir, store))
    }

    fn activate(
        store: &MaterializedViewStore,
        generation: u64,
    ) -> Result<
        super::TransparentAddressRankingMetadata,
        super::TransparentAddressRankingConsumerError,
    > {
        if TransparentAddressRankingConsumer::build_metadata(store)?.is_some() {
            TransparentAddressRankingConsumer::finalize_snapshot_base(store, generation)?;
        }
        TransparentAddressRankingConsumer::activate_snapshot_generation_at_cursor(
            store,
            generation,
            &[9, 8, 7],
        )
    }

    fn initialize(
        store: &MaterializedViewStore,
        generation: u64,
        height: u32,
        expected_summary_count: u64,
    ) -> Result<(), super::TransparentAddressRankingConsumerError> {
        let block_hash = BlockHash::from_bytes([height.to_le_bytes()[0]; 32]);
        TransparentAddressRankingConsumer::initialize_snapshot_generation(
            store,
            TransparentAddressRankingSnapshotPlan {
                generation,
                base_height: BlockHeight::new(height),
                base_block_hash: block_hash,
                target_height: BlockHeight::new(height),
                target_block_hash: block_hash,
                expected_summary_count,
                base_coverage: coverage(height),
            },
        )
    }

    fn transaction_id(seed: u8) -> TransactionId {
        TransactionId::from_bytes([seed; 32])
    }

    fn public_facts(seed: u8, is_coinbase: bool) -> TransactionPublicFacts {
        TransactionPublicFacts {
            transaction_id: transaction_id(seed),
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::V5,
            consensus_branch_id: None,
            lock_time: LockTime::Unlocked,
            expiry_height: None,
            size_bytes: 0,
            counts: TransactionComponentCounts::EMPTY,
            privacy_shape: PrivacyShape::Unclassified,
            is_coinbase,
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            unsupported_sections: Vec::new(),
        }
    }

    fn receive_block(height: u32, script_pub_key: Vec<u8>, value_zat: u64) -> BlockCommitContext {
        let block_height = BlockHeight::new(height);
        let block_hash = BlockHash::from_bytes([height.to_le_bytes()[0]; 32]);
        let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script_pub_key);
        let location = TransactionLocation::new(
            transaction_id(height.to_le_bytes()[0]),
            block_height,
            block_hash,
            0,
        );
        let transaction =
            TransactionFactsArtifact::new(location, public_facts(height.to_le_bytes()[0], true))
                .with_transparent_facts(
                    Vec::new(),
                    vec![TransparentOutputFact::new(
                        0,
                        value_zat,
                        script_pub_key,
                        address_script_hash,
                    )],
                );
        BlockCommitContext::new(
            BlockCommitInput {
                height: block_height,
                block_hash,
                previous_block_hash: BlockHash::from_bytes(
                    [height.saturating_sub(1).to_le_bytes()[0]; 32],
                ),
                block_time_unix_seconds: 1_700_000_000 + i64::from(height),
                block_size_bytes: 0,
                transactions: vec![transaction],
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::Offline,
        )
    }

    fn unresolved_spend_block(height: u32) -> BlockCommitContext {
        let block_height = BlockHeight::new(height);
        let block_hash = BlockHash::from_bytes([height.to_le_bytes()[0]; 32]);
        let location = TransactionLocation::new(transaction_id(9), block_height, block_hash, 0);
        let transaction = TransactionFactsArtifact::new(location, public_facts(9, false))
            .with_transparent_facts(
                vec![TransparentInputFact::new(
                    0,
                    TransparentOutPoint::new(transaction_id(8), 0),
                )],
                Vec::new(),
            );
        BlockCommitContext::new(
            BlockCommitInput {
                height: block_height,
                block_hash,
                previous_block_hash: BlockHash::from_bytes([0; 32]),
                block_time_unix_seconds: 1_700_000_000 + i64::from(height),
                block_size_bytes: 0,
                transactions: vec![transaction],
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::from_map(Arc::new(HashMap::new())),
        )
    }

    fn apply_and_commit(
        store: &MaterializedViewStore,
        consumer: &mut TransparentAddressRankingConsumer,
        block: &BlockCommitContext,
    ) -> TestResult {
        let mut batch = WriteBatch::default();
        let mut context = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut context)?;
        consumer.apply_block(block, &mut context)?;
        consumer.finish_batch(&mut context)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    #[test]
    fn standard_script_predicate_accepts_only_exact_templates() {
        assert!(TransparentAddressRankingConsumer::is_standard_transparent_script(&p2pkh(1)));
        assert!(TransparentAddressRankingConsumer::is_standard_transparent_script(&p2sh(2)));
        let mut p2pkh_with_trailing_byte = p2pkh(1);
        p2pkh_with_trailing_byte.push(0);
        assert!(
            !TransparentAddressRankingConsumer::is_standard_transparent_script(
                &p2pkh_with_trailing_byte
            )
        );
        assert!(
            !TransparentAddressRankingConsumer::is_standard_transparent_script(&[0x6a, 0x01, 0x01])
        );
    }

    #[test]
    fn ranking_key_orders_balance_descending_then_hash_ascending() {
        let low_hash = TransparentAddressScriptHash::from_bytes([1; 32]);
        let high_hash = TransparentAddressScriptHash::from_bytes([2; 32]);
        assert!(ranking_key(7, 20, high_hash) < ranking_key(7, 10, low_hash));
        assert!(ranking_key(7, 20, low_hash) < ranking_key(7, 20, high_hash));
        assert!(ranking_key(7, 20, low_hash) < ranking_key(8, 20, low_hash));
    }

    #[test]
    fn summary_encoding_round_trips_optional_extrema_and_script() -> TestResult {
        let expected = summary(Some(p2pkh(4)), 50);
        assert_eq!(decode_summary(&encode_summary(&expected)?)?, expected);
        let expected_without_options = TransparentAddressSummary {
            script_pub_key: None,
            balance_zat: 0,
            total_received_zat: 0,
            total_sent_zat: 0,
            distinct_transaction_count: 0,
            first_seen_unix_seconds: None,
            last_seen_unix_seconds: None,
            snapshot_first_seen_unix_seconds: None,
            snapshot_last_seen_unix_seconds: None,
        };
        assert_eq!(
            decode_summary(&encode_summary(&expected_without_options)?)?,
            expected_without_options
        );
        Ok(())
    }

    #[test]
    fn delta_uses_checked_balance_and_snapshot_extrema_fallback() -> TestResult {
        let mut before = summary(Some(p2pkh(3)), 10);
        before.first_seen_unix_seconds = None;
        before.last_seen_unix_seconds = None;
        let mut delta = AddressBlockDelta {
            received_zat: 7,
            sent_zat: 4,
            ..AddressBlockDelta::default()
        };
        delta.transaction_positions.extend([1, 2]);
        let after = apply_delta(Some(&before), &delta, 105)?;
        assert_eq!(after.balance_zat, 13);
        assert_eq!(after.total_received_zat, 17);
        assert_eq!(after.total_sent_zat, 4);
        assert_eq!(after.distinct_transaction_count, 3);
        assert_eq!(after.first_seen_unix_seconds, Some(90));
        assert_eq!(after.last_seen_unix_seconds, Some(110));
        Ok(())
    }

    #[test]
    fn delta_rejects_balance_underflow() {
        let delta = AddressBlockDelta {
            sent_zat: 1,
            ..AddressBlockDelta::default()
        };
        assert!(apply_delta(None, &delta, 100).is_err());
    }

    #[test]
    fn snapshot_batches_are_idempotent_and_activation_is_atomic() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let script = p2pkh(1);
        let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
        let row = TransparentAddressRankingSnapshotRow {
            address_script_hash,
            summary: summary(Some(script), 70),
        };
        initialize(&store, 1, 10, 1)?;
        TransparentAddressRankingConsumer::write_snapshot_batch(
            &store,
            1,
            std::slice::from_ref(&row),
        )?;
        initialize(&store, 1, 10, 1)?;
        TransparentAddressRankingConsumer::write_snapshot_batch(&store, 1, &[row])?;
        assert!(TransparentAddressRankingConsumer::active_metadata(&store)?.is_none());
        let activated = activate(&store, 1)?;
        assert_eq!(activated.positive_address_count, 1);
        assert_eq!(activated.total_positive_balance_zat, 70);
        assert_eq!(activated.p2pkh.positive_address_count, 1);
        assert_eq!(activated.p2pkh.total_positive_balance_zat, 70);
        assert_eq!(
            activated.p2sh,
            TransparentAddressScriptTypeTotals::default()
        );
        assert_eq!(activated.top_10_balance_zat, 70);
        assert!(TransparentAddressRankingConsumer::build_metadata(&store)?.is_none());
        assert_eq!(activate(&store, 1)?, activated);
        Ok(())
    }

    #[test]
    fn page_excludes_zero_and_nonstandard_rows_and_preserves_order() -> TestResult {
        let (_tempdir, store) = open_store()?;
        initialize(&store, 2, 20, 4)?;
        let scripts = [p2pkh(1), p2sh(2), vec![0x6a, 0x00], p2pkh(4)];
        let balances = [10, 30, 100, 0];
        let rows = scripts
            .into_iter()
            .zip(balances)
            .map(
                |(script, balance_zat)| TransparentAddressRankingSnapshotRow {
                    address_script_hash: TransparentAddressScriptHash::of_script_pub_key(&script),
                    summary: summary(Some(script), balance_zat),
                },
            )
            .collect::<Vec<_>>();
        TransparentAddressRankingConsumer::write_snapshot_batch(&store, 2, &rows)?;
        activate(&store, 2)?;
        let page =
            TransparentAddressRankingConsumer::page(&store, 1, 10)?.ok_or("active page missing")?;
        assert_eq!(page.metadata.positive_address_count, 2);
        assert_eq!(page.metadata.total_positive_balance_zat, 40);
        assert_eq!(page.metadata.p2pkh.positive_address_count, 1);
        assert_eq!(page.metadata.p2pkh.total_positive_balance_zat, 10);
        assert_eq!(page.metadata.p2sh.positive_address_count, 1);
        assert_eq!(page.metadata.p2sh.total_positive_balance_zat, 30);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].rank, 2);
        assert_eq!(page.entries[0].summary.balance_zat, 10);
        Ok(())
    }

    #[test]
    fn replacing_snapshot_row_removes_old_ranking_key_and_aggregate() -> TestResult {
        let (_tempdir, store) = open_store()?;
        initialize(&store, 3, 30, 1)?;
        let script = p2pkh(7);
        let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
        let row = |balance_zat| TransparentAddressRankingSnapshotRow {
            address_script_hash,
            summary: summary(Some(script.clone()), balance_zat),
        };
        TransparentAddressRankingConsumer::write_snapshot_batch(&store, 3, &[row(80)])?;
        TransparentAddressRankingConsumer::write_snapshot_batch(&store, 3, &[row(25)])?;
        let metadata = activate(&store, 3)?;
        assert_eq!(metadata.positive_address_count, 1);
        assert_eq!(metadata.total_positive_balance_zat, 25);
        let page =
            TransparentAddressRankingConsumer::page(&store, 0, 10)?.ok_or("active page missing")?;
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].summary.balance_zat, 25);
        Ok(())
    }

    #[test]
    fn steady_state_apply_updates_summary_ranking_statistics_and_coverage() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let script = p2pkh(8);
        let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
        initialize(&store, 4, 9, 1)?;
        TransparentAddressRankingConsumer::write_snapshot_batch(
            &store,
            4,
            &[TransparentAddressRankingSnapshotRow {
                address_script_hash,
                summary: summary(Some(script.clone()), 100),
            }],
        )?;
        activate(&store, 4)?;
        let mut consumer = TransparentAddressRankingConsumer::new();
        apply_and_commit(&store, &mut consumer, &receive_block(10, script, 20))?;

        let updated = TransparentAddressRankingConsumer::summary(&store, address_script_hash)?
            .ok_or("updated summary missing")?;
        assert_eq!(updated.balance_zat, 120);
        assert_eq!(updated.total_received_zat, 120);
        assert_eq!(updated.distinct_transaction_count, 2);
        let metadata = TransparentAddressRankingConsumer::active_metadata(&store)?
            .ok_or("active metadata missing")?;
        assert_eq!(metadata.total_positive_balance_zat, 120);
        assert_eq!(metadata.p2pkh.positive_address_count, 1);
        assert_eq!(metadata.p2pkh.total_positive_balance_zat, 120);
        assert_eq!(metadata.top_10_balance_zat, 120);
        assert_eq!(
            metadata.coverage.balance_complete_through_height,
            BlockHeight::new(10)
        );
        assert_eq!(
            metadata.coverage.history_complete_through_height,
            Some(BlockHeight::new(10))
        );
        Ok(())
    }

    #[test]
    fn interrupted_snapshot_cannot_activate_before_exact_row_count() -> TestResult {
        let (_tempdir, store) = open_store()?;
        initialize(&store, 7, 9, 2)?;
        let script = p2pkh(7);
        TransparentAddressRankingConsumer::write_snapshot_batch(
            &store,
            7,
            &[TransparentAddressRankingSnapshotRow {
                address_script_hash: TransparentAddressScriptHash::of_script_pub_key(&script),
                summary: summary(Some(script), 10),
            }],
        )?;

        assert!(matches!(
            TransparentAddressRankingConsumer::finalize_snapshot_base(&store, 7),
            Err(
                super::TransparentAddressRankingConsumerError::SnapshotRowCountMismatch {
                    expected: 2,
                    actual: 1,
                }
            )
        ));
        assert!(TransparentAddressRankingConsumer::active_metadata(&store)?.is_none());
        Ok(())
    }

    #[test]
    fn snapshot_tail_is_cursor_neutral_reorg_ready_and_idempotent() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let script = p2pkh(10);
        let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
        let block = receive_block(10, script.clone(), 20);
        TransparentAddressRankingConsumer::initialize_snapshot_generation(
            &store,
            TransparentAddressRankingSnapshotPlan {
                generation: 8,
                base_height: BlockHeight::new(9),
                base_block_hash: BlockHash::from_bytes([9; 32]),
                target_height: BlockHeight::new(10),
                target_block_hash: block.block_hash,
                expected_summary_count: 1,
                base_coverage: coverage(9),
            },
        )?;
        TransparentAddressRankingConsumer::write_snapshot_batch(
            &store,
            8,
            &[TransparentAddressRankingSnapshotRow {
                address_script_hash,
                summary: summary(Some(script), 100),
            }],
        )?;
        TransparentAddressRankingConsumer::finalize_snapshot_base(&store, 8)?;
        TransparentAddressRankingConsumer::write_snapshot_tail_block(&store, 8, &block)?;
        TransparentAddressRankingConsumer::write_snapshot_tail_block(&store, 8, &block)?;
        assert!(
            store
                .get_chain_event_cursor(super::TRANSPARENT_ADDRESS_RANKING_CONSUMER_NAME)?
                .is_none()
        );
        TransparentAddressRankingConsumer::activate_snapshot_generation_at_cursor(
            &store,
            8,
            &[4, 5, 6],
        )?;
        let mut consumer = TransparentAddressRankingConsumer::new();
        apply_and_commit(&store, &mut consumer, &block)?;
        let unchanged = TransparentAddressRankingConsumer::summary(&store, address_script_hash)?
            .ok_or("summary missing after idempotent replay")?;
        assert_eq!(unchanged.balance_zat, 120);
        assert_eq!(unchanged.total_received_zat, 120);

        let mut batch = WriteBatch::default();
        let mut context = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut context)?;
        consumer.revert_block(BlockHeight::new(10), &mut context)?;
        consumer.finish_batch(&mut context)?;
        store.write_batch(&batch)?;
        let restored = TransparentAddressRankingConsumer::summary(&store, address_script_hash)?
            .ok_or("summary missing after snapshot-tail revert")?;
        assert_eq!(restored.balance_zat, 100);
        Ok(())
    }

    #[test]
    fn page_and_complete_coverage_are_bounded() -> TestResult {
        let (_tempdir, store) = open_store()?;
        initialize(&store, 9, 9, 0)?;
        activate(&store, 9)?;
        assert!(
            TransparentAddressRankingConsumer::page(
                &store,
                0,
                super::TRANSPARENT_ADDRESS_RANKING_MAX_PAGE_SIZE + 1,
            )
            .is_err()
        );

        let invalid = TransparentAddressRankingSnapshotPlan {
            generation: 10,
            base_height: BlockHeight::new(9),
            base_block_hash: BlockHash::from_bytes([9; 32]),
            target_height: BlockHeight::new(9),
            target_block_hash: BlockHash::from_bytes([9; 32]),
            expected_summary_count: 0,
            base_coverage: TransparentAddressRankingCoverage {
                balance_complete_through_height: BlockHeight::new(9),
                history_complete_from_height: Some(BlockHeight::new(2)),
                history_complete_through_height: Some(BlockHeight::new(9)),
                lifetime_statistics_complete: true,
            },
        };
        assert!(matches!(
            TransparentAddressRankingConsumer::initialize_snapshot_generation(&store, invalid),
            Err(super::TransparentAddressRankingConsumerError::InvalidCoverage)
        ));
        Ok(())
    }

    #[test]
    fn ascending_multi_height_revert_restores_earliest_before_image() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let script = p2pkh(9);
        let address_script_hash = TransparentAddressScriptHash::of_script_pub_key(&script);
        initialize(&store, 5, 9, 1)?;
        TransparentAddressRankingConsumer::write_snapshot_batch(
            &store,
            5,
            &[TransparentAddressRankingSnapshotRow {
                address_script_hash,
                summary: summary(Some(script.clone()), 100),
            }],
        )?;
        activate(&store, 5)?;
        let mut consumer = TransparentAddressRankingConsumer::new();
        apply_and_commit(
            &store,
            &mut consumer,
            &receive_block(10, script.clone(), 20),
        )?;
        apply_and_commit(&store, &mut consumer, &receive_block(11, script, 30))?;

        let mut batch = WriteBatch::default();
        let mut context = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut context)?;
        consumer.revert_block(BlockHeight::new(10), &mut context)?;
        consumer.revert_block(BlockHeight::new(11), &mut context)?;
        consumer.finish_batch(&mut context)?;
        store.write_batch(&batch)?;

        let restored = TransparentAddressRankingConsumer::summary(&store, address_script_hash)?
            .ok_or("restored summary missing")?;
        assert_eq!(restored.balance_zat, 100);
        assert_eq!(restored.total_received_zat, 100);
        assert_eq!(restored.distinct_transaction_count, 1);
        let metadata = TransparentAddressRankingConsumer::active_metadata(&store)?
            .ok_or("active metadata missing")?;
        assert_eq!(metadata.total_positive_balance_zat, 100);
        assert_eq!(metadata.top_100_balance_zat, 100);
        assert_eq!(
            metadata.coverage.balance_complete_through_height,
            BlockHeight::new(9)
        );
        Ok(())
    }

    #[test]
    fn non_coinbase_input_requires_a_resolved_spend() -> TestResult {
        let (_tempdir, store) = open_store()?;
        initialize(&store, 6, 9, 0)?;
        activate(&store, 6)?;
        let mut consumer = TransparentAddressRankingConsumer::new();
        let block = unresolved_spend_block(10);
        let mut batch = WriteBatch::default();
        let mut context = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut context)?;
        assert!(consumer.apply_block(&block, &mut context).is_err());
        assert!(
            TransparentAddressRankingConsumer::summary(
                &store,
                TransparentAddressScriptHash::from_bytes([0; 32])
            )?
            .is_none()
        );
        Ok(())
    }
}
