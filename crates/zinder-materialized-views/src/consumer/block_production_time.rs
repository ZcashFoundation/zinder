//! Canonical block production times ordered by exact signed timestamp.
//!
//! The materialized view stores one row per canonical block. Its primary key is the
//! signed block time followed by height and block hash, so equal and
//! non-monotonic timestamps remain lossless. A height index makes rewinds
//! deterministic. No unrelated transaction metadata is interpreted.

use std::collections::BTreeMap;

use rust_rocksdb::WriteBatch;
use zinder_core::wire::{
    decode_height_key_ascending, decode_internal_block_hash, encode_height_key_ascending,
    encode_internal_block_hash,
};
use zinder_core::{BlockHash, BlockHeight};

use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewBlockCheckpoint,
    MaterializedViewConsumerCtx, MaterializedViewConsumerError, MaterializedViewConsumerName,
    MaterializedViewConsumerSchema,
};
use crate::{
    MaterializedViewState, MaterializedViewStore, MaterializedViewStoreError,
    MaterializedViewStoreReadSnapshot,
};

/// Timestamp-ordered canonical block rows.
pub const BLOCK_PRODUCTION_TIME_COLUMN_FAMILY: &str = "block_production_time";
/// Height-to-primary-key index used for canonical rewind.
pub const BLOCK_PRODUCTION_TIME_INDEX_COLUMN_FAMILY: &str = "block_production_time_index";
/// Full-history backfill and seeded-live-tail coverage records.
pub const BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY: &str = "block_production_time_coverage";

/// Column families owned by this consumer.
pub const BLOCK_PRODUCTION_TIME_COLUMN_FAMILIES: &[&str] = &[
    BLOCK_PRODUCTION_TIME_COLUMN_FAMILY,
    BLOCK_PRODUCTION_TIME_INDEX_COLUMN_FAMILY,
    BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY,
];

/// Stable consumer identity persisted in materialized-view metadata and cursor rows.
pub const BLOCK_PRODUCTION_TIME_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("block_production_time");

/// Initial persisted schema for block production times.
pub const BLOCK_PRODUCTION_TIME_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        BLOCK_PRODUCTION_TIME_CONSUMER_NAME,
        1,
        BLOCK_PRODUCTION_TIME_COLUMN_FAMILIES,
    );

/// Hard bound for one production-time page.
pub const BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE: usize = 1_000;

const TIME_KEY_LEN: usize = size_of::<i64>();
const HEIGHT_KEY_LEN: usize = size_of::<u32>();
const BLOCK_HASH_LEN: usize = 32;
const PRIMARY_KEY_LEN: usize = TIME_KEY_LEN + HEIGHT_KEY_LEN + BLOCK_HASH_LEN;
const VALUE_VERSION: u8 = 1;
const VALUE_LEN: usize = 1;
const BACKFILL_COVERAGE_LEN: usize = 2 * HEIGHT_KEY_LEN + 2 * TIME_KEY_LEN;
const TAIL_COVERAGE_LEN: usize = HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN + TIME_KEY_LEN;
const BACKFILL_COVERAGE_KEY: &[u8] = b"full_history_backfill";
const TAIL_COVERAGE_KEY: &[u8] = b"seeded_live_tail";

type PrimaryKey = [u8; PRIMARY_KEY_LEN];
type StoredValue = [u8; VALUE_LEN];
type PendingRow = Option<(PrimaryKey, StoredValue)>;
type RawConsumerEntry = (Vec<u8>, Vec<u8>);

#[derive(Clone, Copy)]
enum BlockProductionTimeRead<'store> {
    Store(&'store MaterializedViewStore),
    Snapshot(&'store MaterializedViewStoreReadSnapshot<'store>),
}

impl BlockProductionTimeRead<'_> {
    fn get_consumer(
        self,
        column_family: &'static str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, MaterializedViewStoreError> {
        match self {
            Self::Store(store) => store.get_consumer(column_family, key),
            Self::Snapshot(snapshot) => snapshot.get_consumer(column_family, key),
        }
    }

    fn range_iterate_consumer(
        self,
        column_family: &'static str,
        start_key: &[u8],
        end_key_inclusive: &[u8],
        entries_cap: usize,
    ) -> Result<Vec<RawConsumerEntry>, MaterializedViewStoreError> {
        match self {
            Self::Store(store) => store.range_iterate_consumer(
                column_family,
                start_key,
                end_key_inclusive,
                entries_cap,
            ),
            Self::Snapshot(snapshot) => snapshot.range_iterate_consumer(
                column_family,
                start_key,
                end_key_inclusive,
                entries_cap,
            ),
        }
    }
}

/// One canonical block production-time row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockProductionTimeRow {
    /// Exact signed Unix block timestamp.
    pub block_time_unix_seconds: i64,
    /// Canonical block height.
    pub block_height: BlockHeight,
    /// Canonical block hash.
    pub block_hash: BlockHash,
}

/// Opaque continuation key for ascending production-time pagination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockProductionTimeCursor(PrimaryKey);

impl BlockProductionTimeCursor {
    /// Decodes and validates a persisted continuation key.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BlockProductionTimeConsumerError> {
        let key: PrimaryKey = bytes.try_into().map_err(|_| {
            BlockProductionTimeConsumerError::MalformedPrimaryKey { bytes: bytes.len() }
        })?;
        decode_primary_key(&key)?;
        Ok(Self(key))
    }

    /// Returns the stable opaque bytes carried by an API continuation token.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; PRIMARY_KEY_LEN] {
        &self.0
    }
}

/// Bounded half-open timestamp-range page request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockProductionTimePageRequest {
    /// Inclusive signed Unix timestamp.
    pub start_time_unix_seconds: i64,
    /// Exclusive signed Unix timestamp.
    pub end_time_unix_seconds: i64,
    /// Last key returned by the preceding page.
    pub after: Option<BlockProductionTimeCursor>,
    /// Highest canonical height included in this frozen query generation.
    pub maximum_height: Option<BlockHeight>,
    /// Requested row count, bounded by [`BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE`].
    pub limit: usize,
    /// Optional cap on candidate rows scanned before stopping early.
    pub max_scanned_rows: Option<usize>,
}

/// One bounded page and its optional continuation cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockProductionTimePage {
    /// Rows in exact primary-key order.
    pub rows: Vec<BlockProductionTimeRow>,
    /// Cursor to resume after the last returned or scanned row when more candidates remain.
    pub next_cursor: Option<BlockProductionTimeCursor>,
    /// True when `max_scanned_rows` stopped scanning before exhausting `[start, end)` or `limit`.
    pub scan_exhausted: bool,
}

/// Contiguous canonical history materialized by explicit full-history backfill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockProductionTimeBackfillCoverage {
    /// First materialized canonical height.
    pub complete_from_height: BlockHeight,
    /// Last materialized canonical height.
    pub complete_through_height: BlockHeight,
    /// Timestamp at the first materialized height.
    pub complete_from_time_unix_seconds: i64,
    /// Timestamp at the last materialized height.
    pub complete_through_time_unix_seconds: i64,
}

impl BlockProductionTimeBackfillCoverage {
    /// Creates a contiguous full-history backfill record.
    #[must_use]
    pub const fn new(
        complete_from_height: BlockHeight,
        complete_through_height: BlockHeight,
        complete_from_time_unix_seconds: i64,
        complete_through_time_unix_seconds: i64,
    ) -> Self {
        Self {
            complete_from_height,
            complete_through_height,
            complete_from_time_unix_seconds,
            complete_through_time_unix_seconds,
        }
    }
}

/// Durable live-tail interval established at an explicit seed boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockProductionTimeTailCoverage {
    /// First height owned by the live tail.
    pub boundary_height: BlockHeight,
    /// Last contiguous live-tail height, absent before seeding any block.
    pub complete_through_height: Option<BlockHeight>,
    /// Timestamp at `complete_through_height`.
    pub complete_through_time_unix_seconds: Option<i64>,
}

impl BlockProductionTimeTailCoverage {
    /// Creates an empty live tail at `boundary_height`.
    #[must_use]
    pub const fn from_boundary(boundary_height: BlockHeight) -> Self {
        Self {
            boundary_height,
            complete_through_height: None,
            complete_through_time_unix_seconds: None,
        }
    }
}

/// Materializes exact canonical block production times.
#[derive(Default)]
pub struct BlockProductionTimeConsumer {
    pending_rows: BTreeMap<BlockHeight, PendingRow>,
}

impl BlockProductionTimeConsumer {
    /// Builds an empty consumer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pending_rows: BTreeMap::new(),
        }
    }

    /// Reads a bounded page from the live store.
    pub fn read_page(
        store: &MaterializedViewStore,
        request: BlockProductionTimePageRequest,
    ) -> Result<BlockProductionTimePage, BlockProductionTimeConsumerError> {
        Self::read_page_from(BlockProductionTimeRead::Store(store), request)
    }

    /// Reads a bounded page and all query bounds from one storage snapshot.
    pub fn read_page_snapshot(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
        request: BlockProductionTimePageRequest,
    ) -> Result<BlockProductionTimePage, BlockProductionTimeConsumerError> {
        Self::read_page_from(BlockProductionTimeRead::Snapshot(snapshot), request)
    }

    fn read_page_from(
        store: BlockProductionTimeRead<'_>,
        request: BlockProductionTimePageRequest,
    ) -> Result<BlockProductionTimePage, BlockProductionTimeConsumerError> {
        validate_page_request(request)?;
        let (range_start, end_inclusive, entries_cap) = page_key_bounds(request)?;
        let mut scan_start = request.after.map_or(range_start, |cursor| cursor.0);
        let mut decoded = Vec::with_capacity(entries_cap);
        let mut scan_start_is_exclusive = request.after.is_some();
        let mut scanned_rows = 0_usize;
        let mut last_scanned_cursor = None;
        let mut budget_exhausted = false;
        'scan: loop {
            let fetch_cap = request.max_scanned_rows.map_or(entries_cap, |budget| {
                budget
                    .saturating_sub(scanned_rows)
                    .saturating_add(2)
                    .min(entries_cap)
            });
            let entries = store.range_iterate_consumer(
                BLOCK_PRODUCTION_TIME_COLUMN_FAMILY,
                &scan_start,
                &end_inclusive,
                fetch_cap,
            )?;
            if entries.is_empty() {
                break;
            }
            let entry_count = entries.len();
            let mut advanced = false;
            for (key, payload) in entries {
                if scan_start_is_exclusive && key == scan_start {
                    continue;
                }
                if request
                    .max_scanned_rows
                    .is_some_and(|budget| scanned_rows >= budget)
                {
                    budget_exhausted = true;
                    break 'scan;
                }
                let cursor = BlockProductionTimeCursor::from_bytes(&key)?;
                let row = decode_row(&key, &payload)?;
                scan_start = cursor.0;
                scan_start_is_exclusive = true;
                scanned_rows = scanned_rows.saturating_add(1);
                last_scanned_cursor = Some(cursor);
                advanced = true;
                if request
                    .maximum_height
                    .is_some_and(|maximum| row.block_height > maximum)
                {
                    continue;
                }
                decoded.push((cursor, row));
                if decoded.len() > request.limit {
                    break 'scan;
                }
            }
            if !advanced || entry_count < fetch_cap {
                break;
            }
        }
        Ok(finalize_page(
            decoded,
            request.limit,
            last_scanned_cursor,
            budget_exhausted,
        ))
    }

    /// Reads explicit full-history backfill coverage.
    pub fn backfill_coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<BlockProductionTimeBackfillCoverage>, MaterializedViewStoreError> {
        Self::read_backfill_coverage(BlockProductionTimeRead::Store(store))
    }

    fn read_backfill_coverage(
        store: BlockProductionTimeRead<'_>,
    ) -> Result<Option<BlockProductionTimeBackfillCoverage>, MaterializedViewStoreError> {
        store
            .get_consumer(
                BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY,
                BACKFILL_COVERAGE_KEY,
            )?
            .map(|payload| {
                decode_backfill_coverage(&payload).map_err(|error| coverage_decode_error(&error))
            })
            .transpose()
    }

    /// Reads the explicit live-tail boundary and contiguous tail tip.
    pub fn tail_coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<BlockProductionTimeTailCoverage>, MaterializedViewStoreError> {
        Self::read_tail_coverage(BlockProductionTimeRead::Store(store))
    }

    fn read_tail_coverage(
        store: BlockProductionTimeRead<'_>,
    ) -> Result<Option<BlockProductionTimeTailCoverage>, MaterializedViewStoreError> {
        store
            .get_consumer(
                BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY,
                TAIL_COVERAGE_KEY,
            )?
            .map(|payload| {
                decode_tail_coverage(&payload).map_err(|error| coverage_decode_error(&error))
            })
            .transpose()
    }

    /// Joins backfill and live-tail coverage only when their height intervals meet.
    pub fn coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<BlockProductionTimeBackfillCoverage>, MaterializedViewStoreError> {
        Self::read_coverage(BlockProductionTimeRead::Store(store))
    }

    /// Reads joined coverage from the same snapshot used for page rows.
    pub fn coverage_snapshot(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    ) -> Result<Option<BlockProductionTimeBackfillCoverage>, MaterializedViewStoreError> {
        Self::read_coverage(BlockProductionTimeRead::Snapshot(snapshot))
    }

    /// Reads one indexed row by height from a stable materialized-view snapshot.
    pub fn row_at_height_snapshot(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
        height: BlockHeight,
    ) -> Result<Option<BlockProductionTimeRow>, BlockProductionTimeConsumerError> {
        read_row_at_height(BlockProductionTimeRead::Snapshot(snapshot), height)
    }

    fn read_coverage(
        store: BlockProductionTimeRead<'_>,
    ) -> Result<Option<BlockProductionTimeBackfillCoverage>, MaterializedViewStoreError> {
        let backfill = Self::read_backfill_coverage(store)?;
        let tail = Self::read_tail_coverage(store)?;
        join_coverage(backfill, tail).map_err(|error| coverage_decode_error(&error))
    }

    /// Initializes the first height owned by the live tail.
    pub fn initialize_tail_boundary(
        store: &MaterializedViewStore,
        boundary_height: BlockHeight,
    ) -> Result<(), BlockProductionTimeConsumerError> {
        let requested = BlockProductionTimeTailCoverage::from_boundary(boundary_height);
        match Self::tail_coverage(store)? {
            None => store.put_consumer(
                BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY,
                TAIL_COVERAGE_KEY,
                &encode_tail_coverage(requested),
            )?,
            Some(existing) if existing == requested => {}
            Some(_) => {
                return Err(BlockProductionTimeConsumerError::TailBoundaryConflict {
                    boundary_height: boundary_height.value(),
                });
            }
        }
        Ok(())
    }

    /// Atomically writes an ordered full-history batch and its coverage record.
    pub fn write_backfill_batch(
        &mut self,
        store: &MaterializedViewStore,
        blocks: &[BlockCommitContext],
        next_coverage: BlockProductionTimeBackfillCoverage,
    ) -> Result<(), MaterializedViewConsumerError> {
        validate_backfill_batch(store, blocks, next_coverage)?;
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        self.begin_batch(&mut ctx)?;
        for block in blocks {
            self.apply_block(block, &mut ctx)?;
        }
        self.finish_batch(&mut ctx)?;
        let coverage_cf =
            store.consumer_column_family(BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY)?;
        ctx.batch.put_cf(
            &coverage_cf,
            BACKFILL_COVERAGE_KEY,
            encode_backfill_coverage(next_coverage),
        );
        store.write_batch(ctx.batch)?;
        Ok(())
    }

    /// Atomically writes pre-derived rows, reverse indexes, and backfill coverage.
    ///
    /// Startup backfill can use this path with existing block-summary rows and
    /// does not need to reconstruct [`BlockCommitContext`] values.
    pub fn write_backfill_rows(
        store: &MaterializedViewStore,
        rows: &[BlockProductionTimeRow],
        next_coverage: BlockProductionTimeBackfillCoverage,
    ) -> Result<(), BlockProductionTimeConsumerError> {
        validate_backfill_rows(store, rows, next_coverage)?;
        let rows_cf = store.consumer_column_family(BLOCK_PRODUCTION_TIME_COLUMN_FAMILY)?;
        let index_cf = store.consumer_column_family(BLOCK_PRODUCTION_TIME_INDEX_COLUMN_FAMILY)?;
        let coverage_cf =
            store.consumer_column_family(BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY)?;
        let mut pending = BTreeMap::new();
        let mut batch = WriteBatch::default();
        for row in rows {
            let key = encode_primary_key(
                row.block_time_unix_seconds,
                row.block_height,
                row.block_hash,
            );
            let payload = encode_value();
            validate_apply_state(store, &pending, row.block_height, key, payload)?;
            pending.insert(row.block_height, Some((key, payload)));
            batch.put_cf(&rows_cf, key, payload);
            batch.put_cf(
                &index_cf,
                encode_height_key_ascending(row.block_height),
                key,
            );
        }
        batch.put_cf(
            &coverage_cf,
            BACKFILL_COVERAGE_KEY,
            encode_backfill_coverage(next_coverage),
        );
        store.write_batch(&batch)?;
        Ok(())
    }

    /// Atomically seeds contiguous blocks at an initialized live-tail boundary.
    pub fn write_tail_seed_batch(
        &mut self,
        store: &MaterializedViewStore,
        blocks: &[BlockCommitContext],
    ) -> Result<(), MaterializedViewConsumerError> {
        validate_tail_seed_batch(store, blocks)?;
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        self.begin_batch(&mut ctx)?;
        for block in blocks {
            self.apply_block(block, &mut ctx)?;
        }
        self.finish_batch(&mut ctx)?;
        store.write_batch(ctx.batch)?;
        Ok(())
    }

    fn stage_tail_coverage(
        &self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), BlockProductionTimeConsumerError> {
        let Some(mut tail) = Self::tail_coverage(ctx.store)? else {
            return Ok(());
        };
        if self.pending_rows.iter().any(|(height, row)| {
            row.is_none()
                && tail
                    .complete_through_height
                    .is_some_and(|tip| *height <= tip)
        }) {
            let first_deleted = self
                .pending_rows
                .iter()
                .find_map(|(height, row)| row.is_none().then_some(*height))
                .ok_or(BlockProductionTimeConsumerError::CoverageDiscontinuous)?;
            tail.complete_through_height = first_deleted
                .value()
                .checked_sub(1)
                .map(BlockHeight::new)
                .filter(|height| *height >= tail.boundary_height);
            tail.complete_through_time_unix_seconds = tail
                .complete_through_height
                .map(|height| self.row_after_batch(ctx.store, height))
                .transpose()?
                .flatten()
                .map(|row| row.block_time_unix_seconds);
        }
        let mut next = tail
            .complete_through_height
            .map_or(Some(tail.boundary_height), BlockHeight::next);
        while let Some(height) = next {
            let Some(row) = self.row_after_batch(ctx.store, height)? else {
                break;
            };
            tail.complete_through_height = Some(height);
            tail.complete_through_time_unix_seconds = Some(row.block_time_unix_seconds);
            next = height.next();
        }
        if let Some(tip) = tail.complete_through_height
            && let Some(Some((key, payload))) = self.pending_rows.get(&tip)
        {
            tail.complete_through_time_unix_seconds =
                Some(decode_row(key, payload)?.block_time_unix_seconds);
        }
        let coverage_cf = ctx
            .store
            .consumer_column_family(BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY)?;
        ctx.batch
            .put_cf(&coverage_cf, TAIL_COVERAGE_KEY, encode_tail_coverage(tail));
        Ok(())
    }

    fn row_after_batch(
        &self,
        store: &MaterializedViewStore,
        height: BlockHeight,
    ) -> Result<Option<BlockProductionTimeRow>, BlockProductionTimeConsumerError> {
        if let Some(pending) = self.pending_rows.get(&height) {
            return pending
                .as_ref()
                .map(|(key, payload)| decode_row(key, payload))
                .transpose();
        }
        read_row_at_height(BlockProductionTimeRead::Store(store), height)
    }
}

impl BlockKeyedConsumer for BlockProductionTimeConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        BLOCK_PRODUCTION_TIME_CONSUMER_NAME
    }

    fn begin_batch(
        &mut self,
        _ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        self.pending_rows.clear();
        Ok(())
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let key = encode_primary_key(
            block.block_time_unix_seconds,
            block.height,
            block.block_hash,
        );
        let payload = encode_value();
        validate_apply_state(ctx.store, &self.pending_rows, block.height, key, payload)?;
        self.pending_rows.insert(block.height, Some((key, payload)));
        let rows_cf = ctx
            .store
            .consumer_column_family(BLOCK_PRODUCTION_TIME_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(BLOCK_PRODUCTION_TIME_INDEX_COLUMN_FAMILY)?;
        ctx.batch.put_cf(&rows_cf, key, payload);
        ctx.batch
            .put_cf(&index_cf, encode_height_key_ascending(block.height), key);
        Ok(())
    }

    fn revert_block(
        &mut self,
        height: BlockHeight,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let key = match self.pending_rows.get(&height) {
            Some(Some((key, _))) => Some(*key),
            Some(None) => None,
            None => read_index_key(BlockProductionTimeRead::Store(ctx.store), height)?,
        };
        let rows_cf = ctx
            .store
            .consumer_column_family(BLOCK_PRODUCTION_TIME_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(BLOCK_PRODUCTION_TIME_INDEX_COLUMN_FAMILY)?;
        if let Some(key) = key {
            if !self.pending_rows.contains_key(&height)
                && ctx
                    .store
                    .get_consumer(BLOCK_PRODUCTION_TIME_COLUMN_FAMILY, &key)?
                    .is_none()
            {
                return Err(Box::new(
                    BlockProductionTimeConsumerError::MissingIndexedRow {
                        height: height.value(),
                    },
                ));
            }
            ctx.batch.delete_cf(&rows_cf, key);
        }
        ctx.batch
            .delete_cf(&index_cf, encode_height_key_ascending(height));
        self.pending_rows.insert(height, None);
        Ok(())
    }

    fn finish_batch(
        &mut self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        self.stage_tail_coverage(ctx)?;
        // The materialized-view checkpoint hook runs after this method and needs the
        // same batch-local overlay to reconcile historical coverage on reorg.
        Ok(())
    }

    fn stage_chain_event_checkpoint(
        &mut self,
        checkpoint: MaterializedViewBlockCheckpoint<'_>,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let tip_height = checkpoint
            .tip_height
            .ok_or(BlockProductionTimeConsumerError::IncompleteMaterializedViewCheckpoint)?;
        let tip_hash = checkpoint
            .tip_hash
            .ok_or(BlockProductionTimeConsumerError::IncompleteMaterializedViewCheckpoint)?;
        self.stage_backfill_coverage_at_tip(ctx, tip_height)?;
        let current = ctx
            .store
            .consumer_state(BLOCK_PRODUCTION_TIME_CONSUMER_NAME)?;
        let revision = current
            .map_or(Some(1), |state| state.revision.checked_add(1))
            .ok_or(BlockProductionTimeConsumerError::MaterializedViewRevisionOverflow)?;
        ctx.store.stage_consumer_state(
            ctx.batch,
            BLOCK_PRODUCTION_TIME_CONSUMER_NAME,
            MaterializedViewState {
                chain_epoch_id: checkpoint.chain_epoch.id,
                tip_height,
                tip_hash,
                revision,
                coverage: None,
            },
        )?;
        Ok(())
    }
}

impl BlockProductionTimeConsumer {
    fn stage_backfill_coverage_at_tip(
        &self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
        tip_height: BlockHeight,
    ) -> Result<(), BlockProductionTimeConsumerError> {
        let Some(existing) = Self::backfill_coverage(ctx.store)? else {
            return Ok(());
        };
        let coverage_cf = ctx
            .store
            .consumer_column_family(BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY)?;
        let mut through = existing.complete_through_height.min(tip_height);
        while through >= existing.complete_from_height
            && self.row_after_batch(ctx.store, through)?.is_none()
        {
            let Some(previous) = through.value().checked_sub(1).map(BlockHeight::new) else {
                break;
            };
            through = previous;
        }
        let Some(from_row) = self.row_after_batch(ctx.store, existing.complete_from_height)? else {
            ctx.batch.delete_cf(&coverage_cf, BACKFILL_COVERAGE_KEY);
            return Ok(());
        };
        if through < existing.complete_from_height {
            ctx.batch.delete_cf(&coverage_cf, BACKFILL_COVERAGE_KEY);
            return Ok(());
        }
        let Some(through_row) = self.row_after_batch(ctx.store, through)? else {
            return Err(BlockProductionTimeConsumerError::CoverageDiscontinuous);
        };
        let adjusted = BlockProductionTimeBackfillCoverage::new(
            existing.complete_from_height,
            through,
            from_row.block_time_unix_seconds,
            through_row.block_time_unix_seconds,
        );
        if adjusted != existing {
            ctx.batch.put_cf(
                &coverage_cf,
                BACKFILL_COVERAGE_KEY,
                encode_backfill_coverage(adjusted),
            );
        }
        Ok(())
    }
}

fn validate_page_request(
    request: BlockProductionTimePageRequest,
) -> Result<(), BlockProductionTimeConsumerError> {
    if request.start_time_unix_seconds >= request.end_time_unix_seconds {
        return Err(BlockProductionTimeConsumerError::InvalidTimeRange {
            start: request.start_time_unix_seconds,
            end: request.end_time_unix_seconds,
        });
    }
    if request.limit == 0 || request.limit > BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE {
        return Err(BlockProductionTimeConsumerError::PageLimitTooLarge {
            requested: request.limit,
            maximum: BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE,
        });
    }
    if request.max_scanned_rows == Some(0) {
        return Err(BlockProductionTimeConsumerError::InvalidScanBudget);
    }
    if let Some(cursor) = request.after {
        let (time, _, _) = decode_primary_key(&cursor.0)?;
        if time < request.start_time_unix_seconds || time >= request.end_time_unix_seconds {
            return Err(BlockProductionTimeConsumerError::CursorOutsideRange);
        }
    }
    Ok(())
}

fn page_key_bounds(
    request: BlockProductionTimePageRequest,
) -> Result<(PrimaryKey, PrimaryKey, usize), BlockProductionTimeConsumerError> {
    let range_start = encode_primary_key(
        request.start_time_unix_seconds,
        BlockHeight::new(0),
        BlockHash::from_bytes([0; BLOCK_HASH_LEN]),
    );
    let end_inclusive = encode_primary_key(
        request.end_time_unix_seconds.checked_sub(1).ok_or(
            BlockProductionTimeConsumerError::InvalidTimeRange {
                start: request.start_time_unix_seconds,
                end: request.end_time_unix_seconds,
            },
        )?,
        BlockHeight::new(u32::MAX),
        BlockHash::from_bytes([u8::MAX; BLOCK_HASH_LEN]),
    );
    let entries_cap = request.limit.checked_add(2).ok_or(
        BlockProductionTimeConsumerError::PageLimitTooLarge {
            requested: request.limit,
            maximum: BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE,
        },
    )?;
    Ok((range_start, end_inclusive, entries_cap))
}

fn finalize_page(
    mut decoded: Vec<(BlockProductionTimeCursor, BlockProductionTimeRow)>,
    limit: usize,
    last_scanned_cursor: Option<BlockProductionTimeCursor>,
    budget_exhausted: bool,
) -> BlockProductionTimePage {
    let has_more = decoded.len() > limit;
    decoded.truncate(limit);
    let (next_cursor, scan_exhausted) = if has_more {
        (decoded.last().map(|(cursor, _)| *cursor), false)
    } else if budget_exhausted {
        (last_scanned_cursor, true)
    } else {
        (None, false)
    };
    BlockProductionTimePage {
        rows: decoded.into_iter().map(|(_, row)| row).collect(),
        next_cursor,
        scan_exhausted,
    }
}

fn validate_apply_state(
    store: &MaterializedViewStore,
    pending: &BTreeMap<BlockHeight, PendingRow>,
    height: BlockHeight,
    expected_key: PrimaryKey,
    expected_payload: StoredValue,
) -> Result<(), BlockProductionTimeConsumerError> {
    if let Some(staged) = pending.get(&height) {
        return match staged {
            None => Ok(()),
            Some((key, payload)) if *key == expected_key && *payload == expected_payload => Ok(()),
            Some(_) => Err(BlockProductionTimeConsumerError::ConflictingHeight {
                height: height.value(),
            }),
        };
    }
    let indexed_key = read_index_key(BlockProductionTimeRead::Store(store), height)?;
    let expected_row = store.get_consumer(BLOCK_PRODUCTION_TIME_COLUMN_FAMILY, &expected_key)?;
    match (indexed_key, expected_row) {
        (None, None) => Ok(()),
        (Some(key), Some(payload)) if key == expected_key && payload == expected_payload => {
            decode_row(&key, &payload)?;
            Ok(())
        }
        (Some(key), _) => {
            let stored = store.get_consumer(BLOCK_PRODUCTION_TIME_COLUMN_FAMILY, &key)?;
            if stored.is_none() {
                Err(BlockProductionTimeConsumerError::MissingIndexedRow {
                    height: height.value(),
                })
            } else {
                Err(BlockProductionTimeConsumerError::ConflictingHeight {
                    height: height.value(),
                })
            }
        }
        (None, Some(_)) => Err(BlockProductionTimeConsumerError::IncompleteHeightState {
            height: height.value(),
        }),
    }
}

fn read_row_at_height(
    store: BlockProductionTimeRead<'_>,
    height: BlockHeight,
) -> Result<Option<BlockProductionTimeRow>, BlockProductionTimeConsumerError> {
    let Some(key) = read_index_key(store, height)? else {
        return Ok(None);
    };
    let payload = store
        .get_consumer(BLOCK_PRODUCTION_TIME_COLUMN_FAMILY, &key)?
        .ok_or_else(|| BlockProductionTimeConsumerError::MissingIndexedRow {
            height: height.value(),
        })?;
    decode_row(&key, &payload).map(Some)
}

fn read_index_key(
    store: BlockProductionTimeRead<'_>,
    height: BlockHeight,
) -> Result<Option<PrimaryKey>, BlockProductionTimeConsumerError> {
    let Some(payload) = store.get_consumer(
        BLOCK_PRODUCTION_TIME_INDEX_COLUMN_FAMILY,
        &encode_height_key_ascending(height),
    )?
    else {
        return Ok(None);
    };
    let key: PrimaryKey = payload.as_slice().try_into().map_err(|_| {
        BlockProductionTimeConsumerError::MalformedHeightIndex {
            height: height.value(),
            bytes: payload.len(),
        }
    })?;
    let (_, indexed_height, _) = decode_primary_key(&key)?;
    if indexed_height != height {
        return Err(BlockProductionTimeConsumerError::IndexHeightMismatch {
            requested_height: height.value(),
            indexed_height: indexed_height.value(),
        });
    }
    Ok(Some(key))
}

fn encode_signed_time(unix_seconds: i64) -> [u8; TIME_KEY_LEN] {
    (unix_seconds.cast_unsigned() ^ (1_u64 << 63)).to_be_bytes()
}

fn decode_signed_time(bytes: &[u8]) -> Result<i64, BlockProductionTimeConsumerError> {
    let encoded = u64::from_be_bytes(bytes.try_into().map_err(|_| {
        BlockProductionTimeConsumerError::MalformedPrimaryKey { bytes: bytes.len() }
    })?);
    Ok((encoded ^ (1_u64 << 63)).cast_signed())
}

fn encode_primary_key(
    block_time_unix_seconds: i64,
    block_height: BlockHeight,
    block_hash: BlockHash,
) -> PrimaryKey {
    let mut key = [0_u8; PRIMARY_KEY_LEN];
    key[..TIME_KEY_LEN].copy_from_slice(&encode_signed_time(block_time_unix_seconds));
    key[TIME_KEY_LEN..TIME_KEY_LEN + HEIGHT_KEY_LEN]
        .copy_from_slice(&encode_height_key_ascending(block_height));
    key[TIME_KEY_LEN + HEIGHT_KEY_LEN..].copy_from_slice(&encode_internal_block_hash(block_hash));
    key
}

fn decode_primary_key(
    key: &[u8],
) -> Result<(i64, BlockHeight, BlockHash), BlockProductionTimeConsumerError> {
    if key.len() != PRIMARY_KEY_LEN {
        return Err(BlockProductionTimeConsumerError::MalformedPrimaryKey { bytes: key.len() });
    }
    let time = decode_signed_time(&key[..TIME_KEY_LEN])?;
    let height = decode_height_key_ascending(&key[TIME_KEY_LEN..TIME_KEY_LEN + HEIGHT_KEY_LEN])
        .map_err(|_| BlockProductionTimeConsumerError::MalformedPrimaryKey { bytes: key.len() })?;
    let hash = decode_internal_block_hash(&key[TIME_KEY_LEN + HEIGHT_KEY_LEN..])
        .map_err(|_| BlockProductionTimeConsumerError::MalformedPrimaryKey { bytes: key.len() })?;
    Ok((time, height, hash))
}

const fn encode_value() -> StoredValue {
    [VALUE_VERSION]
}

fn decode_value(payload: &[u8]) -> Result<(), BlockProductionTimeConsumerError> {
    if payload.len() != VALUE_LEN || payload.first() != Some(&VALUE_VERSION) {
        return Err(BlockProductionTimeConsumerError::MalformedValue {
            bytes: payload.len(),
            version: payload.first().copied(),
        });
    }
    Ok(())
}

fn decode_row(
    key: &[u8],
    payload: &[u8],
) -> Result<BlockProductionTimeRow, BlockProductionTimeConsumerError> {
    let (block_time_unix_seconds, block_height, block_hash) = decode_primary_key(key)?;
    decode_value(payload)?;
    Ok(BlockProductionTimeRow {
        block_time_unix_seconds,
        block_height,
        block_hash,
    })
}

fn validate_backfill_batch(
    store: &MaterializedViewStore,
    blocks: &[BlockCommitContext],
    next: BlockProductionTimeBackfillCoverage,
) -> Result<(), MaterializedViewConsumerError> {
    let Some(first) = blocks.first() else {
        return Err(Box::new(BlockProductionTimeConsumerError::EmptyBackfill));
    };
    let last = blocks
        .last()
        .ok_or(BlockProductionTimeConsumerError::EmptyBackfill)?;
    if blocks
        .windows(2)
        .any(|pair| pair[0].height.next() != Some(pair[1].height))
        || next.complete_from_height > next.complete_through_height
    {
        return Err(Box::new(
            BlockProductionTimeConsumerError::CoverageDiscontinuous,
        ));
    }
    let existing = BlockProductionTimeConsumer::backfill_coverage(store)?;
    if backfill_transition_is_contiguous(existing, first, last, next) {
        Ok(())
    } else {
        Err(Box::new(
            BlockProductionTimeConsumerError::CoverageDiscontinuous,
        ))
    }
}

fn validate_backfill_rows(
    store: &MaterializedViewStore,
    rows: &[BlockProductionTimeRow],
    next: BlockProductionTimeBackfillCoverage,
) -> Result<(), BlockProductionTimeConsumerError> {
    let Some(first) = rows.first() else {
        return Err(BlockProductionTimeConsumerError::EmptyBackfill);
    };
    let last = rows
        .last()
        .ok_or(BlockProductionTimeConsumerError::EmptyBackfill)?;
    if rows
        .windows(2)
        .any(|pair| pair[0].block_height.next() != Some(pair[1].block_height))
        || next.complete_from_height > next.complete_through_height
    {
        return Err(BlockProductionTimeConsumerError::CoverageDiscontinuous);
    }
    let existing = BlockProductionTimeConsumer::backfill_coverage(store)?;
    if backfill_row_transition_is_contiguous(existing, first, last, next) {
        Ok(())
    } else {
        Err(BlockProductionTimeConsumerError::CoverageDiscontinuous)
    }
}

fn backfill_row_transition_is_contiguous(
    existing: Option<BlockProductionTimeBackfillCoverage>,
    first: &BlockProductionTimeRow,
    last: &BlockProductionTimeRow,
    next: BlockProductionTimeBackfillCoverage,
) -> bool {
    existing.map_or_else(
        || {
            first.block_height == next.complete_from_height
                && first.block_time_unix_seconds == next.complete_from_time_unix_seconds
                && last.block_height == next.complete_through_height
                && last.block_time_unix_seconds == next.complete_through_time_unix_seconds
        },
        |existing| {
            let appends = existing.complete_from_height == next.complete_from_height
                && existing.complete_from_time_unix_seconds == next.complete_from_time_unix_seconds
                && existing.complete_through_height.next() == Some(first.block_height)
                && last.block_height == next.complete_through_height
                && last.block_time_unix_seconds == next.complete_through_time_unix_seconds;
            let prepends = first.block_height == next.complete_from_height
                && first.block_time_unix_seconds == next.complete_from_time_unix_seconds
                && last.block_height.next() == Some(existing.complete_from_height)
                && existing.complete_through_height == next.complete_through_height
                && existing.complete_through_time_unix_seconds
                    == next.complete_through_time_unix_seconds;
            appends || prepends
        },
    )
}

fn backfill_transition_is_contiguous(
    existing: Option<BlockProductionTimeBackfillCoverage>,
    first: &BlockCommitContext,
    last: &BlockCommitContext,
    next: BlockProductionTimeBackfillCoverage,
) -> bool {
    existing.map_or_else(
        || {
            first.height == next.complete_from_height
                && first.block_time_unix_seconds == next.complete_from_time_unix_seconds
                && last.height == next.complete_through_height
                && last.block_time_unix_seconds == next.complete_through_time_unix_seconds
        },
        |existing| {
            let appends = existing.complete_from_height == next.complete_from_height
                && existing.complete_from_time_unix_seconds == next.complete_from_time_unix_seconds
                && existing.complete_through_height.next() == Some(first.height)
                && last.height == next.complete_through_height
                && last.block_time_unix_seconds == next.complete_through_time_unix_seconds;
            let prepends = first.height == next.complete_from_height
                && first.block_time_unix_seconds == next.complete_from_time_unix_seconds
                && last.height.next() == Some(existing.complete_from_height)
                && existing.complete_through_height == next.complete_through_height
                && existing.complete_through_time_unix_seconds
                    == next.complete_through_time_unix_seconds;
            appends || prepends
        },
    )
}

fn validate_tail_seed_batch(
    store: &MaterializedViewStore,
    blocks: &[BlockCommitContext],
) -> Result<(), MaterializedViewConsumerError> {
    let Some(first) = blocks.first() else {
        return Err(Box::new(BlockProductionTimeConsumerError::EmptyBackfill));
    };
    if blocks
        .windows(2)
        .any(|pair| pair[0].height.next() != Some(pair[1].height))
    {
        return Err(Box::new(
            BlockProductionTimeConsumerError::CoverageDiscontinuous,
        ));
    }
    let tail = BlockProductionTimeConsumer::tail_coverage(store)?.ok_or_else(|| {
        Box::new(BlockProductionTimeConsumerError::CoverageDiscontinuous)
            as MaterializedViewConsumerError
    })?;
    let expected = tail
        .complete_through_height
        .map_or(Some(tail.boundary_height), BlockHeight::next);
    if expected == Some(first.height) {
        Ok(())
    } else {
        Err(Box::new(
            BlockProductionTimeConsumerError::CoverageDiscontinuous,
        ))
    }
}

fn join_coverage(
    backfill: Option<BlockProductionTimeBackfillCoverage>,
    tail: Option<BlockProductionTimeTailCoverage>,
) -> Result<Option<BlockProductionTimeBackfillCoverage>, BlockProductionTimeConsumerError> {
    let Some(mut joined) = backfill else {
        return Ok(None);
    };
    let Some(tail) = tail else {
        return Ok(Some(joined));
    };
    let Some(tail_height) = tail.complete_through_height else {
        return Ok(Some(joined));
    };
    let meets = joined.complete_through_height >= tail.boundary_height
        || joined.complete_through_height.next() == Some(tail.boundary_height);
    if meets && tail_height > joined.complete_through_height {
        joined.complete_through_height = tail_height;
        joined.complete_through_time_unix_seconds = tail
            .complete_through_time_unix_seconds
            .ok_or(BlockProductionTimeConsumerError::MalformedTailCoverage)?;
    }
    Ok(Some(joined))
}

fn encode_backfill_coverage(
    coverage: BlockProductionTimeBackfillCoverage,
) -> [u8; BACKFILL_COVERAGE_LEN] {
    let mut payload = [0_u8; BACKFILL_COVERAGE_LEN];
    payload[..HEIGHT_KEY_LEN]
        .copy_from_slice(&encode_height_key_ascending(coverage.complete_from_height));
    payload[HEIGHT_KEY_LEN..2 * HEIGHT_KEY_LEN].copy_from_slice(&encode_height_key_ascending(
        coverage.complete_through_height,
    ));
    payload[2 * HEIGHT_KEY_LEN..2 * HEIGHT_KEY_LEN + TIME_KEY_LEN]
        .copy_from_slice(&coverage.complete_from_time_unix_seconds.to_be_bytes());
    payload[2 * HEIGHT_KEY_LEN + TIME_KEY_LEN..]
        .copy_from_slice(&coverage.complete_through_time_unix_seconds.to_be_bytes());
    payload
}

fn decode_backfill_coverage(
    payload: &[u8],
) -> Result<BlockProductionTimeBackfillCoverage, BlockProductionTimeConsumerError> {
    if payload.len() != BACKFILL_COVERAGE_LEN {
        return Err(BlockProductionTimeConsumerError::MalformedBackfillCoverage);
    }
    let from = decode_height_key_ascending(&payload[..HEIGHT_KEY_LEN])
        .map_err(|_| BlockProductionTimeConsumerError::MalformedBackfillCoverage)?;
    let through = decode_height_key_ascending(&payload[HEIGHT_KEY_LEN..2 * HEIGHT_KEY_LEN])
        .map_err(|_| BlockProductionTimeConsumerError::MalformedBackfillCoverage)?;
    if from > through {
        return Err(BlockProductionTimeConsumerError::MalformedBackfillCoverage);
    }
    let from_time = i64::from_be_bytes(
        payload[2 * HEIGHT_KEY_LEN..2 * HEIGHT_KEY_LEN + TIME_KEY_LEN]
            .try_into()
            .map_err(|_| BlockProductionTimeConsumerError::MalformedBackfillCoverage)?,
    );
    let through_time = i64::from_be_bytes(
        payload[2 * HEIGHT_KEY_LEN + TIME_KEY_LEN..]
            .try_into()
            .map_err(|_| BlockProductionTimeConsumerError::MalformedBackfillCoverage)?,
    );
    Ok(BlockProductionTimeBackfillCoverage::new(
        from,
        through,
        from_time,
        through_time,
    ))
}

fn encode_tail_coverage(coverage: BlockProductionTimeTailCoverage) -> [u8; TAIL_COVERAGE_LEN] {
    let mut payload = [0_u8; TAIL_COVERAGE_LEN];
    payload[..HEIGHT_KEY_LEN]
        .copy_from_slice(&encode_height_key_ascending(coverage.boundary_height));
    if let (Some(height), Some(time)) = (
        coverage.complete_through_height,
        coverage.complete_through_time_unix_seconds,
    ) {
        payload[HEIGHT_KEY_LEN] = 1;
        payload[HEIGHT_KEY_LEN + 1..HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN]
            .copy_from_slice(&encode_height_key_ascending(height));
        payload[HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN..].copy_from_slice(&time.to_be_bytes());
    }
    payload
}

fn decode_tail_coverage(
    payload: &[u8],
) -> Result<BlockProductionTimeTailCoverage, BlockProductionTimeConsumerError> {
    if payload.len() != TAIL_COVERAGE_LEN {
        return Err(BlockProductionTimeConsumerError::MalformedTailCoverage);
    }
    let boundary = decode_height_key_ascending(&payload[..HEIGHT_KEY_LEN])
        .map_err(|_| BlockProductionTimeConsumerError::MalformedTailCoverage)?;
    match payload[HEIGHT_KEY_LEN] {
        0 if payload[HEIGHT_KEY_LEN + 1..].iter().all(|byte| *byte == 0) => {
            Ok(BlockProductionTimeTailCoverage::from_boundary(boundary))
        }
        1 => {
            let through = decode_height_key_ascending(
                &payload[HEIGHT_KEY_LEN + 1..HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN],
            )
            .map_err(|_| BlockProductionTimeConsumerError::MalformedTailCoverage)?;
            let time = i64::from_be_bytes(
                payload[HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN..]
                    .try_into()
                    .map_err(|_| BlockProductionTimeConsumerError::MalformedTailCoverage)?,
            );
            if through < boundary {
                return Err(BlockProductionTimeConsumerError::MalformedTailCoverage);
            }
            Ok(BlockProductionTimeTailCoverage {
                boundary_height: boundary,
                complete_through_height: Some(through),
                complete_through_time_unix_seconds: Some(time),
            })
        }
        _ => Err(BlockProductionTimeConsumerError::MalformedTailCoverage),
    }
}

fn coverage_decode_error(error: &BlockProductionTimeConsumerError) -> MaterializedViewStoreError {
    MaterializedViewStoreError::ConsumerPayloadDecode {
        name: BLOCK_PRODUCTION_TIME_COVERAGE_COLUMN_FAMILY,
        reason: error.to_string(),
    }
}

/// Failures surfaced by block production-time materialization and reads.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BlockProductionTimeConsumerError {
    /// A dispatched chain event did not provide one complete materialized-view tip.
    #[error("block-production-time materialized-view checkpoint is incomplete")]
    IncompleteMaterializedViewCheckpoint,
    /// The persisted materialized-view revision cannot advance further.
    #[error("block-production-time materialized-view revision overflow")]
    MaterializedViewRevisionOverflow,
    /// A primary key had the wrong length or invalid components.
    #[error("block-production-time primary key is malformed ({bytes} bytes)")]
    MalformedPrimaryKey {
        /// Stored key length.
        bytes: usize,
    },
    /// A row value had the wrong version or length.
    #[error("block-production-time value is malformed ({bytes} bytes, version {version:?})")]
    MalformedValue {
        /// Stored value length.
        bytes: usize,
        /// Stored version byte, when present.
        version: Option<u8>,
    },
    /// A height index did not contain one exact primary key.
    #[error("block-production-time height index for {height} is malformed ({bytes} bytes)")]
    MalformedHeightIndex {
        /// Indexed height.
        height: u32,
        /// Stored index payload length.
        bytes: usize,
    },
    /// A height index pointed to another height.
    #[error("block-production-time index requested {requested_height} but stores {indexed_height}")]
    IndexHeightMismatch {
        /// Height requested through the reverse index.
        requested_height: u32,
        /// Height encoded in the referenced primary key.
        indexed_height: u32,
    },
    /// A height index pointed to an absent primary row.
    #[error("block-production-time index at height {height} has no row")]
    MissingIndexedRow {
        /// Height whose primary row was absent.
        height: u32,
    },
    /// A primary row existed without its required height index.
    #[error("block-production-time state at height {height} is incomplete")]
    IncompleteHeightState {
        /// Height with only one side of the primary/index pair.
        height: u32,
    },
    /// Existing or batch-local canonical state conflicts with an applied block.
    #[error("block-production-time state at height {height} conflicts with the applied block")]
    ConflictingHeight {
        /// Height claimed by incompatible canonical rows.
        height: u32,
    },
    /// A query did not specify a non-empty half-open range.
    #[error("block-production-time range must be non-empty: [{start}, {end})")]
    InvalidTimeRange {
        /// Inclusive requested timestamp.
        start: i64,
        /// Exclusive requested timestamp.
        end: i64,
    },
    /// A page size was zero or exceeded the hard bound.
    #[error("block-production-time page limit {requested} exceeds valid range 1..={maximum}")]
    PageLimitTooLarge {
        /// Requested page size.
        requested: usize,
        /// Maximum accepted page size.
        maximum: usize,
    },
    /// A scanned-candidate budget was explicitly zero.
    #[error("block-production-time scanned-row budget must be nonzero")]
    InvalidScanBudget,
    /// A continuation cursor did not belong to the requested time range.
    #[error("block-production-time cursor is outside the requested range")]
    CursorOutsideRange,
    /// Full-history coverage had an invalid encoding.
    #[error("block-production-time backfill coverage is malformed")]
    MalformedBackfillCoverage,
    /// Live-tail coverage had an invalid encoding.
    #[error("block-production-time tail coverage is malformed")]
    MalformedTailCoverage,
    /// Backfill or live-tail heights were not contiguous.
    #[error("block-production-time coverage is discontinuous")]
    CoverageDiscontinuous,
    /// A backfill or seed batch contained no blocks.
    #[error("block-production-time backfill batch is empty")]
    EmptyBackfill,
    /// The persisted live-tail boundary differs from the requested boundary.
    #[error("block-production-time tail boundary conflicts at height {boundary_height}")]
    TailBoundaryConflict {
        /// Conflicting requested boundary.
        boundary_height: u32,
    },
    /// Materialized-view store access failed.
    #[error(transparent)]
    Store(#[from] MaterializedViewStoreError),
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rust_rocksdb::WriteBatch;
    use tempfile::tempdir;
    use zinder_core::{
        LockTime, PrivacyShape, TransactionComponentCounts, TransactionFactsArtifact,
        TransactionId, TransactionLocation, TransactionPublicFacts, TransactionVersion,
    };
    use zinder_store::RocksDbResourceBudget;

    use super::*;
    use crate::MaterializedViewStoreOptions;
    use crate::consumer::{BlockCommitInput, TransparentSpendFacts};

    type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

    fn hash(seed: u8) -> BlockHash {
        BlockHash::from_bytes([seed; BLOCK_HASH_LEN])
    }

    fn block(height: u32, hash_seed: u8, time: i64) -> BlockCommitContext {
        let height = BlockHeight::new(height);
        let block_hash = hash(hash_seed);
        let transaction_id = TransactionId::from_bytes([hash_seed.wrapping_add(40); 32]);
        let transaction = TransactionFactsArtifact::new(
            TransactionLocation::new(transaction_id, height, block_hash, 0),
            TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V5,
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 0,
                counts: TransactionComponentCounts::EMPTY,
                privacy_shape: PrivacyShape::Coinbase,
                is_coinbase: true,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                unsupported_sections: Vec::new(),
            },
        );
        BlockCommitContext::new(
            BlockCommitInput {
                height,
                block_hash,
                previous_block_hash: hash(hash_seed.wrapping_sub(1)),
                block_time_unix_seconds: time,
                block_size_bytes: 0,
                transactions: vec![transaction],
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::Offline,
        )
    }

    fn row(height: u32, hash_seed: u8, time: i64) -> BlockProductionTimeRow {
        BlockProductionTimeRow {
            block_time_unix_seconds: time,
            block_height: BlockHeight::new(height),
            block_hash: hash(hash_seed),
        }
    }

    fn open_store() -> TestResult<(tempfile::TempDir, MaterializedViewStore)> {
        let tempdir = tempdir()?;
        let store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[BLOCK_PRODUCTION_TIME_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
                reorg_policy: None,
            },
        )?;
        Ok((tempdir, store))
    }

    fn mutate_blocks(
        store: &MaterializedViewStore,
        consumer: &mut BlockProductionTimeConsumer,
        applies: &[BlockCommitContext],
        reverts: &[BlockHeight],
    ) -> TestResult {
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut ctx)?;
        for height in reverts {
            consumer.revert_block(*height, &mut ctx)?;
        }
        for block in applies {
            consumer.apply_block(block, &mut ctx)?;
        }
        consumer.finish_batch(&mut ctx)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    fn all_rows(
        store: &MaterializedViewStore,
    ) -> Result<Vec<BlockProductionTimeRow>, BlockProductionTimeConsumerError> {
        Ok(BlockProductionTimeConsumer::read_page(
            store,
            BlockProductionTimePageRequest {
                start_time_unix_seconds: i64::MIN,
                end_time_unix_seconds: i64::MAX,
                after: None,
                maximum_height: None,
                limit: BLOCK_PRODUCTION_TIME_MAX_PAGE_SIZE,
                max_scanned_rows: None,
            },
        )?
        .rows)
    }

    #[test]
    fn signed_primary_keys_order_by_time_then_height_then_hash() -> TestResult {
        let negative = encode_primary_key(-1, BlockHeight::new(10), hash(1));
        let zero_low = encode_primary_key(0, BlockHeight::new(9), hash(9));
        let zero_high = encode_primary_key(0, BlockHeight::new(10), hash(1));
        let zero_hash = encode_primary_key(0, BlockHeight::new(10), hash(2));
        assert!(negative < zero_low);
        assert!(zero_low < zero_high);
        assert!(zero_high < zero_hash);
        assert_eq!(decode_signed_time(&encode_signed_time(i64::MIN))?, i64::MIN);
        assert_eq!(decode_signed_time(&encode_signed_time(i64::MAX))?, i64::MAX);
        Ok(())
    }

    #[test]
    fn apply_and_revert_stage_primary_and_reverse_index_atomically() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = BlockProductionTimeConsumer::new();
        BlockProductionTimeConsumer::initialize_tail_boundary(&store, BlockHeight::new(10))?;
        let original = block(10, 1, 100);
        mutate_blocks(&store, &mut consumer, &[original], &[])?;
        assert_eq!(all_rows(&store)?.len(), 1);
        assert_eq!(
            BlockProductionTimeConsumer::tail_coverage(&store)?,
            Some(BlockProductionTimeTailCoverage {
                boundary_height: BlockHeight::new(10),
                complete_through_height: Some(BlockHeight::new(10)),
                complete_through_time_unix_seconds: Some(100),
            })
        );

        let replacement = block(10, 2, 90);
        mutate_blocks(
            &store,
            &mut consumer,
            &[replacement],
            &[BlockHeight::new(10)],
        )?;
        assert_eq!(
            all_rows(&store)?,
            vec![decode_row(
                &encode_primary_key(90, BlockHeight::new(10), hash(2)),
                &encode_value(),
            )?]
        );
        assert_eq!(
            BlockProductionTimeConsumer::tail_coverage(&store)?
                .and_then(|coverage| coverage.complete_through_time_unix_seconds),
            Some(90)
        );

        mutate_blocks(&store, &mut consumer, &[], &[BlockHeight::new(10)])?;
        assert_eq!(all_rows(&store)?, []);
        assert_eq!(
            BlockProductionTimeConsumer::tail_coverage(&store)?,
            Some(BlockProductionTimeTailCoverage::from_boundary(
                BlockHeight::new(10)
            ))
        );
        Ok(())
    }

    #[test]
    fn non_monotonic_block_times_are_retained_in_exact_time_order() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = BlockProductionTimeConsumer::new();
        mutate_blocks(
            &store,
            &mut consumer,
            &[block(10, 1, 100), block(11, 2, 80), block(12, 3, 100)],
            &[],
        )?;
        let rows = all_rows(&store)?;
        assert_eq!(
            rows.iter()
                .map(|row| (row.block_time_unix_seconds, row.block_height.value()))
                .collect::<Vec<_>>(),
            vec![(80, 11), (100, 10), (100, 12)]
        );
        Ok(())
    }

    #[test]
    fn snapshot_pages_are_bounded_and_resume_after_exact_key() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = BlockProductionTimeConsumer::new();
        mutate_blocks(
            &store,
            &mut consumer,
            &[block(10, 1, 100), block(11, 2, 100), block(12, 3, 101)],
            &[],
        )?;
        let snapshot = store.read_snapshot();
        let first = BlockProductionTimeConsumer::read_page_snapshot(
            &snapshot,
            BlockProductionTimePageRequest {
                start_time_unix_seconds: 100,
                end_time_unix_seconds: 102,
                after: None,
                maximum_height: None,
                limit: 2,
                max_scanned_rows: None,
            },
        )?;
        assert_eq!(first.rows.len(), 2);
        let second = BlockProductionTimeConsumer::read_page_snapshot(
            &snapshot,
            BlockProductionTimePageRequest {
                start_time_unix_seconds: 100,
                end_time_unix_seconds: 102,
                after: first.next_cursor,
                maximum_height: None,
                limit: 2,
                max_scanned_rows: None,
            },
        )?;
        assert_eq!(second.rows.len(), 1);
        assert!(second.next_cursor.is_none());
        drop(snapshot);
        Ok(())
    }

    #[test]
    fn frozen_height_pages_skip_later_rows_without_losing_eligible_rows() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = BlockProductionTimeConsumer::new();
        mutate_blocks(
            &store,
            &mut consumer,
            &[
                block(12, 3, 99),
                block(10, 1, 100),
                block(13, 4, 101),
                block(11, 2, 102),
            ],
            &[],
        )?;
        let snapshot = store.read_snapshot();
        let page = BlockProductionTimeConsumer::read_page_snapshot(
            &snapshot,
            BlockProductionTimePageRequest {
                start_time_unix_seconds: 90,
                end_time_unix_seconds: 110,
                after: None,
                maximum_height: Some(BlockHeight::new(11)),
                limit: 10,
                max_scanned_rows: None,
            },
        )?;
        drop(snapshot);
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.block_height.value())
                .collect::<Vec<_>>(),
            vec![10, 11]
        );
        assert!(page.next_cursor.is_none());
        assert!(!page.scan_exhausted);
        Ok(())
    }

    #[test]
    fn max_scanned_rows_bounds_filtered_tail_and_resumes() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = BlockProductionTimeConsumer::new();
        mutate_blocks(
            &store,
            &mut consumer,
            &[
                block(10, 1, 100),
                block(11, 2, 101),
                block(12, 3, 102),
                block(2, 4, 103),
            ],
            &[],
        )?;
        let snapshot = store.read_snapshot();
        let truncated = BlockProductionTimeConsumer::read_page_snapshot(
            &snapshot,
            BlockProductionTimePageRequest {
                start_time_unix_seconds: 100,
                end_time_unix_seconds: 110,
                after: None,
                maximum_height: Some(BlockHeight::new(2)),
                limit: 1,
                max_scanned_rows: Some(2),
            },
        )?;
        assert_eq!(truncated.rows, []);
        assert!(truncated.scan_exhausted);
        assert!(truncated.next_cursor.is_some());

        let resumed = BlockProductionTimeConsumer::read_page_snapshot(
            &snapshot,
            BlockProductionTimePageRequest {
                start_time_unix_seconds: 100,
                end_time_unix_seconds: 110,
                after: truncated.next_cursor,
                maximum_height: Some(BlockHeight::new(2)),
                limit: 1,
                max_scanned_rows: Some(2),
            },
        )?;
        assert!(!resumed.scan_exhausted);
        assert!(resumed.next_cursor.is_none());
        assert_eq!(resumed.rows.len(), 1);
        assert_eq!(resumed.rows[0].block_height, BlockHeight::new(2));

        let invalid = BlockProductionTimeConsumer::read_page_snapshot(
            &snapshot,
            BlockProductionTimePageRequest {
                start_time_unix_seconds: 100,
                end_time_unix_seconds: 110,
                after: None,
                maximum_height: None,
                limit: 1,
                max_scanned_rows: Some(0),
            },
        );
        drop(snapshot);
        assert!(matches!(
            invalid,
            Err(BlockProductionTimeConsumerError::InvalidScanBudget)
        ));
        Ok(())
    }

    #[test]
    fn checked_decoding_rejects_malformed_keys_values_indexes_and_coverage() -> TestResult {
        assert!(decode_primary_key(&[0; PRIMARY_KEY_LEN - 1]).is_err());
        assert!(decode_value(&[]).is_err());
        assert!(decode_value(&[VALUE_VERSION, 0]).is_err());
        let mut unsupported = encode_value();
        unsupported[0] = VALUE_VERSION + 1;
        assert!(decode_value(&unsupported).is_err());
        assert!(decode_backfill_coverage(&[0; BACKFILL_COVERAGE_LEN - 1]).is_err());

        let (_tempdir, store) = open_store()?;
        store.put_consumer(
            BLOCK_PRODUCTION_TIME_INDEX_COLUMN_FAMILY,
            &encode_height_key_ascending(BlockHeight::new(10)),
            &[0; 3],
        )?;
        assert!(matches!(
            read_row_at_height(BlockProductionTimeRead::Store(&store), BlockHeight::new(10)),
            Err(BlockProductionTimeConsumerError::MalformedHeightIndex { .. })
        ));
        Ok(())
    }

    #[test]
    fn row_native_backfill_atomically_builds_primary_index_and_coverage() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let rows = [row(10, 1, 100), row(11, 2, 80)];
        let coverage = BlockProductionTimeBackfillCoverage::new(
            BlockHeight::new(10),
            BlockHeight::new(11),
            100,
            80,
        );
        BlockProductionTimeConsumer::write_backfill_rows(&store, &rows, coverage)?;

        assert_eq!(all_rows(&store)?, vec![rows[1], rows[0]]);
        assert_eq!(
            read_row_at_height(BlockProductionTimeRead::Store(&store), BlockHeight::new(11))?,
            Some(rows[1])
        );
        assert_eq!(
            BlockProductionTimeConsumer::backfill_coverage(&store)?,
            Some(coverage)
        );

        assert!(matches!(
            BlockProductionTimeConsumer::write_backfill_rows(
                &store,
                &[row(13, 3, 70)],
                BlockProductionTimeBackfillCoverage::new(
                    BlockHeight::new(10),
                    BlockHeight::new(13),
                    100,
                    70,
                ),
            ),
            Err(BlockProductionTimeConsumerError::CoverageDiscontinuous)
        ));
        assert!(
            read_row_at_height(BlockProductionTimeRead::Store(&store), BlockHeight::new(13))?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn tip_reduction_rewinds_backfill_coverage_in_the_same_batch() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let rows = [row(10, 1, 100), row(11, 2, 110), row(12, 3, 120)];
        BlockProductionTimeConsumer::write_backfill_rows(
            &store,
            &rows,
            BlockProductionTimeBackfillCoverage::new(
                BlockHeight::new(10),
                BlockHeight::new(12),
                100,
                120,
            ),
        )?;

        let mut consumer = BlockProductionTimeConsumer::new();
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut ctx)?;
        consumer.revert_block(BlockHeight::new(12), &mut ctx)?;
        consumer.finish_batch(&mut ctx)?;
        consumer.stage_backfill_coverage_at_tip(&mut ctx, BlockHeight::new(11))?;
        store.write_batch(&batch)?;

        assert_eq!(
            BlockProductionTimeConsumer::backfill_coverage(&store)?,
            Some(BlockProductionTimeBackfillCoverage::new(
                BlockHeight::new(10),
                BlockHeight::new(11),
                100,
                110,
            ))
        );
        Ok(())
    }

    #[test]
    fn coverage_joins_only_when_backfill_meets_live_tail() -> TestResult {
        let backfill = BlockProductionTimeBackfillCoverage::new(
            BlockHeight::new(0),
            BlockHeight::new(99),
            500,
            400,
        );
        let joined_tail = BlockProductionTimeTailCoverage {
            boundary_height: BlockHeight::new(100),
            complete_through_height: Some(BlockHeight::new(120)),
            complete_through_time_unix_seconds: Some(350),
        };
        assert_eq!(
            join_coverage(Some(backfill), Some(joined_tail))?,
            Some(BlockProductionTimeBackfillCoverage::new(
                BlockHeight::new(0),
                BlockHeight::new(120),
                500,
                350,
            ))
        );
        let gapped_tail = BlockProductionTimeTailCoverage {
            boundary_height: BlockHeight::new(101),
            ..joined_tail
        };
        assert_eq!(
            join_coverage(Some(backfill), Some(gapped_tail))?,
            Some(backfill)
        );
        Ok(())
    }
}
