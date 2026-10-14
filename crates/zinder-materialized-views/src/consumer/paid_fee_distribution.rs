//! Exact positive miner-collected fee frequencies by block time and UTC day.
//!
//! This materialized view combines canonical transparent values with signed
//! transaction-intrinsic shielded value balances. Missing inputs remain
//! explicit unavailable counts; conventional fees are never substituted.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use rust_rocksdb::WriteBatch;
use zinder_core::wire::{
    decode_height_key_ascending, decode_internal_block_hash, encode_height_key_ascending,
    encode_internal_block_hash,
};
use zinder_core::{
    BlockHash, BlockHeight, TransactionFactsArtifact, TransactionId,
    TransactionIntrinsicValueBalances, TransparentOutPoint, TransparentSpendFact,
};
use zinder_proto::capabilities::EXPLORER_PAID_FEE_DISTRIBUTION_V1;

use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewConsumerError, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
};
use crate::{MaterializedViewStore, MaterializedViewStoreError, MaterializedViewStoreReadSnapshot};

/// Per-block miner-collected fee contributions ordered by block time.
pub const PAID_FEE_DISTRIBUTION_COLUMN_FAMILY: &str = "paid_fee_distribution";
/// Per-height contribution keys used for deterministic rewind.
pub const PAID_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY: &str = "paid_fee_distribution_index";
/// One exact frequency aggregate per UTC day.
pub const PAID_FEE_DISTRIBUTION_DAY_COLUMN_FAMILY: &str = "paid_fee_distribution_day";
/// Historical and seeded-live-tail coverage metadata.
pub const PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY: &str = "paid_fee_distribution_coverage";

/// Column families owned by this consumer.
pub const PAID_FEE_DISTRIBUTION_COLUMN_FAMILIES: &[&str] = &[
    PAID_FEE_DISTRIBUTION_COLUMN_FAMILY,
    PAID_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY,
    PAID_FEE_DISTRIBUTION_DAY_COLUMN_FAMILY,
    PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY,
];

/// Stable consumer identity persisted in materialized-view metadata and cursor rows.
pub const PAID_FEE_DISTRIBUTION_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("paid_fee_distribution");

/// Initial consumer-local schema. Existing materialized-view consumers are unaffected.
pub const PAID_FEE_DISTRIBUTION_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        PAID_FEE_DISTRIBUTION_CONSUMER_NAME,
        1,
        PAID_FEE_DISTRIBUTION_COLUMN_FAMILIES,
    );

/// Capability advertised when this materialized view is ready.
pub const PAID_FEE_DISTRIBUTION_CAPABILITIES: &[&str] = &[EXPLORER_PAID_FEE_DISTRIBUTION_V1];

const SECONDS_PER_DAY: i64 = 86_400;
const TIME_KEY_LEN: usize = 8;
const HEIGHT_KEY_LEN: usize = 4;
const BLOCK_HASH_LEN: usize = 32;
const CONTRIBUTION_KEY_LEN: usize = TIME_KEY_LEN + HEIGHT_KEY_LEN + BLOCK_HASH_LEN;
const VALUE_VERSION: u8 = 1;
const VALUE_HEADER_LEN: usize = 1 + TIME_KEY_LEN + size_of::<u64>() + size_of::<u32>();
const FREQUENCY_LEN: usize = 2 * size_of::<u64>();
const COVERAGE_VALUE_LEN: usize = 2 * HEIGHT_KEY_LEN + 2 * TIME_KEY_LEN;
const TAIL_COVERAGE_VALUE_LEN: usize = HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN + TIME_KEY_LEN;
const COVERAGE_KEY: &[u8] = b"canonical_backfill";
const TAIL_COVERAGE_KEY: &[u8] = b"seeded_live_tail";
type RawConsumerEntry = (Vec<u8>, Vec<u8>);

#[derive(Clone, Copy)]
enum PaidFeeDistributionRead<'store> {
    Store(&'store MaterializedViewStore),
    Snapshot(&'store MaterializedViewStoreReadSnapshot<'store>),
}

impl PaidFeeDistributionRead<'_> {
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

/// One exact positive miner-collected fee frequency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaidFeeFrequency {
    /// Exact paid fee in zatoshi.
    pub paid_fee_zat: u64,
    /// Number of transactions having this paid fee.
    pub transaction_count: u64,
}

/// One UTC-day bucket, possibly clipped by a query boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PaidFeeDistributionDay {
    /// UTC midnight as Unix seconds.
    pub day_start_unix_seconds: i64,
    /// Exact frequencies sorted by paid fee ascending.
    pub frequencies: Vec<PaidFeeFrequency>,
    /// Non-coinbase transactions missing a transparent prevout or intrinsic artifact.
    pub unavailable_transaction_count: u64,
}

impl PaidFeeDistributionDay {
    fn empty(day_start_unix_seconds: i64) -> Self {
        Self {
            day_start_unix_seconds,
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.frequencies.is_empty() && self.unavailable_transaction_count == 0
    }

    fn checked_add_assign(&mut self, other: &Self) -> Result<(), PaidFeeDistributionConsumerError> {
        self.require_same_day(other)?;
        let mut merged = BTreeMap::<u64, u64>::new();
        for frequency in self.frequencies.iter().chain(&other.frequencies) {
            let count = merged.entry(frequency.paid_fee_zat).or_default();
            *count = count
                .checked_add(frequency.transaction_count)
                .ok_or(PaidFeeDistributionConsumerError::CounterOverflow)?;
        }
        self.frequencies = frequencies_from_map(merged);
        self.unavailable_transaction_count = self
            .unavailable_transaction_count
            .checked_add(other.unavailable_transaction_count)
            .ok_or(PaidFeeDistributionConsumerError::CounterOverflow)?;
        Ok(())
    }

    fn checked_sub_assign(&mut self, other: &Self) -> Result<(), PaidFeeDistributionConsumerError> {
        self.require_same_day(other)?;
        let mut remaining: BTreeMap<u64, u64> = self
            .frequencies
            .iter()
            .map(|frequency| (frequency.paid_fee_zat, frequency.transaction_count))
            .collect();
        for frequency in &other.frequencies {
            let count = remaining
                .get_mut(&frequency.paid_fee_zat)
                .ok_or(PaidFeeDistributionConsumerError::CounterUnderflow)?;
            *count = count
                .checked_sub(frequency.transaction_count)
                .ok_or(PaidFeeDistributionConsumerError::CounterUnderflow)?;
        }
        remaining.retain(|_, count| *count > 0);
        self.frequencies = frequencies_from_map(remaining);
        self.unavailable_transaction_count = self
            .unavailable_transaction_count
            .checked_sub(other.unavailable_transaction_count)
            .ok_or(PaidFeeDistributionConsumerError::CounterUnderflow)?;
        Ok(())
    }

    fn require_same_day(&self, other: &Self) -> Result<(), PaidFeeDistributionConsumerError> {
        if self.day_start_unix_seconds == other.day_start_unix_seconds {
            Ok(())
        } else {
            Err(PaidFeeDistributionConsumerError::DayMismatch {
                expected: self.day_start_unix_seconds,
                actual: other.day_start_unix_seconds,
            })
        }
    }
}

/// Exact UTC-day buckets for one half-open block-time range.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PaidFeeDistribution {
    /// Non-empty UTC-day buckets in ascending order.
    pub days: Vec<PaidFeeDistributionDay>,
}

/// Exact paid-fee facts for one canonical block contribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaidFeeBlockTotal {
    /// Sum of every proven positive miner-collected transaction fee.
    pub paid_fee_zat: u64,
    /// Non-coinbase transactions whose exact fee could not be proven.
    pub unavailable_transaction_count: u64,
}

/// Contiguous canonical history materialized by backfill and tailing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaidFeeDistributionBackfillCoverage {
    /// First height in the contiguous materialized range.
    pub complete_from_height: BlockHeight,
    /// Last height in the contiguous materialized range.
    pub complete_through_height: BlockHeight,
    /// Block time at the first height.
    pub complete_from_time_unix_seconds: i64,
    /// Block time at the last height.
    pub complete_through_time_unix_seconds: i64,
}

impl PaidFeeDistributionBackfillCoverage {
    /// Creates a contiguous coverage record.
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

/// Durable live-tail interval established when ingest seeds this consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaidFeeDistributionTailCoverage {
    /// First height owned by the seeded live tail.
    pub boundary_height: BlockHeight,
    /// Last contiguous tail height, absent before the first block.
    pub complete_through_height: Option<BlockHeight>,
    /// Block time at the complete-through height.
    pub complete_through_time_unix_seconds: Option<i64>,
}

impl PaidFeeDistributionTailCoverage {
    /// Creates a tail boundary with no materialized blocks.
    #[must_use]
    pub const fn from_boundary(boundary_height: BlockHeight) -> Self {
        Self {
            boundary_height,
            complete_through_height: None,
            complete_through_time_unix_seconds: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BlockContribution {
    block_hash: BlockHash,
    day: PaidFeeDistributionDay,
}

#[derive(Clone, Debug)]
enum DayDelta {
    Add(PaidFeeDistributionDay),
    Subtract(PaidFeeDistributionDay),
}

/// Materializes paid-fee contributions and exact day frequencies.
#[derive(Default)]
pub struct PaidFeeDistributionConsumer {
    pending_height_keys: BTreeMap<BlockHeight, Option<[u8; CONTRIBUTION_KEY_LEN]>>,
    pending_day_deltas: Vec<DayDelta>,
}

impl PaidFeeDistributionConsumer {
    /// Builds an empty consumer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pending_height_keys: BTreeMap::new(),
            pending_day_deltas: Vec::new(),
        }
    }

    /// Queries exact frequencies for the half-open block-time range `[start, end)`.
    pub fn distribution_in_time_range(
        store: &MaterializedViewStore,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
    ) -> Result<PaidFeeDistribution, PaidFeeDistributionConsumerError> {
        Self::read_distribution_in_time_range(
            PaidFeeDistributionRead::Store(store),
            start_time_unix_seconds,
            end_time_unix_seconds,
        )
    }

    /// Queries exact frequencies from one materialized-view read snapshot.
    pub fn distribution_in_time_range_snapshot(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
    ) -> Result<PaidFeeDistribution, PaidFeeDistributionConsumerError> {
        Self::read_distribution_in_time_range(
            PaidFeeDistributionRead::Snapshot(snapshot),
            start_time_unix_seconds,
            end_time_unix_seconds,
        )
    }

    /// Reads one exact block contribution from a materialized-view snapshot.
    pub fn block_total_snapshot(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
        block_time_unix_seconds: i64,
        block_height: BlockHeight,
        block_hash: BlockHash,
    ) -> Result<Option<PaidFeeBlockTotal>, PaidFeeDistributionConsumerError> {
        let key = encode_contribution_key(block_time_unix_seconds, block_height, block_hash);
        let Some(payload) = snapshot.get_consumer(PAID_FEE_DISTRIBUTION_COLUMN_FAMILY, &key)?
        else {
            return Ok(None);
        };
        let day = decode_distribution_value(&payload)?;
        let contribution = BlockContribution { block_hash, day };
        validate_contribution_key(&key, &contribution)?;
        let paid_fee_zat =
            contribution
                .day
                .frequencies
                .iter()
                .try_fold(0_u64, |total, frequency| {
                    let subtotal = frequency
                        .paid_fee_zat
                        .checked_mul(frequency.transaction_count)
                        .ok_or(PaidFeeDistributionConsumerError::CounterOverflow)?;
                    total
                        .checked_add(subtotal)
                        .ok_or(PaidFeeDistributionConsumerError::CounterOverflow)
                })?;
        Ok(Some(PaidFeeBlockTotal {
            paid_fee_zat,
            unavailable_transaction_count: contribution.day.unavailable_transaction_count,
        }))
    }

    fn read_distribution_in_time_range(
        store: PaidFeeDistributionRead<'_>,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
    ) -> Result<PaidFeeDistribution, PaidFeeDistributionConsumerError> {
        if start_time_unix_seconds >= end_time_unix_seconds {
            return Err(PaidFeeDistributionConsumerError::InvalidTimeRange {
                start: start_time_unix_seconds,
                end: end_time_unix_seconds,
            });
        }
        let start_day = utc_day_start(start_time_unix_seconds);
        let last_time = end_time_unix_seconds
            .checked_sub(1)
            .ok_or(PaidFeeDistributionConsumerError::TimeOverflow)?;
        let last_day = utc_day_start(last_time);
        let mut days = BTreeMap::<i64, PaidFeeDistributionDay>::new();

        if start_day == last_day {
            add_contributions_in_range(
                store,
                start_time_unix_seconds,
                end_time_unix_seconds,
                &mut days,
            )?;
        } else {
            let first_day_end = start_day
                .checked_add(SECONDS_PER_DAY)
                .ok_or(PaidFeeDistributionConsumerError::TimeOverflow)?;
            if start_time_unix_seconds == start_day {
                add_stored_day(store, start_day, &mut days)?;
            } else {
                add_contributions_in_range(
                    store,
                    start_time_unix_seconds,
                    first_day_end,
                    &mut days,
                )?;
            }
            let middle_start = first_day_end;
            if middle_start < last_day {
                add_stored_days_in_range(store, middle_start, last_day, &mut days)?;
            }
            let last_day_end = last_day
                .checked_add(SECONDS_PER_DAY)
                .ok_or(PaidFeeDistributionConsumerError::TimeOverflow)?;
            if end_time_unix_seconds == last_day_end {
                add_stored_day(store, last_day, &mut days)?;
            } else {
                add_contributions_in_range(store, last_day, end_time_unix_seconds, &mut days)?;
            }
        }
        Ok(PaidFeeDistribution {
            days: days.into_values().collect(),
        })
    }

    /// Reads contiguous historical coverage, if backfill has started.
    pub fn backfill_coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<PaidFeeDistributionBackfillCoverage>, MaterializedViewStoreError> {
        Self::read_backfill_coverage(PaidFeeDistributionRead::Store(store))
    }

    fn read_backfill_coverage(
        store: PaidFeeDistributionRead<'_>,
    ) -> Result<Option<PaidFeeDistributionBackfillCoverage>, MaterializedViewStoreError> {
        let Some(payload) =
            store.get_consumer(PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY, COVERAGE_KEY)?
        else {
            return Ok(None);
        };
        decode_coverage(&payload)
            .map(Some)
            .map_err(|error| store_decode_error(&error))
    }

    /// Reads the durable live-tail boundary and contiguous tail tip.
    pub fn tail_coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<PaidFeeDistributionTailCoverage>, MaterializedViewStoreError> {
        Self::read_tail_coverage(PaidFeeDistributionRead::Store(store))
    }

    fn read_tail_coverage(
        store: PaidFeeDistributionRead<'_>,
    ) -> Result<Option<PaidFeeDistributionTailCoverage>, MaterializedViewStoreError> {
        let Some(payload) = store.get_consumer(
            PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY,
            TAIL_COVERAGE_KEY,
        )?
        else {
            return Ok(None);
        };
        decode_tail_coverage(&payload)
            .map(Some)
            .map_err(|error| store_decode_error(&error))
    }

    /// Reads complete coverage, joining historical and live-tail intervals.
    pub fn coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<PaidFeeDistributionBackfillCoverage>, MaterializedViewStoreError> {
        Self::read_coverage(PaidFeeDistributionRead::Store(store))
    }

    /// Reads complete coverage from one materialized-view read snapshot.
    pub fn coverage_snapshot(
        snapshot: &MaterializedViewStoreReadSnapshot<'_>,
    ) -> Result<Option<PaidFeeDistributionBackfillCoverage>, MaterializedViewStoreError> {
        Self::read_coverage(PaidFeeDistributionRead::Snapshot(snapshot))
    }

    fn read_coverage(
        store: PaidFeeDistributionRead<'_>,
    ) -> Result<Option<PaidFeeDistributionBackfillCoverage>, MaterializedViewStoreError> {
        let Some(mut coverage) = Self::read_backfill_coverage(store)? else {
            return Self::tail_interval_coverage(store);
        };
        let Some(tail) = Self::read_tail_coverage(store)? else {
            return Ok(Some(coverage));
        };
        let Some(tail_height) = tail.complete_through_height else {
            return Ok(Some(coverage));
        };
        let joins = coverage.complete_through_height >= tail.boundary_height
            || coverage.complete_through_height.next() == Some(tail.boundary_height);
        if joins && tail_height > coverage.complete_through_height {
            coverage.complete_through_height = tail_height;
            coverage.complete_through_time_unix_seconds =
                tail.complete_through_time_unix_seconds.ok_or_else(|| {
                    store_decode_error(&PaidFeeDistributionConsumerError::MalformedTailCoverage)
                })?;
        }
        Ok(Some(coverage))
    }

    /// Synthesizes coverage from the live tail when no backfill has run.
    fn tail_interval_coverage(
        store: PaidFeeDistributionRead<'_>,
    ) -> Result<Option<PaidFeeDistributionBackfillCoverage>, MaterializedViewStoreError> {
        let Some(tail) = Self::read_tail_coverage(store)? else {
            return Ok(None);
        };
        let Some(through_height) = tail.complete_through_height else {
            return Ok(None);
        };
        let through_time = tail.complete_through_time_unix_seconds.ok_or_else(|| {
            store_decode_error(&PaidFeeDistributionConsumerError::MalformedTailCoverage)
        })?;
        let (from_time, _) = height_contribution(store, tail.boundary_height)
            .map_err(|error| store_decode_error(&error))?
            .ok_or_else(|| {
                store_decode_error(
                    &PaidFeeDistributionConsumerError::MissingIndexedContribution {
                        height: tail.boundary_height.value(),
                    },
                )
            })?;
        Ok(Some(PaidFeeDistributionBackfillCoverage::new(
            tail.boundary_height,
            through_height,
            from_time,
            through_time,
        )))
    }

    /// Initializes the first height owned by a seeded live tail.
    pub fn initialize_tail_boundary(
        store: &MaterializedViewStore,
        boundary_height: BlockHeight,
    ) -> Result<(), PaidFeeDistributionConsumerError> {
        let requested = PaidFeeDistributionTailCoverage::from_boundary(boundary_height);
        match Self::tail_coverage(store)? {
            None => store.put_consumer(
                PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY,
                TAIL_COVERAGE_KEY,
                &encode_tail_coverage(requested),
            )?,
            Some(existing) if existing == requested => {}
            Some(_) => {
                return Err(PaidFeeDistributionConsumerError::TailBoundaryConflict {
                    boundary_height: boundary_height.value(),
                });
            }
        }
        Ok(())
    }

    /// Widens an existing startup tail to an earlier canonical boundary.
    pub fn widen_tail_boundary_for_startup(
        store: &MaterializedViewStore,
        boundary_height: BlockHeight,
    ) -> Result<bool, PaidFeeDistributionConsumerError> {
        let Some(existing) = Self::tail_coverage(store)? else {
            Self::initialize_tail_boundary(store, boundary_height)?;
            return Ok(true);
        };
        if boundary_height >= existing.boundary_height {
            return Ok(false);
        }
        store.put_consumer(
            PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY,
            TAIL_COVERAGE_KEY,
            &encode_tail_coverage(PaidFeeDistributionTailCoverage::from_boundary(
                boundary_height,
            )),
        )?;
        Ok(true)
    }

    /// Atomically writes an ordered historical block batch and coverage.
    pub fn write_backfill_batch(
        &mut self,
        store: &MaterializedViewStore,
        blocks: &[BlockCommitContext],
        next_coverage: PaidFeeDistributionBackfillCoverage,
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
            store.consumer_column_family(PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY)?;
        ctx.batch
            .put_cf(&coverage_cf, COVERAGE_KEY, encode_coverage(next_coverage));
        store.write_consumer_batch(PAID_FEE_DISTRIBUTION_SCHEMA.name, ctx.batch)?;
        Ok(())
    }

    /// Atomically seeds canonical blocks at a newly joined live-tail boundary.
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
        store.write_consumer_batch(PAID_FEE_DISTRIBUTION_SCHEMA.name, ctx.batch)?;
        Ok(())
    }

    fn stage_day_aggregates(
        &self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let affected_days: BTreeSet<i64> = self
            .pending_day_deltas
            .iter()
            .map(|delta| match delta {
                DayDelta::Add(day) | DayDelta::Subtract(day) => day.day_start_unix_seconds,
            })
            .collect();
        let day_cf = ctx
            .store
            .consumer_column_family(PAID_FEE_DISTRIBUTION_DAY_COLUMN_FAMILY)?;
        for day_start in affected_days {
            let mut aggregate = read_day(PaidFeeDistributionRead::Store(ctx.store), day_start)?
                .unwrap_or_else(|| PaidFeeDistributionDay::empty(day_start));
            for delta in &self.pending_day_deltas {
                match delta {
                    DayDelta::Add(day) if day.day_start_unix_seconds == day_start => {
                        aggregate.checked_add_assign(day)?;
                    }
                    DayDelta::Subtract(day) if day.day_start_unix_seconds == day_start => {
                        aggregate.checked_sub_assign(day)?;
                    }
                    DayDelta::Add(_) | DayDelta::Subtract(_) => {}
                }
            }
            let key = encode_time_key(day_start);
            if aggregate.is_empty() {
                ctx.batch.delete_cf(&day_cf, key);
            } else {
                ctx.batch
                    .put_cf(&day_cf, key, encode_distribution_value(&aggregate)?);
            }
        }
        Ok(())
    }

    fn stage_tail_coverage(
        &self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let mut tail = if let Some(tail) = Self::tail_coverage(ctx.store)? {
            tail
        } else {
            let Some(boundary_height) = self
                .pending_height_keys
                .iter()
                .find_map(|(height, key)| key.map(|_| *height))
            else {
                return Ok(());
            };
            PaidFeeDistributionTailCoverage::from_boundary(boundary_height)
        };
        while let Some(through) = tail.complete_through_height {
            if height_contribution_after_batch(ctx.store, &self.pending_height_keys, through)?
                .is_some()
            {
                break;
            }
            if through <= tail.boundary_height {
                tail.complete_through_height = None;
                tail.complete_through_time_unix_seconds = None;
                break;
            }
            let previous = BlockHeight::new(through.value() - 1);
            tail.complete_through_height = Some(previous);
            tail.complete_through_time_unix_seconds =
                height_contribution_after_batch(ctx.store, &self.pending_height_keys, previous)?
                    .map(|(time, _)| time);
        }
        loop {
            let candidate = tail
                .complete_through_height
                .map_or(Some(tail.boundary_height), BlockHeight::next);
            let Some(candidate) = candidate else { break };
            let Some((time, _)) =
                height_contribution_after_batch(ctx.store, &self.pending_height_keys, candidate)?
            else {
                break;
            };
            tail.complete_through_height = Some(candidate);
            tail.complete_through_time_unix_seconds = Some(time);
        }
        let coverage_cf = ctx
            .store
            .consumer_column_family(PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY)?;
        ctx.batch
            .put_cf(&coverage_cf, TAIL_COVERAGE_KEY, encode_tail_coverage(tail));
        Ok(())
    }
}

impl BlockKeyedConsumer for PaidFeeDistributionConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        PAID_FEE_DISTRIBUTION_CONSUMER_NAME
    }

    fn begin_batch(
        &mut self,
        _ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        self.pending_height_keys.clear();
        self.pending_day_deltas.clear();
        Ok(())
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let contribution = contribution_for_block(block)?;
        let key = encode_contribution_key(
            block.block_time_unix_seconds,
            block.height,
            block.block_hash,
        );
        let is_new = match self.pending_height_keys.get(&block.height) {
            None => validate_apply_state(ctx.store, block.height, key, &contribution)?,
            Some(None) => true,
            Some(Some(_)) => {
                return Err(Box::new(
                    PaidFeeDistributionConsumerError::DuplicateBatchHeight {
                        height: block.height.value(),
                    },
                ));
            }
        };
        self.pending_height_keys.insert(block.height, Some(key));
        if is_new {
            self.pending_day_deltas
                .push(DayDelta::Add(contribution.day.clone()));
        }
        let contribution_cf = ctx
            .store
            .consumer_column_family(PAID_FEE_DISTRIBUTION_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(PAID_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY)?;
        ctx.batch.put_cf(
            &contribution_cf,
            key,
            encode_distribution_value(&contribution.day)?,
        );
        ctx.batch
            .put_cf(&index_cf, encode_height_key_ascending(block.height), key);
        Ok(())
    }

    fn revert_block(
        &mut self,
        height: BlockHeight,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        if self.pending_height_keys.contains_key(&height) {
            return Err(Box::new(
                PaidFeeDistributionConsumerError::DuplicateBatchHeight {
                    height: height.value(),
                },
            ));
        }
        let index_key = encode_height_key_ascending(height);
        let Some(index_payload) = ctx
            .store
            .get_consumer(PAID_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY, &index_key)?
        else {
            self.pending_height_keys.insert(height, None);
            return Ok(());
        };
        let contribution_key = decode_index_payload(height, &index_payload)?;
        let (_, indexed_height, block_hash) = decode_contribution_key(&contribution_key)?;
        if indexed_height != height {
            return Err(Box::new(
                PaidFeeDistributionConsumerError::IndexHeightMismatch {
                    requested_height: height.value(),
                    indexed_height: indexed_height.value(),
                },
            ));
        }
        let Some(payload) = ctx
            .store
            .get_consumer(PAID_FEE_DISTRIBUTION_COLUMN_FAMILY, &contribution_key)?
        else {
            return Err(Box::new(
                PaidFeeDistributionConsumerError::MissingIndexedContribution {
                    height: height.value(),
                },
            ));
        };
        let contribution = BlockContribution {
            block_hash,
            day: decode_distribution_value(&payload)?,
        };
        validate_contribution_key(&contribution_key, &contribution)?;
        self.pending_height_keys.insert(height, None);
        self.pending_day_deltas
            .push(DayDelta::Subtract(contribution.day));
        let contribution_cf = ctx
            .store
            .consumer_column_family(PAID_FEE_DISTRIBUTION_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(PAID_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY)?;
        ctx.batch.delete_cf(&contribution_cf, contribution_key);
        ctx.batch.delete_cf(&index_cf, index_key);
        Ok(())
    }

    fn finish_batch(
        &mut self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        self.stage_day_aggregates(ctx)?;
        self.stage_tail_coverage(ctx)?;
        self.pending_height_keys.clear();
        self.pending_day_deltas.clear();
        Ok(())
    }
}

fn contribution_for_block(
    block: &BlockCommitContext,
) -> Result<BlockContribution, PaidFeeDistributionConsumerError> {
    let transparent_spends = block.transparent_spends();
    let intrinsic_value_balances = block.transaction_intrinsic_value_balances();
    let mut frequencies = BTreeMap::<u64, u64>::new();
    let mut unavailable = 0_u64;
    for transaction in &block.transactions {
        if transaction.public_facts.is_coinbase {
            continue;
        }
        if let Some(fee) = exact_paid_fee(
            transaction,
            transparent_spends.as_deref(),
            intrinsic_value_balances.as_deref(),
        )? {
            if fee == 0 {
                continue;
            }
            let count = frequencies.entry(fee).or_default();
            *count = count
                .checked_add(1)
                .ok_or(PaidFeeDistributionConsumerError::CounterOverflow)?;
        } else {
            unavailable = unavailable
                .checked_add(1)
                .ok_or(PaidFeeDistributionConsumerError::CounterOverflow)?;
        }
    }
    Ok(BlockContribution {
        block_hash: block.block_hash,
        day: PaidFeeDistributionDay {
            day_start_unix_seconds: utc_day_start(block.block_time_unix_seconds),
            frequencies: frequencies_from_map(frequencies),
            unavailable_transaction_count: unavailable,
        },
    })
}

fn exact_paid_fee(
    transaction: &TransactionFactsArtifact,
    transparent_spends: Option<&HashMap<TransparentOutPoint, TransparentSpendFact>>,
    intrinsic_value_balances: Option<&HashMap<TransactionId, TransactionIntrinsicValueBalances>>,
) -> Result<Option<u64>, PaidFeeDistributionConsumerError> {
    let transaction_id = transaction.location.transaction_id;
    let Some(intrinsic_value_balances) =
        intrinsic_value_balances.and_then(|balances| balances.get(&transaction_id))
    else {
        return Ok(None);
    };

    let mut fee_zat = 0_i128;
    if !transaction.transparent_inputs.is_empty() {
        let Some(transparent_spends) = transparent_spends else {
            return Ok(None);
        };
        for input in &transaction.transparent_inputs {
            let Some(spend) = transparent_spends.get(&input.spent_outpoint) else {
                return Ok(None);
            };
            fee_zat = fee_zat
                .checked_add(i128::from(spend.spent_value_zat))
                .ok_or(PaidFeeDistributionConsumerError::FeeArithmeticOverflow {
                    transaction_id,
                })?;
        }
    }
    for output in &transaction.transparent_outputs {
        fee_zat = fee_zat
            .checked_sub(i128::from(output.value_zat))
            .ok_or(PaidFeeDistributionConsumerError::FeeArithmeticOverflow { transaction_id })?;
    }
    for shielded_value_balance_zat in [
        intrinsic_value_balances.sprout_zat,
        intrinsic_value_balances.sapling_zat,
        intrinsic_value_balances.orchard_zat,
        intrinsic_value_balances.ironwood_zat,
    ] {
        fee_zat = fee_zat
            .checked_add(i128::from(shielded_value_balance_zat))
            .ok_or(PaidFeeDistributionConsumerError::FeeArithmeticOverflow { transaction_id })?;
    }

    u64::try_from(fee_zat)
        .map(Some)
        .map_err(|_| PaidFeeDistributionConsumerError::InvalidPaidFee {
            transaction_id,
            fee_zat,
        })
}

fn frequencies_from_map(frequencies: BTreeMap<u64, u64>) -> Vec<PaidFeeFrequency> {
    frequencies
        .into_iter()
        .map(|(paid_fee_zat, transaction_count)| PaidFeeFrequency {
            paid_fee_zat,
            transaction_count,
        })
        .collect()
}

fn validate_apply_state(
    store: &MaterializedViewStore,
    height: BlockHeight,
    expected_key: [u8; CONTRIBUTION_KEY_LEN],
    expected: &BlockContribution,
) -> Result<bool, PaidFeeDistributionConsumerError> {
    let index = store.get_consumer(
        PAID_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY,
        &encode_height_key_ascending(height),
    )?;
    let contribution = store.get_consumer(PAID_FEE_DISTRIBUTION_COLUMN_FAMILY, &expected_key)?;
    match (index, contribution) {
        (None, None) => Ok(true),
        (Some(index), Some(payload)) => {
            let stored_key = decode_index_payload(height, &index)?;
            let stored = BlockContribution {
                block_hash: decode_contribution_key(&stored_key)?.2,
                day: decode_distribution_value(&payload)?,
            };
            validate_contribution_key(&stored_key, &stored)?;
            if stored_key == expected_key && stored == *expected {
                Ok(false)
            } else {
                Err(PaidFeeDistributionConsumerError::ConflictingHeight {
                    height: height.value(),
                })
            }
        }
        (Some(_), None) | (None, Some(_)) => {
            Err(PaidFeeDistributionConsumerError::IncompleteHeightState {
                height: height.value(),
            })
        }
    }
}

fn height_contribution_after_batch(
    store: &MaterializedViewStore,
    pending: &BTreeMap<BlockHeight, Option<[u8; CONTRIBUTION_KEY_LEN]>>,
    height: BlockHeight,
) -> Result<Option<(i64, BlockHash)>, PaidFeeDistributionConsumerError> {
    if let Some(key) = pending.get(&height) {
        return key
            .map(|key| decode_contribution_key(&key).map(|(time, _, hash)| (time, hash)))
            .transpose();
    }
    height_contribution(PaidFeeDistributionRead::Store(store), height)
}

fn height_contribution(
    store: PaidFeeDistributionRead<'_>,
    height: BlockHeight,
) -> Result<Option<(i64, BlockHash)>, PaidFeeDistributionConsumerError> {
    let Some(index) = store.get_consumer(
        PAID_FEE_DISTRIBUTION_INDEX_COLUMN_FAMILY,
        &encode_height_key_ascending(height),
    )?
    else {
        return Ok(None);
    };
    let key = decode_index_payload(height, &index)?;
    let (time, indexed_height, hash) = decode_contribution_key(&key)?;
    if indexed_height != height {
        return Err(PaidFeeDistributionConsumerError::IndexHeightMismatch {
            requested_height: height.value(),
            indexed_height: indexed_height.value(),
        });
    }
    if store
        .get_consumer(PAID_FEE_DISTRIBUTION_COLUMN_FAMILY, &key)?
        .is_none()
    {
        return Err(
            PaidFeeDistributionConsumerError::MissingIndexedContribution {
                height: height.value(),
            },
        );
    }
    Ok(Some((time, hash)))
}

fn validate_backfill_batch(
    store: &MaterializedViewStore,
    blocks: &[BlockCommitContext],
    next: PaidFeeDistributionBackfillCoverage,
) -> Result<(), MaterializedViewConsumerError> {
    let Some(first) = blocks.first() else {
        return Err(Box::new(PaidFeeDistributionConsumerError::EmptyBackfill));
    };
    let last = blocks
        .last()
        .ok_or(PaidFeeDistributionConsumerError::EmptyBackfill)?;
    if blocks
        .windows(2)
        .any(|pair| pair[0].height.next() != Some(pair[1].height))
        || next.complete_from_height > next.complete_through_height
    {
        return Err(Box::new(
            PaidFeeDistributionConsumerError::CoverageDiscontinuous,
        ));
    }
    let existing = PaidFeeDistributionConsumer::backfill_coverage(store)?;
    if backfill_transition_is_contiguous(existing, first, last, next) {
        Ok(())
    } else {
        Err(Box::new(
            PaidFeeDistributionConsumerError::CoverageDiscontinuous,
        ))
    }
}

fn backfill_transition_is_contiguous(
    existing: Option<PaidFeeDistributionBackfillCoverage>,
    first: &BlockCommitContext,
    last: &BlockCommitContext,
    next: PaidFeeDistributionBackfillCoverage,
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
        return Err(Box::new(PaidFeeDistributionConsumerError::EmptyBackfill));
    };
    if blocks
        .windows(2)
        .any(|pair| pair[0].height.next() != Some(pair[1].height))
    {
        return Err(Box::new(
            PaidFeeDistributionConsumerError::CoverageDiscontinuous,
        ));
    }
    let tail = PaidFeeDistributionConsumer::tail_coverage(store)?.ok_or_else(|| {
        Box::new(PaidFeeDistributionConsumerError::CoverageDiscontinuous)
            as MaterializedViewConsumerError
    })?;
    let expected = tail
        .complete_through_height
        .map_or(Some(tail.boundary_height), BlockHeight::next)
        .ok_or_else(|| {
            Box::new(PaidFeeDistributionConsumerError::CoverageDiscontinuous)
                as MaterializedViewConsumerError
        })?;
    if first.height == expected {
        Ok(())
    } else {
        Err(Box::new(
            PaidFeeDistributionConsumerError::CoverageDiscontinuous,
        ))
    }
}

fn add_contributions_in_range(
    store: PaidFeeDistributionRead<'_>,
    start: i64,
    end: i64,
    days: &mut BTreeMap<i64, PaidFeeDistributionDay>,
) -> Result<(), PaidFeeDistributionConsumerError> {
    for (key, payload) in contribution_entries_in_range(store, start, end)? {
        let contribution = BlockContribution {
            block_hash: decode_contribution_key(&key)?.2,
            day: decode_distribution_value(&payload)?,
        };
        validate_contribution_key(&key, &contribution)?;
        add_day(days, &contribution.day)?;
    }
    Ok(())
}

fn contribution_entries_in_range(
    store: PaidFeeDistributionRead<'_>,
    start: i64,
    end: i64,
) -> Result<Vec<RawConsumerEntry>, PaidFeeDistributionConsumerError> {
    if start >= end {
        return Ok(Vec::new());
    }
    let last_time = end
        .checked_sub(1)
        .ok_or(PaidFeeDistributionConsumerError::TimeOverflow)?;
    Ok(store.range_iterate_consumer(
        PAID_FEE_DISTRIBUTION_COLUMN_FAMILY,
        &encode_contribution_key(start, BlockHeight::new(0), BlockHash::from_bytes([0; 32])),
        &encode_contribution_key(
            last_time,
            BlockHeight::new(u32::MAX),
            BlockHash::from_bytes([u8::MAX; 32]),
        ),
        usize::MAX,
    )?)
}

fn read_day(
    store: PaidFeeDistributionRead<'_>,
    day_start: i64,
) -> Result<Option<PaidFeeDistributionDay>, PaidFeeDistributionConsumerError> {
    let Some(payload) = store.get_consumer(
        PAID_FEE_DISTRIBUTION_DAY_COLUMN_FAMILY,
        &encode_time_key(day_start),
    )?
    else {
        return Ok(None);
    };
    let day = decode_distribution_value(&payload)?;
    if day.day_start_unix_seconds != day_start {
        return Err(PaidFeeDistributionConsumerError::DayMismatch {
            expected: day_start,
            actual: day.day_start_unix_seconds,
        });
    }
    Ok(Some(day))
}

fn add_stored_day(
    store: PaidFeeDistributionRead<'_>,
    day_start: i64,
    days: &mut BTreeMap<i64, PaidFeeDistributionDay>,
) -> Result<(), PaidFeeDistributionConsumerError> {
    if let Some(day) = read_day(store, day_start)? {
        add_day(days, &day)?;
    }
    Ok(())
}

fn add_stored_days_in_range(
    store: PaidFeeDistributionRead<'_>,
    start_day: i64,
    end_day_exclusive: i64,
    days: &mut BTreeMap<i64, PaidFeeDistributionDay>,
) -> Result<(), PaidFeeDistributionConsumerError> {
    let last_day = end_day_exclusive
        .checked_sub(SECONDS_PER_DAY)
        .ok_or(PaidFeeDistributionConsumerError::TimeOverflow)?;
    for (key, payload) in store.range_iterate_consumer(
        PAID_FEE_DISTRIBUTION_DAY_COLUMN_FAMILY,
        &encode_time_key(start_day),
        &encode_time_key(last_day),
        usize::MAX,
    )? {
        let key_day = decode_time_key(&key)?;
        let day = decode_distribution_value(&payload)?;
        if day.day_start_unix_seconds != key_day {
            return Err(PaidFeeDistributionConsumerError::DayMismatch {
                expected: key_day,
                actual: day.day_start_unix_seconds,
            });
        }
        add_day(days, &day)?;
    }
    Ok(())
}

fn add_day(
    days: &mut BTreeMap<i64, PaidFeeDistributionDay>,
    day: &PaidFeeDistributionDay,
) -> Result<(), PaidFeeDistributionConsumerError> {
    let aggregate = days
        .entry(day.day_start_unix_seconds)
        .or_insert_with(|| PaidFeeDistributionDay::empty(day.day_start_unix_seconds));
    aggregate.checked_add_assign(day)
}

fn utc_day_start(unix_seconds: i64) -> i64 {
    unix_seconds.div_euclid(SECONDS_PER_DAY) * SECONDS_PER_DAY
}

fn encode_time_key(unix_seconds: i64) -> [u8; TIME_KEY_LEN] {
    (unix_seconds.cast_unsigned() ^ (1_u64 << 63)).to_be_bytes()
}

fn decode_time_key(key: &[u8]) -> Result<i64, PaidFeeDistributionConsumerError> {
    let bytes: [u8; TIME_KEY_LEN] = key
        .try_into()
        .map_err(|_| PaidFeeDistributionConsumerError::MalformedTimeKey)?;
    Ok((u64::from_be_bytes(bytes) ^ (1_u64 << 63)).cast_signed())
}

fn encode_contribution_key(
    unix_seconds: i64,
    height: BlockHeight,
    block_hash: BlockHash,
) -> [u8; CONTRIBUTION_KEY_LEN] {
    let mut key = [0_u8; CONTRIBUTION_KEY_LEN];
    key[..TIME_KEY_LEN].copy_from_slice(&encode_time_key(unix_seconds));
    key[TIME_KEY_LEN..TIME_KEY_LEN + HEIGHT_KEY_LEN]
        .copy_from_slice(&encode_height_key_ascending(height));
    key[TIME_KEY_LEN + HEIGHT_KEY_LEN..].copy_from_slice(&encode_internal_block_hash(block_hash));
    key
}

fn decode_contribution_key(
    key: &[u8],
) -> Result<(i64, BlockHeight, BlockHash), PaidFeeDistributionConsumerError> {
    if key.len() != CONTRIBUTION_KEY_LEN {
        return Err(PaidFeeDistributionConsumerError::MalformedContributionKey);
    }
    let time = decode_time_key(&key[..TIME_KEY_LEN])?;
    let height = decode_height_key_ascending(&key[TIME_KEY_LEN..TIME_KEY_LEN + HEIGHT_KEY_LEN])
        .map_err(|_| PaidFeeDistributionConsumerError::MalformedContributionKey)?;
    let hash = decode_internal_block_hash(&key[TIME_KEY_LEN + HEIGHT_KEY_LEN..])
        .map_err(|_| PaidFeeDistributionConsumerError::MalformedContributionKey)?;
    Ok((time, height, hash))
}

fn decode_index_payload(
    height: BlockHeight,
    payload: &[u8],
) -> Result<[u8; CONTRIBUTION_KEY_LEN], PaidFeeDistributionConsumerError> {
    payload
        .try_into()
        .map_err(|_| PaidFeeDistributionConsumerError::MalformedHeightIndex {
            height: height.value(),
            bytes: payload.len(),
        })
}

fn validate_contribution_key(
    key: &[u8],
    contribution: &BlockContribution,
) -> Result<(), PaidFeeDistributionConsumerError> {
    let (time, _, hash) = decode_contribution_key(key)?;
    if hash != contribution.block_hash {
        return Err(PaidFeeDistributionConsumerError::ContributionHashMismatch);
    }
    if utc_day_start(time) != contribution.day.day_start_unix_seconds {
        return Err(PaidFeeDistributionConsumerError::ContributionDayMismatch {
            block_time: time,
            day_start: contribution.day.day_start_unix_seconds,
        });
    }
    Ok(())
}

fn encode_distribution_value(
    day: &PaidFeeDistributionDay,
) -> Result<Vec<u8>, PaidFeeDistributionConsumerError> {
    validate_frequencies(&day.frequencies)?;
    let count = u32::try_from(day.frequencies.len())
        .map_err(|_| PaidFeeDistributionConsumerError::TooManyFrequencies)?;
    let mut payload = Vec::with_capacity(VALUE_HEADER_LEN + day.frequencies.len() * FREQUENCY_LEN);
    payload.push(VALUE_VERSION);
    payload.extend_from_slice(&day.day_start_unix_seconds.to_be_bytes());
    payload.extend_from_slice(&day.unavailable_transaction_count.to_be_bytes());
    payload.extend_from_slice(&count.to_be_bytes());
    for frequency in &day.frequencies {
        payload.extend_from_slice(&frequency.paid_fee_zat.to_be_bytes());
        payload.extend_from_slice(&frequency.transaction_count.to_be_bytes());
    }
    Ok(payload)
}

fn decode_distribution_value(
    payload: &[u8],
) -> Result<PaidFeeDistributionDay, PaidFeeDistributionConsumerError> {
    if payload.len() < VALUE_HEADER_LEN || payload[0] != VALUE_VERSION {
        return Err(
            PaidFeeDistributionConsumerError::MalformedDistributionValue {
                bytes: payload.len(),
            },
        );
    }
    let day_start_unix_seconds = i64::from_be_bytes(
        payload[1..=TIME_KEY_LEN]
            .try_into()
            .map_err(|_| malformed_distribution(payload))?,
    );
    if utc_day_start(day_start_unix_seconds) != day_start_unix_seconds {
        return Err(malformed_distribution(payload));
    }
    let unavailable_offset = 1 + TIME_KEY_LEN;
    let unavailable_transaction_count = u64::from_be_bytes(
        payload[unavailable_offset..unavailable_offset + size_of::<u64>()]
            .try_into()
            .map_err(|_| malformed_distribution(payload))?,
    );
    let count_offset = unavailable_offset + size_of::<u64>();
    let frequency_count = u32::from_be_bytes(
        payload[count_offset..count_offset + size_of::<u32>()]
            .try_into()
            .map_err(|_| malformed_distribution(payload))?,
    ) as usize;
    let expected_len = VALUE_HEADER_LEN
        .checked_add(
            frequency_count
                .checked_mul(FREQUENCY_LEN)
                .ok_or_else(|| malformed_distribution(payload))?,
        )
        .ok_or_else(|| malformed_distribution(payload))?;
    if payload.len() != expected_len {
        return Err(malformed_distribution(payload));
    }
    let mut frequencies = Vec::with_capacity(frequency_count);
    for pair in payload[VALUE_HEADER_LEN..].chunks_exact(FREQUENCY_LEN) {
        frequencies.push(PaidFeeFrequency {
            paid_fee_zat: u64::from_be_bytes(
                pair[..size_of::<u64>()]
                    .try_into()
                    .map_err(|_| malformed_distribution(payload))?,
            ),
            transaction_count: u64::from_be_bytes(
                pair[size_of::<u64>()..]
                    .try_into()
                    .map_err(|_| malformed_distribution(payload))?,
            ),
        });
    }
    validate_frequencies(&frequencies).map_err(|_| malformed_distribution(payload))?;
    Ok(PaidFeeDistributionDay {
        day_start_unix_seconds,
        frequencies,
        unavailable_transaction_count,
    })
}

fn validate_frequencies(
    frequencies: &[PaidFeeFrequency],
) -> Result<(), PaidFeeDistributionConsumerError> {
    let mut previous = None;
    for frequency in frequencies {
        if frequency.paid_fee_zat == 0
            || frequency.transaction_count == 0
            || previous.is_some_and(|fee| fee >= frequency.paid_fee_zat)
        {
            return Err(PaidFeeDistributionConsumerError::InvalidFrequencies);
        }
        previous = Some(frequency.paid_fee_zat);
    }
    Ok(())
}

fn malformed_distribution(payload: &[u8]) -> PaidFeeDistributionConsumerError {
    PaidFeeDistributionConsumerError::MalformedDistributionValue {
        bytes: payload.len(),
    }
}

fn encode_coverage(coverage: PaidFeeDistributionBackfillCoverage) -> [u8; COVERAGE_VALUE_LEN] {
    let mut payload = [0_u8; COVERAGE_VALUE_LEN];
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

fn decode_coverage(
    payload: &[u8],
) -> Result<PaidFeeDistributionBackfillCoverage, PaidFeeDistributionConsumerError> {
    if payload.len() != COVERAGE_VALUE_LEN {
        return Err(PaidFeeDistributionConsumerError::MalformedCoverage);
    }
    let from = decode_height_key_ascending(&payload[..HEIGHT_KEY_LEN])
        .map_err(|_| PaidFeeDistributionConsumerError::MalformedCoverage)?;
    let through = decode_height_key_ascending(&payload[HEIGHT_KEY_LEN..2 * HEIGHT_KEY_LEN])
        .map_err(|_| PaidFeeDistributionConsumerError::MalformedCoverage)?;
    let from_time = i64::from_be_bytes(
        payload[2 * HEIGHT_KEY_LEN..2 * HEIGHT_KEY_LEN + TIME_KEY_LEN]
            .try_into()
            .map_err(|_| PaidFeeDistributionConsumerError::MalformedCoverage)?,
    );
    let through_time = i64::from_be_bytes(
        payload[2 * HEIGHT_KEY_LEN + TIME_KEY_LEN..]
            .try_into()
            .map_err(|_| PaidFeeDistributionConsumerError::MalformedCoverage)?,
    );
    if from > through {
        return Err(PaidFeeDistributionConsumerError::MalformedCoverage);
    }
    Ok(PaidFeeDistributionBackfillCoverage::new(
        from,
        through,
        from_time,
        through_time,
    ))
}

fn encode_tail_coverage(
    coverage: PaidFeeDistributionTailCoverage,
) -> [u8; TAIL_COVERAGE_VALUE_LEN] {
    let mut payload = [0_u8; TAIL_COVERAGE_VALUE_LEN];
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
) -> Result<PaidFeeDistributionTailCoverage, PaidFeeDistributionConsumerError> {
    if payload.len() != TAIL_COVERAGE_VALUE_LEN {
        return Err(PaidFeeDistributionConsumerError::MalformedTailCoverage);
    }
    let boundary = decode_height_key_ascending(&payload[..HEIGHT_KEY_LEN])
        .map_err(|_| PaidFeeDistributionConsumerError::MalformedTailCoverage)?;
    match payload[HEIGHT_KEY_LEN] {
        0 if payload[HEIGHT_KEY_LEN + 1..].iter().all(|byte| *byte == 0) => {
            Ok(PaidFeeDistributionTailCoverage::from_boundary(boundary))
        }
        1 => {
            let through = decode_height_key_ascending(
                &payload[HEIGHT_KEY_LEN + 1..HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN],
            )
            .map_err(|_| PaidFeeDistributionConsumerError::MalformedTailCoverage)?;
            let time = i64::from_be_bytes(
                payload[HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN..]
                    .try_into()
                    .map_err(|_| PaidFeeDistributionConsumerError::MalformedTailCoverage)?,
            );
            if through < boundary {
                return Err(PaidFeeDistributionConsumerError::MalformedTailCoverage);
            }
            Ok(PaidFeeDistributionTailCoverage {
                boundary_height: boundary,
                complete_through_height: Some(through),
                complete_through_time_unix_seconds: Some(time),
            })
        }
        _ => Err(PaidFeeDistributionConsumerError::MalformedTailCoverage),
    }
}

fn store_decode_error(error: &PaidFeeDistributionConsumerError) -> MaterializedViewStoreError {
    MaterializedViewStoreError::ConsumerPayloadDecode {
        name: PAID_FEE_DISTRIBUTION_COVERAGE_COLUMN_FAMILY,
        reason: error.to_string(),
    }
}

/// Failures surfaced by paid-fee materialization and reads.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PaidFeeDistributionConsumerError {
    /// Checked fee arithmetic exceeded the internal accumulator.
    #[error("paid-fee arithmetic overflowed for transaction {transaction_id:?}")]
    FeeArithmeticOverflow {
        /// Transaction whose facts overflowed.
        transaction_id: TransactionId,
    },
    /// Complete canonical facts produced a negative or out-of-range fee.
    #[error("invalid paid fee {fee_zat} for transaction {transaction_id:?}")]
    InvalidPaidFee {
        /// Transaction whose facts were inconsistent.
        transaction_id: TransactionId,
        /// Signed aggregate that could not be represented as a fee.
        fee_zat: i128,
    },
    /// A query did not specify a non-empty half-open range.
    #[error("paid-fee time range must be non-empty: [{start}, {end})")]
    InvalidTimeRange {
        /// Inclusive range start.
        start: i64,
        /// Exclusive range end.
        end: i64,
    },
    /// UTC-day or endpoint arithmetic overflowed.
    #[error("paid-fee time arithmetic overflowed")]
    TimeOverflow,
    /// A frequency counter overflowed.
    #[error("paid-fee counter overflowed u64")]
    CounterOverflow,
    /// A subtraction was not represented by the stored aggregate.
    #[error("paid-fee counter underflowed")]
    CounterUnderflow,
    /// Two values claim different UTC days.
    #[error("paid-fee day mismatch: expected {expected}, got {actual}")]
    DayMismatch {
        /// Expected UTC-day start.
        expected: i64,
        /// Encoded UTC-day start.
        actual: i64,
    },
    /// Frequency entries were not non-zero and strictly ascending.
    #[error("paid-fee frequencies are not canonical")]
    InvalidFrequencies,
    /// A frequency vector cannot be represented by the codec.
    #[error("paid-fee frequency count exceeds u32")]
    TooManyFrequencies,
    /// A contribution key had the wrong shape.
    #[error("paid-fee contribution key is malformed")]
    MalformedContributionKey,
    /// A signed-time key had the wrong shape.
    #[error("paid-fee time key is malformed")]
    MalformedTimeKey,
    /// A contribution or day aggregate had an invalid encoding.
    #[error("paid-fee distribution value is malformed ({bytes} bytes)")]
    MalformedDistributionValue {
        /// Stored payload length.
        bytes: usize,
    },
    /// Historical coverage had an invalid encoding.
    #[error("paid-fee coverage value is malformed")]
    MalformedCoverage,
    /// Seeded live-tail coverage had an invalid encoding.
    #[error("paid-fee tail coverage value is malformed")]
    MalformedTailCoverage,
    /// A per-height index did not encode one contribution key.
    #[error("paid-fee height index for {height} has invalid length {bytes}")]
    MalformedHeightIndex {
        /// Indexed height.
        height: u32,
        /// Stored payload length.
        bytes: usize,
    },
    /// A contribution key and payload claim different UTC days.
    #[error("paid-fee contribution at {block_time} claims UTC day {day_start}")]
    ContributionDayMismatch {
        /// Block time encoded by the contribution key.
        block_time: i64,
        /// UTC day encoded by the contribution payload.
        day_start: i64,
    },
    /// A contribution key and canonical block hash disagree.
    #[error("paid-fee contribution hash does not match its key")]
    ContributionHashMismatch,
    /// A height index pointed to another height.
    #[error("paid-fee height index requested {requested_height} but stores {indexed_height}")]
    IndexHeightMismatch {
        /// Requested rewind height.
        requested_height: u32,
        /// Height encoded in the contribution key.
        indexed_height: u32,
    },
    /// A height index pointed to an absent contribution.
    #[error("paid-fee index at height {height} has no contribution")]
    MissingIndexedContribution {
        /// Indexed height.
        height: u32,
    },
    /// Only one of a height index and contribution existed.
    #[error("paid-fee state at height {height} is incomplete")]
    IncompleteHeightState {
        /// Incomplete height.
        height: u32,
    },
    /// Existing canonical state conflicts with the applied block.
    #[error("paid-fee state at height {height} conflicts with the applied block")]
    ConflictingHeight {
        /// Conflicting height.
        height: u32,
    },
    /// One batch attempted to handle a height twice.
    #[error("paid-fee batch contains height {height} more than once")]
    DuplicateBatchHeight {
        /// Repeated height.
        height: u32,
    },
    /// Backfill or tail coverage is not contiguous.
    #[error("paid-fee coverage is discontinuous")]
    CoverageDiscontinuous,
    /// A backfill or seed batch was empty.
    #[error("paid-fee backfill batch is empty")]
    EmptyBackfill,
    /// A seeded tail was initialized with a conflicting boundary.
    #[error("paid-fee tail boundary conflicts at height {boundary_height}")]
    TailBoundaryConflict {
        /// Requested boundary height.
        boundary_height: u32,
    },
    /// Materialized-view store access failed.
    #[error(transparent)]
    Store(#[from] MaterializedViewStoreError),
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error, sync::Arc};

    use rust_rocksdb::WriteBatch;
    use tempfile::tempdir;
    use zinder_core::{
        LockTime, PrivacyShape, TransactionComponentCounts, TransactionFactsArtifact,
        TransactionId, TransactionIntrinsicValueBalances, TransactionLocation,
        TransactionPublicFacts, TransactionVersion, TransparentAddressScriptHash,
        TransparentInputFact, TransparentOutPoint, TransparentOutputFact, TransparentSpendFact,
    };
    use zinder_store::RocksDbResourceBudget;

    use super::*;
    use crate::MaterializedViewStoreOptions;
    use crate::consumer::{
        BlockCommitInput, TransactionIntrinsicValueBalanceFacts, TransparentSpendFacts,
    };

    type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

    fn block_hash(seed: u8) -> BlockHash {
        BlockHash::from_bytes([seed; 32])
    }

    fn transaction(height: BlockHeight, hash: BlockHash, index: u32) -> TransactionFactsArtifact {
        let mut transaction_id_bytes = [0_u8; 32];
        transaction_id_bytes[..4].copy_from_slice(&height.value().to_be_bytes());
        transaction_id_bytes[4..8].copy_from_slice(&index.to_be_bytes());
        let transaction_id = TransactionId::from_bytes(transaction_id_bytes);
        let counts = TransactionComponentCounts::EMPTY;
        TransactionFactsArtifact::new(
            TransactionLocation::new(transaction_id, height, hash, index),
            TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V5,
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 0,
                counts,
                privacy_shape: PrivacyShape::ShieldedOnly,
                is_coinbase: false,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                unsupported_sections: Vec::new(),
            },
        )
    }

    fn block(
        height: u32,
        hash_seed: u8,
        block_time_unix_seconds: i64,
        paid_fees_zat: &[Option<u64>],
    ) -> BlockCommitContext {
        let height = BlockHeight::new(height);
        let hash = block_hash(hash_seed);
        let transactions: Vec<_> = paid_fees_zat
            .iter()
            .enumerate()
            .map(|(index, _)| transaction(height, hash, u32::try_from(index).unwrap_or(u32::MAX)))
            .collect();
        let intrinsic_value_balances = transactions
            .iter()
            .zip(paid_fees_zat)
            .filter_map(|(transaction, paid_fee_zat)| {
                paid_fee_zat.map(|paid_fee_zat| {
                    (
                        transaction.location.transaction_id,
                        TransactionIntrinsicValueBalances::new(
                            i64::try_from(paid_fee_zat).unwrap_or(i64::MAX),
                            0,
                            0,
                            0,
                        ),
                    )
                })
            })
            .collect();
        BlockCommitContext::new(
            BlockCommitInput {
                height,
                block_hash: hash,
                previous_block_hash: block_hash(hash_seed.wrapping_sub(1)),
                block_time_unix_seconds,
                block_size_bytes: 0,
                transactions,
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::Offline,
        )
        .with_transaction_intrinsic_value_balances(
            TransactionIntrinsicValueBalanceFacts::from_map(Arc::new(intrinsic_value_balances)),
        )
    }

    fn transparent_transaction(
        transparent_input_value_zat: u64,
        transparent_output_value_zat: u64,
    ) -> (
        TransactionFactsArtifact,
        HashMap<TransparentOutPoint, TransparentSpendFact>,
    ) {
        let height = BlockHeight::new(100);
        let canonical_block_hash = block_hash(7);
        let transaction_id = TransactionId::from_bytes([8; 32]);
        let spent_outpoint = TransparentOutPoint::new(TransactionId::from_bytes([9; 32]), 0);
        let counts = TransactionComponentCounts {
            transparent_input_count: 1,
            transparent_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let transaction = TransactionFactsArtifact::new(
            TransactionLocation::new(transaction_id, height, canonical_block_hash, 1),
            TransactionPublicFacts {
                transaction_id,
                auth_digest: None,
                wtxid: None,
                version: TransactionVersion::V5,
                consensus_branch_id: None,
                lock_time: LockTime::Unlocked,
                expiry_height: None,
                size_bytes: 0,
                counts,
                privacy_shape: PrivacyShape::TransparentOnly,
                is_coinbase: false,
                orchard_value_balance_zat: None,
                orchard_anchor: None,
                ironwood_value_balance_zat: None,
                unsupported_sections: Vec::new(),
            },
        )
        .with_transparent_facts(
            vec![TransparentInputFact::new(0, spent_outpoint)],
            vec![TransparentOutputFact::new(
                0,
                transparent_output_value_zat,
                b"script".to_vec(),
                TransparentAddressScriptHash::from_bytes([10; 32]),
            )],
        );
        let transparent_spends = HashMap::from([(
            spent_outpoint,
            TransparentSpendFact::new(
                spent_outpoint,
                0,
                transaction_id,
                1,
                height,
                canonical_block_hash,
                transparent_input_value_zat,
                TransparentAddressScriptHash::from_bytes([11; 32]),
                BlockHeight::new(50),
                block_hash(6),
            ),
        )]);
        (transaction, transparent_spends)
    }

    fn open_store() -> TestResult<(tempfile::TempDir, MaterializedViewStore)> {
        let tempdir = tempdir()?;
        let store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[PAID_FEE_DISTRIBUTION_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
            },
        )?;
        Ok((tempdir, store))
    }

    fn write_blocks(
        store: &MaterializedViewStore,
        consumer: &mut PaidFeeDistributionConsumer,
        blocks: &[BlockCommitContext],
    ) -> TestResult {
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut ctx)?;
        for block in blocks {
            consumer.apply_block(block, &mut ctx)?;
        }
        consumer.finish_batch(&mut ctx)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    fn revert_blocks(
        store: &MaterializedViewStore,
        consumer: &mut PaidFeeDistributionConsumer,
        heights: &[BlockHeight],
    ) -> TestResult {
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut ctx)?;
        for height in heights {
            consumer.revert_block(*height, &mut ctx)?;
        }
        consumer.finish_batch(&mut ctx)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    fn replace_block(
        store: &MaterializedViewStore,
        consumer: &mut PaidFeeDistributionConsumer,
        height: BlockHeight,
        replacement: &BlockCommitContext,
    ) -> TestResult {
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut ctx)?;
        consumer.revert_block(height, &mut ctx)?;
        consumer.apply_block(replacement, &mut ctx)?;
        consumer.finish_batch(&mut ctx)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    fn day(entries: &[(u64, u64)], unavailable: u64) -> PaidFeeDistributionDay {
        PaidFeeDistributionDay {
            day_start_unix_seconds: 86_400,
            frequencies: entries
                .iter()
                .map(|&(fee, count)| PaidFeeFrequency {
                    paid_fee_zat: fee,
                    transaction_count: count,
                })
                .collect(),
            unavailable_transaction_count: unavailable,
        }
    }

    #[test]
    fn frequency_merge_and_subtract_are_exact_and_sorted() -> TestResult {
        let mut aggregate = day(&[(10_000, 2), (30_000, 1)], 2);
        let contribution = day(&[(10_000, 1), (20_000, 4)], 1);
        aggregate.checked_add_assign(&contribution)?;
        assert_eq!(aggregate, day(&[(10_000, 3), (20_000, 4), (30_000, 1)], 3));
        aggregate.checked_sub_assign(&contribution)?;
        assert_eq!(aggregate, day(&[(10_000, 2), (30_000, 1)], 2));
        Ok(())
    }

    #[test]
    fn frequency_subtract_rejects_underflow() {
        let mut aggregate = day(&[(10_000, 1)], 0);
        assert!(matches!(
            aggregate.checked_sub_assign(&day(&[(10_000, 2)], 0)),
            Err(PaidFeeDistributionConsumerError::CounterUnderflow)
        ));
    }

    #[test]
    fn distribution_codec_round_trips_canonical_frequencies() -> TestResult {
        let expected = day(&[(10_000, 2), (25_000, 7)], 3);
        let payload = encode_distribution_value(&expected)?;
        assert_eq!(decode_distribution_value(&payload)?, expected);
        Ok(())
    }

    #[test]
    fn distribution_codec_rejects_noncanonical_payloads() -> TestResult {
        let mut payload = encode_distribution_value(&day(&[(10_000, 2), (25_000, 7)], 0))?;
        let second_fee_offset = VALUE_HEADER_LEN + FREQUENCY_LEN;
        payload[second_fee_offset..second_fee_offset + size_of::<u64>()]
            .copy_from_slice(&10_000_u64.to_be_bytes());
        assert!(matches!(
            decode_distribution_value(&payload),
            Err(PaidFeeDistributionConsumerError::MalformedDistributionValue { .. })
        ));
        payload.pop();
        assert!(decode_distribution_value(&payload).is_err());
        Ok(())
    }

    #[test]
    fn signed_time_keys_sort_chronologically_across_epoch() -> TestResult {
        assert!(encode_time_key(-1) < encode_time_key(0));
        assert!(encode_time_key(0) < encode_time_key(1));
        assert_eq!(decode_time_key(&encode_time_key(i64::MIN))?, i64::MIN);
        assert_eq!(decode_time_key(&encode_time_key(i64::MAX))?, i64::MAX);
        Ok(())
    }

    #[test]
    fn coverage_codecs_are_strict() -> TestResult {
        let coverage = PaidFeeDistributionBackfillCoverage::new(
            BlockHeight::new(10),
            BlockHeight::new(20),
            -1,
            100,
        );
        assert_eq!(decode_coverage(&encode_coverage(coverage))?, coverage);
        assert!(decode_coverage(&[0; COVERAGE_VALUE_LEN - 1]).is_err());

        let tail = PaidFeeDistributionTailCoverage {
            boundary_height: BlockHeight::new(21),
            complete_through_height: Some(BlockHeight::new(25)),
            complete_through_time_unix_seconds: Some(200),
        };
        assert_eq!(decode_tail_coverage(&encode_tail_coverage(tail))?, tail);
        let mut malformed = encode_tail_coverage(PaidFeeDistributionTailCoverage::from_boundary(
            BlockHeight::new(21),
        ));
        malformed[HEIGHT_KEY_LEN + 1] = 1;
        assert!(decode_tail_coverage(&malformed).is_err());
        Ok(())
    }

    #[test]
    fn backfill_coverage_accepts_contiguous_append_and_prepend() {
        let first = block(10, 10, 1_000, &[]);
        let last = block(11, 11, 1_100, &[]);
        let initial = PaidFeeDistributionBackfillCoverage::new(
            BlockHeight::new(10),
            BlockHeight::new(11),
            1_000,
            1_100,
        );
        assert!(backfill_transition_is_contiguous(
            None, &first, &last, initial
        ));

        let appended = block(12, 12, 1_200, &[]);
        assert!(backfill_transition_is_contiguous(
            Some(initial),
            &appended,
            &appended,
            PaidFeeDistributionBackfillCoverage::new(
                BlockHeight::new(10),
                BlockHeight::new(12),
                1_000,
                1_200,
            ),
        ));

        let prepended = block(9, 9, 900, &[]);
        assert!(backfill_transition_is_contiguous(
            Some(initial),
            &prepended,
            &prepended,
            PaidFeeDistributionBackfillCoverage::new(
                BlockHeight::new(9),
                BlockHeight::new(11),
                900,
                1_100,
            ),
        ));

        let gap = block(8, 8, 800, &[]);
        assert!(!backfill_transition_is_contiguous(
            Some(initial),
            &gap,
            &gap,
            PaidFeeDistributionBackfillCoverage::new(
                BlockHeight::new(8),
                BlockHeight::new(11),
                800,
                1_100,
            ),
        ));
    }

    #[test]
    fn utc_day_uses_euclidean_division() {
        assert_eq!(utc_day_start(-1), -86_400);
        assert_eq!(utc_day_start(0), 0);
        assert_eq!(utc_day_start(86_399), 0);
        assert_eq!(utc_day_start(86_400), 86_400);
    }

    #[test]
    fn exact_fee_combines_transparent_and_all_intrinsic_balances() -> TestResult {
        let (transaction, transparent_spends) = transparent_transaction(50_000, 40_000);
        let intrinsic_value_balances = HashMap::from([(
            transaction.location.transaction_id,
            TransactionIntrinsicValueBalances::new(5_000, -2_000, 30_000, -3_000),
        )]);

        assert_eq!(
            exact_paid_fee(
                &transaction,
                Some(&transparent_spends),
                Some(&intrinsic_value_balances),
            )?,
            Some(40_000)
        );
        Ok(())
    }

    #[test]
    fn exact_fee_counts_sprout_to_transparent_value_once() -> TestResult {
        let height = BlockHeight::new(100);
        let block_hash = block_hash(7);
        let mut transaction = transaction(height, block_hash, 1);
        transaction.transparent_outputs = vec![TransparentOutputFact::new(
            0,
            101_000_000,
            b"script".to_vec(),
            TransparentAddressScriptHash::from_bytes([10; 32]),
        )];
        let intrinsic_value_balances = HashMap::from([(
            transaction.location.transaction_id,
            TransactionIntrinsicValueBalances::new(101_010_000, 0, 0, 0),
        )]);

        assert_eq!(
            exact_paid_fee(&transaction, None, Some(&intrinsic_value_balances))?,
            Some(10_000)
        );
        Ok(())
    }

    #[test]
    fn exact_fee_cancels_transparent_sprout_deposit_value() -> TestResult {
        let (transaction, transparent_spends) = transparent_transaction(101_010_000, 0);
        let intrinsic_value_balances = HashMap::from([(
            transaction.location.transaction_id,
            TransactionIntrinsicValueBalances::new(-101_010_000, 0, 0, 0),
        )]);

        assert_eq!(
            exact_paid_fee(
                &transaction,
                Some(&transparent_spends),
                Some(&intrinsic_value_balances),
            )?,
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn missing_prevout_or_intrinsic_for_transparent_only_is_unavailable() -> TestResult {
        let (transaction, _transparent_spends) = transparent_transaction(50_000, 40_000);
        assert_eq!(
            transaction.public_facts.privacy_shape,
            PrivacyShape::TransparentOnly
        );
        let intrinsic_value_balances = HashMap::from([(
            transaction.location.transaction_id,
            TransactionIntrinsicValueBalances::default(),
        )]);

        assert_eq!(
            exact_paid_fee(&transaction, None, Some(&intrinsic_value_balances))?,
            None
        );
        assert_eq!(exact_paid_fee(&transaction, None, None)?, None);
        Ok(())
    }

    #[test]
    fn proven_zero_fee_is_excluded_without_becoming_unavailable() -> TestResult {
        let contribution = contribution_for_block(&block(100, 1, 10, &[Some(0)]))?;

        assert!(contribution.day.frequencies.is_empty());
        assert_eq!(contribution.day.unavailable_transaction_count, 0);
        Ok(())
    }

    #[test]
    fn missing_intrinsic_for_transparent_only_increments_unavailable_count() -> TestResult {
        let (transaction, transparent_spends) = transparent_transaction(50_000, 40_000);
        let block = BlockCommitContext::new(
            BlockCommitInput {
                height: BlockHeight::new(100),
                block_hash: block_hash(7),
                previous_block_hash: block_hash(6),
                block_time_unix_seconds: 10,
                block_size_bytes: 0,
                transactions: vec![transaction],
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::from_map(Arc::new(transparent_spends)),
        )
        .with_transaction_intrinsic_value_balances(
            TransactionIntrinsicValueBalanceFacts::from_map(Arc::new(HashMap::new())),
        );

        let contribution = contribution_for_block(&block)?;
        assert!(contribution.day.frequencies.is_empty());
        assert_eq!(contribution.day.unavailable_transaction_count, 1);
        Ok(())
    }

    #[test]
    fn complete_facts_with_negative_fee_fail_closed() {
        let (transaction, transparent_spends) = transparent_transaction(10_000, 40_000);
        let intrinsic_value_balances = HashMap::from([(
            transaction.location.transaction_id,
            TransactionIntrinsicValueBalances::default(),
        )]);

        assert!(matches!(
            exact_paid_fee(
                &transaction,
                Some(&transparent_spends),
                Some(&intrinsic_value_balances),
            ),
            Err(PaidFeeDistributionConsumerError::InvalidPaidFee {
                fee_zat: -30_000,
                ..
            })
        ));
    }

    #[test]
    fn contribution_excludes_coinbase_transactions() -> TestResult {
        let mut coinbase = transaction(BlockHeight::new(100), block_hash(1), 0);
        coinbase.public_facts.is_coinbase = true;
        let block = BlockCommitContext::new(
            BlockCommitInput {
                height: BlockHeight::new(100),
                block_hash: block_hash(1),
                previous_block_hash: block_hash(0),
                block_time_unix_seconds: 10,
                block_size_bytes: 0,
                transactions: vec![coinbase],
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::Offline,
        );

        let contribution = contribution_for_block(&block)?;
        assert!(contribution.day.frequencies.is_empty());
        assert_eq!(contribution.day.unavailable_transaction_count, 0);
        Ok(())
    }

    #[test]
    fn clipped_query_is_half_open_and_tracks_unavailable_transactions() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = PaidFeeDistributionConsumer::new();
        write_blocks(
            &store,
            &mut consumer,
            &[
                block(100, 1, 10, &[Some(10_000)]),
                block(101, 2, 20, &[Some(20_000), None]),
                block(102, 3, SECONDS_PER_DAY, &[Some(10_000)]),
            ],
        )?;

        let distribution =
            PaidFeeDistributionConsumer::distribution_in_time_range(&store, 11, SECONDS_PER_DAY)?;
        assert_eq!(distribution.days.len(), 1);
        assert_eq!(
            distribution.days[0],
            PaidFeeDistributionDay {
                day_start_unix_seconds: 0,
                frequencies: vec![PaidFeeFrequency {
                    paid_fee_zat: 20_000,
                    transaction_count: 1,
                }],
                unavailable_transaction_count: 1,
            }
        );
        Ok(())
    }

    #[test]
    fn revert_subtracts_only_the_reverted_block_frequency() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = PaidFeeDistributionConsumer::new();
        write_blocks(
            &store,
            &mut consumer,
            &[
                block(100, 1, 10, &[Some(10_000)]),
                block(101, 2, 20, &[Some(25_000)]),
            ],
        )?;
        revert_blocks(&store, &mut consumer, &[BlockHeight::new(100)])?;

        let distribution =
            PaidFeeDistributionConsumer::distribution_in_time_range(&store, 0, SECONDS_PER_DAY)?;
        assert_eq!(distribution.days.len(), 1);
        assert_eq!(
            distribution.days[0].frequencies,
            vec![PaidFeeFrequency {
                paid_fee_zat: 25_000,
                transaction_count: 1,
            }]
        );
        Ok(())
    }

    #[test]
    fn fresh_replay_self_initializes_and_advances_the_live_tail() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = PaidFeeDistributionConsumer::new();
        assert!(PaidFeeDistributionConsumer::tail_coverage(&store)?.is_none());

        write_blocks(&store, &mut consumer, &[block(100, 1, 10, &[Some(10_000)])])?;
        let seeded = PaidFeeDistributionConsumer::tail_coverage(&store)?
            .ok_or("replay must self-initialize the live tail")?;
        assert_eq!(seeded.boundary_height, BlockHeight::new(100));
        assert_eq!(seeded.complete_through_height, Some(BlockHeight::new(100)));

        write_blocks(&store, &mut consumer, &[block(101, 2, 20, &[Some(20_000)])])?;
        let advanced = PaidFeeDistributionConsumer::tail_coverage(&store)?
            .ok_or("the live tail must persist across replay batches")?;
        assert_eq!(advanced.boundary_height, BlockHeight::new(100));
        assert_eq!(
            advanced.complete_through_height,
            Some(BlockHeight::new(101))
        );
        Ok(())
    }

    #[test]
    fn coverage_falls_back_to_the_live_tail_without_backfill() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = PaidFeeDistributionConsumer::new();
        assert!(PaidFeeDistributionConsumer::coverage(&store)?.is_none());

        write_blocks(
            &store,
            &mut consumer,
            &[
                block(1, 1, 1_700_000_000, &[Some(10_000)]),
                block(2, 2, 1_700_000_600, &[Some(20_000)]),
            ],
        )?;
        assert!(PaidFeeDistributionConsumer::backfill_coverage(&store)?.is_none());

        let coverage = PaidFeeDistributionConsumer::coverage(&store)?
            .ok_or("coverage must fall back to the live tail")?;
        assert_eq!(coverage.complete_from_height, BlockHeight::new(1));
        assert_eq!(coverage.complete_through_height, BlockHeight::new(2));
        assert_eq!(coverage.complete_from_time_unix_seconds, 1_700_000_000);
        assert_eq!(coverage.complete_through_time_unix_seconds, 1_700_000_600);
        Ok(())
    }

    #[test]
    fn snapshot_range_read_keeps_one_distribution_sequence() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = PaidFeeDistributionConsumer::new();
        write_blocks(&store, &mut consumer, &[block(100, 1, 10, &[Some(10_000)])])?;
        let snapshot = store.read_snapshot();

        let replacement = block(100, 2, 20, &[Some(30_000)]);
        replace_block(&store, &mut consumer, BlockHeight::new(100), &replacement)?;

        let captured = PaidFeeDistributionConsumer::distribution_in_time_range_snapshot(
            &snapshot,
            0,
            SECONDS_PER_DAY,
        )?;
        let captured_block = PaidFeeDistributionConsumer::block_total_snapshot(
            &snapshot,
            10,
            BlockHeight::new(100),
            block_hash(1),
        )?;
        drop(snapshot);
        let current =
            PaidFeeDistributionConsumer::distribution_in_time_range(&store, 0, SECONDS_PER_DAY)?;
        assert_eq!(captured.days[0].frequencies[0].paid_fee_zat, 10_000);
        assert_eq!(
            captured_block,
            Some(PaidFeeBlockTotal {
                paid_fee_zat: 10_000,
                unavailable_transaction_count: 0,
            })
        );
        assert_eq!(current.days[0].frequencies[0].paid_fee_zat, 30_000);
        Ok(())
    }

    #[test]
    fn reorg_replacement_moves_frequency_between_days_atomically() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = PaidFeeDistributionConsumer::new();
        write_blocks(&store, &mut consumer, &[block(100, 1, 10, &[Some(10_000)])])?;
        let replacement = block(100, 2, SECONDS_PER_DAY + 10, &[Some(30_000)]);
        replace_block(&store, &mut consumer, BlockHeight::new(100), &replacement)?;

        let distribution = PaidFeeDistributionConsumer::distribution_in_time_range(
            &store,
            0,
            2 * SECONDS_PER_DAY,
        )?;
        assert_eq!(
            distribution.days,
            vec![PaidFeeDistributionDay {
                day_start_unix_seconds: SECONDS_PER_DAY,
                frequencies: vec![PaidFeeFrequency {
                    paid_fee_zat: 30_000,
                    transaction_count: 1,
                }],
                unavailable_transaction_count: 0,
            }]
        );
        Ok(())
    }
}
