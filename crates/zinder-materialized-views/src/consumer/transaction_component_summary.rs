//! Canonical transaction-component totals by block time and UTC day.
//!
//! The materialized view keeps one fixed-width contribution per block, a height
//! index for deterministic rewind, one aggregate per UTC day, and a separate
//! contiguous historical-coverage row. Time-range reads use day aggregates
//! only for whole UTC days and scan block contributions for clipped boundary
//! days, preserving exact half-open block-time semantics.

use std::collections::{BTreeMap, BTreeSet};

use rust_rocksdb::WriteBatch;
use zinder_core::wire::{
    decode_height_key_ascending, decode_internal_block_hash, encode_height_key_ascending,
    encode_internal_block_hash,
};
use zinder_core::{BlockHash, BlockHeight, TransactionFactsArtifact};

use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewConsumerError, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
};
use crate::{MaterializedViewStore, MaterializedViewStoreError};

/// Per-block contributions ordered by signed block time, then height.
pub const TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY: &str = "transaction_component_summary";
/// Per-height contribution keys used for deterministic rewind.
pub const TRANSACTION_COMPONENT_SUMMARY_INDEX_COLUMN_FAMILY: &str =
    "transaction_component_summary_index";
/// One aggregate row per UTC day.
pub const TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY: &str =
    "transaction_component_summary_day";
/// Contiguous historical materialization coverage.
pub const TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY: &str =
    "transaction_component_summary_coverage";

/// Column families owned by the transaction-component summary consumer.
pub const TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILIES: &[&str] = &[
    TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY,
    TRANSACTION_COMPONENT_SUMMARY_INDEX_COLUMN_FAMILY,
    TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY,
    TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY,
];

/// Stable consumer identity persisted in materialized-view metadata and cursor rows.
pub const TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("transaction_component_summary");

/// Version 2 appends transaction predicate totals to the fixed-width rows.
///
/// Version-1 rows are not readable as version 2, so this change requires a
/// fresh materialized-view store rebuilt from a certified recovery source.
pub const TRANSACTION_COMPONENT_SUMMARY_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
        2,
        TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILIES,
    );

const SECONDS_PER_DAY: i64 = 86_400;
const TIME_KEY_LEN: usize = 8;
const HEIGHT_KEY_LEN: usize = 4;
const BLOCK_HASH_LEN: usize = 32;
const CONTRIBUTION_KEY_LEN: usize = TIME_KEY_LEN + HEIGHT_KEY_LEN + BLOCK_HASH_LEN;
const TOTAL_FIELD_COUNT: usize = 23;
const TOTALS_LEN: usize = TOTAL_FIELD_COUNT * size_of::<u64>();
const EXTREMA_LEN: usize = 1 + 2 * size_of::<i64>();
const SUMMARY_VALUE_LEN: usize = TIME_KEY_LEN + TOTALS_LEN + EXTREMA_LEN;
const COVERAGE_VALUE_LEN: usize = 2 * HEIGHT_KEY_LEN + 2 * TIME_KEY_LEN;
const COVERAGE_KEY: &[u8] = b"canonical_backfill";
const TAIL_COVERAGE_KEY: &[u8] = b"seeded_live_tail";
type RawConsumerEntry = (Vec<u8>, Vec<u8>);
const TAIL_COVERAGE_VALUE_LEN: usize = HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN + TIME_KEY_LEN;

/// Additive transaction-component totals independent of any product wire.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransactionComponentTotals {
    /// Canonical transactions.
    pub transaction_count: u64,
    /// Transparent inputs.
    pub transparent_input_count: u64,
    /// Transparent outputs.
    pub transparent_output_count: u64,
    /// Sapling spends.
    pub sapling_spend_count: u64,
    /// Sapling outputs.
    pub sapling_output_count: u64,
    /// Orchard actions.
    pub orchard_action_count: u64,
    /// Ironwood actions.
    pub ironwood_action_count: u64,
    /// Sprout `JoinSplit` descriptions.
    pub sprout_joinsplit_count: u64,
    /// Transactions with any Sapling component.
    pub sapling_transaction_count: u64,
    /// Transactions with any Orchard component.
    pub orchard_transaction_count: u64,
    /// Transactions with any Ironwood component.
    pub ironwood_transaction_count: u64,
    /// Transactions with any Sprout component.
    pub sprout_transaction_count: u64,
    /// Transactions with a Sapling or Orchard component.
    pub sapling_or_orchard_transaction_count: u64,
    /// Transactions with Sapling but no Orchard component.
    pub sapling_without_orchard_transaction_count: u64,
    /// Transactions with Orchard but no Sapling component.
    pub orchard_without_sapling_transaction_count: u64,
    /// Transactions with both Sapling and Orchard components.
    pub sapling_and_orchard_transaction_count: u64,
    /// Transactions with Sapling-or-Orchard inputs and outputs and no transparent
    /// inputs or outputs.
    pub sapling_or_orchard_fully_shielded_transaction_count: u64,
    /// Transactions, including coinbase, with any Sapling, Orchard, or
    /// Ironwood component. Sprout-only transactions do not count.
    pub sapling_orchard_or_ironwood_transaction_count: u64,
    /// Non-coinbase transactions without Sapling, Orchard, or Ironwood
    /// components. Sprout-only transactions count.
    pub non_coinbase_without_sapling_orchard_or_ironwood_transaction_count: u64,
    /// Non-coinbase transactions with a Sapling, Orchard, or Ironwood
    /// component and at least one transparent input and output.
    pub non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count:
        u64,
    /// Non-coinbase transactions with a Sapling, Orchard, or Ironwood
    /// component and no transparent inputs or outputs.
    pub non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count:
        u64,
    /// Coinbase transactions, independent of their component counts.
    pub coinbase_transaction_count: u64,
    /// Transactions excluded from every predicate counter because the parsed
    /// public facts retain unsupported sections.
    pub transaction_predicate_unavailable_count: u64,
}

impl TransactionComponentTotals {
    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            transaction_count: self
                .transaction_count
                .checked_add(other.transaction_count)?,
            transparent_input_count: self
                .transparent_input_count
                .checked_add(other.transparent_input_count)?,
            transparent_output_count: self
                .transparent_output_count
                .checked_add(other.transparent_output_count)?,
            sapling_spend_count: self
                .sapling_spend_count
                .checked_add(other.sapling_spend_count)?,
            sapling_output_count: self
                .sapling_output_count
                .checked_add(other.sapling_output_count)?,
            orchard_action_count: self
                .orchard_action_count
                .checked_add(other.orchard_action_count)?,
            ironwood_action_count: self
                .ironwood_action_count
                .checked_add(other.ironwood_action_count)?,
            sprout_joinsplit_count: self
                .sprout_joinsplit_count
                .checked_add(other.sprout_joinsplit_count)?,
            sapling_transaction_count: self
                .sapling_transaction_count
                .checked_add(other.sapling_transaction_count)?,
            orchard_transaction_count: self
                .orchard_transaction_count
                .checked_add(other.orchard_transaction_count)?,
            ironwood_transaction_count: self
                .ironwood_transaction_count
                .checked_add(other.ironwood_transaction_count)?,
            sprout_transaction_count: self
                .sprout_transaction_count
                .checked_add(other.sprout_transaction_count)?,
            sapling_or_orchard_transaction_count: self
                .sapling_or_orchard_transaction_count
                .checked_add(other.sapling_or_orchard_transaction_count)?,
            sapling_without_orchard_transaction_count: self
                .sapling_without_orchard_transaction_count
                .checked_add(other.sapling_without_orchard_transaction_count)?,
            orchard_without_sapling_transaction_count: self
                .orchard_without_sapling_transaction_count
                .checked_add(other.orchard_without_sapling_transaction_count)?,
            sapling_and_orchard_transaction_count: self
                .sapling_and_orchard_transaction_count
                .checked_add(other.sapling_and_orchard_transaction_count)?,
            sapling_or_orchard_fully_shielded_transaction_count: self
                .sapling_or_orchard_fully_shielded_transaction_count
                .checked_add(
                other.sapling_or_orchard_fully_shielded_transaction_count,
            )?,
            sapling_orchard_or_ironwood_transaction_count: self
                .sapling_orchard_or_ironwood_transaction_count
                .checked_add(other.sapling_orchard_or_ironwood_transaction_count)?,
            non_coinbase_without_sapling_orchard_or_ironwood_transaction_count: self
                .non_coinbase_without_sapling_orchard_or_ironwood_transaction_count
                .checked_add(other.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count)?,
            non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count: self
                .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count
                .checked_add(
                    other.non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
                )?,
            non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count: self
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count
                .checked_add(
                    other.non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
                )?,
            coinbase_transaction_count: self
                .coinbase_transaction_count
                .checked_add(other.coinbase_transaction_count)?,
            transaction_predicate_unavailable_count: self
                .transaction_predicate_unavailable_count
                .checked_add(other.transaction_predicate_unavailable_count)?,
        })
    }

    fn fields(self) -> [u64; TOTAL_FIELD_COUNT] {
        [
            self.transaction_count,
            self.transparent_input_count,
            self.transparent_output_count,
            self.sapling_spend_count,
            self.sapling_output_count,
            self.orchard_action_count,
            self.ironwood_action_count,
            self.sprout_joinsplit_count,
            self.sapling_transaction_count,
            self.orchard_transaction_count,
            self.ironwood_transaction_count,
            self.sprout_transaction_count,
            self.sapling_or_orchard_transaction_count,
            self.sapling_without_orchard_transaction_count,
            self.orchard_without_sapling_transaction_count,
            self.sapling_and_orchard_transaction_count,
            self.sapling_or_orchard_fully_shielded_transaction_count,
            self.sapling_orchard_or_ironwood_transaction_count,
            self.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
            self.non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
            self.non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            self.coinbase_transaction_count,
            self.transaction_predicate_unavailable_count,
        ]
    }

    fn from_fields(fields: [u64; TOTAL_FIELD_COUNT]) -> Self {
        Self {
            transaction_count: fields[0],
            transparent_input_count: fields[1],
            transparent_output_count: fields[2],
            sapling_spend_count: fields[3],
            sapling_output_count: fields[4],
            orchard_action_count: fields[5],
            ironwood_action_count: fields[6],
            sprout_joinsplit_count: fields[7],
            sapling_transaction_count: fields[8],
            orchard_transaction_count: fields[9],
            ironwood_transaction_count: fields[10],
            sprout_transaction_count: fields[11],
            sapling_or_orchard_transaction_count: fields[12],
            sapling_without_orchard_transaction_count: fields[13],
            orchard_without_sapling_transaction_count: fields[14],
            sapling_and_orchard_transaction_count: fields[15],
            sapling_or_orchard_fully_shielded_transaction_count: fields[16],
            sapling_orchard_or_ironwood_transaction_count: fields[17],
            non_coinbase_without_sapling_orchard_or_ironwood_transaction_count: fields[18],
            non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count: fields[19],
            non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count: fields[20],
            coinbase_transaction_count: fields[21],
            transaction_predicate_unavailable_count: fields[22],
        }
    }
}

/// One UTC-day result bucket, possibly clipped by the requested time range.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransactionComponentDay {
    /// UTC midnight as Unix seconds.
    pub day_start_unix_seconds: i64,
    /// Additive totals inside this bucket.
    pub totals: TransactionComponentTotals,
    /// Earliest block time carrying a Sapling-or-Orchard transaction.
    pub first_sapling_or_orchard_transaction_time_unix_seconds: Option<i64>,
    /// Latest block time carrying a Sapling-or-Orchard transaction.
    pub last_sapling_or_orchard_transaction_time_unix_seconds: Option<i64>,
}

impl TransactionComponentDay {
    fn empty(day_start_unix_seconds: i64) -> Self {
        Self {
            day_start_unix_seconds,
            ..Self::default()
        }
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        if self.day_start_unix_seconds != other.day_start_unix_seconds {
            return None;
        }
        Some(Self {
            day_start_unix_seconds: self.day_start_unix_seconds,
            totals: self.totals.checked_add(other.totals)?,
            first_sapling_or_orchard_transaction_time_unix_seconds: min_optional(
                self.first_sapling_or_orchard_transaction_time_unix_seconds,
                other.first_sapling_or_orchard_transaction_time_unix_seconds,
            ),
            last_sapling_or_orchard_transaction_time_unix_seconds: max_optional(
                self.last_sapling_or_orchard_transaction_time_unix_seconds,
                other.last_sapling_or_orchard_transaction_time_unix_seconds,
            ),
        })
    }
}

/// Exact aggregate and UTC-day buckets for one half-open block-time range.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TransactionComponentSummary {
    /// Additive totals across all returned buckets.
    pub totals: TransactionComponentTotals,
    /// Non-empty UTC-day buckets in ascending order.
    pub days: Vec<TransactionComponentDay>,
}

/// Contiguous canonical history materialized by backfill and subsequent tailing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionComponentBackfillCoverage {
    /// First height in the contiguous materialized range.
    pub complete_from_height: BlockHeight,
    /// Last height in the contiguous materialized range.
    pub complete_through_height: BlockHeight,
    /// Block time at `complete_from_height`.
    pub complete_from_time_unix_seconds: i64,
    /// Block time at `complete_through_height`.
    pub complete_through_time_unix_seconds: i64,
}

/// Durable live-tail interval established when ingest seeds the consumer cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionComponentTailCoverage {
    /// First height the seeded live tail expects to materialize.
    pub boundary_height: BlockHeight,
    /// Last contiguous tail height materialized, absent before the first block.
    pub complete_through_height: Option<BlockHeight>,
    /// Block time at `complete_through_height`.
    pub complete_through_time_unix_seconds: Option<i64>,
}

impl TransactionComponentTailCoverage {
    /// Creates a seeded tail boundary with no materialized tail blocks yet.
    #[must_use]
    pub const fn from_boundary(boundary_height: BlockHeight) -> Self {
        Self {
            boundary_height,
            complete_through_height: None,
            complete_through_time_unix_seconds: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BlockContribution {
    block_hash: BlockHash,
    day: TransactionComponentDay,
}

impl TransactionComponentBackfillCoverage {
    /// Creates a contiguous historical coverage record.
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

/// Materializes transaction component contributions and UTC-day aggregates.
#[derive(Default)]
pub struct TransactionComponentSummaryConsumer {
    pending_contributions: BTreeMap<[u8; CONTRIBUTION_KEY_LEN], Option<BlockContribution>>,
    pending_height_keys: BTreeMap<BlockHeight, Option<[u8; CONTRIBUTION_KEY_LEN]>>,
}

impl TransactionComponentSummaryConsumer {
    /// Builds an empty consumer with no pending batch state.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pending_contributions: BTreeMap::new(),
            pending_height_keys: BTreeMap::new(),
        }
    }

    /// Queries exact `[start, end)` block-time totals and ascending UTC days.
    ///
    /// Whole UTC days read their aggregate row. A clipped first or last day is
    /// rebuilt from per-block contributions so blocks exactly at `end` are
    /// excluded.
    pub fn summary_in_time_range(
        store: &MaterializedViewStore,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
    ) -> Result<TransactionComponentSummary, TransactionComponentSummaryConsumerError> {
        if start_time_unix_seconds >= end_time_unix_seconds {
            return Err(TransactionComponentSummaryConsumerError::InvalidTimeRange {
                start: start_time_unix_seconds,
                end: end_time_unix_seconds,
            });
        }
        let start_day = utc_day_start(start_time_unix_seconds);
        let last_time = end_time_unix_seconds.checked_sub(1).ok_or(
            TransactionComponentSummaryConsumerError::InvalidTimeRange {
                start: start_time_unix_seconds,
                end: end_time_unix_seconds,
            },
        )?;
        let last_day = utc_day_start(last_time);
        let mut days = BTreeMap::<i64, TransactionComponentDay>::new();

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
                .ok_or(TransactionComponentSummaryConsumerError::TimeOverflow)?;
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

            let middle_start = start_day
                .checked_add(SECONDS_PER_DAY)
                .ok_or(TransactionComponentSummaryConsumerError::TimeOverflow)?;
            if middle_start < last_day {
                add_stored_days_in_range(store, middle_start, last_day, &mut days)?;
            }

            let last_day_end = last_day
                .checked_add(SECONDS_PER_DAY)
                .ok_or(TransactionComponentSummaryConsumerError::TimeOverflow)?;
            if end_time_unix_seconds == last_day_end {
                add_stored_day(store, last_day, &mut days)?;
            } else {
                add_contributions_in_range(store, last_day, end_time_unix_seconds, &mut days)?;
            }
        }

        let mut totals = TransactionComponentTotals::default();
        for day in days.values() {
            totals = totals
                .checked_add(day.totals)
                .ok_or(TransactionComponentSummaryConsumerError::CounterOverflow)?;
        }
        Ok(TransactionComponentSummary {
            totals,
            days: days.into_values().collect(),
        })
    }

    /// Reads contiguous historical coverage, when a backfill has started.
    pub fn backfill_coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<TransactionComponentBackfillCoverage>, MaterializedViewStoreError> {
        let Some(payload) = store.get_consumer(
            TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY,
            COVERAGE_KEY,
        )?
        else {
            return Ok(None);
        };
        decode_coverage(&payload)
            .map(Some)
            .map_err(|error| store_decode_error(&error))
    }

    /// Reads the durable seeded live-tail boundary and contiguous tail tip.
    pub fn tail_coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<TransactionComponentTailCoverage>, MaterializedViewStoreError> {
        let Some(payload) = store.get_consumer(
            TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY,
            TAIL_COVERAGE_KEY,
        )?
        else {
            return Ok(None);
        };
        decode_tail_coverage(&payload)
            .map(Some)
            .map_err(|error| store_decode_error(&error))
    }

    /// Initializes the first height owned by a seeded live tail.
    ///
    /// The call is idempotent for the same boundary and rejects a conflicting
    /// boundary. Normal apply/revert batches durably maintain the tail's
    /// contiguous `complete_through` endpoint.
    pub fn initialize_tail_boundary(
        store: &MaterializedViewStore,
        boundary_height: BlockHeight,
    ) -> Result<(), TransactionComponentSummaryConsumerError> {
        let requested = TransactionComponentTailCoverage::from_boundary(boundary_height);
        match Self::tail_coverage(store)? {
            None => store.put_consumer(
                TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY,
                TAIL_COVERAGE_KEY,
                &encode_tail_coverage(requested),
            )?,
            Some(existing) if existing == requested => {}
            Some(_) => {
                return Err(
                    TransactionComponentSummaryConsumerError::TailBoundaryConflict {
                        boundary_height: boundary_height.value(),
                    },
                );
            }
        }
        Ok(())
    }

    /// Widens an existing startup tail to an earlier canonical boundary.
    ///
    /// Contribution rows are preserved, while the contiguous-tail marker is
    /// reset so startup seeding can revalidate every height from the widened
    /// boundary. Calls with the same or a later boundary are no-ops. Ingest
    /// must call this before its chain-event tailer starts.
    pub fn widen_tail_boundary_for_startup(
        store: &MaterializedViewStore,
        boundary_height: BlockHeight,
    ) -> Result<bool, TransactionComponentSummaryConsumerError> {
        let Some(existing) = Self::tail_coverage(store)? else {
            Self::initialize_tail_boundary(store, boundary_height)?;
            return Ok(true);
        };
        if boundary_height >= existing.boundary_height {
            return Ok(false);
        }
        store.put_consumer(
            TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY,
            TAIL_COVERAGE_KEY,
            &encode_tail_coverage(TransactionComponentTailCoverage::from_boundary(
                boundary_height,
            )),
        )?;
        Ok(true)
    }

    /// Reads contiguous complete coverage after joining backfill to live tail.
    ///
    /// Until historical coverage reaches the height immediately before the
    /// seeded tail boundary, this returns the historical range alone. Once the
    /// ranges touch or overlap, `complete_through` advances to the live-tail
    /// endpoint.
    pub fn coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<TransactionComponentBackfillCoverage>, MaterializedViewStoreError> {
        let Some(mut coverage) = Self::backfill_coverage(store)? else {
            return Self::tail_interval_coverage(store);
        };
        let Some(tail) = Self::tail_coverage(store)? else {
            return Ok(Some(coverage));
        };
        let Some(tail_through_height) = tail.complete_through_height else {
            return Ok(Some(coverage));
        };
        let joins_tail = coverage.complete_through_height >= tail.boundary_height
            || coverage.complete_through_height.next() == Some(tail.boundary_height);
        if joins_tail && tail_through_height > coverage.complete_through_height {
            coverage.complete_through_height = tail_through_height;
            coverage.complete_through_time_unix_seconds =
                tail.complete_through_time_unix_seconds.ok_or_else(|| {
                    store_decode_error(
                        &TransactionComponentSummaryConsumerError::MalformedTailCoverage {
                            bytes: TAIL_COVERAGE_VALUE_LEN,
                        },
                    )
                })?;
        }
        Ok(Some(coverage))
    }

    /// Synthesizes coverage from the live tail when no backfill has run.
    fn tail_interval_coverage(
        store: &MaterializedViewStore,
    ) -> Result<Option<TransactionComponentBackfillCoverage>, MaterializedViewStoreError> {
        let Some(tail) = Self::tail_coverage(store)? else {
            return Ok(None);
        };
        let Some(through_height) = tail.complete_through_height else {
            return Ok(None);
        };
        let through_time = tail.complete_through_time_unix_seconds.ok_or_else(|| {
            store_decode_error(
                &TransactionComponentSummaryConsumerError::MalformedTailCoverage {
                    bytes: TAIL_COVERAGE_VALUE_LEN,
                },
            )
        })?;
        let (from_time, _) =
            height_contribution_after_batch(store, &BTreeMap::new(), tail.boundary_height)
                .map_err(|error| store_decode_error(&error))?
                .ok_or_else(|| {
                    store_decode_error(
                        &TransactionComponentSummaryConsumerError::MissingIndexedContribution {
                            height: tail.boundary_height.value(),
                        },
                    )
                })?;
        Ok(Some(TransactionComponentBackfillCoverage::new(
            tail.boundary_height,
            through_height,
            from_time,
            through_time,
        )))
    }

    /// Atomically writes an ordered historical block batch and coverage.
    ///
    /// The chain-event cursor is intentionally untouched. Repeating an
    /// identical batch is idempotent; conflicting rows fail closed.
    pub fn write_backfill_batch(
        &mut self,
        store: &MaterializedViewStore,
        blocks: &[BlockCommitContext],
        next_coverage: TransactionComponentBackfillCoverage,
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
            store.consumer_column_family(TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY)?;
        ctx.batch
            .put_cf(&coverage_cf, COVERAGE_KEY, encode_coverage(next_coverage));
        store.write_consumer_batch(TRANSACTION_COMPONENT_SUMMARY_SCHEMA.name, ctx.batch)?;
        Ok(())
    }

    /// Atomically seeds canonical blocks already visible at a newly joined
    /// live-tail boundary without advancing the chain-event cursor.
    ///
    /// Startup uses this for the unsettled range that predates the cursor a
    /// newly added consumer inherits from existing materialized-view consumers. Reorg
    /// events subsequently own these rows exactly like ordinary tail writes.
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
        store.write_consumer_batch(TRANSACTION_COMPONENT_SUMMARY_SCHEMA.name, ctx.batch)?;
        Ok(())
    }

    fn stage_day_aggregates(
        &self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let affected_days: BTreeSet<i64> = self
            .pending_contributions
            .keys()
            .map(|key| decode_contribution_key(key).map(|entry| utc_day_start(entry.0)))
            .collect::<Result<_, TransactionComponentSummaryConsumerError>>()?;
        let day_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY)?;

        for day_start in affected_days {
            let day_end = day_start
                .checked_add(SECONDS_PER_DAY)
                .ok_or(TransactionComponentSummaryConsumerError::TimeOverflow)?;
            let persisted = read_persisted_contributions(ctx.store, day_start, day_end)?;
            validate_persisted_day(ctx.store, day_start, persisted.values().copied())?;
            let mut final_contributions = persisted;
            for (key, contribution) in self.pending_contributions.range(
                encode_contribution_key(
                    day_start,
                    BlockHeight::new(0),
                    BlockHash::from_bytes([0; BLOCK_HASH_LEN]),
                )
                    ..encode_contribution_key(
                        day_end,
                        BlockHeight::new(0),
                        BlockHash::from_bytes([0; BLOCK_HASH_LEN]),
                    ),
            ) {
                match contribution {
                    Some(contribution) => {
                        final_contributions.insert(*key, *contribution);
                    }
                    None => {
                        final_contributions.remove(key);
                    }
                }
            }
            let aggregate = aggregate_days(
                day_start,
                final_contributions
                    .values()
                    .map(|contribution| contribution.day),
            )?;
            let day_key = encode_time_key(day_start);
            if aggregate.totals.transaction_count == 0 {
                ctx.batch.delete_cf(&day_cf, day_key);
            } else {
                ctx.batch
                    .put_cf(&day_cf, day_key, encode_summary_value(aggregate));
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
            TransactionComponentTailCoverage::from_boundary(boundary_height)
        };
        while let Some(through_height) = tail.complete_through_height {
            if height_contribution_after_batch(
                ctx.store,
                &self.pending_height_keys,
                through_height,
            )?
            .is_some()
            {
                break;
            }
            if through_height <= tail.boundary_height {
                tail.complete_through_height = None;
                tail.complete_through_time_unix_seconds = None;
                break;
            }
            let previous_height = BlockHeight::new(through_height.value() - 1);
            tail.complete_through_height = Some(previous_height);
            tail.complete_through_time_unix_seconds = height_contribution_after_batch(
                ctx.store,
                &self.pending_height_keys,
                previous_height,
            )?
            .map(|(time, _hash)| time);
        }
        loop {
            let candidate = tail
                .complete_through_height
                .map_or(Some(tail.boundary_height), BlockHeight::next);
            let Some(candidate) = candidate else {
                break;
            };
            let Some((block_time, _block_hash)) =
                height_contribution_after_batch(ctx.store, &self.pending_height_keys, candidate)?
            else {
                break;
            };
            tail.complete_through_height = Some(candidate);
            tail.complete_through_time_unix_seconds = Some(block_time);
        }
        let coverage_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY)?;
        ctx.batch
            .put_cf(&coverage_cf, TAIL_COVERAGE_KEY, encode_tail_coverage(tail));
        Ok(())
    }
}

impl BlockKeyedConsumer for TransactionComponentSummaryConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME
    }

    fn begin_batch(
        &mut self,
        _ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        self.pending_contributions.clear();
        self.pending_height_keys.clear();
        Ok(())
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let contribution = contribution_for_block(block)?;
        let contribution_key = encode_contribution_key(
            block.block_time_unix_seconds,
            block.height,
            block.block_hash,
        );
        match self.pending_height_keys.get(&block.height) {
            None => validate_apply_state(ctx.store, block.height, contribution_key, contribution)?,
            Some(None) => {}
            Some(Some(_)) => {
                return Err(Box::new(
                    TransactionComponentSummaryConsumerError::DuplicateBatchHeight {
                        height: block.height.value(),
                    },
                ));
            }
        }
        self.pending_height_keys
            .insert(block.height, Some(contribution_key));
        self.pending_contributions
            .insert(contribution_key, Some(contribution));

        let contribution_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_COMPONENT_SUMMARY_INDEX_COLUMN_FAMILY)?;
        ctx.batch.put_cf(
            &contribution_cf,
            contribution_key,
            encode_summary_value(contribution.day),
        );
        ctx.batch.put_cf(
            &index_cf,
            encode_height_key_ascending(block.height),
            contribution_key,
        );
        Ok(())
    }

    fn revert_block(
        &mut self,
        height: BlockHeight,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        if self.pending_height_keys.contains_key(&height) {
            return Err(Box::new(
                TransactionComponentSummaryConsumerError::DuplicateBatchHeight {
                    height: height.value(),
                },
            ));
        }
        let index_key = encode_height_key_ascending(height);
        let Some(index_payload) = ctx.store.get_consumer(
            TRANSACTION_COMPONENT_SUMMARY_INDEX_COLUMN_FAMILY,
            &index_key,
        )?
        else {
            self.pending_height_keys.insert(height, None);
            return Ok(());
        };
        let contribution_key = decode_index_payload(height, &index_payload)?;
        let (_, indexed_height, _) = decode_contribution_key(&contribution_key)?;
        if indexed_height != height {
            return Err(Box::new(
                TransactionComponentSummaryConsumerError::IndexHeightMismatch {
                    requested_height: height.value(),
                    indexed_height: indexed_height.value(),
                },
            ));
        }
        let Some(payload) = ctx.store.get_consumer(
            TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY,
            &contribution_key,
        )?
        else {
            return Err(Box::new(
                TransactionComponentSummaryConsumerError::MissingIndexedContribution {
                    height: height.value(),
                },
            ));
        };
        let contribution = BlockContribution {
            block_hash: decode_contribution_key(&contribution_key)?.2,
            day: decode_summary_value(&payload)?,
        };
        validate_contribution_key(contribution_key, contribution)?;
        self.pending_height_keys.insert(height, None);
        self.pending_contributions.insert(contribution_key, None);

        let contribution_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(TRANSACTION_COMPONENT_SUMMARY_INDEX_COLUMN_FAMILY)?;
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
        self.pending_contributions.clear();
        self.pending_height_keys.clear();
        Ok(())
    }
}

fn contribution_for_block(
    block: &BlockCommitContext,
) -> Result<BlockContribution, TransactionComponentSummaryConsumerError> {
    let day_start = utc_day_start(block.block_time_unix_seconds);
    let mut contribution = TransactionComponentDay::empty(day_start);
    for transaction in &block.transactions {
        let transaction_totals = totals_for_transaction(transaction);
        contribution.totals = contribution
            .totals
            .checked_add(transaction_totals)
            .ok_or(TransactionComponentSummaryConsumerError::CounterOverflow)?;
        if transaction_totals.sapling_or_orchard_transaction_count > 0 {
            contribution.first_sapling_or_orchard_transaction_time_unix_seconds = Some(
                contribution
                    .first_sapling_or_orchard_transaction_time_unix_seconds
                    .map_or(block.block_time_unix_seconds, |existing| {
                        existing.min(block.block_time_unix_seconds)
                    }),
            );
            contribution.last_sapling_or_orchard_transaction_time_unix_seconds = Some(
                contribution
                    .last_sapling_or_orchard_transaction_time_unix_seconds
                    .map_or(block.block_time_unix_seconds, |existing| {
                        existing.max(block.block_time_unix_seconds)
                    }),
            );
        }
    }
    Ok(BlockContribution {
        block_hash: block.block_hash,
        day: contribution,
    })
}

fn totals_for_transaction(transaction: &TransactionFactsArtifact) -> TransactionComponentTotals {
    let counts = transaction.public_facts.counts;
    let has_sapling = counts.sapling_spend_count > 0 || counts.sapling_output_count > 0;
    let has_orchard = counts.orchard_action_count > 0;
    let has_ironwood = counts.ironwood_action_count > 0;
    let has_sprout = counts.sprout_joinsplit_count > 0;
    let has_shielded_protocol = has_sapling || has_orchard || has_ironwood;
    let predicates_are_unavailable = !transaction.public_facts.unsupported_sections.is_empty();
    let is_sapling_or_orchard = has_sapling || has_orchard;
    let has_sapling_or_orchard_input = counts.sapling_spend_count > 0 || has_orchard;
    let has_sapling_or_orchard_output = counts.sapling_output_count > 0 || has_orchard;
    let is_sapling_or_orchard_fully_shielded = has_sapling_or_orchard_input
        && has_sapling_or_orchard_output
        && counts.transparent_input_count == 0
        && counts.transparent_output_count == 0;
    let is_transparent = !predicates_are_unavailable
        && !transaction.public_facts.is_coinbase
        && !has_shielded_protocol;
    let is_mixed_transparent_shielded = !predicates_are_unavailable
        && !transaction.public_facts.is_coinbase
        && has_shielded_protocol
        && counts.transparent_input_count > 0
        && counts.transparent_output_count > 0;
    let is_fully_shielded = !predicates_are_unavailable
        && !transaction.public_facts.is_coinbase
        && has_shielded_protocol
        && counts.transparent_input_count == 0
        && counts.transparent_output_count == 0;
    TransactionComponentTotals {
        transaction_count: 1,
        transparent_input_count: u64::from(counts.transparent_input_count),
        transparent_output_count: u64::from(counts.transparent_output_count),
        sapling_spend_count: u64::from(counts.sapling_spend_count),
        sapling_output_count: u64::from(counts.sapling_output_count),
        orchard_action_count: u64::from(counts.orchard_action_count),
        ironwood_action_count: u64::from(counts.ironwood_action_count),
        sprout_joinsplit_count: u64::from(counts.sprout_joinsplit_count),
        sapling_transaction_count: u64::from(has_sapling),
        orchard_transaction_count: u64::from(has_orchard),
        ironwood_transaction_count: u64::from(has_ironwood),
        sprout_transaction_count: u64::from(has_sprout),
        sapling_or_orchard_transaction_count: u64::from(is_sapling_or_orchard),
        sapling_without_orchard_transaction_count: u64::from(has_sapling && !has_orchard),
        orchard_without_sapling_transaction_count: u64::from(has_orchard && !has_sapling),
        sapling_and_orchard_transaction_count: u64::from(has_sapling && has_orchard),
        sapling_or_orchard_fully_shielded_transaction_count: u64::from(is_sapling_or_orchard_fully_shielded),
        sapling_orchard_or_ironwood_transaction_count:
            u64::from(!predicates_are_unavailable && has_shielded_protocol),
        non_coinbase_without_sapling_orchard_or_ironwood_transaction_count:
            u64::from(is_transparent),
        non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count:
            u64::from(is_mixed_transparent_shielded),
        non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count:
            u64::from(is_fully_shielded),
        coinbase_transaction_count:
            u64::from(!predicates_are_unavailable && transaction.public_facts.is_coinbase),
        transaction_predicate_unavailable_count: u64::from(predicates_are_unavailable),
    }
}

fn validate_apply_state(
    store: &MaterializedViewStore,
    height: BlockHeight,
    expected_key: [u8; CONTRIBUTION_KEY_LEN],
    expected_contribution: BlockContribution,
) -> Result<(), TransactionComponentSummaryConsumerError> {
    let index_key = encode_height_key_ascending(height);
    let index_payload = store.get_consumer(
        TRANSACTION_COMPONENT_SUMMARY_INDEX_COLUMN_FAMILY,
        &index_key,
    )?;
    let contribution_payload =
        store.get_consumer(TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY, &expected_key)?;
    match (index_payload, contribution_payload) {
        (None, None) => Ok(()),
        (Some(index_payload), Some(contribution_payload)) => {
            let stored_key = decode_index_payload(height, &index_payload)?;
            let stored_contribution = BlockContribution {
                block_hash: decode_contribution_key(&stored_key)?.2,
                day: decode_summary_value(&contribution_payload)?,
            };
            validate_contribution_key(stored_key, stored_contribution)?;
            if stored_key == expected_key && stored_contribution == expected_contribution {
                Ok(())
            } else {
                Err(
                    TransactionComponentSummaryConsumerError::ConflictingHeight {
                        height: height.value(),
                    },
                )
            }
        }
        (Some(_), None) | (None, Some(_)) => Err(
            TransactionComponentSummaryConsumerError::IncompleteHeightState {
                height: height.value(),
            },
        ),
    }
}

fn height_contribution_after_batch(
    store: &MaterializedViewStore,
    pending_height_keys: &BTreeMap<BlockHeight, Option<[u8; CONTRIBUTION_KEY_LEN]>>,
    height: BlockHeight,
) -> Result<Option<(i64, BlockHash)>, TransactionComponentSummaryConsumerError> {
    if let Some(pending_key) = pending_height_keys.get(&height) {
        return pending_key
            .map(|key| decode_contribution_key(&key).map(|(time, _, hash)| (time, hash)))
            .transpose();
    }
    let Some(index_payload) = store.get_consumer(
        TRANSACTION_COMPONENT_SUMMARY_INDEX_COLUMN_FAMILY,
        &encode_height_key_ascending(height),
    )?
    else {
        return Ok(None);
    };
    let contribution_key = decode_index_payload(height, &index_payload)?;
    let (block_time, indexed_height, block_hash) = decode_contribution_key(&contribution_key)?;
    if indexed_height != height {
        return Err(
            TransactionComponentSummaryConsumerError::IndexHeightMismatch {
                requested_height: height.value(),
                indexed_height: indexed_height.value(),
            },
        );
    }
    let Some(payload) = store.get_consumer(
        TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY,
        &contribution_key,
    )?
    else {
        return Err(
            TransactionComponentSummaryConsumerError::MissingIndexedContribution {
                height: height.value(),
            },
        );
    };
    validate_contribution_key(
        contribution_key,
        BlockContribution {
            block_hash,
            day: decode_summary_value(&payload)?,
        },
    )?;
    Ok(Some((block_time, block_hash)))
}

fn validate_backfill_batch(
    store: &MaterializedViewStore,
    blocks: &[BlockCommitContext],
    next: TransactionComponentBackfillCoverage,
) -> Result<(), MaterializedViewConsumerError> {
    let Some(first) = blocks.first() else {
        return Err(Box::new(
            TransactionComponentSummaryConsumerError::EmptyBackfill,
        ));
    };
    let last = blocks
        .last()
        .ok_or(TransactionComponentSummaryConsumerError::EmptyBackfill)?;
    for pair in blocks.windows(2) {
        if pair[0].height.next() != Some(pair[1].height) {
            return Err(Box::new(
                TransactionComponentSummaryConsumerError::CoverageDiscontinuous,
            ));
        }
    }
    let invalid_coverage_bounds = next.complete_from_height > next.complete_through_height;
    let last_block_mismatch = last.height != next.complete_through_height
        || last.block_time_unix_seconds != next.complete_through_time_unix_seconds;
    if invalid_coverage_bounds || last_block_mismatch {
        return Err(Box::new(
            TransactionComponentSummaryConsumerError::CoverageDiscontinuous,
        ));
    }
    match TransactionComponentSummaryConsumer::backfill_coverage(store)? {
        None if first.height == next.complete_from_height
            && first.block_time_unix_seconds == next.complete_from_time_unix_seconds => {}
        Some(existing)
            if existing.complete_from_height == next.complete_from_height
                && existing.complete_from_time_unix_seconds
                    == next.complete_from_time_unix_seconds
                && existing.complete_through_height.next() == Some(first.height) => {}
        None | Some(_) => {
            return Err(Box::new(
                TransactionComponentSummaryConsumerError::CoverageDiscontinuous,
            ));
        }
    }
    Ok(())
}

fn validate_tail_seed_batch(
    store: &MaterializedViewStore,
    blocks: &[BlockCommitContext],
) -> Result<(), MaterializedViewConsumerError> {
    let Some(first) = blocks.first() else {
        return Err(Box::new(
            TransactionComponentSummaryConsumerError::EmptyBackfill,
        ));
    };
    for pair in blocks.windows(2) {
        if pair[0].height.next() != Some(pair[1].height) {
            return Err(Box::new(
                TransactionComponentSummaryConsumerError::CoverageDiscontinuous,
            ));
        }
    }
    let tail = TransactionComponentSummaryConsumer::tail_coverage(store)?.ok_or_else(|| {
        Box::new(TransactionComponentSummaryConsumerError::CoverageDiscontinuous)
            as MaterializedViewConsumerError
    })?;
    let expected_first = tail
        .complete_through_height
        .map_or(Some(tail.boundary_height), BlockHeight::next)
        .ok_or_else(|| {
            Box::new(TransactionComponentSummaryConsumerError::CoverageDiscontinuous)
                as MaterializedViewConsumerError
        })?;
    if first.height != expected_first {
        return Err(Box::new(
            TransactionComponentSummaryConsumerError::CoverageDiscontinuous,
        ));
    }
    Ok(())
}

fn read_persisted_contributions(
    store: &MaterializedViewStore,
    start: i64,
    end: i64,
) -> Result<
    BTreeMap<[u8; CONTRIBUTION_KEY_LEN], BlockContribution>,
    TransactionComponentSummaryConsumerError,
> {
    let entries = contribution_entries_in_range(store, start, end)?;
    let mut contributions = BTreeMap::new();
    for (key, payload) in entries {
        let key: [u8; CONTRIBUTION_KEY_LEN] = key
            .try_into()
            .map_err(|_| TransactionComponentSummaryConsumerError::MalformedContributionKey)?;
        let contribution = BlockContribution {
            block_hash: decode_contribution_key(&key)?.2,
            day: decode_summary_value(&payload)?,
        };
        validate_contribution_key(key, contribution)?;
        contributions.insert(key, contribution);
    }
    Ok(contributions)
}

fn validate_persisted_day(
    store: &MaterializedViewStore,
    day_start: i64,
    contributions: impl Iterator<Item = BlockContribution>,
) -> Result<(), TransactionComponentSummaryConsumerError> {
    let expected = aggregate_days(
        day_start,
        contributions.map(|contribution| contribution.day),
    )?;
    let stored = store.get_consumer(
        TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY,
        &encode_time_key(day_start),
    )?;
    match (stored, expected.totals.transaction_count) {
        (None, 0) => Ok(()),
        (Some(payload), count) if count > 0 => {
            let actual = decode_summary_value(&payload)?;
            if actual == expected {
                Ok(())
            } else {
                Err(TransactionComponentSummaryConsumerError::ConflictingDay { day_start })
            }
        }
        (None, _) | (Some(_), 0) => {
            Err(TransactionComponentSummaryConsumerError::ConflictingDay { day_start })
        }
        (Some(_), _) => unreachable!(),
    }
}

fn aggregate_days(
    day_start: i64,
    mut contributions: impl Iterator<Item = TransactionComponentDay>,
) -> Result<TransactionComponentDay, TransactionComponentSummaryConsumerError> {
    contributions.try_fold(
        TransactionComponentDay::empty(day_start),
        |aggregate, contribution| {
            aggregate
                .checked_add(contribution)
                .ok_or(TransactionComponentSummaryConsumerError::CounterOverflow)
        },
    )
}

fn add_contributions_in_range(
    store: &MaterializedViewStore,
    start: i64,
    end: i64,
    days: &mut BTreeMap<i64, TransactionComponentDay>,
) -> Result<(), TransactionComponentSummaryConsumerError> {
    for (key, payload) in contribution_entries_in_range(store, start, end)? {
        let key: [u8; CONTRIBUTION_KEY_LEN] = key
            .try_into()
            .map_err(|_| TransactionComponentSummaryConsumerError::MalformedContributionKey)?;
        let contribution = BlockContribution {
            block_hash: decode_contribution_key(&key)?.2,
            day: decode_summary_value(&payload)?,
        };
        validate_contribution_key(key, contribution)?;
        add_day(days, contribution.day)?;
    }
    Ok(())
}

fn contribution_entries_in_range(
    store: &MaterializedViewStore,
    start: i64,
    end: i64,
) -> Result<Vec<RawConsumerEntry>, TransactionComponentSummaryConsumerError> {
    if start >= end {
        return Ok(Vec::new());
    }
    let last_time = end
        .checked_sub(1)
        .ok_or(TransactionComponentSummaryConsumerError::TimeOverflow)?;
    Ok(store.range_iterate_consumer(
        TRANSACTION_COMPONENT_SUMMARY_COLUMN_FAMILY,
        &encode_contribution_key(
            start,
            BlockHeight::new(0),
            BlockHash::from_bytes([0; BLOCK_HASH_LEN]),
        ),
        &encode_contribution_key(
            last_time,
            BlockHeight::new(u32::MAX),
            BlockHash::from_bytes([u8::MAX; BLOCK_HASH_LEN]),
        ),
        usize::MAX,
    )?)
}

fn add_stored_day(
    store: &MaterializedViewStore,
    day_start: i64,
    days: &mut BTreeMap<i64, TransactionComponentDay>,
) -> Result<(), TransactionComponentSummaryConsumerError> {
    let Some(payload) = store.get_consumer(
        TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY,
        &encode_time_key(day_start),
    )?
    else {
        return Ok(());
    };
    let day = decode_summary_value(&payload)?;
    if day.day_start_unix_seconds != day_start {
        return Err(TransactionComponentSummaryConsumerError::ConflictingDay { day_start });
    }
    add_day(days, day)
}

fn add_stored_days_in_range(
    store: &MaterializedViewStore,
    start_day: i64,
    end_day_exclusive: i64,
    days: &mut BTreeMap<i64, TransactionComponentDay>,
) -> Result<(), TransactionComponentSummaryConsumerError> {
    let last_day = end_day_exclusive
        .checked_sub(SECONDS_PER_DAY)
        .ok_or(TransactionComponentSummaryConsumerError::TimeOverflow)?;
    for (key, payload) in store.range_iterate_consumer(
        TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY,
        &encode_time_key(start_day),
        &encode_time_key(last_day),
        usize::MAX,
    )? {
        let day_start = decode_time_key(&key)?;
        let day = decode_summary_value(&payload)?;
        if day.day_start_unix_seconds != day_start {
            return Err(TransactionComponentSummaryConsumerError::ConflictingDay { day_start });
        }
        add_day(days, day)?;
    }
    Ok(())
}

fn add_day(
    days: &mut BTreeMap<i64, TransactionComponentDay>,
    day: TransactionComponentDay,
) -> Result<(), TransactionComponentSummaryConsumerError> {
    let aggregate = days
        .get(&day.day_start_unix_seconds)
        .copied()
        .unwrap_or_else(|| TransactionComponentDay::empty(day.day_start_unix_seconds))
        .checked_add(day)
        .ok_or(TransactionComponentSummaryConsumerError::CounterOverflow)?;
    days.insert(day.day_start_unix_seconds, aggregate);
    Ok(())
}

fn utc_day_start(unix_seconds: i64) -> i64 {
    unix_seconds.div_euclid(SECONDS_PER_DAY) * SECONDS_PER_DAY
}

fn min_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(timestamp), None) | (None, Some(timestamp)) => Some(timestamp),
        (None, None) => None,
    }
}

fn max_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(timestamp), None) | (None, Some(timestamp)) => Some(timestamp),
        (None, None) => None,
    }
}

fn encode_time_key(unix_seconds: i64) -> [u8; TIME_KEY_LEN] {
    (unix_seconds.cast_unsigned() ^ (1_u64 << 63)).to_be_bytes()
}

fn decode_time_key(key: &[u8]) -> Result<i64, TransactionComponentSummaryConsumerError> {
    let bytes: [u8; TIME_KEY_LEN] = key
        .try_into()
        .map_err(|_| TransactionComponentSummaryConsumerError::MalformedTimeKey)?;
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
) -> Result<(i64, BlockHeight, BlockHash), TransactionComponentSummaryConsumerError> {
    if key.len() != CONTRIBUTION_KEY_LEN {
        return Err(TransactionComponentSummaryConsumerError::MalformedContributionKey);
    }
    let unix_seconds = decode_time_key(&key[..TIME_KEY_LEN])?;
    let height = decode_height_key_ascending(&key[TIME_KEY_LEN..TIME_KEY_LEN + HEIGHT_KEY_LEN])
        .map_err(|_| TransactionComponentSummaryConsumerError::MalformedContributionKey)?;
    let block_hash = decode_internal_block_hash(&key[TIME_KEY_LEN + HEIGHT_KEY_LEN..])
        .map_err(|_| TransactionComponentSummaryConsumerError::MalformedContributionKey)?;
    Ok((unix_seconds, height, block_hash))
}

fn decode_index_payload(
    height: BlockHeight,
    payload: &[u8],
) -> Result<[u8; CONTRIBUTION_KEY_LEN], TransactionComponentSummaryConsumerError> {
    payload.try_into().map_err(
        |_| TransactionComponentSummaryConsumerError::MalformedHeightIndex {
            height: height.value(),
            bytes: payload.len(),
        },
    )
}

fn validate_contribution_key(
    key: [u8; CONTRIBUTION_KEY_LEN],
    contribution: BlockContribution,
) -> Result<(), TransactionComponentSummaryConsumerError> {
    let (block_time, _, block_hash) = decode_contribution_key(&key)?;
    if block_hash != contribution.block_hash {
        return Err(TransactionComponentSummaryConsumerError::ContributionHashMismatch);
    }
    if utc_day_start(block_time) != contribution.day.day_start_unix_seconds {
        return Err(
            TransactionComponentSummaryConsumerError::ContributionDayMismatch {
                block_time,
                day_start: contribution.day.day_start_unix_seconds,
            },
        );
    }
    Ok(())
}

fn encode_summary_value(summary: TransactionComponentDay) -> [u8; SUMMARY_VALUE_LEN] {
    let mut payload = [0_u8; SUMMARY_VALUE_LEN];
    payload[..TIME_KEY_LEN].copy_from_slice(&summary.day_start_unix_seconds.to_be_bytes());
    for (index, field) in summary.totals.fields().into_iter().enumerate() {
        let offset = TIME_KEY_LEN + index * size_of::<u64>();
        payload[offset..offset + size_of::<u64>()].copy_from_slice(&field.to_be_bytes());
    }
    let extrema_offset = TIME_KEY_LEN + TOTALS_LEN;
    if let (Some(first), Some(last)) = (
        summary.first_sapling_or_orchard_transaction_time_unix_seconds,
        summary.last_sapling_or_orchard_transaction_time_unix_seconds,
    ) {
        payload[extrema_offset] = 1;
        payload[extrema_offset + 1..extrema_offset + 1 + TIME_KEY_LEN]
            .copy_from_slice(&first.to_be_bytes());
        payload[extrema_offset + 1 + TIME_KEY_LEN..].copy_from_slice(&last.to_be_bytes());
    }
    payload
}

fn decode_summary_value(
    payload: &[u8],
) -> Result<TransactionComponentDay, TransactionComponentSummaryConsumerError> {
    if payload.len() != SUMMARY_VALUE_LEN {
        return Err(
            TransactionComponentSummaryConsumerError::MalformedSummaryValue {
                bytes: payload.len(),
            },
        );
    }
    let day_start_unix_seconds =
        i64::from_be_bytes(payload[..TIME_KEY_LEN].try_into().map_err(|_| {
            TransactionComponentSummaryConsumerError::MalformedSummaryValue {
                bytes: payload.len(),
            }
        })?);
    let mut fields = [0_u64; TOTAL_FIELD_COUNT];
    for (index, field) in fields.iter_mut().enumerate() {
        let offset = TIME_KEY_LEN + index * size_of::<u64>();
        let bytes: [u8; size_of::<u64>()] = payload[offset..offset + size_of::<u64>()]
            .try_into()
            .map_err(|_| {
            TransactionComponentSummaryConsumerError::MalformedSummaryValue {
                bytes: payload.len(),
            }
        })?;
        *field = u64::from_be_bytes(bytes);
    }
    let totals = TransactionComponentTotals::from_fields(fields);
    let (first, last) = decode_summary_extrema(payload)?;
    if (totals.sapling_or_orchard_transaction_count == 0) != first.is_none()
        || first.zip(last).is_some_and(|(first, last)| first > last)
    {
        return Err(
            TransactionComponentSummaryConsumerError::MalformedSummaryValue {
                bytes: payload.len(),
            },
        );
    }
    if first.is_some_and(|first| utc_day_start(first) != day_start_unix_seconds)
        || last.is_some_and(|last| utc_day_start(last) != day_start_unix_seconds)
    {
        return Err(
            TransactionComponentSummaryConsumerError::MalformedSummaryValue {
                bytes: payload.len(),
            },
        );
    }
    Ok(TransactionComponentDay {
        day_start_unix_seconds,
        totals,
        first_sapling_or_orchard_transaction_time_unix_seconds: first,
        last_sapling_or_orchard_transaction_time_unix_seconds: last,
    })
}

fn decode_summary_extrema(
    payload: &[u8],
) -> Result<(Option<i64>, Option<i64>), TransactionComponentSummaryConsumerError> {
    let extrema_offset = TIME_KEY_LEN + TOTALS_LEN;
    match payload[extrema_offset] {
        0 => Ok((None, None)),
        1 => {
            let first = i64::from_be_bytes(
                payload[extrema_offset + 1..extrema_offset + 1 + TIME_KEY_LEN]
                    .try_into()
                    .map_err(|_| malformed_summary_value(payload))?,
            );
            let last = i64::from_be_bytes(
                payload[extrema_offset + 1 + TIME_KEY_LEN..]
                    .try_into()
                    .map_err(|_| malformed_summary_value(payload))?,
            );
            Ok((Some(first), Some(last)))
        }
        _ => Err(malformed_summary_value(payload)),
    }
}

fn malformed_summary_value(payload: &[u8]) -> TransactionComponentSummaryConsumerError {
    TransactionComponentSummaryConsumerError::MalformedSummaryValue {
        bytes: payload.len(),
    }
}

fn encode_coverage(coverage: TransactionComponentBackfillCoverage) -> [u8; COVERAGE_VALUE_LEN] {
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
) -> Result<TransactionComponentBackfillCoverage, TransactionComponentSummaryConsumerError> {
    if payload.len() != COVERAGE_VALUE_LEN {
        return Err(
            TransactionComponentSummaryConsumerError::MalformedCoverage {
                bytes: payload.len(),
            },
        );
    }
    let complete_from_height =
        decode_height_key_ascending(&payload[..HEIGHT_KEY_LEN]).map_err(|_| {
            TransactionComponentSummaryConsumerError::MalformedCoverage {
                bytes: payload.len(),
            }
        })?;
    let complete_through_height =
        decode_height_key_ascending(&payload[HEIGHT_KEY_LEN..2 * HEIGHT_KEY_LEN]).map_err(
            |_| TransactionComponentSummaryConsumerError::MalformedCoverage {
                bytes: payload.len(),
            },
        )?;
    let complete_from_time_unix_seconds = i64::from_be_bytes(
        payload[2 * HEIGHT_KEY_LEN..2 * HEIGHT_KEY_LEN + TIME_KEY_LEN]
            .try_into()
            .map_err(
                |_| TransactionComponentSummaryConsumerError::MalformedCoverage {
                    bytes: payload.len(),
                },
            )?,
    );
    let complete_through_time_unix_seconds = i64::from_be_bytes(
        payload[2 * HEIGHT_KEY_LEN + TIME_KEY_LEN..]
            .try_into()
            .map_err(
                |_| TransactionComponentSummaryConsumerError::MalformedCoverage {
                    bytes: payload.len(),
                },
            )?,
    );
    if complete_from_height > complete_through_height {
        return Err(
            TransactionComponentSummaryConsumerError::MalformedCoverage {
                bytes: payload.len(),
            },
        );
    }
    Ok(TransactionComponentBackfillCoverage::new(
        complete_from_height,
        complete_through_height,
        complete_from_time_unix_seconds,
        complete_through_time_unix_seconds,
    ))
}

fn encode_tail_coverage(
    coverage: TransactionComponentTailCoverage,
) -> [u8; TAIL_COVERAGE_VALUE_LEN] {
    let mut payload = [0_u8; TAIL_COVERAGE_VALUE_LEN];
    payload[..HEIGHT_KEY_LEN]
        .copy_from_slice(&encode_height_key_ascending(coverage.boundary_height));
    if let (Some(through_height), Some(through_time)) = (
        coverage.complete_through_height,
        coverage.complete_through_time_unix_seconds,
    ) {
        payload[HEIGHT_KEY_LEN] = 1;
        payload[HEIGHT_KEY_LEN + 1..HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN]
            .copy_from_slice(&encode_height_key_ascending(through_height));
        payload[HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN..].copy_from_slice(&through_time.to_be_bytes());
    }
    payload
}

fn decode_tail_coverage(
    payload: &[u8],
) -> Result<TransactionComponentTailCoverage, TransactionComponentSummaryConsumerError> {
    if payload.len() != TAIL_COVERAGE_VALUE_LEN {
        return Err(
            TransactionComponentSummaryConsumerError::MalformedTailCoverage {
                bytes: payload.len(),
            },
        );
    }
    let boundary_height =
        decode_height_key_ascending(&payload[..HEIGHT_KEY_LEN]).map_err(|_| {
            TransactionComponentSummaryConsumerError::MalformedTailCoverage {
                bytes: payload.len(),
            }
        })?;
    match payload[HEIGHT_KEY_LEN] {
        0 if payload[HEIGHT_KEY_LEN + 1..].iter().all(|byte| *byte == 0) => Ok(
            TransactionComponentTailCoverage::from_boundary(boundary_height),
        ),
        1 => {
            let complete_through_height = decode_height_key_ascending(
                &payload[HEIGHT_KEY_LEN + 1..HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN],
            )
            .map_err(|_| {
                TransactionComponentSummaryConsumerError::MalformedTailCoverage {
                    bytes: payload.len(),
                }
            })?;
            let complete_through_time_unix_seconds = i64::from_be_bytes(
                payload[HEIGHT_KEY_LEN + 1 + HEIGHT_KEY_LEN..]
                    .try_into()
                    .map_err(|_| {
                        TransactionComponentSummaryConsumerError::MalformedTailCoverage {
                            bytes: payload.len(),
                        }
                    })?,
            );
            if complete_through_height < boundary_height {
                return Err(
                    TransactionComponentSummaryConsumerError::MalformedTailCoverage {
                        bytes: payload.len(),
                    },
                );
            }
            Ok(TransactionComponentTailCoverage {
                boundary_height,
                complete_through_height: Some(complete_through_height),
                complete_through_time_unix_seconds: Some(complete_through_time_unix_seconds),
            })
        }
        _ => Err(
            TransactionComponentSummaryConsumerError::MalformedTailCoverage {
                bytes: payload.len(),
            },
        ),
    }
}

fn store_decode_error(
    error: &TransactionComponentSummaryConsumerError,
) -> MaterializedViewStoreError {
    MaterializedViewStoreError::ConsumerPayloadDecode {
        name: TRANSACTION_COMPONENT_SUMMARY_COVERAGE_COLUMN_FAMILY,
        reason: error.to_string(),
    }
}

/// Failures surfaced by transaction-component materialization and reads.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TransactionComponentSummaryConsumerError {
    /// A query did not specify a non-empty half-open range.
    #[error("transaction-component time range must be non-empty: [{start}, {end})")]
    InvalidTimeRange {
        /// Inclusive range start.
        start: i64,
        /// Exclusive range end.
        end: i64,
    },
    /// UTC-day or half-open endpoint arithmetic overflowed.
    #[error("transaction-component time arithmetic overflowed")]
    TimeOverflow,
    /// Adding stored counters would exceed `u64`.
    #[error("transaction-component counter overflowed u64")]
    CounterOverflow,
    /// A per-block contribution key had the wrong shape.
    #[error("transaction-component contribution key is malformed")]
    MalformedContributionKey,
    /// A signed-time key had the wrong shape.
    #[error("transaction-component time key is malformed")]
    MalformedTimeKey,
    /// A contribution or day aggregate had an invalid encoding.
    #[error("transaction-component summary value has invalid length or extrema ({bytes} bytes)")]
    MalformedSummaryValue {
        /// Stored payload length.
        bytes: usize,
    },
    /// The historical coverage row had an invalid encoding.
    #[error("transaction-component coverage value is malformed ({bytes} bytes)")]
    MalformedCoverage {
        /// Stored payload length.
        bytes: usize,
    },
    /// The seeded live-tail row had an invalid encoding.
    #[error("transaction-component tail coverage value is malformed ({bytes} bytes)")]
    MalformedTailCoverage {
        /// Stored payload length.
        bytes: usize,
    },
    /// A per-height index did not encode exactly one contribution key.
    #[error("transaction-component height index for {height} has invalid length {bytes}")]
    MalformedHeightIndex {
        /// Indexed height.
        height: u32,
        /// Stored payload length.
        bytes: usize,
    },
    /// A contribution's persisted UTC day disagreed with its key time.
    #[error("transaction-component contribution at time {block_time} claims UTC day {day_start}")]
    ContributionDayMismatch {
        /// Block time encoded in the key.
        block_time: i64,
        /// UTC day encoded in the payload.
        day_start: i64,
    },
    /// A height index pointed to a contribution for another height.
    #[error(
        "transaction-component height index requested {requested_height} but stores {indexed_height}"
    )]
    IndexHeightMismatch {
        /// Height being reverted.
        requested_height: u32,
        /// Height encoded in the contribution key.
        indexed_height: u32,
    },
    /// A height index pointed to an absent contribution.
    #[error("transaction-component index at height {height} has no contribution")]
    MissingIndexedContribution {
        /// Indexed height.
        height: u32,
    },
    /// Only one of a height index and its expected contribution existed.
    #[error("transaction-component state at height {height} is incomplete")]
    IncompleteHeightState {
        /// Conflicting height.
        height: u32,
    },
    /// Existing rows disagree with the canonical block being applied.
    #[error("transaction-component state at height {height} conflicts with the canonical block")]
    ConflictingHeight {
        /// Conflicting height.
        height: u32,
    },
    /// A UTC-day aggregate disagreed with its persisted contributions.
    #[error("transaction-component UTC-day aggregate at {day_start} conflicts with contributions")]
    ConflictingDay {
        /// UTC midnight identifying the aggregate.
        day_start: i64,
    },
    /// One atomic batch attempted to mutate a height more than once.
    #[error("transaction-component batch repeats height {height}")]
    DuplicateBatchHeight {
        /// Repeated height.
        height: u32,
    },
    /// Historical writes require at least one block.
    #[error("transaction-component backfill batch cannot be empty")]
    EmptyBackfill,
    /// Historical blocks or requested coverage did not extend contiguously.
    #[error("transaction-component backfill coverage must advance contiguously")]
    CoverageDiscontinuous,
    /// Tail seeding attempted to replace an existing boundary.
    #[error("transaction-component tail boundary conflicts at height {boundary_height}")]
    TailBoundaryConflict {
        /// Requested replacement boundary.
        boundary_height: u32,
    },
    /// Contribution hash and key hash disagree.
    #[error("transaction-component contribution block hash disagrees with its key")]
    ContributionHashMismatch,
    /// Materialized-view store read or write failure.
    #[error(transparent)]
    Store(#[from] MaterializedViewStoreError),
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rust_rocksdb::WriteBatch;
    use tempfile::tempdir;
    use zinder_core::{
        BlockHash, BlockHeight, LockTime, TransactionComponentCounts, TransactionFactsArtifact,
        TransactionId, TransactionLocation, TransactionPublicFacts, TransactionVersion,
        UnsupportedSection, classify_privacy_shape,
    };
    use zinder_store::RocksDbResourceBudget;

    use super::{
        SECONDS_PER_DAY, TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME,
        TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY, TRANSACTION_COMPONENT_SUMMARY_SCHEMA,
        TransactionComponentBackfillCoverage, TransactionComponentSummaryConsumer,
        TransactionComponentSummaryConsumerError, TransactionComponentTailCoverage,
        totals_for_transaction,
    };
    use crate::consumer::{
        BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, MaterializedViewConsumerCtx,
        TransparentSpendFacts,
    };
    use crate::{MaterializedViewStore, MaterializedViewStoreOptions};

    type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

    fn block_hash(seed: u8) -> BlockHash {
        BlockHash::from_bytes([seed; 32])
    }

    fn transaction(
        height: BlockHeight,
        hash: BlockHash,
        index: u32,
        counts: TransactionComponentCounts,
    ) -> TransactionFactsArtifact {
        let mut transaction_id_bytes = [0_u8; 32];
        transaction_id_bytes[..4].copy_from_slice(&height.value().to_be_bytes());
        transaction_id_bytes[4..8].copy_from_slice(&index.to_be_bytes());
        let transaction_id = TransactionId::from_bytes(transaction_id_bytes);
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
                privacy_shape: classify_privacy_shape(counts, false, TransactionVersion::V5),
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
        counts: &[TransactionComponentCounts],
    ) -> BlockCommitContext {
        let height = BlockHeight::new(height);
        let hash = block_hash(hash_seed);
        BlockCommitContext::new(
            BlockCommitInput {
                height,
                block_hash: hash,
                previous_block_hash: block_hash(hash_seed.wrapping_sub(1)),
                block_time_unix_seconds,
                block_size_bytes: 0,
                transactions: counts
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(index, counts)| {
                        transaction(
                            height,
                            hash,
                            u32::try_from(index).unwrap_or(u32::MAX),
                            counts,
                        )
                    })
                    .collect(),
                final_note_commitment_roots: None,
            },
            TransparentSpendFacts::Offline,
        )
    }

    fn open_store() -> TestResult<(tempfile::TempDir, MaterializedViewStore)> {
        let tempdir = tempdir()?;
        let store = MaterializedViewStore::open(
            tempdir.path(),
            MaterializedViewStoreOptions {
                consumers: &[TRANSACTION_COMPONENT_SUMMARY_SCHEMA],
                rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
                sync_writes: false,
                reorg_policy: None,
            },
        )?;
        Ok((tempdir, store))
    }

    fn write_blocks(
        store: &MaterializedViewStore,
        consumer: &mut TransactionComponentSummaryConsumer,
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

    fn replace_blocks(
        store: &MaterializedViewStore,
        consumer: &mut TransactionComponentSummaryConsumer,
        reverted: &[BlockHeight],
        replacements: &[BlockCommitContext],
    ) -> TestResult {
        let mut batch = WriteBatch::default();
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer.begin_batch(&mut ctx)?;
        for height in reverted {
            consumer.revert_block(*height, &mut ctx)?;
        }
        for block in replacements {
            consumer.apply_block(block, &mut ctx)?;
        }
        consumer.finish_batch(&mut ctx)?;
        store.write_batch(&batch)?;
        Ok(())
    }

    #[test]
    fn sapling_or_orchard_predicates_exclude_ironwood_and_sprout() {
        let sapling = TransactionComponentCounts {
            sapling_spend_count: 1,
            sapling_output_count: 2,
            ..TransactionComponentCounts::EMPTY
        };
        let orchard_with_transparent = TransactionComponentCounts {
            transparent_output_count: 1,
            orchard_action_count: 3,
            ..TransactionComponentCounts::EMPTY
        };
        let both = TransactionComponentCounts {
            sapling_output_count: 1,
            orchard_action_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let native_only = TransactionComponentCounts {
            ironwood_action_count: 4,
            sprout_joinsplit_count: 2,
            ..TransactionComponentCounts::EMPTY
        };
        let height = BlockHeight::new(1);
        let hash = block_hash(1);

        let sapling = totals_for_transaction(&transaction(height, hash, 0, sapling));
        assert_eq!(sapling.sapling_without_orchard_transaction_count, 1);
        assert_eq!(
            sapling.sapling_or_orchard_fully_shielded_transaction_count,
            1
        );

        let orchard =
            totals_for_transaction(&transaction(height, hash, 1, orchard_with_transparent));
        assert_eq!(orchard.orchard_without_sapling_transaction_count, 1);
        assert_eq!(
            orchard.sapling_or_orchard_fully_shielded_transaction_count,
            0
        );

        let both = totals_for_transaction(&transaction(height, hash, 2, both));
        assert_eq!(both.sapling_and_orchard_transaction_count, 1);
        assert_eq!(both.sapling_or_orchard_fully_shielded_transaction_count, 1);

        let native = totals_for_transaction(&transaction(height, hash, 3, native_only));
        assert_eq!(native.ironwood_action_count, 4);
        assert_eq!(native.sprout_joinsplit_count, 2);
        assert_eq!(native.ironwood_transaction_count, 1);
        assert_eq!(native.sprout_transaction_count, 1);
        assert_eq!(native.sapling_or_orchard_transaction_count, 0);
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "The predicate truth table keeps every protocol, coinbase, and unsupported case visible in one test."
    )]
    fn native_predicates_preserve_their_exact_protocol_and_coinbase_scope() {
        let height = BlockHeight::new(1);
        let hash = block_hash(1);
        let sprout_only = TransactionComponentCounts {
            sprout_joinsplit_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let mixed = TransactionComponentCounts {
            transparent_input_count: 1,
            transparent_output_count: 1,
            sapling_spend_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let fully_shielded = TransactionComponentCounts {
            ironwood_action_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let transparent = TransactionComponentCounts::EMPTY;
        let overlapping_protocols = TransactionComponentCounts {
            transparent_input_count: 1,
            sapling_output_count: 1,
            orchard_action_count: 1,
            ironwood_action_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let mut shielded_coinbase = transaction(
            height,
            hash,
            3,
            TransactionComponentCounts {
                orchard_action_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
        );
        shielded_coinbase.public_facts.is_coinbase = true;
        let mut transparent_coinbase =
            transaction(height, hash, 4, TransactionComponentCounts::EMPTY);
        transparent_coinbase.public_facts.is_coinbase = true;
        let mut unsupported = transaction(
            height,
            hash,
            5,
            TransactionComponentCounts {
                orchard_action_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
        );
        unsupported.public_facts.is_coinbase = true;
        unsupported
            .public_facts
            .unsupported_sections
            .push(UnsupportedSection::FutureShieldedProtocol);

        let sprout_only = totals_for_transaction(&transaction(height, hash, 0, sprout_only));
        assert_eq!(
            sprout_only.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
            1
        );
        assert_eq!(sprout_only.sapling_orchard_or_ironwood_transaction_count, 0);

        let mixed = totals_for_transaction(&transaction(height, hash, 1, mixed));
        assert_eq!(mixed.sapling_orchard_or_ironwood_transaction_count, 1);
        assert_eq!(
            mixed
                .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
            1
        );
        assert_eq!(
            mixed
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            0
        );

        let fully_shielded = totals_for_transaction(&transaction(height, hash, 2, fully_shielded));
        assert_eq!(
            fully_shielded.sapling_orchard_or_ironwood_transaction_count,
            1
        );
        assert_eq!(
            fully_shielded
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            1
        );
        assert_eq!(
            fully_shielded.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
            0
        );

        let shielded_coinbase = totals_for_transaction(&shielded_coinbase);
        assert_eq!(
            shielded_coinbase.sapling_orchard_or_ironwood_transaction_count,
            1
        );
        assert_eq!(shielded_coinbase.coinbase_transaction_count, 1);
        assert_eq!(
            shielded_coinbase
                .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
            0
        );
        assert_eq!(
            shielded_coinbase
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            0
        );

        let transparent = totals_for_transaction(&transaction(height, hash, 6, transparent));
        assert_eq!(transparent.sapling_orchard_or_ironwood_transaction_count, 0);
        assert_eq!(
            transparent.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
            1
        );
        assert_eq!(transparent.coinbase_transaction_count, 0);

        let transparent_coinbase = totals_for_transaction(&transparent_coinbase);
        assert_eq!(
            transparent_coinbase.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
            0
        );
        assert_eq!(transparent_coinbase.coinbase_transaction_count, 1);

        let overlapping_protocols =
            totals_for_transaction(&transaction(height, hash, 7, overlapping_protocols));
        assert_eq!(overlapping_protocols.sapling_transaction_count, 1);
        assert_eq!(overlapping_protocols.orchard_transaction_count, 1);
        assert_eq!(overlapping_protocols.ironwood_transaction_count, 1);
        assert_eq!(
            overlapping_protocols.sapling_orchard_or_ironwood_transaction_count,
            1
        );
        assert_eq!(
            overlapping_protocols
                .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
            0
        );
        assert_eq!(
            overlapping_protocols
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            0
        );

        let unsupported = totals_for_transaction(&unsupported);
        assert_eq!(unsupported.sapling_orchard_or_ironwood_transaction_count, 0);
        assert_eq!(
            unsupported.non_coinbase_without_sapling_orchard_or_ironwood_transaction_count,
            0
        );
        assert_eq!(
            unsupported
                .non_coinbase_sapling_orchard_or_ironwood_with_transparent_inputs_and_outputs_transaction_count,
            0
        );
        assert_eq!(
            unsupported
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            0
        );
        assert_eq!(unsupported.coinbase_transaction_count, 0);
        assert_eq!(unsupported.transaction_predicate_unavailable_count, 1);
    }

    #[test]
    fn exact_half_open_query_uses_same_day_batch_and_clipped_boundaries() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        let sapling = TransactionComponentCounts {
            sapling_spend_count: 1,
            sapling_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let orchard = TransactionComponentCounts {
            orchard_action_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let ironwood = TransactionComponentCounts {
            ironwood_action_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        let blocks = [
            block(100, 1, 10, &[sapling]),
            block(101, 2, 20, &[orchard]),
            block(102, 3, SECONDS_PER_DAY + 10, &[ironwood]),
        ];
        write_blocks(&store, &mut consumer, &blocks)?;

        let clipped = TransactionComponentSummaryConsumer::summary_in_time_range(
            &store,
            15,
            SECONDS_PER_DAY + 10,
        )?;
        assert_eq!(clipped.totals.transaction_count, 1);
        assert_eq!(clipped.totals.orchard_action_count, 1);
        assert_eq!(clipped.totals.ironwood_action_count, 0);
        assert_eq!(clipped.days.len(), 1);
        assert_eq!(
            clipped.days[0].first_sapling_or_orchard_transaction_time_unix_seconds,
            Some(20)
        );

        let whole_days = TransactionComponentSummaryConsumer::summary_in_time_range(
            &store,
            0,
            2 * SECONDS_PER_DAY,
        )?;
        assert_eq!(whole_days.totals.transaction_count, 3);
        assert_eq!(whole_days.days.len(), 2);
        Ok(())
    }

    #[test]
    fn revert_recomputes_first_and_last_sapling_or_orchard_times() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        let sapling = TransactionComponentCounts {
            sapling_spend_count: 1,
            sapling_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        write_blocks(
            &store,
            &mut consumer,
            &[
                block(100, 1, 10, &[sapling]),
                block(101, 2, 20, &[sapling]),
                block(102, 3, 30, &[sapling]),
            ],
        )?;

        replace_blocks(&store, &mut consumer, &[BlockHeight::new(100)], &[])?;
        let summary =
            TransactionComponentSummaryConsumer::summary_in_time_range(&store, 0, SECONDS_PER_DAY)?;
        assert_eq!(
            summary.days[0].first_sapling_or_orchard_transaction_time_unix_seconds,
            Some(20)
        );
        assert_eq!(
            summary.days[0].last_sapling_or_orchard_transaction_time_unix_seconds,
            Some(30)
        );
        assert_eq!(
            summary.totals.sapling_orchard_or_ironwood_transaction_count,
            2
        );
        assert_eq!(
            summary
                .totals
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            2
        );

        replace_blocks(&store, &mut consumer, &[BlockHeight::new(102)], &[])?;
        let summary =
            TransactionComponentSummaryConsumer::summary_in_time_range(&store, 0, SECONDS_PER_DAY)?;
        assert_eq!(
            summary.days[0].last_sapling_or_orchard_transaction_time_unix_seconds,
            Some(20)
        );
        assert_eq!(
            summary.totals.sapling_orchard_or_ironwood_transaction_count,
            1
        );
        assert_eq!(
            summary
                .totals
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            1
        );
        Ok(())
    }

    #[test]
    fn block_hash_conflict_requires_revert_before_replacement() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        let counts = TransactionComponentCounts {
            orchard_action_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        write_blocks(&store, &mut consumer, &[block(100, 1, 10, &[counts])])?;

        let conflict = write_blocks(&store, &mut consumer, &[block(100, 2, 10, &[counts])]);
        assert!(matches!(
            conflict,
            Err(error) if error.downcast_ref::<TransactionComponentSummaryConsumerError>()
                .is_some_and(|error| matches!(error,
                    TransactionComponentSummaryConsumerError::IncompleteHeightState { .. }
                    | TransactionComponentSummaryConsumerError::ConflictingHeight { .. }))
        ));

        replace_blocks(
            &store,
            &mut consumer,
            &[BlockHeight::new(100)],
            &[block(100, 2, 10, &[counts])],
        )?;
        let summary = TransactionComponentSummaryConsumer::summary_in_time_range(&store, 0, 11)?;
        assert_eq!(summary.totals.transaction_count, 1);
        Ok(())
    }

    #[test]
    fn backfill_joins_seeded_tail_without_advancing_cursor() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        let counts = TransactionComponentCounts {
            ironwood_action_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        TransactionComponentSummaryConsumer::initialize_tail_boundary(
            &store,
            BlockHeight::new(102),
        )?;
        store.put_chain_event_cursor(TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME, b"seeded")?;
        write_blocks(
            &store,
            &mut consumer,
            &[block(102, 3, 30, &[counts]), block(103, 4, 40, &[counts])],
        )?;
        assert_eq!(
            TransactionComponentSummaryConsumer::tail_coverage(&store)?,
            Some(TransactionComponentTailCoverage {
                boundary_height: BlockHeight::new(102),
                complete_through_height: Some(BlockHeight::new(103)),
                complete_through_time_unix_seconds: Some(40),
            })
        );

        consumer.write_backfill_batch(
            &store,
            &[block(100, 1, 10, &[counts]), block(101, 2, 20, &[counts])],
            TransactionComponentBackfillCoverage::new(
                BlockHeight::new(100),
                BlockHeight::new(101),
                10,
                20,
            ),
        )?;

        assert_eq!(
            TransactionComponentSummaryConsumer::coverage(&store)?,
            Some(TransactionComponentBackfillCoverage::new(
                BlockHeight::new(100),
                BlockHeight::new(103),
                10,
                40,
            ))
        );
        assert_eq!(
            store.get_chain_event_cursor(TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME)?,
            Some(b"seeded".to_vec())
        );
        let summary =
            TransactionComponentSummaryConsumer::summary_in_time_range(&store, 0, SECONDS_PER_DAY)?;
        assert_eq!(
            summary.totals.sapling_orchard_or_ironwood_transaction_count,
            4
        );
        assert_eq!(
            summary
                .totals
                .non_coinbase_sapling_orchard_or_ironwood_without_transparent_inputs_or_outputs_transaction_count,
            4
        );
        Ok(())
    }

    #[test]
    fn startup_tail_seed_is_contiguous_reorg_aware_and_cursor_neutral() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        let counts = TransactionComponentCounts {
            sapling_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        TransactionComponentSummaryConsumer::initialize_tail_boundary(
            &store,
            BlockHeight::new(101),
        )?;
        store.put_chain_event_cursor(TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME, b"existing")?;

        consumer.write_tail_seed_batch(
            &store,
            &[block(101, 2, 20, &[counts]), block(102, 3, 30, &[counts])],
        )?;
        assert_eq!(
            TransactionComponentSummaryConsumer::tail_coverage(&store)?,
            Some(TransactionComponentTailCoverage {
                boundary_height: BlockHeight::new(101),
                complete_through_height: Some(BlockHeight::new(102)),
                complete_through_time_unix_seconds: Some(30),
            })
        );
        assert_eq!(
            store.get_chain_event_cursor(TRANSACTION_COMPONENT_SUMMARY_CONSUMER_NAME)?,
            Some(b"existing".to_vec())
        );

        replace_blocks(
            &store,
            &mut consumer,
            &[BlockHeight::new(102)],
            &[block(102, 4, 31, &[counts])],
        )?;
        let summary = TransactionComponentSummaryConsumer::summary_in_time_range(&store, 0, 40)?;
        assert_eq!(summary.totals.transaction_count, 2);
        assert_eq!(
            TransactionComponentSummaryConsumer::tail_coverage(&store)?
                .and_then(|coverage| coverage.complete_through_height),
            Some(BlockHeight::new(102))
        );
        Ok(())
    }

    #[test]
    fn startup_tail_seed_rejects_a_gap() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        TransactionComponentSummaryConsumer::initialize_tail_boundary(
            &store,
            BlockHeight::new(101),
        )?;

        let result = consumer.write_tail_seed_batch(&store, &[block(102, 3, 30, &[])]);
        assert!(matches!(
            result,
            Err(error) if error.downcast_ref::<TransactionComponentSummaryConsumerError>()
                .is_some_and(|error| matches!(
                    error,
                    TransactionComponentSummaryConsumerError::CoverageDiscontinuous
                ))
        ));
        Ok(())
    }

    #[test]
    fn startup_can_widen_and_revalidate_an_existing_tail() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        let counts = TransactionComponentCounts {
            orchard_action_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        TransactionComponentSummaryConsumer::initialize_tail_boundary(
            &store,
            BlockHeight::new(102),
        )?;
        consumer.write_tail_seed_batch(
            &store,
            &[block(102, 3, 30, &[counts]), block(103, 4, 40, &[counts])],
        )?;

        assert!(
            TransactionComponentSummaryConsumer::widen_tail_boundary_for_startup(
                &store,
                BlockHeight::new(101),
            )?
        );
        assert_eq!(
            TransactionComponentSummaryConsumer::tail_coverage(&store)?,
            Some(TransactionComponentTailCoverage::from_boundary(
                BlockHeight::new(101)
            ))
        );

        consumer.write_tail_seed_batch(
            &store,
            &[
                block(101, 2, 20, &[counts]),
                block(102, 3, 30, &[counts]),
                block(103, 4, 40, &[counts]),
            ],
        )?;
        assert_eq!(
            TransactionComponentSummaryConsumer::tail_coverage(&store)?
                .and_then(|coverage| coverage.complete_through_height),
            Some(BlockHeight::new(103))
        );
        let summary = TransactionComponentSummaryConsumer::summary_in_time_range(&store, 0, 50)?;
        assert_eq!(summary.totals.transaction_count, 3);
        Ok(())
    }

    #[test]
    fn malformed_day_aggregate_is_rejected_before_update() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        let counts = TransactionComponentCounts {
            sapling_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        write_blocks(&store, &mut consumer, &[block(100, 1, 10, &[counts])])?;
        store.put_consumer(
            TRANSACTION_COMPONENT_SUMMARY_DAY_COLUMN_FAMILY,
            &super::encode_time_key(0),
            b"malformed",
        )?;

        let outcome = write_blocks(&store, &mut consumer, &[block(101, 2, 20, &[counts])]);
        assert!(outcome.is_err());
        Ok(())
    }

    #[test]
    fn fresh_replay_self_initializes_the_tail_and_backs_coverage() -> TestResult {
        let (_tempdir, store) = open_store()?;
        let mut consumer = TransactionComponentSummaryConsumer::new();
        assert!(TransactionComponentSummaryConsumer::tail_coverage(&store)?.is_none());
        assert!(TransactionComponentSummaryConsumer::coverage(&store)?.is_none());

        let counts = TransactionComponentCounts {
            sapling_output_count: 1,
            ..TransactionComponentCounts::EMPTY
        };
        write_blocks(
            &store,
            &mut consumer,
            &[
                block(1, 1, 1_700_000_000, &[counts]),
                block(2, 2, 1_700_000_600, &[counts]),
            ],
        )?;
        let tail = TransactionComponentSummaryConsumer::tail_coverage(&store)?
            .ok_or("replay must self-initialize the live tail")?;
        assert_eq!(tail.boundary_height, BlockHeight::new(1));
        assert_eq!(tail.complete_through_height, Some(BlockHeight::new(2)));
        assert!(TransactionComponentSummaryConsumer::backfill_coverage(&store)?.is_none());

        let coverage = TransactionComponentSummaryConsumer::coverage(&store)?
            .ok_or("coverage must fall back to the live tail")?;
        assert_eq!(coverage.complete_from_height, BlockHeight::new(1));
        assert_eq!(coverage.complete_through_height, BlockHeight::new(2));
        assert_eq!(coverage.complete_from_time_unix_seconds, 1_700_000_000);
        assert_eq!(coverage.complete_through_time_unix_seconds, 1_700_000_600);
        Ok(())
    }
}
