//! Canonical per-transaction value-pool flow events.
//!
//! The primary key orders events newest-first by block time and then by the
//! stable block-local coordinate `(height, transaction index)`. Values retain
//! the transaction id and every signed intrinsic shielded-pool balance. The
//! consumer deliberately stores no address or product-specific label.

use std::collections::BTreeMap;
use zinder_core::wire::{
    HEIGHT_KEY_LEN, IN_BLOCK_POSITION_KEY_LEN, decode_height_key_ascending,
    decode_height_key_descending, decode_in_block_position, decode_internal_transaction_id,
    encode_height_key_ascending, encode_height_key_descending, encode_in_block_position,
    encode_internal_transaction_id,
};

use rust_rocksdb::WriteBatch;
use zinder_core::{BlockHeight, TransactionId, TransactionIntrinsicValueBalances};

use crate::consumer::{
    BlockCommitContext, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewConsumerError, MaterializedViewConsumerName, MaterializedViewConsumerSchema,
};
use crate::{MaterializedViewStore, MaterializedViewStoreColumnFamily, MaterializedViewStoreError};

/// Column family holding canonical per-transaction flow events.
pub const VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY: &str = "value_pool_flow_history";
/// Column family mapping each height to the event keys written for that block.
pub const VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY: &str = "value_pool_flow_history_index";
/// Column family holding historical and live-tail coverage metadata.
pub const VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY: &str = "value_pool_flow_history_coverage";
/// Column families owned by the consumer.
pub const VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILIES: &[&str] = &[
    VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY,
    VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY,
    VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY,
];
/// Stable consumer identity persisted in materialized-view metadata and cursor rows.
pub const VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME: MaterializedViewConsumerName =
    MaterializedViewConsumerName::from_static("value_pool_flow_history");
/// Initial consumer-local schema.
pub const VALUE_POOL_FLOW_HISTORY_SCHEMA: MaterializedViewConsumerSchema =
    MaterializedViewConsumerSchema::new(
        VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME,
        1,
        VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILIES,
    );

const TIME_KEY_LEN: usize = 8;
const TRANSACTION_ID_LEN: usize = 32;
const POOL_COUNT: usize = 4;
const EVENT_VALUE_LEN: usize = TRANSACTION_ID_LEN + POOL_COUNT * size_of::<i64>();
const COVERAGE_KEY: &[u8] = b"historical_v1";
const TAIL_COVERAGE_KEY: &[u8] = b"live_tail_v1";
const HEIGHT_TIME_PREFIX: u8 = b'h';
const COVERAGE_VALUE_LEN: usize = 2 * HEIGHT_KEY_LEN + 2 * TIME_KEY_LEN;
const TAIL_COVERAGE_VALUE_LEN: usize = 1 + 2 * HEIGHT_KEY_LEN + TIME_KEY_LEN;

/// Length of a flow-event key.
pub const VALUE_POOL_FLOW_HISTORY_KEY_LEN: usize =
    TIME_KEY_LEN + HEIGHT_KEY_LEN + IN_BLOCK_POSITION_KEY_LEN;

/// Direction of transparent value relative to shielded pools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValuePoolFlowDirection {
    /// Negative aggregate shielded balance: transparent value enters shielding.
    Shield,
    /// Positive aggregate shielded balance: shielded value exits to transparent value.
    Deshield,
}

/// Shielded pool attribution for one net flow event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValuePoolFlowPool {
    /// Only the Sprout balance is nonzero.
    Sprout,
    /// Only the Sapling balance is nonzero.
    Sapling,
    /// Only the Orchard balance is nonzero.
    Orchard,
    /// Only the Ironwood balance is nonzero.
    Ironwood,
    /// More than one shielded pool has a nonzero balance.
    Mixed,
}

/// One canonical transaction whose net shielded balance crosses the
/// transparent boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValuePoolFlowEvent {
    /// Canonical transaction identifier.
    pub transaction_id: TransactionId,
    /// Height of the containing canonical block.
    pub block_height: BlockHeight,
    /// Containing block time as Unix seconds.
    pub block_time_unix_seconds: i64,
    /// Stable transaction coordinate within the block.
    pub transaction_index_in_block: u32,
    /// Signed transaction-intrinsic balances for every shielded pool.
    pub pool_balances: TransactionIntrinsicValueBalances,
}

/// A persisted flow event together with its stable continuation position.
///
/// The continuation key is storage-local. Explorer callers encode it into an
/// opaque cursor rather than exposing it through a product-facing contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValuePoolFlowHistoryRow {
    /// Decoded canonical flow event.
    pub event: ValuePoolFlowEvent,
    continuation_key: [u8; VALUE_POOL_FLOW_HISTORY_KEY_LEN],
}

impl ValuePoolFlowHistoryRow {
    /// Returns the stable key used to continue a newest-first scan.
    #[must_use]
    pub const fn continuation_key(&self) -> &[u8; VALUE_POOL_FLOW_HISTORY_KEY_LEN] {
        &self.continuation_key
    }
}

impl ValuePoolFlowEvent {
    /// Returns whether this event belongs to the block's coinbase transaction.
    ///
    /// Consensus blocks place the coinbase transaction at index zero. Older
    /// materialized-view rows can contain shielded coinbase payouts, so readers use
    /// this structural fact to exclude issuance from transparent-boundary flow
    /// analytics without requiring a materialized-view replay.
    #[must_use]
    pub const fn is_coinbase(self) -> bool {
        self.transaction_index_in_block == 0
    }

    /// Returns the signed aggregate balance across shielded pools.
    pub fn net_balance_zat(self) -> Result<i64, ValuePoolFlowHistoryConsumerError> {
        checked_net_balance(self.pool_balances)
    }

    /// Returns the unsigned net flow amount in zatoshi.
    pub fn amount_zat(self) -> Result<u64, ValuePoolFlowHistoryConsumerError> {
        Ok(self.net_balance_zat()?.unsigned_abs())
    }

    /// Classifies the net flow direction.
    pub fn direction(self) -> Result<ValuePoolFlowDirection, ValuePoolFlowHistoryConsumerError> {
        match self.net_balance_zat()? {
            ..=-1 => Ok(ValuePoolFlowDirection::Shield),
            1.. => Ok(ValuePoolFlowDirection::Deshield),
            0 => Err(ValuePoolFlowHistoryConsumerError::ZeroNetFlow),
        }
    }

    /// Attributes the event to one pool or to a mixed-pool transaction.
    #[must_use]
    pub fn pool(self) -> ValuePoolFlowPool {
        let balances = self.pool_balances;
        let nonzero = [
            balances.sprout_zat != 0,
            balances.sapling_zat != 0,
            balances.orchard_zat != 0,
            balances.ironwood_zat != 0,
        ];
        match nonzero {
            [true, false, false, false] => ValuePoolFlowPool::Sprout,
            [false, true, false, false] => ValuePoolFlowPool::Sapling,
            [false, false, true, false] => ValuePoolFlowPool::Orchard,
            [false, false, false, true] => ValuePoolFlowPool::Ironwood,
            _ => ValuePoolFlowPool::Mixed,
        }
    }
}

/// Durable contiguous historical range materialized by the backfill.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValuePoolFlowBackfillCoverage {
    /// First completely materialized height.
    pub complete_from_height: BlockHeight,
    /// Last completely materialized height.
    pub complete_through_height: BlockHeight,
    /// Block time at the first height.
    pub complete_from_time_unix_seconds: i64,
    /// Block time at the last height.
    pub complete_through_time_unix_seconds: i64,
}

impl ValuePoolFlowBackfillCoverage {
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

/// Durable live-tail interval established when ingest seeds the cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValuePoolFlowTailCoverage {
    /// First height owned by the seeded live tail.
    pub boundary_height: BlockHeight,
    /// Last contiguous tail height, absent before the first block.
    pub complete_through_height: Option<BlockHeight>,
    /// Block time at the complete-through height.
    pub complete_through_time_unix_seconds: Option<i64>,
}

impl ValuePoolFlowTailCoverage {
    /// Creates an empty tail at `boundary_height`.
    #[must_use]
    pub const fn from_boundary(boundary_height: BlockHeight) -> Self {
        Self {
            boundary_height,
            complete_through_height: None,
            complete_through_time_unix_seconds: None,
        }
    }
}

/// Materializes one neutral flow event per qualifying canonical transaction.
#[derive(Default)]
pub struct ValuePoolFlowHistoryConsumer {
    pending_height_times: BTreeMap<BlockHeight, Option<i64>>,
}

impl ValuePoolFlowHistoryConsumer {
    /// Builds the consumer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pending_height_times: BTreeMap::new(),
        }
    }

    /// Reads durable historical coverage, when backfill has started.
    pub fn backfill_coverage(
        store: &crate::MaterializedViewStore,
    ) -> Result<Option<ValuePoolFlowBackfillCoverage>, MaterializedViewStoreError> {
        store
            .get_consumer(VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY, COVERAGE_KEY)?
            .map(|bytes| decode_coverage(&bytes).map_err(|error| store_decode_error(&error)))
            .transpose()
    }

    /// Reads the seeded live-tail boundary and contiguous endpoint.
    pub fn tail_coverage(
        store: &crate::MaterializedViewStore,
    ) -> Result<Option<ValuePoolFlowTailCoverage>, MaterializedViewStoreError> {
        store
            .get_consumer(
                VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY,
                TAIL_COVERAGE_KEY,
            )?
            .map(|bytes| decode_tail_coverage(&bytes).map_err(|error| store_decode_error(&error)))
            .transpose()
    }

    /// Initializes or widens the startup tail without deleting materialized-view rows.
    pub fn widen_tail_boundary_for_startup(
        store: &crate::MaterializedViewStore,
        boundary_height: BlockHeight,
    ) -> Result<bool, ValuePoolFlowHistoryConsumerError> {
        if Self::tail_coverage(store)?.is_some_and(|tail| boundary_height >= tail.boundary_height) {
            return Ok(false);
        }
        store.put_consumer(
            VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY,
            TAIL_COVERAGE_KEY,
            &encode_tail_coverage(ValuePoolFlowTailCoverage::from_boundary(boundary_height)),
        )?;
        Ok(true)
    }

    /// Atomically writes an ordered historical batch and its coverage.
    pub fn write_backfill_batch(
        &mut self,
        store: &crate::MaterializedViewStore,
        blocks: &[BlockCommitContext],
        next_coverage: ValuePoolFlowBackfillCoverage,
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
        let cf = store.consumer_column_family(VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY)?;
        ctx.batch
            .put_cf(&cf, COVERAGE_KEY, encode_coverage(next_coverage));
        store.write_consumer_batch(VALUE_POOL_FLOW_HISTORY_SCHEMA.name, ctx.batch)?;
        Ok(())
    }

    /// Atomically seeds already-visible tail blocks without advancing a cursor.
    pub fn write_tail_seed_batch(
        &mut self,
        store: &crate::MaterializedViewStore,
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
        store.write_consumer_batch(VALUE_POOL_FLOW_HISTORY_SCHEMA.name, ctx.batch)?;
        Ok(())
    }

    /// Decodes one primary store row.
    pub fn decode_event(
        key: &[u8],
        payload: &[u8],
    ) -> Result<ValuePoolFlowEvent, ValuePoolFlowHistoryConsumerError> {
        let (block_time_unix_seconds, block_height, transaction_index_in_block) =
            decode_event_key(key)?;
        let pool_balances = decode_event_value(payload)?;
        let event = ValuePoolFlowEvent {
            transaction_id: decode_internal_transaction_id(&payload[..TRANSACTION_ID_LEN])
                .map_err(|_| ValuePoolFlowHistoryConsumerError::MalformedEventValue {
                    bytes: payload.len(),
                })?,
            block_height,
            block_time_unix_seconds,
            transaction_index_in_block,
            pool_balances,
        };
        if event.net_balance_zat()? == 0 {
            return Err(ValuePoolFlowHistoryConsumerError::ZeroNetFlow);
        }
        Ok(event)
    }

    /// Reads a bounded newest-first page after an optional opaque continuation
    /// key. The continuation row itself is excluded.
    pub fn read_page_after(
        store: &MaterializedViewStore,
        after: Option<&[u8; VALUE_POOL_FLOW_HISTORY_KEY_LEN]>,
        entries_cap: usize,
    ) -> Result<Vec<ValuePoolFlowHistoryRow>, ValuePoolFlowHistoryConsumerError> {
        if entries_cap == 0 {
            return Ok(Vec::new());
        }
        let start_key = after.map_or_else(|| [0_u8; VALUE_POOL_FLOW_HISTORY_KEY_LEN], |key| *key);
        let fetch_cap = entries_cap.saturating_add(usize::from(after.is_some()));
        let rows = store.range_iterate_consumer(
            VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY,
            &start_key,
            &[u8::MAX; VALUE_POOL_FLOW_HISTORY_KEY_LEN],
            fetch_cap,
        )?;
        rows.into_iter()
            .filter(|(key, _)| after.is_none_or(|after| key.as_slice() != after))
            .take(entries_cap)
            .map(|(key, payload)| {
                let continuation_key: [u8; VALUE_POOL_FLOW_HISTORY_KEY_LEN] =
                    key.as_slice().try_into().map_err(|_| {
                        ValuePoolFlowHistoryConsumerError::MalformedEventKey { bytes: key.len() }
                    })?;
                Ok(ValuePoolFlowHistoryRow {
                    event: Self::decode_event(&key, &payload)?,
                    continuation_key,
                })
            })
            .collect()
    }

    /// Reads every event in a half-open Unix-time range, bounded by `entries_cap`.
    pub fn events_in_time_range(
        store: &MaterializedViewStore,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
        entries_cap: usize,
    ) -> Result<Vec<ValuePoolFlowEvent>, ValuePoolFlowHistoryConsumerError> {
        if start_time_unix_seconds >= end_time_unix_seconds || entries_cap == 0 {
            return Ok(Vec::new());
        }
        let end_inclusive = end_time_unix_seconds.checked_sub(1).ok_or(
            ValuePoolFlowHistoryConsumerError::InvalidTimeRange {
                start_time_unix_seconds,
                end_time_unix_seconds,
            },
        )?;
        let start_key = encode_event_key(end_inclusive, BlockHeight::new(u32::MAX), u32::MAX);
        let end_key = encode_event_key(start_time_unix_seconds, BlockHeight::new(0), 0);
        store
            .range_iterate_consumer(
                VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY,
                &start_key,
                &end_key,
                entries_cap,
            )?
            .into_iter()
            .map(|(key, payload)| Self::decode_event(&key, &payload))
            .collect()
    }

    /// Visits every decoded event in a half-open Unix-time range without
    /// retaining an intermediate event collection.
    ///
    /// Malformed rows and visitor errors fail the entire scan closed.
    pub fn visit_events_in_time_range(
        store: &MaterializedViewStore,
        start_time_unix_seconds: i64,
        end_time_unix_seconds: i64,
        mut visitor: impl FnMut(ValuePoolFlowEvent) -> Result<(), String>,
    ) -> Result<(), ValuePoolFlowHistoryConsumerError> {
        if start_time_unix_seconds >= end_time_unix_seconds {
            return Err(ValuePoolFlowHistoryConsumerError::InvalidTimeRange {
                start_time_unix_seconds,
                end_time_unix_seconds,
            });
        }
        let end_inclusive = end_time_unix_seconds.checked_sub(1).ok_or(
            ValuePoolFlowHistoryConsumerError::InvalidTimeRange {
                start_time_unix_seconds,
                end_time_unix_seconds,
            },
        )?;
        let start_key = encode_event_key(end_inclusive, BlockHeight::new(u32::MAX), u32::MAX);
        let end_key = encode_event_key(start_time_unix_seconds, BlockHeight::new(0), 0);
        store.visit_consumer_range(
            VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY,
            start_key.as_slice()..=end_key.as_slice(),
            |key, payload| {
                let event = Self::decode_event(key, payload).map_err(|error| error.to_string())?;
                visitor(event)
            },
        )?;
        Ok(())
    }
}

impl BlockKeyedConsumer for ValuePoolFlowHistoryConsumer {
    fn name(&self) -> MaterializedViewConsumerName {
        VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME
    }

    fn begin_batch(
        &mut self,
        _ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        self.pending_height_times.clear();
        Ok(())
    }

    fn apply_block(
        &mut self,
        block: &BlockCommitContext,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let intrinsic_balances = block.transaction_intrinsic_value_balances();
        let mut event_rows = Vec::new();
        for transaction in &block.transactions {
            if transaction.public_facts.is_coinbase {
                continue;
            }
            let counts = transaction.public_facts.counts;
            if !counts.has_transparent_input() && !counts.has_transparent_output() {
                continue;
            }
            let transaction_id = transaction.location.transaction_id;
            let pool_balances = intrinsic_balances
                .as_deref()
                .and_then(|balances| balances.get(&transaction_id))
                .copied()
                .ok_or_else(
                    || ValuePoolFlowHistoryConsumerError::MissingIntrinsicBalances {
                        transaction_id,
                        height: block.height.value(),
                    },
                )?;
            if checked_net_balance(pool_balances)? == 0 {
                continue;
            }
            let key = encode_event_key(
                block.block_time_unix_seconds,
                block.height,
                transaction.location.tx_index_in_block,
            );
            event_rows.push((key, encode_event_value(transaction_id, pool_balances)));
        }

        let event_cf = ctx
            .store
            .consumer_column_family(VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY)?;
        let mut index_payload =
            Vec::with_capacity(event_rows.len() * VALUE_POOL_FLOW_HISTORY_KEY_LEN);
        for (key, payload) in event_rows {
            index_payload.extend_from_slice(&key);
            ctx.batch.put_cf(&event_cf, key, payload);
        }
        ctx.batch.put_cf(
            &index_cf,
            encode_height_key_ascending(block.height),
            index_payload,
        );
        self.pending_height_times
            .insert(block.height, Some(block.block_time_unix_seconds));
        Ok(())
    }

    fn revert_block(
        &mut self,
        height: BlockHeight,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let index_key = encode_height_key_ascending(height);
        let Some(index_payload) = ctx
            .store
            .get_consumer(VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY, &index_key)?
        else {
            return Ok(());
        };
        let event_keys = decode_height_index(height, &index_payload)?;
        let event_cf = ctx
            .store
            .consumer_column_family(VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY)?;
        let index_cf = ctx
            .store
            .consumer_column_family(VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY)?;
        for event_key in event_keys {
            ctx.batch.delete_cf(&event_cf, event_key);
        }
        ctx.batch.delete_cf(&index_cf, index_key);
        self.pending_height_times.insert(height, None);
        Ok(())
    }

    fn finish_batch(
        &mut self,
        ctx: &mut MaterializedViewConsumerCtx<'_>,
    ) -> Result<(), MaterializedViewConsumerError> {
        let coverage_cf = ctx
            .store
            .consumer_column_family(VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY)?;
        for (height, time) in &self.pending_height_times {
            let key = encode_height_time_key(*height);
            match time {
                Some(time) => ctx.batch.put_cf(&coverage_cf, key, time.to_be_bytes()),
                None => ctx.batch.delete_cf(&coverage_cf, key),
            }
        }
        stage_tail_coverage(ctx, &self.pending_height_times)?;
        self.pending_height_times.clear();
        Ok(())
    }
}

fn checked_net_balance(
    balances: TransactionIntrinsicValueBalances,
) -> Result<i64, ValuePoolFlowHistoryConsumerError> {
    [
        balances.sprout_zat,
        balances.sapling_zat,
        balances.orchard_zat,
        balances.ironwood_zat,
    ]
    .into_iter()
    .try_fold(0_i64, |total, balance| {
        total
            .checked_add(balance)
            .ok_or(ValuePoolFlowHistoryConsumerError::NetBalanceOverflow)
    })
}

fn encode_event_key(
    block_time_unix_seconds: i64,
    height: BlockHeight,
    transaction_index_in_block: u32,
) -> [u8; VALUE_POOL_FLOW_HISTORY_KEY_LEN] {
    let mut key = [0_u8; VALUE_POOL_FLOW_HISTORY_KEY_LEN];
    key[..TIME_KEY_LEN].copy_from_slice(&encode_time_descending(block_time_unix_seconds));
    key[TIME_KEY_LEN..TIME_KEY_LEN + HEIGHT_KEY_LEN]
        .copy_from_slice(&encode_height_key_descending(height));
    key[TIME_KEY_LEN + HEIGHT_KEY_LEN..].copy_from_slice(&encode_in_block_position(
        u32::MAX - transaction_index_in_block,
    ));
    key
}

fn decode_event_key(
    key: &[u8],
) -> Result<(i64, BlockHeight, u32), ValuePoolFlowHistoryConsumerError> {
    if key.len() != VALUE_POOL_FLOW_HISTORY_KEY_LEN {
        return Err(ValuePoolFlowHistoryConsumerError::MalformedEventKey { bytes: key.len() });
    }
    let block_time_unix_seconds = decode_time_descending(&key[..TIME_KEY_LEN])?;
    let block_height = decode_height_key_descending(
        &key[TIME_KEY_LEN..TIME_KEY_LEN + HEIGHT_KEY_LEN],
    )
    .map_err(|_| ValuePoolFlowHistoryConsumerError::MalformedEventKey { bytes: key.len() })?;
    let descending_position = decode_in_block_position(&key[TIME_KEY_LEN + HEIGHT_KEY_LEN..])
        .map_err(|_| ValuePoolFlowHistoryConsumerError::MalformedEventKey { bytes: key.len() })?;
    Ok((
        block_time_unix_seconds,
        block_height,
        u32::MAX - descending_position,
    ))
}

fn encode_time_descending(unix_seconds: i64) -> [u8; TIME_KEY_LEN] {
    (!(unix_seconds.cast_unsigned() ^ (1_u64 << 63))).to_be_bytes()
}

fn decode_time_descending(key: &[u8]) -> Result<i64, ValuePoolFlowHistoryConsumerError> {
    let bytes: [u8; TIME_KEY_LEN] = key
        .try_into()
        .map_err(|_| ValuePoolFlowHistoryConsumerError::MalformedEventKey { bytes: key.len() })?;
    Ok(((!u64::from_be_bytes(bytes)) ^ (1_u64 << 63)).cast_signed())
}

fn encode_event_value(
    transaction_id: TransactionId,
    balances: TransactionIntrinsicValueBalances,
) -> [u8; EVENT_VALUE_LEN] {
    let mut payload = [0_u8; EVENT_VALUE_LEN];
    payload[..TRANSACTION_ID_LEN].copy_from_slice(&encode_internal_transaction_id(transaction_id));
    for (index, balance) in [
        balances.sprout_zat,
        balances.sapling_zat,
        balances.orchard_zat,
        balances.ironwood_zat,
    ]
    .into_iter()
    .enumerate()
    {
        let start = TRANSACTION_ID_LEN + index * size_of::<i64>();
        payload[start..start + size_of::<i64>()].copy_from_slice(&balance.to_be_bytes());
    }
    payload
}

fn decode_event_value(
    payload: &[u8],
) -> Result<TransactionIntrinsicValueBalances, ValuePoolFlowHistoryConsumerError> {
    if payload.len() != EVENT_VALUE_LEN {
        return Err(ValuePoolFlowHistoryConsumerError::MalformedEventValue {
            bytes: payload.len(),
        });
    }
    let mut balances = [0_i64; POOL_COUNT];
    for (index, balance) in balances.iter_mut().enumerate() {
        let start = TRANSACTION_ID_LEN + index * size_of::<i64>();
        let bytes: [u8; size_of::<i64>()] = payload[start..start + size_of::<i64>()]
            .try_into()
            .map_err(|_| ValuePoolFlowHistoryConsumerError::MalformedEventValue {
                bytes: payload.len(),
            })?;
        *balance = i64::from_be_bytes(bytes);
    }
    Ok(TransactionIntrinsicValueBalances::new(
        balances[0],
        balances[1],
        balances[2],
        balances[3],
    ))
}

fn decode_height_index(
    height: BlockHeight,
    payload: &[u8],
) -> Result<Vec<[u8; VALUE_POOL_FLOW_HISTORY_KEY_LEN]>, ValuePoolFlowHistoryConsumerError> {
    if !payload
        .len()
        .is_multiple_of(VALUE_POOL_FLOW_HISTORY_KEY_LEN)
    {
        return Err(ValuePoolFlowHistoryConsumerError::MalformedHeightIndex {
            height: height.value(),
            bytes: payload.len(),
        });
    }
    let mut keys = Vec::with_capacity(payload.len() / VALUE_POOL_FLOW_HISTORY_KEY_LEN);
    for &key in payload.as_chunks::<VALUE_POOL_FLOW_HISTORY_KEY_LEN>().0 {
        let (_, indexed_height, _) = decode_event_key(&key)?;
        if indexed_height != height {
            return Err(ValuePoolFlowHistoryConsumerError::IndexHeightMismatch {
                requested_height: height.value(),
                indexed_height: indexed_height.value(),
            });
        }
        keys.push(key);
    }
    Ok(keys)
}

fn validate_backfill_batch(
    store: &MaterializedViewStore,
    blocks: &[BlockCommitContext],
    next: ValuePoolFlowBackfillCoverage,
) -> Result<(), MaterializedViewConsumerError> {
    let Some(first) = blocks.first() else {
        return Err(Box::new(ValuePoolFlowHistoryConsumerError::EmptyBackfill));
    };
    let last = blocks
        .last()
        .ok_or(ValuePoolFlowHistoryConsumerError::EmptyBackfill)?;
    let existing = ValuePoolFlowHistoryConsumer::backfill_coverage(store)?;
    let starts_contiguously = existing.map_or(
        first.height == next.complete_from_height
            && first.block_time_unix_seconds == next.complete_from_time_unix_seconds,
        |coverage| {
            coverage.complete_from_height == next.complete_from_height
                && coverage.complete_from_time_unix_seconds == next.complete_from_time_unix_seconds
                && coverage.complete_through_height.next() == Some(first.height)
        },
    );
    if starts_contiguously
        && blocks
            .windows(2)
            .all(|pair| pair[0].height.next() == Some(pair[1].height))
        && last.height == next.complete_through_height
        && last.block_time_unix_seconds == next.complete_through_time_unix_seconds
    {
        Ok(())
    } else {
        Err(Box::new(
            ValuePoolFlowHistoryConsumerError::CoverageDiscontinuous,
        ))
    }
}

fn validate_tail_seed_batch(
    store: &MaterializedViewStore,
    blocks: &[BlockCommitContext],
) -> Result<(), MaterializedViewConsumerError> {
    let Some(first) = blocks.first() else {
        return Err(Box::new(ValuePoolFlowHistoryConsumerError::EmptyBackfill));
    };
    let tail = ValuePoolFlowHistoryConsumer::tail_coverage(store)?.ok_or_else(|| {
        Box::new(ValuePoolFlowHistoryConsumerError::CoverageDiscontinuous)
            as MaterializedViewConsumerError
    })?;
    let expected = tail
        .complete_through_height
        .map_or(Some(tail.boundary_height), BlockHeight::next);
    if expected == Some(first.height)
        && blocks
            .windows(2)
            .all(|pair| pair[0].height.next() == Some(pair[1].height))
    {
        Ok(())
    } else {
        Err(Box::new(
            ValuePoolFlowHistoryConsumerError::CoverageDiscontinuous,
        ))
    }
}

fn stage_tail_coverage(
    ctx: &mut MaterializedViewConsumerCtx<'_>,
    pending: &BTreeMap<BlockHeight, Option<i64>>,
) -> Result<(), MaterializedViewConsumerError> {
    let mut tail = if let Some(tail) = ValuePoolFlowHistoryConsumer::tail_coverage(ctx.store)? {
        tail
    } else {
        let Some(boundary_height) = pending
            .iter()
            .find_map(|(height, time)| time.map(|_| *height))
        else {
            return Ok(());
        };
        ValuePoolFlowTailCoverage::from_boundary(boundary_height)
    };
    while let Some(through) = tail.complete_through_height {
        if height_time_after_batch(ctx.store, pending, through)?.is_some() {
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
            height_time_after_batch(ctx.store, pending, previous)?;
    }
    while let Some(candidate) = tail
        .complete_through_height
        .map_or(Some(tail.boundary_height), BlockHeight::next)
    {
        let Some(time) = height_time_after_batch(ctx.store, pending, candidate)? else {
            break;
        };
        tail.complete_through_height = Some(candidate);
        tail.complete_through_time_unix_seconds = Some(time);
    }
    let cf = ctx
        .store
        .consumer_column_family(VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY)?;
    ctx.batch
        .put_cf(&cf, TAIL_COVERAGE_KEY, encode_tail_coverage(tail));
    Ok(())
}

fn height_time_after_batch(
    store: &MaterializedViewStore,
    pending: &BTreeMap<BlockHeight, Option<i64>>,
    height: BlockHeight,
) -> Result<Option<i64>, ValuePoolFlowHistoryConsumerError> {
    if let Some(time) = pending.get(&height) {
        return Ok(*time);
    }
    let Some(bytes) = store.get_consumer(
        VALUE_POOL_FLOW_HISTORY_COVERAGE_COLUMN_FAMILY,
        &encode_height_time_key(height),
    )?
    else {
        return Ok(None);
    };
    let length = bytes.len();
    let encoded: [u8; TIME_KEY_LEN] = bytes
        .try_into()
        .map_err(|_| ValuePoolFlowHistoryConsumerError::MalformedCoverageValue { bytes: length })?;
    Ok(Some(i64::from_be_bytes(encoded)))
}

fn encode_height_time_key(height: BlockHeight) -> [u8; 1 + HEIGHT_KEY_LEN] {
    let mut key = [0_u8; 1 + HEIGHT_KEY_LEN];
    key[0] = HEIGHT_TIME_PREFIX;
    key[1..].copy_from_slice(&encode_height_key_ascending(height));
    key
}

fn encode_coverage(coverage: ValuePoolFlowBackfillCoverage) -> [u8; COVERAGE_VALUE_LEN] {
    let mut bytes = [0_u8; COVERAGE_VALUE_LEN];
    bytes[..4].copy_from_slice(&encode_height_key_ascending(coverage.complete_from_height));
    bytes[4..8].copy_from_slice(&encode_height_key_ascending(
        coverage.complete_through_height,
    ));
    bytes[8..16].copy_from_slice(&coverage.complete_from_time_unix_seconds.to_be_bytes());
    bytes[16..].copy_from_slice(&coverage.complete_through_time_unix_seconds.to_be_bytes());
    bytes
}

fn decode_coverage(
    bytes: &[u8],
) -> Result<ValuePoolFlowBackfillCoverage, ValuePoolFlowHistoryConsumerError> {
    if bytes.len() != COVERAGE_VALUE_LEN {
        return Err(ValuePoolFlowHistoryConsumerError::MalformedCoverageValue {
            bytes: bytes.len(),
        });
    }
    let malformed =
        || ValuePoolFlowHistoryConsumerError::MalformedCoverageValue { bytes: bytes.len() };
    let from = decode_height_key_ascending(&bytes[..4]).map_err(|_| malformed())?;
    let through = decode_height_key_ascending(&bytes[4..8]).map_err(|_| malformed())?;
    let from_time = i64::from_be_bytes(bytes[8..16].try_into().map_err(|_| malformed())?);
    let through_time = i64::from_be_bytes(bytes[16..].try_into().map_err(|_| malformed())?);
    if from > through {
        return Err(ValuePoolFlowHistoryConsumerError::CoverageDiscontinuous);
    }
    Ok(ValuePoolFlowBackfillCoverage::new(
        from,
        through,
        from_time,
        through_time,
    ))
}

fn encode_tail_coverage(coverage: ValuePoolFlowTailCoverage) -> [u8; TAIL_COVERAGE_VALUE_LEN] {
    let mut bytes = [0_u8; TAIL_COVERAGE_VALUE_LEN];
    bytes[1..5].copy_from_slice(&encode_height_key_ascending(coverage.boundary_height));
    if let (Some(through), Some(time)) = (
        coverage.complete_through_height,
        coverage.complete_through_time_unix_seconds,
    ) {
        bytes[0] = 1;
        bytes[5..9].copy_from_slice(&encode_height_key_ascending(through));
        bytes[9..].copy_from_slice(&time.to_be_bytes());
    }
    bytes
}

fn decode_tail_coverage(
    bytes: &[u8],
) -> Result<ValuePoolFlowTailCoverage, ValuePoolFlowHistoryConsumerError> {
    if bytes.len() != TAIL_COVERAGE_VALUE_LEN {
        return Err(ValuePoolFlowHistoryConsumerError::MalformedCoverageValue {
            bytes: bytes.len(),
        });
    }
    let malformed =
        || ValuePoolFlowHistoryConsumerError::MalformedCoverageValue { bytes: bytes.len() };
    let boundary = decode_height_key_ascending(&bytes[1..5]).map_err(|_| malformed())?;
    match bytes[0] {
        0 if bytes[5..].iter().all(|byte| *byte == 0) => {
            Ok(ValuePoolFlowTailCoverage::from_boundary(boundary))
        }
        1 => {
            let through = decode_height_key_ascending(&bytes[5..9]).map_err(|_| malformed())?;
            let time = i64::from_be_bytes(bytes[9..].try_into().map_err(|_| malformed())?);
            if through < boundary {
                return Err(ValuePoolFlowHistoryConsumerError::CoverageDiscontinuous);
            }
            Ok(ValuePoolFlowTailCoverage {
                boundary_height: boundary,
                complete_through_height: Some(through),
                complete_through_time_unix_seconds: Some(time),
            })
        }
        _ => Err(malformed()),
    }
}

fn store_decode_error(error: &ValuePoolFlowHistoryConsumerError) -> MaterializedViewStoreError {
    MaterializedViewStoreError::Decode {
        column_family: MaterializedViewStoreColumnFamily::ConsumerMetadata,
        reason: error.to_string(),
    }
}

/// Consumer-specific failure modes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ValuePoolFlowHistoryConsumerError {
    /// Materialized-view store operation failed.
    #[error(transparent)]
    Store(#[from] MaterializedViewStoreError),
    /// A transparent-participating transaction lacks its intrinsic balances.
    #[error(
        "transaction {transaction_id:?} at height {height} is missing intrinsic value balances"
    )]
    MissingIntrinsicBalances {
        /// Transaction missing the required canonical artifact.
        transaction_id: TransactionId,
        /// Containing block height.
        height: u32,
    },
    /// The four signed pool balances cannot be summed in `i64`.
    #[error("value-pool net balance overflow")]
    NetBalanceOverflow,
    /// A stored or constructed event has no net transparent/shielded flow.
    #[error("value-pool flow event has zero net balance")]
    ZeroNetFlow,
    /// A half-open time range has no valid ordering.
    #[error(
        "value-pool flow time range [{start_time_unix_seconds}, {end_time_unix_seconds}) is invalid"
    )]
    InvalidTimeRange {
        /// Inclusive lower timestamp.
        start_time_unix_seconds: i64,
        /// Exclusive upper timestamp.
        end_time_unix_seconds: i64,
    },
    /// Primary key bytes do not match schema v1.
    #[error("value-pool flow event key has invalid length {bytes}")]
    MalformedEventKey {
        /// Observed byte length.
        bytes: usize,
    },
    /// Primary value bytes do not match schema v1.
    #[error("value-pool flow event value has invalid length {bytes}")]
    MalformedEventValue {
        /// Observed byte length.
        bytes: usize,
    },
    /// Per-height rewind index bytes do not contain whole event keys.
    #[error("value-pool flow height {height} index has invalid length {bytes}")]
    MalformedHeightIndex {
        /// Height owning the index row.
        height: u32,
        /// Observed byte length.
        bytes: usize,
    },
    /// A rewind index points at a different height.
    #[error(
        "value-pool flow height index mismatch: requested {requested_height}, indexed {indexed_height}"
    )]
    IndexHeightMismatch {
        /// Height being reverted.
        requested_height: u32,
        /// Height decoded from the indexed event key.
        indexed_height: u32,
    },
    /// Historical or live-tail coverage is not contiguous.
    #[error("value-pool flow coverage is discontinuous")]
    CoverageDiscontinuous,
    /// A historical or startup-tail batch was empty.
    #[error("value-pool flow backfill batch is empty")]
    EmptyBackfill,
    /// Coverage metadata has an invalid encoding.
    #[error("value-pool flow coverage value has invalid length {bytes}")]
    MalformedCoverageValue {
        /// Observed payload length.
        bytes: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_keys_sort_by_time_then_coordinate_newest_first()
    -> Result<(), ValuePoolFlowHistoryConsumerError> {
        let older = encode_event_key(100, BlockHeight::new(9), 8);
        let newer = encode_event_key(101, BlockHeight::new(1), 0);
        let lower_height = encode_event_key(101, BlockHeight::new(8), u32::MAX);
        let higher_height = encode_event_key(101, BlockHeight::new(9), 0);
        let lower_coordinate = encode_event_key(101, BlockHeight::new(9), 7);
        let higher_coordinate = encode_event_key(101, BlockHeight::new(9), 8);

        assert!(newer < older);
        assert!(higher_height < lower_height);
        assert!(higher_coordinate < lower_coordinate);
        assert_eq!(
            decode_event_key(&higher_coordinate)?,
            (101, BlockHeight::new(9), 8)
        );
        Ok(())
    }

    #[test]
    fn signed_balances_round_trip_and_classify_mixed_deshield_flow()
    -> Result<(), ValuePoolFlowHistoryConsumerError> {
        let transaction_id = TransactionId::from_bytes([7; 32]);
        let balances = TransactionIntrinsicValueBalances::new(10, -3, 0, 0);
        let key = encode_event_key(123, BlockHeight::new(42), 5);
        let payload = encode_event_value(transaction_id, balances);
        let event = ValuePoolFlowHistoryConsumer::decode_event(&key, &payload)?;

        assert_eq!(event.transaction_id, transaction_id);
        assert_eq!(event.pool_balances, balances);
        assert_eq!(event.net_balance_zat()?, 7);
        assert_eq!(event.amount_zat()?, 7);
        assert_eq!(event.direction()?, ValuePoolFlowDirection::Deshield);
        assert_eq!(event.pool(), ValuePoolFlowPool::Mixed);
        Ok(())
    }

    #[test]
    fn zero_net_and_overflow_are_rejected() {
        let zero = ValuePoolFlowEvent {
            transaction_id: TransactionId::from_bytes([0; 32]),
            block_height: BlockHeight::new(1),
            block_time_unix_seconds: 1,
            transaction_index_in_block: 0,
            pool_balances: TransactionIntrinsicValueBalances::new(5, -5, 0, 0),
        };
        assert!(matches!(
            zero.direction(),
            Err(ValuePoolFlowHistoryConsumerError::ZeroNetFlow)
        ));
        assert!(matches!(
            checked_net_balance(TransactionIntrinsicValueBalances::new(i64::MAX, 1, 0, 0)),
            Err(ValuePoolFlowHistoryConsumerError::NetBalanceOverflow)
        ));
    }
}
