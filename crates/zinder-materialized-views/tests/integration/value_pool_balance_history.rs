use std::error::Error;

use rust_rocksdb::WriteBatch;
use tempfile::tempdir;
use zinder_core::{BlockHash, BlockHeight, ValuePoolBalance};
use zinder_materialized_views::{
    BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, BlockValuePoolBalanceFacts,
    MaterializedViewConsumerCtx, MaterializedViewStore, MaterializedViewStoreOptions,
    TransparentSpendFacts, VALUE_POOL_BALANCE_HISTORY_COLUMN_FAMILY,
    VALUE_POOL_BALANCE_HISTORY_SCHEMA, ValuePoolBalanceBackfillCoverage,
    ValuePoolBalanceHistoryConsumer, ValuePoolBalancePoint, ValuePoolBalanceTailCoverage,
};
use zinder_store::RocksDbResourceBudget;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const DAY: i64 = 86_400;

fn pools(amount_zat: u64) -> Vec<ValuePoolBalance> {
    vec![
        ValuePoolBalance::new("transparent", true, Some(amount_zat)),
        ValuePoolBalance::new("future-pool", false, None),
        ValuePoolBalance::new("sapling", true, Some(amount_zat + 100)),
    ]
}

fn block(height: u32, hash_byte: u8, time: i64, amount_zat: u64) -> BlockCommitContext {
    BlockCommitContext::new(
        BlockCommitInput {
            height: BlockHeight::new(height),
            block_hash: BlockHash::from_bytes([hash_byte; 32]),
            previous_block_hash: BlockHash::from_bytes([0; 32]),
            block_time_unix_seconds: time,
            block_size_bytes: 0,
            transactions: Vec::new(),
            final_note_commitment_roots: None,
        },
        TransparentSpendFacts::Offline,
    )
    .with_block_value_pool_balances(BlockValuePoolBalanceFacts::from_pools(pools(amount_zat)))
}

fn open_store() -> TestResult<(tempfile::TempDir, MaterializedViewStore)> {
    let tempdir = tempdir()?;
    let store = MaterializedViewStore::open(
        tempdir.path(),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: &[VALUE_POOL_BALANCE_HISTORY_SCHEMA],
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    Ok((tempdir, store))
}

fn apply(store: &MaterializedViewStore, blocks: &[BlockCommitContext]) -> TestResult {
    let mut consumer = ValuePoolBalanceHistoryConsumer::new();
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

fn replace(
    store: &MaterializedViewStore,
    height: BlockHeight,
    replacement: &BlockCommitContext,
) -> TestResult {
    let mut consumer = ValuePoolBalanceHistoryConsumer::new();
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

fn point_value(point: &ValuePoolBalancePoint) -> Option<u64> {
    point.pools.first().and_then(|pool| pool.value_zat)
}

#[test]
fn codec_preserves_dynamic_pool_list_and_newest_day_order() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[
            block(10, 10, DAY + 5, 10),
            block(11, 11, DAY + 6, 11),
            block(12, 12, 2 * DAY + 1, 12),
        ],
    )?;

    let mut decoded = Vec::new();
    store.visit_consumer_rows(VALUE_POOL_BALANCE_HISTORY_COLUMN_FAMILY, |key, payload| {
        decoded.push(
            ValuePoolBalanceHistoryConsumer::decode_point(key, payload)
                .map_err(|error| error.to_string())?,
        );
        Ok(())
    })?;
    assert_eq!(decoded.len(), 3);
    assert!(decoded.iter().any(|point| {
        point.block_height == BlockHeight::new(11)
            && point.pools == pools(11)
            && point.block_time_unix_seconds == DAY + 6
    }));

    let days = ValuePoolBalanceHistoryConsumer::read_newest_days(&store, 2)?;
    assert_eq!(days.len(), 2);
    assert_eq!(days[0].day_start_unix_seconds, 2 * DAY);
    assert_eq!(days[0].point.block_height, BlockHeight::new(12));
    assert_eq!(days[1].day_start_unix_seconds, DAY);
    assert_eq!(days[1].point.block_height, BlockHeight::new(11));
    assert!(ValuePoolBalanceHistoryConsumer::decode_point(&[0; 43], &[]).is_err());
    Ok(())
}

#[test]
fn duplicate_day_selects_highest_height_and_exact_day_offsets() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[
            block(10, 10, DAY + 10, 10),
            block(11, 11, DAY + 20, 11),
            block(12, 12, 30 * DAY + 1, 12),
            block(13, 13, 31 * DAY + 1, 13),
        ],
    )?;

    let day_one = ValuePoolBalanceHistoryConsumer::point_for_utc_day(&store, DAY)?;
    assert_eq!(
        day_one.map(|point| point.block_height),
        Some(BlockHeight::new(11))
    );
    let offsets =
        ValuePoolBalanceHistoryConsumer::points_days_before(&store, 31 * DAY + 7, &[1, 7, 30])?;
    assert_eq!(offsets.len(), 3);
    assert_eq!(offsets[0].as_ref().map(point_value), Some(Some(12)));
    assert_eq!(offsets[1], None);
    assert_eq!(offsets[2].as_ref().map(point_value), Some(Some(11)));
    Ok(())
}

#[test]
fn reorg_replacement_deletes_exact_row_and_restores_prior_day_candidate() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[block(10, 0x10, DAY + 10, 10), block(11, 0x11, DAY + 20, 11)],
    )?;
    replace(&store, BlockHeight::new(11), &block(11, 0x22, DAY + 30, 22))?;
    let replacement = ValuePoolBalanceHistoryConsumer::point_for_utc_day(&store, DAY)?
        .ok_or("missing replacement point")?;
    assert_eq!(replacement.block_hash, BlockHash::from_bytes([0x22; 32]));
    assert_eq!(point_value(&replacement), Some(22));
    assert_eq!(
        store.consumer_row_count(VALUE_POOL_BALANCE_HISTORY_COLUMN_FAMILY)?,
        2
    );

    let mut consumer = ValuePoolBalanceHistoryConsumer::new();
    let mut batch = WriteBatch::default();
    let mut ctx = MaterializedViewConsumerCtx {
        store: &store,
        batch: &mut batch,
    };
    consumer.begin_batch(&mut ctx)?;
    consumer.revert_block(BlockHeight::new(11), &mut ctx)?;
    consumer.finish_batch(&mut ctx)?;
    store.write_batch(&batch)?;

    let restored = ValuePoolBalanceHistoryConsumer::point_for_utc_day(&store, DAY)?
        .ok_or("missing restored point")?;
    assert_eq!(restored.block_height, BlockHeight::new(10));
    assert_eq!(restored.block_hash, BlockHash::from_bytes([0x10; 32]));
    assert_eq!(point_value(&restored), Some(10));
    Ok(())
}

#[test]
fn nonmonotonic_timestamps_do_not_change_height_coverage_or_daily_selection() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[
            block(10, 10, 2 * DAY + 20, 10),
            block(11, 11, DAY + 20, 11),
            block(12, 12, DAY + 30, 12),
        ],
    )?;

    assert_eq!(
        ValuePoolBalanceHistoryConsumer::tail_coverage(&store)?,
        Some(ValuePoolBalanceTailCoverage {
            boundary_height: BlockHeight::new(10),
            complete_through_height: Some(BlockHeight::new(12)),
        })
    );
    let prior_day = ValuePoolBalanceHistoryConsumer::point_for_utc_day(&store, DAY)?
        .ok_or("missing nonmonotonic day")?;
    assert_eq!(prior_day.block_height, BlockHeight::new(12));
    let later_day = ValuePoolBalanceHistoryConsumer::point_for_utc_day(&store, 2 * DAY)?
        .ok_or("missing later day")?;
    assert_eq!(later_day.block_height, BlockHeight::new(10));
    Ok(())
}

#[test]
fn historical_and_live_tail_coverage_are_independent_atomic_and_restartable() -> TestResult {
    let (tempdir, store) = open_store()?;
    let historical = block(1, 1, DAY, 1);
    let coverage = ValuePoolBalanceBackfillCoverage::new(BlockHeight::new(1), BlockHeight::new(1));
    ValuePoolBalanceHistoryConsumer::new().write_backfill_batch(&store, &[historical], coverage)?;
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::backfill_coverage(&store)?,
        Some(coverage)
    );
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::tail_coverage(&store)?,
        None
    );
    assert!(
        ValuePoolBalanceHistoryConsumer::new()
            .write_backfill_batch(
                &store,
                &[block(4, 4, 4 * DAY, 4)],
                ValuePoolBalanceBackfillCoverage::new(BlockHeight::new(1), BlockHeight::new(3)),
            )
            .is_err()
    );
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::backfill_coverage(&store)?,
        Some(coverage)
    );

    assert!(
        ValuePoolBalanceHistoryConsumer::widen_tail_boundary_for_startup(
            &store,
            BlockHeight::new(10),
        )?
    );
    ValuePoolBalanceHistoryConsumer::new().write_tail_seed_batch(
        &store,
        &[block(10, 10, 10 * DAY, 10), block(11, 11, 11 * DAY, 11)],
    )?;
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::tail_coverage(&store)?,
        Some(ValuePoolBalanceTailCoverage {
            boundary_height: BlockHeight::new(10),
            complete_through_height: Some(BlockHeight::new(11)),
        })
    );
    drop(store);

    let reopened = MaterializedViewStore::open(
        tempdir.path(),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: &[VALUE_POOL_BALANCE_HISTORY_SCHEMA],
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::backfill_coverage(&reopened)?,
        Some(coverage)
    );
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::tail_coverage(&reopened)?,
        Some(ValuePoolBalanceTailCoverage {
            boundary_height: BlockHeight::new(10),
            complete_through_height: Some(BlockHeight::new(11)),
        })
    );
    Ok(())
}

#[test]
fn sparse_daily_candidates_advance_contiguous_scanned_height_coverage() -> TestResult {
    let (_tempdir, store) = open_store()?;
    let coverage =
        ValuePoolBalanceBackfillCoverage::new(BlockHeight::new(1), BlockHeight::new(1_000));
    ValuePoolBalanceHistoryConsumer::new().write_backfill_batch(
        &store,
        &[block(100, 1, DAY + 1, 100), block(900, 2, 2 * DAY + 1, 900)],
        coverage,
    )?;

    assert_eq!(
        ValuePoolBalanceHistoryConsumer::backfill_coverage(&store)?,
        Some(coverage)
    );
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::point_at_height(&store, BlockHeight::new(100))?
            .map(|point| point.block_hash),
        Some(BlockHash::from_bytes([1; 32]))
    );
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::point_at_height(&store, BlockHeight::new(101))?,
        None
    );

    let next_coverage =
        ValuePoolBalanceBackfillCoverage::new(BlockHeight::new(1), BlockHeight::new(2_000));
    ValuePoolBalanceHistoryConsumer::new().write_backfill_batch(
        &store,
        &[block(1_500, 3, 3 * DAY + 1, 1_500)],
        next_coverage,
    )?;
    assert_eq!(
        ValuePoolBalanceHistoryConsumer::backfill_coverage(&store)?,
        Some(next_coverage)
    );
    Ok(())
}
