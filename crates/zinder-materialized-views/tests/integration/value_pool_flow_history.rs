use std::{collections::HashMap, error::Error, sync::Arc};

use rust_rocksdb::WriteBatch;
use tempfile::tempdir;
use zinder_core::{
    BlockHash, BlockHeight, LockTime, PrivacyShape, TransactionComponentCounts,
    TransactionFactsArtifact, TransactionId, TransactionIntrinsicValueBalances,
    TransactionLocation, TransactionPublicFacts, TransactionVersion,
};
use zinder_materialized_views::{
    BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, MaterializedViewConsumerCtx,
    MaterializedViewStore, MaterializedViewStoreOptions, TransactionIntrinsicValueBalanceFacts,
    TransparentSpendFacts, VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY,
    VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME, VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY,
    VALUE_POOL_FLOW_HISTORY_SCHEMA, ValuePoolFlowBackfillCoverage, ValuePoolFlowDirection,
    ValuePoolFlowEvent, ValuePoolFlowHistoryConsumer, ValuePoolFlowPool, ValuePoolFlowTailCoverage,
};
use zinder_store::RocksDbResourceBudget;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn transaction(height: BlockHeight, block_hash: BlockHash, index: u32) -> TransactionFactsArtifact {
    let mut id = [0_u8; 32];
    id[..4].copy_from_slice(&height.value().to_be_bytes());
    id[4..8].copy_from_slice(&index.to_be_bytes());
    let transaction_id = TransactionId::from_bytes(id);
    TransactionFactsArtifact::new(
        TransactionLocation::new(transaction_id, height, block_hash, index),
        TransactionPublicFacts {
            transaction_id,
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::V5,
            consensus_branch_id: None,
            lock_time: LockTime::Unlocked,
            expiry_height: None,
            size_bytes: 0,
            counts: TransactionComponentCounts {
                transparent_input_count: 1,
                sapling_spend_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
            privacy_shape: PrivacyShape::Deshielding,
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
    time: i64,
    balances: &[TransactionIntrinsicValueBalances],
) -> BlockCommitContext {
    let height = BlockHeight::new(height);
    let block_hash = BlockHash::from_bytes([height.value().to_be_bytes()[3]; 32]);
    let transactions: Vec<_> = balances
        .iter()
        .enumerate()
        .map(|(index, _)| transaction(height, block_hash, u32::try_from(index).unwrap_or(u32::MAX)))
        .collect();
    let intrinsic_balances = transactions
        .iter()
        .zip(balances)
        .map(|(transaction, balances)| (transaction.location.transaction_id, *balances))
        .collect();
    BlockCommitContext::new(
        BlockCommitInput {
            height,
            block_hash,
            previous_block_hash: BlockHash::from_bytes([0; 32]),
            block_time_unix_seconds: time,
            block_size_bytes: 0,
            transactions,
            final_note_commitment_roots: None,
        },
        TransparentSpendFacts::Offline,
    )
    .with_transaction_intrinsic_value_balances(
        TransactionIntrinsicValueBalanceFacts::from_map(Arc::new(intrinsic_balances)),
    )
}

fn open_store() -> TestResult<(tempfile::TempDir, MaterializedViewStore)> {
    let tempdir = tempdir()?;
    let store = MaterializedViewStore::open(
        tempdir.path(),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: &[VALUE_POOL_FLOW_HISTORY_SCHEMA],
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    Ok((tempdir, store))
}

fn apply(store: &MaterializedViewStore, blocks: &[BlockCommitContext]) -> TestResult {
    let mut consumer = ValuePoolFlowHistoryConsumer::new();
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

fn revert(store: &MaterializedViewStore, heights: &[BlockHeight]) -> TestResult {
    let mut consumer = ValuePoolFlowHistoryConsumer::new();
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

fn events(store: &MaterializedViewStore) -> TestResult<Vec<ValuePoolFlowEvent>> {
    let mut events = Vec::new();
    store.visit_consumer_rows(VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY, |key, payload| {
        let event = ValuePoolFlowHistoryConsumer::decode_event(key, payload)
            .map_err(|error| error.to_string())?;
        events.push(event);
        Ok(())
    })?;
    Ok(events)
}

#[test]
fn apply_keeps_one_event_per_qualifying_transaction_in_newest_first_order() -> TestResult {
    let (_tempdir, store) = open_store()?;
    let mut first_block = block(
        10,
        100,
        &[
            TransactionIntrinsicValueBalances::new(0, 8, 0, 0),
            TransactionIntrinsicValueBalances::new(0, -11, 3, 0),
            TransactionIntrinsicValueBalances::new(0, 5, -5, 0),
            TransactionIntrinsicValueBalances::new(0, 17, 0, 0),
        ],
    );
    first_block.transactions[3]
        .public_facts
        .counts
        .transparent_input_count = 0;
    apply(
        &store,
        &[
            first_block,
            block(
                11,
                101,
                &[TransactionIntrinsicValueBalances::new(0, 0, -13, 0)],
            ),
        ],
    )?;

    let events = events(&store)?;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].block_height, BlockHeight::new(11));
    assert_eq!(events[1].transaction_index_in_block, 1);
    assert_eq!(events[2].transaction_index_in_block, 0);
    assert_eq!(events[0].direction()?, ValuePoolFlowDirection::Shield);
    assert_eq!(events[0].amount_zat()?, 13);
    assert_eq!(events[0].pool(), ValuePoolFlowPool::Orchard);
    assert_eq!(events[1].pool(), ValuePoolFlowPool::Mixed);
    assert_eq!(events[1].amount_zat()?, 8);
    assert_eq!(
        store.consumer_row_count(VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY)?,
        2
    );
    Ok(())
}

#[test]
fn apply_excludes_shielded_coinbase_issuance_from_boundary_flows() -> TestResult {
    let (_tempdir, store) = open_store()?;
    let mut block = block(
        10,
        100,
        &[
            TransactionIntrinsicValueBalances::new(0, 0, 0, -125_000_000),
            TransactionIntrinsicValueBalances::new(0, 0, -125_000_000, 0),
        ],
    );
    block.transactions[0].public_facts.is_coinbase = true;

    apply(&store, &[block])?;

    let events = events(&store)?;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].transaction_index_in_block, 1);
    assert!(!events[0].is_coinbase());
    Ok(())
}

#[test]
fn bounded_read_helpers_preserve_newest_first_paging_and_half_open_time_ranges() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[
            block(
                10,
                100,
                &[
                    TransactionIntrinsicValueBalances::new(0, -5, 0, 0),
                    TransactionIntrinsicValueBalances::new(0, 7, 0, 0),
                ],
            ),
            block(
                11,
                200,
                &[TransactionIntrinsicValueBalances::new(0, 0, -9, 0)],
            ),
        ],
    )?;

    let first_page = ValuePoolFlowHistoryConsumer::read_page_after(&store, None, 1)?;
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].event.block_height, BlockHeight::new(11));
    let second_page = ValuePoolFlowHistoryConsumer::read_page_after(
        &store,
        Some(first_page[0].continuation_key()),
        8,
    )?;
    assert_eq!(second_page.len(), 2);
    assert!(
        second_page
            .iter()
            .all(|row| row.event.block_time_unix_seconds == 100)
    );

    let middle = ValuePoolFlowHistoryConsumer::events_in_time_range(&store, 100, 200, 8)?;
    assert_eq!(middle.len(), 2);
    assert!(
        middle
            .iter()
            .all(|event| event.block_time_unix_seconds == 100)
    );

    let mut visited = Vec::new();
    ValuePoolFlowHistoryConsumer::visit_events_in_time_range(&store, 100, 200, |event| {
        visited.push(event);
        Ok(())
    })?;
    assert_eq!(visited, middle);
    Ok(())
}

#[test]
fn time_range_visitor_rejects_malformed_rows() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[block(
            10,
            100,
            &[TransactionIntrinsicValueBalances::new(0, -5, 0, 0)],
        )],
    )?;
    let mut event_key = None;
    store.visit_consumer_rows(VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY, |key, _payload| {
        event_key = Some(key.to_vec());
        Ok(())
    })?;
    store.put_consumer(
        VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY,
        &event_key.ok_or("seeded value-pool flow event missing")?,
        b"malformed",
    )?;

    assert!(
        ValuePoolFlowHistoryConsumer::visit_events_in_time_range(&store, 100, 101, |_event| Ok(()))
            .is_err()
    );
    Ok(())
}

#[test]
fn reorg_replacement_removes_displaced_events_before_writing_replacement_events() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[block(
            10,
            100,
            &[TransactionIntrinsicValueBalances::new(0, -5, 0, 0)],
        )],
    )?;
    let replacement = block(
        10,
        102,
        &[TransactionIntrinsicValueBalances::new(0, 0, 7, 0)],
    );
    let mut consumer = ValuePoolFlowHistoryConsumer::new();
    let mut batch = WriteBatch::default();
    let mut ctx = MaterializedViewConsumerCtx {
        store: &store,
        batch: &mut batch,
    };
    consumer.begin_batch(&mut ctx)?;
    consumer.revert_block(BlockHeight::new(10), &mut ctx)?;
    consumer.apply_block(&replacement, &mut ctx)?;
    consumer.finish_batch(&mut ctx)?;
    store.write_batch(&batch)?;

    let replacement_events = events(&store)?;
    assert_eq!(replacement_events.len(), 1);
    assert_eq!(replacement_events[0].block_time_unix_seconds, 102);
    assert_eq!(replacement_events[0].pool(), ValuePoolFlowPool::Orchard);
    assert_eq!(replacement_events[0].amount_zat()?, 7);
    Ok(())
}

#[test]
fn revert_deletes_only_events_written_for_the_reverted_height() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[
            block(
                10,
                100,
                &[TransactionIntrinsicValueBalances::new(0, -5, 0, 0)],
            ),
            block(
                11,
                101,
                &[TransactionIntrinsicValueBalances::new(0, 0, 7, 0)],
            ),
        ],
    )?;

    revert(&store, &[BlockHeight::new(11)])?;

    let remaining = events(&store)?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].block_height, BlockHeight::new(10));
    assert_eq!(
        store.consumer_row_count(VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY)?,
        1
    );
    Ok(())
}

#[test]
fn missing_intrinsic_balance_fails_before_any_rows_are_written() -> TestResult {
    let (_tempdir, store) = open_store()?;
    let mut block = block(
        10,
        100,
        &[TransactionIntrinsicValueBalances::new(0, -5, 0, 0)],
    );
    block = block.with_transaction_intrinsic_value_balances(
        TransactionIntrinsicValueBalanceFacts::from_map(Arc::new(HashMap::new())),
    );
    let mut consumer = ValuePoolFlowHistoryConsumer::new();
    let mut batch = WriteBatch::default();
    let mut ctx = MaterializedViewConsumerCtx {
        store: &store,
        batch: &mut batch,
    };

    assert!(consumer.apply_block(&block, &mut ctx).is_err());
    store.write_batch(&batch)?;
    assert_eq!(
        store.consumer_row_count(VALUE_POOL_FLOW_HISTORY_COLUMN_FAMILY)?,
        0
    );
    assert_eq!(
        store.consumer_row_count(VALUE_POOL_FLOW_HISTORY_INDEX_COLUMN_FAMILY)?,
        0
    );
    Ok(())
}

#[test]
fn lifecycle_persists_coverage_without_advancing_cursor_and_rewinds_tail() -> TestResult {
    let (tempdir, store) = open_store()?;
    store.put_chain_event_cursor(VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME, b"inherited")?;
    assert!(
        ValuePoolFlowHistoryConsumer::widen_tail_boundary_for_startup(
            &store,
            BlockHeight::new(10),
        )?
    );
    let tail_block = block(
        10,
        100,
        &[TransactionIntrinsicValueBalances::new(0, -5, 0, 0)],
    );
    ValuePoolFlowHistoryConsumer::new().write_tail_seed_batch(&store, &[tail_block])?;
    assert_eq!(
        ValuePoolFlowHistoryConsumer::tail_coverage(&store)?,
        Some(ValuePoolFlowTailCoverage {
            boundary_height: BlockHeight::new(10),
            complete_through_height: Some(BlockHeight::new(10)),
            complete_through_time_unix_seconds: Some(100),
        })
    );
    assert_eq!(
        store.get_chain_event_cursor(VALUE_POOL_FLOW_HISTORY_CONSUMER_NAME)?,
        Some(b"inherited".to_vec())
    );

    let historical_block = block(1, 10, &[TransactionIntrinsicValueBalances::new(0, 0, 7, 0)]);
    let historical_coverage =
        ValuePoolFlowBackfillCoverage::new(BlockHeight::new(1), BlockHeight::new(1), 10, 10);
    ValuePoolFlowHistoryConsumer::new().write_backfill_batch(
        &store,
        &[historical_block],
        historical_coverage,
    )?;
    drop(store);

    let store = MaterializedViewStore::open(
        tempdir.path(),
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: &[VALUE_POOL_FLOW_HISTORY_SCHEMA],
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?;
    assert_eq!(
        ValuePoolFlowHistoryConsumer::backfill_coverage(&store)?,
        Some(historical_coverage)
    );
    revert(&store, &[BlockHeight::new(10)])?;
    assert_eq!(
        ValuePoolFlowHistoryConsumer::tail_coverage(&store)?,
        Some(ValuePoolFlowTailCoverage::from_boundary(BlockHeight::new(
            10
        )))
    );
    assert_eq!(
        ValuePoolFlowHistoryConsumer::backfill_coverage(&store)?,
        Some(historical_coverage)
    );
    Ok(())
}

#[test]
fn fresh_replay_initializes_and_advances_live_tail_coverage() -> TestResult {
    let (_tempdir, store) = open_store()?;
    apply(
        &store,
        &[
            block(
                10,
                100,
                &[TransactionIntrinsicValueBalances::new(0, -5, 0, 0)],
            ),
            block(
                11,
                101,
                &[TransactionIntrinsicValueBalances::new(0, 0, 7, 0)],
            ),
        ],
    )?;

    assert_eq!(
        ValuePoolFlowHistoryConsumer::tail_coverage(&store)?,
        Some(ValuePoolFlowTailCoverage {
            boundary_height: BlockHeight::new(10),
            complete_through_height: Some(BlockHeight::new(11)),
            complete_through_time_unix_seconds: Some(101),
        })
    );
    Ok(())
}
