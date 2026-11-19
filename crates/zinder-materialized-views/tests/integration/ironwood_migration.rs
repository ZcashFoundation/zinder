//! Ironwood-migration consumer: cumulative pool totals and per-migration rows.

use std::path::Path;

use eyre::{Result, eyre};
use rust_rocksdb::WriteBatch;
use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, LockTime, PrivacyShape, TransactionComponentCounts,
    TransactionFactsArtifact, TransactionId, TransactionLocation, TransactionPublicFacts,
    TransactionVersion,
};
use zinder_materialized_views::{
    BlockCommitContext, BlockCommitInput, BlockKeyedConsumer, IRONWOOD_MIGRATION_SCHEMA,
    IronwoodMigrationConsumer, MaterializedViewConsumerCtx, MaterializedViewConsumerSchema,
    MaterializedViewStore, MaterializedViewStoreOptions, TransparentSpendFacts,
};
use zinder_store::RocksDbResourceBudget;

const TEST_CONSUMERS: &[MaterializedViewConsumerSchema] = &[IRONWOOD_MIGRATION_SCHEMA];

fn open_store(path: &Path) -> Result<MaterializedViewStore> {
    Ok(MaterializedViewStore::open(
        path,
        MaterializedViewStoreOptions {
            sync_writes: false,
            consumers: TEST_CONSUMERS,
            rocksdb_resource_budget: RocksDbResourceBudget::for_local_tests(),
            reorg_policy: None,
        },
    )?)
}

fn public_facts(
    seed: u8,
    orchard_value_balance_zat: Option<i64>,
    ironwood_value_balance_zat: Option<i64>,
    counts: TransactionComponentCounts,
    orchard_anchor: Option<[u8; 32]>,
) -> TransactionPublicFacts {
    let transaction_id = TransactionId::from_bytes([seed; 32]);
    TransactionPublicFacts {
        transaction_id,
        auth_digest: None,
        wtxid: None,
        version: TransactionVersion::V6,
        consensus_branch_id: None,
        lock_time: LockTime::Unlocked,
        expiry_height: None,
        size_bytes: 0,
        counts,
        orchard_value_balance_zat,
        orchard_anchor,
        ironwood_value_balance_zat,
        privacy_shape: PrivacyShape::Unclassified,
        is_coinbase: false,
        unsupported_sections: Vec::new(),
    }
}

fn transaction(
    height: u32,
    tx_index_in_block: u32,
    seed: u8,
    facts: TransactionPublicFacts,
) -> TransactionFactsArtifact {
    TransactionFactsArtifact::new(
        TransactionLocation::new(
            TransactionId::from_bytes([seed; 32]),
            BlockHeight::new(height),
            BlockHash::from_bytes([0xAB; 32]),
            tx_index_in_block,
        ),
        facts,
    )
}

/// A non-migration transaction whose signed balances feed the running total.
fn balance_transaction(
    height: u32,
    tx_index_in_block: u32,
    seed: u8,
    orchard: i64,
    ironwood: i64,
) -> TransactionFactsArtifact {
    let facts = public_facts(
        seed,
        Some(orchard),
        Some(ironwood),
        TransactionComponentCounts::EMPTY,
        None,
    );
    transaction(height, tx_index_in_block, seed, facts)
}

fn block(height: u32, transactions: Vec<TransactionFactsArtifact>) -> BlockCommitContext {
    BlockCommitContext::new(
        BlockCommitInput {
            height: BlockHeight::new(height),
            block_hash: BlockHash::from_bytes([0xAB; 32]),
            previous_block_hash: BlockHash::from_bytes([0xCD; 32]),
            block_time_unix_seconds: 0,
            block_size_bytes: 0,
            transactions,
            final_note_commitment_roots: None,
        },
        TransparentSpendFacts::Offline,
    )
}

fn apply_in_own_batch(
    store: &MaterializedViewStore,
    consumer: &mut IronwoodMigrationConsumer,
    block: &BlockCommitContext,
) -> Result<()> {
    let mut batch = WriteBatch::default();
    {
        let mut ctx = MaterializedViewConsumerCtx {
            store,
            batch: &mut batch,
        };
        consumer
            .apply_block(block, &mut ctx)
            .map_err(|error| eyre!("apply_block failed: {error}"))?;
    }
    store.write_batch(&batch)?;
    Ok(())
}

#[test]
fn cumulative_totals_accumulate_each_block_delta() -> Result<()> {
    let tempdir = TempDir::new()?;
    let store = open_store(tempdir.path())?;

    let mut cumulative_orchard: i64 = 0;
    let mut cumulative_ironwood: i64 = 0;
    for height in 1..=5u32 {
        let orchard = i64::from(height) * 10;
        let ironwood = i64::from(height);
        cumulative_orchard += orchard;
        cumulative_ironwood += ironwood;

        let mut consumer = IronwoodMigrationConsumer::new();
        let seed = u8::try_from(height).unwrap_or(0);
        let current = block(
            height,
            vec![balance_transaction(height, 0, seed, orchard, ironwood)],
        );
        apply_in_own_batch(&store, &mut consumer, &current)?;

        let totals = IronwoodMigrationConsumer::read_pool_totals_at_or_before(
            &store,
            BlockHeight::new(height),
        )?
        .ok_or_else(|| eyre!("expected pool totals at height {height}"))?;
        assert_eq!(totals.block_height, height);
        assert_eq!(
            totals.cumulative_orchard_value_balance_zat,
            cumulative_orchard
        );
        assert_eq!(
            totals.cumulative_ironwood_value_balance_zat,
            cumulative_ironwood
        );
        assert_eq!(totals.block_orchard_value_balance_zat, orchard);
        assert_eq!(totals.block_ironwood_value_balance_zat, ironwood);
    }
    Ok(())
}

#[test]
fn multiple_blocks_in_one_batch_thread_the_running_total() -> Result<()> {
    let tempdir = TempDir::new()?;
    let store = open_store(tempdir.path())?;

    let mut consumer = IronwoodMigrationConsumer::new();
    let mut batch = WriteBatch::default();
    {
        let mut ctx = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        for height in 1..=3u32 {
            let seed = u8::try_from(height).unwrap_or(0);
            let current = block(height, vec![balance_transaction(height, 0, seed, 100, -1)]);
            consumer
                .apply_block(&current, &mut ctx)
                .map_err(|error| eyre!("apply_block failed: {error}"))?;
        }
    }
    store.write_batch(&batch)?;

    for height in 1..=3u32 {
        let totals = IronwoodMigrationConsumer::read_pool_totals_at_or_before(
            &store,
            BlockHeight::new(height),
        )?
        .ok_or_else(|| eyre!("expected pool totals at height {height}"))?;
        assert_eq!(
            totals.cumulative_orchard_value_balance_zat,
            i64::from(height) * 100
        );
        assert_eq!(
            totals.cumulative_ironwood_value_balance_zat,
            -i64::from(height)
        );
    }
    Ok(())
}

#[test]
fn reverting_every_block_returns_to_the_pre_apply_state() -> Result<()> {
    let tempdir = TempDir::new()?;
    let store = open_store(tempdir.path())?;

    for height in 1..=4u32 {
        let mut consumer = IronwoodMigrationConsumer::new();
        let seed = u8::try_from(height).unwrap_or(0);
        let current = block(height, vec![balance_transaction(height, 0, seed, 7, -3)]);
        apply_in_own_batch(&store, &mut consumer, &current)?;
    }
    assert!(IronwoodMigrationConsumer::read_latest_pool_totals(&store)?.is_some());

    let mut consumer = IronwoodMigrationConsumer::new();
    let mut batch = WriteBatch::default();
    {
        let mut ctx = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        for height in 1..=4u32 {
            consumer
                .revert_block(BlockHeight::new(height), &mut ctx)
                .map_err(|error| eyre!("revert_block failed: {error}"))?;
        }
    }
    store.write_batch(&batch)?;

    assert_eq!(
        IronwoodMigrationConsumer::read_latest_pool_totals(&store)?,
        None
    );
    assert_eq!(
        IronwoodMigrationConsumer::read_pool_totals_at_or_before(&store, BlockHeight::new(4))?,
        None
    );
    Ok(())
}

#[test]
fn reorg_subtracts_reverted_deltas_and_rebuilds_the_suffix() -> Result<()> {
    let tempdir = TempDir::new()?;
    let store = open_store(tempdir.path())?;

    for height in 1..=5u32 {
        let mut consumer = IronwoodMigrationConsumer::new();
        let seed = u8::try_from(height).unwrap_or(0);
        let current = block(height, vec![balance_transaction(height, 0, seed, 100, -10)]);
        apply_in_own_batch(&store, &mut consumer, &current)?;
    }

    let mut consumer = IronwoodMigrationConsumer::new();
    let mut batch = WriteBatch::default();
    {
        let mut ctx = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        for height in 3..=5u32 {
            consumer
                .revert_block(BlockHeight::new(height), &mut ctx)
                .map_err(|error| eyre!("revert_block failed: {error}"))?;
        }
        for height in 3..=5u32 {
            let seed = u8::try_from(height + 100).unwrap_or(0);
            let replacement = block(height, vec![balance_transaction(height, 0, seed, 1, -1)]);
            consumer
                .apply_block(&replacement, &mut ctx)
                .map_err(|error| eyre!("apply_block failed: {error}"))?;
        }
    }
    store.write_batch(&batch)?;

    let below_fork =
        IronwoodMigrationConsumer::read_pool_totals_at_or_before(&store, BlockHeight::new(2))?
            .ok_or_else(|| eyre!("expected pool totals at height 2"))?;
    assert_eq!(below_fork.cumulative_orchard_value_balance_zat, 200);
    assert_eq!(below_fork.cumulative_ironwood_value_balance_zat, -20);

    let tip = IronwoodMigrationConsumer::read_latest_pool_totals(&store)?
        .ok_or_else(|| eyre!("expected a materialized tip"))?;
    assert_eq!(tip.block_height, 5);
    assert_eq!(tip.cumulative_orchard_value_balance_zat, 200 + 3);
    assert_eq!(tip.cumulative_ironwood_value_balance_zat, -20 - 3);
    Ok(())
}

#[test]
fn read_pool_totals_at_or_before_resolves_above_and_below_the_range() -> Result<()> {
    let tempdir = TempDir::new()?;
    let store = open_store(tempdir.path())?;

    for height in 10..=12u32 {
        let mut consumer = IronwoodMigrationConsumer::new();
        let seed = u8::try_from(height).unwrap_or(0);
        let current = block(height, vec![balance_transaction(height, 0, seed, 5, -5)]);
        apply_in_own_batch(&store, &mut consumer, &current)?;
    }

    let above =
        IronwoodMigrationConsumer::read_pool_totals_at_or_before(&store, BlockHeight::new(99))?
            .ok_or_else(|| eyre!("expected the tip for a height above the range"))?;
    assert_eq!(above.block_height, 12);

    assert_eq!(
        IronwoodMigrationConsumer::read_pool_totals_at_or_before(&store, BlockHeight::new(1))?,
        None
    );
    Ok(())
}

#[test]
fn migration_rows_capture_loose_matches_and_flag_conformance() -> Result<()> {
    let tempdir = TempDir::new()?;
    let store = open_store(tempdir.path())?;

    let conformant_counts = TransactionComponentCounts {
        ironwood_action_count: 1,
        orchard_action_count: 2,
        ..TransactionComponentCounts::EMPTY
    };
    let conformant = transaction(
        7,
        0,
        0x11,
        public_facts(
            0x11,
            Some(1_005),
            Some(-1_000),
            conformant_counts,
            Some([0x42; 32]),
        ),
    );

    let loose_counts = TransactionComponentCounts {
        ironwood_action_count: 2,
        orchard_action_count: 2,
        ..TransactionComponentCounts::EMPTY
    };
    let loose_only = transaction(
        7,
        1,
        0x22,
        public_facts(
            0x22,
            Some(2_010),
            Some(-2_000),
            loose_counts,
            Some([0x43; 32]),
        ),
    );

    let not_a_migration = balance_transaction(7, 2, 0x33, 500, 500);

    let mut consumer = IronwoodMigrationConsumer::new();
    let current = block(7, vec![conformant, loose_only, not_a_migration]);
    apply_in_own_batch(&store, &mut consumer, &current)?;

    let migrations = IronwoodMigrationConsumer::read_migrations_in_range(
        &store,
        BlockHeight::new(0),
        BlockHeight::new(10),
        16,
    )?;
    assert_eq!(migrations.len(), 2);

    let first = migrations
        .first()
        .ok_or_else(|| eyre!("missing first migration"))?;
    assert_eq!(first.tx_index_in_block, 0);
    assert_eq!(first.transaction_id, TransactionId::from_bytes([0x11; 32]));
    assert_eq!(first.orchard_value_balance_zat, 1_005);
    assert_eq!(first.ironwood_value_balance_zat, -1_000);
    assert_eq!(first.orchard_anchor, [0x42; 32]);
    assert!(first.conformant);
    assert_eq!(first.migrated_amount_zat(), 1_000);

    let second = migrations
        .get(1)
        .ok_or_else(|| eyre!("missing second migration"))?;
    assert_eq!(second.tx_index_in_block, 1);
    assert!(!second.conformant);
    assert_eq!(second.migrated_amount_zat(), 2_000);
    Ok(())
}

#[test]
fn migration_rows_are_removed_on_revert() -> Result<()> {
    let tempdir = TempDir::new()?;
    let store = open_store(tempdir.path())?;

    let counts = TransactionComponentCounts {
        ironwood_action_count: 1,
        orchard_action_count: 2,
        ..TransactionComponentCounts::EMPTY
    };
    let migration = transaction(
        3,
        0,
        0x55,
        public_facts(0x55, Some(1_005), Some(-1_000), counts, Some([0x42; 32])),
    );

    let mut consumer = IronwoodMigrationConsumer::new();
    apply_in_own_batch(&store, &mut consumer, &block(3, vec![migration]))?;
    assert_eq!(
        IronwoodMigrationConsumer::read_migrations_in_range(
            &store,
            BlockHeight::new(0),
            BlockHeight::new(10),
            16,
        )?
        .len(),
        1
    );

    let mut consumer = IronwoodMigrationConsumer::new();
    let mut batch = WriteBatch::default();
    {
        let mut ctx = MaterializedViewConsumerCtx {
            store: &store,
            batch: &mut batch,
        };
        consumer
            .revert_block(BlockHeight::new(3), &mut ctx)
            .map_err(|error| eyre!("revert_block failed: {error}"))?;
    }
    store.write_batch(&batch)?;

    assert_eq!(
        IronwoodMigrationConsumer::read_migrations_in_range(
            &store,
            BlockHeight::new(0),
            BlockHeight::new(10),
            16,
        )?,
        []
    );
    Ok(())
}
