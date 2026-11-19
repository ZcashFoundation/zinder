use std::num::NonZeroU32;

use rust_rocksdb::{DEFAULT_COLUMN_FAMILY_NAME, statistics::Ticker};
use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, Network, UnixTimestampMillis,
};

use super::{
    CanonicalEventHistoryRequest, CanonicalLiveAppend, CanonicalReorgPolicy, CanonicalStoreError,
    CanonicalStoreWorkload, ProjectionBuildLease, ProjectionBuildLeaseId,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, ServedTreeState, TreeStateLookup,
    TreeStateReplayOutcome,
    block_load::encode_block_position,
    live_replacement_tests::{
        open_store, open_store_with_retention, published_store, published_store_with_retention,
        replacement_block,
    },
    rocksdb::{COMPACT_BLOCK_COLUMN_FAMILY, validate_open_store_control},
};
use crate::{
    CompactBlockReadBudget, HeightOrder, RawBlobRetention, RocksDbOpenResources,
    RocksDbResourceBudget, SharedBlockCache,
};

const REORG_WINDOW: u32 = 2;

#[test]
fn second_ready_primary_open_is_refused_while_the_first_owns_the_store()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let first = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;

    let error = RocksDbCanonicalStore::open_ready(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("a second primary must not open while the first owns the store")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::RocksDbOperation {
                operation: "open ready",
                ..
            }
        ),
        "unexpected second-primary error: {error:?}"
    );

    let fence = first.event_fence();
    drop(first);
    let reopened = open_store(&store_path, &activations, REORG_WINDOW)?;
    assert_eq!(reopened.event_fence(), fence);
    Ok(())
}

#[test]
fn secondary_observes_a_live_append_only_after_explicit_catch_up()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let mut secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::Transactions,
    )??;
    let baseline_fence = secondary.event_fence();
    assert_eq!(baseline_fence, store.event_fence());
    assert_eq!(secondary.chain_epoch()?, store.chain_epoch()?);

    let store = append_block_five(store, &activations)?;
    assert_eq!(secondary.event_fence(), baseline_fence);
    assert_eq!(secondary.block_header_at(BlockHeight::new(5))?, None);

    let catchup = secondary.try_catch_up()?;
    assert_eq!(catchup.before(), baseline_fence);
    assert_eq!(catchup.after(), store.event_fence());
    assert!(catchup.advanced());
    assert_eq!(secondary.chain_epoch()?, store.chain_epoch()?);
    assert_eq!(
        secondary
            .block_header_at(BlockHeight::new(5))?
            .map(|header| header.block_hash),
        Some(BlockHash::from_bytes([5; 32]))
    );

    let idle = secondary.try_catch_up()?;
    assert_eq!(idle.before(), idle.after());
    assert!(!idle.advanced());
    Ok(())
}

/// Heights whose compact-block rows each get a table file of their own.
///
/// With the test budget's 64 open files, a secondary keeps at most 77 table
/// files open: a 54-entry table cache spread over 64 one-entry shards, plus
/// up to 13 handles pinned at open. Reading every one of these rows must
/// therefore reopen at least one file, whatever the cache's hash seed.
const SPLIT_HEIGHTS: u32 = 96;

#[test]
fn a_stale_secondary_reports_a_compacted_away_table_file_until_it_catches_up()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(SPLIT_HEIGHTS),
        BlockHeight::new(SPLIT_HEIGHTS - REORG_WINDOW),
    )?;
    split_rows_into_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let mut secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::Transactions,
    )??;

    compact_away_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let failures = (1..=SPLIT_HEIGHTS)
        .filter_map(|height| secondary.compact_block_at(BlockHeight::new(height)).err())
        .collect::<Vec<_>>();
    assert!(
        !failures.is_empty(),
        "a stale secondary must reopen at least one compacted-away table file"
    );
    for failure in &failures {
        assert!(
            failure.is_missing_table_file(),
            "only a missing table file may fail the stale read, got {failure:?}"
        );
    }
    // An iterator opens every L0 file its range overlaps and reports the same
    // status for the first one that is gone.
    let family = secondary
        .bounded_open
        .db
        .cf_handle(COMPACT_BLOCK_COLUMN_FAMILY)
        .ok_or("the family must exist")?;
    let scan_failure = secondary
        .bounded_open
        .db
        .iterator_cf(&family, rust_rocksdb::IteratorMode::Start)
        .find_map(Result::err)
        .ok_or("a scan over the compacted-away files must fail")?;
    assert!(crate::is_missing_table_file_error(&scan_failure));
    drop(family);
    // A range read whose rows are in compacted-away files reports one too,
    // through the point lookups that fetch them.
    let failure = secondary
        .compact_blocks_in_range(split_heights())
        .err()
        .ok_or("a range read over the compacted-away files must fail")?;
    assert!(
        failure.is_missing_table_file(),
        "only a missing table file may fail the stale range read, got {failure:?}"
    );

    secondary.try_catch_up()?;
    for height in (1..=SPLIT_HEIGHTS).map(BlockHeight::new) {
        assert_eq!(
            secondary.compact_block_at(height)?,
            store.compact_block_at(height)?
        );
    }
    Ok(())
}

/// A byte-bounded read that its first block fills fetches only that block.
///
/// A stale secondary therefore answers it while the table files of the other
/// heights of the range are gone. The compaction leaves the table files of
/// both starting ends in place, so the read of an end never depends on a
/// cached table handle. Each row has a level-0 and a bottom-level file of its
/// own so that the compaction can leave the ends' files alone. Each end gets a
/// secondary of its own.
#[test]
fn a_bounded_read_its_first_block_fills_fetches_only_that_block()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(SPLIT_HEIGHTS),
        BlockHeight::new(SPLIT_HEIGHTS - REORG_WINDOW),
    )?;
    split_rows_into_bottom_level_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    split_rows_into_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let mut stale = Vec::new();
    for (order, first) in [
        (HeightOrder::Ascending, BlockHeight::new(1)),
        (HeightOrder::Descending, BlockHeight::new(SPLIT_HEIGHTS)),
    ] {
        let secondary = open_secondary(
            &store_path,
            temporary.path().join(format!("{order:?}")),
            &activations,
            RawBlobRetention::Transactions,
        )??;
        stale.push((secondary, order, first));
    }

    compact_away_rows_in(
        &store.bounded_open.db,
        COMPACT_BLOCK_COLUMN_FAMILY,
        BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(SPLIT_HEIGHTS - 1)),
    )?;
    for (secondary, order, first) in &stale {
        let first_block = store
            .compact_block_at(*first)?
            .ok_or("the primary must hold the first block")?;
        assert_eq!(
            secondary.compact_block_at(*first)?.as_ref(),
            Some(&first_block),
            "the {order:?} read's first block must stay readable"
        );
        let later_failures = split_heights()
            .into_iter()
            .filter(|height| height != first)
            .filter_map(|height| secondary.compact_block_at(height).err())
            .collect::<Vec<_>>();
        assert!(
            !later_failures.is_empty()
                && later_failures
                    .iter()
                    .all(CanonicalStoreError::is_missing_table_file),
            "another height of the {order:?} read must be in a missing table file, \
             got {later_failures:?}"
        );
        let first_row_bytes = crate::encode_compact_block_artifact(&first_block).len();
        for max_bytes in [0, first_row_bytes] {
            let before = range_read_tickers(secondary);
            assert_eq!(
                secondary.compact_blocks_in_range_up_to(
                    split_heights(),
                    *order,
                    CompactBlockReadBudget::bytes(max_bytes)
                )?,
                std::slice::from_ref(&first_block),
                "a {order:?} read with a {max_bytes}-byte budget"
            );
            assert_eq!(
                range_read_tickers(secondary).since(before),
                RangeReadTickers {
                    multi_get_calls: 1,
                    multi_get_keys: 1,
                    iterator_seeks: 0,
                },
                "a {order:?} read with a {max_bytes}-byte budget must fetch one row"
            );
        }
    }
    Ok(())
}

/// Low heights whose table files a partial compaction leaves in place.
const KEPT_HEIGHTS: u32 = 2;

/// The heights from 1 through [`KEPT_HEIGHTS`].
fn kept_heights() -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(KEPT_HEIGHTS))
}

/// A range read on a stale secondary does not depend on a deleted table file
/// that holds none of its heights.
///
/// An iterator bounded to a kept height fails on the stale secondary, while
/// point lookups of the kept heights succeed. Every range read of the kept
/// heights fetches its rows with point lookups and moves no iterator, so it
/// succeeds within each byte budget. So do the tree-state replay over the kept
/// heights and a byte-bounded read of every height whose budget the kept rows
/// spend exactly.
#[test]
fn a_range_read_does_not_depend_on_a_deleted_table_file_outside_its_range()
-> Result<(), Box<dyn std::error::Error>> {
    use HeightOrder::{Ascending, Descending};

    let temporary = TempDir::new()?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let (store, secondary) = stale_secondary_over_kept_heights(&temporary, &activations)?;
    let kept = kept_heights();
    let expected = store.compact_blocks_in_range(kept)?;
    for (height, block) in kept.into_iter().zip(&expected) {
        assert_eq!(secondary.compact_block_at(height)?.as_ref(), Some(block));
    }

    let before = range_read_tickers(&secondary);
    assert_eq!(secondary.compact_blocks_in_range(kept)?, expected);
    assert_eq!(
        range_read_tickers(&secondary).since(before),
        RangeReadTickers {
            multi_get_calls: 1,
            multi_get_keys: u64::from(KEPT_HEIGHTS),
            iterator_seeks: 0,
        },
        "the read must fetch the kept rows with one multi-get"
    );
    let [low, high] = &expected[..] else {
        return Err("the kept range must hold two compact blocks".into());
    };
    let row_bytes = |block| crate::encode_compact_block_artifact(block).len();
    assert!(
        row_bytes(high) < 2 * row_bytes(low),
        "the second kept row must fit the budget left only once, so a read whose \
         budget the kept rows spend fetches it alone"
    );
    let both_bytes = row_bytes(low) + row_bytes(high);
    for (range, order, max_bytes, kept_blocks) in [
        (kept, Ascending, usize::MAX, &expected[..]),
        (kept, Descending, usize::MAX, &expected[..]),
        (kept, Ascending, both_bytes, &expected[..]),
        (kept, Descending, both_bytes, &expected[..]),
        (
            kept,
            Ascending,
            row_bytes(low) + 1,
            std::slice::from_ref(low),
        ),
        (
            kept,
            Descending,
            row_bytes(high) + 1,
            std::slice::from_ref(high),
        ),
        (split_heights(), Ascending, both_bytes, &expected[..]),
    ] {
        let before = range_read_tickers(&secondary);
        assert_eq!(
            secondary.compact_blocks_in_range_up_to(
                range,
                order,
                CompactBlockReadBudget::bytes(max_bytes)
            )?,
            kept_blocks,
            "a {order:?} read of {range:?} with a {max_bytes}-byte budget"
        );
        assert_eq!(
            range_read_tickers(&secondary).since(before).iterator_seeks,
            0,
            "a {order:?} read of {range:?} must move no iterator"
        );
    }
    assert_eq!(
        secondary.tree_state_checkpoint_at(kept.end, &activations)?,
        store.tree_state_checkpoint_at(kept.end, &activations)?
    );
    assert_eq!(
        replayed_tree_state(&secondary, kept.end, &activations)?,
        replayed_tree_state(&store, kept.end, &activations)?
    );
    Ok(())
}

/// A range read on a stale secondary that needs a deleted table file fails as
/// a missing table file, so a reader can replace the secondary.
#[test]
fn a_range_read_still_reports_a_deleted_table_file_it_needs()
-> Result<(), Box<dyn std::error::Error>> {
    use HeightOrder::{Ascending, Descending};

    let temporary = TempDir::new()?;
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let (_store, secondary) = stale_secondary_over_kept_heights(&temporary, &activations)?;
    for (read, outcome) in [
        (
            "unbounded",
            secondary.compact_blocks_in_range(split_heights()),
        ),
        (
            "ascending",
            secondary.compact_blocks_in_range_up_to(
                split_heights(),
                Ascending,
                CompactBlockReadBudget::bytes(usize::MAX),
            ),
        ),
        (
            "descending",
            secondary.compact_blocks_in_range_up_to(
                split_heights(),
                Descending,
                CompactBlockReadBudget::bytes(usize::MAX),
            ),
        ),
        (
            "replay",
            secondary
                .tree_state_checkpoint_at(BlockHeight::new(SPLIT_HEIGHTS - 1), &activations)
                .map(|_| Vec::new()),
        ),
    ] {
        let failure = outcome
            .err()
            .ok_or("a read that needs a deleted table file must fail")?;
        assert!(failure.is_missing_table_file(), "{read} read: {failure:?}");
    }
    Ok(())
}

/// Opens a secondary on a primary whose rows each have a level-0 and a
/// bottom-level table file of their own, then compacts away the rows above
/// [`KEPT_HEIGHTS`] on the primary.
///
/// The stale secondary's view still names the deleted files. `RocksDB` opens
/// every level-0 file to build an iterator, so an iterator bounded to a kept
/// height fails on one of them, which this checks. A range read of the kept
/// heights must therefore not build an iterator.
fn stale_secondary_over_kept_heights(
    temporary: &TempDir,
    activations: &zinder_core::NetworkUpgradeActivations,
) -> Result<(RocksDbCanonicalStore, RocksDbCanonicalSecondary), Box<dyn std::error::Error>> {
    let store_path = temporary.path().join("canonical");
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(SPLIT_HEIGHTS),
        BlockHeight::new(SPLIT_HEIGHTS - REORG_WINDOW),
    )?;
    let db = &store.bounded_open.db;
    split_rows_into_bottom_level_files(db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    split_rows_into_table_files(db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        activations,
        RawBlobRetention::Transactions,
    )??;

    compact_away_rows_in(
        db,
        COMPACT_BLOCK_COLUMN_FAMILY,
        BlockHeightRange::inclusive(BlockHeight::new(KEPT_HEIGHTS + 1), split_heights().end),
    )?;
    let kept_end = kept_heights().end;
    let (passed_rows, iterator_status) =
        walk_iterator(&secondary, BlockHeightRange::inclusive(kept_end, kept_end))?;
    assert!(
        passed_rows == 0
            && iterator_status
                .as_ref()
                .is_err_and(crate::is_missing_table_file_error),
        "an iterator bounded to a kept height must open a deleted level-0 file, \
         passed {passed_rows} and got {iterator_status:?}"
    );
    Ok((store, secondary))
}

/// A shared block cache that holds a compacted-away file's blocks does not
/// hide the file's absence from a secondary that has to reopen it.
///
/// A second secondary on the same cache reads every row first, so the cache
/// holds a block of every split file. The stale secondary still opens a file
/// before it looks up any of the file's blocks, so its reads fail, each as a
/// missing table file, until it catches up.
#[test]
fn a_shared_block_cache_does_not_hide_a_compacted_away_table_file()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(SPLIT_HEIGHTS),
        BlockHeight::new(SPLIT_HEIGHTS - REORG_WINDOW),
    )?;
    split_rows_into_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let block_cache = test_block_cache();
    let mut stale = open_secondary_on(
        &store_path,
        temporary.path().join("stale"),
        &activations,
        &block_cache,
    )??;
    let warm = open_secondary_on(
        &store_path,
        temporary.path().join("warm"),
        &activations,
        &block_cache,
    )??;
    read_every_compact_block(&warm)?;
    let (_, warming_misses) = data_block_lookups(&warm);
    read_every_compact_block(&warm)?;
    assert_eq!(
        data_block_lookups(&warm).1,
        warming_misses,
        "warming must cache the block of every split row"
    );
    let warmed_usage = block_cache.usage_bytes();

    compact_away_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let failures = (1..=SPLIT_HEIGHTS)
        .filter_map(|height| stale.compact_block_at(BlockHeight::new(height)).err())
        .collect::<Vec<_>>();
    assert!(
        !failures.is_empty(),
        "a stale secondary must reopen at least one compacted-away table file"
    );
    for failure in &failures {
        assert!(
            failure.is_missing_table_file(),
            "only a missing table file may fail the stale read, got {failure:?}"
        );
    }
    let family = stale
        .bounded_open
        .db
        .cf_handle(COMPACT_BLOCK_COLUMN_FAMILY)
        .ok_or("the family must exist")?;
    let scan_failure = stale
        .bounded_open
        .db
        .iterator_cf(&family, rust_rocksdb::IteratorMode::Start)
        .find_map(Result::err)
        .ok_or("a scan over the compacted-away files must fail")?;
    assert!(crate::is_missing_table_file_error(&scan_failure));
    drop(family);
    assert!(
        block_cache.usage_bytes() >= warmed_usage,
        "the compacted-away files' blocks stay cached while the stale reads fail"
    );

    stale.try_catch_up()?;
    for height in (1..=SPLIT_HEIGHTS).map(BlockHeight::new) {
        assert_eq!(
            stale.compact_block_at(height)?,
            store.compact_block_at(height)?
        );
    }
    Ok(())
}

/// Secondaries of one primary on one shared block cache hold one copy of
/// each block they both read.
///
/// The second secondary's reads find every data block the first one loaded,
/// so they add nothing to the cache. That holds for the bulk-built files of a
/// fresh store, for files the writer flushes, and for files its compaction
/// writes. A change to how `RocksDB` derives cache keys from table files
/// would make the second secondary miss.
#[test]
fn secondaries_on_one_shared_block_cache_hold_one_copy_of_each_block()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(SPLIT_HEIGHTS),
        BlockHeight::new(SPLIT_HEIGHTS - REORG_WINDOW),
    )?;
    let block_cache = test_block_cache();
    let mut first = open_secondary_on(
        &store_path,
        temporary.path().join("first"),
        &activations,
        &block_cache,
    )??;
    let mut second = open_secondary_on(
        &store_path,
        temporary.path().join("second"),
        &activations,
        &block_cache,
    )??;
    assert_second_reads_only_cached_blocks(&first, &second, &block_cache, "bulk-built")?;

    split_rows_into_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    first.try_catch_up()?;
    second.try_catch_up()?;
    assert_second_reads_only_cached_blocks(&first, &second, &block_cache, "flushed")?;

    compact_away_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    first.try_catch_up()?;
    second.try_catch_up()?;
    assert_second_reads_only_cached_blocks(&first, &second, &block_cache, "compacted")?;
    Ok(())
}

/// A shared block cache outlives every secondary opened on it and keeps none
/// of a closed secondary's pins.
///
/// Each secondary pins the index blocks of the level-0 files it holds open.
/// Closing it releases them while the blocks stay cached, and a secondary
/// opened later on the same cache reads every block the closed one loaded.
#[test]
fn a_shared_block_cache_outlives_a_closed_secondary_without_keeping_its_pins()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(SPLIT_HEIGHTS),
        BlockHeight::new(SPLIT_HEIGHTS - REORG_WINDOW),
    )?;
    split_rows_into_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let block_cache = test_block_cache();

    let closed = open_secondary_on(
        &store_path,
        temporary.path().join("closed"),
        &activations,
        &block_cache,
    )??;
    read_every_compact_block(&closed)?;
    assert!(
        block_cache.pinned_usage_bytes() > 0,
        "an open secondary pins its level-0 index blocks"
    );
    let loaded = block_cache.usage_bytes();
    drop(closed);
    assert_eq!(block_cache.pinned_usage_bytes(), 0);
    assert_eq!(block_cache.usage_bytes(), loaded);

    let reopened = open_secondary_on(
        &store_path,
        temporary.path().join("reopened"),
        &activations,
        &block_cache,
    )??;
    let (hits_before, misses_before) = data_block_lookups(&reopened);
    read_every_compact_block(&reopened)?;
    let (hits_after, misses_after) = data_block_lookups(&reopened);
    assert_eq!(
        misses_after, misses_before,
        "a later secondary reads the blocks a closed one loaded"
    );
    assert!(hits_after > hits_before);
    assert_eq!(block_cache.usage_bytes(), loaded);
    drop(reopened);
    assert_eq!(block_cache.pinned_usage_bytes(), 0);
    Ok(())
}

/// A secondary refuses to open on a shared block cache whose capacity
/// differs from its budget's, so the recorded budget always names the cache
/// the secondary reads through.
#[test]
fn a_secondary_refuses_a_shared_block_cache_its_budget_does_not_name()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let _store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let budget = RocksDbResourceBudget::for_local_tests();
    let larger_cache = SharedBlockCache::new(budget.block_cache_bytes * 2);
    let error = open_secondary_on(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        &larger_cache,
    )?
    .err()
    .ok_or("a budget that does not name the shared cache must be refused")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::InvalidResourceBudget { reason }
                if reason.contains("shared block cache")
        ),
        "unexpected error: {error:?}"
    );
    assert_eq!(larger_cache.usage_bytes(), 0);
    Ok(())
}

#[test]
fn a_corrupt_table_file_is_not_a_missing_table_file() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    split_rows_into_table_files(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    let secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::Transactions,
    )??;
    let corrupted_height = BlockHeight::new(3);
    let corrupted_key = encode_block_position(corrupted_height);
    let corrupted_file = store
        .bounded_open
        .db
        .live_files()?
        .into_iter()
        .find(|file| {
            file.column_family_name == COMPACT_BLOCK_COLUMN_FAMILY
                && file.level == 0
                && file.start_key.as_deref() == Some(corrupted_key.as_slice())
        })
        .map(|file| table_file_path(&file))
        .ok_or("the split row must have a table file of its own")?;
    // The first data block starts the file, so flipping its first byte
    // breaks that block's checksum without touching the footer or index.
    let mut table_bytes = std::fs::read(&corrupted_file)?;
    let first_byte = table_bytes
        .first_mut()
        .ok_or("a table file is never empty")?;
    *first_byte ^= 0xff;
    std::fs::write(&corrupted_file, table_bytes)?;

    let failure = secondary
        .compact_block_at(corrupted_height)
        .err()
        .ok_or("a corrupt data block must fail the read")?;
    assert!(
        matches!(
            &failure,
            CanonicalStoreError::RocksDbOperation { source, .. }
                if source.kind() == rust_rocksdb::ErrorKind::Corruption
        ),
        "unexpected failure: {failure:?}"
    );
    assert!(!failure.is_missing_table_file());
    Ok(())
}

/// Live projection-build leases that give the default family more rows than a
/// secondary keeps table files open once each row has a file of its own.
const SPLIT_LEASES: u8 = 96;

/// A store-control scan reports a deleted table file as missing.
///
/// The writer's compaction can delete a default-family table file between a
/// secondary's catch-up and the store-control scan that follows it. The scan
/// then reports the deleted file with its typed source, as every other read
/// does, instead of as an admission refusal, and the next catch-up clears it.
#[test]
fn a_store_control_scan_reports_a_compacted_away_table_file_as_missing()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = append_block_five(
        published_store(
            &store_path,
            REORG_WINDOW,
            BlockHeight::new(4),
            BlockHeight::new(2),
        )?,
        &activations,
    )?;
    let anchor = store
        .canonical_event_history(CanonicalEventHistoryRequest::new(None, NonZeroU32::MIN))?
        .first()
        .ok_or("the append must leave a canonical event")?
        .projection_build_anchor();
    let now = UnixTimestampMillis::now();
    let expires_at = UnixTimestampMillis::new(now.value().saturating_add(60_000));
    for lease_byte in 0..SPLIT_LEASES {
        store.acquire_projection_build_lease(
            ProjectionBuildLease::new(
                ProjectionBuildLeaseId::from_bytes([lease_byte; 16]),
                anchor,
                expires_at,
            ),
            now,
        )?;
    }
    split_rows_into_table_files(&store.bounded_open.db, DEFAULT_COLUMN_FAMILY_NAME)?;
    let mut secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::Transactions,
    )??;

    compact_away_table_files(&store.bounded_open.db, DEFAULT_COLUMN_FAMILY_NAME)?;
    let failure = validate_open_store_control(
        &secondary.bounded_open.db,
        &store_path,
        &secondary.expectation,
    )
    .err()
    .ok_or("a scan over the compacted-away default family must fail")?;
    assert!(
        failure.is_missing_table_file(),
        "the scan must report the missing table file, got {failure:?}"
    );

    secondary.try_catch_up()?;
    assert_eq!(secondary.event_fence(), store.event_fence());
    Ok(())
}

/// A corrupt default-family table file still refuses admission.
#[test]
fn a_corrupt_store_control_row_refuses_admission() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    split_rows_into_table_files(&store.bounded_open.db, DEFAULT_COLUMN_FAMILY_NAME)?;
    for file in store
        .bounded_open
        .db
        .live_files()?
        .into_iter()
        .filter(|file| file.column_family_name == DEFAULT_COLUMN_FAMILY_NAME && file.level == 0)
    {
        // Flipping the first byte breaks the first data block's checksum
        // without touching the footer or index.
        let corrupted_file = table_file_path(&file);
        let mut table_bytes = std::fs::read(&corrupted_file)?;
        let first_byte = table_bytes
            .first_mut()
            .ok_or("a table file is never empty")?;
        *first_byte ^= 0xff;
        std::fs::write(&corrupted_file, table_bytes)?;
    }

    let failure = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::Transactions,
    )?
    .err()
    .ok_or("a corrupt store-control row must refuse admission")?;
    assert!(
        matches!(
            &failure,
            CanonicalStoreError::AdmissionRefused { reason, .. }
                if reason.starts_with("store-control iteration failed: Corruption")
        ),
        "unexpected failure: {failure:?}"
    );
    assert!(!failure.is_missing_table_file());
    Ok(())
}

#[test]
fn secondary_admission_rejects_another_network_activation_table()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let _store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let mainnet_activations = super::test_network_upgrade_activations(Network::ZcashMainnet)?;

    let error = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &mainnet_activations,
        RawBlobRetention::Transactions,
    )?
    .err()
    .ok_or("a secondary for another network must be refused")?;
    assert!(
        matches!(error, CanonicalStoreError::AdmissionRefused { .. }),
        "unexpected network-mismatch error: {error:?}"
    );
    Ok(())
}

/// A reader without a node adopts the writer's schedule: it waits for the
/// writer's first admission, then reads through the admitted table and
/// follows each later revision on catch-up.
#[test]
fn a_secondary_adopts_the_writer_admitted_schedule_once_one_exists()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let construction = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;

    let error = open_adopting_secondary(&store_path, temporary.path().join("early"))?
        .err()
        .ok_or("a READY store without an admitted schedule must refuse a node-free reader")?;
    assert!(
        matches!(
            error,
            CanonicalStoreError::NetworkUpgradeScheduleNotAdmitted { .. }
        ),
        "unexpected pre-admission error: {error:?}"
    );

    store.admit_network_upgrade_schedule(&construction)?;
    let mut secondary = open_adopting_secondary(&store_path, temporary.path().join("reader"))??;
    assert_eq!(secondary.network(), Network::ZcashTestnet);
    assert_eq!(
        secondary
            .network_upgrade_schedule()
            .map(super::CanonicalActivationSchedule::revision),
        Some(1)
    );
    assert_eq!(secondary.network_upgrade_activations(), Some(&construction));

    let extended = extend_with_future_activation(&construction)?;
    store.admit_network_upgrade_schedule(&extended)?;
    secondary.try_catch_up()?;
    assert_eq!(
        secondary
            .network_upgrade_schedule()
            .map(super::CanonicalActivationSchedule::revision),
        Some(2)
    );
    assert_eq!(secondary.network_upgrade_activations(), Some(&extended));
    Ok(())
}

#[test]
fn a_secondary_adopting_the_writer_schedule_refuses_another_network()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let construction = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    store.admit_network_upgrade_schedule(&construction)?;

    let error = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary"),
        Network::ZcashMainnet,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("a node-free reader for another network must be refused")?;
    assert!(
        matches!(error, CanonicalStoreError::AdmissionRefused { .. }),
        "unexpected network-mismatch error: {error:?}"
    );
    Ok(())
}

#[test]
fn secondary_and_reopened_primary_serve_the_last_durable_fence_after_the_primary_drops()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let (final_fence, final_epoch) = {
        let store = published_store(
            &store_path,
            REORG_WINDOW,
            BlockHeight::new(4),
            BlockHeight::new(2),
        )?;
        let store = append_block_five(store, &activations)?;
        (store.event_fence(), store.chain_epoch()?)
    };

    let secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::Transactions,
    )??;
    assert_eq!(secondary.event_fence(), final_fence);
    assert_eq!(secondary.chain_epoch()?, final_epoch);
    assert!(secondary.block_header_at(BlockHeight::new(5))?.is_some());

    let restarted = open_store(&store_path, &activations, REORG_WINDOW)?;
    assert_eq!(restarted.event_fence(), final_fence);
    assert_eq!(restarted.chain_epoch()?, final_epoch);
    Ok(())
}

#[test]
fn owner_checkpoint_reopens_at_the_visible_epoch() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let checkpoint_path = temporary.path().join("checkpoint");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;

    let evidence = store
        .create_owner_checkpoint(&checkpoint_path, RocksDbResourceBudget::for_local_tests())?;
    assert_eq!(evidence.ready_evidence, store.ready_evidence());

    let checkpoint = open_store(&checkpoint_path, &activations, REORG_WINDOW)?;
    assert_eq!(checkpoint.event_fence(), store.event_fence());
    assert_eq!(checkpoint.chain_epoch()?, store.chain_epoch()?);
    assert_eq!(
        checkpoint.block_header_at(BlockHeight::new(4))?,
        store.block_header_at(BlockHeight::new(4))?
    );
    Ok(())
}

#[test]
fn secondary_admission_reports_and_requires_the_persisted_raw_blob_retention()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    for retention in [
        RawBlobRetention::None,
        RawBlobRetention::Transactions,
        RawBlobRetention::All,
    ] {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_store_with_retention(
            &store_path,
            REORG_WINDOW,
            BlockHeight::new(4),
            BlockHeight::new(2),
            retention,
        )?;
        assert_eq!(store.raw_blob_retention(), retention);

        let mut secondary = open_secondary(
            &store_path,
            temporary.path().join("secondary"),
            &activations,
            retention,
        )??;
        secondary.try_catch_up()?;
        assert_eq!(secondary.raw_blob_retention(), retention);

        let mismatched = if retention == RawBlobRetention::All {
            RawBlobRetention::None
        } else {
            RawBlobRetention::All
        };
        let error = open_secondary(
            &store_path,
            temporary.path().join("mismatched-secondary"),
            &activations,
            mismatched,
        )?
        .err()
        .ok_or("a secondary expecting another raw-blob retention must be refused")?;
        assert!(
            matches!(error, CanonicalStoreError::AdmissionRefused { .. }),
            "unexpected retention-mismatch error for {retention:?}: {error:?}"
        );
    }
    Ok(())
}

#[test]
fn ready_primary_reopen_rejects_a_raw_blob_retention_change()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let fence = published_store_with_retention(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
        RawBlobRetention::None,
    )?
    .event_fence();

    let error = RocksDbCanonicalStore::open_ready(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::All,
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("a READY primary must not reopen under another raw-blob retention")?;
    assert!(
        matches!(error, CanonicalStoreError::AdmissionRefused { .. }),
        "unexpected retention-change error: {error:?}"
    );

    let reopened = open_store_with_retention(
        &store_path,
        &activations,
        REORG_WINDOW,
        RawBlobRetention::None,
    )?;
    assert_eq!(reopened.raw_blob_retention(), RawBlobRetention::None);
    assert_eq!(reopened.event_fence(), fence);
    Ok(())
}

/// Opens a wallet secondary, returning the admission outcome separately from
/// fixture-construction failures so tests can match the typed refusal.
fn open_secondary(
    store_path: &std::path::Path,
    secondary_path: std::path::PathBuf,
    activations: &zinder_core::NetworkUpgradeActivations,
    raw_blob_retention: RawBlobRetention,
) -> Result<Result<RocksDbCanonicalSecondary, CanonicalStoreError>, Box<dyn std::error::Error>> {
    Ok(RocksDbCanonicalSecondary::open_ready(
        store_path,
        secondary_path,
        activations,
        CanonicalStoreWorkload::Wallet,
        raw_blob_retention,
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    ))
}

/// Opens a node-free wallet secondary that adopts the writer's schedule,
/// returning the admission outcome separately from fixture failures.
fn open_adopting_secondary(
    store_path: &std::path::Path,
    secondary_path: std::path::PathBuf,
) -> Result<Result<RocksDbCanonicalSecondary, CanonicalStoreError>, Box<dyn std::error::Error>> {
    Ok(
        RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            store_path,
            secondary_path,
            Network::ZcashTestnet,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(REORG_WINDOW)?,
            RocksDbResourceBudget::for_local_tests(),
        ),
    )
}

/// Returns `activations` with one more upgrade activating far above any
/// fixture tip, which a writer admits as a future-only revision.
fn extend_with_future_activation(
    activations: &zinder_core::NetworkUpgradeActivations,
) -> Result<zinder_core::NetworkUpgradeActivations, Box<dyn std::error::Error>> {
    let mut entries = activations.activations().to_vec();
    entries.push(zinder_core::NetworkUpgradeActivation {
        branch_id: zinder_core::ConsensusBranchId::new(0x0fff_fff0),
        activation_height: BlockHeight::new(1_000_000),
        name: "FutureUpgrade".to_owned(),
    });
    Ok(zinder_core::NetworkUpgradeActivations::new(
        activations.network(),
        entries,
    )?)
}

/// Opens a canonical secondary on `block_cache`, returning the admission
/// outcome separately from fixture-construction failures.
fn open_secondary_on(
    store_path: &std::path::Path,
    secondary_path: std::path::PathBuf,
    activations: &zinder_core::NetworkUpgradeActivations,
    block_cache: &SharedBlockCache,
) -> Result<Result<RocksDbCanonicalSecondary, CanonicalStoreError>, Box<dyn std::error::Error>> {
    Ok(RocksDbCanonicalSecondary::open_ready(
        store_path,
        secondary_path,
        activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbOpenResources::with_shared_block_cache(
            RocksDbResourceBudget::for_local_tests(),
            block_cache,
        ),
    ))
}

/// A block cache sized by the local test budget, large enough that nothing a
/// test reads is evicted.
fn test_block_cache() -> SharedBlockCache {
    SharedBlockCache::new(RocksDbResourceBudget::for_local_tests().block_cache_bytes)
}

/// Every height of the split store.
fn split_heights() -> BlockHeightRange {
    BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(SPLIT_HEIGHTS))
}

fn read_every_compact_block(
    secondary: &RocksDbCanonicalSecondary,
) -> Result<(), CanonicalStoreError> {
    for height in (1..=SPLIT_HEIGHTS).map(BlockHeight::new) {
        secondary.compact_block_at(height)?;
    }
    Ok(())
}

/// Data-block cache hits and misses `secondary` has recorded.
fn data_block_lookups(secondary: &RocksDbCanonicalSecondary) -> (u64, u64) {
    let statistics = &secondary.bounded_open.statistics;
    (
        statistics.get_ticker_count(Ticker::BlockCacheDataHit),
        statistics.get_ticker_count(Ticker::BlockCacheDataMiss),
    )
}

/// Reads every compact block through `first` and then through `second`, and
/// asserts that `second` found every data block in the cache and added
/// nothing to it.
fn assert_second_reads_only_cached_blocks(
    first: &RocksDbCanonicalSecondary,
    second: &RocksDbCanonicalSecondary,
    block_cache: &SharedBlockCache,
    files: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let before_first = block_cache.usage_bytes();
    read_every_compact_block(first)?;
    let after_first = block_cache.usage_bytes();
    assert!(
        after_first > before_first,
        "reading {files} files through the first secondary must load blocks"
    );
    let (hits_before, misses_before) = data_block_lookups(second);
    read_every_compact_block(second)?;
    let (hits_after, misses_after) = data_block_lookups(second);
    assert_eq!(
        misses_after, misses_before,
        "the second secondary must find every data block of the {files} files cached"
    );
    assert!(hits_after > hits_before);
    assert_eq!(
        block_cache.usage_bytes(),
        after_first,
        "reading the {files} files through the second secondary must not grow the cache"
    );
    Ok(())
}

/// Rewrites each row of `family` unchanged and flushes it, so every row is the
/// newest version in an L0 table file of its own.
///
/// Automatic compaction stays off for the family so the files survive until
/// [`compact_away_table_files`] replaces them.
pub(super) fn split_rows_into_table_files(
    db: &rust_rocksdb::DB,
    family: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let handle = db.cf_handle(family).ok_or("the family must exist")?;
    db.set_options_cf(&handle, &[("disable_auto_compactions", "true")])?;
    let rows = db
        .iterator_cf(&handle, rust_rocksdb::IteratorMode::Start)
        .collect::<Result<Vec<_>, _>>()?;
    for (key, row) in rows {
        db.put_cf(&handle, key, row)?;
        db.flush_cf(&handle)?;
    }
    Ok(())
}

/// Compacts `family` on the primary and waits until its L0 table files are
/// deleted, as routine compaction does under a serving secondary.
pub(super) fn compact_away_table_files(
    db: &rust_rocksdb::DB,
    family: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let handle = db.cf_handle(family).ok_or("the family must exist")?;
    let replaced = db
        .live_files()?
        .into_iter()
        .filter(|file| file.column_family_name == family && file.level == 0)
        .map(|file| table_file_path(&file))
        .collect::<Vec<_>>();
    assert!(!replaced.is_empty(), "the family must hold L0 table files");
    db.compact_range_cf(&handle, None::<&[u8]>, None::<&[u8]>);
    wait_until_deleted(&replaced)
}

/// Compacts the rows of `family` keyed by `heights` on the primary and waits
/// until every table file holding one of them is deleted.
///
/// The compaction takes every level-0 file overlapping `heights` and every
/// file of the level it writes to that overlaps them, so with each row in a
/// file of its own it leaves the files of the other rows in place.
pub(super) fn compact_away_rows_in(
    db: &rust_rocksdb::DB,
    family: &str,
    heights: BlockHeightRange,
) -> Result<(), Box<dyn std::error::Error>> {
    let handle = db.cf_handle(family).ok_or("the family must exist")?;
    let start = encode_block_position(heights.start);
    let end = encode_block_position(heights.end);
    let replaced = db
        .live_files()?
        .into_iter()
        .filter(|file| {
            file.column_family_name == family
                && file.start_key.as_deref() <= Some(end.as_slice())
                && file.end_key.as_deref() >= Some(start.as_slice())
        })
        .map(|file| table_file_path(&file))
        .collect::<Vec<_>>();
    assert!(!replaced.is_empty(), "the rows must be in table files");
    db.compact_range_cf(&handle, Some(start), Some(end));
    wait_until_deleted(&replaced)
}

/// Waits until the primary deletes every file of `replaced`.
///
/// The primary deletes obsolete files after the compaction job, which can
/// finish just after the compaction call returns.
fn wait_until_deleted(replaced: &[std::path::PathBuf]) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while replaced.iter().any(|path| path.exists()) {
        if std::time::Instant::now() >= deadline {
            return Err("the primary must delete the compacted-away table files".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}

/// Rewrites `family` on the primary so that every row is in a bottom-level
/// table file of its own, and no level-0 file remains.
pub(super) fn split_rows_into_bottom_level_files(
    db: &rust_rocksdb::DB,
    family: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let handle = db.cf_handle(family).ok_or("the family must exist")?;
    // A one-byte target counted with the estimated table tail ends a
    // compaction output file after every row.
    db.set_options_cf(
        &handle,
        &[
            ("target_file_size_base", "1"),
            ("target_file_size_is_upper_bound", "true"),
        ],
    )?;
    split_rows_into_table_files(db, family)?;
    compact_away_table_files(db, family)
}

/// Walks an iterator over the compact-block rows of `secondary` bounded to
/// `heights` in ascending order, and returns how many rows it passed and its
/// status once it stops.
fn walk_iterator(
    secondary: &RocksDbCanonicalSecondary,
    heights: BlockHeightRange,
) -> Result<(usize, Result<(), rust_rocksdb::Error>), Box<dyn std::error::Error>> {
    let db = &secondary.bounded_open.db;
    let family = db
        .cf_handle(COMPACT_BLOCK_COLUMN_FAMILY)
        .ok_or("the family must exist")?;
    let mut read_options = rust_rocksdb::ReadOptions::default();
    read_options.set_iterate_lower_bound(encode_block_position(heights.start));
    read_options.set_iterate_upper_bound(encode_block_position(
        heights
            .end
            .next()
            .ok_or("the range must end below u32::MAX")?,
    ));
    let mut rows = db.raw_iterator_cf_opt(&family, read_options);
    rows.seek(encode_block_position(heights.start));
    let mut passed_rows = 0;
    while rows.valid() {
        passed_rows += 1;
        rows.next();
    }
    Ok((passed_rows, rows.status()))
}

/// How a secondary has fetched rows: its multi-get calls, the keys they
/// requested, and its iterator seeks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RangeReadTickers {
    multi_get_calls: u64,
    multi_get_keys: u64,
    iterator_seeks: u64,
}

impl RangeReadTickers {
    /// The fetches recorded after `before`.
    fn since(self, before: Self) -> Self {
        Self {
            multi_get_calls: self.multi_get_calls - before.multi_get_calls,
            multi_get_keys: self.multi_get_keys - before.multi_get_keys,
            iterator_seeks: self.iterator_seeks - before.iterator_seeks,
        }
    }
}

/// The fetches `secondary` has recorded so far.
fn range_read_tickers(secondary: &RocksDbCanonicalSecondary) -> RangeReadTickers {
    let statistics = &secondary.bounded_open.statistics;
    RangeReadTickers {
        multi_get_calls: statistics.get_ticker_count(Ticker::NumberMultigetCalls),
        multi_get_keys: statistics.get_ticker_count(Ticker::NumberMultigetKeysRead),
        iterator_seeks: statistics.get_ticker_count(Ticker::NumberDbSeek),
    }
}

/// Looks up the tree state at `height`, which `store` stores no checkpoint
/// for, and returns the tree state its replay produces.
fn replayed_tree_state(
    store: &impl super::reader::CanonicalServingRead,
    height: BlockHeight,
    activations: &zinder_core::NetworkUpgradeActivations,
) -> Result<ServedTreeState, Box<dyn std::error::Error>> {
    let Some(TreeStateLookup::Replay(replay)) =
        super::tree_state_replay::lookup_tree_state(store, height, activations)?
    else {
        return Err("the height must replay from a checkpoint below it".into());
    };
    let Some(TreeStateReplayOutcome::Replayed { tree_state, .. }) =
        super::tree_state_replay::replay_tree_state(store, &replay, activations)?
    else {
        return Err("the replay must rebuild the tree state".into());
    };
    Ok(tree_state)
}

pub(super) fn table_file_path(file: &rust_rocksdb::LiveFile) -> std::path::PathBuf {
    std::path::Path::new(&file.directory).join(file.name.trim_start_matches('/'))
}

fn append_block_five(
    store: RocksDbCanonicalStore,
    activations: &zinder_core::NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let fence = store.event_fence();
    let (store, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            fence,
            replacement_block(BlockHeight::new(5), [5; 32], [4; 32], 105)?,
            Vec::new(),
            BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32])),
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        activations,
    )?;
    Ok(store)
}
