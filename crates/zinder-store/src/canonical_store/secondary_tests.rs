use std::{num::NonZeroU32, path::Path};

use rust_rocksdb::{DEFAULT_COLUMN_FAMILY_NAME, statistics::Ticker};
use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, Network, NetworkUpgradeActivations,
    UnixTimestampMillis,
};

use super::{
    CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalLiveAppend, CanonicalReorgPolicy,
    CanonicalSequenceCheckpoint, CanonicalStoreError, CanonicalStoreReadyEvidence,
    ProjectionBuildLease, ProjectionBuildLeaseId, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    ServedTreeState, TreeStateLookup, TreeStateReplayOutcome,
    block_load::{
        encode_block_hash_location, encode_block_header, encode_block_position,
        encode_tree_state_checkpoint,
    },
    control::encode_ready_store_control_with_revisions,
    displaced_archive::{
        backfill_test_root_index_with_batch_limit, reindex_test_archive_with_roots,
        stage_partial_test_root_backfill_first_block,
    },
    live_replacement_tests::{
        canonical_block, open_store, open_store_with_retention, published_store,
        published_store_with_retention, replacement_block, replacement_block_with_retention,
    },
    live_tail::{LiveTailBlockRecord, LiveTailEncodeOptions},
    publication::{column_family, encode_live_append_event, encode_live_chain_epoch},
    rocksdb::{
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
        LIVE_TAIL_BLOCK_COLUMN_FAMILY, STORE_CONTROL_KEY, TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
        validate_open_store_control,
    },
};
use crate::{
    HeightOrder, RawBlobRetention, RocksDbOpenResources, RocksDbResourceBudget,
    RocksDbStatisticsLevel, SharedBlockCache, SharedSecondaryResources,
};

const REORG_WINDOW: u32 = 2;

#[test]
fn missing_live_tail_replay_recovers_on_a_fresh_secondary() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = TempDir::new()?;
    let (store, mut secondary, row) = hide_admitted_live_tail_row(temporary.path())?;
    let height = BlockHeight::new(3);
    let family = column_family(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    store.bounded_open.db.put_cf(
        &family,
        zinder_core::wire::encode_height_key_ascending(height),
        row,
    )?;
    drop(family);
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = append_block_five(store, &activations)?;
    let range = BlockHeightRange::inclusive(height, height);
    assert!(matches!(
        secondary.scan_canonical_replay_range(range)?.next(),
        Some(Err(CanonicalStoreError::MissingLiveTailReplay { height: missing })) if missing == height
    ));

    secondary.reopen_after_missing_live_tail(height)?;
    assert_eq!(secondary.event_fence(), store.event_fence());
    let rows = secondary
        .scan_canonical_replay_range(range)?
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].facts().block_header.height, height);
    Ok(())
}

#[test]
fn missing_live_tail_replay_fails_closed_when_a_reopen_cannot_admit_it()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let (store, mut secondary, _) = hide_admitted_live_tail_row(temporary.path())?;
    let height = BlockHeight::new(3);
    let next = BlockHeight::new(4);
    let fence = secondary.event_fence();
    assert!(matches!(
        secondary.scan_canonical_replay_range(BlockHeightRange::inclusive(next, next)),
        Err(CanonicalStoreError::MissingLiveTailReplay { height: missing }) if missing == height
    ));
    assert!(matches!(
        secondary.reopen_after_missing_live_tail(height),
        Err(CanonicalStoreError::MissingLiveTailReplay { height: missing }) if missing == height
    ));
    assert_eq!(secondary.event_fence(), fence);
    assert_eq!(store.event_fence(), fence);
    Ok(())
}

fn hide_admitted_live_tail_row(
    root: &Path,
) -> Result<(RocksDbCanonicalStore, RocksDbCanonicalSecondary, Vec<u8>), Box<dyn std::error::Error>>
{
    let store_path = root.join("canonical");
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let mut secondary = open_adopting_secondary(&store_path, root.join("secondary"))??;
    let key = zinder_core::wire::encode_height_key_ascending(BlockHeight::new(3));
    let family = column_family(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    let row = store
        .bounded_open
        .db
        .get_cf(&family, key)?
        .ok_or("fixture needs a live replay row")?;
    // Simulate the split view deterministically: the secondary still has
    // the authenticated READY fence when one replay row becomes invisible.
    store.bounded_open.db.delete_cf(&family, key)?;
    drop(family);
    let catchup = secondary.try_catch_up()?;
    assert_eq!(catchup.after(), catchup.before());
    Ok((store, secondary, row))
}

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
        RawBlobRetention::All,
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
        RawBlobRetention::All,
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
/// With the test budget's 64 open files, a secondary keeps at most 69 table
/// files open: a 54-entry table cache spread over 4 fourteen-entry shards, plus
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
    split_rows_into_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    let mut secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::All,
    )??;

    compact_away_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
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
        .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
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
    split_rows_into_bottom_level_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    split_rows_into_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    let mut stale = Vec::new();
    for (order, first) in [
        (HeightOrder::Ascending, BlockHeight::new(1)),
        (HeightOrder::Descending, BlockHeight::new(SPLIT_HEIGHTS)),
    ] {
        let secondary = open_secondary(
            &store_path,
            temporary.path().join(format!("{order:?}")),
            &activations,
            RawBlobRetention::All,
        )??;
        stale.push((secondary, order, first));
    }

    compact_away_rows_in(
        &store.bounded_open.db,
        LIVE_TAIL_BLOCK_COLUMN_FAMILY,
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
                secondary.compact_blocks_in_range_up_to(split_heights(), *order, max_bytes)?,
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
            secondary.compact_blocks_in_range_up_to(range, order, max_bytes)?,
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
            secondary.compact_blocks_in_range_up_to(split_heights(), Ascending, usize::MAX),
        ),
        (
            "descending",
            secondary.compact_blocks_in_range_up_to(split_heights(), Descending, usize::MAX),
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
    split_rows_into_bottom_level_files(db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    split_rows_into_table_files(db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    let secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        activations,
        RawBlobRetention::All,
    )??;

    compact_away_rows_in(
        db,
        LIVE_TAIL_BLOCK_COLUMN_FAMILY,
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
    split_rows_into_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
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

    compact_away_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
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
        .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
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

    split_rows_into_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    first.try_catch_up()?;
    second.try_catch_up()?;
    assert_second_reads_only_cached_blocks(&first, &second, &block_cache, "flushed")?;

    compact_away_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
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
    split_rows_into_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
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
    split_rows_into_bottom_level_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    let secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::All,
    )??;
    let corrupted_height = BlockHeight::new(2);
    let corrupted_key = encode_block_position(corrupted_height);
    let corrupted_file = store
        .bounded_open
        .db
        .live_files()?
        .into_iter()
        .find(|file| {
            file.column_family_name == LIVE_TAIL_BLOCK_COLUMN_FAMILY
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
        RawBlobRetention::All,
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
        RawBlobRetention::All,
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
        RawBlobRetention::All,
    )?
    .err()
    .ok_or("a secondary for another network must be refused")?;
    assert!(
        matches!(error, CanonicalStoreError::AdmissionRefused { .. }),
        "unexpected network-mismatch error: {error:?}"
    );
    Ok(())
}

#[test]
fn a_node_free_secondary_adopts_the_schedule_at_ready_publication()
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
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
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
        RawBlobRetention::All,
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
fn secondary_refuses_a_required_height_below_the_checkpointed_floor()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let _store = super::live_replacement_tests::published_note_commitment_store(
        &path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
        Some(BlockHeight::new(1)),
    )?;
    let requested = BlockHeight::new(1);
    let error = RocksDbCanonicalSecondary::open_ready(
        &path,
        temporary.path().join("secondary"),
        &activations,
        crate::CanonicalStoreRequirements::default().from_height(requested),
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("checkpointed history cannot serve the predecessor block")?;
    assert!(matches!(
        error,
        CanonicalStoreError::MissingStoredData {
            kind: crate::CanonicalStoredData::BlockHistory,
            required_from: Some(required),
            available_from: Some(available),
            ..
        } if required == requested && available == BlockHeight::new(2)
    ));
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
        crate::CanonicalStoreRequirements::for_blob_retention(raw_blob_retention),
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
            crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
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
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
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
        .cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
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

const MULTI_THOUSAND_BATCHES: u32 = 6;
const EPOCHS_PER_L0_BATCH: u32 = 400;
const BASELINE_TIP_HEIGHT: u32 = 12;

fn multi_epoch_block_hash(height: u32) -> Result<[u8; 32], std::num::TryFromIntError> {
    if height <= BASELINE_TIP_HEIGHT {
        return Ok([u8::try_from(height)?; 32]);
    }
    let mut hash = [0x77; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    Ok(hash)
}

fn build_empty_live_block(
    height: BlockHeight,
    frontiers: &zinder_core::CommitmentTreeFrontiers,
    include_checkpoint: bool,
) -> Result<crate::CanonicalBuildBlock, Box<dyn std::error::Error>> {
    let block_hash = multi_epoch_block_hash(height.value())?;
    let parent_hash = multi_epoch_block_hash(height.value() - 1)?;
    let mut block = canonical_block(height, block_hash, parent_hash, 1);
    block.facts.transactions.clear();
    block.facts.serialized_bytes_digest =
        zinder_core::SerializedBytesDigest::from_serialized_bytes(&[]);
    block.replay_envelope = zinder_core::encode_canonical_block_replay(
        &block.facts,
        zinder_core::CanonicalBlockReplayFormatVersion::V1,
        zinder_core::CanonicalBlockFactsDigestVersion::V1,
    );
    block.transaction_blobs.clear();
    block.block_blob = None;
    if include_checkpoint {
        block.tree_state_checkpoint = Some(zinder_core::CommitmentTreeCheckpoint::new(
            BlockId::new(height, BlockHash::from_bytes(block_hash)),
            height.value(),
            frontiers.clone(),
        ));
    }
    Ok(block)
}

#[expect(
    clippy::too_many_lines,
    reason = "seeds 2,400 valid canonical transitions across 6 L0 SSTs in memory-bounded WriteBatches without 2,400 individual fsyncs"
)]
fn seed_multi_thousand_epoch_store(
    store_path: &std::path::Path,
    activations: &zinder_core::NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let store = published_store_with_retention(
        store_path,
        REORG_WINDOW,
        BlockHeight::new(BASELINE_TIP_HEIGHT),
        BlockHeight::new(BASELINE_TIP_HEIGHT - REORG_WINDOW),
        RawBlobRetention::None,
    )?;
    let tip_frontiers = store.append_anchor()?.tip_checkpoint().frontiers.clone();
    let mut checkpoints = std::collections::VecDeque::from(store.replay_tail.checkpoints());
    let mut current_ready = store.ready_evidence();
    let revisions = store.control_revisions();
    let build_plan = store.build_plan().clone();
    let cursor_auth_key = store.cursor_auth_key;
    let db = &store.bounded_open.db;
    for family_name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
        let handle = db.cf_handle(family_name).ok_or("family must exist")?;
        db.set_options_cf(&handle, &[("disable_auto_compactions", "true")])?;
    }

    let mut next_height = BASELINE_TIP_HEIGHT + 1;
    for _ in 0..MULTI_THOUSAND_BATCHES {
        let mut batch = rust_rocksdb::WriteBatch::default();
        let batch_end_height = next_height + EPOCHS_PER_L0_BATCH - 1;
        for height_num in next_height..=batch_end_height {
            let height = BlockHeight::new(height_num);
            let include_checkpoint = height_num == batch_end_height
                || height_num.is_multiple_of(super::TREE_STATE_CHECKPOINT_STRIDE);
            let block = build_empty_live_block(height, &tip_frontiers, include_checkpoint)?;
            let height_key = encode_block_position(height);
            batch.put_cf(
                &db.cf_handle(BLOCK_HEADER_COLUMN_FAMILY)
                    .ok_or("block_header absent")?,
                height_key,
                encode_block_header(&block.facts.block_header),
            );
            let (encoded_tail, _) = LiveTailBlockRecord::encode_from_compact_block_v1(
                &block.compact_block,
                Some(block.facts.clone()),
                LiveTailEncodeOptions {
                    include_supplement: true,
                    retain_canonical_facts: true,
                },
            )?;
            batch.put_cf(
                &db.cf_handle(LIVE_TAIL_BLOCK_COLUMN_FAMILY)
                    .ok_or("live_tail_block absent")?,
                height_key,
                encoded_tail,
            );
            let hash_record =
                encode_block_hash_location(block.facts.block_header.block_hash, height);
            batch.put_cf(
                &db.cf_handle(BLOCK_HASH_INDEX_COLUMN_FAMILY)
                    .ok_or("block_hash_index absent")?,
                &hash_record[..32],
                &hash_record[32..],
            );
            if let Some(checkpoint) = &block.tree_state_checkpoint {
                batch.put_cf(
                    &db.cf_handle(TREE_STATE_CHECKPOINT_COLUMN_FAMILY)
                        .ok_or("tree_state_checkpoint absent")?,
                    height_key,
                    encode_tree_state_checkpoint(
                        checkpoint.block_time_seconds,
                        &checkpoint.frontiers,
                    ),
                );
            }
            let tip_checkpoint = *checkpoints.back().ok_or("checkpoint queue empty")?;
            let mut digest_builder =
                zinder_core::CanonicalBlockFactsSequenceDigestBuilder::resume_from_prefix(
                    tip_checkpoint.sequence_digest(),
                );
            digest_builder.try_append(
                block
                    .facts
                    .digest(zinder_core::CanonicalBlockFactsDigestVersion::V1),
            )?;
            let visible_tip = BlockId::new(height, block.facts.block_header.block_hash);
            let replay_len = u64::try_from(block.replay_envelope.as_bytes().len())?;
            let next_tip_checkpoint = CanonicalSequenceCheckpoint::from_admitted_parts(
                visible_tip,
                tip_checkpoint.retained_block_count() + 1,
                digest_builder.finish(),
                tip_checkpoint.logical_replay_bytes() + replay_len,
            );
            checkpoints.push_back(next_tip_checkpoint);
            let settled_checkpoint = checkpoints.pop_front().ok_or("settled checkpoint absent")?;
            let settled_checkpoint =
                if settled_checkpoint.through().height.value() < height_num - REORG_WINDOW {
                    checkpoints
                        .pop_front()
                        .ok_or("advanced settled checkpoint absent")?
                } else {
                    settled_checkpoint
                };
            checkpoints.push_front(settled_checkpoint);

            let previous_epoch_id = current_ready.visible_epoch;
            let next_epoch_id = zinder_core::ChainEpochId::new(previous_epoch_id.value() + 1);
            let next_sequence = current_ready.visible_event_sequence + 1;
            current_ready = CanonicalStoreReadyEvidence {
                visible_tip,
                visible_epoch: next_epoch_id,
                visible_event_sequence: next_sequence,
                visible_block_count: next_tip_checkpoint.retained_block_count(),
                visible_sequence_digest: next_tip_checkpoint.sequence_digest().as_bytes(),
                visible_logical_replay_bytes: next_tip_checkpoint.logical_replay_bytes(),
                sequence_checkpoint: settled_checkpoint,
                ..current_ready
            };
            let fence = CanonicalEventFence::from_persisted_event(
                next_epoch_id,
                next_sequence,
                visible_tip,
                current_ready.visible_block_count,
                current_ready.visible_sequence_digest,
            );
            batch.put_cf(
                &db.cf_handle(CHAIN_EPOCH_COLUMN_FAMILY)
                    .ok_or("chain_epoch absent")?,
                next_epoch_id.value().to_be_bytes(),
                encode_live_chain_epoch(
                    visible_tip,
                    settled_checkpoint.through(),
                    block.tip_metadata,
                    UnixTimestampMillis::new(1_750_000_000_000 + next_sequence),
                ),
            );
            batch.put_cf(
                &db.cf_handle(CHAIN_EVENT_COLUMN_FAMILY)
                    .ok_or("chain_event absent")?,
                next_sequence.to_be_bytes(),
                encode_live_append_event(previous_epoch_id, fence),
            );
        }
        batch.put(
            STORE_CONTROL_KEY,
            encode_ready_store_control_with_revisions(
                &build_plan,
                cursor_auth_key,
                &current_ready,
                revisions,
            )?,
        );
        db.write(&batch)?;
        for family_name in [
            CHAIN_EPOCH_COLUMN_FAMILY,
            CHAIN_EVENT_COLUMN_FAMILY,
            BLOCK_HEADER_COLUMN_FAMILY,
            LIVE_TAIL_BLOCK_COLUMN_FAMILY,
            TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
        ] {
            let handle = db.cf_handle(family_name).ok_or("family must exist")?;
            db.flush_cf(&handle)?;
        }
        next_height = batch_end_height + 1;
    }
    db.flush()?;
    drop(store);

    let reopened = open_store_with_retention(
        store_path,
        activations,
        REORG_WINDOW,
        RawBlobRetention::None,
    )?;
    for family_name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
        let handle = reopened
            .bounded_open
            .db
            .cf_handle(family_name)
            .ok_or("family must exist")?;
        reopened
            .bounded_open
            .db
            .set_options_cf(&handle, &[("disable_auto_compactions", "true")])?;
        let rows = reopened
            .bounded_open
            .db
            .iterator_cf(&handle, rust_rocksdb::IteratorMode::Start)
            .collect::<Result<Vec<_>, _>>()?;
        for chunk in rows.chunks(EPOCHS_PER_L0_BATCH as usize) {
            let mut batch = rust_rocksdb::WriteBatch::default();
            for (key, encoded) in chunk {
                batch.put_cf(&handle, key, encoded);
            }
            reopened.bounded_open.db.write(&batch)?;
            reopened.bounded_open.db.flush_cf(&handle)?;
        }
    }
    Ok(reopened)
}

#[test]
fn warm_secondary_reopen_caches_live_epoch_and_event_history_across_l0_ssts()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let mut store = seed_multi_thousand_epoch_store(&store_path, &activations)?;
    assert_eq!(store.ready_evidence().visible_epoch.value(), 2_401);
    assert_epoch_and_event_l0_file_counts(&store)?;

    let mut budget = RocksDbResourceBudget::for_local_tests();
    budget.block_cache_bytes = 32 * 1024 * 1024;
    budget.statistics_level = RocksDbStatisticsLevel::Tickers;
    budget.preferred_io_mode = crate::RocksDbIoMode::Direct;
    let shared_resources = SharedSecondaryResources::new(budget);
    let requirements =
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None);
    let reorg_policy = CanonicalReorgPolicy::new(REORG_WINDOW)?;

    let cold_secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary-cold"),
        Network::ZcashTestnet,
        requirements,
        reorg_policy,
        &shared_resources,
    )?;
    let mut ticker_snapshot = SecondaryCacheCounters::sample(&cold_secondary);
    assert!(
        ticker_snapshot.misses <= 512,
        "cold secondary open over 2,401 epochs incurred {} block-cache data misses \
         (expected bounded SST data-block reads instead of O(epochs * L0_files))",
        ticker_snapshot.misses
    );
    for step in 0_u32..3 {
        store = append_live_step_for_multi_epoch_test(store, &activations, step)?;
        verify_warm_secondary_step(
            &store,
            &store_path,
            &temporary.path().join(format!("secondary-warm-{step}")),
            &shared_resources,
            &mut ticker_snapshot,
        )?;
    }
    drop(cold_secondary);
    Ok(())
}

struct SecondaryCacheCounters {
    misses: u64,
    hits: u64,
    bytes_read: u64,
}

impl SecondaryCacheCounters {
    fn sample(secondary: &RocksDbCanonicalSecondary) -> Self {
        Self {
            misses: secondary.rocksdb_ticker_count(Ticker::BlockCacheDataMiss),
            hits: secondary.rocksdb_ticker_count(Ticker::BlockCacheDataHit),
            bytes_read: secondary.rocksdb_ticker_count(Ticker::BytesRead),
        }
    }
}

fn assert_epoch_and_event_l0_file_counts(
    store: &RocksDbCanonicalStore,
) -> Result<(), Box<dyn std::error::Error>> {
    let live_files = store.bounded_open.db.live_files()?;
    let l0_epoch_files = live_files
        .iter()
        .filter(|file| file.column_family_name == CHAIN_EPOCH_COLUMN_FAMILY && file.level == 0)
        .count();
    let l0_event_files = live_files
        .iter()
        .filter(|file| file.column_family_name == CHAIN_EVENT_COLUMN_FAMILY && file.level == 0)
        .count();
    assert!(
        l0_epoch_files >= MULTI_THOUSAND_BATCHES as usize
            && l0_event_files >= MULTI_THOUSAND_BATCHES as usize,
        "expected >= {MULTI_THOUSAND_BATCHES} L0 SSTs, got epoch={l0_epoch_files}, event={l0_event_files}"
    );
    Ok(())
}

fn append_live_step_for_multi_epoch_test(
    store: RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
    step: u32,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let tip_frontiers = store.append_anchor()?.tip_checkpoint().frontiers.clone();
    let next_height = BlockHeight::new(BASELINE_TIP_HEIGHT + 2_400 + 1 + step);
    let settled_height = BlockHeight::new(next_height.value() - REORG_WINDOW);
    let settled_hash = BlockHash::from_bytes(multi_epoch_block_hash(settled_height.value())?);
    let block = build_empty_live_block(next_height, &tip_frontiers, true)?;
    let expected_fence = store.event_fence();
    let (next_store, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            expected_fence,
            block,
            Vec::new(),
            BlockId::new(settled_height, settled_hash),
            UnixTimestampMillis::new(1_750_000_010_000 + u64::from(step)),
        ),
        activations,
    )?;
    if step == 1 {
        for family_name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
            let handle = next_store
                .bounded_open
                .db
                .cf_handle(family_name)
                .ok_or("family must exist")?;
            next_store.bounded_open.db.flush_cf(&handle)?;
        }
    }
    Ok(next_store)
}

fn verify_warm_secondary_step(
    store: &RocksDbCanonicalStore,
    store_path: &Path,
    secondary_path: &Path,
    shared_resources: &SharedSecondaryResources,
    previous: &mut SecondaryCacheCounters,
) -> Result<(), Box<dyn std::error::Error>> {
    let warm_secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        store_path,
        secondary_path,
        Network::ZcashTestnet,
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        shared_resources,
    )?;
    let after = SecondaryCacheCounters::sample(&warm_secondary);
    let delta_misses = after.misses - previous.misses;
    let delta_hits = after.hits - previous.hits;
    let delta_bytes_read = after.bytes_read - previous.bytes_read;

    assert_eq!(warm_secondary.event_fence(), store.event_fence());
    assert!(
        delta_misses <= 4,
        "warm secondary open incurred {delta_misses} block-cache data misses \
         (delta_hits={delta_hits}, delta_bytes_read={delta_bytes_read}, io_mode={:?})",
        warm_secondary.io_mode()
    );
    assert!(
        delta_hits > 0,
        "warm secondary open must serve retained epoch/event blocks from the shared cache"
    );
    *previous = after;
    Ok(())
}

#[test]
fn secondary_open_rejects_malformed_length_keys_in_epoch_and_event_families()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    for family_name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
        for bad_key in [
            &[0_u8; 4][..],
            &3_u32.to_be_bytes()[..],
            &[0_u8, 0, 0, 0, 0, 0, 0, 1, 0][..],
            &[0_u8, 0, 0, 0, 0, 0, 0, 2, 0][..],
            &[0_u8, 0, 0, 0, 0, 0, 0, 9, 0][..],
        ] {
            assert_injected_family_mutation_refused(&activations, family_name, bad_key, false)?;
        }
        assert_injected_family_mutation_refused(
            &activations,
            family_name,
            &[0_u8, 0, 0, 0, 0, 0, 0, 2, 0],
            true,
        )?;
    }
    Ok(())
}

fn assert_injected_family_mutation_refused(
    activations: &NetworkUpgradeActivations,
    family_name: &'static str,
    injected_key: &[u8],
    advance_retention_floor: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = seed_three_epoch_live_store(&store_path, activations)?;
    let handle = store
        .bounded_open
        .db
        .cf_handle(family_name)
        .ok_or("canonical family must exist")?;
    let sample_row = store
        .bounded_open
        .db
        .get_cf(&handle, 1_u64.to_be_bytes())?
        .ok_or("baseline row must exist")?;
    if advance_retention_floor {
        store.bounded_open.db.put(
            super::event_lifecycle::RETENTION_FLOOR_KEY,
            3_u64.to_be_bytes(),
        )?;
        if family_name == CHAIN_EVENT_COLUMN_FAMILY {
            store
                .bounded_open
                .db
                .delete_cf(&handle, 2_u64.to_be_bytes())?;
        }
    }
    store
        .bounded_open
        .db
        .put_cf(&handle, injected_key, sample_row)?;
    drop(handle);
    store.bounded_open.db.flush()?;
    drop(store);

    let failure = open_adopting_secondary(&store_path, temporary.path().join("secondary"))?
        .err()
        .ok_or("malformed key in epoch/event family must refuse secondary open")?;
    assert!(
        matches!(
            &failure,
            CanonicalStoreError::PublicationRefused { reason } if reason.contains("key has length")
        ),
        "unexpected error for {family_name} key {injected_key:?}: {failure:?}"
    );
    Ok(())
}

#[test]
fn secondary_open_rejects_trailing_rows_after_visible_epoch_and_missing_live_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    for family_name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
        assert_trailing_row_refused(&activations, family_name, false)?;
        assert_trailing_row_refused(&activations, family_name, true)?;
        assert_missing_live_row_refused(&activations, family_name, 2, false)?;
    }
    assert_missing_live_row_refused(&activations, CHAIN_EPOCH_COLUMN_FAMILY, 2, true)?;
    Ok(())
}

fn assert_trailing_row_refused(
    activations: &NetworkUpgradeActivations,
    family_name: &'static str,
    live_history: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = if live_history {
        seed_three_epoch_live_store(&store_path, activations)?
    } else {
        published_store(
            &store_path,
            REORG_WINDOW,
            BlockHeight::new(4),
            BlockHeight::new(2),
        )?
    };
    let trailing_sequence = store.ready_evidence().visible_epoch.value() + 1;
    let handle = store
        .bounded_open
        .db
        .cf_handle(family_name)
        .ok_or("canonical family must exist")?;
    let sample_row = store
        .bounded_open
        .db
        .get_cf(&handle, 1_u64.to_be_bytes())?
        .ok_or("baseline row must exist")?;
    store
        .bounded_open
        .db
        .put_cf(&handle, trailing_sequence.to_be_bytes(), sample_row)?;
    drop(handle);
    store.bounded_open.db.flush()?;
    drop(store);

    let failure = open_adopting_secondary(&store_path, temporary.path().join("secondary"))?
        .err()
        .ok_or("trailing row after visible epoch must refuse secondary open")?;
    assert!(
        matches!(
            &failure,
            CanonicalStoreError::PublicationRefused { reason } if reason.contains("unexpected row")
        ),
        "unexpected trailing-row error for {family_name} (live={live_history}): {failure:?}"
    );
    Ok(())
}

fn assert_missing_live_row_refused(
    activations: &NetworkUpgradeActivations,
    family_name: &'static str,
    deleted_sequence: u64,
    advance_retention_floor: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = seed_three_epoch_live_store(&store_path, activations)?;
    if advance_retention_floor {
        store.bounded_open.db.put(
            super::event_lifecycle::RETENTION_FLOOR_KEY,
            3_u64.to_be_bytes(),
        )?;
    }
    let handle = store
        .bounded_open
        .db
        .cf_handle(family_name)
        .ok_or("canonical family must exist")?;
    store
        .bounded_open
        .db
        .delete_cf(&handle, deleted_sequence.to_be_bytes())?;
    drop(handle);
    store.bounded_open.db.flush()?;
    drop(store);

    let failure = open_adopting_secondary(&store_path, temporary.path().join("secondary"))?
        .err()
        .ok_or("missing live row must refuse secondary open")?;
    assert!(
        matches!(&failure, CanonicalStoreError::PublicationRefused { .. }),
        "unexpected missing-row error for {family_name}: {failure:?}"
    );
    Ok(())
}

fn seed_three_epoch_live_store(
    store_path: &Path,
    activations: &NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let store = append_block_five(
        published_store(
            store_path,
            REORG_WINDOW,
            BlockHeight::new(4),
            BlockHeight::new(2),
        )?,
        activations,
    )?;
    let expected_fence = store.event_fence();
    let (store, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            expected_fence,
            replacement_block(BlockHeight::new(6), [6; 32], [5; 32], 106)?,
            Vec::new(),
            BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([4; 32])),
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        activations,
    )?;
    Ok(store)
}

#[test]
fn warm_secondary_reopen_caches_permanent_reorg_archive_admission_reads()
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
    let replacement_fence = store.event_fence();
    let (store, _) = store.commit_live_replacement(
        crate::CanonicalLiveReplacement::new(
            replacement_fence,
            vec![crate::CanonicalReplacementBlock::new(
                replacement_block(BlockHeight::new(4), [0x44; 32], [3; 32], 144)?,
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_010),
        ),
        &activations,
    )?;
    store.bounded_open.db.flush()?;

    let mut budget = RocksDbResourceBudget::for_local_tests();
    budget.block_cache_bytes = 32 * 1024 * 1024;
    budget.statistics_level = RocksDbStatisticsLevel::Tickers;
    budget.preferred_io_mode = crate::RocksDbIoMode::Direct;
    let shared_resources = SharedSecondaryResources::new(budget);
    let requirements = crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All);
    let reorg_policy = CanonicalReorgPolicy::new(REORG_WINDOW)?;

    let cold_secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary-cold"),
        Network::ZcashTestnet,
        requirements.clone(),
        reorg_policy,
        &shared_resources,
    )?;
    let misses_before = cold_secondary.rocksdb_ticker_count(Ticker::BlockCacheDataMiss);

    let append_fence = store.event_fence();
    let (store, _) = store.commit_live_append(
        CanonicalLiveAppend::new(
            append_fence,
            replacement_block(BlockHeight::new(5), [0x55; 32], [0x44; 32], 155)?,
            Vec::new(),
            BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32])),
            UnixTimestampMillis::new(1_750_000_000_011),
        ),
        &activations,
    )?;
    let warm_secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary-warm"),
        Network::ZcashTestnet,
        requirements,
        reorg_policy,
        &shared_resources,
    )?;
    let delta_misses =
        warm_secondary.rocksdb_ticker_count(Ticker::BlockCacheDataMiss) - misses_before;
    assert_eq!(warm_secondary.event_fence(), store.event_fence());
    assert_eq!(
        delta_misses, 0,
        "warm secondary reopen with flushed displaced archive SSTs should hit the shared cache"
    );
    drop(cold_secondary);
    Ok(())
}

fn assert_empty_displaced_archive_root_search(
    secondary: &RocksDbCanonicalSecondary,
) -> Result<(), Box<dyn std::error::Error>> {
    let empty_root_search = secondary.search_displaced_final_note_commitment_roots(
        zinder_core::FinalNoteCommitmentRoot::from_bytes([0; 32]),
        16,
    )?;
    assert_eq!(empty_root_search.matches, []);
    assert_eq!(empty_root_search.coverage, None);
    assert!(
        empty_root_search.scan_complete
            && !empty_root_search.has_more
            && empty_root_search.next_cursor.is_none()
    );
    Ok(())
}

#[test]
fn canonical_secondary_reads_and_catches_up_displaced_archive_rows()
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
    let requirements = crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All);
    let reorg_policy = CanonicalReorgPolicy::new(REORG_WINDOW)?;
    let mut secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary"),
        Network::ZcashTestnet,
        requirements,
        reorg_policy,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(secondary.displaced_block_count()?, 0);
    assert_eq!(secondary.displaced_block_archive_coverage()?, None);
    assert_empty_displaced_archive_root_search(&secondary)?;

    let replacement_fence = store.event_fence();
    let displaced_block_hash = BlockHash::from_bytes([4; 32]);
    let (store, _) = store.commit_live_replacement(
        crate::CanonicalLiveReplacement::new(
            replacement_fence,
            vec![crate::CanonicalReplacementBlock::new(
                replacement_block(BlockHeight::new(4), [0x44; 32], [3; 32], 144)?,
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_010),
        ),
        &activations,
    )?;
    let catchup = secondary.try_catch_up()?;
    assert!(catchup.advanced());
    assert_eq!(secondary.displaced_block_count()?, 1);
    let coverage = secondary
        .displaced_block_archive_coverage()?
        .ok_or("expected displaced archive coverage on secondary")?;
    assert_eq!(
        coverage.activation_event_sequence,
        catchup.after().chain_event_sequence()
    );
    let page = secondary
        .displaced_block_page(None, std::num::NonZeroU32::new(10).ok_or("nonzero limit")?)?;
    assert!(!page.has_more);
    assert_eq!(page.blocks.len(), 1);
    assert_eq!(page.blocks[0].block_hash, displaced_block_hash);
    assert_eq!(
        secondary.displaced_block_by_hash(displaced_block_hash)?,
        Some(page.blocks[0].clone())
    );
    assert_eq!(
        secondary.displaced_blocks_for_event(
            coverage.activation_event_sequence,
            std::num::NonZeroU32::new(10).ok_or("nonzero limit")?,
        )?,
        page.blocks
    );
    let root_search = secondary.search_displaced_final_note_commitment_roots(
        zinder_core::FinalNoteCommitmentRoot::from_bytes([0; 32]),
        16,
    )?;
    assert!(
        root_search.scan_complete && !root_search.has_more && root_search.next_cursor.is_none()
    );
    let root_coverage = root_search
        .coverage
        .ok_or("expected displaced root coverage on secondary")?;
    assert_eq!(root_coverage.captured_block_count, 1);
    assert_eq!(root_coverage.root_artifact_unavailable_count, 0);
    drop(store);
    Ok(())
}

#[test]
fn canonical_secondary_pages_displaced_root_index_without_scanning_full_archive()
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
    let requirements = crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All);
    let reorg_policy = CanonicalReorgPolicy::new(REORG_WINDOW)?;
    let mut secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary"),
        Network::ZcashTestnet,
        requirements,
        reorg_policy,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let fence = store.event_fence();
    let (store, _) = store.commit_live_replacement(
        crate::CanonicalLiveReplacement::new(
            fence,
            vec![crate::CanonicalReplacementBlock::new(
                replacement_block(BlockHeight::new(4), [0x44; 32], [3; 32], 144)?,
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_010),
        ),
        &activations,
    )?;
    assert!(secondary.try_catch_up()?.advanced());

    let root_page = secondary.search_displaced_final_note_commitment_roots_page(
        zinder_core::FinalNoteCommitmentRoot::from_bytes([0; 32]),
        None,
        std::num::NonZeroU32::new(16).ok_or("nonzero limit")?,
    )?;
    assert!(!root_page.has_more);
    assert!(root_page.scan_complete);
    let coverage = root_page.coverage.ok_or("expected root page coverage")?;
    assert_eq!(coverage.captured_block_count, 1);
    assert_eq!(coverage.root_artifact_unavailable_count, 0);
    drop(store);
    Ok(())
}

fn replace_suffix(
    store: RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
    blocks: Vec<crate::CanonicalReplacementBlock>,
    timestamp_millis: u64,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let fence = store.event_fence();
    let (next, _) = store.commit_live_replacement(
        crate::CanonicalLiveReplacement::new(
            fence,
            blocks,
            UnixTimestampMillis::new(timestamp_millis),
        ),
        activations,
    )?;
    Ok(next)
}

fn assert_displaced_summaries_match(
    secondary: &RocksDbCanonicalSecondary,
) -> Result<(), Box<dyn std::error::Error>> {
    let ten = NonZeroU32::new(10).ok_or("nonzero limit")?;
    let full_page = secondary.displaced_block_page(None, ten)?;
    let summary_page = secondary.displaced_block_summary_page(None, ten)?;
    assert_eq!(full_page.blocks.len(), 3);
    assert!(
        full_page
            .blocks
            .iter()
            .all(|block| block.raw_block_bytes.is_some())
    );
    assert_eq!(summary_page.blocks.len(), full_page.blocks.len());
    assert_eq!(summary_page.has_more, full_page.has_more);
    assert_eq!(
        summary_page.blocks[0].block_hash,
        BlockHash::from_bytes([0x44; 32])
    );
    for (summary, full) in summary_page.blocks.iter().zip(full_page.blocks.iter()) {
        assert_eq!(summary.block_hash, full.block_hash);
        assert_eq!(summary.header, full.header);
        assert_eq!(summary.transaction_ids, full.transaction_ids);
        assert_eq!(summary.coinbase_outputs, full.coinbase_outputs);
        assert_eq!(
            summary.displacement_event_sequence,
            full.displacement_event_sequence
        );
        assert_eq!(summary.displacement_epoch, full.displacement_epoch);
        assert_eq!(summary.displaced_at, full.displaced_at);
    }
    Ok(())
}

fn replace_heights_3_and_4(
    store: RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
    retention: RawBlobRetention,
    timestamp_millis: u64,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    replace_suffix(
        store,
        activations,
        vec![
            crate::CanonicalReplacementBlock::new(
                replacement_block_with_retention(
                    BlockHeight::new(3),
                    [0x33; 32],
                    [2; 32],
                    133,
                    retention,
                )?,
                Vec::new(),
            ),
            crate::CanonicalReplacementBlock::new(
                replacement_block_with_retention(
                    BlockHeight::new(4),
                    [0x44; 32],
                    [0x33; 32],
                    144,
                    retention,
                )?,
                Vec::new(),
            ),
        ],
        timestamp_millis,
    )
}

#[test]
fn canonical_secondary_indexes_populated_displaced_roots_and_streams_summaries()
-> Result<(), Box<dyn std::error::Error>> {
    use zinder_core::{BlockFinalNoteCommitmentRoots, FinalNoteCommitmentRoot, ShieldedProtocol};

    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let shared = FinalNoteCommitmentRoot::from_bytes([0xA1; 32]);
    let other = FinalNoteCommitmentRoot::from_bytes([0xB2; 32]);
    let store = published_store_with_retention(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
        RawBlobRetention::All,
    )?;
    let mut secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary"),
        &activations,
        RawBlobRetention::All,
    )??;

    let store = replace_heights_3_and_4(
        store,
        &activations,
        RawBlobRetention::All,
        1_750_000_000_010,
    )?;
    let store = replace_suffix(
        store,
        &activations,
        vec![crate::CanonicalReplacementBlock::new(
            replacement_block_with_retention(
                BlockHeight::new(4),
                [0x55; 32],
                [0x33; 32],
                155,
                RawBlobRetention::All,
            )?,
            Vec::new(),
        )],
        1_750_000_000_020,
    )?;
    let b4 = BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([4; 32]));
    let b44 = BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([0x44; 32]));
    reindex_test_archive_with_roots(
        &store,
        &[
            (
                b4,
                BlockFinalNoteCommitmentRoots::new(
                    b4.height,
                    b4.hash,
                    Some(shared),
                    Some(shared),
                    Some(other),
                ),
            ),
            (
                b44,
                BlockFinalNoteCommitmentRoots::new(b44.height, b44.hash, Some(shared), None, None),
            ),
        ],
    )?;

    assert!(secondary.try_catch_up()?.advanced());
    assert_displaced_summaries_match(&secondary)?;

    let cold_reopen = open_secondary(
        &store_path,
        temporary.path().join("secondary-reopen"),
        &activations,
        RawBlobRetention::All,
    )??;
    assert_indexed_root_pages(
        &cold_reopen,
        shared,
        ShieldedProtocol::Sapling,
        ShieldedProtocol::Orchard,
    )?;
    drop(store);
    Ok(())
}

fn assert_indexed_root_pages(
    secondary: &RocksDbCanonicalSecondary,
    shared: zinder_core::FinalNoteCommitmentRoot,
    sapling: zinder_core::ShieldedProtocol,
    orchard: zinder_core::ShieldedProtocol,
) -> Result<(), Box<dyn std::error::Error>> {
    let one = NonZeroU32::new(1).ok_or("nonzero limit")?;
    let p1 = secondary.search_displaced_final_note_commitment_roots_page(shared, None, one)?;
    assert!(p1.has_more && p1.scan_complete && p1.matches.len() == 1);
    assert_eq!(
        p1.matches[0].block_id.hash,
        BlockHash::from_bytes([0x44; 32])
    );
    assert_eq!(p1.matches[0].protocol, sapling);

    let p2 = secondary.search_displaced_final_note_commitment_roots_page(
        shared,
        p1.next_cursor.as_ref(),
        one,
    )?;
    assert!(p2.has_more);
    assert_eq!(p2.matches[0].block_id.hash, BlockHash::from_bytes([4; 32]));
    assert_eq!(p2.matches[0].protocol, sapling);

    let p3 = secondary.search_displaced_final_note_commitment_roots_page(
        shared,
        p2.next_cursor.as_ref(),
        one,
    )?;
    assert!(!p3.has_more);
    assert_eq!(p3.matches[0].block_id.hash, BlockHash::from_bytes([4; 32]));
    assert_eq!(p3.matches[0].protocol, orchard);
    let cov = p3.coverage.ok_or("missing indexed coverage")?;
    assert_eq!(cov.captured_block_count, 3);
    assert_eq!(cov.root_artifact_unavailable_count, 1);
    Ok(())
}

fn strip_displaced_root_index_rows(
    store: &RocksDbCanonicalStore,
) -> Result<(), Box<dyn std::error::Error>> {
    use rust_rocksdb::IteratorMode;

    let archive_cf = column_family(&store.bounded_open.db, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)?;
    let mut keys_to_delete = Vec::new();
    for entry in store
        .bounded_open
        .db
        .iterator_cf(&archive_cf, IteratorMode::Start)
    {
        let (key, _) = entry?;
        if key.first().is_some_and(|tag| *tag == 0x04 || *tag == 0x05) {
            keys_to_delete.push(key.to_vec());
        }
    }
    for key in keys_to_delete {
        store.bounded_open.db.delete_cf(&archive_cf, &key)?;
    }
    store.bounded_open.db.flush_wal(true)?;
    Ok(())
}

fn assert_bounded_legacy_root_search(
    secondary: &RocksDbCanonicalSecondary,
    matched_root: zinder_core::FinalNoteCommitmentRoot,
    ten: NonZeroU32,
) -> Result<(), Box<dyn std::error::Error>> {
    use zinder_core::FinalNoteCommitmentRoot;

    let bounded_search = secondary.search_displaced_final_note_commitment_roots(matched_root, 1)?;
    assert_eq!(bounded_search.matches.len(), 1);
    assert!(bounded_search.has_more && !bounded_search.scan_complete);
    let bounded_cursor = bounded_search
        .next_cursor
        .as_ref()
        .ok_or("expected continuation cursor when legacy search stops early")?;
    let search_tail = secondary.search_displaced_final_note_commitment_roots_page(
        matched_root,
        Some(bounded_cursor),
        ten,
    )?;
    assert_eq!(search_tail.matches.len(), 1);
    assert!(!search_tail.has_more);

    let absent_root = FinalNoteCommitmentRoot::from_bytes([0x9C; 32]);
    let truncated_absent = super::displaced_archive::search_displaced_test_roots_fallback_with_cap(
        secondary,
        absent_root,
        16,
        1,
    )?;
    assert_eq!(truncated_absent.matches, []);
    assert!(truncated_absent.has_more && !truncated_absent.scan_complete);
    let resume_cursor = truncated_absent
        .next_cursor
        .as_ref()
        .ok_or("expected continuation cursor on scan-capped empty fallback")?;
    let resumed_absent = secondary.search_displaced_final_note_commitment_roots_page(
        absent_root,
        Some(resume_cursor),
        ten,
    )?;
    assert_eq!(resumed_absent.matches, []);
    assert!(!resumed_absent.has_more);
    Ok(())
}

#[test]
fn canonical_secondary_fallback_bounds_work_and_primary_reopen_backfills()
-> Result<(), Box<dyn std::error::Error>> {
    use zinder_core::{BlockFinalNoteCommitmentRoots, FinalNoteCommitmentRoot};

    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let matched_root = FinalNoteCommitmentRoot::from_bytes([0x7E; 32]);
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let store = replace_heights_3_and_4(
        store,
        &activations,
        RawBlobRetention::All,
        1_750_000_000_010,
    )?;
    let b4 = BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([4; 32]));
    reindex_test_archive_with_roots(
        &store,
        &[(
            b4,
            BlockFinalNoteCommitmentRoots::new(
                b4.height,
                b4.hash,
                Some(matched_root),
                Some(matched_root),
                None,
            ),
        )],
    )?;
    strip_displaced_root_index_rows(&store)?;
    drop(store);

    let secondary = open_secondary(
        &store_path,
        temporary.path().join("secondary-legacy"),
        &activations,
        RawBlobRetention::All,
    )??;
    let one = NonZeroU32::new(1).ok_or("nonzero limit")?;
    let page =
        secondary.search_displaced_final_note_commitment_roots_page(matched_root, None, one)?;
    assert_eq!(page.matches.len(), 1);
    assert_eq!(page.scanned_work, 1);
    assert!(page.has_more && !page.scan_complete);

    let ten = NonZeroU32::new(10).ok_or("nonzero limit")?;
    assert_bounded_legacy_root_search(&secondary, matched_root, ten)?;

    let full_legacy =
        secondary.search_displaced_final_note_commitment_roots_page(matched_root, None, ten)?;
    assert_eq!(full_legacy.matches.len(), 2);
    assert_eq!(full_legacy.scanned_work, 2);
    assert!(!full_legacy.has_more && full_legacy.scan_complete);
    let full_cov = full_legacy.coverage.ok_or("missing full legacy coverage")?;
    assert_eq!(full_cov.captured_block_count, 2);
    assert_eq!(full_cov.root_artifact_unavailable_count, 1);

    drop(secondary);
    let _primary = open_store(&store_path, &activations, REORG_WINDOW)?;
    let upgraded_sec = open_secondary(
        &store_path,
        temporary.path().join("secondary-upgraded"),
        &activations,
        RawBlobRetention::All,
    )??;
    let two = NonZeroU32::new(2).ok_or("nonzero limit")?;
    let indexed =
        upgraded_sec.search_displaced_final_note_commitment_roots_page(matched_root, None, two)?;
    assert_eq!(indexed.matches.len(), 2);
    assert!(!indexed.has_more && indexed.scan_complete);
    let indexed_cov = indexed.coverage.ok_or("missing backfilled coverage")?;
    assert_eq!(indexed_cov.captured_block_count, 2);
    assert_eq!(indexed_cov.root_artifact_unavailable_count, 1);
    Ok(())
}

struct ReindexedArchiveFixture {
    store: super::RocksDbCanonicalStore,
    activations: zinder_core::NetworkUpgradeActivations,
    first_key: Vec<u8>,
    first_val: Vec<u8>,
}

fn open_reindexed_archive_for_mutation(
    store_path: &std::path::Path,
) -> Result<ReindexedArchiveFixture, Box<dyn std::error::Error>> {
    use rust_rocksdb::IteratorMode;
    use zinder_core::{BlockFinalNoteCommitmentRoots, FinalNoteCommitmentRoot};

    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let root_a = FinalNoteCommitmentRoot::from_bytes([0x61; 32]);
    let b3 = BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32]));
    let b4 = BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([4; 32]));
    let staged_roots = [
        (
            b3,
            BlockFinalNoteCommitmentRoots::new(b3.height, b3.hash, Some(root_a), None, None),
        ),
        (
            b4,
            BlockFinalNoteCommitmentRoots::new(b4.height, b4.hash, Some(root_a), None, None),
        ),
    ];
    let store = published_store(
        store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let store = replace_heights_3_and_4(
        store,
        &activations,
        RawBlobRetention::All,
        1_750_000_000_010,
    )?;
    reindex_test_archive_with_roots(&store, &staged_roots)?;
    let archive_cf = column_family(&store.bounded_open.db, DISPLACED_BLOCK_FACTS_COLUMN_FAMILY)?;
    let mut first_index_entry = None;
    for entry in store
        .bounded_open
        .db
        .iterator_cf(&archive_cf, IteratorMode::Start)
    {
        let (key, val) = entry?;
        if key.first() == Some(&0x04) {
            first_index_entry = Some((key.to_vec(), val.to_vec()));
            break;
        }
    }
    let (first_key, first_val) = first_index_entry.ok_or("expected [0x04] row")?;
    drop(archive_cf);
    Ok(ReindexedArchiveFixture {
        store,
        activations,
        first_key,
        first_val,
    })
}

#[test]
fn cold_admission_rejects_missing_extra_or_mismatched_displaced_root_index_rows()
-> Result<(), Box<dyn std::error::Error>> {
    // (a) Deleting one [0x04] row while retaining [0x05] = 0 must fail cold admission.
    {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("missing-row");
        let fixture = open_reindexed_archive_for_mutation(&store_path)?;
        let archive_cf = column_family(
            &fixture.store.bounded_open.db,
            DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
        )?;
        fixture
            .store
            .bounded_open
            .db
            .delete_cf(&archive_cf, &fixture.first_key)?;
        fixture.store.bounded_open.db.flush_wal(true)?;
        drop(archive_cf);
        let activations = fixture.activations;
        drop(fixture.store);
        assert!(open_store(&store_path, &activations, REORG_WINDOW).is_err());
    }

    // (b) An extra [0x04] row not backed by any archived block must fail cold admission.
    {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("extra-row");
        let mut fixture = open_reindexed_archive_for_mutation(&store_path)?;
        fixture.first_key[1] ^= 0xff;
        let archive_cf = column_family(
            &fixture.store.bounded_open.db,
            DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
        )?;
        fixture.store.bounded_open.db.put_cf(
            &archive_cf,
            &fixture.first_key,
            &fixture.first_val,
        )?;
        fixture.store.bounded_open.db.flush_wal(true)?;
        drop(archive_cf);
        let activations = fixture.activations;
        drop(fixture.store);
        assert!(open_store(&store_path, &activations, REORG_WINDOW).is_err());
    }

    Ok(())
}

#[test]
fn interrupted_batched_root_index_backfill_resumes_and_publishes_marker_last()
-> Result<(), Box<dyn std::error::Error>> {
    use zinder_core::{BlockFinalNoteCommitmentRoots, FinalNoteCommitmentRoot};

    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let root_a = FinalNoteCommitmentRoot::from_bytes([0x88; 32]);
    let b4 = BlockId::new(BlockHeight::new(4), BlockHash::from_bytes([4; 32]));
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;
    let store = replace_heights_3_and_4(
        store,
        &activations,
        RawBlobRetention::All,
        1_750_000_000_010,
    )?;
    let b4_roots =
        BlockFinalNoteCommitmentRoots::new(b4.height, b4.hash, Some(root_a), Some(root_a), None);
    reindex_test_archive_with_roots(&store, &[(b4, b4_roots)])?;
    strip_displaced_root_index_rows(&store)?;

    // Run 1-block batched backfill on an unindexed store and simulate crash after block 1.
    stage_partial_test_root_backfill_first_block(&store)?;
    drop(store);

    let secondary_mid = open_secondary(
        &store_path,
        temporary.path().join("secondary-mid"),
        &activations,
        RawBlobRetention::All,
    )??;
    let one = NonZeroU32::new(1).ok_or("nonzero limit")?;
    let fallback_page =
        secondary_mid.search_displaced_final_note_commitment_roots_page(root_a, None, one)?;
    assert_eq!(fallback_page.matches.len(), 1);
    assert!(fallback_page.has_more && !fallback_page.scan_complete);
    drop(secondary_mid);

    let resumed_primary = open_store(&store_path, &activations, REORG_WINDOW)?;
    let archive_cf = column_family(
        &resumed_primary.bounded_open.db,
        DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
    )?;
    let has_progress = resumed_primary
        .bounded_open
        .db
        .get_cf(&archive_cf, [0x06])?
        .is_some();
    let has_marker = resumed_primary
        .bounded_open
        .db
        .get_cf(&archive_cf, [0x05])?
        .is_some();
    assert!(!has_progress && has_marker);
    drop(archive_cf);
    drop(resumed_primary);

    // Also verify direct 1-block batched backfill finishes cleanly after stripping again.
    let reopen_for_batch = open_store(&store_path, &activations, REORG_WINDOW)?;
    strip_displaced_root_index_rows(&reopen_for_batch)?;
    backfill_test_root_index_with_batch_limit(&reopen_for_batch, 1)?;
    let cov = reopen_for_batch
        .search_displaced_final_note_commitment_roots_page(
            FinalNoteCommitmentRoot::from_bytes([0x99; 32]),
            None,
            one,
        )?
        .coverage
        .ok_or("missing coverage after batched backfill")?;
    assert_eq!(cov.captured_block_count, 2);
    assert_eq!(cov.root_artifact_unavailable_count, 1);
    Ok(())
}

fn synthetic_settled_entry(
    start: u32,
    end: u32,
    sha_tag: u8,
) -> super::settled_segments::layout::SettledSegmentIndexEntry {
    use super::settled_segments::layout::{SettledSegmentIndexEntry, SettledSegmentModules};
    use zinder_core::{
        CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestVersion,
    };

    let end_block = BlockId::new(
        BlockHeight::new(end),
        BlockHash::from_bytes([u8::try_from(end).unwrap_or(1); 32]),
    );
    let end_sequence_checkpoint = CanonicalSequenceCheckpoint::from_admitted_parts(
        end_block,
        u64::from(end),
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            u64::from(end),
            [sha_tag; 32],
        ),
        u64::from(end) * 64,
    );
    SettledSegmentIndexEntry {
        start_height: BlockHeight::new(start),
        end_block,
        modules: SettledSegmentModules {
            supplement: false,
            raw_bodies: false,
        },
        shielded_identity: crate::ShieldedSegmentIdentity {
            network: Network::ZcashTestnet,
            artifact_schema_version: crate::CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
            start_height: BlockHeight::new(start),
            end_block,
            sha256: [sha_tag; 32],
            index_sha256: [sha_tag; 32],
            fs_verity_sha256: None,
        },
        supplement_identity: None,
        raw_block_identity: None,
        end_sequence_checkpoint,
    }
}

#[test]
fn secondary_catch_up_rejects_non_trailing_segment_mutation_and_beyond_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let store = seed_three_epoch_live_store(&store_path, &activations)?;
    let mut secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary"),
        Network::ZcashTestnet,
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let index_cf = column_family(
        &store.bounded_open.db,
        super::rocksdb::SETTLED_SEGMENT_INDEX_COLUMN_FAMILY,
    )?;
    let put_entry = |entry: &super::settled_segments::layout::SettledSegmentIndexEntry| -> Result<(), Box<dyn std::error::Error>> {
        store.bounded_open.db.put_cf(
            &index_cf,
            zinder_core::wire::encode_height_key_ascending(entry.start_height),
            entry.encode_v1()?,
        )?;
        Ok(())
    };
    let (entry1, entry2, entry3, beyond) = (
        synthetic_settled_entry(1, 1, 1),
        synthetic_settled_entry(2, 2, 2),
        synthetic_settled_entry(3, 4, 3),
        synthetic_settled_entry(5, 6, 4),
    );
    for entry in [&entry1, &entry2, &entry3, &beyond] {
        put_entry(entry)?;
    }
    store.bounded_open.db.flush_wal(true)?;
    let beyond_err = secondary.try_catch_up().err().ok_or("beyond checkpoint")?;
    assert!(
        beyond_err
            .to_string()
            .contains("exceeds READY settled checkpoint")
    );

    store.bounded_open.db.delete_cf(
        &index_cf,
        zinder_core::wire::encode_height_key_ascending(beyond.start_height),
    )?;
    store.bounded_open.db.flush_wal(true)?;
    secondary.sealed_segments = vec![entry1.clone(), entry2.clone(), entry3];
    secondary.try_catch_up()?;

    put_entry(&synthetic_settled_entry(1, 1, 99))?;
    store.bounded_open.db.flush_wal(true)?;
    let mutate_err = secondary.try_catch_up().err().ok_or("mutated prefix")?;
    assert!(mutate_err.to_string().contains("prefix mutated"));

    put_entry(&entry1)?;
    store.bounded_open.db.delete_cf(
        &index_cf,
        zinder_core::wire::encode_height_key_ascending(entry2.start_height),
    )?;
    store.bounded_open.db.flush_wal(true)?;
    let delete_err = secondary.try_catch_up().err().ok_or("deleted interior")?;
    assert!(matches!(
        delete_err,
        CanonicalStoreError::SettledSegment { .. }
    ));

    // Contiguous prefix replacement [1..=1, 2..=2] -> [1..=2] with unchanged trailing [3..=4].
    put_entry(&synthetic_settled_entry(1, 2, 12))?;
    store.bounded_open.db.flush_wal(true)?;
    let merged_err = secondary
        .try_catch_up()
        .err()
        .ok_or("merged contiguous prefix")?;
    assert!(merged_err.to_string().contains("prefix mutated"));
    Ok(())
}

fn boundary_det_hash(height: u32) -> [u8; 32] {
    let mut out = [0_u8; 32];
    out[..4].copy_from_slice(&height.to_be_bytes());
    out[4] = 0xa7;
    out
}

fn boundary_public_facts(
    tx_id: zinder_core::TransactionId,
    height_num: u32,
    counts: zinder_core::TransactionComponentCounts,
    privacy_shape: zinder_core::PrivacyShape,
    size_bytes: u32,
) -> zinder_core::TransactionPublicFacts {
    use zinder_core::{LockTime, TransactionPublicFacts, TransactionVersion};

    TransactionPublicFacts {
        transaction_id: tx_id,
        auth_digest: None,
        wtxid: None,
        version: TransactionVersion::V5,
        consensus_branch_id: None,
        lock_time: LockTime::Height(BlockHeight::new(height_num)),
        expiry_height: Some(BlockHeight::new(height_num + 20)),
        size_bytes,
        counts,
        orchard_value_balance_zat: None,
        orchard_anchor: None,
        ironwood_value_balance_zat: None,
        privacy_shape,
        is_coinbase: true,
        unsupported_sections: Vec::new(),
    }
}

fn boundary_tx_variant(
    height_num: u32,
) -> (
    zinder_core::CompactTransactionData,
    zinder_core::TransactionComponentCounts,
    zinder_core::PrivacyShape,
    Vec<zinder_core::TransparentOutputFact>,
) {
    use zinder_core::{
        CompactSaplingSpend, CompactTransactionData, CompactTransparentOutput, PrivacyShape,
        TransactionComponentCounts, TransparentAddressScriptHash, TransparentOutputFact,
    };

    match height_num % 3 {
        0 => (
            CompactTransactionData {
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: boundary_det_hash(height_num.wrapping_add(200_000)),
                }],
                ..Default::default()
            },
            TransactionComponentCounts {
                sapling_spend_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
            PrivacyShape::ShieldedOnly,
            Vec::new(),
        ),
        1 => (
            CompactTransactionData {
                fee_zat: Some(250),
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 10_000,
                    script_pub_key: vec![0x76, 0xa9, 0x14, 0x88, 0xac],
                }],
                ..Default::default()
            },
            TransactionComponentCounts {
                transparent_output_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
            PrivacyShape::TransparentOnly,
            vec![TransparentOutputFact::new(
                0,
                10_000,
                vec![0x76, 0xa9, 0x14, 0x88, 0xac],
                TransparentAddressScriptHash::from_bytes([0x11; 32]),
            )],
        ),
        _ => (
            CompactTransactionData {
                fee_zat: Some(500),
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: boundary_det_hash(height_num.wrapping_add(300_000)),
                }],
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 20_000,
                    script_pub_key: vec![0x00, 0x14, 0x22],
                }],
                ..Default::default()
            },
            TransactionComponentCounts {
                transparent_output_count: 1,
                sapling_spend_count: 1,
                ..TransactionComponentCounts::EMPTY
            },
            PrivacyShape::Mixed,
            vec![TransparentOutputFact::new(
                0,
                20_000,
                vec![0x00, 0x14, 0x22],
                TransparentAddressScriptHash::from_bytes([0x22; 32]),
            )],
        ),
    }
}

fn boundary_tx_and_facts(
    height_num: u32,
) -> (
    zinder_core::CompactTransaction,
    zinder_core::CanonicalTransactionFacts,
) {
    use zinder_core::{
        CanonicalTransactionFacts, CompactTransaction, SerializedBytesDigest, TransactionId,
        TransactionIntrinsicValueBalances,
    };

    let tx_id = TransactionId::from_bytes(boundary_det_hash(height_num.wrapping_add(100_000)));
    let (tx_data, counts, shape, outputs) = boundary_tx_variant(height_num);
    (
        CompactTransaction {
            index: 0,
            transaction_id: tx_id,
            data: tx_data,
        },
        CanonicalTransactionFacts {
            public_facts: boundary_public_facts(tx_id, height_num, counts, shape, 256),
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
                &height_num.to_be_bytes(),
            ),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
            transparent_inputs: Vec::new(),
            transparent_outputs: outputs,
        },
    )
}

fn boundary_build_block(
    height_num: u32,
    parent_hash: [u8; 32],
    activations: &NetworkUpgradeActivations,
    with_checkpoint: bool,
) -> Result<crate::CanonicalBuildBlock, Box<dyn std::error::Error>> {
    use zinder_core::{
        BlockHeaderArtifact, CanonicalBlockFacts, CanonicalBlockFactsDigestVersion,
        CanonicalBlockReplayFormatVersion, CompactChainMetadata, SerializedBytesDigest,
        encode_canonical_block_replay,
    };

    let height = BlockHeight::new(height_num);
    let block_hash = boundary_det_hash(height_num);
    let (compact_tx, tx_facts) = boundary_tx_and_facts(height_num);
    let facts = CanonicalBlockFacts {
        block_header: BlockHeaderArtifact::new(
            height,
            BlockHash::from_bytes(block_hash),
            BlockHash::from_bytes(parent_hash),
            [3; 32],
            [4; 32],
            1_750_000_000 + i64::from(height_num),
            0x1d00_ffff,
            [5; 32],
            4,
            128,
        ),
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
            &height_num.to_be_bytes(),
        ),
        transactions: vec![tx_facts],
    };
    let replay_envelope = encode_canonical_block_replay(
        &facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    let checkpoint = with_checkpoint
        .then(|| {
            super::test_checkpoint_frontiers(activations, height).map(|frontiers| {
                zinder_core::CommitmentTreeCheckpoint::new(
                    BlockId::new(height, BlockHash::from_bytes(block_hash)),
                    1_750_000_000 + height_num,
                    frontiers,
                )
            })
        })
        .transpose()?;
    Ok(crate::CanonicalBuildBlock {
        compact_block: zinder_core::CompactBlockArtifact::new(
            BlockId::new(height, BlockHash::from_bytes(block_hash)),
            BlockHash::from_bytes(parent_hash),
            1_750_000_000 + height_num,
            vec![compact_tx],
            CompactChainMetadata {
                sapling_commitment_tree_size: 0,
                orchard_commitment_tree_size: 0,
                ironwood_commitment_tree_size: 0,
            },
        )?,
        replay_envelope,
        tip_metadata: zinder_core::ChainTipMetadata::new(0, 0, 0),
        tree_state_checkpoint: checkpoint,
        transaction_blobs: Vec::new(),
        block_blob: None,
        facts,
    })
}

fn published_4096_boundary_store(
    store_path: &std::path::Path,
) -> Result<(RocksDbCanonicalStore, NetworkUpgradeActivations), Box<dyn std::error::Error>> {
    use crate::{
        CanonicalBaselinePublication, CanonicalStoreBuildPlan, RocksDbCanonicalBuilder,
        TREE_STATE_CHECKPOINT_STRIDE,
    };

    const BOUNDARY_TIP: u32 = 4_104;
    const BOUNDARY_SETTLED: u32 = crate::DEFAULT_SETTLED_SEGMENT_BLOCKS;
    const BOUNDARY_REORG: u32 = BOUNDARY_TIP - BOUNDARY_SETTLED;

    let activations = super::test_network_upgrade_activations(Network::ZcashTestnet)?;
    let tip = BlockId::new(
        BlockHeight::new(BOUNDARY_TIP),
        BlockHash::from_bytes(boundary_det_hash(BOUNDARY_TIP)),
    );
    let plan = CanonicalStoreBuildPlan::complete(
        &activations,
        0,
        tip,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(BOUNDARY_REORG)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        store_path,
        plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let mut blocks: Vec<Result<crate::CanonicalBuildBlock, std::io::Error>> =
        Vec::with_capacity(usize::try_from(BOUNDARY_TIP)?);
    let mut parent = Network::ZcashTestnet.genesis_hash().as_bytes();
    for h in 1_u32..=BOUNDARY_TIP {
        let with_ckpt = h == BOUNDARY_TIP || h.is_multiple_of(TREE_STATE_CHECKPOINT_STRIDE);
        let blk = boundary_build_block(h, parent, &activations, with_ckpt)?;
        parent = blk.facts.block_header.block_hash.as_bytes();
        blocks.push(Ok(blk));
    }
    builder.bulk_load_blocks(&activations, blocks)?;
    builder.load_subtree_roots(std::iter::empty())?;
    builder.confirm_source_tip_checkpoint(&zinder_core::CommitmentTreeCheckpoint::new(
        tip,
        1_750_000_000 + BOUNDARY_TIP,
        super::test_checkpoint_frontiers(&activations, BlockHeight::new(BOUNDARY_TIP))?,
    ))?;
    let validated = builder.prepare_cold_certified_publication()?;
    let settled = BlockId::new(
        BlockHeight::new(BOUNDARY_SETTLED),
        BlockHash::from_bytes(boundary_det_hash(BOUNDARY_SETTLED)),
    );
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        settled,
        UnixTimestampMillis::new(1_750_000_000_000),
    ))?;
    let store = validated.publish_baseline(publication)?;
    Ok((store, activations))
}

/// Consumes `RocksDB`'s `min(16, table_cache_capacity / 4)` open-time
/// `table_reader_handle` pins on `default` (`CF 0`) so later column families
/// (including `live_tail_block`) receive none at secondary open.
fn saturate_initial_open_pins(db: &rust_rocksdb::DB) -> Result<(), Box<dyn std::error::Error>> {
    let default_cf = db
        .cf_handle(DEFAULT_COLUMN_FAMILY_NAME)
        .ok_or("default family must exist")?;
    db.set_options_cf(&default_cf, &[("disable_auto_compactions", "true")])?;
    let (control_key, control_row) = db
        .iterator_cf(&default_cf, rust_rocksdb::IteratorMode::Start)
        .next()
        .ok_or("default family must hold store control")??;
    for _ in 0..16 {
        db.put_cf(&default_cf, &control_key, &control_row)?;
        db.flush_cf(&default_cf)?;
    }
    Ok(())
}

/// Splits the first `limit` rows of `family` into individual L0 table files
/// while keeping auto-compactions disabled.
fn split_first_rows_into_table_files(
    db: &rust_rocksdb::DB,
    family: &str,
    limit: usize,
) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
    let handle = db.cf_handle(family).ok_or("the family must exist")?;
    db.set_options_cf(&handle, &[("disable_auto_compactions", "true")])?;
    let rows = db
        .iterator_cf(&handle, rust_rocksdb::IteratorMode::Start)
        .take(limit)
        .collect::<Result<Vec<_>, _>>()?;
    let mut heights = Vec::with_capacity(rows.len());
    for (key, row) in rows {
        if key.len() == 4 {
            let raw: [u8; 4] = key.as_ref().try_into()?;
            heights.push(u32::from_be_bytes(raw));
        }
        db.put_cf(&handle, key, row)?;
        db.flush_cf(&handle)?;
    }
    Ok(heights)
}

/// Evicts unpinned table-cache handles across all shards by point-reading `heights`
/// from `block_header`, each backed by its own L0 SST.
fn evict_table_cache_via_block_headers(
    secondary: &RocksDbCanonicalSecondary,
    heights: &[u32],
) -> Result<(), Box<dyn std::error::Error>> {
    for &height in heights {
        let _ = secondary.block_header_at(BlockHeight::new(height))?;
    }
    Ok(())
}

#[test]
fn encoded_range_and_unary_reads_across_4096_boundary_invalidate_on_missing_table_file()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let (store, activations) = published_4096_boundary_store(&store_path)?;
    saturate_initial_open_pins(&store.bounded_open.db)?;
    let header_heights =
        split_first_rows_into_table_files(&store.bounded_open.db, BLOCK_HEADER_COLUMN_FAMILY, 80)?;
    split_rows_into_bottom_level_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    split_rows_into_table_files(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;

    let mut tight_budget = RocksDbResourceBudget::for_local_tests();
    tight_budget.max_open_files = RocksDbResourceBudget::MIN_MAX_OPEN_FILES;
    let mut secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary"),
        Network::ZcashTestnet,
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(8)?,
        tight_budget,
    )?;
    assert_eq!(secondary.sealed_segments.len(), 1);
    assert_eq!(
        secondary.sealed_segments[0].end_block.height,
        BlockHeight::new(4_096)
    );
    evict_table_cache_via_block_headers(&secondary, &header_heights)?;

    compact_away_rows_in(
        &store.bounded_open.db,
        LIVE_TAIL_BLOCK_COLUMN_FAMILY,
        BlockHeightRange::inclusive(BlockHeight::new(4_097), BlockHeight::new(4_104)),
    )?;
    assert_compacted_boundary_behavior(&secondary, &store)?;

    secondary.try_catch_up()?;
    assert_post_catch_up_boundary_reads(&secondary)?;
    let _ = activations;
    Ok(())
}

fn assert_compacted_boundary_behavior(
    secondary: &RocksDbCanonicalSecondary,
    store: &RocksDbCanonicalStore,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::{
        EncodedCompactBlockOutput, EncodedCompactBlockRangeRequest, ShieldedProjectionSelection,
    };

    for include_transparent in [false, true] {
        let two_settled = secondary
            .encoded_compact_block_range_up_to(
                EncodedCompactBlockRangeRequest {
                    range: BlockHeightRange::inclusive(
                        BlockHeight::new(4_095),
                        BlockHeight::new(4_100),
                    ),
                    order: HeightOrder::Ascending,
                    selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                    include_transparent,
                    output: EncodedCompactBlockOutput::Blocks,
                    max_bytes: 160,
                    enforce_wire_fee_limit: true,
                },
                None,
            )?
            .ok_or("expected settled prefix")?;
        assert!(two_settled.covered.end <= BlockHeight::new(4_096));
    }
    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        for include_transparent in [false, true] {
            let err = secondary
                .encoded_compact_block_range_up_to(
                    EncodedCompactBlockRangeRequest {
                        range: BlockHeightRange::inclusive(
                            BlockHeight::new(4_095),
                            BlockHeight::new(4_100),
                        ),
                        order,
                        selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                        include_transparent,
                        output: EncodedCompactBlockOutput::Blocks,
                        max_bytes: usize::MAX,
                        enforce_wire_fee_limit: true,
                    },
                    None,
                )
                .err()
                .ok_or("compacted boundary must fail with MissingTableFile")?;
            assert!(err.is_missing_table_file(), "unexpected error: {err:?}");
        }
    }
    assert_compacted_unary_tail_behavior(secondary, store)
}

fn assert_compacted_unary_tail_behavior(
    secondary: &RocksDbCanonicalSecondary,
    store: &RocksDbCanonicalStore,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::CompactBlockSpliceSelection;

    for selection in [
        CompactBlockSpliceSelection::FULL_BLOCK,
        CompactBlockSpliceSelection::BLOCK_NULLIFIERS,
    ] {
        let mut missing_failures = 0_usize;
        for h in 4_097_u32..=4_104 {
            let height = BlockHeight::new(h);
            match secondary.encoded_compact_block_at(height, selection) {
                Ok(Some(actual)) => {
                    let expected = store
                        .encoded_compact_block_at(height, selection)?
                        .ok_or("primary must hold live-tail block")?;
                    assert_eq!(actual.payload().as_ref(), expected.payload().as_ref());
                }
                Ok(None) => return Err("live-tail block must not silently return None".into()),
                Err(err) => {
                    assert!(err.is_missing_table_file(), "unexpected error: {err:?}");
                    missing_failures += 1;
                }
            }
        }
        assert!(missing_failures > 0, "expected MissingTableFile on tail");
    }
    let settled_actual = secondary
        .encoded_compact_block_at(
            BlockHeight::new(4_096),
            CompactBlockSpliceSelection::FULL_BLOCK,
        )?
        .ok_or("settled height 4,096 must stay readable")?;
    let settled_expected = store
        .encoded_compact_block_at(
            BlockHeight::new(4_096),
            CompactBlockSpliceSelection::FULL_BLOCK,
        )?
        .ok_or("primary must hold height 4,096")?;
    assert_eq!(
        settled_actual.payload().as_ref(),
        settled_expected.payload().as_ref()
    );
    assert!(
        secondary
            .encoded_compact_block_at(
                BlockHeight::new(5_000),
                CompactBlockSpliceSelection::FULL_BLOCK,
            )?
            .is_none()
    );
    Ok(())
}

fn assert_post_catch_up_boundary_reads(
    secondary: &RocksDbCanonicalSecondary,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::{
        EncodedCompactBlockOutput, EncodedCompactBlockPayload, EncodedCompactBlockRangeRequest,
        ShieldedProjectionSelection,
    };

    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        for include_transparent in [false, true] {
            let full = secondary
                .encoded_compact_block_range_up_to(
                    EncodedCompactBlockRangeRequest {
                        range: BlockHeightRange::inclusive(
                            BlockHeight::new(4_095),
                            BlockHeight::new(4_104),
                        ),
                        order,
                        selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                        include_transparent,
                        output: EncodedCompactBlockOutput::Blocks,
                        max_bytes: usize::MAX,
                        enforce_wire_fee_limit: true,
                    },
                    None,
                )?
                .ok_or("expected full cross-boundary range after catch-up")?;
            assert_eq!(full.block_count, 10);
            let EncodedCompactBlockPayload::Blocks(blocks) = full.payload else {
                return Err("expected Blocks variant".into());
            };
            assert_eq!(blocks.len(), 10);
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct DbScanTicker {
    next_calls: u64,
    prev_calls: u64,
}

fn capture_db_scan_ticker(secondary: &RocksDbCanonicalSecondary) -> DbScanTicker {
    DbScanTicker {
        next_calls: secondary
            .bounded_open
            .statistics
            .get_ticker_count(Ticker::NumberDbNext),
        prev_calls: secondary
            .bounded_open
            .statistics
            .get_ticker_count(Ticker::NumberDbPrev),
    }
}

fn assert_zero_table_scans(
    secondary: &RocksDbCanonicalSecondary,
    before_range: RangeReadTickers,
    before_scan: DbScanTicker,
) {
    let delta = range_read_tickers(secondary).since(before_range);
    let after_scan = capture_db_scan_ticker(secondary);
    assert_eq!(delta.iterator_seeks, 0);
    assert_eq!(after_scan.next_calls - before_scan.next_calls, 0);
    assert_eq!(after_scan.prev_calls - before_scan.prev_calls, 0);
}

#[test]
fn encoded_range_and_unary_reads_have_bounded_lookups_and_zero_table_scans()
-> Result<(), Box<dyn std::error::Error>> {
    use super::settled_segments::layout::SettledSegmentKind;
    use crate::{
        EncodedCompactBlockOutput, EncodedCompactBlockRangeRequest, ShieldedProjectionSelection,
    };

    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let (store, _) = published_4096_boundary_store(&store_path)?;
    let secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
        &store_path,
        temporary.path().join("secondary"),
        Network::ZcashTestnet,
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(8)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    // 1. Settled-only shielded range (1..=4,096): zero RocksDB MultiGet/seek/next/prev, zero ZSTSUP01 opens.
    secondary.segment_cache.clear();
    let before = range_read_tickers(&secondary);
    let scan_before = capture_db_scan_ticker(&secondary);
    let settled_shielded = secondary
        .encoded_compact_block_range_up_to(
            EncodedCompactBlockRangeRequest {
                range: BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(4_096)),
                order: HeightOrder::Ascending,
                selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                include_transparent: false,
                output: EncodedCompactBlockOutput::Blocks,
                max_bytes: usize::MAX,
                enforce_wire_fee_limit: true,
            },
            None,
        )?
        .ok_or("expected settled shielded range")?;
    assert_eq!(settled_shielded.block_count, 4_096);
    assert_zero_table_scans(&secondary, before, scan_before);
    let delta = range_read_tickers(&secondary).since(before);
    assert_eq!(delta.multi_get_calls, 0);
    assert_eq!(delta.multi_get_keys, 0);
    assert_eq!(
        secondary
            .segment_cache
            .open_count(SettledSegmentKind::Shielded),
        1
    );
    assert_eq!(
        secondary
            .segment_cache
            .open_count(SettledSegmentKind::Supplement),
        0
    );

    assert_boundary_range_tickers(&secondary)?;
    assert_unary_lookup_tickers(&secondary)?;
    drop(store);
    Ok(())
}

fn assert_boundary_range_tickers(
    secondary: &RocksDbCanonicalSecondary,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::{
        EncodedCompactBlockOutput, EncodedCompactBlockRangeRequest, ShieldedProjectionSelection,
    };

    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        let before = range_read_tickers(secondary);
        let scan_before = capture_db_scan_ticker(secondary);
        let boundary = secondary
            .encoded_compact_block_range_up_to(
                EncodedCompactBlockRangeRequest {
                    range: BlockHeightRange::inclusive(
                        BlockHeight::new(4_095),
                        BlockHeight::new(4_104),
                    ),
                    order,
                    selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                    include_transparent: true,
                    output: EncodedCompactBlockOutput::Blocks,
                    max_bytes: usize::MAX,
                    enforce_wire_fee_limit: true,
                },
                None,
            )?
            .ok_or("expected cross-boundary range")?;
        assert_eq!(boundary.block_count, 10);
        assert_zero_table_scans(secondary, before, scan_before);
        let delta = range_read_tickers(secondary).since(before);
        // Probe batch (1 key) learns largest_live_row_bytes; second MultiGet fetches the remaining 7 keys.
        assert_eq!(delta.multi_get_calls, 2);
        assert_eq!(delta.multi_get_keys, 8);
    }

    let before_t = range_read_tickers(secondary);
    let scan_t = capture_db_scan_ticker(secondary);
    let settled_transparent = secondary
        .encoded_compact_block_range_up_to(
            EncodedCompactBlockRangeRequest {
                range: BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(4_096)),
                order: HeightOrder::Descending,
                selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
                include_transparent: true,
                output: EncodedCompactBlockOutput::Blocks,
                max_bytes: usize::MAX,
                enforce_wire_fee_limit: true,
            },
            None,
        )?
        .ok_or("expected settled transparent range")?;
    assert_eq!(settled_transparent.block_count, 4_096);
    assert_zero_table_scans(secondary, before_t, scan_t);
    let delta_t = range_read_tickers(secondary).since(before_t);
    assert_eq!(delta_t.multi_get_calls, 0);
    assert_eq!(delta_t.multi_get_keys, 0);
    Ok(())
}

fn assert_unary_lookup_tickers(
    secondary: &RocksDbCanonicalSecondary,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::CompactBlockSpliceSelection;

    let before_settled = range_read_tickers(secondary);
    let scan_settled = capture_db_scan_ticker(secondary);
    assert!(
        secondary
            .encoded_compact_block_at(
                BlockHeight::new(2_048),
                CompactBlockSpliceSelection::FULL_BLOCK,
            )?
            .is_some()
    );
    assert_zero_table_scans(secondary, before_settled, scan_settled);
    assert_eq!(
        range_read_tickers(secondary)
            .since(before_settled)
            .multi_get_calls,
        0
    );

    for selection in [
        CompactBlockSpliceSelection::FULL_BLOCK,
        CompactBlockSpliceSelection::BLOCK_NULLIFIERS,
    ] {
        let before_tail = range_read_tickers(secondary);
        let scan_tail = capture_db_scan_ticker(secondary);
        assert!(
            secondary
                .encoded_compact_block_at(BlockHeight::new(4_100), selection)?
                .is_some()
        );
        assert_zero_table_scans(secondary, before_tail, scan_tail);
    }
    Ok(())
}

#[test]
fn unary_encoded_compact_block_reports_missing_retained_live_tail_row_as_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::CompactBlockSpliceSelection;

    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let store = published_store(
        &store_path,
        REORG_WINDOW,
        BlockHeight::new(4),
        BlockHeight::new(2),
    )?;

    for selection in [
        CompactBlockSpliceSelection::FULL_BLOCK,
        CompactBlockSpliceSelection::BLOCK_NULLIFIERS,
    ] {
        for outside in [BlockHeight::new(0), BlockHeight::new(5)] {
            assert!(
                store
                    .encoded_compact_block_at(outside, selection)?
                    .is_none()
            );
        }
    }

    let live_tail_cf = column_family(&store.bounded_open.db, LIVE_TAIL_BLOCK_COLUMN_FAMILY)?;
    store.bounded_open.db.delete_cf(
        &live_tail_cf,
        zinder_core::wire::encode_height_key_ascending(BlockHeight::new(3)),
    )?;

    for selection in [
        CompactBlockSpliceSelection::FULL_BLOCK,
        CompactBlockSpliceSelection::BLOCK_NULLIFIERS,
    ] {
        assert!(matches!(
            store.encoded_compact_block_at(BlockHeight::new(3), selection),
            Err(CanonicalStoreError::ArtifactUnavailable {
                family: crate::ArtifactFamily::CompactBlock,
                height,
            }) if height == BlockHeight::new(3)
        ));
        assert!(
            store
                .encoded_compact_block_at(BlockHeight::new(2), selection)?
                .is_some()
        );
    }
    Ok(())
}
