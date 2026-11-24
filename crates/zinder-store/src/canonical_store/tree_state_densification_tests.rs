//! Densifying the checkpoints of a store that construction checkpointed every
//! 100 blocks.
//!
//! The published test store settles at its tip, height 150, so the stride from
//! the history predecessor to height 100 is settled and the one from 100 is
//! not. Longer chains with note commitments are covered by the ingest
//! densification tests.

use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, CommitmentTreeCheckpoint, CompactBlockArtifact, Network,
    NetworkUpgradeActivations,
};

use super::{
    CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, TreeStateCheckpointSelection, TreeStateStride,
    block_load::{encode_block_position, encode_tree_state_checkpoint},
    live_replacement_tests::{
        published_fork_store, published_note_commitment_store, published_store,
    },
    publication::column_family,
    rocksdb::{
        BLOCK_HEADER_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    },
    test_network_upgrade_activations,
};
use crate::{RawBlobRetention, RocksDbResourceBudget};

const TIP_HEIGHT: u32 = 150;
const REORG_WINDOW: u32 = 100;
const EVERY_BLOCK: TreeStateCheckpointSelection = TreeStateCheckpointSelection::EveryBlock;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn every_block_densification_stores_the_reconstructed_checkpoint_at_each_settled_height()
-> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let reconstructed = reconstructed_checkpoints(&store, &activations)?;

    let committed = densify_all(&store, &activations, EVERY_BLOCK)?;

    assert_eq!(committed, vec![(stride(0, 100), 99)]);
    for height in 0..=100 {
        assert!(
            stored_row(&store, BlockHeight::new(height))?.is_some(),
            "height {height} has no stored checkpoint"
        );
    }
    for height in [101, 149] {
        assert!(stored_row(&store, BlockHeight::new(height))?.is_none());
    }
    assert_eq!(
        reconstructed_checkpoints(&store, &activations)?,
        reconstructed
    );
    assert_eq!(
        store.next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?,
        None
    );
    Ok(())
}

#[test]
fn densified_rows_hold_the_bytes_construction_writes_for_the_same_trees() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;

    densify_all(&store, &activations, EVERY_BLOCK)?;

    for height in [1, 57, 99] {
        let height = BlockHeight::new(height);
        let checkpoint = store
            .tree_state_checkpoint_at(height, &activations)?
            .ok_or("checkpoint is absent")?;
        assert_eq!(
            stored_row(&store, height)?,
            Some(encode_tree_state_checkpoint(
                checkpoint.block_time_seconds,
                &checkpoint.frontiers
            ))
        );
    }
    Ok(())
}

#[test]
fn a_committed_stride_is_neither_found_nor_rewritten_again() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let densified = store.densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)?;

    let first = store.commit_tree_state_stride(&densified)?;
    let second = store.commit_tree_state_stride(&densified)?;

    assert_eq!((first.rows_written, first.rows_already_present), (99, 0));
    assert_eq!((second.rows_written, second.rows_already_present), (0, 99));
    assert_eq!(
        store.next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?,
        None
    );
    let replayed = store.densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)?;
    assert_eq!(replayed.row_count(), 0);
    Ok(())
}

#[test]
fn an_uncommitted_stride_is_found_again_after_reopening() -> TestResult {
    let temporary = TempDir::new()?;
    let activations = activations()?;
    {
        let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
        let stride = store
            .next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?
            .ok_or("the first stride needs densifying")?;
        // The process stops after replaying the stride and before its commit.
        drop(store.densify_tree_state_stride(stride, EVERY_BLOCK, &activations)?);
    }
    let store = reopen_primary(&temporary, &activations)?;

    assert_eq!(
        store.next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?,
        Some(stride(0, 100))
    );
    assert_eq!(
        densify_all(&store, &activations, EVERY_BLOCK)?,
        vec![(stride(0, 100), 99)]
    );
    Ok(())
}

#[test]
fn a_secondary_densifies_and_the_primary_commits_what_it_replayed() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let mut secondary = open_secondary(&temporary, &activations)?;

    let stride = secondary
        .next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?
        .ok_or("the first stride needs densifying")?;
    let densified = secondary.densify_tree_state_stride(stride, EVERY_BLOCK, &activations)?;
    let commit = store.commit_tree_state_stride(&densified)?;

    assert_eq!(commit.rows_written, 99);
    secondary.try_catch_up()?;
    assert_eq!(
        secondary.next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?,
        None
    );
    Ok(())
}

#[test]
fn a_reader_reopened_after_densification_reads_and_seeds_from_the_new_rows() -> TestResult {
    let temporary = TempDir::new()?;
    let activations = activations()?;
    let reconstructed = {
        let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
        let reconstructed = reconstructed_checkpoints(&store, &activations)?;
        densify_all(&store, &activations, EVERY_BLOCK)?;
        reconstructed
    };
    let store = reopen_primary(&temporary, &activations)?;
    // Without the densified row at 50, a read there seeds from the row at 49.
    // A seed at the cadence row would replay the damaged block at 20 and fail.
    delete_row(&store, BlockHeight::new(50))?;
    break_parent_link(&store, BlockHeight::new(20))?;
    let secondary = open_secondary(&temporary, &activations)?;

    for height in [7_usize, 49, 50, 99] {
        assert_eq!(
            secondary
                .tree_state_checkpoint_at(BlockHeight::new(u32::try_from(height)?), &activations)?,
            reconstructed[height],
            "tree state at height {height}"
        );
    }
    Ok(())
}

/// Densified rows do not stand in for a missing cadence checkpoint.
///
/// Without the checkpoint at 100, a read there and a read above it that would
/// replay from below it are store damage, while the densified rows below it
/// still answer.
#[test]
fn a_densified_store_refuses_reads_that_need_a_missing_cadence_checkpoint() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let reconstructed = reconstructed_checkpoints(&store, &activations)?;
    densify_all(&store, &activations, EVERY_BLOCK)?;
    delete_row(&store, BlockHeight::new(100))?;

    for height in [100, 120] {
        assert!(
            matches!(
                store.tree_state_checkpoint_at(BlockHeight::new(height), &activations),
                Err(CanonicalStoreError::PublicationRefused { .. })
            ),
            "height {height} needs the cadence checkpoint"
        );
    }
    assert_eq!(
        store.tree_state_checkpoint_at(BlockHeight::new(99), &activations)?,
        reconstructed[99]
    );
    Ok(())
}

#[test]
fn a_block_that_breaks_the_parent_link_leaves_its_stride_unwritten() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    break_parent_link(&store, BlockHeight::new(20))?;

    let error = store
        .densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)
        .err()
        .ok_or("a broken parent link must refuse the stride")?;

    assert_stride_mismatch(&error, stride(0, 100), "does not extend the block below it");
    assert!(stored_row(&store, BlockHeight::new(10))?.is_none());
    Ok(())
}

#[test]
fn an_undecodable_compact_block_is_a_stride_failure_in_replay_and_in_a_budget_scan() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let family = column_family(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    store.bounded_open.db.put_cf(
        &family,
        encode_block_position(BlockHeight::new(99)),
        [0xff; 3],
    )?;
    let budget = TreeStateCheckpointSelection::CommitmentBudget(
        std::num::NonZeroU32::new(1).ok_or("budget must be nonzero")?,
    );

    let replay_error = store
        .densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)
        .err()
        .ok_or("an undecodable block must refuse the stride")?;
    let scan_error = store
        .next_tree_state_stride(BlockHeight::new(0), budget)
        .err()
        .ok_or("a budget scan must refuse a stride whose block does not decode")?;

    assert_stride_mismatch(&replay_error, stride(0, 100), "");
    assert_stride_mismatch(&scan_error, stride(0, 100), "");
    Ok(())
}

#[test]
fn a_seed_whose_root_does_not_match_is_refused_before_its_stride_replays() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    densify_all(&store, &activations, EVERY_BLOCK)?;
    let seed = BlockHeight::new(50);
    let mut row = stored_row(&store, seed)?.ok_or("checkpoint row is absent")?;
    // The Sapling root follows the block time and the presence bitmap.
    let root_byte = row
        .get_mut(5)
        .ok_or("checkpoint row holds no Sapling root")?;
    *root_byte ^= 0xff;
    replace_row(&store, seed, &row)?;

    let error = store
        .densify_tree_state_stride(stride(50, 100), EVERY_BLOCK, &activations)
        .err()
        .ok_or("a seed with a wrong root must refuse the stride")?;

    assert_stride_mismatch(
        &error,
        stride(50, 100),
        "frontier root does not match finalRoot",
    );
    Ok(())
}

#[test]
fn a_stride_that_does_not_reproduce_its_end_checkpoint_is_refused() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    // The block time leads the row, so flipping its low bit keeps a decodable
    // checkpoint with the same trees that no replay of the stored blocks
    // reproduces.
    flip_block_time_bit(&store, BlockHeight::new(100))?;

    let error = store
        .densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)
        .err()
        .ok_or("a stride whose end does not reproduce must be refused")?;

    assert_stride_mismatch(
        &error,
        stride(0, 100),
        "does not reproduce the checkpoint stored at height 100",
    );
    assert!(stored_row(&store, BlockHeight::new(50))?.is_none());
    Ok(())
}

#[test]
fn a_differing_stored_row_inside_the_stride_is_refused() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let densified = store.densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)?;
    let mut foreign = stored_row(&store, BlockHeight::new(100))?.ok_or("cadence row is absent")?;
    let first = foreign.first_mut().ok_or("cadence row is empty")?;
    *first ^= 0x01;
    replace_row(&store, BlockHeight::new(50), &foreign)?;

    let commit_error = store
        .commit_tree_state_stride(&densified)
        .err()
        .ok_or("the commit must refuse a differing stored row")?;
    let replay_error = store
        .densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)
        .err()
        .ok_or("the replay must refuse a differing stored row")?;

    assert_stride_mismatch(&commit_error, stride(0, 100), "stored at height 50 differs");
    assert_stride_mismatch(
        &replay_error,
        stride(0, 100),
        "does not reproduce the checkpoint stored at height 50",
    );
    assert!(stored_row(&store, BlockHeight::new(51))?.is_none());
    Ok(())
}

#[test]
fn a_commit_refuses_a_stride_whose_end_checkpoint_changed_after_its_replay() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let densified = store.densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)?;
    flip_block_time_bit(&store, BlockHeight::new(100))?;

    let error = store
        .commit_tree_state_stride(&densified)
        .err()
        .ok_or("a changed end checkpoint must refuse the commit")?;

    assert_stride_mismatch(&error, stride(0, 100), "bounding the stride changed");
    assert!(stored_row(&store, BlockHeight::new(50))?.is_none());
    Ok(())
}

#[test]
fn a_commit_refuses_a_stride_replayed_on_another_chain_with_the_same_bounding_rows() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let fork = TempDir::new()?;
    let fork_store = published_fork_store(
        &fork.path().join("canonical"),
        REORG_WINDOW,
        BlockHeight::new(TIP_HEIGHT),
        BlockHeight::new(TIP_HEIGHT),
        0x80,
    )?;
    let foreign =
        fork_store.densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)?;
    for height in [0, 100] {
        let height = BlockHeight::new(height);
        assert_eq!(
            stored_row(&store, height)?,
            stored_row(&fork_store, height)?
        );
    }
    let end = BlockHeight::new(100);
    assert_ne!(
        store.block_header_at(end)?.map(|header| header.block_hash),
        fork_store
            .block_header_at(end)?
            .map(|header| header.block_hash)
    );

    let error = store
        .commit_tree_state_stride(&foreign)
        .err()
        .ok_or("a stride replayed on another chain must be refused")?;

    assert_stride_mismatch(&error, stride(0, 100), "another chain");
    assert!(stored_row(&store, BlockHeight::new(50))?.is_none());
    Ok(())
}

#[test]
fn strides_above_the_settled_tip_are_neither_found_densified_nor_committed() -> TestResult {
    let settled = TempDir::new()?;
    let activations = activations()?;
    let densified = densification_store(&settled, BlockHeight::new(TIP_HEIGHT))?
        .densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)?;
    let unsettled = TempDir::new()?;
    let store = densification_store(&unsettled, BlockHeight::new(99))?;

    assert_eq!(
        store.next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?,
        None
    );
    let replay_error = store
        .densify_tree_state_stride(stride(0, 100), EVERY_BLOCK, &activations)
        .err()
        .ok_or("an unsettled stride must not be replayed")?;
    let commit_error = store
        .commit_tree_state_stride(&densified)
        .err()
        .ok_or("an unsettled stride must not be committed")?;
    assert_stride_mismatch(&replay_error, stride(0, 100), "above the settled tip");
    assert_stride_mismatch(&commit_error, stride(0, 100), "above the settled tip");
    assert!(stored_row(&store, BlockHeight::new(50))?.is_none());
    Ok(())
}

#[test]
fn a_commitment_budget_needs_no_rows_in_strides_without_commitments() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let selection = TreeStateCheckpointSelection::CommitmentBudget(
        std::num::NonZeroU32::new(1).ok_or("budget must be nonzero")?,
    );

    assert_eq!(
        store.next_tree_state_stride(BlockHeight::new(0), selection)?,
        None
    );
    let densified = store.densify_tree_state_stride(stride(0, 100), selection, &activations()?)?;
    assert_eq!(densified.row_count(), 0);
    assert_eq!(densified.replayed_commitment_count(), 0);
    Ok(())
}

#[test]
fn a_budget_stride_refuses_a_damaged_header_at_a_height_it_neither_compares_nor_writes()
-> TestResult {
    let damages: [(HeaderDamage, &str); 3] = [
        (HeaderDamage::Delete, "block header at height 50 is absent"),
        (
            HeaderDamage::OtherBlockHash,
            "block at height 50 is not the canonical block",
        ),
        (
            HeaderDamage::BlockTimeOutsideU32,
            "block time at height 50 is outside u32",
        ),
    ];
    let budget = TreeStateCheckpointSelection::CommitmentBudget(
        std::num::NonZeroU32::new(1).ok_or("budget must be nonzero")?,
    );
    for (damage, reason) in damages {
        let temporary = TempDir::new()?;
        let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
        let activations = activations()?;
        assert_eq!(
            store
                .densify_tree_state_stride(stride(0, 100), budget, &activations)?
                .row_count(),
            0,
            "a stride without commitments selects no height"
        );
        damage_header(&store, BlockHeight::new(50), damage)?;

        let error = store
            .densify_tree_state_stride(stride(0, 100), budget, &activations)
            .err()
            .ok_or("a damaged header inside the stride must refuse it")?;

        assert_stride_mismatch(&error, stride(0, 100), reason);
    }
    Ok(())
}

#[test]
fn a_stride_that_ends_at_the_settled_tip_is_found_replayed_committed_and_satisfied() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(100))?;
    let activations = activations()?;
    let one = std::num::NonZeroU32::new(1).ok_or("budget must be nonzero")?;

    assert_eq!(
        store.next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?,
        Some(stride(0, 100))
    );
    let scan = store.scan_tree_state_strides(BlockHeight::new(0), EVERY_BLOCK, one)?;
    assert_eq!(scan.incomplete, vec![stride(0, 100)]);
    assert_eq!(scan.examined, 1);
    assert!(!scan.reached_unsettled);
    assert!(!store.tree_state_stride_satisfied(BlockHeight::new(100), EVERY_BLOCK)?);

    assert_eq!(
        densify_all(&store, &activations, EVERY_BLOCK)?,
        vec![(stride(0, 100), 99)]
    );
    assert!(store.tree_state_stride_satisfied(BlockHeight::new(100), EVERY_BLOCK)?);
    Ok(())
}

#[test]
fn a_stride_above_the_settled_tip_is_unsatisfied_though_its_rows_need_no_checkpoint() -> TestResult
{
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(99))?;
    let one = std::num::NonZeroU32::new(1).ok_or("budget must be nonzero")?;
    let budget = TreeStateCheckpointSelection::CommitmentBudget(one);

    assert!(!store.tree_state_stride_satisfied(BlockHeight::new(100), budget)?);
    Ok(())
}

#[test]
fn a_stride_is_unsatisfied_when_a_row_is_missing_right_after_its_seed() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    densify_all(&store, &activations()?, EVERY_BLOCK)?;
    assert!(store.tree_state_stride_satisfied(BlockHeight::new(100), EVERY_BLOCK)?);

    delete_row(&store, BlockHeight::new(1))?;

    assert!(!store.tree_state_stride_satisfied(BlockHeight::new(100), EVERY_BLOCK)?);
    Ok(())
}

#[test]
fn a_search_from_the_middle_of_a_stride_starts_at_the_next_cadence_seed() -> TestResult {
    let temporary = TempDir::new()?;
    let store = long_densification_store(&temporary)?;

    for (from, expected) in [
        (0, stride(0, 100)),
        (50, stride(100, 200)),
        (100, stride(100, 200)),
    ] {
        assert_eq!(
            store.next_tree_state_stride(BlockHeight::new(from), EVERY_BLOCK)?,
            Some(expected),
            "search from height {from}"
        );
    }
    // The stride that starts at 200 ends above the settled tip.
    for from in [101, 150, 200] {
        assert_eq!(
            store.next_tree_state_stride(BlockHeight::new(from), EVERY_BLOCK)?,
            None,
            "search from height {from}"
        );
    }
    Ok(())
}

#[test]
fn a_scan_examines_every_settled_stride_its_read_budget_allows() -> TestResult {
    let temporary = TempDir::new()?;
    let store = long_densification_store(&temporary)?;
    let wide = std::num::NonZeroU32::new(1_000).ok_or("budget must be nonzero")?;

    let scan = store.scan_tree_state_strides(BlockHeight::new(0), EVERY_BLOCK, wide)?;

    assert_eq!(scan.incomplete, vec![stride(0, 100), stride(100, 200)]);
    assert_eq!(scan.examined, 2);
    assert_eq!(scan.reads, 2);
    assert_eq!(scan.next_from, BlockHeight::new(200));
    assert!(scan.reached_unsettled);

    let from_middle = store.scan_tree_state_strides(BlockHeight::new(50), EVERY_BLOCK, wide)?;
    assert_eq!(from_middle.incomplete, vec![stride(100, 200)]);
    assert_eq!(from_middle.examined, 1);
    Ok(())
}

#[test]
fn a_replayed_stride_counts_the_commitments_of_every_pool_it_appended() -> TestResult {
    let temporary = TempDir::new()?;
    let store = note_commitment_store(&temporary, None)?;
    let activations = activations()?;

    // Ironwood activates at height 10 and adds one commitment at each odd
    // height, so the first stride appends 45 and the second 50.
    for (seed, end, ironwood) in [(0, 100, 45), (100, 200, 50)] {
        let densified =
            store.densify_tree_state_stride(stride(seed, end), EVERY_BLOCK, &activations)?;

        let before = tree_sizes_after(&store, seed)?;
        let after = tree_sizes_after(&store, end)?;
        let appended = [
            after.sapling_commitment_tree_size - before.sapling_commitment_tree_size,
            after.orchard_commitment_tree_size - before.orchard_commitment_tree_size,
            after.ironwood_commitment_tree_size - before.ironwood_commitment_tree_size,
        ];
        assert!(
            appended.iter().all(|pool| *pool > 0),
            "the stride from {seed} appends to every pool: {appended:?}"
        );
        assert_eq!(appended[2], ironwood, "ironwood commitments from {seed}");
        assert_eq!(
            densified.replayed_commitment_count(),
            appended.iter().copied().map(u64::from).sum::<u64>(),
            "commitments replayed from {seed}"
        );
    }
    Ok(())
}

#[test]
fn a_history_predecessor_off_the_cadence_seeds_a_stride_that_ends_at_the_next_cadence_height()
-> TestResult {
    let temporary = TempDir::new()?;
    let store = note_commitment_store(&temporary, Some(BlockHeight::new(130)))?;
    let activations = activations()?;

    assert_eq!(
        store.next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?,
        Some(stride(130, 200))
    );
    let densified = store.densify_tree_state_stride(stride(130, 200), EVERY_BLOCK, &activations)?;
    let before = store.build_plan().history_predecessor().tip_metadata();
    let after = tree_sizes_after(&store, 200)?;

    assert_eq!(
        densified.replayed_commitment_count(),
        u64::from(after.sapling_commitment_tree_size - before.sapling_commitment_tree_size)
            + u64::from(after.orchard_commitment_tree_size - before.orchard_commitment_tree_size)
            + u64::from(after.ironwood_commitment_tree_size - before.ironwood_commitment_tree_size)
    );
    assert_eq!(store.commit_tree_state_stride(&densified)?.rows_written, 69);
    assert_eq!(
        store.next_tree_state_stride(BlockHeight::new(0), EVERY_BLOCK)?,
        None,
        "the next stride ends above the settled tip"
    );
    Ok(())
}

fn densification_store(
    temporary: &TempDir,
    settled_height: BlockHeight,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_store(
        &temporary.path().join("canonical"),
        REORG_WINDOW,
        BlockHeight::new(TIP_HEIGHT),
        settled_height,
    )
}

/// A store settled through 250.
///
/// The strides ending at 100 and 200 are settled and the one ending at 300 is
/// not. Test block hashes repeat one byte of the height, so 255 is the
/// tallest chain the fixture builds.
fn long_densification_store(
    temporary: &TempDir,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_store(
        &temporary.path().join("canonical"),
        REORG_WINDOW,
        BlockHeight::new(250),
        BlockHeight::new(250),
    )
}

/// A store settled through 250 whose blocks carry note commitments in every
/// pool, with the strides ending at 100 and 200 settled.
///
/// With a `history_floor`, the store retains only the blocks above it.
fn note_commitment_store(
    temporary: &TempDir,
    history_floor: Option<BlockHeight>,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_note_commitment_store(
        &temporary.path().join("canonical"),
        REORG_WINDOW,
        BlockHeight::new(250),
        BlockHeight::new(250),
        history_floor,
    )
}

/// The commitment-tree sizes after the block at `height`, empty at genesis.
fn tree_sizes_after(
    store: &RocksDbCanonicalStore,
    height: u32,
) -> Result<zinder_core::CompactChainMetadata, Box<dyn std::error::Error>> {
    if height == 0 {
        return Ok(zinder_core::CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        });
    }
    Ok(store
        .compact_block_at(BlockHeight::new(height))?
        .ok_or("compact block is absent")?
        .chain_metadata())
}

fn activations() -> Result<NetworkUpgradeActivations, Box<dyn std::error::Error>> {
    Ok(test_network_upgrade_activations(Network::ZcashTestnet)?)
}

const fn stride(seed: u32, end: u32) -> TreeStateStride {
    TreeStateStride {
        seed_height: BlockHeight::new(seed),
        end_height: BlockHeight::new(end),
    }
}

/// Densifies and commits every settled stride, returning each with its
/// written row count.
fn densify_all(
    store: &RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
    selection: TreeStateCheckpointSelection,
) -> Result<Vec<(TreeStateStride, u32)>, Box<dyn std::error::Error>> {
    let mut committed = Vec::new();
    let mut from = BlockHeight::new(0);
    while let Some(stride) = store.next_tree_state_stride(from, selection)? {
        let densified = store.densify_tree_state_stride(stride, selection, activations)?;
        committed.push((
            stride,
            store.commit_tree_state_stride(&densified)?.rows_written,
        ));
        from = stride.end_height();
    }
    Ok(committed)
}

fn reconstructed_checkpoints(
    store: &RocksDbCanonicalStore,
    activations: &NetworkUpgradeActivations,
) -> Result<Vec<Option<CommitmentTreeCheckpoint>>, Box<dyn std::error::Error>> {
    (0..=TIP_HEIGHT)
        .map(|height| Ok(store.tree_state_checkpoint_at(BlockHeight::new(height), activations)?))
        .collect()
}

fn reopen_primary(
    temporary: &TempDir,
    activations: &NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    Ok(RocksDbCanonicalStore::open_ready(
        temporary.path().join("canonical"),
        activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

fn open_secondary(
    temporary: &TempDir,
    activations: &NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalSecondary, Box<dyn std::error::Error>> {
    Ok(RocksDbCanonicalSecondary::open_ready(
        temporary.path().join("canonical"),
        temporary.path().join("canonical-secondary"),
        activations,
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::Transactions),
        CanonicalReorgPolicy::new(REORG_WINDOW)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

fn stored_row(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    Ok(store
        .bounded_open
        .db
        .get_cf(&family, encode_block_position(height))?)
}

fn replace_row(store: &RocksDbCanonicalStore, height: BlockHeight, row: &[u8]) -> TestResult {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .put_cf(&family, encode_block_position(height), row)?;
    Ok(())
}

fn flip_block_time_bit(store: &RocksDbCanonicalStore, height: BlockHeight) -> TestResult {
    let mut row = stored_row(store, height)?.ok_or("checkpoint row is absent")?;
    let first = row.first_mut().ok_or("checkpoint row is empty")?;
    *first ^= 0x01;
    replace_row(store, height, &row)
}

fn delete_row(store: &RocksDbCanonicalStore, height: BlockHeight) -> TestResult {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .delete_cf(&family, encode_block_position(height))?;
    Ok(())
}

#[derive(Clone, Copy)]
enum HeaderDamage {
    Delete,
    OtherBlockHash,
    BlockTimeOutsideU32,
}

fn damage_header(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
    damage: HeaderDamage,
) -> TestResult {
    let db = &store.bounded_open.db;
    let family = column_family(db, BLOCK_HEADER_COLUMN_FAMILY)?;
    let key = encode_block_position(height);
    let mut header = db.get_cf(&family, key)?.ok_or("block header is absent")?;
    match damage {
        HeaderDamage::Delete => {
            db.delete_cf(&family, key)?;
            return Ok(());
        }
        HeaderDamage::OtherBlockHash => {
            *header.first_mut().ok_or("block header is empty")? ^= 0x01;
        }
        HeaderDamage::BlockTimeOutsideU32 => header
            .get_mut(128..136)
            .ok_or("block header is shorter than its block time")?
            .copy_from_slice(&(i64::from(u32::MAX) + 1).to_le_bytes()),
    }
    db.put_cf(&family, key, header)?;
    Ok(())
}

/// Rewrites the compact block at `height` so it no longer extends its parent.
fn break_parent_link(store: &RocksDbCanonicalStore, height: BlockHeight) -> TestResult {
    let original = store
        .compact_block_at(height)?
        .ok_or("compact block is absent")?;
    let damaged = CompactBlockArtifact::new(
        zinder_core::BlockId::new(height, original.block_hash()),
        BlockHash::from_bytes([0xee; 32]),
        original.time(),
        original.transactions().to_vec(),
        original.chain_metadata(),
    )?;
    let family = column_family(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    store.bounded_open.db.put_cf(
        &family,
        encode_block_position(height),
        crate::encode_compact_block_artifact(&damaged),
    )?;
    Ok(())
}

fn assert_stride_mismatch(error: &CanonicalStoreError, expected: TreeStateStride, reason: &str) {
    assert!(
        matches!(
            error,
            CanonicalStoreError::TreeStateStrideMismatch {
                seed_height,
                end_height,
                reason: actual,
            } if *seed_height == expected.seed_height()
                && *end_height == expected.end_height()
                && actual.contains(reason)
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn a_stride_reports_satisfied_only_when_its_stored_rows_satisfy_the_selection() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let activations = activations()?;
    let end = BlockHeight::new(100);
    assert!(!store.tree_state_stride_satisfied(end, EVERY_BLOCK)?);

    densify_all(&store, &activations, EVERY_BLOCK)?;
    assert!(store.tree_state_stride_satisfied(end, EVERY_BLOCK)?);
    assert!(
        open_secondary(&temporary, &activations)?.tree_state_stride_satisfied(end, EVERY_BLOCK)?
    );
    // Above the settled tip nothing is satisfied, and only cadence heights
    // end a stride.
    assert!(!store.tree_state_stride_satisfied(BlockHeight::new(200), EVERY_BLOCK)?);
    let not_an_end = store
        .tree_state_stride_satisfied(BlockHeight::new(150), EVERY_BLOCK)
        .err()
        .ok_or("a height off the cadence must be refused")?;
    assert!(matches!(
        not_an_end,
        CanonicalStoreError::PublicationRefused { .. }
    ));

    delete_row(&store, BlockHeight::new(99))?;
    assert!(!store.tree_state_stride_satisfied(end, EVERY_BLOCK)?);
    Ok(())
}

#[test]
fn a_bounded_scan_stops_after_the_stride_that_reaches_its_read_budget_and_reports_where_to_continue()
-> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    let one = std::num::NonZeroU32::new(1).ok_or("budget must be nonzero")?;

    let first = store.scan_tree_state_strides(BlockHeight::new(0), EVERY_BLOCK, one)?;
    assert_eq!(first.incomplete, vec![stride(0, 100)]);
    assert_eq!(first.examined, 1);
    assert_eq!(first.reads, 1);
    assert_eq!(first.next_from, BlockHeight::new(100));
    assert!(!first.reached_unsettled);

    // A densified stride costs a read for every checkpoint row it holds.
    let densified_store = TempDir::new()?;
    let densified = densification_store(&densified_store, BlockHeight::new(TIP_HEIGHT))?;
    densify_all(&densified, &activations()?, EVERY_BLOCK)?;
    let dense = densified.scan_tree_state_strides(BlockHeight::new(0), EVERY_BLOCK, one)?;
    assert!(dense.incomplete.is_empty() && dense.damaged.is_empty());
    assert_eq!(dense.examined, 1);
    assert_eq!(dense.reads, 100);

    let second = store.scan_tree_state_strides(first.next_from, EVERY_BLOCK, one)?;
    assert_eq!(second.incomplete, []);
    assert_eq!(second.examined, 0);
    assert_eq!(second.next_from, BlockHeight::new(100));
    assert!(second.reached_unsettled);

    break_parent_link(&store, BlockHeight::new(20))?;
    let family = column_family(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    store.bounded_open.db.put_cf(
        &family,
        encode_block_position(BlockHeight::new(99)),
        [0xff; 3],
    )?;
    let budget = TreeStateCheckpointSelection::CommitmentBudget(one);
    let damaged = store.scan_tree_state_strides(BlockHeight::new(0), budget, one)?;
    assert_eq!(damaged.incomplete, []);
    assert_eq!(
        damaged
            .damaged
            .iter()
            .map(|(stride, _)| *stride)
            .collect::<Vec<_>>(),
        vec![stride(0, 100)]
    );
    assert_eq!(damaged.next_from, BlockHeight::new(100));
    Ok(())
}

#[test]
fn the_costliest_stride_reads_the_most_a_scan_charges_for_one_stride() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densification_store(&temporary, BlockHeight::new(TIP_HEIGHT))?;
    densify_all(&store, &activations()?, EVERY_BLOCK)?;
    for height in (1..100).step_by(2) {
        delete_row(&store, BlockHeight::new(height))?;
    }
    let one = std::num::NonZeroU32::new(1).ok_or("budget must be nonzero")?;
    let budget = TreeStateCheckpointSelection::CommitmentBudget(one);

    let scan = store.scan_tree_state_strides(BlockHeight::new(0), budget, one)?;

    assert!(scan.incomplete.is_empty() && scan.damaged.is_empty());
    assert_eq!(scan.examined, 1);
    assert_eq!(scan.reads, super::MAX_TREE_STATE_STRIDE_SCAN_READS);
    Ok(())
}
