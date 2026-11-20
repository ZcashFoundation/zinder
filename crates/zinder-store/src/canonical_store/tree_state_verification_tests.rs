//! Verifying the stored checkpoints of a published store.
//!
//! The test store settles at height 120 below its tip at 150 and holds a
//! checkpoint at every height through 100 once densified, so the settled rows
//! are heights 0 through 100. Sapling activates at 2, Orchard at 6, and
//! Ironwood at 10, and every tree stays empty. Each test changes the store
//! through its primary and verifies it through a [`RocksDbTreeStateVerifier`]
//! opened on that primary.

use std::num::NonZeroU32;

use tempfile::TempDir;
use zinder_core::{
    BlockHeight, BlockHeightRange, CommitmentTreeFrontier, CommitmentTreeFrontiers, Network,
    NetworkUpgradeActivations, ShieldedProtocol,
};

use super::{
    CanonicalReorgPolicy, CanonicalStoreWorkload, RocksDbCanonicalStore, RocksDbTreeStateVerifier,
    TreeStateCheckpointMismatch, TreeStateCheckpointMismatchKind,
    TreeStateCheckpointRowVerification, TreeStateCheckpointSelection,
    TreeStateCheckpointVerification,
    block_load::{encode_block_position, encode_tree_state_checkpoint},
    live_replacement_tests::published_store,
    publication::column_family,
    rocksdb::TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    secondary_tests::{compact_away_rows_in, split_rows_into_bottom_level_files, table_file_path},
    test_network_upgrade_activations,
};
use crate::{RawBlobRetention, RocksDbResourceBudget};

const TIP_HEIGHT: u32 = 150;
const SETTLED_HEIGHT: u32 = 120;
const SETTLED_ROWS: u32 = 101;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn every_settled_row_of_an_intact_store_verifies() -> TestResult {
    let temporary = TempDir::new()?;
    let _store = densified_store(&temporary)?;

    let verification = verify_all(&temporary)?;

    assert_eq!(verification.mismatch, None);
    assert_eq!(verification.verified_rows, SETTLED_ROWS);
    assert!(verification.reached_settled_tip);
    assert_eq!(verification.next_from, BlockHeight::new(101));
    Ok(())
}

#[test]
fn a_bounded_verification_reports_where_the_next_one_continues() -> TestResult {
    let temporary = TempDir::new()?;
    let _store = densified_store(&temporary)?;
    let verifier = open_verifier(&temporary)?;
    let activations = activations()?;

    let first =
        verifier.verify_tree_state_checkpoints(BlockHeight::new(0), rows(40)?, &activations)?;
    let second =
        verifier.verify_tree_state_checkpoints(first.next_from, rows(40)?, &activations)?;
    let third =
        verifier.verify_tree_state_checkpoints(second.next_from, rows(40)?, &activations)?;

    assert_eq!(
        (
            first.verified_rows,
            first.next_from,
            first.reached_settled_tip
        ),
        (40, BlockHeight::new(40), false)
    );
    assert_eq!(
        (
            second.verified_rows,
            second.next_from,
            second.reached_settled_tip
        ),
        (40, BlockHeight::new(80), false)
    );
    assert_eq!(
        (third.verified_rows, third.reached_settled_tip),
        (SETTLED_ROWS - 80, true)
    );
    Ok(())
}

#[test]
fn a_row_whose_frontier_does_not_derive_its_stored_root_is_a_mismatch_of_that_pool() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let height = BlockHeight::new(50);
    let mut row = stored_row(&store, height)?;
    // The Sapling root follows the block time and the presence bitmap.
    let root_byte = row
        .get_mut(5)
        .ok_or("checkpoint row holds no Sapling root")?;
    *root_byte ^= 0xff;
    replace_row(&store, height, &row)?;

    let verification = verify_all(&temporary)?;

    let mismatch = verification
        .mismatch
        .ok_or("a row with a wrong root must fail")?;
    assert_eq!(
        (mismatch.height, mismatch.kind, mismatch.pool),
        (
            height,
            TreeStateCheckpointMismatchKind::Row,
            Some(ShieldedProtocol::Sapling)
        )
    );
    assert!(
        mismatch.reason.contains("root does not match"),
        "{}",
        mismatch.reason
    );
    assert_eq!(verification.verified_rows, 50);
    assert_eq!(verification.next_from, height);
    assert!(!verification.reached_settled_tip);
    // Serving checks only the framing, so it reads the row as stored.
    assert!(store.tree_state_checkpoint_at_or_before(height)?.is_some());
    Ok(())
}

#[test]
fn a_row_whose_frontiers_differ_from_the_active_pools_is_a_mismatch_of_that_pool() -> TestResult {
    let empty = |protocol| Some(CommitmentTreeFrontier::empty(protocol));
    for (height, frontiers, pool, reason) in [
        (
            100,
            CommitmentTreeFrontiers::from_validated_parts(
                empty(ShieldedProtocol::Sapling),
                empty(ShieldedProtocol::Orchard),
                None,
            )?,
            ShieldedProtocol::Ironwood,
            "the checkpoint has no frontier for the Ironwood pool although it is active",
        ),
        (
            5,
            CommitmentTreeFrontiers::from_validated_parts(
                empty(ShieldedProtocol::Sapling),
                empty(ShieldedProtocol::Orchard),
                None,
            )?,
            ShieldedProtocol::Orchard,
            "the checkpoint carries a frontier for the Orchard pool before it activates",
        ),
    ] {
        let temporary = TempDir::new()?;
        let store = densified_store(&temporary)?;
        let height = BlockHeight::new(height);
        replace_row(
            &store,
            height,
            &encode_tree_state_checkpoint(height.value(), &frontiers),
        )?;

        let verification = verify_all(&temporary)?;

        assert_eq!(
            verification.mismatch,
            Some(TreeStateCheckpointMismatch {
                height,
                kind: TreeStateCheckpointMismatchKind::Row,
                pool: Some(pool),
                reason: reason.to_owned(),
            })
        );
    }
    Ok(())
}

#[test]
fn a_row_that_fails_outside_any_frontier_is_a_mismatch_without_a_pool() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let height = BlockHeight::new(7);
    replace_row(&store, height, &[0, 0])?;

    let verification = verify_all(&temporary)?;

    let mismatch = verification.mismatch.ok_or("a truncated row must fail")?;
    assert_eq!((mismatch.height, mismatch.pool), (height, None));
    Ok(())
}

/// One row is verified by its exact height, which is how a restart checks a
/// row it recorded as damaged.
///
/// A missing row is reported apart from an intact one, so a row that
/// disappeared is never mistaken for one that was repaired.
#[test]
fn a_row_verified_at_its_height_is_intact_absent_or_a_mismatch() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let damaged = BlockHeight::new(7);
    let removed = BlockHeight::new(60);
    replace_row(&store, damaged, &[0, 0])?;
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .delete_cf(&family, encode_block_position(removed))?;
    let verifier = open_verifier(&temporary)?;
    let activations = activations()?;

    assert_eq!(
        verifier.verify_tree_state_checkpoint_at(BlockHeight::new(50), &activations)?,
        TreeStateCheckpointRowVerification::Intact
    );
    assert_eq!(
        verifier.verify_tree_state_checkpoint_at(removed, &activations)?,
        TreeStateCheckpointRowVerification::Absent
    );
    let TreeStateCheckpointRowVerification::Mismatch(mismatch) =
        verifier.verify_tree_state_checkpoint_at(damaged, &activations)?
    else {
        return Err("a truncated row must fail at its height".into());
    };
    assert_eq!(
        (mismatch.height, mismatch.kind, mismatch.pool),
        (damaged, TreeStateCheckpointMismatchKind::Row, None)
    );
    Ok(())
}

/// A key that extends a height key is found again from its reported height.
///
/// Such a key sorts between two heights, so a restart that resumes at the
/// reported height finds the damage instead of skipping it.
#[test]
fn a_key_extending_a_height_key_is_found_again_from_its_reported_height() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    put_extended_key(&store, BlockHeight::new(40))?;
    let verifier = open_verifier(&temporary)?;
    let activations = activations()?;

    let first =
        verifier.verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations)?;
    let mismatch = first
        .mismatch
        .ok_or("a key that is not a height must fail")?;
    assert_eq!(
        (mismatch.height, mismatch.kind, mismatch.pool),
        (
            BlockHeight::new(41),
            TreeStateCheckpointMismatchKind::UnreadableEntry,
            None
        )
    );
    assert_eq!(first.verified_rows, 41);

    let again =
        verifier.verify_tree_state_checkpoints(mismatch.height, rows(1_000)?, &activations)?;
    assert_eq!(
        again.mismatch.map(|mismatch| mismatch.height),
        Some(BlockHeight::new(41))
    );
    assert_eq!(again.verified_rows, 0);
    Ok(())
}

/// A table block that fails its checksum is damage, reported like a row that
/// fails a check, even when an earlier pass read the same block intact.
#[test]
fn a_corrupt_table_block_is_a_mismatch_even_after_an_intact_pass() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    split_rows_into_bottom_level_files(
        &store.bounded_open.db,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    )?;
    let verifier = open_verifier(&temporary)?;
    let activations = activations()?;
    let intact =
        verifier.verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations)?;
    assert_eq!(intact.mismatch, None);

    let corrupted_height = BlockHeight::new(50);
    corrupt_table_file_holding(&store, corrupted_height)?;
    let verification =
        verifier.verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations)?;

    let mismatch = verification
        .mismatch
        .ok_or("a corrupt table block must fail verification")?;
    assert_eq!(
        (mismatch.height, mismatch.kind, mismatch.pool),
        (
            corrupted_height,
            TreeStateCheckpointMismatchKind::UnreadableEntry,
            None
        )
    );
    assert!(mismatch.reason.contains("corrupt"), "{}", mismatch.reason);
    assert_eq!(verification.verified_rows, 50);
    assert_eq!(verification.next_from, corrupted_height);
    Ok(())
}

/// A block that a serving read loaded does not hide damage done to its file
/// since.
///
/// The verifier's checkpoint family has no block cache, so verification
/// reads the block from the file again, whatever read the row first.
#[test]
fn a_block_a_normal_read_loaded_does_not_mask_corruption_on_disk() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    split_rows_into_bottom_level_files(
        &store.bounded_open.db,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    )?;
    let verifier = open_verifier(&temporary)?;
    let activations = activations()?;
    let corrupted_height = BlockHeight::new(50);
    assert!(
        verifier
            .secondary
            .tree_state_checkpoint_at_or_before(corrupted_height)?
            .is_some()
    );

    corrupt_table_file_holding(&store, corrupted_height)?;

    let verification =
        verifier.verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations)?;
    let mismatch = verification
        .mismatch
        .ok_or("a corrupt block must fail although a read loaded it intact")?;
    assert_eq!(mismatch.height, corrupted_height);
    assert!(mismatch.reason.contains("corrupt"), "{}", mismatch.reason);
    let TreeStateCheckpointRowVerification::Mismatch(at_height) =
        verifier.verify_tree_state_checkpoint_at(corrupted_height, &activations)?
    else {
        return Err("the corrupt row must fail at its height".into());
    };
    assert_eq!(
        (at_height.kind, at_height.pool),
        (TreeStateCheckpointMismatchKind::Row, None)
    );
    Ok(())
}

/// A table file the primary deleted under a secondary is not damage.
///
/// The verification stops with an error that keeps the rows it already
/// verified, and continues from there once the secondary catches up.
#[test]
fn a_compacted_away_table_file_stops_verification_with_its_progress() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let db = &store.bounded_open.db;
    split_rows_into_bottom_level_files(db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    let activations = activations()?;
    let mut verifier = open_verifier(&temporary)?;
    let family = column_family(db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    for height in 60..=100 {
        let height = BlockHeight::new(height);
        db.put_cf(
            &family,
            encode_block_position(height),
            stored_row(&store, height)?,
        )?;
    }
    db.flush_cf(&family)?;
    compact_away_rows_in(
        db,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
        BlockHeightRange::inclusive(BlockHeight::new(60), BlockHeight::new(100)),
    )?;

    let interrupted = verifier
        .verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations)
        .err()
        .ok_or("a deleted table file must stop verification")?;
    assert!(
        interrupted.source.is_missing_table_file(),
        "{:?}",
        interrupted.source
    );
    assert_eq!(
        (interrupted.verified_rows, interrupted.next_from),
        (60, BlockHeight::new(60))
    );

    verifier.try_catch_up()?;
    let resumed = verifier.verify_tree_state_checkpoints(
        interrupted.next_from,
        rows(1_000)?,
        &activations,
    )?;
    assert_eq!(resumed.mismatch, None);
    assert_eq!(resumed.verified_rows, SETTLED_ROWS - 60);
    assert!(resumed.reached_settled_tip);
    Ok(())
}

/// Nothing above the settled tip is read, so damage there does not stop
/// verification before the tip passes it.
///
/// That covers a damaged row at the build tip and keys that are not heights
/// just above the settled tip: one that extends the settled tip's key, whose
/// damage starts at the height above it, and one that extends the key of
/// that height.
#[test]
fn entries_above_the_settled_tip_are_not_verified() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    replace_row(&store, BlockHeight::new(TIP_HEIGHT), &[0, 0])?;
    put_extended_key(&store, BlockHeight::new(SETTLED_HEIGHT))?;
    put_extended_key(&store, BlockHeight::new(SETTLED_HEIGHT + 1))?;
    let verifier = open_verifier(&temporary)?;
    let activations = activations()?;

    let from_the_settled_tip = verifier.verify_tree_state_checkpoints(
        BlockHeight::new(SETTLED_HEIGHT),
        rows(1_000)?,
        &activations,
    )?;
    let whole_cycle =
        verifier.verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations)?;

    assert_eq!(from_the_settled_tip.mismatch, None);
    assert_eq!(from_the_settled_tip.verified_rows, 0);
    assert!(from_the_settled_tip.reached_settled_tip);
    assert_eq!(whole_cycle.mismatch, None);
    assert_eq!(whole_cycle.verified_rows, SETTLED_ROWS);
    assert!(whole_cycle.reached_settled_tip);
    Ok(())
}

#[test]
fn verification_refuses_an_activation_table_the_store_does_not_admit() -> TestResult {
    let temporary = TempDir::new()?;
    let _store = densified_store(&temporary)?;
    let verifier = open_verifier(&temporary)?;
    let mut entries = activations()?.activations().to_vec();
    for activation in &mut entries {
        if activation.name == "NU6.3" {
            activation.activation_height = BlockHeight::new(20);
        }
    }
    let other = NetworkUpgradeActivations::new(Network::ZcashTestnet, entries)?;

    let error = verifier
        .verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &other)
        .err()
        .ok_or("another activation table must be refused")?;
    let at_height = verifier
        .verify_tree_state_checkpoint_at(BlockHeight::new(50), &other)
        .err()
        .ok_or("another activation table must be refused at one height")?;

    assert!(
        error.to_string().contains("activation table differs"),
        "{error}"
    );
    assert!(
        at_height.to_string().contains("activation table differs"),
        "{at_height}"
    );
    Ok(())
}

/// A published store with a stored checkpoint at every settled height.
fn densified_store(
    temporary: &TempDir,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let store = published_store(
        &temporary.path().join("canonical"),
        100,
        BlockHeight::new(TIP_HEIGHT),
        BlockHeight::new(SETTLED_HEIGHT),
    )?;
    let activations = activations()?;
    let selection = TreeStateCheckpointSelection::EveryBlock;
    while let Some(stride) = store.next_tree_state_stride(BlockHeight::new(0), selection)? {
        let densified = store.densify_tree_state_stride(stride, selection, &activations)?;
        store.commit_tree_state_stride(&densified)?;
    }
    Ok(store)
}

/// Opens a verifier on the store `densified_store` published in `temporary`.
fn open_verifier(
    temporary: &TempDir,
) -> Result<RocksDbTreeStateVerifier, Box<dyn std::error::Error>> {
    Ok(RocksDbTreeStateVerifier::open_ready(
        temporary.path().join("canonical"),
        temporary.path().join("verifier"),
        &activations()?,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

/// Verifies every settled row of the store in `temporary` at once.
fn verify_all(
    temporary: &TempDir,
) -> Result<TreeStateCheckpointVerification, Box<dyn std::error::Error>> {
    Ok(open_verifier(temporary)?.verify_tree_state_checkpoints(
        BlockHeight::new(0),
        rows(1_000)?,
        &activations()?,
    )?)
}

fn activations() -> Result<NetworkUpgradeActivations, Box<dyn std::error::Error>> {
    Ok(test_network_upgrade_activations(Network::ZcashTestnet)?)
}

fn rows(count: u32) -> Result<NonZeroU32, Box<dyn std::error::Error>> {
    Ok(NonZeroU32::new(count).ok_or("a verification checks at least one row")?)
}

fn stored_row(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    Ok(store
        .bounded_open
        .db
        .get_cf(&family, encode_block_position(height))?
        .ok_or("checkpoint row is absent")?)
}

fn replace_row(store: &RocksDbCanonicalStore, height: BlockHeight, row: &[u8]) -> TestResult {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .put_cf(&family, encode_block_position(height), row)?;
    Ok(())
}

/// Stores a row under the key of `height` extended by one byte, a key no
/// writer stores, which sorts after that height's key and before the next.
fn put_extended_key(store: &RocksDbCanonicalStore, height: BlockHeight) -> TestResult {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    let mut extended_key = encode_block_position(height).to_vec();
    extended_key.push(0);
    store
        .bounded_open
        .db
        .put_cf(&family, extended_key, [0_u8, 0])?;
    Ok(())
}

/// Flips the first byte of the table file that holds only the row at
/// `height`, which breaks its first data block's checksum without touching
/// the footer or index.
fn corrupt_table_file_holding(store: &RocksDbCanonicalStore, height: BlockHeight) -> TestResult {
    let key = encode_block_position(height);
    let file = store
        .bounded_open
        .db
        .live_files()?
        .into_iter()
        .find(|file| {
            file.column_family_name == TREE_STATE_CHECKPOINT_COLUMN_FAMILY
                && file.start_key.as_deref() == Some(key.as_slice())
                && file.end_key.as_deref() == Some(key.as_slice())
        })
        .map(|file| table_file_path(&file))
        .ok_or("the row must have a table file of its own")?;
    let mut table_bytes = std::fs::read(&file)?;
    let first_byte = table_bytes
        .first_mut()
        .ok_or("a table file is never empty")?;
    *first_byte ^= 0xff;
    std::fs::write(&file, table_bytes)?;
    Ok(())
}
