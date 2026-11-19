//! Verifying the stored checkpoints of a published store.
//!
//! The test store settles at height 120 below its tip at 150 and holds a
//! checkpoint at every height through 100 once densified, so the settled rows
//! are heights 0 through 100. Sapling activates at 2, Orchard at 6, and
//! Ironwood at 10, and every tree stays empty.

use std::num::NonZeroU32;

use tempfile::TempDir;
use zinder_core::{
    BlockHeight, CommitmentTreeFrontier, CommitmentTreeFrontiers, Network,
    NetworkUpgradeActivations, ShieldedProtocol,
};

use super::{
    RocksDbCanonicalStore, TreeStateCheckpointMismatch, TreeStateCheckpointSelection,
    block_load::{encode_block_position, encode_tree_state_checkpoint},
    live_replacement_tests::published_store,
    publication::column_family,
    rocksdb::TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    test_network_upgrade_activations,
};

const TIP_HEIGHT: u32 = 150;
const SETTLED_HEIGHT: u32 = 120;
const SETTLED_ROWS: u32 = 101;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn every_settled_row_of_an_intact_store_verifies() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;

    let verification =
        store.verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations()?)?;

    assert_eq!(verification.mismatch, None);
    assert_eq!(verification.verified_rows, SETTLED_ROWS);
    assert!(verification.reached_settled_tip);
    assert_eq!(verification.next_from, BlockHeight::new(101));
    Ok(())
}

#[test]
fn a_bounded_verification_reports_where_the_next_one_continues() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let activations = activations()?;

    let first =
        store.verify_tree_state_checkpoints(BlockHeight::new(0), rows(40)?, &activations)?;
    let second = store.verify_tree_state_checkpoints(first.next_from, rows(40)?, &activations)?;
    let third = store.verify_tree_state_checkpoints(second.next_from, rows(40)?, &activations)?;

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

    let verification =
        store.verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations()?)?;

    let mismatch = verification
        .mismatch
        .ok_or("a row with a wrong root must fail")?;
    assert_eq!(
        (mismatch.height, mismatch.pool),
        (height, Some(ShieldedProtocol::Sapling))
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

        let verification = store.verify_tree_state_checkpoints(
            BlockHeight::new(0),
            rows(1_000)?,
            &activations()?,
        )?;

        assert_eq!(
            verification.mismatch,
            Some(TreeStateCheckpointMismatch {
                height,
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

    let verification =
        store.verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &activations()?)?;

    let mismatch = verification.mismatch.ok_or("a truncated row must fail")?;
    assert_eq!((mismatch.height, mismatch.pool), (height, None));
    Ok(())
}

#[test]
fn rows_above_the_settled_tip_are_not_verified() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    replace_row(&store, BlockHeight::new(TIP_HEIGHT), &[0, 0])?;

    let verification = store.verify_tree_state_checkpoints(
        BlockHeight::new(SETTLED_HEIGHT),
        rows(1_000)?,
        &activations()?,
    )?;

    assert_eq!(verification.mismatch, None);
    assert_eq!(verification.verified_rows, 0);
    assert!(verification.reached_settled_tip);
    Ok(())
}

#[test]
fn verification_refuses_an_activation_table_the_store_does_not_admit() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let mut entries = activations()?.activations().to_vec();
    for activation in &mut entries {
        if activation.name == "NU6.3" {
            activation.activation_height = BlockHeight::new(20);
        }
    }
    let other = NetworkUpgradeActivations::new(Network::ZcashTestnet, entries)?;

    let error = store
        .verify_tree_state_checkpoints(BlockHeight::new(0), rows(1_000)?, &other)
        .err()
        .ok_or("another activation table must be refused")?;

    assert!(
        error.to_string().contains("activation table differs"),
        "{error}"
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
