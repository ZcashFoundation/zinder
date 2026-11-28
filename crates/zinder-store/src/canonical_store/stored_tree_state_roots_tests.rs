//! Batched stored final note-commitment root reads from `tree_state_checkpoint`.
//!
//! The published test store settles at height 148 below its tip at 150, with
//! Sapling activating at 2, Orchard at 6, and Ironwood at 10. Before
//! densification only heights 0, 100, and 150 carry stored checkpoints; after
//! densification every height from 0 through 100 carries one.

use tempfile::TempDir;
use zinder_core::{
    BlockFinalNoteCommitmentRoots, BlockHash, BlockHeight, BlockHeightRange,
    CommitmentTreeFrontier, CommitmentTreeFrontiers, FinalNoteCommitmentRoot, Network,
    NetworkUpgradeActivation, NetworkUpgradeActivations, ShieldedProtocol,
};

use super::{
    CanonicalReorgPolicy, CanonicalStoreError, RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    TreeStateCheckpointSelection,
    block_load::{encode_block_position, encode_tree_state_checkpoint},
    live_replacement_tests::published_store,
    publication::column_family,
    rocksdb::{BLOCK_HEADER_COLUMN_FAMILY, TREE_STATE_CHECKPOINT_COLUMN_FAMILY},
    test_checkpoint_frontiers, test_network_upgrade_activations,
};
use crate::{RawBlobRetention, RocksDbResourceBudget};

const TIP_HEIGHT: u32 = 150;
const SETTLED_HEIGHT: u32 = TIP_HEIGHT - 2;
const DENSIFIED_END: u32 = 100;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn full_densified_range_returns_exact_roots_across_every_pool_activation() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let secondary = open_secondary(&temporary)?;
    let activations = activations()?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(DENSIFIED_END));

    let primary_roots = store.stored_final_note_commitment_roots_in_range(range, &activations)?;
    let secondary_roots =
        secondary.stored_final_note_commitment_roots_in_range(range, &activations)?;
    assert_eq!(primary_roots, secondary_roots);
    assert_eq!(primary_roots.len(), usize::try_from(DENSIFIED_END)?);

    for (offset, actual) in primary_roots.into_iter().enumerate() {
        let height = BlockHeight::new(1 + u32::try_from(offset)?);
        let expected = expected_roots(&activations, height)?;
        assert_eq!(
            actual,
            Some(expected),
            "stored roots at height {} must match activation transitions",
            height.value()
        );
    }

    // Verify activation boundaries explicitly: pre-Sapling (1), Sapling (2),
    // Orchard (6), and Ironwood (10).
    let sample = secondary.stored_final_note_commitment_roots_in_range(
        BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(10)),
        &activations,
    )?;
    let roots_at = |height: usize| sample[height - 1];
    assert_eq!(
        roots_at(1).map(|roots| (
            roots.sapling.is_some(),
            roots.orchard.is_some(),
            roots.ironwood.is_some()
        )),
        Some((false, false, false))
    );
    assert_eq!(
        roots_at(2).map(|roots| (
            roots.sapling.is_some(),
            roots.orchard.is_some(),
            roots.ironwood.is_some()
        )),
        Some((true, false, false))
    );
    assert_eq!(
        roots_at(6).map(|roots| (
            roots.sapling.is_some(),
            roots.orchard.is_some(),
            roots.ironwood.is_some()
        )),
        Some((true, true, false))
    );
    assert_eq!(
        roots_at(10).map(|roots| (
            roots.sapling.is_some(),
            roots.orchard.is_some(),
            roots.ironwood.is_some()
        )),
        Some((true, true, true))
    );
    Ok(())
}

#[test]
fn sparse_range_returns_none_for_missing_checkpoints_and_some_for_stored_ones() -> TestResult {
    let temporary = TempDir::new()?;
    let store = sparse_store(&temporary)?;
    let activations = activations()?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(TIP_HEIGHT));

    let roots = store.stored_final_note_commitment_roots_in_range(range, &activations)?;
    assert_eq!(roots.len(), usize::try_from(TIP_HEIGHT)?);

    for (offset, actual) in roots.into_iter().enumerate() {
        let height = BlockHeight::new(1 + u32::try_from(offset)?);
        if height == BlockHeight::new(100) || height == BlockHeight::new(TIP_HEIGHT) {
            assert_eq!(actual, Some(expected_roots(&activations, height)?));
        } else {
            assert_eq!(
                actual,
                None,
                "height {} has no stored checkpoint",
                height.value()
            );
        }
    }
    Ok(())
}

#[test]
fn stored_roots_stay_aligned_and_reject_damage_across_two_batches() -> TestResult {
    use zinder_testkit::{CanonicalBaseline, CanonicalWriterFixture, ChainFixture};

    let activations = activations()?;
    let chain = ChainFixture::new(activations.network()).extend_blocks(1_101);
    let writer = CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
    let path = writer.canonical_primary_path().to_owned();
    let window = writer
        .canonical_primary()?
        .reorg_policy()
        .reorg_window_blocks();
    // Keep the fixture directory while reopening the primary for a damaged-row read.
    let _readers = writer.into_serving_store_fixture()?;
    let store = RocksDbCanonicalStore::open_ready(
        &path,
        &activations,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(window)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    // The first batch ends at 1,090. Each batch contains both gaps and stored rows.
    let range = BlockHeightRange::inclusive(BlockHeight::new(67), BlockHeight::new(1_101));
    let roots = store.stored_final_note_commitment_roots_in_range(range, &activations)?;
    assert_eq!(roots.len(), range.into_iter().len());
    for (height, actual) in range.into_iter().zip(roots) {
        let expected = if height.value().is_multiple_of(100) || height == range.end {
            let checkpoint = store
                .tree_state_checkpoint_at(height, &activations)?
                .ok_or("fixture checkpoint missing")?;
            Some(
                checkpoint
                    .frontiers
                    .final_note_commitment_roots(checkpoint.block_id),
            )
        } else {
            None
        };
        assert_eq!(actual, expected, "height {}", height.value());
    }

    replace_checkpoint_row(&store, BlockHeight::new(1_100), &[])?;
    assert!(matches!(
        store.stored_final_note_commitment_roots_in_range(range, &activations),
        Err(CanonicalStoreError::PublicationRefused { .. })
    ));
    Ok(())
}

#[test]
fn range_bounds_exclude_heights_outside_retained_history_and_visible_tip() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let activations = activations()?;

    let empty = store.stored_final_note_commitment_roots_in_range(
        BlockHeightRange::empty_at(BlockHeight::new(10)),
        &activations,
    )?;
    assert_eq!(empty, []);
    let excessive = BlockHeightRange::inclusive(BlockHeight::new(0), BlockHeight::new(u32::MAX));
    assert!(matches!(
        store.stored_final_note_commitment_roots_in_range(excessive, &activations),
        Err(CanonicalStoreError::BlockReplaySequenceInvalid { .. })
    ));

    // Height 0 is below first retained block (1), and heights > TIP_HEIGHT are
    // above visible tip. Even if a row is present at 0 (the history
    // predecessor) or written above TIP_HEIGHT, both return None.
    let above_tip = BlockHeight::new(TIP_HEIGHT + 1);
    replace_checkpoint_row(
        &store,
        above_tip,
        &encode_tree_state_checkpoint(
            above_tip.value(),
            &test_checkpoint_frontiers(&activations, BlockHeight::new(TIP_HEIGHT))?,
        ),
    )?;

    let spanning =
        BlockHeightRange::inclusive(BlockHeight::new(0), BlockHeight::new(TIP_HEIGHT + 2));
    let roots = store.stored_final_note_commitment_roots_in_range(spanning, &activations)?;
    assert_eq!(roots.len(), usize::try_from(TIP_HEIGHT + 3)?);
    assert_eq!(roots[0], None, "history predecessor height 0 is excluded");
    assert_eq!(
        roots[1],
        Some(expected_roots(&activations, BlockHeight::new(1))?)
    );
    assert_eq!(
        roots[usize::try_from(TIP_HEIGHT)?],
        Some(expected_roots(&activations, BlockHeight::new(TIP_HEIGHT))?)
    );
    assert_eq!(
        roots[usize::try_from(TIP_HEIGHT + 1)?],
        None,
        "rows above visible tip are never read"
    );
    assert_eq!(roots[usize::try_from(TIP_HEIGHT + 2)?], None);

    // Range wholly above visible tip returns all None.
    let wholly_above = store.stored_final_note_commitment_roots_in_range(
        BlockHeightRange::inclusive(
            BlockHeight::new(TIP_HEIGHT + 1),
            BlockHeight::new(TIP_HEIGHT + 3),
        ),
        &activations,
    )?;
    assert_eq!(wholly_above, vec![None, None, None]);
    Ok(())
}

#[test]
fn unadmitted_activation_table_is_refused() -> TestResult {
    let temporary = TempDir::new()?;
    let store = sparse_store(&temporary)?;
    let admitted = activations()?;
    let shifted = NetworkUpgradeActivations::new(
        Network::ZcashTestnet,
        admitted
            .activations()
            .iter()
            .map(|activation| NetworkUpgradeActivation {
                activation_height: BlockHeight::new(activation.activation_height.value() + 1),
                ..activation.clone()
            })
            .collect(),
    )?;

    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(10));
    assert!(matches!(
        store.stored_final_note_commitment_roots_in_range(range, &shifted),
        Err(CanonicalStoreError::AdmissionRefused { .. })
    ));
    Ok(())
}

#[test]
fn malformed_checkpoint_framing_fails_closed_as_store_damage() -> TestResult {
    let sapling_only = CommitmentTreeFrontiers::from_validated_parts(
        Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
        None,
        None,
    )?;
    let mut bad_node_flag = encode_tree_state_checkpoint(4, &sapling_only);
    // Node presence flag follows block time (4), bitmap (1), root (32), len (2).
    bad_node_flag[5 + 32 + 2] = 2;

    let mut trailing_byte = encode_tree_state_checkpoint(4, &sapling_only);
    trailing_byte.push(0);

    let temporary = TempDir::new()?;
    let store = sparse_store(&temporary)?;
    for bad_row in [
        vec![0, 0],
        vec![4, 0, 0, 0, 0x80],
        bad_node_flag,
        trailing_byte,
    ] {
        replace_checkpoint_row(&store, BlockHeight::new(4), &bad_row)?;

        let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(5));
        assert!(
            matches!(
                store.stored_final_note_commitment_roots_in_range(range, &activations()?),
                Err(CanonicalStoreError::PublicationRefused { .. })
            ),
            "malformed row {bad_row:?} must fail closed"
        );
    }
    Ok(())
}

#[test]
fn wrong_active_pool_presence_fails_closed_as_store_damage() -> TestResult {
    let empty = |protocol| Some(CommitmentTreeFrontier::empty(protocol));
    let cases = [
        // Pre-Sapling height carrying a Sapling frontier.
        (
            BlockHeight::new(1),
            CommitmentTreeFrontiers::from_validated_parts(
                empty(ShieldedProtocol::Sapling),
                None,
                None,
            )?,
        ),
        // Sapling-only height carrying an Orchard frontier before activation.
        (
            BlockHeight::new(5),
            CommitmentTreeFrontiers::from_validated_parts(
                empty(ShieldedProtocol::Sapling),
                empty(ShieldedProtocol::Orchard),
                None,
            )?,
        ),
        // Post-Ironwood height missing Ironwood.
        (
            BlockHeight::new(100),
            CommitmentTreeFrontiers::from_validated_parts(
                empty(ShieldedProtocol::Sapling),
                empty(ShieldedProtocol::Orchard),
                None,
            )?,
        ),
    ];

    let temporary = TempDir::new()?;
    let store = sparse_store(&temporary)?;
    for (height, frontiers) in cases {
        replace_checkpoint_row(
            &store,
            height,
            &encode_tree_state_checkpoint(height.value(), &frontiers),
        )?;

        let range = BlockHeightRange::inclusive(height, height);
        assert!(
            matches!(
                store.stored_final_note_commitment_roots_in_range(range, &activations()?),
                Err(CanonicalStoreError::PublicationRefused { .. })
            ),
            "pool presence mismatch at height {} must fail closed",
            height.value()
        );
    }
    Ok(())
}

#[test]
fn block_time_mismatch_or_missing_header_fails_closed_as_store_damage() -> TestResult {
    let activations = activations()?;

    // Block time differs from the canonical header at height 50.
    let temporary = TempDir::new()?;
    let store = sparse_store(&temporary)?;
    let height = BlockHeight::new(50);
    let frontiers = test_checkpoint_frontiers(&activations, height)?;
    replace_checkpoint_row(
        &store,
        height,
        &encode_tree_state_checkpoint(height.value() + 1, &frontiers),
    )?;
    assert!(matches!(
        store.stored_final_note_commitment_roots_in_range(
            BlockHeightRange::inclusive(BlockHeight::new(45), BlockHeight::new(55)),
            &activations,
        ),
        Err(CanonicalStoreError::PublicationRefused { .. })
    ));

    // Missing canonical header at a height with a stored checkpoint fails closed.
    replace_checkpoint_row(
        &store,
        height,
        &encode_tree_state_checkpoint(height.value(), &frontiers),
    )?;
    let header_family = column_family(&store.bounded_open.db, BLOCK_HEADER_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .delete_cf(&header_family, encode_block_position(height))?;
    assert!(matches!(
        store.stored_final_note_commitment_roots_in_range(
            BlockHeightRange::inclusive(height, height),
            &activations,
        ),
        Err(CanonicalStoreError::PublicationRefused { .. })
    ));
    Ok(())
}

#[test]
fn stored_roots_read_checks_framing_without_rederiving_merkle_roots() -> TestResult {
    let temporary = TempDir::new()?;
    let store = densified_store(&temporary)?;
    let activations = activations()?;
    let height = BlockHeight::new(50);
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    let mut row = store
        .bounded_open
        .db
        .get_cf(&family, encode_block_position(height))?
        .ok_or("checkpoint row must exist")?;
    // Flip first byte of the stored Sapling root while keeping framing,
    // presence bitmap, and block time valid.
    row[5] ^= 0xff;
    replace_checkpoint_row(&store, height, &row)?;

    let expected = expected_roots(&activations, height)?;
    let mut flipped_sapling = expected
        .sapling
        .ok_or("height 50 has a Sapling root")?
        .as_bytes();
    flipped_sapling[0] ^= 0xff;
    let stored = store.stored_final_note_commitment_roots_in_range(
        BlockHeightRange::inclusive(height, height),
        &activations,
    )?;
    assert_eq!(
        stored,
        vec![Some(BlockFinalNoteCommitmentRoots::new(
            height,
            expected.block_hash,
            Some(FinalNoteCommitmentRoot::from_bytes(flipped_sapling)),
            expected.orchard,
            expected.ironwood,
        ))]
    );
    // Revalidating checkpoint read still refuses the tampered root.
    assert!(matches!(
        store.tree_state_checkpoint_at(height, &activations),
        Err(CanonicalStoreError::PublicationRefused { .. })
    ));
    Ok(())
}

fn activations() -> Result<NetworkUpgradeActivations, Box<dyn std::error::Error>> {
    Ok(test_network_upgrade_activations(Network::ZcashTestnet)?)
}

fn sparse_store(temporary: &TempDir) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_store(
        &temporary.path().join("canonical"),
        2,
        BlockHeight::new(TIP_HEIGHT),
        BlockHeight::new(SETTLED_HEIGHT),
    )
}

fn densified_store(
    temporary: &TempDir,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let store = sparse_store(temporary)?;
    let activations = activations()?;
    let stride = store
        .next_tree_state_stride(
            BlockHeight::new(0),
            TreeStateCheckpointSelection::EveryBlock,
        )?
        .ok_or("the settled stride 0..=100 must need densification")?;
    let densified = store.densify_tree_state_stride(
        stride,
        TreeStateCheckpointSelection::EveryBlock,
        &activations,
    )?;
    store.commit_tree_state_stride(&densified)?;
    Ok(store)
}

fn open_secondary(
    temporary: &TempDir,
) -> Result<RocksDbCanonicalSecondary, Box<dyn std::error::Error>> {
    Ok(RocksDbCanonicalSecondary::open_ready(
        temporary.path().join("canonical"),
        temporary.path().join("canonical-secondary"),
        &activations()?,
        crate::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

fn expected_roots(
    activations: &NetworkUpgradeActivations,
    height: BlockHeight,
) -> Result<BlockFinalNoteCommitmentRoots, Box<dyn std::error::Error>> {
    let hash = BlockHash::from_bytes([u8::try_from(height.value())?; 32]);
    let frontiers = test_checkpoint_frontiers(activations, height)?;
    Ok(frontiers.final_note_commitment_roots(zinder_core::BlockId::new(height, hash)))
}

fn replace_checkpoint_row(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
    row: &[u8],
) -> TestResult {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .put_cf(&family, encode_block_position(height), row)?;
    Ok(())
}
