//! Tree-state reconstruction over construction-stride gaps.
//!
//! The published test store checkpoints only every 100 blocks and at its tip,
//! and its pools activate at heights 2, 6, and 10, so reconstruction crosses
//! every activation and every stride boundary.

use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CommitmentTreeCheckpoint, CompactBlockArtifact,
    CompactChainMetadata, ConsensusBranchId, Network, NetworkUpgradeActivation,
    NetworkUpgradeActivations,
};

use super::{
    CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload, RocksDbCanonicalSecondary,
    RocksDbCanonicalStore, ServedTreeState, TreeStateLookup, TreeStateReplay,
    TreeStateReplayOutcome,
    block_load::encode_block_position,
    live_replacement_tests::published_store,
    publication::column_family,
    rocksdb::{COMPACT_BLOCK_COLUMN_FAMILY, TREE_STATE_CHECKPOINT_COLUMN_FAMILY},
    test_checkpoint_frontiers, test_network_upgrade_activations,
};
use crate::{RawBlobRetention, RocksDbResourceBudget};

const TIP_HEIGHT: u32 = 150;

#[test]
fn cancellation_stops_replay_between_compact_blocks() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let Some(TreeStateLookup::Replay(replay)) =
        store.tree_state_lookup(BlockHeight::new(99), &activations)?
    else {
        return Err("missing sparse replay".into());
    };
    let checks = std::cell::Cell::new(0);
    let outcome = store.replay_tree_state_until_cancelled(&replay, &activations, || {
        checks.set(checks.get() + 1);
        checks.get() == 4
    });
    assert!(matches!(outcome, Err(CanonicalStoreError::ReplayCancelled)));
    assert_eq!(checks.get(), 4);
    Ok(())
}

#[test]
fn every_retained_height_reads_the_checkpoint_construction_would_store()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;

    for height in 0..=TIP_HEIGHT {
        let height = BlockHeight::new(height);
        assert_eq!(
            store.tree_state_checkpoint_at(height, &activations)?,
            Some(expected_checkpoint(&activations, height)?),
            "tree state at height {}",
            height.value()
        );
    }
    assert_eq!(
        store.tree_state_checkpoint_at(BlockHeight::new(TIP_HEIGHT + 1), &activations)?,
        None
    );
    Ok(())
}

#[test]
fn reconstruction_refuses_a_compact_block_that_breaks_the_parent_link()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let damaged = BlockHeight::new(120);
    replace_compact_block(
        &store,
        &stored_compact_block(&store, damaged, |parts| {
            parts.previous_block_hash = BlockHash::from_bytes([0xee; 32]);
        })?,
    )?;

    assert_publication_refused(&store, BlockHeight::new(130), &activations)?;
    assert_publication_refused(&store, damaged, &activations)?;
    assert_eq!(
        store.tree_state_checkpoint_at(BlockHeight::new(119), &activations)?,
        Some(expected_checkpoint(&activations, BlockHeight::new(119))?)
    );
    assert_eq!(
        store.tree_state_checkpoint_at(BlockHeight::new(150), &activations)?,
        Some(expected_checkpoint(&activations, BlockHeight::new(150))?),
        "a stored checkpoint does not replay the damaged block"
    );
    Ok(())
}

#[test]
fn reconstruction_refuses_tree_sizes_the_note_commitments_do_not_reach()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    replace_compact_block(
        &store,
        &stored_compact_block(&store, BlockHeight::new(120), |parts| {
            parts.chain_metadata.sapling_commitment_tree_size = 1;
        })?,
    )?;

    assert_publication_refused(&store, BlockHeight::new(120), &activations)
}

#[test]
fn reconstruction_refuses_compact_blocks_that_end_at_another_block()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    replace_compact_block(
        &store,
        &stored_compact_block(&store, BlockHeight::new(130), |parts| {
            parts.block_id.hash = BlockHash::from_bytes([0xee; 32]);
        })?,
    )?;

    assert_publication_refused(&store, BlockHeight::new(130), &activations)
}

#[test]
fn reconstruction_refuses_a_missing_compact_block() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let family = column_family(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .delete_cf(&family, encode_block_position(BlockHeight::new(120)))?;

    assert_publication_refused(&store, BlockHeight::new(140), &activations)
}

#[test]
fn reconstruction_refuses_a_missing_cadence_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    delete_checkpoint(&store, BlockHeight::new(100))?;

    for height in [100, 120, 149] {
        assert_publication_refused(&store, BlockHeight::new(height), &activations)?;
    }
    for height in [99, TIP_HEIGHT] {
        let height = BlockHeight::new(height);
        assert_eq!(
            store.tree_state_checkpoint_at(height, &activations)?,
            Some(expected_checkpoint(&activations, height)?),
            "tree state at height {}",
            height.value()
        );
    }
    Ok(())
}

#[test]
fn reconstruction_refuses_a_missing_history_predecessor_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    delete_checkpoint(&store, BlockHeight::new(0))?;

    for height in [0, 1, 99] {
        assert_publication_refused(&store, BlockHeight::new(height), &activations)?;
    }
    assert_eq!(
        store.tree_state_checkpoint_at(BlockHeight::new(120), &activations)?,
        Some(expected_checkpoint(&activations, BlockHeight::new(120))?)
    );
    Ok(())
}

#[test]
fn reconstruction_refuses_an_activation_table_the_store_did_not_admit()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let admitted = test_network_upgrade_activations(Network::ZcashTestnet)?;
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

    for height in [100, 120] {
        assert!(matches!(
            store.tree_state_checkpoint_at(BlockHeight::new(height), &shifted),
            Err(CanonicalStoreError::AdmissionRefused { .. })
        ));
    }
    Ok(())
}

#[test]
fn a_secondary_reconstructs_with_a_node_table_its_writer_has_not_admitted_yet()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let mut store = gapped_store(&temporary)?;
    let baseline = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let admitted = with_future_activation(&baseline, 99, TIP_HEIGHT + 50)?;
    store.admit_network_upgrade_schedule(&admitted)?;
    let secondary = RocksDbCanonicalSecondary::open_ready(
        temporary.path().join("canonical"),
        temporary.path().join("canonical-secondary"),
        &baseline,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(2)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let node_observed = with_future_activation(&admitted, 100, TIP_HEIGHT + 60)?;
    store.admit_network_upgrade_schedule(&node_observed)?;
    assert_eq!(secondary.network_upgrade_activations(), Some(&admitted));

    for height in [100, 120, TIP_HEIGHT] {
        let height = BlockHeight::new(height);
        assert_eq!(
            secondary.tree_state_checkpoint_at(height, &node_observed)?,
            Some(expected_checkpoint(&baseline, height)?),
            "tree state at height {}",
            height.value()
        );
    }
    let mut historical_edit = node_observed.activations().to_vec();
    historical_edit[0].name = "Changed historical upgrade".to_owned();
    let historical_edit = NetworkUpgradeActivations::new(Network::ZcashTestnet, historical_edit)?;
    assert!(matches!(
        secondary.tree_state_checkpoint_at(BlockHeight::new(120), &historical_edit),
        Err(CanonicalStoreError::AdmissionRefused { .. })
    ));
    Ok(())
}

#[test]
fn a_lookup_answers_stored_heights_and_seeds_the_rest() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;

    for stored in [0, 100, TIP_HEIGHT] {
        let height = BlockHeight::new(stored);
        assert_eq!(
            store.tree_state_lookup(height, &activations)?,
            Some(TreeStateLookup::Stored(expected_tree_state(
                &activations,
                height
            )?))
        );
    }
    let Some(TreeStateLookup::Replay(replay)) =
        store.tree_state_lookup(BlockHeight::new(137), &activations)?
    else {
        return Err("height 137 has no stored checkpoint".into());
    };
    assert_eq!(
        replay.seed(),
        &expected_checkpoint(&activations, BlockHeight::new(100))?
    );
    assert_eq!(replay.height(), BlockHeight::new(137));
    assert_eq!(replay.replayed_blocks(), 37);
    assert_eq!(
        store.tree_state_lookup(BlockHeight::new(TIP_HEIGHT + 1), &activations)?,
        None
    );
    Ok(())
}

#[test]
fn a_replay_starts_from_the_seed_its_lookup_read() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let height = BlockHeight::new(137);
    let Some(TreeStateLookup::Replay(replay)) = store.tree_state_lookup(height, &activations)?
    else {
        return Err("height 137 has no stored checkpoint".into());
    };

    delete_checkpoint(&store, BlockHeight::new(100))?;

    assert_eq!(
        store.replay_tree_state(&replay, &activations)?,
        Some(TreeStateReplayOutcome::Replayed {
            replay: Box::new(replay.clone()),
            tree_state: expected_tree_state(&activations, height)?,
        })
    );
    assert_publication_refused(&store, height, &activations)
}

#[test]
fn a_replay_on_another_visible_chain_reads_its_own_seed() -> Result<(), Box<dyn std::error::Error>>
{
    let issuing_directory = TempDir::new()?;
    let issuing = gapped_store(&issuing_directory)?;
    let other_directory = TempDir::new()?;
    let other = published_store(
        &other_directory.path().join("canonical"),
        2,
        BlockHeight::new(140),
        BlockHeight::new(140),
    )?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let replay_at = |height| -> Result<_, Box<dyn std::error::Error>> {
        match issuing.tree_state_lookup(BlockHeight::new(height), &activations)? {
            Some(TreeStateLookup::Replay(replay)) => Ok(replay),
            other => Err(format!("height {height} looked up as {other:?}").into()),
        }
    };
    let within_both = replay_at(120)?;
    let stored_by_other = replay_at(140)?;
    let above_other_tip = replay_at(145)?;
    let Some(TreeStateLookup::Replay(other_seed)) =
        other.tree_state_lookup(BlockHeight::new(120), &activations)?
    else {
        return Err("height 120 has no stored checkpoint in the other store".into());
    };

    assert_eq!(
        other.replay_tree_state(&within_both, &activations)?,
        Some(TreeStateReplayOutcome::Replayed {
            replay: Box::new(other_seed),
            tree_state: expected_tree_state(&activations, BlockHeight::new(120))?,
        })
    );
    assert_eq!(
        other.replay_tree_state(&stored_by_other, &activations)?,
        Some(TreeStateReplayOutcome::Stored(expected_tree_state(
            &activations,
            BlockHeight::new(140)
        )?))
    );
    assert_eq!(
        other.replay_tree_state(&above_other_tip, &activations)?,
        None
    );
    delete_checkpoint(&other, BlockHeight::new(100))?;
    assert!(matches!(
        other.replay_tree_state(&within_both, &activations),
        Err(CanonicalStoreError::PublicationRefused { .. })
    ));
    assert_eq!(
        issuing.replay_tree_state(&within_both, &activations)?,
        Some(TreeStateReplayOutcome::Replayed {
            replay: Box::new(within_both.clone()),
            tree_state: expected_tree_state(&activations, BlockHeight::new(120))?,
        })
    );
    Ok(())
}

/// A replay built outside a store lookup proves nothing about its seed, so
/// even a store showing the visible chain the seed names replays from the
/// seed it reads itself.
#[test]
fn a_replay_built_outside_a_lookup_reads_the_store_seed() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let height = BlockHeight::new(137);
    let Some(TreeStateLookup::Replay(issued)) = store.tree_state_lookup(height, &activations)?
    else {
        return Err("height 137 has no stored checkpoint".into());
    };
    let unstored = expected_checkpoint(&activations, BlockHeight::new(136))?;
    let mut unlinked = unstored.clone();
    unlinked.block_id.hash = BlockHash::from_bytes([0xee; 32]);

    for seed in [unstored, unlinked] {
        let built = TreeStateReplay::new(seed, height).ok_or("the seed lies below the height")?;
        assert_eq!(
            store.replay_tree_state(&built, &activations)?,
            Some(TreeStateReplayOutcome::Replayed {
                replay: Box::new(issued.clone()),
                tree_state: expected_tree_state(&activations, height)?,
            })
        );
    }
    Ok(())
}

#[test]
fn a_replay_refuses_an_activation_table_the_store_did_not_admit()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let admitted = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let Some(TreeStateLookup::Replay(replay)) =
        store.tree_state_lookup(BlockHeight::new(120), &admitted)?
    else {
        return Err("height 120 has no stored checkpoint".into());
    };
    let mut edited = admitted.activations().to_vec();
    edited[0].name = "Changed historical upgrade".to_owned();
    let edited = NetworkUpgradeActivations::new(Network::ZcashTestnet, edited)?;

    assert!(matches!(
        store.replay_tree_state(&replay, &edited),
        Err(CanonicalStoreError::AdmissionRefused { .. })
    ));
    Ok(())
}

/// A served read of a stored checkpoint neither reads nor re-derives its
/// roots.
///
/// It checks the row's framing, so a root that no longer matches its
/// frontier does not change what it serves. The lookup decides how to decode
/// the row only after its seek lands on the requested height.
#[test]
fn a_stored_checkpoint_is_served_without_its_roots() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let height = BlockHeight::new(100);
    flip_first_root_byte(&store, height)?;
    let expected = expected_tree_state(&activations, height)?;

    assert_eq!(
        store.tree_state_lookup(height, &activations)?,
        Some(TreeStateLookup::Stored(expected.clone()))
    );
    for read_height in [100, 120] {
        assert_eq!(
            store.tree_state_checkpoint_at_or_before(BlockHeight::new(read_height))?,
            Some(expected.clone())
        );
    }
    Ok(())
}

/// A served checkpoint whose framing is broken is still store damage.
#[test]
fn a_stored_checkpoint_with_broken_framing_is_refused_when_served()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    let height = BlockHeight::new(100);
    let mut row = stored_checkpoint_row(&store, height)?;
    row.pop();
    replace_checkpoint_row(&store, height, &row)?;

    assert!(matches!(
        store.tree_state_lookup(height, &activations),
        Err(CanonicalStoreError::PublicationRefused { .. })
    ));
    assert!(matches!(
        store.tree_state_checkpoint_at_or_before(height),
        Err(CanonicalStoreError::PublicationRefused { .. })
    ));
    Ok(())
}

/// Every read that builds further tree state from a stored checkpoint parses
/// it in full and re-derives its roots before using it.
#[test]
fn a_checkpoint_whose_root_does_not_match_is_refused_where_it_seeds_tree_state()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let store = gapped_store(&temporary)?;
    let activations = test_network_upgrade_activations(Network::ZcashTestnet)?;
    flip_first_root_byte(&store, BlockHeight::new(100))?;
    flip_first_root_byte(&store, BlockHeight::new(TIP_HEIGHT))?;

    for height in [100, 120, TIP_HEIGHT] {
        assert_publication_refused(&store, BlockHeight::new(height), &activations)?;
    }
    assert!(
        matches!(
            store.tree_state_lookup(BlockHeight::new(120), &activations),
            Err(CanonicalStoreError::PublicationRefused { .. })
        ),
        "a replay seed is refused before the replay starts"
    );
    assert!(matches!(
        store.append_anchor(),
        Err(CanonicalStoreError::LiveCommitRefused { .. })
    ));
    Ok(())
}

/// `activations` with one more upgrade activating at `activation_height`.
fn with_future_activation(
    activations: &NetworkUpgradeActivations,
    branch_id: u32,
    activation_height: u32,
) -> Result<NetworkUpgradeActivations, Box<dyn std::error::Error>> {
    let mut entries = activations.activations().to_vec();
    entries.push(NetworkUpgradeActivation {
        branch_id: ConsensusBranchId::new(branch_id),
        activation_height: BlockHeight::new(activation_height),
        name: format!("Future {branch_id}"),
    });
    Ok(NetworkUpgradeActivations::new(
        Network::ZcashTestnet,
        entries,
    )?)
}

/// Publishes complete history through [`TIP_HEIGHT`], whose checkpoints are
/// the history predecessor, height 100, and the tip.
fn gapped_store(temporary: &TempDir) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    published_store(
        &temporary.path().join("canonical"),
        2,
        BlockHeight::new(TIP_HEIGHT),
        BlockHeight::new(TIP_HEIGHT),
    )
}

/// The checkpoint a construction checkpoint row would hold at `height`.
///
/// The published test blocks carry no note commitments and use the height as
/// both their hash byte and their block time.
fn expected_checkpoint(
    activations: &NetworkUpgradeActivations,
    height: BlockHeight,
) -> Result<CommitmentTreeCheckpoint, Box<dyn std::error::Error>> {
    let hash = if height.value() == 0 {
        Network::ZcashTestnet.genesis_hash()
    } else {
        BlockHash::from_bytes([u8::try_from(height.value())?; 32])
    };
    Ok(CommitmentTreeCheckpoint::new(
        BlockId::new(height, hash),
        height.value(),
        test_checkpoint_frontiers(activations, height)?,
    ))
}

/// The tree state a wallet read serves at `height`, from the checkpoint
/// [`expected_checkpoint`] names.
fn expected_tree_state(
    activations: &NetworkUpgradeActivations,
    height: BlockHeight,
) -> Result<ServedTreeState, Box<dyn std::error::Error>> {
    Ok(ServedTreeState::from(&expected_checkpoint(
        activations,
        height,
    )?))
}

/// Mutable fields of one stored compact block.
struct CompactBlockParts {
    block_id: BlockId,
    previous_block_hash: BlockHash,
    chain_metadata: CompactChainMetadata,
}

/// Reads the compact block at `height` and rebuilds it with `damage` applied.
fn stored_compact_block(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
    damage: impl FnOnce(&mut CompactBlockParts),
) -> Result<CompactBlockArtifact, Box<dyn std::error::Error>> {
    let stored = store
        .compact_block_at(height)?
        .ok_or("compact block is absent")?;
    let mut parts = CompactBlockParts {
        block_id: BlockId::new(stored.height(), stored.block_hash()),
        previous_block_hash: stored.previous_block_hash(),
        chain_metadata: stored.chain_metadata(),
    };
    damage(&mut parts);
    Ok(CompactBlockArtifact::new(
        parts.block_id,
        parts.previous_block_hash,
        stored.time(),
        stored.transactions().to_vec(),
        parts.chain_metadata,
    )?)
}

fn replace_compact_block(
    store: &RocksDbCanonicalStore,
    compact_block: &CompactBlockArtifact,
) -> Result<(), Box<dyn std::error::Error>> {
    let family = column_family(&store.bounded_open.db, COMPACT_BLOCK_COLUMN_FAMILY)?;
    store.bounded_open.db.put_cf(
        &family,
        encode_block_position(compact_block.height()),
        crate::encode_compact_block_artifact(compact_block),
    )?;
    Ok(())
}

fn delete_checkpoint(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
) -> Result<(), Box<dyn std::error::Error>> {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .delete_cf(&family, encode_block_position(height))?;
    Ok(())
}

fn stored_checkpoint_row(
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

fn replace_checkpoint_row(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
    row: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let family = column_family(&store.bounded_open.db, TREE_STATE_CHECKPOINT_COLUMN_FAMILY)?;
    store
        .bounded_open
        .db
        .put_cf(&family, encode_block_position(height), row)?;
    Ok(())
}

/// Flips one byte of the Sapling root stored at `height`, which follows the
/// block time and the presence bitmap.
fn flip_first_root_byte(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut row = stored_checkpoint_row(store, height)?;
    let root_byte = row
        .get_mut(5)
        .ok_or("checkpoint row holds no Sapling root")?;
    *root_byte ^= 0xff;
    replace_checkpoint_row(store, height, &row)
}

fn assert_publication_refused(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
    activations: &NetworkUpgradeActivations,
) -> Result<(), Box<dyn std::error::Error>> {
    match store.tree_state_checkpoint_at(height, activations) {
        Err(CanonicalStoreError::PublicationRefused { .. }) => Ok(()),
        other => Err(format!(
            "height {} must refuse damaged history, got {other:?}",
            height.value()
        )
        .into()),
    }
}
