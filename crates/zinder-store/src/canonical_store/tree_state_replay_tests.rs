//! Tree-state reconstruction over construction-stride gaps.
//!
//! The published test store checkpoints only every 100 blocks and at its tip,
//! and its pools activate at heights 2, 6, and 10, so reconstruction crosses
//! every activation and every stride boundary.

use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CommitmentTreeCheckpoint, CompactBlockArtifact,
    CompactChainMetadata, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
};

use super::{
    CanonicalStoreError, RocksDbCanonicalStore, block_load::encode_block_position,
    live_replacement_tests::published_store, publication::column_family,
    rocksdb::COMPACT_BLOCK_COLUMN_FAMILY, test_checkpoint_frontiers,
    test_network_upgrade_activations,
};

const TIP_HEIGHT: u32 = 150;

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
        test_checkpoint_frontiers(activations, height),
    ))
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
