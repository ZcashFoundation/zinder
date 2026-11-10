use tempfile::TempDir;
use zinder_core::{BlockHash, BlockHeight, BlockId, Network, UnixTimestampMillis};

use super::{
    CanonicalLiveAppend, CanonicalReorgPolicy, CanonicalStoreError, CanonicalStoreWorkload,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore,
    live_replacement_tests::{
        open_store, open_store_with_retention, published_store, published_store_with_retention,
        replacement_block,
    },
};
use crate::{RawBlobRetention, RocksDbResourceBudget};

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
