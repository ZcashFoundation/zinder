use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockFactsSequenceDigestVersion, ChainEpochId, Network, UnixTimestampMillis,
};
use zinder_store::RocksDbResourceBudget;
use zinder_wallet_projection::{
    WalletCanonicalSourceIdentity, WalletProjectionBuildLease, WalletProjectionBuildLeaseRequest,
    WalletProjectionBuildOwner, WalletProjectionRetainedEventAnchor,
    WalletProjectionSourcePosition,
};
use zinder_wallet_rocksdb::{RocksDbWalletBuildStore, RocksDbWalletError};

#[test]
fn active_build_lease_refuses_a_competing_owner() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let source = source_identity();
    let build = RocksDbWalletBuildStore::create_fresh(
        temporary.path().join("wallet"),
        Network::ZcashRegtest,
        source,
        0,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let now = UnixTimestampMillis::new(100);
    let lease =
        build.try_acquire_lease(request(0x11, source, UnixTimestampMillis::new(200)), now)?;

    assert_eq!(
        lease.owner(),
        WalletProjectionBuildOwner::from_bytes([0x11; 16])
    );
    assert!(matches!(
        build.try_acquire_lease(request(0x22, source, UnixTimestampMillis::new(300)), now),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseHeld { holder, expires_at })
            if holder == lease.owner() && expires_at == lease.expires_at()
    ));
    drop(build);
    Ok(())
}

#[test]
fn same_owner_reclaims_a_live_lease_at_a_higher_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let source = source_identity();
    let path = temporary.path().join("wallet");
    let build = RocksDbWalletBuildStore::create_fresh(
        &path,
        Network::ZcashRegtest,
        source,
        0,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let crashed = build.try_acquire_lease(
        request(0x11, source, UnixTimestampMillis::new(10_000)),
        UnixTimestampMillis::new(100),
    )?;
    drop(build);

    let restarted = RocksDbWalletBuildStore::open(
        &path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert!(matches!(
        restarted.try_acquire_lease(
            request(0x22, source, UnixTimestampMillis::new(10_000)),
            UnixTimestampMillis::new(101),
        ),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseHeld { holder, .. })
            if holder == crashed.owner()
    ));
    let reclaimed = restarted.try_acquire_lease(
        request(0x11, source, UnixTimestampMillis::new(20_000)),
        UnixTimestampMillis::new(101),
    )?;
    assert_eq!(reclaimed.owner(), crashed.owner());
    assert_eq!(reclaimed.generation(), crashed.generation() + 1);
    assert_eq!(reclaimed.expires_at(), UnixTimestampMillis::new(20_000));

    assert!(matches!(
        restarted.renew_lease(
            crashed,
            UnixTimestampMillis::new(30_000),
            UnixTimestampMillis::new(102),
        ),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseGenerationMismatch { .. })
    ));
    assert!(matches!(
        restarted.release_lease(crashed, UnixTimestampMillis::new(102)),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseGenerationMismatch { .. })
    ));
    let renewed = restarted.renew_lease(
        reclaimed,
        UnixTimestampMillis::new(30_000),
        UnixTimestampMillis::new(102),
    )?;
    restarted.release_lease(renewed, UnixTimestampMillis::new(103))?;
    Ok(())
}

#[test]
fn expired_lease_takeover_persists_and_rejects_stale_capabilities()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let source = source_identity();
    let path = temporary.path().join("wallet");
    let build = RocksDbWalletBuildStore::create_fresh(
        &path,
        Network::ZcashRegtest,
        source,
        0,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let original = build.try_acquire_lease(
        request(0x11, source, UnixTimestampMillis::new(200)),
        UnixTimestampMillis::new(100),
    )?;
    drop(build);

    let reopened = RocksDbWalletBuildStore::open(
        &path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let takeover = reopened.try_acquire_lease(
        request(0x22, source, UnixTimestampMillis::new(400)),
        UnixTimestampMillis::new(200),
    )?;
    assert_eq!(takeover.generation(), original.generation() + 1);
    assert!(matches!(
        reopened.renew_lease(
            original,
            UnixTimestampMillis::new(500),
            UnixTimestampMillis::new(201)
        ),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseOwnerMismatch { .. })
    ));

    let stale_generation = WalletProjectionBuildLease::from_request(
        request(0x22, source, UnixTimestampMillis::new(400)),
        original.generation(),
        Network::ZcashRegtest,
    );
    assert!(matches!(
        reopened.renew_lease(
            stale_generation,
            UnixTimestampMillis::new(500),
            UnixTimestampMillis::new(201),
        ),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseGenerationMismatch { .. })
    ));

    let renewed = reopened.renew_lease(
        takeover,
        UnixTimestampMillis::new(500),
        UnixTimestampMillis::new(201),
    )?;
    reopened.release_lease(renewed, UnixTimestampMillis::new(202))?;
    let next = reopened.try_acquire_lease(
        request(0x33, source, UnixTimestampMillis::new(600)),
        UnixTimestampMillis::new(202),
    )?;
    assert_eq!(next.generation(), takeover.generation() + 1);
    Ok(())
}

#[test]
fn build_lease_rejects_a_stale_canonical_or_retained_event_anchor()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let source = source_identity();
    let build = RocksDbWalletBuildStore::create_fresh(
        temporary.path().join("wallet"),
        Network::ZcashRegtest,
        source,
        0,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let stale_source = WalletCanonicalSourceIdentity::new(
        WalletProjectionSourcePosition::new(
            ChainEpochId::new(2),
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([0x55; 32])),
            2,
        ),
        source.source_sequence_digest(),
        source.settled_tip(),
    );
    assert!(matches!(
        build.try_acquire_lease(
            request(0x11, stale_source, UnixTimestampMillis::new(200)),
            UnixTimestampMillis::new(100),
        ),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch { .. })
    ));
    let invalid_retention = WalletProjectionBuildLeaseRequest::new(
        WalletProjectionBuildOwner::from_bytes([0x11; 16]),
        source,
        WalletProjectionRetainedEventAnchor::new(2),
        UnixTimestampMillis::new(200),
    );
    assert!(matches!(
        build.try_acquire_lease(invalid_retention, UnixTimestampMillis::new(100)),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch { .. })
    ));
    Ok(())
}

#[test]
fn discard_unpublished_refuses_another_live_owner_then_removes_an_expired_build()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let source = source_identity();
    let path = temporary.path().join("wallet");
    let build = RocksDbWalletBuildStore::create_fresh(
        &path,
        Network::ZcashRegtest,
        source,
        0,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let lease = build.try_acquire_lease(
        request(0x11, source, UnixTimestampMillis::new(200)),
        UnixTimestampMillis::new(100),
    )?;
    let other_owner = WalletProjectionBuildOwner::from_bytes([0x22; 16]);
    assert!(matches!(
        build
            .clone()
            .discard_unpublished(other_owner, UnixTimestampMillis::new(199)),
        Err(RocksDbWalletError::WalletProjectionBuildLeaseHeld { holder, .. })
            if holder == lease.owner()
    ));
    build.discard_unpublished(other_owner, UnixTimestampMillis::new(200))?;
    assert!(!path.exists());
    Ok(())
}

#[test]
fn discard_unpublished_admits_the_live_lease_owner_immediately()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let source = source_identity();
    let path = temporary.path().join("wallet");
    let build = RocksDbWalletBuildStore::create_fresh(
        &path,
        Network::ZcashRegtest,
        source,
        0,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let lease = build.try_acquire_lease(
        request(0x11, source, UnixTimestampMillis::new(10_000)),
        UnixTimestampMillis::new(100),
    )?;
    build.discard_unpublished(lease.owner(), UnixTimestampMillis::new(101))?;
    assert!(!path.exists());
    Ok(())
}

fn source_identity() -> WalletCanonicalSourceIdentity {
    WalletCanonicalSourceIdentity::new(
        WalletProjectionSourcePosition::new(
            ChainEpochId::new(1),
            BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x33; 32])),
            1,
        ),
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            1,
            [0x44; 32],
        ),
        BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0x33; 32])),
    )
}

fn request(
    owner: u8,
    source: WalletCanonicalSourceIdentity,
    expires_at: UnixTimestampMillis,
) -> WalletProjectionBuildLeaseRequest {
    WalletProjectionBuildLeaseRequest::new(
        WalletProjectionBuildOwner::from_bytes([owner; 16]),
        source,
        WalletProjectionRetainedEventAnchor::new(1),
        expires_at,
    )
}
