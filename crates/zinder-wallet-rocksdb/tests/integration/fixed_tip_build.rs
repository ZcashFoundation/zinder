use std::{
    fs,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
};

#[cfg(unix)]
use std::os::unix::fs::symlink;

use tempfile::TempDir;
use zinder_core::{
    BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId, CanonicalBlockFacts,
    CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigest,
    CanonicalBlockReplayFormatVersion, CanonicalTransactionFacts, ChainTipMetadata,
    CommitmentTreeCheckpoint, CommitmentTreeFrontiers, CompactBlockArtifact, CompactChainMetadata,
    ConsensusBranchId, LockTime, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
    PrivacyShape, SerializedBytesDigest, TransactionBlobArtifact, TransactionComponentCounts,
    TransactionId, TransactionIntrinsicValueBalances, TransactionLocation, TransactionPublicFacts,
    TransactionVersion, TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint,
    TransparentOutputFact, UnixTimestampMillis, encode_canonical_block_replay,
};
use zinder_store::{
    CANONICAL_STORE_IDENTITY, CANONICAL_STORE_SCHEMA_VERSION, CanonicalBaselinePublication,
    CanonicalBuildBlock, CanonicalEventCursor, CanonicalEventFence, CanonicalEventHistoryRequest,
    CanonicalLiveAppend, CanonicalLiveReplacement, CanonicalReorgPolicy, CanonicalReplacementBlock,
    CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreWorkload, ProjectionBuildAnchor,
    ProjectionBuildLease, ProjectionBuildLeaseId, RawBlobRetention, RocksDbCanonicalBuilder,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::process_crash;
use zinder_wallet_projection::{
    WALLET_PROJECTION_STORE_IDENTITY, WalletAddressTransactionKey, WalletAddressUnspentOutputKey,
    WalletCanonicalSourceIdentity, WalletOutpointKey, WalletProjectionBuildLeaseRequest,
    WalletProjectionBuildOwner, WalletProjectionContractError, WalletProjectionFamilyRowCounts,
    WalletProjectionRetainedEventAnchor, WalletProjectionSourcePosition,
};
use zinder_wallet_rocksdb::{
    RocksDbWalletBuildOptions, RocksDbWalletBuildOutcome, RocksDbWalletBuildStore,
    RocksDbWalletError, RocksDbWalletFollowingStore, RocksDbWalletStore,
    WALLET_ROCKSDB_SCHEMA_VERSION, WalletBuildLeaseHeartbeat, WalletBuildLeasePhase,
    WalletProjectionBuildLeaseExecution, WalletRecoveryAdmissionConfig,
    build_wallet_from_canonical, build_wallet_from_canonical_with_lease_and_heartbeat,
    validate_wallet_projection_pre_promotion_fence,
};

#[test]
fn checkpointed_canonical_history_builds_wallet_from_its_authenticated_first_retained_block()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let checkpoint = CommitmentTreeCheckpoint::new(
        BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32])),
        1,
        CommitmentTreeFrontiers::default(),
    );
    let address = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
    let transaction_two = TransactionId::from_bytes([0x22; 32]);
    let transaction_three = TransactionId::from_bytes([0x33; 32]);
    let block_two_output = TransparentOutPoint::new(transaction_two, 0);
    let blocks = vec![
        block_facts(
            2,
            [0xc1; 32],
            [0xc2; 32],
            vec![transaction_facts(
                transaction_two,
                true,
                vec![TransparentInputFact::new(
                    0,
                    TransparentOutPoint::COINBASE_SENTINEL,
                )],
                vec![TransparentOutputFact::new(0, 7, [0x51], address)],
            )],
        ),
        block_facts(
            3,
            [0xc2; 32],
            [0xc3; 32],
            vec![transaction_facts(
                transaction_three,
                false,
                vec![TransparentInputFact::new(0, block_two_output)],
                vec![TransparentOutputFact::new(0, 5, [0x52], address)],
            )],
        ),
    ];
    let canonical_store =
        build_ready_checkpointed_canonical_store(&temporary, checkpoint, &blocks)?;
    let canonical_ready = canonical_store.ready_evidence();
    let expected_first = block_id(&blocks[0]);
    let expected_tip = block_id(&blocks[1]);
    assert_eq!(canonical_ready.first_retained_block, expected_first);
    assert_eq!(canonical_ready.visible_tip, expected_tip);
    assert_eq!(canonical_ready.visible_block_count, 2);

    let outcome = build_wallet_from_canonical(
        &canonical_store,
        temporary.path().join("wallet-checkpointed"),
        RocksDbWalletBuildOptions::for_local_tests(),
    )?;
    let wallet_ready = outcome.store.ready_evidence();
    let expected_source_digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
        canonical_ready.sequence_digest_version,
        canonical_ready.visible_block_count,
        canonical_ready.visible_sequence_digest,
    );

    assert_eq!(outcome.report.scanned_block_count, 2);
    assert_eq!(wallet_ready.source_position.tip, expected_tip);
    assert_eq!(wallet_ready.source_sequence_digest, expected_source_digest);
    assert_eq!(wallet_ready.source_position, outcome.report.source_position);
    assert_eq!(
        wallet_ready.source_sequence_digest,
        outcome.report.source_sequence_digest
    );
    assert_eq!(
        wallet_ready.projection_digest,
        outcome.report.projection_digest
    );
    assert_eq!(wallet_ready.row_counts, outcome.report.row_counts);
    Ok(())
}

#[test]
fn checkpointed_wallet_build_rejects_spends_whose_outputs_precede_the_retained_floor()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let checkpoint = CommitmentTreeCheckpoint::new(
        BlockId::new(BlockHeight::new(1), BlockHash::from_bytes([0xc1; 32])),
        1,
        CommitmentTreeFrontiers::default(),
    );
    let precheckpoint_output = TransparentOutPoint::new(TransactionId::from_bytes([0x11; 32]), 0);
    let block_two = block_facts(
        2,
        [0xc1; 32],
        [0xc2; 32],
        vec![transaction_facts(
            TransactionId::from_bytes([0x22; 32]),
            false,
            vec![TransparentInputFact::new(0, precheckpoint_output)],
            Vec::new(),
        )],
    );
    let canonical_store =
        build_ready_checkpointed_canonical_store(&temporary, checkpoint, &[block_two])?;

    let result = build_wallet_from_canonical(
        &canonical_store,
        temporary.path().join("wallet-unresolved-prefix"),
        RocksDbWalletBuildOptions::for_local_tests(),
    );

    assert!(matches!(
        result,
        Err(RocksDbWalletError::Contract(
            WalletProjectionContractError::MissingTransparentPredecessor
        ))
    ));
    Ok(())
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the owner-checkpoint round trip keeps both stores and their shared advancement fence in one causal proof"
)]
fn owner_checkpoints_are_cold_admitted_and_remain_exact_after_sources_advance()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let activations = inactive_upgrade_activations()?;
    let mut canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let canonical_before = canonical_store.ready_evidence();
    let wallet_path = temporary.path().join("wallet-checkpoint-source");
    let wallet_outcome = build_wallet_from_canonical(
        &canonical_store,
        &wallet_path,
        RocksDbWalletBuildOptions {
            supported_reorg_depth: 2,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    let wallet_before = wallet_outcome.store.ready_evidence().clone();
    let wallet_source_before = wallet_outcome.report.canonical_source_identity();
    drop(wallet_outcome.store);
    let mut wallet_store = RocksDbWalletStore::open_ready_for_following(
        &wallet_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let canonical_existing_target = temporary.path().join("canonical-existing-target");
    fs::create_dir(&canonical_existing_target)?;
    let canonical_sentinel = canonical_existing_target.join("sentinel");
    fs::write(&canonical_sentinel, b"canonical-target-must-remain")?;
    assert!(matches!(
        canonical_store.create_owner_checkpoint(
            &canonical_existing_target,
            RocksDbResourceBudget::for_local_tests(),
        ),
        Err(CanonicalStoreError::CheckpointTargetExists { ref path })
            if path == &canonical_existing_target
    ));
    assert_eq!(
        fs::read(&canonical_sentinel)?,
        b"canonical-target-must-remain"
    );
    assert_eq!(fs::read_dir(&canonical_existing_target)?.count(), 1);

    let wallet_existing_target = temporary.path().join("wallet-existing-target");
    fs::create_dir(&wallet_existing_target)?;
    let wallet_sentinel = wallet_existing_target.join("sentinel");
    fs::write(&wallet_sentinel, b"wallet-target-must-remain")?;
    assert!(matches!(
        wallet_store.create_owner_checkpoint(
            &wallet_existing_target,
            RocksDbResourceBudget::for_local_tests(),
        ),
        Err(RocksDbWalletError::CheckpointTargetExists { ref path })
            if path == &wallet_existing_target
    ));
    assert_eq!(fs::read(&wallet_sentinel)?, b"wallet-target-must-remain");
    assert_eq!(fs::read_dir(&wallet_existing_target)?.count(), 1);

    let canonical_checkpoint_path = temporary.path().join("canonical-checkpoint");
    let canonical_checkpoint = canonical_store.create_owner_checkpoint(
        &canonical_checkpoint_path,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(
        canonical_checkpoint.store_identity,
        CANONICAL_STORE_IDENTITY
    );
    assert_eq!(
        canonical_checkpoint.schema_version,
        CANONICAL_STORE_SCHEMA_VERSION
    );
    assert_eq!(canonical_checkpoint.ready_evidence, canonical_before);
    assert_eq!(canonical_checkpoint.workload, canonical_store.workload());
    assert_eq!(
        canonical_checkpoint.build_plan,
        *canonical_store.build_plan()
    );

    let wallet_checkpoint_path = temporary.path().join("wallet-checkpoint");
    let wallet_checkpoint = wallet_store.create_owner_checkpoint(
        &wallet_checkpoint_path,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(
        wallet_checkpoint.store_identity,
        WALLET_PROJECTION_STORE_IDENTITY
    );
    assert_eq!(
        wallet_checkpoint.schema_version,
        WALLET_ROCKSDB_SCHEMA_VERSION
    );
    assert_eq!(wallet_checkpoint.network, Network::ZcashRegtest);
    assert_eq!(wallet_checkpoint.ready_evidence, wallet_before);

    let initial_fence = canonical_store.event_fence();
    let appended = block_facts(
        4,
        [0xc3; 32],
        [0xc4; 32],
        vec![transaction_facts(
            TransactionId::from_bytes([0x40; 32]),
            true,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            vec![TransparentOutputFact::new(0, 1, [0x60], fixture.address_a)],
        )],
    );
    let (canonical_store, append_fence) = canonical_store.commit_live_append(
        CanonicalLiveAppend::new(
            initial_fence,
            canonical_build_block(appended, true),
            Vec::new(),
            initial_fence.visible_tip(),
            UnixTimestampMillis::new(1_750_000_000_100),
        ),
        &activations,
    )?;
    assert_ne!(canonical_store.ready_evidence(), canonical_before);

    let secondary =
        open_incremental_secondary(&temporary, "canonical-secondary-checkpoint", &activations)?;
    apply_current_retained_event(
        &mut wallet_store,
        &secondary,
        wallet_source_before,
        append_fence,
    )?;
    assert_ne!(wallet_store.ready_evidence(), &wallet_before);

    let cold_canonical_checkpoint = RocksDbCanonicalStore::open_ready(
        &canonical_checkpoint_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(
        cold_canonical_checkpoint.ready_evidence(),
        canonical_checkpoint.ready_evidence
    );
    assert_eq!(
        cold_canonical_checkpoint.build_plan(),
        &canonical_checkpoint.build_plan
    );
    let cold_wallet_checkpoint = RocksDbWalletStore::open_ready_for_following(
        &wallet_checkpoint_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(
        cold_wallet_checkpoint.ready_evidence(),
        &wallet_checkpoint.ready_evidence
    );
    assert_eq!(
        WalletCanonicalSourceIdentity::from_ready_evidence(cold_wallet_checkpoint.ready_evidence()),
        wallet_source_before
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn owner_checkpoints_refuse_and_preserve_broken_symlink_targets()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let mut canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let wallet_path = temporary.path().join("wallet-checkpoint-source");
    let wallet_outcome = build_wallet_from_canonical(
        &canonical_store,
        &wallet_path,
        RocksDbWalletBuildOptions {
            supported_reorg_depth: 2,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    drop(wallet_outcome.store);
    let mut wallet_store = RocksDbWalletStore::open_ready_for_following(
        &wallet_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let canonical_missing_target = temporary.path().join("canonical-missing-checkpoint");
    let canonical_checkpoint_target = temporary.path().join("canonical-checkpoint-link");
    symlink(&canonical_missing_target, &canonical_checkpoint_target)?;
    assert!(matches!(
        canonical_store.create_owner_checkpoint(
            &canonical_checkpoint_target,
            RocksDbResourceBudget::for_local_tests(),
        ),
        Err(CanonicalStoreError::CheckpointTargetExists { ref path })
            if path == &canonical_checkpoint_target
    ));
    assert_eq!(
        fs::read_link(&canonical_checkpoint_target)?,
        canonical_missing_target
    );
    assert!(
        fs::symlink_metadata(&canonical_checkpoint_target)?
            .file_type()
            .is_symlink()
    );
    assert!(!canonical_missing_target.exists());

    let wallet_missing_target = temporary.path().join("wallet-missing-checkpoint");
    let wallet_checkpoint_target = temporary.path().join("wallet-checkpoint-link");
    symlink(&wallet_missing_target, &wallet_checkpoint_target)?;
    assert!(matches!(
        wallet_store.create_owner_checkpoint(
            &wallet_checkpoint_target,
            RocksDbResourceBudget::for_local_tests(),
        ),
        Err(RocksDbWalletError::CheckpointTargetExists { ref path })
            if path == &wallet_checkpoint_target
    ));
    assert_eq!(
        fs::read_link(&wallet_checkpoint_target)?,
        wallet_missing_target
    );
    assert!(
        fs::symlink_metadata(&wallet_checkpoint_target)?
            .file_type()
            .is_symlink()
    );
    assert!(!wallet_missing_target.exists());
    Ok(())
}

#[test]
fn owner_checkpoint_cold_admission_refuses_a_replaced_same_plan_checkpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;

    let source_path = temporary.path().join("wallet-checkpoint-source");
    let source_outcome = build_wallet_from_canonical(
        &canonical_store,
        &source_path,
        RocksDbWalletBuildOptions {
            supported_reorg_depth: 2,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    let source_ready = source_outcome.store.ready_evidence().clone();
    drop(source_outcome.store);
    let mut source = RocksDbWalletStore::open_ready_for_following(
        &source_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let replacement_path = temporary.path().join("wallet-checkpoint-replacement");
    let replacement_outcome = build_wallet_from_canonical(
        &canonical_store,
        &replacement_path,
        RocksDbWalletBuildOptions {
            supported_reorg_depth: 2,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    let replacement_ready = replacement_outcome.store.ready_evidence().clone();
    assert_eq!(replacement_ready, source_ready);
    drop(replacement_outcome.store);
    let mut replacement = RocksDbWalletStore::open_ready_for_following(
        &replacement_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let candidate = temporary.path().join("candidate");
    fs::create_dir(&candidate)?;
    let target = candidate.join("wallet.rocksdb");
    let admission = source.create_owner_checkpoint_physical(&target)?;
    let replacement_target = temporary.path().join("replacement-checkpoint");
    let _replacement_admission =
        replacement.create_owner_checkpoint_physical(&replacement_target)?;
    fs::remove_dir_all(&target)?;
    fs::rename(&replacement_target, &target)?;

    let error = RocksDbWalletFollowingStore::cold_admit_owner_checkpoint(
        &target,
        &admission,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("replaced wallet checkpoint unexpectedly passed cold admission")?;
    assert!(matches!(
        error,
        RocksDbWalletError::AdmissionChanged {
            reason: "checkpoint database identity differs from the physical owner checkpoint"
        }
    ));
    let preserved = RocksDbWalletStore::open_ready_for_following(
        &target,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(preserved.ready_evidence(), &replacement_ready);
    Ok(())
}

#[test]
fn build_removes_siblings_a_crash_left_before_the_wallet_path_existed()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let abandoned = [
        temporary.path().join("wallet.projection-load-staging"),
        temporary.path().join("wallet.initializing"),
    ];
    for path in &abandoned {
        std::fs::create_dir(path)?;
        std::fs::write(path.join("partial"), [0_u8])?;
    }

    let outcome = build_wallet_from_canonical(
        &canonical_store,
        temporary.path().join("wallet"),
        RocksDbWalletBuildOptions::for_local_tests(),
    )?;

    assert_store(&outcome, &fixture)?;
    for path in &abandoned {
        assert!(!path.exists(), "{} survived the build", path.display());
    }
    Ok(())
}

#[test]
fn fixed_tip_build_matches_exact_version_one_wallet_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;

    let outcome = build_wallet_from_canonical(
        &canonical_store,
        temporary.path().join("wallet"),
        RocksDbWalletBuildOptions {
            max_secondary_sort_memory_bytes_per_sorter: 128,
            supported_reorg_depth: 2,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;

    assert!(
        !temporary
            .path()
            .join("wallet.projection-load-staging")
            .exists()
    );
    assert_report(&outcome);
    assert_store(&outcome, &fixture)?;
    let expected_source = outcome.report.canonical_source_identity();
    drop(outcome.store);
    let reopened = RocksDbWalletStore::open_ready(
        temporary.path().join("wallet"),
        Network::ZcashRegtest,
        expected_source,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(
        reopened.ready_evidence().source_sequence_digest,
        expected_source.source_sequence_digest()
    );
    Ok(())
}

#[test]
fn built_wallet_passes_independent_semantic_rederivation_after_digest_readback()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let wallet_path = temporary.path().join("wallet");
    let outcome = build_wallet_from_canonical(
        &canonical_store,
        &wallet_path,
        RocksDbWalletBuildOptions {
            max_secondary_sort_memory_bytes_per_sorter: 128,
            supported_reorg_depth: 2,
            ..RocksDbWalletBuildOptions::for_local_tests()
        },
    )?;
    let built_evidence = outcome.store.ready_evidence().clone();
    drop(outcome.store);

    let mut owner = RocksDbWalletStore::open_ready_for_following(
        &wallet_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let checkpoint_path = temporary.path().join("wallet-checkpoint");
    owner.create_owner_checkpoint(&checkpoint_path, RocksDbResourceBudget::for_local_tests())?;
    drop(owner);

    let admitted = RocksDbWalletFollowingStore::cold_admit_recovery_checkpoint(
        &checkpoint_path,
        Network::ZcashRegtest,
        WalletRecoveryAdmissionConfig {
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            staging_path: temporary.path(),
            max_sort_memory_bytes_per_sorter: 128,
            max_temporary_file_bytes_per_sorter: 256 * 1024 * 1024,
            max_accounted_reorg_undo_bytes: 16 * 1024 * 1024,
        },
    )?;
    assert_eq!(admitted.ready_evidence, built_evidence);
    Ok(())
}

#[test]
fn closed_canonical_primary_secondary_build_matches_primary_oracle()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let options = RocksDbWalletBuildOptions {
        max_secondary_sort_memory_bytes_per_sorter: 128,
        supported_reorg_depth: 2,
        ..RocksDbWalletBuildOptions::for_local_tests()
    };
    let primary_outcome = build_wallet_from_canonical(
        &canonical_store,
        temporary.path().join("wallet-primary"),
        options,
    )?;
    let primary_report = primary_outcome.report.clone();
    drop(primary_outcome.store);
    drop(canonical_store);

    let secondary = RocksDbCanonicalSecondary::open_ready(
        temporary.path().join("canonical"),
        temporary.path().join("canonical-secondary"),
        &inactive_upgrade_activations()?,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let lease_request = WalletProjectionBuildLeaseRequest::new(
        WalletProjectionBuildOwner::from_bytes([0x77; 16]),
        primary_report.canonical_source_identity(),
        WalletProjectionRetainedEventAnchor::new(1),
        UnixTimestampMillis::new(u64::MAX),
    );
    let mut heartbeat = |_, _| Ok(WalletBuildLeaseHeartbeat::at(UnixTimestampMillis::new(1)));
    let secondary_outcome = build_wallet_from_canonical_with_lease_and_heartbeat(
        &secondary,
        temporary.path().join("wallet-secondary"),
        options,
        WalletProjectionBuildLeaseExecution::new(lease_request, UnixTimestampMillis::new(0)),
        &mut heartbeat,
    )?;

    assert_eq!(
        secondary_outcome.report.source_position,
        primary_report.source_position
    );
    assert_eq!(
        secondary_outcome.report.source_sequence_digest,
        primary_report.source_sequence_digest
    );
    assert_eq!(
        secondary_outcome.report.projection_digest,
        primary_report.projection_digest
    );
    assert_eq!(
        secondary_outcome.report.row_counts,
        primary_report.row_counts
    );
    assert_eq!(
        secondary_outcome.report.utxo_summary,
        primary_report.utxo_summary
    );
    assert_store(&secondary_outcome, &fixture)?;
    Ok(())
}

#[test]
fn cancelled_build_releases_its_exact_lease_for_immediate_discard_and_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let wallet_path = temporary.path().join("wallet-cancelled");
    let options = RocksDbWalletBuildOptions::for_local_tests();
    let mut heartbeat = |phase, _| {
        if phase == WalletBuildLeasePhase::Initialized {
            return Err(RocksDbWalletError::ProjectionBuildCancelled);
        }
        Ok(WalletBuildLeaseHeartbeat::at(UnixTimestampMillis::new(1)))
    };

    assert!(matches!(
        build_wallet_from_canonical_with_lease_and_heartbeat(
            &canonical_store,
            &wallet_path,
            options,
            build_lease_execution(&canonical_store),
            &mut heartbeat,
        ),
        Err(RocksDbWalletError::ProjectionBuildCancelled)
    ));

    RocksDbWalletBuildStore::open(
        &wallet_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?
    .discard_unpublished(
        WalletProjectionBuildOwner::from_bytes([0x22; 16]),
        UnixTimestampMillis::new(1),
    )?;
    assert!(!wallet_path.exists());
    let retry = build_wallet_from_canonical(&canonical_store, &wallet_path, options)?;
    assert_store(&retry, &fixture)?;
    Ok(())
}

#[cfg(unix)]
const KILLED_BUILD_CHILD: &str = "integration::fixed_tip_build::killed_wallet_build_child";
const KILLED_BUILD_OWNER: [u8; 16] = [0x5a; 16];
const KILLED_BUILD_LEASE_DURATION_MILLIS: u64 = 4 * 60 * 60 * 1_000;

/// Runs one projector-shaped construction and aborts at the armed build phase.
///
/// Like `zinder-projector`, it leases the canonical anchor under the build
/// owner's identity before it builds the wallet under the same owner.
#[test]
fn killed_wallet_build_child() -> Result<(), Box<dyn std::error::Error>> {
    let Some(child) = process_crash::crash_child() else {
        return Ok(());
    };
    let canonical_store = reopen_canonical_primary(child.store_path())?;
    let now = UnixTimestampMillis::now();
    let _canonical_lease =
        acquire_owner_canonical_lease(&canonical_store, killed_build_lease_expiry(now), now)?;
    let mut heartbeat = |phase: WalletBuildLeasePhase, _| {
        process_crash::crash_point(&killed_build_crash_point(phase));
        Ok(WalletBuildLeaseHeartbeat::at(UnixTimestampMillis::now()))
    };
    build_wallet_from_canonical_with_lease_and_heartbeat(
        &canonical_store,
        child.store_path().join("wallet"),
        RocksDbWalletBuildOptions::for_local_tests(),
        owned_build_lease_execution(
            &canonical_store,
            WalletProjectionBuildOwner::from_bytes(KILLED_BUILD_OWNER),
            killed_build_lease_expiry(now),
            now,
        ),
        &mut heartbeat,
    )?;
    Err("wallet build returned without reaching its crash point".into())
}

#[cfg(unix)]
#[test]
fn killed_wallet_build_restarts_under_the_same_owner_without_waiting_for_lease_expiry()
-> Result<(), Box<dyn std::error::Error>> {
    let owner = WalletProjectionBuildOwner::from_bytes(KILLED_BUILD_OWNER);
    let other_owner = WalletProjectionBuildOwner::from_bytes([0xa5; 16]);
    for phase in [
        WalletBuildLeasePhase::CanonicalReplayComplete,
        WalletBuildLeasePhase::ProjectionRowsLoaded,
    ] {
        let temporary = TempDir::new()?;
        let fixture = wallet_baseline_fixture();
        drop(build_ready_canonical_store(&temporary, &fixture)?);
        process_crash::run_crash_child(
            KILLED_BUILD_CHILD,
            temporary.path(),
            &killed_build_crash_point(phase),
        )?;

        let canonical_store = reopen_canonical_primary(temporary.path())?;
        let now = UnixTimestampMillis::now();
        let crashed_canonical_lease = canonical_store
            .active_projection_build_lease(
                ProjectionBuildLeaseId::from_bytes(KILLED_BUILD_OWNER),
                now,
            )?
            .ok_or("killed build left no live canonical retention lease")?;
        let wallet_path = temporary.path().join("wallet");
        let open_build = || {
            RocksDbWalletBuildStore::open(
                &wallet_path,
                Network::ZcashRegtest,
                RocksDbResourceBudget::for_local_tests(),
            )
        };
        assert!(matches!(
            open_build()?.discard_unpublished(other_owner, now),
            Err(RocksDbWalletError::WalletProjectionBuildLeaseHeld { holder, expires_at })
                if holder == owner && expires_at > now
        ));

        let reclaimed_canonical_lease =
            acquire_owner_canonical_lease(&canonical_store, killed_build_lease_expiry(now), now)?;
        assert!(reclaimed_canonical_lease.generation() > crashed_canonical_lease.generation());
        open_build()?.discard_unpublished(owner, now)?;
        assert!(!wallet_path.exists());
        assert!(
            !temporary
                .path()
                .join("wallet.projection-load-staging")
                .exists()
        );
        let mut heartbeat = |_, _| Ok(WalletBuildLeaseHeartbeat::at(UnixTimestampMillis::now()));
        let rebuilt = build_wallet_from_canonical_with_lease_and_heartbeat(
            &canonical_store,
            &wallet_path,
            RocksDbWalletBuildOptions::for_local_tests(),
            owned_build_lease_execution(
                &canonical_store,
                owner,
                killed_build_lease_expiry(now),
                now,
            ),
            &mut heartbeat,
        )?;
        assert_store(&rebuilt, &fixture)?;
        canonical_store.release_projection_build_lease(reclaimed_canonical_lease)?;
    }
    Ok(())
}

fn killed_build_crash_point(phase: WalletBuildLeasePhase) -> String {
    format!("wallet_projection.build.{phase:?}")
}

fn killed_build_lease_expiry(now: UnixTimestampMillis) -> UnixTimestampMillis {
    UnixTimestampMillis::new(
        now.value()
            .saturating_add(KILLED_BUILD_LEASE_DURATION_MILLIS),
    )
}

fn reopen_canonical_primary(
    root: &std::path::Path,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    Ok(RocksDbCanonicalStore::open_ready(
        root.join("canonical"),
        &inactive_upgrade_activations()?,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

/// Leases the visible canonical event under the build owner's identity, as
/// the projector's construction retention lease does.
fn acquire_owner_canonical_lease(
    canonical_store: &RocksDbCanonicalStore,
    expires_at: UnixTimestampMillis,
    now: UnixTimestampMillis,
) -> Result<ProjectionBuildLease, Box<dyn std::error::Error>> {
    let ready = canonical_store.ready_evidence();
    let anchor = ProjectionBuildAnchor::new(
        ready.visible_epoch,
        CanonicalEventCursor::at(ready.visible_event_sequence)?,
    );
    Ok(canonical_store.acquire_projection_build_lease(
        ProjectionBuildLease::new(
            ProjectionBuildLeaseId::from_bytes(KILLED_BUILD_OWNER),
            anchor,
            expires_at,
        ),
        now,
    )?)
}

#[test]
fn pre_promotion_error_releases_its_exact_lease_for_immediate_discard_and_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let wallet_path = temporary.path().join("wallet-pre-promotion-error");
    let options = RocksDbWalletBuildOptions::for_local_tests();
    let mut heartbeat = |phase, lease| {
        if phase == WalletBuildLeasePhase::BeforePromotion {
            validate_wallet_projection_pre_promotion_fence(&canonical_store, lease)?;
            return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
                reason: "test pre-promotion rejection",
            });
        }
        Ok(WalletBuildLeaseHeartbeat::at(UnixTimestampMillis::new(1)))
    };

    assert!(matches!(
        build_wallet_from_canonical_with_lease_and_heartbeat(
            &canonical_store,
            &wallet_path,
            options,
            build_lease_execution(&canonical_store),
            &mut heartbeat,
        ),
        Err(RocksDbWalletError::CanonicalSourceFenceMismatch { .. })
    ));

    RocksDbWalletBuildStore::open(
        &wallet_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?
    .discard_unpublished(
        WalletProjectionBuildOwner::from_bytes([0x22; 16]),
        UnixTimestampMillis::new(1),
    )?;
    assert!(!wallet_path.exists());
    let retry = build_wallet_from_canonical(&canonical_store, &wallet_path, options)?;
    assert_store(&retry, &fixture)?;
    Ok(())
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the end-to-end follower test keeps cancellation, collapsed reconciliation, and cold-build equality in one causal scenario"
)]
fn incremental_cancellation_and_collapsed_reconciliation_match_a_cold_wallet_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let activations = inactive_upgrade_activations()?;
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let options = RocksDbWalletBuildOptions {
        supported_reorg_depth: 2,
        ..RocksDbWalletBuildOptions::for_local_tests()
    };
    let wallet_path = temporary.path().join("wallet-incremental");
    let initial = build_wallet_from_canonical(&canonical_store, &wallet_path, options)?;
    let initial_source = initial.report.canonical_source_identity();
    let wallet = initial.store;
    let initial_fence = canonical_store.event_fence();
    let appended = block_facts(
        4,
        [0xc3; 32],
        [0xc4; 32],
        vec![
            transaction_facts(
                TransactionId::from_bytes([0x40; 32]),
                true,
                vec![TransparentInputFact::new(
                    0,
                    TransparentOutPoint::COINBASE_SENTINEL,
                )],
                vec![TransparentOutputFact::new(0, 1, [0x60], fixture.address_a)],
            ),
            transaction_facts(
                TransactionId::from_bytes([0x41; 32]),
                false,
                vec![TransparentInputFact::new(
                    0,
                    fixture.final_secondary_unspent,
                )],
                vec![TransparentOutputFact::new(0, 4, [0x61], fixture.address_b)],
            ),
        ],
    );
    let (canonical_store, append_fence) = canonical_store.commit_live_append(
        CanonicalLiveAppend::new(
            initial_fence,
            canonical_build_block(appended, true),
            Vec::new(),
            initial_fence.visible_tip(),
            UnixTimestampMillis::new(1_750_000_000_001),
        ),
        &activations,
    )?;
    drop(wallet);
    let mut wallet = RocksDbWalletStore::open_ready_for_following(
        &wallet_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(
        zinder_wallet_projection::WalletCanonicalSourceIdentity::from_ready_evidence(
            wallet.ready_evidence(),
        ),
        initial_source
    );
    let secondary =
        open_incremental_secondary(&temporary, "canonical-secondary-append", &activations)?;
    let append_event = retained_event_for_fence(&secondary, append_fence)?;
    let before_cancellation = wallet.ready_evidence().clone();
    assert!(matches!(
        wallet.apply_canonical_event_range(
            initial_source,
            append_event,
            append_fence,
            secondary.ready_evidence().sequence_checkpoint.through(),
            NonZeroU64::MIN,
            secondary.scan_canonical_replay_range(append_event.committed_range())?,
        ),
        Err(RocksDbWalletError::TransitionLogicalByteLimit { .. })
    ));
    assert_eq!(wallet.ready_evidence(), &before_cancellation);
    assert!(matches!(
        wallet.apply_canonical_event_range_cancellable(
            initial_source,
            append_event,
            append_fence,
            secondary.ready_evidence().sequence_checkpoint.through(),
            transition_logical_byte_limit(),
            secondary.scan_canonical_replay_range(append_event.committed_range())?,
            || true,
        ),
        Err(RocksDbWalletError::ProjectionTransitionCancelled)
    ));
    assert_eq!(wallet.ready_evidence(), &before_cancellation);
    drop(secondary);

    let replacement = block_facts(
        4,
        [0xc3; 32],
        [0xd4; 32],
        vec![
            transaction_facts(
                TransactionId::from_bytes([0x43; 32]),
                true,
                vec![TransparentInputFact::new(
                    0,
                    TransparentOutPoint::COINBASE_SENTINEL,
                )],
                vec![TransparentOutputFact::new(0, 2, [0x63], fixture.address_b)],
            ),
            transaction_facts(
                TransactionId::from_bytes([0x42; 32]),
                false,
                vec![TransparentInputFact::new(0, fixture.final_primary_unspent)],
                vec![TransparentOutputFact::new(0, 9, [0x62], fixture.address_a)],
            ),
        ],
    );
    let (canonical_store, reorg_fence) = canonical_store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            append_fence,
            vec![CanonicalReplacementBlock::new(
                canonical_build_block(replacement, true),
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_002),
        ),
        &activations,
    )?;
    let secondary =
        open_incremental_secondary(&temporary, "canonical-secondary-reorg", &activations)?;
    let retained_events = retained_events_after(&secondary, initial_source)?;
    assert_eq!(retained_events.len(), 2);
    assert_eq!(retained_events[0].resulting_fence(), append_fence);
    assert_eq!(retained_events[1].resulting_fence(), reorg_fence);
    let replay_range = BlockHeightRange::inclusive(BlockHeight::new(4), BlockHeight::new(4));
    assert_eq!(retained_events[1].committed_range(), replay_range);
    wallet.reconcile_canonical_event_sequence(
        initial_source,
        &retained_events,
        reorg_fence,
        secondary.ready_evidence().sequence_checkpoint.through(),
        None,
        replay_range,
        transition_logical_byte_limit(),
        secondary.scan_canonical_replay_range(replay_range)?,
    )?;
    assert_eq!(
        wallet.ready_evidence().source_position.tip,
        reorg_fence.visible_tip()
    );
    let source_after_collapsed_reconciliation =
        WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    drop(secondary);

    // The follower now contains the first replacement's height-four undo row.
    // Replacing that current suffix exercises direct reconciliation with a
    // verified non-empty durable rollback rather than an append-only replay.
    let second_replacement = block_facts(
        4,
        [0xc3; 32],
        [0xe4; 32],
        vec![transaction_facts(
            TransactionId::from_bytes([0x44; 32]),
            true,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            vec![TransparentOutputFact::new(0, 6, [0x64], fixture.address_a)],
        )],
    );
    let (canonical_store, second_reorg_fence) = canonical_store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            reorg_fence,
            vec![CanonicalReplacementBlock::new(
                canonical_build_block(second_replacement, true),
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_003),
        ),
        &activations,
    )?;
    let secondary =
        open_incremental_secondary(&temporary, "canonical-secondary-second-reorg", &activations)?;
    let retained_events = retained_events_after(&secondary, source_after_collapsed_reconciliation)?;
    assert_eq!(retained_events.len(), 1);
    assert_eq!(retained_events[0].resulting_fence(), second_reorg_fence);
    let replacement_undo = wallet
        .find_reorg_undo(BlockHeight::new(4))?
        .ok_or("replacement undo must exist before the second reorg")?;
    assert!(
        replacement_undo
            .spent_outpoints
            .contains(&WalletOutpointKey::new(fixture.final_primary_unspent))
    );
    wallet.reconcile_canonical_event_sequence(
        source_after_collapsed_reconciliation,
        &retained_events,
        second_reorg_fence,
        secondary.ready_evidence().sequence_checkpoint.through(),
        Some(replay_range),
        replay_range,
        transition_logical_byte_limit(),
        secondary.scan_canonical_replay_range(replay_range)?,
    )?;
    assert_eq!(
        wallet.ready_evidence().source_position.tip,
        second_reorg_fence.visible_tip()
    );

    let cold = build_wallet_from_canonical(
        &secondary,
        temporary.path().join("wallet-cold-reorg"),
        options,
    )?;
    let wallet = wallet.into_ready_store(cold.report.canonical_source_identity())?;
    assert_eq!(wallet.ready_evidence(), cold.store.ready_evidence());
    assert!(
        wallet
            .find_unspent_output(fixture.final_secondary_unspent)?
            .is_some()
    );
    let restored_primary = wallet
        .find_unspent_output(fixture.final_primary_unspent)?
        .ok_or("second reorg must restore the prior unspent output")?;
    assert!(
        wallet
            .find_spent_output(fixture.final_primary_unspent)?
            .is_none()
    );
    assert_eq!(
        wallet.find_unspent_output_by_address_key(WalletAddressUnspentOutputKey::new(
            &restored_primary,
        ))?,
        Some(restored_primary)
    );
    assert_eq!(wallet.address_balance(fixture.address_a)?, 18);
    assert_eq!(wallet.address_balance(fixture.address_b)?, 8);
    drop(canonical_store);
    Ok(())
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "the end-to-end settlement test keeps suffix shrink, repeated reorg, reopen, and cold-build equality in one causal scenario"
)]
fn shortened_maximum_depth_replacements_keep_a_usable_suffix_and_match_a_cold_rebuild()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let activations = inactive_upgrade_activations()?;
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let options = RocksDbWalletBuildOptions {
        supported_reorg_depth: 2,
        ..RocksDbWalletBuildOptions::for_local_tests()
    };
    let wallet_path = temporary.path().join("wallet-shortened-replacement");
    let initial = build_wallet_from_canonical(&canonical_store, &wallet_path, options)?;
    let initial_source = initial.report.canonical_source_identity();
    drop(initial.store);
    let mut wallet = RocksDbWalletStore::open_ready_for_following(
        &wallet_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;

    let initial_fence = canonical_store.event_fence();
    let appended_four = block_facts(
        4,
        [0xc3; 32],
        [0xc4; 32],
        vec![transaction_facts(
            TransactionId::from_bytes([0x70; 32]),
            true,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            vec![TransparentOutputFact::new(0, 4, [0x70], fixture.address_a)],
        )],
    );
    let (canonical_store, appended_four_fence) = canonical_store.commit_live_append(
        CanonicalLiveAppend::new(
            initial_fence,
            canonical_build_block(appended_four, true),
            Vec::new(),
            initial_fence.visible_tip(),
            UnixTimestampMillis::new(1_750_000_000_010),
        ),
        &activations,
    )?;
    let secondary = open_incremental_secondary(
        &temporary,
        "canonical-secondary-shortened-append-four",
        &activations,
    )?;
    apply_current_retained_event(&mut wallet, &secondary, initial_source, appended_four_fence)?;
    assert_eq!(
        wallet.ready_evidence().settled_tip,
        initial_fence.visible_tip()
    );
    assert_eq!(wallet.ready_evidence().row_counts.reorg_undo_count, 1);
    let source_after_four =
        WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    drop(secondary);

    let appended_five = block_facts(
        5,
        [0xc4; 32],
        [0xc5; 32],
        vec![transaction_facts(
            TransactionId::from_bytes([0x71; 32]),
            true,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            vec![TransparentOutputFact::new(0, 5, [0x71], fixture.address_b)],
        )],
    );
    let (canonical_store, appended_five_fence) = canonical_store.commit_live_append(
        CanonicalLiveAppend::new(
            appended_four_fence,
            canonical_build_block(appended_five, true),
            Vec::new(),
            initial_fence.visible_tip(),
            UnixTimestampMillis::new(1_750_000_000_011),
        ),
        &activations,
    )?;
    let secondary = open_incremental_secondary(
        &temporary,
        "canonical-secondary-shortened-append-five",
        &activations,
    )?;
    apply_current_retained_event(
        &mut wallet,
        &secondary,
        source_after_four,
        appended_five_fence,
    )?;
    assert_eq!(
        wallet.ready_evidence().settled_tip,
        initial_fence.visible_tip()
    );
    assert_eq!(wallet.ready_evidence().row_counts.reorg_undo_count, 2);
    let source_after_five =
        WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    drop(secondary);

    // Canonical may shrink a maximum-depth suffix. The retained prefix through
    // the settled tip is pruned atomically, leaving exactly the new unsettled
    // suffix rather than an absolute-height-sized undo window.
    let first_shortened = block_facts(
        4,
        [0xc3; 32],
        [0xd4; 32],
        vec![transaction_facts(
            TransactionId::from_bytes([0x72; 32]),
            true,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            vec![TransparentOutputFact::new(0, 6, [0x72], fixture.address_a)],
        )],
    );
    let (canonical_store, first_shortened_fence) = canonical_store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            appended_five_fence,
            vec![CanonicalReplacementBlock::new(
                canonical_build_block(first_shortened, true),
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_012),
        ),
        &activations,
    )?;
    let secondary = open_incremental_secondary(
        &temporary,
        "canonical-secondary-first-shortened-replacement",
        &activations,
    )?;
    apply_current_retained_event(
        &mut wallet,
        &secondary,
        source_after_five,
        first_shortened_fence,
    )?;
    assert_eq!(wallet.ready_evidence().row_counts.reorg_undo_count, 1);
    assert_eq!(
        wallet.ready_evidence().settled_tip,
        initial_fence.visible_tip()
    );
    assert!(wallet.find_reorg_undo(BlockHeight::new(4))?.is_some());
    assert!(wallet.find_reorg_undo(BlockHeight::new(3))?.is_none());
    let source_after_first_shortening =
        WalletCanonicalSourceIdentity::from_ready_evidence(wallet.ready_evidence());
    drop(secondary);

    // A later reorg wholly inside that retained suffix remains incremental.
    let repeated_shortened = block_facts(
        4,
        [0xc3; 32],
        [0xe4; 32],
        vec![transaction_facts(
            TransactionId::from_bytes([0x73; 32]),
            true,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            vec![TransparentOutputFact::new(0, 7, [0x73], fixture.address_b)],
        )],
    );
    let (canonical_store, repeated_shortened_fence) = canonical_store.commit_live_replacement(
        CanonicalLiveReplacement::new(
            first_shortened_fence,
            vec![CanonicalReplacementBlock::new(
                canonical_build_block(repeated_shortened, true),
                Vec::new(),
            )],
            UnixTimestampMillis::new(1_750_000_000_013),
        ),
        &activations,
    )?;
    let secondary = open_incremental_secondary(
        &temporary,
        "canonical-secondary-repeated-shortened-replacement",
        &activations,
    )?;
    apply_current_retained_event(
        &mut wallet,
        &secondary,
        source_after_first_shortening,
        repeated_shortened_fence,
    )?;
    assert_eq!(wallet.ready_evidence().row_counts.reorg_undo_count, 1);
    assert_eq!(
        wallet.ready_evidence().settled_tip,
        initial_fence.visible_tip()
    );
    let expected_ready_evidence = wallet.ready_evidence().clone();
    drop(wallet);
    let wallet = RocksDbWalletStore::open_ready_for_following(
        &wallet_path,
        Network::ZcashRegtest,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(wallet.ready_evidence(), &expected_ready_evidence);
    let cold_after_repeated = build_wallet_from_canonical(
        &secondary,
        temporary
            .path()
            .join("wallet-cold-after-repeated-shortening"),
        options,
    )?;
    assert_eq!(
        wallet.ready_evidence(),
        cold_after_repeated.store.ready_evidence()
    );
    drop(cold_after_repeated.store);
    drop(secondary);
    drop(canonical_store);
    Ok(())
}

#[test]
fn ready_canonical_store_serves_wallet_artifacts() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let fixture = wallet_baseline_fixture();
    let canonical_store = build_ready_canonical_store(&temporary, &fixture)?;
    let tip = block_id(&fixture.blocks[2]);

    let chain_epoch = canonical_store.chain_epoch()?;
    assert_eq!(
        chain_epoch.id,
        canonical_store.event_fence().chain_epoch_id()
    );
    assert_eq!(chain_epoch.visible_tip_height, tip.height);
    assert_eq!(chain_epoch.visible_tip_hash, tip.hash);

    let compact_block = canonical_store
        .compact_block_at(tip.height)?
        .ok_or("tip compact block must be present")?;
    assert_eq!(compact_block.height(), tip.height);
    assert_eq!(compact_block.block_hash(), tip.hash);

    let transaction_id = fixture.blocks[2].transactions[0]
        .public_facts
        .transaction_id;
    let transaction_location = canonical_store
        .transaction_location(transaction_id)?
        .ok_or("transaction location must be present")?;
    assert_eq!(transaction_location.block_height, tip.height);
    let transaction_blob = canonical_store
        .transaction_blob(transaction_location)?
        .ok_or("transaction blob must be present")?;
    assert_eq!(transaction_blob.location, transaction_location);

    let checkpoint = canonical_store
        .tree_state_checkpoint_at_or_before(tip.height)?
        .ok_or("tip tree-state checkpoint must be present")?;
    assert_eq!(checkpoint.block_id, tip);
    Ok(())
}

fn build_ready_canonical_store(
    temporary: &TempDir,
    fixture: &WalletBaselineFixture,
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let tip = block_id(&fixture.blocks[2]);
    let build_plan = CanonicalStoreBuildPlan::complete(
        &inactive_upgrade_activations()?,
        0,
        tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        temporary.path().join("canonical"),
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let build_blocks = fixture
        .blocks
        .iter()
        .enumerate()
        .map(|(index, facts)| canonical_build_block(facts.clone(), index == 2))
        .map(Ok::<_, std::io::Error>);
    builder.bulk_load_blocks(build_blocks)?;
    builder.load_subtree_roots(std::iter::empty())?;
    let tip_checkpoint = CommitmentTreeCheckpoint::new(tip, 3, CommitmentTreeFrontiers::default());
    builder.confirm_source_tip_checkpoint(&tip_checkpoint)?;
    let validated = builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        tip,
        UnixTimestampMillis::new(1_750_000_000_000),
    ))?;
    Ok(validated.publish_baseline(publication)?)
}

fn build_ready_checkpointed_canonical_store(
    temporary: &TempDir,
    checkpoint: CommitmentTreeCheckpoint,
    blocks: &[CanonicalBlockFacts],
) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
    let tip = blocks
        .last()
        .map(block_id)
        .ok_or("test blocks must not be empty")?;
    let build_plan = CanonicalStoreBuildPlan::checkpointed(
        &inactive_upgrade_activations()?,
        checkpoint,
        tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let mut builder = RocksDbCanonicalBuilder::create_fresh(
        temporary.path().join("canonical-checkpointed"),
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let last_index = blocks.len().saturating_sub(1);
    let build_blocks = blocks
        .iter()
        .enumerate()
        .map(|(index, facts)| canonical_build_block(facts.clone(), index == last_index))
        .map(Ok::<_, std::io::Error>);
    builder.bulk_load_blocks(build_blocks)?;
    builder.load_subtree_roots(std::iter::empty())?;
    let tip_checkpoint =
        CommitmentTreeCheckpoint::new(tip, tip.height.value(), CommitmentTreeFrontiers::default());
    builder.confirm_source_tip_checkpoint(&tip_checkpoint)?;
    let validated = builder.prepare_cold_certified_publication()?;
    let publication = validated.prepare_baseline(CanonicalBaselinePublication::new(
        tip,
        UnixTimestampMillis::new(1_750_000_000_000),
    ))?;
    Ok(validated.publish_baseline(publication)?)
}

fn open_incremental_secondary(
    temporary: &TempDir,
    secondary_name: &str,
    activations: &NetworkUpgradeActivations,
) -> Result<RocksDbCanonicalSecondary, Box<dyn std::error::Error>> {
    Ok(RocksDbCanonicalSecondary::open_ready(
        temporary.path().join("canonical"),
        temporary.path().join(secondary_name),
        activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )?)
}

fn retained_event_for_fence(
    secondary: &RocksDbCanonicalSecondary,
    fence: CanonicalEventFence,
) -> Result<zinder_store::CanonicalRetainedEvent, Box<dyn std::error::Error>> {
    secondary
        .canonical_event_history(CanonicalEventHistoryRequest::new(
            None,
            NonZeroU32::new(16).ok_or("canonical event test page limit must be non-zero")?,
        ))?
        .into_iter()
        .find(|event| event.cursor().event_sequence() == fence.chain_event_sequence())
        .ok_or_else(|| "retained canonical event for test fence is absent".into())
}

fn retained_events_after(
    secondary: &RocksDbCanonicalSecondary,
    source: WalletCanonicalSourceIdentity,
) -> Result<Vec<zinder_store::CanonicalRetainedEvent>, Box<dyn std::error::Error>> {
    let cursor = source.source_position().event_cursor.as_bytes();
    Ok(
        secondary.canonical_event_history(CanonicalEventHistoryRequest::new(
            Some(&cursor),
            NonZeroU32::new(16).ok_or("canonical event test page limit must be non-zero")?,
        ))?,
    )
}

fn apply_current_retained_event(
    wallet: &mut RocksDbWalletFollowingStore,
    secondary: &RocksDbCanonicalSecondary,
    expected_source: WalletCanonicalSourceIdentity,
    fence: CanonicalEventFence,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = retained_event_for_fence(secondary, fence)?;
    let replay_range = event.committed_range();
    wallet.apply_canonical_event_range(
        expected_source,
        event,
        fence,
        secondary.ready_evidence().sequence_checkpoint.through(),
        transition_logical_byte_limit(),
        secondary.scan_canonical_replay_range(replay_range)?,
    )?;
    Ok(())
}

fn transition_logical_byte_limit() -> NonZeroU64 {
    NonZeroU64::new(512 * 1024 * 1024).unwrap_or(NonZeroU64::MIN)
}

fn build_lease_execution(
    canonical_store: &RocksDbCanonicalStore,
) -> WalletProjectionBuildLeaseExecution {
    owned_build_lease_execution(
        canonical_store,
        WalletProjectionBuildOwner::from_bytes([0x91; 16]),
        UnixTimestampMillis::new(u64::MAX),
        UnixTimestampMillis::new(0),
    )
}

fn owned_build_lease_execution(
    canonical_store: &RocksDbCanonicalStore,
    owner: WalletProjectionBuildOwner,
    expires_at: UnixTimestampMillis,
    now: UnixTimestampMillis,
) -> WalletProjectionBuildLeaseExecution {
    let ready = canonical_store.ready_evidence();
    let source = WalletCanonicalSourceIdentity::new(
        WalletProjectionSourcePosition::new(
            ready.visible_epoch,
            ready.visible_tip,
            ready.visible_event_sequence,
        ),
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            ready.sequence_digest_version,
            ready.visible_block_count,
            ready.visible_sequence_digest,
        ),
        ready.sequence_checkpoint.through(),
    );
    let lease_request = WalletProjectionBuildLeaseRequest::new(
        owner,
        source,
        WalletProjectionRetainedEventAnchor::new(ready.visible_event_sequence),
        expires_at,
    );
    WalletProjectionBuildLeaseExecution::new(lease_request, now)
}

fn assert_report(outcome: &RocksDbWalletBuildOutcome) {
    assert_eq!(
        outcome.report.row_counts,
        WalletProjectionFamilyRowCounts {
            transparent_unspent_output_count: 4,
            transparent_unspent_output_by_address_count: 4,
            transparent_spent_output_count: 2,
            transparent_address_transaction_count: 6,
            transparent_address_balance_count: 2,
            reorg_undo_count: 0,
        }
    );
    assert_eq!(
        outcome.report.settled_tip,
        outcome.report.source_position.tip
    );
    assert_eq!(outcome.report.utxo_summary.utxo_count, 4);
    assert_eq!(outcome.report.utxo_summary.total_value_zat, 20);
    assert_eq!(
        outcome.report.projection_digest.as_bytes(),
        [
            0x00, 0x68, 0xe0, 0x28, 0x2d, 0x7f, 0x1e, 0xd5, 0x05, 0x78, 0xf5, 0x29, 0x5c, 0x9e,
            0x12, 0x04, 0x48, 0x4d, 0x2b, 0x41, 0xe9, 0x8b, 0x5d, 0x72, 0x2d, 0x52, 0x21, 0xc9,
            0xa5, 0x8b, 0x7d, 0xeb,
        ]
    );
    assert_eq!(outcome.report.scanned_block_count, 3);
    assert_eq!(outcome.report.scanned_transaction_count, 4);
    assert_eq!(outcome.report.staged_output_count, 6);
    assert_eq!(outcome.report.staged_spend_count, 2);
    assert_eq!(outcome.report.historical_prevout_read_count, 0);
    assert!(outcome.report.logical_row_bytes > 0);
    assert!(outcome.report.sst_file_count > 0);
    assert!(outcome.report.sst_file_bytes > 0);
    assert_eq!(
        outcome
            .report
            .cold_validation_peak_accounted_reorg_undo_bytes,
        0
    );
    assert!(
        outcome
            .report
            .cold_validation_peak_accounted_reorg_undo_bytes
            <= outcome
                .report
                .cold_validation_max_accounted_reorg_undo_bytes
    );
    assert_eq!(outcome.report.cold_validation_random_read_count, 0);
    let phases = outcome.report.phase_durations;
    let validation_phases = phases.cold_validation_phases;
    for scan in [
        validation_phases.primary_output_scan,
        validation_phases.address_index_and_balance_readback,
        validation_phases.address_transaction_readback,
        validation_phases.reorg_undo_readback,
    ] {
        assert!(scan <= phases.cold_validation);
    }
    let measured_phase_total = phases.store_initialization
        + phases.canonical_scan
        + phases.outpoint_sort
        + phases.outpoint_merge
        + phases.secondary_row_derivation
        + phases.logical_evidence
        + phases.row_load
        + phases.flush_and_cold_reopen
        + phases.cold_validation
        + phases.ready_publication;
    assert!(measured_phase_total <= phases.total);
    assert!(!phases.total.is_zero());
}

fn assert_store(
    outcome: &RocksDbWalletBuildOutcome,
    fixture: &WalletBaselineFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(outcome.store.address_balance(fixture.address_a)?, 12);
    assert_eq!(outcome.store.address_balance(fixture.address_b)?, 8);

    for outpoint in [
        fixture.left_unspent,
        fixture.block_two_unspent,
        fixture.final_primary_unspent,
        fixture.final_secondary_unspent,
    ] {
        assert!(outcome.store.find_unspent_output(outpoint)?.is_some());
    }
    for outpoint in [fixture.later_spent, fixture.same_block_spent] {
        assert!(outcome.store.find_spent_output(outpoint)?.is_some());
    }
    let left_unspent = outcome
        .store
        .find_unspent_output(fixture.left_unspent)?
        .ok_or("left unspent output must exist")?;
    assert_eq!(
        outcome
            .store
            .find_unspent_output_by_address_key(WalletAddressUnspentOutputKey::new(
                &left_unspent
            ))?,
        Some(left_unspent)
    );
    let address_transaction_key =
        WalletAddressTransactionKey::new(fixture.address_a, BlockHeight::new(3), 1);
    assert!(
        outcome
            .store
            .find_address_transaction(address_transaction_key)?
            .is_some()
    );
    assert_eq!(
        outcome.store.ready_evidence().settled_tip,
        outcome.report.source_position.tip
    );
    assert!(
        outcome
            .store
            .find_reorg_undo(BlockHeight::new(3))?
            .is_none()
    );
    assert_address_pages(outcome, fixture)?;
    Ok(())
}

fn assert_address_pages(
    outcome: &RocksDbWalletBuildOutcome,
    fixture: &WalletBaselineFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let address_a_outputs = collect_address_unspent_outputs(
        &outcome.store,
        fixture.address_a,
        NonZeroU16::new(1).ok_or("page size must be non-zero")?,
    )?;
    assert_eq!(
        address_a_outputs
            .iter()
            .map(WalletAddressUnspentOutputKey::new)
            .collect::<Vec<_>>(),
        vec![
            WalletAddressUnspentOutputKey::new(
                &outcome
                    .store
                    .find_unspent_output(fixture.left_unspent)?
                    .ok_or("left output must remain unspent")?,
            ),
            WalletAddressUnspentOutputKey::new(
                &outcome
                    .store
                    .find_unspent_output(fixture.final_primary_unspent)?
                    .ok_or("final primary output must remain unspent")?,
            ),
        ]
    );

    let address_a_history = collect_address_transaction_history(
        &outcome.store,
        fixture.address_a,
        NonZeroU16::new(1).ok_or("page size must be non-zero")?,
    )?;
    assert_eq!(
        address_a_history
            .iter()
            .map(|transaction| transaction.key)
            .collect::<Vec<_>>(),
        vec![
            WalletAddressTransactionKey::new(fixture.address_a, BlockHeight::new(1), 0),
            WalletAddressTransactionKey::new(fixture.address_a, BlockHeight::new(2), 0),
            WalletAddressTransactionKey::new(fixture.address_a, BlockHeight::new(3), 0),
            WalletAddressTransactionKey::new(fixture.address_a, BlockHeight::new(3), 1),
        ]
    );
    Ok(())
}

fn collect_address_unspent_outputs(
    store: &zinder_wallet_rocksdb::RocksDbWalletStore,
    address_script_hash: TransparentAddressScriptHash,
    page_size: NonZeroU16,
) -> Result<Vec<zinder_wallet_projection::WalletUnspentOutput>, Box<dyn std::error::Error>> {
    let mut outputs = Vec::new();
    let mut after = None;
    loop {
        let page = store.address_unspent_outputs_page(address_script_hash, after, page_size)?;
        outputs.extend(page.outputs);
        let Some(next_page_after) = page.next_page_after else {
            return Ok(outputs);
        };
        after = Some(next_page_after);
    }
}

fn collect_address_transaction_history(
    store: &zinder_wallet_rocksdb::RocksDbWalletStore,
    address_script_hash: TransparentAddressScriptHash,
    page_size: NonZeroU16,
) -> Result<Vec<zinder_wallet_projection::WalletAddressTransaction>, Box<dyn std::error::Error>> {
    let mut transactions = Vec::new();
    let mut after = None;
    loop {
        let page = store.address_transaction_history_page(address_script_hash, after, page_size)?;
        transactions.extend(page.transactions);
        let Some(next_page_after) = page.next_page_after else {
            return Ok(transactions);
        };
        after = Some(next_page_after);
    }
}

struct WalletBaselineFixture {
    blocks: [CanonicalBlockFacts; 3],
    address_a: TransparentAddressScriptHash,
    address_b: TransparentAddressScriptHash,
    left_unspent: TransparentOutPoint,
    later_spent: TransparentOutPoint,
    same_block_spent: TransparentOutPoint,
    block_two_unspent: TransparentOutPoint,
    final_primary_unspent: TransparentOutPoint,
    final_secondary_unspent: TransparentOutPoint,
}

fn wallet_baseline_fixture() -> WalletBaselineFixture {
    let network = Network::ZcashRegtest;
    let address_a = TransparentAddressScriptHash::from_bytes([0xa1; 32]);
    let address_b = TransparentAddressScriptHash::from_bytes([0xb2; 32]);
    let transaction_one = TransactionId::from_bytes([0x11; 32]);
    let transaction_two = TransactionId::from_bytes([0x22; 32]);
    let transaction_three = TransactionId::from_bytes([0x31; 32]);
    let transaction_four = TransactionId::from_bytes([0x32; 32]);
    let left_unspent = TransparentOutPoint::new(transaction_one, 0);
    let later_spent = TransparentOutPoint::new(transaction_one, 1);
    let same_block_spent = TransparentOutPoint::new(transaction_three, 0);
    let block_two_unspent = TransparentOutPoint::new(transaction_two, 0);
    let final_primary_unspent = TransparentOutPoint::new(transaction_four, 0);
    let final_secondary_unspent = TransparentOutPoint::new(transaction_four, 1);

    let block_one = block_facts(
        1,
        network.genesis_hash().as_bytes(),
        [0xc1; 32],
        vec![transaction_facts(
            transaction_one,
            true,
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::COINBASE_SENTINEL,
            )],
            vec![
                TransparentOutputFact::new(0, 11, [0x51], address_a),
                TransparentOutputFact::new(1, 7, [0x52], address_a),
            ],
        )],
    );
    let block_two = block_facts(
        2,
        [0xc1; 32],
        [0xc2; 32],
        vec![transaction_facts(
            transaction_two,
            false,
            vec![TransparentInputFact::new(0, later_spent)],
            vec![TransparentOutputFact::new(0, 5, [0x53], address_b)],
        )],
    );
    let block_three = block_facts(
        3,
        [0xc2; 32],
        [0xc3; 32],
        vec![
            transaction_facts(
                transaction_three,
                false,
                Vec::new(),
                vec![TransparentOutputFact::new(0, 2, [0x54], address_a)],
            ),
            transaction_facts(
                transaction_four,
                false,
                vec![TransparentInputFact::new(0, same_block_spent)],
                vec![
                    TransparentOutputFact::new(0, 1, [0x55], address_a),
                    TransparentOutputFact::new(1, 3, [0x56], address_b),
                ],
            ),
        ],
    );
    WalletBaselineFixture {
        blocks: [block_one, block_two, block_three],
        address_a,
        address_b,
        left_unspent,
        later_spent,
        same_block_spent,
        block_two_unspent,
        final_primary_unspent,
        final_secondary_unspent,
    }
}

fn inactive_upgrade_activations()
-> Result<NetworkUpgradeActivations, zinder_core::NetworkUpgradeActivationsError> {
    let activations = [
        "Overwinter",
        "Sapling",
        "Blossom",
        "Heartwood",
        "Canopy",
        "NU5",
        "NU6",
        "NU6.1",
        "NU6.2",
        "NU6.3",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, name)| NetworkUpgradeActivation {
        branch_id: ConsensusBranchId::new(u32::try_from(index).unwrap_or(u32::MAX) + 1),
        activation_height: BlockHeight::new(100 + u32::try_from(index).unwrap_or(u32::MAX)),
        name: name.to_owned(),
    })
    .collect();
    NetworkUpgradeActivations::new(Network::ZcashRegtest, activations)
}

fn canonical_build_block(facts: CanonicalBlockFacts, is_tip: bool) -> CanonicalBuildBlock {
    let height = facts.block_header.height;
    let block_hash = facts.block_header.block_hash;
    let parent_hash = facts.block_header.parent_hash;
    let compact_block = CompactBlockArtifact::empty(
        BlockId::new(height, block_hash),
        parent_hash,
        u32::try_from(facts.block_header.block_time).unwrap_or(u32::MAX),
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    );
    let transaction_blobs = facts
        .transactions
        .iter()
        .enumerate()
        .map(|(index, transaction)| {
            TransactionBlobArtifact::new(
                TransactionLocation::new(
                    transaction.public_facts.transaction_id,
                    height,
                    block_hash,
                    u32::try_from(index).unwrap_or(u32::MAX),
                ),
                transaction.public_facts.transaction_id.as_bytes(),
            )
        })
        .collect();
    let tree_state_checkpoint = is_tip.then(|| {
        CommitmentTreeCheckpoint::new(
            BlockId::new(height, block_hash),
            u32::try_from(facts.block_header.block_time).unwrap_or(u32::MAX),
            CommitmentTreeFrontiers::default(),
        )
    });
    let replay_envelope = encode_canonical_block_replay(
        &facts,
        CanonicalBlockReplayFormatVersion::V1,
        CanonicalBlockFactsDigestVersion::V1,
    );
    CanonicalBuildBlock {
        facts,
        replay_envelope,
        compact_block,
        tip_metadata: ChainTipMetadata::new(0, 0, 0),
        tree_state_checkpoint,
        block_final_note_commitment_roots: None,
        transaction_blobs,
        block_blob: None,
    }
}

fn block_id(facts: &CanonicalBlockFacts) -> BlockId {
    BlockId::new(facts.block_header.height, facts.block_header.block_hash)
}

fn block_facts(
    height: u32,
    parent_hash: [u8; 32],
    block_hash: [u8; 32],
    transactions: Vec<CanonicalTransactionFacts>,
) -> CanonicalBlockFacts {
    CanonicalBlockFacts {
        block_header: BlockHeaderArtifact::new(
            BlockHeight::new(height),
            BlockHash::from_bytes(block_hash),
            BlockHash::from_bytes(parent_hash),
            [0; 32],
            [0; 32],
            i64::from(height),
            0,
            [0; 32],
            0,
            0,
        ),
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&block_hash),
        transactions,
    }
}

fn transaction_facts(
    transaction_id: TransactionId,
    is_coinbase: bool,
    transparent_inputs: Vec<TransparentInputFact>,
    transparent_outputs: Vec<TransparentOutputFact>,
) -> CanonicalTransactionFacts {
    let counts = TransactionComponentCounts {
        transparent_input_count: u32::try_from(transparent_inputs.len()).unwrap_or(u32::MAX),
        transparent_output_count: u32::try_from(transparent_outputs.len()).unwrap_or(u32::MAX),
        ..TransactionComponentCounts::EMPTY
    };
    CanonicalTransactionFacts {
        public_facts: TransactionPublicFacts {
            transaction_id,
            auth_digest: None,
            wtxid: None,
            version: TransactionVersion::V4,
            consensus_branch_id: None,
            lock_time: LockTime::Unlocked,
            expiry_height: None,
            size_bytes: 32,
            counts,
            orchard_value_balance_zat: None,
            orchard_anchor: None,
            ironwood_value_balance_zat: None,
            privacy_shape: PrivacyShape::Unclassified,
            is_coinbase,
            unsupported_sections: Vec::new(),
        },
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(
            &transaction_id.as_bytes(),
        ),
        intrinsic_value_balances: TransactionIntrinsicValueBalances::default(),
        transparent_inputs,
        transparent_outputs,
    }
}
