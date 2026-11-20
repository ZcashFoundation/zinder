use std::{
    fs,
    path::{Path, PathBuf},
};

use zinder_core::Network;
use zinder_store::RocksDbCanonicalStore;

use crate::state_bundle::StateBundleError;

use super::{
    CANONICAL_CHECKPOINT_DIRECTORY_NAME, RECOVERY_ARCHIVE_MANIFEST_FILE_NAME, RecoveryArchiveError,
    RecoveryValidationBudget, STATE_BUNDLE_MANIFEST_FILE_NAME, WALLET_CHECKPOINT_DIRECTORY_NAME,
    admit_archive_contents, copy_archive_payload, create_private_directory, publish_directory,
    read_archive_manifest, require_absent, require_contract, resolve_existing_directory,
    sync_directory, validate_absolute_lexical_path, with_private_workspace,
};

/// Verifies a private backup against the independently retained SHA-256 of its outer manifest.
///
/// Checks every payload byte and cold-admits both databases against their shared
/// fence. Keep the archive immutable during verification. Temporary validation
/// files use the system temporary directory.
pub fn verify_private_backup(
    archive: &Path,
    network: Network,
    expected_sha256: &[u8; 32],
    validation_budget: RecoveryValidationBudget,
) -> Result<(), RecoveryArchiveError> {
    let archive = resolve_existing_directory(&absolute_path(archive)?, "private backup")?;
    tracing::info!("verifying the private backup inventory");
    let (_, state_bundle) = admit_archive_contents(&archive, network, None, Some(expected_sha256))?;
    tracing::info!("opening the canonical and wallet checkpoints read-only");
    let temporary = std::env::temp_dir();
    let temporary = fs::canonicalize(&temporary)
        .map_err(|source| RecoveryArchiveError::io(&temporary, source))?;
    require_contract(
        !temporary.starts_with(&archive),
        "verification scratch must be outside the backup",
    )?;
    let mut builder = tempfile::Builder::new();
    builder.prefix(".zinder-verify-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o700));
    }
    let scratch = builder
        .tempdir_in(&temporary)
        .map_err(|source| RecoveryArchiveError::io(&temporary, source))?;
    state_bundle.cold_admit_recovery_checkpoints(
        &archive,
        validation_budget.wallet_admission(scratch.path()),
    )?;
    scratch
        .close()
        .map_err(|source| RecoveryArchiveError::io(&temporary, source))
}

/// Installs a private backup as `canonical/` and `wallet/` in one new deployment directory.
///
/// Both databases pass byte verification and cold admission before publication.
/// The installed canonical store retains its cursor key and history and releases
/// the previous projector's retention leases. The caller owns the destination
/// parent, supplies an absent destination, and holds the source archive immutable.
/// Starting the replacement and fencing the previous writer are separate operator actions.
pub fn restore_private_backup(
    archive: &Path,
    destination: &Path,
    network: Network,
    expected_sha256: &[u8; 32],
    validation_budget: RecoveryValidationBudget,
) -> Result<(), RecoveryArchiveError> {
    let archive = resolve_existing_directory(&absolute_path(archive)?, "private backup")?;
    let destination = absolute_path(destination)?;
    validate_absolute_lexical_path(&destination, "private restore destination")?;
    let (Some(parent), Some(name)) = (destination.parent(), destination.file_name()) else {
        return Err(RecoveryArchiveError::contract(
            "restore destination must have a directory name",
        ));
    };
    let destination = resolve_existing_directory(parent, "private restore parent")?.join(name);
    require_absent(&destination, "private restore destination")?;
    require_contract(
        !destination.starts_with(&archive),
        "restore destination must be outside the backup",
    )?;
    let (manifest, encoded) =
        read_archive_manifest(&archive, None, network, Some(expected_sha256))?;
    tracing::info!(
        files = manifest.payload_file_count,
        bytes = manifest.payload_byte_length,
        "copying private backup into the inactive destination"
    );
    with_private_workspace(&destination, "restore", &archive, |workspace| {
        require_absent(&destination, "private restore destination")?;
        let pair = workspace.join("pair");
        create_private_directory(&pair)?;
        let copied = copy_archive_payload(&archive, &pair, Some(&manifest.payload_files))?;
        require_contract(
            copied == manifest.payload_files,
            "backup changed while copying its payload",
        )?;
        let manifest_copy = pair.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME);
        fs::write(&manifest_copy, encoded)
            .map_err(|source| RecoveryArchiveError::io(&manifest_copy, source))?;
        tracing::info!("verifying the installed files and opening both checkpoint databases");
        let (_, state_bundle) = admit_archive_contents(
            &pair,
            network,
            Some(&manifest.candidate_id),
            Some(expected_sha256),
        )?;
        let scratch = workspace.join("validation");
        create_private_directory(&scratch)?;
        let observed = state_bundle
            .cold_admit_recovery_checkpoints(&pair, validation_budget.wallet_admission(&scratch))?;
        let canonical = pair.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME);
        RocksDbCanonicalStore::release_restored_projection_leases(&canonical, &observed)
            .map_err(StateBundleError::CanonicalRecovery)?;
        sync_store_tree(&canonical)?;
        prepare_deployment_directory(&pair)?;
        publish_directory(&pair, &destination)
    })
}

fn sync_store_tree(directory: &Path) -> Result<(), RecoveryArchiveError> {
    let entries =
        fs::read_dir(directory).map_err(|source| RecoveryArchiveError::io(directory, source))?;
    for entry in entries {
        let entry = entry.map_err(|source| RecoveryArchiveError::io(directory, source))?;
        let path = entry.path();
        let kind = entry
            .file_type()
            .map_err(|source| RecoveryArchiveError::io(&path, source))?;
        if kind.is_dir() {
            sync_store_tree(&path)?;
        } else {
            require_contract(
                kind.is_file(),
                "restored canonical store contains a special file",
            )?;
            fs::File::open(&path)
                .and_then(|file| file.sync_all())
                .map_err(|source| RecoveryArchiveError::io(&path, source))?;
        }
    }
    sync_directory(directory)
}

fn absolute_path(path: &Path) -> Result<PathBuf, RecoveryArchiveError> {
    std::path::absolute(path).map_err(|source| RecoveryArchiveError::io(path, source))
}

fn prepare_deployment_directory(pair: &Path) -> Result<(), RecoveryArchiveError> {
    for (checkpoint, store) in [
        (CANONICAL_CHECKPOINT_DIRECTORY_NAME, "canonical"),
        (WALLET_CHECKPOINT_DIRECTORY_NAME, "wallet"),
    ] {
        let store = pair.join(store);
        fs::rename(pair.join(checkpoint), &store)
            .map_err(|source| RecoveryArchiveError::io(&store, source))?;
        sync_directory(&store)?;
    }
    for name in [
        STATE_BUNDLE_MANIFEST_FILE_NAME,
        RECOVERY_ARCHIVE_MANIFEST_FILE_NAME,
    ] {
        let path = pair.join(name);
        fs::remove_file(&path).map_err(|source| RecoveryArchiveError::io(&path, source))?;
    }
    sync_directory(pair)
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        num::{NonZeroU32, NonZeroU64},
    };

    use sha2::{Digest, Sha256};
    use tempfile::TempDir;
    use zinder_core::{
        BlockHeight, MempoolEvictionReason, TransactionId, TransparentAddressScriptHash,
        TransparentOutPoint, TransparentUnspentOutput, UnixTimestampMillis,
    };
    use zinder_rocksdb_bulk_load::BulkLoadError;
    use zinder_store::{
        CanonicalEventHistoryRequest, ChainEventEnvelope, ChainEventHistoryRequest,
        ChainEventStreamFamily, MempoolEvent, MempoolEventEnvelope, MempoolEventHistoryRequest,
        ProjectionBuildLease, ProjectionBuildLeaseId, RocksDbCanonicalSecondary,
        RocksDbResourceBudget,
    };
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, TransparentTestKey,
        sample_regtest_upgrade_activations,
    };
    use zinder_wallet_rocksdb::{
        RocksDbWalletBuildOptions, RocksDbWalletError, RocksDbWalletStore,
        build_wallet_from_canonical,
    };

    use super::*;
    use crate::{
        recovery_archive::{
            PayloadLayout, RecoveryArchiveManifest, admit_recovery_archive, collect_payload_files,
            export_private_backup,
        },
        state_bundle::{StateBundleManifest, prepare_state_bundle_capture},
    };

    fn capture_checkpoints(
        root: &Path,
        owner: &mut RocksDbCanonicalStore,
    ) -> eyre::Result<(PathBuf, [u8; 32])> {
        let wallet_path = root.join("wallet-primary");
        let budget = RocksDbResourceBudget::for_local_tests();
        let built = build_wallet_from_canonical(
            &*owner,
            &wallet_path,
            RocksDbWalletBuildOptions {
                supported_reorg_depth: owner.reorg_policy().reorg_window_blocks(),
                ..RocksDbWalletBuildOptions::for_local_tests()
            },
        )?;
        drop(built.store);
        let mut wallet =
            RocksDbWalletStore::open_ready_for_following(&wallet_path, owner.network(), budget)?;
        let staging = root.join("captures");
        fs::create_dir(&staging)?;
        let paths = prepare_state_bundle_capture(&staging, "backup")?;
        let canonical = owner.create_owner_checkpoint(paths.canonical_checkpoint(), budget)?;
        let wallet = wallet.create_owner_checkpoint(paths.wallet_checkpoint(), budget)?;
        let manifest =
            StateBundleManifest::from_checkpoints(paths.candidate_id(), &canonical, &wallet)?;
        let encoded = serde_json::to_vec_pretty(&manifest)?;
        let capture_digest = Sha256::digest(&encoded).into();
        fs::write(paths.manifest(), encoded)?;
        Ok((staging, capture_digest))
    }

    fn capture_fixture(root: &Path) -> eyre::Result<(PathBuf, [u8; 32])> {
        let chain = ChainFixture::new(Network::ZcashRegtest).extend_blocks(2);
        let credit_block = chain
            .block_at(BlockHeight::new(2))
            .ok_or_else(|| eyre::eyre!("missing credit block"))?;
        let script = TransparentTestKey::from_seed(&[42; 32])?.address_script_bytes();
        let credit = TransparentUnspentOutput::new(
            TransparentAddressScriptHash::of_script_pub_key(&script),
            script,
            TransparentOutPoint::new(TransactionId::from_bytes([1; 32]), 0),
            10,
            credit_block.height,
            credit_block.hash,
        );
        let chain = chain.with_address_output_index(credit);
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&chain).with_settled_tip(BlockHeight::new(1)),
            &sample_regtest_upgrade_activations(),
        )?;
        capture_checkpoints(root, &mut writer.take_canonical_primary()?)
    }

    #[test]
    fn recovery_enforces_the_callers_wallet_validation_budgets() -> eyre::Result<()> {
        let temporary = TempDir::new()?;
        let root = temporary.path();
        let network = Network::ZcashRegtest;
        let (staging, capture_digest) = capture_fixture(root)?;
        let archive = root.join("backup");
        let destination = root.join("restored");
        let sufficient = RecoveryValidationBudget::default();
        let low_memory = RecoveryValidationBudget {
            max_sort_memory_bytes_per_sorter: NonZeroU64::MIN,
            ..sufficient
        };
        let low_spill = RecoveryValidationBudget {
            max_temporary_file_bytes_per_sorter: NonZeroU64::MIN,
            ..sufficient
        };
        let low_undo = RecoveryValidationBudget {
            max_accounted_reorg_undo_bytes: NonZeroU64::MIN,
            ..sufficient
        };
        assert!(matches!(
            export_private_backup(&staging, &archive, network, &capture_digest, low_memory),
            Err(RecoveryArchiveError::StateBundle {
                source: StateBundleError::WalletRecovery(RocksDbWalletError::BulkLoad(
                    BulkLoadError::AccountedMemoryLimit { limit_bytes: 1, .. },
                )),
            })
        ));
        assert!(!archive.exists());
        let digest =
            export_private_backup(&staging, &archive, network, &capture_digest, sufficient)?;
        assert!(matches!(
            export_private_backup(&staging, &archive, network, &capture_digest, low_undo),
            Err(RecoveryArchiveError::StateBundle {
                source: StateBundleError::WalletRecovery(
                    RocksDbWalletError::AccountedReorgUndoMemoryLimit { limit_bytes: 1, .. }
                ),
            })
        ));
        assert!(matches!(
            verify_private_backup(&archive, network, &digest, low_spill),
            Err(RecoveryArchiveError::StateBundle {
                source: StateBundleError::WalletRecovery(RocksDbWalletError::BulkLoad(
                    BulkLoadError::TemporaryFileLimit { limit_bytes: 1, .. },
                )),
            })
        ));
        assert!(matches!(
            restore_private_backup(&archive, &destination, network, &digest, low_undo),
            Err(RecoveryArchiveError::StateBundle {
                source: StateBundleError::WalletRecovery(
                    RocksDbWalletError::AccountedReorgUndoMemoryLimit { limit_bytes: 1, .. }
                ),
            })
        ));
        assert!(!destination.exists());
        verify_private_backup(&archive, network, &digest, sufficient)?;
        restore_private_backup(&archive, &destination, network, &digest, sufficient)?;
        Ok(())
    }

    #[test]
    fn private_backup_requires_the_canonical_construction_manifest() -> eyre::Result<()> {
        let validation_budget = RecoveryValidationBudget::default();
        let temporary = TempDir::new()?;
        let root = temporary.path();
        let (staging, capture_digest) = capture_fixture(root)?;
        fs::remove_file(
            staging
                .join("backup")
                .join(CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join("canonical-construction-manifest.v4.json"),
        )?;
        let archive = root.join("backup");
        assert!(
            export_private_backup(
                &staging,
                &archive,
                Network::ZcashRegtest,
                &capture_digest,
                validation_budget
            )
            .is_err()
        );
        assert!(!archive.exists());
        Ok(())
    }

    #[test]
    fn private_backup_refuses_mismatched_checkpoints_and_retries_after_repair() -> eyre::Result<()>
    {
        let validation_budget = RecoveryValidationBudget::default();
        let temporary = TempDir::new()?;
        let root = temporary.path();
        let network = Network::ZcashRegtest;
        let (staging, capture_digest) = capture_fixture(root)?;
        let source = staging.join("backup");
        let capture = source.join(STATE_BUNDLE_MANIFEST_FILE_NAME);
        let original = fs::read(&capture)?;
        let mut altered: serde_json::Value = serde_json::from_slice(&original)?;
        altered["wallet_checkpoint"]["database_identity_sha256"] = "aa".repeat(32).into();
        let altered = serde_json::to_vec(&altered)?;
        fs::write(&capture, &altered)?;
        let (inner, inner_digest) = StateBundleManifest::read_with_sha256(&source, network)?;
        let archive = root.join("backup");
        assert!(matches!(
            export_private_backup(
                &staging,
                &archive,
                network,
                &inner_digest,
                validation_budget
            ),
            Err(RecoveryArchiveError::StateBundle { .. })
        ));
        assert!(!archive.exists());
        fs::write(&capture, &original)?;
        assert!(
            export_private_backup(&staging, &archive, network, &[0; 32], validation_budget)
                .is_err()
        );
        assert!(!archive.exists());
        let digest = export_private_backup(
            &staging,
            &archive,
            network,
            &capture_digest,
            validation_budget,
        )?;
        assert_eq!(
            digest,
            <[u8; 32]>::from(Sha256::digest(fs::read(
                archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME)
            )?))
        );
        assert_eq!(
            export_private_backup(
                &staging,
                &archive,
                network,
                &capture_digest,
                validation_budget
            )?,
            digest
        );

        // Seal the false identity into the test archive so only cold admission can detect it.
        fs::write(archive.join(STATE_BUNDLE_MANIFEST_FILE_NAME), altered)?;
        let outer = RecoveryArchiveManifest::from_state_bundle(
            &inner,
            hex::encode(inner_digest),
            collect_payload_files(&archive, PayloadLayout::SealedArchive)?,
        )?;
        let encoded = serde_json::to_vec_pretty(&outer)?;
        let digest = Sha256::digest(&encoded).into();
        fs::write(archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME), encoded)?;
        admit_recovery_archive(root, "backup", network)?;
        assert!(matches!(
            verify_private_backup(&archive, network, &digest, validation_budget),
            Err(RecoveryArchiveError::StateBundle { .. })
        ));
        let destination = root.join("mismatched-restore");
        assert!(matches!(
            restore_private_backup(&archive, &destination, network, &digest, validation_budget),
            Err(RecoveryArchiveError::StateBundle { .. })
        ));
        assert!(!destination.exists());
        Ok(())
    }

    fn hold_retention_lease(
        owner: &RocksDbCanonicalStore,
        now: UnixTimestampMillis,
    ) -> eyre::Result<ProjectionBuildLease> {
        let source_event = owner
            .canonical_event_history(CanonicalEventHistoryRequest::new(None, NonZeroU32::MIN))?
            .into_iter()
            .next()
            .ok_or_else(|| eyre::eyre!("missing baseline"))?;
        Ok(owner.acquire_projection_build_lease(
            ProjectionBuildLease::new(
                ProjectionBuildLeaseId::from_bytes([1; 16]),
                source_event.projection_build_anchor(),
                UnixTimestampMillis::new(200),
            ),
            now,
        )?)
    }

    fn assert_new_lease_generation(
        source: &RocksDbCanonicalStore,
        restored: &RocksDbCanonicalStore,
        lease: ProjectionBuildLease,
        now: UnixTimestampMillis,
    ) -> eyre::Result<()> {
        assert_eq!(
            restored.active_projection_build_lease(lease.id(), now)?,
            None
        );
        assert_eq!(
            source.active_projection_build_lease(lease.id(), now)?,
            Some(lease)
        );
        assert!(
            restored
                .acquire_projection_build_lease(lease, now)?
                .generation()
                > lease.generation()
        );
        Ok(())
    }

    fn append_mempool_event(
        source: &RocksDbCanonicalStore,
        byte: u8,
    ) -> eyre::Result<MempoolEventEnvelope> {
        Ok(source.append_mempool_event(
            MempoolEvent::Invalidated {
                transaction_id: TransactionId::from_bytes([byte; 32]),
                reason: MempoolEvictionReason::Conflict,
            },
            UnixTimestampMillis::new(u64::from(byte)),
        )?)
    }

    fn assert_mempool_stops_at_capture(
        source: &RocksDbCanonicalStore,
        restored: &RocksDbCanonicalStore,
        captured: MempoolEventEnvelope,
    ) -> eyre::Result<()> {
        let later = append_mempool_event(source, 2)?;
        assert_eq!(
            restored.mempool_event_history(MempoolEventHistoryRequest::with_default_limit(None))?,
            [captured]
        );
        assert!(
            restored
                .mempool_event_history(MempoolEventHistoryRequest::with_default_limit(Some(
                    &later.cursor
                )))
                .is_err()
        );
        Ok(())
    }

    fn assert_restored_reader_resumes_cursor(
        source: &RocksDbCanonicalStore,
        restored_primary: &Path,
        secondary: &Path,
        captured_history: &[ChainEventEnvelope],
    ) -> eyre::Result<()> {
        let reader = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            restored_primary,
            secondary,
            source.network(),
            source.workload(),
            source.raw_blob_retention(),
            source.reorg_policy(),
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(
            reader.wallet_chain_event_history(ChainEventHistoryRequest::new_for_family(
                Some(&captured_history[0].cursor),
                ChainEventStreamFamily::Visible,
                NonZeroU32::MIN,
            ))?,
            &captured_history[1..]
        );
        Ok(())
    }

    fn assert_corruption_refused(
        archive: &Path,
        network: Network,
        digest: &[u8; 32],
        root: &Path,
    ) -> eyre::Result<()> {
        let validation_budget = RecoveryValidationBudget::default();
        fs::write(
            archive
                .join(WALLET_CHECKPOINT_DIRECTORY_NAME)
                .join("CURRENT"),
            b"damaged",
        )?;
        let refused = root.join("refused");
        assert!(verify_private_backup(archive, network, digest, validation_budget).is_err());
        assert!(
            restore_private_backup(archive, &refused, network, digest, validation_budget).is_err()
        );
        assert!(!refused.exists());
        Ok(())
    }

    #[test]
    fn private_restore_preserves_retained_cursor_history_and_releases_old_build_leases()
    -> eyre::Result<()> {
        let validation_budget = RecoveryValidationBudget::default();
        let temporary = TempDir::new()?;
        let root = temporary.path();
        let network = Network::ZcashRegtest;
        let activations = sample_regtest_upgrade_activations();
        let chain = ChainFixture::new(network).extend_blocks(2);
        let mut writer = CanonicalWriterFixture::publish(
            &CanonicalBaseline::new(&chain).through(BlockHeight::new(1)),
            &activations,
        )?;
        writer.append_live_block(&chain)?;
        let path = writer.canonical_primary_path().to_path_buf();
        let policy = writer.canonical_primary()?.reorg_policy();
        let workload = writer.canonical_primary()?.workload();
        let retention = writer.canonical_primary()?.raw_blob_retention();
        let mut serving = writer.into_serving_store_fixture()?;
        let (reader, _wallet) = serving.take_readers()?;
        let budget = RocksDbResourceBudget::for_local_tests();
        let open_owner = |path: &Path| {
            RocksDbCanonicalStore::open_ready(
                path,
                &activations,
                workload,
                retention,
                policy,
                budget,
            )
        };
        let mut owner = open_owner(&path)?;
        let now = UnixTimestampMillis::new(100);
        let lease = hold_retention_lease(&owner, now)?;
        let mempool = append_mempool_event(&owner, 1)?;
        let request = ChainEventHistoryRequest::new_for_family(
            None,
            ChainEventStreamFamily::Visible,
            NonZeroU32::MAX,
        );
        let history = reader.wallet_chain_event_history(request)?;
        assert_eq!(history.len(), 2);

        let (staging, capture_digest) = capture_checkpoints(root, &mut owner)?;
        let archive = root.join("backup");
        let digest = export_private_backup(
            &staging,
            &archive,
            network,
            &capture_digest,
            validation_budget,
        )?;
        let destination = root.join("restored");
        assert!(
            restore_private_backup(
                &archive,
                &destination,
                Network::ZcashMainnet,
                &digest,
                validation_budget
            )
            .is_err()
        );
        assert!(!destination.exists());
        verify_private_backup(&archive, network, &digest, validation_budget)?;
        restore_private_backup(&archive, &destination, network, &digest, validation_budget)?;
        assert_eq!(fs::read_dir(&destination)?.count(), 2);
        assert!(destination.join("wallet/CURRENT").is_file());
        let canonical = destination.join("canonical");
        let restored = open_owner(&canonical)?;
        assert_new_lease_generation(&owner, &restored, lease, now)?;
        assert_mempool_stops_at_capture(&owner, &restored, mempool)?;
        assert_restored_reader_resumes_cursor(&owner, &canonical, &root.join("after"), &history)?;
        verify_private_backup(&archive, network, &digest, validation_budget)?;
        assert!(matches!(
            restore_private_backup(&archive, &destination, network, &digest, validation_budget),
            Err(RecoveryArchiveError::TargetExists { .. })
        ));

        assert_corruption_refused(&archive, network, &digest, root)
    }

    #[test]
    fn trusted_digest_is_checked_before_decoding_the_outer_manifest() -> Result<(), Box<dyn Error>>
    {
        let validation_budget = RecoveryValidationBudget::default();
        let temporary = TempDir::new()?;
        let archive = temporary.path().join("private");
        fs::create_dir(&archive)?;
        let manifest = b"invalid JSON";
        fs::write(archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME), manifest)?;
        let destination = temporary.path().join("destination");
        let network = Network::ZcashRegtest;
        assert!(matches!(
            verify_private_backup(&archive, network, &[0; 32], validation_budget),
            Err(RecoveryArchiveError::Contract { reason }) if reason.contains("trusted digest")
        ));
        assert!(matches!(
            restore_private_backup(&archive, &destination, network, &[0; 32], validation_budget),
            Err(RecoveryArchiveError::Contract { reason }) if reason.contains("trusted digest")
        ));
        assert!(matches!(
            verify_private_backup(
                &archive,
                network,
                &Sha256::digest(manifest).into(),
                validation_budget
            ),
            Err(RecoveryArchiveError::ManifestDecode { .. })
        ));
        assert!(!destination.exists());
        assert_eq!(fs::read_dir(temporary.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn restore_refuses_existing_directories_and_destinations_inside_the_archive()
    -> Result<(), Box<dyn Error>> {
        let validation_budget = RecoveryValidationBudget::default();
        let temporary = TempDir::new()?;
        let archive = temporary.path().join("private");
        fs::create_dir(&archive)?;
        let destination = temporary.path().join("empty");
        fs::create_dir(&destination)?;
        for target in [&destination, &archive] {
            assert!(matches!(
                restore_private_backup(
                    &archive,
                    target,
                    Network::ZcashRegtest,
                    &[0; 32],
                    validation_budget
                ),
                Err(RecoveryArchiveError::TargetExists { .. })
            ));
        }
        let nested = archive.join("deployment");
        assert!(matches!(
            restore_private_backup(&archive, &nested, Network::ZcashRegtest, &[0; 32], validation_budget),
            Err(RecoveryArchiveError::Contract { reason }) if reason.contains("outside the backup")
        ));
        assert_eq!(fs::read_dir(&archive)?.count(), 0);
        assert_eq!(fs::read_dir(&destination)?.count(), 0);
        Ok(())
    }
}
