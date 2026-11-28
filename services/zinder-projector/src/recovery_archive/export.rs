//! Private export of an authenticated, complete owner capture.

use std::{fs, io, path::Path};

use sha2::{Digest, Sha256};

use zinder_core::Network;
use zinder_wallet_rocksdb::WalletRecoveryAdmissionConfig;

use super::{
    MAX_RECOVERY_ARCHIVE_MANIFEST_BYTES, PayloadLayout, RECOVERY_ARCHIVE_MANIFEST_FILE_NAME,
    RecoveryArchiveError, RecoveryArchiveManifest, RecoveryValidationBudget, StateBundleManifest,
    admit_archive_contents, collect_payload_files, copy_archive_payload, create_private_directory,
    publish_directory, publish_manifest_last, read_bounded_regular_file, require_contract,
    resolve_existing_directory, validate_absolute_lexical_path, validate_archive_root_entries,
    validate_candidate_id, with_private_workspace,
};

/// Validates the proposed archive path and returns its opaque owner capture identifier.
///
/// The archive must have an existing parent disjoint from the capture, and its
/// path must lie outside the configured staging root. Call this before requesting
/// the owner checkpoint. A retry may name an existing private backup.
pub fn validate_private_backup_destination<'path>(
    staging_root: &Path,
    archive: &'path Path,
) -> Result<&'path str, RecoveryArchiveError> {
    validate_absolute_lexical_path(archive, "private backup archive")?;
    let candidate = archive
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            RecoveryArchiveError::contract("private backup archive requires a candidate name")
        })?;
    validate_candidate_id(candidate)?;
    let parent = archive.parent().ok_or_else(|| {
        RecoveryArchiveError::contract("private backup archive requires an existing parent")
    })?;
    let parent = resolve_existing_directory(parent, "private backup archive parent")?;
    super::workspace::require_recovery_output_parent(&parent)?;
    let archive = parent.join(candidate);
    let staging_root = resolve_existing_directory(staging_root, "state-bundle staging root")?;
    require_contract(
        !archive.starts_with(&staging_root) && !staging_root.starts_with(&archive),
        "private backup archive and state-bundle staging root must be disjoint",
    )?;
    match fs::symlink_metadata(&archive) {
        Ok(_) => validate_archive_root_entries(&archive, true)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(RecoveryArchiveError::io(&archive, error)),
    }
    Ok(candidate)
}

/// Exports the completed owner capture named by the archive's last path component.
///
/// Supply the SHA-256 of `state-bundle.json` returned by the authenticated capture.
/// Both copied checkpoints must match its evidence before the new, private directory
/// is published. The returned SHA-256 of the exact `recovery-archive.json` bytes is
/// the recipient's independent verification input. A retry recovers the digest
/// of a published archive after matching its entire inventory to the original
/// capture and repeating admission. Existing destinations are preserved.
pub fn export_private_backup(
    staging_root: &Path,
    archive: &Path,
    network: Network,
    expected_capture_sha256: &[u8; 32],
    validation_budget: RecoveryValidationBudget,
) -> Result<[u8; 32], RecoveryArchiveError> {
    let candidate = validate_private_backup_destination(staging_root, archive)?;
    let source =
        resolve_existing_directory(staging_root, "state-bundle staging root")?.join(candidate);
    let (manifest, capture_digest) = StateBundleManifest::read_with_sha256(&source, network)?;
    require_contract(
        manifest.candidate_id() == candidate && capture_digest == *expected_capture_sha256,
        "state-bundle candidate or manifest digest differs from the owner capture receipt",
    )?;
    with_private_workspace(archive, "backup", &source, |workspace| {
        if archive
            .try_exists()
            .map_err(|error| RecoveryArchiveError::io(archive, error))?
        {
            return readmit_private_export(
                &source,
                archive,
                &manifest,
                expected_capture_sha256,
                validation_budget.wallet_admission(workspace),
            );
        }
        let target = workspace.join("archive");
        create_private_directory(&target)?;
        tracing::info!(
            candidate,
            "copying the captured canonical and wallet checkpoints"
        );
        let copied = copy_archive_payload(&source, &target, None)?;
        let (copied_manifest, copied_digest) =
            StateBundleManifest::read_with_sha256(&target, network)?;
        require_contract(
            copied_digest == *expected_capture_sha256,
            "copied state-bundle manifest SHA-256 differs from the owner capture receipt",
        )?;
        tracing::info!(
            candidate,
            "verifying the private copy before sealing the backup"
        );
        copied_manifest.cold_admit_recovery_checkpoints(
            &target,
            validation_budget.wallet_admission(workspace),
        )?;
        let outer = RecoveryArchiveManifest::from_state_bundle(
            &copied_manifest,
            hex::encode(copied_digest),
            copied,
        )?;
        let digest = publish_manifest_last(&target, &outer)?;
        admit_archive_contents(&target, network, Some(candidate), Some(&digest))?;
        publish_directory(&target, archive)?;
        Ok(digest)
    })
}

fn readmit_private_export(
    source: &Path,
    archive: &Path,
    captured: &StateBundleManifest,
    capture_digest: &[u8; 32],
    wallet_admission: WalletRecoveryAdmissionConfig<'_>,
) -> Result<[u8; 32], RecoveryArchiveError> {
    let encoded = read_bounded_regular_file(
        &archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME),
        "private backup manifest",
        MAX_RECOVERY_ARCHIVE_MANIFEST_BYTES,
    )?;
    let digest = Sha256::digest(&encoded).into();
    let (outer, inner) = admit_archive_contents(
        archive,
        captured.network()?,
        Some(captured.candidate_id()),
        Some(&digest),
    )?;
    require_contract(
        outer.source_state_bundle_manifest_sha256 == hex::encode(capture_digest)
            && outer.payload_files == collect_payload_files(source, PayloadLayout::OwnerCapture)?,
        "existing private backup differs from the authenticated owner capture",
    )?;
    require_private_archive_permissions(archive, &outer)?;
    inner.cold_admit_recovery_checkpoints(archive, wallet_admission)?;
    Ok(digest)
}

#[cfg(unix)]
fn require_private_archive_permissions(
    archive: &Path,
    outer: &RecoveryArchiveManifest,
) -> Result<(), RecoveryArchiveError> {
    for name in [
        "",
        super::CANONICAL_CHECKPOINT_DIRECTORY_NAME,
        super::WALLET_CHECKPOINT_DIRECTORY_NAME,
        RECOVERY_ARCHIVE_MANIFEST_FILE_NAME,
    ]
    .into_iter()
    .chain(outer.payload_files.iter().map(|file| file.path.as_str()))
    {
        require_private_entry_permissions(&archive.join(name))?;
    }
    require_private_segments_permissions(archive)
}

#[cfg(unix)]
fn require_private_segments_permissions(archive: &Path) -> Result<(), RecoveryArchiveError> {
    let segments_root = archive
        .join(super::CANONICAL_CHECKPOINT_DIRECTORY_NAME)
        .join(super::SEGMENTS_DIR_NAME);
    if !require_private_dir_permissions_if_present(&segments_root)? {
        return Ok(());
    }
    for subdir in [
        super::SHIELDED_SEGMENT_DIR,
        super::SUPPLEMENT_SEGMENT_DIR,
        super::RAW_BLOCK_SEGMENT_DIR,
    ] {
        let _ = require_private_dir_permissions_if_present(&segments_root.join(subdir))?;
    }
    Ok(())
}

#[cfg(unix)]
fn require_private_dir_permissions_if_present(path: &Path) -> Result<bool, RecoveryArchiveError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(RecoveryArchiveError::io(path, error)),
    };
    require_contract(
        !metadata.file_type().is_symlink() && metadata.is_dir(),
        "existing private backup segments entry must be a real directory",
    )?;
    require_contract(
        metadata.permissions().mode().trailing_zeros() >= 6,
        "existing private backup has group or other permissions",
    )?;
    Ok(true)
}

#[cfg(unix)]
fn require_private_entry_permissions(path: &Path) -> Result<(), RecoveryArchiveError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata =
        fs::symlink_metadata(path).map_err(|source| RecoveryArchiveError::io(path, source))?;
    require_contract(
        !metadata.file_type().is_symlink(),
        "existing private backup contains a symlink",
    )?;
    require_contract(
        metadata.permissions().mode().trailing_zeros() >= 6,
        "existing private backup has group or other permissions",
    )
}

#[cfg(not(unix))]
fn require_private_archive_permissions(
    _archive: &Path,
    _outer: &RecoveryArchiveManifest,
) -> Result<(), RecoveryArchiveError> {
    Err(RecoveryArchiveError::contract(
        "private archive permissions require a Unix filesystem",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn candidate_preflight_refuses_staging_aliases_and_preserves_foreign_targets()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::TempDir::new()?;
        let staging = root.path().join("staging");
        let staging_alias = root.path().join("alias");
        fs::create_dir(&staging)?;
        std::os::unix::fs::symlink(&staging, &staging_alias)?;
        fs::create_dir(staging.join("checkpoint"))?;
        assert!(
            validate_private_backup_destination(&staging, &staging_alias.join("checkpoint/backup"))
                .is_err()
        );
        assert!(validate_private_backup_destination(&staging, &root.path().join("BAD")).is_err());
        let live_store = root.path().join("canonical");
        fs::create_dir(&live_store)?;
        fs::write(live_store.join("CURRENT"), b"MANIFEST-000001\n")?;
        fs::write(live_store.join("IDENTITY"), b"identity")?;
        assert!(validate_private_backup_destination(&staging, &live_store.join("backup")).is_err());
        assert_eq!(fs::read_dir(&live_store)?.count(), 2);
        let existing = root.path().join("existing");
        fs::write(&existing, b"untouched")?;
        assert!(validate_private_backup_destination(&staging, &existing).is_err());
        assert_eq!(fs::read(&existing)?, b"untouched");
        assert_eq!(
            validate_private_backup_destination(&staging, &root.path().join("bundle-a"))?,
            "bundle-a"
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn private_archive_permissions_check_nested_segments_directories()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::TempDir::new()?;
        let archive = root.path().join("archive");
        let shielded = archive
            .join(super::super::CANONICAL_CHECKPOINT_DIRECTORY_NAME)
            .join(super::super::SEGMENTS_DIR_NAME)
            .join(super::super::SHIELDED_SEGMENT_DIR);
        fs::create_dir_all(&shielded)?;
        fs::create_dir(archive.join(super::super::WALLET_CHECKPOINT_DIRECTORY_NAME))?;
        let state_bundle = crate::state_bundle::tests::sample_manifest()?;
        fs::write(
            archive.join(super::super::STATE_BUNDLE_MANIFEST_FILE_NAME),
            b"inner",
        )?;
        fs::write(archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME), b"outer")?;
        let segment = shielded.join("0000000001-0000004096.zshrng02");
        fs::write(&segment, b"segment")?;
        let payload = collect_payload_files(&archive, PayloadLayout::SealedArchive)?;
        let outer =
            RecoveryArchiveManifest::from_state_bundle(&state_bundle, "11".repeat(32), payload)?;
        for private_dir in [
            archive.as_path(),
            &archive.join(super::super::CANONICAL_CHECKPOINT_DIRECTORY_NAME),
            &archive.join(super::super::WALLET_CHECKPOINT_DIRECTORY_NAME),
            &archive
                .join(super::super::CANONICAL_CHECKPOINT_DIRECTORY_NAME)
                .join(super::super::SEGMENTS_DIR_NAME),
            &shielded,
        ] {
            fs::set_permissions(private_dir, fs::Permissions::from_mode(0o700))?;
        }
        for private_file in [
            &archive.join(super::super::STATE_BUNDLE_MANIFEST_FILE_NAME),
            &archive.join(RECOVERY_ARCHIVE_MANIFEST_FILE_NAME),
            &segment,
        ] {
            fs::set_permissions(private_file, fs::Permissions::from_mode(0o600))?;
        }
        require_private_archive_permissions(&archive, &outer)?;

        fs::set_permissions(&shielded, fs::Permissions::from_mode(0o755))?;
        assert!(require_private_archive_permissions(&archive, &outer).is_err());
        Ok(())
    }
}
