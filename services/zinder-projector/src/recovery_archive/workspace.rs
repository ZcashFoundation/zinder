//! Private same-filesystem workspaces for publishing recovered state.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use super::{
    RecoveryArchiveError, require_contract, require_directory, require_not_hard_link_metadata,
    resolve_existing_directory, sync_directory,
};

pub(super) fn create_private_directory(path: &Path) -> Result<(), RecoveryArchiveError> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder
        .create(path)
        .map_err(|source| RecoveryArchiveError::io(path, source))
}

pub(super) fn create_private_file(path: &Path) -> Result<File, RecoveryArchiveError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options
        .open(path)
        .map_err(|source| RecoveryArchiveError::io(path, source))
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub(super) fn publish_directory(
    source: &Path,
    destination: &Path,
) -> Result<(), RecoveryArchiveError> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};

    let parent = destination.parent().ok_or_else(|| {
        RecoveryArchiveError::contract("publication requires a destination parent")
    })?;
    match renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE) {
        Ok(()) => sync_directory(parent),
        Err(rustix::io::Errno::EXIST | rustix::io::Errno::NOTEMPTY) => {
            Err(RecoveryArchiveError::TargetExists {
                path: destination.to_path_buf(),
            })
        }
        Err(error) => Err(RecoveryArchiveError::io(destination, error.into())),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(super) fn publish_directory(
    _source: &Path,
    _destination: &Path,
) -> Result<(), RecoveryArchiveError> {
    Err(RecoveryArchiveError::contract(
        "atomic no-overwrite recovery publication is unsupported on this platform",
    ))
}

pub(super) fn with_private_workspace<T>(
    destination: &Path,
    operation: &'static str,
    source: &Path,
    work: impl FnOnce(&Path) -> Result<T, RecoveryArchiveError>,
) -> Result<T, RecoveryArchiveError> {
    let parent = destination.parent().ok_or_else(|| {
        RecoveryArchiveError::contract("private workspace requires a destination parent")
    })?;
    let parent = resolve_existing_directory(parent, "private workspace parent")?;
    require_recovery_output_parent(&parent)?;
    let name = destination.file_name().ok_or_else(|| {
        RecoveryArchiveError::contract("private workspace requires a destination name")
    })?;
    let destination = parent.join(name);
    let source = resolve_existing_directory(source, "private workspace source")?;
    require_disjoint(&destination, &source)?;

    let destination_hash = hex::encode(Sha256::digest(name.as_encoded_bytes()));
    let identifier = format!("zinder-{operation}-v1-{destination_hash}");
    let marker_path = parent.join(format!(".zinder-{operation}-{destination_hash}.work"));
    require_disjoint(&marker_path, &source)?;
    let (marker, workspace) = lock_workspace(&marker_path, &parent, &identifier)?;
    require_disjoint(&workspace, &source)?;
    require_disjoint(&workspace, &destination)?;
    match fs::symlink_metadata(&workspace) {
        Ok(_) => {
            require_directory(&workspace, "abandoned private workspace")?;
            fs::remove_dir_all(&workspace)
                .map_err(|source| RecoveryArchiveError::io(&workspace, source))?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(RecoveryArchiveError::io(&workspace, error)),
    }
    create_private_directory(&workspace)?;
    let completed = work(&workspace);
    let cleanup = fs::remove_dir_all(&workspace)
        .map_err(|source| RecoveryArchiveError::io(&workspace, source));
    drop(marker);
    let outcome = completed?;
    cleanup?;
    Ok(outcome)
}

pub(super) fn require_recovery_output_parent(parent: &Path) -> Result<(), RecoveryArchiveError> {
    for ancestor in parent.ancestors() {
        let exists = |name: &str| {
            let path = ancestor.join(name);
            path.try_exists()
                .map_err(|source| RecoveryArchiveError::io(&path, source))
        };
        let protected = exists(super::STATE_BUNDLE_MANIFEST_FILE_NAME)?
            || exists(super::RECOVERY_ARCHIVE_MANIFEST_FILE_NAME)?
            || (exists("CURRENT")? && exists("IDENTITY")?);
        require_contract(
            !protected,
            "private recovery destination must be outside checkpoint stores, captures, and archives",
        )?;
    }
    Ok(())
}

fn require_disjoint(left: &Path, right: &Path) -> Result<(), RecoveryArchiveError> {
    require_contract(
        !left.starts_with(right) && !right.starts_with(left),
        "private recovery source, workspace, and destination must be disjoint",
    )
}

// The marker remains at one inode so all retries lock the same file. Its random
// directory name bounds crash cleanup to space reserved for this operation.
fn lock_workspace(
    path: &Path,
    parent: &Path,
    identifier: &str,
) -> Result<(File, PathBuf), RecoveryArchiveError> {
    if !path
        .try_exists()
        .map_err(|source| RecoveryArchiveError::io(path, source))?
    {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce)
            .map_err(|source| RecoveryArchiveError::io(path, io::Error::other(source)))?;
        let mut reservation = tempfile::NamedTempFile::new_in(parent)
            .map_err(|source| RecoveryArchiveError::io(path, source))?;
        reservation
            .write_all(identifier.as_bytes())
            .and_then(|()| reservation.write_all(&nonce))
            .and_then(|()| reservation.as_file().sync_all())
            .map_err(|source| RecoveryArchiveError::io(path, source))?;
        match reservation.persist_noclobber(path) {
            Ok(_) => {}
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(RecoveryArchiveError::io(path, error.error)),
        }
    }
    super::require_regular_file(path, "private workspace reservation")?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|source| RecoveryArchiveError::io(path, source))?;
    let metadata = file
        .metadata()
        .map_err(|source| RecoveryArchiveError::io(path, source))?;
    require_not_hard_link_metadata(path, &metadata, "private workspace reservation")?;
    file.try_lock().map_err(|error| match error {
        fs::TryLockError::Error(source) => RecoveryArchiveError::io(path, source),
        fs::TryLockError::WouldBlock => RecoveryArchiveError::contract(
            "private recovery operation already in progress at this destination",
        ),
    })?;
    let mut saved = vec![0_u8; identifier.len() + 16];
    require_contract(
        metadata.len() == u64::try_from(saved.len()).unwrap_or(u64::MAX),
        "unrecognized private workspace reservation",
    )?;
    file.read_exact(&mut saved)
        .map_err(|source| RecoveryArchiveError::io(path, source))?;
    let nonce = saved.strip_prefix(identifier.as_bytes()).ok_or_else(|| {
        RecoveryArchiveError::contract(
            "private workspace reservation belongs to a different operation",
        )
    })?;
    sync_directory(parent)?;
    let workspace = parent.join(format!(".{identifier}-{}.incomplete", hex::encode(nonce)));
    Ok((file, workspace))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn retry_reclaims_only_the_reserved_scratch_and_excludes_concurrent_callers()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = TempDir::new()?;
        let destination = parent.path().join("destination");
        let source = parent.path().join("source");
        create_private_directory(&source)?;
        let unrelated = parent.path().join(".zinder-other.incomplete");
        fs::write(&unrelated, b"unrelated")?;
        let abandoned = with_private_workspace(&destination, "backup", &source, |scratch| {
            assert!(with_private_workspace(&destination, "backup", &source, |_| Ok(())).is_err());
            assert!(scratch.is_dir());
            Ok(scratch.to_owned())
        })?;
        create_private_directory(&abandoned)?;
        fs::write(abandoned.join("partial"), b"abandoned copy")?;

        with_private_workspace(&destination, "backup", &source, |scratch| {
            assert_eq!(scratch, abandoned);
            assert!(!scratch.join("partial").exists());
            Ok(())
        })?;
        assert!(!abandoned.exists());
        assert_eq!(fs::read(&unrelated)?, b"unrelated");
        assert!(source.is_dir() && !destination.exists());
        Ok(())
    }

    #[test]
    fn retry_preserves_a_source_inside_a_reserved_workspace()
    -> Result<(), Box<dyn std::error::Error>> {
        let parent = TempDir::new()?;
        let destination = parent.path().join("destination");
        let source = parent.path().join("source");
        create_private_directory(&source)?;
        let reserved = with_private_workspace(&destination, "restore", &source, |scratch| {
            Ok(scratch.to_owned())
        })?;
        create_private_directory(&reserved)?;
        let unexpected_source = reserved.join("source");
        create_private_directory(&unexpected_source)?;
        fs::write(unexpected_source.join("sentinel"), b"preserve")?;
        assert!(
            with_private_workspace(&destination, "restore", &unexpected_source, |_| Ok(()))
                .is_err()
        );
        assert_eq!(fs::read(unexpected_source.join("sentinel"))?, b"preserve");
        Ok(())
    }

    #[test]
    fn directory_publication_preserves_even_an_empty_existing_destination()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TempDir::new()?;
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        create_private_directory(&source)?;
        fs::write(source.join("marker"), b"source")?;
        create_private_directory(&destination)?;
        assert!(publish_directory(&source, &destination).is_err());
        assert!(!destination.join("marker").exists());
        assert_eq!(fs::read(source.join("marker"))?, b"source");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn reservations_and_archived_files_have_private_modes_from_creation()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let root = TempDir::new()?;
        let source = root.path().join("source");
        create_private_directory(&source)?;
        let destination = root.path().join("target");
        with_private_workspace(&destination, "backup", &source, |scratch| {
            assert_eq!(
                fs::metadata(scratch)
                    .map_err(|e| RecoveryArchiveError::io(scratch, e))?
                    .permissions()
                    .mode()
                    & 0o077,
                0
            );
            let child = scratch.join("private");
            let file = create_private_file(&child)?;
            assert_eq!(
                file.metadata()
                    .map_err(|e| RecoveryArchiveError::io(&child, e))?
                    .permissions()
                    .mode()
                    & 0o077,
                0
            );
            Ok(())
        })?;
        Ok(())
    }
}
