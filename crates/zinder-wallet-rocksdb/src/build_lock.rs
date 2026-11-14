//! Process-lifetime exclusion for constructing or discarding one wallet path.

use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
};

use crate::RocksDbWalletError;

/// Advisory lock on a wallet path, held while one process builds or discards it.
///
/// The durable build lease lets the owner of a crashed build reclaim it, so it
/// cannot tell a restarted owner from a second live process configured with the
/// same owner. This lock does: the operating system releases it when its
/// process exits, including on a kill, so a restart always acquires it while a
/// live builder always excludes a concurrent build or discard of its path. The
/// lock file is never removed, because removing it would let two processes lock
/// different files for the same path.
pub(crate) struct WalletBuildLock {
    _file: File,
}

impl WalletBuildLock {
    /// Acquires the lock for `wallet_path` without blocking.
    pub(crate) fn acquire(wallet_path: &Path) -> Result<Self, RocksDbWalletError> {
        let lock_path = build_lock_path(wallet_path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| RocksDbWalletError::PathUnavailable {
                path: lock_path.clone(),
                source,
            })?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(RocksDbWalletError::WalletBuildInProgress {
                path: wallet_path.to_path_buf(),
            }),
            Err(TryLockError::Error(source)) => Err(RocksDbWalletError::PathUnavailable {
                path: lock_path,
                source,
            }),
        }
    }
}

/// Resolves the lock file beside `wallet_path` through its canonical parent,
/// so every spelling of one wallet path locks the same file.
fn build_lock_path(wallet_path: &Path) -> Result<PathBuf, RocksDbWalletError> {
    let parent = match wallet_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    fs::create_dir_all(parent).map_err(|source| RocksDbWalletError::PathUnavailable {
        path: parent.to_path_buf(),
        source,
    })?;
    let parent =
        fs::canonicalize(parent).map_err(|source| RocksDbWalletError::PathUnavailable {
            path: parent.to_path_buf(),
            source,
        })?;
    let mut file_name = OsString::from(wallet_path.file_name().unwrap_or(wallet_path.as_os_str()));
    file_name.push(".build-lock");
    Ok(parent.join(file_name))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn a_held_lock_refuses_a_second_holder_until_it_is_dropped()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let wallet_path = temporary.path().join("wallet");
        let held = WalletBuildLock::acquire(&wallet_path)?;

        assert!(matches!(
            WalletBuildLock::acquire(&temporary.path().join("wallet/")),
            Err(RocksDbWalletError::WalletBuildInProgress { .. })
        ));
        drop(held);
        drop(WalletBuildLock::acquire(&wallet_path)?);
        assert!(temporary.path().join("wallet.build-lock").is_file());
        Ok(())
    }
}
