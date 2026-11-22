use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use tempfile::{Builder, TempDir};

pub(super) struct PublicationScratch {
    directory: PathBuf,
    _lease: File,
}

impl PublicationScratch {
    pub(super) fn reserve(root: &Path, catalog: &Path, primary: &Path) -> io::Result<Self> {
        let catalog = resolve_catalog(catalog)?;
        let primary = primary.canonicalize()?;
        let identity = Sha256::new()
            .chain_update(catalog.as_os_str().as_encoded_bytes())
            .chain_update([0])
            .chain_update(primary.as_os_str().as_encoded_bytes())
            .finalize();
        let name = format!("history-publisher-{}", hex::encode(identity));
        fs::create_dir_all(root)?;
        let reservation = root.join(&name);
        let marker = reservation.join(".publisher-owned");
        let mut create = fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut create, 0o700);
        match create.create(&reservation) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        if !reservation.symlink_metadata()?.is_dir() {
            return Err(io::Error::other("expected a publisher scratch directory"));
        }
        let lease = match marker.symlink_metadata() {
            Ok(metadata) if metadata.is_file() && metadata.len() == 0 => {
                File::options().read(true).write(true).open(&marker)?
            }
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    && fs::read_dir(&reservation)?.next().is_none() =>
            {
                File::create_new(&marker)?
            }
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => return Err(io::Error::other("unrecognized publisher scratch directory")),
        };
        lease.try_lock()?;
        File::open(&reservation)?.sync_all()?;
        Ok(Self {
            directory: reservation,
            _lease: lease,
        })
    }

    pub(super) fn generation(&self) -> io::Result<TempDir> {
        const GENERATION: &str = "canonical";
        let reservation = &self.directory;
        let path = reservation.join(GENERATION);
        match path.symlink_metadata() {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&path)?,
            Ok(_) => return Err(io::Error::other("expected a publisher secondary directory")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Builder::new()
            .prefix(GENERATION)
            .rand_bytes(0)
            .tempdir_in(reservation)
    }
}

// The catalog may not exist until its first publisher acquires the catalog lock.
fn resolve_catalog(path: &Path) -> io::Result<PathBuf> {
    let path = std::path::absolute(path)?;
    for ancestor in path.ancestors() {
        match ancestor.canonicalize() {
            Ok(mut base) => {
                let suffix = path.strip_prefix(ancestor).map_err(io::Error::other)?;
                for component in suffix.components() {
                    if component == std::path::Component::ParentDir {
                        base.pop();
                    } else {
                        base.push(component);
                    }
                }
                return Ok(base);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::other(
        "publisher catalog has no existing ancestor",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pending_catalog_reopens_the_same_reservation_and_excludes_concurrent_users()
    -> io::Result<()> {
        let files = tempfile::tempdir()?;
        let root = files.path().join("scratch");
        let catalog = files.path().join("missing/../catalog/child");
        let first = tempfile::tempdir_in(files.path())?;
        let second = tempfile::tempdir_in(files.path())?;
        let held = PublicationScratch::reserve(&root, &catalog, first.path())?;
        let identity = held.directory.clone();
        fs::create_dir_all(&catalog)?;
        assert!(PublicationScratch::reserve(&root, &catalog, first.path()).is_err());
        let other = PublicationScratch::reserve(&root, &catalog, second.path())?;
        assert_ne!(identity, other.directory);
        drop(held);
        assert_eq!(
            PublicationScratch::reserve(&root, &catalog, first.path())?.directory,
            identity
        );
        Ok(())
    }

    #[test]
    fn an_unrecognized_reservation_preserves_its_contents() -> io::Result<()> {
        let files = tempfile::tempdir()?;
        let root = files.path().join("scratch");
        let catalog = files.path().join("catalog");
        let held = PublicationScratch::reserve(&root, &catalog, files.path())?;
        let directory = held.directory.clone();
        drop(held);
        fs::remove_file(directory.join(".publisher-owned"))?;
        let operator = directory.join("canonical");
        fs::create_dir(&operator)?;
        fs::write(operator.join("keep"), b"operator")?;
        assert!(PublicationScratch::reserve(&root, &catalog, files.path()).is_err());
        assert_eq!(fs::read(operator.join("keep"))?, b"operator");
        Ok(())
    }
}
