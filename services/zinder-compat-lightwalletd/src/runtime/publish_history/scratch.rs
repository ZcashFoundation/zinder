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
        let primary = primary.canonicalize().map_err(|error| {
            path_io_error("canonical primary path is unavailable", primary, &error)
        })?;
        let identity = Sha256::new()
            .chain_update(catalog.as_os_str().as_encoded_bytes())
            .chain_update([0])
            .chain_update(primary.as_os_str().as_encoded_bytes())
            .finalize();
        let name = format!("history-publisher-{}", hex::encode(identity));
        fs::create_dir_all(root).map_err(|error| {
            path_io_error("could not create canonical secondary root", root, &error)
        })?;
        let reservation = root.join(&name);
        let marker = reservation.join(".publisher-owned");
        let mut create = fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut create, 0o700);
        match create.create(&reservation) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(path_io_error(
                    "could not create publisher scratch directory",
                    &reservation,
                    &error,
                ));
            }
        }
        let reservation_meta = reservation.symlink_metadata().map_err(|error| {
            path_io_error(
                "could not inspect publisher scratch directory",
                &reservation,
                &error,
            )
        })?;
        if !reservation_meta.is_dir() {
            return Err(io::Error::other(format!(
                "expected a publisher scratch directory at {}",
                reservation.display()
            )));
        }
        let lease = open_scratch_lease(&reservation, &marker)?;
        lease.try_lock().map_err(|error| {
            path_io_error(
                "could not lock publisher scratch lease",
                &marker,
                &io::Error::from(error),
            )
        })?;
        File::open(&reservation)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                path_io_error(
                    "could not sync publisher scratch directory",
                    &reservation,
                    &error,
                )
            })?;
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
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(&path).map_err(|error| {
                path_io_error("could not remove stale publisher secondary", &path, &error)
            })?,
            Ok(_) => {
                return Err(io::Error::other(format!(
                    "expected a publisher secondary directory at {}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(path_io_error(
                    "could not inspect publisher secondary",
                    &path,
                    &error,
                ));
            }
        }
        Builder::new()
            .prefix(GENERATION)
            .rand_bytes(0)
            .tempdir_in(reservation)
            .map_err(|error| {
                path_io_error(
                    "could not create publisher secondary directory under",
                    reservation,
                    &error,
                )
            })
    }
}

fn open_scratch_lease(reservation: &Path, marker: &Path) -> io::Result<File> {
    match marker.symlink_metadata() {
        Ok(metadata) if metadata.is_file() && metadata.len() == 0 => File::options()
            .read(true)
            .write(true)
            .open(marker)
            .map_err(|error| {
                path_io_error("could not open publisher lease marker", marker, &error)
            }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut entries = fs::read_dir(reservation).map_err(|read_error| {
                path_io_error(
                    "could not read publisher scratch directory",
                    reservation,
                    &read_error,
                )
            })?;
            if entries.next().is_some() {
                return Err(io::Error::other(format!(
                    "unrecognized publisher scratch directory {}",
                    reservation.display()
                )));
            }
            File::create_new(marker).map_err(|create_error| {
                path_io_error(
                    "could not create publisher lease marker",
                    marker,
                    &create_error,
                )
            })
        }
        Err(error) => Err(path_io_error(
            "could not inspect publisher lease marker",
            marker,
            &error,
        )),
        Ok(_) => Err(io::Error::other(format!(
            "unrecognized publisher scratch directory {}",
            reservation.display()
        ))),
    }
}

pub(super) fn path_io_error(context: &'static str, path: &Path, source: &io::Error) -> io::Error {
    io::Error::new(
        source.kind(),
        format!("{context} {}: {source}", path.display()),
    )
}

// The catalog may not exist until its first publisher acquires the catalog lock.
fn resolve_catalog(path: &Path) -> io::Result<PathBuf> {
    let absolute_path = std::path::absolute(path).map_err(|error| {
        path_io_error("could not resolve shielded history directory", path, &error)
    })?;
    for ancestor in absolute_path.ancestors() {
        match ancestor.canonicalize() {
            Ok(mut base) => {
                let suffix = absolute_path.strip_prefix(ancestor).map_err(|_| {
                    io::Error::other(format!(
                        "could not resolve shielded history directory {}",
                        path.display()
                    ))
                })?;
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
            Err(error) => {
                return Err(path_io_error(
                    "could not access ancestor of shielded history directory",
                    ancestor,
                    &error,
                ));
            }
        }
    }
    Err(io::Error::other(format!(
        "publisher catalog {} has no existing ancestor",
        path.display()
    )))
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
