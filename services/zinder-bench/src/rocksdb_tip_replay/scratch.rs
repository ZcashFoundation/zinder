//! Scratch copies of the input stores, so a replay never writes to its inputs.

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use zinder_bench::BenchError;

/// Free space the scratch filesystem must keep beyond one copy of both stores.
const SCRATCH_HEADROOM_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// The scratch directory a replay creates; removed on drop unless kept.
pub(super) struct ScratchRoot {
    path: PathBuf,
    keep: bool,
}

impl ScratchRoot {
    pub(super) fn create(path: &Path, keep: bool) -> Result<Self, BenchError> {
        fs::create_dir(path).map_err(|source| BenchError::io(path, source))?;
        Ok(Self {
            path: path.to_path_buf(),
            keep,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchRoot {
    fn drop(&mut self) {
        if !self.keep {
            remove_scratch(&self.path);
        }
    }
}

/// One repeat's scratch stores; removed on drop unless kept.
///
/// Callers must drop every store opened inside it first.
pub(super) struct RepeatScratch {
    path: PathBuf,
    keep: bool,
}

impl RepeatScratch {
    pub(super) fn create(
        root: &ScratchRoot,
        repeat_index: u32,
        keep: bool,
    ) -> Result<Self, BenchError> {
        let path = root.path().join(format!("repeat-{repeat_index}"));
        fs::create_dir(&path).map_err(|source| BenchError::io(&path, source))?;
        Ok(Self { path, keep })
    }

    pub(super) fn canonical_path(&self) -> PathBuf {
        self.path.join("canonical")
    }

    pub(super) fn wallet_path(&self) -> PathBuf {
        self.path.join("wallet")
    }

    pub(super) fn secondary_path(&self) -> PathBuf {
        self.path.join("canonical-secondary")
    }

    /// Whether the stores survive this repeat.
    pub(super) const fn is_kept(&self) -> bool {
        self.keep
    }
}

impl Drop for RepeatScratch {
    fn drop(&mut self) {
        if !self.keep {
            remove_scratch(&self.path);
        }
    }
}

fn remove_scratch(path: &Path) {
    if let Err(error) = fs::remove_dir_all(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            target: "zinder::bench",
            event = "tip_replay_scratch_cleanup_failed",
            path = %path.display(),
            error = %error,
            "could not remove tip replay scratch data"
        );
    }
}

/// Copies one closed store directory and syncs every copied file, so a replay
/// does not start while the copy is still being written back.
pub(super) fn copy_store(source: &Path, destination: &Path) -> Result<(), BenchError> {
    fs::create_dir(destination).map_err(|error| BenchError::io(destination, error))?;
    for entry in fs::read_dir(source).map_err(|error| BenchError::io(source, error))? {
        let entry = entry.map_err(|error| BenchError::io(source, error))?;
        let entry_path = entry.path();
        let target_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .map_err(|error| BenchError::io(&entry_path, error))?;
        if file_type.is_dir() {
            copy_store(&entry_path, &target_path)?;
        } else if file_type.is_file() {
            fs::copy(&entry_path, &target_path)
                .map_err(|error| BenchError::io(&entry_path, error))?;
            File::open(&target_path)
                .and_then(|copied| copied.sync_all())
                .map_err(|error| BenchError::io(&target_path, error))?;
        } else {
            return Err(BenchError::invalid_argument(format!(
                "store input {} holds {}, which is neither a file nor a directory",
                source.display(),
                entry_path.display()
            )));
        }
    }
    Ok(())
}

/// Returns the bytes of every regular file below `path`.
pub(super) fn directory_bytes(path: &Path) -> Result<u64, BenchError> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path).map_err(|error| BenchError::io(path, error))? {
        let entry = entry.map_err(|error| BenchError::io(path, error))?;
        let metadata = entry
            .metadata()
            .map_err(|error| BenchError::io(entry.path(), error))?;
        total = total.saturating_add(if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        });
    }
    Ok(total)
}

/// Refuses to start when one copy of the inputs would leave less than the
/// headroom free on the scratch filesystem.
pub(super) fn require_scratch_space(
    scratch_parent: &Path,
    input_bytes: u64,
) -> Result<(), BenchError> {
    let filesystem = rustix::fs::statvfs(scratch_parent)
        .map_err(|error| BenchError::io(scratch_parent, error.into()))?;
    let available_bytes = filesystem.f_bavail.saturating_mul(filesystem.f_frsize);
    let required_bytes = input_bytes.saturating_add(SCRATCH_HEADROOM_BYTES);
    if available_bytes < required_bytes {
        return Err(BenchError::invalid_argument(format!(
            "--scratch-dir needs {required_bytes} free bytes for one copy of both stores plus headroom, but {available_bytes} are available"
        )));
    }
    Ok(())
}

/// Resolves an absent path whose parent exists, for an output the run creates.
pub(super) fn resolve_absent_path(path: &Path, flag: &str) -> Result<PathBuf, BenchError> {
    if path.symlink_metadata().is_ok() {
        return Err(BenchError::invalid_argument(format!(
            "--{flag} {} already exists",
            path.display()
        )));
    }
    let file_name = path
        .file_name()
        .ok_or_else(|| BenchError::invalid_argument(format!("--{flag} needs a file name")))?;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    let parent = fs::canonicalize(parent).map_err(|error| BenchError::io(parent, error))?;
    Ok(parent.join(file_name))
}

/// Resolves an existing input directory.
pub(super) fn resolve_existing_directory(path: &Path, flag: &str) -> Result<PathBuf, BenchError> {
    let resolved = fs::canonicalize(path).map_err(|error| BenchError::io(path, error))?;
    if !resolved.is_dir() {
        return Err(BenchError::invalid_argument(format!(
            "--{flag} {} is not a directory",
            path.display()
        )));
    }
    Ok(resolved)
}

/// Refuses two paths when either contains the other.
pub(super) fn require_disjoint(
    first: (&Path, &str),
    second: (&Path, &str),
) -> Result<(), BenchError> {
    if first.0.starts_with(second.0) || second.0.starts_with(first.0) {
        return Err(BenchError::invalid_argument(format!(
            "--{} and --{} must not contain each other",
            first.1, second.1
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use tempfile::tempdir;

    use super::{copy_store, directory_bytes, require_disjoint, resolve_absent_path};

    #[test]
    fn store_copy_preserves_nested_files_and_sizes() -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let source = directory.path().join("source");
        std::fs::create_dir_all(source.join("nested"))?;
        std::fs::write(source.join("000001.sst"), b"table bytes")?;
        std::fs::write(source.join("nested").join("LOG"), b"log")?;

        let destination = directory.path().join("destination");
        copy_store(&source, &destination)?;

        assert_eq!(
            std::fs::read(destination.join("000001.sst"))?,
            b"table bytes"
        );
        assert_eq!(
            std::fs::read(destination.join("nested").join("LOG"))?,
            b"log"
        );
        assert_eq!(directory_bytes(&destination)?, 14);
        Ok(())
    }

    #[test]
    fn output_paths_must_be_absent_and_inputs_disjoint() -> Result<(), Box<dyn Error>> {
        let directory = tempdir()?;
        let existing = directory.path().join("report.json");
        std::fs::write(&existing, b"evidence")?;

        assert!(resolve_absent_path(&existing, "report").is_err());
        assert!(resolve_absent_path(&directory.path().join("fresh.json"), "report").is_ok());
        assert!(
            require_disjoint(
                (directory.path(), "scratch-dir"),
                (&directory.path().join("canonical"), "canonical-store"),
            )
            .is_err()
        );
        assert!(
            require_disjoint(
                (&directory.path().join("a"), "scratch-dir"),
                (&directory.path().join("b"), "canonical-store"),
            )
            .is_ok()
        );
        Ok(())
    }
}
