//! Pauses Explorer view replay before the views fill the storage volume.
//!
//! The Explorer view store nests inside the canonical store directory, so the
//! views share the canonical volume, and often the wallet store's volume too.
//! Views are rebuildable; canonical storage is not cheap to rebuild. When the
//! volume's available space falls below the configured floor, replay stops
//! at its next page boundary so the remaining space stays with canonical
//! ingest and wallet serving.

use std::{
    io,
    path::{Path, PathBuf},
};

use parking_lot::Mutex;

/// Default `ingest.explorer_views_min_free_bytes`: 10 GiB.
pub const DEFAULT_EXPLORER_VIEWS_MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

/// Watches available space on the volume holding the canonical and Explorer
/// view stores.
///
/// Replay pauses while fewer than `min_free_bytes` are available and resumes
/// once at least a tenth more than that is available again, so a volume
/// hovering at the floor does not flap. One guard serves every replay budget
/// in a process, so each transition is logged once.
#[derive(Debug)]
pub struct VolumeSpaceGuard {
    storage_path: PathBuf,
    min_free_bytes: u64,
    state: Mutex<VolumeSpaceState>,
}

#[derive(Debug, Default)]
struct VolumeSpaceState {
    low: bool,
    sample_failed: bool,
}

impl VolumeSpaceGuard {
    /// Returns a guard for the volume holding `storage_path`, or `None` when
    /// `min_free_bytes` is zero, which disables the guard.
    ///
    /// The path does not need to exist yet: the guard measures its nearest
    /// existing ancestor, which is on the same volume once the store exists.
    #[must_use]
    pub fn new(storage_path: impl Into<PathBuf>, min_free_bytes: u64) -> Option<Self> {
        (min_free_bytes > 0).then(|| Self {
            storage_path: storage_path.into(),
            min_free_bytes,
            state: Mutex::new(VolumeSpaceState::default()),
        })
    }

    /// Samples the volume and returns whether replay must stay paused.
    ///
    /// A failed sample keeps the previous decision and is logged once per run
    /// of failures.
    pub(super) fn is_low(&self) -> bool {
        let sample = available_bytes(&self.storage_path);
        let mut state = self.state.lock();
        match sample {
            Ok(available_bytes) => {
                self.record(&mut state, available_bytes);
            }
            Err(error) => {
                if !state.sample_failed {
                    state.sample_failed = true;
                    tracing::warn!(
                        target: "zinder::ingest",
                        event = "materialized_view_replay_volume_space_unavailable",
                        path = %self.storage_path.display(),
                        error = %error,
                        "failed to read available space on the storage volume; the Explorer view free-space guard keeps its last decision"
                    );
                }
            }
        }
        state.low
    }

    fn record(&self, state: &mut VolumeSpaceState, available_bytes: u64) {
        state.sample_failed = false;
        metrics::gauge!("zinder_materialized_view_replay_volume_available_bytes")
            .set(super::u64_to_f64(available_bytes));
        metrics::gauge!("zinder_materialized_view_replay_volume_min_free_bytes")
            .set(super::u64_to_f64(self.min_free_bytes));
        let low = next_volume_space_low(state.low, available_bytes, self.min_free_bytes);
        if low == state.low {
            return;
        }
        state.low = low;
        if low {
            tracing::warn!(
                target: "zinder::ingest",
                event = "materialized_view_replay_volume_space_low",
                path = %self.storage_path.display(),
                available_bytes,
                min_free_bytes = self.min_free_bytes,
                resume_free_bytes = resume_free_bytes(self.min_free_bytes),
                "Explorer view replay paused because the storage volume has less available space than ingest.explorer_views_min_free_bytes; canonical ingest and wallet serving continue"
            );
        } else {
            tracing::info!(
                target: "zinder::ingest",
                event = "materialized_view_replay_volume_space_recovered",
                path = %self.storage_path.display(),
                available_bytes,
                min_free_bytes = self.min_free_bytes,
                "Explorer view replay resumed because the storage volume has available space again"
            );
        }
    }
}

/// Available bytes at which a paused replay resumes.
const fn resume_free_bytes(min_free_bytes: u64) -> u64 {
    min_free_bytes.saturating_add(min_free_bytes / 10)
}

/// Advances the low-space hysteresis for one sample.
const fn next_volume_space_low(low: bool, available_bytes: u64, min_free_bytes: u64) -> bool {
    if low {
        available_bytes < resume_free_bytes(min_free_bytes)
    } else {
        available_bytes < min_free_bytes
    }
}

/// Returns the bytes an unprivileged writer can still allocate on the volume
/// holding `path`, measured at its nearest existing ancestor.
fn available_bytes(path: &Path) -> io::Result<u64> {
    let mut candidate = path;
    loop {
        let measured = if candidate.as_os_str().is_empty() {
            Path::new(".")
        } else {
            candidate
        };
        match volume_available_bytes(measured) {
            Ok(available_bytes) => return Ok(available_bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                candidate = candidate.parent().ok_or(error)?;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(unix)]
fn volume_available_bytes(path: &Path) -> io::Result<u64> {
    let statistics = rustix::fs::statvfs(path)?;
    Ok(statistics.f_bavail.saturating_mul(statistics.f_frsize))
}

#[cfg(not(unix))]
fn volume_available_bytes(_path: &Path) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "volume free-space sampling requires a Unix filesystem",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn replay_pauses_below_the_floor_and_resumes_a_tenth_above_it() {
        let min_free_bytes = 10 * GIB;
        assert!(!next_volume_space_low(
            false,
            min_free_bytes,
            min_free_bytes
        ));
        assert!(next_volume_space_low(
            false,
            min_free_bytes - 1,
            min_free_bytes
        ));
        assert!(next_volume_space_low(true, min_free_bytes, min_free_bytes));
        assert!(next_volume_space_low(true, 11 * GIB - 1, min_free_bytes));
        assert!(!next_volume_space_low(true, 11 * GIB, min_free_bytes));
    }

    #[test]
    fn a_zero_floor_disables_the_guard() {
        assert!(VolumeSpaceGuard::new("/var/lib/zinder/store", 0).is_none());
    }

    #[test]
    fn a_store_path_that_does_not_exist_yet_measures_its_volume()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::TempDir::new()?;
        let missing = directory
            .path()
            .join("canonical")
            .join("materialized-views");
        assert!(available_bytes(&missing)? > 0);
        assert_eq!(
            available_bytes(&missing)?,
            available_bytes(directory.path())?
        );
        Ok(())
    }

    #[test]
    fn a_floor_above_the_volume_size_holds_replay_paused() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::TempDir::new()?;
        let guard = VolumeSpaceGuard::new(directory.path(), u64::MAX).ok_or("guard enabled")?;
        assert!(guard.is_low());
        assert!(guard.is_low());
        Ok(())
    }
}
