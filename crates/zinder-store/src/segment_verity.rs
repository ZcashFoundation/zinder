//! Shared regular-file opening and Linux `fs-verity` protection helpers
//! for immutable settled segment files.

use std::{fs::File, io, path::Path};

/// Error returned while enabling or verifying `fs-verity` on a settled segment.
#[derive(Debug)]
pub(crate) enum SegmentVerityError {
    /// Underlying filesystem or ioctl I/O error.
    Io(io::Error),
    /// Measured `fs-verity` digest was absent or mismatched the pinned digest.
    DigestMismatch(String),
}

impl From<io::Error> for SegmentVerityError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Opens an existing non-directory file read-only without blocking and verifies it is a regular file.
pub(crate) fn open_regular_file(path: &Path, label: &'static str) -> io::Result<File> {
    #[cfg(unix)]
    let file = {
        use rustix::fs::{Mode, OFlags, open};
        File::from(open(
            path,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?)
    };
    #[cfg(not(unix))]
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected a regular settled {label} file"),
        ));
    }
    Ok(file)
}

#[cfg(target_os = "linux")]
const SHA256_ALGORITHM: u8 = fs_verity::InnerHashAlgorithm::Sha256 as u8;

#[cfg(target_os = "linux")]
fn native_verity_filesystem(file: &File) -> io::Result<bool> {
    let stats = rustix::fs::fstatfs(file)?;
    Ok(matches!(
        i128::from(stats.f_type) & 0xffff_ffff,
        0xef53 | 0x9123_683e | 0xf2f5_2010
    ))
}

/// Reopens a staged temporary file read-only and verifies its `(dev, ino)` identity matches.
#[cfg(target_os = "linux")]
pub(crate) fn reopen_staged(
    staged: &tempfile::NamedTempFile,
    label: &'static str,
) -> io::Result<File> {
    use std::os::unix::fs::MetadataExt as _;
    let readonly = open_regular_file(staged.path(), label)?;
    let written = staged.as_file().metadata()?;
    let reopened = readonly.metadata()?;
    if (written.dev(), written.ino()) != (reopened.dev(), reopened.ino()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("staged {label} inode changed before fs-verity protection"),
        ));
    }
    Ok(readonly)
}

/// Reopens a staged temporary file read-only on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub(crate) fn reopen_staged(
    staged: &tempfile::NamedTempFile,
    label: &'static str,
) -> io::Result<File> {
    open_regular_file(staged.path(), label)
}

/// Enables kernel `fs-verity` on `file` when supported and checks it against `expected`.
#[cfg(target_os = "linux")]
pub(crate) fn protect_new_segment(
    file: &File,
    expected: Option<[u8; 32]>,
    label: &'static str,
) -> Result<(), SegmentVerityError> {
    use composefs_ioctls::fsverity::{EnableVerityError, fs_ioc_enable_verity};
    use rustix::io::Errno;

    if !native_verity_filesystem(file)? {
        return Ok(());
    }
    match fs_ioc_enable_verity(file, SHA256_ALGORITHM, 4096) {
        Ok(()) => {}
        Err(EnableVerityError::FilesystemNotSupported) => return Ok(()),
        Err(EnableVerityError::Io(source)) => match Errno::from_io_error(&source) {
            Some(Errno::NOPKG | Errno::INVAL) => return Ok(()),
            Some(_) | None => return Err(SegmentVerityError::Io(source)),
        },
        Err(EnableVerityError::AlreadyEnabled) => {
            return Err(SegmentVerityError::Io(Errno::EXIST.into()));
        }
        Err(EnableVerityError::FileOpenedForWrite) => {
            return Err(SegmentVerityError::Io(Errno::TXTBSY.into()));
        }
        Err(EnableVerityError::SignatureVerificationFailed) => {
            return Err(SegmentVerityError::Io(Errno::KEYREJECTED.into()));
        }
    }
    if expected.is_none() || measure(file)? != expected {
        return Err(SegmentVerityError::DigestMismatch(format!(
            "newly protected {label} fs-verity digest mismatch"
        )));
    }
    file.sync_all()?;
    Ok(())
}

/// No-op `fs-verity` enablement on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub(crate) fn protect_new_segment(
    _file: &File,
    _expected: Option<[u8; 32]>,
    _label: &'static str,
) -> Result<(), SegmentVerityError> {
    Ok(())
}

/// Measures the kernel `fs-verity` SHA-256 digest of `file` when active.
#[cfg(target_os = "linux")]
pub(crate) fn measure(file: &File) -> io::Result<Option<[u8; 32]>> {
    use composefs_ioctls::fsverity::{MeasureVerityError, fs_ioc_measure_verity};
    use rustix::fs::{AtFlags, StatxAttributes, StatxFlags, statx};

    if !native_verity_filesystem(file)?
        || !statx(file, c"", AtFlags::EMPTY_PATH, StatxFlags::empty()).is_ok_and(|stat| {
            stat.stx_attributes_mask.contains(StatxAttributes::VERITY)
                && stat.stx_attributes.contains(StatxAttributes::VERITY)
        })
    {
        return Ok(None);
    }
    fs_ioc_measure_verity(file, SHA256_ALGORITHM)
        .map(Some)
        .map_err(|error| match error {
            MeasureVerityError::Io(source) => source,
            MeasureVerityError::FilesystemNotSupported => {
                io::Error::new(io::ErrorKind::Unsupported, error)
            }
            MeasureVerityError::VerityMissing
            | MeasureVerityError::InvalidDigestAlgorithm { .. }
            | MeasureVerityError::InvalidDigestSize { .. } => {
                io::Error::new(io::ErrorKind::InvalidData, error)
            }
        })
}

/// Returns `Ok(None)` for `fs-verity` measurement on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
pub(crate) fn measure(_file: &File) -> io::Result<Option<[u8; 32]>> {
    Ok(None)
}

/// Checks an optional `measured` kernel `fs-verity` digest against an optional `expected` pin.
pub(crate) fn check_pinned_verity(
    expected: Option<[u8; 32]>,
    measured: Option<[u8; 32]>,
    label: &'static str,
) -> Result<bool, SegmentVerityError> {
    let Some(pin) = expected else {
        return Ok(false);
    };
    match measured {
        Some(actual) if actual != pin => Err(SegmentVerityError::DigestMismatch(format!(
            "{label} fs-verity digest mismatch"
        ))),
        Some(_) => Ok(true),
        None => Ok(false),
    }
}
