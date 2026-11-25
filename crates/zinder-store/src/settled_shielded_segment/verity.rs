//! Regular-file opening and Linux `fs-verity` protection helpers.

use std::{fs::File, io, path::Path};

use super::SettledShieldedSegmentError;

pub(super) fn open_regular_file(path: &Path) -> io::Result<File> {
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
            "expected a regular settled shielded segment file",
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

#[cfg(target_os = "linux")]
pub(super) fn reopen_staged(staged: &tempfile::NamedTempFile) -> io::Result<File> {
    use std::os::unix::fs::MetadataExt as _;
    let readonly = open_regular_file(staged.path())?;
    let written = staged.as_file().metadata()?;
    let reopened = readonly.metadata()?;
    if (written.dev(), written.ino()) != (reopened.dev(), reopened.ino()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged shielded segment inode changed before fs-verity protection",
        ));
    }
    Ok(readonly)
}

#[cfg(not(target_os = "linux"))]
pub(super) fn reopen_staged(staged: &tempfile::NamedTempFile) -> io::Result<File> {
    open_regular_file(staged.path())
}

#[cfg(target_os = "linux")]
pub(super) fn protect_new_segment(
    file: &File,
    expected: Option<[u8; 32]>,
) -> Result<(), SettledShieldedSegmentError> {
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
            Some(_) | None => return Err(SettledShieldedSegmentError::Io(source)),
        },
        Err(EnableVerityError::AlreadyEnabled) => {
            return Err(SettledShieldedSegmentError::Io(Errno::EXIST.into()));
        }
        Err(EnableVerityError::FileOpenedForWrite) => {
            return Err(SettledShieldedSegmentError::Io(Errno::TXTBSY.into()));
        }
        Err(EnableVerityError::SignatureVerificationFailed) => {
            return Err(SettledShieldedSegmentError::Io(Errno::KEYREJECTED.into()));
        }
    }
    if expected.is_none() || measure(file)? != expected {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(
            "newly protected shielded segment fs-verity digest mismatch".to_owned(),
        ));
    }
    file.sync_all()?;
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(super) fn protect_new_segment(
    _file: &File,
    _expected: Option<[u8; 32]>,
) -> Result<(), SettledShieldedSegmentError> {
    Ok(())
}

#[cfg(target_os = "linux")]
pub(super) fn measure(file: &File) -> io::Result<Option<[u8; 32]>> {
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

#[cfg(not(target_os = "linux"))]
pub(super) fn measure(_file: &File) -> io::Result<Option<[u8; 32]>> {
    Ok(None)
}

pub(super) fn check_pinned_verity(
    expected: Option<[u8; 32]>,
    measured: Option<[u8; 32]>,
) -> Result<bool, SettledShieldedSegmentError> {
    let Some(pin) = expected else {
        return Ok(false);
    };
    match measured {
        Some(actual) if actual != pin => Err(SettledShieldedSegmentError::PayloadIntegrity(
            "shielded segment fs-verity digest mismatch".to_owned(),
        )),
        Some(_) => Ok(true),
        None => Ok(false),
    }
}
