use std::{fs::File, io};

#[cfg(target_os = "linux")]
use super::invalid;

#[cfg(target_os = "linux")]
const SHA256: u8 = fs_verity::InnerHashAlgorithm::Sha256 as u8;

#[cfg(target_os = "linux")]
fn native_filesystem(file: &File) -> io::Result<bool> {
    // Userspace and network filesystems can supply their own verity attributes.
    let stats = rustix::fs::fstatfs(file)?;
    Ok(matches!(
        i128::from(stats.f_type) & 0xffff_ffff,
        0xef53 | 0x9123_683e | 0xf2f5_2010
    ))
}

#[cfg(target_os = "linux")]
pub(crate) fn reopen_staged(staged: &tempfile::NamedTempFile) -> io::Result<File> {
    use std::os::unix::fs::MetadataExt as _;
    let readonly = super::open_regular_file(staged.path())?;
    let written = staged.as_file().metadata()?;
    let reopened = readonly.metadata()?;
    if (written.dev(), written.ino()) != (reopened.dev(), reopened.ino()) {
        return Err(invalid("staged shielded segment changed before protection"));
    }
    Ok(readonly)
}

#[cfg(target_os = "linux")]
pub(crate) fn protect_new_segment(file: &File, expected: Option<[u8; 32]>) -> io::Result<()> {
    use composefs_ioctls::fsverity::{EnableVerityError, fs_ioc_enable_verity};
    use rustix::io::Errno;

    if !native_filesystem(file)? {
        return Ok(());
    }
    match fs_ioc_enable_verity(file, SHA256, 4096) {
        Ok(()) => {}
        Err(EnableVerityError::FilesystemNotSupported) => return Ok(()),
        Err(EnableVerityError::Io(error)) => match Errno::from_io_error(&error) {
            Some(Errno::NOPKG | Errno::INVAL) => return Ok(()),
            _ => return Err(error),
        },
        Err(EnableVerityError::AlreadyEnabled) => return Err(Errno::EXIST.into()),
        Err(EnableVerityError::FileOpenedForWrite) => return Err(Errno::TXTBSY.into()),
        Err(EnableVerityError::SignatureVerificationFailed) => {
            return Err(Errno::KEYREJECTED.into());
        }
    }
    if expected.is_none() || measure(file)? != expected {
        return Err(invalid("new shielded segment fs-verity digest mismatch"));
    }
    file.sync_all()
}

#[cfg(target_os = "linux")]
pub(super) fn measure(file: &File) -> io::Result<Option<[u8; 32]>> {
    use composefs_ioctls::fsverity::{MeasureVerityError, fs_ioc_measure_verity};
    use rustix::fs::{AtFlags, StatxAttributes, StatxFlags, statx};

    if !native_filesystem(file)?
        || !statx(file, c"", AtFlags::EMPTY_PATH, StatxFlags::empty()).is_ok_and(|stat| {
            stat.stx_attributes_mask.contains(StatxAttributes::VERITY)
                && stat.stx_attributes.contains(StatxAttributes::VERITY)
        })
    {
        return Ok(None);
    }
    fs_ioc_measure_verity(file, SHA256)
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
