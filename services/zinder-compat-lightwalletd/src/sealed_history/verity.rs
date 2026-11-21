use std::{fs::File, io};

#[cfg(target_os = "linux")]
use super::invalid;

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
    use std::os::fd::AsFd as _;

    use rustix::io::Errno;
    if !native_filesystem(file)? {
        return Ok(());
    }
    match fs_verity::linux::fsverity_enable(
        file.as_fd(),
        4096,
        fs_verity::InnerHashAlgorithm::Sha256,
        &[],
    ) {
        Ok(()) => {}
        Err(error)
            if matches!(
                Errno::from_io_error(&error),
                Some(Errno::NOTTY | Errno::OPNOTSUPP | Errno::NOPKG | Errno::INVAL)
            ) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    }
    if expected.is_none() || measure(file)? != expected {
        return Err(invalid("new shielded segment fs-verity digest mismatch"));
    }
    file.sync_all()
}

#[cfg(target_os = "linux")]
pub(super) fn measure(file: &File) -> io::Result<Option<[u8; 32]>> {
    use rustix::fs::{AtFlags, StatxAttributes, StatxFlags, statx};
    if !native_filesystem(file)?
        || !statx(file, c"", AtFlags::EMPTY_PATH, StatxFlags::empty()).is_ok_and(|stat| {
            stat.stx_attributes_mask.contains(StatxAttributes::VERITY)
                && stat.stx_attributes.contains(StatxAttributes::VERITY)
        })
    {
        return Ok(None);
    }
    let (algorithm, digest) = fs_verity::linux::fsverity_measure(std::os::fd::AsFd::as_fd(file))?;
    if algorithm != fs_verity::InnerHashAlgorithm::Sha256 {
        return Err(invalid("unsupported shielded segment fs-verity algorithm"));
    }
    digest
        .as_ref()
        .try_into()
        .map(Some)
        .map_err(|_| invalid("invalid shielded segment fs-verity digest"))
}

#[cfg(not(target_os = "linux"))]
pub(super) fn measure(_file: &File) -> io::Result<Option<[u8; 32]>> {
    Ok(None)
}
