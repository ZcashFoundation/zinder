//! Lifecycle, manifest, snapshot, and lazy shared reader cache for
//! canonical store settled segments (`ZSHRNG02`, `ZSTSUP01`, and `ZSBLKB01`).

pub(crate) mod cache;
pub(crate) mod layout;
pub(crate) mod sealer;
pub(crate) mod snapshot;
pub(crate) mod store_access;
#[cfg(test)]
mod tests;

use std::{
    fs::{self, File},
    io::{self, Read as _, Write as _},
    path::{Path, PathBuf},
};

use thiserror::Error;

use crate::{
    settled_raw_block_segment::SettledRawBlockSegmentError,
    settled_shielded_segment::SettledShieldedSegmentError,
    settled_supplement_segment::SettledSupplementSegmentError,
};

/// Errors returned by canonical settled segment lifecycle operations.
#[derive(Debug, Error)]
pub(crate) enum SettledSegmentsError {
    /// Filesystem or directory synchronization failure.
    #[error("settled segments I/O failure: {0}")]
    Io(#[from] io::Error),
    /// Shielded segment writer/reader error.
    #[error("settled shielded segment error: {0}")]
    Shielded(#[from] SettledShieldedSegmentError),
    /// Supplement segment writer/reader error.
    #[error("settled supplement segment error: {0}")]
    Supplement(#[from] SettledSupplementSegmentError),
    /// Raw block segment writer/reader error.
    #[error("settled raw block segment error: {0}")]
    RawBlock(#[from] SettledRawBlockSegmentError),
    /// Manifest JSON serialization or deserialization error.
    #[error("settled segments manifest JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Invalid layout path, filename, symlink refusal, or cache bound.
    #[error("invalid settled segments layout or path: {0}")]
    InvalidLayout(String),
    /// Malformed, truncated, or unversioned binary encoding.
    #[error("invalid settled segments binary format: {0}")]
    InvalidFormat(String),
    /// Integrity check, digest verification, or replay validation failed.
    #[error("settled segments integrity check failed: {0}")]
    Integrity(String),
    /// Block continuity, span, or identity mismatch.
    #[error("settled segments chain linkage violation: {0}")]
    ChainLinkage(String),
}

impl From<crate::segment_verity::SegmentVerityError> for SettledSegmentsError {
    fn from(error: crate::segment_verity::SegmentVerityError) -> Self {
        match error {
            crate::segment_verity::SegmentVerityError::Io(source) => Self::Io(source),
            crate::segment_verity::SegmentVerityError::DigestMismatch(reason) => {
                Self::Integrity(reason)
            }
        }
    }
}

impl From<SettledSegmentsError> for super::CanonicalStoreError {
    fn from(error: SettledSegmentsError) -> Self {
        Self::SettledSegment {
            reason: error.to_string(),
        }
    }
}

pub(in crate::canonical_store) struct ByteReader<'a> {
    rest: &'a [u8],
}

impl<'a> ByteReader<'a> {
    pub(in crate::canonical_store) const fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }

    pub(in crate::canonical_store) fn read_slice(
        &mut self,
        len: usize,
        field_label: &str,
    ) -> Result<&'a [u8], SettledSegmentsError> {
        let slice = self.rest.get(..len).ok_or_else(|| {
            SettledSegmentsError::InvalidFormat(format!(
                "truncated settled segments payload while reading {field_label}"
            ))
        })?;
        self.rest = &self.rest[len..];
        Ok(slice)
    }

    pub(in crate::canonical_store) fn read_u8(
        &mut self,
        field_label: &str,
    ) -> Result<u8, SettledSegmentsError> {
        let slice = self.read_slice(1, field_label)?;
        Ok(slice[0])
    }

    pub(in crate::canonical_store) fn read_bool(
        &mut self,
        field_label: &str,
    ) -> Result<bool, SettledSegmentsError> {
        match self.read_u8(field_label)? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(SettledSegmentsError::InvalidFormat(format!(
                "invalid boolean marker {other} for {field_label}"
            ))),
        }
    }

    pub(in crate::canonical_store) fn read_u16_be(
        &mut self,
        field_label: &str,
    ) -> Result<u16, SettledSegmentsError> {
        let bytes = self.read_array::<2>(field_label)?;
        Ok(u16::from_be_bytes(bytes))
    }

    pub(in crate::canonical_store) fn read_u32_be(
        &mut self,
        field_label: &str,
    ) -> Result<u32, SettledSegmentsError> {
        let bytes = self.read_array::<4>(field_label)?;
        Ok(u32::from_be_bytes(bytes))
    }

    pub(in crate::canonical_store) fn read_u64_be(
        &mut self,
        field_label: &str,
    ) -> Result<u64, SettledSegmentsError> {
        let bytes = self.read_array::<8>(field_label)?;
        Ok(u64::from_be_bytes(bytes))
    }

    pub(in crate::canonical_store) fn read_i64_be(
        &mut self,
        field_label: &str,
    ) -> Result<i64, SettledSegmentsError> {
        let bytes = self.read_array::<8>(field_label)?;
        Ok(i64::from_be_bytes(bytes))
    }

    pub(in crate::canonical_store) fn read_array<const N: usize>(
        &mut self,
        field_label: &str,
    ) -> Result<[u8; N], SettledSegmentsError> {
        let slice = self.read_slice(N, field_label)?;
        slice.try_into().map_err(|_| {
            SettledSegmentsError::InvalidFormat(format!(
                "invalid byte length while reading {field_label}"
            ))
        })
    }

    pub(in crate::canonical_store) fn read_len_prefixed_slice(
        &mut self,
        max_len: usize,
        field_label: &str,
    ) -> Result<&'a [u8], SettledSegmentsError> {
        let raw_len = self.read_u32_be(field_label)?;
        let byte_len = usize::try_from(raw_len).map_err(|_| {
            SettledSegmentsError::InvalidFormat(format!(
                "length for {field_label} exceeds host address space"
            ))
        })?;
        if byte_len > max_len {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "length {byte_len} for {field_label} exceeds limit {max_len}"
            )));
        }
        self.read_slice(byte_len, field_label)
    }

    pub(in crate::canonical_store) fn read_len_prefixed_vec(
        &mut self,
        max_len: usize,
        field_label: &str,
    ) -> Result<Vec<u8>, SettledSegmentsError> {
        Ok(self.read_len_prefixed_slice(max_len, field_label)?.to_vec())
    }

    pub(in crate::canonical_store) fn finish(
        self,
        record_label: &str,
    ) -> Result<(), SettledSegmentsError> {
        if !self.rest.is_empty() {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "{record_label} contains {} trailing byte(s)",
                self.rest.len()
            )));
        }
        Ok(())
    }
}

pub(in crate::canonical_store) fn push_len_prefixed_bytes(
    out: &mut Vec<u8>,
    bytes: &[u8],
    field_label: &str,
) -> Result<(), SettledSegmentsError> {
    let len_u32 = u32::try_from(bytes.len()).map_err(|_| {
        SettledSegmentsError::InvalidFormat(format!("byte length for {field_label} exceeds u32"))
    })?;
    out.extend_from_slice(&len_u32.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EntryKind {
    RegularFile,
    Directory,
    SymlinkOrOther,
}

/// Verified non-symlink directory handle holding a pinned descriptor on Unix.
///
/// Subsequent child directory creation, file opens, scans, and promotions
/// operate strictly relative to the verified parent descriptor.
#[derive(Debug)]
pub(super) struct VerifiedDir {
    #[cfg(unix)]
    fd: rustix::fd::OwnedFd,
    display_path: PathBuf,
}

impl VerifiedDir {
    #[must_use]
    pub(super) fn display_path(&self) -> &Path {
        &self.display_path
    }

    pub(super) fn try_clone(&self) -> Result<Self, SettledSegmentsError> {
        #[cfg(unix)]
        {
            let cloned = self.fd.try_clone()?;
            Ok(Self {
                fd: cloned,
                display_path: self.display_path.clone(),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                display_path: self.display_path.clone(),
            })
        }
    }

    pub(super) fn open_existing(
        store_root: &Path,
        subcomponents: &[&str],
    ) -> Result<Self, SettledSegmentsError> {
        let mut current = Self::open_root_existing(store_root)?;
        for component in subcomponents {
            current = current.open_child_dir_existing(component)?;
        }
        Ok(current)
    }

    pub(super) fn open_existing_optional(
        store_root: &Path,
        subcomponents: &[&str],
    ) -> Result<Option<Self>, SettledSegmentsError> {
        let Some(mut current) = Self::open_root_optional(store_root)? else {
            return Ok(None);
        };
        for component in subcomponents {
            let Some(child) = current.open_child_dir_optional(component)? else {
                return Ok(None);
            };
            current = child;
        }
        Ok(Some(current))
    }

    pub(super) fn create_or_open(
        store_root: &Path,
        subcomponents: &[&str],
    ) -> Result<Self, SettledSegmentsError> {
        let mut current = Self::create_or_open_root(store_root)?;
        for component in subcomponents {
            current = current.create_or_open_child_dir(component)?;
        }
        Ok(current)
    }

    fn open_root_existing(store_root: &Path) -> Result<Self, SettledSegmentsError> {
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags, open};

            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let fd = open(store_root, flags, Mode::empty()).map_err(|err| {
                SettledSegmentsError::InvalidLayout(format!(
                    "cannot open directory {}: {err}",
                    store_root.display()
                ))
            })?;
            Ok(Self {
                fd,
                display_path: store_root.to_path_buf(),
            })
        }
        #[cfg(not(unix))]
        {
            verify_existing_dir_no_symlink(store_root)?;
            Ok(Self {
                display_path: store_root.to_path_buf(),
            })
        }
    }

    fn open_root_optional(store_root: &Path) -> Result<Option<Self>, SettledSegmentsError> {
        #[cfg(unix)]
        {
            use rustix::{
                fs::{Mode, OFlags, open},
                io::Errno,
            };

            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            match open(store_root, flags, Mode::empty()) {
                Ok(fd) => Ok(Some(Self {
                    fd,
                    display_path: store_root.to_path_buf(),
                })),
                Err(Errno::NOENT) => Ok(None),
                Err(err) => Err(SettledSegmentsError::InvalidLayout(format!(
                    "cannot open directory {}: {err}",
                    store_root.display()
                ))),
            }
        }
        #[cfg(not(unix))]
        {
            match fs::symlink_metadata(store_root) {
                Ok(_) => Self::open_root_existing(store_root).map(Some),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(SettledSegmentsError::Io(err)),
            }
        }
    }

    fn create_or_open_root(store_root: &Path) -> Result<Self, SettledSegmentsError> {
        if let Some(existing) = Self::open_root_optional(store_root)? {
            return Ok(existing);
        }
        fs::create_dir_all(store_root)?;
        Self::open_root_existing(store_root)
    }

    pub(super) fn open_child_dir_existing(
        &self,
        component: &str,
    ) -> Result<Self, SettledSegmentsError> {
        reject_unsafe_component(component)?;
        let child_path = self.display_path.join(component);
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags, openat};

            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let fd = openat(&self.fd, component, flags, Mode::empty()).map_err(|err| {
                SettledSegmentsError::InvalidLayout(format!(
                    "cannot open directory {}: {err}",
                    child_path.display()
                ))
            })?;
            Ok(Self {
                fd,
                display_path: child_path,
            })
        }
        #[cfg(not(unix))]
        {
            verify_existing_dir_no_symlink(&child_path)?;
            Ok(Self {
                display_path: child_path,
            })
        }
    }

    pub(super) fn open_child_dir_optional(
        &self,
        component: &str,
    ) -> Result<Option<Self>, SettledSegmentsError> {
        reject_unsafe_component(component)?;
        let child_path = self.display_path.join(component);
        #[cfg(unix)]
        {
            use rustix::{
                fs::{Mode, OFlags, openat},
                io::Errno,
            };

            let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            match openat(&self.fd, component, flags, Mode::empty()) {
                Ok(fd) => Ok(Some(Self {
                    fd,
                    display_path: child_path,
                })),
                Err(Errno::NOENT) => Ok(None),
                Err(err) => Err(SettledSegmentsError::InvalidLayout(format!(
                    "cannot open directory {}: {err}",
                    child_path.display()
                ))),
            }
        }
        #[cfg(not(unix))]
        {
            match fs::symlink_metadata(&child_path) {
                Ok(_) => self.open_child_dir_existing(component).map(Some),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(SettledSegmentsError::Io(err)),
            }
        }
    }

    pub(super) fn create_or_open_child_dir(
        &self,
        component: &str,
    ) -> Result<Self, SettledSegmentsError> {
        reject_unsafe_component(component)?;
        if let Some(existing) = self.open_child_dir_optional(component)? {
            return Ok(existing);
        }
        #[cfg(unix)]
        {
            use rustix::{
                fs::{Mode, mkdirat},
                io::Errno,
            };

            let mode = Mode::from_bits_truncate(0o755);
            match mkdirat(&self.fd, component, mode) {
                Ok(()) => self.sync()?,
                Err(Errno::EXIST) => {}
                Err(err) => return Err(SettledSegmentsError::Io(io::Error::from(err))),
            }
        }
        #[cfg(not(unix))]
        {
            let child_path = self.display_path.join(component);
            match fs::create_dir(&child_path) {
                Ok(()) => self.sync()?,
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(SettledSegmentsError::Io(err)),
            }
        }
        self.open_child_dir_existing(component)
    }

    pub(super) fn open_regular_file_nofollow(
        &self,
        name: &str,
    ) -> Result<File, SettledSegmentsError> {
        reject_unsafe_component(name)?;
        let entry_path = self.display_path.join(name);
        #[cfg(unix)]
        let opened = {
            use rustix::{
                fs::{Mode, OFlags, openat},
                io::Errno,
            };

            let flags = OFlags::RDONLY | OFlags::NONBLOCK | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let fd = openat(&self.fd, name, flags, Mode::empty()).map_err(|err| match err {
                Errno::LOOP | Errno::NOTDIR | Errno::ISDIR => {
                    SettledSegmentsError::InvalidLayout(format!(
                        "refusing non-regular segment entry {}",
                        entry_path.display()
                    ))
                }
                other => SettledSegmentsError::Io(io::Error::from(other)),
            })?;
            File::from(fd)
        };
        #[cfg(not(unix))]
        let opened = {
            let meta = fs::symlink_metadata(&entry_path)?;
            if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                return Err(SettledSegmentsError::InvalidLayout(format!(
                    "expected regular file at {}",
                    entry_path.display()
                )));
            }
            File::open(&entry_path)?
        };
        if !opened.metadata()?.is_file() {
            return Err(SettledSegmentsError::InvalidLayout(format!(
                "expected regular file at {}",
                entry_path.display()
            )));
        }
        Ok(opened)
    }

    pub(super) fn open_regular_matching_inode(
        &self,
        name: &str,
        expected_dev_ino: (u64, u64),
    ) -> Result<File, SettledSegmentsError> {
        let opened = self.open_regular_file_nofollow(name)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let meta = opened.metadata()?;
            if (meta.dev(), meta.ino()) != expected_dev_ino {
                return Err(SettledSegmentsError::InvalidLayout(format!(
                    "staged file {} inode changed before fs-verity protection",
                    self.display_path.join(name).display()
                )));
            }
        }
        #[cfg(not(unix))]
        let _ = expected_dev_ino;
        Ok(opened)
    }

    pub(super) fn read_bounded_regular_file_nofollow(
        &self,
        name: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, SettledSegmentsError> {
        let mut file = self.open_regular_file_nofollow(name)?;
        let file_len = file.metadata()?.len();
        let max_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
        if file_len > max_u64 {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "file {} exceeds maximum size of {max_bytes} bytes",
                self.display_path.join(name).display()
            )));
        }
        let cap = usize::try_from(file_len).map_err(|_| {
            SettledSegmentsError::InvalidFormat("file length exceeds host address space".to_owned())
        })?;
        let mut buf = Vec::with_capacity(cap);
        let mut limited = (&mut file).take(max_u64.saturating_add(1));
        limited.read_to_end(&mut buf)?;
        if buf.len() > max_bytes {
            return Err(SettledSegmentsError::InvalidFormat(format!(
                "file {} grew beyond maximum size of {max_bytes} bytes",
                self.display_path.join(name).display()
            )));
        }
        Ok(buf)
    }

    pub(super) fn create_named_exclusive_rw(
        &self,
        name: &str,
    ) -> Result<File, SettledSegmentsError> {
        reject_unsafe_component(name)?;
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags, openat};

            let flags =
                OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            let mode = Mode::from_bits_truncate(0o600);
            let fd = openat(&self.fd, name, flags, mode).map_err(io::Error::from)?;
            Ok(File::from(fd))
        }
        #[cfg(not(unix))]
        {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(self.display_path.join(name))?;
            Ok(file)
        }
    }

    pub(super) fn create_temp_file(
        &self,
        prefix: &str,
    ) -> Result<(String, File), SettledSegmentsError> {
        reject_unsafe_component(prefix)?;
        for _ in 0..64 {
            let mut entropy = [0_u8; 8];
            getrandom::fill(&mut entropy).map_err(|err| {
                SettledSegmentsError::Io(io::Error::other(format!(
                    "failed to sample temporary filename entropy: {err}"
                )))
            })?;
            let suffix = u64::from_le_bytes(entropy);
            let candidate = format!("{prefix}-{suffix:016x}.tmp");
            match self.create_named_exclusive_rw(&candidate) {
                Ok(file) => return Ok((candidate, file)),
                Err(SettledSegmentsError::Io(err))
                    if err.kind() == io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(err),
            }
        }
        Err(SettledSegmentsError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "exhausted temporary file creation retries",
        )))
    }

    pub(super) fn stat_entry_nofollow(
        &self,
        name: &str,
    ) -> Result<Option<EntryKind>, SettledSegmentsError> {
        reject_unsafe_component(name)?;
        #[cfg(unix)]
        {
            use rustix::{
                fs::{AtFlags, FileType, statat},
                io::Errno,
            };

            match statat(&self.fd, name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(stat) => {
                    let kind = match FileType::from_raw_mode(stat.st_mode) {
                        FileType::RegularFile => EntryKind::RegularFile,
                        FileType::Directory => EntryKind::Directory,
                        FileType::Symlink
                        | FileType::Fifo
                        | FileType::Socket
                        | FileType::CharacterDevice
                        | FileType::BlockDevice
                        | FileType::Unknown => EntryKind::SymlinkOrOther,
                    };
                    Ok(Some(kind))
                }
                Err(Errno::NOENT) => Ok(None),
                Err(err) => Err(SettledSegmentsError::Io(io::Error::from(err))),
            }
        }
        #[cfg(not(unix))]
        {
            match fs::symlink_metadata(self.display_path.join(name)) {
                Ok(meta) => {
                    let kind = if meta.file_type().is_symlink() {
                        EntryKind::SymlinkOrOther
                    } else if meta.is_dir() {
                        EntryKind::Directory
                    } else if meta.file_type().is_file() {
                        EntryKind::RegularFile
                    } else {
                        EntryKind::SymlinkOrOther
                    };
                    Ok(Some(kind))
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(SettledSegmentsError::Io(err)),
            }
        }
    }

    pub(super) fn link_from_dir(
        &self,
        src_name: &str,
        dst_dir: &Self,
        dst_name: &str,
    ) -> io::Result<()> {
        reject_unsafe_component(src_name)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        reject_unsafe_component(dst_name)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        #[cfg(unix)]
        {
            use rustix::fs::{AtFlags, linkat};

            linkat(&self.fd, src_name, &dst_dir.fd, dst_name, AtFlags::empty())
                .map_err(io::Error::from)
        }
        #[cfg(not(unix))]
        {
            fs::hard_link(
                self.display_path.join(src_name),
                dst_dir.display_path.join(dst_name),
            )
        }
    }

    pub(super) fn rename_noclobber_to_dir(
        &self,
        src_name: &str,
        dst_dir: &Self,
        dst_name: &str,
    ) -> io::Result<()> {
        reject_unsafe_component(src_name)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        reject_unsafe_component(dst_name)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        #[cfg(unix)]
        {
            use rustix::fs::{AtFlags, linkat, unlinkat};
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "redox"
            ))]
            {
                use rustix::{
                    fs::{RenameFlags, renameat_with},
                    io::Errno,
                };

                match renameat_with(
                    &self.fd,
                    src_name,
                    &dst_dir.fd,
                    dst_name,
                    RenameFlags::NOREPLACE,
                ) {
                    Ok(()) => return Ok(()),
                    Err(Errno::NOSYS | Errno::INVAL) => {}
                    Err(err) => return Err(io::Error::from(err)),
                }
            }
            linkat(&self.fd, src_name, &dst_dir.fd, dst_name, AtFlags::empty())
                .map_err(io::Error::from)?;
            unlinkat(&self.fd, src_name, AtFlags::empty()).map_err(io::Error::from)
        }
        #[cfg(not(unix))]
        {
            let dst_path = dst_dir.display_path.join(dst_name);
            if dst_path.exists() {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            fs::rename(self.display_path.join(src_name), dst_path)
        }
    }

    pub(super) fn rename_replace_to_dir(
        &self,
        src_name: &str,
        dst_dir: &Self,
        dst_name: &str,
    ) -> io::Result<()> {
        reject_unsafe_component(src_name)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        reject_unsafe_component(dst_name)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        #[cfg(unix)]
        {
            rustix::fs::renameat(&self.fd, src_name, &dst_dir.fd, dst_name).map_err(io::Error::from)
        }
        #[cfg(not(unix))]
        {
            fs::rename(
                self.display_path.join(src_name),
                dst_dir.display_path.join(dst_name),
            )
        }
    }

    pub(super) fn unlink_file(&self, name: &str) -> Result<(), SettledSegmentsError> {
        reject_unsafe_component(name)?;
        #[cfg(unix)]
        {
            use rustix::fs::{AtFlags, unlinkat};
            unlinkat(&self.fd, name, AtFlags::empty()).map_err(io::Error::from)?;
        }
        #[cfg(not(unix))]
        {
            fs::remove_file(self.display_path.join(name))?;
        }
        Ok(())
    }

    pub(super) fn remove_empty_dir(&self, name: &str) -> io::Result<()> {
        reject_unsafe_component(name)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
        #[cfg(unix)]
        {
            use rustix::fs::{AtFlags, unlinkat};
            unlinkat(&self.fd, name, AtFlags::REMOVEDIR).map_err(io::Error::from)
        }
        #[cfg(not(unix))]
        {
            fs::remove_dir(self.display_path.join(name))
        }
    }

    pub(super) fn list_entry_names(&self) -> Result<Vec<String>, SettledSegmentsError> {
        #[cfg(unix)]
        {
            use rustix::fs::{Mode, OFlags, RawDir, openat};

            let iter_fd = openat(
                &self.fd,
                ".",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(io::Error::from)?;
            let mut names = Vec::new();
            let mut buf = [std::mem::MaybeUninit::uninit(); 8192];
            let mut dir = RawDir::new(&iter_fd, &mut buf);
            while let Some(next_entry) = dir.next() {
                let entry = next_entry.map_err(io::Error::from)?;
                let raw_name = entry.file_name().to_bytes();
                if raw_name == b"." || raw_name == b".." {
                    continue;
                }
                let name = std::str::from_utf8(raw_name).map_err(|_| {
                    SettledSegmentsError::InvalidLayout(
                        "non-UTF-8 entry found inside verified directory".to_owned(),
                    )
                })?;
                reject_unsafe_component(name)?;
                names.push(name.to_owned());
            }
            Ok(names)
        }
        #[cfg(not(unix))]
        {
            let mut names = Vec::new();
            for entry in fs::read_dir(&self.display_path)? {
                let entry = entry?;
                let os_name = entry.file_name();
                let name = os_name.to_str().ok_or_else(|| {
                    SettledSegmentsError::InvalidLayout(
                        "non-UTF-8 entry found inside verified directory".to_owned(),
                    )
                })?;
                reject_unsafe_component(name)?;
                names.push(name.to_owned());
            }
            Ok(names)
        }
    }

    pub(super) fn sync(&self) -> Result<(), SettledSegmentsError> {
        #[cfg(unix)]
        {
            rustix::fs::fsync(&self.fd).map_err(io::Error::from)?;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            File::open(&self.display_path)?.sync_all()?;
            Ok(())
        }
    }

    pub(super) fn write_file_atomic(
        &self,
        target_name: &str,
        bytes: &[u8],
    ) -> Result<(), SettledSegmentsError> {
        reject_unsafe_component(target_name)?;
        let (temp_name, mut staged) = self.create_temp_file(".atomic")?;
        let write_outcome = (|| -> Result<(), SettledSegmentsError> {
            staged.write_all(bytes)?;
            staged.sync_all()?;
            drop(staged);
            self.rename_replace_to_dir(&temp_name, self, target_name)?;
            Ok(())
        })();
        if let Err(err) = write_outcome {
            let _ = self.unlink_file(&temp_name);
            return Err(err);
        }
        self.sync()
    }
}

#[cfg(not(unix))]
fn verify_existing_dir_no_symlink(path: &Path) -> Result<(), SettledSegmentsError> {
    let meta = fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "refusing symlink directory {}",
            path.display()
        )));
    }
    if !meta.is_dir() {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "expected directory at {}",
            path.display()
        )));
    }
    Ok(())
}

pub(super) fn reject_unsafe_component(component: &str) -> Result<(), SettledSegmentsError> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.contains('/')
        || component.contains('\\')
        || component.contains('\0')
    {
        return Err(SettledSegmentsError::InvalidLayout(format!(
            "unsafe path component {component:?}"
        )));
    }
    Ok(())
}
