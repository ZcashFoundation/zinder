//! Positional byte-source trait and byte-range descriptor for storage-neutral
//! settled segment readers.
//!
//! Keep this module free of storage-engine (`RocksDB`, SQL) imports so the same
//! segment decoders work identically over local files (`File` `pread`), in-memory
//! buffers (`Bytes`), asynchronous database slices (`PostgreSQL` `substring`),
//! and HTTP `Range` fetches.

use std::{collections::VecDeque, fs::File, io, num::NonZeroU64, ops::Range, sync::Arc};

use bytes::Bytes;
use parking_lot::Mutex;

/// Half-open byte interval `[offset, offset + len)` within a segment artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    /// Starting byte offset relative to the beginning of the segment.
    pub offset: u64,
    /// Number of contiguous bytes covered by the range.
    pub len: usize,
}

impl ByteRange {
    /// Creates a byte range starting at `offset` spanning `len` bytes.
    #[must_use]
    pub const fn new(offset: u64, len: usize) -> Self {
        Self { offset, len }
    }

    /// Returns the exclusive end offset, or `None` on `u64` overflow.
    #[must_use]
    pub fn end(&self) -> Option<u64> {
        u64::try_from(self.len)
            .ok()
            .and_then(|span| self.offset.checked_add(span))
    }

    /// Returns whether this range covers zero bytes.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

type DiscardScanPages<S> = fn(&S, u64, NonZeroU64);

fn bounded_file_scan(file: &File, constrained: bool) -> Option<DiscardScanPages<File>> {
    if !constrained {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{Advice, fadvise};
        if let Err(error) = fadvise(file, 0, None, Advice::Random) {
            tracing::debug!(target: "zinder::store", %error, "could not advise settled segment readahead");
        }
        Some(|source, offset, len| {
            #[cfg(test)]
            {
                use std::os::unix::fs::MetadataExt as _;
                tracing::trace!(
                    target: "zinder::store",
                    event = "settled_scan_discard_pages",
                    inode = source.metadata().map_or(0, |meta| meta.ino()),
                    offset,
                    len = len.get()
                );
            }
            if let Err(error) = fadvise(source, offset, Some(len), Advice::DontNeed) {
                tracing::debug!(target: "zinder::store", %error, "could not discard settled segment scan pages");
            }
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        None
    }
}

/// Carries incomplete consumed pages across scans of one immutable file.
pub(crate) struct TrackedScanPages<S> {
    discard: DiscardScanPages<S>,
    page_size: NonZeroU64,
    partial: Mutex<VecDeque<Range<u64>>>,
}

impl TrackedScanPages<File> {
    pub(crate) fn for_file(file: &File, constrained: bool) -> Option<Box<Self>> {
        let discard = bounded_file_scan(file, constrained)?;
        #[cfg(target_os = "linux")]
        {
            let page_size = NonZeroU64::new(u64::try_from(rustix::param::page_size()).ok()?)?;
            Some(Box::new(Self {
                discard,
                page_size,
                partial: Mutex::new(VecDeque::new()),
            }))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = discard;
            None
        }
    }
}

impl<S> TrackedScanPages<S> {
    fn completed_pages(&self, mut consumed: Range<u64>) -> Option<Range<u64>> {
        let mut partial = self.partial.lock();
        while let Some(index) = partial
            .iter()
            .position(|old| old.start <= consumed.end && consumed.start <= old.end)
        {
            let old = partial.remove(index)?;
            consumed.start = consumed.start.min(old.start);
            consumed.end = consumed.end.max(old.end);
        }
        let page_size = self.page_size.get();
        let end = consumed.end - consumed.end % page_size;
        let start = consumed
            .start
            .checked_next_multiple_of(page_size)
            .filter(|start| *start < end);
        let Some(start) = start else {
            Self::remember(&mut partial, consumed);
            return None;
        };
        Self::remember(&mut partial, consumed.start..start);
        Self::remember(&mut partial, end..consumed.end);
        drop(partial);
        Some(start..end)
    }

    fn remember(partial: &mut VecDeque<Range<u64>>, extent: Range<u64>) {
        const MAX_PARTIAL_EXTENTS: usize = 16;
        if !extent.is_empty() {
            if partial.len() == MAX_PARTIAL_EXTENTS {
                // Forgetting a fragment leaves its page cached until reclaim or another scan.
                partial.pop_front();
            }
            partial.push_back(extent);
        }
    }
}

/// Collects consumed payload ranges before advising the kernel.
pub(crate) struct ScanDropBehind<'a, S> {
    source: &'a S,
    pages: &'a TrackedScanPages<S>,
    pending: Option<(u64, u64)>,
}

impl<'a, S> ScanDropBehind<'a, S> {
    pub(crate) const fn tracked(source: &'a S, pages: &'a TrackedScanPages<S>) -> Self {
        Self {
            source,
            pages,
            pending: None,
        }
    }

    pub(crate) fn consumed(&mut self, range: ByteRange) {
        const BATCH_BYTES: u64 = 1024 * 1024;
        let Some(end) = range.end() else {
            return;
        };
        if let Some((start, previous_end)) = self.pending
            && range.offset <= previous_end
            && end >= start
        {
            self.pending = Some((start.min(range.offset), previous_end.max(end)));
        } else {
            self.flush();
            self.pending = Some((range.offset, end));
        }
        if self
            .pending
            .is_some_and(|(start, end)| end - start >= BATCH_BYTES)
        {
            self.flush();
        }
    }

    fn flush(&mut self) {
        let Some((start, end)) = self.pending.take() else {
            return;
        };
        if let Some(extent) = self.pages.completed_pages(start..end)
            && let Some(len) = NonZeroU64::new(extent.end - extent.start)
        {
            (self.pages.discard)(self.source, extent.start, len);
        }
    }
}

impl<S> Drop for ScanDropBehind<'_, S> {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Engine-neutral positional read interface for settled segment blobs.
pub trait PositionalByteSource: Send + Sync {
    /// Returns the total byte length of the segment artifact.
    ///
    /// # Errors
    ///
    /// Returns [`io::Error`] if the underlying metadata query fails.
    fn total_len(&self) -> io::Result<u64>;

    /// Reads exactly `buf.len()` bytes starting at `offset` without mutating any
    /// shared cursor.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::UnexpectedEof`] when fewer than `buf.len()`
    /// bytes exist at `offset`, or any underlying I/O failure.
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;

    /// Reads `len` bytes starting at `offset` into an owned buffer.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::UnexpectedEof`] when fewer than `len` bytes
    /// exist at `offset`, or any underlying I/O failure.
    fn read_vec_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut owned = vec![0_u8; len];
        self.read_exact_at(offset, &mut owned)?;
        Ok(owned)
    }
}

impl PositionalByteSource for File {
    fn total_len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        read_exact_file_at(self, offset, buf)
    }

    fn read_vec_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        read_vec_file_at(self, offset, len)
    }
}

impl PositionalByteSource for Bytes {
    fn total_len(&self) -> io::Result<u64> {
        u64::try_from(self.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "byte buffer length exceeds u64")
        })
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let start =
            usize::try_from(offset).map_err(|_| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let end = start
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let slice = self
            .get(start..end)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        buf.copy_from_slice(slice);
        Ok(())
    }

    fn read_vec_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let start =
            usize::try_from(offset).map_err(|_| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let slice = self
            .get(start..end)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        Ok(slice.to_vec())
    }
}

impl<S: PositionalByteSource + ?Sized> PositionalByteSource for Arc<S> {
    fn total_len(&self) -> io::Result<u64> {
        (**self).total_len()
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        (**self).read_exact_at(offset, buf)
    }

    fn read_vec_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        (**self).read_vec_at(offset, len)
    }
}

#[cfg(unix)]
fn read_exact_file_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

#[cfg(not(unix))]
fn read_exact_file_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    use std::os::windows::fs::FileExt as _;
    let mut read_total = 0_usize;
    let mut cursor_offset = offset;
    while read_total < buf.len() {
        let count = file.seek_read(&mut buf[read_total..], cursor_offset)?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        read_total += count;
        cursor_offset += count as u64;
    }
    Ok(())
}

#[cfg(unix)]
fn read_vec_file_at(file: &File, mut offset: u64, len: usize) -> io::Result<Vec<u8>> {
    use rustix::{
        buffer::spare_capacity,
        io::{pread, retry_on_intr},
    };

    let mut owned = Vec::with_capacity(len);
    while owned.len() < len {
        let count = retry_on_intr(|| pread(file, spare_capacity(&mut owned), offset))?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        offset += count as u64;
    }
    owned.truncate(len);
    Ok(owned)
}

#[cfg(not(unix))]
fn read_vec_file_at(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut owned = vec![0_u8; len];
    read_exact_file_at(file, offset, &mut owned)?;
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, fs, sync::Arc};

    use bytes::Bytes;
    use tempfile::TempDir;

    use super::{ByteRange, PositionalByteSource, ScanDropBehind, TrackedScanPages};

    type TestOutcome<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    #[test]
    fn interleaved_scan_fragments_only_discard_fully_consumed_pages() -> TestOutcome {
        let discarded = RefCell::new(Vec::new());
        let pages = TrackedScanPages {
            discard: |out: &RefCell<Vec<(u64, u64)>>, offset, len| {
                out.borrow_mut().push((offset, len.get()));
            },
            page_size: std::num::NonZeroU64::new(4_096).ok_or("expected nonzero page size")?,
            partial: parking_lot::Mutex::default(),
        };
        for (offset, len) in [(0, 1024), (8192, 2048), (2048, 2048)] {
            ScanDropBehind::tracked(&discarded, &pages).consumed(ByteRange::new(offset, len));
        }
        assert!(discarded.borrow().is_empty());
        ScanDropBehind::tracked(&discarded, &pages).consumed(ByteRange::new(1024, 1024));
        assert_eq!(*discarded.borrow(), [(0, 4096)]);
        ScanDropBehind::tracked(&discarded, &pages).consumed(ByteRange::new(10240, 2048));
        assert_eq!(*discarded.borrow(), [(0, 4096), (8192, 4096)]);
        Ok(())
    }

    #[test]
    fn scan_discards_only_consumed_extents_in_bounded_batches() -> TestOutcome {
        let discarded = RefCell::new(Vec::new());
        let pages = TrackedScanPages {
            discard: |out: &RefCell<Vec<(u64, u64)>>, offset, len| {
                out.borrow_mut().push((offset, len.get()));
            },
            page_size: std::num::NonZeroU64::new(4_096).ok_or("expected nonzero page size")?,
            partial: parking_lot::Mutex::default(),
        };
        let mut scan = ScanDropBehind::tracked(&discarded, &pages);
        for offset in [524_288, 0, 3_145_728, 3_670_016, 5_242_880] {
            scan.consumed(ByteRange::new(offset, 524_288));
        }
        assert_eq!(
            *discarded.borrow(),
            [(0, 1_048_576), (3_145_728, 1_048_576)]
        );
        drop(scan);
        assert_eq!(
            *discarded.borrow(),
            [(0, 1_048_576), (3_145_728, 1_048_576), (5_242_880, 524_288)]
        );
        Ok(())
    }

    #[test]
    fn byte_range_arithmetic_and_empty_checks() {
        let empty = ByteRange::new(10, 0);
        assert!(empty.is_empty());
        assert_eq!(empty.end(), Some(10));

        let non_empty = ByteRange::new(128, 512);
        assert!(!non_empty.is_empty());
        assert_eq!(non_empty.end(), Some(640));

        let overflow = ByteRange::new(u64::MAX - 2, 10);
        assert_eq!(overflow.end(), None);
    }

    #[test]
    fn bytes_file_and_arc_sources_read_positionally_and_fail_on_eof() -> TestOutcome {
        let payload = b"zinder-positional-source-bytes";
        let bytes_source = Bytes::from_static(payload);
        assert_eq!(bytes_source.total_len()?, u64::try_from(payload.len())?);
        assert_eq!(bytes_source.read_vec_at(7, 10)?, b"positional");
        assert!(matches!(
            bytes_source.read_vec_at(u64::try_from(payload.len())?, 1),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof
        ));

        let shared_source = Arc::new(bytes_source);
        let mut exact = [0_u8; 6];
        shared_source.read_exact_at(0, &mut exact)?;
        assert_eq!(&exact, b"zinder");

        let dir = TempDir::new()?;
        let path = dir.path().join("sample.bin");
        fs::write(&path, payload)?;
        let file = fs::File::open(&path)?;
        assert_eq!(file.total_len()?, u64::try_from(payload.len())?);
        assert_eq!(file.read_vec_at(18, 6)?, b"source");
        assert!(matches!(
            file.read_vec_at(100, 4),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof
        ));
        Ok(())
    }
}
