//! Admission and fail-closed block/transaction reader for `ZSBLKB01` segments.

use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
    sync::atomic::Ordering,
};

use sha2::{Digest, Sha256};
use zinder_core::BlockHeight;

use super::{
    RawBlockCodec, RawBlockSegmentIdentity, SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES,
    SettledRawBlockSegmentError,
    format::{
        DecodedRowIndexEntry, FRAME_PREFIX_LEN, FRAME_PREFIX_U32, HEADER_LEN, IndexedBlock,
        ROW_INDEX_ENTRY_LEN, SegmentHeader, TRAILER_LEN, TX_SLICE_ENTRY_LEN, TxSliceEntry,
        VALIDATION_INVALID, VALIDATION_VALID, decode_frame_prefix, frame_span,
        max_stored_payload_bytes,
    },
    verity,
};

/// Admission options when opening a [`SettledRawBlockSegmentReader`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SettledRawBlockSegmentOpenOptions {
    /// Fail admission if Linux `fs-verity` is not active and verified on the file.
    pub require_fs_verity: bool,
}

/// Authenticated reader over one settled `ZSBLKB01` raw block segment.
pub struct SettledRawBlockSegmentReader {
    identity: RawBlockSegmentIdentity,
    kernel_verified: bool,
    file: File,
    slab_offset: u64,
    slab_len: u32,
    rows: Vec<IndexedBlock>,
    tx_entries: Vec<TxSliceEntry>,
}

pub(super) struct PortableFrameDigest {
    pub(super) rel_offset: u32,
    pub(super) frame_len: u32,
    pub(super) payload_sha256: [u8; 32],
}

impl SettledRawBlockSegmentReader {
    /// Opens and authenticates a `ZSBLKB01` segment against `expected`.
    pub fn open(
        path: &Path,
        expected: &RawBlockSegmentIdentity,
        options: SettledRawBlockSegmentOpenOptions,
    ) -> Result<Self, SettledRawBlockSegmentError> {
        let file = verity::open_regular_file(path)?;
        let file_len = file.metadata()?.len();
        let measured_verity = verity::measure(&file)?;
        let kernel_verified =
            verity::check_pinned_verity(expected.fs_verity_sha256, measured_verity)?;
        if options.require_fs_verity && !kernel_verified {
            return Err(SettledRawBlockSegmentError::FsVerityRequired);
        }

        let mut reader_ref = &file;
        let mut header_bytes = [0_u8; HEADER_LEN];
        reader_ref.read_exact(&mut header_bytes)?;
        let header = SegmentHeader::decode(&header_bytes)?;
        header.matches_identity(expected, file_len)?;

        if kernel_verified {
            admit_protected(file, expected, header)
        } else {
            admit_portable(file, expected, header, &header_bytes)
        }
    }

    /// Returns whether admission proved kernel `fs-verity` protection.
    #[must_use]
    pub const fn kernel_verified(&self) -> bool {
        self.kernel_verified
    }

    /// Returns the authenticated segment identity.
    #[must_use]
    pub const fn identity(&self) -> RawBlockSegmentIdentity {
        self.identity
    }

    /// Returns the number of blocks stored in this segment.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.rows.len()
    }

    /// Reads, decompresses if needed, verifies, and returns the exact consensus
    /// serialized raw block bytes at `height`.
    pub fn read_raw_block(
        &self,
        height: BlockHeight,
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let row_idx = self.resolve_row_index(height)?;
        self.read_verified_block(row_idx)
    }

    /// Reads and verifies the containing block once, slices `[tx_offset..tx_offset + tx_len]`,
    /// verifies `tx_sha256`, and returns the exact consensus serialized transaction bytes.
    pub fn read_raw_transaction(
        &self,
        height: BlockHeight,
        tx_index: u32,
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let row_idx = self.resolve_row_index(height)?;
        let row = &self.rows[row_idx];
        if row.validation_state.load(Ordering::Acquire) == VALIDATION_INVALID {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment row previously failed validation".to_owned(),
            ));
        }
        if tx_index >= row.tx_count {
            return Err(SettledRawBlockSegmentError::TransactionIndexOutOfRange {
                height,
                tx_index,
                tx_count: row.tx_count,
            });
        }
        let global_tx_idx = row
            .tx_start_index
            .checked_add(tx_index)
            .and_then(|idx| usize::try_from(idx).ok())
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat("transaction index overflow".to_owned())
            })?;
        let tx_entry = self.tx_entries.get(global_tx_idx).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "transaction slice entry out of bounds".to_owned(),
            )
        })?;
        let block_bytes = self.read_verified_block(row_idx)?;
        slice_and_verify_tx(&block_bytes, tx_entry).inspect_err(|_| {
            row.mark_invalid();
        })
    }

    fn resolve_row_index(&self, height: BlockHeight) -> Result<usize, SettledRawBlockSegmentError> {
        let seg_start = self.identity.start_height;
        let seg_end = self.identity.end_block.height;
        if height < seg_start || height > seg_end {
            return Err(SettledRawBlockSegmentError::HeightOutOfRange {
                height,
                start: seg_start,
                end: seg_end,
            });
        }
        usize::try_from(height.value() - seg_start.value()).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("height index overflow".to_owned())
        })
    }

    fn read_verified_block(&self, row_idx: usize) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let row = &self.rows[row_idx];
        let state = row.validation_state.load(Ordering::Acquire);
        if state == VALIDATION_INVALID {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment row previously failed validation".to_owned(),
            ));
        }
        match self.load_and_verify_frame(row_idx, row, state) {
            Ok(raw_block) => {
                row.mark_valid();
                Ok(raw_block)
            }
            Err(err) => {
                row.mark_invalid();
                Err(err)
            }
        }
    }

    fn load_and_verify_frame(
        &self,
        row_idx: usize,
        row: &IndexedBlock,
        state: u8,
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let (rel_offset, frame_len) = frame_span(&self.rows, self.slab_len, row_idx)?;
        let frame_len_usize = usize::try_from(frame_len).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("frame length exceeds usize".to_owned())
        })?;
        let frame = read_slab_at(
            &self.file,
            frame_len_usize,
            self.slab_offset + u64::from(rel_offset),
        )?;
        self.decode_and_verify_frame(row, &frame, state)
    }

    fn decode_and_verify_frame(
        &self,
        row: &IndexedBlock,
        frame: &[u8],
        state: u8,
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let stored_len = decode_frame_prefix(frame, self.identity.codec)?;
        if FRAME_PREFIX_LEN + stored_len != frame.len() {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block frame prefix length disagrees with index".to_owned(),
            ));
        }
        let payload = &frame[FRAME_PREFIX_LEN..];
        let verify_digests = !self.kernel_verified || state != VALIDATION_VALID;
        if verify_digests && <[u8; 32]>::from(Sha256::digest(payload)) != row.payload_sha256 {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block payload SHA-256 mismatch on read".to_owned(),
            ));
        }
        let expected_len = usize::try_from(row.uncompressed_len).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat(
                "uncompressed length exceeds usize".to_owned(),
            )
        })?;
        let raw_block = decode_payload(payload, expected_len, self.identity.codec)?;
        if verify_digests && <[u8; 32]>::from(Sha256::digest(&raw_block)) != row.uncompressed_sha256
        {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block uncompressed SHA-256 mismatch on read".to_owned(),
            ));
        }
        Ok(raw_block)
    }
}

fn slice_and_verify_tx(
    block_bytes: &[u8],
    tx_entry: &TxSliceEntry,
) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
    let start = usize::try_from(tx_entry.offset).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("tx offset exceeds usize".to_owned())
    })?;
    let len = usize::try_from(tx_entry.len).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("tx length exceeds usize".to_owned())
    })?;
    let end = start.checked_add(len).ok_or_else(|| {
        SettledRawBlockSegmentError::InvalidFormat("tx slice end overflows usize".to_owned())
    })?;
    let tx_bytes = block_bytes.get(start..end).ok_or_else(|| {
        SettledRawBlockSegmentError::InvalidFormat(
            "tx slice falls outside decompressed block bytes".to_owned(),
        )
    })?;
    if <[u8; 32]>::from(Sha256::digest(tx_bytes)) != tx_entry.sha256 {
        return Err(SettledRawBlockSegmentError::PayloadIntegrity(
            "raw transaction SHA-256 mismatch on read".to_owned(),
        ));
    }
    Ok(tx_bytes.to_vec())
}

fn decode_payload(
    payload: &[u8],
    expected_len: usize,
    codec: RawBlockCodec,
) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
    if expected_len == 0 || expected_len > SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES {
        return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
            "raw block uncompressed length {expected_len} out of bounds"
        )));
    }
    match codec {
        RawBlockCodec::None => {
            if payload.len() != expected_len {
                return Err(SettledRawBlockSegmentError::PayloadIntegrity(format!(
                    "uncompressed block length {} differs from expected {expected_len}",
                    payload.len()
                )));
            }
            Ok(payload.to_vec())
        }
        RawBlockCodec::Zstd => {
            let decompressed = zstd::bulk::decompress(payload, expected_len).map_err(|err| {
                SettledRawBlockSegmentError::PayloadIntegrity(format!(
                    "raw block zstd decompression failed: {err}"
                ))
            })?;
            if decompressed.len() != expected_len {
                return Err(SettledRawBlockSegmentError::PayloadIntegrity(format!(
                    "decompressed block length {} differs from expected {expected_len}",
                    decompressed.len()
                )));
            }
            Ok(decompressed)
        }
    }
}

fn admit_protected(
    file: File,
    expected: &RawBlockSegmentIdentity,
    header: SegmentHeader,
) -> Result<SettledRawBlockSegmentReader, SettledRawBlockSegmentError> {
    let mut input = &file;
    input.seek(SeekFrom::Start(header.tx_table_offset))?;
    let tx_count = header.tx_entry_count()?;
    let block_count = usize::try_from(header.block_count).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("block count exceeds usize".to_owned())
    })?;
    let slab_len = header.slab_len()?;
    let tx_entries = read_tx_entries(&mut input, tx_count)?;
    let entries = read_index_entries(&mut input, block_count)?;
    let mut trailer = [0_u8; TRAILER_LEN];
    input.read_exact(&mut trailer)?;
    if trailer != expected.sha256 {
        return Err(SettledRawBlockSegmentError::PayloadIntegrity(
            "protected raw block segment trailer SHA-256 mismatch".to_owned(),
        ));
    }
    let rows = validate_entries(&entries, &tx_entries, slab_len, &header, None)?;
    Ok(SettledRawBlockSegmentReader {
        identity: *expected,
        kernel_verified: true,
        file,
        slab_offset: header.slab_offset,
        slab_len,
        rows,
        tx_entries,
    })
}

fn admit_portable(
    file: File,
    expected: &RawBlockSegmentIdentity,
    header: SegmentHeader,
    header_bytes: &[u8; HEADER_LEN],
) -> Result<SettledRawBlockSegmentReader, SettledRawBlockSegmentError> {
    let mut buffered = BufReader::new(&file);
    let mut whole_digest = Sha256::new();
    whole_digest.update(header_bytes);
    let count = usize::try_from(header.block_count).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("block count exceeds usize".to_owned())
    })?;
    let tx_count = header.tx_entry_count()?;
    let slab_len = header.slab_len()?;
    let streamed = hash_slab_frames(
        &mut buffered,
        count,
        slab_len,
        expected.codec,
        &mut whole_digest,
    )?;
    let tx_entries = hash_tx_entries(&mut buffered, tx_count, &mut whole_digest)?;
    let entries = hash_index_entries(&mut buffered, count, &mut whole_digest)?;
    let mut trailer = [0_u8; TRAILER_LEN];
    buffered.read_exact(&mut trailer)?;
    let computed: [u8; 32] = whole_digest.finalize().into();
    if !buffered.fill_buf()?.is_empty() || computed != expected.sha256 || trailer != computed {
        return Err(SettledRawBlockSegmentError::PayloadIntegrity(
            "portable raw block segment length or SHA-256 mismatch".to_owned(),
        ));
    }
    let rows = validate_entries(&entries, &tx_entries, slab_len, &header, Some(&streamed))?;
    Ok(SettledRawBlockSegmentReader {
        identity: *expected,
        kernel_verified: false,
        file,
        slab_offset: header.slab_offset,
        slab_len,
        rows,
        tx_entries,
    })
}

fn hash_slab_frames(
    input: &mut impl BufRead,
    count: usize,
    slab_len: u32,
    codec: RawBlockCodec,
    whole_digest: &mut Sha256,
) -> Result<Vec<PortableFrameDigest>, SettledRawBlockSegmentError> {
    let mut frames = Vec::with_capacity(count);
    let mut rel_offset = 0_u64;
    for _ in 0..count {
        let mut prefix = [0_u8; FRAME_PREFIX_LEN];
        input.read_exact(&mut prefix)?;
        whole_digest.update(prefix);
        let payload_len = decode_frame_prefix(&prefix, codec)?;
        let frame_len = u32::try_from(FRAME_PREFIX_LEN + payload_len).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("raw block frame exceeds u32".to_owned())
        })?;
        let next_offset = rel_offset + u64::from(frame_len);
        if next_offset > u64::from(slab_len) {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "streamed slab frames exceed slab length".to_owned(),
            ));
        }
        let payload_sha256 = hash_streamed_payload(input, payload_len, whole_digest)?;
        let rel_u32 = u32::try_from(rel_offset).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("slab offset exceeds u32".to_owned())
        })?;
        frames.push(PortableFrameDigest {
            rel_offset: rel_u32,
            frame_len,
            payload_sha256,
        });
        rel_offset = next_offset;
    }
    if rel_offset != u64::from(slab_len) {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "streamed slab bytes do not fill header slab length".to_owned(),
        ));
    }
    Ok(frames)
}

fn hash_streamed_payload(
    input: &mut impl BufRead,
    payload_len: usize,
    whole_digest: &mut Sha256,
) -> Result<[u8; 32], SettledRawBlockSegmentError> {
    let mut payload_digest = Sha256::new();
    let mut remaining = payload_len;
    while remaining > 0 {
        let available = input.fill_buf()?;
        if available.is_empty() {
            return Err(SettledRawBlockSegmentError::Io(
                io::ErrorKind::UnexpectedEof.into(),
            ));
        }
        let take = remaining.min(available.len());
        payload_digest.update(&available[..take]);
        whole_digest.update(&available[..take]);
        input.consume(take);
        remaining -= take;
    }
    Ok(payload_digest.finalize().into())
}

fn read_tx_entries(
    input: &mut impl Read,
    count: usize,
) -> Result<Vec<TxSliceEntry>, SettledRawBlockSegmentError> {
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let mut raw = [0_u8; TX_SLICE_ENTRY_LEN];
        input.read_exact(&mut raw)?;
        entries.push(TxSliceEntry::decode(&raw)?);
    }
    Ok(entries)
}

fn hash_tx_entries(
    input: &mut impl BufRead,
    count: usize,
    whole_digest: &mut Sha256,
) -> Result<Vec<TxSliceEntry>, SettledRawBlockSegmentError> {
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let mut raw = [0_u8; TX_SLICE_ENTRY_LEN];
        input.read_exact(&mut raw)?;
        whole_digest.update(raw);
        entries.push(TxSliceEntry::decode(&raw)?);
    }
    Ok(entries)
}

fn read_index_entries(
    input: &mut impl Read,
    count: usize,
) -> Result<Vec<DecodedRowIndexEntry>, SettledRawBlockSegmentError> {
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let mut raw = [0_u8; ROW_INDEX_ENTRY_LEN];
        input.read_exact(&mut raw)?;
        entries.push(DecodedRowIndexEntry::decode(&raw)?);
    }
    Ok(entries)
}

fn hash_index_entries(
    input: &mut impl BufRead,
    count: usize,
    whole_digest: &mut Sha256,
) -> Result<Vec<DecodedRowIndexEntry>, SettledRawBlockSegmentError> {
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let mut raw = [0_u8; ROW_INDEX_ENTRY_LEN];
        input.read_exact(&mut raw)?;
        whole_digest.update(raw);
        entries.push(DecodedRowIndexEntry::decode(&raw)?);
    }
    Ok(entries)
}

pub(super) fn validate_entries(
    entries: &[DecodedRowIndexEntry],
    tx_entries: &[TxSliceEntry],
    slab_len: u32,
    header: &SegmentHeader,
    streamed: Option<&[PortableFrameDigest]>,
) -> Result<Vec<IndexedBlock>, SettledRawBlockSegmentError> {
    let last = validate_index_envelope(entries, slab_len, header)?;
    if last.block_hash_wire != header.last_block_hash_wire {
        return Err(SettledRawBlockSegmentError::ChainLinkage(
            "last row hash disagrees with header last_block_hash".to_owned(),
        ));
    }
    let codec = header.codec()?;
    let mut rows = Vec::with_capacity(entries.len());
    let mut expected_tx_start = 0_u32;
    for (idx, entry) in entries.iter().enumerate() {
        let next_rel = entries
            .get(idx + 1)
            .map_or(slab_len, |next| next.slab_rel_offset);
        let frame_len = next_rel.checked_sub(entry.slab_rel_offset).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("non-ascending row slab offsets".to_owned())
        })?;
        validate_entry_bounds(entry, frame_len, codec)?;
        expected_tx_start = validate_entry_tx_slices(entry, expected_tx_start, tx_entries)?;
        if let Some(frames) = streamed {
            let frame = frames.get(idx).ok_or_else(|| {
                SettledRawBlockSegmentError::PayloadIntegrity(
                    "streamed slab frame count disagrees with row index".to_owned(),
                )
            })?;
            if frame.rel_offset != entry.slab_rel_offset
                || frame.frame_len != frame_len
                || frame.payload_sha256 != entry.payload_sha256
            {
                return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                    "streamed slab frame disagrees with row index".to_owned(),
                ));
            }
        }
        rows.push(IndexedBlock::from_entry(entry));
    }
    if usize::try_from(expected_tx_start).ok() != Some(tx_entries.len()) {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "row index transaction counts do not match tx table length".to_owned(),
        ));
    }
    Ok(rows)
}

fn validate_index_envelope<'a>(
    entries: &'a [DecodedRowIndexEntry],
    slab_len: u32,
    header: &SegmentHeader,
) -> Result<&'a DecodedRowIndexEntry, SettledRawBlockSegmentError> {
    if entries.is_empty() || entries[0].slab_rel_offset != 0 {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "row index must be non-empty and start at slab offset 0".to_owned(),
        ));
    }
    if usize::try_from(header.block_count).ok() != Some(entries.len()) {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "row index entry count disagrees with header block count".to_owned(),
        ));
    }
    if slab_len <= FRAME_PREFIX_U32 {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "raw block slab is shorter than one frame prefix".to_owned(),
        ));
    }
    entries
        .last()
        .ok_or_else(|| SettledRawBlockSegmentError::InvalidFormat("row index is empty".to_owned()))
}

fn validate_entry_bounds(
    entry: &DecodedRowIndexEntry,
    frame_len: u32,
    codec: RawBlockCodec,
) -> Result<(), SettledRawBlockSegmentError> {
    let max_block_u32 = u32::try_from(SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("max block size exceeds u32".to_owned())
    })?;
    if entry.uncompressed_len == 0 || entry.uncompressed_len > max_block_u32 {
        return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
            "raw block uncompressed length {} exceeds bounds",
            entry.uncompressed_len
        )));
    }
    let max_stored_u32 = u32::try_from(max_stored_payload_bytes(codec)).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("max stored payload exceeds u32".to_owned())
    })?;
    let min_frame = FRAME_PREFIX_U32 + 1;
    let max_frame = FRAME_PREFIX_U32.saturating_add(max_stored_u32);
    if frame_len < min_frame || frame_len > max_frame {
        return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
            "raw block frame span {frame_len} is out of bounds"
        )));
    }
    if codec == RawBlockCodec::None {
        if frame_len != FRAME_PREFIX_U32.saturating_add(entry.uncompressed_len) {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "uncompressed frame span disagrees with uncompressed_len".to_owned(),
            ));
        }
        if entry.payload_sha256 != entry.uncompressed_sha256 {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "uncompressed payload digest disagrees with uncompressed block digest".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_entry_tx_slices(
    entry: &DecodedRowIndexEntry,
    expected_tx_start: u32,
    tx_entries: &[TxSliceEntry],
) -> Result<u32, SettledRawBlockSegmentError> {
    if entry.tx_start_index != expected_tx_start {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "non-contiguous transaction slice start index".to_owned(),
        ));
    }
    let tx_end = entry
        .tx_start_index
        .checked_add(entry.tx_count)
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "transaction slice range overflows u32".to_owned(),
            )
        })?;
    let start_usize = usize::try_from(entry.tx_start_index).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("tx start exceeds usize".to_owned())
    })?;
    let end_usize = usize::try_from(tx_end).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("tx end exceeds usize".to_owned())
    })?;
    let block_slices = tx_entries.get(start_usize..end_usize).ok_or_else(|| {
        SettledRawBlockSegmentError::InvalidFormat(
            "row transaction slice range exceeds tx table".to_owned(),
        )
    })?;
    for slice in block_slices {
        let Some(slice_end) = slice.offset.checked_add(slice.len) else {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "transaction slice offset plus length overflows u32".to_owned(),
            ));
        };
        if slice_end > entry.uncompressed_len {
            return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
                "transaction slice [{}..{slice_end}] exceeds uncompressed block length {}",
                slice.offset, entry.uncompressed_len
            )));
        }
    }
    Ok(tx_end)
}

#[cfg(unix)]
fn read_slab_at(file: &File, len: usize, mut offset: u64) -> io::Result<Vec<u8>> {
    use rustix::{
        buffer::spare_capacity,
        io::{pread, retry_on_intr},
    };

    let mut slab = Vec::with_capacity(len);
    while slab.len() < len {
        let count = retry_on_intr(|| pread(file, spare_capacity(&mut slab), offset))?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        offset += count as u64;
    }
    slab.truncate(len);
    Ok(slab)
}

#[cfg(windows)]
fn read_slab_at(file: &File, len: usize, mut offset: u64) -> io::Result<Vec<u8>> {
    use std::os::windows::fs::FileExt as _;
    let mut slab = vec![0_u8; len];
    let mut slice = slab.as_mut_slice();
    while !slice.is_empty() {
        let count = file.seek_read(slice, offset)?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        offset += count as u64;
        slice = &mut slice[count..];
    }
    Ok(slab)
}
