//! Positional sans-I/O range planner and reader for `ZSTSUP01` segments.

use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::File,
    io,
    ops::Range,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use zinder_core::{BlockHash, BlockHeaderArtifact, BlockHeight, CompactBlockArtifact};
use zinder_proto::compat::lightwalletd::EncodedBlock;
use zstd::bulk::Decompressor;

use crate::{
    HeightOrder,
    positional_source::{ByteRange, PositionalByteSource, ScanDropBehind, SegmentSource},
    segment_verity,
};

use super::{
    SettledSupplementSegmentError, SupplementCompressionCodec, SupplementSegmentHeader,
    SupplementSegmentIdentity,
    format::{
        CHUNK_INDEX_ENTRY_LEN, CompactSupplementRow, DecodedChunkIndexEntry, DecodedRowIndexEntry,
        HEADER_LEN, INDEX_PAGE_ROWS, IndexedChunk, ROW_INDEX_ENTRY_LEN, STAGING_CHUNK, TRAILER_LEN,
        VALIDATION_INVALID, max_chunk_bytes, max_compressed_chunk_bytes,
        validate_block_payload_limit,
    },
    section::{
        BlockEnvelopeIdentity, BlockSupplementRecord, CanonicalBlockSupplement,
        CompactTransparentSupplementTx, block_hash_wire_to_domain, decode_block_record,
    },
    wire_splice::{
        CompactBlockSpliceSelection, SpliceScratch, SpliceTarget,
        reconstruct_compact_block_artifact, splice_compact_block_wire_into,
    },
};

/// Options governing admission when opening a `ZSTSUP01` segment.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SettledSupplementSegmentOpenOptions {
    /// Reject segments that lack kernel `fs-verity` protection or expected pin.
    pub require_fs_verity: bool,
    /// Verify every chunk digest and whole-body SHA-256 eagerly during open.
    pub verify_full_payload: bool,
}

/// On-disk slice of raw `80`-byte `ZSTSUP01` row index entries paired with its
/// byte offset inside the segment.
#[derive(Clone, Copy, Debug)]
pub struct SupplementRowIndexSlice<'a> {
    /// Segment byte offset at which `bytes[0]` begins.
    pub base_offset: u64,
    /// Raw on-disk row index bytes covering at least the requested rows.
    pub bytes: &'a [u8],
}

impl<'a> SupplementRowIndexSlice<'a> {
    /// Creates a positioned slice over raw `80`-byte `ZSTSUP01` row index entries.
    #[must_use]
    pub const fn new(base_offset: u64, bytes: &'a [u8]) -> Self {
        Self { base_offset, bytes }
    }
}

/// Authenticated view over raw on-disk `ZSTSUP01` row index entries.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedSupplementRowIndexSlice<'a> {
    base_row: usize,
    bytes: &'a [u8],
}

impl VerifiedSupplementRowIndexSlice<'_> {
    fn entry(&self, row_idx: usize) -> Result<DecodedRowIndexEntry, SettledSupplementSegmentError> {
        let local_idx = row_idx.checked_sub(self.base_row).ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "supplement row index slice precedes requested row".to_owned(),
            )
        })?;
        let byte_start = local_idx.checked_mul(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "supplement row index offset overflow".to_owned(),
            )
        })?;
        let byte_end = byte_start.checked_add(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "supplement row index offset overflow".to_owned(),
            )
        })?;
        let slice = self.bytes.get(byte_start..byte_end).ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "supplement row index slice does not cover requested row".to_owned(),
            )
        })?;
        let fixed: &[u8; ROW_INDEX_ENTRY_LEN] = slice.try_into().map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat(
                "short supplement row index entry".to_owned(),
            )
        })?;
        DecodedRowIndexEntry::decode(fixed)
    }

    fn block_hash_wire(&self, row_idx: usize) -> Result<[u8; 32], SettledSupplementSegmentError> {
        Ok(self.entry(row_idx)?.block_hash_wire)
    }
}

/// One compressed chunk fetch and decode step emitted by
/// [`SettledSupplementSegmentIndex::plan_block_range`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupplementChunkReadStep {
    chunk_idx: usize,
    row_range: Range<usize>,
    compressed_range: ByteRange,
    uncompressed_len: usize,
}

impl SupplementChunkReadStep {
    /// Zero-based index of the touched compressed chunk.
    #[must_use]
    pub const fn chunk_idx(&self) -> usize {
        self.chunk_idx
    }

    /// Segment-relative block row indices within this chunk requested by the caller.
    #[must_use]
    pub const fn row_range(&self) -> Range<usize> {
        self.row_range.start..self.row_range.end
    }

    /// Positional byte range of the compressed chunk payload in the segment.
    #[must_use]
    pub const fn compressed_range(&self) -> ByteRange {
        self.compressed_range
    }

    /// Exact expected decompressed byte length of this chunk.
    #[must_use]
    pub const fn uncompressed_len(&self) -> usize {
        self.uncompressed_len
    }
}

/// Sans-I/O range plan returned by [`SettledSupplementSegmentIndex::plan_block_range`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupplementRangePlan {
    row_index_range: ByteRange,
    first_row: usize,
    last_row: usize,
    chunks: Vec<SupplementChunkReadStep>,
}

impl SupplementRangePlan {
    /// Positional [`ByteRange`] of raw `80`-byte row index entries to fetch once
    /// for all chunks in this plan (page-aligned when `!kernel_verified`).
    #[must_use]
    pub const fn row_index_range(&self) -> ByteRange {
        self.row_index_range
    }

    /// First segment-relative block row covered by the plan.
    #[must_use]
    pub const fn first_row(&self) -> usize {
        self.first_row
    }

    /// Last inclusive segment-relative block row covered by the plan.
    #[must_use]
    pub const fn last_row(&self) -> usize {
        self.last_row
    }

    /// Ordered compressed chunk read steps covering `[first_row..=last_row]`.
    #[must_use]
    pub fn chunks(&self) -> &[SupplementChunkReadStep] {
        &self.chunks
    }
}

/// One chunk step in [`SupplementSplicePlan`] where `compressed_range` is `None`
/// if Tag-1 data is not needed for any block in `step.row_range()`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupplementSpliceChunkStep {
    decode_step: SupplementChunkReadStep,
    needs_chunk: bool,
}

impl SupplementSpliceChunkStep {
    /// Underlying decode step for chunk index and row bounds.
    #[must_use]
    pub const fn decode_step(&self) -> &SupplementChunkReadStep {
        &self.decode_step
    }

    /// Segment byte range for the compressed chunk if Tag-1 splicing needs it.
    #[must_use]
    pub const fn compressed_range(&self) -> Option<ByteRange> {
        if self.needs_chunk {
            Some(self.decode_step.compressed_range)
        } else {
            None
        }
    }
}

/// Sans-I/O splice plan returned by [`SettledSupplementSegmentIndex::plan_splice_range`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupplementSplicePlan {
    row_index_range: ByteRange,
    first_row: usize,
    last_row: usize,
    chunks: Vec<SupplementSpliceChunkStep>,
}

impl SupplementSplicePlan {
    /// Positional [`ByteRange`] of raw `80`-byte row index entries needed for
    /// envelope checks and row decoding.
    #[must_use]
    pub const fn row_index_range(&self) -> ByteRange {
        self.row_index_range
    }

    /// First inclusive segment-relative row covered by this splice plan.
    #[must_use]
    pub const fn first_row(&self) -> usize {
        self.first_row
    }

    /// Last inclusive segment-relative row covered by this splice plan.
    #[must_use]
    pub const fn last_row(&self) -> usize {
        self.last_row
    }

    /// Ordered splice steps across chunks.
    #[must_use]
    pub fn chunks(&self) -> &[SupplementSpliceChunkStep] {
        &self.chunks
    }

    /// Returns `true` when at least one chunk in this plan carries Tag-1 entries
    /// required by the splice selection.
    #[must_use]
    pub fn needs_any_chunk(&self) -> bool {
        self.chunks.iter().any(|step| step.needs_chunk)
    }
}

/// Compact `ZSTSUP01` in-memory index (`4.5 B/block` under `fs-verity`,
/// `5.0 B/block` portable) with sans-I/O range planning and verification.
pub struct SettledSupplementSegmentIndex {
    identity: SupplementSegmentIdentity,
    header: SupplementSegmentHeader,
    codec: SupplementCompressionCodec,
    chunks: Box<[IndexedChunk]>,
    rows: Box<[CompactSupplementRow]>,
    row_index_page_digests: Option<Box<[[u8; 32]]>>,
    kernel_verified: bool,
}

impl SettledSupplementSegmentIndex {
    /// Authenticates and decodes a `ZSTSUP01` header, raw chunk index, raw row
    /// index, and trailing `32`-byte SHA-256 trailer in `O(index)` without
    /// reading compressed chunks.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on malformed fields, `sha256`
    /// or `index_sha256` pin mismatch, or invalid chunk/row bounds.
    pub fn decode_index(
        header_bytes: &[u8; HEADER_LEN],
        indexes_and_trailer: &[u8],
        expected: &SupplementSegmentIdentity,
        total_len: u64,
        kernel_verified: bool,
    ) -> Result<Self, SettledSupplementSegmentError> {
        let header = SupplementSegmentHeader::decode(header_bytes, expected, total_len)?;
        let codec = header.codec()?;
        let expected_range = header.indexes_and_trailer_range()?;
        if indexes_and_trailer.len() != expected_range.len {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement indexes and trailer length disagrees with header".to_owned(),
            ));
        }
        let chunk_count = usize::try_from(header.chunk_count()?).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("chunk count exceeds usize".to_owned())
        })?;
        let row_count = usize::try_from(header.block_count).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("block count exceeds usize".to_owned())
        })?;
        let chunk_bytes_len = chunk_count * CHUNK_INDEX_ENTRY_LEN;
        let row_bytes_len = row_count * ROW_INDEX_ENTRY_LEN;
        let (raw_chunks, rest) = indexes_and_trailer.split_at(chunk_bytes_len);
        let (raw_rows, trailer) = rest.split_at(row_bytes_len);
        if trailer != expected.sha256 {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "supplement segment SHA-256 trailer disagrees with pinned identity".to_owned(),
            ));
        }
        verify_index_digest(
            header_bytes,
            raw_chunks,
            raw_rows,
            trailer,
            &expected.index_sha256,
        )?;
        let (chunks, rows) = decode_and_validate_tables(&header, codec, raw_chunks, raw_rows)?;
        let row_index_page_digests = (!kernel_verified).then(|| compute_row_page_digests(raw_rows));
        Ok(Self {
            identity: *expected,
            header,
            codec,
            chunks,
            rows,
            row_index_page_digests,
            kernel_verified,
        })
    }

    /// Returns the authenticated identity metadata for this segment.
    #[must_use]
    pub const fn identity(&self) -> &SupplementSegmentIdentity {
        &self.identity
    }

    /// Returns the decoded `128`-byte header.
    #[must_use]
    pub const fn header(&self) -> SupplementSegmentHeader {
        self.header
    }

    /// Returns the compression codec declared in the admitted segment header.
    #[must_use]
    pub const fn codec(&self) -> SupplementCompressionCodec {
        self.codec
    }

    /// Returns the number of contiguous blocks grouped per chunk frame.
    #[must_use]
    pub const fn chunk_blocks(&self) -> u16 {
        self.header.chunk_blocks
    }

    /// Returns the number of contiguous blocks covered by this segment.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` when kernel `fs-verity` verified a pinned measurement on open.
    #[must_use]
    pub const fn kernel_verified(&self) -> bool {
        self.kernel_verified
    }

    /// Returns the heap bytes retained by compact chunk metadata (`56 B/chunk`),
    /// compact `1`-byte row states, and optional portable page digests.
    #[must_use]
    pub fn heap_metadata_bytes(&self) -> usize {
        let chunk_bytes = self.chunks.len() * std::mem::size_of::<IndexedChunk>();
        let row_bytes = self.rows.len() * std::mem::size_of::<CompactSupplementRow>();
        let page_bytes = self
            .row_index_page_digests
            .as_ref()
            .map_or(0, |pages| pages.len() * 32);
        chunk_bytes + row_bytes + page_bytes
    }

    /// Returns the positional [`ByteRange`] needed to read `height`'s block hash
    /// (verified `64`-row page when `!kernel_verified`, or `32` bytes when `kernel_verified`).
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] if `height` is outside the segment
    /// or its chunk/row failed prior verification.
    pub fn block_hash_byte_range(
        &self,
        height: BlockHeight,
    ) -> Result<ByteRange, SettledSupplementSegmentError> {
        let row_idx = self.row_index_for(height)?;
        self.ensure_row_not_invalid(row_idx)?;
        if !self.kernel_verified {
            let (_start_row, range) = self.aligned_page_range_for_rows(row_idx, row_idx + 1)?;
            return Ok(range);
        }
        let offset = self
            .header
            .row_index_offset
            .checked_add((row_idx as u64) * ROW_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("row index offset overflow".to_owned())
            })?;
        Ok(ByteRange::new(offset, 32))
    }

    /// Decodes `height`'s authenticated [`BlockHash`] from a fetched row-index slice.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] if `height` is invalid, `slice`
    /// fails bounds or page-digest verification, or the hash is non-canonical.
    pub fn decode_block_hash_from_slice(
        &self,
        height: BlockHeight,
        slice: SupplementRowIndexSlice<'_>,
    ) -> Result<BlockHash, SettledSupplementSegmentError> {
        let row_idx = self.row_index_for(height)?;
        self.ensure_row_not_invalid(row_idx)?;
        if !self.kernel_verified {
            let verified = self.authenticate_row_index_slice(slice)?;
            return block_hash_wire_to_domain(&verified.block_hash_wire(row_idx)?);
        }
        let expected_offset = self
            .header
            .row_index_offset
            .checked_add((row_idx as u64) * ROW_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("row index offset overflow".to_owned())
            })?;
        let local_start = expected_offset
            .checked_sub(slice.base_offset)
            .and_then(|delta| usize::try_from(delta).ok())
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat(
                    "supplement row slice does not cover block hash".to_owned(),
                )
            })?;
        let bytes = slice
            .bytes
            .get(local_start..local_start + 32)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat(
                    "short supplement block hash slice".to_owned(),
                )
            })?;
        let wire: &[u8; 32] = bytes.try_into().map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("short supplement hash bytes".to_owned())
        })?;
        block_hash_wire_to_domain(wire)
    }

    /// Authenticates raw `80`-byte row index bytes against full `64`-row page
    /// digests when `!kernel_verified`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError::InvalidFormat`] on misaligned
    /// slices or [`SettledSupplementSegmentError::PayloadIntegrity`] on page
    /// digest mismatch, latching sticky invalid state on that page's rows.
    pub fn authenticate_row_index_slice<'a>(
        &self,
        slice: SupplementRowIndexSlice<'a>,
    ) -> Result<VerifiedSupplementRowIndexSlice<'a>, SettledSupplementSegmentError> {
        let rel = slice
            .base_offset
            .checked_sub(self.header.row_index_offset)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat(
                    "supplement row index slice begins before row_index_offset".to_owned(),
                )
            })?;
        if !rel.is_multiple_of(ROW_INDEX_ENTRY_LEN as u64)
            || !slice.bytes.len().is_multiple_of(ROW_INDEX_ENTRY_LEN)
        {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement row index slice is not aligned to 80-byte entries".to_owned(),
            ));
        }
        let start_row = usize::try_from(rel / ROW_INDEX_ENTRY_LEN as u64).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("row offset exceeds usize".to_owned())
        })?;
        let row_count = slice.bytes.len() / ROW_INDEX_ENTRY_LEN;
        let end_row = start_row.checked_add(row_count).ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("row end index exceeds usize".to_owned())
        })?;
        if end_row > self.rows.len() {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement row index slice extends past block_count".to_owned(),
            ));
        }
        if let Some(digests) = &self.row_index_page_digests {
            self.verify_portable_row_pages(start_row, end_row, slice.bytes, digests)?;
        }
        Ok(VerifiedSupplementRowIndexSlice {
            base_row: start_row,
            bytes: slice.bytes,
        })
    }

    fn verify_portable_row_pages(
        &self,
        start_row: usize,
        end_row: usize,
        raw_slice: &[u8],
        digests: &[[u8; 32]],
    ) -> Result<(), SettledSupplementSegmentError> {
        if start_row == end_row {
            return Ok(());
        }
        if !start_row.is_multiple_of(INDEX_PAGE_ROWS)
            || (end_row != self.rows.len() && !end_row.is_multiple_of(INDEX_PAGE_ROWS))
        {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "portable supplement row index slice must cover full 64-row pages".to_owned(),
            ));
        }
        let first_page = start_row / INDEX_PAGE_ROWS;
        for (page_offset, page_bytes) in raw_slice
            .chunks(INDEX_PAGE_ROWS * ROW_INDEX_ENTRY_LEN)
            .enumerate()
        {
            let page_idx = first_page + page_offset;
            let computed: [u8; 32] = Sha256::digest(page_bytes).into();
            if computed != digests[page_idx] {
                let bad_start = page_idx * INDEX_PAGE_ROWS;
                let bad_end = (bad_start + INDEX_PAGE_ROWS).min(self.rows.len());
                for idx in bad_start..bad_end {
                    self.rows[idx].mark_invalid();
                }
                return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                    "portable supplement row index page {page_idx} SHA-256 mismatch"
                )));
            }
        }
        Ok(())
    }

    /// Plans sans-I/O fetches (one coalesced `row_index_range` plus touched
    /// `16`-block compressed chunks) across `[start_height, end_height]`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on invalid height bounds or
    /// previously latched invalid chunks/rows.
    pub fn plan_block_range(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<SupplementRangePlan, SettledSupplementSegmentError> {
        let (first_row, last_row) = self.row_range_for(start_height, end_height)?;
        let chunks = self.build_chunk_steps(first_row, last_row)?;
        let row_index_range = self.row_index_range_for(first_row, last_row)?;
        Ok(SupplementRangePlan {
            row_index_range,
            first_row,
            last_row,
            chunks,
        })
    }

    /// Plans sans-I/O fetches for wire splicing across `[start_height, end_height]`,
    /// omitting chunk fetches where no block carries needed Tag-1 entries.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on out-of-range heights, sticky
    /// invalid state, or wide fees unrepresentable on `CompactTx` wire frames.
    pub fn plan_splice_range(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
        selection: CompactBlockSpliceSelection,
    ) -> Result<SupplementSplicePlan, SettledSupplementSegmentError> {
        let (first_row, last_row) = self.row_range_for(start_height, end_height)?;
        for row_idx in first_row..=last_row {
            self.ensure_row_not_invalid(row_idx)?;
            if self.rows[row_idx].has_wide_fee() {
                return Err(SettledSupplementSegmentError::PayloadIntegrity(
                    "supplement compact transaction fee exceeds u32".to_owned(),
                ));
            }
        }
        let raw_steps = self.build_chunk_steps(first_row, last_row)?;
        let chunks = raw_steps
            .into_iter()
            .map(|decode_step| {
                let needs_chunk = decode_step
                    .row_range
                    .clone()
                    .any(|idx| splice_needs_tag1(selection, &self.rows[idx]));
                SupplementSpliceChunkStep {
                    decode_step,
                    needs_chunk,
                }
            })
            .collect();
        let row_index_range = self.row_index_range_for(first_row, last_row)?;
        Ok(SupplementSplicePlan {
            row_index_range,
            first_row,
            last_row,
            chunks,
        })
    }

    /// Verifies and decodes one sans-I/O [`SupplementChunkReadStep`] from a
    /// fetched compressed chunk buffer and authenticated `verified_rows` slice.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on buffer size mismatch,
    /// index/chunk/payload SHA-256 mismatch, Zstd decompression error, or
    /// malformed tagged sections, latching sticky `VALIDATION_INVALID`.
    pub fn verify_and_decode_chunk(
        &self,
        step: &SupplementChunkReadStep,
        compressed_chunk_bytes: &[u8],
        verified_rows: VerifiedSupplementRowIndexSlice<'_>,
    ) -> Result<Vec<BlockSupplementRecord>, SettledSupplementSegmentError> {
        let chunk_bytes =
            self.verify_and_decompress_chunk(step.chunk_idx, compressed_chunk_bytes)?;
        self.decode_chunk_rows(step, &chunk_bytes, verified_rows)
    }

    fn decode_chunk_rows(
        &self,
        step: &SupplementChunkReadStep,
        chunk_bytes: &[u8],
        verified_rows: VerifiedSupplementRowIndexSlice<'_>,
    ) -> Result<Vec<BlockSupplementRecord>, SettledSupplementSegmentError> {
        for row_idx in step.row_range() {
            self.ensure_row_not_invalid(row_idx)?;
        }
        let mut out = Vec::with_capacity(step.row_range.len());
        for row_idx in step.row_range() {
            out.push(self.decode_verified_row_from_chunk(row_idx, chunk_bytes, verified_rows)?);
        }
        Ok(out)
    }

    fn verify_and_decompress_chunk(
        &self,
        chunk_idx: usize,
        compressed_chunk_bytes: &[u8],
    ) -> Result<Vec<u8>, SettledSupplementSegmentError> {
        let chunk = self.chunks.get(chunk_idx).ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("chunk index out of bounds".to_owned())
        })?;
        if chunk.validation_state.load(Ordering::Acquire) == VALIDATION_INVALID {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "supplement chunk failed prior verification".to_owned(),
            ));
        }
        let expected_len = usize::try_from(chunk.compressed_len).map_err(|_| {
            chunk.mark_invalid();
            SettledSupplementSegmentError::InvalidFormat(
                "compressed chunk exceeds usize".to_owned(),
            )
        })?;
        if compressed_chunk_bytes.len() != expected_len {
            chunk.mark_invalid();
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "fetched compressed chunk length disagrees with index".to_owned(),
            ));
        }
        let actual_sha: [u8; 32] = Sha256::digest(compressed_chunk_bytes).into();
        if actual_sha != chunk.chunk_sha256 {
            chunk.mark_invalid();
            return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                "supplement chunk {chunk_idx} SHA-256 digest mismatch"
            )));
        }
        let uncompressed_len = usize::try_from(chunk.uncompressed_len).map_err(|_| {
            chunk.mark_invalid();
            SettledSupplementSegmentError::InvalidFormat(
                "uncompressed chunk exceeds usize".to_owned(),
            )
        })?;
        let decoded = match self.codec {
            SupplementCompressionCodec::None => compressed_chunk_bytes.to_vec(),
            SupplementCompressionCodec::Zstd => {
                decompress_exact_chunk(chunk_idx, chunk, compressed_chunk_bytes, uncompressed_len)?
            }
        };
        chunk.mark_valid();
        Ok(decoded)
    }

    fn decode_verified_row_from_chunk(
        &self,
        row_idx: usize,
        chunk_bytes: &[u8],
        verified_rows: VerifiedSupplementRowIndexSlice<'_>,
    ) -> Result<BlockSupplementRecord, SettledSupplementSegmentError> {
        let row = &self.rows[row_idx];
        let entry = verified_rows
            .entry(row_idx)
            .inspect_err(|_| row.mark_invalid())?;
        let start = usize::try_from(entry.chunk_rel_offset).map_err(|_| {
            row.mark_invalid();
            SettledSupplementSegmentError::InvalidFormat("row offset exceeds usize".to_owned())
        })?;
        let len = usize::try_from(entry.uncompressed_len).map_err(|_| {
            row.mark_invalid();
            SettledSupplementSegmentError::InvalidFormat("row length exceeds usize".to_owned())
        })?;
        let end = start.checked_add(len).ok_or_else(|| {
            row.mark_invalid();
            SettledSupplementSegmentError::InvalidFormat("row span overflow".to_owned())
        })?;
        let raw_row = chunk_bytes.get(start..end).ok_or_else(|| {
            row.mark_invalid();
            SettledSupplementSegmentError::PayloadIntegrity(
                "row span extends past decompressed chunk bounds".to_owned(),
            )
        })?;
        let actual_sha: [u8; 32] = Sha256::digest(raw_row).into();
        if actual_sha != entry.payload_sha256 {
            row.mark_invalid();
            return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                "supplement block row {row_idx} payload SHA-256 mismatch"
            )));
        }
        let envelope = expected_envelope_from_slice(&self.header, row_idx, &entry, verified_rows)
            .inspect_err(|_| row.mark_invalid())?;
        match decode_block_record(raw_row, envelope, row.row_flags()) {
            Ok(record) => {
                row.mark_valid();
                Ok(record)
            }
            Err(err) => {
                row.mark_invalid();
                Err(err)
            }
        }
    }

    fn build_chunk_steps(
        &self,
        first_row: usize,
        last_row: usize,
    ) -> Result<Vec<SupplementChunkReadStep>, SettledSupplementSegmentError> {
        let chunk_span = usize::from(self.header.chunk_blocks);
        let first_chunk = first_row / chunk_span;
        let last_chunk = last_row / chunk_span;
        let mut steps = Vec::with_capacity(last_chunk - first_chunk + 1);
        for chunk_idx in first_chunk..=last_chunk {
            let chunk_row_start = (chunk_idx * chunk_span).max(first_row);
            let chunk_row_end = ((chunk_idx + 1) * chunk_span - 1).min(last_row);
            for row_idx in chunk_row_start..=chunk_row_end {
                self.ensure_row_not_invalid(row_idx)?;
            }
            let chunk = &self.chunks[chunk_idx];
            let abs_offset = self
                .header
                .data_offset
                .checked_add(chunk.data_rel_offset)
                .ok_or_else(|| {
                    SettledSupplementSegmentError::InvalidFormat(
                        "chunk file offset overflow".to_owned(),
                    )
                })?;
            let compressed_len = usize::try_from(chunk.compressed_len).map_err(|_| {
                SettledSupplementSegmentError::InvalidFormat(
                    "compressed chunk exceeds usize".to_owned(),
                )
            })?;
            let uncompressed_len = usize::try_from(chunk.uncompressed_len).map_err(|_| {
                SettledSupplementSegmentError::InvalidFormat(
                    "uncompressed chunk exceeds usize".to_owned(),
                )
            })?;
            steps.push(SupplementChunkReadStep {
                chunk_idx,
                row_range: chunk_row_start..chunk_row_end + 1,
                compressed_range: ByteRange::new(abs_offset, compressed_len),
                uncompressed_len,
            });
        }
        Ok(steps)
    }

    fn row_index_range_for(
        &self,
        first_row: usize,
        last_row: usize,
    ) -> Result<ByteRange, SettledSupplementSegmentError> {
        let needed_start = first_row.saturating_sub(1);
        let needed_end = last_row + 1;
        if !self.kernel_verified {
            let (_start, range) = self.aligned_page_range_for_rows(needed_start, needed_end)?;
            return Ok(range);
        }
        self.exact_row_index_range(needed_start, needed_end)
    }

    fn aligned_page_range_for_rows(
        &self,
        start_row: usize,
        end_row_excl: usize,
    ) -> Result<(usize, ByteRange), SettledSupplementSegmentError> {
        let aligned_start = (start_row / INDEX_PAGE_ROWS) * INDEX_PAGE_ROWS;
        let aligned_end = end_row_excl
            .div_ceil(INDEX_PAGE_ROWS)
            .saturating_mul(INDEX_PAGE_ROWS)
            .min(self.rows.len());
        let range = self.exact_row_index_range(aligned_start, aligned_end)?;
        Ok((aligned_start, range))
    }

    fn exact_row_index_range(
        &self,
        start_row: usize,
        end_row_excl: usize,
    ) -> Result<ByteRange, SettledSupplementSegmentError> {
        let offset = self
            .header
            .row_index_offset
            .checked_add((start_row as u64) * ROW_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("row index offset overflow".to_owned())
            })?;
        let byte_len = end_row_excl
            .checked_sub(start_row)
            .and_then(|rows| rows.checked_mul(ROW_INDEX_ENTRY_LEN))
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat(
                    "row index slice length overflow".to_owned(),
                )
            })?;
        Ok(ByteRange::new(offset, byte_len))
    }

    fn row_index_for(&self, height: BlockHeight) -> Result<usize, SettledSupplementSegmentError> {
        if height < self.identity.start_height || height > self.identity.end_block.height {
            return Err(SettledSupplementSegmentError::HeightOutOfRange {
                height,
                start: self.identity.start_height,
                end: self.identity.end_block.height,
            });
        }
        usize::try_from(height.value() - self.identity.start_height.value()).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("row index exceeds usize".to_owned())
        })
    }

    fn row_range_for(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<(usize, usize), SettledSupplementSegmentError> {
        if end_height < start_height {
            return Err(SettledSupplementSegmentError::HeightOutOfRange {
                height: end_height,
                start: start_height,
                end: self.identity.end_block.height,
            });
        }
        Ok((
            self.row_index_for(start_height)?,
            self.row_index_for(end_height)?,
        ))
    }

    fn chunk_index_for_row(&self, row_idx: usize) -> Result<usize, SettledSupplementSegmentError> {
        let chunk_idx = row_idx / usize::from(self.header.chunk_blocks);
        if chunk_idx >= self.chunks.len() {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "row maps past chunk index end".to_owned(),
            ));
        }
        Ok(chunk_idx)
    }

    fn ensure_row_not_invalid(&self, row_idx: usize) -> Result<(), SettledSupplementSegmentError> {
        let chunk_idx = self.chunk_index_for_row(row_idx)?;
        if self.chunks[chunk_idx]
            .validation_state
            .load(Ordering::Acquire)
            == VALIDATION_INVALID
            || self.rows[row_idx].validation_state() == VALIDATION_INVALID
        {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "supplement block chunk or row failed prior verification".to_owned(),
            ));
        }
        Ok(())
    }
}

const CHUNK_CACHE_MAX_BYTES: usize = 4 * 1024 * 1024;
const CHUNK_CACHE_MAX_ENTRIES: usize = 256;

thread_local! {
    static ZSTD_DECOMPRESSOR: RefCell<Option<Decompressor<'static>>> = const { RefCell::new(None) };
}

static CHUNK_CACHE: Mutex<ChunkCache> = Mutex::new(ChunkCache {
    entries: VecDeque::new(),
    bytes: 0,
});

struct CachedChunk {
    // Separate opens must authenticate their own source even if their pins match.
    reader_id: Arc<()>,
    chunk_idx: usize,
    bytes: Arc<Vec<u8>>,
}

struct ChunkCache {
    entries: VecDeque<CachedChunk>,
    bytes: usize,
}

impl ChunkCache {
    fn get(&mut self, reader_id: &Arc<()>, chunk_idx: usize) -> Option<Arc<Vec<u8>>> {
        let position = self.entries.iter().rposition(|entry| {
            Arc::ptr_eq(&entry.reader_id, reader_id) && entry.chunk_idx == chunk_idx
        })?;
        let entry = self.entries.remove(position)?;
        let bytes = Arc::clone(&entry.bytes);
        self.entries.push_back(entry);
        Some(bytes)
    }

    fn insert(&mut self, entry: CachedChunk) {
        if self.get(&entry.reader_id, entry.chunk_idx).is_some() {
            return;
        }
        let size = entry.bytes.capacity();
        if size <= CHUNK_CACHE_MAX_BYTES {
            while self.bytes + size > CHUNK_CACHE_MAX_BYTES
                || self.entries.len() == CHUNK_CACHE_MAX_ENTRIES
            {
                if let Some(evicted) = self.entries.pop_front() {
                    self.bytes -= evicted.bytes.capacity();
                }
            }
            self.bytes += size;
            self.entries.push_back(entry);
        }
    }
}

/// Authenticated positional reader for one settled `ZSTSUP01` supplement segment.
pub struct SettledSupplementSegmentReader<S = File> {
    source: SegmentSource<S>,
    index: SettledSupplementSegmentIndex,
    cache_id: Arc<()>,
}

impl SettledSupplementSegmentReader<File> {
    /// Opens, admits, and binds one regular `ZSTSUP01` segment file against `expected`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on open I/O failure, `fs-verity`
    /// mismatch/absence when required, or header/`index_sha256` verification failure.
    pub fn open(
        path: impl AsRef<Path>,
        expected: &SupplementSegmentIdentity,
        options: SettledSupplementSegmentOpenOptions,
    ) -> Result<Self, SettledSupplementSegmentError> {
        let file = segment_verity::open_regular_file(path.as_ref(), "supplement segment")?;
        Self::open_file(file, expected, options)
    }

    /// Admits and binds one already-opened regular `ZSTSUP01` segment file against `expected`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on non-regular files, `fs-verity`
    /// mismatch/absence when required, or header/`index_sha256` verification failure.
    pub fn open_file(
        file: File,
        expected: &SupplementSegmentIdentity,
        options: SettledSupplementSegmentOpenOptions,
    ) -> Result<Self, SettledSupplementSegmentError> {
        Self::open_file_with_scan_policy(file, expected, options, false)
    }

    pub(crate) fn open_file_with_scan_policy(
        file: File,
        expected: &SupplementSegmentIdentity,
        options: SettledSupplementSegmentOpenOptions,
        constrained: bool,
    ) -> Result<Self, SettledSupplementSegmentError> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected a regular settled supplement segment file",
            )
            .into());
        }
        let verity_verified = segment_verity::check_pinned_verity(
            expected.fs_verity_sha256,
            segment_verity::measure(&file)?,
            "supplement segment",
        )?;
        if options.require_fs_verity && !verity_verified {
            return Err(SettledSupplementSegmentError::FsVerityRequired);
        }
        Self::admit_with_verity(
            SegmentSource::file(file, "supplement", constrained),
            expected,
            options,
            verity_verified,
        )
    }
}

impl<S: PositionalByteSource> SettledSupplementSegmentReader<S> {
    /// Opens and authenticates any [`PositionalByteSource`] against `expected`
    /// in `O(index)` (or full-payload mode when `options.verify_full_payload` is set).
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError::FsVerityRequired`] if `require_fs_verity`
    /// is requested on a generic source, or any header/`index_sha256`/payload failure.
    pub fn open_source(
        source: S,
        expected: &SupplementSegmentIdentity,
        options: SettledSupplementSegmentOpenOptions,
    ) -> Result<Self, SettledSupplementSegmentError> {
        if options.require_fs_verity {
            return Err(SettledSupplementSegmentError::FsVerityRequired);
        }
        Self::admit_with_verity(SegmentSource::unmeasured(source), expected, options, false)
    }

    pub(crate) fn index_heap_bytes(&self) -> usize {
        std::mem::size_of_val(self.index.rows.as_ref())
            + std::mem::size_of_val(self.index.chunks.as_ref())
            + self
                .index
                .row_index_page_digests
                .as_deref()
                .map_or(0, std::mem::size_of_val)
    }

    fn admit_with_verity(
        source: SegmentSource<S>,
        expected: &SupplementSegmentIdentity,
        options: SettledSupplementSegmentOpenOptions,
        kernel_verified: bool,
    ) -> Result<Self, SettledSupplementSegmentError> {
        let total_len = source.total_len()?;
        let min_len = u64::try_from(HEADER_LEN + TRAILER_LEN).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("header size overflow".to_owned())
        })?;
        if total_len < min_len {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement segment file truncated before header/trailer".to_owned(),
            ));
        }
        let mut header_bytes = [0_u8; HEADER_LEN];
        source.read_exact_at(0, &mut header_bytes)?;
        let header = SupplementSegmentHeader::decode(&header_bytes, expected, total_len)?;
        let idx_range = header.indexes_and_trailer_range()?;
        let indexes_and_trailer = source.read_vec_at(idx_range.offset, idx_range.len)?;
        let index = SettledSupplementSegmentIndex::decode_index(
            &header_bytes,
            &indexes_and_trailer,
            expected,
            total_len,
            kernel_verified,
        )?;
        let reader = Self {
            source,
            index,
            cache_id: Arc::new(()),
        };
        if options.verify_full_payload {
            reader.verify_full_payload_with_raw(&header_bytes, &indexes_and_trailer)?;
        }
        Ok(reader)
    }

    /// Streams header, compressed chunk payloads, chunk index, and row index
    /// from the source and verifies every chunk SHA-256 and the whole-body
    /// trailer digest.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on I/O failure or any body/index
    /// digest mismatch, latching sticky `VALIDATION_INVALID` on affected chunks.
    pub fn verify_full_payload(&self) -> Result<(), SettledSupplementSegmentError> {
        let total_len = self.source.total_len()?;
        let mut header_bytes = [0_u8; HEADER_LEN];
        self.source.read_exact_at(0, &mut header_bytes)?;
        let header =
            SupplementSegmentHeader::decode(&header_bytes, &self.index.identity, total_len)?;
        let idx_range = header.indexes_and_trailer_range()?;
        let indexes_and_trailer = self.source.read_vec_at(idx_range.offset, idx_range.len)?;
        self.verify_full_payload_with_raw(&header_bytes, &indexes_and_trailer)
    }

    fn scan_drop_behind(&self) -> Option<ScanDropBehind<'_, S>> {
        self.source.scan_drop_behind()
    }

    fn verify_full_payload_with_raw(
        &self,
        header_bytes: &[u8; HEADER_LEN],
        indexes_and_trailer: &[u8],
    ) -> Result<(), SettledSupplementSegmentError> {
        let chunk_bytes_len = self.index.chunks.len() * CHUNK_INDEX_ENTRY_LEN;
        let row_bytes_len = self.index.rows.len() * ROW_INDEX_ENTRY_LEN;
        if indexes_and_trailer.len() != chunk_bytes_len + row_bytes_len + TRAILER_LEN {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement indexes and trailer length disagrees with header".to_owned(),
            ));
        }
        let (raw_chunks, rest) = indexes_and_trailer.split_at(chunk_bytes_len);
        let (raw_rows, trailer) = rest.split_at(row_bytes_len);
        if trailer != self.index.identity.sha256 {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "supplement segment SHA-256 trailer disagrees with pinned identity".to_owned(),
            ));
        }
        verify_index_digest(
            header_bytes,
            raw_chunks,
            raw_rows,
            trailer,
            &self.index.identity.index_sha256,
        )?;

        let mut body_hasher = Sha256::new();
        body_hasher.update(*header_bytes);
        verify_all_compressed_chunks(
            &self.source,
            &self.index,
            &mut body_hasher,
            self.scan_drop_behind(),
        )?;
        body_hasher.update(raw_chunks);
        body_hasher.update(raw_rows);
        let computed: [u8; 32] = body_hasher.finalize().into();
        if computed != trailer || computed != self.index.identity.sha256 {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "supplement segment SHA-256 trailer or pinned digest mismatch".to_owned(),
            ));
        }
        if let Some(mut scan) = self.scan_drop_behind() {
            scan.consumed(ByteRange::new(
                self.index.header.chunk_index_offset,
                indexes_and_trailer.len(),
            ));
        }
        Ok(())
    }

    /// Returns a reference to the compact in-memory index and sans-I/O planner.
    #[must_use]
    pub const fn index(&self) -> &SettledSupplementSegmentIndex {
        &self.index
    }

    /// Returns the authenticated identity metadata for this opened segment.
    #[must_use]
    pub const fn identity(&self) -> &SupplementSegmentIdentity {
        self.index.identity()
    }

    /// Returns the compression codec declared in the admitted segment header.
    #[must_use]
    pub const fn codec(&self) -> SupplementCompressionCodec {
        self.index.codec()
    }

    /// Returns the number of contiguous blocks grouped per chunk frame.
    #[must_use]
    pub const fn chunk_blocks(&self) -> u16 {
        self.index.chunk_blocks()
    }

    /// Returns the number of contiguous blocks covered by this segment.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.index.block_count()
    }

    /// Returns `true` when kernel `fs-verity` verified a pinned measurement on open.
    #[must_use]
    pub const fn kernel_verified(&self) -> bool {
        self.index.kernel_verified()
    }

    /// Returns the heap bytes retained by the in-memory chunk and row tables.
    #[must_use]
    pub fn heap_metadata_bytes(&self) -> usize {
        self.index.heap_metadata_bytes()
    }

    /// Returns the authenticated previous block hash declared before the segment's first block.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError::PayloadIntegrity`] if the header
    /// hash is not a valid internal block hash.
    pub fn first_previous_block_hash(&self) -> Result<BlockHash, SettledSupplementSegmentError> {
        block_hash_wire_to_domain(&self.index.header.first_prev_hash_wire)
    }

    /// Returns the authenticated block hash recorded in the row index for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] if `height` is outside the
    /// segment or that row/chunk failed prior verification.
    pub fn block_hash_at(
        &self,
        height: BlockHeight,
    ) -> Result<BlockHash, SettledSupplementSegmentError> {
        let row_idx = self.index.row_index_for(height)?;
        let range = self.index.block_hash_byte_range(height)?;
        let bytes = self
            .source
            .read_vec_at(range.offset, range.len)
            .inspect_err(|_| self.index.rows[row_idx].mark_invalid())?;
        self.index.decode_block_hash_from_slice(
            height,
            SupplementRowIndexSlice::new(range.offset, &bytes),
        )
    }

    /// Reads and authenticates every row's block hash in a single coalesced
    /// row-index read in ascending block height order.
    pub(crate) fn read_all_block_hashes(
        &self,
    ) -> Result<Vec<BlockHash>, SettledSupplementSegmentError> {
        let row_count = self.index.rows.len();
        for row_idx in 0..row_count {
            self.index.ensure_row_not_invalid(row_idx)?;
        }
        let byte_len = row_count.checked_mul(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "supplement row index size overflow".to_owned(),
            )
        })?;
        let raw_rows = self
            .source
            .read_vec_at(self.index.header.row_index_offset, byte_len)
            .inspect_err(|_| {
                if let Some(last_row) = row_count.checked_sub(1) {
                    mark_row_range_invalid(&self.index, 0, last_row);
                }
            })?;
        let verified = self
            .index
            .authenticate_row_index_slice(SupplementRowIndexSlice::new(
                self.index.header.row_index_offset,
                &raw_rows,
            ))?;
        let mut hashes = Vec::with_capacity(row_count);
        for row_idx in 0..row_count {
            let wire = verified.block_hash_wire(row_idx).inspect_err(|_| {
                self.index.rows[row_idx].mark_invalid();
            })?;
            let hash = block_hash_wire_to_domain(&wire).inspect_err(|_| {
                self.index.rows[row_idx].mark_invalid();
            })?;
            hashes.push(hash);
        }
        if let Some(mut scan) = self.scan_drop_behind() {
            scan.consumed(ByteRange::new(self.index.header.row_index_offset, byte_len));
        }
        Ok(hashes)
    }

    #[cfg(test)]
    pub(super) fn chunk_validation_state(&self, chunk_idx: usize) -> u8 {
        self.index.chunks[chunk_idx]
            .validation_state
            .load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn chunk_is_unvalidated_at(
        &self,
        height: BlockHeight,
    ) -> Result<bool, SettledSupplementSegmentError> {
        let row_idx = self.index.row_index_for(height)?;
        let chunk_idx = self.index.chunk_index_for_row(row_idx)?;
        Ok(self.chunk_validation_state(chunk_idx) == super::format::VALIDATION_UNVALIDATED)
    }

    /// Returns `true` if `height` carries Tag-1 transparent supplement transactions.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] if `height` is out of bounds or
    /// that row/chunk previously entered sticky invalid state.
    pub fn has_transparent_at(
        &self,
        height: BlockHeight,
    ) -> Result<bool, SettledSupplementSegmentError> {
        let row_idx = self.index.row_index_for(height)?;
        self.index.ensure_row_not_invalid(row_idx)?;
        Ok(self.index.rows[row_idx].has_transparent())
    }

    /// Returns `true` if `height` records any transaction fee exceeding `u32::MAX`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] if `height` is out of bounds or
    /// that row/chunk previously entered sticky invalid state.
    pub fn has_wide_fee_at(
        &self,
        height: BlockHeight,
    ) -> Result<bool, SettledSupplementSegmentError> {
        let row_idx = self.index.row_index_for(height)?;
        self.index.ensure_row_not_invalid(row_idx)?;
        Ok(self.index.rows[row_idx].has_wide_fee())
    }

    /// Reads and verifies the full decoded supplement record for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, I/O, compression,
    /// or payload/envelope verification errors.
    pub fn read_block(
        &self,
        height: BlockHeight,
    ) -> Result<BlockSupplementRecord, SettledSupplementSegmentError> {
        let mut records = self.read_range(height, height)?;
        records.pop().ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "missing decoded supplement block".to_owned(),
            )
        })
    }

    /// Reads and verifies decoded supplement records across `[start_height, end_height]`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, I/O, compression,
    /// or payload/envelope verification errors.
    pub fn read_range(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<BlockSupplementRecord>, SettledSupplementSegmentError> {
        self.read_range_checked(start_height, end_height, None)
    }

    /// Reads supplements whose authenticated envelopes match the given contiguous headers.
    pub(crate) fn read_range_for_headers(
        &self,
        headers: &[BlockHeaderArtifact],
    ) -> Result<Vec<BlockSupplementRecord>, SettledSupplementSegmentError> {
        let Some(first) = headers.first() else {
            return Ok(Vec::new());
        };
        let end_height = self.end_height_for_count(first.height, headers.len())?;
        self.read_range_checked(first.height, end_height, Some(headers))
    }

    fn read_range_checked(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
        headers: Option<&[BlockHeaderArtifact]>,
    ) -> Result<Vec<BlockSupplementRecord>, SettledSupplementSegmentError> {
        let plan = self.index.plan_block_range(start_height, end_height)?;
        let row_range = plan.row_index_range();
        let raw_row_index = self
            .source
            .read_vec_at(row_range.offset, row_range.len)
            .inspect_err(|_| {
                mark_row_range_invalid(&self.index, plan.first_row(), plan.last_row());
            })?;
        let verified_rows =
            self.index
                .authenticate_row_index_slice(SupplementRowIndexSlice::new(
                    row_range.offset,
                    &raw_row_index,
                ))?;
        if let Some(headers) = headers {
            for (offset, header) in headers.iter().enumerate() {
                let row_idx = plan.first_row() + offset;
                let entry = verified_rows.entry(row_idx)?;
                let envelope = expected_envelope_from_slice(
                    &self.index.header,
                    row_idx,
                    &entry,
                    verified_rows,
                )?;
                verify_header_identity(envelope, header)?;
            }
        }
        let mut records = Vec::with_capacity(plan.last_row() - plan.first_row() + 1);
        let mut drop_behind = (start_height != end_height)
            .then(|| self.scan_drop_behind())
            .flatten();
        for step in plan.chunks() {
            records.extend(self.read_chunk_rows(step, verified_rows, drop_behind.as_mut())?);
        }
        Ok(records)
    }

    fn read_chunk_rows(
        &self,
        step: &SupplementChunkReadStep,
        verified_rows: VerifiedSupplementRowIndexSlice<'_>,
        drop_behind: Option<&mut ScanDropBehind<'_, S>>,
    ) -> Result<Vec<BlockSupplementRecord>, SettledSupplementSegmentError> {
        let chunk_idx = step.chunk_idx();
        let cached = CHUNK_CACHE.lock().get(&self.cache_id, chunk_idx);
        if let Some(bytes) = cached {
            return self.index.decode_chunk_rows(step, &bytes, verified_rows);
        }
        let range = step.compressed_range();
        let compressed = self
            .source
            .read_vec_at(range.offset, range.len)
            .inspect_err(|_| self.index.chunks[chunk_idx].mark_invalid())?;
        let bytes = self
            .index
            .verify_and_decompress_chunk(chunk_idx, &compressed)?;
        let records = self.index.decode_chunk_rows(step, &bytes, verified_rows)?;
        CHUNK_CACHE.lock().insert(CachedChunk {
            reader_id: Arc::clone(&self.cache_id),
            chunk_idx,
            bytes: Arc::new(bytes),
        });
        if let Some(scan) = drop_behind {
            scan.consumed(range);
        }
        Ok(records)
    }

    /// Reads and verifies only the Tag-1 transparent supplement vector for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, I/O, compression,
    /// or integrity failures.
    pub fn read_transparent_block(
        &self,
        height: BlockHeight,
    ) -> Result<Vec<CompactTransparentSupplementTx>, SettledSupplementSegmentError> {
        Ok(self.read_block(height)?.transparent_transactions)
    }

    /// Reads and verifies optional Tag-2 extensible canonical metadata for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, I/O, compression,
    /// or integrity failures.
    pub fn read_canonical_metadata(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CanonicalBlockSupplement>, SettledSupplementSegmentError> {
        Ok(self.read_block(height)?.canonical_metadata)
    }

    /// Streams optional Tag-2 canonical metadata across `[start_height, end_height]`
    /// one chunk at a time.
    ///
    /// # Errors
    ///
    /// Returns caller error `E` from segment read errors or `visit` callback failures.
    pub fn visit_canonical_metadata_range<E, F>(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
        mut visit: F,
    ) -> Result<(), E>
    where
        E: From<SettledSupplementSegmentError>,
        F: FnMut(BlockHeight, Option<&CanonicalBlockSupplement>) -> Result<(), E>,
    {
        let plan = self.index.plan_block_range(start_height, end_height)?;
        let row_range = plan.row_index_range();
        let raw_row_index = self
            .source
            .read_vec_at(row_range.offset, row_range.len)
            .inspect_err(|_| {
                mark_row_range_invalid(&self.index, plan.first_row(), plan.last_row());
            })
            .map_err(SettledSupplementSegmentError::Io)?;
        let verified_rows =
            self.index
                .authenticate_row_index_slice(SupplementRowIndexSlice::new(
                    row_range.offset,
                    &raw_row_index,
                ))?;
        let mut drop_behind = self.scan_drop_behind();
        if let Some(scan) = &mut drop_behind {
            scan.consumed(row_range);
        }
        for step in plan.chunks() {
            let records = self.read_chunk_rows(step, verified_rows, drop_behind.as_mut())?;
            for (row_idx, record) in step.row_range().zip(records.iter()) {
                let offset = u32::try_from(row_idx - plan.first_row()).map_err(|_| {
                    SettledSupplementSegmentError::InvalidFormat(
                        "row offset exceeds u32".to_owned(),
                    )
                })?;
                let height = BlockHeight::new(
                    start_height.value().checked_add(offset).ok_or_else(|| {
                        SettledSupplementSegmentError::InvalidFormat("height overflow".to_owned())
                    })?,
                );
                visit(height, record.canonical_metadata.as_ref())?;
            }
        }
        Ok(())
    }

    /// Splices one `ZSHRNG02` `Slab 0` `CompactBlock` payload at `height` with this
    /// segment's Tag-1 transparent supplement into one gRPC-framed [`EncodedBlock`].
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, wide-fee, I/O, or
    /// integrity errors.
    pub fn splice_block_wire(
        &self,
        height: BlockHeight,
        shielded_payload: &[u8],
        selection: CompactBlockSpliceSelection,
    ) -> Result<EncodedBlock, SettledSupplementSegmentError> {
        let mut spliced = self.splice_range_wire(height, &[shielded_payload], selection)?;
        spliced.pop().ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("missing spliced block".to_owned())
        })
    }

    /// Splices a contiguous `ZSHRNG02` `Slab 0` payload slice starting at `start_height`
    /// into gRPC-framed [`EncodedBlock`] entries, decoding each touched chunk once.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, wide-fee, I/O, or
    /// integrity errors.
    pub fn splice_range_wire<P: AsRef<[u8]>>(
        &self,
        start_height: BlockHeight,
        shielded_payloads: &[P],
        selection: CompactBlockSpliceSelection,
    ) -> Result<Vec<EncodedBlock>, SettledSupplementSegmentError> {
        if shielded_payloads.is_empty() {
            return Ok(Vec::new());
        }
        let mut encoded_blocks = Vec::with_capacity(shielded_payloads.len());
        let completed = self.splice_payloads_into(
            SupplementSpliceRequest {
                start_height,
                selection,
                order: HeightOrder::Ascending,
                cancel: None,
            },
            shielded_payloads,
            &mut |block| {
                encoded_blocks.push(block);
                Ok(())
            },
        )?;
        if !completed {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "uncancellable splice aborted unexpectedly".to_owned(),
            ));
        }
        Ok(encoded_blocks)
    }

    /// Convenience wrapper around [`Self::splice_range_wire`] accepting gRPC-framed
    /// `ZSHRNG02` [`EncodedBlock`] slices directly.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, wide-fee, I/O, or
    /// integrity errors.
    pub fn splice_encoded_range_wire(
        &self,
        start_height: BlockHeight,
        shielded_blocks: &[EncodedBlock],
        selection: CompactBlockSpliceSelection,
    ) -> Result<Vec<EncodedBlock>, SettledSupplementSegmentError> {
        let mut encoded_blocks = Vec::with_capacity(shielded_blocks.len());
        let completed = self.splice_encoded_range_into(
            SupplementSpliceRequest {
                start_height,
                selection,
                order: HeightOrder::Ascending,
                cancel: None,
            },
            shielded_blocks,
            |block| {
                encoded_blocks.push(block);
                Ok(())
            },
        )?;
        if !completed {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "uncancellable splice aborted unexpectedly".to_owned(),
            ));
        }
        Ok(encoded_blocks)
    }

    /// Splices a contiguous slice of `ZSHRNG02` [`EncodedBlock`] entries in `request.order`,
    /// reading `row_index_range` once and decompressing only chunks with needed Tag-1 entries.
    ///
    /// Returns `Ok(false)` if cooperative cancellation triggered before completion.
    pub(crate) fn splice_encoded_range_into(
        &self,
        request: SupplementSpliceRequest<'_>,
        shielded_blocks: &[EncodedBlock],
        mut emit: impl FnMut(EncodedBlock) -> Result<(), SettledSupplementSegmentError>,
    ) -> Result<bool, SettledSupplementSegmentError> {
        if request
            .cancel
            .is_some_and(|flag| flag.load(Ordering::Relaxed))
        {
            return Ok(false);
        }
        if shielded_blocks.is_empty() {
            return Ok(true);
        }
        let payloads: Vec<bytes::Bytes> =
            shielded_blocks.iter().map(EncodedBlock::payload).collect();
        self.splice_payloads_into(request, &payloads, &mut emit)
    }

    fn splice_payloads_into<P: AsRef<[u8]>>(
        &self,
        request: SupplementSpliceRequest<'_>,
        shielded_payloads: &[P],
        emit: &mut impl FnMut(EncodedBlock) -> Result<(), SettledSupplementSegmentError>,
    ) -> Result<bool, SettledSupplementSegmentError> {
        let end_height =
            self.end_height_for_count(request.start_height, shielded_payloads.len())?;
        let plan =
            self.index
                .plan_splice_range(request.start_height, end_height, request.selection)?;
        let row_range = plan.row_index_range();
        let raw_row_index = self
            .source
            .read_vec_at(row_range.offset, row_range.len)
            .inspect_err(|_| {
                mark_row_range_invalid(&self.index, plan.first_row(), plan.last_row());
            })?;
        let verified_rows =
            self.index
                .authenticate_row_index_slice(SupplementRowIndexSlice::new(
                    row_range.offset,
                    &raw_row_index,
                ))?;
        let mut scratch = SpliceScratch::default();
        let mut ctx = OrderedSpliceContext {
            verified_rows,
            plan: &plan,
            shielded_payloads,
            selection: request.selection,
            order: request.order,
            cancel: request.cancel,
            scratch: &mut scratch,
            emit,
        };
        let mut drop_behind = self.scan_drop_behind();
        match request.order {
            HeightOrder::Ascending => {
                for splice_step in plan.chunks() {
                    if !self.process_splice_step(splice_step, &mut ctx, &mut drop_behind)? {
                        return Ok(false);
                    }
                }
            }
            HeightOrder::Descending => {
                for splice_step in plan.chunks().iter().rev() {
                    if !self.process_splice_step(splice_step, &mut ctx, &mut drop_behind)? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    fn process_splice_step<P: AsRef<[u8]>>(
        &self,
        splice_step: &SupplementSpliceChunkStep,
        ctx: &mut OrderedSpliceContext<'_, P>,
        drop_behind: &mut Option<ScanDropBehind<'_, S>>,
    ) -> Result<bool, SettledSupplementSegmentError> {
        if ctx.cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
            return Ok(false);
        }
        let decode_step = splice_step.decode_step();
        let decoded_chunk = if splice_step.compressed_range().is_some() {
            Some(self.read_chunk_rows(decode_step, ctx.verified_rows, drop_behind.as_mut())?)
        } else {
            None
        };
        splice_chunk_rows_ordered(
            &self.index.header,
            ctx.verified_rows,
            decode_step.row_range(),
            ctx.plan.first_row(),
            &OrderedChunkInputs {
                shielded_payloads: ctx.shielded_payloads,
                decoded_chunk: decoded_chunk.as_deref(),
                selection: ctx.selection,
                order: ctx.order,
            },
            ctx.scratch,
            ctx.emit,
        )?;
        Ok(true)
    }

    /// Losslessly reconstructs one [`CompactBlockArtifact`] at `height` by combining
    /// its `ZSHRNG02` `Slab 0` payload with this block's decoded supplement record.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, I/O, or envelope
    /// identity mismatch.
    pub fn reconstruct_block(
        &self,
        height: BlockHeight,
        shielded_payload: &[u8],
    ) -> Result<CompactBlockArtifact, SettledSupplementSegmentError> {
        let mut reconstructed = self.reconstruct_range(height, &[shielded_payload])?;
        reconstructed.pop().ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "missing reconstructed compact block".to_owned(),
            )
        })
    }

    /// Losslessly reconstructs a contiguous slice of [`CompactBlockArtifact`] records
    /// starting at `start_height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on height, I/O, or envelope
    /// identity mismatch.
    pub fn reconstruct_range<P: AsRef<[u8]>>(
        &self,
        start_height: BlockHeight,
        shielded_payloads: &[P],
    ) -> Result<Vec<CompactBlockArtifact>, SettledSupplementSegmentError> {
        if shielded_payloads.is_empty() {
            return Ok(Vec::new());
        }
        let end_height = self.end_height_for_count(start_height, shielded_payloads.len())?;
        let plan = self.index.plan_block_range(start_height, end_height)?;
        let row_range = plan.row_index_range();
        let raw_row_index = self
            .source
            .read_vec_at(row_range.offset, row_range.len)
            .inspect_err(|_| {
                mark_row_range_invalid(&self.index, plan.first_row(), plan.last_row());
            })?;
        let verified_rows =
            self.index
                .authenticate_row_index_slice(SupplementRowIndexSlice::new(
                    row_range.offset,
                    &raw_row_index,
                ))?;
        let mut records = Vec::with_capacity(shielded_payloads.len());
        for step in plan.chunks() {
            records.extend(self.read_chunk_rows(step, verified_rows, None)?);
        }
        let mut artifacts = Vec::with_capacity(shielded_payloads.len());
        let base_row = plan.first_row();
        for (offset, (payload, record)) in shielded_payloads.iter().zip(records.iter()).enumerate()
        {
            let offset_u32 = u32::try_from(offset).map_err(|_| {
                SettledSupplementSegmentError::InvalidFormat("range offset exceeds u32".to_owned())
            })?;
            let height =
                BlockHeight::new(start_height.value().checked_add(offset_u32).ok_or_else(
                    || SettledSupplementSegmentError::InvalidFormat("height overflow".to_owned()),
                )?);
            let row_idx = base_row + offset;
            let entry = verified_rows.entry(row_idx)?;
            let expected_envelope =
                expected_envelope_from_slice(&self.index.header, row_idx, &entry, verified_rows)?;
            let reconstructed = reconstruct_compact_block_artifact(payload.as_ref(), record)?;
            verify_reconstructed_identity(expected_envelope, height, &reconstructed)?;
            artifacts.push(reconstructed);
        }
        Ok(artifacts)
    }

    fn end_height_for_count(
        &self,
        start_height: BlockHeight,
        count: usize,
    ) -> Result<BlockHeight, SettledSupplementSegmentError> {
        let end_offset = u32::try_from(count - 1).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("block range exceeds u32".to_owned())
        })?;
        start_height
            .value()
            .checked_add(end_offset)
            .map(BlockHeight::new)
            .ok_or_else(|| SettledSupplementSegmentError::HeightOutOfRange {
                height: BlockHeight::new(u32::MAX),
                start: self.index.identity.start_height,
                end: self.index.identity.end_block.height,
            })
    }
}

fn mark_row_range_invalid(
    index: &SettledSupplementSegmentIndex,
    first_row: usize,
    last_row: usize,
) {
    let chunk_span = usize::from(index.header.chunk_blocks);
    for row_idx in first_row..=last_row {
        index.rows[row_idx].mark_invalid();
        index.chunks[row_idx / chunk_span].mark_invalid();
    }
}

/// Ordering and cancellation controls for [`SettledSupplementSegmentReader::splice_encoded_range_into`].
#[derive(Clone, Copy)]
pub(crate) struct SupplementSpliceRequest<'a> {
    pub(crate) start_height: BlockHeight,
    pub(crate) selection: CompactBlockSpliceSelection,
    pub(crate) order: HeightOrder,
    pub(crate) cancel: Option<&'a AtomicBool>,
}

struct OrderedSpliceContext<'a, P> {
    verified_rows: VerifiedSupplementRowIndexSlice<'a>,
    plan: &'a SupplementSplicePlan,
    shielded_payloads: &'a [P],
    selection: CompactBlockSpliceSelection,
    order: HeightOrder,
    cancel: Option<&'a AtomicBool>,
    scratch: &'a mut SpliceScratch,
    emit: &'a mut dyn FnMut(EncodedBlock) -> Result<(), SettledSupplementSegmentError>,
}

struct OrderedChunkInputs<'a, P> {
    shielded_payloads: &'a [P],
    decoded_chunk: Option<&'a [BlockSupplementRecord]>,
    selection: CompactBlockSpliceSelection,
    order: HeightOrder,
}

#[allow(
    clippy::too_many_arguments,
    reason = "Internal chunk splice loops over header, authenticated row view, payloads, and optional chunk records."
)]
fn splice_chunk_rows_ordered<P: AsRef<[u8]>>(
    header: &SupplementSegmentHeader,
    verified_rows: VerifiedSupplementRowIndexSlice<'_>,
    row_range: Range<usize>,
    first_row: usize,
    inputs: &OrderedChunkInputs<'_, P>,
    scratch: &mut SpliceScratch,
    emit: &mut dyn FnMut(EncodedBlock) -> Result<(), SettledSupplementSegmentError>,
) -> Result<(), SettledSupplementSegmentError> {
    let chunk_start_row = row_range.start;
    let mut emit_row = |row_idx: usize| -> Result<(), SettledSupplementSegmentError> {
        let local_idx = row_idx - chunk_start_row;
        let slice_idx = row_idx - first_row;
        let shielded_payload = inputs.shielded_payloads[slice_idx].as_ref();
        let entry = verified_rows.entry(row_idx)?;
        let expected_envelope = Some(expected_envelope_from_slice(
            header,
            row_idx,
            &entry,
            verified_rows,
        )?);
        let transparent = inputs
            .decoded_chunk
            .and_then(|records| records.get(local_idx))
            .map_or(&[][..], |record| record.transparent_transactions.as_slice());
        let mut frame = Vec::with_capacity(EncodedBlock::PREFIX_LEN + shielded_payload.len() + 64);
        splice_compact_block_wire_into(
            shielded_payload,
            SpliceTarget {
                transparent,
                selection: inputs.selection,
                expected_envelope,
            },
            &mut frame,
            scratch,
        )?;
        emit(finish_encoded_frame(frame)?)
    };
    match inputs.order {
        HeightOrder::Ascending => {
            for row_idx in row_range {
                emit_row(row_idx)?;
            }
        }
        HeightOrder::Descending => {
            for row_idx in row_range.rev() {
                emit_row(row_idx)?;
            }
        }
    }
    Ok(())
}

fn verify_index_digest(
    header_bytes: &[u8; HEADER_LEN],
    raw_chunks: &[u8],
    raw_rows: &[u8],
    trailer: &[u8],
    expected_index_sha256: &[u8; 32],
) -> Result<(), SettledSupplementSegmentError> {
    let mut digest = Sha256::new();
    digest.update(header_bytes);
    digest.update(raw_chunks);
    digest.update(raw_rows);
    digest.update(trailer);
    let computed: [u8; 32] = digest.finalize().into();
    if &computed != expected_index_sha256 {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "supplement segment index SHA-256 disagrees with pinned identity".to_owned(),
        ));
    }
    Ok(())
}

fn compute_row_page_digests(raw_rows: &[u8]) -> Box<[[u8; 32]]> {
    raw_rows
        .chunks(INDEX_PAGE_ROWS * ROW_INDEX_ENTRY_LEN)
        .map(|page| Sha256::digest(page).into())
        .collect::<Vec<[u8; 32]>>()
        .into_boxed_slice()
}

type DecodedTables = (Box<[IndexedChunk]>, Box<[CompactSupplementRow]>);

fn decode_and_validate_tables(
    header: &SupplementSegmentHeader,
    codec: SupplementCompressionCodec,
    raw_chunks: &[u8],
    raw_rows: &[u8],
) -> Result<DecodedTables, SettledSupplementSegmentError> {
    let (chunk_entries, chunk_rem) = raw_chunks.as_chunks::<CHUNK_INDEX_ENTRY_LEN>();
    let (row_entries, row_rem) = raw_rows.as_chunks::<ROW_INDEX_ENTRY_LEN>();
    if !chunk_rem.is_empty() || !row_rem.is_empty() {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "supplement index alignment error".to_owned(),
        ));
    }
    let mut chunks = Vec::with_capacity(chunk_entries.len());
    for fixed in chunk_entries {
        chunks.push(IndexedChunk::from_entry(&DecodedChunkIndexEntry::decode(
            fixed,
        )?));
    }
    let mut decoded_rows = Vec::with_capacity(row_entries.len());
    let mut compact_rows = Vec::with_capacity(row_entries.len());
    for fixed in row_entries {
        let entry = DecodedRowIndexEntry::decode(fixed)?;
        compact_rows.push(CompactSupplementRow::from_entry(&entry));
        decoded_rows.push(entry);
    }
    validate_indices(header, codec, &chunks, &decoded_rows, None)?;
    Ok((chunks.into_boxed_slice(), compact_rows.into_boxed_slice()))
}

fn verify_all_compressed_chunks<S: PositionalByteSource>(
    source: &SegmentSource<S>,
    index: &SettledSupplementSegmentIndex,
    body_hasher: &mut Sha256,
    mut drop_behind: Option<ScanDropBehind<'_, S>>,
) -> Result<(), SettledSupplementSegmentError> {
    let mut scratch = vec![0_u8; STAGING_CHUNK];
    for (idx, chunk) in index.chunks.iter().enumerate() {
        let abs_offset = index
            .header
            .data_offset
            .checked_add(chunk.data_rel_offset)
            .ok_or_else(|| {
                chunk.mark_invalid();
                SettledSupplementSegmentError::InvalidFormat(
                    "chunk data offset overflow".to_owned(),
                )
            })?;
        let mut chunk_hasher = Sha256::new();
        hash_source_span_into(
            source,
            abs_offset,
            u64::from(chunk.compressed_len),
            &mut scratch,
            &mut [body_hasher, &mut chunk_hasher],
        )
        .inspect_err(|_| chunk.mark_invalid())?;
        let computed: [u8; 32] = chunk_hasher.finalize().into();
        if computed != chunk.chunk_sha256 {
            chunk.mark_invalid();
            return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                "supplement chunk {idx} SHA-256 digest mismatch during admission"
            )));
        }
        chunk.mark_valid();
        if let Some(scan) = &mut drop_behind {
            scan.consumed(ByteRange::new(abs_offset, chunk.compressed_len as usize));
        }
    }
    Ok(())
}

fn expected_envelope_from_slice(
    header: &SupplementSegmentHeader,
    row_idx: usize,
    entry: &DecodedRowIndexEntry,
    verified_rows: VerifiedSupplementRowIndexSlice<'_>,
) -> Result<BlockEnvelopeIdentity, SettledSupplementSegmentError> {
    let offset_u32 = u32::try_from(row_idx).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("row index exceeds u32".to_owned())
    })?;
    let height_u32 = header.start_height.checked_add(offset_u32).ok_or_else(|| {
        SettledSupplementSegmentError::InvalidFormat("row height overflow".to_owned())
    })?;
    let prev_hash_wire = if row_idx == 0 {
        header.first_prev_hash_wire
    } else {
        verified_rows.block_hash_wire(row_idx - 1)?
    };
    Ok(BlockEnvelopeIdentity {
        height: BlockHeight::new(height_u32),
        block_hash_wire: entry.block_hash_wire,
        prev_hash_wire,
        block_time: entry.block_time,
    })
}

fn verify_header_identity(
    envelope: BlockEnvelopeIdentity,
    header: &BlockHeaderArtifact,
) -> Result<(), SettledSupplementSegmentError> {
    if header.height != envelope.height
        || header.block_hash != block_hash_wire_to_domain(&envelope.block_hash_wire)?
        || header.parent_hash != block_hash_wire_to_domain(&envelope.prev_hash_wire)?
        || header.block_time != i64::from(envelope.block_time)
    {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "block_header at height {} disagrees with ZSTSUP01 envelope",
            header.height.value()
        )));
    }
    Ok(())
}

fn verify_reconstructed_identity(
    expected: BlockEnvelopeIdentity,
    expected_height: BlockHeight,
    reconstructed: &CompactBlockArtifact,
) -> Result<(), SettledSupplementSegmentError> {
    let expected_hash = block_hash_wire_to_domain(&expected.block_hash_wire)?;
    let expected_prev_hash = block_hash_wire_to_domain(&expected.prev_hash_wire)?;
    if expected.height != expected_height
        || reconstructed.height() != expected.height
        || reconstructed.block_hash() != expected_hash
        || reconstructed.previous_block_hash() != expected_prev_hash
        || reconstructed.time() != expected.block_time
    {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "shielded CompactBlock identity at height {} disagrees with ZSTSUP01 row index",
            expected_height.value()
        )));
    }
    Ok(())
}

fn splice_needs_tag1(
    selection: CompactBlockSpliceSelection,
    row_has_transparent: &CompactSupplementRow,
) -> bool {
    row_has_transparent.has_transparent()
        && (selection.include_transparent || selection.retain_all_transactions)
}

fn finish_encoded_frame(frame: Vec<u8>) -> Result<EncodedBlock, SettledSupplementSegmentError> {
    let alloc_len = frame.capacity();
    EncodedBlock::from_length_prefixed_in(bytes::Bytes::from(frame), alloc_len).map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity(
            "spliced CompactBlock failed gRPC framing".to_owned(),
        )
    })
}

fn decompress_exact_chunk(
    chunk_idx: usize,
    chunk: &IndexedChunk,
    compressed: &[u8],
    uncompressed_len: usize,
) -> Result<Vec<u8>, SettledSupplementSegmentError> {
    let mut decompressed = vec![0_u8; uncompressed_len];
    let written = ZSTD_DECOMPRESSOR
        .with(|slot| {
            let Ok(mut slot) = slot.try_borrow_mut() else {
                return Decompressor::new()?.decompress_to_buffer(compressed, &mut decompressed);
            };
            let decompressor = match slot.as_mut() {
                Some(decompressor) => decompressor,
                None => slot.insert(Decompressor::new()?),
            };
            decompressor.decompress_to_buffer(compressed, &mut decompressed)
        })
        .map_err(|err| {
            chunk.mark_invalid();
            SettledSupplementSegmentError::PayloadIntegrity(format!(
                "supplement chunk {chunk_idx} zstd decompression failed: {err}"
            ))
        })?;
    if written != uncompressed_len {
        chunk.mark_invalid();
        return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
            "supplement chunk {chunk_idx} decompressed to {written} bytes instead of {uncompressed_len}"
        )));
    }
    Ok(decompressed)
}

pub(super) fn validate_indices(
    header: &SupplementSegmentHeader,
    codec: SupplementCompressionCodec,
    chunks: &[IndexedChunk],
    rows: &[DecodedRowIndexEntry],
    streamed: Option<&[[u8; 32]]>,
) -> Result<(), SettledSupplementSegmentError> {
    let chunk_count = usize::try_from(header.chunk_count()?).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("chunk count exceeds usize".to_owned())
    })?;
    let row_count = usize::try_from(header.block_count).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("block count exceeds usize".to_owned())
    })?;
    if chunks.len() != chunk_count || rows.len() != row_count {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "supplement segment index cardinality disagrees with header".to_owned(),
        ));
    }
    validate_chunk_entries(header, codec, chunks)?;
    validate_row_entries(header, chunks, rows)?;
    if let Some(digests) = streamed {
        if digests.len() != chunks.len() {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "streamed chunk digest count disagrees with chunk index".to_owned(),
            ));
        }
        for (idx, (chunk, computed)) in chunks.iter().zip(digests.iter()).enumerate() {
            if *computed != chunk.chunk_sha256 {
                chunk.mark_invalid();
                return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                    "supplement chunk {idx} SHA-256 digest mismatch during admission"
                )));
            }
            chunk.mark_valid();
        }
    }
    Ok(())
}

fn validate_chunk_entries(
    header: &SupplementSegmentHeader,
    codec: SupplementCompressionCodec,
    chunks: &[IndexedChunk],
) -> Result<(), SettledSupplementSegmentError> {
    let uncompressed_limit = max_chunk_bytes(header.chunk_blocks)?;
    let compressed_limit = max_compressed_chunk_bytes(header.chunk_blocks, codec)?;
    let total_data_len = header.data_len()?;
    let mut expected_rel = 0_u64;
    for (idx, chunk) in chunks.iter().enumerate() {
        if chunk.compressed_len == 0 || chunk.compressed_len > compressed_limit {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "supplement chunk {idx} compressed length {} violates bounds [1, {compressed_limit}]",
                chunk.compressed_len
            )));
        }
        if chunk.uncompressed_len == 0 || chunk.uncompressed_len > uncompressed_limit {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "supplement chunk {idx} uncompressed length {} violates bounds [1, {uncompressed_limit}]",
                chunk.uncompressed_len
            )));
        }
        if codec == SupplementCompressionCodec::None
            && chunk.compressed_len != chunk.uncompressed_len
        {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "uncompressed supplement chunk {idx} has mismatched lengths ({} vs {})",
                chunk.compressed_len, chunk.uncompressed_len
            )));
        }
        if chunk.data_rel_offset != expected_rel {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "supplement chunk {idx} offset {} does not tile expected {expected_rel}",
                chunk.data_rel_offset
            )));
        }
        expected_rel = expected_rel
            .checked_add(u64::from(chunk.compressed_len))
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("chunk tiling overflow".to_owned())
            })?;
    }
    if expected_rel != total_data_len {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "supplement chunk index does not cover entire data region".to_owned(),
        ));
    }
    Ok(())
}

fn validate_row_entries(
    header: &SupplementSegmentHeader,
    chunks: &[IndexedChunk],
    rows: &[DecodedRowIndexEntry],
) -> Result<(), SettledSupplementSegmentError> {
    let chunk_span = usize::from(header.chunk_blocks);
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        let start = chunk_idx * chunk_span;
        let end = (start + chunk_span).min(rows.len());
        if start >= end {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement chunk index covers zero blocks".to_owned(),
            ));
        }
        let mut expected_chunk_rel = 0_u32;
        for (offset, row) in rows[start..end].iter().enumerate() {
            let offset_u32 = u32::try_from(start + offset).map_err(|_| {
                SettledSupplementSegmentError::InvalidFormat("row index exceeds u32".to_owned())
            })?;
            let height_u32 = header.start_height.checked_add(offset_u32).ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("row height overflow".to_owned())
            })?;
            let row_height = BlockHeight::new(height_u32);
            let row_len = usize::try_from(row.uncompressed_len).map_err(|_| {
                SettledSupplementSegmentError::InvalidFormat("row length exceeds usize".to_owned())
            })?;
            validate_block_payload_limit(row_height, row_len)?;
            if row.uncompressed_len == 0 {
                return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                    "supplement row at height {} is empty",
                    row_height.value()
                )));
            }
            if row.chunk_rel_offset != expected_chunk_rel {
                return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                    "supplement row at height {} offset {} disagrees with {expected_chunk_rel}",
                    row_height.value(),
                    row.chunk_rel_offset
                )));
            }
            expected_chunk_rel = expected_chunk_rel
                .checked_add(row.uncompressed_len)
                .ok_or_else(|| {
                    SettledSupplementSegmentError::InvalidFormat(
                        "chunk row tiling overflow".to_owned(),
                    )
                })?;
        }
        if expected_chunk_rel != chunk.uncompressed_len {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "supplement chunk {chunk_idx} rows sum to {expected_chunk_rel} instead of {}",
                chunk.uncompressed_len
            )));
        }
    }
    if rows.last().map(|row| row.block_hash_wire) != Some(header.last_block_hash_wire) {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "last row block hash disagrees with header".to_owned(),
        ));
    }
    Ok(())
}

fn hash_source_span_into<S: PositionalByteSource>(
    source: &S,
    mut offset: u64,
    mut remaining: u64,
    scratch: &mut [u8],
    hashers: &mut [&mut Sha256],
) -> Result<(), SettledSupplementSegmentError> {
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(scratch.len() as u64)).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("staging span exceeds usize".to_owned())
        })?;
        source.read_exact_at(offset, &mut scratch[..wanted])?;
        for hasher in hashers.iter_mut() {
            hasher.update(&scratch[..wanted]);
        }
        offset += wanted as u64;
        remaining -= wanted as u64;
    }
    Ok(())
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    #[test]
    fn cache_evicts_least_recently_used_by_bytes_and_skips_oversized_chunks() {
        let reader_id = Arc::new(());
        let mut cache = ChunkCache {
            entries: VecDeque::new(),
            bytes: 0,
        };
        let chunk = |chunk_idx, len| CachedChunk {
            reader_id: Arc::clone(&reader_id),
            chunk_idx,
            bytes: Arc::new(vec![0; len]),
        };
        let half = CHUNK_CACHE_MAX_BYTES / 2;
        cache.insert(chunk(0, half));
        cache.insert(chunk(1, half));
        assert_eq!(cache.bytes, CHUNK_CACHE_MAX_BYTES);
        assert!(cache.get(&reader_id, 0).is_some());
        cache.insert(chunk(2, half / 2));
        assert!(cache.get(&reader_id, 1).is_none());
        assert!(cache.get(&reader_id, 0).is_some());
        assert!(cache.get(&reader_id, 2).is_some());
        assert_eq!(cache.bytes, half + half / 2);
        cache.insert(chunk(3, CHUNK_CACHE_MAX_BYTES + 1));
        assert!(cache.get(&reader_id, 3).is_none());
        assert_eq!(cache.bytes, half + half / 2);
    }
}
