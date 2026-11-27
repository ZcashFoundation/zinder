//! Admission, sans-I/O range planning, and fail-closed block/transaction reader for `ZSBLKB01`.

use std::{fs::File, io, path::Path};

use sha2::{Digest, Sha256};
use zinder_core::{
    BlockHash, BlockHeight, SerializedBytesDigest, wire::decode_internal_block_hash,
};

use crate::{
    positional_source::{ByteRange, PositionalByteSource},
    segment_verity,
};

use super::{
    RawBlockCodec, RawBlockSegmentHeader, RawBlockSegmentIdentity,
    SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES, SettledRawBlockSegmentError,
    format::{
        CompactRawBlockRow, DecodedRowIndexEntry, FRAME_PREFIX_LEN, FRAME_PREFIX_U32, HEADER_LEN,
        MAX_TX_ENTRIES_PER_BLOCK, ROW_INDEX_ENTRY_LEN, ROW_INDEX_PAGE_ROWS, STAGING_CHUNK,
        TRAILER_LEN, TX_SLICE_ENTRY_LEN, TX_TABLE_PAGE_ENTRIES, TxSliceEntry, VALIDATION_INVALID,
        VALIDATION_VALID, compact_frame_span, decode_frame_prefix, max_stored_payload_bytes,
    },
};

/// Admission options when opening a [`SettledRawBlockSegmentReader`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SettledRawBlockSegmentOpenOptions {
    /// Fail admission if Linux `fs-verity` is not active and verified on the file.
    pub require_fs_verity: bool,
    /// Verify every `Slab 0` frame digest and the whole-body SHA-256 eagerly
    /// during open instead of deferring `Slab 0` verification to read time.
    pub verify_full_payload: bool,
}

/// On-disk slice of raw `128`-byte `ZSBLKB01` row index entries paired with its
/// byte offset inside the segment.
#[derive(Clone, Copy, Debug)]
pub struct RawBlockRowIndexSlice<'a> {
    /// Segment byte offset at which `bytes[0]` starts.
    pub base_offset: u64,
    /// Raw on-disk row index bytes covering at least the requested rows.
    pub bytes: &'a [u8],
}

impl<'a> RawBlockRowIndexSlice<'a> {
    /// Creates a positioned slice over raw on-disk row index entries.
    #[must_use]
    pub const fn new(base_offset: u64, bytes: &'a [u8]) -> Self {
        Self { base_offset, bytes }
    }
}

/// Authenticated view over raw on-disk `ZSBLKB01` row index entries.
///
/// For non-`fs-verity` segments, constructing this value verifies every covered
/// `64`-row page once against the segment's in-memory `row_index_page_digests`.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedRawBlockRowIndexSlice<'a> {
    base_row: usize,
    bytes: &'a [u8],
}

impl VerifiedRawBlockRowIndexSlice<'_> {
    fn entry(&self, row_idx: usize) -> Result<DecodedRowIndexEntry, SettledRawBlockSegmentError> {
        let local_idx = row_idx.checked_sub(self.base_row).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block row index slice precedes requested row".to_owned(),
            )
        })?;
        let byte_start = local_idx.checked_mul(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block row index offset overflow".to_owned(),
            )
        })?;
        let byte_end = byte_start.checked_add(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block row index offset overflow".to_owned(),
            )
        })?;
        let raw_slice = self.bytes.get(byte_start..byte_end).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block row index slice does not cover requested row".to_owned(),
            )
        })?;
        let fixed: &[u8; ROW_INDEX_ENTRY_LEN] = raw_slice.try_into().map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("short raw block row index entry".to_owned())
        })?;
        DecodedRowIndexEntry::decode(fixed)
    }
}

/// On-disk slice of raw `40`-byte `ZSBLKB01` transaction slice entries paired
/// with its byte offset inside the segment.
#[derive(Clone, Copy, Debug)]
pub struct RawBlockTxTableSlice<'a> {
    /// Segment byte offset at which `bytes[0]` starts.
    pub base_offset: u64,
    /// Raw on-disk transaction slice entries covering at least the requested range.
    pub bytes: &'a [u8],
}

impl<'a> RawBlockTxTableSlice<'a> {
    /// Creates a positioned slice over raw on-disk transaction slice entries.
    #[must_use]
    pub const fn new(base_offset: u64, bytes: &'a [u8]) -> Self {
        Self { base_offset, bytes }
    }
}

/// Authenticated view over raw on-disk `ZSBLKB01` transaction slice entries.
///
/// For non-`fs-verity` segments, constructing this value verifies every covered
/// `256`-entry page once against the segment's in-memory `tx_table_page_digests`.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedRawBlockTxTableSlice<'a> {
    base_tx: usize,
    bytes: &'a [u8],
}

impl VerifiedRawBlockTxTableSlice<'_> {
    fn entry(&self, global_tx_idx: usize) -> Result<TxSliceEntry, SettledRawBlockSegmentError> {
        let local_idx = global_tx_idx.checked_sub(self.base_tx).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block tx table slice precedes requested entry".to_owned(),
            )
        })?;
        let byte_start = local_idx.checked_mul(TX_SLICE_ENTRY_LEN).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block tx table offset overflow".to_owned(),
            )
        })?;
        let byte_end = byte_start.checked_add(TX_SLICE_ENTRY_LEN).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block tx table offset overflow".to_owned(),
            )
        })?;
        let raw_slice = self.bytes.get(byte_start..byte_end).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block tx table slice does not cover requested entry".to_owned(),
            )
        })?;
        let fixed: &[u8; TX_SLICE_ENTRY_LEN] = raw_slice.try_into().map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("short raw block tx slice entry".to_owned())
        })?;
        TxSliceEntry::decode(fixed)
    }
}

/// Sans-I/O fetch plan for reading one consensus raw block at `height`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawBlockReadPlan {
    row_idx: usize,
    height: BlockHeight,
    slab_byte_range: ByteRange,
    row_index_byte_range: ByteRange,
}

impl RawBlockReadPlan {
    /// Segment-relative zero-based row index of the requested block.
    #[must_use]
    pub const fn row_idx(&self) -> usize {
        self.row_idx
    }

    /// Block height targeted by this read plan.
    #[must_use]
    pub const fn height(&self) -> BlockHeight {
        self.height
    }

    /// Positional byte range of the block's frame in `Slab 0`.
    #[must_use]
    pub const fn slab_byte_range(&self) -> ByteRange {
        self.slab_byte_range
    }

    /// Positional byte range in the `128`-byte row index required for verification.
    #[must_use]
    pub const fn row_index_byte_range(&self) -> ByteRange {
        self.row_index_byte_range
    }
}

/// Sans-I/O fetch plan for reading one consensus transaction at `(height, tx_index)`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawTransactionReadPlan {
    block_plan: RawBlockReadPlan,
    global_tx_idx: usize,
    tx_table_byte_range: ByteRange,
}

impl RawTransactionReadPlan {
    /// Underlying block fetch plan containing the target transaction.
    #[must_use]
    pub const fn block_plan(&self) -> RawBlockReadPlan {
        self.block_plan
    }

    /// Positional byte range of the containing block frame in `Slab 0`.
    #[must_use]
    pub const fn slab_byte_range(&self) -> ByteRange {
        self.block_plan.slab_byte_range
    }

    /// Positional byte range in the `128`-byte row index required for verification.
    #[must_use]
    pub const fn row_index_byte_range(&self) -> ByteRange {
        self.block_plan.row_index_byte_range
    }

    /// Positional byte range in the `40`-byte tx slice table required for slicing and digest checks.
    #[must_use]
    pub const fn tx_table_byte_range(&self) -> ByteRange {
        self.tx_table_byte_range
    }

    /// Zero-based global transaction index inside the segment's tx slice table.
    #[must_use]
    pub const fn global_tx_idx(&self) -> usize {
        self.global_tx_idx
    }
}

/// Sans-I/O fetch plan for reading every transaction digest of one block without
/// reading `Slab 0`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawBlockTxDigestsPlan {
    row_idx: usize,
    height: BlockHeight,
    tx_start_index: usize,
    tx_count: u32,
    tx_table_byte_range: Option<ByteRange>,
}

impl RawBlockTxDigestsPlan {
    /// Segment-relative zero-based row index of the block.
    #[must_use]
    pub const fn row_idx(&self) -> usize {
        self.row_idx
    }

    /// Block height covered by this plan.
    #[must_use]
    pub const fn height(&self) -> BlockHeight {
        self.height
    }

    /// First global transaction index belonging to `height`.
    #[must_use]
    pub const fn tx_start_index(&self) -> usize {
        self.tx_start_index
    }

    /// Number of indexed transactions in `height`.
    #[must_use]
    pub const fn tx_count(&self) -> u32 {
        self.tx_count
    }

    /// Positional byte range in the `40`-byte tx slice table, or `None` when `tx_count == 0`.
    #[must_use]
    pub const fn tx_table_byte_range(&self) -> Option<ByteRange> {
        self.tx_table_byte_range
    }
}

/// Compact in-memory `ZSBLKB01` index (`20 B/block`, zero `Vec<TxSliceEntry>`)
/// supporting sans-I/O range planning and verification.
pub struct SettledRawBlockSegmentIndex {
    identity: RawBlockSegmentIdentity,
    kernel_verified: bool,
    slab_offset: u64,
    slab_len: u32,
    tx_table_offset: u64,
    tx_entry_count: usize,
    row_index_offset: u64,
    first_prev_hash_wire: [u8; 32],
    rows: Box<[CompactRawBlockRow]>,
    row_index_page_digests: Option<Box<[[u8; 32]]>>,
    tx_table_page_digests: Option<Box<[[u8; 32]]>>,
}

impl SettledRawBlockSegmentIndex {
    /// Authenticates and decodes a `ZSBLKB01` header, raw transaction slice
    /// table, raw `128`-byte row index, and trailing `32`-byte SHA-256 trailer
    /// in `O(index)` without reading `Slab 0` or retaining `Vec<TxSliceEntry>`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on malformed fields, `sha256` or
    /// `index_sha256` pin mismatch, invalid row bounds/tiling, or invalid tx slices.
    pub fn decode_index(
        header_bytes: &[u8; HEADER_LEN],
        index_and_trailer: &[u8],
        expected: &RawBlockSegmentIdentity,
        total_len: u64,
        kernel_verified: bool,
    ) -> Result<Self, SettledRawBlockSegmentError> {
        let header = RawBlockSegmentHeader::decode(header_bytes, expected, total_len)?;
        let expected_range = header.index_and_trailer_range()?;
        if index_and_trailer.len() != expected_range.len {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block index and trailer slice length disagrees with header".to_owned(),
            ));
        }
        let tx_count = header.tx_entry_count()?;
        let tx_table_len = tx_count * TX_SLICE_ENTRY_LEN;
        let row_index_len = usize::try_from(header.block_count).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("block count exceeds usize".to_owned())
        })? * ROW_INDEX_ENTRY_LEN;
        if tx_table_len + row_index_len + TRAILER_LEN != index_and_trailer.len() {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block index layout length mismatch".to_owned(),
            ));
        }
        let (raw_tx_table, rest) = index_and_trailer.split_at(tx_table_len);
        let (raw_row_index, trailer) = rest.split_at(row_index_len);
        if trailer != expected.sha256 {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment trailer SHA-256 disagrees with pinned identity".to_owned(),
            ));
        }
        verify_index_digest(
            header_bytes,
            raw_tx_table,
            raw_row_index,
            trailer,
            &expected.index_sha256,
        )?;
        let slab_len = header.slab_len()?;
        let rows = decode_and_validate_rows(raw_row_index, raw_tx_table, slab_len, &header)?;
        let (row_index_page_digests, tx_table_page_digests) = if kernel_verified {
            (None, None)
        } else {
            (
                Some(compute_page_digests(
                    raw_row_index,
                    ROW_INDEX_PAGE_ROWS * ROW_INDEX_ENTRY_LEN,
                )),
                Some(compute_page_digests(
                    raw_tx_table,
                    TX_TABLE_PAGE_ENTRIES * TX_SLICE_ENTRY_LEN,
                )),
            )
        };
        Ok(Self {
            identity: *expected,
            kernel_verified,
            slab_offset: header.slab_offset,
            slab_len,
            tx_table_offset: header.tx_table_offset,
            tx_entry_count: tx_count,
            row_index_offset: header.row_index_offset,
            first_prev_hash_wire: header.first_prev_hash_wire,
            rows,
            row_index_page_digests,
            tx_table_page_digests,
        })
    }

    /// Returns whether this index was admitted under kernel `fs-verity` protection.
    #[must_use]
    pub const fn kernel_verified(&self) -> bool {
        self.kernel_verified
    }

    /// Returns the authenticated segment identity.
    #[must_use]
    pub const fn identity(&self) -> RawBlockSegmentIdentity {
        self.identity
    }

    /// Returns the number of contiguous blocks stored in this segment.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns the total transaction slice entries indexed in this segment.
    #[must_use]
    pub const fn tx_entry_count(&self) -> usize {
        self.tx_entry_count
    }

    /// Returns the byte offset of `Slab 0`.
    #[must_use]
    pub const fn slab_offset(&self) -> u64 {
        self.slab_offset
    }

    /// Returns the byte length of `Slab 0`.
    #[must_use]
    pub const fn slab_len(&self) -> u32 {
        self.slab_len
    }

    /// Returns the starting byte offset of the flat transaction slice table.
    #[must_use]
    pub const fn tx_table_offset(&self) -> u64 {
        self.tx_table_offset
    }

    /// Returns the starting byte offset of the `128`-byte per-block row index.
    #[must_use]
    pub const fn row_index_offset(&self) -> u64 {
        self.row_index_offset
    }

    /// Returns the wire-order previous block hash declared before the first block.
    #[must_use]
    pub const fn first_prev_hash_wire(&self) -> &[u8; 32] {
        &self.first_prev_hash_wire
    }

    /// Returns the authenticated previous block hash declared before the segment's first block.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError::PayloadIntegrity`] if the header
    /// hash is not a valid internal block hash.
    pub fn first_previous_block_hash(&self) -> Result<BlockHash, SettledRawBlockSegmentError> {
        decode_wire_hash(&self.first_prev_hash_wire, "first_prev_hash")
    }

    /// Returns the heap bytes retained by compact in-memory row tables and optional
    /// portable page digests (zero `Vec<TxSliceEntry>` allocations).
    #[must_use]
    pub fn heap_metadata_bytes(&self) -> usize {
        let rows_bytes = self.rows.len() * std::mem::size_of::<CompactRawBlockRow>();
        let row_pages = self
            .row_index_page_digests
            .as_ref()
            .map_or(0, |pages| pages.len() * 32);
        let tx_pages = self
            .tx_table_page_digests
            .as_ref()
            .map_or(0, |pages| pages.len() * 32);
        rows_bytes + row_pages + tx_pages
    }

    /// Plans sans-I/O positional fetches (`row_index` + `Slab 0` frame) to read one block.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] if `height` is out of range or
    /// its row previously entered sticky invalid state.
    pub fn plan_block_read(
        &self,
        height: BlockHeight,
    ) -> Result<RawBlockReadPlan, SettledRawBlockSegmentError> {
        let row_idx = self.resolve_row_index(height)?;
        if self.rows[row_idx].validation_state() == VALIDATION_INVALID {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment row previously failed validation".to_owned(),
            ));
        }
        let (rel_offset, frame_len) = compact_frame_span(&self.rows, self.slab_len, row_idx)?;
        let slab_byte_range = ByteRange::new(
            self.slab_offset
                .checked_add(u64::from(rel_offset))
                .ok_or_else(|| {
                    SettledRawBlockSegmentError::InvalidFormat("slab offset overflow".to_owned())
                })?,
            usize::try_from(frame_len).map_err(|_| {
                SettledRawBlockSegmentError::InvalidFormat("frame length exceeds usize".to_owned())
            })?,
        );
        let row_index_byte_range = self.row_index_byte_range_for_span(row_idx, row_idx + 1)?;
        Ok(RawBlockReadPlan {
            row_idx,
            height,
            slab_byte_range,
            row_index_byte_range,
        })
    }

    /// Plans sans-I/O positional fetches (`tx_table` + `row_index` + `Slab 0`
    /// frame) to read and verify one consensus transaction.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on height/transaction index bounds
    /// or sticky invalid row state.
    pub fn plan_tx_read(
        &self,
        height: BlockHeight,
        tx_index: u32,
    ) -> Result<RawTransactionReadPlan, SettledRawBlockSegmentError> {
        let block_plan = self.plan_block_read(height)?;
        let row = &self.rows[block_plan.row_idx];
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
        let tx_table_byte_range = self
            .tx_table_byte_range_for_span(global_tx_idx, global_tx_idx + 1)?
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat(
                    "empty tx table byte range for single transaction".to_owned(),
                )
            })?;
        Ok(RawTransactionReadPlan {
            block_plan,
            global_tx_idx,
            tx_table_byte_range,
        })
    }

    /// Plans a sans-I/O positioned read over all `tx_table` slice digests for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] if `height` is out of bounds or
    /// previously entered sticky invalid state.
    pub fn plan_tx_digests_read(
        &self,
        height: BlockHeight,
    ) -> Result<RawBlockTxDigestsPlan, SettledRawBlockSegmentError> {
        let row_idx = self.resolve_row_index(height)?;
        let row = &self.rows[row_idx];
        if row.validation_state() == VALIDATION_INVALID {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment row previously failed validation".to_owned(),
            ));
        }
        let tx_start_index = usize::try_from(row.tx_start_index).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("transaction index overflow".to_owned())
        })?;
        let tx_count_usize = usize::try_from(row.tx_count).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("transaction count overflow".to_owned())
        })?;
        let tx_end = tx_start_index.checked_add(tx_count_usize).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("transaction end overflow".to_owned())
        })?;
        let tx_table_byte_range = self.tx_table_byte_range_for_span(tx_start_index, tx_end)?;
        Ok(RawBlockTxDigestsPlan {
            row_idx,
            height,
            tx_start_index,
            tx_count: row.tx_count,
            tx_table_byte_range,
        })
    }

    /// Returns the coalesced [`ByteRange`] covering row index entries across
    /// `[start_height, end_height]` (page-aligned when `!kernel_verified`).
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on invalid height ordering, out-of-range
    /// bounds, or sticky invalid rows.
    pub fn plan_row_index_range(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<ByteRange, SettledRawBlockSegmentError> {
        if start_height > end_height {
            return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
                "reversed raw block row range {}..={}",
                start_height.value(),
                end_height.value()
            )));
        }
        let start_row = self.resolve_row_index(start_height)?;
        let end_row = self.resolve_row_index(end_height)?;
        for row_idx in start_row..=end_row {
            if self.rows[row_idx].validation_state() == VALIDATION_INVALID {
                return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                    "raw block segment row previously failed validation".to_owned(),
                ));
            }
        }
        self.row_index_byte_range_for_span(start_row, end_row + 1)
    }

    /// Returns the coalesced [`ByteRange`] covering `[start_tx..start_tx + tx_count)`
    /// in the flat tx slice table (page-aligned when `!kernel_verified`), or `None`
    /// when `tx_count == 0`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError::InvalidFormat`] when the requested
    /// tx range exceeds `tx_entry_count`.
    pub fn plan_tx_table_range(
        &self,
        start_tx: usize,
        tx_count: u32,
    ) -> Result<Option<ByteRange>, SettledRawBlockSegmentError> {
        let count_usize = usize::try_from(tx_count).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("tx count exceeds usize".to_owned())
        })?;
        let end_tx = start_tx.checked_add(count_usize).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("tx table range overflows usize".to_owned())
        })?;
        self.tx_table_byte_range_for_span(start_tx, end_tx)
    }

    /// Authenticates raw `128`-byte row index bytes covering full `64`-row pages
    /// (when `!kernel_verified`) or exact entries (when `kernel_verified`).
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError::InvalidFormat`] on misaligned bounds
    /// or [`SettledRawBlockSegmentError::PayloadIntegrity`] if any non-`fs-verity`
    /// row page digest disagrees, latching `VALIDATION_INVALID` on affected rows.
    pub fn authenticate_row_index_slice<'a>(
        &self,
        slice: RawBlockRowIndexSlice<'a>,
    ) -> Result<VerifiedRawBlockRowIndexSlice<'a>, SettledRawBlockSegmentError> {
        let rel = slice
            .base_offset
            .checked_sub(self.row_index_offset)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat(
                    "raw block row index slice begins before row_index_offset".to_owned(),
                )
            })?;
        if !rel.is_multiple_of(ROW_INDEX_ENTRY_LEN as u64)
            || !slice.bytes.len().is_multiple_of(ROW_INDEX_ENTRY_LEN)
        {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block row index slice is not aligned to 128-byte entries".to_owned(),
            ));
        }
        let start_row = usize::try_from(rel / ROW_INDEX_ENTRY_LEN as u64).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("row offset exceeds usize".to_owned())
        })?;
        let end_row = start_row
            .checked_add(slice.bytes.len() / ROW_INDEX_ENTRY_LEN)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat("row end exceeds usize".to_owned())
            })?;
        if end_row > self.rows.len() {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block row index slice extends past block_count".to_owned(),
            ));
        }
        if let Some(digests) = &self.row_index_page_digests {
            self.verify_portable_row_pages(start_row, end_row, slice.bytes, digests)?;
        }
        Ok(VerifiedRawBlockRowIndexSlice {
            base_row: start_row,
            bytes: slice.bytes,
        })
    }

    /// Authenticates raw `40`-byte transaction slice entries covering full `256`-entry
    /// pages (when `!kernel_verified`) or exact entries (when `kernel_verified`).
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError::InvalidFormat`] on misaligned bounds
    /// or [`SettledRawBlockSegmentError::PayloadIntegrity`] if any non-`fs-verity`
    /// tx-table page digest disagrees, latching `VALIDATION_INVALID` on affected rows.
    pub fn authenticate_tx_table_slice<'a>(
        &self,
        slice: RawBlockTxTableSlice<'a>,
    ) -> Result<VerifiedRawBlockTxTableSlice<'a>, SettledRawBlockSegmentError> {
        let rel = slice
            .base_offset
            .checked_sub(self.tx_table_offset)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat(
                    "raw block tx table slice begins before tx_table_offset".to_owned(),
                )
            })?;
        if !rel.is_multiple_of(TX_SLICE_ENTRY_LEN as u64)
            || !slice.bytes.len().is_multiple_of(TX_SLICE_ENTRY_LEN)
        {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block tx table slice is not aligned to 40-byte entries".to_owned(),
            ));
        }
        let start_tx = usize::try_from(rel / TX_SLICE_ENTRY_LEN as u64).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("tx table offset exceeds usize".to_owned())
        })?;
        let end_tx = start_tx
            .checked_add(slice.bytes.len() / TX_SLICE_ENTRY_LEN)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat("tx table end exceeds usize".to_owned())
            })?;
        if end_tx > self.tx_entry_count {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block tx table slice extends past tx_entry_count".to_owned(),
            ));
        }
        if let Some(digests) = &self.tx_table_page_digests {
            self.verify_portable_tx_pages(start_tx, end_tx, slice.bytes, digests)?;
        }
        Ok(VerifiedRawBlockTxTableSlice {
            base_tx: start_tx,
            bytes: slice.bytes,
        })
    }

    /// Verifies and decodes one fetched `Slab 0` block frame using `verified_rows`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] if framing, length, codec, or
    /// SHA-256 digests disagree, latching `VALIDATION_INVALID` on failure.
    pub fn verify_and_decode_block(
        &self,
        plan: &RawBlockReadPlan,
        verified_rows: VerifiedRawBlockRowIndexSlice<'_>,
        frame: &[u8],
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let row = &self.rows[plan.row_idx];
        let state = row.validation_state();
        if state == VALIDATION_INVALID {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment row previously failed validation".to_owned(),
            ));
        }
        let decode_attempt = (|| {
            if frame.len() != plan.slab_byte_range.len {
                return Err(SettledRawBlockSegmentError::InvalidFormat(
                    "fetched raw block frame length disagrees with plan".to_owned(),
                ));
            }
            let row_entry = verified_rows.entry(plan.row_idx)?;
            if row_entry.slab_rel_offset != row.slab_rel_offset
                || row_entry.uncompressed_len != row.uncompressed_len
                || row_entry.tx_start_index != row.tx_start_index
                || row_entry.tx_count != row.tx_count
            {
                return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                    "fetched raw block row descriptor disagrees with admitted index".to_owned(),
                ));
            }
            decode_and_verify_frame(
                &row_entry,
                frame,
                self.identity.codec,
                !self.kernel_verified || state != VALIDATION_VALID,
            )
        })();
        match decode_attempt {
            Ok(decoded_block) => {
                row.mark_valid();
                Ok(decoded_block)
            }
            Err(err) => {
                row.mark_invalid();
                Err(err)
            }
        }
    }

    /// Verifies, decodes, slices, and SHA-256 checks one consensus transaction
    /// from fetched `Slab 0`, `row_index`, and `tx_table` bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on framing, decompression, slice
    /// bounds, or SHA-256 mismatch, latching `VALIDATION_INVALID` on failure.
    pub fn verify_and_decode_tx(
        &self,
        plan: &RawTransactionReadPlan,
        verified_rows: VerifiedRawBlockRowIndexSlice<'_>,
        verified_txs: VerifiedRawBlockTxTableSlice<'_>,
        frame: &[u8],
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let decoded_block = self.verify_and_decode_block(&plan.block_plan, verified_rows, frame)?;
        self.verify_and_decode_tx_from_block(plan, &decoded_block, verified_txs)
    }

    /// Slices and SHA-256 checks one consensus transaction from already-verified
    /// `decoded_block` bytes and `verified_txs`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] if the slice is out of bounds or
    /// its SHA-256 digest disagrees, latching `VALIDATION_INVALID` on the row.
    pub fn verify_and_decode_tx_from_block(
        &self,
        plan: &RawTransactionReadPlan,
        decoded_block: &[u8],
        verified_txs: VerifiedRawBlockTxTableSlice<'_>,
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let row = &self.rows[plan.block_plan.row_idx];
        let outcome = (|| {
            let tx_entry = verified_txs.entry(plan.global_tx_idx)?;
            slice_and_verify_tx(decoded_block, &tx_entry)
        })();
        outcome.inspect_err(|_| row.mark_invalid())
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

    fn row_index_byte_range_for_span(
        &self,
        start_row: usize,
        end_row: usize,
    ) -> Result<ByteRange, SettledRawBlockSegmentError> {
        let (aligned_start, aligned_end) = if self.kernel_verified {
            (start_row, end_row)
        } else {
            let page_start = (start_row / ROW_INDEX_PAGE_ROWS) * ROW_INDEX_PAGE_ROWS;
            let page_end = end_row
                .div_ceil(ROW_INDEX_PAGE_ROWS)
                .saturating_mul(ROW_INDEX_PAGE_ROWS)
                .min(self.rows.len());
            (page_start, page_end)
        };
        let byte_offset = self
            .row_index_offset
            .checked_add((aligned_start as u64) * ROW_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat("row index offset overflow".to_owned())
            })?;
        let byte_len = aligned_end
            .checked_sub(aligned_start)
            .and_then(|rows| rows.checked_mul(ROW_INDEX_ENTRY_LEN))
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat("row index slice overflow".to_owned())
            })?;
        Ok(ByteRange::new(byte_offset, byte_len))
    }

    fn tx_table_byte_range_for_span(
        &self,
        start_tx: usize,
        end_tx: usize,
    ) -> Result<Option<ByteRange>, SettledRawBlockSegmentError> {
        if end_tx > self.tx_entry_count || start_tx > end_tx {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "transaction slice entry out of bounds".to_owned(),
            ));
        }
        if start_tx == end_tx {
            return Ok(None);
        }
        let (aligned_start, aligned_end) = if self.kernel_verified {
            (start_tx, end_tx)
        } else {
            let page_start = (start_tx / TX_TABLE_PAGE_ENTRIES) * TX_TABLE_PAGE_ENTRIES;
            let page_end = end_tx
                .div_ceil(TX_TABLE_PAGE_ENTRIES)
                .saturating_mul(TX_TABLE_PAGE_ENTRIES)
                .min(self.tx_entry_count);
            (page_start, page_end)
        };
        let byte_offset = self
            .tx_table_offset
            .checked_add((aligned_start as u64) * TX_SLICE_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat("tx table offset overflow".to_owned())
            })?;
        let byte_len = aligned_end
            .checked_sub(aligned_start)
            .and_then(|count| count.checked_mul(TX_SLICE_ENTRY_LEN))
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat("tx table slice overflow".to_owned())
            })?;
        Ok(Some(ByteRange::new(byte_offset, byte_len)))
    }

    fn verify_portable_row_pages(
        &self,
        start_row: usize,
        end_row: usize,
        raw_slice: &[u8],
        digests: &[[u8; 32]],
    ) -> Result<(), SettledRawBlockSegmentError> {
        if start_row == end_row {
            return Ok(());
        }
        if !start_row.is_multiple_of(ROW_INDEX_PAGE_ROWS)
            || (end_row != self.rows.len() && !end_row.is_multiple_of(ROW_INDEX_PAGE_ROWS))
        {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "portable raw block row index slice must cover full 64-row pages".to_owned(),
            ));
        }
        let first_page = start_row / ROW_INDEX_PAGE_ROWS;
        for (page_offset, page_bytes) in raw_slice
            .chunks(ROW_INDEX_PAGE_ROWS * ROW_INDEX_ENTRY_LEN)
            .enumerate()
        {
            let page_idx = first_page + page_offset;
            let computed: [u8; 32] = Sha256::digest(page_bytes).into();
            if computed != digests[page_idx] {
                let bad_start = page_idx * ROW_INDEX_PAGE_ROWS;
                let bad_end = (bad_start + ROW_INDEX_PAGE_ROWS).min(self.rows.len());
                for idx in bad_start..bad_end {
                    self.rows[idx].mark_invalid();
                }
                return Err(SettledRawBlockSegmentError::PayloadIntegrity(format!(
                    "portable raw block row index page {page_idx} SHA-256 mismatch"
                )));
            }
        }
        Ok(())
    }

    fn verify_portable_tx_pages(
        &self,
        start_tx: usize,
        end_tx: usize,
        raw_slice: &[u8],
        digests: &[[u8; 32]],
    ) -> Result<(), SettledRawBlockSegmentError> {
        if start_tx == end_tx {
            return Ok(());
        }
        if !start_tx.is_multiple_of(TX_TABLE_PAGE_ENTRIES)
            || (end_tx != self.tx_entry_count && !end_tx.is_multiple_of(TX_TABLE_PAGE_ENTRIES))
        {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "portable raw block tx table slice must cover full 256-entry pages".to_owned(),
            ));
        }
        let first_page = start_tx / TX_TABLE_PAGE_ENTRIES;
        for (page_offset, page_bytes) in raw_slice
            .chunks(TX_TABLE_PAGE_ENTRIES * TX_SLICE_ENTRY_LEN)
            .enumerate()
        {
            let page_idx = first_page + page_offset;
            let computed: [u8; 32] = Sha256::digest(page_bytes).into();
            if computed != digests[page_idx] {
                let bad_start = page_idx * TX_TABLE_PAGE_ENTRIES;
                let bad_end = (bad_start + TX_TABLE_PAGE_ENTRIES).min(self.tx_entry_count);
                for row in &self.rows {
                    let row_start = usize::try_from(row.tx_start_index).unwrap_or(usize::MAX);
                    let row_end =
                        row_start.saturating_add(usize::try_from(row.tx_count).unwrap_or(0));
                    if row_end > bad_start && row_start < bad_end {
                        row.mark_invalid();
                    }
                }
                return Err(SettledRawBlockSegmentError::PayloadIntegrity(format!(
                    "portable raw block tx table page {page_idx} SHA-256 mismatch"
                )));
            }
        }
        Ok(())
    }
}

/// Authenticated reader over one settled `ZSBLKB01` raw block segment backed
/// by a [`PositionalByteSource`] (default [`File`]).
pub struct SettledRawBlockSegmentReader<S = File> {
    source: S,
    index: SettledRawBlockSegmentIndex,
}

impl SettledRawBlockSegmentReader<File> {
    /// Opens and authenticates a `ZSBLKB01` segment against `expected`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on open I/O failure, `fs-verity`
    /// check failure, or header/`index_sha256` verification failure.
    pub fn open(
        path: &Path,
        expected: &RawBlockSegmentIdentity,
        options: SettledRawBlockSegmentOpenOptions,
    ) -> Result<Self, SettledRawBlockSegmentError> {
        let file = segment_verity::open_regular_file(path, "raw block segment")?;
        Self::open_file(file, expected, options)
    }

    /// Authenticates an already-opened regular `ZSBLKB01` segment file against `expected`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on non-regular files, `fs-verity`
    /// mismatch/absence when required, or header/`index_sha256` verification failure.
    pub fn open_file(
        file: File,
        expected: &RawBlockSegmentIdentity,
        options: SettledRawBlockSegmentOpenOptions,
    ) -> Result<Self, SettledRawBlockSegmentError> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected a regular settled raw block segment file",
            )
            .into());
        }
        let measured_verity = segment_verity::measure(&file)?;
        let kernel_verified = segment_verity::check_pinned_verity(
            expected.fs_verity_sha256,
            measured_verity,
            "raw block segment",
        )?;
        if options.require_fs_verity && !kernel_verified {
            return Err(SettledRawBlockSegmentError::FsVerityRequired);
        }
        Self::admit_with_verity(file, expected, options, kernel_verified)
    }
}

impl<S: PositionalByteSource> SettledRawBlockSegmentReader<S> {
    /// Opens and authenticates any [`PositionalByteSource`] against `expected`
    /// in `O(index)` (or full-payload mode when `options.verify_full_payload` is set).
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError::FsVerityRequired`] if
    /// `require_fs_verity` is requested on a generic source, or any
    /// header/`index_sha256`/payload failure.
    pub fn open_source(
        source: S,
        expected: &RawBlockSegmentIdentity,
        options: SettledRawBlockSegmentOpenOptions,
    ) -> Result<Self, SettledRawBlockSegmentError> {
        if options.require_fs_verity {
            return Err(SettledRawBlockSegmentError::FsVerityRequired);
        }
        Self::admit_with_verity(source, expected, options, false)
    }

    fn admit_with_verity(
        source: S,
        expected: &RawBlockSegmentIdentity,
        options: SettledRawBlockSegmentOpenOptions,
        kernel_verified: bool,
    ) -> Result<Self, SettledRawBlockSegmentError> {
        let total_len = source.total_len()?;
        let mut header_bytes = [0_u8; HEADER_LEN];
        source.read_exact_at(0, &mut header_bytes)?;
        let header = RawBlockSegmentHeader::decode(&header_bytes, expected, total_len)?;
        let trailer_range = header.trailer_range()?;
        let mut trailer_bytes = [0_u8; TRAILER_LEN];
        source.read_exact_at(trailer_range.offset, &mut trailer_bytes)?;
        if trailer_bytes != expected.sha256 {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment trailer SHA-256 mismatch".to_owned(),
            ));
        }
        let index_range = header.index_and_trailer_range()?;
        let index_and_trailer = source.read_vec_at(index_range.offset, index_range.len)?;
        let index = SettledRawBlockSegmentIndex::decode_index(
            &header_bytes,
            &index_and_trailer,
            expected,
            total_len,
            kernel_verified,
        )?;
        let reader = Self { source, index };
        if options.verify_full_payload {
            reader.verify_full_payload_with_raw(&header_bytes, &index_and_trailer)?;
        }
        Ok(reader)
    }

    /// Streams header, every `Slab 0` frame, tx slice table, and row index from
    /// the source and verifies whole-body SHA-256 and per-frame digests.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on I/O failure or any frame/index
    /// digest mismatch, latching sticky `VALIDATION_INVALID` on affected rows.
    pub fn verify_full_payload(&self) -> Result<(), SettledRawBlockSegmentError> {
        let total_len = self.source.total_len()?;
        let mut header_bytes = [0_u8; HEADER_LEN];
        self.source.read_exact_at(0, &mut header_bytes)?;
        let header = RawBlockSegmentHeader::decode(&header_bytes, &self.index.identity, total_len)?;
        let index_range = header.index_and_trailer_range()?;
        let index_and_trailer = self
            .source
            .read_vec_at(index_range.offset, index_range.len)?;
        self.verify_full_payload_with_raw(&header_bytes, &index_and_trailer)
    }

    fn verify_full_payload_with_raw(
        &self,
        header_bytes: &[u8; HEADER_LEN],
        index_and_trailer: &[u8],
    ) -> Result<(), SettledRawBlockSegmentError> {
        let tx_table_len = self.index.tx_entry_count * TX_SLICE_ENTRY_LEN;
        let row_index_len = self.index.rows.len() * ROW_INDEX_ENTRY_LEN;
        if index_and_trailer.len() != tx_table_len + row_index_len + TRAILER_LEN {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block index and trailer slice length disagrees with header".to_owned(),
            ));
        }
        let (raw_tx_table, rest) = index_and_trailer.split_at(tx_table_len);
        let (raw_row_index, trailer) = rest.split_at(row_index_len);
        if trailer != self.index.identity.sha256 {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment trailer SHA-256 mismatch".to_owned(),
            ));
        }
        verify_index_digest(
            header_bytes,
            raw_tx_table,
            raw_row_index,
            trailer,
            &self.index.identity.index_sha256,
        )?;
        let mut whole_digest = Sha256::new();
        whole_digest.update(header_bytes);
        verify_all_slab_frames(&self.source, &self.index, raw_row_index, &mut whole_digest)?;
        whole_digest.update(raw_tx_table);
        whole_digest.update(raw_row_index);
        let computed: [u8; 32] = whole_digest.finalize().into();
        if computed != trailer || computed != self.index.identity.sha256 {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "portable raw block segment length or SHA-256 mismatch".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns a reference to the compact in-memory index and sans-I/O planner.
    #[must_use]
    pub const fn index(&self) -> &SettledRawBlockSegmentIndex {
        &self.index
    }

    /// Returns whether admission proved kernel `fs-verity` protection.
    #[must_use]
    pub const fn kernel_verified(&self) -> bool {
        self.index.kernel_verified
    }

    /// Returns the authenticated segment identity.
    #[must_use]
    pub const fn identity(&self) -> RawBlockSegmentIdentity {
        self.index.identity
    }

    /// Returns the number of blocks stored in this segment.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.index.rows.len()
    }

    /// Returns the total transaction slice entries indexed in this segment.
    #[must_use]
    pub const fn tx_entry_count(&self) -> usize {
        self.index.tx_entry_count
    }

    /// Returns the heap bytes retained by compact in-memory index metadata.
    #[must_use]
    pub fn heap_metadata_bytes(&self) -> usize {
        self.index.heap_metadata_bytes()
    }

    /// Returns the authenticated previous block hash declared before the segment's first block.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError::PayloadIntegrity`] if the stored
    /// header hash is not a valid internal block hash.
    pub fn first_previous_block_hash(&self) -> Result<BlockHash, SettledRawBlockSegmentError> {
        self.index.first_previous_block_hash()
    }

    /// Returns the authenticated block hash recorded in the row index for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] if `height` is outside the
    /// segment or the row failed validation.
    pub fn block_hash_at(
        &self,
        height: BlockHeight,
    ) -> Result<BlockHash, SettledRawBlockSegmentError> {
        let entry = self.read_verified_row_entry(height)?;
        decode_wire_hash(&entry.block_hash_wire, "row.block_hash")
    }

    /// Returns the authenticated uncompressed consensus raw block digest recorded for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] if `height` is outside the
    /// segment or the row failed validation.
    pub fn uncompressed_digest_at(
        &self,
        height: BlockHeight,
    ) -> Result<SerializedBytesDigest, SettledRawBlockSegmentError> {
        let entry = self.read_verified_row_entry(height)?;
        Ok(SerializedBytesDigest::from_bytes(entry.uncompressed_sha256))
    }

    /// Reads, decompresses if needed, verifies, and returns the exact consensus
    /// serialized raw block bytes at `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on height, I/O, decompression,
    /// or integrity errors.
    pub fn read_raw_block(
        &self,
        height: BlockHeight,
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let plan = self.index.plan_block_read(height)?;
        let row = &self.index.rows[plan.row_idx];
        let raw_rows = self
            .source
            .read_vec_at(
                plan.row_index_byte_range.offset,
                plan.row_index_byte_range.len,
            )
            .inspect_err(|_| row.mark_invalid())?;
        let verified_rows = self
            .index
            .authenticate_row_index_slice(RawBlockRowIndexSlice::new(
                plan.row_index_byte_range.offset,
                &raw_rows,
            ))
            .inspect_err(|_| row.mark_invalid())?;
        let frame = self
            .source
            .read_vec_at(plan.slab_byte_range.offset, plan.slab_byte_range.len)
            .inspect_err(|_| row.mark_invalid())?;
        self.index
            .verify_and_decode_block(&plan, verified_rows, &frame)
    }

    /// Reads and verifies the containing block once, reads the transaction slice
    /// entry via positioned I/O, slices `[tx_offset..tx_offset + tx_len]`,
    /// verifies `tx_sha256`, and returns the exact consensus transaction bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on height/transaction index bounds,
    /// I/O, decompression, or SHA-256 verification errors.
    pub fn read_raw_transaction(
        &self,
        height: BlockHeight,
        tx_index: u32,
    ) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
        let plan = self.index.plan_tx_read(height, tx_index)?;
        let row = &self.index.rows[plan.block_plan.row_idx];
        let raw_txs = self
            .source
            .read_vec_at(
                plan.tx_table_byte_range.offset,
                plan.tx_table_byte_range.len,
            )
            .inspect_err(|_| row.mark_invalid())?;
        let verified_txs = self
            .index
            .authenticate_tx_table_slice(RawBlockTxTableSlice::new(
                plan.tx_table_byte_range.offset,
                &raw_txs,
            ))
            .inspect_err(|_| row.mark_invalid())?;
        let block_bytes = self.read_raw_block(height)?;
        self.index
            .verify_and_decode_tx_from_block(&plan, &block_bytes, verified_txs)
    }

    /// Reads and decompresses `height` once and extracts multiple transactions in
    /// one coalesced positioned `tx_table` read, returning them in caller order.
    ///
    /// # Errors
    ///
    /// Returns [`SettledRawBlockSegmentError`] on height/transaction index bounds,
    /// I/O, decompression, or SHA-256 verification errors.
    pub fn read_raw_transactions_in_block(
        &self,
        height: BlockHeight,
        tx_indices: &[u32],
    ) -> Result<Vec<Vec<u8>>, SettledRawBlockSegmentError> {
        let digests_plan = self.index.plan_tx_digests_read(height)?;
        let row_idx = digests_plan.row_idx;
        let row = &self.index.rows[row_idx];
        for &tx_index in tx_indices {
            if tx_index >= digests_plan.tx_count {
                return Err(SettledRawBlockSegmentError::TransactionIndexOutOfRange {
                    height,
                    tx_index,
                    tx_count: digests_plan.tx_count,
                });
            }
        }
        if tx_indices.is_empty() {
            return Ok(Vec::new());
        }
        let Some(tx_range) = digests_plan.tx_table_byte_range else {
            return Ok(Vec::new());
        };
        let raw_txs = self
            .source
            .read_vec_at(tx_range.offset, tx_range.len)
            .inspect_err(|_| row.mark_invalid())?;
        let verified_txs = self
            .index
            .authenticate_tx_table_slice(RawBlockTxTableSlice::new(tx_range.offset, &raw_txs))
            .inspect_err(|_| row.mark_invalid())?;
        let block_bytes = self.read_raw_block(height)?;
        let mut txs = Vec::with_capacity(tx_indices.len());
        for &tx_index in tx_indices {
            let tx_offset = usize::try_from(tx_index).map_err(|_| {
                SettledRawBlockSegmentError::InvalidFormat("tx index exceeds usize".to_owned())
            })?;
            let global_tx_idx = digests_plan
                .tx_start_index
                .checked_add(tx_offset)
                .ok_or_else(|| {
                    SettledRawBlockSegmentError::InvalidFormat(
                        "global tx index overflow".to_owned(),
                    )
                })?;
            let tx_entry = verified_txs
                .entry(global_tx_idx)
                .inspect_err(|_| row.mark_invalid())?;
            let tx_bytes =
                slice_and_verify_tx(&block_bytes, &tx_entry).inspect_err(|_| row.mark_invalid())?;
            txs.push(tx_bytes);
        }
        Ok(txs)
    }

    /// Iterates each transaction's zero-based index and authenticated SHA-256
    /// digest for the block at `height` via a positioned `tx_table` read without
    /// reading or decompressing `Slab 0`, returning the block's transaction count.
    pub(crate) fn visit_indexed_transaction_digests<E, F>(
        &self,
        height: BlockHeight,
        mut visit: F,
    ) -> Result<u32, E>
    where
        E: From<SettledRawBlockSegmentError>,
        F: FnMut(u32, SerializedBytesDigest) -> Result<(), E>,
    {
        let plan = self.index.plan_tx_digests_read(height)?;
        let Some(tx_range) = plan.tx_table_byte_range else {
            return Ok(0);
        };
        let row = &self.index.rows[plan.row_idx];
        let raw_txs = self
            .source
            .read_vec_at(tx_range.offset, tx_range.len)
            .map_err(SettledRawBlockSegmentError::Io)
            .inspect_err(|_| row.mark_invalid())?;
        let verified_txs = self
            .index
            .authenticate_tx_table_slice(RawBlockTxTableSlice::new(tx_range.offset, &raw_txs))
            .inspect_err(|_| row.mark_invalid())?;
        for tx_idx in 0..plan.tx_count {
            let offset_usize = usize::try_from(tx_idx).map_err(|_| {
                SettledRawBlockSegmentError::InvalidFormat(
                    "transaction index exceeds usize".to_owned(),
                )
            })?;
            let global_tx_idx = plan
                .tx_start_index
                .checked_add(offset_usize)
                .ok_or_else(|| {
                    SettledRawBlockSegmentError::InvalidFormat(
                        "global transaction index overflow".to_owned(),
                    )
                })?;
            let tx_entry = verified_txs
                .entry(global_tx_idx)
                .inspect_err(|_| row.mark_invalid())?;
            visit(tx_idx, SerializedBytesDigest::from_bytes(tx_entry.sha256))?;
        }
        Ok(plan.tx_count)
    }

    /// Reads and authenticates every block's `(BlockHash, uncompressed_digest)`
    /// pair in a single coalesced `row_index` read in ascending height order.
    pub(crate) fn read_all_row_hashes_and_digests(
        &self,
    ) -> Result<Vec<(BlockHash, SerializedBytesDigest)>, SettledRawBlockSegmentError> {
        let start = self.index.identity.start_height;
        let end = self.index.identity.end_block.height;
        let byte_range = self.index.plan_row_index_range(start, end)?;
        let raw_rows = self
            .source
            .read_vec_at(byte_range.offset, byte_range.len)
            .inspect_err(|_| {
                for row in &self.index.rows {
                    row.mark_invalid();
                }
            })?;
        let verified_rows = self
            .index
            .authenticate_row_index_slice(RawBlockRowIndexSlice::new(
                byte_range.offset,
                &raw_rows,
            ))?;
        let mut rows_meta = Vec::with_capacity(self.index.rows.len());
        for row_idx in 0..self.index.rows.len() {
            let entry = verified_rows
                .entry(row_idx)
                .inspect_err(|_| self.index.rows[row_idx].mark_invalid())?;
            let block_hash = decode_wire_hash(&entry.block_hash_wire, "row.block_hash")
                .inspect_err(|_| self.index.rows[row_idx].mark_invalid())?;
            let digest = SerializedBytesDigest::from_bytes(entry.uncompressed_sha256);
            rows_meta.push((block_hash, digest));
        }
        Ok(rows_meta)
    }

    fn read_verified_row_entry(
        &self,
        height: BlockHeight,
    ) -> Result<DecodedRowIndexEntry, SettledRawBlockSegmentError> {
        let row_idx = self.index.resolve_row_index(height)?;
        let row = &self.index.rows[row_idx];
        if row.validation_state() == VALIDATION_INVALID {
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "raw block segment row previously failed validation".to_owned(),
            ));
        }
        let byte_range = self.index.plan_row_index_range(height, height)?;
        let raw_rows = self
            .source
            .read_vec_at(byte_range.offset, byte_range.len)
            .inspect_err(|_| row.mark_invalid())?;
        let verified_rows = self
            .index
            .authenticate_row_index_slice(RawBlockRowIndexSlice::new(byte_range.offset, &raw_rows))
            .inspect_err(|_| row.mark_invalid())?;
        verified_rows
            .entry(row_idx)
            .inspect_err(|_| row.mark_invalid())
    }
}

fn verify_index_digest(
    header_bytes: &[u8; HEADER_LEN],
    raw_tx_table: &[u8],
    raw_row_index: &[u8],
    trailer: &[u8],
    expected_index_sha256: &[u8; 32],
) -> Result<(), SettledRawBlockSegmentError> {
    let mut hasher = Sha256::new();
    hasher.update(header_bytes);
    hasher.update(raw_tx_table);
    hasher.update(raw_row_index);
    hasher.update(trailer);
    let computed: [u8; 32] = hasher.finalize().into();
    if &computed != expected_index_sha256 {
        return Err(SettledRawBlockSegmentError::PayloadIntegrity(
            "raw block segment index_sha256 pin mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn compute_page_digests(raw_bytes: &[u8], page_bytes: usize) -> Box<[[u8; 32]]> {
    raw_bytes
        .chunks(page_bytes)
        .map(|chunk| Sha256::digest(chunk).into())
        .collect::<Vec<[u8; 32]>>()
        .into_boxed_slice()
}

fn decode_and_validate_rows(
    raw_row_index: &[u8],
    raw_tx_table: &[u8],
    slab_len: u32,
    header: &RawBlockSegmentHeader,
) -> Result<Box<[CompactRawBlockRow]>, SettledRawBlockSegmentError> {
    let block_count = usize::try_from(header.block_count).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("block count exceeds usize".to_owned())
    })?;
    let tx_count_total = header.tx_entry_count()?;
    if raw_row_index.len() != block_count * ROW_INDEX_ENTRY_LEN
        || raw_tx_table.len() != tx_count_total * TX_SLICE_ENTRY_LEN
    {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "raw block index length disagrees with header counts".to_owned(),
        ));
    }
    if slab_len <= FRAME_PREFIX_U32 {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "raw block slab is shorter than one frame prefix".to_owned(),
        ));
    }
    let codec = header.codec()?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(block_count).map_err(|err| {
        SettledRawBlockSegmentError::InvalidFormat(format!("compact rows allocation failed: {err}"))
    })?;
    let mut expected_tx_start = 0_u32;
    let mut prev_offset: Option<u32> = None;
    for idx in 0..block_count {
        let row_bytes = raw_row_index_slice_at(raw_row_index, idx)?;
        let entry = DecodedRowIndexEntry::decode(row_bytes)?;
        if idx == 0 && entry.slab_rel_offset != 0 {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "row index must start at slab offset 0".to_owned(),
            ));
        }
        if idx + 1 == block_count && entry.block_hash_wire != header.last_block_hash_wire {
            return Err(SettledRawBlockSegmentError::ChainLinkage(
                "last row hash disagrees with header last_block_hash".to_owned(),
            ));
        }
        if let Some(prev) = prev_offset
            && entry.slab_rel_offset <= prev
        {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "non-ascending row slab offsets".to_owned(),
            ));
        }
        let next_rel = if idx + 1 < block_count {
            read_row_slab_offset_at(raw_row_index, idx + 1)?
        } else {
            slab_len
        };
        let frame_len = next_rel.checked_sub(entry.slab_rel_offset).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("non-ascending row slab offsets".to_owned())
        })?;
        validate_entry_bounds(&entry, frame_len, codec)?;
        expected_tx_start = validate_entry_tx_slices(&entry, expected_tx_start, raw_tx_table)?;
        prev_offset = Some(entry.slab_rel_offset);
        rows.push(CompactRawBlockRow::from_entry(&entry));
    }
    if usize::try_from(expected_tx_start).ok() != Some(tx_count_total) {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "row index transaction counts do not match tx table length".to_owned(),
        ));
    }
    Ok(rows.into_boxed_slice())
}

fn raw_row_index_slice_at(
    raw_row_index: &[u8],
    idx: usize,
) -> Result<&[u8; ROW_INDEX_ENTRY_LEN], SettledRawBlockSegmentError> {
    let start = idx * ROW_INDEX_ENTRY_LEN;
    raw_row_index
        .get(start..start + ROW_INDEX_ENTRY_LEN)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("row index slice out of bounds".to_owned())
        })
}

fn read_row_slab_offset_at(
    raw_row_index: &[u8],
    idx: usize,
) -> Result<u32, SettledRawBlockSegmentError> {
    let row_bytes = raw_row_index_slice_at(raw_row_index, idx)?;
    let bytes: [u8; 4] = row_bytes[96..100].try_into().map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("truncated slab_rel_offset".to_owned())
    })?;
    Ok(u32::from_be_bytes(bytes))
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
    raw_tx_table: &[u8],
) -> Result<u32, SettledRawBlockSegmentError> {
    if entry.tx_start_index != expected_tx_start {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "non-contiguous transaction slice start index".to_owned(),
        ));
    }
    if u64::from(entry.tx_count) > MAX_TX_ENTRIES_PER_BLOCK {
        return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
            "block transaction count {} exceeds maximum {MAX_TX_ENTRIES_PER_BLOCK}",
            entry.tx_count
        )));
    }
    let tx_end = entry
        .tx_start_index
        .checked_add(entry.tx_count)
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "transaction slice range overflows u32".to_owned(),
            )
        })?;
    let byte_start = usize::try_from(entry.tx_start_index)
        .ok()
        .and_then(|start| start.checked_mul(TX_SLICE_ENTRY_LEN))
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("tx table byte start overflow".to_owned())
        })?;
    let byte_end = usize::try_from(tx_end)
        .ok()
        .and_then(|end| end.checked_mul(TX_SLICE_ENTRY_LEN))
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("tx table byte end overflow".to_owned())
        })?;
    let slice_bytes = raw_tx_table.get(byte_start..byte_end).ok_or_else(|| {
        SettledRawBlockSegmentError::InvalidFormat(
            "row transaction slice range exceeds tx table".to_owned(),
        )
    })?;
    for fixed in slice_bytes.as_chunks::<TX_SLICE_ENTRY_LEN>().0 {
        let tx_slice = TxSliceEntry::decode(fixed)?;
        let Some(slice_end) = tx_slice.offset.checked_add(tx_slice.len) else {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "transaction slice offset plus length overflows u32".to_owned(),
            ));
        };
        if slice_end > entry.uncompressed_len {
            return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
                "transaction slice [{}..{slice_end}] exceeds uncompressed block length {}",
                tx_slice.offset, entry.uncompressed_len
            )));
        }
    }
    Ok(tx_end)
}

fn verify_all_slab_frames(
    source: &impl PositionalByteSource,
    index: &SettledRawBlockSegmentIndex,
    raw_row_index: &[u8],
    whole_digest: &mut Sha256,
) -> Result<(), SettledRawBlockSegmentError> {
    let total_slab = u64::from(index.slab_len);
    let mut cursor = 0_u64;
    let mut idx = 0_usize;
    while idx < index.rows.len() {
        let batch_end = advance_slab_verify_batch(index, idx, STAGING_CHUNK);
        let batch_start_rel = u64::from(index.rows[idx].slab_rel_offset);
        let batch_end_rel = if batch_end < index.rows.len() {
            u64::from(index.rows[batch_end].slab_rel_offset)
        } else {
            total_slab
        };
        let batch_len = usize::try_from(batch_end_rel - batch_start_rel).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("slab batch exceeds usize".to_owned())
        })?;
        let batch_bytes = source
            .read_vec_at(index.slab_offset + batch_start_rel, batch_len)
            .inspect_err(|_| mark_row_slice_invalid(&index.rows[idx..batch_end]))?;
        whole_digest.update(&batch_bytes);
        verify_slab_batch_frames(index, raw_row_index, idx, batch_end, &batch_bytes)?;
        cursor += u64::try_from(batch_bytes.len()).unwrap_or(0);
        idx = batch_end;
    }
    if cursor != total_slab {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "streamed slab bytes do not fill header slab length".to_owned(),
        ));
    }
    Ok(())
}

fn advance_slab_verify_batch(
    index: &SettledRawBlockSegmentIndex,
    start_idx: usize,
    max_batch_bytes: usize,
) -> usize {
    let base_rel = index.rows[start_idx].slab_rel_offset;
    let mut end_idx = start_idx + 1;
    while end_idx < index.rows.len() {
        let next_end_rel = if end_idx + 1 < index.rows.len() {
            index.rows[end_idx + 1].slab_rel_offset
        } else {
            index.slab_len
        };
        if usize::try_from(next_end_rel - base_rel).is_ok_and(|span| span <= max_batch_bytes) {
            end_idx += 1;
        } else {
            break;
        }
    }
    end_idx
}

fn verify_slab_batch_frames(
    index: &SettledRawBlockSegmentIndex,
    raw_row_index: &[u8],
    batch_start: usize,
    batch_end: usize,
    batch_bytes: &[u8],
) -> Result<(), SettledRawBlockSegmentError> {
    let base_rel = index.rows[batch_start].slab_rel_offset;
    for row_idx in batch_start..batch_end {
        let row = &index.rows[row_idx];
        let (rel_offset, frame_len) = compact_frame_span(&index.rows, index.slab_len, row_idx)?;
        let local_start = usize::try_from(rel_offset - base_rel).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("batch local offset overflow".to_owned())
        })?;
        let local_end = local_start + usize::try_from(frame_len).unwrap_or(0);
        let frame = batch_bytes.get(local_start..local_end).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("short slab verify batch".to_owned())
        })?;
        let stored_len = decode_frame_prefix(frame, index.identity.codec)?;
        if FRAME_PREFIX_LEN + stored_len != frame.len() {
            row.mark_invalid();
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "streamed slab frame length disagrees with row index".to_owned(),
            ));
        }
        let row_entry =
            DecodedRowIndexEntry::decode(raw_row_index_slice_at(raw_row_index, row_idx)?)?;
        let computed: [u8; 32] = Sha256::digest(&frame[FRAME_PREFIX_LEN..]).into();
        if computed != row_entry.payload_sha256 {
            row.mark_invalid();
            return Err(SettledRawBlockSegmentError::PayloadIntegrity(
                "streamed slab frame digest disagrees with row index".to_owned(),
            ));
        }
    }
    Ok(())
}

fn mark_row_slice_invalid(rows: &[CompactRawBlockRow]) {
    for row in rows {
        row.mark_invalid();
    }
}

fn decode_and_verify_frame(
    row_entry: &DecodedRowIndexEntry,
    frame: &[u8],
    codec: RawBlockCodec,
    verify_digests: bool,
) -> Result<Vec<u8>, SettledRawBlockSegmentError> {
    let stored_len = decode_frame_prefix(frame, codec)?;
    if FRAME_PREFIX_LEN + stored_len != frame.len() {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "raw block frame prefix length disagrees with index".to_owned(),
        ));
    }
    let payload = &frame[FRAME_PREFIX_LEN..];
    if verify_digests && <[u8; 32]>::from(Sha256::digest(payload)) != row_entry.payload_sha256 {
        return Err(SettledRawBlockSegmentError::PayloadIntegrity(
            "raw block payload SHA-256 mismatch on read".to_owned(),
        ));
    }
    let expected_len = usize::try_from(row_entry.uncompressed_len).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("uncompressed length exceeds usize".to_owned())
    })?;
    let raw_block = decode_payload(payload, expected_len, codec)?;
    if verify_digests
        && <[u8; 32]>::from(Sha256::digest(&raw_block)) != row_entry.uncompressed_sha256
    {
        return Err(SettledRawBlockSegmentError::PayloadIntegrity(
            "raw block uncompressed SHA-256 mismatch on read".to_owned(),
        ));
    }
    Ok(raw_block)
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

fn decode_wire_hash(
    bytes: &[u8; 32],
    label: &str,
) -> Result<BlockHash, SettledRawBlockSegmentError> {
    decode_internal_block_hash(bytes).map_err(|err| {
        SettledRawBlockSegmentError::PayloadIntegrity(format!("invalid {label}: {err}"))
    })
}
