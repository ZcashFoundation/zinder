//! Admission, sans-I/O range planning, and streaming reader for `ZSHRNG02`.

use std::{
    fs::File,
    io,
    ops::Range,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, wire::decode_internal_block_hash,
};
use zinder_proto::compat::lightwalletd::{EncodedBlock, EncodedBlockBatcher};

use crate::{
    HeightOrder,
    positional_source::{
        ByteRange, DiscardScanPages, PositionalByteSource, ScanDropBehind, bounded_file_scan,
    },
    segment_verity,
};

use super::{
    PoolPresenceMask, SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES, SettledShieldedSegmentError,
    ShieldedProjectionSelection, ShieldedSegmentHeader, ShieldedSegmentIdentity,
    format::{
        CompactShieldedRow, DecodedRowIndexEntry, FRAME_PREFIX_LEN, FRAME_PREFIX_U32, HEADER_LEN,
        INDEX_PAGE_ROWS, LARGE_ROW_ISOLATION_MIN_BYTES, REPACKED_SHORT_BATCH_BYTES,
        ROW_INDEX_ENTRY_LEN, SLAB_TARGET_BYTES, STAGING_CHUNK, TRAILER_LEN, VALIDATION_INVALID,
        VALIDATION_VALID, decode_frame_prefix, frame_span, read_hash32, read_u32_be,
    },
    wire_projection::{
        ExpectedShieldedBlock, project_shielded_block_wire_with_scratch,
        synthesize_empty_nullifier_block, synthesize_empty_nullifier_payload,
        validate_shielded_block_wire,
    },
};

/// Admission options when opening a [`SettledShieldedSegmentReader`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SettledShieldedSegmentOpenOptions {
    /// Fail admission if Linux `fs-verity` is not active and verified on the file.
    pub require_fs_verity: bool,
    /// Verify every block payload digest, wire envelope, and whole-body SHA-256
    /// eagerly during open instead of deferring slab verification to read time.
    pub verify_full_payload: bool,
}

/// Reusable scratch buffers for sans-I/O and buffered `ZSHRNG02` projection.
#[derive(Debug, Default)]
pub struct ShieldedReadScratch {
    projected_frame: Vec<u8>,
    tx_scratch: Vec<u8>,
}

/// On-disk slice of raw `80`-byte `ZSHRNG02` row index entries paired with its
/// byte offset inside the segment.
#[derive(Clone, Copy, Debug)]
pub struct ShieldedIndexSlice<'a> {
    /// Segment byte offset at which `bytes[0]` starts.
    pub base_offset: u64,
    /// Raw on-disk row index bytes covering at least the requested rows.
    pub bytes: &'a [u8],
}

impl<'a> ShieldedIndexSlice<'a> {
    /// Creates a positioned slice over raw on-disk row index entries.
    #[must_use]
    pub const fn new(base_offset: u64, bytes: &'a [u8]) -> Self {
        Self { base_offset, bytes }
    }
}

/// Authenticated view over raw on-disk `ZSHRNG02` row index entries.
///
/// For non-`fs-verity` segments, constructing this value verifies every covered
/// `64`-row page once against the segment's in-memory `index_page_digests`.
#[derive(Clone, Copy, Debug)]
pub struct VerifiedShieldedIndexSlice<'a> {
    base_row: usize,
    bytes: &'a [u8],
}

impl VerifiedShieldedIndexSlice<'_> {
    fn entry(&self, row_idx: usize) -> Result<DecodedRowIndexEntry, SettledShieldedSegmentError> {
        let local_idx = row_idx.checked_sub(self.base_row).ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded index slice precedes requested row".to_owned(),
            )
        })?;
        let byte_start = local_idx.checked_mul(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat("shielded index offset overflow".to_owned())
        })?;
        let byte_end = byte_start.checked_add(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat("shielded index offset overflow".to_owned())
        })?;
        let raw_slice = self.bytes.get(byte_start..byte_end).ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded index slice does not cover requested row".to_owned(),
            )
        })?;
        let fixed: &[u8; ROW_INDEX_ENTRY_LEN] = raw_slice.try_into().map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("short shielded index entry".to_owned())
        })?;
        DecodedRowIndexEntry::decode(fixed)
    }

    fn block_hash_wire(&self, row_idx: usize) -> Result<[u8; 32], SettledShieldedSegmentError> {
        Ok(self.entry(row_idx)?.block_hash_wire)
    }
}

/// Fetched byte slices supplying one [`ShieldedReadStep`] to sans-I/O decoding.
#[derive(Clone, Debug, Default)]
pub struct ShieldedFetchedSlab<'a> {
    /// Raw `Slab 0` bytes for `step.slab_byte_range()`, if any.
    pub slab_bytes: Option<Vec<u8>>,
    /// Authenticated on-disk index view from [`SettledShieldedSegmentIndex::authenticate_index_slice`], if needed.
    pub verified_index: Option<VerifiedShieldedIndexSlice<'a>>,
}

impl<'a> ShieldedFetchedSlab<'a> {
    /// Bundles an optional fetched slab buffer and optional authenticated index view.
    #[must_use]
    pub const fn new(
        slab_bytes: Option<Vec<u8>>,
        verified_index: Option<VerifiedShieldedIndexSlice<'a>>,
    ) -> Self {
        Self {
            slab_bytes,
            verified_index,
        }
    }
}

/// Sans-I/O request for verifying and streaming one [`ShieldedReadStep`] into
/// an [`EncodedBlockBatcher`].
pub struct ShieldedSlabDecodeRequest<'a> {
    /// Planned step to decode.
    pub step: &'a ShieldedReadStep,
    /// Fetched slab and index bytes matching `step`.
    pub fetched: ShieldedFetchedSlab<'a>,
    /// Shielded pool and redaction projection to emit.
    pub selection: ShieldedProjectionSelection,
    /// Output emission order across the rows in `step`.
    pub order: HeightOrder,
}

/// Byte budget and oversized-first-block policy for bounded `ZSHRNG02` plans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShieldedRangeByteBudget {
    /// Maximum cumulative `native_bytes` across selected blocks.
    pub max_bytes: usize,
    /// Permit the first block even if its `native_bytes` exceeds `max_bytes`.
    pub allow_oversized_first: bool,
    /// Reject rows whose omitted wide transaction fee exceeds wire `u32`.
    pub enforce_wire_fee_limit: bool,
}

/// Request parameters for streaming a `ZSHRNG02` height range into [`EncodedBlockBatcher`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShieldedBatchReadRequest {
    /// Inclusive height range inside the segment.
    pub range: BlockHeightRange,
    /// Iteration order across `range`.
    pub order: HeightOrder,
    /// Shielded pool and redaction projection.
    pub selection: ShieldedProjectionSelection,
    /// Reject rows whose omitted wide transaction fee exceeds wire `u32`.
    pub enforce_wire_fee_limit: bool,
}

/// Request parameters for bounded `ZSHRNG02` streaming into [`EncodedBlockBatcher`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShieldedBoundedBatchReadRequest {
    /// Inclusive candidate height range inside the segment.
    pub range: BlockHeightRange,
    /// Iteration order across `range`.
    pub order: HeightOrder,
    /// Native byte budget and fee policy.
    pub budget: ShieldedRangeByteBudget,
    /// Shielded pool and redaction projection.
    pub selection: ShieldedProjectionSelection,
}

/// Bounded range summary paired with its sans-I/O [`ShieldedRangePlan`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedShieldedRangePlan {
    /// Lowest block height selected within the byte budget.
    pub lowest_height: BlockHeight,
    /// Highest block height selected within the byte budget.
    pub highest_height: BlockHeight,
    /// Number of selected blocks.
    pub kept_blocks: usize,
    /// Cumulative `native_bytes` of the selected blocks.
    pub kept_native_bytes: usize,
    /// Sans-I/O fetch plan covering `[lowest_height, highest_height]`.
    pub plan: ShieldedRangePlan,
}

/// Execution options for running an already-planned [`BoundedShieldedRangePlan`].
#[derive(Clone, Copy, Debug)]
pub struct ShieldedPlanExecution<'a> {
    /// Iteration order across the planned range.
    pub order: HeightOrder,
    /// Shielded pool and redaction projection.
    pub selection: ShieldedProjectionSelection,
    /// Optional cooperative cancellation flag polled between read steps.
    pub cancel: Option<&'a AtomicBool>,
}

/// One sans-I/O fetch and decode unit produced by [`SettledShieldedSegmentIndex::plan_block_range`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShieldedReadStep {
    row_range: Range<usize>,
    slab_byte_range: Option<ByteRange>,
    index_byte_range: Option<ByteRange>,
    isolated_large_row: bool,
}

impl ShieldedReadStep {
    /// Segment-relative zero-based row indices covered by this step.
    #[must_use]
    pub const fn row_range(&self) -> Range<usize> {
        self.row_range.start..self.row_range.end
    }

    /// Positional byte range in `Slab 0`, or `None` when empty/unmatched rows skip slab I/O.
    #[must_use]
    pub const fn slab_byte_range(&self) -> Option<ByteRange> {
        self.slab_byte_range
    }

    /// Positional byte range in the `80`-byte row index needed for verification or synthesis.
    #[must_use]
    pub const fn index_byte_range(&self) -> Option<ByteRange> {
        self.index_byte_range
    }

    /// Returns whether this step reads a single row of at least `32 KiB` (`EncodedBlock::LARGE_MESSAGE_MIN_BYTES`).
    #[must_use]
    pub const fn is_isolated_large_row(&self) -> bool {
        self.isolated_large_row
    }

    /// Returns whether this step synthesizes empty nullifier blocks without reading `Slab 0`.
    #[must_use]
    pub const fn is_synthesized_empty(&self) -> bool {
        self.slab_byte_range.is_none()
    }
}

/// Ordered sequence of sans-I/O read steps for one `ZSHRNG02` range query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShieldedRangePlan {
    steps: Vec<ShieldedReadStep>,
    needed_index_byte_range: Option<ByteRange>,
    block_count: usize,
}

impl ShieldedRangePlan {
    /// Ordered ascending-height steps covering the requested block range.
    #[must_use]
    pub fn steps(&self) -> &[ShieldedReadStep] {
        &self.steps
    }

    /// Single coalesced byte range covering every step's `index_byte_range()`, if any.
    #[must_use]
    pub const fn needed_index_byte_range(&self) -> Option<ByteRange> {
        self.needed_index_byte_range
    }

    /// Total number of blocks spanned by this plan.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.block_count
    }
}

/// Compact in-memory `ZSHRNG02` index (`12 B/block` `fs-verity`, `12.5 B/block` portable)
/// supporting sans-I/O range planning and verification.
pub struct SettledShieldedSegmentIndex {
    identity: ShieldedSegmentIdentity,
    kernel_verified: bool,
    slab_offset: u64,
    slab_len: u32,
    index_offset: u64,
    first_prev_hash_wire: [u8; 32],
    rows: Box<[CompactShieldedRow]>,
    index_page_digests: Option<Box<[[u8; 32]]>>,
}

impl SettledShieldedSegmentIndex {
    /// Authenticates and decodes a `ZSHRNG02` header, raw `80`-byte row index, and
    /// trailing `32`-byte SHA-256 trailer in `O(index)` without reading `Slab 0`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on malformed fields, `sha256` or
    /// `index_sha256` pin mismatch, or invalid row bounds/tiling.
    pub fn decode_index(
        header_bytes: &[u8; HEADER_LEN],
        index_and_trailer: &[u8],
        expected: &ShieldedSegmentIdentity,
        total_len: u64,
        kernel_verified: bool,
    ) -> Result<Self, SettledShieldedSegmentError> {
        let header = ShieldedSegmentHeader::decode(header_bytes, expected, total_len)?;
        let expected_range = header.index_and_trailer_range()?;
        if index_and_trailer.len() != expected_range.len {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded index and trailer slice length disagrees with header".to_owned(),
            ));
        }
        let raw_index_len = expected_range.len - TRAILER_LEN;
        let (raw_index, trailer) = index_and_trailer.split_at(raw_index_len);
        if trailer != expected.sha256 {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "shielded segment trailer SHA-256 disagrees with pinned identity".to_owned(),
            ));
        }
        verify_index_digest(header_bytes, raw_index, trailer, &expected.index_sha256)?;
        let slab_len = header.slab_len()?;
        let rows = decode_and_validate_rows(raw_index, slab_len, &header)?;
        let index_page_digests = (!kernel_verified).then(|| compute_index_page_digests(raw_index));
        Ok(Self {
            identity: *expected,
            kernel_verified,
            slab_offset: header.slab_offset,
            slab_len,
            index_offset: header.index_offset,
            first_prev_hash_wire: header.first_prev_hash_wire,
            rows,
            index_page_digests,
        })
    }

    /// Returns whether this index was admitted under kernel `fs-verity` protection.
    #[must_use]
    pub const fn kernel_verified(&self) -> bool {
        self.kernel_verified
    }

    /// Returns the authenticated segment identity.
    #[must_use]
    pub const fn identity(&self) -> ShieldedSegmentIdentity {
        self.identity
    }

    /// Returns the number of contiguous blocks covered by this segment.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.rows.len()
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

    /// Returns the byte offset where the on-disk row index begins.
    #[must_use]
    pub const fn index_offset(&self) -> u64 {
        self.index_offset
    }

    /// Returns the previous block hash in wire order declared before the first block.
    #[must_use]
    pub const fn first_prev_hash_wire(&self) -> &[u8; 32] {
        &self.first_prev_hash_wire
    }

    /// Returns the heap bytes pinned by compact in-memory row tables and optional
    /// portable page digests (`~12 B/block` under `fs-verity`, `~12.5 B/block` portable).
    #[must_use]
    pub fn heap_metadata_bytes(&self) -> usize {
        let rows_bytes = self.rows.len() * std::mem::size_of::<CompactShieldedRow>();
        let page_bytes = self
            .index_page_digests
            .as_ref()
            .map_or(0, |pages| pages.len() * 32);
        rows_bytes + page_bytes
    }

    /// Returns the uncompressed canonical byte budget recorded in memory for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::HeightOutOfRange`] if `height` is
    /// outside this segment.
    pub fn native_bytes_at(&self, height: BlockHeight) -> Result<u32, SettledShieldedSegmentError> {
        let range = self.resolve_index_range(BlockHeightRange::inclusive(height, height))?;
        Ok(self.rows[range.start].native_bytes)
    }

    /// Returns the positional [`ByteRange`] needed to read `height`'s block hash
    /// (verified 64-row page when `!kernel_verified`, or 32 bytes when `kernel_verified`).
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] if `height` is out of range or
    /// its row has previously entered sticky invalid state.
    pub fn block_hash_byte_range(
        &self,
        height: BlockHeight,
    ) -> Result<ByteRange, SettledShieldedSegmentError> {
        let range = self.resolve_index_range(BlockHeightRange::inclusive(height, height))?;
        let row_idx = range.start;
        if self.rows[row_idx].validation_state() == VALIDATION_INVALID {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "shielded segment frame previously failed validation".to_owned(),
            ));
        }
        if !self.kernel_verified {
            let (_base, byte_range) = self.aligned_page_range_for_rows(row_idx, row_idx + 1)?;
            return Ok(byte_range);
        }
        let byte_offset = self
            .index_offset
            .checked_add((row_idx as u64) * ROW_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat("index offset overflow".to_owned())
            })?;
        Ok(ByteRange::new(byte_offset, 32))
    }

    /// Decodes `height`'s authenticated [`BlockHash`] from a fetched index slice.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] if `height` is invalid, `slice`
    /// fails bounds or page-digest verification, or the stored hash is non-canonical.
    pub fn decode_block_hash_from_slice(
        &self,
        height: BlockHeight,
        slice: ShieldedIndexSlice<'_>,
    ) -> Result<BlockHash, SettledShieldedSegmentError> {
        let range = self.resolve_index_range(BlockHeightRange::inclusive(height, height))?;
        let row_idx = range.start;
        if self.rows[row_idx].validation_state() == VALIDATION_INVALID {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "shielded segment frame previously failed validation".to_owned(),
            ));
        }
        if !self.kernel_verified {
            let verified = self.authenticate_index_slice(slice)?;
            return decode_wire_hash(&verified.block_hash_wire(row_idx)?, "row.block_hash");
        }
        let expected_offset = self
            .index_offset
            .checked_add((row_idx as u64) * ROW_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat("index offset overflow".to_owned())
            })?;
        let local_start = expected_offset
            .checked_sub(slice.base_offset)
            .and_then(|delta| usize::try_from(delta).ok())
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat(
                    "index slice does not cover block hash".to_owned(),
                )
            })?;
        let hash_bytes = slice
            .bytes
            .get(local_start..local_start + 32)
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat("short block hash slice".to_owned())
            })?;
        let wire: &[u8; 32] = hash_bytes.try_into().map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("short block hash bytes".to_owned())
        })?;
        decode_wire_hash(wire, "row.block_hash")
    }

    /// Authenticates raw index bytes covering full `64`-row pages (when
    /// `!kernel_verified`) or arbitrary row entries (when `kernel_verified`).
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::InvalidFormat`] on misaligned bounds
    /// or [`SettledShieldedSegmentError::PayloadIntegrity`] if any non-`fs-verity`
    /// index page digest disagrees, latching `VALIDATION_INVALID` on that page's rows.
    pub fn authenticate_index_slice<'a>(
        &self,
        slice: ShieldedIndexSlice<'a>,
    ) -> Result<VerifiedShieldedIndexSlice<'a>, SettledShieldedSegmentError> {
        let rel = slice
            .base_offset
            .checked_sub(self.index_offset)
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat(
                    "shielded index slice begins before index_offset".to_owned(),
                )
            })?;
        if !rel.is_multiple_of(ROW_INDEX_ENTRY_LEN as u64)
            || !slice.bytes.len().is_multiple_of(ROW_INDEX_ENTRY_LEN)
        {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded index slice is not aligned to 80-byte entries".to_owned(),
            ));
        }
        let start_row = usize::try_from(rel / ROW_INDEX_ENTRY_LEN as u64).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("index row offset exceeds usize".to_owned())
        })?;
        let row_count = slice.bytes.len() / ROW_INDEX_ENTRY_LEN;
        let end_row = start_row.checked_add(row_count).ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat("index row end exceeds usize".to_owned())
        })?;
        if end_row > self.rows.len() {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded index slice extends past segment block_count".to_owned(),
            ));
        }
        if let Some(digests) = &self.index_page_digests {
            self.verify_portable_index_pages(start_row, end_row, slice.bytes, digests)?;
        }
        Ok(VerifiedShieldedIndexSlice {
            base_row: start_row,
            bytes: slice.bytes,
        })
    }

    fn verify_portable_index_pages(
        &self,
        start_row: usize,
        end_row: usize,
        raw_slice: &[u8],
        digests: &[[u8; 32]],
    ) -> Result<(), SettledShieldedSegmentError> {
        if start_row == end_row {
            return Ok(());
        }
        if !start_row.is_multiple_of(INDEX_PAGE_ROWS)
            || (end_row != self.rows.len() && !end_row.is_multiple_of(INDEX_PAGE_ROWS))
        {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "portable shielded index slice must cover full 64-row pages".to_owned(),
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
                return Err(SettledShieldedSegmentError::PayloadIntegrity(format!(
                    "portable shielded index page {page_idx} SHA-256 mismatch"
                )));
            }
        }
        Ok(())
    }

    /// Plans sans-I/O positional fetches (`512 KiB` short-row slabs, `>= 32 KiB`
    /// isolated large rows, and skipped `Slab 0` spans for unmatched nullifier rows)
    /// across `range`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] if `range` is out of bounds,
    /// contains a row with an unrepresentable wide fee, or hits a sticky-invalid row.
    pub fn plan_block_range(
        &self,
        range: BlockHeightRange,
        selection: ShieldedProjectionSelection,
    ) -> Result<ShieldedRangePlan, SettledShieldedSegmentError> {
        self.plan_block_range_with_wire_fee_check(range, selection, true)
    }

    /// Plans sans-I/O fetches across `range`, optionally permitting omitted wide-fee
    /// rows for `ZSTSUP01` canonical reconstruction.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on out-of-range heights, sticky
    /// invalid rows, or disallowed wide-fee rows.
    pub fn plan_block_range_with_wire_fee_check(
        &self,
        range: BlockHeightRange,
        selection: ShieldedProjectionSelection,
        enforce_wire_fee_limit: bool,
    ) -> Result<ShieldedRangePlan, SettledShieldedSegmentError> {
        let index_range = self.resolve_index_range(range)?;
        self.check_range_preconditions(index_range.clone(), enforce_wire_fee_limit)?;
        self.build_range_plan(index_range, selection)
    }

    /// Plans a bounded prefix (when `Ascending`) or suffix (when `Descending`) of
    /// `range` within `budget.max_bytes` using in-memory `native_bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on out-of-bounds heights, sticky
    /// invalid rows, or disallowed wide-fee rows.
    pub fn plan_bounded_block_range(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        budget: ShieldedRangeByteBudget,
        selection: ShieldedProjectionSelection,
    ) -> Result<Option<BoundedShieldedRangePlan>, SettledShieldedSegmentError> {
        let index_range = self.resolve_index_range(range)?;
        let Some((selected, kept_native_bytes)) = select_bounded_rows(
            &self.rows,
            index_range,
            order,
            budget.max_bytes,
            budget.allow_oversized_first,
        )?
        else {
            return Ok(None);
        };
        self.check_range_preconditions(selected.clone(), budget.enforce_wire_fee_limit)?;
        let lowest_height = self.height_at(selected.start)?;
        let highest_height = self.height_at(selected.end - 1)?;
        let plan = self.build_range_plan(selected.clone(), selection)?;
        Ok(Some(BoundedShieldedRangePlan {
            lowest_height,
            highest_height,
            kept_blocks: selected.len(),
            kept_native_bytes,
            plan,
        }))
    }

    fn check_range_preconditions(
        &self,
        index_range: Range<usize>,
        enforce_wire_fee_limit: bool,
    ) -> Result<(), SettledShieldedSegmentError> {
        for idx in index_range {
            let row = &self.rows[idx];
            if row.validation_state() == VALIDATION_INVALID {
                return Err(SettledShieldedSegmentError::PayloadIntegrity(
                    "shielded segment frame previously failed validation".to_owned(),
                ));
            }
            if enforce_wire_fee_limit && row.has_unrepresentable_wire_fee() {
                return Err(SettledShieldedSegmentError::PayloadIntegrity(
                    "shielded block contains transaction fee exceeding u32 wire limit".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn build_range_plan(
        &self,
        index_range: Range<usize>,
        selection: ShieldedProjectionSelection,
    ) -> Result<ShieldedRangePlan, SettledShieldedSegmentError> {
        let block_count = index_range.len();
        let mut steps = Vec::new();
        let mut idx = index_range.start;
        while idx < index_range.end {
            if can_skip_slab(&self.rows[idx], selection) {
                let run_end = find_skip_run_end(&self.rows, idx, index_range.end, selection);
                let index_byte_range = Some(self.step_index_byte_range(idx..run_end)?);
                steps.push(ShieldedReadStep {
                    row_range: idx..run_end,
                    slab_byte_range: None,
                    index_byte_range,
                    isolated_large_row: false,
                });
                idx = run_end;
                continue;
            }
            let span_end = find_slab_span_end(&self.rows, idx, index_range.end, selection);
            self.append_slab_steps(idx..span_end, &mut steps)?;
            idx = span_end;
        }
        let needed_index_byte_range = self.span_needed_index_range(&steps)?;
        Ok(ShieldedRangePlan {
            steps,
            needed_index_byte_range,
            block_count,
        })
    }

    fn append_slab_steps(
        &self,
        span: Range<usize>,
        steps: &mut Vec<ShieldedReadStep>,
    ) -> Result<(), SettledShieldedSegmentError> {
        let mut batch_start = span.start;
        let mut batch_bytes = 0_usize;
        for idx in span.clone() {
            let (_, frame_len_u32) = frame_span(&self.rows, self.slab_len, idx)?;
            let frame_len = usize::try_from(frame_len_u32).map_err(|_| {
                SettledShieldedSegmentError::InvalidFormat("frame length exceeds usize".to_owned())
            })?;
            if frame_len >= LARGE_ROW_ISOLATION_MIN_BYTES {
                if batch_bytes != 0 {
                    steps.push(self.make_slab_step(batch_start..idx, false)?);
                    batch_bytes = 0;
                }
                steps.push(self.make_slab_step(idx..idx + 1, true)?);
                batch_start = idx + 1;
                continue;
            }
            if batch_bytes != 0 && batch_bytes + frame_len > SLAB_TARGET_BYTES {
                steps.push(self.make_slab_step(batch_start..idx, false)?);
                batch_start = idx;
                batch_bytes = 0;
            }
            batch_bytes += frame_len;
        }
        if batch_bytes != 0 {
            steps.push(self.make_slab_step(batch_start..span.end, false)?);
        }
        Ok(())
    }

    fn make_slab_step(
        &self,
        row_range: Range<usize>,
        isolated_large_row: bool,
    ) -> Result<ShieldedReadStep, SettledShieldedSegmentError> {
        let first_rel = self.rows[row_range.start].slab_rel_offset;
        let (last_rel, last_len) = frame_span(&self.rows, self.slab_len, row_range.end - 1)?;
        let total_len = usize::try_from((last_rel + last_len) - first_rel).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("slab chunk length exceeds usize".to_owned())
        })?;
        let offset = self
            .slab_offset
            .checked_add(u64::from(first_rel))
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat("slab byte offset overflow".to_owned())
            })?;
        let any_unvalidated = self.rows[row_range.clone()]
            .iter()
            .any(|row| row.validation_state() != VALIDATION_VALID);
        let index_byte_range = if !self.kernel_verified || any_unvalidated {
            Some(self.step_index_byte_range(row_range.clone())?)
        } else {
            None
        };
        Ok(ShieldedReadStep {
            row_range,
            slab_byte_range: Some(ByteRange::new(offset, total_len)),
            index_byte_range,
            isolated_large_row,
        })
    }

    fn step_index_byte_range(
        &self,
        row_range: Range<usize>,
    ) -> Result<ByteRange, SettledShieldedSegmentError> {
        let needed_start = row_range.start.saturating_sub(1);
        let needed_end = row_range.end;
        if !self.kernel_verified {
            let (_aligned_start, range) =
                self.aligned_page_range_for_rows(needed_start, needed_end)?;
            return Ok(range);
        }
        self.exact_index_byte_range(needed_start, needed_end)
    }

    fn span_needed_index_range(
        &self,
        steps: &[ShieldedReadStep],
    ) -> Result<Option<ByteRange>, SettledShieldedSegmentError> {
        let mut min_row: Option<usize> = None;
        let mut max_row_end: usize = 0;
        for step in steps {
            if step.index_byte_range.is_none() {
                continue;
            }
            let needed_start = step.row_range.start.saturating_sub(1);
            min_row = Some(min_row.map_or(needed_start, |prev| prev.min(needed_start)));
            max_row_end = max_row_end.max(step.row_range.end);
        }
        let Some(first_needed) = min_row else {
            return Ok(None);
        };
        if !self.kernel_verified {
            let (_aligned, range) = self.aligned_page_range_for_rows(first_needed, max_row_end)?;
            return Ok(Some(range));
        }
        Ok(Some(
            self.exact_index_byte_range(first_needed, max_row_end)?,
        ))
    }

    fn aligned_page_range_for_rows(
        &self,
        start_row: usize,
        end_row_excl: usize,
    ) -> Result<(usize, ByteRange), SettledShieldedSegmentError> {
        let aligned_start = (start_row / INDEX_PAGE_ROWS) * INDEX_PAGE_ROWS;
        let aligned_end = end_row_excl
            .div_ceil(INDEX_PAGE_ROWS)
            .saturating_mul(INDEX_PAGE_ROWS)
            .min(self.rows.len());
        let range = self.exact_index_byte_range(aligned_start, aligned_end)?;
        Ok((aligned_start, range))
    }

    fn exact_index_byte_range(
        &self,
        start_row: usize,
        end_row_excl: usize,
    ) -> Result<ByteRange, SettledShieldedSegmentError> {
        let byte_start = self
            .index_offset
            .checked_add((start_row as u64) * ROW_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat("index range offset overflow".to_owned())
            })?;
        let byte_len = end_row_excl
            .checked_sub(start_row)
            .and_then(|rows| rows.checked_mul(ROW_INDEX_ENTRY_LEN))
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat(
                    "index range byte length overflow".to_owned(),
                )
            })?;
        Ok(ByteRange::new(byte_start, byte_len))
    }

    /// Verifies and decodes one sans-I/O [`ShieldedReadStep`] into individual
    /// `EncodedBlock` messages, repacking short (`< 32 KiB`) blocks so none
    /// pin a `512 KiB` slab allocation.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on slice mismatch, integrity error,
    /// or malformed protobuf framing, latching sticky `VALIDATION_INVALID`.
    pub fn verify_and_decode_slab(
        &self,
        step: &ShieldedReadStep,
        fetched: ShieldedFetchedSlab<'_>,
        selection: ShieldedProjectionSelection,
        order: HeightOrder,
    ) -> Result<Vec<EncodedBlock>, SettledShieldedSegmentError> {
        let mut out = Vec::with_capacity(step.row_range.len());
        let mut packed_out = Vec::new();
        let mut scratch = ShieldedReadScratch::default();
        let mut sink = StepOutputSink::Blocks {
            packed_out: &mut packed_out,
            out: &mut out,
        };
        let params = StepEmitParams {
            step,
            slab_bytes: fetched.slab_bytes,
            verified_index: fetched.verified_index,
            selection,
            order,
        };
        self.verify_and_emit_step(params, &mut scratch, &mut sink)?;
        flush_packed_blocks(&mut packed_out, &mut out)?;
        Ok(out)
    }

    /// Verifies and streams one sans-I/O [`ShieldedReadStep`] into `batcher`
    /// (`8 KiB` opening / `32 KiB` subsequent batches, zero-copy for isolated
    /// large passthrough blocks).
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on slice mismatch, integrity error,
    /// or malformed protobuf framing, latching sticky `VALIDATION_INVALID`.
    pub fn verify_and_decode_slab_into_batcher(
        &self,
        request: ShieldedSlabDecodeRequest<'_>,
        scratch: &mut ShieldedReadScratch,
        batcher: &mut EncodedBlockBatcher,
    ) -> Result<(), SettledShieldedSegmentError> {
        let mut sink = StepOutputSink::Batcher(batcher);
        let params = StepEmitParams {
            step: request.step,
            slab_bytes: request.fetched.slab_bytes,
            verified_index: request.fetched.verified_index,
            selection: request.selection,
            order: request.order,
        };
        self.verify_and_emit_step(params, scratch, &mut sink)
    }

    fn verify_and_emit_step(
        &self,
        params: StepEmitParams<'_>,
        scratch: &mut ShieldedReadScratch,
        sink: &mut StepOutputSink<'_>,
    ) -> Result<(), SettledShieldedSegmentError> {
        for idx in params.step.row_range.clone() {
            if self.rows[idx].validation_state() == VALIDATION_INVALID {
                return Err(SettledShieldedSegmentError::PayloadIntegrity(
                    "shielded segment frame previously failed validation".to_owned(),
                ));
            }
        }
        let Some(expected_slab_range) = params.step.slab_byte_range else {
            if params.slab_bytes.is_some() {
                return Err(SettledShieldedSegmentError::InvalidFormat(
                    "unexpected slab buffer passed to synthesized step".to_owned(),
                ));
            }
            return self.emit_synthesized_step(&params, scratch, sink);
        };
        let slab_bytes = params.slab_bytes.ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat(
                "missing slab bytes for slab-backed read step".to_owned(),
            )
        })?;
        if slab_bytes.len() != expected_slab_range.len {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "slab buffer length disagrees with planned byte range".to_owned(),
            ));
        }
        self.verify_slab_rows(params.step, &slab_bytes, params.verified_index)?;
        emit_verified_slab_rows(
            self,
            params.step,
            slab_bytes,
            RangeEmitTarget {
                order: params.order,
                selection: params.selection,
                cancel: None,
            },
            &mut EmitBuffers { scratch, sink },
        )
    }

    fn verify_slab_rows(
        &self,
        step: &ShieldedReadStep,
        slab_bytes: &[u8],
        verified_index: Option<VerifiedShieldedIndexSlice<'_>>,
    ) -> Result<(), SettledShieldedSegmentError> {
        let first_rel = self.rows[step.row_range.start].slab_rel_offset;
        for idx in step.row_range.clone() {
            let (rel, frame_len) = frame_span(&self.rows, self.slab_len, idx)?;
            let (local_start, local_end) = frame_bounds(rel, first_rel, frame_len)?;
            let frame = slab_bytes.get(local_start..local_end).ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat(
                    "slab frame bounds exceed slice".to_owned(),
                )
            })?;
            let verdict = self.verify_frame(idx, frame, verified_index);
            if verdict.is_err() {
                self.rows[idx].mark_invalid();
            }
            verdict?;
        }
        Ok(())
    }

    fn verify_frame(
        &self,
        row_idx: usize,
        frame: &[u8],
        verified_index: Option<VerifiedShieldedIndexSlice<'_>>,
    ) -> Result<(), SettledShieldedSegmentError> {
        let row = &self.rows[row_idx];
        let state = row.validation_state();
        if state == VALIDATION_INVALID {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "shielded segment frame previously failed validation".to_owned(),
            ));
        }
        let payload_len = decode_frame_prefix(frame)?;
        if FRAME_PREFIX_LEN + payload_len != frame.len() {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded frame prefix length disagrees with index".to_owned(),
            ));
        }
        let payload = &frame[FRAME_PREFIX_LEN..];
        let need_index_checks = !self.kernel_verified || state != VALIDATION_VALID;
        if !need_index_checks {
            return Ok(());
        }
        let index_view = verified_index.ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat(
                "missing index slice for shielded frame verification".to_owned(),
            )
        })?;
        let entry = index_view.entry(row_idx)?;
        if <[u8; 32]>::from(Sha256::digest(payload)) != entry.payload_sha256 {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "shielded segment payload SHA-256 mismatch on read".to_owned(),
            ));
        }
        if state != VALIDATION_VALID {
            let height = self.height_at(row_idx)?;
            let prev_hash_wire = if row_idx == 0 {
                self.first_prev_hash_wire
            } else {
                index_view.block_hash_wire(row_idx - 1)?
            };
            validate_shielded_block_wire(
                payload,
                ExpectedShieldedBlock {
                    height,
                    block_hash_wire: &entry.block_hash_wire,
                    prev_hash_wire: &prev_hash_wire,
                    block_time: entry.block_time,
                    pool_presence: row.pool_presence,
                },
            )?;
            row.mark_valid();
        }
        Ok(())
    }

    fn emit_synthesized_step(
        &self,
        params: &StepEmitParams<'_>,
        scratch: &mut ShieldedReadScratch,
        sink: &mut StepOutputSink<'_>,
    ) -> Result<(), SettledShieldedSegmentError> {
        let index_view = params.verified_index.ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat(
                "missing index slice for empty-nullifier block synthesis".to_owned(),
            )
        })?;
        let mut emit_row = |row_idx: usize| -> Result<(), SettledShieldedSegmentError> {
            let entry = index_view.entry(row_idx)?;
            let prev_hash_wire = if row_idx == 0 {
                self.first_prev_hash_wire
            } else {
                index_view.block_hash_wire(row_idx - 1)?
            };
            let height = self.height_at(row_idx)?;
            match sink {
                StepOutputSink::Batcher(batcher) => {
                    scratch.projected_frame.clear();
                    synthesize_empty_nullifier_payload(
                        height,
                        &entry.block_hash_wire,
                        &prev_hash_wire,
                        entry.block_time,
                        &mut scratch.projected_frame,
                    );
                    batcher
                        .push_payload(&scratch.projected_frame)
                        .map_err(|_| {
                            SettledShieldedSegmentError::InvalidFormat(
                                "synthesized empty nullifier frame invalid".to_owned(),
                            )
                        })?;
                }
                StepOutputSink::Blocks { packed_out, out } => {
                    scratch.projected_frame.clear();
                    synthesize_empty_nullifier_block(
                        height,
                        &entry.block_hash_wire,
                        &prev_hash_wire,
                        entry.block_time,
                        &mut scratch.projected_frame,
                    )?;
                    push_packed_frame(&scratch.projected_frame, packed_out, out)?;
                }
            }
            Ok(())
        };
        match params.order {
            HeightOrder::Ascending => {
                for row_idx in params.step.row_range.clone() {
                    emit_row(row_idx)?;
                }
            }
            HeightOrder::Descending => {
                for row_idx in params.step.row_range.clone().rev() {
                    emit_row(row_idx)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn resolve_index_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<Range<usize>, SettledShieldedSegmentError> {
        let seg_start = self.identity.start_height;
        let seg_end = self.identity.end_block.height;
        if range.start > range.end || range.start < seg_start {
            return Err(SettledShieldedSegmentError::HeightOutOfRange {
                height: range.start,
                start: seg_start,
                end: seg_end,
            });
        }
        if range.end > seg_end {
            return Err(SettledShieldedSegmentError::HeightOutOfRange {
                height: range.end,
                start: seg_start,
                end: seg_end,
            });
        }
        let first = usize::try_from(range.start.value() - seg_start.value()).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("height index overflow".to_owned())
        })?;
        let last = usize::try_from(range.end.value() - seg_start.value()).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("height index overflow".to_owned())
        })?;
        Ok(first..last + 1)
    }

    pub(super) fn height_at(
        &self,
        row_idx: usize,
    ) -> Result<BlockHeight, SettledShieldedSegmentError> {
        let delta = u32::try_from(row_idx).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("row index exceeds u32".to_owned())
        })?;
        let raw = self
            .identity
            .start_height
            .value()
            .checked_add(delta)
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat("block height overflow".to_owned())
            })?;
        Ok(BlockHeight::new(raw))
    }
}

/// Authenticated positional reader over one settled `ZSHRNG02` shielded segment.
pub struct SettledShieldedSegmentReader<S = File> {
    source: S,
    discard_scan_pages: Option<DiscardScanPages<S>>,
    index: SettledShieldedSegmentIndex,
}

impl SettledShieldedSegmentReader<File> {
    /// Opens and authenticates a regular `ZSHRNG02` segment file against `expected`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on open I/O failure, `fs-verity`
    /// mismatch/absence when required, or header/`index_sha256` verification failure.
    pub fn open(
        path: &Path,
        expected: &ShieldedSegmentIdentity,
        options: SettledShieldedSegmentOpenOptions,
    ) -> Result<Self, SettledShieldedSegmentError> {
        let file = segment_verity::open_regular_file(path, "shielded segment")?;
        Self::open_file(file, expected, options)
    }

    /// Authenticates an already-opened regular `ZSHRNG02` segment file against `expected`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on non-regular files, `fs-verity`
    /// mismatch/absence when required, or header/`index_sha256` verification failure.
    pub fn open_file(
        file: File,
        expected: &ShieldedSegmentIdentity,
        options: SettledShieldedSegmentOpenOptions,
    ) -> Result<Self, SettledShieldedSegmentError> {
        Self::open_file_with_scan_policy(file, expected, options, false)
    }

    pub(crate) fn open_file_with_scan_policy(
        file: File,
        expected: &ShieldedSegmentIdentity,
        options: SettledShieldedSegmentOpenOptions,
        constrained: bool,
    ) -> Result<Self, SettledShieldedSegmentError> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected a regular settled shielded segment file",
            )
            .into());
        }
        let measured_verity = segment_verity::measure(&file)?;
        let kernel_verified = segment_verity::check_pinned_verity(
            expected.fs_verity_sha256,
            measured_verity,
            "shielded segment",
        )?;
        if options.require_fs_verity && !kernel_verified {
            return Err(SettledShieldedSegmentError::FsVerityRequired);
        }
        let discard_scan_pages = bounded_file_scan(&file, constrained);
        let mut reader = Self::admit_with_verity(file, expected, options, kernel_verified)?;
        reader.discard_scan_pages = discard_scan_pages;
        Ok(reader)
    }
}

impl<S: PositionalByteSource> SettledShieldedSegmentReader<S> {
    /// Opens and authenticates any [`PositionalByteSource`] against `expected`
    /// in `O(index)` (or full-payload mode when `options.verify_full_payload` is set).
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::FsVerityRequired`] if `require_fs_verity`
    /// is requested on a generic source, or any header/`index_sha256`/payload failure.
    pub fn open_source(
        source: S,
        expected: &ShieldedSegmentIdentity,
        options: SettledShieldedSegmentOpenOptions,
    ) -> Result<Self, SettledShieldedSegmentError> {
        if options.require_fs_verity {
            return Err(SettledShieldedSegmentError::FsVerityRequired);
        }
        Self::admit_with_verity(source, expected, options, false)
    }

    fn admit_with_verity(
        source: S,
        expected: &ShieldedSegmentIdentity,
        options: SettledShieldedSegmentOpenOptions,
        kernel_verified: bool,
    ) -> Result<Self, SettledShieldedSegmentError> {
        let total_len = source.total_len()?;
        let mut header_bytes = [0_u8; HEADER_LEN];
        source.read_exact_at(0, &mut header_bytes)?;
        let header = ShieldedSegmentHeader::decode(&header_bytes, expected, total_len)?;
        let index_range = header.index_and_trailer_range()?;
        let index_and_trailer = source.read_vec_at(index_range.offset, index_range.len)?;
        let index = SettledShieldedSegmentIndex::decode_index(
            &header_bytes,
            &index_and_trailer,
            expected,
            total_len,
            kernel_verified,
        )?;
        let reader = Self {
            source,
            discard_scan_pages: None,
            index,
        };
        if options.verify_full_payload {
            reader.verify_full_payload_with_raw(&header_bytes, &index_and_trailer)?;
        }
        Ok(reader)
    }

    /// Streams the complete segment (`header || Slab 0 || index`) and verifies
    /// whole-body `SHA-256`, every block's `payload_sha256`, and wire envelope
    /// consistency, marking verified rows valid and latching `VALIDATION_INVALID`
    /// on any mismatch.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on I/O failure or any body/wire mismatch.
    pub fn verify_full_payload(&self) -> Result<(), SettledShieldedSegmentError> {
        let mut header_bytes = [0_u8; HEADER_LEN];
        self.source.read_exact_at(0, &mut header_bytes)?;
        let total_len = self.source.total_len()?;
        let header = ShieldedSegmentHeader::decode(&header_bytes, &self.index.identity, total_len)?;
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
    ) -> Result<(), SettledShieldedSegmentError> {
        let raw_index_len = self.index.rows.len() * ROW_INDEX_ENTRY_LEN;
        if index_and_trailer.len() != raw_index_len + TRAILER_LEN {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded index and trailer slice length disagrees with header".to_owned(),
            ));
        }
        let (raw_index, trailer) = index_and_trailer.split_at(raw_index_len);
        if trailer != self.index.identity.sha256 {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "shielded segment trailer SHA-256 mismatch".to_owned(),
            ));
        }
        verify_index_digest(
            header_bytes,
            raw_index,
            trailer,
            &self.index.identity.index_sha256,
        )?;

        let mut whole_digest = Sha256::new();
        whole_digest.update(*header_bytes);
        verify_all_slab_frames(&self.source, &self.index, raw_index, &mut whole_digest)?;
        whole_digest.update(raw_index);
        let computed: [u8; 32] = whole_digest.finalize().into();
        if computed != trailer || computed != self.index.identity.sha256 {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "portable shielded segment length or SHA-256 mismatch".to_owned(),
            ));
        }
        Ok(())
    }

    /// Returns a reference to the compact in-memory index and sans-I/O planner.
    #[must_use]
    pub const fn index(&self) -> &SettledShieldedSegmentIndex {
        &self.index
    }

    /// Returns whether admission proved kernel `fs-verity` protection.
    #[must_use]
    pub const fn kernel_verified(&self) -> bool {
        self.index.kernel_verified()
    }

    /// Returns the authenticated segment identity.
    #[must_use]
    pub const fn identity(&self) -> ShieldedSegmentIdentity {
        self.index.identity()
    }

    /// Returns the number of blocks stored in this segment.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.index.block_count()
    }

    /// Returns the heap bytes retained by the in-memory index metadata.
    #[must_use]
    pub fn heap_metadata_bytes(&self) -> usize {
        self.index.heap_metadata_bytes()
    }

    /// Returns the authenticated previous block hash declared before the segment's first block.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::PayloadIntegrity`] if the stored
    /// header hash is not a valid internal block hash.
    pub fn first_previous_block_hash(&self) -> Result<BlockHash, SettledShieldedSegmentError> {
        decode_wire_hash(self.index.first_prev_hash_wire(), "first_prev_hash")
    }

    /// Returns the authenticated block hash recorded in the row index for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] if `height` is outside the segment
    /// or the row failed validation.
    pub fn block_hash_at(
        &self,
        height: BlockHeight,
    ) -> Result<BlockHash, SettledShieldedSegmentError> {
        let byte_range = self.index.block_hash_byte_range(height)?;
        let bytes = self.source.read_vec_at(byte_range.offset, byte_range.len)?;
        self.index.decode_block_hash_from_slice(
            height,
            ShieldedIndexSlice::new(byte_range.offset, &bytes),
        )
    }

    /// Returns the authenticated `(BlockId, previous_block_hash)` pair at `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] if `height` is outside the segment
    /// or a required index entry failed validation.
    pub fn block_link_at(
        &self,
        height: BlockHeight,
    ) -> Result<(BlockId, BlockHash), SettledShieldedSegmentError> {
        let block_hash = self.block_hash_at(height)?;
        let previous_block_hash = if height == self.index.identity().start_height {
            self.first_previous_block_hash()?
        } else {
            self.block_hash_at(BlockHeight::new(height.value() - 1))?
        };
        Ok((BlockId::new(height, block_hash), previous_block_hash))
    }

    /// Reads and authenticates every row's block hash in a single coalesced
    /// index read in ascending block height order.
    pub(crate) fn read_all_block_hashes(
        &self,
    ) -> Result<Vec<BlockHash>, SettledShieldedSegmentError> {
        let row_count = self.index.rows.len();
        for row in &self.index.rows {
            if row.validation_state() == VALIDATION_INVALID {
                return Err(SettledShieldedSegmentError::PayloadIntegrity(
                    "shielded segment frame previously failed validation".to_owned(),
                ));
            }
        }
        let byte_len = row_count.checked_mul(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat("shielded index size overflow".to_owned())
        })?;
        let raw_index = self
            .source
            .read_vec_at(self.index.index_offset, byte_len)
            .inspect_err(|_| {
                for row in &self.index.rows {
                    row.mark_invalid();
                }
            })?;
        let verified = self
            .index
            .authenticate_index_slice(ShieldedIndexSlice::new(
                self.index.index_offset,
                &raw_index,
            ))?;
        let mut hashes = Vec::with_capacity(row_count);
        for row_idx in 0..row_count {
            let wire = verified.block_hash_wire(row_idx).inspect_err(|_| {
                self.index.rows[row_idx].mark_invalid();
            })?;
            let hash = decode_wire_hash(&wire, "row.block_hash").inspect_err(|_| {
                self.index.rows[row_idx].mark_invalid();
            })?;
            hashes.push(hash);
        }
        Ok(hashes)
    }

    /// Returns the uncompressed canonical byte budget recorded for `height`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::HeightOutOfRange`] if `height` is
    /// outside this segment.
    pub fn native_bytes_at(&self, height: BlockHeight) -> Result<u32, SettledShieldedSegmentError> {
        self.index.native_bytes_at(height)
    }

    /// Reads the default shielded `CompactBlock` frames across `range`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on height, I/O, wire-fee, or
    /// integrity errors.
    pub fn read_default_range(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
    ) -> Result<Vec<EncodedBlock>, SettledShieldedSegmentError> {
        self.read_projected_range(range, order, ShieldedProjectionSelection::DEFAULT_SHIELDED)
    }

    /// Reads the default shielded `CompactBlock` frames across `range` for canonical
    /// artifact reconstruction alongside `ZSTSUP01`, allowing rows marked with
    /// wide fees above `u32::MAX` (whose `Slab 0` fee is zeroed).
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on height, I/O, or integrity errors.
    pub fn read_default_range_for_reconstruction(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
    ) -> Result<Vec<EncodedBlock>, SettledShieldedSegmentError> {
        self.read_projected_range_inner(
            range,
            order,
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
            false,
        )
    }

    /// Reads and projects `CompactBlock` frames across `range` for `selection`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on height, I/O, wire-fee, or
    /// integrity errors.
    pub fn read_projected_range(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        selection: ShieldedProjectionSelection,
    ) -> Result<Vec<EncodedBlock>, SettledShieldedSegmentError> {
        self.read_projected_range_inner(range, order, selection, true)
    }

    fn read_projected_range_inner(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        selection: ShieldedProjectionSelection,
        enforce_wire_fee_limit: bool,
    ) -> Result<Vec<EncodedBlock>, SettledShieldedSegmentError> {
        let plan = self.index.plan_block_range_with_wire_fee_check(
            range,
            selection,
            enforce_wire_fee_limit,
        )?;
        let mut out = Vec::with_capacity(plan.block_count());
        let mut packed_out = Vec::new();
        let mut scratch = ShieldedReadScratch::default();
        let mut sink = StepOutputSink::Blocks {
            packed_out: &mut packed_out,
            out: &mut out,
        };
        self.execute_range_plan(
            &plan,
            RangeEmitTarget {
                order,
                selection,
                cancel: None,
            },
            &mut EmitBuffers {
                scratch: &mut scratch,
                sink: &mut sink,
            },
            plan.block_count() > 1,
        )?;
        flush_packed_blocks(&mut packed_out, &mut out)?;
        Ok(out)
    }

    /// Streams projected `CompactBlock` wire payloads across `request.range` directly
    /// into `batcher`, coalescing short messages into `8 KiB` opening and `32 KiB`
    /// subsequent [`EncodedBlockFrame`] batches and emitting isolated `>= 32 KiB`
    /// passthrough blocks zero-copy.
    ///
    /// [`EncodedBlockFrame`]: zinder_proto::compat::lightwalletd::EncodedBlockFrame
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on height, I/O, wire-fee, or
    /// integrity errors.
    pub fn append_projected_frames(
        &self,
        request: ShieldedBatchReadRequest,
        scratch: &mut ShieldedReadScratch,
        batcher: &mut EncodedBlockBatcher,
    ) -> Result<(), SettledShieldedSegmentError> {
        let plan = self.index.plan_block_range_with_wire_fee_check(
            request.range,
            request.selection,
            request.enforce_wire_fee_limit,
        )?;
        let mut sink = StepOutputSink::Batcher(batcher);
        self.execute_range_plan(
            &plan,
            RangeEmitTarget {
                order: request.order,
                selection: request.selection,
                cancel: None,
            },
            &mut EmitBuffers {
                scratch,
                sink: &mut sink,
            },
            true,
        )
    }

    /// Streams a bounded prefix (or suffix) within `request.budget` directly into
    /// `batcher`, returning `None` if no complete block fits the budget.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on height, I/O, wire-fee, or
    /// integrity errors.
    pub fn append_bounded_projected_frames(
        &self,
        request: ShieldedBoundedBatchReadRequest,
        scratch: &mut ShieldedReadScratch,
        batcher: &mut EncodedBlockBatcher,
    ) -> Result<Option<BoundedShieldedRangePlan>, SettledShieldedSegmentError> {
        let Some(bounded) = self.index.plan_bounded_block_range(
            request.range,
            request.order,
            request.budget,
            request.selection,
        )?
        else {
            return Ok(None);
        };
        let mut sink = StepOutputSink::Batcher(batcher);
        self.execute_range_plan(
            &bounded.plan,
            RangeEmitTarget {
                order: request.order,
                selection: request.selection,
                cancel: None,
            },
            &mut EmitBuffers {
                scratch,
                sink: &mut sink,
            },
            true,
        )?;
        Ok(Some(bounded))
    }

    /// Reads encoded wire blocks for a precomputed `plan` with scratch reuse and
    /// cooperative cancellation polling between read steps.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on I/O, integrity, or cancellation.
    pub fn read_bounded_projected_blocks_for_plan(
        &self,
        plan: &BoundedShieldedRangePlan,
        execution: ShieldedPlanExecution<'_>,
        scratch: &mut ShieldedReadScratch,
    ) -> Result<Vec<EncodedBlock>, SettledShieldedSegmentError> {
        let mut out = Vec::with_capacity(plan.kept_blocks);
        let mut packed_out = Vec::new();
        let mut sink = StepOutputSink::Blocks {
            packed_out: &mut packed_out,
            out: &mut out,
        };
        self.execute_range_plan(
            &plan.plan,
            RangeEmitTarget {
                order: execution.order,
                selection: execution.selection,
                cancel: execution.cancel,
            },
            &mut EmitBuffers {
                scratch,
                sink: &mut sink,
            },
            true,
        )?;
        flush_packed_blocks(&mut packed_out, &mut out)?;
        Ok(out)
    }

    /// Appends projected wire frames for a precomputed `plan` directly into
    /// `batcher`, polling cooperative cancellation between read steps.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on I/O, integrity, or cancellation.
    pub fn append_bounded_projected_frames_for_plan(
        &self,
        plan: &BoundedShieldedRangePlan,
        execution: ShieldedPlanExecution<'_>,
        scratch: &mut ShieldedReadScratch,
        batcher: &mut EncodedBlockBatcher,
    ) -> Result<(), SettledShieldedSegmentError> {
        let mut sink = StepOutputSink::Batcher(batcher);
        self.execute_range_plan(
            &plan.plan,
            RangeEmitTarget {
                order: execution.order,
                selection: execution.selection,
                cancel: execution.cancel,
            },
            &mut EmitBuffers {
                scratch,
                sink: &mut sink,
            },
            true,
        )
    }

    fn execute_range_plan(
        &self,
        plan: &ShieldedRangePlan,
        target: RangeEmitTarget<'_>,
        buffers: &mut EmitBuffers<'_, '_>,
        scan: bool,
    ) -> Result<(), SettledShieldedSegmentError> {
        if target
            .cancel
            .is_some_and(|flag| flag.load(Ordering::Relaxed))
        {
            return Err(SettledShieldedSegmentError::Cancelled);
        }
        let needed_range = plan.needed_index_byte_range();
        let fetched_index = match needed_range {
            Some(byte_range) => Some(
                self.source
                    .read_vec_at(byte_range.offset, byte_range.len)
                    .inspect_err(|_| mark_plan_rows_invalid(&self.index, plan))?,
            ),
            None => None,
        };
        let verified_index = match (needed_range, fetched_index.as_deref()) {
            (Some(byte_range), Some(bytes)) => Some(
                self.index
                    .authenticate_index_slice(ShieldedIndexSlice::new(byte_range.offset, bytes))?,
            ),
            _ => None,
        };
        let mut drop_behind = scan
            .then(|| ScanDropBehind::new(&self.source, self.discard_scan_pages))
            .flatten();
        let mut emit_step = |step: &ShieldedReadStep,
                             step_scratch: &mut ShieldedReadScratch,
                             step_sink: &mut StepOutputSink<'_>|
         -> Result<(), SettledShieldedSegmentError> {
            let slab_bytes = match step.slab_byte_range() {
                Some(byte_range) => {
                    let bytes = self
                        .source
                        .read_vec_at(byte_range.offset, byte_range.len)
                        .inspect_err(|_| {
                            for row_idx in step.row_range() {
                                self.index.rows[row_idx].mark_invalid();
                            }
                        })?;
                    if let Some(scan) = &mut drop_behind {
                        scan.consumed(byte_range);
                    }
                    Some(bytes)
                }
                None => None,
            };
            self.index.verify_and_emit_step(
                StepEmitParams {
                    step,
                    slab_bytes,
                    verified_index,
                    selection: target.selection,
                    order: target.order,
                },
                step_scratch,
                step_sink,
            )
        };
        let mut steps = plan.steps().iter();
        while let Some(step) = match target.order {
            HeightOrder::Ascending => steps.next(),
            HeightOrder::Descending => steps.next_back(),
        } {
            if target
                .cancel
                .is_some_and(|flag| flag.load(Ordering::Relaxed))
            {
                return Err(SettledShieldedSegmentError::Cancelled);
            }
            emit_step(step, buffers.scratch, buffers.sink)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct RangeEmitTarget<'a> {
    order: HeightOrder,
    selection: ShieldedProjectionSelection,
    cancel: Option<&'a AtomicBool>,
}

struct EmitBuffers<'s, 'b> {
    scratch: &'s mut ShieldedReadScratch,
    sink: &'s mut StepOutputSink<'b>,
}

struct StepEmitParams<'a> {
    step: &'a ShieldedReadStep,
    slab_bytes: Option<Vec<u8>>,
    verified_index: Option<VerifiedShieldedIndexSlice<'a>>,
    selection: ShieldedProjectionSelection,
    order: HeightOrder,
}

enum StepOutputSink<'a> {
    Batcher(&'a mut EncodedBlockBatcher),
    Blocks {
        packed_out: &'a mut Vec<u8>,
        out: &'a mut Vec<EncodedBlock>,
    },
}

fn mark_plan_rows_invalid(index: &SettledShieldedSegmentIndex, plan: &ShieldedRangePlan) {
    for step in plan.steps() {
        for row_idx in step.row_range() {
            index.rows[row_idx].mark_invalid();
        }
    }
}

fn verify_index_digest(
    header_bytes: &[u8; HEADER_LEN],
    raw_index: &[u8],
    trailer: &[u8],
    expected_index_sha256: &[u8; 32],
) -> Result<(), SettledShieldedSegmentError> {
    let mut index_digest = Sha256::new();
    index_digest.update(header_bytes);
    index_digest.update(raw_index);
    index_digest.update(trailer);
    let computed: [u8; 32] = index_digest.finalize().into();
    if &computed != expected_index_sha256 {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(
            "shielded segment index SHA-256 disagrees with pinned identity".to_owned(),
        ));
    }
    Ok(())
}

fn compute_index_page_digests(raw_index: &[u8]) -> Box<[[u8; 32]]> {
    raw_index
        .chunks(INDEX_PAGE_ROWS * ROW_INDEX_ENTRY_LEN)
        .map(|page| Sha256::digest(page).into())
        .collect::<Vec<[u8; 32]>>()
        .into_boxed_slice()
}

fn decode_and_validate_rows(
    raw_index: &[u8],
    slab_len: u32,
    header: &ShieldedSegmentHeader,
) -> Result<Box<[CompactShieldedRow]>, SettledShieldedSegmentError> {
    let (raw_entries, rem) = raw_index.as_chunks::<ROW_INDEX_ENTRY_LEN>();
    if !rem.is_empty() || raw_entries.is_empty() {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "shielded row index alignment or empty count error".to_owned(),
        ));
    }
    let mut entries = Vec::with_capacity(raw_entries.len());
    for fixed in raw_entries {
        entries.push(DecodedRowIndexEntry::decode(fixed)?);
    }
    let compact = validate_entries(&entries, slab_len, header)?;
    Ok(compact.into_boxed_slice())
}

pub(super) fn validate_entries(
    entries: &[DecodedRowIndexEntry],
    slab_len: u32,
    header: &ShieldedSegmentHeader,
) -> Result<Vec<CompactShieldedRow>, SettledShieldedSegmentError> {
    if entries.is_empty() || entries[0].slab_rel_offset != 0 {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "row index must be non-empty and start at slab offset 0".to_owned(),
        ));
    }
    let Some(last) = entries.last() else {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "row index is empty".to_owned(),
        ));
    };
    if last.block_hash_wire != header.last_block_hash_wire {
        return Err(SettledShieldedSegmentError::ChainLinkage(
            "last row hash disagrees with header last_block_hash".to_owned(),
        ));
    }
    let max_payload = u32::try_from(SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES).map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat(
            "maximum shielded payload limit exceeds u32".to_owned(),
        )
    })?;
    let max_frame = FRAME_PREFIX_U32.checked_add(max_payload).ok_or_else(|| {
        SettledShieldedSegmentError::InvalidFormat(
            "maximum shielded frame limit exceeds u32".to_owned(),
        )
    })?;
    let min_frame = FRAME_PREFIX_U32 + 1;
    let mut rows = Vec::with_capacity(entries.len());
    for (idx, entry) in entries.iter().enumerate() {
        let next_rel = entries
            .get(idx + 1)
            .map_or(slab_len, |next| next.slab_rel_offset);
        let frame_len = next_rel.checked_sub(entry.slab_rel_offset).ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat("non-ascending row slab offsets".to_owned())
        })?;
        if frame_len < min_frame {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "row index frame span is smaller than minimum frame".to_owned(),
            ));
        }
        if frame_len > max_frame {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "row index frame span exceeds maximum frame".to_owned(),
            ));
        }
        rows.push(CompactShieldedRow::from_entry(entry));
    }
    Ok(rows)
}

fn verify_all_slab_frames<S: PositionalByteSource>(
    source: &S,
    index: &SettledShieldedSegmentIndex,
    raw_index: &[u8],
    whole_digest: &mut Sha256,
) -> Result<(), SettledShieldedSegmentError> {
    let mut reader = BufferedSourceSpan::new(source, index.slab_offset, u64::from(index.slab_len));
    let mut payload_buf = Vec::new();
    let mut prev_hash_wire = index.first_prev_hash_wire;
    for (idx, compact_row) in index.rows.iter().enumerate() {
        let entry_start = idx * ROW_INDEX_ENTRY_LEN;
        let entry_raw = &raw_index[entry_start..entry_start + ROW_INDEX_ENTRY_LEN];
        let mut prefix = [0_u8; FRAME_PREFIX_LEN];
        reader.read_exact(&mut prefix)?;
        whole_digest.update(prefix);
        let payload_len = decode_frame_prefix(&prefix)?;
        let (_, expected_frame_len) = frame_span(&index.rows, index.slab_len, idx)?;
        if u32::try_from(FRAME_PREFIX_LEN + payload_len).ok() != Some(expected_frame_len) {
            compact_row.mark_invalid();
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "streamed slab frame length disagrees with row index".to_owned(),
            ));
        }
        payload_buf.resize(payload_len, 0);
        reader.read_exact(&mut payload_buf)?;
        whole_digest.update(&payload_buf);
        let payload_sha: [u8; 32] = Sha256::digest(&payload_buf).into();
        let expected_sha = read_hash32(&entry_raw[32..64])?;
        if payload_sha != expected_sha {
            compact_row.mark_invalid();
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "streamed slab frame disagrees with row index".to_owned(),
            ));
        }
        let block_hash_wire = read_hash32(&entry_raw[..32])?;
        let block_time = read_u32_be(&entry_raw[64..68])?;
        let height = index.height_at(idx)?;
        if let Err(err) = validate_shielded_block_wire(
            &payload_buf,
            ExpectedShieldedBlock {
                height,
                block_hash_wire: &block_hash_wire,
                prev_hash_wire: &prev_hash_wire,
                block_time,
                pool_presence: compact_row.pool_presence,
            },
        ) {
            compact_row.mark_invalid();
            return Err(err);
        }
        compact_row.mark_valid();
        prev_hash_wire = block_hash_wire;
    }
    if reader.remaining != 0 {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "streamed slab bytes do not fill header slab length".to_owned(),
        ));
    }
    Ok(())
}

struct BufferedSourceSpan<'a, S> {
    source: &'a S,
    cursor_offset: u64,
    remaining: u64,
    buffer: Vec<u8>,
    pos: usize,
    filled: usize,
}

impl<'a, S: PositionalByteSource> BufferedSourceSpan<'a, S> {
    fn new(source: &'a S, offset: u64, len: u64) -> Self {
        Self {
            source,
            cursor_offset: offset,
            remaining: len,
            buffer: Vec::new(),
            pos: 0,
            filled: 0,
        }
    }

    fn read_exact(&mut self, mut dst: &mut [u8]) -> Result<(), SettledShieldedSegmentError> {
        while !dst.is_empty() {
            if self.pos == self.filled {
                if self.remaining == 0 {
                    return Err(SettledShieldedSegmentError::Io(
                        io::ErrorKind::UnexpectedEof.into(),
                    ));
                }
                let next_len =
                    usize::try_from(self.remaining.min(STAGING_CHUNK as u64)).map_err(|_| {
                        SettledShieldedSegmentError::InvalidFormat(
                            "staging chunk size overflow".to_owned(),
                        )
                    })?;
                self.buffer.resize(next_len, 0);
                self.source
                    .read_exact_at(self.cursor_offset, &mut self.buffer)?;
                self.cursor_offset += next_len as u64;
                self.remaining -= next_len as u64;
                self.pos = 0;
                self.filled = next_len;
            }
            let take = dst.len().min(self.filled - self.pos);
            dst[..take].copy_from_slice(&self.buffer[self.pos..self.pos + take]);
            self.pos += take;
            dst = &mut dst[take..];
        }
        Ok(())
    }
}

fn select_bounded_rows(
    rows: &[CompactShieldedRow],
    range: Range<usize>,
    order: HeightOrder,
    max_bytes: usize,
    allow_oversized_first: bool,
) -> Result<Option<(Range<usize>, usize)>, SettledShieldedSegmentError> {
    let mut remaining = max_bytes;
    let mut kept_native_bytes = 0_usize;
    let mut count = 0_usize;
    let mut iter = rows[range.clone()].iter();
    while let Some(row) = match order {
        HeightOrder::Ascending => iter.next(),
        HeightOrder::Descending => iter.next_back(),
    } {
        let row_bytes = usize::try_from(row.native_bytes).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("native row bytes exceed usize".to_owned())
        })?;
        if (count != 0 || !allow_oversized_first) && row_bytes > remaining {
            break;
        }
        count += 1;
        kept_native_bytes = kept_native_bytes.saturating_add(row_bytes);
        remaining = remaining.saturating_sub(row_bytes);
    }
    if count == 0 {
        return Ok(None);
    }
    let selected = match order {
        HeightOrder::Ascending => range.start..range.start + count,
        HeightOrder::Descending => range.end - count..range.end,
    };
    Ok(Some((selected, kept_native_bytes)))
}

#[derive(Clone, Copy)]
struct ShortOrProjectedRow<'a> {
    passthrough: bool,
    framed_row: &'a [u8],
    raw_payload: &'a [u8],
    selection: ShieldedProjectionSelection,
}

fn emit_verified_slab_rows(
    index: &SettledShieldedSegmentIndex,
    step: &ShieldedReadStep,
    slab_bytes: Vec<u8>,
    target: RangeEmitTarget<'_>,
    buffers: &mut EmitBuffers<'_, '_>,
) -> Result<(), SettledShieldedSegmentError> {
    if step.isolated_large_row && step.row_range.len() == 1 {
        let row_idx = step.row_range.start;
        if can_pass_through_zero_copy(index.rows[row_idx].pool_presence, target.selection) {
            return emit_zero_copy_isolated_row(slab_bytes, buffers.sink);
        }
    }
    let first_rel = index.rows[step.row_range.start].slab_rel_offset;
    let mut emit_row = |row_idx: usize| -> Result<(), SettledShieldedSegmentError> {
        let (rel, frame_len) = frame_span(&index.rows, index.slab_len, row_idx)?;
        let (local_start, local_end) = frame_bounds(rel, first_rel, frame_len)?;
        let raw_payload = &slab_bytes[local_start + FRAME_PREFIX_LEN..local_end];
        let passthrough =
            can_pass_through_zero_copy(index.rows[row_idx].pool_presence, target.selection);
        emit_short_or_projected_row(
            ShortOrProjectedRow {
                passthrough,
                framed_row: &slab_bytes[local_start..local_end],
                raw_payload,
                selection: target.selection,
            },
            buffers.scratch,
            buffers.sink,
        )
    };
    match target.order {
        HeightOrder::Ascending => {
            for row_idx in step.row_range.clone() {
                emit_row(row_idx)?;
            }
        }
        HeightOrder::Descending => {
            for row_idx in step.row_range.clone().rev() {
                emit_row(row_idx)?;
            }
        }
    }
    Ok(())
}

fn emit_zero_copy_isolated_row(
    slab_bytes: Vec<u8>,
    sink: &mut StepOutputSink<'_>,
) -> Result<(), SettledShieldedSegmentError> {
    let alloc_len = slab_bytes.capacity();
    let block = EncodedBlock::from_length_prefixed_in(Bytes::from(slab_bytes), alloc_len).map_err(
        |_| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded frame is not valid gRPC message".to_owned(),
            )
        },
    )?;
    match sink {
        StepOutputSink::Batcher(batcher) => {
            batcher.push_block(block);
        }
        StepOutputSink::Blocks { packed_out, out } => {
            flush_packed_blocks(packed_out, out)?;
            out.push(block);
        }
    }
    Ok(())
}

fn emit_short_or_projected_row(
    row: ShortOrProjectedRow<'_>,
    scratch: &mut ShieldedReadScratch,
    sink: &mut StepOutputSink<'_>,
) -> Result<(), SettledShieldedSegmentError> {
    if row.passthrough {
        return match sink {
            StepOutputSink::Batcher(batcher) => {
                batcher.push_payload(row.raw_payload).map_err(|_| {
                    SettledShieldedSegmentError::InvalidFormat(
                        "shielded frame is not valid gRPC message".to_owned(),
                    )
                })
            }
            StepOutputSink::Blocks { packed_out, out } => {
                push_packed_frame(row.framed_row, packed_out, out)
            }
        };
    }
    project_shielded_block_wire_with_scratch(
        row.raw_payload,
        row.selection,
        &mut scratch.projected_frame,
        &mut scratch.tx_scratch,
    )?;
    match sink {
        StepOutputSink::Batcher(batcher) => batcher
            .push_payload(&scratch.projected_frame[FRAME_PREFIX_LEN..])
            .map_err(|_| {
                SettledShieldedSegmentError::InvalidFormat(
                    "projected frame is not valid gRPC message".to_owned(),
                )
            }),
        StepOutputSink::Blocks { packed_out, out } => {
            push_packed_frame(&scratch.projected_frame, packed_out, out)
        }
    }
}

fn push_packed_frame(
    frame: &[u8],
    packed_out: &mut Vec<u8>,
    out: &mut Vec<EncodedBlock>,
) -> Result<(), SettledShieldedSegmentError> {
    if !packed_out.is_empty() && packed_out.len() + frame.len() > REPACKED_SHORT_BATCH_BYTES {
        flush_packed_blocks(packed_out, out)?;
    }
    packed_out.extend_from_slice(frame);
    if packed_out.len() >= REPACKED_SHORT_BATCH_BYTES {
        flush_packed_blocks(packed_out, out)?;
    }
    Ok(())
}

fn decode_wire_hash(
    bytes: &[u8; 32],
    label: &str,
) -> Result<BlockHash, SettledShieldedSegmentError> {
    decode_internal_block_hash(bytes).map_err(|err| {
        SettledShieldedSegmentError::PayloadIntegrity(format!("invalid {label}: {err}"))
    })
}

fn can_skip_slab(row: &CompactShieldedRow, selection: ShieldedProjectionSelection) -> bool {
    selection.nullifiers_only
        && row.validation_state() != VALIDATION_INVALID
        && !row
            .pool_presence
            .intersects(selection.requested_pool_mask())
}

const fn can_pass_through_zero_copy(
    presence: PoolPresenceMask,
    selection: ShieldedProjectionSelection,
) -> bool {
    let excluded = selection.requested_pool_mask().complement_known();
    !selection.nullifiers_only && !presence.intersects(excluded)
}

fn find_skip_run_end(
    rows: &[CompactShieldedRow],
    start: usize,
    limit: usize,
    selection: ShieldedProjectionSelection,
) -> usize {
    let mut end = start;
    while end < limit && can_skip_slab(&rows[end], selection) {
        end += 1;
    }
    end
}

fn find_slab_span_end(
    rows: &[CompactShieldedRow],
    start: usize,
    limit: usize,
    selection: ShieldedProjectionSelection,
) -> usize {
    let mut end = start;
    while end < limit && !can_skip_slab(&rows[end], selection) {
        end += 1;
    }
    end
}

fn frame_bounds(
    rel: u32,
    first_rel: u32,
    frame_len: u32,
) -> Result<(usize, usize), SettledShieldedSegmentError> {
    let rel_delta = rel.checked_sub(first_rel).ok_or_else(|| {
        SettledShieldedSegmentError::InvalidFormat("non-ascending chunk frame offset".to_owned())
    })?;
    let local_start = usize::try_from(rel_delta).map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("frame offset exceeds usize".to_owned())
    })?;
    let frame_usize = usize::try_from(frame_len).map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("frame length exceeds usize".to_owned())
    })?;
    let local_end = local_start.checked_add(frame_usize).ok_or_else(|| {
        SettledShieldedSegmentError::InvalidFormat("frame end exceeds usize".to_owned())
    })?;
    Ok((local_start, local_end))
}

fn flush_packed_blocks(
    packed_out: &mut Vec<u8>,
    out: &mut Vec<EncodedBlock>,
) -> Result<(), SettledShieldedSegmentError> {
    if packed_out.is_empty() {
        return Ok(());
    }
    let mut taken = std::mem::take(packed_out);
    if taken.capacity() > REPACKED_SHORT_BATCH_BYTES && taken.len() <= REPACKED_SHORT_BATCH_BYTES {
        taken.shrink_to_fit();
    }
    let alloc_len = taken.capacity();
    let frozen = Bytes::from(taken);
    let mut offset = 0_usize;
    while offset < frozen.len() {
        let payload_len = decode_frame_prefix(&frozen[offset..])?;
        let end = offset + FRAME_PREFIX_LEN + payload_len;
        let block = EncodedBlock::from_length_prefixed_in(frozen.slice(offset..end), alloc_len)
            .map_err(|_| {
                SettledShieldedSegmentError::InvalidFormat(
                    "packed projected block has invalid gRPC prefix".to_owned(),
                )
            })?;
        out.push(block);
        offset = end;
    }
    Ok(())
}
