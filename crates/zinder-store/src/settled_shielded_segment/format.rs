//! Binary framing, header, and compact row-index definitions for `ZSHRNG02`.

use std::sync::atomic::{AtomicU8, Ordering};

use zinder_core::{BlockHeight, BlockId, wire::encode_internal_block_hash};
use zinder_proto::compat::lightwalletd::{BoundedChunkRetention, EncodedBlock};

use crate::positional_source::ByteRange;

use super::{
    PoolPresenceMask, SETTLED_SHIELDED_SEGMENT_MAGIC, SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
    SettledShieldedSegmentError, ShieldedSegmentIdentity,
};

pub(super) const HEADER_LEN: usize = 128;
pub(crate) const FRAME_PREFIX_LEN: usize = EncodedBlock::PREFIX_LEN;
pub(super) const FRAME_PREFIX_U32: u32 = 5;
pub(super) const ROW_INDEX_ENTRY_LEN: usize = 80;
pub(super) const TRAILER_LEN: usize = 32;
pub(super) const MAX_SLAB_BYTES: u64 = u32::MAX as u64;
/// Default sealed shielded segment `Slab 0` byte cap (`256 MiB`) matching `PostgreSQL` `BYTEA` bounds.
pub const DEFAULT_MAX_SLAB_BYTES: u64 = 256 * 1024 * 1024;
pub(super) const SLAB_TARGET_BYTES: usize = BoundedChunkRetention::MAX_ALLOCATION_BYTES;
pub(super) const LARGE_ROW_ISOLATION_MIN_BYTES: usize = EncodedBlock::LARGE_MESSAGE_MIN_BYTES;
pub(super) const REPACKED_SHORT_BATCH_BYTES: usize = EncodedBlock::LARGE_MESSAGE_MIN_BYTES;
pub(super) const STAGING_CHUNK: usize = 256 * 1024;
pub(super) const INDEX_PAGE_ROWS: usize = 64;

const _: () = assert!(FRAME_PREFIX_LEN == FRAME_PREFIX_U32 as usize);
const _: () = assert!(std::mem::size_of::<CompactShieldedRow>() == 12);

pub(super) const VALIDATION_UNVALIDATED: u8 = 0;
pub(super) const VALIDATION_VALID: u8 = 1;
pub(super) const VALIDATION_INVALID: u8 = 2;

pub(super) const ROW_FLAG_UNREPRESENTABLE_WIRE_FEE: u8 = 0x01;
pub(super) const ROW_KNOWN_FLAGS: u8 = ROW_FLAG_UNREPRESENTABLE_WIRE_FEE;

/// Fixed 128-byte header of one `ZSHRNG02` settled shielded segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShieldedSegmentHeader {
    pub(super) network_tag: u32,
    pub(super) artifact_schema_version: u16,
    pub(super) segment_flags: u16,
    pub(super) start_height: u32,
    pub(super) block_count: u32,
    pub(super) first_prev_hash_wire: [u8; 32],
    pub(super) last_block_hash_wire: [u8; 32],
    pub(super) slab_offset: u64,
    pub(super) index_offset: u64,
}

impl ShieldedSegmentHeader {
    /// Decodes a `128`-byte `ZSHRNG02` header and verifies its structural fields,
    /// segment `expected` identity, and total byte length.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::InvalidFormat`] or
    /// [`SettledShieldedSegmentError::ChainLinkage`] if magic, flags, offsets,
    /// identity pins, or total length disagree.
    pub fn decode(
        bytes: &[u8; HEADER_LEN],
        expected: &ShieldedSegmentIdentity,
        total_len: u64,
    ) -> Result<Self, SettledShieldedSegmentError> {
        let header = Self::decode_unchecked(bytes)?;
        header.matches_identity(expected, total_len)?;
        Ok(header)
    }

    pub(super) fn encode(self) -> [u8; HEADER_LEN] {
        let mut header = [0_u8; HEADER_LEN];
        header[..8].copy_from_slice(SETTLED_SHIELDED_SEGMENT_MAGIC);
        header[8..12].copy_from_slice(&self.network_tag.to_be_bytes());
        header[12..14].copy_from_slice(&self.artifact_schema_version.to_be_bytes());
        header[14..16].copy_from_slice(&self.segment_flags.to_be_bytes());
        header[16..20].copy_from_slice(&self.start_height.to_be_bytes());
        header[20..24].copy_from_slice(&self.block_count.to_be_bytes());
        header[24..56].copy_from_slice(&self.first_prev_hash_wire);
        header[56..88].copy_from_slice(&self.last_block_hash_wire);
        header[88..96].copy_from_slice(&self.slab_offset.to_be_bytes());
        header[96..104].copy_from_slice(&self.index_offset.to_be_bytes());
        header
    }

    pub(super) fn decode_unchecked(
        bytes: &[u8; HEADER_LEN],
    ) -> Result<Self, SettledShieldedSegmentError> {
        if &bytes[..8] != SETTLED_SHIELDED_SEGMENT_MAGIC {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded segment magic mismatch".to_owned(),
            ));
        }
        if bytes[104..HEADER_LEN].iter().any(|byte| *byte != 0) {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded segment header reserved bytes are non-zero".to_owned(),
            ));
        }
        let header = Self {
            network_tag: read_u32_be(&bytes[8..12])?,
            artifact_schema_version: read_u16_be(&bytes[12..14])?,
            segment_flags: read_u16_be(&bytes[14..16])?,
            start_height: read_u32_be(&bytes[16..20])?,
            block_count: read_u32_be(&bytes[20..24])?,
            first_prev_hash_wire: read_hash32(&bytes[24..56])?,
            last_block_hash_wire: read_hash32(&bytes[56..88])?,
            slab_offset: read_u64_be(&bytes[88..96])?,
            index_offset: read_u64_be(&bytes[96..104])?,
        };
        validate_header_fields(&header)?;
        Ok(header)
    }

    /// Returns the first covered height encoded in the header.
    #[must_use]
    pub const fn start_height(self) -> BlockHeight {
        BlockHeight::new(self.start_height)
    }

    /// Returns the number of blocks covered by this segment.
    #[must_use]
    pub const fn block_count(self) -> u32 {
        self.block_count
    }

    /// Returns the byte offset of `Slab 0`.
    #[must_use]
    pub const fn slab_offset(self) -> u64 {
        self.slab_offset
    }

    /// Returns the byte offset where the `80`-byte row index begins.
    #[must_use]
    pub const fn index_offset(self) -> u64 {
        self.index_offset
    }

    /// Returns the byte length of `Slab 0`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::InvalidFormat`] if the index
    /// offset precedes `Slab 0` or the span exceeds `u32::MAX`.
    pub fn slab_len(self) -> Result<u32, SettledShieldedSegmentError> {
        let len = self
            .index_offset
            .checked_sub(self.slab_offset)
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat(
                    "shielded segment index precedes slab".to_owned(),
                )
            })?;
        u32::try_from(len).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded segment slab exceeds u32 offset space".to_owned(),
            )
        })
    }

    /// Returns the exact [`ByteRange`] spanning the on-disk row index and trailing
    /// `32`-byte SHA-256 trailer (`[index_offset .. total_len)`).
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError::InvalidFormat`] if the range size
    /// overflows `usize`.
    pub fn index_and_trailer_range(self) -> Result<ByteRange, SettledShieldedSegmentError> {
        let index_bytes = usize::try_from(self.block_count)
            .ok()
            .and_then(|count| count.checked_mul(ROW_INDEX_ENTRY_LEN))
            .and_then(|bytes| bytes.checked_add(TRAILER_LEN))
            .ok_or_else(|| {
                SettledShieldedSegmentError::InvalidFormat("segment index size overflow".to_owned())
            })?;
        Ok(ByteRange::new(self.index_offset, index_bytes))
    }

    pub(super) fn matches_identity(
        self,
        expected: &ShieldedSegmentIdentity,
        file_len: u64,
    ) -> Result<(), SettledShieldedSegmentError> {
        let expected_count = expected.block_count()?;
        if self.network_tag != expected.network.id()
            || self.artifact_schema_version != expected.artifact_schema_version
            || self.start_height != expected.start_height.value()
            || self.block_count != expected_count
            || self.last_block_hash_wire != encode_internal_block_hash(expected.end_block.hash)
        {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded segment header does not match expected identity".to_owned(),
            ));
        }
        let expected_file_len = expected_total_file_len(self.index_offset, expected_count)?;
        if file_len != expected_file_len {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded segment file length does not match header and block count".to_owned(),
            ));
        }
        Ok(())
    }
}

fn validate_header_fields(
    header: &ShieldedSegmentHeader,
) -> Result<(), SettledShieldedSegmentError> {
    if header.segment_flags != 0 {
        return Err(SettledShieldedSegmentError::InvalidFormat(format!(
            "unsupported shielded segment flags {:#06x}",
            header.segment_flags
        )));
    }
    if header.block_count == 0 {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "shielded segment block count must be positive".to_owned(),
        ));
    }
    if header.slab_offset != HEADER_LEN as u64 {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "shielded segment slab must start immediately after header".to_owned(),
        ));
    }
    header.slab_len()?;
    expected_total_file_len(header.index_offset, header.block_count)?;
    Ok(())
}

pub(super) fn expected_total_file_len(
    index_offset: u64,
    block_count: u32,
) -> Result<u64, SettledShieldedSegmentError> {
    let index_bytes = u64::from(block_count)
        .checked_mul(ROW_INDEX_ENTRY_LEN as u64)
        .ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat("segment index size overflow".to_owned())
        })?;
    index_offset
        .checked_add(index_bytes)
        .and_then(|trailer_offset| trailer_offset.checked_add(TRAILER_LEN as u64))
        .ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat("segment total length overflow".to_owned())
        })
}

/// Decoded `80`-byte on-disk index record for one block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DecodedRowIndexEntry {
    pub(super) block_hash_wire: [u8; 32],
    pub(super) payload_sha256: [u8; 32],
    pub(super) block_time: u32,
    pub(super) slab_rel_offset: u32,
    pub(super) native_bytes: u32,
    pub(super) pool_presence: PoolPresenceMask,
    pub(super) row_flags: u8,
}

impl DecodedRowIndexEntry {
    pub(super) fn encode(self) -> [u8; ROW_INDEX_ENTRY_LEN] {
        let mut encoded = [0_u8; ROW_INDEX_ENTRY_LEN];
        encoded[..32].copy_from_slice(&self.block_hash_wire);
        encoded[32..64].copy_from_slice(&self.payload_sha256);
        encoded[64..68].copy_from_slice(&self.block_time.to_be_bytes());
        encoded[68..72].copy_from_slice(&self.slab_rel_offset.to_be_bytes());
        encoded[72..76].copy_from_slice(&self.native_bytes.to_be_bytes());
        encoded[76] = self.pool_presence.bits();
        encoded[77] = self.row_flags;
        encoded
    }

    pub(super) fn decode(
        raw: &[u8; ROW_INDEX_ENTRY_LEN],
    ) -> Result<Self, SettledShieldedSegmentError> {
        if raw[78..80] != [0_u8; 2] {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "row index entry reserved bytes are non-zero".to_owned(),
            ));
        }
        let row_flags = raw[77];
        if (row_flags & !ROW_KNOWN_FLAGS) != 0 {
            return Err(SettledShieldedSegmentError::InvalidFormat(format!(
                "row index entry has unsupported flags {row_flags:#04x}"
            )));
        }
        let pool_presence = PoolPresenceMask::from_bits(raw[76]).ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat(format!(
                "row index entry has reserved pool presence bits {:#04x}",
                raw[76]
            ))
        })?;
        Ok(Self {
            block_hash_wire: read_hash32(&raw[..32])?,
            payload_sha256: read_hash32(&raw[32..64])?,
            block_time: read_u32_be(&raw[64..68])?,
            slab_rel_offset: read_u32_be(&raw[68..72])?,
            native_bytes: read_u32_be(&raw[72..76])?,
            pool_presence,
            row_flags,
        })
    }
}

/// Compact `12`-byte in-memory per-block row descriptor retained by
/// [`super::SettledShieldedSegmentIndex`].
pub(super) struct CompactShieldedRow {
    pub(super) slab_rel_offset: u32,
    pub(super) native_bytes: u32,
    pub(super) pool_presence: PoolPresenceMask,
    pub(super) row_flags: u8,
    pub(super) validation_state: AtomicU8,
}

impl CompactShieldedRow {
    pub(super) fn from_entry(entry: &DecodedRowIndexEntry) -> Self {
        Self {
            slab_rel_offset: entry.slab_rel_offset,
            native_bytes: entry.native_bytes,
            pool_presence: entry.pool_presence,
            row_flags: entry.row_flags,
            validation_state: AtomicU8::new(VALIDATION_UNVALIDATED),
        }
    }

    pub(super) const fn has_unrepresentable_wire_fee(&self) -> bool {
        (self.row_flags & ROW_FLAG_UNREPRESENTABLE_WIRE_FEE) != 0
    }

    pub(super) fn validation_state(&self) -> u8 {
        self.validation_state.load(Ordering::Acquire)
    }

    pub(super) fn mark_valid(&self) {
        let _ = self.validation_state.compare_exchange(
            VALIDATION_UNVALIDATED,
            VALIDATION_VALID,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(super) fn mark_invalid(&self) {
        self.validation_state
            .store(VALIDATION_INVALID, Ordering::Release);
    }
}

pub(super) fn encode_frame_prefix(
    payload_len: usize,
) -> Result<[u8; FRAME_PREFIX_LEN], SettledShieldedSegmentError> {
    if payload_len == 0 || payload_len > SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES {
        return Err(SettledShieldedSegmentError::InvalidFormat(format!(
            "shielded frame payload length {payload_len} out of bounds"
        )));
    }
    let len_u32 = u32::try_from(payload_len).map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("shielded frame length overflow".to_owned())
    })?;
    let mut prefix = [0_u8; FRAME_PREFIX_LEN];
    prefix[0] = 0;
    prefix[1..FRAME_PREFIX_LEN].copy_from_slice(&len_u32.to_be_bytes());
    Ok(prefix)
}

pub(super) fn decode_frame_prefix(raw: &[u8]) -> Result<usize, SettledShieldedSegmentError> {
    if raw.len() < FRAME_PREFIX_LEN {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "short shielded segment frame prefix".to_owned(),
        ));
    }
    if raw[0] != 0 {
        return Err(SettledShieldedSegmentError::InvalidFormat(format!(
            "unexpected gRPC compression flag {:#04x} in segment frame",
            raw[0]
        )));
    }
    let len_bytes: [u8; 4] = raw[1..FRAME_PREFIX_LEN].try_into().map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("short shielded segment frame length".to_owned())
    })?;
    let payload_len = usize::try_from(u32::from_be_bytes(len_bytes)).map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat(
            "shielded frame length exceeds address space".to_owned(),
        )
    })?;
    if payload_len == 0 || payload_len > SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES {
        return Err(SettledShieldedSegmentError::InvalidFormat(format!(
            "shielded frame payload length {payload_len} out of bounds"
        )));
    }
    Ok(payload_len)
}

pub(super) fn check_contiguous_block(
    expected: &ShieldedSegmentIdentity,
    block: BlockId,
    prior: Option<BlockId>,
) -> Result<(), SettledShieldedSegmentError> {
    let expected_height = prior.map_or(Some(expected.start_height), |prev| prev.height.next());
    if expected_height != Some(block.height)
        || block.height > expected.end_block.height
        || (block.height == expected.end_block.height && block != expected.end_block)
    {
        return Err(SettledShieldedSegmentError::ChainLinkage(format!(
            "block {} ({:?}) does not extend segment [{}, {}]",
            block.height.value(),
            block.hash,
            expected.start_height.value(),
            expected.end_block.height.value()
        )));
    }
    Ok(())
}

pub(super) fn frame_span(
    rows: &[CompactShieldedRow],
    slab_len: u32,
    row_idx: usize,
) -> Result<(u32, u32), SettledShieldedSegmentError> {
    let start = rows
        .get(row_idx)
        .map(|row| row.slab_rel_offset)
        .ok_or_else(|| {
            SettledShieldedSegmentError::InvalidFormat("row index out of bounds".to_owned())
        })?;
    let end = rows
        .get(row_idx + 1)
        .map_or(slab_len, |next| next.slab_rel_offset);
    let len = end.checked_sub(start).ok_or_else(|| {
        SettledShieldedSegmentError::InvalidFormat("non-ascending slab offsets".to_owned())
    })?;
    if len <= FRAME_PREFIX_U32 {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "shielded segment frame is shorter than prefix".to_owned(),
        ));
    }
    Ok((start, len))
}

pub(super) fn validate_payload_limit(
    height: BlockHeight,
    payload_len: usize,
) -> Result<(), SettledShieldedSegmentError> {
    if payload_len > SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES {
        return Err(SettledShieldedSegmentError::PayloadLimitExceeded {
            block_height: height,
            payload_bytes: payload_len,
            max_bytes: SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
        });
    }
    Ok(())
}

pub(super) fn read_u16_be(slice: &[u8]) -> Result<u16, SettledShieldedSegmentError> {
    let bytes: [u8; 2] = slice.try_into().map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("truncated u16 field".to_owned())
    })?;
    Ok(u16::from_be_bytes(bytes))
}

pub(super) fn read_u32_be(slice: &[u8]) -> Result<u32, SettledShieldedSegmentError> {
    let bytes: [u8; 4] = slice.try_into().map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("truncated u32 field".to_owned())
    })?;
    Ok(u32::from_be_bytes(bytes))
}

pub(super) fn read_u64_be(slice: &[u8]) -> Result<u64, SettledShieldedSegmentError> {
    let bytes: [u8; 8] = slice.try_into().map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("truncated u64 field".to_owned())
    })?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn read_hash32(slice: &[u8]) -> Result<[u8; 32], SettledShieldedSegmentError> {
    slice.try_into().map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("truncated 32-byte hash field".to_owned())
    })
}
