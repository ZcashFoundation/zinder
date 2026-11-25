//! Binary framing, header, chunk index, and row index definitions for `ZSTSUP01`.

use std::sync::atomic::{AtomicU8, Ordering};

use zinder_core::{BlockHeight, BlockId, Network, wire::encode_internal_block_hash};

use super::{
    SETTLED_SUPPLEMENT_MAX_CHUNK_BLOCKS, SETTLED_SUPPLEMENT_SEGMENT_MAGIC,
    SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES, SettledSupplementSegmentError,
    SupplementCompressionCodec, SupplementSegmentIdentity,
};

pub(super) const HEADER_LEN: usize = 128;
pub(super) const CHUNK_INDEX_ENTRY_LEN: usize = 48;
pub(super) const ROW_INDEX_ENTRY_LEN: usize = 80;
pub(super) const TRAILER_LEN: usize = 32;
pub(super) const STAGING_CHUNK: usize = 256 * 1024;
pub(super) const ZSTD_COMPRESSION_LEVEL: i32 = 3;

pub(super) const ROW_FLAG_HAS_TRANSPARENT: u8 = 0x01;
pub(super) const ROW_FLAG_HAS_CANONICAL_METADATA: u8 = 0x02;
pub(super) const ROW_FLAG_HAS_WIDE_FEE: u8 = 0x04;
pub(super) const ROW_KNOWN_FLAGS: u8 =
    ROW_FLAG_HAS_TRANSPARENT | ROW_FLAG_HAS_CANONICAL_METADATA | ROW_FLAG_HAS_WIDE_FEE;

pub(super) const VALIDATION_UNVALIDATED: u8 = 0;
pub(super) const VALIDATION_VALID: u8 = 1;
pub(super) const VALIDATION_INVALID: u8 = 2;

const MAX_BLOCK_BYTES_U32: u32 = 16 * 1024 * 1024;
const _: () = assert!(
    (SETTLED_SUPPLEMENT_MAX_CHUNK_BLOCKS as u64) * (MAX_BLOCK_BYTES_U32 as u64) <= u32::MAX as u64
);
const _: () = assert!(MAX_BLOCK_BYTES_U32 as usize == SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES);

/// Fixed 128-byte header of one `ZSTSUP01` segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SegmentHeader {
    pub(super) network_tag: u32,
    pub(super) artifact_schema_version: u16,
    pub(super) segment_flags: u16,
    pub(super) codec_id: u16,
    pub(super) chunk_blocks: u16,
    pub(super) start_height: u32,
    pub(super) block_count: u32,
    pub(super) first_prev_hash_wire: [u8; 32],
    pub(super) last_block_hash_wire: [u8; 32],
    pub(super) data_offset: u64,
    pub(super) chunk_index_offset: u64,
    pub(super) row_index_offset: u64,
}

impl SegmentHeader {
    pub(super) fn encode(self) -> [u8; HEADER_LEN] {
        let mut header = [0_u8; HEADER_LEN];
        header[..8].copy_from_slice(SETTLED_SUPPLEMENT_SEGMENT_MAGIC);
        header[8..12].copy_from_slice(&self.network_tag.to_be_bytes());
        header[12..14].copy_from_slice(&self.artifact_schema_version.to_be_bytes());
        header[14..16].copy_from_slice(&self.segment_flags.to_be_bytes());
        header[16..18].copy_from_slice(&self.codec_id.to_be_bytes());
        header[18..20].copy_from_slice(&self.chunk_blocks.to_be_bytes());
        header[20..24].copy_from_slice(&self.start_height.to_be_bytes());
        header[24..28].copy_from_slice(&self.block_count.to_be_bytes());
        header[28..60].copy_from_slice(&self.first_prev_hash_wire);
        header[60..92].copy_from_slice(&self.last_block_hash_wire);
        header[92..100].copy_from_slice(&self.data_offset.to_be_bytes());
        header[100..108].copy_from_slice(&self.chunk_index_offset.to_be_bytes());
        header[108..116].copy_from_slice(&self.row_index_offset.to_be_bytes());
        header
    }

    pub(super) fn decode(bytes: &[u8; HEADER_LEN]) -> Result<Self, SettledSupplementSegmentError> {
        if &bytes[..8] != SETTLED_SUPPLEMENT_SEGMENT_MAGIC {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement segment magic mismatch".to_owned(),
            ));
        }
        if bytes[116..HEADER_LEN].iter().any(|byte| *byte != 0) {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement segment header reserved bytes are non-zero".to_owned(),
            ));
        }
        let header = Self {
            network_tag: read_u32_be(&bytes[8..12])?,
            artifact_schema_version: read_u16_be(&bytes[12..14])?,
            segment_flags: read_u16_be(&bytes[14..16])?,
            codec_id: read_u16_be(&bytes[16..18])?,
            chunk_blocks: read_u16_be(&bytes[18..20])?,
            start_height: read_u32_be(&bytes[20..24])?,
            block_count: read_u32_be(&bytes[24..28])?,
            first_prev_hash_wire: read_hash32(&bytes[28..60])?,
            last_block_hash_wire: read_hash32(&bytes[60..92])?,
            data_offset: read_u64_be(&bytes[92..100])?,
            chunk_index_offset: read_u64_be(&bytes[100..108])?,
            row_index_offset: read_u64_be(&bytes[108..116])?,
        };
        validate_header_fields(&header)?;
        Ok(header)
    }

    pub(super) fn codec(self) -> Result<SupplementCompressionCodec, SettledSupplementSegmentError> {
        SupplementCompressionCodec::from_id(self.codec_id)
    }

    pub(super) fn chunk_count(self) -> Result<u32, SettledSupplementSegmentError> {
        if self.chunk_blocks == 0 {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement segment chunk_blocks must be positive".to_owned(),
            ));
        }
        Ok(self.block_count.div_ceil(u32::from(self.chunk_blocks)))
    }

    pub(super) fn data_len(self) -> Result<u64, SettledSupplementSegmentError> {
        self.chunk_index_offset
            .checked_sub(self.data_offset)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat(
                    "supplement segment chunk index precedes data region".to_owned(),
                )
            })
    }

    pub(super) fn matches_identity(
        self,
        expected: &SupplementSegmentIdentity,
        file_len: u64,
    ) -> Result<(), SettledSupplementSegmentError> {
        let expected_count = expected.block_count()?;
        if self.network_tag != expected.network.id()
            || self.artifact_schema_version != expected.artifact_schema_version
            || self.start_height != expected.start_height.value()
            || self.block_count != expected_count
            || self.last_block_hash_wire != encode_internal_block_hash(expected.end_block.hash)
        {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement segment header does not match expected identity".to_owned(),
            ));
        }
        let expected_file_len = expected_total_file_len(self.row_index_offset, expected_count)?;
        if file_len != expected_file_len {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement segment file length does not match header and block count".to_owned(),
            ));
        }
        Ok(())
    }
}

fn validate_header_fields(header: &SegmentHeader) -> Result<(), SettledSupplementSegmentError> {
    if Network::from_id(header.network_tag).is_none() {
        return Err(SettledSupplementSegmentError::InvalidFormat(format!(
            "unknown network tag {}",
            header.network_tag
        )));
    }
    if header.segment_flags != 0 {
        return Err(SettledSupplementSegmentError::InvalidFormat(format!(
            "unsupported supplement segment flags {:#06x}",
            header.segment_flags
        )));
    }
    header.codec()?;
    if header.chunk_blocks == 0 || header.chunk_blocks > SETTLED_SUPPLEMENT_MAX_CHUNK_BLOCKS {
        return Err(SettledSupplementSegmentError::InvalidFormat(format!(
            "unsupported supplement chunk_blocks {}",
            header.chunk_blocks
        )));
    }
    if header.block_count == 0 {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "supplement segment block count must be positive".to_owned(),
        ));
    }
    if header.data_offset != HEADER_LEN as u64 {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "supplement segment data must start immediately after header".to_owned(),
        ));
    }
    header.data_len()?;
    let chunk_count = header.chunk_count()?;
    let chunk_index_bytes = u64::from(chunk_count)
        .checked_mul(CHUNK_INDEX_ENTRY_LEN as u64)
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("chunk index size overflow".to_owned())
        })?;
    let expected_row_offset = header
        .chunk_index_offset
        .checked_add(chunk_index_bytes)
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("row index offset overflow".to_owned())
        })?;
    if header.row_index_offset != expected_row_offset {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "supplement segment row index offset disagrees with chunk index size".to_owned(),
        ));
    }
    expected_total_file_len(header.row_index_offset, header.block_count)?;
    Ok(())
}

pub(super) fn expected_total_file_len(
    row_index_offset: u64,
    block_count: u32,
) -> Result<u64, SettledSupplementSegmentError> {
    let row_index_bytes = u64::from(block_count)
        .checked_mul(ROW_INDEX_ENTRY_LEN as u64)
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("row index size overflow".to_owned())
        })?;
    row_index_offset
        .checked_add(row_index_bytes)
        .and_then(|trailer_offset| trailer_offset.checked_add(TRAILER_LEN as u64))
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "supplement segment total length overflow".to_owned(),
            )
        })
}

pub(super) fn max_chunk_bytes(chunk_blocks: u16) -> Result<u32, SettledSupplementSegmentError> {
    if chunk_blocks == 0 || chunk_blocks > SETTLED_SUPPLEMENT_MAX_CHUNK_BLOCKS {
        return Err(SettledSupplementSegmentError::InvalidFormat(format!(
            "unsupported supplement chunk_blocks {chunk_blocks}"
        )));
    }
    u32::from(chunk_blocks)
        .checked_mul(MAX_BLOCK_BYTES_U32)
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("chunk byte limit overflow".to_owned())
        })
}

/// Decoded 48-byte on-disk chunk index record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DecodedChunkIndexEntry {
    pub(super) data_rel_offset: u64,
    pub(super) compressed_len: u32,
    pub(super) uncompressed_len: u32,
    pub(super) chunk_sha256: [u8; 32],
}

impl DecodedChunkIndexEntry {
    pub(super) fn encode(self) -> [u8; CHUNK_INDEX_ENTRY_LEN] {
        let mut encoded = [0_u8; CHUNK_INDEX_ENTRY_LEN];
        encoded[..8].copy_from_slice(&self.data_rel_offset.to_be_bytes());
        encoded[8..12].copy_from_slice(&self.compressed_len.to_be_bytes());
        encoded[12..16].copy_from_slice(&self.uncompressed_len.to_be_bytes());
        encoded[16..48].copy_from_slice(&self.chunk_sha256);
        encoded
    }

    pub(super) fn decode(
        raw: &[u8; CHUNK_INDEX_ENTRY_LEN],
    ) -> Result<Self, SettledSupplementSegmentError> {
        Ok(Self {
            data_rel_offset: read_u64_be(&raw[..8])?,
            compressed_len: read_u32_be(&raw[8..12])?,
            uncompressed_len: read_u32_be(&raw[12..16])?,
            chunk_sha256: read_hash32(&raw[16..48])?,
        })
    }
}

/// Decoded 80-byte on-disk per-block row index record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DecodedRowIndexEntry {
    pub(super) block_hash_wire: [u8; 32],
    pub(super) payload_sha256: [u8; 32],
    pub(super) chunk_rel_offset: u32,
    pub(super) uncompressed_len: u32,
    pub(super) block_time: u32,
    pub(super) row_flags: u8,
}

impl DecodedRowIndexEntry {
    pub(super) fn encode(self) -> [u8; ROW_INDEX_ENTRY_LEN] {
        let mut encoded = [0_u8; ROW_INDEX_ENTRY_LEN];
        encoded[..32].copy_from_slice(&self.block_hash_wire);
        encoded[32..64].copy_from_slice(&self.payload_sha256);
        encoded[64..68].copy_from_slice(&self.chunk_rel_offset.to_be_bytes());
        encoded[68..72].copy_from_slice(&self.uncompressed_len.to_be_bytes());
        encoded[72..76].copy_from_slice(&self.block_time.to_be_bytes());
        encoded[76] = self.row_flags;
        encoded
    }

    pub(super) fn decode(
        raw: &[u8; ROW_INDEX_ENTRY_LEN],
    ) -> Result<Self, SettledSupplementSegmentError> {
        if raw[77..80] != [0_u8; 3] {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement row index entry reserved bytes are non-zero".to_owned(),
            ));
        }
        let row_flags = raw[76];
        if (row_flags & !ROW_KNOWN_FLAGS) != 0 {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "supplement row index entry has reserved flag bits {row_flags:#04x}"
            )));
        }
        Ok(Self {
            block_hash_wire: read_hash32(&raw[..32])?,
            payload_sha256: read_hash32(&raw[32..64])?,
            chunk_rel_offset: read_u32_be(&raw[64..68])?,
            uncompressed_len: read_u32_be(&raw[68..72])?,
            block_time: read_u32_be(&raw[72..76])?,
            row_flags,
        })
    }
}

/// In-memory chunk metadata retained by [`super::SettledSupplementSegmentReader`].
pub(super) struct IndexedChunk {
    pub(super) data_rel_offset: u64,
    pub(super) compressed_len: u32,
    pub(super) uncompressed_len: u32,
    pub(super) chunk_sha256: [u8; 32],
    pub(super) validation_state: AtomicU8,
}

impl IndexedChunk {
    pub(super) fn from_entry(entry: &DecodedChunkIndexEntry) -> Self {
        Self {
            data_rel_offset: entry.data_rel_offset,
            compressed_len: entry.compressed_len,
            uncompressed_len: entry.uncompressed_len,
            chunk_sha256: entry.chunk_sha256,
            validation_state: AtomicU8::new(VALIDATION_UNVALIDATED),
        }
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

/// In-memory per-block row metadata retained by [`super::SettledSupplementSegmentReader`].
pub(super) struct IndexedSupplementBlock {
    pub(super) block_hash_wire: [u8; 32],
    pub(super) payload_sha256: [u8; 32],
    pub(super) chunk_rel_offset: u32,
    pub(super) uncompressed_len: u32,
    pub(super) block_time: u32,
    pub(super) row_flags: u8,
    pub(super) validation_state: AtomicU8,
}

impl IndexedSupplementBlock {
    pub(super) fn from_entry(entry: &DecodedRowIndexEntry) -> Self {
        Self {
            block_hash_wire: entry.block_hash_wire,
            payload_sha256: entry.payload_sha256,
            chunk_rel_offset: entry.chunk_rel_offset,
            uncompressed_len: entry.uncompressed_len,
            block_time: entry.block_time,
            row_flags: entry.row_flags,
            validation_state: AtomicU8::new(VALIDATION_UNVALIDATED),
        }
    }

    pub(super) const fn has_transparent(&self) -> bool {
        (self.row_flags & ROW_FLAG_HAS_TRANSPARENT) != 0
    }

    pub(super) const fn has_wide_fee(&self) -> bool {
        (self.row_flags & ROW_FLAG_HAS_WIDE_FEE) != 0
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

pub(super) fn check_contiguous_block(
    expected: &SupplementSegmentIdentity,
    block: BlockId,
    prior: Option<BlockId>,
) -> Result<(), SettledSupplementSegmentError> {
    let expected_height = prior.map_or(Some(expected.start_height), |prev| prev.height.next());
    if expected_height != Some(block.height)
        || block.height > expected.end_block.height
        || (block.height == expected.end_block.height && block != expected.end_block)
    {
        return Err(SettledSupplementSegmentError::ChainLinkage(format!(
            "block {} ({:?}) does not extend segment [{}, {}]",
            block.height.value(),
            block.hash,
            expected.start_height.value(),
            expected.end_block.height.value()
        )));
    }
    Ok(())
}

pub(super) fn validate_block_payload_limit(
    height: BlockHeight,
    payload_len: usize,
) -> Result<(), SettledSupplementSegmentError> {
    if payload_len > SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES {
        return Err(SettledSupplementSegmentError::PayloadLimitExceeded {
            block_height: height,
            payload_bytes: payload_len,
            max_bytes: SETTLED_SUPPLEMENT_SEGMENT_MAX_BLOCK_BYTES,
        });
    }
    Ok(())
}

pub(super) fn read_u16_be(slice: &[u8]) -> Result<u16, SettledSupplementSegmentError> {
    let bytes: [u8; 2] = slice.try_into().map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("truncated u16 field".to_owned())
    })?;
    Ok(u16::from_be_bytes(bytes))
}

pub(super) fn read_u32_be(slice: &[u8]) -> Result<u32, SettledSupplementSegmentError> {
    let bytes: [u8; 4] = slice.try_into().map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("truncated u32 field".to_owned())
    })?;
    Ok(u32::from_be_bytes(bytes))
}

pub(super) fn read_u64_be(slice: &[u8]) -> Result<u64, SettledSupplementSegmentError> {
    let bytes: [u8; 8] = slice.try_into().map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("truncated u64 field".to_owned())
    })?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn read_hash32(slice: &[u8]) -> Result<[u8; 32], SettledSupplementSegmentError> {
    slice.try_into().map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("truncated 32-byte hash field".to_owned())
    })
}
