//! Binary framing, header, transaction slice table, and row index definitions for `ZSBLKB01`.

use std::sync::atomic::{AtomicU8, Ordering};

use zinder_core::{BlockId, wire::encode_internal_block_hash};

use super::{
    RawBlockCodec, RawBlockSegmentIdentity, SETTLED_RAW_BLOCK_SEGMENT_MAGIC,
    SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES, SettledRawBlockSegmentError,
};

pub(super) const HEADER_LEN: usize = 128;
pub(super) const FRAME_PREFIX_LEN: usize = 4;
pub(super) const FRAME_PREFIX_U32: u32 = 4;
pub(super) const TX_SLICE_ENTRY_LEN: usize = 40;
pub(super) const ROW_INDEX_ENTRY_LEN: usize = 128;
pub(super) const TRAILER_LEN: usize = 32;
pub(super) const MAX_SLAB_BYTES: u64 = u32::MAX as u64;
pub(super) const STAGING_CHUNK: usize = 256 * 1024;
pub(super) const ZSTD_COMPRESSION_LEVEL: i32 = 3;
/// Worst-case `Zstd` compressed frame payload upper bound for one `16 MiB` block.
pub(super) const MAX_COMPRESSED_PAYLOAD_BYTES: usize =
    SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES + 128 * 1024;

const _: () = assert!(FRAME_PREFIX_LEN == FRAME_PREFIX_U32 as usize);
const _: () = assert!(TX_SLICE_ENTRY_LEN == 40);
const _: () = assert!(ROW_INDEX_ENTRY_LEN == 128);

pub(super) const VALIDATION_UNVALIDATED: u8 = 0;
pub(super) const VALIDATION_VALID: u8 = 1;
pub(super) const VALIDATION_INVALID: u8 = 2;

/// Fixed 128-byte header of one `ZSBLKB01` segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SegmentHeader {
    pub(super) network_tag: u32,
    pub(super) segment_flags: u16,
    pub(super) codec_id: u16,
    pub(super) start_height: u32,
    pub(super) block_count: u32,
    pub(super) first_prev_hash_wire: [u8; 32],
    pub(super) last_block_hash_wire: [u8; 32],
    pub(super) slab_offset: u64,
    pub(super) tx_table_offset: u64,
    pub(super) row_index_offset: u64,
}

impl SegmentHeader {
    pub(super) fn encode(self) -> [u8; HEADER_LEN] {
        let mut header = [0_u8; HEADER_LEN];
        header[..8].copy_from_slice(SETTLED_RAW_BLOCK_SEGMENT_MAGIC);
        header[8..12].copy_from_slice(&self.network_tag.to_be_bytes());
        header[12..14].copy_from_slice(&self.segment_flags.to_be_bytes());
        header[14..16].copy_from_slice(&self.codec_id.to_be_bytes());
        header[16..20].copy_from_slice(&self.start_height.to_be_bytes());
        header[20..24].copy_from_slice(&self.block_count.to_be_bytes());
        header[24..56].copy_from_slice(&self.first_prev_hash_wire);
        header[56..88].copy_from_slice(&self.last_block_hash_wire);
        header[88..96].copy_from_slice(&self.slab_offset.to_be_bytes());
        header[96..104].copy_from_slice(&self.tx_table_offset.to_be_bytes());
        header[104..112].copy_from_slice(&self.row_index_offset.to_be_bytes());
        header
    }

    pub(super) fn decode(bytes: &[u8; HEADER_LEN]) -> Result<Self, SettledRawBlockSegmentError> {
        if &bytes[..8] != SETTLED_RAW_BLOCK_SEGMENT_MAGIC {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment magic mismatch".to_owned(),
            ));
        }
        if bytes[112..HEADER_LEN].iter().any(|byte| *byte != 0) {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment header reserved bytes are non-zero".to_owned(),
            ));
        }
        let header = Self {
            network_tag: read_u32_be(&bytes[8..12])?,
            segment_flags: read_u16_be(&bytes[12..14])?,
            codec_id: read_u16_be(&bytes[14..16])?,
            start_height: read_u32_be(&bytes[16..20])?,
            block_count: read_u32_be(&bytes[20..24])?,
            first_prev_hash_wire: read_hash32(&bytes[24..56])?,
            last_block_hash_wire: read_hash32(&bytes[56..88])?,
            slab_offset: read_u64_be(&bytes[88..96])?,
            tx_table_offset: read_u64_be(&bytes[96..104])?,
            row_index_offset: read_u64_be(&bytes[104..112])?,
        };
        validate_header_fields(&header)?;
        Ok(header)
    }

    pub(super) fn codec(self) -> Result<RawBlockCodec, SettledRawBlockSegmentError> {
        RawBlockCodec::from_id(self.codec_id).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(format!(
                "unsupported raw block segment codec_id {}",
                self.codec_id
            ))
        })
    }

    pub(super) fn slab_len(self) -> Result<u32, SettledRawBlockSegmentError> {
        let len = self
            .tx_table_offset
            .checked_sub(self.slab_offset)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat(
                    "raw block segment tx table precedes slab".to_owned(),
                )
            })?;
        u32::try_from(len).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment slab exceeds u32 offset space".to_owned(),
            )
        })
    }

    pub(super) fn tx_entry_count(self) -> Result<usize, SettledRawBlockSegmentError> {
        let byte_len = self
            .row_index_offset
            .checked_sub(self.tx_table_offset)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat(
                    "raw block segment row index precedes tx table".to_owned(),
                )
            })?;
        let stride = TX_SLICE_ENTRY_LEN as u64;
        if !byte_len.is_multiple_of(stride) {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment tx table length is not a multiple of entry size".to_owned(),
            ));
        }
        let count_u64 = byte_len / stride;
        let _count_u32 = u32::try_from(count_u64).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment tx table exceeds u32 entry count".to_owned(),
            )
        })?;
        usize::try_from(count_u64).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment tx table exceeds usize".to_owned(),
            )
        })
    }

    pub(super) fn matches_identity(
        self,
        expected: &RawBlockSegmentIdentity,
        file_len: u64,
    ) -> Result<(), SettledRawBlockSegmentError> {
        let expected_count = expected.block_count()?;
        if self.network_tag != expected.network.id()
            || self.codec_id != expected.codec.id()
            || self.start_height != expected.start_height.value()
            || self.block_count != expected_count
            || self.last_block_hash_wire != encode_internal_block_hash(expected.end_block.hash)
        {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment header does not match expected identity".to_owned(),
            ));
        }
        let expected_file_len = expected_total_file_len(self.row_index_offset, expected_count)?;
        if file_len != expected_file_len {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment file length does not match header and block count".to_owned(),
            ));
        }
        Ok(())
    }
}

fn validate_header_fields(header: &SegmentHeader) -> Result<(), SettledRawBlockSegmentError> {
    if header.segment_flags != 0 {
        return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
            "unsupported raw block segment flags {:#06x}",
            header.segment_flags
        )));
    }
    header.codec()?;
    if header.block_count == 0 {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "raw block segment block count must be positive".to_owned(),
        ));
    }
    if header.slab_offset != HEADER_LEN as u64 {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "raw block segment slab must start immediately after header".to_owned(),
        ));
    }
    header.slab_len()?;
    header.tx_entry_count()?;
    Ok(())
}

pub(super) fn expected_total_file_len(
    row_index_offset: u64,
    block_count: u32,
) -> Result<u64, SettledRawBlockSegmentError> {
    let index_bytes = u64::from(block_count)
        .checked_mul(ROW_INDEX_ENTRY_LEN as u64)
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("segment index size overflow".to_owned())
        })?;
    row_index_offset
        .checked_add(index_bytes)
        .and_then(|trailer_offset| trailer_offset.checked_add(TRAILER_LEN as u64))
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("segment total length overflow".to_owned())
        })
}

/// Decoded 40-byte transaction slice record in the flat transaction table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TxSliceEntry {
    pub(super) offset: u32,
    pub(super) len: u32,
    pub(super) sha256: [u8; 32],
}

impl TxSliceEntry {
    pub(super) fn encode(self) -> [u8; TX_SLICE_ENTRY_LEN] {
        let mut encoded = [0_u8; TX_SLICE_ENTRY_LEN];
        encoded[..4].copy_from_slice(&self.offset.to_be_bytes());
        encoded[4..8].copy_from_slice(&self.len.to_be_bytes());
        encoded[8..40].copy_from_slice(&self.sha256);
        encoded
    }

    pub(super) fn decode(
        raw: &[u8; TX_SLICE_ENTRY_LEN],
    ) -> Result<Self, SettledRawBlockSegmentError> {
        Ok(Self {
            offset: read_u32_be(&raw[..4])?,
            len: read_u32_be(&raw[4..8])?,
            sha256: read_hash32(&raw[8..40])?,
        })
    }
}

/// Decoded 128-byte on-disk row index record for one block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DecodedRowIndexEntry {
    pub(super) block_hash_wire: [u8; 32],
    pub(super) payload_sha256: [u8; 32],
    pub(super) uncompressed_sha256: [u8; 32],
    pub(super) slab_rel_offset: u32,
    pub(super) uncompressed_len: u32,
    pub(super) tx_start_index: u32,
    pub(super) tx_count: u32,
}

impl DecodedRowIndexEntry {
    pub(super) fn encode(self) -> [u8; ROW_INDEX_ENTRY_LEN] {
        let mut encoded = [0_u8; ROW_INDEX_ENTRY_LEN];
        encoded[..32].copy_from_slice(&self.block_hash_wire);
        encoded[32..64].copy_from_slice(&self.payload_sha256);
        encoded[64..96].copy_from_slice(&self.uncompressed_sha256);
        encoded[96..100].copy_from_slice(&self.slab_rel_offset.to_be_bytes());
        encoded[100..104].copy_from_slice(&self.uncompressed_len.to_be_bytes());
        encoded[104..108].copy_from_slice(&self.tx_start_index.to_be_bytes());
        encoded[108..112].copy_from_slice(&self.tx_count.to_be_bytes());
        encoded
    }

    pub(super) fn decode(
        raw: &[u8; ROW_INDEX_ENTRY_LEN],
    ) -> Result<Self, SettledRawBlockSegmentError> {
        if raw[112..ROW_INDEX_ENTRY_LEN] != [0_u8; 16] {
            return Err(SettledRawBlockSegmentError::InvalidFormat(
                "row index entry reserved bytes are non-zero".to_owned(),
            ));
        }
        Ok(Self {
            block_hash_wire: read_hash32(&raw[..32])?,
            payload_sha256: read_hash32(&raw[32..64])?,
            uncompressed_sha256: read_hash32(&raw[64..96])?,
            slab_rel_offset: read_u32_be(&raw[96..100])?,
            uncompressed_len: read_u32_be(&raw[100..104])?,
            tx_start_index: read_u32_be(&raw[104..108])?,
            tx_count: read_u32_be(&raw[108..112])?,
        })
    }
}

/// In-memory per-block index entry retained by [`super::SettledRawBlockSegmentReader`].
pub(super) struct IndexedBlock {
    pub(super) payload_sha256: [u8; 32],
    pub(super) uncompressed_sha256: [u8; 32],
    pub(super) slab_rel_offset: u32,
    pub(super) uncompressed_len: u32,
    pub(super) tx_start_index: u32,
    pub(super) tx_count: u32,
    pub(super) validation_state: AtomicU8,
}

impl IndexedBlock {
    pub(super) fn from_entry(entry: &DecodedRowIndexEntry) -> Self {
        Self {
            payload_sha256: entry.payload_sha256,
            uncompressed_sha256: entry.uncompressed_sha256,
            slab_rel_offset: entry.slab_rel_offset,
            uncompressed_len: entry.uncompressed_len,
            tx_start_index: entry.tx_start_index,
            tx_count: entry.tx_count,
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

pub(super) const fn max_stored_payload_bytes(codec: RawBlockCodec) -> usize {
    match codec {
        RawBlockCodec::None => SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES,
        RawBlockCodec::Zstd => MAX_COMPRESSED_PAYLOAD_BYTES,
    }
}

pub(super) fn encode_frame_prefix(
    payload_len: usize,
    codec: RawBlockCodec,
) -> Result<[u8; FRAME_PREFIX_LEN], SettledRawBlockSegmentError> {
    let max_bytes = max_stored_payload_bytes(codec);
    if payload_len == 0 || payload_len > max_bytes {
        return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
            "raw block frame payload length {payload_len} out of bounds"
        )));
    }
    let len_u32 = u32::try_from(payload_len).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("raw block frame length overflow".to_owned())
    })?;
    Ok(len_u32.to_be_bytes())
}

pub(super) fn decode_frame_prefix(
    raw: &[u8],
    codec: RawBlockCodec,
) -> Result<usize, SettledRawBlockSegmentError> {
    let prefix: [u8; FRAME_PREFIX_LEN] = raw
        .get(..FRAME_PREFIX_LEN)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "short raw block segment frame prefix".to_owned(),
            )
        })?;
    let payload_len = usize::try_from(u32::from_be_bytes(prefix)).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat(
            "raw block frame length exceeds address space".to_owned(),
        )
    })?;
    let max_bytes = max_stored_payload_bytes(codec);
    if payload_len == 0 || payload_len > max_bytes {
        return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
            "raw block frame payload length {payload_len} out of bounds"
        )));
    }
    Ok(payload_len)
}

pub(super) fn check_contiguous_block(
    expected: &RawBlockSegmentIdentity,
    block: BlockId,
    prior: Option<BlockId>,
) -> Result<(), SettledRawBlockSegmentError> {
    let expected_height = prior.map_or(Some(expected.start_height), |prev| prev.height.next());
    if expected_height != Some(block.height)
        || block.height > expected.end_block.height
        || (block.height == expected.end_block.height && block != expected.end_block)
    {
        return Err(SettledRawBlockSegmentError::ChainLinkage(format!(
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
    rows: &[IndexedBlock],
    slab_len: u32,
    row_idx: usize,
) -> Result<(u32, u32), SettledRawBlockSegmentError> {
    let start = rows
        .get(row_idx)
        .map(|row| row.slab_rel_offset)
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("row index out of bounds".to_owned())
        })?;
    let end = rows
        .get(row_idx + 1)
        .map_or(slab_len, |next| next.slab_rel_offset);
    let len = end.checked_sub(start).ok_or_else(|| {
        SettledRawBlockSegmentError::InvalidFormat("non-ascending slab offsets".to_owned())
    })?;
    if len <= FRAME_PREFIX_U32 {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "raw block segment frame is shorter than prefix".to_owned(),
        ));
    }
    Ok((start, len))
}

fn read_u16_be(slice: &[u8]) -> Result<u16, SettledRawBlockSegmentError> {
    let bytes: [u8; 2] = slice.try_into().map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("truncated u16 field".to_owned())
    })?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u32_be(slice: &[u8]) -> Result<u32, SettledRawBlockSegmentError> {
    let bytes: [u8; 4] = slice.try_into().map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("truncated u32 field".to_owned())
    })?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64_be(slice: &[u8]) -> Result<u64, SettledRawBlockSegmentError> {
    let bytes: [u8; 8] = slice.try_into().map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("truncated u64 field".to_owned())
    })?;
    Ok(u64::from_be_bytes(bytes))
}

fn read_hash32(slice: &[u8]) -> Result<[u8; 32], SettledRawBlockSegmentError> {
    slice.try_into().map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("truncated 32-byte hash field".to_owned())
    })
}
