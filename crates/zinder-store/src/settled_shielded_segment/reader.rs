//! Admission and zero-allocation range reader for `ZSHRNG02` segments.

use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
    sync::atomic::Ordering,
};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use zinder_core::{BlockHeight, BlockHeightRange};
use zinder_proto::compat::lightwalletd::EncodedBlock;

use crate::HeightOrder;

use super::{
    PoolPresenceMask, SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES, SettledShieldedSegmentError,
    ShieldedProjectionSelection, ShieldedSegmentIdentity,
    format::{
        DecodedRowIndexEntry, FRAME_PREFIX_LEN, FRAME_PREFIX_U32, HEADER_LEN, IndexedBlock,
        ROW_INDEX_ENTRY_LEN, SLAB_TARGET_BYTES, SegmentHeader, TRAILER_LEN, VALIDATION_INVALID,
        VALIDATION_VALID, decode_frame_prefix, frame_span,
    },
    verity,
    wire_projection::{
        ExpectedShieldedBlock, project_shielded_block_wire_into, synthesize_empty_nullifier_block,
        validate_shielded_block_wire,
    },
};

/// Admission options when opening a [`SettledShieldedSegmentReader`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SettledShieldedSegmentOpenOptions {
    /// Fail admission if Linux `fs-verity` is not active and verified on the file.
    pub require_fs_verity: bool,
}

/// Authenticated reader over one settled `ZSHRNG02` shielded segment.
pub struct SettledShieldedSegmentReader {
    identity: ShieldedSegmentIdentity,
    kernel_verified: bool,
    file: File,
    slab_offset: u64,
    slab_len: u32,
    first_prev_hash_wire: [u8; 32],
    rows: Vec<IndexedBlock>,
    /// Retained only when `!kernel_verified` so portable reads re-verify SHA-256.
    portable_payload_sha256: Option<Vec<[u8; 32]>>,
}

pub(super) struct PortableFrameDigest {
    rel_offset: u32,
    frame_len: u32,
    payload_sha256: [u8; 32],
}

struct ProjectedReadBuffers<'a> {
    packed_out: &'a mut Vec<u8>,
    tx_scratch: &'a mut Vec<u8>,
    out: &'a mut Vec<EncodedBlock>,
}

#[derive(Clone, Copy)]
struct SlabChunkView<'a> {
    bytes: &'a [u8],
    first_rel: u32,
}

struct OwnedSlabChunk {
    bytes: Vec<u8>,
    first_rel: u32,
}

impl SettledShieldedSegmentReader {
    /// Opens and authenticates a `ZSHRNG02` segment against `expected`.
    pub fn open(
        path: &Path,
        expected: &ShieldedSegmentIdentity,
        options: SettledShieldedSegmentOpenOptions,
    ) -> Result<Self, SettledShieldedSegmentError> {
        let file = verity::open_regular_file(path)?;
        let file_len = file.metadata()?.len();
        let measured_verity = verity::measure(&file)?;
        let kernel_verified =
            verity::check_pinned_verity(expected.fs_verity_sha256, measured_verity)?;
        if options.require_fs_verity && !kernel_verified {
            return Err(SettledShieldedSegmentError::FsVerityRequired);
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
    pub const fn identity(&self) -> ShieldedSegmentIdentity {
        self.identity
    }

    /// Returns the number of blocks stored in this segment.
    #[must_use]
    pub const fn block_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns the uncompressed canonical byte budget recorded for `height`.
    pub fn native_bytes_at(&self, height: BlockHeight) -> Result<u32, SettledShieldedSegmentError> {
        let range = self.resolve_index_range(BlockHeightRange::inclusive(height, height))?;
        Ok(self.rows[range.start].native_bytes)
    }

    /// Reads the default shielded `CompactBlock` frames across `range`.
    pub fn read_default_range(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
    ) -> Result<Vec<EncodedBlock>, SettledShieldedSegmentError> {
        self.read_projected_range(range, order, ShieldedProjectionSelection::DEFAULT_SHIELDED)
    }

    /// Reads and projects `CompactBlock` frames across `range` for `selection`.
    pub fn read_projected_range(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        selection: ShieldedProjectionSelection,
    ) -> Result<Vec<EncodedBlock>, SettledShieldedSegmentError> {
        let index_range = self.resolve_index_range(range)?;
        let mut out = Vec::with_capacity(index_range.len());
        let mut packed_out = Vec::new();
        let mut tx_scratch = Vec::new();
        let mut buffers = ProjectedReadBuffers {
            packed_out: &mut packed_out,
            tx_scratch: &mut tx_scratch,
            out: &mut out,
        };
        let mut idx = index_range.start;
        while idx < index_range.end {
            let row = &self.rows[idx];
            if row.validation_state.load(Ordering::Acquire) == VALIDATION_INVALID {
                return Err(SettledShieldedSegmentError::PayloadIntegrity(
                    "shielded segment frame previously failed validation".to_owned(),
                ));
            }
            if can_skip_slab(row, selection) {
                emit_synthesized_block(self, idx, buffers.packed_out)?;
                if buffers.packed_out.len() >= SLAB_TARGET_BYTES {
                    flush_packed_blocks(buffers.packed_out, buffers.out)?;
                }
                idx += 1;
                continue;
            }
            let span_end = find_slab_span_end(&self.rows, idx, index_range.end, selection);
            flush_packed_blocks(buffers.packed_out, buffers.out)?;
            self.read_slab_span(idx..span_end, selection, &mut buffers)?;
            idx = span_end;
        }
        flush_packed_blocks(buffers.packed_out, buffers.out)?;
        if order == HeightOrder::Descending {
            buffers.out.reverse();
        }
        Ok(out)
    }

    fn resolve_index_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<std::ops::Range<usize>, SettledShieldedSegmentError> {
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

    fn height_at(&self, row_idx: usize) -> Result<BlockHeight, SettledShieldedSegmentError> {
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

    fn prev_hash_wire_at(&self, row_idx: usize) -> &[u8; 32] {
        if row_idx == 0 {
            &self.first_prev_hash_wire
        } else {
            &self.rows[row_idx - 1].block_hash_wire
        }
    }

    fn read_slab_span(
        &self,
        span: std::ops::Range<usize>,
        selection: ShieldedProjectionSelection,
        buffers: &mut ProjectedReadBuffers<'_>,
    ) -> Result<(), SettledShieldedSegmentError> {
        let mut cursor = span.start;
        while cursor < span.end {
            let chunk = self.next_slab_chunk(cursor, span.end)?;
            self.process_slab_chunk(chunk.clone(), selection, buffers)?;
            cursor = chunk.end;
        }
        Ok(())
    }

    fn next_slab_chunk(
        &self,
        start_idx: usize,
        limit_idx: usize,
    ) -> Result<std::ops::Range<usize>, SettledShieldedSegmentError> {
        let mut end_idx = start_idx;
        let mut chunk_bytes = 0_usize;
        while end_idx < limit_idx {
            let (_, frame_len_u32) = frame_span(&self.rows, self.slab_len, end_idx)?;
            let frame_len = usize::try_from(frame_len_u32).map_err(|_| {
                SettledShieldedSegmentError::InvalidFormat("frame length overflow".to_owned())
            })?;
            if chunk_bytes != 0 && chunk_bytes + frame_len > SLAB_TARGET_BYTES {
                break;
            }
            chunk_bytes += frame_len;
            end_idx += 1;
        }
        Ok(start_idx..end_idx)
    }

    fn process_slab_chunk(
        &self,
        chunk: std::ops::Range<usize>,
        selection: ShieldedProjectionSelection,
        buffers: &mut ProjectedReadBuffers<'_>,
    ) -> Result<(), SettledShieldedSegmentError> {
        for idx in chunk.clone() {
            if self.rows[idx].validation_state.load(Ordering::Acquire) == VALIDATION_INVALID {
                return Err(SettledShieldedSegmentError::PayloadIntegrity(
                    "shielded segment frame previously failed validation".to_owned(),
                ));
            }
        }
        let first_rel = self.rows[chunk.start].slab_rel_offset;
        let (last_rel, last_len) = frame_span(&self.rows, self.slab_len, chunk.end - 1)?;
        let total_len = usize::try_from((last_rel + last_len) - first_rel).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("slab chunk length overflow".to_owned())
        })?;
        let slab_bytes = read_slab_at(
            &self.file,
            total_len,
            self.slab_offset + u64::from(first_rel),
        )
        .inspect_err(|_| {
            for idx in chunk.clone() {
                self.rows[idx].mark_invalid();
            }
        })?;

        let all_zero_copy = chunk
            .clone()
            .all(|idx| can_pass_through_zero_copy(self.rows[idx].pool_presence, selection));
        let all_projected = chunk
            .clone()
            .all(|idx| !can_pass_through_zero_copy(self.rows[idx].pool_presence, selection));

        for idx in chunk.clone() {
            let (rel, frame_len) = frame_span(&self.rows, self.slab_len, idx)?;
            let (local_start, local_end) = frame_bounds(rel, first_rel, frame_len)?;
            let frame = &slab_bytes[local_start..local_end];
            self.verify_frame(idx, frame)?;
        }

        if all_zero_copy {
            flush_packed_blocks(buffers.packed_out, buffers.out)?;
            emit_zero_copy_slab(&self.rows, self.slab_len, chunk, slab_bytes, buffers.out)?;
            return Ok(());
        }
        if all_projected {
            emit_projected_slab(
                self,
                chunk,
                SlabChunkView {
                    bytes: &slab_bytes,
                    first_rel,
                },
                selection,
                buffers,
            )?;
            return Ok(());
        }
        emit_mixed_slab(
            self,
            chunk,
            OwnedSlabChunk {
                bytes: slab_bytes,
                first_rel,
            },
            selection,
            buffers,
        )
    }

    fn verify_frame(
        &self,
        row_idx: usize,
        frame: &[u8],
    ) -> Result<(), SettledShieldedSegmentError> {
        let row = &self.rows[row_idx];
        let state = row.validation_state.load(Ordering::Acquire);
        if state == VALIDATION_INVALID {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "shielded segment frame previously failed validation".to_owned(),
            ));
        }
        let verdict = self.verify_frame_inner(row_idx, frame, state);
        if verdict.is_err() {
            row.mark_invalid();
        }
        verdict
    }

    fn verify_frame_inner(
        &self,
        row_idx: usize,
        frame: &[u8],
        state: u8,
    ) -> Result<(), SettledShieldedSegmentError> {
        let payload_len = decode_frame_prefix(frame)?;
        if FRAME_PREFIX_LEN + payload_len != frame.len() {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded frame prefix length disagrees with index".to_owned(),
            ));
        }
        let payload = &frame[FRAME_PREFIX_LEN..];
        if let Some(digests) = &self.portable_payload_sha256
            && <[u8; 32]>::from(Sha256::digest(payload)) != digests[row_idx]
        {
            return Err(SettledShieldedSegmentError::PayloadIntegrity(
                "portable segment payload SHA-256 mismatch on read".to_owned(),
            ));
        }
        if state != VALIDATION_VALID {
            let row = &self.rows[row_idx];
            let height = self.height_at(row_idx)?;
            let prev_hash_wire = self.prev_hash_wire_at(row_idx);
            validate_shielded_block_wire(
                payload,
                ExpectedShieldedBlock {
                    height,
                    block_hash_wire: &row.block_hash_wire,
                    prev_hash_wire,
                    block_time: row.block_time,
                    pool_presence: row.pool_presence,
                },
            )?;
            row.mark_valid();
        }
        Ok(())
    }
}

fn can_skip_slab(row: &IndexedBlock, selection: ShieldedProjectionSelection) -> bool {
    selection.nullifiers_only
        && row.validation_state.load(Ordering::Acquire) == VALIDATION_VALID
        && !row
            .pool_presence
            .intersects(selection.requested_pool_mask())
}

fn can_pass_through_zero_copy(
    presence: PoolPresenceMask,
    selection: ShieldedProjectionSelection,
) -> bool {
    let excluded = selection.requested_pool_mask().complement_known();
    !selection.nullifiers_only && !presence.intersects(excluded)
}

fn find_slab_span_end(
    rows: &[IndexedBlock],
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

fn emit_synthesized_block(
    reader: &SettledShieldedSegmentReader,
    row_idx: usize,
    packed_out: &mut Vec<u8>,
) -> Result<(), SettledShieldedSegmentError> {
    let height = reader.height_at(row_idx)?;
    let row = &reader.rows[row_idx];
    let prev_hash_wire = reader.prev_hash_wire_at(row_idx);
    synthesize_empty_nullifier_block(
        height,
        &row.block_hash_wire,
        prev_hash_wire,
        row.block_time,
        packed_out,
    )
}

fn emit_zero_copy_slab(
    rows: &[IndexedBlock],
    slab_len: u32,
    chunk: std::ops::Range<usize>,
    slab_bytes: Vec<u8>,
    out: &mut Vec<EncodedBlock>,
) -> Result<(), SettledShieldedSegmentError> {
    let first_rel = rows[chunk.start].slab_rel_offset;
    let slab_alloc = slab_bytes.capacity();
    let packed = Bytes::from(slab_bytes);
    for idx in chunk {
        let (rel, frame_len) = frame_span(rows, slab_len, idx)?;
        let (local_start, local_end) = frame_bounds(rel, first_rel, frame_len)?;
        let block =
            EncodedBlock::from_length_prefixed_in(packed.slice(local_start..local_end), slab_alloc)
                .map_err(|_| {
                    SettledShieldedSegmentError::InvalidFormat(
                        "shielded frame is not valid gRPC message".to_owned(),
                    )
                })?;
        out.push(block);
    }
    Ok(())
}

fn emit_projected_slab(
    reader: &SettledShieldedSegmentReader,
    chunk: std::ops::Range<usize>,
    slab: SlabChunkView<'_>,
    selection: ShieldedProjectionSelection,
    buffers: &mut ProjectedReadBuffers<'_>,
) -> Result<(), SettledShieldedSegmentError> {
    for idx in chunk {
        let (rel, frame_len) = frame_span(&reader.rows, reader.slab_len, idx)?;
        let (local_start, local_end) = frame_bounds(rel, slab.first_rel, frame_len)?;
        let payload = &slab.bytes[local_start + FRAME_PREFIX_LEN..local_end];
        project_shielded_block_wire_into(
            payload,
            selection,
            buffers.packed_out,
            buffers.tx_scratch,
        )?;
        if buffers.packed_out.len() >= SLAB_TARGET_BYTES {
            flush_packed_blocks(buffers.packed_out, buffers.out)?;
        }
    }
    Ok(())
}

fn emit_mixed_slab(
    reader: &SettledShieldedSegmentReader,
    chunk: std::ops::Range<usize>,
    slab: OwnedSlabChunk,
    selection: ShieldedProjectionSelection,
    buffers: &mut ProjectedReadBuffers<'_>,
) -> Result<(), SettledShieldedSegmentError> {
    let slab_alloc = slab.bytes.capacity();
    let packed = Bytes::from(slab.bytes);
    for idx in chunk {
        let (rel, frame_len) = frame_span(&reader.rows, reader.slab_len, idx)?;
        let (local_start, local_end) = frame_bounds(rel, slab.first_rel, frame_len)?;
        if can_pass_through_zero_copy(reader.rows[idx].pool_presence, selection) {
            flush_packed_blocks(buffers.packed_out, buffers.out)?;
            let block = EncodedBlock::from_length_prefixed_in(
                packed.slice(local_start..local_end),
                slab_alloc,
            )
            .map_err(|_| {
                SettledShieldedSegmentError::InvalidFormat(
                    "shielded frame is not valid gRPC message".to_owned(),
                )
            })?;
            buffers.out.push(block);
        } else {
            let payload = &packed[local_start + FRAME_PREFIX_LEN..local_end];
            project_shielded_block_wire_into(
                payload,
                selection,
                buffers.packed_out,
                buffers.tx_scratch,
            )?;
            if buffers.packed_out.len() >= SLAB_TARGET_BYTES {
                flush_packed_blocks(buffers.packed_out, buffers.out)?;
            }
        }
    }
    Ok(())
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
    let taken = std::mem::take(packed_out);
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

fn admit_protected(
    file: File,
    expected: &ShieldedSegmentIdentity,
    header: SegmentHeader,
) -> Result<SettledShieldedSegmentReader, SettledShieldedSegmentError> {
    let mut input = &file;
    input.seek(SeekFrom::Start(header.index_offset))?;
    let count = usize::try_from(header.block_count).map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("block count exceeds usize".to_owned())
    })?;
    let slab_len = header.slab_len()?;
    let entries = read_index_entries(&mut input, count)?;
    let mut trailer = [0_u8; TRAILER_LEN];
    input.read_exact(&mut trailer)?;
    if trailer != expected.sha256 {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(
            "protected shielded segment trailer SHA-256 mismatch".to_owned(),
        ));
    }
    let rows = validate_entries(&entries, slab_len, &header, None)?;
    Ok(SettledShieldedSegmentReader {
        identity: *expected,
        kernel_verified: true,
        file,
        slab_offset: header.slab_offset,
        slab_len,
        first_prev_hash_wire: header.first_prev_hash_wire,
        rows,
        portable_payload_sha256: None,
    })
}

fn admit_portable(
    file: File,
    expected: &ShieldedSegmentIdentity,
    header: SegmentHeader,
    header_bytes: &[u8; HEADER_LEN],
) -> Result<SettledShieldedSegmentReader, SettledShieldedSegmentError> {
    let mut buffered = BufReader::new(&file);
    let mut whole_digest = Sha256::new();
    whole_digest.update(header_bytes);
    let count = usize::try_from(header.block_count).map_err(|_| {
        SettledShieldedSegmentError::InvalidFormat("block count exceeds usize".to_owned())
    })?;
    let slab_len = header.slab_len()?;
    let streamed = hash_slab_frames(&mut buffered, count, slab_len, &mut whole_digest)?;
    let entries = hash_index_entries(&mut buffered, count, &mut whole_digest)?;
    let mut trailer = [0_u8; TRAILER_LEN];
    buffered.read_exact(&mut trailer)?;
    let computed: [u8; 32] = whole_digest.finalize().into();
    if !buffered.fill_buf()?.is_empty() || computed != expected.sha256 || trailer != computed {
        return Err(SettledShieldedSegmentError::PayloadIntegrity(
            "portable shielded segment length or SHA-256 mismatch".to_owned(),
        ));
    }
    let rows = validate_entries(&entries, slab_len, &header, Some(&streamed))?;
    let digests = entries.iter().map(|entry| entry.payload_sha256).collect();
    Ok(SettledShieldedSegmentReader {
        identity: *expected,
        kernel_verified: false,
        file,
        slab_offset: header.slab_offset,
        slab_len,
        first_prev_hash_wire: header.first_prev_hash_wire,
        rows,
        portable_payload_sha256: Some(digests),
    })
}

fn hash_slab_frames(
    input: &mut impl BufRead,
    count: usize,
    slab_len: u32,
    whole_digest: &mut Sha256,
) -> Result<Vec<PortableFrameDigest>, SettledShieldedSegmentError> {
    let mut frames = Vec::with_capacity(count);
    let mut rel_offset = 0_u64;
    for _ in 0..count {
        let mut prefix = [0_u8; FRAME_PREFIX_LEN];
        input.read_exact(&mut prefix)?;
        whole_digest.update(prefix);
        let payload_len = decode_frame_prefix(&prefix)?;
        let frame_len = u32::try_from(FRAME_PREFIX_LEN + payload_len).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("shielded frame exceeds u32".to_owned())
        })?;
        let next_offset = rel_offset + u64::from(frame_len);
        if next_offset > u64::from(slab_len) {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "streamed slab frames exceed slab length".to_owned(),
            ));
        }
        let payload_sha256 = hash_streamed_payload(input, payload_len, whole_digest)?;
        let rel_u32 = u32::try_from(rel_offset).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("slab offset exceeds u32".to_owned())
        })?;
        frames.push(PortableFrameDigest {
            rel_offset: rel_u32,
            frame_len,
            payload_sha256,
        });
        rel_offset = next_offset;
    }
    if rel_offset != u64::from(slab_len) {
        return Err(SettledShieldedSegmentError::InvalidFormat(
            "streamed slab bytes do not fill header slab length".to_owned(),
        ));
    }
    Ok(frames)
}

fn hash_streamed_payload(
    input: &mut impl BufRead,
    payload_len: usize,
    whole_digest: &mut Sha256,
) -> Result<[u8; 32], SettledShieldedSegmentError> {
    let mut payload_digest = Sha256::new();
    let mut remaining = payload_len;
    while remaining > 0 {
        let available = input.fill_buf()?;
        if available.is_empty() {
            return Err(SettledShieldedSegmentError::Io(
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

fn read_index_entries(
    input: &mut impl Read,
    count: usize,
) -> Result<Vec<DecodedRowIndexEntry>, SettledShieldedSegmentError> {
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
) -> Result<Vec<DecodedRowIndexEntry>, SettledShieldedSegmentError> {
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
    slab_len: u32,
    header: &SegmentHeader,
    streamed: Option<&[PortableFrameDigest]>,
) -> Result<Vec<IndexedBlock>, SettledShieldedSegmentError> {
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
        if let Some(frames) = streamed {
            let frame = &frames[idx];
            if frame.rel_offset != entry.slab_rel_offset
                || frame.frame_len != frame_len
                || frame.payload_sha256 != entry.payload_sha256
            {
                return Err(SettledShieldedSegmentError::PayloadIntegrity(
                    "streamed slab frame disagrees with row index".to_owned(),
                ));
            }
        }
        rows.push(IndexedBlock::from_entry(entry));
    }
    Ok(rows)
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
