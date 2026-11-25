//! Positional `pread`-backed reader and index/verity admission for `ZSTSUP01`.

use std::{fs::File, io, path::Path, sync::atomic::Ordering};

use sha2::{Digest as _, Sha256};
use zinder_core::{BlockHeight, CompactBlockArtifact};
use zinder_proto::compat::lightwalletd::EncodedBlock;
use zstd::bulk::Decompressor;

use super::{
    SettledSupplementSegmentError, SupplementCompressionCodec, SupplementSegmentIdentity,
    format::{
        CHUNK_INDEX_ENTRY_LEN, DecodedChunkIndexEntry, DecodedRowIndexEntry, HEADER_LEN,
        IndexedChunk, IndexedSupplementBlock, ROW_INDEX_ENTRY_LEN, STAGING_CHUNK, SegmentHeader,
        TRAILER_LEN, VALIDATION_INVALID, max_chunk_bytes, validate_block_payload_limit,
    },
    section::{
        BlockEnvelopeIdentity, BlockSupplementRecord, CanonicalBlockSupplement,
        CompactTransparentSupplementTx, block_hash_wire_to_domain, decode_block_record,
    },
    verity,
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
}

/// Authenticated positional reader for one settled `ZSTSUP01` supplement segment.
pub struct SettledSupplementSegmentReader {
    file: File,
    identity: SupplementSegmentIdentity,
    header: SegmentHeader,
    codec: SupplementCompressionCodec,
    chunks: Vec<IndexedChunk>,
    rows: Vec<IndexedSupplementBlock>,
    kernel_verified: bool,
}

impl SettledSupplementSegmentReader {
    /// Opens, admits, and binds one `ZSTSUP01` segment against `expected`.
    pub fn open(
        path: impl AsRef<Path>,
        expected: &SupplementSegmentIdentity,
        options: SettledSupplementSegmentOpenOptions,
    ) -> Result<Self, SettledSupplementSegmentError> {
        let file = verity::open_regular_file(path.as_ref())?;
        let file_len = file.metadata()?.len();
        let min_len = u64::try_from(HEADER_LEN + TRAILER_LEN).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("header size overflow".to_owned())
        })?;
        if file_len < min_len {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "supplement segment file truncated before header/trailer".to_owned(),
            ));
        }

        let mut header_bytes = [0_u8; HEADER_LEN];
        read_exact_at(&file, &mut header_bytes, 0)?;
        let header = SegmentHeader::decode(&header_bytes)?;
        let codec = header.codec()?;
        header.matches_identity(expected, file_len)?;

        let verity_verified =
            verity::check_pinned_verity(expected.fs_verity_sha256, verity::measure(&file)?)?;
        if options.require_fs_verity && !verity_verified {
            return Err(SettledSupplementSegmentError::FsVerityRequired);
        }

        let decoded = read_indexes_and_trailer(&file, &header)?;
        let tables = SegmentTables {
            chunks: &decoded.chunks,
            rows: &decoded.rows,
        };
        let pin = PortablePin {
            expected,
            trailer: &decoded.trailer,
        };
        if verity_verified {
            admit_protected(&header, tables, pin)?;
        } else {
            let raw_bytes = AdmittedHeaderAndIndexBytes {
                header: &header_bytes,
                raw_chunks: &decoded.raw_chunks,
                raw_rows: &decoded.raw_rows,
            };
            admit_portable(&file, &header, tables, raw_bytes, pin)?;
        }

        Ok(Self {
            file,
            identity: *expected,
            header,
            codec,
            chunks: decoded.chunks,
            rows: decoded.rows,
            kernel_verified: verity_verified,
        })
    }

    /// Returns the authenticated identity metadata for this opened segment.
    #[must_use]
    pub const fn identity(&self) -> &SupplementSegmentIdentity {
        &self.identity
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

    #[cfg(test)]
    pub(super) fn chunk_validation_state(&self, chunk_idx: usize) -> u8 {
        self.chunks[chunk_idx]
            .validation_state
            .load(Ordering::Acquire)
    }

    /// Returns `true` if `height` carries Tag-1 transparent supplement transactions.
    ///
    /// Fails closed if the target chunk or row has previously entered sticky invalid state.
    pub fn has_transparent_at(
        &self,
        height: BlockHeight,
    ) -> Result<bool, SettledSupplementSegmentError> {
        let row_idx = self.row_index_for(height)?;
        self.ensure_row_not_invalid(row_idx)?;
        Ok(self.rows[row_idx].has_transparent())
    }

    /// Reads and verifies the full decoded supplement record for `height`.
    pub fn read_block(
        &self,
        height: BlockHeight,
    ) -> Result<BlockSupplementRecord, SettledSupplementSegmentError> {
        let row_idx = self.row_index_for(height)?;
        self.ensure_row_not_invalid(row_idx)?;
        let chunk_idx = self.chunk_index_for_row(row_idx)?;
        let chunk_bytes = self.read_chunk_bytes(chunk_idx)?;
        self.decode_row_from_chunk(row_idx, &chunk_bytes)
    }

    /// Reads and verifies decoded supplement records across `[start_height, end_height]`.
    pub fn read_range(
        &self,
        start_height: BlockHeight,
        end_height: BlockHeight,
    ) -> Result<Vec<BlockSupplementRecord>, SettledSupplementSegmentError> {
        let (first_row, last_row) = self.row_range_for(start_height, end_height)?;
        let mut records = Vec::with_capacity(last_row - first_row + 1);
        let chunk_span = usize::from(self.header.chunk_blocks);
        let first_chunk = first_row / chunk_span;
        let last_chunk = last_row / chunk_span;
        for chunk_idx in first_chunk..=last_chunk {
            let chunk_row_start = (chunk_idx * chunk_span).max(first_row);
            let chunk_row_end = ((chunk_idx + 1) * chunk_span - 1).min(last_row);
            for row_idx in chunk_row_start..=chunk_row_end {
                self.ensure_row_not_invalid(row_idx)?;
            }
            let chunk_bytes = self.read_chunk_bytes(chunk_idx)?;
            for row_idx in chunk_row_start..=chunk_row_end {
                records.push(self.decode_row_from_chunk(row_idx, &chunk_bytes)?);
            }
        }
        Ok(records)
    }

    /// Reads and verifies only the Tag-1 transparent supplement vector for `height`.
    pub fn read_transparent_block(
        &self,
        height: BlockHeight,
    ) -> Result<Vec<CompactTransparentSupplementTx>, SettledSupplementSegmentError> {
        Ok(self.read_block(height)?.transparent_transactions)
    }

    /// Reads and verifies optional Tag-2 extensible canonical metadata for `height`.
    pub fn read_canonical_metadata(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CanonicalBlockSupplement>, SettledSupplementSegmentError> {
        Ok(self.read_block(height)?.canonical_metadata)
    }

    /// Splices one `ZSHRNG02` `Slab 0` `CompactBlock` payload at `height` with this
    /// segment's Tag-1 transparent supplement into one gRPC-framed [`EncodedBlock`].
    pub fn splice_block_wire(
        &self,
        height: BlockHeight,
        shielded_payload: &[u8],
        selection: CompactBlockSpliceSelection,
    ) -> Result<EncodedBlock, SettledSupplementSegmentError> {
        let row_idx = self.row_index_for(height)?;
        self.ensure_row_not_invalid(row_idx)?;
        let expected_envelope = Some(self.expected_envelope_identity(row_idx)?);
        let transparent = if splice_needs_tag1(selection, &self.rows[row_idx]) {
            self.read_block(height)?.transparent_transactions
        } else {
            Vec::new()
        };
        let mut frame = Vec::with_capacity(EncodedBlock::PREFIX_LEN + shielded_payload.len() + 64);
        let mut scratch = SpliceScratch::default();
        splice_compact_block_wire_into(
            shielded_payload,
            SpliceTarget {
                transparent: &transparent,
                selection,
                expected_envelope,
            },
            &mut frame,
            &mut scratch,
        )?;
        finish_encoded_frame(frame)
    }

    /// Splices a contiguous `ZSHRNG02` `Slab 0` payload slice starting at `start_height`
    /// into gRPC-framed [`EncodedBlock`] entries, decoding each touched chunk once.
    pub fn splice_range_wire<P: AsRef<[u8]>>(
        &self,
        start_height: BlockHeight,
        shielded_payloads: &[P],
        selection: CompactBlockSpliceSelection,
    ) -> Result<Vec<EncodedBlock>, SettledSupplementSegmentError> {
        if shielded_payloads.is_empty() {
            return Ok(Vec::new());
        }
        let end_height = self.end_height_for_count(start_height, shielded_payloads.len())?;
        let (first_row, last_row) = self.row_range_for(start_height, end_height)?;
        for row_idx in first_row..=last_row {
            self.ensure_row_not_invalid(row_idx)?;
        }

        let mut encoded_blocks = Vec::with_capacity(shielded_payloads.len());
        let mut scratch = SpliceScratch::default();
        let chunk_span = usize::from(self.header.chunk_blocks);
        let first_chunk = first_row / chunk_span;
        let last_chunk = last_row / chunk_span;
        for chunk_idx in first_chunk..=last_chunk {
            let chunk_row_start = (chunk_idx * chunk_span).max(first_row);
            let chunk_row_end = ((chunk_idx + 1) * chunk_span - 1).min(last_row);
            let need_chunk = (chunk_row_start..=chunk_row_end)
                .any(|idx| splice_needs_tag1(selection, &self.rows[idx]));
            let chunk_bytes = if need_chunk {
                Some(self.read_chunk_bytes(chunk_idx)?)
            } else {
                None
            };
            for row_idx in chunk_row_start..=chunk_row_end {
                let slice_idx = row_idx - first_row;
                encoded_blocks.push(self.splice_row(
                    SpliceRowInput {
                        row_idx,
                        shielded_payload: shielded_payloads[slice_idx].as_ref(),
                        chunk_bytes: chunk_bytes.as_deref(),
                    },
                    selection,
                    &mut scratch,
                )?);
            }
        }
        Ok(encoded_blocks)
    }

    /// Convenience wrapper around [`Self::splice_range_wire`] accepting gRPC-framed
    /// `ZSHRNG02` [`EncodedBlock`] slices directly.
    pub fn splice_encoded_range_wire(
        &self,
        start_height: BlockHeight,
        shielded_blocks: &[EncodedBlock],
        selection: CompactBlockSpliceSelection,
    ) -> Result<Vec<EncodedBlock>, SettledSupplementSegmentError> {
        let payloads: Vec<bytes::Bytes> =
            shielded_blocks.iter().map(EncodedBlock::payload).collect();
        self.splice_range_wire(start_height, &payloads, selection)
    }

    /// Losslessly reconstructs one [`CompactBlockArtifact`] at `height` by combining
    /// its `ZSHRNG02` `Slab 0` payload with this block's decoded supplement record.
    pub fn reconstruct_block(
        &self,
        height: BlockHeight,
        shielded_payload: &[u8],
    ) -> Result<CompactBlockArtifact, SettledSupplementSegmentError> {
        let row_idx = self.row_index_for(height)?;
        let record = self.read_block(height)?;
        let reconstructed = reconstruct_compact_block_artifact(shielded_payload, &record)?;
        self.verify_reconstructed_identity(row_idx, height, &reconstructed)?;
        Ok(reconstructed)
    }

    /// Losslessly reconstructs a contiguous slice of [`CompactBlockArtifact`] records
    /// starting at `start_height`.
    pub fn reconstruct_range<P: AsRef<[u8]>>(
        &self,
        start_height: BlockHeight,
        shielded_payloads: &[P],
    ) -> Result<Vec<CompactBlockArtifact>, SettledSupplementSegmentError> {
        if shielded_payloads.is_empty() {
            return Ok(Vec::new());
        }
        let end_height = self.end_height_for_count(start_height, shielded_payloads.len())?;
        let records = self.read_range(start_height, end_height)?;
        let mut artifacts = Vec::with_capacity(shielded_payloads.len());
        let base_row = self.row_index_for(start_height)?;
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
            let reconstructed = reconstruct_compact_block_artifact(payload.as_ref(), record)?;
            self.verify_reconstructed_identity(row_idx, height, &reconstructed)?;
            artifacts.push(reconstructed);
        }
        Ok(artifacts)
    }

    fn expected_envelope_identity(
        &self,
        row_idx: usize,
    ) -> Result<BlockEnvelopeIdentity, SettledSupplementSegmentError> {
        let row = self.rows.get(row_idx).ok_or_else(|| {
            SettledSupplementSegmentError::PayloadIntegrity(
                "supplement row index out of range".to_owned(),
            )
        })?;
        let offset_u32 = u32::try_from(row_idx).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("row index exceeds u32".to_owned())
        })?;
        let height_u32 = self
            .header
            .start_height
            .checked_add(offset_u32)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("row height overflow".to_owned())
            })?;
        let prev_hash_wire = if row_idx == 0 {
            self.header.first_prev_hash_wire
        } else {
            self.rows[row_idx - 1].block_hash_wire
        };
        Ok(BlockEnvelopeIdentity {
            height: BlockHeight::new(height_u32),
            block_hash_wire: row.block_hash_wire,
            prev_hash_wire,
            block_time: row.block_time,
        })
    }

    fn verify_reconstructed_identity(
        &self,
        row_idx: usize,
        expected_height: BlockHeight,
        reconstructed: &CompactBlockArtifact,
    ) -> Result<(), SettledSupplementSegmentError> {
        let expected = self.expected_envelope_identity(row_idx)?;
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
                start: self.identity.start_height,
                end: self.identity.end_block.height,
            })
    }

    fn splice_row(
        &self,
        input: SpliceRowInput<'_>,
        selection: CompactBlockSpliceSelection,
        scratch: &mut SpliceScratch,
    ) -> Result<EncodedBlock, SettledSupplementSegmentError> {
        let mut frame =
            Vec::with_capacity(EncodedBlock::PREFIX_LEN + input.shielded_payload.len() + 64);
        let expected_envelope = Some(self.expected_envelope_identity(input.row_idx)?);
        match input.chunk_bytes {
            Some(bytes) if splice_needs_tag1(selection, &self.rows[input.row_idx]) => {
                let record = self.decode_row_from_chunk(input.row_idx, bytes)?;
                splice_compact_block_wire_into(
                    input.shielded_payload,
                    SpliceTarget {
                        transparent: &record.transparent_transactions,
                        selection,
                        expected_envelope,
                    },
                    &mut frame,
                    scratch,
                )?;
            }
            Some(_) | None => {
                splice_compact_block_wire_into(
                    input.shielded_payload,
                    SpliceTarget {
                        transparent: &[],
                        selection,
                        expected_envelope,
                    },
                    &mut frame,
                    scratch,
                )?;
            }
        }
        finish_encoded_frame(frame)
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
            || self.rows[row_idx].validation_state.load(Ordering::Acquire) == VALIDATION_INVALID
        {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "supplement block chunk or row failed prior verification".to_owned(),
            ));
        }
        Ok(())
    }

    fn read_chunk_bytes(&self, chunk_idx: usize) -> Result<Vec<u8>, SettledSupplementSegmentError> {
        let chunk = &self.chunks[chunk_idx];
        if chunk.validation_state.load(Ordering::Acquire) == VALIDATION_INVALID {
            return Err(SettledSupplementSegmentError::PayloadIntegrity(
                "supplement chunk failed prior verification".to_owned(),
            ));
        }
        let compressed_len = usize::try_from(chunk.compressed_len).map_err(|_| {
            chunk.mark_invalid();
            SettledSupplementSegmentError::InvalidFormat(
                "compressed chunk exceeds usize".to_owned(),
            )
        })?;
        let uncompressed_len = usize::try_from(chunk.uncompressed_len).map_err(|_| {
            chunk.mark_invalid();
            SettledSupplementSegmentError::InvalidFormat(
                "uncompressed chunk exceeds usize".to_owned(),
            )
        })?;
        let abs_offset = self
            .header
            .data_offset
            .checked_add(chunk.data_rel_offset)
            .ok_or_else(|| {
                chunk.mark_invalid();
                SettledSupplementSegmentError::InvalidFormat(
                    "chunk file offset overflow".to_owned(),
                )
            })?;

        let mut compressed = vec![0_u8; compressed_len];
        if let Err(err) = read_exact_at(&self.file, &mut compressed, abs_offset) {
            chunk.mark_invalid();
            return Err(SettledSupplementSegmentError::Io(err));
        }
        let actual_sha: [u8; 32] = Sha256::digest(&compressed).into();
        if actual_sha != chunk.chunk_sha256 {
            chunk.mark_invalid();
            return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                "supplement chunk {chunk_idx} SHA-256 digest mismatch"
            )));
        }

        let decoded = match self.codec {
            SupplementCompressionCodec::None => compressed,
            SupplementCompressionCodec::Zstd => {
                decompress_exact_chunk(chunk_idx, chunk, &compressed, uncompressed_len)?
            }
        };
        chunk.mark_valid();
        Ok(decoded)
    }

    fn decode_row_from_chunk(
        &self,
        row_idx: usize,
        chunk_bytes: &[u8],
    ) -> Result<BlockSupplementRecord, SettledSupplementSegmentError> {
        let row = &self.rows[row_idx];
        let start = usize::try_from(row.chunk_rel_offset).map_err(|_| {
            row.mark_invalid();
            SettledSupplementSegmentError::InvalidFormat("row offset exceeds usize".to_owned())
        })?;
        let len = usize::try_from(row.uncompressed_len).map_err(|_| {
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
        if actual_sha != row.payload_sha256 {
            row.mark_invalid();
            return Err(SettledSupplementSegmentError::PayloadIntegrity(format!(
                "supplement block row {row_idx} payload SHA-256 mismatch"
            )));
        }

        let envelope = self
            .expected_envelope_identity(row_idx)
            .inspect_err(|_| row.mark_invalid())?;
        let decoded = decode_block_record(raw_row, envelope, row.row_flags);
        match decoded {
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
}

#[derive(Clone, Copy)]
struct SpliceRowInput<'a> {
    row_idx: usize,
    shielded_payload: &'a [u8],
    chunk_bytes: Option<&'a [u8]>,
}

#[derive(Clone, Copy)]
pub(super) struct SegmentTables<'a> {
    pub(super) chunks: &'a [IndexedChunk],
    pub(super) rows: &'a [IndexedSupplementBlock],
}

#[derive(Clone, Copy)]
pub(super) struct PortablePin<'a> {
    pub(super) expected: &'a SupplementSegmentIdentity,
    pub(super) trailer: &'a [u8; TRAILER_LEN],
}

#[derive(Clone, Copy)]
struct FileSpan {
    start: u64,
    len: u64,
}

const fn splice_needs_tag1(
    selection: CompactBlockSpliceSelection,
    row: &IndexedSupplementBlock,
) -> bool {
    row.has_transparent() && (selection.include_transparent || selection.retain_all_transactions)
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
    let mut decompressor = Decompressor::new().inspect_err(|_| chunk.mark_invalid())?;
    let written = decompressor
        .decompress_to_buffer(compressed, &mut decompressed)
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

pub(super) struct DecodedIndexData {
    pub(super) chunks: Vec<IndexedChunk>,
    pub(super) rows: Vec<IndexedSupplementBlock>,
    pub(super) raw_chunks: Vec<u8>,
    pub(super) raw_rows: Vec<u8>,
    pub(super) trailer: [u8; TRAILER_LEN],
}

#[derive(Clone, Copy)]
pub(super) struct AdmittedHeaderAndIndexBytes<'a> {
    pub(super) header: &'a [u8; HEADER_LEN],
    pub(super) raw_chunks: &'a [u8],
    pub(super) raw_rows: &'a [u8],
}

pub(super) fn read_indexes_and_trailer(
    file: &File,
    header: &SegmentHeader,
) -> Result<DecodedIndexData, SettledSupplementSegmentError> {
    let chunk_count = usize::try_from(header.chunk_count()?).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("chunk count exceeds usize".to_owned())
    })?;
    let row_count = usize::try_from(header.block_count).map_err(|_| {
        SettledSupplementSegmentError::InvalidFormat("block count exceeds usize".to_owned())
    })?;
    let chunk_bytes_len = chunk_count
        .checked_mul(CHUNK_INDEX_ENTRY_LEN)
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat(
                "chunk index byte count overflow".to_owned(),
            )
        })?;
    let row_bytes_len = row_count.checked_mul(ROW_INDEX_ENTRY_LEN).ok_or_else(|| {
        SettledSupplementSegmentError::InvalidFormat("row index byte count overflow".to_owned())
    })?;

    let mut raw_chunks = vec![0_u8; chunk_bytes_len];
    read_exact_at(file, &mut raw_chunks, header.chunk_index_offset)?;
    let (chunk_entries, chunk_rem) = raw_chunks.as_chunks::<CHUNK_INDEX_ENTRY_LEN>();
    if !chunk_rem.is_empty() {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "chunk index entry alignment error".to_owned(),
        ));
    }
    let mut chunks = Vec::with_capacity(chunk_count);
    for fixed in chunk_entries {
        chunks.push(IndexedChunk::from_entry(&DecodedChunkIndexEntry::decode(
            fixed,
        )?));
    }

    let mut raw_rows = vec![0_u8; row_bytes_len];
    read_exact_at(file, &mut raw_rows, header.row_index_offset)?;
    let (row_entries, row_rem) = raw_rows.as_chunks::<ROW_INDEX_ENTRY_LEN>();
    if !row_rem.is_empty() {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "row index entry alignment error".to_owned(),
        ));
    }
    let mut rows = Vec::with_capacity(row_count);
    for fixed in row_entries {
        rows.push(IndexedSupplementBlock::from_entry(
            &DecodedRowIndexEntry::decode(fixed)?,
        ));
    }

    let trailer_offset = header
        .row_index_offset
        .checked_add(u64::try_from(row_bytes_len).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("row index length exceeds u64".to_owned())
        })?)
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("trailer offset overflow".to_owned())
        })?;
    let mut trailer = [0_u8; TRAILER_LEN];
    read_exact_at(file, &mut trailer, trailer_offset)?;
    Ok(DecodedIndexData {
        chunks,
        rows,
        raw_chunks,
        raw_rows,
        trailer,
    })
}

pub(super) fn admit_protected(
    header: &SegmentHeader,
    tables: SegmentTables<'_>,
    pin: PortablePin<'_>,
) -> Result<(), SettledSupplementSegmentError> {
    if *pin.trailer != pin.expected.sha256 {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "supplement segment SHA-256 trailer disagrees with pinned identity".to_owned(),
        ));
    }
    let codec = header.codec()?;
    validate_indices(header, codec, tables.chunks, tables.rows, None)
}

pub(super) fn admit_portable(
    file: &File,
    header: &SegmentHeader,
    tables: SegmentTables<'_>,
    raw_bytes: AdmittedHeaderAndIndexBytes<'_>,
    pin: PortablePin<'_>,
) -> Result<(), SettledSupplementSegmentError> {
    let codec = header.codec()?;
    validate_indices(header, codec, tables.chunks, tables.rows, None)?;
    let expected_end = u64::try_from(raw_bytes.raw_rows.len())
        .ok()
        .and_then(|rows_len| header.row_index_offset.checked_add(rows_len));
    if header.data_offset != HEADER_LEN as u64
        || raw_bytes.raw_chunks.len() != tables.chunks.len() * CHUNK_INDEX_ENTRY_LEN
        || raw_bytes.raw_rows.len() != tables.rows.len() * ROW_INDEX_ENTRY_LEN
        || expected_end != Some(expected_trailer_offset(header)?)
    {
        return Err(SettledSupplementSegmentError::InvalidFormat(
            "admitted header or index slice sizes disagree with segment header".to_owned(),
        ));
    }

    let mut body_hasher = Sha256::new();
    body_hasher.update(raw_bytes.header);

    let mut scratch = vec![0_u8; STAGING_CHUNK];
    let mut chunk_digests = Vec::with_capacity(tables.chunks.len());
    for chunk in tables.chunks {
        let abs_offset = header
            .data_offset
            .checked_add(chunk.data_rel_offset)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat(
                    "chunk data offset overflow".to_owned(),
                )
            })?;
        let mut chunk_hasher = Sha256::new();
        hash_span_into(
            file,
            FileSpan {
                start: abs_offset,
                len: u64::from(chunk.compressed_len),
            },
            &mut scratch,
            &mut [&mut body_hasher, &mut chunk_hasher],
        )?;
        chunk_digests.push(chunk_hasher.finalize().into());
    }

    body_hasher.update(raw_bytes.raw_chunks);
    body_hasher.update(raw_bytes.raw_rows);

    let computed_body: [u8; 32] = body_hasher.finalize().into();
    if computed_body != *pin.trailer || computed_body != pin.expected.sha256 {
        return Err(SettledSupplementSegmentError::PayloadIntegrity(
            "supplement segment SHA-256 trailer or pinned digest mismatch".to_owned(),
        ));
    }

    validate_indices(
        header,
        codec,
        tables.chunks,
        tables.rows,
        Some(&chunk_digests),
    )
}

pub(super) fn validate_indices(
    header: &SegmentHeader,
    codec: SupplementCompressionCodec,
    chunks: &[IndexedChunk],
    rows: &[IndexedSupplementBlock],
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
    header: &SegmentHeader,
    codec: SupplementCompressionCodec,
    chunks: &[IndexedChunk],
) -> Result<(), SettledSupplementSegmentError> {
    let chunk_limit = max_chunk_bytes(header.chunk_blocks)?;
    let total_data_len = header.data_len()?;
    let mut expected_rel = 0_u64;
    for (idx, chunk) in chunks.iter().enumerate() {
        if chunk.compressed_len == 0 || chunk.compressed_len > chunk_limit {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "supplement chunk {idx} compressed length {} violates bounds [1, {chunk_limit}]",
                chunk.compressed_len
            )));
        }
        if chunk.uncompressed_len == 0 || chunk.uncompressed_len > chunk_limit {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "supplement chunk {idx} uncompressed length {} violates bounds [1, {chunk_limit}]",
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
    header: &SegmentHeader,
    chunks: &[IndexedChunk],
    rows: &[IndexedSupplementBlock],
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

fn expected_trailer_offset(header: &SegmentHeader) -> Result<u64, SettledSupplementSegmentError> {
    let row_bytes = u64::from(header.block_count)
        .checked_mul(u64::try_from(ROW_INDEX_ENTRY_LEN).unwrap_or(u64::MAX))
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("row index length overflow".to_owned())
        })?;
    header
        .row_index_offset
        .checked_add(row_bytes)
        .ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("trailer offset overflow".to_owned())
        })
}

fn hash_span_into(
    file: &File,
    span: FileSpan,
    scratch: &mut [u8],
    hashers: &mut [&mut Sha256],
) -> Result<(), SettledSupplementSegmentError> {
    let mut offset = span.start;
    let mut remaining = span.len;
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(scratch.len() as u64)).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("staging span exceeds usize".to_owned())
        })?;
        read_exact_at(file, &mut scratch[..wanted], offset)?;
        for hasher in hashers.iter_mut() {
            hasher.update(&scratch[..wanted]);
        }
        offset += wanted as u64;
        remaining -= wanted as u64;
    }
    Ok(())
}

fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt as _;
        file.read_exact_at(buf, offset)
    }
    #[cfg(not(unix))]
    {
        use std::os::windows::fs::FileExt as _;
        let mut read = 0;
        let mut cursor_offset = offset;
        while read < buf.len() {
            let count = file.seek_read(&mut buf[read..], cursor_offset)?;
            if count == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            read += count;
            cursor_offset += count as u64;
        }
        Ok(())
    }
}
