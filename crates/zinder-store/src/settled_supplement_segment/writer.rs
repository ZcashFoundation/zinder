//! Atomic, `fs-verity`-protected writer for `ZSTSUP01` settled supplement segments.

use std::{
    fs::File,
    io::{self, BufReader, Read as _, Seek as _, SeekFrom, Write as _},
    path::{Path, PathBuf},
};

use sha2::{Digest as _, Sha256};
use tempfile::{NamedTempFile, TempPath};
use zinder_core::{
    BlockHash, BlockId, CanonicalBlockFacts, CompactBlockArtifact, wire::encode_internal_block_hash,
};
use zstd::bulk::Compressor;

use crate::segment_verity;

use super::{
    SETTLED_SUPPLEMENT_DEFAULT_CHUNK_BLOCKS, SETTLED_SUPPLEMENT_MAX_CHUNK_BLOCKS,
    SettledSupplementSegmentError, SettledSupplementSegmentOpenOptions,
    SettledSupplementSegmentReader, SupplementCompressionCodec, SupplementSegmentHeader,
    SupplementSegmentIdentity,
    format::{
        CHUNK_INDEX_ENTRY_LEN, DecodedChunkIndexEntry, DecodedRowIndexEntry, HEADER_LEN,
        ROW_INDEX_ENTRY_LEN, STAGING_CHUNK, ZSTD_COMPRESSION_LEVEL, check_contiguous_block,
        max_chunk_bytes, max_compressed_chunk_bytes, validate_block_payload_limit,
    },
    parent_directory,
    section::{
        BlockEnvelopeIdentity, BlockSupplementRecord, CanonicalBlockSupplement,
        encode_block_record, transparent_supplement_from_compact,
    },
};

/// Tuning options controlling how a new `ZSTSUP01` segment is encoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SettledSupplementSegmentWriteOptions {
    /// Compression codec applied to each chunk frame.
    pub codec: SupplementCompressionCodec,
    /// Number of contiguous blocks packed into each chunk (`1..=255`).
    pub chunk_blocks: u16,
}

impl Default for SettledSupplementSegmentWriteOptions {
    fn default() -> Self {
        Self {
            codec: SupplementCompressionCodec::Zstd,
            chunk_blocks: SETTLED_SUPPLEMENT_DEFAULT_CHUNK_BLOCKS,
        }
    }
}

enum StagedTarget {
    Temp(NamedTempFile),
    OwnedFile(File),
}

struct FinalizedStagedBody {
    target: StagedTarget,
    trailer: [u8; 32],
    index_sha256: [u8; 32],
    verity_digest: [u8; 32],
}

impl StagedTarget {
    fn as_file(&self) -> &File {
        match self {
            Self::Temp(temp) => temp.as_file(),
            Self::OwnedFile(file) => file,
        }
    }

    fn as_file_mut(&mut self) -> &mut File {
        match self {
            Self::Temp(temp) => temp.as_file_mut(),
            Self::OwnedFile(file) => file,
        }
    }
}

/// Streaming writer that constructs one immutable `ZSTSUP01` segment.
pub struct SettledSupplementSegmentWriter {
    target: PathBuf,
    identity: SupplementSegmentIdentity,
    options: SettledSupplementSegmentWriteOptions,
    staged_file: Option<StagedTarget>,
    compressor: Option<Compressor<'static>>,
    first_prev_hash_wire: Option<[u8; 32]>,
    last_block: Option<BlockId>,
    data_cursor: u64,
    pending_chunk: Vec<u8>,
    pending_chunk_blocks: u16,
    chunk_index: Vec<DecodedChunkIndexEntry>,
    row_index: Vec<DecodedRowIndexEntry>,
    #[cfg(test)]
    fail_flush_after_partial_bytes: std::cell::Cell<Option<usize>>,
}

impl SettledSupplementSegmentWriter {
    /// Creates a staged writer targeting `path` for one contiguous settled span.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on invalid `expected` intervals,
    /// unsupported `chunk_blocks`, or temp-file I/O failure.
    pub fn create(
        path: impl AsRef<Path>,
        expected: SupplementSegmentIdentity,
        options: SettledSupplementSegmentWriteOptions,
    ) -> Result<Self, SettledSupplementSegmentError> {
        let target = path.as_ref().to_path_buf();
        let staged_file = NamedTempFile::new_in(parent_directory(&target))?;
        Self::create_target(target, StagedTarget::Temp(staged_file), expected, options)
    }

    /// Creates a staged `ZSTSUP01` writer backed directly by a caller-owned `File`.
    pub(crate) fn create_in_file(
        staged_file: File,
        expected: SupplementSegmentIdentity,
        options: SettledSupplementSegmentWriteOptions,
    ) -> Result<Self, SettledSupplementSegmentError> {
        Self::create_target(
            PathBuf::new(),
            StagedTarget::OwnedFile(staged_file),
            expected,
            options,
        )
    }

    fn create_target(
        target: PathBuf,
        mut staged_file: StagedTarget,
        expected: SupplementSegmentIdentity,
        options: SettledSupplementSegmentWriteOptions,
    ) -> Result<Self, SettledSupplementSegmentError> {
        let block_count = expected.block_count()?;
        if options.chunk_blocks == 0 || options.chunk_blocks > SETTLED_SUPPLEMENT_MAX_CHUNK_BLOCKS {
            return Err(SettledSupplementSegmentError::InvalidFormat(format!(
                "unsupported supplement chunk_blocks {}",
                options.chunk_blocks
            )));
        }
        staged_file.as_file_mut().write_all(&[0_u8; HEADER_LEN])?;

        let compressor = match options.codec {
            SupplementCompressionCodec::None => None,
            SupplementCompressionCodec::Zstd => Some(Compressor::new(ZSTD_COMPRESSION_LEVEL)?),
        };
        let chunk_count = usize::try_from(block_count.div_ceil(u32::from(options.chunk_blocks)))
            .map_err(|_| {
                SettledSupplementSegmentError::InvalidFormat(
                    "chunk count exceeds host address space".to_owned(),
                )
            })?;
        let row_capacity = usize::try_from(block_count).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat(
                "block count exceeds host address space".to_owned(),
            )
        })?;

        Ok(Self {
            target,
            identity: expected,
            options,
            staged_file: Some(staged_file),
            compressor,
            first_prev_hash_wire: None,
            last_block: None,
            data_cursor: 0,
            pending_chunk: Vec::with_capacity(64 * 1024),
            pending_chunk_blocks: 0,
            chunk_index: Vec::with_capacity(chunk_count),
            row_index: Vec::with_capacity(row_capacity),
            #[cfg(test)]
            fail_flush_after_partial_bytes: std::cell::Cell::new(None),
        })
    }

    #[cfg(test)]
    pub(super) fn inject_partial_write_failure(&self, partial_bytes: usize) {
        self.fail_flush_after_partial_bytes.set(Some(partial_bytes));
    }

    /// Appends the next contiguous block derived from `block` and optional
    /// [`CanonicalBlockFacts`].
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] if `canonical_facts` disagrees
    /// with `block`, linkage fails, payload size exceeds `16 MiB`, or writing fails.
    pub fn push(
        &mut self,
        block: &CompactBlockArtifact,
        canonical_facts: Option<&CanonicalBlockFacts>,
    ) -> Result<(), SettledSupplementSegmentError> {
        let canonical_metadata = canonical_facts
            .map(|facts| {
                check_canonical_facts_header(block, facts)?;
                CanonicalBlockSupplement::from_canonical_facts(facts, Some(block))
            })
            .transpose()?;
        let record = BlockSupplementRecord {
            transparent_transactions: transparent_supplement_from_compact(block),
            canonical_metadata,
        };
        self.push_record(
            BlockId::new(block.height(), block.block_hash()),
            block.previous_block_hash(),
            block.time(),
            &record,
        )
    }

    /// Appends the next contiguous block using an explicit
    /// [`BlockSupplementRecord`].
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on writer poisoning, linkage
    /// errors, payload overflow, or I/O/compression failure.
    pub fn push_record(
        &mut self,
        block_id: BlockId,
        prev_hash: BlockHash,
        block_time: u32,
        record: &BlockSupplementRecord,
    ) -> Result<(), SettledSupplementSegmentError> {
        if self.staged_file.is_none() {
            return Err(writer_poisoned());
        }
        check_contiguous_block(&self.identity, block_id, self.last_block)?;
        if let Some(prior) = self.last_block
            && prev_hash != prior.hash
        {
            return Err(SettledSupplementSegmentError::ChainLinkage(format!(
                "block {} prev_hash does not match prior block {} hash",
                block_id.height.value(),
                prior.height.value()
            )));
        }
        let block_hash_wire = encode_internal_block_hash(block_id.hash);
        let prev_hash_wire = encode_internal_block_hash(prev_hash);

        let (encoded, row_flags) = encode_block_record(
            BlockEnvelopeIdentity {
                height: block_id.height,
                block_hash_wire,
                prev_hash_wire,
                block_time,
            },
            record,
        )?;
        validate_block_payload_limit(block_id.height, encoded.len())?;

        let chunk_rel_offset = u32::try_from(self.pending_chunk.len()).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat(
                "pending chunk length exceeds u32".to_owned(),
            )
        })?;
        let uncompressed_len = u32::try_from(encoded.len()).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat(
                "block record length exceeds u32".to_owned(),
            )
        })?;
        let payload_sha256: [u8; 32] = Sha256::digest(&encoded).into();
        if self.row_index.is_empty() {
            self.first_prev_hash_wire = Some(prev_hash_wire);
        }
        self.pending_chunk.extend_from_slice(&encoded);
        self.pending_chunk_blocks = self.pending_chunk_blocks.saturating_add(1);
        self.row_index.push(DecodedRowIndexEntry {
            block_hash_wire,
            payload_sha256,
            chunk_rel_offset,
            uncompressed_len,
            block_time,
            row_flags,
        });
        self.last_block = Some(block_id);
        if self.pending_chunk_blocks == self.options.chunk_blocks {
            self.flush_pending_chunk()?;
        }
        Ok(())
    }

    /// Finalizes indexes, SHA-256 trailer, `index_sha256` pin, `fs-verity`,
    /// and atomic target placement.
    ///
    /// # Errors
    ///
    /// Returns [`SettledSupplementSegmentError`] on incomplete spans, hashing,
    /// `fs-verity`, or atomic file persistence failures.
    pub fn finish(mut self) -> Result<SupplementSegmentIdentity, SettledSupplementSegmentError> {
        let FinalizedStagedBody {
            target: staged_target,
            trailer,
            index_sha256,
            verity_digest,
        } = self.finalize_staged_body()?;
        let StagedTarget::Temp(staged_file) = staged_target else {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "caller-owned staged supplement writer must use finish_staged_file".to_owned(),
            ));
        };

        let readonly_staged = segment_verity::reopen_staged(&staged_file, "supplement segment")?;
        let (writable_file, temp_path) = staged_file.into_parts();
        drop(writable_file);
        segment_verity::protect_new_segment(
            &readonly_staged,
            Some(verity_digest),
            "supplement segment",
        )?;
        drop(readonly_staged);

        let identity = SupplementSegmentIdentity {
            sha256: trailer,
            index_sha256,
            fs_verity_sha256: Some(verity_digest),
            ..self.identity
        };
        persist_segment(temp_path, &self.target, &identity)?;
        File::open(parent_directory(&self.target))?.sync_all()?;
        Ok(identity)
    }

    /// Finalizes indexes, trailer, and sync on a caller-owned staged file,
    /// returning its Unix `(dev, ino)` and complete identity (including `index_sha256`)
    /// before dropping the writable descriptor.
    pub(crate) fn finish_staged_file(
        mut self,
    ) -> Result<((u64, u64), SupplementSegmentIdentity), SettledSupplementSegmentError> {
        let FinalizedStagedBody {
            target: staged_target,
            trailer,
            index_sha256,
            verity_digest,
        } = self.finalize_staged_body()?;
        let StagedTarget::OwnedFile(staged_file) = staged_target else {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "path-backed staged supplement writer must use finish".to_owned(),
            ));
        };
        let dev_ino = staged_file_dev_ino(&staged_file)?;
        drop(staged_file);
        let identity = SupplementSegmentIdentity {
            sha256: trailer,
            index_sha256,
            fs_verity_sha256: Some(verity_digest),
            ..self.identity
        };
        Ok((dev_ino, identity))
    }

    fn finalize_staged_body(
        &mut self,
    ) -> Result<FinalizedStagedBody, SettledSupplementSegmentError> {
        if self.staged_file.is_none() {
            return Err(writer_poisoned());
        }
        let block_count = self.identity.block_count()?;
        let row_len = u32::try_from(self.row_index.len()).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat("row index length exceeds u32".to_owned())
        })?;
        if row_len != block_count || self.last_block != Some(self.identity.end_block) {
            return Err(SettledSupplementSegmentError::ChainLinkage(format!(
                "incomplete supplement segment: wrote {} of {block_count} blocks",
                self.row_index.len()
            )));
        }
        let first_prev_hash_wire = self.first_prev_hash_wire.ok_or_else(|| {
            SettledSupplementSegmentError::ChainLinkage(
                "supplement segment received zero blocks".to_owned(),
            )
        })?;
        if self.pending_chunk_blocks > 0 {
            self.flush_pending_chunk()?;
        }

        let mut staged_file = self.staged_file.take().ok_or_else(writer_poisoned)?;
        let (trailer_offset, header_bytes) = self.write_indexes_and_header(
            staged_file.as_file_mut(),
            block_count,
            first_prev_hash_wire,
        )?;
        let (trailer, verity_digest) = hash_staged(staged_file.as_file(), trailer_offset)?;
        write_all_at(staged_file.as_file(), &trailer, trailer_offset)?;
        staged_file.as_file().sync_all()?;

        let index_sha256 =
            compute_index_sha256(&header_bytes, &self.chunk_index, &self.row_index, &trailer);
        Ok(FinalizedStagedBody {
            target: staged_file,
            trailer,
            index_sha256,
            verity_digest,
        })
    }

    fn flush_pending_chunk(&mut self) -> Result<(), SettledSupplementSegmentError> {
        let mut staged_file = self.staged_file.take().ok_or_else(writer_poisoned)?;
        let uncompressed_len = u32::try_from(self.pending_chunk.len()).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat(
                "chunk uncompressed length exceeds u32".to_owned(),
            )
        })?;
        let uncompressed_limit = max_chunk_bytes(self.options.chunk_blocks)?;
        let compressed_limit =
            max_compressed_chunk_bytes(self.options.chunk_blocks, self.options.codec)?;
        if uncompressed_len == 0 || uncompressed_len > uncompressed_limit {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "chunk uncompressed length outside admitted bounds".to_owned(),
            ));
        }

        let compressed_buf = self
            .compressor
            .as_mut()
            .map(|compressor| compressor.compress(&self.pending_chunk))
            .transpose()?;
        let frame = compressed_buf.as_deref().unwrap_or(&self.pending_chunk);
        let compressed_len = u32::try_from(frame.len()).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat(
                "compressed chunk length exceeds u32".to_owned(),
            )
        })?;
        if compressed_len == 0 || compressed_len > compressed_limit {
            return Err(SettledSupplementSegmentError::InvalidFormat(
                "compressed chunk length outside admitted bounds".to_owned(),
            ));
        }

        let chunk_sha256: [u8; 32] = Sha256::digest(frame).into();
        #[cfg(test)]
        write_chunk_frame(
            staged_file.as_file_mut(),
            frame,
            &self.fail_flush_after_partial_bytes,
        )?;
        #[cfg(not(test))]
        write_chunk_frame(staged_file.as_file_mut(), frame)?;
        let next_data_cursor = self
            .data_cursor
            .checked_add(u64::from(compressed_len))
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat(
                    "chunk data cursor overflow".to_owned(),
                )
            })?;
        self.chunk_index.push(DecodedChunkIndexEntry {
            data_rel_offset: self.data_cursor,
            compressed_len,
            uncompressed_len,
            chunk_sha256,
        });
        self.data_cursor = next_data_cursor;
        self.pending_chunk.clear();
        self.pending_chunk_blocks = 0;
        self.staged_file = Some(staged_file);
        Ok(())
    }

    fn write_indexes_and_header(
        &self,
        file: &mut File,
        block_count: u32,
        first_prev_hash_wire: [u8; 32],
    ) -> Result<(u64, [u8; HEADER_LEN]), SettledSupplementSegmentError> {
        let data_offset = HEADER_LEN as u64;
        let chunk_index_offset = data_offset.checked_add(self.data_cursor).ok_or_else(|| {
            SettledSupplementSegmentError::InvalidFormat("chunk index offset overflow".to_owned())
        })?;
        let chunk_index_bytes = (self.chunk_index.len() as u64)
            .checked_mul(CHUNK_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("chunk index size overflow".to_owned())
            })?;
        let row_index_offset = chunk_index_offset
            .checked_add(chunk_index_bytes)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("row index offset overflow".to_owned())
            })?;
        let row_index_bytes = (self.row_index.len() as u64)
            .checked_mul(ROW_INDEX_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("row index size overflow".to_owned())
            })?;
        let trailer_offset = row_index_offset
            .checked_add(row_index_bytes)
            .ok_or_else(|| {
                SettledSupplementSegmentError::InvalidFormat("trailer offset overflow".to_owned())
            })?;

        for entry in &self.chunk_index {
            file.write_all(&entry.encode())?;
        }
        for entry in &self.row_index {
            file.write_all(&entry.encode())?;
        }

        let header_bytes = SupplementSegmentHeader {
            network_tag: self.identity.network.id(),
            artifact_schema_version: self.identity.artifact_schema_version,
            segment_flags: 0,
            codec_id: self.options.codec.id(),
            chunk_blocks: self.options.chunk_blocks,
            start_height: self.identity.start_height.value(),
            block_count,
            first_prev_hash_wire,
            last_block_hash_wire: encode_internal_block_hash(self.identity.end_block.hash),
            data_offset,
            chunk_index_offset,
            row_index_offset,
        }
        .encode();
        file.flush()?;
        write_all_at(file, &header_bytes, 0)?;
        file.sync_all()?;
        Ok((trailer_offset, header_bytes))
    }
}

fn compute_index_sha256(
    header_bytes: &[u8; HEADER_LEN],
    chunk_index: &[DecodedChunkIndexEntry],
    row_index: &[DecodedRowIndexEntry],
    trailer: &[u8; 32],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(header_bytes);
    for chunk in chunk_index {
        digest.update(chunk.encode());
    }
    for row in row_index {
        digest.update(row.encode());
    }
    digest.update(trailer);
    digest.finalize().into()
}

#[cfg(test)]
fn write_chunk_frame(
    file: &mut File,
    frame: &[u8],
    fail_after_partial: &std::cell::Cell<Option<usize>>,
) -> Result<(), SettledSupplementSegmentError> {
    if let Some(partial_bytes) = fail_after_partial.take() {
        let prefix_len = partial_bytes.min(frame.len());
        if prefix_len > 0 {
            file.write_all(&frame[..prefix_len])?;
        }
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "injected partial supplement chunk write failure",
        )
        .into());
    }
    file.write_all(frame)?;
    Ok(())
}

#[cfg(not(test))]
fn write_chunk_frame(file: &mut File, frame: &[u8]) -> Result<(), SettledSupplementSegmentError> {
    file.write_all(frame)?;
    Ok(())
}

fn staged_file_dev_ino(file: &File) -> io::Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let meta = file.metadata()?;
        Ok((meta.dev(), meta.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        Ok((0, 0))
    }
}

fn writer_poisoned() -> SettledSupplementSegmentError {
    SettledSupplementSegmentError::InvalidFormat(
        "supplement segment writer is poisoned after a failed write".to_owned(),
    )
}

fn check_canonical_facts_header(
    block: &CompactBlockArtifact,
    facts: &CanonicalBlockFacts,
) -> Result<(), SettledSupplementSegmentError> {
    if facts.block_header.height != block.height()
        || facts.block_header.block_hash != block.block_hash()
        || facts.block_header.parent_hash != block.previous_block_hash()
        || facts.block_header.block_time != i64::from(block.time())
    {
        return Err(SettledSupplementSegmentError::ChainLinkage(format!(
            "canonical block facts header for height {} disagrees with CompactBlockArtifact",
            block.height().value()
        )));
    }
    Ok(())
}

fn persist_segment(
    temp_path: TempPath,
    target: &Path,
    identity: &SupplementSegmentIdentity,
) -> Result<(), SettledSupplementSegmentError> {
    if let Err(failure) = temp_path.persist_noclobber(target) {
        if failure.error.kind() != io::ErrorKind::AlreadyExists {
            return Err(SettledSupplementSegmentError::Io(failure.error));
        }
        SettledSupplementSegmentReader::open(
            target,
            identity,
            SettledSupplementSegmentOpenOptions::default(),
        )?;
        failure
            .path
            .persist(target)
            .map_err(|persist_err| SettledSupplementSegmentError::Io(persist_err.error))?;
    }
    Ok(())
}

fn write_all_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt as _;
        file.write_all_at(bytes, offset)
    }
    #[cfg(not(unix))]
    {
        use std::os::windows::fs::FileExt as _;
        let mut written = 0;
        let mut cursor_offset = offset;
        while written < bytes.len() {
            let count = file.seek_write(&bytes[written..], cursor_offset)?;
            if count == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            written += count;
            cursor_offset += count as u64;
        }
        Ok(())
    }
}

fn hash_staged(
    file: &File,
    trailer_offset: u64,
) -> Result<([u8; 32], [u8; 32]), SettledSupplementSegmentError> {
    let mut digest = Sha256::new();
    let mut verity_hasher = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
    let mut input = BufReader::with_capacity(STAGING_CHUNK, file);
    input.seek(SeekFrom::Start(0))?;
    let mut chunk = vec![0_u8; STAGING_CHUNK];
    let mut remaining = trailer_offset;
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(chunk.len() as u64)).map_err(|_| {
            SettledSupplementSegmentError::InvalidFormat(
                "supplement segment exceeds address space".to_owned(),
            )
        })?;
        input.read_exact(&mut chunk[..wanted])?;
        digest.update(&chunk[..wanted]);
        verity_hasher.write_all(&chunk[..wanted])?;
        remaining -= wanted as u64;
    }
    let trailer: [u8; 32] = digest.finalize().into();
    verity_hasher.write_all(&trailer)?;
    let verity_digest: [u8; 32] = verity_hasher.finalize().as_ref().try_into().map_err(|_| {
        SettledSupplementSegmentError::PayloadIntegrity(
            "invalid fs-verity digest length".to_owned(),
        )
    })?;
    Ok((trailer, verity_digest))
}
