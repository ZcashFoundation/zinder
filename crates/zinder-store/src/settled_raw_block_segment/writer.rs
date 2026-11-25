//! Single-pass atomic writer for `ZSBLKB01` settled raw block segments.

use std::{
    borrow::Cow,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use zinder_core::{BlockHash, BlockHeight, BlockId, wire::encode_internal_block_hash};

use super::{
    RawBlockCodec, RawBlockSegmentIdentity, RawBlockTransactionSlice,
    SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES, SettledRawBlockSegmentError,
    SettledRawBlockSegmentOpenOptions, SettledRawBlockSegmentReader,
    format::{
        DecodedRowIndexEntry, FRAME_PREFIX_LEN, HEADER_LEN, MAX_SLAB_BYTES, ROW_INDEX_ENTRY_LEN,
        STAGING_CHUNK, SegmentHeader, TX_SLICE_ENTRY_LEN, TxSliceEntry, ZSTD_COMPRESSION_LEVEL,
        check_contiguous_block, encode_frame_prefix,
    },
    parent_directory, verity,
};

/// Single-pass staged writer for one immutable `ZSBLKB01` segment.
pub struct SettledRawBlockSegmentWriter {
    identity: RawBlockSegmentIdentity,
    target: PathBuf,
    staged: Option<BufWriter<NamedTempFile>>,
    slab_len: u32,
    tx_entries: Vec<[u8; TX_SLICE_ENTRY_LEN]>,
    rows: Vec<[u8; ROW_INDEX_ENTRY_LEN]>,
    prior: Option<BlockId>,
    first_prev_hash: Option<BlockHash>,
}

impl SettledRawBlockSegmentWriter {
    /// Creates a staged `ZSBLKB01` writer destined for `target`.
    pub fn create(
        target: &Path,
        staged: NamedTempFile,
        span: RawBlockSegmentIdentity,
    ) -> Result<Self, SettledRawBlockSegmentError> {
        span.block_count()?;
        let mut buffered = BufWriter::new(staged);
        buffered.write_all(&[0_u8; HEADER_LEN])?;
        Ok(Self {
            identity: span,
            target: target.to_owned(),
            staged: Some(buffered),
            slab_len: 0,
            tx_entries: Vec::new(),
            rows: Vec::new(),
            prior: None,
            first_prev_hash: None,
        })
    }

    /// Validates, optionally compresses, and writes one contiguous canonical raw
    /// block and its transaction slice metadata to `Slab 0`.
    pub fn push(
        &mut self,
        block_id: BlockId,
        previous_block_hash: BlockHash,
        raw_block_bytes: &[u8],
        tx_slices: &[RawBlockTransactionSlice],
    ) -> Result<(), SettledRawBlockSegmentError> {
        validate_push_linkage(
            &self.identity,
            block_id,
            previous_block_hash,
            self.prior,
            &mut self.first_prev_hash,
        )?;
        let uncompressed_len = validate_raw_block_len(block_id.height, raw_block_bytes)?;
        let tx_start_index = u32::try_from(self.tx_entries.len()).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment tx table exceeds u32".to_owned(),
            )
        })?;
        let tx_count = u32::try_from(tx_slices.len()).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat(
                "block transaction count exceeds u32".to_owned(),
            )
        })?;
        tx_start_index.checked_add(tx_count).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment tx table exceeds u32".to_owned(),
            )
        })?;
        let encoded_tx_entries = encode_block_tx_slices(raw_block_bytes, tx_slices)?;
        let payload = encode_block_payload(raw_block_bytes, self.identity.codec)?;
        let prefix = encode_frame_prefix(payload.len(), self.identity.codec)?;
        let (rel_offset, next_slab_len) = advance_slab_len(self.slab_len, payload.len())?;

        let mut staged = self.staged.take().ok_or_else(|| {
            SettledRawBlockSegmentError::Io(io::Error::other("staged segment writer poisoned"))
        })?;
        staged.write_all(&prefix)?;
        staged.write_all(&payload)?;
        self.staged = Some(staged);

        let entry = DecodedRowIndexEntry {
            block_hash_wire: encode_internal_block_hash(block_id.hash),
            payload_sha256: Sha256::digest(payload.as_ref()).into(),
            uncompressed_sha256: Sha256::digest(raw_block_bytes).into(),
            slab_rel_offset: rel_offset,
            uncompressed_len,
            tx_start_index,
            tx_count,
        };
        self.tx_entries.extend(encoded_tx_entries);
        self.rows.push(entry.encode());
        self.slab_len = next_slab_len;
        self.prior = Some(block_id);
        Ok(())
    }

    /// Finalizes the transaction slice table, row index, header, SHA-256
    /// trailer, optional `fs-verity` protection, and atomic target placement.
    pub fn finish(mut self) -> Result<RawBlockSegmentIdentity, SettledRawBlockSegmentError> {
        let (block_count, first_prev_hash) = self.validate_complete()?;
        let mut staged = self.staged.take().ok_or_else(|| {
            SettledRawBlockSegmentError::Io(io::Error::other("staged segment writer poisoned"))
        })?;
        for tx_entry in &self.tx_entries {
            staged.write_all(tx_entry)?;
        }
        for row_entry in &self.rows {
            staged.write_all(row_entry)?;
        }
        let staged_file = staged
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        let header = self.build_header(block_count, first_prev_hash)?;
        write_all_at(staged_file.as_file(), &header.encode(), 0)?;
        let index_bytes = u64::from(block_count) * ROW_INDEX_ENTRY_LEN as u64;
        let trailer_offset = header.row_index_offset + index_bytes;
        let (trailer, verity_digest) = hash_staged(staged_file.as_file(), trailer_offset)?;
        write_all_at(staged_file.as_file(), &trailer, trailer_offset)?;
        staged_file.as_file().sync_all()?;

        let readonly_staged = verity::reopen_staged(&staged_file)?;
        let (writable_file, temp_path) = staged_file.into_parts();
        drop(writable_file);
        verity::protect_new_segment(&readonly_staged, Some(verity_digest))?;
        drop(readonly_staged);

        let identity = RawBlockSegmentIdentity {
            sha256: trailer,
            fs_verity_sha256: Some(verity_digest),
            ..self.identity
        };
        persist_segment(temp_path, &self.target, &identity)?;
        File::open(parent_directory(&self.target))?.sync_all()?;
        Ok(identity)
    }

    fn validate_complete(&self) -> Result<(u32, BlockHash), SettledRawBlockSegmentError> {
        let block_count = self.identity.block_count()?;
        let first_prev_hash = self.first_prev_hash.ok_or_else(|| {
            SettledRawBlockSegmentError::ChainLinkage("empty raw block segment".to_owned())
        })?;
        if self.prior != Some(self.identity.end_block)
            || u32::try_from(self.rows.len()).ok() != Some(block_count)
        {
            return Err(SettledRawBlockSegmentError::ChainLinkage(
                "incomplete raw block segment".to_owned(),
            ));
        }
        Ok((block_count, first_prev_hash))
    }

    fn build_header(
        &self,
        block_count: u32,
        first_prev_hash: BlockHash,
    ) -> Result<SegmentHeader, SettledRawBlockSegmentError> {
        let tx_table_offset = HEADER_LEN as u64 + u64::from(self.slab_len);
        let tx_count_u64 = u64::try_from(self.tx_entries.len()).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("tx table count exceeds u64".to_owned())
        })?;
        let tx_table_bytes = tx_count_u64
            .checked_mul(TX_SLICE_ENTRY_LEN as u64)
            .ok_or_else(|| {
                SettledRawBlockSegmentError::InvalidFormat("tx table size overflow".to_owned())
            })?;
        let row_index_offset = tx_table_offset.checked_add(tx_table_bytes).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("row index offset overflow".to_owned())
        })?;
        Ok(SegmentHeader {
            network_tag: self.identity.network.id(),
            segment_flags: 0,
            codec_id: self.identity.codec.id(),
            start_height: self.identity.start_height.value(),
            block_count,
            first_prev_hash_wire: encode_internal_block_hash(first_prev_hash),
            last_block_hash_wire: encode_internal_block_hash(self.identity.end_block.hash),
            slab_offset: HEADER_LEN as u64,
            tx_table_offset,
            row_index_offset,
        })
    }
}

fn validate_push_linkage(
    identity: &RawBlockSegmentIdentity,
    block_id: BlockId,
    previous_block_hash: BlockHash,
    prior: Option<BlockId>,
    first_prev_hash: &mut Option<BlockHash>,
) -> Result<(), SettledRawBlockSegmentError> {
    check_contiguous_block(identity, block_id, prior)?;
    if let Some(prev) = prior {
        if previous_block_hash != prev.hash {
            return Err(SettledRawBlockSegmentError::ChainLinkage(format!(
                "block {} parent hash {:?} does not match prior block {:?}",
                block_id.height.value(),
                previous_block_hash,
                prev.hash
            )));
        }
    } else {
        *first_prev_hash = Some(previous_block_hash);
    }
    Ok(())
}

fn validate_raw_block_len(
    height: BlockHeight,
    raw_block_bytes: &[u8],
) -> Result<u32, SettledRawBlockSegmentError> {
    if raw_block_bytes.is_empty() {
        return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
            "block {} raw payload must not be empty",
            height.value()
        )));
    }
    if raw_block_bytes.len() > SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES {
        return Err(SettledRawBlockSegmentError::PayloadLimitExceeded {
            block_height: height,
            payload_bytes: raw_block_bytes.len(),
            max_bytes: SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES,
        });
    }
    u32::try_from(raw_block_bytes.len()).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("raw block byte length exceeds u32".to_owned())
    })
}

fn encode_block_tx_slices(
    raw_block_bytes: &[u8],
    tx_slices: &[RawBlockTransactionSlice],
) -> Result<Vec<[u8; TX_SLICE_ENTRY_LEN]>, SettledRawBlockSegmentError> {
    let block_len_u32 = u32::try_from(raw_block_bytes.len()).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat("raw block byte length exceeds u32".to_owned())
    })?;
    let mut encoded = Vec::with_capacity(tx_slices.len());
    for slice in tx_slices {
        let end_u32 = slice.offset.checked_add(slice.len).ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat(
                "transaction slice offset plus length overflows u32".to_owned(),
            )
        })?;
        if end_u32 > block_len_u32 {
            return Err(SettledRawBlockSegmentError::InvalidFormat(format!(
                "transaction slice [{}..{end_u32}] exceeds raw block length {block_len_u32}",
                slice.offset
            )));
        }
        let start = usize::try_from(slice.offset).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("tx offset exceeds usize".to_owned())
        })?;
        let end = usize::try_from(end_u32).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat("tx end exceeds usize".to_owned())
        })?;
        let tx_sha256: [u8; 32] = Sha256::digest(&raw_block_bytes[start..end]).into();
        encoded.push(
            TxSliceEntry {
                offset: slice.offset,
                len: slice.len,
                sha256: tx_sha256,
            }
            .encode(),
        );
    }
    Ok(encoded)
}

fn encode_block_payload(
    raw_block_bytes: &[u8],
    codec: RawBlockCodec,
) -> Result<Cow<'_, [u8]>, SettledRawBlockSegmentError> {
    match codec {
        RawBlockCodec::None => Ok(Cow::Borrowed(raw_block_bytes)),
        RawBlockCodec::Zstd => {
            let compressed = zstd::bulk::compress(raw_block_bytes, ZSTD_COMPRESSION_LEVEL)?;
            Ok(Cow::Owned(compressed))
        }
    }
}

fn advance_slab_len(
    current_slab_len: u32,
    payload_len: usize,
) -> Result<(u32, u32), SettledRawBlockSegmentError> {
    let frame_len = FRAME_PREFIX_LEN
        .checked_add(payload_len)
        .and_then(|total| u32::try_from(total).ok())
        .ok_or_else(|| {
            SettledRawBlockSegmentError::InvalidFormat("raw block frame exceeds u32".to_owned())
        })?;
    let next_slab_len = u64::from(current_slab_len) + u64::from(frame_len);
    if next_slab_len > MAX_SLAB_BYTES {
        return Err(SettledRawBlockSegmentError::InvalidFormat(
            "raw block segment slab exceeds 4 GiB offset limit".to_owned(),
        ));
    }
    let next_u32 = u32::try_from(next_slab_len).map_err(|_| {
        SettledRawBlockSegmentError::InvalidFormat(
            "raw block segment slab exceeds u32 offset limit".to_owned(),
        )
    })?;
    Ok((current_slab_len, next_u32))
}

fn persist_segment(
    temp_path: tempfile::TempPath,
    target: &Path,
    identity: &RawBlockSegmentIdentity,
) -> Result<(), SettledRawBlockSegmentError> {
    if let Err(failure) = temp_path.persist_noclobber(target) {
        if failure.error.kind() != io::ErrorKind::AlreadyExists {
            return Err(SettledRawBlockSegmentError::Io(failure.error));
        }
        SettledRawBlockSegmentReader::open(
            target,
            identity,
            SettledRawBlockSegmentOpenOptions::default(),
        )?;
        failure
            .path
            .persist(target)
            .map_err(|persist_err| SettledRawBlockSegmentError::Io(persist_err.error))?;
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
) -> Result<([u8; 32], [u8; 32]), SettledRawBlockSegmentError> {
    let mut digest = Sha256::new();
    let mut verity_hasher = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
    let mut input = BufReader::with_capacity(STAGING_CHUNK, file);
    input.seek(io::SeekFrom::Start(0))?;
    let mut chunk = vec![0_u8; STAGING_CHUNK];
    let mut remaining = trailer_offset;
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(chunk.len() as u64)).map_err(|_| {
            SettledRawBlockSegmentError::InvalidFormat(
                "raw block segment exceeds address space".to_owned(),
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
        SettledRawBlockSegmentError::PayloadIntegrity("invalid fs-verity digest length".to_owned())
    })?;
    Ok((trailer, verity_digest))
}
