//! Single-pass atomic writer for `ZSHRNG02` settled shielded segments.

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use zinder_core::{BlockHash, BlockId, CompactBlockArtifact, wire::encode_internal_block_hash};

use super::{
    SettledShieldedSegmentError, SettledShieldedSegmentOpenOptions, SettledShieldedSegmentReader,
    ShieldedSegmentIdentity,
    format::{
        DecodedRowIndexEntry, FRAME_PREFIX_LEN, HEADER_LEN, MAX_SLAB_BYTES, ROW_INDEX_ENTRY_LEN,
        STAGING_CHUNK, SegmentHeader, check_contiguous_block, encode_frame_prefix,
        validate_payload_limit,
    },
    parent_directory, verity,
    wire_projection::encode_default_shielded_block,
};

/// Single-pass staged writer for one immutable `ZSHRNG02` segment.
pub struct SettledShieldedSegmentWriter {
    identity: ShieldedSegmentIdentity,
    target: PathBuf,
    staged: Option<BufWriter<NamedTempFile>>,
    slab_len: u32,
    rows: Vec<[u8; ROW_INDEX_ENTRY_LEN]>,
    prior: Option<BlockId>,
    first_prev_hash: Option<BlockHash>,
}

impl SettledShieldedSegmentWriter {
    /// Creates a staged `ZSHRNG02` writer destined for `target`.
    pub fn create(
        target: &Path,
        staged: NamedTempFile,
        span: ShieldedSegmentIdentity,
    ) -> Result<Self, SettledShieldedSegmentError> {
        span.block_count()?;
        let mut buffered = BufWriter::new(staged);
        buffered.write_all(&[0_u8; HEADER_LEN])?;
        Ok(Self {
            identity: span,
            target: target.to_owned(),
            staged: Some(buffered),
            slab_len: 0,
            rows: Vec::new(),
            prior: None,
            first_prev_hash: None,
        })
    }

    /// Encodes and writes one contiguous canonical compact block to `Slab 0`.
    pub fn push(
        &mut self,
        block: &CompactBlockArtifact,
        native_bytes: usize,
    ) -> Result<(), SettledShieldedSegmentError> {
        let block_id = BlockId::new(block.height(), block.block_hash());
        if let Some(prev) = self.prior {
            if block.previous_block_hash() != prev.hash {
                return Err(SettledShieldedSegmentError::ChainLinkage(format!(
                    "block {} parent hash {:?} does not match prior block {:?}",
                    block.height().value(),
                    block.previous_block_hash(),
                    prev.hash
                )));
            }
        } else {
            self.first_prev_hash = Some(block.previous_block_hash());
        }
        check_contiguous_block(&self.identity, block_id, self.prior)?;
        let (payload, pool_presence) = encode_default_shielded_block(block)?;
        validate_payload_limit(block.height(), payload.len())?;
        let native_u32 = u32::try_from(native_bytes).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded segment native byte length exceeds u32".to_owned(),
            )
        })?;
        let prefix = encode_frame_prefix(payload.len())?;
        let rel_offset = self.slab_len;
        let frame_len = u32::try_from(FRAME_PREFIX_LEN + payload.len()).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat("shielded frame exceeds u32".to_owned())
        })?;
        let next_slab_len = u64::from(rel_offset) + u64::from(frame_len);
        if next_slab_len > MAX_SLAB_BYTES {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "shielded segment slab exceeds 4 GiB offset limit".to_owned(),
            ));
        }
        let mut staged = self.staged.take().ok_or_else(|| {
            SettledShieldedSegmentError::Io(io::Error::other("staged segment writer poisoned"))
        })?;
        staged.write_all(&prefix)?;
        staged.write_all(&payload)?;
        let entry = DecodedRowIndexEntry {
            block_hash_wire: encode_internal_block_hash(block_id.hash),
            payload_sha256: Sha256::digest(&payload).into(),
            block_time: block.time(),
            slab_rel_offset: rel_offset,
            native_bytes: native_u32,
            pool_presence,
        };
        self.rows.push(entry.encode());
        self.slab_len = u32::try_from(next_slab_len).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded segment slab exceeds u32 offset limit".to_owned(),
            )
        })?;
        self.staged = Some(staged);
        self.prior = Some(block_id);
        Ok(())
    }

    /// Finalizes the segment index, header, SHA-256 trailer, optional
    /// `fs-verity` protection, and atomic target placement.
    pub fn finish(mut self) -> Result<ShieldedSegmentIdentity, SettledShieldedSegmentError> {
        let block_count = self.identity.block_count()?;
        let first_prev_hash = self.first_prev_hash.ok_or_else(|| {
            SettledShieldedSegmentError::ChainLinkage("empty shielded segment".to_owned())
        })?;
        if self.prior != Some(self.identity.end_block)
            || u32::try_from(self.rows.len()).ok() != Some(block_count)
        {
            return Err(SettledShieldedSegmentError::ChainLinkage(
                "incomplete shielded segment".to_owned(),
            ));
        }
        let mut staged = self.staged.take().ok_or_else(|| {
            SettledShieldedSegmentError::Io(io::Error::other("staged segment writer poisoned"))
        })?;
        for entry in &self.rows {
            staged.write_all(entry)?;
        }
        let staged_file = staged
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        let index_offset = HEADER_LEN as u64 + u64::from(self.slab_len);
        let header = SegmentHeader {
            network_tag: self.identity.network.id(),
            artifact_schema_version: self.identity.artifact_schema_version,
            segment_flags: 0,
            start_height: self.identity.start_height.value(),
            block_count,
            first_prev_hash_wire: encode_internal_block_hash(first_prev_hash),
            last_block_hash_wire: encode_internal_block_hash(self.identity.end_block.hash),
            slab_offset: HEADER_LEN as u64,
            index_offset,
        };
        write_all_at(staged_file.as_file(), &header.encode(), 0)?;
        let index_bytes = u64::from(block_count) * ROW_INDEX_ENTRY_LEN as u64;
        let trailer_offset = index_offset + index_bytes;
        let (trailer, verity_digest) = hash_staged(staged_file.as_file(), trailer_offset)?;
        write_all_at(staged_file.as_file(), &trailer, trailer_offset)?;
        staged_file.as_file().sync_all()?;

        let readonly_staged = verity::reopen_staged(&staged_file)?;
        let (writable_file, temp_path) = staged_file.into_parts();
        drop(writable_file);
        verity::protect_new_segment(&readonly_staged, Some(verity_digest))?;
        drop(readonly_staged);

        let identity = ShieldedSegmentIdentity {
            sha256: trailer,
            fs_verity_sha256: Some(verity_digest),
            ..self.identity
        };
        persist_segment(temp_path, &self.target, &identity)?;
        File::open(parent_directory(&self.target))?.sync_all()?;
        Ok(identity)
    }
}

fn persist_segment(
    temp_path: tempfile::TempPath,
    target: &Path,
    identity: &ShieldedSegmentIdentity,
) -> Result<(), SettledShieldedSegmentError> {
    if let Err(failure) = temp_path.persist_noclobber(target) {
        if failure.error.kind() != io::ErrorKind::AlreadyExists {
            return Err(SettledShieldedSegmentError::Io(failure.error));
        }
        SettledShieldedSegmentReader::open(
            target,
            identity,
            SettledShieldedSegmentOpenOptions::default(),
        )?;
        failure
            .path
            .persist(target)
            .map_err(|persist_err| SettledShieldedSegmentError::Io(persist_err.error))?;
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
) -> Result<([u8; 32], [u8; 32]), SettledShieldedSegmentError> {
    let mut digest = Sha256::new();
    let mut verity_hasher = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
    let mut input = BufReader::with_capacity(STAGING_CHUNK, file);
    input.seek(io::SeekFrom::Start(0))?;
    let mut chunk = vec![0_u8; STAGING_CHUNK];
    let mut remaining = trailer_offset;
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(chunk.len() as u64)).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded segment exceeds address space".to_owned(),
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
        SettledShieldedSegmentError::PayloadIntegrity("invalid fs-verity digest length".to_owned())
    })?;
    Ok((trailer, verity_digest))
}
