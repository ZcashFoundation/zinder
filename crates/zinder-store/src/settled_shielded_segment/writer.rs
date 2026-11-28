//! Single-pass atomic writer for `ZSHRNG02` settled shielded segments.

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use zinder_core::{BlockHash, BlockId, CompactBlockArtifact, wire::encode_internal_block_hash};

use crate::segment_verity;

use super::{
    SettledShieldedSegmentError, SettledShieldedSegmentOpenOptions, SettledShieldedSegmentReader,
    ShieldedSegmentHeader, ShieldedSegmentIdentity,
    format::{
        DecodedRowIndexEntry, FRAME_PREFIX_LEN, HEADER_LEN, MAX_SLAB_BYTES,
        ROW_FLAG_UNREPRESENTABLE_WIRE_FEE, ROW_INDEX_ENTRY_LEN, STAGING_CHUNK,
        check_contiguous_block, encode_frame_prefix, validate_payload_limit,
    },
    parent_directory,
    wire_projection::encode_default_shielded_block_with_options,
};

/// Uncommitted or named staged file target for `SettledShieldedSegmentWriter`.
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
}

impl io::Write for StagedTarget {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Temp(temp) => temp.write(buf),
            Self::OwnedFile(file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Temp(temp) => temp.flush(),
            Self::OwnedFile(file) => file.flush(),
        }
    }
}

/// Single-pass staged writer for one immutable `ZSHRNG02` segment.
pub struct SettledShieldedSegmentWriter {
    identity: ShieldedSegmentIdentity,
    target: PathBuf,
    staged: Option<BufWriter<StagedTarget>>,
    slab_len: u32,
    rows: Vec<[u8; ROW_INDEX_ENTRY_LEN]>,
    prior: Option<BlockId>,
    first_prev_hash: Option<BlockHash>,
}

impl SettledShieldedSegmentWriter {
    /// Creates a staged `ZSHRNG02` writer destined for `target`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] if `span` is invalid or writing the
    /// initial zeroed header fails.
    pub fn create(
        target: &Path,
        staged: NamedTempFile,
        span: ShieldedSegmentIdentity,
    ) -> Result<Self, SettledShieldedSegmentError> {
        Self::create_target(target.to_owned(), StagedTarget::Temp(staged), span)
    }

    /// Creates a staged `ZSHRNG02` writer backed directly by a caller-owned `File`.
    pub(crate) fn create_in_file(
        staged: File,
        span: ShieldedSegmentIdentity,
    ) -> Result<Self, SettledShieldedSegmentError> {
        Self::create_target(PathBuf::new(), StagedTarget::OwnedFile(staged), span)
    }

    fn create_target(
        target: PathBuf,
        staged: StagedTarget,
        span: ShieldedSegmentIdentity,
    ) -> Result<Self, SettledShieldedSegmentError> {
        span.block_count()?;
        let mut buffered = BufWriter::new(staged);
        buffered.write_all(&[0_u8; HEADER_LEN])?;
        Ok(Self {
            identity: span,
            target,
            staged: Some(buffered),
            slab_len: 0,
            rows: Vec::new(),
            prior: None,
            first_prev_hash: None,
        })
    }

    /// Encodes and writes one contiguous canonical compact block to `Slab 0`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on height/hash linkage gaps,
    /// fees exceeding `u32`, payload size overflows, or staged I/O failure.
    pub fn push(
        &mut self,
        block: &CompactBlockArtifact,
        native_bytes: usize,
    ) -> Result<(), SettledShieldedSegmentError> {
        self.push_inner(block, native_bytes, false)
    }

    /// Encodes and writes one contiguous canonical compact block when paired with a
    /// `ZSTSUP01` supplement segment, zeroing shielded wire fees above `u32::MAX`
    /// in `Slab 0` and marking the row flag on any transaction fee above `u32::MAX`
    /// (including omitted non-shielded transactions) so direct wire serving fails
    /// closed while canonical reconstruction reads recover full `Option<u64>` fees
    /// from `ZSTSUP01`.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] on height/hash linkage gaps,
    /// payload size overflows, or staged I/O failure.
    pub fn push_with_supplement(
        &mut self,
        block: &CompactBlockArtifact,
        native_bytes: usize,
    ) -> Result<(), SettledShieldedSegmentError> {
        self.push_inner(block, native_bytes, true)
    }

    fn push_inner(
        &mut self,
        block: &CompactBlockArtifact,
        native_bytes: usize,
        allow_wide_fee_in_supplement: bool,
    ) -> Result<(), SettledShieldedSegmentError> {
        let block_id = BlockId::new(block.height(), block.block_hash());
        if let Some(prev) = self.prior
            && block.previous_block_hash() != prev.hash
        {
            return Err(SettledShieldedSegmentError::ChainLinkage(format!(
                "block {} parent hash {:?} does not match prior block {:?}",
                block.height().value(),
                block.previous_block_hash(),
                prev.hash
            )));
        }
        check_contiguous_block(&self.identity, block_id, self.prior)?;
        let encoded =
            encode_default_shielded_block_with_options(block, allow_wide_fee_in_supplement)?;
        validate_payload_limit(block.height(), encoded.payload.len())?;
        let native_u32 = u32::try_from(native_bytes).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded segment native byte length exceeds u32".to_owned(),
            )
        })?;
        let prefix = encode_frame_prefix(encoded.payload.len())?;
        let rel_offset = self.slab_len;
        let frame_len = u32::try_from(FRAME_PREFIX_LEN + encoded.payload.len()).map_err(|_| {
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
        staged.write_all(&encoded.payload)?;
        let row_flags = if encoded.has_unrepresentable_wire_fee {
            ROW_FLAG_UNREPRESENTABLE_WIRE_FEE
        } else {
            0
        };
        let entry = DecodedRowIndexEntry {
            block_hash_wire: encode_internal_block_hash(block_id.hash),
            payload_sha256: Sha256::digest(&encoded.payload).into(),
            block_time: block.time(),
            slab_rel_offset: rel_offset,
            native_bytes: native_u32,
            pool_presence: encoded.pool_presence,
            row_flags,
        };
        self.rows.push(entry.encode());
        self.slab_len = u32::try_from(next_slab_len).map_err(|_| {
            SettledShieldedSegmentError::InvalidFormat(
                "shielded segment slab exceeds u32 offset limit".to_owned(),
            )
        })?;
        self.staged = Some(staged);
        if self.prior.is_none() {
            self.first_prev_hash = Some(block.previous_block_hash());
        }
        self.prior = Some(block_id);
        Ok(())
    }

    /// Finalizes the segment index, header, SHA-256 trailer, `index_sha256` pin,
    /// optional `fs-verity` protection, and atomic target placement.
    ///
    /// # Errors
    ///
    /// Returns [`SettledShieldedSegmentError`] if the segment is incomplete,
    /// hashing or `fs-verity` protection fails, or atomic persistence fails.
    pub fn finish(mut self) -> Result<ShieldedSegmentIdentity, SettledShieldedSegmentError> {
        let FinalizedStagedBody {
            target: staged_target,
            trailer,
            index_sha256,
            verity_digest,
        } = self.finalize_staged_body()?;
        let StagedTarget::Temp(staged_file) = staged_target else {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "caller-owned staged shielded writer must use finish_staged_file".to_owned(),
            ));
        };

        let readonly_staged = segment_verity::reopen_staged(&staged_file, "shielded segment")?;
        let (writable_file, temp_path) = staged_file.into_parts();
        drop(writable_file);
        segment_verity::protect_new_segment(
            &readonly_staged,
            Some(verity_digest),
            "shielded segment",
        )?;
        drop(readonly_staged);

        let identity = ShieldedSegmentIdentity {
            sha256: trailer,
            index_sha256,
            fs_verity_sha256: Some(verity_digest),
            ..self.identity
        };
        persist_segment(temp_path, &self.target, &identity)?;
        File::open(parent_directory(&self.target))?.sync_all()?;
        Ok(identity)
    }

    /// Finalizes headers, indexes, trailer, and sync on a caller-owned staged file,
    /// returning its Unix `(dev, ino)` and complete identity (including `index_sha256`)
    /// before dropping the writable descriptor.
    pub(crate) fn finish_staged_file(
        mut self,
    ) -> Result<((u64, u64), ShieldedSegmentIdentity), SettledShieldedSegmentError> {
        let FinalizedStagedBody {
            target: staged_target,
            trailer,
            index_sha256,
            verity_digest,
        } = self.finalize_staged_body()?;
        let StagedTarget::OwnedFile(staged_file) = staged_target else {
            return Err(SettledShieldedSegmentError::InvalidFormat(
                "path-backed staged shielded writer must use finish".to_owned(),
            ));
        };
        let dev_ino = staged_file_dev_ino(&staged_file)?;
        drop(staged_file);
        let identity = ShieldedSegmentIdentity {
            sha256: trailer,
            index_sha256,
            fs_verity_sha256: Some(verity_digest),
            ..self.identity
        };
        Ok((dev_ino, identity))
    }

    fn finalize_staged_body(&mut self) -> Result<FinalizedStagedBody, SettledShieldedSegmentError> {
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
        let header_bytes = ShieldedSegmentHeader {
            network_tag: self.identity.network.id(),
            artifact_schema_version: self.identity.artifact_schema_version,
            segment_flags: 0,
            start_height: self.identity.start_height.value(),
            block_count,
            first_prev_hash_wire: encode_internal_block_hash(first_prev_hash),
            last_block_hash_wire: encode_internal_block_hash(self.identity.end_block.hash),
            slab_offset: HEADER_LEN as u64,
            index_offset,
        }
        .encode();
        write_all_at(staged_file.as_file(), &header_bytes, 0)?;
        let index_bytes = u64::from(block_count) * ROW_INDEX_ENTRY_LEN as u64;
        let trailer_offset = index_offset + index_bytes;
        let (trailer, verity_digest) = hash_staged(staged_file.as_file(), trailer_offset)?;
        write_all_at(staged_file.as_file(), &trailer, trailer_offset)?;
        staged_file.as_file().sync_all()?;

        let index_sha256 = compute_index_sha256(&header_bytes, &self.rows, &trailer);
        Ok(FinalizedStagedBody {
            target: staged_file,
            trailer,
            index_sha256,
            verity_digest,
        })
    }
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

fn compute_index_sha256(
    header_bytes: &[u8; HEADER_LEN],
    rows: &[[u8; ROW_INDEX_ENTRY_LEN]],
    trailer: &[u8; 32],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(header_bytes);
    for row in rows {
        digest.update(row);
    }
    digest.update(trailer);
    digest.finalize().into()
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
