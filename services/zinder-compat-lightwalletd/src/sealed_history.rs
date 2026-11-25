//! Signed catalog segment for the default shielded and nullifier ranges.
//!
//! A segment file carries one settled height interval in two slabs of
//! pre-encoded lightwalletd `CompactBlock` frames, so `GetBlockRange` and
//! `GetBlockRangeNullifiers` both serve zero-copy byte spans:
//!
//! ```text
//! [Header 128 B]
//! [Slab 0: shielded frames]     [native_len u32][wire_len u32][payload] per block
//! [Slab 1: nullifier frames]    same framing, one frame per block
//! [Index]
//!   shielded_rel_offsets:  [u32; block_count + 1]   relative to the slab base
//!   nullifier_rel_offsets: [u32; block_count + 1]
//!   rows: [hash 32][parent 32][native_len u32][reserved u32]
//!         [shielded_sha256 32][nullifier_sha256 32] per block
//! [Trailer: SHA-256 of every byte before it]
//! ```
//!
//! The trailer digest is pinned by the signed catalog and, where the
//! filesystem provides it, by fs-verity over the whole file. Each row keeps
//! its own payload digests and chain identity, so a portable file that
//! changes after admission still fails closed when its bytes are read.

use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU8, Ordering},
};

use prost::{Message, bytes::Bytes};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CompactBlockArtifact, Network,
    wire::{decode_internal_block_hash, encode_internal_block_hash},
};
use zinder_proto::compat::lightwalletd::{self, EncodedBlock};
use zinder_query::DEFAULT_MAX_COMPACT_BLOCK_RANGE;
use zinder_store::HeightOrder;

use crate::compact_block_messages::CompactBlockShape;

// The magic binds the unified dual-slab segment format. All integers are big-endian.
const MAGIC: &[u8; 8] = b"ZSHRNG01";
pub(crate) const HEADER_LEN: usize = 128;
const LENGTHS_LEN: usize = 8;
// A row's length field ends with its big-endian payload length, so its last
// bytes can carry the gRPC message prefix.
const _: () = assert!(LENGTHS_LEN >= EncodedBlock::PREFIX_LEN);
// One slab holds at most 4 GiB of frames, the reach of its u32 offset table.
const MAX_SLAB_BYTES: u64 = u32::MAX as u64;
const ROW_META_LEN: usize = 136;
/// Read and write granularity for staged files.
const STAGING_CHUNK: usize = 256 * 1024;
const TRAILER_LEN: usize = 32;
mod index;
pub(crate) mod verity;
// Bound the encoded bytes allocated for one authenticated row.
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const SLAB_TARGET_BYTES: usize = 64 * 1024;
// Bit 0 marks the nullifier slab present, which every segment must carry.
const SEGMENT_FLAGS_HAS_NULLIFIER_SLAB: u16 = 0x0001;

/// Counter of bytes read from signed-history catalog segments, labelled by `slab`.
const PUBLISHED_HISTORY_READ_BYTES_COUNTER: &str =
    "zinder_compat_published_history_read_bytes_total";
/// Histogram of signed-history catalog file read duration, labelled by `slab`.
const PUBLISHED_HISTORY_READ_DURATION_HISTOGRAM: &str =
    "zinder_compat_published_history_read_duration_seconds";

/// Which slab of a segment a range read serves.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SealedSlab {
    /// Pre-encoded `CompactBlockShape::DEFAULT_RANGE` frames.
    Shielded,
    /// Pre-encoded `CompactBlockShape::DEFAULT_NULLIFIER_RANGE` frames.
    Nullifier,
}

impl SealedSlab {
    const ALL: [Self; 2] = [Self::Shielded, Self::Nullifier];

    const fn label(self) -> &'static str {
        match self {
            Self::Shielded => "shielded",
            Self::Nullifier => "nullifier",
        }
    }
}

/// Describes and pre-registers signed-catalog read telemetry at zero for both slabs.
pub(crate) fn register_published_history_metrics() {
    metrics::describe_counter!(
        PUBLISHED_HISTORY_READ_BYTES_COUNTER,
        metrics::Unit::Bytes,
        "Bytes read from signed-history catalog segment files through OS buffered I/O, \
         labelled by `slab` (`shielded` or `nullifier`)."
    );
    metrics::describe_histogram!(
        PUBLISHED_HISTORY_READ_DURATION_HISTOGRAM,
        metrics::Unit::Seconds,
        "Time spent reading framed payloads from signed-history catalog segment files, \
         labelled by `slab` (`shielded` or `nullifier`)."
    );
    for slab in SealedSlab::ALL {
        metrics::counter!(PUBLISHED_HISTORY_READ_BYTES_COUNTER, "slab" => slab.label())
            .increment(0);
    }
}

fn record_published_history_read(slab: SealedSlab, bytes_read: u64, elapsed: std::time::Duration) {
    let slab_label = slab.label();
    metrics::counter!(PUBLISHED_HISTORY_READ_BYTES_COUNTER, "slab" => slab_label)
        .increment(bytes_read);
    metrics::histogram!(PUBLISHED_HISTORY_READ_DURATION_HISTOGRAM, "slab" => slab_label)
        .record(elapsed);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentIdentity {
    pub(crate) network: Network,
    pub(crate) artifact_schema: u16,
    pub(crate) start: BlockHeight,
    pub(crate) end: BlockId,
    pub(crate) sha256: [u8; 32],
    pub(crate) verity: Option<[u8; 32]>,
}

/// The fixed header of one segment, in file order.
pub(crate) struct SegmentHeader {
    network_tag: u32,
    artifact_schema_version: u16,
    segment_flags: u16,
    start_height: u32,
    block_count: u32,
    first_prev_hash: [u8; 32],
    last_block_hash: [u8; 32],
    shielded_slab_offset: u64,
    nullifier_slab_offset: u64,
    index_offset: u64,
}

/// Reads `N` bytes at `offset` of a fixed-width header field.
fn field<const N: usize>(bytes: &[u8], offset: usize) -> io::Result<[u8; N]> {
    bytes
        .get(offset..offset + N)
        .and_then(|raw| raw.try_into().ok())
        .ok_or_else(|| invalid("short shielded segment header"))
}

impl SegmentHeader {
    fn encode(self) -> [u8; HEADER_LEN] {
        let mut header = [0; HEADER_LEN];
        header[..8].copy_from_slice(MAGIC);
        header[8..12].copy_from_slice(&self.network_tag.to_be_bytes());
        header[12..14].copy_from_slice(&self.artifact_schema_version.to_be_bytes());
        header[14..16].copy_from_slice(&self.segment_flags.to_be_bytes());
        header[16..20].copy_from_slice(&self.start_height.to_be_bytes());
        header[20..24].copy_from_slice(&self.block_count.to_be_bytes());
        header[24..56].copy_from_slice(&self.first_prev_hash);
        header[56..88].copy_from_slice(&self.last_block_hash);
        header[88..96].copy_from_slice(&self.shielded_slab_offset.to_be_bytes());
        header[96..104].copy_from_slice(&self.nullifier_slab_offset.to_be_bytes());
        header[104..112].copy_from_slice(&self.index_offset.to_be_bytes());
        header
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if &field::<8>(bytes, 0)? != MAGIC {
            return Err(invalid("shielded segment magic mismatch"));
        }
        if !field::<16>(bytes, 112)?.iter().all(|byte| *byte == 0) {
            return Err(invalid("shielded segment header reserved bytes are set"));
        }
        let u32_at = |offset: usize| field::<4>(bytes, offset).map(u32::from_be_bytes);
        let u16_at = |offset: usize| field::<2>(bytes, offset).map(u16::from_be_bytes);
        let u64_at = |offset: usize| field::<8>(bytes, offset).map(u64::from_be_bytes);
        let hash_at = |offset: usize| field::<32>(bytes, offset);
        Ok(Self {
            network_tag: u32_at(8)?,
            artifact_schema_version: u16_at(12)?,
            segment_flags: u16_at(14)?,
            start_height: u32_at(16)?,
            block_count: u32_at(20)?,
            first_prev_hash: hash_at(24)?,
            last_block_hash: hash_at(56)?,
            shielded_slab_offset: u64_at(88)?,
            nullifier_slab_offset: u64_at(96)?,
            index_offset: u64_at(104)?,
        })
    }
}

impl SegmentIdentity {
    fn block_count(self) -> io::Result<u32> {
        let span = self
            .end
            .height
            .value()
            .checked_sub(self.start.value())
            .ok_or_else(|| invalid("reversed shielded segment interval"))?;
        span.checked_add(1)
            .ok_or_else(|| invalid("shielded segment interval exceeds its height type"))
    }

    /// The interval identity of a segment a writer is about to stage.
    pub(crate) fn span(
        network: Network,
        artifact_schema: u16,
        start: BlockHeight,
        end: BlockId,
    ) -> Self {
        Self {
            network,
            artifact_schema,
            start,
            end,
            sha256: [0; 32],
            verity: None,
        }
    }

    fn check_block(self, id: BlockId, parent: BlockHash, prior: Option<BlockId>) -> io::Result<()> {
        let expected_height = prior.map_or(Some(self.start), |previous| previous.height.next());
        if expected_height != Some(id.height)
            || prior.is_some_and(|previous| previous.hash != parent)
            || id.height > self.end.height
            || (id.height == self.end.height && id != self.end)
        {
            return Err(invalid(
                "block does not link to the shielded segment interval",
            ));
        }
        Ok(())
    }
}

/// Expected payload SHA-256 digests for both slabs of one block.
pub(super) struct SlabDigests {
    pub(super) shielded: [u8; 32],
    pub(super) nullifier: [u8; 32],
}

impl SlabDigests {
    fn for_slab(&self, slab: SealedSlab) -> &[u8; 32] {
        match slab {
            SealedSlab::Shielded => &self.shielded,
            SealedSlab::Nullifier => &self.nullifier,
        }
    }
}

/// Two 4-bit first-read validation verdicts packed into one byte per block:
/// low nibble for [`SealedSlab::Shielded`], high nibble for [`SealedSlab::Nullifier`].
pub(super) struct RowValidation(AtomicU8);

impl RowValidation {
    const UNVALIDATED: u8 = 0;
    const VALID: u8 = 1;
    const INVALID_DIGEST: u8 = 2;
    const INVALID_PROTOBUF: u8 = 3;
    const INVALID_IDENTITY: u8 = 4;

    pub(super) const fn new() -> Self {
        Self(AtomicU8::new(Self::UNVALIDATED))
    }

    const fn shift(slab: SealedSlab) -> u8 {
        match slab {
            SealedSlab::Shielded => 0,
            SealedSlab::Nullifier => 4,
        }
    }

    const fn encode(verdict: Result<(), PayloadError>) -> u8 {
        match verdict {
            Ok(()) => Self::VALID,
            Err(PayloadError::Digest) => Self::INVALID_DIGEST,
            Err(PayloadError::Protobuf) => Self::INVALID_PROTOBUF,
            Err(PayloadError::Identity) => Self::INVALID_IDENTITY,
        }
    }

    const fn decode(code: u8) -> Option<Result<(), PayloadError>> {
        match code {
            Self::VALID => Some(Ok(())),
            Self::INVALID_DIGEST => Some(Err(PayloadError::Digest)),
            Self::INVALID_PROTOBUF => Some(Err(PayloadError::Protobuf)),
            Self::INVALID_IDENTITY => Some(Err(PayloadError::Identity)),
            _ => None,
        }
    }

    fn load(&self, slab: SealedSlab) -> Option<Result<(), PayloadError>> {
        let nibble = (self.0.load(Ordering::Acquire) >> Self::shift(slab)) & 0x0f;
        Self::decode(nibble)
    }

    fn store_if_unvalidated(
        &self,
        slab: SealedSlab,
        verdict: Result<(), PayloadError>,
    ) -> Result<(), PayloadError> {
        let shift = Self::shift(slab);
        let encoded = Self::encode(verdict);
        match self
            .0
            .try_update(Ordering::AcqRel, Ordering::Acquire, |bits| {
                let current = (bits >> shift) & 0x0f;
                if Self::decode(current).is_some() {
                    None
                } else {
                    Some((bits & !(0x0f << shift)) | (encoded << shift))
                }
            }) {
            Ok(_) => verdict,
            Err(existing) => Self::decode((existing >> shift) & 0x0f).unwrap_or(verdict),
        }
    }
}

const _: () = assert!(std::mem::size_of::<BlockHash>() == 32);
const _: () = assert!(std::mem::size_of::<RowValidation>() == 1);
const _: () = assert!(std::mem::size_of::<SlabDigests>() == 64);

fn check_digest(expected: &[u8; 32], payload: &[u8]) -> Result<(), PayloadError> {
    if <[u8; 32]>::from(Sha256::digest(payload)) != *expected {
        return Err(PayloadError::Digest);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum PayloadError {
    #[error("shielded segment payload SHA-256 mismatch")]
    Digest,
    #[error("malformed shielded segment CompactBlock protobuf")]
    Protobuf,
    #[error("shielded segment payload height, hash, or parent differs from its index")]
    Identity,
}
pub(crate) struct SegmentWriter {
    identity: SegmentIdentity,
    target: PathBuf,
    staged: Option<BufWriter<NamedTempFile>>,
    /// Nullifier frames staged beside the segment until the shielded slab is
    /// complete, because the nullifier slab must follow the shielded slab
    /// contiguously on disk. Staging keeps memory independent of the segment
    /// length.
    nullifiers: BufWriter<NamedTempFile>,
    rows: Vec<[u8; ROW_META_LEN]>,
    shielded_rel: Vec<u32>,
    nullifier_rel: Vec<u32>,
    prior: Option<BlockId>,
}

impl SegmentWriter {
    pub(crate) fn create(
        path: &Path,
        (staged, nullifiers): (NamedTempFile, NamedTempFile),
        span: SegmentIdentity,
    ) -> io::Result<Self> {
        if span.start > span.end.height {
            return Err(invalid("reversed shielded segment interval"));
        }
        let identity = span;
        identity.block_count()?;
        // The header is rewritten with its final anchors and offsets once the
        // frames and index are in place, so a partial write publishes nothing.
        let mut staged = BufWriter::new(staged);
        staged.write_all(&[0; HEADER_LEN])?;
        Ok(Self {
            identity,
            target: path.to_owned(),
            staged: Some(staged),
            nullifiers: BufWriter::new(nullifiers),
            rows: Vec::new(),
            shielded_rel: vec![0],
            nullifier_rel: vec![0],
            prior: None,
        })
    }

    pub(crate) fn push(
        &mut self,
        block: CompactBlockArtifact,
        native_bytes: usize,
    ) -> io::Result<()> {
        let id = BlockId::new(block.height(), block.block_hash());
        self.identity
            .check_block(id, block.previous_block_hash(), self.prior)?;
        let parent = block.previous_block_hash();
        let shielded = CompactBlockShape::DEFAULT_RANGE
            .message(block.clone())
            .map_err(|status| io::Error::new(io::ErrorKind::InvalidData, status))?;
        let nullifiers = CompactBlockShape::DEFAULT_NULLIFIER_RANGE
            .message(block)
            .map_err(|status| io::Error::new(io::ErrorKind::InvalidData, status))?;
        let shielded_payload = shielded.encode_to_vec();
        let nullifier_payload = nullifiers.encode_to_vec();
        validate_lengths(native_bytes, shielded_payload.len())?;
        validate_lengths(native_bytes, nullifier_payload.len())?;
        let mut shielded_frame = [0; LENGTHS_LEN];
        shielded_frame[..4].copy_from_slice(&encode_len(native_bytes)?);
        shielded_frame[4..].copy_from_slice(&encode_len(shielded_payload.len())?);
        let mut nullifier_frame = [0; LENGTHS_LEN];
        nullifier_frame[..4].copy_from_slice(&encode_len(native_bytes)?);
        nullifier_frame[4..].copy_from_slice(&encode_len(nullifier_payload.len())?);
        // A partial write poisons the staged file so a later finish cannot publish it.
        let mut staged = self
            .staged
            .take()
            .ok_or_else(|| invalid("failed shielded segment write"))?;
        staged.write_all(&shielded_frame)?;
        staged.write_all(&shielded_payload)?;
        self.push_slab_offset(SealedSlab::Shielded, LENGTHS_LEN + shielded_payload.len())?;
        self.nullifiers.write_all(&nullifier_frame)?;
        self.nullifiers.write_all(&nullifier_payload)?;
        self.push_slab_offset(SealedSlab::Nullifier, LENGTHS_LEN + nullifier_payload.len())?;
        self.rows.push(index::encode_row(
            id.hash,
            parent,
            native_bytes,
            Sha256::digest(&shielded_payload).into(),
            Sha256::digest(&nullifier_payload).into(),
        )?);
        self.staged = Some(staged);
        self.prior = Some(id);
        Ok(())
    }

    fn push_slab_offset(&mut self, slab: SealedSlab, frame_len: usize) -> io::Result<()> {
        let rel = match slab {
            SealedSlab::Shielded => &mut self.shielded_rel,
            SealedSlab::Nullifier => &mut self.nullifier_rel,
        };
        let prior = *rel
            .last()
            .ok_or_else(|| invalid("failed shielded segment write"))?;
        let offset = u64::from(prior)
            .checked_add(frame_len as u64)
            .ok_or_else(|| invalid("shielded segment slab exceeds its offset range"))?;
        if offset > MAX_SLAB_BYTES {
            return Err(invalid("shielded segment slab exceeds its offset range"));
        }
        rel.push(
            u32::try_from(offset).map_err(|_| invalid("shielded segment slab offset overflow"))?,
        );
        Ok(())
    }

    /// Streams the staged nullifier slab behind the shielded slab, then
    /// flushes the segment so the index follows it.
    fn append_staged_nullifiers(
        &mut self,
        staged: &mut BufWriter<NamedTempFile>,
        nullifier_len: u64,
    ) -> io::Result<()> {
        self.nullifiers.flush()?;
        let spill = self.nullifiers.get_mut().as_file_mut();
        spill.seek(io::SeekFrom::Start(0))?;
        let copied = {
            let mut input =
                BufReader::with_capacity(STAGING_CHUNK, (&mut *spill).take(nullifier_len));
            io::copy(&mut input, staged)?
        };
        if copied != nullifier_len {
            return Err(invalid("staged nullifier slab is shorter than its index"));
        }
        // A surplus byte means the spill file and its offsets disagree.
        if spill.read(&mut [0])? != 0 {
            return Err(invalid("staged nullifier slab is longer than its index"));
        }
        staged.flush()
    }

    /// The segment's fixed header, from the writer's completed state.
    fn finished_header(
        &self,
        block_count: u32,
        nullifier_slab_offset: u64,
        index_offset: u64,
    ) -> io::Result<SegmentHeader> {
        let first_prev_hash: [u8; 32] = self
            .rows
            .first()
            .ok_or_else(|| invalid("incomplete shielded segment"))?[32..64]
            .try_into()
            .map_err(|_| invalid("incomplete shielded segment"))?;
        Ok(SegmentHeader {
            network_tag: self.identity.network.id(),
            artifact_schema_version: self.identity.artifact_schema,
            segment_flags: SEGMENT_FLAGS_HAS_NULLIFIER_SLAB,
            start_height: self.identity.start.value(),
            block_count,
            first_prev_hash,
            last_block_hash: encode_internal_block_hash(self.identity.end.hash),
            shielded_slab_offset: HEADER_LEN as u64,
            nullifier_slab_offset,
            index_offset,
        })
    }

    pub(crate) fn finish(self) -> io::Result<SegmentIdentity> {
        self.finish_with_dir_sync(true)
    }

    pub(crate) fn finish_staged(self) -> io::Result<SegmentIdentity> {
        self.finish_with_dir_sync(false)
    }

    fn finish_with_dir_sync(mut self, sync_directory: bool) -> io::Result<SegmentIdentity> {
        if self.prior != Some(self.identity.end) {
            return Err(invalid("incomplete shielded segment"));
        }
        let block_count = self.identity.block_count()?;
        if u32::try_from(self.rows.len()).ok() != Some(block_count) {
            return Err(invalid("incomplete shielded segment"));
        }
        let mut staged = self
            .staged
            .take()
            .ok_or_else(|| invalid("failed shielded segment write"))?;
        let shielded_len = u64::from(
            *self
                .shielded_rel
                .last()
                .ok_or_else(|| invalid("incomplete shielded segment"))?,
        );
        let nullifier_len = u64::from(
            *self
                .nullifier_rel
                .last()
                .ok_or_else(|| invalid("incomplete shielded segment"))?,
        );
        let nullifier_slab_offset = HEADER_LEN as u64 + shielded_len;
        let index_offset = nullifier_slab_offset + nullifier_len;
        self.append_staged_nullifiers(&mut staged, nullifier_len)?;
        write_index(
            &mut staged,
            (&self.shielded_rel, &self.nullifier_rel),
            &self.rows,
        )?;
        let header = self.finished_header(block_count, nullifier_slab_offset, index_offset)?;
        let staged = staged
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        // The staged file is a private temporary file, so this in-place header
        // patch cannot race a reader.
        write_all_at(staged.as_file(), &header.encode(), 0)?;
        let index_len = index::offset_tables_len(block_count)
            + u64::try_from(self.rows.len() * ROW_META_LEN)
                .map_err(|_| invalid("shielded segment index exceeds address space"))?;
        let body_len = index_offset
            .checked_add(index_len)
            .ok_or_else(|| invalid("shielded segment exceeds address space"))?;
        let (trailer, verity) = hash_staged(staged.as_file(), body_len)?;
        write_all_at(staged.as_file(), &trailer, body_len)?;
        staged.as_file().sync_all()?;
        let identity = SegmentIdentity {
            sha256: trailer,
            verity: Some(
                verity
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid("invalid fs-verity digest length"))?,
            ),
            ..self.identity
        };
        if let Err(failure) = staged.persist_noclobber(&self.target) {
            if failure.error.kind() != io::ErrorKind::AlreadyExists {
                return Err(failure.error);
            }
            // Keep the newly written inode for protection after authenticating the old staging name.
            SealedHistory::open(&self.target, identity)?;
            failure.file.persist(&self.target)?;
        }
        if sync_directory {
            File::open(parent_directory(&self.target))?.sync_all()?;
        }
        Ok(identity)
    }
}

/// Writes the offset tables and per-block metadata rows of one staged index.
fn write_index(
    staged: &mut BufWriter<NamedTempFile>,
    (shielded_rel, nullifier_rel): (&[u32], &[u32]),
    rows: &[[u8; ROW_META_LEN]],
) -> io::Result<()> {
    for offset in shielded_rel {
        staged.write_all(&offset.to_be_bytes())?;
    }
    for offset in nullifier_rel {
        staged.write_all(&offset.to_be_bytes())?;
    }
    for entry in rows {
        staged.write_all(entry)?;
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
        let mut offset = offset;
        while written < bytes.len() {
            let count = file.seek_write(&bytes[written..], offset)?;
            if count == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            written += count;
            offset += count as u64;
        }
        Ok(())
    }
}

/// Hashes the finished body, then the whole staged file behind it, in one pass.
fn hash_staged(file: &File, body_len: u64) -> io::Result<([u8; 32], Vec<u8>)> {
    const CHUNK: usize = STAGING_CHUNK;
    let mut digest = Sha256::new();
    let mut verity = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
    let mut input = BufReader::with_capacity(CHUNK, file);
    input.seek(io::SeekFrom::Start(0))?;
    let mut chunk = vec![0; CHUNK];
    let mut remaining = body_len;
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(chunk.len() as u64))
            .map_err(|_| invalid("shielded segment exceeds address space"))?;
        input.read_exact(&mut chunk[..wanted])?;
        digest.update(&chunk[..wanted]);
        verity.write_all(&chunk[..wanted])?;
        remaining -= wanted as u64;
    }
    let trailer: [u8; 32] = digest
        .finalize()
        .as_slice()
        .try_into()
        .map_err(|_| invalid("invalid shielded segment digest length"))?;
    verity.write_all(&trailer)?;
    Ok((trailer, verity.finalize().as_ref().to_vec()))
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

pub(crate) struct SealedHistory {
    pub(crate) kernel_verified: bool,
    identity: SegmentIdentity,
    first_parent: BlockHash,
    file: File,
    nullifier_base: u64,
    row_meta_base: u64,
    shielded_slab_len: u32,
    nullifier_slab_len: u32,
    block_hashes: Vec<BlockHash>,
    /// Stored compact-block bytes each row budgets, before pool filtering.
    native_bytes: Vec<u32>,
    /// Frame-start offsets of the shielded slab, relative to its base
    /// (`len == block_count`).
    shielded_rel: Vec<u32>,
    nullifier_rel: Vec<u32>,
    validation: Vec<RowValidation>,
    /// Retained only when `!kernel_verified` so portable reads re-verify
    /// payload SHA-256 on every read.
    portable_digests: Option<Vec<SlabDigests>>,
}

#[derive(Clone)]
pub(crate) struct SealedBlock {
    pub(crate) id: BlockId,
    pub(crate) parent: BlockHash,
    pub(crate) native_bytes: usize,
    pub(crate) message: EncodedBlock,
}

pub(crate) fn open_regular_file(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    let file = {
        use rustix::fs::{Mode, OFlags, open};
        File::from(open(
            path,
            OFlags::RDONLY | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?)
    };
    #[cfg(not(unix))]
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("expected a regular shielded history file"));
    }
    Ok(file)
}

impl SealedHistory {
    pub(crate) fn open(path: &Path, expected: SegmentIdentity) -> io::Result<Self> {
        Self::open_with_verity(path, expected, verity::measure)
    }

    fn open_with_verity(
        path: &Path,
        expected: SegmentIdentity,
        measure: impl FnOnce(&File) -> io::Result<Option<[u8; 32]>>,
    ) -> io::Result<Self> {
        let file = open_regular_file(path)?;
        let file_len = file.metadata()?.len();
        let protected = index::pinned_verity(expected.verity, || measure(&file))?;
        let index::AdmittedSegment {
            first_parent,
            nullifier_base,
            row_meta_base,
            shielded_slab_len,
            nullifier_slab_len,
            block_hashes,
            native_bytes,
            shielded_rel,
            nullifier_rel,
            validation,
            portable_digests,
        } = index::admit(&mut &file, expected, file_len, protected)?;
        Ok(Self {
            kernel_verified: protected,
            identity: expected,
            first_parent,
            file,
            // The shielded slab always starts right after the fixed header.
            nullifier_base,
            row_meta_base,
            shielded_slab_len,
            nullifier_slab_len,
            block_hashes,
            native_bytes,
            shielded_rel,
            nullifier_rel,
            validation,
            portable_digests,
        })
    }

    #[cfg(test)]
    fn heap_metadata_bytes(&self) -> usize {
        self.block_hashes.capacity() * std::mem::size_of::<BlockHash>()
            + self.native_bytes.capacity() * std::mem::size_of::<u32>()
            + self.shielded_rel.capacity() * std::mem::size_of::<u32>()
            + self.nullifier_rel.capacity() * std::mem::size_of::<u32>()
            + self.validation.capacity() * std::mem::size_of::<RowValidation>()
            + self.portable_digests.as_ref().map_or(0, |digests| {
                digests.capacity() * std::mem::size_of::<SlabDigests>()
            })
    }

    pub(crate) const fn first_parent(&self) -> BlockHash {
        self.first_parent
    }

    pub(crate) const fn identity(&self) -> SegmentIdentity {
        self.identity
    }

    fn height_at(&self, index: usize) -> io::Result<BlockHeight> {
        let offset = u32::try_from(index)
            .map_err(|_| invalid("shielded segment height exceeds address space"))?;
        self.identity
            .start
            .value()
            .checked_add(offset)
            .map(BlockHeight::new)
            .ok_or_else(|| invalid("shielded segment interval exceeds its height type"))
    }

    fn block_id(&self, index: usize) -> io::Result<BlockId> {
        Ok(BlockId::new(
            self.height_at(index)?,
            self.block_hashes[index],
        ))
    }

    fn parent_at(&self, index: usize) -> BlockHash {
        if index == 0 {
            self.first_parent
        } else {
            self.block_hashes[index - 1]
        }
    }

    fn native_bytes_at(&self, index: usize) -> io::Result<usize> {
        usize::try_from(self.native_bytes[index])
            .map_err(|_| invalid("shielded segment row exceeds address space"))
    }

    #[cfg(test)]
    pub(crate) fn read_chunk(
        &self,
        slab: SealedSlab,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
    ) -> io::Result<Vec<SealedBlock>> {
        self.bounded_rows(slab, range, order, max_bytes, true)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the slab, range, order, byte budget, and oversized-row exception describe one bounded read"
    )]
    pub(crate) fn bounded_rows(
        &self,
        slab: SealedSlab,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
        allow_oversized: bool,
    ) -> io::Result<Vec<SealedBlock>> {
        if range.start < self.identity.start
            || range.end > self.identity.end.height
            || range.start > range.end
            || range.end.value() - range.start.value() >= DEFAULT_MAX_COMPACT_BLOCK_RANGE.get()
        {
            return Err(invalid(
                "shielded chunk is outside admitted coverage or height limit",
            ));
        }
        let index = |height: BlockHeight| {
            usize::try_from(height.value() - self.identity.start.value())
                .map_err(|_| invalid("shielded segment height exceeds address space"))
        };
        let first = index(range.start)?;
        let last = index(range.end)?;
        let mut remaining = max_bytes;
        let mut count = 0;
        let mut iter = self.native_bytes[first..=last].iter();
        while let Some(&native) = match order {
            HeightOrder::Ascending => iter.next(),
            HeightOrder::Descending => iter.next_back(),
        } {
            let row_bytes = usize::try_from(native)
                .map_err(|_| invalid("shielded segment row exceeds address space"))?;
            if (count != 0 || !allow_oversized) && row_bytes > remaining {
                break;
            }
            count += 1;
            remaining = remaining.saturating_sub(row_bytes);
        }
        let selected = match order {
            HeightOrder::Ascending => first..first + count,
            HeightOrder::Descending => last + 1 - count..last + 1,
        };
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        self.read_payloads(slab, selected)
    }

    fn slab_rel(&self, slab: SealedSlab) -> &[u32] {
        match slab {
            SealedSlab::Shielded => &self.shielded_rel,
            SealedSlab::Nullifier => &self.nullifier_rel,
        }
    }

    const fn slab_len(&self, slab: SealedSlab) -> u32 {
        match slab {
            SealedSlab::Shielded => self.shielded_slab_len,
            SealedSlab::Nullifier => self.nullifier_slab_len,
        }
    }

    const fn slab_base(&self, slab: SealedSlab) -> u64 {
        match slab {
            SealedSlab::Shielded => HEADER_LEN as u64,
            SealedSlab::Nullifier => self.nullifier_base,
        }
    }

    fn read_payloads(
        &self,
        slab: SealedSlab,
        selected: std::ops::Range<usize>,
    ) -> io::Result<Vec<SealedBlock>> {
        let rel = self.slab_rel(slab);
        let slab_len = self.slab_len(slab);
        let first = selected.start;
        let last = selected.end - 1;
        let first_frame = self.slab_base(slab) + u64::from(rel[first]);
        let end_rel = rel_end(rel, slab_len, last)?;
        let len = usize::try_from(u64::from(end_rel) - u64::from(rel[first]))
            .map_err(|_| invalid("shielded chunk exceeds address space"))?;
        let mut plan = Vec::new();
        if selected.len() == 1 || len <= SLAB_TARGET_BYTES {
            plan.push((selected.clone(), len));
        } else {
            let mut start = first;
            let mut bytes = 0;
            for index in selected.clone() {
                let frame_len = frame_len(rel, slab_len, index)?;
                if bytes != 0 && bytes + frame_len > SLAB_TARGET_BYTES {
                    plan.push((start..index, bytes));
                    start = index;
                    bytes = 0;
                }
                bytes += frame_len;
            }
            plan.push((start..selected.end, bytes));
        }
        let mut offset = first_frame;
        let mut slabs = Vec::with_capacity(plan.len());
        let mut total_bytes_read = 0_u64;
        let read_started_at = std::time::Instant::now();
        for (rows, len) in plan {
            slabs.push((rows, read_slab_at(&self.file, len, offset)?));
            let slab_bytes_len = len as u64;
            offset += slab_bytes_len;
            total_bytes_read = total_bytes_read.saturating_add(slab_bytes_len);
        }
        record_published_history_read(slab, total_bytes_read, read_started_at.elapsed());
        let mut out = Vec::with_capacity(selected.len());
        for (rows, slab_bytes) in slabs {
            self.append_slab(slab, &mut out, rows, slab_bytes)?;
        }
        Ok(out)
    }

    fn append_slab(
        &self,
        slab: SealedSlab,
        out: &mut Vec<SealedBlock>,
        rows: std::ops::Range<usize>,
        mut slab_bytes: Vec<u8>,
    ) -> io::Result<()> {
        let rel = self.slab_rel(slab);
        let slab_len = self.slab_len(slab);
        let first_rel = rel[rows.start];
        let mut starts = Vec::with_capacity(rows.len());
        for index in rows.clone() {
            let frame_start = usize::try_from(u64::from(rel[index]) - u64::from(first_rel))
                .map_err(|_| invalid("shielded chunk exceeds address space"))?;
            self.verified_row_start(slab, index, &slab_bytes, frame_start)?;
            // The checked length field becomes the gRPC message prefix, so the
            // generated server's response body returns the row as a slice of
            // the slab instead of copying it into an encode buffer.
            slab_bytes[frame_start + LENGTHS_LEN - EncodedBlock::PREFIX_LEN] = 0;
            starts.push(frame_start + LENGTHS_LEN);
        }
        let slab_allocation = slab_bytes.capacity();
        let packed = Bytes::from(slab_bytes);
        let lazy_meta = self.load_unvalidated_meta(slab, rows.clone())?;
        for (index, payload_start) in rows.zip(starts) {
            let payload_len = frame_len(rel, slab_len, index)? - LENGTHS_LEN;
            // Every row of the slab keeps the whole slab alive.
            let message = EncodedBlock::from_length_prefixed_in(
                packed.slice(payload_start - EncodedBlock::PREFIX_LEN..payload_start + payload_len),
                slab_allocation,
            )
            .map_err(|_| invalid("shielded segment frame is not a gRPC message"))?;
            let lazy_digest = if self.kernel_verified && self.validation[index].load(slab).is_none()
            {
                digest_from_meta(lazy_meta.as_ref(), slab, index)?
            } else {
                None
            };
            self.verify_payload(slab, index, &message.payload(), lazy_digest.as_ref())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            out.push(SealedBlock {
                id: self.block_id(index)?,
                parent: self.parent_at(index),
                native_bytes: self.native_bytes_at(index)?,
                message,
            });
        }
        Ok(())
    }

    /// Reads the contiguous slice of on-disk row metadata covering any
    /// unvalidated rows in `rows` on `kernel_verified` segments.
    fn load_unvalidated_meta(
        &self,
        slab: SealedSlab,
        rows: std::ops::Range<usize>,
    ) -> io::Result<Option<(usize, Vec<u8>)>> {
        if !self.kernel_verified {
            return Ok(None);
        }
        let mut bounds: Option<(usize, usize)> = None;
        for index in rows {
            if self.validation[index].load(slab).is_none() {
                bounds = Some(match bounds {
                    Some((first, _)) => (first, index),
                    None => (index, index),
                });
            }
        }
        let Some((first, last)) = bounds else {
            return Ok(None);
        };
        let row_offset = u64::try_from(first)
            .map_err(|_| invalid("shielded segment row exceeds address space"))?
            .checked_mul(ROW_META_LEN as u64)
            .ok_or_else(|| invalid("shielded segment metadata exceeds address space"))?;
        let offset = self
            .row_meta_base
            .checked_add(row_offset)
            .ok_or_else(|| invalid("shielded segment metadata exceeds address space"))?;
        let len = (last - first + 1)
            .checked_mul(ROW_META_LEN)
            .ok_or_else(|| invalid("shielded segment metadata exceeds address space"))?;
        let meta_bytes = read_slab_at(&self.file, len, offset)?;
        Ok(Some((first, meta_bytes)))
    }

    fn verify_payload(
        &self,
        slab: SealedSlab,
        index: usize,
        payload: &Bytes,
        lazy_digest: Option<&[u8; 32]>,
    ) -> Result<(), PayloadError> {
        // Portable files can change between reads. Only authenticated bytes share a verdict.
        if !self.kernel_verified {
            let Some(digest) = self
                .portable_digests
                .as_ref()
                .and_then(|digests| digests.get(index))
                .map(|entry| entry.for_slab(slab))
            else {
                return Err(PayloadError::Digest);
            };
            check_digest(digest, payload)?;
        }
        if let Some(cached) = self.validation[index].load(slab) {
            return cached;
        }
        let verdict = (|| {
            if self.kernel_verified {
                let Some(digest) = lazy_digest else {
                    return Err(PayloadError::Digest);
                };
                check_digest(digest, payload)?;
            }
            let Ok(id) = self.block_id(index) else {
                return Err(PayloadError::Identity);
            };
            let parent = self.parent_at(index);
            let block = lightwalletd::CompactBlock::decode(payload.clone())
                .map_err(|_| PayloadError::Protobuf)?;
            if block.height != u64::from(id.height.value())
                || block.hash != encode_internal_block_hash(id.hash)
                || block.prev_hash != encode_internal_block_hash(parent)
            {
                return Err(PayloadError::Identity);
            }
            Ok(())
        })();
        self.validation[index].store_if_unvalidated(slab, verdict)
    }

    fn verified_row_start(
        &self,
        slab: SealedSlab,
        index: usize,
        raw: &[u8],
        frame_start: usize,
    ) -> io::Result<()> {
        let lengths: [u8; LENGTHS_LEN] = raw
            .get(frame_start..frame_start + LENGTHS_LEN)
            .and_then(|field| field.try_into().ok())
            .ok_or_else(|| invalid("missing shielded segment frame lengths"))?;
        let frame_len = frame_len(self.slab_rel(slab), self.slab_len(slab), index)?;
        let (native, wire) = decode_lengths(lengths)?;
        if native != self.native_bytes_at(index)? || wire != frame_len - LENGTHS_LEN {
            return Err(invalid(
                "shielded segment frame lengths differ from its index",
            ));
        }
        Ok(())
    }
}

fn digest_from_meta(
    meta: Option<&(usize, Vec<u8>)>,
    slab: SealedSlab,
    index: usize,
) -> io::Result<Option<[u8; 32]>> {
    let Some((first, bytes)) = meta else {
        return Ok(None);
    };
    let row_offset = index
        .checked_sub(*first)
        .and_then(|delta| delta.checked_mul(ROW_META_LEN))
        .ok_or_else(|| invalid("short shielded segment metadata row"))?;
    let digest_offset = row_offset
        + match slab {
            SealedSlab::Shielded => 72,
            SealedSlab::Nullifier => 104,
        };
    let digest = bytes
        .get(digest_offset..digest_offset + 32)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| invalid("short shielded segment metadata row"))?;
    Ok(Some(digest))
}

fn rel_end(rel: &[u32], slab_len: u32, index: usize) -> io::Result<u32> {
    if index + 1 == rel.len() {
        Ok(slab_len)
    } else {
        rel.get(index + 1)
            .copied()
            .ok_or_else(|| invalid("shielded segment offset table is short"))
    }
}

/// A row's whole frame length: its lengths field plus its payload.
fn frame_len(rel: &[u32], slab_len: u32, index: usize) -> io::Result<usize> {
    let offset = rel
        .get(index)
        .ok_or_else(|| invalid("shielded segment offset table is short"))?;
    let next = rel_end(rel, slab_len, index)?;
    let span = u64::from(next)
        .checked_sub(u64::from(*offset))
        .ok_or_else(|| invalid("shielded segment frame offsets are not ascending"))?;
    usize::try_from(span).map_err(|_| invalid("shielded segment frame offsets are not ascending"))
}

/// Reads `len` bytes at `offset` into a new slab.
#[cfg(unix)]
fn read_slab_at(file: &File, len: usize, mut offset: u64) -> io::Result<Vec<u8>> {
    use rustix::{
        buffer::spare_capacity,
        io::{pread, retry_on_intr},
    };

    let mut slab = Vec::with_capacity(len);
    while slab.len() < len {
        // Reading into spare capacity spares the slab a zero fill that the read
        // would overwrite.
        let count = retry_on_intr(|| pread(file, spare_capacity(&mut slab), offset))?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        offset += count as u64;
    }
    // A capacity above `len` may read bytes past the slab.
    slab.truncate(len);
    Ok(slab)
}

#[cfg(windows)]
fn read_slab_at(file: &File, len: usize, mut offset: u64) -> io::Result<Vec<u8>> {
    use std::os::windows::fs::FileExt;
    let mut slab = vec![0; len];
    let mut buffer = slab.as_mut_slice();
    while !buffer.is_empty() {
        let count = file.seek_read(buffer, offset)?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        offset += count as u64;
        buffer = &mut buffer[count..];
    }
    Ok(slab)
}

fn validate_lengths(native_bytes: usize, payload_len: usize) -> io::Result<()> {
    // The shielded projection drops fields and stores both block hashes in binary.
    if payload_len == 0 || payload_len > MAX_PAYLOAD_BYTES || payload_len > native_bytes {
        return Err(invalid("invalid shielded segment row lengths"));
    }
    Ok(())
}

fn encode_len(len: usize) -> io::Result<[u8; 4]> {
    u32::try_from(len)
        .map(u32::to_be_bytes)
        .map_err(|_| invalid("shielded segment row length overflow"))
}

fn decode_lengths(lengths: [u8; LENGTHS_LEN]) -> io::Result<(usize, usize)> {
    let (native, wire) = lengths.split_at(4);
    let native = decode_len(native)?;
    let wire = decode_len(wire)?;
    validate_lengths(native, wire)?;
    Ok((native, wire))
}

fn decode_len(encoded: &[u8]) -> io::Result<usize> {
    let bytes = encoded
        .try_into()
        .map_err(|_| invalid("invalid shielded segment length field"))?;
    usize::try_from(u32::from_be_bytes(bytes))
        .map_err(|_| invalid("shielded segment row exceeds address space"))
}

fn decode_hash(encoded: &[u8]) -> io::Result<BlockHash> {
    decode_internal_block_hash(encoded).map_err(|_| invalid("invalid shielded segment block hash"))
}

fn invalid(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod telemetry_tests {
    use super::{SealedSlab, record_published_history_read, register_published_history_metrics};

    #[test]
    fn published_history_read_records_bytes_and_duration_for_both_slabs() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);

        register_published_history_metrics();
        let before = handle.render();
        assert!(
            before
                .contains("zinder_compat_published_history_read_bytes_total{slab=\"shielded\"} 0")
        );
        assert!(
            before
                .contains("zinder_compat_published_history_read_bytes_total{slab=\"nullifier\"} 0")
        );

        record_published_history_read(
            SealedSlab::Shielded,
            4_096,
            std::time::Duration::from_millis(2),
        );
        record_published_history_read(
            SealedSlab::Nullifier,
            1_024,
            std::time::Duration::from_millis(1),
        );

        let after = handle.render();
        assert!(
            after.contains(
                "zinder_compat_published_history_read_bytes_total{slab=\"shielded\"} 4096"
            ),
            "missing shielded bytes counter in:\n{after}"
        );
        assert!(
            after.contains(
                "zinder_compat_published_history_read_bytes_total{slab=\"nullifier\"} 1024"
            ),
            "missing nullifier bytes counter in:\n{after}"
        );
        assert!(
            after.contains(
                "zinder_compat_published_history_read_duration_seconds_count{slab=\"shielded\"} 1"
            ),
            "missing shielded duration histogram in:\n{after}"
        );
        assert!(
            after.contains(
                "zinder_compat_published_history_read_duration_seconds_count{slab=\"nullifier\"} 1"
            ),
            "missing nullifier duration histogram in:\n{after}"
        );
    }
}
