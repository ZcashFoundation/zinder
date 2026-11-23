//! Signed catalog segment for the default shielded `GetBlockRange`.

use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, Write},
    path::{Path, PathBuf},
    sync::OnceLock,
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

// The magic binds version 2 to the default shielded range projection. All integers are big-endian.
const MAGIC: &[u8; 8] = b"ZSHRNG02";
const HEADER_LEN: usize = 52;
const LENGTHS_LEN: usize = 8;
// A row's length field ends with its big-endian payload length, so its last
// bytes can carry the gRPC message prefix.
const _: () = assert!(LENGTHS_LEN >= EncodedBlock::PREFIX_LEN);
const INDEX_LEN: usize = 112;
mod index;
pub(crate) mod verity;
// Bound the encoded bytes allocated for one authenticated row.
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const SLAB_TARGET_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentIdentity {
    pub(crate) network: Network,
    pub(crate) start: BlockHeight,
    pub(crate) end: BlockId,
    pub(crate) sha256: [u8; 32],
    pub(crate) verity: Option<[u8; 32]>,
}

impl SegmentIdentity {
    fn header(self) -> io::Result<[u8; HEADER_LEN]> {
        if self.start > self.end.height {
            return Err(invalid("reversed shielded segment interval"));
        }
        let mut header = [0; HEADER_LEN];
        header[..8].copy_from_slice(MAGIC);
        header[8..12].copy_from_slice(&self.network.id().to_be_bytes());
        header[12..16].copy_from_slice(&self.start.value().to_be_bytes());
        header[16..20].copy_from_slice(&self.end.height.value().to_be_bytes());
        header[20..].copy_from_slice(&encode_internal_block_hash(self.end.hash));
        Ok(header)
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

pub(crate) struct SegmentWriter {
    identity: SegmentIdentity,
    target: PathBuf,
    staged: Option<BufWriter<NamedTempFile>>,
    digest: Sha256,
    verity: Box<dyn fs_verity::DynFsVerityDigest + 'static>,
    index: Vec<[u8; INDEX_LEN]>,
    offset: u64,
    prior: Option<BlockId>,
}

impl SegmentWriter {
    pub(crate) fn create(
        path: &Path,
        staged: NamedTempFile,
        network: Network,
        start: BlockHeight,
        end: BlockId,
    ) -> io::Result<Self> {
        let identity = SegmentIdentity {
            network,
            start,
            end,
            sha256: [0; 32],
            verity: None,
        };
        let header = identity.header()?;
        let mut verity = fs_verity::new_dyn(fs_verity::InnerHashAlgorithm::Sha256);
        verity.write_all(&header)?;
        let mut staged = BufWriter::new(staged);
        staged.write_all(&header)?;
        Ok(Self {
            identity,
            target: path.to_owned(),
            staged: Some(staged),
            digest: Sha256::new_with_prefix(header),
            verity,
            index: Vec::new(),
            offset: HEADER_LEN as u64,
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
        let message = CompactBlockShape::for_range(&[])
            .and_then(|shape| shape.message(block))
            .map_err(|status| io::Error::new(io::ErrorKind::InvalidData, status))?;
        let payload_len = message.encoded_len();
        validate_lengths(native_bytes, payload_len)?;
        let mut lengths = [0; LENGTHS_LEN];
        lengths[..4].copy_from_slice(&encode_len(native_bytes)?);
        lengths[4..].copy_from_slice(&encode_len(payload_len)?);
        let payload = message.encode_to_vec();
        // A partial write poisons the staged file so a later finish cannot publish it.
        let mut staged = self
            .staged
            .take()
            .ok_or_else(|| invalid("failed shielded segment write"))?;
        staged.write_all(&lengths)?;
        staged.write_all(&payload)?;
        self.verity.write_all(&lengths)?;
        self.verity.write_all(&payload)?;
        self.digest.update(lengths);
        self.digest.update(&payload);
        let offset = self.offset + LENGTHS_LEN as u64;
        self.index.push(index::encode(
            offset,
            lengths,
            id.hash,
            parent,
            Sha256::digest(&payload).into(),
        ));
        self.offset = offset + payload.len() as u64;
        self.staged = Some(staged);
        self.prior = Some(id);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> io::Result<SegmentIdentity> {
        if self.prior != Some(self.identity.end) {
            return Err(invalid("incomplete shielded segment"));
        }
        let mut staged = self
            .staged
            .take()
            .ok_or_else(|| invalid("failed shielded segment write"))?;
        for entry in self.index {
            staged.write_all(&entry)?;
            self.digest.update(entry);
            self.verity.write_all(&entry)?;
        }
        let staged = staged
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        staged.as_file().sync_all()?;
        let identity = SegmentIdentity {
            sha256: self.digest.finalize().into(),
            verity: Some(
                self.verity
                    .finalize()
                    .as_ref()
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
        File::open(parent_directory(&self.target))?.sync_all()?;
        Ok(identity)
    }
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

pub(crate) struct SealedHistory {
    pub(crate) kernel_verified: bool,
    identity: SegmentIdentity,
    file: File,
    rows: Vec<IndexedBlock>,
}

struct IndexedBlock {
    id: BlockId,
    parent: BlockHash,
    native_bytes: usize,
    offset: u64,
    len: usize,
    sha256: [u8; 32],
    validation: OnceLock<Result<(), PayloadError>>,
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
enum PayloadError {
    #[error("shielded segment payload SHA-256 mismatch")]
    Digest,
    #[error("malformed shielded segment CompactBlock protobuf")]
    Protobuf,
    #[error("shielded segment payload height, hash, or parent differs from its index")]
    Identity,
}

impl IndexedBlock {
    fn check_digest(&self, payload: &[u8]) -> Result<(), PayloadError> {
        if <[u8; 32]>::from(Sha256::digest(payload)) != self.sha256 {
            return Err(PayloadError::Digest);
        }
        Ok(())
    }

    fn verify_payload(&self, payload: &Bytes, kernel_verified: bool) -> Result<(), PayloadError> {
        // Portable files can change between reads. Only authenticated bytes share a verdict.
        if !kernel_verified {
            self.check_digest(payload)?;
        }
        *self.validation.get_or_init(|| {
            if kernel_verified {
                self.check_digest(payload)?;
            }
            let block = lightwalletd::CompactBlock::decode(payload.clone())
                .map_err(|_| PayloadError::Protobuf)?;
            if block.height != u64::from(self.id.height.value())
                || block.hash != encode_internal_block_hash(self.id.hash)
                || block.prev_hash != encode_internal_block_hash(self.parent)
            {
                return Err(PayloadError::Identity);
            }
            Ok(())
        })
    }
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
        let file = open_regular_file(path)?;
        let file_len = file.metadata()?.len();
        let protected = index::pinned_verity(expected.verity, || verity::measure(&file))?;
        let rows = index::admit(&mut &file, expected, file_len, protected)?;
        Ok(Self {
            kernel_verified: protected,
            identity: expected,
            file,
            rows,
        })
    }

    pub(crate) fn first_parent(&self) -> BlockHash {
        self.rows[0].parent
    }

    pub(crate) const fn identity(&self) -> SegmentIdentity {
        self.identity
    }

    #[cfg(test)]
    pub(crate) fn read_chunk(
        &self,
        range: BlockHeightRange,
        order: HeightOrder,
        max_bytes: usize,
    ) -> io::Result<Vec<SealedBlock>> {
        self.bounded_rows(range, order, max_bytes, true)
    }

    pub(crate) fn bounded_rows(
        &self,
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
        let candidates = &self.rows[index(range.start)?..=index(range.end)?];
        let mut remaining = max_bytes;
        let mut count = 0;
        let mut iter = candidates.iter();
        while let Some(row) = match order {
            HeightOrder::Ascending => iter.next(),
            HeightOrder::Descending => iter.next_back(),
        } {
            if (count != 0 || !allow_oversized) && row.native_bytes > remaining {
                break;
            }
            count += 1;
            remaining = remaining.saturating_sub(row.native_bytes);
        }
        let selected = match order {
            HeightOrder::Ascending => &candidates[..count],
            HeightOrder::Descending => &candidates[candidates.len() - count..],
        };
        if selected.is_empty() {
            return Ok(Vec::new());
        }
        self.read_payloads(selected)
    }

    fn read_payloads(&self, selected: &[IndexedBlock]) -> io::Result<Vec<SealedBlock>> {
        let first = selected
            .first()
            .ok_or_else(|| invalid("empty shielded chunk"))?;
        let last = selected
            .last()
            .ok_or_else(|| invalid("empty shielded chunk"))?;
        let first_frame = first.offset - LENGTHS_LEN as u64;
        let len = relative_frame_offset(last, first_frame)?
            .checked_add(last.len)
            .ok_or_else(|| invalid("shielded chunk exceeds address space"))?;
        let mut plan = Vec::new();
        if selected.len() == 1 || len <= SLAB_TARGET_BYTES {
            plan.push((selected, len));
        } else {
            let mut start = 0;
            let mut bytes = 0;
            for (index, row) in selected.iter().enumerate() {
                let frame_len = LENGTHS_LEN + row.len;
                if bytes != 0 && bytes + frame_len > SLAB_TARGET_BYTES {
                    plan.push((&selected[start..index], bytes));
                    start = index;
                    bytes = 0;
                }
                bytes += frame_len;
            }
            plan.push((&selected[start..], bytes));
        }
        let mut offset = first_frame;
        let mut slabs = Vec::with_capacity(plan.len());
        for (rows, len) in plan {
            slabs.push((rows, read_slab_at(&self.file, len, offset)?));
            offset += len as u64;
        }
        let mut out = Vec::with_capacity(selected.len());
        for (rows, slab) in slabs {
            self.append_slab(&mut out, rows, slab)?;
        }
        Ok(out)
    }

    fn append_slab(
        &self,
        out: &mut Vec<SealedBlock>,
        rows: &[IndexedBlock],
        mut slab: Vec<u8>,
    ) -> io::Result<()> {
        let first_frame = rows[0].offset - LENGTHS_LEN as u64;
        let mut starts = Vec::with_capacity(rows.len());
        for row in rows {
            let start = verified_row_start(row, &slab, first_frame)?;
            // The checked length field becomes the gRPC message prefix, so the
            // generated server's response body returns the row as a slice of
            // the slab instead of copying it into an encode buffer.
            slab[start - EncodedBlock::PREFIX_LEN] = 0;
            starts.push(start);
        }
        let slab_allocation = slab.capacity();
        let packed = Bytes::from(slab);
        for (row, start) in rows.iter().zip(starts) {
            // Every row of the slab keeps the whole slab alive.
            let message = EncodedBlock::from_length_prefixed_in(
                packed.slice(start - EncodedBlock::PREFIX_LEN..start + row.len),
                slab_allocation,
            )
            .map_err(|_| invalid("shielded segment frame is not a gRPC message"))?;
            out.push(verify_and_build_block(row, message, self.kernel_verified)?);
        }
        Ok(())
    }
}

fn relative_frame_offset(row: &IndexedBlock, first_frame: u64) -> io::Result<usize> {
    usize::try_from(row.offset - first_frame)
        .map_err(|_| invalid("shielded chunk exceeds address space"))
}

fn verified_row_start(row: &IndexedBlock, raw: &[u8], first_frame: u64) -> io::Result<usize> {
    let start = relative_frame_offset(row, first_frame)?;
    let lengths = raw[..start]
        .last_chunk()
        .ok_or_else(|| invalid("missing shielded segment frame lengths"))?;
    if decode_lengths(*lengths)? != (row.native_bytes, row.len) {
        return Err(invalid(
            "shielded segment frame lengths differ from its index",
        ));
    }
    Ok(start)
}

fn verify_and_build_block(
    row: &IndexedBlock,
    message: EncodedBlock,
    kernel_verified: bool,
) -> io::Result<SealedBlock> {
    row.verify_payload(&message.payload(), kernel_verified)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(SealedBlock {
        id: row.id,
        parent: row.parent,
        native_bytes: row.native_bytes,
        message,
    })
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
