//! Signed catalog segment for the default shielded `GetBlockRange`.

use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use prost::{Message, bytes::Bytes};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CompactBlockArtifact, Network,
    wire::{decode_internal_block_hash, encode_internal_block_hash},
};
use zinder_query::DEFAULT_MAX_COMPACT_BLOCK_RANGE;
use zinder_store::HeightOrder;

use crate::compact_block_messages::CompactBlockShape;

// The magic binds version 1 to the default shielded range projection. All integers are big-endian.
const MAGIC: &[u8; 8] = b"ZSHRNG01";
const HEADER_LEN: usize = 52;
const LENGTHS_LEN: usize = 8;
// Bound allocation before parsing a row from a file whose digest is still being checked.
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentIdentity {
    pub(crate) network: Network,
    pub(crate) start: BlockHeight,
    pub(crate) end: BlockId,
    pub(crate) sha256: [u8; 32],
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
    prior: Option<BlockId>,
}

impl SegmentWriter {
    pub(crate) fn create(
        path: &Path,
        network: Network,
        start: BlockHeight,
        end: BlockId,
    ) -> io::Result<Self> {
        let identity = SegmentIdentity {
            network,
            start,
            end,
            sha256: [0; 32],
        };
        let header = identity.header()?;
        let mut staged = BufWriter::new(NamedTempFile::new_in(parent_directory(path))?);
        staged.write_all(&header)?;
        Ok(Self {
            identity,
            target: path.to_owned(),
            staged: Some(staged),
            digest: Sha256::new_with_prefix(header),
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
        self.staged = Some(staged);
        self.digest.update(lengths);
        self.digest.update(payload);
        self.prior = Some(id);
        Ok(())
    }

    pub(crate) fn finish(self) -> io::Result<SegmentIdentity> {
        if self.prior != Some(self.identity.end) {
            return Err(invalid("incomplete shielded segment"));
        }
        let staged = self
            .staged
            .ok_or_else(|| invalid("failed shielded segment write"))?
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        staged.as_file().sync_all()?;
        let identity = SegmentIdentity {
            sha256: self.digest.finalize().into(),
            ..self.identity
        };
        if let Err(failure) = staged.persist_noclobber(&self.target) {
            if failure.error.kind() != io::ErrorKind::AlreadyExists {
                return Err(failure.error);
            }
            // A crash after the directory entry appeared can leave a complete earlier publication.
            SealedHistory::open(&self.target, identity)?;
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
    sha256: Option<[u8; 32]>,
}

pub(crate) struct SealedBlock {
    pub(crate) id: BlockId,
    pub(crate) parent: BlockHash,
    pub(crate) native_bytes: usize,
    pub(crate) payload: Bytes,
}

// Admission only needs the three identity fields from the vendored CompactBlock message.
// The publisher uses the complete release projection to encode each payload.
#[derive(Message)]
struct WireBlockLink {
    #[prost(uint64, tag = "2")]
    height: u64,
    #[prost(bytes, tag = "3")]
    hash: Vec<u8>,
    #[prost(bytes, tag = "4")]
    prev_hash: Vec<u8>,
}

impl SealedHistory {
    pub(crate) fn open(path: &Path, expected: SegmentIdentity) -> io::Result<Self> {
        let header = expected.header()?;
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        let count = u64::from(expected.end.height.value() - expected.start.value()) + 1;
        let file_len = metadata.len();
        // Each message contains two 32-byte hashes with one-byte tags and lengths.
        let minimum = HEADER_LEN as u64 + count * (LENGTHS_LEN as u64 + 68);
        let maximum = HEADER_LEN as u64 + count * (LENGTHS_LEN + MAX_PAYLOAD_BYTES) as u64;
        if !metadata.is_file() || !(minimum..=maximum).contains(&file_len) {
            return Err(invalid(
                "shielded segment file size does not fit its interval",
            ));
        }
        let protected = kernel_verifies_reads(&file);
        let mut input = BufReader::new(&file);
        let mut observed = [0; HEADER_LEN];
        input.read_exact(&mut observed)?;
        if observed != header {
            return Err(invalid(
                "shielded segment format, network, or interval mismatch",
            ));
        }
        let mut digest = Sha256::new_with_prefix(observed);
        let mut rows = Vec::new();
        let mut offset = HEADER_LEN as u64;
        let mut payload = Vec::new();
        let mut prior = None;
        for height in BlockHeightRange::inclusive(expected.start, expected.end.height) {
            let mut lengths = [0; LENGTHS_LEN];
            input.read_exact(&mut lengths)?;
            digest.update(lengths);
            let (native, wire) = lengths.split_at(4);
            let native_bytes = decode_len(native)?;
            let len = decode_len(wire)?;
            validate_lengths(native_bytes, len)?;
            offset += LENGTHS_LEN as u64;
            if offset + len as u64 > file_len {
                return Err(invalid("shielded segment row exceeds file size"));
            }
            payload.resize(len, 0);
            input.read_exact(&mut payload)?;
            digest.update(&payload);
            let link = WireBlockLink::decode(payload.as_slice())?;
            if link.height != u64::from(height.value()) {
                return Err(invalid("wrong shielded segment payload height"));
            }
            let id = BlockId::new(height, decode_hash(&link.hash)?);
            let parent = decode_hash(&link.prev_hash)?;
            expected.check_block(id, parent, prior)?;
            rows.push(IndexedBlock {
                id,
                parent,
                native_bytes,
                offset,
                len,
                sha256: (!protected).then(|| Sha256::digest(&payload).into()),
            });
            // At most 2^32 rows of at most MAX_PAYLOAD_BYTES fit the trusted interval.
            offset += len as u64;
            prior = Some(id);
        }
        if !input.fill_buf()?.is_empty() || <[u8; 32]>::from(digest.finalize()) != expected.sha256 {
            return Err(invalid("shielded segment length or SHA-256 mismatch"));
        }
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
        let relative = |row: &IndexedBlock| {
            usize::try_from(row.offset - first.offset)
                .map_err(|_| invalid("shielded chunk exceeds address space"))
        };
        let len = relative(last)?
            .checked_add(last.len)
            .ok_or_else(|| invalid("shielded chunk exceeds address space"))?;
        let mut packed = vec![0; len];
        read_exact_at(&self.file, &mut packed, first.offset)?;
        let packed = Bytes::from(packed);
        selected
            .iter()
            .map(|row| {
                let start = relative(row)?;
                let payload = packed.slice(start..start + row.len);
                if row
                    .sha256
                    .is_some_and(|expected| <[u8; 32]>::from(Sha256::digest(&payload)) != expected)
                {
                    return Err(invalid("shielded segment payload SHA-256 mismatch"));
                }
                Ok(SealedBlock {
                    id: row.id,
                    parent: row.parent,
                    native_bytes: row.native_bytes,
                    payload,
                })
            })
            .collect()
    }
}

#[cfg(target_os = "linux")]
fn kernel_verifies_reads(file: &File) -> bool {
    use rustix::fs::{AtFlags, StatxAttributes, StatxFlags, fstatfs, statx};

    // Restrict the attestation to kernel filesystems implementing fs-verity.
    // A userspace or network filesystem can supply its own statx attributes.
    let native = fstatfs(file).is_ok_and(|stats| {
        matches!(
            i128::from(stats.f_type) & 0xffff_ffff,
            0xef53 | 0x9123_683e | 0xf2f5_2010
        )
    });
    native
        && statx(file, c"", AtFlags::EMPTY_PATH, StatxFlags::empty()).is_ok_and(|stat| {
            stat.stx_attributes_mask.contains(StatxAttributes::VERITY)
                && stat.stx_attributes.contains(StatxAttributes::VERITY)
        })
}

#[cfg(not(target_os = "linux"))]
fn kernel_verifies_reads(_file: &File) -> bool {
    false
}

#[cfg(unix)]
fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buffer, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buffer.is_empty() {
        let count = file.seek_read(buffer, offset)?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        offset += count as u64;
        buffer = &mut buffer[count..];
    }
    Ok(())
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
