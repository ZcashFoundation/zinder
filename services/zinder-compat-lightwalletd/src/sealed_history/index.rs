//! Admission of one dual-slab segment against its signed identity.
//!
//! A portable segment is streamed once: both slabs' frames are hashed into
//! the whole-body digest and each frame's payload digest is recorded, then
//! the offset tables and per-row metadata are checked against exactly what
//! was streamed. A kernel-verified segment skips the payload scan; fs-verity
//! pins its bytes, so admission reads only the header, tables, and metadata.

use std::io::{BufRead, BufReader};

use super::{
    BlockHash, BlockHeight, BlockId, Digest as _, HEADER_LEN, LENGTHS_LEN, MAX_PAYLOAD_BYTES,
    ROW_META_LEN, Read, RowValidation, SegmentHeader, SegmentIdentity, Sha256, SlabDigests,
    TRAILER_LEN, decode_hash, decode_lengths, encode_internal_block_hash, encode_len, invalid, io,
};

/// Bytes of the two `[u32; block_count + 1]` relative offset tables.
pub(super) fn offset_tables_len(block_count: u32) -> u64 {
    2 * (u64::from(block_count) + 1) * 4
}

pub(super) fn pinned_verity(
    expected: Option<[u8; 32]>,
    measure: impl FnOnce() -> io::Result<Option<[u8; 32]>>,
) -> io::Result<bool> {
    let Some(pin) = expected else {
        return Ok(false);
    };
    match measure()? {
        Some(actual) if actual != pin => Err(invalid("shielded segment fs-verity digest mismatch")),
        Some(_) => Ok(true),
        None => Ok(false),
    }
}

/// Encodes one per-block metadata row: identity, native length, and the
/// payload digests of both slabs.
pub(super) fn encode_row(
    hash: BlockHash,
    parent: BlockHash,
    native_bytes: usize,
    shielded_sha256: [u8; 32],
    nullifier_sha256: [u8; 32],
) -> io::Result<[u8; ROW_META_LEN]> {
    let mut row = [0; ROW_META_LEN];
    row[..32].copy_from_slice(&encode_internal_block_hash(hash));
    row[32..64].copy_from_slice(&encode_internal_block_hash(parent));
    row[64..68].copy_from_slice(&encode_len(native_bytes)?);
    row[72..104].copy_from_slice(&shielded_sha256);
    row[104..].copy_from_slice(&nullifier_sha256);
    Ok(row)
}

/// What one streaming pass over a portable segment recorded for one slab.
struct StreamedSlab {
    /// Frame-start offsets relative to the slab base, plus its total length.
    rel: Vec<u32>,
    wire: Vec<usize>,
    native: Vec<usize>,
    digests: Vec<[u8; 32]>,
}

impl StreamedSlab {
    fn new() -> Self {
        Self {
            rel: vec![0],
            wire: Vec::new(),
            native: Vec::new(),
            digests: Vec::new(),
        }
    }
}

/// One slab's `[u32; count]` frame-start offsets plus the trailing slab length.
struct OffsetTable {
    starts: Vec<u32>,
    slab_len: u32,
}

/// Validates both slabs' relative offset tables against the header's
/// offsets, before any span arithmetic or row decoding runs.
///
/// Each table must start at its slab base, every adjacent span must be
/// ascending, and its last entry must be the slab's exact byte length.
fn validate_tables(
    header: &SegmentHeader,
    (shielded_table, nullifier_table): (&OffsetTable, &OffsetTable),
    count: usize,
) -> io::Result<()> {
    let shielded_len = header
        .nullifier_slab_offset
        .checked_sub(HEADER_LEN as u64)
        .ok_or_else(|| invalid("shielded segment index precedes its slabs"))?;
    let nullifier_len = header
        .index_offset
        .checked_sub(header.nullifier_slab_offset)
        .ok_or_else(|| invalid("shielded segment index precedes its slabs"))?;
    validate_offset_table(shielded_table, count, shielded_len)?;
    validate_offset_table(nullifier_table, count, nullifier_len)?;
    Ok(())
}

/// Validates one slab's `[u32; count + 1]` relative offset table.
fn validate_offset_table(table: &OffsetTable, count: usize, slab_len: u64) -> io::Result<()> {
    if table.starts.len() != count || count == 0 {
        return Err(invalid("shielded segment offset table is short"));
    }
    if u64::from(table.starts[0]) != 0 {
        return Err(invalid(
            "shielded segment offset table does not start at its slab",
        ));
    }
    if table
        .starts
        .windows(2)
        .any(|pair| u64::from(pair[1]) <= u64::from(pair[0]))
        || table
            .starts
            .last()
            .is_some_and(|last| u64::from(table.slab_len) <= u64::from(*last))
    {
        return Err(invalid(
            "shielded segment offset table spans are not ascending",
        ));
    }
    if u64::from(table.slab_len) != slab_len {
        return Err(invalid(
            "shielded segment offset table ends away from its slab",
        ));
    }
    Ok(())
}

/// The segment state admission hands to the serving history.
pub(super) struct AdmittedSegment {
    pub(super) first_parent: BlockHash,
    /// Absolute file offset of the nullifier slab's base.
    pub(super) nullifier_base: u64,
    /// Absolute file offset of the per-block metadata rows.
    pub(super) row_meta_base: u64,
    pub(super) shielded_slab_len: u32,
    pub(super) nullifier_slab_len: u32,
    pub(super) block_hashes: Vec<BlockHash>,
    pub(super) native_bytes: Vec<u32>,
    pub(super) shielded_rel: Vec<u32>,
    pub(super) nullifier_rel: Vec<u32>,
    pub(super) validation: Vec<RowValidation>,
    pub(super) portable_digests: Option<Vec<SlabDigests>>,
}

struct DecodedRows {
    first_parent: BlockHash,
    shielded_slab_len: u32,
    nullifier_slab_len: u32,
    block_hashes: Vec<BlockHash>,
    native_bytes: Vec<u32>,
    shielded_rel: Vec<u32>,
    nullifier_rel: Vec<u32>,
    validation: Vec<RowValidation>,
    portable_digests: Option<Vec<SlabDigests>>,
}

/// The header, interval, and file-size checks every admission starts with.
fn admit_header(
    expected: SegmentIdentity,
    file_len: u64,
    header_bytes: &[u8; HEADER_LEN],
) -> io::Result<SegmentHeader> {
    let header = SegmentHeader::decode(header_bytes)?;
    let block_count = expected.block_count()?;
    if header.network_tag != expected.network.id()
        || header.artifact_schema_version != expected.artifact_schema
        || header.segment_flags != super::SEGMENT_FLAGS_HAS_NULLIFIER_SLAB
        || header.start_height != expected.start.value()
        || header.block_count != block_count
        || header.last_block_hash != encode_internal_block_hash(expected.end.hash)
        || header.shielded_slab_offset != HEADER_LEN as u64
    {
        return Err(invalid(
            "shielded segment format, network, schema, or interval mismatch",
        ));
    }
    let index_len = offset_tables_len(block_count)
        .checked_add(u64::from(block_count) * ROW_META_LEN as u64)
        .ok_or_else(|| invalid("shielded segment index exceeds address space"))?;
    let body_len = header
        .index_offset
        .checked_add(index_len)
        .and_then(|end| end.checked_add(TRAILER_LEN as u64))
        .ok_or_else(|| invalid("shielded segment exceeds address space"))?;
    if file_len != body_len {
        return Err(invalid(
            "shielded segment file size does not fit its interval",
        ));
    }
    Ok(header)
}

/// Admits a kernel-verified segment: fs-verity pins its bytes, so admission
/// reads only the offset tables and metadata rows behind the header.
fn admit_protected(
    input: &mut (impl Read + std::io::Seek),
    expected: SegmentIdentity,
    header: &SegmentHeader,
) -> io::Result<AdmittedSegment> {
    let count = usize::try_from(expected.block_count()?)
        .map_err(|_| invalid("shielded segment height exceeds address space"))?;
    input.seek(std::io::SeekFrom::Start(header.index_offset))?;
    let shielded_table = read_offset_table(&mut *input, count)?;
    let nullifier_table = read_offset_table(&mut *input, count)?;
    let metadata = read_metadata(&mut *input, count)?;
    validate_tables(header, (&shielded_table, &nullifier_table), count)?;
    admitted(
        expected,
        header,
        decode_rows(
            expected,
            header,
            (shielded_table, nullifier_table),
            metadata,
            None,
        )?,
    )
}

/// Admits a portable segment by streaming it exactly once.
fn admit_portable(
    input: &mut (impl Read + std::io::Seek),
    expected: SegmentIdentity,
    header: &SegmentHeader,
    header_bytes: &[u8],
) -> io::Result<AdmittedSegment> {
    let count = usize::try_from(expected.block_count()?)
        .map_err(|_| invalid("shielded segment height exceeds address space"))?;
    let mut digest = Sha256::new();
    digest.update(header_bytes);
    let mut input = BufReader::new(input);
    let region = header
        .index_offset
        .checked_sub(HEADER_LEN as u64)
        .ok_or_else(|| invalid("shielded segment index precedes its slabs"))?;
    let shielded = hash_frames(&mut input, count, region, &mut digest)?;
    let nullifiers = hash_frames(&mut input, count, region, &mut digest)?;
    if u64::from(shielded.rel[count]) + u64::from(nullifiers.rel[count]) != region {
        return Err(invalid("shielded segment slabs differ from their index"));
    }
    let shielded_table = hash_table(&mut input, count, &mut digest)?;
    let nullifier_table = hash_table(&mut input, count, &mut digest)?;
    let metadata = hash_metadata(&mut input, count, &mut digest)?;
    validate_tables(header, (&shielded_table, &nullifier_table), count)?;
    let mut trailer = [0; TRAILER_LEN];
    input.read_exact(&mut trailer)?;
    // The trailer carries the body digest the catalog's identity pins.
    let body_digest: [u8; 32] = digest
        .finalize()
        .as_slice()
        .try_into()
        .map_err(|_| invalid("invalid shielded segment digest length"))?;
    if !input.fill_buf()?.is_empty() || body_digest != expected.sha256 || trailer != body_digest {
        return Err(invalid("shielded segment length or SHA-256 mismatch"));
    }
    admitted(
        expected,
        header,
        decode_rows(
            expected,
            header,
            (shielded_table, nullifier_table),
            metadata,
            Some((&shielded, &nullifiers)),
        )?,
    )
}

pub(super) fn admit(
    input: &mut (impl Read + std::io::Seek),
    expected: SegmentIdentity,
    file_len: u64,
    protected: bool,
) -> io::Result<AdmittedSegment> {
    let mut header_bytes = [0; HEADER_LEN];
    input.read_exact(&mut header_bytes)?;
    let header = admit_header(expected, file_len, &header_bytes)?;
    if protected {
        admit_protected(&mut *input, expected, &header)
    } else {
        admit_portable(&mut *input, expected, &header, &header_bytes)
    }
}

/// Wraps decoded rows with their slab and metadata bases from the header.
fn admitted(
    expected: SegmentIdentity,
    header: &SegmentHeader,
    decoded: DecodedRows,
) -> io::Result<AdmittedSegment> {
    let row_meta_base = header
        .index_offset
        .checked_add(offset_tables_len(expected.block_count()?))
        .ok_or_else(|| invalid("shielded segment index exceeds address space"))?;
    Ok(AdmittedSegment {
        first_parent: decoded.first_parent,
        nullifier_base: header.nullifier_slab_offset,
        row_meta_base,
        shielded_slab_len: decoded.shielded_slab_len,
        nullifier_slab_len: decoded.nullifier_slab_len,
        block_hashes: decoded.block_hashes,
        native_bytes: decoded.native_bytes,
        shielded_rel: decoded.shielded_rel,
        nullifier_rel: decoded.nullifier_rel,
        validation: decoded.validation,
        portable_digests: decoded.portable_digests,
    })
}

/// Streams `count` frames of one slab, hashing each into `digest` and
/// recording its framing for comparison against the offset table.
fn hash_frames(
    input: &mut impl BufRead,
    count: usize,
    region: u64,
    digest: &mut Sha256,
) -> io::Result<StreamedSlab> {
    let mut slab = StreamedSlab::new();
    let mut position = 0_u64;
    for _ in 0..count {
        let mut lengths = [0; LENGTHS_LEN];
        input.read_exact(&mut lengths)?;
        let (native, wire) = decode_lengths(lengths)?;
        position += LENGTHS_LEN as u64 + wire as u64;
        if position > region {
            return Err(invalid("shielded segment row exceeds file size"));
        }
        digest.update(lengths);
        let mut payload = Sha256::new();
        let mut remaining = wire;
        while remaining > 0 {
            let available = input.fill_buf()?;
            if available.is_empty() {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let take = remaining.min(available.len());
            payload.update(&available[..take]);
            digest.update(&available[..take]);
            input.consume(take);
            remaining -= take;
        }
        slab.rel.push(
            u32::try_from(position)
                .map_err(|_| invalid("shielded segment slab exceeds its offset range"))?,
        );
        slab.wire.push(wire);
        slab.native.push(native);
        slab.digests.push(payload.finalize().into());
    }
    Ok(slab)
}

/// Reads one `[u32; count + 1]` relative offset table, hashing it on the way.
fn hash_table(
    input: &mut impl BufRead,
    count: usize,
    digest: &mut Sha256,
) -> io::Result<OffsetTable> {
    let mut starts = Vec::with_capacity(count);
    for _ in 0..count {
        let mut raw = [0; 4];
        input.read_exact(&mut raw)?;
        digest.update(raw);
        starts.push(u32::from_be_bytes(raw));
    }
    let mut end_raw = [0; 4];
    input.read_exact(&mut end_raw)?;
    digest.update(end_raw);
    Ok(OffsetTable {
        starts,
        slab_len: u32::from_be_bytes(end_raw),
    })
}

/// Reads the per-block metadata rows, hashing them on the way.
fn hash_metadata(
    input: &mut impl BufRead,
    count: usize,
    digest: &mut Sha256,
) -> io::Result<Vec<[u8; ROW_META_LEN]>> {
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let mut row = [0; ROW_META_LEN];
        input.read_exact(&mut row)?;
        digest.update(row);
        rows.push(row);
    }
    Ok(rows)
}

/// Reads one `[u32; count + 1]` relative offset table from a protected file.
fn read_offset_table(input: &mut impl Read, count: usize) -> io::Result<OffsetTable> {
    let mut starts = Vec::with_capacity(count);
    for _ in 0..count {
        let mut raw = [0; 4];
        input.read_exact(&mut raw)?;
        starts.push(u32::from_be_bytes(raw));
    }
    let mut end_raw = [0; 4];
    input.read_exact(&mut end_raw)?;
    Ok(OffsetTable {
        starts,
        slab_len: u32::from_be_bytes(end_raw),
    })
}

/// Reads the per-block metadata rows from a protected file.
fn read_metadata(input: &mut impl Read, count: usize) -> io::Result<Vec<[u8; ROW_META_LEN]>> {
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let mut row = [0; ROW_META_LEN];
        input.read_exact(&mut row)?;
        rows.push(row);
    }
    Ok(rows)
}

/// Validates the offset tables and metadata against the segment identity, and
/// against the streamed frames of a portable admission, and decodes the
/// compact serving columns.
fn decode_rows(
    expected: SegmentIdentity,
    header: &SegmentHeader,
    (shielded_table, nullifier_table): (OffsetTable, OffsetTable),
    metadata: Vec<[u8; ROW_META_LEN]>,
    streamed: Option<(&StreamedSlab, &StreamedSlab)>,
) -> io::Result<DecodedRows> {
    let count = metadata.len();
    let check = RowCheck {
        shielded_table: &shielded_table,
        nullifier_table: &nullifier_table,
        streamed,
    };
    let mut prior = None;
    let mut first_parent = None;
    let mut block_hashes = Vec::with_capacity(count);
    let mut native_bytes = Vec::with_capacity(count);
    let mut validation = Vec::with_capacity(count);
    let mut portable_digests = streamed.map(|_| Vec::with_capacity(count));
    for (i, entry) in metadata.into_iter().enumerate() {
        let native = check.lengths(i, &entry)?;
        let native_u32 =
            u32::try_from(native).map_err(|_| invalid("shielded segment row length overflow"))?;
        let height_offset = u32::try_from(i)
            .map_err(|_| invalid("shielded segment height exceeds address space"))?;
        let height = expected
            .start
            .value()
            .checked_add(height_offset)
            .map(BlockHeight::new)
            .ok_or_else(|| invalid("shielded segment interval exceeds its height type"))?;
        let id = BlockId::new(height, decode_hash(&entry[..32])?);
        let parent = decode_hash(&entry[32..64])?;
        if i == 0 {
            if parent.as_bytes() != header.first_prev_hash {
                return Err(invalid(
                    "shielded segment does not start at its claimed parent",
                ));
            }
            first_parent = Some(parent);
        }
        expected.check_block(id, parent, prior)?;
        prior = Some(id);
        block_hashes.push(id.hash);
        native_bytes.push(native_u32);
        validation.push(RowValidation::new());
        if let Some(digests) = portable_digests.as_mut() {
            digests.push(SlabDigests {
                shielded: digest_field(&entry, 72)?,
                nullifier: digest_field(&entry, 104)?,
            });
        }
    }
    let first_parent = first_parent.ok_or_else(|| invalid("empty shielded segment interval"))?;
    let last_id = prior.ok_or_else(|| invalid("empty shielded segment interval"))?;
    if last_id != expected.end {
        return Err(invalid(
            "shielded segment ends away from its terminal block",
        ));
    }
    Ok(DecodedRows {
        first_parent,
        shielded_slab_len: shielded_table.slab_len,
        nullifier_slab_len: nullifier_table.slab_len,
        block_hashes,
        native_bytes,
        shielded_rel: shielded_table.starts,
        nullifier_rel: nullifier_table.starts,
        validation,
        portable_digests,
    })
}

/// One metadata row's framing check against its offset tables and, for a
/// portable admission, the frames that were streamed.
struct RowCheck<'a> {
    shielded_table: &'a OffsetTable,
    nullifier_table: &'a OffsetTable,
    streamed: Option<(&'a StreamedSlab, &'a StreamedSlab)>,
}

impl RowCheck<'_> {
    /// Validates row `i`'s lengths and streamed framing, and returns its
    /// native byte count, which budgets both slabs equally.
    fn lengths(&self, i: usize, entry: &[u8; ROW_META_LEN]) -> io::Result<usize> {
        if entry[68..72] != [0; 4] {
            return Err(invalid("shielded segment metadata reserved bytes are set"));
        }
        let native = super::decode_len(&entry[64..68])?;
        let shielded_wire = wire_for(self.shielded_table, i)?;
        let nullifier_wire = wire_for(self.nullifier_table, i)?;
        if super::validate_lengths(native, shielded_wire).is_err()
            || super::validate_lengths(native, nullifier_wire).is_err()
        {
            return Err(invalid("invalid shielded segment row lengths"));
        }
        if let Some((shielded, nullifiers)) = self.streamed {
            let shielded_digest = digest_field(entry, 72)?;
            let nullifier_digest = digest_field(entry, 104)?;
            if shielded.rel[i] != self.shielded_table.starts[i]
                || shielded.wire[i] != shielded_wire
                || shielded.native[i] != native
                || shielded.digests[i] != shielded_digest
                || nullifiers.rel[i] != self.nullifier_table.starts[i]
                || nullifiers.wire[i] != nullifier_wire
                || nullifiers.native[i] != native
                || nullifiers.digests[i] != nullifier_digest
            {
                return Err(invalid(
                    "shielded segment payload or framing differs from its index",
                ));
            }
        }
        Ok(native)
    }
}

/// Reads one 32-byte payload digest field of a metadata row.
fn digest_field(entry: &[u8; ROW_META_LEN], offset: usize) -> io::Result<[u8; 32]> {
    entry
        .get(offset..offset + 32)
        .and_then(|field| field.try_into().ok())
        .ok_or_else(|| invalid("invalid shielded segment metadata row"))
}

/// A row's payload length in one slab, from its offset table span.
fn wire_for(table: &OffsetTable, i: usize) -> io::Result<usize> {
    let offset = table
        .starts
        .get(i)
        .copied()
        .ok_or_else(|| invalid("shielded segment offset table is short"))?;
    let next = if i + 1 == table.starts.len() {
        table.slab_len
    } else {
        table
            .starts
            .get(i + 1)
            .copied()
            .ok_or_else(|| invalid("shielded segment offset table is short"))?
    };
    let span = u64::from(next)
        .checked_sub(u64::from(offset))
        .ok_or_else(|| invalid("shielded segment frame offsets are not ascending"))?;
    let frame = usize::try_from(span)
        .map_err(|_| invalid("shielded segment frame offsets are not ascending"))?;
    if frame <= LENGTHS_LEN || frame > MAX_PAYLOAD_BYTES + LENGTHS_LEN {
        return Err(invalid("invalid shielded segment row lengths"));
    }
    Ok(frame - LENGTHS_LEN)
}
