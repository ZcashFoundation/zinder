use super::{
    BlockHash, BlockHeightRange, BlockId, BufRead, BufReader, Digest as _, HEADER_LEN, INDEX_LEN,
    IndexedBlock, LENGTHS_LEN, MAX_PAYLOAD_BYTES, Read, Seek, SegmentIdentity, Sha256, decode_hash,
    decode_lengths, encode_internal_block_hash, invalid, io,
};

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

pub(super) fn encode(
    offset: u64,
    lengths: [u8; 8],
    hash: BlockHash,
    parent: BlockHash,
    sha256: [u8; 32],
) -> [u8; INDEX_LEN] {
    let mut row = [0; INDEX_LEN];
    row[..8].copy_from_slice(&offset.to_be_bytes());
    row[8..16].copy_from_slice(&lengths);
    row[16..48].copy_from_slice(&encode_internal_block_hash(hash));
    row[48..80].copy_from_slice(&encode_internal_block_hash(parent));
    row[80..].copy_from_slice(&sha256);
    row
}

type Frame = ([u8; LENGTHS_LEN], [u8; 32]);

fn hash_frames(
    input: &mut impl BufRead,
    count: u64,
    trailer: u64,
    digest: &mut Sha256,
) -> io::Result<Vec<Frame>> {
    let mut frames = Vec::new();
    let mut end = HEADER_LEN as u64;
    for _ in 0..count {
        let mut lengths = [0; LENGTHS_LEN];
        input.read_exact(&mut lengths)?;
        let (_, mut len) = decode_lengths(lengths)?;
        end += LENGTHS_LEN as u64 + len as u64;
        if end > trailer {
            return Err(invalid("shielded segment row exceeds file size"));
        }
        digest.update(lengths);
        let mut payload = Sha256::new();
        while len > 0 {
            let available = input.fill_buf()?;
            if available.is_empty() {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let take = len.min(available.len());
            payload.update(&available[..take]);
            digest.update(&available[..take]);
            input.consume(take);
            len -= take;
        }
        frames.push((lengths, payload.finalize().into()));
    }
    if end != trailer {
        return Err(invalid("shielded segment index offset mismatch"));
    }
    Ok(frames)
}

pub(super) fn admit(
    input: &mut (impl Read + Seek),
    expected: SegmentIdentity,
    file_len: u64,
    protected: bool,
) -> io::Result<Vec<IndexedBlock>> {
    let mut header = [0; HEADER_LEN];
    input.read_exact(&mut header)?;
    if header != expected.header()? {
        return Err(invalid(
            "shielded segment format, network, or interval mismatch",
        ));
    }
    let count = u64::from(expected.end.height.value() - expected.start.value()) + 1;
    let minimum = HEADER_LEN as u64 + count * (INDEX_LEN + LENGTHS_LEN + 68) as u64;
    let maximum = HEADER_LEN as u64 + count * (INDEX_LEN + LENGTHS_LEN + MAX_PAYLOAD_BYTES) as u64;
    if !(minimum..=maximum).contains(&file_len) {
        return Err(invalid(
            "shielded segment file size does not fit its interval",
        ));
    }
    let trailer = file_len - count * INDEX_LEN as u64;
    if protected {
        input.seek(io::SeekFrom::Start(trailer))?;
        let count =
            usize::try_from(count).map_err(|_| invalid("shielded index exceeds address space"))?;
        let mut entries = Vec::new();
        entries.try_reserve_exact(count).map_err(io::Error::other)?;
        entries.resize(count, [0; INDEX_LEN]);
        input.read_exact(entries.as_flattened_mut())?;
        return decode_index(entries, expected, None, trailer);
    }
    let mut digest = Sha256::new_with_prefix(header);
    let mut input = BufReader::new(input);
    let frames = hash_frames(&mut input, count, trailer, &mut digest)?;
    let mut entries = Vec::new();
    for _ in 0..count {
        let mut entry = [0; INDEX_LEN];
        input.read_exact(&mut entry)?;
        digest.update(entry);
        entries.push(entry);
    }
    if !input.fill_buf()?.is_empty() || <[u8; 32]>::from(digest.finalize()) != expected.sha256 {
        return Err(invalid("shielded segment length or SHA-256 mismatch"));
    }
    decode_index(entries, expected, Some(&frames), trailer)
}

fn decode_index(
    entries: Vec<[u8; INDEX_LEN]>,
    expected: SegmentIdentity,
    frames: Option<&[Frame]>,
    trailer: u64,
) -> io::Result<Vec<IndexedBlock>> {
    let mut next = HEADER_LEN as u64;
    let mut prior = None;
    let mut rows = Vec::with_capacity(entries.len());
    for ((i, entry), height) in entries
        .into_iter()
        .enumerate()
        .zip(BlockHeightRange::inclusive(
            expected.start,
            expected.end.height,
        ))
    {
        let offset = u64::from_be_bytes(field(&entry[..8])?);
        let lengths = field(&entry[8..16])?;
        let (native_bytes, len) = decode_lengths(lengths)?;
        let sha256 = field(&entry[80..])?;
        if offset != next + LENGTHS_LEN as u64
            || frames.is_some_and(|rows| rows[i] != (lengths, sha256))
        {
            return Err(invalid(
                "shielded segment payload or framing differs from its index",
            ));
        }
        next = offset + len as u64;
        let id = BlockId::new(height, decode_hash(&entry[16..48])?);
        let parent = decode_hash(&entry[48..80])?;
        expected.check_block(id, parent, prior)?;
        prior = Some(id);
        rows.push(IndexedBlock {
            id,
            parent,
            native_bytes,
            offset,
            len,
            sha256: frames.map(|_| sha256),
        });
    }
    if next != trailer {
        return Err(invalid("shielded segment index offset mismatch"));
    }
    Ok(rows)
}

fn field<const N: usize>(bytes: &[u8]) -> io::Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| invalid("invalid shielded segment index field"))
}
