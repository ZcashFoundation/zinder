use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempDir};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, Network, SerializedBytesDigest,
    wire::encode_internal_block_hash,
};

use crate::positional_source::PositionalByteSource;

use super::{
    DEFAULT_SETTLED_RAW_BLOCK_SEGMENT_MAX_SLAB_BYTES, RawBlockCodec, RawBlockRowIndexSlice,
    RawBlockSegmentHeader, RawBlockSegmentIdentity, RawBlockTransactionSlice, RawBlockTxTableSlice,
    SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES, SettledRawBlockSegmentError,
    SettledRawBlockSegmentIndex, SettledRawBlockSegmentOpenOptions, SettledRawBlockSegmentReader,
    SettledRawBlockSegmentWriter,
    format::{
        CompactRawBlockRow, DecodedRowIndexEntry, FRAME_PREFIX_LEN, FRAME_PREFIX_U32, HEADER_LEN,
        MAX_TX_ENTRIES_PER_BLOCK, ROW_INDEX_ENTRY_LEN, TRAILER_LEN, TX_SLICE_ENTRY_LEN,
        TxSliceEntry, VALIDATION_INVALID, VALIDATION_UNVALIDATED, VALIDATION_VALID,
    },
};

type TestOutcome<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const TEST_NETWORK: Network = Network::ZcashRegtest;

struct FixtureBlock {
    id: BlockId,
    parent_hash: BlockHash,
    raw_bytes: Vec<u8>,
    tx_slices: Vec<RawBlockTransactionSlice>,
}

fn block_hash(seed: u32) -> BlockHash {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&seed.to_le_bytes());
    bytes[31] = 0xbb;
    BlockHash::from_bytes(bytes)
}

fn synthetic_raw_block(
    seed: u8,
    tx_lengths: &[usize],
) -> TestOutcome<(Vec<u8>, Vec<RawBlockTransactionSlice>)> {
    let header_len = 148_usize;
    let mut bytes = vec![seed; header_len];
    let mut slices = Vec::with_capacity(tx_lengths.len());
    for (tx_idx, &tx_len) in tx_lengths.iter().enumerate() {
        let offset = u32::try_from(bytes.len())?;
        let len_u32 = u32::try_from(tx_len)?;
        let tx_marker = u8::try_from((usize::from(seed) + tx_idx) % 251)?;
        for pos in 0..tx_len {
            let byte = tx_marker.wrapping_add(u8::try_from(pos % 199)?);
            bytes.push(byte);
        }
        slices.push(RawBlockTransactionSlice::new(offset, len_u32));
    }
    Ok((bytes, slices))
}

fn fixture_blocks() -> TestOutcome<Vec<FixtureBlock>> {
    let specs: [(u32, &[usize]); 4] = [
        (1, &[64]),
        (2, &[96, 128, 256]),
        (3, &[]),
        (4, &[48, 512, 1024, 77]),
    ];
    let mut blocks = Vec::with_capacity(specs.len());
    for (height_num, tx_lengths) in specs {
        let seed = u8::try_from(height_num)?;
        let (raw_bytes, tx_slices) = synthetic_raw_block(seed, tx_lengths)?;
        blocks.push(FixtureBlock {
            id: BlockId::new(BlockHeight::new(height_num), block_hash(height_num)),
            parent_hash: block_hash(height_num - 1),
            raw_bytes,
            tx_slices,
        });
    }
    Ok(blocks)
}

fn write_segment(
    dir: &Path,
    name: &str,
    codec: RawBlockCodec,
    blocks: &[FixtureBlock],
) -> TestOutcome<(PathBuf, RawBlockSegmentIdentity)> {
    let target = dir.join(name);
    let staged = NamedTempFile::new_in(dir)?;
    let first = blocks
        .first()
        .ok_or("cannot write empty test segment")?
        .id
        .height;
    let end = blocks.last().ok_or("cannot write empty test segment")?.id;
    let mut writer = SettledRawBlockSegmentWriter::create(
        &target,
        staged,
        RawBlockSegmentIdentity::span(TEST_NETWORK, codec, first, end),
    )?;
    for block in blocks {
        writer.push(
            block.id,
            block.parent_hash,
            &block.raw_bytes,
            &block.tx_slices,
        )?;
    }
    let identity = writer.finish()?;
    Ok((target, identity))
}

fn recompute_portable_pins(bytes: &mut [u8]) -> TestOutcome<([u8; 32], [u8; 32])> {
    let body_end = bytes.len() - TRAILER_LEN;
    let trailer: [u8; 32] = Sha256::digest(&bytes[..body_end]).into();
    bytes[body_end..].copy_from_slice(&trailer);
    let (tx_table_offset, _) = header_offsets(bytes)?;
    let mut index_hasher = Sha256::new();
    index_hasher.update(&bytes[..HEADER_LEN]);
    index_hasher.update(&bytes[tx_table_offset..]);
    Ok((trailer, index_hasher.finalize().into()))
}

fn header_offsets(segment: &[u8]) -> TestOutcome<(usize, usize)> {
    let tx_table_offset = usize::try_from(u64::from_be_bytes(segment[96..104].try_into()?))?;
    let row_index_offset = usize::try_from(u64::from_be_bytes(segment[104..112].try_into()?))?;
    Ok((tx_table_offset, row_index_offset))
}

fn assert_exact_block_and_tx_recovery<S: PositionalByteSource>(
    reader: &SettledRawBlockSegmentReader<S>,
    blocks: &[FixtureBlock],
) -> TestOutcome {
    assert_eq!(reader.block_count(), blocks.len());
    for block in blocks {
        assert_eq!(reader.block_hash_at(block.id.height)?, block.id.hash);
        let recovered_block = reader.read_raw_block(block.id.height)?;
        assert_eq!(recovered_block, block.raw_bytes);
        assert_eq!(
            reader.uncompressed_digest_at(block.id.height)?,
            SerializedBytesDigest::from_serialized_bytes(&block.raw_bytes)
        );
        for (tx_idx, slice) in block.tx_slices.iter().enumerate() {
            let tx_idx_u32 = u32::try_from(tx_idx)?;
            let recovered_tx = reader.read_raw_transaction(block.id.height, tx_idx_u32)?;
            let start = usize::try_from(slice.offset)?;
            let end = start + usize::try_from(slice.len)?;
            assert_eq!(recovered_tx.as_slice(), &block.raw_bytes[start..end]);
        }
        let oob_idx = u32::try_from(block.tx_slices.len())?;
        assert!(matches!(
            reader.read_raw_transaction(block.id.height, oob_idx),
            Err(SettledRawBlockSegmentError::TransactionIndexOutOfRange {
                tx_index,
                tx_count,
                ..
            }) if tx_index == oob_idx && tx_count == oob_idx
        ));
    }
    Ok(())
}

#[test]
fn zstd_and_uncompressed_round_trips_recover_exact_blocks_and_txs() -> TestOutcome {
    assert_eq!(
        DEFAULT_SETTLED_RAW_BLOCK_SEGMENT_MAX_SLAB_BYTES,
        256 * 1024 * 1024
    );
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    for (codec, name) in [
        (RawBlockCodec::Zstd, "seg-zstd"),
        (RawBlockCodec::None, "seg-none"),
    ] {
        let (path, identity) = write_segment(dir.path(), name, codec, &blocks)?;
        let reader = SettledRawBlockSegmentReader::open(
            &path,
            &identity,
            SettledRawBlockSegmentOpenOptions {
                require_fs_verity: false,
                verify_full_payload: true,
            },
        )?;
        assert_eq!(reader.identity(), identity);
        assert_exact_block_and_tx_recovery(&reader, &blocks)?;
        assert!(matches!(
            reader.read_raw_block(BlockHeight::new(0)),
            Err(SettledRawBlockSegmentError::HeightOutOfRange { .. })
        ));
        assert!(matches!(
            reader.read_raw_block(BlockHeight::new(5)),
            Err(SettledRawBlockSegmentError::HeightOutOfRange { .. })
        ));
        assert!(matches!(
            reader.read_raw_transaction(BlockHeight::new(5), 0),
            Err(SettledRawBlockSegmentError::HeightOutOfRange { .. })
        ));
    }
    let (re_path, re_id) = write_segment(dir.path(), "seg-zstd", RawBlockCodec::Zstd, &blocks)?;
    let reopened = SettledRawBlockSegmentReader::open(
        &re_path,
        &re_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    assert_eq!(reopened.block_count(), blocks.len());
    Ok(())
}

#[test]
fn zstd_compresses_repetitive_multi_tx_block_and_enforces_16_mib_writer_cap() -> TestOutcome {
    let dir = TempDir::new()?;
    let block_bytes = vec![0x42_u8; 3 * 1024 * 1024];
    let slice = RawBlockTransactionSlice::new(200, 1024 * 1024);
    let block = FixtureBlock {
        id: BlockId::new(BlockHeight::new(10), block_hash(10)),
        parent_hash: block_hash(9),
        raw_bytes: block_bytes.clone(),
        tx_slices: vec![slice],
    };
    let (zstd_path, zstd_id) =
        write_segment(dir.path(), "zstd-big", RawBlockCodec::Zstd, &[block])?;
    assert!(fs::metadata(&zstd_path)?.len() < 64 * 1024);

    let reader = SettledRawBlockSegmentReader::open(
        &zstd_path,
        &zstd_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    assert_eq!(reader.read_raw_block(BlockHeight::new(10))?, block_bytes);
    assert_eq!(
        reader.read_raw_transaction(BlockHeight::new(10), 0)?,
        vec![0x42_u8; 1024 * 1024]
    );

    let oversized = vec![0x11_u8; SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES + 1];
    let target = dir.path().join("oversized-write");
    let staged = NamedTempFile::new_in(dir.path())?;
    let mut writer = SettledRawBlockSegmentWriter::create(
        &target,
        staged,
        RawBlockSegmentIdentity::span(
            TEST_NETWORK,
            RawBlockCodec::Zstd,
            BlockHeight::new(1),
            BlockId::new(BlockHeight::new(1), block_hash(1)),
        ),
    )?;
    let outcome = writer.push(
        BlockId::new(BlockHeight::new(1), block_hash(1)),
        block_hash(0),
        &oversized,
        &[],
    );
    assert!(matches!(
        outcome,
        Err(SettledRawBlockSegmentError::PayloadLimitExceeded {
            max_bytes: SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES,
            ..
        })
    ));
    Ok(())
}

#[test]
fn exact_16_mib_block_succeeds_in_zstd_mode() -> TestOutcome {
    let dir = TempDir::new()?;
    let exact_bytes = vec![0x7a_u8; SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES];
    let exact_block = FixtureBlock {
        id: BlockId::new(BlockHeight::new(1), block_hash(1)),
        parent_hash: block_hash(0),
        raw_bytes: exact_bytes.clone(),
        tx_slices: vec![RawBlockTransactionSlice::new(0, 512)],
    };
    let (path, identity) =
        write_segment(dir.path(), "exact-16m", RawBlockCodec::Zstd, &[exact_block])?;
    let reader = SettledRawBlockSegmentReader::open(
        &path,
        &identity,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    assert_eq!(
        reader.read_raw_block(BlockHeight::new(1))?.len(),
        exact_bytes.len()
    );
    assert_eq!(
        reader.read_raw_transaction(BlockHeight::new(1), 0)?,
        vec![0x7a_u8; 512]
    );
    Ok(())
}

#[test]
fn writer_rejects_broken_linkage_incomplete_finish_and_bad_tx_slices() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let target = dir.path().join("broken-writer");
    let staged = NamedTempFile::new_in(dir.path())?;
    let mut writer = SettledRawBlockSegmentWriter::create(
        &target,
        staged,
        RawBlockSegmentIdentity::span(
            TEST_NETWORK,
            RawBlockCodec::Zstd,
            BlockHeight::new(1),
            BlockId::new(BlockHeight::new(3), block_hash(3)),
        ),
    )?;
    assert!(matches!(
        writer.push(blocks[0].id, blocks[0].parent_hash, &[], &[]),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    let bad_slice = RawBlockTransactionSlice::new(u32::try_from(blocks[0].raw_bytes.len())?, 1);
    assert!(matches!(
        writer.push(
            blocks[0].id,
            blocks[0].parent_hash,
            &blocks[0].raw_bytes,
            &[bad_slice]
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    writer.push(
        blocks[0].id,
        blocks[0].parent_hash,
        &blocks[0].raw_bytes,
        &blocks[0].tx_slices,
    )?;
    assert!(matches!(
        writer.push(
            blocks[2].id,
            blocks[0].id.hash,
            &blocks[2].raw_bytes,
            &blocks[2].tx_slices
        ),
        Err(SettledRawBlockSegmentError::ChainLinkage(_))
    ));
    assert!(matches!(
        writer.push(
            blocks[1].id,
            block_hash(99),
            &blocks[1].raw_bytes,
            &blocks[1].tx_slices
        ),
        Err(SettledRawBlockSegmentError::ChainLinkage(_))
    ));
    assert!(matches!(
        writer.finish(),
        Err(SettledRawBlockSegmentError::ChainLinkage(_))
    ));
    Ok(())
}

fn synthetic_index_identity(
    header: &RawBlockSegmentHeader,
    raw_tx_table: &[u8],
    raw_row_index: &[u8],
) -> (Vec<u8>, RawBlockSegmentIdentity, u64) {
    let header_bytes = header.encode();
    let trailer = [0x44_u8; TRAILER_LEN];
    let mut index_hasher = Sha256::new();
    index_hasher.update(header_bytes);
    index_hasher.update(raw_tx_table);
    index_hasher.update(raw_row_index);
    index_hasher.update(trailer);
    let mut index_and_trailer =
        Vec::with_capacity(raw_tx_table.len() + raw_row_index.len() + TRAILER_LEN);
    index_and_trailer.extend_from_slice(raw_tx_table);
    index_and_trailer.extend_from_slice(raw_row_index);
    index_and_trailer.extend_from_slice(&trailer);
    let total_len = header.tx_table_offset + u64::try_from(index_and_trailer.len()).unwrap_or(0);
    let identity = RawBlockSegmentIdentity {
        network: TEST_NETWORK,
        codec: RawBlockCodec::from_id(header.codec_id).unwrap_or(RawBlockCodec::None),
        start_height: BlockHeight::new(header.start_height),
        end_block: BlockId::new(
            BlockHeight::new(header.start_height + header.block_count - 1),
            block_hash(header.start_height + header.block_count - 1),
        ),
        sha256: trailer,
        index_sha256: index_hasher.finalize().into(),
        fs_verity_sha256: None,
    };
    (index_and_trailer, identity, total_len)
}

#[test]
fn protected_index_admission_refuses_oversized_block_and_frame_without_allocating() -> TestOutcome {
    let max_u32 = u32::try_from(SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES)?;
    let slab_len = 100_u64;
    let header = RawBlockSegmentHeader {
        network_tag: TEST_NETWORK.id(),
        segment_flags: 0,
        codec_id: RawBlockCodec::Zstd.id(),
        start_height: 1,
        block_count: 1,
        first_prev_hash_wire: encode_internal_block_hash(block_hash(0)),
        last_block_hash_wire: encode_internal_block_hash(block_hash(1)),
        slab_offset: HEADER_LEN as u64,
        tx_table_offset: HEADER_LEN as u64 + slab_len,
        row_index_offset: HEADER_LEN as u64 + slab_len,
    };
    let oversized_uncompressed = DecodedRowIndexEntry {
        block_hash_wire: encode_internal_block_hash(block_hash(1)),
        payload_sha256: [0xaa; 32],
        uncompressed_sha256: [0xbb; 32],
        slab_rel_offset: 0,
        uncompressed_len: max_u32 + 1,
        tx_start_index: 0,
        tx_count: 0,
    };
    let (raw_idx, expected, total_len) =
        synthetic_index_identity(&header, &[], &oversized_uncompressed.encode());
    assert!(matches!(
        SettledRawBlockSegmentIndex::decode_index(
            &header.encode(),
            &raw_idx,
            &expected,
            total_len,
            true,
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));

    let valid_uncompressed = DecodedRowIndexEntry {
        uncompressed_len: 1024,
        ..oversized_uncompressed
    };
    let near_4gib_slab_len = u64::from(u32::MAX - 1024);
    let oversized_header = RawBlockSegmentHeader {
        tx_table_offset: HEADER_LEN as u64 + near_4gib_slab_len,
        row_index_offset: HEADER_LEN as u64 + near_4gib_slab_len,
        ..header
    };
    let (raw_idx_big, expected_big, total_big) =
        synthetic_index_identity(&oversized_header, &[], &valid_uncompressed.encode());
    assert!(matches!(
        SettledRawBlockSegmentIndex::decode_index(
            &oversized_header.encode(),
            &raw_idx_big,
            &expected_big,
            total_big,
            true,
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[test]
fn open_rejects_oversized_uncompressed_len_and_oob_tx_slice_in_forged_segment() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "bounds", RawBlockCodec::Zstd, &blocks)?;
    let original = fs::read(&path)?;
    let (tx_table_offset, row_index_offset) = header_offsets(&original)?;

    let mut oversized_buf = original.clone();
    let max_plus_one = u32::try_from(SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES + 1)?;
    oversized_buf[row_index_offset + 100..row_index_offset + 104]
        .copy_from_slice(&max_plus_one.to_be_bytes());
    let (oversized_sha, oversized_idx_sha) = recompute_portable_pins(&mut oversized_buf)?;
    let oversized_path = dir.path().join("forged-oversized");
    fs::write(&oversized_path, &oversized_buf)?;
    let oversized_id = RawBlockSegmentIdentity {
        sha256: oversized_sha,
        index_sha256: oversized_idx_sha,
        fs_verity_sha256: None,
        ..identity
    };
    assert!(matches!(
        SettledRawBlockSegmentReader::open(
            &oversized_path,
            &oversized_id,
            SettledRawBlockSegmentOpenOptions::default(),
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));

    let mut oob_tx_buf = original;
    let raw_block_len = u32::try_from(blocks[0].raw_bytes.len())?.to_be_bytes();
    oob_tx_buf[tx_table_offset..tx_table_offset + 4].copy_from_slice(&raw_block_len);
    let (oob_sha, oob_idx_sha) = recompute_portable_pins(&mut oob_tx_buf)?;
    let oob_path = dir.path().join("forged-oob-tx");
    fs::write(&oob_path, &oob_tx_buf)?;
    let oob_id = RawBlockSegmentIdentity {
        sha256: oob_sha,
        index_sha256: oob_idx_sha,
        fs_verity_sha256: None,
        ..identity
    };
    assert!(matches!(
        SettledRawBlockSegmentReader::open(
            &oob_path,
            &oob_id,
            SettledRawBlockSegmentOpenOptions::default(),
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[derive(Clone, Copy)]
struct TamperBase<'a> {
    out: &'a Path,
    original: &'a [u8],
    identity: RawBlockSegmentIdentity,
}

fn corrupt_and_open(
    base: TamperBase<'_>,
    mutator: &dyn Fn(&mut Vec<u8>),
    recompute_pins: bool,
    verify_full_payload: bool,
) -> TestOutcome<SettledRawBlockSegmentError> {
    let mut mutated = base.original.to_vec();
    mutator(&mut mutated);
    let mut expected = base.identity;
    expected.fs_verity_sha256 = None;
    if recompute_pins {
        let (forged_sha, forged_idx_sha) = recompute_portable_pins(&mut mutated)?;
        expected.sha256 = forged_sha;
        expected.index_sha256 = forged_idx_sha;
    }
    fs::write(base.out, &mutated)?;
    match SettledRawBlockSegmentReader::open(
        base.out,
        &expected,
        SettledRawBlockSegmentOpenOptions {
            require_fs_verity: false,
            verify_full_payload,
        },
    ) {
        Ok(_) => Err("expected tamper refusal on open".into()),
        Err(err) => Ok(err),
    }
}

fn assert_slab_prefix_tamper_rejected(base: TamperBase<'_>) -> TestOutcome {
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[HEADER_LEN] = 0xff, false, true)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[HEADER_LEN] = 0xff, true, true)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[HEADER_LEN + 3] ^= 0x01, false, true)?,
        SettledRawBlockSegmentError::PayloadIntegrity(_)
    ));
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[HEADER_LEN + 3] ^= 0x01, true, true)?,
        SettledRawBlockSegmentError::PayloadIntegrity(_)
    ));
    Ok(())
}

#[test]
fn reader_fails_closed_on_tampered_header_index_slab_and_trailer() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "tamper-base", RawBlockCodec::Zstd, &blocks)?;
    let original = fs::read(&path)?;
    let (_tx_off, row_off) = header_offsets(&original)?;
    let out = dir.path().join("tampered");
    let base = TamperBase {
        out: &out,
        original: &original,
        identity,
    };

    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[0] ^= 0xff, false, true)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[13] = 1, false, true)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[15] = 9, false, true)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[120] = 1, false, true)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[row_off + 115] = 1, true, true)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert_slab_prefix_tamper_rejected(base)?;
    let slab_payload_byte = HEADER_LEN + usize::try_from(FRAME_PREFIX_U32)? + 2;
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[slab_payload_byte] ^= 0x01, false, true)?,
        SettledRawBlockSegmentError::PayloadIntegrity(_)
    ));
    assert!(matches!(
        corrupt_and_open(
            base,
            &|buf| {
                let last = buf.len() - 1;
                buf[last] ^= 0x01;
            },
            false,
            false,
        )?,
        SettledRawBlockSegmentError::PayloadIntegrity(_)
    ));
    let last_row = row_off + (blocks.len() - 1) * ROW_INDEX_ENTRY_LEN;
    assert!(matches!(
        corrupt_and_open(base, &|buf| buf[last_row] ^= 0x01, true, false)?,
        SettledRawBlockSegmentError::ChainLinkage(_)
    ));
    Ok(())
}

#[test]
fn reader_rejects_block_and_tx_hash_tamper_with_sticky_invalid_state() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "sticky-base", RawBlockCodec::Zstd, &blocks)?;
    let original = fs::read(&path)?;
    let (tx_off, row_off) = header_offsets(&original)?;

    let mut bad_block_buf = original.clone();
    bad_block_buf[row_off + ROW_INDEX_ENTRY_LEN + 64] ^= 0x01;
    let (bad_block_sha, bad_block_idx_sha) = recompute_portable_pins(&mut bad_block_buf)?;
    let bad_block_path = dir.path().join("bad-block-sha");
    fs::write(&bad_block_path, &bad_block_buf)?;
    let reader_block = SettledRawBlockSegmentReader::open(
        &bad_block_path,
        &RawBlockSegmentIdentity {
            sha256: bad_block_sha,
            index_sha256: bad_block_idx_sha,
            fs_verity_sha256: None,
            ..identity
        },
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    assert!(matches!(
        reader_block.read_raw_block(BlockHeight::new(2)),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_block.read_raw_block(BlockHeight::new(2)),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_block.read_raw_transaction(BlockHeight::new(2), 0),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));

    let mut bad_tx_buf = original;
    bad_tx_buf[tx_off + 2 * TX_SLICE_ENTRY_LEN + 8] ^= 0x01;
    let (bad_tx_sha, bad_tx_idx_sha) = recompute_portable_pins(&mut bad_tx_buf)?;
    let bad_tx_path = dir.path().join("bad-tx-sha");
    fs::write(&bad_tx_path, &bad_tx_buf)?;
    let reader_tx = SettledRawBlockSegmentReader::open(
        &bad_tx_path,
        &RawBlockSegmentIdentity {
            sha256: bad_tx_sha,
            index_sha256: bad_tx_idx_sha,
            fs_verity_sha256: None,
            ..identity
        },
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    assert_eq!(
        reader_tx.read_raw_block(BlockHeight::new(2))?,
        blocks[1].raw_bytes
    );
    assert!(matches!(
        reader_tx.read_raw_transaction(BlockHeight::new(2), 1),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_tx.read_raw_transaction(BlockHeight::new(2), 1),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_tx.read_raw_block(BlockHeight::new(2)),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn reader_detects_post_admission_tamper_and_enforces_require_fs_verity() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "portable-src", RawBlockCodec::None, &blocks)?;
    let original = fs::read(&path)?;
    let portable_path = dir.path().join("portable-copy");
    fs::write(&portable_path, &original)?;
    let portable_id = RawBlockSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledRawBlockSegmentReader::open(
        &portable_path,
        &portable_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    assert!(!reader.kernel_verified());

    let mut writer_file = OpenOptions::new().write(true).open(&portable_path)?;
    let tamper_pos = HEADER_LEN + usize::try_from(FRAME_PREFIX_U32)? + 8;
    writer_file.seek(SeekFrom::Start(u64::try_from(tamper_pos)?))?;
    writer_file.write_all(&[original[tamper_pos] ^ 0x55])?;
    writer_file.sync_all()?;
    drop(writer_file);

    assert!(matches!(
        reader.read_raw_block(BlockHeight::new(1)),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_raw_transaction(BlockHeight::new(1), 0),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        SettledRawBlockSegmentReader::open(
            &portable_path,
            &portable_id,
            SettledRawBlockSegmentOpenOptions {
                require_fs_verity: true,
                verify_full_payload: false,
            },
        ),
        Err(SettledRawBlockSegmentError::FsVerityRequired)
    ));
    Ok(())
}

#[test]
fn tx_slice_entry_round_trips_and_overflow_slice_fails_admission() -> TestOutcome {
    let entry = TxSliceEntry {
        offset: 148,
        len: 512,
        sha256: [0x3c; 32],
    };
    assert_eq!(TxSliceEntry::decode(&entry.encode())?, entry);

    let header = RawBlockSegmentHeader {
        network_tag: TEST_NETWORK.id(),
        segment_flags: 0,
        codec_id: RawBlockCodec::Zstd.id(),
        start_height: 1,
        block_count: 1,
        first_prev_hash_wire: encode_internal_block_hash(block_hash(0)),
        last_block_hash_wire: encode_internal_block_hash(block_hash(1)),
        slab_offset: HEADER_LEN as u64,
        tx_table_offset: HEADER_LEN as u64 + 64,
        row_index_offset: HEADER_LEN as u64 + 64 + TX_SLICE_ENTRY_LEN as u64,
    };
    let row = DecodedRowIndexEntry {
        block_hash_wire: encode_internal_block_hash(block_hash(1)),
        payload_sha256: [0x11; 32],
        uncompressed_sha256: [0x22; 32],
        slab_rel_offset: 0,
        uncompressed_len: 1024,
        tx_start_index: 0,
        tx_count: 1,
    };
    let overflow_tx = TxSliceEntry {
        offset: u32::MAX - 4,
        len: 16,
        sha256: [0x33; 32],
    };
    let (raw_idx, expected, total_len) =
        synthetic_index_identity(&header, &overflow_tx.encode(), &row.encode());
    assert!(matches!(
        SettledRawBlockSegmentIndex::decode_index(
            &header.encode(),
            &raw_idx,
            &expected,
            total_len,
            true,
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[test]
fn compact_raw_block_row_mark_valid_preserves_invalid_state() {
    let row = CompactRawBlockRow::from_entry(&DecodedRowIndexEntry {
        block_hash_wire: [0x11; 32],
        payload_sha256: [0x22; 32],
        uncompressed_sha256: [0x33; 32],
        slab_rel_offset: 0,
        uncompressed_len: 128,
        tx_start_index: 0,
        tx_count: 0,
    });
    assert_eq!(
        row.validation_state.load(Ordering::Acquire),
        VALIDATION_UNVALIDATED
    );
    row.mark_invalid();
    assert_eq!(
        row.validation_state.load(Ordering::Acquire),
        VALIDATION_INVALID
    );
    row.mark_valid();
    assert_eq!(
        row.validation_state.load(Ordering::Acquire),
        VALIDATION_INVALID
    );

    let valid_row = CompactRawBlockRow::from_entry(&DecodedRowIndexEntry {
        block_hash_wire: [0x44; 32],
        payload_sha256: [0x55; 32],
        uncompressed_sha256: [0x66; 32],
        slab_rel_offset: 0,
        uncompressed_len: 128,
        tx_start_index: 0,
        tx_count: 0,
    });
    valid_row.mark_valid();
    assert_eq!(
        valid_row.validation_state.load(Ordering::Acquire),
        VALIDATION_VALID
    );
}

#[test]
fn reader_latches_invalid_on_truncated_slab_io_error() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "truncate-src", RawBlockCodec::None, &blocks)?;
    let original = fs::read(&path)?;
    let portable_path = dir.path().join("truncate-portable");
    fs::write(&portable_path, &original)?;
    let portable_id = RawBlockSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledRawBlockSegmentReader::open(
        &portable_path,
        &portable_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    assert_eq!(
        reader.read_raw_block(BlockHeight::new(4))?,
        blocks[3].raw_bytes
    );

    let truncated_file = OpenOptions::new().write(true).open(&portable_path)?;
    truncated_file.set_len(u64::try_from(HEADER_LEN + 4)?)?;
    truncated_file.sync_all()?;
    drop(truncated_file);

    for height in [BlockHeight::new(1), BlockHeight::new(4)] {
        assert!(matches!(
            reader.read_raw_block(height),
            Err(SettledRawBlockSegmentError::Io(_))
        ));
        assert!(matches!(
            reader.read_raw_block(height),
            Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
        ));
        assert!(matches!(
            reader.read_raw_transaction(height, 0),
            Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
        ));
    }

    assert!(matches!(
        reader.read_raw_transaction(BlockHeight::new(2), 0),
        Err(SettledRawBlockSegmentError::Io(_))
    ));
    assert!(matches!(
        reader.read_raw_block(BlockHeight::new(2)),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_raw_transaction(BlockHeight::new(2), 0),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[derive(Clone, Copy)]
struct ForgedSparseSpec {
    block_count: u32,
    slab_len: u64,
    tx_count: u64,
    write_frame_prefix: bool,
}

fn write_forged_sparse_segment(
    path: &Path,
    spec: ForgedSparseSpec,
) -> TestOutcome<RawBlockSegmentIdentity> {
    let slab_offset = HEADER_LEN as u64;
    let tx_table_offset = slab_offset + spec.slab_len;
    let tx_table_bytes = spec.tx_count * TX_SLICE_ENTRY_LEN as u64;
    let row_index_offset = tx_table_offset + tx_table_bytes;
    let index_bytes = u64::from(spec.block_count) * ROW_INDEX_ENTRY_LEN as u64;
    let total_len = row_index_offset + index_bytes + TRAILER_LEN as u64;

    let end_block = BlockId::new(
        BlockHeight::new(spec.block_count),
        block_hash(spec.block_count),
    );
    let header = RawBlockSegmentHeader {
        network_tag: TEST_NETWORK.id(),
        segment_flags: 0,
        codec_id: RawBlockCodec::None.id(),
        start_height: 1,
        block_count: spec.block_count,
        first_prev_hash_wire: encode_internal_block_hash(block_hash(0)),
        last_block_hash_wire: encode_internal_block_hash(end_block.hash),
        slab_offset,
        tx_table_offset,
        row_index_offset,
    };

    let mut sparse = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    sparse.write_all(&header.encode())?;
    if spec.write_frame_prefix {
        let payload_len = spec.slab_len - u64::from(FRAME_PREFIX_U32);
        sparse.write_all(&u32::try_from(payload_len)?.to_be_bytes())?;
    }
    sparse.set_len(total_len)?;
    sparse.sync_all()?;

    let mut identity = RawBlockSegmentIdentity::span(
        TEST_NETWORK,
        RawBlockCodec::None,
        BlockHeight::new(1),
        end_block,
    );
    identity.sha256 = [0x99; 32];
    Ok(identity)
}

#[test]
fn open_rejects_forged_sparse_file_with_huge_tx_table_without_allocating() -> TestOutcome {
    let dir = TempDir::new()?;
    let path = dir.path().join("sparse-huge-tx.seg");
    let expected = write_forged_sparse_segment(
        &path,
        ForgedSparseSpec {
            block_count: 1,
            slab_len: u64::from(FRAME_PREFIX_U32) + 64,
            tx_count: u64::from(u32::MAX),
            write_frame_prefix: false,
        },
    )?;
    let outcome = SettledRawBlockSegmentReader::open(
        &path,
        &expected,
        SettledRawBlockSegmentOpenOptions::default(),
    );
    assert!(matches!(
        outcome,
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[test]
fn open_rejects_forged_sparse_file_with_bounded_tx_table_and_bad_digest() -> TestOutcome {
    let dir = TempDir::new()?;
    let path = dir.path().join("sparse-bounded-tx.seg");
    let expected = write_forged_sparse_segment(
        &path,
        ForgedSparseSpec {
            block_count: 1,
            slab_len: u64::from(FRAME_PREFIX_U32) + 64,
            tx_count: 65_536,
            write_frame_prefix: true,
        },
    )?;
    let outcome = SettledRawBlockSegmentReader::open(
        &path,
        &expected,
        SettledRawBlockSegmentOpenOptions::default(),
    );
    assert!(matches!(
        outcome,
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn open_rejects_forged_sparse_file_with_oversized_block_count_without_allocating() -> TestOutcome {
    let dir = TempDir::new()?;
    let path = dir.path().join("sparse-huge-blocks.seg");
    let expected = write_forged_sparse_segment(
        &path,
        ForgedSparseSpec {
            block_count: 100_000_000,
            slab_len: u64::from(FRAME_PREFIX_U32) + 1,
            tx_count: 0,
            write_frame_prefix: false,
        },
    )?;
    let outcome = SettledRawBlockSegmentReader::open(
        &path,
        &expected,
        SettledRawBlockSegmentOpenOptions::default(),
    );
    assert!(matches!(
        outcome,
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[test]
fn open_rejects_forged_sparse_file_with_large_bounded_block_count_and_bad_digest() -> TestOutcome {
    let dir = TempDir::new()?;
    let path = dir.path().join("sparse-bounded-blocks.seg");
    let expected = write_forged_sparse_segment(
        &path,
        ForgedSparseSpec {
            block_count: 500_000,
            slab_len: 2_500_000,
            tx_count: 0,
            write_frame_prefix: false,
        },
    )?;
    let outcome = SettledRawBlockSegmentReader::open(
        &path,
        &expected,
        SettledRawBlockSegmentOpenOptions::default(),
    );
    assert!(matches!(
        outcome,
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn compact_index_drops_in_ram_tx_slice_table_while_reading_across_pages() -> TestOutcome {
    let dir = TempDir::new()?;
    let slices_per_block = 3_500_usize;
    let raw_block = vec![0xab_u8; 8_192];
    let mut tx_slices = Vec::with_capacity(slices_per_block);
    for idx in 0..slices_per_block {
        let offset = u32::try_from(idx % 8_000)?;
        tx_slices.push(RawBlockTransactionSlice::new(offset, 1));
    }
    let blocks = vec![
        FixtureBlock {
            id: BlockId::new(BlockHeight::new(1), block_hash(1)),
            parent_hash: block_hash(0),
            raw_bytes: raw_block.clone(),
            tx_slices: tx_slices.clone(),
        },
        FixtureBlock {
            id: BlockId::new(BlockHeight::new(2), block_hash(2)),
            parent_hash: block_hash(1),
            raw_bytes: raw_block.clone(),
            tx_slices,
        },
    ];
    let (path, identity) = write_segment(dir.path(), "prot-buf", RawBlockCodec::Zstd, &blocks)?;
    let raw_file_bytes = fs::read(&path)?;
    let portable_id = RawBlockSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledRawBlockSegmentReader::open_source(
        Bytes::from(raw_file_bytes.clone()),
        &portable_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    // 7,000 txs * 40 B = 280 KiB on disk; in-memory footprint stays tiny (zero Vec<TxSliceEntry>).
    assert_eq!(reader.tx_entry_count(), 7_000);
    assert!(reader.heap_metadata_bytes() <= 2 * 20 + 30 * 32);
    assert_eq!(reader.read_raw_block(BlockHeight::new(2))?, raw_block);
    assert_eq!(
        reader.read_raw_transaction(BlockHeight::new(2), 3_499)?,
        vec![0xab_u8]
    );
    let batch = reader.read_raw_transactions_in_block(BlockHeight::new(2), &[0, 3_499, 1_024])?;
    assert_eq!(batch, vec![vec![0xab_u8], vec![0xab_u8], vec![0xab_u8]]);

    let mut wrong_identity = portable_id;
    wrong_identity.sha256[0] ^= 0xff;
    assert!(matches!(
        SettledRawBlockSegmentReader::open_source(
            Bytes::from(raw_file_bytes),
            &wrong_identity,
            SettledRawBlockSegmentOpenOptions::default(),
        ),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn writer_rejects_block_exceeding_max_tx_entries_limit() -> TestOutcome {
    let dir = TempDir::new()?;
    let target = dir.path().join("too-many-txs");
    let staged = NamedTempFile::new_in(dir.path())?;
    let block_id = BlockId::new(BlockHeight::new(1), block_hash(1));
    let mut writer = SettledRawBlockSegmentWriter::create(
        &target,
        staged,
        RawBlockSegmentIdentity::span(
            TEST_NETWORK,
            RawBlockCodec::None,
            BlockHeight::new(1),
            block_id,
        ),
    )?;
    let overflow_count = usize::try_from(MAX_TX_ENTRIES_PER_BLOCK)? + 1;
    let raw_bytes = vec![0x5a_u8; 64];
    let tx_slices = vec![RawBlockTransactionSlice::new(0, 1); overflow_count];
    let outcome = writer.push(block_id, block_hash(0), &raw_bytes, &tx_slices);
    assert!(matches!(
        outcome,
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[test]
fn decode_index_rejects_single_block_exceeding_per_block_tx_limit() -> TestOutcome {
    let per_block_overflow = u32::try_from(MAX_TX_ENTRIES_PER_BLOCK)? + 1;
    let tx_count_usize = usize::try_from(per_block_overflow)?;
    let sample_tx = TxSliceEntry {
        offset: 0,
        len: 1,
        sha256: [0x11; 32],
    }
    .encode();
    let mut raw_tx_table = Vec::with_capacity(tx_count_usize * TX_SLICE_ENTRY_LEN);
    for _ in 0..tx_count_usize {
        raw_tx_table.extend_from_slice(&sample_tx);
    }
    let frame_len = u64::from(FRAME_PREFIX_U32) + 64;
    let slab_len = 2 * frame_len;
    let tx_table_offset = HEADER_LEN as u64 + slab_len;
    let tx_table_bytes = u64::from(per_block_overflow) * TX_SLICE_ENTRY_LEN as u64;
    let row_index_offset = tx_table_offset + tx_table_bytes;
    let header = RawBlockSegmentHeader {
        network_tag: TEST_NETWORK.id(),
        segment_flags: 0,
        codec_id: RawBlockCodec::None.id(),
        start_height: 1,
        block_count: 2,
        first_prev_hash_wire: encode_internal_block_hash(block_hash(0)),
        last_block_hash_wire: encode_internal_block_hash(block_hash(2)),
        slab_offset: HEADER_LEN as u64,
        tx_table_offset,
        row_index_offset,
    };
    let mut raw_rows = Vec::with_capacity(2 * ROW_INDEX_ENTRY_LEN);
    raw_rows.extend_from_slice(
        &DecodedRowIndexEntry {
            block_hash_wire: encode_internal_block_hash(block_hash(1)),
            payload_sha256: [0xaa; 32],
            uncompressed_sha256: [0xaa; 32],
            slab_rel_offset: 0,
            uncompressed_len: 64,
            tx_start_index: 0,
            tx_count: per_block_overflow,
        }
        .encode(),
    );
    raw_rows.extend_from_slice(
        &DecodedRowIndexEntry {
            block_hash_wire: encode_internal_block_hash(block_hash(2)),
            payload_sha256: [0xbb; 32],
            uncompressed_sha256: [0xbb; 32],
            slab_rel_offset: u32::try_from(frame_len)?,
            uncompressed_len: 64,
            tx_start_index: per_block_overflow,
            tx_count: 0,
        }
        .encode(),
    );
    let (index_and_trailer, expected, total_len) =
        synthetic_index_identity(&header, &raw_tx_table, &raw_rows);
    assert!(matches!(
        SettledRawBlockSegmentIndex::decode_index(
            &header.encode(),
            &index_and_trailer,
            &expected,
            total_len,
            true,
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[test]
fn sans_io_planner_and_page_tamper_detection_on_bytes_source() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "sans-io-src", RawBlockCodec::Zstd, &blocks)?;
    let raw_bytes = fs::read(&path)?;
    let portable_id = RawBlockSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledRawBlockSegmentReader::open_source(
        Bytes::from(raw_bytes.clone()),
        &portable_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    let index = reader.index();
    let tx_plan = index.plan_tx_read(BlockHeight::new(4), 2)?;
    let row_start = usize::try_from(tx_plan.row_index_byte_range().offset)?;
    let raw_rows = &raw_bytes[row_start..row_start + tx_plan.row_index_byte_range().len];
    let tx_start = usize::try_from(tx_plan.tx_table_byte_range().offset)?;
    let raw_txs = &raw_bytes[tx_start..tx_start + tx_plan.tx_table_byte_range().len];
    let slab_start = usize::try_from(tx_plan.slab_byte_range().offset)?;
    let raw_frame = &raw_bytes[slab_start..slab_start + tx_plan.slab_byte_range().len];
    let verified_rows = index.authenticate_row_index_slice(RawBlockRowIndexSlice::new(
        tx_plan.row_index_byte_range().offset,
        raw_rows,
    ))?;
    let verified_txs = index.authenticate_tx_table_slice(RawBlockTxTableSlice::new(
        tx_plan.tx_table_byte_range().offset,
        raw_txs,
    ))?;
    let recovered_tx =
        index.verify_and_decode_tx(&tx_plan, verified_rows, verified_txs, raw_frame)?;
    let slice = blocks[3].tx_slices[2];
    let expected_slice =
        &blocks[3].raw_bytes[slice.offset as usize..slice.offset as usize + slice.len as usize];
    assert_eq!(recovered_tx.as_slice(), expected_slice);

    let (tx_off, row_off) = header_offsets(&raw_bytes)?;
    let mut tampered_row = raw_rows.to_vec();
    tampered_row[0] ^= 0xff;
    assert!(matches!(
        index.authenticate_row_index_slice(RawBlockRowIndexSlice::new(
            tx_plan.row_index_byte_range().offset,
            &tampered_row,
        )),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    let _ = (tx_off, row_off);

    let reader2 = SettledRawBlockSegmentReader::open_source(
        Bytes::from(raw_bytes),
        &portable_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    let mut visited = Vec::new();
    let count = reader2.visit_indexed_transaction_digests::<SettledRawBlockSegmentError, _>(
        BlockHeight::new(4),
        |tx_idx, digest| {
            visited.push((tx_idx, digest));
            Ok(())
        },
    )?;
    assert_eq!(count, 4);
    assert_eq!(visited.len(), 4);
    let rows_meta = reader2.read_all_row_hashes_and_digests()?;
    assert_eq!(rows_meta.len(), blocks.len());
    assert_eq!(
        rows_meta[3].1,
        SerializedBytesDigest::from_serialized_bytes(&blocks[3].raw_bytes)
    );
    Ok(())
}

#[test]
fn portable_post_admission_tamper_of_row_index_and_tx_table_latches_invalid() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let (path, identity) =
        write_segment(dir.path(), "post-admit-meta", RawBlockCodec::None, &blocks)?;
    let original = fs::read(&path)?;
    let (tx_off, row_off) = header_offsets(&original)?;
    let portable_id = RawBlockSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };

    let row_path = dir.path().join("post-admit-row");
    fs::write(&row_path, &original)?;
    let reader_row = SettledRawBlockSegmentReader::open(
        &row_path,
        &portable_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    let mut file_row = OpenOptions::new().write(true).open(&row_path)?;
    file_row.seek(SeekFrom::Start(u64::try_from(row_off + 12)?))?;
    file_row.write_all(&[original[row_off + 12] ^ 0x55])?;
    file_row.sync_all()?;
    drop(file_row);
    assert!(matches!(
        reader_row.read_raw_block(BlockHeight::new(1)),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_row.read_raw_block(BlockHeight::new(1)),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));

    let tx_path = dir.path().join("post-admit-tx");
    fs::write(&tx_path, &original)?;
    let reader_tx = SettledRawBlockSegmentReader::open(
        &tx_path,
        &portable_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    let mut file_tx = OpenOptions::new().write(true).open(&tx_path)?;
    file_tx.seek(SeekFrom::Start(u64::try_from(tx_off + 12)?))?;
    file_tx.write_all(&[original[tx_off + 12] ^ 0x55])?;
    file_tx.sync_all()?;
    drop(file_tx);
    assert!(matches!(
        reader_tx.read_raw_transaction(BlockHeight::new(2), 0),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_tx.read_raw_transaction(BlockHeight::new(2), 0),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));

    let frame_tamper_path = dir.path().join("post-admit-full-verify");
    let mut frame_tamper = original;
    let payload_byte = HEADER_LEN + FRAME_PREFIX_LEN + 1;
    frame_tamper[payload_byte] ^= 0x33;
    fs::write(&frame_tamper_path, &frame_tamper)?;
    assert!(matches!(
        SettledRawBlockSegmentReader::open(
            &frame_tamper_path,
            &portable_id,
            SettledRawBlockSegmentOpenOptions {
                require_fs_verity: false,
                verify_full_payload: true,
            },
        ),
        Err(SettledRawBlockSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}
