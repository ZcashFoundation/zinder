use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};

use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempDir};
use zinder_core::{BlockHash, BlockHeight, BlockId, Network, wire::encode_internal_block_hash};

use super::{
    RawBlockCodec, RawBlockSegmentIdentity, RawBlockTransactionSlice,
    SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES, SettledRawBlockSegmentError,
    SettledRawBlockSegmentOpenOptions, SettledRawBlockSegmentReader, SettledRawBlockSegmentWriter,
    format::{
        DecodedRowIndexEntry, FRAME_PREFIX_U32, HEADER_LEN, IndexedBlock, ROW_INDEX_ENTRY_LEN,
        SegmentHeader, TRAILER_LEN, TX_SLICE_ENTRY_LEN, TxSliceEntry, VALIDATION_INVALID,
        VALIDATION_UNVALIDATED, VALIDATION_VALID,
    },
    reader::{PortableFrameDigest, validate_entries},
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

fn recompute_portable_sha256(bytes: &[u8]) -> [u8; 32] {
    let body_end = bytes.len() - TRAILER_LEN;
    Sha256::digest(&bytes[..body_end]).into()
}

fn header_offsets(segment: &[u8]) -> TestOutcome<(usize, usize)> {
    let tx_table_offset = usize::try_from(u64::from_be_bytes(segment[96..104].try_into()?))?;
    let row_index_offset = usize::try_from(u64::from_be_bytes(segment[104..112].try_into()?))?;
    Ok((tx_table_offset, row_index_offset))
}

fn assert_exact_block_and_tx_recovery(
    reader: &SettledRawBlockSegmentReader,
    blocks: &[FixtureBlock],
) -> TestOutcome {
    assert_eq!(reader.block_count(), blocks.len());
    for block in blocks {
        let recovered_block = reader.read_raw_block(block.id.height)?;
        assert_eq!(recovered_block, block.raw_bytes);
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
            SettledRawBlockSegmentOpenOptions::default(),
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
    // Writing the identical segment a second time atomically replaces after verifying existing.
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

#[test]
fn protected_index_admission_refuses_oversized_block_and_frame_without_allocating() -> TestOutcome {
    let max_u32 = u32::try_from(SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES)?;
    let header = SegmentHeader {
        network_tag: TEST_NETWORK.id(),
        segment_flags: 0,
        codec_id: RawBlockCodec::Zstd.id(),
        start_height: 1,
        block_count: 1,
        first_prev_hash_wire: encode_internal_block_hash(block_hash(0)),
        last_block_hash_wire: encode_internal_block_hash(block_hash(1)),
        slab_offset: HEADER_LEN as u64,
        tx_table_offset: HEADER_LEN as u64 + 100,
        row_index_offset: HEADER_LEN as u64 + 100,
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
    assert!(matches!(
        validate_entries(
            &[oversized_uncompressed],
            &[],
            100,
            &header,
            None::<&[PortableFrameDigest]>,
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));

    let valid_uncompressed = DecodedRowIndexEntry {
        uncompressed_len: 1024,
        ..oversized_uncompressed
    };
    let near_4gib_slab_len = u32::MAX - 1024;
    assert!(matches!(
        validate_entries(
            &[valid_uncompressed],
            &[],
            near_4gib_slab_len,
            &header,
            None::<&[PortableFrameDigest]>,
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
    let body_end = original.len() - TRAILER_LEN;

    let mut oversized_buf = original.clone();
    let max_plus_one = u32::try_from(SETTLED_RAW_BLOCK_SEGMENT_MAX_BLOCK_BYTES + 1)?;
    let max_plus_one_bytes = max_plus_one.to_be_bytes();
    oversized_buf[row_index_offset + 100..row_index_offset + 104]
        .copy_from_slice(&max_plus_one_bytes);
    let oversized_sha = recompute_portable_sha256(&oversized_buf);
    oversized_buf[body_end..].copy_from_slice(&oversized_sha);
    let oversized_path = dir.path().join("forged-oversized");
    fs::write(&oversized_path, &oversized_buf)?;
    let oversized_id = RawBlockSegmentIdentity {
        sha256: oversized_sha,
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
    let oob_sha = recompute_portable_sha256(&oob_tx_buf);
    oob_tx_buf[body_end..].copy_from_slice(&oob_sha);
    let oob_path = dir.path().join("forged-oob-tx");
    fs::write(&oob_path, &oob_tx_buf)?;
    let oob_id = RawBlockSegmentIdentity {
        sha256: oob_sha,
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

fn corrupt_and_open(
    tampered_path: &Path,
    original: &[u8],
    identity: RawBlockSegmentIdentity,
    mutator: &dyn Fn(&mut Vec<u8>),
    recompute_trailer: bool,
) -> TestOutcome<SettledRawBlockSegmentError> {
    let mut mutated = original.to_vec();
    mutator(&mut mutated);
    let mut expected = identity;
    expected.fs_verity_sha256 = None;
    if recompute_trailer {
        let forged_sha = recompute_portable_sha256(&mutated);
        let body_end = mutated.len() - TRAILER_LEN;
        mutated[body_end..].copy_from_slice(&forged_sha);
        expected.sha256 = forged_sha;
    }
    fs::write(tampered_path, &mutated)?;
    match SettledRawBlockSegmentReader::open(
        tampered_path,
        &expected,
        SettledRawBlockSegmentOpenOptions::default(),
    ) {
        Ok(_) => Err("expected tamper refusal on open".into()),
        Err(err) => Ok(err),
    }
}

#[test]
fn reader_fails_closed_on_tampered_header_index_slab_and_trailer() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "tamper-base", RawBlockCodec::Zstd, &blocks)?;
    let original = fs::read(&path)?;
    let (_tx_off, row_off) = header_offsets(&original)?;
    let out = dir.path().join("tampered");

    assert!(matches!(
        corrupt_and_open(&out, &original, identity, &|buf| buf[0] ^= 0xff, false)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(&out, &original, identity, &|buf| buf[13] = 1, false)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(&out, &original, identity, &|buf| buf[15] = 9, false)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(&out, &original, identity, &|buf| buf[120] = 1, false)?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(
            &out,
            &original,
            identity,
            &|buf| buf[row_off + 115] = 1,
            true
        )?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    assert!(matches!(
        corrupt_and_open(
            &out,
            &original,
            identity,
            &|buf| buf[HEADER_LEN] = 0xff,
            false
        )?,
        SettledRawBlockSegmentError::InvalidFormat(_)
    ));
    let slab_payload_byte = HEADER_LEN + usize::try_from(FRAME_PREFIX_U32)? + 2;
    assert!(matches!(
        corrupt_and_open(
            &out,
            &original,
            identity,
            &|buf| buf[slab_payload_byte] ^= 0x01,
            false,
        )?,
        SettledRawBlockSegmentError::PayloadIntegrity(_)
    ));
    assert!(matches!(
        corrupt_and_open(
            &out,
            &original,
            identity,
            &|buf| {
                let last = buf.len() - 1;
                buf[last] ^= 0x01;
            },
            false,
        )?,
        SettledRawBlockSegmentError::PayloadIntegrity(_)
    ));
    let last_row = row_off + (blocks.len() - 1) * ROW_INDEX_ENTRY_LEN;
    assert!(matches!(
        corrupt_and_open(
            &out,
            &original,
            identity,
            &|buf| buf[last_row] ^= 0x01,
            true
        )?,
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
    let bad_block_sha = recompute_portable_sha256(&bad_block_buf);
    let bad_block_trailer_start = bad_block_buf.len() - TRAILER_LEN;
    bad_block_buf[bad_block_trailer_start..].copy_from_slice(&bad_block_sha);
    let bad_block_path = dir.path().join("bad-block-sha");
    fs::write(&bad_block_path, &bad_block_buf)?;
    let reader_block = SettledRawBlockSegmentReader::open(
        &bad_block_path,
        &RawBlockSegmentIdentity {
            sha256: bad_block_sha,
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
    let bad_tx_sha = recompute_portable_sha256(&bad_tx_buf);
    let bad_tx_trailer_start = bad_tx_buf.len() - TRAILER_LEN;
    bad_tx_buf[bad_tx_trailer_start..].copy_from_slice(&bad_tx_sha);
    let bad_tx_path = dir.path().join("bad-tx-sha");
    fs::write(&bad_tx_path, &bad_tx_buf)?;
    let reader_tx = SettledRawBlockSegmentReader::open(
        &bad_tx_path,
        &RawBlockSegmentIdentity {
            sha256: bad_tx_sha,
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

    let header = SegmentHeader {
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
    assert!(matches!(
        validate_entries(
            &[row],
            &[overflow_tx],
            64,
            &header,
            None::<&[PortableFrameDigest]>,
        ),
        Err(SettledRawBlockSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

#[test]
fn indexed_block_mark_valid_preserves_invalid_state() {
    let row = IndexedBlock::from_entry(&DecodedRowIndexEntry {
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

    let valid_row = IndexedBlock::from_entry(&DecodedRowIndexEntry {
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
