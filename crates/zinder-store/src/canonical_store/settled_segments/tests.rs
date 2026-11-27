//! T0 unit tests for canonical settled segments layout, manifests, live-tail
//! records, aligned sealing, `.staging/` recovery, pinned snapshots, and LRU cache.

use std::{fs, io, path::Path, sync::Arc};

use sha2::{Digest as _, Sha256};
use tempfile::{NamedTempFile, TempDir};
use zinder_core::{
    AuthDigest, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFacts, CanonicalTransactionFacts, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, CompactTransparentInput, CompactTransparentOutput, ConsensusBranchId,
    LockTime, Network, PrivacyShape, SerializedBytesDigest, TransactionComponentCounts,
    TransactionId, TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
    TransparentAddressScriptHash, TransparentInputFact, TransparentOutPoint, TransparentOutputFact,
    UnsupportedSection, Wtxid,
};

use crate::{
    HeightOrder,
    settled_raw_block_segment::{
        RAW_FRAME_PREFIX_LEN, RAW_HEADER_LEN, RAW_ROW_INDEX_ENTRY_LEN, RAW_TRAILER_LEN,
        RAW_TX_SLICE_ENTRY_LEN, RawBlockCodec, RawBlockSegmentIdentity, RawBlockTransactionSlice,
        SettledRawBlockSegmentOpenOptions, SettledRawBlockSegmentReader,
        SettledRawBlockSegmentWriter,
    },
    settled_shielded_segment::{
        PoolPresenceMask, SHIELDED_HEADER_LEN, SHIELDED_ROW_INDEX_ENTRY_LEN, SHIELDED_TRAILER_LEN,
        SettledShieldedSegmentError, SettledShieldedSegmentOpenOptions,
        SettledShieldedSegmentReader,
    },
    settled_supplement_segment::{
        CompactBlockSpliceSelection, SUPPLEMENT_CHUNK_INDEX_ENTRY_LEN, SUPPLEMENT_HEADER_LEN,
        SUPPLEMENT_ROW_INDEX_ENTRY_LEN, SUPPLEMENT_TRAILER_LEN,
        SettledSupplementSegmentOpenOptions, SettledSupplementSegmentReader,
        SettledSupplementSegmentWriteOptions,
    },
};

use super::super::CanonicalSequenceCheckpoint;
use super::super::live_tail::{
    LIVE_TAIL_RAW_BLOCK_VERSION_V1, LIVE_TAIL_RECORD_VERSION_V1, LiveTailBlockRecord,
    LiveTailRawBlock, LiveTailRawTxSlice, encode_default_shielded_payload,
    encode_default_shielded_payload_with_options,
};
use super::{
    SettledSegmentsError, VerifiedDir,
    cache::{DEFAULT_SETTLED_SEGMENT_READER_CACHE_CAPACITY, SettledSegmentReaderCache},
    layout::{
        CanonicalSegmentsManifest, SEGMENTS_DIR_NAME, SEGMENTS_MANIFEST_FILE_NAME,
        SEGMENTS_STAGING_DIR_NAME, SETTLED_SEGMENT_INDEX_FORMAT_V1, SettledSegmentFilePaths,
        SettledSegmentIndexEntry, SettledSegmentKind, SettledSegmentModules,
        aligned_segment_end_height, is_aligned_segment_end_height, parse_segment_filename,
        read_segments_manifest, segment_dir_path, segment_file_path, segment_staging_dir_path,
        segments_root_dir, write_segments_manifest_atomic,
        write_segments_manifest_atomic_in_dir_with_limit,
    },
    sealer::{
        DEFAULT_MAX_SETTLED_SEGMENT_BYTES, SettledSegmentBlockInput, SettledSegmentSealer,
        SettledSegmentSealerConfig, cleanup_stale_staging_files,
    },
    snapshot::{
        snapshot_pinned_segments, snapshot_pinned_segments_with_linker,
        verify_pinned_manifest_and_segments, verify_pinned_segments,
    },
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const TEST_SCHEMA_VERSION: u16 = 7;

#[test]
fn layout_paths_filenames_and_aligned_strides_validate_strictly() -> TestResult {
    let root = Path::new("/var/zinder/store");
    assert_eq!(segments_root_dir(root), root.join("segments"));
    assert_eq!(
        segment_staging_dir_path(root),
        root.join("segments").join(".staging")
    );
    assert_eq!(
        segment_dir_path(root, SettledSegmentKind::Shielded),
        root.join("segments").join("shielded")
    );
    assert_eq!(
        segment_dir_path(root, SettledSegmentKind::Supplement),
        root.join("segments").join("supplement")
    );
    assert_eq!(
        segment_dir_path(root, SettledSegmentKind::RawBlock),
        root.join("segments").join("raw_block")
    );

    let shielded_path = segment_file_path(
        root,
        SettledSegmentKind::Shielded,
        BlockHeight::new(1),
        BlockHeight::new(4096),
    )?;
    assert_eq!(
        shielded_path,
        root.join("segments")
            .join("shielded")
            .join("0000000001-0000004096.zshrng02")
    );

    assert_eq!(
        parse_segment_filename(
            SettledSegmentKind::Shielded,
            "0000000001-0000004096.zshrng02",
        )?,
        (BlockHeight::new(1), BlockHeight::new(4096))
    );
    assert_eq!(
        parse_segment_filename(
            SettledSegmentKind::Supplement,
            "0000004097-0000008192.zstsup01",
        )?,
        (BlockHeight::new(4097), BlockHeight::new(8192))
    );
    assert_eq!(
        parse_segment_filename(
            SettledSegmentKind::RawBlock,
            "0000004097-0000008192.zsblkb01",
        )?,
        (BlockHeight::new(4097), BlockHeight::new(8192))
    );

    check_invalid_filenames();
    check_aligned_stride_math()?;
    Ok(())
}

fn check_invalid_filenames() {
    let bad_cases = [
        (SettledSegmentKind::Shielded, "1-4.zshrng02"),
        (
            SettledSegmentKind::Shielded,
            "00000000001-0000000004.zshrng02",
        ),
        (
            SettledSegmentKind::Shielded,
            "0000000005-0000000004.zshrng02",
        ),
        (
            SettledSegmentKind::Shielded,
            "0000000001-0000000004.ZSHRNG02",
        ),
        (
            SettledSegmentKind::Shielded,
            "0000000001-0000000004.zstsup01",
        ),
        (
            SettledSegmentKind::Supplement,
            "../0000000001-0000000004.zstsup01",
        ),
        (SettledSegmentKind::RawBlock, ""),
    ];
    for (kind, name) in bad_cases {
        assert!(
            parse_segment_filename(kind, name).is_err(),
            "expected failure for filename {name:?}"
        );
    }
    for utf8_name in [
        "000000000é-0000000004.zshrng0",
        "0000000001-0000000004.zshrn🦀",
    ] {
        let outcome = parse_segment_filename(SettledSegmentKind::Shielded, utf8_name);
        assert!(
            matches!(outcome, Err(SettledSegmentsError::InvalidLayout(_))),
            "expected InvalidLayout for non-ASCII filename {utf8_name:?}"
        );
    }
}

fn check_aligned_stride_math() -> TestResult {
    assert_eq!(
        aligned_segment_end_height(BlockHeight::new(0), 4)?,
        BlockHeight::new(0)
    );
    assert_eq!(
        aligned_segment_end_height(BlockHeight::new(1), 4)?,
        BlockHeight::new(4)
    );
    assert_eq!(
        aligned_segment_end_height(BlockHeight::new(3), 4)?,
        BlockHeight::new(4)
    );
    assert_eq!(
        aligned_segment_end_height(BlockHeight::new(5), 4)?,
        BlockHeight::new(8)
    );
    assert!(is_aligned_segment_end_height(BlockHeight::new(4), 4));
    assert!(!is_aligned_segment_end_height(BlockHeight::new(3), 4));
    assert!(!is_aligned_segment_end_height(BlockHeight::new(4), 0));
    assert!(aligned_segment_end_height(BlockHeight::new(1), 0).is_err());
    assert!(aligned_segment_end_height(BlockHeight::new(u32::MAX), 4).is_err());
    Ok(())
}

#[test]
fn index_entry_binary_v1_round_trip_and_fail_closed_checks() -> TestResult {
    let dir = TempDir::new()?;
    let modules_all = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let entry_full = seal_test_stride(dir.path(), 1..=4, 4, modules_all, None)?;
    let encoded_full = entry_full.encode_v1()?;
    let decoded_full = SettledSegmentIndexEntry::decode_v1(&encoded_full)?;
    assert_eq!(decoded_full, entry_full);

    let modules_shielded = SettledSegmentModules {
        supplement: false,
        raw_bodies: false,
    };
    let dir_shielded = TempDir::new()?;
    let entry_shielded = seal_test_stride(dir_shielded.path(), 1..=4, 4, modules_shielded, None)?;
    let encoded_shielded = entry_shielded.encode_v1()?;
    let decoded_shielded = SettledSegmentIndexEntry::decode_v1(&encoded_shielded)?;
    assert_eq!(decoded_shielded, entry_shielded);

    assert_index_corruption_failures(&entry_full, &encoded_full, &encoded_shielded);
    Ok(())
}

fn assert_index_corruption_failures(
    entry_full: &SettledSegmentIndexEntry,
    encoded_full: &[u8],
    encoded_shielded: &[u8],
) {
    for cut in 0..encoded_full.len() {
        assert!(SettledSegmentIndexEntry::decode_v1(&encoded_full[..cut]).is_err());
    }

    let mut with_trailing = encoded_full.to_vec();
    with_trailing.push(0x00);
    assert!(SettledSegmentIndexEntry::decode_v1(&with_trailing).is_err());

    let mut bad_version = encoded_full.to_vec();
    let bad_ver = (SETTLED_SEGMENT_INDEX_FORMAT_V1 + 1).to_be_bytes();
    bad_version[0..2].copy_from_slice(&bad_ver);
    assert!(SettledSegmentIndexEntry::decode_v1(&bad_version).is_err());

    let mut bad_flags = encoded_full.to_vec();
    bad_flags[2] |= 0x80;
    assert!(SettledSegmentIndexEntry::decode_v1(&bad_flags).is_err());

    let mut bad_reserved = encoded_full.to_vec();
    bad_reserved[3] = 0x01;
    assert!(SettledSegmentIndexEntry::decode_v1(&bad_reserved).is_err());

    let mut claimed_sup_but_absent = encoded_shielded.to_vec();
    claimed_sup_but_absent[2] |= 0x01;
    assert!(SettledSegmentIndexEntry::decode_v1(&claimed_sup_but_absent).is_err());

    let mut missing_sup = entry_full.clone();
    missing_sup.supplement_identity = None;
    assert!(missing_sup.validate().is_err());

    let mut missing_raw = entry_full.clone();
    missing_raw.raw_block_identity = None;
    assert!(missing_raw.validate().is_err());
}

#[test]
fn manifest_json_atomic_round_trip_and_validation() -> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: false,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let entry_1 = push_synthetic_range(&mut sealer, 1..=4, false)?.ok_or("expected stride 1")?;
    let entry_2 = push_synthetic_range(&mut sealer, 5..=8, false)?.ok_or("expected stride 2")?;

    let manifest = CanonicalSegmentsManifest::from_entries(
        Network::ZcashRegtest,
        vec![entry_1.clone(), entry_2.clone()],
    )?;
    write_segments_manifest_atomic(dir.path(), &manifest)?;
    let reloaded = read_segments_manifest(dir.path())?;
    assert_eq!(reloaded, manifest);

    assert_manifest_rejections(&manifest, &entry_1, &entry_2)?;
    Ok(())
}

fn assert_manifest_rejections(
    manifest: &CanonicalSegmentsManifest,
    entry_1: &SettledSegmentIndexEntry,
    entry_2: &SettledSegmentIndexEntry,
) -> TestResult {
    let wrong_net = CanonicalSegmentsManifest {
        network: Network::ZcashTestnet,
        ..manifest.clone()
    };
    assert!(wrong_net.validate().is_err());

    let wrong_tip = CanonicalSegmentsManifest {
        sealed_tip: Some(entry_1.end_block),
        ..manifest.clone()
    };
    assert!(wrong_tip.validate().is_err());

    let mut gap_entry = entry_2.clone();
    gap_entry.start_height = BlockHeight::new(6);
    gap_entry.shielded_identity.start_height = BlockHeight::new(6);
    let non_contiguous = CanonicalSegmentsManifest {
        entries: vec![entry_1.clone(), gap_entry],
        ..manifest.clone()
    };
    assert!(non_contiguous.validate().is_err());

    let bad_dir = TempDir::new()?;
    let manifest_path = bad_dir.path().join(SEGMENTS_MANIFEST_FILE_NAME);
    write_segments_manifest_atomic(bad_dir.path(), manifest)?;
    let json_text = fs::read_to_string(&manifest_path)?;
    let with_unknown = json_text.replacen(
        "\"schema_version\": 2,",
        "\"schema_version\": 2,\n  \"extra_unknown\": true,",
        1,
    );
    fs::write(&manifest_path, with_unknown)?;
    assert!(read_segments_manifest(bad_dir.path()).is_err());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let sym_dir = TempDir::new()?;
        let real_manifest = sym_dir.path().join("other.json");
        fs::write(&real_manifest, json_text)?;
        symlink(
            &real_manifest,
            sym_dir.path().join(SEGMENTS_MANIFEST_FILE_NAME),
        )?;
        assert!(read_segments_manifest(sym_dir.path()).is_err());
    }
    Ok(())
}

#[test]
fn live_tail_v1_round_trip_and_tamper_fail_closed() -> TestResult {
    assert_eq!(LIVE_TAIL_RECORD_VERSION_V1, 1);
    let fixture = build_block_fixture(9);
    let (expected_payload, expected_mask) =
        encode_default_shielded_payload(&fixture.compact_block)?;
    assert!(!expected_mask.is_empty());

    let record_full = LiveTailBlockRecord::from_compact_block(
        &fixture.compact_block,
        true,
        Some(fixture.canonical_facts.clone()),
    )?;
    assert_eq!(record_full.shielded_payload, expected_payload);
    assert_eq!(record_full.pool_presence, expected_mask);
    assert_eq!(
        record_full.reconstruct_compact_block()?,
        fixture.compact_block
    );

    let framed_full = record_full.splice_compact_block(CompactBlockSpliceSelection::FULL_BLOCK)?;
    assert!(!framed_full.as_length_prefixed().is_empty());
    let sliced_tx0 = fixture.raw_block.read_verified_raw_transaction(0)?;
    assert_eq!(sliced_tx0, fixture.tx0_raw);

    let encoded = record_full.encode_v1()?;
    let decoded = LiveTailBlockRecord::decode_v1(&encoded)?;
    assert_eq!(decoded, record_full);

    let record_shielded_only =
        LiveTailBlockRecord::from_compact_block(&fixture.compact_block, false, None)?;
    assert!(
        record_shielded_only
            .splice_compact_block(CompactBlockSpliceSelection::FULL_BLOCK)
            .is_err()
    );
    let encoded_minimal = record_shielded_only.encode_v1()?;
    assert_eq!(
        LiveTailBlockRecord::decode_v1(&encoded_minimal)?,
        record_shielded_only
    );

    assert_live_tail_rejections(&encoded, &record_full, &fixture);
    Ok(())
}

fn assert_live_tail_rejections(
    encoded: &[u8],
    record_full: &LiveTailBlockRecord,
    fixture: &SyntheticBlockFixture,
) {
    for cut in [0, 8, 32, encoded.len() / 2, encoded.len() - 1] {
        assert!(LiveTailBlockRecord::decode_v1(&encoded[..cut]).is_err());
    }

    let mut trailing = encoded.to_vec();
    trailing.push(0xaa);
    assert!(LiveTailBlockRecord::decode_v1(&trailing).is_err());

    let mut bit_flip = encoded.to_vec();
    bit_flip[40] ^= 0x01;
    assert!(LiveTailBlockRecord::decode_v1(&bit_flip).is_err());

    assert_live_tail_header_field_rejections(encoded);

    let mut bad_raw = fixture.raw_block.clone();
    bad_raw.tx_slices[0].sha256[0] ^= 0xff;
    assert!(bad_raw.validate().is_err());

    let mut wrong_mask = record_full.clone();
    wrong_mask.pool_presence = PoolPresenceMask::EMPTY;
    assert!(wrong_mask.validate().is_err());

    let mut bad_replay = record_full.clone();
    if let Some(facts) = bad_replay.canonical_facts.as_mut() {
        facts.serialized_bytes_digest = SerializedBytesDigest::from_bytes([0xee; 32]);
    }
    assert!(bad_replay.validate().is_err());
}

fn assert_live_tail_header_field_rejections(encoded: &[u8]) {
    let mut bad_ver = encoded.to_vec();
    bad_ver[8..10].copy_from_slice(&2_u16.to_be_bytes());
    recompute_live_tail_trailer(&mut bad_ver);
    assert!(LiveTailBlockRecord::decode_v1(&bad_ver).is_err());

    let mut bad_flags = encoded.to_vec();
    bad_flags[10] |= 0x80;
    recompute_live_tail_trailer(&mut bad_flags);
    assert!(LiveTailBlockRecord::decode_v1(&bad_flags).is_err());

    let mut bad_reserved = encoded.to_vec();
    bad_reserved[11] = 0x01;
    recompute_live_tail_trailer(&mut bad_reserved);
    assert!(LiveTailBlockRecord::decode_v1(&bad_reserved).is_err());
}

fn recompute_live_tail_trailer(bytes: &mut [u8]) {
    let split_idx = bytes.len() - 32;
    let digest: [u8; 32] = Sha256::digest(&bytes[..split_idx]).into();
    bytes[split_idx..].copy_from_slice(&digest);
}

#[test]
fn live_tail_v1_rejects_oversize_limits_and_matches_boundary() -> TestResult {
    let fixture = build_block_fixture(11);
    let mut record = LiveTailBlockRecord::from_compact_block(
        &fixture.compact_block,
        true,
        Some(fixture.canonical_facts),
    )?;
    record.canonical_facts = None;
    let one_mib = 1024 * 1024;
    if let Some(sup) = record.supplement.as_mut() {
        sup.transparent_transactions[0].transparent_outputs[0].script_pub_key = vec![0xaa; one_mib];
        if let Some(meta) = sup.canonical_metadata.as_mut() {
            meta.transactions[0].public_facts.unsupported_sections =
                vec![UnsupportedSection::FutureVersionHeader; 64];
        }
    }
    record.validate()?;
    let encoded_ok = record.encode_v1()?;
    let decoded_ok = LiveTailBlockRecord::decode_v1(&encoded_ok)?;
    assert_eq!(decoded_ok, record);

    let mut script_too_big = record.clone();
    if let Some(sup) = script_too_big.supplement.as_mut() {
        sup.transparent_transactions[0].transparent_outputs[0]
            .script_pub_key
            .push(0xbb);
    }
    assert!(script_too_big.validate().is_err());
    assert!(script_too_big.encode_v1().is_err());

    let mut too_many_sections = record.clone();
    if let Some(sup) = too_many_sections.supplement.as_mut()
        && let Some(meta) = sup.canonical_metadata.as_mut()
    {
        meta.transactions[0]
            .public_facts
            .unsupported_sections
            .push(UnsupportedSection::FutureShieldedProtocol);
    }
    assert!(too_many_sections.validate().is_err());
    assert!(too_many_sections.encode_v1().is_err());

    let mut anchor_mismatch = record.clone();
    if let Some(sup) = anchor_mismatch.supplement.as_mut()
        && let Some(meta) = sup.canonical_metadata.as_mut()
    {
        meta.transactions[0].shielded_anchors.orchard = None;
    }
    assert!(anchor_mismatch.validate().is_err());
    assert!(anchor_mismatch.encode_v1().is_err());

    assert_live_tail_total_byte_budget_rejection(&record, one_mib);
    Ok(())
}

fn assert_live_tail_total_byte_budget_rejection(record: &LiveTailBlockRecord, one_mib: usize) {
    let mut oversized_total = record.clone();
    if let Some(sup) = oversized_total.supplement.as_mut() {
        sup.transparent_transactions[0].transparent_outputs = vec![
            CompactTransparentOutput {
                value_zat: 1,
                script_pub_key: vec![0xaa; one_mib],
            };
            65
        ];
    }
    assert!(oversized_total.validate().is_err());
    assert!(oversized_total.encode_v1().is_err());
}

#[test]
fn live_tail_raw_slices_reject_empty_oob_overlapping_and_out_of_order() -> TestResult {
    let raw_bytes = b"0123456789abcdef".to_vec();
    for bad in [
        vec![RawBlockTransactionSlice::new(0, 0)],
        vec![RawBlockTransactionSlice::new(12, 5)],
        vec![RawBlockTransactionSlice::new(u32::MAX, 2)],
        vec![
            RawBlockTransactionSlice::new(0, 6),
            RawBlockTransactionSlice::new(5, 4),
        ],
        vec![
            RawBlockTransactionSlice::new(8, 4),
            RawBlockTransactionSlice::new(2, 4),
        ],
    ] {
        assert!(LiveTailRawBlock::from_bytes_and_slices(raw_bytes.clone(), &bad).is_err());
    }

    let valid_raw = LiveTailRawBlock::from_bytes_and_slices(
        raw_bytes,
        &[
            RawBlockTransactionSlice::new(0, 6),
            RawBlockTransactionSlice::new(6, 10),
        ],
    )?;
    let valid_encoded = valid_raw.encode_v1()?;
    assert_eq!(LiveTailRawBlock::decode_v1(&valid_encoded)?, valid_raw);

    let mut bad_validate = valid_raw;
    bad_validate.tx_slices.swap(0, 1);
    assert!(bad_validate.validate().is_err());

    let mut forged_empty_slice = valid_encoded.clone();
    let second_len_pos = forged_empty_slice.len() - 32 - 36;
    forged_empty_slice[second_len_pos..second_len_pos + 4].copy_from_slice(&0_u32.to_be_bytes());
    recompute_live_tail_trailer(&mut forged_empty_slice);
    assert!(LiveTailRawBlock::decode_v1(&forged_empty_slice).is_err());

    let mut forged_overlap = valid_encoded;
    let second_off_pos = forged_overlap.len() - 32 - 40;
    forged_overlap[second_off_pos..second_off_pos + 4].copy_from_slice(&4_u32.to_be_bytes());
    recompute_live_tail_trailer(&mut forged_overlap);
    assert!(LiveTailRawBlock::decode_v1(&forged_overlap).is_err());
    Ok(())
}

#[test]
fn sealer_multi_stride_covers_all_three_module_combinations() -> TestResult {
    for modules in [
        SettledSegmentModules {
            supplement: false,
            raw_bodies: false,
        },
        SettledSegmentModules {
            supplement: true,
            raw_bodies: false,
        },
        SettledSegmentModules {
            supplement: true,
            raw_bodies: true,
        },
    ] {
        verify_sealer_for_modules(modules)?;
    }
    Ok(())
}

fn verify_sealer_for_modules(modules: SettledSegmentModules) -> TestResult {
    let dir = TempDir::new()?;
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    assert_eq!(sealer.pending_len(), 0);
    assert!(sealer.prior_checkpoint().is_none());

    for height in 1..=3_u32 {
        let input = build_block_input(height, modules.raw_bodies);
        assert_eq!(sealer.push_block(input)?, Vec::new());
        assert_eq!(sealer.pending_len(), usize::try_from(height)?);
    }
    let entry_1 = expect_single_entry(
        sealer.push_block(build_block_input(4, modules.raw_bodies))?,
        "stride 1 should seal at height 4",
    )?;
    assert_eq!(sealer.pending_len(), 0);
    assert_eq!(entry_1.start_height, BlockHeight::new(1));
    assert_eq!(entry_1.end_block.height, BlockHeight::new(4));
    assert_eq!(entry_1.end_sequence_checkpoint.retained_block_count(), 4);

    let mut resumed_sealer =
        SettledSegmentSealer::new(dir.path(), config, Some(entry_1.end_sequence_checkpoint))?;
    for height in 5..=7_u32 {
        let input = build_block_input(height, modules.raw_bodies);
        assert_eq!(resumed_sealer.push_block(input)?, Vec::new());
    }
    let entry_2 = expect_single_entry(
        resumed_sealer.push_block(build_block_input(8, modules.raw_bodies))?,
        "stride 2 should seal at height 8",
    )?;
    assert_eq!(entry_2.start_height, BlockHeight::new(5));
    assert_eq!(entry_2.end_block.height, BlockHeight::new(8));
    assert_eq!(entry_2.end_sequence_checkpoint.retained_block_count(), 8);
    assert!(
        entry_2.end_sequence_checkpoint.logical_replay_bytes()
            > entry_1.end_sequence_checkpoint.logical_replay_bytes()
    );

    assert_sealed_files_and_contents(dir.path(), &entry_1, &entry_2, modules)?;
    Ok(())
}

fn assert_sealed_files_and_contents(
    store_root: &Path,
    entry_1: &SettledSegmentIndexEntry,
    entry_2: &SettledSegmentIndexEntry,
    modules: SettledSegmentModules,
) -> TestResult {
    let staging_dir = segment_staging_dir_path(store_root);
    assert_eq!(fs::read_dir(&staging_dir)?.count(), 0);
    verify_pinned_segments(store_root, &[entry_1.clone(), entry_2.clone()], false)?;

    let paths_1 = SettledSegmentFilePaths::for_entry(store_root, entry_1)?;
    assert_eq!(paths_1.supplement.is_some(), modules.supplement);
    assert_eq!(paths_1.raw_block.is_some(), modules.raw_bodies);

    if !modules.supplement {
        assert!(!segment_dir_path(store_root, SettledSegmentKind::Supplement).exists());
    }
    if !modules.raw_bodies {
        assert!(!segment_dir_path(store_root, SettledSegmentKind::RawBlock).exists());
    }

    let shielded_reader = SettledShieldedSegmentReader::open(
        &paths_1.shielded,
        &entry_1.shielded_identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let frames = shielded_reader.read_default_range(
        BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(4)),
        HeightOrder::Ascending,
    )?;
    assert_eq!(frames.len(), 4);

    if let (Some(sup_path), Some(sup_id)) = (
        paths_1.supplement.as_deref(),
        entry_1.supplement_identity.as_ref(),
    ) {
        let sup_reader = SettledSupplementSegmentReader::open(
            sup_path,
            sup_id,
            SettledSupplementSegmentOpenOptions::default(),
        )?;
        let sup_rec = sup_reader.read_block(BlockHeight::new(2))?;
        assert_ne!(sup_rec.transparent_transactions.len(), 0);
        assert!(sup_rec.canonical_metadata.is_some());
    }

    if let (Some(raw_path), Some(raw_id)) = (
        paths_1.raw_block.as_deref(),
        entry_1.raw_block_identity.as_ref(),
    ) {
        let raw_reader = SettledRawBlockSegmentReader::open(
            raw_path,
            raw_id,
            SettledRawBlockSegmentOpenOptions::default(),
        )?;
        let fixture = build_block_fixture(3);
        assert_eq!(
            raw_reader.read_raw_block(BlockHeight::new(3))?,
            fixture.raw_block.raw_block_bytes
        );
        assert_eq!(
            raw_reader.read_raw_transaction(BlockHeight::new(3), 0)?,
            fixture.tx0_raw
        );
    }
    Ok(())
}

#[test]
fn sealer_rejects_gaps_wrong_parents_and_missing_raw_bodies() -> TestResult {
    let dir = TempDir::new()?;
    let config = SettledSegmentSealerConfig::new(
        Network::ZcashRegtest,
        TEST_SCHEMA_VERSION,
        SettledSegmentModules {
            supplement: true,
            raw_bodies: true,
        },
    )
    .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;

    let mut missing_raw = build_block_input(1, true);
    missing_raw.raw_block = None;
    assert!(sealer.push_block(missing_raw).is_err());

    assert_eq!(sealer.push_block(build_block_input(1, true))?, Vec::new());
    assert!(sealer.push_block(build_block_input(3, true)).is_err());

    let mut wrong_parent = build_block_fixture(2);
    let wrong_hash = BlockHash::from_bytes([0xfe; 32]);
    let parts = wrong_parent.compact_block.into_parts();
    wrong_parent.compact_block = CompactBlockArtifact::new(
        parts.block_id,
        wrong_hash,
        parts.time,
        parts.transactions,
        parts.chain_metadata,
    )?;
    wrong_parent.canonical_facts.block_header.parent_hash = wrong_hash;
    let bad_input = SettledSegmentBlockInput {
        compact_block: wrong_parent.compact_block,
        native_bytes: 512,
        canonical_facts: wrong_parent.canonical_facts,
        raw_block: Some(wrong_parent.raw_block),
    };
    assert!(sealer.push_block(bad_input).is_err());

    let mut bad_tx_digest = build_block_input(2, true);
    bad_tx_digest.canonical_facts.transactions[0].serialized_bytes_digest =
        SerializedBytesDigest::from_bytes([0xde; 32]);
    assert!(sealer.push_block(bad_tx_digest).is_err());
    Ok(())
}

#[test]
fn sealer_spools_to_staging_with_budget_and_cleans_up_on_drop_or_error() -> TestResult {
    let dir = TempDir::new()?;
    let config = SettledSegmentSealerConfig::new(
        Network::ZcashRegtest,
        TEST_SCHEMA_VERSION,
        SettledSegmentModules {
            supplement: true,
            raw_bodies: false,
        },
    )
    .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let staging_dir = segment_staging_dir_path(dir.path());
    for height in 1..=3_u32 {
        assert_eq!(
            sealer.push_block(build_block_input(height, false))?,
            Vec::new()
        );
    }
    assert_eq!(sealer.pending_len(), 3);
    assert_eq!(fs::read_dir(&staging_dir)?.count(), 1);
    drop(sealer);
    assert_eq!(fs::read_dir(&staging_dir)?.count(), 0);

    let tiny_budget = TempDir::new()?;
    let budgeted_config = SettledSegmentSealerConfig::new(
        Network::ZcashRegtest,
        TEST_SCHEMA_VERSION,
        SettledSegmentModules::default(),
    )
    .with_segment_blocks(4)
    .with_max_spool_bytes(128);
    let mut budgeted_sealer = SettledSegmentSealer::new(tiny_budget.path(), budgeted_config, None)?;
    assert!(
        budgeted_sealer
            .push_block(build_block_input(1, false))
            .is_err()
    );
    assert_eq!(budgeted_sealer.pending_len(), 0);
    assert!(!budgeted_sealer.is_poisoned());
    assert!(!segment_staging_dir_path(tiny_budget.path()).exists());
    Ok(())
}

#[test]
fn sealer_rejects_compact_tx_alignment_even_without_supplement() -> TestResult {
    let dir = TempDir::new()?;
    let config = SettledSegmentSealerConfig::new(
        Network::ZcashRegtest,
        TEST_SCHEMA_VERSION,
        SettledSegmentModules {
            supplement: false,
            raw_bodies: false,
        },
    )
    .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;

    let mut wrong_txid = build_block_input(1, false);
    wrong_txid.canonical_facts.transactions[0]
        .public_facts
        .transaction_id = TransactionId::from_bytes([0xcc; 32]);
    assert!(matches!(
        sealer.push_block(wrong_txid),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));

    let mut bad_idx_input = build_block_input(1, false);
    let parts = bad_idx_input.compact_block.into_parts();
    let mut shifted_txs = parts.transactions;
    shifted_txs[1].index = 99;
    bad_idx_input.compact_block = CompactBlockArtifact::new(
        parts.block_id,
        parts.previous_block_hash,
        parts.time,
        shifted_txs,
        parts.chain_metadata,
    )?;
    assert!(matches!(
        sealer.push_block(bad_idx_input),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));

    let mut extra_tx_input = build_block_input(1, false);
    extra_tx_input.canonical_facts.transactions.pop();
    assert!(matches!(
        sealer.push_block(extra_tx_input),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));
    Ok(())
}

#[test]
fn sealer_restart_idempotent_promotion_reuses_identical_and_rejects_tamper() -> TestResult {
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let full_dir = TempDir::new()?;
    let expected = seal_test_stride(full_dir.path(), 1..=4, 4, modules, None)?;
    let full_paths = SettledSegmentFilePaths::for_entry(full_dir.path(), &expected)?;

    let partial_dir = TempDir::new()?;
    let partial_paths = SettledSegmentFilePaths::for_entry(partial_dir.path(), &expected)?;
    if let Some(parent) = partial_paths.shielded.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(&full_paths.shielded, &partial_paths.shielded)?;
    let reused = seal_test_stride(partial_dir.path(), 1..=4, 4, modules, None)?;
    assert_eq!(reused, expected);
    assert_eq!(
        fs::read_dir(segment_staging_dir_path(partial_dir.path()))?.count(),
        0
    );
    verify_pinned_segments(partial_dir.path(), &[reused], false)?;

    let tampered_dir = TempDir::new()?;
    let tampered_paths = SettledSegmentFilePaths::for_entry(tampered_dir.path(), &expected)?;
    let tampered_sup = tampered_paths.supplement.ok_or("missing sup path")?;
    if let Some(parent) = tampered_sup.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(
        full_paths.supplement.as_ref().ok_or("missing src sup")?,
        &tampered_sup,
    )?;
    strip_verity_by_replace(&tampered_sup, true)?;
    let original_tamper = fs::read(&tampered_sup)?;
    assert!(matches!(
        seal_test_stride(tampered_dir.path(), 1..=4, 4, modules, None),
        Err(SettledSegmentsError::Integrity(_))
    ));
    assert_eq!(fs::read(&tampered_sup)?, original_tamper);

    assert_promotion_rejects_non_regular_destination(&expected, modules)
}

fn assert_promotion_rejects_non_regular_destination(
    expected: &SettledSegmentIndexEntry,
    modules: SettledSegmentModules,
) -> TestResult {
    let dir_conflict = TempDir::new()?;
    let conflict_paths = SettledSegmentFilePaths::for_entry(dir_conflict.path(), expected)?;
    let conflict_raw = conflict_paths.raw_block.ok_or("missing raw path")?;
    fs::create_dir_all(&conflict_raw)?;
    assert!(matches!(
        seal_test_stride(dir_conflict.path(), 1..=4, 4, modules, None),
        Err(SettledSegmentsError::InvalidLayout(_))
    ));

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let sym_conflict = TempDir::new()?;
        let outside = sym_conflict.path().join("outside-target");
        fs::write(&outside, b"untouched")?;
        let sym_paths = SettledSegmentFilePaths::for_entry(sym_conflict.path(), expected)?;
        let sym_raw = sym_paths.raw_block.ok_or("missing sym raw")?;
        if let Some(parent) = sym_raw.parent() {
            fs::create_dir_all(parent)?;
        }
        symlink(&outside, &sym_raw)?;
        assert!(matches!(
            seal_test_stride(sym_conflict.path(), 1..=4, 4, modules, None),
            Err(SettledSegmentsError::InvalidLayout(_))
        ));
        assert_eq!(fs::read(&outside)?, b"untouched");
    }
    Ok(())
}

#[test]
fn cleanup_stale_staging_files_recovers_orphans_and_refuses_symlinks() -> TestResult {
    let dir = TempDir::new()?;
    assert_eq!(cleanup_stale_staging_files(dir.path())?, 0);

    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: false,
    };
    let entry = seal_test_stride(dir.path(), 1..=4, 4, modules, None)?;
    let staging_dir = segment_staging_dir_path(dir.path());
    fs::write(staging_dir.join(".tmp-orphan-1"), b"partial shielded")?;
    fs::write(staging_dir.join("staged-partial.zstsup01"), b"partial sup")?;

    let removed = cleanup_stale_staging_files(dir.path())?;
    assert_eq!(removed, 2);
    assert_eq!(fs::read_dir(&staging_dir)?.count(), 0);
    verify_pinned_segments(dir.path(), &[entry], false)?;

    fs::create_dir(staging_dir.join("nested-dir"))?;
    assert!(cleanup_stale_staging_files(dir.path()).is_err());

    #[cfg(unix)]
    assert_staging_symlink_refusal()?;
    Ok(())
}

#[cfg(unix)]
fn assert_staging_symlink_refusal() -> TestResult {
    use std::os::unix::fs::symlink;

    let sym_dir = TempDir::new()?;
    let target_file = sym_dir.path().join("outside.txt");
    fs::write(&target_file, b"keep")?;
    let staging = segment_staging_dir_path(sym_dir.path());
    fs::create_dir_all(&staging)?;
    symlink(&target_file, staging.join("escape-link"))?;
    assert!(cleanup_stale_staging_files(sym_dir.path()).is_err());
    assert!(target_file.exists());

    let sym_parent_dir = TempDir::new()?;
    let real_other = sym_parent_dir.path().join("real-other");
    fs::create_dir(&real_other)?;
    let seg_dir = sym_parent_dir.path().join(SEGMENTS_DIR_NAME);
    fs::create_dir(&seg_dir)?;
    symlink(&real_other, seg_dir.join(SEGMENTS_STAGING_DIR_NAME))?;
    assert!(cleanup_stale_staging_files(sym_parent_dir.path()).is_err());
    Ok(())
}

#[test]
fn snapshot_pins_prefix_ignores_unpinned_supports_exdev_and_detects_tamper() -> TestResult {
    let source = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(source.path(), config, None)?;
    let entry_1 = push_synthetic_range(&mut sealer, 1..=4, true)?.ok_or("entry 1")?;
    let entry_2 = push_synthetic_range(&mut sealer, 5..=8, true)?.ok_or("entry 2")?;
    let entry_3 = push_synthetic_range(&mut sealer, 9..=12, true)?.ok_or("entry 3")?;

    let unsealed_tmp = segment_staging_dir_path(source.path()).join("in-flight.tmp");
    fs::write(&unsealed_tmp, b"unsealed")?;

    let hardlink_snap = TempDir::new()?;
    let pinned = vec![entry_1.clone(), entry_2.clone()];
    let manifest = snapshot_pinned_segments(source.path(), hardlink_snap.path(), &pinned)?;
    assert_eq!(manifest.sealed_tip, Some(entry_2.end_block));
    let verified = verify_pinned_manifest_and_segments(hardlink_snap.path(), false)?;
    assert_eq!(verified, manifest);

    let unpinned_paths = SettledSegmentFilePaths::for_entry(hardlink_snap.path(), &entry_3)?;
    assert!(!unpinned_paths.shielded.exists());
    let snap_inflight = segment_staging_dir_path(hardlink_snap.path()).join("in-flight.tmp");
    assert!(!snap_inflight.exists());
    assert_hardlink_inode_match(source.path(), hardlink_snap.path(), &entry_1)?;

    let copy_snap = TempDir::new()?;
    let copied_manifest = snapshot_pinned_segments_with_linker(
        source.path(),
        copy_snap.path(),
        &pinned,
        |_src, _dst| Err(io::Error::from(io::ErrorKind::CrossesDevices)),
    )?;
    assert_eq!(copied_manifest, manifest);
    verify_pinned_manifest_and_segments(copy_snap.path(), false)?;

    assert_pinned_tamper_and_verity_failures(modules)?;
    Ok(())
}

#[cfg(unix)]
fn assert_hardlink_inode_match(
    source_root: &Path,
    snap_root: &Path,
    entry: &SettledSegmentIndexEntry,
) -> TestResult {
    use std::os::unix::fs::MetadataExt as _;

    let src_paths = SettledSegmentFilePaths::for_entry(source_root, entry)?;
    let dst_paths = SettledSegmentFilePaths::for_entry(snap_root, entry)?;
    assert_eq!(
        fs::metadata(&src_paths.shielded)?.ino(),
        fs::metadata(&dst_paths.shielded)?.ino()
    );
    Ok(())
}

#[cfg(not(unix))]
fn assert_hardlink_inode_match(
    _source_root: &Path,
    _snap_root: &Path,
    _entry: &SettledSegmentIndexEntry,
) -> TestResult {
    Ok(())
}

fn assert_pinned_tamper_and_verity_failures(modules: SettledSegmentModules) -> TestResult {
    let target_kinds = [
        SettledSegmentKind::Shielded,
        SettledSegmentKind::Supplement,
        SettledSegmentKind::RawBlock,
    ];
    for kind in target_kinds {
        let base = TempDir::new()?;
        let source = TempDir::new()?;
        let config =
            SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
                .with_segment_blocks(4);
        let mut sealer = SettledSegmentSealer::new(source.path(), config, None)?;
        let e1 = push_synthetic_range(&mut sealer, 1..=4, true)?.ok_or("e1")?;
        snapshot_pinned_segments_with_linker(
            source.path(),
            base.path(),
            std::slice::from_ref(&e1),
            |_src, _dst| Err(io::Error::from(io::ErrorKind::CrossesDevices)),
        )?;
        let paths = SettledSegmentFilePaths::for_entry(base.path(), &e1)?;
        let victim = match kind {
            SettledSegmentKind::Shielded => paths.shielded,
            SettledSegmentKind::Supplement => paths.supplement.ok_or("missing sup")?,
            SettledSegmentKind::RawBlock => paths.raw_block.ok_or("missing raw")?,
        };
        strip_verity_by_replace(&victim, false)?;
        let single = std::slice::from_ref(&e1);
        assert!(verify_pinned_segments(base.path(), single, true).is_err());

        strip_verity_by_replace(&victim, true)?;
        assert!(verify_pinned_segments(base.path(), single, false).is_err());
    }
    Ok(())
}

#[test]
fn verify_pinned_segments_rejects_mixed_fork_cross_stride_and_cross_module() -> TestResult {
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4);
    let main_dir = TempDir::new()?;
    let mut main_sealer = SettledSegmentSealer::new(main_dir.path(), config, None)?;
    let main_e1 = push_synthetic_range(&mut main_sealer, 1..=4, true)?.ok_or("main e1")?;
    let main_e2 = push_synthetic_range(&mut main_sealer, 5..=8, true)?.ok_or("main e2")?;

    assert_mixed_stride_fork_rejection(main_dir.path(), config, &main_e1)?;
    assert_cross_module_fork_rejection(main_dir.path(), config, &main_e2)?;
    assert_raw_digest_cross_module_rejection(main_dir.path(), config, &main_e2)
}

fn assert_mixed_stride_fork_rejection(
    main_dir: &Path,
    config: SettledSegmentSealerConfig,
    main_e1: &SettledSegmentIndexEntry,
) -> TestResult {
    let fork_dir = TempDir::new()?;
    let mut fork_sealer = SettledSegmentSealer::new(fork_dir.path(), config, None)?;
    for height in 1..=3_u32 {
        assert_eq!(
            fork_sealer.push_block(build_block_input(height, true))?,
            Vec::new()
        );
    }
    let fork_hash4 = BlockHash::from_bytes([0xef; 32]);
    let mut fork_b4 = build_block_input(4, true);
    let parts4 = fork_b4.compact_block.into_parts();
    fork_b4.compact_block = CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(4), fork_hash4),
        parts4.previous_block_hash,
        parts4.time,
        parts4.transactions,
        parts4.chain_metadata,
    )?;
    fork_b4.canonical_facts.block_header.block_hash = fork_hash4;
    let fork_entry1 = expect_single_entry(fork_sealer.push_block(fork_b4)?, "fork e1")?;

    let mut fork_b5 = build_block_input(5, true);
    let parts5 = fork_b5.compact_block.into_parts();
    fork_b5.compact_block = CompactBlockArtifact::new(
        parts5.block_id,
        fork_hash4,
        parts5.time,
        parts5.transactions,
        parts5.chain_metadata,
    )?;
    fork_b5.canonical_facts.block_header.parent_hash = fork_hash4;
    assert_eq!(fork_sealer.push_block(fork_b5)?, Vec::new());
    let fork_entry2 = push_synthetic_range(&mut fork_sealer, 6..=8, true)?.ok_or("fork e2")?;
    assert_ne!(fork_entry1.end_block.hash, main_e1.end_block.hash);

    let mixed_dir = TempDir::new()?;
    copy_entry_segment_files(main_dir, mixed_dir.path(), main_e1)?;
    copy_entry_segment_files(fork_dir.path(), mixed_dir.path(), &fork_entry2)?;
    assert!(matches!(
        verify_pinned_segments(mixed_dir.path(), &[main_e1.clone(), fork_entry2], false),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));
    Ok(())
}

fn assert_cross_module_fork_rejection(
    main_dir: &Path,
    config: SettledSegmentSealerConfig,
    main_e2: &SettledSegmentIndexEntry,
) -> TestResult {
    let alt_dir = TempDir::new()?;
    let mut alt_sealer = SettledSegmentSealer::new(alt_dir.path(), config, None)?;
    let alt_prev = BlockHash::from_bytes([0xaa; 32]);
    let mut alt_b5 = build_block_input(5, true);
    let parts5 = alt_b5.compact_block.into_parts();
    alt_b5.compact_block = CompactBlockArtifact::new(
        parts5.block_id,
        alt_prev,
        parts5.time,
        parts5.transactions,
        parts5.chain_metadata,
    )?;
    alt_b5.canonical_facts.block_header.parent_hash = alt_prev;
    assert_eq!(alt_sealer.push_block(alt_b5)?, Vec::new());
    let alt_entry2 = push_synthetic_range(&mut alt_sealer, 6..=8, true)?.ok_or("alt e2")?;
    assert_eq!(alt_entry2.end_block, main_e2.end_block);

    let sup_swap_dir = TempDir::new()?;
    copy_entry_segment_files(main_dir, sup_swap_dir.path(), main_e2)?;
    let alt_paths = SettledSegmentFilePaths::for_entry(alt_dir.path(), &alt_entry2)?;
    let sup_swap_paths = SettledSegmentFilePaths::for_entry(sup_swap_dir.path(), main_e2)?;
    fs::copy(
        alt_paths.supplement.as_ref().ok_or("alt sup")?,
        sup_swap_paths.supplement.as_ref().ok_or("swap sup")?,
    )?;
    let mut sup_swapped_entry = main_e2.clone();
    sup_swapped_entry.supplement_identity = alt_entry2.supplement_identity;
    assert!(matches!(
        verify_pinned_segments(sup_swap_dir.path(), &[sup_swapped_entry], false),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));

    let raw_swap_dir = TempDir::new()?;
    copy_entry_segment_files(main_dir, raw_swap_dir.path(), main_e2)?;
    let raw_swap_paths = SettledSegmentFilePaths::for_entry(raw_swap_dir.path(), main_e2)?;
    fs::copy(
        alt_paths.raw_block.as_ref().ok_or("alt raw")?,
        raw_swap_paths.raw_block.as_ref().ok_or("swap raw")?,
    )?;
    let mut raw_swapped_entry = main_e2.clone();
    raw_swapped_entry.raw_block_identity = alt_entry2.raw_block_identity;
    assert!(matches!(
        verify_pinned_segments(raw_swap_dir.path(), &[raw_swapped_entry], false),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));
    Ok(())
}

fn assert_raw_digest_cross_module_rejection(
    main_dir: &Path,
    config: SettledSegmentSealerConfig,
    main_e2: &SettledSegmentIndexEntry,
) -> TestResult {
    let raw_alt_dir = TempDir::new()?;
    let mut sealer = SettledSegmentSealer::new(raw_alt_dir.path(), config, None)?;
    let mut block5 = build_block_input(5, true);
    let new_raw_bytes = b"hdr-5-tampered-raw:tx".to_vec();
    let new_raw = LiveTailRawBlock::from_bytes_and_slices(
        new_raw_bytes.clone(),
        &[RawBlockTransactionSlice::new(0, 10)],
    )?;
    block5.canonical_facts.serialized_bytes_digest =
        SerializedBytesDigest::from_serialized_bytes(&new_raw_bytes);
    block5.canonical_facts.transactions.truncate(1);
    block5.canonical_facts.transactions[0].serialized_bytes_digest =
        SerializedBytesDigest::from_serialized_bytes(&new_raw_bytes[..10]);
    let parts5 = block5.compact_block.into_parts();
    block5.compact_block = CompactBlockArtifact::new(
        parts5.block_id,
        parts5.previous_block_hash,
        parts5.time,
        parts5.transactions[..1].to_vec(),
        parts5.chain_metadata,
    )?;
    block5.raw_block = Some(new_raw);
    assert_eq!(sealer.push_block(block5)?, Vec::new());
    let alt_e2 = push_synthetic_range(&mut sealer, 6..=8, true)?.ok_or("raw alt e2")?;

    let swap_dir = TempDir::new()?;
    copy_entry_segment_files(main_dir, swap_dir.path(), main_e2)?;
    let alt_paths = SettledSegmentFilePaths::for_entry(raw_alt_dir.path(), &alt_e2)?;
    let dst_paths = SettledSegmentFilePaths::for_entry(swap_dir.path(), main_e2)?;
    fs::copy(
        alt_paths.raw_block.as_ref().ok_or("alt raw")?,
        dst_paths.raw_block.as_ref().ok_or("dst raw")?,
    )?;
    let mut bad_entry = main_e2.clone();
    bad_entry.raw_block_identity = alt_e2.raw_block_identity;
    assert!(matches!(
        verify_pinned_segments(swap_dir.path(), &[bad_entry], false),
        Err(SettledSegmentsError::Integrity(_))
    ));
    Ok(())
}

fn copy_entry_segment_files(
    src_root: &Path,
    dst_root: &Path,
    entry: &SettledSegmentIndexEntry,
) -> TestResult {
    let src = SettledSegmentFilePaths::for_entry(src_root, entry)?;
    let dst = SettledSegmentFilePaths::for_entry(dst_root, entry)?;
    if let Some(parent) = dst.shielded.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(&src.shielded, &dst.shielded)?;
    if let (Some(src_sup), Some(dst_sup)) = (&src.supplement, &dst.supplement) {
        if let Some(parent) = dst_sup.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(src_sup, dst_sup)?;
    }
    if let (Some(src_raw), Some(dst_raw)) = (&src.raw_block, &dst.raw_block) {
        if let Some(parent) = dst_raw.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(src_raw, dst_raw)?;
    }
    Ok(())
}

fn replace_file_from(src: &Path, dst: &Path) -> TestResult {
    let replaced = dst.with_extension("replaced");
    fs::copy(src, &replaced)?;
    fs::rename(&replaced, dst)?;
    Ok(())
}

fn strip_verity_by_replace(path: &Path, flip_last_byte: bool) -> TestResult {
    let mut bytes = fs::read(path)?;
    if flip_last_byte {
        let last = bytes.last_mut().ok_or("empty segment file")?;
        *last ^= 0xff;
    }
    let replaced = path.with_extension("replaced");
    fs::write(&replaced, &bytes)?;
    fs::rename(&replaced, path)?;
    Ok(())
}

#[test]
fn reader_cache_enforces_lru_eviction_and_identity_checks() -> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let entry_1 = push_synthetic_range(&mut sealer, 1..=4, true)?.ok_or("entry 1")?;
    let entry_2 = push_synthetic_range(&mut sealer, 5..=8, true)?.ok_or("entry 2")?;
    let entry_3 = push_synthetic_range(&mut sealer, 9..=12, true)?.ok_or("entry 3")?;

    assert!(SettledSegmentReaderCache::new(dir.path(), 0, false).is_err());
    let cache = SettledSegmentReaderCache::new(dir.path(), 2, false)?;
    assert_eq!(cache.capacity(), 2);
    assert!(cache.is_empty());

    let r1_first = cache.get_or_open(&entry_1)?;
    let _r2_first = cache.get_or_open(&entry_2)?;
    assert_eq!(cache.len(), 2);

    let r1_hit = cache.get_or_open(&entry_1)?;
    assert!(Arc::ptr_eq(&r1_first.shielded, &r1_hit.shielded));

    let _r3 = cache.get_or_open(&entry_3)?;
    assert_eq!(cache.len(), 2);
    assert!(cache.contains_start_height(entry_1.start_height));
    assert!(cache.contains_start_height(entry_3.start_height));
    assert!(!cache.contains_start_height(entry_2.start_height));

    assert_cache_identity_rejections(&cache, &entry_1);

    let r2_reopened = cache.get_or_open(&entry_2)?;
    assert!(!cache.contains_start_height(entry_1.start_height));
    let raw_reader = r2_reopened.raw_block.as_ref().ok_or("missing raw reader")?;
    assert_eq!(
        raw_reader.read_raw_block(BlockHeight::new(6))?,
        build_block_fixture(6).raw_block.raw_block_bytes
    );

    assert!(cache.evict(entry_2.start_height));
    assert!(!cache.evict(entry_2.start_height));
    cache.clear();
    assert!(cache.is_empty());
    Ok(())
}

fn assert_cache_identity_rejections(
    cache: &SettledSegmentReaderCache,
    entry_1: &SettledSegmentIndexEntry,
) {
    let mut wrong_sha = entry_1.clone();
    wrong_sha.shielded_identity.sha256[0] ^= 0x01;
    assert!(cache.get_or_open(&wrong_sha).is_err());

    let mut wrong_modules = entry_1.clone();
    wrong_modules.modules.supplement = false;
    wrong_modules.supplement_identity = None;
    assert!(cache.get_or_open(&wrong_modules).is_err());

    let mut wrong_end = entry_1.clone();
    wrong_end.end_block.hash = BlockHash::from_bytes([0xde; 32]);
    wrong_end.shielded_identity.end_block = wrong_end.end_block;
    assert!(cache.get_or_open(&wrong_end).is_err());
}

struct SyntheticBlockFixture {
    compact_block: CompactBlockArtifact,
    canonical_facts: CanonicalBlockFacts,
    raw_block: LiveTailRawBlock,
    tx0_raw: Vec<u8>,
}

fn seal_test_stride(
    store_root: &Path,
    heights: std::ops::RangeInclusive<u32>,
    stride: u32,
    modules: SettledSegmentModules,
    prior: Option<CanonicalSequenceCheckpoint>,
) -> Result<SettledSegmentIndexEntry, SettledSegmentsError> {
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(stride)
            .with_supplement_options(SettledSupplementSegmentWriteOptions::default())
            .with_raw_block_codec(RawBlockCodec::Zstd);
    let mut sealer = SettledSegmentSealer::new(store_root, config, prior)?;
    push_synthetic_range(&mut sealer, heights, modules.raw_bodies)?
        .ok_or_else(|| SettledSegmentsError::ChainLinkage("stride did not seal".to_owned()))
}

fn expect_single_entry(
    entries: Vec<SettledSegmentIndexEntry>,
    message: &str,
) -> Result<SettledSegmentIndexEntry, Box<dyn std::error::Error>> {
    let [entry]: [SettledSegmentIndexEntry; 1] =
        entries.try_into().map_err(|_| message.to_owned())?;
    Ok(entry)
}

fn push_synthetic_range(
    sealer: &mut SettledSegmentSealer,
    heights: std::ops::RangeInclusive<u32>,
    include_raw: bool,
) -> Result<Option<SettledSegmentIndexEntry>, SettledSegmentsError> {
    let mut tail_entry = None;
    for height in heights {
        let input = build_block_input(height, include_raw);
        let emitted_entries = sealer.push_block(input)?;
        if !emitted_entries.is_empty() {
            tail_entry = emitted_entries.into_iter().last();
        }
    }
    Ok(tail_entry)
}

fn build_block_input(height: u32, include_raw: bool) -> SettledSegmentBlockInput {
    let fixture = build_block_fixture(height);
    SettledSegmentBlockInput {
        compact_block: fixture.compact_block,
        native_bytes: 1_024 + usize::try_from(height).unwrap_or(0),
        canonical_facts: fixture.canonical_facts,
        raw_block: include_raw.then_some(fixture.raw_block),
    }
}

fn deterministic_hash(tag: u8, height: u32) -> [u8; 32] {
    let mut out = [0_u8; 32];
    out[0] = tag;
    out[1..5].copy_from_slice(&height.to_be_bytes());
    out
}

fn build_block_fixture(height: u32) -> SyntheticBlockFixture {
    let block_hash = BlockHash::from_bytes(deterministic_hash(0xa1, height));
    let prev_hash = if height == 0 {
        BlockHash::from_bytes([0_u8; 32])
    } else {
        BlockHash::from_bytes(deterministic_hash(0xa1, height - 1))
    };
    let tx0_id = TransactionId::from_bytes(deterministic_hash(0xb0, height));
    let tx1_id = TransactionId::from_bytes(deterministic_hash(0xb1, height));
    let (raw_bytes, raw_block, tx0_raw, tx1_raw) = build_raw_block_and_slices(height);

    let block_time = 1_700_000_000_u32 + height;
    let compact_txs = build_compact_txs(height, tx0_id, tx1_id);
    let metadata = CompactChainMetadata {
        sapling_commitment_tree_size: 10 + height,
        orchard_commitment_tree_size: 20 + height,
        ironwood_commitment_tree_size: 30 + height,
    };
    let block_id = BlockId::new(BlockHeight::new(height), block_hash);
    let compact_block =
        CompactBlockArtifact::new(block_id, prev_hash, block_time, compact_txs, metadata)
            .unwrap_or_else(|_| {
                CompactBlockArtifact::empty(block_id, prev_hash, block_time, metadata)
            });

    let tx_facts = vec![
        build_tx_facts(tx0_id, &tx0_raw, height, true),
        build_tx_facts(tx1_id, &tx1_raw, height, false),
    ];
    let canonical_facts = CanonicalBlockFacts {
        block_header: BlockHeaderArtifact::new(
            BlockHeight::new(height),
            block_hash,
            prev_hash,
            [0x33; 32],
            [0x44; 32],
            i64::from(block_time),
            0x1f07_ffff,
            [0x55; 32],
            4,
            u64::try_from(raw_bytes.len()).unwrap_or(0),
        ),
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&raw_bytes),
        transactions: tx_facts,
    };
    SyntheticBlockFixture {
        compact_block,
        canonical_facts,
        raw_block,
        tx0_raw,
    }
}

fn build_raw_block_and_slices(height: u32) -> (Vec<u8>, LiveTailRawBlock, Vec<u8>, Vec<u8>) {
    let tx0_raw = format!("raw-tx0-height-{height}").into_bytes();
    let tx1_raw = format!("raw-tx1-height-{height}").into_bytes();
    let mut raw_bytes = format!("hdr-{height}:").into_bytes();
    let tx0_off = u32::try_from(raw_bytes.len()).unwrap_or(0);
    let tx0_len = u32::try_from(tx0_raw.len()).unwrap_or(0);
    raw_bytes.extend_from_slice(&tx0_raw);
    let tx1_off = u32::try_from(raw_bytes.len()).unwrap_or(0);
    let tx1_len = u32::try_from(tx1_raw.len()).unwrap_or(0);
    raw_bytes.extend_from_slice(&tx1_raw);

    let slices = [
        RawBlockTransactionSlice::new(tx0_off, tx0_len),
        RawBlockTransactionSlice::new(tx1_off, tx1_len),
    ];
    let raw_block = LiveTailRawBlock::from_bytes_and_slices(raw_bytes.clone(), &slices)
        .unwrap_or_else(|_| LiveTailRawBlock {
            raw_block_bytes: Vec::new(),
            tx_slices: Vec::new(),
        });
    (raw_bytes, raw_block, tx0_raw, tx1_raw)
}

fn build_compact_txs(
    height: u32,
    tx0_id: TransactionId,
    tx1_id: TransactionId,
) -> Vec<CompactTransaction> {
    let seed = u8::try_from(height % 200).unwrap_or(1);
    vec![
        CompactTransaction {
            index: 0,
            transaction_id: tx0_id,
            data: CompactTransactionData {
                fee_zat: Some(1_000),
                sapling_spends: vec![CompactSaplingSpend {
                    nullifier: [seed; 32],
                }],
                sapling_outputs: vec![CompactSaplingOutput {
                    commitment: [seed + 1; 32],
                    ephemeral_key: [seed + 2; 32],
                    ciphertext: [seed + 3; 52],
                }],
                orchard_actions: vec![CompactShieldedAction {
                    nullifier: [seed + 4; 32],
                    commitment: [seed + 5; 32],
                    ephemeral_key: [seed + 6; 32],
                    ciphertext: [seed + 7; 52],
                }],
                ironwood_actions: vec![CompactShieldedAction {
                    nullifier: [seed + 8; 32],
                    commitment: [seed + 9; 32],
                    ephemeral_key: [seed + 10; 32],
                    ciphertext: [seed + 11; 52],
                }],
                transparent_inputs: vec![CompactTransparentInput {
                    previous_transaction_id: TransactionId::from_bytes([0x99; 32]),
                    previous_output_index: 1,
                }],
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 50_000,
                    script_pub_key: vec![0x76, 0xa9, 0x14],
                }],
            },
        },
        CompactTransaction {
            index: 1,
            transaction_id: tx1_id,
            data: CompactTransactionData {
                fee_zat: None,
                sapling_spends: Vec::new(),
                sapling_outputs: Vec::new(),
                orchard_actions: Vec::new(),
                ironwood_actions: Vec::new(),
                transparent_inputs: Vec::new(),
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 25_000,
                    script_pub_key: vec![0x51],
                }],
            },
        },
    ]
}

fn build_tx_facts(
    txid: TransactionId,
    raw_tx: &[u8],
    height: u32,
    shielded: bool,
) -> CanonicalTransactionFacts {
    let counts = if shielded {
        TransactionComponentCounts {
            transparent_input_count: 1,
            transparent_output_count: 1,
            sapling_spend_count: 1,
            sapling_output_count: 1,
            orchard_action_count: 1,
            ironwood_action_count: 1,
            sprout_joinsplit_count: 0,
        }
    } else {
        TransactionComponentCounts {
            transparent_input_count: 0,
            transparent_output_count: 1,
            sapling_spend_count: 0,
            sapling_output_count: 0,
            orchard_action_count: 0,
            ironwood_action_count: 0,
            sprout_joinsplit_count: 0,
        }
    };
    let anchor = shielded.then_some([0x64; 32]);
    let tx_size = u32::try_from(raw_tx.len()).unwrap_or(0);
    CanonicalTransactionFacts {
        public_facts: TransactionPublicFacts {
            transaction_id: txid,
            auth_digest: Some(AuthDigest::from_bytes([0x62; 32])),
            wtxid: Some(Wtxid::from_bytes([0x63; 64])),
            version: TransactionVersion::V6,
            consensus_branch_id: Some(ConsensusBranchId::new(0x1234_5678)),
            lock_time: LockTime::Height(BlockHeight::new(height)),
            expiry_height: Some(BlockHeight::new(height + 20)),
            size_bytes: tx_size,
            counts,
            orchard_value_balance_zat: shielded.then_some(-10),
            orchard_anchor: anchor,
            ironwood_value_balance_zat: shielded.then_some(0),
            privacy_shape: if shielded {
                PrivacyShape::Mixed
            } else {
                PrivacyShape::Coinbase
            },
            is_coinbase: !shielded,
            unsupported_sections: if shielded {
                vec![UnsupportedSection::FutureVersionHeader]
            } else {
                Vec::new()
            },
        },
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(raw_tx),
        intrinsic_value_balances: TransactionIntrinsicValueBalances::new(0, 0, -10, 0),
        transparent_inputs: if shielded {
            vec![TransparentInputFact::new(
                0,
                TransparentOutPoint::new(TransactionId::from_bytes([0x99; 32]), 1),
            )]
        } else {
            Vec::new()
        },
        transparent_outputs: vec![TransparentOutputFact::new(
            0,
            if shielded { 50_000 } else { 25_000 },
            vec![0x76, 0xa9],
            TransparentAddressScriptHash::from_bytes([0x72; 32]),
        )],
    }
}

#[test]
fn sealer_rolls_over_before_raw_slab_limit_under_uncompressed_codec() -> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let sample_raw_len = build_block_input(1, true)
        .raw_block
        .as_ref()
        .ok_or("missing raw block fixture")?
        .raw_block_bytes
        .len();
    let per_block_frame = u64::try_from(RAW_FRAME_PREFIX_LEN + sample_raw_len)?;
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(6)
            .with_raw_block_codec(RawBlockCodec::None)
            .with_max_raw_segment_bytes(
                u64::try_from(
                    RAW_HEADER_LEN
                        + RAW_TRAILER_LEN
                        + 2 * RAW_ROW_INDEX_ENTRY_LEN
                        + 4 * RAW_TX_SLICE_ENTRY_LEN,
                )? + per_block_frame * 2
                    + 4,
            );
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;

    let mut sealed_entries = Vec::new();
    for height in 1..=6_u32 {
        sealed_entries.extend(sealer.push_block(build_block_input(height, true))?);
    }
    assert_eq!(sealed_entries.len(), 3);
    assert_eq!(
        sealed_entries
            .iter()
            .map(|entry| (entry.start_height.value(), entry.end_block.height.value()))
            .collect::<Vec<_>>(),
        vec![(1, 2), (3, 4), (5, 6)]
    );
    assert_eq!(sealer.pending_len(), 0);
    verify_pinned_segments(dir.path(), &sealed_entries, false)?;
    Ok(())
}

fn build_componentless_and_wide_fee_input(
    wide_fee: u64,
) -> Result<SettledSegmentBlockInput, Box<dyn std::error::Error>> {
    let mut block_input = build_block_input(1, true);
    let parts = block_input.compact_block.into_parts();
    let tx2_id = TransactionId::from_bytes(deterministic_hash(0xb2, 1));
    let mut compact_txs = parts.transactions;
    compact_txs[0].data.fee_zat = Some(wide_fee);
    compact_txs.push(CompactTransaction {
        index: 2,
        transaction_id: tx2_id,
        data: CompactTransactionData {
            fee_zat: None,
            sapling_spends: Vec::new(),
            sapling_outputs: Vec::new(),
            orchard_actions: Vec::new(),
            ironwood_actions: Vec::new(),
            transparent_inputs: Vec::new(),
            transparent_outputs: Vec::new(),
        },
    });
    block_input.compact_block = CompactBlockArtifact::new(
        parts.block_id,
        parts.previous_block_hash,
        parts.time,
        compact_txs,
        parts.chain_metadata,
    )?;

    let tx2_raw = b"raw-tx2-componentless".to_vec();
    let mut raw_bytes = block_input
        .raw_block
        .as_ref()
        .ok_or("missing raw block")?
        .raw_block_bytes
        .clone();
    let mut tx_slices = block_input
        .raw_block
        .as_ref()
        .ok_or("missing raw slices")?
        .raw_slices();
    let tx2_off = u32::try_from(raw_bytes.len())?;
    let tx2_len = u32::try_from(tx2_raw.len())?;
    raw_bytes.extend_from_slice(&tx2_raw);
    tx_slices.push(RawBlockTransactionSlice::new(tx2_off, tx2_len));
    block_input.raw_block = Some(LiveTailRawBlock::from_bytes_and_slices(
        raw_bytes.clone(),
        &tx_slices,
    )?);
    block_input.canonical_facts.serialized_bytes_digest =
        SerializedBytesDigest::from_serialized_bytes(&raw_bytes);
    let mut tx2_facts = build_tx_facts(tx2_id, &tx2_raw, 1, false);
    tx2_facts.public_facts.counts.transparent_output_count = 0;
    tx2_facts.transparent_outputs.clear();
    tx2_facts.public_facts.is_coinbase = false;
    block_input.canonical_facts.transactions.push(tx2_facts);
    Ok(block_input)
}

#[test]
fn sealer_preserves_all_componentless_compact_transactions_with_supplement() -> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let wide_fee = u64::from(u32::MAX) + 77;
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(1);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let block_input = build_componentless_and_wide_fee_input(wide_fee)?;
    let expected_block = block_input.compact_block.clone();
    let entry = expect_single_entry(
        sealer.push_block(block_input)?,
        "segment size 1 should seal immediately",
    )?;
    verify_pinned_segments(dir.path(), std::slice::from_ref(&entry), false)?;

    let paths = SettledSegmentFilePaths::for_entry(dir.path(), &entry)?;
    let shielded_reader = SettledShieldedSegmentReader::open(
        &paths.shielded,
        &entry.shielded_identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let wire_blocks = shielded_reader.read_default_range_for_reconstruction(
        BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1)),
        HeightOrder::Ascending,
    )?;
    let sup_reader = SettledSupplementSegmentReader::open(
        paths.supplement.as_deref().ok_or("missing supplement")?,
        entry.supplement_identity.as_ref().ok_or("missing sup id")?,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    let reconstructed =
        sup_reader.reconstruct_block(BlockHeight::new(1), &wire_blocks[0].payload())?;
    assert_eq!(reconstructed.transactions().len(), 3);
    assert_eq!(reconstructed.transactions()[0].data.fee_zat, Some(wide_fee));
    assert_eq!(
        reconstructed.transactions()[2],
        expected_block.transactions()[2]
    );

    let mut tail_record = LiveTailBlockRecord::from_compact_block(&expected_block, true, None)?;
    assert!(
        tail_record
            .splice_compact_block(CompactBlockSpliceSelection::default())
            .is_err()
    );
    let sup = tail_record.supplement.as_mut().ok_or("missing tail sup")?;
    sup.transparent_transactions[0].transparent_inputs.clear();
    sup.transparent_transactions[0].transparent_outputs.clear();
    sup.transparent_transactions[0].fee_zat = None;
    assert!(tail_record.validate().is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn verified_dir_blocks_symlink_swap_outside_store() -> TestResult {
    let main_dir = TempDir::new()?;
    let ext_dir = TempDir::new()?;
    let snap_dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let entry = seal_test_stride(main_dir.path(), 1..=4, 4, modules, None)?;
    copy_entry_segment_files(main_dir.path(), ext_dir.path(), &entry)?;

    let main_paths = SettledSegmentFilePaths::for_entry(main_dir.path(), &entry)?;
    let ext_paths = SettledSegmentFilePaths::for_entry(ext_dir.path(), &entry)?;
    fs::remove_file(&main_paths.shielded)?;
    std::os::unix::fs::symlink(&ext_paths.shielded, &main_paths.shielded)?;

    assert!(verify_pinned_segments(main_dir.path(), std::slice::from_ref(&entry), false).is_err());
    assert!(
        snapshot_pinned_segments(
            main_dir.path(),
            snap_dir.path(),
            std::slice::from_ref(&entry)
        )
        .is_err()
    );
    let cache = SettledSegmentReaderCache::new(main_dir.path(), 4, false)?;
    assert!(cache.get_or_open(&entry).is_err());

    fs::remove_file(&main_paths.shielded)?;
    fs::copy(&ext_paths.shielded, &main_paths.shielded)?;
    let main_shielded_dir = segment_dir_path(main_dir.path(), SettledSegmentKind::Shielded);
    let ext_shielded_dir = segment_dir_path(ext_dir.path(), SettledSegmentKind::Shielded);
    fs::remove_dir_all(&main_shielded_dir)?;
    std::os::unix::fs::symlink(&ext_shielded_dir, &main_shielded_dir)?;
    assert!(verify_pinned_segments(main_dir.path(), &[entry], false).is_err());
    Ok(())
}

#[test]
fn snapshot_rejects_interior_block_raw_digest_mismatch() -> TestResult {
    let main_dir = TempDir::new()?;
    let alt_dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let main_entry = seal_test_stride(main_dir.path(), 1..=4, 4, modules, None)?;
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4)
            .with_raw_block_codec(RawBlockCodec::Zstd);
    let mut alt_sealer = SettledSegmentSealer::new(alt_dir.path(), config, None)?;
    assert_eq!(
        alt_sealer.push_block(build_block_input(1, true))?,
        Vec::new()
    );

    let mut alt_b2 = build_block_input(2, true);
    let modified_raw = b"hdr-2:interior-tx0interior-tx1".to_vec();
    let alt_slices = [
        RawBlockTransactionSlice::new(6, 12),
        RawBlockTransactionSlice::new(18, 12),
    ];
    alt_b2.raw_block = Some(LiveTailRawBlock::from_bytes_and_slices(
        modified_raw.clone(),
        &alt_slices,
    )?);
    alt_b2.canonical_facts.serialized_bytes_digest =
        SerializedBytesDigest::from_serialized_bytes(&modified_raw);
    alt_b2.canonical_facts.transactions[0].serialized_bytes_digest =
        SerializedBytesDigest::from_serialized_bytes(&modified_raw[6..18]);
    alt_b2.canonical_facts.transactions[1].serialized_bytes_digest =
        SerializedBytesDigest::from_serialized_bytes(&modified_raw[18..30]);
    assert_eq!(alt_sealer.push_block(alt_b2)?, Vec::new());
    assert_eq!(
        alt_sealer.push_block(build_block_input(3, true))?,
        Vec::new()
    );
    let alt_entry = expect_single_entry(
        alt_sealer.push_block(build_block_input(4, true))?,
        "alt stride should seal at height 4",
    )?;

    let main_paths = SettledSegmentFilePaths::for_entry(main_dir.path(), &main_entry)?;
    let alt_paths = SettledSegmentFilePaths::for_entry(alt_dir.path(), &alt_entry)?;
    replace_file_from(
        alt_paths.raw_block.as_ref().ok_or("missing alt raw")?,
        main_paths.raw_block.as_ref().ok_or("missing main raw")?,
    )?;
    let mut swapped_entry = main_entry;
    swapped_entry.raw_block_identity = alt_entry.raw_block_identity;
    assert!(matches!(
        verify_pinned_segments(main_dir.path(), &[swapped_entry], false),
        Err(SettledSegmentsError::Integrity(_))
    ));
    Ok(())
}

#[test]
fn sealer_rolls_over_before_shielded_slab_and_raw_tx_caps() -> TestResult {
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let sample_shielded =
        encode_default_shielded_payload(&build_block_input(1, true).compact_block)?;
    let shielded_frame_len = u64::try_from(
        crate::settled_shielded_segment::SHIELDED_FRAME_PREFIX_LEN + sample_shielded.0.len(),
    )?;
    let shielded_dir = TempDir::new()?;
    let shielded_cfg =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4)
            .with_max_shielded_segment_bytes(
                u64::try_from(
                    SHIELDED_HEADER_LEN + SHIELDED_TRAILER_LEN + 2 * SHIELDED_ROW_INDEX_ENTRY_LEN,
                )? + shielded_frame_len * 2
                    + 8,
            );
    let mut shielded_sealer = SettledSegmentSealer::new(shielded_dir.path(), shielded_cfg, None)?;
    let mut shielded_entries = Vec::new();
    for height in 1..=4_u32 {
        shielded_entries.extend(shielded_sealer.push_block(build_block_input(height, true))?);
    }
    assert_eq!(
        shielded_entries
            .iter()
            .map(|entry| (entry.start_height.value(), entry.end_block.height.value()))
            .collect::<Vec<_>>(),
        vec![(1, 2), (3, 4)]
    );
    verify_pinned_segments(shielded_dir.path(), &shielded_entries, false)?;

    let tx_cap_dir = TempDir::new()?;
    let tx_cap_cfg =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4)
            .with_max_raw_tx_entries(3);
    let mut tx_cap_sealer = SettledSegmentSealer::new(tx_cap_dir.path(), tx_cap_cfg, None)?;
    let mut tx_cap_entries = Vec::new();
    for height in 1..=4_u32 {
        tx_cap_entries.extend(tx_cap_sealer.push_block(build_block_input(height, true))?);
    }
    assert_eq!(
        tx_cap_entries
            .iter()
            .map(|entry| (entry.start_height.value(), entry.end_block.height.value()))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2), (3, 3), (4, 4)]
    );
    verify_pinned_segments(tx_cap_dir.path(), &tx_cap_entries, false)?;
    Ok(())
}

#[test]
fn encode_default_shielded_payload_validates_omitted_wide_fees_in_strict_mode() -> TestResult {
    let wide_fee = Some(u64::from(u32::MAX) + 1);
    let metadata = CompactChainMetadata {
        sapling_commitment_tree_size: 0,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    let cases = [
        (
            21_u32,
            CompactTransactionData {
                fee_zat: wide_fee,
                transparent_outputs: vec![CompactTransparentOutput {
                    value_zat: 1_000,
                    script_pub_key: vec![0x51],
                }],
                ..CompactTransactionData::default()
            },
        ),
        (
            22_u32,
            CompactTransactionData {
                fee_zat: wide_fee,
                ..CompactTransactionData::default()
            },
        ),
    ];
    for (height, tx_scan) in cases {
        let block = CompactBlockArtifact::new(
            BlockId::new(BlockHeight::new(height), BlockHash::from_bytes([0x41; 32])),
            BlockHash::from_bytes([0x40; 32]),
            1_700_000_000 + height,
            vec![CompactTransaction {
                index: 0,
                transaction_id: TransactionId::from_bytes([0x51; 32]),
                data: tx_scan,
            }],
            metadata,
        )?;
        assert!(matches!(
            encode_default_shielded_payload(&block),
            Err(SettledSegmentsError::InvalidFormat(_))
        ));
        let (payload, presence) = encode_default_shielded_payload_with_options(&block, true)?;
        assert_eq!(presence, PoolPresenceMask::EMPTY);
        assert_ne!(payload.len(), 0);
    }
    Ok(())
}

#[test]
fn manifest_writer_rejects_serialized_manifest_exceeding_byte_limit() -> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: false,
        raw_bodies: false,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let entry = push_synthetic_range(&mut sealer, 1..=4, false)?.ok_or("expected entry")?;
    let manifest = CanonicalSegmentsManifest::from_entries(Network::ZcashRegtest, vec![entry])?;

    write_segments_manifest_atomic(dir.path(), &manifest)?;
    let manifest_path = dir.path().join(SEGMENTS_MANIFEST_FILE_NAME);
    let exact_len = usize::try_from(fs::metadata(&manifest_path)?.len())?;
    let root_dir = VerifiedDir::open_existing(dir.path(), &[])?;

    assert!(matches!(
        write_segments_manifest_atomic_in_dir_with_limit(&root_dir, &manifest, exact_len - 1),
        Err(SettledSegmentsError::InvalidFormat(_))
    ));
    assert_eq!(read_segments_manifest(dir.path())?, manifest);

    write_segments_manifest_atomic_in_dir_with_limit(&root_dir, &manifest, exact_len)?;
    assert_eq!(read_segments_manifest(dir.path())?, manifest);
    Ok(())
}

#[cfg(unix)]
#[test]
fn snapshot_manifest_publication_uses_pinned_target_dir_across_ancestor_swap() -> TestResult {
    use std::os::unix::fs::symlink;

    let root = TempDir::new()?;
    let source = root.path().join("source");
    let real_parent = root.path().join("real-parent");
    let moved_parent = root.path().join("moved-parent");
    let evil_parent = root.path().join("evil-parent");
    let target_snap = real_parent.join("snap");
    let evil_snap = evil_parent.join("snap");
    fs::create_dir_all(&source)?;
    fs::create_dir_all(&target_snap)?;
    fs::create_dir_all(&evil_snap)?;
    let evil_manifest = evil_snap.join(SEGMENTS_MANIFEST_FILE_NAME);
    fs::write(&evil_manifest, b"sentinel")?;

    let modules = SettledSegmentModules {
        supplement: false,
        raw_bodies: false,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(&source, config, None)?;
    let entry = push_synthetic_range(&mut sealer, 1..=4, false)?.ok_or("expected entry")?;

    let mut swapped = false;
    let manifest = snapshot_pinned_segments_with_linker(
        &source,
        &target_snap,
        std::slice::from_ref(&entry),
        |src, dst| {
            fs::hard_link(src, dst)?;
            if !swapped {
                swapped = true;
                fs::rename(&real_parent, &moved_parent)?;
                symlink(&evil_parent, &real_parent)?;
            }
            Ok(())
        },
    )?;

    assert_eq!(fs::read(&evil_manifest)?, b"sentinel");
    assert_eq!(
        read_segments_manifest(&moved_parent.join("snap"))?,
        manifest
    );
    Ok(())
}

#[test]
fn snapshot_rejects_raw_slice_table_tamper_with_unchanged_block_bytes() -> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let mut entry = push_synthetic_range(&mut sealer, 1..=4, true)?.ok_or("expected entry")?;
    let orig_raw_id = entry.raw_block_identity.ok_or("missing raw identity")?;
    let paths = SettledSegmentFilePaths::for_entry(dir.path(), &entry)?;
    let raw_path = paths.raw_block.ok_or("missing raw path")?;

    let span = RawBlockSegmentIdentity::span(
        orig_raw_id.network,
        orig_raw_id.codec,
        orig_raw_id.start_height,
        orig_raw_id.end_block,
    );
    let tampered_path = dir.path().join("tampered.zsblkb01");
    let mut writer = SettledRawBlockSegmentWriter::create(
        &tampered_path,
        NamedTempFile::new_in(dir.path())?,
        span,
    )?;
    for height in 1..=4_u32 {
        let block = build_block_input(height, true);
        let raw_block = block.raw_block.ok_or("missing raw block")?;
        let mut slices: Vec<RawBlockTransactionSlice> = raw_block
            .tx_slices
            .iter()
            .map(|slice| RawBlockTransactionSlice::new(slice.offset, slice.len))
            .collect();
        if height == 2 {
            slices[0].len -= 1;
        }
        writer.push(
            BlockId::new(
                BlockHeight::new(height),
                block.canonical_facts.block_header.block_hash,
            ),
            block.canonical_facts.block_header.parent_hash,
            &raw_block.raw_block_bytes,
            &slices,
        )?;
    }
    let tampered_raw_id = writer.finish()?;
    fs::rename(&tampered_path, &raw_path)?;
    let reader = SettledRawBlockSegmentReader::open(
        &raw_path,
        &tampered_raw_id,
        SettledRawBlockSegmentOpenOptions::default(),
    )?;
    assert_eq!(
        reader.uncompressed_digest_at(BlockHeight::new(2))?,
        build_block_input(2, true)
            .canonical_facts
            .serialized_bytes_digest
    );

    entry.raw_block_identity = Some(tampered_raw_id);
    assert!(matches!(
        verify_pinned_segments(dir.path(), &[entry], false),
        Err(SettledSegmentsError::Integrity(_))
    ));
    Ok(())
}

#[test]
fn sealer_rejects_oversized_single_block_spool_before_rollover_seal() -> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let b1 = build_block_input(1, true);
    let rec1 = LiveTailBlockRecord::from_compact_block(
        &b1.compact_block,
        true,
        Some(b1.canonical_facts.clone()),
    )?;
    let encoded_b1 = rec1.encode_v1()?;
    let encoded_b1_raw = b1
        .raw_block
        .as_ref()
        .ok_or("missing raw block")?
        .encode_v1()?;
    let b1_spool_bytes = u64::try_from(16 + encoded_b1.len() + encoded_b1_raw.len())?;
    let max_spool_bytes = b1_spool_bytes * 3;
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(2)
            .with_max_spool_bytes(max_spool_bytes);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    assert_eq!(sealer.push_block(b1)?, Vec::new());
    assert_eq!(sealer.pending_len(), 1);

    let mut oversized_b2 = build_block_input(2, true);
    let raw2 = oversized_b2
        .raw_block
        .as_ref()
        .ok_or("expected full raw block")?;
    let padding_len = usize::try_from(max_spool_bytes)? + 512;
    let mut padded_bytes = raw2.raw_block_bytes.clone();
    padded_bytes.extend(vec![0xab; padding_len]);
    let raw2_slices: Vec<_> = raw2
        .tx_slices
        .iter()
        .copied()
        .map(LiveTailRawTxSlice::as_raw_slice)
        .collect();
    oversized_b2.raw_block = Some(LiveTailRawBlock::from_bytes_and_slices(
        padded_bytes.clone(),
        &raw2_slices,
    )?);
    oversized_b2.canonical_facts.serialized_bytes_digest =
        SerializedBytesDigest::from_serialized_bytes(&padded_bytes);
    assert!(matches!(
        sealer.push_block(oversized_b2),
        Err(SettledSegmentsError::InvalidFormat(_))
    ));
    assert_eq!(sealer.pending_len(), 1);
    assert!(sealer.prior_checkpoint().is_none());
    assert!(!sealer.is_poisoned());
    assert_eq!(
        fs::read_dir(segment_dir_path(dir.path(), SettledSegmentKind::Shielded))?.count(),
        0
    );

    let entries = sealer.push_block(build_block_input(2, true))?;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].start_height.value(), 1);
    assert_eq!(entries[0].end_block.height.value(), 2);
    assert_eq!(sealer.pending_len(), 0);
    assert!(!sealer.is_poisoned());
    verify_pinned_segments(dir.path(), &entries, false)?;
    Ok(())
}

#[test]
fn sealer_poisons_handle_when_append_fails_after_rollover_seal() -> TestResult {
    let dir = TempDir::new()?;
    let b1 = build_block_fixture(1);
    let rec1 = LiveTailBlockRecord::from_compact_block(&b1.compact_block, false, None)?;
    let shielded_frame_len = u64::try_from(5 + rec1.shielded_payload.len())?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: false,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4)
            .with_max_shielded_segment_bytes(
                u64::try_from(
                    SHIELDED_HEADER_LEN + SHIELDED_TRAILER_LEN + SHIELDED_ROW_INDEX_ENTRY_LEN,
                )? + shielded_frame_len
                    + 8,
            );
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    assert_eq!(sealer.push_block(build_block_input(1, false))?, Vec::new());
    assert_eq!(sealer.pending_len(), 1);

    assert!(matches!(
        sealer.push_block_with_injected_append_failure(build_block_input(2, false)),
        Err(SettledSegmentsError::Io(_))
    ));
    assert_eq!(
        fs::read_dir(segment_dir_path(dir.path(), SettledSegmentKind::Shielded))?.count(),
        1
    );
    assert_eq!(
        fs::read_dir(segment_staging_dir_path(dir.path()))?.count(),
        0
    );
    assert!(sealer.prior_checkpoint().is_some());
    assert_eq!(sealer.pending_len(), 0);
    assert!(sealer.is_poisoned());

    assert!(matches!(
        sealer.push_block(build_block_input(2, false)),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));
    assert!(matches!(
        sealer.push_block(build_block_input(3, false)),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));

    let mut recovered = SettledSegmentSealer::new(dir.path(), config, None)?;
    let mut recovered_entries = Vec::new();
    for height in 1..=4_u32 {
        recovered_entries.extend(recovered.push_block(build_block_input(height, false))?);
    }
    assert_eq!(recovered.pending_len(), 0);
    assert_eq!(recovered_entries[0].start_height.value(), 1);
    assert_eq!(recovered_entries[0].end_block.height.value(), 1);
    assert_eq!(recovered_entries[1].start_height.value(), 2);
    assert_eq!(
        recovered_entries
            .last()
            .ok_or("expected recovered entry")?
            .end_block
            .height
            .value(),
        4
    );
    super::layout::validate_contiguous_index_entries(&recovered_entries)?;
    verify_pinned_segments(dir.path(), &recovered_entries, false)?;
    Ok(())
}

#[test]
fn sealer_rolls_over_when_cumulative_spool_exceeds_budget() -> TestResult {
    let dir = TempDir::new()?;
    let b1 = build_block_fixture(1);
    let mut rec1 =
        LiveTailBlockRecord::from_compact_block(&b1.compact_block, true, Some(b1.canonical_facts))?;
    rec1.canonical_facts = None;
    let encoded_raw_len = b1.raw_block.encode_v1()?.len();
    let single_frame_bytes = u64::try_from(16 + rec1.encode_v1()?.len() + encoded_raw_len)?;
    let max_spool_bytes = single_frame_bytes + single_frame_bytes / 2;

    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4)
            .with_max_spool_bytes(max_spool_bytes);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;

    assert_eq!(sealer.push_block(build_block_input(1, true))?, Vec::new());
    assert_eq!(sealer.pending_len(), 1);

    let e2 = sealer.push_block(build_block_input(2, true))?;
    assert_eq!(e2.len(), 1);
    assert_eq!(e2[0].start_height.value(), 1);
    assert_eq!(e2[0].end_block.height.value(), 1);
    assert_eq!(sealer.pending_len(), 1);

    let e3 = sealer.push_block(build_block_input(3, true))?;
    assert_eq!(e3.len(), 1);
    assert_eq!(e3[0].start_height.value(), 2);
    assert_eq!(e3[0].end_block.height.value(), 2);
    assert_eq!(sealer.pending_len(), 1);

    let e4 = sealer.push_block(build_block_input(4, true))?;
    assert_eq!(e4.len(), 2);
    assert_eq!(e4[0].start_height.value(), 3);
    assert_eq!(e4[0].end_block.height.value(), 3);
    assert_eq!(e4[1].start_height.value(), 4);
    assert_eq!(e4[1].end_block.height.value(), 4);
    assert_eq!(sealer.pending_len(), 0);
    assert!(!sealer.is_poisoned());

    let mut all_entries = Vec::new();
    all_entries.extend(e2);
    all_entries.extend(e3);
    all_entries.extend(e4);
    assert_eq!(all_entries.len(), 4);
    super::layout::validate_contiguous_index_entries(&all_entries)?;
    verify_pinned_segments(dir.path(), &all_entries, false)?;
    Ok(())
}

#[test]
fn live_tail_predicted_encoded_len_matches_actual_for_some_and_none_zip317_actions() -> TestResult {
    let fixture_none = build_block_fixture(1);
    let mut parts_none = fixture_none.compact_block.into_parts();
    parts_none.transactions.truncate(1);
    let compact_none = CompactBlockArtifact::new(
        parts_none.block_id,
        parts_none.previous_block_hash,
        parts_none.time,
        parts_none.transactions,
        parts_none.chain_metadata,
    )?;
    let mut facts_none = fixture_none.canonical_facts;
    facts_none.transactions.truncate(1);
    let record_none =
        LiveTailBlockRecord::from_compact_block(&compact_none, true, Some(facts_none))?;
    let meta_none = record_none
        .supplement
        .as_ref()
        .and_then(|sup| sup.canonical_metadata.as_ref())
        .ok_or("missing canonical_metadata for None variant")?;
    assert_eq!(meta_none.transactions[0].zip317_logical_actions, None);
    assert_eq!(
        record_none.predict_encoded_v1_len()?,
        record_none.encode_v1()?.len()
    );

    let fixture_some = build_block_fixture(1);
    let mut parts_some = fixture_some.compact_block.into_parts();
    let mut tx_some = parts_some
        .transactions
        .pop()
        .ok_or("missing second tx in fixture")?;
    tx_some.index = 0;
    let compact_some = CompactBlockArtifact::new(
        parts_some.block_id,
        parts_some.previous_block_hash,
        parts_some.time,
        vec![tx_some],
        parts_some.chain_metadata,
    )?;
    let mut facts_some = fixture_some.canonical_facts;
    let kept_tx_facts = facts_some
        .transactions
        .pop()
        .ok_or("missing second tx fact in fixture")?;
    facts_some.transactions = vec![kept_tx_facts];
    let record_some =
        LiveTailBlockRecord::from_compact_block(&compact_some, true, Some(facts_some))?;
    let meta_some = record_some
        .supplement
        .as_ref()
        .and_then(|sup| sup.canonical_metadata.as_ref())
        .ok_or("missing canonical_metadata for Some variant")?;
    assert!(meta_some.transactions[0].zip317_logical_actions.is_some());
    assert_eq!(
        record_some.predict_encoded_v1_len()?,
        record_some.encode_v1()?.len()
    );
    Ok(())
}

#[test]
fn live_tail_raw_block_split_codec_round_trip_and_legacy_flag_rejection() -> TestResult {
    assert_eq!(LIVE_TAIL_RAW_BLOCK_VERSION_V1, 1);
    let fixture = build_block_fixture(19);
    let encoded_raw = fixture.raw_block.encode_v1()?;
    let decoded_raw = LiveTailRawBlock::decode_v1(&encoded_raw)?;
    assert_eq!(decoded_raw, fixture.raw_block);
    assert_eq!(
        decoded_raw.read_verified_raw_transaction(0)?,
        fixture.tx0_raw
    );
    assert_eq!(
        decoded_raw.serialized_bytes_digest(),
        fixture.canonical_facts.serialized_bytes_digest
    );

    assert_live_tail_raw_rejections(&encoded_raw);

    let record = LiveTailBlockRecord::from_compact_block(
        &fixture.compact_block,
        true,
        Some(fixture.canonical_facts),
    )?;
    let mut legacy_flagged = record.encode_v1()?;
    legacy_flagged[10] |= 0x04;
    recompute_live_tail_trailer(&mut legacy_flagged);
    assert!(matches!(
        LiveTailBlockRecord::decode_v1(&legacy_flagged),
        Err(SettledSegmentsError::InvalidFormat(_))
    ));
    Ok(())
}

fn assert_live_tail_raw_rejections(encoded_raw: &[u8]) {
    for cut in [0, 8, 32, encoded_raw.len() - 1] {
        assert!(LiveTailRawBlock::decode_v1(&encoded_raw[..cut]).is_err());
    }

    let mut trailing = encoded_raw.to_vec();
    trailing.push(0xaa);
    assert!(LiveTailRawBlock::decode_v1(&trailing).is_err());

    let mut tampered = encoded_raw.to_vec();
    tampered[20] ^= 0x01;
    assert!(LiveTailRawBlock::decode_v1(&tampered).is_err());

    let mut bad_ver = encoded_raw.to_vec();
    bad_ver[8..10].copy_from_slice(&2_u16.to_be_bytes());
    recompute_live_tail_trailer(&mut bad_ver);
    assert!(LiveTailRawBlock::decode_v1(&bad_ver).is_err());

    let mut bad_flags = encoded_raw.to_vec();
    bad_flags[10] = 0x01;
    recompute_live_tail_trailer(&mut bad_flags);
    assert!(LiveTailRawBlock::decode_v1(&bad_flags).is_err());

    let mut bad_reserved = encoded_raw.to_vec();
    bad_reserved[11] = 0x01;
    recompute_live_tail_trailer(&mut bad_reserved);
    assert!(LiveTailRawBlock::decode_v1(&bad_reserved).is_err());
}

fn build_padded_block_input(
    height: u32,
    extra_script_bytes: usize,
) -> Result<SettledSegmentBlockInput, Box<dyn std::error::Error>> {
    let mut input = build_block_input(height, true);
    let mut pattern = Vec::with_capacity(extra_script_bytes);
    for idx in 0..extra_script_bytes {
        pattern.push(u8::try_from(
            (idx.wrapping_mul(31).wrapping_add(height as usize)) & 0xff,
        )?);
    }
    let tx_facts = &mut input.canonical_facts.transactions[0];
    if let Some(first_out) = tx_facts.transparent_outputs.first_mut() {
        first_out.script_pub_key = pattern.clone();
    }
    let parts = input.compact_block.into_parts();
    let mut txs = parts.transactions;
    if let Some(first_out) = txs[0].data.transparent_outputs.first_mut() {
        first_out.script_pub_key = pattern;
    }
    input.compact_block = CompactBlockArtifact::new(
        parts.block_id,
        parts.previous_block_hash,
        parts.time,
        txs,
        parts.chain_metadata,
    )?;
    Ok(input)
}

#[test]
fn sealer_enforces_256_mib_default_cap_and_presplits_all_three_segment_types() -> TestResult {
    assert_eq!(DEFAULT_MAX_SETTLED_SEGMENT_BYTES, 256 * 1024 * 1024);
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let defaults =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules);
    assert_eq!(
        defaults.max_shielded_segment_bytes,
        DEFAULT_MAX_SETTLED_SEGMENT_BYTES
    );
    assert_eq!(
        defaults.max_supplement_segment_bytes,
        DEFAULT_MAX_SETTLED_SEGMENT_BYTES
    );
    assert_eq!(
        defaults.max_raw_segment_bytes,
        DEFAULT_MAX_SETTLED_SEGMENT_BYTES
    );

    assert_shielded_file_cap_presplit(modules)?;
    assert_supplement_file_cap_presplit(modules)?;
    assert_raw_file_cap_presplit(modules)?;
    Ok(())
}

fn assert_shielded_file_cap_presplit(modules: SettledSegmentModules) -> TestResult {
    let dir = TempDir::new()?;
    let sample = encode_default_shielded_payload(&build_block_input(1, true).compact_block)?;
    let frame_len =
        u64::try_from(crate::settled_shielded_segment::SHIELDED_FRAME_PREFIX_LEN + sample.0.len())?;
    let cap = u64::try_from(
        SHIELDED_HEADER_LEN + SHIELDED_TRAILER_LEN + 2 * SHIELDED_ROW_INDEX_ENTRY_LEN,
    )? + frame_len * 2
        + 8;
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4)
            .with_max_shielded_segment_bytes(cap);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let mut entries = Vec::new();
    for height in 1..=4_u32 {
        entries.extend(sealer.push_block(build_block_input(height, true))?);
    }
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        let paths = SettledSegmentFilePaths::for_entry(dir.path(), entry)?;
        assert!(fs::metadata(&paths.shielded)?.len() <= cap);
    }
    verify_pinned_segments(dir.path(), &entries, false)?;
    Ok(())
}

fn assert_supplement_file_cap_presplit(modules: SettledSegmentModules) -> TestResult {
    let dir = TempDir::new()?;
    let sup_cap = u64::try_from(
        SUPPLEMENT_HEADER_LEN
            + SUPPLEMENT_TRAILER_LEN
            + SUPPLEMENT_CHUNK_INDEX_ENTRY_LEN
            + 64
            + SUPPLEMENT_ROW_INDEX_ENTRY_LEN
            + 2_800,
    )?;
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4)
            .with_max_supplement_segment_bytes(sup_cap);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let mut entries = Vec::new();
    for height in 1..=4_u32 {
        entries.extend(sealer.push_block(build_padded_block_input(height, 1_200)?)?);
    }
    assert_eq!(entries.len(), 4);
    assert_eq!(sealer.pending_len(), 0);
    for entry in &entries {
        let sup_path = SettledSegmentFilePaths::for_entry(dir.path(), entry)?
            .supplement
            .ok_or("missing supplement path")?;
        assert!(fs::metadata(&sup_path)?.len() <= sup_cap);
    }
    verify_pinned_segments(dir.path(), &entries, false)?;

    // Single block exceeding supplement cap fails before rollover without poisoning.
    assert_eq!(
        sealer.push_block(build_padded_block_input(5, 1_200)?)?,
        Vec::new()
    );
    assert!(matches!(
        sealer.push_block(build_padded_block_input(6, 4_096)?),
        Err(SettledSegmentsError::InvalidFormat(_))
    ));
    assert!(!sealer.is_poisoned());
    assert_eq!(sealer.pending_len(), 1);
    Ok(())
}

fn assert_raw_file_cap_presplit(modules: SettledSegmentModules) -> TestResult {
    let dir = TempDir::new()?;
    let sample_raw = build_block_input(1, true)
        .raw_block
        .ok_or("missing raw")?
        .raw_block_bytes
        .len();
    let per_block_frame = u64::try_from(RAW_FRAME_PREFIX_LEN + sample_raw)?;
    let cap = u64::try_from(
        RAW_HEADER_LEN + RAW_TRAILER_LEN + 2 * RAW_ROW_INDEX_ENTRY_LEN + 4 * RAW_TX_SLICE_ENTRY_LEN,
    )? + per_block_frame * 2
        + 8;
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(4)
            .with_raw_block_codec(RawBlockCodec::None)
            .with_max_raw_segment_bytes(cap);
    let mut sealer = SettledSegmentSealer::new(dir.path(), config, None)?;
    let mut entries = Vec::new();
    for height in 1..=4_u32 {
        entries.extend(sealer.push_block(build_block_input(height, true))?);
    }
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        let raw_path = SettledSegmentFilePaths::for_entry(dir.path(), entry)?
            .raw_block
            .ok_or("missing raw path")?;
        assert!(fs::metadata(&raw_path)?.len() <= cap);
    }
    verify_pinned_segments(dir.path(), &entries, false)?;
    Ok(())
}

fn seal_multi_test_strides(
    store_root: &Path,
    stride_count: u32,
    stride: u32,
    modules: SettledSegmentModules,
) -> Result<Vec<SettledSegmentIndexEntry>, SettledSegmentsError> {
    let config =
        SettledSegmentSealerConfig::new(Network::ZcashRegtest, TEST_SCHEMA_VERSION, modules)
            .with_segment_blocks(stride);
    let mut sealer = SettledSegmentSealer::new(store_root, config, None)?;
    let mut entries = Vec::new();
    for height in 1..=(stride_count * stride) {
        entries.extend(sealer.push_block(build_block_input(height, modules.raw_bodies))?);
    }
    Ok(entries)
}

#[test]
fn reader_cache_opens_modules_lazily_and_never_touches_raw_or_supplement_on_shielded_reads()
-> TestResult {
    assert_eq!(DEFAULT_SETTLED_SEGMENT_READER_CACHE_CAPACITY, 1_024);
    let dir = TempDir::new()?;
    let modules_full = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let entries = seal_multi_test_strides(dir.path(), 2, 4, modules_full)?;
    let entry_broken_aux = &entries[0];
    let entry_lazy = &entries[1];

    let paths_broken = SettledSegmentFilePaths::for_entry(dir.path(), entry_broken_aux)?;
    fs::write(
        paths_broken.supplement.as_ref().ok_or("missing sup")?,
        b"corrupted",
    )?;
    fs::remove_file(paths_broken.raw_block.as_ref().ok_or("missing raw")?)?;

    let cache = SettledSegmentReaderCache::with_default_capacity(dir.path(), false)?;
    assert_eq!(
        cache.capacity(),
        DEFAULT_SETTLED_SEGMENT_READER_CACHE_CAPACITY
    );

    let shielded = cache.get_or_open_shielded(entry_broken_aux)?;
    assert_eq!(shielded.identity(), entry_broken_aux.shielded_identity);
    assert!(cache.contains_module(entry_broken_aux.start_height, SettledSegmentKind::Shielded));
    assert!(!cache.contains_module(
        entry_broken_aux.start_height,
        SettledSegmentKind::Supplement
    ));
    assert!(!cache.contains_module(entry_broken_aux.start_height, SettledSegmentKind::RawBlock));
    assert_eq!(cache.open_count(SettledSegmentKind::Shielded), 1);
    assert_eq!(cache.open_count(SettledSegmentKind::Supplement), 0);
    assert_eq!(cache.open_count(SettledSegmentKind::RawBlock), 0);

    let _ = cache.get_or_open_shielded(entry_lazy)?;
    assert_eq!(cache.open_count(SettledSegmentKind::Shielded), 2);
    assert_eq!(cache.open_count(SettledSegmentKind::Supplement), 0);
    assert_eq!(cache.open_count(SettledSegmentKind::RawBlock), 0);

    assert!(cache.get_or_open_supplement(entry_lazy)?.is_some());
    assert_eq!(cache.open_count(SettledSegmentKind::Supplement), 1);
    assert_eq!(cache.open_count(SettledSegmentKind::RawBlock), 0);

    assert!(cache.get_or_open_raw_block(entry_lazy)?.is_some());
    assert_eq!(cache.open_count(SettledSegmentKind::RawBlock), 1);

    let min_dir = TempDir::new()?;
    let min_entry = seal_test_stride(
        min_dir.path(),
        1..=4,
        4,
        SettledSegmentModules {
            supplement: false,
            raw_bodies: false,
        },
        None,
    )?;
    let min_cache = SettledSegmentReaderCache::with_default_capacity(min_dir.path(), false)?;
    assert!(min_cache.get_or_open_supplement(&min_entry)?.is_none());
    assert!(min_cache.get_or_open_raw_block(&min_entry)?.is_none());
    assert_eq!(min_cache.open_count(SettledSegmentKind::Supplement), 0);
    assert_eq!(min_cache.open_count(SettledSegmentKind::RawBlock), 0);
    Ok(())
}

#[test]
fn reader_cache_deduplicates_concurrent_cold_misses_via_singleflight() -> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let entry = seal_test_stride(dir.path(), 1..=4, 4, modules, None)?;
    let cache = SettledSegmentReaderCache::with_default_capacity(dir.path(), false)?;
    cache.set_on_disk_open_hook(Some(Arc::new(|_| {
        std::thread::sleep(std::time::Duration::from_millis(25));
    })));

    assert_singleflight_module(16, || cache.get_or_open_shielded(&entry))?;
    assert_eq!(cache.open_count(SettledSegmentKind::Shielded), 1);

    let sup_first = assert_singleflight_module(16, || {
        cache
            .get_or_open_supplement(&entry)?
            .ok_or_else(|| SettledSegmentsError::InvalidFormat("expected supplement".to_owned()))
    })?;
    assert!(Arc::strong_count(&sup_first) >= 1);
    assert_eq!(cache.open_count(SettledSegmentKind::Supplement), 1);

    assert_singleflight_module(16, || {
        cache
            .get_or_open_raw_block(&entry)?
            .ok_or_else(|| SettledSegmentsError::InvalidFormat("expected raw block".to_owned()))
    })?;
    assert_eq!(cache.open_count(SettledSegmentKind::RawBlock), 1);

    let mut wrong_pin = entry.clone();
    wrong_pin.shielded_identity.sha256[0] ^= 0xff;
    assert!(matches!(
        cache.get_or_open_shielded(&wrong_pin),
        Err(SettledSegmentsError::ChainLinkage(_))
    ));

    assert_singleflight_error_recovery(&cache, dir.path(), &entry)?;
    Ok(())
}

fn assert_singleflight_module<T, F>(
    threads: usize,
    fetch: F,
) -> Result<Arc<T>, Box<dyn std::error::Error>>
where
    T: Send + Sync,
    F: Fn() -> Result<Arc<T>, SettledSegmentsError> + Sync,
{
    let barrier = std::sync::Barrier::new(threads);
    let handles: Vec<Arc<T>> = std::thread::scope(|scope| {
        let mut joined = Vec::with_capacity(threads);
        for _ in 0..threads {
            joined.push(scope.spawn(|| {
                barrier.wait();
                fetch()
            }));
        }
        let mut outputs = Vec::with_capacity(threads);
        for handle in joined {
            outputs.push(handle.join().map_err(|_| {
                SettledSegmentsError::InvalidFormat("thread panicked".to_owned())
            })??);
        }
        Ok::<_, SettledSegmentsError>(outputs)
    })?;
    let first = handles.first().ok_or("empty handle set")?.clone();
    for candidate in &handles[1..] {
        assert!(Arc::ptr_eq(&first, candidate));
    }
    Ok(first)
}

fn assert_singleflight_error_recovery(
    cache: &SettledSegmentReaderCache,
    store_root: &Path,
    entry: &SettledSegmentIndexEntry,
) -> TestResult {
    let paths = SettledSegmentFilePaths::for_entry(store_root, entry)?;
    let backup = fs::read(&paths.shielded)?;
    fs::write(&paths.shielded, b"corrupt")?;
    assert!(cache.evict(entry.start_height));

    let barrier = std::sync::Barrier::new(8);
    let errors: Vec<SettledSegmentsError> = std::thread::scope(|scope| {
        let mut joined = Vec::with_capacity(8);
        for _ in 0..8 {
            joined.push(scope.spawn(|| {
                barrier.wait();
                cache.get_or_open_shielded(entry)
            }));
        }
        let mut collected = Vec::with_capacity(8);
        for handle in joined {
            let outcome = handle
                .join()
                .map_err(|_| SettledSegmentsError::InvalidFormat("thread panicked".to_owned()))?;
            assert!(outcome.is_err());
            if let Err(err) = outcome {
                collected.push(err);
            }
        }
        Ok::<_, SettledSegmentsError>(collected)
    })?;
    assert_eq!(errors.len(), 8);

    fs::write(&paths.shielded, backup)?;
    let recovered = cache.get_or_open_shielded(entry)?;
    assert_eq!(recovered.identity(), entry.shielded_identity);
    Ok(())
}

#[test]
fn verify_pinned_segments_verifies_multi_stride_in_parallel_and_reports_earliest_failure()
-> TestResult {
    let dir = TempDir::new()?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let entries = seal_multi_test_strides(dir.path(), 8, 2, modules)?;
    assert_eq!(entries.len(), 8);
    verify_pinned_segments(dir.path(), &entries, false)?;

    // Truncate stride 5 (InvalidFormat) and flip trailer of stride 2 (Integrity).
    // Lowest index (stride 2) must win deterministically.
    let paths_5 = SettledSegmentFilePaths::for_entry(dir.path(), &entries[5])?;
    fs::write(&paths_5.shielded, b"short")?;
    let paths_2 = SettledSegmentFilePaths::for_entry(dir.path(), &entries[2])?;
    let mut bytes_2 = fs::read(&paths_2.shielded)?;
    let last_2 = bytes_2.last_mut().ok_or("empty stride 2 shielded")?;
    *last_2 ^= 0xff;
    fs::write(&paths_2.shielded, bytes_2)?;

    let err = verify_pinned_segments(dir.path(), &entries, false)
        .err()
        .ok_or("expected verification failure")?;
    assert!(matches!(
        err,
        SettledSegmentsError::Shielded(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn manifest_uses_standardized_segments_manifest_json_filename() -> TestResult {
    assert_eq!(SEGMENTS_MANIFEST_FILE_NAME, "segments-manifest.json");
    let root = TempDir::new()?;
    let src_dir = root.path().join("src");
    let snap_dir = root.path().join("snap");
    fs::create_dir_all(&src_dir)?;
    let modules = SettledSegmentModules {
        supplement: true,
        raw_bodies: true,
    };
    let entry = seal_test_stride(&src_dir, 1..=4, 4, modules, None)?;

    snapshot_pinned_segments(&src_dir, &snap_dir, std::slice::from_ref(&entry))?;
    assert!(snap_dir.join("segments-manifest.json").exists());
    assert!(
        !snap_dir
            .join("canonical-segments-manifest.v2.json")
            .exists()
    );
    verify_pinned_manifest_and_segments(&snap_dir, false)?;
    Ok(())
}
