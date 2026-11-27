use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    future::{Future, poll_fn},
    io::{self, Seek, SeekFrom, Write},
    path::Path,
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use parking_lot::Mutex;
use prost::{
    Message,
    encoding::{WireType, encode_key, encode_varint},
};
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempDir};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, CompactTransparentInput, CompactTransparentOutput, Network,
    TransactionId,
    wire::{encode_internal_block_hash, encode_internal_transaction_id},
};
use zinder_proto::compat::lightwalletd::{
    self, BoundedChunkRetention, EncodedBlock, EncodedBlockBatcher, EncodedBlockFrame,
    EncodedBlockFrameStream, EncodedBlockRange, EncodedBlockRangeStream,
    compact_tx_streamer_client::CompactTxStreamerClient,
    compact_tx_streamer_server::{CompactTxStreamer, CompactTxStreamerServer},
};

use crate::{
    HeightOrder,
    positional_source::{ByteRange, PositionalByteSource},
};

use super::{
    BoundedShieldedRangePlan, PoolPresenceMask, SEGMENT_FLAG_RESERVED_SHIFTED_EPK,
    SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES, SettledShieldedSegmentError,
    SettledShieldedSegmentIndex, SettledShieldedSegmentOpenOptions, SettledShieldedSegmentReader,
    SettledShieldedSegmentWriter, ShieldedBatchReadRequest, ShieldedBoundedBatchReadRequest,
    ShieldedFetchedSlab, ShieldedIndexSlice, ShieldedProjectionSelection, ShieldedRangeByteBudget,
    ShieldedReadScratch, ShieldedSegmentHeader, ShieldedSegmentIdentity, ShieldedSlabDecodeRequest,
    VerifiedShieldedIndexSlice,
    format::{
        CompactShieldedRow, DecodedRowIndexEntry, FRAME_PREFIX_LEN, FRAME_PREFIX_U32, HEADER_LEN,
        INDEX_PAGE_ROWS, LARGE_ROW_ISOLATION_MIN_BYTES, REPACKED_SHORT_BATCH_BYTES,
        ROW_INDEX_ENTRY_LEN, SLAB_TARGET_BYTES, TRAILER_LEN, VALIDATION_INVALID,
        VALIDATION_UNVALIDATED, VALIDATION_VALID, encode_frame_prefix,
    },
    is_passthrough_shielded_block_wire, project_shielded_block_wire,
    project_shielded_block_wire_with_scratch,
    reader::validate_entries,
    wire_projection::synthesize_empty_nullifier_block,
};

type TestOutcome<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const TEST_NETWORK: Network = Network::ZcashRegtest;
const TEST_SCHEMA: u16 = 1;

fn block_hash(seed: u32) -> BlockHash {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&seed.to_le_bytes());
    bytes[31] = 0xaa;
    BlockHash::from_bytes(bytes)
}

fn txid(seed: u8) -> TransactionId {
    TransactionId::from_bytes([seed; 32])
}

fn sapling_spend(seed: u8) -> CompactSaplingSpend {
    CompactSaplingSpend {
        nullifier: [seed; 32],
    }
}

fn sapling_output(seed: u8) -> CompactSaplingOutput {
    CompactSaplingOutput {
        commitment: [seed; 32],
        ephemeral_key: [seed.wrapping_add(1); 32],
        ciphertext: [seed.wrapping_add(2); 52],
    }
}

fn shielded_action(seed: u8) -> CompactShieldedAction {
    CompactShieldedAction {
        nullifier: [seed; 32],
        commitment: [seed.wrapping_add(1); 32],
        ephemeral_key: [seed.wrapping_add(2); 32],
        ciphertext: [seed.wrapping_add(3); 52],
    }
}

fn make_tx(index: u64, seed: u8, scan: CompactTransactionData) -> CompactTransaction {
    CompactTransaction {
        index,
        transaction_id: txid(seed),
        data: scan,
    }
}

fn all_pools_fixture_tx() -> CompactTransaction {
    make_tx(
        5,
        15,
        CompactTransactionData {
            fee_zat: Some(0),
            sapling_spends: vec![sapling_spend(0x61)],
            sapling_outputs: vec![sapling_output(0x62)],
            orchard_actions: vec![shielded_action(0x63)],
            ironwood_actions: vec![shielded_action(0x64)],
            transparent_inputs: vec![CompactTransparentInput {
                previous_transaction_id: txid(0x01),
                previous_output_index: 1,
            }],
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 42,
                script_pub_key: vec![0x76, 0xa9],
            }],
        },
    )
}

fn rich_fixture_block_2() -> Result<CompactBlockArtifact, zinder_core::CompactBlockArtifactError> {
    let transparent_coinbase = make_tx(
        0,
        10,
        CompactTransactionData {
            fee_zat: None,
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 625_000_000,
                script_pub_key: vec![0x51],
            }],
            ..CompactTransactionData::default()
        },
    );
    let sapling_spend_only = make_tx(
        1,
        11,
        CompactTransactionData {
            fee_zat: Some(10_000),
            sapling_spends: vec![sapling_spend(0x21)],
            ..CompactTransactionData::default()
        },
    );
    // Sapling output-only deposit (zero spends): must survive Sapling nullifier projection.
    let sapling_deposit_only = make_tx(
        2,
        12,
        CompactTransactionData {
            fee_zat: Some(15_000),
            sapling_outputs: vec![sapling_output(0x31)],
            ..CompactTransactionData::default()
        },
    );
    let orchard_only = make_tx(
        3,
        13,
        CompactTransactionData {
            fee_zat: Some(20_000),
            orchard_actions: vec![shielded_action(0x41), shielded_action(0x45)],
            ..CompactTransactionData::default()
        },
    );
    let ironwood_only = make_tx(
        4,
        14,
        CompactTransactionData {
            fee_zat: Some(25_000),
            ironwood_actions: vec![shielded_action(0x51), shielded_action(0x55)],
            ..CompactTransactionData::default()
        },
    );
    CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(2), block_hash(2)),
        block_hash(1),
        1_700_000_002,
        vec![
            transparent_coinbase,
            sapling_spend_only,
            sapling_deposit_only,
            orchard_only,
            ironwood_only,
            all_pools_fixture_tx(),
        ],
        CompactChainMetadata {
            sapling_commitment_tree_size: 12,
            orchard_commitment_tree_size: 23,
            ironwood_commitment_tree_size: 33,
        },
    )
}

fn trailing_fixture_blocks()
-> Result<Vec<CompactBlockArtifact>, zinder_core::CompactBlockArtifactError> {
    let orchard_block_3 = CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(3), block_hash(3)),
        block_hash(2),
        1_700_000_003,
        vec![make_tx(
            0,
            20,
            CompactTransactionData {
                fee_zat: Some(5_000),
                orchard_actions: vec![shielded_action(0x71)],
                ..CompactTransactionData::default()
            },
        )],
        CompactChainMetadata {
            sapling_commitment_tree_size: 12,
            orchard_commitment_tree_size: 24,
            ironwood_commitment_tree_size: 33,
        },
    )?;

    let sapling_block_4 = CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(4), block_hash(4)),
        block_hash(3),
        1_700_000_004,
        vec![make_tx(
            0,
            21,
            CompactTransactionData {
                fee_zat: Some(5_000),
                sapling_spends: vec![sapling_spend(0x81)],
                sapling_outputs: vec![sapling_output(0x82)],
                ..CompactTransactionData::default()
            },
        )],
        CompactChainMetadata {
            sapling_commitment_tree_size: 13,
            orchard_commitment_tree_size: 24,
            ironwood_commitment_tree_size: 33,
        },
    )?;

    let mixed_block_5 = CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(5), block_hash(5)),
        block_hash(4),
        u32::MAX,
        vec![make_tx(
            0,
            22,
            CompactTransactionData {
                fee_zat: None,
                sapling_outputs: vec![sapling_output(0x91)],
                orchard_actions: vec![shielded_action(0x92)],
                ironwood_actions: vec![shielded_action(0x93)],
                ..CompactTransactionData::default()
            },
        )],
        CompactChainMetadata {
            sapling_commitment_tree_size: 14,
            orchard_commitment_tree_size: 25,
            ironwood_commitment_tree_size: 34,
        },
    )?;

    let empty_block_6 = CompactBlockArtifact::empty(
        BlockId::new(BlockHeight::new(6), block_hash(6)),
        block_hash(5),
        1_700_000_006,
        CompactChainMetadata {
            sapling_commitment_tree_size: 14,
            orchard_commitment_tree_size: 25,
            ironwood_commitment_tree_size: 34,
        },
    );

    Ok(vec![
        orchard_block_3,
        sapling_block_4,
        mixed_block_5,
        empty_block_6,
    ])
}

fn mixed_fixture_blocks()
-> Result<Vec<CompactBlockArtifact>, zinder_core::CompactBlockArtifactError> {
    let empty_block_1 = CompactBlockArtifact::empty(
        BlockId::new(BlockHeight::new(1), block_hash(1)),
        block_hash(0),
        0,
        CompactChainMetadata {
            sapling_commitment_tree_size: 10,
            orchard_commitment_tree_size: 20,
            ironwood_commitment_tree_size: 30,
        },
    );
    let mut blocks = Vec::with_capacity(6);
    blocks.push(empty_block_1);
    blocks.push(rich_fixture_block_2()?);
    blocks.extend(trailing_fixture_blocks()?);
    Ok(blocks)
}

fn reference_encode_actions(
    actions: &[CompactShieldedAction],
    nullifiers_only: bool,
) -> Vec<lightwalletd::CompactOrchardAction> {
    actions
        .iter()
        .map(|action| {
            if nullifiers_only {
                lightwalletd::CompactOrchardAction {
                    nullifier: action.nullifier.to_vec(),
                    ..lightwalletd::CompactOrchardAction::default()
                }
            } else {
                lightwalletd::CompactOrchardAction {
                    nullifier: action.nullifier.to_vec(),
                    cmx: action.commitment.to_vec(),
                    ephemeral_key: action.ephemeral_key.to_vec(),
                    ciphertext: action.ciphertext.to_vec(),
                }
            }
        })
        .collect()
}

fn reference_encode_block(
    block: &CompactBlockArtifact,
    selection: ShieldedProjectionSelection,
) -> TestOutcome<EncodedBlock> {
    let mut vtx = Vec::new();
    for tx in block.transactions() {
        let scan = &tx.data;
        let has_sapling = !scan.sapling_spends.is_empty() || !scan.sapling_outputs.is_empty();
        let has_orchard = !scan.orchard_actions.is_empty();
        let has_ironwood = !scan.ironwood_actions.is_empty();
        let included = (selection.sapling && has_sapling)
            || (selection.orchard && has_orchard)
            || (selection.ironwood && has_ironwood);
        if !included {
            continue;
        }
        let fee = scan
            .fee_zat
            .map(u32::try_from)
            .transpose()?
            .unwrap_or_default();
        let mut msg = lightwalletd::CompactTx {
            index: tx.index,
            txid: encode_internal_transaction_id(tx.transaction_id).to_vec(),
            fee,
            ..lightwalletd::CompactTx::default()
        };
        if selection.sapling {
            msg.spends = scan
                .sapling_spends
                .iter()
                .map(|spend| lightwalletd::CompactSaplingSpend {
                    nf: spend.nullifier.to_vec(),
                })
                .collect();
            if !selection.nullifiers_only {
                msg.outputs = scan
                    .sapling_outputs
                    .iter()
                    .map(|output| lightwalletd::CompactSaplingOutput {
                        cmu: output.commitment.to_vec(),
                        ephemeral_key: output.ephemeral_key.to_vec(),
                        ciphertext: output.ciphertext.to_vec(),
                    })
                    .collect();
            }
        }
        if selection.orchard {
            msg.actions =
                reference_encode_actions(&scan.orchard_actions, selection.nullifiers_only);
        }
        if selection.ironwood {
            msg.ironwood_actions =
                reference_encode_actions(&scan.ironwood_actions, selection.nullifiers_only);
        }
        vtx.push(msg);
    }
    let meta = block.chain_metadata();
    let proto_block = lightwalletd::CompactBlock {
        height: u64::from(block.height().value()),
        hash: encode_internal_block_hash(block.block_hash()).to_vec(),
        prev_hash: encode_internal_block_hash(block.previous_block_hash()).to_vec(),
        time: block.time(),
        header: Vec::new(),
        vtx,
        chain_metadata: Some(if selection.nullifiers_only {
            lightwalletd::ChainMetadata::default()
        } else {
            lightwalletd::ChainMetadata {
                sapling_commitment_tree_size: meta.sapling_commitment_tree_size,
                orchard_commitment_tree_size: meta.orchard_commitment_tree_size,
                ironwood_commitment_tree_size: meta.ironwood_commitment_tree_size,
            }
        }),
    };
    Ok(EncodedBlock::encode(&proto_block)?)
}

fn write_segment(
    dir: &Path,
    name: &str,
    blocks: &[CompactBlockArtifact],
) -> TestOutcome<(std::path::PathBuf, ShieldedSegmentIdentity)> {
    let target = dir.join(name);
    let staged = NamedTempFile::new_in(dir)?;
    let first = blocks
        .first()
        .ok_or("cannot write empty test segment")?
        .height();
    let last_block = blocks.last().ok_or("cannot write empty test segment")?;
    let end = BlockId::new(last_block.height(), last_block.block_hash());
    let mut writer = SettledShieldedSegmentWriter::create(
        &target,
        staged,
        ShieldedSegmentIdentity::span(TEST_NETWORK, TEST_SCHEMA, first, end),
    )?;
    for block in blocks {
        let native_bytes = crate::encode_compact_block_artifact(block).len();
        writer.push(block, native_bytes)?;
    }
    let identity = writer.finish()?;
    Ok((target, identity))
}

fn recompute_portable_digests(bytes: &[u8]) -> TestOutcome<([u8; 32], [u8; 32])> {
    let body_end = bytes.len() - TRAILER_LEN;
    let header_bytes: &[u8; HEADER_LEN] = bytes[..HEADER_LEN].try_into()?;
    let index_offset =
        usize::try_from(ShieldedSegmentHeader::decode_unchecked(header_bytes)?.index_offset())?;
    let trailer: [u8; 32] = Sha256::digest(&bytes[..body_end]).into();
    let mut index_digest = Sha256::new();
    index_digest.update(header_bytes);
    index_digest.update(&bytes[index_offset..body_end]);
    index_digest.update(trailer);
    Ok((trailer, index_digest.finalize().into()))
}

#[test]
fn indexed_block_row_stays_12_bytes() {
    assert_eq!(std::mem::size_of::<CompactShieldedRow>(), 12);
}

fn assert_blocks_match_expected(
    actual: &[EncodedBlock],
    expected: &[EncodedBlock],
    selection: ShieldedProjectionSelection,
    order: HeightOrder,
) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "length mismatch for {selection:?} {order:?}"
    );
    for (idx, (got, want)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            got.as_length_prefixed(),
            want.as_length_prefixed(),
            "wire mismatch at idx={idx} for {selection:?} {order:?}"
        );
    }
}

#[test]
fn all_sixteen_pool_and_nullifier_projections_match_reference_prost_bytes() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "seg-001", &blocks)?;
    let reader = SettledShieldedSegmentReader::open(
        &path,
        &identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    assert_eq!(reader.block_count(), blocks.len());
    for block in &blocks {
        assert_eq!(
            reader.native_bytes_at(block.height())?,
            u32::try_from(crate::encode_compact_block_artifact(block).len())?
        );
    }
    assert!(matches!(
        reader.native_bytes_at(BlockHeight::new(0)),
        Err(SettledShieldedSegmentError::HeightOutOfRange { .. })
    ));
    assert!(matches!(
        reader.native_bytes_at(BlockHeight::new(7)),
        Err(SettledShieldedSegmentError::HeightOutOfRange { .. })
    ));

    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6));
    for sapling in [false, true] {
        for orchard in [false, true] {
            for ironwood in [false, true] {
                for nullifiers_only in [false, true] {
                    let selection = ShieldedProjectionSelection::new(
                        sapling,
                        orchard,
                        ironwood,
                        nullifiers_only,
                    );
                    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
                        let fresh_reader = SettledShieldedSegmentReader::open(
                            &path,
                            &identity,
                            SettledShieldedSegmentOpenOptions::default(),
                        )?;
                        let first = fresh_reader.read_projected_range(range, order, selection)?;
                        let second = fresh_reader.read_projected_range(range, order, selection)?;
                        let mut expected: Vec<EncodedBlock> = blocks
                            .iter()
                            .map(|block| reference_encode_block(block, selection))
                            .collect::<TestOutcome<Vec<_>>>()?;
                        if order == HeightOrder::Descending {
                            expected.reverse();
                        }
                        assert_blocks_match_expected(&first, &expected, selection, order);
                        assert_blocks_match_expected(&second, &expected, selection, order);
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn synthesized_empty_nullifier_block_matches_prost_at_zero_and_nonzero_fields() -> TestOutcome {
    let mut synthesized = Vec::new();
    for (height_val, block_time) in [(0_u32, 0_u32), (0, 42), (1, 0), (3_593_347, 1_732_000_000)] {
        synthesized.clear();
        let hash_wire = encode_internal_block_hash(block_hash(height_val.saturating_add(1)));
        let prev_wire = encode_internal_block_hash(block_hash(height_val));
        synthesize_empty_nullifier_block(
            BlockHeight::new(height_val),
            &hash_wire,
            &prev_wire,
            block_time,
            &mut synthesized,
        )?;
        let expected = EncodedBlock::encode(&lightwalletd::CompactBlock {
            height: u64::from(height_val),
            hash: hash_wire.to_vec(),
            prev_hash: prev_wire.to_vec(),
            time: block_time,
            header: Vec::new(),
            vtx: Vec::new(),
            chain_metadata: Some(lightwalletd::ChainMetadata::default()),
        })?;
        assert_eq!(
            synthesized.as_slice(),
            expected.as_length_prefixed().as_ref()
        );
    }
    Ok(())
}

fn single_orchard_block(
    height_val: u32,
    seed: u8,
    action_count: usize,
) -> TestOutcome<CompactBlockArtifact> {
    let tx = make_tx(
        0,
        seed,
        CompactTransactionData {
            orchard_actions: vec![shielded_action(seed); action_count],
            ..CompactTransactionData::default()
        },
    );
    Ok(CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(height_val), block_hash(height_val)),
        block_hash(height_val - 1),
        1_700_000_001,
        vec![tx],
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: u32::try_from(action_count)?,
            ironwood_commitment_tree_size: 0,
        },
    )?)
}

#[test]
fn large_block_above_2_mib_succeeds_and_above_16_mib_is_rejected() -> TestOutcome {
    let dir = TempDir::new()?;
    let block = single_orchard_block(1, 99, 16_000)?;
    let (path, identity) = write_segment(dir.path(), "large-ok", std::slice::from_ref(&block))?;
    let reader = SettledShieldedSegmentReader::open(
        &path,
        &identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let read_back = reader.read_default_range(
        BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1)),
        HeightOrder::Ascending,
    )?;
    assert_eq!(read_back.len(), 1);
    assert!(read_back[0].payload_len() > 2 * 1024 * 1024);
    assert_eq!(
        read_back[0].as_length_prefixed(),
        reference_encode_block(&block, ShieldedProjectionSelection::DEFAULT_SHIELDED)?
            .as_length_prefixed()
    );

    let oversized_block = single_orchard_block(1, 100, 115_000)?;
    let oversized_target = dir.path().join("oversized");
    let staged = NamedTempFile::new_in(dir.path())?;
    let mut writer = SettledShieldedSegmentWriter::create(
        &oversized_target,
        staged,
        ShieldedSegmentIdentity::span(
            TEST_NETWORK,
            TEST_SCHEMA,
            BlockHeight::new(1),
            BlockId::new(BlockHeight::new(1), block_hash(1)),
        ),
    )?;
    let outcome = writer.push(&oversized_block, 20_000_000);
    assert!(matches!(
        outcome,
        Err(SettledShieldedSegmentError::PayloadLimitExceeded {
            max_bytes: SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES,
            ..
        })
    ));
    Ok(())
}

#[test]
fn writer_rejects_broken_parent_and_incomplete_finish() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let target = dir.path().join("broken");
    let staged = NamedTempFile::new_in(dir.path())?;
    let mut writer = SettledShieldedSegmentWriter::create(
        &target,
        staged,
        ShieldedSegmentIdentity::span(
            TEST_NETWORK,
            TEST_SCHEMA,
            BlockHeight::new(1),
            BlockId::new(BlockHeight::new(3), block_hash(3)),
        ),
    )?;
    writer.push(&blocks[0], 128)?;

    let bad_parent = CompactBlockArtifact::empty(
        BlockId::new(BlockHeight::new(2), block_hash(2)),
        block_hash(99),
        10,
        CompactChainMetadata {
            sapling_commitment_tree_size: 0,
            orchard_commitment_tree_size: 0,
            ironwood_commitment_tree_size: 0,
        },
    );
    assert!(matches!(
        writer.push(&bad_parent, 128),
        Err(SettledShieldedSegmentError::ChainLinkage(_))
    ));
    assert!(matches!(
        writer.finish(),
        Err(SettledShieldedSegmentError::ChainLinkage(_))
    ));
    Ok(())
}

fn corrupt_and_open(
    dir: &Path,
    original: &[u8],
    identity: ShieldedSegmentIdentity,
    mutator: &dyn Fn(&mut Vec<u8>),
) -> SettledShieldedSegmentError {
    let mut mutated = original.to_vec();
    mutator(&mut mutated);
    let tampered_path = dir.join("tampered_case");
    if let Err(err) = fs::write(&tampered_path, &mutated) {
        return SettledShieldedSegmentError::Io(err);
    }
    let altered_identity = ShieldedSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    match SettledShieldedSegmentReader::open(
        &tampered_path,
        &altered_identity,
        SettledShieldedSegmentOpenOptions::default(),
    ) {
        Ok(_) => SettledShieldedSegmentError::InvalidFormat("expected tamper refusal".to_owned()),
        Err(err) => err,
    }
}

fn pin_with_digests(
    base: ShieldedSegmentIdentity,
    mutated: &[u8],
) -> TestOutcome<ShieldedSegmentIdentity> {
    let (sha256, index_sha256) = recompute_portable_digests(mutated)?;
    Ok(ShieldedSegmentIdentity {
        sha256,
        index_sha256,
        ..base
    })
}

#[test]
fn reader_fails_closed_on_tampered_header_and_index() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "tamper-header", &blocks)?;
    let original = fs::read(&path)?;
    let index_offset = usize::try_from(u64::from_be_bytes(original[96..104].try_into()?))?;

    for mutator in [
        (&|buf: &mut Vec<u8>| buf[0] ^= 0xff) as &dyn Fn(&mut Vec<u8>),
        &|buf| buf[14..16].copy_from_slice(&SEGMENT_FLAG_RESERVED_SHIFTED_EPK.to_be_bytes()),
        &|buf| buf[120] = 1,
    ] {
        assert!(matches!(
            corrupt_and_open(dir.path(), &original, identity, mutator),
            SettledShieldedSegmentError::InvalidFormat(_)
        ));
    }

    for (offset, byte_val) in [
        (index_offset + 78, 1_u8),
        (index_offset + 77, 0x02),
        (index_offset + 76, 0x80),
    ] {
        let mut mutated = original.clone();
        mutated[offset] = byte_val;
        let pin = pin_with_digests(identity, &mutated)?;
        assert!(matches!(
            corrupt_and_open(dir.path(), &original, pin, &|buf| {
                buf[offset] = byte_val;
                let end = buf.len() - TRAILER_LEN;
                buf[end..].copy_from_slice(&pin.sha256);
            }),
            SettledShieldedSegmentError::InvalidFormat(_)
        ));
    }
    Ok(())
}

fn assert_corrupt_slab_rejected(
    dir: &Path,
    name: &str,
    tampered: &[u8],
    portable_id: &ShieldedSegmentIdentity,
    expect_integrity: bool,
) -> TestOutcome {
    let tampered_path = dir.join(name);
    fs::write(&tampered_path, tampered)?;
    let full_err = SettledShieldedSegmentReader::open(
        &tampered_path,
        portable_id,
        SettledShieldedSegmentOpenOptions {
            require_fs_verity: false,
            verify_full_payload: true,
        },
    );
    let cold_reader = SettledShieldedSegmentReader::open(
        &tampered_path,
        portable_id,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let read_err = cold_reader.read_default_range(
        BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1)),
        HeightOrder::Ascending,
    );
    if expect_integrity {
        assert!(matches!(
            full_err,
            Err(SettledShieldedSegmentError::PayloadIntegrity(_))
        ));
        assert!(matches!(
            read_err,
            Err(SettledShieldedSegmentError::PayloadIntegrity(_))
        ));
    } else {
        assert!(matches!(
            full_err,
            Err(SettledShieldedSegmentError::InvalidFormat(_))
        ));
        assert!(matches!(
            read_err,
            Err(SettledShieldedSegmentError::InvalidFormat(_))
        ));
    }
    Ok(())
}

#[test]
fn reader_fails_closed_on_tampered_slab_and_trailer() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "tamper-slab", &blocks)?;
    let original = fs::read(&path)?;
    let index_offset = usize::try_from(u64::from_be_bytes(original[96..104].try_into()?))?;
    let portable_id = ShieldedSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };

    let mut bad_prefix = original.clone();
    bad_prefix[HEADER_LEN] = 1;
    assert_corrupt_slab_rejected(
        dir.path(),
        "bad-slab-prefix",
        &bad_prefix,
        &portable_id,
        false,
    )?;

    let mut bad_slab_payload = original.clone();
    bad_slab_payload[HEADER_LEN + 10] ^= 0x01;
    assert_corrupt_slab_rejected(
        dir.path(),
        "bad-slab-payload",
        &bad_slab_payload,
        &portable_id,
        true,
    )?;

    assert!(matches!(
        corrupt_and_open(dir.path(), &original, identity, &|buf| {
            let last = buf.len() - 1;
            buf[last] ^= 0x01;
        }),
        SettledShieldedSegmentError::PayloadIntegrity(_)
    ));
    assert!(matches!(
        corrupt_and_open(dir.path(), &original, identity, &|buf| buf
            [index_offset + ROW_INDEX_ENTRY_LEN] ^=
            0x01),
        SettledShieldedSegmentError::PayloadIntegrity(_)
    ));

    let mut stale_idx_pin = original.clone();
    stale_idx_pin[index_offset + ROW_INDEX_ENTRY_LEN] ^= 0x01;
    let (recomputed_sha, _) = recompute_portable_digests(&stale_idx_pin)?;
    let stale_id = ShieldedSegmentIdentity {
        sha256: recomputed_sha,
        ..identity
    };
    assert!(matches!(
        corrupt_and_open(dir.path(), &original, stale_id, &|buf| {
            buf[index_offset + ROW_INDEX_ENTRY_LEN] ^= 0x01;
            let end = buf.len() - TRAILER_LEN;
            buf[end..].copy_from_slice(&recomputed_sha);
        }),
        SettledShieldedSegmentError::PayloadIntegrity(_)
    ));

    let last_row_offset = index_offset + (blocks.len() - 1) * ROW_INDEX_ENTRY_LEN;
    let mut bad_last_hash = original.clone();
    bad_last_hash[last_row_offset] ^= 0x01;
    let last_pin = pin_with_digests(identity, &bad_last_hash)?;
    assert!(matches!(
        corrupt_and_open(dir.path(), &original, last_pin, &|buf| {
            buf[last_row_offset] ^= 0x01;
            let end = buf.len() - TRAILER_LEN;
            buf[end..].copy_from_slice(&last_pin.sha256);
        }),
        SettledShieldedSegmentError::ChainLinkage(_)
    ));
    Ok(())
}

#[test]
fn reader_rejects_mismatched_interior_row_hash_on_read_and_caches_invalid_state() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "bad-interior", &blocks)?;
    let mut mutated = fs::read(&path)?;
    let index_offset = usize::try_from(u64::from_be_bytes(mutated[96..104].try_into()?))?;
    // Corrupt Orchard-only block height = 3 (row index 2), which has no Sapling bits.
    mutated[index_offset + 2 * ROW_INDEX_ENTRY_LEN] ^= 0x01;
    let (forged_sha, forged_index_sha) = recompute_portable_digests(&mutated)?;
    let body_end = mutated.len() - TRAILER_LEN;
    mutated[body_end..].copy_from_slice(&forged_sha);
    let forged_path = dir.path().join("forged-interior");
    fs::write(&forged_path, &mutated)?;
    let forged_identity = ShieldedSegmentIdentity {
        sha256: forged_sha,
        index_sha256: forged_index_sha,
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledShieldedSegmentReader::open(
        &forged_path,
        &forged_identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(3));
    assert!(matches!(
        reader.read_default_range(range, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    // Subsequent full read hits sticky VALIDATION_INVALID state and still fails closed.
    assert!(matches!(
        reader.read_default_range(range, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    // Non-matching nullifier projection must also fail closed instead of synthesizing from index.
    let sapling_nullifiers_only = ShieldedProjectionSelection::new(true, false, false, true);
    assert!(matches!(
        reader.read_projected_range(range, HeightOrder::Ascending, sapling_nullifiers_only),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn reader_detects_post_admission_tamper_verity_guard_and_transparent_wire() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "portable", &blocks)?;
    let original = fs::read(&path)?;
    let portable_path = dir.path().join("portable_post_tamper");
    fs::write(&portable_path, &original)?;
    let portable_identity = ShieldedSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledShieldedSegmentReader::open(
        &portable_path,
        &portable_identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    assert!(!reader.kernel_verified());

    let mut writer_file = OpenOptions::new().write(true).open(&portable_path)?;
    writer_file.seek(SeekFrom::Start((HEADER_LEN + 8) as u64))?;
    writer_file.write_all(&[original[HEADER_LEN + 8] ^ 0x55])?;
    writer_file.sync_all()?;
    drop(writer_file);
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6));
    assert!(matches!(
        reader.read_default_range(range, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));

    let verity_required = SettledShieldedSegmentReader::open(
        &portable_path,
        &portable_identity,
        SettledShieldedSegmentOpenOptions {
            require_fs_verity: true,
            ..SettledShieldedSegmentOpenOptions::default()
        },
    );
    assert!(matches!(
        verity_required,
        Err(SettledShieldedSegmentError::FsVerityRequired)
    ));
    assert!(matches!(
        reader.read_default_range(
            BlockHeightRange::inclusive(BlockHeight::new(5), BlockHeight::new(2)),
            HeightOrder::Ascending,
        ),
        Err(SettledShieldedSegmentError::HeightOutOfRange { .. })
    ));
    assert!(matches!(
        reader.read_default_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(7)),
            HeightOrder::Ascending,
        ),
        Err(SettledShieldedSegmentError::HeightOutOfRange { .. })
    ));

    let invalid_tx_block = lightwalletd::CompactBlock {
        height: 1,
        hash: encode_internal_block_hash(block_hash(1)).to_vec(),
        prev_hash: encode_internal_block_hash(block_hash(0)).to_vec(),
        vtx: vec![lightwalletd::CompactTx {
            index: 0,
            txid: encode_internal_transaction_id(txid(1)).to_vec(),
            vout: vec![lightwalletd::TxOut {
                value: 1,
                script_pub_key: vec![0x51],
            }],
            ..lightwalletd::CompactTx::default()
        }],
        ..lightwalletd::CompactBlock::default()
    };
    let mut projected = Vec::new();
    assert!(matches!(
        project_shielded_block_wire(
            &invalid_tx_block.encode_to_vec(),
            ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
            &mut projected,
        ),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn validate_entries_rejects_oversized_frame_on_protected_path() -> TestOutcome {
    let max_payload = u32::try_from(SETTLED_SHIELDED_SEGMENT_MAX_PAYLOAD_BYTES)?;
    let max_frame = FRAME_PREFIX_U32 + max_payload;
    let last_hash_wire = encode_internal_block_hash(block_hash(1));
    let header_len_u64 = u64::try_from(HEADER_LEN)?;
    let header = ShieldedSegmentHeader {
        network_tag: TEST_NETWORK.id(),
        artifact_schema_version: TEST_SCHEMA,
        segment_flags: 0,
        start_height: 1,
        block_count: 1,
        first_prev_hash_wire: encode_internal_block_hash(block_hash(0)),
        last_block_hash_wire: last_hash_wire,
        slab_offset: header_len_u64,
        index_offset: header_len_u64 + u64::from(max_frame),
    };
    let entry = DecodedRowIndexEntry {
        block_hash_wire: last_hash_wire,
        payload_sha256: [0x11; 32],
        block_time: 1_700_000_001,
        slab_rel_offset: 0,
        native_bytes: 128,
        pool_presence: PoolPresenceMask::EMPTY,
        row_flags: 0,
    };

    let exact_max_rows = validate_entries(&[entry], max_frame, &header)?;
    assert_eq!(exact_max_rows.len(), 1);

    assert!(matches!(
        validate_entries(&[entry], max_frame + 1, &header),
        Err(SettledShieldedSegmentError::InvalidFormat(_))
    ));
    assert!(matches!(
        validate_entries(&[entry], u32::MAX, &header),
        Err(SettledShieldedSegmentError::InvalidFormat(_))
    ));
    Ok(())
}

fn encode_length_delimited_field(tag: u32, payload: &[u8], out: &mut Vec<u8>) -> TestOutcome {
    encode_key(tag, WireType::LengthDelimited, out);
    encode_varint(u64::try_from(payload.len())?, out);
    out.extend_from_slice(payload);
    Ok(())
}

fn build_raw_block_header_fields(out: &mut Vec<u8>) -> TestOutcome {
    encode_key(2, WireType::Varint, out);
    encode_varint(1, out);
    let hash_wire = encode_internal_block_hash(block_hash(1));
    let prev_wire = encode_internal_block_hash(block_hash(0));
    encode_length_delimited_field(3, &hash_wire, out)?;
    encode_length_delimited_field(4, &prev_wire, out)?;
    encode_key(5, WireType::Varint, out);
    encode_varint(1_700_000_001, out);
    Ok(())
}

fn build_raw_block_with_tx_field(tx_tag: u32, nested_bytes: &[u8]) -> TestOutcome<Vec<u8>> {
    let mut tx_wire = Vec::new();
    encode_key(1, WireType::Varint, &mut tx_wire);
    encode_varint(0, &mut tx_wire);
    encode_length_delimited_field(2, &encode_internal_transaction_id(txid(1)), &mut tx_wire)?;
    encode_length_delimited_field(tx_tag, nested_bytes, &mut tx_wire)?;

    let mut block_wire = Vec::new();
    build_raw_block_header_fields(&mut block_wire)?;
    encode_length_delimited_field(7, &tx_wire, &mut block_wire)?;
    encode_length_delimited_field(8, &[], &mut block_wire)?;
    Ok(block_wire)
}

fn build_raw_block_with_chain_metadata(meta_bytes: &[u8]) -> TestOutcome<Vec<u8>> {
    let mut block_wire = Vec::new();
    build_raw_block_header_fields(&mut block_wire)?;
    encode_length_delimited_field(8, meta_bytes, &mut block_wire)?;
    Ok(block_wire)
}

fn open_raw_portable_segment(
    dir: &Path,
    name: &str,
    payload: &[u8],
) -> TestOutcome<SettledShieldedSegmentReader> {
    let all_pools = PoolPresenceMask::SAPLING_SPEND
        .union(PoolPresenceMask::SAPLING_OUTPUT)
        .union(PoolPresenceMask::ORCHARD)
        .union(PoolPresenceMask::IRONWOOD);
    open_raw_portable_segment_with_presence(dir, name, payload, all_pools)
}

fn open_raw_portable_segment_with_presence(
    dir: &Path,
    name: &str,
    payload: &[u8],
    pool_presence: PoolPresenceMask,
) -> TestOutcome<SettledShieldedSegmentReader> {
    let prefix = encode_frame_prefix(payload.len())?;
    let frame_len = u64::try_from(FRAME_PREFIX_LEN + payload.len())?;
    let header_len_u64 = u64::try_from(HEADER_LEN)?;
    let index_offset = header_len_u64 + frame_len;
    let first_prev_hash_wire = encode_internal_block_hash(block_hash(0));
    let end_id = BlockId::new(BlockHeight::new(1), block_hash(1));
    let last_block_hash_wire = encode_internal_block_hash(end_id.hash);
    let header = ShieldedSegmentHeader {
        network_tag: TEST_NETWORK.id(),
        artifact_schema_version: TEST_SCHEMA,
        segment_flags: 0,
        start_height: 1,
        block_count: 1,
        first_prev_hash_wire,
        last_block_hash_wire,
        slab_offset: header_len_u64,
        index_offset,
    };
    let entry = DecodedRowIndexEntry {
        block_hash_wire: last_block_hash_wire,
        payload_sha256: Sha256::digest(payload).into(),
        block_time: 1_700_000_001,
        slab_rel_offset: 0,
        native_bytes: 128,
        pool_presence,
        row_flags: 0,
    };
    let mut segment_bytes = Vec::with_capacity(
        HEADER_LEN + FRAME_PREFIX_LEN + payload.len() + ROW_INDEX_ENTRY_LEN + TRAILER_LEN,
    );
    segment_bytes.extend_from_slice(&header.encode());
    segment_bytes.extend_from_slice(&prefix);
    segment_bytes.extend_from_slice(payload);
    segment_bytes.extend_from_slice(&entry.encode());
    let trailer: [u8; TRAILER_LEN] = Sha256::digest(&segment_bytes).into();
    segment_bytes.extend_from_slice(&trailer);
    let (_, index_sha256) = recompute_portable_digests(&segment_bytes)?;

    let file_path = dir.join(name);
    fs::write(&file_path, &segment_bytes)?;
    let identity = ShieldedSegmentIdentity {
        sha256: trailer,
        index_sha256,
        ..ShieldedSegmentIdentity::span(TEST_NETWORK, TEST_SCHEMA, BlockHeight::new(1), end_id)
    };
    Ok(SettledShieldedSegmentReader::open(
        &file_path,
        &identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?)
}

fn assert_malformed_nested_wire_rejected(dir: &Path, label: &str, payload: &[u8]) -> TestOutcome {
    let mut projected = Vec::new();
    let selections = [
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
        ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
        ShieldedProjectionSelection::new(false, false, false, false),
    ];
    for selection in selections {
        assert!(
            matches!(
                project_shielded_block_wire(payload, selection, &mut projected),
                Err(SettledShieldedSegmentError::PayloadIntegrity(_))
            ),
            "projection accepted malformed {label} for {selection:?}"
        );
    }
    let reader = open_raw_portable_segment(dir, label, payload)?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1));
    assert!(
        matches!(
            reader.read_default_range(range, HeightOrder::Ascending),
            Err(SettledShieldedSegmentError::PayloadIntegrity(_))
        ),
        "reader validation accepted malformed {label}"
    );
    Ok(())
}

#[test]
fn nested_wire_type_validators_reject_malformed_wire_types() -> TestOutcome {
    let dir = TempDir::new()?;
    let malformed_cases = [
        (
            "spend_tag1_varint",
            build_raw_block_with_tx_field(4, &[0x08, 0x01])?,
        ),
        (
            "output_tag1_varint",
            build_raw_block_with_tx_field(5, &[0x08, 0x01])?,
        ),
        (
            "output_tag2_varint",
            build_raw_block_with_tx_field(5, &[0x10, 0x01])?,
        ),
        (
            "output_tag3_varint",
            build_raw_block_with_tx_field(5, &[0x18, 0x01])?,
        ),
        (
            "orchard_tag1_varint",
            build_raw_block_with_tx_field(6, &[0x08, 0x01])?,
        ),
        (
            "orchard_tag2_varint",
            build_raw_block_with_tx_field(6, &[0x10, 0x01])?,
        ),
        (
            "orchard_tag3_varint",
            build_raw_block_with_tx_field(6, &[0x18, 0x01])?,
        ),
        (
            "orchard_tag4_varint",
            build_raw_block_with_tx_field(6, &[0x20, 0x01])?,
        ),
        (
            "ironwood_tag1_varint",
            build_raw_block_with_tx_field(9, &[0x08, 0x01])?,
        ),
        (
            "ironwood_tag2_varint",
            build_raw_block_with_tx_field(9, &[0x10, 0x01])?,
        ),
        (
            "ironwood_tag3_varint",
            build_raw_block_with_tx_field(9, &[0x18, 0x01])?,
        ),
        (
            "ironwood_tag4_varint",
            build_raw_block_with_tx_field(9, &[0x20, 0x01])?,
        ),
        (
            "meta_tag1_bytes",
            build_raw_block_with_chain_metadata(&[0x0a, 0x01, 0x01])?,
        ),
        (
            "meta_tag2_bytes",
            build_raw_block_with_chain_metadata(&[0x12, 0x01, 0x01])?,
        ),
        (
            "meta_tag3_bytes",
            build_raw_block_with_chain_metadata(&[0x1a, 0x01, 0x01])?,
        ),
    ];
    for (label, payload) in &malformed_cases {
        assert_malformed_nested_wire_rejected(dir.path(), label, payload)?;
    }
    Ok(())
}

#[test]
fn nested_wire_type_validators_preserve_skip_wire_field_for_unknown_tags() -> TestOutcome {
    let dir = TempDir::new()?;
    let mut unknown_suffix = Vec::new();
    encode_key(99, WireType::Varint, &mut unknown_suffix);
    encode_varint(7, &mut unknown_suffix);

    let mut spend_wire = lightwalletd::CompactSaplingSpend { nf: vec![0x11; 32] }.encode_to_vec();
    spend_wire.extend_from_slice(&unknown_suffix);

    let mut output_wire = lightwalletd::CompactSaplingOutput {
        cmu: vec![0x21; 32],
        ephemeral_key: vec![0x22; 32],
        ciphertext: vec![0x23; 52],
    }
    .encode_to_vec();
    output_wire.extend_from_slice(&unknown_suffix);

    let mut action_wire = lightwalletd::CompactOrchardAction {
        nullifier: vec![0x31; 32],
        cmx: vec![0x32; 32],
        ephemeral_key: vec![0x33; 32],
        ciphertext: vec![0x34; 52],
    }
    .encode_to_vec();
    action_wire.extend_from_slice(&unknown_suffix);

    let mut meta_wire = lightwalletd::ChainMetadata {
        sapling_commitment_tree_size: 1,
        orchard_commitment_tree_size: 2,
        ironwood_commitment_tree_size: 3,
    }
    .encode_to_vec();
    meta_wire.extend_from_slice(&unknown_suffix);

    let mut tx_wire = Vec::new();
    encode_key(1, WireType::Varint, &mut tx_wire);
    encode_varint(0, &mut tx_wire);
    encode_length_delimited_field(2, &encode_internal_transaction_id(txid(2)), &mut tx_wire)?;
    encode_length_delimited_field(4, &spend_wire, &mut tx_wire)?;
    encode_length_delimited_field(5, &output_wire, &mut tx_wire)?;
    encode_length_delimited_field(6, &action_wire, &mut tx_wire)?;
    encode_length_delimited_field(9, &action_wire, &mut tx_wire)?;

    let mut payload = Vec::new();
    build_raw_block_header_fields(&mut payload)?;
    encode_length_delimited_field(7, &tx_wire, &mut payload)?;
    encode_length_delimited_field(8, &meta_wire, &mut payload)?;

    let mut projected = Vec::new();
    for selection in [
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
        ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
        ShieldedProjectionSelection::new(false, false, false, false),
    ] {
        project_shielded_block_wire(&payload, selection, &mut projected)?;
        assert_ne!(projected, [] as [u8; 0]);
    }
    let reader = open_raw_portable_segment(dir.path(), "unknown-nested-tags", &payload)?;
    let range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1));
    assert_eq!(
        reader
            .read_default_range(range, HeightOrder::Ascending)?
            .len(),
        1
    );
    Ok(())
}

fn row_slab_offset(bytes: &[u8], index_offset: usize, row_idx: usize) -> TestOutcome<usize> {
    let offset_start = index_offset + row_idx * ROW_INDEX_ENTRY_LEN + 68;
    let raw: [u8; 4] = bytes[offset_start..offset_start + 4].try_into()?;
    Ok(usize::try_from(u32::from_be_bytes(raw))?)
}

#[test]
fn indexed_block_mark_valid_preserves_invalid_state() {
    let entry = DecodedRowIndexEntry {
        block_hash_wire: encode_internal_block_hash(block_hash(1)),
        payload_sha256: [0x22; 32],
        block_time: 1_700_000_001,
        slab_rel_offset: 0,
        native_bytes: 64,
        pool_presence: PoolPresenceMask::EMPTY,
        row_flags: 0,
    };

    let valid_row = CompactShieldedRow::from_entry(&entry);
    assert_eq!(valid_row.validation_state(), VALIDATION_UNVALIDATED);
    valid_row.mark_valid();
    assert_eq!(valid_row.validation_state(), VALIDATION_VALID);

    let invalid_row = CompactShieldedRow::from_entry(&entry);
    invalid_row.mark_invalid();
    invalid_row.mark_valid();
    assert_eq!(invalid_row.validation_state(), VALIDATION_INVALID);
}

#[test]
fn post_admission_frame_and_digest_failures_latch_invalid_state() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "portable-latch", &blocks)?;
    let original = fs::read(&path)?;
    let index_offset = usize::try_from(u64::from_be_bytes(original[96..104].try_into()?))?;
    let portable_path = dir.path().join("portable_latch_tamper");
    fs::write(&portable_path, &original)?;
    let portable_identity = ShieldedSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledShieldedSegmentReader::open(
        &portable_path,
        &portable_identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;

    let range_3 = BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(3));
    assert_eq!(
        reader
            .read_default_range(range_3, HeightOrder::Ascending)?
            .len(),
        1
    );

    let row3_payload_pos =
        HEADER_LEN + row_slab_offset(&original, index_offset, 2)? + FRAME_PREFIX_LEN;
    let row4_prefix_pos = HEADER_LEN + row_slab_offset(&original, index_offset, 3)?;
    let mut writer_file = OpenOptions::new().write(true).open(&portable_path)?;
    writer_file.seek(SeekFrom::Start(u64::try_from(row3_payload_pos)?))?;
    writer_file.write_all(&[original[row3_payload_pos] ^ 0x55])?;
    writer_file.seek(SeekFrom::Start(u64::try_from(row4_prefix_pos)?))?;
    writer_file.write_all(&[1_u8])?;
    writer_file.sync_all()?;
    drop(writer_file);

    assert!(matches!(
        reader.read_default_range(range_3, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    let non_matching_ironwood = ShieldedProjectionSelection::new(false, false, true, true);
    assert!(matches!(
        reader.read_projected_range(range_3, HeightOrder::Ascending, non_matching_ironwood),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));

    let range_4 = BlockHeightRange::inclusive(BlockHeight::new(4), BlockHeight::new(4));
    assert!(matches!(
        reader.read_default_range(range_4, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::InvalidFormat(_))
    ));
    let non_matching_orchard = ShieldedProjectionSelection::new(false, true, false, true);
    assert!(matches!(
        reader.read_projected_range(range_4, HeightOrder::Ascending, non_matching_orchard),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

fn open_forged_pool_presence_segment(
    dir: &Path,
    name: &str,
    blocks: &[CompactBlockArtifact],
    row_idx: usize,
    forged_mask: PoolPresenceMask,
) -> TestOutcome<SettledShieldedSegmentReader> {
    let (src_path, identity) = write_segment(dir, &format!("{name}-src"), blocks)?;
    let mut mutated = fs::read(&src_path)?;
    let index_offset = usize::try_from(u64::from_be_bytes(mutated[96..104].try_into()?))?;
    mutated[index_offset + row_idx * ROW_INDEX_ENTRY_LEN + 76] = forged_mask.bits();
    let (forged_sha, forged_index_sha) = recompute_portable_digests(&mutated)?;
    let body_end = mutated.len() - TRAILER_LEN;
    mutated[body_end..].copy_from_slice(&forged_sha);
    let forged_path = dir.join(name);
    fs::write(&forged_path, &mutated)?;
    let forged_identity = ShieldedSegmentIdentity {
        sha256: forged_sha,
        index_sha256: forged_index_sha,
        fs_verity_sha256: None,
        ..identity
    };
    Ok(SettledShieldedSegmentReader::open(
        &forged_path,
        &forged_identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?)
}

#[test]
fn reader_rejects_empty_pool_presence_mask_for_orchard_block() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let range_3 = BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(3));

    // With O(index) open, full validation (via verify_full_payload or read_default_range) detects the
    // forged index mask against the protobuf wire payload and latches sticky VALIDATION_INVALID so
    // subsequent DEFAULT_NULLIFIERS reads also fail closed.
    let reader_first = open_forged_pool_presence_segment(
        dir.path(),
        "empty-mask-first",
        &blocks,
        2,
        PoolPresenceMask::EMPTY,
    )?;
    assert!(matches!(
        reader_first.verify_full_payload(),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_first.read_projected_range(
            range_3,
            HeightOrder::Ascending,
            ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
        ),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_first.read_default_range(range_3, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));

    let reader_second = open_forged_pool_presence_segment(
        dir.path(),
        "empty-mask-second",
        &blocks,
        2,
        PoolPresenceMask::EMPTY,
    )?;
    assert!(matches!(
        reader_second.read_default_range(range_3, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader_second.read_projected_range(
            range_3,
            HeightOrder::Ascending,
            ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
        ),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn reader_rejects_understated_pool_presence_mask_on_zero_copy_projection() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let sapling_only_mask = PoolPresenceMask::SAPLING_SPEND.union(PoolPresenceMask::SAPLING_OUTPUT);
    let reader = open_forged_pool_presence_segment(
        dir.path(),
        "sapling-only-mask",
        &blocks,
        1,
        sapling_only_mask,
    )?;
    let range_2 = BlockHeightRange::inclusive(BlockHeight::new(2), BlockHeight::new(2));
    let sapling_full = ShieldedProjectionSelection::new(true, false, false, false);
    assert!(matches!(
        reader.read_projected_range(range_2, HeightOrder::Ascending, sapling_full),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_default_range(range_2, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    let ironwood_nullifiers = ShieldedProjectionSelection::new(false, false, true, true);
    assert!(matches!(
        reader.read_projected_range(range_2, HeightOrder::Ascending, ironwood_nullifiers),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn reader_latches_invalid_state_on_slab_read_truncation() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "trunc-src", &blocks)?;
    let original = fs::read(&path)?;
    let index_offset = usize::try_from(u64::from_be_bytes(original[96..104].try_into()?))?;
    let portable_path = dir.path().join("trunc_portable");
    fs::write(&portable_path, &original)?;
    let portable_identity = ShieldedSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledShieldedSegmentReader::open(
        &portable_path,
        &portable_identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;

    let range_3 = BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(3));
    assert_eq!(
        reader
            .read_default_range(range_3, HeightOrder::Ascending)?
            .len(),
        1
    );

    let truncate_offset = HEADER_LEN + row_slab_offset(&original, index_offset, 2)? + 1;
    let truncate_len = u64::try_from(truncate_offset)?;
    OpenOptions::new()
        .write(true)
        .open(&portable_path)?
        .set_len(truncate_len)?;

    assert!(matches!(
        reader.read_default_range(range_3, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::Io(_))
    ));
    let sapling_nullifiers = ShieldedProjectionSelection::new(true, false, false, true);
    assert!(matches!(
        reader.read_projected_range(range_3, HeightOrder::Ascending, sapling_nullifiers),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn reader_rejects_mismatched_block_time_on_consecutive_unmatched_nullifier_reads() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (src_path, identity) = write_segment(dir.path(), "bad-time-src", &blocks)?;
    let mut mutated = fs::read(&src_path)?;
    let index_offset = usize::try_from(u64::from_be_bytes(mutated[96..104].try_into()?))?;
    // Mutate Orchard-only height = 3 (row index 2) indexed block_time (bytes 64..68).
    mutated[index_offset + 2 * ROW_INDEX_ENTRY_LEN + 67] ^= 0x01;
    let (forged_sha, forged_index_sha) = recompute_portable_digests(&mutated)?;
    let body_end = mutated.len() - TRAILER_LEN;
    mutated[body_end..].copy_from_slice(&forged_sha);
    let forged_path = dir.path().join("forged-block-time");
    fs::write(&forged_path, &mutated)?;
    let forged_identity = ShieldedSegmentIdentity {
        sha256: forged_sha,
        index_sha256: forged_index_sha,
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledShieldedSegmentReader::open(
        &forged_path,
        &forged_identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let range_3 = BlockHeightRange::inclusive(BlockHeight::new(3), BlockHeight::new(3));
    assert!(matches!(
        reader.read_default_range(range_3, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    let sapling_nullifiers = ShieldedProjectionSelection::new(true, false, false, true);
    assert!(matches!(
        reader.read_projected_range(range_3, HeightOrder::Ascending, sapling_nullifiers),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_projected_range(range_3, HeightOrder::Ascending, sapling_nullifiers),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));

    let mut omitted_time_payload = Vec::new();
    encode_key(2, WireType::Varint, &mut omitted_time_payload);
    encode_varint(1, &mut omitted_time_payload);
    let hash_wire = encode_internal_block_hash(block_hash(1));
    let prev_wire = encode_internal_block_hash(block_hash(0));
    encode_length_delimited_field(3, &hash_wire, &mut omitted_time_payload)?;
    encode_length_delimited_field(4, &prev_wire, &mut omitted_time_payload)?;
    encode_length_delimited_field(8, &[], &mut omitted_time_payload)?;
    let omitted_reader = open_raw_portable_segment_with_presence(
        dir.path(),
        "omitted-time",
        &omitted_time_payload,
        PoolPresenceMask::EMPTY,
    )?;
    let range_1 = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1));
    assert!(matches!(
        omitted_reader.read_default_range(range_1, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn reader_rejects_nonempty_compact_block_header() -> TestOutcome {
    let dir = TempDir::new()?;
    let mut header_payload = Vec::new();
    build_raw_block_header_fields(&mut header_payload)?;
    encode_length_delimited_field(6, &[0x01, 0x02], &mut header_payload)?;
    encode_length_delimited_field(8, &[], &mut header_payload)?;
    let reader = open_raw_portable_segment_with_presence(
        dir.path(),
        "nonempty-header",
        &header_payload,
        PoolPresenceMask::EMPTY,
    )?;
    let range_1 = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1));
    assert!(matches!(
        reader.read_default_range(range_1, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn passthrough_wire_check_matches_full_projection_and_rejects_corrupt_tags() -> TestOutcome {
    let blocks = mixed_fixture_blocks()?;
    let empty_shielded =
        reference_encode_block(&blocks[0], ShieldedProjectionSelection::DEFAULT_SHIELDED)?;
    let empty_nullifier =
        reference_encode_block(&blocks[0], ShieldedProjectionSelection::DEFAULT_NULLIFIERS)?;
    let orchard_non_empty =
        reference_encode_block(&blocks[2], ShieldedProjectionSelection::DEFAULT_NULLIFIERS)?;

    let orchard_range = ShieldedProjectionSelection::new(false, true, false, false);
    let orchard_nf = ShieldedProjectionSelection::new(false, true, false, true);

    assert!(is_passthrough_shielded_block_wire(
        &empty_shielded.payload(),
        orchard_range
    )?);
    assert!(!is_passthrough_shielded_block_wire(
        &empty_shielded.payload(),
        orchard_nf
    )?);
    assert!(is_passthrough_shielded_block_wire(
        &empty_nullifier.payload(),
        orchard_nf
    )?);
    assert!(!is_passthrough_shielded_block_wire(
        &orchard_non_empty.payload(),
        orchard_nf
    )?);

    let mut out = Vec::new();
    let mut tx_scratch = Vec::new();
    project_shielded_block_wire_with_scratch(
        &empty_nullifier.payload(),
        orchard_nf,
        &mut out,
        &mut tx_scratch,
    )?;
    assert_eq!(
        out.as_slice(),
        empty_nullifier.as_length_prefixed().as_ref()
    );

    let mut unknown_tag = empty_nullifier.payload().to_vec();
    encode_key(99, WireType::Varint, &mut unknown_tag);
    encode_varint(7, &mut unknown_tag);
    assert!(!is_passthrough_shielded_block_wire(
        &unknown_tag,
        orchard_nf
    )?);

    assert!(!is_passthrough_shielded_block_wire(&[], orchard_range)?);
    assert!(!is_passthrough_shielded_block_wire(&[], orchard_nf)?);
    assert!(matches!(
        is_passthrough_shielded_block_wire(&[0x10, 0x80], orchard_nf),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}

#[test]
fn writer_push_with_supplement_marks_wide_fees_for_reconstruction_only() -> TestOutcome {
    let dir = TempDir::new()?;
    let wide_block = CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(10), block_hash(10)),
        block_hash(9),
        1_700_000_010,
        vec![make_tx(
            0,
            10,
            CompactTransactionData {
                fee_zat: Some(u64::from(u32::MAX) + 42),
                orchard_actions: vec![shielded_action(0x41)],
                ..CompactTransactionData::default()
            },
        )],
        CompactChainMetadata {
            sapling_commitment_tree_size: 10,
            orchard_commitment_tree_size: 20,
            ironwood_commitment_tree_size: 30,
        },
    )?;
    let span = ShieldedSegmentIdentity::span(
        TEST_NETWORK,
        TEST_SCHEMA,
        wide_block.height(),
        BlockId::new(wide_block.height(), wide_block.block_hash()),
    );
    let reject_path = dir.path().join("reject-wide.zshrng02");
    let mut strict_writer = SettledShieldedSegmentWriter::create(
        &reject_path,
        NamedTempFile::new_in(dir.path())?,
        span,
    )?;
    assert!(matches!(
        strict_writer.push(&wide_block, 512),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));

    let sup_path = dir.path().join("wide-sup.zshrng02");
    let mut sup_writer =
        SettledShieldedSegmentWriter::create(&sup_path, NamedTempFile::new_in(dir.path())?, span)?;
    sup_writer.push_with_supplement(&wide_block, 512)?;
    let identity = sup_writer.finish()?;

    let reader = SettledShieldedSegmentReader::open(
        &sup_path,
        &identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let range = BlockHeightRange::inclusive(wide_block.height(), wide_block.height());
    assert!(matches!(
        reader.read_default_range(range, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_projected_range(
            range,
            HeightOrder::Ascending,
            ShieldedProjectionSelection::DEFAULT_NULLIFIERS
        ),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    let reconstructed_frames =
        reader.read_default_range_for_reconstruction(range, HeightOrder::Ascending)?;
    assert_eq!(reconstructed_frames.len(), 1);
    let decoded = lightwalletd::CompactBlock::decode(reconstructed_frames[0].payload().as_ref())?;
    assert_eq!(decoded.vtx.len(), 1);
    assert_eq!(decoded.vtx[0].fee, 0);
    Ok(())
}

fn build_omitted_wide_fee_block(
    seed: u8,
    transparent_outputs: Vec<CompactTransparentOutput>,
) -> Result<CompactBlockArtifact, Box<dyn std::error::Error + Send + Sync>> {
    let h = u32::from(seed);
    let metadata = CompactChainMetadata {
        sapling_commitment_tree_size: 0,
        orchard_commitment_tree_size: 0,
        ironwood_commitment_tree_size: 0,
    };
    let tx_scan = CompactTransactionData {
        fee_zat: Some(u64::from(u32::MAX) + 1),
        transparent_outputs,
        ..CompactTransactionData::default()
    };
    Ok(CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(h), block_hash(h)),
        block_hash(h - 1),
        1_700_000_000 + h,
        vec![make_tx(0, seed, tx_scan)],
        metadata,
    )?)
}

fn single_block_span(blk: &CompactBlockArtifact) -> ShieldedSegmentIdentity {
    ShieldedSegmentIdentity::span(
        TEST_NETWORK,
        TEST_SCHEMA,
        blk.height(),
        BlockId::new(blk.height(), blk.block_hash()),
    )
}

#[test]
fn writer_push_rejects_omitted_transparent_and_componentless_wide_fees() -> TestOutcome {
    let dir = TempDir::new()?;
    let transparent_output = CompactTransparentOutput {
        value_zat: 1_000,
        script_pub_key: vec![0x51],
    };
    let transparent_block = build_omitted_wide_fee_block(11, vec![transparent_output])?;
    let componentless_block = build_omitted_wide_fee_block(12, Vec::new())?;
    for (idx, blk) in [&transparent_block, &componentless_block]
        .iter()
        .enumerate()
    {
        let mut writer = SettledShieldedSegmentWriter::create(
            &dir.path().join(format!("strict-{idx}.zshrng02")),
            NamedTempFile::new_in(dir.path())?,
            single_block_span(blk),
        )?;
        assert!(matches!(
            writer.push(blk, 256),
            Err(SettledShieldedSegmentError::PayloadIntegrity(_))
        ));
    }
    let sup_path = dir.path().join("transparent-wide-sup.zshrng02");
    let mut sup_writer = SettledShieldedSegmentWriter::create(
        &sup_path,
        NamedTempFile::new_in(dir.path())?,
        single_block_span(&transparent_block),
    )?;
    sup_writer.push_with_supplement(&transparent_block, 256)?;
    let identity = sup_writer.finish()?;
    let reader = SettledShieldedSegmentReader::open(
        &sup_path,
        &identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let range = BlockHeightRange::inclusive(transparent_block.height(), transparent_block.height());
    assert!(matches!(
        reader.read_default_range(range, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    let frames = reader.read_default_range_for_reconstruction(range, HeightOrder::Ascending)?;
    assert_eq!(frames.len(), 1);
    Ok(())
}

struct CountingByteSource {
    bytes: Bytes,
    reads: Mutex<Vec<ByteRange>>,
}

impl CountingByteSource {
    fn new(bytes: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            bytes: Bytes::from(bytes),
            reads: Mutex::new(Vec::new()),
        })
    }

    fn take_reads(&self) -> Vec<ByteRange> {
        std::mem::take(&mut *self.reads.lock())
    }
}

impl PositionalByteSource for CountingByteSource {
    fn total_len(&self) -> io::Result<u64> {
        self.bytes.total_len()
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.reads.lock().push(ByteRange::new(offset, buf.len()));
        self.bytes.read_exact_at(offset, buf)
    }

    fn read_vec_at(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.reads.lock().push(ByteRange::new(offset, len));
        self.bytes.read_vec_at(offset, len)
    }
}

struct QueueStream<T>(VecDeque<Result<T, tonic::Status>>);

impl<T: Unpin> tonic::codegen::tokio_stream::Stream for QueueStream<T> {
    type Item = Result<T, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().0.pop_front())
    }
}

struct MockEncodedStreamer {
    use_frames: bool,
    blocks: Vec<EncodedBlock>,
    frames: Vec<EncodedBlockFrame>,
}

#[tonic::async_trait]
impl EncodedBlockRange for MockEncodedStreamer {
    fn supports_encoded_frames(&self) -> bool {
        self.use_frames
    }

    async fn get_encoded_block_range(
        &self,
        _request: tonic::Request<lightwalletd::BlockRange>,
    ) -> Result<tonic::Response<EncodedBlockRangeStream>, tonic::Status> {
        let queued: VecDeque<_> = self.blocks.iter().cloned().map(Ok).collect();
        Ok(tonic::Response::new(Box::pin(QueueStream(queued))))
    }

    async fn get_encoded_block_range_frames(
        &self,
        _request: tonic::Request<lightwalletd::BlockRange>,
    ) -> Result<tonic::Response<EncodedBlockFrameStream>, tonic::Status> {
        let queued: VecDeque<_> = self.frames.iter().cloned().map(Ok).collect();
        Ok(tonic::Response::new(Box::pin(QueueStream(queued))))
    }
}

#[tonic::async_trait]
impl CompactTxStreamer for MockEncodedStreamer {
    type GetBlockRangeStream = tonic::codegen::BoxStream<lightwalletd::CompactBlock>;
    type GetBlockRangeNullifiersStream = tonic::codegen::BoxStream<lightwalletd::CompactBlock>;
    type GetTaddressTxidsStream = tonic::codegen::BoxStream<lightwalletd::RawTransaction>;
    type GetTaddressTransactionsStream = tonic::codegen::BoxStream<lightwalletd::RawTransaction>;
    type GetMempoolTxStream = tonic::codegen::BoxStream<lightwalletd::CompactTx>;
    type GetMempoolStreamStream = tonic::codegen::BoxStream<lightwalletd::RawTransaction>;
    type GetSubtreeRootsStream = tonic::codegen::BoxStream<lightwalletd::SubtreeRoot>;
    type GetAddressUtxosStreamStream =
        tonic::codegen::BoxStream<lightwalletd::GetAddressUtxosReply>;

    fn encoded_block_range(&self) -> Option<&dyn EncodedBlockRange> {
        Some(self)
    }

    async fn get_latest_block(
        &self,
        _request: tonic::Request<lightwalletd::ChainSpec>,
    ) -> Result<tonic::Response<lightwalletd::BlockId>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_block(
        &self,
        _request: tonic::Request<lightwalletd::BlockId>,
    ) -> Result<tonic::Response<lightwalletd::CompactBlock>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_block_nullifiers(
        &self,
        _request: tonic::Request<lightwalletd::BlockId>,
    ) -> Result<tonic::Response<lightwalletd::CompactBlock>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_block_range(
        &self,
        _request: tonic::Request<lightwalletd::BlockRange>,
    ) -> Result<tonic::Response<Self::GetBlockRangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_block_range_nullifiers(
        &self,
        _request: tonic::Request<lightwalletd::BlockRange>,
    ) -> Result<tonic::Response<Self::GetBlockRangeNullifiersStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_transaction(
        &self,
        _request: tonic::Request<lightwalletd::TxFilter>,
    ) -> Result<tonic::Response<lightwalletd::RawTransaction>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn send_transaction(
        &self,
        _request: tonic::Request<lightwalletd::RawTransaction>,
    ) -> Result<tonic::Response<lightwalletd::SendResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_taddress_txids(
        &self,
        _request: tonic::Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<tonic::Response<Self::GetTaddressTxidsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_taddress_transactions(
        &self,
        _request: tonic::Request<lightwalletd::TransparentAddressBlockFilter>,
    ) -> Result<tonic::Response<Self::GetTaddressTransactionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_taddress_balance(
        &self,
        _request: tonic::Request<lightwalletd::AddressList>,
    ) -> Result<tonic::Response<lightwalletd::Balance>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_taddress_balance_stream(
        &self,
        _request: tonic::Request<tonic::Streaming<lightwalletd::Address>>,
    ) -> Result<tonic::Response<lightwalletd::Balance>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_mempool_tx(
        &self,
        _request: tonic::Request<lightwalletd::GetMempoolTxRequest>,
    ) -> Result<tonic::Response<Self::GetMempoolTxStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_mempool_stream(
        &self,
        _request: tonic::Request<lightwalletd::Empty>,
    ) -> Result<tonic::Response<Self::GetMempoolStreamStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_tree_state(
        &self,
        _request: tonic::Request<lightwalletd::BlockId>,
    ) -> Result<tonic::Response<lightwalletd::TreeState>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_latest_tree_state(
        &self,
        _request: tonic::Request<lightwalletd::Empty>,
    ) -> Result<tonic::Response<lightwalletd::TreeState>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_subtree_roots(
        &self,
        _request: tonic::Request<lightwalletd::GetSubtreeRootsArg>,
    ) -> Result<tonic::Response<Self::GetSubtreeRootsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_address_utxos(
        &self,
        _request: tonic::Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<tonic::Response<lightwalletd::GetAddressUtxosReplyList>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_address_utxos_stream(
        &self,
        _request: tonic::Request<lightwalletd::GetAddressUtxosArg>,
    ) -> Result<tonic::Response<Self::GetAddressUtxosStreamStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_lightd_info(
        &self,
        _request: tonic::Request<lightwalletd::Empty>,
    ) -> Result<tonic::Response<lightwalletd::LightdInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn get_standard_fee(
        &self,
        _request: tonic::Request<lightwalletd::Empty>,
    ) -> Result<tonic::Response<lightwalletd::StandardFee>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }

    async fn ping(
        &self,
        _request: tonic::Request<lightwalletd::Duration>,
    ) -> Result<tonic::Response<lightwalletd::PingResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused"))
    }
}

#[derive(Default)]
struct CapturedServerResponse {
    has_bounded_retention: bool,
    frames: Vec<Bytes>,
}

struct CaptureServerService {
    server: CompactTxStreamerServer<MockEncodedStreamer>,
    captured: Arc<Mutex<Option<CapturedServerResponse>>>,
}

impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
    for CaptureServerService
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = tonic::Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        let mut inner = self.server.clone();
        let captured = Arc::clone(&self.captured);
        Box::pin(async move {
            let response = tonic::codegen::Service::call(&mut inner, request)
                .await
                .map_err(|_| tonic::Status::internal("infallible server error"))?;
            let has_bounded_retention = response
                .extensions()
                .get::<BoundedChunkRetention>()
                .is_some();
            let (parts, body) = response.into_parts();
            let mut body = pin!(body);
            let mut data_frames = Vec::new();
            while let Some(frame_outcome) =
                poll_fn(|cx| tonic::codegen::Body::poll_frame(body.as_mut(), cx)).await
            {
                let frame = frame_outcome?;
                if let Ok(chunk) = frame.into_data() {
                    data_frames.push(chunk);
                }
            }
            *captured.lock() = Some(CapturedServerResponse {
                has_bounded_retention,
                frames: data_frames,
            });
            Ok(tonic::codegen::http::Response::from_parts(
                parts,
                tonic::body::Body::empty(),
            ))
        })
    }
}

fn serve_encoded_range(streamer: MockEncodedStreamer) -> TestOutcome<CapturedServerResponse> {
    let captured = Arc::new(Mutex::new(None));
    let svc = CaptureServerService {
        server: CompactTxStreamerServer::new(streamer),
        captured: Arc::clone(&captured),
    };
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async move {
        let mut client = CompactTxStreamerClient::new(svc);
        let _ignored = client
            .get_block_range(tonic::Request::new(lightwalletd::BlockRange::default()))
            .await;
    });
    captured
        .lock()
        .take()
        .ok_or_else(|| "missing captured HTTP response".into())
}

fn decode_wire_messages_from_frames(frame_chunks: &[Bytes]) -> TestOutcome<Vec<EncodedBlock>> {
    let combined = frame_chunks.concat();
    let mut offset = 0_usize;
    let mut messages = Vec::new();
    while offset < combined.len() {
        let raw_len: [u8; 4] = combined[offset + 1..offset + FRAME_PREFIX_LEN].try_into()?;
        let payload_len = usize::try_from(u32::from_be_bytes(raw_len))?;
        let end = offset + FRAME_PREFIX_LEN + payload_len;
        messages.push(EncodedBlock::from_length_prefixed(Bytes::copy_from_slice(
            &combined[offset..end],
        ))?);
        offset = end;
    }
    Ok(messages)
}

#[test]
fn cold_open_source_reads_header_and_index_only_without_touching_slab() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "cold-open.zshrng02", &blocks)?;
    let raw = fs::read(&path)?;
    let header_bytes: &[u8; HEADER_LEN] = raw[..HEADER_LEN].try_into()?;
    let portable_id = ShieldedSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let header =
        ShieldedSegmentHeader::decode(header_bytes, &portable_id, u64::try_from(raw.len())?)?;
    let expected_idx_range = header.index_and_trailer_range()?;

    let source = CountingByteSource::new(raw.clone());
    assert!(matches!(
        SettledShieldedSegmentReader::open_source(
            Arc::clone(&source),
            &portable_id,
            SettledShieldedSegmentOpenOptions {
                require_fs_verity: true,
                verify_full_payload: false,
            },
        ),
        Err(SettledShieldedSegmentError::FsVerityRequired)
    ));
    assert_eq!(source.take_reads(), Vec::new());

    let reader = SettledShieldedSegmentReader::open_source(
        Arc::clone(&source),
        &portable_id,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let reads = source.take_reads();
    assert_eq!(
        reads,
        vec![ByteRange::new(0, HEADER_LEN), expected_idx_range]
    );
    assert_eq!(reader.block_count(), blocks.len());

    let full_source = CountingByteSource::new(raw);
    let _full = SettledShieldedSegmentReader::open_source(
        Arc::clone(&full_source),
        &portable_id,
        SettledShieldedSegmentOpenOptions {
            require_fs_verity: false,
            verify_full_payload: true,
        },
    )?;
    let full_reads = full_source.take_reads();
    let idx_reads = full_reads
        .iter()
        .filter(|span| **span == expected_idx_range)
        .count();
    assert_eq!(
        idx_reads, 1,
        "verify_full_payload must reuse initial index read"
    );
    Ok(())
}

fn assert_no_slab_reads(reads: &[ByteRange], index_offset: u64) {
    let slab_end = index_offset;
    for span in reads {
        let start = span.offset;
        let end = span.end().unwrap_or(u64::MAX);
        assert!(
            end <= HEADER_LEN as u64 || start >= slab_end,
            "unexpected Slab 0 read at {span:?} (index_offset={index_offset})"
        );
    }
}

fn build_skip_slab_fixture() -> TestOutcome<Vec<CompactBlockArtifact>> {
    let empty_meta = CompactChainMetadata {
        sapling_commitment_tree_size: 10,
        orchard_commitment_tree_size: 20,
        ironwood_commitment_tree_size: 30,
    };
    let mut blocks = Vec::new();
    for h in 1..=3_u32 {
        blocks.push(CompactBlockArtifact::empty(
            BlockId::new(BlockHeight::new(h), block_hash(h)),
            block_hash(h - 1),
            1_700_000_000 + h,
            empty_meta,
        ));
    }
    let sapling_block = CompactBlockArtifact::new(
        BlockId::new(BlockHeight::new(4), block_hash(4)),
        block_hash(3),
        1_700_000_004,
        vec![make_tx(
            0,
            40,
            CompactTransactionData {
                fee_zat: Some(1_000),
                sapling_spends: vec![sapling_spend(0x41)],
                sapling_outputs: vec![sapling_output(0x42)],
                ..CompactTransactionData::default()
            },
        )],
        empty_meta,
    )?;
    blocks.push(sapling_block);
    Ok(blocks)
}

#[test]
fn cold_nullifier_can_skip_slab_reads_zero_slab_bytes() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = build_skip_slab_fixture()?;
    let (path, identity) = write_segment(dir.path(), "skip-slab.zshrng02", &blocks)?;
    let portable_id = ShieldedSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let source = CountingByteSource::new(fs::read(&path)?);
    let reader = SettledShieldedSegmentReader::open_source(
        Arc::clone(&source),
        &portable_id,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let _open_reads = source.take_reads();
    let index_offset = reader.index().index_offset();

    let empty_range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(3));
    let empty_actual = reader.read_projected_range(
        empty_range,
        HeightOrder::Ascending,
        ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
    )?;
    assert_no_slab_reads(&source.take_reads(), index_offset);
    let empty_expected: Vec<_> = blocks[..3]
        .iter()
        .map(|blk| reference_encode_block(blk, ShieldedProjectionSelection::DEFAULT_NULLIFIERS))
        .collect::<TestOutcome<Vec<_>>>()?;
    assert_blocks_match_expected(
        &empty_actual,
        &empty_expected,
        ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
        HeightOrder::Ascending,
    );

    let orchard_nf = ShieldedProjectionSelection::new(false, true, false, true);
    let sapling_range = BlockHeightRange::inclusive(BlockHeight::new(4), BlockHeight::new(4));
    let skipped = reader.read_projected_range(sapling_range, HeightOrder::Ascending, orchard_nf)?;
    assert_no_slab_reads(&source.take_reads(), index_offset);
    assert_blocks_match_expected(
        &skipped,
        &[reference_encode_block(&blocks[3], orchard_nf)?],
        orchard_nf,
        HeightOrder::Ascending,
    );

    let mut scratch = ShieldedReadScratch::default();
    let mut batcher = EncodedBlockBatcher::new(true);
    reader.append_projected_frames(
        ShieldedBatchReadRequest {
            range: empty_range,
            order: HeightOrder::Ascending,
            selection: ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
            enforce_wire_fee_limit: true,
        },
        &mut scratch,
        &mut batcher,
    )?;
    assert_no_slab_reads(&source.take_reads(), index_offset);
    let served = serve_encoded_range(MockEncodedStreamer {
        use_frames: true,
        blocks: Vec::new(),
        frames: batcher.finish(),
    })?;
    assert_blocks_match_expected(
        &decode_wire_messages_from_frames(&served.frames)?,
        &empty_expected,
        ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
        HeightOrder::Ascending,
    );
    Ok(())
}

#[test]
fn warm_kernel_verified_index_plans_zero_index_reads() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "warm-kernel.zshrng02", &blocks)?;
    let raw = fs::read(&path)?;
    let header_bytes: &[u8; HEADER_LEN] = raw[..HEADER_LEN].try_into()?;
    let total_len = u64::try_from(raw.len())?;
    let header = ShieldedSegmentHeader::decode(header_bytes, &identity, total_len)?;
    let idx_range = header.index_and_trailer_range()?;
    let idx_start = usize::try_from(idx_range.offset)?;
    let index = SettledShieldedSegmentIndex::decode_index(
        header_bytes,
        &raw[idx_start..],
        &identity,
        total_len,
        true,
    )?;

    let full_range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6));
    let cold_plan =
        index.plan_block_range(full_range, ShieldedProjectionSelection::DEFAULT_SHIELDED)?;
    let cold_idx_span = cold_plan
        .needed_index_byte_range()
        .ok_or("cold plan should request index bytes")?;
    let slice_start = usize::try_from(cold_idx_span.offset)?;
    let verified = index.authenticate_index_slice(ShieldedIndexSlice::new(
        cold_idx_span.offset,
        &raw[slice_start..slice_start + cold_idx_span.len],
    ))?;
    for step in cold_plan.steps() {
        let slab_bytes = step
            .slab_byte_range()
            .map(|span| {
                let start = usize::try_from(span.offset)?;
                Ok::<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>(
                    raw[start..start + span.len].to_vec(),
                )
            })
            .transpose()?;
        let _ = index.verify_and_decode_slab(
            step,
            ShieldedFetchedSlab::new(slab_bytes, Some(verified)),
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
            HeightOrder::Ascending,
        )?;
    }

    let warm_plan =
        index.plan_block_range(full_range, ShieldedProjectionSelection::DEFAULT_SHIELDED)?;
    assert_eq!(warm_plan.needed_index_byte_range(), None);
    for step in warm_plan.steps() {
        assert_eq!(step.index_byte_range(), None);
        let slab_bytes = step
            .slab_byte_range()
            .map(|span| {
                let start = usize::try_from(span.offset)?;
                Ok::<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>(
                    raw[start..start + span.len].to_vec(),
                )
            })
            .transpose()?;
        let warm_blocks = index.verify_and_decode_slab(
            step,
            ShieldedFetchedSlab::new(slab_bytes, None),
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
            HeightOrder::Ascending,
        )?;
        assert_eq!(warm_blocks.len(), step.row_range().len());
    }
    Ok(())
}

fn decode_via_sans_io_plans(
    index: &SettledShieldedSegmentIndex,
    raw: &[u8],
    selection: ShieldedProjectionSelection,
    order: HeightOrder,
) -> TestOutcome<Vec<EncodedBlock>> {
    let range = BlockHeightRange::inclusive(
        index.identity().start_height,
        index.identity().end_block.height,
    );
    let plan = index.plan_block_range(range, selection)?;
    let verified = plan
        .needed_index_byte_range()
        .map(|span| {
            let start = usize::try_from(span.offset)?;
            Ok::<VerifiedShieldedIndexSlice<'_>, Box<dyn std::error::Error + Send + Sync>>(
                index.authenticate_index_slice(ShieldedIndexSlice::new(
                    span.offset,
                    &raw[start..start + span.len],
                ))?,
            )
        })
        .transpose()?;
    let mut batcher = EncodedBlockBatcher::new(true);
    let mut scratch = ShieldedReadScratch::default();
    let mut via_blocks = Vec::new();
    let steps_iter: Vec<_> = match order {
        HeightOrder::Ascending => plan.steps().iter().collect(),
        HeightOrder::Descending => plan.steps().iter().rev().collect(),
    };
    for step in steps_iter {
        let slab_bytes = step
            .slab_byte_range()
            .map(|span| {
                let start = usize::try_from(span.offset)?;
                Ok::<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>(
                    raw[start..start + span.len].to_vec(),
                )
            })
            .transpose()?;
        via_blocks.extend(index.verify_and_decode_slab(
            step,
            ShieldedFetchedSlab::new(slab_bytes.clone(), verified),
            selection,
            order,
        )?);
        index.verify_and_decode_slab_into_batcher(
            ShieldedSlabDecodeRequest {
                step,
                fetched: ShieldedFetchedSlab::new(slab_bytes, verified),
                selection,
                order,
            },
            &mut scratch,
            &mut batcher,
        )?;
    }
    let served = serve_encoded_range(MockEncodedStreamer {
        use_frames: true,
        blocks: Vec::new(),
        frames: batcher.finish(),
    })?;
    assert!(served.has_bounded_retention);
    assert_blocks_match_expected(
        &decode_wire_messages_from_frames(&served.frames)?,
        &via_blocks,
        selection,
        order,
    );
    Ok(via_blocks)
}

#[test]
fn sans_io_byte_range_roundtrip_without_file_io() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "sans-io.zshrng02", &blocks)?;
    let raw = fs::read(&path)?;
    let header_bytes: &[u8; HEADER_LEN] = raw[..HEADER_LEN].try_into()?;
    let total_len = u64::try_from(raw.len())?;
    let header = ShieldedSegmentHeader::decode(header_bytes, &identity, total_len)?;
    let idx_range = header.index_and_trailer_range()?;
    let idx_start = usize::try_from(idx_range.offset)?;
    let index = SettledShieldedSegmentIndex::decode_index(
        header_bytes,
        &raw[idx_start..idx_start + idx_range.len],
        &identity,
        total_len,
        false,
    )?;

    for block in &blocks {
        let hash_range = index.block_hash_byte_range(block.height())?;
        let start = usize::try_from(hash_range.offset)?;
        let decoded_hash = index.decode_block_hash_from_slice(
            block.height(),
            ShieldedIndexSlice::new(hash_range.offset, &raw[start..start + hash_range.len]),
        )?;
        assert_eq!(decoded_hash, block.block_hash());
    }

    let bounded = index
        .plan_bounded_block_range(
            BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(6)),
            HeightOrder::Ascending,
            ShieldedRangeByteBudget {
                max_bytes: 2_500,
                allow_oversized_first: true,
                enforce_wire_fee_limit: true,
            },
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
        )?
        .ok_or("expected bounded plan")?;
    assert!(bounded.kept_blocks >= 1);
    assert!(bounded.kept_native_bytes > 0);
    assert_eq!(bounded.lowest_height, BlockHeight::new(1));

    let selections = [
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
        ShieldedProjectionSelection::new(true, false, false, false),
        ShieldedProjectionSelection::new(false, true, false, false),
        ShieldedProjectionSelection::DEFAULT_NULLIFIERS,
    ];
    for selection in selections {
        for order in [HeightOrder::Ascending, HeightOrder::Descending] {
            let actual = decode_via_sans_io_plans(&index, &raw, selection, order)?;
            let mut expected: Vec<_> = blocks
                .iter()
                .map(|blk| reference_encode_block(blk, selection))
                .collect::<TestOutcome<Vec<_>>>()?;
            if order == HeightOrder::Descending {
                expected.reverse();
            }
            assert_blocks_match_expected(&actual, &expected, selection, order);
        }
    }
    Ok(())
}

fn build_slab_and_large_row_corpus() -> TestOutcome<Vec<CompactBlockArtifact>> {
    let meta = CompactChainMetadata {
        sapling_commitment_tree_size: 100,
        orchard_commitment_tree_size: 200,
        ironwood_commitment_tree_size: 0,
    };
    let mut blocks = Vec::with_capacity(72);
    for raw_height in 1..=72_u32 {
        let action_count = if raw_height == 61 { 280 } else { 62 };
        let seed = u8::try_from(raw_height)?;
        let tx = make_tx(
            0,
            seed,
            CompactTransactionData {
                fee_zat: Some(1_000),
                orchard_actions: vec![shielded_action(seed); action_count],
                ..CompactTransactionData::default()
            },
        );
        blocks.push(CompactBlockArtifact::new(
            BlockId::new(BlockHeight::new(raw_height), block_hash(raw_height)),
            block_hash(raw_height - 1),
            1_700_000_000 + raw_height,
            vec![tx],
            meta,
        )?);
    }
    Ok(blocks)
}

fn assert_repacked_short_batches_bounded(blocks: &[EncodedBlock], large_idx: usize) {
    assert!(blocks[large_idx].as_length_prefixed().is_unique());
    for group in [&blocks[..large_idx], &blocks[large_idx + 1..]] {
        let mut cursor = 0_usize;
        while cursor < group.len() {
            let batch_start = group[cursor].as_length_prefixed().as_ptr() as usize;
            let mut max_end = batch_start + group[cursor].as_length_prefixed().len();
            let mut next = cursor + 1;
            while next < group.len() {
                let ptr = group[next].as_length_prefixed().as_ptr() as usize;
                let end = ptr + group[next].as_length_prefixed().len();
                if ptr >= batch_start
                    && end.saturating_sub(batch_start) <= REPACKED_SHORT_BATCH_BYTES
                {
                    max_end = max_end.max(end);
                    next += 1;
                } else {
                    break;
                }
            }
            assert!(
                max_end - batch_start <= REPACKED_SHORT_BATCH_BYTES,
                "repacked batch span exceeded 32 KiB: {}",
                max_end - batch_start
            );
            cursor = next;
        }
    }
}

#[test]
fn slab_target_512kib_and_large_row_isolation_and_repacking() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = build_slab_and_large_row_corpus()?;
    let (path, identity) = write_segment(dir.path(), "slab-isolation.zshrng02", &blocks)?;
    let reader = SettledShieldedSegmentReader::open(
        &path,
        &identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let full_range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(72));
    let plan = reader
        .index()
        .plan_block_range(full_range, ShieldedProjectionSelection::DEFAULT_SHIELDED)?;

    let isolated_steps: Vec<_> = plan
        .steps()
        .iter()
        .filter(|step| step.is_isolated_large_row())
        .collect();
    assert_eq!(isolated_steps.len(), 1);
    assert_eq!(isolated_steps[0].row_range(), 60..61);
    let isolated_len = isolated_steps[0]
        .slab_byte_range()
        .ok_or("isolated step missing slab range")?
        .len;
    assert!(isolated_len >= LARGE_ROW_ISOLATION_MIN_BYTES);

    assert!(
        plan.steps()
            .iter()
            .filter(|step| step.row_range().end <= 60)
            .count()
            >= 2
    );
    for step in plan.steps() {
        if !step.is_isolated_large_row() {
            let slab_len = step
                .slab_byte_range()
                .ok_or("short step missing slab range")?
                .len;
            assert!(slab_len <= SLAB_TARGET_BYTES);
        }
    }

    let read_blocks = reader.read_projected_range(
        full_range,
        HeightOrder::Ascending,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
    )?;
    assert_eq!(read_blocks[60].as_length_prefixed().len(), isolated_len);
    assert_repacked_short_batches_bounded(&read_blocks, 60);

    let expected: Vec<_> = blocks
        .iter()
        .map(|blk| reference_encode_block(blk, ShieldedProjectionSelection::DEFAULT_SHIELDED))
        .collect::<TestOutcome<Vec<_>>>()?;
    assert_blocks_match_expected(
        &read_blocks,
        &expected,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
        HeightOrder::Ascending,
    );
    Ok(())
}

fn verify_streamed_batcher_order(
    reader: &SettledShieldedSegmentReader,
    blocks: &[CompactBlockArtifact],
    full_range: BlockHeightRange,
    large_frame_len: usize,
    order: HeightOrder,
) -> TestOutcome {
    let mut scratch = ShieldedReadScratch::default();
    let mut batcher = EncodedBlockBatcher::new(true);
    reader.append_projected_frames(
        ShieldedBatchReadRequest {
            range: full_range,
            order,
            selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
            enforce_wire_fee_limit: true,
        },
        &mut scratch,
        &mut batcher,
    )?;
    let served = serve_encoded_range(MockEncodedStreamer {
        use_frames: true,
        blocks: Vec::new(),
        frames: batcher.finish(),
    })?;
    assert!(served.has_bounded_retention);
    assert!(served.frames[0].len() >= 8 * 1024 && served.frames[0].len() < 32 * 1024);
    assert!(
        served
            .frames
            .iter()
            .any(|frame| frame.len() == large_frame_len)
    );
    let mut expected: Vec<_> = blocks
        .iter()
        .map(|blk| reference_encode_block(blk, ShieldedProjectionSelection::DEFAULT_SHIELDED))
        .collect::<TestOutcome<Vec<_>>>()?;
    if order == HeightOrder::Descending {
        expected.reverse();
    }
    assert_blocks_match_expected(
        &decode_wire_messages_from_frames(&served.frames)?,
        &expected,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
        order,
    );
    Ok(())
}

#[test]
fn append_projected_frames_emits_8kib_32kib_and_zero_copy_large_frames() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = build_slab_and_large_row_corpus()?;
    let (path, identity) = write_segment(dir.path(), "batcher-frames.zshrng02", &blocks)?;
    let reader = SettledShieldedSegmentReader::open(
        &path,
        &identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let full_range = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(72));
    let large_frame_len =
        reference_encode_block(&blocks[60], ShieldedProjectionSelection::DEFAULT_SHIELDED)?
            .as_length_prefixed()
            .len();

    for order in [HeightOrder::Ascending, HeightOrder::Descending] {
        verify_streamed_batcher_order(&reader, &blocks, full_range, large_frame_len, order)?;
    }

    let mut scratch = ShieldedReadScratch::default();
    let mut bounded_batcher = EncodedBlockBatcher::new(true);
    let bounded_plan: BoundedShieldedRangePlan = reader
        .append_bounded_projected_frames(
            ShieldedBoundedBatchReadRequest {
                range: full_range,
                order: HeightOrder::Ascending,
                budget: ShieldedRangeByteBudget {
                    max_bytes: 5_000,
                    allow_oversized_first: true,
                    enforce_wire_fee_limit: true,
                },
                selection: ShieldedProjectionSelection::DEFAULT_SHIELDED,
            },
            &mut scratch,
            &mut bounded_batcher,
        )?
        .ok_or("expected bounded plan from reader")?;
    assert!(bounded_plan.kept_blocks >= 1);
    let bounded_served = serve_encoded_range(MockEncodedStreamer {
        use_frames: true,
        blocks: Vec::new(),
        frames: bounded_batcher.finish(),
    })?;
    assert_eq!(
        decode_wire_messages_from_frames(&bounded_served.frames)?.len(),
        bounded_plan.kept_blocks
    );

    let direct_blocks = reader.read_projected_range(
        full_range,
        HeightOrder::Ascending,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
    )?;
    let expected_large_ptr = direct_blocks[60].as_length_prefixed().as_ptr();
    let direct_served = serve_encoded_range(MockEncodedStreamer {
        use_frames: false,
        blocks: direct_blocks,
        frames: Vec::new(),
    })?;
    assert!(direct_served.has_bounded_retention);
    assert!(
        direct_served
            .frames
            .iter()
            .any(|frame| frame.as_ptr() == expected_large_ptr),
        "isolated >= 32 KiB block must pass through HTTP body zero-copy"
    );
    Ok(())
}

#[test]
fn compact_4096_block_heap_metadata_budget() -> TestOutcome {
    let dir = TempDir::new()?;
    let meta = CompactChainMetadata {
        sapling_commitment_tree_size: 500,
        orchard_commitment_tree_size: 1_000,
        ironwood_commitment_tree_size: 0,
    };
    let mut blocks = Vec::with_capacity(4_096);
    for raw_height in 1..=4_096_u32 {
        let block = if raw_height % 4 == 0 {
            single_orchard_block(raw_height, u8::try_from(raw_height % 251)?, 1)?
        } else {
            CompactBlockArtifact::empty(
                BlockId::new(BlockHeight::new(raw_height), block_hash(raw_height)),
                block_hash(raw_height - 1),
                1_700_000_000 + raw_height,
                meta,
            )
        };
        blocks.push(block);
    }
    let (path, identity) = write_segment(dir.path(), "heap-4096.zshrng02", &blocks)?;
    let raw = fs::read(&path)?;
    let header_bytes: &[u8; HEADER_LEN] = raw[..HEADER_LEN].try_into()?;
    let total_len = u64::try_from(raw.len())?;
    let header = ShieldedSegmentHeader::decode(header_bytes, &identity, total_len)?;
    let idx_start = usize::try_from(header.index_offset())?;

    let kernel_index = SettledShieldedSegmentIndex::decode_index(
        header_bytes,
        &raw[idx_start..],
        &identity,
        total_len,
        true,
    )?;
    assert_eq!(kernel_index.heap_metadata_bytes(), 4_096 * 12);

    let portable_index = SettledShieldedSegmentIndex::decode_index(
        header_bytes,
        &raw[idx_start..],
        &identity,
        total_len,
        false,
    )?;
    let expected_pages = 4_096_usize.div_ceil(INDEX_PAGE_ROWS);
    assert_eq!(
        portable_index.heap_metadata_bytes(),
        4_096 * 12 + expected_pages * 32
    );
    assert!(portable_index.heap_metadata_bytes() <= 4_096 * 13);
    Ok(())
}

#[test]
fn portable_post_admission_index_page_tamper_fails_closed_and_latches_invalid() -> TestOutcome {
    let dir = TempDir::new()?;
    let blocks = mixed_fixture_blocks()?;
    let (path, identity) = write_segment(dir.path(), "portable-idx-tamper.zshrng02", &blocks)?;
    let portable_path = dir.path().join("portable-idx-copy.zshrng02");
    let original = fs::read(&path)?;
    fs::write(&portable_path, &original)?;
    let portable_id = ShieldedSegmentIdentity {
        fs_verity_sha256: None,
        ..identity
    };
    let reader = SettledShieldedSegmentReader::open(
        &portable_path,
        &portable_id,
        SettledShieldedSegmentOpenOptions::default(),
    )?;

    let tamper_pos = usize::try_from(reader.index().index_offset())? + 10;
    let mut flip_file = OpenOptions::new().write(true).open(&portable_path)?;
    flip_file.seek(SeekFrom::Start(u64::try_from(tamper_pos)?))?;
    flip_file.write_all(&[original[tamper_pos] ^ 0x55])?;
    flip_file.sync_all()?;
    drop(flip_file);

    assert!(matches!(
        reader.block_hash_at(BlockHeight::new(1)),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));

    let mut restore_file = OpenOptions::new().write(true).open(&portable_path)?;
    restore_file.seek(SeekFrom::Start(u64::try_from(tamper_pos)?))?;
    restore_file.write_all(&[original[tamper_pos]])?;
    restore_file.sync_all()?;
    drop(restore_file);

    let range_1 = BlockHeightRange::inclusive(BlockHeight::new(1), BlockHeight::new(1));
    assert!(matches!(
        reader.block_hash_at(BlockHeight::new(1)),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    assert!(matches!(
        reader.read_default_range(range_1, HeightOrder::Ascending),
        Err(SettledShieldedSegmentError::PayloadIntegrity(_))
    ));
    Ok(())
}
