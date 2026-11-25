//! Performance gate for `ZSTSUP01` lossless wire splice and unary lookup.

use std::{error::Error, time::Instant};

use tempfile::{NamedTempFile, TempDir};
use zinder_core::{
    AuthDigest, BlockHash, BlockHeaderArtifact, BlockHeight, BlockHeightRange, BlockId,
    CanonicalBlockFacts, CanonicalTransactionFacts, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, CompactTransparentInput, CompactTransparentOutput, ConsensusBranchId,
    LockTime, Network, PrivacyShape, SerializedBytesDigest, TransactionComponentCounts,
    TransactionId, TransactionIntrinsicValueBalances, TransactionPublicFacts, TransactionVersion,
    Wtxid,
};
use zinder_store::{
    CompactBlockSpliceSelection, HeightOrder, SettledShieldedSegmentOpenOptions,
    SettledShieldedSegmentReader, SettledShieldedSegmentWriter,
    SettledSupplementSegmentOpenOptions, SettledSupplementSegmentReader,
    SettledSupplementSegmentWriteOptions, SettledSupplementSegmentWriter,
    ShieldedProjectionSelection, ShieldedSegmentIdentity, SupplementSegmentIdentity,
};

type TestOutcome<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const TOTAL_BLOCKS: u32 = 16_384;
const SEGMENT_BLOCKS: u32 = 4_096;
const FIRST_HEIGHT: u32 = 3_400_000;
const UNARY_SAMPLES: u32 = 64;
const DENSE_TAG2_TXS: u32 = 64;

#[cfg(debug_assertions)]
const MAX_SPLICE_RANGE_MS: u128 = 15_000;
#[cfg(not(debug_assertions))]
const MAX_SPLICE_RANGE_MS: u128 = 1_500;

const fn det_hash(seed: u32) -> BlockHash {
    let mut bytes = [0_u8; 32];
    let le = seed.to_le_bytes();
    let be = seed.to_be_bytes();
    bytes[0] = le[0];
    bytes[1] = le[1];
    bytes[2] = le[2];
    bytes[3] = le[3];
    bytes[28] = be[0];
    bytes[29] = be[1];
    bytes[30] = be[2];
    bytes[31] = be[3];
    BlockHash::from_bytes(bytes)
}

const fn det_txid(seed: u8) -> TransactionId {
    let mut bytes = [seed; 32];
    bytes[0] = seed.wrapping_add(5);
    TransactionId::from_bytes(bytes)
}

struct PerfCorpus {
    _dir: TempDir,
    pairs: Vec<(SettledShieldedSegmentReader, SettledSupplementSegmentReader)>,
}

fn build_perf_templates() -> (
    CompactTransaction,
    CompactTransaction,
    CompactTransaction,
    CompactChainMetadata,
) {
    let orchard_tx = CompactTransaction {
        index: 1,
        transaction_id: det_txid(0x11),
        data: CompactTransactionData {
            fee_zat: Some(10_000),
            orchard_actions: vec![CompactShieldedAction {
                nullifier: [0x21; 32],
                commitment: [0x22; 32],
                ephemeral_key: [0x23; 32],
                ciphertext: [0x24; 52],
            }],
            ..CompactTransactionData::default()
        },
    };
    let transparent_tx = CompactTransaction {
        index: 0,
        transaction_id: det_txid(0x31),
        data: CompactTransactionData {
            fee_zat: Some(0),
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 312_500_000,
                script_pub_key: vec![0x76, 0xa9, 0x14, 0x55, 0x88, 0xac],
            }],
            ..CompactTransactionData::default()
        },
    };
    let mixed_tx = CompactTransaction {
        index: 2,
        transaction_id: det_txid(0x41),
        data: CompactTransactionData {
            fee_zat: Some(20_000),
            sapling_spends: vec![CompactSaplingSpend {
                nullifier: [0x42; 32],
            }],
            sapling_outputs: vec![CompactSaplingOutput {
                commitment: [0x43; 32],
                ephemeral_key: [0x44; 32],
                ciphertext: [0x45; 52],
            }],
            transparent_inputs: vec![CompactTransparentInput {
                previous_transaction_id: det_txid(0x46),
                previous_output_index: 1,
            }],
            transparent_outputs: vec![CompactTransparentOutput {
                value_zat: 75_000,
                script_pub_key: vec![0x76, 0xa9, 0x14, 0x66, 0x88, 0xac],
            }],
            ..CompactTransactionData::default()
        },
    };
    let metadata = CompactChainMetadata {
        sapling_commitment_tree_size: 8_000_000,
        orchard_commitment_tree_size: 4_000_000,
        ironwood_commitment_tree_size: 0,
    };
    (orchard_tx, transparent_tx, mixed_tx, metadata)
}

fn build_dense_shielded_txs() -> Vec<CompactTransaction> {
    (0..DENSE_TAG2_TXS)
        .map(|idx| {
            let seed = u8::try_from(idx % 251).unwrap_or(1);
            CompactTransaction {
                index: u64::from(idx),
                transaction_id: det_txid(seed),
                data: CompactTransactionData {
                    fee_zat: (idx % 2 == 0).then_some(10_000),
                    orchard_actions: vec![CompactShieldedAction {
                        nullifier: [seed; 32],
                        commitment: [seed.wrapping_add(1); 32],
                        ephemeral_key: [seed.wrapping_add(2); 32],
                        ciphertext: [seed.wrapping_add(3); 52],
                    }],
                    ..CompactTransactionData::default()
                },
            }
        })
        .collect()
}

fn build_dense_canonical_facts(block: &CompactBlockArtifact) -> CanonicalBlockFacts {
    let header = BlockHeaderArtifact::new(
        block.height(),
        block.block_hash(),
        block.previous_block_hash(),
        [0xAA; 32],
        [0xBB; 32],
        i64::from(block.time()),
        0x1d00_ffff,
        [0xCC; 32],
        5,
        4_096,
    );
    let transactions = block
        .transactions()
        .iter()
        .enumerate()
        .map(|(pos, tx)| CanonicalTransactionFacts {
            public_facts: TransactionPublicFacts {
                transaction_id: tx.transaction_id,
                auth_digest: Some(AuthDigest::from_bytes([0x11; 32])),
                wtxid: Some(Wtxid::from_bytes([0x22; 64])),
                version: TransactionVersion::V5,
                consensus_branch_id: Some(ConsensusBranchId::new(0xC2D6_D0B4)),
                lock_time: LockTime::Height(block.height()),
                expiry_height: Some(BlockHeight::new(block.height().value() + 40)),
                size_bytes: 512,
                counts: TransactionComponentCounts {
                    transparent_input_count: 0,
                    transparent_output_count: 0,
                    sapling_spend_count: 0,
                    sapling_output_count: 0,
                    orchard_action_count: 1,
                    ironwood_action_count: 0,
                    sprout_joinsplit_count: 0,
                },
                orchard_value_balance_zat: Some(-25_000),
                orchard_anchor: Some([0x77; 32]),
                ironwood_value_balance_zat: None,
                privacy_shape: PrivacyShape::ShieldedOnly,
                is_coinbase: pos == 0,
                unsupported_sections: Vec::new(),
            },
            serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0x33; 16]),
            intrinsic_value_balances: TransactionIntrinsicValueBalances::new(0, 0, 25_000, 0),
            transparent_inputs: Vec::new(),
            transparent_outputs: Vec::new(),
        })
        .collect();
    CanonicalBlockFacts {
        block_header: header,
        serialized_bytes_digest: SerializedBytesDigest::from_serialized_bytes(&[0x99; 8]),
        transactions,
    }
}

fn select_block_txs(
    abs: u32,
    orchard_tx: &CompactTransaction,
    transparent_tx: &CompactTransaction,
    mixed_tx: &CompactTransaction,
    dense_shielded_txs: &[CompactTransaction],
) -> Vec<CompactTransaction> {
    if abs.is_multiple_of(512) {
        return dense_shielded_txs.to_vec();
    }
    match abs % 8 {
        0 => vec![transparent_tx.clone(), orchard_tx.clone(), mixed_tx.clone()],
        1 | 2 => vec![transparent_tx.clone(), mixed_tx.clone()],
        3 => vec![orchard_tx.clone()],
        4 => vec![transparent_tx.clone()],
        _ => Vec::new(),
    }
}

fn write_perf_pair(
    dir: &TempDir,
    seg_idx: u32,
    offset: u32,
    count: u32,
) -> TestOutcome<(SettledShieldedSegmentReader, SettledSupplementSegmentReader)> {
    let (orchard_tx, transparent_tx, mixed_tx, metadata) = build_perf_templates();
    let dense_shielded_txs = build_dense_shielded_txs();
    let start = BlockHeight::new(FIRST_HEIGHT + offset);
    let end_height = BlockHeight::new(FIRST_HEIGHT + offset + count - 1);
    let end_block = BlockId::new(end_height, det_hash(end_height.value()));
    let shielded_path = dir.path().join(format!("seg-{seg_idx:03}.zshrng02"));
    let sup_path = dir.path().join(format!("seg-{seg_idx:03}.zstsup01"));

    let mut shielded_writer = SettledShieldedSegmentWriter::create(
        &shielded_path,
        NamedTempFile::new_in(dir.path())?,
        ShieldedSegmentIdentity::span(Network::ZcashMainnet, 1, start, end_block),
    )?;
    let mut sup_writer = SettledSupplementSegmentWriter::create(
        &sup_path,
        SupplementSegmentIdentity::span(Network::ZcashMainnet, 1, start, end_block),
        SettledSupplementSegmentWriteOptions::default(),
    )?;

    for local in 0..count {
        let abs = offset + local;
        let height_val = FIRST_HEIGHT + abs;
        let txs = select_block_txs(
            abs,
            &orchard_tx,
            &transparent_tx,
            &mixed_tx,
            &dense_shielded_txs,
        );
        let block = CompactBlockArtifact::new(
            BlockId::new(BlockHeight::new(height_val), det_hash(height_val)),
            det_hash(height_val - 1),
            1_735_000_000 + abs,
            txs,
            metadata,
        )?;
        shielded_writer.push(&block, 256 + block.transactions().len() * 384)?;
        let facts = abs
            .is_multiple_of(512)
            .then(|| build_dense_canonical_facts(&block));
        sup_writer.push(&block, facts.as_ref())?;
    }

    let shielded_id = shielded_writer.finish()?;
    let sup_id = sup_writer.finish()?;
    let shielded_reader = SettledShieldedSegmentReader::open(
        &shielded_path,
        &shielded_id,
        SettledShieldedSegmentOpenOptions::default(),
    )?;
    let sup_reader = SettledSupplementSegmentReader::open(
        &sup_path,
        &sup_id,
        SettledSupplementSegmentOpenOptions::default(),
    )?;
    Ok((shielded_reader, sup_reader))
}

fn build_corpus() -> TestOutcome<PerfCorpus> {
    let dir = TempDir::new()?;
    let mut pairs = Vec::new();
    let mut offset = 0_u32;
    let mut seg_idx = 0_u32;
    while offset < TOTAL_BLOCKS {
        let count = SEGMENT_BLOCKS.min(TOTAL_BLOCKS - offset);
        pairs.push(write_perf_pair(&dir, seg_idx, offset, count)?);
        offset += count;
        seg_idx += 1;
    }
    Ok(PerfCorpus { _dir: dir, pairs })
}

fn run_range_splice(
    pairs: &[(SettledShieldedSegmentReader, SettledSupplementSegmentReader)],
    selection: CompactBlockSpliceSelection,
) -> TestOutcome<(usize, usize)> {
    let mut blocks_total = 0_usize;
    let mut wire_bytes = 0_usize;
    for (shielded, supplement) in pairs {
        let identity = supplement.identity();
        let range = BlockHeightRange::inclusive(identity.start_height, identity.end_block.height);
        let projected = shielded.read_projected_range(
            range,
            HeightOrder::Ascending,
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
        )?;
        let spliced =
            supplement.splice_encoded_range_wire(identity.start_height, &projected, selection)?;
        blocks_total += spliced.len();
        wire_bytes += spliced
            .iter()
            .map(zinder_proto::compat::lightwalletd::EncodedBlock::payload_len)
            .sum::<usize>();
    }
    Ok((blocks_total, wire_bytes))
}

fn verify_dense_tag2_block(
    shielded: &SettledShieldedSegmentReader,
    supplement: &SettledSupplementSegmentReader,
) -> TestOutcome {
    let dense_height = BlockHeight::new(FIRST_HEIGHT);
    let dense_meta = supplement.read_canonical_metadata(dense_height)?;
    assert_eq!(
        dense_meta.map(|meta| meta.transactions.len()),
        Some(usize::try_from(DENSE_TAG2_TXS)?)
    );
    let dense_projected = shielded.read_projected_range(
        BlockHeightRange::inclusive(dense_height, dense_height),
        HeightOrder::Ascending,
        ShieldedProjectionSelection::DEFAULT_SHIELDED,
    )?;
    let dense_restored =
        supplement.reconstruct_block(dense_height, &dense_projected[0].payload())?;
    assert_eq!(
        dense_restored.transactions().len(),
        usize::try_from(DENSE_TAG2_TXS)?
    );
    Ok(())
}

#[test]
fn zstsup01_lossless_splice_and_unary_lookup_stay_under_budget() -> TestOutcome {
    let corpus = build_corpus()?;

    let start_full = Instant::now();
    let (full_blocks, full_bytes) =
        run_range_splice(&corpus.pairs, CompactBlockSpliceSelection::FULL_BLOCK)?;
    let full_range_ms = start_full.elapsed().as_millis();

    let start_shielded = Instant::now();
    let (shielded_blocks, shielded_bytes) =
        run_range_splice(&corpus.pairs, CompactBlockSpliceSelection::DEFAULT_SHIELDED)?;
    let shielded_range_ms = start_shielded.elapsed().as_millis();

    let start_unary = Instant::now();
    let stride = SEGMENT_BLOCKS / UNARY_SAMPLES;
    let (first_shielded, first_supplement) = &corpus.pairs[0];
    let mut unary_reconstructed = 0_usize;
    for sample in 0..UNARY_SAMPLES {
        let height = BlockHeight::new(FIRST_HEIGHT + sample * stride);
        let projected = first_shielded.read_projected_range(
            BlockHeightRange::inclusive(height, height),
            HeightOrder::Ascending,
            ShieldedProjectionSelection::DEFAULT_SHIELDED,
        )?;
        let spliced = first_supplement.splice_block_wire(
            height,
            &projected[0].payload(),
            CompactBlockSpliceSelection::FULL_BLOCK,
        )?;
        assert_ne!(spliced.as_length_prefixed().as_ref(), [] as [u8; 0]);
        let restored = first_supplement.reconstruct_block(height, &projected[0].payload())?;
        assert_eq!(restored.height(), height);
        unary_reconstructed += 1;
    }
    let unary_ms = start_unary.elapsed().as_millis();

    verify_dense_tag2_block(first_shielded, first_supplement)?;

    assert_eq!(full_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(shielded_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(unary_reconstructed, usize::try_from(UNARY_SAMPLES)?);
    assert!(full_bytes > shielded_bytes);

    #[allow(
        clippy::print_stderr,
        reason = "perf gate reports measured durations across the ZSTSUP01 chunked corpus"
    )]
    {
        eprintln!(
            "zstsup01_perf blocks={} segments={} full_splice_ms={} shielded_splice_ms={} unary_{}x_ms={} full_bytes={} shielded_bytes={}",
            TOTAL_BLOCKS,
            corpus.pairs.len(),
            full_range_ms,
            shielded_range_ms,
            UNARY_SAMPLES,
            unary_ms,
            full_bytes,
            shielded_bytes,
        );
    }

    assert!(
        full_range_ms <= MAX_SPLICE_RANGE_MS && shielded_range_ms <= MAX_SPLICE_RANGE_MS,
        "ZSTSUP01 splice exceeded budget ({MAX_SPLICE_RANGE_MS} ms): full={full_range_ms} ms, shielded={shielded_range_ms} ms"
    );
    Ok(())
}
