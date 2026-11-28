//! Performance gate for `ZSHRNG02` range projection over `#554`-shaped Mainnet ranges.

use std::{error::Error, path::PathBuf, time::Instant};

use tempfile::{NamedTempFile, TempDir};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, CompactBlockArtifact, CompactChainMetadata,
    CompactSaplingOutput, CompactSaplingSpend, CompactShieldedAction, CompactTransaction,
    CompactTransactionData, Network, TransactionId,
};
use zinder_store::{
    HeightOrder, SettledShieldedSegmentOpenOptions, SettledShieldedSegmentReader,
    SettledShieldedSegmentWriter, ShieldedProjectionSelection, ShieldedSegmentIdentity,
};

type TestOutcome<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[cfg(target_os = "linux")]
#[path = "../common/payload_page_probe.rs"]
mod payload_page_probe;

const TOTAL_BLOCKS: u32 = 165_205;
const SEGMENT_BLOCKS: u32 = 4_096;
const TARGET_ORCHARD_ACTIONS: usize = 256_055;
const FIRST_HEIGHT: u32 = 3_428_143;
const WARM_SCAN_BLOCKS: u32 = 8 * SEGMENT_BLOCKS;

#[cfg(debug_assertions)]
const MAX_NULLIFIER_SCAN_MS: u128 = 15_000;
#[cfg(not(debug_assertions))]
const MAX_NULLIFIER_SCAN_MS: u128 = 500;
#[cfg(debug_assertions)]
const MAX_WARM_SCAN_MS: u128 = 15_000;
#[cfg(not(debug_assertions))]
const MAX_WARM_SCAN_MS: u128 = 1_500;

fn deterministic_hash(seed: u32) -> BlockHash {
    let mut bytes = [0_u8; 32];
    bytes[..4].copy_from_slice(&seed.to_le_bytes());
    bytes[28..].copy_from_slice(&seed.to_be_bytes());
    BlockHash::from_bytes(bytes)
}

fn orchard_action(seed: u8) -> CompactShieldedAction {
    CompactShieldedAction {
        nullifier: [seed; 32],
        commitment: [seed.wrapping_add(1); 32],
        ephemeral_key: [seed.wrapping_add(2); 32],
        ciphertext: [seed.wrapping_add(3); 52],
    }
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

fn segment_path(dir: &TempDir, seg_idx: usize) -> PathBuf {
    dir.path().join(format!("seg-{seg_idx:03}.zshrng02"))
}

fn build_corpus_templates() -> (
    CompactTransaction,
    CompactTransaction,
    CompactTransaction,
    CompactChainMetadata,
) {
    let orchard_tx = CompactTransaction {
        index: 1,
        transaction_id: TransactionId::from_bytes([0x44; 32]),
        data: CompactTransactionData {
            fee_zat: Some(10_000),
            orchard_actions: vec![orchard_action(0x11); 2],
            ..CompactTransactionData::default()
        },
    };
    let tail_orchard_tx = CompactTransaction {
        index: 1,
        transaction_id: TransactionId::from_bytes([0x45; 32]),
        data: CompactTransactionData {
            fee_zat: Some(10_000),
            orchard_actions: vec![orchard_action(0x21); 3],
            ..CompactTransactionData::default()
        },
    };
    let sapling_deposit_tx = CompactTransaction {
        index: 1,
        transaction_id: TransactionId::from_bytes([0x55; 32]),
        data: CompactTransactionData {
            fee_zat: Some(10_000),
            sapling_spends: vec![sapling_spend(0x31)],
            sapling_outputs: vec![sapling_output(0x32)],
            ..CompactTransactionData::default()
        },
    };
    let metadata = CompactChainMetadata {
        sapling_commitment_tree_size: 1_000_000,
        orchard_commitment_tree_size: 2_000_000,
        ironwood_commitment_tree_size: 0,
    };
    (orchard_tx, tail_orchard_tx, sapling_deposit_tx, metadata)
}

fn select_perf_block_txs(
    abs: u32,
    orchard_written: &mut usize,
    orchard_tx: &CompactTransaction,
    tail_orchard_tx: &CompactTransaction,
    sapling_deposit_tx: &CompactTransaction,
) -> Vec<CompactTransaction> {
    let remaining_orchard = TARGET_ORCHARD_ACTIONS.saturating_sub(*orchard_written);
    if remaining_orchard == 3 {
        *orchard_written += 3;
        vec![tail_orchard_tx.clone()]
    } else if remaining_orchard >= 4 && (abs % 5 < 4 || abs + 2_000 >= TOTAL_BLOCKS) {
        *orchard_written += 2;
        vec![orchard_tx.clone()]
    } else if abs % 10 == 4 {
        vec![sapling_deposit_tx.clone()]
    } else {
        Vec::new()
    }
}

fn write_perf_segment(
    dir: &TempDir,
    seg_idx: usize,
    offset: u32,
    count: u32,
    orchard_written: &mut usize,
) -> TestOutcome<SettledShieldedSegmentReader> {
    let (orchard_tx, tail_orchard_tx, sapling_deposit_tx, metadata) = build_corpus_templates();
    let start = BlockHeight::new(FIRST_HEIGHT + offset);
    let end_height = BlockHeight::new(FIRST_HEIGHT + offset + count - 1);
    let end_block = BlockId::new(end_height, deterministic_hash(end_height.value()));
    let seg_path = segment_path(dir, seg_idx);
    let staged = NamedTempFile::new_in(dir.path())?;
    let mut writer = SettledShieldedSegmentWriter::create(
        &seg_path,
        staged,
        ShieldedSegmentIdentity::span(Network::ZcashMainnet, 1, start, end_block),
    )?;
    for local in 0..count {
        let abs = offset + local;
        let height_val = FIRST_HEIGHT + abs;
        let txs = select_perf_block_txs(
            abs,
            orchard_written,
            &orchard_tx,
            &tail_orchard_tx,
            &sapling_deposit_tx,
        );
        let block = CompactBlockArtifact::new(
            BlockId::new(BlockHeight::new(height_val), deterministic_hash(height_val)),
            deterministic_hash(height_val - 1),
            1_730_000_000 + abs,
            txs,
            metadata,
        )?;
        let approx_native = 256 + block.transactions().len() * 512;
        writer.push(&block, approx_native)?;
    }
    let identity = writer.finish()?;
    Ok(SettledShieldedSegmentReader::open(
        &seg_path,
        &identity,
        SettledShieldedSegmentOpenOptions::default(),
    )?)
}

fn build_corpus(
    dir: &TempDir,
    total_blocks: u32,
) -> TestOutcome<Vec<SettledShieldedSegmentReader>> {
    let mut readers = Vec::new();
    let mut offset = 0_u32;
    let mut orchard_written = 0_usize;
    while offset < total_blocks {
        let count = SEGMENT_BLOCKS.min(total_blocks - offset);
        let reader = write_perf_segment(dir, readers.len(), offset, count, &mut orchard_written)?;
        readers.push(reader);
        offset += count;
    }

    if total_blocks == TOTAL_BLOCKS {
        assert_eq!(orchard_written, TARGET_ORCHARD_ACTIONS);
    }
    Ok(readers)
}

fn read_all_segments(
    readers: &[SettledShieldedSegmentReader],
    selection: ShieldedProjectionSelection,
) -> TestOutcome<(usize, usize)> {
    let mut total_blocks = 0_usize;
    let mut total_bytes = 0_usize;
    for reader in readers {
        let identity = reader.identity();
        let blocks = reader.read_projected_range(
            BlockHeightRange::inclusive(identity.start_height, identity.end_block.height),
            HeightOrder::Ascending,
            selection,
        )?;
        total_blocks += blocks.len();
        total_bytes += blocks
            .iter()
            .map(zinder_proto::compat::lightwalletd::EncodedBlock::payload_len)
            .sum::<usize>();
    }
    Ok((total_blocks, total_bytes))
}

#[test]
fn z554_nullifier_projection_across_165k_blocks_stays_under_budget() -> TestOutcome {
    let dir = TempDir::new()?;
    let readers = build_corpus(&dir, TOTAL_BLOCKS)?;

    let start_default_nf = Instant::now();
    let (nf_all_blocks, nf_all_bytes) =
        read_all_segments(&readers, ShieldedProjectionSelection::DEFAULT_NULLIFIERS)?;
    let default_nf_ms = start_default_nf.elapsed().as_millis();

    let orchard_only_nf = ShieldedProjectionSelection::new(false, true, false, true);
    let start_orchard_nf = Instant::now();
    let (nf_orchard_blocks, nf_orchard_bytes) = read_all_segments(&readers, orchard_only_nf)?;
    let orchard_nf_ms = start_orchard_nf.elapsed().as_millis();

    let start_default_range = Instant::now();
    let (default_blocks, default_bytes) =
        read_all_segments(&readers, ShieldedProjectionSelection::DEFAULT_SHIELDED)?;
    let default_range_ms = start_default_range.elapsed().as_millis();

    assert_eq!(nf_all_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(nf_orchard_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert_eq!(default_blocks, usize::try_from(TOTAL_BLOCKS)?);
    assert!(nf_orchard_bytes < nf_all_bytes);
    assert!(nf_all_bytes < default_bytes);

    #[allow(
        clippy::print_stderr,
        reason = "perf gate reports measured durations across the 165,205-block corpus"
    )]
    {
        eprintln!(
            "zshrng02_perf blocks={} segments={} default_nf_ms={} orchard_nf_ms={} default_range_ms={} nf_all_bytes={} nf_orchard_bytes={} default_bytes={}",
            TOTAL_BLOCKS,
            readers.len(),
            default_nf_ms,
            orchard_nf_ms,
            default_range_ms,
            nf_all_bytes,
            nf_orchard_bytes,
            default_bytes,
        );
    }

    assert!(
        default_nf_ms <= MAX_NULLIFIER_SCAN_MS && orchard_nf_ms <= MAX_NULLIFIER_SCAN_MS,
        "nullifier projection exceeded budget ({MAX_NULLIFIER_SCAN_MS} ms): default_nf={default_nf_ms} ms, orchard_nf={orchard_nf_ms} ms"
    );
    Ok(())
}

#[test]
fn unbounded_warm_cache_range_scan_preserves_residency_and_throughput() -> TestOutcome {
    let dir = TempDir::new()?;
    let readers = build_corpus(&dir, WARM_SCAN_BLOCKS)?;
    let scan = || read_all_segments(&readers, ShieldedProjectionSelection::DEFAULT_SHIELDED);
    #[cfg(target_os = "linux")]
    let probes = {
        let mut probes = Vec::with_capacity(readers.len());
        for (idx, reader) in readers.iter().enumerate() {
            // The format header fits in the first page. Exclude that page and the index boundary.
            let probe = payload_page_probe::PayloadPageProbe::open(
                &segment_path(&dir, idx),
                1,
                reader.index().index_offset(),
            )?;
            probe.evict()?;
            probes.push(probe);
        }
        probes
    };
    #[cfg(target_os = "linux")]
    let calibration = sample_corpus_pages(&probes)?;
    #[cfg(target_os = "linux")]
    let calibrated = calibration.is_some_and(payload_page_probe::PageResidency::confirms_eviction);
    #[cfg(not(target_os = "linux"))]
    let calibrated = false;
    let first_pass_label = if calibrated {
        "cold_ms"
    } else {
        "first_pass_ms"
    };

    let started = Instant::now();
    let first_pass = scan()?;
    let first_pass_ms = started.elapsed().as_millis();
    assert_eq!(first_pass.0, usize::try_from(WARM_SCAN_BLOCKS)?);
    assert!(first_pass.1 > 4 * 1024 * 1024);
    assert_eq!(scan()?, first_pass);
    #[cfg(target_os = "linux")]
    let warm_before = sample_corpus_pages(&probes)?;

    let started = Instant::now();
    let warm = scan()?;
    let warm_ms = started.elapsed().as_millis();
    #[cfg(target_os = "linux")]
    let residency = {
        let warm_after = sample_corpus_pages(&probes)?;
        if calibrated {
            for sample in [warm_before, warm_after] {
                let pages = sample.ok_or("residency probe stopped working after calibration")?;
                assert!(pages.mostly_resident(), "warm scan retained {pages:?}");
            }
        }
        format!("calibration={calibration:?} warm_before={warm_before:?} warm_after={warm_after:?}")
    };
    #[cfg(not(target_os = "linux"))]
    let residency = "residency=unavailable";
    #[allow(
        clippy::print_stderr,
        reason = "reports first-pass and warm scan timings and page residency"
    )]
    {
        eprintln!(
            "zshrng02_warm_cache segments={} blocks={} payload_bytes={} {first_pass_label}={first_pass_ms} warm_ms={warm_ms} calibrated={calibrated} {residency}",
            readers.len(),
            warm.0,
            warm.1,
        );
    }
    assert_eq!(warm, first_pass);
    assert!(
        warm_ms < MAX_WARM_SCAN_MS,
        "warm scan exceeded {MAX_WARM_SCAN_MS} ms: {warm_ms} ms"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
fn sample_corpus_pages(
    probes: &[payload_page_probe::PayloadPageProbe],
) -> std::io::Result<Option<payload_page_probe::PageResidency>> {
    let mut total = payload_page_probe::PageResidency::default();
    for probe in probes {
        let Some(sample) = probe.sample()? else {
            return Ok(None);
        };
        total.resident += sample.resident;
        total.total += sample.total;
    }
    Ok(Some(total))
}
