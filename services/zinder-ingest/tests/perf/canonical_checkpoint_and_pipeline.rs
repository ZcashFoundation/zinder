#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

//! Deterministic T2 performance-regression checks for canonical commitment-tree
//! checkpoint batching and pipelined SST staging.
//!
//! Instead of asserting wall-clock thresholds that vary across CI hosts, this
//! suite asserts the structural work bounds that govern canonical construction
//! speed on Mainnet-like block mixes (~85% empty shielded blocks):
//! - multi-worker construction combines 64-block commitment-tree batches rather
//!   than dispatching per-block checkpoint jobs,
//! - unchanged pool frontiers reuse shared cross-checked states across batches
//!   and threads while preserving byte-for-byte identical column-family outputs
//!   between `tree_hashing_workers = 1` and `tree_hashing_workers = 8`, and
//! - zero prohibited historical prevout or cross-block wallet reads occur.

use std::{
    num::{NonZeroU32, NonZeroU64},
    path::Path,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use tempfile::TempDir;
use zinder_core::{BlockHeight, BlockId, Network, NetworkUpgradeActivations};
use zinder_ingest::{
    CanonicalConstructionConfig, CanonicalPipelineLimits, load_fresh_canonical_blocks,
};
use zinder_runtime::{OpsServer, install_metrics_recorder};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceBlockHeader, SourceChainSegment,
    SourceChainSegmentLimits, SourceError,
};
use zinder_store::{
    CanonicalBlockLoadEvidence, CanonicalBuildSubtreeRoot, CanonicalReorgPolicy,
    CanonicalStoreBuildPlan, CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder,
    RocksDbResourceBudget, TreeStateCheckpointSelection,
};

use crate::common::construction_fixture::{
    chained_source_block, fixture_orchard_source_block, fixture_sapling_source_block,
    fixture_source_block, regtest_activations, shielded_transactions_with_lock_time,
};

const PERF_CHAIN_BLOCKS: u32 = 200;

#[derive(Clone)]
struct MainnetMixBenchSource {
    genesis: SourceBlock,
    blocks: Arc<Vec<SourceBlock>>,
}

#[async_trait]
impl NodeSource for MainnetMixBenchSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        if height.value() == 0 {
            return Ok(self.genesis.clone());
        }
        let idx = usize::try_from(height.value().saturating_sub(1)).unwrap_or(usize::MAX);
        self.blocks
            .get(idx)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "missing benchmark block".to_owned(),
            })
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        let next_height =
            limits
                .cursor
                .next_connected_height()
                .ok_or(SourceError::SourceProtocolMismatch {
                    reason: "missing next connected height",
                })?;
        let start = usize::try_from(next_height.value().saturating_sub(1)).unwrap_or(usize::MAX);
        let max_blocks = usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX);
        let end = start.saturating_add(max_blocks).min(self.blocks.len());
        let slice = self.blocks.get(start..end).unwrap_or(&[]).to_vec();
        Ok(SourceChainSegment::connected_blocks(slice))
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let tip = self
            .blocks
            .last()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height: BlockHeight::new(1),
                reason: "empty benchmark source".to_owned(),
            })?;
        Ok(BlockId::new(tip.height, tip.hash))
    }
}

fn build_mainnet_mix_source(block_count: u32) -> eyre::Result<MainnetMixBenchSource> {
    let block_one = fixture_source_block().map_err(|err| eyre::eyre!("{err}"))?;
    let sapling_fixture = fixture_sapling_source_block().map_err(|err| eyre::eyre!("{err}"))?;
    let orchard_fixture = fixture_orchard_source_block().map_err(|err| eyre::eyre!("{err}"))?;
    let genesis = SourceBlock::new(
        SourceBlockHeader {
            network: Network::ZcashRegtest,
            height: BlockHeight::new(0),
            hash: block_one.parent_hash,
            parent_hash: block_one.parent_hash,
            block_time_seconds: block_one.block_time_seconds.saturating_sub(1),
        },
        Vec::new(),
    );
    let mut blocks = Vec::with_capacity(usize::try_from(block_count)?);
    let mut parent_hash = genesis.hash;
    for height in 1..=block_count {
        let block = if height == 1 {
            block_one.clone()
        } else {
            let mut extra = Vec::new();
            if height >= 2 && height % 7 == 0 {
                extra.extend(
                    shielded_transactions_with_lock_time(&sapling_fixture, Some(10_000 + height))
                        .map_err(|err| eyre::eyre!("{err}"))?,
                );
            }
            if height >= 4 && (height + 3) % 7 == 0 {
                extra.extend(
                    shielded_transactions_with_lock_time(&orchard_fixture, Some(30_000 + height))
                        .map_err(|err| eyre::eyre!("{err}"))?,
                );
            }
            chained_source_block(
                BlockHeight::new(height),
                parent_hash,
                u8::try_from(height).unwrap_or(0),
                extra,
            )
            .map_err(|err| eyre::eyre!("{err}"))?
        };
        parent_hash = block.hash;
        blocks.push(block);
    }
    Ok(MainnetMixBenchSource {
        genesis,
        blocks: Arc::new(blocks),
    })
}

struct LoadedStoreSnapshot {
    evidence: CanonicalBlockLoadEvidence,
    derived_subtree_roots: Vec<CanonicalBuildSubtreeRoot>,
    checkpoints_cf: Vec<(Vec<u8>, Vec<u8>)>,
    final_roots_cf: Vec<(Vec<u8>, Vec<u8>)>,
}

fn read_raw_cf(path: &Path, family_name: &str) -> eyre::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let options = rust_rocksdb::Options::default();
    let families = rust_rocksdb::DB::list_cf(&options, path)?;
    let db = rust_rocksdb::DB::open_cf_for_read_only(&options, path, families, false)?;
    let Some(family) = db.cf_handle(family_name) else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    for row in db.iterator_cf(&family, rust_rocksdb::IteratorMode::Start) {
        let (key, bytes) = row?;
        rows.push((key.to_vec(), bytes.to_vec()));
    }
    Ok(rows)
}

async fn run_load_for_workers(
    source: &MainnetMixBenchSource,
    activations: &Arc<NetworkUpgradeActivations>,
    workload: CanonicalStoreWorkload,
    workers: u32,
) -> eyre::Result<LoadedStoreSnapshot> {
    let directory = TempDir::new()?;
    let store_path = directory.path().join("canonical");
    let tip = source.tip_id().await?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        source.genesis.block_time_seconds,
        tip,
        RawBlobRetention::All,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &store_path,
        workload,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?
    .with_tree_state_checkpoint_selection(TreeStateCheckpointSelection::EveryBlock)?;

    let mut limits = CanonicalPipelineLimits::resolve(
        NonZeroU64::new(4 * 1_024 * 1_024 * 1_024),
        NonZeroU32::new(8).ok_or_else(|| eyre::eyre!("nonzero cores"))?,
        NonZeroU64::new(64 * 1_024 * 1_024).ok_or_else(|| eyre::eyre!("nonzero max response"))?,
    );
    limits.tree_hashing_workers =
        NonZeroU32::new(workers).ok_or_else(|| eyre::eyre!("nonzero workers"))?;
    let config = CanonicalConstructionConfig {
        request_timeout: Duration::from_secs(15),
        pipeline_limits: limits,
        network_upgrade_activations: Arc::clone(activations),
    };

    let outcome = load_fresh_canonical_blocks(builder, source, &config).await?;
    let evidence = outcome.evidence;
    let derived_subtree_roots = outcome.derived_subtree_roots;
    drop(outcome.builder);

    let checkpoints_cf = read_raw_cf(&store_path, "tree_state_checkpoint")?;
    let final_roots_cf = read_raw_cf(&store_path, "block_final_note_commitment_roots")?;
    Ok(LoadedStoreSnapshot {
        evidence,
        derived_subtree_roots,
        checkpoints_cf,
        final_roots_cf,
    })
}

fn metric_value(rendered: &str, series: &str) -> f64 {
    rendered
        .lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(0.0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mainnet_mix_pipelined_construction_batches_checkpoints_and_matches_serial()
-> eyre::Result<()> {
    let metrics_handle = install_metrics_recorder(&OpsServer {
        service_name: "zinder-ingest",
        service_version: "0.0.0",
        network_name: "zcash-regtest",
        advertised_capabilities: Arc::from([]),
    })?;
    let activations = regtest_activations();
    let source = build_mainnet_mix_source(PERF_CHAIN_BLOCKS)?;

    let serial_wallet =
        run_load_for_workers(&source, &activations, CanonicalStoreWorkload::Wallet, 1).await?;
    let before_parallel = metrics_handle.render();
    let parallel_wallet =
        run_load_for_workers(&source, &activations, CanonicalStoreWorkload::Wallet, 8).await?;
    let after_parallel = metrics_handle.render();

    let ckpt_samples = metric_value(
        &after_parallel,
        "zinder_ingest_canonical_block_load_stage_duration_seconds_count{stage=\"tree_hashing_checkpoint\"}",
    ) - metric_value(
        &before_parallel,
        "zinder_ingest_canonical_block_load_stage_duration_seconds_count{stage=\"tree_hashing_checkpoint\"}",
    );
    assert!(
        (1.0..=f64::from(PERF_CHAIN_BLOCKS)).contains(&ckpt_samples),
        "expected checkpoint cross-check stage samples in 1..={PERF_CHAIN_BLOCKS}, got {ckpt_samples}"
    );
    assert_eq!(
        metric_value(
            &after_parallel,
            "zinder_ingest_canonical_historical_prevout_reads_total"
        ),
        0.0
    );
    assert_eq!(
        metric_value(
            &after_parallel,
            "zinder_ingest_canonical_cross_block_wallet_reads_total"
        ),
        0.0
    );

    assert_eq!(
        parallel_wallet.evidence.sequence_digest,
        serial_wallet.evidence.sequence_digest
    );
    assert_eq!(
        parallel_wallet.evidence.tip_metadata,
        serial_wallet.evidence.tip_metadata
    );
    assert_eq!(
        parallel_wallet.evidence.tree_state_checkpoint_count,
        u64::from(PERF_CHAIN_BLOCKS) + 1
    );
    assert_eq!(parallel_wallet.checkpoints_cf, serial_wallet.checkpoints_cf);
    assert_eq!(
        parallel_wallet.derived_subtree_roots,
        serial_wallet.derived_subtree_roots
    );

    let serial_explorer =
        run_load_for_workers(&source, &activations, CanonicalStoreWorkload::Explorer, 1).await?;
    let parallel_explorer =
        run_load_for_workers(&source, &activations, CanonicalStoreWorkload::Explorer, 8).await?;
    assert_eq!(
        parallel_explorer.evidence.sequence_digest,
        serial_explorer.evidence.sequence_digest
    );
    assert_eq!(
        parallel_explorer.checkpoints_cf,
        serial_explorer.checkpoints_cf
    );
    assert_eq!(
        parallel_explorer.final_roots_cf,
        serial_explorer.final_roots_cf
    );
    assert_eq!(
        parallel_explorer.final_roots_cf.len(),
        usize::try_from(PERF_CHAIN_BLOCKS)?
    );
    Ok(())
}
