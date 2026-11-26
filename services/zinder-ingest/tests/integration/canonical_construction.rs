#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use zebra_chain::{
    block::{Block as ZebraBlock, Height as ZebraHeight},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeCheckpoint,
    CommitmentTreeFrontier, CommitmentTreeFrontiers, Network, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprintVersion, ShieldedProtocol,
};
use zinder_ingest::{
    CanonicalBlockLoadOutcome, CanonicalConstructionConfig, CanonicalConstructionError,
    CanonicalWriterConfig, CanonicalWriterError, IngestError, admit_canonical_store,
    load_fresh_canonical, load_fresh_canonical_blocks,
};
use zinder_runtime::{
    ComposedReadiness, IngestPhase, OpsServer, Readiness, ReadinessCause, install_metrics_recorder,
};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceBlockHeader, SourceChainSegment,
    SourceChainSegmentLimits, SourceError,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreWorkload,
    RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalStore, RocksDbResourceBudget,
    TreeStateLookup,
};
use zinder_testkit::frontier_from_note_commitments;

use crate::common::construction_fixture::{
    GatedConstructionSource, MultiBlockConstructionSource, gated_construction_writer_config,
    held_construction_writer_config, multi_block_writer_config, regtest_activations,
};

use super::{
    canonical_block_context::shielded_transactions_with_lock_time,
    fixture_block::{
        fixture_ironwood_source_block, fixture_orchard_source_block, fixture_sapling_source_block,
        fixture_source_block, fixture_zakura_ironwood_source_block,
        fixture_zakura_orchard_source_block,
    },
};

#[derive(Clone)]
struct SingleBlockSource {
    block: SourceBlock,
    expected_predecessor: BlockId,
    chain_checkpoint: Option<CommitmentTreeCheckpoint>,
    request_count: Arc<AtomicUsize>,
}

impl SingleBlockSource {
    fn new(block: SourceBlock, expected_predecessor: BlockId) -> Self {
        Self {
            block,
            expected_predecessor,
            chain_checkpoint: None,
            request_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_chain_checkpoint(mut self, chain_checkpoint: CommitmentTreeCheckpoint) -> Self {
        self.chain_checkpoint = Some(chain_checkpoint);
        self
    }

    fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl NodeSource for SingleBlockSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        if height == self.block.height {
            Ok(self.block.clone())
        } else {
            Err(SourceError::BlockUnavailable {
                height,
                reason: "single-block source has no block at the requested height".to_owned(),
            })
        }
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        if limits.cursor.block_id() != Some(self.expected_predecessor) {
            return Err(SourceError::SourceProtocolMismatch {
                reason: "canonical construction did not anchor its first request",
            });
        }
        Ok(SourceChainSegment::connected_blocks([self.block.clone()]))
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        Ok(BlockId::new(self.block.height, self.block.hash))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.chain_checkpoint
            .as_ref()
            .filter(|checkpoint| checkpoint.block_id.height == height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "single-block source has no checkpoint at the requested height".to_owned(),
            })
    }
}

#[tokio::test]
async fn wallet_canonical_blocks_retain_transactions_and_position_compact_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let (temporary, store_path, outcome, expected_tip_metadata) = load_ironwood_fixture(
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
    )
    .await?;
    let CanonicalBlockLoadOutcome {
        builder, evidence, ..
    } = outcome;

    assert_eq!(evidence.block_count, 1);
    assert_eq!(evidence.block_header_count, 1);
    assert_eq!(evidence.block_hash_index_count, 1);
    assert_eq!(evidence.block_replay_count, 1);
    assert_eq!(evidence.compact_block_count, 1);
    assert!(evidence.transaction_location_count > 0);
    assert_eq!(
        evidence.transaction_blob_count,
        evidence.transaction_location_count
    );
    assert_eq!(evidence.block_blob_count, 0);
    assert_eq!(evidence.tree_state_checkpoint_count, 2);
    assert_eq!(evidence.block_final_note_commitment_roots_count, 0);
    assert_eq!(evidence.tip_metadata, expected_tip_metadata);
    drop(builder);

    let error = RocksDbCanonicalStore::open_ready(
        &store_path,
        &regtest_activations(),
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("block-local canonical construction must remain BUILDING")?;
    assert!(matches!(error, CanonicalStoreError::StoreNotReady { .. }));
    assert!(!temporary.path().join("materialized-views").exists());
    Ok(())
}

#[tokio::test]
async fn explorer_canonical_blocks_add_block_blob_retention()
-> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, _store_path, outcome, expected_tip_metadata) =
        load_ironwood_fixture(CanonicalStoreWorkload::Explorer, RawBlobRetention::All).await?;

    assert_eq!(outcome.evidence.block_count, 1);
    assert_eq!(outcome.evidence.block_blob_count, 1);
    assert_eq!(outcome.evidence.tree_state_checkpoint_count, 2);
    assert_eq!(outcome.evidence.block_final_note_commitment_roots_count, 1);
    assert!(outcome.evidence.transaction_location_count > 0);
    assert_eq!(
        outcome.evidence.transaction_blob_count,
        outcome.evidence.transaction_location_count
    );
    assert_eq!(outcome.evidence.tip_metadata, expected_tip_metadata);
    Ok(())
}

#[tokio::test]
async fn wallet_all_retention_keeps_block_blobs_without_explorer_families()
-> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, _store_path, outcome, expected_tip_metadata) =
        load_ironwood_fixture(CanonicalStoreWorkload::Wallet, RawBlobRetention::All).await?;

    assert_eq!(outcome.evidence.block_count, 1);
    assert_eq!(outcome.evidence.block_blob_count, 1);
    assert_eq!(outcome.evidence.block_final_note_commitment_roots_count, 0);
    assert_eq!(outcome.evidence.tip_metadata, expected_tip_metadata);
    Ok(())
}

async fn load_ironwood_fixture(
    workload: CanonicalStoreWorkload,
    raw_blob_retention: RawBlobRetention,
) -> Result<
    (
        TempDir,
        std::path::PathBuf,
        CanonicalBlockLoadOutcome,
        ChainTipMetadata,
    ),
    Box<dyn std::error::Error>,
> {
    let source_block = fixture_ironwood_source_block()?;
    let predecessor_height = source_block
        .height
        .value()
        .checked_sub(1)
        .map(BlockHeight::new)
        .ok_or("fixture must have a predecessor")?;
    let checkpoint = BlockId::new(predecessor_height, source_block.parent_hash);
    let predecessor_frontiers = pre_ironwood_empty_frontiers()?;
    let expected_tip_metadata = ChainTipMetadata::new(0, 0, 2);
    let fixed_tip = BlockId::new(source_block.height, source_block.hash);
    let activations = regtest_activations();
    let build_plan = CanonicalStoreBuildPlan::checkpointed(
        &activations,
        CommitmentTreeCheckpoint::new(
            checkpoint,
            source_block.block_time_seconds.saturating_sub(1),
            predecessor_frontiers,
        ),
        fixed_tip,
        raw_blob_retention,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &store_path,
        workload,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let source = SingleBlockSource::new(source_block, checkpoint);
    let config = CanonicalConstructionConfig::for_local_tests(Duration::from_secs(5), activations);
    let outcome = load_fresh_canonical_blocks(builder, &source, &config).await?;
    Ok((temporary, store_path, outcome, expected_tip_metadata))
}

fn pre_ironwood_empty_frontiers()
-> Result<CommitmentTreeFrontiers, zinder_core::CommitmentTreeFrontiersError> {
    CommitmentTreeFrontiers::from_validated_parts(
        Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
        Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
        None,
    )
}

#[tokio::test]
async fn canonical_blocks_reach_fixed_source_tip_without_wallet_state_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let source_block = fixture_source_block()?;
    assert_eq!(source_block.height, BlockHeight::new(1));
    assert_eq!(
        source_block.parent_hash,
        Network::ZcashRegtest.genesis_hash()
    );
    let fixed_tip = BlockId::new(source_block.height, source_block.hash);
    let activations = regtest_activations();
    let build_plan = CanonicalStoreBuildPlan::complete(
        &activations,
        source_block.block_time_seconds.saturating_sub(1),
        fixed_tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let history_predecessor = build_plan.history_predecessor().block_id;
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &store_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let source = SingleBlockSource::new(source_block, history_predecessor);
    let config = CanonicalConstructionConfig::for_local_tests(Duration::from_secs(5), activations);

    let outcome = load_fresh_canonical_blocks(builder, &source, &config).await?;
    assert_eq!(outcome.builder.build_plan().build_tip(), fixed_tip);
    assert_eq!(outcome.evidence.block_count, 1);
    assert_eq!(outcome.evidence.tip_height, fixed_tip.height);
    assert_eq!(outcome.evidence.tip_hash, fixed_tip.hash);
    assert_eq!(outcome.evidence.block_header_count, 1);
    assert_eq!(outcome.evidence.block_replay_count, 1);
    assert_eq!(outcome.evidence.compact_block_count, 1);
    assert_eq!(outcome.evidence.tree_state_checkpoint_count, 2);
    assert_eq!(outcome.evidence.block_final_note_commitment_roots_count, 0);
    assert_eq!(
        outcome.evidence.transaction_location_count,
        outcome.evidence.transaction_blob_count
    );
    assert!(outcome.evidence.logical_bytes > 0);
    assert!(outcome.evidence.sst_file_bytes > 0);
    drop(outcome.builder);

    let error = RocksDbCanonicalStore::open_ready(
        &store_path,
        &regtest_activations(),
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("block-family construction must remain BUILDING")?;
    assert!(matches!(error, CanonicalStoreError::StoreNotReady { .. }));

    let column_families =
        rust_rocksdb::DB::list_cf(&rust_rocksdb::Options::default(), &store_path)?;
    for wallet_state_family in [
        "address_output_index",
        "transparent_output",
        "transparent_spend_fact",
        "transaction_facts",
    ] {
        assert!(
            !column_families
                .iter()
                .any(|family| family == wallet_state_family)
        );
    }
    assert!(!temporary.path().join("materialized-views").exists());
    Ok(())
}

#[tokio::test]
async fn canonical_source_families_authenticate_empty_subtree_ranges_and_fixed_tip()
-> Result<(), Box<dyn std::error::Error>> {
    let source_block = fixture_source_block()?;
    let fixed_tip = BlockId::new(source_block.height, source_block.hash);
    let activations = regtest_activations();
    let build_plan = CanonicalStoreBuildPlan::complete(
        &activations,
        source_block.block_time_seconds.saturating_sub(1),
        fixed_tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let history_predecessor = build_plan.history_predecessor().block_id;
    let source_checkpoint = CommitmentTreeCheckpoint::new(
        fixed_tip,
        source_block.block_time_seconds,
        CommitmentTreeFrontiers::from_validated_parts(
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
            None,
            None,
        )?,
    );
    let temporary = TempDir::new()?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        temporary.path().join("canonical"),
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let source = SingleBlockSource::new(source_block, history_predecessor)
        .with_chain_checkpoint(source_checkpoint);
    let config = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::clone(&activations),
    );

    let source_outcome = load_fresh_canonical(builder, &source, &config).await?;

    assert_eq!(source_outcome.block_evidence.block_count, 1);
    assert_eq!(source_outcome.subtree_root_evidence.subtree_root_count, 0);
    assert!(source_outcome.builder.is_source_tip_checkpoint_confirmed());
    assert_eq!(source.request_count(), 2);
    Ok(())
}

/// The node holds a sibling of the build tip when construction reads the
/// tip's tree state. Construction stops with a changed-view source error and
/// publishes nothing.
#[tokio::test]
async fn construction_stops_when_the_build_tip_left_the_node_best_chain()
-> Result<(), Box<dyn std::error::Error>> {
    let source_block = fixture_source_block()?;
    let fixed_tip = BlockId::new(source_block.height, source_block.hash);
    let activations = regtest_activations();
    let build_plan = CanonicalStoreBuildPlan::complete(
        &activations,
        source_block.block_time_seconds.saturating_sub(1),
        fixed_tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let history_predecessor = build_plan.history_predecessor().block_id;
    let sibling_checkpoint = CommitmentTreeCheckpoint::new(
        BlockId::new(fixed_tip.height, BlockHash::from_bytes([7; 32])),
        source_block.block_time_seconds,
        CommitmentTreeFrontiers::from_validated_parts(
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
            None,
            None,
        )?,
    );
    assert_ne!(sibling_checkpoint.block_id, fixed_tip);
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &store_path,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let source = SingleBlockSource::new(source_block, history_predecessor)
        .with_chain_checkpoint(sibling_checkpoint);
    let config = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::clone(&activations),
    );

    let error = load_fresh_canonical(builder, &source, &config)
        .await
        .err()
        .ok_or("a build tip outside the node's best chain must not publish")?;

    assert!(
        matches!(
            &error,
            CanonicalConstructionError::Source {
                source: IngestError::Source(SourceError::BlockReorgDuringFetch { height, .. }),
            } if *height == fixed_tip.height
        ),
        "{error}"
    );
    let reopen_error = RocksDbCanonicalStore::open_ready(
        &store_path,
        &activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or("an unconfirmed build tip must leave the store BUILDING")?;
    assert!(matches!(
        reopen_error,
        CanonicalStoreError::StoreNotReady { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn canonical_construction_rejects_source_blocks_from_another_network()
-> Result<(), Box<dyn std::error::Error>> {
    let source_block = fixture_source_block()?;
    let fixed_tip = BlockId::new(source_block.height, source_block.hash);
    let activations = regtest_activations();
    let build_plan = CanonicalStoreBuildPlan::complete(
        &activations,
        source_block.block_time_seconds.saturating_sub(1),
        fixed_tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let history_predecessor = build_plan.history_predecessor().block_id;
    let temporary = TempDir::new()?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        temporary.path().join("canonical"),
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let source = SingleBlockSource::new(
        SourceBlock {
            network: Network::ZcashMainnet,
            ..source_block
        },
        history_predecessor,
    );
    let config = CanonicalConstructionConfig::for_local_tests(Duration::from_secs(5), activations);

    let error = load_fresh_canonical_blocks(builder, &source, &config)
        .await
        .err()
        .ok_or("wrong-network source block must be rejected")?;

    assert!(matches!(
        error,
        CanonicalConstructionError::SourceBlockNetworkMismatch {
            height,
            store_network: Network::ZcashRegtest,
            source_network: Network::ZcashMainnet,
        } if height == BlockHeight::new(1)
    ));
    Ok(())
}

#[tokio::test]
async fn canonical_construction_rejects_activation_mismatch_before_source_work()
-> Result<(), Box<dyn std::error::Error>> {
    let source_block = fixture_source_block()?;
    let fixed_tip = BlockId::new(source_block.height, source_block.hash);
    let activations = regtest_activations();
    let mut store_activation_rows = activations.activations().to_vec();
    let latest_activation = store_activation_rows
        .last_mut()
        .ok_or("regtest activation fixture must not be empty")?;
    latest_activation.activation_height = BlockHeight::new(
        latest_activation
            .activation_height
            .value()
            .saturating_add(1),
    );
    let store_activations =
        NetworkUpgradeActivations::new(Network::ZcashRegtest, store_activation_rows)?;
    assert_ne!(&store_activations, activations.as_ref());
    let store_fingerprint =
        store_activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
    let configured_fingerprint =
        activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
    let build_plan = CanonicalStoreBuildPlan::complete(
        &store_activations,
        source_block.block_time_seconds.saturating_sub(1),
        fixed_tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(100)?,
    )?;
    let history_predecessor = build_plan.history_predecessor().block_id;
    let temporary = TempDir::new()?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        temporary.path().join("canonical"),
        CanonicalStoreWorkload::Wallet,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    let source = SingleBlockSource::new(source_block, history_predecessor);
    let config = CanonicalConstructionConfig::for_local_tests(Duration::from_secs(5), activations);

    let error = load_fresh_canonical_blocks(builder, &source, &config)
        .await
        .err()
        .ok_or("activation mismatch must be rejected")?;

    assert!(matches!(
        error,
        CanonicalConstructionError::NetworkUpgradeActivationsMismatch {
            store_fingerprint: persisted,
            configured_fingerprint: configured,
        } if persisted == store_fingerprint && configured == configured_fingerprint
    ));
    assert_eq!(source.request_count(), 0);
    Ok(())
}

/// Upper bound for a cancelled construction to return, well inside the
/// wallet-serving supervisor's 20-second shutdown grace window.
const CANCELLED_CONSTRUCTION_STOP_BOUND: Duration = Duration::from_secs(5);

/// Serves a one-block chain to the canonical writer and can hold the block
/// stream open or request shutdown with the fixed-tip checkpoint, so shutdown
/// lands while construction is in progress.
#[derive(Clone)]
struct HeldConstructionSource {
    block: SourceBlock,
    chain_checkpoint: CommitmentTreeCheckpoint,
    hold_chain_segments: Arc<AtomicBool>,
    chain_segment_held: Arc<Notify>,
    chain_segment_requests: Arc<AtomicUsize>,
    cancel_with_tip_checkpoint: Option<CancellationToken>,
}

impl HeldConstructionSource {
    fn new(block: SourceBlock, chain_checkpoint: CommitmentTreeCheckpoint) -> Self {
        Self {
            block,
            chain_checkpoint,
            hold_chain_segments: Arc::new(AtomicBool::new(true)),
            chain_segment_held: Arc::new(Notify::new()),
            chain_segment_requests: Arc::new(AtomicUsize::new(0)),
            cancel_with_tip_checkpoint: None,
        }
    }

    /// Streams blocks without holding and cancels `cancel` as it returns the
    /// fixed-tip checkpoint, the last source request before publication.
    fn cancelling_before_publication(
        block: SourceBlock,
        chain_checkpoint: CommitmentTreeCheckpoint,
        cancel: CancellationToken,
    ) -> Self {
        let source = Self {
            cancel_with_tip_checkpoint: Some(cancel),
            ..Self::new(block, chain_checkpoint)
        };
        source.release_chain_segments();
        source
    }

    fn release_chain_segments(&self) {
        self.hold_chain_segments.store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl NodeSource for HeldConstructionSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        if height == BlockHeight::new(0) {
            return Ok(SourceBlock::new(
                SourceBlockHeader {
                    network: Network::ZcashRegtest,
                    height,
                    hash: self.block.parent_hash,
                    parent_hash: self.block.parent_hash,
                    block_time_seconds: self.block.block_time_seconds.saturating_sub(1),
                },
                Vec::new(),
            ));
        }
        if height == self.block.height {
            return Ok(self.block.clone());
        }
        Err(SourceError::BlockUnavailable {
            height,
            reason: "held construction source has no block at the requested height".to_owned(),
        })
    }

    async fn fetch_chain_segment(
        &self,
        _limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.chain_segment_requests.fetch_add(1, Ordering::SeqCst);
        if self.hold_chain_segments.load(Ordering::SeqCst) {
            self.chain_segment_held.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(SourceChainSegment::connected_blocks([self.block.clone()]))
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        Ok(BlockId::new(self.block.height, self.block.hash))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        if height == self.chain_checkpoint.block_id.height {
            if let Some(cancel) = &self.cancel_with_tip_checkpoint {
                cancel.cancel();
            }
            return Ok(self.chain_checkpoint.clone());
        }
        Err(SourceError::BlockUnavailable {
            height,
            reason: "held construction source has no checkpoint at the requested height".to_owned(),
        })
    }
}

fn held_construction_chain()
-> Result<(SourceBlock, CommitmentTreeCheckpoint), Box<dyn std::error::Error>> {
    let source_block = fixture_source_block()?;
    let fixed_tip = BlockId::new(source_block.height, source_block.hash);
    let chain_checkpoint = CommitmentTreeCheckpoint::new(
        fixed_tip,
        source_block.block_time_seconds,
        CommitmentTreeFrontiers::from_validated_parts(
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
            None,
            None,
        )?,
    );
    Ok((source_block, chain_checkpoint))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_construction_stops_promptly_and_the_next_start_rebuilds()
-> Result<(), Box<dyn std::error::Error>> {
    Box::pin(assert_shutdown_during_construction_stops_promptly(1)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_idle_tree_hashing_workers_stops_promptly_and_the_next_start_rebuilds()
-> Result<(), Box<dyn std::error::Error>> {
    Box::pin(assert_shutdown_during_construction_stops_promptly(4)).await
}

async fn assert_shutdown_during_construction_stops_promptly(
    tree_hashing_workers: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let (source_block, chain_checkpoint) = held_construction_chain()?;
    let fixed_tip = chain_checkpoint.block_id;
    let source = HeldConstructionSource::new(source_block, chain_checkpoint);
    let temporary = TempDir::new()?;
    let mut config =
        held_construction_writer_config(temporary.path().join("canonical"), regtest_activations());
    config.construction.pipeline_limits.tree_hashing_workers =
        NonZeroU32::new(tree_hashing_workers).ok_or("tree hashing workers must be non-zero")?;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();

    let admission =
        admit_canonical_store(&source, regtest_activations(), &config, &readiness, &cancel);
    tokio::pin!(admission);
    tokio::select! {
        () = source.chain_segment_held.notified() => {}
        _ = &mut admission => {
            return Err("construction finished before its block stream was held".into());
        }
    }
    cancel.cancel();
    let stopped = tokio::time::timeout(CANCELLED_CONSTRUCTION_STOP_BOUND, admission)
        .await
        .map_err(|_| "cancelled construction outlived its shutdown bound")?;
    assert!(matches!(stopped, Err(CanonicalWriterError::Cancelled)));
    assert!(!config.storage_path.exists());
    // Shutdown leaves the block-load staging for the next start to discard.
    assert!(
        temporary
            .path()
            .join("canonical.building.block-load-staging")
            .exists()
    );

    // Reopening the staging proves the cancelled loader already closed it.
    source.release_chain_segments();
    let store = admit_canonical_store(
        &source,
        regtest_activations(),
        &config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(store.event_fence().visible_tip(), fixed_tip);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_before_publication_finishes_publishing_and_the_next_start_installs_it()
-> Result<(), Box<dyn std::error::Error>> {
    let (source_block, chain_checkpoint) = held_construction_chain()?;
    let fixed_tip = chain_checkpoint.block_id;
    let cancel = CancellationToken::new();
    let source = HeldConstructionSource::cancelling_before_publication(
        source_block,
        chain_checkpoint,
        cancel.clone(),
    );
    let temporary = TempDir::new()?;
    let config =
        held_construction_writer_config(temporary.path().join("canonical"), regtest_activations());
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);

    // Publication cannot be interrupted, so admission returns only after it.
    let stopped =
        admit_canonical_store(&source, regtest_activations(), &config, &readiness, &cancel).await;
    assert!(matches!(stopped, Err(CanonicalWriterError::Cancelled)));
    assert!(!config.storage_path.exists());
    assert!(temporary.path().join("canonical.building").exists());
    let block_requests = source.chain_segment_requests.load(Ordering::SeqCst);

    // The next start installs the staged READY store without rebuilding it.
    let store = admit_canonical_store(
        &source,
        regtest_activations(),
        &config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(store.event_fence().visible_tip(), fixed_tip);
    assert_eq!(
        source.chain_segment_requests.load(Ordering::SeqCst),
        block_requests
    );
    Ok(())
}

/// Every construction phase and serial loader stage records a duration.
///
/// The loader runs on a blocking thread, which a thread-local recorder does
/// not reach, so this test installs a process-wide one. Other tests in a
/// shared libtest process may add samples, so counts are lower bounds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn construction_times_each_phase_and_serial_loader_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let recorded_metrics = install_metrics_recorder(&OpsServer {
        service_name: "zinder-ingest",
        service_version: "0.0.0",
        network_name: "zcash-regtest",
        advertised_capabilities: Arc::from([]),
    })?;
    let (source_block, chain_checkpoint) = held_construction_chain()?;
    let source = HeldConstructionSource::new(source_block, chain_checkpoint);
    source.release_chain_segments();
    let temporary = TempDir::new()?;
    let config =
        held_construction_writer_config(temporary.path().join("canonical"), regtest_activations());
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);

    admit_canonical_store(
        &source,
        regtest_activations(),
        &config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;

    let rendered = recorded_metrics.render();
    let sample_count = |series: &str| {
        rendered
            .lines()
            .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
            .and_then(|count| count.parse::<u64>().ok())
    };
    let mut expected_series = Vec::new();
    for phase in [
        "construction_range",
        "block_load",
        "source_families",
        "publication",
    ] {
        expected_series.push(format!(
            "zinder_ingest_canonical_construction_phase_duration_seconds_count{{phase=\"{phase}\"}}"
        ));
    }
    // The one-block chain's only block is the build tip, which carries a
    // tree-state checkpoint.
    for stage in [
        "receive_wait",
        "commitment_extract",
        "tree_hashing_append",
        "position",
        "tree_hashing_checkpoint",
    ] {
        expected_series.push(format!(
            "zinder_ingest_canonical_block_load_stage_duration_seconds_count{{stage=\"{stage}\"}}"
        ));
    }
    expected_series.push(
        "zinder_store_canonical_block_load_stage_duration_seconds_count{stage=\"write\"}"
            .to_owned(),
    );
    for series in expected_series {
        assert!(
            sample_count(&series).is_some_and(|count| count >= 1),
            "missing {series}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_block_construction_stores_every_height_while_sparse_construction_keeps_cadence_and_tip()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    // 6-block source with reorg_window_blocks = 1 fixes the build tip at 5.
    let source = MultiBlockConstructionSource::build(6, &activations)?;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);

    let dense_dir = TempDir::new()?;
    let dense_config = multi_block_writer_config(
        dense_dir.path().join("canonical"),
        Arc::clone(&activations),
        true,
    );
    let dense_store = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &dense_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        dense_store.event_fence().visible_tip().height,
        BlockHeight::new(5)
    );
    for height in 0_u32..=5 {
        let lookup = dense_store.tree_state_lookup(BlockHeight::new(height), &activations)?;
        assert!(
            matches!(lookup, Some(TreeStateLookup::Stored(_))),
            "dense construction must store checkpoint at height {height}"
        );
        let stored =
            dense_store.tree_state_checkpoint_at(BlockHeight::new(height), &activations)?;
        assert_eq!(stored.as_ref(), Some(source.checkpoint_at(height)?));
    }

    let sparse_dir = TempDir::new()?;
    let sparse_config = multi_block_writer_config(
        sparse_dir.path().join("canonical"),
        Arc::clone(&activations),
        false,
    );
    let sparse_store = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &sparse_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        sparse_store.event_fence().visible_tip().height,
        BlockHeight::new(5)
    );
    for height in [0_u32, 5] {
        assert!(matches!(
            sparse_store.tree_state_lookup(BlockHeight::new(height), &activations)?,
            Some(TreeStateLookup::Stored(_))
        ));
    }
    for height in 1_u32..=4 {
        assert!(
            matches!(
                sparse_store.tree_state_lookup(BlockHeight::new(height), &activations)?,
                Some(TreeStateLookup::Replay(_))
            ),
            "sparse construction must keep off-cadence height {height} unstored"
        );
        let replayed =
            sparse_store.tree_state_checkpoint_at(BlockHeight::new(height), &activations)?;
        assert_eq!(replayed.as_ref(), Some(source.checkpoint_at(height)?));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commitment_budget_construction_serves_the_same_tree_states_as_every_block()
-> Result<(), Box<dyn std::error::Error>> {
    // The loader selects budget checkpoints in height order whether it hashes
    // serially or ahead on workers, so both place them at the same heights.
    for tree_hashing_workers in [1_u32, 4] {
        Box::pin(assert_commitment_budget_matches_every_block(
            tree_hashing_workers,
        ))
        .await?;
    }
    Ok(())
}

async fn assert_commitment_budget_matches_every_block(
    tree_hashing_workers: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    use zinder_store::TreeStateCheckpointSelection::CommitmentBudget;

    let activations = regtest_activations();
    let budget = NonZeroU32::new(3).ok_or("zero commitment budget")?;
    let source = commitment_budget_construction_source(&activations)?;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let directory = TempDir::new()?;
    let mut dense = multi_block_writer_config(
        directory.path().join("every-block"),
        Arc::clone(&activations),
        true,
    );
    dense.construction.pipeline_limits.tree_hashing_workers =
        NonZeroU32::new(tree_hashing_workers).ok_or("tree hashing workers must be non-zero")?;
    let budgeted = CanonicalWriterConfig {
        storage_path: directory.path().join("commitment-budget"),
        tree_state_checkpoint_selection: Some(CommitmentBudget(budget)),
        ..dense.clone()
    };
    let cancel = CancellationToken::new();
    let dense = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &dense,
        &readiness,
        &cancel,
    )
    .await?;
    let budgeted = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &budgeted,
        &readiness,
        &cancel,
    )
    .await?;
    for height in 0..=104 {
        let at = BlockHeight::new(height);
        let lookup = budgeted.tree_state_lookup(at, &activations)?;
        assert_eq!(
            matches!(lookup, Some(TreeStateLookup::Stored(_))),
            matches!(height, 0 | 95 | 98 | 100 | 102 | 104),
            "unexpected checkpoint coverage at height {height} with {tree_hashing_workers} workers"
        );
        let expected = dense
            .tree_state_checkpoint_at(at, &activations)?
            .ok_or("every-block checkpoint absent")?;
        if let Some(TreeStateLookup::Replay(replay)) = lookup {
            let start = replay.seed().tip_metadata();
            let end = expected.tip_metadata();
            let replayed =
                u64::from(end.sapling_commitment_tree_size - start.sapling_commitment_tree_size)
                    + u64::from(
                        end.orchard_commitment_tree_size - start.orchard_commitment_tree_size,
                    )
                    + u64::from(
                        end.ironwood_commitment_tree_size - start.ironwood_commitment_tree_size,
                    );
            assert!(
                replayed < u64::from(budget.get()),
                "replay at height {height} reached the shared budget"
            );
        }
        assert_eq!(
            budgeted
                .tree_state_checkpoint_at(at, &activations)?
                .as_ref(),
            Some(&expected),
        );
    }
    Ok(())
}

fn commitment_budget_construction_source(
    activations: &NetworkUpgradeActivations,
) -> Result<MultiBlockConstructionSource, Box<dyn std::error::Error>> {
    let sapling = fixture_sapling_source_block()?;
    let orchard = fixture_orchard_source_block()?;
    // One block exceeds the budget, later blocks reach it jointly, and a
    // final sub-budget block immediately precedes the required tip checkpoint.
    let source =
        MultiBlockConstructionSource::build_with_transactions(105, activations, |height| {
            let fixture = match height {
                95 => {
                    let mut transactions =
                        shielded_transactions_with_lock_time(&orchard, Some(95))?;
                    transactions.extend(shielded_transactions_with_lock_time(&orchard, Some(195))?);
                    return Ok(transactions);
                }
                97 | 99 | 102 | 103 => &sapling,
                98 | 101 => &orchard,
                _ => return Ok(Vec::new()),
            };
            // Distinct lock times give repeated shielded bundles distinct transaction IDs.
            shielded_transactions_with_lock_time(fixture, Some(height))
        })?;
    let sizes_at = |height| {
        source
            .checkpoint_at(height)
            .map(CommitmentTreeCheckpoint::tip_metadata)
    };
    assert_eq!(
        sizes_at(95)?.orchard_commitment_tree_size - sizes_at(94)?.orchard_commitment_tree_size,
        4
    );
    assert_eq!(
        sizes_at(103)?.sapling_commitment_tree_size - sizes_at(102)?.sapling_commitment_tree_size,
        1
    );
    Ok(source)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_block_construction_preserves_checkpointed_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    let source = MultiBlockConstructionSource::build(6, &activations)?;
    let temporary = TempDir::new()?;
    let mut config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        true,
    );
    config.checkpoint_height = Some(BlockHeight::new(2));
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);

    let store = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;

    assert_eq!(
        store.history_bounds().first_available_height(),
        BlockHeight::new(3)
    );
    assert_eq!(
        store.event_fence().visible_tip().height,
        BlockHeight::new(5)
    );
    assert!(
        store
            .tree_state_lookup(BlockHeight::new(1), &activations)?
            .is_none()
    );
    for height in 2_u32..=5 {
        assert!(
            matches!(
                store.tree_state_lookup(BlockHeight::new(height), &activations)?,
                Some(TreeStateLookup::Stored(_))
            ),
            "checkpointed dense construction must store height {height}"
        );
        let stored = store.tree_state_checkpoint_at(BlockHeight::new(height), &activations)?;
        assert_eq!(stored.as_ref(), Some(source.checkpoint_at(height)?));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_every_block_construction_discards_unpublished_staging_and_rebuilds_densely()
-> Result<(), Box<dyn std::error::Error>> {
    Box::pin(assert_interrupted_dense_construction_rebuilds(1)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_parallel_dense_construction_discards_unpublished_staging_and_rebuilds()
-> Result<(), Box<dyn std::error::Error>> {
    Box::pin(assert_interrupted_dense_construction_rebuilds(4)).await
}

async fn assert_interrupted_dense_construction_rebuilds(
    tree_hashing_workers: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    let source = MultiBlockConstructionSource::build(6, &activations)?;
    let reached = Arc::new(Notify::new());
    let mut interrupted_source = source.clone();
    interrupted_source.hold_checkpoint = Some((BlockHeight::new(5), Arc::clone(&reached)));
    let temporary = TempDir::new()?;
    let mut config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        true,
    );
    config.construction.pipeline_limits.tree_hashing_workers =
        NonZeroU32::new(tree_hashing_workers).ok_or("tree hashing workers must be non-zero")?;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();

    let admission = admit_canonical_store(
        &interrupted_source,
        Arc::clone(&activations),
        &config,
        &readiness,
        &cancel,
    );
    tokio::pin!(admission);
    tokio::select! {
        () = reached.notified() => {}
        _ = &mut admission => {
            return Err("construction finished before the build-tip checkpoint request".into());
        }
    }
    cancel.cancel();
    let stopped = tokio::time::timeout(CANCELLED_CONSTRUCTION_STOP_BOUND, admission)
        .await
        .map_err(|_| "cancelled dense construction outlived its shutdown bound")?;
    assert!(matches!(stopped, Err(CanonicalWriterError::Cancelled)));
    assert!(!config.storage_path.exists());
    {
        let staging = temporary.path().join("canonical.building");
        let options = rust_rocksdb::Options::default();
        let families = rust_rocksdb::DB::list_cf(&options, &staging)?;
        let db = rust_rocksdb::DB::open_cf_for_read_only(&options, &staging, families, false)?;
        let checkpoints = db
            .cf_handle("tree_state_checkpoint")
            .ok_or("missing checkpoint family")?;
        let key = zinder_core::wire::encode_height_key_ascending(BlockHeight::new(2));
        assert!(
            db.get_cf(&checkpoints, key)?.is_some(),
            "off-cadence checkpoint must survive cancellation in staging"
        );
    }
    let requests_before_restart = source.chain_segment_requests.load(Ordering::SeqCst);
    let store = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        store.event_fence().visible_tip().height,
        BlockHeight::new(5)
    );
    assert!(source.chain_segment_requests.load(Ordering::SeqCst) > requests_before_restart);
    for height in 0_u32..=5 {
        assert!(matches!(
            store.tree_state_lookup(BlockHeight::new(height), &activations)?,
            Some(TreeStateLookup::Stored(_))
        ));
        assert_eq!(
            store
                .tree_state_checkpoint_at(BlockHeight::new(height), &activations)?
                .as_ref(),
            Some(source.checkpoint_at(height)?),
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_ready_dense_store_installs_on_restart_even_if_config_switches_to_sparse()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    let cancel = CancellationToken::new();
    let source = MultiBlockConstructionSource::build(6, &activations)?
        .cancelling_at_checkpoint(BlockHeight::new(5), cancel.clone());
    let temporary = TempDir::new()?;
    let dense_config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        true,
    );
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);

    let stopped = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &dense_config,
        &readiness,
        &cancel,
    )
    .await;
    assert!(matches!(stopped, Err(CanonicalWriterError::Cancelled)));
    assert!(!dense_config.storage_path.exists());
    assert!(temporary.path().join("canonical.building").exists());
    let segment_requests_before_restart = source.chain_segment_requests.load(Ordering::SeqCst);

    let sparse_config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        false,
    );
    let store = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &sparse_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        source.chain_segment_requests.load(Ordering::SeqCst),
        segment_requests_before_restart
    );
    for height in 0_u32..=5 {
        assert!(matches!(
            store.tree_state_lookup(BlockHeight::new(height), &activations)?,
            Some(TreeStateLookup::Stored(_))
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prior_sparse_store_reopens_with_dense_construction_enabled_without_rebuilding()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    let source = MultiBlockConstructionSource::build(6, &activations)?;
    let temporary = TempDir::new()?;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);

    let sparse_config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        false,
    );
    let sparse_store = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &sparse_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    let expected_fence = sparse_store.event_fence();
    drop(sparse_store);
    let segment_requests_after_build = source.chain_segment_requests.load(Ordering::SeqCst);

    let dense_config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        true,
    );
    let reopened = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &dense_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(reopened.event_fence(), expected_fence);
    assert_eq!(
        source.chain_segment_requests.load(Ordering::SeqCst),
        segment_requests_after_build
    );
    for height in 1_u32..=4 {
        assert!(matches!(
            reopened.tree_state_lookup(BlockHeight::new(height), &activations)?,
            Some(TreeStateLookup::Replay(_))
        ));
    }
    Ok(())
}

/// Multi-block Regtest source crossing NU6.3 (Ironwood) activation at height 603.
///
/// Uses captured Zebra and Zakura regtest blocks and derives expected
/// checkpoints independently from raw consensus block bytes.
#[derive(Clone)]
struct IronwoodActivationConstructionSource {
    blocks: Vec<SourceBlock>,
    expected_checkpoints: Vec<CommitmentTreeCheckpoint>,
}

impl IronwoodActivationConstructionSource {
    fn build(activations: &NetworkUpgradeActivations) -> Result<Self, Box<dyn std::error::Error>> {
        let ironwood_603_fixture = fixture_ironwood_source_block()?;
        let base_time = ironwood_603_fixture.block_time_seconds;
        let predecessor_block_id =
            BlockId::new(BlockHeight::new(601), BlockHash::from_bytes([0x61; 32]));
        let predecessor_time = base_time.saturating_sub(20);

        let block_602 = relink_captured_block(
            fixture_zakura_orchard_source_block()?,
            BlockHeight::new(602),
            predecessor_block_id.hash,
            base_time.saturating_sub(10),
            Some((
                &[0xff, 0xff, 0xff, 0xff, 0x29, 0x01, 0x6f],
                &[0xff, 0xff, 0xff, 0xff, 0x2a, 0x02, 0x5a, 0x02],
            )),
        )?;
        let block_603 = relink_captured_block(
            ironwood_603_fixture,
            BlockHeight::new(603),
            block_602.hash,
            base_time,
            None,
        )?;
        let block_604 = relink_captured_block(
            fixture_zakura_ironwood_source_block()?,
            BlockHeight::new(604),
            block_603.hash,
            base_time.saturating_add(10),
            Some((
                &[0xff, 0xff, 0xff, 0xff, 0x2a, 0x02, 0x84, 0x00],
                &[0xff, 0xff, 0xff, 0xff, 0x2a, 0x02, 0x5c, 0x02],
            )),
        )?;
        let template = fixture_zakura_orchard_source_block()?;
        let block_605 = relink_captured_block(
            coinbase_only_source_block(&template, 604)?,
            BlockHeight::new(605),
            block_604.hash,
            base_time.saturating_add(20),
            Some((
                &[0xff, 0xff, 0xff, 0xff, 0x29, 0x01, 0x6f],
                &[0xff, 0xff, 0xff, 0xff, 0x2a, 0x02, 0x5d, 0x02],
            )),
        )?;
        let block_606 = relink_captured_block(
            coinbase_only_source_block(&template, 605)?,
            BlockHeight::new(606),
            block_605.hash,
            base_time.saturating_add(30),
            Some((
                &[0xff, 0xff, 0xff, 0xff, 0x29, 0x01, 0x6f],
                &[0xff, 0xff, 0xff, 0xff, 0x2a, 0x02, 0x5e, 0x02],
            )),
        )?;

        let blocks = vec![block_602, block_603, block_604, block_605, block_606];
        let expected_checkpoints = derive_independent_checkpoints(
            predecessor_block_id,
            predecessor_time,
            &blocks,
            activations,
        )?;
        Ok(Self {
            blocks,
            expected_checkpoints,
        })
    }

    fn expected_checkpoint_at(
        &self,
        height: u32,
    ) -> Result<&CommitmentTreeCheckpoint, Box<dyn std::error::Error>> {
        self.expected_checkpoints
            .iter()
            .find(|checkpoint| checkpoint.block_id.height == BlockHeight::new(height))
            .ok_or_else(|| {
                format!("missing independent expected checkpoint at height {height}").into()
            })
    }
}

#[async_trait]
impl NodeSource for IronwoodActivationConstructionSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        self.blocks
            .iter()
            .find(|block| block.height == height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "Ironwood activation test source has no block at the requested height"
                    .to_owned(),
            })
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        let next_height = limits
            .cursor
            .block_id()
            .map_or(602, |cursor| cursor.height.value().saturating_add(1));
        let max_blocks = usize::try_from(limits.max_connected_blocks.get()).unwrap_or(usize::MAX);
        let segment = self
            .blocks
            .iter()
            .filter(|block| block.height.value() >= next_height)
            .take(max_blocks)
            .cloned()
            .collect::<Vec<_>>();
        Ok(SourceChainSegment::connected_blocks(segment))
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        let tip = self
            .blocks
            .last()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height: BlockHeight::new(606),
                reason: "Ironwood activation test source has no blocks".to_owned(),
            })?;
        Ok(BlockId::new(tip.height, tip.hash))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.expected_checkpoints
            .iter()
            .find(|checkpoint| checkpoint.block_id.height == height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "Ironwood activation test source has no checkpoint at the requested height"
                    .to_owned(),
            })
    }
}

fn coinbase_only_source_block(
    source_block: &SourceBlock,
    lock_height: u32,
) -> Result<SourceBlock, Box<dyn std::error::Error>> {
    let mut parsed_block: ZebraBlock = source_block
        .raw_block_bytes
        .as_slice()
        .zcash_deserialize_into()?;
    let coinbase = parsed_block
        .transactions
        .first()
        .ok_or("missing fixture coinbase")?;
    let mut bytes = coinbase.zcash_serialize_to_vec()?;
    // ZIP 244 excludes the coinbase input script from the txid. Use a distinct
    // v5 lock height so the repeated transparent coinbase has a distinct txid.
    assert_eq!(
        bytes.get(..4),
        Some(0x8000_0005_u32.to_le_bytes().as_slice())
    );
    bytes
        .get_mut(12..16)
        .ok_or("missing v5 lock time")?
        .copy_from_slice(&lock_height.to_le_bytes());
    parsed_block.transactions = vec![Arc::new(bytes.as_slice().zcash_deserialize_into()?)];
    let raw_bytes = parsed_block.zcash_serialize_to_vec()?;
    Ok(SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        source_block.height,
        raw_bytes,
    )?)
}

fn relink_captured_block(
    source_block: SourceBlock,
    target_height: BlockHeight,
    parent_hash: BlockHash,
    block_time_seconds: u32,
    coinbase_height_patch: Option<(&[u8], &[u8])>,
) -> Result<SourceBlock, Box<dyn std::error::Error>> {
    let mut raw_bytes = source_block.raw_block_bytes;
    raw_bytes
        .get_mut(4..36)
        .ok_or("captured block is too short to rewrite parent hash")?
        .copy_from_slice(&parent_hash.as_bytes());
    raw_bytes
        .get_mut(100..104)
        .ok_or("captured block is too short to rewrite header timestamp")?
        .copy_from_slice(&block_time_seconds.to_le_bytes());
    if let Some((old_prefix, new_prefix)) = coinbase_height_patch {
        let mut matches = raw_bytes
            .windows(old_prefix.len())
            .enumerate()
            .filter_map(|(offset, window)| (window == old_prefix).then_some(offset));
        let offset = matches
            .next()
            .ok_or("captured block coinbase height prefix not found")?;
        if matches.next().is_some() {
            return Err("captured block coinbase height prefix was not unique".into());
        }
        raw_bytes.splice(
            offset..offset.saturating_add(old_prefix.len()),
            new_prefix.iter().copied(),
        );
    }
    let parsed_block: ZebraBlock = raw_bytes.as_slice().zcash_deserialize_into()?;
    if parsed_block.coinbase_height() != Some(ZebraHeight(target_height.value())) {
        return Err(format!(
            "relinked block coinbase height {:?} does not match target {}",
            parsed_block.coinbase_height(),
            target_height.value()
        )
        .into());
    }
    Ok(SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        target_height,
        raw_bytes,
    )?)
}

/// Derives per-height expected checkpoints independently from consensus block
/// bytes without invoking `prepare_canonical_block` or
/// `CommitmentTreeAccumulator`.
fn derive_independent_checkpoints(
    predecessor_block_id: BlockId,
    predecessor_time: u32,
    blocks: &[SourceBlock],
    activations: &NetworkUpgradeActivations,
) -> Result<Vec<CommitmentTreeCheckpoint>, Box<dyn std::error::Error>> {
    let ironwood_activation = activations
        .activation_height_by_name(ShieldedProtocol::Ironwood.activation_upgrade_name())
        .ok_or("regtest activations must define Ironwood activation height")?;
    let mut sapling_commitments = Vec::new();
    let mut orchard_commitments = Vec::new();
    let mut ironwood_commitments = Vec::new();

    let predecessor_frontiers = CommitmentTreeFrontiers::from_validated_parts(
        Some(frontier_from_note_commitments(
            ShieldedProtocol::Sapling,
            &sapling_commitments,
        )?),
        Some(frontier_from_note_commitments(
            ShieldedProtocol::Orchard,
            &orchard_commitments,
        )?),
        None,
    )?;
    let mut checkpoints = vec![CommitmentTreeCheckpoint::new(
        predecessor_block_id,
        predecessor_time,
        predecessor_frontiers,
    )];

    for block in blocks {
        let parsed_block: ZebraBlock = block.raw_block_bytes.as_slice().zcash_deserialize_into()?;
        for transaction in &parsed_block.transactions {
            for commitment in transaction.sapling_note_commitments() {
                sapling_commitments.push(commitment.to_bytes());
            }
            for commitment in transaction.orchard_note_commitments() {
                orchard_commitments.push(commitment.to_bytes());
            }
            for commitment in transaction.ironwood_note_commitments() {
                ironwood_commitments.push(commitment.to_bytes());
            }
        }
        let ironwood_frontier = if block.height >= ironwood_activation {
            Some(frontier_from_note_commitments(
                ShieldedProtocol::Ironwood,
                &ironwood_commitments,
            )?)
        } else {
            None
        };
        let frontiers = CommitmentTreeFrontiers::from_validated_parts(
            Some(frontier_from_note_commitments(
                ShieldedProtocol::Sapling,
                &sapling_commitments,
            )?),
            Some(frontier_from_note_commitments(
                ShieldedProtocol::Orchard,
                &orchard_commitments,
            )?),
            ironwood_frontier,
        )?;
        checkpoints.push(CommitmentTreeCheckpoint::new(
            BlockId::new(block.height, block.hash),
            block.block_time_seconds,
            frontiers,
        ));
    }
    Ok(checkpoints)
}

fn assert_stored_checkpoint_matches(
    store: &RocksDbCanonicalStore,
    height: BlockHeight,
    expected: &CommitmentTreeCheckpoint,
    activations: &NetworkUpgradeActivations,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(TreeStateLookup::Stored(served)) = store.tree_state_lookup(height, activations)?
    else {
        return Err(format!(
            "dense construction crossing Ironwood activation must store checkpoint at height {}",
            height.value()
        )
        .into());
    };
    assert_eq!(served.block_id(), expected.block_id);
    assert_eq!(served.block_time_seconds(), expected.block_time_seconds);
    assert_eq!(
        served.final_state_bytes(ShieldedProtocol::Sapling),
        expected
            .frontiers
            .sapling()
            .map(CommitmentTreeFrontier::final_state_bytes)
    );
    assert_eq!(
        served.final_state_bytes(ShieldedProtocol::Orchard),
        expected
            .frontiers
            .orchard()
            .map(CommitmentTreeFrontier::final_state_bytes)
    );
    assert_eq!(
        served.final_state_bytes(ShieldedProtocol::Ironwood),
        expected
            .frontiers
            .ironwood()
            .map(CommitmentTreeFrontier::final_state_bytes)
    );
    let stored = store.tree_state_checkpoint_at(height, activations)?;
    assert_eq!(stored.as_ref(), Some(expected));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_block_construction_stores_independent_checkpoints_across_ironwood_activation()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    let source = IronwoodActivationConstructionSource::build(&activations)?;
    let mut event_fences = Vec::new();
    // Parallel tree hashing must store the checkpoints a serial loader stores.
    for tree_hashing_workers in [1_u32, 4] {
        let (_directory, store) =
            build_ironwood_activation_store(&source, &activations, tree_hashing_workers).await?;
        assert_ironwood_activation_checkpoints(&store, &source, &activations)?;
        event_fences.push(store.event_fence());
    }
    assert_eq!(event_fences.first(), event_fences.last());
    Ok(())
}

async fn build_ironwood_activation_store(
    source: &IronwoodActivationConstructionSource,
    activations: &Arc<NetworkUpgradeActivations>,
    tree_hashing_workers: u32,
) -> Result<(TempDir, RocksDbCanonicalStore), Box<dyn std::error::Error>> {
    let temporary = TempDir::new()?;
    let mut config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(activations),
        true,
    );
    config.checkpoint_height = Some(BlockHeight::new(601));
    config.construction.pipeline_limits.tree_hashing_workers =
        NonZeroU32::new(tree_hashing_workers).ok_or("tree hashing workers must be non-zero")?;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);

    let store = admit_canonical_store(
        source,
        Arc::clone(activations),
        &config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    Ok((temporary, store))
}

fn assert_ironwood_activation_checkpoints(
    store: &RocksDbCanonicalStore,
    source: &IronwoodActivationConstructionSource,
    activations: &NetworkUpgradeActivations,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        store.history_bounds().first_available_height(),
        BlockHeight::new(602)
    );
    assert_eq!(
        store.event_fence().visible_tip().height,
        BlockHeight::new(605)
    );

    for height in 601_u32..=605 {
        let expected = source.expected_checkpoint_at(height)?;
        assert_stored_checkpoint_matches(store, BlockHeight::new(height), expected, activations)?;
    }

    assert!(
        source
            .expected_checkpoint_at(602)?
            .frontiers
            .ironwood()
            .is_none()
    );
    let activation_ironwood = source
        .expected_checkpoint_at(603)?
        .frontiers
        .ironwood()
        .ok_or("Ironwood frontier must be present at activation height 603")?;
    let post_activation_ironwood = source
        .expected_checkpoint_at(604)?
        .frontiers
        .ironwood()
        .ok_or("Ironwood frontier must be present at post-activation height 604")?;
    let tip_ironwood = source
        .expected_checkpoint_at(605)?
        .frontiers
        .ironwood()
        .ok_or("Ironwood frontier must be present at build tip height 605")?;
    assert_eq!(activation_ironwood.tree_size(), 2);
    assert_eq!(post_activation_ironwood.tree_size(), 5);
    assert_eq!(tip_ironwood.tree_size(), 5);
    assert_ne!(
        activation_ironwood.final_root(),
        post_activation_ironwood.final_root()
    );
    assert_eq!(post_activation_ironwood, tip_ironwood);
    Ok(())
}

async fn wait_for_composed_progress(
    composed: &ComposedReadiness,
    expected_current: u32,
    expected_target: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let report = composed.runtime_readiness().report();
        if report.current_height == Some(expected_current)
            && report.target_height == Some(expected_target)
        {
            assert!(!report.is_ready);
            assert_eq!(report.phase, Some(IngestPhase::BulkCatchup));
            assert_eq!(
                report.cause,
                ReadinessCause::Syncing {
                    lag_blocks: Some(u64::from(expected_target.saturating_sub(expected_current))),
                    wait_reason: None,
                }
            );
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for readiness progress {expected_current}/{expected_target}; last={report:?}"
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_construction_with_tree_hashing_workers_and_checkpoint_advances_readiness_lag()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    let source =
        GatedConstructionSource::new(MultiBlockConstructionSource::build(10, &activations)?);
    let temporary = TempDir::new()?;
    let mut config = gated_construction_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        4,
    )?;
    config.checkpoint_height = Some(BlockHeight::new(2));
    let composed = ComposedReadiness::new(Readiness::default(), &[]);

    let admission_task = tokio::spawn({
        let source = source.clone();
        let activations = Arc::clone(&activations);
        let composed = composed.clone();
        async move {
            admit_canonical_store(
                &source,
                activations,
                &config,
                &composed,
                &CancellationToken::new(),
            )
            .await
        }
    });

    wait_for_composed_progress(&composed, 2, 9).await?;
    source.allow_through(5);
    wait_for_composed_progress(&composed, 5, 9).await?;
    source.allow_through(9);
    let store = admission_task.await??;
    wait_for_composed_progress(&composed, 9, 9).await?;
    assert_eq!(
        store.event_fence().visible_tip().height,
        BlockHeight::new(9)
    );
    Ok(())
}

type ColumnFamilyEntries = Vec<(Vec<u8>, Vec<u8>)>;

fn read_cf_entries(
    store_path: &std::path::Path,
    family_name: &str,
) -> Result<ColumnFamilyEntries, Box<dyn std::error::Error>> {
    let options = rust_rocksdb::Options::default();
    let families = rust_rocksdb::DB::list_cf(&options, store_path)?;
    let db = rust_rocksdb::DB::open_cf_for_read_only(&options, store_path, families, false)?;
    let family = db
        .cf_handle(family_name)
        .ok_or_else(|| format!("missing column family {family_name}"))?;
    let mut entries = Vec::new();
    for entry in db.iterator_cf(&family, rust_rocksdb::IteratorMode::Start) {
        let (key, bytes) = entry?;
        entries.push((key.to_vec(), bytes.to_vec()));
    }
    Ok(entries)
}

fn dense_multi_pool_source(
    activations: &NetworkUpgradeActivations,
) -> Result<MultiBlockConstructionSource, Box<dyn std::error::Error>> {
    let sapling = fixture_sapling_source_block()?;
    let orchard = fixture_orchard_source_block()?;
    MultiBlockConstructionSource::build_with_transactions(8, activations, |height| match height {
        3 => shielded_transactions_with_lock_time(&sapling, Some(300)),
        4 => {
            let mut transactions = shielded_transactions_with_lock_time(&sapling, Some(400))?;
            transactions.extend(shielded_transactions_with_lock_time(&orchard, Some(401))?);
            Ok(transactions)
        }
        5 => {
            let mut transactions = shielded_transactions_with_lock_time(&sapling, Some(500))?;
            for index in 0_u32..20 {
                transactions.extend(shielded_transactions_with_lock_time(
                    &orchard,
                    Some(501 + index),
                )?);
            }
            Ok(transactions)
        }
        7 => shielded_transactions_with_lock_time(&orchard, Some(700)),
        _ => Ok(Vec::new()),
    })
}

async fn build_explorer_every_block_store(
    source: &MultiBlockConstructionSource,
    activations: &Arc<NetworkUpgradeActivations>,
    store_path: &std::path::Path,
    tree_hashing_workers: u32,
    tip_height: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let tip = source.checkpoint_at(tip_height)?;
    let genesis = source.checkpoint_at(0)?;
    let build_plan = CanonicalStoreBuildPlan::complete(
        activations,
        genesis.block_time_seconds,
        tip.block_id,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(1)?,
    )?;
    let builder = RocksDbCanonicalBuilder::create_fresh(
        store_path,
        CanonicalStoreWorkload::Explorer,
        build_plan,
        RocksDbResourceBudget::for_local_tests(),
    )?
    .with_tree_state_checkpoint_selection(zinder_store::TreeStateCheckpointSelection::EveryBlock)?;
    let mut config = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::clone(activations),
    );
    config.pipeline_limits.tree_hashing_workers =
        NonZeroU32::new(tree_hashing_workers).ok_or("tree hashing workers must be non-zero")?;
    let loaded = load_fresh_canonical(builder, source, &config).await?;
    // Explorer publication requires authenticated daily value-pool evidence, so
    // stop after bulk loading ingests checkpoints, final roots, and subtrees.
    drop(loaded);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_block_construction_with_dense_and_multi_pool_commitments_matches_serial_across_workers()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    let source = dense_multi_pool_source(&activations)?;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let temporary = TempDir::new()?;

    let mut serial_checkpoints: Option<ColumnFamilyEntries> = None;
    let mut serial_final_roots: Option<ColumnFamilyEntries> = None;
    let mut serial_subtrees: Option<ColumnFamilyEntries> = None;
    let mut serial_fence = None;

    for workers in [1_u32, 2, 4, 8] {
        let wallet_path = temporary.path().join(format!("wallet-{workers}"));
        let mut wallet_config =
            multi_block_writer_config(wallet_path.clone(), Arc::clone(&activations), true);
        wallet_config
            .construction
            .pipeline_limits
            .tree_hashing_workers = NonZeroU32::new(workers).ok_or("workers must be non-zero")?;
        let wallet_store = admit_canonical_store(
            &source,
            Arc::clone(&activations),
            &wallet_config,
            &readiness,
            &CancellationToken::new(),
        )
        .await?;
        for height in 0_u32..=7 {
            assert_eq!(
                wallet_store
                    .tree_state_checkpoint_at(BlockHeight::new(height), &activations)?
                    .as_ref(),
                Some(source.checkpoint_at(height)?)
            );
        }
        let fence = wallet_store.event_fence();
        drop(wallet_store);
        let wallet_checkpoints = read_cf_entries(&wallet_path, "tree_state_checkpoint")?;
        assert_eq!(wallet_checkpoints.len(), 8);

        let explorer_path = temporary.path().join(format!("explorer-{workers}"));
        build_explorer_every_block_store(&source, &activations, &explorer_path, workers, 7).await?;
        let explorer_checkpoints = read_cf_entries(&explorer_path, "tree_state_checkpoint")?;
        let explorer_final_roots =
            read_cf_entries(&explorer_path, "block_final_note_commitment_roots")?;
        let explorer_subtrees = read_cf_entries(&explorer_path, "subtree_root")?;
        assert_eq!(explorer_checkpoints, wallet_checkpoints);
        assert_eq!(explorer_final_roots.len(), 7);

        if let Some(expected_checkpoints) = &serial_checkpoints {
            assert_eq!(&wallet_checkpoints, expected_checkpoints);
            assert_eq!(Some(&explorer_final_roots), serial_final_roots.as_ref());
            assert_eq!(Some(&explorer_subtrees), serial_subtrees.as_ref());
            assert_eq!(Some(fence), serial_fence);
        } else {
            serial_checkpoints = Some(wallet_checkpoints);
            serial_final_roots = Some(explorer_final_roots);
            serial_subtrees = Some(explorer_subtrees);
            serial_fence = Some(fence);
        }
    }
    Ok(())
}

mod indexer_parity {
    use std::{collections::HashMap, num::NonZeroU32, pin::Pin, sync::Arc, time::Duration};

    use futures_util::Stream;
    use serde_json::{Value, json};
    use tonic::{Request, Response, Status, transport::Server};
    use zinder_core::{
        BlockHeight, CommitmentTreeCheckpoint, CommitmentTreeFrontier, Network,
        NetworkUpgradeActivations, wire::encode_rpc_block_hash_hex,
    };
    use zinder_ingest::{CanonicalWriterConfig, admit_canonical_store};
    use zinder_proto::external::zebra_indexer_rpc::{
        BlockAndHash, BlockHashAndHeight, BlockRequest, Empty, MempoolChangeMessage,
        NonFinalizedStateChangeRequest,
        indexer_server::{Indexer, IndexerServer},
    };
    use zinder_runtime::{ComposedReadiness, Readiness};
    use zinder_source::{
        NodeAuth, NodeSource, NodeTarget, SourceBlock, ZebraIndexerBlockSource, ZebraJsonRpcSource,
        ZebraJsonRpcSourceOptions,
    };
    use zinder_store::{RocksDbCanonicalStore, TreeStateCheckpointSelection};
    use zinder_testkit::{JsonRpcTestServer, RpcReply, method};

    use crate::common::construction_fixture::{
        MultiBlockConstructionSource, multi_block_writer_config,
    };

    type ChainTipStream =
        Pin<Box<dyn Stream<Item = Result<BlockHashAndHeight, Status>> + Send + 'static>>;
    type BlockStream = Pin<Box<dyn Stream<Item = Result<BlockAndHash, Status>> + Send + 'static>>;
    type MempoolStream =
        Pin<Box<dyn Stream<Item = Result<MempoolChangeMessage, Status>> + Send + 'static>>;

    #[derive(Clone)]
    struct ParityIndexerService {
        blocks_by_height: Arc<HashMap<u32, BlockAndHash>>,
    }

    #[tonic::async_trait]
    impl Indexer for ParityIndexerService {
        type ChainTipChangeStream = ChainTipStream;
        type NonFinalizedStateChangeStream = BlockStream;
        type MempoolChangeStream = MempoolStream;

        async fn chain_tip_change(
            &self,
            _request: Request<Empty>,
        ) -> Result<Response<Self::ChainTipChangeStream>, Status> {
            Err(Status::unimplemented("parity server serves only GetBlock"))
        }

        async fn non_finalized_state_change(
            &self,
            _request: Request<NonFinalizedStateChangeRequest>,
        ) -> Result<Response<Self::NonFinalizedStateChangeStream>, Status> {
            Err(Status::unimplemented("parity server serves only GetBlock"))
        }

        async fn mempool_change(
            &self,
            _request: Request<Empty>,
        ) -> Result<Response<Self::MempoolChangeStream>, Status> {
            Err(Status::unimplemented("parity server serves only GetBlock"))
        }

        async fn get_block(
            &self,
            request: Request<BlockRequest>,
        ) -> Result<Response<BlockAndHash>, Status> {
            let key = request.into_inner().hash_or_height;
            if key.len() != 4 {
                return Err(Status::invalid_argument("expected 4-byte height"));
            }
            let height = u32::from_be_bytes([key[0], key[1], key[2], key[3]]);
            self.blocks_by_height
                .get(&height)
                .cloned()
                .map(Response::new)
                .ok_or_else(|| Status::not_found("block unavailable"))
        }
    }

    fn frontier_json(frontier: Option<&CommitmentTreeFrontier>) -> Value {
        frontier.map_or_else(
            || json!({"commitments": {}}),
            |frontier| {
                json!({
                    "commitments": {
                        "finalRoot": hex::encode(frontier.final_root().as_bytes()),
                        "finalState": hex::encode(frontier.final_state_bytes()),
                    }
                })
            },
        )
    }

    fn checkpoint_json(checkpoint: &CommitmentTreeCheckpoint) -> Value {
        json!({
            "height": checkpoint.block_id.height.value(),
            "hash": encode_rpc_block_hash_hex(checkpoint.block_id.hash),
            "time": checkpoint.block_time_seconds,
            "sapling": frontier_json(checkpoint.frontiers.sapling()),
            "orchard": frontier_json(checkpoint.frontiers.orchard()),
            "ironwood": frontier_json(checkpoint.frontiers.ironwood()),
        })
    }

    pub(super) struct ParityFixture {
        pub(super) tip_height: u32,
        pub(super) tip_json: Value,
        pub(super) block_hex_by_height: Arc<HashMap<String, String>>,
        pub(super) checkpoints_by_key: Arc<HashMap<String, Value>>,
        pub(super) grpc_blocks: Arc<HashMap<u32, BlockAndHash>>,
    }

    pub(super) async fn build_parity_fixture(
        tip_height: u32,
        activations: &Arc<NetworkUpgradeActivations>,
    ) -> Result<ParityFixture, Box<dyn std::error::Error>> {
        let chain_source = MultiBlockConstructionSource::build(tip_height, activations)?;
        let tip = chain_source.tip_id().await?;
        let mut block_hex_by_height = HashMap::new();
        let mut checkpoints_by_key = HashMap::new();
        let mut grpc_blocks = HashMap::new();
        for height_num in 0..=tip_height {
            let height = BlockHeight::new(height_num);
            let checkpoint = chain_source
                .fetch_chain_checkpoint(height, activations)
                .await?;
            let checkpoint_payload = checkpoint_json(&checkpoint);
            checkpoints_by_key.insert(height_num.to_string(), checkpoint_payload.clone());
            checkpoints_by_key.insert(
                encode_rpc_block_hash_hex(checkpoint.block_id.hash),
                checkpoint_payload,
            );
            if height_num > 0 {
                let source_block: SourceBlock = chain_source.fetch_block_at(height).await?;
                let hash_bytes = hex::decode(encode_rpc_block_hash_hex(source_block.hash))?;
                block_hex_by_height.insert(
                    height_num.to_string(),
                    hex::encode(&source_block.raw_block_bytes),
                );
                grpc_blocks.insert(
                    height_num,
                    BlockAndHash {
                        hash: hash_bytes,
                        data: source_block.raw_block_bytes,
                    },
                );
            }
        }
        Ok(ParityFixture {
            tip_height,
            tip_json: json!({
                "height": tip.height.value(),
                "hash": tip.hash.as_bytes(),
            }),
            block_hex_by_height: Arc::new(block_hex_by_height),
            checkpoints_by_key: Arc::new(checkpoints_by_key),
            grpc_blocks: Arc::new(grpc_blocks),
        })
    }

    pub(super) fn parity_writer_config(
        path: std::path::PathBuf,
        activations: Arc<NetworkUpgradeActivations>,
    ) -> Result<CanonicalWriterConfig, Box<dyn std::error::Error>> {
        let mut config = multi_block_writer_config(path, activations, true);
        config.checkpoint_height = Some(BlockHeight::new(1));
        config.reorg_window_blocks = 2;
        config
            .construction
            .pipeline_limits
            .source_segment_max_blocks =
            NonZeroU32::new(2).ok_or("segment max blocks must be non-zero")?;
        config
            .construction
            .pipeline_limits
            .source_fetch_max_in_flight_requests =
            NonZeroU32::new(2).ok_or("in-flight requests must be non-zero")?;
        config
            .construction
            .pipeline_limits
            .block_prepare_concurrency =
            NonZeroU32::new(3).ok_or("prepare concurrency must be non-zero")?;
        config.tree_state_checkpoint_selection = Some(TreeStateCheckpointSelection::EveryBlock);
        Ok(config)
    }

    pub(super) async fn start_parity_indexer(
        grpc_blocks: Arc<HashMap<u32, BlockAndHash>>,
    ) -> Result<
        (
            std::net::SocketAddr,
            tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let incoming = futures_util::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let server = tokio::spawn(
            Server::builder()
                .add_service(IndexerServer::new(ParityIndexerService {
                    blocks_by_height: grpc_blocks,
                }))
                .serve_with_incoming(Box::pin(incoming)),
        );
        Ok((address, server))
    }

    pub(super) fn start_parity_rpc_server(
        fixture: &ParityFixture,
        serve_getblock: bool,
    ) -> Result<JsonRpcTestServer, Box<dyn std::error::Error>> {
        let tip_json = fixture.tip_json.clone();
        let checkpoints = Arc::clone(&fixture.checkpoints_by_key);
        let blocks = Arc::clone(&fixture.block_hex_by_height);
        let mut stubs = vec![
            method("getbestblockheightandhash")
                .reply_with(move |_| RpcReply::result(tip_json.clone())),
            method("z_gettreestate").reply_with(move |params| {
                let key = params.get(0).and_then(Value::as_str).unwrap_or_default();
                checkpoints.get(key).map_or_else(
                    || RpcReply::error_with_code(-5, "tree state not found"),
                    |payload| RpcReply::result(payload.clone()),
                )
            }),
        ];
        if serve_getblock {
            stubs.push(method("getblock").reply_with(move |params| {
                let key = params.get(0).and_then(Value::as_str).unwrap_or_default();
                blocks.get(key).map_or_else(
                    || RpcReply::error("height out of range"),
                    |hex_block| RpcReply::result(json!(hex_block)),
                )
            }));
        }
        JsonRpcTestServer::start(stubs).map_err(Into::into)
    }

    pub(super) async fn construct_with_rpc(
        fixture: &ParityFixture,
        activations: &Arc<NetworkUpgradeActivations>,
        path: std::path::PathBuf,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        let rpc_server = start_parity_rpc_server(fixture, true)?;
        let rpc_source = ZebraJsonRpcSource::with_options(
            Network::ZcashRegtest,
            rpc_server.url(),
            NodeAuth::None,
            ZebraJsonRpcSourceOptions::default(),
        )?;
        let config = parity_writer_config(path, Arc::clone(activations))?;
        let store = admit_canonical_store(
            &rpc_source,
            Arc::clone(activations),
            &config,
            &ComposedReadiness::new(Readiness::default(), &[]),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await?;
        Ok(store)
    }

    pub(super) async fn construct_with_indexer(
        fixture: &ParityFixture,
        activations: &Arc<NetworkUpgradeActivations>,
        path: std::path::PathBuf,
    ) -> Result<(RocksDbCanonicalStore, JsonRpcTestServer), Box<dyn std::error::Error>> {
        let (grpc_addr, grpc_server) =
            start_parity_indexer(Arc::clone(&fixture.grpc_blocks)).await?;
        let control_plane = start_parity_rpc_server(fixture, false)?;
        let target = NodeTarget::new(
            Network::ZcashRegtest,
            control_plane.url(),
            NodeAuth::None,
            Duration::from_secs(5),
            std::num::NonZeroU64::new(64 * 1_024 * 1_024).unwrap_or(std::num::NonZeroU64::MIN),
        )
        .with_indexer_grpc_addr(Some(format!("http://{grpc_addr}")));
        let indexer_source = ZebraIndexerBlockSource::from_node_target_lazy(&target)?;
        let config = parity_writer_config(path, Arc::clone(activations))?;
        let outcome = admit_canonical_store(
            &indexer_source,
            Arc::clone(activations),
            &config,
            &ComposedReadiness::new(Readiness::default(), &[]),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;
        grpc_server.abort();
        Ok((outcome?, control_plane))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexer_and_json_rpc_sources_construct_identical_canonical_stores()
-> Result<(), Box<dyn std::error::Error>> {
    let activations = regtest_activations();
    let fixture = indexer_parity::build_parity_fixture(10, &activations).await?;
    let rpc_dir = TempDir::new()?;
    let indexer_dir = TempDir::new()?;

    let rpc_store = indexer_parity::construct_with_rpc(
        &fixture,
        &activations,
        rpc_dir.path().join("canonical"),
    )
    .await?;
    let (indexer_store, control_plane) = Box::pin(indexer_parity::construct_with_indexer(
        &fixture,
        &activations,
        indexer_dir.path().join("canonical"),
    ))
    .await?;

    assert_eq!(rpc_store.event_fence(), indexer_store.event_fence());
    assert_eq!(rpc_store.history_bounds(), indexer_store.history_bounds());
    for height_num in 0..=fixture.tip_height {
        let height = BlockHeight::new(height_num);
        assert_eq!(
            rpc_store.tree_state_checkpoint_at(height, &activations)?,
            indexer_store.tree_state_checkpoint_at(height, &activations)?,
            "checkpoint mismatch at height {height_num}"
        );
    }
    assert!(
        control_plane.requests_for("getblock")?.is_empty(),
        "indexer source must never fetch blocks through JSON-RPC getblock"
    );
    let tip_polls = control_plane
        .requests_for("getbestblockheightandhash")?
        .len();
    assert!(
        tip_polls <= 5,
        "expected O(1) tip polls during 10-block settled construction, observed {tip_polls}"
    );
    Ok(())
}

fn non_coinbase_zebra_transactions(
    source_block: &SourceBlock,
) -> Result<Vec<Arc<zebra_chain::transaction::Transaction>>, Box<dyn std::error::Error>> {
    let parsed: ZebraBlock = source_block
        .raw_block_bytes
        .as_slice()
        .zcash_deserialize_into()?;
    Ok(parsed
        .transactions
        .into_iter()
        .filter(|tx| !tx.is_coinbase())
        .collect())
}

fn batched_multi_pool_source(
    activations: &NetworkUpgradeActivations,
) -> Result<MultiBlockConstructionSource, Box<dyn std::error::Error>> {
    let sapling = fixture_sapling_source_block()?;
    let orchard = fixture_orchard_source_block()?;
    let ironwood_a = non_coinbase_zebra_transactions(&fixture_ironwood_source_block()?)?;
    let ironwood_b = non_coinbase_zebra_transactions(&fixture_zakura_ironwood_source_block()?)?;
    MultiBlockConstructionSource::build_with_transactions(141, activations, |height| {
        if height == 70 {
            let mut transactions = shielded_transactions_with_lock_time(&sapling, Some(height))?;
            transactions.extend(shielded_transactions_with_lock_time(
                &orchard,
                Some(5_000 + height),
            )?);
            transactions.extend(ironwood_a.iter().cloned());
            return Ok(transactions);
        }
        if height == 132 {
            return Ok(ironwood_b.clone());
        }
        if height == 15 || height == 95 {
            let mut transactions = shielded_transactions_with_lock_time(&sapling, Some(height))?;
            for offset in 0_u32..10 {
                transactions.extend(shielded_transactions_with_lock_time(
                    &orchard,
                    Some(1_000 + height * 16 + offset),
                )?);
            }
            return Ok(transactions);
        }
        match height % 7 {
            1 => shielded_transactions_with_lock_time(&sapling, Some(height)),
            3 => shielded_transactions_with_lock_time(&orchard, Some(height)),
            5 => {
                let mut transactions =
                    shielded_transactions_with_lock_time(&sapling, Some(2_000 + height))?;
                transactions.extend(shielded_transactions_with_lock_time(
                    &orchard,
                    Some(3_000 + height),
                )?);
                Ok(transactions)
            }
            _ => Ok(Vec::new()),
        }
    })
}

fn checkpoint_frontiers_hex(
    checkpoint: &CommitmentTreeCheckpoint,
) -> (Option<String>, Option<String>, Option<String>) {
    let encode_pool =
        |pool: Option<&CommitmentTreeFrontier>| pool.map(|f| hex::encode(f.final_state_bytes()));
    (
        encode_pool(checkpoint.frontiers.sapling()),
        encode_pool(checkpoint.frontiers.orchard()),
        encode_pool(checkpoint.frontiers.ironwood()),
    )
}

fn staggered_all_pool_activations()
-> Result<Arc<NetworkUpgradeActivations>, Box<dyn std::error::Error>> {
    use zinder_core::{ConsensusBranchId, NetworkUpgradeActivation};
    let activations = NetworkUpgradeActivations::new(
        Network::ZcashRegtest,
        vec![
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x5ba8_1b19),
                activation_height: BlockHeight::new(1),
                name: "Overwinter".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x76b8_09bb),
                activation_height: BlockHeight::new(1),
                name: "Sapling".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x2bb4_0e60),
                activation_height: BlockHeight::new(1),
                name: "Blossom".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xf5b9_230b),
                activation_height: BlockHeight::new(1),
                name: "Heartwood".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xe9ff_75a6),
                activation_height: BlockHeight::new(1),
                name: "Canopy".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xc2d6_d0b4),
                activation_height: BlockHeight::new(2),
                name: "NU5".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0xc8e7_1055),
                activation_height: BlockHeight::new(2),
                name: "NU6".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x4dec_4df0),
                activation_height: BlockHeight::new(3),
                name: "NU6.1".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x5437_f330),
                activation_height: BlockHeight::new(3),
                name: "NU6.2".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x37a5_165b),
                activation_height: BlockHeight::new(60),
                name: "NU6.3".to_owned(),
            },
            NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(0x7719_0ad9),
                activation_height: BlockHeight::new(130),
                name: "NU7".to_owned(),
            },
        ],
    )?;
    Ok(Arc::new(activations))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_block_multi_batch_construction_matches_serial_across_workers()
-> Result<(), Box<dyn std::error::Error>> {
    let recorded_metrics = install_metrics_recorder(&OpsServer {
        service_name: "zinder-ingest",
        service_version: "0.0.0",
        network_name: "zcash-regtest",
        advertised_capabilities: Arc::from([]),
    })?;
    let activations = staggered_all_pool_activations()?;
    let source = batched_multi_pool_source(&activations)?;
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let temporary = TempDir::new()?;

    let mut serial_checkpoints: Option<ColumnFamilyEntries> = None;
    let mut serial_final_roots: Option<ColumnFamilyEntries> = None;
    let mut serial_subtrees: Option<ColumnFamilyEntries> = None;
    let mut serial_fence = None;

    for workers in [1_u32, 2, 4, 8] {
        let wallet_path = temporary.path().join(format!("wallet-batch-{workers}"));
        let mut wallet_config =
            multi_block_writer_config(wallet_path.clone(), Arc::clone(&activations), true);
        wallet_config
            .construction
            .pipeline_limits
            .tree_hashing_workers = NonZeroU32::new(workers).ok_or("workers must be non-zero")?;
        let wallet_store = admit_canonical_store(
            &source,
            Arc::clone(&activations),
            &wallet_config,
            &readiness,
            &CancellationToken::new(),
        )
        .await?;
        for height in 0_u32..=140 {
            let stored = wallet_store
                .tree_state_checkpoint_at(BlockHeight::new(height), &activations)?
                .ok_or("missing stored checkpoint")?;
            let expected = source.checkpoint_at(height)?;
            assert_eq!(&stored, expected);
            assert_eq!(
                checkpoint_frontiers_hex(&stored),
                checkpoint_frontiers_hex(expected)
            );
        }
        let fence = wallet_store.event_fence();
        drop(wallet_store);
        let wallet_checkpoints = read_cf_entries(&wallet_path, "tree_state_checkpoint")?;
        assert_eq!(wallet_checkpoints.len(), 141);

        let explorer_path = temporary.path().join(format!("explorer-batch-{workers}"));
        build_explorer_every_block_store(&source, &activations, &explorer_path, workers, 140)
            .await?;
        let explorer_checkpoints = read_cf_entries(&explorer_path, "tree_state_checkpoint")?;
        let explorer_final_roots =
            read_cf_entries(&explorer_path, "block_final_note_commitment_roots")?;
        let explorer_subtrees = read_cf_entries(&explorer_path, "subtree_root")?;
        assert_eq!(explorer_checkpoints, wallet_checkpoints);
        assert_eq!(explorer_final_roots.len(), 140);

        if let Some(expected_checkpoints) = &serial_checkpoints {
            assert_eq!(&wallet_checkpoints, expected_checkpoints);
            assert_eq!(Some(&explorer_final_roots), serial_final_roots.as_ref());
            assert_eq!(Some(&explorer_subtrees), serial_subtrees.as_ref());
            assert_eq!(Some(fence), serial_fence);
        } else {
            serial_checkpoints = Some(wallet_checkpoints);
            serial_final_roots = Some(explorer_final_roots);
            serial_subtrees = Some(explorer_subtrees);
            serial_fence = Some(fence);
        }
    }

    let rendered = recorded_metrics.render();
    assert!(
        rendered.contains(
            "zinder_ingest_canonical_block_load_stage_duration_seconds_count{stage=\"tree_hashing_append\"}"
        ),
        "expected tree_hashing_append stage histogram sample in rendered metrics"
    );
    Ok(())
}

fn assert_compose_legacy_transactions_print_config(
    temporary_root: &std::path::Path,
    store_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let compose_ingest_config =
        zinder_testkit::manifest_dir!().join("../../deploy/config/ingest.toml");
    let token_path = temporary_root.join("fixture.token");
    std::fs::write(&token_path, "fixture-token\n")?;
    let printed = std::process::Command::new(env!("CARGO_BIN_EXE_zinder-ingest"))
        .args(["--print-config", "--config"])
        .arg(&compose_ingest_config)
        .env_remove("ZINDER_DEPLOYMENT__PROFILE")
        .env("ZINDER_STORAGE__RAW_BLOB_POLICY", "transactions")
        .env("ZINDER_NETWORK__NAME", "zcash-regtest")
        .env("ZINDER_NODE__JSON_RPC_ADDR", "http://127.0.0.1:18232")
        .env("ZINDER_STORAGE__PATH", store_path)
        .env("ZINDER_INGEST_CONTROL__BEARER_TOKEN_PATH", &token_path)
        .env(
            "ZINDER_INGEST_CONTROL__CHECKPOINT_BEARER_TOKEN_PATH",
            &token_path,
        )
        .output()?;
    assert!(printed.status.success(), "{printed:?}");
    let stdout = String::from_utf8(printed.stdout)?;
    assert!(
        stdout.contains("raw_blob_policy = \"transactions\""),
        "{stdout}"
    );
    assert!(stdout.contains("coverage = \"wallet-serving\""), "{stdout}");
    assert!(!stdout.contains("[deployment]"), "{stdout}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_transactions_compose_override_reopens_pre_change_store()
-> Result<(), Box<dyn std::error::Error>> {
    let (source_block, chain_checkpoint) = held_construction_chain()?;
    let fixed_tip = chain_checkpoint.block_id;
    let source = HeldConstructionSource::new(source_block, chain_checkpoint);
    source.release_chain_segments();
    let temporary = TempDir::new()?;
    let store_path = temporary.path().join("canonical");
    let legacy_config = held_construction_writer_config(store_path.clone(), regtest_activations());
    assert_eq!(
        legacy_config.raw_blob_retention,
        RawBlobRetention::Transactions
    );

    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let initial_store = admit_canonical_store(
        &source,
        regtest_activations(),
        &legacy_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        initial_store.raw_blob_retention(),
        RawBlobRetention::Transactions
    );
    assert_eq!(initial_store.event_fence().visible_tip(), fixed_tip);
    let construction_requests = source.chain_segment_requests.load(Ordering::SeqCst);
    drop(initial_store);

    // Upgrading to the default `wallet` profile (`raw_blob_policy = "none"`)
    // must fail writer admission against a pre-change `transactions` store.
    let wallet_profile_config = CanonicalWriterConfig {
        raw_blob_retention: RawBlobRetention::None,
        ..legacy_config.clone()
    };
    let rejected = admit_canonical_store(
        &source,
        regtest_activations(),
        &wallet_profile_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("default wallet profile must reject reopening a pre-change transactions store")?;
    assert!(
        matches!(
            &rejected,
            CanonicalWriterError::Store(CanonicalStoreError::AdmissionRefused { reason, .. })
                if reason.contains("raw blob retention")
                    && reason.contains("transactions")
                    && reason.contains("none")
        ),
        "{rejected}"
    );

    // Using the legacy transactions overlay (`ZINDER_DEPLOYMENT__PROFILE`
    // unset, `ZINDER_STORAGE__RAW_BLOB_POLICY=transactions`) resolves cleanly
    // against `deploy/config/ingest.toml` and reopens the pre-change store.
    assert_compose_legacy_transactions_print_config(temporary.path(), &store_path)?;

    let reopened = admit_canonical_store(
        &source,
        regtest_activations(),
        &legacy_config,
        &readiness,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        reopened.raw_blob_retention(),
        RawBlobRetention::Transactions
    );
    assert_eq!(reopened.event_fence().visible_tip(), fixed_tip);
    assert_eq!(
        source.chain_segment_requests.load(Ordering::SeqCst),
        construction_requests
    );
    Ok(())
}
