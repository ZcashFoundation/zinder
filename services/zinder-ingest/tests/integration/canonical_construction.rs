#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
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
use zinder_core::{
    BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeCheckpoint, CommitmentTreeFrontier,
    CommitmentTreeFrontiers, Network, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprintVersion, ShieldedProtocol,
};
use zinder_ingest::{
    CanonicalBlockLoadOutcome, CanonicalConstructionConfig, CanonicalConstructionError,
    CanonicalFollowConfig, CanonicalWriterConfig, CanonicalWriterError, admit_canonical_store,
    load_fresh_canonical, load_fresh_canonical_blocks,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceBlockHeader, SourceChainSegment,
    SourceChainSegmentLimits, SourceError,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreWorkload,
    RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_testkit::sample_regtest_upgrade_activations;

use super::fixture_block::{fixture_ironwood_source_block, fixture_source_block};

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
    let CanonicalBlockLoadOutcome { builder, evidence } = outcome;

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
    let predecessor_frontiers = pre_ironwood_empty_frontiers();
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

fn pre_ironwood_empty_frontiers() -> CommitmentTreeFrontiers {
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
        ),
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

fn regtest_activations() -> Arc<NetworkUpgradeActivations> {
    Arc::new(sample_regtest_upgrade_activations())
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

fn held_construction_writer_config(
    storage_path: std::path::PathBuf,
    activations: Arc<NetworkUpgradeActivations>,
) -> CanonicalWriterConfig {
    CanonicalWriterConfig {
        storage_path,
        resource_budget: RocksDbResourceBudget::for_local_tests(),
        construction: CanonicalConstructionConfig::for_local_tests(
            Duration::from_secs(5),
            activations,
        ),
        checkpoint_height: None,
        raw_blob_retention: RawBlobRetention::Transactions,
        reorg_window_blocks: 100,
        follow: CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(10),
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
        },
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
        ),
    );
    Ok((source_block, chain_checkpoint))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_construction_stops_promptly_and_the_next_start_rebuilds()
-> Result<(), Box<dyn std::error::Error>> {
    let (source_block, chain_checkpoint) = held_construction_chain()?;
    let fixed_tip = chain_checkpoint.block_id;
    let source = HeldConstructionSource::new(source_block, chain_checkpoint);
    let temporary = TempDir::new()?;
    let config =
        held_construction_writer_config(temporary.path().join("canonical"), regtest_activations());
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
