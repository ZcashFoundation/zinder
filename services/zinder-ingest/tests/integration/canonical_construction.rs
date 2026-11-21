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
use zebra_chain::{
    block::{Block as ZebraBlock, Height as ZebraHeight},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, ChainTipMetadata, CommitmentTreeAccumulator,
    CommitmentTreeCheckpoint, CommitmentTreeFrontier, CommitmentTreeFrontiers, Network,
    NetworkUpgradeActivations, NetworkUpgradeActivationsFingerprintVersion, ShieldedProtocol,
};
use zinder_ingest::{
    CanonicalBlockLoadOutcome, CanonicalConstructionConfig, CanonicalConstructionError,
    CanonicalFollowConfig, CanonicalWriterConfig, CanonicalWriterError, IngestError,
    admit_canonical_store, load_fresh_canonical, load_fresh_canonical_blocks,
};
use zinder_runtime::{ComposedReadiness, Readiness};
use zinder_source::{
    NodeCapabilities, NodeSource, SourceBlock, SourceBlockHeader, SourceChainSegment,
    SourceChainSegmentLimits, SourceError,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreError, CanonicalStoreWorkload,
    RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalStore, RocksDbResourceBudget,
    TreeStateLookup,
};
use zinder_testkit::{frontier_from_note_commitments, sample_regtest_upgrade_activations};

use super::{
    canonical_block_context::{chained_source_block, pool_commitments, shielded_transactions},
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
        every_block_tree_state_checkpoints: true,
        follow: CanonicalFollowConfig {
            request_timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(10),
            lag_threshold_blocks: 0,
            target_height: None,
            event_retention_window: None,
            event_retention_check_interval: Duration::from_secs(1),
            mempool_ready_gate: None,
            max_replacement_bytes: zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES,
            node_failover: None,
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
        )?,
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

/// Every construction phase and serial loader stage records a duration.
///
/// The loader runs on a blocking thread, which a thread-local recorder does
/// not reach, so this test installs a process-wide one. Other tests in a
/// shared libtest process may add samples, so counts are lower bounds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn construction_times_each_phase_and_serial_loader_stage()
-> Result<(), Box<dyn std::error::Error>> {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let recorded_metrics = recorder.handle();
    metrics::set_global_recorder(recorder)
        .map_err(|_| "another test in this process installed a global recorder")?;
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

/// Multi-block Regtest source with exact per-height checkpoints and optional
/// cancellation hooks for construction lifecycle tests.
#[derive(Clone)]
struct MultiBlockConstructionSource {
    genesis: SourceBlock,
    blocks: Vec<SourceBlock>,
    checkpoints: Vec<CommitmentTreeCheckpoint>,
    hold_chain_segments: Arc<AtomicBool>,
    chain_segment_held: Arc<Notify>,
    chain_segment_requests: Arc<AtomicUsize>,
    cancel_with_tip_checkpoint: Option<(BlockHeight, CancellationToken)>,
}

impl MultiBlockConstructionSource {
    fn build(
        height_count: u32,
        activations: &NetworkUpgradeActivations,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let block_one = fixture_source_block()?;
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
        let sapling_shielded = shielded_transactions(&fixture_sapling_source_block()?)?;
        let orchard_shielded = shielded_transactions(&fixture_orchard_source_block()?)?;
        let mut blocks = Vec::with_capacity(usize::try_from(height_count)?);
        let mut parent_hash = genesis.hash;
        for height_num in 1..=height_count {
            let block = if height_num == 1 {
                block_one.clone()
            } else {
                let extra = match height_num {
                    2 => sapling_shielded.clone(),
                    4 => orchard_shielded.clone(),
                    _ => Vec::new(),
                };
                chained_source_block(
                    BlockHeight::new(height_num),
                    parent_hash,
                    u8::try_from(height_num)?,
                    extra,
                )?
            };
            parent_hash = block.hash;
            blocks.push(block);
        }
        let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
            BlockHeight::new(0),
            &CommitmentTreeFrontiers::default(),
            activations,
        )?;
        let mut checkpoints = vec![CommitmentTreeCheckpoint::new(
            BlockId::new(genesis.height, genesis.hash),
            genesis.block_time_seconds,
            accumulator.cross_checked_frontiers()?,
        )];
        for block in &blocks {
            let commitments = pool_commitments(block)?;
            accumulator.append_block_commitments(
                block.height,
                &commitments.sapling,
                &commitments.orchard,
                &commitments.ironwood,
            )?;
            checkpoints.push(CommitmentTreeCheckpoint::new(
                BlockId::new(block.height, block.hash),
                block.block_time_seconds,
                accumulator.cross_checked_frontiers()?,
            ));
        }
        Ok(Self {
            genesis,
            blocks,
            checkpoints,
            hold_chain_segments: Arc::new(AtomicBool::new(false)),
            chain_segment_held: Arc::new(Notify::new()),
            chain_segment_requests: Arc::new(AtomicUsize::new(0)),
            cancel_with_tip_checkpoint: None,
        })
    }

    fn holding_segments(self) -> Self {
        self.hold_chain_segments.store(true, Ordering::SeqCst);
        self
    }

    fn release_chain_segments(&self) {
        self.hold_chain_segments.store(false, Ordering::SeqCst);
    }

    fn cancelling_at_checkpoint(mut self, height: BlockHeight, cancel: CancellationToken) -> Self {
        self.cancel_with_tip_checkpoint = Some((height, cancel));
        self
    }

    fn checkpoint_at(
        &self,
        height: u32,
    ) -> Result<&CommitmentTreeCheckpoint, Box<dyn std::error::Error>> {
        self.checkpoints
            .get(usize::try_from(height)?)
            .ok_or_else(|| format!("missing checkpoint at height {height}").into())
    }
}

#[async_trait]
impl NodeSource for MultiBlockConstructionSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        if height == BlockHeight::new(0) {
            return Ok(self.genesis.clone());
        }
        self.blocks
            .iter()
            .find(|block| block.height == height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "multi-block test source has no block at the requested height".to_owned(),
            })
    }

    async fn fetch_chain_segment(
        &self,
        limits: SourceChainSegmentLimits,
    ) -> Result<SourceChainSegment, SourceError> {
        self.chain_segment_requests.fetch_add(1, Ordering::SeqCst);
        if self.hold_chain_segments.load(Ordering::SeqCst) {
            self.chain_segment_held.notify_one();
            std::future::pending::<()>().await;
        }
        let next_height = limits
            .cursor
            .block_id()
            .map_or(1, |cursor| cursor.height.value().saturating_add(1));
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
                height: BlockHeight::new(1),
                reason: "multi-block test source has no blocks".to_owned(),
            })?;
        Ok(BlockId::new(tip.height, tip.hash))
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        let checkpoint = self
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.block_id.height == height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "multi-block test source has no checkpoint at the requested height"
                    .to_owned(),
            })?;
        if let Some((cancel_height, cancel)) = &self.cancel_with_tip_checkpoint
            && *cancel_height == height
        {
            cancel.cancel();
        }
        Ok(checkpoint)
    }
}

fn multi_block_writer_config(
    storage_path: std::path::PathBuf,
    activations: Arc<NetworkUpgradeActivations>,
    every_block_tree_state_checkpoints: bool,
) -> CanonicalWriterConfig {
    CanonicalWriterConfig {
        reorg_window_blocks: 1,
        every_block_tree_state_checkpoints,
        ..held_construction_writer_config(storage_path, activations)
    }
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
    let activations = regtest_activations();
    let source = MultiBlockConstructionSource::build(6, &activations)?.holding_segments();
    let temporary = TempDir::new()?;
    let config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        true,
    );
    let readiness = ComposedReadiness::new(Readiness::default(), &[]);
    let cancel = CancellationToken::new();

    let admission = admit_canonical_store(
        &source,
        Arc::clone(&activations),
        &config,
        &readiness,
        &cancel,
    );
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
        .map_err(|_| "cancelled dense construction outlived its shutdown bound")?;
    assert!(matches!(stopped, Err(CanonicalWriterError::Cancelled)));
    assert!(!config.storage_path.exists());
    assert!(temporary.path().join("canonical.building").exists());

    source.release_chain_segments();
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
    for height in 0_u32..=5 {
        assert!(matches!(
            store.tree_state_lookup(BlockHeight::new(height), &activations)?,
            Some(TreeStateLookup::Stored(_))
        ));
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
async fn prior_sparse_store_reopens_under_every_block_default_without_rebuilding()
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
    let temporary = TempDir::new()?;
    let mut config = multi_block_writer_config(
        temporary.path().join("canonical"),
        Arc::clone(&activations),
        true,
    );
    config.checkpoint_height = Some(BlockHeight::new(601));
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
        BlockHeight::new(602)
    );
    assert_eq!(
        store.event_fence().visible_tip().height,
        BlockHeight::new(605)
    );

    for height in 601_u32..=605 {
        let expected = source.expected_checkpoint_at(height)?;
        assert_stored_checkpoint_matches(&store, BlockHeight::new(height), expected, &activations)?;
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
