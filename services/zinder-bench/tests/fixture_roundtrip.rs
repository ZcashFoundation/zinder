#![allow(
    missing_docs,
    reason = "Integration test names describe the behavior under test."
)]

use std::{
    fs::OpenOptions, io::Write, num::NonZeroU32, process::Command, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use eyre::{Result, eyre};
use parking_lot::Mutex;
use serde_json::Value;
use tempfile::tempdir;
use zebra_chain::{
    block::{Block as ZebraBlock, Height as ZebraHeight},
    serialization::{ZcashDeserializeInto, ZcashSerialize},
};
use zinder_bench::fixture::{
    ActivationRecord, CanonicalBlockFactsDigestEvidence, CapturedBlockId, CapturedSubtreeRoot,
    FIXTURE_CONTRACT_IDENTITY, FIXTURE_FORMAT_VERSION, FixtureManifest, FixtureNodeSource,
    SegmentDescriptor, SubtreeRootSet, WorkloadDensity, read_segment_blocks, write_segment,
};
use zinder_bench::{
    canonical_fixture_replay::{
        CanonicalFixtureNodeSource, CanonicalFixtureReplayPlan,
        CanonicalFixtureRocksDbReplayConfig, CanonicalFixtureRocksDbReplayOutcome,
        TreeStateCheckpointCoverage, capture_canonical_fixture_replay_plan,
        replay_canonical_fixture_into_rocksdb,
    },
    capture::measure_fixture_blocks,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockId, CommitmentTreeAccumulator, CommitmentTreeCheckpoint,
    CommitmentTreeFrontier, CommitmentTreeFrontiers, Network, NetworkUpgradeActivations,
    ShieldedProtocol, SubtreeRootArtifact, SubtreeRootIndex, SubtreeRootRange,
    wire::encode_zinder_native_chain_name,
};
use zinder_ingest::{CanonicalConstructionConfig, RawBlobPolicy, prepare_canonical_block};
use zinder_source::{
    NodeCapabilities, NodeCapability, NodeSource, SourceBlock, SourceChainCursor,
    SourceChainSegmentLimits, SourceError,
};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreError, CanonicalSubtreeRootLoadCoverage, RawBlobRetention,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget, TreeStateLookup,
};
use zinder_testkit::{completed_sapling_subtree_frontier, sample_regtest_upgrade_activations};

const REGTEST_BLOCK_1: &str =
    include_str!("../../zinder-ingest/tests/fixtures/z3-regtest-block-1.json");
const REGTEST_BLOCK_603: &str =
    include_str!("../../zinder-ingest/tests/fixtures/z3-regtest-ironwood-block-603.json");

fn load_regtest_block(fixture_json: &str) -> Result<SourceBlock> {
    let fixture: Value = serde_json::from_str(fixture_json)?;
    let raw_block_hex = fixture
        .get("raw_block_hex")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("fixture raw_block_hex must be a string"))?;
    let height = fixture
        .get("height")
        .and_then(Value::as_u64)
        .ok_or_else(|| eyre!("fixture height must be an integer"))?;
    let raw_block_bytes = hex::decode(raw_block_hex)?;
    let height = u32::try_from(height)?;
    Ok(SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        BlockHeight::new(height),
        raw_block_bytes,
    )?)
}

fn raw_block_hex_length(fixture_json: &str) -> Result<u64> {
    let fixture: Value = serde_json::from_str(fixture_json)?;
    let raw_block_hex = fixture
        .get("raw_block_hex")
        .and_then(Value::as_str)
        .ok_or_else(|| eyre!("fixture raw_block_hex must be a string"))?;
    Ok(u64::try_from(raw_block_hex.len())?)
}

fn regtest_activation_records() -> Vec<ActivationRecord> {
    sample_regtest_upgrade_activations()
        .activations()
        .iter()
        .map(|activation| ActivationRecord {
            branch_id: activation.branch_id.value(),
            activation_height: activation.activation_height.value(),
            name: activation.name.clone(),
        })
        .collect()
}

struct RegtestFixtureCase {
    directory: tempfile::TempDir,
    block: SourceBlock,
    descriptor: SegmentDescriptor,
    manifest: FixtureManifest,
}

#[derive(Clone)]
struct CheckpointSource {
    checkpoints: Arc<[CommitmentTreeCheckpoint]>,
    requested_heights: Arc<Mutex<Vec<BlockHeight>>>,
}

impl CheckpointSource {
    fn new(checkpoints: impl Into<Arc<[CommitmentTreeCheckpoint]>>) -> Self {
        Self {
            checkpoints: checkpoints.into(),
            requested_heights: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl NodeSource for CheckpointSource {
    fn capabilities(&self) -> NodeCapabilities {
        NodeCapabilities::default()
    }

    async fn fetch_block_at(&self, height: BlockHeight) -> Result<SourceBlock, SourceError> {
        Err(SourceError::BlockUnavailable {
            height,
            reason: "checkpoint source serves only tree state".to_owned(),
        })
    }

    async fn fetch_chain_checkpoint(
        &self,
        height: BlockHeight,
        _network_upgrade_activations: &NetworkUpgradeActivations,
    ) -> Result<CommitmentTreeCheckpoint, SourceError> {
        self.requested_heights.lock().push(height);
        self.checkpoints
            .iter()
            .find(|checkpoint| checkpoint.block_id.height == height)
            .cloned()
            .ok_or_else(|| SourceError::BlockUnavailable {
                height,
                reason: "checkpoint source has no tree state at this height".to_owned(),
            })
    }

    async fn tip_id(&self) -> Result<BlockId, SourceError> {
        self.checkpoints
            .last()
            .map(|checkpoint| checkpoint.block_id)
            .ok_or_else(|| SourceError::NodeUnavailable {
                reason: "checkpoint source is empty".to_owned(),
            })
    }
}

fn write_regtest_fixture() -> Result<RegtestFixtureCase> {
    let block = load_regtest_block(REGTEST_BLOCK_603)?;
    let directory = tempdir()?;
    let blocks = vec![block.clone()];
    let descriptor = write_segment(directory.path(), 0, &blocks)?;
    let measurements = measure_fixture_blocks(&blocks, &sample_regtest_upgrade_activations())?;
    let manifest = FixtureManifest {
        contract_identity: FIXTURE_CONTRACT_IDENTITY.to_owned(),
        fixture_format_version: FIXTURE_FORMAT_VERSION,
        network: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
        from_height: 603,
        to_height: 603,
        block_count: 1,
        workload_density: measurements.workload_density,
        canonical_artifact_schema_version: zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        canonical_block_facts_digest_evidence: measurements
            .canonical_block_facts_digest_evidence()?,
        tip_hash_hex: hex::encode(block.hash.as_bytes()),
        network_upgrade_activations: regtest_activation_records(),
        segments: vec![descriptor.clone()],
        subtree_roots: SubtreeRootSet::default(),
    };
    manifest.write(directory.path())?;
    Ok(RegtestFixtureCase {
        directory,
        block,
        descriptor,
        manifest,
    })
}

fn write_repeated_payload_fixture() -> Result<RegtestFixtureCase> {
    let mut fixture = write_regtest_fixture()?;
    let mut blocks = vec![fixture.block.clone()];
    for offset in 1..4 {
        blocks.push(SourceBlock::new(
            zinder_source::SourceBlockHeader {
                network: Network::ZcashRegtest,
                height: BlockHeight::new(603 + offset),
                hash: fixture.block.hash,
                parent_hash: fixture.block.hash,
                block_time_seconds: fixture.block.block_time_seconds.saturating_add(offset),
            },
            fixture.block.raw_block_bytes.clone(),
        ));
    }
    let descriptor = write_segment(fixture.directory.path(), 0, &blocks)?;
    fixture.descriptor = descriptor.clone();
    fixture.manifest.to_height = 606;
    fixture.manifest.block_count = 4;
    fixture.manifest.workload_density.block_count = 4;
    fixture
        .manifest
        .canonical_block_facts_digest_evidence
        .block_count = 4;
    fixture.manifest.segments = vec![descriptor];
    Ok(fixture)
}

fn subtree_root_record(
    index: u32,
    root_byte: u8,
    completing_block: BlockId,
) -> CapturedSubtreeRoot {
    CapturedSubtreeRoot {
        protocol: ShieldedProtocol::Sapling.rpc_pool_name().to_owned(),
        index,
        root_hash_hex: hex::encode([root_byte; 32]),
        completing_block: CapturedBlockId {
            height: completing_block.height.value(),
            hash_hex: hex::encode(completing_block.hash.as_bytes()),
        },
    }
}

fn checkpoint_frontiers_at(height: BlockHeight) -> Result<CommitmentTreeFrontiers> {
    CommitmentTreeFrontiers::from_validated_parts(
        Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Sapling)),
        Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
        (height.value() >= 603).then(|| CommitmentTreeFrontier::empty(ShieldedProtocol::Ironwood)),
    )
    .map_err(Into::into)
}

fn fixture_checkpoint_source(
    fixture: &RegtestFixtureCase,
) -> Result<(
    CheckpointSource,
    CommitmentTreeCheckpoint,
    CommitmentTreeCheckpoint,
)> {
    let predecessor_height = BlockHeight::new(fixture.manifest.from_height - 1);
    let predecessor = CommitmentTreeCheckpoint::new(
        BlockId::new(predecessor_height, fixture.block.parent_hash),
        fixture.block.block_time_seconds.saturating_sub(1),
        checkpoint_frontiers_at(predecessor_height)?,
    );
    let fixed_tip = CommitmentTreeCheckpoint::new(
        BlockId::new(fixture.block.height, fixture.block.hash),
        fixture.block.block_time_seconds,
        checkpoint_frontiers_at(fixture.block.height)?,
    );
    let source = CheckpointSource::new(vec![predecessor.clone(), fixed_tip.clone()]);
    Ok((source, predecessor, fixed_tip))
}

fn fixture_construction_checkpoint_source(
    fixture: &RegtestFixtureCase,
) -> Result<(
    CheckpointSource,
    CommitmentTreeCheckpoint,
    CommitmentTreeCheckpoint,
)> {
    let (_, predecessor, _) = fixture_checkpoint_source(fixture)?;
    fixture_construction_checkpoint_source_from_predecessor(fixture, predecessor)
}

fn fixture_construction_checkpoint_source_with_completed_sapling(
    fixture: &RegtestFixtureCase,
) -> Result<(
    CheckpointSource,
    CommitmentTreeCheckpoint,
    CommitmentTreeCheckpoint,
)> {
    let predecessor_height = BlockHeight::new(fixture.manifest.from_height - 1);
    let predecessor = CommitmentTreeCheckpoint::new(
        BlockId::new(predecessor_height, fixture.block.parent_hash),
        fixture.block.block_time_seconds.saturating_sub(1),
        CommitmentTreeFrontiers::from_validated_parts(
            Some(completed_sapling_subtree_frontier()?),
            Some(CommitmentTreeFrontier::empty(ShieldedProtocol::Orchard)),
            None,
        )?,
    );
    fixture_construction_checkpoint_source_from_predecessor(fixture, predecessor)
}

fn fixture_construction_checkpoint_source_from_predecessor(
    fixture: &RegtestFixtureCase,
    predecessor: CommitmentTreeCheckpoint,
) -> Result<(
    CheckpointSource,
    CommitmentTreeCheckpoint,
    CommitmentTreeCheckpoint,
)> {
    let activations = sample_regtest_upgrade_activations();
    let prepared = prepare_canonical_block(&fixture.block, &activations, RawBlobPolicy::None)?;
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        predecessor.block_id.height,
        &predecessor.frontiers,
        &activations,
    )?;
    let mut sapling_commitments = Vec::new();
    let mut orchard_commitments = Vec::new();
    let mut ironwood_commitments = Vec::new();
    for transaction in prepared.partial_compact_block.transactions() {
        for output in &transaction.data.sapling_outputs {
            sapling_commitments.push(output.commitment);
        }
        for action in &transaction.data.orchard_actions {
            orchard_commitments.push(action.commitment);
        }
        for action in &transaction.data.ironwood_actions {
            ironwood_commitments.push(action.commitment);
        }
    }
    accumulator.append_block_commitments(
        fixture.block.height,
        &sapling_commitments,
        &orchard_commitments,
        &ironwood_commitments,
    )?;
    let fixed_tip = CommitmentTreeCheckpoint::new(
        BlockId::new(fixture.block.height, fixture.block.hash),
        fixture.block.block_time_seconds,
        accumulator.validated_frontiers()?,
    );
    let source = CheckpointSource::new(vec![predecessor.clone(), fixed_tip.clone()]);
    Ok((source, predecessor, fixed_tip))
}

fn add_historical_sapling_root(fixture: &mut RegtestFixtureCase) -> Result<SubtreeRootArtifact> {
    let completing_block = BlockId::new(
        BlockHeight::new(fixture.manifest.from_height.saturating_sub(1)),
        fixture.block.parent_hash,
    );
    fixture.manifest.subtree_roots.sapling = vec![subtree_root_record(0, 0x51, completing_block)];
    fixture.manifest.write(fixture.directory.path())?;
    fixture
        .manifest
        .subtree_root_artifacts()?
        .into_iter()
        .next()
        .ok_or_else(|| eyre!("historical Sapling root must be present"))
}

fn canonical_fixture_rocksdb_replay_config(
    fixture_directory: &std::path::Path,
    canonical_store_path: &std::path::Path,
) -> CanonicalFixtureRocksDbReplayConfig {
    let construction = CanonicalConstructionConfig::for_local_tests(
        Duration::from_secs(5),
        Arc::new(sample_regtest_upgrade_activations()),
    );
    CanonicalFixtureRocksDbReplayConfig {
        fixture_directory: fixture_directory.to_path_buf(),
        canonical_store_path: canonical_store_path.to_path_buf(),
        request_timeout: construction.request_timeout,
        pipeline_limits: construction.pipeline_limits,
        resource_budget: RocksDbResourceBudget::for_local_tests(),
        raw_blob_retention: RawBlobRetention::None,
        supported_reorg_depth: 1,
        source_segment_delay: Duration::ZERO,
        tree_state_checkpoint_coverage: TreeStateCheckpointCoverage::EveryBlock,
        tree_state_commitment_budget: None,
    }
}

fn assert_canonical_fixture_load_evidence(
    outcome: &CanonicalFixtureRocksDbReplayOutcome,
    fixture: &RegtestFixtureCase,
    predecessor: &CommitmentTreeCheckpoint,
    fixed_tip: &CommitmentTreeCheckpoint,
) {
    assert_eq!(
        outcome.tree_state_checkpoint_coverage,
        TreeStateCheckpointCoverage::EveryBlock
    );
    assert_eq!(
        outcome.block_load_evidence.first_height,
        fixture.block.height
    );
    assert_eq!(outcome.block_load_evidence.first_hash, fixture.block.hash);
    assert_eq!(
        outcome.block_load_evidence.first_parent_hash,
        predecessor.block_id.hash
    );
    assert_eq!(
        BlockId::new(
            outcome.block_load_evidence.tip_height,
            outcome.block_load_evidence.tip_hash,
        ),
        fixed_tip.block_id
    );
    assert_eq!(
        outcome.block_load_evidence.block_count,
        fixture
            .manifest
            .canonical_block_facts_digest_evidence
            .block_count
    );
    assert_eq!(outcome.subtree_root_load_evidence.subtree_root_count, 0);
    assert!(outcome.source_tip_checkpoint_authenticated);
    assert_eq!(outcome.settled_tip, fixed_tip.block_id);
    assert_eq!(outcome.block_load_evidence.settled_segment_index_count, 0);
    assert_eq!(
        outcome.block_load_evidence.live_tail_block_count,
        outcome.block_load_evidence.block_count
    );
    assert!(outcome.block_load_evidence.logical_replay_bytes > 0);
    assert_eq!(
        outcome
            .block_load_evidence
            .settled_segment_index_logical_bytes,
        0
    );
    assert!(outcome.block_load_evidence.live_tail_block_logical_bytes > 0);
    assert_eq!(outcome.block_load_evidence.transaction_blob_count, 0);
    assert_eq!(
        outcome.block_load_evidence.transaction_blob_logical_bytes,
        0
    );
    assert_eq!(outcome.block_load_evidence.block_blob_count, 0);
    assert_eq!(outcome.block_load_evidence.block_blob_logical_bytes, 0);
    assert_eq!(
        outcome
            .block_load_evidence
            .live_tail_raw_block_logical_bytes,
        0
    );
}

fn assert_canonical_fixture_ready_evidence(
    outcome: &CanonicalFixtureRocksDbReplayOutcome,
    fixture: &RegtestFixtureCase,
    fixed_tip: &CommitmentTreeCheckpoint,
) -> Result<()> {
    let expected_digest_evidence = &fixture.manifest.canonical_block_facts_digest_evidence;
    let expected_sequence_digest: [u8; 32] =
        hex::decode(&expected_digest_evidence.sequence_digest_sha256)?
            .try_into()
            .map_err(|_| eyre!("fixture sequence digest must contain 32 bytes"))?;
    assert_eq!(outcome.event_fence.visible_tip(), fixed_tip.block_id);
    assert_eq!(outcome.event_fence.chain_epoch_id().value(), 1);
    assert_eq!(outcome.event_fence.chain_event_sequence(), 1);
    assert_eq!(
        outcome.replayed_block_count,
        expected_digest_evidence.block_count
    );
    assert_eq!(
        outcome.published_ready_evidence,
        outcome.reopened_ready_evidence
    );
    assert_eq!(
        outcome.published_ready_evidence.first_retained_block,
        BlockId::new(fixture.block.height, fixture.block.hash)
    );
    assert_eq!(
        outcome.published_ready_evidence.visible_tip,
        fixed_tip.block_id
    );
    assert_eq!(outcome.published_ready_evidence.visible_epoch.value(), 1);
    assert_eq!(outcome.published_ready_evidence.visible_event_sequence, 1);
    assert_eq!(
        outcome.published_ready_evidence.visible_block_count,
        expected_digest_evidence.block_count
    );
    assert_eq!(
        outcome
            .published_ready_evidence
            .block_digest_version
            .value(),
        expected_digest_evidence.block_digest_version
    );
    assert_eq!(
        outcome
            .published_ready_evidence
            .sequence_digest_version
            .value(),
        expected_digest_evidence.sequence_digest_version
    );
    assert_eq!(
        outcome.published_ready_evidence.visible_sequence_digest,
        expected_sequence_digest
    );
    Ok(())
}

fn assert_no_materialized_view_store(canonical_store_path: &std::path::Path) {
    assert!(
        !zinder_materialized_views::MaterializedViewStore::path_for_canonical(canonical_store_path)
            .exists(),
        "canonical fixture replay must not create a materialized-view store"
    );
}

fn assert_raw_blob_retention_comparison_report(report: &Value, fixture: &FixtureManifest) {
    assert_eq!(
        report["contract_identity"],
        "rocksdb-raw-blob-retention-comparison"
    );
    assert_eq!(report["report_format_version"], 1);
    assert_eq!(
        report["arm_execution_order"],
        serde_json::json!(["none", "all"])
    );
    assert_eq!(report["none"]["raw_blob_retention"], "none");
    assert_eq!(report["all"]["raw_blob_retention"], "all");
    assert_eq!(
        report["none"]["fixture_manifest_digest_sha256"],
        report["all"]["fixture_manifest_digest_sha256"]
    );
    assert_eq!(
        report["none"]["replay_plan_digest_sha256"],
        report["all"]["replay_plan_digest_sha256"]
    );
    assert_eq!(
        report["none"]["effective_limits"],
        report["all"]["effective_limits"]
    );
    assert_eq!(
        report["none"]["logical_replay_identity"],
        report["all"]["logical_replay_identity"]
    );
    assert_raw_blob_retention_counts_and_v2_identity(report, fixture);
    assert_eq!(
        report["none"]["secondary_ready_matches_reopened_primary"].as_bool(),
        Some(true)
    );
    assert_eq!(
        report["all"]["secondary_ready_matches_reopened_primary"].as_bool(),
        Some(true)
    );
    assert!(
        report["none"]["authenticated_replay_lifecycle_seconds"]
            .as_f64()
            .is_some_and(|seconds| seconds > 0.0)
    );
    assert!(
        report["all"]["authenticated_replay_lifecycle_seconds"]
            .as_f64()
            .is_some_and(|seconds| seconds > 0.0)
    );
    assert!(
        report["none"]["authenticated_replay_lifecycle_blocks_per_second"]
            .as_f64()
            .is_some_and(|blocks_per_second| blocks_per_second > 0.0)
    );
    assert!(
        report["all"]["authenticated_replay_lifecycle_blocks_per_second"]
            .as_f64()
            .is_some_and(|blocks_per_second| blocks_per_second > 0.0)
    );
}

fn assert_raw_blob_retention_counts_and_v2_identity(report: &Value, fixture: &FixtureManifest) {
    let none_identity = &report["none"]["logical_replay_identity"];
    let none_layout = &report["none"]["storage_layout_evidence"];
    let all_layout = &report["all"]["storage_layout_evidence"];
    assert_eq!(none_layout["settled_segment_index_count"], 0);
    assert_eq!(none_layout["live_tail_block_count"], fixture.block_count);
    assert_eq!(none_layout["settled_segment_index_logical_bytes"], 0);
    assert!(
        none_layout["live_tail_block_logical_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(all_layout["settled_segment_index_count"], 0);
    assert_eq!(all_layout["live_tail_block_count"], fixture.block_count);
    assert_eq!(all_layout["settled_segment_index_logical_bytes"], 0);
    assert!(
        all_layout["live_tail_block_logical_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert!(
        none_identity["logical_replay_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(none_identity["subtree_root_count"], 1);
    assert_eq!(none_identity["subtree_root_coverage"], "complete-prefix");
    assert_eq!(none_identity["subtree_root_sequence_digest_version"], 1);

    let none_blobs = &report["none"]["raw_blob_counts"];
    let all_blobs = &report["all"]["raw_blob_counts"];
    assert_eq!(none_blobs["transaction_blob_count"], 0);
    assert_eq!(none_blobs["transaction_blob_logical_bytes"], 0);
    assert_eq!(none_blobs["block_blob_count"], 0);
    assert_eq!(none_blobs["block_blob_logical_bytes"], 0);
    assert_eq!(none_blobs["live_tail_raw_block_logical_bytes"], 0);
    assert!(
        all_blobs["transaction_blob_count"]
            .as_u64()
            .is_some_and(|count| count > 0)
    );
    assert!(
        all_blobs["transaction_blob_logical_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(all_blobs["block_blob_count"], fixture.block_count);
    assert!(
        all_blobs["block_blob_logical_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert!(
        all_blobs["live_tail_raw_block_logical_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
}

#[tokio::test]
async fn canonical_replay_plan_captures_digest_bound_predecessor_and_tip_checkpoints() -> Result<()>
{
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let predecessor_height = BlockHeight::new(fixture.manifest.from_height - 1);
    let (source, predecessor, fixed_tip) = fixture_checkpoint_source(&fixture)?;
    let manifest_path = fixture.directory.path().join("manifest.json");
    let original_manifest_bytes = std::fs::read(&manifest_path)?;

    let captured =
        capture_canonical_fixture_replay_plan(fixture.directory.path(), &source, &activations)
            .await?;
    let admitted = CanonicalFixtureReplayPlan::read(
        fixture.directory.path(),
        &fixture.manifest,
        &activations,
    )?;

    assert_eq!(
        source.requested_heights.lock().as_slice(),
        [predecessor_height, fixture.block.height]
    );
    assert_eq!(captured, admitted);
    assert_eq!(admitted.history_predecessor_checkpoint()?, predecessor);
    assert_eq!(admitted.source_tip_checkpoint()?, fixed_tip);
    assert_eq!(admitted.digest_sha256()?.len(), 64);
    assert_eq!(std::fs::read(manifest_path)?, original_manifest_bytes);

    let sidecar =
        std::fs::read_to_string(fixture.directory.path().join("canonical-replay-plan.json"))?;
    assert!(!sidecar.contains("tree_size"));
    assert!(sidecar.contains("final_root_hex"));
    assert!(sidecar.contains("final_state_hex"));
    let encoded_plan: Value = serde_json::from_str(&sidecar)?;
    assert_eq!(
        encoded_plan["contract_identity"],
        "canonical-fixture-replay-plan"
    );
    assert_eq!(encoded_plan["format_version"], 1);
    assert_eq!(
        encoded_plan["network_upgrade_activations_fingerprint_version"],
        1
    );
    assert!(
        encoded_plan
            .get("canonical_replay_plan_format_version")
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_source_serves_only_the_admitted_boundary_checkpoints() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, predecessor, fixed_tip) = fixture_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;

    let source = CanonicalFixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)?;

    assert_eq!(
        source
            .fetch_chain_checkpoint(predecessor.block_id.height, &activations)
            .await?,
        predecessor
    );
    assert_eq!(
        source
            .fetch_chain_checkpoint(fixed_tip.block_id.height, &activations)
            .await?,
        fixed_tip
    );
    assert!(!source.capabilities().supports(NodeCapability::TreeState));
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_source_rejects_a_different_activation_fingerprint() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, predecessor, _) = fixture_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    let source = CanonicalFixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)?;
    let mismatched_activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);

    let outcome = source
        .fetch_chain_checkpoint(predecessor.block_id.height, &mismatched_activations)
        .await;

    assert!(matches!(
        outcome,
        Err(SourceError::SourceProtocolMismatch { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_source_rejects_an_unrelated_checkpoint_height() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, _, _) = fixture_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    let source = CanonicalFixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)?;
    let unrelated_height = BlockHeight::new(fixture.manifest.from_height.saturating_sub(2));

    let outcome = source
        .fetch_chain_checkpoint(unrelated_height, &activations)
        .await;

    assert!(matches!(
        outcome,
        Err(SourceError::BlockUnavailable { height, .. }) if height == unrelated_height
    ));
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_rocksdb_replay_publishes_and_cold_reopens_ready() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, predecessor, fixed_tip) =
        fixture_construction_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    let output_directory = tempdir()?;
    let canonical_store_path = output_directory.path().join("canonical");
    let config =
        canonical_fixture_rocksdb_replay_config(fixture.directory.path(), &canonical_store_path);

    let outcome = replay_canonical_fixture_into_rocksdb(config).await?;

    assert_canonical_fixture_load_evidence(&outcome, &fixture, &predecessor, &fixed_tip);
    assert_canonical_fixture_ready_evidence(&outcome, &fixture, &fixed_tip)?;
    assert_no_materialized_view_store(&canonical_store_path);
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_replay_preserves_historical_subtree_prefix_after_cold_reopen_and_secondary()
-> Result<()> {
    let mut fixture = write_regtest_fixture()?;
    let expected_root = add_historical_sapling_root(&mut fixture)?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, _, _) =
        fixture_construction_checkpoint_source_with_completed_sapling(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    let output_directory = tempdir()?;
    let canonical_store_path = output_directory.path().join("canonical");
    let secondary_path = output_directory.path().join("secondary");
    let config =
        canonical_fixture_rocksdb_replay_config(fixture.directory.path(), &canonical_store_path);

    let outcome = replay_canonical_fixture_into_rocksdb(config).await?;

    assert_eq!(
        outcome.subtree_root_load_evidence.coverage,
        CanonicalSubtreeRootLoadCoverage::CompletePrefix
    );
    assert_eq!(
        outcome.subtree_root_load_evidence.sequence_digest_version,
        1
    );
    assert_eq!(outcome.subtree_root_load_evidence.subtree_root_count, 1);
    let subtree_range = SubtreeRootRange::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(0),
        NonZeroU32::MIN,
    );
    let primary = RocksDbCanonicalStore::open_ready(
        &canonical_store_path,
        &activations,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(1)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(
        primary.subtree_roots(subtree_range)?,
        vec![expected_root.clone()]
    );
    drop(primary);

    let secondary = RocksDbCanonicalSecondary::open_ready(
        &canonical_store_path,
        &secondary_path,
        &activations,
        zinder_store::CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::None),
        CanonicalReorgPolicy::new(1)?,
        RocksDbResourceBudget::for_local_tests(),
    )?;
    assert_eq!(secondary.subtree_roots(subtree_range)?, vec![expected_root]);
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_replay_cli_emits_self_validating_zero_prohibited_reads() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, _, _) = fixture_construction_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    let output_directory = tempdir()?;
    let canonical_store_path = output_directory.path().join("canonical");
    let report_path = output_directory
        .path()
        .join("canonical-fixture-replay.json");

    let output = Command::new(zinder_testkit::bin_exe!("zinder-bench"))
        .arg("rocksdb-canonical-fixture-replay")
        .arg("--fixture")
        .arg(fixture.directory.path())
        .arg("--canonical-store")
        .arg(&canonical_store_path)
        .arg("--request-timeout-secs")
        .arg("5")
        .arg("--supported-reorg-depth")
        .arg("1")
        .arg("--report")
        .arg(&report_path)
        .output()?;
    if !output.status.success() {
        return Err(eyre!(
            "canonical fixture replay CLI failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let report: Value = serde_json::from_slice(&std::fs::read(&report_path)?)?;
    assert_eq!(
        report["measurement_kind"],
        "rocksdb-canonical-fixture-replay"
    );
    assert_cli_report_v2_coverage(&report, "cadence-and-tip", 2, 1);
    assert_eq!(
        report["prohibited_reads"]["historical_prevout_read_count"],
        0
    );
    assert_eq!(
        report["prohibited_reads"]["cross_block_wallet_read_count"],
        0
    );
    Ok(())
}

fn assert_unsealed_v2_source_load(source_load: &Value, expected_blocks: u64) {
    assert_eq!(source_load["block_count"], expected_blocks);
    assert_eq!(source_load["settled_segment_index_count"], 0);
    assert_eq!(source_load["live_tail_block_count"], expected_blocks);
    assert!(
        source_load["logical_replay_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(source_load["settled_segment_index_logical_bytes"], 0);
    assert!(
        source_load["live_tail_block_logical_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(source_load["live_tail_raw_block_logical_bytes"], 0);
}

fn assert_cli_report_v2_coverage(
    report: &Value,
    expected_coverage: &str,
    expected_checkpoints: u64,
    expected_blocks: u64,
) {
    assert_eq!(report["report_format_version"], 1);
    assert_eq!(
        report["resource_limits"]["tree_state_checkpoint_coverage"],
        expected_coverage
    );
    assert_eq!(
        report["source_load"]["tree_state_checkpoint_count"],
        expected_checkpoints
    );
    assert_unsealed_v2_source_load(&report["source_load"], expected_blocks);
}

fn coinbase_only_linked_block(
    template: &SourceBlock,
    target_height: u32,
    parent_hash: BlockHash,
    block_time_seconds: u32,
) -> Result<SourceBlock> {
    let mut parsed: ZebraBlock = template
        .raw_block_bytes
        .as_slice()
        .zcash_deserialize_into()?;
    let coinbase = parsed
        .transactions
        .first()
        .ok_or_else(|| eyre!("missing template coinbase"))?;
    let mut bytes = coinbase.zcash_serialize_to_vec()?;
    // ZIP 244 excludes the coinbase input script from the transaction ID.
    // Give each synthetic coinbase a distinct lock time as well as a height.
    let lock_height = target_height
        .checked_sub(1)
        .ok_or_else(|| eyre!("missing predecessor"))?;
    bytes
        .get_mut(12..16)
        .ok_or_else(|| eyre!("missing lock time in fixture transaction"))?
        .copy_from_slice(&lock_height.to_le_bytes());
    parsed.transactions = vec![std::sync::Arc::new(
        bytes.as_slice().zcash_deserialize_into()?,
    )];
    let mut raw_bytes = parsed.zcash_serialize_to_vec()?;
    raw_bytes
        .get_mut(4..36)
        .ok_or_else(|| eyre!("template block is too short for parent hash"))?
        .copy_from_slice(&parent_hash.as_bytes());
    raw_bytes
        .get_mut(100..104)
        .ok_or_else(|| eyre!("template block is too short for header timestamp"))?
        .copy_from_slice(&block_time_seconds.to_le_bytes());
    let old_prefix = [0xff_u8, 0xff, 0xff, 0xff, 0x2a, 0x02, 0x5b, 0x02];
    let target_le = u16::try_from(target_height)?.to_le_bytes();
    let new_prefix = [
        0xff_u8,
        0xff,
        0xff,
        0xff,
        0x2a,
        0x02,
        target_le[0],
        target_le[1],
    ];
    let offset = raw_bytes
        .windows(old_prefix.len())
        .position(|window| window == old_prefix)
        .ok_or_else(|| eyre!("template coinbase height prefix not found"))?;
    raw_bytes[offset..offset + old_prefix.len()].copy_from_slice(&new_prefix);
    let reparsed: ZebraBlock = raw_bytes.as_slice().zcash_deserialize_into()?;
    if reparsed.coinbase_height() != Some(ZebraHeight(target_height)) {
        return Err(eyre!(
            "relinked block coinbase height {:?} != {target_height}",
            reparsed.coinbase_height()
        ));
    }
    Ok(SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        BlockHeight::new(target_height),
        raw_bytes,
    )?)
}

fn write_multi_block_regtest_fixture() -> Result<(tempfile::TempDir, Vec<SourceBlock>)> {
    let block_603 = load_regtest_block(REGTEST_BLOCK_603)?;
    let block_604 = coinbase_only_linked_block(
        &block_603,
        604,
        block_603.hash,
        block_603.block_time_seconds.saturating_add(10),
    )?;
    let block_605 = coinbase_only_linked_block(
        &block_603,
        605,
        block_604.hash,
        block_603.block_time_seconds.saturating_add(20),
    )?;
    let block_606 = coinbase_only_linked_block(
        &block_603,
        606,
        block_605.hash,
        block_603.block_time_seconds.saturating_add(30),
    )?;
    let blocks = vec![block_603, block_604, block_605, block_606];
    let activations = sample_regtest_upgrade_activations();
    let directory = tempdir()?;
    let descriptor = write_segment(directory.path(), 0, &blocks)?;
    let measurements = measure_fixture_blocks(&blocks, &activations)?;
    let tip = blocks
        .last()
        .ok_or_else(|| eyre!("multi-block fixture must contain blocks"))?;
    let manifest = FixtureManifest {
        contract_identity: FIXTURE_CONTRACT_IDENTITY.to_owned(),
        fixture_format_version: FIXTURE_FORMAT_VERSION,
        network: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
        from_height: 603,
        to_height: 606,
        block_count: 4,
        workload_density: measurements.workload_density,
        canonical_artifact_schema_version: zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        canonical_block_facts_digest_evidence: measurements
            .canonical_block_facts_digest_evidence()?,
        tip_hash_hex: hex::encode(tip.hash.as_bytes()),
        network_upgrade_activations: regtest_activation_records(),
        segments: vec![descriptor],
        subtree_roots: SubtreeRootSet::default(),
    };
    manifest.write(directory.path())?;
    Ok((directory, blocks))
}

fn multi_block_checkpoint_source(blocks: &[SourceBlock]) -> Result<CheckpointSource> {
    let activations = sample_regtest_upgrade_activations();
    let first = blocks
        .first()
        .ok_or_else(|| eyre!("multi-block fixture must have a first block"))?;
    let predecessor_height = BlockHeight::new(first.height.value() - 1);
    let predecessor = CommitmentTreeCheckpoint::new(
        BlockId::new(predecessor_height, first.parent_hash),
        first.block_time_seconds.saturating_sub(1),
        checkpoint_frontiers_at(predecessor_height)?,
    );
    let mut accumulator = CommitmentTreeAccumulator::from_validated_frontiers(
        predecessor.block_id.height,
        &predecessor.frontiers,
        &activations,
    )?;
    for block in blocks {
        let prepared = prepare_canonical_block(block, &activations, RawBlobPolicy::None)?;
        let mut sapling = Vec::new();
        let mut orchard = Vec::new();
        let mut ironwood = Vec::new();
        for transaction in prepared.partial_compact_block.transactions() {
            sapling.extend(
                transaction
                    .data
                    .sapling_outputs
                    .iter()
                    .map(|output| output.commitment),
            );
            orchard.extend(
                transaction
                    .data
                    .orchard_actions
                    .iter()
                    .map(|action| action.commitment),
            );
            ironwood.extend(
                transaction
                    .data
                    .ironwood_actions
                    .iter()
                    .map(|action| action.commitment),
            );
        }
        accumulator.append_block_commitments(block.height, &sapling, &orchard, &ironwood)?;
    }
    let tip = blocks
        .last()
        .ok_or_else(|| eyre!("multi-block fixture must have a tip block"))?;
    let fixed_tip = CommitmentTreeCheckpoint::new(
        BlockId::new(tip.height, tip.hash),
        tip.block_time_seconds,
        accumulator.validated_frontiers()?,
    );
    Ok(CheckpointSource::new(vec![predecessor, fixed_tip]))
}

fn run_canonical_fixture_replay_cli(
    fixture_directory: &std::path::Path,
    canonical_store_path: &std::path::Path,
    report_path: &std::path::Path,
    coverage_flag: Option<&str>,
) -> Result<Value> {
    let mut command = Command::new(zinder_testkit::bin_exe!("zinder-bench"));
    command
        .arg("rocksdb-canonical-fixture-replay")
        .arg("--fixture")
        .arg(fixture_directory)
        .arg("--canonical-store")
        .arg(canonical_store_path)
        .arg("--request-timeout-secs")
        .arg("5")
        .arg("--supported-reorg-depth")
        .arg("1")
        .arg("--report")
        .arg(report_path);
    if let Some(coverage) = coverage_flag {
        command
            .arg("--tree-state-checkpoint-coverage")
            .arg(coverage);
    }
    let output = command.output()?;
    if !output.status.success() {
        return Err(eyre!(
            "canonical fixture replay CLI failed:\nstderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(serde_json::from_slice(&std::fs::read(report_path)?)?)
}

#[tokio::test]
async fn canonical_fixture_replay_cli_defaults_to_cadence_and_tip_and_supports_every_block()
-> Result<()> {
    let (fixture_directory, blocks) = write_multi_block_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let checkpoint_source = multi_block_checkpoint_source(&blocks)?;
    capture_canonical_fixture_replay_plan(
        fixture_directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;

    let output_directory = tempdir()?;
    let dense_store_path = output_directory.path().join("canonical-dense");
    let dense_report = run_canonical_fixture_replay_cli(
        fixture_directory.path(),
        &dense_store_path,
        &output_directory.path().join("dense-report.json"),
        Some("every-block"),
    )?;
    assert_cli_report_v2_coverage(&dense_report, "every-block", 5, 4);
    let reopen = |path: &std::path::Path| -> Result<RocksDbCanonicalStore> {
        Ok(RocksDbCanonicalStore::open_ready(
            path,
            &activations,
            RawBlobRetention::None,
            CanonicalReorgPolicy::new(1)?,
            RocksDbResourceBudget::for_local_tests(),
        )?)
    };
    let dense_store = reopen(&dense_store_path)?;
    for height in 602_u32..=606 {
        assert!(
            matches!(
                dense_store.tree_state_lookup(BlockHeight::new(height), &activations)?,
                Some(TreeStateLookup::Stored(_))
            ),
            "every-block fixture replay must store checkpoint at height {height}"
        );
    }

    let sparse_store_path = output_directory.path().join("canonical-sparse");
    let sparse_report = run_canonical_fixture_replay_cli(
        fixture_directory.path(),
        &sparse_store_path,
        &output_directory.path().join("sparse-report.json"),
        None,
    )?;
    assert_cli_report_v2_coverage(&sparse_report, "cadence-and-tip", 2, 4);
    let sparse_store = reopen(&sparse_store_path)?;
    for height in [602_u32, 606] {
        assert!(matches!(
            sparse_store.tree_state_lookup(BlockHeight::new(height), &activations)?,
            Some(TreeStateLookup::Stored(_))
        ));
    }
    for height in 603_u32..=605 {
        assert!(
            matches!(
                sparse_store.tree_state_lookup(BlockHeight::new(height), &activations)?,
                Some(TreeStateLookup::Replay(_))
            ),
            "cadence-and-tip fixture replay must keep off-cadence height {height} unstored"
        );
    }
    Ok(())
}

#[tokio::test]
async fn raw_blob_retention_comparison_replays_both_arms_and_admits_fresh_secondaries() -> Result<()>
{
    let mut fixture = write_regtest_fixture()?;
    add_historical_sapling_root(&mut fixture)?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, _, _) =
        fixture_construction_checkpoint_source_with_completed_sapling(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    let output_directory = tempdir()?;
    let none_canonical = output_directory.path().join("none-canonical");
    let none_secondary = output_directory.path().join("none-secondary");
    let all_canonical = output_directory.path().join("all-canonical");
    let all_secondary = output_directory.path().join("all-secondary");
    let report_path = output_directory.path().join("comparison.json");

    let output = Command::new(zinder_testkit::bin_exe!("zinder-bench"))
        .arg("rocksdb-raw-blob-retention-comparison")
        .arg("--fixture")
        .arg(fixture.directory.path())
        .arg("--none-canonical-store")
        .arg(&none_canonical)
        .arg("--none-secondary-root")
        .arg(&none_secondary)
        .arg("--all-canonical-store")
        .arg(&all_canonical)
        .arg("--all-secondary-root")
        .arg(&all_secondary)
        .arg("--request-timeout-secs")
        .arg("5")
        .arg("--supported-reorg-depth")
        .arg("1")
        .arg("--report")
        .arg(&report_path)
        .output()?;
    if !output.status.success() {
        return Err(eyre!(
            "raw-blob retention comparison failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let report: Value = serde_json::from_slice(&std::fs::read(&report_path)?)?;
    assert_raw_blob_retention_comparison_report(&report, &fixture.manifest);
    assert_eq!(
        report["none"]["effective_limits"]["tree_state_checkpoint_coverage"],
        "cadence-and-tip"
    );
    assert!(none_canonical.is_dir());
    assert!(none_secondary.is_dir());
    assert!(all_canonical.is_dir());
    assert!(all_secondary.is_dir());
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_rocksdb_replay_preserves_an_existing_store_path() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, _, _) = fixture_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    let output_directory = tempdir()?;
    let canonical_store_path = output_directory.path().join("canonical");
    std::fs::create_dir(&canonical_store_path)?;
    let sentinel_path = canonical_store_path.join("sentinel");
    std::fs::write(&sentinel_path, b"preserve-existing-store")?;
    let config =
        canonical_fixture_rocksdb_replay_config(fixture.directory.path(), &canonical_store_path);

    let error = replay_canonical_fixture_into_rocksdb(config)
        .await
        .err()
        .ok_or_else(|| eyre!("an existing canonical path must be rejected"))?;

    assert!(error.to_string().contains("requires a fresh path"));
    assert_eq!(std::fs::read(&sentinel_path)?, b"preserve-existing-store");
    assert_eq!(std::fs::read_dir(&canonical_store_path)?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_rocksdb_replay_leaves_a_mismatched_tip_checkpoint_unpublished()
-> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (_, predecessor, mut fixed_tip) = fixture_construction_checkpoint_source(&fixture)?;
    fixed_tip.block_time_seconds = fixed_tip.block_time_seconds.saturating_add(1);
    let checkpoint_source = CheckpointSource::new(vec![predecessor, fixed_tip]);
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    let output_directory = tempdir()?;
    let canonical_store_path = output_directory.path().join("canonical");
    let config =
        canonical_fixture_rocksdb_replay_config(fixture.directory.path(), &canonical_store_path);

    let error = replay_canonical_fixture_into_rocksdb(config)
        .await
        .err()
        .ok_or_else(|| eyre!("a mismatched fixed-tip checkpoint must fail construction"))?;

    assert!(error.to_string().contains("fixed-tip checkpoint differs"));
    let open_error = RocksDbCanonicalStore::open_ready(
        &canonical_store_path,
        &activations,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(1)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or_else(|| eyre!("a failed source authentication must not publish READY"))?;
    assert!(matches!(
        open_error,
        CanonicalStoreError::StoreNotReady { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_rocksdb_replay_rejects_zero_supported_reorg_depth() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let output_directory = tempdir()?;
    let canonical_store_path = output_directory.path().join("canonical");
    let mut config =
        canonical_fixture_rocksdb_replay_config(fixture.directory.path(), &canonical_store_path);
    config.supported_reorg_depth = 0;

    let error = replay_canonical_fixture_into_rocksdb(config)
        .await
        .err()
        .ok_or_else(|| eyre!("zero supported reorg depth must be rejected"))?;

    assert!(error.to_string().contains("supported_reorg_depth"));
    assert!(!canonical_store_path.exists());
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_rocksdb_replay_rejects_a_zero_request_timeout() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let output_directory = tempdir()?;
    let canonical_store_path = output_directory.path().join("canonical");
    let mut config =
        canonical_fixture_rocksdb_replay_config(fixture.directory.path(), &canonical_store_path);
    config.request_timeout = Duration::ZERO;

    let error = replay_canonical_fixture_into_rocksdb(config)
        .await
        .err()
        .ok_or_else(|| eyre!("a zero request timeout must be rejected"))?;

    assert!(error.to_string().contains("request_timeout"));
    assert!(!canonical_store_path.exists());
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_rocksdb_replay_leaves_manifest_digest_drift_unpublished() -> Result<()> {
    let mut fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (checkpoint_source, _, _) = fixture_construction_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &checkpoint_source,
        &activations,
    )
    .await?;
    fixture
        .manifest
        .canonical_block_facts_digest_evidence
        .sequence_digest_sha256 = "00".repeat(32);
    fixture.manifest.write(fixture.directory.path())?;
    let sidecar_path = fixture.directory.path().join("canonical-replay-plan.json");
    let mut encoded_plan: Value = serde_json::from_slice(&std::fs::read(&sidecar_path)?)?;
    encoded_plan["fixture_manifest_sha256"] = Value::String(fixture.manifest.digest_sha256()?);
    std::fs::write(&sidecar_path, serde_json::to_vec_pretty(&encoded_plan)?)?;
    let output_directory = tempdir()?;
    let canonical_store_path = output_directory.path().join("canonical");
    let config =
        canonical_fixture_rocksdb_replay_config(fixture.directory.path(), &canonical_store_path);

    let error = replay_canonical_fixture_into_rocksdb(config)
        .await
        .err()
        .ok_or_else(|| eyre!("manifest digest drift must fail canonical publication"))?;

    assert!(error.to_string().contains("block load evidence differs"));
    let open_error = RocksDbCanonicalStore::open_ready(
        &canonical_store_path,
        &activations,
        RawBlobRetention::None,
        CanonicalReorgPolicy::new(1)?,
        RocksDbResourceBudget::for_local_tests(),
    )
    .err()
    .ok_or_else(|| eyre!("manifest digest drift must leave canonical READY unpublished"))?;
    assert!(matches!(
        open_error,
        CanonicalStoreError::StoreNotReady { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_checkpoint_capture_preserves_existing_sidecar_bytes() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (source, _, _) = fixture_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(fixture.directory.path(), &source, &activations).await?;
    let sidecar_path = fixture.directory.path().join("canonical-replay-plan.json");
    let captured_bytes = std::fs::read(&sidecar_path)?;
    let second_source = source.clone();

    let second_capture = capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &second_source,
        &activations,
    )
    .await;

    let error = second_capture.err().ok_or_else(|| {
        eyre!("canonical fixture checkpoint capture must refuse an existing sidecar")
    })?;
    assert!(error.to_string().contains("already exists"));
    assert_eq!(std::fs::read(sidecar_path)?, captured_bytes);
    assert_eq!(second_source.requested_heights.lock().len(), 2);
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_replay_plan_rejects_a_different_manifest_digest() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (source, _, _) = fixture_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(fixture.directory.path(), &source, &activations).await?;
    let sidecar_path = fixture.directory.path().join("canonical-replay-plan.json");
    let mut encoded_plan: Value = serde_json::from_slice(&std::fs::read(&sidecar_path)?)?;
    encoded_plan["fixture_manifest_sha256"] = Value::String("00".repeat(32));
    std::fs::write(&sidecar_path, serde_json::to_vec_pretty(&encoded_plan)?)?;

    let error =
        CanonicalFixtureReplayPlan::read(fixture.directory.path(), &fixture.manifest, &activations)
            .err()
            .ok_or_else(|| eyre!("a replay plan bound to another manifest must be rejected"))?;

    assert!(error.to_string().contains("manifest SHA-256"));
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_checkpoint_capture_rejects_a_different_activation_fingerprint()
-> Result<()> {
    let fixture = write_regtest_fixture()?;
    let (source, _, _) = fixture_checkpoint_source(&fixture)?;
    let mismatched_activations = NetworkUpgradeActivations::empty(Network::ZcashRegtest);

    let error = capture_canonical_fixture_replay_plan(
        fixture.directory.path(),
        &source,
        &mismatched_activations,
    )
    .await
    .err()
    .ok_or_else(|| eyre!("checkpoint capture must reject a different activation table"))?;

    assert!(error.to_string().contains("activation fingerprint"));
    assert!(source.requested_heights.lock().is_empty());
    assert!(
        !fixture
            .directory
            .path()
            .join("canonical-replay-plan.json")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_replay_plan_rejects_a_malformed_frontier() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (source, _, _) = fixture_checkpoint_source(&fixture)?;
    capture_canonical_fixture_replay_plan(fixture.directory.path(), &source, &activations).await?;
    let sidecar_path = fixture.directory.path().join("canonical-replay-plan.json");
    let mut encoded_plan: Value = serde_json::from_slice(&std::fs::read(&sidecar_path)?)?;
    encoded_plan["source_tip_checkpoint"]["frontiers"]["sapling"]["final_state_hex"] =
        Value::String("00".to_owned());
    std::fs::write(&sidecar_path, serde_json::to_vec_pretty(&encoded_plan)?)?;

    let error =
        CanonicalFixtureReplayPlan::read(fixture.directory.path(), &fixture.manifest, &activations)
            .err()
            .ok_or_else(|| eyre!("a malformed canonical finalState must be rejected"))?;

    assert!(
        error
            .to_string()
            .contains("invalid source tip checkpoint Sapling frontier")
    );
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_checkpoint_capture_rejects_a_disconnected_predecessor() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (_, mut predecessor, fixed_tip) = fixture_checkpoint_source(&fixture)?;
    predecessor.block_id = BlockId::new(
        predecessor.block_id.height,
        BlockHash::from_bytes([0x42; 32]),
    );
    let source = CheckpointSource::new(vec![predecessor, fixed_tip]);

    let error =
        capture_canonical_fixture_replay_plan(fixture.directory.path(), &source, &activations)
            .await
            .err()
            .ok_or_else(|| {
                eyre!("a checkpoint disconnected from the first block must be rejected")
            })?;

    assert!(
        error
            .to_string()
            .contains("does not link to fixture first block")
    );
    assert!(
        !fixture
            .directory
            .path()
            .join("canonical-replay-plan.json")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_checkpoint_capture_rejects_a_wrong_source_tip() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let activations = sample_regtest_upgrade_activations();
    let (_, predecessor, mut fixed_tip) = fixture_checkpoint_source(&fixture)?;
    fixed_tip.block_id = BlockId::new(fixed_tip.block_id.height, BlockHash::from_bytes([0x24; 32]));
    let source = CheckpointSource::new(vec![predecessor, fixed_tip]);

    let error =
        capture_canonical_fixture_replay_plan(fixture.directory.path(), &source, &activations)
            .await
            .err()
            .ok_or_else(|| eyre!("a checkpoint different from the fixture tip must be rejected"))?;

    assert!(error.to_string().contains("and manifest tip"));
    assert!(
        !fixture
            .directory
            .path()
            .join("canonical-replay-plan.json")
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn canonical_fixture_checkpoint_capture_rejects_disconnected_segment_boundaries_before_rpc()
-> Result<()> {
    let first_block = load_regtest_block(REGTEST_BLOCK_603)?;
    let second_block = SourceBlock::from_raw_block_bytes(
        Network::ZcashRegtest,
        BlockHeight::new(604),
        first_block.raw_block_bytes.clone(),
    )?;
    let directory = tempdir()?;
    let first_segment = write_segment(directory.path(), 0, &[first_block])?;
    let second_segment = write_segment(directory.path(), 1, std::slice::from_ref(&second_block))?;
    let manifest = FixtureManifest {
        contract_identity: FIXTURE_CONTRACT_IDENTITY.to_owned(),
        fixture_format_version: FIXTURE_FORMAT_VERSION,
        network: encode_zinder_native_chain_name(Network::ZcashRegtest).to_owned(),
        from_height: 603,
        to_height: 604,
        block_count: 2,
        workload_density: WorkloadDensity {
            block_count: 2,
            ..WorkloadDensity::default()
        },
        canonical_artifact_schema_version: zinder_store::CURRENT_ARTIFACT_SCHEMA_VERSION.value(),
        canonical_block_facts_digest_evidence: CanonicalBlockFactsDigestEvidence {
            block_digest_version: zinder_core::CanonicalBlockFactsDigestVersion::CURRENT.value(),
            sequence_digest_version: zinder_core::CanonicalBlockFactsSequenceDigestVersion::CURRENT
                .value(),
            block_count: 2,
            sequence_digest_sha256: "00".repeat(32),
        },
        tip_hash_hex: hex::encode(second_block.hash.as_bytes()),
        network_upgrade_activations: regtest_activation_records(),
        segments: vec![first_segment, second_segment],
        subtree_roots: SubtreeRootSet::default(),
    };
    manifest.write(directory.path())?;
    let source = CheckpointSource::new(Vec::<CommitmentTreeCheckpoint>::new());

    let error = capture_canonical_fixture_replay_plan(
        directory.path(),
        &source,
        &sample_regtest_upgrade_activations(),
    )
    .await
    .err()
    .ok_or_else(|| eyre!("disconnected fixture segments must fail admission"))?;

    assert!(error.to_string().contains("not an ordered connected pair"));
    assert!(source.requested_heights.lock().is_empty());
    assert!(!directory.path().join("canonical-replay-plan.json").exists());
    Ok(())
}

#[test]
fn fixture_v1_requires_exact_contract_identity() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let mut missing_identity = serde_json::to_value(&fixture.manifest)?;
    missing_identity
        .as_object_mut()
        .ok_or_else(|| eyre!("fixture manifest must encode as an object"))?
        .remove("contract_identity");
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&missing_identity)?,
    )?;
    let Err(error) = FixtureManifest::read(fixture.directory.path()) else {
        return Err(eyre!("fixture v3 contract identity must be mandatory"));
    };
    assert!(error.to_string().contains("contract_identity"));

    let mut unrecognized_identity = fixture.manifest;
    unrecognized_identity.contract_identity = "zinder-bench-fixture-manifest".to_owned();
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&unrecognized_identity)?,
    )?;
    let Err(error) = FixtureManifest::read(fixture.directory.path()) else {
        return Err(eyre!(
            "fixture v3 must reject an unrecognized contract identity"
        ));
    };
    assert!(error.to_string().contains("fixture contract identity"));
    Ok(())
}

#[test]
fn fixture_v1_rejects_unknown_manifest_fields() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let mut unknown_field = serde_json::to_value(&fixture.manifest)?;
    unknown_field
        .as_object_mut()
        .ok_or_else(|| eyre!("fixture manifest must encode as an object"))?
        .insert(
            "unexpected_schema_version".to_owned(),
            serde_json::json!(14),
        );
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&unknown_field)?,
    )?;
    let Err(error) = FixtureManifest::read(fixture.directory.path()) else {
        return Err(eyre!("fixture v3 must reject unknown manifest fields"));
    };
    assert!(error.to_string().contains("unknown field"));
    Ok(())
}

#[test]
fn fixture_v1_rejects_format_v2_without_compatibility_decoding() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let mut encoded = serde_json::to_value(&fixture.manifest)?;
    encoded["fixture_format_version"] = serde_json::json!(2);
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&encoded)?,
    )?;

    let error = FixtureManifest::read(fixture.directory.path())
        .err()
        .ok_or_else(|| eyre!("fixture format v2 must be rejected"))?;

    assert!(
        error
            .to_string()
            .contains("unsupported fixture format version 2")
    );
    Ok(())
}

#[test]
fn fixture_v1_requires_an_exact_completing_block_hash() -> Result<()> {
    let mut fixture = write_regtest_fixture()?;
    let completing_block = BlockId::new(fixture.block.height, fixture.block.hash);
    fixture.manifest.subtree_roots.sapling = vec![subtree_root_record(0, 0x51, completing_block)];
    let mut encoded = serde_json::to_value(&fixture.manifest)?;
    encoded["subtree_roots"]["sapling"][0]["completing_block"]
        .as_object_mut()
        .ok_or_else(|| eyre!("completing block must encode as an object"))?
        .remove("hash_hex");
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&encoded)?,
    )?;

    let error = FixtureManifest::read(fixture.directory.path())
        .err()
        .ok_or_else(|| eyre!("a subtree root without a completion hash must be rejected"))?;

    assert!(error.to_string().contains("hash_hex"));
    Ok(())
}

#[test]
fn fixture_source_rejects_an_in_range_completing_block_hash_mismatch() -> Result<()> {
    let mut fixture = write_regtest_fixture()?;
    let completing_block = BlockId::new(fixture.block.height, BlockHash::from_bytes([0x9a; 32]));
    fixture.manifest.subtree_roots.sapling = vec![subtree_root_record(0, 0x51, completing_block)];

    let error = FixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)
        .err()
        .ok_or_else(|| eyre!("an in-range completion hash mismatch must be rejected"))?;

    assert!(
        error
            .to_string()
            .contains("completing block hash differs from captured block")
    );
    Ok(())
}

#[test]
fn fixture_v1_rejects_invalid_subtree_root_prefix_shape() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let completing_block = BlockId::new(fixture.block.height, fixture.block.hash);
    let root = subtree_root_record(0, 0x51, completing_block);

    let mut wrong_protocol = fixture.manifest.clone();
    wrong_protocol.subtree_roots.sapling = vec![CapturedSubtreeRoot {
        protocol: ShieldedProtocol::Orchard.rpc_pool_name().to_owned(),
        ..root.clone()
    }];
    let error = wrong_protocol
        .write(fixture.directory.path())
        .err()
        .ok_or_else(|| eyre!("a wrong-protocol subtree root must be rejected"))?;
    assert!(error.to_string().contains("claims protocol"));

    let mut noncontiguous = fixture.manifest.clone();
    noncontiguous.subtree_roots.sapling = vec![CapturedSubtreeRoot {
        index: 1,
        ..root.clone()
    }];
    let error = noncontiguous
        .write(fixture.directory.path())
        .err()
        .ok_or_else(|| eyre!("a non-contiguous subtree root must be rejected"))?;
    assert!(error.to_string().contains("has index 1, expected 0"));

    let mut duplicate = fixture.manifest.clone();
    duplicate.subtree_roots.sapling = vec![root.clone(), root.clone()];
    let error = duplicate
        .write(fixture.directory.path())
        .err()
        .ok_or_else(|| eyre!("a duplicate subtree-root index must be rejected"))?;
    assert!(error.to_string().contains("has index 0, expected 1"));

    let mut after_tip = fixture.manifest;
    after_tip.subtree_roots.sapling = vec![CapturedSubtreeRoot {
        completing_block: CapturedBlockId {
            height: after_tip.to_height.saturating_add(1),
            hash_hex: "42".repeat(32),
        },
        ..root
    }];
    let error = after_tip
        .write(fixture.directory.path())
        .err()
        .ok_or_else(|| eyre!("a completion after the fixture tip must be rejected"))?;
    assert!(error.to_string().contains("after fixture tip"));
    Ok(())
}

#[test]
fn segment_rejects_noncontiguous_blocks() -> Result<()> {
    let block_one = load_regtest_block(REGTEST_BLOCK_1)?;
    let block_six_hundred_three = load_regtest_block(REGTEST_BLOCK_603)?;
    let fixture_dir = tempdir()?;
    let noncontiguous = write_segment(
        fixture_dir.path(),
        99,
        &[block_one, block_six_hundred_three],
    );
    assert!(noncontiguous.is_err());
    Ok(())
}

#[tokio::test]
async fn segment_and_manifest_round_trip_preserves_blocks() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    assert_eq!(fixture.descriptor.block_count, 1);
    assert_eq!(fixture.descriptor.from_height, 603);
    assert_eq!(fixture.descriptor.to_height, 603);
    assert_eq!(fixture.descriptor.sha256.len(), 64);

    let read_back = read_segment_blocks(
        fixture.directory.path(),
        &fixture.descriptor,
        Network::ZcashRegtest,
    )?;
    assert_eq!(read_back, vec![fixture.block.clone()]);

    let reloaded = FixtureManifest::read(fixture.directory.path())?;
    assert_eq!(reloaded.digest_sha256()?.len(), 64);
    assert_eq!(reloaded.network_typed()?, Network::ZcashRegtest);
    assert_eq!(reloaded.to_height, 603);
    assert_eq!(reloaded.workload_density.block_count, 1);
    assert_eq!(reloaded.workload_density.transaction_count, 2);
    assert!(reloaded.workload_density.raw_block_bytes > 0);
    assert_eq!(
        reloaded
            .canonical_block_facts_digest_evidence
            .block_digest_version,
        zinder_core::CanonicalBlockFactsDigestVersion::CURRENT.value()
    );
    assert_eq!(
        reloaded
            .canonical_block_facts_digest_evidence
            .sequence_digest_version,
        zinder_core::CanonicalBlockFactsSequenceDigestVersion::CURRENT.value()
    );
    assert_eq!(
        reloaded.canonical_block_facts_digest_evidence.block_count,
        1
    );
    assert_eq!(
        reloaded
            .canonical_block_facts_digest_evidence
            .sequence_digest_sha256
            .len(),
        64
    );
    assert_eq!(
        reloaded.activations_typed()?.activations().len(),
        sample_regtest_upgrade_activations().activations().len()
    );

    let source = FixtureNodeSource::open(fixture.directory.path(), &reloaded)?;
    let fetched = source.fetch_block_at(BlockHeight::new(603)).await?;
    assert_eq!(fetched, fixture.block);
    assert_eq!(source.tip_id().await?, reloaded.tip_id()?);

    let segment_path = fixture.directory.path().join(&fixture.descriptor.file);
    OpenOptions::new()
        .append(true)
        .open(&segment_path)?
        .write_all(b"corruption")?;
    let Err(error) = FixtureNodeSource::open(fixture.directory.path(), &reloaded) else {
        return Err(eyre!("fixture open must verify each segment digest"));
    };
    assert!(error.to_string().contains("SHA-256"));
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn fixture_source_delays_each_segment_response_by_the_configured_duration() -> Result<()> {
    let fixture = write_repeated_payload_fixture()?;
    let source_segment_delay = Duration::from_millis(250);
    let source = FixtureNodeSource::open_with_segment_delay(
        fixture.directory.path(),
        &fixture.manifest,
        source_segment_delay,
    )?;
    let one_block_payload_bytes = raw_block_hex_length(REGTEST_BLOCK_603)?;
    let segment_limits = SourceChainSegmentLimits::new(
        SourceChainCursor::before_height(BlockHeight::new(603)),
        NonZeroU32::new(4).ok_or_else(|| eyre!("four must be non-zero"))?,
        u64::MAX,
        one_block_payload_bytes,
    );

    let segment_fetch =
        tokio::spawn(async move { source.fetch_chain_segment(segment_limits).await });
    tokio::task::yield_now().await;
    assert!(!segment_fetch.is_finished());

    tokio::time::advance(Duration::from_millis(249)).await;
    tokio::task::yield_now().await;
    assert!(!segment_fetch.is_finished());

    tokio::time::advance(Duration::from_millis(1)).await;
    let segment = segment_fetch.await??;
    assert_eq!(segment.stats().connected_blocks(), 4);
    assert_eq!(segment.stats().split_count(), 3);
    Ok(())
}

#[tokio::test]
async fn fixture_source_reports_zebra_json_hex_payload_bytes() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let source = FixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)?;
    let segment = source
        .fetch_chain_segment(SourceChainSegmentLimits::new(
            SourceChainCursor::before_height(BlockHeight::new(603)),
            NonZeroU32::MIN,
            u64::MAX,
            u64::MAX,
        ))
        .await?;

    assert_eq!(
        segment.stats().response_payload_bytes(),
        raw_block_hex_length(REGTEST_BLOCK_603)?
    );
    Ok(())
}

#[tokio::test]
async fn fixture_source_rejects_a_single_block_above_the_response_limit() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let source = FixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)?;
    let max_response_bytes = raw_block_hex_length(REGTEST_BLOCK_603)?.saturating_sub(1);

    let error = source
        .fetch_chain_segment(SourceChainSegmentLimits::new(
            SourceChainCursor::before_height(BlockHeight::new(603)),
            NonZeroU32::MIN,
            u64::MAX,
            max_response_bytes,
        ))
        .await
        .err()
        .ok_or_else(|| eyre!("a single oversized fixture block must fail closed"))?;

    assert!(matches!(
        error,
        SourceError::SourceResponseTooLarge {
            operation: "batch_getblock",
            max_response_bytes: actual_limit,
        } if actual_limit == max_response_bytes
    ));
    Ok(())
}

#[tokio::test]
async fn fixture_source_splits_oversized_multi_block_responses() -> Result<()> {
    let fixture = write_repeated_payload_fixture()?;
    let source = FixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)?;
    let one_block_payload_bytes = raw_block_hex_length(REGTEST_BLOCK_603)?;

    let segment = source
        .fetch_chain_segment(SourceChainSegmentLimits::new(
            SourceChainCursor::before_height(BlockHeight::new(603)),
            NonZeroU32::new(4).ok_or_else(|| eyre!("four must be non-zero"))?,
            u64::MAX,
            one_block_payload_bytes,
        ))
        .await?;

    assert_eq!(segment.stats().connected_blocks(), 4);
    assert_eq!(
        segment.stats().response_payload_bytes(),
        one_block_payload_bytes.saturating_mul(4)
    );
    assert_eq!(segment.stats().split_count(), 3);
    Ok(())
}

#[tokio::test]
async fn fixture_source_serves_complete_exact_subtree_root_ranges() -> Result<()> {
    let mut fixture = write_regtest_fixture()?;
    let completing_block = BlockId::new(fixture.block.height, fixture.block.hash);
    fixture.manifest.subtree_roots.sapling = (0..=5)
        .map(|index| subtree_root_record(index, 0x11, completing_block))
        .collect();
    let source = FixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)?;
    let range = SubtreeRootRange::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(4),
        NonZeroU32::new(2).ok_or_else(|| eyre!("two must be non-zero"))?,
    );

    let subtree_roots = source.fetch_subtree_root_range(range).await?;

    assert_eq!(subtree_roots.protocol, ShieldedProtocol::Sapling);
    assert_eq!(subtree_roots.start_index, SubtreeRootIndex::new(4));
    assert_eq!(subtree_roots.subtree_roots.len(), 2);
    assert_eq!(
        subtree_roots.subtree_roots[0].subtree_index,
        SubtreeRootIndex::new(4)
    );
    assert_eq!(
        subtree_roots.subtree_roots[1].subtree_index,
        SubtreeRootIndex::new(5)
    );
    Ok(())
}

#[tokio::test]
async fn fixture_source_rejects_short_missing_or_disconnected_exact_subtree_ranges() -> Result<()> {
    let mut fixture = write_regtest_fixture()?;
    let completing_block = BlockId::new(fixture.block.height, fixture.block.hash);
    fixture.manifest.subtree_roots.sapling = (0..=5)
        .map(|index| subtree_root_record(index, 0x11, completing_block))
        .collect();
    let source = FixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)?;
    let short_range = SubtreeRootRange::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(4),
        NonZeroU32::new(3).ok_or_else(|| eyre!("three must be non-zero"))?,
    );
    let missing_range = SubtreeRootRange::new(
        ShieldedProtocol::Sapling,
        SubtreeRootIndex::new(6),
        NonZeroU32::MIN,
    );

    assert!(matches!(
        source.fetch_subtree_root_range(short_range).await,
        Err(SourceError::SubtreeRootsUnavailable {
            protocol: ShieldedProtocol::Sapling,
            start_index,
            ..
        }) if start_index == SubtreeRootIndex::new(4)
    ));
    assert!(matches!(
        source.fetch_subtree_root_range(missing_range).await,
        Err(SourceError::SubtreeRootsUnavailable {
            protocol: ShieldedProtocol::Sapling,
            start_index,
            ..
        }) if start_index == SubtreeRootIndex::new(6)
    ));

    fixture.manifest.subtree_roots.sapling[5] = subtree_root_record(6, 0x33, completing_block);
    let error = FixtureNodeSource::open(fixture.directory.path(), &fixture.manifest)
        .err()
        .ok_or_else(|| eyre!("a non-contiguous captured subtree-root prefix must be rejected"))?;
    assert!(error.to_string().contains("has index 6, expected 5"));
    Ok(())
}

#[test]
fn fixture_v1_requires_digest_evidence() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let mut missing_digest_evidence = serde_json::to_value(&fixture.manifest)?;
    missing_digest_evidence
        .as_object_mut()
        .ok_or_else(|| eyre!("fixture manifest must encode as an object"))?
        .remove("canonical_block_facts_digest_evidence");
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&missing_digest_evidence)?,
    )?;
    let Err(error) = FixtureManifest::read(fixture.directory.path()) else {
        return Err(eyre!("fixture v3 digest evidence must be mandatory"));
    };
    assert!(
        error
            .to_string()
            .contains("canonical_block_facts_digest_evidence")
    );
    Ok(())
}

#[test]
fn fixture_v1_rejects_inconsistent_counts() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let mut inconsistent_digest_count = fixture.manifest.clone();
    inconsistent_digest_count
        .canonical_block_facts_digest_evidence
        .block_count = 0;
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&inconsistent_digest_count)?,
    )?;
    let Err(error) = FixtureManifest::read(fixture.directory.path()) else {
        return Err(eyre!(
            "canonical fact digest count must match the fixture range"
        ));
    };
    assert!(error.to_string().contains("digest count"));

    let mut inconsistent = fixture.manifest;
    inconsistent.workload_density.block_count = 0;
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&inconsistent)?,
    )?;
    let Err(error) = FixtureManifest::read(fixture.directory.path()) else {
        return Err(eyre!("density block count must match the fixture range"));
    };
    assert!(error.to_string().contains("density block count"));
    Ok(())
}

#[test]
fn fixture_v1_rejects_unknown_digest_version() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let mut unknown_digest_version = fixture.manifest;
    unknown_digest_version
        .canonical_block_facts_digest_evidence
        .block_digest_version = u16::MAX;
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&unknown_digest_version)?,
    )?;
    let Err(error) = FixtureManifest::read(fixture.directory.path()) else {
        return Err(eyre!(
            "unknown canonical fact digest version must fail closed"
        ));
    };
    assert!(
        error
            .to_string()
            .contains("unsupported canonical block facts digest version")
    );

    Ok(())
}

#[test]
fn fixture_v1_rejects_uppercase_sequence_digest_hex() -> Result<()> {
    let fixture = write_regtest_fixture()?;
    let mut uppercase_sequence_digest = fixture.manifest;
    uppercase_sequence_digest
        .canonical_block_facts_digest_evidence
        .sequence_digest_sha256 = "AB".repeat(32);
    std::fs::write(
        fixture.directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&uppercase_sequence_digest)?,
    )?;

    let Err(error) = FixtureManifest::read(fixture.directory.path()) else {
        return Err(eyre!(
            "uppercase canonical fact sequence digest must be rejected"
        ));
    };
    assert!(error.to_string().contains("lowercase hexadecimal"));
    Ok(())
}

#[test]
fn checkpoint_policy_comparison_measures_off_cadence_checkpoints() -> Result<()> {
    let fixture = zinder_testkit::manifest_dir!().join("fixtures/checkpoint-policy");
    let scratch = tempdir()?;
    let output = Command::new(zinder_testkit::bin_exe!("zinder-bench"))
        .env("TOKIO_WORKER_THREADS", "2")
        .arg("rocksdb-checkpoint-policy-comparison")
        .arg("--fixture")
        .arg(fixture)
        .arg("--scratch-directory")
        .arg(scratch.path())
        .output()?;
    assert!(
        output.status.success(),
        "comparison failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["byte_equivalent"], true);
    assert_eq!(
        report["sampled_heights"],
        serde_json::json!([603, 604, 605, 606])
    );
    let arms = report["arms"]
        .as_array()
        .ok_or_else(|| eyre!("comparison arms missing"))?;
    assert_eq!(arms.len(), 4);
    assert_eq!(arms[0]["coverage"], "cadence-and-tip");
    assert_eq!(arms[1]["commitment_budget"], 1);
    assert_eq!(arms[2]["commitment_budget"], 3);
    assert_eq!(arms[3]["coverage"], "every-block");
    for (arm, (rows, misses, max_blocks, max_commitments)) in
        arms.iter()
            .zip([(2, 3, 3, 2), (3, 2, 2, 0), (2, 3, 3, 2), (5, 0, 0, 0)])
    {
        assert_eq!(arm["checkpoint_rows"], rows);
        assert_eq!(arm["miss_samples"], misses);
        assert_eq!(arm["max_replayed_blocks"], max_blocks);
        assert_eq!(arm["max_replayed_commitments"], max_commitments);
        assert_eq!(arm["miss_latency_seconds"].is_null(), misses == 0);
        for bytes in ["checkpoint_logical_bytes", "checkpoint_sst_bytes"] {
            assert!(arm[bytes].as_u64().is_some_and(|count| count > 0));
        }
        for phase in ["ready_seconds", "reopen_seconds"] {
            assert!(arm[phase].as_f64().is_some_and(|seconds| seconds > 0.0));
        }
    }
    assert_eq!(std::fs::read_dir(scratch.path())?.count(), 0);
    Ok(())
}
