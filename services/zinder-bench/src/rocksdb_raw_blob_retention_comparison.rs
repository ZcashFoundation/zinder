//! Pairwise raw-blob retention cost measurement over one authenticated fixture.

use std::{
    fs,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::Args;
use serde::Serialize;
use zinder_bench::{
    BenchError,
    canonical_fixture_replay::{
        CanonicalFixtureRocksDbReplayConfig, CanonicalFixtureRocksDbReplayOutcome,
        replay_canonical_fixture_into_rocksdb,
    },
    fixture::FixtureManifest,
    report::{
        CANONICAL_FIXTURE_REPLAY_PROFILE_CPU_CORES, CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES,
    },
};
use zinder_core::{BlockId, ChainTipMetadata};
use zinder_ingest::CanonicalPipelineLimits;
use zinder_store::{
    CanonicalEventFence, CanonicalReorgPolicy, CanonicalStoreReadyEvidence, CanonicalStoreWorkload,
    CanonicalSubtreeRootLoadCoverage, RawBlobRetention, RocksDbCanonicalSecondary,
    RocksDbResourceBudget,
};

const REPORT_CONTRACT_IDENTITY: &str = "rocksdb-raw-blob-retention-comparison";
const REPORT_FORMAT_VERSION: u16 = 2;
const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_SUPPORTED_REORG_DEPTH: u32 = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS;

/// CLI contract for one authenticated `transactions` versus `all` comparison.
#[derive(Args)]
pub(crate) struct RocksDbRawBlobRetentionComparisonArgs {
    /// Directory containing the fixture manifest, segments, and replay plan.
    #[arg(long)]
    fixture: PathBuf,
    /// Fresh canonical destination for the `transactions` arm.
    #[arg(long = "transactions-canonical-store")]
    transactions_canonical_store: PathBuf,
    /// Fresh local secondary metadata destination for the `transactions` arm.
    #[arg(long = "transactions-secondary-root")]
    transactions_secondary_root: PathBuf,
    /// Fresh canonical destination for the `all` arm.
    #[arg(long = "all-canonical-store")]
    all_canonical_store: PathBuf,
    /// Fresh local secondary metadata destination for the `all` arm.
    #[arg(long = "all-secondary-root")]
    all_secondary_root: PathBuf,
    /// Per-operation source timeout in seconds.
    #[arg(long = "request-timeout-secs", default_value_t = DEFAULT_REQUEST_TIMEOUT_SECONDS)]
    request_timeout_seconds: u64,
    /// Retained shallow-reorg depth used for baseline settlement.
    #[arg(long, default_value_t = DEFAULT_SUPPORTED_REORG_DEPTH)]
    supported_reorg_depth: u32,
    /// Deterministic delay applied to every outer fixture segment response.
    #[arg(long = "source-segment-delay-millis", default_value_t = 0)]
    source_segment_delay_millis: u64,
    /// Maximum accepted source response body.
    #[arg(long)]
    max_response_bytes: Option<u64>,
    /// Adaptive target for one source response.
    #[arg(long = "source-segment-target-response-bytes")]
    source_segment_target_response_bytes: Option<u64>,
    /// Maximum blocks requested in one source segment.
    #[arg(long = "source-segment-max-blocks")]
    source_segment_max_blocks: Option<u32>,
    /// Maximum concurrent source segment requests.
    #[arg(long = "source-fetch-max-in-flight-requests")]
    source_fetch_max_in_flight_requests: Option<u32>,
    /// Aggregate in-flight source response watermark.
    #[arg(long = "source-fetch-max-in-flight-bytes")]
    source_fetch_max_in_flight_bytes: Option<u64>,
    /// Maximum canonical block preparations in flight.
    #[arg(long = "block-prepare-concurrency")]
    block_prepare_concurrency: Option<u32>,
    /// Aggregate canonical preparation memory watermark.
    #[arg(long = "block-prepare-memory-watermark-bytes")]
    block_prepare_memory_watermark_bytes: Option<u64>,
    /// Write the JSON report to this fresh path instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
}

/// Report and optional output path produced by the pairwise comparison.
pub(crate) struct RocksDbRawBlobRetentionComparisonOutput {
    pub(crate) report: RocksDbRawBlobRetentionComparisonReport,
    pub(crate) report_path: Option<PathBuf>,
}

/// Closed report for one pairwise raw-blob retention measurement.
#[derive(Serialize)]
pub(crate) struct RocksDbRawBlobRetentionComparisonReport {
    contract_identity: &'static str,
    report_format_version: u16,
    arm_execution_order: [RawBlobRetention; 2],
    transactions: RawBlobRetentionArmReport,
    all: RawBlobRetentionArmReport,
}

#[derive(Serialize)]
struct RawBlobRetentionArmReport {
    raw_blob_retention: RawBlobRetention,
    fixture_manifest_digest_sha256: String,
    replay_plan_digest_sha256: String,
    canonical_store_path: String,
    secondary_root_path: String,
    effective_limits: EffectiveLimits,
    logical_replay_identity: LogicalReplayIdentity,
    raw_blob_counts: RawBlobCounts,
    ready_identity: ReadyIdentity,
    physical_canonical_bytes: u64,
    authenticated_replay_lifecycle_seconds: f64,
    authenticated_replay_lifecycle_blocks_per_second: Option<f64>,
    secondary_open_ready_and_initial_catch_up_seconds: f64,
    secondary_ready_matches_reopened_primary: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct EffectiveLimits {
    workload: &'static str,
    request_timeout_seconds: u64,
    supported_reorg_depth: u32,
    source_segment_delay_millis: u64,
    derived_for_cpu_limit_cores: u32,
    derived_for_memory_limit_bytes: u64,
    pipeline: PipelineLimitSummary,
    canonical_writer_rocksdb: RocksDbResourceBudgetSummary,
    canonical_reader_rocksdb: RocksDbResourceBudgetSummary,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct PipelineLimitSummary {
    max_response_bytes: u64,
    source_segment_target_response_bytes: u64,
    source_segment_max_blocks: u32,
    source_fetch_max_in_flight_requests: u32,
    source_fetch_max_in_flight_bytes: u64,
    block_prepare_concurrency: u32,
    block_prepare_memory_watermark_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct RocksDbResourceBudgetSummary {
    block_cache_bytes: u64,
    max_wal_bytes: u64,
    max_open_files: i32,
    write_buffer_bytes: u64,
    max_write_buffer_count: i32,
    max_background_jobs: i32,
    memtable_budget_bytes: u64,
    statistics_level: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct LogicalReplayIdentity {
    first_retained_block: BlockIdSummary,
    first_parent_hash_hex: String,
    visible_tip: BlockIdSummary,
    visible_tip_tree_sizes: ChainTipTreeSizes,
    block_count: u64,
    transaction_count: u64,
    block_header_count: u64,
    block_hash_index_count: u64,
    block_replay_count: u64,
    compact_block_count: u64,
    transaction_location_count: u64,
    tree_state_checkpoint_count: u64,
    block_final_note_commitment_roots_count: u64,
    block_header_logical_bytes: u64,
    block_hash_index_logical_bytes: u64,
    block_replay_logical_bytes: u64,
    compact_block_logical_bytes: u64,
    transaction_location_logical_bytes: u64,
    tree_state_checkpoint_logical_bytes: u64,
    block_final_note_commitment_roots_logical_bytes: u64,
    replay_format_version: u32,
    block_digest_version: u16,
    sequence_digest_version: u16,
    sequence_digest_sha256: String,
    subtree_root_count: u64,
    subtree_root_logical_bytes: u64,
    subtree_root_coverage: CanonicalSubtreeRootLoadCoverage,
    subtree_root_sequence_digest_version: u16,
    subtree_root_sequence_digest_sha256: String,
    source_tip_checkpoint_authenticated: bool,
    settled_tip: BlockIdSummary,
    event_fence: EventFenceSummary,
    full_scan_block_count: u64,
    ready: ReadyLogicalIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct RawBlobCounts {
    transaction_blob_count: u64,
    block_blob_count: u64,
    transaction_blob_logical_bytes: u64,
    block_blob_logical_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ReadyIdentity {
    logical: ReadyLogicalIdentity,
    construction_manifest_version: u16,
    construction_manifest_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct ReadyLogicalIdentity {
    first_retained_block: BlockIdSummary,
    visible_tip: BlockIdSummary,
    visible_epoch_id: u64,
    visible_event_sequence: u64,
    visible_block_count: u64,
    block_digest_version: u16,
    replay_format_version: u32,
    sequence_digest_version: u16,
    visible_sequence_digest_sha256: String,
    visible_logical_replay_bytes: u64,
    sequence_checkpoint: SequenceCheckpointSummary,
    construction_manifest_version: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SequenceCheckpointSummary {
    through: BlockIdSummary,
    retained_block_count: u64,
    sequence_digest_version: u16,
    sequence_digest_block_count: u64,
    sequence_digest_sha256: String,
    logical_replay_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct EventFenceSummary {
    chain_epoch_id: u64,
    chain_event_sequence: u64,
    visible_tip: BlockIdSummary,
    sequence_digest_version: u16,
    sequence_digest_block_count: u64,
    sequence_digest_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct BlockIdSummary {
    height: u32,
    hash_hex: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
struct ChainTipTreeSizes {
    sapling: u32,
    orchard: u32,
    ironwood: u32,
}

struct ValidatedComparisonArgs {
    fixture: PathBuf,
    transactions_canonical_store: PathBuf,
    transactions_secondary_root: PathBuf,
    all_canonical_store: PathBuf,
    all_secondary_root: PathBuf,
    request_timeout: Duration,
    request_timeout_seconds: u64,
    pipeline_limits: CanonicalPipelineLimits,
    writer_resource_budget: RocksDbResourceBudget,
    reader_resource_budget: RocksDbResourceBudget,
    supported_reorg_depth: u32,
    source_segment_delay: Duration,
    source_segment_delay_millis: u64,
    report: Option<PathBuf>,
}

struct FreshComparisonPaths {
    fixture: PathBuf,
    transactions_canonical_store: PathBuf,
    transactions_secondary_root: PathBuf,
    all_canonical_store: PathBuf,
    all_secondary_root: PathBuf,
    report: Option<PathBuf>,
}

struct ValidatedComparisonLimits {
    request_timeout_seconds: NonZeroU64,
    pipeline_limits: CanonicalPipelineLimits,
    writer_resource_budget: RocksDbResourceBudget,
    reader_resource_budget: RocksDbResourceBudget,
    supported_reorg_depth: NonZeroU32,
}

/// Replays both retention arms and fails closed unless their logical evidence matches.
pub(crate) async fn run_rocksdb_raw_blob_retention_comparison(
    args: RocksDbRawBlobRetentionComparisonArgs,
) -> Result<RocksDbRawBlobRetentionComparisonOutput, BenchError> {
    let validated = args.validate()?;
    let manifest = FixtureManifest::read(&validated.fixture)?;
    let activations = manifest.activations_typed()?;
    let transactions = run_retention_arm(
        &validated,
        &activations,
        RawBlobRetention::Transactions,
        &validated.transactions_canonical_store,
        &validated.transactions_secondary_root,
    )
    .await?;
    let all = run_retention_arm(
        &validated,
        &activations,
        RawBlobRetention::All,
        &validated.all_canonical_store,
        &validated.all_secondary_root,
    )
    .await?;
    require_comparable_arms(&transactions, &all)?;

    Ok(RocksDbRawBlobRetentionComparisonOutput {
        report: RocksDbRawBlobRetentionComparisonReport {
            contract_identity: REPORT_CONTRACT_IDENTITY,
            report_format_version: REPORT_FORMAT_VERSION,
            arm_execution_order: [RawBlobRetention::Transactions, RawBlobRetention::All],
            transactions,
            all,
        },
        report_path: validated.report,
    })
}

async fn run_retention_arm(
    validated: &ValidatedComparisonArgs,
    activations: &zinder_core::NetworkUpgradeActivations,
    raw_blob_retention: RawBlobRetention,
    canonical_store_path: &Path,
    secondary_root_path: &Path,
) -> Result<RawBlobRetentionArmReport, BenchError> {
    let replay_started = Instant::now();
    let outcome = replay_canonical_fixture_into_rocksdb(CanonicalFixtureRocksDbReplayConfig {
        fixture_directory: validated.fixture.clone(),
        canonical_store_path: canonical_store_path.to_path_buf(),
        request_timeout: validated.request_timeout,
        pipeline_limits: validated.pipeline_limits,
        resource_budget: validated.writer_resource_budget,
        raw_blob_retention,
        supported_reorg_depth: validated.supported_reorg_depth,
        source_segment_delay: validated.source_segment_delay,
    })
    .await?;
    let authenticated_replay_lifecycle_seconds = replay_started.elapsed().as_secs_f64();
    require_arm_replay_evidence(raw_blob_retention, &outcome)?;
    let physical_canonical_bytes = directory_bytes(canonical_store_path)?;

    let secondary_started = Instant::now();
    let secondary = RocksDbCanonicalSecondary::open_ready(
        canonical_store_path,
        secondary_root_path,
        activations,
        CanonicalStoreWorkload::Wallet,
        raw_blob_retention,
        CanonicalReorgPolicy::new(validated.supported_reorg_depth)?,
        validated.reader_resource_budget,
    )?;
    let secondary_open_ready_and_initial_catch_up_seconds =
        secondary_started.elapsed().as_secs_f64();
    if secondary.ready_evidence() != outcome.reopened_ready_evidence {
        return Err(BenchError::report_format(format!(
            "{raw_blob_retention} secondary READY identity differs from the reopened primary"
        )));
    }

    Ok(RawBlobRetentionArmReport {
        raw_blob_retention,
        fixture_manifest_digest_sha256: outcome.fixture_manifest_digest_sha256.clone(),
        replay_plan_digest_sha256: outcome.replay_plan_digest_sha256.clone(),
        canonical_store_path: canonical_store_path.to_string_lossy().into_owned(),
        secondary_root_path: secondary_root_path.to_string_lossy().into_owned(),
        effective_limits: EffectiveLimits::from_validated(validated),
        logical_replay_identity: LogicalReplayIdentity::from_outcome(&outcome),
        raw_blob_counts: RawBlobCounts::from_outcome(&outcome),
        ready_identity: ReadyIdentity::from(outcome.reopened_ready_evidence),
        physical_canonical_bytes,
        authenticated_replay_lifecycle_seconds,
        authenticated_replay_lifecycle_blocks_per_second: blocks_per_second(
            outcome.block_load_evidence.block_count,
            authenticated_replay_lifecycle_seconds,
        )?,
        secondary_open_ready_and_initial_catch_up_seconds,
        secondary_ready_matches_reopened_primary: true,
    })
}

impl RocksDbRawBlobRetentionComparisonArgs {
    fn validate(&self) -> Result<ValidatedComparisonArgs, BenchError> {
        let paths = self.validate_paths()?;
        let limits = self.validate_limits()?;
        Ok(ValidatedComparisonArgs {
            fixture: paths.fixture,
            transactions_canonical_store: paths.transactions_canonical_store,
            transactions_secondary_root: paths.transactions_secondary_root,
            all_canonical_store: paths.all_canonical_store,
            all_secondary_root: paths.all_secondary_root,
            request_timeout: Duration::from_secs(limits.request_timeout_seconds.get()),
            request_timeout_seconds: limits.request_timeout_seconds.get(),
            pipeline_limits: limits.pipeline_limits,
            writer_resource_budget: limits.writer_resource_budget,
            reader_resource_budget: limits.reader_resource_budget,
            supported_reorg_depth: limits.supported_reorg_depth.get(),
            source_segment_delay: Duration::from_millis(self.source_segment_delay_millis),
            source_segment_delay_millis: self.source_segment_delay_millis,
            report: paths.report,
        })
    }

    fn validate_paths(&self) -> Result<FreshComparisonPaths, BenchError> {
        let fixture = fs::canonicalize(&self.fixture)
            .map_err(|source| BenchError::io(&self.fixture, source))?;
        let transactions_canonical_store = resolve_fresh_path(
            "--transactions-canonical-store",
            &self.transactions_canonical_store,
        )?;
        let transactions_secondary_root = resolve_fresh_path(
            "--transactions-secondary-root",
            &self.transactions_secondary_root,
        )?;
        let all_canonical_store =
            resolve_fresh_path("--all-canonical-store", &self.all_canonical_store)?;
        let all_secondary_root =
            resolve_fresh_path("--all-secondary-root", &self.all_secondary_root)?;
        let mut scoped_paths = vec![
            ("--fixture", fixture.as_path()),
            (
                "--transactions-canonical-store",
                transactions_canonical_store.as_path(),
            ),
            (
                "--transactions-secondary-root",
                transactions_secondary_root.as_path(),
            ),
            ("--all-canonical-store", all_canonical_store.as_path()),
            ("--all-secondary-root", all_secondary_root.as_path()),
        ];
        let report = self
            .report
            .as_deref()
            .map(|path| resolve_fresh_path("--report", path))
            .transpose()?;
        if let Some(report) = report.as_deref() {
            scoped_paths.push(("--report", report));
        }
        require_pairwise_disjoint(&scoped_paths)?;
        Ok(FreshComparisonPaths {
            fixture,
            transactions_canonical_store,
            transactions_secondary_root,
            all_canonical_store,
            all_secondary_root,
            report,
        })
    }

    fn validate_limits(&self) -> Result<ValidatedComparisonLimits, BenchError> {
        let request_timeout_seconds =
            require_nonzero_u64(self.request_timeout_seconds, "request-timeout-secs")?;
        let supported_reorg_depth =
            require_nonzero_u32(self.supported_reorg_depth, "supported-reorg-depth")?;
        let max_response_bytes = self
            .max_response_bytes
            .map(|bytes| require_nonzero_u64(bytes, "max-response-bytes"))
            .transpose()?
            .unwrap_or(require_nonzero_u64(
                DEFAULT_MAX_RESPONSE_BYTES,
                "max-response-bytes",
            )?);
        let mut pipeline_limits = CanonicalPipelineLimits::resolve(
            NonZeroU64::new(CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES),
            NonZeroU32::new(CANONICAL_FIXTURE_REPLAY_PROFILE_CPU_CORES).unwrap_or(NonZeroU32::MIN),
            max_response_bytes,
        );
        self.apply_pipeline_overrides(&mut pipeline_limits)?;
        let pipeline_limits = pipeline_limits.validate().map_err(|source| {
            BenchError::invalid_argument(format!(
                "raw-blob retention comparison pipeline limits are invalid: {source}"
            ))
        })?;
        let writer_resource_budget = RocksDbResourceBudget::canonical_writer_defaults();
        writer_resource_budget.validate().map_err(|reason| {
            BenchError::invalid_argument(format!(
                "raw-blob retention comparison writer resource budget is invalid: {reason}"
            ))
        })?;
        let reader_resource_budget = RocksDbResourceBudget::canonical_reader_defaults();
        reader_resource_budget.validate().map_err(|reason| {
            BenchError::invalid_argument(format!(
                "raw-blob retention comparison reader resource budget is invalid: {reason}"
            ))
        })?;
        Ok(ValidatedComparisonLimits {
            request_timeout_seconds,
            pipeline_limits,
            writer_resource_budget,
            reader_resource_budget,
            supported_reorg_depth,
        })
    }

    fn apply_pipeline_overrides(
        &self,
        limits: &mut CanonicalPipelineLimits,
    ) -> Result<(), BenchError> {
        if let Some(bytes) = self.source_segment_target_response_bytes {
            limits.source_segment_target_response_bytes =
                require_nonzero_u64(bytes, "source-segment-target-response-bytes")?;
        }
        if let Some(blocks) = self.source_segment_max_blocks {
            limits.source_segment_max_blocks =
                require_nonzero_u32(blocks, "source-segment-max-blocks")?;
        }
        if let Some(requests) = self.source_fetch_max_in_flight_requests {
            limits.source_fetch_max_in_flight_requests =
                require_nonzero_u32(requests, "source-fetch-max-in-flight-requests")?;
        }
        if let Some(bytes) = self.source_fetch_max_in_flight_bytes {
            limits.source_fetch_max_in_flight_bytes =
                require_nonzero_u64(bytes, "source-fetch-max-in-flight-bytes")?;
        }
        if let Some(concurrency) = self.block_prepare_concurrency {
            limits.block_prepare_concurrency =
                require_nonzero_u32(concurrency, "block-prepare-concurrency")?;
        }
        if let Some(bytes) = self.block_prepare_memory_watermark_bytes {
            limits.block_prepare_memory_watermark_bytes =
                require_nonzero_u64(bytes, "block-prepare-memory-watermark-bytes")?;
        }
        Ok(())
    }
}

impl EffectiveLimits {
    fn from_validated(validated: &ValidatedComparisonArgs) -> Self {
        Self {
            workload: CanonicalStoreWorkload::Wallet.as_str(),
            request_timeout_seconds: validated.request_timeout_seconds,
            supported_reorg_depth: validated.supported_reorg_depth,
            source_segment_delay_millis: validated.source_segment_delay_millis,
            derived_for_cpu_limit_cores: CANONICAL_FIXTURE_REPLAY_PROFILE_CPU_CORES,
            derived_for_memory_limit_bytes: CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES,
            pipeline: PipelineLimitSummary::from(validated.pipeline_limits),
            canonical_writer_rocksdb: RocksDbResourceBudgetSummary::from(
                validated.writer_resource_budget,
            ),
            canonical_reader_rocksdb: RocksDbResourceBudgetSummary::from(
                validated.reader_resource_budget,
            ),
        }
    }
}

impl From<CanonicalPipelineLimits> for PipelineLimitSummary {
    fn from(limits: CanonicalPipelineLimits) -> Self {
        Self {
            max_response_bytes: limits.max_response_bytes.get(),
            source_segment_target_response_bytes: limits.source_segment_target_response_bytes.get(),
            source_segment_max_blocks: limits.source_segment_max_blocks.get(),
            source_fetch_max_in_flight_requests: limits.source_fetch_max_in_flight_requests.get(),
            source_fetch_max_in_flight_bytes: limits.source_fetch_max_in_flight_bytes.get(),
            block_prepare_concurrency: limits.block_prepare_concurrency.get(),
            block_prepare_memory_watermark_bytes: limits.block_prepare_memory_watermark_bytes.get(),
        }
    }
}

impl From<RocksDbResourceBudget> for RocksDbResourceBudgetSummary {
    fn from(budget: RocksDbResourceBudget) -> Self {
        Self {
            block_cache_bytes: budget.block_cache_bytes,
            max_wal_bytes: budget.max_wal_bytes,
            max_open_files: budget.max_open_files,
            write_buffer_bytes: budget.write_buffer_bytes,
            max_write_buffer_count: budget.max_write_buffer_count,
            max_background_jobs: budget.max_background_jobs,
            memtable_budget_bytes: budget.memtable_budget_bytes,
            statistics_level: budget.statistics_level.as_str(),
        }
    }
}

impl LogicalReplayIdentity {
    fn from_outcome(outcome: &CanonicalFixtureRocksDbReplayOutcome) -> Self {
        let load = outcome.block_load_evidence;
        let subtree = outcome.subtree_root_load_evidence;
        Self {
            first_retained_block: BlockIdSummary::from(BlockId::new(
                load.first_height,
                load.first_hash,
            )),
            first_parent_hash_hex: hex::encode(load.first_parent_hash.as_bytes()),
            visible_tip: BlockIdSummary::from(BlockId::new(load.tip_height, load.tip_hash)),
            visible_tip_tree_sizes: ChainTipTreeSizes::from(load.tip_metadata),
            block_count: load.block_count,
            transaction_count: load.transaction_count,
            block_header_count: load.block_header_count,
            block_hash_index_count: load.block_hash_index_count,
            block_replay_count: load.block_replay_count,
            compact_block_count: load.compact_block_count,
            transaction_location_count: load.transaction_location_count,
            tree_state_checkpoint_count: load.tree_state_checkpoint_count,
            block_final_note_commitment_roots_count: load.block_final_note_commitment_roots_count,
            block_header_logical_bytes: load.block_header_logical_bytes,
            block_hash_index_logical_bytes: load.block_hash_index_logical_bytes,
            block_replay_logical_bytes: load.block_replay_logical_bytes,
            compact_block_logical_bytes: load.compact_block_logical_bytes,
            transaction_location_logical_bytes: load.transaction_location_logical_bytes,
            tree_state_checkpoint_logical_bytes: load.tree_state_checkpoint_logical_bytes,
            block_final_note_commitment_roots_logical_bytes: load
                .block_final_note_commitment_roots_logical_bytes,
            replay_format_version: load.replay_format_version.value(),
            block_digest_version: load.block_digest_version.value(),
            sequence_digest_version: load.sequence_digest_version.value(),
            sequence_digest_sha256: hex::encode(load.sequence_digest.as_bytes()),
            subtree_root_count: subtree.subtree_root_count,
            subtree_root_logical_bytes: subtree.subtree_root_logical_bytes,
            subtree_root_coverage: subtree.coverage,
            subtree_root_sequence_digest_version: subtree.sequence_digest_version,
            subtree_root_sequence_digest_sha256: hex::encode(subtree.subtree_root_sequence_digest),
            source_tip_checkpoint_authenticated: outcome.source_tip_checkpoint_authenticated,
            settled_tip: BlockIdSummary::from(outcome.settled_tip),
            event_fence: EventFenceSummary::from(outcome.event_fence),
            full_scan_block_count: outcome.replayed_block_count,
            ready: ReadyLogicalIdentity::from(outcome.reopened_ready_evidence),
        }
    }
}

impl RawBlobCounts {
    fn from_outcome(outcome: &CanonicalFixtureRocksDbReplayOutcome) -> Self {
        let load = outcome.block_load_evidence;
        Self {
            transaction_blob_count: load.transaction_blob_count,
            block_blob_count: load.block_blob_count,
            transaction_blob_logical_bytes: load.transaction_blob_logical_bytes,
            block_blob_logical_bytes: load.block_blob_logical_bytes,
        }
    }
}

impl From<CanonicalStoreReadyEvidence> for ReadyIdentity {
    fn from(ready: CanonicalStoreReadyEvidence) -> Self {
        Self {
            logical: ReadyLogicalIdentity::from(ready),
            construction_manifest_version: ready.construction_manifest_version,
            construction_manifest_sha256: hex::encode(ready.construction_manifest_sha256),
        }
    }
}

impl From<CanonicalStoreReadyEvidence> for ReadyLogicalIdentity {
    fn from(ready: CanonicalStoreReadyEvidence) -> Self {
        Self {
            first_retained_block: BlockIdSummary::from(ready.first_retained_block),
            visible_tip: BlockIdSummary::from(ready.visible_tip),
            visible_epoch_id: ready.visible_epoch.value(),
            visible_event_sequence: ready.visible_event_sequence,
            visible_block_count: ready.visible_block_count,
            block_digest_version: ready.block_digest_version.value(),
            replay_format_version: ready.replay_format_version.value(),
            sequence_digest_version: ready.sequence_digest_version.value(),
            visible_sequence_digest_sha256: hex::encode(ready.visible_sequence_digest),
            visible_logical_replay_bytes: ready.visible_logical_replay_bytes,
            sequence_checkpoint: SequenceCheckpointSummary::from(ready.sequence_checkpoint),
            construction_manifest_version: ready.construction_manifest_version,
        }
    }
}

impl From<zinder_store::CanonicalSequenceCheckpoint> for SequenceCheckpointSummary {
    fn from(checkpoint: zinder_store::CanonicalSequenceCheckpoint) -> Self {
        let digest = checkpoint.sequence_digest();
        Self {
            through: BlockIdSummary::from(checkpoint.through()),
            retained_block_count: checkpoint.retained_block_count(),
            sequence_digest_version: digest.version().value(),
            sequence_digest_block_count: digest.block_count(),
            sequence_digest_sha256: hex::encode(digest.as_bytes()),
            logical_replay_bytes: checkpoint.logical_replay_bytes(),
        }
    }
}

impl From<CanonicalEventFence> for EventFenceSummary {
    fn from(fence: CanonicalEventFence) -> Self {
        let digest = fence.sequence_digest();
        Self {
            chain_epoch_id: fence.chain_epoch_id().value(),
            chain_event_sequence: fence.chain_event_sequence(),
            visible_tip: BlockIdSummary::from(fence.visible_tip()),
            sequence_digest_version: digest.version().value(),
            sequence_digest_block_count: digest.block_count(),
            sequence_digest_sha256: hex::encode(digest.as_bytes()),
        }
    }
}

impl From<BlockId> for BlockIdSummary {
    fn from(block: BlockId) -> Self {
        Self {
            height: block.height.value(),
            hash_hex: hex::encode(block.hash.as_bytes()),
        }
    }
}

impl From<ChainTipMetadata> for ChainTipTreeSizes {
    fn from(metadata: ChainTipMetadata) -> Self {
        Self {
            sapling: metadata.sapling_commitment_tree_size,
            orchard: metadata.orchard_commitment_tree_size,
            ironwood: metadata.ironwood_commitment_tree_size,
        }
    }
}

fn require_arm_replay_evidence(
    raw_blob_retention: RawBlobRetention,
    outcome: &CanonicalFixtureRocksDbReplayOutcome,
) -> Result<(), BenchError> {
    if outcome.published_ready_evidence != outcome.reopened_ready_evidence {
        return Err(BenchError::report_format(format!(
            "{raw_blob_retention} publication READY identity differs from its cold reopen"
        )));
    }
    if !outcome.source_tip_checkpoint_authenticated {
        return Err(BenchError::report_format(format!(
            "{raw_blob_retention} replay did not authenticate the fixture tip checkpoint"
        )));
    }
    if outcome.replayed_block_count != outcome.block_load_evidence.block_count {
        return Err(BenchError::report_format(format!(
            "{raw_blob_retention} full-scan block count differs from its loaded block count"
        )));
    }
    require_retention_blob_counts(
        raw_blob_retention,
        outcome.block_load_evidence.block_count,
        outcome.block_load_evidence.transaction_count,
        outcome.block_load_evidence.transaction_blob_count,
        outcome.block_load_evidence.block_blob_count,
    )
}

fn require_retention_blob_counts(
    raw_blob_retention: RawBlobRetention,
    block_count: u64,
    transaction_count: u64,
    transaction_blob_count: u64,
    block_blob_count: u64,
) -> Result<(), BenchError> {
    if transaction_blob_count != transaction_count {
        return Err(BenchError::report_format(format!(
            "{raw_blob_retention} retained {transaction_blob_count} transaction blobs for {transaction_count} transactions"
        )));
    }
    let expected_block_blob_count = match raw_blob_retention {
        RawBlobRetention::Transactions => 0,
        RawBlobRetention::All => block_count,
        RawBlobRetention::None => {
            return Err(BenchError::report_format(
                "raw-blob retention comparison does not admit a none arm",
            ));
        }
    };
    if block_blob_count != expected_block_blob_count {
        return Err(BenchError::report_format(format!(
            "{raw_blob_retention} retained {block_blob_count} block blobs; expected {expected_block_blob_count}"
        )));
    }
    Ok(())
}

fn require_comparable_arms(
    transactions: &RawBlobRetentionArmReport,
    all: &RawBlobRetentionArmReport,
) -> Result<(), BenchError> {
    if transactions.raw_blob_retention != RawBlobRetention::Transactions
        || all.raw_blob_retention != RawBlobRetention::All
    {
        return Err(BenchError::report_format(
            "raw-blob retention comparison arms have unexpected retention identities",
        ));
    }
    if transactions.fixture_manifest_digest_sha256 != all.fixture_manifest_digest_sha256 {
        return Err(BenchError::report_format(
            "raw-blob retention arms admitted different fixture manifest identities",
        ));
    }
    if transactions.replay_plan_digest_sha256 != all.replay_plan_digest_sha256 {
        return Err(BenchError::report_format(
            "raw-blob retention arms admitted different replay plan identities",
        ));
    }
    if transactions.effective_limits != all.effective_limits {
        return Err(BenchError::report_format(
            "raw-blob retention arms used different effective limits",
        ));
    }
    if transactions.logical_replay_identity != all.logical_replay_identity {
        return Err(BenchError::report_format(
            "raw-blob retention arms produced different logical replay identities",
        ));
    }
    if transactions.raw_blob_counts.transaction_blob_count
        != all.raw_blob_counts.transaction_blob_count
        || transactions.raw_blob_counts.transaction_blob_logical_bytes
            != all.raw_blob_counts.transaction_blob_logical_bytes
    {
        return Err(BenchError::report_format(
            "raw-blob retention arms produced different transaction-blob evidence",
        ));
    }
    Ok(())
}

fn resolve_fresh_path(flag: &str, candidate: &Path) -> Result<PathBuf, BenchError> {
    let file_name = candidate.file_name().ok_or_else(|| {
        BenchError::invalid_argument(format!("{flag} must name a path below an existing parent"))
    })?;
    let parent = candidate
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let resolved_parent =
        fs::canonicalize(parent).map_err(|source| BenchError::io(parent, source))?;
    let resolved = resolved_parent.join(file_name);
    match fs::symlink_metadata(&resolved) {
        Ok(_) => Err(BenchError::invalid_argument(format!(
            "{flag} must name a fresh path"
        ))),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(resolved),
        Err(source) => Err(BenchError::io(&resolved, source)),
    }
}

fn require_pairwise_disjoint(paths: &[(&str, &Path)]) -> Result<(), BenchError> {
    for (index, (left_name, left)) in paths.iter().enumerate() {
        for (right_name, right) in &paths[index + 1..] {
            if left == right || left.starts_with(right) || right.starts_with(left) {
                return Err(BenchError::invalid_argument(format!(
                    "{left_name} and {right_name} must be disjoint paths"
                )));
            }
        }
    }
    Ok(())
}

fn directory_bytes(path: &Path) -> Result<u64, BenchError> {
    let entries = fs::read_dir(path).map_err(|source| BenchError::io(path, source))?;
    let mut bytes = 0_u64;
    for entry in entries {
        let entry = entry.map_err(|source| BenchError::io(path, source))?;
        let entry_path = entry.path();
        let metadata = entry
            .metadata()
            .map_err(|source| BenchError::io(&entry_path, source))?;
        let entry_bytes = if metadata.is_dir() {
            directory_bytes(&entry_path)?
        } else {
            metadata.len()
        };
        bytes = bytes.checked_add(entry_bytes).ok_or_else(|| {
            BenchError::report_format("physical canonical byte count exceeds u64::MAX")
        })?;
    }
    Ok(bytes)
}

fn blocks_per_second(block_count: u64, seconds: f64) -> Result<Option<f64>, BenchError> {
    if seconds > 0.0 {
        let block_count = u32::try_from(block_count).map_err(|_| {
            BenchError::report_format("replayed block count exceeds the fixture u32 range")
        })?;
        Ok(Some(f64::from(block_count) / seconds))
    } else {
        Ok(None)
    }
}

fn require_nonzero_u32(candidate: u32, flag: &str) -> Result<NonZeroU32, BenchError> {
    NonZeroU32::new(candidate)
        .ok_or_else(|| BenchError::invalid_argument(format!("--{flag} must be greater than zero")))
}

fn require_nonzero_u64(candidate: u64, flag: &str) -> Result<NonZeroU64, BenchError> {
    NonZeroU64::new(candidate)
        .ok_or_else(|| BenchError::invalid_argument(format!("--{flag} must be greater than zero")))
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fs};

    use clap::Parser;
    use tempfile::tempdir;

    use super::{
        REPORT_CONTRACT_IDENTITY, REPORT_FORMAT_VERSION, RocksDbRawBlobRetentionComparisonArgs,
        blocks_per_second, require_retention_blob_counts,
    };
    use zinder_store::RawBlobRetention;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        comparison: RocksDbRawBlobRetentionComparisonArgs,
    }

    fn arguments(root: &std::path::Path, extra: &[&str]) -> Vec<String> {
        [
            "test".to_owned(),
            "--fixture".to_owned(),
            root.join("fixture").to_string_lossy().into_owned(),
            "--transactions-canonical-store".to_owned(),
            root.join("transactions-canonical")
                .to_string_lossy()
                .into_owned(),
            "--transactions-secondary-root".to_owned(),
            root.join("transactions-secondary")
                .to_string_lossy()
                .into_owned(),
            "--all-canonical-store".to_owned(),
            root.join("all-canonical").to_string_lossy().into_owned(),
            "--all-secondary-root".to_owned(),
            root.join("all-secondary").to_string_lossy().into_owned(),
        ]
        .into_iter()
        .chain(extra.iter().map(|argument| (*argument).to_owned()))
        .collect()
    }

    #[test]
    fn validation_resolves_four_fresh_pairwise_disjoint_outputs() -> Result<(), Box<dyn Error>> {
        let root = tempdir()?;
        fs::create_dir(root.path().join("fixture"))?;
        let cli = TestCli::try_parse_from(arguments(root.path(), &[]))?;

        let validated = cli.comparison.validate()?;

        assert_eq!(validated.request_timeout_seconds, 30);
        assert_eq!(
            validated.supported_reorg_depth,
            zinder_store::DEFAULT_REORG_WINDOW_BLOCKS
        );
        assert!(!validated.transactions_canonical_store.exists());
        assert!(!validated.transactions_secondary_root.exists());
        assert!(!validated.all_canonical_store.exists());
        assert!(!validated.all_secondary_root.exists());
        Ok(())
    }

    #[test]
    fn validation_rejects_existing_output_before_replay() -> Result<(), Box<dyn Error>> {
        let root = tempdir()?;
        fs::create_dir(root.path().join("fixture"))?;
        fs::create_dir(root.path().join("all-secondary"))?;
        let cli = TestCli::try_parse_from(arguments(root.path(), &[]))?;

        let error = cli
            .comparison
            .validate()
            .err()
            .ok_or("existing output path must be rejected")?;

        assert!(error.to_string().contains("--all-secondary-root"));
        assert!(error.to_string().contains("fresh path"));
        Ok(())
    }

    #[test]
    fn validation_rejects_output_overlap_with_fixture() -> Result<(), Box<dyn Error>> {
        let root = tempdir()?;
        let fixture = root.path().join("fixture");
        fs::create_dir(&fixture)?;
        let nested_output = fixture.join("transactions-canonical");
        let mut arguments = arguments(root.path(), &[]);
        arguments[4] = nested_output.to_string_lossy().into_owned();
        let cli = TestCli::try_parse_from(arguments)?;

        let error = cli
            .comparison
            .validate()
            .err()
            .ok_or("fixture overlap must be rejected")?;

        assert!(error.to_string().contains("--fixture"));
        assert!(error.to_string().contains("--transactions-canonical-store"));
        Ok(())
    }

    #[test]
    fn retention_blob_counts_enforce_each_arm_contract() -> Result<(), Box<dyn Error>> {
        require_retention_blob_counts(RawBlobRetention::Transactions, 5, 9, 9, 0)?;
        require_retention_blob_counts(RawBlobRetention::All, 5, 9, 9, 5)?;
        assert!(require_retention_blob_counts(RawBlobRetention::Transactions, 5, 9, 8, 0).is_err());
        assert!(require_retention_blob_counts(RawBlobRetention::All, 5, 9, 9, 0).is_err());
        assert!(require_retention_blob_counts(RawBlobRetention::None, 5, 9, 0, 0).is_err());
        Ok(())
    }

    #[test]
    fn throughput_avoids_zero_duration_division() -> Result<(), Box<dyn Error>> {
        assert_eq!(blocks_per_second(5, 0.0)?, None);
        assert_eq!(blocks_per_second(0, 0.0)?, None);
        assert_eq!(blocks_per_second(10, 2.0)?, Some(5.0));
        Ok(())
    }

    #[test]
    fn report_contract_identity_and_local_format_version_are_stable() {
        assert_eq!(
            REPORT_CONTRACT_IDENTITY,
            "rocksdb-raw-blob-retention-comparison"
        );
        assert_eq!(REPORT_FORMAT_VERSION, 2);
    }
}
