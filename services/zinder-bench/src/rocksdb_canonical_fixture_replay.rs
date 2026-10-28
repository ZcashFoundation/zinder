//! Authenticated checkpointed fixture replay into the production canonical store.

use std::{
    fs,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::{Args, ValueEnum};
use zinder_bench::{
    BenchError,
    canonical_fixture_replay::{
        CanonicalFixtureRocksDbReplayConfig, CanonicalFixtureRocksDbReplayOutcome,
        replay_canonical_fixture_into_rocksdb, replay_canonical_fixture_transport_into_rocksdb,
    },
    fixture::FixtureManifest,
    recorder::install_recorder,
    report::{
        BenchmarkReport, CANONICAL_FIXTURE_REPLAY_PROFILE_CPU_CORES,
        CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES, CanonicalBlockFactsSequenceDigestSummary,
        FixtureSummary, RocksDbCanonicalFixtureEventFenceSummary,
        RocksDbCanonicalFixtureReadySummary, RocksDbCanonicalFixtureReplayMeasurements,
        RocksDbCanonicalFixtureReplayResourceLimits, RocksDbCanonicalFixtureSourceLoadSummary,
        StorageLifecycleBlockId, build_rocksdb_canonical_fixture_replay_report,
    },
    rss::peak_rss,
};
use zinder_core::{BlockId, UnixTimestampMillis};
use zinder_ingest::CanonicalPipelineLimits;
use zinder_source::{
    NodeAuth, ZebraIndexerBlockSource, ZebraIndexerBlockSourceOptions, ZebraIndexerSourceTarget,
    ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};
use zinder_store::{CanonicalStoreReadyEvidence, RawBlobRetention, RocksDbResourceBudget};

const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_SUPPORTED_REORG_DEPTH: u32 = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS;
const DEFAULT_INDEXER_GET_BLOCK_MAX_IN_FLIGHT_REQUESTS: u32 = 12;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CanonicalFixtureBlockSource {
    Fixture,
    ZebraJsonRpc,
    ZebraIndexerGrpc,
}

impl CanonicalFixtureBlockSource {
    const fn label(self) -> &'static str {
        match self {
            Self::Fixture => "fixture",
            Self::ZebraJsonRpc => "zebra-json-rpc",
            Self::ZebraIndexerGrpc => "zebra-indexer-grpc",
        }
    }
}

/// CLI contract for authenticated canonical fixture replay.
#[derive(Args)]
pub(crate) struct RocksDbCanonicalFixtureReplayArgs {
    /// Directory containing the fixture manifest, segments, and replay plan.
    #[arg(long)]
    fixture: PathBuf,
    /// Fresh destination for the canonical store.
    #[arg(long = "canonical-store")]
    canonical_store: PathBuf,
    /// Concrete raw-block source. Transport sources are fixture-authenticated.
    #[arg(long, value_enum, default_value_t = CanonicalFixtureBlockSource::Fixture)]
    block_source: CanonicalFixtureBlockSource,
    /// JSON-RPC endpoint for block retrieval and Zebra Indexer control-plane setup.
    #[arg(long)]
    json_rpc_addr: Option<String>,
    /// Zebra indexer gRPC endpoint for unary historical `GetBlock`.
    #[arg(long)]
    indexer_grpc_addr: Option<String>,
    /// Global actual unary `GetBlock` request limit.
    #[arg(long)]
    indexer_get_block_max_in_flight_requests: Option<u32>,
    /// Injected server response delay recorded as experiment provenance.
    #[arg(long, default_value_t = 0)]
    injected_response_delay_millis: u64,
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
    /// Write the JSON report to this path instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
}

/// Report and optional output path produced by canonical fixture replay.
pub(crate) struct RocksDbCanonicalFixtureReplayOutput {
    pub(crate) report: BenchmarkReport,
    pub(crate) report_path: Option<PathBuf>,
}

struct ValidatedReplayArgs {
    fixture: PathBuf,
    canonical_store: PathBuf,
    request_timeout: Duration,
    pipeline_limits: CanonicalPipelineLimits,
    supported_reorg_depth: u32,
    source_segment_delay: Duration,
    block_source: CanonicalFixtureBlockSource,
    json_rpc_addr: Option<String>,
    indexer_grpc_addr: Option<String>,
    indexer_get_block_max_in_flight_requests: Option<NonZeroU32>,
    injected_response_delay_millis: u64,
}

struct ValidatedBlockSourceArgs {
    json_rpc_addr: Option<String>,
    indexer_grpc_addr: Option<String>,
    indexer_get_block_max_in_flight_requests: Option<NonZeroU32>,
}

struct MeasuredReplay {
    resource_budget: RocksDbResourceBudget,
    outcome: CanonicalFixtureRocksDbReplayOutcome,
    physical_store_bytes: u64,
    total_seconds: f64,
    run_started_at_unix_millis: u64,
    run_completed_at_unix_millis: u64,
}

/// Executes the real canonical fixture replay and builds its closed report.
pub(crate) async fn run_rocksdb_canonical_fixture_replay(
    args: RocksDbCanonicalFixtureReplayArgs,
) -> Result<RocksDbCanonicalFixtureReplayOutput, BenchError> {
    let run_started_at_unix_millis = UnixTimestampMillis::now().value();
    let validated = args.validate()?;
    let manifest = FixtureManifest::read(&validated.fixture)?;
    let fixture = FixtureSummary::try_from(&manifest)?;
    let resource_budget = RocksDbResourceBudget::canonical_writer_defaults();
    let metrics_handle = install_recorder()?;
    let replay_started = Instant::now();
    let replay_config = CanonicalFixtureRocksDbReplayConfig {
        fixture_directory: validated.fixture.clone(),
        canonical_store_path: validated.canonical_store.clone(),
        request_timeout: validated.request_timeout,
        pipeline_limits: validated.pipeline_limits,
        resource_budget,
        raw_blob_retention: RawBlobRetention::Transactions,
        supported_reorg_depth: validated.supported_reorg_depth,
        source_segment_delay: validated.source_segment_delay,
    };
    let outcome = replay_with_block_source(&validated, &manifest, replay_config).await?;
    let total_seconds = replay_started.elapsed().as_secs_f64();
    let physical_store_bytes = directory_bytes(&validated.canonical_store)?;
    let run_completed_at_unix_millis = UnixTimestampMillis::now().value();
    let measurements = replay_measurements(
        &validated,
        MeasuredReplay {
            resource_budget,
            outcome,
            physical_store_bytes,
            total_seconds,
            run_started_at_unix_millis,
            run_completed_at_unix_millis,
        },
    );
    let report = build_rocksdb_canonical_fixture_replay_report(
        fixture,
        measurements,
        Some(&metrics_handle.render()),
    );
    Ok(RocksDbCanonicalFixtureReplayOutput {
        report,
        report_path: args.report,
    })
}

impl RocksDbCanonicalFixtureReplayArgs {
    fn validate(&self) -> Result<ValidatedReplayArgs, BenchError> {
        let fixture = std::path::absolute(&self.fixture)
            .map_err(|source| BenchError::io(&self.fixture, source))?;
        let canonical_store = std::path::absolute(&self.canonical_store)
            .map_err(|source| BenchError::io(&self.canonical_store, source))?;
        if fixture == canonical_store
            || fixture.starts_with(&canonical_store)
            || canonical_store.starts_with(&fixture)
        {
            return Err(BenchError::invalid_argument(
                "--fixture and --canonical-store must be disjoint paths",
            ));
        }
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
        apply_pipeline_overrides(self, &mut pipeline_limits)?;
        let pipeline_limits = pipeline_limits.validate().map_err(|source| {
            BenchError::invalid_argument(format!(
                "canonical fixture replay pipeline limits are invalid: {source}"
            ))
        })?;
        let block_source_args = validate_block_source_args(self)?;
        Ok(ValidatedReplayArgs {
            fixture,
            canonical_store,
            request_timeout: Duration::from_secs(request_timeout_seconds.get()),
            pipeline_limits,
            supported_reorg_depth: supported_reorg_depth.get(),
            source_segment_delay: Duration::from_millis(self.source_segment_delay_millis),
            block_source: self.block_source,
            json_rpc_addr: block_source_args.json_rpc_addr,
            indexer_grpc_addr: block_source_args.indexer_grpc_addr,
            indexer_get_block_max_in_flight_requests: block_source_args
                .indexer_get_block_max_in_flight_requests,
            injected_response_delay_millis: self.injected_response_delay_millis,
        })
    }
}

fn validate_block_source_args(
    args: &RocksDbCanonicalFixtureReplayArgs,
) -> Result<ValidatedBlockSourceArgs, BenchError> {
    if args.block_source != CanonicalFixtureBlockSource::Fixture
        && args.source_segment_delay_millis != 0
    {
        return Err(BenchError::invalid_argument(
            "--source-segment-delay-millis is only valid with --block-source fixture",
        ));
    }
    match args.block_source {
        CanonicalFixtureBlockSource::Fixture => {
            if args.json_rpc_addr.is_some()
                || args.indexer_grpc_addr.is_some()
                || args.indexer_get_block_max_in_flight_requests.is_some()
                || args.injected_response_delay_millis != 0
            {
                return Err(BenchError::invalid_argument(
                    "fixture block source does not accept transport endpoint, concurrency, or injected-delay flags",
                ));
            }
            Ok(ValidatedBlockSourceArgs {
                json_rpc_addr: None,
                indexer_grpc_addr: None,
                indexer_get_block_max_in_flight_requests: None,
            })
        }
        CanonicalFixtureBlockSource::ZebraJsonRpc => {
            let json_rpc_addr = args.json_rpc_addr.clone().ok_or_else(|| {
                BenchError::invalid_argument(
                    "--block-source zebra-json-rpc requires --json-rpc-addr",
                )
            })?;
            if args.indexer_grpc_addr.is_some()
                || args.indexer_get_block_max_in_flight_requests.is_some()
            {
                return Err(BenchError::invalid_argument(
                    "zebra-json-rpc block source does not accept indexer flags",
                ));
            }
            Ok(ValidatedBlockSourceArgs {
                json_rpc_addr: Some(json_rpc_addr),
                indexer_grpc_addr: None,
                indexer_get_block_max_in_flight_requests: None,
            })
        }
        CanonicalFixtureBlockSource::ZebraIndexerGrpc => {
            let json_rpc_addr = args.json_rpc_addr.clone().ok_or_else(|| {
                BenchError::invalid_argument(
                    "--block-source zebra-indexer-grpc requires --json-rpc-addr",
                )
            })?;
            let indexer_grpc_addr = args.indexer_grpc_addr.clone().ok_or_else(|| {
                BenchError::invalid_argument(
                    "--block-source zebra-indexer-grpc requires --indexer-grpc-addr",
                )
            })?;
            let max_in_flight = require_nonzero_u32(
                args.indexer_get_block_max_in_flight_requests
                    .unwrap_or(DEFAULT_INDEXER_GET_BLOCK_MAX_IN_FLIGHT_REQUESTS),
                "indexer-get-block-max-in-flight-requests",
            )?;
            Ok(ValidatedBlockSourceArgs {
                json_rpc_addr: Some(json_rpc_addr),
                indexer_grpc_addr: Some(indexer_grpc_addr),
                indexer_get_block_max_in_flight_requests: Some(max_in_flight),
            })
        }
    }
}

async fn replay_with_block_source(
    validated: &ValidatedReplayArgs,
    manifest: &FixtureManifest,
    config: CanonicalFixtureRocksDbReplayConfig,
) -> Result<CanonicalFixtureRocksDbReplayOutcome, BenchError> {
    match validated.block_source {
        CanonicalFixtureBlockSource::Fixture => replay_canonical_fixture_into_rocksdb(config).await,
        CanonicalFixtureBlockSource::ZebraJsonRpc => {
            let source = json_rpc_source(validated, manifest)?;
            replay_canonical_fixture_transport_into_rocksdb(config, source).await
        }
        CanonicalFixtureBlockSource::ZebraIndexerGrpc => {
            let control_plane = json_rpc_source(validated, manifest)?;
            let endpoint = validated.indexer_grpc_addr.clone().ok_or_else(|| {
                BenchError::invalid_argument("validated gRPC source is missing its endpoint")
            })?;
            let max_in_flight_requests = validated
                .indexer_get_block_max_in_flight_requests
                .ok_or_else(|| {
                    BenchError::invalid_argument(
                        "validated gRPC source is missing its request limit",
                    )
                })?;
            let source = ZebraIndexerBlockSource::connect(
                ZebraIndexerSourceTarget::new(endpoint),
                control_plane,
                ZebraIndexerBlockSourceOptions {
                    connect_timeout: validated.request_timeout,
                    request_timeout: validated.request_timeout,
                    max_in_flight_requests,
                },
            )
            .await?;
            replay_canonical_fixture_transport_into_rocksdb(config, source).await
        }
    }
}

fn json_rpc_source(
    validated: &ValidatedReplayArgs,
    manifest: &FixtureManifest,
) -> Result<ZebraJsonRpcSource, BenchError> {
    let json_rpc_addr = validated.json_rpc_addr.clone().ok_or_else(|| {
        BenchError::invalid_argument("validated transport source is missing its JSON-RPC endpoint")
    })?;
    Ok(ZebraJsonRpcSource::with_options(
        manifest.network_typed()?,
        json_rpc_addr,
        NodeAuth::None,
        ZebraJsonRpcSourceOptions {
            request_timeout: validated.request_timeout,
            max_response_bytes: validated.pipeline_limits.max_response_bytes,
            broadcast_timeout: None,
        },
    )?)
}

fn apply_pipeline_overrides(
    args: &RocksDbCanonicalFixtureReplayArgs,
    limits: &mut CanonicalPipelineLimits,
) -> Result<(), BenchError> {
    if let Some(bytes) = args.source_segment_target_response_bytes {
        limits.source_segment_target_response_bytes =
            require_nonzero_u64(bytes, "source-segment-target-response-bytes")?;
    }
    if let Some(blocks) = args.source_segment_max_blocks {
        limits.source_segment_max_blocks =
            require_nonzero_u32(blocks, "source-segment-max-blocks")?;
    }
    if let Some(requests) = args.source_fetch_max_in_flight_requests {
        limits.source_fetch_max_in_flight_requests =
            require_nonzero_u32(requests, "source-fetch-max-in-flight-requests")?;
    }
    if let Some(bytes) = args.source_fetch_max_in_flight_bytes {
        limits.source_fetch_max_in_flight_bytes =
            require_nonzero_u64(bytes, "source-fetch-max-in-flight-bytes")?;
    }
    if let Some(concurrency) = args.block_prepare_concurrency {
        limits.block_prepare_concurrency =
            require_nonzero_u32(concurrency, "block-prepare-concurrency")?;
    }
    if let Some(bytes) = args.block_prepare_memory_watermark_bytes {
        limits.block_prepare_memory_watermark_bytes =
            require_nonzero_u64(bytes, "block-prepare-memory-watermark-bytes")?;
    }
    Ok(())
}

fn replay_measurements(
    validated: &ValidatedReplayArgs,
    measured: MeasuredReplay,
) -> RocksDbCanonicalFixtureReplayMeasurements {
    let source_load = source_load_summary(&measured.outcome);
    let canonical_ready = ready_summary(&measured.outcome);
    RocksDbCanonicalFixtureReplayMeasurements {
        replay_plan_fixture_manifest_sha256: measured.outcome.fixture_manifest_digest_sha256,
        replay_plan_digest_sha256: measured.outcome.replay_plan_digest_sha256,
        resource_limits: resource_limits(validated, measured.resource_budget),
        publication_proof_provenance: "trusted-fresh-writer",
        total_seconds: measured.total_seconds,
        source_load,
        canonical_ready,
        physical_store_bytes: measured.physical_store_bytes,
        benchmark_client_peak_rss: peak_rss(),
        run_started_at_unix_millis: measured.run_started_at_unix_millis,
        run_completed_at_unix_millis: measured.run_completed_at_unix_millis,
    }
}

fn resource_limits(
    validated: &ValidatedReplayArgs,
    resource_budget: RocksDbResourceBudget,
) -> RocksDbCanonicalFixtureReplayResourceLimits {
    RocksDbCanonicalFixtureReplayResourceLimits {
        block_source: validated.block_source.label(),
        injected_response_delay_millis: validated.injected_response_delay_millis,
        indexer_get_block_max_in_flight_requests: validated
            .indexer_get_block_max_in_flight_requests
            .map(NonZeroU32::get),
        derived_for_cpu_limit_cores: CANONICAL_FIXTURE_REPLAY_PROFILE_CPU_CORES,
        derived_for_memory_limit_bytes: CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES,
        request_timeout_seconds: validated.request_timeout.as_secs(),
        max_response_bytes: validated.pipeline_limits.max_response_bytes.get(),
        source_segment_target_response_bytes: validated
            .pipeline_limits
            .source_segment_target_response_bytes
            .get(),
        source_segment_max_blocks: validated.pipeline_limits.source_segment_max_blocks.get(),
        source_fetch_max_in_flight_requests: validated
            .pipeline_limits
            .source_fetch_max_in_flight_requests
            .get(),
        source_fetch_max_in_flight_bytes: validated
            .pipeline_limits
            .source_fetch_max_in_flight_bytes
            .get(),
        block_prepare_concurrency: validated.pipeline_limits.block_prepare_concurrency.get(),
        block_prepare_memory_watermark_bytes: validated
            .pipeline_limits
            .block_prepare_memory_watermark_bytes
            .get(),
        supported_reorg_depth: validated.supported_reorg_depth,
        source_segment_delay_millis: u64::try_from(validated.source_segment_delay.as_millis())
            .unwrap_or(u64::MAX),
        canonical_rocksdb: resource_budget.into(),
    }
}

fn source_load_summary(
    outcome: &CanonicalFixtureRocksDbReplayOutcome,
) -> RocksDbCanonicalFixtureSourceLoadSummary {
    let load = outcome.block_load_evidence;
    RocksDbCanonicalFixtureSourceLoadSummary {
        first_block: block_id(BlockId::new(load.first_height, load.first_hash)),
        first_parent_hash_hex: hex::encode(load.first_parent_hash.as_bytes()),
        tip: block_id(BlockId::new(load.tip_height, load.tip_hash)),
        block_count: load.block_count,
        transaction_count: load.transaction_count,
        block_header_count: load.block_header_count,
        block_hash_index_count: load.block_hash_index_count,
        block_replay_count: load.block_replay_count,
        compact_block_count: load.compact_block_count,
        transaction_location_count: load.transaction_location_count,
        transaction_blob_count: load.transaction_blob_count,
        block_blob_count: load.block_blob_count,
        tree_state_checkpoint_count: load.tree_state_checkpoint_count,
        block_final_note_commitment_roots_count: load.block_final_note_commitment_roots_count,
        subtree_root_count: outcome.subtree_root_load_evidence.subtree_root_count,
        logical_bytes: load.logical_bytes,
        sst_file_bytes: load.sst_file_bytes,
        sst_file_count: load.sst_file_count,
        replay_format_version: load.replay_format_version.value(),
        sequence_digest: CanonicalBlockFactsSequenceDigestSummary::from_digest(
            load.block_digest_version,
            load.sequence_digest,
        ),
    }
}

fn ready_summary(
    outcome: &CanonicalFixtureRocksDbReplayOutcome,
) -> RocksDbCanonicalFixtureReadySummary {
    let published = outcome.published_ready_evidence;
    let reopened = outcome.reopened_ready_evidence;
    let fence = outcome.event_fence;
    RocksDbCanonicalFixtureReadySummary {
        scope: "canonical-fixture-ready",
        workload: "wallet",
        first_retained_block: block_id(reopened.first_retained_block),
        visible_tip: block_id(reopened.visible_tip),
        visible_epoch_id: reopened.visible_epoch.value(),
        visible_event_sequence: reopened.visible_event_sequence,
        visible_block_count: reopened.visible_block_count,
        replay_format_version: reopened.replay_format_version.value(),
        sequence_digest: ready_sequence_digest(&reopened),
        logical_replay_bytes: reopened.visible_logical_replay_bytes,
        settled_tip: block_id(outcome.settled_tip),
        event_fence: RocksDbCanonicalFixtureEventFenceSummary {
            chain_epoch_id: fence.chain_epoch_id().value(),
            chain_event_sequence: fence.chain_event_sequence(),
            visible_tip: block_id(fence.visible_tip()),
            sequence_digest: CanonicalBlockFactsSequenceDigestSummary::from_digest(
                reopened.block_digest_version,
                fence.sequence_digest(),
            ),
        },
        source_tip_checkpoint_authenticated: outcome.source_tip_checkpoint_authenticated,
        published_and_reopened_ready_match: published == reopened,
        reopened_ready_and_event_fence_match: fence.chain_epoch_id() == reopened.visible_epoch
            && fence.chain_event_sequence() == reopened.visible_event_sequence
            && fence.visible_tip() == reopened.visible_tip
            && fence.sequence_digest().as_bytes() == reopened.visible_sequence_digest,
        full_scan_block_count: outcome.replayed_block_count,
    }
}

fn ready_sequence_digest(
    ready: &CanonicalStoreReadyEvidence,
) -> CanonicalBlockFactsSequenceDigestSummary {
    CanonicalBlockFactsSequenceDigestSummary {
        block_digest_version: ready.block_digest_version.value(),
        sequence_digest_version: ready.sequence_digest_version.value(),
        block_count: ready.visible_block_count,
        sha256: hex::encode(ready.visible_sequence_digest),
    }
}

fn block_id(block: BlockId) -> StorageLifecycleBlockId {
    StorageLifecycleBlockId {
        height: block.height.value(),
        hash_hex: hex::encode(block.hash.as_bytes()),
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
        bytes = bytes.saturating_add(entry_bytes);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use clap::Parser;

    use super::{CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES, RocksDbCanonicalFixtureReplayArgs};

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        replay: RocksDbCanonicalFixtureReplayArgs,
    }

    fn arguments(extra: &[&str]) -> Vec<String> {
        [
            "test",
            "--fixture",
            "/fixture",
            "--canonical-store",
            "/canonical",
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .map(str::to_owned)
        .collect()
    }

    #[test]
    fn defaults_use_the_ten_cpu_ten_gib_resource_profile() -> Result<(), Box<dyn Error>> {
        let cli = TestCli::try_parse_from(arguments(&[]))?;
        let validated = cli.replay.validate()?;

        assert_eq!(validated.request_timeout.as_secs(), 30);
        assert_eq!(
            validated.supported_reorg_depth,
            zinder_store::DEFAULT_REORG_WINDOW_BLOCKS
        );
        assert_eq!(
            validated.pipeline_limits.max_response_bytes.get(),
            64 * 1024 * 1024
        );
        assert_eq!(
            validated
                .pipeline_limits
                .source_segment_target_response_bytes
                .get(),
            32 * 1024 * 1024
        );
        assert_eq!(
            validated.pipeline_limits.source_segment_max_blocks.get(),
            64
        );
        assert_eq!(
            validated
                .pipeline_limits
                .source_fetch_max_in_flight_requests
                .get(),
            12
        );
        assert_eq!(
            validated
                .pipeline_limits
                .source_fetch_max_in_flight_bytes
                .get(),
            CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES / 64
        );
        assert_eq!(
            validated.pipeline_limits.block_prepare_concurrency.get(),
            10
        );
        assert_eq!(
            validated
                .pipeline_limits
                .block_prepare_memory_watermark_bytes
                .get(),
            CANONICAL_FIXTURE_REPLAY_PROFILE_MEMORY_BYTES / 64
        );
        Ok(())
    }

    #[test]
    fn every_pipeline_limit_accepts_an_explicit_override() -> Result<(), Box<dyn Error>> {
        let cli = TestCli::try_parse_from(arguments(&[
            "--max-response-bytes",
            "67108864",
            "--source-segment-target-response-bytes",
            "16777216",
            "--source-segment-max-blocks",
            "17",
            "--source-fetch-max-in-flight-requests",
            "5",
            "--source-fetch-max-in-flight-bytes",
            "402653184",
            "--block-prepare-concurrency",
            "7",
            "--block-prepare-memory-watermark-bytes",
            "268435456",
        ]))?;
        let limits = cli.replay.validate()?.pipeline_limits;

        assert_eq!(limits.max_response_bytes.get(), 67_108_864);
        assert_eq!(
            limits.source_segment_target_response_bytes.get(),
            16_777_216
        );
        assert_eq!(limits.source_segment_max_blocks.get(), 17);
        assert_eq!(limits.source_fetch_max_in_flight_requests.get(), 5);
        assert_eq!(limits.source_fetch_max_in_flight_bytes.get(), 402_653_184);
        assert_eq!(limits.block_prepare_concurrency.get(), 7);
        assert_eq!(
            limits.block_prepare_memory_watermark_bytes.get(),
            268_435_456
        );
        Ok(())
    }

    #[test]
    fn zero_is_rejected_for_every_positive_limit() -> Result<(), Box<dyn Error>> {
        for flag in [
            "--request-timeout-secs",
            "--supported-reorg-depth",
            "--max-response-bytes",
            "--source-segment-target-response-bytes",
            "--source-segment-max-blocks",
            "--source-fetch-max-in-flight-requests",
            "--source-fetch-max-in-flight-bytes",
            "--block-prepare-concurrency",
            "--block-prepare-memory-watermark-bytes",
        ] {
            let cli = TestCli::try_parse_from(arguments(&[flag, "0"]))?;
            let error = cli
                .replay
                .validate()
                .err()
                .ok_or("zero limit must be rejected")?;
            assert!(
                error.to_string().contains("must be greater than zero"),
                "{flag}"
            );
        }
        Ok(())
    }

    #[test]
    fn indexer_block_source_requires_both_endpoints_and_defaults_to_twelve_unary_calls()
    -> Result<(), Box<dyn Error>> {
        let cli = TestCli::try_parse_from(arguments(&[
            "--block-source",
            "zebra-indexer-grpc",
            "--json-rpc-addr",
            "http://transport:19432",
            "--indexer-grpc-addr",
            "http://transport:19430",
            "--injected-response-delay-millis",
            "2070",
        ]))?;
        let validated = cli.replay.validate()?;

        assert_eq!(
            validated
                .indexer_get_block_max_in_flight_requests
                .ok_or("validated indexer source must have a request limit")?
                .get(),
            12
        );
        assert_eq!(validated.injected_response_delay_millis, 2_070);
        Ok(())
    }

    #[test]
    fn block_source_specific_flags_fail_closed() -> Result<(), Box<dyn Error>> {
        for extra in [
            vec!["--json-rpc-addr", "http://transport:19432"],
            vec!["--injected-response-delay-millis", "2070"],
            vec!["--block-source", "zebra-json-rpc"],
            vec![
                "--block-source",
                "zebra-indexer-grpc",
                "--json-rpc-addr",
                "http://transport:19432",
            ],
        ] {
            let cli = TestCli::try_parse_from(arguments(&extra))?;
            assert!(cli.replay.validate().is_err(), "accepted {extra:?}");
        }
        Ok(())
    }
}
