//! Complete-history canonical and wallet `RocksDB` lifecycle measurement.

use std::{
    fs,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::Args;
use zinder_bench::{
    BenchError,
    report::{
        AcceptanceThresholds, BenchmarkReport, BenchmarkRunProvenance,
        CanonicalBlockFactsSequenceDigestSummary, CanonicalStorageReadySummary, ReportProvenance,
        RocksDbResourceBudgetSummary, RocksDbStorageLifecycleMeasurements, RunnerProvenance,
        StorageLifecycleBlockId, StorageLifecycleContractSummary, StorageLifecyclePhaseDurations,
        StorageLifecycleResourceLimits, StorageLifecycleSourceSummary,
        WalletStorageConstructionEvidence, WalletStoragePhaseDurations, WalletStorageReadySummary,
        WalletStorageRowCounts, WalletStorageUtxoSummary, WalletVariableValueSortEvidence,
        build_rocksdb_storage_lifecycle_report, is_immutable_image_reference,
        is_valid_benchmark_trial_id, summarize_rocksdb_storage_lifecycle_acceptance,
    },
    rss::peak_rss,
};
use zinder_core::{
    BlockHeight, BlockId, CanonicalBlockFactsSequenceDigest,
    NetworkUpgradeActivationsFingerprintVersion, UnixTimestampMillis,
    wire::{decode_zinder_native_chain_name, encode_zinder_native_chain_name},
};
use zinder_ingest::{CanonicalConstructionConfig, CanonicalPipelineLimits, load_fresh_canonical};
use zinder_source::{
    CookieSource, NodeAuth, NodeSource, ZebraJsonRpcSource, ZebraJsonRpcSourceOptions,
};
use zinder_store::{
    CANONICAL_STORE_IDENTITY, CANONICAL_STORE_SCHEMA_VERSION, CanonicalBaselinePublication,
    CanonicalReorgPolicy, CanonicalStoreBuildPlan, CanonicalStoreReadyEvidence,
    CanonicalStoreWorkload, RawBlobRetention, RocksDbCanonicalBuilder, RocksDbCanonicalStore,
    RocksDbResourceBudget,
};
use zinder_wallet_projection::{
    WALLET_PROJECTION_SCHEMA_VERSION, WALLET_PROJECTION_VALUE_ENCODING_VERSION,
};
use zinder_wallet_rocksdb::{
    RocksDbWalletBuildOptions, RocksDbWalletBuildReport, RocksDbWalletStore,
    WALLET_ROCKSDB_SCHEMA_VERSION, build_wallet_from_canonical,
};

const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_SUPPORTED_REORG_DEPTH: u32 = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS;

// These ceilings intentionally describe the measured production build, not a
// hidden unbounded allocation. Sorters use their limits independently and the
// cold validator runs after sorting has released its memory.
const WALLET_OUTPOINT_SORT_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER: u64 = 1024 * 1024 * 1024;
const WALLET_TEMPORARY_FILE_BYTES_PER_SORTER: u64 = 64 * 1024 * 1024 * 1024;
const WALLET_SST_TARGET_LOGICAL_BYTES: u64 = 128 * 1024 * 1024;
const WALLET_ACCOUNTED_REORG_UNDO_BYTES: u64 = 512 * 1024 * 1024;

/// CLI contract for a clean complete-history `RocksDB` storage lifecycle.
#[derive(Args)]
pub(crate) struct RocksDbStorageLifecycleArgs {
    /// Network name, such as zcash-testnet.
    #[arg(long)]
    network: String,
    /// Zebra JSON-RPC base URL.
    #[arg(long = "json-rpc-addr")]
    json_rpc_addr: String,
    /// Optional node cookie file path.
    #[arg(long = "node-auth-cookie")]
    node_auth_cookie: Option<PathBuf>,
    /// Fresh canonical store path.
    #[arg(long = "canonical-store")]
    canonical_store: PathBuf,
    /// Fresh wallet store path.
    #[arg(long = "wallet-store")]
    wallet_store: PathBuf,
    /// Exact source height to freeze; defaults to the first observed node tip.
    #[arg(long = "tip-height")]
    tip_height: Option<u32>,
    /// Per-request source timeout in seconds.
    #[arg(long = "request-timeout-secs", default_value_t = DEFAULT_REQUEST_TIMEOUT_SECONDS)]
    request_timeout_seconds: u64,
    /// Maximum accepted source response body.
    #[arg(long, default_value_t = DEFAULT_MAX_RESPONSE_BYTES)]
    max_response_bytes: u64,
    /// Number of exact-tip wallet undo rows retained for reorg handling.
    #[arg(long, default_value_t = DEFAULT_SUPPORTED_REORG_DEPTH)]
    supported_reorg_depth: u32,
    /// Source revision of the measured binary.
    #[arg(long = "software-revision")]
    software_revision: Option<String>,
    /// Unique evidence identifier for this invocation.
    #[arg(long = "trial-id")]
    trial_id: Option<String>,
    /// Stable operator label for the runner.
    #[arg(long = "runner-id")]
    runner_id: Option<String>,
    /// CPU limit applied to the benchmark container, in logical cores.
    #[arg(long = "cpu-limit-cores")]
    cpu_limit_cores: f64,
    /// Memory limit applied to the benchmark container, in bytes.
    #[arg(long = "memory-limit-bytes")]
    memory_limit_bytes: u64,
    /// Stable operator-defined storage performance class.
    #[arg(long = "storage-class")]
    storage_class: Option<String>,
    /// Immutable container image reference for the measured binary.
    #[arg(long = "image-reference")]
    image_reference: Option<String>,
    /// Desired complete canonical storage-ready time.
    #[arg(long = "canonical-storage-ready-target-secs")]
    canonical_storage_ready_target_seconds: Option<f64>,
    /// Maximum accepted complete canonical storage-ready time.
    #[arg(long = "canonical-storage-ready-hard-limit-secs")]
    canonical_storage_ready_hard_limit_seconds: Option<f64>,
    /// Desired wallet derivation and storage-ready time after canonical readiness.
    #[arg(long = "wallet-storage-ready-target-secs")]
    wallet_storage_ready_target_seconds: Option<f64>,
    /// Maximum accepted wallet derivation and storage-ready time after canonical readiness.
    #[arg(long = "wallet-storage-ready-hard-limit-secs")]
    wallet_storage_ready_hard_limit_seconds: Option<f64>,
    /// Write the JSON report to this path instead of stdout.
    #[arg(long)]
    report: Option<PathBuf>,
}

/// Report and optional output path produced by the lifecycle command.
pub(crate) struct RocksDbStorageLifecycleOutput {
    pub(crate) report: BenchmarkReport,
    pub(crate) report_path: Option<PathBuf>,
}

struct ValidatedLifecycleArgs {
    network: zinder_core::Network,
    canonical_store: PathBuf,
    wallet_store: PathBuf,
    fixed_tip_height: Option<BlockHeight>,
    request_timeout: Duration,
    pipeline_limits: CanonicalPipelineLimits,
    supported_reorg_depth: u32,
    canonical_storage_ready_thresholds: Option<AcceptanceThresholds>,
    wallet_storage_ready_thresholds: Option<AcceptanceThresholds>,
}

/// Executes a complete canonical build and wallet derivation at one immutable fence.
#[allow(
    clippy::too_many_lines,
    reason = "the lifecycle owner keeps all measured phase boundaries and admitted evidence in order"
)]
pub(crate) async fn run_rocksdb_storage_lifecycle(
    args: RocksDbStorageLifecycleArgs,
) -> Result<RocksDbStorageLifecycleOutput, BenchError> {
    // Capture wall-clock provenance first so its millisecond interval encloses
    // the monotonic duration that the report claims as total runtime.
    let run_started_at_unix_millis = UnixTimestampMillis::now().value();
    let total_started = Instant::now();
    let validated = args.validate()?;
    let canonical_resource_budget = RocksDbResourceBudget::canonical_writer_defaults();
    let wallet_resource_budget = RocksDbResourceBudget::materialized_view_writer_defaults();

    let source_discovery_started = Instant::now();
    let node_auth = args
        .node_auth_cookie
        .clone()
        .map_or(NodeAuth::None, |path| {
            NodeAuth::Cookie(CookieSource::File(path))
        });
    let source = ZebraJsonRpcSource::with_options(
        validated.network,
        args.json_rpc_addr.clone(),
        node_auth,
        ZebraJsonRpcSourceOptions {
            request_timeout: validated.request_timeout,
            max_response_bytes: validated.pipeline_limits.max_response_bytes,
            broadcast_timeout: None,
        },
    )?;
    let network_upgrade_activations = source
        .discover_network_upgrade_activations("zinder-bench")
        .await?;
    let source_tip_at_freeze = source.tip_id().await?;
    let fixed_build_tip = resolve_fixed_build_tip(
        &source,
        validated.network,
        source_tip_at_freeze,
        validated.fixed_tip_height,
    )
    .await?;
    let genesis = source.fetch_block_at(BlockHeight::new(0)).await?;
    validate_source_block_identity(
        &genesis,
        validated.network,
        BlockId::new(BlockHeight::new(0), validated.network.genesis_hash()),
        "genesis",
    )?;
    let settled_height = BlockHeight::new(
        fixed_build_tip
            .height
            .value()
            .saturating_sub(validated.supported_reorg_depth)
            .max(1),
    );
    let settled_block = source.fetch_block_at(settled_height).await?;
    let settled_tip = BlockId::new(settled_height, settled_block.hash);
    validate_source_block_identity(
        &settled_block,
        validated.network,
        settled_tip,
        "settled baseline",
    )?;
    let source_discovery = source_discovery_started.elapsed();

    let build_plan = CanonicalStoreBuildPlan::complete(
        &network_upgrade_activations,
        genesis.block_time_seconds,
        fixed_build_tip,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(validated.supported_reorg_depth)?,
    )?;
    let canonical_store_initialization_started = Instant::now();
    let builder = RocksDbCanonicalBuilder::create_fresh(
        &validated.canonical_store,
        CanonicalStoreWorkload::Wallet,
        build_plan,
        canonical_resource_budget,
    )?;
    let database_io_mode = builder.io_mode().as_str().to_owned();
    let canonical_store_initialization = canonical_store_initialization_started.elapsed();

    let canonical_source_load_started = Instant::now();
    let canonical_load = load_fresh_canonical(
        builder,
        &source,
        &CanonicalConstructionConfig {
            request_timeout: validated.request_timeout,
            pipeline_limits: validated.pipeline_limits,
            network_upgrade_activations: network_upgrade_activations.clone(),
        },
    )
    .await?;
    let canonical_source_load = canonical_source_load_started.elapsed();
    let source_tip_after_canonical_load = source.tip_id().await?;
    let canonical_block_evidence = canonical_load.block_evidence;
    let canonical_subtree_evidence = canonical_load.subtree_root_evidence;
    let source_tip_checkpoint_authenticated =
        canonical_load.builder.is_source_tip_checkpoint_confirmed();

    let canonical_cold_validation_started = Instant::now();
    let validated_canonical = canonical_load
        .builder
        .prepare_cold_certified_publication()?;
    let canonical_cold_validation = canonical_cold_validation_started.elapsed();

    let canonical_ready_publication_started = Instant::now();
    let prepared_publication = validated_canonical.prepare_baseline(
        CanonicalBaselinePublication::new(settled_tip, UnixTimestampMillis::now()),
    )?;
    let canonical_store = validated_canonical.publish_baseline(prepared_publication)?;
    let published_canonical_ready = canonical_store.ready_evidence();
    let canonical_ready_publication = canonical_ready_publication_started.elapsed();

    drop(canonical_store);
    let canonical_cold_reopen_started = Instant::now();
    let canonical_store = RocksDbCanonicalStore::open_ready(
        &validated.canonical_store,
        &network_upgrade_activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(validated.supported_reorg_depth)?,
        canonical_resource_budget,
    )?;
    let canonical_cold_reopen = canonical_cold_reopen_started.elapsed();
    let canonical_cold_reopen_evidence_match =
        canonical_store.ready_evidence() == published_canonical_ready;
    let canonical_storage_ready_seconds = total_started.elapsed().as_secs_f64();
    let canonical_physical_store_bytes = directory_bytes(&validated.canonical_store)?;

    let wallet_started = Instant::now();
    let wallet_outcome = build_wallet_from_canonical(
        &canonical_store,
        &validated.wallet_store,
        RocksDbWalletBuildOptions {
            resource_budget: wallet_resource_budget,
            max_outpoint_sort_memory_bytes: WALLET_OUTPOINT_SORT_MEMORY_BYTES,
            max_secondary_sort_memory_bytes_per_sorter:
                WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
            max_temporary_file_bytes_per_sorter: WALLET_TEMPORARY_FILE_BYTES_PER_SORTER,
            sst_target_logical_bytes: WALLET_SST_TARGET_LOGICAL_BYTES,
            max_accounted_reorg_undo_bytes: WALLET_ACCOUNTED_REORG_UNDO_BYTES,
            supported_reorg_depth: validated.supported_reorg_depth,
        },
    )?;
    let wallet_build = wallet_outcome.report.phase_durations.total;
    let expected_wallet_source = wallet_outcome.report.canonical_source_identity();
    let published_wallet_ready = wallet_outcome.store.ready_evidence().clone();
    let wallet_build_report = wallet_outcome.report;
    drop(wallet_outcome.store);
    drop(canonical_store);

    let final_cold_reopen_started = Instant::now();
    let canonical_store = RocksDbCanonicalStore::open_ready(
        &validated.canonical_store,
        &network_upgrade_activations,
        CanonicalStoreWorkload::Wallet,
        RawBlobRetention::Transactions,
        CanonicalReorgPolicy::new(validated.supported_reorg_depth)?,
        canonical_resource_budget,
    )?;
    let wallet_store = RocksDbWalletStore::open_ready(
        &validated.wallet_store,
        validated.network,
        expected_wallet_source,
        wallet_resource_budget,
    )?;
    let final_cold_reopen = final_cold_reopen_started.elapsed();
    let final_canonical_ready = canonical_store.ready_evidence();
    let final_wallet_ready = wallet_store.ready_evidence();
    let wallet_cold_reopen_evidence_match = *final_wallet_ready == published_wallet_ready;
    let wallet_canonical_fence_match =
        wallet_source_matches_canonical(final_wallet_ready, &final_canonical_ready);
    let wallet_storage_ready_seconds = wallet_started.elapsed().as_secs_f64();
    let total = total_started.elapsed();
    let wallet_physical_store_bytes = directory_bytes(&validated.wallet_store)?;

    let canonical_ready_summary = canonical_ready_summary(
        &published_canonical_ready,
        &canonical_block_evidence,
        canonical_subtree_evidence,
        canonical_physical_store_bytes,
        database_io_mode,
        source_tip_checkpoint_authenticated,
        canonical_cold_reopen_evidence_match,
    );
    let wallet_ready_summary = wallet_ready_summary(
        &wallet_build_report,
        wallet_physical_store_bytes,
        wallet_cold_reopen_evidence_match,
        wallet_canonical_fence_match,
    );
    let activation_fingerprint =
        network_upgrade_activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
    let report = build_rocksdb_storage_lifecycle_report(RocksDbStorageLifecycleMeasurements {
        provenance: ReportProvenance {
            benchmark_version: env!("CARGO_PKG_VERSION"),
            software_revision: args.software_revision,
            run: BenchmarkRunProvenance {
                trial_id: args.trial_id,
                fixture_cache_policy: None,
                started_at_unix_millis: run_started_at_unix_millis,
                completed_at_unix_millis: UnixTimestampMillis::now().value(),
            },
            runner: RunnerProvenance {
                id: args.runner_id,
                cpu_limit_cores: Some(args.cpu_limit_cores),
                memory_limit_bytes: Some(args.memory_limit_bytes),
                storage_class: args.storage_class,
            },
            image_reference: args.image_reference,
            target_os: std::env::consts::OS,
            target_arch: std::env::consts::ARCH,
        },
        source: StorageLifecycleSourceSummary {
            family: "zebra-json-rpc",
            network: encode_zinder_native_chain_name(validated.network).to_owned(),
            network_upgrade_activation_count: network_upgrade_activations.activations().len(),
            network_upgrade_activations_fingerprint_version: activation_fingerprint
                .version()
                .value(),
            network_upgrade_activations_fingerprint_hex: hex::encode(
                activation_fingerprint.as_bytes(),
            ),
            source_tip_at_freeze: block_id_summary(source_tip_at_freeze),
            fixed_build_tip: block_id_summary(fixed_build_tip),
            source_tip_after_canonical_load: block_id_summary(source_tip_after_canonical_load),
        },
        contracts: StorageLifecycleContractSummary {
            canonical_store_identity: CANONICAL_STORE_IDENTITY,
            canonical_store_schema_version: CANONICAL_STORE_SCHEMA_VERSION,
            wallet_store_identity: "wallet",
            wallet_store_schema_version: WALLET_ROCKSDB_SCHEMA_VERSION,
            wallet_projection_schema_version: WALLET_PROJECTION_SCHEMA_VERSION,
            wallet_value_encoding_version: WALLET_PROJECTION_VALUE_ENCODING_VERSION,
        },
        resource_limits: StorageLifecycleResourceLimits {
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
            canonical_rocksdb: RocksDbResourceBudgetSummary::from(canonical_resource_budget),
            wallet_rocksdb: RocksDbResourceBudgetSummary::from(wallet_resource_budget),
            supported_reorg_depth: validated.supported_reorg_depth,
            wallet_max_outpoint_sort_memory_bytes: WALLET_OUTPOINT_SORT_MEMORY_BYTES,
            wallet_max_secondary_sort_memory_bytes_per_sorter:
                WALLET_SECONDARY_SORT_MEMORY_BYTES_PER_SORTER,
            wallet_max_temporary_file_bytes_per_sorter: WALLET_TEMPORARY_FILE_BYTES_PER_SORTER,
            wallet_sst_target_logical_bytes: WALLET_SST_TARGET_LOGICAL_BYTES,
            wallet_max_accounted_reorg_undo_bytes: WALLET_ACCOUNTED_REORG_UNDO_BYTES,
        },
        acceptance: summarize_rocksdb_storage_lifecycle_acceptance(
            canonical_storage_ready_seconds,
            validated.canonical_storage_ready_thresholds,
            wallet_storage_ready_seconds,
            validated.wallet_storage_ready_thresholds,
        ),
        phase_durations: StorageLifecyclePhaseDurations {
            source_discovery_seconds: source_discovery.as_secs_f64(),
            canonical_store_initialization_seconds: canonical_store_initialization.as_secs_f64(),
            canonical_source_load_seconds: canonical_source_load.as_secs_f64(),
            canonical_cold_validation_seconds: canonical_cold_validation.as_secs_f64(),
            canonical_ready_publication_seconds: canonical_ready_publication.as_secs_f64(),
            canonical_cold_reopen_seconds: canonical_cold_reopen.as_secs_f64(),
            wallet_build_seconds: wallet_build.as_secs_f64(),
            final_cold_reopen_seconds: final_cold_reopen.as_secs_f64(),
            total_seconds: total.as_secs_f64(),
        },
        canonical_storage_ready: canonical_ready_summary,
        wallet_storage_ready: wallet_ready_summary,
        benchmark_client_peak_rss: peak_rss(),
    });
    Ok(RocksDbStorageLifecycleOutput {
        report,
        report_path: args.report,
    })
}

impl RocksDbStorageLifecycleArgs {
    #[allow(
        clippy::too_many_lines,
        reason = "the CLI contract validates each exposed resource and provenance field in one pass"
    )]
    fn validate(&self) -> Result<ValidatedLifecycleArgs, BenchError> {
        let network = decode_zinder_native_chain_name(&self.network)
            .map_err(|source| BenchError::invalid_argument(source.to_string()))?;
        let canonical_store = std::path::absolute(&self.canonical_store)
            .map_err(|source| BenchError::io(&self.canonical_store, source))?;
        let wallet_store = std::path::absolute(&self.wallet_store)
            .map_err(|source| BenchError::io(&self.wallet_store, source))?;
        if canonical_store == wallet_store
            || canonical_store.starts_with(&wallet_store)
            || wallet_store.starts_with(&canonical_store)
        {
            return Err(BenchError::invalid_argument(
                "--canonical-store and --wallet-store must be disjoint paths",
            ));
        }
        if self.tip_height == Some(0) {
            return Err(BenchError::invalid_argument(
                "--tip-height must be greater than zero",
            ));
        }
        if self.supported_reorg_depth == 0 {
            return Err(BenchError::invalid_argument(
                "--supported-reorg-depth must be greater than zero",
            ));
        }
        if self
            .trial_id
            .as_deref()
            .is_some_and(|trial_id| !is_valid_benchmark_trial_id(trial_id))
        {
            return Err(BenchError::invalid_argument(
                "--trial-id must start with an ASCII alphanumeric character and contain only ASCII alphanumeric characters, '.', '_', or '-'",
            ));
        }
        validate_positive_f64(Some(self.cpu_limit_cores), "--cpu-limit-cores")?;
        validate_positive_u64(Some(self.memory_limit_bytes), "--memory-limit-bytes")?;
        if self
            .image_reference
            .as_deref()
            .is_some_and(|reference| !is_immutable_image_reference(reference))
        {
            return Err(BenchError::invalid_argument(
                "--image-reference must be a sha256 image ID or digest-pinned image reference",
            ));
        }
        let max_response_bytes =
            require_nonzero_u64(self.max_response_bytes, "max-response-bytes")?;
        let memory_limit_bytes =
            require_nonzero_u64(self.memory_limit_bytes, "memory-limit-bytes")?;
        let pipeline_limits = CanonicalPipelineLimits::resolve(
            Some(memory_limit_bytes),
            logical_core_count_from_limit(self.cpu_limit_cores),
            max_response_bytes,
        );
        Ok(ValidatedLifecycleArgs {
            network,
            canonical_store,
            wallet_store,
            fixed_tip_height: self.tip_height.map(BlockHeight::new),
            request_timeout: Duration::from_secs(
                require_nonzero_u64(self.request_timeout_seconds, "request-timeout-secs")?.get(),
            ),
            pipeline_limits,
            supported_reorg_depth: self.supported_reorg_depth,
            canonical_storage_ready_thresholds: acceptance_thresholds(
                self.canonical_storage_ready_target_seconds,
                self.canonical_storage_ready_hard_limit_seconds,
                "canonical-storage-ready",
            )?,
            wallet_storage_ready_thresholds: acceptance_thresholds(
                self.wallet_storage_ready_target_seconds,
                self.wallet_storage_ready_hard_limit_seconds,
                "wallet-storage-ready",
            )?,
        })
    }
}

async fn resolve_fixed_build_tip(
    source: &ZebraJsonRpcSource,
    network: zinder_core::Network,
    source_tip: BlockId,
    requested_height: Option<BlockHeight>,
) -> Result<BlockId, BenchError> {
    let Some(requested_height) = requested_height else {
        return Ok(source_tip);
    };
    if requested_height > source_tip.height {
        return Err(BenchError::invalid_argument(format!(
            "--tip-height {} exceeds the observed Zebra tip {}",
            requested_height.value(),
            source_tip.height.value()
        )));
    }
    if requested_height == source_tip.height {
        return Ok(source_tip);
    }
    let source_block = source.fetch_block_at(requested_height).await?;
    let fixed_tip = BlockId::new(requested_height, source_block.hash);
    validate_source_block_identity(&source_block, network, fixed_tip, "fixed build tip")?;
    Ok(fixed_tip)
}

fn validate_source_block_identity(
    source_block: &zinder_source::SourceBlock,
    expected_network: zinder_core::Network,
    expected_block: BlockId,
    role: &'static str,
) -> Result<(), BenchError> {
    if source_block.network != expected_network
        || source_block.height != expected_block.height
        || source_block.hash != expected_block.hash
    {
        return Err(BenchError::acceptance_completion_mismatch(format!(
            "{role} source identity does not match the requested network and block"
        )));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the summary maps distinct canonical lifecycle evidence without creating a second aggregate type"
)]
fn canonical_ready_summary(
    ready: &CanonicalStoreReadyEvidence,
    block_load: &zinder_store::CanonicalBlockLoadEvidence,
    subtree_load: zinder_store::CanonicalSubtreeRootLoadEvidence,
    physical_store_bytes: u64,
    database_io_mode: String,
    source_tip_checkpoint_authenticated: bool,
    cold_reopen_evidence_match: bool,
) -> CanonicalStorageReadySummary {
    let sequence_digest = CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
        ready.sequence_digest_version,
        ready.visible_block_count,
        ready.visible_sequence_digest,
    );
    CanonicalStorageReadySummary {
        scope: "canonical-storage-ready",
        workload: CanonicalStoreWorkload::Wallet.as_str(),
        first_retained_block: block_id_summary(ready.first_retained_block),
        visible_tip: block_id_summary(ready.visible_tip),
        visible_epoch_id: ready.visible_epoch.value(),
        visible_event_sequence: ready.visible_event_sequence,
        block_count: ready.visible_block_count,
        transaction_count: block_load.transaction_count,
        subtree_root_count: subtree_load.subtree_root_count,
        replay_format_version: ready.replay_format_version.value(),
        sequence_digest: CanonicalBlockFactsSequenceDigestSummary::from_digest(
            ready.block_digest_version,
            sequence_digest,
        ),
        logical_replay_bytes: ready.visible_logical_replay_bytes,
        logical_storage_bytes: block_load.logical_bytes,
        sst_file_bytes: block_load.sst_file_bytes,
        sst_file_count: block_load.sst_file_count,
        physical_store_bytes,
        database_io_mode,
        source_tip_checkpoint_authenticated,
        cold_reopen_evidence_match,
    }
}

fn wallet_ready_summary(
    report: &RocksDbWalletBuildReport,
    physical_store_bytes: u64,
    cold_reopen_evidence_match: bool,
    canonical_fence_match: bool,
) -> WalletStorageReadySummary {
    let row_counts = report.row_counts;
    let utxo_summary = &report.utxo_summary;
    WalletStorageReadySummary {
        scope: "wallet-storage-ready",
        source_epoch_id: report.source_position.chain_epoch_id.value(),
        source_tip: block_id_summary(report.source_position.tip),
        source_event_sequence: report.source_position.event_sequence,
        source_sequence_digest: CanonicalBlockFactsSequenceDigestSummary::from_digest(
            zinder_core::CanonicalBlockFactsDigestVersion::V1,
            report.source_sequence_digest,
        ),
        wallet_projection_digest_hex: hex::encode(report.projection_digest.as_bytes()),
        row_counts: WalletStorageRowCounts {
            transparent_unspent_output_count: row_counts.transparent_unspent_output_count,
            transparent_unspent_output_by_address_count: row_counts
                .transparent_unspent_output_by_address_count,
            transparent_spent_output_count: row_counts.transparent_spent_output_count,
            transparent_address_transaction_count: row_counts.transparent_address_transaction_count,
            transparent_address_balance_count: row_counts.transparent_address_balance_count,
            reorg_undo_count: row_counts.reorg_undo_count,
        },
        utxo_summary: WalletStorageUtxoSummary {
            utxo_count: utxo_summary.utxo_count,
            total_value_zat: utxo_summary.total_value_zat,
            commitment_scheme: match utxo_summary.commitment.scheme() {
                zinder_core::UtxoSetCommitmentScheme::LtHash16 => "lthash16",
                zinder_core::UtxoSetCommitmentScheme::Unspecified => "unspecified",
                _ => "unknown",
            },
            commitment_accumulator_hex: hex::encode(utxo_summary.commitment.accumulator()),
            commitment_display_digest_hex: hex::encode(utxo_summary.commitment.display_digest()),
        },
        scanned_block_count: report.scanned_block_count,
        scanned_transaction_count: report.scanned_transaction_count,
        historical_prevout_read_count: report.historical_prevout_read_count,
        construction: WalletStorageConstructionEvidence {
            outpoint_sort: variable_value_sort_evidence(report.outpoint_sort),
            address_index_sort: variable_value_sort_evidence(report.address_index_sort),
            address_transaction_sort: variable_value_sort_evidence(report.address_transaction_sort),
            cold_validation_address_index_sort: variable_value_sort_evidence(
                report.cold_validation_address_index_sort,
            ),
            cold_validation_address_transaction_sort: variable_value_sort_evidence(
                report.cold_validation_address_transaction_sort,
            ),
            peak_accounted_reorg_undo_bytes: report.peak_accounted_reorg_undo_bytes,
            max_accounted_reorg_undo_bytes: report.max_accounted_reorg_undo_bytes,
            cold_validation_peak_accounted_reorg_undo_bytes: report
                .cold_validation_peak_accounted_reorg_undo_bytes,
            cold_validation_max_accounted_reorg_undo_bytes: report
                .cold_validation_max_accounted_reorg_undo_bytes,
            cold_validation_random_read_count: report.cold_validation_random_read_count,
            logical_row_bytes: report.logical_row_bytes,
            sst_file_bytes: report.sst_file_bytes,
            sst_file_count: report.sst_file_count,
        },
        phase_durations: wallet_phase_durations(report),
        physical_store_bytes,
        cold_reopen_evidence_match,
        canonical_fence_match,
    }
}

fn wallet_phase_durations(report: &RocksDbWalletBuildReport) -> WalletStoragePhaseDurations {
    let durations = report.phase_durations;
    WalletStoragePhaseDurations {
        store_initialization_seconds: durations.store_initialization.as_secs_f64(),
        canonical_scan_seconds: durations.canonical_scan.as_secs_f64(),
        outpoint_sort_seconds: durations.outpoint_sort.as_secs_f64(),
        outpoint_merge_seconds: durations.outpoint_merge.as_secs_f64(),
        secondary_row_derivation_seconds: durations.secondary_row_derivation.as_secs_f64(),
        logical_evidence_seconds: durations.logical_evidence.as_secs_f64(),
        row_load_seconds: durations.row_load.as_secs_f64(),
        flush_and_cold_reopen_seconds: durations.flush_and_cold_reopen.as_secs_f64(),
        cold_validation_seconds: durations.cold_validation.as_secs_f64(),
        ready_publication_seconds: durations.ready_publication.as_secs_f64(),
        total_seconds: durations.total.as_secs_f64(),
    }
}

fn variable_value_sort_evidence(
    evidence: zinder_rocksdb_bulk_load::VariableValueSortEvidence,
) -> WalletVariableValueSortEvidence {
    WalletVariableValueSortEvidence {
        record_count: evidence.record_count,
        initial_run_count: evidence.initial_run_count,
        merge_pass_count: evidence.merge_pass_count,
        peak_accounted_sort_memory_bytes: evidence.peak_accounted_memory_bytes,
        max_accounted_sort_memory_bytes: evidence.max_accounted_memory_bytes,
        peak_temporary_file_bytes: evidence.peak_temporary_file_bytes,
        max_temporary_file_bytes: evidence.max_temporary_file_bytes,
        final_run_file_bytes: evidence.final_run_file_bytes,
    }
}

fn wallet_source_matches_canonical(
    wallet: &zinder_wallet_projection::WalletProjectionReadyEvidence,
    canonical: &CanonicalStoreReadyEvidence,
) -> bool {
    let canonical_sequence_digest =
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            canonical.sequence_digest_version,
            canonical.visible_block_count,
            canonical.visible_sequence_digest,
        );
    wallet.source_position.chain_epoch_id == canonical.visible_epoch
        && wallet.source_position.tip == canonical.visible_tip
        && wallet.source_position.event_sequence == canonical.visible_event_sequence
        && wallet.source_sequence_digest == canonical_sequence_digest
}

fn block_id_summary(block_id: BlockId) -> StorageLifecycleBlockId {
    StorageLifecycleBlockId {
        height: block_id.height.value(),
        hash_hex: hex::encode(block_id.hash.as_bytes()),
    }
}

fn acceptance_thresholds(
    target_seconds: Option<f64>,
    hard_limit_seconds: Option<f64>,
    boundary: &'static str,
) -> Result<Option<AcceptanceThresholds>, BenchError> {
    match (target_seconds, hard_limit_seconds) {
        (None, None) => Ok(None),
        (Some(target_seconds), Some(hard_limit_seconds)) => {
            AcceptanceThresholds::try_from_seconds(target_seconds, hard_limit_seconds).map(Some)
        }
        _ => Err(BenchError::invalid_argument(format!(
            "--{boundary}-target-secs and --{boundary}-hard-limit-secs must be supplied together"
        ))),
    }
}

fn require_nonzero_u64(candidate: u64, flag: &str) -> Result<NonZeroU64, BenchError> {
    NonZeroU64::new(candidate)
        .ok_or_else(|| BenchError::invalid_argument(format!("--{flag} must be greater than zero")))
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the positive finite CPU limit is rounded up and capped to u32 before conversion"
)]
fn logical_core_count_from_limit(cpu_limit_cores: f64) -> NonZeroU32 {
    let logical_core_count = cpu_limit_cores.ceil().min(f64::from(u32::MAX)) as u32;
    NonZeroU32::new(logical_core_count).unwrap_or(NonZeroU32::MIN)
}

fn validate_positive_f64(candidate: Option<f64>, flag: &str) -> Result<(), BenchError> {
    if candidate.is_some_and(|limit| !limit.is_finite() || limit <= 0.0) {
        return Err(BenchError::invalid_argument(format!(
            "{flag} must be finite and greater than zero"
        )));
    }
    Ok(())
}

fn validate_positive_u64(candidate: Option<u64>, flag: &str) -> Result<(), BenchError> {
    if candidate == Some(0) {
        return Err(BenchError::invalid_argument(format!(
            "{flag} must be greater than zero"
        )));
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
        bytes = bytes.saturating_add(entry_bytes);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use clap::Parser;
    use zinder_core::BlockHeight;

    use super::RocksDbStorageLifecycleArgs;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        args: RocksDbStorageLifecycleArgs,
    }

    fn required_args() -> Vec<&'static str> {
        vec![
            "test",
            "--network",
            "zcash-testnet",
            "--json-rpc-addr",
            "http://zebra:18232",
            "--canonical-store",
            "canonical",
            "--wallet-store",
            "wallet",
            "--cpu-limit-cores",
            "10",
            "--memory-limit-bytes",
            "10737418240",
        ]
    }

    #[test]
    fn compose_facing_flags_parse_and_validate() -> Result<(), Box<dyn Error>> {
        let mut argv = required_args();
        argv.extend([
            "--tip-height",
            "42",
            "--request-timeout-secs",
            "60",
            "--max-response-bytes",
            "67108864",
            "--supported-reorg-depth",
            "100",
        ]);
        let cli = TestCli::try_parse_from(argv)?;

        let validated = cli.args.validate()?;
        assert_eq!(validated.fixed_tip_height, Some(BlockHeight::new(42)));
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
            160 * 1024 * 1024
        );
        assert_eq!(
            validated.pipeline_limits.block_prepare_concurrency.get(),
            10
        );
        Ok(())
    }

    #[test]
    fn threshold_flags_are_paired() -> Result<(), Box<dyn Error>> {
        let mut argv = required_args();
        argv.extend(["--wallet-storage-ready-target-secs", "60"]);
        let cli = TestCli::try_parse_from(argv)?;

        let error = cli.args.validate().err().ok_or("missing pair must fail")?;
        assert!(error.to_string().contains(
            "--wallet-storage-ready-target-secs and --wallet-storage-ready-hard-limit-secs"
        ));
        Ok(())
    }

    #[test]
    fn storage_paths_must_be_disjoint() -> Result<(), Box<dyn Error>> {
        let mut argv = required_args();
        let wallet_path_index = argv
            .iter()
            .position(|argument| *argument == "--wallet-store")
            .ok_or("wallet path flag must exist")?
            .saturating_add(1);
        argv[wallet_path_index] = "canonical/wallet";
        let cli = TestCli::try_parse_from(argv)?;

        assert!(cli.args.validate().is_err());
        Ok(())
    }
}
