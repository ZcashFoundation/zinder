//! Configuration loading for the `zinder-ingest` binary.
//!
//! [`IngestCommandConfig`] resolves the phase-driven ingest loop's input
//! (`zinder-ingest --config X` default invocation and `zinder-ingest probe`).

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zinder_core::BlockHeight;
use zinder_ingest::{
    CanonicalConstructionSettings, CanonicalFollowSettings, CanonicalPipelineLimits,
    CanonicalRunOverrides, DEFAULT_RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES,
    DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS, IngestError, IngestRuntimeConfig,
    MempoolIngestSettings, NodeSourceKind, PhaseClassificationConfig, RawBlobPolicy,
    container_memory_budget_bytes,
};
use zinder_runtime::{
    ConfigError, ConfigLoader, IngestControlSection, IngestControlWriterToml, NetworkSection,
    NetworkToml, NodeToml, OpsSection, OpsServerError, OpsToml, ResolvedIngestControlWriter,
    ResolvedRetention, RetentionSection, RetentionToml, RuntimeService, SecuritySection,
    SecurityToml, StorageRoleSection, StorageRoleToml, duration_as_millis_u64,
    guard_optional_serving_bind, require_field, resolve_allow_public_bind,
    resolve_canonical_writer_rocksdb_budget, resolve_ingest_control_writer,
    resolve_materialized_view_writer_rocksdb_budget, resolve_ops_listen_addr, resolve_retention,
};
use zinder_source::{
    DEFAULT_MEMPOOL_MAX_TOTAL_RAW_TRANSACTION_BYTES, DEFAULT_MEMPOOL_MAX_TRANSACTION_COUNT,
    MempoolSourceAdmissionLimits, NodeSection, NodeTarget,
};

use crate::cli::parse::{parse_node_source, parse_poll_interval_ms, parse_reorg_window_blocks};

const DEFAULT_REORG_WINDOW_BLOCKS: u32 = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS;

const DEFAULT_TIP_FOLLOW_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_INGEST_COVERAGE: IngestCoverage = IngestCoverage::Explicit;
const DEFAULT_RAW_BLOB_POLICY: RawBlobPolicy = RawBlobPolicy::None;

/// Fully loaded command configuration for the `zinder-ingest`
/// run (no subcommand and the `probe` subcommand both consume this).
#[derive(Debug)]
pub(crate) struct IngestCommandConfig {
    pub(crate) runtime_config: IngestRuntimeConfig,
    pub(crate) coverage: IngestCoverage,
    pub(crate) explorer_views: bool,
    pub(crate) ingest_control_listen_addr: Option<SocketAddr>,
    pub(crate) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(crate) ingest_control_checkpoint_bearer_token_path: Option<PathBuf>,
    pub(crate) ingest_control_bearer_token: Option<zinder_runtime::BearerToken>,
    pub(crate) ingest_control_checkpoint_bearer_token: Option<zinder_runtime::BearerToken>,
    pub(crate) ingest_control_checkpoint_staging_root: PathBuf,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
    pub(crate) allow_public_bind: bool,
    pub(crate) retention: ResolvedRetention,
}

/// Coverage policy applied to the [`CanonicalRunOverrides`] bootstrap path.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum IngestCoverage {
    /// Use explicitly supplied modifier heights as-is.
    Explicit,
    /// Derive the historical floor needed by native wallet serving. The
    /// ingest loop looks up the checkpoint against the
    /// upstream node before entering the first phase.
    WalletServing,
}

impl IngestCoverage {
    pub(crate) const fn as_kebab_case(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::WalletServing => "wallet-serving",
        }
    }
}

impl Default for IngestCoverage {
    fn default() -> Self {
        DEFAULT_INGEST_COVERAGE
    }
}

/// Command-line overrides for the phase-driven ingest invocation.
#[derive(Debug, Default)]
pub(crate) struct IngestConfigOverrides {
    pub(crate) network: Option<String>,
    pub(crate) node_source: Option<String>,
    pub(crate) json_rpc_addr: Option<String>,
    pub(crate) node_auth_method: Option<String>,
    pub(crate) node_auth_username: Option<String>,
    pub(crate) node_auth_path: Option<PathBuf>,
    pub(crate) storage_path: Option<PathBuf>,
    pub(crate) request_timeout_secs: Option<u64>,
    pub(crate) max_response_bytes: Option<u64>,
    pub(crate) reorg_window_blocks: Option<u32>,
    pub(crate) catchup_threshold_blocks: Option<u32>,
    pub(crate) source_segment_max_blocks: Option<u32>,
    pub(crate) source_segment_target_response_bytes: Option<u64>,
    pub(crate) source_fetch_max_in_flight_requests: Option<u32>,
    pub(crate) source_fetch_max_in_flight_bytes: Option<u64>,
    pub(crate) block_prepare_concurrency: Option<u32>,
    pub(crate) poll_interval_ms: Option<u64>,
    pub(crate) lag_threshold_blocks: Option<u64>,
    pub(crate) target_height: Option<u32>,
    pub(crate) checkpoint_height: Option<u32>,
    pub(crate) wallet_serving: Option<bool>,
    pub(crate) ingest_control_listen_addr: Option<SocketAddr>,
    pub(crate) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(crate) ingest_control_checkpoint_bearer_token_path: Option<PathBuf>,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
}

/// Error returned while resolving command configuration.
#[derive(Debug, Error)]
pub(crate) enum IngestConfigError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    OpsServer(#[from] OpsServerError),

    #[error(transparent)]
    Ingest(#[from] IngestError),

    #[error(transparent)]
    CanonicalWriter(#[from] zinder_ingest::CanonicalWriterError),
}

/// Loads and validates the phase-driven ingest configuration.
#[allow(
    clippy::too_many_lines,
    reason = "the override chain is one auditable list of TOML keys; splitting it into helpers would scatter the precedence contract across multiple sites."
)]
pub(crate) fn load_ingest_config(
    config_path: Option<PathBuf>,
    overrides: IngestConfigOverrides,
) -> Result<IngestCommandConfig, IngestConfigError> {
    let raw_config: IngestConfig = ConfigLoader::new()
        // Storage default matches the canonical Zinder layout. The writer's
        // primary store lives at `/var/lib/zinder/store`. Operators on
        // non-PaaS hosts override via `ZINDER_STORAGE__PATH` env var or the
        // `--storage-path` CLI flag.
        .with_default("storage.path", "/var/lib/zinder/store")?
        .with_default("ingest.reorg_window_blocks", DEFAULT_REORG_WINDOW_BLOCKS)?
        .with_default(
            "ingest.mempool.max_transaction_count",
            DEFAULT_MEMPOOL_MAX_TRANSACTION_COUNT.get(),
        )?
        .with_default(
            "ingest.mempool.max_total_raw_transaction_bytes",
            DEFAULT_MEMPOOL_MAX_TOTAL_RAW_TRANSACTION_BYTES.get(),
        )?
        .with_default(
            "ingest.mempool.reconciliation_batch_target_raw_transaction_bytes",
            DEFAULT_RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES,
        )?
        .with_default(
            "ingest.follow.poll_interval_ms",
            DEFAULT_TIP_FOLLOW_POLL_INTERVAL_MS,
        )?
        .with_default(
            "ingest.follow.lag_threshold_blocks",
            DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS,
        )?
        .with_default(
            "ingest.follow.max_replacement_bytes",
            zinder_ingest::DEFAULT_MAX_REPLACEMENT_BYTES.get(),
        )?
        .with_default(
            "ingest.run_overrides.coverage",
            DEFAULT_INGEST_COVERAGE.as_kebab_case(),
        )?
        .with_ops_section(RuntimeService::Ingest)?
        .with_security_section()?
        .with_file(config_path)
        .with_zinder_env()?
        .with_override_if("network.name", overrides.network)?
        .with_override_if("ingest.source", overrides.node_source)?
        .with_override_if("node.json_rpc_addr", overrides.json_rpc_addr)?
        .with_override_if("node.auth.method", overrides.node_auth_method)?
        .with_override_if("node.auth.username", overrides.node_auth_username)?
        .with_override_path_if("node.auth.path", overrides.node_auth_path)?
        .with_override_path_if("storage.path", overrides.storage_path)?
        .with_override_if("node.request_timeout_secs", overrides.request_timeout_secs)?
        .with_override_if("node.max_response_bytes", overrides.max_response_bytes)?
        .with_override_if("ingest.reorg_window_blocks", overrides.reorg_window_blocks)?
        .with_override_if(
            "ingest.phase_classification.catchup_threshold_blocks",
            overrides.catchup_threshold_blocks,
        )?
        .with_override_if(
            "ingest.construction.source_segment_max_blocks",
            overrides.source_segment_max_blocks,
        )?
        .with_override_if(
            "ingest.construction.source_segment_target_response_bytes",
            overrides.source_segment_target_response_bytes,
        )?
        .with_override_if(
            "ingest.construction.source_fetch_max_in_flight_requests",
            overrides.source_fetch_max_in_flight_requests,
        )?
        .with_override_if(
            "ingest.construction.source_fetch_max_in_flight_bytes",
            overrides.source_fetch_max_in_flight_bytes,
        )?
        .with_override_if(
            "ingest.construction.block_prepare_concurrency",
            overrides.block_prepare_concurrency,
        )?
        .with_override_if("ingest.follow.poll_interval_ms", overrides.poll_interval_ms)?
        .with_override_if(
            "ingest.follow.lag_threshold_blocks",
            overrides.lag_threshold_blocks,
        )?
        .with_override_if(
            "ingest.run_overrides.target_height",
            overrides.target_height,
        )?
        .with_override_if(
            "ingest.run_overrides.checkpoint_height",
            overrides.checkpoint_height,
        )?
        .with_override_if(
            "ingest.run_overrides.coverage",
            (overrides.wallet_serving == Some(true))
                .then_some(IngestCoverage::WalletServing.as_kebab_case()),
        )?
        .with_override_if(
            "ingest_control.listen_addr",
            overrides
                .ingest_control_listen_addr
                .map(|addr| addr.to_string()),
        )?
        .with_override_path_if(
            "ingest_control.bearer_token_path",
            overrides.ingest_control_bearer_token_path,
        )?
        .with_override_path_if(
            "ingest_control.checkpoint_bearer_token_path",
            overrides.ingest_control_checkpoint_bearer_token_path,
        )?
        .with_override_if(
            "ops.listen_addr",
            overrides.ops_listen_addr.map(|addr| addr.to_string()),
        )?
        .load()?;

    resolve_ingest_config(raw_config)
}

/// Renders the effective ingest configuration in the accepted TOML
/// shape.
pub(crate) fn redacted_ingest_config_toml(
    config: &IngestCommandConfig,
) -> Result<String, IngestConfigError> {
    let rendered = toml::to_string(&RedactedIngestConfigToml::from_ingest_config(config))
        .map_err(|source| ConfigError::Render { source })?;
    Ok(rendered)
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestConfig {
    network: NetworkSection,
    ops: OpsSection,
    node: NodeSection,
    storage: IngestPrimaryStorageSection,
    ingest: IngestSection,
    ingest_control: IngestControlSection,
    retention: RetentionSection,
    security: SecuritySection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestPrimaryStorageSection {
    path: Option<PathBuf>,
    canonical: StorageRoleSection,
    materialized_views: StorageRoleSection,
    raw_blob_policy: Option<RawBlobPolicy>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestSection {
    /// Canonical source-adapter selector. The supported value is
    /// `zebra-json-rpc`.
    source: Option<String>,
    /// Whether the complete bundled Explorer materialized-view workload is
    /// requested. Omitted values remain false for every coverage.
    explorer_views: bool,
    /// Chain-truth invariant: how deep into the upstream tip the
    /// settled-tip cliff sits. Defaults to `100`.
    reorg_window_blocks: Option<u32>,
    /// Phase classifier knobs for `zinder-ingest probe`.
    phase_classification: IngestPhaseClassificationSection,
    /// Source-fetch and block-preparation knobs for canonical construction.
    construction: IngestConstructionSection,
    /// Live mempool admission and durable reconciliation limits.
    mempool: IngestMempoolSection,
    /// Canonical follower knobs.
    follow: IngestFollowSection,
    /// One-shot `run_overrides` for the ingest run.
    run_overrides: IngestRunOverridesSection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestMempoolSection {
    max_transaction_count: Option<u32>,
    max_total_raw_transaction_bytes: Option<u64>,
    reconciliation_batch_target_raw_transaction_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestPhaseClassificationSection {
    catchup_threshold_blocks: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestConstructionSection {
    source_segment_max_blocks: Option<u32>,
    source_segment_target_response_bytes: Option<u64>,
    source_fetch_max_in_flight_requests: Option<u32>,
    source_fetch_max_in_flight_bytes: Option<u64>,
    block_prepare_concurrency: Option<u32>,
    block_prepare_memory_watermark_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestFollowSection {
    poll_interval_ms: Option<u64>,
    lag_threshold_blocks: Option<u64>,
    max_replacement_bytes: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestRunOverridesSection {
    target_height: Option<u32>,
    checkpoint_height: Option<u32>,
    coverage: Option<IngestCoverage>,
}

const fn node_source_name(node_source: NodeSourceKind) -> &'static str {
    match node_source {
        NodeSourceKind::ZebraJsonRpc => "zebra-json-rpc",
    }
}

fn nonzero_u32_config(amount: Option<u32>, path: &'static str) -> Result<NonZeroU32, ConfigError> {
    let amount = require_field(amount, path)?;
    NonZeroU32::new(amount)
        .ok_or_else(|| ConfigError::invalid(format!("{path} must be greater than zero")))
}

fn nonzero_u64_config(amount: Option<u64>, path: &'static str) -> Result<NonZeroU64, ConfigError> {
    let amount = require_field(amount, path)?;
    NonZeroU64::new(amount)
        .ok_or_else(|| ConfigError::invalid(format!("{path} must be greater than zero")))
}

fn optional_nonzero_u64_config(
    amount: Option<u64>,
    path: &'static str,
) -> Result<Option<NonZeroU64>, ConfigError> {
    amount
        .map(|amount| {
            NonZeroU64::new(amount)
                .ok_or_else(|| ConfigError::invalid(format!("{path} must be greater than zero")))
        })
        .transpose()
}

fn optional_nonzero_u32_config(
    amount: Option<u32>,
    path: &'static str,
) -> Result<Option<NonZeroU32>, ConfigError> {
    amount
        .map(|amount| {
            NonZeroU32::new(amount)
                .ok_or_else(|| ConfigError::invalid(format!("{path} must be greater than zero")))
        })
        .transpose()
}

fn available_logical_core_count() -> NonZeroU32 {
    let logical_core_count =
        std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get);
    NonZeroU32::new(u32::try_from(logical_core_count).unwrap_or(u32::MAX))
        .unwrap_or(NonZeroU32::MIN)
}

#[allow(
    clippy::too_many_lines,
    reason = "the ingest resolver composes the network, source, storage, phase, construction, follow, and modifier knobs in one auditable validation sequence."
)]
fn resolve_ingest_config(config: IngestConfig) -> Result<IngestCommandConfig, IngestConfigError> {
    let network = config.network.resolve()?;
    let node_target = NodeTarget::resolve(network, config.node).map_err(ConfigError::from)?;
    let resolved_pipeline_limits = CanonicalPipelineLimits::resolve(
        container_memory_budget_bytes().and_then(NonZeroU64::new),
        available_logical_core_count(),
        node_target.max_response_bytes,
    );
    let node_source_text = config
        .ingest
        .source
        .clone()
        .unwrap_or_else(|| node_source_name(NodeSourceKind::ZebraJsonRpc).to_owned());
    let node_source = parse_node_source(&node_source_text)?;
    let configured_raw_blob_policy = config.storage.raw_blob_policy;
    let storage_path = require_field(config.storage.path, "storage.path")?;
    let canonical_rocksdb_budget =
        resolve_canonical_writer_rocksdb_budget(config.storage.canonical.rocksdb)?;
    let materialized_view_rocksdb_budget =
        resolve_materialized_view_writer_rocksdb_budget(config.storage.materialized_views.rocksdb)?;

    let reorg_window_blocks = require_field(
        config.ingest.reorg_window_blocks,
        "ingest.reorg_window_blocks",
    )?;
    let reorg_window_blocks = parse_reorg_window_blocks(reorg_window_blocks)?;

    let catchup_threshold_blocks = config
        .ingest
        .phase_classification
        .catchup_threshold_blocks
        .unwrap_or(reorg_window_blocks);

    let mempool_max_transaction_count = nonzero_u32_config(
        config.ingest.mempool.max_transaction_count,
        "ingest.mempool.max_transaction_count",
    )?;
    let mempool_max_total_raw_transaction_bytes = nonzero_u64_config(
        config.ingest.mempool.max_total_raw_transaction_bytes,
        "ingest.mempool.max_total_raw_transaction_bytes",
    )?;
    let reconciliation_batch_target_raw_transaction_bytes = nonzero_u64_config(
        config
            .ingest
            .mempool
            .reconciliation_batch_target_raw_transaction_bytes,
        "ingest.mempool.reconciliation_batch_target_raw_transaction_bytes",
    )?;

    let source_segment_max_blocks = optional_nonzero_u32_config(
        config.ingest.construction.source_segment_max_blocks,
        "ingest.construction.source_segment_max_blocks",
    )?
    .unwrap_or(resolved_pipeline_limits.source_segment_max_blocks);
    let block_prepare_concurrency = optional_nonzero_u32_config(
        config.ingest.construction.block_prepare_concurrency,
        "ingest.construction.block_prepare_concurrency",
    )?
    .unwrap_or(resolved_pipeline_limits.block_prepare_concurrency);
    let block_prepare_memory_watermark_bytes = optional_nonzero_u64_config(
        config
            .ingest
            .construction
            .block_prepare_memory_watermark_bytes,
        "ingest.construction.block_prepare_memory_watermark_bytes",
    )?
    .unwrap_or(resolved_pipeline_limits.block_prepare_memory_watermark_bytes);
    let source_segment_target_response_bytes = optional_nonzero_u64_config(
        config
            .ingest
            .construction
            .source_segment_target_response_bytes,
        "ingest.construction.source_segment_target_response_bytes",
    )?
    .unwrap_or(resolved_pipeline_limits.source_segment_target_response_bytes);
    let source_fetch_max_in_flight_requests = optional_nonzero_u32_config(
        config
            .ingest
            .construction
            .source_fetch_max_in_flight_requests,
        "ingest.construction.source_fetch_max_in_flight_requests",
    )?
    .unwrap_or(resolved_pipeline_limits.source_fetch_max_in_flight_requests);
    let source_fetch_max_in_flight_bytes = optional_nonzero_u64_config(
        config.ingest.construction.source_fetch_max_in_flight_bytes,
        "ingest.construction.source_fetch_max_in_flight_bytes",
    )?
    .unwrap_or(resolved_pipeline_limits.source_fetch_max_in_flight_bytes);
    let pipeline_limits = CanonicalPipelineLimits {
        max_response_bytes: node_target.max_response_bytes,
        source_segment_target_response_bytes,
        source_segment_max_blocks,
        source_fetch_max_in_flight_requests,
        source_fetch_max_in_flight_bytes,
        block_prepare_concurrency,
        block_prepare_memory_watermark_bytes,
    }
    .validate()
    .map_err(|error| {
        ConfigError::invalid(format!(
            "invalid ingest.construction pipeline limits: {error}"
        ))
    })?;

    let poll_interval_ms = require_field(
        config.ingest.follow.poll_interval_ms,
        "ingest.follow.poll_interval_ms",
    )?;
    let poll_interval = parse_poll_interval_ms(poll_interval_ms)?;

    let lag_threshold_blocks = require_field(
        config.ingest.follow.lag_threshold_blocks,
        "ingest.follow.lag_threshold_blocks",
    )?;
    let max_replacement_bytes = NonZeroU64::new(require_field(
        config.ingest.follow.max_replacement_bytes,
        "ingest.follow.max_replacement_bytes",
    )?)
    .ok_or_else(|| {
        ConfigError::invalid("ingest.follow.max_replacement_bytes must be greater than zero")
    })?;

    let coverage = config.ingest.run_overrides.coverage.unwrap_or_default();
    let raw_blob_policy = resolve_raw_blob_policy(coverage, configured_raw_blob_policy)?;
    let explorer_views = resolve_explorer_views(
        config.ingest.explorer_views,
        config.ingest.run_overrides.checkpoint_height,
    )?;

    if matches!(coverage, IngestCoverage::WalletServing)
        && config.ingest.run_overrides.checkpoint_height.is_some()
    {
        return Err(ConfigError::invalid(
            "ingest.run_overrides.coverage = \"wallet-serving\" requires complete transparent history and sets checkpoint_height to zero; remove ingest.run_overrides.checkpoint_height",
        )
        .into());
    }

    let ResolvedIngestControlWriter {
        listen_addr: ingest_control_listen_addr,
        bearer_token_path: ingest_control_bearer_token_path,
        bearer_token: ingest_control_bearer_token,
        checkpoint_bearer_token: ingest_control_checkpoint_bearer_token,
        checkpoint_bearer_token_path: ingest_control_checkpoint_bearer_token_path,
        checkpoint_staging_root: ingest_control_checkpoint_staging_root,
    } = resolve_ingest_control_writer(config.ingest_control)?;
    let retention = resolve_retention(config.retention)?;
    let ops_listen_addr = resolve_ops_listen_addr(config.ops)?;
    let allow_public_bind = resolve_allow_public_bind(config.security)?;
    guard_optional_serving_bind(
        "ingest_control.listen_addr",
        ingest_control_listen_addr,
        allow_public_bind,
    )?;
    guard_ingest_control_bearer_token(
        ingest_control_listen_addr,
        ingest_control_bearer_token.as_ref(),
    )?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;
    let run_overrides = CanonicalRunOverrides {
        target_height: config
            .ingest
            .run_overrides
            .target_height
            .map(BlockHeight::new),
        checkpoint_height: config
            .ingest
            .run_overrides
            .checkpoint_height
            .map(BlockHeight::new),
    };

    let runtime_config = IngestRuntimeConfig {
        node: node_target,
        node_source,
        storage_path,
        canonical_rocksdb_budget,
        materialized_view_rocksdb_budget,
        raw_blob_policy,
        reorg_window_blocks,
        phase_classification: PhaseClassificationConfig {
            catchup_threshold_blocks,
        },
        construction: CanonicalConstructionSettings { pipeline_limits },
        mempool: MempoolIngestSettings {
            source_admission_limits: MempoolSourceAdmissionLimits {
                max_transaction_count: mempool_max_transaction_count,
                max_total_raw_transaction_bytes: mempool_max_total_raw_transaction_bytes,
            },
            reconciliation_batch_target_raw_transaction_bytes,
        },
        follow: CanonicalFollowSettings {
            poll_interval,
            lag_threshold_blocks,
            max_replacement_bytes,
        },
        run_overrides,
    };

    Ok(IngestCommandConfig {
        runtime_config,
        coverage,
        explorer_views,
        ingest_control_listen_addr,
        ingest_control_bearer_token_path,
        ingest_control_checkpoint_bearer_token_path,
        ingest_control_bearer_token,
        ingest_control_checkpoint_bearer_token,
        ingest_control_checkpoint_staging_root,
        ops_listen_addr,
        allow_public_bind,
        retention,
    })
}

fn resolve_explorer_views(
    explorer_views: bool,
    checkpoint_height: Option<u32>,
) -> Result<bool, ConfigError> {
    if explorer_views && checkpoint_height.is_some_and(|height| height > 0) {
        return Err(ConfigError::invalid(
            "ingest.explorer_views = true requires genesis-complete canonical history or checkpoint_height = 0; checkpoint heights above zero are not supported for Explorer views, or set explorer_views = false",
        ));
    }
    Ok(explorer_views)
}

fn guard_ingest_control_bearer_token(
    listen_addr: Option<SocketAddr>,
    bearer_token: Option<&zinder_runtime::BearerToken>,
) -> Result<(), ConfigError> {
    if listen_addr.is_some_and(|address| !address.ip().is_loopback()) && bearer_token.is_none() {
        return Err(ConfigError::invalid(
            "ingest_control.listen_addr outside loopback requires ingest_control.bearer_token_path",
        ));
    }
    Ok(())
}

fn resolve_raw_blob_policy(
    coverage: IngestCoverage,
    configured_policy: Option<RawBlobPolicy>,
) -> Result<RawBlobPolicy, ConfigError> {
    match (coverage, configured_policy) {
        (IngestCoverage::WalletServing, None) => Ok(RawBlobPolicy::Transactions),
        (IngestCoverage::WalletServing, Some(RawBlobPolicy::None)) => Err(ConfigError::invalid(
            "ingest.run_overrides.coverage = \"wallet-serving\" requires storage.raw_blob_policy = \"transactions\" or \"all\"; remove storage.raw_blob_policy to use \"transactions\"",
        )),
        (_, Some(raw_blob_policy)) => Ok(raw_blob_policy),
        (_, None) => Ok(DEFAULT_RAW_BLOB_POLICY),
    }
}

#[derive(Serialize)]
struct RedactedIngestConfigToml {
    network: NetworkToml,
    ops: OpsToml,
    security: SecurityToml,
    node: NodeToml,
    storage: IngestStorageToml,
    ingest: IngestToml,
    ingest_control: IngestControlWriterToml,
    retention: RetentionToml,
}

impl RedactedIngestConfigToml {
    #[allow(
        clippy::too_many_lines,
        reason = "redacted print-config mirrors the resolved ingest TOML shape field by field"
    )]
    fn from_ingest_config(config: &IngestCommandConfig) -> Self {
        let runtime_config = &config.runtime_config;
        Self {
            network: NetworkToml::from_network(runtime_config.node.network),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            security: SecurityToml::from_resolved(config.allow_public_bind),
            node: NodeToml::from_node_target(&runtime_config.node),
            storage: IngestStorageToml {
                path: runtime_config.storage_path.display().to_string(),
                raw_blob_policy: runtime_config.raw_blob_policy,
                canonical: StorageRoleToml::from_resolved(runtime_config.canonical_rocksdb_budget),
                materialized_views: StorageRoleToml::from_resolved(
                    runtime_config.materialized_view_rocksdb_budget,
                ),
            },
            ingest: IngestToml {
                source: node_source_name(runtime_config.node_source),
                explorer_views: config.explorer_views,
                reorg_window_blocks: runtime_config.reorg_window_blocks,
                phase_classification: IngestPhaseClassificationToml {
                    catchup_threshold_blocks: runtime_config
                        .phase_classification
                        .catchup_threshold_blocks,
                },
                construction: IngestConstructionToml {
                    source_segment_max_blocks: runtime_config
                        .construction
                        .pipeline_limits
                        .source_segment_max_blocks
                        .get(),
                    source_segment_target_response_bytes: runtime_config
                        .construction
                        .pipeline_limits
                        .source_segment_target_response_bytes
                        .get(),
                    source_fetch_max_in_flight_requests: runtime_config
                        .construction
                        .pipeline_limits
                        .source_fetch_max_in_flight_requests
                        .get(),
                    source_fetch_max_in_flight_bytes: runtime_config
                        .construction
                        .pipeline_limits
                        .source_fetch_max_in_flight_bytes
                        .get(),
                    block_prepare_concurrency: runtime_config
                        .construction
                        .pipeline_limits
                        .block_prepare_concurrency
                        .get(),
                    block_prepare_memory_watermark_bytes: runtime_config
                        .construction
                        .pipeline_limits
                        .block_prepare_memory_watermark_bytes
                        .get(),
                },
                mempool: IngestMempoolToml {
                    max_transaction_count: runtime_config
                        .mempool
                        .source_admission_limits
                        .max_transaction_count
                        .get(),
                    max_total_raw_transaction_bytes: runtime_config
                        .mempool
                        .source_admission_limits
                        .max_total_raw_transaction_bytes
                        .get(),
                    reconciliation_batch_target_raw_transaction_bytes: runtime_config
                        .mempool
                        .reconciliation_batch_target_raw_transaction_bytes
                        .get(),
                },
                follow: IngestFollowToml {
                    poll_interval_ms: duration_as_millis_u64(runtime_config.follow.poll_interval),
                    lag_threshold_blocks: runtime_config.follow.lag_threshold_blocks,
                    max_replacement_bytes: runtime_config.follow.max_replacement_bytes.get(),
                },
                run_overrides: IngestRunOverridesToml {
                    target_height: runtime_config
                        .run_overrides
                        .target_height
                        .map(BlockHeight::value),
                    checkpoint_height: runtime_config
                        .run_overrides
                        .checkpoint_height
                        .map(BlockHeight::value),
                    coverage: config.coverage,
                },
            },
            ingest_control: IngestControlWriterToml::from_resolved(
                config.ingest_control_listen_addr,
                config.ingest_control_bearer_token_path.as_deref(),
                config
                    .ingest_control_checkpoint_bearer_token_path
                    .as_deref(),
                &config.ingest_control_checkpoint_staging_root,
            ),
            retention: RetentionToml::from_resolved(config.retention),
        }
    }
}

#[derive(Serialize)]
struct IngestToml {
    source: &'static str,
    explorer_views: bool,
    reorg_window_blocks: u32,
    phase_classification: IngestPhaseClassificationToml,
    construction: IngestConstructionToml,
    mempool: IngestMempoolToml,
    follow: IngestFollowToml,
    run_overrides: IngestRunOverridesToml,
}

#[derive(Serialize)]
struct IngestMempoolToml {
    max_transaction_count: u32,
    max_total_raw_transaction_bytes: u64,
    reconciliation_batch_target_raw_transaction_bytes: u64,
}

#[derive(Serialize)]
struct IngestStorageToml {
    path: String,
    raw_blob_policy: RawBlobPolicy,
    canonical: StorageRoleToml,
    materialized_views: StorageRoleToml,
}

#[derive(Serialize)]
struct IngestPhaseClassificationToml {
    catchup_threshold_blocks: u32,
}

#[derive(Serialize)]
struct IngestConstructionToml {
    source_segment_max_blocks: u32,
    source_segment_target_response_bytes: u64,
    source_fetch_max_in_flight_requests: u32,
    source_fetch_max_in_flight_bytes: u64,
    block_prepare_concurrency: u32,
    block_prepare_memory_watermark_bytes: u64,
}

#[derive(Serialize)]
struct IngestFollowToml {
    poll_interval_ms: u64,
    lag_threshold_blocks: u64,
    max_replacement_bytes: u64,
}

#[derive(Serialize)]
struct IngestRunOverridesToml {
    #[serde(skip_serializing_if = "Option::is_none")]
    target_height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_height: Option<u32>,
    coverage: IngestCoverage,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_loopback_ingest_control_requires_bearer_token() {
        let error =
            guard_ingest_control_bearer_token(Some(SocketAddr::from(([0, 0, 0, 0], 8240))), None)
                .err()
                .unwrap_or_else(|| ConfigError::invalid("public ingest control was accepted"));

        assert!(
            error
                .to_string()
                .contains("outside loopback requires ingest_control.bearer_token_path")
        );
        assert!(guard_ingest_control_bearer_token(
            Some(SocketAddr::from(([127, 0, 0, 1], 8240))),
            None,
        )
        .is_ok());
    }

    #[test]
    fn explorer_views_default_false_for_every_coverage() {
        assert!(matches!(resolve_explorer_views(false, None), Ok(false)));
        assert!(matches!(
            resolve_explorer_views(false, Some(100)),
            Ok(false)
        ));
    }

    #[test]
    fn explorer_views_accept_zero_checkpoint_and_reject_positive_checkpoint() {
        assert!(matches!(resolve_explorer_views(true, Some(0)), Ok(true)));
        let error_result = resolve_explorer_views(true, Some(100));
        assert!(error_result.is_err());
        let Some(error) = error_result.err() else {
            return;
        };
        assert!(error.to_string().contains("explorer_views = true"));
    }
}
