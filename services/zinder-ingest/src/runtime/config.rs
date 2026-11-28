//! Configuration loading for the `zinder-ingest` runtime.
//!
//! [`IngestCommandConfig`] resolves the phase-driven ingest loop's input
//! (`zinder-ingest --config X` default invocation and `zinder-ingest probe`).

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroU64},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zinder_core::BlockHeight;
use zinder_runtime::{
    ConfigError, ConfigLoader, DeploymentProfile, DeploymentSection, DeploymentToml,
    IngestControlSection, IngestControlWriterToml, NetworkSection, NetworkToml, NodeFallbacksToml,
    NodeToml, OpsSection, OpsServerError, OpsToml, ResolvedIngestControlWriter, ResolvedRetention,
    RetentionSection, RetentionToml, RuntimeService, SecuritySection, SecurityToml,
    StorageRoleSection, StorageRoleToml, derive_canonical_path, duration_as_millis_u64,
    guard_optional_serving_bind, require_field, resolve_allow_public_bind,
    resolve_canonical_writer_rocksdb_budget, resolve_ingest_control_writer_with_data_dir,
    resolve_manual_raw_blob_retention, resolve_materialized_view_writer_rocksdb_budget,
    resolve_ops_listen_addr, resolve_retention, section_defaults::DEFAULT_DATA_DIR,
    validate_optional_data_dir,
};
use zinder_source::{
    DEFAULT_MEMPOOL_MAX_TOTAL_RAW_TRANSACTION_BYTES, DEFAULT_MEMPOOL_MAX_TRANSACTION_COUNT,
    MempoolSourceAdmissionLimits, NodeFallbackSections, NodeSection, NodeTarget,
    ZebraIndexerSourceTarget,
};
use zinder_store::{RawBlobRetention, TreeStateCheckpointSelection};

use crate::{
    CanonicalConstructionSettings, CanonicalFollowSettings, CanonicalPipelineLimits,
    CanonicalRunOverrides, DEFAULT_EXPLORER_VIEWS_MIN_FREE_BYTES,
    DEFAULT_RECONCILIATION_BATCH_TARGET_RAW_TRANSACTION_BYTES,
    DEFAULT_TIP_FOLLOW_LAG_THRESHOLD_BLOCKS, DEFAULT_TREE_STATE_COMMITMENT_BUDGET, IngestError,
    IngestRuntimeConfig, MempoolIngestSettings, NodeSourceKind, PhaseClassificationConfig,
    RawBlobPolicy, TreeStateDensificationConfig, TreeStateVerificationConfig,
    container_memory_budget_bytes,
};

use super::cli::parse::{parse_node_source, parse_poll_interval_ms, parse_reorg_window_blocks};

const DEFAULT_REORG_WINDOW_BLOCKS: u32 = zinder_store::DEFAULT_REORG_WINDOW_BLOCKS;

const DEFAULT_TIP_FOLLOW_POLL_INTERVAL_MS: u64 = 1_000;
const DEFAULT_INGEST_COVERAGE: IngestCoverage = IngestCoverage::Explicit;
const DEFAULT_RAW_BLOB_POLICY: RawBlobPolicy = RawBlobPolicy::None;

/// Fully loaded command configuration for the `zinder-ingest`
/// run (no subcommand and the `probe` subcommand both consume this).
#[derive(Debug)]
pub struct IngestCommandConfig {
    profile: Option<DeploymentProfile>,
    /// Configured `storage.data_dir`, when set.
    pub storage_data_dir: Option<PathBuf>,
    /// Configured `storage.full_blocks` override, when set.
    pub storage_full_blocks: Option<bool>,
    /// Resolved core ingest runtime configuration.
    pub runtime_config: IngestRuntimeConfig,
    /// Selected bootstrap coverage policy.
    pub coverage: IngestCoverage,
    /// Whether complete bundled Explorer materialized views are enabled.
    pub explorer_views: bool,
    /// Free-space floor, in bytes, below which Explorer view replay pauses.
    pub explorer_views_min_free_bytes: u64,
    /// Optional `IngestControl` gRPC listen address.
    pub ingest_control_listen_addr: Option<SocketAddr>,
    /// Path to the shared-secret bearer token file for `IngestControl`.
    pub ingest_control_bearer_token_path: Option<PathBuf>,
    /// Path to the checkpoint bearer token file for `IngestControl`.
    pub ingest_control_checkpoint_bearer_token_path: Option<PathBuf>,
    /// Loaded bearer token enforced on `IngestControl` RPCs.
    pub ingest_control_bearer_token: Option<zinder_runtime::BearerToken>,
    /// Loaded bearer token enforced on checkpoint creation RPCs.
    pub ingest_control_checkpoint_bearer_token: Option<zinder_runtime::BearerToken>,
    /// Staging root used by owner checkpoints.
    pub ingest_control_checkpoint_staging_root: PathBuf,
    /// Operational HTTP endpoint address, or `None` when disabled.
    pub ops_listen_addr: Option<SocketAddr>,
    /// Optional trace export settings.
    pub observability: zinder_runtime::ObservabilityConfig,
    /// Whether non-loopback listener binds are allowed.
    pub allow_public_bind: bool,
    /// Retained event-history and mempool cleanup schedule.
    pub retention: ResolvedRetention,
    /// Background tree-state checkpoint densification settings.
    pub tree_state_densification: TreeStateDensificationConfig,
    /// Background tree-state checkpoint verification settings.
    pub tree_state_verification: TreeStateVerificationConfig,
}

/// Coverage policy applied to the [`CanonicalRunOverrides`] bootstrap path.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum IngestCoverage {
    /// Use explicitly supplied modifier heights as-is.
    Explicit,
    /// Derive the historical floor needed by native wallet serving. The
    /// ingest loop looks up the checkpoint against the
    /// upstream node before entering the first phase.
    WalletServing,
}

impl IngestCoverage {
    /// Returns the kebab-case configuration representation of this coverage mode.
    #[must_use]
    pub const fn as_kebab_case(self) -> &'static str {
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
pub struct IngestConfigOverrides {
    /// Override for `network.name`.
    pub network: Option<String>,
    /// Override for `ingest.source`.
    pub node_source: Option<String>,
    /// Override for `node.json_rpc_addr`.
    pub json_rpc_addr: Option<String>,
    /// Override for `node.auth.method`.
    pub node_auth_method: Option<String>,
    /// Override for `node.auth.username`.
    pub node_auth_username: Option<String>,
    /// Override for `node.auth.path`.
    pub node_auth_path: Option<PathBuf>,
    /// Override for `storage.path`.
    pub storage_path: Option<PathBuf>,
    /// Override for `node.request_timeout_secs`.
    pub request_timeout_secs: Option<u64>,
    /// Override for `node.max_response_bytes`.
    pub max_response_bytes: Option<u64>,
    /// Override for `ingest.reorg_window_blocks`.
    pub reorg_window_blocks: Option<u32>,
    /// Override for `ingest.phase_classification.catchup_threshold_blocks`.
    pub catchup_threshold_blocks: Option<u32>,
    /// Override for `ingest.construction.source_segment_max_blocks`.
    pub source_segment_max_blocks: Option<u32>,
    /// Override for `ingest.construction.source_segment_target_response_bytes`.
    pub source_segment_target_response_bytes: Option<u64>,
    /// Override for `ingest.construction.source_fetch_max_in_flight_requests`.
    pub source_fetch_max_in_flight_requests: Option<u32>,
    /// Override for `ingest.construction.source_fetch_max_in_flight_bytes`.
    pub source_fetch_max_in_flight_bytes: Option<u64>,
    /// Override for `ingest.construction.block_prepare_concurrency`.
    pub block_prepare_concurrency: Option<u32>,
    /// Override for `ingest.follow.poll_interval_ms`.
    pub poll_interval_ms: Option<u64>,
    /// Override for `ingest.follow.lag_threshold_blocks`.
    pub lag_threshold_blocks: Option<u64>,
    /// Override for `ingest.run_overrides.target_height`.
    pub target_height: Option<u32>,
    /// Override for `ingest.run_overrides.checkpoint_height`.
    pub checkpoint_height: Option<u32>,
    /// Override selecting `wallet-serving` coverage when `Some(true)`.
    pub wallet_serving: Option<bool>,
    /// Override for `ingest_control.listen_addr`.
    pub ingest_control_listen_addr: Option<SocketAddr>,
    /// Override for `ingest_control.bearer_token_path`.
    pub ingest_control_bearer_token_path: Option<PathBuf>,
    /// Override for `ingest_control.checkpoint_bearer_token_path`.
    pub ingest_control_checkpoint_bearer_token_path: Option<PathBuf>,
    /// Override for `ops.listen_addr`.
    pub ops_listen_addr: Option<SocketAddr>,
}

/// Error returned while resolving command configuration or running ingest.
#[derive(Debug, Error)]
pub enum IngestConfigError {
    /// Layered configuration loading or validation failed.
    #[error(transparent)]
    Config(#[from] ConfigError),

    /// Operational HTTP endpoint setup or shutdown failed.
    #[error(transparent)]
    OpsServer(#[from] OpsServerError),

    /// Ingest domain or source error.
    #[error(transparent)]
    Ingest(#[from] IngestError),

    /// Canonical writer admission, construction, or tip-follow error.
    #[error(transparent)]
    CanonicalWriter(#[from] crate::CanonicalWriterError),

    /// Upstream node endpoint admission failed.
    #[error(transparent)]
    NodeEndpoints(#[from] crate::NoNodeEndpointAdmitted),
}

/// Loads and validates the phase-driven ingest configuration.
pub fn load_ingest_config(
    config_path: Option<PathBuf>,
    overrides: IngestConfigOverrides,
) -> Result<IngestCommandConfig, IngestConfigError> {
    load_ingest_config_with_env(config_path, overrides, std::env::vars())
}

/// Loads and validates the phase-driven ingest configuration using explicit
/// `ZINDER_*` environment pairs.
#[allow(
    clippy::too_many_lines,
    reason = "the override chain is one auditable list of TOML keys; splitting it into helpers would scatter the precedence contract across multiple sites."
)]
pub fn load_ingest_config_with_env<I>(
    config_path: Option<PathBuf>,
    overrides: IngestConfigOverrides,
    environment: I,
) -> Result<IngestCommandConfig, IngestConfigError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let raw_config: IngestConfig = ConfigLoader::new()
        .with_default("ingest.reorg_window_blocks", DEFAULT_REORG_WINDOW_BLOCKS)?
        .with_default(
            "ingest.explorer_views_min_free_bytes",
            DEFAULT_EXPLORER_VIEWS_MIN_FREE_BYTES,
        )?
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
            crate::DEFAULT_MAX_REPLACEMENT_BYTES.get(),
        )?
        .with_ops_section(RuntimeService::Ingest)?
        .with_security_section()?
        .with_file(config_path)
        .with_zinder_env_from_map(environment)?
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
pub fn redacted_ingest_config_toml(
    config: &IngestCommandConfig,
) -> Result<String, IngestConfigError> {
    let rendered = toml::to_string(&RedactedIngestConfigToml::from_ingest_config(config))
        .map_err(|source| ConfigError::Render { source })?;
    Ok(rendered)
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestConfig {
    deployment: DeploymentSection,
    network: NetworkSection,
    ops: OpsSection,
    observability: zinder_runtime::ObservabilityConfig,
    node: NodeSection,
    node_fallbacks: NodeFallbackSections,
    storage: IngestPrimaryStorageSection,
    ingest: IngestSection,
    ingest_control: IngestControlSection,
    retention: RetentionSection,
    security: SecuritySection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestPrimaryStorageSection {
    data_dir: Option<PathBuf>,
    path: Option<PathBuf>,
    canonical: StorageRoleSection,
    materialized_views: StorageRoleSection,
    raw_blob_policy: Option<RawBlobPolicy>,
    full_blocks: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestSection {
    /// Canonical source-adapter selector (`zebra-json-rpc` or
    /// `zebra-indexer-grpc`).
    source: Option<String>,
    /// Whether the complete bundled Explorer materialized-view workload is
    /// requested. Omitted values remain false unless `deployment.profile = "explorer"`.
    explorer_views: Option<bool>,
    /// Available bytes on the store volume below which Explorer view replay
    /// pauses. Zero disables the guard.
    explorer_views_min_free_bytes: Option<u64>,
    /// Chain-truth invariant: how deep into the upstream tip the
    /// settled-tip cliff sits. Defaults to
    /// `zinder_store::DEFAULT_REORG_WINDOW_BLOCKS`.
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
    /// Background densification of settled tree-state checkpoints.
    tree_state_densification: IngestTreeStateDensificationSection,
    /// Background verification of stored tree-state checkpoints.
    tree_state_verification: IngestTreeStateVerificationSection,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestTreeStateDensificationSection {
    enabled: Option<bool>,
    during_construction: Option<bool>,
    selection: Option<TreeStateSelectionName>,
    commitment_budget: Option<u32>,
    workers: Option<u32>,
    max_blocks_per_second: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestTreeStateVerificationSection {
    enabled: Option<bool>,
    max_rows_per_second: Option<u32>,
}

/// Configured name of a tree-state checkpoint selection.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum TreeStateSelectionName {
    EveryBlock,
    CommitmentBudget,
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
    tree_hashing_workers: Option<u32>,
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
        NodeSourceKind::ZebraIndexerGrpc => "zebra-indexer-grpc",
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

/// Returns the smallest and the largest `max_response_bytes` of the ordered
/// node list.
///
/// Startup can make any endpoint active, and each endpoint's client accepts
/// responses up to its own limit. Source fetch reserves and admits responses
/// against the largest, so the in-flight byte bound holds on every endpoint,
/// and aims responses at the smallest, so no endpoint refuses the default
/// target.
fn node_response_byte_range(
    node_target: &NodeTarget,
    node_fallbacks: &[NodeTarget],
) -> (NonZeroU64, NonZeroU64) {
    node_fallbacks
        .iter()
        .map(|fallback| fallback.max_response_bytes)
        .fold(
            (
                node_target.max_response_bytes,
                node_target.max_response_bytes,
            ),
            |(smallest, largest), limit| (smallest.min(limit), largest.max(limit)),
        )
}

/// The logical CPUs this process may use, or `None` when the platform
/// cannot tell.
fn detected_logical_core_count() -> Option<NonZeroU32> {
    std::thread::available_parallelism()
        .ok()
        .and_then(|cpus| NonZeroU32::new(u32::try_from(cpus.get()).unwrap_or(u32::MAX)))
}

fn validate_node_source_targets(
    node_source: NodeSourceKind,
    primary: &NodeTarget,
    fallbacks: &[NodeTarget],
) -> Result<(), ConfigError> {
    validate_node_source_target(node_source, "node", primary)?;
    for (index, fallback) in fallbacks.iter().enumerate() {
        let section = format!("node_fallbacks.{}", index + 1);
        validate_node_source_target(node_source, &section, fallback)?;
    }
    Ok(())
}

fn validate_node_source_target(
    node_source: NodeSourceKind,
    section: &str,
    target: &NodeTarget,
) -> Result<(), ConfigError> {
    match node_source {
        NodeSourceKind::ZebraJsonRpc => Ok(()),
        NodeSourceKind::ZebraIndexerGrpc => {
            if ZebraIndexerSourceTarget::from_node_target(target).is_some() {
                Ok(())
            } else {
                Err(ConfigError::invalid(format!(
                    "ingest.source = \"zebra-indexer-grpc\" requires {section}.indexer_grpc_addr"
                )))
            }
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the ingest resolver composes the network, source, storage, phase, construction, follow, and modifier knobs in one auditable validation sequence."
)]
fn resolve_ingest_config(
    mut config: IngestConfig,
) -> Result<IngestCommandConfig, IngestConfigError> {
    apply_deployment_profile(&mut config)?;
    let network = config.network.resolve()?;
    let node_target = NodeTarget::resolve(network, config.node).map_err(ConfigError::from)?;
    let node_fallbacks =
        NodeTarget::resolve_fallbacks(network, config.node_fallbacks).map_err(ConfigError::from)?;
    let (smallest_node_response_bytes, largest_node_response_bytes) =
        node_response_byte_range(&node_target, &node_fallbacks);
    let coverage = config.ingest.run_overrides.coverage.unwrap_or_default();
    let explorer_views = resolve_explorer_views(
        config.ingest.explorer_views.unwrap_or(false),
        config.ingest.run_overrides.checkpoint_height,
    )?;
    let mut densification = config.ingest.tree_state_densification;
    if matches!(coverage, IngestCoverage::WalletServing) && !explorer_views {
        densification.during_construction.get_or_insert(true);
    }
    let tree_state_densification = resolve_tree_state_densification(densification)?;
    let detected_logical_cores = detected_logical_core_count();
    let logical_core_count =
        detected_logical_cores.unwrap_or_else(|| NonZeroU32::new(8).unwrap_or(NonZeroU32::MIN));
    let mut resolved_pipeline_limits = CanonicalPipelineLimits::resolve(
        container_memory_budget_bytes().and_then(NonZeroU64::new),
        logical_core_count,
        largest_node_response_bytes,
    );
    if tree_state_densification.construction_selection().is_some() {
        resolved_pipeline_limits =
            resolved_pipeline_limits.with_checkpoint_hashing(detected_logical_cores);
    }
    let node_source_text = config
        .ingest
        .source
        .clone()
        .unwrap_or_else(|| node_source_name(NodeSourceKind::ZebraJsonRpc).to_owned());
    let node_source = parse_node_source(&node_source_text)?;
    validate_node_source_targets(node_source, &node_target, &node_fallbacks)?;
    let configured_raw_blob_policy = config.storage.raw_blob_policy;
    let storage_data_dir = validate_optional_data_dir(config.storage.data_dir)?;
    let storage_full_blocks = config.storage.full_blocks;
    let storage_path = if let Some(path) = config.storage.path {
        if path.to_string_lossy().trim().is_empty() {
            return Err(ConfigError::invalid("storage.path must not be blank").into());
        }
        path
    } else {
        let root = storage_data_dir
            .as_deref()
            .unwrap_or_else(|| Path::new(DEFAULT_DATA_DIR));
        derive_canonical_path(root)
    };
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
    .unwrap_or_else(|| {
        resolved_pipeline_limits
            .source_segment_target_response_bytes
            .min(smallest_node_response_bytes)
    });
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
    let tree_hashing_workers = optional_nonzero_u32_config(
        config.ingest.construction.tree_hashing_workers,
        "ingest.construction.tree_hashing_workers",
    )?
    .unwrap_or(resolved_pipeline_limits.tree_hashing_workers);
    let pipeline_limits = CanonicalPipelineLimits {
        max_response_bytes: largest_node_response_bytes,
        source_segment_target_response_bytes,
        source_segment_max_blocks,
        source_fetch_max_in_flight_requests,
        source_fetch_max_in_flight_bytes,
        block_prepare_concurrency,
        block_prepare_memory_watermark_bytes,
        tree_hashing_workers,
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

    let raw_blob_policy = resolve_raw_blob_policy(
        config.deployment.profile,
        storage_full_blocks,
        configured_raw_blob_policy,
    )?;
    let explorer_views_min_free_bytes = require_field(
        config.ingest.explorer_views_min_free_bytes,
        "ingest.explorer_views_min_free_bytes",
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
    } = resolve_ingest_control_writer_with_data_dir(
        config.ingest_control,
        storage_data_dir.as_deref(),
    )?;
    let retention = resolve_retention(config.retention)?;
    let tree_state_verification =
        resolve_tree_state_verification(config.ingest.tree_state_verification)?;
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
        node_fallbacks,
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
        profile: config.deployment.profile,
        storage_data_dir,
        storage_full_blocks,
        runtime_config,
        coverage,
        explorer_views,
        explorer_views_min_free_bytes,
        ingest_control_listen_addr,
        ingest_control_bearer_token_path,
        ingest_control_checkpoint_bearer_token_path,
        ingest_control_bearer_token,
        ingest_control_checkpoint_bearer_token,
        ingest_control_checkpoint_staging_root,
        ops_listen_addr,
        observability: config.observability,
        allow_public_bind,
        retention,
        tree_state_densification,
        tree_state_verification,
    })
}

fn resolve_tree_state_verification(
    section: IngestTreeStateVerificationSection,
) -> Result<TreeStateVerificationConfig, ConfigError> {
    let defaults = TreeStateVerificationConfig::default();
    let max_rows_per_second = match section.max_rows_per_second {
        None => defaults.max_rows_per_second,
        Some(configured) => NonZeroU32::new(configured).ok_or_else(|| {
            ConfigError::invalid(
                "ingest.tree_state_verification.max_rows_per_second must be greater than zero",
            )
        })?,
    };
    Ok(TreeStateVerificationConfig {
        enabled: section.enabled.unwrap_or(defaults.enabled),
        max_rows_per_second,
    })
}

fn resolve_tree_state_densification(
    section: IngestTreeStateDensificationSection,
) -> Result<TreeStateDensificationConfig, ConfigError> {
    let defaults = TreeStateDensificationConfig::default();
    let nonzero = |configured: Option<u32>, default: NonZeroU32, field: &str| {
        configured.map_or(Ok(default), |configured| {
            NonZeroU32::new(configured)
                .ok_or_else(|| ConfigError::invalid(format!("{field} must be greater than zero")))
        })
    };
    let selection = match (section.selection, section.commitment_budget) {
        (None | Some(TreeStateSelectionName::EveryBlock), None) => {
            TreeStateCheckpointSelection::EveryBlock
        }
        (Some(TreeStateSelectionName::EveryBlock), Some(_)) => {
            return Err(ConfigError::invalid(
                "ingest.tree_state_densification.commitment_budget applies only to selection = \"commitment-budget\"",
            ));
        }
        (None | Some(TreeStateSelectionName::CommitmentBudget), budget) => {
            TreeStateCheckpointSelection::CommitmentBudget(nonzero(
                budget,
                NonZeroU32::new(DEFAULT_TREE_STATE_COMMITMENT_BUDGET).unwrap_or(NonZeroU32::MIN),
                "ingest.tree_state_densification.commitment_budget",
            )?)
        }
    };
    Ok(TreeStateDensificationConfig {
        enabled: section.enabled.unwrap_or(defaults.enabled),
        during_construction: section
            .during_construction
            .unwrap_or(defaults.during_construction),
        selection,
        workers: nonzero(
            section.workers,
            defaults.workers,
            "ingest.tree_state_densification.workers",
        )?,
        max_blocks_per_second: nonzero(
            section.max_blocks_per_second,
            defaults.max_blocks_per_second,
            "ingest.tree_state_densification.max_blocks_per_second",
        )?,
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
            "ingest_control.listen_addr outside loopback requires ingest_control.bearer_token_path or ingest_control.bearer_token",
        ));
    }
    Ok(())
}

fn raw_blob_retention_from_policy(policy: RawBlobPolicy) -> RawBlobRetention {
    match policy {
        RawBlobPolicy::None => RawBlobRetention::None,
        RawBlobPolicy::All => RawBlobRetention::All,
    }
}

fn apply_deployment_profile(config: &mut IngestConfig) -> Result<(), ConfigError> {
    let Some(profile) = config.deployment.profile else {
        return Ok(());
    };
    let raw_blob_policy = RawBlobPolicy::from(
        profile
            .resolve_raw_blob_retention_with_full_blocks(
                config.storage.full_blocks,
                config
                    .storage
                    .raw_blob_policy
                    .map(raw_blob_retention_from_policy),
            )
            .map_err(|error| {
                let message = error.to_string();
                if message.contains("remove conflicting overrides") {
                    error
                } else {
                    ConfigError::invalid(format!("{message}; remove conflicting overrides"))
                }
            })?,
    );
    if profile == DeploymentProfile::Explorer && config.ingest.explorer_views == Some(false) {
        return Err(ConfigError::invalid(
            "deployment.profile = \"explorer\" requires ingest.explorer_views = true; remove ingest.explorer_views or choose a wallet profile",
        ));
    }
    let conflicting_coverage = config
        .ingest
        .run_overrides
        .coverage
        .is_some_and(|coverage| coverage != IngestCoverage::WalletServing);
    if conflicting_coverage {
        return Err(ConfigError::invalid(format!(
            "deployment.profile = \"{}\" requires wallet-serving coverage. To select this profile, remove conflicting ingest.run_overrides.coverage",
            profile.as_str(),
        )));
    }
    config.ingest.run_overrides.coverage = Some(IngestCoverage::WalletServing);
    if profile == DeploymentProfile::Explorer {
        config.ingest.explorer_views.get_or_insert(true);
    }
    config.storage.raw_blob_policy = Some(raw_blob_policy);
    Ok(())
}

fn resolve_raw_blob_policy(
    profile: Option<DeploymentProfile>,
    full_blocks: Option<bool>,
    configured_policy: Option<RawBlobPolicy>,
) -> Result<RawBlobPolicy, ConfigError> {
    if profile.is_some() {
        return Ok(configured_policy.unwrap_or(DEFAULT_RAW_BLOB_POLICY));
    }
    let resolved_retention = resolve_manual_raw_blob_retention(
        full_blocks,
        configured_policy.map(raw_blob_retention_from_policy),
    )?;
    let resolved_policy = resolved_retention.map(RawBlobPolicy::from);
    Ok(resolved_policy.unwrap_or(DEFAULT_RAW_BLOB_POLICY))
}

#[derive(Serialize)]
struct RedactedIngestConfigToml {
    #[serde(skip_serializing_if = "DeploymentToml::is_empty")]
    deployment: DeploymentToml,
    network: NetworkToml,
    ops: OpsToml,
    #[serde(skip_serializing_if = "zinder_runtime::ObservabilityConfig::is_empty")]
    observability: zinder_runtime::ObservabilityConfig,
    security: SecurityToml,
    node: NodeToml,
    #[serde(skip_serializing_if = "NodeFallbacksToml::is_empty")]
    node_fallbacks: NodeFallbacksToml,
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
            deployment: DeploymentToml::from_resolved(config.profile),
            network: NetworkToml::from_network(runtime_config.node.network),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            observability: config.observability.clone(),
            security: SecurityToml::from_resolved(config.allow_public_bind),
            node: NodeToml::from_node_target(&runtime_config.node),
            node_fallbacks: NodeFallbacksToml::from_node_targets(&runtime_config.node_fallbacks),
            storage: IngestStorageToml {
                data_dir: config
                    .storage_data_dir
                    .as_ref()
                    .map(|path| path.display().to_string()),
                path: runtime_config.storage_path.display().to_string(),
                full_blocks: config.storage_full_blocks,
                raw_blob_policy: config
                    .profile
                    .is_none()
                    .then_some(runtime_config.raw_blob_policy),
                canonical: StorageRoleToml::from_resolved(runtime_config.canonical_rocksdb_budget),
                materialized_views: StorageRoleToml::from_resolved(
                    runtime_config.materialized_view_rocksdb_budget,
                ),
            },
            ingest: IngestToml {
                source: node_source_name(runtime_config.node_source),
                explorer_views: (config.profile != Some(DeploymentProfile::Explorer)
                    && (config.profile.is_none() || config.explorer_views))
                    .then_some(config.explorer_views),
                explorer_views_min_free_bytes: config.explorer_views_min_free_bytes,
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
                    tree_hashing_workers: runtime_config
                        .construction
                        .pipeline_limits
                        .tree_hashing_workers
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
                    coverage: config.profile.is_none().then_some(config.coverage),
                },
                tree_state_densification: IngestTreeStateDensificationToml::from_resolved(
                    config.tree_state_densification,
                ),
                tree_state_verification: IngestTreeStateVerificationToml {
                    enabled: config.tree_state_verification.enabled,
                    max_rows_per_second: config.tree_state_verification.max_rows_per_second.get(),
                },
            },
            ingest_control: IngestControlWriterToml::from_writer(&ResolvedIngestControlWriter {
                listen_addr: config.ingest_control_listen_addr,
                bearer_token_path: config.ingest_control_bearer_token_path.clone(),
                bearer_token: config.ingest_control_bearer_token.clone(),
                checkpoint_bearer_token_path: config
                    .ingest_control_checkpoint_bearer_token_path
                    .clone(),
                checkpoint_bearer_token: config.ingest_control_checkpoint_bearer_token.clone(),
                checkpoint_staging_root: config.ingest_control_checkpoint_staging_root.clone(),
            }),
            retention: RetentionToml::from_resolved(config.retention),
        }
    }
}

#[derive(Serialize)]
struct IngestToml {
    source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    explorer_views: Option<bool>,
    explorer_views_min_free_bytes: u64,
    reorg_window_blocks: u32,
    phase_classification: IngestPhaseClassificationToml,
    construction: IngestConstructionToml,
    mempool: IngestMempoolToml,
    follow: IngestFollowToml,
    run_overrides: IngestRunOverridesToml,
    tree_state_densification: IngestTreeStateDensificationToml,
    tree_state_verification: IngestTreeStateVerificationToml,
}

#[derive(Serialize)]
struct IngestTreeStateVerificationToml {
    enabled: bool,
    max_rows_per_second: u32,
}

#[derive(Serialize)]
struct IngestTreeStateDensificationToml {
    enabled: bool,
    during_construction: bool,
    selection: TreeStateSelectionName,
    #[serde(skip_serializing_if = "Option::is_none")]
    commitment_budget: Option<u32>,
    workers: u32,
    max_blocks_per_second: u32,
}

impl IngestTreeStateDensificationToml {
    fn from_resolved(config: TreeStateDensificationConfig) -> Self {
        let (selection, commitment_budget) = match config.selection {
            TreeStateCheckpointSelection::CommitmentBudget(budget) => {
                (TreeStateSelectionName::CommitmentBudget, Some(budget.get()))
            }
            TreeStateCheckpointSelection::EveryBlock | _ => {
                (TreeStateSelectionName::EveryBlock, None)
            }
        };
        Self {
            enabled: config.enabled,
            during_construction: config.during_construction,
            selection,
            commitment_budget,
            workers: config.workers.get(),
            max_blocks_per_second: config.max_blocks_per_second.get(),
        }
    }
}

#[derive(Serialize)]
struct IngestMempoolToml {
    max_transaction_count: u32,
    max_total_raw_transaction_bytes: u64,
    reconciliation_batch_target_raw_transaction_bytes: u64,
}

#[derive(Serialize)]
struct IngestStorageToml {
    #[serde(skip_serializing_if = "Option::is_none")]
    data_dir: Option<String>,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    full_blocks: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_blob_policy: Option<RawBlobPolicy>,
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
    tree_hashing_workers: u32,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    coverage: Option<IngestCoverage>,
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
                .contains("outside loopback requires ingest_control.bearer_token_path or ingest_control.bearer_token")
        );
        assert!(
            guard_ingest_control_bearer_token(Some(SocketAddr::from(([127, 0, 0, 1], 8240))), None)
                .is_ok()
        );
    }

    #[test]
    fn tree_state_densification_defaults_to_every_block_and_accepts_a_commitment_budget() {
        let defaults =
            resolve_tree_state_densification(IngestTreeStateDensificationSection::default());
        assert_eq!(
            defaults.as_ref().ok(),
            Some(&TreeStateDensificationConfig::default())
        );
        assert!(matches!(
            defaults,
            Ok(TreeStateDensificationConfig {
                enabled: true,
                during_construction: false,
                selection: zinder_store::TreeStateCheckpointSelection::EveryBlock,
                workers,
                max_blocks_per_second,
            }) if workers.get() == 2 && max_blocks_per_second.get() == 500
        ));
        let budget_without_selection =
            resolve_tree_state_densification(IngestTreeStateDensificationSection {
                commitment_budget: Some(512),
                ..IngestTreeStateDensificationSection::default()
            });
        assert!(matches!(
            budget_without_selection,
            Ok(TreeStateDensificationConfig {
                selection: zinder_store::TreeStateCheckpointSelection::CommitmentBudget(budget),
                ..
            }) if budget.get() == 512
        ));
        assert!(matches!(
            resolve_tree_state_densification(IngestTreeStateDensificationSection {
                selection: Some(TreeStateSelectionName::EveryBlock),
                ..IngestTreeStateDensificationSection::default()
            }),
            Ok(TreeStateDensificationConfig {
                selection: zinder_store::TreeStateCheckpointSelection::EveryBlock,
                ..
            })
        ));
        let budgeted = resolve_tree_state_densification(IngestTreeStateDensificationSection {
            selection: Some(TreeStateSelectionName::CommitmentBudget),
            commitment_budget: Some(2_048),
            workers: Some(4),
            ..IngestTreeStateDensificationSection::default()
        });
        assert!(matches!(
            budgeted,
            Ok(TreeStateDensificationConfig {
                selection: zinder_store::TreeStateCheckpointSelection::CommitmentBudget(budget),
                workers,
                ..
            }) if budget.get() == 2_048 && workers.get() == 4
        ));
        let default_budget =
            resolve_tree_state_densification(IngestTreeStateDensificationSection {
                selection: Some(TreeStateSelectionName::CommitmentBudget),
                ..IngestTreeStateDensificationSection::default()
            });
        assert!(matches!(
            default_budget,
            Ok(TreeStateDensificationConfig {
                selection: zinder_store::TreeStateCheckpointSelection::CommitmentBudget(budget),
                ..
            }) if budget.get() == 1_024
        ));
    }

    #[test]
    fn tree_state_densification_rejects_zero_limits_and_a_stray_budget() {
        for (section, field) in [
            (
                IngestTreeStateDensificationSection {
                    workers: Some(0),
                    ..IngestTreeStateDensificationSection::default()
                },
                "ingest.tree_state_densification.workers",
            ),
            (
                IngestTreeStateDensificationSection {
                    max_blocks_per_second: Some(0),
                    ..IngestTreeStateDensificationSection::default()
                },
                "ingest.tree_state_densification.max_blocks_per_second",
            ),
            (
                IngestTreeStateDensificationSection {
                    selection: Some(TreeStateSelectionName::CommitmentBudget),
                    commitment_budget: Some(0),
                    ..IngestTreeStateDensificationSection::default()
                },
                "ingest.tree_state_densification.commitment_budget",
            ),
            (
                IngestTreeStateDensificationSection {
                    selection: Some(TreeStateSelectionName::EveryBlock),
                    commitment_budget: Some(1_024),
                    ..IngestTreeStateDensificationSection::default()
                },
                "ingest.tree_state_densification.commitment_budget",
            ),
        ] {
            let error = resolve_tree_state_densification(section)
                .err()
                .map_or_default(|error| error.to_string());
            assert!(error.contains(field), "{field}: {error}");
        }
    }

    #[test]
    fn tree_state_verification_defaults_to_on_at_a_conservative_rate_and_rejects_zero() {
        assert_eq!(
            resolve_tree_state_verification(IngestTreeStateVerificationSection::default()).ok(),
            Some(TreeStateVerificationConfig {
                enabled: true,
                max_rows_per_second: NonZeroU32::MIN.saturating_add(24),
            })
        );
        assert!(matches!(
            resolve_tree_state_verification(IngestTreeStateVerificationSection {
                enabled: Some(false),
                max_rows_per_second: Some(200),
            }),
            Ok(TreeStateVerificationConfig {
                enabled: false,
                max_rows_per_second,
            }) if max_rows_per_second.get() == 200
        ));
        let error = resolve_tree_state_verification(IngestTreeStateVerificationSection {
            max_rows_per_second: Some(0),
            ..IngestTreeStateVerificationSection::default()
        })
        .err()
        .map_or_default(|error| error.to_string());
        assert!(
            error.contains("ingest.tree_state_verification.max_rows_per_second"),
            "{error}"
        );
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

    #[test]
    fn parse_node_source_accepts_zebra_json_rpc_and_zebra_indexer_grpc() {
        assert_eq!(
            parse_node_source("zebra-json-rpc").ok(),
            Some(NodeSourceKind::ZebraJsonRpc)
        );
        assert_eq!(
            parse_node_source("zebra-indexer-grpc").ok(),
            Some(NodeSourceKind::ZebraIndexerGrpc)
        );
        assert!(parse_node_source("unsupported-source").is_err());
    }

    #[test]
    fn validate_node_source_targets_requires_indexer_grpc_addr_on_primary_and_fallbacks()
    -> Result<(), Box<dyn std::error::Error>> {
        let rpc_only_section: NodeSection =
            toml::from_str("json_rpc_addr = \"http://127.0.0.1:8232\"")?;
        let with_indexer_section: NodeSection = toml::from_str(
            "json_rpc_addr = \"http://127.0.0.1:8232\"\n\
             indexer_grpc_addr = \"http://127.0.0.1:8231\"",
        )?;
        let rpc_only_target =
            NodeTarget::resolve(zinder_core::Network::ZcashRegtest, rpc_only_section)?;
        let with_indexer_target =
            NodeTarget::resolve(zinder_core::Network::ZcashRegtest, with_indexer_section)?;

        assert!(
            validate_node_source_targets(NodeSourceKind::ZebraJsonRpc, &rpc_only_target, &[])
                .is_ok()
        );

        let missing_primary =
            validate_node_source_targets(NodeSourceKind::ZebraIndexerGrpc, &rpc_only_target, &[])
                .err()
                .map_or_default(|error| error.to_string());
        assert!(
            missing_primary.contains("node.indexer_grpc_addr"),
            "{missing_primary}"
        );

        let missing_fallback = validate_node_source_targets(
            NodeSourceKind::ZebraIndexerGrpc,
            &with_indexer_target,
            &[rpc_only_target],
        )
        .err()
        .map_or_default(|error| error.to_string());
        assert!(
            missing_fallback.contains("node_fallbacks.1.indexer_grpc_addr"),
            "{missing_fallback}"
        );

        assert!(
            validate_node_source_targets(
                NodeSourceKind::ZebraIndexerGrpc,
                &with_indexer_target,
                std::slice::from_ref(&with_indexer_target),
            )
            .is_ok()
        );
        Ok(())
    }

    #[test]
    fn validate_node_source_targets_rejects_invalid_indexer_tls_combination()
    -> Result<(), Box<dyn std::error::Error>> {
        let invalid_tls_section: NodeSection = toml::from_str(
            "json_rpc_addr = \"http://127.0.0.1:8232\"\n\
             indexer_grpc_addr = \"http://127.0.0.1:8231\"\n\
             [indexer_tls]\n\
             server_name = \"zebra.internal\"",
        )?;
        let tls_error =
            NodeTarget::resolve(zinder_core::Network::ZcashRegtest, invalid_tls_section)
                .err()
                .map_or_default(|error| error.to_string());
        assert!(
            tls_error.contains("node.indexer_tls requires an https:// node.indexer_grpc_addr"),
            "{tls_error}"
        );
        Ok(())
    }
}
