//! Configuration loading for the `zinder-query` runtime.

use std::{
    net::SocketAddr,
    num::{NonZeroU8, NonZeroU32},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zinder_core::Network;
use zinder_runtime::{
    BearerToken, BearerTokenError, BrowserOrigins, CanonicalSecondaryStorageSection,
    CanonicalSecondaryStorageToml, ClientAddressSource, ConfigError, ConfigLoader,
    DeploymentProfile, DeploymentSection, DeploymentToml, GrpcProxyProtocol, GrpcRangeAdmission,
    IngestControlReaderToml, IngestControlSection, NetworkSection, NetworkToml, NodeFallbacksToml,
    NodeToml, OpsSection, OpsServerError, OpsToml, ResolvedCanonicalSecondaryStorage,
    ResolvedIngestControlReader, RocksDbResourceBudgetSection, RocksDbResourceBudgetToml,
    RuntimeService, SecuritySection, SecurityToml, derive_canonical_path,
    derive_canonical_secondary_path, derive_wallet_path, derive_wallet_secondary_path,
    guard_optional_serving_bind, guard_serving_bind, parse_socket_addr, require_field,
    resolve_allow_public_bind, resolve_canonical_secondary_storage, resolve_ingest_control_reader,
    resolve_ops_listen_addr, resolve_wallet_projection_reader_rocksdb_budget,
    section_defaults::DEFAULT_DATA_DIR, validate_optional_data_dir,
};
use zinder_source::{NodeFallbackSections, NodeSection, NodeTarget};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreBuildPlanError, CanonicalStoreRequirements,
    RocksDbResourceBudget,
};

use crate::{
    DEFAULT_TREE_STATE_REPLAY_WAIT, DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT, EpochPinRetention,
    ForwardedReadConfig, MAX_TREE_STATE_REPLAY_WAIT, MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
};

/// Resolved native wallet-query runtime configuration.
#[derive(Clone, Debug)]
pub struct QueryConfig {
    /// Coordinated deployment profile, when selected.
    pub profile: Option<DeploymentProfile>,
    /// Target Zcash network.
    pub network: Network,
    /// Canonical primary and secondary storage configuration.
    pub storage: ResolvedCanonicalSecondaryStorage,
    /// Wallet projection primary `RocksDB` path.
    pub wallet_primary_path: PathBuf,
    /// Root containing this process's wallet secondary generations.
    pub wallet_secondary_root: PathBuf,
    /// Resource budget applied to opened wallet secondary instances.
    pub wallet_rocksdb_budget: RocksDbResourceBudget,
    /// `zinder-ingest` control-plane gRPC URL.
    pub ingest_control_addr: String,
    /// Path to the shared-secret bearer token file for `IngestControl`.
    pub ingest_control_bearer_token_path: Option<PathBuf>,
    /// Resolved bearer token presented to `IngestControl`.
    pub ingest_control_bearer_token: Option<BearerToken>,
    /// Native `WalletQuery` gRPC listen address.
    pub listen_addr: SocketAddr,
    /// Browser origins permitted to use gRPC-Web. An empty list disables it.
    pub browser_origins: BrowserOrigins,
    /// Operational HTTP endpoint listen address, or `None` when disabled.
    pub ops_listen_addr: Option<SocketAddr>,
    /// Optional trace export settings.
    pub observability: zinder_runtime::ObservabilityConfig,
    /// Whether public or unspecified bind addresses are permitted.
    pub allow_public_bind: bool,
    /// How the gRPC listener tells clients apart for its receive limit.
    pub client_address_source: ClientAddressSource,
    /// Trusted TCP proxy peers for a listener requiring PROXY protocol.
    pub proxy_protocol: GrpcProxyProtocol,
    /// Client quotas and overload policy for block-range RPCs.
    pub range_admission: GrpcRangeAdmission,
    /// Canonical reorg-window identity enforced on admitted stores.
    pub canonical_reorg_policy: CanonicalReorgPolicy,
    /// Maximum bounded attempts to converge the serving read pair.
    pub pair_convergence_attempts: NonZeroU8,
    /// Retention after a response's chain epoch is replaced.
    pub epoch_pin_retention: EpochPinRetention,
    /// Longest a tree-state rebuild waits for a rebuild slot.
    pub tree_state_replay_wait: Duration,
    /// Most tree-state rebuilds that wait for each rebuild slot.
    pub tree_state_replay_waiters_per_slot: NonZeroU32,
    /// Admission and cache settings for transaction bytes read from the node.
    pub forwarded_reads: ForwardedReadConfig,
    /// First node endpoint transaction broadcast tries, or `None` for a
    /// storage-only query.
    pub node: Option<NodeTarget>,
    /// Endpoints broadcast tries after `node`, in position order.
    pub node_fallbacks: Vec<NodeTarget>,
}

impl QueryConfig {
    /// Returns canonical store requirements derived from deployment profile and raw-blob policy.
    #[must_use]
    pub fn canonical_requirements(&self) -> CanonicalStoreRequirements {
        let requirements = self.profile.map_or_else(
            CanonicalStoreRequirements::default,
            DeploymentProfile::canonical_requirement,
        );
        requirements.with_blob_retention(self.storage.expected_raw_blob_retention)
    }
}

/// Command-line overrides for the native wallet-query command.
#[derive(Debug, Default)]
pub struct QueryConfigOverrides {
    /// Override for `network.name`.
    pub network: Option<String>,
    /// Override for `storage.path`.
    pub canonical_primary_path: Option<PathBuf>,
    /// Override for `storage.secondary_path`.
    pub canonical_secondary_root: Option<PathBuf>,
    /// Override for `storage.raw_blob_policy`.
    pub raw_blob_policy: Option<String>,
    /// Override for `wallet.path`.
    pub wallet_primary_path: Option<PathBuf>,
    /// Override for `wallet.secondary_path`.
    pub wallet_secondary_root: Option<PathBuf>,
    /// Override for `ingest_control.addr`.
    pub ingest_control_addr: Option<String>,
    /// Override for `ingest_control.bearer_token_path`.
    pub ingest_control_bearer_token_path: Option<PathBuf>,
    /// Override for `query.listen_addr`.
    pub listen_addr: Option<SocketAddr>,
    /// Override for `ops.listen_addr`.
    pub ops_listen_addr: Option<SocketAddr>,
    /// Override for `node.json_rpc_addr`.
    pub node_json_rpc_addr: Option<String>,
    /// Override for `query.reorg_window_blocks`.
    pub reorg_window_blocks: Option<u32>,
}

/// Error returned while resolving config or running the native query server.
#[derive(Debug, Error)]
pub enum QueryConfigError {
    /// Layered configuration resolution failed.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// Canonical store build-plan policy failed validation.
    #[error(transparent)]
    CanonicalStoreBuildPlan(#[from] CanonicalStoreBuildPlanError),
    /// Serving read pair admission or publisher bootstrap failed.
    #[error(transparent)]
    WalletServingPair(#[from] crate::WalletServingPairError),
    /// `IngestControl` endpoint admission failed.
    #[error(transparent)]
    IngestControlAdmission(#[from] crate::IngestControlAdmissionError),
    /// Native query execution failed.
    #[error(transparent)]
    WalletQuery(#[from] crate::QueryError),
    /// Upstream node source construction failed.
    #[error("node source initialization failed: {0}")]
    Source(Box<zinder_source::SourceError>),
    /// gRPC server transport failed.
    #[error("gRPC transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Shared-secret bearer token could not be loaded.
    #[error("invalid ingest-control bearer token: {0}")]
    BearerToken(#[from] BearerTokenError),
    /// gRPC reflection descriptor initialization failed.
    #[error("gRPC reflection service build failed: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),
    /// Operational HTTP endpoint setup or shutdown failed.
    #[error(transparent)]
    Operations(#[from] OpsServerError),
    /// Supervised runtime or background task failed.
    #[error("wallet query runtime stopped: {0}")]
    Supervision(#[from] zinder_runtime::RuntimeSupervisionError),
}

/// Loads and validates native query configuration from defaults, file, environment, and overrides.
pub fn load_query_config(
    config_path: Option<PathBuf>,
    overrides: QueryConfigOverrides,
) -> Result<QueryConfig, QueryConfigError> {
    load_query_config_with_env(config_path, overrides, std::env::vars())
}

/// Loads and validates native query configuration using explicit `ZINDER_*`
/// environment pairs.
pub fn load_query_config_with_env<I>(
    config_path: Option<PathBuf>,
    overrides: QueryConfigOverrides,
    environment: I,
) -> Result<QueryConfig, QueryConfigError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let raw_config: QueryRawConfig = ConfigLoader::new()
        .with_default("query.listen_addr", "127.0.0.1:9102")?
        .with_default(
            "query.reorg_window_blocks",
            zinder_store::DEFAULT_REORG_WINDOW_BLOCKS,
        )?
        .with_ops_section(RuntimeService::Query)?
        .with_security_section()?
        .with_file(config_path)
        .with_zinder_env_from_map(environment)?
        .with_override_if("network.name", overrides.network)?
        .with_override_path_if("storage.path", overrides.canonical_primary_path)?
        .with_override_path_if("storage.secondary_path", overrides.canonical_secondary_root)?
        .with_override_if("storage.raw_blob_policy", overrides.raw_blob_policy)?
        .with_override_path_if("wallet.path", overrides.wallet_primary_path)?
        .with_override_path_if("wallet.secondary_path", overrides.wallet_secondary_root)?
        .with_override_if("ingest_control.addr", overrides.ingest_control_addr)?
        .with_override_path_if(
            "ingest_control.bearer_token_path",
            overrides.ingest_control_bearer_token_path,
        )?
        .with_override_if(
            "query.listen_addr",
            overrides.listen_addr.map(|addr| addr.to_string()),
        )?
        .with_override_if(
            "ops.listen_addr",
            overrides.ops_listen_addr.map(|addr| addr.to_string()),
        )?
        .with_override_if("node.json_rpc_addr", overrides.node_json_rpc_addr)?
        .with_override_if("query.reorg_window_blocks", overrides.reorg_window_blocks)?
        .load()?;

    resolve_query_config(raw_config)
}

/// Renders a resolved native query configuration as redacted TOML for `--print-config`.
pub fn query_config_toml(config: &QueryConfig) -> Result<String, QueryConfigError> {
    toml::to_string(&QueryConfigToml::from_query_config(config))
        .map_err(|source| ConfigError::Render { source }.into())
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct QueryRawConfig {
    deployment: DeploymentSection,
    network: NetworkSection,
    ops: OpsSection,
    observability: zinder_runtime::ObservabilityConfig,
    storage: CanonicalSecondaryStorageSection,
    wallet: WalletSection,
    ingest_control: IngestControlSection,
    query: QuerySection,
    node: Option<NodeSection>,
    node_fallbacks: NodeFallbackSections,
    security: SecuritySection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct QuerySection {
    listen_addr: Option<String>,
    browser_origins: BrowserOrigins,
    reorg_window_blocks: Option<u32>,
    pair_convergence_attempts: Option<u8>,
    epoch_pin_retention_seconds: Option<u64>,
    tree_state_replay_wait_ms: Option<u64>,
    tree_state_replay_waiters_per_slot: Option<u32>,
    forwarded_read_max_concurrent: Option<u32>,
    forwarded_read_wait_ms: Option<u64>,
    forwarded_read_waiters_per_slot: Option<u32>,
    forwarded_transaction_cache_bytes: Option<u64>,
    trust_proxy_client_address: Option<bool>,
    proxy_protocol_trusted_cidrs: Option<String>,
    range_admission: GrpcRangeAdmission,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WalletSection {
    path: Option<PathBuf>,
    secondary_path: Option<PathBuf>,
    rocksdb: RocksDbResourceBudgetSection,
}

fn apply_deployment_profile(config: &mut QueryRawConfig) -> Result<(), ConfigError> {
    let profile = config.deployment.profile;
    if let Some(profile) = profile {
        if profile == DeploymentProfile::LightWallet {
            return Err(ConfigError::invalid(
                "the light-wallet profile has no native wallet query",
            ));
        }
        config.storage.raw_blob_policy =
            Some(profile.resolve_raw_blob_retention_with_full_blocks(
                config.storage.full_blocks,
                config.storage.raw_blob_policy,
            )?);
        if matches!(
            profile,
            DeploymentProfile::Wallet | DeploymentProfile::Explorer
        ) && config.storage.full_blocks != Some(true)
            && config.node.is_none()
        {
            return Err(ConfigError::invalid(format!(
                "the {} profile requires [node] for verified mined transaction bytes",
                profile.as_str(),
            )));
        }
    }
    Ok(())
}

fn derive_query_storage_paths(config: &mut QueryRawConfig) -> Result<(), ConfigError> {
    config.storage.data_dir = validate_optional_data_dir(config.storage.data_dir.take())?;
    let all_paths_unset = config.storage.path.is_none()
        && config.storage.secondary_path.is_none()
        && config.wallet.path.is_none()
        && config.wallet.secondary_path.is_none();
    let effective_data_dir = config
        .storage
        .data_dir
        .as_deref()
        .or_else(|| all_paths_unset.then_some(Path::new(DEFAULT_DATA_DIR)));
    if let Some(dir) = effective_data_dir {
        config
            .storage
            .path
            .get_or_insert_with(|| derive_canonical_path(dir));
        config
            .storage
            .secondary_path
            .get_or_insert_with(|| derive_canonical_secondary_path(dir, "query"));
        config
            .wallet
            .path
            .get_or_insert_with(|| derive_wallet_path(dir));
        config
            .wallet
            .secondary_path
            .get_or_insert_with(|| derive_wallet_secondary_path(dir, "query"));
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the field resolution and startup validation order are easier to audit together"
)]
fn resolve_query_config(mut config: QueryRawConfig) -> Result<QueryConfig, QueryConfigError> {
    apply_deployment_profile(&mut config)?;
    derive_query_storage_paths(&mut config)?;
    let network = config.network.resolve()?;
    let storage = resolve_canonical_secondary_storage(config.storage)?;
    let wallet_primary_path = require_field(config.wallet.path, "wallet.path")?;
    let wallet_secondary_root =
        require_field(config.wallet.secondary_path, "wallet.secondary_path")?;
    let wallet_rocksdb_budget =
        resolve_wallet_projection_reader_rocksdb_budget(config.wallet.rocksdb)?;
    require_distinct_storage_paths(
        &storage.path,
        &storage.secondary_path,
        &wallet_primary_path,
        &wallet_secondary_root,
    )?;
    let ResolvedIngestControlReader {
        addr: ingest_control_addr,
        bearer_token_path: ingest_control_bearer_token_path,
        bearer_token: ingest_control_bearer_token,
    } = resolve_ingest_control_reader(config.ingest_control)?;
    let listen_addr = parse_socket_addr(
        "query.listen_addr",
        &require_field(config.query.listen_addr, "query.listen_addr")?,
    )?;
    let ops_listen_addr = resolve_ops_listen_addr(config.ops)?;
    let allow_public_bind = resolve_allow_public_bind(config.security)?;
    guard_serving_bind("query.listen_addr", listen_addr, allow_public_bind)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;
    let canonical_reorg_policy = CanonicalReorgPolicy::new(require_field(
        config.query.reorg_window_blocks,
        "query.reorg_window_blocks",
    )?)?;
    let forwarded_reads = ForwardedReadConfig::resolve(
        config.query.forwarded_read_max_concurrent,
        config.query.forwarded_read_wait_ms,
        config.query.forwarded_read_waiters_per_slot,
        config.query.forwarded_transaction_cache_bytes,
    )
    .map_err(|error| ConfigError::invalid(format!("query.{error}")))?;
    let (node, node_fallbacks) = resolve_query_nodes(network, config.node, config.node_fallbacks)?;

    let client_address_source = ClientAddressSource::from_trust_proxy_client_address(
        config.query.trust_proxy_client_address.unwrap_or(false),
    );
    let proxy_protocol = GrpcProxyProtocol::resolve(
        config.query.proxy_protocol_trusted_cidrs.as_deref(),
        client_address_source,
    )?;

    Ok(QueryConfig {
        profile: config.deployment.profile,
        network,
        storage,
        wallet_primary_path,
        wallet_secondary_root,
        wallet_rocksdb_budget,
        ingest_control_addr,
        ingest_control_bearer_token_path,
        ingest_control_bearer_token,
        listen_addr,
        browser_origins: config.query.browser_origins,
        ops_listen_addr,
        observability: config.observability,
        allow_public_bind,
        client_address_source,
        proxy_protocol,
        range_admission: config.query.range_admission,
        canonical_reorg_policy,
        pair_convergence_attempts: require_pair_convergence_attempts(
            config.query.pair_convergence_attempts,
        )?,
        epoch_pin_retention: resolve_epoch_pin_retention(config.query.epoch_pin_retention_seconds)?,
        tree_state_replay_wait: require_tree_state_replay_wait(
            config.query.tree_state_replay_wait_ms,
        )?,
        tree_state_replay_waiters_per_slot: require_tree_state_replay_waiters_per_slot(
            config.query.tree_state_replay_waiters_per_slot,
        )?,
        forwarded_reads,
        node,
        node_fallbacks,
    })
}

fn resolve_query_nodes(
    network: Network,
    node: Option<NodeSection>,
    fallbacks: NodeFallbackSections,
) -> Result<(Option<NodeTarget>, Vec<NodeTarget>), ConfigError> {
    let node = node
        .map(|section| NodeTarget::resolve(network, section))
        .transpose()
        .map_err(ConfigError::from)?;
    let fallbacks = NodeTarget::resolve_fallbacks(network, fallbacks).map_err(ConfigError::from)?;
    NodeTarget::require_primary_for_fallbacks(node.as_ref(), &fallbacks)
        .map_err(ConfigError::from)?;
    Ok((node, fallbacks))
}

const DEFAULT_PAIR_CONVERGENCE_ATTEMPTS: u8 = 12;
const MAX_PAIR_CONVERGENCE_ATTEMPTS: u8 = 64;

fn require_pair_convergence_attempts(configured: Option<u8>) -> Result<NonZeroU8, ConfigError> {
    let configured = configured.unwrap_or(DEFAULT_PAIR_CONVERGENCE_ATTEMPTS);
    let attempts = NonZeroU8::new(configured).ok_or_else(|| {
        ConfigError::invalid("query.pair_convergence_attempts must be greater than zero")
    })?;
    if attempts.get() > MAX_PAIR_CONVERGENCE_ATTEMPTS {
        return Err(ConfigError::invalid(format!(
            "query.pair_convergence_attempts must not exceed {MAX_PAIR_CONVERGENCE_ATTEMPTS}"
        )));
    }
    Ok(attempts)
}

fn resolve_epoch_pin_retention(seconds: Option<u64>) -> Result<EpochPinRetention, ConfigError> {
    let Some(seconds) = seconds else {
        return Ok(EpochPinRetention::default());
    };
    EpochPinRetention::from_seconds(seconds).ok_or_else(|| {
        ConfigError::invalid(format!(
            "query.epoch_pin_retention_seconds must be in {}..={}",
            EpochPinRetention::MIN_SECONDS,
            EpochPinRetention::MAX_SECONDS,
        ))
    })
}

fn require_tree_state_replay_wait(configured_ms: Option<u64>) -> Result<Duration, ConfigError> {
    let Some(configured_ms) = configured_ms else {
        return Ok(DEFAULT_TREE_STATE_REPLAY_WAIT);
    };
    let wait = Duration::from_millis(configured_ms);
    if wait > MAX_TREE_STATE_REPLAY_WAIT {
        return Err(ConfigError::invalid(format!(
            "query.tree_state_replay_wait_ms must not exceed {}",
            MAX_TREE_STATE_REPLAY_WAIT.as_millis()
        )));
    }
    Ok(wait)
}

fn require_tree_state_replay_waiters_per_slot(
    configured: Option<u32>,
) -> Result<NonZeroU32, ConfigError> {
    let Some(configured) = configured else {
        return Ok(DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT);
    };
    NonZeroU32::new(configured)
        .filter(|waiters| *waiters <= MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT)
        .ok_or_else(|| {
            ConfigError::invalid(format!(
                "query.tree_state_replay_waiters_per_slot must be in 1..={MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT}"
            ))
        })
}

fn require_distinct_storage_paths(
    canonical_primary: &Path,
    canonical_secondary_root: &Path,
    wallet_primary: &Path,
    wallet_secondary_root: &Path,
) -> Result<(), ConfigError> {
    let paths = [
        canonical_primary,
        canonical_secondary_root,
        wallet_primary,
        wallet_secondary_root,
    ];
    let identities = paths
        .iter()
        .map(|path| normalized_storage_path_identity(path))
        .collect::<Result<Vec<_>, _>>()?;
    for (index, path) in identities.iter().enumerate() {
        if identities[index + 1..]
            .iter()
            .any(|other| other == path || other.starts_with(path) || path.starts_with(other))
        {
            return Err(ConfigError::invalid(
                "storage.path, storage.secondary_path, wallet.path, and wallet.secondary_path must be disjoint roots",
            ));
        }
    }
    Ok(())
}

fn normalized_storage_path_identity(path: &Path) -> Result<PathBuf, ConfigError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|source| {
                ConfigError::invalid(format!(
                    "could not resolve relative storage path {}: {source}",
                    path.display()
                ))
            })?
            .join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}

#[derive(Serialize)]
struct QueryConfigToml {
    #[serde(skip_serializing_if = "DeploymentToml::is_empty")]
    deployment: DeploymentToml,
    network: NetworkToml,
    ops: OpsToml,
    #[serde(skip_serializing_if = "zinder_runtime::ObservabilityConfig::is_empty")]
    observability: zinder_runtime::ObservabilityConfig,
    security: SecurityToml,
    storage: CanonicalSecondaryStorageToml,
    wallet: WalletToml,
    ingest_control: IngestControlReaderToml,
    query: QueryToml,
    #[serde(skip_serializing_if = "Option::is_none")]
    node: Option<NodeToml>,
    #[serde(skip_serializing_if = "NodeFallbacksToml::is_empty")]
    node_fallbacks: NodeFallbacksToml,
}

impl QueryConfigToml {
    fn from_query_config(config: &QueryConfig) -> Self {
        let mut storage = CanonicalSecondaryStorageToml::from_resolved(&config.storage);
        if config.profile.is_some() {
            storage.raw_blob_policy = None;
        }
        Self {
            network: NetworkToml::from_network(config.network),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            observability: config.observability.clone(),
            security: SecurityToml::from_resolved(config.allow_public_bind),
            deployment: DeploymentToml::from_resolved(config.profile),
            storage,
            wallet: WalletToml {
                path: config.wallet_primary_path.clone(),
                secondary_path: config.wallet_secondary_root.clone(),
                rocksdb: RocksDbResourceBudgetToml::from_resolved(config.wallet_rocksdb_budget),
            },
            ingest_control: IngestControlReaderToml::from_reader(&ResolvedIngestControlReader {
                addr: config.ingest_control_addr.clone(),
                bearer_token_path: config.ingest_control_bearer_token_path.clone(),
                bearer_token: config.ingest_control_bearer_token.clone(),
            }),
            query: QueryToml {
                listen_addr: config.listen_addr.to_string(),
                browser_origins: config.browser_origins.clone(),
                reorg_window_blocks: config.canonical_reorg_policy.reorg_window_blocks(),
                pair_convergence_attempts: config.pair_convergence_attempts.get(),
                epoch_pin_retention_seconds: config.epoch_pin_retention.seconds(),
                tree_state_replay_wait_ms: u64::try_from(config.tree_state_replay_wait.as_millis())
                    .unwrap_or(u64::MAX),
                tree_state_replay_waiters_per_slot: config.tree_state_replay_waiters_per_slot.get(),
                forwarded_read_max_concurrent: config.forwarded_reads.max_concurrent().get(),
                forwarded_read_wait_ms: u64::try_from(config.forwarded_reads.wait().as_millis())
                    .unwrap_or(u64::MAX),
                forwarded_read_waiters_per_slot: config.forwarded_reads.waiters_per_slot().get(),
                forwarded_transaction_cache_bytes: config.forwarded_reads.transaction_cache_bytes(),
                trust_proxy_client_address: config
                    .client_address_source
                    .trust_proxy_client_address(),
                proxy_protocol_trusted_cidrs: config.proxy_protocol.redacted_cidrs(),
                range_admission: config.range_admission,
            },
            node: config.node.as_ref().map(NodeToml::from_node_target),
            node_fallbacks: NodeFallbacksToml::from_node_targets(&config.node_fallbacks),
        }
    }
}

#[derive(Serialize)]
struct QueryToml {
    listen_addr: String,
    browser_origins: BrowserOrigins,
    reorg_window_blocks: u32,
    pair_convergence_attempts: u8,
    epoch_pin_retention_seconds: u64,
    tree_state_replay_wait_ms: u64,
    tree_state_replay_waiters_per_slot: u32,
    forwarded_read_max_concurrent: u32,
    forwarded_read_wait_ms: u64,
    forwarded_read_waiters_per_slot: u32,
    forwarded_transaction_cache_bytes: u64,
    trust_proxy_client_address: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    proxy_protocol_trusted_cidrs: Option<&'static str>,
    range_admission: GrpcRangeAdmission,
}

#[derive(Serialize)]
struct WalletToml {
    path: PathBuf,
    secondary_path: PathBuf,
    rocksdb: RocksDbResourceBudgetToml,
}
