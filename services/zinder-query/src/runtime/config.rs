//! Configuration loading for the `zinder-query` runtime.

use std::{
    net::SocketAddr,
    num::{NonZeroU8, NonZeroU32},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use thiserror::Error;
use zinder_core::Network;
use zinder_runtime::{
    BearerToken, BearerTokenError, BrowserOrigins, ClientAddressSource, ConfigError, ConfigLoader,
    DEFAULT_ALLOW_PUBLIC_BIND, DeploymentProfile, DeploymentSection, GrpcProxyProtocol,
    GrpcRangeAdmission, IngestControlSection, NetworkSection, OpsSection, OpsServerError,
    QuerySection, RedactedCidrs, ResolvedCanonicalSecondaryStorage, ResolvedIngestControlReader,
    RuntimeService, SecuritySection, StorageSection, WalletSection, ZinderConfig,
    derive_canonical_path, derive_canonical_secondary_path, derive_wallet_path,
    derive_wallet_secondary_path, guard_optional_serving_bind, guard_serving_bind,
    parse_socket_addr, require_field, resolve_allow_public_bind,
    resolve_canonical_secondary_storage, resolve_ingest_control_reader, resolve_ops_listen_addr,
    resolve_wallet_projection_reader_rocksdb_budget, section_defaults::DEFAULT_DATA_DIR,
    validate_optional_data_dir, validate_optional_path,
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
    let raw_config: ZinderConfig = ConfigLoader::new()
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

    resolve_query_config_from_raw(raw_config)
}

/// Renders a resolved native query configuration as redacted TOML for `--print-config`.
pub fn query_config_toml(config: &QueryConfig) -> Result<String, QueryConfigError> {
    let mut storage = StorageSection::from_canonical_secondary(&config.storage);
    if config.profile.is_some() {
        storage.raw_blob_policy = None;
    }
    let toml_config = ZinderConfig {
        deployment: DeploymentSection::from_resolved(config.profile),
        network: NetworkSection::from_network(config.network),
        ops: OpsSection::from_resolved(config.ops_listen_addr),
        observability: config.observability.clone(),
        security: SecuritySection::from_resolved(config.allow_public_bind),
        storage,
        wallet: WalletSection::from_resolved_reader(
            config.wallet_primary_path.clone(),
            config.wallet_secondary_root.clone(),
            config.wallet_rocksdb_budget,
        ),
        ingest_control: IngestControlSection::from_reader(&ResolvedIngestControlReader {
            addr: config.ingest_control_addr.clone(),
            bearer_token_path: config.ingest_control_bearer_token_path.clone(),
            bearer_token: config.ingest_control_bearer_token.clone(),
        }),
        query: QuerySection {
            listen_addr: Some(config.listen_addr.to_string()),
            browser_origins: config.browser_origins.clone(),
            reorg_window_blocks: Some(config.canonical_reorg_policy.reorg_window_blocks()),
            pair_convergence_attempts: Some(config.pair_convergence_attempts.get()),
            epoch_pin_retention_seconds: Some(config.epoch_pin_retention.seconds()),
            tree_state_replay_wait_ms: Some(
                u64::try_from(config.tree_state_replay_wait.as_millis()).unwrap_or(u64::MAX),
            ),
            tree_state_replay_waiters_per_slot: Some(
                config.tree_state_replay_waiters_per_slot.get(),
            ),
            forwarded_read_max_concurrent: Some(config.forwarded_reads.max_concurrent().get()),
            forwarded_read_wait_ms: Some(
                u64::try_from(config.forwarded_reads.wait().as_millis()).unwrap_or(u64::MAX),
            ),
            forwarded_read_waiters_per_slot: Some(config.forwarded_reads.waiters_per_slot().get()),
            forwarded_transaction_cache_bytes: Some(
                config.forwarded_reads.transaction_cache_bytes(),
            ),
            trust_proxy_client_address: Some(
                config.client_address_source.trust_proxy_client_address(),
            ),
            proxy_protocol_trusted_cidrs: RedactedCidrs::from_resolved(
                config.proxy_protocol.is_enabled(),
            ),
            range_admission: config.range_admission,
        },
        node: config
            .node
            .as_ref()
            .map_or_default(NodeSection::from_resolved),
        node_fallbacks: NodeFallbackSections::from_resolved(&config.node_fallbacks),
        ..ZinderConfig::default()
    };
    toml::to_string(&toml_config).map_err(|source| ConfigError::Render { source }.into())
}

fn apply_deployment_profile(config: &mut ZinderConfig) -> Result<(), ConfigError> {
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
            && config.node.is_empty()
        {
            return Err(ConfigError::invalid(format!(
                "the {} profile requires [node] for verified mined transaction bytes",
                profile.as_str(),
            )));
        }
    }
    Ok(())
}

fn derive_query_storage_paths(config: &mut ZinderConfig) -> Result<(), ConfigError> {
    config.storage.data_dir = validate_optional_data_dir(config.storage.data_dir.take())?;
    config.wallet.path = validate_optional_path(config.wallet.path.take(), "wallet.path")?;
    config.wallet.secondary_path =
        validate_optional_path(config.wallet.secondary_path.take(), "wallet.secondary_path")?;
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

struct QueryListenersAndProxy {
    listen_addr: SocketAddr,
    ops_listen_addr: Option<SocketAddr>,
    allow_public_bind: bool,
    client_address_source: ClientAddressSource,
    proxy_protocol: GrpcProxyProtocol,
}

fn resolve_query_listeners(
    query: &QuerySection,
    ops: OpsSection,
    security: SecuritySection,
) -> Result<QueryListenersAndProxy, QueryConfigError> {
    let listen_addr_text = query
        .listen_addr
        .clone()
        .unwrap_or_else(|| "127.0.0.1:9102".to_owned());
    let listen_addr = parse_socket_addr("query.listen_addr", &listen_addr_text)?;
    let ops_listen_addr = resolve_ops_listen_addr(ops)?;
    let allow_public_bind = resolve_allow_public_bind(SecuritySection {
        allow_public_bind: Some(
            security
                .allow_public_bind
                .unwrap_or(DEFAULT_ALLOW_PUBLIC_BIND),
        ),
    })?;
    guard_serving_bind("query.listen_addr", listen_addr, allow_public_bind)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;
    let client_address_source = ClientAddressSource::from_trust_proxy_client_address(
        query.trust_proxy_client_address.unwrap_or(false),
    );
    let proxy_protocol = GrpcProxyProtocol::resolve(
        query.proxy_protocol_trusted_cidrs.as_deref(),
        client_address_source,
    )?;
    Ok(QueryListenersAndProxy {
        listen_addr,
        ops_listen_addr,
        allow_public_bind,
        client_address_source,
        proxy_protocol,
    })
}

/// Validates and resolves an already-deserialized unified [`ZinderConfig`] for `zinder-query`.
pub fn resolve_query_config_from_raw(
    mut config: ZinderConfig,
) -> Result<QueryConfig, QueryConfigError> {
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
    let ingest_control = resolve_ingest_control_reader(config.ingest_control)?;
    let listeners = resolve_query_listeners(&config.query, config.ops, config.security)?;
    let canonical_reorg_policy = CanonicalReorgPolicy::new(
        config
            .query
            .reorg_window_blocks
            .unwrap_or(zinder_store::DEFAULT_REORG_WINDOW_BLOCKS),
    )?;
    let forwarded_reads = ForwardedReadConfig::resolve(
        config.query.forwarded_read_max_concurrent,
        config.query.forwarded_read_wait_ms,
        config.query.forwarded_read_waiters_per_slot,
        config.query.forwarded_transaction_cache_bytes,
    )
    .map_err(|error| ConfigError::invalid(format!("query.{error}")))?;
    let (node, node_fallbacks) = resolve_query_nodes(network, config.node, config.node_fallbacks)?;

    Ok(QueryConfig {
        profile: config.deployment.profile,
        network,
        storage,
        wallet_primary_path,
        wallet_secondary_root,
        wallet_rocksdb_budget,
        ingest_control_addr: ingest_control.addr,
        ingest_control_bearer_token_path: ingest_control.bearer_token_path,
        ingest_control_bearer_token: ingest_control.bearer_token,
        listen_addr: listeners.listen_addr,
        browser_origins: config.query.browser_origins,
        ops_listen_addr: listeners.ops_listen_addr,
        observability: config.observability,
        allow_public_bind: listeners.allow_public_bind,
        client_address_source: listeners.client_address_source,
        proxy_protocol: listeners.proxy_protocol,
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
    node: NodeSection,
    fallbacks: NodeFallbackSections,
) -> Result<(Option<NodeTarget>, Vec<NodeTarget>), ConfigError> {
    let node = (!node.is_empty())
        .then_some(node)
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
