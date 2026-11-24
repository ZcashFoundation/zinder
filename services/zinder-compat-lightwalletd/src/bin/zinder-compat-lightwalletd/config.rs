//! Configuration loading for the `zinder-compat-lightwalletd` binary.

use std::{
    net::SocketAddr,
    num::{NonZeroU8, NonZeroU32},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use clap::ValueEnum;
use http::HeaderValue;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use thiserror::Error;
use url::Url;
use zinder_compat_lightwalletd::{
    DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS, DEFAULT_STALE_TIP_LIMIT, DEFAULT_TIP_WAIT,
    DEFAULT_TIP_WAIT_MAX_BLOCKS, DEFAULT_TIP_WAIT_MAX_WAITERS, LightwalletdAdmissionError,
    StaleTipLimit,
};
use zinder_core::Network;
use zinder_query::{
    DEFAULT_TREE_STATE_REPLAY_WAIT, DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
    MAX_TREE_STATE_REPLAY_WAIT, MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT,
};
use zinder_runtime::{
    BearerToken, BearerTokenError, CanonicalSecondaryStorageSection, CanonicalSecondaryStorageToml,
    ClientAddressSource, ConfigError, ConfigLoader, DeploymentProfile, DeploymentSection,
    DeploymentToml, IngestControlReaderToml, IngestControlSection, NetworkSection, NetworkToml,
    NodeFallbacksToml, NodeToml, OpsSection, OpsToml, ResolvedCanonicalSecondaryStorage,
    ResolvedIngestControlReader, RocksDbResourceBudgetSection, RocksDbResourceBudgetToml,
    RuntimeService, SecuritySection, SecurityToml, StorageRoleToml, guard_optional_serving_bind,
    guard_serving_bind, parse_socket_addr, require_field, resolve_allow_public_bind,
    resolve_canonical_reader_rocksdb_budget, resolve_canonical_secondary_storage,
    resolve_ingest_control_reader, resolve_ops_listen_addr,
    resolve_wallet_projection_reader_rocksdb_budget,
};
use zinder_source::{NodeFallbackSections, NodeSection, NodeTarget};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreBuildPlanError, CanonicalStoreRequirements,
    RocksDbResourceBudget, StoreError,
};

/// Operator-selected lightwalletd compatibility capability.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CompatServing {
    /// Full wallet-compatible serving over an admitted canonical and wallet pair.
    #[default]
    Wallet,
    /// Canonical compact-block serving without wallet storage.
    CompactBlocks,
    /// Canonical wallet reads with node-backed transparent methods.
    LightWallet,
}

impl CompatServing {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Wallet => "wallet",
            Self::CompactBlocks => "compact-blocks",
            Self::LightWallet => "light-wallet",
        }
    }
}

/// Resolved lightwalletd compatibility runtime configuration.
#[derive(Clone, Debug)]
pub(crate) struct LightwalletdConfig {
    profile: Option<DeploymentProfile>,
    pub(crate) network: Network,
    pub(crate) serving: CompatServing,
    pub(crate) shielded_history: Option<ShieldedHistorySection>,
    pub(crate) storage: ResolvedCanonicalSecondaryStorage,
    pub(crate) wallet_primary_path: Option<PathBuf>,
    pub(crate) wallet_secondary_root: Option<PathBuf>,
    pub(crate) wallet_rocksdb_budget: Option<RocksDbResourceBudget>,
    pub(crate) ingest_control_addr: String,
    pub(crate) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(crate) ingest_control_bearer_token: Option<BearerToken>,
    pub(crate) listen_addr: SocketAddr,
    pub(crate) browser_origins: Vec<HeaderValue>,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
    pub(crate) allow_public_bind: bool,
    /// How the gRPC listener tells clients apart for its receive limit.
    pub(crate) client_address_source: ClientAddressSource,
    pub(crate) canonical_reorg_policy: CanonicalReorgPolicy,
    pub(crate) pair_convergence_attempts: NonZeroU8,
    pub(crate) max_concurrent_block_range_streams: NonZeroU32,
    /// Longest a tree-state rebuild waits for a rebuild slot in wallet
    /// serving. Compact-block serving answers no tree-state request and has
    /// none.
    pub(crate) tree_state_replay_wait: Option<Duration>,
    /// Most tree-state rebuilds that wait for each rebuild slot in wallet
    /// serving. Compact-block serving has none.
    pub(crate) tree_state_replay_waiters_per_slot: Option<NonZeroU32>,
    /// How old the served tip may be before `GetLatestBlock` and
    /// `GetLightdInfo` fail with `UNAVAILABLE`.
    pub(crate) stale_tip_limit: StaleTipLimit,
    /// Longest a height-selected block read waits for a height just past the
    /// visible tip. Zero disables the wait.
    pub(crate) tip_wait: Duration,
    /// Furthest past the visible tip, in blocks, a read may name and still
    /// wait.
    pub(crate) tip_wait_max_blocks: NonZeroU32,
    /// Most reads that wait for the tip at once.
    pub(crate) tip_wait_max_waiters: NonZeroU32,
    /// Whether `Ping` answers, as `lightwalletd --ping-very-insecure` does.
    pub(crate) ping_very_insecure: bool,
    pub(crate) broadcaster: Option<NodeTarget>,
    /// Endpoints wallet-mode broadcast tries after `broadcaster`, in position
    /// order. Compact-blocks serving refuses them.
    pub(crate) node_fallbacks: Vec<NodeTarget>,
}

/// Command-line overrides for the lightwalletd compat command.
#[derive(Debug, Default)]
pub(crate) struct LightwalletdConfigOverrides {
    pub(crate) network: Option<String>,
    pub(crate) serving: Option<CompatServing>,
    pub(crate) canonical_primary_path: Option<PathBuf>,
    pub(crate) canonical_secondary_root: Option<PathBuf>,
    pub(crate) wallet_primary_path: Option<PathBuf>,
    pub(crate) wallet_secondary_root: Option<PathBuf>,
    pub(crate) ingest_control_addr: Option<String>,
    pub(crate) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(crate) listen_addr: Option<SocketAddr>,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
    pub(crate) node_json_rpc_addr: Option<String>,
    pub(crate) reorg_window_blocks: Option<u32>,
    pub(crate) ping_very_insecure: bool,
}

/// Error returned while resolving lightwalletd compat config or running its gRPC server.
#[derive(Debug, Error)]
pub(crate) enum LightwalletdConfigError {
    #[error("shielded history publication failed: {0}")]
    ShieldedHistory(#[from] std::io::Error),

    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Store(#[from] StoreError),

    #[error(transparent)]
    CanonicalStore(#[from] zinder_store::CanonicalStoreError),

    #[error(transparent)]
    CanonicalStoreBuildPlan(#[from] CanonicalStoreBuildPlanError),

    #[error(transparent)]
    WalletStore(#[from] zinder_wallet_rocksdb::RocksDbWalletError),

    #[error(transparent)]
    WalletServingPair(#[from] zinder_query::WalletServingPairError),

    #[error(transparent)]
    IngestControlAdmission(#[from] zinder_query::IngestControlAdmissionError),

    #[error(transparent)]
    Query(#[from] zinder_query::QueryError),

    #[error(transparent)]
    CompatibilityAdmission(#[from] LightwalletdAdmissionError),

    #[error(transparent)]
    CompactServing(#[from] crate::compact_block_serving::CompactBlockServingError),

    #[error("node source initialization failed: {0}")]
    Source(Box<zinder_source::SourceError>),

    #[error("gRPC transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("could not open gRPC listener at {addr}: {source}")]
    Listen {
        addr: SocketAddr,
        source: std::io::Error,
    },

    #[error("invalid ingest-control bearer token: {0}")]
    BearerToken(#[from] BearerTokenError),

    #[error("gRPC reflection service build failed: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),

    #[error(transparent)]
    Operations(#[from] zinder_runtime::OpsServerError),

    #[error("compatibility runtime stopped: {0}")]
    Supervision(#[from] zinder_runtime::RuntimeSupervisionError),
}

impl LightwalletdConfig {
    pub(crate) fn canonical_requirements(&self) -> CanonicalStoreRequirements {
        let profile = self.profile.or_else(|| {
            (self.serving == CompatServing::LightWallet).then_some(DeploymentProfile::LightWallet)
        });
        let requirements = profile.map_or_else(
            CanonicalStoreRequirements::default,
            DeploymentProfile::canonical_requirement,
        );
        requirements.with_blob_retention(self.storage.expected_raw_blob_retention)
    }
}

/// Loads and validates lightwalletd compat configuration.
pub(crate) fn load_lightwalletd_config(
    config_path: Option<PathBuf>,
    overrides: LightwalletdConfigOverrides,
) -> Result<LightwalletdConfig, LightwalletdConfigError> {
    resolve_lightwalletd_config(load_raw_config(config_path, overrides)?)
}

pub(super) struct HistoryPublisherConfig {
    pub(super) network: Network,
    pub(super) primary_path: PathBuf,
    pub(super) secondary_path: PathBuf,
    pub(super) canonical_rocksdb_budget: RocksDbResourceBudget,
    pub(super) canonical_reorg_policy: CanonicalReorgPolicy,
}

impl HistoryPublisherConfig {
    pub(super) fn to_toml(&self) -> Result<String, LightwalletdConfigError> {
        let rendered = HistoryPublisherToml {
            network: NetworkToml::from_network(self.network),
            storage: HistoryPublisherStorageToml {
                path: &self.primary_path,
                secondary_path: &self.secondary_path,
                canonical: StorageRoleToml::from_resolved(self.canonical_rocksdb_budget),
            },
            compat: HistoryPublisherCompatToml {
                reorg_window_blocks: self.canonical_reorg_policy.reorg_window_blocks(),
            },
        };
        Ok(toml::to_string(&rendered).map_err(|source| ConfigError::Render { source })?)
    }
}

#[derive(Serialize)]
struct HistoryPublisherToml<'a> {
    network: NetworkToml,
    storage: HistoryPublisherStorageToml<'a>,
    compat: HistoryPublisherCompatToml,
}

#[derive(Serialize)]
struct HistoryPublisherStorageToml<'a> {
    path: &'a Path,
    secondary_path: &'a Path,
    canonical: StorageRoleToml,
}

#[derive(Serialize)]
struct HistoryPublisherCompatToml {
    reorg_window_blocks: u32,
}

pub(super) fn load_history_publisher_config(
    config_path: Option<PathBuf>,
    overrides: LightwalletdConfigOverrides,
) -> Result<HistoryPublisherConfig, LightwalletdConfigError> {
    let config = load_raw_config(config_path, overrides)?;
    Ok(HistoryPublisherConfig {
        network: config.network.resolve()?,
        primary_path: require_field(config.storage.path, "storage.path")?,
        secondary_path: require_field(config.storage.secondary_path, "storage.secondary_path")?,
        canonical_rocksdb_budget: resolve_canonical_reader_rocksdb_budget(
            config.storage.canonical.rocksdb,
        )?,
        canonical_reorg_policy: resolve_reorg_policy(&config.compat)?,
    })
}

fn load_raw_config(
    config_path: Option<PathBuf>,
    overrides: LightwalletdConfigOverrides,
) -> Result<LightwalletdRawConfig, ConfigError> {
    ConfigLoader::new()
        .with_default("compat.listen_addr", "127.0.0.1:9067")?
        .with_default(
            "compat.reorg_window_blocks",
            zinder_store::DEFAULT_REORG_WINDOW_BLOCKS,
        )?
        .with_ops_section(RuntimeService::CompatLightwalletd)?
        .with_security_section()?
        .with_file(config_path)
        .with_zinder_env()?
        .with_override_if("network.name", overrides.network)?
        .with_override_if(
            "compat.serving",
            overrides.serving.map(|serving| serving.as_str().to_owned()),
        )?
        .with_override_path_if("storage.path", overrides.canonical_primary_path)?
        .with_override_path_if("storage.secondary_path", overrides.canonical_secondary_root)?
        .with_override_path_if("wallet.path", overrides.wallet_primary_path)?
        .with_override_path_if("wallet.secondary_path", overrides.wallet_secondary_root)?
        .with_override_if("ingest_control.addr", overrides.ingest_control_addr)?
        .with_override_path_if(
            "ingest_control.bearer_token_path",
            overrides.ingest_control_bearer_token_path,
        )?
        .with_override_if(
            "compat.listen_addr",
            overrides.listen_addr.map(|addr| addr.to_string()),
        )?
        .with_override_if(
            "ops.listen_addr",
            overrides.ops_listen_addr.map(|addr| addr.to_string()),
        )?
        .with_override_if("node.json_rpc_addr", overrides.node_json_rpc_addr)?
        .with_override_if("compat.reorg_window_blocks", overrides.reorg_window_blocks)?
        .with_override_if(
            "compat.ping_very_insecure",
            overrides.ping_very_insecure.then_some(true),
        )?
        .load()
}

/// Renders the effective lightwalletd compat configuration in the accepted TOML shape.
pub(crate) fn lightwalletd_config_toml(
    config: &LightwalletdConfig,
) -> Result<String, LightwalletdConfigError> {
    let rendered = toml::to_string(&LightwalletdConfigToml::from_lightwalletd_config(config))
        .map_err(|source| ConfigError::Render { source })?;
    Ok(rendered)
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LightwalletdRawConfig {
    deployment: DeploymentSection,
    network: NetworkSection,
    ops: OpsSection,
    storage: CanonicalSecondaryStorageSection,
    wallet: Option<WalletSection>,
    ingest_control: IngestControlSection,
    compat: CompatSection,
    node: Option<NodeSection>,
    node_fallbacks: NodeFallbackSections,
    security: SecuritySection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CompatSection {
    shielded_history: Option<ShieldedHistorySection>,
    listen_addr: Option<String>,
    #[serde(deserialize_with = "deserialize_browser_origins")]
    browser_origins: Vec<HeaderValue>,
    reorg_window_blocks: Option<u32>,
    pair_convergence_attempts: Option<u8>,
    max_concurrent_block_range_streams: Option<u32>,
    tree_state_replay_wait_ms: Option<u64>,
    tree_state_replay_waiters_per_slot: Option<u32>,
    stale_tip_limit_ms: Option<u64>,
    tip_wait_ms: Option<u64>,
    tip_wait_max_blocks: Option<u32>,
    tip_wait_max_waiters: Option<u32>,
    ping_very_insecure: Option<bool>,
    serving: Option<CompatServing>,
    trust_proxy_client_address: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "ShieldedHistoryConfig")]
pub(crate) struct ShieldedHistorySection {
    #[serde(flatten)]
    pub(crate) location: HistoryLocation,
    pub(crate) publisher_key: PathBuf,
    #[serde(default)]
    pub(crate) require_fs_verity: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShieldedHistoryConfig {
    directory: Option<PathBuf>,
    manifest: Option<PathBuf>,
    publisher_key: PathBuf,
    #[serde(default)]
    require_fs_verity: bool,
}

impl TryFrom<ShieldedHistoryConfig> for ShieldedHistorySection {
    type Error = &'static str;

    fn try_from(config: ShieldedHistoryConfig) -> Result<Self, Self::Error> {
        let location = match (config.directory, config.manifest) {
            (Some(path), None) => HistoryLocation::Directory(path),
            (None, Some(path)) => HistoryLocation::Manifest(path),
            _ => return Err("shielded history requires exactly one directory or manifest"),
        };
        Ok(Self {
            location,
            publisher_key: config.publisher_key,
            require_fs_verity: config.require_fs_verity,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryLocation {
    Manifest(PathBuf),
    Directory(PathBuf),
}

impl HistoryLocation {
    pub(super) fn manifest(&self) -> PathBuf {
        match self {
            Self::Manifest(path) => path.clone(),
            Self::Directory(path) => path.join("current.json"),
        }
    }
}

fn deserialize_browser_origins<'de, D>(deserializer: D) -> Result<Vec<HeaderValue>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(
        untagged,
        expecting = "an array of browser origins or a comma-separated list"
    )]
    enum Origins {
        List(Vec<String>),
        CommaSeparated(String),
    }

    let origins = match Origins::deserialize(deserializer)? {
        Origins::List(origins) => origins,
        Origins::CommaSeparated(origins) if origins.is_empty() => Vec::new(),
        Origins::CommaSeparated(origins) => origins
            .split(',')
            .map(|origin| origin.trim().to_owned())
            .collect(),
    };
    if origins.iter().any(|origin| origin == "*") {
        if origins.len() == 1 {
            return Ok(vec![HeaderValue::from_static("*")]);
        }
        return Err(D::Error::custom(
            "compat.browser_origins must use '*' alone",
        ));
    }

    let invalid_origin = || {
        D::Error::custom(
            "compat.browser_origins requires an exact HTTP or HTTPS origin such as https://wallet.example",
        )
    };
    let mut headers = Vec::with_capacity(origins.len());
    for origin in origins {
        let (scheme, authority) = origin
            .strip_suffix('/')
            .unwrap_or(&origin)
            .split_once("://")
            .ok_or_else(invalid_origin)?;
        if !(scheme.eq_ignore_ascii_case("https") || scheme.eq_ignore_ascii_case("http"))
            || authority.contains(['/', '\\', '@', '?', '#'])
            || authority.bytes().any(|b| b.is_ascii_control() || b == b' ')
        {
            return Err(invalid_origin());
        }
        let url = Url::parse(&origin).map_err(|_| invalid_origin())?;
        let serialized = url.origin().ascii_serialization();
        if serialized.contains('*') {
            return Err(invalid_origin());
        }
        headers.push(HeaderValue::try_from(serialized).map_err(|_| invalid_origin())?);
    }
    Ok(headers)
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WalletSection {
    path: Option<PathBuf>,
    secondary_path: Option<PathBuf>,
    rocksdb: RocksDbResourceBudgetSection,
}

fn resolve_lightwalletd_config(
    mut config: LightwalletdRawConfig,
) -> Result<LightwalletdConfig, LightwalletdConfigError> {
    let serving = resolve_serving(&mut config)?;
    let wallet = config.wallet;
    if serving != CompatServing::Wallet && wallet.is_some() {
        return Err(ConfigError::invalid(format!(
            "compat.serving = \"{}\" does not accept wallet.path, wallet.secondary_path, or wallet.rocksdb settings; remove the wallet section or select compat.serving = \"wallet\"", serving.as_str()
        ))
        .into());
    }
    let network = config.network.resolve()?;
    let storage = resolve_canonical_secondary_storage(config.storage)?;
    let (wallet_primary_path, wallet_secondary_root, wallet_rocksdb_budget) =
        resolve_wallet_storage(serving, wallet.unwrap_or_default(), &storage)?;
    let (tree_state_replay_wait, tree_state_replay_waiters_per_slot) =
        resolve_tree_state_replay(serving, &config.compat)?;
    let (tip_wait, tip_wait_max_blocks, tip_wait_max_waiters) =
        resolve_tip_wait(&config.compat, storage.secondary_catchup_interval)?;
    let ResolvedIngestControlReader {
        addr: ingest_control_addr,
        bearer_token_path: ingest_control_bearer_token_path,
        bearer_token: ingest_control_bearer_token,
    } = resolve_ingest_control_reader(config.ingest_control)?;
    let canonical_reorg_policy = resolve_reorg_policy(&config.compat)?;
    let listen_addr_string = require_field(config.compat.listen_addr, "compat.listen_addr")?;
    let listen_addr = parse_socket_addr("compat.listen_addr", &listen_addr_string)?;
    let ops_listen_addr = resolve_ops_listen_addr(config.ops)?;
    let allow_public_bind = resolve_allow_public_bind(config.security)?;
    let pair_convergence_attempts =
        require_pair_convergence_attempts(config.compat.pair_convergence_attempts)?;
    let max_concurrent_block_range_streams = require_max_concurrent_block_range_streams(
        config.compat.max_concurrent_block_range_streams,
    )?;
    let stale_tip_limit = resolve_stale_tip_limit(config.compat.stale_tip_limit_ms)?;
    guard_serving_bind("compat.listen_addr", listen_addr, allow_public_bind)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;
    let (broadcaster, node_fallbacks) =
        resolve_node_list(serving, network, config.node, config.node_fallbacks)?;

    if config.deployment.profile == Some(DeploymentProfile::Wallet) && broadcaster.is_none() {
        return Err(ConfigError::invalid(
            "the wallet profile requires [node] for verified mined transaction bytes",
        )
        .into());
    }

    Ok(LightwalletdConfig {
        profile: config.deployment.profile,
        network,
        serving,
        storage,
        wallet_primary_path,
        wallet_secondary_root,
        wallet_rocksdb_budget,
        ingest_control_addr,
        ingest_control_bearer_token_path,
        ingest_control_bearer_token,
        listen_addr,
        browser_origins: config.compat.browser_origins,
        shielded_history: config.compat.shielded_history,
        ops_listen_addr,
        allow_public_bind,
        client_address_source: ClientAddressSource::from_trust_proxy_client_address(
            config.compat.trust_proxy_client_address.unwrap_or(false),
        ),
        canonical_reorg_policy,
        pair_convergence_attempts,
        max_concurrent_block_range_streams,
        tree_state_replay_wait,
        tree_state_replay_waiters_per_slot,
        stale_tip_limit,
        tip_wait,
        tip_wait_max_blocks,
        tip_wait_max_waiters,
        ping_very_insecure: config.compat.ping_very_insecure.unwrap_or(false),
        broadcaster,
        node_fallbacks,
    })
}

fn resolve_reorg_policy(
    compat: &CompatSection,
) -> Result<CanonicalReorgPolicy, LightwalletdConfigError> {
    Ok(CanonicalReorgPolicy::new(require_field(
        compat.reorg_window_blocks,
        "compat.reorg_window_blocks",
    )?)?)
}

fn resolve_serving(config: &mut LightwalletdRawConfig) -> Result<CompatServing, ConfigError> {
    let Some(profile) = config.deployment.profile else {
        return Ok(config.compat.serving.unwrap_or_default());
    };
    let serving = if profile == DeploymentProfile::LightWallet {
        CompatServing::LightWallet
    } else {
        CompatServing::Wallet
    };
    if config
        .compat
        .serving
        .is_some_and(|configured| configured != serving)
    {
        return Err(ConfigError::invalid(format!(
            "deployment.profile = \"{}\" requires compat.serving = \"{}\". To select this profile, remove conflicting overrides",
            profile.as_str(),
            serving.as_str(),
        )));
    }
    config.storage.raw_blob_policy =
        Some(profile.resolve_raw_blob_retention(config.storage.raw_blob_policy)?);
    Ok(serving)
}

/// The wallet primary path, secondary root, and `RocksDB` budget that wallet
/// serving opens. Compact-block serving opens none.
type WalletStorage = (
    Option<PathBuf>,
    Option<PathBuf>,
    Option<RocksDbResourceBudget>,
);

fn resolve_wallet_storage(
    serving: CompatServing,
    wallet: WalletSection,
    storage: &ResolvedCanonicalSecondaryStorage,
) -> Result<WalletStorage, LightwalletdConfigError> {
    if serving != CompatServing::Wallet {
        return Ok((None, None, None));
    }
    let wallet_primary_path = require_field(wallet.path, "wallet.path")?;
    let wallet_secondary_root = require_field(wallet.secondary_path, "wallet.secondary_path")?;
    let wallet_rocksdb_budget = resolve_wallet_projection_reader_rocksdb_budget(wallet.rocksdb)?;
    require_distinct_storage_paths(
        &storage.path,
        &storage.secondary_path,
        &wallet_primary_path,
        &wallet_secondary_root,
    )?;
    Ok((
        Some(wallet_primary_path),
        Some(wallet_secondary_root),
        Some(wallet_rocksdb_budget),
    ))
}

/// Resolves the wallet-mode broadcast node and its fallbacks.
///
/// Any node setting asks for broadcast, so a section without
/// `node.json_rpc_addr` is an error rather than a compatibility process
/// without `SendTransaction`. Compact-block serving never contacts a node and
/// rejects every node setting.
fn resolve_node_list(
    serving: CompatServing,
    network: Network,
    node: Option<NodeSection>,
    node_fallbacks: NodeFallbackSections,
) -> Result<(Option<NodeTarget>, Vec<NodeTarget>), ConfigError> {
    if matches!(serving, CompatServing::CompactBlocks)
        && (node.is_some() || !node_fallbacks.is_empty())
    {
        return Err(ConfigError::invalid(
            "compat.serving = \"compact-blocks\" does not accept node settings, because compact-block serving never contacts a node; remove the node and node_fallbacks sections and every ZINDER_NODE__* and ZINDER_NODE_FALLBACKS__* variable, or select compat.serving = \"wallet\"",
        ));
    }
    if serving == CompatServing::LightWallet && (node.is_none() || !node_fallbacks.is_empty()) {
        return Err(ConfigError::invalid(
            "light-wallet serving requires one [node] and does not accept node_fallbacks",
        ));
    }
    let broadcaster = node
        .map(|section| NodeTarget::resolve(network, section))
        .transpose()?;
    let node_fallbacks = NodeTarget::resolve_fallbacks(network, node_fallbacks)?;
    NodeTarget::require_primary_for_fallbacks(broadcaster.as_ref(), &node_fallbacks)?;
    Ok((broadcaster, node_fallbacks))
}

#[derive(Serialize)]
struct LightwalletdConfigToml {
    #[serde(skip_serializing_if = "DeploymentToml::is_empty")]
    deployment: DeploymentToml,
    network: NetworkToml,
    ops: OpsToml,
    security: SecurityToml,
    storage: CanonicalSecondaryStorageToml,
    #[serde(skip_serializing_if = "Option::is_none")]
    wallet: Option<WalletToml>,
    ingest_control: IngestControlReaderToml,
    compat: CompatToml,
    #[serde(skip_serializing_if = "Option::is_none")]
    node: Option<NodeToml>,
    #[serde(skip_serializing_if = "NodeFallbacksToml::is_empty")]
    node_fallbacks: NodeFallbacksToml,
}

impl LightwalletdConfigToml {
    fn from_lightwalletd_config(config: &LightwalletdConfig) -> Self {
        let mut storage = CanonicalSecondaryStorageToml::from_resolved(&config.storage);
        if config.profile.is_some() {
            storage.raw_blob_policy = None;
        }
        Self {
            deployment: DeploymentToml::from_resolved(config.profile),
            network: NetworkToml::from_network(config.network),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            security: SecurityToml::from_resolved(config.allow_public_bind),
            storage,
            wallet: config
                .wallet_primary_path
                .as_ref()
                .zip(config.wallet_secondary_root.as_ref())
                .zip(config.wallet_rocksdb_budget)
                .map(|((path, secondary_path), rocksdb)| WalletToml {
                    path: path.clone(),
                    secondary_path: secondary_path.clone(),
                    rocksdb: RocksDbResourceBudgetToml::from_resolved(rocksdb),
                }),
            ingest_control: IngestControlReaderToml::from_resolved(
                config.ingest_control_addr.clone(),
                config.ingest_control_bearer_token_path.as_deref(),
            ),
            compat: CompatToml {
                shielded_history: config.shielded_history.clone(),
                listen_addr: config.listen_addr.to_string(),
                browser_origins: config
                    .browser_origins
                    .iter()
                    .map(|origin| String::from_utf8_lossy(origin.as_bytes()).into_owned())
                    .collect(),
                reorg_window_blocks: config.canonical_reorg_policy.reorg_window_blocks(),
                pair_convergence_attempts: config.pair_convergence_attempts.get(),
                max_concurrent_block_range_streams: config.max_concurrent_block_range_streams.get(),
                tree_state_replay_wait_ms: config
                    .tree_state_replay_wait
                    .map(|wait| u64::try_from(wait.as_millis()).unwrap_or(u64::MAX)),
                tree_state_replay_waiters_per_slot: config
                    .tree_state_replay_waiters_per_slot
                    .map(NonZeroU32::get),
                stale_tip_limit_ms: config.stale_tip_limit.limit().map_or(0, |limit| {
                    u64::try_from(limit.as_millis()).unwrap_or(u64::MAX)
                }),
                tip_wait_ms: u64::try_from(config.tip_wait.as_millis()).unwrap_or(u64::MAX),
                tip_wait_max_blocks: config.tip_wait_max_blocks.get(),
                tip_wait_max_waiters: config.tip_wait_max_waiters.get(),
                ping_very_insecure: config.ping_very_insecure,
                serving: config.profile.is_none().then_some(config.serving),
                trust_proxy_client_address: config
                    .client_address_source
                    .trust_proxy_client_address(),
            },
            node: config.broadcaster.as_ref().map(NodeToml::from_node_target),
            node_fallbacks: NodeFallbacksToml::from_node_targets(&config.node_fallbacks),
        }
    }
}

#[derive(Serialize)]
struct CompatToml {
    #[serde(skip_serializing_if = "Option::is_none")]
    shielded_history: Option<ShieldedHistorySection>,
    listen_addr: String,
    browser_origins: Vec<String>,
    reorg_window_blocks: u32,
    pair_convergence_attempts: u8,
    max_concurrent_block_range_streams: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    tree_state_replay_wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tree_state_replay_waiters_per_slot: Option<u32>,
    stale_tip_limit_ms: u64,
    tip_wait_ms: u64,
    tip_wait_max_blocks: u32,
    tip_wait_max_waiters: u32,
    ping_very_insecure: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    serving: Option<CompatServing>,
    trust_proxy_client_address: bool,
}

#[derive(Serialize)]
struct WalletToml {
    path: PathBuf,
    secondary_path: PathBuf,
    rocksdb: RocksDbResourceBudgetToml,
}

const DEFAULT_PAIR_CONVERGENCE_ATTEMPTS: u8 = 12;
const MAX_PAIR_CONVERGENCE_ATTEMPTS: u8 = 64;

fn require_pair_convergence_attempts(
    configured_attempts: Option<u8>,
) -> Result<NonZeroU8, ConfigError> {
    let configured_attempts = configured_attempts.unwrap_or(DEFAULT_PAIR_CONVERGENCE_ATTEMPTS);
    let Some(non_zero_attempts) = NonZeroU8::new(configured_attempts) else {
        return Err(ConfigError::invalid(
            "compat.pair_convergence_attempts must be greater than zero",
        ));
    };
    if non_zero_attempts.get() > MAX_PAIR_CONVERGENCE_ATTEMPTS {
        return Err(ConfigError::invalid(format!(
            "compat.pair_convergence_attempts must not exceed {MAX_PAIR_CONVERGENCE_ATTEMPTS}",
        )));
    }
    Ok(non_zero_attempts)
}

/// Largest accepted `compat.max_concurrent_block_range_streams`.
const MAX_CONCURRENT_BLOCK_RANGE_STREAMS: u32 = 65_536;

fn require_max_concurrent_block_range_streams(
    configured_streams: Option<u32>,
) -> Result<NonZeroU32, ConfigError> {
    let Some(configured_streams) = configured_streams else {
        return Ok(DEFAULT_MAX_CONCURRENT_BLOCK_RANGE_STREAMS);
    };
    NonZeroU32::new(configured_streams)
        .filter(|streams| streams.get() <= MAX_CONCURRENT_BLOCK_RANGE_STREAMS)
        .ok_or_else(|| {
            ConfigError::invalid(format!(
                "compat.max_concurrent_block_range_streams must be between 1 and \
                 {MAX_CONCURRENT_BLOCK_RANGE_STREAMS}",
            ))
        })
}

/// Resolves the tree-state rebuild wait and waiters per slot, which only
/// wallet serving uses.
fn resolve_tree_state_replay(
    serving: CompatServing,
    compat: &CompatSection,
) -> Result<(Option<Duration>, Option<NonZeroU32>), ConfigError> {
    Ok((
        resolve_tree_state_replay_wait(serving, compat.tree_state_replay_wait_ms)?,
        resolve_tree_state_replay_waiters_per_slot(
            serving,
            compat.tree_state_replay_waiters_per_slot,
        )?,
    ))
}

/// Resolves the tree-state rebuild wait, which only wallet serving uses.
fn resolve_tree_state_replay_wait(
    serving: CompatServing,
    configured_ms: Option<u64>,
) -> Result<Option<Duration>, ConfigError> {
    match (serving == CompatServing::CompactBlocks, configured_ms) {
        (true, None) => Ok(None),
        (true, Some(_)) => Err(ConfigError::invalid(
            "compat.serving = \"compact-blocks\" does not accept compat.tree_state_replay_wait_ms, because compact-block serving answers no tree-state request; remove it or select compat.serving = \"wallet\"",
        )),
        (false, None) => Ok(Some(DEFAULT_TREE_STATE_REPLAY_WAIT)),
        (false, Some(configured_ms)) => {
            let wait = Duration::from_millis(configured_ms);
            if wait > MAX_TREE_STATE_REPLAY_WAIT {
                return Err(ConfigError::invalid(format!(
                    "compat.tree_state_replay_wait_ms must not exceed {}",
                    MAX_TREE_STATE_REPLAY_WAIT.as_millis()
                )));
            }
            Ok(Some(wait))
        }
    }
}

/// Resolves the tree-state rebuild waiters per slot, which only wallet
/// serving uses.
fn resolve_tree_state_replay_waiters_per_slot(
    serving: CompatServing,
    configured: Option<u32>,
) -> Result<Option<NonZeroU32>, ConfigError> {
    match (serving == CompatServing::CompactBlocks, configured) {
        (true, None) => Ok(None),
        (true, Some(_)) => Err(ConfigError::invalid(
            "compat.serving = \"compact-blocks\" does not accept compat.tree_state_replay_waiters_per_slot, because compact-block serving answers no tree-state request; remove it or select compat.serving = \"wallet\"",
        )),
        (false, None) => Ok(Some(DEFAULT_TREE_STATE_REPLAY_WAITERS_PER_SLOT)),
        (false, Some(configured)) => NonZeroU32::new(configured)
            .filter(|waiters| *waiters <= MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT)
            .map(Some)
            .ok_or_else(|| {
                ConfigError::invalid(format!(
                    "compat.tree_state_replay_waiters_per_slot must be in 1..={MAX_TREE_STATE_REPLAY_WAITERS_PER_SLOT}"
                ))
            }),
    }
}

/// Shortest accepted nonzero `compat.stale_tip_limit_ms`, which keeps a
/// limit written in seconds from refusing every tip call.
const MIN_STALE_TIP_LIMIT: Duration = Duration::from_mins(5);

/// Resolves the stale-tip limit: 0 disables it, and an unset limit takes
/// the default on every network.
fn resolve_stale_tip_limit(configured_ms: Option<u64>) -> Result<StaleTipLimit, ConfigError> {
    match configured_ms {
        None => Ok(StaleTipLimit::after(DEFAULT_STALE_TIP_LIMIT)),
        Some(0) => Ok(StaleTipLimit::DISABLED),
        Some(configured_ms) => {
            let limit = Duration::from_millis(configured_ms);
            if limit < MIN_STALE_TIP_LIMIT {
                return Err(ConfigError::invalid(format!(
                    "compat.stale_tip_limit_ms must be 0 or at least {}",
                    MIN_STALE_TIP_LIMIT.as_millis()
                )));
            }
            Ok(StaleTipLimit::after(limit))
        }
    }
}

/// Resolves the tip wait, its block distance, and its waiter cap.
///
/// Every serving mode shares them. A nonzero wait must span two catch-up
/// intervals, because a shorter one cannot reliably see the next publication.
fn resolve_tip_wait(
    compat: &CompatSection,
    catchup_interval: Duration,
) -> Result<(Duration, NonZeroU32, NonZeroU32), ConfigError> {
    let tip_wait = compat
        .tip_wait_ms
        .map_or(DEFAULT_TIP_WAIT, Duration::from_millis);
    if !tip_wait.is_zero() && tip_wait < catchup_interval.saturating_mul(2) {
        return Err(ConfigError::invalid(format!(
            "compat.tip_wait_ms must be 0 or at least twice storage.secondary_catchup_interval_ms ({}), because a shorter wait cannot reliably see the next publication",
            catchup_interval.saturating_mul(2).as_millis()
        )));
    }
    let max_blocks = match compat.tip_wait_max_blocks {
        None => DEFAULT_TIP_WAIT_MAX_BLOCKS,
        Some(configured) => NonZeroU32::new(configured)
            .ok_or_else(|| ConfigError::invalid("compat.tip_wait_max_blocks must be at least 1"))?,
    };
    let max_waiters = match compat.tip_wait_max_waiters {
        None => DEFAULT_TIP_WAIT_MAX_WAITERS,
        Some(configured) => NonZeroU32::new(configured).ok_or_else(|| {
            ConfigError::invalid("compat.tip_wait_max_waiters must be at least 1")
        })?,
    };
    Ok((tip_wait, max_blocks, max_waiters))
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

/// Resolves a configured storage path to the same lexical absolute identity
/// the process will use, without requiring a secondary directory to exist.
///
/// The reader lifecycle must reject `./`, `..`, and nested aliases before
/// `RocksDB` creates secondary metadata. Filesystem canonicalization alone is
/// insufficient because the next inactive generation is intentionally absent
/// on first startup.
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
                let _was_popped = normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}
