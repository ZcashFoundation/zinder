//! Configuration loading for the `zinder-compat-lightwalletd` binary.

use std::{
    net::SocketAddr,
    num::NonZeroU8,
    path::{Component, Path, PathBuf},
};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zinder_compat_lightwalletd::LightwalletdAdmissionError;
use zinder_core::Network;
use zinder_runtime::{
    BearerToken, BearerTokenError, CanonicalSecondaryStorageSection, CanonicalSecondaryStorageToml,
    ConfigError, ConfigLoader, IngestControlReaderToml, IngestControlSection, NetworkSection,
    NetworkToml, NodeToml, OpsSection, OpsToml, ResolvedCanonicalSecondaryStorage,
    ResolvedIngestControlReader, RocksDbResourceBudgetSection, RocksDbResourceBudgetToml,
    RuntimeService, SecuritySection, SecurityToml, guard_optional_serving_bind, guard_serving_bind,
    parse_socket_addr, require_field, resolve_allow_public_bind,
    resolve_canonical_secondary_storage, resolve_ingest_control_reader, resolve_ops_listen_addr,
    resolve_wallet_projection_reader_rocksdb_budget,
};
use zinder_source::{NodeSection, NodeTarget};
use zinder_store::{
    CanonicalReorgPolicy, CanonicalStoreBuildPlanError, RocksDbResourceBudget, StoreError,
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
}

impl CompatServing {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Wallet => "wallet",
            Self::CompactBlocks => "compact-blocks",
        }
    }
}

/// Resolved lightwalletd compatibility runtime configuration.
#[derive(Clone, Debug)]
pub(crate) struct LightwalletdConfig {
    pub(crate) network: Network,
    pub(crate) serving: CompatServing,
    pub(crate) storage: ResolvedCanonicalSecondaryStorage,
    pub(crate) wallet_primary_path: Option<PathBuf>,
    pub(crate) wallet_secondary_root: Option<PathBuf>,
    pub(crate) wallet_rocksdb_budget: Option<RocksDbResourceBudget>,
    pub(crate) ingest_control_addr: String,
    pub(crate) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(crate) ingest_control_bearer_token: Option<BearerToken>,
    pub(crate) listen_addr: SocketAddr,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
    pub(crate) allow_public_bind: bool,
    pub(crate) canonical_reorg_policy: CanonicalReorgPolicy,
    pub(crate) pair_convergence_attempts: NonZeroU8,
    pub(crate) broadcaster: Option<NodeTarget>,
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
}

/// Error returned while resolving lightwalletd compat config or running its gRPC server.
#[derive(Debug, Error)]
pub(crate) enum LightwalletdConfigError {
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
    Query(#[from] zinder_query::QueryError),

    #[error(transparent)]
    CompatibilityAdmission(#[from] LightwalletdAdmissionError),

    #[error(transparent)]
    CompactServing(#[from] crate::compact_block_serving::CompactBlockServingError),

    #[error("node source initialization failed: {0}")]
    Source(Box<zinder_source::SourceError>),

    #[error("gRPC transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("invalid ingest-control bearer token: {0}")]
    BearerToken(#[from] BearerTokenError),

    #[error("gRPC reflection service build failed: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),

    #[error(transparent)]
    Operations(#[from] zinder_runtime::OpsServerError),
}

/// Loads and validates lightwalletd compat configuration.
pub(crate) fn load_lightwalletd_config(
    config_path: Option<PathBuf>,
    overrides: LightwalletdConfigOverrides,
) -> Result<LightwalletdConfig, LightwalletdConfigError> {
    let raw_config: LightwalletdRawConfig = ConfigLoader::new()
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
        .load()?;

    resolve_lightwalletd_config(raw_config)
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
    network: NetworkSection,
    ops: OpsSection,
    storage: CanonicalSecondaryStorageSection,
    wallet: Option<WalletSection>,
    ingest_control: IngestControlSection,
    compat: CompatSection,
    node: NodeSection,
    security: SecuritySection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CompatSection {
    listen_addr: Option<String>,
    reorg_window_blocks: Option<u32>,
    pair_convergence_attempts: Option<u8>,
    serving: Option<CompatServing>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WalletSection {
    path: Option<PathBuf>,
    secondary_path: Option<PathBuf>,
    rocksdb: RocksDbResourceBudgetSection,
}

fn resolve_lightwalletd_config(
    config: LightwalletdRawConfig,
) -> Result<LightwalletdConfig, LightwalletdConfigError> {
    let serving = config.compat.serving.unwrap_or_default();
    let wallet = config.wallet;
    if matches!(serving, CompatServing::CompactBlocks) && wallet.is_some() {
        return Err(ConfigError::invalid(
            "compat.serving = \"compact-blocks\" does not accept wallet.path, wallet.secondary_path, or wallet.rocksdb settings; remove the wallet section or select compat.serving = \"wallet\"",
        )
        .into());
    }
    let wallet = wallet.unwrap_or_default();
    let network = config.network.resolve()?;
    let storage = resolve_canonical_secondary_storage(config.storage)?;
    let (wallet_primary_path, wallet_secondary_root, wallet_rocksdb_budget) =
        if matches!(serving, CompatServing::Wallet) {
            let wallet_primary_path = require_field(wallet.path, "wallet.path")?;
            let wallet_secondary_root =
                require_field(wallet.secondary_path, "wallet.secondary_path")?;
            let wallet_rocksdb_budget =
                resolve_wallet_projection_reader_rocksdb_budget(wallet.rocksdb)?;
            require_distinct_storage_paths(
                &storage.path,
                &storage.secondary_path,
                &wallet_primary_path,
                &wallet_secondary_root,
            )?;
            (
                Some(wallet_primary_path),
                Some(wallet_secondary_root),
                Some(wallet_rocksdb_budget),
            )
        } else {
            (None, None, None)
        };
    let ResolvedIngestControlReader {
        addr: ingest_control_addr,
        bearer_token_path: ingest_control_bearer_token_path,
        bearer_token: ingest_control_bearer_token,
    } = resolve_ingest_control_reader(config.ingest_control)?;
    let listen_addr_string = require_field(config.compat.listen_addr, "compat.listen_addr")?;
    let listen_addr = parse_socket_addr("compat.listen_addr", &listen_addr_string)?;
    let ops_listen_addr = resolve_ops_listen_addr(config.ops)?;
    let allow_public_bind = resolve_allow_public_bind(config.security)?;
    let canonical_reorg_policy = CanonicalReorgPolicy::new(require_field(
        config.compat.reorg_window_blocks,
        "compat.reorg_window_blocks",
    )?)?;
    let pair_convergence_attempts =
        require_pair_convergence_attempts(config.compat.pair_convergence_attempts)?;
    guard_serving_bind("compat.listen_addr", listen_addr, allow_public_bind)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;
    let broadcaster =
        NodeTarget::resolve_optional(network, config.node).map_err(ConfigError::from)?;

    Ok(LightwalletdConfig {
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
        ops_listen_addr,
        allow_public_bind,
        canonical_reorg_policy,
        pair_convergence_attempts,
        broadcaster,
    })
}

#[derive(Serialize)]
struct LightwalletdConfigToml {
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
}

impl LightwalletdConfigToml {
    fn from_lightwalletd_config(config: &LightwalletdConfig) -> Self {
        Self {
            network: NetworkToml::from_network(config.network),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            security: SecurityToml::from_resolved(config.allow_public_bind),
            storage: CanonicalSecondaryStorageToml::from_resolved(&config.storage),
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
                listen_addr: config.listen_addr.to_string(),
                reorg_window_blocks: config.canonical_reorg_policy.reorg_window_blocks(),
                pair_convergence_attempts: config.pair_convergence_attempts.get(),
                serving: config.serving,
            },
            node: config.broadcaster.as_ref().map(NodeToml::from_node_target),
        }
    }
}

#[derive(Serialize)]
struct CompatToml {
    listen_addr: String,
    reorg_window_blocks: u32,
    pair_convergence_attempts: u8,
    serving: CompatServing,
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
