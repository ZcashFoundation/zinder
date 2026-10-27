//! Configuration loading for the `zinder-query` binary.

use std::{
    net::SocketAddr,
    num::NonZeroU8,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
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
use zinder_store::{CanonicalReorgPolicy, CanonicalStoreBuildPlanError, RocksDbResourceBudget};

/// Resolved native wallet-query runtime configuration.
#[derive(Clone, Debug)]
pub(super) struct QueryConfig {
    pub(super) network: Network,
    pub(super) storage: ResolvedCanonicalSecondaryStorage,
    pub(super) wallet_primary_path: PathBuf,
    pub(super) wallet_secondary_root: PathBuf,
    pub(super) wallet_rocksdb_budget: RocksDbResourceBudget,
    pub(super) ingest_control_addr: String,
    pub(super) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(super) ingest_control_bearer_token: Option<BearerToken>,
    pub(super) listen_addr: SocketAddr,
    pub(super) ops_listen_addr: Option<SocketAddr>,
    pub(super) allow_public_bind: bool,
    pub(super) canonical_reorg_policy: CanonicalReorgPolicy,
    pub(super) pair_convergence_attempts: NonZeroU8,
    pub(super) node: NodeTarget,
}

/// Command-line overrides for the native wallet-query command.
#[derive(Debug, Default)]
pub(super) struct QueryConfigOverrides {
    pub(super) network: Option<String>,
    pub(super) canonical_primary_path: Option<PathBuf>,
    pub(super) canonical_secondary_root: Option<PathBuf>,
    pub(super) raw_blob_policy: Option<String>,
    pub(super) wallet_primary_path: Option<PathBuf>,
    pub(super) wallet_secondary_root: Option<PathBuf>,
    pub(super) ingest_control_addr: Option<String>,
    pub(super) ingest_control_bearer_token_path: Option<PathBuf>,
    pub(super) listen_addr: Option<SocketAddr>,
    pub(super) ops_listen_addr: Option<SocketAddr>,
    pub(super) node_json_rpc_addr: Option<String>,
    pub(super) reorg_window_blocks: Option<u32>,
}

/// Error returned while resolving config or running the native query server.
#[derive(Debug, Error)]
pub(super) enum QueryConfigError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    CanonicalStoreBuildPlan(#[from] CanonicalStoreBuildPlanError),
    #[error(transparent)]
    WalletServingPair(#[from] zinder_query::WalletServingPairError),
    #[error(transparent)]
    IngestControlAdmission(#[from] zinder_query::IngestControlAdmissionError),
    #[error(transparent)]
    WalletQuery(#[from] zinder_query::QueryError),
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
    #[error("wallet query {task} stopped before runtime shutdown")]
    RuntimeTaskStopped { task: &'static str },
    #[error("wallet query {task} task failed: {source}")]
    RuntimeTaskJoin {
        task: &'static str,
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("wallet query gRPC server stopped before runtime shutdown")]
    GrpcServerStopped,
}

pub(super) fn load_query_config(
    config_path: Option<PathBuf>,
    overrides: QueryConfigOverrides,
) -> Result<QueryConfig, QueryConfigError> {
    let raw_config: QueryRawConfig = ConfigLoader::new()
        .with_default("query.listen_addr", "127.0.0.1:9102")?
        .with_default(
            "query.reorg_window_blocks",
            zinder_store::DEFAULT_REORG_WINDOW_BLOCKS,
        )?
        .with_ops_section(RuntimeService::Query)?
        .with_security_section()?
        .with_file(config_path)
        .with_zinder_env()?
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

pub(super) fn query_config_toml(config: &QueryConfig) -> Result<String, QueryConfigError> {
    toml::to_string(&QueryConfigToml::from_query_config(config))
        .map_err(|source| ConfigError::Render { source }.into())
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct QueryRawConfig {
    network: NetworkSection,
    ops: OpsSection,
    storage: CanonicalSecondaryStorageSection,
    wallet: WalletSection,
    ingest_control: IngestControlSection,
    query: QuerySection,
    node: NodeSection,
    security: SecuritySection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct QuerySection {
    listen_addr: Option<String>,
    reorg_window_blocks: Option<u32>,
    pair_convergence_attempts: Option<u8>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WalletSection {
    path: Option<PathBuf>,
    secondary_path: Option<PathBuf>,
    rocksdb: RocksDbResourceBudgetSection,
}

fn resolve_query_config(config: QueryRawConfig) -> Result<QueryConfig, QueryConfigError> {
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
    let pair_convergence_attempts =
        require_pair_convergence_attempts(config.query.pair_convergence_attempts)?;
    let node = NodeTarget::resolve(network, config.node).map_err(ConfigError::from)?;

    Ok(QueryConfig {
        network,
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
        node,
    })
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
    network: NetworkToml,
    ops: OpsToml,
    security: SecurityToml,
    storage: CanonicalSecondaryStorageToml,
    wallet: WalletToml,
    ingest_control: IngestControlReaderToml,
    query: QueryToml,
    node: NodeToml,
}

impl QueryConfigToml {
    fn from_query_config(config: &QueryConfig) -> Self {
        Self {
            network: NetworkToml::from_network(config.network),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            security: SecurityToml::from_resolved(config.allow_public_bind),
            storage: CanonicalSecondaryStorageToml::from_resolved(&config.storage),
            wallet: WalletToml {
                path: config.wallet_primary_path.clone(),
                secondary_path: config.wallet_secondary_root.clone(),
                rocksdb: RocksDbResourceBudgetToml::from_resolved(config.wallet_rocksdb_budget),
            },
            ingest_control: IngestControlReaderToml::from_resolved(
                config.ingest_control_addr.clone(),
                config.ingest_control_bearer_token_path.as_deref(),
            ),
            query: QueryToml {
                listen_addr: config.listen_addr.to_string(),
                reorg_window_blocks: config.canonical_reorg_policy.reorg_window_blocks(),
                pair_convergence_attempts: config.pair_convergence_attempts.get(),
            },
            node: NodeToml::from_node_target(&config.node),
        }
    }
}

#[derive(Serialize)]
struct QueryToml {
    listen_addr: String,
    reorg_window_blocks: u32,
    pair_convergence_attempts: u8,
}

#[derive(Serialize)]
struct WalletToml {
    path: PathBuf,
    secondary_path: PathBuf,
    rocksdb: RocksDbResourceBudgetToml,
}
