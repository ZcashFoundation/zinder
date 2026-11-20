//! Configuration loading for the `zinder-explorer` binary.

use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zinder_core::Network;
use zinder_runtime::{
    BearerToken, BearerTokenError, ClientAddressSource, ConfigError, ConfigLoader, NetworkSection,
    NetworkToml, OpsSection, OpsServerError, OpsToml, ResolvedSecondaryStorage, RuntimeService,
    SecondaryStorageSection, SecondaryStorageToml, SecuritySection, SecurityToml,
    guard_optional_serving_bind, guard_serving_bind, load_bearer_token, parse_socket_addr,
    require_field, resolve_allow_public_bind, resolve_ops_listen_addr, resolve_secondary_storage,
};
use zinder_source::{NodeFallbackSections, NodeSection, NodeTarget};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:9068";

/// Resolved explorer-plane runtime configuration.
#[derive(Clone, Debug)]
pub(crate) struct ExplorerConfig {
    pub(crate) network: Network,
    pub(crate) storage: ResolvedSecondaryStorage,
    pub(crate) listen_addr: SocketAddr,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
    pub(crate) allow_public_bind: bool,
    /// How the gRPC listener tells clients apart for its receive limit.
    pub(crate) client_address_source: ClientAddressSource,
    pub(crate) bearer_token_path: Option<PathBuf>,
    pub(crate) bearer_token: Option<BearerToken>,
    /// Wallet-query endpoint that backs the explorer's wallet-composed reads
    /// (transaction detail, block views, search, mempool activity). An empty
    /// string omits the explorer capabilities that compose wallet reads.
    pub(crate) wallet_query_endpoint: Option<String>,
    /// Resolved upstream node target. `None` when the operator did not
    /// configure `[node]`; the upstream-observation probe stays unspawned
    /// and every `ExplorerFreshness.chain_view.upstream_tip` field is unset.
    pub(crate) node: Option<NodeTarget>,
}

/// Command-line overrides applied on top of the layered configuration.
#[derive(Debug, Default)]
pub(crate) struct ExplorerConfigOverrides {
    pub(crate) network: Option<String>,
    pub(crate) storage_path: Option<PathBuf>,
    pub(crate) secondary_path: Option<PathBuf>,
    pub(crate) listen_addr: Option<SocketAddr>,
    pub(crate) ops_listen_addr: Option<SocketAddr>,
    pub(crate) bearer_token_path: Option<PathBuf>,
    pub(crate) wallet_query_endpoint: Option<String>,
}

/// Error returned while resolving explorer configuration or running the binary.
#[derive(Debug, Error)]
pub(crate) enum ExplorerConfigError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    OpsServer(#[from] OpsServerError),

    #[error(transparent)]
    Store(#[from] zinder_explorer::MaterializedViewStoreError),

    #[error("explorer runtime failed: {0}")]
    Runtime(#[from] zinder_explorer::MaterializedViewError),

    #[error(transparent)]
    Supervision(#[from] zinder_runtime::RuntimeSupervisionError),

    #[error("gRPC reflection initialization failed: {0}")]
    Reflection(#[from] tonic_reflection::server::Error),

    #[error("invalid explorer bearer token: {0}")]
    BearerToken(#[from] BearerTokenError),

    #[error("invalid [node] configuration: {0}")]
    Node(#[from] zinder_source::NodeConfigError),

    #[error("failed to build upstream node source: {0}")]
    NodeSource(#[from] zinder_source::SourceError),
}

/// Loads and validates explorer configuration from defaults, file, environment, and CLI overrides.
pub(crate) fn load_explorer_config(
    config_path: Option<PathBuf>,
    overrides: ExplorerConfigOverrides,
) -> Result<ExplorerConfig, ExplorerConfigError> {
    let raw: ExplorerRawConfig = ConfigLoader::new()
        // Storage defaults match the canonical Zinder layout. The explorer
        // locates the materialized-view primary beneath `storage.path` and
        // keeps its process-owned secondary metadata beneath
        // `storage.secondary_path`; it does not open the canonical primary.
        // Operators override via env vars (`ZINDER_STORAGE__PATH`,
        // `ZINDER_STORAGE__SECONDARY_PATH`) or CLI flags.
        .with_default("storage.path", "/var/lib/zinder/store")?
        .with_default(
            "storage.secondary_path",
            "/var/lib/zinder/explorer-secondary",
        )?
        .with_default("explorer.listen_addr", DEFAULT_LISTEN_ADDR)?
        .with_ops_section(RuntimeService::Explorer)?
        .with_security_section()?
        .with_file(config_path)
        .with_zinder_env()?
        .with_override_if("network.name", overrides.network)?
        .with_override_path_if("storage.path", overrides.storage_path)?
        .with_override_path_if("storage.secondary_path", overrides.secondary_path)?
        .with_override_if(
            "explorer.listen_addr",
            overrides.listen_addr.map(|addr| addr.to_string()),
        )?
        .with_override_path_if("explorer.bearer_token_path", overrides.bearer_token_path)?
        .with_override_if(
            "ops.listen_addr",
            overrides.ops_listen_addr.map(|addr| addr.to_string()),
        )?
        .with_override_if(
            "explorer.wallet_query_endpoint",
            overrides.wallet_query_endpoint,
        )?
        .load()?;
    resolve_explorer_config(raw)
}

/// Renders a resolved explorer configuration as TOML for `--print-config`.
pub(crate) fn explorer_config_toml(config: &ExplorerConfig) -> Result<String, ExplorerConfigError> {
    let toml_text = toml::to_string(&ExplorerConfigToml::from_resolved(config))
        .map_err(|source| ConfigError::Render { source })?;
    Ok(toml_text)
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ExplorerRawConfig {
    network: NetworkSection,
    storage: SecondaryStorageSection,
    explorer: ExplorerSection,
    ops: OpsSection,
    node: NodeSection,
    node_fallbacks: NodeFallbackSections,
    security: SecuritySection,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ExplorerSection {
    listen_addr: Option<String>,
    bearer_token_path: Option<PathBuf>,
    wallet_query_endpoint: Option<String>,
    trust_proxy_client_address: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ExplorerConfigToml {
    network: NetworkToml,
    ops: OpsToml,
    security: SecurityToml,
    storage: SecondaryStorageToml,
    explorer: ExplorerToml,
}

#[derive(Debug, Serialize)]
struct ExplorerToml {
    listen_addr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    bearer_token_path: Option<String>,
    wallet_query_endpoint: String,
    trust_proxy_client_address: bool,
}

impl ExplorerConfigToml {
    fn from_resolved(config: &ExplorerConfig) -> Self {
        Self {
            network: NetworkToml::from_network(config.network),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            security: SecurityToml::from_resolved(config.allow_public_bind),
            storage: SecondaryStorageToml::from_resolved(&config.storage),
            explorer: ExplorerToml {
                listen_addr: config.listen_addr.to_string(),
                bearer_token_path: config
                    .bearer_token_path
                    .as_ref()
                    .map(|path| path.display().to_string()),
                wallet_query_endpoint: config.wallet_query_endpoint.clone().unwrap_or_default(),
                trust_proxy_client_address: config
                    .client_address_source
                    .trust_proxy_client_address(),
            },
        }
    }
}

fn resolve_explorer_config(raw: ExplorerRawConfig) -> Result<ExplorerConfig, ExplorerConfigError> {
    let network = raw.network.resolve()?;
    let storage = resolve_secondary_storage(raw.storage)?;
    let listen_addr_text = require_field(raw.explorer.listen_addr, "explorer.listen_addr")?;
    let listen_addr = parse_socket_addr("explorer.listen_addr", &listen_addr_text)?;
    let bearer_token_path = raw.explorer.bearer_token_path;
    let bearer_token = load_bearer_token(bearer_token_path.as_deref())?;
    let ops_listen_addr = resolve_ops_listen_addr(raw.ops)?;
    let allow_public_bind = resolve_allow_public_bind(raw.security)?;
    guard_serving_bind("explorer.listen_addr", listen_addr, allow_public_bind)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;
    let wallet_query_endpoint = raw
        .explorer
        .wallet_query_endpoint
        .filter(|endpoint| !endpoint.is_empty());
    let node = NodeTarget::resolve_optional(network, raw.node)?;
    // The shared node list is validated as every runtime validates it, but the
    // upstream-observation probe reads only `[node]`.
    let node_fallbacks = NodeTarget::resolve_fallbacks(network, raw.node_fallbacks)?;
    NodeTarget::require_primary_for_fallbacks(node.as_ref(), &node_fallbacks)?;
    Ok(ExplorerConfig {
        network,
        storage,
        listen_addr,
        ops_listen_addr,
        allow_public_bind,
        client_address_source: ClientAddressSource::from_trust_proxy_client_address(
            raw.explorer.trust_proxy_client_address.unwrap_or(false),
        ),
        bearer_token_path,
        bearer_token,
        wallet_query_endpoint,
        node,
    })
}
