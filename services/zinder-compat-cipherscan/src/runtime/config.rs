//! Configuration loading for the `zinder-compat-cipherscan` runtime.

use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zinder_core::Network;
use zinder_runtime::{
    BearerToken, BearerTokenConnectError, BearerTokenError, ConfigError, ConfigLoader,
    InvalidZinderGrpcEndpoint, NetworkSection, NetworkToml, OpsSection, OpsServerError, OpsToml,
    RuntimeService, SecuritySection, SecurityToml, guard_optional_serving_bind, guard_serving_bind,
    parse_socket_addr, require_field, resolve_allow_public_bind, resolve_ops_listen_addr,
    resolve_optional_bearer_token, validate_zinder_grpc_endpoint,
};

use crate::CipherscanMarketPriceEndpoints;

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:9070";
const DEFAULT_EXPLORER_QUERY_ENDPOINT: &str = "http://127.0.0.1:9068";
const DEFAULT_WALLET_QUERY_ENDPOINT: &str = "http://127.0.0.1:9102";
const DEFAULT_CURRENT_PRICE_ENDPOINT: &str = "https://api.coingecko.com/api/v3/simple/price?ids=zcash&vs_currencies=usd&include_24hr_change=true";
const DEFAULT_HISTORICAL_PRICE_ENDPOINT_TEMPLATE: &str =
    "https://api.coingecko.com/api/v3/coins/zcash/history?localization=false&date={date}";

/// Resolved configuration for the Cipherscan REST compatibility adapter.
#[derive(Clone, Debug)]
pub struct CipherscanConfig {
    /// Target Zcash network.
    pub network: Network,
    /// Socket address for the Cipherscan HTTP/WebSocket listener.
    pub listen_addr: SocketAddr,
    /// Operational HTTP endpoint address, or `None` when disabled.
    pub ops_listen_addr: Option<SocketAddr>,
    /// Whether public or unspecified bind addresses are permitted.
    pub allow_public_bind: bool,
    /// `ExplorerQuery` gRPC endpoint URL.
    pub explorer_query_endpoint: String,
    /// `WalletQuery` gRPC endpoint URL.
    pub wallet_query_endpoint: String,
    /// Market-price HTTP endpoints for ZEC/USD lookups.
    pub market_price_endpoints: CipherscanMarketPriceEndpoints,
    /// Path to the shared bearer token file, when configured via file.
    pub bearer_token_path: Option<PathBuf>,
    /// Resolved bearer token presented to `ExplorerQuery` and `WalletQuery`.
    pub bearer_token: Option<BearerToken>,
}

/// Command-line overrides applied on top of layered Cipherscan configuration.
#[derive(Debug, Default)]
pub struct CipherscanConfigOverrides {
    /// Override for `network.name`.
    pub network: Option<String>,
    /// Override for `cipherscan.listen_addr`.
    pub listen_addr: Option<SocketAddr>,
    /// Override for `ops.listen_addr`.
    pub ops_listen_addr: Option<SocketAddr>,
    /// Override for `cipherscan.explorer_query_endpoint`.
    pub explorer_query_endpoint: Option<String>,
    /// Override for `cipherscan.wallet_query_endpoint`.
    pub wallet_query_endpoint: Option<String>,
    /// Override for `cipherscan.current_price_endpoint`.
    pub current_price_endpoint: Option<String>,
    /// Override for `cipherscan.historical_price_endpoint_template`.
    pub historical_price_endpoint_template: Option<String>,
    /// Override for `cipherscan.bearer_token_path`.
    pub bearer_token_path: Option<PathBuf>,
}

/// Error returned while loading configuration or running the Cipherscan adapter.
#[derive(Debug, Error)]
pub enum CipherscanConfigError {
    /// Layered configuration resolution failed.
    #[error(transparent)]
    Config(#[from] ConfigError),

    /// Operational HTTP endpoint initialization or shutdown failed.
    #[error(transparent)]
    OpsServer(#[from] OpsServerError),

    /// Shared-secret bearer token could not be loaded or parsed.
    #[error("invalid adapter bearer token: {0}")]
    BearerToken(#[from] BearerTokenError),

    /// Configured upstream Zinder gRPC endpoint URL is invalid.
    #[error(transparent)]
    InvalidZinderGrpcEndpoint(#[from] InvalidZinderGrpcEndpoint),

    /// Configured market-price endpoint URL is invalid.
    #[error("invalid current-price endpoint URL: {0}")]
    InvalidCurrentPriceEndpoint(String),

    /// Market-price HTTP client initialization failed.
    #[error("failed to build market-price HTTP client: {0}")]
    MarketPriceClient(#[from] crate::MarketPriceInitializationError),

    /// Upstream gRPC connection setup failed.
    #[error("failed to connect to Zinder gRPC endpoint: {0}")]
    ZinderGrpcConnect(#[from] BearerTokenConnectError),

    /// Binding the Cipherscan HTTP listener failed.
    #[error("failed to bind Cipherscan REST listen address {listen_addr}: {source}")]
    Bind {
        /// Listen socket address that failed to bind.
        listen_addr: SocketAddr,
        /// Underlying OS bind error.
        source: std::io::Error,
    },

    /// Serving the Cipherscan HTTP server failed.
    #[error("Cipherscan REST HTTP server failed: {0}")]
    Serve(std::io::Error),
}

/// Loads and validates Cipherscan configuration from defaults, TOML, env, and CLI overrides.
pub fn load_cipherscan_config(
    config_path: Option<PathBuf>,
    overrides: CipherscanConfigOverrides,
) -> Result<CipherscanConfig, CipherscanConfigError> {
    load_cipherscan_config_with_env(config_path, overrides, std::env::vars())
}

/// Loads and validates Cipherscan configuration using explicit `ZINDER_*`
/// environment pairs.
pub fn load_cipherscan_config_with_env<I>(
    config_path: Option<PathBuf>,
    overrides: CipherscanConfigOverrides,
    environment: I,
) -> Result<CipherscanConfig, CipherscanConfigError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let raw: CipherscanRawConfig = ConfigLoader::new()
        .with_default("cipherscan.listen_addr", DEFAULT_LISTEN_ADDR)?
        .with_default(
            "cipherscan.explorer_query_endpoint",
            DEFAULT_EXPLORER_QUERY_ENDPOINT,
        )?
        .with_default(
            "cipherscan.wallet_query_endpoint",
            DEFAULT_WALLET_QUERY_ENDPOINT,
        )?
        .with_default(
            "cipherscan.current_price_endpoint",
            DEFAULT_CURRENT_PRICE_ENDPOINT,
        )?
        .with_default(
            "cipherscan.historical_price_endpoint_template",
            DEFAULT_HISTORICAL_PRICE_ENDPOINT_TEMPLATE,
        )?
        .with_ops_section(RuntimeService::CompatCipherscan)?
        .with_security_section()?
        .with_file(config_path)
        .with_zinder_env_from_map(environment)?
        .with_override_if("network.name", overrides.network)?
        .with_override_if(
            "cipherscan.listen_addr",
            overrides.listen_addr.map(|addr| addr.to_string()),
        )?
        .with_override_if(
            "ops.listen_addr",
            overrides.ops_listen_addr.map(|addr| addr.to_string()),
        )?
        .with_override_if(
            "cipherscan.explorer_query_endpoint",
            overrides.explorer_query_endpoint,
        )?
        .with_override_if(
            "cipherscan.wallet_query_endpoint",
            overrides.wallet_query_endpoint,
        )?
        .with_override_if(
            "cipherscan.current_price_endpoint",
            overrides.current_price_endpoint,
        )?
        .with_override_if(
            "cipherscan.historical_price_endpoint_template",
            overrides.historical_price_endpoint_template,
        )?
        .with_override_path_if("cipherscan.bearer_token_path", overrides.bearer_token_path)?
        .load()?;
    resolve_cipherscan_config(raw)
}

/// Renders a resolved Cipherscan configuration as redacted TOML for `--print-config`.
pub fn cipherscan_config_toml(config: &CipherscanConfig) -> Result<String, CipherscanConfigError> {
    toml::to_string(&CipherscanConfigToml::from_resolved(config))
        .map_err(|source| ConfigError::Render { source }.into())
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CipherscanRawConfig {
    network: NetworkSection,
    cipherscan: CipherscanSection,
    ops: OpsSection,
    security: SecuritySection,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CipherscanSection {
    listen_addr: Option<String>,
    explorer_query_endpoint: Option<String>,
    wallet_query_endpoint: Option<String>,
    current_price_endpoint: Option<String>,
    historical_price_endpoint_template: Option<String>,
    bearer_token_path: Option<PathBuf>,
    bearer_token: Option<String>,
}

impl std::fmt::Debug for CipherscanSection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CipherscanSection")
            .field("listen_addr", &self.listen_addr)
            .field("explorer_query_endpoint", &self.explorer_query_endpoint)
            .field("wallet_query_endpoint", &self.wallet_query_endpoint)
            .field("current_price_endpoint", &self.current_price_endpoint)
            .field(
                "historical_price_endpoint_template",
                &self.historical_price_endpoint_template,
            )
            .field("bearer_token_path", &self.bearer_token_path)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[derive(Debug, Serialize)]
struct CipherscanConfigToml {
    network: NetworkToml,
    ops: OpsToml,
    security: SecurityToml,
    cipherscan: CipherscanToml,
}

#[derive(Debug, Serialize)]
struct CipherscanToml {
    listen_addr: String,
    explorer_query_endpoint: String,
    wallet_query_endpoint: String,
    current_price_endpoint: String,
    historical_price_endpoint_template: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    bearer_token_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bearer_token: Option<&'static str>,
}

impl CipherscanConfigToml {
    fn from_resolved(config: &CipherscanConfig) -> Self {
        Self {
            network: NetworkToml::from_network(config.network),
            ops: OpsToml::from_resolved(config.ops_listen_addr),
            security: SecurityToml::from_resolved(config.allow_public_bind),
            cipherscan: CipherscanToml {
                listen_addr: config.listen_addr.to_string(),
                explorer_query_endpoint: config.explorer_query_endpoint.clone(),
                wallet_query_endpoint: config.wallet_query_endpoint.clone(),
                current_price_endpoint: config.market_price_endpoints.current.to_string(),
                historical_price_endpoint_template: config
                    .market_price_endpoints
                    .historical_template
                    .clone(),
                bearer_token_path: config
                    .bearer_token_path
                    .as_ref()
                    .map(|path| path.display().to_string()),
                bearer_token: (config.bearer_token.is_some() && config.bearer_token_path.is_none())
                    .then_some("[REDACTED]"),
            },
        }
    }
}

fn resolve_cipherscan_config(
    raw: CipherscanRawConfig,
) -> Result<CipherscanConfig, CipherscanConfigError> {
    let network = raw.network.resolve()?;
    let listen_addr_text = require_field(raw.cipherscan.listen_addr, "cipherscan.listen_addr")?;
    let listen_addr = parse_socket_addr("cipherscan.listen_addr", &listen_addr_text)?;
    let ops_listen_addr = resolve_ops_listen_addr(raw.ops)?;
    let allow_public_bind = resolve_allow_public_bind(raw.security)?;
    guard_serving_bind("cipherscan.listen_addr", listen_addr, allow_public_bind)?;
    guard_optional_serving_bind("ops.listen_addr", ops_listen_addr, allow_public_bind)?;

    let explorer_query_endpoint = require_field(
        raw.cipherscan.explorer_query_endpoint,
        "cipherscan.explorer_query_endpoint",
    )?;
    validate_zinder_grpc_endpoint(&explorer_query_endpoint)?;
    let wallet_query_endpoint = require_field(
        raw.cipherscan.wallet_query_endpoint,
        "cipherscan.wallet_query_endpoint",
    )?;
    validate_zinder_grpc_endpoint(&wallet_query_endpoint)?;
    let current_price_endpoint_text = require_field(
        raw.cipherscan.current_price_endpoint,
        "cipherscan.current_price_endpoint",
    )?;
    let current_price_endpoint = reqwest::Url::parse(&current_price_endpoint_text)
        .map_err(|source| CipherscanConfigError::InvalidCurrentPriceEndpoint(source.to_string()))?;
    if !matches!(current_price_endpoint.scheme(), "http" | "https") {
        return Err(CipherscanConfigError::InvalidCurrentPriceEndpoint(
            "scheme must be http or https".to_owned(),
        ));
    }
    let historical_price_endpoint_template = require_field(
        raw.cipherscan.historical_price_endpoint_template,
        "cipherscan.historical_price_endpoint_template",
    )?;
    let market_price_endpoints = CipherscanMarketPriceEndpoints::new(
        current_price_endpoint,
        historical_price_endpoint_template,
    )?;
    let bearer_token_path = raw.cipherscan.bearer_token_path;
    let bearer_token = resolve_optional_bearer_token(
        bearer_token_path.as_deref(),
        raw.cipherscan.bearer_token.as_deref(),
        "cipherscan.bearer_token_path",
        "cipherscan.bearer_token",
    )?;

    Ok(CipherscanConfig {
        network,
        listen_addr,
        ops_listen_addr,
        allow_public_bind,
        explorer_query_endpoint,
        wallet_query_endpoint,
        market_price_endpoints,
        bearer_token_path,
        bearer_token,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CipherscanConfigError, CipherscanConfigOverrides, DEFAULT_WALLET_QUERY_ENDPOINT,
        cipherscan_config_toml, load_cipherscan_config,
    };

    #[test]
    fn wallet_query_default_uses_the_native_query_listener() {
        assert_eq!(DEFAULT_WALLET_QUERY_ENDPOINT, "http://127.0.0.1:9102");
    }

    #[test]
    fn inline_bearer_token_is_redacted_and_conflicts_with_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let scratch = tempfile::tempdir()?;
        let config_path = scratch.path().join("cipherscan.toml");
        std::fs::write(
            &config_path,
            "[network]\nname = \"zcash-regtest\"\n\n[cipherscan]\nbearer_token = \"cipherscan-inline-secret\"\n",
        )?;
        let resolved = load_cipherscan_config(
            Some(config_path.clone()),
            CipherscanConfigOverrides::default(),
        )?;
        let rendered = cipherscan_config_toml(&resolved)?;
        assert!(
            rendered.contains("bearer_token = \"[REDACTED]\""),
            "{rendered}"
        );
        assert!(!rendered.contains("cipherscan-inline-secret"), "{rendered}");

        let token_path = scratch.path().join("tok");
        std::fs::write(&token_path, "file-secret\n")?;
        let conflict = load_cipherscan_config(
            Some(config_path),
            CipherscanConfigOverrides {
                bearer_token_path: Some(token_path),
                ..CipherscanConfigOverrides::default()
            },
        );
        assert!(matches!(conflict, Err(CipherscanConfigError::Config(_))));
        Ok(())
    }
}
