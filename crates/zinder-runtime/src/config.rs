//! Shared configuration loading helpers used by every Zinder service binary.
//!
//! Every binary follows the same `defaults -> file -> ZINDER_* environment ->
//! CLI overrides` precedence. This module owns that policy so the rules
//! cannot drift between binaries.
//!
//! Live tests reuse the production env-var schema directly; the
//! `ZINDER_TEST_LIVE` gate (and other `ZINDER_TEST_*` knobs like
//! `ZINDER_STORE_CRASH_*`) are stripped here so test-only acknowledgements
//! cannot leak into a production binary's config.
//!
//! Secret hygiene is handled at emit time rather than at load time. Secrets
//! pass through this loader unchanged; redaction happens in
//! [`NodeAuthToml::from_node_auth`] (used by `--print-config`) and in the
//! manual `Debug` impls on [`zinder_source::NodeAuth`] and
//! [`crate::auth::BearerToken`]. Per-surface file-only constraints (see
//! [ADR-0006](../../../docs/adrs/0006-ingest-control-transport-security.md))
//! remain enforced at their respective config types.

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use ::config::{
    Config, ConfigBuilder, ConfigError as InnerConfigError, Environment, File, FileFormat, Value,
    builder::DefaultState,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use zinder_core::Network;
use zinder_core::wire::{decode_zinder_native_chain_name, encode_zinder_native_chain_name};
use zinder_source::{NodeAuth, NodeConfigError, NodeHealthConfig, NodeTarget};

use crate::auth::{BearerToken, BearerTokenError};
use crate::env_diagnostics::{RejectedEnvVar, translate_env_error};
use crate::sections::{
    DEFAULT_ALLOW_PUBLIC_BIND, RuntimeService, defaults::default_ops_listen_addr,
};

const ENV_PREFIX: &str = "ZINDER_";
const TEST_ENV_PREFIXES: &[&str] = &["ZINDER_TEST_"];

/// Error returned while resolving Zinder service configuration from defaults,
/// file, environment, and CLI overrides.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// Loading or deserializing layered configuration failed.
    #[error("failed to load configuration: {source}")]
    Load {
        /// Underlying `config-rs` error.
        #[source]
        source: InnerConfigError,
    },

    /// Rendering effective configuration to TOML failed.
    #[error("failed to render configuration: {source}")]
    Render {
        /// Underlying TOML serialization error.
        #[source]
        source: toml::ser::Error,
    },

    /// A required configuration field is missing after all layers are merged.
    #[error("missing configuration field: {field}")]
    MissingField {
        /// Public configuration field path, such as `node.json_rpc_addr`.
        field: &'static str,
    },

    /// Configuration contains an invalid combination or value.
    #[error("invalid configuration: {reason}")]
    Invalid {
        /// Validation failure reason.
        reason: String,
    },

    /// A CLI-supplied path is not valid UTF-8 and cannot be carried through
    /// the TOML-shaped configuration layer.
    #[error("configuration path field {field} is not valid UTF-8")]
    NonUnicodePath {
        /// Public configuration field path.
        field: &'static str,
    },

    /// The operator set an environment variable the loader recognized as
    /// the source of a deserialization failure. Carries the original
    /// `ZINDER_…` name plus a suggested correction so the operator does
    /// not have to map a serde unknown-field message back to the env var
    /// themselves.
    #[error(
        "rejected environment variable `{original_name}`: produced configuration key \
         `{rejected_key}`, which is not a valid field. Zinder env vars use `__` (double \
         underscore) between TOML section names and single `_` inside field names. \
         {}See `docs/architecture/public-interfaces.md` for the canonical env-var table.",
        suggestion_sentence(suggested_name.as_deref())
    )]
    RejectedEnvVar {
        /// Original `ZINDER_…` name as set by the operator.
        original_name: String,
        /// Config-key path the env var produced.
        rejected_key: String,
        /// Suggested corrected env var name. `None` when the heuristic
        /// cannot confidently propose an alternative.
        suggested_name: Option<String>,
    },
}

fn suggestion_sentence(suggested_name: Option<&str>) -> String {
    suggested_name.map_or_else(String::new, |name| format!("Try `{name}` instead. "))
}

impl ConfigError {
    /// Builds a [`ConfigError::Load`] from a `config-rs` error.
    #[must_use]
    pub fn load(source: InnerConfigError) -> Self {
        Self::Load { source }
    }

    /// Builds a [`ConfigError::Invalid`] from a free-form reason.
    #[must_use]
    pub fn invalid(reason: impl Into<String>) -> Self {
        Self::Invalid {
            reason: reason.into(),
        }
    }

    /// Builds a [`ConfigError::MissingField`] for the given field path.
    #[must_use]
    pub const fn missing_field(field: &'static str) -> Self {
        Self::MissingField { field }
    }

    fn from_rejected_env_var(rejection: RejectedEnvVar) -> Self {
        Self::RejectedEnvVar {
            original_name: rejection.original_name,
            rejected_key: rejection.rejected_key,
            suggested_name: rejection.suggested_name,
        }
    }

    /// Translates an `InnerConfigError` to a [`ConfigError::RejectedEnvVar`]
    /// when the failure can be attributed to an env var, falling back to
    /// [`ConfigError::Load`] otherwise.
    fn from_inner_with_env_diagnostics(
        inner: InnerConfigError,
        reverse_index: Option<&HashMap<String, String>>,
    ) -> Self {
        if let Some(reverse_index) = reverse_index
            && let Some(rejection) = translate_env_error(&inner, reverse_index)
        {
            return Self::from_rejected_env_var(rejection);
        }
        Self::Load { source: inner }
    }
}

impl From<NodeConfigError> for ConfigError {
    fn from(error: NodeConfigError) -> Self {
        match error {
            NodeConfigError::MissingField { field } => Self::MissingField { field },
            NodeConfigError::Invalid { reason } => Self::invalid(reason),
            NodeConfigError::AuthFieldNotApplicable { field, method } => Self::invalid(format!(
                "{field} is not valid when node.auth.method is {method}"
            )),
            NodeConfigError::UnknownAuthMethod { method } => {
                Self::invalid(format!("unknown node.auth.method: {method}"))
            }
            NodeConfigError::EnvParseFailed { field, reason } => Self::invalid(format!(
                "failed to parse environment variable for {field}: {reason}"
            )),
            other => Self::invalid(other.to_string()),
        }
    }
}

/// Standard Zinder environment source for `config-rs`.
///
/// Paired with the reverse mapping from produced config-key paths back to
/// the original `ZINDER_…` names so the loader can attribute
/// deserialization failures to specific env vars.
#[derive(Debug)]
pub struct ZinderEnvironmentSource {
    /// `config-rs` source ready to be added to a [`ConfigBuilder`].
    pub source: Environment,
    /// Map from `config-rs`-style config-key path (e.g. `ops_listen_addr`,
    /// `ops.listen_addr`) to the `ZINDER_…` name that produced it. Used by
    /// the diagnostic loader to turn serde unknown-field errors back into
    /// operator-actionable messages.
    pub reverse_index: HashMap<String, String>,
}

/// Builds the standard Zinder environment source from the process
/// environment.
///
/// Reads `std::env::vars()`. Strips `ZINDER_TEST_*` prefixes so test-only
/// acknowledgements cannot leak into production config. Secret values
/// pass through unchanged; redaction happens at emit boundaries.
pub fn zinder_environment_source() -> Result<ZinderEnvironmentSource, ConfigError> {
    zinder_environment_source_from_map(std::env::vars())
}

/// Builds a Zinder environment source from an explicit iterator of
/// `(name, value)` pairs.
///
/// Use this in tests that want deterministic env input without mutating
/// the global process environment.
pub fn zinder_environment_source_from_map<I>(
    env_iter: I,
) -> Result<ZinderEnvironmentSource, ConfigError>
where
    I: IntoIterator<Item = (String, String)>,
{
    let mut filtered_env = HashMap::new();
    let mut reverse_index = HashMap::new();

    for (variable, env_value) in env_iter {
        if TEST_ENV_PREFIXES
            .iter()
            .any(|test_prefix| variable.starts_with(test_prefix))
        {
            continue;
        }

        let Some(env_key) = variable.strip_prefix(ENV_PREFIX) else {
            continue;
        };

        reverse_index.insert(env_key_to_config_key(env_key), variable.clone());
        filtered_env.insert(env_key.to_owned(), env_value);
    }

    let source = Environment::default()
        .separator("__")
        .try_parsing(true)
        .source(Some(filtered_env));

    Ok(ZinderEnvironmentSource {
        source,
        reverse_index,
    })
}

/// Mirrors how `config-rs` transforms an env-var key (post-`ZINDER_`
/// strip) into a config-key path: lowercase everything and treat `__` as
/// a nesting separator. Single `_` stays inside the key segment.
fn env_key_to_config_key(env_key: &str) -> String {
    env_key.to_lowercase().replace("__", ".")
}

/// Returns `field_value` or a [`ConfigError::MissingField`] error pointing at
/// `field`.
pub fn require_field<T>(field_value: Option<T>, field: &'static str) -> Result<T, ConfigError> {
    field_value.ok_or(ConfigError::MissingField { field })
}

/// Converts `path` to a UTF-8 string suitable for the TOML-shaped config
/// layer, returning [`ConfigError::NonUnicodePath`] if the path is not valid
/// UTF-8.
fn path_to_config_string(path: PathBuf, field: &'static str) -> Result<String, ConfigError> {
    path.into_os_string()
        .into_string()
        .map_err(|_| ConfigError::NonUnicodePath { field })
}

/// Converts a [`Duration`] to whole milliseconds as `u64`.
///
/// Saturates at [`u64::MAX`] for durations that exceed the representable
/// range. Used by every binary's `--print-config` renderer; the saturating
/// fallback keeps the cast lint-clean without panicking on absurd
/// configurations.
#[must_use]
pub fn duration_as_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Parses `text` as a [`SocketAddr`], producing a [`ConfigError::Invalid`]
/// that names the offending field and value on failure.
///
/// Centralizing the error shape keeps the operator-visible message
/// consistent across every section that takes a listen address.
pub fn parse_socket_addr(field: &str, text: &str) -> Result<SocketAddr, ConfigError> {
    text.parse::<SocketAddr>().map_err(|source| {
        ConfigError::invalid(format!("{field} {text} is not a socket address: {source}"))
    })
}

/// Loads a [`BearerToken`] from `path` when present.
///
/// Returns `Ok(None)` when no path is supplied, so callers can express the
/// "optional secret" shape without branching on the inner result.
pub fn load_bearer_token(path: Option<&Path>) -> Result<Option<BearerToken>, ConfigError> {
    path.map(BearerToken::from_file)
        .transpose()
        .map_err(|source: BearerTokenError| ConfigError::invalid(source.to_string()))
}

/// Raw `[network]` config section shared by every Zinder service binary.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkSection {
    /// Network name: `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`.
    pub name: Option<String>,
}

impl NetworkSection {
    /// Resolves the configured [`Network`] or returns
    /// [`ConfigError::MissingField`] / [`ConfigError::Invalid`].
    pub fn resolve(self) -> Result<Network, ConfigError> {
        let name = require_field(self.name, "network.name")?;
        decode_zinder_native_chain_name(&name).ok().ok_or_else(|| {
            ConfigError::invalid(format!(
                "unknown network: {name}; expected zcash-mainnet, zcash-testnet, or zcash-regtest"
            ))
        })
    }
}

/// Fluent layered configuration loader.
///
/// Encapsulates the canonical Zinder precedence: `defaults -> file ->
/// ZINDER_* environment -> CLI overrides`. Each binary builds the same shape
/// by chaining [`ConfigLoader::with_default`], [`ConfigLoader::with_file`],
/// [`ConfigLoader::with_zinder_env`], and [`ConfigLoader::with_override_if`]
/// before calling [`ConfigLoader::load`] to deserialize.
#[must_use]
pub struct ConfigLoader {
    builder: ConfigBuilder<DefaultState>,
    env_reverse_index: Option<HashMap<String, String>>,
}

impl ConfigLoader {
    /// Starts a new loader with no sources or defaults.
    pub fn new() -> Self {
        Self {
            builder: Config::builder(),
            env_reverse_index: None,
        }
    }

    /// Records a default for `key`. Defaults sit at the lowest precedence
    /// regardless of when they are recorded.
    pub fn with_default<V>(mut self, key: &str, default_for_key: V) -> Result<Self, ConfigError>
    where
        V: Into<Value>,
    {
        self.builder = self
            .builder
            .set_default(key, default_for_key)
            .map_err(ConfigError::load)?;
        Ok(self)
    }

    /// Adds an optional TOML file source. When `path` is `None` the loader
    /// is unchanged; when `Some`, the file is required to exist.
    pub fn with_file(mut self, path: Option<PathBuf>) -> Self {
        if let Some(path) = path {
            self.builder = self
                .builder
                .add_source(File::from(path).format(FileFormat::Toml).required(true));
        }
        self
    }

    /// Adds the standard Zinder environment source from the process
    /// environment.
    ///
    /// Strips `ZINDER_TEST_*` prefixes, refuses to start on any renamed env
    /// var, and stores the env-key reverse index used by [`Self::load`] to
    /// translate serde "unknown field" errors back to the env var that
    /// caused them.
    pub fn with_zinder_env(self) -> Result<Self, ConfigError> {
        let env_source = zinder_environment_source()?;
        Ok(self.attach_zinder_env_source(env_source))
    }

    /// Adds a Zinder environment source built from an explicit iterator of
    /// `(name, value)` pairs.
    ///
    /// Use in tests that want deterministic env input without mutating the
    /// global process environment. Behavior is otherwise identical to
    /// [`Self::with_zinder_env`].
    pub fn with_zinder_env_from_map<I>(self, env_iter: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let env_source = zinder_environment_source_from_map(env_iter)?;
        Ok(self.attach_zinder_env_source(env_source))
    }

    fn attach_zinder_env_source(mut self, env_source: ZinderEnvironmentSource) -> Self {
        self.builder = self.builder.add_source(env_source.source);
        self.env_reverse_index = Some(merge_reverse_index(
            self.env_reverse_index,
            env_source.reverse_index,
        ));
        self
    }

    /// Wires the shared `[ops]` section with the per-service default
    /// operational listen address.
    ///
    /// Every service binary calls this exactly once so the on-by-default
    /// policy applies uniformly: a binary started without any TOML or env
    /// override binds the loopback default for its service. Operators opt
    /// out by setting `ops.listen_addr = ""` (or
    /// `ZINDER_OPS__LISTEN_ADDR=` to the empty string).
    pub fn with_ops_section(self, service: RuntimeService) -> Result<Self, ConfigError> {
        self.with_default("ops.listen_addr", default_ops_listen_addr(service))
    }

    /// Wires the shared `[security]` section with the default public-bind
    /// posture.
    ///
    /// Every service binary calls this exactly once so the same default
    /// applies uniformly: a binary started without any override refuses to
    /// bind a plaintext serving surface to a public or unspecified address
    /// (see [`guard_serving_bind`](crate::guard_serving_bind)). Operators
    /// opt in by setting `security.allow_public_bind = true` (or
    /// `ZINDER_SECURITY__ALLOW_PUBLIC_BIND=true`).
    pub fn with_security_section(self) -> Result<Self, ConfigError> {
        self.with_default("security.allow_public_bind", DEFAULT_ALLOW_PUBLIC_BIND)
    }

    /// Records an override for `key` when `override_for_key` is `Some`.
    /// Overrides sit at the highest precedence regardless of when they are
    /// recorded.
    pub fn with_override_if<V>(
        mut self,
        key: &str,
        override_for_key: Option<V>,
    ) -> Result<Self, ConfigError>
    where
        V: Into<Value>,
    {
        if let Some(override_for_key) = override_for_key {
            self.builder = self
                .builder
                .set_override(key, override_for_key)
                .map_err(ConfigError::load)?;
        }
        Ok(self)
    }

    /// Records an override for `key` from a [`PathBuf`] when present,
    /// converting to UTF-8 for the TOML-shaped config layer.
    pub fn with_override_path_if(
        mut self,
        key: &'static str,
        override_path: Option<PathBuf>,
    ) -> Result<Self, ConfigError> {
        if let Some(path) = override_path {
            let path_string = path_to_config_string(path, key)?;
            self.builder = self
                .builder
                .set_override(key, path_string)
                .map_err(ConfigError::load)?;
        }
        Ok(self)
    }

    /// Builds the merged configuration and deserializes it as `T`.
    ///
    /// When deserialization fails and the failure can be attributed to an
    /// env var the operator set, returns [`ConfigError::RejectedEnvVar`]
    /// with the original `ZINDER_…` name and a suggested correction.
    /// Otherwise returns [`ConfigError::Load`] wrapping the underlying
    /// `config-rs` error.
    pub fn load<T>(self) -> Result<T, ConfigError>
    where
        T: DeserializeOwned,
    {
        let reverse_index = self.env_reverse_index;
        let built = self.builder.build().map_err(|inner| {
            ConfigError::from_inner_with_env_diagnostics(inner, reverse_index.as_ref())
        })?;
        built.try_deserialize::<T>().map_err(|inner| {
            ConfigError::from_inner_with_env_diagnostics(inner, reverse_index.as_ref())
        })
    }
}

fn merge_reverse_index(
    existing: Option<HashMap<String, String>>,
    incoming: HashMap<String, String>,
) -> HashMap<String, String> {
    match existing {
        Some(mut existing) => {
            existing.extend(incoming);
            existing
        }
        None => incoming,
    }
}

impl Default for ConfigLoader {
    fn default() -> Self {
        Self::new()
    }
}

/// Redacted TOML projection of `[network]` for `--print-config`.
#[derive(Debug, Serialize)]
pub struct NetworkToml {
    /// Network name in canonical form (e.g. `zcash-mainnet`).
    pub name: &'static str,
}

impl NetworkToml {
    /// Builds a [`NetworkToml`] from a resolved [`Network`].
    #[must_use]
    pub const fn from_network(network: Network) -> Self {
        Self {
            name: encode_zinder_native_chain_name(network),
        }
    }
}

/// Redacted TOML projection of `[node.auth]` for `--print-config`.
#[derive(Debug, Serialize)]
pub struct NodeAuthToml {
    /// Auth method: `none`, `basic`, or `cookie`.
    pub method: &'static str,
    /// Basic-auth username, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Basic-auth password placeholder. Always `[REDACTED]` when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<&'static str>,
    /// Cookie-auth path placeholder. Always `[REDACTED]` when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<&'static str>,
}

impl NodeAuthToml {
    /// Builds a redacted [`NodeAuthToml`] from a resolved [`NodeAuth`].
    ///
    /// Both cookie sources (file path and inline credentials) redact to
    /// `[REDACTED]` so an operator inspecting `--print-config` cannot tell
    /// the credential apart from the path. Distinguishing them is not
    /// useful for debugging and risks leaking metadata about how the secret
    /// was injected.
    #[must_use]
    pub fn from_node_auth(auth: &NodeAuth) -> Self {
        match auth {
            NodeAuth::None => Self {
                method: "none",
                username: None,
                password: None,
                path: None,
            },
            NodeAuth::Basic { username, .. } => Self {
                method: "basic",
                username: Some(username.clone()),
                password: Some("[REDACTED]"),
                path: None,
            },
            NodeAuth::Cookie(_) => Self {
                method: "cookie",
                username: None,
                password: None,
                path: Some("[REDACTED]"),
            },
        }
    }
}

/// TOML projection of `[node.health]` for `--print-config`.
#[derive(Debug, Serialize)]
pub struct NodeHealthToml {
    /// Full URL of the upstream node's readiness endpoint.
    pub addr: String,
    /// Upstream-health probe cadence in milliseconds.
    pub poll_interval_ms: u64,
    /// Verification-progress floor used by the JSON-RPC fallback.
    pub verification_progress_floor: f64,
    /// Estimated-height gap floor, in blocks, used by the JSON-RPC fallback.
    pub estimated_gap_floor_blocks: u32,
}

impl NodeHealthToml {
    /// Builds a [`NodeHealthToml`] from a resolved [`NodeHealthConfig`].
    #[must_use]
    pub fn from_node_health_config(health: &NodeHealthConfig) -> Self {
        Self {
            addr: health.addr.clone(),
            poll_interval_ms: duration_as_millis_u64(health.poll_interval),
            verification_progress_floor: health.verification_progress_floor,
            estimated_gap_floor_blocks: health.estimated_gap_floor_blocks,
        }
    }
}

/// Redacted TOML projection of `[node]` for `--print-config`.
#[derive(Debug, Serialize)]
pub struct NodeToml {
    /// Node JSON-RPC base URL.
    pub json_rpc_addr: String,
    /// Optional Zebra indexer gRPC endpoint URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexer_grpc_addr: Option<String>,
    /// Per-RPC request timeout in seconds.
    pub request_timeout_secs: u64,
    /// Maximum JSON-RPC response body size accepted from the node.
    pub max_response_bytes: u64,
    /// Per-broadcast timeout in seconds, applied only to `sendrawtransaction`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broadcast_timeout_secs: Option<u64>,
    /// Optional upstream-health probe subsection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<NodeHealthToml>,
    /// Auth subsection.
    pub auth: NodeAuthToml,
}

impl NodeToml {
    /// Builds a redacted [`NodeToml`] from a resolved [`NodeTarget`].
    #[must_use]
    pub fn from_node_target(target: &NodeTarget) -> Self {
        Self {
            json_rpc_addr: target.json_rpc_addr.clone(),
            indexer_grpc_addr: target.indexer_grpc_addr.clone(),
            request_timeout_secs: target.request_timeout.as_secs(),
            max_response_bytes: target.max_response_bytes.get(),
            broadcast_timeout_secs: target.broadcast_timeout.map(|d| d.as_secs()),
            health: target
                .health
                .as_ref()
                .map(NodeHealthToml::from_node_health_config),
            auth: NodeAuthToml::from_node_auth(&target.node_auth),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_field_returns_missing_field_for_none() {
        let outcome: Result<u32, _> =
            require_field(None, "ingest.construction.source_segment_max_blocks");
        assert!(matches!(
            outcome,
            Err(ConfigError::MissingField {
                field: "ingest.construction.source_segment_max_blocks"
            })
        ));
    }

    #[test]
    fn require_field_passes_through_value() -> Result<(), ConfigError> {
        let resolved: String = require_field(Some("hello".to_owned()), "x")?;
        assert_eq!(resolved, "hello");
        Ok(())
    }

    #[test]
    fn duration_as_millis_u64_saturates_on_overflow() {
        assert_eq!(duration_as_millis_u64(Duration::from_millis(1_500)), 1_500);
        assert_eq!(duration_as_millis_u64(Duration::MAX), u64::MAX);
    }

    #[test]
    fn configured_node_health_survives_node_toml_serialization() -> Result<(), toml::ser::Error> {
        let target = NodeTarget::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:18232".to_owned(),
            NodeAuth::None,
            Duration::from_secs(30),
            std::num::NonZeroU64::MIN,
        )
        .with_health(Some(NodeHealthConfig::new(
            "http://127.0.0.1:18233/ready".to_owned(),
            Duration::from_millis(1_250),
            0.875,
            42,
        )));

        let rendered = toml::to_string(&NodeToml::from_node_target(&target))?;

        assert_eq!(
            rendered,
            concat!(
                "json_rpc_addr = \"http://127.0.0.1:18232\"\n",
                "request_timeout_secs = 30\n",
                "max_response_bytes = 1\n",
                "\n",
                "[health]\n",
                "addr = \"http://127.0.0.1:18233/ready\"\n",
                "poll_interval_ms = 1250\n",
                "verification_progress_floor = 0.875\n",
                "estimated_gap_floor_blocks = 42\n",
                "\n",
                "[auth]\n",
                "method = \"none\"\n",
            )
        );
        Ok(())
    }

    #[test]
    fn absent_node_health_is_omitted_from_node_toml_serialization() -> Result<(), toml::ser::Error>
    {
        let target = NodeTarget::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:18232".to_owned(),
            NodeAuth::None,
            Duration::from_secs(30),
            std::num::NonZeroU64::MIN,
        );

        let rendered = toml::to_string(&NodeToml::from_node_target(&target))?;

        assert_eq!(
            rendered,
            concat!(
                "json_rpc_addr = \"http://127.0.0.1:18232\"\n",
                "request_timeout_secs = 30\n",
                "max_response_bytes = 1\n",
                "\n",
                "[auth]\n",
                "method = \"none\"\n",
            )
        );
        Ok(())
    }
}
