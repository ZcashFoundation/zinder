//! Shared configuration loading helpers used by every Zinder service binary.
//!
//! Every binary follows the same `defaults -> file -> ZINDER_* environment ->
//! CLI overrides` precedence. This module owns that policy so the rules
//! cannot drift between binaries.
//!
//! Live tests reuse the production env-var schema directly; the
//! `ZINDER_TEST_LIVE` gate (and other `ZINDER_TEST_*` knobs like
//! `ZINDER_TEST_CRASH_POINT`) are stripped here so test-only acknowledgements
//! cannot leak into a production binary's config.
//!
//! Secret hygiene is handled by redacting wrapper types
//! ([`SecretInput`], [`RedactedUrl`], [`RedactedCidrs`], [`zinder_source::SecretNodeInput`])
//! and by [`NodeSection::from_resolved`](zinder_source::NodeSection::from_resolved) /
//! [`NodeFallbackSections::from_resolved`](zinder_source::NodeFallbackSections::from_resolved),
//! together with the manual `Debug` impls on [`zinder_source::NodeAuth`],
//! [`zinder_source::IndexerTlsMaterial`], [`crate::auth::BearerToken`], and
//! [`ObservabilityConfig`](crate::ObservabilityConfig).

mod deserializer;
mod primitives;
mod schema;
mod tree;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use zinder_core::{
    Network,
    wire::{decode_zinder_native_chain_name, encode_zinder_native_chain_name},
};
use zinder_source::NodeConfigError;

use self::tree::{ConfigTree, PrimitiveValue};
pub use self::{
    primitives::{
        DISABLED_SENTINEL, REDACTED_PLACEHOLDER, RedactedCidrs, RedactedUrl, SecretInput, Toggle,
    },
    schema::{ZinderConfig, discover_schema_leaf_paths},
    tree::ConfigValue,
};
use crate::auth::{BearerToken, BearerTokenError};
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
    /// Reading a TOML configuration file failed.
    #[error("failed to read configuration file {}: {source}", path.display())]
    ReadFile {
        /// File path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Parsing a TOML configuration file failed.
    #[error("failed to parse TOML configuration file {}: {}", path.display(), source.message())]
    ParseToml {
        /// File path whose contents were not valid TOML.
        path: PathBuf,
        /// Underlying TOML parse error.
        #[source]
        source: Box<toml::de::Error>,
    },

    /// An environment variable failed typed deserialization or validation.
    #[error(
        "invalid value for environment variable `{original_name}` (configuration key `{key}`): {reason}"
    )]
    InvalidEnvValue {
        /// Original `ZINDER_…` variable name set by the operator.
        original_name: String,
        /// Target configuration-key path, such as `node.request_timeout_secs`.
        key: String,
        /// Parse or validation failure reason without echoing raw env values.
        reason: String,
    },

    /// Deserializing merged configuration failed for a non-environment source.
    #[error("{}", format_deserialize_error(path.as_deref(), reason))]
    Deserialize {
        /// Offending configuration path when known.
        path: Option<String>,
        /// Deserialization failure reason.
        reason: String,
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
         {}See `docs/reference/configuration.md` for the canonical env-var table.{}",
        suggestion_sentence(suggested_name.as_deref()),
        detail_suffix(detail.as_deref())
    )]
    RejectedEnvVar {
        /// Original `ZINDER_…` name as set by the operator.
        original_name: String,
        /// Config-key path the env var produced.
        rejected_key: String,
        /// Suggested corrected env var name. `None` when the heuristic
        /// cannot confidently propose an alternative.
        suggested_name: Option<String>,
        /// Underlying serde unknown-field detail when rejected during schema decoding.
        detail: Option<Box<str>>,
    },
}

fn format_deserialize_error(path: Option<&str>, reason: &str) -> String {
    if reason.starts_with("unknown field ") {
        return reason.to_owned();
    }
    match path {
        Some(path) if !reason.contains(path) => format!("{path}: {reason}"),
        _ => reason.to_owned(),
    }
}

fn suggestion_sentence(suggested_name: Option<&str>) -> String {
    suggested_name.map_or_else(String::new, |name| format!("Try `{name}` instead. "))
}

fn detail_suffix(detail: Option<&str>) -> String {
    detail.map_or_else(String::new, |text| format!("\n{text}"))
}

impl ConfigError {
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
            NodeConfigError::IndexerTlsWithoutIndexerAddr
            | NodeConfigError::IndexerTlsRequiresHttps
            | NodeConfigError::IndexerTlsClientIdentityIncomplete
            | NodeConfigError::IndexerTlsPathAndPemConflict { .. }
            | NodeConfigError::Fallback { .. }
            | NodeConfigError::FallbackPositionMissing { .. }
            | NodeConfigError::FallbacksWithoutPrimary => Self::invalid(error.to_string()),
            other => Self::invalid(other.to_string()),
        }
    }
}

pub(crate) fn collect_zinder_env_entries<I>(env_iter: I) -> Vec<(String, String)>
where
    I: IntoIterator<Item = (String, String)>,
{
    env_iter
        .into_iter()
        .filter_map(|(variable, env_raw)| {
            if TEST_ENV_PREFIXES
                .iter()
                .any(|test_prefix| variable.starts_with(test_prefix))
            {
                return None;
            }
            let env_key = variable.strip_prefix(ENV_PREFIX)?;
            Some((env_key.to_owned(), env_raw))
        })
        .collect()
}

pub(crate) fn top_level_segment(config_path: &str) -> &str {
    config_path
        .split_once('.')
        .map_or(config_path, |(section, _)| section)
}

fn is_positive_position_segment(segment: &str) -> bool {
    !segment.is_empty()
        && !segment.starts_with('0')
        && segment.bytes().all(|byte| byte.is_ascii_digit())
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right_chars: Vec<char> = right.chars().collect();
    let mut previous_row: Vec<usize> = (0..=right_chars.len()).collect();
    let mut current_row = vec![0_usize; right_chars.len() + 1];
    for (left_idx, left_char) in left.chars().enumerate() {
        current_row[0] = left_idx + 1;
        for (right_idx, right_char) in right_chars.iter().enumerate() {
            let cost = usize::from(left_char != *right_char);
            current_row[right_idx + 1] = (current_row[right_idx] + 1)
                .min(previous_row[right_idx + 1] + 1)
                .min(previous_row[right_idx] + cost);
        }
        std::mem::swap(&mut previous_row, &mut current_row);
    }
    previous_row[right_chars.len()]
}

fn suggest_double_underscore_form(original_name: &str) -> Option<String> {
    let suffix = original_name.strip_prefix(ENV_PREFIX)?;
    let first_underscore = suffix.find('_')?;
    let (head, tail) = suffix.split_at(first_underscore);
    let rest = tail.strip_prefix('_')?;
    if head.is_empty() || rest.is_empty() {
        return None;
    }
    Some(format!("{ENV_PREFIX}{head}__{rest}"))
}

fn normalize_position_pattern(rejected_key: &str) -> (String, Option<String>) {
    let mut captured_position = None;
    let normalized = rejected_key
        .split('.')
        .map(|segment| {
            if is_positive_position_segment(segment) && captured_position.is_none() {
                captured_position = Some(segment.to_owned());
                "<N>"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join(".");
    (normalized, captured_position)
}

fn suggest_known_env_var(rejected_key: &str) -> Option<String> {
    let (normalized_key, position) = normalize_position_pattern(rejected_key);
    let section = top_level_segment(&normalized_key);
    let same_section = find_closest_doc_name(&normalized_key, Some(section));
    let matched = same_section.or_else(|| find_closest_doc_name(&normalized_key, None))?;
    Some(position.map_or_else(|| matched.to_owned(), |pos| matched.replace("<N>", &pos)))
}

fn find_closest_doc_name(
    normalized_key: &str,
    section_filter: Option<&str>,
) -> Option<&'static str> {
    crate::env_var_docs::ENVIRONMENT_VARIABLES
        .iter()
        .filter(|doc| {
            section_filter.is_none_or(|section| top_level_segment(doc.toml_path) == section)
        })
        .filter_map(|doc| {
            let distance = edit_distance(normalized_key, doc.toml_path);
            (distance <= 3).then_some((distance, doc.name))
        })
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, name)| name)
}

pub(crate) fn suggest_env_var(original_name: &str, rejected_key: &str) -> Option<String> {
    if !rejected_key.contains('.') {
        return suggest_double_underscore_form(original_name)
            .or_else(|| suggest_known_env_var(rejected_key));
    }
    suggest_known_env_var(rejected_key)
}

/// Returns `field_value` or a [`ConfigError::MissingField`] error pointing at
/// `field`.
pub fn require_field<T>(field_value: Option<T>, field: &'static str) -> Result<T, ConfigError> {
    field_value.ok_or(ConfigError::MissingField { field })
}

fn path_to_config_string(path: PathBuf, field: &'static str) -> Result<String, ConfigError> {
    path.into_os_string()
        .into_string()
        .map_err(|_| ConfigError::NonUnicodePath { field })
}

/// Rejects empty/whitespace paths and the literal `[REDACTED]` placeholder.
pub fn validate_optional_path(
    path: Option<PathBuf>,
    field: &'static str,
) -> Result<Option<PathBuf>, ConfigError> {
    match path {
        Some(candidate) => {
            let lossy = candidate.as_os_str().to_string_lossy();
            let trimmed = lossy.trim();
            if trimmed.is_empty() {
                return Err(ConfigError::invalid(format!("{field} must not be empty")));
            }
            if trimmed == REDACTED_PLACEHOLDER {
                return Err(ConfigError::invalid(format!(
                    "{field} must not be `{REDACTED_PLACEHOLDER}`"
                )));
            }
            Ok(Some(candidate))
        }
        None => Ok(None),
    }
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
    resolve_optional_bearer_token(path, None, "bearer_token_path", "bearer_token")
}

/// Resolves an optional [`BearerToken`] from either a file path (`*_path`) or
/// an inline secret (`*`), rejecting simultaneous use of both.
pub fn resolve_optional_bearer_token(
    path: Option<&Path>,
    inline: Option<&str>,
    path_field: &str,
    inline_field: &str,
) -> Result<Option<BearerToken>, ConfigError> {
    match (path, inline) {
        (Some(_), Some(_)) => Err(ConfigError::invalid(format!(
            "{path_field} and {inline_field} are mutually exclusive"
        ))),
        (Some(file_path), None) => BearerToken::from_file(file_path)
            .map(Some)
            .map_err(|source: BearerTokenError| ConfigError::invalid(source.to_string())),
        (None, Some(secret_text)) => secret_text
            .parse::<BearerToken>()
            .map(Some)
            .map_err(|source: BearerTokenError| ConfigError::invalid(source.to_string())),
        (None, None) => Ok(None),
    }
}

/// Raw and serializable `[network]` config section shared by every Zinder
/// service binary.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkSection {
    /// Network name: `zcash-mainnet`, `zcash-testnet`, or `zcash-regtest`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl NetworkSection {
    /// Reports whether `network.name` is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.name.is_none()
    }

    /// Builds a `[network]` section from a resolved [`Network`].
    #[must_use]
    pub fn from_network(network: Network) -> Self {
        Self {
            name: Some(encode_zinder_native_chain_name(network).to_owned()),
        }
    }

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

enum LoaderSource {
    File(PathBuf),
    Env(Vec<(String, String)>),
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
    defaults: Vec<(String, PrimitiveValue)>,
    sources: Vec<LoaderSource>,
    overrides: Vec<(String, PrimitiveValue)>,
}

impl ConfigLoader {
    /// Starts a new loader with no sources or defaults.
    pub fn new() -> Self {
        Self {
            defaults: Vec::new(),
            sources: Vec::new(),
            overrides: Vec::new(),
        }
    }

    /// Records a default for `key`. Defaults sit at the lowest precedence
    /// regardless of when they are recorded.
    pub fn with_default<V>(mut self, key: &str, default_for_key: V) -> Result<Self, ConfigError>
    where
        V: Into<ConfigValue>,
    {
        self.defaults
            .push((key.to_owned(), default_for_key.into().inner));
        Ok(self)
    }

    /// Adds an optional TOML file source. When `path` is `None` the loader
    /// is unchanged; when `Some`, the file is required to exist.
    pub fn with_file(mut self, path: Option<PathBuf>) -> Self {
        if let Some(path) = path {
            self.sources.push(LoaderSource::File(path));
        }
        self
    }

    /// Adds the standard Zinder environment source from the process
    /// environment.
    ///
    /// Strips `ZINDER_TEST_*` prefixes and filters known sibling-component
    /// sections when scoped to a [`RuntimeService`], while preserving exact
    /// env-var provenance for diagnostics.
    pub fn with_zinder_env(self) -> Result<Self, ConfigError> {
        self.with_zinder_env_from_map(std::env::vars())
    }

    /// Adds a Zinder environment source built from an explicit iterator of
    /// `(name, value)` pairs.
    ///
    /// Use in tests that want deterministic env input without mutating the
    /// global process environment. Behavior is otherwise identical to
    /// [`Self::with_zinder_env`].
    pub fn with_zinder_env_from_map<I>(mut self, env_iter: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        self.sources
            .push(LoaderSource::Env(env_iter.into_iter().collect()));
        Ok(self)
    }

    /// Wires the shared `[ops]` section with the per-service default
    /// operational listen address.
    ///
    /// Every service binary calls this exactly once so the on-by-default
    /// policy applies uniformly: a binary started without any TOML or env
    /// override binds the loopback default for its service. Operators opt
    /// out by setting `ops.listen_addr = "none"` (or
    /// `ZINDER_OPS__LISTEN_ADDR=none`).
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
        V: Into<ConfigValue>,
    {
        if let Some(override_for_key) = override_for_key {
            self.overrides
                .push((key.to_owned(), override_for_key.into().inner));
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
            self.overrides
                .push((key.to_owned(), PrimitiveValue::String(path_string)));
        }
        Ok(self)
    }

    /// Builds the merged configuration and deserializes it as `T`.
    ///
    /// When deserialization fails and the failure can be attributed to an
    /// env var the operator set, returns [`ConfigError::RejectedEnvVar`] or
    /// [`ConfigError::InvalidEnvValue`] with the original `ZINDER_…` name.
    pub fn load<T>(self) -> Result<T, ConfigError>
    where
        T: DeserializeOwned,
    {
        let tree = self.merge_sources()?;
        tree.deserialize()
    }

    pub(crate) fn merge_sources(self) -> Result<ConfigTree, ConfigError> {
        let mut tree = ConfigTree::new();
        for (key, default_primitive) in self.defaults {
            tree.insert_primitive_path(&key, default_primitive, false)?;
        }
        for source in self.sources {
            match source {
                LoaderSource::File(path) => tree.merge_toml_file(&path)?,
                LoaderSource::Env(entries) => tree.merge_env_entries(entries)?,
            }
        }
        for (key, override_primitive) in self.overrides {
            tree.insert_primitive_path(&key, override_primitive, true)?;
        }
        Ok(tree)
    }
}

impl Default for ConfigLoader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zinder_source::{
        IndexerTlsMaterial, NodeAuth, NodeFallbackSections, NodeHealthConfig, NodeIndexerTls,
        NodeSection, NodeTarget,
    };

    #[test]
    fn suggests_double_underscore_and_normalized_known_vars() {
        assert_eq!(
            suggest_double_underscore_form("ZINDER_OPS_LISTEN_ADDR"),
            Some("ZINDER_OPS__LISTEN_ADDR".to_owned()),
        );
        assert_eq!(suggest_double_underscore_form("ZINDER_NETWORK"), None);
        assert_eq!(suggest_double_underscore_form("FOO_BAR"), None);
        assert_eq!(
            suggest_env_var("ZINDER_INJEST__SOURCE", "injest.source"),
            Some("ZINDER_INGEST__SOURCE".to_owned()),
        );
        assert_eq!(
            suggest_env_var(
                "ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADRR",
                "node_fallbacks.1.json_rpc_adrr",
            ),
            Some("ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR".to_owned()),
        );
    }

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

        let rendered = toml::to_string(&NodeSection::from_resolved(&target))?;

        assert_eq!(
            rendered,
            concat!(
                "json_rpc_addr = \"http://127.0.0.1:18232\"\n",
                "request_timeout_secs = 30\n",
                "max_response_bytes = 1\n",
                "max_concurrent_requests = 64\n",
                "\n",
                "[auth]\n",
                "method = \"none\"\n",
                "\n",
                "[health]\n",
                "addr = \"http://127.0.0.1:18233/ready\"\n",
                "poll_interval_ms = 1250\n",
                "verification_progress_floor = 0.875\n",
                "estimated_gap_floor_blocks = 42\n",
            )
        );
        Ok(())
    }

    #[test]
    fn indexer_tls_paths_survive_node_toml_serialization() -> Result<(), toml::ser::Error> {
        let target = NodeTarget::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:18232".to_owned(),
            NodeAuth::None,
            Duration::from_secs(30),
            std::num::NonZeroU64::MIN,
        )
        .with_indexer_grpc_addr(Some("https://zakura:8230".to_owned()))
        .with_indexer_tls(Some(
            NodeIndexerTls::new()
                .with_ca_cert_path("/run/secrets/indexer-ca.pem")
                .with_client_identity(
                    "/run/secrets/indexer-client.pem",
                    "/run/secrets/indexer-client.key",
                )
                .with_server_name("zakura.internal"),
        ));

        let rendered = toml::to_string(&NodeSection::from_resolved(&target))?;

        assert_eq!(
            rendered,
            concat!(
                "json_rpc_addr = \"http://127.0.0.1:18232\"\n",
                "indexer_grpc_addr = \"https://zakura:8230\"\n",
                "request_timeout_secs = 30\n",
                "max_response_bytes = 1\n",
                "max_concurrent_requests = 64\n",
                "\n",
                "[indexer_tls]\n",
                "ca_cert_path = \"/run/secrets/indexer-ca.pem\"\n",
                "client_cert_path = \"/run/secrets/indexer-client.pem\"\n",
                "client_key_path = \"/run/secrets/indexer-client.key\"\n",
                "server_name = \"zakura.internal\"\n",
                "\n",
                "[auth]\n",
                "method = \"none\"\n",
            )
        );
        Ok(())
    }

    #[test]
    fn indexer_tls_inline_pems_reach_loader_and_print_config_redacts_them()
    -> Result<(), ConfigError> {
        #[derive(Debug, Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct Probe {
            node: NodeSection,
        }

        let loaded: Probe = ConfigLoader::new()
            .with_zinder_env_from_map([
                (
                    "ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM".to_owned(),
                    "ca-secret-material\nline-two".to_owned(),
                ),
                (
                    "ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM".to_owned(),
                    "client-cert-secret".to_owned(),
                ),
                (
                    "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM".to_owned(),
                    "-----BEGIN PRIVATE KEY-----\nclient-key-secret\n".to_owned(),
                ),
            ])?
            .load()?;
        assert_eq!(
            loaded
                .node
                .indexer_tls
                .ca_cert_pem
                .as_ref()
                .map(zinder_source::SecretNodeInput::expose_secret),
            Some("ca-secret-material\nline-two")
        );
        let target = NodeTarget::new(
            Network::ZcashRegtest,
            "http://127.0.0.1:18232".to_owned(),
            NodeAuth::None,
            Duration::from_secs(30),
            std::num::NonZeroU64::MIN,
        )
        .with_indexer_grpc_addr(Some("https://zakura:8230".to_owned()))
        .with_indexer_tls(Some(
            NodeIndexerTls::new()
                .with_ca_cert_pem("ca-secret-material")
                .with_client_identity_pem("client-cert-secret", "client-key-secret"),
        ));

        let rendered = toml::to_string(&NodeSection::from_resolved(&target))
            .map_err(|source| ConfigError::Render { source })?;
        for secret in [
            "ca-secret-material",
            "client-cert-secret",
            "client-key-secret",
        ] {
            assert!(!rendered.contains(secret), "{rendered}");
        }
        assert!(
            rendered.contains("ca_cert_pem = \"[REDACTED]\""),
            "{rendered}"
        );
        Ok(())
    }

    #[test]
    fn fallback_indexer_tls_pem_variables_reach_the_loader() -> Result<(), ConfigError> {
        #[derive(Debug, Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct Probe {
            node: NodeSection,
            node_fallbacks: NodeFallbackSections,
        }

        let loaded: Probe = ConfigLoader::new()
            .with_zinder_env_from_map([
                (
                    "ZINDER_NODE__JSON_RPC_ADDR".to_owned(),
                    "http://127.0.0.1:18232".to_owned(),
                ),
                (
                    "ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR".to_owned(),
                    "https://fallback-1:8232".to_owned(),
                ),
                (
                    "ZINDER_NODE_FALLBACKS__2__JSON_RPC_ADDR".to_owned(),
                    "https://fallback-2:8232".to_owned(),
                ),
                (
                    "ZINDER_NODE_FALLBACKS__2__INDEXER_GRPC_ADDR".to_owned(),
                    "https://fallback-2:8230".to_owned(),
                ),
                (
                    "ZINDER_NODE_FALLBACKS__2__INDEXER_TLS__CLIENT_CERT_PEM".to_owned(),
                    "-----BEGIN CERTIFICATE-----\ncert-material\n".to_owned(),
                ),
                (
                    "ZINDER_NODE_FALLBACKS__2__INDEXER_TLS__CLIENT_KEY_PEM".to_owned(),
                    "-----BEGIN PRIVATE KEY-----\nsecret-material\n".to_owned(),
                ),
            ])?
            .load()?;
        let targets = NodeTarget::resolve_fallbacks(Network::ZcashRegtest, loaded.node_fallbacks)?;
        assert_eq!(targets.len(), 2);
        let tls = targets[1]
            .indexer_tls
            .as_ref()
            .ok_or_else(|| ConfigError::invalid("expected fallback 2 indexer_tls"))?;
        assert!(matches!(
            tls.client_identity.as_ref().map(|identity| &identity.key),
            Some(IndexerTlsMaterial::Inline(_))
        ));
        Ok(())
    }

    #[test]
    fn unified_schema_accepts_all_sections_and_rejects_typos_and_empty_env()
    -> Result<(), ConfigError> {
        let dir = tempfile::tempdir().map_err(|err| ConfigError::invalid(err.to_string()))?;
        let toml_path = dir.path().join("unified.toml");
        std::fs::write(
            &toml_path,
            concat!(
                "[network]\nname = \"zcash-regtest\"\n",
                "[ingest]\nsource = \"zebra-json-rpc\"\n",
                "[explorer]\nlisten_addr = \"127.0.0.1:9068\"\nwallet_query_endpoint = \"none\"\n",
            ),
        )
        .map_err(|err| ConfigError::invalid(err.to_string()))?;

        let loaded: ZinderConfig = ConfigLoader::new()
            .with_ops_section(RuntimeService::CompatCipherscan)?
            .with_file(Some(toml_path))
            .with_zinder_env_from_map([
                (
                    "ZINDER_QUERY__LISTEN_ADDR".to_owned(),
                    "127.0.0.1:9102".to_owned(),
                ),
                ("ZINDER_WALLET__PATH".to_owned(), "/tmp/wallet".to_owned()),
                (
                    "ZINDER_STORAGE__DATA_DIR".to_owned(),
                    "/tmp/data".to_owned(),
                ),
            ])?
            .load()?;
        assert_eq!(loaded.network.name.as_deref(), Some("zcash-regtest"));
        assert_eq!(
            loaded.ops.listen_addr,
            Toggle::Enabled(
                "127.0.0.1:9108"
                    .parse::<std::net::SocketAddr>()
                    .map_err(|err| ConfigError::invalid(err.to_string()))?
            )
        );
        assert!(loaded.explorer.wallet_query_endpoint.is_disabled());

        let disabled_ops: ZinderConfig = ConfigLoader::new()
            .with_ops_section(RuntimeService::Ingest)?
            .with_zinder_env_from_map([("ZINDER_OPS__LISTEN_ADDR".to_owned(), "none".to_owned())])?
            .load()?;
        assert!(disabled_ops.ops.listen_addr.is_disabled());
        Ok(())
    }

    #[test]
    fn unified_schema_rejects_typos_and_empty_env() -> Result<(), ConfigError> {
        let dir = tempfile::tempdir().map_err(|err| ConfigError::invalid(err.to_string()))?;
        let toml_path = dir.path().join("unified.toml");
        std::fs::write(&toml_path, "[network]\nname = \"zcash-regtest\"\n")
            .map_err(|err| ConfigError::invalid(err.to_string()))?;

        let empty_ops = ConfigLoader::new()
            .with_ops_section(RuntimeService::Ingest)?
            .with_zinder_env_from_map([("ZINDER_OPS__LISTEN_ADDR".to_owned(), String::new())])?
            .load::<ZinderConfig>();
        assert!(matches!(
            empty_ops,
            Err(ConfigError::InvalidEnvValue { .. })
        ));

        let typo = ConfigLoader::new()
            .with_ops_section(RuntimeService::CompatCipherscan)?
            .with_file(Some(toml_path))
            .with_zinder_env_from_map([("ZINDER_INJEST__SOURCE".to_owned(), "x".to_owned())])?
            .load::<ZinderConfig>();
        assert!(matches!(
            typo,
            Err(ConfigError::RejectedEnvVar {
                ref original_name,
                ref suggested_name,
                ..
            }) if original_name == "ZINDER_INJEST__SOURCE"
                && suggested_name.as_deref() == Some("ZINDER_INGEST__SOURCE")
        ));

        let nested_env_typo = ConfigLoader::new()
            .with_ops_section(RuntimeService::Ingest)?
            .with_zinder_env_from_map([(
                "ZINDER_QUERY__LISTEN_ADRR".to_owned(),
                "127.0.0.1:9102".to_owned(),
            )])?
            .load::<ZinderConfig>();
        assert!(matches!(
            nested_env_typo,
            Err(ConfigError::RejectedEnvVar {
                ref original_name,
                ref suggested_name,
                ..
            }) if original_name == "ZINDER_QUERY__LISTEN_ADRR"
                && suggested_name.as_deref() == Some("ZINDER_QUERY__LISTEN_ADDR")
        ));

        let nested_toml_typo_path = dir.path().join("nested_typo.toml");
        std::fs::write(
            &nested_toml_typo_path,
            "[network]\nname = \"zcash-regtest\"\n[query]\nlisten_adrr = \"127.0.0.1:9102\"\n",
        )
        .map_err(|err| ConfigError::invalid(err.to_string()))?;
        let nested_toml_typo = ConfigLoader::new()
            .with_ops_section(RuntimeService::Ingest)?
            .with_file(Some(nested_toml_typo_path))
            .load::<ZinderConfig>();
        assert!(nested_toml_typo.is_err());

        let shared_typo = ConfigLoader::new()
            .with_ops_section(RuntimeService::Query)?
            .with_zinder_env_from_map([(
                "ZINDER_WALLET__SECONDRY_PATH".to_owned(),
                "/typo".to_owned(),
            )])?
            .load::<ZinderConfig>();
        assert!(matches!(
            shared_typo,
            Err(ConfigError::RejectedEnvVar {
                ref original_name,
                ref suggested_name,
                ..
            }) if original_name == "ZINDER_WALLET__SECONDRY_PATH"
                && suggested_name.as_deref() == Some("ZINDER_WALLET__SECONDARY_PATH")
        ));
        Ok(())
    }

    #[test]
    fn fallback_tables_render_in_position_order_with_secrets_redacted()
    -> Result<(), toml::ser::Error> {
        #[derive(Serialize)]
        struct Rendered {
            node_fallbacks: NodeFallbackSections,
        }
        let fallback = |address: &str| {
            NodeTarget::new(
                Network::ZcashRegtest,
                address.to_owned(),
                NodeAuth::basic("zebra".to_owned(), "secret-password".to_owned()),
                Duration::from_secs(30),
                std::num::NonZeroU64::MIN,
            )
        };
        let targets = (1..=10)
            .map(|position| fallback(&format!("http://node-{position}:8232")))
            .collect::<Vec<_>>();

        let rendered = toml::to_string(&Rendered {
            node_fallbacks: NodeFallbackSections::from_resolved(&targets),
        })?;

        let second = rendered.find("[node_fallbacks.2]");
        let tenth = rendered.find("[node_fallbacks.10]");
        assert!(
            second.is_some() && tenth.is_some() && second < tenth,
            "{rendered}"
        );
        assert!(rendered.contains("password = \"[REDACTED]\""), "{rendered}");
        assert!(!rendered.contains("secret-password"), "{rendered}");
        Ok(())
    }

    #[test]
    fn endpoint_urls_render_with_their_credentials_redacted() -> Result<(), toml::ser::Error> {
        #[derive(Serialize)]
        struct Rendered {
            node: NodeSection,
            node_fallbacks: NodeFallbackSections,
        }
        let target = NodeTarget::new(
            Network::ZcashRegtest,
            "http://zebra:rpc-secret@127.0.0.1:18232".to_owned(),
            NodeAuth::None,
            Duration::from_secs(30),
            std::num::NonZeroU64::MIN,
        )
        .with_indexer_grpc_addr(Some(
            "http://zebra:indexer-secret@127.0.0.1:18230/?token=query-secret".to_owned(),
        ))
        .with_health(Some(NodeHealthConfig::new(
            "http://127.0.0.1:18233/ready?token=health-secret".to_owned(),
            Duration::from_millis(1_250),
            0.875,
            42,
        )));

        let rendered = toml::to_string(&Rendered {
            node: NodeSection::from_resolved(&target),
            node_fallbacks: NodeFallbackSections::from_resolved(std::slice::from_ref(&target)),
        })?;

        assert!(!rendered.contains("secret"), "{rendered}");
        let sections = rendered
            .split_once("[node_fallbacks.1]")
            .unwrap_or_default();
        for fields in <[&str; 2]>::from(sections) {
            assert!(
                fields.contains("json_rpc_addr = \"http://[REDACTED]@127.0.0.1:18232\""),
                "{rendered}"
            );
            assert!(
                fields.contains("addr = \"http://127.0.0.1:18233/ready?[REDACTED]\""),
                "{rendered}"
            );
        }
        Ok(())
    }

    #[test]
    fn unrecognized_indexer_tls_pem_name_is_rejected_with_env_name() {
        #[derive(Debug, Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct Probe {
            node: NodeSection,
        }

        let outcome = ConfigLoader::new()
            .with_zinder_env_from_map([(
                "ZINDER_NODE__INDEXER_TLS__CLIENT_PEM".to_owned(),
                "-----BEGIN CERTIFICATE-----\n".to_owned(),
            )])
            .and_then(ConfigLoader::load::<Probe>);
        assert!(matches!(
            outcome,
            Err(ConfigError::RejectedEnvVar {
                ref original_name,
                ..
            }) if original_name == "ZINDER_NODE__INDEXER_TLS__CLIENT_PEM"
        ));
    }

    #[test]
    fn indexer_tls_path_variables_reach_the_loader() -> Result<(), ConfigError> {
        #[derive(Debug, Default, Deserialize)]
        #[serde(default, deny_unknown_fields)]
        struct Probe {
            node: NodeSection,
        }

        let loaded: Probe = ConfigLoader::new()
            .with_zinder_env_from_map([(
                "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH".to_owned(),
                "/run/secrets/indexer-client.key".to_owned(),
            )])?
            .load()?;
        assert_eq!(
            loaded.node.indexer_tls.client_key_path.as_deref(),
            Some(Path::new("/run/secrets/indexer-client.key"))
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

        let rendered = toml::to_string(&NodeSection::from_resolved(&target))?;

        assert_eq!(
            rendered,
            concat!(
                "json_rpc_addr = \"http://127.0.0.1:18232\"\n",
                "request_timeout_secs = 30\n",
                "max_response_bytes = 1\n",
                "max_concurrent_requests = 64\n",
                "\n",
                "[auth]\n",
                "method = \"none\"\n",
            )
        );
        Ok(())
    }

    #[test]
    fn resolves_mutually_exclusive_bearer_tokens() -> Result<(), ConfigError> {
        let token_conflict = resolve_optional_bearer_token(
            Some(Path::new("/tmp/token")),
            Some("inline-token"),
            "test.bearer_token_path",
            "test.bearer_token",
        );
        assert!(token_conflict.is_err());
        assert!(load_bearer_token(None)?.is_none());
        Ok(())
    }
}
