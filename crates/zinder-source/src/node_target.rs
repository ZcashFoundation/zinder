//! Resolved upstream node endpoint shared by production binaries and live tests.
//!
//! `NodeTarget` is the canonical "what does it take to talk to a node" type:
//! every production binary's resolved `Config` embeds one, and live tests read
//! the same shape directly from environment variables. The `[node]` TOML block
//! deserializes into [`NodeSection`], and [`NodeTarget::resolve`] turns that
//! raw section into the resolved type.
//!
//! See [Public interfaces §Configuration Conventions](../../../docs/architecture/public-interfaces.md#configuration-conventions)
//! for the canonical TOML schema. The env-var keys (after `config-rs`'s `__`
//! flattening) are:
//!
//! | Env var | Field |
//! | ------- | ----- |
//! | `ZINDER_NETWORK` | resolved separately (each binary owns its `[network]`) |
//! | `ZINDER_NODE__JSON_RPC_ADDR` | [`NodeTarget::json_rpc_addr`] |
//! | `ZINDER_NODE__INDEXER_GRPC_ADDR` | [`NodeTarget::indexer_grpc_addr`] |
//! | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH` | [`NodeIndexerTls::ca_cert_path`] |
//! | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH` | [`NodeIndexerClientIdentity::cert_path`] |
//! | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH` | [`NodeIndexerClientIdentity::key_path`] |
//! | `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` | [`NodeIndexerTls::server_name`] |
//! | `ZINDER_NODE__AUTH__METHOD` | `none` / `basic` / `cookie` |
//! | `ZINDER_NODE__AUTH__USERNAME` | Basic-auth username |
//! | `ZINDER_NODE__AUTH__PASSWORD` | Basic-auth password |
//! | `ZINDER_NODE__AUTH__PATH` | Cookie-auth file path |
//! | `ZINDER_NODE__AUTH__COOKIE` | Cookie-auth inline credentials |
//! | `ZINDER_NODE__REQUEST_TIMEOUT_SECS` | [`NodeTarget::request_timeout`] |
//! | `ZINDER_NODE__MAX_RESPONSE_BYTES` | [`NodeTarget::max_response_bytes`] |
//!
//! Cookie credentials may be supplied either by file path or inline (but not
//! both). Inline credentials let `PaaS`-style deployments inject the secret
//! directly through the environment without writing a shim that materializes
//! a cookie file.
//!
//! Indexer TLS material is accepted only as file paths. The indexer URL scheme
//! decides whether the channel uses TLS; see
//! [ADR-0019](../../../docs/adrs/0019-transport-policy-ownership.md).

use std::{num::NonZeroU64, path::PathBuf, time::Duration};

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use zinder_core::Network;
use zinder_core::wire::decode_zinder_native_chain_name;

use crate::{CookieSource, DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES, NodeAuth};

/// Default per-RPC node request timeout when the configuration omits one.
pub const DEFAULT_NODE_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Default poll cadence for [`NodeHealthConfig::poll_interval`].
pub const DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS: u64 = 30_000;

/// Default lower bound for [`NodeHealthConfig::verification_progress_floor`].
pub const DEFAULT_NODE_HEALTH_VERIFICATION_PROGRESS_FLOOR: f64 = 0.999;

/// Default upper bound for [`NodeHealthConfig::estimated_gap_floor_blocks`].
pub const DEFAULT_NODE_HEALTH_ESTIMATED_GAP_FLOOR_BLOCKS: u32 = 10;

/// Resolved upstream node endpoint shared across production binaries and live tests.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct NodeTarget {
    /// Network the node answers for.
    pub network: Network,
    /// Node JSON-RPC base URL.
    pub json_rpc_addr: String,
    /// Optional Zebra indexer gRPC endpoint URL.
    ///
    /// When set, enables the streaming
    /// [`ZebraIndexerMempoolSource`](crate::ZebraIndexerMempoolSource)
    /// backend instead of the polling fallback. Zebra exposes this port
    /// when started with `ZEBRA_RPC__INDEXER_LISTEN_ADDR`.
    pub indexer_grpc_addr: Option<String>,
    /// TLS settings for an `https://` indexer endpoint.
    ///
    /// `None` with an `https://` indexer URL verifies the node against the
    /// system roots and presents no client certificate.
    pub indexer_tls: Option<NodeIndexerTls>,
    /// Node authentication.
    pub node_auth: NodeAuth,
    /// Per-RPC request timeout.
    pub request_timeout: Duration,
    /// Maximum JSON-RPC response body size accepted from the node.
    pub max_response_bytes: NonZeroU64,
    /// Per-broadcast timeout, applied only to `sendrawtransaction`.
    ///
    /// `None` falls back to `request_timeout`.
    pub broadcast_timeout: Option<Duration>,
    /// Resolved upstream-health probe configuration.
    ///
    /// `None` means the operator did not set `[node.health].addr`, so the
    /// upstream-health signal falls back to the JSON-RPC
    /// `getblockchaininfo.verificationprogress` path per
    /// [ADR-0015 §Upstream sync detection].
    ///
    /// [ADR-0015 §Upstream sync detection]:
    ///     ../../../docs/adrs/0015-phase-driven-ingest.md#upstream-sync-detection
    pub health: Option<NodeHealthConfig>,
}

/// Resolved `[node.health]` configuration for the upstream readiness probe.
///
/// All fields are post-default-application: a missing key in TOML resolves
/// to the documented default before this type is constructed. The probe
/// loop never re-applies defaults.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct NodeHealthConfig {
    /// Full URL of Zebra's `/ready` endpoint, e.g.
    /// `http://127.0.0.1:18233/ready`.
    pub addr: String,
    /// Cadence at which the probe task hits the endpoint.
    pub poll_interval: Duration,
    /// Fallback threshold: the JSON-RPC path treats upstream as not-ready
    /// when `verificationprogress < verification_progress_floor`.
    pub verification_progress_floor: f64,
    /// Fallback threshold: the JSON-RPC path treats upstream as not-ready
    /// when `estimatedheight - blocks > estimated_gap_floor_blocks`.
    pub estimated_gap_floor_blocks: u32,
}

impl NodeHealthConfig {
    /// Builds a config from already-resolved required fields.
    #[must_use]
    pub const fn new(
        addr: String,
        poll_interval: Duration,
        verification_progress_floor: f64,
        estimated_gap_floor_blocks: u32,
    ) -> Self {
        Self {
            addr,
            poll_interval,
            verification_progress_floor,
            estimated_gap_floor_blocks,
        }
    }

    /// Returns the floors used by the JSON-RPC fallback when no
    /// [`NodeHealthConfig`] is attached to the source. Callers that
    /// already hold a config should read its fields directly.
    #[must_use]
    pub const fn default_floors() -> (f64, u32) {
        (
            DEFAULT_NODE_HEALTH_VERIFICATION_PROGRESS_FLOOR,
            DEFAULT_NODE_HEALTH_ESTIMATED_GAP_FLOOR_BLOCKS,
        )
    }
}

impl NodeTarget {
    /// Builds a [`NodeTarget`] from already-resolved required fields. The
    /// optional [`NodeTarget::indexer_grpc_addr`] defaults to `None`; opt
    /// in to streaming with [`NodeTarget::with_indexer_grpc_addr`].
    /// [`NodeTarget::health`] also defaults to `None`; opt in to the
    /// upstream-health probe with [`NodeTarget::with_health`].
    #[must_use]
    pub const fn new(
        network: Network,
        json_rpc_addr: String,
        node_auth: NodeAuth,
        request_timeout: Duration,
        max_response_bytes: NonZeroU64,
    ) -> Self {
        Self {
            network,
            json_rpc_addr,
            indexer_grpc_addr: None,
            indexer_tls: None,
            node_auth,
            request_timeout,
            max_response_bytes,
            broadcast_timeout: None,
            health: None,
        }
    }

    /// Sets the per-broadcast timeout.
    #[must_use]
    pub fn with_broadcast_timeout(mut self, broadcast_timeout: Option<Duration>) -> Self {
        self.broadcast_timeout = broadcast_timeout;
        self
    }

    /// Returns a new [`NodeTarget`] with [`NodeTarget::indexer_grpc_addr`]
    /// replaced by `indexer_grpc_addr`.
    #[must_use]
    pub fn with_indexer_grpc_addr(mut self, indexer_grpc_addr: Option<String>) -> Self {
        self.indexer_grpc_addr = indexer_grpc_addr;
        self
    }

    /// Returns a new [`NodeTarget`] with [`NodeTarget::indexer_tls`] replaced
    /// by `indexer_tls`.
    #[must_use]
    pub fn with_indexer_tls(mut self, indexer_tls: Option<NodeIndexerTls>) -> Self {
        self.indexer_tls = indexer_tls;
        self
    }

    /// Returns a new [`NodeTarget`] with [`NodeTarget::health`] replaced by
    /// `health`.
    #[must_use]
    pub fn with_health(mut self, health: Option<NodeHealthConfig>) -> Self {
        self.health = health;
        self
    }

    /// Resolves an optional [`NodeTarget`] from a deserialized [`NodeSection`].
    ///
    /// Returns `Ok(None)` when [`NodeSection::json_rpc_addr`] is unset, so a
    /// missing `[node]` section means "no node target," not "validation
    /// failure." Once the address is present every other required field is
    /// validated through [`NodeTarget::resolve`]. Used by reader binaries
    /// where the broadcaster is optional.
    pub fn resolve_optional(
        network: Network,
        section: NodeSection,
    ) -> Result<Option<Self>, NodeConfigError> {
        if section.json_rpc_addr.is_none() {
            return Ok(None);
        }
        Self::resolve(network, section).map(Some)
    }

    /// Resolves a [`NodeTarget`] from a deserialized [`NodeSection`].
    ///
    /// Each production binary calls this after deserializing its raw config
    /// through `config-rs`. Live tests call [`NodeTarget::from_environment`].
    pub fn resolve(network: Network, section: NodeSection) -> Result<Self, NodeConfigError> {
        let json_rpc_addr = section.json_rpc_addr.ok_or(NodeConfigError::MissingField {
            field: "node.json_rpc_addr",
        })?;
        let request_timeout = Duration::from_secs(
            section
                .request_timeout_secs
                .unwrap_or(DEFAULT_NODE_REQUEST_TIMEOUT_SECS),
        );
        let max_response_bytes_value = section
            .max_response_bytes
            .unwrap_or_else(|| DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES.get());
        let max_response_bytes =
            NonZeroU64::new(max_response_bytes_value).ok_or(NodeConfigError::Invalid {
                reason: "node.max_response_bytes must be greater than zero",
            })?;
        let broadcast_timeout = section.broadcast_timeout_secs.map(Duration::from_secs);
        let node_auth = resolve_node_auth(section.auth)?;
        let health = resolve_node_health(section.health)?;
        let indexer_grpc_addr = nonempty_optional(section.indexer_grpc_addr);
        let indexer_tls =
            resolve_node_indexer_tls(indexer_grpc_addr.as_deref(), section.indexer_tls)?;

        Ok(Self::new(
            network,
            json_rpc_addr,
            node_auth,
            request_timeout,
            max_response_bytes,
        )
        .with_indexer_grpc_addr(indexer_grpc_addr)
        .with_indexer_tls(indexer_tls)
        .with_broadcast_timeout(broadcast_timeout)
        .with_health(health))
    }

    /// Resolves a [`NodeTarget`] directly from the unified env-var schema.
    ///
    /// Reads `ZINDER_NETWORK` and `ZINDER_NODE__*` from `std::env` without
    /// going through the production loader, which rejects sensitive leaves.
    /// Used by live tests via `zinder_testkit::live::LiveTestEnv` so the same
    /// env-var schema serves production and tests.
    pub fn from_environment() -> Result<Self, NodeConfigError> {
        let network_name = read_required("ZINDER_NETWORK")?;
        let network = decode_zinder_native_chain_name(&network_name).ok().ok_or(
            NodeConfigError::Invalid {
                reason: "ZINDER_NETWORK must be zcash-mainnet, zcash-testnet, or zcash-regtest",
            },
        )?;

        let section = NodeSection {
            json_rpc_addr: read_optional("ZINDER_NODE__JSON_RPC_ADDR"),
            indexer_grpc_addr: read_optional("ZINDER_NODE__INDEXER_GRPC_ADDR"),
            indexer_tls: NodeIndexerTlsSection {
                ca_cert_path: read_optional("ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH")
                    .map(PathBuf::from),
                client_cert_path: read_optional("ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH")
                    .map(PathBuf::from),
                client_key_path: read_optional("ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH")
                    .map(PathBuf::from),
                server_name: read_optional("ZINDER_NODE__INDEXER_TLS__SERVER_NAME"),
            },
            request_timeout_secs: read_optional_parsed::<u64>(
                "ZINDER_NODE__REQUEST_TIMEOUT_SECS",
                "node.request_timeout_secs",
            )?,
            max_response_bytes: read_optional_parsed::<u64>(
                "ZINDER_NODE__MAX_RESPONSE_BYTES",
                "node.max_response_bytes",
            )?,
            broadcast_timeout_secs: read_optional_parsed::<u64>(
                "ZINDER_NODE__BROADCAST_TIMEOUT_SECS",
                "node.broadcast_timeout_secs",
            )?,
            auth: NodeAuthSection {
                method: read_optional("ZINDER_NODE__AUTH__METHOD"),
                username: read_optional("ZINDER_NODE__AUTH__USERNAME"),
                password: read_optional("ZINDER_NODE__AUTH__PASSWORD"),
                path: read_optional("ZINDER_NODE__AUTH__PATH").map(PathBuf::from),
                cookie: read_optional("ZINDER_NODE__AUTH__COOKIE"),
            },
            health: NodeHealthSection {
                addr: read_optional("ZINDER_NODE__HEALTH__ADDR"),
                poll_interval_ms: read_optional_parsed::<u64>(
                    "ZINDER_NODE__HEALTH__POLL_INTERVAL_MS",
                    "node.health.poll_interval_ms",
                )?,
                verification_progress_floor: read_optional_parsed::<f64>(
                    "ZINDER_NODE__HEALTH__VERIFICATION_PROGRESS_FLOOR",
                    "node.health.verification_progress_floor",
                )?,
                estimated_gap_floor_blocks: read_optional_parsed::<u32>(
                    "ZINDER_NODE__HEALTH__ESTIMATED_GAP_FLOOR_BLOCKS",
                    "node.health.estimated_gap_floor_blocks",
                )?,
            },
        };

        Self::resolve(network, section)
    }
}

/// Raw `[node]` config section. Each binary's typed `Config` embeds one of
/// these and passes it to [`NodeTarget::resolve`].
///
/// `[node]` describes the upstream node itself (endpoint, transport, auth).
/// Ingest's choice of which adapter implementation to instantiate lives on
/// `[ingest].source` per [ADR-0016](../../../docs/adrs/0016-source-segment-fetching.md)
/// because it is a writer-private implementation decision, not a property
/// of the node.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeSection {
    /// Node JSON-RPC base URL.
    pub json_rpc_addr: Option<String>,
    /// Zebra indexer gRPC endpoint URL.
    ///
    /// When set, enables the streaming mempool source backend and chain-tip
    /// wakeups. Omit to fall back to JSON-RPC polling. An `https://` URL
    /// negotiates TLS.
    pub indexer_grpc_addr: Option<String>,
    /// Per-RPC request timeout in seconds.
    pub request_timeout_secs: Option<u64>,
    /// Maximum JSON-RPC response body size accepted from the node.
    pub max_response_bytes: Option<u64>,
    /// Per-broadcast timeout in seconds, applied only to `sendrawtransaction`.
    ///
    /// Overrides `request_timeout_secs` for broadcast calls only. Omit to
    /// fall back to `request_timeout_secs`.
    pub broadcast_timeout_secs: Option<u64>,
    /// TLS subsection for an `https://` indexer endpoint.
    pub indexer_tls: NodeIndexerTlsSection,
    /// Authentication subsection.
    pub auth: NodeAuthSection,
    /// Upstream-readiness probe subsection.
    pub health: NodeHealthSection,
}

/// Raw `[node.indexer_tls]` config section.
///
/// Every field holds a path or a name, never key material. Empty or
/// whitespace-only fields count as absent, so an environment that sets a
/// variable to the empty string leaves the field unset.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeIndexerTlsSection {
    /// PEM CA bundle that verifies the indexer's server certificate. Omit to
    /// use the system roots.
    pub ca_cert_path: Option<PathBuf>,
    /// PEM client certificate chain presented to the indexer. Set together
    /// with [`Self::client_key_path`].
    pub client_cert_path: Option<PathBuf>,
    /// PEM private key for [`Self::client_cert_path`].
    pub client_key_path: Option<PathBuf>,
    /// Name verified against the indexer's certificate when it differs from
    /// the URL host.
    pub server_name: Option<String>,
}

/// Resolved `[node.indexer_tls]` configuration.
///
/// Holds file paths only. The transport reads the files each time it builds
/// an indexer channel, so key bytes never live in configuration values.
#[non_exhaustive]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeIndexerTls {
    /// PEM CA bundle that verifies the indexer's server certificate. `None`
    /// uses the system roots.
    pub ca_cert_path: Option<PathBuf>,
    /// Client identity presented to the indexer. `None` presents none.
    pub client_identity: Option<NodeIndexerClientIdentity>,
    /// Name verified against the indexer's certificate instead of the URL
    /// host.
    pub server_name: Option<String>,
}

impl NodeIndexerTls {
    /// Returns settings that verify the indexer against the system roots and
    /// present no client certificate.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Verifies the indexer's certificate against the PEM CA bundle at `path`
    /// instead of the system roots.
    #[must_use]
    pub fn with_ca_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_cert_path = Some(path.into());
        self
    }

    /// Presents the PEM certificate chain at `cert_path`, signed with the PEM
    /// private key at `key_path`.
    #[must_use]
    pub fn with_client_identity(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Self {
        self.client_identity = Some(NodeIndexerClientIdentity {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        });
        self
    }

    /// Verifies the indexer's certificate against `server_name` instead of the
    /// URL host.
    #[must_use]
    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = Some(server_name.into());
        self
    }
}

/// Client certificate and key paths for indexer mutual TLS.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeIndexerClientIdentity {
    /// PEM client certificate chain, leaf first.
    pub cert_path: PathBuf,
    /// PEM private key for the leaf certificate.
    pub key_path: PathBuf,
}

/// Raw `[node.health]` config section.
///
/// Defaults applied during [`NodeTarget::resolve`]. The probe is opt-in:
/// omitting [`Self::addr`] leaves the resolved
/// [`NodeTarget::health`] at `None` and the writer falls back to the
/// `verificationprogress` JSON-RPC path per
/// [ADR-0015 §Upstream sync detection].
///
/// [ADR-0015 §Upstream sync detection]:
///     ../../../docs/adrs/0015-phase-driven-ingest.md#upstream-sync-detection
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeHealthSection {
    /// Full URL of Zebra's `/ready` endpoint when the operator exposes it.
    pub addr: Option<String>,
    /// Probe cadence in milliseconds. Defaults to
    /// [`DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS`].
    pub poll_interval_ms: Option<u64>,
    /// Lower bound for `verificationprogress` used by the JSON-RPC
    /// fallback. Defaults to
    /// [`DEFAULT_NODE_HEALTH_VERIFICATION_PROGRESS_FLOOR`].
    pub verification_progress_floor: Option<f64>,
    /// Upper bound for `estimatedheight - blocks` used by the JSON-RPC
    /// fallback. Defaults to
    /// [`DEFAULT_NODE_HEALTH_ESTIMATED_GAP_FLOOR_BLOCKS`].
    pub estimated_gap_floor_blocks: Option<u32>,
}

/// Raw `[node.auth]` config section.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeAuthSection {
    /// Auth method: `none`, `basic`, or `cookie`. Defaults to `none`.
    pub method: Option<String>,
    /// Basic-auth username.
    pub username: Option<String>,
    /// Basic-auth password.
    pub password: Option<String>,
    /// Cookie-auth file path. Mutually exclusive with [`Self::cookie`].
    pub path: Option<PathBuf>,
    /// Cookie-auth inline credentials. Used by `PaaS` deployments that inject
    /// the secret as a configuration value. Mutually exclusive with
    /// [`Self::path`].
    pub cookie: Option<String>,
}

/// Error returned while resolving node configuration.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum NodeConfigError {
    /// A required field is missing.
    #[error("missing required configuration field: {field}")]
    MissingField {
        /// Configuration field path.
        field: &'static str,
    },

    /// A field value is invalid for the chosen mode.
    #[error("invalid node configuration: {reason}")]
    Invalid {
        /// Human-readable reason describing the validation failure.
        reason: &'static str,
    },

    /// A field is not valid in combination with the selected auth method.
    #[error("{field} is not valid when node.auth.method is {method}")]
    AuthFieldNotApplicable {
        /// Conflicting field path.
        field: &'static str,
        /// Selected auth method.
        method: &'static str,
    },

    /// Auth method string is not recognized.
    #[error("unknown node.auth.method: {method}")]
    UnknownAuthMethod {
        /// Unrecognized method string.
        method: String,
    },

    /// `[node.indexer_tls]` is set without an indexer endpoint to apply it to.
    #[error("node.indexer_tls is set but node.indexer_grpc_addr is not")]
    IndexerTlsWithoutIndexerAddr,

    /// `[node.indexer_tls]` is set for an indexer URL that does not use TLS.
    #[error(
        "node.indexer_tls requires an https:// node.indexer_grpc_addr; the configured URL does not negotiate TLS"
    )]
    IndexerTlsRequiresHttps,

    /// Only one of the client certificate and client key paths is set.
    #[error(
        "node.indexer_tls.client_cert_path and node.indexer_tls.client_key_path must be set together"
    )]
    IndexerTlsClientIdentityIncomplete,

    /// An environment variable could not be parsed as the expected type.
    #[error("failed to parse environment variable for {field}: {reason}")]
    EnvParseFailed {
        /// Configuration field path.
        field: &'static str,
        /// Parse failure reason.
        reason: String,
    },
}

fn resolve_node_health(
    section: NodeHealthSection,
) -> Result<Option<NodeHealthConfig>, NodeConfigError> {
    let Some(addr) = section.addr else {
        return Ok(None);
    };
    let poll_interval_ms = section
        .poll_interval_ms
        .unwrap_or(DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS);
    if poll_interval_ms == 0 {
        return Err(NodeConfigError::Invalid {
            reason: "node.health.poll_interval_ms must be greater than zero",
        });
    }
    let verification_progress_floor = section
        .verification_progress_floor
        .unwrap_or(DEFAULT_NODE_HEALTH_VERIFICATION_PROGRESS_FLOOR);
    if !(verification_progress_floor > 0.0 && verification_progress_floor < 1.0) {
        return Err(NodeConfigError::Invalid {
            reason: "node.health.verification_progress_floor must be in (0.0, 1.0)",
        });
    }
    let estimated_gap_floor_blocks = section
        .estimated_gap_floor_blocks
        .unwrap_or(DEFAULT_NODE_HEALTH_ESTIMATED_GAP_FLOOR_BLOCKS);

    Ok(Some(NodeHealthConfig::new(
        addr,
        Duration::from_millis(poll_interval_ms),
        verification_progress_floor,
        estimated_gap_floor_blocks,
    )))
}

fn resolve_node_indexer_tls(
    indexer_grpc_addr: Option<&str>,
    section: NodeIndexerTlsSection,
) -> Result<Option<NodeIndexerTls>, NodeConfigError> {
    let ca_cert_path = nonempty_path(section.ca_cert_path);
    let client_cert_path = nonempty_path(section.client_cert_path);
    let client_key_path = nonempty_path(section.client_key_path);
    let server_name = nonempty_optional(section.server_name);
    if ca_cert_path.is_none()
        && client_cert_path.is_none()
        && client_key_path.is_none()
        && server_name.is_none()
    {
        return Ok(None);
    }

    let indexer_grpc_addr =
        indexer_grpc_addr.ok_or(NodeConfigError::IndexerTlsWithoutIndexerAddr)?;
    if !uses_tls_scheme(indexer_grpc_addr) {
        return Err(NodeConfigError::IndexerTlsRequiresHttps);
    }
    let client_identity = match (client_cert_path, client_key_path) {
        (Some(cert_path), Some(key_path)) => Some(NodeIndexerClientIdentity {
            cert_path,
            key_path,
        }),
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => {
            return Err(NodeConfigError::IndexerTlsClientIdentityIncomplete);
        }
    };

    Ok(Some(NodeIndexerTls {
        ca_cert_path,
        client_identity,
        server_name,
    }))
}

/// Returns whether `url` names the `https` scheme, which selects TLS for the
/// indexer channel. Schemes are case-insensitive, as in the transport's URI
/// parser.
fn uses_tls_scheme(url: &str) -> bool {
    url.get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
}

fn resolve_node_auth(section: NodeAuthSection) -> Result<NodeAuth, NodeConfigError> {
    let method = section.method.as_deref().unwrap_or("none");

    match method {
        "none" => {
            warn_unused(section.username.is_some(), "node.auth.username");
            warn_unused(section.password.is_some(), "node.auth.password");
            warn_unused(section.path.is_some(), "node.auth.path");
            warn_unused(section.cookie.is_some(), "node.auth.cookie");
            Ok(NodeAuth::None)
        }
        "basic" => {
            reject_present(section.path.is_some(), "node.auth.path", "basic")?;
            reject_present(section.cookie.is_some(), "node.auth.cookie", "basic")?;
            let username = section.username.ok_or(NodeConfigError::MissingField {
                field: "node.auth.username",
            })?;
            let password = section.password.ok_or(NodeConfigError::MissingField {
                field: "node.auth.password",
            })?;
            Ok(NodeAuth::basic(username, password))
        }
        "cookie" => {
            reject_present(section.username.is_some(), "node.auth.username", "cookie")?;
            reject_present(section.password.is_some(), "node.auth.password", "cookie")?;
            let source = match (section.path, section.cookie) {
                (Some(_), Some(_)) => {
                    return Err(NodeConfigError::Invalid {
                        reason: "node.auth.path and node.auth.cookie are mutually exclusive",
                    });
                }
                (Some(path), None) => CookieSource::File(path),
                (None, Some(content)) => CookieSource::Inline(SecretString::from(content)),
                (None, None) => {
                    return Err(NodeConfigError::MissingField {
                        field: "node.auth.path or node.auth.cookie",
                    });
                }
            };
            Ok(NodeAuth::Cookie(source))
        }
        other => Err(NodeConfigError::UnknownAuthMethod {
            method: other.to_owned(),
        }),
    }
}

fn reject_present(
    is_field_present: bool,
    field: &'static str,
    method: &'static str,
) -> Result<(), NodeConfigError> {
    if is_field_present {
        return Err(NodeConfigError::AuthFieldNotApplicable { field, method });
    }
    Ok(())
}

fn warn_unused(is_field_present: bool, field: &'static str) {
    if is_field_present {
        warn!(
            event = "node_auth_field_ignored",
            field,
            method = "none",
            "node.auth field is set but ignored because node.auth.method is none"
        );
    }
}

fn read_required(env_var: &'static str) -> Result<String, NodeConfigError> {
    std::env::var(env_var).map_err(|_| NodeConfigError::MissingField { field: env_var })
}

fn read_optional(env_var: &'static str) -> Option<String> {
    std::env::var(env_var).ok()
}

fn nonempty_optional(text: Option<String>) -> Option<String> {
    text.filter(|text| !text.trim().is_empty())
}

fn nonempty_path(path: Option<PathBuf>) -> Option<PathBuf> {
    path.filter(|path| !path.as_os_str().to_string_lossy().trim().is_empty())
}

fn read_optional_parsed<TargetType>(
    env_var: &'static str,
    field: &'static str,
) -> Result<Option<TargetType>, NodeConfigError>
where
    TargetType: std::str::FromStr,
    TargetType::Err: std::fmt::Display,
{
    std::env::var(env_var).map_or(Ok(None), |raw| {
        raw.parse::<TargetType>()
            .map(Some)
            .map_err(|error| NodeConfigError::EnvParseFailed {
                field,
                reason: error.to_string(),
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_basic_auth_round_trip() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: Some("http://127.0.0.1:8155".to_owned()),
            request_timeout_secs: Some(15),
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("basic".to_owned()),
                username: Some("zebra".to_owned()),
                password: Some("zebra".to_owned()),
                path: None,
                cookie: None,
            },
            health: NodeHealthSection::default(),
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;

        assert_eq!(target.network, Network::ZcashRegtest);
        assert_eq!(target.json_rpc_addr, "http://127.0.0.1:8232");
        assert_eq!(
            target.indexer_grpc_addr.as_deref(),
            Some("http://127.0.0.1:8155")
        );
        assert_eq!(target.request_timeout, Duration::from_secs(15));
        assert_eq!(target.node_auth.scheme_name(), "basic");
        Ok(())
    }

    #[test]
    fn resolve_none_auth_tolerates_unused_path() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("none".to_owned()),
                username: None,
                password: None,
                path: Some(PathBuf::from("/var/run/auth/.cookie")),
                cookie: None,
            },
            health: NodeHealthSection::default(),
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;

        assert!(matches!(target.node_auth, NodeAuth::None));
        Ok(())
    }

    #[test]
    fn resolve_omits_indexer_grpc_addr_when_unset() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("none".to_owned()),
                username: None,
                password: None,
                path: None,
                cookie: None,
            },
            health: NodeHealthSection::default(),
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;

        assert!(target.indexer_grpc_addr.is_none());
        Ok(())
    }

    #[test]
    fn resolve_omits_indexer_grpc_addr_when_blank() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: Some("   ".to_owned()),
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection::default(),
            health: NodeHealthSection::default(),
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;

        assert!(target.indexer_grpc_addr.is_none());
        Ok(())
    }

    fn indexer_tls_section(
        indexer_grpc_addr: Option<&str>,
        indexer_tls: NodeIndexerTlsSection,
    ) -> NodeSection {
        NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: indexer_grpc_addr.map(str::to_owned),
            indexer_tls,
            ..NodeSection::default()
        }
    }

    fn mutual_tls_section() -> NodeIndexerTlsSection {
        NodeIndexerTlsSection {
            ca_cert_path: Some(PathBuf::from("/run/secrets/indexer-ca.pem")),
            client_cert_path: Some(PathBuf::from("/run/secrets/indexer-client.pem")),
            client_key_path: Some(PathBuf::from("/run/secrets/indexer-client.key")),
            server_name: Some("zakura.internal".to_owned()),
        }
    }

    #[test]
    fn https_indexer_without_tls_section_uses_system_roots() -> Result<(), NodeConfigError> {
        let target = NodeTarget::resolve(
            Network::ZcashRegtest,
            indexer_tls_section(
                Some("https://zakura:8230"),
                NodeIndexerTlsSection::default(),
            ),
        )?;

        assert_eq!(
            target.indexer_grpc_addr.as_deref(),
            Some("https://zakura:8230")
        );
        assert!(target.indexer_tls.is_none());
        Ok(())
    }

    #[test]
    fn indexer_tls_section_resolves_paths_and_server_name() -> Result<(), NodeConfigError> {
        let target = NodeTarget::resolve(
            Network::ZcashRegtest,
            indexer_tls_section(Some("https://zakura:8230"), mutual_tls_section()),
        )?;

        assert_eq!(
            target.indexer_tls,
            Some(
                NodeIndexerTls::new()
                    .with_ca_cert_path("/run/secrets/indexer-ca.pem")
                    .with_client_identity(
                        "/run/secrets/indexer-client.pem",
                        "/run/secrets/indexer-client.key",
                    )
                    .with_server_name("zakura.internal")
            )
        );
        Ok(())
    }

    #[test]
    fn indexer_tls_scheme_match_ignores_case() -> Result<(), NodeConfigError> {
        let target = NodeTarget::resolve(
            Network::ZcashRegtest,
            indexer_tls_section(Some("HTTPS://zakura:8230"), mutual_tls_section()),
        )?;

        assert!(target.indexer_tls.is_some());
        Ok(())
    }

    #[test]
    fn indexer_tls_section_rejects_plaintext_indexer_url() {
        let outcome = NodeTarget::resolve(
            Network::ZcashRegtest,
            indexer_tls_section(Some("http://zakura:8230"), mutual_tls_section()),
        );

        assert!(matches!(
            outcome,
            Err(NodeConfigError::IndexerTlsRequiresHttps)
        ));
    }

    #[test]
    fn indexer_tls_section_requires_indexer_url() {
        for indexer_grpc_addr in [None, Some(""), Some("  ")] {
            let outcome = NodeTarget::resolve(
                Network::ZcashRegtest,
                indexer_tls_section(indexer_grpc_addr, mutual_tls_section()),
            );

            assert!(matches!(
                outcome,
                Err(NodeConfigError::IndexerTlsWithoutIndexerAddr)
            ));
        }
    }

    #[test]
    fn indexer_client_certificate_and_key_are_set_together() {
        let without_key = NodeIndexerTlsSection {
            client_key_path: None,
            ..mutual_tls_section()
        };
        let without_certificate = NodeIndexerTlsSection {
            client_cert_path: Some(PathBuf::from(" ")),
            ..mutual_tls_section()
        };

        for indexer_tls in [without_key, without_certificate] {
            let outcome = NodeTarget::resolve(
                Network::ZcashRegtest,
                indexer_tls_section(Some("https://zakura:8230"), indexer_tls),
            );

            assert!(matches!(
                outcome,
                Err(NodeConfigError::IndexerTlsClientIdentityIncomplete)
            ));
        }
    }

    #[test]
    fn blank_indexer_tls_fields_count_as_absent() -> Result<(), NodeConfigError> {
        let blank = NodeIndexerTlsSection {
            ca_cert_path: Some(PathBuf::new()),
            client_cert_path: Some(PathBuf::from("   ")),
            client_key_path: Some(PathBuf::new()),
            server_name: Some(String::new()),
        };

        let target = NodeTarget::resolve(
            Network::ZcashRegtest,
            indexer_tls_section(Some("http://127.0.0.1:8230"), blank),
        )?;

        assert!(target.indexer_tls.is_none());
        Ok(())
    }

    #[test]
    fn resolve_rejects_missing_json_rpc_addr() {
        let section = NodeSection::default();
        let outcome = NodeTarget::resolve(Network::ZcashRegtest, section);

        assert!(matches!(
            outcome,
            Err(NodeConfigError::MissingField {
                field: "node.json_rpc_addr"
            })
        ));
    }

    #[test]
    fn resolve_rejects_basic_auth_with_path() {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("basic".to_owned()),
                username: Some("zebra".to_owned()),
                password: Some("zebra".to_owned()),
                path: Some(PathBuf::from("/etc/zebra/cookie")),
                cookie: None,
            },
            health: NodeHealthSection::default(),
        };
        let outcome = NodeTarget::resolve(Network::ZcashRegtest, section);

        assert!(matches!(
            outcome,
            Err(NodeConfigError::AuthFieldNotApplicable {
                field: "node.auth.path",
                method: "basic",
            })
        ));
    }

    #[test]
    fn resolve_cookie_auth_from_path() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("cookie".to_owned()),
                username: None,
                password: None,
                path: Some(PathBuf::from("/var/run/auth/.cookie")),
                cookie: None,
            },
            health: NodeHealthSection::default(),
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;

        assert!(matches!(
            target.node_auth,
            NodeAuth::Cookie(CookieSource::File(_))
        ));
        Ok(())
    }

    #[test]
    fn resolve_cookie_auth_from_inline() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("cookie".to_owned()),
                username: None,
                password: None,
                path: None,
                cookie: Some("user:cookie-secret".to_owned()),
            },
            health: NodeHealthSection::default(),
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;

        assert!(matches!(
            target.node_auth,
            NodeAuth::Cookie(CookieSource::Inline(_))
        ));
        Ok(())
    }

    #[test]
    fn resolve_rejects_cookie_auth_with_both_path_and_inline() {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("cookie".to_owned()),
                username: None,
                password: None,
                path: Some(PathBuf::from("/var/run/auth/.cookie")),
                cookie: Some("user:cookie-secret".to_owned()),
            },
            health: NodeHealthSection::default(),
        };
        let outcome = NodeTarget::resolve(Network::ZcashRegtest, section);

        assert!(matches!(
            outcome,
            Err(NodeConfigError::Invalid {
                reason: "node.auth.path and node.auth.cookie are mutually exclusive",
            })
        ));
    }

    #[test]
    fn resolve_rejects_cookie_auth_without_path_or_inline() {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("cookie".to_owned()),
                username: None,
                password: None,
                path: None,
                cookie: None,
            },
            health: NodeHealthSection::default(),
        };
        let outcome = NodeTarget::resolve(Network::ZcashRegtest, section);

        assert!(matches!(
            outcome,
            Err(NodeConfigError::MissingField {
                field: "node.auth.path or node.auth.cookie",
            })
        ));
    }

    #[test]
    fn resolve_rejects_unknown_auth_method() {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("oauth".to_owned()),
                username: None,
                password: None,
                path: None,
                cookie: None,
            },
            health: NodeHealthSection::default(),
        };
        let outcome = NodeTarget::resolve(Network::ZcashRegtest, section);

        assert!(matches!(
            outcome,
            Err(NodeConfigError::UnknownAuthMethod { method }) if method == "oauth"
        ));
    }

    #[test]
    fn resolve_omits_health_when_addr_unset() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection::default(),
            health: NodeHealthSection::default(),
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;
        assert!(target.health.is_none());
        Ok(())
    }

    #[test]
    fn resolve_health_applies_defaults_when_addr_set() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection::default(),
            health: NodeHealthSection {
                addr: Some("http://127.0.0.1:18233/ready".to_owned()),
                poll_interval_ms: None,
                verification_progress_floor: None,
                estimated_gap_floor_blocks: None,
            },
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;
        let health = target.health.ok_or(NodeConfigError::Invalid {
            reason: "expected resolved health config",
        })?;
        assert_eq!(health.addr, "http://127.0.0.1:18233/ready");
        assert_eq!(
            health.poll_interval,
            Duration::from_millis(DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS)
        );
        assert!(
            (health.verification_progress_floor - DEFAULT_NODE_HEALTH_VERIFICATION_PROGRESS_FLOOR)
                .abs()
                < f64::EPSILON
        );
        assert_eq!(
            health.estimated_gap_floor_blocks,
            DEFAULT_NODE_HEALTH_ESTIMATED_GAP_FLOOR_BLOCKS
        );
        Ok(())
    }

    #[test]
    fn resolve_health_rejects_zero_poll_interval() {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection::default(),
            health: NodeHealthSection {
                addr: Some("http://127.0.0.1:18233/ready".to_owned()),
                poll_interval_ms: Some(0),
                verification_progress_floor: None,
                estimated_gap_floor_blocks: None,
            },
        };
        assert!(matches!(
            NodeTarget::resolve(Network::ZcashRegtest, section),
            Err(NodeConfigError::Invalid {
                reason: "node.health.poll_interval_ms must be greater than zero",
            })
        ));
    }

    #[test]
    fn resolve_health_rejects_progress_floor_out_of_range() {
        for floor in [0.0_f64, 1.0_f64, -0.5_f64, 1.5_f64] {
            let section = NodeSection {
                json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
                indexer_grpc_addr: None,
                request_timeout_secs: None,
                max_response_bytes: None,
                broadcast_timeout_secs: None,
                indexer_tls: NodeIndexerTlsSection::default(),
                auth: NodeAuthSection::default(),
                health: NodeHealthSection {
                    addr: Some("http://127.0.0.1:18233/ready".to_owned()),
                    poll_interval_ms: None,
                    verification_progress_floor: Some(floor),
                    estimated_gap_floor_blocks: None,
                },
            };
            assert!(matches!(
                NodeTarget::resolve(Network::ZcashRegtest, section),
                Err(NodeConfigError::Invalid {
                    reason: "node.health.verification_progress_floor must be in (0.0, 1.0)",
                })
            ));
        }
    }
}
