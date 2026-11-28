//! Resolved upstream node endpoint shared by production binaries and live tests.
//!
//! `NodeTarget` is the canonical "what does it take to talk to a node" type:
//! every production binary's resolved `Config` embeds one, and live tests read
//! the same shape directly from environment variables. The `[node]` TOML block
//! deserializes into [`NodeSection`], and [`NodeTarget::resolve`] turns that
//! raw section into the resolved type.
//!
//! See [Configuration](../../../docs/reference/configuration.md)
//! for the canonical TOML schema. The env-var keys are:
//!
//! | Env var | Field |
//! | ------- | ----- |
//! | `ZINDER_NETWORK` | resolved separately (each binary owns its `[network]`) |
//! | `ZINDER_NODE__JSON_RPC_ADDR` | [`NodeTarget::json_rpc_addr`] |
//! | `ZINDER_NODE__INDEXER_GRPC_ADDR` | [`NodeTarget::indexer_grpc_addr`] |
//! | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PATH` | [`NodeIndexerTls::ca_cert`] (file path) |
//! | `ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM` | [`NodeIndexerTls::ca_cert`] (inline PEM) |
//! | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH` | [`NodeIndexerClientIdentity::cert`] (file path) |
//! | `ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM` | [`NodeIndexerClientIdentity::cert`] (inline PEM) |
//! | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH` | [`NodeIndexerClientIdentity::key`] (file path) |
//! | `ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM` | [`NodeIndexerClientIdentity::key`] (inline PEM) |
//! | `ZINDER_NODE__INDEXER_TLS__SERVER_NAME` | [`NodeIndexerTls::server_name`] |
//! | `ZINDER_NODE__AUTH__METHOD` | `none` / `basic` / `cookie` |
//! | `ZINDER_NODE__AUTH__USERNAME` | Basic-auth username |
//! | `ZINDER_NODE__AUTH__PASSWORD` | Basic-auth password |
//! | `ZINDER_NODE__AUTH__PATH` | Cookie-auth file path |
//! | `ZINDER_NODE__AUTH__COOKIE` | Cookie-auth inline credentials |
//! | `ZINDER_NODE__REQUEST_TIMEOUT_SECS` | [`NodeTarget::request_timeout`] |
//! | `ZINDER_NODE__MAX_RESPONSE_BYTES` | [`NodeTarget::max_response_bytes`] |
//! | `ZINDER_NODE__MAX_CONCURRENT_REQUESTS` | [`NodeTarget::max_concurrent_requests`] |
//!
//! Cookie credentials may be supplied either by file path or inline (but not
//! both). Indexer TLS certificates and private keys may likewise be supplied
//! either by file path or inline as PEM text (but not both for the same
//! credential). Inline credentials let `PaaS`-style deployments inject the
//! secret directly through the environment without writing a shim that
//! materializes secret files.
//!
//! The indexer URL scheme decides whether the channel uses TLS; see
//! [ADR-0007](../../../docs/adrs/0007-node-failure-recovery-and-transport-policy.md).

use std::{
    collections::BTreeMap,
    fmt,
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    time::Duration,
};

use secrecy::{ExposeSecret, SecretString};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, MapAccess, Visitor},
};
use thiserror::Error;
use tracing::warn;
use zinder_core::Network;
use zinder_core::wire::decode_zinder_native_chain_name;

use crate::{CookieSource, DEFAULT_MAX_JSON_RPC_RESPONSE_BYTES, NodeAuth};

/// Default per-RPC node request timeout when the configuration omits one.
pub const DEFAULT_NODE_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Default ceiling on JSON-RPC calls one process has in flight to one node
/// endpoint.
///
/// Zebra's JSON-RPC server accepts 100 connections by default and answers
/// HTTP 429 beyond that. 64 leaves room for a second Zinder process and
/// operator tooling sharing the node, while staying above the forwarded-read
/// slots a reader holds, so reader traffic is not throttled twice.
pub const DEFAULT_NODE_MAX_CONCURRENT_REQUESTS: NonZeroU32 = NonZeroU32::MIN.saturating_add(63);

/// Largest accepted `node.max_concurrent_requests`.
pub const MAX_NODE_MAX_CONCURRENT_REQUESTS: u32 = 1024;

/// Default poll cadence for [`NodeHealthConfig::poll_interval`].
pub const DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS: u64 = 30_000;

/// Default lower bound for [`NodeHealthConfig::verification_progress_floor`].
pub const DEFAULT_NODE_HEALTH_VERIFICATION_PROGRESS_FLOOR: f64 = 0.999;

/// Default upper bound for [`NodeHealthConfig::estimated_gap_floor_blocks`].
pub const DEFAULT_NODE_HEALTH_ESTIMATED_GAP_FLOOR_BLOCKS: u32 = 10;

/// Explicit sentinel string used to disable optional node fields such as
/// `node.indexer_grpc_addr`.
pub const DISABLED_SENTINEL: &str = "none";

/// Placeholder emitted in redacted `Debug` and `Serialize` configuration
/// outputs and rejected as input.
pub const REDACTED_PLACEHOLDER: &str = "[REDACTED]";

/// Secret string configuration input that preserves exact bytes while
/// redacting `Debug` and `Serialize` outputs and rejecting empty values or the
/// literal `[REDACTED]` placeholder on input.
#[derive(Clone)]
pub struct SecretNodeInput(SecretString);

impl SecretNodeInput {
    /// Validates and wraps a raw credential string.
    ///
    /// Rejects empty/whitespace-only inputs and the literal `[REDACTED]`
    /// placeholder so `--print-config` output cannot be accidentally re-fed as
    /// a real secret.
    pub fn new(raw: impl Into<String>) -> Result<Self, &'static str> {
        let raw = raw.into();
        if raw.trim().is_empty() {
            return Err("empty secret value is not allowed");
        }
        if raw.trim() == REDACTED_PLACEHOLDER {
            return Err(
                "literal `[REDACTED]` is not a valid secret; supply the real credential instead of `--print-config` output",
            );
        }
        Ok(Self(SecretString::from(raw)))
    }

    /// Constructs a redacted placeholder instance for `--print-config` rendering.
    #[must_use]
    pub fn redacted_placeholder() -> Self {
        Self(SecretString::from(REDACTED_PLACEHOLDER.to_owned()))
    }

    /// Exposes the underlying credential bytes for explicit resolution paths.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for SecretNodeInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("\"[REDACTED]\"")
    }
}

impl PartialEq for SecretNodeInput {
    fn eq(&self, other: &Self) -> bool {
        self.0.expose_secret() == other.0.expose_secret()
    }
}

impl Eq for SecretNodeInput {}

impl Serialize for SecretNodeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(REDACTED_PLACEHOLDER)
    }
}

impl<'de> Deserialize<'de> for SecretNodeInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::new(raw).map_err(de::Error::custom)
    }
}

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
    /// Most JSON-RPC calls this process has in flight to the node at once.
    ///
    /// Calls beyond it wait for a free slot, for at most their own timeout.
    /// Upstream-health probes use a separate slot, so a saturated budget
    /// never fails readiness.
    pub max_concurrent_requests: NonZeroU32,
    /// Resolved upstream-health probe configuration.
    ///
    /// `None` uses the default JSON-RPC fallback settings.
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
    /// Full URL of Zebra's `/ready` endpoint. Without it, probes use JSON-RPC.
    pub addr: Option<String>,
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
            addr: Some(addr),
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
            max_concurrent_requests: DEFAULT_NODE_MAX_CONCURRENT_REQUESTS,
            health: None,
        }
    }

    /// Sets the per-broadcast timeout.
    #[must_use]
    pub fn with_broadcast_timeout(mut self, broadcast_timeout: Option<Duration>) -> Self {
        self.broadcast_timeout = broadcast_timeout;
        self
    }

    /// Sets the most JSON-RPC calls this process has in flight to the node.
    #[must_use]
    pub const fn with_max_concurrent_requests(
        mut self,
        max_concurrent_requests: NonZeroU32,
    ) -> Self {
        self.max_concurrent_requests = max_concurrent_requests;
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
    /// Live tests call [`NodeTarget::from_environment`].
    pub fn resolve(network: Network, section: NodeSection) -> Result<Self, NodeConfigError> {
        let json_rpc_addr = reject_blank_optional(
            section.json_rpc_addr,
            "node.json_rpc_addr must not be empty",
        )?
        .ok_or(NodeConfigError::MissingField {
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
        let max_concurrent_requests =
            resolve_max_concurrent_requests(section.max_concurrent_requests)?;
        let node_auth = resolve_node_auth(section.auth)?;
        let health = resolve_node_health(section.health)?;
        let indexer_grpc_addr = resolve_optional_disabled_url(
            section.indexer_grpc_addr,
            "node.indexer_grpc_addr must not be empty; omit it or set it to \"none\" to disable",
        )?;
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
        .with_max_concurrent_requests(max_concurrent_requests)
        .with_health(health))
    }

    /// Resolves the `[node_fallbacks.N]` tables into targets ordered by
    /// position, so the ordered node list is `[node]` followed by them.
    ///
    /// Positions must run 1, 2, 3, and so on without a gap, and each table
    /// is validated as [`Self::resolve`] validates `[node]`.
    pub fn resolve_fallbacks(
        network: Network,
        sections: NodeFallbackSections,
    ) -> Result<Vec<Self>, NodeConfigError> {
        let mut targets = Vec::with_capacity(sections.positions.len());
        for (expected_position, (position, section)) in (1_u16..).zip(sections.positions) {
            if position != expected_position {
                return Err(NodeConfigError::FallbackPositionMissing {
                    position: expected_position,
                });
            }
            let target =
                Self::resolve(network, section).map_err(|source| NodeConfigError::Fallback {
                    position,
                    source: Box::new(source),
                })?;
            targets.push(target);
        }
        Ok(targets)
    }

    /// Fails when fallback endpoints are configured without the `[node]`
    /// endpoint, for runtimes whose `[node]` is optional.
    pub fn require_primary_for_fallbacks(
        primary: Option<&Self>,
        fallbacks: &[Self],
    ) -> Result<(), NodeConfigError> {
        if primary.is_none() && !fallbacks.is_empty() {
            return Err(NodeConfigError::FallbacksWithoutPrimary);
        }
        Ok(())
    }

    /// Resolves a [`NodeTarget`] directly from the unified env-var schema.
    ///
    /// Reads `ZINDER_NETWORK` and `ZINDER_NODE__*` from `std::env` without
    /// going through the runtime config loader.
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
                ca_cert_pem: read_optional_secret(
                    "ZINDER_NODE__INDEXER_TLS__CA_CERT_PEM",
                    "node.indexer_tls.ca_cert_pem",
                )?,
                client_cert_path: read_optional("ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PATH")
                    .map(PathBuf::from),
                client_cert_pem: read_optional_secret(
                    "ZINDER_NODE__INDEXER_TLS__CLIENT_CERT_PEM",
                    "node.indexer_tls.client_cert_pem",
                )?,
                client_key_path: read_optional("ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PATH")
                    .map(PathBuf::from),
                client_key_pem: read_optional_secret(
                    "ZINDER_NODE__INDEXER_TLS__CLIENT_KEY_PEM",
                    "node.indexer_tls.client_key_pem",
                )?,
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
            max_concurrent_requests: read_optional_parsed::<u32>(
                "ZINDER_NODE__MAX_CONCURRENT_REQUESTS",
                "node.max_concurrent_requests",
            )?,
            auth: NodeAuthSection {
                method: read_optional("ZINDER_NODE__AUTH__METHOD"),
                username: read_optional("ZINDER_NODE__AUTH__USERNAME"),
                password: read_optional_secret(
                    "ZINDER_NODE__AUTH__PASSWORD",
                    "node.auth.password",
                )?,
                path: read_optional("ZINDER_NODE__AUTH__PATH").map(PathBuf::from),
                cookie: read_optional_secret("ZINDER_NODE__AUTH__COOKIE", "node.auth.cookie")?,
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
/// `[ingest].source` per [ADR-0006](../../../docs/adrs/0006-node-source-boundary.md)
/// because it is a writer-private implementation decision, not a property
/// of the node.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeSection {
    /// Node JSON-RPC base URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json_rpc_addr: Option<String>,
    /// Zebra indexer gRPC endpoint URL.
    ///
    /// When set, enables the streaming mempool source backend and chain-tip
    /// wakeups. Omit or set to `"none"` to fall back to JSON-RPC polling. An
    /// `https://` URL negotiates TLS.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexer_grpc_addr: Option<String>,
    /// Per-RPC request timeout in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_timeout_secs: Option<u64>,
    /// Maximum JSON-RPC response body size accepted from the node.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_response_bytes: Option<u64>,
    /// Per-broadcast timeout in seconds, applied only to `sendrawtransaction`.
    ///
    /// Overrides `request_timeout_secs` for broadcast calls only. Omit to
    /// fall back to `request_timeout_secs`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broadcast_timeout_secs: Option<u64>,
    /// Most JSON-RPC calls this process has in flight to the node at once,
    /// from 1 to 1024. Omit for 64.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_requests: Option<u32>,
    /// TLS subsection for an `https://` indexer endpoint.
    #[serde(skip_serializing_if = "NodeIndexerTlsSection::is_empty")]
    pub indexer_tls: NodeIndexerTlsSection,
    /// Authentication subsection.
    #[serde(skip_serializing_if = "NodeAuthSection::is_empty")]
    pub auth: NodeAuthSection,
    /// Upstream-readiness probe subsection.
    #[serde(skip_serializing_if = "NodeHealthSection::is_empty")]
    pub health: NodeHealthSection,
}

impl NodeSection {
    /// Returns whether every field and subsection of `[node]` is unset.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.json_rpc_addr.is_none()
            && self.indexer_grpc_addr.is_none()
            && self.request_timeout_secs.is_none()
            && self.max_response_bytes.is_none()
            && self.broadcast_timeout_secs.is_none()
            && self.max_concurrent_requests.is_none()
            && self.indexer_tls.is_empty()
            && self.auth.is_empty()
            && self.health.is_empty()
    }

    /// Builds a redacted [`NodeSection`] projection from a resolved [`NodeTarget`].
    #[must_use]
    pub fn from_resolved(target: &NodeTarget) -> Self {
        Self {
            json_rpc_addr: Some(redact_endpoint_url(&target.json_rpc_addr).into_owned()),
            indexer_grpc_addr: target
                .indexer_grpc_addr
                .as_deref()
                .map(|addr| redact_endpoint_url(addr).into_owned()),
            request_timeout_secs: Some(target.request_timeout.as_secs()),
            max_response_bytes: Some(target.max_response_bytes.get()),
            broadcast_timeout_secs: target.broadcast_timeout.map(|duration| duration.as_secs()),
            max_concurrent_requests: Some(target.max_concurrent_requests.get()),
            indexer_tls: target
                .indexer_tls
                .as_ref()
                .map_or_default(NodeIndexerTlsSection::from_resolved),
            auth: NodeAuthSection::from_resolved(&target.node_auth),
            health: target
                .health
                .as_ref()
                .map_or_default(NodeHealthSection::from_resolved),
        }
    }
}

/// Raw `[node_fallbacks.N]` tables: the fallback node endpoints, keyed by
/// their position in the ordered node list whose position 0 is `[node]`.
///
/// Each table holds the fields of [`NodeSection`]. Positions are integer table
/// keys in TOML (`[node_fallbacks.1]`) and in the environment
/// (`ZINDER_NODE_FALLBACKS__1__JSON_RPC_ADDR`), so a file and the environment
/// merge field by field. A TOML array of tables is refused because the
/// configuration loader replaces an array with a table as soon as one
/// environment variable names an entry.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeFallbackSections {
    positions: BTreeMap<u16, NodeSection>,
}

impl NodeFallbackSections {
    /// Builds [`NodeFallbackSections`] from a fixed position map.
    #[must_use]
    pub const fn from_positions(positions: BTreeMap<u16, NodeSection>) -> Self {
        Self { positions }
    }

    /// Builds a redacted [`NodeFallbackSections`] projection from resolved fallback targets.
    #[must_use]
    pub fn from_resolved(targets: &[NodeTarget]) -> Self {
        let positions = (1_u16..)
            .zip(targets)
            .map(|(position, target)| (position, NodeSection::from_resolved(target)))
            .collect();
        Self { positions }
    }

    /// Returns whether no fallback table is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Iterates over configured `(position, section)` fallback entries.
    pub fn iter(&self) -> impl Iterator<Item = (&u16, &NodeSection)> {
        self.positions.iter()
    }

    /// Mutably iterates over configured `(position, section)` fallback entries.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&u16, &mut NodeSection)> {
        self.positions.iter_mut()
    }

    /// Returns whether any fallback configures indexer TLS certificate or key material.
    #[must_use]
    pub fn has_any_indexer_tls_material(&self) -> bool {
        self.positions
            .values()
            .any(|section| section.indexer_tls.has_any_material())
    }
}

impl Serialize for NodeFallbackSections {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_map(
            self.positions
                .iter()
                .map(|(position, section)| (position.to_string(), section)),
        )
    }
}

impl<'de> Deserialize<'de> for NodeFallbackSections {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(NodeFallbackSectionsVisitor)
    }
}

struct NodeFallbackSectionsVisitor;

impl<'de> Visitor<'de> for NodeFallbackSectionsVisitor {
    type Value = NodeFallbackSections;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "tables keyed by node position, such as [node_fallbacks.1]; an array of tables is not accepted",
        )
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut positions = BTreeMap::new();
        while let Some(key) = map.next_key::<String>()? {
            let position = key
                .parse::<u16>()
                .ok()
                .filter(|position| *position > 0)
                .ok_or_else(|| {
                    de::Error::custom(format!(
                        "`{key}`: expected a position key 1, 2, 3, ...; position 0 is [node]"
                    ))
                })?;
            let section = map.next_value::<NodeSection>()?;
            if positions.insert(position, section).is_some() {
                return Err(de::Error::custom(format!(
                    "node position {position} is configured more than once"
                )));
            }
        }
        Ok(NodeFallbackSections { positions })
    }
}

/// Raw `[node.indexer_tls]` config section.
///
/// Each credential accepts either a file path (`*_path`) or inline PEM text
/// (`*_pem`), never both. Empty or whitespace-only fields and `[REDACTED]`
/// placeholders are rejected.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeIndexerTlsSection {
    /// PEM CA bundle file path that verifies the indexer's server certificate.
    /// Omit both this and [`Self::ca_cert_pem`] to use the system roots.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_cert_path: Option<PathBuf>,
    /// Inline PEM CA bundle that verifies the indexer's server certificate.
    /// Mutually exclusive with [`Self::ca_cert_path`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_cert_pem: Option<SecretNodeInput>,
    /// PEM client certificate chain file path presented to the indexer.
    /// Mutually exclusive with [`Self::client_cert_pem`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_cert_path: Option<PathBuf>,
    /// Inline PEM client certificate chain presented to the indexer.
    /// Mutually exclusive with [`Self::client_cert_path`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_cert_pem: Option<SecretNodeInput>,
    /// PEM private key file path for the client certificate.
    /// Mutually exclusive with [`Self::client_key_pem`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key_path: Option<PathBuf>,
    /// Inline PEM private key for the client certificate.
    /// Mutually exclusive with [`Self::client_key_path`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_key_pem: Option<SecretNodeInput>,
    /// Name verified against the indexer's certificate when it differs from
    /// the URL host.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
}

impl NodeIndexerTlsSection {
    /// Returns whether every field in `[node.indexer_tls]` is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.ca_cert_path.is_none()
            && self.ca_cert_pem.is_none()
            && self.client_cert_path.is_none()
            && self.client_cert_pem.is_none()
            && self.client_key_path.is_none()
            && self.client_key_pem.is_none()
            && self.server_name.is_none()
    }

    /// Returns whether any indexer TLS certificate or private-key source is configured.
    #[must_use]
    pub const fn has_any_material(&self) -> bool {
        self.ca_cert_path.is_some()
            || self.ca_cert_pem.is_some()
            || self.client_cert_path.is_some()
            || self.client_cert_pem.is_some()
            || self.client_key_path.is_some()
            || self.client_key_pem.is_some()
    }

    /// Builds a redacted [`NodeIndexerTlsSection`] projection from resolved [`NodeIndexerTls`].
    #[must_use]
    pub fn from_resolved(tls: &NodeIndexerTls) -> Self {
        let (ca_cert_path, ca_cert_pem) = split_tls_material_section(tls.ca_cert.as_ref());
        let (client_cert_path, client_cert_pem) =
            split_tls_material_section(tls.client_identity.as_ref().map(|identity| &identity.cert));
        let (client_key_path, client_key_pem) =
            split_tls_material_section(tls.client_identity.as_ref().map(|identity| &identity.key));
        Self {
            ca_cert_path,
            ca_cert_pem,
            client_cert_path,
            client_cert_pem,
            client_key_path,
            client_key_pem,
            server_name: tls.server_name.clone(),
        }
    }
}

fn split_tls_material_section(
    material: Option<&IndexerTlsMaterial>,
) -> (Option<PathBuf>, Option<SecretNodeInput>) {
    match material {
        Some(IndexerTlsMaterial::File(path)) => (Some(path.clone()), None),
        Some(IndexerTlsMaterial::Inline(_)) => {
            (None, Some(SecretNodeInput::redacted_placeholder()))
        }
        None => (None, None),
    }
}

/// Source of PEM-encoded TLS material for the Zebra indexer gRPC channel.
#[derive(Clone)]
pub enum IndexerTlsMaterial {
    /// Path to a PEM file on disk, re-read on every connection setup.
    File(PathBuf),
    /// Inline PEM-encoded material kept in memory only and redacted in logs and
    /// debug output.
    Inline(SecretString),
}

impl fmt::Debug for IndexerTlsMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => formatter.debug_tuple("File").field(path).finish(),
            Self::Inline(_) => formatter
                .debug_tuple("Inline")
                .field(&"[REDACTED]")
                .finish(),
        }
    }
}

impl PartialEq for IndexerTlsMaterial {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::File(left), Self::File(right)) => left == right,
            (Self::Inline(left), Self::Inline(right)) => {
                left.expose_secret() == right.expose_secret()
            }
            (Self::File(_), Self::Inline(_)) | (Self::Inline(_), Self::File(_)) => false,
        }
    }
}

impl Eq for IndexerTlsMaterial {}

/// Resolved `[node.indexer_tls]` configuration.
///
/// Supports either file paths (re-read on each channel build) or inline PEM
/// secrets kept in memory with redacted `Debug` formatting.
#[non_exhaustive]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeIndexerTls {
    /// PEM CA bundle that verifies the indexer's server certificate. `None`
    /// uses the system roots.
    pub ca_cert: Option<IndexerTlsMaterial>,
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
        self.ca_cert = Some(IndexerTlsMaterial::File(path.into()));
        self
    }

    /// Verifies the indexer's certificate against an inline PEM CA bundle
    /// instead of the system roots.
    #[must_use]
    pub fn with_ca_cert_pem(mut self, pem: impl Into<String>) -> Self {
        self.ca_cert = Some(IndexerTlsMaterial::Inline(SecretString::from(pem.into())));
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
            cert: IndexerTlsMaterial::File(cert_path.into()),
            key: IndexerTlsMaterial::File(key_path.into()),
        });
        self
    }

    /// Presents an inline PEM certificate chain and inline PEM private key.
    #[must_use]
    pub fn with_client_identity_pem(
        mut self,
        cert_pem: impl Into<String>,
        key_pem: impl Into<String>,
    ) -> Self {
        self.client_identity = Some(NodeIndexerClientIdentity {
            cert: IndexerTlsMaterial::Inline(SecretString::from(cert_pem.into())),
            key: IndexerTlsMaterial::Inline(SecretString::from(key_pem.into())),
        });
        self
    }

    /// Presents a client certificate and private key from independent file or
    /// inline sources.
    #[must_use]
    pub fn with_client_identity_sources(
        mut self,
        cert: IndexerTlsMaterial,
        key: IndexerTlsMaterial,
    ) -> Self {
        self.client_identity = Some(NodeIndexerClientIdentity { cert, key });
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

/// Client certificate and key sources for indexer mutual TLS.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeIndexerClientIdentity {
    /// PEM client certificate chain, leaf first.
    pub cert: IndexerTlsMaterial,
    /// PEM private key for the leaf certificate.
    pub key: IndexerTlsMaterial,
}

/// Raw `[node.health]` config section.
///
/// [`NodeTarget::resolve`] applies defaults to any supplied settings.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeHealthSection {
    /// Full URL of Zebra's `/ready` endpoint when the operator exposes it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    /// Probe cadence in milliseconds. Defaults to
    /// [`DEFAULT_NODE_HEALTH_POLL_INTERVAL_MS`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u64>,
    /// Lower bound for `verificationprogress` used by the JSON-RPC
    /// fallback. Defaults to
    /// [`DEFAULT_NODE_HEALTH_VERIFICATION_PROGRESS_FLOOR`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_progress_floor: Option<f64>,
    /// Upper bound for `estimatedheight - blocks` used by the JSON-RPC
    /// fallback. Defaults to
    /// [`DEFAULT_NODE_HEALTH_ESTIMATED_GAP_FLOOR_BLOCKS`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_gap_floor_blocks: Option<u32>,
}

impl NodeHealthSection {
    /// Returns whether every `[node.health]` field is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.addr.is_none()
            && self.poll_interval_ms.is_none()
            && self.verification_progress_floor.is_none()
            && self.estimated_gap_floor_blocks.is_none()
    }

    /// Builds a redacted [`NodeHealthSection`] projection from resolved [`NodeHealthConfig`].
    #[must_use]
    pub fn from_resolved(health: &NodeHealthConfig) -> Self {
        Self {
            addr: health
                .addr
                .as_deref()
                .map(|addr| redact_endpoint_url(addr).into_owned()),
            poll_interval_ms: Some(
                u64::try_from(health.poll_interval.as_millis()).unwrap_or(u64::MAX),
            ),
            verification_progress_floor: Some(health.verification_progress_floor),
            estimated_gap_floor_blocks: Some(health.estimated_gap_floor_blocks),
        }
    }
}

/// Raw `[node.auth]` config section.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeAuthSection {
    /// Auth method: `none`, `basic`, or `cookie`. Defaults to `none`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Basic-auth username.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Basic-auth password.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretNodeInput>,
    /// Cookie-auth file path. Mutually exclusive with [`Self::cookie`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    /// Cookie-auth inline credentials. Used by `PaaS` deployments that inject
    /// the secret as a configuration value. Mutually exclusive with
    /// [`Self::path`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cookie: Option<SecretNodeInput>,
}

impl NodeAuthSection {
    /// Returns whether every field in `[node.auth]` is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.method.is_none()
            && self.username.is_none()
            && self.password.is_none()
            && self.path.is_none()
            && self.cookie.is_none()
    }

    /// Builds a redacted [`NodeAuthSection`] projection from resolved [`NodeAuth`].
    ///
    /// Both cookie sources render as `path = "[REDACTED]"` so operators inspecting
    /// `--print-config` cannot tell file vs inline injection apart.
    #[must_use]
    pub fn from_resolved(auth: &NodeAuth) -> Self {
        match auth {
            NodeAuth::None => Self {
                method: Some("none".to_owned()),
                username: None,
                password: None,
                path: None,
                cookie: None,
            },
            NodeAuth::Basic { username, .. } => Self {
                method: Some("basic".to_owned()),
                username: Some(username.clone()),
                password: Some(SecretNodeInput::redacted_placeholder()),
                path: None,
                cookie: None,
            },
            NodeAuth::Cookie(_) => Self {
                method: Some("cookie".to_owned()),
                username: None,
                password: None,
                path: Some(PathBuf::from(REDACTED_PLACEHOLDER)),
                cookie: None,
            },
        }
    }
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

    /// Only one of the client certificate and client key credentials is set.
    #[error(
        "node.indexer_tls client certificate and client key (path or PEM) must be set together"
    )]
    IndexerTlsClientIdentityIncomplete,

    /// Both `*_path` and `*_pem` were set for the same indexer TLS credential.
    #[error(
        "node.indexer_tls.{field}_path and node.indexer_tls.{field}_pem are mutually exclusive"
    )]
    IndexerTlsPathAndPemConflict {
        /// Credential stem (`ca_cert`, `client_cert`, or `client_key`).
        field: &'static str,
    },

    /// A `[node_fallbacks.N]` table is invalid.
    #[error("[node_fallbacks.{position}] (fields as in [node]): {source}")]
    Fallback {
        /// Position of the invalid table.
        position: u16,
        /// Validation failure of the table.
        #[source]
        source: Box<Self>,
    },

    /// The `[node_fallbacks.N]` positions skip a number.
    #[error(
        "[node_fallbacks.{position}] is missing; fallback positions must run 1, 2, 3, ... without a gap"
    )]
    FallbackPositionMissing {
        /// First position that has no table.
        position: u16,
    },

    /// `[node_fallbacks.N]` is set without the `[node]` endpoint at position 0.
    #[error("[node_fallbacks.N] requires [node] with node.json_rpc_addr, which is position 0")]
    FallbacksWithoutPrimary,

    /// An environment variable could not be parsed as the expected type.
    #[error("failed to parse environment variable for {field}: {reason}")]
    EnvParseFailed {
        /// Configuration field path.
        field: &'static str,
        /// Parse failure reason.
        reason: String,
    },
}

fn resolve_max_concurrent_requests(configured: Option<u32>) -> Result<NonZeroU32, NodeConfigError> {
    let Some(configured) = configured else {
        return Ok(DEFAULT_NODE_MAX_CONCURRENT_REQUESTS);
    };
    NonZeroU32::new(configured)
        .filter(|limit| limit.get() <= MAX_NODE_MAX_CONCURRENT_REQUESTS)
        .ok_or(NodeConfigError::Invalid {
            reason: "node.max_concurrent_requests must be from 1 to 1024",
        })
}

fn resolve_node_health(
    mut section: NodeHealthSection,
) -> Result<Option<NodeHealthConfig>, NodeConfigError> {
    section.addr = resolve_optional_disabled_url(
        section.addr,
        "node.health.addr must not be empty; omit it or set it to \"none\" to disable",
    )?;
    if section == NodeHealthSection::default() {
        return Ok(None);
    }
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

    Ok(Some(NodeHealthConfig {
        addr: section.addr,
        poll_interval: Duration::from_millis(poll_interval_ms),
        verification_progress_floor,
        estimated_gap_floor_blocks,
    }))
}

fn resolve_tls_material(
    path: Option<PathBuf>,
    pem: Option<SecretNodeInput>,
    field: &'static str,
) -> Result<Option<IndexerTlsMaterial>, NodeConfigError> {
    let checked_path = reject_blank_or_redacted_path(
        path,
        "node.indexer_tls path must not be empty or [REDACTED]",
    )?;
    let checked_pem = match pem {
        Some(secret_pem) => Some(SecretNodeInput::new(secret_pem.expose_secret()).map_err(
            |_| NodeConfigError::Invalid {
                reason: "node.indexer_tls PEM must not be empty or [REDACTED]",
            },
        )?),
        None => None,
    };
    match (checked_path, checked_pem) {
        (Some(_), Some(_)) => Err(NodeConfigError::IndexerTlsPathAndPemConflict { field }),
        (Some(file_path), None) => Ok(Some(IndexerTlsMaterial::File(file_path))),
        (None, Some(inline_pem)) => Ok(Some(IndexerTlsMaterial::Inline(SecretString::from(
            inline_pem.expose_secret().to_owned(),
        )))),
        (None, None) => Ok(None),
    }
}

fn resolve_node_indexer_tls(
    indexer_grpc_addr: Option<&str>,
    section: NodeIndexerTlsSection,
) -> Result<Option<NodeIndexerTls>, NodeConfigError> {
    let ca_cert = resolve_tls_material(section.ca_cert_path, section.ca_cert_pem, "ca_cert")?;
    let client_cert = resolve_tls_material(
        section.client_cert_path,
        section.client_cert_pem,
        "client_cert",
    )?;
    let client_key = resolve_tls_material(
        section.client_key_path,
        section.client_key_pem,
        "client_key",
    )?;
    let server_name = reject_blank_optional(
        section.server_name,
        "node.indexer_tls.server_name must not be empty",
    )?;
    if ca_cert.is_none() && client_cert.is_none() && client_key.is_none() && server_name.is_none() {
        return Ok(None);
    }

    let indexer_grpc_addr =
        indexer_grpc_addr.ok_or(NodeConfigError::IndexerTlsWithoutIndexerAddr)?;
    if !uses_tls_scheme(indexer_grpc_addr) {
        return Err(NodeConfigError::IndexerTlsRequiresHttps);
    }
    let client_identity = match (client_cert, client_key) {
        (Some(cert), Some(key)) => Some(NodeIndexerClientIdentity { cert, key }),
        (None, None) => None,
        (Some(_), None) | (None, Some(_)) => {
            return Err(NodeConfigError::IndexerTlsClientIdentityIncomplete);
        }
    };

    Ok(Some(NodeIndexerTls {
        ca_cert,
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

/// Returns `url` for configuration output and logs, with its user
/// information and its query and fragment each replaced by `[REDACTED]`.
///
/// An endpoint URL can carry a credential in either place. The scheme, host,
/// port, and path stay visible, so an operator can still tell endpoints
/// apart. A URL without user information, query, or fragment is returned
/// unchanged.
#[must_use]
pub fn redact_endpoint_url(url: &str) -> std::borrow::Cow<'_, str> {
    const REDACTED: &str = "[REDACTED]";
    let (scheme, rest) = match url.split_once("://") {
        Some((scheme, rest)) => (Some(scheme), rest),
        None => (None, url),
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, after_authority) = rest.split_at(authority_end);
    let host = authority.rsplit_once('@').map(|(_userinfo, host)| host);
    let path_end = after_authority
        .find(['?', '#'])
        .unwrap_or(after_authority.len());
    let (path, query_and_fragment) = after_authority.split_at(path_end);
    if host.is_none() && query_and_fragment.is_empty() {
        return std::borrow::Cow::Borrowed(url);
    }
    let mut redacted = String::with_capacity(url.len());
    if let Some(scheme) = scheme {
        redacted.push_str(scheme);
        redacted.push_str("://");
    }
    match host {
        Some(host) => {
            redacted.push_str(REDACTED);
            redacted.push('@');
            redacted.push_str(host);
        }
        None => redacted.push_str(authority),
    }
    redacted.push_str(path);
    if let Some(delimiter) = query_and_fragment.chars().next() {
        redacted.push(delimiter);
        redacted.push_str(REDACTED);
    }
    std::borrow::Cow::Owned(redacted)
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
            let username =
                reject_blank_optional(section.username, "node.auth.username must not be empty")?
                    .ok_or(NodeConfigError::MissingField {
                        field: "node.auth.username",
                    })?;
            let password = section.password.ok_or(NodeConfigError::MissingField {
                field: "node.auth.password",
            })?;
            let validated_password =
                SecretNodeInput::new(password.expose_secret()).map_err(|_| {
                    NodeConfigError::Invalid {
                        reason: "node.auth.password must not be empty or [REDACTED]",
                    }
                })?;
            Ok(NodeAuth::basic(
                username,
                validated_password.expose_secret().to_owned(),
            ))
        }
        "cookie" => {
            reject_present(section.username.is_some(), "node.auth.username", "cookie")?;
            reject_present(section.password.is_some(), "node.auth.password", "cookie")?;
            let cookie_path = reject_blank_or_redacted_path(
                section.path,
                "node.auth.path must not be empty or [REDACTED]",
            )?;
            let source =
                match (cookie_path, section.cookie) {
                    (Some(_), Some(_)) => {
                        return Err(NodeConfigError::Invalid {
                            reason: "node.auth.path and node.auth.cookie are mutually exclusive",
                        });
                    }
                    (Some(path), None) => CookieSource::File(path),
                    (None, Some(content)) => {
                        let validated_cookie = SecretNodeInput::new(content.expose_secret())
                            .map_err(|_| NodeConfigError::Invalid {
                                reason: "node.auth.cookie must not be empty or [REDACTED]",
                            })?;
                        CookieSource::Inline(SecretString::from(
                            validated_cookie.expose_secret().to_owned(),
                        ))
                    }
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

fn read_optional_secret(
    env_var: &'static str,
    field: &'static str,
) -> Result<Option<SecretNodeInput>, NodeConfigError> {
    let Some(raw) = read_optional(env_var) else {
        return Ok(None);
    };
    SecretNodeInput::new(raw)
        .map(Some)
        .map_err(|reason| NodeConfigError::EnvParseFailed {
            field,
            reason: reason.to_owned(),
        })
}

fn reject_blank_optional(
    text: Option<String>,
    reason: &'static str,
) -> Result<Option<String>, NodeConfigError> {
    match text {
        Some(text) if text.trim().is_empty() || text.contains(REDACTED_PLACEHOLDER) => {
            Err(NodeConfigError::Invalid { reason })
        }
        other => Ok(other),
    }
}

fn resolve_optional_disabled_url(
    text: Option<String>,
    reason: &'static str,
) -> Result<Option<String>, NodeConfigError> {
    match text {
        None => Ok(None),
        Some(text) if text.trim().is_empty() || text.contains(REDACTED_PLACEHOLDER) => {
            Err(NodeConfigError::Invalid { reason })
        }
        Some(text) if text.trim() == DISABLED_SENTINEL => Ok(None),
        Some(text) => Ok(Some(text)),
    }
}

fn reject_blank_or_redacted_path(
    path: Option<PathBuf>,
    reason: &'static str,
) -> Result<Option<PathBuf>, NodeConfigError> {
    match path {
        Some(path) => {
            let trimmed = path.as_os_str().to_string_lossy();
            if trimmed.trim().is_empty() || trimmed.trim() == REDACTED_PLACEHOLDER {
                return Err(NodeConfigError::Invalid { reason });
            }
            Ok(Some(path))
        }
        None => Ok(None),
    }
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
    fn redacted_endpoint_urls_keep_host_and_path_but_not_credentials() {
        for (url, expected) in [
            ("http://127.0.0.1:8232", "http://127.0.0.1:8232"),
            ("http://zebra:8233/ready", "http://zebra:8233/ready"),
            (
                "http://zebra:rpc-secret@127.0.0.1:8232",
                "http://[REDACTED]@127.0.0.1:8232",
            ),
            (
                "https://token-secret@zebra.example/v1/",
                "https://[REDACTED]@zebra.example/v1/",
            ),
            (
                "http://zebra:8233/ready?token=query-secret",
                "http://zebra:8233/ready?[REDACTED]",
            ),
            (
                "http://user:secret@zebra:8233?token=secret#fragment-secret",
                "http://[REDACTED]@zebra:8233?[REDACTED]",
            ),
            (
                "http://zebra:8233/#fragment-secret",
                "http://zebra:8233/#[REDACTED]",
            ),
            ("user:secret@zebra:8232", "[REDACTED]@zebra:8232"),
        ] {
            assert_eq!(redact_endpoint_url(url), expected, "{url}");
        }
    }

    fn test_secret(text: &str) -> Result<SecretNodeInput, NodeConfigError> {
        SecretNodeInput::new(text).map_err(|reason| NodeConfigError::EnvParseFailed {
            field: "test_secret",
            reason: reason.to_owned(),
        })
    }

    #[test]
    fn resolve_basic_auth_round_trip() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: Some("http://127.0.0.1:8155".to_owned()),
            request_timeout_secs: Some(15),
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            max_concurrent_requests: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("basic".to_owned()),
                username: Some("zebra".to_owned()),
                password: Some(test_secret("zebra")?),
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
            max_concurrent_requests: None,
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
            max_concurrent_requests: None,
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
    fn resolve_accepts_none_sentinel_and_rejects_blank_indexer_grpc_addr()
    -> Result<(), NodeConfigError> {
        let disabled = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: Some("none".to_owned()),
            ..NodeSection::default()
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, disabled)?;
        assert!(target.indexer_grpc_addr.is_none());

        for blank in ["", "   "] {
            let invalid = NodeSection {
                json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
                indexer_grpc_addr: Some(blank.to_owned()),
                ..NodeSection::default()
            };
            assert!(matches!(
                NodeTarget::resolve(Network::ZcashRegtest, invalid),
                Err(NodeConfigError::Invalid { .. })
            ));
        }
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
            ..NodeIndexerTlsSection::default()
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
    fn indexer_tls_section_resolves_inline_pems_and_mixed_sources() -> Result<(), NodeConfigError> {
        let section = NodeIndexerTlsSection {
            ca_cert_pem: Some(test_secret(
                "-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n",
            )?),
            client_cert_path: Some(PathBuf::from("/run/secrets/indexer-client.pem")),
            client_key_pem: Some(test_secret(
                "-----BEGIN PRIVATE KEY-----\nclient-key\n-----END PRIVATE KEY-----\n",
            )?),
            server_name: Some("zakura.internal".to_owned()),
            ..NodeIndexerTlsSection::default()
        };
        let target = NodeTarget::resolve(
            Network::ZcashRegtest,
            indexer_tls_section(Some("https://zakura:8230"), section),
        )?;

        let expected = NodeIndexerTls::new()
            .with_ca_cert_pem("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n")
            .with_client_identity_sources(
                IndexerTlsMaterial::File(PathBuf::from("/run/secrets/indexer-client.pem")),
                IndexerTlsMaterial::Inline(SecretString::from(
                    "-----BEGIN PRIVATE KEY-----\nclient-key\n-----END PRIVATE KEY-----\n",
                )),
            )
            .with_server_name("zakura.internal");
        assert_eq!(target.indexer_tls, Some(expected));
        Ok(())
    }

    #[test]
    fn indexer_tls_rejects_simultaneous_path_and_pem_for_same_credential()
    -> Result<(), NodeConfigError> {
        for (conflict, expected_field) in [
            (
                NodeIndexerTlsSection {
                    ca_cert_path: Some(PathBuf::from("/ca.pem")),
                    ca_cert_pem: Some(test_secret("-----BEGIN CERTIFICATE-----\nca\n")?),
                    ..NodeIndexerTlsSection::default()
                },
                "ca_cert",
            ),
            (
                NodeIndexerTlsSection {
                    client_cert_path: Some(PathBuf::from("/client.pem")),
                    client_cert_pem: Some(test_secret("-----BEGIN CERTIFICATE-----\nclient\n")?),
                    client_key_pem: Some(test_secret("-----BEGIN PRIVATE KEY-----\nkey\n")?),
                    ..NodeIndexerTlsSection::default()
                },
                "client_cert",
            ),
            (
                NodeIndexerTlsSection {
                    client_cert_pem: Some(test_secret("-----BEGIN CERTIFICATE-----\nclient\n")?),
                    client_key_path: Some(PathBuf::from("/client.key")),
                    client_key_pem: Some(test_secret("-----BEGIN PRIVATE KEY-----\nkey\n")?),
                    ..NodeIndexerTlsSection::default()
                },
                "client_key",
            ),
        ] {
            let outcome = NodeTarget::resolve(
                Network::ZcashRegtest,
                indexer_tls_section(Some("https://zakura:8230"), conflict),
            );
            assert!(matches!(
                outcome,
                Err(NodeConfigError::IndexerTlsPathAndPemConflict { field })
                    if field == expected_field
            ));
        }
        Ok(())
    }

    #[test]
    fn debug_formatting_redacts_inline_pems_and_node_secrets() -> Result<(), NodeConfigError> {
        let tls_section = NodeIndexerTlsSection {
            ca_cert_pem: Some(test_secret("ca-secret-pem")?),
            client_cert_pem: Some(test_secret("client-cert-secret")?),
            client_key_pem: Some(test_secret("client-key-secret")?),
            server_name: Some("zakura.internal".to_owned()),
            ..NodeIndexerTlsSection::default()
        };
        let auth_section = NodeAuthSection {
            method: Some("basic".to_owned()),
            username: Some("zebra".to_owned()),
            password: Some(test_secret("password-secret")?),
            path: None,
            cookie: Some(test_secret("cookie-secret")?),
        };
        let section_debug = format!("{tls_section:?} {auth_section:?}");
        for secret in [
            "ca-secret-pem",
            "client-cert-secret",
            "client-key-secret",
            "password-secret",
            "cookie-secret",
        ] {
            assert!(!section_debug.contains(secret), "{section_debug}");
        }
        let target = NodeTarget::resolve(
            Network::ZcashRegtest,
            indexer_tls_section(
                Some("https://zakura:8230"),
                NodeIndexerTlsSection {
                    ca_cert_pem: Some(test_secret("ca-secret-pem")?),
                    client_cert_pem: Some(test_secret("client-cert-secret")?),
                    client_key_pem: Some(test_secret("client-key-secret")?),
                    ..NodeIndexerTlsSection::default()
                },
            ),
        )?;
        let target_debug = format!("{target:?}");
        for secret in ["ca-secret-pem", "client-cert-secret", "client-key-secret"] {
            assert!(!target_debug.contains(secret), "{target_debug}");
        }
        assert!(target_debug.contains("[REDACTED]"), "{target_debug}");
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
        for indexer_grpc_addr in [None, Some("none")] {
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
            client_cert_path: None,
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
    fn blank_indexer_tls_fields_are_rejected() {
        for section in [
            NodeIndexerTlsSection {
                ca_cert_path: Some(PathBuf::new()),
                ..NodeIndexerTlsSection::default()
            },
            NodeIndexerTlsSection {
                client_cert_path: Some(PathBuf::from("   ")),
                ..NodeIndexerTlsSection::default()
            },
            NodeIndexerTlsSection {
                server_name: Some(String::new()),
                ..NodeIndexerTlsSection::default()
            },
        ] {
            assert!(matches!(
                NodeTarget::resolve(
                    Network::ZcashRegtest,
                    indexer_tls_section(Some("https://zakura:8230"), section),
                ),
                Err(NodeConfigError::Invalid { .. })
            ));
        }
        assert!(SecretNodeInput::new("   ").is_err());
        assert!(SecretNodeInput::new("[REDACTED]").is_err());
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
    fn resolve_rejects_basic_auth_with_path() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            max_concurrent_requests: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("basic".to_owned()),
                username: Some("zebra".to_owned()),
                password: Some(test_secret("zebra")?),
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
        Ok(())
    }

    #[test]
    fn resolve_cookie_auth_from_path() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            max_concurrent_requests: None,
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
            max_concurrent_requests: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("cookie".to_owned()),
                username: None,
                password: None,
                path: None,
                cookie: Some(test_secret("user:cookie-secret")?),
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
    fn resolve_rejects_cookie_auth_with_both_path_and_inline() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            max_concurrent_requests: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection {
                method: Some("cookie".to_owned()),
                username: None,
                password: None,
                path: Some(PathBuf::from("/var/run/auth/.cookie")),
                cookie: Some(test_secret("user:cookie-secret")?),
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
        Ok(())
    }

    #[test]
    fn resolve_rejects_cookie_auth_without_path_or_inline() {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            max_concurrent_requests: None,
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
            max_concurrent_requests: None,
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
            max_concurrent_requests: None,
            indexer_tls: NodeIndexerTlsSection::default(),
            auth: NodeAuthSection::default(),
            health: NodeHealthSection::default(),
        };
        let target = NodeTarget::resolve(Network::ZcashRegtest, section)?;
        assert!(target.health.is_none());
        Ok(())
    }

    fn section_with_max_concurrent_requests(max_concurrent_requests: Option<u32>) -> NodeSection {
        NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            max_concurrent_requests,
            ..NodeSection::default()
        }
    }

    #[test]
    fn max_concurrent_requests_defaults_below_the_node_connection_limit()
    -> Result<(), NodeConfigError> {
        let target = NodeTarget::resolve(
            Network::ZcashRegtest,
            section_with_max_concurrent_requests(None),
        )?;
        assert_eq!(
            target.max_concurrent_requests,
            DEFAULT_NODE_MAX_CONCURRENT_REQUESTS
        );
        assert_eq!(target.max_concurrent_requests.get(), 64);
        Ok(())
    }

    #[test]
    fn max_concurrent_requests_accepts_one_through_the_maximum() -> Result<(), NodeConfigError> {
        for configured in [1, 8, MAX_NODE_MAX_CONCURRENT_REQUESTS] {
            let target = NodeTarget::resolve(
                Network::ZcashRegtest,
                section_with_max_concurrent_requests(Some(configured)),
            )?;
            assert_eq!(target.max_concurrent_requests.get(), configured);
        }
        Ok(())
    }

    #[test]
    fn max_concurrent_requests_rejects_zero_and_values_above_the_maximum() {
        for configured in [0, MAX_NODE_MAX_CONCURRENT_REQUESTS + 1] {
            let outcome = NodeTarget::resolve(
                Network::ZcashRegtest,
                section_with_max_concurrent_requests(Some(configured)),
            );
            assert!(
                matches!(
                    outcome,
                    Err(NodeConfigError::Invalid { reason }) if reason.contains("max_concurrent_requests")
                ),
                "{configured} must be rejected"
            );
        }
    }

    #[test]
    fn resolve_health_applies_defaults_when_addr_set() -> Result<(), NodeConfigError> {
        let section = NodeSection {
            json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
            indexer_grpc_addr: None,
            request_timeout_secs: None,
            max_response_bytes: None,
            broadcast_timeout_secs: None,
            max_concurrent_requests: None,
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
        assert_eq!(health.addr.as_deref(), Some("http://127.0.0.1:18233/ready"));
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
            max_concurrent_requests: None,
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
        for addr in [None, Some("http://127.0.0.1:18233/ready")] {
            for floor in [0.0, 1.0, -0.5, 1.5] {
                let section = NodeSection {
                    json_rpc_addr: Some("http://127.0.0.1:8232".to_owned()),
                    health: NodeHealthSection {
                        addr: addr.map(str::to_owned),
                        verification_progress_floor: Some(floor),
                        ..Default::default()
                    },
                    ..Default::default()
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

    fn fallback_sections(
        tables: serde_json::Value,
    ) -> Result<NodeFallbackSections, serde_json::Error> {
        serde_json::from_value(tables)
    }

    #[test]
    fn fallback_tables_resolve_in_position_order() -> Result<(), Box<dyn std::error::Error>> {
        let sections = fallback_sections(serde_json::json!({
            "2": { "json_rpc_addr": "http://zakura:8232" },
            "1": {
                "json_rpc_addr": "http://zebra-b:8232",
                "auth": { "method": "basic", "username": "zebra", "password": "secret" },
            },
        }))?;
        let targets = NodeTarget::resolve_fallbacks(Network::ZcashRegtest, sections)?;

        let addresses = targets
            .iter()
            .map(|target| target.json_rpc_addr.as_str())
            .collect::<Vec<_>>();
        assert_eq!(addresses, ["http://zebra-b:8232", "http://zakura:8232"]);
        assert_eq!(targets[0].node_auth.scheme_name(), "basic");
        Ok(())
    }

    #[test]
    fn fallback_positions_sort_numerically_past_nine() -> Result<(), Box<dyn std::error::Error>> {
        let tables = (1..=10)
            .map(|position| {
                (
                    position.to_string(),
                    serde_json::json!({ "json_rpc_addr": format!("http://node-{position}:8232") }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let targets = NodeTarget::resolve_fallbacks(
            Network::ZcashRegtest,
            fallback_sections(serde_json::Value::Object(tables))?,
        )?;

        assert_eq!(targets.len(), 10);
        assert_eq!(targets[1].json_rpc_addr, "http://node-2:8232");
        assert_eq!(targets[9].json_rpc_addr, "http://node-10:8232");
        Ok(())
    }

    #[test]
    fn fallback_tables_refuse_an_array() {
        let refusal = fallback_sections(serde_json::json!([
            { "json_rpc_addr": "http://zebra-b:8232" },
        ]))
        .err()
        .map(|error| error.to_string());

        assert!(
            refusal.as_deref().is_some_and(
                |message| message.contains("[node_fallbacks.1]") && message.contains("array")
            ),
            "{refusal:?}"
        );
    }

    #[test]
    fn fallback_tables_refuse_keys_that_are_not_positions() {
        for key in ["0", "first", "-1", "contract_probe"] {
            let refusal = fallback_sections(serde_json::json!({
                key: { "json_rpc_addr": "http://zebra-b:8232" },
            }))
            .err()
            .map(|error| error.to_string());

            assert!(
                refusal
                    .as_deref()
                    .is_some_and(|message| message.contains("expected a position key")),
                "{key}: {refusal:?}"
            );
        }
    }

    #[test]
    fn fallback_positions_must_not_skip_a_number() -> Result<(), Box<dyn std::error::Error>> {
        let sections = fallback_sections(serde_json::json!({
            "1": { "json_rpc_addr": "http://zebra-b:8232" },
            "3": { "json_rpc_addr": "http://zakura:8232" },
        }))?;

        assert!(matches!(
            NodeTarget::resolve_fallbacks(Network::ZcashRegtest, sections),
            Err(NodeConfigError::FallbackPositionMissing { position: 2 })
        ));
        Ok(())
    }

    #[test]
    fn fallback_validation_errors_name_their_position() -> Result<(), Box<dyn std::error::Error>> {
        let sections = fallback_sections(serde_json::json!({
            "1": { "json_rpc_addr": "http://zebra-b:8232" },
            "2": { "auth": { "method": "basic" } },
        }))?;

        let message = NodeTarget::resolve_fallbacks(Network::ZcashRegtest, sections)
            .err()
            .map(|error| error.to_string());
        assert!(
            message
                .as_deref()
                .is_some_and(|message| message.starts_with("[node_fallbacks.2]")
                    && message.contains("node.json_rpc_addr")),
            "{message:?}"
        );
        Ok(())
    }

    #[test]
    fn fallbacks_require_the_primary_endpoint() -> Result<(), NodeConfigError> {
        let fallback = NodeTarget::resolve(
            Network::ZcashRegtest,
            NodeSection {
                json_rpc_addr: Some("http://zebra-b:8232".to_owned()),
                ..NodeSection::default()
            },
        )?;

        assert!(matches!(
            NodeTarget::require_primary_for_fallbacks(None, std::slice::from_ref(&fallback)),
            Err(NodeConfigError::FallbacksWithoutPrimary)
        ));
        NodeTarget::require_primary_for_fallbacks(None, &[])?;
        NodeTarget::require_primary_for_fallbacks(Some(&fallback), std::slice::from_ref(&fallback))
    }
}
