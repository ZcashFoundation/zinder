//! Shared `[ingest_control]` config section.
//!
//! The writer reads `listen_addr` to bind the private control-plane
//! endpoint; the readers read `addr` to connect; both sides read
//! `bearer_token_path` when ADR-0011 auth is enforced. Sharing one
//! section means operators configure the secret in one place and
//! ingest-control discovery works the same way regardless of which
//! binary is reading it.

use std::{
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    BearerToken,
    config::{ConfigError, parse_socket_addr, resolve_optional_bearer_token},
    sections::defaults::{
        DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT, DEFAULT_INGEST_CONTROL_LISTEN_ADDR,
        DEFAULT_INGEST_CONTROL_READER_URL,
    },
};

/// Raw `[ingest_control]` config section.
///
/// The writer reads [`Self::listen_addr`]; the readers read
/// [`Self::addr`]; both read [`Self::bearer_token_path`] or
/// [`Self::bearer_token`]. Fields the other side does not consume are
/// ignored, so the same operator-supplied section serves both planes.
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestControlSection {
    /// Writer-side bind address for the `IngestControl` gRPC endpoint.
    /// Empty string disables the endpoint (used by one-shot bulk-catchup runs when an
    /// operator wants a one-shot bootstrap with no live readers).
    pub listen_addr: Option<String>,
    /// Reader-side URL the colocated readers dial. Defaults to
    /// `http://127.0.0.1:9100`.
    pub addr: Option<String>,
    /// Shared-secret bearer token path enforced on every `IngestControl`
    /// request when auth is enabled (ADR-0011). The writer reads this
    /// file to verify; the readers read the same file to present.
    /// Mutually exclusive with [`Self::bearer_token`].
    pub bearer_token_path: Option<PathBuf>,
    /// Inline shared-secret bearer token for `IngestControl` requests.
    /// Mutually exclusive with [`Self::bearer_token_path`].
    pub bearer_token: Option<String>,
    /// File containing the capability token required only for canonical owner
    /// checkpoint creation. Reader-only compat processes must not receive it.
    /// Mutually exclusive with [`Self::checkpoint_bearer_token`].
    pub checkpoint_bearer_token_path: Option<PathBuf>,
    /// Inline capability token required only for canonical owner checkpoint
    /// creation. Mutually exclusive with [`Self::checkpoint_bearer_token_path`].
    pub checkpoint_bearer_token: Option<String>,
    /// Writer-owned directory below which canonical checkpoint candidates are
    /// staged. The checkpoint control RPC accepts opaque identifiers only and
    /// never accepts an arbitrary filesystem path.
    pub checkpoint_staging_root: Option<PathBuf>,
}

impl fmt::Debug for IngestControlSection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IngestControlSection")
            .field("listen_addr", &self.listen_addr)
            .field("addr", &self.addr)
            .field("bearer_token_path", &self.bearer_token_path)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "checkpoint_bearer_token_path",
                &self.checkpoint_bearer_token_path,
            )
            .field(
                "checkpoint_bearer_token",
                &self.checkpoint_bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("checkpoint_staging_root", &self.checkpoint_staging_root)
            .finish()
    }
}

/// Resolved writer-side ingest-control configuration.
#[derive(Clone, Debug)]
pub struct ResolvedIngestControlWriter {
    /// Listen address. `None` when the operator opted out via empty
    /// string.
    pub listen_addr: Option<SocketAddr>,
    /// Bearer token file path, when ADR-0011 auth is enabled.
    pub bearer_token_path: Option<PathBuf>,
    /// Loaded bearer token, when [`Self::bearer_token_path`] is set.
    pub bearer_token: Option<BearerToken>,
    /// Loaded method-level checkpoint capability token.
    pub checkpoint_bearer_token: Option<BearerToken>,
    /// Checkpoint capability-token file path, when configured.
    pub checkpoint_bearer_token_path: Option<PathBuf>,
    /// Root directory below which owner checkpoint candidates are created.
    pub checkpoint_staging_root: PathBuf,
}

/// Resolved reader-side ingest-control configuration.
#[derive(Clone, Debug)]
pub struct ResolvedIngestControlReader {
    /// URL the reader dials. Validated as a [`tonic::transport::Endpoint`].
    pub addr: String,
    /// Bearer token file path, when ADR-0011 auth is enabled.
    pub bearer_token_path: Option<PathBuf>,
    /// Loaded bearer token, when [`Self::bearer_token_path`] is set.
    pub bearer_token: Option<BearerToken>,
}

/// Validates and resolves an [`IngestControlSection`] for the writer
/// (`zinder-ingest`).
///
/// An unset [`IngestControlSection::listen_addr`] falls back to
/// [`DEFAULT_INGEST_CONTROL_LISTEN_ADDR`]. An empty string resolves to
/// `Ok(ResolvedIngestControlWriter { listen_addr: None, .. })` so
/// callers that allow disabling the endpoint (such as one-shot bulk-catchup runs) can
/// pattern-match without re-parsing.
pub fn resolve_ingest_control_writer(
    section: IngestControlSection,
) -> Result<ResolvedIngestControlWriter, ConfigError> {
    let listen_addr_text = section
        .listen_addr
        .unwrap_or_else(|| DEFAULT_INGEST_CONTROL_LISTEN_ADDR.to_owned());
    let listen_addr = if listen_addr_text.trim().is_empty() {
        None
    } else {
        Some(parse_socket_addr(
            "ingest_control.listen_addr",
            &listen_addr_text,
        )?)
    };
    let bearer_token = resolve_optional_bearer_token(
        section.bearer_token_path.as_deref(),
        section.bearer_token.as_deref(),
        "ingest_control.bearer_token_path",
        "ingest_control.bearer_token",
    )?;
    let checkpoint_bearer_token = resolve_optional_bearer_token(
        section.checkpoint_bearer_token_path.as_deref(),
        section.checkpoint_bearer_token.as_deref(),
        "ingest_control.checkpoint_bearer_token_path",
        "ingest_control.checkpoint_bearer_token",
    )?;
    Ok(ResolvedIngestControlWriter {
        listen_addr,
        bearer_token_path: section.bearer_token_path,
        bearer_token,
        checkpoint_bearer_token,
        checkpoint_bearer_token_path: section.checkpoint_bearer_token_path,
        checkpoint_staging_root: section
            .checkpoint_staging_root
            .unwrap_or_else(|| PathBuf::from(DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT)),
    })
}

/// Validates and resolves an [`IngestControlSection`] for a reader
/// (`zinder-projector`, `zinder-query`).
///
/// An unset [`IngestControlSection::addr`] falls back to
/// [`DEFAULT_INGEST_CONTROL_READER_URL`]. The URL is validated as a
/// [`tonic::transport::Endpoint`] so a malformed value fails at config
/// load rather than at first connect.
pub fn resolve_ingest_control_reader(
    section: IngestControlSection,
) -> Result<ResolvedIngestControlReader, ConfigError> {
    let addr = section
        .addr
        .unwrap_or_else(|| DEFAULT_INGEST_CONTROL_READER_URL.to_owned());
    crate::transport::validate_zinder_grpc_endpoint(&addr).map_err(|source| {
        ConfigError::invalid(format!(
            "ingest_control.addr {addr} is not a valid endpoint: {source}"
        ))
    })?;
    let bearer_token = resolve_optional_bearer_token(
        section.bearer_token_path.as_deref(),
        section.bearer_token.as_deref(),
        "ingest_control.bearer_token_path",
        "ingest_control.bearer_token",
    )?;
    Ok(ResolvedIngestControlReader {
        addr,
        bearer_token_path: section.bearer_token_path,
        bearer_token,
    })
}

/// Redacted TOML projection of the writer-side `[ingest_control]` section.
#[derive(Debug, Serialize)]
pub struct IngestControlWriterToml {
    /// Resolved listen address as a string; empty when the writer opted
    /// out via `listen_addr = ""`.
    pub listen_addr: String,
    /// Bearer-token file path. The token value is never echoed; only the
    /// path the operator supplied is rendered so operators can verify
    /// which file is in use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_path: Option<String>,
    /// Redacted placeholder when an inline bearer token is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<&'static str>,
    /// Checkpoint capability-token path, never token material.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_bearer_token_path: Option<String>,
    /// Redacted placeholder when an inline checkpoint capability token is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_bearer_token: Option<&'static str>,
    /// Staging root used for canonical owner checkpoint candidates.
    pub checkpoint_staging_root: String,
}

impl IngestControlWriterToml {
    /// Builds a writer-side TOML projection from a resolved writer config.
    #[must_use]
    pub fn from_writer(resolved: &ResolvedIngestControlWriter) -> Self {
        Self {
            listen_addr: resolved.listen_addr.map_or_default(|addr| addr.to_string()),
            bearer_token_path: resolved
                .bearer_token_path
                .as_ref()
                .map(|path| path.display().to_string()),
            bearer_token: (resolved.bearer_token.is_some() && resolved.bearer_token_path.is_none())
                .then_some("[REDACTED]"),
            checkpoint_bearer_token_path: resolved
                .checkpoint_bearer_token_path
                .as_ref()
                .map(|path| path.display().to_string()),
            checkpoint_bearer_token: (resolved.checkpoint_bearer_token.is_some()
                && resolved.checkpoint_bearer_token_path.is_none())
            .then_some("[REDACTED]"),
            checkpoint_staging_root: resolved.checkpoint_staging_root.display().to_string(),
        }
    }

    /// Builds a writer-side TOML projection from a resolved listen
    /// address and the optional bearer-token paths.
    #[must_use]
    pub fn from_resolved(
        listen_addr: Option<SocketAddr>,
        bearer_token_path: Option<&Path>,
        checkpoint_bearer_token_path: Option<&Path>,
        checkpoint_staging_root: &Path,
    ) -> Self {
        Self {
            listen_addr: listen_addr.map_or_default(|addr| addr.to_string()),
            bearer_token_path: bearer_token_path.map(|path| path.display().to_string()),
            bearer_token: None,
            checkpoint_bearer_token_path: checkpoint_bearer_token_path
                .map(|path| path.display().to_string()),
            checkpoint_bearer_token: None,
            checkpoint_staging_root: checkpoint_staging_root.display().to_string(),
        }
    }
}

/// Redacted TOML projection of the reader-side `[ingest_control]` section.
#[derive(Debug, Serialize)]
pub struct IngestControlReaderToml {
    /// Resolved reader endpoint URL.
    pub addr: String,
    /// Bearer-token file path. The token value is never echoed; only the
    /// path the operator supplied is rendered so operators can verify
    /// which file is in use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_path: Option<String>,
    /// Redacted placeholder when an inline bearer token is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<&'static str>,
}

impl IngestControlReaderToml {
    /// Builds a reader-side TOML projection from a resolved reader config.
    #[must_use]
    pub fn from_reader(resolved: &ResolvedIngestControlReader) -> Self {
        Self {
            addr: resolved.addr.clone(),
            bearer_token_path: resolved
                .bearer_token_path
                .as_ref()
                .map(|path| path.display().to_string()),
            bearer_token: (resolved.bearer_token.is_some() && resolved.bearer_token_path.is_none())
                .then_some("[REDACTED]"),
        }
    }

    /// Builds a reader-side TOML projection from a resolved endpoint and
    /// the optional bearer-token path.
    #[must_use]
    pub fn from_resolved(addr: String, bearer_token_path: Option<&Path>) -> Self {
        Self {
            addr,
            bearer_token_path: bearer_token_path.map(|path| path.display().to_string()),
            bearer_token: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    const DEFAULT_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9100);

    #[test]
    fn writer_defaults_to_loopback_listen_addr() -> Result<(), ConfigError> {
        let resolved = resolve_ingest_control_writer(IngestControlSection::default())?;
        assert_eq!(resolved.listen_addr, Some(DEFAULT_LISTEN));
        Ok(())
    }

    #[test]
    fn writer_empty_string_disables_listener() -> Result<(), ConfigError> {
        let resolved = resolve_ingest_control_writer(IngestControlSection {
            listen_addr: Some(String::new()),
            ..IngestControlSection::default()
        })?;
        assert_eq!(resolved.listen_addr, None);
        Ok(())
    }

    #[test]
    fn writer_rejects_malformed_listen_addr() {
        let outcome = resolve_ingest_control_writer(IngestControlSection {
            listen_addr: Some("not a socket".to_owned()),
            ..IngestControlSection::default()
        });
        assert!(matches!(outcome, Err(ConfigError::Invalid { .. })));
    }

    #[test]
    fn reader_defaults_to_loopback_url() -> Result<(), ConfigError> {
        let resolved = resolve_ingest_control_reader(IngestControlSection::default())?;
        assert_eq!(resolved.addr, DEFAULT_INGEST_CONTROL_READER_URL);
        Ok(())
    }

    #[test]
    fn reader_rejects_malformed_url() {
        let outcome = resolve_ingest_control_reader(IngestControlSection {
            addr: Some("not a url".to_owned()),
            ..IngestControlSection::default()
        });
        assert!(matches!(outcome, Err(ConfigError::Invalid { .. })));
    }
}
