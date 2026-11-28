//! Shared `[ingest_control]` config section.
//!
//! The writer reads `listen_addr` to bind the private control-plane
//! endpoint; the readers read `addr` to connect; both sides read
//! `bearer_token_path` or `bearer_token` when ADR-0011 auth is enforced.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    BearerToken,
    config::{
        ConfigError, RedactedUrl, SecretInput, Toggle, parse_socket_addr,
        resolve_optional_bearer_token, validate_optional_path,
    },
    sections::{
        defaults::{
            DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT, DEFAULT_INGEST_CONTROL_LISTEN_ADDR,
            DEFAULT_INGEST_CONTROL_READER_URL,
        },
        storage::derive_checkpoints_path,
    },
};

/// Raw and serializable `[ingest_control]` config section.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestControlSection {
    /// Writer-side bind address for the `IngestControl` gRPC endpoint, or
    /// `"none"` to disable the endpoint for one-shot bootstrap runs.
    #[serde(skip_serializing_if = "Toggle::is_defaulted")]
    pub listen_addr: Toggle<SocketAddr>,
    /// Reader-side URL the colocated readers dial. Defaults to
    /// `http://127.0.0.1:9100`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addr: Option<RedactedUrl>,
    /// Shared-secret bearer token path enforced on every `IngestControl`
    /// request when auth is enabled (ADR-0011).
    /// Mutually exclusive with [`Self::bearer_token`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_path: Option<PathBuf>,
    /// Inline shared-secret bearer token for `IngestControl` requests.
    /// Mutually exclusive with [`Self::bearer_token_path`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<SecretInput>,
    /// File containing the capability token required only for canonical owner
    /// checkpoint creation. Reader-only compat processes must not receive it.
    /// Mutually exclusive with [`Self::checkpoint_bearer_token`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_bearer_token_path: Option<PathBuf>,
    /// Inline capability token required only for canonical owner checkpoint
    /// creation. Mutually exclusive with [`Self::checkpoint_bearer_token_path`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_bearer_token: Option<SecretInput>,
    /// Writer-owned directory below which canonical checkpoint candidates are
    /// staged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_staging_root: Option<PathBuf>,
}

impl IngestControlSection {
    /// Returns whether every field in `[ingest_control]` is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.listen_addr.is_defaulted()
            && self.addr.is_none()
            && self.bearer_token_path.is_none()
            && self.bearer_token.is_none()
            && self.checkpoint_bearer_token_path.is_none()
            && self.checkpoint_bearer_token.is_none()
            && self.checkpoint_staging_root.is_none()
    }

    /// Returns whether writer-only checkpoint capability credentials are present.
    #[must_use]
    pub const fn has_writer_only_secrets(&self) -> bool {
        self.checkpoint_bearer_token_path.is_some() || self.checkpoint_bearer_token.is_some()
    }

    /// Builds a writer-side redacted projection from [`ResolvedIngestControlWriter`].
    #[must_use]
    pub fn from_writer(resolved: &ResolvedIngestControlWriter) -> Self {
        Self {
            listen_addr: Toggle::from_resolved_defaulted_on(resolved.listen_addr),
            addr: None,
            bearer_token_path: resolved.bearer_token_path.clone(),
            bearer_token: (resolved.bearer_token.is_some() && resolved.bearer_token_path.is_none())
                .then(SecretInput::redacted_placeholder),
            checkpoint_bearer_token_path: resolved.checkpoint_bearer_token_path.clone(),
            checkpoint_bearer_token: (resolved.checkpoint_bearer_token.is_some()
                && resolved.checkpoint_bearer_token_path.is_none())
            .then(SecretInput::redacted_placeholder),
            checkpoint_staging_root: Some(resolved.checkpoint_staging_root.clone()),
        }
    }

    /// Builds a reader-side redacted projection from [`ResolvedIngestControlReader`].
    #[must_use]
    pub fn from_reader(resolved: &ResolvedIngestControlReader) -> Self {
        Self {
            listen_addr: Toggle::Defaulted,
            addr: Some(RedactedUrl::from_redacted_display(&resolved.addr)),
            bearer_token_path: resolved.bearer_token_path.clone(),
            bearer_token: (resolved.bearer_token.is_some() && resolved.bearer_token_path.is_none())
                .then(SecretInput::redacted_placeholder),
            checkpoint_bearer_token_path: None,
            checkpoint_bearer_token: None,
            checkpoint_staging_root: None,
        }
    }
}

/// Resolved writer-side ingest-control configuration.
#[derive(Clone, Debug)]
pub struct ResolvedIngestControlWriter {
    /// Listen address. `None` when the operator opted out via `"none"`.
    pub listen_addr: Option<SocketAddr>,
    /// Bearer token file path, when ADR-0011 auth is enabled.
    pub bearer_token_path: Option<PathBuf>,
    /// Loaded bearer token, when configured.
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
    /// Loaded bearer token, when configured.
    pub bearer_token: Option<BearerToken>,
}

/// Validates and resolves an [`IngestControlSection`] for the writer (`zinder-ingest`).
pub fn resolve_ingest_control_writer(
    section: IngestControlSection,
) -> Result<ResolvedIngestControlWriter, ConfigError> {
    resolve_ingest_control_writer_with_data_dir(section, None)
}

/// Validates and resolves an [`IngestControlSection`] for the writer
/// (`zinder-ingest`), deriving an omitted `checkpoint_staging_root` from `data_dir` when set.
pub fn resolve_ingest_control_writer_with_data_dir(
    section: IngestControlSection,
    data_dir: Option<&Path>,
) -> Result<ResolvedIngestControlWriter, ConfigError> {
    let default_addr = parse_socket_addr(
        "ingest_control.listen_addr",
        DEFAULT_INGEST_CONTROL_LISTEN_ADDR,
    )?;
    let listen_addr = section.listen_addr.resolve_or(default_addr);
    let bearer_token_path = validate_optional_path(
        section.bearer_token_path,
        "ingest_control.bearer_token_path",
    )?;
    let checkpoint_bearer_token_path = validate_optional_path(
        section.checkpoint_bearer_token_path,
        "ingest_control.checkpoint_bearer_token_path",
    )?;
    let checkpoint_staging_root = validate_optional_path(
        section.checkpoint_staging_root,
        "ingest_control.checkpoint_staging_root",
    )?;
    let bearer_token = resolve_optional_bearer_token(
        bearer_token_path.as_deref(),
        section
            .bearer_token
            .as_ref()
            .map(SecretInput::expose_secret),
        "ingest_control.bearer_token_path",
        "ingest_control.bearer_token",
    )?;
    let checkpoint_bearer_token = resolve_optional_bearer_token(
        checkpoint_bearer_token_path.as_deref(),
        section
            .checkpoint_bearer_token
            .as_ref()
            .map(SecretInput::expose_secret),
        "ingest_control.checkpoint_bearer_token_path",
        "ingest_control.checkpoint_bearer_token",
    )?;
    Ok(ResolvedIngestControlWriter {
        listen_addr,
        bearer_token_path,
        bearer_token,
        checkpoint_bearer_token,
        checkpoint_bearer_token_path,
        checkpoint_staging_root: checkpoint_staging_root.unwrap_or_else(|| {
            data_dir.map_or_else(
                || PathBuf::from(DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT),
                derive_checkpoints_path,
            )
        }),
    })
}

/// Validates and resolves an [`IngestControlSection`] for a reader
/// (`zinder-projector`, `zinder-query`, `zinder-compat-lightwalletd`).
pub fn resolve_ingest_control_reader(
    section: IngestControlSection,
) -> Result<ResolvedIngestControlReader, ConfigError> {
    let addr = section.addr.map_or_else(
        || DEFAULT_INGEST_CONTROL_READER_URL.to_owned(),
        RedactedUrl::into_inner,
    );
    crate::transport::validate_zinder_grpc_endpoint(&addr).map_err(|source| {
        ConfigError::invalid(format!(
            "ingest_control.addr {addr} is not a valid endpoint: {source}"
        ))
    })?;
    let bearer_token_path = validate_optional_path(
        section.bearer_token_path,
        "ingest_control.bearer_token_path",
    )?;
    let bearer_token = resolve_optional_bearer_token(
        bearer_token_path.as_deref(),
        section
            .bearer_token
            .as_ref()
            .map(SecretInput::expose_secret),
        "ingest_control.bearer_token_path",
        "ingest_control.bearer_token",
    )?;
    Ok(ResolvedIngestControlReader {
        addr,
        bearer_token_path,
        bearer_token,
    })
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
    fn writer_none_disables_listener_and_empty_is_rejected() -> Result<(), ConfigError> {
        let section: IngestControlSection = toml::from_str(r#"listen_addr = "none""#)
            .map_err(|source| ConfigError::invalid(source.to_string()))?;
        let resolved = resolve_ingest_control_writer(section)?;
        assert_eq!(resolved.listen_addr, None);
        assert!(toml::from_str::<IngestControlSection>(r#"listen_addr = """#).is_err());
        Ok(())
    }

    #[test]
    fn writer_rejects_malformed_listen_addr() {
        let outcome = toml::from_str::<IngestControlSection>(r#"listen_addr = "not a socket""#);
        assert!(outcome.is_err());
    }

    #[test]
    fn writer_derives_checkpoint_staging_root_from_data_dir() -> Result<(), ConfigError> {
        let derived = resolve_ingest_control_writer_with_data_dir(
            IngestControlSection::default(),
            Some(Path::new("/custom/zinder-data")),
        )?;
        assert_eq!(
            derived.checkpoint_staging_root,
            Path::new("/custom/zinder-data/checkpoints")
        );

        let explicit = resolve_ingest_control_writer_with_data_dir(
            IngestControlSection {
                checkpoint_staging_root: Some(PathBuf::from("/explicit/checkpoints")),
                ..IngestControlSection::default()
            },
            Some(Path::new("/custom/zinder-data")),
        )?;
        assert_eq!(
            explicit.checkpoint_staging_root,
            Path::new("/explicit/checkpoints")
        );
        Ok(())
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
            addr: Some(RedactedUrl::from_redacted_display("not a url")),
            ..IngestControlSection::default()
        });
        assert!(outcome.is_err());
    }
}
