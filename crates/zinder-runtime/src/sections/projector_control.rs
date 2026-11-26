//! Private owner-control configuration for coherent state-bundle capture.

use std::{
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    BearerToken, ConfigError,
    config::{parse_socket_addr, resolve_optional_bearer_token},
};

use super::defaults::DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT;

/// Raw `[projector_control]` configuration.
///
/// This surface is opt-in. An enabled control endpoint always requires a
/// bearer-token file or inline token and a loopback address because it commands
/// the sole wallet primary to create owner checkpoints.
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectorControlSection {
    /// Loopback TCP address, or an empty string to disable control.
    pub listen_addr: Option<String>,
    /// File containing the bearer token required for every request.
    /// Mutually exclusive with [`Self::bearer_token`].
    pub bearer_token_path: Option<PathBuf>,
    /// Inline bearer token required for every request.
    /// Mutually exclusive with [`Self::bearer_token_path`].
    pub bearer_token: Option<String>,
    /// Shared root that contains freshly prepared state-bundle candidates.
    pub checkpoint_staging_root: Option<PathBuf>,
}

impl fmt::Debug for ProjectorControlSection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectorControlSection")
            .field("listen_addr", &self.listen_addr)
            .field("bearer_token_path", &self.bearer_token_path)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("checkpoint_staging_root", &self.checkpoint_staging_root)
            .finish()
    }
}

/// Resolved projector-owner control configuration.
#[derive(Clone, Debug)]
pub struct ResolvedProjectorControl {
    /// Endpoint address when capture control is enabled.
    pub listen_addr: Option<SocketAddr>,
    /// Token path rendered in redacted effective configuration.
    pub bearer_token_path: Option<PathBuf>,
    /// Loaded bearer token when the endpoint is enabled.
    pub bearer_token: Option<BearerToken>,
    /// Shared candidate root used by coordinator and canonical owner.
    pub checkpoint_staging_root: PathBuf,
}

/// Resolves private projector owner control without a public-bind escape hatch.
pub fn resolve_projector_control(
    section: ProjectorControlSection,
) -> Result<ResolvedProjectorControl, ConfigError> {
    let listen_addr = match section.listen_addr {
        None => None,
        Some(text) if text.trim().is_empty() => None,
        Some(text) => {
            let address = parse_socket_addr("projector_control.listen_addr", &text)?;
            if !address.ip().is_loopback() {
                return Err(ConfigError::invalid(
                    "projector_control.listen_addr must be a loopback address",
                ));
            }
            Some(address)
        }
    };
    let bearer_token = resolve_optional_bearer_token(
        section.bearer_token_path.as_deref(),
        section.bearer_token.as_deref(),
        "projector_control.bearer_token_path",
        "projector_control.bearer_token",
    )?;
    if listen_addr.is_some() && bearer_token.is_none() {
        return Err(ConfigError::missing_field(
            "projector_control.bearer_token_path or projector_control.bearer_token",
        ));
    }
    Ok(ResolvedProjectorControl {
        listen_addr,
        bearer_token_path: section.bearer_token_path,
        bearer_token,
        checkpoint_staging_root: section
            .checkpoint_staging_root
            .unwrap_or_else(|| PathBuf::from(DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT)),
    })
}

/// Redacted TOML representation of `[projector_control]`.
#[derive(Debug, Serialize)]
pub struct ProjectorControlToml {
    /// Empty when control is disabled.
    pub listen_addr: String,
    /// Token-file path, never token material.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_path: Option<String>,
    /// Redacted placeholder when an inline bearer token is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<&'static str>,
    /// Shared candidate root.
    pub checkpoint_staging_root: String,
}

impl ProjectorControlToml {
    /// Renders resolved owner-control settings from [`ResolvedProjectorControl`]
    /// without exposing auth material.
    #[must_use]
    pub fn from_control(control: &ResolvedProjectorControl) -> Self {
        Self {
            listen_addr: control.listen_addr.map_or_default(|addr| addr.to_string()),
            bearer_token_path: control
                .bearer_token_path
                .as_ref()
                .map(|path| path.display().to_string()),
            bearer_token: (control.bearer_token.is_some() && control.bearer_token_path.is_none())
                .then_some("[REDACTED]"),
            checkpoint_staging_root: control.checkpoint_staging_root.display().to_string(),
        }
    }

    /// Renders resolved owner-control settings without exposing auth material.
    #[must_use]
    pub fn from_resolved(
        listen_addr: Option<SocketAddr>,
        bearer_token_path: Option<&Path>,
        checkpoint_staging_root: &Path,
    ) -> Self {
        Self {
            listen_addr: listen_addr.map_or_default(|addr| addr.to_string()),
            bearer_token_path: bearer_token_path.map(|path| path.display().to_string()),
            bearer_token: None,
            checkpoint_staging_root: checkpoint_staging_root.display().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProjectorControlSection, resolve_projector_control};

    #[test]
    fn enabled_owner_control_requires_a_bearer_token_file() {
        let outcome = resolve_projector_control(ProjectorControlSection {
            listen_addr: Some("127.0.0.1:9101".to_owned()),
            ..ProjectorControlSection::default()
        });
        assert!(outcome.is_err());
    }

    #[test]
    fn owner_control_refuses_non_loopback_bind_even_with_auth() {
        let outcome = resolve_projector_control(ProjectorControlSection {
            listen_addr: Some("0.0.0.0:9101".to_owned()),
            bearer_token_path: Some("/not-read-because-bind-is-invalid".into()),
            ..ProjectorControlSection::default()
        });
        assert!(outcome.is_err());
    }
}
