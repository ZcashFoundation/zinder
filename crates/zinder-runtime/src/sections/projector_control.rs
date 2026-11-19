//! Private owner-control configuration for coherent state-bundle capture.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    BearerToken, ConfigError,
    config::{load_bearer_token, parse_socket_addr},
};

use super::defaults::DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT;

/// Raw `[projector_control]` configuration.
///
/// This surface is opt-in. An enabled control endpoint always requires a
/// bearer-token file and a loopback address because it commands the sole
/// wallet primary to create owner checkpoints.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectorControlSection {
    /// Loopback TCP address, or an empty string to disable control.
    pub listen_addr: Option<String>,
    /// File containing the bearer token required for every request.
    pub bearer_token_path: Option<PathBuf>,
    /// Shared root that contains freshly prepared state-bundle candidates.
    pub checkpoint_staging_root: Option<PathBuf>,
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
    if listen_addr.is_some() && section.bearer_token_path.is_none() {
        return Err(ConfigError::missing_field(
            "projector_control.bearer_token_path",
        ));
    }
    let bearer_token = load_bearer_token(section.bearer_token_path.as_deref())?;
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
    /// Shared candidate root.
    pub checkpoint_staging_root: String,
}

impl ProjectorControlToml {
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
