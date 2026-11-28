//! Private owner-control configuration for coherent state-bundle capture.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    BearerToken, ConfigError,
    config::{SecretInput, Toggle, resolve_optional_bearer_token, validate_optional_path},
};

use super::{
    defaults::DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT, storage::derive_checkpoints_path,
};

/// Raw and serializable `[projector_control]` configuration.
///
/// This surface is opt-in. An enabled control endpoint always requires a
/// bearer-token file or inline token and a loopback address because it commands
/// the sole wallet primary to create owner checkpoints.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectorControlSection {
    /// Loopback TCP address, or `"none"` to disable control explicitly.
    #[serde(skip_serializing_if = "Toggle::is_defaulted")]
    pub listen_addr: Toggle<SocketAddr>,
    /// File containing the bearer token required for every request.
    /// Mutually exclusive with [`Self::bearer_token`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token_path: Option<PathBuf>,
    /// Inline bearer token required for every request.
    /// Mutually exclusive with [`Self::bearer_token_path`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_token: Option<SecretInput>,
    /// Shared root that contains freshly prepared state-bundle candidates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_staging_root: Option<PathBuf>,
}

impl ProjectorControlSection {
    /// Returns whether every field in `[projector_control]` is unset.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.listen_addr.is_defaulted()
            && self.bearer_token_path.is_none()
            && self.bearer_token.is_none()
            && self.checkpoint_staging_root.is_none()
    }

    /// Returns whether any secret or credential path is set in `[projector_control]`.
    #[must_use]
    pub const fn has_secrets(&self) -> bool {
        self.bearer_token_path.is_some() || self.bearer_token.is_some()
    }

    /// Renders resolved owner-control settings from [`ResolvedProjectorControl`]
    /// without exposing secret token material.
    #[must_use]
    pub fn from_control(control: &ResolvedProjectorControl) -> Self {
        Self {
            listen_addr: Toggle::from_resolved_defaulted_on(control.listen_addr),
            bearer_token_path: control.bearer_token_path.clone(),
            bearer_token: (control.bearer_token.is_some() && control.bearer_token_path.is_none())
                .then(SecretInput::redacted_placeholder),
            checkpoint_staging_root: Some(control.checkpoint_staging_root.clone()),
        }
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
    resolve_projector_control_with_data_dir(section, None)
}

/// Resolves private projector owner control without a public-bind escape hatch,
/// deriving an omitted `checkpoint_staging_root` from `data_dir` when set.
pub fn resolve_projector_control_with_data_dir(
    section: ProjectorControlSection,
    data_dir: Option<&Path>,
) -> Result<ResolvedProjectorControl, ConfigError> {
    let listen_addr = match section.listen_addr {
        Toggle::Defaulted | Toggle::Disabled => None,
        Toggle::Enabled(address) => {
            if !address.ip().is_loopback() {
                return Err(ConfigError::invalid(
                    "projector_control.listen_addr must be a loopback address",
                ));
            }
            Some(address)
        }
    };
    let bearer_token_path = validate_optional_path(
        section.bearer_token_path,
        "projector_control.bearer_token_path",
    )?;
    let checkpoint_staging_root = validate_optional_path(
        section.checkpoint_staging_root,
        "projector_control.checkpoint_staging_root",
    )?;
    let bearer_token = resolve_optional_bearer_token(
        bearer_token_path.as_deref(),
        section
            .bearer_token
            .as_ref()
            .map(SecretInput::expose_secret),
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
        bearer_token_path,
        bearer_token,
        checkpoint_staging_root: checkpoint_staging_root.unwrap_or_else(|| {
            data_dir.map_or_else(
                || PathBuf::from(DEFAULT_INGEST_CONTROL_CHECKPOINT_STAGING_ROOT),
                derive_checkpoints_path,
            )
        }),
    })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::{ProjectorControlSection, resolve_projector_control};
    use crate::{ConfigError, Toggle};

    #[test]
    fn enabled_owner_control_requires_a_bearer_token_file() {
        let outcome = resolve_projector_control(ProjectorControlSection {
            listen_addr: Toggle::Enabled(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9101)),
            ..ProjectorControlSection::default()
        });
        assert!(outcome.is_err());
    }

    #[test]
    fn owner_control_refuses_non_loopback_bind_even_with_auth() {
        let outcome = resolve_projector_control(ProjectorControlSection {
            listen_addr: Toggle::Enabled(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 9101)),
            bearer_token_path: Some("/not-read-because-bind-is-invalid".into()),
            ..ProjectorControlSection::default()
        });
        assert!(outcome.is_err());
    }

    #[test]
    fn owner_control_derives_checkpoint_staging_root_from_data_dir() -> Result<(), ConfigError> {
        let derived = super::resolve_projector_control_with_data_dir(
            ProjectorControlSection::default(),
            Some(std::path::Path::new("/custom/zinder-data")),
        )?;
        assert_eq!(
            derived.checkpoint_staging_root,
            std::path::Path::new("/custom/zinder-data/checkpoints")
        );
        Ok(())
    }
}
