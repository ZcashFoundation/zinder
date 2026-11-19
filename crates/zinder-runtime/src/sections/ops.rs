//! Shared `[ops]` config section.
//!
//! Every service binary's `RawConfig` carries an [`OpsSection`]; the
//! per-service default is wired through [`crate::ConfigLoader::with_ops_section`].
//! Empty string in `listen_addr` opts the service out of binding any
//! operational endpoint.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::config::{ConfigError, parse_socket_addr};

/// Raw `[ops]` config section.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpsSection {
    /// Listen address for the operational HTTP endpoint (`/healthz`,
    /// `/readyz`, `/metrics`). Empty string opts the service out of
    /// binding an operational endpoint entirely.
    pub listen_addr: Option<String>,
}

/// Resolves the operational listen address from an [`OpsSection`].
///
/// Returns `Ok(None)` when the operator opted out (empty string).
/// Returns `Ok(Some(_))` with the parsed address when the field is set
/// to a non-empty value. The per-service default is applied by the
/// loader before this function runs, so an absent field still resolves
/// to the default rather than disabling the endpoint.
pub fn resolve_ops_listen_addr(section: OpsSection) -> Result<Option<SocketAddr>, ConfigError> {
    let Some(text) = section.listen_addr else {
        return Ok(None);
    };
    if text.trim().is_empty() {
        return Ok(None);
    }
    parse_socket_addr("ops.listen_addr", &text).map(Some)
}

/// Redacted TOML projection of `[ops]` for `--print-config`.
#[derive(Debug, Serialize)]
pub struct OpsToml {
    /// Resolved listen address as a string; empty when ops is disabled.
    pub listen_addr: String,
}

impl OpsToml {
    /// Builds an [`OpsToml`] from a resolved listen address.
    #[must_use]
    pub fn from_resolved(listen_addr: Option<SocketAddr>) -> Self {
        Self {
            listen_addr: listen_addr.map_or_default(|addr| addr.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    const LOOPBACK_OPS_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9106);

    #[test]
    fn unset_section_resolves_to_disabled() -> Result<(), ConfigError> {
        assert_eq!(resolve_ops_listen_addr(OpsSection::default())?, None);
        Ok(())
    }

    #[test]
    fn empty_string_resolves_to_disabled() -> Result<(), ConfigError> {
        let section = OpsSection {
            listen_addr: Some(String::new()),
        };
        assert_eq!(resolve_ops_listen_addr(section)?, None);
        Ok(())
    }

    #[test]
    fn whitespace_only_resolves_to_disabled() -> Result<(), ConfigError> {
        let section = OpsSection {
            listen_addr: Some("   ".to_owned()),
        };
        assert_eq!(resolve_ops_listen_addr(section)?, None);
        Ok(())
    }

    #[test]
    fn valid_address_parses() -> Result<(), ConfigError> {
        let section = OpsSection {
            listen_addr: Some("127.0.0.1:9106".to_owned()),
        };
        assert_eq!(resolve_ops_listen_addr(section)?, Some(LOOPBACK_OPS_ADDR));
        Ok(())
    }

    #[test]
    fn invalid_address_returns_invalid_error() {
        let section = OpsSection {
            listen_addr: Some("not a socket".to_owned()),
        };
        let outcome = resolve_ops_listen_addr(section);
        assert!(matches!(outcome, Err(ConfigError::Invalid { .. })));
    }

    #[test]
    fn ops_toml_renders_empty_when_disabled() {
        let toml = OpsToml::from_resolved(None);
        assert_eq!(toml.listen_addr, "");
    }

    #[test]
    fn ops_toml_renders_address_when_enabled() {
        let toml = OpsToml::from_resolved(Some(LOOPBACK_OPS_ADDR));
        assert_eq!(toml.listen_addr, "127.0.0.1:9106");
    }
}
