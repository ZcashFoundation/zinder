//! Shared `[ops]` config section.
//!
//! Every service binary's configuration carries an [`OpsSection`]; the
//! per-service default is wired through [`crate::ConfigLoader::with_ops_section`].
//! Setting `listen_addr = "none"` opts the service out of binding an
//! operational endpoint.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::config::{ConfigError, Toggle};

/// Raw and serializable `[ops]` config section.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpsSection {
    /// Listen address for the operational HTTP endpoint (`/healthz`,
    /// `/readyz`, `/metrics`), or `"none"` to opt out explicitly.
    #[serde(skip_serializing_if = "Toggle::is_defaulted")]
    pub listen_addr: Toggle<SocketAddr>,
}

impl OpsSection {
    /// Returns whether `[ops]` has no explicit setting or opt-out.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.listen_addr.is_defaulted()
    }

    /// Builds an [`OpsSection`] projection from a resolved listen address,
    /// encoding `None` as `"none"`.
    #[must_use]
    pub fn from_resolved(listen_addr: Option<SocketAddr>) -> Self {
        Self {
            listen_addr: Toggle::from_resolved_defaulted_on(listen_addr),
        }
    }
}

/// Resolves the operational listen address from an [`OpsSection`].
///
/// Returns `Ok(None)` when the operator opted out (`"none"`) or when no default
/// was wired. Returns `Ok(Some(_))` when a socket address was configured or
/// defaulted by [`crate::ConfigLoader::with_ops_section`].
pub const fn resolve_ops_listen_addr(
    section: OpsSection,
) -> Result<Option<SocketAddr>, ConfigError> {
    match section.listen_addr {
        Toggle::Defaulted | Toggle::Disabled => Ok(None),
        Toggle::Enabled(addr) => Ok(Some(addr)),
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
    fn none_sentinel_resolves_to_disabled_and_empty_is_rejected() -> Result<(), ConfigError> {
        let disabled: OpsSection = toml::from_str(r#"listen_addr = "none""#)
            .map_err(|source| ConfigError::invalid(source.to_string()))?;
        assert_eq!(resolve_ops_listen_addr(disabled)?, None);
        assert!(toml::from_str::<OpsSection>(r#"listen_addr = """#).is_err());
        assert!(toml::from_str::<OpsSection>(r#"listen_addr = "   ""#).is_err());
        Ok(())
    }

    #[test]
    fn valid_address_parses() -> Result<(), ConfigError> {
        let section = OpsSection {
            listen_addr: Toggle::Enabled(LOOPBACK_OPS_ADDR),
        };
        assert_eq!(resolve_ops_listen_addr(section)?, Some(LOOPBACK_OPS_ADDR));
        Ok(())
    }

    #[test]
    fn invalid_address_is_rejected_at_deserialize() {
        let outcome = toml::from_str::<OpsSection>(r#"listen_addr = "not a socket""#);
        assert!(outcome.is_err());
    }

    #[test]
    fn ops_section_renders_none_when_disabled() -> Result<(), toml::ser::Error> {
        let rendered = toml::to_string(&OpsSection::from_resolved(None))?;
        assert_eq!(rendered.trim(), r#"listen_addr = "none""#);
        Ok(())
    }

    #[test]
    fn ops_section_renders_address_when_enabled() -> Result<(), toml::ser::Error> {
        let rendered = toml::to_string(&OpsSection::from_resolved(Some(LOOPBACK_OPS_ADDR)))?;
        assert_eq!(rendered.trim(), r#"listen_addr = "127.0.0.1:9106""#);
        Ok(())
    }
}
