//! Shared `[security]` config section.
//!
//! Carries the cross-cutting serving-surface security posture. Today it
//! holds one field: [`SecuritySection::allow_public_bind`], the opt-in
//! that lets a plaintext serving surface bind a public or unspecified
//! address (see [`guard_serving_bind`](crate::guard_serving_bind)). Every
//! service binary wires the per-service default through
//! [`ConfigLoader::with_security_section`](crate::ConfigLoader::with_security_section)
//! and passes the resolved flag to the bind guard at validation time.

use serde::{Deserialize, Serialize};

use crate::config::{ConfigError, require_field};

/// Default for `[security] allow_public_bind`.
///
/// `false` keeps the loopback-only posture honest: a binary built once
/// refuses a public bind until the proxy-fronted deployment consciously
/// opts in.
pub const DEFAULT_ALLOW_PUBLIC_BIND: bool = false;

/// Raw and serializable `[security]` config section.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecuritySection {
    /// Opt-in to binding plaintext serving surfaces to public or
    /// unspecified addresses. Defaults to `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_public_bind: Option<bool>,
}

impl SecuritySection {
    /// Returns whether `[security]` has no explicit setting.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.allow_public_bind.is_none()
    }

    /// Builds a `[security]` section projection from a resolved opt-in flag.
    #[must_use]
    pub const fn from_resolved(allow_public_bind: bool) -> Self {
        Self {
            allow_public_bind: Some(allow_public_bind),
        }
    }
}

/// Resolves the public-bind opt-in from a [`SecuritySection`].
///
/// The loader applies [`DEFAULT_ALLOW_PUBLIC_BIND`] before this runs, so an
/// absent field still resolves rather than failing on a missing value.
pub fn resolve_allow_public_bind(section: SecuritySection) -> Result<bool, ConfigError> {
    require_field(section.allow_public_bind, "security.allow_public_bind")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_section_requires_loader_default() {
        let outcome = resolve_allow_public_bind(SecuritySection::default());
        assert!(matches!(outcome, Err(ConfigError::MissingField { .. })));
    }

    #[test]
    fn explicit_true_resolves() -> Result<(), ConfigError> {
        let section = SecuritySection {
            allow_public_bind: Some(true),
        };
        assert!(resolve_allow_public_bind(section)?);
        Ok(())
    }

    #[test]
    fn explicit_false_resolves() -> Result<(), ConfigError> {
        let section = SecuritySection {
            allow_public_bind: Some(false),
        };
        assert!(!resolve_allow_public_bind(section)?);
        Ok(())
    }
}
