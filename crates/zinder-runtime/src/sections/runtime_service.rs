//! Deployable runtime service used by shared configuration defaults.

use std::fmt;

/// A deployable Zinder service process.
///
/// Adding a service requires a variant, which in turn forces its default
/// addresses and metrics label to be chosen explicitly. The string
/// returned by [`Self::binary_name`] is stable across releases and is used as
/// the `service` Prometheus label.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuntimeService {
    /// `zinder-ingest`: writer plane.
    Ingest,
    /// `zinder-projector`: wallet projection writer.
    Projector,
    /// `zinder-query`: native wallet query reader.
    Query,
    /// `zinder-compat-lightwalletd`: lightwalletd-protocol compatibility reader.
    CompatLightwalletd,
    /// `zinder-compat-cipherscan`: Cipherscan REST compat reader.
    CompatCipherscan,
    /// `zinder-explorer`: explorer-plane materialized-view reader.
    Explorer,
}

impl RuntimeService {
    /// Canonical binary name used in metrics labels, log targets, and
    /// docs. Stable across releases.
    #[must_use]
    pub const fn binary_name(self) -> &'static str {
        match self {
            Self::Ingest => "zinder-ingest",
            Self::Projector => "zinder-projector",
            Self::Query => "zinder-query",
            Self::CompatLightwalletd => "zinder-compat-lightwalletd",
            Self::CompatCipherscan => "zinder-compat-cipherscan",
            Self::Explorer => "zinder-explorer",
        }
    }
}

impl fmt::Display for RuntimeService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.binary_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_name_matches_display() {
        for service in [
            RuntimeService::Ingest,
            RuntimeService::Projector,
            RuntimeService::Query,
            RuntimeService::CompatLightwalletd,
            RuntimeService::CompatCipherscan,
            RuntimeService::Explorer,
        ] {
            assert_eq!(service.binary_name(), service.to_string());
        }
    }
}
