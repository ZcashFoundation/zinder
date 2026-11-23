//! Shared `[deployment]` configuration for standalone runtimes.

use serde::{Deserialize, Serialize};
use zinder_core::CanonicalHistoryBounds;
use zinder_store::{
    CanonicalStoreRequirements, CanonicalStoreWorkload, CanonicalStoredSet, RawBlobRetention,
};

/// Selects a coordinated deployment configuration across runtimes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum DeploymentProfile {
    /// Canonical wallet history with node-backed reads and no wallet projection.
    LightWallet,
}

impl DeploymentProfile {
    /// Returns the retained data policy the canonical writer builds under this profile.
    #[must_use]
    pub const fn canonical_stored_set(self) -> CanonicalStoredSet {
        match self {
            Self::LightWallet => CanonicalStoredSet::for_history(
                CanonicalStoreWorkload::Wallet,
                RawBlobRetention::None,
                CanonicalHistoryBounds::complete(),
            ),
        }
    }

    /// Returns the minimum canonical data a serving reader requires under this profile.
    ///
    /// Light-wallet serving checks forwarded reads against canonical history from height
    /// one and serves completed subtree roots from index zero.
    #[must_use]
    pub fn canonical_requirement(self) -> CanonicalStoreRequirements {
        match self {
            Self::LightWallet => CanonicalStoreRequirements::default()
                .from_height(CanonicalHistoryBounds::complete().first_available_height())
                .with_complete_subtree_root_prefix(),
        }
    }
}

/// Raw `[deployment]` config section.
#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeploymentSection {
    /// Named deployment profile. Unset keeps the runtime's own settings.
    pub profile: Option<DeploymentProfile>,
}

/// Redacted TOML projection of the resolved `[deployment]` section.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct DeploymentToml {
    /// Selected deployment profile, omitted when unset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<DeploymentProfile>,
}

impl DeploymentToml {
    /// Builds a section from the selected profile.
    #[must_use]
    pub const fn from_resolved(profile: Option<DeploymentProfile>) -> Self {
        Self { profile }
    }

    /// Reports whether the entire section can be omitted.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.profile.is_none()
    }
}
