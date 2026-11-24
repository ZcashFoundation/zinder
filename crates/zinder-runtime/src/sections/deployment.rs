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

#[cfg(test)]
mod tests {
    use eyre::{Result, eyre};
    use zinder_core::{BlockHeight, Network};
    use zinder_store::{CanonicalStoredData, RocksDbCanonicalSecondary, RocksDbResourceBudget};
    use zinder_testkit::{
        CanonicalBaseline, CanonicalWriterFixture, ChainFixture, sample_regtest_upgrade_activations,
    };

    use super::{DeploymentProfile, DeploymentSection};

    #[test]
    fn deployment_section_deserializes_light_wallet() -> Result<()> {
        let section: DeploymentSection = toml::from_str("profile = \"light-wallet\"")?;
        assert_eq!(section.profile, Some(DeploymentProfile::LightWallet));
        Ok(())
    }

    #[test]
    fn light_wallet_reader_admits_a_published_store_built_with_its_profile() -> Result<()> {
        let profile = DeploymentProfile::LightWallet;
        let stored_set = profile.canonical_stored_set();
        let first = stored_set
            .first_height(CanonicalStoredData::BlockHistory)
            .ok_or_else(|| eyre!("the light-wallet profile needs block history"))?;
        assert_eq!(first, BlockHeight::new(1));
        assert_eq!(
            stored_set.raw_blob_retention(),
            zinder_store::RawBlobRetention::None
        );
        let chain = ChainFixture::new(Network::ZcashRegtest)
            .with_raw_blob_retention(stored_set.raw_blob_retention())
            .extend_blocks(2);
        let activations = sample_regtest_upgrade_activations();
        let writer =
            CanonicalWriterFixture::publish(&CanonicalBaseline::new(&chain), &activations)?;
        let primary = writer.canonical_primary()?;
        assert_eq!(
            primary.build_plan().stored_set(primary.workload()),
            stored_set
        );
        let directory = tempfile::tempdir()?;
        let reader = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            writer.canonical_primary_path(),
            directory.path(),
            Network::ZcashRegtest,
            profile.canonical_requirement(),
            primary.reorg_policy(),
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(reader.event_fence(), primary.event_fence());
        assert!(reader.stored_set().subtree_root_coverage().is_some());
        Ok(())
    }
}
