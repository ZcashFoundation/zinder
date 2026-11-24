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
    /// Native and compatibility wallet serving over a local wallet projection
    /// without raw transaction blobs; mined transaction bytes are fetched from
    /// a verified upstream node on demand.
    Wallet,
    /// Native and compatibility wallet serving over a local wallet projection
    /// with retained raw transaction blobs.
    WalletArchive,
}

impl DeploymentProfile {
    /// Returns the persisted configuration spelling of this profile.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LightWallet => "light-wallet",
            Self::Wallet => "wallet",
            Self::WalletArchive => "wallet-archive",
        }
    }

    /// Returns the retained data policy the canonical writer builds under this profile.
    #[must_use]
    pub const fn canonical_stored_set(self) -> CanonicalStoredSet {
        CanonicalStoredSet::for_history(
            CanonicalStoreWorkload::Wallet,
            self.raw_blob_retention(),
            CanonicalHistoryBounds::complete(),
        )
    }

    /// Returns the raw-blob retention the writer stores under this profile.
    #[must_use]
    pub const fn raw_blob_retention(self) -> RawBlobRetention {
        match self {
            Self::LightWallet | Self::Wallet => RawBlobRetention::None,
            Self::WalletArchive => RawBlobRetention::All,
        }
    }

    /// Resolves retention and checks an explicitly configured value for agreement.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the requested retention conflicts.
    pub fn resolve_raw_blob_retention(
        self,
        configured: Option<RawBlobRetention>,
    ) -> Result<RawBlobRetention, crate::ConfigError> {
        let retention = self.raw_blob_retention();
        if configured.is_some_and(|policy| policy != retention) {
            return Err(crate::ConfigError::invalid(format!(
                "deployment.profile = \"{}\" requires storage.raw_blob_policy = \"{}\"",
                self.as_str(),
                retention.as_kebab_case(),
            )));
        }
        Ok(retention)
    }

    /// Returns the minimum canonical coverage for this profile's stored set.
    #[must_use]
    pub fn canonical_requirement(self) -> CanonicalStoreRequirements {
        let requirements = CanonicalStoreRequirements::default()
            .from_height(CanonicalHistoryBounds::complete().first_available_height())
            .with_blob_retention(self.raw_blob_retention());
        if self == Self::LightWallet {
            requirements.with_complete_subtree_root_prefix()
        } else {
            requirements
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
    fn deployment_section_deserializes_every_profile() -> Result<()> {
        for (encoded, expected) in [
            ("light-wallet", DeploymentProfile::LightWallet),
            ("wallet", DeploymentProfile::Wallet),
            ("wallet-archive", DeploymentProfile::WalletArchive),
        ] {
            let section: DeploymentSection = toml::from_str(&format!("profile = \"{encoded}\""))?;
            assert_eq!(section.profile, Some(expected));
        }
        assert!(toml::from_str::<DeploymentSection>(r#"profile = "explorer""#).is_err());
        Ok(())
    }

    #[test]
    fn every_profile_fixes_the_writer_retention_and_complete_history() {
        use zinder_store::RawBlobRetention;
        for (profile, retention) in [
            (DeploymentProfile::LightWallet, RawBlobRetention::None),
            (DeploymentProfile::Wallet, RawBlobRetention::None),
            (DeploymentProfile::WalletArchive, RawBlobRetention::All),
        ] {
            assert_eq!(profile.raw_blob_retention(), retention);
            let stored_set = profile.canonical_stored_set();
            assert_eq!(stored_set.raw_blob_retention(), retention);
            assert_eq!(
                stored_set.first_height(CanonicalStoredData::BlockHistory),
                Some(BlockHeight::new(1))
            );
            assert!(!stored_set.contains(CanonicalStoredData::FinalNoteCommitmentRoots));
        }
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
