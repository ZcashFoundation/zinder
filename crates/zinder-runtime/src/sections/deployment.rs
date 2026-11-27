//! Shared `[deployment]` configuration for standalone runtimes.

use serde::{Deserialize, Serialize};
use zinder_core::CanonicalHistoryBounds;
use zinder_store::{
    CanonicalStoreRequirements, CanonicalStoreWorkload, CanonicalStoredSet, RawBlobRetention,
};

/// Selects a coordinated consumer preset across standalone runtimes.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum DeploymentProfile {
    /// Canonical wallet history with node-backed reads and no wallet projection.
    LightWallet,
    /// Native and compatibility wallet serving over a local wallet projection
    /// without raw transaction blobs by default; mined transaction bytes are
    /// fetched from a verified upstream node on demand unless
    /// `storage.full_blocks = true`.
    #[default]
    Wallet,
    /// Explorer views plus canonical wallet history and wallet projection
    /// without raw transaction blobs by default (`storage.full_blocks = true`
    /// retains full blocks).
    Explorer,
    /// Deprecated compatibility alias for [`Self::Wallet`] paired with
    /// `storage.full_blocks = true` (`raw_blob_policy = "all"`).
    WalletArchive,
}

impl DeploymentProfile {
    /// Returns the persisted configuration spelling of this profile.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LightWallet => "light-wallet",
            Self::Wallet => "wallet",
            Self::Explorer => "explorer",
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

    /// Returns the default raw-blob retention the writer stores under this preset
    /// when `storage.full_blocks` is unset or `false`.
    #[must_use]
    pub const fn raw_blob_retention(self) -> RawBlobRetention {
        match self {
            Self::LightWallet | Self::Wallet | Self::Explorer => RawBlobRetention::None,
            Self::WalletArchive => RawBlobRetention::All,
        }
    }

    /// Resolves raw-blob retention for this preset with optional
    /// `storage.full_blocks` opt-in and checks an explicitly configured
    /// `storage.raw_blob_policy` for agreement.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the requested retention conflicts.
    pub fn resolve_raw_blob_retention_with_full_blocks(
        self,
        full_blocks: Option<bool>,
        configured: Option<RawBlobRetention>,
    ) -> Result<RawBlobRetention, crate::ConfigError> {
        if self == Self::WalletArchive && full_blocks == Some(false) {
            return Err(crate::ConfigError::invalid(
                "deployment.profile = \"wallet-archive\" requires raw_blob_policy = \"all\" and conflicts with storage.full_blocks = false; use deployment.profile = \"wallet\" instead",
            ));
        }
        let retention = if full_blocks == Some(true) {
            RawBlobRetention::All
        } else {
            self.raw_blob_retention()
        };
        if configured.is_some_and(|policy| policy != retention) {
            let modifier = if full_blocks == Some(true) {
                " with storage.full_blocks = true"
            } else {
                ""
            };
            return Err(crate::ConfigError::invalid(format!(
                "deployment.profile = \"{}\"{modifier} requires storage.raw_blob_policy = \"{}\"",
                self.as_str(),
                retention.as_kebab_case(),
            )));
        }
        Ok(retention)
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
        self.resolve_raw_blob_retention_with_full_blocks(None, configured)
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

/// Resolves an optional explicit `raw_blob_policy` together with `storage.full_blocks`
/// when no deployment preset is selected.
///
/// # Errors
///
/// Returns [`crate::ConfigError`] when `storage.full_blocks` conflicts with an
/// explicit `storage.raw_blob_policy`.
pub fn resolve_manual_raw_blob_retention(
    full_blocks: Option<bool>,
    configured: Option<RawBlobRetention>,
) -> Result<Option<RawBlobRetention>, crate::ConfigError> {
    match (full_blocks, configured) {
        (Some(true), Some(policy)) if policy != RawBlobRetention::All => {
            Err(crate::ConfigError::invalid(
                "storage.full_blocks = true requires storage.raw_blob_policy = \"all\"; remove conflicting overrides",
            ))
        }
        (Some(true), _) => Ok(Some(RawBlobRetention::All)),
        (Some(false), Some(RawBlobRetention::All)) => Err(crate::ConfigError::invalid(
            "storage.full_blocks = false conflicts with storage.raw_blob_policy = \"all\"; remove conflicting overrides",
        )),
        (Some(false) | None, configured) => Ok(configured),
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

    use super::{DeploymentProfile, DeploymentSection, resolve_manual_raw_blob_retention};

    #[test]
    fn deployment_section_deserializes_every_profile() -> Result<()> {
        for (encoded, expected) in [
            ("light-wallet", DeploymentProfile::LightWallet),
            ("wallet", DeploymentProfile::Wallet),
            ("explorer", DeploymentProfile::Explorer),
            ("wallet-archive", DeploymentProfile::WalletArchive),
        ] {
            let section: DeploymentSection = toml::from_str(&format!("profile = \"{encoded}\""))?;
            assert_eq!(section.profile, Some(expected));
        }
        assert_eq!(DeploymentProfile::default(), DeploymentProfile::Wallet);
        assert_eq!(
            DeploymentProfile::WalletArchive.raw_blob_retention(),
            zinder_store::RawBlobRetention::All
        );
        assert!(
            DeploymentProfile::WalletArchive
                .resolve_raw_blob_retention_with_full_blocks(Some(false), None)
                .is_err()
        );
        assert_eq!(
            DeploymentProfile::WalletArchive
                .resolve_raw_blob_retention_with_full_blocks(None, None)?,
            zinder_store::RawBlobRetention::All
        );
        Ok(())
    }

    #[test]
    fn every_profile_fixes_the_writer_retention_and_complete_history() -> Result<()> {
        use zinder_store::RawBlobRetention;
        for profile in [
            DeploymentProfile::LightWallet,
            DeploymentProfile::Wallet,
            DeploymentProfile::Explorer,
        ] {
            assert_eq!(profile.raw_blob_retention(), RawBlobRetention::None);
            let stored_set = profile.canonical_stored_set();
            assert_eq!(stored_set.raw_blob_retention(), RawBlobRetention::None);
            assert_eq!(
                stored_set.first_height(CanonicalStoredData::BlockHistory),
                Some(BlockHeight::new(1))
            );
            assert!(!stored_set.contains(CanonicalStoredData::FinalNoteCommitmentRoots));
            assert_eq!(
                profile.resolve_raw_blob_retention_with_full_blocks(Some(true), None)?,
                RawBlobRetention::All
            );
            assert!(
                profile
                    .resolve_raw_blob_retention_with_full_blocks(
                        Some(true),
                        Some(RawBlobRetention::None)
                    )
                    .is_err()
            );
        }
        assert_eq!(
            resolve_manual_raw_blob_retention(Some(true), None)?,
            Some(RawBlobRetention::All)
        );
        assert!(
            resolve_manual_raw_blob_retention(Some(false), Some(RawBlobRetention::All)).is_err()
        );
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
