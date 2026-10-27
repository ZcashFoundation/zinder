//! Per-network consensus-upgrade activations, sourced from the running node.
//!
//! The activations answer three questions Zinder repeatedly needs on every
//! network (mainnet, testnet, regtest, custom testnets): which upgrades are
//! activated, at what heights, and what is the active consensus branch id at
//! a given height. Mainnet and testnet have stable answers baked into
//! upstream zebra-chain; regtest and custom testnets are operator-configured
//! and must be discovered from the running node. To keep one path correct in
//! all cases, Zinder treats the running node as the source of truth and
//! caches the result in this owned type
//! ([../../../docs/architecture/chain-ingestion.md][cha]).
//!
//! Callers receive an [`Arc<NetworkUpgradeActivations>`] from process startup
//! and query it directly; no static singletons, no library-default fallbacks.
//!
//! [cha]: ../../../docs/architecture/chain-ingestion.md

use std::collections::BTreeSet;
use std::fmt;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::chain_epoch::{BlockHeight, Network};

const ACTIVATION_FINGERPRINT_DOMAIN: &[u8] =
    b"zinder:network-upgrade-activations:fingerprint:sha256\0";

/// Zcash consensus branch identifier (ZIP-200 §`CONSENSUS_BRANCH_ID`).
///
/// Network upgrades stamp every block and transaction with a 32-bit branch
/// identifier that anchors them to a specific set of consensus rules. Branch
/// identifiers are global protocol constants: a given upgrade carries the
/// same branch id on every network that adopts it, and Zinder treats the
/// value as opaque material discovered from the running node.
///
/// The bytes flow through the wire (`uint32`) and through `getblockchaininfo`
/// (hex string) unchanged; conversions happen at the boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsensusBranchId(u32);

impl ConsensusBranchId {
    /// Branch identifier reserved for the pre-Overwinter consensus rules.
    ///
    /// The wire protocol uses `0` to mean "no branch id is associated with
    /// this height" (block was mined before the first soft-fork). The
    /// wallet read API preserves the same convention.
    pub const PRE_OVERWINTER: Self = Self(0);

    /// Wraps a raw branch identifier.
    #[must_use]
    pub const fn new(branch_id: u32) -> Self {
        Self(branch_id)
    }

    /// Returns the raw 32-bit branch identifier.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

impl fmt::Display for ConsensusBranchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:#010x}", self.0)
    }
}

impl fmt::LowerHex for ConsensusBranchId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, formatter)
    }
}

/// Concrete table of network upgrades active on a given Zcash network, as
/// advertised by the running node.
///
/// Build with [`NetworkUpgradeActivations::new`] (in-memory construction) or
/// via the `fetch_network_upgrade_activations` helper in `zinder-source`,
/// which parses Zebra's `getblockchaininfo.upgrades` response. Once
/// constructed the table is immutable; create a new one to reflect operator
/// changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkUpgradeActivations {
    network: Network,
    /// Sorted by `activation_height` ascending. Branch ids are unique.
    activations: Vec<NetworkUpgradeActivation>,
}

/// A single entry in [`NetworkUpgradeActivations`]: one upgrade and the
/// height at which the running node activates it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NetworkUpgradeActivation {
    /// The consensus branch identifier assigned to this upgrade. Stable for
    /// the lifetime of the upgrade across all networks that adopt it.
    pub branch_id: ConsensusBranchId,
    /// The block height at which this upgrade's rules first apply on the
    /// node's network.
    pub activation_height: BlockHeight,
    /// The upgrade's canonical name as reported by the node (for example
    /// `"Sapling"`, `"NU5"`, `"NU6"`). Carried verbatim so unknown future
    /// upgrades remain serviceable without a Zinder code change.
    pub name: String,
}

/// Version of the immutable network-upgrade activation-table fingerprint.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum NetworkUpgradeActivationsFingerprintVersion {
    /// Initial domain-separated SHA-256 contract.
    V1,
}

impl NetworkUpgradeActivationsFingerprintVersion {
    /// Version emitted for newly created canonical stores.
    pub const CURRENT: Self = Self::V1;

    /// Returns the stable numeric encoding.
    #[must_use]
    pub const fn value(self) -> u16 {
        match self {
            Self::V1 => 1,
        }
    }
}

/// An encoded activation fingerprint version this binary does not support.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("unsupported network upgrade activations fingerprint version {encoded_version}")]
pub struct UnsupportedNetworkUpgradeActivationsFingerprintVersion {
    encoded_version: u16,
}

impl TryFrom<u16> for NetworkUpgradeActivationsFingerprintVersion {
    type Error = UnsupportedNetworkUpgradeActivationsFingerprintVersion;

    fn try_from(encoded_version: u16) -> Result<Self, Self::Error> {
        match encoded_version {
            1 => Ok(Self::V1),
            _ => Err(UnsupportedNetworkUpgradeActivationsFingerprintVersion { encoded_version }),
        }
    }
}

/// Immutable identity of one node-discovered activation table.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NetworkUpgradeActivationsFingerprint {
    version: NetworkUpgradeActivationsFingerprintVersion,
    bytes: [u8; 32],
}

impl NetworkUpgradeActivationsFingerprint {
    /// Reconstructs a fingerprint from an admitted durable record.
    #[must_use]
    pub const fn from_bytes(
        version: NetworkUpgradeActivationsFingerprintVersion,
        bytes: [u8; 32],
    ) -> Self {
        Self { version, bytes }
    }

    /// Returns the fingerprint algorithm version.
    #[must_use]
    pub const fn version(self) -> NetworkUpgradeActivationsFingerprintVersion {
        self.version
    }

    /// Returns the domain-separated SHA-256 bytes.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 32] {
        self.bytes
    }
}

/// Validation failures encountered while constructing a
/// [`NetworkUpgradeActivations`].
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum NetworkUpgradeActivationsError {
    /// Two activations advertised the same `branch_id`. Branch identifiers
    /// are global, so this indicates a malformed node response.
    DuplicateBranchId {
        /// The branch identifier that appeared more than once.
        branch_id: ConsensusBranchId,
    },
}

impl fmt::Display for NetworkUpgradeActivationsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateBranchId { branch_id } => write!(
                formatter,
                "duplicate consensus branch id {branch_id} in network upgrade activations"
            ),
        }
    }
}

impl std::error::Error for NetworkUpgradeActivationsError {}

impl NetworkUpgradeActivations {
    /// Builds an activation table that advertises no upgrades.
    ///
    /// Convenience for callers that need a table-typed value but cannot
    /// resolve consensus branch ids themselves: the explorer plane uses
    /// this when parsing transactions whose branch id is supplied by the
    /// upstream wallet response. `consensus_branch_id_at` always returns
    /// [`ConsensusBranchId::PRE_OVERWINTER`] for an empty table.
    #[must_use]
    pub const fn empty(network: Network) -> Self {
        Self {
            network,
            activations: Vec::new(),
        }
    }

    /// Builds the activation table from an unsorted list.
    ///
    /// Stably sorts activations by `activation_height` ascending, preserving
    /// the caller's order among upgrades that share a height so [`active_at`]
    /// resolves same-height ties to the last-advertised upgrade. Returns
    /// [`NetworkUpgradeActivationsError::DuplicateBranchId`] if any branch
    /// id appears more than once.
    ///
    /// [`active_at`]: Self::active_at
    pub fn new(
        network: Network,
        mut activations: Vec<NetworkUpgradeActivation>,
    ) -> Result<Self, NetworkUpgradeActivationsError> {
        let mut seen = BTreeSet::new();
        for activation in &activations {
            if !seen.insert(activation.branch_id) {
                return Err(NetworkUpgradeActivationsError::DuplicateBranchId {
                    branch_id: activation.branch_id,
                });
            }
        }
        activations.sort_by_key(|activation| activation.activation_height.value());
        Ok(Self {
            network,
            activations,
        })
    }

    /// The network these activations describe.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.network
    }

    /// All activations, sorted by activation height ascending.
    #[must_use]
    pub fn activations(&self) -> &[NetworkUpgradeActivation] {
        &self.activations
    }

    /// Commits to the exact network and ordered activation table.
    ///
    /// Distinct activation heights are canonicalized by [`Self::new`]. The
    /// stable order of entries at the same height remains significant because
    /// it determines which branch is active at that height.
    #[must_use]
    pub fn fingerprint(
        &self,
        version: NetworkUpgradeActivationsFingerprintVersion,
    ) -> NetworkUpgradeActivationsFingerprint {
        fingerprint_activation_entries(self.network, &self.activations, version)
    }

    /// Returns the length of the leading entries whose fingerprint equals
    /// `fingerprint`, when this table extends that complete earlier table.
    ///
    /// A match proves an exact ordered prefix on the same network, not merely
    /// matching consensus behavior at selected heights, so it never admits an
    /// edit, removal, or reordering of an earlier entry.
    #[must_use]
    pub fn matching_fingerprint_prefix_len(
        &self,
        fingerprint: NetworkUpgradeActivationsFingerprint,
    ) -> Option<usize> {
        (0..=self.activations.len()).find(|&prefix_len| {
            fingerprint_activation_entries(
                self.network,
                &self.activations[..prefix_len],
                fingerprint.version(),
            ) == fingerprint
        })
    }

    /// Returns whether both tables describe the same network and carry
    /// identical entries at or below `height`, including the order of entries
    /// that share an activation height.
    #[must_use]
    pub fn preserves_through(&self, previous: &Self, height: BlockHeight) -> bool {
        self.network == previous.network
            && activations_through(&self.activations, height)
                .eq(activations_through(&previous.activations, height))
    }

    /// Returns the activation active at `height`: the entry with the largest
    /// `activation_height` such that `activation_height <= height`. When
    /// several upgrades share that height (regtest activates every upgrade at
    /// height 1), the one advertised last by the node wins, matching the
    /// node's own consensus-branch-id at the tip.
    ///
    /// Returns `None` if no advertised activation has yet activated at
    /// `height` (pre-Overwinter heights, when the node only reports
    /// post-Overwinter upgrades).
    #[must_use]
    pub fn active_at(&self, height: BlockHeight) -> Option<&NetworkUpgradeActivation> {
        self.activations
            .iter()
            .rev()
            .find(|activation| activation.activation_height.value() <= height.value())
    }

    /// Returns the consensus branch identifier active at `height`, or
    /// [`ConsensusBranchId::PRE_OVERWINTER`] when no upgrade is active yet.
    #[must_use]
    pub fn consensus_branch_id_at(&self, height: BlockHeight) -> ConsensusBranchId {
        self.active_at(height)
            .map_or(ConsensusBranchId::PRE_OVERWINTER, |activation| {
                activation.branch_id
            })
    }

    /// Returns the activation height for a given branch identifier, if
    /// advertised.
    #[must_use]
    pub fn activation_height_by_branch_id(
        &self,
        branch_id: ConsensusBranchId,
    ) -> Option<BlockHeight> {
        self.activations
            .iter()
            .find(|activation| activation.branch_id == branch_id)
            .map(|activation| activation.activation_height)
    }

    /// Returns the activation height of the upgrade named `name`
    /// (case-insensitive), if advertised. Used by protocol surfaces such as
    /// lightwalletd's `saplingActivationHeight` response.
    #[must_use]
    pub fn activation_height_by_name(&self, name: &str) -> Option<BlockHeight> {
        self.activations
            .iter()
            .find(|activation| activation.name.eq_ignore_ascii_case(name))
            .map(|activation| activation.activation_height)
    }
}

fn activations_through(
    activations: &[NetworkUpgradeActivation],
    height: BlockHeight,
) -> impl Iterator<Item = &NetworkUpgradeActivation> {
    activations
        .iter()
        .take_while(move |activation| activation.activation_height <= height)
}

fn fingerprint_activation_entries(
    network: Network,
    activations: &[NetworkUpgradeActivation],
    version: NetworkUpgradeActivationsFingerprintVersion,
) -> NetworkUpgradeActivationsFingerprint {
    let mut hasher = Sha256::new();
    hasher.update(ACTIVATION_FINGERPRINT_DOMAIN);
    hasher.update(version.value().to_le_bytes());
    hasher.update(network.id().to_le_bytes());
    hasher.update(
        u64::try_from(activations.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    for activation in activations {
        hasher.update(activation.branch_id.value().to_le_bytes());
        hasher.update(activation.activation_height.value().to_le_bytes());
        hasher.update(
            u64::try_from(activation.name.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(activation.name.as_bytes());
    }
    NetworkUpgradeActivationsFingerprint {
        version,
        bytes: hasher.finalize().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn sample_regtest_activations()
    -> Result<NetworkUpgradeActivations, NetworkUpgradeActivationsError> {
        NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0x76b8_09bb),
                    activation_height: BlockHeight::new(1),
                    name: "Sapling".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0xc2d6_d0b4),
                    activation_height: BlockHeight::new(2),
                    name: "NU5".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0xc8e7_1055),
                    activation_height: BlockHeight::new(2),
                    name: "NU6".to_owned(),
                },
            ],
        )
    }

    #[test]
    fn active_at_returns_none_before_any_activation() -> TestResult {
        let activations = sample_regtest_activations()?;
        assert!(activations.active_at(BlockHeight::new(0)).is_none());
        Ok(())
    }

    #[test]
    fn active_at_returns_highest_active_upgrade() -> TestResult {
        let activations = sample_regtest_activations()?;
        let current = activations
            .active_at(BlockHeight::new(7404))
            .ok_or("regtest tip must have at least one activation at or below")?;
        assert_eq!(current.branch_id, ConsensusBranchId::new(0xc8e7_1055));
        assert_eq!(current.name, "NU6");
        Ok(())
    }

    #[test]
    fn consensus_branch_id_at_is_pre_overwinter_below_floor() -> TestResult {
        let activations = sample_regtest_activations()?;
        assert_eq!(
            activations.consensus_branch_id_at(BlockHeight::new(0)),
            ConsensusBranchId::PRE_OVERWINTER
        );
        Ok(())
    }

    #[test]
    fn consensus_branch_id_at_matches_current() -> TestResult {
        let activations = sample_regtest_activations()?;
        assert_eq!(
            activations.consensus_branch_id_at(BlockHeight::new(7404)),
            ConsensusBranchId::new(0xc8e7_1055)
        );
        Ok(())
    }

    #[test]
    fn activation_height_by_branch_id_round_trips() -> TestResult {
        let activations = sample_regtest_activations()?;
        assert_eq!(
            activations.activation_height_by_branch_id(ConsensusBranchId::new(0xc2d6_d0b4)),
            Some(BlockHeight::new(2))
        );
        assert_eq!(
            activations.activation_height_by_branch_id(ConsensusBranchId::new(0xdead_beef)),
            None
        );
        Ok(())
    }

    #[test]
    fn activation_height_by_name_is_case_insensitive() -> TestResult {
        let activations = sample_regtest_activations()?;
        assert_eq!(
            activations.activation_height_by_name("sapling"),
            Some(BlockHeight::new(1))
        );
        assert_eq!(
            activations.activation_height_by_name("SAPLING"),
            Some(BlockHeight::new(1))
        );
        Ok(())
    }

    #[test]
    fn new_rejects_duplicate_branch_ids() {
        let outcome = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0xc8e7_1055),
                    activation_height: BlockHeight::new(1),
                    name: "First".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0xc8e7_1055),
                    activation_height: BlockHeight::new(2),
                    name: "Second".to_owned(),
                },
            ],
        );
        assert_eq!(
            outcome,
            Err(NetworkUpgradeActivationsError::DuplicateBranchId {
                branch_id: ConsensusBranchId::new(0xc8e7_1055),
            })
        );
    }

    #[test]
    fn new_sorts_activations_by_height() -> TestResult {
        let activations = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0xc8e7_1055),
                    activation_height: BlockHeight::new(2),
                    name: "NU6".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0x76b8_09bb),
                    activation_height: BlockHeight::new(1),
                    name: "Sapling".to_owned(),
                },
            ],
        )?;
        let names: Vec<&str> = activations
            .activations()
            .iter()
            .map(|activation| activation.name.as_str())
            .collect();
        assert_eq!(names, vec!["Sapling", "NU6"]);
        Ok(())
    }

    #[test]
    fn activation_fingerprint_v1_matches_known_answer() -> TestResult {
        let fingerprint = sample_regtest_activations()?
            .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
        assert_eq!(
            hex::encode(fingerprint.as_bytes()),
            "f6a66ed2897330b0f1237d37df807d9329225b23f56c67ad30877e10dddee24a"
        );
        assert_eq!(
            fingerprint.version(),
            NetworkUpgradeActivationsFingerprintVersion::V1
        );
        Ok(())
    }

    #[test]
    fn matching_fingerprint_prefix_proves_the_complete_earlier_table() -> TestResult {
        let earlier = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![activation(1, 1, "Sapling"), activation(2, 2, "NU5")],
        )?;
        let extended = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                activation(1, 1, "Sapling"),
                activation(2, 2, "NU5"),
                activation(3, 3, "NU6"),
            ],
        )?;
        let fingerprint = earlier.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
        assert_eq!(
            extended.matching_fingerprint_prefix_len(fingerprint),
            Some(2)
        );
        assert_eq!(
            earlier.matching_fingerprint_prefix_len(fingerprint),
            Some(2)
        );
        assert_eq!(
            NetworkUpgradeActivations::new(Network::ZcashTestnet, extended.activations().to_vec())?
                .matching_fingerprint_prefix_len(fingerprint),
            None
        );
        let renamed = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                activation(1, 1, "Sapling"),
                activation(2, 2, "nu5"),
                activation(3, 3, "NU6"),
            ],
        )?;
        assert_eq!(renamed.matching_fingerprint_prefix_len(fingerprint), None);
        Ok(())
    }

    #[test]
    fn preserves_through_compares_same_height_order_and_rejects_historical_edits() -> TestResult {
        let previous = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![activation(1, 1, "Sapling"), activation(2, 1, "NU5")],
        )?;
        let future_extension = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                activation(1, 1, "Sapling"),
                activation(2, 1, "NU5"),
                activation(3, 8, "NU6"),
            ],
        )?;
        let reordered = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![activation(2, 1, "NU5"), activation(1, 1, "Sapling")],
        )?;
        let other_network =
            NetworkUpgradeActivations::new(Network::ZcashTestnet, previous.activations().to_vec())?;
        assert!(future_extension.preserves_through(&previous, BlockHeight::new(7)));
        assert!(!future_extension.preserves_through(&previous, BlockHeight::new(8)));
        assert!(!reordered.preserves_through(&previous, BlockHeight::new(1)));
        assert!(reordered.preserves_through(&previous, BlockHeight::new(0)));
        assert!(!other_network.preserves_through(&previous, BlockHeight::new(0)));
        Ok(())
    }

    #[test]
    fn zebra_seven_tables_extend_zebra_six_tables_on_mainnet_and_testnet() -> TestResult {
        for (network, nu7_height) in [
            (Network::ZcashMainnet, ZEBRA_7_MAINNET_NU7_HEIGHT),
            (Network::ZcashTestnet, ZEBRA_7_TESTNET_NU7_HEIGHT),
        ] {
            let zebra_six = zebra_table(network, false)?;
            let zebra_seven = zebra_table(network, true)?;
            let baseline = zebra_six.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
            assert_eq!(
                zebra_seven.matching_fingerprint_prefix_len(baseline),
                Some(zebra_six.activations().len())
            );
            assert_eq!(
                zebra_seven.activation_height_by_branch_id(ConsensusBranchId::new(NU7_BRANCH_ID)),
                Some(BlockHeight::new(nu7_height))
            );
            let last_pre_nu7 = BlockHeight::new(nu7_height - 1);
            assert!(zebra_seven.preserves_through(&zebra_six, last_pre_nu7));
            assert!(!zebra_seven.preserves_through(&zebra_six, BlockHeight::new(nu7_height)));

            let mut reordered = zebra_seven.activations().to_vec();
            reordered.swap(0, 1);
            reordered[0].activation_height = reordered[1].activation_height;
            let reordered = NetworkUpgradeActivations::new(network, reordered)?;
            assert_eq!(reordered.matching_fingerprint_prefix_len(baseline), None);

            let mut moved_history = zebra_seven.activations().to_vec();
            if let Some(nu6_3) = moved_history.iter_mut().find(|entry| entry.name == "NU6.3") {
                nu6_3.activation_height = BlockHeight::new(nu6_3.activation_height.value() + 1);
            }
            let moved_history = NetworkUpgradeActivations::new(network, moved_history)?;
            assert_eq!(
                moved_history.matching_fingerprint_prefix_len(baseline),
                None
            );
            assert!(!moved_history.preserves_through(&zebra_six, last_pre_nu7));
        }
        Ok(())
    }

    #[test]
    fn activation_fingerprint_canonicalizes_distinct_height_input_order() -> TestResult {
        let sorted = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![
                activation(1, 1, "first"),
                activation(2, 2, "second"),
                activation(3, 3, "third"),
            ],
        )?;
        let reversed = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            sorted.activations().iter().cloned().rev().collect(),
        )?;
        assert_eq!(
            sorted.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
            reversed.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
        );
        Ok(())
    }

    #[test]
    fn activation_fingerprint_preserves_same_height_tie_order() -> TestResult {
        let first = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![activation(1, 1, "first"), activation(2, 1, "second")],
        )?;
        let second = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![activation(2, 1, "second"), activation(1, 1, "first")],
        )?;
        assert_ne!(
            first.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
            second.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
        );
        Ok(())
    }

    #[test]
    fn activation_fingerprint_commits_to_network_and_exact_fields() -> TestResult {
        let base = NetworkUpgradeActivations::new(
            Network::ZcashRegtest,
            vec![activation(1, 1, "Sapling")],
        )?;
        let variants = [
            NetworkUpgradeActivations::new(
                Network::ZcashTestnet,
                vec![activation(1, 1, "Sapling")],
            )?,
            NetworkUpgradeActivations::new(
                Network::ZcashRegtest,
                vec![activation(2, 1, "Sapling")],
            )?,
            NetworkUpgradeActivations::new(
                Network::ZcashRegtest,
                vec![activation(1, 2, "Sapling")],
            )?,
            NetworkUpgradeActivations::new(
                Network::ZcashRegtest,
                vec![activation(1, 1, "sapling")],
            )?,
        ];
        let base_fingerprint = base.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1);
        for variant in variants {
            assert_ne!(
                base_fingerprint,
                variant.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
            );
        }
        Ok(())
    }

    #[test]
    fn activation_fingerprint_version_rejects_unknown_values() {
        assert_eq!(
            NetworkUpgradeActivationsFingerprintVersion::try_from(2),
            Err(UnsupportedNetworkUpgradeActivationsFingerprintVersion { encoded_version: 2 })
        );
    }

    #[test]
    fn empty_activation_fingerprints_commit_to_network() {
        assert_ne!(
            NetworkUpgradeActivations::empty(Network::ZcashRegtest)
                .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
            NetworkUpgradeActivations::empty(Network::ZcashTestnet)
                .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
        );
    }

    const NU7_BRANCH_ID: u32 = 0x7719_0ad9;
    const ZEBRA_7_MAINNET_NU7_HEIGHT: u32 = 3_543_405;
    const ZEBRA_7_TESTNET_NU7_HEIGHT: u32 = 4_465_026;

    /// `getblockchaininfo.upgrades` as Zebra 6.2.2 advertises it, optionally
    /// followed by the NU7 row Zebra 7.0.0 appends.
    fn zebra_table(
        network: Network,
        with_nu7: bool,
    ) -> Result<NetworkUpgradeActivations, NetworkUpgradeActivationsError> {
        let (heights, nu7_height): ([u32; 10], u32) = if network == Network::ZcashMainnet {
            (
                [
                    347_500, 419_200, 653_600, 903_000, 1_046_400, 1_687_104, 2_726_400, 3_146_400,
                    3_364_600, 3_428_143,
                ],
                ZEBRA_7_MAINNET_NU7_HEIGHT,
            )
        } else {
            (
                [
                    207_500, 280_000, 584_000, 903_800, 1_028_500, 1_842_420, 2_976_000, 3_536_500,
                    4_052_000, 4_134_000,
                ],
                ZEBRA_7_TESTNET_NU7_HEIGHT,
            )
        };
        let upgrades = [
            (0x5ba8_1b19, "Overwinter"),
            (0x76b8_09bb, "Sapling"),
            (0x2bb4_0e60, "Blossom"),
            (0xf5b9_230b, "Heartwood"),
            (0xe9ff_75a6, "Canopy"),
            (0xc2d6_d0b4, "NU5"),
            (0xc8e7_1055, "NU6"),
            (0x4dec_4df0, "NU6.1"),
            (0x5437_f330, "NU6.2"),
            (0x37a5_165b, "NU6.3"),
        ];
        let mut entries: Vec<_> = upgrades
            .into_iter()
            .zip(heights)
            .map(|((branch_id, name), height)| activation(branch_id, height, name))
            .collect();
        if with_nu7 {
            entries.push(activation(NU7_BRANCH_ID, nu7_height, "NU7"));
        }
        NetworkUpgradeActivations::new(network, entries)
    }

    fn activation(branch_id: u32, height: u32, name: &str) -> NetworkUpgradeActivation {
        NetworkUpgradeActivation {
            branch_id: ConsensusBranchId::new(branch_id),
            activation_height: BlockHeight::new(height),
            name: name.to_owned(),
        }
    }
}
