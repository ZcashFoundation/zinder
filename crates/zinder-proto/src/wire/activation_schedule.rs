//! Wire translation for writer-admitted network-upgrade schedules.

use thiserror::Error;
use zinder_core::{
    BlockHeight, ConsensusBranchId, Network, NetworkUpgradeActivation, NetworkUpgradeActivations,
    NetworkUpgradeActivationsError, NetworkUpgradeActivationsFingerprintVersion,
};

use crate::v1::ingest::{CanonicalNetworkUpgradeActivation, CanonicalNetworkUpgradeSchedule};

/// Invalid writer-admitted schedule evidence on the wire.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ActivationScheduleWireError {
    /// Admitted schedule revisions start at 1.
    #[error("activation schedule revision must be nonzero")]
    ZeroRevision,
    /// The fingerprint algorithm version is not supported by this release.
    #[error("unsupported activation schedule fingerprint version {fingerprint_version}")]
    UnsupportedFingerprintVersion {
        /// Version carried on the wire.
        fingerprint_version: u32,
    },
    /// The activation entries violate the table invariants.
    #[error("activation schedule table is invalid: {0}")]
    Table(#[from] NetworkUpgradeActivationsError),
    /// The fingerprint does not authenticate the carried table on the
    /// expected network.
    #[error("activation schedule fingerprint does not authenticate its activation table")]
    FingerprintMismatch,
}

/// Encodes one complete admitted table with its writer-owned revision.
#[must_use]
pub fn encode_canonical_network_upgrade_schedule(
    activations: &NetworkUpgradeActivations,
    revision: u64,
) -> CanonicalNetworkUpgradeSchedule {
    let fingerprint = activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::CURRENT);
    CanonicalNetworkUpgradeSchedule {
        revision,
        fingerprint_version: u32::from(fingerprint.version().value()),
        fingerprint: fingerprint.as_bytes().to_vec(),
        activations: activations
            .activations()
            .iter()
            .map(|activation| CanonicalNetworkUpgradeActivation {
                branch_id: activation.branch_id.value(),
                height: activation.activation_height.value(),
                name: activation.name.clone(),
            })
            .collect(),
    }
}

/// Decodes an admitted schedule and authenticates its table against
/// `expected_network` and the carried fingerprint.
///
/// Returns the table and its revision.
pub fn decode_canonical_network_upgrade_schedule(
    message: CanonicalNetworkUpgradeSchedule,
    expected_network: Network,
) -> Result<(NetworkUpgradeActivations, u64), ActivationScheduleWireError> {
    if message.revision == 0 {
        return Err(ActivationScheduleWireError::ZeroRevision);
    }
    let fingerprint_version = u16::try_from(message.fingerprint_version)
        .ok()
        .and_then(|version| NetworkUpgradeActivationsFingerprintVersion::try_from(version).ok())
        .ok_or(ActivationScheduleWireError::UnsupportedFingerprintVersion {
            fingerprint_version: message.fingerprint_version,
        })?;
    let activations = NetworkUpgradeActivations::new(
        expected_network,
        message
            .activations
            .into_iter()
            .map(|activation| NetworkUpgradeActivation {
                branch_id: ConsensusBranchId::new(activation.branch_id),
                activation_height: BlockHeight::new(activation.height),
                name: activation.name,
            })
            .collect(),
    )?;
    if activations.fingerprint(fingerprint_version).as_bytes() != message.fingerprint.as_slice() {
        return Err(ActivationScheduleWireError::FingerprintMismatch);
    }
    Ok((activations, message.revision))
}

#[cfg(test)]
mod tests {
    use zinder_core::{
        BlockHeight, ConsensusBranchId, Network, NetworkUpgradeActivation,
        NetworkUpgradeActivations,
    };

    use super::{
        ActivationScheduleWireError, decode_canonical_network_upgrade_schedule,
        encode_canonical_network_upgrade_schedule,
    };

    fn nu7_schedule()
    -> Result<NetworkUpgradeActivations, zinder_core::NetworkUpgradeActivationsError> {
        NetworkUpgradeActivations::new(
            Network::ZcashMainnet,
            vec![
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0x37a5_165b),
                    activation_height: BlockHeight::new(3_428_143),
                    name: "NU6.3".to_owned(),
                },
                NetworkUpgradeActivation {
                    branch_id: ConsensusBranchId::new(0x7719_0ad9),
                    activation_height: BlockHeight::new(3_543_405),
                    name: "NU7".to_owned(),
                },
            ],
        )
    }

    #[test]
    fn admitted_schedule_round_trips_with_revision() -> Result<(), Box<dyn std::error::Error>> {
        let activations = nu7_schedule()?;
        let message = encode_canonical_network_upgrade_schedule(&activations, 3);
        let (decoded, revision) =
            decode_canonical_network_upgrade_schedule(message, Network::ZcashMainnet)?;
        assert_eq!(decoded, activations);
        assert_eq!(revision, 3);
        Ok(())
    }

    #[test]
    fn admitted_schedule_rejects_tampering_wrong_network_and_unknown_versions()
    -> Result<(), Box<dyn std::error::Error>> {
        let message = encode_canonical_network_upgrade_schedule(&nu7_schedule()?, 1);

        let mut renamed = message.clone();
        if let Some(last) = renamed.activations.last_mut() {
            last.name = "NU8".to_owned();
        }
        assert!(matches!(
            decode_canonical_network_upgrade_schedule(renamed, Network::ZcashMainnet),
            Err(ActivationScheduleWireError::FingerprintMismatch)
        ));
        assert!(matches!(
            decode_canonical_network_upgrade_schedule(message.clone(), Network::ZcashTestnet),
            Err(ActivationScheduleWireError::FingerprintMismatch)
        ));

        let mut zero_revision = message.clone();
        zero_revision.revision = 0;
        assert!(matches!(
            decode_canonical_network_upgrade_schedule(zero_revision, Network::ZcashMainnet),
            Err(ActivationScheduleWireError::ZeroRevision)
        ));

        let mut future_version = message;
        future_version.fingerprint_version = 2;
        assert!(matches!(
            decode_canonical_network_upgrade_schedule(future_version, Network::ZcashMainnet),
            Err(ActivationScheduleWireError::UnsupportedFingerprintVersion {
                fingerprint_version: 2
            })
        ));
        Ok(())
    }
}
