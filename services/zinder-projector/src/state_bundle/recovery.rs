//! Read-only physical admission against the owner-issued capture manifest.

use std::path::Path;

use zinder_core::{
    CanonicalBlockFactsSequenceDigest, NetworkUpgradeActivationsFingerprintVersion,
    ShieldedProtocol,
};
use zinder_store::{CanonicalOwnerCheckpointEvidence, CanonicalReorgPolicy, RocksDbCanonicalStore};
use zinder_wallet_rocksdb::{RocksDbWalletFollowingStore, WalletRecoveryAdmissionConfig};

use super::{
    AdmittedScheduleIdentity, CANONICAL_CHECKPOINT_DIRECTORY_NAME, CanonicalAdmission,
    CanonicalBuildPlanAdmission, CanonicalCheckpointManifest, CanonicalFrontierAdmission,
    CanonicalHistoryPredecessorAdmission, StateBundleError, StateBundleFence, StateBundleManifest,
    WALLET_CHECKPOINT_DIRECTORY_NAME, WalletAdmission, WalletCheckpointManifest,
    require_manifest_value, sha256_hex, validate_exact_fence,
};

impl StateBundleManifest {
    #[cfg(test)]
    pub(crate) fn from_checkpoints(
        candidate_id: &str,
        canonical: &CanonicalOwnerCheckpointEvidence,
        wallet: &zinder_wallet_rocksdb::WalletOwnerCheckpointEvidence,
    ) -> Result<Self, StateBundleError> {
        Self::from_admitted(
            candidate_id,
            &CanonicalAdmission::try_from(canonical)?,
            &WalletAdmission::try_from(wallet)?,
        )
    }

    /// Verifies both database identities, their READY state, and the exact owner-captured fence.
    pub(crate) fn cold_admit_recovery_checkpoints(
        &self,
        root: &Path,
        wallet_admission: WalletRecoveryAdmissionConfig<'_>,
    ) -> Result<CanonicalOwnerCheckpointEvidence, StateBundleError> {
        let network = self.network()?;
        let plan = &self.canonical_checkpoint.build_plan;
        let window = self
            .canonical_checkpoint
            .effective_reorg_window_blocks
            .unwrap_or(plan.reorg_window_blocks);
        let policy = CanonicalReorgPolicy::new(window)
            .map_err(|error| StateBundleError::manifest_contract(error.to_string()))?;
        let canonical_checkpoint = RocksDbCanonicalStore::cold_admit_recovery_checkpoint(
            &root.join(CANONICAL_CHECKPOINT_DIRECTORY_NAME),
            network,
            plan.raw_blob_retention,
            policy,
        )
        .map_err(StateBundleError::CanonicalRecovery)?;
        let canonical = CanonicalAdmission::try_from(&canonical_checkpoint)?;
        require_manifest_value(
            CanonicalCheckpointManifest::from_admitted(&canonical) == self.canonical_checkpoint
                && StateBundleFence::from_canonical(&canonical) == self.fence,
            "canonical checkpoint and captured manifest disagree",
        )?;
        let observed = RocksDbWalletFollowingStore::cold_admit_recovery_checkpoint(
            root.join(WALLET_CHECKPOINT_DIRECTORY_NAME),
            network,
            wallet_admission,
        )
        .map_err(StateBundleError::WalletRecovery)?;
        let wallet = WalletAdmission::try_from(&observed)?;
        require_manifest_value(
            WalletCheckpointManifest::from_admitted(&wallet)? == self.wallet_checkpoint,
            "wallet checkpoint and captured manifest disagree",
        )?;
        validate_exact_fence(&canonical, &wallet)?;
        Ok(canonical_checkpoint)
    }
}

impl TryFrom<&CanonicalOwnerCheckpointEvidence> for CanonicalAdmission {
    type Error = StateBundleError;

    fn try_from(evidence: &CanonicalOwnerCheckpointEvidence) -> Result<Self, Self::Error> {
        let schedule = evidence.activation_schedule.as_ref().ok_or_else(|| {
            StateBundleError::manifest_contract(
                "canonical checkpoint has no admitted network-upgrade schedule",
            )
        })?;
        let ready = evidence.ready_evidence;
        let sequence_checkpoint = ready.sequence_checkpoint;
        Ok(Self {
            effective_reorg_window_blocks: (evidence.reorg_policy
                != evidence.build_plan.reorg_policy())
            .then(|| evidence.reorg_policy.reorg_window_blocks()),
            admitted_schedule: AdmittedScheduleIdentity {
                revision: schedule.revision(),
                fingerprint: schedule
                    .activations()
                    .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
                    .as_bytes(),
            },
            database_identity_sha256: sha256_hex(&evidence.database_identity),
            construction_manifest_version: ready.construction_manifest_version,
            construction_manifest_sha256: ready.construction_manifest_sha256,
            store_identity: evidence.store_identity.to_owned(),
            schema_version: evidence.schema_version,
            network: evidence.build_plan.network(),
            workload: evidence.workload,
            build_plan: CanonicalBuildPlanAdmission::from(evidence.build_plan.clone()),
            first_retained_block: ready.first_retained_block,
            visible_tip: ready.visible_tip,
            visible_epoch: ready.visible_epoch.value(),
            visible_event_sequence: ready.visible_event_sequence,
            visible_block_count: ready.visible_block_count,
            block_digest_version: ready.block_digest_version.value(),
            replay_format_version: ready.replay_format_version.value(),
            visible_sequence_digest:
                CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    ready.sequence_digest_version,
                    ready.visible_block_count,
                    ready.visible_sequence_digest,
                ),
            visible_logical_replay_bytes: ready.visible_logical_replay_bytes,
            settled_tip: sequence_checkpoint.through(),
            settled_retained_block_count: sequence_checkpoint.retained_block_count(),
            settled_sequence_digest: sequence_checkpoint.sequence_digest(),
            settled_logical_replay_bytes: sequence_checkpoint.logical_replay_bytes(),
        })
    }
}

impl From<zinder_store::CanonicalStoreBuildPlan> for CanonicalBuildPlanAdmission {
    fn from(build_plan: zinder_store::CanonicalStoreBuildPlan) -> Self {
        let fingerprint = build_plan.network_upgrade_activations_fingerprint();
        let predecessor = build_plan.history_predecessor();
        Self {
            activation_fingerprint_version: fingerprint.version().value(),
            activation_fingerprint: fingerprint.as_bytes(),
            raw_blob_retention: build_plan.raw_blob_retention(),
            reorg_window_blocks: build_plan.reorg_policy().reorg_window_blocks(),
            history_preceding_checkpoint: build_plan.history_bounds().preceding_checkpoint(),
            history_predecessor: CanonicalHistoryPredecessorAdmission {
                block_id: predecessor.block_id,
                block_time_seconds: predecessor.block_time_seconds,
                sapling_frontier: predecessor
                    .frontiers
                    .get(ShieldedProtocol::Sapling)
                    .map(CanonicalFrontierAdmission::from),
                orchard_frontier: predecessor
                    .frontiers
                    .get(ShieldedProtocol::Orchard)
                    .map(CanonicalFrontierAdmission::from),
                ironwood_frontier: predecessor
                    .frontiers
                    .get(ShieldedProtocol::Ironwood)
                    .map(CanonicalFrontierAdmission::from),
            },
            build_tip: build_plan.build_tip(),
        }
    }
}

impl From<&zinder_core::CommitmentTreeFrontier> for CanonicalFrontierAdmission {
    fn from(frontier: &zinder_core::CommitmentTreeFrontier) -> Self {
        Self {
            final_root: frontier.final_root().as_bytes(),
            final_state: frontier.final_state_bytes().to_vec(),
        }
    }
}
