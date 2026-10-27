use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use rust_rocksdb::{
    BoundColumnFamily, DB, DEFAULT_COLUMN_FAMILY_NAME, FlushOptions, ReadOptions, WriteBatch,
    WriteOptions,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpochId, ChainTipMetadata,
    ShieldedProtocol, UnixTimestampMillis,
};

use crate::{BoundedRocksDbOpen, RocksDbOpenRole, open_bounded_rocksdb};

use super::{
    CanonicalEventFence, CanonicalSequenceCheckpoint, CanonicalStoreBuildState,
    CanonicalStoreError, CanonicalStoreReadyEvidence, RocksDbCanonicalBuilder,
    RocksDbCanonicalStore,
    block_load::{
        CanonicalBlockLoadEvidence, CanonicalTrustedFreshBlockEvidence,
        read_persisted_tip_metadata, validate_persisted_commitment_tree_families,
        validate_source_tip_checkpoint,
    },
    block_replay::{
        PersistedBlockReplayValidation, validate_persisted_block_replays_with_checkpoints,
    },
    construction_manifest::{
        CanonicalConstructionFamilyEvidence, CanonicalConstructionManifestDraft,
        CanonicalConstructionManifestInputs,
    },
    control::{DecodedStoreControl, decode_store_control, encode_ready_store_control},
    displaced_archive::validate_permanent_reorg_archive,
    replay_tail::AuthenticatedReplayTail,
    rocksdb::{
        BLOCK_BLOB_COLUMN_FAMILY, BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, CANONICAL_DATA_COLUMN_FAMILIES,
        CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY, COMPACT_BLOCK_COLUMN_FAMILY,
        CanonicalStoreAdmissionExpectation, DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY,
        DISPLACED_BLOCK_FACTS_COLUMN_FAMILY, MEMPOOL_EVENT_COLUMN_FAMILY, STORE_CONTROL_KEY,
        TRANSACTION_BLOB_COLUMN_FAMILY, TRANSACTION_LOCATION_COLUMN_FAMILY,
        TREE_STATE_CHECKPOINT_COLUMN_FAMILY, admit_existing_store,
        canonical_column_family_descriptors, validate_open_store_control,
    },
    subtree_load::{retained_block_hash, validate_persisted_subtree_root_family},
};

const BASELINE_EPOCH_ID: ChainEpochId = ChainEpochId::new(1);
const BASELINE_EVENT_SEQUENCE: u64 = 1;
const VERSION_ONE: u8 = 1;
const COMMITTED_EVENT: u8 = 1;
const REORG_EVENT: u8 = 2;
const REVERTED_RANGE_ABSENT: u8 = 0;
const REVERTED_RANGE_PRESENT: u8 = 1;
const EPOCH_VALUE_LENGTH: usize = 1 + 4 + 32 + 4 + 32 + 12 + 8;
const EVENT_VALUE_LENGTH: usize = 1 + 1 + 8 + 8 + 1 + 4 + 4 + 4 + 4 + 4 + 32 + 8 + 32;

/// Explicit settlement and observation time for the first visible canonical epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalBaselinePublication {
    /// Highest block whose canonical identity is settled under the caller's reorg policy.
    pub settled_tip: BlockId,
    /// Wall-clock time at which epoch 1 became visible.
    pub created_at: UnixTimestampMillis,
}

impl CanonicalBaselinePublication {
    /// Creates the explicit baseline publication input.
    #[must_use]
    pub const fn new(settled_tip: BlockId, created_at: UnixTimestampMillis) -> Self {
        Self {
            settled_tip,
            created_at,
        }
    }
}

/// Exclusive owner of a reopened canonical v1 build prepared for publication.
///
/// Trusted fresh-writer preparation and explicit cold certification are
/// separate builder APIs that both produce this publication capability.
pub struct ValidatedRocksDbCanonicalBuild {
    bounded_open: BoundedRocksDbOpen,
    workload: super::CanonicalStoreWorkload,
    build_plan: super::CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
    ready_evidence: CanonicalStoreReadyEvidence,
    construction_manifest: CanonicalConstructionManifestDraft,
    retained_sequence_checkpoints: VecDeque<CanonicalSequenceCheckpoint>,
    tip_metadata: ChainTipMetadata,
}

/// Baseline input checked against one specific cold-validated canonical build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreparedCanonicalBaselinePublication {
    publication: CanonicalBaselinePublication,
    build_tip: BlockId,
    source_sequence_digest: [u8; 32],
    sequence_checkpoint: CanonicalSequenceCheckpoint,
}

struct EncodedBaselinePublication {
    epoch: [u8; EPOCH_VALUE_LENGTH],
    event: [u8; EVENT_VALUE_LENGTH],
    control: Vec<u8>,
}

struct PublicationContext {
    store_path: std::path::PathBuf,
    resource_budget: crate::RocksDbResourceBudget,
    network: zinder_core::Network,
    workload: super::CanonicalStoreWorkload,
    build_plan: super::CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
    block_evidence: CanonicalBlockLoadEvidence,
    trusted_fresh_block_evidence: CanonicalTrustedFreshBlockEvidence,
    staged_ssts: Vec<super::block_load::CanonicalStagedSstEvidence>,
    subtree_root_evidence: super::CanonicalSubtreeRootLoadEvidence,
    trusted_fresh_subtree_family_evidence: CanonicalConstructionFamilyEvidence,
    source_tip_checkpoint: zinder_core::CommitmentTreeCheckpoint,
}

impl RocksDbCanonicalBuilder {
    /// Prepares publication from evidence owned by this one-shot fresh writer.
    ///
    /// This path still flushes, closes, reopens, and verifies immutable identity
    /// and BUILDING control. It validates family boundaries and authenticated tip
    /// points without rescanning complete historical families.
    pub fn prepare_trusted_fresh_publication(
        self,
    ) -> Result<ValidatedRocksDbCanonicalBuild, CanonicalStoreError> {
        let context = PublicationContext::from_builder(&self)?;
        flush_complete_build(&self.bounded_open.db)?;
        let built_database_identity = self.bounded_open.db.get_db_identity().map_err(|source| {
            CanonicalStoreError::RocksDbOperation {
                operation: "publication database identity read",
                source,
            }
        })?;
        abort_at_publication_failpoint("after_flush");
        drop(self);
        abort_at_publication_failpoint("after_builder_drop");
        context.prepare_trusted_fresh_reopen(&built_database_identity)
    }

    /// Flushes, closes, cold-reopens, and independently certifies a complete v1 build.
    pub fn prepare_cold_certified_publication(
        self,
    ) -> Result<ValidatedRocksDbCanonicalBuild, CanonicalStoreError> {
        let context = PublicationContext::from_builder(&self)?;
        flush_complete_build(&self.bounded_open.db)?;
        let built_database_identity = self.bounded_open.db.get_db_identity().map_err(|source| {
            CanonicalStoreError::RocksDbOperation {
                operation: "publication database identity read",
                source,
            }
        })?;
        abort_at_publication_failpoint("after_flush");
        drop(self);
        abort_at_publication_failpoint("after_builder_drop");
        context.validate_cold_reopen(&built_database_identity)
    }
}

impl PublicationContext {
    fn from_builder(builder: &RocksDbCanonicalBuilder) -> Result<Self, CanonicalStoreError> {
        if builder.workload == super::CanonicalStoreWorkload::Explorer {
            return Err(CanonicalStoreError::publication(
                "explorer publication requires authenticated daily value-pool evidence",
            ));
        }
        let block_evidence = builder.canonical_block_evidence.ok_or_else(|| {
            CanonicalStoreError::publication("canonical block families were not loaded")
        })?;
        let staged_ssts = builder.canonical_block_staged_ssts.clone().ok_or_else(|| {
            CanonicalStoreError::publication(
                "canonical block families have no staged SST construction evidence",
            )
        })?;
        let trusted_fresh_block_evidence = builder
            .trusted_fresh_block_evidence
            .clone()
            .ok_or_else(|| {
                CanonicalStoreError::publication(
                    "canonical block families have no trusted fresh-writer evidence",
                )
            })?;
        let subtree_root_evidence = builder.subtree_root_evidence.ok_or_else(|| {
            CanonicalStoreError::publication("canonical subtree-root ranges were not loaded")
        })?;
        let trusted_fresh_subtree_family_evidence = builder
            .trusted_fresh_subtree_family_evidence
            .clone()
            .ok_or_else(|| {
                CanonicalStoreError::publication(
                    "canonical subtree roots have no trusted fresh-writer evidence",
                )
            })?;
        let source_tip_checkpoint =
            builder
                .confirmed_source_tip_checkpoint
                .clone()
                .ok_or_else(|| {
                    CanonicalStoreError::publication(
                        "the fixed canonical tip was not confirmed by the node source",
                    )
                })?;
        Ok(Self {
            store_path: builder.store_path.clone(),
            resource_budget: builder.resource_budget,
            network: builder.network,
            workload: builder.workload,
            build_plan: builder.build_plan.clone(),
            cursor_auth_key: builder.cursor_auth_key,
            block_evidence,
            trusted_fresh_block_evidence,
            staged_ssts,
            subtree_root_evidence,
            trusted_fresh_subtree_family_evidence,
            source_tip_checkpoint,
        })
    }

    fn prepare_trusted_fresh_reopen(
        self,
        built_database_identity: &[u8],
    ) -> Result<ValidatedRocksDbCanonicalBuild, CanonicalStoreError> {
        let bounded_open = self.reopen_complete_build(built_database_identity)?;
        validate_source_tip_checkpoint(
            &bounded_open.db,
            &self.build_plan,
            &self.block_evidence,
            &self.source_tip_checkpoint,
        )?;
        validate_trusted_fresh_tip_points(
            &bounded_open.db,
            &self.build_plan,
            &self.block_evidence,
        )?;

        let CanonicalTrustedFreshBlockEvidence {
            mut family_evidence,
            retained_sequence_checkpoints,
            logical_replay_bytes,
        } = self.trusted_fresh_block_evidence;
        family_evidence.push(self.trusted_fresh_subtree_family_evidence);
        for name in [
            DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY,
            CHAIN_EPOCH_COLUMN_FAMILY,
            CHAIN_EVENT_COLUMN_FAMILY,
            MEMPOOL_EVENT_COLUMN_FAMILY,
            DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
        ] {
            family_evidence.push(CanonicalConstructionFamilyEvidence::accumulator(name).finish());
        }
        validate_trusted_fresh_family_boundaries(&bounded_open.db, &family_evidence)?;

        let construction_manifest = CanonicalConstructionManifestDraft::new_trusted_fresh(
            CanonicalConstructionManifestInputs {
                build_plan: self.build_plan.clone(),
                workload: self.workload,
                source_checkpoint: self.source_tip_checkpoint,
                block_evidence: self.block_evidence,
                subtree_evidence: self.subtree_root_evidence,
                family_evidence,
                staged_ssts: self.staged_ssts,
            },
        )?;
        let ready_evidence = ready_evidence_from_trusted_fresh(
            &self.block_evidence,
            logical_replay_bytes,
            &retained_sequence_checkpoints,
        )?;
        abort_at_publication_failpoint("after_cold_validation");
        Ok(ValidatedRocksDbCanonicalBuild {
            bounded_open,
            workload: self.workload,
            build_plan: self.build_plan,
            cursor_auth_key: self.cursor_auth_key,
            ready_evidence,
            construction_manifest,
            retained_sequence_checkpoints,
            tip_metadata: self.block_evidence.tip_metadata,
        })
    }

    fn validate_cold_reopen(
        self,
        built_database_identity: &[u8],
    ) -> Result<ValidatedRocksDbCanonicalBuild, CanonicalStoreError> {
        let bounded_open = self.reopen_complete_build(built_database_identity)?;
        let cold_validation = validate_cold_block_families(
            &bounded_open.db,
            self.workload,
            &self.build_plan,
            &self.block_evidence,
        )?;
        validate_cold_evidence_matches_writer(
            &cold_validation.family_evidence,
            &self.trusted_fresh_block_evidence.family_evidence,
        )?;
        let persisted_subtree_validation = validate_persisted_subtree_root_family(
            &bounded_open.db,
            &self.build_plan,
            &self.block_evidence,
            self.subtree_root_evidence,
        )?;
        let persisted_subtree_evidence = persisted_subtree_validation.evidence;
        if persisted_subtree_evidence != self.subtree_root_evidence {
            return Err(CanonicalStoreError::publication(
                "cold subtree-root evidence differs from the authenticated source load",
            ));
        }
        if persisted_subtree_validation.family_evidence
            != self.trusted_fresh_subtree_family_evidence
        {
            return Err(CanonicalStoreError::publication(
                "cold subtree-root family evidence differs from the trusted writer evidence",
            ));
        }
        validate_source_tip_checkpoint(
            &bounded_open.db,
            &self.build_plan,
            &self.block_evidence,
            &self.source_tip_checkpoint,
        )?;
        let mut family_evidence = cold_validation.family_evidence;
        family_evidence.push(persisted_subtree_validation.family_evidence);
        let construction_manifest = CanonicalConstructionManifestDraft::new_cold_certified(
            CanonicalConstructionManifestInputs {
                build_plan: self.build_plan.clone(),
                workload: self.workload,
                source_checkpoint: self.source_tip_checkpoint,
                block_evidence: self.block_evidence,
                subtree_evidence: self.subtree_root_evidence,
                family_evidence,
                staged_ssts: self.staged_ssts,
            },
        )?;
        let ready_evidence = ready_evidence_from_cold_replay(&cold_validation.replay)?;
        abort_at_publication_failpoint("after_cold_validation");
        Ok(ValidatedRocksDbCanonicalBuild {
            bounded_open,
            workload: self.workload,
            build_plan: self.build_plan,
            cursor_auth_key: self.cursor_auth_key,
            ready_evidence,
            construction_manifest,
            retained_sequence_checkpoints: cold_validation.replay.retained_sequence_checkpoints,
            tip_metadata: self.block_evidence.tip_metadata,
        })
    }

    fn expected_building_control(&self) -> DecodedStoreControl {
        DecodedStoreControl {
            network: self.network,
            workload: self.workload,
            build_plan: self.build_plan.clone(),
            cursor_auth_key: self.cursor_auth_key,
            build_state: CanonicalStoreBuildState::Building,
            activation_schedule_binding: None,
            revised_reorg_policy: None,
        }
    }

    fn reopen_complete_build(
        &self,
        built_database_identity: &[u8],
    ) -> Result<BoundedRocksDbOpen, CanonicalStoreError> {
        let expected_control = self.expected_building_control();
        let expectation = CanonicalStoreAdmissionExpectation::from_build_plan(
            &self.build_plan,
            self.workload,
            self.build_plan.reorg_policy(),
        );
        let (admitted_database_identity, admitted_control) =
            admit_existing_store(&self.store_path, &expectation)?;
        if admitted_database_identity.as_slice() != built_database_identity
            || admitted_control != expected_control
        {
            return Err(CanonicalStoreError::publication(
                "database identity or BUILDING control changed before publication reopen",
            ));
        }
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary {
                path: &self.store_path,
            },
            self.resource_budget,
            canonical_column_family_descriptors,
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "publication reopen",
            source,
        })?;
        validate_reopened_identity_and_control(
            &bounded_open.db,
            &self.store_path,
            built_database_identity,
            &expected_control,
        )?;
        Ok(bounded_open)
    }
}

fn ready_evidence_from_cold_replay(
    validation: &PersistedBlockReplayValidation,
) -> Result<CanonicalStoreReadyEvidence, CanonicalStoreError> {
    let evidence = &validation.evidence;
    let sequence_checkpoint = validation
        .retained_sequence_checkpoints
        .back()
        .copied()
        .ok_or_else(|| {
            CanonicalStoreError::publication(
                "cold replay validation did not retain the visible prefix",
            )
        })?;
    Ok(CanonicalStoreReadyEvidence {
        first_retained_block: BlockId::new(evidence.first_height, evidence.first_hash),
        visible_tip: BlockId::new(evidence.tip_height, evidence.tip_hash),
        visible_epoch: BASELINE_EPOCH_ID,
        visible_event_sequence: BASELINE_EVENT_SEQUENCE,
        visible_block_count: evidence.block_count,
        block_digest_version: evidence.block_digest_version,
        replay_format_version: evidence.replay_format_version,
        sequence_digest_version: evidence.sequence_digest_version,
        visible_sequence_digest: evidence.sequence_digest.as_bytes(),
        visible_logical_replay_bytes: evidence.logical_replay_bytes,
        sequence_checkpoint,
        construction_manifest_version: 0,
        construction_manifest_sha256: [0; 32],
    })
}

fn ready_evidence_from_trusted_fresh(
    evidence: &CanonicalBlockLoadEvidence,
    logical_replay_bytes: u64,
    retained_sequence_checkpoints: &VecDeque<CanonicalSequenceCheckpoint>,
) -> Result<CanonicalStoreReadyEvidence, CanonicalStoreError> {
    let sequence_checkpoint = retained_sequence_checkpoints
        .back()
        .copied()
        .ok_or_else(|| {
            CanonicalStoreError::publication(
                "trusted fresh-writer evidence did not retain the visible prefix",
            )
        })?;
    let visible_tip = BlockId::new(evidence.tip_height, evidence.tip_hash);
    if sequence_checkpoint.through() != visible_tip
        || sequence_checkpoint.retained_block_count() != evidence.block_count
        || sequence_checkpoint.sequence_digest() != evidence.sequence_digest
        || sequence_checkpoint.logical_replay_bytes() != logical_replay_bytes
    {
        return Err(CanonicalStoreError::publication(
            "trusted fresh-writer sequence checkpoint differs from the block load",
        ));
    }
    Ok(CanonicalStoreReadyEvidence {
        first_retained_block: BlockId::new(evidence.first_height, evidence.first_hash),
        visible_tip,
        visible_epoch: BASELINE_EPOCH_ID,
        visible_event_sequence: BASELINE_EVENT_SEQUENCE,
        visible_block_count: evidence.block_count,
        block_digest_version: evidence.block_digest_version,
        replay_format_version: evidence.replay_format_version,
        sequence_digest_version: evidence.sequence_digest_version,
        visible_sequence_digest: evidence.sequence_digest.as_bytes(),
        visible_logical_replay_bytes: logical_replay_bytes,
        sequence_checkpoint,
        construction_manifest_version: 0,
        construction_manifest_sha256: [0; 32],
    })
}

fn validate_trusted_fresh_tip_points(
    db: &DB,
    build_plan: &super::CanonicalStoreBuildPlan,
    evidence: &CanonicalBlockLoadEvidence,
) -> Result<(), CanonicalStoreError> {
    let first = BlockId::new(evidence.first_height, evidence.first_hash);
    let tip = BlockId::new(evidence.tip_height, evidence.tip_hash);
    if first.height != build_plan.history_bounds().first_available_height()
        || tip != build_plan.build_tip()
        || retained_block_hash(db, first.height)? != first.hash
        || retained_block_hash(db, tip.height)? != tip.hash
        || read_persisted_tip_metadata(db, tip)? != evidence.tip_metadata
    {
        return Err(CanonicalStoreError::publication(
            "trusted fresh-writer boundary points differ from the source load",
        ));
    }
    Ok(())
}

fn validate_trusted_fresh_family_boundaries(
    db: &DB,
    families: &[CanonicalConstructionFamilyEvidence],
) -> Result<(), CanonicalStoreError> {
    for evidence in families {
        let family = db.cf_handle(evidence.family).ok_or_else(|| {
            CanonicalStoreError::publication(format!(
                "{} column family is absent during trusted fresh publication",
                evidence.family
            ))
        })?;
        match (&evidence.first_key, &evidence.last_key, evidence.row_count) {
            (None, None, 0) => {
                let mut rows = db.raw_iterator_cf(&family);
                rows.seek_to_first();
                if rows.valid() {
                    return Err(CanonicalStoreError::publication(format!(
                        "{} must be empty during trusted fresh publication",
                        evidence.family
                    )));
                }
                rows.status()
                    .map_err(|source| CanonicalStoreError::RocksDbOperation {
                        operation: "trusted fresh empty-family boundary validation",
                        source,
                    })?;
            }
            (Some(first_key), Some(last_key), row_count) if row_count > 0 => {
                let first_present = db
                    .get_cf(&family, first_key)
                    .map_err(|source| CanonicalStoreError::RocksDbOperation {
                        operation: "trusted fresh first-family boundary validation",
                        source,
                    })?
                    .is_some();
                let last_present = db
                    .get_cf(&family, last_key)
                    .map_err(|source| CanonicalStoreError::RocksDbOperation {
                        operation: "trusted fresh last-family boundary validation",
                        source,
                    })?
                    .is_some();
                if !first_present || !last_present {
                    return Err(CanonicalStoreError::publication(format!(
                        "{} boundary row is absent during trusted fresh publication",
                        evidence.family
                    )));
                }
            }
            _ => {
                return Err(CanonicalStoreError::publication(
                    "trusted fresh family evidence has inconsistent boundaries",
                ));
            }
        }
    }
    Ok(())
}

fn validate_cold_evidence_matches_writer(
    cold: &[CanonicalConstructionFamilyEvidence],
    writer: &[CanonicalConstructionFamilyEvidence],
) -> Result<(), CanonicalStoreError> {
    for writer_family in writer {
        let cold_family = cold
            .iter()
            .find(|cold_family| cold_family.family == writer_family.family)
            .ok_or_else(|| {
                CanonicalStoreError::publication(format!(
                    "trusted writer {} evidence has no cold certification",
                    writer_family.family
                ))
            })?;
        if cold_family != writer_family {
            return Err(CanonicalStoreError::publication(format!(
                "cold {} certification differs from trusted writer evidence",
                cold_family.family
            )));
        }
    }
    Ok(())
}

impl ValidatedRocksDbCanonicalBuild {
    /// Validates baseline settlement and time without consuming the expensive build.
    pub fn prepare_baseline(
        &self,
        publication: CanonicalBaselinePublication,
    ) -> Result<PreparedCanonicalBaselinePublication, CanonicalStoreError> {
        validate_settled_tip(
            &self.bounded_open.db,
            &self.ready_evidence,
            publication.settled_tip,
        )?;
        let settled_lag = self
            .ready_evidence
            .visible_tip
            .height
            .value()
            .checked_sub(publication.settled_tip.height.value())
            .ok_or_else(|| CanonicalStoreError::publication("settled tip exceeds visible tip"))?;
        if settled_lag > self.build_plan.reorg_policy().reorg_window_blocks() {
            return Err(CanonicalStoreError::publication(format!(
                "baseline settled tip lags visible tip by {settled_lag} blocks; reorg window is {}",
                self.build_plan.reorg_policy().reorg_window_blocks()
            )));
        }
        let sequence_checkpoint = self
            .retained_sequence_checkpoints
            .iter()
            .find(|checkpoint| checkpoint.through() == publication.settled_tip)
            .copied()
            .ok_or_else(|| {
                CanonicalStoreError::publication(
                    "settled tip has no authenticated cold replay prefix",
                )
            })?;
        let _ = require_empty_family(&self.bounded_open.db, CHAIN_EPOCH_COLUMN_FAMILY)?;
        let _ = require_empty_family(&self.bounded_open.db, CHAIN_EVENT_COLUMN_FAMILY)?;
        Ok(PreparedCanonicalBaselinePublication {
            publication,
            build_tip: self.build_plan.build_tip(),
            source_sequence_digest: self.ready_evidence.visible_sequence_digest,
            sequence_checkpoint,
        })
    }

    /// Atomically publishes epoch 1, event 1, and READY with a synced WAL write.
    pub fn publish_baseline(
        self,
        prepared: PreparedCanonicalBaselinePublication,
    ) -> Result<RocksDbCanonicalStore, CanonicalStoreError> {
        if prepared.build_tip != self.build_plan.build_tip()
            || prepared.source_sequence_digest != self.ready_evidence.visible_sequence_digest
        {
            return Err(CanonicalStoreError::publication(
                "prepared baseline belongs to a different canonical build",
            ));
        }

        let unbound_ready_evidence = CanonicalStoreReadyEvidence {
            sequence_checkpoint: prepared.sequence_checkpoint,
            ..self.ready_evidence
        };
        let binding = self
            .construction_manifest
            .persist(self.bounded_open.db.path(), &unbound_ready_evidence)?;
        // A crash here leaves a durable, immutable sidecar beside a BUILDING
        // store. The READY control record is still absent, so admission fails
        // closed and the whole fresh build must be discarded before retrying.
        abort_at_publication_failpoint("after_construction_manifest");
        let ready_evidence = CanonicalStoreReadyEvidence {
            construction_manifest_version: binding.version,
            construction_manifest_sha256: binding.sha256,
            ..unbound_ready_evidence
        };
        let epoch_key = BASELINE_EPOCH_ID.value().to_be_bytes();
        let event_key = BASELINE_EVENT_SEQUENCE.to_be_bytes();
        let encoded_publication = EncodedBaselinePublication {
            epoch: encode_baseline_epoch(&ready_evidence, prepared.publication, self.tip_metadata),
            event: encode_baseline_event(&ready_evidence),
            control: encode_ready_store_control(
                self.workload,
                &self.build_plan,
                self.cursor_auth_key,
                &ready_evidence,
            )
            .map_err(|source| CanonicalStoreError::publication(source.to_string()))?,
        };
        let epoch_family = column_family(&self.bounded_open.db, CHAIN_EPOCH_COLUMN_FAMILY)?;
        let event_family = column_family(&self.bounded_open.db, CHAIN_EVENT_COLUMN_FAMILY)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&epoch_family, epoch_key, encoded_publication.epoch);
        batch.put_cf(&event_family, event_key, encoded_publication.event);
        batch.put(STORE_CONTROL_KEY, &encoded_publication.control);
        if batch.len() != 3 {
            return Err(CanonicalStoreError::publication(
                "baseline publication batch must contain exactly three writes",
            ));
        }
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(false);
        write_options.set_sync(true);
        abort_at_publication_failpoint("before_atomic_write");
        self.bounded_open
            .db
            .write_opt(&batch, &write_options)
            .map_err(
                |source| CanonicalStoreError::PublicationWriteOutcomeUnknown {
                    path: self.bounded_open.db.path().to_path_buf(),
                    source,
                },
            )?;
        abort_at_publication_failpoint("after_atomic_write");
        let replay_tail = validate_exact_publication_readback(
            &self.bounded_open.db,
            &self.build_plan,
            &ready_evidence,
            &encoded_publication,
        )
        .map_err(
            |source| CanonicalStoreError::PublicationCommittedButUnverified {
                path: self.bounded_open.db.path().to_path_buf(),
                reason: source.to_string(),
            },
        )?;
        drop(epoch_family);
        drop(event_family);
        Ok(RocksDbCanonicalStore::from_published(
            self.bounded_open,
            self.workload,
            self.build_plan,
            self.cursor_auth_key,
            &ready_evidence,
            replay_tail,
        ))
    }
}

/// Validates one READY publication against its construction identity and the
/// store's effective reorg window, which bounds every retained transition.
///
/// Returns the unsettled replay tail it authenticated, which later
/// transitions extend incrementally.
pub(super) fn validate_ready_publication(
    db: &DB,
    build_plan: &super::CanonicalStoreBuildPlan,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<AuthenticatedReplayTail, CanonicalStoreError> {
    validate_first_retained_block(db, ready_evidence.first_retained_block)?;
    let replay_tail = authenticate_ready_replay_tail(db, reorg_policy, ready_evidence)?;
    let baseline_tip = build_plan.build_tip();
    let baseline_epoch = decode_chain_epoch(&read_family_row(
        db,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &BASELINE_EPOCH_ID.value().to_be_bytes(),
    )?)?;
    let baseline_event = decode_chain_event(&read_family_row(
        db,
        CHAIN_EVENT_COLUMN_FAMILY,
        &BASELINE_EVENT_SEQUENCE.to_be_bytes(),
    )?)?;
    validate_baseline_publication(build_plan, ready_evidence, baseline_epoch, baseline_event)?;
    if ready_evidence.visible_epoch == BASELINE_EPOCH_ID {
        validate_epoch_record(
            db,
            ready_evidence.first_retained_block.height,
            baseline_tip,
            baseline_epoch,
        )?;
    } else {
        validate_live_history(db, build_plan, reorg_policy, ready_evidence, baseline_epoch)?;
    }
    validate_permanent_reorg_archive(
        db,
        reorg_policy.reorg_window_blocks(),
        ready_evidence.visible_event_sequence,
    )?;
    super::event_lifecycle::validate_projection_lifecycle_records(db, ready_evidence)?;
    Ok(replay_tail)
}

pub(super) fn validate_live_append_publication(
    db: &DB,
    ready_evidence: &CanonicalStoreReadyEvidence,
    previous_epoch_id: ChainEpochId,
) -> Result<(), CanonicalStoreError> {
    if ready_evidence.visible_event_sequence != ready_evidence.visible_epoch.value()
        || ready_evidence.visible_epoch.value() != previous_epoch_id.value().saturating_add(1)
    {
        return Err(CanonicalStoreError::publication(
            "READY live epoch and chain-event sequence must advance together",
        ));
    }
    let previous_epoch = decode_chain_epoch(&read_family_row(
        db,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &previous_epoch_id.value().to_be_bytes(),
    )?)?;
    let current_epoch = decode_chain_epoch(&read_family_row(
        db,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &ready_evidence.visible_epoch.value().to_be_bytes(),
    )?)?;
    let current_event = decode_chain_event(&read_family_row(
        db,
        CHAIN_EVENT_COLUMN_FAMILY,
        &ready_evidence.visible_event_sequence.to_be_bytes(),
    )?)?;
    let appended_height =
        previous_epoch.visible_tip.height.next().ok_or_else(|| {
            CanonicalStoreError::publication("live append height exceeds u32::MAX")
        })?;
    let appended_range = BlockHeightRange::inclusive(appended_height, appended_height);
    validate_epoch_record(
        db,
        ready_evidence.first_retained_block.height,
        ready_evidence.visible_tip,
        current_epoch,
    )?;
    if current_epoch.visible_tip.height != appended_height
        || current_event.kind != COMMITTED_EVENT
        || current_event.resulting_epoch_id != ready_evidence.visible_epoch
        || current_event.previous_epoch_id != previous_epoch_id.value()
        || current_event.reverted_range.is_some()
        || current_event.committed_range != appended_range
    {
        return Err(CanonicalStoreError::publication(
            "READY live transition is not one valid version-1 append",
        ));
    }
    Ok(())
}

pub(super) fn validate_live_replacement_publication(
    db: &DB,
    ready_evidence: &CanonicalStoreReadyEvidence,
    previous_epoch_id: ChainEpochId,
    reverted_range: BlockHeightRange,
    committed_range: BlockHeightRange,
) -> Result<(), CanonicalStoreError> {
    if ready_evidence.visible_event_sequence != ready_evidence.visible_epoch.value()
        || ready_evidence.visible_epoch.value() != previous_epoch_id.value().saturating_add(1)
    {
        return Err(CanonicalStoreError::publication(
            "READY replacement epoch and chain-event sequence must advance together",
        ));
    }
    let previous_epoch = decode_chain_epoch(&read_family_row(
        db,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &previous_epoch_id.value().to_be_bytes(),
    )?)?;
    let current_epoch = decode_chain_epoch(&read_family_row(
        db,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &ready_evidence.visible_epoch.value().to_be_bytes(),
    )?)?;
    let current_event = decode_chain_event(&read_family_row(
        db,
        CHAIN_EVENT_COLUMN_FAMILY,
        &ready_evidence.visible_event_sequence.to_be_bytes(),
    )?)?;
    validate_epoch_record(
        db,
        ready_evidence.first_retained_block.height,
        ready_evidence.visible_tip,
        current_epoch,
    )?;
    if previous_epoch.settled_tip != current_epoch.settled_tip
        || current_event.kind != REORG_EVENT
        || current_event.resulting_epoch_id != ready_evidence.visible_epoch
        || current_event.previous_epoch_id != previous_epoch_id.value()
        || current_event.reverted_range != Some(reverted_range)
        || current_event.committed_range != committed_range
        || reverted_range.end != previous_epoch.visible_tip.height
        || reverted_range.start <= previous_epoch.settled_tip.height
        || committed_range.start != reverted_range.start
        || committed_range.end != current_epoch.visible_tip.height
    {
        return Err(CanonicalStoreError::publication(
            "READY live transition is not one valid version-1 replacement",
        ));
    }
    Ok(())
}

fn validate_first_retained_block(
    db: &DB,
    first_retained_block: BlockId,
) -> Result<(), CanonicalStoreError> {
    if retained_block_hash(db, first_retained_block.height)? != first_retained_block.hash {
        return Err(CanonicalStoreError::publication(
            "first retained block does not match the current canonical rows",
        ));
    }
    Ok(())
}

/// Re-derives the whole unsettled replay tail from the persisted settled
/// checkpoint and checks it against READY evidence.
pub(super) fn authenticate_ready_replay_tail(
    db: &DB,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<AuthenticatedReplayTail, CanonicalStoreError> {
    validate_ready_settlement_window(db, reorg_policy, ready_evidence)?;
    let replay_tail = AuthenticatedReplayTail::authenticate(
        db,
        ready_evidence.sequence_checkpoint,
        ready_evidence.visible_tip.height,
        reorg_policy.reorg_window_blocks(),
    )?;
    validate_replay_tail_matches_ready(&replay_tail, ready_evidence)?;
    Ok(replay_tail)
}

/// Brings `replay_tail` to the READY evidence a transition just published,
/// re-deriving only the rows above `retained_height`.
///
/// Once the incrementally derived rows reach one reorg window, the whole
/// unsettled suffix is re-derived instead, so an out-of-band change to a row
/// no transition touched is still found within one window of blocks.
pub(super) fn advance_ready_replay_tail(
    db: &DB,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &CanonicalStoreReadyEvidence,
    retained_height: BlockHeight,
    replay_tail: &mut AuthenticatedReplayTail,
) -> Result<(), CanonicalStoreError> {
    let window = reorg_policy.reorg_window_blocks();
    if replay_tail.full_authentication_due(window) {
        *replay_tail = authenticate_ready_replay_tail(db, reorg_policy, ready_evidence)?;
        return Ok(());
    }
    validate_ready_settlement_window(db, reorg_policy, ready_evidence)?;
    replay_tail.reauthenticate_above(
        db,
        retained_height,
        ready_evidence.visible_tip.height,
        window,
    )?;
    replay_tail.settle(ready_evidence.sequence_checkpoint)?;
    validate_replay_tail_matches_ready(replay_tail, ready_evidence)
}

/// Brings a reader's `replay_tail` from `previous` to `ready_evidence`,
/// re-deriving only the rows the retained chain events between them changed.
///
/// A reader that cannot read every intervening event, for example because
/// retention pruned them while it lagged, or whose incremental check fails,
/// re-derives the whole unsettled suffix, so it accepts exactly the READY
/// evidence a complete derivation accepts.
pub(super) fn follow_ready_replay_tail(
    db: &DB,
    reorg_policy: super::CanonicalReorgPolicy,
    previous: &CanonicalStoreReadyEvidence,
    ready_evidence: &CanonicalStoreReadyEvidence,
    replay_tail: &mut AuthenticatedReplayTail,
) -> Result<(), CanonicalStoreError> {
    if let Some(retained_height) = lowest_retained_height_since(db, previous, ready_evidence)? {
        let mut followed = replay_tail.clone();
        if advance_ready_replay_tail(
            db,
            reorg_policy,
            ready_evidence,
            retained_height,
            &mut followed,
        )
        .is_ok()
        {
            *replay_tail = followed;
            return Ok(());
        }
    }
    *replay_tail = authenticate_ready_replay_tail(db, reorg_policy, ready_evidence)?;
    Ok(())
}

/// Returns the highest height every chain event after `previous` left
/// unchanged, or `None` when an intervening event is unavailable.
fn lowest_retained_height_since(
    db: &DB,
    previous: &CanonicalStoreReadyEvidence,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<Option<BlockHeight>, CanonicalStoreError> {
    let first_sequence = previous.visible_event_sequence.saturating_add(1);
    let retention_floor = super::event_lifecycle::canonical_event_retention_floor_from_db(
        db,
        ready_evidence.visible_event_sequence,
    )?;
    if first_sequence < retention_floor {
        return Ok(None);
    }
    let mut retained_height = previous.visible_tip.height;
    for sequence in first_sequence..=ready_evidence.visible_event_sequence {
        let Ok(encoded) = read_family_row(db, CHAIN_EVENT_COLUMN_FAMILY, &sequence.to_be_bytes())
        else {
            return Ok(None);
        };
        let Ok(event) = decode_chain_event(&encoded) else {
            return Ok(None);
        };
        if event.resulting_epoch_id.value() != sequence {
            return Ok(None);
        }
        let first_changed = event
            .reverted_range
            .map_or(event.committed_range.start, |reverted| {
                reverted.start.min(event.committed_range.start)
            });
        let Some(unchanged_through) = first_changed.value().checked_sub(1) else {
            return Ok(None);
        };
        retained_height = retained_height.min(BlockHeight::new(unchanged_through));
    }
    Ok(Some(retained_height))
}

fn validate_ready_settlement_window(
    db: &DB,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<(), CanonicalStoreError> {
    let visible_epoch = decode_chain_epoch(&read_family_row(
        db,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &ready_evidence.visible_epoch.value().to_be_bytes(),
    )?)?;
    let checkpoint = ready_evidence.sequence_checkpoint;
    if visible_epoch.visible_tip != ready_evidence.visible_tip
        || visible_epoch.settled_tip != checkpoint.through()
    {
        return Err(CanonicalStoreError::publication(
            "READY sequence checkpoint does not match the visible epoch settled tip",
        ));
    }
    let settled_lag = ready_evidence
        .visible_tip
        .height
        .value()
        .checked_sub(checkpoint.through().height.value())
        .ok_or_else(|| CanonicalStoreError::publication("settled tip exceeds visible tip"))?;
    let reorg_window = reorg_policy.reorg_window_blocks();
    if settled_lag > reorg_window {
        return Err(CanonicalStoreError::publication(format!(
            "visible epoch exceeds its {reorg_window}-block settlement window"
        )));
    }
    Ok(())
}

fn validate_replay_tail_matches_ready(
    replay_tail: &AuthenticatedReplayTail,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<(), CanonicalStoreError> {
    let expected_visible = CanonicalSequenceCheckpoint::from_admitted_parts(
        ready_evidence.visible_tip,
        ready_evidence.visible_block_count,
        zinder_core::CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            ready_evidence.sequence_digest_version,
            ready_evidence.visible_block_count,
            ready_evidence.visible_sequence_digest,
        ),
        ready_evidence.visible_logical_replay_bytes,
    );
    if replay_tail.settled() != ready_evidence.sequence_checkpoint
        || replay_tail.tip() != expected_visible
    {
        return Err(CanonicalStoreError::publication(
            "READY sequence checkpoint does not authenticate the visible replay tail",
        ));
    }
    Ok(())
}

fn validate_baseline_publication(
    build_plan: &super::CanonicalStoreBuildPlan,
    ready_evidence: &CanonicalStoreReadyEvidence,
    baseline_epoch: DecodedChainEpoch,
    baseline_event: DecodedChainEvent,
) -> Result<(), CanonicalStoreError> {
    let baseline_tip = build_plan.build_tip();
    let expected_range = BlockHeightRange::inclusive(
        ready_evidence.first_retained_block.height,
        baseline_tip.height,
    );
    let expected_baseline_block_count = u64::from(
        baseline_tip
            .height
            .value()
            .checked_sub(ready_evidence.first_retained_block.height.value())
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| {
                CanonicalStoreError::publication(
                    "baseline tip precedes the first retained canonical block",
                )
            })?,
    );
    if baseline_epoch.visible_tip != baseline_tip
        || baseline_event.kind != COMMITTED_EVENT
        || baseline_event.resulting_epoch_id != BASELINE_EPOCH_ID
        || baseline_event.previous_epoch_id != 0
        || baseline_event.reverted_range.is_some()
        || baseline_event.committed_range != expected_range
        || baseline_event.visible_tip != baseline_tip
        || baseline_event.visible_block_count != expected_baseline_block_count
    {
        return Err(CanonicalStoreError::publication(
            "visible baseline epoch 1 or event 1 is inconsistent with the fixed build",
        ));
    }
    if ready_evidence.visible_epoch == BASELINE_EPOCH_ID
        && (ready_evidence.visible_event_sequence != BASELINE_EVENT_SEQUENCE
            || ready_evidence.visible_tip != baseline_tip
            || baseline_event.visible_block_count != ready_evidence.visible_block_count
            || baseline_event.visible_sequence_digest != ready_evidence.visible_sequence_digest)
    {
        return Err(CanonicalStoreError::publication(
            "READY baseline pointer does not match epoch 1 and event 1",
        ));
    }
    Ok(())
}

fn validate_live_history(
    db: &DB,
    build_plan: &super::CanonicalStoreBuildPlan,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &CanonicalStoreReadyEvidence,
    baseline_epoch: DecodedChainEpoch,
) -> Result<(), CanonicalStoreError> {
    if ready_evidence.visible_event_sequence != ready_evidence.visible_epoch.value() {
        return Err(CanonicalStoreError::publication(
            "READY live epoch and chain-event sequence must advance together",
        ));
    }
    let retention_floor = super::event_lifecycle::canonical_event_retention_floor_from_db(
        db,
        ready_evidence.visible_event_sequence,
    )?;
    let first_event_sequence = retention_floor.max(2);
    let (mut previous_epoch_id, mut previous_epoch) = if first_event_sequence == 2 {
        (BASELINE_EPOCH_ID, baseline_epoch)
    } else {
        let previous_epoch_id = ChainEpochId::new(first_event_sequence - 1);
        let previous_epoch = decode_chain_epoch(&read_family_row(
            db,
            CHAIN_EPOCH_COLUMN_FAMILY,
            &previous_epoch_id.value().to_be_bytes(),
        )?)?;
        (previous_epoch_id, previous_epoch)
    };
    for epoch_value in first_event_sequence..=ready_evidence.visible_epoch.value() {
        let chain_epoch_id = ChainEpochId::new(epoch_value);
        let chain_epoch = decode_chain_epoch(&read_family_row(
            db,
            CHAIN_EPOCH_COLUMN_FAMILY,
            &epoch_value.to_be_bytes(),
        )?)?;
        let chain_event = decode_chain_event(&read_family_row(
            db,
            CHAIN_EVENT_COLUMN_FAMILY,
            &epoch_value.to_be_bytes(),
        )?)?;
        if chain_event.resulting_epoch_id != chain_epoch_id
            || chain_event.previous_epoch_id != previous_epoch_id.value()
            || chain_event.visible_tip != chain_epoch.visible_tip
            || chain_event.visible_block_count == 0
            || !live_transition_is_valid(
                build_plan,
                reorg_policy,
                previous_epoch,
                chain_epoch,
                chain_event,
            )?
        {
            return Err(CanonicalStoreError::publication(
                "READY live history contains an invalid version-1 transition",
            ));
        }
        previous_epoch_id = chain_epoch_id;
        previous_epoch = chain_epoch;
    }
    validate_epoch_record(
        db,
        ready_evidence.first_retained_block.height,
        ready_evidence.visible_tip,
        previous_epoch,
    )?;
    let current_event = decode_chain_event(&read_family_row(
        db,
        CHAIN_EVENT_COLUMN_FAMILY,
        &ready_evidence.visible_event_sequence.to_be_bytes(),
    )?)?;
    let epoch_matches_ready = previous_epoch.visible_tip == ready_evidence.visible_tip;
    let expected_epoch_id = ready_evidence.visible_epoch;
    let expected_tip = ready_evidence.visible_tip;
    let expected_block_count = ready_evidence.visible_block_count;
    let expected_sequence_digest = ready_evidence.visible_sequence_digest;
    let event_matches_ready = current_event.resulting_epoch_id == expected_epoch_id
        && current_event.visible_tip == expected_tip
        && current_event.visible_block_count == expected_block_count
        && current_event.visible_sequence_digest == expected_sequence_digest;
    if !(epoch_matches_ready && event_matches_ready) {
        return Err(CanonicalStoreError::publication(
            "READY live history does not end at the authenticated visible fence",
        ));
    }
    Ok(())
}

fn live_transition_is_valid(
    build_plan: &super::CanonicalStoreBuildPlan,
    reorg_policy: super::CanonicalReorgPolicy,
    previous_epoch: DecodedChainEpoch,
    chain_epoch: DecodedChainEpoch,
    chain_event: DecodedChainEvent,
) -> Result<bool, CanonicalStoreError> {
    if !epoch_bounds_are_valid(
        build_plan.history_bounds().first_available_height(),
        chain_epoch,
    ) {
        return Ok(false);
    }
    if chain_event.kind == COMMITTED_EVENT {
        let appended_height = previous_epoch.visible_tip.height.next().ok_or_else(|| {
            CanonicalStoreError::publication("live append height exceeds u32::MAX")
        })?;
        let appended_range = BlockHeightRange::inclusive(appended_height, appended_height);
        return Ok(chain_epoch.visible_tip.height == appended_height
            && chain_epoch.settled_tip.height >= previous_epoch.settled_tip.height
            && chain_event.reverted_range.is_none()
            && chain_event.committed_range == appended_range);
    }
    let Some(reverted) = chain_event.reverted_range else {
        return Ok(false);
    };
    let committed = chain_event.committed_range;
    let displaced_count = reverted
        .end
        .value()
        .checked_sub(reverted.start.value())
        .and_then(|difference| difference.checked_add(1))
        .ok_or_else(|| CanonicalStoreError::publication("live replacement range is empty"))?;
    let visible_lag = chain_epoch
        .visible_tip
        .height
        .value()
        .checked_sub(chain_epoch.settled_tip.height.value())
        .ok_or_else(|| CanonicalStoreError::publication("replacement tip precedes settlement"))?;
    Ok(chain_event.kind == REORG_EVENT
        && reverted.end == previous_epoch.visible_tip.height
        && reverted.start > previous_epoch.settled_tip.height
        && committed.start == reverted.start
        && committed.end == chain_epoch.visible_tip.height
        && previous_epoch.settled_tip == chain_epoch.settled_tip
        && displaced_count <= reorg_policy.reorg_window_blocks()
        && visible_lag <= reorg_policy.reorg_window_blocks())
}

fn validate_epoch_record(
    db: &DB,
    first_height: BlockHeight,
    expected_visible_tip: BlockId,
    epoch: DecodedChainEpoch,
) -> Result<(), CanonicalStoreError> {
    if epoch.visible_tip != expected_visible_tip
        || !epoch_bounds_are_valid(first_height, epoch)
        || retained_block_hash(db, expected_visible_tip.height)? != expected_visible_tip.hash
        || retained_block_hash(db, epoch.settled_tip.height)? != epoch.settled_tip.hash
        || epoch.tip_metadata != read_persisted_tip_metadata(db, expected_visible_tip)?
    {
        return Err(CanonicalStoreError::publication(
            "chain epoch does not match its retained canonical rows",
        ));
    }
    Ok(())
}

fn epoch_bounds_are_valid(first_retained_height: BlockHeight, epoch: DecodedChainEpoch) -> bool {
    epoch.settled_tip.height >= first_retained_height
        && epoch.settled_tip.height <= epoch.visible_tip.height
}

fn flush_complete_build(db: &DB) -> Result<(), CanonicalStoreError> {
    let mut families = Vec::with_capacity(CANONICAL_DATA_COLUMN_FAMILIES.len() + 1);
    for name in std::iter::once(DEFAULT_COLUMN_FAMILY_NAME).chain(CANONICAL_DATA_COLUMN_FAMILIES) {
        families.push(column_family(db, name)?);
    }
    let family_refs = families.iter().collect::<Vec<_>>();
    let mut flush_options = FlushOptions::default();
    flush_options.set_wait(true);
    db.flush_cfs_opt(&family_refs, &flush_options)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "publication column-family flush",
            source,
        })?;
    db.flush_wal(true)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "publication WAL sync",
            source,
        })
}

fn validate_reopened_identity_and_control(
    db: &DB,
    store_path: &std::path::Path,
    expected_database_identity: &[u8],
    expected_control: &DecodedStoreControl,
) -> Result<(), CanonicalStoreError> {
    let database_identity =
        db.get_db_identity()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "cold publication database identity read",
                source,
            })?;
    let expectation = CanonicalStoreAdmissionExpectation::from_build_plan(
        &expected_control.build_plan,
        expected_control.workload,
        expected_control.effective_reorg_policy(),
    );
    let control = validate_open_store_control(db, store_path, &expectation)?;
    if database_identity.as_slice() != expected_database_identity || &control != expected_control {
        return Err(CanonicalStoreError::publication(
            "database identity or BUILDING control changed during cold reopen",
        ));
    }
    Ok(())
}

fn validate_cold_block_families(
    db: &DB,
    workload: super::CanonicalStoreWorkload,
    build_plan: &super::CanonicalStoreBuildPlan,
    expected: &CanonicalBlockLoadEvidence,
) -> Result<ColdCanonicalBlockValidation, CanonicalStoreError> {
    let retained_checkpoint_count =
        usize::try_from(u64::from(build_plan.reorg_policy().reorg_window_blocks()) + 1)
            .map_err(|_| CanonicalStoreError::publication("reorg window exceeds usize"))?;
    let replay = validate_persisted_block_replays_with_checkpoints(db, retained_checkpoint_count)?;
    if !replay.evidence.has_same_sequence(expected) {
        return Err(CanonicalStoreError::BlockLoadReadbackMismatch);
    }
    validate_persisted_commitment_tree_families(db, workload, build_plan, expected)?;
    let expected_families = [
        (
            BLOCK_HEADER_COLUMN_FAMILY,
            expected.block_header_count,
            expected.block_header_logical_bytes,
        ),
        (
            BLOCK_HASH_INDEX_COLUMN_FAMILY,
            expected.block_hash_index_count,
            expected.block_hash_index_logical_bytes,
        ),
        (
            COMPACT_BLOCK_COLUMN_FAMILY,
            expected.compact_block_count,
            expected.compact_block_logical_bytes,
        ),
        (
            TRANSACTION_LOCATION_COLUMN_FAMILY,
            expected.transaction_location_count,
            expected.transaction_location_logical_bytes,
        ),
        (
            TRANSACTION_BLOB_COLUMN_FAMILY,
            expected.transaction_blob_count,
            expected.transaction_blob_logical_bytes,
        ),
        (
            BLOCK_BLOB_COLUMN_FAMILY,
            expected.block_blob_count,
            expected.block_blob_logical_bytes,
        ),
        (
            TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
            expected.tree_state_checkpoint_count,
            expected.tree_state_checkpoint_logical_bytes,
        ),
        (
            BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
            expected.block_final_note_commitment_roots_count,
            expected.block_final_note_commitment_roots_logical_bytes,
        ),
    ];
    let mut family_evidence = Vec::with_capacity(expected_families.len() + 5);
    family_evidence.push(replay.evidence.family_evidence.clone());
    record_cold_family_scan(&ColdFamilyScanEvidence::from_replay(&replay.evidence));
    for (name, expected_count, expected_logical_bytes) in expected_families {
        let observed = scan_family(db, name)?;
        if !observed.matches(expected_count, expected_logical_bytes) {
            return Err(CanonicalStoreError::publication(format!(
                "cold {name} count or logical bytes differ from the source load"
            )));
        }
        family_evidence.push(observed.evidence);
    }
    family_evidence.push(require_empty_family(
        db,
        DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY,
    )?);
    for name in [
        CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY,
        MEMPOOL_EVENT_COLUMN_FAMILY,
        DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
    ] {
        family_evidence.push(require_empty_family(db, name)?);
    }
    Ok(ColdCanonicalBlockValidation {
        replay,
        family_evidence,
    })
}

struct ColdCanonicalBlockValidation {
    replay: PersistedBlockReplayValidation,
    family_evidence: Vec<CanonicalConstructionFamilyEvidence>,
}

pub(super) fn validate_settled_tip(
    db: &DB,
    ready_evidence: &CanonicalStoreReadyEvidence,
    settled_tip: BlockId,
) -> Result<(), CanonicalStoreError> {
    if settled_tip.height < ready_evidence.first_retained_block.height
        || settled_tip.height > ready_evidence.visible_tip.height
        || retained_block_hash(db, settled_tip.height)? != settled_tip.hash
    {
        return Err(CanonicalStoreError::publication(
            "settled tip is outside retained history or has the wrong canonical hash",
        ));
    }
    Ok(())
}

fn encode_baseline_epoch(
    ready_evidence: &CanonicalStoreReadyEvidence,
    publication: CanonicalBaselinePublication,
    tip_metadata: ChainTipMetadata,
) -> [u8; EPOCH_VALUE_LENGTH] {
    encode_chain_epoch(
        ready_evidence.visible_tip,
        publication.settled_tip,
        tip_metadata,
        publication.created_at,
    )
}

pub(super) fn encode_live_chain_epoch(
    visible_tip: BlockId,
    settled_tip: BlockId,
    tip_metadata: ChainTipMetadata,
    created_at: UnixTimestampMillis,
) -> [u8; EPOCH_VALUE_LENGTH] {
    encode_chain_epoch(visible_tip, settled_tip, tip_metadata, created_at)
}

fn encode_chain_epoch(
    visible_tip: BlockId,
    settled_tip: BlockId,
    tip_metadata: ChainTipMetadata,
    created_at: UnixTimestampMillis,
) -> [u8; EPOCH_VALUE_LENGTH] {
    let mut encoded = [0; EPOCH_VALUE_LENGTH];
    encoded[0] = VERSION_ONE;
    encoded[1..5].copy_from_slice(&visible_tip.height.value().to_le_bytes());
    encoded[5..37].copy_from_slice(&visible_tip.hash.as_bytes());
    encoded[37..41].copy_from_slice(&settled_tip.height.value().to_le_bytes());
    encoded[41..73].copy_from_slice(&settled_tip.hash.as_bytes());
    encoded[73..77].copy_from_slice(
        &tip_metadata
            .commitment_tree_size(ShieldedProtocol::Sapling)
            .to_le_bytes(),
    );
    encoded[77..81].copy_from_slice(
        &tip_metadata
            .commitment_tree_size(ShieldedProtocol::Orchard)
            .to_le_bytes(),
    );
    encoded[81..85].copy_from_slice(
        &tip_metadata
            .commitment_tree_size(ShieldedProtocol::Ironwood)
            .to_le_bytes(),
    );
    encoded[85..].copy_from_slice(&created_at.value().to_le_bytes());
    encoded
}

fn encode_baseline_event(ready_evidence: &CanonicalStoreReadyEvidence) -> [u8; EVENT_VALUE_LENGTH] {
    let mut encoded = [0; EVENT_VALUE_LENGTH];
    encoded[0] = VERSION_ONE;
    encoded[1] = COMMITTED_EVENT;
    encoded[2..10].copy_from_slice(&BASELINE_EPOCH_ID.value().to_le_bytes());
    encoded[10..18].copy_from_slice(&0_u64.to_le_bytes());
    encoded[18] = REVERTED_RANGE_ABSENT;
    encoded[19..27].fill(0);
    encode_event_range(
        &mut encoded,
        27,
        BlockHeightRange::inclusive(
            ready_evidence.first_retained_block.height,
            ready_evidence.visible_tip.height,
        ),
    );
    encode_event_fence(
        &mut encoded,
        CanonicalEventFence::from_persisted_event(
            ready_evidence.visible_epoch,
            ready_evidence.visible_event_sequence,
            ready_evidence.visible_tip,
            ready_evidence.visible_block_count,
            ready_evidence.visible_sequence_digest,
        ),
    );
    encoded
}

pub(super) fn encode_live_append_event(
    previous_epoch_id: ChainEpochId,
    fence: CanonicalEventFence,
) -> [u8; EVENT_VALUE_LENGTH] {
    encode_live_event(
        fence.chain_epoch_id(),
        previous_epoch_id,
        None,
        BlockHeightRange::inclusive(fence.visible_tip().height, fence.visible_tip().height),
        fence,
    )
}

/// Encodes one version-1 live canonical transition.
pub(super) fn encode_live_event(
    resulting_epoch_id: ChainEpochId,
    previous_epoch_id: ChainEpochId,
    reverted_range: Option<BlockHeightRange>,
    committed_range: BlockHeightRange,
    fence: CanonicalEventFence,
) -> [u8; EVENT_VALUE_LENGTH] {
    let mut encoded = [0; EVENT_VALUE_LENGTH];
    encoded[0] = VERSION_ONE;
    encoded[1] = if reverted_range.is_some() {
        REORG_EVENT
    } else {
        COMMITTED_EVENT
    };
    encoded[2..10].copy_from_slice(&resulting_epoch_id.value().to_le_bytes());
    encoded[10..18].copy_from_slice(&previous_epoch_id.value().to_le_bytes());
    if let Some(reverted_range) = reverted_range {
        encoded[18] = REVERTED_RANGE_PRESENT;
        encode_event_range(&mut encoded, 19, reverted_range);
    } else {
        encoded[18] = REVERTED_RANGE_ABSENT;
        encoded[19..27].fill(0);
    }
    encode_event_range(&mut encoded, 27, committed_range);
    encode_event_fence(&mut encoded, fence);
    encoded
}

fn encode_event_fence(encoded: &mut [u8; EVENT_VALUE_LENGTH], fence: CanonicalEventFence) {
    encoded[35..39].copy_from_slice(&fence.visible_tip().height.value().to_le_bytes());
    encoded[39..71].copy_from_slice(&fence.visible_tip().hash.as_bytes());
    encoded[71..79].copy_from_slice(&fence.sequence_digest().block_count().to_le_bytes());
    encoded[79..111].copy_from_slice(&fence.sequence_digest().as_bytes());
}

fn encode_event_range(
    encoded: &mut [u8; EVENT_VALUE_LENGTH],
    offset: usize,
    block_range: BlockHeightRange,
) {
    encoded[offset..offset + 4].copy_from_slice(&block_range.start.value().to_le_bytes());
    encoded[offset + 4..offset + 8].copy_from_slice(&block_range.end.value().to_le_bytes());
}

fn validate_exact_publication_readback(
    db: &DB,
    build_plan: &super::CanonicalStoreBuildPlan,
    ready_evidence: &CanonicalStoreReadyEvidence,
    expected: &EncodedBaselinePublication,
) -> Result<AuthenticatedReplayTail, CanonicalStoreError> {
    let epoch_family = column_family(db, CHAIN_EPOCH_COLUMN_FAMILY)?;
    let event_family = column_family(db, CHAIN_EVENT_COLUMN_FAMILY)?;
    let epoch = db
        .get_cf(&epoch_family, BASELINE_EPOCH_ID.value().to_be_bytes())
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "baseline epoch readback",
            source,
        })?;
    let event = db
        .get_cf(&event_family, BASELINE_EVENT_SEQUENCE.to_be_bytes())
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "baseline event readback",
            source,
        })?;
    let control =
        db.get(STORE_CONTROL_KEY)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "READY control readback",
                source,
            })?;
    if epoch.as_deref() != Some(expected.epoch.as_slice())
        || event.as_deref() != Some(expected.event.as_slice())
        || control.as_deref() != Some(expected.control.as_slice())
    {
        return Err(CanonicalStoreError::publication(
            "atomic baseline publication readback differs from its three writes",
        ));
    }
    let decoded_control = decode_store_control(db.path(), &expected.control)?;
    if decoded_control.build_state != CanonicalStoreBuildState::Ready(*ready_evidence) {
        return Err(CanonicalStoreError::publication(
            "READY control does not contain the validated baseline evidence",
        ));
    }
    validate_ready_publication(db, build_plan, build_plan.reorg_policy(), ready_evidence)
}

fn decode_chain_epoch(encoded: &[u8]) -> Result<DecodedChainEpoch, CanonicalStoreError> {
    if encoded.len() != EPOCH_VALUE_LENGTH || encoded[0] != VERSION_ONE {
        return Err(CanonicalStoreError::publication(
            "baseline epoch is not the exact version-1 record",
        ));
    }
    Ok(DecodedChainEpoch {
        visible_tip: BlockId::new(
            BlockHeight::new(read_u32(encoded, 1)?),
            BlockHash::from_bytes(read_array(encoded, 5)?),
        ),
        settled_tip: BlockId::new(
            BlockHeight::new(read_u32(encoded, 37)?),
            BlockHash::from_bytes(read_array(encoded, 41)?),
        ),
        tip_metadata: ChainTipMetadata::new(
            read_u32(encoded, 73)?,
            read_u32(encoded, 77)?,
            read_u32(encoded, 81)?,
        ),
        created_at: UnixTimestampMillis::new(read_u64(encoded, 85)?),
    })
}

pub(super) fn canonical_event_created_at(
    db: &DB,
    event_sequence: u64,
) -> Result<UnixTimestampMillis, CanonicalStoreError> {
    Ok(decode_chain_epoch(&read_family_row(
        db,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &event_sequence.to_be_bytes(),
    )?)?
    .created_at)
}

pub(super) fn read_chain_epoch_tips(
    db: &DB,
    chain_epoch_id: ChainEpochId,
) -> Result<(BlockId, BlockId), CanonicalStoreError> {
    let epoch = decode_chain_epoch(&read_family_row(
        db,
        CHAIN_EPOCH_COLUMN_FAMILY,
        &chain_epoch_id.value().to_be_bytes(),
    )?)?;
    Ok((epoch.visible_tip, epoch.settled_tip))
}

pub(super) fn decode_chain_event(encoded: &[u8]) -> Result<DecodedChainEvent, CanonicalStoreError> {
    if encoded.len() != EVENT_VALUE_LENGTH || encoded[0] != VERSION_ONE {
        return Err(CanonicalStoreError::publication(
            "chain event is not the exact version-1 record",
        ));
    }
    let event_kind = encoded[1];
    let resulting_epoch_id = ChainEpochId::new(read_u64(encoded, 2)?);
    let previous_epoch_id = read_u64(encoded, 10)?;
    if resulting_epoch_id.value() == 0 || previous_epoch_id >= resulting_epoch_id.value() {
        return Err(CanonicalStoreError::publication(
            "chain event epoch transition is not monotonic",
        ));
    }
    let reverted_range = decode_optional_reverted_range(encoded)?;
    let committed_range = decode_event_range(encoded, 27)?;
    let visible_tip = BlockId::new(
        BlockHeight::new(read_u32(encoded, 35)?),
        BlockHash::from_bytes(read_array(encoded, 39)?),
    );
    let visible_block_count = read_u64(encoded, 71)?;
    let visible_sequence_digest = read_array(encoded, 79)?;
    match event_kind {
        COMMITTED_EVENT if reverted_range.is_none() => {}
        REORG_EVENT if previous_epoch_id > 0 && reverted_range.is_some() => {}
        _ => {
            return Err(CanonicalStoreError::publication(
                "chain event kind and version-1 ranges are inconsistent",
            ));
        }
    }
    Ok(DecodedChainEvent {
        kind: event_kind,
        resulting_epoch_id,
        previous_epoch_id,
        reverted_range,
        committed_range,
        visible_tip,
        visible_block_count,
        visible_sequence_digest,
    })
}

fn decode_optional_reverted_range(
    encoded: &[u8],
) -> Result<Option<BlockHeightRange>, CanonicalStoreError> {
    match encoded[18] {
        REVERTED_RANGE_ABSENT if encoded[19..27].iter().all(|byte| *byte == 0) => Ok(None),
        REVERTED_RANGE_ABSENT => Err(CanonicalStoreError::publication(
            "absent reverted range contains nonzero heights",
        )),
        REVERTED_RANGE_PRESENT => {
            let block_range = decode_event_range(encoded, 19)?;
            if block_range.start > block_range.end {
                return Err(CanonicalStoreError::publication(
                    "reverted chain event range must not be empty",
                ));
            }
            Ok(Some(block_range))
        }
        presence => Err(CanonicalStoreError::publication(format!(
            "chain event contains unknown reverted-range presence {presence}"
        ))),
    }
}

fn decode_event_range(
    encoded: &[u8],
    offset: usize,
) -> Result<BlockHeightRange, CanonicalStoreError> {
    let start = BlockHeight::new(read_u32(encoded, offset)?);
    let end = BlockHeight::new(read_u32(encoded, offset + 4)?);
    let is_nonempty = start <= end;
    let is_anchored_empty = end.next() == Some(start);
    if !is_nonempty && !is_anchored_empty {
        return Err(CanonicalStoreError::publication(
            "chain event range is neither inclusive nor anchored empty",
        ));
    }
    Ok(BlockHeightRange::inclusive(start, end))
}

fn read_u32(encoded: &[u8], offset: usize) -> Result<u32, CanonicalStoreError> {
    Ok(u32::from_le_bytes(read_array(encoded, offset)?))
}

fn read_u64(encoded: &[u8], offset: usize) -> Result<u64, CanonicalStoreError> {
    Ok(u64::from_le_bytes(read_array(encoded, offset)?))
}

fn read_array<const N: usize>(
    encoded: &[u8],
    offset: usize,
) -> Result<[u8; N], CanonicalStoreError> {
    encoded
        .get(offset..offset + N)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| CanonicalStoreError::publication("version-1 record is truncated"))
}

fn require_empty_family(
    db: &DB,
    name: &'static str,
) -> Result<CanonicalConstructionFamilyEvidence, CanonicalStoreError> {
    let family = column_family(db, name)?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    let mut iterator = db.raw_iterator_cf_opt(&family, read_options);
    iterator.seek_to_first();
    let is_empty = !iterator.valid();
    iterator
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "empty column-family admission",
            source,
        })?;
    if !is_empty {
        return Err(CanonicalStoreError::publication(format!(
            "{name} must be empty before baseline publication"
        )));
    }
    Ok(CanonicalConstructionFamilyEvidence::accumulator(name).finish())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ColdFamilyScanEvidence {
    evidence: CanonicalConstructionFamilyEvidence,
    elapsed: Duration,
}

impl ColdFamilyScanEvidence {
    const fn matches(&self, expected_row_count: u64, expected_logical_bytes: u64) -> bool {
        self.evidence.row_count == expected_row_count
            && self.evidence.logical_bytes == expected_logical_bytes
    }

    fn from_replay(replay: &super::block_replay::PersistedBlockReplayEvidence) -> Self {
        Self {
            evidence: replay.family_evidence.clone(),
            elapsed: replay.elapsed,
        }
    }
}

/// Performs the exact cold-readback scan used to admit a canonical build.
///
/// The metrics are emitted only after `iterator.status` succeeds, so every
/// reported observation represents a complete scan of exactly one family.
fn scan_family(db: &DB, name: &'static str) -> Result<ColdFamilyScanEvidence, CanonicalStoreError> {
    let started_at = Instant::now();
    let family = column_family(db, name)?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    read_options.set_readahead_size(2 * 1024 * 1024);
    let mut iterator = db.raw_iterator_cf_opt(&family, read_options);
    iterator.seek_to_first();
    let mut evidence = CanonicalConstructionFamilyEvidence::accumulator(name);
    while iterator.valid() {
        let Some((key, encoded_row)) = iterator.item() else {
            break;
        };
        evidence.observe(key, encoded_row)?;
        iterator.next();
    }
    iterator
        .status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "cold column-family readback",
            source,
        })?;
    let evidence = ColdFamilyScanEvidence {
        evidence: evidence.finish(),
        elapsed: started_at.elapsed(),
    };
    record_cold_family_scan(&evidence);
    Ok(evidence)
}

fn record_cold_family_scan(evidence: &ColdFamilyScanEvidence) {
    metrics::histogram!(
        "zinder_store_canonical_publication_family_scan_duration_seconds",
        "family" => evidence.evidence.family
    )
    .record(evidence.elapsed);
    metrics::counter!(
        "zinder_store_canonical_publication_family_scan_rows_total",
        "family" => evidence.evidence.family
    )
    .increment(evidence.evidence.row_count);
    metrics::counter!(
        "zinder_store_canonical_publication_family_scan_logical_bytes_total",
        "family" => evidence.evidence.family
    )
    .increment(evidence.evidence.logical_bytes);
    tracing::info!(
        target: "zinder::store",
        event = "canonical_publication_family_scan_completed",
        family = evidence.evidence.family,
        duration_seconds = evidence.elapsed.as_secs_f64(),
        rows = evidence.evidence.row_count,
        logical_bytes = evidence.evidence.logical_bytes,
        "canonical cold publication family scan complete"
    );
}

fn read_family_row(
    db: &DB,
    name: &'static str,
    key: &[u8],
) -> Result<Vec<u8>, CanonicalStoreError> {
    let family = column_family(db, name)?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(false);
    db.get_cf_opt(&family, key, &read_options)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "READY publication point read",
            source,
        })?
        .ok_or_else(|| CanonicalStoreError::publication(format!("{name} row is absent")))
}

pub(super) fn column_family<'db>(
    db: &'db DB,
    name: &'static str,
) -> Result<Arc<BoundColumnFamily<'db>>, CanonicalStoreError> {
    db.cf_handle(name)
        .ok_or_else(|| CanonicalStoreError::publication(format!("{name} column family is absent")))
}

#[derive(Clone, Copy)]
struct DecodedChainEpoch {
    visible_tip: BlockId,
    settled_tip: BlockId,
    tip_metadata: ChainTipMetadata,
    created_at: UnixTimestampMillis,
}

#[derive(Clone, Copy)]
pub(super) struct DecodedChainEvent {
    pub(super) kind: u8,
    pub(super) resulting_epoch_id: ChainEpochId,
    pub(super) previous_epoch_id: u64,
    pub(super) reverted_range: Option<BlockHeightRange>,
    pub(super) committed_range: BlockHeightRange,
    pub(super) visible_tip: BlockId,
    pub(super) visible_block_count: u64,
    pub(super) visible_sequence_digest: [u8; 32],
}

#[cfg(test)]
fn abort_at_publication_failpoint(expected: &str) {
    if std::env::var("ZINDER_TEST_CANONICAL_PUBLICATION_FAILPOINT").as_deref() == Ok(expected) {
        std::process::abort();
    }
}

#[cfg(not(test))]
const fn abort_at_publication_failpoint(_expected: &str) {}

#[cfg(test)]
mod tests {
    use zinder_core::{
        CanonicalBlockFactsDigestVersion, CanonicalBlockFactsSequenceDigestVersion,
        CanonicalBlockReplayFormatVersion,
    };

    use super::*;

    #[test]
    fn cold_family_scan_evidence_requires_matching_rows_and_logical_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut accumulator =
            CanonicalConstructionFamilyEvidence::accumulator(BLOCK_HEADER_COLUMN_FAMILY);
        for key in [b"a", b"b", b"c"] {
            accumulator.observe(key, b"123456")?;
        }
        let evidence = ColdFamilyScanEvidence {
            evidence: accumulator.finish(),
            elapsed: Duration::from_secs(2),
        };

        assert!(evidence.matches(3, 21));
        assert!(!evidence.matches(2, 21));
        assert!(!evidence.matches(3, 20));
        Ok(())
    }

    #[test]
    fn baseline_epoch_and_event_have_exact_version_one_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let evidence = ready_evidence();
        let publication = CanonicalBaselinePublication::new(
            BlockId::new(BlockHeight::new(15), BlockHash::from_bytes([3; 32])),
            UnixTimestampMillis::new(7),
        );
        let epoch = encode_baseline_epoch(&evidence, publication, ChainTipMetadata::new(4, 5, 6));
        assert_eq!(epoch.len(), 93);
        assert_eq!(epoch[0], 1);
        assert_eq!(&epoch[1..5], &20_u32.to_le_bytes());
        assert_eq!(&epoch[5..37], &[2; 32]);
        assert_eq!(&epoch[37..41], &15_u32.to_le_bytes());
        assert_eq!(&epoch[41..73], &[3; 32]);
        assert_eq!(&epoch[73..77], &4_u32.to_le_bytes());
        assert_eq!(&epoch[77..81], &5_u32.to_le_bytes());
        assert_eq!(&epoch[81..85], &6_u32.to_le_bytes());
        assert_eq!(&epoch[85..], &7_u64.to_le_bytes());
        let decoded_epoch = decode_chain_epoch(&epoch)?;
        assert_eq!(decoded_epoch.visible_tip.height, BlockHeight::new(20));
        assert_eq!(decoded_epoch.settled_tip, publication.settled_tip);
        assert_eq!(decoded_epoch.tip_metadata, ChainTipMetadata::new(4, 5, 6));

        let event = encode_baseline_event(&evidence);
        assert_eq!(event.len(), EVENT_VALUE_LENGTH);
        assert_eq!(event[0], 1);
        assert_eq!(event[1], COMMITTED_EVENT);
        assert_eq!(&event[2..10], &1_u64.to_le_bytes());
        assert_eq!(&event[10..27], &[0; 17]);
        assert_eq!(&event[27..31], &10_u32.to_le_bytes());
        assert_eq!(&event[31..35], &20_u32.to_le_bytes());
        assert_eq!(&event[35..39], &20_u32.to_le_bytes());
        assert_eq!(&event[39..71], &[2; 32]);
        assert_eq!(&event[71..79], &11_u64.to_le_bytes());
        assert_eq!(&event[79..], &[4; 32]);
        let decoded_event = decode_chain_event(&event)?;
        assert_eq!(decoded_event.resulting_epoch_id, ChainEpochId::new(1));
        assert_eq!(
            decoded_event.committed_range,
            BlockHeightRange::inclusive(BlockHeight::new(10), BlockHeight::new(20))
        );
        assert_eq!(decoded_event.visible_tip, evidence.visible_tip);
        assert_eq!(decoded_event.visible_block_count, 11);
        assert_eq!(decoded_event.visible_sequence_digest, [4; 32]);

        let mut wrong_epoch_version = epoch;
        wrong_epoch_version[0] = 2;
        assert!(decode_chain_epoch(&wrong_epoch_version).is_err());
        let mut wrong_event_version = event;
        wrong_event_version[0] = 2;
        assert!(decode_chain_event(&wrong_event_version).is_err());
        Ok(())
    }

    #[test]
    fn event_version_one_preserves_reorg_and_empty_ranges() -> Result<(), Box<dyn std::error::Error>>
    {
        let replacement_range =
            BlockHeightRange::inclusive(BlockHeight::new(19), BlockHeight::new(20));
        let reorg_event = encode_live_event(
            ChainEpochId::new(3),
            ChainEpochId::new(2),
            Some(replacement_range),
            replacement_range,
            CanonicalEventFence::from_persisted_event(
                ChainEpochId::new(3),
                3,
                BlockId::new(BlockHeight::new(20), BlockHash::from_bytes([20; 32])),
                20,
                [20; 32],
            ),
        );
        assert_eq!(reorg_event.len(), EVENT_VALUE_LENGTH);
        assert_eq!(reorg_event[0], 1);
        assert_eq!(reorg_event[1], 2);
        assert_eq!(&reorg_event[2..10], &3_u64.to_le_bytes());
        assert_eq!(&reorg_event[10..18], &2_u64.to_le_bytes());
        assert_eq!(reorg_event[18], 1);
        assert_eq!(&reorg_event[19..23], &19_u32.to_le_bytes());
        assert_eq!(&reorg_event[23..27], &20_u32.to_le_bytes());
        assert_eq!(&reorg_event[27..31], &19_u32.to_le_bytes());
        assert_eq!(&reorg_event[31..35], &20_u32.to_le_bytes());

        let decoded_reorg = decode_chain_event(&reorg_event)?;
        assert_eq!(decoded_reorg.resulting_epoch_id, ChainEpochId::new(3));
        assert_eq!(decoded_reorg.previous_epoch_id, 2);
        assert_eq!(decoded_reorg.reverted_range, Some(replacement_range));
        assert_eq!(decoded_reorg.committed_range, replacement_range);

        let mut settled_tip_event = [0; EVENT_VALUE_LENGTH];
        settled_tip_event[0] = VERSION_ONE;
        settled_tip_event[1] = COMMITTED_EVENT;
        settled_tip_event[2..10].copy_from_slice(&4_u64.to_le_bytes());
        settled_tip_event[10..18].copy_from_slice(&3_u64.to_le_bytes());
        let empty_range = BlockHeightRange::empty_at(BlockHeight::new(20));
        encode_event_range(&mut settled_tip_event, 27, empty_range);
        assert_eq!(
            decode_chain_event(&settled_tip_event)?.committed_range,
            empty_range
        );

        let mut genesis_event = settled_tip_event;
        let genesis_range = BlockHeightRange::inclusive(BlockHeight::new(0), BlockHeight::new(0));
        encode_event_range(&mut genesis_event, 27, genesis_range);
        assert_eq!(
            decode_chain_event(&genesis_event)?.committed_range,
            genesis_range
        );
        Ok(())
    }

    fn ready_evidence() -> CanonicalStoreReadyEvidence {
        CanonicalStoreReadyEvidence {
            first_retained_block: BlockId::new(
                BlockHeight::new(10),
                BlockHash::from_bytes([1; 32]),
            ),
            visible_tip: BlockId::new(BlockHeight::new(20), BlockHash::from_bytes([2; 32])),
            visible_epoch: ChainEpochId::new(1),
            visible_event_sequence: 1,
            visible_block_count: 11,
            block_digest_version: CanonicalBlockFactsDigestVersion::V1,
            replay_format_version: CanonicalBlockReplayFormatVersion::V1,
            sequence_digest_version: CanonicalBlockFactsSequenceDigestVersion::V1,
            visible_sequence_digest: [4; 32],
            visible_logical_replay_bytes: 1,
            sequence_checkpoint: CanonicalSequenceCheckpoint::from_admitted_parts(
                BlockId::new(BlockHeight::new(20), BlockHash::from_bytes([2; 32])),
                11,
                zinder_core::CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
                    zinder_core::CanonicalBlockFactsSequenceDigestVersion::V1,
                    11,
                    [4; 32],
                ),
                1,
            ),
            construction_manifest_version: crate::CANONICAL_CONSTRUCTION_MANIFEST_FORMAT_VERSION,
            construction_manifest_sha256: [6; 32],
        }
    }
}
