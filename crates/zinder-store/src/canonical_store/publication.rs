use std::{
    borrow::Cow,
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use rust_rocksdb::{
    BoundColumnFamily, DB, DEFAULT_COLUMN_FAMILY_NAME, FlushOptions, LiveFile, ReadOptions,
    ReadTier, WriteBatch, WriteOptions,
};
use zinder_core::{
    BlockHash, BlockHeight, BlockHeightRange, BlockId, ChainEpochId, ChainTipMetadata,
    ShieldedProtocol, UnixTimestampMillis,
};

use crate::{BoundedRocksDbOpen, RocksDbOpenRole, open_bounded_rocksdb};

use super::{
    CanonicalActivationSchedule, CanonicalEventFence, CanonicalSequenceCheckpoint,
    CanonicalStoreBuildState, CanonicalStoreError, CanonicalStoreReadyEvidence,
    RocksDbCanonicalBuilder, RocksDbCanonicalStore,
    block_load::{
        CanonicalBlockLoadEvidence, CanonicalTrustedFreshBlockEvidence,
        ConstructionTreeStateCheckpointPolicy, encode_block_position, read_persisted_tip_metadata,
        validate_persisted_commitment_tree_families, validate_source_tip_checkpoint,
    },
    block_replay::{
        PersistedBlockReplayValidation, validate_persisted_block_replays_with_checkpoints,
    },
    construction_manifest::{
        CanonicalConstructionFamilyEvidence, CanonicalConstructionManifestDraft,
        CanonicalConstructionManifestInputs,
    },
    control::{
        ControlRevisions, DecodedStoreControl, decode_store_control,
        encode_ready_store_control_with_revisions,
    },
    displaced_archive::validate_permanent_reorg_archive,
    replay_tail::AuthenticatedReplayTail,
    rocksdb::{
        BLOCK_HASH_INDEX_COLUMN_FAMILY, BLOCK_HEADER_COLUMN_FAMILY, CHAIN_EPOCH_COLUMN_FAMILY,
        CHAIN_EVENT_COLUMN_FAMILY, CanonicalStoreAdmissionExpectation,
        LIVE_TAIL_BLOCK_COLUMN_FAMILY, LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY, STORE_CONTROL_KEY,
        TRANSACTION_LOCATION_COLUMN_FAMILY, TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
        admit_existing_store, canonical_column_family_descriptors, validate_open_store_control,
    },
    settled_segments::{
        cache::SettledSegmentReaderCache,
        layout::SettledSegmentIndexEntry,
        store_access::{
            SegmentStoreContext, read_all_settled_segment_entries, segment_modules_for_stored_set,
            verified_read_options, verify_cold_segments_and_manifest,
        },
    },
    stored_set::{CANONICAL_DATA_COLUMN_FAMILIES, CANONICAL_PRE_PUBLICATION_EMPTY_FAMILIES},
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

/// Exclusive owner of a reopened canonical v2 build prepared for publication.
///
/// Trusted fresh-writer preparation and explicit cold certification are
/// separate builder APIs that both produce this publication capability.
pub struct ValidatedRocksDbCanonicalBuild {
    bounded_open: BoundedRocksDbOpen,
    build_plan: super::CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
    activation_schedule: CanonicalActivationSchedule,
    ready_evidence: CanonicalStoreReadyEvidence,
    construction_manifest: CanonicalConstructionManifestDraft,
    retained_sequence_checkpoints: VecDeque<CanonicalSequenceCheckpoint>,
    tip_metadata: ChainTipMetadata,
    sealed_segments: Vec<SettledSegmentIndexEntry>,
    segment_cache: Arc<SettledSegmentReaderCache>,
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
    build_plan: super::CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
    activation_schedule: CanonicalActivationSchedule,
    tree_state_checkpoint_policy: ConstructionTreeStateCheckpointPolicy,
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
        super::crash_point("canonical.publication.after_flush");
        drop(self);
        super::crash_point("canonical.publication.after_builder_drop");
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
        super::crash_point("canonical.publication.after_flush");
        drop(self);
        super::crash_point("canonical.publication.after_builder_drop");
        context.validate_cold_reopen(&built_database_identity)
    }
}

impl PublicationContext {
    fn from_builder(builder: &RocksDbCanonicalBuilder) -> Result<Self, CanonicalStoreError> {
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
        let activations = builder.construction_activations.clone().ok_or_else(|| {
            CanonicalStoreError::publication("canonical construction activation table is absent")
        })?;
        let activation_schedule = CanonicalActivationSchedule::first_revision(
            activations,
            builder.build_plan.network_upgrade_activations_fingerprint(),
            builder.build_plan.build_tip(),
        )
        .map_err(|error| CanonicalStoreError::publication(error.to_string()))?;
        Ok(Self {
            activation_schedule,
            store_path: builder.store_path.clone(),
            resource_budget: builder.resource_budget,
            network: builder.network,
            build_plan: builder.build_plan.clone(),
            cursor_auth_key: builder.cursor_auth_key,
            tree_state_checkpoint_policy: builder.tree_state_checkpoint_policy,
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
        let stored_set = self.build_plan.stored_set();
        let sealed_segments = read_all_settled_segment_entries(&bounded_open.db)?;
        let expected_modules = segment_modules_for_stored_set(stored_set);
        for entry in &sealed_segments {
            if entry.modules != expected_modules {
                return Err(CanonicalStoreError::publication(format!(
                    "settled segment {}..={} modules {:?} differ from admitted modules {:?}",
                    entry.start_height.value(),
                    entry.end_block.height.value(),
                    entry.modules,
                    expected_modules,
                )));
            }
        }
        let segment_cache =
            SettledSegmentReaderCache::with_admitted_corpus(&self.store_path, &sealed_segments)?;
        let ctx = SegmentStoreContext {
            db: &bounded_open.db,
            segment_cache: Some(&segment_cache),
            sealed_segments: Some(&sealed_segments),
        };
        validate_source_tip_checkpoint(
            &bounded_open.db,
            &self.build_plan,
            &self.block_evidence,
            &self.source_tip_checkpoint,
        )?;
        validate_trusted_fresh_tip_points(ctx, &self.build_plan, &self.block_evidence)?;

        let CanonicalTrustedFreshBlockEvidence {
            mut family_evidence,
            retained_sequence_checkpoints,
            logical_replay_bytes,
        } = self.trusted_fresh_block_evidence;
        family_evidence.push(self.trusted_fresh_subtree_family_evidence);
        for name in CANONICAL_PRE_PUBLICATION_EMPTY_FAMILIES {
            family_evidence.push(CanonicalConstructionFamilyEvidence::accumulator(name).finish());
        }
        validate_trusted_fresh_family_boundaries(&bounded_open.db, &family_evidence)?;

        let construction_manifest = CanonicalConstructionManifestDraft::new_trusted_fresh(
            CanonicalConstructionManifestInputs {
                build_plan: self.build_plan.clone(),
                tree_state_checkpoint_policy: self.tree_state_checkpoint_policy,
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
        super::crash_point("canonical.publication.after_cold_validation");
        Ok(ValidatedRocksDbCanonicalBuild {
            bounded_open,
            build_plan: self.build_plan,
            cursor_auth_key: self.cursor_auth_key,
            activation_schedule: self.activation_schedule,
            ready_evidence,
            construction_manifest,
            retained_sequence_checkpoints,
            tip_metadata: self.block_evidence.tip_metadata,
            sealed_segments,
            segment_cache,
        })
    }

    fn validate_cold_reopen(
        self,
        built_database_identity: &[u8],
    ) -> Result<ValidatedRocksDbCanonicalBuild, CanonicalStoreError> {
        let bounded_open = self.reopen_complete_build(built_database_identity)?;
        let stored_set = self.build_plan.stored_set();
        let (sealed_segments, segment_cache) = verify_cold_segments_and_manifest(
            &self.store_path,
            &bounded_open.db,
            self.network,
            stored_set,
        )?;
        let ctx = SegmentStoreContext {
            db: &bounded_open.db,
            segment_cache: Some(&segment_cache),
            sealed_segments: Some(&sealed_segments),
        };
        let cold_validation = validate_cold_block_families(
            ctx,
            &self.build_plan,
            self.tree_state_checkpoint_policy,
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
                tree_state_checkpoint_policy: self.tree_state_checkpoint_policy,
                source_checkpoint: self.source_tip_checkpoint,
                block_evidence: self.block_evidence,
                subtree_evidence: self.subtree_root_evidence,
                family_evidence,
                staged_ssts: self.staged_ssts,
            },
        )?;
        let ready_evidence = ready_evidence_from_cold_replay(&cold_validation.replay)?;
        super::crash_point("canonical.publication.after_cold_validation");
        Ok(ValidatedRocksDbCanonicalBuild {
            bounded_open,
            build_plan: self.build_plan,
            cursor_auth_key: self.cursor_auth_key,
            activation_schedule: self.activation_schedule,
            ready_evidence,
            construction_manifest,
            retained_sequence_checkpoints: cold_validation.replay.retained_sequence_checkpoints,
            tip_metadata: self.block_evidence.tip_metadata,
            sealed_segments,
            segment_cache,
        })
    }

    fn expected_building_control(&self) -> DecodedStoreControl {
        DecodedStoreControl {
            network: self.network,
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
            self.build_plan.reorg_policy(),
        );
        let (admitted_database_identity, admitted_control, _) =
            admit_existing_store(&self.store_path, &expectation, self.resource_budget)?;
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
        schema_version: crate::CANONICAL_STORE_SCHEMA_VERSION,
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
        schema_version: crate::CANONICAL_STORE_SCHEMA_VERSION,
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
    ctx: SegmentStoreContext<'_>,
    build_plan: &super::CanonicalStoreBuildPlan,
    evidence: &CanonicalBlockLoadEvidence,
) -> Result<(), CanonicalStoreError> {
    let first = BlockId::new(evidence.first_height, evidence.first_hash);
    let tip = BlockId::new(evidence.tip_height, evidence.tip_hash);
    if first.height != build_plan.first_retained_height()
        || tip != build_plan.build_tip()
        || retained_block_hash(ctx.db, first.height)? != first.hash
        || retained_block_hash(ctx.db, tip.height)? != tip.hash
        || read_persisted_tip_metadata(ctx, tip)? != evidence.tip_metadata
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

    /// Atomically publishes epoch 1, event 1, activation schedule revision 1, and READY with a synced WAL write.
    #[allow(
        clippy::too_many_lines,
        reason = "baseline publication binds the manifest, epoch, event, and READY control in one synced write"
    )]
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

        let manifest_details = self.construction_manifest.details();
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
        super::crash_point("canonical.publication.after_construction_manifest");
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
            control: encode_ready_store_control_with_revisions(
                &self.build_plan,
                self.cursor_auth_key,
                &ready_evidence,
                ControlRevisions {
                    activation_schedule_binding: Some(self.activation_schedule.binding()),
                    revised_reorg_policy: None,
                },
            )
            .map_err(|source| CanonicalStoreError::publication(source.to_string()))?,
        };
        let epoch_family = column_family(&self.bounded_open.db, CHAIN_EPOCH_COLUMN_FAMILY)?;
        let event_family = column_family(&self.bounded_open.db, CHAIN_EVENT_COLUMN_FAMILY)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&epoch_family, epoch_key, encoded_publication.epoch);
        batch.put_cf(&event_family, event_key, encoded_publication.event);
        batch.put(STORE_CONTROL_KEY, &encoded_publication.control);
        self.activation_schedule
            .put_into(&mut batch)
            .map_err(CanonicalStoreError::publication)?;
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(false);
        write_options.set_sync(true);
        super::crash_point("canonical.publication.before_atomic_write");
        self.bounded_open
            .db
            .write_opt(&batch, &write_options)
            .map_err(
                |source| CanonicalStoreError::PublicationWriteOutcomeUnknown {
                    path: self.bounded_open.db.path().to_path_buf(),
                    source,
                },
            )?;
        super::crash_point("canonical.publication.after_atomic_write");
        let ctx = SegmentStoreContext {
            db: &self.bounded_open.db,
            segment_cache: Some(&self.segment_cache),
            sealed_segments: Some(&self.sealed_segments),
        };
        let replay_tail = validate_exact_publication_readback(
            ctx,
            &self.build_plan,
            &ready_evidence,
            &encoded_publication,
            &self.activation_schedule,
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
            self.build_plan,
            self.cursor_auth_key,
            &ready_evidence,
            manifest_details,
            self.activation_schedule,
            replay_tail,
            self.sealed_segments,
            self.segment_cache,
        ))
    }
}

/// Validates one READY publication against its construction identity and the
/// store's effective reorg window, which bounds every retained transition.
///
/// Returns the unsettled replay tail it authenticated, which later
/// transitions extend incrementally.
pub(super) fn validate_ready_publication(
    ctx: SegmentStoreContext<'_>,
    build_plan: &super::CanonicalStoreBuildPlan,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<AuthenticatedReplayTail, CanonicalStoreError> {
    let db = ctx.db;
    validate_first_retained_block(db, ready_evidence.first_retained_block)?;
    validate_settled_live_tail_boundaries(ctx, build_plan, ready_evidence)?;
    let replay_tail = authenticate_ready_replay_tail(ctx, reorg_policy, ready_evidence)?;
    let baseline_tip = build_plan.build_tip();
    let mut epoch_cursor = CachedSequenceCursor::open_at_start(db, CHAIN_EPOCH_COLUMN_FAMILY)?;
    let mut event_cursor = CachedSequenceCursor::open_at_start(db, CHAIN_EVENT_COLUMN_FAMILY)?;
    let baseline_epoch =
        epoch_cursor.read_current_expected(BASELINE_EPOCH_ID.value(), decode_chain_epoch)?;
    let baseline_event =
        event_cursor.read_current_expected(BASELINE_EVENT_SEQUENCE, decode_chain_event)?;
    validate_baseline_publication(build_plan, ready_evidence, baseline_epoch, baseline_event)?;
    if ready_evidence.visible_epoch == BASELINE_EPOCH_ID {
        epoch_cursor.advance()?;
        epoch_cursor.ensure_exhausted(BASELINE_EPOCH_ID.value())?;
        event_cursor.advance()?;
        event_cursor.ensure_exhausted(BASELINE_EVENT_SEQUENCE)?;
        validate_epoch_record(
            ctx,
            ready_evidence.first_retained_block.height,
            baseline_tip,
            baseline_epoch,
        )?;
    } else {
        let context = LiveHistoryValidationContext {
            build_plan,
            reorg_policy,
            ready_evidence,
        };
        validate_live_history(
            ctx,
            &context,
            baseline_epoch,
            &mut epoch_cursor,
            &mut event_cursor,
        )?;
    }
    validate_permanent_reorg_archive(
        db,
        reorg_policy.reorg_window_blocks(),
        ready_evidence.visible_event_sequence,
    )?;
    super::event_lifecycle::validate_projection_lifecycle_records(db, ready_evidence)?;
    Ok(replay_tail)
}

fn validate_settled_live_tail_boundaries(
    ctx: SegmentStoreContext<'_>,
    build_plan: &super::CanonicalStoreBuildPlan,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<(), CanonicalStoreError> {
    let settled = match ctx.sealed_segments {
        Some(entries) => Cow::Borrowed(entries),
        None => Cow::Owned(read_all_settled_segment_entries(ctx.db)?),
    };
    let settled_tip_height = ready_evidence.sequence_checkpoint.through().height;
    let first_retained_height = ready_evidence.first_retained_block.height;
    let first_unsealed_height = if let Some(first) = settled.first()
        && let Some(last) = settled.last()
    {
        if first.start_height != first_retained_height {
            return Err(CanonicalStoreError::publication(
                "settled_segment_index must begin at first_retained_block",
            ));
        }
        if last.end_block.height > settled_tip_height {
            return Err(CanonicalStoreError::publication(
                "settled_segment_index extends past READY settled_tip",
            ));
        }
        if last.end_block.height == settled_tip_height
            && last.end_sequence_checkpoint != ready_evidence.sequence_checkpoint
        {
            return Err(CanonicalStoreError::publication(
                "terminal settled segment sequence checkpoint disagrees with READY settled checkpoint",
            ));
        }
        last.end_block.height.next()
    } else {
        Some(first_retained_height)
    };
    let live_files =
        ctx.db
            .live_files()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "live-tail admission live files",
                source,
            })?;
    let bounds = TailFamilyBounds {
        first_unsealed_height,
        visible_tip_height: ready_evidence.visible_tip.height,
        may_have_rows: true,
    };
    validate_tail_family_bounds(ctx.db, &live_files, LIVE_TAIL_BLOCK_COLUMN_FAMILY, bounds)?;
    validate_tail_family_bounds(
        ctx.db,
        &live_files,
        LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
        TailFamilyBounds {
            may_have_rows: build_plan.raw_blob_retention().full_blocks(),
            ..bounds
        },
    )
}

#[derive(Clone, Copy)]
struct TailFamilyBounds {
    first_unsealed_height: Option<BlockHeight>,
    visible_tip_height: BlockHeight,
    may_have_rows: bool,
}

#[derive(Clone, Copy)]
enum TailKeyBound {
    Unbounded,
    Lower([u8; 4]),
    Upper([u8; 4]),
}

fn has_forbidden_tail_entry(
    db: &DB,
    family: &Arc<BoundColumnFamily<'_>>,
    sst_overlaps: bool,
    bound: TailKeyBound,
    operation: &'static str,
) -> Result<bool, CanonicalStoreError> {
    let mut options = verified_read_options();
    match bound {
        TailKeyBound::Unbounded => {}
        TailKeyBound::Lower(lower) => options.set_iterate_lower_bound(lower),
        TailKeyBound::Upper(upper) => options.set_iterate_upper_bound(upper),
    }
    if !sst_overlaps {
        options.set_read_tier(ReadTier::Memtable);
    }
    let mut iter = db.raw_iterator_cf_opt(family, options);
    iter.seek_to_first();
    let valid = iter.valid();
    iter.status()
        .map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })?;
    Ok(valid)
}

fn validate_tail_family_bounds(
    db: &DB,
    live_files: &[LiveFile],
    family_name: &'static str,
    bounds: TailFamilyBounds,
) -> Result<(), CanonicalStoreError> {
    let family = column_family(db, family_name)?;
    let family_files = || {
        live_files
            .iter()
            .filter(|file| file.column_family_name == family_name)
    };
    if !bounds.may_have_rows {
        let has_sst = family_files().next().is_some();
        if has_forbidden_tail_entry(
            db,
            &family,
            has_sst,
            TailKeyBound::Unbounded,
            "tail family emptiness admission",
        )? {
            return Err(CanonicalStoreError::publication(format!(
                "{family_name} must be empty when full_blocks is disabled",
            )));
        }
        return Ok(());
    }
    let min_key = bounds.first_unsealed_height.map(encode_block_position);
    let sst_overlaps_lower = family_files().any(|file| {
        min_key.as_ref().is_none_or(|key| {
            file.start_key
                .as_deref()
                .is_none_or(|start| start < key.as_slice())
        })
    });
    let lower_bound = min_key.map_or(TailKeyBound::Unbounded, TailKeyBound::Upper);
    if has_forbidden_tail_entry(
        db,
        &family,
        sst_overlaps_lower,
        lower_bound,
        "tail family lower-bound admission",
    )? {
        return Err(CanonicalStoreError::publication(format!(
            "{family_name} contains unpruned rows inside settled segments",
        )));
    }
    if let Some(after_tip) = bounds.visible_tip_height.next() {
        let after_tip_key = encode_block_position(after_tip);
        let sst_overlaps_upper = family_files().any(|file| {
            file.end_key
                .as_deref()
                .is_none_or(|end| end >= after_tip_key.as_slice())
        });
        if has_forbidden_tail_entry(
            db,
            &family,
            sst_overlaps_upper,
            TailKeyBound::Lower(after_tip_key),
            "tail family upper-bound admission",
        )? {
            return Err(CanonicalStoreError::publication(format!(
                "{family_name} contains rows beyond visible_tip",
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_live_append_publication(
    ctx: SegmentStoreContext<'_>,
    ready_evidence: &CanonicalStoreReadyEvidence,
    previous_epoch_id: ChainEpochId,
) -> Result<(), CanonicalStoreError> {
    let db = ctx.db;
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
        ctx,
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
    ctx: SegmentStoreContext<'_>,
    ready_evidence: &CanonicalStoreReadyEvidence,
    previous_epoch_id: ChainEpochId,
    reverted_range: BlockHeightRange,
    committed_range: BlockHeightRange,
) -> Result<(), CanonicalStoreError> {
    let db = ctx.db;
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
        ctx,
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
    ctx: SegmentStoreContext<'_>,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<AuthenticatedReplayTail, CanonicalStoreError> {
    validate_ready_settlement_window(ctx.db, reorg_policy, ready_evidence)?;
    let replay_tail = AuthenticatedReplayTail::authenticate(
        ctx,
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
    ctx: SegmentStoreContext<'_>,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &CanonicalStoreReadyEvidence,
    retained_height: BlockHeight,
    replay_tail: &mut AuthenticatedReplayTail,
) -> Result<(), CanonicalStoreError> {
    let window = reorg_policy.reorg_window_blocks();
    if replay_tail.full_authentication_due(window) {
        *replay_tail = authenticate_ready_replay_tail(ctx, reorg_policy, ready_evidence)?;
        return Ok(());
    }
    validate_ready_settlement_window(ctx.db, reorg_policy, ready_evidence)?;
    replay_tail.reauthenticate_above(
        ctx,
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
    ctx: SegmentStoreContext<'_>,
    reorg_policy: super::CanonicalReorgPolicy,
    previous: &CanonicalStoreReadyEvidence,
    ready_evidence: &CanonicalStoreReadyEvidence,
    replay_tail: &mut AuthenticatedReplayTail,
) -> Result<(), CanonicalStoreError> {
    if let Some(retained_height) = lowest_retained_height_since(
        ctx.db,
        reorg_policy.reorg_window_blocks(),
        previous,
        ready_evidence,
    )? {
        let mut followed = replay_tail.clone();
        if advance_ready_replay_tail(
            ctx,
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
    *replay_tail = authenticate_ready_replay_tail(ctx, reorg_policy, ready_evidence)?;
    Ok(())
}

/// Returns the highest height every chain event after `previous` left
/// unchanged, or `None` when an intervening event is unavailable.
///
/// The incremental path re-derives at least every block the visible tip
/// advanced, so it cannot succeed once the tip advanced by more than one
/// reorg window; that case returns `None` before reading the events between.
fn lowest_retained_height_since(
    db: &DB,
    reorg_window_blocks: u32,
    previous: &CanonicalStoreReadyEvidence,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> Result<Option<BlockHeight>, CanonicalStoreError> {
    if ready_evidence.visible_event_sequence < previous.visible_event_sequence {
        return Ok(None);
    }
    if ready_evidence.visible_event_sequence == previous.visible_event_sequence {
        return Ok(Some(previous.visible_tip.height));
    }
    let tip_advance = ready_evidence
        .visible_tip
        .height
        .value()
        .saturating_sub(previous.visible_tip.height.value());
    if tip_advance > reorg_window_blocks {
        return Ok(None);
    }
    let Some(first_sequence) = previous.visible_event_sequence.checked_add(1) else {
        return Ok(None);
    };
    let retention_floor = super::event_lifecycle::canonical_event_retention_floor_from_db(
        db,
        ready_evidence.visible_event_sequence,
    )?;
    if first_sequence < retention_floor {
        return Ok(None);
    }
    if first_sequence == ready_evidence.visible_event_sequence {
        let encoded_event =
            match read_family_row(db, CHAIN_EVENT_COLUMN_FAMILY, &first_sequence.to_be_bytes()) {
                Ok(encoded) => encoded,
                Err(CanonicalStoreError::PublicationRefused { .. }) => return Ok(None),
                Err(other_error) => return Err(other_error),
            };
        let Ok(event) = decode_chain_event(&encoded_event) else {
            return Ok(None);
        };
        return Ok(unchanged_height_through_event(
            previous.visible_tip.height,
            first_sequence,
            &event,
        ));
    }
    let mut event_cursor =
        CachedSequenceCursor::open_from_sequence(db, CHAIN_EVENT_COLUMN_FAMILY, first_sequence)?;
    let mut retained_height = previous.visible_tip.height;
    for sequence in first_sequence..=ready_evidence.visible_event_sequence {
        let event = match event_cursor.read_expected_and_advance(sequence, decode_chain_event) {
            Ok(decoded) => decoded,
            Err(CanonicalStoreError::PublicationRefused { .. }) => return Ok(None),
            Err(other_error) => return Err(other_error),
        };
        let Some(next_retained) = unchanged_height_through_event(retained_height, sequence, &event)
        else {
            return Ok(None);
        };
        retained_height = next_retained;
    }
    Ok(Some(retained_height))
}

fn unchanged_height_through_event(
    retained_height: BlockHeight,
    expected_sequence: u64,
    event: &DecodedChainEvent,
) -> Option<BlockHeight> {
    if event.resulting_epoch_id.value() != expected_sequence {
        return None;
    }
    let first_changed = event
        .reverted_range
        .map_or(event.committed_range.start, |reverted| {
            reverted.start.min(event.committed_range.start)
        });
    let unchanged_through = first_changed.value().checked_sub(1)?;
    Some(retained_height.min(BlockHeight::new(unchanged_through)))
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

struct LiveHistoryValidationContext<'ctx> {
    build_plan: &'ctx super::CanonicalStoreBuildPlan,
    reorg_policy: super::CanonicalReorgPolicy,
    ready_evidence: &'ctx CanonicalStoreReadyEvidence,
}

fn validate_live_history(
    ctx: SegmentStoreContext<'_>,
    context: &LiveHistoryValidationContext<'_>,
    baseline_epoch: DecodedChainEpoch,
    epoch_cursor: &mut CachedSequenceCursor<'_>,
    event_cursor: &mut CachedSequenceCursor<'_>,
) -> Result<(), CanonicalStoreError> {
    let db = ctx.db;
    let ready_evidence = context.ready_evidence;
    if ready_evidence.visible_epoch.value() < 2
        || ready_evidence.visible_event_sequence != ready_evidence.visible_epoch.value()
    {
        return Err(CanonicalStoreError::publication(
            "READY live epoch and chain-event sequence must advance together",
        ));
    }
    let retention_floor = super::event_lifecycle::canonical_event_retention_floor_from_db(
        db,
        ready_evidence.visible_event_sequence,
    )?;
    let first_event_sequence = retention_floor.max(2);
    let (mut previous_epoch_id, mut previous_epoch) = seek_live_history_cursors(
        first_event_sequence,
        baseline_epoch,
        epoch_cursor,
        event_cursor,
    )?;
    let mut latest_event = None;
    for epoch_value in first_event_sequence..=ready_evidence.visible_epoch.value() {
        let chain_epoch_id = ChainEpochId::new(epoch_value);
        let chain_epoch =
            epoch_cursor.read_expected_and_advance(epoch_value, decode_chain_epoch)?;
        let chain_event =
            event_cursor.read_expected_and_advance(epoch_value, decode_chain_event)?;
        if chain_event.resulting_epoch_id != chain_epoch_id
            || chain_event.previous_epoch_id != previous_epoch_id.value()
            || chain_event.visible_tip != chain_epoch.visible_tip
            || chain_event.visible_block_count == 0
            || !live_transition_is_valid(
                context.build_plan,
                context.reorg_policy,
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
        latest_event = Some(chain_event);
    }
    epoch_cursor.ensure_exhausted(ready_evidence.visible_epoch.value())?;
    event_cursor.ensure_exhausted(ready_evidence.visible_event_sequence)?;
    validate_epoch_record(
        ctx,
        ready_evidence.first_retained_block.height,
        ready_evidence.visible_tip,
        previous_epoch,
    )?;
    let current_event = latest_event.ok_or_else(|| {
        CanonicalStoreError::publication("READY live history must contain at least one transition")
    })?;
    if !live_history_matches_ready_fence(previous_epoch, current_event, ready_evidence) {
        return Err(CanonicalStoreError::publication(
            "READY live history does not end at the authenticated visible fence",
        ));
    }
    Ok(())
}

fn seek_live_history_cursors(
    first_event_sequence: u64,
    baseline_epoch: DecodedChainEpoch,
    epoch_cursor: &mut CachedSequenceCursor<'_>,
    event_cursor: &mut CachedSequenceCursor<'_>,
) -> Result<(ChainEpochId, DecodedChainEpoch), CanonicalStoreError> {
    if first_event_sequence == 2 {
        epoch_cursor.advance()?;
        event_cursor.advance()?;
        return Ok((BASELINE_EPOCH_ID, baseline_epoch));
    }
    let previous_epoch_id = ChainEpochId::new(first_event_sequence - 1);
    epoch_cursor.seek_from_sequence(previous_epoch_id.value())?;
    let previous_epoch =
        epoch_cursor.read_expected_and_advance(previous_epoch_id.value(), decode_chain_epoch)?;
    event_cursor.seek_from_sequence(first_event_sequence)?;
    Ok((previous_epoch_id, previous_epoch))
}

fn live_history_matches_ready_fence(
    final_epoch: DecodedChainEpoch,
    final_event: DecodedChainEvent,
    ready_evidence: &CanonicalStoreReadyEvidence,
) -> bool {
    let epoch_matches_ready = final_epoch.visible_tip == ready_evidence.visible_tip;
    let expected_epoch_id = ready_evidence.visible_epoch;
    let expected_tip = ready_evidence.visible_tip;
    let expected_block_count = ready_evidence.visible_block_count;
    let expected_sequence_digest = ready_evidence.visible_sequence_digest;
    let event_matches_ready = final_event.resulting_epoch_id == expected_epoch_id
        && final_event.visible_tip == expected_tip
        && final_event.visible_block_count == expected_block_count
        && final_event.visible_sequence_digest == expected_sequence_digest;
    epoch_matches_ready && event_matches_ready
}

fn live_transition_is_valid(
    build_plan: &super::CanonicalStoreBuildPlan,
    reorg_policy: super::CanonicalReorgPolicy,
    previous_epoch: DecodedChainEpoch,
    chain_epoch: DecodedChainEpoch,
    chain_event: DecodedChainEvent,
) -> Result<bool, CanonicalStoreError> {
    if !epoch_bounds_are_valid(build_plan.first_retained_height(), chain_epoch) {
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
    ctx: SegmentStoreContext<'_>,
    first_height: BlockHeight,
    expected_visible_tip: BlockId,
    epoch: DecodedChainEpoch,
) -> Result<(), CanonicalStoreError> {
    if epoch.visible_tip != expected_visible_tip
        || !epoch_bounds_are_valid(first_height, epoch)
        || retained_block_hash(ctx.db, expected_visible_tip.height)? != expected_visible_tip.hash
        || retained_block_hash(ctx.db, epoch.settled_tip.height)? != epoch.settled_tip.hash
        || epoch.tip_metadata != read_persisted_tip_metadata(ctx, expected_visible_tip)?
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

#[allow(
    clippy::too_many_lines,
    reason = "cold publication validates each source-derived family in one ordered readback path"
)]
fn validate_cold_block_families(
    ctx: SegmentStoreContext<'_>,
    build_plan: &super::CanonicalStoreBuildPlan,
    tree_state_checkpoint_policy: ConstructionTreeStateCheckpointPolicy,
    expected: &CanonicalBlockLoadEvidence,
) -> Result<ColdCanonicalBlockValidation, CanonicalStoreError> {
    let retained_checkpoint_count =
        usize::try_from(u64::from(build_plan.reorg_policy().reorg_window_blocks()) + 1)
            .map_err(|_| CanonicalStoreError::publication("reorg window exceeds usize"))?;
    let replay = validate_persisted_block_replays_with_checkpoints(ctx, retained_checkpoint_count)?;
    if !replay.evidence.has_same_sequence(expected) {
        return Err(CanonicalStoreError::BlockLoadReadbackMismatch);
    }
    validate_persisted_commitment_tree_families(
        ctx,
        build_plan,
        tree_state_checkpoint_policy,
        expected,
    )?;
    if !build_plan.raw_blob_retention().full_blocks() && expected.live_tail_raw_block_count != 0 {
        return Err(CanonicalStoreError::BlockLoadReadbackMismatch);
    }
    if build_plan.raw_blob_retention().full_blocks()
        && expected.live_tail_raw_block_count != expected.live_tail_block_count
    {
        return Err(CanonicalStoreError::BlockLoadReadbackMismatch);
    }
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
            LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
            expected.live_tail_raw_block_count,
            expected.live_tail_raw_block_logical_bytes,
        ),
        (
            TRANSACTION_LOCATION_COLUMN_FAMILY,
            expected.transaction_location_count,
            expected.transaction_location_logical_bytes,
        ),
        (
            TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
            expected.tree_state_checkpoint_count,
            expected.tree_state_checkpoint_logical_bytes,
        ),
    ];
    let mut family_evidence = Vec::with_capacity(
        expected_families.len() + 2 + CANONICAL_PRE_PUBLICATION_EMPTY_FAMILIES.len(),
    );
    family_evidence.push(replay.evidence.settled_segment_index_evidence.clone());
    family_evidence.push(replay.evidence.live_tail_block_evidence.clone());
    record_cold_family_scan(&ColdFamilyScanEvidence::from_replay(
        &replay.evidence.settled_segment_index_evidence,
        replay.evidence.elapsed,
    ));
    record_cold_family_scan(&ColdFamilyScanEvidence::from_replay(
        &replay.evidence.live_tail_block_evidence,
        replay.evidence.elapsed,
    ));
    for (name, expected_count, expected_logical_bytes) in expected_families {
        let observed = scan_family(ctx.db, name)?;
        if !observed.matches(expected_count, expected_logical_bytes) {
            return Err(CanonicalStoreError::publication(format!(
                "cold {name} count or logical bytes differ from the source load"
            )));
        }
        family_evidence.push(observed.evidence);
    }
    for name in CANONICAL_PRE_PUBLICATION_EMPTY_FAMILIES {
        family_evidence.push(require_empty_family(ctx.db, name)?);
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
    ctx: SegmentStoreContext<'_>,
    build_plan: &super::CanonicalStoreBuildPlan,
    ready_evidence: &CanonicalStoreReadyEvidence,
    expected: &EncodedBaselinePublication,
    schedule: &CanonicalActivationSchedule,
) -> Result<AuthenticatedReplayTail, CanonicalStoreError> {
    let db = ctx.db;
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
            "atomic baseline publication readback differs from its epoch, event, or control write",
        ));
    }
    let decoded_control = decode_store_control(db.path(), &expected.control)?;
    if decoded_control.build_state != CanonicalStoreBuildState::Ready(*ready_evidence) {
        return Err(CanonicalStoreError::publication(
            "READY control does not contain the validated baseline evidence",
        ));
    }
    let persisted_schedule = CanonicalActivationSchedule::read_bound(
        db,
        db.path(),
        schedule.binding(),
        build_plan
            .network_upgrade_activations_fingerprint()
            .as_bytes(),
    )?;
    if persisted_schedule != *schedule {
        return Err(CanonicalStoreError::publication(
            "baseline activation schedule readback differs from its write",
        ));
    }
    validate_ready_publication(ctx, build_plan, build_plan.reorg_policy(), ready_evidence)
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

    fn from_replay(evidence: &CanonicalConstructionFamilyEvidence, elapsed: Duration) -> Self {
        Self {
            evidence: evidence.clone(),
            elapsed,
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

struct CachedSequenceCursor<'db> {
    family_name: &'static str,
    iterator: rust_rocksdb::DBRawIteratorWithThreadMode<'db, DB>,
    previous_key: Option<u64>,
}

impl<'db> CachedSequenceCursor<'db> {
    fn open_at_start(db: &'db DB, family_name: &'static str) -> Result<Self, CanonicalStoreError> {
        let family = column_family(db, family_name)?;
        let mut read_options = ReadOptions::default();
        read_options.fill_cache(true);
        let mut iterator = db.raw_iterator_cf_opt(&family, read_options);
        iterator.seek_to_first();
        let cursor = Self {
            family_name,
            iterator,
            previous_key: None,
        };
        cursor.check_status("READY publication history scan")?;
        Ok(cursor)
    }

    fn open_from_sequence(
        db: &'db DB,
        family_name: &'static str,
        first_sequence: u64,
    ) -> Result<Self, CanonicalStoreError> {
        Self::open_from_sequence_with_cache(db, family_name, first_sequence, true)
    }

    fn open_from_sequence_with_cache(
        db: &'db DB,
        family_name: &'static str,
        first_sequence: u64,
        fill_cache: bool,
    ) -> Result<Self, CanonicalStoreError> {
        let family = column_family(db, family_name)?;
        let mut read_options = ReadOptions::default();
        read_options.fill_cache(fill_cache);
        let iterator = db.raw_iterator_cf_opt(&family, read_options);
        let mut cursor = Self {
            family_name,
            iterator,
            previous_key: None,
        };
        cursor.seek_from_sequence(first_sequence)?;
        Ok(cursor)
    }

    fn seek_from_sequence(&mut self, first_sequence: u64) -> Result<(), CanonicalStoreError> {
        if first_sequence <= 1 {
            self.previous_key = None;
            self.iterator.seek_to_first();
        } else {
            let start_key = ascending_u64_key_scan_start(first_sequence);
            self.previous_key = Some(first_sequence - 1);
            self.iterator.seek(start_key);
        }
        self.check_status("READY publication history seek")
    }

    fn read_current_expected<Decoded, Decode>(
        &mut self,
        expected_sequence: u64,
        decode: Decode,
    ) -> Result<Decoded, CanonicalStoreError>
    where
        Decode: FnOnce(&[u8]) -> Result<Decoded, CanonicalStoreError>,
    {
        if !self.iterator.valid() {
            self.check_status("READY publication history scan")?;
            return Err(CanonicalStoreError::publication(format!(
                "{} row {expected_sequence} is absent",
                self.family_name
            )));
        }
        let Some((key_bytes, row_bytes)) = self.iterator.item() else {
            self.check_status("READY publication history scan")?;
            return Err(CanonicalStoreError::publication(format!(
                "{} row {expected_sequence} is absent",
                self.family_name
            )));
        };
        let observed_sequence =
            decode_u64_key_ascending(self.family_name, key_bytes, self.previous_key)?;
        if observed_sequence != expected_sequence {
            return Err(CanonicalStoreError::publication(format!(
                "{} expected sequence {expected_sequence}, found {observed_sequence}",
                self.family_name
            )));
        }
        let decoded = decode(row_bytes)?;
        self.previous_key = Some(observed_sequence);
        Ok(decoded)
    }

    fn read_expected_and_advance<Decoded, Decode>(
        &mut self,
        expected_sequence: u64,
        decode: Decode,
    ) -> Result<Decoded, CanonicalStoreError>
    where
        Decode: FnOnce(&[u8]) -> Result<Decoded, CanonicalStoreError>,
    {
        let decoded = self.read_current_expected(expected_sequence, decode)?;
        self.advance()?;
        Ok(decoded)
    }

    fn advance(&mut self) -> Result<(), CanonicalStoreError> {
        self.iterator.next();
        if !self.iterator.valid() {
            self.check_status("READY publication history scan")?;
        }
        Ok(())
    }

    fn ensure_exhausted(&self, visible_sequence: u64) -> Result<(), CanonicalStoreError> {
        if !self.iterator.valid() {
            return self.check_status("READY publication trailing-row check");
        }
        if let Some((key_bytes, _)) = self.iterator.item() {
            let trailing_sequence =
                decode_u64_key_ascending(self.family_name, key_bytes, self.previous_key)?;
            return Err(CanonicalStoreError::publication(format!(
                "{} contains unexpected row {trailing_sequence} after visible sequence {visible_sequence}",
                self.family_name
            )));
        }
        self.check_status("READY publication trailing-row check")?;
        Err(CanonicalStoreError::publication(format!(
            "{} contains unexpected trailing row after visible sequence {visible_sequence}",
            self.family_name
        )))
    }

    fn check_status(&self, operation: &'static str) -> Result<(), CanonicalStoreError> {
        self.iterator
            .status()
            .map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })
    }
}

pub(super) struct ChainEpochCreatedAtCursor<'db> {
    cursor: CachedSequenceCursor<'db>,
}

impl<'db> ChainEpochCreatedAtCursor<'db> {
    pub(super) fn open_from_sequence(
        db: &'db DB,
        first_sequence: u64,
    ) -> Result<Self, CanonicalStoreError> {
        Ok(Self {
            cursor: CachedSequenceCursor::open_from_sequence_with_cache(
                db,
                CHAIN_EPOCH_COLUMN_FAMILY,
                first_sequence,
                false,
            )?,
        })
    }

    pub(super) fn read_expected_created_at_and_advance(
        &mut self,
        expected_sequence: u64,
    ) -> Result<UnixTimestampMillis, CanonicalStoreError> {
        Ok(self
            .cursor
            .read_expected_and_advance(expected_sequence, decode_chain_epoch)?
            .created_at)
    }
}

/// Returns the seek key immediately preceding `sequence`.
///
/// The 9-byte key sorts strictly after `(sequence - 1)`'s 8-byte big-endian
/// key, skipping 8-byte point tombstones below `sequence` while still landing
/// on any malformed 9-byte key extending `sequence - 1`.
fn ascending_u64_key_scan_start(sequence: u64) -> [u8; 9] {
    let predecessor = sequence.saturating_sub(1).to_be_bytes();
    let mut start = [0_u8; 9];
    start[..8].copy_from_slice(&predecessor);
    start
}

fn decode_u64_key_ascending(
    family_name: &'static str,
    key: &[u8],
    previous_key: Option<u64>,
) -> Result<u64, CanonicalStoreError> {
    let key_bytes: [u8; 8] = key.try_into().map_err(|_| {
        CanonicalStoreError::publication(format!(
            "{family_name} key has length {}, expected 8",
            key.len()
        ))
    })?;
    let sequence = u64::from_be_bytes(key_bytes);
    if let Some(previous) = previous_key
        && sequence <= previous
    {
        return Err(CanonicalStoreError::publication(format!(
            "{family_name} key {sequence} is not strictly ascending after {previous}"
        )));
    }
    Ok(sequence)
}

fn read_family_row(
    db: &DB,
    name: &'static str,
    key: &[u8],
) -> Result<Vec<u8>, CanonicalStoreError> {
    let family = column_family(db, name)?;
    let mut read_options = ReadOptions::default();
    read_options.fill_cache(true);
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

    #[test]
    fn ascending_u64_key_decoding_rejects_malformed_lengths_and_non_monotonic_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            decode_u64_key_ascending(CHAIN_EPOCH_COLUMN_FAMILY, &1_u64.to_be_bytes(), None)?,
            1
        );
        assert_eq!(
            decode_u64_key_ascending(CHAIN_EPOCH_COLUMN_FAMILY, &2_u64.to_be_bytes(), Some(1))?,
            2
        );
        assert_eq!(
            decode_u64_key_ascending(
                CHAIN_EVENT_COLUMN_FAMILY,
                &u64::MAX.to_be_bytes(),
                Some(u64::MAX - 1)
            )?,
            u64::MAX
        );
        for bad_key in [
            &[][..],
            &[0_u8; 4][..],
            &[0_u8; 7][..],
            &[0_u8; 9][..],
            &ascending_u64_key_scan_start(5)[..],
        ] {
            assert!(
                decode_u64_key_ascending(CHAIN_EPOCH_COLUMN_FAMILY, bad_key, None).is_err(),
                "expected malformed key of len {} to fail",
                bad_key.len()
            );
        }
        assert!(
            decode_u64_key_ascending(CHAIN_EPOCH_COLUMN_FAMILY, &2_u64.to_be_bytes(), Some(2))
                .is_err()
        );
        assert!(
            decode_u64_key_ascending(CHAIN_EPOCH_COLUMN_FAMILY, &1_u64.to_be_bytes(), Some(2))
                .is_err()
        );
        let scan_start = ascending_u64_key_scan_start(5);
        assert!(scan_start.as_slice() > 4_u64.to_be_bytes().as_slice());
        assert!(scan_start.as_slice() < 5_u64.to_be_bytes().as_slice());
        Ok(())
    }

    fn ready_evidence() -> CanonicalStoreReadyEvidence {
        CanonicalStoreReadyEvidence {
            schema_version: crate::CANONICAL_STORE_SCHEMA_VERSION,
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
