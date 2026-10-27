//! Process-local secondary access to an admitted canonical store.

use std::path::{Path, PathBuf};

use rust_rocksdb::{DB, DEFAULT_COLUMN_FAMILY_NAME, Options};
use zinder_core::{
    BlockHeight, BlockHeightRange, CanonicalBlockFacts, CanonicalHistoryBounds, Network,
    NetworkUpgradeActivations, NetworkUpgradeActivationsFingerprint,
};

use crate::{
    BoundedRocksDbOpen, RawBlobRetention, RocksDbIoMode, RocksDbOpenRole, RocksDbResourceBudget,
    open_bounded_rocksdb,
};

use super::{
    CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalRetainedEvent,
    CanonicalSequenceCheckpoint, CanonicalStoreBuildState, CanonicalStoreError,
    CanonicalStoreReadyEvidence, CanonicalStoreWorkload,
    activation_schedule::CanonicalActivationSchedule,
    block_replay::{CanonicalReplayRangeScan, CanonicalReplayScan, read_replay_facts_at},
    construction_manifest::validate_ready_construction_manifest,
    event_lifecycle::{canonical_event_history_from_db, canonical_event_retention_floor_from_db},
    live_commit::event_fence_from_ready,
    mempool_lifecycle::validate_mempool_lifecycle_admission,
    publication::{follow_ready_replay_tail, validate_ready_publication},
    replay_tail::AuthenticatedReplayTail,
    rocksdb::{
        CANONICAL_DATA_COLUMN_FAMILIES, CanonicalStoreAdmissionExpectation,
        canonical_column_family_descriptors, canonical_store_path, validate_open_store_control,
        validate_resource_budget,
    },
};

/// Result of one explicit canonical-secondary catch-up barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalSecondaryCatchupOutcome {
    before: CanonicalEventFence,
    after: CanonicalEventFence,
}

impl CanonicalSecondaryCatchupOutcome {
    /// Returns the authenticated fence visible before catch-up.
    #[must_use]
    pub const fn before(self) -> CanonicalEventFence {
        self.before
    }

    /// Returns the authenticated fence visible after catch-up.
    #[must_use]
    pub const fn after(self) -> CanonicalEventFence {
        self.after
    }

    /// Reports whether catch-up advanced the visible canonical fence.
    #[must_use]
    pub const fn advanced(self) -> bool {
        self.before.chain_epoch_id().value() < self.after.chain_epoch_id().value()
    }
}

/// One admitted read-only `RocksDB` secondary for a canonical primary.
///
/// The reader owns a process-unique secondary metadata path. It cannot publish
/// canonical transitions because the primary mutation API is not implemented
/// on this type. Initial admission validates the complete READY publication;
/// later catch-up is explicit and authenticates the bounded visible replay tail
/// from the persisted settled checkpoint before returning the new fence.
pub struct RocksDbCanonicalSecondary {
    pub(super) bounded_open: BoundedRocksDbOpen,
    primary_path: PathBuf,
    expectation: CanonicalStoreAdmissionExpectation,
    workload: CanonicalStoreWorkload,
    pub(super) build_plan: super::CanonicalStoreBuildPlan,
    pub(super) ready_evidence: CanonicalStoreReadyEvidence,
    pub(super) cursor_auth_key: [u8; 32],
    pub(super) activation_schedule: Option<CanonicalActivationSchedule>,
    reorg_policy: super::CanonicalReorgPolicy,
    pub(super) replay_tail: AuthenticatedReplayTail,
}

impl RocksDbCanonicalSecondary {
    /// Returns the writer-admitted schedule revision observed at the last
    /// catch-up, or `None` before the writer's first admission.
    #[must_use]
    pub const fn network_upgrade_schedule(&self) -> Option<&CanonicalActivationSchedule> {
        self.activation_schedule.as_ref()
    }

    /// Returns the complete writer-admitted activation table observed at the
    /// last catch-up.
    #[must_use]
    pub const fn network_upgrade_activations(&self) -> Option<&NetworkUpgradeActivations> {
        match self.activation_schedule.as_ref() {
            Some(schedule) => Some(schedule.activations()),
            None => None,
        }
    }

    /// Opens one exactly admitted secondary and catches it up to the primary's current state.
    ///
    /// The expected raw-blob retention is an immutable store identity, not a
    /// request to infer coverage from currently present rows.
    #[allow(
        clippy::too_many_arguments,
        reason = "secondary admission keeps both filesystem identities and every immutable canonical contract explicit"
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "initial secondary admission binds control, schedule, manifest, and READY publication"
    )]
    pub fn open_ready(
        primary_path: impl AsRef<Path>,
        secondary_path: impl AsRef<Path>,
        expected_network_upgrade_activations: &NetworkUpgradeActivations,
        expected_workload: CanonicalStoreWorkload,
        expected_raw_blob_retention: RawBlobRetention,
        expected_reorg_policy: super::CanonicalReorgPolicy,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        validate_resource_budget(resource_budget)?;
        let primary_path = canonical_store_path(primary_path.as_ref())?;
        let secondary_path = secondary_path.as_ref();
        if secondary_path == primary_path {
            return Err(CanonicalStoreError::admission(
                &primary_path,
                "secondary metadata path must differ from the canonical primary path",
            ));
        }
        let expectation = CanonicalStoreAdmissionExpectation::from_activations(
            expected_network_upgrade_activations,
            expected_workload,
            expected_raw_blob_retention,
            expected_reorg_policy,
        );
        require_exact_primary_column_family_metadata(&primary_path)?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Secondary {
                primary_path: &primary_path,
                secondary_path,
            },
            resource_budget,
            canonical_column_family_descriptors,
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "open canonical secondary",
            source,
        })?;
        bounded_open
            .db
            .try_catch_up_with_primary()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "initial canonical secondary catch-up",
                source,
            })?;
        require_exact_secondary_column_families(&bounded_open, &primary_path)?;
        let opened_control =
            validate_open_store_control(&bounded_open.db, &primary_path, &expectation)?;
        validate_mempool_lifecycle_admission(
            &bounded_open.db,
            opened_control.network,
            opened_control.cursor_auth_key,
        )?;
        let CanonicalStoreBuildState::Ready(ready_evidence) = opened_control.build_state else {
            return Err(CanonicalStoreError::StoreNotReady { path: primary_path });
        };
        validate_ready_construction_manifest(
            &primary_path,
            &ready_evidence,
            opened_control.workload,
            &opened_control.build_plan,
        )?;
        let reorg_policy = opened_control.effective_reorg_policy();
        let replay_tail = validate_ready_publication(
            &bounded_open.db,
            &opened_control.build_plan,
            reorg_policy,
            &ready_evidence,
        )?;
        let activation_schedule = opened_control
            .activation_schedule_binding
            .map(|binding| {
                CanonicalActivationSchedule::read_bound(
                    &bounded_open.db,
                    &primary_path,
                    binding,
                    opened_control
                        .build_plan
                        .network_upgrade_activations_fingerprint()
                        .as_bytes(),
                )
            })
            .transpose()?;
        Ok(Self {
            bounded_open,
            primary_path,
            // The reader's node table is checked only at open. Catch-ups
            // authenticate the schedule journal and follow each revision the
            // writer admits, so a long-running reader never pins its startup
            // observation. The reorg window stays exact: a writer raise
            // after open fails catch-up until the reader restarts with it.
            expectation: CanonicalStoreAdmissionExpectation::from_build_plan(
                &opened_control.build_plan,
                expected_workload,
                reorg_policy,
            ),
            workload: expected_workload,
            build_plan: opened_control.build_plan,
            ready_evidence,
            cursor_auth_key: opened_control.cursor_auth_key,
            activation_schedule,
            reorg_policy,
            replay_tail,
        })
    }

    /// Catches up and authenticates the new visible replay tail.
    ///
    /// The initial open has already cold-validated every canonical family and
    /// derived the unsettled replay tail. A live catch-up validates the exact
    /// control contract, returns early when the READY evidence is unchanged,
    /// and otherwise re-derives only the replay rows the intervening chain
    /// events changed. It falls back to re-deriving the whole unsettled suffix,
    /// at most one reorg window, when those events are unavailable, when the
    /// incremental check fails, and once per window of incrementally derived
    /// rows. It never repeats a full historical scan for each writer
    /// transition.
    pub fn try_catch_up(
        &mut self,
    ) -> Result<CanonicalSecondaryCatchupOutcome, CanonicalStoreError> {
        let before = self.event_fence();
        self.bounded_open
            .db
            .try_catch_up_with_primary()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "canonical secondary catch-up",
                source,
            })?;
        require_exact_secondary_column_families(&self.bounded_open, &self.primary_path)?;
        let opened_control = validate_open_store_control(
            &self.bounded_open.db,
            &self.primary_path,
            &self.expectation,
        )?;
        validate_mempool_lifecycle_admission(
            &self.bounded_open.db,
            opened_control.network,
            opened_control.cursor_auth_key,
        )?;
        let CanonicalStoreBuildState::Ready(ready_evidence) = opened_control.build_state else {
            return Err(CanonicalStoreError::StoreNotReady {
                path: self.primary_path.clone(),
            });
        };
        if opened_control.build_plan != self.build_plan {
            return Err(CanonicalStoreError::admission(
                &self.primary_path,
                "canonical build identity changed during secondary catch-up",
            ));
        }
        if opened_control.cursor_auth_key != self.cursor_auth_key {
            return Err(CanonicalStoreError::admission(
                &self.primary_path,
                "canonical cursor authentication key changed during secondary catch-up",
            ));
        }
        if ready_evidence.visible_event_sequence < self.ready_evidence.visible_event_sequence {
            return Err(CanonicalStoreError::admission(
                &self.primary_path,
                "canonical event sequence regressed during secondary catch-up",
            ));
        }
        if ready_evidence.visible_event_sequence == self.ready_evidence.visible_event_sequence
            && ready_evidence != self.ready_evidence
        {
            return Err(CanonicalStoreError::admission(
                &self.primary_path,
                "canonical READY publication changed without advancing its event sequence",
            ));
        }
        // Unchanged READY evidence is still explained by the tail this reader
        // already authenticated; otherwise re-derive only what changed.
        if ready_evidence != self.ready_evidence {
            follow_ready_replay_tail(
                &self.bounded_open.db,
                self.reorg_policy,
                &self.ready_evidence,
                &ready_evidence,
                &mut self.replay_tail,
            )?;
        }
        self.ready_evidence = ready_evidence;
        self.activation_schedule = opened_control
            .activation_schedule_binding
            .map(|binding| {
                CanonicalActivationSchedule::read_bound(
                    &self.bounded_open.db,
                    &self.primary_path,
                    binding,
                    self.build_plan
                        .network_upgrade_activations_fingerprint()
                        .as_bytes(),
                )
            })
            .transpose()?;
        Ok(CanonicalSecondaryCatchupOutcome {
            before,
            after: self.event_fence(),
        })
    }

    /// Returns the immutable canonical network.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.build_plan.network()
    }

    /// Returns the admitted network-upgrade activation identity.
    #[must_use]
    pub const fn network_upgrade_activations_fingerprint(
        &self,
    ) -> NetworkUpgradeActivationsFingerprint {
        self.build_plan.network_upgrade_activations_fingerprint()
    }

    /// Returns the immutable canonical workload.
    #[must_use]
    pub const fn workload(&self) -> CanonicalStoreWorkload {
        self.workload
    }

    /// Returns the effective maximum canonical replacement depth admitted at
    /// open: the construction policy or a writer-admitted raise.
    #[must_use]
    pub const fn reorg_policy(&self) -> super::CanonicalReorgPolicy {
        self.reorg_policy
    }

    /// Returns the immutable raw-blob retention authenticated at admission.
    #[must_use]
    pub const fn raw_blob_retention(&self) -> RawBlobRetention {
        self.build_plan.raw_blob_retention()
    }

    /// Returns the immutable retained canonical range.
    #[must_use]
    pub const fn history_bounds(&self) -> CanonicalHistoryBounds {
        self.build_plan.history_bounds()
    }

    /// Returns the complete admitted build identity.
    #[must_use]
    pub const fn build_plan(&self) -> &super::CanonicalStoreBuildPlan {
        &self.build_plan
    }

    /// Returns the currently admitted READY evidence.
    #[must_use]
    pub const fn ready_evidence(&self) -> CanonicalStoreReadyEvidence {
        self.ready_evidence
    }

    /// Returns the authenticated settled sequence checkpoint.
    #[must_use]
    pub const fn sequence_checkpoint(&self) -> CanonicalSequenceCheckpoint {
        self.ready_evidence.sequence_checkpoint
    }

    /// Returns the exact authenticated source fence visible to this secondary.
    #[must_use]
    pub fn event_fence(&self) -> CanonicalEventFence {
        event_fence_from_ready(&self.ready_evidence)
    }

    /// Scans and authenticates the canonical replay visible to this secondary.
    pub fn scan_canonical_replay(&self) -> Result<CanonicalReplayScan<'_>, CanonicalStoreError> {
        CanonicalReplayScan::new(&self.bounded_open.db, &self.ready_evidence)
    }

    /// Reads one bounded connected replay range from this admitted fence.
    ///
    /// This is the incremental-projector path. It authenticates each decoded
    /// row against its canonical predecessor without rescanning the historical
    /// prefix; the caller must compare its resulting source digest and event
    /// cursor with the same secondary fence before publishing derived state.
    pub fn scan_canonical_replay_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<CanonicalReplayRangeScan<'_>, CanonicalStoreError> {
        CanonicalReplayRangeScan::new(&self.bounded_open.db, &self.ready_evidence, range)
    }

    /// Reads one retained block's canonical facts by height.
    ///
    /// Heights outside the admitted retained range return `None`. A missing row
    /// inside that range is a canonical replay-sequence error: READY admission
    /// proves the retained range is contiguous.
    pub fn block_replay_facts_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CanonicalBlockFacts>, CanonicalStoreError> {
        read_replay_facts_at(&self.bounded_open.db, &self.ready_evidence, height)
    }

    /// Reads retained canonical events from this secondary's admitted READY fence.
    ///
    /// Consumers must call [`Self::try_catch_up`] before a new read when they
    /// require a later primary fence; this method never reads the primary
    /// directly or mixes event rows from two secondary catch-up epochs.
    pub fn canonical_event_history(
        &self,
        request: CanonicalEventHistoryRequest<'_>,
    ) -> Result<Vec<CanonicalRetainedEvent>, CanonicalStoreError> {
        canonical_event_history_from_db(
            &self.bounded_open.db,
            self.ready_evidence.visible_event_sequence,
            request,
        )
    }

    /// Returns this secondary's admitted inclusive retained-event floor.
    pub fn canonical_event_retention_floor(&self) -> Result<u64, CanonicalStoreError> {
        canonical_event_retention_floor_from_db(
            &self.bounded_open.db,
            self.ready_evidence.visible_event_sequence,
        )
    }

    /// Returns the filesystem I/O mode selected for this secondary.
    #[must_use]
    pub const fn io_mode(&self) -> RocksDbIoMode {
        self.bounded_open.io_mode
    }
}

fn required_canonical_column_family_names() -> Vec<String> {
    let mut expected = CANONICAL_DATA_COLUMN_FAMILIES
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    expected.push(DEFAULT_COLUMN_FAMILY_NAME.to_owned());
    expected.sort_unstable();
    expected
}

fn require_exact_primary_column_family_metadata(
    primary_path: &Path,
) -> Result<(), CanonicalStoreError> {
    let observed = DB::list_cf(&Options::default(), primary_path).map_err(|source| {
        CanonicalStoreError::admission(
            primary_path,
            format!("canonical column-family metadata discovery failed: {source}"),
        )
    })?;
    require_exact_column_family_names(primary_path, observed)
}

fn require_exact_secondary_column_families(
    bounded_open: &BoundedRocksDbOpen,
    primary_path: &Path,
) -> Result<(), CanonicalStoreError> {
    require_exact_column_family_names(primary_path, bounded_open.db.cf_names())
}

fn require_exact_column_family_names(
    primary_path: &Path,
    mut observed: Vec<String>,
) -> Result<(), CanonicalStoreError> {
    let expected = required_canonical_column_family_names();
    observed.sort_unstable();
    if observed != expected {
        return Err(CanonicalStoreError::admission(
            primary_path,
            format!(
                "column families {observed:?} do not exactly match required canonical set {expected:?}"
            ),
        ));
    }
    Ok(())
}
