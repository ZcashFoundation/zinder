use std::{fs, path::Path, sync::Arc};

use parking_lot::Mutex;
use rust_rocksdb::{
    Cache, ColumnFamilyDescriptor, DB, DBCompressionType, DEFAULT_COLUMN_FAMILY_NAME, IteratorMode,
    Options, WriteBatch, WriteOptions, checkpoint::Checkpoint,
};
use zinder_core::{
    CanonicalHistoryBounds, Network, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
};

use crate::{
    BoundedRocksDbOpen, RawBlobRetention, RocksDbIoMode, RocksDbOpenRole, RocksDbResourceBudget,
    build_block_based_table_factory, open_bounded_rocksdb,
};

use super::{
    CANONICAL_STORE_IDENTITY, CANONICAL_STORE_SCHEMA_VERSION, CanonicalStoreBuildState,
    CanonicalStoreError, CanonicalStoreReadyEvidence, CanonicalStoreWorkload,
    activation_schedule::CanonicalActivationSchedule,
    block_replay::{BLOCK_REPLAY_COLUMN_FAMILY, CanonicalReplayScan},
    construction_manifest::{
        copy_construction_manifest, read_construction_manifest_binding,
        validate_ready_construction_manifest,
    },
    control::{
        ActivationScheduleBinding, ControlRevisions, DecodedStoreControl, decode_store_control,
        encode_building_store_control, encode_ready_store_control_with_revisions,
    },
    event_lifecycle::{
        PROJECTION_BUILD_LEASE_GENERATION_KEY, RETENTION_FLOOR_KEY, is_projection_build_lease_key,
    },
    mempool_lifecycle::{
        MEMPOOL_EVENT_RETENTION_FLOOR_KEY, MEMPOOL_EVENT_SEQUENCE_KEY,
        MempoolEventRetentionProgress, validate_mempool_lifecycle_admission,
    },
    publication::validate_ready_publication,
    replay_tail::AuthenticatedReplayTail,
};

/// Cold-admitted identity and READY evidence for an owner-created canonical checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalOwnerCheckpointEvidence {
    /// Exact `RocksDB` database identity captured before physical checkpoint
    /// creation and re-read from the cold checkpoint.
    pub database_identity: Vec<u8>,
    /// Exact physical store identity admitted from the checkpoint.
    pub store_identity: &'static str,
    /// Exact physical schema admitted from the checkpoint.
    pub schema_version: u16,
    /// Immutable workload admitted from the checkpoint.
    pub workload: CanonicalStoreWorkload,
    /// Complete canonical construction identity admitted from the checkpoint.
    pub build_plan: super::CanonicalStoreBuildPlan,
    /// Persisted READY evidence read from the cold-opened checkpoint.
    pub ready_evidence: CanonicalStoreReadyEvidence,
    /// Writer-admitted schedule read from this physical checkpoint, if any.
    pub activation_schedule: Option<CanonicalActivationSchedule>,
    /// Effective replacement depth read from this physical checkpoint: the
    /// construction policy or a writer-admitted raise.
    pub reorg_policy: super::CanonicalReorgPolicy,
}

/// Immutable context captured by the canonical owner when it creates a
/// physical checkpoint.
///
/// This opaque context lets a background worker cold-admit the immutable copy
/// without reopening or retaining the writer's primary handle. It deliberately
/// carries no filesystem authority and is only accepted by
/// [`RocksDbCanonicalStore::cold_admit_owner_checkpoint`].
#[derive(Clone, Debug)]
pub struct CanonicalOwnerCheckpointAdmission {
    workload: CanonicalStoreWorkload,
    build_plan: super::CanonicalStoreBuildPlan,
    database_identity: Vec<u8>,
    activation_schedule: Option<CanonicalActivationSchedule>,
    reorg_policy: super::CanonicalReorgPolicy,
}

pub(super) const STORE_CONTROL_KEY: &[u8] = b"store_control";
pub(super) const BLOCK_HEADER_COLUMN_FAMILY: &str = "block_header";
pub(super) const BLOCK_HASH_INDEX_COLUMN_FAMILY: &str = "block_hash_index";
pub(super) const DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY: &str = "daily_value_pool_balance";
pub(super) const TRANSACTION_LOCATION_COLUMN_FAMILY: &str = "transaction_location";
pub(super) const COMPACT_BLOCK_COLUMN_FAMILY: &str = "compact_block";
pub(super) const TREE_STATE_CHECKPOINT_COLUMN_FAMILY: &str = "tree_state_checkpoint";
pub(super) const BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY: &str =
    "block_final_note_commitment_roots";
pub(super) const SUBTREE_ROOT_COLUMN_FAMILY: &str = "subtree_root";
pub(super) const CHAIN_EPOCH_COLUMN_FAMILY: &str = "chain_epoch";
pub(super) const CHAIN_EVENT_COLUMN_FAMILY: &str = "chain_event";
pub(super) const MEMPOOL_EVENT_COLUMN_FAMILY: &str = "mempool_event";
pub(super) const DISPLACED_BLOCK_FACTS_COLUMN_FAMILY: &str = "displaced_block_facts";
pub(super) const BLOCK_BLOB_COLUMN_FAMILY: &str = "block_blob";
pub(super) const TRANSACTION_BLOB_COLUMN_FAMILY: &str = "transaction_blob";

pub(super) const CANONICAL_DATA_COLUMN_FAMILIES: [&str; 15] = [
    BLOCK_HEADER_COLUMN_FAMILY,
    BLOCK_HASH_INDEX_COLUMN_FAMILY,
    BLOCK_REPLAY_COLUMN_FAMILY,
    DAILY_VALUE_POOL_BALANCE_COLUMN_FAMILY,
    TRANSACTION_LOCATION_COLUMN_FAMILY,
    COMPACT_BLOCK_COLUMN_FAMILY,
    TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
    BLOCK_FINAL_NOTE_COMMITMENT_ROOTS_COLUMN_FAMILY,
    SUBTREE_ROOT_COLUMN_FAMILY,
    CHAIN_EPOCH_COLUMN_FAMILY,
    CHAIN_EVENT_COLUMN_FAMILY,
    MEMPOOL_EVENT_COLUMN_FAMILY,
    DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
    BLOCK_BLOB_COLUMN_FAMILY,
    TRANSACTION_BLOB_COLUMN_FAMILY,
];

#[derive(Clone)]
pub(super) struct CanonicalStoreAdmissionExpectation {
    network: Network,
    activations_fingerprint: NetworkUpgradeActivationsFingerprint,
    expected_activations: Option<NetworkUpgradeActivations>,
    allow_schedule_update: bool,
    workload: CanonicalStoreWorkload,
    raw_blob_retention: RawBlobRetention,
    reorg_policy: ReorgPolicyAdmission,
}

/// How admission compares a caller's window with the persisted effective one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReorgPolicyAdmission {
    /// Readers, readback, and checkpoint admission require the exact
    /// effective window.
    Exact(super::CanonicalReorgPolicy),
    /// The canonical writer admits an effective window at or below its own,
    /// then records any raise. It never lowers one.
    RaiseTo(super::CanonicalReorgPolicy),
}

impl CanonicalStoreAdmissionExpectation {
    pub(super) fn from_activations(
        activations: &NetworkUpgradeActivations,
        workload: CanonicalStoreWorkload,
        raw_blob_retention: RawBlobRetention,
        reorg_policy: super::CanonicalReorgPolicy,
    ) -> Self {
        Self {
            network: activations.network(),
            activations_fingerprint: activations
                .fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1),
            expected_activations: Some(activations.clone()),
            allow_schedule_update: false,
            workload,
            raw_blob_retention,
            reorg_policy: ReorgPolicyAdmission::Exact(reorg_policy),
        }
    }

    /// Expects the persisted construction identity with an exact effective
    /// window, which equals the construction window until a writer raise.
    pub(super) fn from_build_plan(
        build_plan: &super::CanonicalStoreBuildPlan,
        workload: CanonicalStoreWorkload,
        effective_reorg_policy: super::CanonicalReorgPolicy,
    ) -> Self {
        Self {
            network: build_plan.network(),
            activations_fingerprint: build_plan.network_upgrade_activations_fingerprint(),
            expected_activations: None,
            allow_schedule_update: false,
            workload,
            raw_blob_retention: build_plan.raw_blob_retention(),
            reorg_policy: ReorgPolicyAdmission::Exact(effective_reorg_policy),
        }
    }

    pub(super) fn allowing_schedule_update(mut self) -> Self {
        self.allow_schedule_update = true;
        self
    }

    const fn allowing_reorg_policy_raise(mut self) -> Self {
        if let ReorgPolicyAdmission::Exact(reorg_policy) = self.reorg_policy {
            self.reorg_policy = ReorgPolicyAdmission::RaiseTo(reorg_policy);
        }
        self
    }
}

/// One admitted READY canonical `RocksDB` store.
///
/// Construction is owned exclusively by [`super::RocksDbCanonicalBuilder`].
/// This serving type cannot represent or reopen an unpublished BUILDING store.
pub struct RocksDbCanonicalStore {
    pub(super) bounded_open: BoundedRocksDbOpen,
    pub(super) workload: CanonicalStoreWorkload,
    pub(super) build_plan: super::CanonicalStoreBuildPlan,
    pub(super) cursor_auth_key: [u8; 32],
    pub(super) ready_evidence: CanonicalStoreReadyEvidence,
    /// Serializes retained-event, projection-lease, and mempool-log lifecycle changes.
    ///
    /// The primary handle owns every canonical mutation. Keeping this lock on
    /// that handle makes the pruning floor and the durable lease set one
    /// indivisible lifecycle boundary without exposing a second writer API.
    pub(super) lifecycle_lock: Arc<Mutex<()>>,
    /// Resumable process-local state for bounded mempool retention steps.
    pub(super) mempool_retention_progress: Mutex<Option<MempoolEventRetentionProgress>>,
    pub(super) activation_schedule: Option<CanonicalActivationSchedule>,
    /// Writer-admitted window above the construction policy, if any.
    pub(super) revised_reorg_policy: Option<super::CanonicalReorgPolicy>,
    /// Unsettled replay rows this handle has authenticated, extended by each
    /// committed transition.
    pub(super) replay_tail: AuthenticatedReplayTail,
}

impl RocksDbCanonicalStore {
    #[allow(
        clippy::too_many_arguments,
        reason = "publication hands over every authenticated part of the READY store it just verified"
    )]
    pub(super) fn from_published(
        bounded_open: BoundedRocksDbOpen,
        workload: CanonicalStoreWorkload,
        build_plan: super::CanonicalStoreBuildPlan,
        cursor_auth_key: [u8; 32],
        ready_evidence: &CanonicalStoreReadyEvidence,
        replay_tail: AuthenticatedReplayTail,
    ) -> Self {
        Self {
            bounded_open,
            workload,
            build_plan,
            cursor_auth_key,
            ready_evidence: *ready_evidence,
            lifecycle_lock: Arc::new(Mutex::new(())),
            mempool_retention_progress: Mutex::new(None),
            activation_schedule: None,
            revised_reorg_policy: None,
            replay_tail,
        }
    }

    /// Opens an existing READY canonical store after exact admission.
    ///
    /// Admission validates the complete column-family set, singleton control
    /// key, identity, schema, exact baseline or admitted network-upgrade table,
    /// workload, raw-blob retention, canonical reorg policy, source range,
    /// construction manifest identity, and readiness evidence before opening a
    /// writer that cannot create data families.
    #[allow(
        clippy::too_many_arguments,
        reason = "primary admission keeps the filesystem identity and every immutable canonical contract explicit"
    )]
    pub fn open_ready(
        path: impl AsRef<Path>,
        expected_network_upgrade_activations: &NetworkUpgradeActivations,
        expected_workload: CanonicalStoreWorkload,
        expected_raw_blob_retention: RawBlobRetention,
        expected_reorg_policy: super::CanonicalReorgPolicy,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        let path = path.as_ref();
        let expectation = CanonicalStoreAdmissionExpectation::from_activations(
            expected_network_upgrade_activations,
            expected_workload,
            expected_raw_blob_retention,
            expected_reorg_policy,
        );
        Self::open_ready_with_expectation(path, &expectation, resource_budget)
    }

    /// Opens the primary as the canonical writer, admits the node's current
    /// schedule as a writer-owned future-only revision, and raises the
    /// effective reorg window to `reorg_policy` when it is larger.
    ///
    /// A raise is a monotonic store revision: the settled tip does not move,
    /// so the replaceable suffix grows to the new window as blocks arrive.
    /// A smaller window than the persisted effective one is refused.
    #[allow(
        clippy::too_many_arguments,
        reason = "writer admission preserves the existing explicit immutable-store options"
    )]
    pub fn open_ready_with_network_upgrade_schedule(
        path: impl AsRef<Path>,
        observed_activations: &NetworkUpgradeActivations,
        expected_workload: CanonicalStoreWorkload,
        expected_raw_blob_retention: RawBlobRetention,
        reorg_policy: super::CanonicalReorgPolicy,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        let expectation = CanonicalStoreAdmissionExpectation::from_activations(
            observed_activations,
            expected_workload,
            expected_raw_blob_retention,
            reorg_policy,
        )
        .allowing_schedule_update()
        .allowing_reorg_policy_raise();
        let mut store =
            Self::open_ready_with_expectation(path.as_ref(), &expectation, resource_budget)?;
        store.admit_network_upgrade_schedule(observed_activations)?;
        store.raise_reorg_policy(reorg_policy)?;
        Ok(store)
    }

    /// Durably records `reorg_policy` as the effective window when it raises
    /// the current one. Equal windows are a no-op and smaller ones are refused.
    fn raise_reorg_policy(
        &mut self,
        reorg_policy: super::CanonicalReorgPolicy,
    ) -> Result<(), CanonicalStoreError> {
        let current = self.reorg_policy();
        if reorg_policy == current {
            return Ok(());
        }
        if reorg_policy < current {
            return Err(reorg_policy_lowering_refused(
                self.bounded_open.db.path(),
                current,
                reorg_policy,
            ));
        }
        let revisions = ControlRevisions {
            revised_reorg_policy: Some(reorg_policy),
            ..self.control_revisions()
        };
        let encoded_control = encode_ready_store_control_with_revisions(
            self.workload,
            &self.build_plan,
            self.cursor_auth_key,
            &self.ready_evidence,
            revisions,
        )
        .map_err(|source| {
            CanonicalStoreError::admission(self.bounded_open.db.path(), source.to_string())
        })?;
        let mut options = WriteOptions::default();
        options.disable_wal(false);
        options.set_sync(true);
        self.bounded_open
            .db
            .put_opt(STORE_CONTROL_KEY, encoded_control, &options)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "reorg policy revision",
                source,
            })?;
        let decoded = validate_open_store_control(
            &self.bounded_open.db,
            self.bounded_open.db.path(),
            &CanonicalStoreAdmissionExpectation::from_build_plan(
                &self.build_plan,
                self.workload,
                reorg_policy,
            ),
        )?;
        if decoded.revisions() != revisions {
            return Err(CanonicalStoreError::admission(
                self.bounded_open.db.path(),
                "reorg policy revision readback differs from the admitted revision",
            ));
        }
        tracing::info!(
            target: "zinder::store",
            event = "canonical_reorg_policy_raised",
            previous_reorg_window_blocks = current.reorg_window_blocks(),
            reorg_window_blocks = reorg_policy.reorg_window_blocks(),
            settled_tip_height = self.ready_evidence.sequence_checkpoint.through().height.value(),
            "raised the canonical reorg window; the settled tip holds until the visible tip is that deep"
        );
        self.revised_reorg_policy = Some(reorg_policy);
        Ok(())
    }

    /// Returns every writer-admitted revision bound to store control.
    pub(super) fn control_revisions(&self) -> ControlRevisions {
        ControlRevisions {
            activation_schedule_binding: self.activation_schedule_binding(),
            revised_reorg_policy: self.revised_reorg_policy,
        }
    }

    /// Returns the schedule revision admitted by the canonical writer, or
    /// `None` for a store that no writer of this release has opened yet.
    #[must_use]
    pub const fn network_upgrade_schedule(&self) -> Option<&CanonicalActivationSchedule> {
        self.activation_schedule.as_ref()
    }

    /// Returns the complete activation table admitted by the canonical writer.
    #[must_use]
    pub const fn network_upgrade_activations(&self) -> Option<&NetworkUpgradeActivations> {
        match self.activation_schedule.as_ref() {
            Some(schedule) => Some(schedule.activations()),
            None => None,
        }
    }

    /// Admits a future-only activation-table revision and durably binds it to
    /// store control in one synced write batch.
    pub fn admit_network_upgrade_schedule(
        &mut self,
        observed_activations: &NetworkUpgradeActivations,
    ) -> Result<(), CanonicalStoreError> {
        if observed_activations.network() != self.network() {
            return Err(CanonicalStoreError::admission(
                self.bounded_open.db.path(),
                "activation schedule network differs from the canonical store network",
            ));
        }
        if let Some(schedule) = self.next_activation_schedule(observed_activations)? {
            self.persist_activation_schedule(&schedule)?;
        }
        Ok(())
    }

    fn next_activation_schedule(
        &self,
        observed_activations: &NetworkUpgradeActivations,
    ) -> Result<Option<CanonicalActivationSchedule>, CanonicalStoreError> {
        let visible_tip = self.ready_evidence.visible_tip;
        let schedule = if let Some(previous) = &self.activation_schedule {
            if !observed_activations.preserves_through(previous.activations(), visible_tip.height) {
                return Err(CanonicalStoreError::admission(
                    self.bounded_open.db.path(),
                    format!(
                        "node activation table differs from admitted schedule revision {} at or below canonical tip {}; refusing admission",
                        previous.revision(),
                        visible_tip.height.value(),
                    ),
                ));
            }
            if observed_activations == previous.activations() {
                return Ok(None);
            }
            CanonicalActivationSchedule::successor(
                previous,
                visible_tip,
                observed_activations.clone(),
            )
            .map_err(|reason| CanonicalStoreError::admission(self.bounded_open.db.path(), reason))?
        } else {
            let baseline = self.build_plan.network_upgrade_activations_fingerprint();
            let prefix_len = observed_activations
                .matching_fingerprint_prefix_len(baseline)
                .ok_or_else(|| {
                    CanonicalStoreError::admission(
                        self.bounded_open.db.path(),
                        "node activation table does not extend the table this store was built with as an exact prefix; restore the original activations on the node and restart the writer",
                    )
                })?;
            let prior = NetworkUpgradeActivations::new(
                self.network(),
                observed_activations.activations()[..prefix_len].to_vec(),
            )
            .map_err(|error| {
                CanonicalStoreError::admission(self.bounded_open.db.path(), error.to_string())
            })?;
            if !observed_activations.preserves_through(&prior, visible_tip.height) {
                return Err(CanonicalStoreError::admission(
                    self.bounded_open.db.path(),
                    format!(
                        "node activation table adds an activation at or below canonical tip {}; refusing admission",
                        visible_tip.height.value(),
                    ),
                ));
            }
            CanonicalActivationSchedule::initial(
                observed_activations.clone(),
                baseline.as_bytes(),
                u32::try_from(prefix_len).map_err(|_| {
                    CanonicalStoreError::admission(
                        self.bounded_open.db.path(),
                        "construction activation prefix length exceeds u32",
                    )
                })?,
                visible_tip,
            )
            .map_err(|reason| CanonicalStoreError::admission(self.bounded_open.db.path(), reason))?
        };
        Ok(Some(schedule))
    }

    fn persist_activation_schedule(
        &mut self,
        schedule: &CanonicalActivationSchedule,
    ) -> Result<(), CanonicalStoreError> {
        let binding = schedule.binding();
        let encoded_control = encode_ready_store_control_with_revisions(
            self.workload,
            &self.build_plan,
            self.cursor_auth_key,
            &self.ready_evidence,
            ControlRevisions {
                activation_schedule_binding: Some(binding),
                ..self.control_revisions()
            },
        )
        .map_err(|source| {
            CanonicalStoreError::admission(self.bounded_open.db.path(), source.to_string())
        })?;
        let mut batch = WriteBatch::default();
        schedule.put_into(&mut batch).map_err(|reason| {
            CanonicalStoreError::admission(self.bounded_open.db.path(), reason)
        })?;
        batch.put(STORE_CONTROL_KEY, encoded_control);
        let mut options = WriteOptions::default();
        options.disable_wal(false);
        options.set_sync(true);
        abort_at_activation_schedule_failpoint("before_atomic_write");
        self.bounded_open
            .db
            .write_opt(&batch, &options)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "activation schedule admission",
                source,
            })?;
        abort_at_activation_schedule_failpoint("after_atomic_write");
        let decoded = validate_open_store_control(
            &self.bounded_open.db,
            self.bounded_open.db.path(),
            &CanonicalStoreAdmissionExpectation::from_build_plan(
                &self.build_plan,
                self.workload,
                self.reorg_policy(),
            ),
        )?;
        let stored_schedule = decoded
            .activation_schedule_binding
            .map(|stored_binding| {
                CanonicalActivationSchedule::read_bound(
                    &self.bounded_open.db,
                    self.bounded_open.db.path(),
                    stored_binding,
                    self.build_plan
                        .network_upgrade_activations_fingerprint()
                        .as_bytes(),
                )
            })
            .transpose()?
            .ok_or_else(|| {
                CanonicalStoreError::admission(
                    self.bounded_open.db.path(),
                    "activation schedule control trailer was not persisted",
                )
            })?;
        if stored_schedule != *schedule {
            return Err(CanonicalStoreError::admission(
                self.bounded_open.db.path(),
                "activation schedule readback differs from the admitted revision",
            ));
        }
        self.activation_schedule = Some(stored_schedule);
        Ok(())
    }

    fn open_ready_with_expectation(
        path: &Path,
        expectation: &CanonicalStoreAdmissionExpectation,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        validate_resource_budget(resource_budget)?;
        let store_path = canonical_store_path(path)?;
        let (admitted_database_identity, admitted_control) =
            admit_existing_store(&store_path, expectation)?;
        let CanonicalStoreBuildState::Ready(admitted_ready_evidence) = admitted_control.build_state
        else {
            return Err(CanonicalStoreError::StoreNotReady { path: store_path });
        };
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path: &store_path },
            resource_budget,
            canonical_column_family_descriptors,
        )
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "open ready",
            source,
        })?;
        let opened_database_identity = bounded_open.db.get_db_identity().map_err(|source| {
            CanonicalStoreError::RocksDbOperation {
                operation: "database identity read",
                source,
            }
        })?;
        if opened_database_identity != admitted_database_identity {
            return Err(CanonicalStoreError::admission(
                &store_path,
                "database identity changed during admission",
            ));
        }
        let opened_control =
            validate_open_store_control(&bounded_open.db, &store_path, expectation)?;
        validate_mempool_lifecycle_admission(
            &bounded_open.db,
            opened_control.network,
            opened_control.cursor_auth_key,
        )?;
        if opened_control != admitted_control {
            return Err(CanonicalStoreError::admission(
                &store_path,
                "store control changed during admission",
            ));
        }
        let CanonicalStoreBuildState::Ready(opened_ready_evidence) = opened_control.build_state
        else {
            return Err(CanonicalStoreError::StoreNotReady { path: store_path });
        };
        if opened_ready_evidence != admitted_ready_evidence {
            return Err(CanonicalStoreError::admission(
                &store_path,
                "ready evidence changed during admission",
            ));
        }
        let activation_schedule =
            read_admitted_schedule(&bounded_open.db, &store_path, &opened_control)?;
        let reorg_policy = opened_control.effective_reorg_policy();
        let build_plan = opened_control.build_plan;
        let replay_tail = validate_ready_publication(
            &bounded_open.db,
            &build_plan,
            reorg_policy,
            &opened_ready_evidence,
        )?;
        Ok(Self {
            bounded_open,
            workload: expectation.workload,
            build_plan,
            cursor_auth_key: opened_control.cursor_auth_key,
            ready_evidence: opened_ready_evidence,
            lifecycle_lock: Arc::new(Mutex::new(())),
            mempool_retention_progress: Mutex::new(None),
            activation_schedule,
            revised_reorg_policy: opened_control.revised_reorg_policy,
            replay_tail,
        })
    }

    /// Creates and cold-admits one physical checkpoint from this canonical owner.
    ///
    /// `target` must not exist. The returned identity and READY evidence are
    /// read through exact cold admission of the completed checkpoint, never
    /// copied from this live handle. Requiring mutable access keeps this API on
    /// the canonical owner's mutation surface; serving secondaries expose no
    /// checkpoint operation.
    pub fn create_owner_checkpoint(
        &mut self,
        target: impl AsRef<Path>,
        admission_resource_budget: RocksDbResourceBudget,
    ) -> Result<CanonicalOwnerCheckpointEvidence, CanonicalStoreError> {
        let target = target.as_ref();
        let admission = self.create_owner_checkpoint_physical(target)?;
        Self::cold_admit_owner_checkpoint(target, &admission, admission_resource_budget)
    }

    /// Creates the physical checkpoint while the canonical owner holds the
    /// primary handle, then returns immutable admission context.
    ///
    /// Callers must run [`Self::cold_admit_owner_checkpoint`] outside the
    /// writer's serialization queue. A full canonical cold admission may scan
    /// a mainnet-sized immutable copy for a long time; holding primary
    /// ownership through that scan would stall live following.
    pub fn create_owner_checkpoint_physical(
        &mut self,
        target: impl AsRef<Path>,
    ) -> Result<CanonicalOwnerCheckpointAdmission, CanonicalStoreError> {
        let target = target.as_ref();
        require_absent_checkpoint_target(target)?;
        let admission = self.owner_checkpoint_readmission(target)?;
        let checkpoint = Checkpoint::new(&self.bounded_open.db).map_err(|source| {
            CanonicalStoreError::CheckpointFailed {
                path: target.to_path_buf(),
                source,
            }
        })?;
        checkpoint.create_checkpoint(target).map_err(|source| {
            CanonicalStoreError::CheckpointFailed {
                path: target.to_path_buf(),
                source,
            }
        })?;
        copy_construction_manifest(self.bounded_open.db.path(), target)?;
        Ok(admission)
    }

    /// Captures an opaque primary-owner admission context for re-admitting an
    /// existing physical checkpoint.
    ///
    /// The context contains no filesystem authority and can only be consumed
    /// by [`Self::cold_admit_owner_checkpoint`], which still performs complete
    /// cold admission. The canonical control owner obtains this context on its
    /// serialized primary queue immediately before it re-admits a checkpoint;
    /// secondaries never expose this operation.
    pub fn owner_checkpoint_readmission(
        &self,
        target: impl AsRef<Path>,
    ) -> Result<CanonicalOwnerCheckpointAdmission, CanonicalStoreError> {
        let target = target.as_ref();
        let database_identity = self.bounded_open.db.get_db_identity().map_err(|source| {
            CanonicalStoreError::CheckpointFailed {
                path: target.to_path_buf(),
                source,
            }
        })?;
        Ok(CanonicalOwnerCheckpointAdmission {
            workload: self.workload,
            build_plan: self.build_plan.clone(),
            database_identity,
            activation_schedule: self.activation_schedule.clone(),
            reorg_policy: self.reorg_policy(),
        })
    }

    /// Cold-admits an immutable owner-created checkpoint without opening or
    /// retaining its source primary.
    ///
    /// This is intentionally a static operation so the expensive complete
    /// readback runs independently of the canonical writer queue.
    pub fn cold_admit_owner_checkpoint(
        target: impl AsRef<Path>,
        admission: &CanonicalOwnerCheckpointAdmission,
        admission_resource_budget: RocksDbResourceBudget,
    ) -> Result<CanonicalOwnerCheckpointEvidence, CanonicalStoreError> {
        validate_resource_budget(admission_resource_budget)?;
        let target = target.as_ref();
        let expectation = CanonicalStoreAdmissionExpectation::from_build_plan(
            &admission.build_plan,
            admission.workload,
            admission.reorg_policy,
        );
        let cold_checkpoint =
            Self::open_ready_with_expectation(target, &expectation, admission_resource_budget)?;
        let cold_database_identity =
            cold_checkpoint
                .bounded_open
                .db
                .get_db_identity()
                .map_err(|source| CanonicalStoreError::CheckpointFailed {
                    path: target.to_path_buf(),
                    source,
                })?;
        if cold_database_identity != admission.database_identity {
            return Err(CanonicalStoreError::admission(
                target,
                "checkpoint database identity differs from the physical owner checkpoint",
            ));
        }
        if cold_checkpoint.activation_schedule != admission.activation_schedule {
            return Err(CanonicalStoreError::admission(
                target,
                "checkpoint activation schedule differs from the owner-captured schedule",
            ));
        }
        Ok(CanonicalOwnerCheckpointEvidence {
            database_identity: cold_database_identity,
            store_identity: CANONICAL_STORE_IDENTITY,
            schema_version: CANONICAL_STORE_SCHEMA_VERSION,
            workload: cold_checkpoint.workload,
            build_plan: cold_checkpoint.build_plan.clone(),
            ready_evidence: cold_checkpoint.ready_evidence,
            reorg_policy: cold_checkpoint.reorg_policy(),
            activation_schedule: cold_checkpoint.activation_schedule,
        })
    }

    /// Reads the immutable construction-manifest identity without opening a
    /// `RocksDB` primary. Archive packagers use this narrow descriptor to bind
    /// a physical checkpoint to the first READY construction proof.
    pub fn read_construction_manifest_binding(
        path: impl AsRef<Path>,
    ) -> Result<super::CanonicalConstructionManifestBinding, CanonicalStoreError> {
        read_construction_manifest_binding(path.as_ref())
    }

    /// Returns the immutable network persisted by the store control record.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.build_plan.network()
    }

    /// Returns the admitted activation-table identity persisted by the store.
    #[must_use]
    pub const fn network_upgrade_activations_fingerprint(
        &self,
    ) -> NetworkUpgradeActivationsFingerprint {
        self.build_plan.network_upgrade_activations_fingerprint()
    }

    pub(super) fn activation_table_is_admitted(
        &self,
        activations: &NetworkUpgradeActivations,
    ) -> bool {
        if activations.network() != self.network() {
            return false;
        }
        self.activation_schedule.as_ref().map_or_else(
            || {
                activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
                    == self.network_upgrade_activations_fingerprint()
            },
            |schedule| activations == schedule.activations(),
        )
    }

    pub(super) fn activation_schedule_binding(&self) -> Option<ActivationScheduleBinding> {
        self.activation_schedule
            .as_ref()
            .map(CanonicalActivationSchedule::binding)
    }

    /// Returns the immutable canonical workload persisted by the store.
    #[must_use]
    pub const fn workload(&self) -> CanonicalStoreWorkload {
        self.workload
    }

    /// Returns the immutable raw-blob retention persisted by the store.
    #[must_use]
    pub const fn raw_blob_retention(&self) -> RawBlobRetention {
        self.build_plan.raw_blob_retention()
    }

    /// Returns the effective maximum canonical replacement depth: the
    /// construction policy, or the larger window a canonical writer admitted.
    #[must_use]
    pub fn reorg_policy(&self) -> super::CanonicalReorgPolicy {
        self.revised_reorg_policy
            .unwrap_or_else(|| self.build_plan.reorg_policy())
    }

    /// Returns the durable boundary of intentionally retained history.
    #[must_use]
    pub const fn history_bounds(&self) -> CanonicalHistoryBounds {
        self.build_plan.history_bounds()
    }

    /// Returns the complete admitted canonical construction identity.
    #[must_use]
    pub const fn build_plan(&self) -> &super::CanonicalStoreBuildPlan {
        &self.build_plan
    }

    /// Returns the evidence that admitted this store as READY.
    #[must_use]
    pub const fn ready_evidence(&self) -> CanonicalStoreReadyEvidence {
        self.ready_evidence
    }

    /// Returns the authenticated replay prefix through the settled tip.
    #[must_use]
    pub const fn sequence_checkpoint(&self) -> super::CanonicalSequenceCheckpoint {
        self.ready_evidence.sequence_checkpoint
    }

    /// Scans the complete published canonical replay exactly once in height order.
    ///
    /// The scan bypasses the `RocksDB` block cache, decodes and authenticates every
    /// replay row, and verifies the final count, tip, and ordered sequence digest
    /// against the READY record before it terminates successfully.
    pub fn scan_canonical_replay(&self) -> Result<CanonicalReplayScan<'_>, CanonicalStoreError> {
        CanonicalReplayScan::new(&self.bounded_open.db, &self.ready_evidence)
    }

    /// Returns the filesystem I/O mode selected by the bounded `RocksDB` open.
    #[must_use]
    pub const fn io_mode(&self) -> RocksDbIoMode {
        self.bounded_open.io_mode
    }
}

#[cfg(test)]
fn abort_at_activation_schedule_failpoint(expected: &str) {
    if std::env::var("ZINDER_TEST_CANONICAL_ACTIVATION_SCHEDULE_FAILPOINT").as_deref()
        == Ok(expected)
    {
        std::process::abort();
    }
}

#[cfg(not(test))]
const fn abort_at_activation_schedule_failpoint(_expected: &str) {}

fn require_absent_checkpoint_target(path: &Path) -> Result<(), CanonicalStoreError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(CanonicalStoreError::CheckpointTargetExists {
            path: path.to_path_buf(),
        }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(CanonicalStoreError::PathUnavailable {
            path: path.to_path_buf(),
            source,
        }),
    }
}

pub(super) fn canonical_column_family_descriptors(
    block_cache: &Cache,
    resource_budget: RocksDbResourceBudget,
) -> Vec<ColumnFamilyDescriptor> {
    CANONICAL_DATA_COLUMN_FAMILIES
        .into_iter()
        .map(|name| {
            ColumnFamilyDescriptor::new(name, canonical_data_options(block_cache, resource_budget))
        })
        .collect()
}

pub(super) fn canonical_data_options(
    block_cache: &Cache,
    resource_budget: RocksDbResourceBudget,
) -> Options {
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Snappy);
    options.set_block_based_table_factory(&build_block_based_table_factory(block_cache));
    options.set_write_buffer_size(
        usize::try_from(resource_budget.write_buffer_bytes).unwrap_or(usize::MAX),
    );
    options.set_max_write_buffer_number(resource_budget.max_write_buffer_count);
    options
}

pub(super) fn canonical_store_path(path: &Path) -> Result<std::path::PathBuf, CanonicalStoreError> {
    fs::canonicalize(path).map_err(|source| CanonicalStoreError::PathUnavailable {
        path: path.to_path_buf(),
        source,
    })
}

pub(super) fn validate_resource_budget(
    resource_budget: RocksDbResourceBudget,
) -> Result<(), CanonicalStoreError> {
    resource_budget
        .validate()
        .map_err(|reason| CanonicalStoreError::InvalidResourceBudget { reason })
}

pub(super) fn create_fresh_directory(path: &Path) -> Result<(), CanonicalStoreError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| CanonicalStoreError::PathUnavailable {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(CanonicalStoreError::PathNotFresh {
                path: path.to_path_buf(),
            })
        }
        Err(source) => Err(CanonicalStoreError::PathUnavailable {
            path: path.to_path_buf(),
            source,
        }),
    }
}

pub(super) fn initialize_store_identity(
    path: &Path,
    workload: CanonicalStoreWorkload,
    build_plan: &super::CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
) -> Result<(), CanonicalStoreError> {
    let mut database_options = Options::default();
    database_options.create_if_missing(true);
    let db = DB::open(&database_options, path).map_err(|source| {
        CanonicalStoreError::RocksDbOperation {
            operation: "identity open",
            source,
        }
    })?;
    let mut batch = WriteBatch::default();
    let encoded_control = encode_building_store_control(workload, build_plan, cursor_auth_key)
        .map_err(|source| CanonicalStoreError::admission(path, source.to_string()))?;
    batch.put(STORE_CONTROL_KEY, encoded_control);
    let mut options = WriteOptions::default();
    options.disable_wal(false);
    options.set_sync(true);
    db.write_opt(&batch, &options)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "identity initialization",
            source,
        })
}

pub(super) fn admit_existing_store(
    path: &Path,
    expectation: &CanonicalStoreAdmissionExpectation,
) -> Result<(Vec<u8>, DecodedStoreControl), CanonicalStoreError> {
    let column_families = DB::list_cf(&Options::default(), path).map_err(|source| {
        CanonicalStoreError::admission(path, format!("column-family discovery failed: {source}"))
    })?;
    validate_exact_column_families(path, &column_families)?;
    let db = DB::open_cf_for_read_only(&Options::default(), path, &column_families, false)
        .map_err(|source| {
            CanonicalStoreError::admission(path, format!("read-only open failed: {source}"))
        })?;
    let control = validate_open_store_control(&db, path, expectation)?;
    if let CanonicalStoreBuildState::Ready(ready) = control.build_state {
        validate_ready_construction_manifest(path, &ready, control.workload, &control.build_plan)?;
    }
    validate_mempool_lifecycle_admission(&db, control.network, control.cursor_auth_key)?;
    let database_identity = db.get_db_identity().map_err(|source| {
        CanonicalStoreError::admission(path, format!("database identity read failed: {source}"))
    })?;
    Ok((database_identity, control))
}

fn validate_exact_column_families(
    path: &Path,
    observed: &[String],
) -> Result<(), CanonicalStoreError> {
    let mut observed = observed.to_vec();
    observed.sort_unstable();
    let mut expected = CANONICAL_DATA_COLUMN_FAMILIES
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    expected.push(DEFAULT_COLUMN_FAMILY_NAME.to_owned());
    expected.sort_unstable();
    if observed != expected {
        return Err(CanonicalStoreError::admission(
            path,
            format!(
                "column families {observed:?} do not exactly match required canonical set {expected:?}"
            ),
        ));
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "one default-family admission pass binds control and every schedule-journal key"
)]
pub(super) fn validate_open_store_control(
    db: &DB,
    path: &Path,
    expectation: &CanonicalStoreAdmissionExpectation,
) -> Result<DecodedStoreControl, CanonicalStoreError> {
    let mut control = None;
    let mut schedule_keys = Vec::new();
    for row in db.iterator(IteratorMode::Start) {
        let (key, encoded_control) = row.map_err(|source| {
            CanonicalStoreError::admission(
                path,
                format!("store-control iteration failed: {source}"),
            )
        })?;
        match key.as_ref() {
            STORE_CONTROL_KEY => control = Some(encoded_control),
            RETENTION_FLOOR_KEY
            | PROJECTION_BUILD_LEASE_GENERATION_KEY
            | MEMPOOL_EVENT_SEQUENCE_KEY
            | MEMPOOL_EVENT_RETENTION_FLOOR_KEY => {}
            key if key.starts_with(super::activation_schedule::ACTIVATION_SCHEDULE_KEY_PREFIX) => {
                schedule_keys.push(key.to_vec());
            }
            lease_key if is_projection_build_lease_key(lease_key) => {}
            unknown => {
                return Err(CanonicalStoreError::admission(
                    path,
                    format!("unexpected default-column-family key {unknown:?}"),
                ));
            }
        }
    }
    let control =
        control.ok_or_else(|| CanonicalStoreError::admission(path, "store identity is absent"))?;
    let persisted = decode_store_control(path, &control)?;
    if persisted.network != expectation.network {
        return Err(CanonicalStoreError::admission(
            path,
            format!(
                "persisted network {:?} does not equal requested network {:?}",
                persisted.network, expectation.network
            ),
        ));
    }
    let persisted_activations_fingerprint = persisted
        .build_plan
        .network_upgrade_activations_fingerprint();
    let admitted_schedule = match persisted.activation_schedule_binding {
        Some(binding) => Some(CanonicalActivationSchedule::read_bound(
            db,
            path,
            binding,
            persisted_activations_fingerprint.as_bytes(),
        )?),
        None => None,
    };
    let visible_tip = match persisted.build_state {
        CanonicalStoreBuildState::Ready(ready) => Some(ready.visible_tip.height),
        CanonicalStoreBuildState::Building => None,
    };
    validate_activation_expectation(
        expectation,
        persisted_activations_fingerprint,
        admitted_schedule.as_ref(),
        visible_tip,
    )
    .map_err(|reason| CanonicalStoreError::admission(path, reason))?;
    let expected_schedule_key_count = match persisted.activation_schedule_binding {
        Some(binding) => usize::try_from(binding.revision).map_err(|_| {
            CanonicalStoreError::admission(path, "activation schedule revision exceeds memory size")
        })?,
        None => 0,
    };
    if schedule_keys.len() != expected_schedule_key_count {
        return Err(CanonicalStoreError::admission(
            path,
            "activation schedule journal keys do not exactly match store control",
        ));
    }
    if persisted.workload != expectation.workload {
        return Err(CanonicalStoreError::admission(
            path,
            format!(
                "persisted workload {} does not equal requested workload {}",
                persisted.workload.as_str(),
                expectation.workload.as_str()
            ),
        ));
    }
    if persisted.build_plan.raw_blob_retention() != expectation.raw_blob_retention {
        return Err(CanonicalStoreError::admission(
            path,
            format!(
                "persisted raw blob retention {} does not equal requested raw blob retention {}; rebuild the canonical store to change retention",
                persisted.build_plan.raw_blob_retention(),
                expectation.raw_blob_retention
            ),
        ));
    }
    let persisted_reorg_policy = persisted.effective_reorg_policy();
    match expectation.reorg_policy {
        ReorgPolicyAdmission::Exact(requested) if requested != persisted_reorg_policy => {
            return Err(CanonicalStoreError::admission(
                path,
                format!(
                    "persisted reorg window {} does not equal requested reorg window {}; configure the store's window, or restart the canonical writer with the larger window so it raises the store",
                    persisted_reorg_policy.reorg_window_blocks(),
                    requested.reorg_window_blocks()
                ),
            ));
        }
        ReorgPolicyAdmission::RaiseTo(requested) if requested < persisted_reorg_policy => {
            return Err(reorg_policy_lowering_refused(
                path,
                persisted_reorg_policy,
                requested,
            ));
        }
        ReorgPolicyAdmission::Exact(_) | ReorgPolicyAdmission::RaiseTo(_) => {}
    }
    Ok(persisted)
}

fn read_admitted_schedule(
    db: &DB,
    path: &Path,
    control: &DecodedStoreControl,
) -> Result<Option<CanonicalActivationSchedule>, CanonicalStoreError> {
    control
        .activation_schedule_binding
        .map(|binding| {
            CanonicalActivationSchedule::read_bound(
                db,
                path,
                binding,
                control
                    .build_plan
                    .network_upgrade_activations_fingerprint()
                    .as_bytes(),
            )
        })
        .transpose()
}

fn reorg_policy_lowering_refused(
    path: &Path,
    persisted: super::CanonicalReorgPolicy,
    requested: super::CanonicalReorgPolicy,
) -> CanonicalStoreError {
    CanonicalStoreError::admission(
        path,
        format!(
            "requested reorg window {} is below the store's effective window {}; a window is never lowered in place, so rebuild the canonical store to lower it",
            requested.reorg_window_blocks(),
            persisted.reorg_window_blocks()
        ),
    )
}

/// Checks the caller's activation expectation against persisted identity.
///
/// Construction and exact re-admission compare the immutable construction
/// fingerprint. A writer opening for schedule admission defers to
/// [`RocksDbCanonicalStore::admit_network_upgrade_schedule`]. A reader with a
/// node-observed table is admitted only when the writer has already admitted
/// a schedule that agrees with that table through the visible tip; before the
/// first admission, the reader's table must equal the construction baseline.
fn validate_activation_expectation(
    expectation: &CanonicalStoreAdmissionExpectation,
    persisted_fingerprint: NetworkUpgradeActivationsFingerprint,
    admitted_schedule: Option<&CanonicalActivationSchedule>,
    visible_tip: Option<zinder_core::BlockHeight>,
) -> Result<(), &'static str> {
    let exact_baseline = || {
        if persisted_fingerprint == expectation.activations_fingerprint {
            Ok(())
        } else {
            Err(
                "persisted network upgrade activations do not equal the requested activation table; restart the canonical writer so it admits the node's schedule",
            )
        }
    };
    let Some(observed) = expectation.expected_activations.as_ref() else {
        return exact_baseline();
    };
    match (admitted_schedule, visible_tip) {
        (_, Some(_)) if expectation.allow_schedule_update => Ok(()),
        (Some(admitted), Some(visible_tip)) => {
            if observed.preserves_through(admitted.activations(), visible_tip) {
                Ok(())
            } else {
                Err(
                    "node network upgrade activations differ from the writer-admitted schedule at or below the visible tip",
                )
            }
        }
        (None, _) => exact_baseline(),
        (Some(_), None) => Err("BUILDING store control must not bind an activation schedule"),
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use zinder_core::{BlockHash, BlockHeight, BlockId};

    use super::*;
    use crate::{
        CANONICAL_STORE_IDENTITY, CANONICAL_STORE_SCHEMA_VERSION, CanonicalReorgPolicy,
        CanonicalStoreBuildPlan, RocksDbCanonicalBuilder,
    };

    #[test]
    fn building_layout_is_exact_and_not_servable() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("canonical");
        let store = RocksDbCanonicalBuilder::create_fresh(
            &path,
            CanonicalStoreWorkload::Explorer,
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(store.network(), Network::ZcashTestnet);
        let testnet_activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        assert_eq!(
            store.network_upgrade_activations_fingerprint(),
            testnet_activations.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
        );
        assert_eq!(store.workload(), CanonicalStoreWorkload::Explorer);
        assert_eq!(
            store.build_plan().history_bounds(),
            CanonicalHistoryBounds::complete()
        );
        drop(store);

        let mut observed = DB::list_cf(&Options::default(), &path)?;
        observed.sort_unstable();
        let mut expected = CANONICAL_DATA_COLUMN_FAMILIES
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        expected.push(DEFAULT_COLUMN_FAMILY_NAME.to_owned());
        expected.sort_unstable();
        assert_eq!(observed, expected);

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &testnet_activations,
            CanonicalStoreWorkload::Explorer,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("a BUILDING store must not be servable")?;
        assert!(matches!(error, CanonicalStoreError::StoreNotReady { .. }));

        let control_before = read_control(&path)?;
        let mainnet_activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashMainnet)?;
        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &mainnet_activations,
            CanonicalStoreWorkload::Explorer,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("network mismatch should be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreError::AdmissionRefused { .. }
        ));
        assert_eq!(read_control(&path)?, control_before);

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &testnet_activations,
            CanonicalStoreWorkload::Wallet,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("workload mismatch should be rejected")?;
        assert!(error.to_string().contains("persisted workload explorer"));
        assert_eq!(read_control(&path)?, control_before);
        Ok(())
    }

    #[test]
    fn ready_open_rejects_activation_table_mismatch() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("activation-mismatch");
        let store = RocksDbCanonicalBuilder::create_fresh(
            &path,
            CanonicalStoreWorkload::Explorer,
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        drop(store);

        let testnet_activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let mut shifted_activation_rows = testnet_activations.activations().to_vec();
        let latest_activation = shifted_activation_rows
            .last_mut()
            .ok_or("canonical activation fixture must not be empty")?;
        latest_activation.activation_height = BlockHeight::new(
            latest_activation
                .activation_height
                .value()
                .saturating_add(1),
        );
        let shifted_activations = zinder_core::NetworkUpgradeActivations::new(
            Network::ZcashTestnet,
            shifted_activation_rows,
        )?;

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &shifted_activations,
            CanonicalStoreWorkload::Explorer,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("activation mismatch should be rejected")?;
        assert!(error.to_string().contains("network upgrade activations"));
        Ok(())
    }

    #[test]
    fn unadmitted_schema_marker_is_refused_without_adoption()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("unadmitted-schema-marker");
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let descriptors = CANONICAL_DATA_COLUMN_FAMILIES
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()));
        let db = DB::open_cf_descriptors(&options, &path, descriptors)?;
        db.put(b"schema_version", 1_u16.to_le_bytes())?;
        drop(db);

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Explorer,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("unadmitted schema marker should be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreError::AdmissionRefused { .. }
        ));

        let column_families = DB::list_cf(&Options::default(), &path)?;
        let db = DB::open_cf_for_read_only(&Options::default(), &path, &column_families, false)?;
        assert_eq!(
            db.get(b"schema_version")?,
            Some(1_u16.to_le_bytes().to_vec())
        );
        assert_eq!(db.get(STORE_CONTROL_KEY)?, None);
        Ok(())
    }

    #[test]
    fn another_schema_version_is_refused_without_rewriting_control()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("wrong-schema-version");
        let store = RocksDbCanonicalBuilder::create_fresh(
            &path,
            CanonicalStoreWorkload::Explorer,
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        drop(store);

        let column_families = DB::list_cf(&Options::default(), &path)?;
        let db = DB::open_cf(&Options::default(), &path, &column_families)?;
        let mut control = db.get(STORE_CONTROL_KEY)?.ok_or("control should exist")?;
        let schema_version_start = CANONICAL_STORE_IDENTITY.len();
        control[schema_version_start..schema_version_start + 2]
            .copy_from_slice(&1_u16.to_le_bytes());
        db.put(STORE_CONTROL_KEY, &control)?;
        drop(db);

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Explorer,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("another schema version should be rejected")?;
        assert!(
            error
                .to_string()
                .contains("schema version 1 does not equal required version 7"),
            "{error}"
        );
        assert_eq!(read_control(&path)?, control);
        Ok(())
    }

    #[test]
    fn ready_open_does_not_recreate_a_missing_required_family()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("missing-family");
        let store = RocksDbCanonicalBuilder::create_fresh(
            &path,
            CanonicalStoreWorkload::Explorer,
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        drop(store);

        let column_families = DB::list_cf(&Options::default(), &path)?;
        let db = DB::open_cf(&Options::default(), &path, &column_families)?;
        db.drop_cf(TRANSACTION_BLOB_COLUMN_FAMILY)?;
        drop(db);
        let families_without_transaction_blobs = DB::list_cf(&Options::default(), &path)?;

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            CanonicalStoreWorkload::Explorer,
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("missing required family should be rejected")?;
        assert!(matches!(
            error,
            CanonicalStoreError::AdmissionRefused { .. }
        ));
        assert_eq!(
            DB::list_cf(&Options::default(), &path)?,
            families_without_transaction_blobs
        );
        assert!(
            !families_without_transaction_blobs
                .iter()
                .any(|family| family == TRANSACTION_BLOB_COLUMN_FAMILY)
        );
        Ok(())
    }

    #[test]
    fn contract_identity_and_schema_are_exact() {
        assert_eq!(CANONICAL_STORE_IDENTITY, "canonical");
        assert_eq!(CANONICAL_STORE_SCHEMA_VERSION, 7);
    }

    fn read_control(path: &Path) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let column_families = DB::list_cf(&Options::default(), path)?;
        let db = DB::open_cf_for_read_only(&Options::default(), path, &column_families, false)?;
        db.get(STORE_CONTROL_KEY)?
            .ok_or_else(|| "store control should exist".into())
    }

    fn complete_build_plan() -> Result<CanonicalStoreBuildPlan, Box<dyn std::error::Error>> {
        let activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?;
        Ok(CanonicalStoreBuildPlan::complete(
            &activations,
            0,
            BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
            RawBlobRetention::Transactions,
            CanonicalReorgPolicy::new(100)?,
        )?)
    }
}
