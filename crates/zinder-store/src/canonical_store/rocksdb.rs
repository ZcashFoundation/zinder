use std::{fs, path::Path, sync::Arc};

use parking_lot::Mutex;
use rust_rocksdb::{
    Cache, ColumnFamilyDescriptor, DB, DBCompressionType, IteratorMode, Options, WriteBatch,
    WriteOptions, checkpoint::Checkpoint,
};
use zinder_core::{
    BlockId, CanonicalHistoryBounds, Network, NetworkUpgradeActivations,
    NetworkUpgradeActivationsFingerprint, NetworkUpgradeActivationsFingerprintVersion,
};

use crate::{
    BoundedRocksDbOpen, RawBlobRetention, RocksDbIoMode, RocksDbOpenRole, RocksDbResourceBudget,
    StoreRole, build_block_based_table_factory, build_point_lookup_block_based_table_factory,
    keep_blocks_of_dropped_table_files, kv::build_uncached_block_based_table_factory,
    open_bounded_rocksdb, open_bounded_rocksdb_read_only_probe,
};

use super::{
    CANONICAL_STORE_IDENTITY, CANONICAL_STORE_SCHEMA_VERSION, CanonicalStoreBuildState,
    CanonicalStoreError, CanonicalStoreReadyEvidence, ConstructionTreeStateCheckpointPolicy,
    activation_schedule::CanonicalActivationSchedule,
    block_replay::CanonicalReplayScan,
    construction_manifest::{
        ConstructionManifestDetails, copy_construction_manifest,
        read_construction_manifest_binding, validate_ready_construction_manifest,
    },
    control::{
        ActivationScheduleBinding, ControlRevisions, DecodedStoreControl, decode_store_control,
        encode_building_store_control, encode_ready_store_control_with_revisions,
    },
    event_lifecycle::{
        PROJECTION_BUILD_LEASE_GENERATION_KEY, RETENTION_FLOOR_KEY, is_projection_build_lease_key,
    },
    mempool_lifecycle::{
        MEMPOOL_EVENT_RETENTION_FLOOR_KEY, MEMPOOL_EVENT_SEQUENCE_KEY, MempoolEventReadCounters,
        MempoolEventRetentionProgress, read_initial_mempool_event_sequence,
        validate_mempool_lifecycle_admission,
    },
    publication::{read_chain_epoch_tips, validate_ready_publication},
    replay_tail::AuthenticatedReplayTail,
    settled_segments::{
        cache::SettledSegmentReaderCache,
        layout::SettledSegmentIndexEntry,
        sealer::{cleanup_stale_staging_files, cleanup_unreferenced_segment_files},
        snapshot::{snapshot_pinned_segments, verify_pinned_segments_into_cache},
        store_access::{
            SegmentStoreContext, read_all_settled_segment_entries, segment_modules_for_stored_set,
            sync_segments_manifest_from_db, verify_cold_segments_and_manifest,
        },
    },
    stored_set::{CANONICAL_DATA_COLUMN_FAMILIES, validate_exact_column_families},
};

/// What a READY canonical store records that a writer checks node endpoints
/// against before it opens the store.
///
/// Read with [`RocksDbCanonicalStore::read_ready_summary`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalReadyStoreSummary {
    admitted_activations: Option<NetworkUpgradeActivations>,
    construction_activations_fingerprint: NetworkUpgradeActivationsFingerprint,
    reorg_policy: super::CanonicalReorgPolicy,
    visible_tip: BlockId,
    settled_tip: BlockId,
}

impl CanonicalReadyStoreSummary {
    /// Returns the effective canonical replacement depth admitted by this store.
    #[must_use]
    pub const fn reorg_policy(&self) -> super::CanonicalReorgPolicy {
        self.reorg_policy
    }

    /// Returns the visible tip of the store's READY fence.
    #[must_use]
    pub const fn visible_tip(&self) -> BlockId {
        self.visible_tip
    }

    /// Returns the settled tip of the visible chain epoch, below which a
    /// reorg is beyond the store's window.
    #[must_use]
    pub const fn settled_tip(&self) -> BlockId {
        self.settled_tip
    }

    /// Returns whether `activations` is exactly the table the store follows:
    /// its latest writer-admitted schedule revision or, before the first
    /// revision, the table it was constructed with.
    ///
    /// Any other table, including the construction table with entries
    /// appended, would make the writer admit a new revision.
    #[must_use]
    pub fn follows_activations(&self, activations: &NetworkUpgradeActivations) -> bool {
        self.admitted_activations.as_ref().map_or_else(
            || {
                activations.fingerprint(self.construction_activations_fingerprint.version())
                    == self.construction_activations_fingerprint
            },
            |admitted| admitted == activations,
        )
    }
}

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
    build_plan: super::CanonicalStoreBuildPlan,
    database_identity: Vec<u8>,
    activation_schedule: Option<CanonicalActivationSchedule>,
    reorg_policy: super::CanonicalReorgPolicy,
}

pub(super) const STORE_CONTROL_KEY: &[u8] = b"store_control";
pub(super) const BLOCK_HEADER_COLUMN_FAMILY: &str = "block_header";
pub(super) const BLOCK_HASH_INDEX_COLUMN_FAMILY: &str = "block_hash_index";
pub(super) const TRANSACTION_LOCATION_COLUMN_FAMILY: &str = "transaction_location";
pub(super) const LIVE_TAIL_BLOCK_COLUMN_FAMILY: &str = "live_tail_block";
pub(super) const LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY: &str = "live_tail_raw_block";
pub(super) const SETTLED_SEGMENT_INDEX_COLUMN_FAMILY: &str = "settled_segment_index";
pub(super) const TREE_STATE_CHECKPOINT_COLUMN_FAMILY: &str = "tree_state_checkpoint";
pub(super) const SUBTREE_ROOT_COLUMN_FAMILY: &str = "subtree_root";
pub(super) const CHAIN_EPOCH_COLUMN_FAMILY: &str = "chain_epoch";
pub(super) const CHAIN_EVENT_COLUMN_FAMILY: &str = "chain_event";
pub(super) const MEMPOOL_EVENT_COLUMN_FAMILY: &str = "mempool_event";
pub(super) const DISPLACED_BLOCK_FACTS_COLUMN_FAMILY: &str = "displaced_block_facts";

#[derive(Clone)]
pub(super) struct CanonicalStoreAdmissionExpectation {
    network: Network,
    activations: ActivationAdmission,
    allow_schedule_update: bool,
    stored_data: StoredDataAdmission,
    reorg_policy: ReorgPolicyAdmission,
}

#[derive(Clone)]
pub(super) enum StoredDataAdmission {
    Exact(RawBlobRetention),
    Minimum(super::CanonicalStoreRequirements),
}

/// How admission compares a caller's activation table with persisted identity.
#[derive(Clone, Debug)]
enum ActivationAdmission {
    /// The persisted construction fingerprint must equal this one.
    ConstructionBaseline(NetworkUpgradeActivationsFingerprint),
    /// A node-observed table, admitted against the writer schedule through the tip.
    NodeObserved(NetworkUpgradeActivations),
    /// The caller adopts the writer-admitted schedule.
    WriterAdmitted,
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
        stored_data: StoredDataAdmission,
        reorg_policy: super::CanonicalReorgPolicy,
    ) -> Self {
        Self {
            network: activations.network(),
            activations: ActivationAdmission::NodeObserved(activations.clone()),
            allow_schedule_update: false,
            stored_data,
            reorg_policy: ReorgPolicyAdmission::Exact(reorg_policy),
        }
    }

    /// Expects a READY store of `network` whose writer has admitted a
    /// network-upgrade schedule, which the reader then adopts.
    pub(super) const fn adopting_writer_schedule(
        network: Network,
        stored_data: StoredDataAdmission,
        reorg_policy: super::CanonicalReorgPolicy,
    ) -> Self {
        Self {
            network,
            activations: ActivationAdmission::WriterAdmitted,
            allow_schedule_update: false,
            stored_data,
            reorg_policy: ReorgPolicyAdmission::Exact(reorg_policy),
        }
    }

    /// Expects the persisted construction identity with an exact effective
    /// window, which equals the construction window until a writer raise.
    pub(super) fn from_build_plan(
        build_plan: &super::CanonicalStoreBuildPlan,
        effective_reorg_policy: super::CanonicalReorgPolicy,
    ) -> Self {
        Self {
            network: build_plan.network(),
            activations: ActivationAdmission::ConstructionBaseline(
                build_plan.network_upgrade_activations_fingerprint(),
            ),
            allow_schedule_update: false,
            stored_data: StoredDataAdmission::Exact(build_plan.raw_blob_retention()),
            reorg_policy: ReorgPolicyAdmission::Exact(effective_reorg_policy),
        }
    }

    pub(super) fn validate_reader_requirements(
        &self,
        path: &Path,
        stored_set: super::CanonicalStoredSet,
    ) -> Result<(), CanonicalStoreError> {
        if let StoredDataAdmission::Minimum(requirements) = &self.stored_data {
            stored_set.require(path, requirements)?;
        }
        Ok(())
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
    pub(super) build_plan: super::CanonicalStoreBuildPlan,
    pub(super) cursor_auth_key: [u8; 32],
    pub(super) ready_evidence: CanonicalStoreReadyEvidence,
    construction_tree_state_checkpoint_policy: Option<ConstructionTreeStateCheckpointPolicy>,
    pub(super) stored_set: super::CanonicalStoredSet,
    /// Serializes retained-event, projection-lease, and mempool-log lifecycle changes.
    ///
    /// The primary handle owns every canonical mutation. Keeping this lock on
    /// that handle makes the pruning floor and the durable lease set one
    /// indivisible lifecycle boundary without exposing a second writer API.
    pub(super) lifecycle_lock: Arc<Mutex<()>>,
    /// Resumable process-local state for bounded mempool retention steps.
    pub(super) mempool_retention_progress: Mutex<Option<MempoolEventRetentionProgress>>,
    /// Broadcasts monotonic durable mempool-event head advances to caught-up subscribers.
    pub(super) mempool_event_head: tokio::sync::watch::Sender<u64>,
    /// Cumulative durable mempool-event read and decode counts for diagnostics and tests.
    pub(super) mempool_event_read_counters: MempoolEventReadCounters,
    pub(super) activation_schedule: Option<CanonicalActivationSchedule>,
    /// Writer-admitted window above the construction policy, if any.
    pub(super) revised_reorg_policy: Option<super::CanonicalReorgPolicy>,
    /// Unsettled replay rows this handle has authenticated, extended by each
    /// committed transition.
    pub(super) replay_tail: AuthenticatedReplayTail,
    /// Bounded LRU cache of verified settled-segment readers rooted at this store.
    pub(super) segment_cache: Arc<SettledSegmentReaderCache>,
    /// Contiguous ascending settled-segment entries cached in memory for `O(log N)` routing.
    pub(super) sealed_segments: Vec<SettledSegmentIndexEntry>,
}

impl RocksDbCanonicalStore {
    #[allow(
        clippy::too_many_arguments,
        reason = "publication hands over every authenticated part of the READY store it just verified"
    )]
    pub(super) fn from_published(
        bounded_open: BoundedRocksDbOpen,
        build_plan: super::CanonicalStoreBuildPlan,
        cursor_auth_key: [u8; 32],
        ready_evidence: &CanonicalStoreReadyEvidence,
        manifest_details: ConstructionManifestDetails,
        activation_schedule: CanonicalActivationSchedule,
        replay_tail: AuthenticatedReplayTail,
        sealed_segments: Vec<SettledSegmentIndexEntry>,
        segment_cache: Arc<SettledSegmentReaderCache>,
    ) -> Self {
        let stored_set = build_plan.admitted_stored_set(manifest_details.subtree_root_coverage);
        segment_cache.record_corpus_admission(StoreRole::CanonicalPrimary);
        Self {
            bounded_open,
            build_plan,
            cursor_auth_key,
            ready_evidence: *ready_evidence,
            construction_tree_state_checkpoint_policy: manifest_details
                .tree_state_checkpoint_policy,
            stored_set,
            lifecycle_lock: Arc::new(Mutex::new(())),
            mempool_retention_progress: Mutex::new(None),
            mempool_event_head: tokio::sync::watch::Sender::new(0),
            mempool_event_read_counters: MempoolEventReadCounters::default(),
            activation_schedule: Some(activation_schedule),
            revised_reorg_policy: None,
            replay_tail,
            segment_cache,
            sealed_segments,
        }
    }

    pub(super) fn path(&self) -> &Path {
        self.bounded_open.db.path()
    }

    pub(super) fn segment_context(&self) -> SegmentStoreContext<'_> {
        SegmentStoreContext {
            db: &self.bounded_open.db,
            segment_cache: Some(&self.segment_cache),
            sealed_segments: Some(&self.sealed_segments),
        }
    }

    /// Opens an existing READY canonical store after exact admission.
    ///
    /// Admission validates the complete column-family set, singleton control
    /// key, identity, schema, exact baseline or admitted network-upgrade table,
    /// raw-blob retention, canonical reorg policy, source range,
    /// construction manifest identity, and readiness evidence before opening a
    /// writer that cannot create data families.
    pub fn open_ready(
        path: impl AsRef<Path>,
        expected_network_upgrade_activations: &NetworkUpgradeActivations,
        expected_raw_blob_retention: RawBlobRetention,
        expected_reorg_policy: super::CanonicalReorgPolicy,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        let path = path.as_ref();
        let expectation = CanonicalStoreAdmissionExpectation::from_activations(
            expected_network_upgrade_activations,
            StoredDataAdmission::Exact(expected_raw_blob_retention),
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
    pub fn open_ready_with_network_upgrade_schedule(
        path: impl AsRef<Path>,
        observed_activations: &NetworkUpgradeActivations,
        expected_raw_blob_retention: RawBlobRetention,
        reorg_policy: super::CanonicalReorgPolicy,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        let expectation = CanonicalStoreAdmissionExpectation::from_activations(
            observed_activations,
            StoredDataAdmission::Exact(expected_raw_blob_retention),
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
        let decoded = self.write_control_revisions(
            WriteBatch::default(),
            revisions,
            reorg_policy,
            "reorg policy revision",
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

    /// Returns the schedule revision bound to this READY store.
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
        let previous = self.activation_schedule.as_ref().ok_or_else(|| {
            CanonicalStoreError::admission(
                self.bounded_open.db.path(),
                "READY activation schedule is absent",
            )
        })?;
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
        let schedule = CanonicalActivationSchedule::successor(
            previous,
            visible_tip,
            observed_activations.clone(),
        )
        .map_err(|reason| CanonicalStoreError::admission(self.bounded_open.db.path(), reason))?;
        Ok(Some(schedule))
    }

    fn persist_activation_schedule(
        &mut self,
        schedule: &CanonicalActivationSchedule,
    ) -> Result<(), CanonicalStoreError> {
        let mut batch = WriteBatch::default();
        schedule.put_into(&mut batch).map_err(|reason| {
            CanonicalStoreError::admission(self.bounded_open.db.path(), reason)
        })?;
        let decoded = self.write_control_revisions(
            batch,
            ControlRevisions {
                activation_schedule_binding: Some(schedule.binding()),
                ..self.control_revisions()
            },
            self.reorg_policy(),
            "activation schedule admission",
        )?;
        let stored_schedule =
            read_admitted_schedule(&self.bounded_open.db, self.bounded_open.db.path(), &decoded)?
                .ok_or_else(|| {
                CanonicalStoreError::admission(
                    self.bounded_open.db.path(),
                    "activation schedule binding was not persisted",
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

    /// Durably writes `batch` with the store control that binds `revisions`,
    /// then returns that control as validated on readback under
    /// `reorg_policy`. Each caller compares the readback with what it admitted.
    fn write_control_revisions(
        &self,
        mut batch: WriteBatch,
        revisions: ControlRevisions,
        reorg_policy: super::CanonicalReorgPolicy,
        operation: &'static str,
    ) -> Result<DecodedStoreControl, CanonicalStoreError> {
        let path = self.bounded_open.db.path();
        let encoded_control = encode_ready_store_control_with_revisions(
            &self.build_plan,
            self.cursor_auth_key,
            &self.ready_evidence,
            revisions,
        )
        .map_err(|source| CanonicalStoreError::admission(path, source.to_string()))?;
        batch.put(STORE_CONTROL_KEY, encoded_control);
        let mut options = WriteOptions::default();
        options.disable_wal(false);
        options.set_sync(true);
        super::crash_point("canonical.control_revision.before_atomic_write");
        self.bounded_open
            .db
            .write_opt(&batch, &options)
            .map_err(|source| CanonicalStoreError::RocksDbOperation { operation, source })?;
        super::crash_point("canonical.control_revision.after_atomic_write");
        validate_open_store_control(
            &self.bounded_open.db,
            path,
            &CanonicalStoreAdmissionExpectation::from_build_plan(&self.build_plan, reorg_policy),
        )
    }

    fn open_ready_with_expectation(
        path: &Path,
        expectation: &CanonicalStoreAdmissionExpectation,
        resource_budget: RocksDbResourceBudget,
    ) -> Result<Self, CanonicalStoreError> {
        let store = Self::open_ready_with_mode(
            path,
            expectation,
            resource_budget,
            PrimarySegmentOpenMode::HealPrimary,
        )?;
        store
            .segment_cache
            .record_corpus_admission(StoreRole::CanonicalPrimary);
        Ok(store)
    }

    fn open_ready_with_mode(
        path: &Path,
        expectation: &CanonicalStoreAdmissionExpectation,
        resource_budget: RocksDbResourceBudget,
        mode: PrimarySegmentOpenMode,
    ) -> Result<Self, CanonicalStoreError> {
        validate_resource_budget(resource_budget)?;
        let store_path = canonical_store_path(path)?;
        let (admitted_database_identity, admitted_control, admitted_manifest_details) =
            admit_existing_store(&store_path, expectation, resource_budget)?;
        let (CanonicalStoreBuildState::Ready(_), Some(manifest_details)) =
            (admitted_control.build_state, admitted_manifest_details)
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
        let opened_control = verify_opened_primary_control(
            &bounded_open.db,
            &store_path,
            expectation,
            &admitted_database_identity,
            &admitted_control,
        )?;
        let CanonicalStoreBuildState::Ready(opened_ready_evidence) = opened_control.build_state
        else {
            return Err(CanonicalStoreError::StoreNotReady { path: store_path });
        };
        let activation_schedule =
            read_admitted_schedule(&bounded_open.db, &store_path, &opened_control)?;
        let reorg_policy = opened_control.effective_reorg_policy();
        let build_plan = opened_control.build_plan;
        let stored_set = build_plan.admitted_stored_set(manifest_details.subtree_root_coverage);
        let (sealed_segments, segment_cache) = match mode {
            PrimarySegmentOpenMode::HealPrimary => cleanup_primary_segments_and_build_cache(
                &store_path,
                &bounded_open.db,
                build_plan.network(),
                stored_set,
            )?,
            PrimarySegmentOpenMode::VerifyImmutableCold => verify_cold_segments_and_manifest(
                &store_path,
                &bounded_open.db,
                build_plan.network(),
                stored_set,
            )?,
        };
        let ctx = SegmentStoreContext {
            db: &bounded_open.db,
            segment_cache: Some(&segment_cache),
            sealed_segments: Some(&sealed_segments),
        };
        let replay_tail =
            validate_ready_publication(ctx, &build_plan, reorg_policy, &opened_ready_evidence)?;
        super::displaced_archive::backfill_displaced_root_index_if_absent(&bounded_open.db)?;
        let initial_mempool_event_sequence = read_initial_mempool_event_sequence(&bounded_open.db)?;
        Ok(Self {
            bounded_open,
            build_plan,
            cursor_auth_key: opened_control.cursor_auth_key,
            ready_evidence: opened_ready_evidence,
            construction_tree_state_checkpoint_policy: manifest_details
                .tree_state_checkpoint_policy,
            stored_set,
            lifecycle_lock: Arc::new(Mutex::new(())),
            mempool_retention_progress: Mutex::new(None),
            mempool_event_head: tokio::sync::watch::Sender::new(initial_mempool_event_sequence),
            mempool_event_read_counters: MempoolEventReadCounters::default(),
            activation_schedule,
            revised_reorg_policy: opened_control.revised_reorg_policy,
            replay_tail,
            segment_cache,
            sealed_segments,
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
        snapshot_pinned_segments(self.bounded_open.db.path(), target, &self.sealed_segments)?;
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
            admission.reorg_policy,
        );
        let cold_checkpoint = Self::open_ready_with_mode(
            target,
            &expectation,
            admission_resource_budget,
            PrimarySegmentOpenMode::VerifyImmutableCold,
        )?;
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
            build_plan: cold_checkpoint.build_plan.clone(),
            ready_evidence: cold_checkpoint.ready_evidence,
            reorg_policy: cold_checkpoint.reorg_policy(),
            activation_schedule: cold_checkpoint.activation_schedule,
        })
    }

    /// Reads the READY evidence of a private wallet-serving checkpoint without
    /// opening a writer. The caller compares the evidence against its trusted manifest.
    pub fn cold_admit_recovery_checkpoint(
        target: &Path,
        network: Network,
        retention: RawBlobRetention,
        reorg_policy: super::CanonicalReorgPolicy,
    ) -> Result<CanonicalOwnerCheckpointEvidence, CanonicalStoreError> {
        let budget = RocksDbResourceBudget::read_only_probe_defaults();
        let opened = open_canonical_admission_probe(target, budget)?;
        let expectation = CanonicalStoreAdmissionExpectation::adopting_writer_schedule(
            network,
            StoredDataAdmission::Exact(retention),
            reorg_policy,
        );
        let (database_identity, control, manifest_details) =
            admit_open_store(target, &opened.db, &expectation)?;
        let (CanonicalStoreBuildState::Ready(ready_evidence), Some(manifest_details)) =
            (control.build_state, manifest_details)
        else {
            return Err(CanonicalStoreError::StoreNotReady {
                path: target.to_path_buf(),
            });
        };
        let stored_set = control
            .build_plan
            .admitted_stored_set(manifest_details.subtree_root_coverage);
        let (sealed_segments, segment_cache) =
            verify_cold_segments_and_manifest(target, &opened.db, control.network, stored_set)?;
        let ctx = SegmentStoreContext {
            db: &opened.db,
            segment_cache: Some(&segment_cache),
            sealed_segments: Some(&sealed_segments),
        };
        validate_ready_publication(ctx, &control.build_plan, reorg_policy, &ready_evidence)?;
        let activation_schedule = read_admitted_schedule(&opened.db, target, &control)?;
        Ok(CanonicalOwnerCheckpointEvidence {
            database_identity,
            store_identity: CANONICAL_STORE_IDENTITY,
            schema_version: CANONICAL_STORE_SCHEMA_VERSION,
            build_plan: control.build_plan,
            ready_evidence,
            reorg_policy,
            activation_schedule,
        })
    }

    /// Releases the source projector's retention leases on an exclusively owned,
    /// inactive private restore. The next owner acquires a fresh generation.
    /// The caller must supply the cold evidence for this exact copy.
    pub fn release_restored_projection_leases(
        target: &Path,
        checkpoint: &CanonicalOwnerCheckpointEvidence,
    ) -> Result<(), CanonicalStoreError> {
        let expectation = CanonicalStoreAdmissionExpectation::from_build_plan(
            &checkpoint.build_plan,
            checkpoint.reorg_policy,
        );
        let store = Self::open_ready_with_mode(
            target,
            &expectation,
            RocksDbResourceBudget::read_only_probe_defaults(),
            PrimarySegmentOpenMode::VerifyImmutableCold,
        )?;
        let identity = store.bounded_open.db.get_db_identity().map_err(|source| {
            CanonicalStoreError::CheckpointFailed {
                path: target.to_path_buf(),
                source,
            }
        })?;
        if identity != checkpoint.database_identity
            || store.ready_evidence != checkpoint.ready_evidence
            || store.activation_schedule != checkpoint.activation_schedule
        {
            return Err(CanonicalStoreError::admission(
                target,
                "private restore checkpoint changed before releasing its source leases",
            ));
        }
        store.clear_restored_retention_leases()
    }

    /// Reads the immutable construction-manifest identity without opening a
    /// `RocksDB` primary. Archive packagers use this narrow descriptor to bind
    /// a physical checkpoint to the first READY construction proof.
    pub fn read_construction_manifest_binding(
        path: impl AsRef<Path>,
    ) -> Result<super::CanonicalConstructionManifestBinding, CanonicalStoreError> {
        read_construction_manifest_binding(path.as_ref())
    }

    /// Reads what a canonical writer checks node endpoints against before it
    /// opens the store, without opening a `RocksDB` primary.
    ///
    /// Returns `None` when no store exists at `path` or the store is still
    /// being built.
    pub fn read_ready_summary(
        path: impl AsRef<Path>,
    ) -> Result<Option<CanonicalReadyStoreSummary>, CanonicalStoreError> {
        let path = path.as_ref();
        let exists = path.try_exists().map_err(|error| {
            CanonicalStoreError::admission(path, format!("store path check failed: {error}"))
        })?;
        if !exists {
            return Ok(None);
        }
        let mut column_families = DB::list_cf(&Options::default(), path).map_err(|source| {
            CanonicalStoreError::admission(
                path,
                format!("column-family discovery failed: {source}"),
            )
        })?;
        validate_exact_column_families(path, &mut column_families)?;
        let bounded_open = open_bounded_rocksdb_read_only_probe(
            path,
            &column_families,
            RocksDbResourceBudget::read_only_probe_defaults(),
        )
        .map_err(|source| {
            CanonicalStoreError::admission(path, format!("read-only open failed: {source}"))
        })?;
        let db = &bounded_open.db;
        let encoded_control = db
            .get(STORE_CONTROL_KEY)
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "store control read",
                source,
            })?
            .ok_or_else(|| CanonicalStoreError::admission(path, "store identity is absent"))?;
        let control = decode_store_control(path, &encoded_control)?;
        let CanonicalStoreBuildState::Ready(ready_evidence) = control.build_state else {
            return Ok(None);
        };
        let (visible_tip, settled_tip) = read_chain_epoch_tips(db, ready_evidence.visible_epoch)?;
        if visible_tip != ready_evidence.visible_tip {
            return Err(CanonicalStoreError::admission(
                path,
                "visible chain epoch does not match the READY fence",
            ));
        }
        let admitted_activations = read_admitted_schedule(db, path, &control)?
            .map(|schedule| schedule.activations().clone());
        let reorg_policy = control.effective_reorg_policy();
        Ok(Some(CanonicalReadyStoreSummary {
            admitted_activations,
            construction_activations_fingerprint: control
                .build_plan
                .network_upgrade_activations_fingerprint(),
            reorg_policy,
            visible_tip,
            settled_tip,
        }))
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
        super::activation_schedule::admits_activation_table(
            &self.build_plan,
            self.activation_schedule.as_ref(),
            activations,
        )
    }

    pub(super) fn activation_schedule_binding(&self) -> Option<ActivationScheduleBinding> {
        self.activation_schedule
            .as_ref()
            .map(CanonicalActivationSchedule::binding)
    }

    /// Returns the retained block data and historical floor.
    #[must_use]
    pub const fn stored_set(&self) -> super::CanonicalStoredSet {
        self.stored_set
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

    /// Returns the policy recorded in the authenticated construction manifest.
    ///
    /// A legacy manifest without this record returns `None`.
    #[must_use]
    pub const fn construction_tree_state_checkpoint_policy(
        &self,
    ) -> Option<ConstructionTreeStateCheckpointPolicy> {
        self.construction_tree_state_checkpoint_policy
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
        CanonicalReplayScan::new(self.segment_context(), &self.ready_evidence)
    }

    /// Returns a `Sync` slice source for concurrent height-bounded replay scans
    /// against this admitted READY fence.
    #[must_use]
    pub fn canonical_replay_slice_source(&self) -> super::CanonicalReplaySliceSource<'_> {
        super::CanonicalReplaySliceSource::new(self.segment_context(), &self.ready_evidence)
    }

    /// Publishes throttled resource gauges for the canonical writer.
    pub fn record_resource_gauges(&self) {
        self.bounded_open
            .record_resource_gauges(StoreRole::CanonicalPrimary, &CANONICAL_DATA_COLUMN_FAMILIES);
    }

    /// Returns the filesystem I/O mode selected by the bounded `RocksDB` open.
    #[must_use]
    pub const fn io_mode(&self) -> RocksDbIoMode {
        self.bounded_open.io_mode
    }

    /// Returns one DB-wide `RocksDB` statistics ticker count for this canonical writer.
    #[must_use]
    pub fn rocksdb_ticker_count(&self, ticker: rust_rocksdb::statistics::Ticker) -> u64 {
        self.bounded_open.statistics.get_ticker_count(ticker)
    }
}

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
    let options = CanonicalColumnFamilyOptions::new(block_cache, resource_budget);
    CANONICAL_DATA_COLUMN_FAMILIES
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, options.for_family(name).clone()))
        .collect()
}

/// Column families of a canonical secondary that verifies stored tree-state
/// checkpoints.
///
/// They match [`canonical_column_family_descriptors`] except that the
/// checkpoint and block-header families bypass the block cache so reads
/// recheck the checksum of each data block fetched from their table files.
pub(super) fn canonical_verification_column_family_descriptors(
    block_cache: &Cache,
    resource_budget: RocksDbResourceBudget,
) -> Vec<ColumnFamilyDescriptor> {
    let family_options = CanonicalColumnFamilyOptions::new(block_cache, resource_budget);
    CANONICAL_DATA_COLUMN_FAMILIES
        .into_iter()
        .map(|name| {
            let mut options = family_options.for_family(name).clone();
            if matches!(
                name,
                TREE_STATE_CHECKPOINT_COLUMN_FAMILY | BLOCK_HEADER_COLUMN_FAMILY
            ) {
                options.set_block_based_table_factory(&build_uncached_block_based_table_factory());
            }
            ColumnFamilyDescriptor::new(name, options)
        })
        .collect()
}

const EPOCH_EVENT_BLOOM_FILTER_BITS_PER_KEY: f64 = 10.0;

pub(super) struct CanonicalColumnFamilyOptions {
    hot: Options,
    filtered_hot: Options,
    cold: Options,
    uncompressed: Options,
}

impl CanonicalColumnFamilyOptions {
    pub(super) fn new(block_cache: &Cache, resource_budget: RocksDbResourceBudget) -> Self {
        let hot = canonical_data_options(block_cache, resource_budget);
        let mut filtered_hot = hot.clone();
        let mut epoch_event_table = build_block_based_table_factory(block_cache);
        epoch_event_table.set_bloom_filter(EPOCH_EVENT_BLOOM_FILTER_BITS_PER_KEY, false);
        filtered_hot.set_block_based_table_factory(&epoch_event_table);

        let mut cold = hot.clone();
        cold.set_bottommost_compression_type(DBCompressionType::Zstd);
        cold.set_bottommost_compression_options(-14, 3, 0, 0, true);

        let mut uncompressed = hot.clone();
        uncompressed.set_compression_type(DBCompressionType::None);
        uncompressed.set_block_based_table_factory(&build_point_lookup_block_based_table_factory(
            block_cache,
        ));
        Self {
            hot,
            filtered_hot,
            cold,
            uncompressed,
        }
    }

    pub(super) fn for_family(&self, name: &str) -> &Options {
        match name {
            TREE_STATE_CHECKPOINT_COLUMN_FAMILY
            | DISPLACED_BLOCK_FACTS_COLUMN_FAMILY
            | LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY => &self.cold,
            BLOCK_HASH_INDEX_COLUMN_FAMILY | TRANSACTION_LOCATION_COLUMN_FAMILY => {
                &self.uncompressed
            }
            CHAIN_EPOCH_COLUMN_FAMILY | CHAIN_EVENT_COLUMN_FAMILY => &self.filtered_hot,
            _ => &self.hot,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrimarySegmentOpenMode {
    HealPrimary,
    VerifyImmutableCold,
}

fn verify_opened_primary_control(
    db: &DB,
    store_path: &Path,
    expectation: &CanonicalStoreAdmissionExpectation,
    admitted_database_identity: &[u8],
    admitted_control: &DecodedStoreControl,
) -> Result<DecodedStoreControl, CanonicalStoreError> {
    let opened_database_identity =
        db.get_db_identity()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "opened database identity read",
                source,
            })?;
    if opened_database_identity.as_slice() != admitted_database_identity {
        return Err(CanonicalStoreError::admission(
            store_path,
            "store database identity changed before opening primary",
        ));
    }
    let opened_control = validate_open_store_control(db, store_path, expectation)?;
    validate_mempool_lifecycle_admission(
        db,
        opened_control.network,
        opened_control.cursor_auth_key,
    )?;
    if opened_control != *admitted_control {
        return Err(CanonicalStoreError::admission(
            store_path,
            "store control state changed before opening primary",
        ));
    }
    Ok(opened_control)
}

fn cleanup_primary_segments_and_build_cache(
    store_path: &Path,
    db: &DB,
    network: Network,
    stored_set: super::CanonicalStoredSet,
) -> Result<
    (
        Vec<SettledSegmentIndexEntry>,
        Arc<SettledSegmentReaderCache>,
    ),
    CanonicalStoreError,
> {
    cleanup_stale_staging_files(store_path)?;
    let entries = read_all_settled_segment_entries(db)?;
    let expected_modules = segment_modules_for_stored_set(stored_set);
    for entry in &entries {
        if entry.modules != expected_modules {
            return Err(CanonicalStoreError::SettledSegment {
                reason: format!(
                    "settled segment {}-{} modules {:?} do not match admitted modules {:?}",
                    entry.start_height.value(),
                    entry.end_block.height.value(),
                    entry.modules,
                    expected_modules
                ),
            });
        }
    }
    cleanup_unreferenced_segment_files(store_path, &entries)?;
    let cache = SettledSegmentReaderCache::with_admitted_corpus(store_path, &entries)?;
    verify_pinned_segments_into_cache(store_path, &entries, false, &cache)?;
    let _ = sync_segments_manifest_from_db(db, network)?;
    Ok((entries, cache))
}

fn canonical_data_options(block_cache: &Cache, resource_budget: RocksDbResourceBudget) -> Options {
    let mut options = Options::default();
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_block_based_table_factory(&build_block_based_table_factory(block_cache));
    options.set_write_buffer_size(
        usize::try_from(resource_budget.write_buffer_bytes).unwrap_or(usize::MAX),
    );
    options.set_max_write_buffer_number(resource_budget.max_write_buffer_count);
    keep_blocks_of_dropped_table_files(&mut options);
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
    build_plan: &super::CanonicalStoreBuildPlan,
    cursor_auth_key: [u8; 32],
    resource_budget: RocksDbResourceBudget,
) -> Result<(), CanonicalStoreError> {
    let bounded_open = open_bounded_rocksdb(
        RocksDbOpenRole::Primary { path },
        resource_budget,
        |_, _| Vec::new(),
    )
    .map_err(|source| CanonicalStoreError::RocksDbOperation {
        operation: "identity open",
        source,
    })?;
    let mut batch = WriteBatch::default();
    let encoded_control = encode_building_store_control(build_plan, cursor_auth_key)
        .map_err(|source| CanonicalStoreError::admission(path, source.to_string()))?;
    batch.put(STORE_CONTROL_KEY, encoded_control);
    let mut options = WriteOptions::default();
    options.disable_wal(false);
    options.set_sync(true);
    bounded_open
        .db
        .write_opt(&batch, &options)
        .map_err(|source| CanonicalStoreError::RocksDbOperation {
            operation: "identity initialization",
            source,
        })
}

pub(super) fn admit_existing_store(
    path: &Path,
    expectation: &CanonicalStoreAdmissionExpectation,
    resource_budget: RocksDbResourceBudget,
) -> Result<
    (
        Vec<u8>,
        DecodedStoreControl,
        Option<ConstructionManifestDetails>,
    ),
    CanonicalStoreError,
> {
    let opened = open_canonical_admission_probe(path, resource_budget)?;
    admit_open_store(path, &opened.db, expectation)
}

fn open_canonical_admission_probe(
    path: &Path,
    resource_budget: RocksDbResourceBudget,
) -> Result<BoundedRocksDbOpen, CanonicalStoreError> {
    let mut column_families = DB::list_cf(&Options::default(), path).map_err(|source| {
        CanonicalStoreError::admission(path, format!("column-family discovery failed: {source}"))
    })?;
    validate_exact_column_families(path, &mut column_families)?;
    open_bounded_rocksdb_read_only_probe(path, &column_families, resource_budget).map_err(
        |source| CanonicalStoreError::admission(path, format!("read-only open failed: {source}")),
    )
}

fn admit_open_store(
    path: &Path,
    db: &DB,
    expectation: &CanonicalStoreAdmissionExpectation,
) -> Result<
    (
        Vec<u8>,
        DecodedStoreControl,
        Option<ConstructionManifestDetails>,
    ),
    CanonicalStoreError,
> {
    let control = validate_open_store_control(db, path, expectation)?;
    let manifest_details = match control.build_state {
        CanonicalStoreBuildState::Ready(ready) => Some(validate_ready_construction_manifest(
            path,
            &ready,
            &control.build_plan,
        )?),
        CanonicalStoreBuildState::Building => None,
    };
    validate_mempool_lifecycle_admission(db, control.network, control.cursor_auth_key)?;
    let database_identity = db.get_db_identity().map_err(|source| {
        CanonicalStoreError::admission(path, format!("database identity read failed: {source}"))
    })?;
    Ok((database_identity, control, manifest_details))
}

fn validate_stored_data(
    path: &Path,
    expectation: &CanonicalStoreAdmissionExpectation,
    persisted: &DecodedStoreControl,
) -> Result<(), CanonicalStoreError> {
    let StoredDataAdmission::Exact(retention) = expectation.stored_data else {
        return Ok(());
    };
    if persisted.build_plan.raw_blob_retention() != retention {
        return Err(CanonicalStoreError::admission(
            path,
            format!(
                "persisted raw blob retention {} does not equal requested raw blob retention {}; rebuild the canonical store to change retention",
                persisted.build_plan.raw_blob_retention(),
                retention
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
        let (key, encoded_control) =
            row.map_err(|source| store_control_iteration_error(path, source))?;
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
    let admitted_schedule = read_admitted_schedule(db, path, &persisted)?;
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
    validate_stored_data(path, expectation, &persisted)?;
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

/// Classifies a failed read of the store-control scan.
///
/// A table file deleted under a secondary's view, which the primary's
/// compaction does between a catch-up and this scan, keeps its typed
/// `RocksDB` source, so a reader can catch up and try again. Every other
/// failure refuses admission.
fn store_control_iteration_error(path: &Path, source: rust_rocksdb::Error) -> CanonicalStoreError {
    if crate::is_missing_table_file_error(&source) {
        return CanonicalStoreError::RocksDbOperation {
            operation: "store-control iteration",
            source,
        };
    }
    CanonicalStoreError::admission(path, format!("store-control iteration failed: {source}"))
}

pub(super) fn read_admitted_schedule(
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

/// Checks node-observed tables through the visible tip. A writer opening to
/// revise its schedule checks the replacement before writing it.
fn validate_activation_expectation(
    expectation: &CanonicalStoreAdmissionExpectation,
    persisted_fingerprint: NetworkUpgradeActivationsFingerprint,
    admitted_schedule: Option<&CanonicalActivationSchedule>,
    visible_tip: Option<zinder_core::BlockHeight>,
) -> Result<(), &'static str> {
    let expected_fingerprint = match &expectation.activations {
        ActivationAdmission::ConstructionBaseline(fingerprint) => *fingerprint,
        ActivationAdmission::WriterAdmitted => return Ok(()),
        ActivationAdmission::NodeObserved(observed) => {
            if let (Some(admitted), Some(visible_tip)) = (admitted_schedule, visible_tip) {
                if expectation.allow_schedule_update
                    || observed.preserves_through(admitted.activations(), visible_tip)
                {
                    return Ok(());
                }
                return Err(
                    "node network upgrade activations differ from the writer-admitted schedule at or below the visible tip",
                );
            }
            observed.fingerprint(NetworkUpgradeActivationsFingerprintVersion::V1)
        }
    };
    if persisted_fingerprint != expected_fingerprint {
        return Err(
            "persisted network upgrade activations do not equal the requested activation table; restart the canonical writer so it admits the node's schedule",
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rust_rocksdb::{DEFAULT_COLUMN_FAMILY_NAME, statistics::Ticker};
    use tempfile::TempDir;
    use zinder_core::{
        BlockHash, BlockHeight, BlockId, TransactionId, TransactionLocation, UnixTimestampMillis,
    };

    use super::*;
    use crate::{
        BlockHashLookup, CANONICAL_STORE_IDENTITY, CANONICAL_STORE_SCHEMA_VERSION,
        CanonicalLiveAppend, CanonicalReorgPolicy, CanonicalStoreBuildPlan,
        CanonicalStoreRequirements, RocksDbCanonicalBuilder, RocksDbCanonicalSecondary,
        RocksDbOpenResources, SharedBlockCache,
        canonical_store::live_replacement_tests::{published_store, replacement_block},
    };

    /// A canonical secondary may share its block cache with other secondaries
    /// of the store, so no data family erases the blocks of a file it drops.
    /// Every data family opens with these options.
    #[test]
    fn data_families_keep_the_blocks_of_dropped_table_files() {
        let options = canonical_data_options(
            &Cache::new_lru_cache(1024 * 1024),
            RocksDbResourceBudget::for_local_tests(),
        );
        assert_eq!(options.get_uncache_aggressiveness(), 0);
    }

    #[test]
    fn canonical_compression_profiles_apply_to_external_ssts()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let options = CanonicalColumnFamilyOptions::new(
            &Cache::new_lru_cache(1024 * 1024),
            RocksDbResourceBudget::for_local_tests(),
        );
        let mut uncompressed_size = 0;
        for (family, upper, bottom) in [
            (
                BLOCK_HASH_INDEX_COLUMN_FAMILY,
                DBCompressionType::None,
                None,
            ),
            (
                TRANSACTION_LOCATION_COLUMN_FAMILY,
                DBCompressionType::None,
                None,
            ),
            (BLOCK_HEADER_COLUMN_FAMILY, DBCompressionType::Lz4, None),
            (SUBTREE_ROOT_COLUMN_FAMILY, DBCompressionType::Lz4, None),
            (LIVE_TAIL_BLOCK_COLUMN_FAMILY, DBCompressionType::Lz4, None),
            (
                SETTLED_SEGMENT_INDEX_COLUMN_FAMILY,
                DBCompressionType::Lz4,
                None,
            ),
            (
                LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY,
                DBCompressionType::Lz4,
                Some(DBCompressionType::Zstd),
            ),
            (
                TREE_STATE_CHECKPOINT_COLUMN_FAMILY,
                DBCompressionType::Lz4,
                Some(DBCompressionType::Zstd),
            ),
            (
                DISPLACED_BLOCK_FACTS_COLUMN_FAMILY,
                DBCompressionType::Lz4,
                Some(DBCompressionType::Zstd),
            ),
        ] {
            let options = options.for_family(family);
            assert_eq!(options.get_compression_type(), Some(upper), "{family}");
            assert_eq!(
                options.get_bottommost_compression_type(),
                bottom,
                "{family}"
            );
            let mut writer = zinder_rocksdb_bulk_load::OrderedSstWriter::new(
                temporary.path(),
                family,
                options,
                1024 * 1024,
            )?;
            for index in 0_u32..512 {
                writer.put(&index.to_be_bytes(), &[0x7b; 72])?;
            }
            let files = writer.finish()?;
            if upper == DBCompressionType::None {
                assert!(files.file_bytes >= 512 * 72, "{family}");
                uncompressed_size = files.file_bytes;
            } else {
                assert!(files.file_bytes < uncompressed_size / 2, "{family}");
            }
        }
        Ok(())
    }

    #[test]
    fn v2_physical_inventory_is_exact_for_every_construction_policy()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let activations = super::super::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let mut expected = [
            "block_hash_index",
            "block_header",
            "chain_epoch",
            "chain_event",
            "default",
            "displaced_block_facts",
            "live_tail_block",
            "live_tail_raw_block",
            "mempool_event",
            "settled_segment_index",
            "subtree_root",
            "transaction_location",
            "tree_state_checkpoint",
        ];
        expected.sort_unstable();
        for retention in [RawBlobRetention::None, RawBlobRetention::All] {
            let path = temporary.path().join(format!("{retention}"));
            let build_plan = CanonicalStoreBuildPlan::complete(
                &activations,
                0,
                BlockId::new(BlockHeight::new(2), BlockHash::from_bytes([2; 32])),
                retention,
                CanonicalReorgPolicy::new(100)?,
            )?;
            drop(RocksDbCanonicalBuilder::create_fresh(
                &path,
                build_plan,
                RocksDbResourceBudget::for_local_tests(),
            )?);
            let mut observed = DB::list_cf(&Options::default(), &path)?;
            observed.sort_unstable();
            assert_eq!(observed, expected, "{retention}");
        }
        Ok(())
    }

    #[test]
    fn building_store_is_not_servable() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("canonical");
        let store = RocksDbCanonicalBuilder::create_fresh(
            &path,
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
        assert_eq!(
            store.build_plan().history_bounds(),
            CanonicalHistoryBounds::complete()
        );
        drop(store);

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &testnet_activations,
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("a BUILDING store must not be servable")?;
        assert!(matches!(error, CanonicalStoreError::StoreNotReady { .. }));
        assert_eq!(RocksDbCanonicalStore::read_ready_summary(&path)?, None);

        let control_before = read_control(&path)?;
        let mainnet_activations =
            crate::canonical_store::test_network_upgrade_activations(Network::ZcashMainnet)?;
        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &mainnet_activations,
            RawBlobRetention::All,
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
            RawBlobRetention::None,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("retention mismatch should be rejected")?;
        assert!(
            error
                .to_string()
                .contains("persisted raw blob retention all")
        );
        assert_eq!(read_control(&path)?, control_before);
        Ok(())
    }

    #[test]
    fn ready_open_rejects_activation_table_mismatch() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let path = temporary.path().join("activation-mismatch");
        let store = RocksDbCanonicalBuilder::create_fresh(
            &path,
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
            RawBlobRetention::All,
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
            RawBlobRetention::All,
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
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        drop(store);

        let column_families = DB::list_cf(&Options::default(), &path)?;
        let db = DB::open_cf(&Options::default(), &path, &column_families)?;
        let mut control = db.get(STORE_CONTROL_KEY)?.ok_or("control should exist")?;
        let schema_version_start = CANONICAL_STORE_IDENTITY.len();
        control[schema_version_start..schema_version_start + 2]
            .copy_from_slice(&7_u16.to_le_bytes());
        db.put(STORE_CONTROL_KEY, &control)?;
        drop(db);

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(100)?,
            RocksDbResourceBudget::for_local_tests(),
        )
        .err()
        .ok_or("another schema version should be rejected")?;
        assert!(
            error
                .to_string()
                .contains("schema version 7 does not equal required version 2"),
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
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        drop(store);

        let column_families = DB::list_cf(&Options::default(), &path)?;
        let db = DB::open_cf(&Options::default(), &path, &column_families)?;
        db.drop_cf(LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY)?;
        drop(db);
        let families_without_raw_blocks = DB::list_cf(&Options::default(), &path)?;

        let error = RocksDbCanonicalStore::open_ready(
            &path,
            &crate::canonical_store::test_network_upgrade_activations(Network::ZcashTestnet)?,
            RawBlobRetention::All,
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
            families_without_raw_blocks
        );
        assert!(
            !families_without_raw_blocks
                .iter()
                .any(|family| family == LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY)
        );
        Ok(())
    }

    #[test]
    fn read_ready_summary_returns_none_for_absent_path_and_refuses_missing_family()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let missing_path = temporary.path().join("absent-canonical");
        assert_eq!(
            RocksDbCanonicalStore::read_ready_summary(&missing_path)?,
            None
        );

        let broken_path = temporary.path().join("missing-family-summary");
        let store = RocksDbCanonicalBuilder::create_fresh(
            &broken_path,
            complete_build_plan()?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        drop(store);
        let column_families = DB::list_cf(&Options::default(), &broken_path)?;
        let db = DB::open_cf(&Options::default(), &broken_path, &column_families)?;
        db.drop_cf(LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY)?;
        drop(db);

        let error = RocksDbCanonicalStore::read_ready_summary(&broken_path)
            .err()
            .ok_or("missing required family should fail summary probe")?;
        assert!(matches!(
            error,
            CanonicalStoreError::AdmissionRefused { .. }
        ));
        Ok(())
    }

    #[test]
    fn contract_identity_and_schema_are_exact() {
        assert_eq!(CANONICAL_STORE_IDENTITY, "canonical");
        assert_eq!(CANONICAL_STORE_SCHEMA_VERSION, 2);
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
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(100)?,
        )?)
    }

    #[test]
    fn point_lookup_families_carry_partitioned_filters_and_the_rest_do_not()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
        let activations = super::super::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let store = append_block_five_for_filter_test(store, &activations)?;
        store.bounded_open.db.flush()?;
        for family_name in [
            BLOCK_HASH_INDEX_COLUMN_FAMILY,
            TRANSACTION_LOCATION_COLUMN_FAMILY,
            CHAIN_EPOCH_COLUMN_FAMILY,
            CHAIN_EVENT_COLUMN_FAMILY,
            BLOCK_HEADER_COLUMN_FAMILY,
            LIVE_TAIL_BLOCK_COLUMN_FAMILY,
        ] {
            let family = store
                .bounded_open
                .db
                .cf_handle(family_name)
                .ok_or("canonical family absent")?;
            store.bounded_open.db.flush_cf(&family)?;
        }

        for (family_name, minimum_entries, filtered) in [
            (BLOCK_HASH_INDEX_COLUMN_FAMILY, 5, true),
            (TRANSACTION_LOCATION_COLUMN_FAMILY, 5, true),
            (BLOCK_HEADER_COLUMN_FAMILY, 5, false),
            (LIVE_TAIL_BLOCK_COLUMN_FAMILY, 5, false),
            (DEFAULT_COLUMN_FAMILY_NAME, 1, false),
        ] {
            assert_filter_table_properties(
                &store.bounded_open,
                family_name,
                minimum_entries,
                filtered,
            )?;
        }
        for family_name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
            assert_full_bloom_filter_table_properties(&store.bounded_open, family_name, 2)?;
        }
        Ok(())
    }

    #[test]
    fn absent_point_lookups_stop_at_the_filter_before_any_data_block()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let activations = super::super::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
        let block_cache =
            SharedBlockCache::new(RocksDbResourceBudget::for_local_tests().block_cache_bytes);
        let secondary = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join("secondary"),
            &activations,
            CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
            CanonicalReorgPolicy::new(2)?,
            RocksDbOpenResources::with_shared_block_cache(
                RocksDbResourceBudget::for_local_tests(),
                &block_cache,
            ),
        )?;

        // Both absent keys sort strictly between heights 1 and 4, so RocksDB
        // cannot skip the SST on its key range and must consult the filter.
        let mut absent_hash_bytes = [2_u8; 32];
        absent_hash_bytes[16] = 0x55;
        let absent_hash = BlockHash::from_bytes(absent_hash_bytes);

        let mut absent_tx_bytes = [102_u8; 32];
        absent_tx_bytes[16] = 0x55;
        let absent_tx = TransactionId::from_bytes(absent_tx_bytes);

        assert_filter_skips_data_blocks(&store.bounded_open, || {
            Ok(store.block_hash_lookup(absent_hash)? == BlockHashLookup::NotIndexed)
        })?;
        assert_filter_skips_data_blocks(&store.bounded_open, || {
            Ok(store.transaction_location(absent_tx)?.is_none())
        })?;
        assert_filter_skips_data_blocks(&secondary.bounded_open, || {
            Ok(secondary.block_hash_lookup(absent_hash)? == BlockHashLookup::NotIndexed)
        })?;
        assert_filter_skips_data_blocks(&secondary.bounded_open, || {
            Ok(secondary.transaction_location(absent_tx)?.is_none())
        })?;
        assert!(
            block_cache.pinned_usage_bytes() > 0,
            "reading partitioned point-lookup SSTs pins top-level metadata in the shared cache"
        );
        drop(secondary);
        assert_eq!(block_cache.pinned_usage_bytes(), 0);
        Ok(())
    }

    #[test]
    fn existing_v1_store_without_filters_opens_and_reads_under_ribbon_options()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let activations = super::super::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
        drop(store);

        strip_point_lookup_filters(&store_path)?;

        let store = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(2)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut secondary = RocksDbCanonicalSecondary::open_ready(
            &store_path,
            temporary.path().join("secondary"),
            &activations,
            CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
            CanonicalReorgPolicy::new(2)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;

        assert_v1_point_lookups(&store, BlockHeight::new(3), [3; 32], 103)?;
        assert_v1_point_lookups(&secondary, BlockHeight::new(3), [3; 32], 103)?;
        assert_eq!(
            store.block_hash_lookup(BlockHash::from_bytes([0xee; 32]))?,
            BlockHashLookup::NotIndexed
        );
        assert_eq!(
            store.transaction_location(TransactionId::from_bytes([0xee; 32]))?,
            None
        );
        assert_eq!(
            secondary.block_hash_lookup(BlockHash::from_bytes([0xee; 32]))?,
            BlockHashLookup::NotIndexed
        );
        assert_eq!(
            secondary.transaction_location(TransactionId::from_bytes([0xee; 32]))?,
            None
        );

        let store = append_block_five_for_filter_test(store, &activations)?;
        for family_name in [
            BLOCK_HASH_INDEX_COLUMN_FAMILY,
            TRANSACTION_LOCATION_COLUMN_FAMILY,
        ] {
            let family = store
                .bounded_open
                .db
                .cf_handle(family_name)
                .ok_or("canonical family absent")?;
            store.bounded_open.db.flush_cf(&family)?;
            assert!(
                aggregated_table_property(&store.bounded_open, family_name, "filter block size")?
                    > Some(0),
                "{family_name}"
            );
        }
        assert_v1_point_lookups(&store, BlockHeight::new(5), [5; 32], 105)?;
        secondary.try_catch_up()?;
        assert_v1_point_lookups(&secondary, BlockHeight::new(3), [3; 32], 103)?;
        assert_v1_point_lookups(&secondary, BlockHeight::new(5), [5; 32], 105)?;
        Ok(())
    }

    #[test]
    fn existing_v1_store_without_epoch_or_event_filters_opens_and_flushes_bloom_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = TempDir::new()?;
        let store_path = temporary.path().join("canonical");
        let activations = super::super::test_network_upgrade_activations(Network::ZcashTestnet)?;
        let store = published_store(&store_path, 2, BlockHeight::new(4), BlockHeight::new(2))?;
        drop(store);

        strip_family_filters(
            &store_path,
            &[CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY],
        )?;

        let summary = RocksDbCanonicalStore::read_ready_summary(&store_path)?
            .ok_or("READY summary must exist for unfiltered store")?;
        assert_eq!(summary.visible_tip().height, BlockHeight::new(4));

        let store = RocksDbCanonicalStore::open_ready(
            &store_path,
            &activations,
            RawBlobRetention::All,
            CanonicalReorgPolicy::new(2)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        let mut secondary = RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            &store_path,
            temporary.path().join("secondary"),
            Network::ZcashTestnet,
            CanonicalStoreRequirements::for_blob_retention(RawBlobRetention::All),
            CanonicalReorgPolicy::new(2)?,
            RocksDbResourceBudget::for_local_tests(),
        )?;
        assert_eq!(secondary.event_fence(), store.event_fence());

        let store = append_block_five_for_filter_test(store, &activations)?;
        for family_name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
            let family = store
                .bounded_open
                .db
                .cf_handle(family_name)
                .ok_or("canonical family absent")?;
            store.bounded_open.db.flush_cf(&family)?;
            assert!(
                aggregated_table_property(&store.bounded_open, family_name, "filter block size")?
                    > Some(0),
                "{family_name}"
            );
        }
        secondary.try_catch_up()?;
        assert_eq!(secondary.event_fence(), store.event_fence());
        let post_flush_summary = RocksDbCanonicalStore::read_ready_summary(&store_path)?
            .ok_or("READY summary must exist across mixed filtered and unfiltered SSTs")?;
        assert_eq!(post_flush_summary.visible_tip().height, BlockHeight::new(5));
        for family_name in [CHAIN_EPOCH_COLUMN_FAMILY, CHAIN_EVENT_COLUMN_FAMILY] {
            let family = secondary
                .bounded_open
                .db
                .cf_handle(family_name)
                .ok_or("secondary family absent")?;
            for sequence in [1_u64, 2_u64] {
                assert!(
                    secondary
                        .bounded_open
                        .db
                        .get_cf(&family, sequence.to_be_bytes())?
                        .is_some(),
                    "{family_name} point lookup missed sequence {sequence}"
                );
            }
        }
        Ok(())
    }

    trait PointLookupReader {
        fn lookup_block_hash(
            &self,
            block_hash: BlockHash,
        ) -> Result<BlockHashLookup, CanonicalStoreError>;
        fn lookup_transaction_location(
            &self,
            transaction_id: TransactionId,
        ) -> Result<Option<TransactionLocation>, CanonicalStoreError>;
    }

    impl PointLookupReader for RocksDbCanonicalStore {
        fn lookup_block_hash(
            &self,
            block_hash: BlockHash,
        ) -> Result<BlockHashLookup, CanonicalStoreError> {
            self.block_hash_lookup(block_hash)
        }

        fn lookup_transaction_location(
            &self,
            transaction_id: TransactionId,
        ) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
            self.transaction_location(transaction_id)
        }
    }

    impl PointLookupReader for RocksDbCanonicalSecondary {
        fn lookup_block_hash(
            &self,
            block_hash: BlockHash,
        ) -> Result<BlockHashLookup, CanonicalStoreError> {
            self.block_hash_lookup(block_hash)
        }

        fn lookup_transaction_location(
            &self,
            transaction_id: TransactionId,
        ) -> Result<Option<TransactionLocation>, CanonicalStoreError> {
            self.transaction_location(transaction_id)
        }
    }

    fn assert_v1_point_lookups(
        reader: &impl PointLookupReader,
        height: BlockHeight,
        block_hash_bytes: [u8; 32],
        transaction_tag: u8,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let block_hash = BlockHash::from_bytes(block_hash_bytes);
        assert_eq!(
            reader.lookup_block_hash(block_hash)?,
            BlockHashLookup::Resolved(BlockId::new(height, block_hash))
        );
        let location = reader
            .lookup_transaction_location(TransactionId::from_bytes([transaction_tag; 32]))?
            .ok_or("transaction location missing")?;
        assert_eq!(location.block_height, height);
        assert_eq!(location.block_hash, block_hash);
        Ok(())
    }

    fn strip_point_lookup_filters(store_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        strip_family_filters(
            store_path,
            &[
                BLOCK_HASH_INDEX_COLUMN_FAMILY,
                TRANSACTION_LOCATION_COLUMN_FAMILY,
            ],
        )
    }

    fn strip_family_filters(
        store_path: &Path,
        family_names: &[&str],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let legacy_open = open_bounded_rocksdb(
            RocksDbOpenRole::ExistingPrimary { path: store_path },
            RocksDbResourceBudget::for_local_tests(),
            unfiltered_canonical_descriptors,
        )?;
        for &family_name in family_names {
            let handle = legacy_open
                .db
                .cf_handle(family_name)
                .ok_or("canonical family absent")?;
            let rows = legacy_open
                .db
                .iterator_cf(&handle, IteratorMode::Start)
                .collect::<Result<Vec<_>, _>>()?;
            for (key_bytes, row_bytes) in rows {
                legacy_open.db.put_cf(&handle, key_bytes, row_bytes)?;
            }
            legacy_open.db.flush_cf(&handle)?;
            legacy_open
                .db
                .compact_range_cf(&handle, None::<&[u8]>, None::<&[u8]>);
            assert_eq!(
                aggregated_table_property(&legacy_open, family_name, "filter block size")?,
                Some(0),
                "{family_name}"
            );
            assert_eq!(
                aggregated_table_property(&legacy_open, family_name, "# index partitions")?,
                None,
                "{family_name}"
            );
        }
        Ok(())
    }

    fn unfiltered_canonical_descriptors(
        block_cache: &Cache,
        resource_budget: RocksDbResourceBudget,
    ) -> Vec<ColumnFamilyDescriptor> {
        let hot = canonical_data_options(block_cache, resource_budget);
        let mut cold = hot.clone();
        cold.set_bottommost_compression_type(DBCompressionType::Zstd);
        cold.set_bottommost_compression_options(-14, 3, 0, 0, true);

        let mut uncompressed = hot.clone();
        uncompressed.set_compression_type(DBCompressionType::None);
        CANONICAL_DATA_COLUMN_FAMILIES
            .into_iter()
            .map(|name| {
                let cf_options = match name {
                    LIVE_TAIL_RAW_BLOCK_COLUMN_FAMILY
                    | TREE_STATE_CHECKPOINT_COLUMN_FAMILY
                    | DISPLACED_BLOCK_FACTS_COLUMN_FAMILY => cold.clone(),
                    BLOCK_HASH_INDEX_COLUMN_FAMILY | TRANSACTION_LOCATION_COLUMN_FAMILY => {
                        uncompressed.clone()
                    }
                    _ => hot.clone(),
                };
                ColumnFamilyDescriptor::new(name, cf_options)
            })
            .collect()
    }

    fn append_block_five_for_filter_test(
        store: RocksDbCanonicalStore,
        activations: &NetworkUpgradeActivations,
    ) -> Result<RocksDbCanonicalStore, Box<dyn std::error::Error>> {
        let fence = store.event_fence();
        let (store, _) = store.commit_live_append(
            CanonicalLiveAppend::new(
                fence,
                replacement_block(BlockHeight::new(5), [5; 32], [4; 32], 105)?,
                Vec::new(),
                BlockId::new(BlockHeight::new(3), BlockHash::from_bytes([3; 32])),
                UnixTimestampMillis::new(1_750_000_000_002),
            ),
            activations,
        )?;
        Ok(store)
    }

    fn assert_full_bloom_filter_table_properties(
        bounded_open: &BoundedRocksDbOpen,
        family_name: &str,
        minimum_entries: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let property = |field| aggregated_table_property(bounded_open, family_name, field);
        let entry_count = property("# entries")?.ok_or("missing # entries property")?;
        assert!(
            entry_count >= minimum_entries,
            "{family_name}: {entry_count}"
        );
        assert!(property("filter block size")? > Some(0), "{family_name}");
        assert_eq!(
            property("# entries for filter")?,
            Some(entry_count),
            "{family_name}"
        );
        assert_eq!(property("# index partitions")?, None, "{family_name}");
        Ok(())
    }

    fn assert_filter_table_properties(
        bounded_open: &BoundedRocksDbOpen,
        family_name: &str,
        minimum_entries: u64,
        filtered: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let property = |field| aggregated_table_property(bounded_open, family_name, field);
        let entry_count = property("# entries")?.ok_or("missing # entries property")?;
        assert!(
            entry_count >= minimum_entries,
            "{family_name}: {entry_count}"
        );
        if filtered {
            assert!(property("filter block size")? > Some(0), "{family_name}");
            assert_eq!(
                property("# entries for filter")?,
                Some(entry_count),
                "{family_name}"
            );
            assert!(property("# index partitions")? > Some(0), "{family_name}");
        } else {
            assert_eq!(property("filter block size")?, Some(0), "{family_name}");
            assert_eq!(property("# entries for filter")?, Some(0), "{family_name}");
            assert_eq!(property("# index partitions")?, None, "{family_name}");
        }
        Ok(())
    }

    fn assert_filter_skips_data_blocks(
        bounded_open: &BoundedRocksDbOpen,
        lookup: impl FnOnce() -> Result<bool, Box<dyn std::error::Error>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let statistics = &bounded_open.statistics;
        let useful_before = statistics.get_ticker_count(Ticker::BloomFilterUseful);
        let data_misses_before = statistics.get_ticker_count(Ticker::BlockCacheDataMiss);

        let absent = lookup()?;

        assert!(absent);
        assert!(statistics.get_ticker_count(Ticker::BloomFilterUseful) > useful_before);
        assert_eq!(
            statistics.get_ticker_count(Ticker::BlockCacheDataMiss),
            data_misses_before
        );
        Ok(())
    }

    fn aggregated_table_property(
        bounded_open: &BoundedRocksDbOpen,
        family_name: &str,
        field: &str,
    ) -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let family = bounded_open
            .db
            .cf_handle(family_name)
            .ok_or_else(|| format!("column family {family_name} is absent"))?;
        let properties = bounded_open
            .db
            .property_value_cf(&family, "rocksdb.aggregated-table-properties")?
            .ok_or("aggregated table properties are unavailable")?;
        for entry in properties.split("; ") {
            if let Some((name, number)) = entry.split_once('=')
                && name.trim() == field
            {
                return Ok(Some(number.trim().parse()?));
            }
        }
        Ok(None)
    }
}
