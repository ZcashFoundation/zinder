//! Process-local secondary access to an admitted canonical store.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use rust_rocksdb::{Cache, ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use zinder_core::{
    BlockHeight, BlockHeightRange, CanonicalBlockFacts, CanonicalHistoryBounds, Network,
    NetworkUpgradeActivations, NetworkUpgradeActivationsFingerprint,
};

use crate::{
    BoundedRocksDbOpen, RawBlobRetention, RocksDbInstanceMemory, RocksDbIoMode,
    RocksDbOpenResources, RocksDbOpenRole, RocksDbResourceBudget, StoreRole, open_bounded_rocksdb,
};

use super::{
    CanonicalEventFence, CanonicalEventHistoryRequest, CanonicalRetainedEvent,
    CanonicalSequenceCheckpoint, CanonicalStoreBuildState, CanonicalStoreError,
    CanonicalStoreReadyEvidence, CanonicalStoreRequirements, ConstructionTreeStateCheckpointPolicy,
    activation_schedule::CanonicalActivationSchedule,
    block_replay::{
        CanonicalReplayRangeScan, CanonicalReplayScan, read_replay_facts_at_with_cache,
        visit_replay_facts_at_heights_with_cache,
    },
    construction_manifest::validate_ready_construction_manifest,
    event_lifecycle::{
        canonical_event_history_from_db, canonical_event_retention_floor_from_db,
        read_retained_event_from_db,
    },
    live_commit::event_fence_from_ready,
    live_tail_cache::LiveTailShieldedCache,
    mempool_lifecycle::validate_mempool_lifecycle_admission,
    publication::{follow_ready_replay_tail, validate_ready_publication},
    replay_tail::AuthenticatedReplayTail,
    rocksdb::{
        CanonicalStoreAdmissionExpectation, StoredDataAdmission,
        canonical_column_family_descriptors, canonical_store_path, validate_open_store_control,
    },
    settled_segments::{
        cache::SettledSegmentReaderCache,
        layout::SettledSegmentIndexEntry,
        store_access::{self, SegmentStoreContext},
    },
    stored_set::{CANONICAL_DATA_COLUMN_FAMILIES, validate_exact_column_families},
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
    open_settings: Box<SecondaryOpenSettings>,
    pub(super) expectation: CanonicalStoreAdmissionExpectation,
    pub(super) build_plan: super::CanonicalStoreBuildPlan,
    history_predecessor_digest: [u8; 32],
    pub(super) ready_evidence: CanonicalStoreReadyEvidence,
    construction_tree_state_checkpoint_policy: Option<ConstructionTreeStateCheckpointPolicy>,
    pub(super) stored_set: super::CanonicalStoredSet,
    pub(super) cursor_auth_key: [u8; 32],
    pub(super) activation_schedule: Option<CanonicalActivationSchedule>,
    reorg_policy: super::CanonicalReorgPolicy,
    pub(super) replay_tail: AuthenticatedReplayTail,
    read_caches: Box<SecondaryReadCaches>,
    pub(super) sealed_segments: Vec<SettledSegmentIndexEntry>,
}

struct SecondaryOpenSettings {
    primary_path: PathBuf,
    secondary_path: PathBuf,
    resources: RocksDbOpenResources,
    column_families: fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor>,
    recovery_directory: Option<TempDir>,
}

struct SecondaryReadCaches {
    settled: Arc<SettledSegmentReaderCache>,
    live_tail: Option<LiveTailShieldedCache>,
}

impl RocksDbCanonicalSecondary {
    fn primary_path(&self) -> &Path {
        &self.open_settings.primary_path
    }

    /// Returns the writer-admitted schedule revision observed at the last
    /// catch-up.
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

    /// Opens a secondary with explicit minimum retained data and history.
    ///
    /// The node-observed activation table must agree with the writer-admitted
    /// schedule through the visible tip.
    #[allow(
        clippy::too_many_arguments,
        reason = "secondary admission keeps filesystem, schedule, and reorg policy explicit"
    )]
    pub fn open_ready(
        primary_path: impl AsRef<Path>,
        secondary_path: impl AsRef<Path>,
        expected_network_upgrade_activations: &NetworkUpgradeActivations,
        requirements: CanonicalStoreRequirements,
        expected_reorg_policy: super::CanonicalReorgPolicy,
        resources: impl Into<RocksDbOpenResources>,
    ) -> Result<Self, CanonicalStoreError> {
        Self::open_ready_with_expectation(
            primary_path.as_ref(),
            secondary_path.as_ref(),
            &CanonicalStoreAdmissionExpectation::from_activations(
                expected_network_upgrade_activations,
                StoredDataAdmission::Minimum(requirements),
                expected_reorg_policy,
            ),
            resources.into(),
            canonical_column_family_descriptors,
        )
    }

    /// Opens a secondary with explicit minimum retained data and history.
    ///
    /// The reader adopts the canonical writer's admitted activation schedule
    /// and follows later revisions on catch-up.
    #[allow(
        clippy::too_many_arguments,
        reason = "secondary admission keeps filesystem, schedule, and reorg policy explicit"
    )]
    pub fn open_ready_adopting_writer_schedule(
        primary_path: impl AsRef<Path>,
        secondary_path: impl AsRef<Path>,
        expected_network: Network,
        requirements: CanonicalStoreRequirements,
        expected_reorg_policy: super::CanonicalReorgPolicy,
        resources: impl Into<RocksDbOpenResources>,
    ) -> Result<Self, CanonicalStoreError> {
        Self::open_ready_with_expectation(
            primary_path.as_ref(),
            secondary_path.as_ref(),
            &CanonicalStoreAdmissionExpectation::adopting_writer_schedule(
                expected_network,
                StoredDataAdmission::Minimum(requirements),
                expected_reorg_policy,
            ),
            resources.into(),
            canonical_column_family_descriptors,
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "initial secondary admission binds control, schedule, manifest, and READY publication"
    )]
    pub(super) fn open_ready_with_expectation(
        primary_path: &Path,
        secondary_path: &Path,
        expectation: &CanonicalStoreAdmissionExpectation,
        resources: RocksDbOpenResources,
        column_families: fn(&Cache, RocksDbResourceBudget) -> Vec<ColumnFamilyDescriptor>,
    ) -> Result<Self, CanonicalStoreError> {
        resources
            .validate()
            .map_err(|reason| CanonicalStoreError::InvalidResourceBudget { reason })?;
        let primary_path = canonical_store_path(primary_path)?;
        if secondary_path == primary_path {
            return Err(CanonicalStoreError::admission(
                &primary_path,
                "secondary metadata path must differ from the canonical primary path",
            ));
        }
        require_exact_primary_column_family_metadata(&primary_path)?;
        let bounded_open = open_bounded_rocksdb(
            RocksDbOpenRole::Secondary {
                primary_path: &primary_path,
                secondary_path,
            },
            resources.clone(),
            column_families,
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
        validate_exact_column_families(&primary_path, &mut bounded_open.db.cf_names())?;
        let opened_control =
            validate_open_store_control(&bounded_open.db, &primary_path, expectation)?;
        validate_mempool_lifecycle_admission(
            &bounded_open.db,
            opened_control.network,
            opened_control.cursor_auth_key,
        )?;
        let CanonicalStoreBuildState::Ready(ready_evidence) = opened_control.build_state else {
            return Err(CanonicalStoreError::StoreNotReady { path: primary_path });
        };
        let manifest_details = validate_ready_construction_manifest(
            &primary_path,
            &ready_evidence,
            &opened_control.build_plan,
        )?;
        let stored_set = opened_control
            .build_plan
            .admitted_stored_set(manifest_details.subtree_root_coverage);
        expectation.validate_reader_requirements(&primary_path, stored_set)?;
        let reorg_policy = opened_control.effective_reorg_policy();
        let sealed_segments = store_access::read_all_settled_segment_entries(&bounded_open.db)?;
        let expected_modules = store_access::segment_modules_for_stored_set(stored_set);
        for entry in &sealed_segments {
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
        let segment_cache = SettledSegmentReaderCache::with_default_capacity(&primary_path, false)?;
        let ctx = SegmentStoreContext {
            db: &bounded_open.db,
            segment_cache: Some(&segment_cache),
            sealed_segments: Some(&sealed_segments),
        };
        let replay_tail = validate_ready_publication(
            ctx,
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
            open_settings: Box::new(SecondaryOpenSettings {
                primary_path,
                secondary_path: secondary_path.to_path_buf(),
                resources,
                column_families,
                recovery_directory: None,
            }),
            // An opening table is checked only at open. Catch-ups
            // authenticate the schedule journal and follow each revision the
            // writer admits, so a long-running reader never pins its startup
            // observation. The reorg window stays exact: a writer raise
            // after open fails catch-up until the reader restarts with it.
            expectation: CanonicalStoreAdmissionExpectation::from_build_plan(
                &opened_control.build_plan,
                reorg_policy,
            ),
            history_predecessor_digest: opened_control.build_plan.history_predecessor_digest(),
            build_plan: opened_control.build_plan,
            ready_evidence,
            construction_tree_state_checkpoint_policy: manifest_details
                .tree_state_checkpoint_policy,
            stored_set,
            cursor_auth_key: opened_control.cursor_auth_key,
            activation_schedule,
            reorg_policy,
            replay_tail,
            read_caches: Box::new(SecondaryReadCaches {
                settled: segment_cache,
                live_tail: Some(live_tail_cache_for(&ready_evidence)),
            }),
            sealed_segments,
        })
    }

    /// Reopens a live follower after an admitted replay height was missing.
    ///
    /// The fresh handle must admit READY for the same store and retain the
    /// previous fence exactly, either as READY or in its event history.
    /// After verifying any external writer attestation, the caller can retry
    /// its original replay operation once. Any further error ends that operation.
    pub fn reopen_after_missing_live_tail(
        &mut self,
        height: BlockHeight,
    ) -> Result<(), CanonicalStoreError> {
        let observed_ready = self.ready_evidence;
        *self = self.open_live_tail_recovery(height, &observed_ready)?;
        Ok(())
    }

    fn open_live_tail_recovery(
        &mut self,
        height: BlockHeight,
        observed_ready: &CanonicalStoreReadyEvidence,
    ) -> Result<Self, CanonicalStoreError> {
        self.read_caches.live_tail = None;
        let before = self.event_fence();
        if height < observed_ready.first_retained_block.height
            || height > observed_ready.visible_tip.height
        {
            return Err(CanonicalStoreError::MissingLiveTailReplay { height });
        }
        tracing::warn!(
            target: "zinder::store",
            event = "canonical_secondary_live_tail_reopen",
            missing_height = height.value(),
            visible_height = before.visible_tip().height.value(),
            replay_tip_height = observed_ready.visible_tip.height.value(),
            event_sequence = before.chain_event_sequence(),
            "reopening canonical secondary after an admitted live-tail replay row was missing"
        );
        metrics::counter!("zinder_store_canonical_secondary_live_tail_reopens_total").increment(1);

        let directory = tempfile::Builder::new()
            .prefix("live-tail-recovery-")
            .tempdir_in(&self.open_settings.secondary_path)
            .map_err(|source| CanonicalStoreError::PathUnavailable {
                path: self.open_settings.secondary_path.clone(),
                source,
            })?;
        let mut fresh = Self::open_ready_with_expectation(
            self.primary_path(),
            directory.path(),
            &self.expectation,
            self.open_settings.resources.clone(),
            self.open_settings.column_families,
        )?;
        self.validate_reopened_fence(&fresh, before)?;
        if fresh.ready_evidence.visible_event_sequence <= observed_ready.visible_event_sequence
            && fresh.ready_evidence != *observed_ready
        {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "reopened secondary did not reach the READY fence observed during replay",
            ));
        }
        fresh
            .open_settings
            .secondary_path
            .clone_from(&self.open_settings.secondary_path);
        fresh.open_settings.recovery_directory = Some(directory);
        Ok(fresh)
    }

    fn validate_reopened_fence(
        &self,
        fresh: &Self,
        before: CanonicalEventFence,
    ) -> Result<(), CanonicalStoreError> {
        if fresh.build_plan != self.build_plan
            || fresh.cursor_auth_key != self.cursor_auth_key
            || fresh.ready_evidence.construction_manifest_version
                != self.ready_evidence.construction_manifest_version
            || fresh.ready_evidence.construction_manifest_sha256
                != self.ready_evidence.construction_manifest_sha256
        {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "canonical store identity changed during secondary reopen",
            ));
        }
        let after = fresh.event_fence();
        let continuous = if after.chain_event_sequence() > before.chain_event_sequence()
            && after.chain_epoch_id().value() > before.chain_epoch_id().value()
        {
            read_retained_event_from_db(&fresh.bounded_open.db, before.chain_event_sequence())?
                .resulting_fence()
                == before
        } else {
            fresh.ready_evidence == self.ready_evidence
        };
        if !continuous {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "canonical fence regressed or diverged during secondary reopen",
            ));
        }
        Ok(())
    }

    /// Publishes this secondary's `RocksDB` cache, ticker, and per-family
    /// gauges under the `canonical_secondary` store role.
    ///
    /// A process that hydrates history through one secondary calls this
    /// periodically, so its block-cache hit rate and device reads are
    /// observable beside the walk they serve.
    pub fn record_resource_gauges(&self) {
        self.bounded_open.record_resource_gauges(
            StoreRole::CanonicalSecondary,
            &CANONICAL_DATA_COLUMN_FAMILIES,
        );
    }

    /// Publishes cache composition through one serving generation.
    pub fn record_block_cache_entry_gauges(&self) {
        self.bounded_open
            .record_block_cache_entry_gauges(StoreRole::CanonicalSecondary);
    }

    /// Publishes per-column-family integer properties and level file counts
    /// for one serving generation under the `canonical_secondary` store role.
    pub fn record_column_family_property_gauges(&self) {
        self.bounded_open.record_column_family_property_gauges(
            StoreRole::CanonicalSecondary,
            &CANONICAL_DATA_COLUMN_FAMILIES,
        );
    }

    /// Returns this secondary's cumulative ticker, or zero with statistics off.
    #[must_use]
    pub fn rocksdb_ticker_count(&self, ticker: rust_rocksdb::statistics::Ticker) -> u64 {
        self.bounded_open.statistics.get_ticker_count(ticker)
    }

    /// Snapshots a cumulative histogram on this secondary. Timer histograms
    /// collect observations when the resource budget selects full statistics.
    #[must_use]
    pub fn rocksdb_histogram_data(
        &self,
        histogram: rust_rocksdb::statistics::Histogram,
    ) -> rust_rocksdb::statistics::HistogramData {
        self.bounded_open.statistics.get_histogram_data(histogram)
    }

    /// Returns the admitted column families, including the default family.
    #[must_use]
    pub fn rocksdb_column_family_names(&self) -> Vec<String> {
        self.bounded_open.db.cf_names()
    }

    /// Reads one engine property at this secondary's view of a column family.
    /// An unsupported property returns `None`.
    pub fn rocksdb_property_value_cf(
        &self,
        family: &str,
        property: &str,
    ) -> Result<Option<String>, CanonicalStoreError> {
        let db = &self.bounded_open.db;
        let cf = db.cf_handle(family).ok_or_else(|| {
            CanonicalStoreError::admission(
                self.primary_path(),
                format!("unknown column family {family}"),
            )
        })?;
        db.property_value_cf(&cf, property).map_err(|source| {
            CanonicalStoreError::RocksDbOperation {
                operation: "read canonical secondary column family property",
                source,
            }
        })
    }

    /// Lists the live SST metadata at the secondary's current view.
    pub fn rocksdb_live_files(&self) -> Result<Vec<rust_rocksdb::LiveFile>, CanonicalStoreError> {
        self.bounded_open
            .db
            .live_files()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "list canonical secondary live SSTs",
                source,
            })
    }

    /// Samples the memory this secondary holds outside its block cache.
    #[must_use]
    pub fn instance_memory(&self) -> RocksDbInstanceMemory {
        self.bounded_open.instance_memory()
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
        self.catch_up(false)
    }

    /// Catches up an active writer follower, reopening once if a live replay
    /// row needed to authenticate the newly visible READY fence is absent.
    ///
    /// The reopen must independently admit READY at or past the observed
    /// fence and preserve continuity with the last admitted fence.
    pub fn try_catch_up_following(
        &mut self,
    ) -> Result<CanonicalSecondaryCatchupOutcome, CanonicalStoreError> {
        self.catch_up(true)
    }

    fn catch_up(
        &mut self,
        recover_missing_live_tail: bool,
    ) -> Result<CanonicalSecondaryCatchupOutcome, CanonicalStoreError> {
        // RocksDB can advance before admission fails. Restore cached rows
        // only when this handle successfully admits the same READY evidence.
        let previous_cache = self.read_caches.live_tail.take();
        let previous_ready = self.ready_evidence;
        let outcome = self.catch_up_inner(recover_missing_live_tail);
        // A successful reopen already installed its own empty cache.
        if outcome.is_ok() && self.read_caches.live_tail.is_none() {
            let cache = match previous_cache {
                Some(cache) if previous_ready == self.ready_evidence => cache,
                _ => live_tail_cache_for(&self.ready_evidence),
            };
            self.read_caches.live_tail = Some(cache);
        }
        outcome
    }

    fn catch_up_inner(
        &mut self,
        recover_missing_live_tail: bool,
    ) -> Result<CanonicalSecondaryCatchupOutcome, CanonicalStoreError> {
        let before = self.event_fence();
        self.bounded_open
            .db
            .try_catch_up_with_primary()
            .map_err(|source| CanonicalStoreError::RocksDbOperation {
                operation: "canonical secondary catch-up",
                source,
            })?;
        validate_exact_column_families(self.primary_path(), &mut self.bounded_open.db.cf_names())?;
        let opened_control = validate_open_store_control(
            &self.bounded_open.db,
            self.primary_path(),
            &self.expectation,
        )?;
        validate_mempool_lifecycle_admission(
            &self.bounded_open.db,
            opened_control.network,
            opened_control.cursor_auth_key,
        )?;
        let CanonicalStoreBuildState::Ready(ready_evidence) = opened_control.build_state else {
            return Err(CanonicalStoreError::StoreNotReady {
                path: self.primary_path().to_path_buf(),
            });
        };
        let ready_changed = ready_evidence != self.ready_evidence;
        self.validate_catch_up_identity(&opened_control, &ready_evidence, ready_changed)?;
        if ready_evidence.visible_event_sequence < self.ready_evidence.visible_event_sequence {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "canonical event sequence regressed during secondary catch-up",
            ));
        }
        if ready_evidence.visible_event_sequence == self.ready_evidence.visible_event_sequence
            && ready_changed
        {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "canonical READY publication changed without advancing its event sequence",
            ));
        }
        let updated_sealed_segments = self.sync_settled_segments_on_catch_up(&ready_evidence)?;
        // Unchanged READY evidence is still explained by the tail this reader
        // already authenticated; otherwise re-derive only what changed.
        if ready_changed
            && let Err(error) =
                self.authenticate_catch_up_tail(&ready_evidence, updated_sealed_segments.as_deref())
        {
            if recover_missing_live_tail
                && let CanonicalStoreError::MissingLiveTailReplay { height } = error
            {
                self.reopen_during_catch_up(height, &ready_evidence)?;
                return Ok(CanonicalSecondaryCatchupOutcome {
                    before,
                    after: self.event_fence(),
                });
            }
            return Err(error);
        }
        self.ready_evidence = ready_evidence;
        if let Some(entries) = updated_sealed_segments {
            self.sealed_segments = entries;
        }
        self.activation_schedule = opened_control
            .activation_schedule_binding
            .map(|binding| {
                CanonicalActivationSchedule::read_bound(
                    &self.bounded_open.db,
                    self.primary_path(),
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

    fn authenticate_catch_up_tail(
        &mut self,
        ready_evidence: &CanonicalStoreReadyEvidence,
        updated_sealed_segments: Option<&[SettledSegmentIndexEntry]>,
    ) -> Result<(), CanonicalStoreError> {
        let ctx = SegmentStoreContext {
            db: &self.bounded_open.db,
            segment_cache: Some(&self.read_caches.settled),
            sealed_segments: Some(updated_sealed_segments.unwrap_or(&self.sealed_segments)),
        };
        follow_ready_replay_tail(
            ctx,
            self.reorg_policy,
            &self.ready_evidence,
            ready_evidence,
            &mut self.replay_tail,
        )
    }

    fn reopen_during_catch_up(
        &mut self,
        height: BlockHeight,
        ready_evidence: &CanonicalStoreReadyEvidence,
    ) -> Result<(), CanonicalStoreError> {
        *self = self
            .open_live_tail_recovery(height, ready_evidence)
            .map_err(|error| {
                // Followers retry missing SSTs on ordinary catch-up. A failed
                // recovery open must end this attempt.
                if error.is_missing_table_file() {
                    CanonicalStoreError::admission(self.primary_path(), error.to_string())
                } else {
                    error
                }
            })?;
        Ok(())
    }

    fn sync_settled_segments_on_catch_up(
        &self,
        ready_evidence: &CanonicalStoreReadyEvidence,
    ) -> Result<Option<Vec<SettledSegmentIndexEntry>>, CanonicalStoreError> {
        let entries = store_access::read_all_settled_segment_entries(&self.bounded_open.db)?;
        if let (Some(first), Some(last)) = (entries.first(), entries.last()) {
            if first.start_height != ready_evidence.first_retained_block.height {
                return Err(CanonicalStoreError::admission(
                    self.primary_path(),
                    format!(
                        "settled segment start {} disagrees with READY first retained height {}",
                        first.start_height.value(),
                        ready_evidence.first_retained_block.height.value(),
                    ),
                ));
            }
            if last.end_block.height > ready_evidence.sequence_checkpoint.through().height {
                return Err(CanonicalStoreError::admission(
                    self.primary_path(),
                    format!(
                        "settled segment tip {} exceeds READY settled checkpoint {}",
                        last.end_block.height.value(),
                        ready_evidence.sequence_checkpoint.through().height.value(),
                    ),
                ));
            }
        }
        if let Some(previous) = self.sealed_segments.last() {
            let Some(current) = entries.last() else {
                return Err(CanonicalStoreError::admission(
                    self.primary_path(),
                    "settled segment index disappeared during secondary catch-up",
                ));
            };
            if current.end_block.height < previous.end_block.height {
                return Err(CanonicalStoreError::admission(
                    self.primary_path(),
                    "settled segment tip regressed during secondary catch-up",
                ));
            }
            if current.end_block.height == previous.end_block.height
                && (current.end_block.hash != previous.end_block.hash || current != previous)
            {
                return Err(CanonicalStoreError::admission(
                    self.primary_path(),
                    "settled segment tip changed at equal height during secondary catch-up",
                ));
            }
        }
        if entries.len() < self.sealed_segments.len() || !entries.starts_with(&self.sealed_segments)
        {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "retained settled segment prefix mutated during secondary catch-up",
            ));
        }
        if entries.len() == self.sealed_segments.len() {
            return Ok(None);
        }
        let expected_modules = store_access::segment_modules_for_stored_set(self.stored_set);
        for entry in &entries[self.sealed_segments.len()..] {
            if entry.modules != expected_modules {
                return Err(CanonicalStoreError::admission(
                    self.primary_path(),
                    format!(
                        "newly settled segment {}-{} modules {:?} do not match admitted modules {:?}",
                        entry.start_height.value(),
                        entry.end_block.height.value(),
                        entry.modules,
                        expected_modules
                    ),
                ));
            }
            store_access::open_replay_segment_readers(
                &self.bounded_open.db,
                Some(&self.read_caches.settled),
                entry,
            )
            .map_err(|err| CanonicalStoreError::admission(self.primary_path(), err.to_string()))?;
        }
        Ok(Some(entries))
    }

    /// Returns the immutable canonical network.
    #[must_use]
    pub const fn network(&self) -> Network {
        self.build_plan.network()
    }

    fn validate_catch_up_identity(
        &self,
        opened_control: &super::control::DecodedStoreControl,
        ready_evidence: &CanonicalStoreReadyEvidence,
        ready_changed: bool,
    ) -> Result<(), CanonicalStoreError> {
        if opened_control.build_plan != self.build_plan {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "canonical build identity changed during secondary catch-up",
            ));
        }
        if ready_changed
            && (ready_evidence.construction_manifest_version
                != self.ready_evidence.construction_manifest_version
                || ready_evidence.construction_manifest_sha256
                    != self.ready_evidence.construction_manifest_sha256)
        {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "canonical construction manifest changed during secondary catch-up",
            ));
        }
        if opened_control.cursor_auth_key != self.cursor_auth_key {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                "canonical cursor authentication key changed during secondary catch-up",
            ));
        }
        Ok(())
    }

    pub(super) fn live_tail_cache(&self) -> Option<&LiveTailShieldedCache> {
        self.read_caches.live_tail.as_ref()
    }

    pub(super) fn segment_context(&self) -> SegmentStoreContext<'_> {
        SegmentStoreContext {
            db: &self.bounded_open.db,
            segment_cache: Some(&self.read_caches.settled),
            sealed_segments: Some(&self.sealed_segments),
        }
    }

    /// Returns the admitted network-upgrade activation identity.
    #[must_use]
    pub const fn network_upgrade_activations_fingerprint(
        &self,
    ) -> NetworkUpgradeActivationsFingerprint {
        self.build_plan.network_upgrade_activations_fingerprint()
    }

    /// Returns the effective maximum canonical replacement depth admitted at
    /// open: the construction policy or a writer-admitted raise.
    #[must_use]
    pub const fn reorg_policy(&self) -> super::CanonicalReorgPolicy {
        self.reorg_policy
    }

    /// Returns the retained block data and historical floor.
    #[must_use]
    pub const fn stored_set(&self) -> super::CanonicalStoredSet {
        self.stored_set
    }

    /// Requires a replacement to hold the same data as the serving reader.
    ///
    /// # Errors
    /// Returns an admission refusal when the stored sets differ.
    pub fn require_stored_set(
        &self,
        expected: super::CanonicalStoredSet,
    ) -> Result<(), CanonicalStoreError> {
        if self.stored_set != expected {
            return Err(CanonicalStoreError::admission(
                self.primary_path(),
                format!(
                    "canonical stored set changed from {expected:?} to {:?}",
                    self.stored_set
                ),
            ));
        }
        Ok(())
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

    /// Returns the digest of the complete predecessor checkpoint admitted at open.
    #[must_use]
    pub const fn history_predecessor_digest(&self) -> [u8; 32] {
        self.history_predecessor_digest
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

    /// Returns the policy recorded in the authenticated construction manifest.
    ///
    /// A legacy manifest without this record returns `None`.
    #[must_use]
    pub const fn construction_tree_state_checkpoint_policy(
        &self,
    ) -> Option<ConstructionTreeStateCheckpointPolicy> {
        self.construction_tree_state_checkpoint_policy
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
        CanonicalReplayScan::new(self.segment_context(), &self.ready_evidence)
    }

    /// Returns a `Sync` slice source for concurrent height-bounded replay scans
    /// against this secondary's admitted READY fence.
    #[must_use]
    pub fn canonical_replay_slice_source(&self) -> super::CanonicalReplaySliceSource<'_> {
        super::CanonicalReplaySliceSource::new(self.segment_context(), &self.ready_evidence)
    }

    /// Reads one bounded connected replay range from this admitted fence.
    ///
    /// This is the incremental-projector path. It authenticates each decoded
    /// row against its canonical predecessor without rescanning the historical
    /// prefix; the caller must compare its resulting source digest and event
    /// cursor with the same secondary fence before publishing derived state.
    /// A missing live-tail row can fail scan creation or iteration with
    /// [`CanonicalStoreError::MissingLiveTailReplay`].
    pub fn scan_canonical_replay_range(
        &self,
        range: BlockHeightRange,
    ) -> Result<CanonicalReplayRangeScan<'_>, CanonicalStoreError> {
        CanonicalReplayRangeScan::new(self.segment_context(), &self.ready_evidence, range)
    }

    /// Reads one retained block's canonical facts by height.
    ///
    /// Heights outside the admitted retained range return `None`. A missing row
    /// inside that range is an error because READY admission proves the
    /// retained range is contiguous.
    pub fn block_replay_facts_at(
        &self,
        height: BlockHeight,
    ) -> Result<Option<CanonicalBlockFacts>, CanonicalStoreError> {
        read_replay_facts_at_with_cache(self.segment_context(), &self.ready_evidence, height)
    }

    /// Visits several retained blocks' canonical facts in height order.
    ///
    /// `visit` receives each requested height's position in `heights` with
    /// the facts [`Self::block_replay_facts_at`] would return for it. The
    /// rows are read in small batched lookups that bypass the block cache, so
    /// a walk that reads many unrelated historical blocks holds only a few
    /// rows at once and keeps the cache for the index and transaction-location
    /// blocks it reuses.
    pub fn visit_block_replay_facts_at_heights<E>(
        &self,
        heights: &[BlockHeight],
        visit: impl FnMut(usize, Option<CanonicalBlockFacts>) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<CanonicalStoreError>,
    {
        visit_replay_facts_at_heights_with_cache(
            self.segment_context(),
            &self.ready_evidence,
            heights,
            visit,
        )
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

fn live_tail_cache_for(evidence: &CanonicalStoreReadyEvidence) -> LiveTailShieldedCache {
    LiveTailShieldedCache::new(
        evidence.sequence_checkpoint.through().height,
        evidence.visible_tip.height,
    )
}

fn require_exact_primary_column_family_metadata(
    primary_path: &Path,
) -> Result<(), CanonicalStoreError> {
    let mut observed = DB::list_cf(&Options::default(), primary_path).map_err(|source| {
        CanonicalStoreError::admission(
            primary_path,
            format!("canonical column-family metadata discovery failed: {source}"),
        )
    })?;
    validate_exact_column_families(primary_path, &mut observed)
}
