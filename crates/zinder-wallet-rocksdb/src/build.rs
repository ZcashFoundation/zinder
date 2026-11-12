//! Fresh wallet construction from one authenticated canonical replay scan.

use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use zinder_core::{
    BlockId, CanonicalBlockFactsSequenceDigest, CanonicalBlockFactsSequenceDigestVersion, Network,
    UnixTimestampMillis,
};
use zinder_rocksdb_bulk_load::VariableValueSortEvidence;
use zinder_store::{
    CanonicalReplayScan, CanonicalStoreError, CanonicalStoreReadyEvidence,
    RocksDbCanonicalSecondary, RocksDbCanonicalStore, RocksDbResourceBudget,
};
use zinder_wallet_projection::{
    WalletCanonicalSourceIdentity, WalletProjectionAccumulator, WalletProjectionBuildLease,
    WalletProjectionBuildLeaseRequest, WalletProjectionBuildOwner, WalletProjectionDigest,
    WalletProjectionFamilyRowCounts, WalletProjectionReadyEvidence,
    WalletProjectionRetainedEventAnchor, WalletProjectionSourcePosition, WalletUtxoSetSummary,
};

use crate::{
    RocksDbWalletBuildStore, RocksDbWalletError, RocksDbWalletStore,
    projection_load::{
        PreparedWalletProjectionLoad, WalletProjectionLoadConfig, write_projection_ssts,
    },
    store::{RocksDbWalletBuilder, WalletColdValidationConfig},
};

const DEFAULT_BUILD_LEASE_DURATION_MILLIS: u64 = 60 * 60 * 1000;

/// Read-only canonical source sufficient for one fixed-tip wallet projection build.
///
/// Implementations expose only authenticated canonical replay, so projection
/// construction can use a process-local canonical secondary without access to
/// the canonical primary's mutation surface.
pub trait WalletProjectionReplaySource {
    /// Returns the immutable network admitted by this replay source.
    fn wallet_projection_network(&self) -> zinder_core::Network;

    /// Returns the authenticated canonical fence pinned before replay begins.
    fn wallet_projection_ready_evidence(&self) -> CanonicalStoreReadyEvidence;

    /// Opens the canonical replay scan used for the fixed-tip projection build.
    fn wallet_projection_scan(&self) -> Result<CanonicalReplayScan<'_>, CanonicalStoreError>;
}

/// Observable boundary at which a long-running fixed-tip build may renew its lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalletBuildLeasePhase {
    /// The empty BUILDING store and initial lease are durable.
    Initialized,
    /// The authenticated canonical replay scan and staged rows are complete.
    CanonicalReplayComplete,
    /// Generated projection SSTs are durably ingested into the BUILDING store.
    ProjectionRowsLoaded,
    /// Independent cold validation has bound publishable READY evidence.
    ColdValidationComplete,
    /// Last caller hook before the lease-guarded READY promotion.
    ///
    /// Callers that own the canonical writer should use
    /// [`validate_wallet_projection_pre_promotion_fence`] from this phase to
    /// reject a build whose pinned writer fence has advanced. The build also
    /// repeats that check after the heartbeat is durably applied.
    BeforePromotion,
}

/// Caller-observed clock and optional extension for a build-lease heartbeat.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletBuildLeaseHeartbeat {
    now: UnixTimestampMillis,
    renew_until: Option<UnixTimestampMillis>,
}

/// Explicit lease input for one fixed-tip build invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalletProjectionBuildLeaseExecution {
    lease_request: WalletProjectionBuildLeaseRequest,
    initial_now: UnixTimestampMillis,
}

impl WalletProjectionBuildLeaseExecution {
    /// Binds the lease request and caller-observed initial clock to one build.
    #[must_use]
    pub const fn new(
        lease_request: WalletProjectionBuildLeaseRequest,
        initial_now: UnixTimestampMillis,
    ) -> Self {
        Self {
            lease_request,
            initial_now,
        }
    }

    /// Returns the initial durable ownership request.
    #[must_use]
    pub const fn lease_request(self) -> WalletProjectionBuildLeaseRequest {
        self.lease_request
    }

    /// Returns the caller-observed time used for initial acquisition.
    #[must_use]
    pub const fn initial_now(self) -> UnixTimestampMillis {
        self.initial_now
    }
}

impl WalletBuildLeaseHeartbeat {
    /// Records the caller's current time without changing the durable expiry.
    #[must_use]
    pub const fn at(now: UnixTimestampMillis) -> Self {
        Self {
            now,
            renew_until: None,
        }
    }

    /// Records the caller's time and an explicit new expiry to persist atomically.
    #[must_use]
    pub const fn renew(now: UnixTimestampMillis, renew_until: UnixTimestampMillis) -> Self {
        Self {
            now,
            renew_until: Some(renew_until),
        }
    }

    /// Returns the caller-observed current time.
    #[must_use]
    pub const fn now(self) -> UnixTimestampMillis {
        self.now
    }

    /// Returns the optional requested durable expiry extension.
    #[must_use]
    pub const fn renew_until(self) -> Option<UnixTimestampMillis> {
        self.renew_until
    }
}

fn default_build_lease_heartbeat(
    now: UnixTimestampMillis,
    current_expires_at: UnixTimestampMillis,
) -> WalletBuildLeaseHeartbeat {
    let desired_expires_at = now
        .value()
        .saturating_add(DEFAULT_BUILD_LEASE_DURATION_MILLIS);
    let renew_until = UnixTimestampMillis::new(
        desired_expires_at.max(current_expires_at.value().saturating_add(1)),
    );
    if renew_until <= current_expires_at {
        WalletBuildLeaseHeartbeat::at(now)
    } else {
        WalletBuildLeaseHeartbeat::renew(now, renew_until)
    }
}

impl WalletProjectionReplaySource for RocksDbCanonicalStore {
    fn wallet_projection_network(&self) -> zinder_core::Network {
        self.network()
    }

    fn wallet_projection_ready_evidence(&self) -> CanonicalStoreReadyEvidence {
        self.ready_evidence()
    }

    fn wallet_projection_scan(&self) -> Result<CanonicalReplayScan<'_>, CanonicalStoreError> {
        self.scan_canonical_replay()
    }
}

impl WalletProjectionReplaySource for RocksDbCanonicalSecondary {
    fn wallet_projection_network(&self) -> zinder_core::Network {
        self.network()
    }

    fn wallet_projection_ready_evidence(&self) -> CanonicalStoreReadyEvidence {
        self.ready_evidence()
    }

    fn wallet_projection_scan(&self) -> Result<CanonicalReplayScan<'_>, CanonicalStoreError> {
        self.scan_canonical_replay()
    }
}

/// Verifies that a canonical source still matches a build lease immediately before READY.
///
/// Invoke this from the [`WalletBuildLeasePhase::BeforePromotion`] heartbeat
/// when the caller can query the canonical writer's current fence. A mismatch
/// rejects publication before the wallet control record can become READY.
pub fn validate_wallet_projection_pre_promotion_fence<Source>(
    canonical_store: &Source,
    lease: WalletProjectionBuildLease,
) -> Result<(), RocksDbWalletError>
where
    Source: WalletProjectionReplaySource + ?Sized,
{
    if canonical_store.wallet_projection_network() != lease.network() {
        return Err(RocksDbWalletError::NetworkMismatch {
            expected: lease.network(),
            observed: canonical_store.wallet_projection_network(),
        });
    }
    let observed = canonical_source_identity(&canonical_store.wallet_projection_ready_evidence());
    if observed != lease.pinned_canonical_anchor() {
        return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
            reason: "canonical source changed before wallet READY promotion",
        });
    }
    Ok(())
}

/// Explicit resource limits for one production external wallet build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RocksDbWalletBuildOptions {
    /// Bounded resources applied to the wallet `RocksDB` instance.
    pub resource_budget: RocksDbResourceBudget,
    /// Accounted memory ceiling for the outpoint event sorter.
    pub max_outpoint_sort_memory_bytes: u64,
    /// Accounted memory ceiling applied independently to each secondary sorter.
    pub max_secondary_sort_memory_bytes_per_sorter: u64,
    /// Temporary-run byte ceiling applied independently to each external sorter.
    pub max_temporary_file_bytes_per_sorter: u64,
    /// Target logical key-and-value bytes per generated SST file.
    pub sst_target_logical_bytes: u64,
    /// Accounted memory ceiling for the retained reorg-undo suffix.
    pub max_accounted_reorg_undo_bytes: u64,
    /// Number of exact tip undo records retained for reorg handling.
    pub supported_reorg_depth: u32,
}

impl RocksDbWalletBuildOptions {
    /// Small deterministic limits for unit and deployment-smoke fixtures.
    #[must_use]
    pub const fn for_local_tests() -> Self {
        Self {
            resource_budget: RocksDbResourceBudget::for_local_tests(),
            max_outpoint_sort_memory_bytes: 16 * 1024 * 1024,
            max_secondary_sort_memory_bytes_per_sorter: 16 * 1024 * 1024,
            max_temporary_file_bytes_per_sorter: 256 * 1024 * 1024,
            sst_target_logical_bytes: 1024 * 1024,
            max_accounted_reorg_undo_bytes: 16 * 1024 * 1024,
            supported_reorg_depth: 100,
        }
    }
}

/// Wall-clock duration of each family scan inside cold validation.
///
/// Cold validation reads every durable row once, in stored key order. The
/// primary output, address-index, and address-transaction scans run
/// concurrently, so these durations overlap: each is the wall time of its own
/// scan and none exceeds [`WalletBuildPhaseDurations::cold_validation`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalletColdValidationPhaseDurations {
    /// Merged scan of the unspent and spent primary output families.
    pub primary_output_scan: Duration,
    /// Scan of the address-unspent index and address-balance families.
    pub address_index_and_balance_readback: Duration,
    /// Scan of the address-transaction family.
    pub address_transaction_readback: Duration,
    /// Scan of the retained reorg-undo suffix.
    pub reorg_undo_readback: Duration,
}

/// Wall-clock duration of each observable fresh-build phase.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalletBuildPhaseDurations {
    /// Fresh path creation and durable BUILDING publication.
    pub store_initialization: Duration,
    /// Authenticated canonical replay scan and event staging.
    pub canonical_scan: Duration,
    /// External run finalization and merge for outpoint events.
    pub outpoint_sort: Duration,
    /// Ordered output/spend merge and primary-family SST writes.
    pub outpoint_merge: Duration,
    /// Secondary sorting, deduplication, balances, history, and retained undo.
    pub secondary_row_derivation: Duration,
    /// Row-count and full order-independent accumulator finalization.
    pub logical_evidence: Duration,
    /// External SST ingestion into the unpublished BUILDING store.
    pub row_load: Duration,
    /// Blocking flush, close, and cold BUILDING reopen.
    pub flush_and_cold_reopen: Duration,
    /// Digest and row-count readback against the candidate READY evidence.
    pub cold_validation: Duration,
    /// Breakdown of `cold_validation` by scanned row family.
    pub cold_validation_phases: WalletColdValidationPhaseDurations,
    /// Synchronous READY control publication and readback.
    pub ready_publication: Duration,
    /// Complete call duration.
    pub total: Duration,
}

/// Measured evidence from one completed fixed-tip wallet build.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RocksDbWalletBuildReport {
    /// Exact canonical epoch, tip, and event sequence represented by the store.
    pub source_position: WalletProjectionSourcePosition,
    /// Authenticated digest of every canonical facts row through the source tip.
    pub source_sequence_digest: CanonicalBlockFactsSequenceDigest,
    /// Canonical settled tip that bounds the retained undo suffix.
    pub settled_tip: BlockId,
    /// Digest of all six logical wallet row families.
    pub projection_digest: WalletProjectionDigest,
    /// Full durable row accumulator from which `projection_digest` is derived.
    pub projection_accumulator: WalletProjectionAccumulator,
    /// Exact row counts published by READY.
    pub row_counts: WalletProjectionFamilyRowCounts,
    /// Exact current transparent UTXO aggregate.
    pub utxo_summary: WalletUtxoSetSummary,
    /// Canonical blocks consumed by the single replay scan.
    pub scanned_block_count: u64,
    /// Canonical transactions inspected by the replay scan.
    pub scanned_transaction_count: u64,
    /// Transparent outputs staged for the outpoint merge.
    pub staged_output_count: u64,
    /// Non-coinbase transparent spends staged for the outpoint merge.
    pub staged_spend_count: u64,
    /// Historical canonical-store prevout reads; version 1 requires zero.
    pub historical_prevout_read_count: u64,
    /// Exact bounded-work evidence for the outpoint event sorter.
    pub outpoint_sort: VariableValueSortEvidence,
    /// Exact bounded-work evidence for the address-unspent secondary sorter.
    pub address_index_sort: VariableValueSortEvidence,
    /// Exact bounded-work evidence for the address-history secondary sorter.
    pub address_transaction_sort: VariableValueSortEvidence,
    /// Highest explicitly accounted retained reorg-undo footprint.
    pub peak_accounted_reorg_undo_bytes: u64,
    /// Caller-supplied retained reorg-undo ceiling.
    pub max_accounted_reorg_undo_bytes: u64,
    /// Highest accounted cold-validation reorg-undo suffix footprint.
    pub cold_validation_peak_accounted_reorg_undo_bytes: u64,
    /// Caller-supplied cold-validation reorg-undo suffix ceiling.
    pub cold_validation_max_accounted_reorg_undo_bytes: u64,
    /// Logical durable key and value bytes emitted across all six families.
    pub logical_row_bytes: u64,
    /// Physical bytes occupied by all generated SST files before ingestion.
    pub sst_file_bytes: u64,
    /// Number of generated SST files ingested into the BUILDING store.
    pub sst_file_count: u64,
    /// Random point reads performed by cold validation.
    pub cold_validation_random_read_count: u64,
    /// Phase-level wall-clock timings.
    pub phase_durations: WalletBuildPhaseDurations,
}

impl RocksDbWalletBuildReport {
    /// Returns the exact source identity required to reopen this wallet store.
    #[must_use]
    pub const fn canonical_source_identity(&self) -> WalletCanonicalSourceIdentity {
        WalletCanonicalSourceIdentity::new(
            self.source_position,
            self.source_sequence_digest,
            self.settled_tip,
        )
    }
}

/// Ready store and its measured fresh-build evidence.
pub struct RocksDbWalletBuildOutcome {
    /// Admitted READY wallet store, usable immediately by the caller.
    pub store: RocksDbWalletStore,
    /// Exact logical, physical, and timing evidence from construction.
    pub report: RocksDbWalletBuildReport,
}

/// Exact durable capability that must be cleared when an unpublished build fails.
///
/// The cleanup intentionally reopens through [`RocksDbWalletBuildStore`] and
/// delegates authorization to its compare-exact lease boundary. A stale owner,
/// generation, or expiry therefore cannot clear a lease acquired by another
/// build after a recovery race.
struct FailedWalletBuildLeaseCleanup {
    wallet_path: PathBuf,
    network: Network,
    resource_budget: RocksDbResourceBudget,
    lease: Option<WalletProjectionBuildLease>,
    now: UnixTimestampMillis,
}

impl FailedWalletBuildLeaseCleanup {
    fn new(
        wallet_path: &Path,
        network: Network,
        resource_budget: RocksDbResourceBudget,
        now: UnixTimestampMillis,
    ) -> Self {
        Self {
            wallet_path: wallet_path.to_path_buf(),
            network,
            resource_budget,
            lease: None,
            now,
        }
    }

    fn arm(&mut self, lease: WalletProjectionBuildLease, now: UnixTimestampMillis) {
        self.lease = Some(lease);
        self.now = now;
    }

    fn disarm(&mut self) {
        self.lease = None;
    }

    fn release_after_error(&mut self) -> Result<(), RocksDbWalletError> {
        let Some(lease) = self.lease.take() else {
            return Ok(());
        };
        let build_store = match RocksDbWalletBuildStore::open(
            &self.wallet_path,
            self.network,
            self.resource_budget,
        ) {
            Ok(build_store) => build_store,
            // A concurrent successful publication has already consumed the
            // lease. Never attempt a recovery mutation against READY state.
            Err(RocksDbWalletError::StoreNotReady { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        match build_store.release_lease(lease, self.now) {
            // An expired lease no longer blocks discard/retry. The remaining
            // variants prove that a different owner or generation now owns
            // the control record, so this failed build must not clear it.
            Ok(())
            | Err(
                RocksDbWalletError::WalletProjectionBuildLeaseExpired { .. }
                | RocksDbWalletError::WalletProjectionBuildLeaseMissing
                | RocksDbWalletError::WalletProjectionBuildLeaseOwnerMismatch { .. }
                | RocksDbWalletError::WalletProjectionBuildLeaseGenerationMismatch { .. }
                | RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch { .. },
            ) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

fn finish_failed_wallet_build<T>(
    cleanup: &mut FailedWalletBuildLeaseCleanup,
    build_error: RocksDbWalletError,
) -> Result<T, RocksDbWalletError> {
    match cleanup.release_after_error() {
        Ok(()) => Err(build_error),
        Err(cleanup_error) => Err(RocksDbWalletError::BuildLeaseCleanup {
            build_error: Box::new(build_error),
            cleanup_error: Box::new(cleanup_error),
        }),
    }
}

/// Builds and publishes a fresh wallet store at one canonical fence.
///
/// The target path and its deterministic sibling staging path must not exist.
/// Any failure leaves only a non-queryable BUILDING target; staging artifacts
/// owned by this invocation are removed on every return path.
#[allow(
    clippy::too_many_lines,
    reason = "the public orchestrator keeps phase timing and publication evidence in one order"
)]
pub fn build_wallet_from_canonical<Source>(
    canonical_store: &Source,
    wallet_path: impl AsRef<Path>,
    options: RocksDbWalletBuildOptions,
) -> Result<RocksDbWalletBuildOutcome, RocksDbWalletError>
where
    Source: WalletProjectionReplaySource + ?Sized,
{
    let canonical_ready = canonical_store.wallet_projection_ready_evidence();
    let source_identity = canonical_source_identity(&canonical_ready);
    let now = UnixTimestampMillis::now();
    let lease_request = WalletProjectionBuildLeaseRequest::new(
        default_build_owner(now),
        source_identity,
        WalletProjectionRetainedEventAnchor::new(canonical_ready.visible_event_sequence),
        UnixTimestampMillis::new(
            now.value()
                .saturating_add(DEFAULT_BUILD_LEASE_DURATION_MILLIS),
        ),
    );
    build_wallet_from_canonical_with_lease(
        canonical_store,
        wallet_path,
        options,
        lease_request,
        now,
    )
}

/// Builds and publishes a fresh wallet store under an explicit durable lease.
///
/// The request pins the exact canonical fence the finished projection must
/// reproduce. The build refuses a stale request before any projection rows are
/// admitted, and READY publication consumes the same active lease atomically.
pub fn build_wallet_from_canonical_with_lease<Source>(
    canonical_store: &Source,
    wallet_path: impl AsRef<Path>,
    options: RocksDbWalletBuildOptions,
    lease_request: WalletProjectionBuildLeaseRequest,
    now: UnixTimestampMillis,
) -> Result<RocksDbWalletBuildOutcome, RocksDbWalletError>
where
    Source: WalletProjectionReplaySource + ?Sized,
{
    let mut default_heartbeat = |_: WalletBuildLeasePhase, lease: WalletProjectionBuildLease| {
        let now = UnixTimestampMillis::now();
        Ok(default_build_lease_heartbeat(now, lease.expires_at()))
    };
    build_wallet_from_canonical_with_lease_and_heartbeat(
        canonical_store,
        wallet_path,
        options,
        WalletProjectionBuildLeaseExecution::new(lease_request, now),
        &mut default_heartbeat,
    )
}

/// Builds and publishes a fresh wallet store with caller-controlled lease heartbeats.
///
/// The callback runs between durable build phases. It supplies the time used
/// for lease admission and READY promotion, and may extend the lease without
/// reopening the wallet primary while the build owns it.
#[allow(
    clippy::too_many_lines,
    reason = "the public orchestrator keeps phase timing and publication evidence in one order"
)]
pub fn build_wallet_from_canonical_with_lease_and_heartbeat<Source, Heartbeat>(
    canonical_store: &Source,
    wallet_path: impl AsRef<Path>,
    options: RocksDbWalletBuildOptions,
    execution: WalletProjectionBuildLeaseExecution,
    heartbeat: &mut Heartbeat,
) -> Result<RocksDbWalletBuildOutcome, RocksDbWalletError>
where
    Source: WalletProjectionReplaySource + ?Sized,
    Heartbeat: FnMut(
        WalletBuildLeasePhase,
        zinder_wallet_projection::WalletProjectionBuildLease,
    ) -> Result<WalletBuildLeaseHeartbeat, RocksDbWalletError>,
{
    let lease_request = execution.lease_request();
    let now = execution.initial_now();
    let total_started = Instant::now();
    let wallet_path = wallet_path.as_ref();
    let staging = FreshWalletProjectionStaging::create(projection_staging_path(wallet_path))?;
    let canonical_ready = canonical_store.wallet_projection_ready_evidence();
    let source_position = WalletProjectionSourcePosition::new(
        canonical_ready.visible_epoch,
        canonical_ready.visible_tip,
        canonical_ready.visible_event_sequence,
    );
    let source_identity = canonical_source_identity(&canonical_ready);
    if lease_request.pinned_canonical_anchor() != source_identity {
        return Err(
            RocksDbWalletError::WalletProjectionBuildLeaseCanonicalAnchorMismatch {
                reason: "requested canonical anchor differs from the source READY fence",
            },
        );
    }

    let network = canonical_store.wallet_projection_network();
    let phase_started = Instant::now();
    let build_store = RocksDbWalletBuildStore::create_fresh(
        wallet_path,
        network,
        source_identity,
        options.supported_reorg_depth,
        options.resource_budget,
    )?;
    let mut builder = RocksDbWalletBuilder::create_fresh(build_store, lease_request, now)?;
    let store_initialization = phase_started.elapsed();
    let mut lease_cleanup =
        FailedWalletBuildLeaseCleanup::new(wallet_path, network, options.resource_budget, now);
    lease_cleanup.arm(builder.lease(), now);

    let build_result = (|| -> Result<RocksDbWalletBuildOutcome, RocksDbWalletError> {
        let initialized_heartbeat = heartbeat(WalletBuildLeasePhase::Initialized, builder.lease())?;
        if let Err(error) = builder.heartbeat(initialized_heartbeat) {
            lease_cleanup.arm(builder.lease(), initialized_heartbeat.now());
            return Err(error);
        }
        lease_cleanup.arm(builder.lease(), initialized_heartbeat.now());

        let data_options = builder.data_options();
        let mut prepared = write_projection_ssts(
            WalletProjectionLoadConfig {
                staging_path: staging.path(),
                options: &data_options,
                network,
                first_retained_block: canonical_ready.first_retained_block,
                settled_tip: canonical_ready.sequence_checkpoint.through(),
                supported_reorg_depth: options.supported_reorg_depth,
                max_outpoint_sort_memory_bytes: options.max_outpoint_sort_memory_bytes,
                max_secondary_sort_memory_bytes_per_sorter: options
                    .max_secondary_sort_memory_bytes_per_sorter,
                max_temporary_file_bytes_per_sorter: options.max_temporary_file_bytes_per_sorter,
                sst_target_logical_bytes: options.sst_target_logical_bytes,
                max_accounted_reorg_undo_bytes: options.max_accounted_reorg_undo_bytes,
            },
            canonical_store
                .wallet_projection_scan()
                .map_err(|source| RocksDbWalletError::CanonicalReplay { source })?,
        )?;
        let replay_heartbeat = heartbeat(
            WalletBuildLeasePhase::CanonicalReplayComplete,
            builder.lease(),
        )?;
        if let Err(error) = builder.heartbeat(replay_heartbeat) {
            lease_cleanup.arm(builder.lease(), replay_heartbeat.now());
            return Err(error);
        }
        lease_cleanup.arm(builder.lease(), replay_heartbeat.now());
        validate_canonical_fence(&prepared, &canonical_ready)?;

        let ready_evidence = WalletProjectionReadyEvidence {
            source_position,
            source_sequence_digest: prepared.source_sequence_digest,
            settled_tip: canonical_ready.sequence_checkpoint.through(),
            projection_digest: prepared.projection_digest,
            projection_accumulator: prepared.projection_accumulator.clone(),
            row_counts: prepared.row_counts,
            utxo_summary: prepared.utxo_summary.clone(),
        };
        let counters = prepared.counters;
        let load_durations = prepared.phase_durations;
        let outpoint_sort = prepared.outpoint_sort_evidence;
        let address_index_sort = prepared.address_index_sort_evidence;
        let address_transaction_sort = prepared.address_transaction_sort_evidence;
        let logical_row_bytes = prepared.logical_row_bytes;
        let sst_file_bytes = prepared.sst_file_bytes;
        let sst_file_count = prepared.sst_file_count;

        let phase_started = Instant::now();
        builder.ingest_projection_ssts(&mut prepared)?;
        let row_load = phase_started.elapsed();
        let loaded_heartbeat =
            heartbeat(WalletBuildLeasePhase::ProjectionRowsLoaded, builder.lease())?;
        if let Err(error) = builder.heartbeat(loaded_heartbeat) {
            lease_cleanup.arm(builder.lease(), loaded_heartbeat.now());
            return Err(error);
        }
        lease_cleanup.arm(builder.lease(), loaded_heartbeat.now());

        let phase_started = Instant::now();
        let cold_build = builder.reopen_for_validation()?;
        let flush_and_cold_reopen = phase_started.elapsed();

        let phase_started = Instant::now();
        let mut validated = cold_build.validate_rows(
            ready_evidence,
            WalletColdValidationConfig {
                max_accounted_reorg_undo_bytes: options.max_accounted_reorg_undo_bytes,
            },
        )?;
        let cold_validation = phase_started.elapsed();
        let validation_evidence = validated.validation_evidence();
        let validated_heartbeat = heartbeat(
            WalletBuildLeasePhase::ColdValidationComplete,
            validated.lease(),
        )?;
        if let Err(error) = validated.heartbeat(validated_heartbeat) {
            lease_cleanup.arm(validated.lease(), validated_heartbeat.now());
            return Err(error);
        }
        lease_cleanup.arm(validated.lease(), validated_heartbeat.now());
        staging.remove()?;

        let phase_started = Instant::now();
        let promotion_heartbeat =
            heartbeat(WalletBuildLeasePhase::BeforePromotion, validated.lease())?;
        let promotion_now = promotion_heartbeat.now();
        if let Err(error) = validated.heartbeat(promotion_heartbeat) {
            lease_cleanup.arm(validated.lease(), promotion_now);
            return Err(error);
        }
        lease_cleanup.arm(validated.lease(), promotion_now);
        validate_wallet_projection_pre_promotion_fence(canonical_store, validated.lease())?;
        let store = validated.publish_ready_at(promotion_now)?;
        let ready_publication = phase_started.elapsed();
        let total = total_started.elapsed();
        let report = RocksDbWalletBuildReport {
            source_position,
            source_sequence_digest: prepared.source_sequence_digest,
            settled_tip: canonical_ready.sequence_checkpoint.through(),
            projection_digest: prepared.projection_digest,
            projection_accumulator: prepared.projection_accumulator,
            row_counts: prepared.row_counts,
            utxo_summary: prepared.utxo_summary,
            scanned_block_count: counters.scanned_block_count,
            scanned_transaction_count: counters.scanned_transaction_count,
            staged_output_count: counters.staged_output_count,
            staged_spend_count: counters.staged_spend_count,
            historical_prevout_read_count: counters.historical_prevout_read_count,
            outpoint_sort,
            address_index_sort,
            address_transaction_sort,
            peak_accounted_reorg_undo_bytes: counters.peak_accounted_reorg_undo_bytes,
            max_accounted_reorg_undo_bytes: counters.max_accounted_reorg_undo_bytes,
            cold_validation_peak_accounted_reorg_undo_bytes: validation_evidence
                .peak_accounted_reorg_undo_bytes,
            cold_validation_max_accounted_reorg_undo_bytes: validation_evidence
                .max_accounted_reorg_undo_bytes,
            logical_row_bytes,
            sst_file_bytes,
            sst_file_count,
            cold_validation_random_read_count: validation_evidence.random_read_count,
            phase_durations: WalletBuildPhaseDurations {
                store_initialization,
                canonical_scan: load_durations.canonical_scan,
                outpoint_sort: load_durations.outpoint_sort,
                outpoint_merge: load_durations.outpoint_merge,
                secondary_row_derivation: load_durations.secondary_row_derivation,
                logical_evidence: load_durations.logical_evidence,
                row_load,
                flush_and_cold_reopen,
                cold_validation,
                cold_validation_phases: validation_evidence.phase_durations,
                ready_publication,
                total,
            },
        };
        Ok(RocksDbWalletBuildOutcome { store, report })
    })();

    match build_result {
        Ok(outcome) => {
            lease_cleanup.disarm();
            Ok(outcome)
        }
        Err(build_error) => finish_failed_wallet_build(&mut lease_cleanup, build_error),
    }
}

fn canonical_source_identity(
    evidence: &CanonicalStoreReadyEvidence,
) -> WalletCanonicalSourceIdentity {
    WalletCanonicalSourceIdentity::new(
        WalletProjectionSourcePosition::new(
            evidence.visible_epoch,
            evidence.visible_tip,
            evidence.visible_event_sequence,
        ),
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            CanonicalBlockFactsSequenceDigestVersion::V1,
            evidence.visible_block_count,
            evidence.visible_sequence_digest,
        ),
        evidence.sequence_checkpoint.through(),
    )
}

fn default_build_owner(now: UnixTimestampMillis) -> WalletProjectionBuildOwner {
    let mut owner = [0_u8; 16];
    owner[..8].copy_from_slice(&now.value().to_be_bytes());
    owner[8..12].copy_from_slice(&std::process::id().to_be_bytes());
    WalletProjectionBuildOwner::from_bytes(owner)
}

fn validate_canonical_fence(
    prepared: &PreparedWalletProjectionLoad,
    ready: &zinder_store::CanonicalStoreReadyEvidence,
) -> Result<(), RocksDbWalletError> {
    let expected_sequence_digest =
        CanonicalBlockFactsSequenceDigest::from_admitted_checkpoint_parts(
            ready.sequence_digest_version,
            ready.visible_block_count,
            ready.visible_sequence_digest,
        );
    if prepared.first_block != ready.first_retained_block {
        return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
            reason: "prepared first block differs from canonical READY",
        });
    }
    if prepared.tip != ready.visible_tip {
        return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
            reason: "prepared tip differs from canonical READY",
        });
    }
    if prepared.counters.scanned_block_count != ready.visible_block_count {
        return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
            reason: "prepared block count differs from canonical READY",
        });
    }
    if prepared.source_sequence_digest != expected_sequence_digest {
        return Err(RocksDbWalletError::CanonicalSourceFenceMismatch {
            reason: "prepared sequence digest differs from canonical READY",
        });
    }
    Ok(())
}

pub(crate) fn projection_staging_path(wallet_path: &Path) -> PathBuf {
    let file_name = wallet_path.file_name().unwrap_or(wallet_path.as_os_str());
    let mut staging_file_name = OsString::from(file_name);
    staging_file_name.push(".projection-load-staging");
    wallet_path.with_file_name(staging_file_name)
}

struct FreshWalletProjectionStaging {
    path: PathBuf,
    remove_on_drop: bool,
}

impl FreshWalletProjectionStaging {
    fn create(path: PathBuf) -> Result<Self, RocksDbWalletError> {
        match fs::create_dir(&path) {
            Ok(()) => Ok(Self {
                path,
                remove_on_drop: true,
            }),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(RocksDbWalletError::ProjectionStagingPathNotFresh { path })
            }
            Err(source) => Err(RocksDbWalletError::PathUnavailable { path, source }),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn remove(mut self) -> Result<(), RocksDbWalletError> {
        fs::remove_dir_all(&self.path).map_err(|source| RocksDbWalletError::PathUnavailable {
            path: self.path.clone(),
            source,
        })?;
        self.remove_on_drop = false;
        Ok(())
    }
}

impl Drop for FreshWalletProjectionStaging {
    fn drop(&mut self) {
        if self.remove_on_drop {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_heartbeat_strictly_extends_an_equal_expiry() {
        let now = UnixTimestampMillis::new(10);
        let current_expires_at = UnixTimestampMillis::new(
            now.value()
                .saturating_add(DEFAULT_BUILD_LEASE_DURATION_MILLIS),
        );
        let expected_expires_at = UnixTimestampMillis::new(current_expires_at.value() + 1);

        assert_eq!(
            default_build_lease_heartbeat(now, current_expires_at),
            WalletBuildLeaseHeartbeat::renew(now, expected_expires_at)
        );
    }

    #[test]
    fn projection_staging_path_is_a_sibling_for_trailing_separators() {
        assert_eq!(
            projection_staging_path(Path::new("/var/lib/zinder/wallet/")),
            Path::new("/var/lib/zinder/wallet.projection-load-staging")
        );
    }
}
