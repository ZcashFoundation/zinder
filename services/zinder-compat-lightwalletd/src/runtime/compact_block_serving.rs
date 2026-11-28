//! Private compact-blocks compatibility composition.
//!
//! This module owns the canonical-secondary lifecycle that publishes the
//! reader [`crate::CompactBlocksGrpcAdapter`] serves. No compact-only
//! topology type crosses the binary boundary.
//!
//! Both generations open on one block cache that the publisher owns, so the
//! configured cache size bounds the process, and a generation opened on a
//! refresh finds the blocks the previous one loaded.

use std::{num::NonZeroU32, path::PathBuf, sync::Arc, time::Duration};

use tokio::{task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status};
use zinder_core::Network;
use zinder_proto::v1::ingest::{
    CanonicalWriterFence, CanonicalWriterStatusRequest, CanonicalWriterStatusResponse,
    canonical_control_client::CanonicalControlClient,
};
use zinder_query::{
    CanonicalReader, RocksDbCanonicalReader, StorageReadError, wallet_ingest_control_request,
};
use zinder_runtime::{
    AuthenticatedChannel, BearerToken, CanonicalFenceConvergence, CanonicalFenceConvergenceError,
    CanonicalFenceConvergencePolicy, CanonicalFenceProbe, CanonicalFenceRelation, Readiness,
    ReadinessCause, ReadinessState, WriterAttestation, classify_canonical_writer_fence,
    connect_zinder_grpc, converge_on_canonical_writer_fence, expected_writer_fence,
    refresh_within_stale_deadline, writer_attests_network_upgrade_schedule,
};
use zinder_store::{
    CanonicalActivationSchedule, CanonicalReorgPolicy, CanonicalStoreError,
    CanonicalStoreRequirements, CanonicalStoredSet, ResourceGaugeThrottle,
    RocksDbCanonicalSecondary, RocksDbOpenResources, RocksDbResourceBudget,
    SharedSecondaryResources, StoreRole, record_instance_memory_gauges,
};

use crate::CanonicalReaderSlot;

const GENERATION_COUNT: usize = 2;
const CONVERGENCE_RETRY_DELAY_CAP: Duration = Duration::from_millis(100);
const STORED_SET_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Compact-block secondary publisher lifecycle error.
#[derive(Debug, thiserror::Error)]
pub enum CompactBlockServingError {
    /// Canonical store read or lifecycle error.
    #[error("compact-block storage failed: {0}")]
    Storage(#[from] CanonicalStoreError),
    /// Canonical reader admission failed.
    #[error("compact-block generation could not be admitted: {0}")]
    Admission(#[from] StorageReadError),
    /// Upstream writer control status error.
    #[error("compact-block writer status failed: {0}")]
    WriterStatus(String),
    /// Runtime task supervision error.
    #[error("compact-block runtime failed: {0}")]
    Runtime(String),
    /// Secondary publisher teardown task join error.
    #[error("compact-block publisher teardown failed: {0}")]
    Teardown(#[source] tokio::task::JoinError),
}

/// Drops `publisher` on a blocking thread, because the last owner of a
/// secondary pays its close.
async fn tear_down_on_blocking_thread<T: Send + 'static>(
    publisher: T,
) -> Result<(), CompactBlockServingError> {
    tokio::task::spawn_blocking(move || drop(publisher))
        .await
        .map_err(CompactBlockServingError::Teardown)?;
    tracing::info!(
        target: "zinder::compat_lightwalletd",
        event = "compact_serving_publisher_teardown_complete",
        "compact serving publisher teardown completed"
    );
    Ok(())
}

async fn close_unpublished_generation<T: Send + 'static>(generation: Option<T>) {
    let Some(generation) = generation else {
        return;
    };
    if let Err(error) = tokio::task::spawn_blocking(move || drop(generation)).await {
        tracing::error!(
            target: "zinder::compat_lightwalletd",
            event = "compact_serving_candidate_teardown_failed",
            error = %error,
            "unpublished compact-block generation teardown failed"
        );
    }
}

#[derive(Debug, thiserror::Error)]
enum ConvergenceFailure {
    #[error("writer status transport failed: {0}")]
    WriterStatusTransport(String),
    #[error("writer is ahead of the candidate secondary")]
    WriterAhead,
    #[error("writer fence diverged from the candidate secondary")]
    WriterFenceDiverged,
    #[error("candidate storage failed: {0}")]
    Storage(CompactBlockServingError),
}

/// Readiness inputs and reader admission policy for the selected canonical serving mode.
#[derive(Clone)]
pub(crate) struct CompactServingReadiness {
    inputs: zinder_query::WalletServingReadiness,
    requires_writer_schedule: bool,
}

impl CompactServingReadiness {
    pub(crate) fn new(runtime: Readiness) -> Self {
        Self {
            inputs: zinder_query::WalletServingReadiness::pair_only(runtime),
            requires_writer_schedule: false,
        }
    }

    pub(crate) fn for_light_wallet(runtime: Readiness) -> Self {
        Self {
            inputs: zinder_query::WalletServingReadiness::with_node_and_ingest_control(runtime),
            requires_writer_schedule: true,
        }
    }

    pub(crate) fn dependencies(&self) -> zinder_query::WalletServingReadiness {
        self.inputs.clone()
    }

    pub(crate) fn runtime(&self) -> Readiness {
        self.inputs.runtime_readiness()
    }

    pub(crate) fn publish_storage(&self, storage: ReadinessState) {
        self.inputs.canonical_admission().publish_admission(storage);
    }

    fn begin_refresh(&self, attestation: &mut WriterAttestation, reader: &RocksDbCanonicalReader) {
        if self.requires_writer_schedule {
            self.inputs.begin_canonical_refresh(
                attestation,
                reader.event_fence().visible_tip().height.value(),
            );
        }
    }

    fn confirm_attestation(&self) {
        if self.requires_writer_schedule {
            self.inputs.confirm_canonical_attestation();
        }
    }

    fn accepts_reader_schedule(
        &self,
        status: &CanonicalWriterStatusResponse,
        schedule: Option<&CanonicalActivationSchedule>,
    ) -> bool {
        !self.requires_writer_schedule || writer_attests_network_upgrade_schedule(status, schedule)
    }

    fn observe_reader_schedule(
        &self,
        attestation: &WriterAttestation,
        status: &CanonicalWriterStatusResponse,
        reader: &RocksDbCanonicalReader,
    ) -> bool {
        if self.accepts_reader_schedule(status, reader.network_upgrade_schedule()) {
            return true;
        }
        attestation.withdraw_allowance();
        let fence = reader.event_fence();
        let lag = status.fence.as_ref().map_or(0, |writer| {
            writer
                .chain_epoch_id
                .saturating_sub(fence.chain_epoch_id().value())
        });
        self.publish_storage(ReadinessState::replica_lagging(
            lag,
            Some(fence.visible_tip().height.value()),
        ));
        false
    }

    /// Publishes the status of the chain epoch `reader` serves.
    fn publish_served_epoch(&self, reader: &RocksDbCanonicalReader) {
        self.inputs
            .runtime_readiness()
            .set_served_chain_epoch(&reader.chain_epoch());
    }

    pub(crate) fn publish_shutting_down(&self) {
        self.inputs.publish_shutting_down();
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CompactBlockServingConfig {
    pub(crate) canonical_primary_path: PathBuf,
    pub(crate) canonical_secondary_root: PathBuf,
    pub(crate) network: Network,
    pub(crate) requirements: CanonicalStoreRequirements,
    pub(crate) reorg_policy: CanonicalReorgPolicy,
    pub(crate) resource_budget: RocksDbResourceBudget,
    pub(crate) catchup_interval: Duration,
    pub(crate) convergence_timeout: Duration,
    pub(crate) convergence_attempts: u8,
    pub(crate) staleness_ceiling: Duration,
    pub(crate) lag_threshold: u64,
}

struct WriterStatus {
    client: CanonicalControlClient<AuthenticatedChannel>,
}

impl WriterStatus {
    async fn connect(endpoint: &str, token: Option<&BearerToken>) -> Result<Self, Status> {
        let channel = connect_zinder_grpc(endpoint, token)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        Ok(Self {
            client: CanonicalControlClient::new(channel),
        })
    }

    /// Bounded by the wallet ingest-control request timeout, so a writer that
    /// accepts the call and never answers cannot hold the publisher's refresh
    /// open. Admission ends at the staleness ceiling meanwhile.
    async fn fetch(&mut self) -> Result<CanonicalWriterStatusResponse, Status> {
        self.client
            .writer_status(wallet_ingest_control_request(
                CanonicalWriterStatusRequest {},
            ))
            .await
            .map(Response::into_inner)
            .map_err(|error| Status::unavailable(error.to_string()))
    }
}

pub(crate) struct CompactBlockPublisher {
    config: CompactBlockServingConfig,
    writer_status: WriterStatus,
    readiness: CompactServingReadiness,
    slot: CanonicalReaderSlot,
    active_generation: usize,
    retired: Option<Arc<RocksDbCanonicalReader>>,
    stored_set_retry_at: Option<Instant>,
    /// Both generations use this cache and statistics base so retirement preserves
    /// the blocks and read counts from the replaced generation.
    resources: SharedSecondaryResources,
    /// Gates the memory sample of the open generations, whose table-cache
    /// lookups are too costly to repeat on every refresh.
    instance_memory_throttle: ResourceGaugeThrottle,
    /// Last attestation of the active reader by a following writer with a
    /// confirmed tip, and the halt the writer last reported, which
    /// writer-status failures republish.
    writer_attestation: WriterAttestation,
    #[cfg(test)]
    metrics_recorder: Option<Arc<metrics_exporter_prometheus::PrometheusRecorder>>,
}

impl CompactBlockPublisher {
    pub(crate) async fn bootstrap(
        config: CompactBlockServingConfig,
        readiness: CompactServingReadiness,
        writer_status_endpoint: &str,
        bearer_token: Option<&BearerToken>,
    ) -> Result<(Self, CanonicalReaderSlot), CompactBlockServingError> {
        let mut writer_status = WriterStatus::connect(writer_status_endpoint, bearer_token)
            .await
            .map_err(|error| CompactBlockServingError::WriterStatus(error.message().to_owned()))?;
        let resources = SharedSecondaryResources::new(config.resource_budget);
        let mut writer_attestation =
            WriterAttestation::new(config.network, config.staleness_ceiling);
        let (candidate, attesting_status) = converge_candidate(
            &mut writer_status,
            &config,
            CandidateGeneration {
                resources: &resources,
                generation: 0,
            },
            ServedReader {
                attestation: &mut writer_attestation,
                readiness: &readiness,
                active: None,
            },
            &CancellationToken::new(),
        )
        .await
        .and_then(|candidate| candidate.ok_or_else(candidate_unavailable))
        .map_err(|failure| match failure {
            ConvergenceFailure::Storage(error) => error,
            failure @ (ConvergenceFailure::WriterStatusTransport(_)
            | ConvergenceFailure::WriterAhead
            | ConvergenceFailure::WriterFenceDiverged) => {
                CompactBlockServingError::Runtime(failure.to_string())
            }
        })?;
        let reader = Arc::new(admit_generation(candidate, None).await?);
        let slot = CanonicalReaderSlot::new(Arc::clone(&reader));
        let visible_height = reader.event_fence().visible_tip().height.value();
        let refusal = writer_attestation.observe(&attesting_status, Some(visible_height));
        if refusal.is_none() {
            writer_attestation.attest();
        }
        readiness.publish_served_epoch(&reader);
        readiness.publish_storage(
            refusal.unwrap_or_else(|| ReadinessState::ready(Some(visible_height))),
        );
        let publisher = Self {
            config,
            writer_status,
            readiness,
            slot: slot.clone(),
            active_generation: 0,
            retired: None,
            stored_set_retry_at: None,
            resources,
            instance_memory_throttle: ResourceGaugeThrottle::for_instance_memory(),
            writer_attestation,
            #[cfg(test)]
            metrics_recorder: None,
        };
        publisher.record_resource_gauges().await;
        Ok((publisher, slot))
    }

    /// Refreshes the served generation until `cancel` is cancelled, then
    /// closes the publisher's secondaries on a blocking thread once
    /// `serving_drained` is cancelled. The task fails when that teardown
    /// fails.
    pub(crate) fn spawn(
        mut self,
        cancel: CancellationToken,
        serving_drained: CancellationToken,
    ) -> JoinHandle<Result<(), CompactBlockServingError>> {
        tokio::spawn(async move {
            // Refreshing at least every half ceiling lets a writer that
            // answers attest the reader before the ceiling passes. The
            // deadline ends admission only for a refresh left unanswered,
            // even while it still runs, or for a loop that starts late.
            let refresh_interval = self
                .writer_attestation
                .refresh_interval(self.config.catchup_interval);
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(refresh_interval) => {}
                    () = self.writer_attestation.stale_deadline().passed() => {}
                }
                let deadline = self.writer_attestation.stale_deadline();
                let admission = self.readiness.inputs.canonical_admission().clone();
                refresh_within_stale_deadline(
                    deadline,
                    &admission,
                    Box::pin(self.refresh_once(&cancel)),
                )
                .await;
                if cancel.is_cancelled() {
                    break;
                }
                self.writer_attestation.end_lapsed_allowance();
                self.record_resource_gauges().await;
            }
            serving_drained.cancelled().await;
            tear_down_on_blocking_thread(self).await
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Replacement admission keeps writer attestation, generation leasing, and readiness transitions together."
    )]
    async fn refresh_once(&mut self, shutdown: &CancellationToken) {
        let active = self.slot.capture();
        self.readiness
            .begin_refresh(&mut self.writer_attestation, &active);
        let status = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            status = self.writer_status.fetch() => status,
        };
        let status = match status {
            Ok(status) => status,
            Err(_error) => {
                // A writer last seen parked keeps its halt through an outage;
                // only a valid status without the halt clears it.
                if let Some(halt) = self.writer_attestation.remembered_halt() {
                    self.readiness.publish_storage(halt);
                } else {
                    self.record_transport_failure(
                        active.event_fence().visible_tip().height.value(),
                    );
                }
                return;
            }
        };
        // A parked writer keeps attesting its frozen fence; the reader stays
        // published but refuses traffic with the writer's typed halt.
        if let Some(refusal) = self.writer_attestation.observe(
            &status,
            Some(active.event_fence().visible_tip().height.value()),
        ) {
            self.readiness.publish_storage(refusal);
            return;
        }
        let behind = match classify_canonical_writer_fence(
            &status,
            self.config.network,
            &expected_writer_fence(active.event_fence()),
        ) {
            CanonicalFenceRelation::Diverged(_) => {
                self.record_failure(
                    ReadinessCause::SchemaMismatch,
                    active.event_fence().visible_tip().height.value(),
                );
                return;
            }
            CanonicalFenceRelation::Matches => None,
            CanonicalFenceRelation::SecondaryBehind(behind) => Some(behind),
        };
        let lag = behind.map_or(0, |behind| behind.lag_chain_epochs);
        let accepts_active_schedule =
            self.readiness
                .observe_reader_schedule(&self.writer_attestation, &status, &active);
        let visible_height = Some(active.event_fence().visible_tip().height.value());
        if accepts_active_schedule && behind.is_none() {
            self.stored_set_retry_at = None;
            self.writer_attestation.attest();
            self.readiness.confirm_attestation();
            self.readiness
                .publish_storage(ReadinessState::ready(visible_height));
            return;
        }
        if let Some(retry_at) = self.stored_set_retry_at {
            self.readiness
                .publish_storage(ReadinessState::not_ready(ReadinessCause::SchemaMismatch));
            if Instant::now() < retry_at {
                return;
            }
        } else if let Some(behind) = behind.filter(|_| accepts_active_schedule) {
            if lag <= self.config.lag_threshold {
                self.readiness
                    .publish_storage(ReadinessState::ready_with_target(
                        visible_height,
                        Some(behind.writer_visible_tip_height),
                    ));
            } else if let Some(staleness) = self.writer_attestation.stale_allowance() {
                self.readiness
                    .publish_storage(ReadinessState::serving_pair_stale(
                        lag,
                        staleness.as_secs(),
                        visible_height,
                    ));
            } else {
                self.record_failure(
                    ReadinessCause::ReplicaLagging {
                        lag_chain_epochs: lag,
                    },
                    active.event_fence().visible_tip().height.value(),
                );
            }
        }
        if let Some(retired) = self.retired.take() {
            // A request or a read in flight still holds the retired
            // generation, whose secondary path the candidate reopens.
            let retired = match Arc::try_unwrap(retired).map(RocksDbCanonicalReader::into_secondary)
            {
                Ok(Ok(secondary)) => secondary,
                Ok(Err(reader)) => {
                    self.retired = Some(Arc::new(reader));
                    return;
                }
                Err(retired) => {
                    self.retired = Some(retired);
                    return;
                }
            };
            if let Err(error) = tokio::task::spawn_blocking(move || drop(retired)).await {
                tracing::error!(
                    target: "zinder::compat_lightwalletd",
                    event = "compact_serving_retired_generation_teardown_failed",
                    error = %error,
                    "retired compact serving generation teardown failed"
                );
            }
        }
        let candidate_generation = (self.active_generation + 1) % GENERATION_COUNT;
        let converged = converge_candidate(
            &mut self.writer_status,
            &self.config,
            CandidateGeneration {
                resources: &self.resources,
                generation: candidate_generation,
            },
            ServedReader {
                attestation: &mut self.writer_attestation,
                readiness: &self.readiness,
                active: Some(&active),
            },
            shutdown,
        )
        .await;
        // A halt or store damage a status read during convergence reported
        // outlasts the failure that ended it.
        if let (Err(_), Some(halt)) = (&converged, self.writer_attestation.remembered_halt()) {
            self.readiness.publish_storage(halt);
            return;
        }
        let (candidate, attesting_status) = match converged {
            Ok(Some(converged)) => converged,
            Ok(None) => return,
            Err(ConvergenceFailure::WriterStatusTransport(_)) => {
                let cause = self.writer_attestation.stale_allowance().map_or(
                    ReadinessCause::WriterStatusUnavailable,
                    |staleness| ReadinessCause::ServingPairStale {
                        lag_chain_epochs: lag,
                        staleness_seconds: staleness.as_secs(),
                    },
                );
                self.record_failure(cause, active.event_fence().visible_tip().height.value());
                return;
            }
            Err(ConvergenceFailure::WriterAhead) => {
                let cause = self.writer_attestation.stale_allowance().map_or(
                    ReadinessCause::ReplicaLagging {
                        lag_chain_epochs: lag,
                    },
                    |staleness| ReadinessCause::ServingPairStale {
                        lag_chain_epochs: lag,
                        staleness_seconds: staleness.as_secs(),
                    },
                );
                self.record_failure(cause, active.event_fence().visible_tip().height.value());
                return;
            }
            Err(ConvergenceFailure::WriterFenceDiverged) => {
                self.record_failure(
                    ReadinessCause::SchemaMismatch,
                    active.event_fence().visible_tip().height.value(),
                );
                return;
            }
            Err(ConvergenceFailure::Storage(_)) => {
                self.record_failure(
                    ReadinessCause::StorageUnavailable,
                    active.event_fence().visible_tip().height.value(),
                );
                return;
            }
        };
        let stored_set = active.secondary().stored_set();
        let stored_set_changed = candidate.stored_set() != stored_set;
        let admitted = admit_generation(candidate, Some(stored_set)).await;
        if shutdown.is_cancelled() {
            close_unpublished_generation(admitted.ok()).await;
            return;
        }
        let reader = match admitted {
            Ok(reader) => reader,
            Err(error) => {
                if stored_set_changed {
                    self.writer_attestation.withdraw_allowance();
                    self.stored_set_retry_at = Some(Instant::now() + STORED_SET_RETRY_DELAY);
                }
                let cause = if matches!(error, CompactBlockServingError::Storage(_)) {
                    ReadinessCause::SchemaMismatch
                } else {
                    ReadinessCause::StorageUnavailable
                };
                tracing::warn!(%error, "refusing compact-block replacement generation");
                self.record_failure(cause, active.event_fence().visible_tip().height.value());
                return;
            }
        };
        self.stored_set_retry_at = None;
        let reader = Arc::new(reader);
        self.retired = Some(self.slot.publish(Arc::clone(&reader)));
        self.active_generation = candidate_generation;
        self.readiness.publish_served_epoch(&reader);
        // The writer can park between the first fetch and the one that
        // attested the candidate; the attesting status decides, as at bootstrap.
        let visible_height = reader.event_fence().visible_tip().height.value();
        let refusal = self
            .writer_attestation
            .observe(&attesting_status, Some(visible_height));
        if refusal.is_none() {
            self.writer_attestation.attest();
            self.readiness.confirm_attestation();
        }
        self.readiness.publish_storage(
            refusal.unwrap_or_else(|| ReadinessState::ready(Some(visible_height))),
        );
    }

    /// Publishes shared cache usage, cumulative tickers, and once per throttle
    /// interval the cache composition, per-column-family properties of the
    /// active generation, and the summed memory of the active generation and
    /// of a replaced one that requests still hold.
    ///
    /// The memory sample looks up every table file's reader, so it runs on
    /// the blocking pool instead of the publisher task.
    async fn record_resource_gauges(&self) {
        self.resources
            .record_resource_gauges(StoreRole::CanonicalSecondary);
        if !self.instance_memory_throttle.should_sample() {
            return;
        }
        let active = self.slot.capture();
        let retired = self.retired.clone();
        #[cfg(test)]
        let metrics_recorder = self.metrics_recorder.clone();
        let sampled = tokio::task::spawn_blocking(move || {
            let sample = || {
                active.secondary().record_block_cache_entry_gauges();
                active.secondary().record_column_family_property_gauges();
                std::iter::once(active)
                    .chain(retired)
                    .map(|generation| generation.secondary().instance_memory())
                    .sum()
            };
            #[cfg(test)]
            if let Some(recorder) = metrics_recorder {
                return metrics::with_local_recorder(recorder.as_ref(), sample);
            }
            sample()
        })
        .await;
        if let Ok(instances) = sampled {
            record_instance_memory_gauges(StoreRole::CanonicalSecondary, instances);
        }
    }

    fn record_failure(&self, cause: ReadinessCause, height: u32) {
        metrics::counter!(
            "zinder_compat_compact_refresh_failures_total",
            "cause" => cause.metric_label()
        )
        .increment(1);
        tracing::warn!(
            target: "zinder::compat_lightwalletd",
            event = "compact_serving_refresh_failed",
            cause = cause.metric_label(),
            height,
            "compact serving refresh failed"
        );
        self.readiness.publish_storage(ReadinessState {
            cause,
            current_height: Some(height),
            target_height: None,
            phase: None,
        });
    }

    fn record_transport_failure(&self, height: u32) {
        self.record_failure(
            transport_failure_cause(self.writer_attestation.stale_allowance()),
            height,
        );
    }
}

fn transport_failure_cause(stale_allowance: Option<Duration>) -> ReadinessCause {
    stale_allowance.map_or(ReadinessCause::WriterStatusUnavailable, |staleness| {
        ReadinessCause::ServingPairStale {
            lag_chain_epochs: 0,
            staleness_seconds: staleness.as_secs(),
        }
    })
}

async fn open_generation(
    config: &CompactBlockServingConfig,
    candidate: CandidateGeneration<'_>,
) -> Result<RocksDbCanonicalSecondary, CompactBlockServingError> {
    let CandidateGeneration {
        resources,
        generation,
    } = candidate;
    let primary = config.canonical_primary_path.clone();
    let secondary_root = config.canonical_secondary_root.clone();
    let network = config.network;
    let requirements = config.requirements.clone();
    let reorg = config.reorg_policy;
    let resources = RocksDbOpenResources::from(resources);
    tokio::task::spawn_blocking(move || {
        let secondary = secondary_root.join(format!("generation-{generation}"));
        std::fs::create_dir_all(&secondary).map_err(|error| {
            CompactBlockServingError::Runtime(format!(
                "cannot create compact-block secondary: {error}"
            ))
        })?;
        RocksDbCanonicalSecondary::open_ready_adopting_writer_schedule(
            primary,
            secondary,
            network,
            requirements,
            reorg,
            resources,
        )
        .map_err(CompactBlockServingError::Storage)
    })
    .await
    .map_err(|error| {
        CompactBlockServingError::Runtime(format!("compact-block secondary task failed: {error}"))
    })?
}

/// Admits a converged candidate as the reader requests read, reading the
/// chain epoch it serves.
async fn admit_generation(
    candidate: RocksDbCanonicalSecondary,
    admitted_stored_set: Option<CanonicalStoredSet>,
) -> Result<RocksDbCanonicalReader, CompactBlockServingError> {
    tokio::task::spawn_blocking(move || {
        if let Some(expected) = admitted_stored_set {
            candidate.require_stored_set(expected)?;
        }
        RocksDbCanonicalReader::new(candidate).map_err(CompactBlockServingError::Admission)
    })
    .await
    .map_err(|error| {
        CompactBlockServingError::Runtime(format!("compact-block admission task failed: {error}"))
    })?
}

async fn catch_up_generation(
    mut candidate: RocksDbCanonicalSecondary,
) -> Result<RocksDbCanonicalSecondary, CompactBlockServingError> {
    Ok(
        tokio::task::spawn_blocking(move || candidate.try_catch_up().map(|_| candidate))
            .await
            .map_err(|error| {
                CompactBlockServingError::Runtime(format!(
                    "compact-block catch-up task failed: {error}"
                ))
            })??,
    )
}

/// The generation slot a candidate opens in, on the publisher's shared cache.
#[derive(Clone, Copy)]
struct CandidateGeneration<'a> {
    resources: &'a SharedSecondaryResources,
    generation: usize,
}

struct CandidateFenceProbe<'a> {
    writer_status: &'a mut WriterStatus,
    candidate: Option<RocksDbCanonicalSecondary>,
    served: ServedReader<'a>,
}

/// The reader a converging candidate would replace, which every writer
/// status the convergence reads may refuse.
struct ServedReader<'a> {
    attestation: &'a mut WriterAttestation,
    readiness: &'a CompactServingReadiness,
    /// The published reader, or `None` at bootstrap.
    active: Option<&'a RocksDbCanonicalReader>,
}

impl CanonicalFenceProbe for CandidateFenceProbe<'_> {
    type Error = ConvergenceFailure;

    async fn catch_up(&mut self) -> Result<Option<CanonicalWriterFence>, ConvergenceFailure> {
        let candidate = self.candidate.take().ok_or_else(candidate_unavailable)?;
        let candidate = catch_up_generation(candidate)
            .await
            .map_err(ConvergenceFailure::Storage)?;
        let fence = expected_writer_fence(candidate.event_fence());
        self.candidate = Some(candidate);
        Ok(Some(fence))
    }

    async fn writer_status(&mut self) -> Result<CanonicalWriterStatusResponse, ConvergenceFailure> {
        self.writer_status
            .fetch()
            .await
            .map_err(|error| ConvergenceFailure::WriterStatusTransport(error.message().to_owned()))
    }

    fn attests_writer_schedule(&self, status: &CanonicalWriterStatusResponse) -> bool {
        self.candidate.as_ref().is_some_and(|candidate| {
            self.served
                .readiness
                .accepts_reader_schedule(status, candidate.network_upgrade_schedule())
        })
    }

    fn observe_writer_status(&mut self, status: &CanonicalWriterStatusResponse) {
        let height = self
            .served
            .active
            .map(|reader| reader.event_fence().visible_tip().height.value());
        if let Some(refusal) = self.served.attestation.observe(status, height) {
            self.served.readiness.publish_storage(refusal);
        } else if let Some(reader) = self.served.active {
            self.served
                .readiness
                .observe_reader_schedule(self.served.attestation, status, reader);
        }
    }
}

fn candidate_unavailable() -> ConvergenceFailure {
    ConvergenceFailure::Storage(CompactBlockServingError::Runtime(
        "compact-block candidate is unavailable".to_owned(),
    ))
}

/// Opens a candidate in `generation`, converges it on the writer fence, and
/// returns it with the writer status that attested it, or `None` on shutdown.
async fn converge_candidate<'a>(
    writer_status: &'a mut WriterStatus,
    config: &CompactBlockServingConfig,
    generation: CandidateGeneration<'_>,
    served: ServedReader<'a>,
    shutdown: &CancellationToken,
) -> Result<Option<(RocksDbCanonicalSecondary, CanonicalWriterStatusResponse)>, ConvergenceFailure>
{
    let retry_delay = config.catchup_interval.min(CONVERGENCE_RETRY_DELAY_CAP);
    if shutdown.is_cancelled() {
        return Ok(None);
    }
    let opened = open_generation(config, generation).await;
    if shutdown.is_cancelled() {
        close_unpublished_generation(opened.ok()).await;
        return Ok(None);
    }
    let candidate = opened.map_err(ConvergenceFailure::Storage)?;
    let mut probe = CandidateFenceProbe {
        writer_status,
        candidate: Some(candidate),
        served,
    };
    let outcome = converge_on_canonical_writer_fence(
        &mut probe,
        config.network,
        CanonicalFenceConvergencePolicy {
            timeout: config.convergence_timeout,
            max_attempts: NonZeroU32::new(u32::from(config.convergence_attempts.max(1))),
            initial_delay: retry_delay,
            max_delay: retry_delay,
        },
        shutdown,
    )
    .await;
    let unpublished = match outcome {
        Ok(CanonicalFenceConvergence::Converged(attesting_status)) => {
            let candidate = probe.candidate.ok_or_else(candidate_unavailable)?;
            return Ok(Some((candidate, *attesting_status)));
        }
        Ok(CanonicalFenceConvergence::Cancelled) => Ok(None),
        Ok(CanonicalFenceConvergence::Trailing(_))
        | Err(CanonicalFenceConvergenceError::DidNotConverge) => {
            Err(ConvergenceFailure::WriterAhead)
        }
        Err(CanonicalFenceConvergenceError::Probe(failure)) => Err(failure),
        Err(CanonicalFenceConvergenceError::Diverged(_)) => {
            Err(ConvergenceFailure::WriterFenceDiverged)
        }
    };
    close_unpublished_generation(probe.candidate).await;
    unpublished
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod teardown_tests;
